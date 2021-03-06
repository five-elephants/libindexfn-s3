use libindexfn::{AccessStorage,ObjectName,IdxResult,IdxError};
use rusoto_core::{Region,ByteStream};
use rusoto_s3::{S3,S3Client,GetObjectRequest,ListObjectsV2Request,PutObjectRequest};
use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use log::{debug,info,warn};

#[derive(Clone)]
pub struct S3Storage {
    bucket: String,
    prefix: String,
    client: S3Client,
}


impl S3Storage {
    pub fn new(region: Region, bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        let client = S3Client::new(region);
        let bucket = bucket.into();
        let mut prefix = prefix.into();

        if !prefix.ends_with('/') && !prefix.is_empty() {
            prefix.push('/');
        }

        info!("Creating new S3Storage for bucket '{}' with prefix '{}'", bucket, prefix);

        Self {
            bucket: bucket,
            prefix: prefix,
            client,
        }
    }


    fn make_prefix(&self, dir_name: ObjectName<'_>) -> Option<String> {
        let mut s = self.prefix.clone() + dir_name.as_str();
        if !s.is_empty() {
            if !s.ends_with('/') {
                s.push('/');
            }

            Some(s)
        } else {
            None
        }
    }


    fn strip_prefix(&self, dir_name: ObjectName<'_>, key: String) -> Option<String> {
        if let Some(prefix) = self.make_prefix(dir_name) {
            key.strip_prefix(&prefix)
                .map(|s| s.to_string())
        } else {
            Some(key.clone())
        }
    }


    fn make_key(&self, obj_name: ObjectName<'_>) -> String {
        if !self.prefix.is_empty() {
            self.prefix.clone() + obj_name.as_str()
        } else {
            String::from(obj_name.as_str())
        }
    }
}


#[async_trait]
impl AccessStorage for S3Storage {
    type ListIntoIter = Vec<String>;

    async fn list(&self, dir_name: ObjectName<'_>) -> IdxResult<Self::ListIntoIter> {
        let prefix = self.make_prefix(dir_name);

        info!("List request for directory '{}' using prefix '{:?}'",
                dir_name.as_str(), prefix);

        let mut req = ListObjectsV2Request {
            bucket: self.bucket.clone(),
            prefix: prefix,
            ..ListObjectsV2Request::default()
        };

        let mut rv: Vec<String> = Vec::new();
        loop {
            let listing = self.client.list_objects_v2(req.clone()).await
                .map_err(IdxError::storage_error)?;
            if let Some(objects) = listing.contents {
                for object in objects {
                    if let Some(key) = object.key {
                        debug!("Listing '{}'", key);
                        let s = self.strip_prefix(dir_name, key)
                            .ok_or(IdxError::storage_error_msg("Could not strip prefix"))?;
                        rv.push(s.to_string());
                    } else {
                        warn!("Returned object {:?} did not have a key", object);
                    }
                }
            } else {
                warn!("List request did not return any objects: {:?}", listing);
            }

            if let Some(cont) = listing.next_continuation_token {
                info!("Continuing list request with token {:?}", cont);
                req.continuation_token = Some(cont);
            } else {
                break;
            }
        }

        Ok(rv)
    }


    async fn read_bytes(&self, obj_name: ObjectName<'_>) -> IdxResult<Vec<u8>> {
        info!("Read bytes request for {:?}", obj_name.as_str());

        let key = self.make_key(obj_name);
        let req = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: key,
            ..GetObjectRequest::default()
        };

        let resp = self.client.get_object(req).await
                .map_err(IdxError::storage_error)?;

        if let Some(strm) = resp.body {
            let mut contents = Vec::new();
            strm.into_async_read().read_to_end(&mut contents).await?;

            Ok(contents)
        } else {
            Err(IdxError::storage_error_msg("Reading from S3 failed: no body returned"))
        }
    }


    async fn write_bytes<T>(&self, name: ObjectName<'_>, data: T) -> IdxResult<()>
        where
            T: AsRef<[u8]> + Unpin + Send
    {
        info!("Write bytes request for {:?}", name.as_str());

        let strm = ByteStream::from(data.as_ref().to_owned());
        let key = self.make_key(name);
        let req = PutObjectRequest {
            bucket: self.bucket.clone(),
            key: key,
            body: Some(strm),

            ..PutObjectRequest::default()
        };

        self.client.put_object(req).await
            .map_err(IdxError::storage_error)?;

        Ok(())
    }
}