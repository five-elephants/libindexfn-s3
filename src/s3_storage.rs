use libindexfn::{AccessStorage,ObjectName,IdxResult,IdxError};
use rusoto_core::{Region,ByteStream};
use rusoto_s3::{S3,S3Client,GetObjectRequest,ListObjectsV2Request,PutObjectRequest};
use async_trait::async_trait;
use tokio::io::AsyncReadExt;

pub struct S3Storage {
    bucket: String,
    client: S3Client,
}


impl S3Storage {
    pub fn new(region: Region, bucket: impl Into<String>) -> Self {
        let client = S3Client::new(region);

        Self {
            bucket: bucket.into(),
            client,
        }
    }


    fn make_prefix(dir_name: ObjectName<'_>) -> Option<String> {
        let s = dir_name.as_str();
        if !s.is_empty() {
            Some(String::from(dir_name.as_str()) + "/")
        } else {
            None
        }
    }
}


#[async_trait]
impl AccessStorage for S3Storage {
    type ListIntoIter = Vec<String>;

    async fn list(&self, dir_name: ObjectName<'_>) -> IdxResult<Self::ListIntoIter> {
        let mut req = ListObjectsV2Request {
            bucket: self.bucket.clone(),
            prefix: Self::make_prefix(dir_name),
            ..ListObjectsV2Request::default()
        };

        let mut rv = Vec::new();
        loop {
            let listing = self.client.list_objects_v2(req.clone()).await
                .map_err(|_| IdxError::StorageError)?;
            if let Some(objects) = listing.contents {
                for object in objects {
                    if let Some(key) = object.key {
                        rv.push(key);
                    }
                }
            }

            if let Some(cont) = listing.next_continuation_token {
                req.continuation_token = Some(cont);
            } else {
                break;
            }
        }

        Ok(rv)
    }


    async fn read_bytes(&self, obj_name: ObjectName<'_>) -> IdxResult<Vec<u8>> {
        let req = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: obj_name.as_str().to_string(),
            ..GetObjectRequest::default()
        };

        if let Some(strm) = self.client.get_object(req).await
                .map_err(|_| IdxError::StorageError)?.body {
            let mut contents = Vec::new();
            strm.into_async_read().read_to_end(&mut contents).await?;

            Ok(contents)
        } else {
            Err(IdxError::StorageError)
        }
    }


    async fn write_bytes<T>(&self, name: ObjectName<'_>, data: T) -> IdxResult<()>
        where
            T: AsRef<[u8]> + Unpin + Send
    {
        let strm = ByteStream::from(data.as_ref().to_owned());
        let req = PutObjectRequest {
            bucket: self.bucket.clone(),
            key: name.as_str().to_string(),
            body: Some(strm),

            ..PutObjectRequest::default()
        };

        self.client.put_object(req).await
            .map_err(|_| IdxError::StorageError)?;

        Ok(())
    }
}