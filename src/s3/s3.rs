use tokio::runtime::Runtime;
use rusoto_s3::{ListObjectsV2Request, S3, S3Client};
use crate::s3::models::s3_list_object::S3ListObject;
use crate::errors::models::error_response::ErrorResponse;


pub fn ls(client: &S3Client, bucket: &str, prefix: &str) -> Result<Vec<S3ListObject>, String> {
    let mut rt  = Runtime::new().unwrap();

    rt.block_on(async {
        s3_list(&client, bucket, prefix).await
    })
}

async fn s3_list(client: &S3Client, bucket: &str, prefix: &str) -> Result<Vec<S3ListObject>, String> {
    fn build_s3_request(bucket: &str, prefix: &str, continuation_token: Option<String>) -> ListObjectsV2Request {
        ListObjectsV2Request {
            bucket: String::from(bucket),
            prefix: Some(String::from(prefix)),
            continuation_token,
            ..ListObjectsV2Request::default()
        }
    }

    async fn rec(acc: &mut Vec<S3ListObject>, next_continuation_token: Option<String>, client: &S3Client, bucket: &str, prefix: &str) -> Result<Option<String>, String> {
        match client.list_objects_v2(build_s3_request(bucket, prefix, next_continuation_token)).await {
            Ok(l) => {
                match &l.contents {
                    Some(c) => {
                        let mut x = c.iter().map(|i| S3ListObject::from(i)).collect();
                        acc.append(&mut x);
                        Ok(l.next_continuation_token)
                    },
                    None => Ok(None),
                }
            },
            Err(_e) => Err(ErrorResponse::json(_e.to_string().as_str())),
        }
    }

    let mut list: Vec<S3ListObject> = vec![];
    let mut next_continuation_token = None;
    let mut errors = None;
    loop {
        match rec(&mut list, next_continuation_token, client, bucket, prefix).await {
            Ok(token) => next_continuation_token = token,
            Err(_e) => {
                errors = Some(_e);
                break;
            }
        }
        if next_continuation_token.is_none() { break; }
    }
    match errors {
        Some(e) => Err(e),
        None => Ok(list)
    }
}