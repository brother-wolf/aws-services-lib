use serde_derive::Serialize;

#[derive(Debug, Serialize)]
pub struct S3ListObject {
    last_modified: String,
    size: i64,
    key: String,
}

impl S3ListObject {
    pub fn from(o: &rusoto_s3::Object) -> S3ListObject {
        S3ListObject {
            last_modified: o.last_modified.as_ref().unwrap().to_string(),
            size: o.size.as_ref().unwrap().to_owned(),
            key: o.key.as_ref().unwrap().to_string()
        }
    }
}