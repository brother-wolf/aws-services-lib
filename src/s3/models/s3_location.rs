use regex::Regex;

#[derive(Clone,Debug,PartialEq)]
pub struct S3Location {
    pub bucket: String,
    pub key: String,
}

impl S3Location {
    pub fn from(path: &str) -> Result<S3Location, String> {
        let re = Regex::new(r"s3[an]?://(?P<bucket>[^/]+)/(?P<key>.*)").unwrap();
        let results = re.captures_iter(path)
            .map(|cap| S3Location { bucket: cap[1].to_string(), key: cap[2].to_string() })
            .collect::<Vec::<S3Location>>();
        match results.len() {
            1 => Ok(results[0].clone()),
            0 => Err("No results!".to_string()),
            _ => Err("Too many results!".to_string()),
        }
    }
}

#[test]
fn test_valid_s3_locations() {
    let test_cases = vec![
        ("this-is-the-bucket", "here-is/the-key", "s3://this-is-the-bucket/here-is/the-key"),
        ("this-is-the-bucket", "here-is/the-key", "s3a://this-is-the-bucket/here-is/the-key"),
        ("this-is-the-bucket", "here-is/the-key", "s3n://this-is-the-bucket/here-is/the-key"),
        ("this_is_the_bucket", "here_is/the_key", "s3://this_is_the_bucket/here_is/the_key"),
        ("this_is_the_bucket", "", "s3://this_is_the_bucket/"),
    ];
    test_cases.iter().for_each(|(b, k, p )| {
        let actual= S3Location::from(p);
        let expected = S3Location { bucket: b.to_string(), key: k.to_string() };
        assert!(actual.is_ok());
        assert_eq!(expected, actual.unwrap());
    });
}

#[test]
fn test_invalid_s3_locations() {
    let test_cases = vec![
        "s://this-is-the-bucket/",
        "s3://this-is-the-bucket",
        "s3d://this-is-the-bucket/here-is/the-key",
    ];
    test_cases.iter().for_each(|p| {
        let actual= S3Location::from(p);
        assert!(actual.is_err());
    });
}