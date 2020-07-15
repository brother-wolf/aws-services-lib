use std::collections::HashMap;

pub fn get_or_blank(field: &str, fields: &HashMap<String, String>) -> String {
    match fields.get(field) {
        Some(value) => value.clone(),
        _ => "".to_string(),
    }
}

#[test]
fn get_or_blank_value_returned_when_contains_field_name() {
    let fields = rusty_toolbox::hashmap![
        "a".to_string() => "b".to_string(),
        "c".to_string() => "d".to_string(),
        "e".to_string() => "f".to_string()
    ];

    assert_eq!(get_or_blank("a", &fields), "b".to_string());
    assert_eq!(get_or_blank("c", &fields), "d".to_string());
    assert_eq!(get_or_blank("e", &fields), "f".to_string());
}

#[test]
fn get_or_blank_empty_string_returned_when_field_name_not_there() {
    let fields = rusty_toolbox::hashmap![
        "a".to_string() => "b".to_string(),
        "c".to_string() => "d".to_string(),
        "e".to_string() => "f".to_string()
    ];
    assert_eq!(get_or_blank("r", &fields), "".to_string());
}