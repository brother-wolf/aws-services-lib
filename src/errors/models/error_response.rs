use serde_derive::Serialize;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub message: String,
}

impl ErrorResponse {
    pub fn json(error_message: &str) -> String {
        serde_json::to_string(&ErrorResponse{ message: error_message.to_string()} ).unwrap()
    }
    pub fn print_json(error_message: &str) {
        println!("{}", ErrorResponse::json(error_message));
    }
}