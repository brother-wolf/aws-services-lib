use ::serde_derive::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct PipelineTasks {
    pub pipeline_id: String,
    pub task_id: String,
    pub task_name: String,
    pub status: String,
    pub attempt_status: String,
}
