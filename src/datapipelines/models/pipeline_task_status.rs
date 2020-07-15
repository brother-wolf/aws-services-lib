#[derive(PartialEq,Debug)]
pub enum PipelineTaskStatus {
    Creating,
    Failed,
    Finished,
    Running,
    WaitingForRunner,
    WaitingOnDependencies,
}

impl PipelineTaskStatus {
    #[inline]
    fn building_states() -> Vec<&'static str> {
        vec![PipelineTaskStatus::Running.as_str(),
        PipelineTaskStatus::Creating.as_str(),
        PipelineTaskStatus::WaitingForRunner.as_str(),
        PipelineTaskStatus::WaitingOnDependencies.as_str()]
    }

    pub fn as_str(&self) -> &'static str {
        match *self {
            PipelineTaskStatus::Creating => "CREATING",
            PipelineTaskStatus::Failed => "FAILED",
            PipelineTaskStatus::Finished => "FINISHED",
            PipelineTaskStatus::Running => "RUNNING",
            PipelineTaskStatus::WaitingForRunner => "WAITING_FOR_RUNNER",
            PipelineTaskStatus::WaitingOnDependencies => "WAITING_ON_DEPENDENCIES",
        }
    }

    pub fn value(str_value: &str) -> Option<PipelineTaskStatus> {
        match str_value {
            "CREATING" => Some(PipelineTaskStatus::Creating),
            "FAILED" => Some(PipelineTaskStatus::Failed),
            "FINISHED" => Some(PipelineTaskStatus::Finished),
            "RUNNING" => Some(PipelineTaskStatus::Running),
            "WAITING_FOR_RUNNER" => Some(PipelineTaskStatus::WaitingForRunner),
            "WAITING_ON_DEPENDENCIES" => Some(PipelineTaskStatus::WaitingOnDependencies),
            _ => None,
        }
    }

    pub fn is_building(status: &str) -> bool {
        PipelineTaskStatus::building_states().contains(&status)
    }
}

#[test]
fn pipeline_task_status_can_convert_to_the_expected_string() {
    assert_eq!(PipelineTaskStatus::Creating.as_str(), "CREATING");
    assert_eq!(PipelineTaskStatus::Failed.as_str(), "FAILED");
}

#[test]
fn string_converts_to_pipeline_tsak_status() {
    assert_eq!(PipelineTaskStatus::value("CREATING").unwrap(), PipelineTaskStatus::Creating);
    assert_eq!(PipelineTaskStatus::value("FAILED").unwrap(), PipelineTaskStatus::Failed);
}

#[test]
fn status_that_is_building_identified_correctly() {
    assert!(PipelineTaskStatus::is_building(PipelineTaskStatus::Creating.as_str()));
    assert!(PipelineTaskStatus::is_building(PipelineTaskStatus::WaitingForRunner.as_str()));
    assert!(PipelineTaskStatus::is_building(PipelineTaskStatus::Running.as_str()));
    assert!(PipelineTaskStatus::is_building(PipelineTaskStatus::WaitingOnDependencies.as_str()));
}

#[test]
fn status_that_is_inactive_identified_correctly() {
    assert!(!PipelineTaskStatus::is_building(PipelineTaskStatus::Failed.as_str()));
    assert!(!PipelineTaskStatus::is_building(PipelineTaskStatus::Finished.as_str()));
}

