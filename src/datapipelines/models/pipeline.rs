use std::collections::HashMap;
use ::serde_derive::Serialize;
use chrono::{DateTime,Utc,TimeZone};
use crate::datapipelines::models::pipeline_task_status::PipelineTaskStatus;
use crate::datapipelines::models::pipeline_tasks::PipelineTasks;
use crate::utilities::get_or_blank;

#[derive(Serialize, Debug, Clone)]
pub struct Pipeline {
    pub id: String,
    pub name: String,
    pub account_id: String,
    pub health_status: String,
    pub pipeline_state: String,
    pub latest_run_time: Option<DateTime<Utc>>,
    pub next_run_time: Option<DateTime<Utc>>,
    pub scheduled_period: String,
    pub since_last_run_time: Option<String>,
    pub tasks: Vec<PipelineTasks>,
}

impl Pipeline {
    pub fn create(tasks: Vec<PipelineTasks>, fields: HashMap<String, String>, query_run_time: DateTime<Utc>) -> Option<Pipeline> {
        let pipeline_state = get_or_blank(&"@pipelineState".to_string(), &fields);
        let latest_run_time = get_or_blank(&"@latestRunTime".to_string(), &fields);
        let since_last_run_time = if pipeline_state == "SCHEDULED" { seconds_ago(&query_run_time, &latest_run_time) } else { None };

        let pipeline = Pipeline {
            id: get_or_blank(&"@id".to_string(), &fields),
            name: get_or_blank(&"name".to_string(), &fields),
            account_id: get_or_blank(&"@accountId".to_string(), &fields),
            health_status: get_or_blank(&"@healthStatus".to_string(), &fields),
            pipeline_state,
            latest_run_time: convert_to_date_time(&latest_run_time),
            next_run_time: convert_to_date_time(&get_or_blank(&"@nextRunTime".to_string(), &fields)),
            scheduled_period: get_or_blank(&"@scheduledPeriod".to_string(), &fields),
            since_last_run_time,
            tasks,
        };

        if pipeline.id.is_empty() ||
            pipeline.name.is_empty() ||
            pipeline.health_status.is_empty() {
            None
        } else {
            Some(pipeline)
        }
    }

    pub fn is_building(&self) -> bool {
        self.tasks.iter()
        .filter(|task| PipelineTaskStatus::is_building(&task.status)).fold(0, |acc, _| acc + 1) != 0
    }


    pub fn is_healthy(&self) -> bool {
        self.health_status == "HEALTHY"
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

pub fn seconds_ago(now: &DateTime<Utc>, ago: &str) -> Option<String> {
    match Utc.datetime_from_str(ago, &"%Y-%m-%dT%H:%M:%S") {
        Ok(data_time_ago) => Some((now.timestamp() - data_time_ago.timestamp()).to_string()),
        Err(_e) => None,
    }
}

fn convert_to_date_time(timestamp: &str) -> Option<DateTime<Utc>> {
    match Utc.datetime_from_str(&timestamp, "%Y-%m-%dT%H:%M:%S") {
        Ok(date_time) => Some(date_time),
        Err(_e) => None,
    }
}

#[test]
fn pipeline_status_should_serialize() {

    let expected = "{\"id\":\"df-0977100BVBIK29Y9RF6\",\"name\":\"Scopus Author Profile Backfill Pipeline\",\"account_id\":\"242194143705\",\"health_status\":\"HEALTHY\",\"pipeline_state\":\"FINISHED\",\"latest_run_time\":\"2017-08-31T14:58:04Z\",\"next_run_time\":\"2017-08-31T14:58:04Z\",\"scheduled_period\":\"24 hours\",\"since_last_run_time\":\"48448299\",\"tasks\":[]}";

    let actual = Pipeline {
        id: "df-0977100BVBIK29Y9RF6".to_string(),
        name: "Scopus Author Profile Backfill Pipeline".to_string(),
        account_id: "242194143705".to_string(),
        health_status: "HEALTHY".to_string(),
        pipeline_state: "FINISHED".to_string(),
        latest_run_time: convert_to_date_time(&"2017-08-31T14:58:04"),
        next_run_time: convert_to_date_time(&"2017-08-31T14:58:04"),
        scheduled_period: "24 hours".to_string(),
        since_last_run_time: Some("48448299".to_string()),
        tasks: vec![],
    };

    assert_eq!(expected, actual.to_json());
}

#[test]
fn healthy_pipeline_is_identified_correctly() {
    let healthy_pipeline = Pipeline {
        id: "df-0977100BVBIK29Y9RF6".to_string(),
        name: "Scopus Author Profile Backfill Pipeline".to_string(),
        account_id: "242194143705".to_string(),
        health_status: "HEALTHY".to_string(),
        pipeline_state: "FINISHED".to_string(),
        latest_run_time: convert_to_date_time(&"2017-08-31T14:58:04"),
        next_run_time: convert_to_date_time(&"2017-08-31T14:58:04"),
        scheduled_period: "24 hours".to_string(),
        since_last_run_time: Some("48448299".to_string()),
        tasks: vec![],
    };

    assert_eq!(true, healthy_pipeline.is_healthy());
}

#[test]
fn broken_pipeline_is_identified_correctly() {
    let broken_pipeline = Pipeline {
        id: "df-0977100BVBIK29Y9RF6".to_string(),
        name: "Scopus Author Profile Backfill Pipeline".to_string(),
        account_id: "242194143705".to_string(),
        health_status: "ERROR".to_string(),
        pipeline_state: "FINISHED".to_string(),
        latest_run_time: convert_to_date_time(&"2017-08-31T14:58:04"),
        next_run_time: convert_to_date_time(&"2017-08-31T14:58:04"),
        scheduled_period: "24 hours".to_string(),
        since_last_run_time: Some("48448299".to_string()),
        tasks: vec![],
    };

    assert_eq!(false, broken_pipeline.is_healthy());
}

#[test]
fn building_healthy_pipeline_is_identified_correctly() {
    let building_healthy_pipeline = Pipeline {
        id: "df-0977100BVBIK29Y9RF6".to_string(),
        name: "Scopus Author Profile Backfill Pipeline".to_string(),
        account_id: "242194143705".to_string(),
        health_status: "HEALTHY".to_string(),
        pipeline_state: "FINISHED".to_string(),
        latest_run_time: convert_to_date_time(&"2017-08-31T14:58:04"),
        next_run_time: convert_to_date_time(&"2017-08-31T14:58:04"),
        scheduled_period: "24 hours".to_string(),
        since_last_run_time: Some("48448299".to_string()),
        tasks: vec![],
    };

    assert_eq!(false, building_healthy_pipeline.is_building());
}

#[test]
fn seconds_ago_should_be_positive_for_historic_time() {
    let now = convert_to_date_time("2012-02-14T09:00:00");
    let actual: Option<String> = seconds_ago(&now.unwrap(), "2012-02-13T07:30:00");
    assert!(actual.is_some());
    assert_eq!("91800", actual.unwrap());
}

#[test]
fn seconds_ago_should_be_negative_for_future_time() {
    let now = convert_to_date_time("2012-02-14T09:00:00");
    let actual: Option<String> = seconds_ago(&now.unwrap(), "2012-02-15T07:30:00");
    assert!(actual.is_some());
    assert_eq!("-81000", actual.unwrap());
}

#[test]
fn seconds_ago_should_be_zero_for_now_time() {
    let now = convert_to_date_time("2012-02-14T09:00:00");
    let actual: Option<String> = seconds_ago(&now.unwrap(), "2012-02-14T09:00:00");
    assert!(actual.is_some());
    assert_eq!("0", actual.unwrap());
}

#[test]
fn convert_to_date_time_validates_and_converts_datetime_string() {
    assert_eq!(convert_to_date_time("2010-10-10T10:10:10").unwrap(), Utc.datetime_from_str("2010-10-10T10:10:10", "%Y-%m-%dT%H:%M:%S").unwrap());
    assert_eq!(convert_to_date_time("2020-12-12T12:12:12").unwrap(), Utc.datetime_from_str("2020-12-12T12:12:12", "%Y-%m-%dT%H:%M:%S").unwrap());
}

#[test]
fn convert_to_date_time_invalidates_incorrect_datetime_string() {
    assert!(convert_to_date_time("2010-10-10T10").is_none());
    assert!(convert_to_date_time("").is_none());
    assert!(convert_to_date_time("2010-10-10T10:1010").is_none());
}