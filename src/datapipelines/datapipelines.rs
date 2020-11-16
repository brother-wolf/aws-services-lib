use std::collections::HashMap;
use ::chrono::Utc;
use ::rusoto_datapipeline::DataPipelineClient;
use ::rusoto_datapipeline::{ListPipelinesInput, PipelineIdName, DataPipeline, PipelineDescription, DescribePipelinesInput, Field};
use ::rusoto_datapipeline::{QueryObjectsInput, DescribeObjectsInput};
use crate::datapipelines::models::pipeline::Pipeline;
use crate::datapipelines::models::pipeline_tasks::PipelineTasks;
use crate::datapipelines::models::pipeline_task_status::{PipelineTaskStatus, PipelineTaskStatus::*};
use crate::utilities::get_or_blank;
use tokio::runtime::Runtime;

async fn get_pipeline_id_names(data_pipeline_client: &DataPipelineClient) -> Vec<PipelineIdName> {
    let mut all_pipelines: Vec<PipelineIdName> = vec![];
    let mut _marker = Some("".to_string());
    while _marker.is_some() {
        let list_pipelines_output = data_pipeline_client.list_pipelines(ListPipelinesInput { marker: _marker.clone() }).await;
        match list_pipelines_output {
            Ok(pipelines) => {
                all_pipelines.append(&mut pipelines.pipeline_id_list.clone());
                _marker = pipelines.marker.clone();
            }
            Err(e) => {
                println!("Error listing pipelines {}", e);
                all_pipelines = vec![];
                _marker = None
            }
        }
    }
    all_pipelines
}

async fn get_pipeline_tasks(pipeline_id: String, client: &DataPipelineClient, allowed_statuses: &Vec<PipelineTaskStatus>) -> Vec<PipelineTasks> {
    let allowed_statuses_strs = allowed_statuses.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
    let query_objects_input: QueryObjectsInput = QueryObjectsInput {
        limit: None,
        marker: None,
        pipeline_id: pipeline_id.clone(),
        query: None,
        sphere: "ATTEMPT".to_string(),
    };

    let query_objects_response = client.query_objects(query_objects_input).await;
    let task_ids: Vec<String> = match query_objects_response {
        Ok(output) => {
            match output.ids {
                Some(task_ids) => task_ids,
                None => vec![],
            }
        }

        Err(_e) => vec![],
    };
    let describe_objects_input = DescribeObjectsInput {
        evaluate_expressions: None,
        marker: None,
        object_ids: task_ids,
        pipeline_id: pipeline_id.clone(),
    };
    let describe_objects_response = client.describe_objects(describe_objects_input).await;

    match describe_objects_response {
        Ok(output) => {
            output.pipeline_objects.iter().flat_map(|pipeline_object| {
                let fields = convert(&pipeline_object.fields);
                let task_status = match PipelineTaskStatus::value(&get_or_blank(&"@status".to_string(), &fields)) {
                    Some(ts) => ts.as_str(),
                    None => "",
                };

                if allowed_statuses_strs.contains(&task_status) || allowed_statuses.is_empty() {
                    Some(PipelineTasks {
                        pipeline_id: pipeline_id.clone(),
                        task_id: pipeline_object.id.clone(),
                        task_name: pipeline_object.name.clone(),
                        status: task_status.to_string(),
                        attempt_status: get_or_blank(&"attemptStatus".to_string(), &fields),
                    })
                } else { None }
            }).collect::<Vec<PipelineTasks>>()
        }
        Err(_e) => {
            println!("pipeline[{}]: {:?}", pipeline_id, _e);
            vec![]
        }
    }
}

async fn get_pipelines_descriptions(pipeline_ids: Vec<String>, data_pipeline_client: &DataPipelineClient) -> Vec<PipelineDescription> {
    let mut all_describe_pipelines_output: Vec<PipelineDescription> = vec![];
    let _max = pipeline_ids.len();
    let mut _start = 0;
    let mut _end = if _max < 25 { _max } else { 25 };
    while _start < _max {
        let subset_pipelines = pipeline_ids[_start.._end].to_vec();
        let describe_pipelines_output = data_pipeline_client.describe_pipelines(DescribePipelinesInput { pipeline_ids: subset_pipelines }).await;
        match describe_pipelines_output {
            Ok(output) => {
                all_describe_pipelines_output.append(&mut output.pipeline_description_list.clone());
                _start += 25;
                _end += 25;
                if _max < _end { _end = _max };
            }
            Err(e) => {
                println!("Error describing pipelines {}", e);
                all_describe_pipelines_output = vec![];
            }
        }
    }
    all_describe_pipelines_output
}

fn convert(fields: &Vec<Field>) -> HashMap<String, String> {
    let mut hashm: HashMap<String, String> = HashMap::new();
    for f in fields {
        if f.string_value.is_some() {
            hashm.insert(f.key.clone(), f.string_value.clone().unwrap());
        }
    }
    hashm
}

pub fn status(client: &DataPipelineClient, pipeline_name_filters: &Vec<String>, filter_operation: &str) -> Vec<Pipeline> {
    let mut rt = Runtime::new().unwrap();
    let allowed_status_query = vec![Running, WaitingOnDependencies, Creating, WaitingForRunner];
    let now = Utc::now();
    let pipeline_ids = rt.block_on(async {get_pipeline_id_names(&client).await}).iter().flat_map(|pin| &pin.id).map(|c| c.clone()).collect();
    let pipelines_status = rt.block_on(async {get_pipelines_descriptions(pipeline_ids, &client).await}).iter()
        .filter(|pipe_desc| if filter_operation == "include" {
            pipeline_name_filters.contains(&pipe_desc.name)
        } else {
            !pipeline_name_filters.contains(&pipe_desc.name) })
        .flat_map( |pipeline_desc| {
            let fields = convert(&pipeline_desc.fields);
            let tasks = rt.block_on(async {get_pipeline_tasks(
                pipeline_desc.pipeline_id.clone(),
                client,
                &allowed_status_query).await});
            Pipeline::create(tasks, fields, now)
        })
        .collect();

    pipelines_status
}
