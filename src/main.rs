mod parq;
use chrono::prelude::*;

use deltalake::action::Add;
use deltalake::{
    action, action::Protocol, DeltaDataTypeLong, DeltaTable, DeltaTableBuilder, DeltaTableError,
    DeltaTableMetaData, Schema, SchemaDataType, SchemaField,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

#[tokio::main]
async fn main() {
    let d = Path::new("./data/tables");
    std::fs::create_dir_all(d).unwrap_or_default();
    let d_file = d.join("users");
    std::fs::remove_dir_all(d_file.clone()).unwrap_or_default();

    let delta_md = DeltaTableMetaData::new(
        Some("Users".to_string()),
        Some("This table is made to test the create function for a DeltaTable".to_string()),
        None,
        Schema::new(vec![
            SchemaField::new(
                "id".to_string(),
                SchemaDataType::primitive("integer".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "account_id".to_string(),
                SchemaDataType::primitive("integer".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "name".to_string(),
                SchemaDataType::primitive("string".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "created_at".to_string(),
                SchemaDataType::primitive("timestamp".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "updated_at".to_string(),
                SchemaDataType::primitive("timestamp".to_string()),
                true,
                HashMap::new(),
            ),
        ]),
        vec!["account_id".to_string()],
        HashMap::new(),
    );

    std::fs::create_dir(d_file.clone()).unwrap();

    let mut dt = DeltaTableBuilder::from_uri(d_file.clone().to_str().unwrap()).build().unwrap();

    let mut commit_info = Map::<String, Value>::new();
    commit_info.insert(
        "operation".to_string(),
        serde_json::Value::String("CREATE TABLE".to_string()),
    );
    commit_info.insert(
        "users".to_string(),
        serde_json::Value::String("test user".to_string()),
    );

    let add_files: Vec<action::Add> = vec![{
        let p = d_file.clone().join("account_id=3");
        std::fs::create_dir_all(p.clone()).unwrap_or_default();
        let p_file = p.join("00000000000000000000.parquet");
        std::fs::remove_file(p_file.clone()).unwrap_or_default();
        parq::write_sample_parquet(p_file.clone().to_str().unwrap());
        
        Add {
        path: p_file.clone().to_str().unwrap().to_string(),
        size: (fs::metadata(p_file.clone()).unwrap().len()) as DeltaDataTypeLong,
        partition_values: {
            let mut m = HashMap::new();
            m.insert("account_id".to_string(), Some("3".to_string()));
            m
        },
        partition_values_parsed: None,
        modification_time: Utc::now().timestamp_millis(),
        data_change: true,
        stats: None,
        stats_parsed: None,
        tags: None,
    }}];

    // Action
    dt.create(
        delta_md.clone(),
        Protocol {
            min_reader_version: 1,
            min_writer_version: 1,
        },
        Some(commit_info),
        Some(add_files),
    ).await.unwrap();

    for file in WalkDir::new("./data")
        .into_iter()
        .filter_map(|file| file.ok())
    {
        println!("{}", file.path().display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_creates_delta_table() {
        let d = "./data/tables/testing";
        std::fs::remove_dir_all(d).unwrap_or_default();
    }
}
