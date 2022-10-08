mod parq;

use deltalake::{
    action::Protocol, DeltaTableBuilder, DeltaTableError, DeltaTableMetaData, Schema,
    SchemaDataType, SchemaField,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::Path;
use walkdir::WalkDir;

#[tokio::main]
async fn main() {
    let p = Path::new("./data/table_init");
    std::fs::create_dir_all(p).unwrap_or_default();
    let p_file = p.join("simple.parquet");
    std::fs::remove_file(p_file.clone()).unwrap_or_default();
    parq::write_sample_parquet(p_file.to_str().unwrap());

    let d = Path::new("./data/tables");
    std::fs::create_dir_all(d).unwrap_or_default();
    let d_file = d.join("users");
    std::fs::remove_dir_all(d_file.clone()).unwrap_or_default();
    let res = write_delta_table(d_file.to_str().unwrap()).await;
    if let Err(res) = res {
        println!("{:?}", res);
    }
    for file in WalkDir::new("./data")
        .into_iter()
        .filter_map(|file| file.ok())
    {
        println!("{}", file.path().display());
    }
}

async fn write_delta_table(table_uri: &str) -> Result<(), DeltaTableError> {
    // Setup
    let table_schema = Schema::new(vec![
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
    ]);

    let delta_md = DeltaTableMetaData::new(
        Some("Users".to_string()),
        Some("This table is made to test the create function for a DeltaTable".to_string()),
        None,
        table_schema,
        vec!["account_id".to_string()],
        HashMap::new(),
    );

    let protocol = Protocol {
        min_reader_version: 1,
        min_writer_version: 1,
    };

    std::fs::create_dir(Path::new(table_uri)).unwrap();

    let mut dt = DeltaTableBuilder::from_uri(table_uri).build().unwrap();

    let mut commit_info = Map::<String, Value>::new();
    commit_info.insert(
        "operation".to_string(),
        serde_json::Value::String("CREATE TABLE".to_string()),
    );
    commit_info.insert(
        "users".to_string(),
        serde_json::Value::String("test user".to_string()),
    );
    // Action
    dt.create(delta_md.clone(), protocol.clone(), Some(commit_info), None)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_creates_delta_table() {
        let d = "./data/tables/testing";
        std::fs::remove_dir_all(d).unwrap_or_default();
        write_delta_table(d)
            .await
            .expect("Failed to create deltatable");
    }
}
