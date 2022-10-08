mod parq;

use deltalake::action::Action;
use deltalake::table_state::DeltaTableState;
use deltalake::{
    action, action::Protocol, DeltaTableBuilder, DeltaTableError, DeltaTableMetaData, Schema,
    SchemaDataType, SchemaField,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::{fs, path::Path};

#[tokio::main]
async fn main() {
    let p = "./data/table_init/simple.parquet";
    std::fs::remove_file(p).unwrap_or_default();
    parq::write_sample_parquet(p);

    let d = "./data/tables/users";
    std::fs::remove_dir_all(d).unwrap_or_default();
    let res = write_delta_table(d).await;
    if let Err(res) = res {
        println!("{:?}", res);
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
    // dt.create(delta_md.clone(), protocol.clone(), Some(commit_info), None).await
    let meta = action::MetaData::try_from(delta_md)?;

    // delta-rs commit info will include the delta-rs version and timestamp as of now
    commit_info.insert("delta-rs".to_string(), Value::String("0.4.1".to_string()));
    commit_info.insert(
        "timestamp".to_string(),
        Value::Number(serde_json::Number::from(
            chrono::Utc::now().timestamp_millis(),
        )),
    );

    let actions = vec![
        Action::commitInfo(commit_info),
        Action::protocol(protocol),
        Action::metaData(meta),
    ];

    let mut transaction = dt.create_transaction(None);
    transaction.add_actions(actions.clone());

    let prepared_commit = transaction.prepare_commit(None, None).await?;
    println!("{:?}", prepared_commit);

    let committed_version = dt.try_commit_transaction(&prepared_commit, 0).await?;
    println!("{:?}", committed_version);

    let new_state = DeltaTableState::from_commit(&dt, committed_version).await?;
    dt.state.merge(
        new_state,
        dt.config.require_tombstones,
        dt.config.require_files,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_creates_delta_table() {
        let d = "/mnt/c/Users/Req/CLionProjects/delta-trial/data/tables/testing";
        std::fs::remove_dir_all(d).unwrap_or_default();
        write_delta_table(d)
            .await
            .expect("Failed to create deltatable");
    }
}
