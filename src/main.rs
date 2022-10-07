use deltalake::{
    action::Protocol, DeltaTableBuilder, DeltaTableMetaData, Schema, SchemaDataType, SchemaField,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::{fs, path::Path, sync::Arc};

use parquet::{
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};

#[tokio::main]
async fn main() {
    write_sample_parquet("./data/table_init/simple.parquet");
    write_delta_table("./data/tables/users").await;

    // let mut table = deltalake::open_table("./tables/simple").await.unwrap();
    // let mut table = DeltaTableBuilder::from_uri("./tables/simple").without_files().without_tombstones().load().await?;
    // let mut tx = table.create_transaction(Some(deltalake::DeltaTransactionOptions::default()));
}

async fn write_delta_table(table_uri: &str) {
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
    let maybe_err = dt.create(delta_md.clone(), protocol.clone(), Some(commit_info), None)
        .await;

    match maybe_err {
        Err(error) => {
            println!("Error creating deltatable: {:?}", error)
        },
        Ok(()) => {unimplemented!();}
    }
}

fn write_sample_parquet(target: &str) {
    // Let's start our table as a simple parquet file
    let path = Path::new(target);

    let message_type = "
  message schema {
    REQUIRED INT32 id;
    REQUIRED INT32 account_id;
    REQUIRED BINARY name (UTF8);
    REQUIRED INT64 created_at (TIMESTAMP(MILLIS,true));
    OPTIONAL INT64 updated_at (TIMESTAMP(MILLIS,true));
  }
";
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();

    let mut rows: i64 = 0;
    let data = vec![
        (1, 3, "Albert", 1665031876786, 1665031876786),
        (2, 3, "Beth", 1665031876786, 1665031876786),
        (3, 7, "Carl", 1665031876786, 1665031876786),
        (4, 7, "Doug", 1665031876786, 1665031876786),
    ];

    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    for (id, account_id, name, created_at, updated_at) in data {
        let mut idx: i32 = 0;
        let mut row_group_writer = writer.next_row_group().unwrap();
        while let Some(mut writer) = row_group_writer.next_column().unwrap() {
            // ... write values to a column writer
            let column_idx: i32 = &idx % 5;
            match writer.untyped() {
                ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                    match column_idx {
                        0 => {
                            // ID column
                            let values = vec![id];
                            rows += typed.write_batch(&values[..], None, None).unwrap() as i64;
                        }
                        1 => {
                            // AccountID column
                            let values = vec![account_id];
                            rows += typed.write_batch(&values[..], None, None).unwrap() as i64;
                        }
                        _ => {
                            unimplemented!();
                        }
                    }
                }
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    if column_idx == 2 {
                        // Name column
                        let values = ByteArray::from(name);
                        rows += typed.write_batch(&[values], None, None).unwrap() as i64;
                    }
                }
                ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                    match column_idx {
                        3 => {
                            // Created_at column
                            let values = vec![created_at];
                            rows += typed.write_batch(&values[..], None, None).unwrap() as i64;
                        }
                        4 => {
                            // Updated_at column
                            let values = vec![updated_at];

                            // Nullable, so we need to provide "definition" levels
                            let def_levels = values
                                .iter()
                                .map(|x| if *x == 0 { 0 } else { 1 })
                                .collect::<Vec<i16>>();

                            rows += typed
                                .write_batch(&values[..], Some(&*def_levels), None)
                                .unwrap() as i64;
                        }
                        _ => {
                            unimplemented!();
                        }
                    }
                }
                _ => {
                    unimplemented!();
                }
            }
            writer.close().unwrap();
            idx += 1;
        }
        row_group_writer.close().unwrap();
    }
    writer.close().unwrap();

    println!("Wrote {}", rows);
}
