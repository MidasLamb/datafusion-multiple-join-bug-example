use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    arrow::{
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    prelude::{CsvReadOptions, NdJsonReadOptions, ParquetReadOptions, SessionContext},
};
use datafusion_remote_tables::provider::RemoteTable;

#[tokio::main]
async fn main() -> Result<()> {
    my_main().await
}

async fn my_main() -> Result<()> {
    let ctx = SessionContext::new();

    let read_from_file = true;

    if read_from_file {
        ctx.register_parquet("bag", "bag.parquet", ParquetReadOptions::default())
            .await?;
        ctx.register_parquet(
            "service_article",
            "service_article.parquet",
            ParquetReadOptions::default(),
        )
        .await?;
        ctx.register_parquet(
            "metadata",
            "metadata.parquet",
            ParquetReadOptions::default(),
        )
        .await?;
    } else {
        let conn_str = "mysql://root:password@10.6.0.6/twintag";
        let remote_table = RemoteTable::new(
            "bag".to_string(),
            conn_str.to_string(),
            SchemaRef::from(Schema::empty()),
        )
        .await?;

        ctx.register_table("bag", Arc::new(remote_table)).unwrap();

        let conn_str = "mysql://root:password@10.6.0.7/twintag";
        let remote_table = RemoteTable::new(
            "75620d04a8b7b34c70c68f7af68953af".to_string(),
            conn_str.to_string(),
            SchemaRef::from(Schema::empty()),
        )
        .await?;

        ctx.register_table("service_article", Arc::new(remote_table))
            .unwrap();

        let conn_str = "mysql://root:password@10.6.0.7/twintag";
        let remote_table = RemoteTable::new(
            "f4be58656cd5e54b9bffa1706c3c49ff".to_string(),
            conn_str.to_string(),
            SchemaRef::from(Schema::empty()),
        )
        .await?;

        ctx.register_table("metadata", Arc::new(remote_table))
            .unwrap();

        ctx.sql(r#"SELECT * FROM bag"#)
            .await
            .unwrap()
            .write_parquet("bag.parquet", None)
            .await
            .unwrap();
        ctx.sql(r#"SELECT * FROM service_article"#)
            .await
            .unwrap()
            .write_parquet("service_article.parquet", None)
            .await
            .unwrap();
        ctx.sql(r#"SELECT * FROM metadata"#)
            .await
            .unwrap()
            .write_parquet("metadata.parquet", None)
            .await
            .unwrap();
    }

    // This shows what I expect
    ctx.sql(
        r#"SELECT * FROM service_article
JOIN metadata ON
    metadata."776ff32c7d7bfb1c1a49d5c0d94e0db8" = service_article."82ad7656c6b6be56e9abfac772c89b4c"
JOIN bag ON
    bag.storage_qid = metadata."dataScope"
    "#,
    )
    .await?
    .show()
    .await
    .unwrap();

    // This suddenly creates wrong joins
    ctx.sql(
        r#"SELECT * FROM service_article
JOIN metadata ON
    metadata."776ff32c7d7bfb1c1a49d5c0d94e0db8" = service_article."82ad7656c6b6be56e9abfac772c89b4c"
JOIN bag ON
    bag.storage_qid = metadata."dataScope"
    WHERE bag.storage_qid = 'b936b63f271c21b4a00957b325cb9a22'
    "#,
    )
    .await?
    .show()
    .await
    .unwrap();


    println!("===== SECOND TEST =======");

    ctx.sql(
        r#"SELECT * FROM metadata
JOIN bag ON
    bag.storage_qid = metadata."dataScope"
    "#,
    )
    .await?
    .show()
    .await
    .unwrap();

    ctx.sql(
        r#"SELECT * FROM metadata
JOIN bag ON
    bag.storage_qid = metadata."dataScope"
    WHERE bag.storage_qid = 'b936b63f271c21b4a00957b325cb9a22'
    "#,
    )
    .await?
    .show()
    .await
    .unwrap();

    Ok(())
}
