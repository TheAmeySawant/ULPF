use arrow_json::ReaderBuilder;
use bytes::Bytes;
use chrono::Utc;
use object_store::{aws::AmazonS3Builder, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

const BATCH_SIZE_LIMIT: usize = 50_000;
const BATCH_TIME_LIMIT: u64 = 60;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting ULPF Parquet Worker...");

    let s3_endpoint = env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://ulpf-storage-lake:9000".to_string());
    let s3_user = env::var("MINIO_ROOT_USER").expect("MINIO_ROOT_USER must be set");
    let s3_pass = env::var("MINIO_ROOT_PASSWORD").expect("MINIO_ROOT_PASSWORD must be set");
    let s3_bucket = env::var("MINIO_BUCKET").expect("MINIO_BUCKET must be set");

    let s3 = AmazonS3Builder::new()
        .with_endpoint(s3_endpoint)
        .with_access_key_id(s3_user)
        .with_secret_access_key(s3_pass)
        .with_bucket_name(&s3_bucket)
        .with_region("us-east-1")
        .with_allow_http(true)
        .build()?;
    let store: Arc<dyn ObjectStore> = Arc::new(s3);

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "ulpf-streaming-bus:9092".to_string());
    let topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| "ocsf-events".to_string());

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "ulpf-parquet-writer-group")
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[&topic])?;
    println!("Connected to Redpanda topic '{}', buffering batches...", topic);

    let mut batch: Vec<serde_json::Value> = Vec::with_capacity(BATCH_SIZE_LIMIT);
    let mut interval = time::interval(Duration::from_secs(BATCH_TIME_LIMIT));

    loop {
        tokio::select! {
            msg_result = consumer.recv() => {
                match msg_result {
                    Ok(msg) => {
                        if let Some(payload) = msg.payload() {
                            if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(payload) {
                                batch.push(json_val);
                            }
                        }
                        
                        if batch.len() >= BATCH_SIZE_LIMIT {
                            flush_to_parquet(&mut batch, &store, &s3_bucket).await?;
                            consumer.commit_message(&msg, CommitMode::Async)?;
                        }
                    }
                    Err(e) => eprintln!("Kafka error: {}", e),
                }
            }
            _ = interval.tick() => {
                if !batch.is_empty() {
                    println!("Time limit reached. Flushing {} records.", batch.len());
                    flush_to_parquet(&mut batch, &store, &s3_bucket).await?;
                }
            }
        }
    }
}

async fn flush_to_parquet(
    batch: &mut Vec<serde_json::Value>,
    store: &Arc<dyn ObjectStore>,
    bucket: &str
) -> Result<(), Box<dyn std::error::Error>> {
    if batch.is_empty() { return Ok(()); }

    let schema = arrow_json::reader::infer_json_schema_from_iterator(batch.iter().map(|v| Ok(v.clone())))?;
    let schema_ref = Arc::new(schema);

    let mut decoder = ReaderBuilder::new(schema_ref.clone()).build_decoder()?;
    decoder.serialize(batch)?;
    let record_batch = decoder.flush()?.expect("Failed to flush decoder");

    let mut parquet_bytes = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(&mut parquet_bytes, schema_ref, Some(props))?;
    writer.write(&record_batch)?;
    writer.close()?;

    let partition_date = Utc::now().format("%Y-%m-%d");
    let file_name = format!("ocsf_parquet/date={}/{}.parquet", partition_date, uuid::Uuid::new_v4());
    let path = object_store::path::Path::from(file_name.clone());

    store.put(&path, Bytes::from(parquet_bytes).into()).await?;
    
    println!("Successfully uploaded {} rows to s3://{}/{}", batch.len(), bucket, file_name);
    batch.clear();
    Ok(())
}
