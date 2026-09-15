use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_s3::{Client, primitives::ByteStream};
use dotenvy::dotenv;
use image::ImageFormat;
use lapin::options::BasicGetOptions;
use lapin::{
    Connection, ConnectionProperties,
    options::{BasicAckOptions, BasicNackOptions, BasicQosOptions, QueueDeclareOptions},
    types::{FieldTable, ShortString},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{env, io::Cursor, process};
use tokio::time::timeout;

#[derive(Clone)]
struct Config {
    rabbitmq_host: String,
    bucket: String,
    upload_prefix: String,
    processing_prefix: String,
    aws_config: SdkConfig,
    queue: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    content_type: String,
    image_key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = configure().await;
    println!("Started processing {}", &config.rabbitmq_host);
    let connection = timeout(
        Duration::from_secs(10),
        Connection::connect(&config.rabbitmq_host, ConnectionProperties::default()),
    )
    .await
    .map_err(|_| "RabbitMQ connection timed out")??;

    let channel = connection.create_channel().await?;
    println!("connected--");
    channel.basic_qos(1, BasicQosOptions::default()).await?;
    println!("connected");
    channel
        .queue_declare(
            ShortString::from(config.queue.clone()),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    println!("Image worker started");
    loop {
        match channel
            .basic_get("image_queue".into(), BasicGetOptions::default())
            .await?
        {
            Some(delivery) => {
                let message: Message = match serde_json::from_slice(&delivery.data) {
                    Ok(message) => message,
                    Err(error) => {
                        eprintln!("Invalid message: {error}");

                        delivery
                            .nack(BasicNackOptions {
                                requeue: false,
                                ..Default::default()
                            })
                            .await?;

                        continue;
                    }
                };

                match image_handler(&message, &config).await {
                    Ok(()) => {
                        delivery.ack(BasicAckOptions::default()).await?;

                        println!("Processed {}", message.image_key);
                    }

                    Err(error) => {
                        eprintln!("Failed to process {}: {error}", message.image_key);

                        delivery
                            .nack(BasicNackOptions {
                                requeue: true,
                                ..Default::default()
                            })
                            .await?;

                        return Err(error);
                    }
                }
            }

            None => {
                println!("Queue is empty, exiting");
                break;
            }
        }
    }
    Ok(())
}

fn get_env_or_fail(key: &str) -> String {
    match env::var(key) {
        Ok(value) if !value.is_empty() => value,

        _ => {
            eprintln!("{key} environment variable is not set");
            process::exit(1);
        }
    }
}

async fn configure() -> Config {
    dotenv().ok();

    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    Config {
        rabbitmq_host: get_env_or_fail("RABBITMQ_HOST"),
        bucket: get_env_or_fail("BUCKET"),
        upload_prefix: get_env_or_fail("UPLOAD_PREFIX"),
        processing_prefix: get_env_or_fail("PROCESSING_PREFIX"),
        aws_config,
        queue: get_env_or_fail("QUEUE"),
    }
}

async fn image_handler(
    message: &Message,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let format = match message.content_type.as_str() {
        "image/jpeg" => ImageFormat::Jpeg,
        "image/png" => ImageFormat::Png,

        content_type => {
            return Err(format!("Unsupported content type: {content_type}").into());
        }
    };

    let client = Client::new(&config.aws_config);

    let object = client
        .get_object()
        .bucket(&config.bucket)
        .key(&message.image_key)
        .send()
        .await?;

    let original_bytes = object.body.collect().await?.into_bytes();

    let image = image::load_from_memory_with_format(&original_bytes, format)?;

    let thumbnail = image.thumbnail(400, 400);

    let mut thumbnail_buffer = Cursor::new(Vec::new());

    thumbnail.write_to(&mut thumbnail_buffer, format)?;

    let thumbnail_bytes = thumbnail_buffer.into_inner();

    let relative_key = message
        .image_key
        .strip_prefix(&config.upload_prefix)
        .ok_or_else(|| {
            format!(
                "Image key '{}' does not start with upload prefix '{}'",
                message.image_key, config.upload_prefix
            )
        })?;

    let thumbnail_key = format!("{}/thumbnail{}", config.processing_prefix, relative_key);

    let original_key = format!("{}/original{}", config.processing_prefix, relative_key);

    client
        .put_object()
        .bucket(&config.bucket)
        .key(&thumbnail_key)
        .content_type(&message.content_type)
        .body(ByteStream::from(thumbnail_bytes))
        .send()
        .await?;

    client
        .put_object()
        .bucket(&config.bucket)
        .key(&original_key)
        .content_type(&message.content_type)
        .body(ByteStream::from(original_bytes))
        .send()
        .await?;

    println!("Processed {} -> {}", message.image_key, thumbnail_key);

    Ok(())
}
