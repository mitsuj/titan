use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("Starting Titan Ingestion Engine...");

    let broker = "localhost:9092";
    let topic = "market.ticks.raw";

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    println!("Connected to kafka at {broker}");

    let dummy_tick = r#"{"symbol": "RELIANCE", "price": 2850.45, "volume": 100}"#;
    let record = FutureRecord::to(topic).payload(dummy_tick).key("RELIANCE");

    println!("Publishing tick: {dummy_tick}");

    match producer.send(record, Duration::from_secs(0)).await {
        Ok(delivery) => println!(
            "Delivered successfully to partition {} at offset {}",
            delivery.partition, delivery.offset
        ),
        Err((e, _)) => println!("Error delivering message: {e:?}"),
    }
}
