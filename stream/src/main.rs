mod stream;
mod unixpub;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Stream application started");
    
    // Run the gRPC streaming service
    stream::run_grpc().await?;
    
    Ok(())
}

