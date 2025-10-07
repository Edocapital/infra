use tokio::net::UnixStream;
use tokio::io::{BufReader, AsyncBufReadExt};
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::collections::HashMap;
use std::sync::Arc;

/// Simple event structure for parsing JSON events from the stream
#[derive(Debug, Deserialize, Serialize)]
struct SimpleEvent {
    #[serde(flatten)]
    data: HashMap<String, serde_json::Value>,
}


/// Bot structure that handles stream processing
struct Bot {
    socket_path: String,
}

impl Bot {
    /// Create a new Bot instance
    fn new(socket_path: String) -> Self {
        Self { socket_path }
    }

    /// Handle stream processing with reader, readlines, and main event loop
    /// Processes events from the Unix socket and handles periodic reporting
    async fn handle_stream(
        &mut self,
        stream: UnixStream        
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reader = BufReader::new(stream);
        let mut lines = reader.lines();

        loop {
            tokio::select! {
                // Handle incoming events
                line_result = lines.next_line() => {
                    match line_result {
                        Ok(Some(line)) => {
                            if let Ok(event) = from_str::<SimpleEvent>(&line) {
                                println!("Received event: {:?}", event);
                                // Process the event here
                            } else {
                                eprintln!("Failed to parse event JSON: {}", line);
                            }
                        }
                        Ok(None) => {
                            println!("Disconnected from Unix socket");
                            break;
                        }
                        Err(e) => {
                            eprintln!("Error reading from socket: {}", e);
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Run the bot
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Bot connecting to socket: {}", self.socket_path);
        
        // Connect to the Unix socket
        let stream = UnixStream::connect(&self.socket_path).await?;
        println!("Connected to Unix socket");


        // Handle stream processing
        let stream_result = self.handle_stream(stream).await;

        if let Err(e) = stream_result {
            eprintln!("Stream processing error: {}", e);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Bot application started");
    
    // Socket path for connecting to the stream
    let socket_path = "/tmp/dexstream.sock";
    
    // Create and run the bot
    let mut bot = Bot::new(socket_path.to_string());
    bot.run().await?;
    
    Ok(())
}
