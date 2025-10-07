use tokio::net::UnixListener;
use tokio::io::AsyncWriteExt;
use std::path::Path;
use tokio::sync::broadcast;

/// Simple Unix socket publisher that broadcasts any JSON-serializable event
pub struct UnixPublisher {
    socket_path: String,
    tx: broadcast::Sender<String>,
}

impl UnixPublisher {
    /// Create a new Unix socket publisher
    pub fn new(socket_path: String) -> Self {
        // Create a broadcast channel with buffer size of 1000 events
        let (tx, _rx) = broadcast::channel::<String>(1000);
        
        Self {
            socket_path,
            tx,
        }
    }

    /// Get a sender for publishing events
    pub fn sender(&self) -> broadcast::Sender<String> {
        self.tx.clone()
    }

    /// Start the Unix socket publisher server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Remove existing socket file if it exists
        if Path::new(&self.socket_path).exists() {
            std::fs::remove_file(&self.socket_path)?;
            println!("Removed existing socket: {}", self.socket_path);
        }

        // Bind to the Unix socket
        let listener = UnixListener::bind(&self.socket_path)?;
        println!("Unix socket publisher running on: {}", self.socket_path);

        // Accept connections and handle them
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let mut rx = self.tx.subscribe();
                    println!("New client connected");
                    
                    // Spawn a task to handle this client
                    tokio::spawn(async move {
                        let (_, mut writer) = stream.into_split();
                        
                        // Send events to this client
                        while let Ok(event_json) = rx.recv().await {
                            // Write the JSON event followed by newline
                            if writer.write_all(event_json.as_bytes()).await.is_err() {
                                break;
                            }
                            if writer.write_all(b"\n").await.is_err() {
                                break;
                            }
                            if writer.flush().await.is_err() {
                                break;
                            }
                        }
                        
                        println!("Client disconnected");
                    });
                }
                Err(e) => {
                    println!("Failed to accept connection: {}", e);
                    return Err(e.into());
                }
            }
        }
    }
}

/// Publish any event to the Unix socket
/// The event will be serialized to JSON and broadcast to all connected clients
pub fn publish_event<T: serde::Serialize>(
    sender: &broadcast::Sender<String>,
    event: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    // Serialize event to JSON
    let json = serde_json::to_string(event)?;
    
    // Send to all subscribers
    // Ignore errors - they just mean no subscribers are listening
    let _ = sender.send(json);
    
    Ok(())
}

