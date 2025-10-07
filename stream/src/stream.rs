
use std::env;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tokio::time::{interval, Duration};
use dotenv::dotenv;

use solana_streamer_sdk::{
    match_event,
    streaming::{
        event_parser::{
            protocols::{
                pumpfun::parser::PUMPFUN_PROGRAM_ID,
            },
            Protocol, UnifiedEvent,
        },
        grpc::ClientConfig,
        yellowstone_grpc::{AccountFilter, TransactionFilter},
        YellowstoneGrpc,
    },
};

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

/// Simple event wrapper for serialization
#[derive(serde::Serialize)]
struct EventWrapper {
    event_type: String,
    transaction_index: Option<u64>,
    timestamp: u64,
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

/// Publish a UnifiedEvent to the Unix socket
/// Creates a simple wrapper with basic event information
pub fn publish_unified_event(
    sender: &broadcast::Sender<String>,
    event: &Box<dyn UnifiedEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple wrapper with basic event information
    let wrapper = EventWrapper {
        event_type: format!("{:?}", event.event_type()),
        transaction_index: event.transaction_index(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    };
    
    // Serialize wrapper to JSON
    let json = serde_json::to_string(&wrapper)?;
    
    // Send to all subscribers
    let _ = sender.send(json);
    
    Ok(())
}

// Minimal gRPC streaming setup
pub async fn run_grpc() -> Result<(), Box<dyn std::error::Error>> {
    println!("Subscribing to Yellowstone gRPC events...");

    // Load environment variables from .env file
    dotenv().ok();

    // Get endpoint and auth token from environment variables
    let endpoint = env::var("GRPC_ENDPOINT")?;
    let auth_token = env::var("GRPC_AUTH_TOKEN").ok();
    
    // Get Unix socket path from environment variable
    let socket_path = env::var("UNIX_SOCKET_PATH")
        .unwrap_or_else(|_| "/tmp/dexstream.sock".to_string());

    println!("Attempting to connect to gRPC endpoint: {}", endpoint);
    println!("Auth token present: {}", auth_token.is_some());

    // Create gRPC client with basic config
    let client_config = ClientConfig::default();
    
    let grpc = match YellowstoneGrpc::new_with_config(
        endpoint.clone(),
        auth_token.clone(),
        client_config,
    ) {
        Ok(client) => client,
        Err(e) => {
            println!("Failed to create gRPC client: {}", e);
            return Err(e.into());
        }
    };

    println!("Successfully connected to gRPC endpoint: {}", endpoint);

    // Create event counter
    let event_count = Arc::new(AtomicU64::new(0));
    
    // Create Unix socket publisher
    let publisher = UnixPublisher::new(socket_path.clone());
    let event_sender = publisher.sender();
    
    // Start Unix socket publisher in background
    let _publisher_handle = {
        let publisher = publisher;
        tokio::spawn(async move {
            if let Err(e) = publisher.start().await {
                eprintln!("Unix socket publisher error: {}", e);
            }
        })
    };
    
    println!("Unix socket publisher started on: {}", socket_path);

    // Start event counter reporting task
    let event_count_clone = event_count.clone();
    let _counter_handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let count = event_count_clone.load(Ordering::Relaxed);
            println!("Events published in last 10s: {}", count);
            event_count_clone.store(0, Ordering::Relaxed);
        }
    });

    // Create event callback that publishes to Unix socket
    let callback = create_event_callback_with_publisher(event_sender, event_count);

    // Monitor PumpFun protocol only
    let protocols = vec![
        Protocol::PumpFun,
    ];

    println!("Protocols to monitor: {:?}", protocols);

    // Filter accounts - PumpFun only
    let account_include = vec![
        PUMPFUN_PROGRAM_ID.to_string(),        // Listen to pumpfun program ID
    ];

    let transaction_filter = TransactionFilter {
        account_include: account_include.clone(),
        account_exclude: vec![],
        account_required: vec![],
    };
    let account_filter = AccountFilter {
        account: vec![],
        owner: account_include.clone(),
        filters: vec![],
    };

    println!("Starting subscription...");

    // Subscribe to events
    match grpc
        .subscribe_events_immediate(
            protocols,
            None,
            vec![transaction_filter],
            vec![account_filter],
            None,
            None,
            callback,
        )
        .await
    {
        Ok(_) => println!("Subscription started successfully"),
        Err(e) => {
            println!("Subscription error: {:?}", e);
            return Err(e.into());
        }
    }

    // Keep the stream running - wait for Ctrl+C to stop
    println!("Stream is running... Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    println!("Received Ctrl+C, shutting down...");

    Ok(())
}

// Event callback that publishes to Unix socket and counts events
fn create_event_callback_with_publisher(
    event_sender: broadcast::Sender<String>,
    event_count: Arc<AtomicU64>
) -> impl Fn(Box<dyn UnifiedEvent>) + Clone {
    move |event: Box<dyn UnifiedEvent>| {
        // Increment event counter
        event_count.fetch_add(1, Ordering::Relaxed);
        
        // Publish event to Unix socket
        if let Err(e) = publish_unified_event(&event_sender, &event) {
            eprintln!("Failed to publish event: {}", e);
        }
        
        // Process different event types
        match_event!(event, {
            // Add specific event handling here as needed
        });
    }
}

// Minimal event callback that just logs events (kept for compatibility)
fn create_event_callback() -> impl Fn(Box<dyn UnifiedEvent>) + Clone {
    move |event: Box<dyn UnifiedEvent>| {
        // Basic event handling - just log for now
        println!("Received event: {:?}", event);
        
        // Process different event types
        match_event!(event, {
            // Add specific event handling here as needed
        });
    }
}

