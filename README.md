# ChannelMap

A DashMap wrapper over asynchronous tokio channels. Provides a convenient way to send messages over named channels.

## Example

```rust
use channelmap::ChannelMap;
use tokio::task::JoinSet;

#[tokio::main] // (or whatever executor you're using)
async fn main() {
    let channels = ChannelMap::new();
    let mut set = JoinSet::new();

    for i in 0..10 {
        let mut rx = channels.add(&i.to_string());
        set.spawn(async move {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg, "bar");
            println!("Channel {i} got message {msg}");
        });
    }

    for tx in channels.iter() {
        tx.send("bar").await.unwrap();
    }
    
    set.join_all().await;
}
```

## License

Licensed under MIT.