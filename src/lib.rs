//! `ChannelMap` is a `DashMap` wrapper over Tokio asynchronous channels

use dashmap::DashMap;
use flume::{Receiver, SendError, Sender, bounded};
use std::sync::Arc;
use thiserror::Error;

pub use flume;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error<T> {
    /// This channel does not exist.
    #[error("Channel does not exist")]
    Nonexistent,
    /// The message failed to be sent through the channel,
    /// possibly due to the `Receiver` being dropped or
    /// closed.
    #[error("Send Error: {0}")]
    Send(#[from] SendError<T>),
}

/// A concurrent map with asynchronous channels
#[derive(Debug, Clone)]
pub struct ChannelMap<T> {
    channels: Arc<DashMap<String, Sender<T>>>,
}

impl<T> ChannelMap<T> {
    /// Creates a new [`ChannelMap`]
    #[must_use]
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
        }
    }

    /// Adds a channel to [`ChannelMap`] with a set buffer of 100
    #[must_use]
    pub fn add(&self, name: &str) -> Receiver<T> {
        self.add_with_buffer(name, 100)
    }

    /// Adds a channel to [`ChannelMap`] with a specified buffer capacity
    #[must_use]
    pub fn add_with_buffer(&self, name: &str, buffer: usize) -> Receiver<T> {
        let (sender, receiver) = bounded(buffer);
        self.channels.insert(name.to_owned(), sender);
        receiver
    }

    /// Returns the total number of channels inside of [`ChannelMap`]
    #[must_use]
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// Returns `true` if [`ChannelMap`] is empty, otherwise `false`
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    #[must_use]
    /// Returns `true` if [`ChannelMap`] contains the name, otherwise `false`
    pub fn contains(&self, name: &str) -> bool {
        self.channels.contains_key(name)
    }

    /// Returns an iterator over every sender in [`ChannelMap`]
    pub fn iter(&self) -> impl Iterator<Item = Sender<T>> {
        self.channels.iter().map(|x| x.value().clone())
    }

    /// Returns an iterator over every key (names) in [`ChannelMap`]
    pub fn keys(&self) -> impl Iterator<Item = String> {
        self.channels.iter().map(|x| x.key().clone())
    }

    /// Clears all channels from [`ChannelMap`]
    pub fn clear(&self) {
        self.channels.clear();
    }

    /// Removes a channel from [`ChannelMap`] by its name
    pub fn remove(&self, name: &str) {
        self.channels.remove(name);
    }

    #[must_use]
    /// Returns a named `Sender<T>`, or `None` if it doesn't exist
    pub fn get(&self, name: &str) -> Option<Sender<T>> {
        Some(self.channels.get(name)?.value().clone())
    }

    /// Convenience method allowing to send messages
    ///
    /// # Errors
    ///
    /// If there was an error sending a message in the channel,
    /// this function will return an error.
    pub fn send(&self, name: &str, msg: T) -> Result<(), Error<T>> {
        self.get(name).ok_or(Error::Nonexistent)?.send(msg)?;
        Ok(())
    }

    /// Convenience method allowing to asynchronously send messages
    ///
    /// # Errors
    ///
    /// If there was an error sending a message in the channel,
    /// the function will return an error.
    pub async fn send_async(&self, name: &str, msg: T) -> Result<(), Error<T>> {
        self.get(name)
            .ok_or(Error::Nonexistent)?
            .send_async(msg)
            .await?;
        Ok(())
    }

    #[must_use]
    /// Returns an [`Arc`] reference to the inner `DashMap`
    pub fn get_inner(&self) -> Arc<DashMap<String, Sender<T>>> {
        self.channels.clone()
    }
}

impl<T> Default for ChannelMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_channel() {
        let channelmap = ChannelMap::new();
        let receiver = channelmap.add("foo");
        assert!(channelmap.contains("foo"));
        channelmap.send("foo", "bar").unwrap();
        assert_eq!(receiver.recv(), Ok("bar"));
    }

    #[tokio::test]
    async fn send_channel_async() {
        let channelmap = ChannelMap::new();
        let receiver = channelmap.add("foo");
        channelmap.send_async("foo", "bar").await.unwrap();
        assert_eq!(receiver.recv_async().await, Ok("bar"));
    }

    #[test]
    fn remove_channel() {
        let channelmap: ChannelMap<&str> = ChannelMap::new();
        let _receiver = channelmap.add("foo");
        channelmap.remove("foo");
        assert_eq!(channelmap.send("foo", "bar"), Err(Error::Nonexistent));
    }

    #[test]
    fn send_channel_threads() {
        let channelmap = ChannelMap::new();
        let channelmap_clone = channelmap.clone();
        let receiver: Receiver<&str> = channelmap_clone.add("foo");
        let thread = std::thread::spawn(move || {
            assert_eq!(receiver.recv(), Ok("bar"));
        });
        std::thread::spawn(move || {
            assert!(channelmap.send("foo", "bar").is_ok());
        });
        assert!(thread.join().is_ok()); // exit only when receiver receives message
    }

    #[test]
    fn test_length() {
        let channelmap = ChannelMap::new();
        assert!(channelmap.is_empty());
        let _receiver = channelmap.add("foo");
        channelmap.send("foo", "bar").unwrap();
        assert_eq!(channelmap.len(), 1);
        channelmap.remove("foo");
        assert!(channelmap.is_empty());
        assert_eq!(channelmap.len(), 0);
    }
}
