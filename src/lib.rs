//! `ChannelMap` is a `DashMap` wrapper over Tokio asynchronous channels

use dashmap::DashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender, channel, error::SendError};

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error<T> {
    #[error("Channel does not exist")]
    Nonexistent,
    #[error("Send Error: {0}")]
    Send(#[from] SendError<T>),
}

/// A concurrent map with asynchronous channels
#[derive(Debug, Clone)]
pub struct ChannelMap<T> {
    channels: Arc<DashMap<String, Sender<T>>>,
}

impl<T> ChannelMap<T> {
    /// Creates a new `ChannelMap`
    #[must_use]
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
        }
    }

    /// Adds a channel to the `ChannelMap` with a set buffer of 100
    #[must_use]
    pub fn add(&self, name: &str) -> Receiver<T> {
        self.add_with_buffer(name, 100)
    }

    /// Adds a channel to the `ChannelMap` with a specified buffer capacity
    #[must_use]
    pub fn add_with_buffer(&self, name: &str, buffer: usize) -> Receiver<T> {
        let (sender, receiver) = channel(buffer);
        self.channels.insert(name.to_owned(), sender);
        receiver
    }

    /// Returns the total number of channels inside of the `ChannelMap`
    #[must_use]
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// Returns `true` if the `ChannelMap` is empty, otherwise `false`
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// Returns an iterator over every element in the `ChannelMap`
    pub fn iter(&self) -> impl Iterator<Item = Sender<T>> {
        self.channels
            .iter()
            .map(|x| x.value().clone())
    }

    /// Removes a channel from the `ChannelMap` by its name
    pub fn remove(&self, name: &str) {
        self.channels.remove(name);
    }

    /// Sends a message through a channel inside the `ChannelMap` by its name
    /// 
    /// # Errors
    ///
    /// Will return 'Err' when one of two things happen. If the channel does not exist
    /// inside of the `DashMap`, it will return `channelmap::Error::Nonexistent`. If
    /// there was an error sending a message in the channel, it will return
    /// `channelmap::Error::Send`
    pub async fn send(&self, name: &str, msg: T) -> Result<(), Error<T>> {
        self.channels
            .get(name)
            .ok_or(Error::Nonexistent)?
            .send(msg)
            .await?;
        Ok(())
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

    #[tokio::test]
    async fn send_channel() {
        let channelmap = ChannelMap::new();
        let mut receiver = channelmap.add("foo");
        channelmap.send("foo", "bar").await.unwrap();
        assert_eq!(receiver.recv().await, Some("bar"));
    }

    #[tokio::test]
    async fn remove_channel() {
        let channelmap: ChannelMap<&str> = ChannelMap::new();
        let _receiver = channelmap.add("foo");
        channelmap.remove("foo");
        assert_eq!(channelmap.send("foo", "bar").await, Err(Error::Nonexistent));
    }

    #[tokio::test]
    async fn send_channel_threads() {
        let channelmap = ChannelMap::new();
        let channelmap_clone = channelmap.clone();
        let future = async move {
            let mut receiver: Receiver<&str> = channelmap_clone.add("foo");
            assert_eq!(receiver.recv().await, Some("bar"));
        };
        let other_future = async move {
            assert!(channelmap.send("foo", "bar").await.is_ok());
        };
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(future);
        tasks.spawn(other_future);
        tasks.join_all().await;
    }

    #[tokio::test]
    async fn test_length() {
        let channelmap = ChannelMap::new();
        assert!(channelmap.is_empty());
        let _receiver = channelmap.add("foo");
        channelmap.send("foo", "bar").await.unwrap();
        assert_eq!(channelmap.len(), 1);
    }
}
