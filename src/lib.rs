//! `ChannelMap` is a `DashMap` wrapper over flume asynchronous/synchronous channels

use dashmap::{
    DashMap,
    mapref::entry::Entry,
};
use flume::{bounded, Receiver, Sender, TrySendError};
use std::sync::Arc;
use thiserror::Error;

pub use flume;

#[non_exhaustive]
#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    /// This channel does not exist.
    #[error("Channel does not exist")]
    Nonexistent,
    /// The channel is full.
    #[error("Channel is full")]
    Full,
    /// The receiver disconnected from the channel.
    #[error("Channel disconnected")]
    Disconnected,
    /// This channel already exists.
    #[error("Channel already exists")]
    AlreadyExists(String),
}

/// A concurrent map with asynchronous channels
#[derive(Debug, Clone)]
pub struct ChannelMap<T> {
    channels: Arc<DashMap<String, Sender<T>>>,
    buffer: usize,
}

impl<T> ChannelMap<T> {
    /// Creates a new [`ChannelMap`] with a default buffer size of 100
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_buffer(100)
    }

    /// Creates a new [`ChannelMap`] with the specified default buffer size
    #[must_use]
    pub fn new_with_buffer(buffer: usize) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            buffer,
        }
    }

    /// Adds a channel to [`ChannelMap`] with the default buffer size.
    /// This will usually be set to 100 if the user doesn't specify.
    ///
    /// # Errors
    ///
    /// If the channel already exists inside [`ChannelMap`], this
    /// function will return an error.
    pub fn add(&self, name: &str) -> Result<Receiver<T>, Error> {
        self.add_with_buffer(name, self.buffer)
    }

    /// Adds a channel to [`ChannelMap`] with a specified buffer
    ///
    /// # Errors
    ///
    /// If the channel already exists inside [`ChannelMap`], this
    /// function will return an error.
    pub fn add_with_buffer(&self, name: &str, buffer: usize) -> Result<Receiver<T>, Error> {
        let (sender, receiver) = bounded(buffer);
        match self.channels.entry(name.to_owned()) {
            Entry::Occupied(_) => return Err(Error::AlreadyExists(name.to_owned())),
            Entry::Vacant(entry) => entry.insert(sender),
        };
        Ok(receiver)
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

    /// Returns `true` if the channel was removed from [`ChannelMap`], otherwise
    /// `false`
    #[allow(clippy::must_use_candidate)]
    pub fn remove(&self, name: &str) -> bool {
        self.channels.remove(name).is_some()
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
    /// this method will remove the faulty channel and return
    /// an error.
    pub fn send(&self, name: &str, msg: T) -> Result<(), Error> {
        let tx = self.get(name).ok_or(Error::Nonexistent)?;
        if tx.send(msg).is_err() {
            self.remove(name);
            return Err(Error::Disconnected);
        }
        Ok(())
    }

    /// Convenience method allowing to asynchronously send messages
    ///
    /// # Errors
    ///
    /// If there was an error sending a message in the channel,
    /// the method will remove the faulty channel and return
    /// an error.
    pub async fn send_async(&self, name: &str, msg: T) -> Result<(), Error> {
        let tx = self.get(name).ok_or(Error::Nonexistent)?;
        if tx.send_async(msg).await.is_err() {
            self.remove(name);
            return Err(Error::Disconnected);
        }
        Ok(())
    }

    /// Convenience method allowing to send messages without awaiting
    /// 
    /// # Errors
    /// 
    /// If the channel is full, the method will return an error. If
    /// the channel was disconnected, the method will remove the faulty
    /// channel and return an error.
    pub fn try_send(&self, name: &str, msg: T) -> Result<(), Error> {
        let tx = self.get(name).ok_or(Error::Nonexistent)?;
        match tx.try_send(msg) {
            Err(TrySendError::Full(_)) => Err(Error::Full),
            Err(TrySendError::Disconnected(_)) => {
                self.remove(name);
                Err(Error::Disconnected)
            },
            Ok(()) => Ok(())
        }
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
        let receiver = channelmap.add("foo").unwrap();
        assert!(channelmap.contains("foo"));
        channelmap.send("foo", "bar").unwrap();
        assert_eq!(receiver.recv(), Ok("bar"));
    }

    #[tokio::test]
    async fn send_channel_async() {
        let channelmap = ChannelMap::new();
        let receiver = channelmap.add("foo").unwrap();
        channelmap.send_async("foo", "bar").await.unwrap();
        assert_eq!(receiver.recv_async().await, Ok("bar"));
    }

    #[test]
    fn remove_channel() {
        let channelmap: ChannelMap<&str> = ChannelMap::new();
        let _receiver = channelmap.add("foo").unwrap();
        channelmap.remove("foo");
        assert_eq!(channelmap.send("foo", "bar"), Err(Error::Nonexistent));
    }

    #[test]
    fn send_channel_threads() {
        let channelmap = ChannelMap::new();
        let channelmap_clone = channelmap.clone();
        let receiver: Receiver<&str> = channelmap_clone.add("foo").unwrap();
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
        let channelmap: ChannelMap<&str> = ChannelMap::new();
        assert!(channelmap.is_empty());
        let _receiver = channelmap.add("foo").unwrap();
        assert_eq!(channelmap.len(), 1);
        channelmap.remove("foo");
        assert!(channelmap.is_empty());
        assert_eq!(channelmap.len(), 0);
    }

    #[test]
    fn already_exists() {
        let channelmap: ChannelMap<&str> = ChannelMap::new();
        let _receiver = channelmap.add("foo").unwrap();
        assert_eq!(channelmap.add("foo").unwrap_err(), Error::AlreadyExists("foo".to_owned()));
    }

    #[test]
    fn dropped_rx() {
        let channelmap = ChannelMap::new();
        {
            let _receiver = channelmap.add("foo").unwrap();
        }
        assert_eq!(channelmap.send("foo", "bar").unwrap_err(), Error::Disconnected);
    }
}
