use crate::{MessageEnum, MessageTrait};
use crossbeam::channel::{Receiver, Sender};
use std::mem::replace;
use tokio::time::{Duration, interval};

pub struct Transmitter {
    rx: Receiver<MessageEnum>,
    tx: Sender<Vec<u8>>,
}

impl Transmitter {
    const MAX_BYTES: usize = 4096;

    pub fn new(rx: Receiver<MessageEnum>, tx: Sender<Vec<u8>>) -> Self {
        Self { rx, tx }
    }

    pub async fn start(&mut self) {
        let mut ticker = interval(Duration::from_millis(10));
        let mut buffer = Vec::new();

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    while let Ok(message) = self.rx.try_recv() {
                        let message_bytes = message.to_bytes();
                        let message_len = message_bytes.len();

                        if buffer.len() + message_len <= Self::MAX_BYTES {
                            buffer.extend_from_slice(&message_bytes);
                        } else {
                            break;
                        }
                    }

                    if !buffer.is_empty() {
                        let _ = self.tx.send(replace(&mut buffer, Vec::new()));
                    }
                }
            }

            tokio::task::yield_now().await;
        }
    }
}
