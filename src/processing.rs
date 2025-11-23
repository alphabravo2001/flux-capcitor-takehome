use crate::{MessageEnum, MessageTrait, MessageType};
use crossbeam::channel::{Receiver, Sender};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::time::{Duration, Instant};

const BATCH_INTERVAL_MS: u64 = 9;     // Slightly less than 10ms to account for processing time
const MAX_BATCH_BYTES: usize = 4096;


#[derive(Clone)]
struct PendingMessage {
    message: MessageEnum,
    size: usize,
}


pub struct Processing {
    rx: Receiver<MessageEnum>,
    tx: Sender<MessageEnum>,
    
    ready_queue: VecDeque<PendingMessage>,  // Messages ready to send (no parent or parent already sent)
    waiting_map: HashMap<Vec<u8>, Vec<PendingMessage>>,  // Messages waiting for their parent to be sent (parent_signature -> Vec<children>)
    sent_signatures: HashSet<Vec<u8>>,  // Signatures of messages that have been sent 
    recent_types: VecDeque<MessageType>,  // Recent type history for bonus prediction (last 3 types)
    current_dependency_length: u64,  // Track dependency chain length
}


impl Processing {
    pub fn new(rx: Receiver<MessageEnum>, tx: Sender<MessageEnum>) -> Self {
        Self {
            rx,
            tx,
            ready_queue: VecDeque::new(),
            waiting_map: HashMap::new(),
            sent_signatures: HashSet::new(),
            recent_types: VecDeque::with_capacity(3),
            current_dependency_length: 0,
        }
    }


    //Helper funciton to predict points based on recent type history and dependency chain length
    fn estimate_points(&self, message: &MessageEnum) -> u64 {
        let base_value = message.get_points_value() as u64;
        
        // Predict multiplier based on recent type history
        let mut multiplier = 4u64;
        
        if self.recent_types.len() >= 2 {
            let last_two: Vec<MessageType> = self.recent_types.iter().rev().take(2).cloned().collect();
            let msg_type = message.get_type();
            
            // Check if this would be 3rd same type in a row 
            if last_two.len() == 2 && last_two[0] == msg_type && last_two[1] == msg_type {
                multiplier /= 2; // Penalty
            }
            
            // Check if this would complete 3 different types
            let mut unique_types: HashSet<MessageType> = last_two.into_iter().collect();
            unique_types.insert(msg_type);
            if unique_types.len() == 3 {
                multiplier *= 2; // Bonus
            }
        }
        
        // Estimate dependency length bonus
        let dependency_bonus = if message.get_parent_signature().is_some() {
            self.current_dependency_length + 1
        } else {
            0
        };
        
        base_value * multiplier + dependency_bonus
    }


    /// Helper function to calculate the point/byte efficiency ratio for a message.
    /// 
    /// This ratio is used to prioritize messages during batch selection. 
    //  The calculation includes estimated points (base value × multiplier + dependency bonus) and 
    /// a 5% boost for messages with waiting children to unblock dependencies faster. 
    /// 
    /// Returns the efficiency ratio (points / bytes)
    fn calculate_point_per_byte(&self, message: &MessageEnum, size: usize) -> f64 {
        if size == 0 {
            return 0.0;
        }

        let mut points = self.estimate_points(message) as f64;  // float allows for division
        
        // Boost for messages that have children waiting to unlock more messages
        let signature = message.get_signature();
        if let Some(children) = self.waiting_map.get(signature) {
            let num_children = children.len();
            
            // This prioritizes messages that unblock more dependencies
            let boost = if num_children >= 4 {
                1.12  // 12% boost for 4+ children
            } else if num_children >= 2 {
                1.08  // 8% boost for 2-3 children
            } else {
                1.05  // 5% boost for 1 child
            };
            points *= boost;
        }
        
        points / size as f64
    }


    // Helper function called from start main function to process incoming messages and add them to the ready queue or waiting map  
    fn process_incoming_message(&mut self, message: MessageEnum) {
        let size = message.to_bytes().len();
        
        // Struct to wrap message and size for easier processing
        let pending = PendingMessage {
            message,
            size,
        };

        // Check if this message has a parent dependency
        if let Some(parent_sig) = pending.message.get_parent_signature() {
            let parent_sig_clone = parent_sig.clone();
            
            // If parent has been sent, this message is ready
            if self.sent_signatures.contains(&parent_sig_clone) {
                self.ready_queue.push_back(pending);
            } else {
                // Parent not sent yet, add to waiting map
                self.waiting_map.entry(parent_sig_clone).or_default().push(pending);
            }
        } else {
            // No parent dependency, ready to send
            self.ready_queue.push_back(pending);
        }
    }


    // Helper function to move ready messages from waiting map to ready queue to remove dependancy blocking
    fn move_ready_messages(&mut self, parent_signature: &Vec<u8>) {
        if let Some(children) = self.waiting_map.remove(parent_signature) {
            for child in children {
                self.ready_queue.push_back(child);
            }
        }
    }


    // Helper function to select the optimal batch of messages to send
    fn select_optimal_batch(&mut self) -> Vec<MessageEnum> {
        let mut batch = Vec::new();
        let mut batch_size = 0;
        let mut selected_indices = HashSet::new();
        
        // Create a working copy of ready queue with indices
        let candidates: Vec<(usize, &PendingMessage)> = self
            .ready_queue
            .iter()
            .enumerate()
            .collect();
        
        // Sort by point/byte ratio (descending) for greedy selection
        let mut sorted_candidates: Vec<(usize, f64, &PendingMessage)> = candidates
            .iter()
            .map(|(idx, pending)| {
                let ratio = self.calculate_point_per_byte(&pending.message, pending.size);
                (*idx, ratio, *pending)
            })
            .collect();
        
        // Sort by point/byte ratio in descending order (highest ratio first)
        sorted_candidates.sort_by(|(_, ratio_a, _), (_, ratio_b, _)| {
            ratio_b.partial_cmp(ratio_a).unwrap()
        });
        
        // Greedy selection to process highest point/byte ratio messages first
        let mut temp_types = self.recent_types.clone();
        
        // Iterate through sorted candidates to add to batch 
        for (idx, _ratio, pending) in sorted_candidates {
            if selected_indices.contains(&idx) {
                continue;
            }
            
            // Check if adding this message would exceed the batch size limit
            if batch_size + pending.size <= MAX_BATCH_BYTES {

                // Check if this message would improve type sequence
                let msg_type = pending.message.get_type();
                let would_complete_diversity = if temp_types.len() >= 2 {
                    let mut unique: HashSet<MessageType> = temp_types.iter().cloned().collect();
                    unique.insert(msg_type);
                    unique.len() == 3
                } else {
                    false
                };
                
                // Check if adding this message would create a 3rd same type in a row (bad - gets penalty)
                let would_be_third_same_type = if temp_types.len() >= 2 {
                    // Check if the last 2 types in the batch are both the same as this message's type
                    temp_types.iter().rev().take(2).all(|&t| t == msg_type)
                } else {
                    false
                };
                
                // Prefer messages that complete diversity bonus, avoid creating 3rd same type in a row
                if !would_be_third_same_type || would_complete_diversity {
                    batch.push(pending.message.clone());    // Finally add message to batch
                    batch_size += pending.size;
                    selected_indices.insert(idx);  // Add index within ready queue to hash map 
                    
                    // Update temp type history to track same streak of types 
                    if temp_types.len() >= 3 {
                        temp_types.pop_front();
                    }
                    temp_types.push_back(msg_type);
                }
            }
        }
        

        // Second pass: Fill remaining space with smaller high-value messages to maximize bandwidth utilization
        let remaining_space = MAX_BATCH_BYTES.saturating_sub(batch_size);
        if remaining_space > 200 {  // Only if significant space remains >200 bytes 

            // Get remaining candidates, sorted by point/byte ratio (descending)
            let mut remaining_candidates: Vec<(usize, f64, &PendingMessage)> = self
                .ready_queue
                .iter()
                .enumerate()
                .filter(|(idx, _)| !selected_indices.contains(idx))
                .map(|(idx, pending)| {
                    let ratio = self.calculate_point_per_byte(&pending.message, pending.size);
                    (idx, ratio, pending)
                })
                .collect();
            
            remaining_candidates.sort_by(|(_, ratio_a, a), (_, ratio_b, b)| {
                // Sort by ratio first, then by size (smaller fits better)
                ratio_b.partial_cmp(ratio_a).unwrap()
                    .then_with(|| a.size.cmp(&b.size))
            });
            
            for (idx, _ratio, pending) in remaining_candidates {
                if pending.size <= remaining_space {
                    // Check type sequence impact
                    let msg_type = pending.message.get_type();
                    let would_be_third_same = if temp_types.len() >= 2 {
                        temp_types.iter().rev().take(2).all(|&t| t == msg_type)
                    } else {
                        false
                    };
                    
                    // Only add if it doesn't break type sequence unless it has a ratio > 2.0
                    if !would_be_third_same || _ratio > 2.0 {
                        batch.push(pending.message.clone());
                        batch_size += pending.size;
                        selected_indices.insert(idx);
                        
                        // Update temp type history
                        if temp_types.len() >= 3 {
                            temp_types.pop_front();
                        }
                        temp_types.push_back(msg_type);
                        
                        // Recalculate remaining space
                        let new_remaining = MAX_BATCH_BYTES.saturating_sub(batch_size);
                        if new_remaining < 150 {  // Stop if less than 150 bytes left
                            break;
                        }
                    }
                }
            }
        }
        
        // Remove selected messages from ready queue because they are already in the batch and will be sent 
        let mut indices_to_remove: Vec<usize> = selected_indices.into_iter().collect();
        indices_to_remove.sort_by(|a, b| b.cmp(a)); // Sort descending
        
        for idx in indices_to_remove {
            self.ready_queue.remove(idx);
        }
        
        batch
    }


    // Helper function to send a batch of messages to transmitter and updates internal state
    fn send_batch(&mut self, batch: Vec<MessageEnum>) {

        // Iterate through batch 
        for message in batch {

            // Extract signature, type, and parent dependency information
            let signature = message.get_signature().clone();
            let msg_type = message.get_type();
            let has_parent = message.get_parent_signature().is_some();
            
            // Send the message
            if let Err(e) = self.tx.send(message) {
                println!("Transmitter channel closed: {}", e);
                return;
            }
            
            // Records the message's signature in `sent_signatures` to track what has been sent
            self.sent_signatures.insert(signature.clone());
            
            // Update dependency length counter
            if has_parent {
                self.current_dependency_length += 1;
            } else {
                self.current_dependency_length = 0;
            }
            
            // Maintains a sliding window of the last 3 message types for bonus/penalty calculations
            // by updating type history
            if self.recent_types.len() >= 3 {
                self.recent_types.pop_front();
            }
            self.recent_types.push_back(msg_type);
            
            // Move any child messages that were waiting for this signature
            self.move_ready_messages(&signature);
        }
    }


    // Main function for processing.rs
    pub async fn start(&mut self) {
        let mut last_batch_time = Instant::now();
        let batch_interval = Duration::from_millis(BATCH_INTERVAL_MS);

        loop {
            // Collect incoming messages and process them
            while let Ok(message) = self.rx.try_recv() {
                self.process_incoming_message(message);
            }

            // Check if it's time to send a batch
            // Send if 9ms have elapsed (default interval), or
            // send early after 5 ms if batch is full
            let elapsed = last_batch_time.elapsed();
            let should_send = elapsed >= batch_interval 
                || (elapsed >= Duration::from_millis(5) && self.has_full_batch_ready());

            if should_send {
                if !self.ready_queue.is_empty() {
                    let batch = self.select_optimal_batch();
                    if !batch.is_empty() {
                        self.send_batch(batch);
                    }
                }
                last_batch_time = Instant::now();
            }

            tokio::task::yield_now().await;  // yields control back to Tokio runtime 
        }
    }

    // Helper function for main start to check if enough messages to fill a 4096-byte batch
    fn has_full_batch_ready(&self) -> bool {
        let mut total_size = 0;
        for pending in &self.ready_queue {
            total_size += pending.size;
            if total_size >= MAX_BATCH_BYTES {
                return true;
            }
        }
        false
    }
}
