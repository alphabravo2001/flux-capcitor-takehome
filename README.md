# Flux Capacitor

We have bad news. The Flux Capacitor is broken. We need you to fix it.

## What is the Flux Capacitor?

It's an imaginary system with made-up rules, dependencies, and constraints. It is designed to test how you might navigate working on a real system.

At a high level:

```
┌─────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────┐
│ SOURCE  │───▶│ PROCESSING  │───▶│ TRANSMITTER │───▶│ SINK │
└─────────┘    └─────────────┘    └─────────────┘    └──────┘
```

Each of these is an async tokio task, and they are connected via channels.

### Source
Generates imaginary blockchain transactions. There are different types of different sizes (byte lengths).
Transactions may or may not have dependencies on previous transactions (Option<parent_signature>), and each transaction has a different inherent 'value'.

### Processing
This is yours to fill in, you need to read the transactions as they are generated, and pass along transactions in a way that maximizes your 'points'.

### Transmitter
The transmitter reads transactions (only up to 4096 bytes at a time!) every 10ms, and forwards them to the sink.

### Sink
The sink validates the transactions, checks any dependencies, and measures the transaction value.

## Objective and Constraints
Your goal is to get as many points as you can in 10 seconds. The only constraint is that you cannot edit any files in the ./src/constraints directory. Feel free to suggest quality-of-life improvements, or let us know if you think there is a bug, as of this writing this is a brand new technical evaluation.
We encourage you to ask questions. There are no rules other than what is written above, and there are many different ways to approach this problem.

## My Changes
To solve this problem I edited processing.rs. To support my solution, I added 5 new variables to the Processing struct: `waiting_map`, `ready_queue` , `sent_signatures`, `recent_types`, `current_dependency_length`. The start function is the main function in processing.rs and calls process_incoming_message to route incoming messages to either the ready_queue if their parent is already sent or waiting_map if these messages waiting for a parent. Periodically, start calls select_optimal_batch which uses calculate_point_per_byte, which in turn calls estimate_points, to prioritize messages by efficiency ratio. Once a batch is selected, start calls send_batch which sends messages to the transmitter and then calls move_ready_messages to unblock any children that were waiting for those messages.

I transformed the simple passthrough implementation into an intelligent message scheduler that maximizes points through several key optimizations. The solution implements dependency resolution by tracking parent-child relationships in a `waiting_map` and `ready_queue`, ensuring parents are always sent before children to avoid zero-point messages. I implemented a greedy batch selection algorithm that prioritizes messages by point/byte efficiency ratio, considering type sequence bonuses (2x for 3 different types) and penalties (0.5x for 3 same types). Through experimenation, I refined the prioritization system to use a scaled boost mechanism that increases with the number of waiting children (5% for 1 child, 8% for 2-3 children, 12% for 4+ children), ensuring messages that unblock more dependencies are prioritized higher. Additionally, I implemented a second-pass optimization that fills remaining batch space (>200 bytes) with smaller high-value messages to maximize bandwidth utilization within the 4096-byte limit. The timing is optimized with a 9ms batch interval, which provides a 1ms buffer before the transmitter's 10ms tick to ensure messages are received in time, and early batch sending after 5ms if the batch is full. These changes improved the score from the baseline of around 14000 points to around 24000 points. 

## Potential Next Steps

While my current algorithm provides a good balance between performance and computational efficiency, there are several potential improvements that could further optimize the solution. Right now I implement immediate parent-child dependency resolution, tracking which messages are waiting for their direct parent to be sent and unblocking children once parents are processed. A potential improvement would be to analyze the entire dependency graph structure, considering not just immediate parents but also the depth and breadth of the dependency tree, and prioritizing messages that would unblock the most descendants in the dependency chain. Additionally, right now I implement a greedy algorithm for batch selection, but a dynamic programming approach could solve the knapsack problem optimally by evaluating all possible message combinations within the 4096-byte constraint when selecting each batch. However, this would come with significantly increased computational complexity (approximately O(n * W) where n is the number of messages and W is the batch size in bytes), which may not be feasible within the 9ms batch interval constraint. 
