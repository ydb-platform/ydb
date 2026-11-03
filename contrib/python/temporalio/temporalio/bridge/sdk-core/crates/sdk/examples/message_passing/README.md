# Message Passing

This sample demonstrates signals, queries, and updates on a workflow:

- **Signal**: increment a counter by a given amount.
- **Query**: read the current counter value without affecting the workflow.
- **Update with validation**: set the counter to a new value, rejecting negative values.

The workflow waits until the counter reaches a target value, then completes.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example message-passing-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example message-passing-starter
```

The starter signals, queries, and updates the workflow, then prints the final result.
