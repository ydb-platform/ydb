# Polling

This sample demonstrates a polling pattern that repeatedly checks an external condition.

The workflow calls an activity to check if a condition is met. Between attempts it sleeps with a timer. It completes when the condition is met or the maximum number of attempts is exhausted.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example polling-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example polling-starter
```

The starter should print:

    Workflow result: Condition met on attempt 3
