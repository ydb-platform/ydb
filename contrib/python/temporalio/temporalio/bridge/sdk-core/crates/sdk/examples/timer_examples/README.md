# Timer Examples

This sample demonstrates various timer patterns in Temporal workflows:

- **Simple timer**: sleeping for a fixed duration.
- **Racing a timer against an activity**: using `select!` to run whichever finishes first.
- **Cancelling a timer**: starting a long timer and immediately cancelling it.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example timer-examples-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example timer-examples-starter
```

The starter should print:

    Workflow result: race_winner=activity, timer_cancelled=true
