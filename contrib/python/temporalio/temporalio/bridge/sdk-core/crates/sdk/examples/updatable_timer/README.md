# Updatable Timer

This sample demonstrates a timer that can be rescheduled via signals.

The workflow starts with a far-future deadline. A signal updates the deadline to a near-future time, causing the workflow to recompute the remaining duration and fire sooner.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example updatable-timer-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example updatable-timer-starter
```

The starter sets an initial deadline 1 hour out, then signals a new deadline 2 seconds from now. The workflow should complete shortly after the signal.
