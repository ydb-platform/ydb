# Cancellation

This sample demonstrates automatic workflow cancellation propagation and detached cleanup.

The workflow starts a long-running activity that inherits the workflow cancellation token. When the
workflow is cancelled, the SDK automatically cancels the activity. The workflow then handles that
cancellation and runs cleanup with a detached token so cleanup can finish before the workflow
completes.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example cancellation-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example cancellation-starter
```

The starter waits 2 seconds, then requests cancellation. The workflow should complete with a message indicating cancellation and cleanup.
