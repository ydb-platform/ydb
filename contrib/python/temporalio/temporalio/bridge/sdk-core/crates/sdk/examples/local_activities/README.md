# Local Activities

This sample demonstrates the difference between remote and local activity execution.

- **Remote activities** are scheduled via the server and can run on any worker.
- **Local activities** execute directly on the same worker, avoiding the scheduling round-trip.

The workflow runs the same activity both ways and returns both results.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example local-activities-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example local-activities-starter
```

The starter should print both the remote and local activity results.
