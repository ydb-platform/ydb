# Activity Heartbeating

This sample shows a long-running activity that periodically heartbeats its progress. On retry, the activity resumes from the last heartbeated checkpoint instead of starting over.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example activity-heartbeating-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example activity-heartbeating-starter
```

The starter should print:

    Workflow result: Completed 10 steps
