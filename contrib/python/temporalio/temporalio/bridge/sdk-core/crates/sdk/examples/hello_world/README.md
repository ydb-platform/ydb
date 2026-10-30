# Hello World

This sample shows a basic workflow that calls a single activity and returns the result.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example hello-world-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example hello-world-starter
```

The starter should print:

    Workflow result: Hello, Temporal!
