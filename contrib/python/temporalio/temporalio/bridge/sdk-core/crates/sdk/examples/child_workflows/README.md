# Child Workflows

This sample shows a parent workflow that spawns multiple child workflows and collects their results. Each child workflow greets a name, and the parent gathers all greetings into a list.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example child-workflows-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example child-workflows-starter
```

The starter should print:

    Workflow result: ["Hello, Alice!", "Hello, Bob!", "Hello, Charlie!"]
