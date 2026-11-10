# Continue-As-New

This sample demonstrates the continue-as-new pattern for workflows that need to run for a long time. Instead of building up an unbounded history, the workflow periodically re-creates itself with updated state.

The workflow increments a counter each run and continues-as-new until it reaches the target iteration count.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example continue-as-new-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example continue-as-new-starter
```

The starter should print:

    Workflow result: Completed after 5 iterations
