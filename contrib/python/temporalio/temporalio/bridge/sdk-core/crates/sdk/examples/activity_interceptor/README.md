# Activity Inbound Interceptor

Demonstrates wrapping inbound activity execution with an interceptor that logs decoded activity
inputs and outputs.

Run the worker first, then start the workflow in another terminal:

```bash
cargo run --features examples --example activity-interceptor-worker
cargo run --features examples --example activity-interceptor-starter
```

Expected worker output includes logs for the matched `greet` activity input and output, plus a
generic log for the other activity type.
