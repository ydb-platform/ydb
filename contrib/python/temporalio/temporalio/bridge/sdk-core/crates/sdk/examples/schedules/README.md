# Schedules

This sample demonstrates creating and managing scheduled workflows using the Temporal schedule API.

The starter creates a schedule that triggers a workflow every 10 seconds, describes it, lists all schedules, triggers it immediately, waits for execution, then cleans up by deleting the schedule.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example schedules-worker
```

3. In another terminal, run the starter:

```bash
  cargo run --features examples --example schedules-starter
```

The starter creates, triggers, and deletes the schedule, printing status at each step.
