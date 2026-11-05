# Saga

This sample demonstrates the saga (compensation) pattern for distributed transactions. The workflow books a hotel, flight, and car in sequence. If any step fails, all previously completed bookings are compensated (cancelled) in reverse order.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example saga-worker
```

3. In another terminal, run the workflow:

```bash
  cargo run --features examples --example saga-starter
```

By default, all bookings succeed. To trigger a failure and see compensations run:

```bash
  cargo run --features examples --example saga-starter -- fail-trip
```
