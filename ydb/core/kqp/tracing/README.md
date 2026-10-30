# Compilation tracing

The shared machinery lives here:

- `kqp_compile_trace`: compiler parent stack, regions, scope spans, deferred
  rule spans, dependency groups and future completion.
- `kqp_compile_trace_transformer`: pipeline wrappers and begin/end markers.
  A provider resolves the current trace on each turn, so a reused pipeline
  follows the current compilation without depending on the query context type.
- `kqp_trace_flow`: flow boundaries and dependency links.
- `ut`: machinery tests; `../ut/tracing`: query integration tests.

Keep names, verbosity and operation boundaries at the instrumented call sites.
Use `TCompileTraceDependencies` to start a wait with its first request, mark
concurrent flows and link recorded requests. Pass each span and the released
join to `TraceCompileDependency` to finish them before their consumers resume.
These callbacks own spans, not compiler state. A wait records the lifetime of
its dependencies, not the exact time the compiler was suspended.
