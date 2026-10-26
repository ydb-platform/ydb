This fixture exercises WASI `clock_time_get` through the actual C/C++ standard
library: `time()`, `system_clock::now()` and `steady_clock::now()`. It also calls
all three from global constructors, before any asynchronous invocation exists.

Regenerate the embedded binary from this directory:

```sh
../../../../../ya make --build relwithdebinfo --target-platform=clang20-emscripten-wasm64 --no-bazel-remote-store
cp libclock_fixture.so clock.wasm
```

The host supports realtime (Unix epoch) and monotonic clocks in nanoseconds.
Clock reads are immediate. The precision argument is a tolerance hint; the host
returns its full available precision. CPU clocks return WASI `NOTSUP`, unknown
IDs return `INVAL`, and invalid output ranges return `FAULT` without writing.
Other system calls remain subject to their existing restrictions.
