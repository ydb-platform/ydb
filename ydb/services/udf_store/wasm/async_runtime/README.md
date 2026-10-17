# Async WASM prototype (P1)

This is an experimental byte-oriented async path for RFC 016. It is exercised
by a real C++20 coroutine module on WAVM and a controlled fake transport.
It does not register asynchronous scalar UDFs or change the synchronous Bridge
ABI. SQL lowering, service manifests, HTTP/gRPC clients and connection metadata
are subsequent integration work.

## Ownership and Scheduling

`TRuntime` owns one query compartment and its MiniKQL allocator. Arguments are
copied into a call-owned guest allocation before `WasmAsyncCallStart` and stay
valid until `Drop`. Operation requests and responses use owned host buffers.
Neither a transport callback nor a suspended guest frame keeps a pointer to a
stack invocation context or resident scratch. The prototype does not expose
Bridge handles through its payload ABI.

Every guest entry binds the allocator, query/compartment TLS and a fresh
invocation/run scope. These guards end before the caller waits. Guest entries
are serialized; concurrent or reentrant owner calls are rejected. Call handles
are process-unique and operation handles are checked against their owning call.
Callbacks retain a weak registry reference and reject duplicate or late replies.

`ITransport::Start` may complete synchronously or from another thread. Completion
only records a host result. The optional wakeup callback must enqueue an owner
event (with a safe lifetime), not enter guest code. On that event the owner takes
`TakeReady()` and calls `Poll` for each ready call. Completion and wait registration
share a lock, so completion between operation poll and suspension is latched.
No timer-based I/O polling is needed. The owner arms a deadline event using
`NextDeadline()` and calls `Expire(now)` on that event.

The guest SDK provides scoped `TOperation` and move-only `TTask<T>` with nested
coroutines. `TCallContext` resumes the suspended leaf. Starting multiple scoped
operations before awaiting them provides concurrent transport operations.
Uncaught guest exceptions trap; transport failure is an ordinary operation status.

`Cancel` cancels operations and destroys guest continuations; `Drop` also releases
the call wrapper and argument allocation. Owner teardown cancels all operations
even if guest cleanup does not run. A guest trap retires the entire compartment
and fails its other calls. Host buffer/call/operation quotas and cumulative CPU
accounting are bounded locally. Each entry arms WAVM's interruption deadline
using the remaining call/CPU budget; cleanup gets its own bounded entry budget.
These local limits are not distributed query quotas. Transport-owned buffers
after physical cancellation and guest heap quotas require further integration.

## Fixture and Tests

The guest fixture is built with the existing `clang20-emscripten-wasm64`
toolchain. Its reusable arena makes guest frame lifetime observable. Its source
is in `fixture/main.cpp`; the generated binary is committed as a test resource
so native unit tests do not require a guest toolchain download.

Regenerate the fixture from the repository root:

```sh
./ya make --build release --target-platform=clang20-emscripten-wasm64 \
    ydb/services/udf_store/wasm/async_runtime/fixture
cp -L ydb/services/udf_store/wasm/async_runtime/fixture/libasync_runtime-fixture.so \
    ydb/services/udf_store/ut/data/async_coroutine.wasm
```

Run the native harness and existing synchronous regressions:

```sh
./ya make --build relwithdebinfo -tA ydb/services/udf_store/ut
```

The harness covers ready/pending, sequential/nested and parallel operations,
interleaved calls, completion-before-wait, callback threading, duplicate/stale
handles, cancellation, deadlines, traps, CPU interruption, forced teardown,
quotas and frame/buffer counters returning to baseline.
