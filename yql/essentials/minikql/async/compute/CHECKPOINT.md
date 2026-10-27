# D4: generic async graph checkpoint and restore

This layer extends the D1 lifecycle, D2 MiniKQL continuations and D3 scheduler.
It has no WASM, HTTP, credential, SQL or actor dependency. It is exercised with
native providers and actual MiniKQL Stream/Flow graphs. Production coordinator,
channel and source adapters remain D6; UDAF/operator-specific state is D5.

## Execution settings and participation

Checkpoint is disabled by default. Set `TExecutionSettings::Checkpoint` to
`Drain` or `Replay`, supply a stable `PlanFingerprint`, and bind the same
`TExecutionServices` to both `TComputationOptsFull::AsyncServices` and
`AsyncCheckpoint`. The fingerprint must cover the typed graph, source cursor
format, operator codecs and relevant configuration. This is a compatibility
contract, not a runtime-generated hash of arbitrary objects.

`AsyncMap` is now a serializable MiniKQL value. The existing
`SaveGraphState/LoadGraphState` path saves it alongside source/operator state in
`SerializableValues`. Each active map registers owner-only drain readiness.
Saved async values are hydrated after graph parsing, including values behind
lazy Stream/Flow wrappers. This does not Fetch input, decode replay descriptors,
submit operations or call providers. `CompleteRestore` verifies the set of
materialized participants before enabling execution.

The embedding engine must provide checkpointable sources and participating
stateful operators. It must stop source admission at the agreed input positions,
snapshot buffered input, and account for downstream queues. An arbitrary
`Iterator` over a list is not a checkpointable source. The tests use a real
serializable source cursor and verify all 200 input positions after recovery.
The engine must reject unsupported graph shapes before running them; merely
setting Replay is not proof that a whole distributed graph has a consistent cut.

## Owner protocol

All these calls run on the task owner with its MiniKQL allocator bound. Provider
callbacks still only publish owned bytes through D1; they never mutate frames or
snapshot state.

1. `BeginCheckpoint(id)` starts a monotonically increasing boundary and pauses
   new source admission (`IsInputPaused`). Existing frames keep making progress.
2. The source/channel adapter establishes its cursor and buffered-input cut,
   then calls `EstablishInputCut(id)`. This is an explicit assertion by the
   adapter; the generic library cannot discover network channel positions.
3. After each result leaves the root graph, call `OutputProduced()`. Keep the
   returned sequence until downstream acknowledges it, then call
   `OutputAcknowledged(generation, sequence)`. Acknowledgements are ordered and
   refer to the current execution generation; stale acknowledgements are ignored.
4. Wait asynchronously for `CanSaveCheckpoint()`. No polling or dedicated thread
   is introduced. Completion, source, quota and downstream events drive the
   existing owner loop. Checkpoint timeout/cancellation belongs to the coordinator.
5. Call `graph.SaveGraphState()`. It returns an immutable image; execution pauses
   until its bytes have been transferred to bounded checkpoint storage and the
   owner calls `SnapshotStored(snapshotId)`. Capture `GetSnapshotId()` with the
   returned bytes: the acknowledgement includes execution generation, checkpoint
   ID and snapshot revision. Late acknowledgements cannot confirm a different
   image or release its reservation. Call this only after the storage operation
   required by the coordinator succeeds. This acknowledges that local operation,
   **not global checkpoint completion**. A save failure releases the temporary
   reservation and permits retry of the same cut.
6. Only `CanForwardBoundary()` permits forwarding the barrier, followed by
   `ReleaseBoundary()`. The latter resumes input and posts one coalesced rerun.
   `CanForwardWatermark()` is a conservative readiness gate: no outstanding
   pre-watermark frames or unacknowledged root outputs. The engine associates
   watermarks with its own input/output positions; this layer has no event-time
   implementation.

The engine must keep channels and durable checkpoint metadata on the same cut.
Source positions, output acknowledgement and pending descriptors alone are not
interchangeable proofs of a distributed checkpoint.

## Drain and Replay

**Drain** waits until all accepted frames have produced output and downstream has
acknowledged it. No pending operation is serialized, and providers need no replay
codec. Waiting keeps the compute worker available for other tasks.

**Replay** requires an explicit `IReplayCodec` for every invoked provider, checked
before input consumption. The codec has a versioned `Contract`, `Encode` and
`Decode`. It describes logical arguments and permits repeating an external call;
async or batch capability does not imply replay safety. Encode/Decode must be
pure, bounded codecs. They must not serialize secrets, transport handles,
reservations, execution identity, native stacks or process-local clock deadlines.
Fresh provider registrations supply runtime context after restore.

Replay can save pending frames at a consistent local recovery cut. A ready result
already drained by the owner is saved as a terminal result and is not called again.
A completion still in the callback queue is in the explicitly replayable scope.
Errors retain only their outcome/code, not provider error text. Ready ordered
outputs and unfinished synchronous/conditional instructions are also preserved.

**A pending Replay image does not authorize the barrier to overtake its results.**
In this D4 adapter, any subsequent frame progress/output invalidates that image
for barrier forwarding. Once the accepted work drains and downstream acknowledges
it, save/store a new image at the advanced cut before forwarding. This is the
conservative Drain fallback for barrier completion. An engine that wants to
commit a distributed checkpoint with pending descriptors needs a coordinated
channel/output-cut codec in D6; D4 does not claim such an engine protocol.

This distinction prevents a subtle duplicate: forwarding a barrier after results
were consumed downstream while attaching an older snapshot that still replays
those results. A regression restores the refreshed image and verifies that rows
1–16 are not emitted again, while rows 17–200 are processed exactly once locally.
There is no exactly-once guarantee for external side effects.

## Saved state and restore

An async-only graph envelope contains a magic/version, plan fingerprint, mode,
checkpoint ID and participant IDs. Synchronous graphs retain their existing
byte layout. Each AsyncMap has its own versioned state with:

- original row and typed computed slots, including branch decisions;
- instruction stack, frame order and round-robin position;
- source EOF, suspended source-fetch charge/reservation;
- terminal result or versioned logical replay descriptor for a pending invocation.

Operation IDs, generations, providers, sockets and completion tokens are never
serialized. Frame count/depth, indexes, program metadata, codec contract, terminal
outcome, payload bounds and trailing bytes are validated. Corrupt/incompatible
state fails explicitly. After any restore failure discard that graph/context.

Restore into a **fresh** graph and services generation with fresh provider/codec
registrations. Call `LoadGraphState`, obtain the root with `GetValue`, then call
`CompleteRestore`. Parsing and hydration issue no external calls. Only a later
`BeginTurn`/Fetch can decode a logical descriptor and resubmit it with a new ID.
Normal D3 admission, batching, concurrency and CPU bounds apply to replay. Old
callbacks remain bound to the old lifecycle and cannot affect restored frames.
Input stays paused until the boundary can be forwarded/released.

Snapshot copies reserve three times `MaxSnapshotBytes` from the same D3 quota
before saving, covering serializer buffers plus the returned image. The configured
size bound is checked during serialization. Restore reserves bounded temporary
snapshot space before parsing/hydration and charges restored frames/descriptors
and retained allocator pages. `SnapshotStored` transfers image accounting to the
embedding checkpoint store; it must not be called merely to waive a memory limit.
Existing allocator guards still enforce transient allocation peaks, and provider
private buffers keep their own accounting as in D3. No unlimited waiting for an
impossible snapshot reservation is introduced.

## Tests

From `yql/essentials/minikql/async`:

```sh
../../../../ya make --build relwithdebinfo -tA --retest --no-bazel-remote-store
../../../../ya make --build relwithdebinfo -tA --retest --no-bazel-remote-store --sanitize=thread
```

The D4 tests cover Drain with a slow downstream; replay of pending and
completed-but-not-applied calls; a callback thread racing an immutable image;
late old-generation callbacks/acknowledgements; nested Stream/Flow and one-step
CPU yields; preservation of IF decisions; terminal errors without retries or
error-text disclosure; actual batches of eight; queued work under concurrency
one; 200-row recovery; stale-image rejection and a second crash from the refreshed
cut; missing codecs, invalid cuts/mode/plan/version/truncated data; snapshot
memory/size failures; restore gating; and legacy synchronous graph state.
