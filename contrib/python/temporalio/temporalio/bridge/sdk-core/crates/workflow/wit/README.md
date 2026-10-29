# Workflow Runtime WIT

This directory defines `temporal:workflow-runtime@0.1.0`, the high-level guest interface that
workflow code targets when it is compiled to a WebAssembly component. It is intentionally separate
from the core activation/completion protocol that worker SDKs speak to the Temporal cluster: core
remains the worker-facing protocol, and a worker is responsible for translating core activations
and completions into calls against this interface.

## What's in here

- `world.wit` — the `workflow-module` world: imports `workflow-host`, exports `workflow-guest`.
- `guest.wit` — `workflow-guest`: the entry points the guest exports for the host to drive
  (`list-workflows`, `instantiate-workflow`, and the `workflow-instance` resource with `activate`
  and `poll-routine`).
- `host.wit` — `workflow-host`: the host capabilities the guest imports (e.g. `set-current-details`,
  `push-command`, and `patch-activation`).
- `types.wit` — shared records and variants used by both sides (init/activation/completion shapes,
  routine kinds, terminal outcomes, etc.). Some fields are typed as `list<u8>` and carry encoded
  protobuf messages — those proto schemas are part of the ABI; see *Stability* below.

## How a workflow runs against it

1. The host instantiates a workflow run by calling `instantiate-workflow` with `workflow-init`.
2. For each activation, the host calls `activate` on the `workflow-instance` resource. The guest
   processes the activation's jobs and reports per-job results (started routines, query responses,
   update rejections).
3. The guest drives its routines (main, signals, updates) by being polled — the host calls
   `poll-routine(routine-id)` until the routine either completes or reports it made no progress.
4. While running, the guest invokes host functions to push commands and update execution state.

The guest interface is deliberately higher-level than core's activation protocol: ordering rules
and worker bookkeeping live in the host translation layer, not in the guest contract.

## Synchronous ABI and the WASIp3 future

The guest interface is synchronous: `activate` and `poll-routine` return ordinary results, and a
routine signals "blocked" by returning `routine-poll-result.made-progress = false`. The host
re-enters `poll-routine` after the relevant activation lands. This shape is what stable Wasmtime
and `wit-bindgen` support today.

When component-model async (WASIp3 / Preview 3) is stable in Wasmtime, the natural evolution is:

- `activate` and `poll-routine` become `async func`.
- `routine-poll-result.made-progress` and the explicit `routine-id`-keyed polling protocol can
  go away — the host can `await` each routine directly.

That is a breaking ABI change and will require a major-version WIT bump.

## Stability

The package is published as `temporal:workflow-runtime@0.1.0` and is **not yet stable**. Until it
is bumped to `1.0.0`, any release may bump the minor version with breaking changes. Once external
SDKs (Python, TypeScript, Go, etc.) begin compiling guest workflows against this WIT, breaking
changes need to follow normal SemVer discipline: bump the package version, dual-export the old
and new world from the host for a deprecation window, and document the migration path.

Changes that force a major bump include:

- Renaming or removing any record field, variant case, function, interface, or world.
- Changing a field/case/parameter type (including widening `u32` to `u64` etc.).
- Reordering variant cases (the discriminant order is part of the ABI).
- Changing the proto messages encoded into `list<u8>`-typed fields (`failure`, `payload`,
  `workflow-command`, `workflow-activation`, `continue-as-new-request`).
