# ydb_bench run lifecycle model

`run_lifecycle.pml` is a bounded Promela model of the orchestration contract for
`ydb_bench`. It models ordering and failures, not benchmark performance or the
Python implementation itself.

## Scope and bounds

The checked instance has three ordered steps, one executor, at most one
controller crash/recovery, at most one cancellation request, at most one stale
completion event, and no automatic retry. Two steps are supported and one is
unsupported. The controller nondeterministically selects fail-fast or
continue-on-error policy.

Three steps are the smallest useful bound for checking a running step, a later
step, and an unsupported step in one execution. A successful exhaustive check
applies only to these bounds and assumptions.

Modeled outcomes and failures are:

- successful exit, non-zero exit, and invalid metrics;
- unsupported affinity before process creation;
- controller crash before or after process completion;
- a process that remains alive while the controller is down;
- stale completion with a mismatched attempt token;
- cancellation racing with process completion and recovery;
- storage failure while publishing artifacts or a manifest;
- fail-fast and continue-on-error queue policies.

Timeout and interrupt collapse into the same terminal process-error transition.
Disk-full, permission, and atomic-replace failures collapse into
`storage_failed`. The model does not cover multiple executors, retries, remote
workers, corrupt files that were previously acknowledged as durable, PID reuse,
or loss of the machine itself.

## Protocol contract

The model established the following requirements for the implementation:

1. A completion is one indivisible event containing step ID, attempt token, and
   outcome. The event must not become visible before all fields are initialized.
2. Every completion handler, not only the stale-event branch, checks the attempt
   token. A mismatched event is ignored without changing the running attempt.
3. A recovered controller never starts a replacement while the old process is
   alive or while a completion for its attempt is pending. An unresolvable
   attempt becomes `LOST`; it is never reported as passed.
4. Result artifacts become durable before an atomic manifest version refers to
   them. `PASSED` requires both valid metrics and durable artifacts.
5. Durable cancellation atomically records the cancellation state and the
   current start generation. No later process may start.
6. Detecting a storage error and cleaning it up are separate transitions. Once
   `storage_failed` is visible, a mandatory handler stops or loses the current
   process, cancels pending steps, and terminates the run as
   `INFRA_FAILED`.
7. A controller with no work blocks for an event. It must not busy-poll through
   an always-enabled no-op transition.
8. Terminal run states contain no live benchmark process. Repeated cancel and
   stale completion events are idempotent with respect to terminal state.

## Properties

Safety properties are also backed by local assertions near the corresponding
transitions:

- `no_pass_without_durable_results`;
- `no_manifest_before_artifact`;
- `no_duplicate_start`;
- `terminal_has_no_process`;
- `no_start_after_cancel`;
- `unsupported_never_started`;
- `completed_is_clean`.

`eventually_terminal` is the liveness property. It is valid only under the fair
finite environment encoded by this model: the single allowed crash is followed
by recovery, a live process eventually emits a completion, storage failure is
eventually handled, and every enabled model transition is eventually selected.
An unbounded sequence of crashes, permanently hung process without a timeout,
or permanently unavailable storage intentionally invalidates this claim.

## Counterexamples found while developing the model

SPIN exposed four distinct protocol/model defects before the final run:

1. Overlapping Promela guards made recovery/cancel behavior depend on branch
   selection. Guards are now mutually exclusive. Implementation transitions
   need the same explicit preconditions.
2. An always-enabled controller no-op could starve process completion forever.
   The controller transition is now enabled only when work exists.
3. Completion readiness could become visible before its outcome. Completion is
   now published as one atomic event.
4. A storage failure raised inside result publication disabled the only storage
   failure handler and left the run nonterminal. Detection now always enables a
   separate cleanup transition.
5. A stale event could still enter a normal outcome branch because those
   branches did not require a matching attempt token. All outcome branches now
   require it.
6. Cancellation became visible before its start-generation snapshot. Both are
   now one atomic durable transition.

These sequences should become regression tests for the Python state machine and
result writer.

## Verification result

The final model was checked with SPIN 6.5.2 using the exhaustive profile, a
maximum depth of 100,000, and 1 GiB memory limit. Every property completed with
zero errors; no bitstate result is used as evidence.

| Property | Stored states | Transitions | Run ID |
| --- | ---: | ---: | --- |
| `no_pass_without_durable_results` | 42,618 | 44,404 | `ae98df36b3a34e7d` |
| `no_manifest_before_artifact` | 42,618 | 44,404 | `6f92046a3e0a4cf2` |
| `no_duplicate_start` | 42,618 | 44,404 | `d70ac290c93748a5` |
| `terminal_has_no_process` | 42,618 | 44,404 | `6350d084abc54428` |
| `no_start_after_cancel` | 42,618 | 44,404 | `c9f1929fccd74deb` |
| `unsupported_never_started` | 42,618 | 44,404 | `03064e5bf2654ca5` |
| `completed_is_clean` | 42,618 | 44,404 | `ebc8d56f820344a1` |
| `eventually_terminal` | 27,850 | 85,417 | `3fbd8cfdc0d845c8` |

Validate the complete source first, selecting one named property, then run one
exhaustive verification per property. The Nerve SPIN tools reject a model with
multiple LTL claims unless `property_name` is explicit.
