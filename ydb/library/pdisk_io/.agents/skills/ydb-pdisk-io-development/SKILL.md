---
name: ydb-pdisk-io-development
description: "Implement, debug, or review I/O backends, TUringRouter, buffer and completion ownership, and thread shutdown in ydb/library/pdisk_io."
---

# PDisk I/O Library Development

Read the [library source and contract guide](../../../README.md), then select the relevant backend, buffer, or router implementation. This skill covers the reusable I/O library; PDisk log/chunk state belongs to the [PDisk internals skill](../../../../../core/blobstorage/pdisk/.agents/skills/ydb-pdisk-development/SKILL.md).

## Trace Ownership Across Threads

For a router change, follow setup before Start, admission, queueing, device submission, completion, and Stop. Identify who owns every operation, buffer, file handle, registered resource, and completion object until it is safe to release. Verify the exactly-once completion contract only for accepted work, and inspect the rejection path separately.

Read the actual caller when changing a boundary. DDisk and its PB child have separate actor lifetimes and can own separate routers over duplicated handles. Completion on an I/O thread must use the caller's supported handoff to actor execution; it must not manipulate actor-local continuations or TLS-bound APIs directly.

Distinguish router modes, optional registered files, and the caller's PDisk raw-event fallback. Tests that cover one mode do not establish behavior for every backend. Do not copy DDisk quorum, PB erase, or PDisk owner policy into this library's contract.

## Documentation Updates

Read linked documentation as context. During code changes, update documentation only when the change alters a documented contract or makes an existing statement inaccurate, or when the user explicitly requests documentation work. For contributor pages under `ydb/docs/en/` or `ydb/docs/ru/`, update the corresponding path in the other language in the same change.

Delegate needed documentation updates to a subagent with forked conversation context (`fork_turns: "all"` when supported). Give it the affected paths, the final behavior change, and relevant validation results; scope its edits to documentation. Review its changes for accuracy and English/Russian consistency before completing the task.

## Validation

Use the focused suites in `ydb/library/pdisk_io/ut` following active build instructions. For resource registration or shutdown changes, select cases with outstanding work, rejected submissions, initialization failure, and delayed completion as applicable. For changed DDisk integration, inspect the DDisk/PDisk caller tests as well.

Check whether a test actually exercised io_uring, selected a fallback, or skipped because of kernel capabilities. Report that distinction. Keep hardware-specific stress tests separate from deterministic unit validation.
