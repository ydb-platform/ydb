# Actor Runtime Development

These instructions apply to `ydb/library/actors/`. For NActors event protocols, lifetime, coroutine handlers, or shutdown, read the [actor development skill](.agents/skills/ydb-actor-development/SKILL.md). The [canonical contributor guide](../../docs/en/core/contributor/actor-system/index.md) documents the runtime contracts; application actors and tablets also follow their own component guidance.

- For runtime changes, trace the affected contract through its implementation and behavioral tests. Use the guide's source map to find the relevant core, async, or Interconnect code.
- Keep application guarantees distinct from executor or transport implementation details. Update the owning contributor page when an observable contract changes.
- For dispatch, lifetime, or threading changes, check event ownership, deferred actor death, forced teardown, and any off-thread completion path that the change affects.
- Use the narrow core, async, Interconnect, or component test target identified in [Testing Actors](../../docs/en/core/contributor/actor-system/testing.md), with the active build instructions.

The guide is maintained under `ydb/docs/en/core/contributor/actor-system/`; do not copy it into this file or a second manual under the library.
