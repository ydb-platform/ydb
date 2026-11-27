---
name: ydb-actor-development
description: "Implement, debug, or review NActors event protocols, actor lifetime, coroutine handlers, and shutdown, including actor-based tablets. Use when these mechanisms are involved; ordinary business logic in an actor does not require this skill."
---

# Actor Development

Use the [actor system guide](../../../../../docs/en/core/contributor/actor-system/index.md) for runtime contracts. Tablets also have component-specific transaction and recovery rules; inspect the existing tablet fixture and neighboring code. For runtime scheduling failures such as stuck mailboxes, lost wakeups, or executor sleep races, also use the [actor system debugging skill](../ydb-actor-system-debug/SKILL.md).

## Select Context

1. Read the guide's invariants. Use its runtime source map when changing or verifying runtime behavior.
2. Read only the topic pages the change needs:
   - Construction, registration, dispatch, pool choice: [lifecycle](../../../../../docs/en/core/contributor/actor-system/actor-lifecycle.md).
   - Payloads, ownership, cookies, forwarding, timers, Interconnect: [messaging](../../../../../docs/en/core/contributor/actor-system/events-and-messaging.md).
   - Death, cancellation, RAII, and forced teardown: [shutdown](../../../../../docs/en/core/contributor/actor-system/failure-and-shutdown.md). Read it when changing cancellation or cleanup, or when introducing work that may outlive its initiating handler.
   - Stackless handlers: [coroutines](../../../../../docs/en/core/contributor/actor-system/coroutine-actors.md).
   - A wait or combinator: the selection table in [primitives](../../../../../docs/en/core/contributor/actor-system/coroutine-primitives.md), then only the section of the chosen primitive.
   - Foreign threads or custom awaiters: [integration](../../../../../docs/en/core/contributor/actor-system/coroutine-integration.md).
   - Frame-cache budget, statistics, or allocation tests: [frame cache](../../../../../docs/en/core/contributor/actor-system/coroutine-frame-cache.md).
3. Before finishing, check the change against the review checklist in [testing](../../../../../docs/en/core/contributor/actor-system/testing.md), and against its coroutine checklist when the change has stackless handlers.

## Apply the Contract

Trace the changed event from sender through the handler to its reply and cleanup. For each event, buffer, coroutine frame, child actor, and subscription, identify its owner at each step and the code that releases or ends it.

For lifetime changes, account for late events while cancellation is pending and for cleanup during forced teardown. For a tablet, identify which state is durable and which state must be reconstructed after restart; actor serialization does not itself provide persistence.

Select tests from the test entry points in [testing](../../../../../docs/en/core/contributor/actor-system/testing.md). Add runtime tests when changing runtime guarantees, and component tests when changing a component protocol. Follow active build instructions for the target.

## Documentation Updates

Update a contributor page when the change alters a documented contract or makes a statement wrong, or when the user asks for documentation work. Change the matching pages under `ydb/docs/en/` and `ydb/docs/ru/` together, with the same meaning.
