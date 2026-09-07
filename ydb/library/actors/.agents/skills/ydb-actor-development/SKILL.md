---
name: ydb-actor-development
description: "Implement, debug, or review NActors event protocols, actor lifetime, coroutine handlers, and shutdown, including actor-based tablets. Use when these mechanisms are involved; ordinary business logic in an actor does not require this skill."
---

# Actor Development

Use the [actor system guide](../../../../../docs/en/core/contributor/actor-system/index.md) for runtime contracts. Tablets also have component-specific transaction and recovery rules; inspect the existing tablet fixture and neighboring code.

## Select Context

Read the guide's invariants, then only the topic pages needed for the change:

- Construction, registration, dispatch, pool choice: [lifecycle](../../../../../docs/en/core/contributor/actor-system/actor-lifecycle.md).
- Payloads, ownership, cookies, forwarding, timers, Interconnect: [messaging](../../../../../docs/en/core/contributor/actor-system/events-and-messaging.md).
- Death, cancellation, RAII, and forced teardown: [shutdown](../../../../../docs/en/core/contributor/actor-system/failure-and-shutdown.md).
- Stackless handlers: [coroutines](../../../../../docs/en/core/contributor/actor-system/coroutine-actors.md); load [primitives](../../../../../docs/en/core/contributor/actor-system/coroutine-primitives.md) for the relevant wait/combinator and [integration](../../../../../docs/en/core/contributor/actor-system/coroutine-integration.md) for foreign threads or custom awaiters.
- Fixtures and behavioral coverage: [testing](../../../../../docs/en/core/contributor/actor-system/testing.md).

## Apply the Contract

Trace the changed event from sender through the handler to its reply and cleanup. Identify who owns each event, buffer, coroutine frame, child actor, and subscription. Treat every suspension as a point at which actor state can change. Distinguish a delivery notification from application completion.

For lifetime changes, account for late events while cancellation is pending and for cleanup during forced teardown. For a tablet, identify which state is durable and which state must be reconstructed after restart; actor serialization does not itself provide persistence.

Select tests from the affected component. Add runtime tests when changing runtime guarantees, and component tests when changing a component protocol. Follow active build instructions for the target. Update the canonical topic page if the observable contract changes.
