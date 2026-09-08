# Actor System Subsystems

A subsystem is an `ISubSystem` object owned by one `TActorSystem`. It exposes a typed C++ API and participates in system startup and shutdown. Calling that API is a normal synchronous C++ call: the registry does not provide a mailbox, serialization, or an actor activation context. A subsystem can create actors to handle asynchronous work internally.

## Available implementations

The public subsystem classes are abstract interfaces. Create the supplied implementations with their factories, or implement the same interface in a mock.

| Interface | Installation and purpose |
|---|---|
| `TActorSystemStatsSubSystem` | Installed by the `TActorSystem` constructor unless already registered under this type. Provides executor pool, thread, and harmonizer statistics. Factory: `MakeActorSystemStatsSubSystem`. |
| `TCGroupV1StatsSubSystem` | Explicit registration with `MakeCGroupV1StatsSubSystem(config)`. Reads Linux cgroup v1 CPU, memory, I/O, and PID statistics. |
| `TCGroupV2StatsSubSystem` | Explicit registration with `MakeCGroupV2StatsSubSystem(config)`. Reads Linux cgroup v2 statistics, including pressure information. |
| `TCGroupOomSubSystem` | Explicit registration with `MakeCGroupOomSubSystem(config)`. Uses cgroup memory providers for threshold alerts and configured OOM trend windows. |

Only actor-system statistics are installed automatically. The cgroup implementations belong to the `ydb/library/actors/subsystems` build target. `ICGroupMemoryStatsProvider` is their version-neutral memory interface, not an additional automatically installed subsystem.

Cgroup readers perform filesystem I/O in `config.ExecutorPoolId`; select an existing I/O executor pool explicitly. The default pool ID is zero, which need not be an I/O pool in your setup. Readers cache immutable snapshots for one second by default; a zero `RefreshPeriod` disables caching. Replies preserve the request cookie, and a null snapshot means the provider is unavailable or the read failed. Calls require a running subsystem and a local recipient actor.

The supplied OOM implementation prefers both providers, then v2 alone, then v1 alone. If neither is registered, dependency resolution removes OOM at startup. With both providers, it can fall back when the first hierarchy has no memory controller. Its cgroup identity and trend-window configuration are fixed for the running instance; changing the process's cgroup stops monitoring. See the [configuration and API](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/subsystems/cgroup/cgroup_oom.h) for thresholds, subscription semantics, and trend requests.

## Registration and lookup

Register subsystems while configuring `TActorSystemSetup`, or on a constructed `TActorSystem` before calling `Start()`. For example, after configuring the executor pools and scheduler:

```cpp
#include <ydb/library/actors/subsystems/cgroup/cgroup_v2.h>
#include <ydb/library/actors/subsystems/cgroup/cgroup_oom.h>

NActors::TCGroupV2StatsConfig statsConfig;
statsConfig.ExecutorPoolId = ioPoolId;
setup->RegisterSubSystem(NActors::MakeCGroupV2StatsSubSystem(statsConfig));

NActors::TCGroupOomConfig oomConfig;
oomConfig.ExecutorPoolId = systemPoolId;
setup->RegisterSubSystem(NActors::MakeCGroupOomSubSystem(oomConfig));
```

The factory's return type determines the registry key. Lookup uses the exact same type:

```cpp
auto* stats = actorSystem.GetSubSystem<NActors::TCGroupV2StatsSubSystem>();
if (stats) {
    stats->ReadStats(recipient, cookie); // after Start() has returned
}
```

`GetSubSystem<T>()` returns a borrowed pointer, or null if the slot is absent. The named convenience getters, such as `GetActorSystemStats(actorSystem)` and `GetCGroupV2StatsSubSystem(actorSystem)`, abort if the subsystem is absent. Their no-argument forms require a current actor activation; use the explicit system form from other threads.

There is one owned object per type key. Registering the same key replaces and destroys its previous object. Lookup does not search the inheritance hierarchy or create aliases for base classes. Runtime type IDs are process-local indices; do not persist them or use them as wire identifiers.

## Creating a subsystem and a mock

Define the API separately from the implementation. Use public, unambiguous, non-virtual inheritance from `ISubSystem`, because typed lookup uses a static downcast. For example:

```cpp
#include <ydb/library/actors/core/actorsystem.h>

class TRequestLimitSubSystem : public NActors::ISubSystem {
public:
    virtual ui32 GetLimit() const = 0;
};

class TFixedRequestLimitSubSystem final : public TRequestLimitSubSystem {
public:
    explicit TFixedRequestLimitSubSystem(ui32 limit)
        : Limit(limit)
    {}

    ui32 GetLimit() const override {
        return Limit;
    }

private:
    const ui32 Limit;
};

class TMockRequestLimitSubSystem final : public TRequestLimitSubSystem {
public:
    ui32 GetLimit() const override {
        return 7;
    }
};
```

Specify the interface key when registering a concrete implementation or mock:

```cpp
setup->RegisterSubSystem<TRequestLimitSubSystem>(
    std::make_unique<TFixedRequestLimitSubSystem>(100));

// In a test, replace the production object before constructing the system.
setup->RegisterSubSystem<TRequestLimitSubSystem>(
    std::make_unique<TMockRequestLimitSubSystem>());

NActors::TActorSystem actorSystem(setup);
auto* limits = actorSystem.GetSubSystem<TRequestLimitSubSystem>();
Y_ABORT_UNLESS(limits && limits->GetLimit() == 7);
```

Omitting `<TRequestLimitSubSystem>` would register the mock under its own type; consumers and dependencies using `TRequestLimitSubSystem` would not see it. The same rule allows a mock `TActorSystemStatsSubSystem` to replace automatic statistics: register it under that interface in the setup, or replace the default on the constructed system before `Start()`.

Implement every pure virtual API method in a mock. Lifecycle hooks inherited from `ISubSystem` do nothing by default, so the mock does not start production actors or access a real CPU manager or cgroup filesystem. An asynchronous mock must still obey its API's event type, recipient, cookie, ownership, and lifetime contract.

## Dependencies and lifecycle

Override `GetDependencies()` to declare prerequisites using their registry keys. An empty result means no dependencies. For example:

```cpp
NActors::TSubSystemDependencies GetDependencies() const override {
    return NActors::DependsOn<TRequestLimitSubSystem>() &&
        (NActors::DependsOn<NActors::TCGroupV2StatsSubSystem>() ||
         NActors::DependsOn<NActors::TCGroupV1StatsSubSystem>());
}
```

`&&` requires all listed subsystems; `||` offers alternatives. At `Start()`, resolution removes and destroys subsystems with unsatisfied dependencies, including transitively unavailable ones. It searches available alternatives in declaration order for a globally acyclic dependency plan. An unavoidable cycle aborts startup. Keep expressions small: construction aborts if the expanded disjunctive normal form would exceed 100 alternatives.

`OnDependenciesResolved` receives the selected alternative's type IDs and borrowed instance pointers in declaration order. Only selected dependencies establish lifecycle ordering; a different registered alternative may exist without being ordered before the consumer. Independent subsystems have no application-level ordering guarantee.

Startup follows the resolved dependency order. Both stop-hook passes traverse that same order in reverse, like unwinding a stack: consumers stop before the subsystems they use. This order comes from dependencies, not registration time. In the example below, `A` depends on `B`, and `B` depends on `C`.

| Phase | Order and runtime state |
|---|---|
| `OnDependenciesResolved` | `C → B → A`, before any start hook. All selected instances exist but have not run start hooks. |
| `OnBeforeStart` | `C → B → A`: dependencies before consumers, before executor and scheduler preparation. Prepare synchronous state here. |
| `OnAfterStart` | `C → B → A`, after executor and scheduler startup. Subsystems can register their internal actors here. |
| `OnBeforeStop` | `A → B → C`: consumers before dependencies, while executor threads still run; before deferred pre-stop callbacks. Stop accepting work and initiate cleanup. |
| `OnAfterStop` | `A → B → C`, after scheduler stop and executor shutdown. Release resources that no longer require actor execution. |

Each hook is a separate pass over all subsystems. First, `OnBeforeStop` runs for `A`, then `B`, then `C`; next, deferred pre-stop callbacks run and the scheduler and executors shut down; finally, `OnAfterStop` runs for `A`, then `B`, then `C`. Thus, `A` can use `B` during its `OnBeforeStop` before `B` begins its own stop hook. This hook order does not guarantee C++ object destruction order.

Hooks run synchronously in the thread calling `Start()` or `Stop()`, without a provided actor activation context. Use the supplied `TActorSystem` for actor operations. Executor threads can process actors before all `OnAfterStart` callbacks finish. If early actors need a subsystem, provide an explicit readiness protocol. External callers can wait for `Start()` to return.

## Limits and validation

- Registration must be serialized with startup and lookup. The registry has no lock; the atomic start gate does not make concurrent registration safe. Registration after `Start()` begins aborts, including after `Stop()`.
- API calls inherit the implementation's threading contract. Shared subsystem state needs synchronization or internal actor ownership; actor mailbox serialization does not protect ordinary calls from different actors.
- Borrowed pointers become invalid on replacement, dependency pruning, or destruction of the actor system. Do not retain setup-time pointers across dependency resolution without ensuring the subsystem survives.
- `Start()` is one-shot and `Stop()` is idempotent. A subsystem cannot be restarted or hot-swapped through this API. Hook failures have no automatic rollback protocol; design initialization and shutdown accordingly.
- `Stop()` does not destroy subsystem objects. Dependency order applies to hooks, not C++ destructors. Finish work requiring dependencies in the stop hooks, and do not rely on destructor order or actor TLS during destruction.
- Sending poison in `OnBeforeStop` is not proof that an internal actor processed it. Use explicit completion if cleanup must finish before executor shutdown; see [Failure and Shutdown](failure-and-shutdown.md).

For registry identity, replacement, dependencies, and lifecycle tests, use `ydb/library/actors/core/ut` (`TSubSystemTest`). Cgroup implementation and mock integration tests are in `ydb/library/actors/subsystems/ut` (`TCGroupStatsSubSystemTest`). Follow the active build instructions in [Testing Actors](testing.md).

The source contract is in [core/subsystem.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/subsystem.h), [core/subsystem.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/subsystem.cpp), and [core/actorsystem.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/actorsystem.cpp).
