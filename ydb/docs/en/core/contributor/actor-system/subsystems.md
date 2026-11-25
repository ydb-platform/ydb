# Actor System Subsystems

When several actors need statistics for the same cgroup, each can request a snapshot from a shared subsystem. The subsystem reads the files and caches the result. Actors look it up with `GetSubSystem<T>()`, so they need no knowledge of the reader actor or the pool it runs in.

A subsystem implements `ISubSystem` and belongs to a `TActorSystem`. You choose the implementation when configuring the system and can substitute a mock in a test. All actors in that system get the same object when they look up the same type. A second actor system in the same process owns its own instance. The actor system runs startup and shutdown hooks in dependency order.

A subsystem method runs on the caller's thread like any other C++ method. The implementation is responsible for synchronization if several actors call it concurrently. For example, `ReadStats` forwards the request to an internal actor so the caller can finish handling its event without waiting for file I/O.

## Available implementations

| Interface | Use it to | How to install |
|---|---|---|
| `TActorSystemStatsSubSystem` | Retrieve executor pool, thread, and harmonizer statistics. | The `TActorSystem` constructor calls `MakeActorSystemStatsSubSystem` unless this type is already registered. |
| `TCGroupV1StatsSubSystem` | Read Linux cgroup v1 CPU, memory, I/O, and PID statistics. | Register the result of `MakeCGroupV1StatsSubSystem(config)`. |
| `TCGroupV2StatsSubSystem` | Read Linux cgroup v2 statistics, including resource pressure. | Register the result of `MakeCGroupV2StatsSubSystem(config)`. |
| `TCGroupOomSubSystem` | Receive alerts as cgroup memory usage approaches its limit and request forecasts of time to OOM. | Register the result of `MakeCGroupOomSubSystem(config)` and at least one cgroup statistics subsystem. |

## Registration and lookup

Register subsystems in `TActorSystemSetup` or on a constructed `TActorSystem` before calling `Start()`. For example, to monitor a process's memory usage under cgroup v2, link the `ydb/library/actors/subsystems` target and register the two subsystems below. Select an existing I/O pool for file reads (`ioPoolId` in the example). The default is pool 0, whose role depends on your configuration. The monitor actor runs in `systemPoolId`.

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

Once the actor system has started, request a snapshot through the interface returned by the factory. Pass the ID of the local actor that should receive the reply.

```cpp
auto* stats = actorSystem.GetSubSystem<NActors::TCGroupV2StatsSubSystem>();
if (stats) {
    stats->ReadStats(recipient, cookie); // after Start() has returned
}
```

The `TEvCGroupV2Stats` reply carries the same `cookie` so the recipient can match it to the request. If cgroup v2 is unavailable or the read fails, the `Stats` field is null. By default, requests within one second share an immutable snapshot. Set `statsConfig.RefreshPeriod` to zero if each request needs fresh data.

For memory alerts, an actor subscribes once with `TCGroupOomSubSystem::Subscribe`. The monitor periodically requests memory statistics through `ICGroupMemoryStatsProvider`, the interface shared by the v1 and v2 subsystems. For example, when the fraction of memory in use reaches the configured threshold, subscribers receive a `TEvCGroupOomAlert` event.

If both statistics subsystems are registered, the monitor queries v2 first and tries v1 if v2 returns no memory data. If only one is registered, it uses that one. With neither registered, the actor system removes the monitor subsystem from the registry at startup. The monitor tracks one cgroup and stops polling if it detects that the process has moved to a different cgroup. Alert thresholds and forecast windows are set during configuration. See the [monitor configuration and subscription API](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/subsystems/cgroup/cgroup_oom.h).

`GetSubSystem<T>()` returns a borrowed pointer, or null if the slot is absent. The named convenience getters, such as `GetActorSystemStats(actorSystem)` and `GetCGroupV2StatsSubSystem(actorSystem)`, abort if the subsystem is absent. Their no-argument forms require a current actor activation; use the explicit system form from other threads.

There is one owned object per type key. Registering the same key replaces and destroys its previous object. Lookup does not search the inheritance hierarchy or create aliases for base classes. Runtime type IDs are process-local indices; do not persist them or use them as wire identifiers.

## Creating a subsystem and a mock

Suppose several actors enforce a shared request limit. To change that limit in tests without changing the actors, give them an interface with a `GetLimit()` method. Use public, unambiguous, non-virtual inheritance from `ISubSystem`, because typed lookup uses a static downcast. For example:

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

Override `GetDependencies()` to declare prerequisites using their registry keys. An empty result means no dependencies. For example, the memory monitor declares which statistics subsystems it can use. The first alternative lets it fall back to v1 when v2 has no data.

```cpp
NActors::TSubSystemDependencies GetDependencies() const override {
    using namespace NActors;
    return (DependsOn<TCGroupV2StatsSubSystem>() && DependsOn<TCGroupV1StatsSubSystem>()) ||
        DependsOn<TCGroupV2StatsSubSystem>() ||
        DependsOn<TCGroupV1StatsSubSystem>();
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
