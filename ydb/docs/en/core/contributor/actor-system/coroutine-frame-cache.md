# Coroutine Frame Cache

The coroutine frame cache is a per-worker set of free lists that reuses memory blocks for stackless coroutine frames. It reduces heap-allocation overhead without changing frame ownership, actor lifetime, cancellation, or forced teardown; those rules are described in [Coroutine Actors](coroutine-actors.md) and [Failure and Shutdown](failure-and-shutdown.md). Read this page when you configure the cache budget, read its statistics, or test allocation behavior.

## Which frames use the cache

Top-level actor coroutines and every `async<T>` frame, including free functions, const members, and coroutine lambdas, allocate through `TAsyncFrameCache::Allocate` and release through sized `TAsyncFrameCache::Free`. The compiler may elide a coroutine allocation entirely.

Each executor thread of the BASIC, IO, and shared pools has one `TAllocationCache<TAsyncFrameCacheTag>`, shared by all actors running on that thread. A shared worker that switches pools keeps the same cache. Outside an executor thread there is no current cache, and frames use the heap.

## Size classes and budget

The cache keeps seven last-in, first-out (LIFO) lists of idle blocks with capacities of 1, 2, 4, 8, 16, 32, and 64 KiB:

- a request of at most 1 KiB uses the 1 KiB list, and a larger request of at most 64 KiB uses the smallest list that fits;
- a request over 64 KiB uses the heap directly and is never retained;
- a request of at most 64 KiB is rounded to its list capacity even outside a worker or with a zero budget, so any worker can later retain that block.

The budget limits the total rounded capacity of idle blocks that one worker retains. A returned block that does not fit into the remaining budget is freed to the heap; there is no separate limit per list. The budget excludes active frames, cache metadata, and allocator bookkeeping, so it does not bound total coroutine memory. Allocations keep `__STDCPP_DEFAULT_NEW_ALIGNMENT__`; extended coroutine-frame alignment is unsupported.

A frame stores no pointer to the cache that allocated it. Sized deletion returns the block to the cache of the thread that frees it, subject to that cache's budget, or to the heap outside an executor thread. A frame can therefore be freed on another worker, or during shutdown without actor TLS. When an executor thread stops, its cache is unbound, so later frees on that thread go to the heap. Idle blocks retained by worker caches are released only when the actor system is destroyed; live frames are never freed by the cache.

## Configuring the budget

The default budget is 4 MiB per worker (`TAsyncFrameCache::DefaultSizeBytes`). A budget of `0` disables retention: the cache still exists, but every returned block goes to the heap. To set a different budget, register the `TAsyncFrameCache` subsystem in `TActorSystemSetup` before the actor system starts:

```cpp
setup->RegisterSubSystem(std::make_unique<TAsyncFrameCache>(8 * 1024 * 1024));
```

If the setup does not register this subsystem, the actor system registers one with the default budget. For {{ ydb-short-name }} servers, `NKikimrConfig::TActorSystemConfig::AsyncFrameCacheSizeBytes` (default `4194304`) is passed to this subsystem. Worker caches are created when the actor system starts, so a budget change takes effect only after a restart.

## Statistics

On an executor thread, `TAllocationCache<TAsyncFrameCacheTag>::GetCurrent()->GetStats()` returns the statistics of that worker's cache: `HeapAllocations` (cache misses, including requests over 64 KiB), `CachedBytes` (rounded capacity of idle blocks), `CachedFrames`, and `SizeClasses` (number of non-empty lists). Call it only on the owner thread. Measure on the worker that performs the workload and warm the cache up on that same worker.

For a whole actor system, `actorSystem->GetSubSystem<TAllocationCacheSubSystem>()->GetCachedStats(TAsyncFrameCache::FamilyId())` returns total `CachedFrames` and `CachedBytes`. After the actor system has started, it can be called from any thread without a lock, but it must finish before the actor system is destroyed.

The total counts every executor thread of that actor system once, including stopped threads until the actor system is destroyed. It excludes active frames, other actor systems, and caches bound by tests. Workers update the counts with relaxed loads and stores, so the total is approximate while workers allocate or free and exact when caches are idle.

The actor-system stats collector publishes this total once per collection cycle, normally every second, as non-derivative gauges without thread, pool, or size-class breakdown:

| Counter group | Gauges |
|---|---|
| `counters=utils/subsystem=async_frame_cache` | `CachedFrames`, `CachedBytes` |
| `counters=utils/subsystem=allocation_cache/family=AsyncFrames` | `CachedBlocks`, `CachedBytes` |

## Sanitizers

Cache reuse remains enabled in sanitizer builds. Under AddressSanitizer (ASAN), idle blocks are poisoned in full, including the list link; the cache temporarily unpoisons that link for its own bookkeeping. A live allocation exposes only the requested bytes, and the rest of the rounded capacity stays poisoned, including allocations made outside workers or with a zero budget. Under MemorySanitizer (MSAN), reused frame contents are marked uninitialized again. These annotations detect stale accesses while a block is idle; they cannot detect a stale pointer after the same address has been legitimately reused.

## Testing allocation behavior

A deterministic test can bind its own cache to the current thread instead of relying on executor scheduling:

```cpp
TAllocationCache<TAsyncFrameCacheTag> cache(64 * 1024);
TScopedAllocationCache<TAsyncFrameCacheTag> guard(&cache);
```

`TScopedAllocationCache` is declared in `ydb/library/actors/testlib/scoped_allocation_cache.h`. A nested guard restores the previous binding when it is destroyed. Allocation, deallocation, and `GetStats()` must stay on the binding thread; `cache.GetCachedStats()` samples idle-block counts from any thread. Existing coverage is in `ydb/library/actors/async/ut/frame_cache_ut.cpp` and `ydb/library/actors/core/ut/allocation_cache_ut.cpp`.

## Implementation

Actors do not own frame caches: `IActor::GetAsyncFrameCache()` and origin-cache `LiveFrames` accounting have been removed, and no actor argument selects a cache. The frame cache is one family of a generic allocation-cache subsystem; another component can define its own tag and register a `TAllocationCacheFamily` subsystem with a separate budget.

| File under `ydb/library/actors/core/` | Contents |
|---|---|
| `allocation_cache.h` | Size classes, allocation and release, sanitizer annotations, statistics |
| `subsystems/allocation_cache.h` | `TAllocationCacheSubSystem`, the caches of each executor thread, `TAllocationCacheFamily` |
| `subsystems/allocation_cache_tls.h` | `TAllocationCache<TTag>::GetCurrent()` |
| `subsystems/async_frame_cache.h` | `TAsyncFrameCacheTag` and `TAsyncFrameCache` |
