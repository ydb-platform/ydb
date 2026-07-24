#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_shared.h>
#include <ydb/library/actors/core/thread_context.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <map>
#include <memory>
#include <optional>
#include <vector>

namespace {

using namespace NActors;

constexpr size_t MaxOperations = 384;
constexpr ui32 MaxPools = 4;
constexpr ui32 MailboxesPerPool = 6;

struct TOverriddenThreadContext {
    std::optional<bool> IsNeededToWaitNextActivation;
};

class TThreadContextGuard {
public:
    TThreadContextGuard(TThreadContext* newContext, std::optional<TOverriddenThreadContext> overriddenContext = std::nullopt)
        : OriginalContext(TlsThreadContext)
    {
        TlsThreadContext = newContext;
        if (overriddenContext) {
            PreviousChangedContext = OverrideContext(newContext, *overriddenContext);
        }
    }

    ~TThreadContextGuard() {
        if (PreviousChangedContext) {
            OverrideContext(TlsThreadContext, *PreviousChangedContext);
        }
        TlsThreadContext = OriginalContext;
    }

private:
    static TOverriddenThreadContext OverrideContext(TThreadContext* context, TOverriddenThreadContext overriddenContext) {
        TOverriddenThreadContext previousContext;
        if (overriddenContext.IsNeededToWaitNextActivation) {
            previousContext.IsNeededToWaitNextActivation = std::exchange(
                context->ExecutionContext.IsNeededToWaitNextActivation,
                *overriddenContext.IsNeededToWaitNextActivation);
        }
        return previousContext;
    }

private:
    TThreadContext* OriginalContext = nullptr;
    std::optional<TOverriddenThreadContext> PreviousChangedContext;
};

struct TWorkerIdentity {
    IExecutorPool* Pool = nullptr;
    TWorkerId WorkerId = 0;
};

class TThreadContexts {
private:
    using TContextKey = std::pair<IExecutorPool*, TWorkerId>;

public:
    TThreadContexts(const std::vector<IExecutorPool*>& pools, TSharedExecutorPool* sharedPool)
        : Pools(pools)
        , SharedPool(sharedPool)
    {
        ExecutorThreadStats.resize(SumThreadCount());
        InitContexts();
    }

    TThreadContext* GetContext(TWorkerIdentity workerIdentity) {
        auto it = Contexts.find(TContextKey(workerIdentity.Pool, workerIdentity.WorkerId));
        Y_ABORT_UNLESS(it != Contexts.end());
        return it->second.get();
    }

private:
    TExecutionStats* MakeExecutionStats(TWorkerIdentity workerIdentity) {
        auto key = TContextKey(workerIdentity.Pool, workerIdentity.WorkerId);
        auto it = ExecutionStats.find(key);
        if (it == ExecutionStats.end()) {
            const ui32 nextIdx = ExecutionStats.size();
            it = ExecutionStats.emplace(key, TExecutionStats()).first;
            it->second.Stats = &ExecutorThreadStats[nextIdx];
        }
        return &it->second;
    }

    ui32 SumThreadCount() const {
        ui32 threadCount = 0;
        for (auto* pool : Pools) {
            threadCount += pool->GetMaxFullThreadCount();
        }
        if (SharedPool) {
            threadCount += SharedPool->GetThreads();
        }
        return threadCount;
    }

    void InitContexts() {
        for (auto* pool : Pools) {
            for (TWorkerId workerId = 0; workerId < pool->GetMaxFullThreadCount(); ++workerId) {
                auto key = TContextKey(pool, workerId);
                Contexts[key] = std::make_unique<TThreadContext>(workerId, pool, nullptr);
                Contexts[key]->ExecutionStats = MakeExecutionStats({pool, workerId});
            }
        }
        if (!SharedPool) {
            return;
        }

        TPoolManager poolManager = SharedPool->GetPoolManager();
        for (const auto poolId : poolManager.PriorityOrder) {
            for (TWorkerId workerId = poolManager.PoolThreadRanges[poolId].Begin;
                    workerId < poolManager.PoolThreadRanges[poolId].End;
                    ++workerId) {
                auto key = TContextKey(SharedPool, workerId);
                Contexts[key] = std::make_unique<TThreadContext>(workerId, Pools[poolId], SharedPool);
                Contexts[key]->ExecutionStats = MakeExecutionStats({SharedPool, workerId});
            }
        }
    }

private:
    std::map<TContextKey, std::unique_ptr<TThreadContext>> Contexts;
    std::vector<IExecutorPool*> Pools;
    std::map<TContextKey, TExecutionStats> ExecutionStats;
    std::vector<TExecutorThreadStats> ExecutorThreadStats;
    TSharedExecutorPool* SharedPool = nullptr;
};

class TThreadEmulator {
public:
    TThreadEmulator(const std::vector<IExecutorPool*>& pools, TSharedExecutorPool* sharedPool)
        : Contexts(pools, sharedPool)
    {}

    TMailbox* GetReadyActivation(
            TWorkerIdentity workerIdentity,
            ui64 revolvingReadCounter,
            std::optional<TOverriddenThreadContext> overriddenContext = std::nullopt) {
        TThreadContext* context = Contexts.GetContext(workerIdentity);
        TThreadContextGuard guard(context, overriddenContext);
        return context->IsShared()
            ? context->SharedPool()->GetReadyActivation(revolvingReadCounter)
            : context->Pool()->GetReadyActivation(revolvingReadCounter);
    }

    void ScheduleActivation(
            TWorkerIdentity workerIdentity,
            IExecutorPool* pool,
            TMailbox* mailbox,
            ui64 revolvingWriteCounter,
            std::optional<TOverriddenThreadContext> overriddenContext = std::nullopt) {
        Y_ABORT_UNLESS(pool);
        Y_ABORT_UNLESS(mailbox);
        TThreadContext* context = Contexts.GetContext(workerIdentity);
        TThreadContextGuard guard(context, overriddenContext);
        pool->ScheduleActivationEx(mailbox, revolvingWriteCounter);
    }

    IExecutorPool* GetActualPool(TWorkerIdentity workerIdentity) {
        return Contexts.GetContext(workerIdentity)->Pool();
    }

private:
    TThreadContexts Contexts;
};

void PreparePool(IExecutorPool* pool) {
    NSchedulerQueue::TReader* scheduleReaders = nullptr;
    ui32 scheduleSz = 0;
    pool->Prepare(nullptr, &scheduleReaders, &scheduleSz);
}

void TieBasicPoolsAndSharedPool(const std::vector<std::unique_ptr<TBasicExecutorPool>>& pools, TSharedExecutorPool* sharedPool) {
    for (auto& pool : pools) {
        pool->SetSharedPool(sharedPool);
        sharedPool->SetBasicPool(pool.get());
    }
}

struct TExecutorHarness {
    TVector<std::unique_ptr<TBasicExecutorPool>> BasicPools;
    std::unique_ptr<TSharedExecutorPool> SharedPool;
    std::vector<IExecutorPool*> PoolPtrs;
    std::unique_ptr<TThreadEmulator> Emulator;
    TVector<TVector<TMailbox*>> Mailboxes;
    TVector<TWorkerIdentity> Workers;
    bool HasShared = false;

    explicit TExecutorHarness(FuzzedDataProvider& fdp) {
        HasShared = fdp.ConsumeBool();
        const ui32 poolCount = HasShared ? fdp.ConsumeIntegralInRange<ui32>(2, MaxPools) : 1;
        const ui32 basicThreads = fdp.ConsumeIntegralInRange<ui32>(1, 2);

        if (HasShared) {
            std::vector<TPoolShortInfo> infos;
            for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
                infos.push_back(TPoolShortInfo{
                    .PoolId = static_cast<i16>(poolId),
                    .SharedThreadCount = 1,
                    .ForeignSlots = static_cast<i16>(fdp.ConsumeIntegralInRange<ui32>(0, poolCount)),
                    .InPriorityOrder = true,
                    .PoolName = "fuzz-pool-" + ToString(poolId),
                });
            }
            SharedPool = std::make_unique<TSharedExecutorPool>(TSharedExecutorPoolConfig{
                .Threads = poolCount,
                .United = fdp.ConsumeBool(),
            }, infos);
        }

        for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
            const ui16 minLocalQueueSize = static_cast<ui16>(fdp.ConsumeIntegralInRange<ui32>(0, 8));
            const ui16 maxLocalQueueSize = static_cast<ui16>(fdp.ConsumeIntegralInRange<ui32>(minLocalQueueSize, 16));
            BasicPools.push_back(std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
                .PoolId = poolId,
                .PoolName = "fuzz-basic-" + ToString(poolId),
                .Threads = basicThreads,
                .TimePerMailbox = TDuration::MicroSeconds(fdp.ConsumeIntegralInRange<ui32>(1, 5000)),
                .EventsPerMailbox = fdp.ConsumeIntegralInRange<ui32>(1, 256),
                .Priority = static_cast<i16>(poolId),
                .HasSharedThread = HasShared,
                .UseRingQueue = fdp.ConsumeBool(),
                .AllThreadsAreShared = false,
                .MinLocalQueueSize = minLocalQueueSize,
                .MaxLocalQueueSize = maxLocalQueueSize,
            }, nullptr));
            PoolPtrs.push_back(BasicPools.back().get());
        }

        if (SharedPool) {
            TieBasicPoolsAndSharedPool(BasicPools, SharedPool.get());
        }
        for (auto* pool : PoolPtrs) {
            PreparePool(pool);
        }
        if (SharedPool) {
            PreparePool(SharedPool.get());
        }

        Mailboxes.resize(poolCount);
        for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
            for (ui32 i = 0; i < MailboxesPerPool; ++i) {
                Mailboxes[poolId].push_back(BasicPools[poolId]->GetMailboxTable()->Allocate());
            }
        }

        for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
            for (TWorkerId workerId = 0; workerId < BasicPools[poolId]->GetMaxFullThreadCount(); ++workerId) {
                Workers.push_back({BasicPools[poolId].get(), workerId});
            }
        }
        if (SharedPool) {
            for (TWorkerId workerId = 0; workerId < SharedPool->GetThreads(); ++workerId) {
                Workers.push_back({SharedPool.get(), workerId});
            }
        }
        Emulator = std::make_unique<TThreadEmulator>(PoolPtrs, SharedPool.get());
    }

    TWorkerIdentity PickWorker(FuzzedDataProvider& fdp) {
        return Workers[fdp.ConsumeIntegralInRange<size_t>(0, Workers.size() - 1)];
    }

    IExecutorPool* PickPool(FuzzedDataProvider& fdp) {
        return PoolPtrs[fdp.ConsumeIntegralInRange<size_t>(0, PoolPtrs.size() - 1)];
    }

    TMailbox* PickMailbox(IExecutorPool* pool, FuzzedDataProvider& fdp) {
        const ui32 poolId = pool->PoolId;
        return Mailboxes[poolId][fdp.ConsumeIntegralInRange<size_t>(0, Mailboxes[poolId].size() - 1)];
    }
};

void CheckPool(IExecutorPool* pool, bool hasMailboxLimits = true) {
    TExecutorPoolState state;
    pool->GetExecutorPoolState(state);

    TExecutorPoolStats stats;
    TVector<TExecutorThreadStats> threadStats;
    pool->GetCurrentStats(stats, threadStats);
    if (hasMailboxLimits) {
        Y_ABORT_UNLESS(pool->TimePerMailboxTs() > 0);
        Y_ABORT_UNLESS(pool->EventsPerMailbox() > 0);
    }
    pool->GetName();
}

void RunExecutorPoolFuzz(FuzzedDataProvider& fdp) {
    TExecutorHarness harness(fdp);
    TOverriddenThreadContext noWait{.IsNeededToWaitNextActivation = false};

    for (size_t step = 0; step < MaxOperations && fdp.remaining_bytes(); ++step) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 7)) {
            case 0:
            case 1:
            case 2: {
                auto worker = harness.PickWorker(fdp);
                IExecutorPool* pool = harness.HasShared && fdp.ConsumeBool()
                    ? harness.PickPool(fdp)
                    : harness.Emulator->GetActualPool(worker);
                harness.Emulator->ScheduleActivation(
                    worker,
                    pool,
                    harness.PickMailbox(pool, fdp),
                    fdp.ConsumeIntegralInRange<ui64>(0, 64),
                    fdp.ConsumeBool() ? std::optional<TOverriddenThreadContext>(noWait) : std::nullopt);
                break;
            }

            case 3:
            case 4: {
                auto worker = harness.PickWorker(fdp);
                TMailbox* activation = harness.Emulator->GetReadyActivation(
                    worker,
                    fdp.ConsumeIntegralInRange<ui64>(0, 64),
                    noWait);
                if (activation) {
                    Y_ABORT_UNLESS(activation->Hint);
                }
                break;
            }

            case 5:
                if (harness.SharedPool) {
                    const i16 poolId = static_cast<i16>(fdp.ConsumeIntegralInRange<ui32>(0, harness.BasicPools.size() - 1));
                    harness.SharedPool->SetForeignThreadSlots(poolId, static_cast<i16>(fdp.ConsumeIntegralInRange<ui32>(0, harness.BasicPools.size())));
                } else {
                    harness.BasicPools[0]->SetLocalQueueSize(static_cast<ui16>(fdp.ConsumeIntegralInRange<ui32>(
                        harness.BasicPools[0]->GetMinLocalQueueSize(),
                        harness.BasicPools[0]->GetMaxLocalQueueSize())));
                }
                break;

            case 6:
                CheckPool(harness.PickPool(fdp));
                if (harness.SharedPool) {
                    CheckPool(harness.SharedPool.get(), false);
                    std::vector<i16> owners;
                    std::vector<i16> allowed;
                    std::vector<i16> owned;
                    harness.SharedPool->FillThreadOwners(owners);
                    harness.SharedPool->FillForeignThreadsAllowed(allowed);
                    harness.SharedPool->FillOwnedThreads(owned);
                }
                break;

            case 7:
                TBasicExecutorPool* basic = harness.BasicPools[fdp.ConsumeIntegralInRange<size_t>(0, harness.BasicPools.size() - 1)].get();
                TWaitingStats<ui64> waiting;
                basic->GetWaitingStats(waiting);
                basic->CalcSpinPerThread(fdp.ConsumeIntegralInRange<ui64>(0, 10000));
                basic->ClearWaitingStats();
                basic->GetSemaphore();
                break;
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    RunExecutorPoolFuzz(fdp);
    return 0;
}
