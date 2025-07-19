#include "cpu_manager.h"
#include "executor_pool_jail.h"
#include "mon_stats.h"
#include "probes.h"
#include "debug.h"

#include "executor_pool_basic.h"
#include "executor_pool_io.h"
#include "executor_pool_shared.h"

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    TCpuManager::TCpuManager(THolder<TActorSystemSetup>& setup)
        : ExecutorPoolCount(setup->GetExecutorsCount())
        , Config(setup->CpuManager)
    {
        if (setup->Executors) { // Explicit mode w/o united pools
            Executors.Reset(setup->Executors.Release());
        } else {
            Setup();
        }
    }

    TCpuManager::~TCpuManager() {
    }

    void TCpuManager::SetupShared() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::SetupShared");
        bool hasSharedThread = false;
        for (TBasicExecutorPoolConfig& cfg : Config.Basic) {
            if (cfg.HasSharedThread) {
                hasSharedThread = true;
                break;
            }
        }
        if (!hasSharedThread) {
            ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::SetupShared: no shared threads, skipping");
            return;
        }

        TVector<TPoolShortInfo> poolInfos;

        std::vector<i16> poolIds(Config.Basic.size());
        std::iota(poolIds.begin(), poolIds.end(), 0);
        std::sort(poolIds.begin(), poolIds.end(), [&](i16 a, i16 b) {
            if (Config.Basic[a].Priority != Config.Basic[b].Priority) {
                return Config.Basic[a].Priority > Config.Basic[b].Priority;
            }
            return Config.Basic[a].PoolId < Config.Basic[b].PoolId;
        });

        i16 sht = 1;
        for (ui32 i = 0; i < Config.Basic.size(); ++i) {
            i16 sharedThreadCount = Config.Basic[poolIds[i]].HasSharedThread ? sht : 0;
            if (sharedThreadCount) {
                sht = 1;
            }
            poolInfos.push_back(TPoolShortInfo{
                .PoolId = static_cast<i16>(Config.Basic[poolIds[i]].PoolId),
                .SharedThreadCount = sharedThreadCount,
                .ForeignSlots = Config.Basic[poolIds[i]].ForcedForeignSlotCount,
                .InPriorityOrder = true,
                .PoolName = Config.Basic[poolIds[i]].PoolName,
                .ForcedForeignSlots = Config.Basic[poolIds[i]].ForcedForeignSlotCount > 0,
            });
        }
        for (ui32 i = 0; i < Config.IO.size(); ++i) {
            poolInfos.push_back(TPoolShortInfo{
                .PoolId = static_cast<i16>(Config.IO[i].PoolId),
                .SharedThreadCount = 0,
                .ForeignSlots = 0,
                .InPriorityOrder = false,
                .PoolName = Config.IO[i].PoolName
            });
        }
        Shared = std::make_unique<TSharedExecutorPool>(Config.Shared, poolInfos);

        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::SetupShared: created");
    }

    void TCpuManager::Setup() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Setup");
        TAffinity available;
        available.Current();

        Config.Shared.SoftProcessingDurationTs = Us2Ts(10'000);
        SetupShared();

        if (Config.Jail) {
            Jail = std::make_unique<TExecutorPoolJail>(ExecutorPoolCount, *Config.Jail);
        }

        std::vector<i16> poolsWithSharedThreads;
        for (TBasicExecutorPoolConfig& cfg : Config.Basic) {
            if (cfg.HasSharedThread) {
                poolsWithSharedThreads.push_back(cfg.PoolId);
            }
        }

        ui64 ts = GetCycleCountFast();
        Harmonizer = MakeHarmonizer(ts);
        Harmonizer->SetSharedPool(Shared.get());

        Executors.Reset(new TAutoPtr<IExecutorPool>[ExecutorPoolCount]);

        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx].Reset(CreateExecutorPool(excIdx));
            bool ignoreThreads = dynamic_cast<TIOExecutorPool*>(Executors[excIdx].Get());
            TSelfPingInfo *pingInfo = (excIdx < Config.PingInfoByPool.size()) ? &Config.PingInfoByPool[excIdx] : nullptr;
            ignoreThreads &= (Shared != nullptr);
            Harmonizer->AddPool(Executors[excIdx].Get(), pingInfo, ignoreThreads);
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Setup: created");
    }

    void TCpuManager::PrepareStart(TVector<NSchedulerQueue::TReader*>& scheduleReaders, TActorSystem* actorSystem) {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::PrepareStart");
        NSchedulerQueue::TReader* readers;
        ui32 readersCount = 0;
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Y_ABORT_UNLESS(Executors[excIdx].Get() != nullptr, "Executor pool is nullptr excIdx %" PRIu32, excIdx);
            Executors[excIdx]->Prepare(actorSystem, &readers, &readersCount);
            for (ui32 i = 0; i != readersCount; ++i, ++readers) {
                scheduleReaders.push_back(readers);
            }
        }
        if (Shared) {
            ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::PrepareStart: prepare shared");
            Shared->Prepare(actorSystem, &readers, &readersCount);
            for (ui32 i = 0; i != readersCount; ++i, ++readers) {
                scheduleReaders.push_back(readers);
            }
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::PrepareStart: prepared");
    }

    void TCpuManager::Start() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Start");
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->Start();
        }
        if (Shared) {
            Shared->Start();
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Start: started");
    }

    void TCpuManager::PrepareStop() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::PrepareStop");
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->PrepareStop();
        }
        if (Shared) {
            Shared->PrepareStop();
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::PrepareStop: prepared");
    }

    void TCpuManager::Shutdown() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Shutdown");
        if (Shared) {
            Shared->Shutdown();
        }
        for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
            Executors[excIdx]->Shutdown();
        }
        for (ui32 round = 0, done = 0; done < ExecutorPoolCount; ++round) {
            Y_ABORT_UNLESS(round < 10, "actorsystem cleanup could not be completed in 10 rounds");
            done = 0;
            for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
                if (Executors[excIdx]->Cleanup()) {
                    ++done;
                }
            }
        }
        if (Shared) {
            Shared->Cleanup();
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Shutdown: shutdown");
    }

    void TCpuManager::Cleanup() {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Cleanup");
        if (Shared) {
            Shared->Cleanup();
        }
        for (ui32 round = 0, done = 0; done < ExecutorPoolCount; ++round) {
            Y_ABORT_UNLESS(round < 10, "actorsystem cleanup could not be completed in 10 rounds");
            done = 0;
            for (ui32 excIdx = 0; excIdx != ExecutorPoolCount; ++excIdx) {
                if (Executors[excIdx]->Cleanup()) {
                    ++done;
                }
            }
        }
        if (Shared) {
            Shared.reset();
        }
        Executors.Destroy();

        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TCpuManager::Cleanup: destroyed");
    }

    IExecutorPool* TCpuManager::CreateExecutorPool(ui32 poolId) {
        for (TBasicExecutorPoolConfig& cfg : Config.Basic) {
            if (cfg.PoolId == poolId) {
                if (Shared) {
                    auto *pool = new TBasicExecutorPool(cfg, Harmonizer.get(), Jail.get());
                    Shared->SetBasicPool(pool);
                    pool->SetSharedPool(Shared.get());
                    return pool;
                } else {
                    return new TBasicExecutorPool(cfg, Harmonizer.get(), Jail.get());
                }
            }
        }
        for (TIOExecutorPoolConfig& cfg : Config.IO) {
            if (cfg.PoolId == poolId) {
                return new TIOExecutorPool(cfg);
            }
        }
        Y_ABORT("missing PoolId: %d", int(poolId));
    }

    TVector<IExecutorPool*> TCpuManager::GetBasicExecutorPools() const {
        TVector<IExecutorPool*> pools;
        for (ui32 idx = 0; idx < ExecutorPoolCount; ++idx) {
            if (auto basicPool = dynamic_cast<TBasicExecutorPool*>(Executors[idx].Get()); basicPool != nullptr) {
                pools.push_back(basicPool);
            }
        }
        return pools;
    }

    void TCpuManager::GetPoolStats(ui32 poolId, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy, TVector<TExecutorThreadStats>& sharedStatsCopy) const {
        if (poolId < ExecutorPoolCount) {
            Executors[poolId]->GetCurrentStats(poolStats, statsCopy);
        }
        if (Shared) {
            Shared->GetSharedStats(poolId, sharedStatsCopy);
        }
    }

    void TCpuManager::GetExecutorPoolState(i16 poolId, TExecutorPoolState &state) const {
        if (static_cast<ui32>(poolId) < ExecutorPoolCount) {
            Executors[poolId]->GetExecutorPoolState(state);
        }
    }

    void TCpuManager::GetExecutorPoolStates(std::vector<TExecutorPoolState> &states) const {
        states.resize(ExecutorPoolCount);
        for (i16 poolId = 0; poolId < static_cast<ui16>(ExecutorPoolCount); ++poolId) {
            GetExecutorPoolState(poolId, states[poolId]);
        }
    }

}
