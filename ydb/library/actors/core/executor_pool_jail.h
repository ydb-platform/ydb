#pragma once

#include "defs.h"
#include "config.h"

#include <ydb/library/actors/util/affinity.h>

#include <deque>
#include <optional>
#include <memory>


namespace NActors {

    struct TPoolAffinityGuards {
        std::unique_ptr<std::unique_ptr<TAffinityGuard>[]> Guards;
    };

    class TExecutorPoolJail {

        struct TPrisoner {
            ui16 PoolId;
            ui16 ThreadIdx;

            TPrisoner(ui16 poolId, ui16 threadIdx)
                : PoolId(poolId)
                , ThreadIdx(threadIdx)
            {}
        };

        ui32 PoolCount;
        std::unique_ptr<TPoolAffinityGuards[]> Pools;
        std::deque<TPrisoner> Prisoners;

        TCpuMask BaseCpuMask;
        TCpuMask JailCpuMask;
        ui32 UsedCoresForJail = 0;
        ui32 MaxThreadsInJailCore = 0;
    
    public:
        TExecutorPoolJail(ui32 poolCount, TExecutorPoolJailConfig config)
            : PoolCount(poolCount)
            , Pools(new TPoolAffinityGuards[PoolCount])
            , BaseCpuMask(config.JailAffinity)
            , MaxThreadsInJailCore(config.MaxThreadsInJailCore)
        {
        }

        void AddPool(ui32 poolId, const std::vector<size_t> &pids) {
            if (poolId < PoolCount) {
                return;
            }
            if (Pools[poolId].Guards) {
                return;
            }
            Pools[poolId].Guards.reset(new std::unique_ptr<TAffinityGuard>[pids.size()]);
            for (ui32 threadIdx = 0; threadIdx < pids.size(); ++threadIdx) {
                Pools[poolId].Guards[threadIdx].reset(new TAffinityGuard(nullptr, pids[threadIdx]));
            }
        }

        ui32 SetJailCpuMask(ui32 jailCpus) {
            JailCpuMask.ResetAll();
            bool baseIsEmpty = BaseCpuMask.IsEmpty();
            ui32 jailCoresLeft = jailCpus;
            for (i16 idx = BaseCpuMask.Size() - 1; idx >= 0 && jailCoresLeft; --idx) {
                if (baseIsEmpty) {
                    JailCpuMask.Set(idx);
                    jailCoresLeft--;
                } else if (BaseCpuMask.IsSet(idx)) {
                    JailCpuMask.Set(idx);
                    jailCoresLeft--;
                }
            }
            return jailCpus;
        }

        bool ReevaluateJailCpuMask() {
            if (MaxThreadsInJailCore) {
                ui32 arrestedThreads = Prisoners.size();
                ui32 newUsedCoreForJail = (arrestedThreads + MaxThreadsInJailCore - 1) / MaxThreadsInJailCore;
                UsedCoresForJail = newUsedCoreForJail - SetJailCpuMask(newUsedCoreForJail);
                return true;
            } else if (bool(Prisoners.size()) != UsedCoresForJail) {
                SetJailCpuMask(!Prisoners.empty());
                UsedCoresForJail = !Prisoners.empty();
                return true;
            }
            return false;
        }

        void Arrest(ui16 poolId, ui16 threadIdx) {
            Prisoners.emplace_back(poolId, threadIdx);
            if (ReevaluateJailCpuMask()) {
                for (auto &[prinonerPoolId, prisonerThreadIdx] : Prisoners) {
                    Pools[prinonerPoolId].Guards[prisonerThreadIdx]->SetCpuMask(JailCpuMask);
                }
            } else {
                Pools[poolId].Guards[threadIdx]->SetCpuMask(JailCpuMask);
            }
        }

        void Free(ui16 poolId, ui16 threadIdx) {
            for (auto it = Prisoners.begin(); it != Prisoners.end(); ++it) {
                if (poolId == it->PoolId && threadIdx == it->ThreadIdx) {
                    Pools[poolId].Guards[threadIdx]->Release();
                    Prisoners.erase(it);
                    break;
                }
            }
        }

    };

}