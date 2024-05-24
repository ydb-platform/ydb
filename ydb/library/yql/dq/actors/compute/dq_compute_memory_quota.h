
#pragma once

#include <util/system/mem_info.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>

#include <ydb/library/actors/core/log.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>

namespace NYql::NDq {
#define CAMQ_LOG_T(s) \
    LOG_TRACE_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define CAMQ_LOG_D(s) \
    LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define CAMQ_LOG_I(s) \
    LOG_INFO_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define CAMQ_LOG_W(s) \
    LOG_WARN_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)

    class THardMemoryLimitException : public NKikimr::TMemoryLimitExceededException {
    };

    class TDqMemoryQuota {
    public:
        TDqMemoryQuota(::NMonitoring::TDynamicCounters::TCounterPtr& mkqlMemoryQuota, ui64 initialMkqlMemoryLimit, const NYql::NDq::TComputeMemoryLimits& memoryLimits, NYql::NDq::TTxId txId, ui64 taskId, bool profileStats, bool canAllocateExtraMemory, NActors::TActorSystem* actorSystem)
            : MkqlMemoryQuota(mkqlMemoryQuota)
            , InitialMkqlMemoryLimit(initialMkqlMemoryLimit)
            , MkqlMemoryLimit(initialMkqlMemoryLimit)
            , MemoryLimits(memoryLimits)
            , TxId(txId)
            , TaskId(taskId)
            , ProfileStats(profileStats ? MakeHolder<TProfileStats>() : nullptr)
            , CanAllocateExtraMemory(canAllocateExtraMemory)
            , ActorSystem(actorSystem) {

            Y_ABORT_UNLESS(MemoryLimits.MemoryQuotaManager->AllocateQuota(MkqlMemoryLimit));
            if (MkqlMemoryQuota) {
                MkqlMemoryQuota->Add(MkqlMemoryLimit);
            }
        }

        ui64 GetMkqlMemoryLimit() const {
            return MkqlMemoryLimit;
        }

        void TrySetIncreaseMemoryLimitCallback(NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            if (CanAllocateExtraMemory) {
                alloc->Ref().SetIncreaseMemoryLimitCallback([this, alloc](ui64 limit, ui64 required) {
                    RequestExtraMemory(required - limit, alloc);
                });
            }
        }

        // This callback is created for testing purposes and will be enabled only with spilling.
        // Most likely this callback will be removed after KIKIMR-21481.
        void TrySetIncreaseMemoryLimitCallbackWithRSSControl(NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            if (!CanAllocateExtraMemory) return;
            const ui64 limitRSS = std::numeric_limits<ui64>::max();
            const ui64 criticalRSSValue = limitRSS / 100 * 80;

            alloc->Ref().SetIncreaseMemoryLimitCallback([this, alloc](ui64 limit, ui64 required) {
                RequestExtraMemory(required - limit, alloc);
                
                ui64 currentRSS = NMemInfo::GetMemInfo().RSS;
                if (currentRSS > criticalRSSValue) {
                    alloc->SetMaximumLimitValueReached(true);
                }
            });
        }

        void TryShrinkMemory(NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            if (alloc->GetAllocated() - alloc->GetUsed() > MemoryLimits.MinMemFreeSize) {
                alloc->ReleaseFreePages();
                auto newLimit = std::max(alloc->GetAllocated(), InitialMkqlMemoryLimit);
                if (MkqlMemoryLimit > newLimit) {
                    auto freedSize = MkqlMemoryLimit - newLimit;
                    MkqlMemoryLimit = newLimit;
                    alloc->SetLimit(newLimit);
                    MemoryLimits.MemoryQuotaManager->FreeQuota(freedSize);
                    if (MkqlMemoryQuota) {
                        MkqlMemoryQuota->Sub(freedSize);
                    }
                    CAMQ_LOG_D("[Mem] memory shrinked, new limit: " << MkqlMemoryLimit);
                }
            }

            if (Y_UNLIKELY(ProfileStats)) {
                ProfileStats->MkqlMaxUsedMemory = std::max(ProfileStats->MkqlMaxUsedMemory, alloc->GetPeakAllocated());
                CAMQ_LOG_T("Peak memory usage: " << ProfileStats->MkqlMaxUsedMemory);
            }
        }

    public:
        struct TProfileStats
        {
            ui64 MkqlMaxUsedMemory = 0;
            ui64 MkqlExtraMemoryBytes = 0;
            ui32 MkqlExtraMemoryRequests = 0;
        };

        const TProfileStats* GetProfileStats() const {
            return ProfileStats.Get();
        }

        void ResetProfileStats() {
            ProfileStats.Destroy();
        }

        void TryReleaseQuota() {
            if (MkqlMemoryLimit) {
                MemoryLimits.MemoryQuotaManager->FreeQuota(MkqlMemoryLimit);
                if (MkqlMemoryQuota) {
                    MkqlMemoryQuota->Sub(MkqlMemoryLimit);
                }
                MkqlMemoryLimit = 0;
            }
        }

        bool GetCanAllocateExtraMemory() const {
            return CanAllocateExtraMemory;
        }

        ui64 GetHardMemoryLimit() const {
            return MemoryLimits.MkqlProgramHardMemoryLimit;
        }

    private:
        void RequestExtraMemory(ui64 memory, NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            memory = std::max(AlignMemorySizeToMbBoundary(memory), MemoryLimits.MinMemAllocSize);

            if (MemoryLimits.MkqlProgramHardMemoryLimit && MkqlMemoryLimit + memory > MemoryLimits.MkqlProgramHardMemoryLimit) {
                throw THardMemoryLimitException();
            }

            if (MemoryLimits.MemoryQuotaManager->AllocateQuota(memory)) {
                MkqlMemoryLimit += memory;
                if (MkqlMemoryQuota) {
                    MkqlMemoryQuota->Add(memory);
                }
                CAMQ_LOG_D("[Mem] memory " << memory << " granted, new limit: " << MkqlMemoryLimit);
                alloc->SetLimit(MkqlMemoryLimit);
            } else {
                CAMQ_LOG_W("[Mem] memory " << memory << " NOT granted");
                //            throw yexception() << "Can not allocate extra memory, limit: " << MkqlMemoryLimit
                //                << ", requested: " << memory << ", host: " << HostName();
            }

            if (Y_UNLIKELY(ProfileStats)) {
                ProfileStats->MkqlExtraMemoryBytes += memory;
                ProfileStats->MkqlExtraMemoryRequests++;
            }
        }

        ui64 AlignMemorySizeToMbBoundary(ui64 memory) {
            // allocate memory in 1_MB (2^20B) chunks, so requested value is rounded up to MB boundary
            constexpr ui64 alignMask = 1_MB - 1;
            return (memory + alignMask) & ~alignMask;
        }

    private:
        ::NMonitoring::TDynamicCounters::TCounterPtr MkqlMemoryQuota;
        const ui64 InitialMkqlMemoryLimit;
        ui64 MkqlMemoryLimit;
        const TComputeMemoryLimits MemoryLimits;
        const TTxId TxId;
        const ui64 TaskId;
        THolder<TProfileStats> ProfileStats;
        const bool CanAllocateExtraMemory;
        NActors::TActorSystem* ActorSystem;
    };
} // namespace NYql::NDq
