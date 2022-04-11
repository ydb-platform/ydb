
#pragma once

#include <ydb/core/protos/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <library/cpp/actors/core/log.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>

namespace NYql::NDq {
#define CAMQ_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define CAMQ_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)
#define CAMQ_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s)

    class TDqMemoryQuota {
    public:
        TDqMemoryQuota(ui64 initialMkqlMemoryLimit, const NYql::NDq::TComputeMemoryLimits& memoryLimits, NYql::NDq::TTxId txId, ui64 taskId, bool profileStats, bool canAllocateExtraMemory)
            : InitialMkqlMemoryLimit(initialMkqlMemoryLimit)
            , MkqlMemoryLimit(initialMkqlMemoryLimit)
            , MemoryLimits(memoryLimits)
            , TxId(txId)
            , TaskId(taskId)
            , ProfileStats(profileStats ? MakeHolder<TProfileStats>() : nullptr)
            , CanAllocateExtraMemory(canAllocateExtraMemory) {
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

        void TryShrinkMemory(NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            if (alloc->GetAllocated() - alloc->GetUsed() > MemoryLimits.MinMemFreeSize) {
                alloc->ReleaseFreePages();
                if (MemoryLimits.FreeMemoryFn) {
                    auto newLimit = std::max(alloc->GetAllocated(), InitialMkqlMemoryLimit);
                    if (MkqlMemoryLimit > newLimit) {
                        auto freedSize = MkqlMemoryLimit - newLimit;
                        MkqlMemoryLimit = newLimit;
                        alloc->SetLimit(newLimit);
                        MemoryLimits.FreeMemoryFn(TxId, TaskId, freedSize);
                        CAMQ_LOG_I("[Mem] memory shrinked, new limit: " << MkqlMemoryLimit);
                    }
                }
            }

            if (Y_UNLIKELY(ProfileStats)) {
                ProfileStats->MkqlMaxUsedMemory = std::max(ProfileStats->MkqlMaxUsedMemory, alloc->GetPeakAllocated());
                CAMQ_LOG_D("Peak memory usage: " << ProfileStats->MkqlMaxUsedMemory);
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
            if (MkqlMemoryLimit && MemoryLimits.FreeMemoryFn) {
                MemoryLimits.FreeMemoryFn(TxId, TaskId, MkqlMemoryLimit);
                MkqlMemoryLimit = 0;
            }
        }

        bool GetCanAllocateExtraMemory() const {
            return CanAllocateExtraMemory;
        }

    private:
        void RequestExtraMemory(ui64 memory, NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            memory = std::max(AlignMemorySizeToMbBoundary(memory), MemoryLimits.MinMemAllocSize);

            CAMQ_LOG_I("not enough memory, request +" << memory);

            if (MemoryLimits.AllocateMemoryFn(TxId, TaskId, memory)) {
                MkqlMemoryLimit += memory;
                CAMQ_LOG_I("[Mem] memory granted, new limit: " << MkqlMemoryLimit);
                alloc->SetLimit(MkqlMemoryLimit);
            } else {
                CAMQ_LOG_W("[Mem] memory not granted");
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
        const ui64 InitialMkqlMemoryLimit;
        ui64 MkqlMemoryLimit;
        const TComputeMemoryLimits MemoryLimits;
        const TTxId TxId;
        const ui64 TaskId;
        THolder<TProfileStats> ProfileStats;
        const bool CanAllocateExtraMemory;
    };
} // namespace NYql::NDq