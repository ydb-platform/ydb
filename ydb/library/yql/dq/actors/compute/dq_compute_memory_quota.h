
#pragma once

#include <util/system/mem_info.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/comp_nodes/dq_operator_memory_quota.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/aligned_page_pool.h>

#include <ydb/library/actors/core/log.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>

namespace NYql::NDq {
// ActorSystem is null in standalone unit tests
#define CAMQ_LOG_T(s) \
    do { if (ActorSystem) { LOG_TRACE_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s); } } while (0)
#define CAMQ_LOG_D(s) \
    do { if (ActorSystem) { LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s); } } while (0)
#define CAMQ_LOG_I(s) \
    do { if (ActorSystem) { LOG_INFO_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s); } } while (0)
#define CAMQ_LOG_W(s) \
    do { if (ActorSystem) { LOG_WARN_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", task: " << TaskId << ". " << s); } } while (0)

    class THardMemoryLimitException : public NKikimr::TMemoryLimitExceededException {
    };

    // Bridge between the MKQL allocator of a task and IMemoryQuotaManager (RFC dq_memory_quota_20, section 2).
    // Also the operator-facing IDqOperatorMemoryQuota: the owner binds it to the executing thread with
    // TDqOperatorMemoryQuotaScope around graph execution (see GetOperatorQuota).
    class TDqMemoryQuota final : public IDqOperatorMemoryQuota {
    public:
        TDqMemoryQuota(::NMonitoring::TDynamicCounters::TCounterPtr& mkqlMemoryQuota, ui64 initialMkqlMemoryLimit, const NYql::NDq::TComputeMemoryLimits& memoryLimits, NYql::NDq::TTxId txId, ui64 taskId, bool profileStats, NActors::TActorSystem* actorSystem)
            : MkqlMemoryQuota(mkqlMemoryQuota)
            , InitialMkqlMemoryLimit(initialMkqlMemoryLimit)
            , MkqlMemoryLimit(0)
            , MemoryLimits(memoryLimits)
            , TxId(txId)
            , TaskId(taskId)
            , ProfileStats(profileStats ? MakeHolder<TProfileStats>() : nullptr)
            , ActorSystem(actorSystem) {

            auto memoryLimit = initialMkqlMemoryLimit;
            if (!MemoryLimits.MemoryQuotaManager->AllocateQuota(memoryLimit, /* isOptional = */ false)) {
                // fall back to the guaranteed part of the quota manager, this allocation should never fail
                memoryLimit = std::min(InitialMkqlMemoryLimit, MemoryLimits.MemoryQuotaManager->GetMaxMemorySize());
                if (!MemoryLimits.MemoryQuotaManager->AllocateQuota(memoryLimit, /* isOptional = */ false)) {
                    CAMQ_LOG_W("[Mem] initial memory allocation of " << memoryLimit << " failed, starting with 0");
                    return;
                }
            }
            MkqlMemoryLimit = memoryLimit;
            if (MkqlMemoryQuota) {
                MkqlMemoryQuota->Add(memoryLimit);
            }
        }

        ui64 GetMkqlMemoryLimit() const {
            return MkqlMemoryLimit;
        }

        void TrySetIncreaseMemoryLimitCallback(NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            Alloc = alloc;
            alloc->Ref().SetIncreaseMemoryLimitCallback([this, alloc](ui64 limit, ui64 required) {
                RequestExtraMemory(required - limit, /* isOptional = */ false, alloc);
            });
        }

        // This callback is created for testing purposes and will be enabled only with spilling.
        // Most likely this callback will be removed after KIKIMR-21481.
        void TrySetIncreaseMemoryLimitCallbackWithRSSControl(NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            const ui64 limitRSS = std::numeric_limits<ui64>::max();
            const ui64 criticalRSSValue = limitRSS / 100 * 80;

            Alloc = alloc;
            alloc->Ref().SetIncreaseMemoryLimitCallback([this, alloc](ui64 limit, ui64 required) {
                RequestExtraMemory(required - limit, /* isOptional = */ false, alloc);

                ui64 currentRSS = NMemInfo::GetMemInfo().RSS;
                if (currentRSS > criticalRSSValue) {
                    alloc->SetMaximumLimitValueReached(true);
                }
            });
        }

        // Raise the MKQL memory limit by `memory` (rounded up to MB / MinMemAllocSize), returns true on success.
        // Mandatory (isOptional == false): the per-task hard limit throws THardMemoryLimitException; a refusal of the
        //   quota manager is logged and the limit stays, the allocator then throws TMemoryLimitExceededException itself.
        // Optional: never throws, false when refused by the hard limit or by the quota manager.
        bool RequestExtraMemory(ui64 memory, bool isOptional, NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            memory = std::max(AlignMemorySizeToMbBoundary(memory), MemoryLimits.MinMemAllocSize);

            bool granted = false;
            if (MemoryLimits.MkqlProgramHardMemoryLimit && MkqlMemoryLimit + memory > MemoryLimits.MkqlProgramHardMemoryLimit) {
                if (!isOptional) {
                    throw THardMemoryLimitException();
                }
                CAMQ_LOG_D("[Mem] optional memory " << memory << " refused by hard limit " << MemoryLimits.MkqlProgramHardMemoryLimit);
            } else if (MemoryLimits.MemoryQuotaManager->AllocateQuota(memory, isOptional)) {
                MkqlMemoryLimit += memory;
                if (MkqlMemoryQuota) {
                    MkqlMemoryQuota->Add(memory);
                }
                CAMQ_LOG_D("[Mem] " << (isOptional ? "optional " : "") << "memory " << memory << " granted, new limit: " << MkqlMemoryLimit);
                alloc->SetLimit(MkqlMemoryLimit);
                granted = true;
            } else if (isOptional) {
                CAMQ_LOG_D("[Mem] optional memory " << memory << " NOT granted");
            } else {
                CAMQ_LOG_W("[Mem] memory " << memory << " NOT granted");
            }

            // negative availability is the memory pressure signal of the quota manager (former IsReasonableToUseSpilling)
            alloc->SetMaximumLimitValueReached(MemoryLimits.MemoryQuotaManager->GetMemoryAvailability() < 0);

            if (Y_UNLIKELY(ProfileStats)) {
                if (isOptional) {
                    ProfileStats->MkqlOptionalMemoryRequests++;
                    if (granted) {
                        ProfileStats->MkqlExtraMemoryBytes += memory;
                    } else {
                        ProfileStats->MkqlOptionalMemoryRefusals++;
                    }
                } else {
                    ProfileStats->MkqlExtraMemoryBytes += memory;
                    ProfileStats->MkqlExtraMemoryRequests++;
                }
            }
            return granted;
        }

        // Explicit give-back: release free pages and return the unused part of the limit to the quota manager.
        // GetUsed() excludes free pages, so MkqlMemoryLimit - used is what can be returned after ReleaseFreePages().
        // The gate is deliberately not based on the free page count: blocks larger than a page are malloc-backed
        // (sized allocators) and their release never produces free pages.
        void TryShrinkMemory(NKikimr::NMiniKQL::TScopedAlloc* alloc) {
            const ui64 used = alloc->GetUsed();
            if (MkqlMemoryLimit > used && MkqlMemoryLimit - used > MemoryLimits.MinMemFreeSize) {
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
                auto& previousMaxUsedMemory = ProfileStats->MkqlMaxUsedMemory;
                auto currentUsedMemory = alloc->GetPeakAllocated();
                if (currentUsedMemory > previousMaxUsedMemory) {
                    previousMaxUsedMemory = currentUsedMemory;
                    CAMQ_LOG_T("Peak memory usage: " << currentUsedMemory);
                }
            }
        }

        // IDqOperatorMemoryQuota: operators call these inside a bound scope, on the thread that runs the graph
        // under this quota's allocator. Silent no-ops otherwise (operators then fall back to the allocator heuristics).
        bool RequestExtraMemory(ui64 bytes, bool isOptional) override {
            if (!IsBoundAllocator()) {
                return false;
            }
            return RequestExtraMemory(bytes, isOptional, Alloc);
        }

        i64 GetMemoryAvailability() const override {
            return MemoryLimits.MemoryQuotaManager->GetMemoryAvailability();
        }

        void TryShrinkMemory() override {
            if (IsBoundAllocator()) {
                TryShrinkMemory(Alloc);
            }
        }

        // What the owner binds for the operators, nullptr when the operator memory quota is disabled
        // or the allocator is not attached yet
        IDqOperatorMemoryQuota* GetOperatorQuota() {
            return (MemoryLimits.EnableOperatorMemoryQuota && Alloc) ? this : nullptr;
        }

    public:
        struct TProfileStats
        {
            ui64 MkqlMaxUsedMemory = 0;
            ui64 MkqlExtraMemoryBytes = 0;
            ui32 MkqlExtraMemoryRequests = 0;
            ui32 MkqlOptionalMemoryRequests = 0;
            ui32 MkqlOptionalMemoryRefusals = 0;
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

        ui64 GetHardMemoryLimit() const {
            return MemoryLimits.MkqlProgramHardMemoryLimit;
        }

    private:
        bool IsBoundAllocator() const {
            return Alloc && NKikimr::NMiniKQL::TlsAllocState == &Alloc->Ref();
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
        NActors::TActorSystem* ActorSystem;
        NKikimr::NMiniKQL::TScopedAlloc* Alloc = nullptr; // attached by TrySetIncreaseMemoryLimitCallback[WithRSSControl]
    };
} // namespace NYql::NDq
