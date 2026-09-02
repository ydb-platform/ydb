#include "kqp_memory_quota.h"

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/actors/core/log.h>

#include <ydb/library/services/services.pb.h>

namespace NKikimr::NKqp {

// Increment optional-quota-denied counters; no-op when counters pointer is null (e.g. in tests).
static void RecordOptionalQuotaDenied(const TIntrusivePtr<TKqpCounters>& counters, ui64 bytes) {
    if (counters) {
        counters->RmOptionalQuotaDenied->Inc();
        counters->RmOptionalQuotaDeniedBytes->Add(bytes);
    }
}

// Per-task quota manager. NOT thread safe.
struct TMemoryQuotaManager : public NYql::NDq::TGuaranteeQuotaManager {

    TMemoryQuotaManager(std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
        TIntrusivePtr<NRm::TTxState> tx,
        ui64 taskId,
        ui64 limit)
    : NYql::NDq::TGuaranteeQuotaManager(limit, limit)
    , ResourceManager(std::move(resourceManager))
    , Tx(std::move(tx))
    , TaskId(taskId)
    {}

    ~TMemoryQuotaManager() override {
        ResourceManager->FreeResources(*Tx, TaskId, NRm::TKqpResourcesRequest{
            .ExecutionUnits = 1,
            .Memory = Limit - Guarantee,
            .ExternalMemory = Guarantee,
        });
    }

    bool AllocateExtraQuota(ui64 extraSize, bool isOptional) override {
        if (isOptional) {
            i64 available = GetMemoryAvailability();
            if (available <= 0 || static_cast<ui64>(available) < extraSize) {
                RecordOptionalQuotaDenied(ResourceManager->GetCounters(), extraSize);
                return false;
            }
        }

        auto result = ResourceManager->AllocateResources(*Tx, TaskId,
            NRm::TKqpResourcesRequest{.Memory = extraSize});

        if (!result) {
            if (isOptional) {
                RecordOptionalQuotaDenied(ResourceManager->GetCounters(), extraSize);
                return false;
            }
            YDB_LOG_WARN_COMP(NKikimrServices::KQP_COMPUTE, "",
                {"problem", "cannot_allocate_memory"},
                {"txId", Tx->TxId},
                {"taskId", TaskId},
                {"memory", extraSize});
            return false;
        }

        return true;
    }

    void FreeExtraQuota(ui64 extraSize) override {
        ResourceManager->FreeResources(*Tx, TaskId, NRm::TKqpResourcesRequest{.Memory = extraSize});
    }

    i64 GetMemoryAvailability() const override {
        return ResourceManager->GetTxMemoryAvailability(*Tx);
    }

    TString MemoryConsumptionDetails() const override {
        return Tx->ToString();
    }

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager;
    TIntrusivePtr<NRm::TTxState> Tx;
    ui64 TaskId;
};

NYql::NDq::IMemoryQuotaManager::TPtr CreateTaskQuotaManager(
    std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    TIntrusivePtr<NRm::TTxState> tx,
    ui64 taskId,
    ui64 initialMemoryLimit)
{
    return std::make_shared<TMemoryQuotaManager>(resourceManager, tx, taskId, initialMemoryLimit);
}

// Per-TX channel quota manager. Thread-safe.
struct TChannelQuotaManager : public NYql::NDq::IMemoryQuotaManager {

    TChannelQuotaManager(std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
        TIntrusivePtr<NRm::TTxState> tx,
        ui64 limit,
        ui64 step = 1_MB)
    : ResourceManager(std::move(resourceManager))
    , Tx(std::move(tx))
    , AvailableQuota(limit)
    , Limit(limit)
    , DataMemoryLimit(limit)
    , AllocationStep(step)
    {}

    ~TChannelQuotaManager() {
        ResourceManager->FreeResources(*Tx, 0, NRm::TKqpResourcesRequest{
            .Memory = Limit.load() - DataMemoryLimit,
            .ExternalMemory = DataMemoryLimit,
        });
    }

    bool AllocateQuota(ui64 memorySize, bool isOptional) override {
        i64 quota = AvailableQuota.fetch_sub(memorySize);

        // Yellow-zone check: deny optional immediately before touching RM.
        if (isOptional && Tx->IsReasonableToStartSpilling()) {
            AvailableQuota.fetch_add(memorySize);
            RecordOptionalQuotaDenied(ResourceManager->GetCounters(), memorySize);
            return false;
        }

        if (static_cast<i64>(memorySize) > quota) {
            ui64 memoryRequired = memorySize - quota;
            memoryRequired += AllocationStep - 1;
            memoryRequired &= ~(AllocationStep - 1);

            if (isOptional) {
                i64 available = GetMemoryAvailability();
                if (available <= 0 || static_cast<ui64>(available) < memoryRequired) {
                    AvailableQuota.fetch_add(memorySize);
                    RecordOptionalQuotaDenied(ResourceManager->GetCounters(), memorySize);
                    return false;
                }
            }

            auto result = ResourceManager->AllocateResources(*Tx, 0, NRm::TKqpResourcesRequest{.Memory = memoryRequired});
            if (result) {
                AvailableQuota.fetch_add(memoryRequired);
                Limit.fetch_add(memoryRequired);
            } else {
                if (isOptional) {
                    AvailableQuota.fetch_add(memorySize);
                    RecordOptionalQuotaDenied(ResourceManager->GetCounters(), memorySize);
                    return false;
                }
                YDB_LOG_WARN_COMP(NKikimrServices::KQP_COMPUTE, "",
                    {"problem", "cannot_allocate_memory"},
                    {"txId", Tx->TxId},
                    {"taskId", 0},
                    {"memory", memoryRequired});
                if (memoryRequired >= AllocationStep * 10) {
                    AvailableQuota.fetch_add(memorySize);
                    return false;
                }
            }
        }

        AllocatedQuota.fetch_add(memorySize);
        return true;
    }

    // Node level memory pressure signal, see NRm::TTxState::IsReasonableToStartSpilling.
    // Channels do not spill on it, but propagate it as back pressure, see TInputDescriptor::MemoryPressure
    i64 GetMemoryAvailability() const override {
        return ResourceManager->GetTxMemoryAvailability(*Tx);
    }

    void FreeQuota(ui64 memorySize) override {
        auto prevQuota = AllocatedQuota.fetch_sub(memorySize);
        Y_DEBUG_ABORT_UNLESS(prevQuota >= memorySize);
        i64 quota = AvailableQuota.fetch_add(memorySize);
        if (quota > static_cast<i64>(AllocationStep * 10 + DataMemoryLimit)) {
            AvailableQuota.fetch_sub(AllocationStep);
            Limit.fetch_sub(AllocationStep);
            ResourceManager->FreeResources(*Tx, 0, NRm::TKqpResourcesRequest{.Memory = AllocationStep});
        }
    }

    ui64 GetCurrentQuota() const override {
        return AllocatedQuota.load();
    }

    ui64 GetMaxMemorySize() const override {
        return AllocatedQuota.load();
    };

    TString MemoryConsumptionDetails() const override {
        return TString();
    }

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager;
    TIntrusivePtr<NRm::TTxState> Tx;
    std::atomic<ui64> AllocatedQuota = 0;
    std::atomic<i64> AvailableQuota;
    std::atomic<ui64> Limit;
    const ui64 DataMemoryLimit;
    const ui64 AllocationStep;
};

NYql::NDq::IMemoryQuotaManager::TPtr CreateChannelQuotaManager(
    std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    TIntrusivePtr<NRm::TTxState> tx,
    ui64 initialMemoryLimit,
    ui64 allocationStep)
{
    return std::make_shared<TChannelQuotaManager>(resourceManager, tx, initialMemoryLimit, allocationStep);
}

} // namespace NKikimr::NKqp
