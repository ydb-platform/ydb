#include "kqp_rm_memory_quota.h"
#include "kqp_rm_service.h"

namespace NKikimr::NKqp::NRm {

namespace {

class TMemoryQuotaManager final : public NYql::NDq::IMemoryQuotaManager {
public:
    explicit TMemoryQuotaManager(std::shared_ptr<IKqpResourceManager> resourceManager)
        : ResourceManager(std::move(resourceManager))
        , State(MakeIntrusive<TTxState>(ResourceManager, /* txId */ static_cast<ui64>(0), TInstant::Now(), /* poolId */ "", /* memoryPoolPercent */ 100.0, /* database */ "", /* collectBacktrace */ false))
    {}

    bool AllocateQuota(ui64 size) final {
        if (size && !ResourceManager->AllocateResources(*State, /* taskId */ 0, {.Memory = size})) {
            return false;
        }
        Allocated.fetch_add(size);
        return true;
    }

    void FreeQuota(ui64 size) final {
        const auto previous = Allocated.fetch_sub(size);
        Y_ABORT_UNLESS(previous >= size);
        if (size) {
            ResourceManager->FreeResources(*State, /* taskId */ 0, {.Memory = size});
        }
    }

    ui64 GetCurrentQuota() const final {
        return Allocated.load();
    }

    ui64 GetMaxMemorySize() const final {
        return GetCurrentQuota();
    }

    bool IsReasonableToUseSpilling() const final {
        return false;
    }

    TString MemoryConsumptionDetails() const final {
        return State->ToString();
    }

private:
    std::shared_ptr<IKqpResourceManager> ResourceManager;
    const TIntrusivePtr<TTxState> State;
    std::atomic<ui64> Allocated = 0;
};

} // anonymous namespace

NYql::NDq::IMemoryQuotaManager::TPtr CreateMemoryQuotaManager(std::shared_ptr<IKqpResourceManager> resourceManager) {
    return std::make_shared<TMemoryQuotaManager>(std::move(resourceManager));
}

} // namespace NKikimr::NKqp::NRm
