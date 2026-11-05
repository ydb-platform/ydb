#pragma once

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/tx/limiter/grouped_memory/service/counters.h>

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TStageFeatures {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY(ui64, Limit, 0);
    YDB_READONLY_DEF(std::optional<ui64>, HardLimit);
    YDB_READONLY_DEF(std::optional<ui64>, UnrestrictedSoft);
    YDB_ACCESSOR_DEF(TPositiveControlInteger, Usage);
    YDB_ACCESSOR_DEF(TPositiveControlInteger, Waiting);
    std::shared_ptr<TStageFeatures> Owner;
    std::shared_ptr<TStageCounters> Counters;
    std::function<void(ui64)> MemoryConsumptionUpdate;
    const bool UseLimitFromConfig;
    const bool UseHardLimitFromConfig;
    static constexpr ui64 DEFAULT_LIMIT = ui64(3) << 30;

    void UpdateConsumption(const TStageFeatures* current) const;

    // The band never goes below the soft limit.
    ui64 GetUnrestrictedLimit() const {
        return std::max(UnrestrictedSoft.value_or(Limit), Limit);
    }

public:
    TString DebugString() const;

    ui64 GetFullMemory() const {
        return Usage.Val() + Waiting.Val();
    }

    TStageFeatures(const TString& name, const std::optional<ui64>& limit, const std::optional<ui64>& hardLimit, const std::shared_ptr<TStageFeatures>& owner,
        const std::shared_ptr<TStageCounters>& counters, const std::optional<ui64>& unrestrictedSoft = std::nullopt);

    [[nodiscard]] TConclusionStatus Allocate(const ui64 volume);

    void Free(const ui64 volume, const bool allocated);
    void UpdateVolume(const ui64 from, const ui64 to, const bool allocated);
    bool IsAllocatable(const ui64 volume, const ui64 additional) const;
    bool IsAllocatableUnrestricted(const ui64 volume, const ui64 additional) const;
    // false: the volume exceeds a limit on the chain even at zero usage, so waiting cannot help.
    // nullopt: the root stage has no band yet, so the answer is not known.
    std::optional<bool> CanEverFitUnrestricted(const ui64 volume) const;
    // Smallest unrestricted limit on the chain. Used for error messages.
    ui64 GetEffectiveUnrestrictedLimit() const;
    // A waiting request was rejected without going through Allocate.
    void OnCannotAllocate() {
        if (Counters) {
            Counters->OnCannotAllocate();
        }
    }
    void Add(const ui64 volume, const bool allocated);

    void SetMemoryConsumptionUpdateFunction(std::function<void(ui64)> func);

    void AttachOwner(const std::shared_ptr<TStageFeatures>& owner);
    void AttachCounters(const std::shared_ptr<TStageCounters>& counters);

    void UpdateMemoryLimits(const ui64 limit, const std::optional<ui64>& hardLimit, bool& isLimitIncreased,
        const std::optional<ui64>& unrestrictedSoft = std::nullopt);
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
