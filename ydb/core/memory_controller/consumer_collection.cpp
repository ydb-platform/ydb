#include "consumer_collection.h"
#include "limit_budget.h"

#include <util/generic/mapfindptr.h>
#include <util/generic/reserve.h>

#include <limits>

namespace NKikimr::NMemory {

namespace {

// An eighth of the kind limit is shared equally among automatic registrants.
constexpr ui64 EqualShareDivisor = 8;

}

void TRegistrantConsumer::SetReport(TConsumerReport report) {
    Used.store(report.Used);
    Demand.store(report.Demand);
    Reclaimable.store(report.Reclaimable);
}

TConsumerReport TRegistrantConsumer::GetReport() const {
    TConsumerReport report{.Used = Used.load(), .Demand = Demand.load(), .Reclaimable = Reclaimable.load()};
    report.Demand = Max(report.Demand, report.Used);
    report.Reclaimable = Min(report.Reclaimable, report.Used);
    return report;
}

TIntrusivePtr<TRegistrantConsumer> TConsumerCollection::Register(TActorId registrant) {
    // A fresh object even for the same actor: a pointer handed out earlier may outlive the registration that got it
    auto& slot = Registrants[registrant];
    slot = {.Consumer = MakeIntrusive<TRegistrantConsumer>()};
    return slot.Consumer;
}

bool TConsumerCollection::Unregister(TActorId registrant) {
    return Registrants.erase(registrant) > 0;
}

bool TConsumerCollection::SetDesired(TActorId registrant, std::optional<ui64> desired, ui64 requestOrder) {
    if (auto* entry = MapFindPtr(Registrants, registrant)) {
        if (entry->Desired != desired) {
            entry->Desired = desired;
            entry->RequestOrder = requestOrder;
        }
        return true;
    }
    return false;
}

TConsumerCollectionSnapshot TConsumerCollection::GetSnapshot() const {
    TConsumerCollectionSnapshot result;
    result.Entries.reserve(Registrants.size());
    for (const auto& [registrant, entry] : Registrants) {
        const auto report = entry.Consumer->GetReport();
        result.Entries.push_back({.Registrant = registrant, .Report = report, .Desired = entry.Desired,
            .RequestOrder = entry.RequestOrder, .PlannedLimit = entry.Desired,
            .LimitCeiling = entry.Consumer->GetLimitCeiling()});
        result.Total.Used = SaturatingAdd(result.Total.Used, report.Used);
        result.Total.Demand = SaturatingAdd(result.Total.Demand, report.Demand);
        result.Total.Reclaimable = SaturatingAdd(result.Total.Reclaimable, report.Reclaimable);
        if (!entry.Desired) {
            ++result.AutomaticRegistrantCount;
            result.AutomaticDemand = SaturatingAdd(result.AutomaticDemand, report.Demand);
        }
    }
    return result;
}

ui64 TConsumerCollectionSnapshot::ComputeLimitShare(const TEntry& entry, ui64 limitBytes) const {
    if (entry.Desired) {
        return entry.PlannedLimit.value_or(*entry.Desired);
    }
    // A slice ignores demand so a usage-only cache can grow out of zero.
    const ui64 equalShare = limitBytes / (EqualShareDivisor * AutomaticRegistrantCount);
    const ui64 rest = limitBytes - equalShare * AutomaticRegistrantCount;
    const ui64 covered = Min(AutomaticDemand, rest);
    const ui64 surplusShare = (rest - covered) / AutomaticRegistrantCount;
    const ui64 demandShare = AutomaticDemand
        ? static_cast<ui64>(static_cast<unsigned __int128>(covered) * entry.Report.Demand / AutomaticDemand)
        : 0;
    return Min(equalShare + demandShare + surplusShare, entry.LimitCeiling);
}

TVector<TConsumerShare> TConsumerCollectionSnapshot::ComputeLimitShares(ui64 limitBytes, ui64 limitReduction) const {
    TVector<TConsumerShare> result(::Reserve(Entries.size()));
    unsigned __int128 total = 0;
    for (const auto& entry : Entries) {
        const ui64 bytes = ComputeLimitShare(entry, limitBytes);
        result.push_back({.Registrant = entry.Registrant, .Bytes = bytes});
        total += bytes;
    }
    // Soft pressure reduces current grants, including explicit ones, without erasing Desired.
    if (limitReduction && total) {
        const auto reduction = Min<unsigned __int128>(limitReduction, total);
        for (auto& share : result) {
            const auto weighted = static_cast<unsigned __int128>(share.Bytes) * reduction;
            share.Bytes -= static_cast<ui64>(weighted / total + (weighted % total != 0));
        }
    }
    return result;
}

ui64 TConsumerCollectionSnapshot::GetDesiredLimit(ui64 defaultLimitBytes) const {
    bool hasOverride = false;
    for (const auto& entry : Entries) {
        hasOverride |= entry.Desired.has_value() || entry.LimitCeiling != std::numeric_limits<ui64>::max();
    }
    if (!hasOverride) {
        return defaultLimitBytes;
    }
    ui64 total = 0;
    for (const auto& entry : Entries) {
        const ui64 bytes = entry.PlannedLimit.value_or(ComputeLimitShare(entry, defaultLimitBytes));
        total = SaturatingAdd(total, bytes);
    }
    return total;
}

TVector<TConsumerShare> TConsumerCollection::ComputeReleaseRequests(ui64 limitBytes) const {
    TVector<TConsumerShare> result;
    TVector<std::pair<TActorId, ui64>> reclaimables(::Reserve(Registrants.size()));
    ui64 totalUsed = 0;
    ui64 totalReclaimable = 0;
    for (const auto& [registrant, entry] : Registrants) {
        const auto report = entry.Consumer->GetReport();
        totalUsed += report.Used;
        if (report.Reclaimable) {
            reclaimables.emplace_back(registrant, report.Reclaimable);
            totalReclaimable += report.Reclaimable;
        }
    }
    const ui64 excess = totalUsed - Min(totalUsed, limitBytes);
    if (!excess) {
        return result;
    }
    result.reserve(reclaimables.size());
    for (const auto& [registrant, reclaimable] : reclaimables) {
        const ui64 proportional = static_cast<ui64>(static_cast<unsigned __int128>(excess) * reclaimable / totalReclaimable);
        const ui64 bytes = Min(reclaimable, proportional);
        if (bytes) {
            result.push_back({.Registrant = registrant, .Bytes = bytes});
        }
    }
    return result;
}

}
