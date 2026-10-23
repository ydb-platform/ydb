#include "consumer_collection.h"

#include <util/generic/mapfindptr.h>
#include <util/generic/reserve.h>

#include <limits>

namespace NKikimr::NMemory {

namespace {

// An eighth of a kind limit is split equally among its registrants regardless of demand
constexpr ui64 BootstrapLimitDivisor = 8;

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

bool TConsumerCollection::SetDesired(TActorId registrant, ui64 desired) {
    if (auto* entry = MapFindPtr(Registrants, registrant)) {
        entry->Desired = desired;
        return true;
    }
    return false;
}

TConsumerCollectionSnapshot TConsumerCollection::GetSnapshot() const {
    TConsumerCollectionSnapshot result;
    result.Entries.reserve(Registrants.size());
    for (const auto& [registrant, entry] : Registrants) {
        const auto report = entry.Consumer->GetReport();
        result.Entries.push_back({registrant, report, entry.Desired, entry.Desired.value_or(0)});
        result.Total.Used += report.Used;
        result.Total.Demand += report.Demand;
        result.Total.Reclaimable += report.Reclaimable;
    }
    return result;
}

ui64 TConsumerCollectionSnapshot::ComputeLimitShare(const TEntry& entry, ui64 limitBytes) const {
    if (entry.Desired) {
        return *entry.Desired;
    }
    // A slice ignores demand so a usage-only cache can grow out of zero.
    const ui64 bootstrapShare = limitBytes / BootstrapLimitDivisor / Entries.size();
    const ui64 rest = limitBytes - bootstrapShare * Entries.size();
    const ui64 covered = Min(Total.Demand, rest);
    const ui64 surplusShare = (rest - covered) / Entries.size();
    const ui64 demandShare = Total.Demand
        ? static_cast<ui64>(static_cast<unsigned __int128>(covered) * entry.Report.Demand / Total.Demand)
        : 0;
    return bootstrapShare + demandShare + surplusShare;
}

TVector<TConsumerShare> TConsumerCollectionSnapshot::ComputeLimitShares(ui64 limitBytes) const {
    TVector<TConsumerShare> result(::Reserve(Entries.size()));
    for (const auto& entry : Entries) {
        result.push_back({.Registrant = entry.Registrant, .Bytes = ComputeLimitShare(entry, limitBytes)});
    }
    return result;
}

ui64 TConsumerCollectionSnapshot::GetDesiredLimit(ui64 defaultLimitBytes) const {
    bool hasRequest = false;
    for (const auto& entry : Entries) {
        hasRequest |= entry.Desired.has_value();
    }
    if (!hasRequest) {
        return defaultLimitBytes;
    }
    ui64 total = 0;
    for (const auto& entry : Entries) {
        const ui64 bytes = entry.Desired ? entry.PlannedLimit : ComputeLimitShare(entry, defaultLimitBytes);
        total += Min(bytes, std::numeric_limits<ui64>::max() - total);
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
