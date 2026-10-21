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
    slot = {MakeIntrusive<TRegistrantConsumer>(), std::nullopt};
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
        result.Entries.push_back({registrant, entry.Consumer->GetReport(), entry.Desired, entry.Desired.value_or(0)});
    }
    return result;
}

TConsumerReport TConsumerCollection::GetTotal() const {
    return GetSnapshot().GetTotal();
}

TConsumerReport TConsumerCollectionSnapshot::GetTotal() const {
    TConsumerReport total;
    for (const auto& entry : Entries) {
        total.Used += entry.Report.Used;
        total.Demand += entry.Report.Demand;
        total.Reclaimable += entry.Report.Reclaimable;
    }
    return total;
}

TVector<TConsumerShare> TConsumerCollection::ComputeLimitShares(ui64 limitBytes) const {
    return GetSnapshot().ComputeLimitShares(limitBytes);
}

TVector<TConsumerShare> TConsumerCollectionSnapshot::ComputeLimitShares(ui64 limitBytes) const {
    TVector<TConsumerShare> result(::Reserve(Entries.size()));
    if (Entries.empty()) {
        return result;
    }
    ui64 totalDemand = 0;
    for (const auto& entry : Entries) {
        totalDemand += entry.Report.Demand;
    }
    // A slice ignores demand so a usage-only cache can grow out of zero.
    const ui64 bootstrapShare = limitBytes / BootstrapLimitDivisor / Entries.size();
    const ui64 rest = limitBytes - bootstrapShare * Entries.size();
    const ui64 covered = Min(totalDemand, rest);
    const ui64 surplusShare = (rest - covered) / Entries.size();
    for (const auto& entry : Entries) {
        const ui64 demandShare = totalDemand
            ? static_cast<ui64>(static_cast<unsigned __int128>(covered) * entry.Report.Demand / totalDemand)
            : 0;
        result.push_back({.Registrant = entry.Registrant,
            .Bytes = entry.Desired.value_or(bootstrapShare + demandShare + surplusShare)});
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
    const auto shares = ComputeLimitShares(defaultLimitBytes);
    for (size_t i = 0; i < shares.size(); ++i) {
        const auto& entry = Entries[i];
        const ui64 bytes = entry.Desired ? entry.PlannedLimit : shares[i].Bytes;
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
