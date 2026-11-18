#include "consumer_collection.h"

#include <util/generic/reserve.h>

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

TIntrusivePtr<TRegistrantConsumer> TConsumerCollection::Register(TActorId registrant, bool demandOnly) {
    // A fresh object even for the same actor: a pointer handed out earlier may outlive the registration that got it
    auto& slot = Registrants[registrant];
    Retiring.erase(registrant);
    slot = MakeIntrusive<TRegistrantConsumer>(demandOnly);
    return slot;
}

bool TConsumerCollection::Unregister(TActorId registrant, bool retainUntilUnused) {
    const auto* consumer = Registrants.FindPtr(registrant);
    if (consumer && (retainUntilUnused || (*consumer)->DemandOnly) && (*consumer)->GetReport().Used) {
        Retiring.insert(registrant);
        return true;
    }
    Retiring.erase(registrant);
    return Registrants.erase(registrant) > 0;
}

void TConsumerCollection::CollectRetired() {
    for (auto it = Retiring.begin(); it != Retiring.end();) {
        const auto* consumer = Registrants.FindPtr(*it);
        if (!consumer || !(*consumer)->GetReport().Used) {
            Registrants.erase(*it);
            Retiring.erase(it++);
        } else {
            ++it;
        }
    }
}

TConsumerReport TConsumerCollection::GetTotal() const {
    TConsumerReport total;
    for (const auto& [registrant, consumer] : Registrants) {
        const auto report = consumer->GetReport();
        total.Used += report.Used;
        total.Demand += Retiring.contains(registrant) ? report.Used : report.Demand;
        total.Reclaimable += report.Reclaimable;
    }
    return total;
}

TVector<TConsumerShare> TConsumerCollection::ComputeLimitShares(ui64 limitBytes) const {
    TVector<TConsumerShare> result(::Reserve(Registrants.size()));
    if (Registrants.empty()) {
        return result;
    }
    TVector<ui64> demands(::Reserve(Registrants.size()));
    ui64 totalDemand = 0;
    size_t bootstrapConsumers = 0;
    for (const auto& [registrant, consumer] : Registrants) {
        bootstrapConsumers += !consumer->DemandOnly;
        const auto report = consumer->GetReport();
        demands.push_back(Retiring.contains(registrant) ? report.Used : report.Demand);
        totalDemand += demands.back();
    }
    // A slice of the limit ignores demand: a cache that obeys its limit and reports only Used could never grow out of zero
    const ui64 bootstrapShare = bootstrapConsumers ? limitBytes / (BootstrapLimitDivisor * bootstrapConsumers) : 0;
    const ui64 rest = limitBytes - bootstrapShare * bootstrapConsumers;
    // The demand-covered part of the rest splits proportionally; what demand leaves over splits equally.
    const ui64 covered = Min(totalDemand, rest);
    const ui64 surplusShare = bootstrapConsumers ? (rest - covered) / bootstrapConsumers : 0;
    size_t index = 0;
    for (const auto& [registrant, consumer] : Registrants) {
        const ui64 demandShare = totalDemand
            ? static_cast<ui64>(static_cast<unsigned __int128>(covered) * demands[index] / totalDemand)
            : 0;
        if (!Retiring.contains(registrant)) {
            result.push_back({.Registrant = registrant, .Bytes = demandShare + (consumer->DemandOnly ? 0 : bootstrapShare + surplusShare)});
        }
        ++index;
    }
    return result;
}

TVector<TConsumerShare> TConsumerCollection::ComputeReleaseRequests(ui64 limitBytes) const {
    TVector<TConsumerShare> result;
    TVector<std::pair<TActorId, ui64>> reclaimables(::Reserve(Registrants.size()));
    ui64 totalUsed = 0;
    ui64 totalReclaimable = 0;
    for (const auto& [registrant, consumer] : Registrants) {
        const auto report = consumer->GetReport();
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
