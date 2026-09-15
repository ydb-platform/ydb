#include "consumer_collection.h"

#include <util/generic/reserve.h>

namespace NKikimr::NMemory {

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
    slot = MakeIntrusive<TRegistrantConsumer>();
    return slot;
}

bool TConsumerCollection::Unregister(TActorId registrant) {
    return Registrants.erase(registrant) > 0;
}

TConsumerReport TConsumerCollection::GetTotal() const {
    TConsumerReport total;
    for (const auto& [registrant, consumer] : Registrants) {
        const auto report = consumer->GetReport();
        total.Used += report.Used;
        total.Demand += report.Demand;
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
    for (const auto& [registrant, consumer] : Registrants) {
        demands.push_back(consumer->GetReport().Demand);
        totalDemand += demands.back();
    }
    // The demand-covered part splits proportionally; the surplus splits equally so an idle registrant can still grow.
    const ui64 covered = Min(totalDemand, limitBytes);
    const ui64 surplusShare = (limitBytes - covered) / Registrants.size();
    size_t index = 0;
    for (const auto& [registrant, consumer] : Registrants) {
        const ui64 demandShare = totalDemand
            ? static_cast<ui64>(static_cast<unsigned __int128>(covered) * demands[index] / totalDemand)
            : 0;
        result.push_back({.Registrant = registrant, .Bytes = demandShare + surplusShare});
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
