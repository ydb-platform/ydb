#include "tablet_stats_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <map>
#include <set>
#include <algorithm>

namespace NKikimr::NDDisk {
namespace {

class TTabletStatsActor : public NActors::TActorBootstrapped<TTabletStatsActor> {
    const NActors::TActorId Owner;
    std::map<ui64, TDDiskMonTabletStats> Tablets;
    bool RequestInFlight = false;
    bool TimerScheduled = false;

    void RequestBatch() {
        RequestInFlight = true;
        Send(Owner, new TEvCollectTabletStats());
    }

    void Handle(TEvTabletStatsChanged::TPtr ev) {
        Y_ABORT_UNLESS(ev->Sender == Owner);
        if (!RequestInFlight && !TimerScheduled) {
            RequestBatch();
        }
    }

    void HandleCollectWakeup() {
        TimerScheduled = false;
        RequestBatch();
    }

    // Maintaining indexes at collection time bounds each snapshot request to 100 rows.
    std::set<std::pair<double, ui64>> ByThroughput;
    std::set<std::pair<double, ui64>> ByIops;
    std::set<std::pair<ui64, ui64>> ByChunks;
    std::set<ui64> ByTabletId;
    std::map<ui64, ui32> ParticipantSlots;
    ui64 TotalChunks = 0;
    double TotalIops = 0;
    double TotalBytesPerSecond = 0;

    static std::pair<double, double> Rates(const TDDiskMonTabletStats& row) {
        double iops = 0, bytes = 0;
        for (const auto& rate : row.Rates) {
            iops += rate.Iops;
            bytes += rate.BytesPerSecond;
        }
        return {iops, bytes};
    }

    void Remove(const TDDiskMonTabletStats& row) {
        const auto [iops, bytes] = Rates(row);
        ByThroughput.erase({bytes, row.TabletId});
        ByIops.erase({iops, row.TabletId});
        ByChunks.erase({row.Chunks, row.TabletId});
        TotalChunks -= row.Chunks;
        TotalIops -= iops;
        TotalBytesPerSecond -= bytes;
    }

    void Handle(TEvTabletStatsBatch::TPtr ev) {
        Y_ABORT_UNLESS(ev->Sender == Owner && RequestInFlight);
        RequestInFlight = false;
        Y_ABORT_UNLESS(ev->Get()->Samples.size() <= TTabletStatsLimits::MaxBatch);
        for (const auto& sample : ev->Get()->Samples) {
            auto [it, inserted] = Tablets.try_emplace(sample.TabletId);
            auto& row = it->second;
            if (!inserted) {
                Remove(row);
            }
            if (sample.Retired) {
                ByTabletId.erase(sample.TabletId);
                Tablets.erase(it);
                continue;
            }
            ByTabletId.insert(sample.TabletId);
            row.TabletId = sample.TabletId;
            row.Chunks = sample.Chunks;
            row.SampledAt = ev->Get()->SampledAt;
            row.Interval = sample.Elapsed;
            for (size_t i = 0; i < row.Rates.size(); ++i) {
                row.Rates[i] = CalculateDDiskMonRate(sample.Previous[i].Requests, sample.Previous[i].Bytes,
                    sample.Current[i].Requests, sample.Current[i].Bytes, sample.Elapsed).value_or(TDDiskMonRate{});
            }
            const auto [iops, bytes] = Rates(row);
            ByThroughput.emplace(bytes, row.TabletId);
            ByIops.emplace(iops, row.TabletId);
            ByChunks.emplace(row.Chunks, row.TabletId);
            TotalChunks += row.Chunks;
            TotalIops += iops;
            TotalBytesPerSecond += bytes;
        }
        if (const auto deadline = ev->Get()->NextDeadline) {
            const auto now = NActors::TActivationContext::Monotonic();
            if (*deadline <= now) {
                RequestBatch();
            } else {
                TimerScheduled = true;
                Schedule(*deadline - now, new NActors::TEvents::TEvWakeup());
            }
        }
    }

    void Handle(TEvGetTabletStatsSnapshot::TPtr ev) {
        // Read-only page requests also arrive from the monitoring snapshot actor.
        auto& request = *ev->Get();
        auto result = std::make_unique<TEvTabletStatsSnapshot>();
        auto& info = result->Info;
        info.StatsTablets = Tablets.size();
        info.StatsChunks = TotalChunks;
        info.StatsIops = std::max(0.0, TotalIops);
        info.StatsBytesPerSecond = std::max(0.0, TotalBytesPerSecond);
        info.StatsAvailable = true;
        // Each walk examines at most 30 ranked entries, irrespective of tablet count.
        const auto leaders = [&](const auto& index, long double total) {
            std::vector<TDDiskMonTabletStats> result;
            long double selected = 0;
            for (auto it = index.rbegin(); it != index.rend() && result.size() < 30 && it->first > 0; ++it) {
                if (total <= 0) {
                    break;
                }
                result.push_back(Tablets.at(it->second));
                selected += it->first;
                if (selected > total / 2) {
                    break;
                }
            }
            return result;
        };
        auto throughputShares = leaders(ByThroughput, info.StatsBytesPerSecond);
        auto iopsShares = leaders(ByIops, info.StatsIops);
        auto spaceShares = leaders(ByChunks, TotalChunks);
        const auto selectedId = request.Query.SearchTabletId ? request.Query.SearchTabletId : request.Query.StatsSelectedTabletId;
        if (selectedId) {
            const auto it = Tablets.find(*selectedId);
            if (it != Tablets.end()) {
                // Search uses one of the 30 segment slots, never an extra slot.
                for (auto* shares : {&throughputShares, &iopsShares, &spaceShares}) {
                    if (std::none_of(shares->begin(), shares->end(), [&](const auto& row) {
                            return row.TabletId == it->first;
                        })) {
                        if (shares->size() == 30) {
                            shares->pop_back();
                        }
                        shares->push_back(it->second);
                    }
                }
            }
        }
        std::set<ui64> prominent;
        std::vector<ui64> prominentOrder;
        for (const auto* shares : {&throughputShares, &iopsShares, &spaceShares}) {
            for (const auto& row : *shares) {
                if (prominent.insert(row.TabletId).second) {
                    prominentOrder.push_back(row.TabletId);
                }
            }
        }
        // All bars expose the union: a colored tablet must never hide in Other.
        for (ui64 id : prominentOrder) {
            info.StatsShares.push_back(Tablets.at(id));
        }
        std::erase_if(ParticipantSlots, [&](const auto& item) { return !prominent.contains(item.first); });
        std::array<bool, 90> usedColors{};
        for (const auto& [id, slot] : ParticipantSlots) {
            usedColors[slot] = true;
        }
        for (ui64 id : prominent) {
            if (!ParticipantSlots.contains(id)) {
                const auto free = std::find(usedColors.begin(), usedColors.end(), false);
                Y_ABORT_UNLESS(free != usedColors.end());
                ParticipantSlots.emplace(id, free - usedColors.begin());
                *free = true;
            }
        }
        info.ParticipantSlots = ParticipantSlots;
        std::set<ui64> excluded;
        if (!request.Query.StatsOther.empty()) {
            for (const auto& row : info.StatsShares) {
                excluded.insert(row.TabletId);
            }
        }
        info.StatsFilteredTablets = Tablets.size() - excluded.size();
        if (request.Query.SearchTabletId) {
            const auto it = Tablets.find(*request.Query.SearchTabletId);
            info.StatsFilteredTablets = it != Tablets.end();
            if (it != Tablets.end()) {
                info.TabletStats.push_back(it->second);
            }
        } else {
            if (!request.Query.AfterTabletId) {
                for (ui64 id : prominentOrder) {
                    if (!excluded.contains(id)) {
                        info.TabletStats.push_back(Tablets.at(id));
                    }
                }
            }
            // Stable ID cursor avoids traversing all previous pages. At most the
            // page size plus the 90 prominent IDs are examined on any request.
            auto it = request.Query.AfterTabletId ? ByTabletId.upper_bound(*request.Query.AfterTabletId) : ByTabletId.begin();
            std::optional<ui64> lastId;
            for (; it != ByTabletId.end(); ++it) {
                if (prominent.contains(*it) || excluded.contains(*it)) {
                    continue;
                }
                if (info.TabletStats.size() == TTabletStatsSnapshotQuery::MaxRows) {
                    info.StatsNextTabletId = lastId;
                    break;
                }
                info.TabletStats.push_back(Tablets.at(*it));
                lastId = *it;
            }
        }
        Send(ev->Sender, result.release(), 0, ev->Cookie);
    }

    void Handle(TEvGetTabletStats::TPtr ev) {
        const auto exportRow = [](const TDDiskMonTabletStats& row) {
            TTabletStats result;
            result.TabletId = row.TabletId;
            result.DataMappedChunks = row.Chunks;
            result.SampledAt = row.SampledAt;
            result.Interval = row.Interval;
            for (size_t i = 0; i < result.Rates.size(); ++i) {
                result.Rates[i] = {static_cast<float>(row.Rates[i].Iops), static_cast<float>(row.Rates[i].BytesPerSecond)};
            }
            return result;
        };
        const auto& query = *ev->Get();
        auto result = std::make_unique<TEvTabletStats>();
        result->TotalChunks = TotalChunks;
        result->TotalIops = std::max(0.0, TotalIops);
        result->TotalBytesPerSecond = std::max(0.0, TotalBytesPerSecond);
        if (!query.RankBy.empty()) {
            const auto top = [&](const auto& index) {
                const size_t limit = std::clamp<size_t>(query.Limit, 1, 10);
                for (auto it = index.rbegin(); it != index.rend() && result->Tablets.size() < limit; ++it) {
                    result->Tablets.push_back(exportRow(Tablets.at(it->second)));
                }
            };
            if (query.RankBy == "iops") {
                top(ByIops);
            } else if (query.RankBy == "chunks") {
                top(ByChunks);
            } else {
                top(ByThroughput);
            }
        } else if (query.TabletId) {
            if (auto it = Tablets.find(*query.TabletId); it != Tablets.end()) {
                result->Tablets.push_back(exportRow(it->second));
            }
        } else {
            const size_t limit = std::clamp<size_t>(query.Limit, 1, TTabletStatsLimits::MaxBatch);
            auto it = query.AfterTabletId ? Tablets.upper_bound(*query.AfterTabletId) : Tablets.begin();
            for (; it != Tablets.end() && result->Tablets.size() < limit; ++it) {
                result->Tablets.push_back(exportRow(it->second));
            }
            if (it != Tablets.end()) {
                result->NextTabletId = result->Tablets.back().TabletId;
            }
        }
        Send(ev->Sender, result.release(), 0, ev->Cookie);
    }

public:
    explicit TTabletStatsActor(NActors::TActorId owner) : Owner(owner) {}

    void Bootstrap() { Become(&TThis::StateWork); }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTabletStatsBatch, Handle)
        hFunc(TEvTabletStatsChanged, Handle)
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleCollectWakeup)
        hFunc(TEvGetTabletStats, Handle)
        hFunc(TEvGetTabletStatsSnapshot, Handle)
        cFunc(NActors::TEvents::TSystem::Poison, PassAway)
    )
};

} // namespace

NActors::IActor* CreateTabletStatsActor(NActors::TActorId owner) {
    return new TTabletStatsActor(owner);
}

} // namespace NKikimr::NDDisk
