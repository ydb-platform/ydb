#include "tablet_stats_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <algorithm>
#include <map>

namespace NKikimr::NDDisk {
namespace {

class TTabletStatsActor : public NActors::TActorBootstrapped<TTabletStatsActor> {
    const NActors::TActorId Owner;
    std::map<ui64, TTabletStats> Tablets;

    void Handle(TEvTabletStatsBatch::TPtr ev) {
        if (ev->Sender != Owner) {
            return;
        }
        Y_ABORT_UNLESS(ev->Get()->Samples.size() <= TTabletStatsTracker::MaxBatch);
        for (const auto& sample : ev->Get()->Samples) {
            if (sample.Retired) {
                Tablets.erase(sample.TabletId);
                continue;
            }
            auto& row = Tablets[sample.TabletId];
            row.TabletId = sample.TabletId;
            row.DataMappedChunks = sample.Chunks;
            row.SampledAt = ev->Get()->SampledAt;
            row.Interval = sample.Elapsed;
            for (size_t i = 0; i < row.Rates.size(); ++i) {
                row.Rates[i] = CalculateTabletIoRate(sample.Previous[i], sample.Current[i], sample.Elapsed);
            }
        }
        Send(Owner, new TEvTabletStatsAck());
    }

    void Handle(TEvGetTabletStats::TPtr ev) {
        const auto& query = *ev->Get();
        auto result = std::make_unique<TEvTabletStats>();
        if (query.TabletId) {
            if (auto it = Tablets.find(*query.TabletId); it != Tablets.end()) {
                result->Tablets.push_back(it->second);
            }
        } else {
            const size_t limit = std::clamp<size_t>(query.Limit, 1, TTabletStatsTracker::MaxBatch);
            auto it = query.AfterTabletId ? Tablets.upper_bound(*query.AfterTabletId) : Tablets.begin();
            for (; it != Tablets.end() && result->Tablets.size() < limit; ++it) {
                result->Tablets.push_back(it->second);
            }
            if (it != Tablets.end()) {
                result->NextTabletId = result->Tablets.back().TabletId;
            }
        }
        Send(ev->Sender, result.release(), 0, ev->Cookie);
    }

public:
    explicit TTabletStatsActor(NActors::TActorId owner)
        : Owner(owner)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTabletStatsBatch, Handle)
        hFunc(TEvGetTabletStats, Handle)
        cFunc(NActors::TEvents::TSystem::Poison, PassAway)
    )
};

} // namespace

NActors::IActor* CreateTabletStatsActor(NActors::TActorId owner) {
    return new TTabletStatsActor(owner);
}

} // namespace NKikimr::NDDisk
