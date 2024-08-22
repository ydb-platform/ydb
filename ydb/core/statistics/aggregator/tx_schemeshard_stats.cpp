#include "aggregator_impl.h"

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxSchemeShardStats : public TTxBase {
    NKikimrStat::TEvSchemeShardStats Record;

    TTxSchemeShardStats(TSelf* self, NKikimrStat::TEvSchemeShardStats&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEMESHARD_STATS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        ui64 schemeShardId = Record.GetSchemeShardId();
        const auto& stats = Record.GetStats();

        SA_LOG_D("[" << Self->TabletID() << "] TTxSchemeShardStats::Execute: "
            << "schemeshard id# " << schemeShardId
            << ", stats size# " << stats.size());

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::BaseStatistics>().Key(schemeShardId).Update(
            NIceDb::TUpdate<Schema::BaseStatistics::Stats>(stats));

        Self->BaseStatistics[schemeShardId] = stats;

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        NKikimrStat::TSchemeShardStats statRecord;
        Y_PROTOBUF_SUPPRESS_NODISCARD statRecord.ParseFromString(stats);

        auto& oldPathIds = Self->ScheduleTraversalsBySchemeShard[schemeShardId];
        std::unordered_set<TPathId> newPathIds;

        for (auto& entry : statRecord.GetEntries()) {
            auto pathId = PathIdFromPathId(entry.GetPathId());
            newPathIds.insert(pathId);
            if (oldPathIds.find(pathId) == oldPathIds.end()) {
                TStatisticsAggregator::TScheduleTraversal traversalTable;
                traversalTable.PathId = pathId;
                traversalTable.SchemeShardId = schemeShardId;
                traversalTable.LastUpdateTime = TInstant::MicroSeconds(0);
                traversalTable.IsColumnTable = entry.GetIsColumnTable();
                auto [it, _] = Self->ScheduleTraversals.emplace(pathId, traversalTable);
                if (!Self->ScheduleTraversalsByTime.Has(&it->second)) {
                    Self->ScheduleTraversalsByTime.Add(&it->second);
                }
                db.Table<Schema::ScheduleTraversals>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::ScheduleTraversals::SchemeShardId>(schemeShardId),
                    NIceDb::TUpdate<Schema::ScheduleTraversals::LastUpdateTime>(0),
                    NIceDb::TUpdate<Schema::ScheduleTraversals::IsColumnTable>(entry.GetIsColumnTable()));
            }
        }

        for (auto& pathId : oldPathIds) {
            if (newPathIds.find(pathId) == newPathIds.end()) {
                auto it = Self->ScheduleTraversals.find(pathId);
                if (it != Self->ScheduleTraversals.end()) {
                    if (Self->ScheduleTraversalsByTime.Has(&it->second)) {
                        Self->ScheduleTraversalsByTime.Remove(&it->second);
                    }
                    Self->ScheduleTraversals.erase(it);
                }
                db.Table<Schema::ScheduleTraversals>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
            }
        }

        oldPathIds.swap(newPathIds);

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSchemeShardStats::Complete");
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvSchemeShardStats::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxSchemeShardStats(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
