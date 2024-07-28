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
        db.Table<Schema::BaseStats>().Key(schemeShardId).Update(
            NIceDb::TUpdate<Schema::BaseStats::Stats>(stats));

        Self->BaseStats[schemeShardId] = stats;

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        NKikimrStat::TSchemeShardStats statRecord;
        Y_PROTOBUF_SUPPRESS_NODISCARD statRecord.ParseFromString(stats);

        auto& oldPathIds = Self->ScanTablesBySchemeShard[schemeShardId];
        std::unordered_set<TPathId> newPathIds;

        for (auto& entry : statRecord.GetEntries()) {
            auto pathId = PathIdFromPathId(entry.GetPathId());
            newPathIds.insert(pathId);
            if (oldPathIds.find(pathId) == oldPathIds.end()) {
                TStatisticsAggregator::TScanTable scanTable;
                scanTable.PathId = pathId;
                scanTable.SchemeShardId = schemeShardId;
                scanTable.LastUpdateTime = TInstant::MicroSeconds(0);
                scanTable.IsColumnTable = entry.GetIsColumnTable();
                auto [it, _] = Self->ScanTables.emplace(pathId, scanTable);
                if (!Self->ScanTablesByTime.Has(&it->second)) {
                    Self->ScanTablesByTime.Add(&it->second);
                }
                db.Table<Schema::ScanTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::ScanTables::SchemeShardId>(schemeShardId),
                    NIceDb::TUpdate<Schema::ScanTables::LastUpdateTime>(0),
                    NIceDb::TUpdate<Schema::ScanTables::IsColumnTable>(entry.GetIsColumnTable()));
            }
        }

        for (auto& pathId : oldPathIds) {
            if (newPathIds.find(pathId) == newPathIds.end()) {
                auto it = Self->ScanTables.find(pathId);
                if (it != Self->ScanTables.end()) {
                    if (Self->ScanTablesByTime.Has(&it->second)) {
                        Self->ScanTablesByTime.Remove(&it->second);
                    }
                    Self->ScanTables.erase(it);
                }
                db.Table<Schema::ScanTables>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
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
