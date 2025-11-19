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

        NKikimrStat::TSchemeShardStats statRecord;
        Y_PROTOBUF_SUPPRESS_NODISCARD statRecord.ParseFromString(stats);

        // if statistics is sent from schemeshard for the first time or
        // AreAllStatsFull field is not set (schemeshard is working on previous code version) or
        // statistics is full for all tables
        // then persist incoming statistics without changes
        if (!Self->BaseStatistics.contains(schemeShardId) ||
            !statRecord.HasAreAllStatsFull() || statRecord.GetAreAllStatsFull())
        {
            db.Table<Schema::BaseStatistics>().Key(schemeShardId).Update(
                NIceDb::TUpdate<Schema::BaseStatistics::Stats>(stats));
            Self->BaseStatistics[schemeShardId] = stats;

        } else {
            NKikimrStat::TSchemeShardStats oldStatRecord;
            const auto& oldStats = Self->BaseStatistics[schemeShardId];
            Y_PROTOBUF_SUPPRESS_NODISCARD oldStatRecord.ParseFromString(oldStats);

            struct TOldStats {
                ui64 RowCount = 0;
                ui64 BytesSize = 0;
            };
            THashMap<TPathId, TOldStats> oldStatsMap;

            for (const auto& entry : oldStatRecord.GetEntries()) {
                auto& oldEntry = oldStatsMap[TPathId::FromProto(entry.GetPathId())];
                oldEntry.RowCount = entry.GetRowCount();
                oldEntry.BytesSize = entry.GetBytesSize();
            }

            NKikimrStat::TSchemeShardStats newStatRecord;
            for (const auto& entry : statRecord.GetEntries()) {
                auto* newEntry = newStatRecord.AddEntries();
                *newEntry->MutablePathId() = entry.GetPathId();
                newEntry->SetIsColumnTable(entry.GetIsColumnTable());
                newEntry->SetAreStatsFull(entry.GetAreStatsFull());

                if (entry.GetAreStatsFull()) {
                    newEntry->SetRowCount(entry.GetRowCount());
                    newEntry->SetBytesSize(entry.GetBytesSize());
                } else {
                    auto oldIter = oldStatsMap.find(TPathId::FromProto(entry.GetPathId()));
                    if (oldIter != oldStatsMap.end()) {
                        newEntry->SetRowCount(oldIter->second.RowCount);
                        newEntry->SetBytesSize(oldIter->second.BytesSize);
                    } else {
                        newEntry->SetRowCount(0);
                        newEntry->SetBytesSize(0);
                    }
                }
            }

            TString newStats;
            Y_PROTOBUF_SUPPRESS_NODISCARD newStatRecord.SerializeToString(&newStats);

            db.Table<Schema::BaseStatistics>().Key(schemeShardId).Update(
                NIceDb::TUpdate<Schema::BaseStatistics::Stats>(newStats));
            Self->BaseStatistics[schemeShardId] = newStats;
        }

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        auto& oldPathIds = Self->ScheduleTraversalsBySchemeShard[schemeShardId];
        std::unordered_set<TPathId> newPathIds;

        for (auto& entry : statRecord.GetEntries()) {
            auto pathId = TPathId::FromProto(entry.GetPathId());
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
        Self->ReportBaseStatisticsCounters();
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvSchemeShardStats::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxSchemeShardStats(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
