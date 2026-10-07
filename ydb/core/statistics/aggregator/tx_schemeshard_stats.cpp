#include "aggregator_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxSchemeShardStats : public TTxBase {
    NKikimrStat::TEvSchemeShardStats Record;
    std::shared_ptr<TString> UpdatedStats;

    TTxSchemeShardStats(TSelf* self, NKikimrStat::TEvSchemeShardStats&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEMESHARD_STATS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        ui64 schemeShardId = Record.GetSchemeShardId();
        const auto& stats = Record.GetStats();

        NKikimrStat::TSchemeShardStats statRecord;
        Y_PROTOBUF_SUPPRESS_NODISCARD statRecord.ParseFromString(stats);

        YDB_LOG_DEBUG("TTxSchemeShardStats::Execute",
            {"tabletId", Self->TabletID()},
            {"schemeShardId", schemeShardId},
            {"statsByteSize", stats.size()},
            {"entriesCount", statRecord.GetEntries().size()},
            {"areAllStatsFull", statRecord.GetAreAllStatsFull()});

        NIceDb::TNiceDb db(txc.DB);

        TSerializedBaseStats& existingStats = Self->BaseStatistics[schemeShardId];

        // Persist incoming statistics without changes when:
        //  - AreAllStatsFull is unset (old schemeshard) or true, or
        //  - this is the first report AND it is already full.
        // Never bootstrap BaseStatistics from an incomplete (zeroed) snapshot:
        // FinishTraversal would baseline LastAnalyze at 0 and the next full
        // report would look like a huge change ratio. Path discovery below
        // still runs so never-analyzed tables can be scheduled.
        const bool areAllStatsFull =
            !statRecord.HasAreAllStatsFull() || statRecord.GetAreAllStatsFull();
        if (areAllStatsFull) {
            UpdatedStats = std::make_shared<TString>(stats);
        } else if (existingStats.Latest) {
            NKikimrStat::TSchemeShardStats oldStatRecord;
            Y_PROTOBUF_SUPPRESS_NODISCARD oldStatRecord.ParseFromString(*existingStats.Latest);

            struct TOldStats {
                ui64 RowCount = 0;
                ui64 RowUpdates = 0;
                ui64 RowDeletes = 0;
                ui64 BytesSize = 0;
            };
            THashMap<TPathId, TOldStats> oldStatsMap;

            for (const auto& entry : oldStatRecord.GetEntries()) {
                auto& oldEntry = oldStatsMap[TPathId::FromProto(entry.GetPathId())];
                oldEntry.RowCount = entry.GetRowCount();
                oldEntry.RowUpdates = entry.GetRowUpdates();
                oldEntry.RowDeletes = entry.GetRowDeletes();
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
                    newEntry->SetRowUpdates(entry.GetRowUpdates());
                    newEntry->SetRowDeletes(entry.GetRowDeletes());
                    newEntry->SetBytesSize(entry.GetBytesSize());
                } else {
                    auto oldIter = oldStatsMap.find(TPathId::FromProto(entry.GetPathId()));
                    if (oldIter != oldStatsMap.end()) {
                        newEntry->SetRowCount(oldIter->second.RowCount);
                        newEntry->SetRowUpdates(oldIter->second.RowUpdates);
                        newEntry->SetRowDeletes(oldIter->second.RowDeletes);
                        newEntry->SetBytesSize(oldIter->second.BytesSize);
                    } else {
                        newEntry->SetRowCount(0);
                        newEntry->SetRowUpdates(0);
                        newEntry->SetRowDeletes(0);
                        newEntry->SetBytesSize(0);
                    }
                }
            }

            UpdatedStats = std::make_shared<TString>();
            Y_PROTOBUF_SUPPRESS_NODISCARD newStatRecord.SerializeToString(UpdatedStats.get());
        }

        if (UpdatedStats) {
            db.Table<Schema::BaseStatistics>().Key(schemeShardId).Update(
                NIceDb::TUpdate<Schema::BaseStatistics::Stats>(*UpdatedStats));
            existingStats.Latest = UpdatedStats;
        }

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        auto& oldPathIds = Self->ScheduleTraversalsBySchemeShard[schemeShardId];
        std::unordered_set<TPathId> newPathIds;

        for (auto& entry : statRecord.GetEntries()) {
            auto pathId = TPathId::FromProto(entry.GetPathId());
            if (Self->IsStatisticsTable(pathId)) {
                continue;
            }
            newPathIds.insert(pathId);
            if (oldPathIds.find(pathId) == oldPathIds.end()) {
                TStatisticsAggregator::TScheduleTraversal traversalTable;
                traversalTable.PathId = pathId;
                traversalTable.SchemeShardId = schemeShardId;
                traversalTable.LastUpdateTime = TInstant::MicroSeconds(0);
                traversalTable.IsColumnTable = entry.GetIsColumnTable();
                traversalTable.LastAnalyzeRowUpdates = Max<ui64>();
                traversalTable.LastAnalyzeRowDeletes = Max<ui64>();
                auto [it, _] = Self->ScheduleTraversals.emplace(pathId, traversalTable);
                if (!Self->ScheduleTraversalsByTime.Has(&it->second)) {
                    Self->ScheduleTraversalsByTime.Add(&it->second);
                }
                db.Table<Schema::ScheduleTraversals>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::ScheduleTraversals::SchemeShardId>(schemeShardId),
                    NIceDb::TUpdate<Schema::ScheduleTraversals::LastUpdateTime>(0),
                    NIceDb::TUpdate<Schema::ScheduleTraversals::IsColumnTable>(entry.GetIsColumnTable()),
                    NIceDb::TUpdate<Schema::ScheduleTraversals::LastAnalyzeRowUpdates>(Max<ui64>()),
                    NIceDb::TUpdate<Schema::ScheduleTraversals::LastAnalyzeRowDeletes>(Max<ui64>()));
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
        YDB_LOG_DEBUG("TTxSchemeShardStats::Complete",
            {"tabletId", Self->TabletID()});
        if (UpdatedStats) {
            Self->BaseStatistics[Record.GetSchemeShardId()].Committed = UpdatedStats;
        }

        Self->InvalidateCachedChangeCounters();
        Self->ReportBaseStatisticsCounters();
        Self->ReportAnalyzeCounters();
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvSchemeShardStats::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxSchemeShardStats(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
