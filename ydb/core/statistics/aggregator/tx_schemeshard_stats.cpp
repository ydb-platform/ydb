#include "aggregator_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxSchemeShardStats : public TTxBase {
    NKikimrStat::TEvSchemeShardStats Record;
    std::shared_ptr<TString> UpdatedStats;
    TBaseStatisticsTotals UpdatedTotals;

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
            UpdatedTotals = CalculateBaseStatisticsTotals(statRecord);
        } else if (existingStats.Latest) {
            const auto& oldTables = existingStats.GetTables();
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
                    if (entry.HasBytesSize()) {
                        newEntry->SetBytesSize(entry.GetBytesSize());
                    }
                } else {
                    const auto old = oldTables.Value(TPathId::FromProto(entry.GetPathId()), TTableBaseStats{});
                    newEntry->SetRowCount(old.Counters.RowCount);
                    newEntry->SetRowUpdates(old.Counters.RowUpdates);
                    newEntry->SetRowDeletes(old.Counters.RowDeletes);
                    if (old.BytesSize) {
                        newEntry->SetBytesSize(*old.BytesSize);
                    }
                }
            }

            UpdatedStats = std::make_shared<TString>();
            Y_PROTOBUF_SUPPRESS_NODISCARD newStatRecord.SerializeToString(UpdatedStats.get());
            UpdatedTotals = CalculateBaseStatisticsTotals(newStatRecord);
        }

        if (UpdatedStats) {
            db.Table<Schema::BaseStatistics>().Key(schemeShardId).Update(
                NIceDb::TUpdate<Schema::BaseStatistics::Stats>(*UpdatedStats));
            existingStats.Latest = UpdatedStats;
            existingStats.ParsedTables.reset();
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
            auto& stats = Self->BaseStatistics[Record.GetSchemeShardId()];
            // Latest may already contain a later transaction's update.
            Self->BaseStatisticsTotals.RowCount -= stats.CommittedTotals.RowCount;
            Self->BaseStatisticsTotals.BytesSize -= stats.CommittedTotals.BytesSize;
            Self->BaseStatisticsTotals.RowCount += UpdatedTotals.RowCount;
            Self->BaseStatisticsTotals.BytesSize += UpdatedTotals.BytesSize;
            stats.Committed = UpdatedStats;
            stats.CommittedTotals = UpdatedTotals;
            Self->ReportBaseStatisticsCounters();
        }

        Self->AnalyzeCountersDirty = true;
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvSchemeShardStats::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxSchemeShardStats(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
