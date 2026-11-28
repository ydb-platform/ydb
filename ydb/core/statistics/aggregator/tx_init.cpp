#include "aggregator_impl.h"

#include <util/generic/algorithm.h>
#include <util/string/vector.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxInit : public TTxBase {
    std::vector<TTraversalReply> ExpiredOperations;

    explicit TTxInit(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    void LoadLegacyTraversal(TTraversal& legacy) {
        // The old manual scheduler persisted the operation id and table status,
        // but did not always persist the active path and start time.
        if (legacy.OperationId) {
            auto* operation = Self->ForceTraversalOperation(legacy.OperationId);
            if (!operation || IsTerminalAnalyzeState(operation->State)) {
                return;
            }
            auto table = FindIf(operation->Tables, [](const auto& candidate) {
                return candidate.Status == TForceTraversalTable::EStatus::AnalyzeStarted;
            });
            if (table == operation->Tables.end()) {
                return;
            }
            legacy.PathId = table->PathId;
            legacy.Database = operation->DatabaseName;
            if (!legacy.StartTime) {
                legacy.StartTime = operation->CreatedAt;
            }
        }
        if (legacy.PathId) {
            // A legacy binary may have claimed the path again during a rollback.
            YDB_LOG_DEBUG("Loaded legacy traversal",
                {"tabletId", Self->TabletID()},
                {"operationId", legacy.OperationId.Quote()},
                {"pathId", legacy.PathId},
                {"database", legacy.Database},
                {"startTime", legacy.StartTime});
            Self->ActiveTraversals.insert_or_assign(legacy.PathId, std::move(legacy));
        }
    }

    bool RecoverTraversals(NIceDb::TNiceDb& db, TTraversal& legacy, bool legacyParamsPresent, TInstant now) {
        using TTable = Schema::ActiveTraversals;
        auto rows = db.Table<TTable>().Range().Select();
        if (!rows.IsReady()) {
            return false;
        }
        Self->ActiveTraversals.clear();
        TVector<TPathId> obsolete;
        while (!rows.EndOfSet()) {
            TTraversal traversal;
            traversal.PathId = TPathId(rows.GetValue<TTable::OwnerId>(), rows.GetValue<TTable::LocalPathId>());
            traversal.OperationId = rows.GetValue<TTable::OperationId>();
            traversal.Database = rows.GetValue<TTable::DatabaseName>();
            traversal.StartTime = TInstant::MicroSeconds(rows.GetValue<TTable::StartTime>());

            // An older binary ignores this table. It may have completed this
            // operation or refreshed the background baseline before re-upgrade.
            bool resume;
            if (traversal.OperationId) {
                const auto* operation = Self->ForceTraversalOperation(traversal.OperationId);
                const auto* table = Self->ForceTraversalTable(traversal.OperationId, traversal.PathId);
                resume = operation && !IsTerminalAnalyzeState(operation->State) && table
                    && operation->DatabaseName == traversal.Database
                    && table->Status != TForceTraversalTable::EStatus::TraversalFinished
                    && operation->CreatedAt <= traversal.StartTime;
            } else {
                const auto it = Self->ScheduleTraversals.find(traversal.PathId);
                // A missing schedule entry can mean the table was dropped during the scan.
                // Navigate again so a confirmed drop still cleans up its stored statistics.
                resume = it == Self->ScheduleTraversals.end() || it->second.LastUpdateTime < traversal.StartTime;
            }
            if (resume) {
                Self->ActiveTraversals.emplace(traversal.PathId, std::move(traversal));
            } else {
                obsolete.push_back(traversal.PathId);
            }
            if (!rows.Next()) {
                return false;
            }
        }

        for (const auto& pathId : obsolete) {
            Self->DeletePersistedTraversal(db, pathId);
        }
        if (legacyParamsPresent) {
            LoadLegacyTraversal(legacy);
            for (auto id : {Schema::SysParam_TraversalTableDatabase, Schema::SysParam_TraversalTableOwnerId,
                     Schema::SysParam_TraversalTableLocalPathId, Schema::SysParam_TraversalStartTime,
                     Schema::SysParam_ForceTraversalOperationId}) {
                db.Table<Schema::SysParams>().Key(id).Delete();
            }
        }
        Self->ExpireAnalyzeOperations(db, now, ExpiredOperations);
        // Keyed reservations are the sole start marker. Legacy AnalyzeStarted
        // rows with no matching reservation must become schedulable as well.
        for (auto& operation : Self->ForceTraversals) {
            if (IsTerminalAnalyzeState(operation.State)) {
                continue;
            }
            for (auto& table : operation.Tables) {
                if (table.Status == TForceTraversalTable::EStatus::AnalyzeStarted) {
                    Self->UpdateForceTraversalTableStatus(
                        TForceTraversalTable::EStatus::None, operation.OperationId, table, db);
                }
            }
        }
        TVector<TTraversal*> candidates;
        for (auto& [_, traversal] : Self->ActiveTraversals) {
            candidates.push_back(&traversal);
        }
        SortBy(candidates, [](const TTraversal* traversal) {
            return std::make_tuple(traversal->OperationId.empty(), traversal->StartTime, traversal->PathId);
        });
        // A restart may also change the configured table limit. Excess manual
        // work is durably queued; background entries stay eligible in the heap.
        const size_t limit = Self->EnableColumnStatistics
            ? std::max<ui32>(1, Self->StatisticsConfig.GetAnalyzeMaxTablesInFlight()) : 0;
        for (size_t i = 0; i < candidates.size(); ++i) {
            auto& traversal = *candidates[i];
            if (i >= limit || (traversal.OperationId.empty() && !Self->StatisticsConfig.GetEnableBackgroundColumnStatsCollection())) {
                const auto pathId = traversal.PathId;
                if (traversal.OperationId.empty() && !Self->ScheduleTraversals.contains(pathId)) {
                    // There is no scheduled table to retry this path when capacity or collection returns.
                    Self->PendingBackgroundChecks.insert(pathId);
                    Self->PersistTraversal(db, traversal);
                } else {
                    Self->DeletePersistedTraversal(db, pathId);
                }
                Self->ActiveTraversals.erase(pathId);
                continue;
            }
            if (traversal.OperationId) {
                auto* table = Self->ForceTraversalTable(traversal.OperationId, traversal.PathId);
                table->Status = TForceTraversalTable::EStatus::AnalyzeStarted;
            }
            traversal.Attempt = Self->AllocateTraversalAttempt();
            Self->PersistTraversal(db, traversal);
        }
        return true;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxInit::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);

        { // precharge
            auto sysParamsRowset = db.Table<Schema::SysParams>().Range().Select();
            auto baseStatisticsRowset = db.Table<Schema::BaseStatistics>().Range().Select();
            auto scheduleTraversalRowset = db.Table<Schema::ScheduleTraversals>().Range().Select();
            auto forceTraversalOperationsRowset = db.Table<Schema::ForceTraversalOperations>().Range().Select();
            auto forceTraversalTablesRowset = db.Table<Schema::ForceTraversalTables>().Range().Select();
            auto activeTraversals = db.Table<Schema::ActiveTraversals>().Range().Select();

            if (!sysParamsRowset.IsReady() ||
                !baseStatisticsRowset.IsReady() ||
                !scheduleTraversalRowset.IsReady() ||
                !forceTraversalOperationsRowset.IsReady() ||
                !forceTraversalTablesRowset.IsReady() ||
                !activeTraversals.IsReady())
            {
                return false;
            }
        }

        TTraversal legacy;
        bool legacyParamsPresent = false;
        // SysParams
        {
            auto rowset = db.Table<Schema::SysParams>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui64 id = rowset.GetValue<Schema::SysParams::Id>();
                TString value = rowset.GetValue<Schema::SysParams::Value>();

                switch (id) {
                    case Schema::SysParam_Database:
                        Self->Database = value;
                        YDB_LOG_DEBUG("Loaded database",
                            {"tabletId", Self->TabletID()},
                            {"database", Self->Database});
                        break;
                    case Schema::SysParam_TraversalTableDatabase:
                        legacyParamsPresent = true;
                        legacy.Database = value;
                        break;
                    case Schema::SysParam_TraversalTableOwnerId:
                        legacyParamsPresent = true;
                        legacy.PathId.OwnerId = FromString<ui64>(value);
                        break;
                    case Schema::SysParam_TraversalTableLocalPathId:
                        legacyParamsPresent = true;
                        legacy.PathId.LocalPathId = FromString<ui64>(value);
                        break;
                    case Schema::SysParam_TraversalStartTime:
                        legacyParamsPresent = true;
                        legacy.StartTime = TInstant::MicroSeconds(FromString<ui64>(value));
                        break;
                    case Schema::SysParam_ForceTraversalOperationId:
                        legacyParamsPresent = true;
                        legacy.OperationId = value;
                        break;
                    case 2:  // deprecated SysParam_TraversalStartKey
                    case 11: // deprecated SysParam_TraversalIsColumnTable
                    case 12: // deprecated SysParam_GlobalTraversalRound
                        break;
                    default:
                        YDB_LOG_CRIT("Unexpected SysParam",
                            {"tabletId", Self->TabletID()},
                            {"id", id});
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // BaseStatistics
        {
            Self->BaseStatistics.clear();

            auto rowset = db.Table<Schema::BaseStatistics>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui64 schemeShardId = rowset.GetValue<Schema::BaseStatistics::SchemeShardId>();
                TString stats = rowset.GetValue<Schema::BaseStatistics::Stats>();
                auto& schemeShardStats = Self->BaseStatistics[schemeShardId];
                schemeShardStats.Committed = std::make_shared<TString>(std::move(stats));
                schemeShardStats.Latest = schemeShardStats.Committed;

                if (!rowset.Next()) {
                    return false;
                }
            }

            YDB_LOG_DEBUG("Loaded BaseStatistics",
                {"tabletId", Self->TabletID()},
                {"schemeShardsCount", Self->BaseStatistics.size()});
        }

        // ScheduleTraversals
        {
            Self->ScheduleTraversalsByTime.Clear();
            Self->ScheduleTraversalsBySchemeShard.clear();
            Self->ScheduleTraversals.clear();

            auto rowset = db.Table<Schema::ScheduleTraversals>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui64 ownerId = rowset.GetValue<Schema::ScheduleTraversals::OwnerId>();
                ui64 localPathId = rowset.GetValue<Schema::ScheduleTraversals::LocalPathId>();
                ui64 lastUpdateTime = rowset.GetValue<Schema::ScheduleTraversals::LastUpdateTime>();
                ui64 schemeShardId = rowset.GetValue<Schema::ScheduleTraversals::SchemeShardId>();
                bool isColumnTable = rowset.GetValue<Schema::ScheduleTraversals::IsColumnTable>();
                ui64 lastAnalyzeRowUpdates = rowset.GetValueOrDefault<Schema::ScheduleTraversals::LastAnalyzeRowUpdates>(Max<ui64>());
                ui64 lastAnalyzeRowDeletes = rowset.GetValueOrDefault<Schema::ScheduleTraversals::LastAnalyzeRowDeletes>(Max<ui64>());

                auto pathId = TPathId(ownerId, localPathId);

                TScheduleTraversal scheduleTraversal;
                scheduleTraversal.PathId = pathId;
                scheduleTraversal.SchemeShardId = schemeShardId;
                scheduleTraversal.LastUpdateTime = TInstant::MicroSeconds(lastUpdateTime);
                scheduleTraversal.IsColumnTable = isColumnTable;
                scheduleTraversal.LastAnalyzeRowUpdates = lastAnalyzeRowUpdates;
                scheduleTraversal.LastAnalyzeRowDeletes = lastAnalyzeRowDeletes;

                auto [it, _] = Self->ScheduleTraversals.emplace(pathId, scheduleTraversal);
                Self->ScheduleTraversalsByTime.Add(&it->second);
                Self->ScheduleTraversalsBySchemeShard[schemeShardId].insert(pathId);

                if (!rowset.Next()) {
                    return false;
                }
            }

            YDB_LOG_DEBUG("Loaded ScheduleTraversals",
                {"tabletId", Self->TabletID()},
                {"tablesCount", Self->ScheduleTraversals.size()});
        }

        // ForceTraversalOperations
        {
            Self->ForceTraversals.clear();

            auto rowset = db.Table<Schema::ForceTraversalOperations>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                TString operationId = rowset.GetValue<Schema::ForceTraversalOperations::OperationId>();
                TString types = rowset.GetValue<Schema::ForceTraversalOperations::Types>();
                ui64 createdAt = rowset.GetValue<Schema::ForceTraversalOperations::CreatedAt>();
                TString databaseName = rowset.GetValue<Schema::ForceTraversalOperations::DatabaseName>();
                TActorId replyToActorId = rowset.GetValue<Schema::ForceTraversalOperations::ReplyToActorId>();
                ui64 endTime = rowset.GetValueOrDefault<Schema::ForceTraversalOperations::EndTime>(0);
                ui64 stateVal = rowset.GetValueOrDefault<Schema::ForceTraversalOperations::State>(0);

                // Guard against a corrupted/future enum value
                auto state = Ydb::Table::AnalyzeState::STATE_UNSPECIFIED;
                if (Ydb::Table::AnalyzeState::State_IsValid(static_cast<int>(stateVal))) {
                    state = static_cast<Ydb::Table::AnalyzeState::State>(stateVal);
                } else {
                    YDB_LOG_WARN("tx_init: invalid persisted AnalyzeState, clamping to STATE_UNSPECIFIED",
                        {"tabletId", Self->TabletID()},
                        {"analyzeState", stateVal},
                        {"operationId", operationId.Quote()});
                }

                TForceTraversalOperation operation {
                    .OperationId = operationId,
                    .DatabaseName = databaseName,
                    .Tables = {},
                    .Types = types,
                    .ReplyToActorId = replyToActorId,
                    .CreatedAt = TInstant::FromValue(createdAt),
                    .State = state,
                    .EndTime = TInstant::FromValue(endTime),
                };
                Self->ForceTraversals.emplace_back(operation);

                if (!rowset.Next()) {
                    return false;
                }
            }

            Self->ForceTraversals.sort([](const auto& left, const auto& right) {
                return std::tie(left.CreatedAt, left.OperationId) < std::tie(right.CreatedAt, right.OperationId);
            });
            Self->RecalcForceTraversalsInflightSizeCounter();

            YDB_LOG_DEBUG("Loaded ForceTraversalOperations",
                {"tabletId", Self->TabletID()},
                {"tableCount", Self->ForceTraversals.size()});
        }

        // ForceTraversalTables
        {
            auto rowset = db.Table<Schema::ForceTraversalTables>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            size_t size = 0;
            while (!rowset.EndOfSet()) {
                ++size;

                TString operationId = rowset.GetValue<Schema::ForceTraversalTables::OperationId>();
                ui64 ownerId = rowset.GetValue<Schema::ForceTraversalTables::OwnerId>();
                ui64 localPathId = rowset.GetValue<Schema::ForceTraversalTables::LocalPathId>();
                TString columnTagsStr = rowset.GetValue<Schema::ForceTraversalTables::ColumnTags>();
                TForceTraversalTable::EStatus status = (TForceTraversalTable::EStatus)rowset.GetValue<Schema::ForceTraversalTables::Status>();
                TString path = rowset.GetValueOrDefault<Schema::ForceTraversalTables::Path>(TString{});

                auto pathId = TPathId(ownerId, localPathId);
                auto columnTags = Scan<ui32>(SplitString(columnTagsStr, ","));

                TForceTraversalTable operationTable {
                    .PathId = pathId,
                    .ColumnTags = std::move(columnTags),
                    .Path = path,
                    .SampleRate = rowset.GetValueOrDefault<Schema::ForceTraversalTables::SampleRate>(1.0),
                    .Status = status,
                };
                auto forceTraversalOperation = Self->ForceTraversalOperation(operationId);
                if (forceTraversalOperation) {
                    forceTraversalOperation->Tables.emplace_back(operationTable);
                } else {
                    YDB_LOG_ERROR("ForceTraversalTables contains unknown operationId",
                        {"tabletId", Self->TabletID()},
                        {"operationId", operationId.Quote()});
                }

                if (!rowset.Next()) {
                    return false;
                }
            }

            YDB_LOG_DEBUG("Loaded ForceTraversalTables",
                {"tabletId", Self->TabletID()},
                {"tablesCount", size});
        }

        return RecoverTraversals(db, legacy, legacyParamsPresent, ctx.Now());
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxInit::Complete",
            {"tabletId", Self->TabletID()});

        Self->SignalTabletActive(ctx);
        Self->ReplyToExpiredAnalyzeOperations(ctx, ExpiredOperations);
        Self->SubscribeForConfigChanges(ctx);

        Self->Schedule(Self->GetPropagateInterval(), new TEvPrivate::TEvPropagate());
        Self->Schedule(Self->AnalyzeCountersReportPeriod, new TEvPrivate::TEvReportAnalyzeCounters());
        Self->ScheduleAnalyzeDeadlineCheck();

        if (Self->EnableColumnStatistics) {
            Self->StartTraversalScheduler();
        } else {
            YDB_LOG_WARN("TTxInit::Complete. EnableColumnStatistics=false",
                {"tabletId", Self->TabletID()});
        }

        if (Self->Database) {
            Self->InitializeStatisticsTable();
        }

        Self->RebuildBaseStatisticsCounters();
        for (auto& [_, traversal] : Self->ActiveTraversals) {
            YDB_LOG_DEBUG("TTxInit::Complete. Resume traversal with TAnalyzeActor.",
                {"tabletId", Self->TabletID()},
                {"operationId", traversal.OperationId.Quote()},
                {"traversalPathId", traversal.PathId});
            Self->StartTraversal(ctx, traversal);
        }
        Self->RefreshScanUserDependencies();

        Self->Become(&TThis::StateWork);
    }
};

NTabletFlatExecutor::ITransaction* TStatisticsAggregator::CreateTxInit() {
    return new TTxInit(this);
}

} // NKikimr::NStat
