#include "aggregator_impl.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>

#include <util/string/vector.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxInit : public TTxBase {
    explicit TTxInit(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxInit::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);

        { // precharge
            auto sysParamsRowset = db.Table<Schema::SysParams>().Range().Select();
            auto baseStatisticsRowset = db.Table<Schema::BaseStatistics>().Range().Select();
            auto statisticsRowset = db.Table<Schema::ColumnStatistics>().Range().Select();
            auto scheduleTraversalRowset = db.Table<Schema::ScheduleTraversals>().Range().Select();
            auto forceTraversalOperationsRowset = db.Table<Schema::ForceTraversalOperations>().Range().Select();
            auto forceTraversalTablesRowset = db.Table<Schema::ForceTraversalTables>().Range().Select();

            if (!sysParamsRowset.IsReady() ||
                !baseStatisticsRowset.IsReady() ||
                !statisticsRowset.IsReady() ||
                !scheduleTraversalRowset.IsReady() ||
                !forceTraversalOperationsRowset.IsReady() ||
                !forceTraversalTablesRowset.IsReady())
            {
                return false;
            }
        }

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
                    case Schema::SysParam_TraversalStartKey:
                        Self->TraversalStartKey = TSerializedCellVec(value);
                        YDB_LOG_DEBUG("Loaded traversal start key",
                            {"tabletId", Self->TabletID()});
                        break;
                    case Schema::SysParam_TraversalTableDatabase:
                        Self->TraversalDatabase = value;
                        YDB_LOG_DEBUG("Loaded traversal table database",
                            {"tabletId", Self->TabletID()},
                            {"database", Self->TraversalDatabase});
                        break;
                    case Schema::SysParam_TraversalTableOwnerId:
                        Self->TraversalPathId.OwnerId = FromString<ui64>(value);
                        YDB_LOG_DEBUG("Loaded traversal table owner",
                            {"tabletId", Self->TabletID()},
                            {"id", Self->TraversalPathId.OwnerId});
                        break;
                    case Schema::SysParam_TraversalTableLocalPathId:
                        Self->TraversalPathId.LocalPathId = FromString<ui64>(value);
                        YDB_LOG_DEBUG("Loaded traversal table local path",
                            {"tabletId", Self->TabletID()},
                            {"id", Self->TraversalPathId.LocalPathId});
                        break;
                    case Schema::SysParam_TraversalStartTime: {
                        auto us = FromString<ui64>(value);
                        Self->TraversalStartTime = TInstant::MicroSeconds(us);
                        YDB_LOG_DEBUG("Loaded traversal start time",
                            {"tabletId", Self->TabletID()},
                            {"time", us});
                        break;
                    }
                    case Schema::SysParam_TraversalIsColumnTable: {
                        Self->TraversalIsColumnTable = FromString<bool>(value);
                        YDB_LOG_DEBUG("Loaded traversal IsColumnTable",
                            {"tabletId", Self->TabletID()},
                            {"isColumnTable", value});
                        break;
                    }
                    case Schema::SysParam_GlobalTraversalRound: {
                        Self->GlobalTraversalRound = FromString<ui64>(value);
                        YDB_LOG_DEBUG("Loaded global traversal round",
                            {"tabletId", Self->TabletID()},
                            {"round", value});
                        break;
                    }
                    case Schema::SysParam_ForceTraversalOperationId:
                        Self->ForceTraversalOperationId = value;
                        YDB_LOG_DEBUG("Loaded force traversal operation id",
                            {"tabletId", Self->TabletID()},
                            {"id", value});
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

        // ColumnStatistics
        {
            Self->CountMinSketches.clear();

            auto rowset = db.Table<Schema::ColumnStatistics>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui32 columnTag = rowset.GetValue<Schema::ColumnStatistics::ColumnTag>();
                TString sketch = rowset.GetValue<Schema::ColumnStatistics::CountMinSketch>();

                Self->CountMinSketches[columnTag].reset(
                    TCountMinSketch::FromString(sketch.data(), sketch.size()));

                if (!rowset.Next()) {
                    return false;
                }
            }

            YDB_LOG_DEBUG("Loaded ColumnStatistics",
                {"tabletId", Self->TabletID()},
                {"columnCount", Self->CountMinSketches.size()});
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

                auto pathId = TPathId(ownerId, localPathId);

                TScheduleTraversal scheduleTraversal;
                scheduleTraversal.PathId = pathId;
                scheduleTraversal.SchemeShardId = schemeShardId;
                scheduleTraversal.LastUpdateTime = TInstant::MicroSeconds(lastUpdateTime);
                scheduleTraversal.IsColumnTable = isColumnTable;

                auto [it, _] = Self->ScheduleTraversals.emplace(pathId, scheduleTraversal);
                Self->ScheduleTraversalsByTime.Add(&it->second);
                Self->ScheduleTraversalsBySchemeShard[schemeShardId].insert(pathId);

                if (!rowset.Next()) {
                    return false;
                }
            }

            YDB_LOG_DEBUG("Loaded ScheduleTraversals: table",
                {"tabletId", Self->TabletID()},
                {"count", Self->ScheduleTraversals.size()});
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
                    YDB_LOG_WARN("For clamping to STATE_UNSPECIFIED",
                        {"tabletId", Self->TabletID()},
                        {"analyzeState", stateVal},
                        {"operationId", operationId});
                }

                TForceTraversalOperation operation {
                    .OperationId = operationId,
                    .DatabaseName = databaseName,
                    .Tables = {},
                    .Types = types,
                    .ReplyToActorId = replyToActorId,
                    .RequestingActorReattached = false,
                    .CreatedAt = TInstant::FromValue(createdAt),
                    .State = state,
                    .EndTime = TInstant::FromValue(endTime),
                };
                Self->ForceTraversals.emplace_back(operation);

                if (!rowset.Next()) {
                    return false;
                }
            }

            Self->RecalcForceTraversalsInflightSizeCounter();

            YDB_LOG_DEBUG("Loaded ForceTraversalOperations: table",
                {"tabletId", Self->TabletID()},
                {"count", Self->ForceTraversals.size()});
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
                    .Status = status,
                };
                auto forceTraversalOperation = Self->ForceTraversalOperation(operationId);
                if (forceTraversalOperation) {
                    forceTraversalOperation->Tables.emplace_back(operationTable);
                } else {
                    YDB_LOG_ERROR("ForceTraversalTables contains unknown",
                        {"tabletId", Self->TabletID()},
                        {"operationId", operationId});
                }

                if (!rowset.Next()) {
                    return false;
                }
            }

            YDB_LOG_DEBUG("Loaded ForceTraversalTables: table",
                {"tabletId", Self->TabletID()},
                {"count", size});
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxInit::Complete",
            {"tabletId", Self->TabletID()});

        Self->SignalTabletActive(ctx);

        Self->EnableStatistics = AppData(ctx)->FeatureFlags.GetEnableStatistics();
        Self->EnableColumnStatistics = AppData(ctx)->FeatureFlags.GetEnableColumnStatistics();
        Self->SubscribeForConfigChanges(ctx);

        Self->Schedule(Self->GetPropagateInterval(), new TEvPrivate::TEvPropagate());

        if (Self->EnableColumnStatistics) {
            Self->Schedule(Self->TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());
            Self->Schedule(Self->SendAnalyzePeriod, new TEvPrivate::TEvSendAnalyze());
            Self->Schedule(Self->AnalyzeDeliveryProblemPeriod, new TEvPrivate::TEvAnalyzeDeliveryProblem());
            Self->Schedule(Self->AnalyzeDeadlinePeriod, new TEvPrivate::TEvAnalyzeDeadline());
        } else {
            YDB_LOG_WARN("TTxInit::Complete. EnableColumnStatistics=false",
                {"tabletId", Self->TabletID()});
        }

        if (Self->Database) {
            Self->InitializeStatisticsTable();
        }

        if (Self->TraversalPathId && Self->TraversalStartKey) {
            YDB_LOG_DEBUG("TTxInit::Complete. Start navigate. PathId",
                {"tabletId", Self->TabletID()},
                {"traversalPathId", Self->TraversalPathId});
            Self->NavigateDatabase = Self->TraversalDatabase;
            Self->NavigatePathId = Self->TraversalPathId;
            Self->Navigate();
        }

        Self->ReportBaseStatisticsCounters();
        Self->Become(&TThis::StateWork);
    }
};

NTabletFlatExecutor::ITransaction* TStatisticsAggregator::CreateTxInit() {
    return new TTxInit(this);
}

} // NKikimr::NStat
