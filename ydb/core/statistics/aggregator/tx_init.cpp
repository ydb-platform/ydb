#include "aggregator_impl.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxInit : public TTxBase {
    explicit TTxInit(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxInit::Execute");

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
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded database: " << Self->Database);
                        break;
                    case Schema::SysParam_TraversalStartKey:
                        Self->TraversalStartKey = TSerializedCellVec(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded traversal start key");
                        break;
                    case Schema::SysParam_TraversalTableOwnerId:
                        Self->TraversalPathId.OwnerId = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded traversal table owner id: "
                            << Self->TraversalPathId.OwnerId);
                        break;
                    case Schema::SysParam_TraversalTableLocalPathId:
                        Self->TraversalPathId.LocalPathId = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded traversal table local path id: "
                            << Self->TraversalPathId.LocalPathId);
                        break;
                    case Schema::SysParam_TraversalStartTime: {
                        auto us = FromString<ui64>(value);
                        Self->TraversalStartTime = TInstant::MicroSeconds(us);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded traversal start time: " << us);
                        break;
                    }
                    case Schema::SysParam_TraversalIsColumnTable: {
                        Self->TraversalIsColumnTable = FromString<bool>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded traversal IsColumnTable: " << value);
                        break;
                    }
                    case Schema::SysParam_GlobalTraversalRound: {
                        Self->GlobalTraversalRound = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded global traversal round: " << value);
                        break;
                    }
                    default:
                        SA_LOG_CRIT("[" << Self->TabletID() << "] Unexpected SysParam id: " << id);
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

                Self->BaseStatistics[schemeShardId] = stats;

                if (!rowset.Next()) {
                    return false;
                }
            }

            SA_LOG_D("[" << Self->TabletID() << "] Loaded BaseStatistics: "
                << "schemeshard count# " << Self->BaseStatistics.size());
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

            SA_LOG_D("[" << Self->TabletID() << "] Loaded ColumnStatistics: "
                << "column count# " << Self->CountMinSketches.size());
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

            SA_LOG_D("[" << Self->TabletID() << "] Loaded ScheduleTraversals: "
                << "table count# " << Self->ScheduleTraversals.size());
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

                TForceTraversalOperation operation {
                    .OperationId = operationId,
                    .Tables = {},
                    .Types = types,
                    .ReplyToActorId = {},
                    .CreatedAt = TInstant::FromValue(createdAt)
                };
                Self->ForceTraversals.emplace_back(operation);

                if (!rowset.Next()) {
                    return false;
                }
            }

            Self->TabletCounters->Simple()[COUNTER_FORCE_TRAVERSALS_INFLIGHT_SIZE].Set(Self->ForceTraversals.size());

            SA_LOG_D("[" << Self->TabletID() << "] Loaded ForceTraversalOperations: "
                << "table count# " << Self->ForceTraversals.size());
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
                TString columnTags = rowset.GetValue<Schema::ForceTraversalTables::ColumnTags>();
                TForceTraversalTable::EStatus status = (TForceTraversalTable::EStatus)rowset.GetValue<Schema::ForceTraversalTables::Status>();

                if (status == TForceTraversalTable::EStatus::AnalyzeStarted) {
                    // Resent TEvAnalyzeTable to shards
                    status = TForceTraversalTable::EStatus::None;
                }

                auto pathId = TPathId(ownerId, localPathId);

                TForceTraversalTable operationTable {
                    .PathId = pathId,
                    .ColumnTags = columnTags,
                    .Status = status,
                };
                auto forceTraversalOperation = Self->ForceTraversalOperation(operationId);
                if (!forceTraversalOperation) {
                    SA_LOG_E("[" << Self->TabletID() << "] ForceTraversalTables contains unknown operationId: " << operationId);
                    continue;
                }
                forceTraversalOperation->Tables.emplace_back(operationTable);

                if (!rowset.Next()) {
                    return false;
                }
            }

            SA_LOG_D("[" << Self->TabletID() << "] Loaded ForceTraversalTables: "
                << "table count# " << size);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxInit::Complete");

        Self->SignalTabletActive(ctx);

        Self->EnableStatistics = AppData(ctx)->FeatureFlags.GetEnableStatistics();
        Self->EnableColumnStatistics = AppData(ctx)->FeatureFlags.GetEnableColumnStatistics();
        Self->SubscribeForConfigChanges(ctx);

        Self->Schedule(Self->PropagateInterval, new TEvPrivate::TEvPropagate());

        if (Self->EnableColumnStatistics) {
            Self->Schedule(Self->TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());
            Self->Schedule(Self->SendAnalyzePeriod, new TEvPrivate::TEvSendAnalyze());
            Self->Schedule(Self->AnalyzeDeliveryProblemPeriod, new TEvPrivate::TEvAnalyzeDeliveryProblem());
            Self->Schedule(Self->AnalyzeDeadlinePeriod, new TEvPrivate::TEvAnalyzeDeadline());
        } else {
            SA_LOG_W("[" << Self->TabletID() << "] TTxInit::Complete. EnableColumnStatistics=false");
        }

        Self->InitializeStatisticsTable();

        if (Self->TraversalPathId && Self->TraversalStartKey) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxInit::Complete. Start navigate. PathId " << Self->TraversalPathId);
            Self->NavigateType = ENavigateType::Traversal;
            Self->NavigatePathId = Self->TraversalPathId;
            Self->Navigate();
        }

        Self->Become(&TThis::StateWork);
    }
};

NTabletFlatExecutor::ITransaction* TStatisticsAggregator::CreateTxInit() {
    return new TTxInit(this);
}

} // NKikimr::NStat
