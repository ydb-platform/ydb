#include "aggregator_impl.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>

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
            auto baseStatsRowset = db.Table<Schema::BaseStats>().Range().Select();
            auto statisticsRowset = db.Table<Schema::Statistics>().Range().Select();
            auto scanTablesRowset = db.Table<Schema::ScanTables>().Range().Select();
            auto scanOperationsRowset = db.Table<Schema::ScanOperations>().Range().Select();

            if (!sysParamsRowset.IsReady() ||
                !baseStatsRowset.IsReady() ||
                !statisticsRowset.IsReady() ||
                !scanTablesRowset.IsReady() ||
                !scanOperationsRowset.IsReady())
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
                        SA_LOG_D("[" << Self->TabletID() << "] Loading database: " << Self->Database);
                        break;
                    case Schema::SysParam_StartKey:
                        Self->StartKey = TSerializedCellVec(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loading start key");
                        break;
                    case Schema::SysParam_ScanTableOwnerId:
                        Self->ScanTableId.PathId.OwnerId = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loading scan table owner id: "
                            << Self->ScanTableId.PathId.OwnerId);
                        break;
                    case Schema::SysParam_ScanTableLocalPathId:
                        Self->ScanTableId.PathId.LocalPathId = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loading scan table local path id: "
                            << Self->ScanTableId.PathId.LocalPathId);
                        break;
                    case Schema::SysParam_ScanStartTime: {
                        auto us = FromString<ui64>(value);
                        Self->ScanStartTime = TInstant::MicroSeconds(us);
                        SA_LOG_D("[" << Self->TabletID() << "] Loading scan start time: " << us);
                        break;
                    }
                    case Schema::SysParam_LastScanOperationId: {
                        Self->LastScanOperationId = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loading last scan operation id: " << value);
                        break;
                    }
                    case Schema::SysParam_IsColumnTable: {
                        Self->IsColumnTable = FromString<bool>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loading IsColumnTable: " << value);
                        break;
                    }
                    case Schema::SysParam_GlobalTraversalRound: {
                        Self->GlobalTraversalRound = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loading global traversal round: " << value);
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

        // BaseStats
        {
            Self->BaseStats.clear();

            auto rowset = db.Table<Schema::BaseStats>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui64 schemeShardId = rowset.GetValue<Schema::BaseStats::SchemeShardId>();
                TString stats = rowset.GetValue<Schema::BaseStats::Stats>();

                Self->BaseStats[schemeShardId] = stats;

                if (!rowset.Next()) {
                    return false;
                }
            }

            SA_LOG_D("[" << Self->TabletID() << "] Loading base stats: "
                << "schemeshard count# " << Self->BaseStats.size());
        }

        // Statistics
        {
            Self->CountMinSketches.clear();

            auto rowset = db.Table<Schema::Statistics>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui32 columnTag = rowset.GetValue<Schema::Statistics::ColumnTag>();
                TString sketch = rowset.GetValue<Schema::Statistics::CountMinSketch>();

                Self->CountMinSketches[columnTag].reset(
                    TCountMinSketch::FromString(sketch.data(), sketch.size()));

                if (!rowset.Next()) {
                    return false;
                }
            }

            SA_LOG_D("[" << Self->TabletID() << "] Loading statistics: "
                << "column count# " << Self->CountMinSketches.size());
        }

        // ScanTables
        {
            Self->ScanTablesByTime.Clear();
            Self->ScanTablesBySchemeShard.clear();
            Self->ScanTables.clear();

            auto rowset = db.Table<Schema::ScanTables>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui64 ownerId = rowset.GetValue<Schema::ScanTables::OwnerId>();
                ui64 localPathId = rowset.GetValue<Schema::ScanTables::LocalPathId>();
                ui64 lastUpdateTime = rowset.GetValue<Schema::ScanTables::LastUpdateTime>();
                ui64 schemeShardId = rowset.GetValue<Schema::ScanTables::SchemeShardId>();
                bool isColumnTable = rowset.GetValue<Schema::ScanTables::IsColumnTable>();

                auto pathId = TPathId(ownerId, localPathId);

                TScanTable scanTable;
                scanTable.PathId = pathId;
                scanTable.SchemeShardId = schemeShardId;
                scanTable.LastUpdateTime = TInstant::MicroSeconds(lastUpdateTime);
                scanTable.IsColumnTable = isColumnTable;

                auto [it, _] = Self->ScanTables.emplace(pathId, scanTable);
                Self->ScanTablesByTime.Add(&it->second);
                Self->ScanTablesBySchemeShard[schemeShardId].insert(pathId);

                if (!rowset.Next()) {
                    return false;
                }
            }

            SA_LOG_D("[" << Self->TabletID() << "] Loading scan tables: "
                << "table count# " << Self->ScanTables.size());
        }

        // ScanOperations
        {
            Self->ScanOperations.Clear();
            Self->ScanOperationsByPathId.clear();

            auto rowset = db.Table<Schema::ScanOperations>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui64 operationId = rowset.GetValue<Schema::ScanOperations::OperationId>();
                ui64 ownerId = rowset.GetValue<Schema::ScanOperations::OwnerId>();
                ui64 localPathId = rowset.GetValue<Schema::ScanOperations::LocalPathId>();

                auto pathId = TPathId(ownerId, localPathId);

                TScanOperation& operation = Self->ScanOperationsByPathId[pathId];
                operation.PathId = pathId;
                operation.OperationId = operationId;
                Self->ScanOperations.PushBack(&operation);

                if (!rowset.Next()) {
                    return false;
                }
            }

            SA_LOG_D("[" << Self->TabletID() << "] Loading scan operations: "
                << "table count# " << Self->ScanOperationsByPathId.size());
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
        Self->Schedule(Self->ScheduleScanIntervalTime, new TEvPrivate::TEvScheduleScan());

        Self->InitializeStatisticsTable();

        if (Self->ScanTableId.PathId) {
            Self->Navigate();
        }

        Self->Become(&TThis::StateWork);
    }
};

NTabletFlatExecutor::ITransaction* TStatisticsAggregator::CreateTxInit() {
    return new TTxInit(this);
}

} // NKikimr::NStat
