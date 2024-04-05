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

            if (!sysParamsRowset.IsReady() ||
                !baseStatsRowset.IsReady() ||
                !statisticsRowset.IsReady())
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

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxInit::Complete");

        Self->SignalTabletActive(ctx);

        Self->EnableStatistics = AppData(ctx)->FeatureFlags.GetEnableStatistics();
        Self->SubscribeForConfigChanges(ctx);

        Self->Schedule(Self->PropagateInterval, new TEvPrivate::TEvPropagate());

        Self->Initialize();
        if (Self->ScanTableId.PathId) {
            Self->InitStartKey = false;
            Self->Navigate();
        }

        Self->Become(&TThis::StateWork);
    }
};

NTabletFlatExecutor::ITransaction* TStatisticsAggregator::CreateTxInit() {
    return new TTxInit(this);
}

} // NKikimr::NStat
