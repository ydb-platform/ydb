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
            auto baseStatisticsRowset = db.Table<Schema::BaseStatistics>().Range().Select();
            auto statisticsRowset = db.Table<Schema::ColumnStatistics>().Range().Select();
            auto scheduleTraversalRowset = db.Table<Schema::ScheduleTraversals>().Range().Select();
            auto forceTraversalRowset = db.Table<Schema::ForceTraversals>().Range().Select();

            if (!sysParamsRowset.IsReady() ||
                !baseStatisticsRowset.IsReady() ||
                !statisticsRowset.IsReady() ||
                !scheduleTraversalRowset.IsReady() ||
                !forceTraversalRowset.IsReady())
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
                        Self->TraversalTableId.PathId.OwnerId = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded traversal table owner id: "
                            << Self->TraversalTableId.PathId.OwnerId);
                        break;
                    case Schema::SysParam_TraversalTableLocalPathId:
                        Self->TraversalTableId.PathId.LocalPathId = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded traversal table local path id: "
                            << Self->TraversalTableId.PathId.LocalPathId);
                        break;
                    case Schema::SysParam_TraversalStartTime: {
                        auto us = FromString<ui64>(value);
                        Self->TraversalStartTime = TInstant::MicroSeconds(us);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded traversal start time: " << us);
                        break;
                    }
                    case Schema::SysParam_TraversalCookie: {
                        Self->TraversalCookie = FromString<ui64>(value);
                        SA_LOG_D("[" << Self->TabletID() << "] Loaded traversal cookie: " << value);
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

        // ForceTraversals
        {
            Self->ForceTraversals.clear();

            auto rowset = db.Table<Schema::ForceTraversals>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui64 cookie = rowset.GetValue<Schema::ForceTraversals::Cookie>();
                ui64 ownerId = rowset.GetValue<Schema::ForceTraversals::OwnerId>();
                ui64 localPathId = rowset.GetValue<Schema::ForceTraversals::LocalPathId>();

                auto pathId = TPathId(ownerId, localPathId);

                TForceTraversal operation {
                    .Cookie = cookie,
                    .PathId = pathId,
                    .ReplyToActorId = {}
                };
                Self->ForceTraversals.emplace_back(operation);

                if (!rowset.Next()) {
                    return false;
                }
            }

            SA_LOG_D("[" << Self->TabletID() << "] Loaded ForceTraversals: "
                << "table count# " << Self->ForceTraversals.size());
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
        Self->Schedule(Self->TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());

        Self->InitializeStatisticsTable();

        if (Self->TraversalTableId.PathId) {
            Self->Navigate();
        }

        Self->Become(&TThis::StateWork);
    }
};

NTabletFlatExecutor::ITransaction* TStatisticsAggregator::CreateTxInit() {
    return new TTxInit(this);
}

} // NKikimr::NStat
