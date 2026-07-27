#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScheduleTraversal : public TTxBase {
    TTxScheduleTraversal(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEDULE_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {

        if (!Self->EnableColumnStatistics) {
            YDB_LOG_TRACE("Column statistics disabled, won't schedule traversals",
                {"tabletId", Self->TabletID()});
            return true;
        }

        Self->RecalcForceTraversalInflightMaxTimeCounter(ctx.Now());

        if (Self->TraversalPathId) {
            YDB_LOG_TRACE("TTxScheduleTraversal::Execute. Traverse is in progress.",
                {"tabletId", Self->TabletID()},
                {"pathId", Self->TraversalPathId});
            return true;
        }

        if (Self->ScheduleTraversals.empty()) {
            YDB_LOG_TRACE("TTxScheduleTraversal. No info from schemeshard",
                {"tabletId", Self->TabletID()});
            return true;
        }

        YDB_LOG_TRACE("TTxScheduleTraversal::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);

        // First try to dispatch a table analyze operation.
        Self->ScheduleNextAnalyze(db, ctx);

        // Next, if there is no analyze operation, try to schedule background traversal.
        if (!Self->TraversalPathId
                && Self->StatisticsConfig.GetEnableBackgroundColumnStatsCollection()) {
            Self->ScheduleNextBackgroundTraversal(db);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_TRACE("TTxScheduleTraversal::Complete",
            {"tabletId", Self->TabletID()});

        Self->Schedule(Self->TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleTraversal::TPtr&) {
    Execute(new TTxScheduleTraversal(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
