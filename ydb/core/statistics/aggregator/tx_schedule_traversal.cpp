#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScheduleTraversal : public TTxBase {
    const bool ForceTraversal;

    TTxScheduleTraversal(TSelf* self, bool forceTraversal)
        : TTxBase(self)
        , ForceTraversal(forceTraversal)
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

        YDB_LOG_TRACE("TTxScheduleTraversal::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);

        // First try to dispatch a table analyze operation.
        Self->ScheduleNextAnalyze(db, ctx);

        // Avoid immediate retries of failed background scans.
        if (!ForceTraversal
                && !Self->TraversalPathId
                && Self->StatisticsTablePathId
                && !Self->ScheduleTraversals.empty()
                && Self->StatisticsConfig.GetEnableBackgroundColumnStatsCollection()) {
            Self->ScheduleNextBackgroundTraversal(db, ctx);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_TRACE("TTxScheduleTraversal::Complete",
            {"tabletId", Self->TabletID()});

        Self->ResolveStatisticsTablePathId();
        if (!ForceTraversal) {
            if (Self->EnableColumnStatistics) {
                Self->Schedule(Self->TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());
            } else {
                Self->TraversalSchedulerStarted = false;
            }
        }
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleTraversal::TPtr&) {
    Execute(new TTxScheduleTraversal(this, /*forceTraversal=*/false), TActivationContext::AsActorContext());
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleForceTraversal::TPtr&) {
    Execute(new TTxScheduleTraversal(this, /*forceTraversal=*/true), TActivationContext::AsActorContext());
}

void TStatisticsAggregator::StartTraversalScheduler() {
    if (!EnableColumnStatistics || TraversalSchedulerStarted) {
        return;
    }
    TraversalSchedulerStarted = true;
    Schedule(TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());
}

} // NKikimr::NStat
