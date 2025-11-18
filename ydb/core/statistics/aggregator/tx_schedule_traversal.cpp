#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScheduleTraversal : public TTxBase {
    TTxScheduleTraversal(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEDULE_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {

        if (!Self->EnableColumnStatistics) {
            SA_LOG_T("[" << Self->TabletID() << "] Column statistics disabled"
                << ", won't schedule traversals");
            return true;
        }

        TDuration time = TDuration::Zero();
        if (!Self->ForceTraversals.empty()) {
            time = ctx.Now() - Self->ForceTraversals.front().CreatedAt;
        }
        Self->TabletCounters->Simple()[COUNTER_FORCE_TRAVERSAL_INFLIGHT_MAX_TIME].Set(time.MicroSeconds());

        if (Self->TraversalPathId) {
            SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTraversal::Execute. Traverse is in progress. PathId " << Self->TraversalPathId);
            return true;
        }

        if (Self->ScheduleTraversals.empty()) {
            SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTraversal. No info from schemeshard");
            return true;
        }

        SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTraversal::Execute");

        NIceDb::TNiceDb db(txc.DB);

        // First try to dispatch a table analyze operation.
        Self->ScheduleNextAnalyze(db, ctx);

        // Next, if there is no analyze operation, try to schedule background traversal.
        if (!Self->TraversalPathId && Self->EnableBackgroundColumnStatsCollection) {
            Self->ScheduleNextBackgroundTraversal(db);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTraversal::Complete");

        Self->Schedule(Self->TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleTraversal::TPtr&) {
    Execute(new TTxScheduleTraversal(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
