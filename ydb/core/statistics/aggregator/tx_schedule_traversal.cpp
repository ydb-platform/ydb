#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScheduleTrasersal : public TTxBase {
    TTxScheduleTrasersal(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEDULE_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        ui64 time = 0;
        if (!Self->ForceTraversalsCreationTime.empty()) {
            const auto oldest = *Self->ForceTraversalsCreationTime.begin();
            time = ctx.Now().GetValue() - oldest;
        }
        Self->TabletCounters->Simple()[COUNTER_IN_FLY_ANALYZE_MAXIMUM_WAITING_TIME].Set(time);

        if (Self->TraversalPathId) {
            SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTrasersal::Execute. Traverse is in progress. PathId " << Self->TraversalPathId);
            return true;
        }

        if (Self->ScheduleTraversals.empty()) {
            SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTrasersal. No info from schemeshard");
            return true;
        }

        SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTrasersal::Execute");

        NIceDb::TNiceDb db(txc.DB);

        switch (Self->NavigateType) {
        case ENavigateType::Analyze:
            Self->NavigateType = ENavigateType::Traversal;
            Self->ScheduleNextTraversal(db);
            break;
        case ENavigateType::Traversal:
            Self->NavigateType = ENavigateType::Analyze;
            Self->ScheduleNextAnalyze(db);
            break;
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTrasersal::Complete");

        Self->Schedule(Self->TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleTraversal::TPtr&) {
    Execute(new TTxScheduleTrasersal(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
