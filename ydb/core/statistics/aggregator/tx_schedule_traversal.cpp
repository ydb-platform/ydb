#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScheduleTrasersal : public TTxBase {
    TTxScheduleTrasersal(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEDULE_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTrasersal::Execute");

        Self->Schedule(Self->TraversalPeriod, new TEvPrivate::TEvScheduleTraversal());

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        if (Self->TraversalTableId.PathId) {
            return true; // traverse is in progress
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->ScheduleNextTraversal(db);
        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleTrasersal::Complete");
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleTraversal::TPtr&) {
    Execute(new TTxScheduleTrasersal(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
