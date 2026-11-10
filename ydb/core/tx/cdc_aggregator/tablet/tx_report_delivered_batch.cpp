#include "protocol.h"
#include "stream_repository_over_local_db.h"
#include "tablet_impl.h"

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxReportDeliveredBatch : public NTabletFlatExecutor::TTransactionBase<TCdcAggregator> {
public:
    TTxReportDeliveredBatch(TCdcAggregator* self, TEvCdcAggregator::TEvReportDeliveredBatch::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_REPORT_DELIVERED_BATCH;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TStreamRepositoryOverLocalDb repository(txc.DB);
        Result = ReportDelivered(Self->Stream, repository, ParseReportDeliveredCommand(Ev->Get()->Record));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto response = MakeHolder<TEvCdcAggregator::TEvReportDeliveredBatchResult>();
        FillReportDeliveredResult(Result, response->Record);
        response->Record.SetOrigin(Self->TabletID());
        ctx.Send(Ev->Sender, response.Release(), 0, Ev->Cookie);

        CountReports();
        if (Result.AdvancedReadyBarrier) {
            Self->OnReadyBarrierCommitted(*Result.AdvancedReadyBarrier);
        }
        Self->UpdateStreamCounters();
    }

private:
    void CountReports() {
        using ESourceStatus = TReportDeliveredResult::TSourceResult::EStatus;
        ui64 advanced = 0;
        ui64 notAdvanced = 0;
        ui64 rejected = 0;
        for (const auto& sourceResult : Result.Results) {
            if (sourceResult.Status != ESourceStatus::Success) {
                ++rejected;
            } else if (sourceResult.Advanced) {
                ++advanced;
            } else {
                ++notAdvanced;
            }
        }

        auto& cumulative = Self->TabletCounters->Cumulative();
        cumulative[COUNTER_REPORTS_ADVANCED].Increment(advanced);
        cumulative[COUNTER_REPORTS_NOT_ADVANCED].Increment(notAdvanced);
        cumulative[COUNTER_REPORTS_REJECTED].Increment(rejected);
    }

private:
    TEvCdcAggregator::TEvReportDeliveredBatch::TPtr Ev;
    TReportDeliveredResult Result;
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvReportDeliveredBatch::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxReportDeliveredBatch(this, std::move(ev)), ctx);
}

} // namespace NKikimr::NCdcAggregator
