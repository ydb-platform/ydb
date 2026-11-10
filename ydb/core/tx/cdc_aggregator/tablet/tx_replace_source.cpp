#include "protocol.h"
#include "stream_repository_over_local_db.h"
#include "tablet_impl.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxReplaceSource : public NTabletFlatExecutor::TTransactionBase<TCdcAggregator> {
public:
    TTxReplaceSource(TCdcAggregator* self, TEvCdcAggregator::TEvReplaceSource::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_REPLACE_SOURCE;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TStreamRepositoryOverLocalDb repository(txc.DB);
        Result = ReplaceStreamSource(Self->Stream, repository, ParseReplaceSourceCommand(Ev->Get()->Record));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const auto& record = Ev->Get()->Record;
        if (Result.Status == TReplaceSourceResult::EStatus::Success) {
            Self->TabletCounters->Cumulative()[COUNTER_SOURCE_REPLACEMENTS].Increment(1);
            YDB_LOG_INFO("Source replaced",
                {"tabletId", Self->TabletID()},
                {"parent", record.GetParentTabletId()},
                {"children", record.ChildTabletIdsSize()},
                {"operationId", record.GetOperationId()});
        } else {
            YDB_LOG_WARN("Source replacement rejected",
                {"tabletId", Self->TabletID()},
                {"parent", record.GetParentTabletId()},
                {"error", Result.Error});
        }

        auto response = MakeHolder<TEvCdcAggregator::TEvReplaceSourceResult>();
        FillReplaceSourceResult(Result, response->Record);
        response->Record.SetOrigin(Self->TabletID());
        ctx.Send(Ev->Sender, response.Release(), 0, Ev->Cookie);

        if (Result.AdvancedReadyBarrier) {
            Self->OnReadyBarrierCommitted(*Result.AdvancedReadyBarrier);
        }
        Self->UpdateStreamCounters();
    }

private:
    TEvCdcAggregator::TEvReplaceSource::TPtr Ev;
    TReplaceSourceResult Result;
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvReplaceSource::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxReplaceSource(this, std::move(ev)), ctx);
}

} // namespace NKikimr::NCdcAggregator
