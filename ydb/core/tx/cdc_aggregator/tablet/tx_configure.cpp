#include "protocol.h"
#include "stream_repository_over_local_db.h"
#include "tablet_impl.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxConfigure : public NTabletFlatExecutor::TTransactionBase<TCdcAggregator> {
public:
    TTxConfigure(TCdcAggregator* self, TEvCdcAggregator::TEvConfigure::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CONFIGURE;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TStreamRepositoryOverLocalDb repository(txc.DB);
        Result = ConfigureStream(Self->Stream, repository, ParseConfigureCommand(Ev->Get()->Record));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Result.Status == TConfigureResult::EStatus::Success) {
            Self->UpdateStreamCounters();
        } else {
            YDB_LOG_WARN("Configuration rejected",
                {"tabletId", Self->TabletID()},
                {"error", Result.Error});
        }

        auto response = MakeHolder<TEvCdcAggregator::TEvConfigureResult>();
        FillConfigureResult(Result, response->Record);
        response->Record.SetOrigin(Self->TabletID());
        ctx.Send(Ev->Sender, response.Release(), 0, Ev->Cookie);
    }

private:
    TEvCdcAggregator::TEvConfigure::TPtr Ev;
    TConfigureResult Result;
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvConfigure::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxConfigure(this, std::move(ev)), ctx);
}

} // namespace NKikimr::NCdcAggregator
