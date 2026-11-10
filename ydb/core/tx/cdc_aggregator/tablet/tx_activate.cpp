#include "protocol.h"
#include "stream_repository_over_local_db.h"
#include "tablet_impl.h"

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxActivate : public NTabletFlatExecutor::TTransactionBase<TCdcAggregator> {
public:
    TTxActivate(TCdcAggregator* self, TEvCdcAggregator::TEvActivate::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_ACTIVATE;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TStreamRepositoryOverLocalDb repository(txc.DB);
        Result = ActivateStream(Self->Stream, repository, ParseActivateCommand(Ev->Get()->Record));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto response = MakeHolder<TEvCdcAggregator::TEvActivateResult>();
        FillActivateResult(Result, response->Record);
        response->Record.SetOrigin(Self->TabletID());
        ctx.Send(Ev->Sender, response.Release(), 0, Ev->Cookie);

        if (Result.Activated) {
            Self->OnActivationCommitted();
            Self->UpdateStreamCounters();
        }
    }

private:
    TEvCdcAggregator::TEvActivate::TPtr Ev;
    TActivateResult Result;
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvActivate::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxActivate(this, std::move(ev)), ctx);
}

} // namespace NKikimr::NCdcAggregator
