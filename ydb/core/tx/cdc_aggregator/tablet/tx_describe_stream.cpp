#include "protocol.h"
#include "tablet_impl.h"

namespace NKikimr::NCdcAggregator {

// Read-only: the executor completes it after the preceding commits, so the
// state captured in Execute is committed by the time it is sent.
class TCdcAggregator::TTxDescribeStream : public NTabletFlatExecutor::TTransactionBase<TCdcAggregator> {
public:
    TTxDescribeStream(TCdcAggregator* self, TEvCdcAggregator::TEvDescribeStream::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DESCRIBE_STREAM;
    }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        Response = MakeHolder<TEvCdcAggregator::TEvDescribeStreamResult>();
        FillStreamDescription(Self->Stream, Response->Record);
        Response->Record.SetOrigin(Self->TabletID());
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Ev->Sender, Response.Release(), 0, Ev->Cookie);
    }

private:
    TEvCdcAggregator::TEvDescribeStream::TPtr Ev;
    THolder<TEvCdcAggregator::TEvDescribeStreamResult> Response;
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvDescribeStream::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDescribeStream(this, std::move(ev)), ctx);
}

} // namespace NKikimr::NCdcAggregator
