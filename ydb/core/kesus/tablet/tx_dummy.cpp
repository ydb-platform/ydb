#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxDummy : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;

    TTxDummy(TSelf* self, const TActorId& sender, ui64 cookie)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
    {}

    TTxType GetTxType() const override { return TXTYPE_DUMMY; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        Y_UNUSED(ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Sender, new TEvKesus::TEvDummyResponse(), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvDummyRequest::TPtr& ev) {
    if (ev->Get()->Record.GetUseTransactions()) {
        Execute(new TTxDummy(this, ev->Sender, ev->Cookie), TActivationContext::AsActorContext());
    } else {
        Send(ev->Sender, new TEvKesus::TEvDummyResponse(), 0, ev->Cookie);
    }
}

}
}
