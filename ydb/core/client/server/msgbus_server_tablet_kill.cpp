
#include "msgbus_tabletreq.h"

namespace NKikimr {
namespace NMsgBusProxy {

class TMessageBusTabletKillRequest : public TMessageBusSimpleTabletRequest<TMessageBusTabletKillRequest, TEvTablet::TEvTabletDead, NKikimrServices::TActivity::FRONT_POISON_TABLET> {
public:
    TMessageBusTabletKillRequest(TBusMessageContext &msg)
        : TMessageBusSimpleTabletRequest(msg, static_cast<TBusTabletKillRequest*>(msg.GetMessage())->Record.GetTabletID(), false, TDuration::Seconds(60), false)
    {}

    TEvents::TEvPoisonPill* MakeReq(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        return new TEvents::TEvPoisonPill();
    }

    void Handle(TEvTablet::TEvTabletDead::TPtr&, const TActorContext &) {
    }

    virtual NBus::TBusMessage* CreateErrorReply(EResponseStatus, const TActorContext&, const TString& text = TString()) override {
        return new TBusResponseStatus(MSTATUS_OK, text);
    }
};

IActor* CreateMessageBusTabletKillRequest(TBusMessageContext &msg) {
    return new TMessageBusTabletKillRequest(msg);
}

}
}
