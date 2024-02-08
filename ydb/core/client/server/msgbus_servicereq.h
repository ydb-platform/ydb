#pragma once

#include "msgbus_server.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/appdata.h>
#include <ydb/public/lib/base/msgbus.h>
#include <util/network/address.h>

namespace NKikimr {
namespace NMsgBusProxy {

template<typename TDerived, NKikimrServices::TActivity::EType Activity>
class TMessageBusLocalServiceRequest : public TActorBootstrapped<TDerived>, public TMessageBusSessionIdentHolder {
protected:
    TActorId  ServiceID;
    const TDuration Timeout;

    void SendReplyAndDie(NBus::TBusMessage *reply, const TActorContext &ctx) {
        SendReplyMove(reply);
        return this->Die(ctx);
    }

    TMessageBusLocalServiceRequest(TBusMessageContext &msg, const TDuration& timeout)
        : TMessageBusSessionIdentHolder(msg)
        , ServiceID()
        , Timeout(timeout)
    {}

public:
    NBus::TBusMessage* CreateErrorReply(EResponseStatus status, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        return new TBusResponseStatus(status, "undocumented error 3");
    }

    void Bootstrap(const TActorContext &ctx) {
        ServiceID = static_cast<TDerived *>(this)->MakeServiceID(ctx);
        if (ServiceID) {
            IEventBase* request = static_cast<TDerived *>(this)->MakeReq(ctx);

            ctx.Send(ServiceID, request, IEventHandle::FlagTrackDelivery, 0);
            if (Timeout) {
                this->Become(&TDerived::StateFunc, ctx, Timeout, new TEvents::TEvWakeup());
            } else {
                // no timeout
                this->Become(&TDerived::StateFunc);
            }
        } else {
            return SendReplyAndDie(static_cast<TDerived *>(this)->CreateErrorReply(MSTATUS_ERROR, ctx), ctx);
        }
    }

    static constexpr auto ActorActivityType() {
        return Activity;
    }
};

}
}
