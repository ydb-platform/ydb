#pragma once
#include "msgbus_server.h"
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#define CHECK_PROTOBUF_FIELD_PRESENCE(pb, field, errslist) \
    if (!pb.Has##field()) { \
        errslist.push_back( #field );\
    }

namespace NKikimr {
namespace NMsgBusProxy {

template<typename TDerived, typename TTabletReplyEvent>
class TMessageBusTabletRequest : public TActorBootstrapped<TDerived>, public TMessageBusSessionIdentHolder {

protected:
    const TDuration Timeout;
    const bool WithRetry;
    const bool ConnectToFollower;

private:
    TActorId PipeClient;
    ui64 TabletId = 0;

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            PipeClient = TActorId();
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx,
                Sprintf("Tablet pipe client connected with status# %s for tablet %" PRIu64 " Marker# MBT3",
                    NKikimrProto::EReplyStatus_Name(msg->Status).data(), msg->TabletId)), ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        PipeClient = TActorId();
        SendReplyMove(CreateErrorReply(MSTATUS_ERROR, ctx, "Tablet pipe client destroyed Marker# MBT2"));
        return Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        return SendReplyAndDie(CreateErrorReply(MSTATUS_TIMEOUT, ctx, "Tablet request timed out Marker# MBT4"), ctx);
    }
protected:
    void Die(const TActorContext &ctx) override {
        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
            PipeClient = TActorId();
        }
        TActorBootstrapped<TDerived>::Die(ctx);
    }

    virtual NBus::TBusMessage* CreateErrorReply(EResponseStatus status, const TActorContext &ctx,
            const TString& text = TString()) {
        LOG_ERROR_S(ctx, NKikimrServices::MSGBUS_REQUEST, "TabletRequest TabletId# " << TabletId
            << " status# " << status << " text# \"" << text << "\"" << Endl);
        return new TBusResponseStatus(status, text);
    }

    void SendReplyAndDie(NBus::TBusMessage *reply, const TActorContext &ctx) {
        SendReplyMove(reply);
        return Die(ctx);
    }

    TMessageBusTabletRequest(TBusMessageContext &msg, bool withRetry, TDuration timeout, bool connectToFollower)
        : TMessageBusSessionIdentHolder(msg)
        , Timeout(timeout)
        , WithRetry(withRetry)
        , ConnectToFollower(connectToFollower)
    {
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    void Bootstrap(const TActorContext &ctx) {
        NTabletPipe::TClientConfig clientConfig;
        if (WithRetry) {
            clientConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        }

        if (ConnectToFollower) {
            clientConfig.AllowFollower = true;
            clientConfig.ForceFollower = true;
        }

        std::pair<ui64, TAutoPtr<IEventBase>> reqPair = static_cast<TDerived *>(this)->MakeReqPair(ctx);
        TabletId = reqPair.first;

        if (reqPair.first) {
            PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, reqPair.first, clientConfig));
            NTabletPipe::SendData(ctx, PipeClient, reqPair.second.Release());

            this->Become(&TDerived::StateFunc, ctx, Timeout, new TEvents::TEvWakeup());
        } else {
            return SendReplyAndDie(CreateErrorReply(MSTATUS_ERROR, ctx,
                "Unable to obtain TabletId Marker# MBT1"), ctx);
        }
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HTemplFunc(TTabletReplyEvent, static_cast<TDerived *>(this)->Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

template<typename TDerived, typename TTabletReplyEvent, NKikimrServices::TActivity::EType Activity>
class TMessageBusSimpleTabletRequest : public TMessageBusTabletRequest<TDerived, TTabletReplyEvent> {
protected:
    const ui64 TabletID;

    TMessageBusSimpleTabletRequest(TBusMessageContext &msg, ui64 tabletId, bool withRetry, TDuration timeout, bool connectToFollower)
        : TMessageBusTabletRequest<TDerived, TTabletReplyEvent>(msg, withRetry, timeout, connectToFollower)
        , TabletID(tabletId)
    {}

public:
    std::pair<ui64, TAutoPtr<IEventBase>> MakeReqPair(const TActorContext &ctx) {
        return std::pair<ui64, TAutoPtr<IEventBase>>(TabletID, static_cast<TDerived *>(this)->MakeReq(ctx));
    }

    static constexpr auto ActorActivityType() {
        return Activity;
    }
};


}
}
