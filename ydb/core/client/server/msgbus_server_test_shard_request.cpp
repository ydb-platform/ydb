#include "msgbus_tabletreq.h"
#include <ydb/core/client/server/msgbus_securereq.h>
#include <ydb/core/test_tablet/events.h>
#include <ydb/core/load_test/events.h>
#include <ydb/core/base/services/blobstorage_service_id.h>

namespace NKikimr::NMsgBusProxy {

static constexpr TDuration RequestTimeout = TDuration::Seconds(90);

class TMessageBusTestShardControl : public TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TMessageBusTestShardControl,
        NTestShard::TEvControlResponse, NKikimrServices::TActivity::FRONT_TEST_SHARD_REQUEST>> {
    using TBase = TMessageBusSecureRequest;

    NKikimrClient::TTestShardControlRequest Request;

public:
    TMessageBusTestShardControl(TBusMessageContext& msg, NKikimrClient::TTestShardControlRequest& record)
        : TBase(msg, record.GetTabletId(), true, RequestTimeout, false /* no followers */)
        , Request(std::move(record))
    {
        TBase::SetSecurityToken(Request.GetSecurityToken());
        TBase::SetPeerName(msg.GetPeerName());
        TBase::SetRequireAdminAccess(true);
    }

    void Handle(NTestShard::TEvControlResponse::TPtr /*ev*/, const TActorContext& ctx) {
        auto response = std::make_unique<TBusResponse>();
        auto& record = response->Record;
        record.SetStatus(MSTATUS_OK);
        TBase::SendReplyAndDie(response.release(), ctx);
    }

    NTestShard::TEvControlRequest *MakeReq(const TActorContext&) {
        auto request = std::make_unique<NTestShard::TEvControlRequest>();
        request->Record.CopyFrom(Request);
        return request.release();
    }

     NBus::TBusMessage *CreateErrorReply(EResponseStatus status, const TActorContext& /*ctx*/, const TString& text) override {
        auto response = std::make_unique<TBusResponse>();
        auto& record = response->Record;
        record.SetStatus(status);
        if (text) {
            record.SetErrorReason(text);
        } else {
            record.SetErrorReason(TStringBuilder() << "TMessageBusTestShardControl unknown error TabletId# " << TabletID
                << " Status# " << status);
        }
        return response.release();
    }
};

// The RPC actor only forwards control; accepted workloads belong to the load service.
class TNbsDbgLikeLoadControlRequest : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TNbsDbgLikeLoadControlRequest>> {
    using TBase = TMessageBusSecureRequest<TMessageBusServerRequestBase<TNbsDbgLikeLoadControlRequest>>;
    NKikimrClient::TNbsDbgLikeLoadControl Request;
public:
    TNbsDbgLikeLoadControlRequest(TBusMessageContext& msg, const NKikimrClient::TTestShardControlRequest& request)
        : TBase(msg), Request(request.GetNbsDbgLikeLoadControl())
    {
        SetSecurityToken(request.GetSecurityToken());
        SetPeerName(msg.GetPeerName());
        SetRequireAdminAccess(true);
    }

    void Bootstrap(const TActorContext& ctx) {
        if (Request.GetDatabase().empty()) {
            return Fail("explicit database is required", ctx);
        }
        auto event = std::make_unique<TEvLoad::TEvNbsDbgLikeLoadControl>();
        event->Record = Request;
        ctx.Send(MakeLoadServiceID(Request.GetCoordinatorNodeId() ? Request.GetCoordinatorNodeId() : ctx.SelfID.NodeId()),
            event.release(), IEventHandle::FlagTrackDelivery);
        ctx.Schedule(TDuration::MilliSeconds(Request.GetRpcTimeoutMs()), new TEvents::TEvWakeup);
        Become(&TNbsDbgLikeLoadControlRequest::StateWork);
    }

    void Fail(const TString& error, const TActorContext& ctx) {
        auto response = MakeHolder<TBusResponse>();
        response->Record.SetStatus(MSTATUS_ERROR);
        response->Record.SetErrorReason(error);
        SendReplyMove(response.Release());
        Die(ctx);
    }

    void Handle(TEvLoad::TEvNbsDbgLikeLoadControlResponse::TPtr& ev, const TActorContext& ctx) {
        auto response = MakeHolder<TBusResponse>();
        response->Record.SetStatus(ev->Get()->Record.GetStatus());
        response->Record.SetErrorReason(ev->Get()->Record.GetError());
        *response->Record.MutableNbsDbgLikeLoadControl() = ev->Get()->Record;
        SendReplyMove(response.Release());
        Die(ctx);
    }
    void Undelivered(const TActorContext& ctx) {
        Fail("coordinator load service unavailable; accepted work may still be running", ctx);
    }
    void Timeout(const TActorContext& ctx) {
        Fail("control deadline exceeded; outcome unknown, retry the same request ID", ctx);
    }
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLoad::TEvNbsDbgLikeLoadControlResponse, Handle);
            CFunc(TEvents::TSystem::Undelivered, Undelivered);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
            CFunc(TEvents::TSystem::PoisonPill, TBase::Cancel);
        }
    }
};

IActor* CreateMessageBusTestShardControl(NKikimr::NMsgBusProxy::TBusMessageContext& msg) {
    auto& request = static_cast<TBusTestShardControlRequest*>(msg.GetMessage())->Record;
    if (request.HasNbsDbgLikeLoadControl()) {
        return new TNbsDbgLikeLoadControlRequest(msg, request);
    }
    return new TMessageBusTestShardControl(msg, static_cast<TBusTestShardControlRequest*>(msg.GetMessage())->Record);
}

} // NKikimr::NMsgBusProxy
