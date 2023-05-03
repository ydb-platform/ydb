#include "msgbus_tabletreq.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {
namespace NMsgBusProxy {

class TMessageBusTxStatusRequestActor : public TMessageBusSimpleTabletRequest<TMessageBusTxStatusRequestActor, NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, NKikimrServices::TActivity::FRONT_SCHEME_TXSTATUS> {
    const ui64 TxId;
    const ui64 PathId;
    bool InProgress;
public:
    TMessageBusTxStatusRequestActor(NMsgBusProxy::TBusMessageContext& msg, const TBusSchemeOperationStatus* casted)
        : TMessageBusSimpleTabletRequest(msg, casted->Record.GetFlatTxId().GetSchemeShardTabletId(), false,
            TDuration::MilliSeconds(casted->Record.GetPollOptions().GetTimeout()), false)
        , TxId(casted->Record.GetFlatTxId().GetTxId())
        , PathId(casted->Record.GetFlatTxId().GetPathId())
        , InProgress(false)
    {}

    TMessageBusTxStatusRequestActor(NMsgBusProxy::TBusMessageContext& msg)
        : TMessageBusTxStatusRequestActor(msg, static_cast<TBusSchemeOperationStatus*>(msg.GetMessage()))
    {}

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&, const TActorContext& ctx) {
        TAutoPtr<NMsgBusProxy::TBusResponse> response(new NMsgBusProxy::TBusResponse());
        response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
        response->Record.MutableFlatTxId()->SetTxId(TxId);
        response->Record.MutableFlatTxId()->SetPathId(PathId);
        response->Record.MutableFlatTxId()->SetSchemeShardTabletId(TabletID);
        SendReplyAndDie(response.Release(), ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {
        InProgress = true;
    }

    void HandleTimeout(const TActorContext& ctx) {
        TAutoPtr<NMsgBusProxy::TBusResponse> response = new NMsgBusProxy::TBusResponse();
        response->Record.SetStatus(InProgress ? NMsgBusProxy::MSTATUS_INPROGRESS : NMsgBusProxy::MSTATUS_TIMEOUT);
        response->Record.MutableFlatTxId()->SetTxId(TxId);
        response->Record.MutableFlatTxId()->SetPathId(PathId);
        response->Record.MutableFlatTxId()->SetSchemeShardTabletId(TabletID);
        SendReplyAndDie(response.Release(), ctx);
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx) {
        TAutoPtr<NMsgBusProxy::TBusResponse> response(new NMsgBusProxy::TBusResponseStatus(NMsgBusProxy::MSTATUS_ERROR, "HandleUndelivered"));
        response->Record.SetErrorReason("Cannot deliver request to the service");
        SendReplyAndDie(response.Release(), ctx);
    }

    NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion* MakeReq(const TActorContext&) {
        THolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion> request(new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion());
        request->Record.SetTxId(TxId);
        return request.Release();
    }

    void StateFunc(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

IActor* CreateMessageBusSchemeOperationStatus(TBusMessageContext &msg) {
    return new TMessageBusTxStatusRequestActor(msg);
}

}
}
