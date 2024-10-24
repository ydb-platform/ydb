#include "msgbus_tabletreq.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {
namespace NMsgBusProxy {

class TMessageBusTxStatusRequestActor : public TMessageBusSimpleTabletRequest<TMessageBusTxStatusRequestActor, NSchemeShard::NEvSchemeShard::TEvNotifyTxCompletionResult, NKikimrServices::TActivity::FRONT_SCHEME_TXSTATUS> {
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

    void Handle(NSchemeShard::NEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&, const TActorContext& ctx) {
        TAutoPtr<NMsgBusProxy::TBusResponse> response(new NMsgBusProxy::TBusResponse());
        response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
        response->Record.MutableFlatTxId()->SetTxId(TxId);
        response->Record.MutableFlatTxId()->SetPathId(PathId);
        response->Record.MutableFlatTxId()->SetSchemeShardTabletId(TabletID);
        SendReplyAndDie(response.Release(), ctx);
    }

    void Handle(NSchemeShard::NEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {
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

    NSchemeShard::NEvSchemeShard::TEvNotifyTxCompletion* MakeReq(const TActorContext&) {
        THolder<NSchemeShard::NEvSchemeShard::TEvNotifyTxCompletion> request(new NSchemeShard::NEvSchemeShard::TEvNotifyTxCompletion());
        request->Record.SetTxId(TxId);
        return request.Release();
    }

    void StateFunc(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            HFunc(NSchemeShard::NEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
            HFunc(NSchemeShard::NEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

IActor* CreateMessageBusSchemeOperationStatus(TBusMessageContext &msg) {
    return new TMessageBusTxStatusRequestActor(msg);
}

}
}
