#include "msgbus_servicereq.h"
#include <ydb/core/load_test/events.h>

namespace NKikimr {
namespace NMsgBusProxy {

class TBsTestLoadActorRequest : public TActorBootstrapped<TBsTestLoadActorRequest>, public TMessageBusSessionIdentHolder {
    TVector<ui32> NodeIds;
    NKikimr::TEvLoadTestRequest Cmd;
    NKikimrClient::TBsTestLoadResponse Response;
    ui32 ResponsesPending;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TBsTestLoadActorRequest(NKikimrClient::TBsTestLoadRequest& record, NMsgBusProxy::TBusMessageContext& msg)
        : TMessageBusSessionIdentHolder(msg)
        , NodeIds(record.GetNodeId().begin(), record.GetNodeId().end())
        , Cmd(record.GetEvent())
        , ResponsesPending(0)
    {}

    void Bootstrap(const TActorContext& ctx) {
        for (ui32 nodeId : NodeIds) {
            auto msg = MakeHolder<TEvLoad::TEvLoadTestRequest>();
            msg->Record = Cmd;
            msg->Record.SetCookie(nodeId);
            ctx.Send(MakeLoadServiceID(nodeId), msg.Release());
            ++ResponsesPending;
        }
        TVector<ui32>().swap(NodeIds);
        Become(&TBsTestLoadActorRequest::StateFunc);
        CheckResponse(ctx);
    }

    void Handle(TEvLoad::TEvLoadTestResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        ui32 nodeId = record.GetCookie();
        --ResponsesPending;

        NKikimrClient::TBsTestLoadResponse::TItem *item = Response.AddItems();
        item->SetNodeId(nodeId);
        if (record.HasStatus()) {
            item->SetStatus(record.GetStatus());
        }
        if (record.HasErrorReason()) {
            item->SetErrorReason(record.GetErrorReason());
        }

        CheckResponse(ctx);
    }

    void CheckResponse(const TActorContext& ctx) {
        if (!ResponsesPending) {
            auto response = MakeHolder<TBusBsTestLoadResponse>();
            response->Record = Response;
            SendReplyMove(response.Release());
            Die(ctx);
        }
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLoad::TEvLoadTestResponse, Handle);
        }
    }
};

IActor *CreateMessageBusBlobStorageLoadRequest(NMsgBusProxy::TBusMessageContext& msg) {
    NKikimrClient::TBsTestLoadRequest& record = static_cast<TBusBsTestLoadRequest *>(msg.GetMessage())->Record;
    return new TBsTestLoadActorRequest(record, msg);
}

} // NMsgBusProxy
} // NKikimr
