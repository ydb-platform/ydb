#include "msgbus_servicereq.h"

#include <ydb/core/base/services/datashard_service_id.h>
#include <ydb/core/tx/datashard/testload/test_load_actor.h>

namespace NKikimr::NMsgBusProxy {

class TDsTestLoadActorRequest : public TActorBootstrapped<TDsTestLoadActorRequest>, public TMessageBusSessionIdentHolder {
    ui32 NodeId = 0;
    NKikimrDataShardLoad::TEvTestLoadRequest Cmd;
    NKikimrClient::TDsTestLoadResponse Response;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TDsTestLoadActorRequest(NKikimrClient::TDsTestLoadRequest& record, NMsgBusProxy::TBusMessageContext& msg)
        : TMessageBusSessionIdentHolder(msg)
        , NodeId(record.GetNodeId())
        , Cmd(record.GetEvent())
    {}

    void Bootstrap(const TActorContext& ctx) {
        auto msg = std::make_unique<TEvDataShardLoad::TEvTestLoadRequest>();
        msg->Record = Cmd;
        msg->Record.SetCookie(NodeId);
        ctx.Send(MakeDataShardLoadId(NodeId), msg.release());

        Become(&TDsTestLoadActorRequest::StateFunc);
    }

    void Handle(TEvDataShardLoad::TEvTestLoadResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        ui32 nodeId = record.GetCookie();

        NKikimrClient::TDsTestLoadResponse::TItem *item = Response.AddItems();
        item->SetNodeId(nodeId);
        if (record.HasStatus()) {
            item->SetStatus(record.GetStatus());
        }
        if (record.HasErrorReason()) {
            item->SetErrorReason(record.GetErrorReason());
        }

        auto response = MakeHolder<TBusDsTestLoadResponse>();
        response->Record = Response;
        SendReplyMove(response.Release());
        Die(ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvDataShardLoad::TEvTestLoadResponse, Handle);
        }
    }
};

IActor *CreateMessageBusDataShardLoadRequest(NMsgBusProxy::TBusMessageContext& msg) {
    NKikimrClient::TDsTestLoadRequest& record = static_cast<TBusDsTestLoadRequest *>(msg.GetMessage())->Record;
    return new TDsTestLoadActorRequest(record, msg);
}

} // NKikimr::NMsgBusProxy
