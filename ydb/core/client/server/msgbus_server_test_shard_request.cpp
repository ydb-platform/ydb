#include "msgbus_tabletreq.h"
#include <ydb/core/client/server/msgbus_securereq.h>
#include <ydb/core/test_tablet/events.h>

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

IActor* CreateMessageBusTestShardControl(NKikimr::NMsgBusProxy::TBusMessageContext& msg) {
    return new TMessageBusTestShardControl(msg, static_cast<TBusTestShardControlRequest*>(msg.GetMessage())->Record);
}

} // NKikimr::NMsgBusProxy
