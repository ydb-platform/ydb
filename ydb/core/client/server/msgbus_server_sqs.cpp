#include "msgbus_servicereq.h"

#include <ydb/core/ymq/actor/actor.h>
#include <util/generic/guid.h>

namespace NKikimr {
namespace NMsgBusProxy {
namespace {

class TMessageBusCallback
    : public NSQS::IReplyCallback
    , public NMsgBusProxy::TMessageBusSessionIdentHolder
{
public:
    TMessageBusCallback(NMsgBusProxy::TBusMessageContext& msg)
        : TMessageBusSessionIdentHolder(msg)
    {
    }

    void DoSendReply(const NKikimrClient::TSqsResponse& resp) override {
        auto response = MakeHolder<NMsgBusProxy::TBusSqsResponse>();
        response->Record.CopyFrom(resp);
        SendReplyMove(response.Release());
    }
};

} // namespace

IActor* CreateMessageBusSqsRequest(NMsgBusProxy::TBusMessageContext& msg)
{
    NKikimrClient::TSqsRequest record
        = static_cast<NMsgBusProxy::TBusSqsRequest*>(msg.GetMessage())->Record;
    record.SetRequestId(CreateGuidAsString());

    return CreateProxyActionActor(record, MakeHolder<TMessageBusCallback>(msg), true);
}

} // namespace NMsgBusProxy
} // namespace NKikimr
