#include "msgbus_servicereq.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {
namespace NMsgBusProxy {

class TBsGetRequest : public TActorBootstrapped<TBsGetRequest>, public TMessageBusSessionIdentHolder {
    NKikimrClient::TBsGetRequest Request;
    NKikimrClient::TBsGetResponse Response;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TBsGetRequest(NKikimrClient::TBsGetRequest& record, NMsgBusProxy::TBusMessageContext& msg)
        : TMessageBusSessionIdentHolder(msg)
        , Request(record)
    {}

    void Bootstrap(const TActorContext& ctx) {
        ui32 groupId = 0;
        if (!Request.HasGroupId()) {
            return IssueErrorResponse("GroupId field is not set", ctx);
        } else {
            groupId = Request.GetGroupId();
        }

        THolder<IEventBase> request;

        switch (Request.Query_case()) {
            case NKikimrClient::TBsGetRequest::kExtreme: {
                const TLogoBlobID id(LogoBlobIDFromLogoBlobID(Request.GetExtreme()));
                if (id != id.FullID()) {
                    return IssueErrorResponse("LogoBlobId is not full one", ctx);
                }
                request.Reset(new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::AsyncRead));
                break;
            }

            case NKikimrClient::TBsGetRequest::QUERY_NOT_SET:
                return IssueErrorResponse("Query field is not set", ctx);
        }

        SendToBSProxy(ctx, groupId, request.Release());
        Become(&TBsGetRequest::StateFunc);
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr& ev, const TActorContext& ctx) {
        TEvBlobStorage::TEvGetResult *msg = ev->Get();
        Response.SetStatus(msg->Status);
        if (msg->Status == NKikimrProto::OK && msg->ResponseSz == 1) {
            const TEvBlobStorage::TEvGetResult::TResponse& resp = msg->Responses[0];
            Response.SetStatus(resp.Status);
            Response.SetBuffer(resp.Buffer.ConvertToString());
        }
        IssueResponse(ctx);
    }

    void IssueErrorResponse(TString message, const TActorContext& ctx) {
        Response.SetErrorDescription(message);
        IssueResponse(ctx);
    }

    void IssueResponse(const TActorContext& ctx) {
        auto response = MakeHolder<TBusBsGetResponse>();
        response->Record = Response;
        SendReplyMove(response.Release());
        Die(ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvGetResult, Handle);
        }
    }
};

IActor *CreateMessageBusBlobStorageGetRequest(NMsgBusProxy::TBusMessageContext& msg) {
    NKikimrClient::TBsGetRequest& record = static_cast<TBusBsGetRequest *>(msg.GetMessage())->Record;
    return new TBsGetRequest(record, msg);
}

} // NMsgBusProxy
} // NKikimr
