#include "actor.h"
#include "action.h"
#include "ping.h"
#include "proxy_actor.h"

#include <util/system/defaults.h>

using namespace NKikimrTxUserProxy;

namespace NKikimr::NSQS {

class TUnimplementedRequestActor
    : public TActionActor<TUnimplementedRequestActor>
{
public:
    TUnimplementedRequestActor(const NKikimrClient::TSqsRequest& req, THolder<IReplyCallback> cb)
        : TActionActor(req, EAction::Unknown, std::move(cb))
    {
        Response_.MutableGetQueueUrl()->SetRequestId(RequestId_);
    }

private:
    void DoAction() override {
        SendReplyAndDie();
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableGetQueueUrl()->MutableError();
    }

    TString DoGetQueueName() const override {
        return TString();
    }
};

IActor* CreateActionActor(const NKikimrClient::TSqsRequest& req, THolder<IReplyCallback> cb) {
    Y_ABORT_UNLESS(req.GetRequestId());

#define REQUEST_CASE(action) \
    case NKikimrClient::TSqsRequest::Y_CAT(k, action): {                \
        extern IActor* Y_CAT(Y_CAT(Create, action), Actor)(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb); \
        return Y_CAT(Y_CAT(Create, action), Actor)(req, std::move(cb));  \
    }

    switch (req.GetRequestCase()) {
        REQUEST_CASE(ChangeMessageVisibility)
        REQUEST_CASE(ChangeMessageVisibilityBatch)
        REQUEST_CASE(CreateQueue)
        REQUEST_CASE(CreateUser)
        REQUEST_CASE(DeleteMessage)
        REQUEST_CASE(DeleteMessageBatch)
        REQUEST_CASE(DeleteQueue)
        REQUEST_CASE(DeleteQueueBatch)
        REQUEST_CASE(DeleteUser)
        REQUEST_CASE(ListPermissions)
        REQUEST_CASE(GetQueueAttributes)
        REQUEST_CASE(GetQueueAttributesBatch)
        REQUEST_CASE(GetQueueUrl)
        REQUEST_CASE(ListQueues)
        REQUEST_CASE(ListUsers)
        REQUEST_CASE(ModifyPermissions)
        REQUEST_CASE(PurgeQueue)
        REQUEST_CASE(PurgeQueueBatch)
        REQUEST_CASE(ReceiveMessage)
        REQUEST_CASE(SendMessage)
        REQUEST_CASE(SendMessageBatch)
        REQUEST_CASE(SetQueueAttributes)
        REQUEST_CASE(ListDeadLetterSourceQueues)
        REQUEST_CASE(CountQueues)
        REQUEST_CASE(ListQueueTags)
        REQUEST_CASE(TagQueue)
        REQUEST_CASE(UntagQueue)

#undef REQUEST_CASE

        case NKikimrClient::TSqsRequest::REQUEST_NOT_SET:
            return new TUnimplementedRequestActor(req, std::move(cb));
    }

    Y_ABORT();
}

IActor* CreateProxyActionActor(const NKikimrClient::TSqsRequest& req, THolder<IReplyCallback> cb, bool enableQueueLeader) {
    if (enableQueueLeader && TProxyActor::NeedCreateProxyActor(req)) {
        return new TProxyActor(req, std::move(cb));
    } else {
        return CreateActionActor(req, std::move(cb));
    }
}

IActor* CreatePingActor(THolder<IPingReplyCallback> cb, const TString& requestId) {
    return new TPingActor(std::move(cb), requestId);
}

} // namespace NKikimr::NSQS
