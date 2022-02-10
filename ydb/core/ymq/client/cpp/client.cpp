#include "client.h"

#include <ydb/public/lib/deprecated/client/grpc_client.h>

#include <util/generic/yexception.h>
#include <util/string/join.h>
#include <util/system/defaults.h>
#include <util/system/event.h>
#include <util/system/user.h>

using namespace NKikimr::NGRpcProxy;

namespace NKikimr::NSQS {

class TQueueClient::TImpl {
public:
    TImpl(const TClientOptions& options)
        : Options_(options)
        , Client_(TGRpcClientConfig(Join(":", options.Host, options.Port)))
    {
    }

#define METHOD_IMPL(name, hint) \
    Y_CAT(Y_CAT(T, name), Response) name(const Y_CAT(Y_CAT(T, name), Request)& req) { \
        NKikimrClient::TSqsRequest request; \
        Y_CAT(Y_CAT(T, name), Response) resp; \
        request.Y_CAT(Mutable, name)()->CopyFrom(req);  \
        TAutoEvent e; \
        Client_.SqsRequest(request, [e, &resp] (const NGRpcProxy::TGrpcError* error, const NKikimrClient::TSqsResponse& result) mutable \
            { \
                if (error) { \
                    resp.MutableError()->SetStatus(502); \
                    resp.MutableError()->SetMessage(error->first); \
                } else { \
                    resp.CopyFrom(result.Y_CAT(Get, name)());   \
                } \
                e.Signal(); \
            } \
        ); \
        e.WaitI(); \
        if (resp.HasError() && Options_.Throw) { \
            ythrow TQueueException(resp.GetError(), resp.GetRequestId()); \
        } \
        return resp; \
    }

    METHOD_IMPL(ChangeMessageVisibility, "can't change visibility");
    METHOD_IMPL(CreateQueue,             "can't create a queue");
    METHOD_IMPL(CreateUser,              "can't create a user");
    METHOD_IMPL(DeleteMessage,           "can't delete a message");
    METHOD_IMPL(DeleteMessageBatch,      "can't delete messages");
    METHOD_IMPL(DeleteQueue,             "can't delete a queue");
    METHOD_IMPL(DeleteUser,              "can't delete user");
    METHOD_IMPL(ListQueues,              "can't list queues");
    METHOD_IMPL(ListUsers,               "can't list users");
    METHOD_IMPL(PurgeQueue,              "can't purge queue");
    METHOD_IMPL(ReceiveMessage,          "can't receive a message");
    METHOD_IMPL(SendMessage,             "can't enqueue a message");
    METHOD_IMPL(ModifyPermissions,       "can't modify permissions");
    METHOD_IMPL(GetQueueAttributes,      "can't get queue attributes");
    METHOD_IMPL(SetQueueAttributes,      "can't set queue attributes");
    METHOD_IMPL(ListPermissions,         "can't list permissions");

#undef METHOD_IMPL

private:
    const TClientOptions Options_;
    TGRpcClient Client_;
};

TQueueClient::TQueueClient(const TClientOptions& options)
    : Impl_(new TImpl(options))
{ }

TQueueClient::~TQueueClient()
{ }

TChangeMessageVisibilityResponse TQueueClient::ChangeMessageVisibility(const TChangeMessageVisibilityRequest& req) {
    return Impl_->ChangeMessageVisibility(req);
}

TCreateQueueResponse TQueueClient::CreateQueue(const TCreateQueueRequest& req) {
    return Impl_->CreateQueue(req);
}

TCreateUserResponse TQueueClient::CreateUser(const TCreateUserRequest& req) {
    return Impl_->CreateUser(req);
}

TDeleteMessageResponse TQueueClient::DeleteMessage(const TDeleteMessageRequest& req) {
    return Impl_->DeleteMessage(req);
}

TDeleteMessageBatchResponse TQueueClient::DeleteMessageBatch(const TDeleteMessageBatchRequest& req) {
    return Impl_->DeleteMessageBatch(req);
}

TDeleteQueueResponse TQueueClient::DeleteQueue(const TString& name) {
    TDeleteQueueRequest req;
    req.MutableAuth()->SetUserName(GetUsername());
    req.SetQueueName(name);
    return Impl_->DeleteQueue(req);
}

TDeleteQueueResponse TQueueClient::DeleteQueue(const TDeleteQueueRequest& req) {
    return Impl_->DeleteQueue(req);
}

TDeleteUserResponse TQueueClient::DeleteUser(const TDeleteUserRequest& req) {
    return Impl_->DeleteUser(req);
}

TListQueuesResponse TQueueClient::ListQueues(const TListQueuesRequest& req) {
    return Impl_->ListQueues(req);
}

TListUsersResponse TQueueClient::ListUsers(const TListUsersRequest& req) {
    return Impl_->ListUsers(req);
}

TPurgeQueueResponse TQueueClient::PurgeQueue(const TPurgeQueueRequest& req) {
    return Impl_->PurgeQueue(req);
}

TReceiveMessageResponse TQueueClient::ReceiveMessage(const TReceiveMessageRequest& req) {
    return Impl_->ReceiveMessage(req);
}

TSendMessageResponse TQueueClient::SendMessage(const TSendMessageRequest& req) {
    return Impl_->SendMessage(req);
}

TModifyPermissionsResponse TQueueClient::ModifyPermissions(const TModifyPermissionsRequest& req) {
    return Impl_->ModifyPermissions(req);
}

TGetQueueAttributesResponse TQueueClient::GetQueueAttributes(const TGetQueueAttributesRequest& req) {
    return Impl_->GetQueueAttributes(req);
}

TSetQueueAttributesResponse TQueueClient::SetQueueAttributes(const TSetQueueAttributesRequest& req) {
    return Impl_->SetQueueAttributes(req);
}

TListPermissionsResponse TQueueClient::ListPermissions(const TListPermissionsRequest& req) {
    return Impl_->ListPermissions(req);
}

} // namespace NKikimr::NSQS
