#pragma once

#include <ydb/core/protos/sqs.pb.h>

#include <util/generic/yexception.h>
#include <util/generic/vector.h>
#include <util/system/defaults.h>

namespace NKikimr::NSQS {

struct TClientOptions {
#define DECLARE_FIELD(name, type, default) \
    type name{default}; \
    TClientOptions& Y_CAT(Set, name)(const type& value) {   \
        name = value; \
        return *this; \
    }

    /// Hostname of server to bind to.
    DECLARE_FIELD(Host, TString, "127.0.0.1");
    /// Service port.
    DECLARE_FIELD(Port, ui16, 2135);
    /// Throw exception when queue's request finished with error.
    DECLARE_FIELD(Throw, bool, true);

#undef DECLARE_FIELD
};

class TQueueException : public yexception {
public:
    TQueueException()
    { }

    TQueueException(const TError& error, const TString& requestId)
        : Error_(error)
        , RequestId(requestId)
    {
        Append(error.GetMessage());
    }

    TString Message() const {
        return Error_.GetMessage();
    }

    int Status() const {
        return Error_.GetStatus();
    }

    const TError& Error() const {
        return Error_;
    }

    const TString& GetRequestId() const {
        return RequestId;
    }

private:
    TError Error_;
    TString RequestId;
};

class TQueueClient {
public:
     TQueueClient(const TClientOptions& options = TClientOptions());
    ~TQueueClient();

    TChangeMessageVisibilityResponse ChangeMessageVisibility(const TChangeMessageVisibilityRequest& req);

    TCreateQueueResponse CreateQueue(const TCreateQueueRequest& req);

    TCreateUserResponse CreateUser(const TCreateUserRequest& req);

    /// Deletes message from a specified persistent queue.
    TDeleteMessageResponse DeleteMessage(const TDeleteMessageRequest& req);
    TDeleteMessageBatchResponse DeleteMessageBatch(const TDeleteMessageBatchRequest& req);

    TDeleteQueueResponse DeleteQueue(const TString& name);

    TDeleteQueueResponse DeleteQueue(const TDeleteQueueRequest& req);

    TDeleteUserResponse DeleteUser(const TDeleteUserRequest& req);

    TListQueuesResponse ListQueues(const TListQueuesRequest& req);

    TListUsersResponse ListUsers(const TListUsersRequest& req);

    TPurgeQueueResponse PurgeQueue(const TPurgeQueueRequest& req);

    TReceiveMessageResponse ReceiveMessage(const TReceiveMessageRequest& req);

    TSendMessageResponse SendMessage(const TSendMessageRequest& req);

    TModifyPermissionsResponse ModifyPermissions(const TModifyPermissionsRequest& req);

    TGetQueueAttributesResponse GetQueueAttributes(const TGetQueueAttributesRequest& req);

    TSetQueueAttributesResponse SetQueueAttributes(const TSetQueueAttributesRequest& req);

    TListPermissionsResponse ListPermissions(const TListPermissionsRequest& req);

private:
    class TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NKikimr::NSQS
