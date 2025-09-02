#pragma once

#include <ydb/core/protos/msgbus.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NSQS {

enum EAction {
    Unknown = 0,
    ChangeMessageVisibility,
    ChangeMessageVisibilityBatch,
    CreateQueue,
    CreateUser,
    GetQueueAttributes,
    GetQueueAttributesBatch,
    GetQueueUrl,
    DeleteMessage,
    DeleteMessageBatch,
    DeleteQueue,
    DeleteQueueBatch,
    DeleteUser,
    ListQueues,
    ListUsers,
    PurgeQueue,
    PurgeQueueBatch,
    ReceiveMessage,
    SendMessage,
    SendMessageBatch,
    SetQueueAttributes,
    ModifyPermissions,
    ListPermissions,
    ListDeadLetterSourceQueues,
    CountQueues,
    ListQueueTags,
    TagQueue,
    UntagQueue,

    ActionsArraySize,
};

EAction ActionFromString(const TString& name);
const TString& ActionToString(EAction action);
const TString& ActionToCloudConvMethod(EAction action);

bool IsBatchAction(EAction action);
bool IsActionForQueue(EAction action);
bool IsActionForQueueYMQ(EAction action);
bool IsActionForUser(EAction action);
bool IsActionForUserYMQ(EAction action);
bool IsProxyAction(EAction action);
bool IsActionForMessage(EAction action);
bool IsFastAction(EAction action);
bool IsPrivateAction(EAction action);

bool IsModifySchemaRequest(const NKikimrClient::TSqsRequest& req);

// get nonbatch action variant for given action
EAction GetNonBatchAction(EAction action);

#define SQS_REQUEST_CASE_WRAP(action)                    \
    case NKikimrClient::TSqsRequest::Y_CAT(k, action): { \
        SQS_REQUEST_CASE(action)                         \
        break;                                           \
    }

// DO NOT proxy account creation or queue listing

#define SQS_SWITCH_REQUEST_CUSTOM(request, enumerate, default_case) \
    switch ((request).GetRequestCase()) {                           \
        enumerate(SQS_REQUEST_CASE_WRAP)                            \
        default:                                                    \
            default_case;                                           \
    }



// Actions with proxy
#define ENUMERATE_PROXY_ACTIONS(macro)      \
        macro(ChangeMessageVisibility)      \
        macro(ChangeMessageVisibilityBatch) \
        macro(DeleteMessage)                \
        macro(DeleteMessageBatch)           \
        macro(DeleteQueue)                  \
        macro(GetQueueAttributes)           \
        macro(PurgeQueue)                   \
        macro(ReceiveMessage)               \
        macro(SendMessage)                  \
        macro(SendMessageBatch)             \
        macro(ListDeadLetterSourceQueues)   \
        macro(SetQueueAttributes)           \
        macro(ListQueueTags)                \
        macro(TagQueue)                     \
        macro(UntagQueue)


// All actions
#define ENUMERATE_ALL_ACTIONS(macro)     \
    macro(ChangeMessageVisibility)       \
    macro(ChangeMessageVisibilityBatch)  \
    macro(CreateQueue)                   \
    macro(CreateUser)                    \
    macro(GetQueueAttributes)            \
    macro(GetQueueAttributesBatch)       \
    macro(GetQueueUrl)                   \
    macro(DeleteMessage)                 \
    macro(DeleteMessageBatch)            \
    macro(DeleteQueue)                   \
    macro(DeleteQueueBatch)              \
    macro(DeleteUser)                    \
    macro(ListQueues)                    \
    macro(ListUsers)                     \
    macro(PurgeQueue)                    \
    macro(PurgeQueueBatch)               \
    macro(ReceiveMessage)                \
    macro(SendMessage)                   \
    macro(SendMessageBatch)              \
    macro(SetQueueAttributes)            \
    macro(ModifyPermissions)             \
    macro(ListPermissions)               \
    macro(CountQueues)                   \
    macro(ListDeadLetterSourceQueues)    \
    macro(ListQueueTags)                 \
    macro(TagQueue)                      \
    macro(UntagQueue)

} // namespace NKikimr::NSQS
