#pragma once

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

// get nonbatch action variant for given action
EAction GetNonBatchAction(EAction action);

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
        macro(SetQueueAttributes)

} // namespace NKikimr::NSQS
