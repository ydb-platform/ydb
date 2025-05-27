#include "action.h"

#include <util/generic/is_in.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NSQS {

namespace {

constexpr ui32 FOR_QUEUE = 1;
constexpr ui32 FOR_USER = 2;
constexpr ui32 FOR_MESSAGE = 4;
constexpr ui32 BATCH = 8;
constexpr ui32 FAST = 16;
constexpr ui32 PRIVATE = 32;
constexpr ui32 YMQ_FOR_QUEUE = 64;
constexpr ui32 YMQ_FOR_USER = 128;

struct TActionProps {
    TString StringName;
    TString ConvMethodName;
    EAction Action;
    ui32 Flags;
    EAction NonBatchAction;
};

static const TActionProps ActionProps[] = {
    {"Unknown",                      "unknown",                         EAction::Unknown,                       0,                                      EAction::Unknown},
    {"ChangeMessageVisibility",      "change_message_visibility",       EAction::ChangeMessageVisibility,       FOR_QUEUE | FOR_MESSAGE | FAST,         EAction::ChangeMessageVisibility},
    {"ChangeMessageVisibilityBatch", "change_message_visibility_batch", EAction::ChangeMessageVisibilityBatch,  FOR_QUEUE | FOR_MESSAGE | BATCH | FAST, EAction::ChangeMessageVisibility},
    {"CreateQueue",                  "create_queue",                    EAction::CreateQueue,                   FOR_USER  | YMQ_FOR_USER,               EAction::CreateQueue},
    {"CreateUser",                   "create_user",                     EAction::CreateUser,                    FOR_USER,                               EAction::CreateUser},
    {"GetQueueAttributes",           "get_queue_attributes",            EAction::GetQueueAttributes,            FOR_QUEUE | YMQ_FOR_QUEUE | FAST,       EAction::GetQueueAttributes},
    {"GetQueueAttributesBatch",      "get_queue_attributes_batch",      EAction::GetQueueAttributesBatch,       FOR_USER | BATCH | FAST | PRIVATE,      EAction::GetQueueAttributes},
    {"GetQueueUrl",                  "get_queue_url",                   EAction::GetQueueUrl,                   FOR_QUEUE | YMQ_FOR_QUEUE | FAST,       EAction::GetQueueUrl},
    {"DeleteMessage",                "delete_message",                  EAction::DeleteMessage,                 FOR_QUEUE | YMQ_FOR_QUEUE | FOR_MESSAGE | FAST,         EAction::DeleteMessage},
    {"DeleteMessageBatch",           "delete_message_batch",            EAction::DeleteMessageBatch,            FOR_QUEUE | YMQ_FOR_QUEUE | FOR_MESSAGE | BATCH | FAST, EAction::DeleteMessage},
    {"DeleteQueue",                  "delete_queue",                    EAction::DeleteQueue,                   FOR_USER | YMQ_FOR_USER,                EAction::DeleteQueue},
    {"DeleteQueueBatch",             "delete_queue_batch",              EAction::DeleteQueueBatch,              FOR_USER | BATCH | PRIVATE,             EAction::DeleteQueue},
    {"DeleteUser",                   "delete_user",                     EAction::DeleteUser,                    FOR_USER,                               EAction::DeleteUser},
    {"ListQueues",                   "list_queues",                     EAction::ListQueues,                    FOR_USER | YMQ_FOR_USER | FAST,         EAction::ListQueues},
    {"ListUsers",                    "list_users",                      EAction::ListUsers,                     FOR_USER | FAST,                        EAction::ListUsers},
    {"PurgeQueue",                   "purge_queue",                     EAction::PurgeQueue,                    FOR_QUEUE | YMQ_FOR_QUEUE | FAST,       EAction::PurgeQueue},
    {"PurgeQueueBatch",              "purge_queue_batch",               EAction::PurgeQueueBatch,               FOR_USER | BATCH | FAST | PRIVATE,      EAction::PurgeQueue},
    {"ReceiveMessage",               "receive_message",                 EAction::ReceiveMessage,                FOR_QUEUE | YMQ_FOR_QUEUE | FOR_MESSAGE | FAST,         EAction::ReceiveMessage},
    {"SendMessage",                  "send_message",                    EAction::SendMessage,                   FOR_QUEUE | YMQ_FOR_QUEUE | FOR_MESSAGE | FAST,         EAction::SendMessage},
    {"SendMessageBatch",             "send_message_batch",              EAction::SendMessageBatch,              FOR_QUEUE | YMQ_FOR_QUEUE | FOR_MESSAGE | BATCH | FAST, EAction::SendMessage},
    {"SetQueueAttributes",           "set_queue_attributes",            EAction::SetQueueAttributes,            FOR_QUEUE | YMQ_FOR_QUEUE | FAST,                       EAction::SetQueueAttributes},
    {"ModifyPermissions",            "modify_permissions",              EAction::ModifyPermissions,             FOR_USER | FAST,                        EAction::ModifyPermissions},
    {"ListPermissions",              "list_permissions",                EAction::ListPermissions,               FOR_USER | FAST,                        EAction::ListPermissions},
    {"ListDeadLetterSourceQueues",   "list_dead_letter_source_queues",  EAction::ListDeadLetterSourceQueues,    FOR_QUEUE | FAST,                       EAction::ListDeadLetterSourceQueues},
    {"CountQueues",                  "count_queues",                    EAction::CountQueues,                   FOR_USER | FAST | PRIVATE,              EAction::CountQueues},
    {"ListQueueTags",                "list_queue_tags",                 EAction::ListQueueTags,                 FOR_QUEUE | YMQ_FOR_QUEUE | FAST,       EAction::ListQueueTags},
    {"TagQueue",                     "tag_queue",                       EAction::TagQueue,                      FOR_QUEUE | YMQ_FOR_QUEUE | FAST,       EAction::TagQueue},
    {"UntagQueue",                   "untag_queue",                     EAction::UntagQueue,                    FOR_QUEUE | YMQ_FOR_QUEUE | FAST,       EAction::UntagQueue},
};

static_assert(Y_ARRAY_SIZE(ActionProps) == EAction::ActionsArraySize);

THashMap<TString, EAction> GetStringToAction() {
    THashMap<TString, EAction> ret;
    for (int action = EAction::Unknown + 1; action < EAction::ActionsArraySize; ++action) {
        const TActionProps& props = ActionProps[action];
        ret[props.StringName] = props.Action;
    }
    return ret;
}

const TActionProps& GetProps(EAction action) {
    int index = static_cast<int>(action);
    if (index < 0 || index >= EAction::ActionsArraySize) {
        index = EAction::Unknown;
    }
    return ActionProps[index];
}

const THashMap<TString, EAction> StringToAction = GetStringToAction();

} // namespace

EAction ActionFromString(const TString& name) {
    auto ai = StringToAction.find(name);
    if (ai == StringToAction.end()) {
        return EAction::Unknown;
    }
    return ai->second;
}

const TString& ActionToString(EAction action) {
    return GetProps(action).StringName;
}

const TString& ActionToCloudConvMethod(EAction action) {
    return GetProps(action).ConvMethodName;
}

bool IsBatchAction(EAction action) {
    return GetProps(action).Flags & BATCH;
}

EAction GetNonBatchAction(EAction action) {
    return GetProps(action).NonBatchAction;
}

bool IsActionForQueue(EAction action) {
    return GetProps(action).Flags & FOR_QUEUE;
}

bool IsActionForQueueYMQ(EAction action) {
    return GetProps(action).Flags & YMQ_FOR_QUEUE;
}

bool IsActionForUser(EAction action) {
    return GetProps(action).Flags & FOR_USER;
}

bool IsActionForUserYMQ(EAction action) {
    return GetProps(action).Flags & YMQ_FOR_USER;
}

bool IsActionForMessage(EAction action) {
    return GetProps(action).Flags & FOR_MESSAGE;
}

bool IsFastAction(EAction action) {
    return GetProps(action).Flags & FAST;
}

bool IsPrivateAction(EAction action) {
    return GetProps(action).Flags & PRIVATE;
}

bool IsProxyAction(EAction action) {
#define ACTION_CASE(a) case EAction::a: \
    return true;

    switch (action) {
        ENUMERATE_PROXY_ACTIONS(ACTION_CASE)
    default:
        return false;
    }

#undef ACTION_CASE
}


// Actions modifying a schema
#define ENUMERATE_MODIFY_SCHEME_ACTIONS(macro)      \
        macro(CreateQueue)      \
        macro(CreateUser) \
        macro(DeleteQueue)                \
        macro(DeleteQueueBatch)           \
        macro(DeleteUser)                  \
        macro(ModifyPermissions)


#define SQS_SWITCH_MODIFY_SCHEME_REQUEST(request, default_case)       \
    SQS_SWITCH_REQUEST_CUSTOM(request, ENUMERATE_MODIFY_SCHEME_ACTIONS, default_case)


bool IsModifySchemaRequest(const NKikimrClient::TSqsRequest& req) {
#define SQS_REQUEST_CASE(action) return true;

    SQS_SWITCH_MODIFY_SCHEME_REQUEST(req, return false)

#undef SQS_REQUEST_CASE
}


} // namespace NKikimr::NSQS

template<>
void Out<NKikimr::NSQS::EAction>(IOutputStream& out, typename TTypeTraits<NKikimr::NSQS::EAction>::TFuncParam action) {
    out << ActionToString(action);
}
