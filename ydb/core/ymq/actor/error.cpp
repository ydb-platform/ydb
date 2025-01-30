#include "error.h"

namespace NKikimr::NSQS {

void MakeError(NSQS::TError* error, const TErrorClass& errorClass, const TString& message) {
    error->SetErrorCode(errorClass.ErrorCode);
    error->SetStatus(errorClass.HttpStatusCode);
    if (!message.empty()) {
        error->SetMessage(message);
    } else {
        error->SetMessage(errorClass.DefaultMessage);
    }
}

size_t ErrorsCount(const NKikimrClient::TSqsResponse& response, TAPIStatusesCounters* counters) {
#define RESPONSE_CASE(action)                                           \
    case NKikimrClient::TSqsResponse::Y_CAT(k, action): {               \
        const auto& actionResponse = response.Y_CAT(Get, action)();     \
        const size_t hasError = actionResponse.HasError();              \
        if (counters) {                                                 \
            if (hasError) {                                             \
                counters->AddError(                                     \
                    actionResponse.GetError().GetErrorCode());          \
            } else {                                                    \
                counters->AddOk();                                      \
            }                                                           \
        }                                                               \
        return hasError;                                                \
    }

#define RESPONSE_BATCH_CASE(action)                                     \
    case NKikimrClient::TSqsResponse::Y_CAT(k, action): {               \
        const auto& r = response.Y_CAT(Get, action)();                  \
        size_t errors = r.HasError();                                   \
        if (errors && counters) {                                       \
            counters->AddError(r.GetError().GetErrorCode());            \
        }                                                               \
        for (const auto& entry : r.GetEntries()) {                      \
            const bool hasError = entry.HasError();                     \
            if (hasError) {                                             \
                ++errors;                                               \
            }                                                           \
            if (counters) {                                             \
                if (hasError) {                                         \
                    counters->AddError(                                 \
                        entry.GetError().GetErrorCode());               \
                } else {                                                \
                    counters->AddOk();                                  \
                }                                                       \
            }                                                           \
        }                                                               \
        return errors;                                                  \
    }

    switch (response.GetResponseCase()) {
        RESPONSE_CASE(ChangeMessageVisibility)
        RESPONSE_BATCH_CASE(ChangeMessageVisibilityBatch)
        RESPONSE_CASE(CreateQueue)
        RESPONSE_CASE(CreateUser)
        RESPONSE_CASE(DeleteMessage)
        RESPONSE_BATCH_CASE(DeleteMessageBatch)
        RESPONSE_CASE(DeleteQueue)
        RESPONSE_BATCH_CASE(DeleteQueueBatch)
        RESPONSE_CASE(DeleteUser)
        RESPONSE_CASE(ListPermissions)
        RESPONSE_CASE(GetQueueAttributes)
        RESPONSE_BATCH_CASE(GetQueueAttributesBatch)
        RESPONSE_CASE(GetQueueUrl)
        RESPONSE_CASE(ListQueues)
        RESPONSE_CASE(ListUsers)
        RESPONSE_CASE(ModifyPermissions)
        RESPONSE_CASE(PurgeQueue)
        RESPONSE_BATCH_CASE(PurgeQueueBatch)
        RESPONSE_CASE(ReceiveMessage)
        RESPONSE_CASE(SendMessage)
        RESPONSE_BATCH_CASE(SendMessageBatch)
        RESPONSE_CASE(SetQueueAttributes)
        RESPONSE_CASE(ListDeadLetterSourceQueues)
        RESPONSE_CASE(CountQueues)
        RESPONSE_CASE(ListQueueTags)
        RESPONSE_CASE(TagQueue)
        RESPONSE_CASE(UntagQueue)

    case NKikimrClient::TSqsResponse::RESPONSE_NOT_SET:
        return 0;
    }

#undef RESPONSE_BATCH_CASE
#undef RESPONSE_CASE
}

} // namespace NKikimr::NSQS
