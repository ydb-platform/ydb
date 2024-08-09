#pragma once

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NKikimr::NSQS {

struct TErrorClass {
    const TString ErrorCode;
    const ui32 HttpStatusCode;
    const TString DefaultMessage;
    const ui32 Id;

    TErrorClass(TString errorCode, ui32 httpStatusCode, TString defaultMessage, ui32 id);
    TErrorClass() = delete;
    TErrorClass(const TErrorClass&) = delete;
    TErrorClass(TErrorClass&&) = delete;

    static const THashSet<TString>& GetAvailableErrorCodes() {
        return RegisteredCodes;
    }

    static const std::tuple<TString, ui32> GetErrorAndCode(ui32 id);
    static ui32 GetId(const TString& code);

private:
    static THashSet<TString> RegisteredCodes;
    static THashMap<ui32, std::tuple<TString, ui32>> IdToErrorAndCode;
    static THashMap<TString, ui32> ErrorToId;
};

struct TSQSException: public yexception {
    explicit TSQSException(const TErrorClass& errorClass);
    TSQSException(); // NErrors::INTERNAL_FAILURE

    const TErrorClass& ErrorClass;
};

namespace NErrors {
// Common errors
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html
extern const TErrorClass ACCESS_DENIED;
extern const TErrorClass INCOMPLETE_SIGNATURE;
extern const TErrorClass INTERNAL_FAILURE;
extern const TErrorClass INVALID_ACTION;
extern const TErrorClass INVALID_CLIENT_TOKEN_ID;
extern const TErrorClass INVALID_PARAMETER_COMBINATION;
extern const TErrorClass INVALID_PARAMETER_VALUE;
extern const TErrorClass INVALID_QUERY_PARAMETER;
extern const TErrorClass MALFORMED_QUERY_STRING;
extern const TErrorClass MISSING_ACTION;
extern const TErrorClass MISSING_AUTENTICATION_TOKEN;
extern const TErrorClass MISSING_PARAMETER;
extern const TErrorClass OPT_IN_REQUIRED;
extern const TErrorClass REQUEST_EXPIRED;
extern const TErrorClass SERVICE_UNAVAILABLE;
extern const TErrorClass THROTTLING_EXCEPTION;
extern const TErrorClass VALIDATION_ERROR;

// Batch requests errors
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessageBatch.html
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html
extern const TErrorClass BATCH_ENTRY_IDS_NOT_DISTINCT;
extern const TErrorClass EMPTY_BATCH_REQUEST;
extern const TErrorClass INVALID_BATCH_ENTRY_ID;
extern const TErrorClass TOO_MANY_ENTRIES_IN_BATCH_REQUEST;
extern const TErrorClass BATCH_REQUEST_TOO_LONG;

// GetQueueUrl errors
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueUrl.html
extern const TErrorClass NON_EXISTENT_QUEUE;

// ReceiveMessage errors
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
extern const TErrorClass OVER_LIMIT;

// CreateQueue errors
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html
extern const TErrorClass QUEUE_DELETED_RECENTLY;

// ChangeMessageVisibility errors
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
extern const TErrorClass MESSAGE_NOT_INFLIGHT;

// DeleteMessage errors
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
extern const TErrorClass RECEIPT_HANDLE_IS_INVALID;

// GetQueueAttributes errors
// https://docs.aws.amazon.com/en_us/AWSSimpleQueueService/latest/APIReference/API_GetQueueAttributes.html
extern const TErrorClass INVALID_ATTRIBUTE_NAME;
extern const TErrorClass INVALID_ATTRIBUTE_VALUE;

// Leader resolving errors
extern const TErrorClass LEADER_RESOLVING_ERROR;
extern const TErrorClass LEADER_SESSION_ERROR;

// Internal timeout
extern const TErrorClass TIMEOUT;
} // namespace NErrors

} // namespace NKikimr::NSQS
