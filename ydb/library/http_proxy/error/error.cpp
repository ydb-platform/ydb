#include "error.h"

namespace NKikimr::NSQS {

THashSet<TString> TErrorClass::RegisteredCodes;

TErrorClass::TErrorClass(TString errorCode, ui32 httpStatusCode, TString defaultMessage)
    : ErrorCode(std::move(errorCode))
    , HttpStatusCode(httpStatusCode)
    , DefaultMessage(std::move(defaultMessage))
{
    RegisteredCodes.insert(ErrorCode);
}

TSQSException::TSQSException(const TErrorClass& errorClass)
    : ErrorClass(errorClass)
{
}

TSQSException::TSQSException()
    : TSQSException(NErrors::INTERNAL_FAILURE)
{
}

namespace NErrors {
extern const TErrorClass ACCESS_DENIED = {
    "AccessDeniedException",
    400,
    "You do not have sufficient access to perform this action."
};

extern const TErrorClass INCOMPLETE_SIGNATURE = {
    "IncompleteSignature",
    400,
    "The request signature does not conform to AWS standards."
};

extern const TErrorClass INTERNAL_FAILURE = {
    "InternalFailure",
    500,
    "The request processing has failed because of an unknown error, exception or failure."
};

extern const TErrorClass INVALID_ACTION = {
    "InvalidAction",
    400,
    "The action or operation requested is invalid. Verify that the action is typed correctly."
};

extern const TErrorClass INVALID_CLIENT_TOKEN_ID = {
    "InvalidClientTokenId",
    403,
    "The X.509 certificate or AWS access key ID provided does not exist in our records."
};

extern const TErrorClass INVALID_PARAMETER_COMBINATION = {
    "InvalidParameterCombination",
    400,
    "Parameters that must not be used together were used together."
};

extern const TErrorClass INVALID_PARAMETER_VALUE = {
    "InvalidParameterValue",
    400,
    "An invalid or out-of-range value was supplied for the input parameter."
};

extern const TErrorClass INVALID_QUERY_PARAMETER = {
    "InvalidQueryParameter",
    400,
    "The AWS query string is malformed or does not adhere to AWS standards."
};

extern const TErrorClass MALFORMED_QUERY_STRING = {
    "MalformedQueryString",
    404,
    "The query string contains a syntax error."
};

extern const TErrorClass MISSING_ACTION = {
    "MissingAction",
    400,
    "The request is missing an action or a required parameter."
};

extern const TErrorClass MISSING_AUTENTICATION_TOKEN = {
    "MissingAuthenticationToken",
    403,
    "The request must contain either a valid (registered) AWS access key ID or X.509 certificate."
};

extern const TErrorClass MISSING_PARAMETER = {
    "MissingParameter",
    400,
    "A required parameter for the specified action is not supplied."
};

extern const TErrorClass OPT_IN_REQUIRED = {
    "OptInRequired",
    403,
    "The AWS access key ID needs a subscription for the service."
};

extern const TErrorClass REQUEST_EXPIRED = {
    "RequestExpired",
    400,
    "The request reached the service more than 15 minutes after the date stamp on the request or more than 15 minutes after the request expiration date (such as for pre-signed URLs), or the date stamp on the request is more than 15 minutes in the future."
};

extern const TErrorClass SERVICE_UNAVAILABLE = {
    "ServiceUnavailable",
    503,
    "The request has failed due to a temporary failure of the server."
};

extern const TErrorClass THROTTLING_EXCEPTION = {
    "ThrottlingException",
    403,
    "The request was denied due to request throttling."
};

extern const TErrorClass VALIDATION_ERROR = {
    "ValidationError",
    400,
    "The input fails to satisfy the constraints specified by an AWS service."
};

extern const TErrorClass BATCH_ENTRY_IDS_NOT_DISTINCT = {
    "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
    400,
    "Two or more batch entries in the request have the same Id."
};

extern const TErrorClass EMPTY_BATCH_REQUEST = {
    "AWS.SimpleQueueService.EmptyBatchRequest",
    400,
    "The batch request doesn't contain any entries."
};

extern const TErrorClass INVALID_BATCH_ENTRY_ID = {
    "AWS.SimpleQueueService.InvalidBatchEntryId",
    400,
    "The Id of a batch entry in a batch request doesn't abide by the specification."
};

extern const TErrorClass TOO_MANY_ENTRIES_IN_BATCH_REQUEST = {
    "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
    400,
    "The batch request contains more entries than permissible."
};

extern const TErrorClass BATCH_REQUEST_TOO_LONG = {
    "AWS.SimpleQueueService.BatchRequestTooLong",
    400,
    "The length of all the messages put together is more than the limit."
};

extern const TErrorClass NON_EXISTENT_QUEUE = {
    "AWS.SimpleQueueService.NonExistentQueue",
    400,
    "The specified queue doesn't exist."
};

extern const TErrorClass OVER_LIMIT = {
    "OverLimit",
    403,
    "The specified action violates a limit. For example, ReceiveMessage returns this error if the maximum number of inflight messages is reached and AddPermission returns this error if the maximum number of permissions for the queue is reached."
};

extern const TErrorClass QUEUE_DELETED_RECENTLY = {
    "AWS.SimpleQueueService.QueueDeletedRecently",
    400,
    "You must wait some time after deleting a queue before you can create another queue with the same name."
};

extern const TErrorClass MESSAGE_NOT_INFLIGHT = {
    "AWS.SimpleQueueService.MessageNotInflight",
    400,
    "The specified message isn't in flight."
};

extern const TErrorClass RECEIPT_HANDLE_IS_INVALID = {
    "ReceiptHandleIsInvalid",
    400,
    "The specified receipt handle isn't valid."
};

extern const TErrorClass INVALID_ATTRIBUTE_NAME = {
    "InvalidAttributeName",
    400,
    "The specified attribute doesn't exist."
};

extern const TErrorClass INVALID_ATTRIBUTE_VALUE = { 
    "InvalidAttributeValue", 
    400, 
    "The specified attribute value is invalid." 
}; 
 
extern const TErrorClass LEADER_RESOLVING_ERROR = {
    "InternalFailure",
    500,
    "Queue leader resolving error."
};

extern const TErrorClass LEADER_SESSION_ERROR = {
    "InternalFailure",
    500,
    "Queue leader session error."
};

extern const TErrorClass TIMEOUT = {
    "InternalFailure",
    504,
    "Timeout."
};

} // namespace NErrors
} // namespace NKikimr::NSQS
