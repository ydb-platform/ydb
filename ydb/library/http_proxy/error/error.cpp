#include "error.h"

namespace NKikimr::NSQS {

THashSet<TString> TErrorClass::RegisteredCodes;
THashMap<ui32, std::tuple<TString, ui32>> TErrorClass::IdToErrorAndCode;
THashMap<TString, ui32> TErrorClass::ErrorToId;

TErrorClass::TErrorClass(TString errorCode, ui32 httpStatusCode, TString defaultMessage, ui32 id)
    : ErrorCode(std::move(errorCode))
    , HttpStatusCode(httpStatusCode)
    , DefaultMessage(std::move(defaultMessage))
    , Id(id)
{
    RegisteredCodes.insert(ErrorCode);
    IdToErrorAndCode.emplace(Id, std::make_tuple(ErrorCode, HttpStatusCode));
    ErrorToId.emplace(ErrorCode, Id);
}

TSQSException::TSQSException(const TErrorClass& errorClass)
    : ErrorClass(errorClass)
{
}

TSQSException::TSQSException()
    : TSQSException(NErrors::INTERNAL_FAILURE)
{
}

const std::tuple<TString, ui32> TErrorClass::GetErrorAndCode(ui32 id)  {
    auto it = NKikimr::NSQS::TErrorClass::IdToErrorAndCode.find(id);
    return (it == NKikimr::NSQS::TErrorClass::IdToErrorAndCode.end())
        ? std::tuple<TString, ui32>(NErrors::INTERNAL_FAILURE.ErrorCode, NErrors::INTERNAL_FAILURE.HttpStatusCode)
        : it->second;
}


ui32 TErrorClass::GetId(const TString& code) {
    auto it = NKikimr::NSQS::TErrorClass::ErrorToId.find(code);
    return (it == NKikimr::NSQS::TErrorClass::ErrorToId.end())
        ? NErrors::INTERNAL_FAILURE.HttpStatusCode
        : it->second;
};


namespace NErrors {
extern const TErrorClass ACCESS_DENIED = {
    "AccessDeniedException",
    400,
    "You do not have sufficient access to perform this action.",
    1
};

extern const TErrorClass INCOMPLETE_SIGNATURE = {
    "IncompleteSignature",
    400,
    "The request signature does not conform to AWS standards.",
    2
};

extern const TErrorClass INTERNAL_FAILURE = {
    "InternalFailure",
    500,
    "The request processing has failed because of an unknown error, exception or failure.",
    3
};

extern const TErrorClass INVALID_ACTION = {
    "InvalidAction",
    400,
    "The action or operation requested is invalid. Verify that the action is typed correctly.",
    4
};

extern const TErrorClass INVALID_CLIENT_TOKEN_ID = {
    "InvalidClientTokenId",
    403,
    "The X.509 certificate or AWS access key ID provided does not exist in our records.",
    5
};

extern const TErrorClass INVALID_PARAMETER_COMBINATION = {
    "InvalidParameterCombination",
    400,
    "Parameters that must not be used together were used together.",
    6
};

extern const TErrorClass INVALID_PARAMETER_VALUE = {
    "InvalidParameterValue",
    400,
    "An invalid or out-of-range value was supplied for the input parameter.",
    7
};

extern const TErrorClass INVALID_QUERY_PARAMETER = {
    "InvalidQueryParameter",
    400,
    "The AWS query string is malformed or does not adhere to AWS standards.",
    8
};

extern const TErrorClass MALFORMED_QUERY_STRING = {
    "MalformedQueryString",
    404,
    "The query string contains a syntax error.",
    9
};

extern const TErrorClass MISSING_ACTION = {
    "MissingAction",
    400,
    "The request is missing an action or a required parameter.",
    10
};

extern const TErrorClass MISSING_AUTENTICATION_TOKEN = {
    "MissingAuthenticationToken",
    403,
    "The request must contain either a valid (registered) AWS access key ID or X.509 certificate.",
    11
};

extern const TErrorClass MISSING_PARAMETER = {
    "MissingParameter",
    400,
    "A required parameter for the specified action is not supplied.",
    12
};

extern const TErrorClass OPT_IN_REQUIRED = {
    "OptInRequired",
    403,
    "The AWS access key ID needs a subscription for the service.",
    13
};

extern const TErrorClass REQUEST_EXPIRED = {
    "RequestExpired",
    400,
    "The request reached the service more than 15 minutes after the date stamp on the request or more than 15 minutes after the request expiration date (such as for pre-signed URLs), or the date stamp on the request is more than 15 minutes in the future.",
    14
};

extern const TErrorClass SERVICE_UNAVAILABLE = {
    "ServiceUnavailable",
    503,
    "The request has failed due to a temporary failure of the server.",
    15
};

extern const TErrorClass THROTTLING_EXCEPTION = {
    "ThrottlingException",
    403,
    "The request was denied due to request throttling.",
    16
};

extern const TErrorClass VALIDATION_ERROR = {
    "ValidationError",
    400,
    "The input fails to satisfy the constraints specified by an AWS service.",
    17
};

extern const TErrorClass BATCH_ENTRY_IDS_NOT_DISTINCT = {
    "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
    400,
    "Two or more batch entries in the request have the same Id.",
    18
};

extern const TErrorClass EMPTY_BATCH_REQUEST = {
    "AWS.SimpleQueueService.EmptyBatchRequest",
    400,
    "The batch request doesn't contain any entries.",
    19
};

extern const TErrorClass INVALID_BATCH_ENTRY_ID = {
    "AWS.SimpleQueueService.InvalidBatchEntryId",
    400,
    "The Id of a batch entry in a batch request doesn't abide by the specification.",
    20
};

extern const TErrorClass TOO_MANY_ENTRIES_IN_BATCH_REQUEST = {
    "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
    400,
    "The batch request contains more entries than permissible.",
    21
};

extern const TErrorClass BATCH_REQUEST_TOO_LONG = {
    "AWS.SimpleQueueService.BatchRequestTooLong",
    400,
    "The length of all the messages put together is more than the limit.",
    22
};

extern const TErrorClass NON_EXISTENT_QUEUE = {
    "AWS.SimpleQueueService.NonExistentQueue",
    400,
    "The specified queue doesn't exist.",
    23
};

extern const TErrorClass OVER_LIMIT = {
    "OverLimit",
    403,
    "The specified action violates a limit. For example, ReceiveMessage returns this error if the maximum number of inflight messages is reached and AddPermission returns this error if the maximum number of permissions for the queue is reached.",
    24
};

extern const TErrorClass QUEUE_DELETED_RECENTLY = {
    "AWS.SimpleQueueService.QueueDeletedRecently",
    400,
    "You must wait some time after deleting a queue before you can create another queue with the same name.",
    25
};

extern const TErrorClass MESSAGE_NOT_INFLIGHT = {
    "AWS.SimpleQueueService.MessageNotInflight",
    400,
    "The specified message isn't in flight.",
    26
};

extern const TErrorClass RECEIPT_HANDLE_IS_INVALID = {
    "ReceiptHandleIsInvalid",
    400,
    "The specified receipt handle isn't valid.",
    27
};

extern const TErrorClass INVALID_ATTRIBUTE_NAME = {
    "InvalidAttributeName",
    400,
    "The specified attribute doesn't exist.",
    28
};

extern const TErrorClass INVALID_ATTRIBUTE_VALUE = {
    "InvalidAttributeValue",
    400,
    "The specified attribute value is invalid.",
    29
};

extern const TErrorClass LEADER_RESOLVING_ERROR = {
    "InternalFailure",
    500,
    "Queue leader resolving error.",
    30
};

extern const TErrorClass LEADER_SESSION_ERROR = {
    "InternalFailure",
    500,
    "Queue leader session error.",
    30
};

extern const TErrorClass TIMEOUT = {
    "InternalFailure",
    504,
    "Timeout.",
    31
};

} // namespace NErrors
} // namespace NKikimr::NSQS
