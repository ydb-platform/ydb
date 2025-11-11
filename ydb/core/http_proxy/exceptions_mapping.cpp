#include "exceptions_mapping.h"

#include <unordered_map>


namespace NKikimr::NHttpProxy {

enum class EMethodId : ui8 {
    CREATE_STREAM =                 100,
    DELETE_STREAM =                 101,
    UPDATE_SHARD_COUNT =            102,
    UPDATE_STREAM_MODE =            103,
    UPDATE_STREAM =                 104,
    SET_WRITE_QUOTA =               105,
    SET_STREAM_RETENTION_PERIOD =   106,
    DESCRIBE_STREAM =               107,
    LIST_STREAMS =                  108,
    LIST_STREAM_CONSUMERS =         109,
    REGISTER_STREAM_CONSUMER =      110,
    DEREGISTER_STREAM_CONSUMER =    111,
    GET_SHARD_ITERATOR =            112,
    GET_RECORDS =                   113,
    LIST_SHARDS =                   114,
    DESCRIBE_STREAM_SUMMARY =       115
};

static std::unordered_map<TString, EMethodId> getEMethodId = {
    {"CreateStream", EMethodId::CREATE_STREAM},
    {"DeleteStream", EMethodId::DELETE_STREAM},
    {"UpdateShardCount", EMethodId::UPDATE_SHARD_COUNT},
    {"UpdateStreamMode", EMethodId::UPDATE_STREAM_MODE},
    {"UpdateStream", EMethodId::UPDATE_STREAM},
    {"SetWriteQuota", EMethodId::SET_WRITE_QUOTA},
    {"SetStreamRetentionPeriod", EMethodId::SET_STREAM_RETENTION_PERIOD},
    {"DescribeStream", EMethodId::DESCRIBE_STREAM},
    {"ListStreams", EMethodId::LIST_STREAMS},
    {"ListStreamConsumers", EMethodId::LIST_STREAM_CONSUMERS},
    {"RegisterStreamConsumer", EMethodId::REGISTER_STREAM_CONSUMER},
    {"DeregisterStreamConsumer", EMethodId::DEREGISTER_STREAM_CONSUMER},
    {"GetShardIterator", EMethodId::GET_SHARD_ITERATOR},
    {"GetRecords", EMethodId::GET_RECORDS},
    {"ListShards", EMethodId::LIST_SHARDS},
    {"DescribeStreamSummary", EMethodId::DESCRIBE_STREAM_SUMMARY}
};


TException AlreadyExistsExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    Y_UNUSED(method);
    Y_UNUSED(issueCode);
    return TException("ResourceInUseException", HTTP_BAD_REQUEST);
}

TException BadRequestExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    EMethodId Method = getEMethodId[method];

    switch (issueCode) {
        case NYds::EErrorCodes::ACCESS_DENIED:
            return TException("AccessDeniedException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::INVALID_ARGUMENT:
            return TException("InvalidArgumentException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::VALIDATION_ERROR:
            return TException("ValidationException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::MISSING_PARAMETER:
            return TException("MissingParameter", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::IN_USE:
            return TException("ResourceInUseException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::GENERIC_ERROR:
            switch (Method) {
                case EMethodId::LIST_STREAMS:
                    return TException("ValidationException", HTTP_BAD_REQUEST);
                case EMethodId::DESCRIBE_STREAM:
                    return TException("ResourceNotFoundException", HTTP_BAD_REQUEST);
                case EMethodId::CREATE_STREAM:
                    return TException("LimitExceededException", HTTP_BAD_REQUEST);
                default:
                    return TException("ValidationException", HTTP_BAD_REQUEST);
            }
        case NYds::EErrorCodes::EXPIRED_TOKEN:
            return TException("ExpiredNextTokenException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::EXPIRED_ITERATOR:
            return TException("ExpiredIteratorException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::NOT_FOUND:
            return TException("ResourceNotFoundException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::BAD_REQUEST:
            return TException("ValidationException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::INCOMPLETE_SIGNATURE:
            return TException("IncompleteSignature", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::INVALID_PARAMETER_COMBINATION:
            return TException("InvalidParameterCombination", HTTP_BAD_REQUEST);
        default:
            return TException("UnknownError", HTTP_BAD_REQUEST);
    }
}

TException GenericErrorExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    Y_UNUSED(issueCode);
    EMethodId Method = getEMethodId[method];
    switch (Method) {
        case EMethodId::CREATE_STREAM:
            return TException("LimitExceededException", HTTP_BAD_REQUEST);
        default:
            return TException("GenericError", HTTP_BAD_REQUEST);
    }
}

TException InternalErrorExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    Y_UNUSED(method);
    Y_UNUSED(issueCode);
    return TException("InternalError", HTTP_INTERNAL_SERVER_ERROR);
}

TException NotFoundExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    Y_UNUSED(method);
    switch (issueCode) {
        case NYds::EErrorCodes::INVALID_ARGUMENT:
            return TException("InvalidArgumentException", HTTP_BAD_REQUEST);
        default:
            return TException("ResourceNotFoundException", HTTP_BAD_REQUEST);
    }
}

TException OverloadedExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    Y_UNUSED(method);
    switch (issueCode) {
        case NYds::EErrorCodes::ERROR:
            return TException("ResourceInUseException", HTTP_BAD_REQUEST);
        default:
            return TException("ThrottlingException", HTTP_BAD_REQUEST);
    }
}

TException PreconditionFailedExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    Y_UNUSED(method);
    Y_UNUSED(issueCode);
    return TException("ValidationException", HTTP_BAD_REQUEST);
}

TException SchemeErrorExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    Y_UNUSED(method);
    switch (issueCode) {
        case NYds::EErrorCodes::INVALID_ARGUMENT:
            return TException("InvalidArgumentException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::VALIDATION_ERROR:
            return TException("ValidationException", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::NOT_FOUND:
        case NYds::EErrorCodes::ACCESS_DENIED:
            return TException("ResourceNotFoundException", HTTP_BAD_REQUEST);
        default:
            return TException("InvalidArgumentException", HTTP_BAD_REQUEST);
    }
}

TException UnauthorizedExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    Y_UNUSED(method);
    switch (issueCode) {
        case NYds::EErrorCodes::INCOMPLETE_SIGNATURE:
            return TException("IncompleteSignature", HTTP_BAD_REQUEST);
        case NYds::EErrorCodes::MISSING_AUTHENTICATION_TOKEN:
            return TException("MissingAuthenticationToken", HTTP_BAD_REQUEST);
        default:
            return TException("AccessDeniedException", HTTP_BAD_REQUEST);
    }
}

TException UnsupportedExceptions(const TString& method, NYds::EErrorCodes issueCode) {
    Y_UNUSED(method);
    switch (issueCode) {
        case NYds::EErrorCodes::MISSING_ACTION:
            return TException("MissingAction", HTTP_BAD_REQUEST);
        default:
            return TException("InvalidAction", HTTP_BAD_REQUEST);
    }
}

}
