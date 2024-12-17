#include "common.h"

#include <util/charset/unidata.h>

namespace NYdb::NPersQueue {

ERetryErrorClass GetRetryErrorClass(EStatus status) {
    switch (status) {
    case EStatus::SUCCESS:          // NoRetry?
    case EStatus::INTERNAL_ERROR:   // NoRetry?
    case EStatus::ABORTED:
    case EStatus::UNAVAILABLE:
    case EStatus::GENERIC_ERROR:    // NoRetry?
    case EStatus::BAD_SESSION:      // NoRetry?
    case EStatus::SESSION_EXPIRED:
    case EStatus::CANCELLED:
    case EStatus::UNDETERMINED:
    case EStatus::SESSION_BUSY:
    case EStatus::CLIENT_INTERNAL_ERROR:
    case EStatus::CLIENT_CANCELLED:
    case EStatus::CLIENT_OUT_OF_RANGE:
        return ERetryErrorClass::ShortRetry;

    case EStatus::OVERLOADED:
    case EStatus::TIMEOUT:
    case EStatus::TRANSPORT_UNAVAILABLE:
    case EStatus::CLIENT_RESOURCE_EXHAUSTED:
    case EStatus::CLIENT_DEADLINE_EXCEEDED:
    case EStatus::CLIENT_LIMITS_REACHED:
    case EStatus::CLIENT_DISCOVERY_FAILED:
        return ERetryErrorClass::LongRetry;

    case EStatus::SCHEME_ERROR:
    case EStatus::STATUS_UNDEFINED:
    case EStatus::BAD_REQUEST:
    case EStatus::UNAUTHORIZED:
    case EStatus::PRECONDITION_FAILED:
    case EStatus::UNSUPPORTED:
    case EStatus::ALREADY_EXISTS:
    case EStatus::NOT_FOUND:
    case EStatus::EXTERNAL_ERROR:
    case EStatus::CLIENT_UNAUTHENTICATED:
    case EStatus::CLIENT_CALL_UNIMPLEMENTED:
        return ERetryErrorClass::NoRetry;
    }
}

ERetryErrorClass GetRetryErrorClassV2(EStatus status) {
    switch (status) {
        case EStatus::SCHEME_ERROR:
            return ERetryErrorClass::NoRetry;
        default:
            return GetRetryErrorClass(status);

    }
}

}  // namespace NYdb::NPersQueue
