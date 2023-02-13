#pragma once

#include <util/stream/output.h>
#include <util/system/types.h>


namespace NYds {

enum class EErrorCodes : size_t {
    // Server statuses
    OK =                            0,  // compatible with PersQueue::ErrorCode

    BAD_REQUEST =                   500003,  // compatible with PersQueue::ErrorCode
    ERROR =                         500100,  // compatible with PersQueue::ErrorCode
    ACCESS_DENIED =                 500018,  // compatible with PersQueue::ErrorCode

    GENERIC_ERROR =                 500030,
    INVALID_ARGUMENT =              500040,
    MISSING_PARAMETER =             500050,
    NOT_FOUND =                     500060,
    IN_USE =                        500070,

    VALIDATION_ERROR =              500080,
    MISSING_ACTION =                500090,

    INVALID_PARAMETER_COMBINATION = 500110,

    EXPIRED_ITERATOR =              500120,
    EXPIRED_TOKEN =                 500130,

    INCOMPLETE_SIGNATURE =          500140,
    MISSING_AUTHENTICATION_TOKEN =  500150,
};

}
