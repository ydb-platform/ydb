#pragma once

#include "fwd.h"

#include <cstddef>

namespace NYdb::inline V3 {

constexpr size_t TRANSPORT_STATUSES_FIRST = 401000;
constexpr size_t TRANSPORT_STATUSES_LAST  = 401999;
constexpr size_t INTERNAL_CLIENT_FIRST = 402000;

enum class EStatus : size_t {
    // Server statuses
    STATUS_UNDEFINED =    0,
    SUCCESS =             400000,
    BAD_REQUEST =         400010,
    UNAUTHORIZED =        400020,
    INTERNAL_ERROR =      400030,
    ABORTED =             400040,
    UNAVAILABLE =         400050,
    OVERLOADED =          400060,
    SCHEME_ERROR =        400070,
    GENERIC_ERROR =       400080,
    TIMEOUT =             400090,
    BAD_SESSION =         400100,
    PRECONDITION_FAILED = 400120,
    ALREADY_EXISTS      = 400130,
    NOT_FOUND =           400140,
    SESSION_EXPIRED =     400150,
    CANCELLED =           400160,
    UNDETERMINED =        400170,
    UNSUPPORTED =         400180,
    SESSION_BUSY =        400190,
    EXTERNAL_ERROR =      400200,

    // Client statuses
    // Cannot connect or unrecoverable network error. (map from gRPC UNAVAILABLE)
    TRANSPORT_UNAVAILABLE =      TRANSPORT_STATUSES_FIRST + 10,
    // No more resources to accept RPC call
    CLIENT_RESOURCE_EXHAUSTED =  TRANSPORT_STATUSES_FIRST + 20,
    // Network layer does not receive response in given time
    CLIENT_DEADLINE_EXCEEDED =   TRANSPORT_STATUSES_FIRST + 30,
    // Unknown client error
    CLIENT_INTERNAL_ERROR =      TRANSPORT_STATUSES_FIRST + 50,
    CLIENT_CANCELLED =           TRANSPORT_STATUSES_FIRST + 60,
    CLIENT_UNAUTHENTICATED =     TRANSPORT_STATUSES_FIRST + 70,
    // Unknown gRPC call
    CLIENT_CALL_UNIMPLEMENTED =  TRANSPORT_STATUSES_FIRST + 80,
    // Attempt to read out of stream
    CLIENT_OUT_OF_RANGE =        TRANSPORT_STATUSES_FIRST + 90,
    CLIENT_DISCOVERY_FAILED =    INTERNAL_CLIENT_FIRST    + 10, // Not used
    CLIENT_LIMITS_REACHED =      INTERNAL_CLIENT_FIRST    + 20
};

} // namespace NYdb
