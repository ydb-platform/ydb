#pragma once

#include "codegen.h"
#include "defs.h"

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

namespace NBus {
////////////////////////////////////////////////////////////////
/// \brief Status of message communication

#define MESSAGE_STATUS_MAP(XX)                                                                 \
    XX(MESSAGE_OK, "OK")                                                                       \
    XX(MESSAGE_CONNECT_FAILED, "Connect failed")                                               \
    XX(MESSAGE_TIMEOUT, "Message timed out")                                                   \
    XX(MESSAGE_SERVICE_UNKNOWN, "Locator hasn't found address for key")                        \
    XX(MESSAGE_BUSY, "Too many messages in flight")                                            \
    XX(MESSAGE_UNKNOWN, "Request not found by id, usually it means that message is timed out") \
    XX(MESSAGE_DESERIALIZE_ERROR, "Deserialize by TBusProtocol failed")                        \
    XX(MESSAGE_HEADER_CORRUPTED, "Header corrupted")                                           \
    XX(MESSAGE_DECOMPRESS_ERROR, "Failed to decompress")                                       \
    XX(MESSAGE_MESSAGE_TOO_LARGE, "Message too large")                                         \
    XX(MESSAGE_REPLY_FAILED, "Unused by messagebus, used by other code")                       \
    XX(MESSAGE_DELIVERY_FAILED, "Message delivery failed because connection is closed")        \
    XX(MESSAGE_INVALID_VERSION, "Protocol error: invalid version")                             \
    XX(MESSAGE_SERVICE_TOOMANY, "Locator failed to resolve address")                           \
    XX(MESSAGE_SHUTDOWN, "Failure because of either session or connection shutdown")           \
    XX(MESSAGE_DONT_ASK, "Internal error code used by modules")

    enum EMessageStatus {
        MESSAGE_STATUS_MAP(ENUM_VALUE_GEN_NO_VALUE)
            MESSAGE_STATUS_COUNT
    };

    ENUM_TO_STRING(EMessageStatus, MESSAGE_STATUS_MAP)

    const char* MessageStatusDescription(EMessageStatus);

    static inline const char* GetMessageStatus(EMessageStatus status) {
        return ToCString(status);
    }

    // For lwtrace
    struct TMessageStatusField {
        typedef int TStoreType;
        typedef int TFuncParam;

        static void ToString(int value, TString* out) {
            *out = GetMessageStatus((NBus::EMessageStatus)value);
        }

        static int ToStoreType(int value) {
            return value;
        }
    };

} // ns
