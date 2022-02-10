#pragma once

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NBus {
    namespace NPrivate {
#define MESSAGE_LOCAL_FLAGS_MAP(XX)          \
    XX(MESSAGE_REPLY_INTERNAL, 0x0001)       \
    XX(MESSAGE_IN_WORK, 0x0002)              \
    XX(MESSAGE_IN_FLIGHT_ON_CLIENT, 0x0004)  \
    XX(MESSAGE_REPLY_IS_BEGING_SENT, 0x0008) \
    XX(MESSAGE_ONE_WAY_INTERNAL, 0x0010)     \
    /**/

        enum EMessageLocalFlags {
            MESSAGE_LOCAL_FLAGS_MAP(ENUM_VALUE_GEN)
        };

        ENUM_TO_STRING(EMessageLocalFlags, MESSAGE_LOCAL_FLAGS_MAP)

        TString LocalFlagSetToString(ui32);
    }
}
