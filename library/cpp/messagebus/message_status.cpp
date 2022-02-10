#include "message_status.h"

using namespace NBus;

const char* NBus::MessageStatusDescription(EMessageStatus messageStatus) {
#define MESSAGE_STATUS_DESCRIPTION_GEN(name, description, ...) \
    if (messageStatus == name)                                 \
        return description;

    MESSAGE_STATUS_MAP(MESSAGE_STATUS_DESCRIPTION_GEN)

    return "Unknown";
}
