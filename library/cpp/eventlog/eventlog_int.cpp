#include "eventlog_int.h"

#include <util/string/cast.h>

TMaybe<TEventLogFormat> ParseEventLogFormat(TStringBuf str) {
    EEventLogFormat format;
    if (TryFromString(str, format)) {
        return static_cast<TEventLogFormat>(format);
    } else {
        return {};
    }
}
