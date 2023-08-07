#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

// Any change to this enum must be also propagated to FormatLevel.
DEFINE_ENUM(ELogLevel,
    (Minimum)
    (Trace)
    (Debug)
    (Info)
    (Warning)
    (Error)
    (Alert)
    (Fatal)
    (Maximum)
);

DEFINE_ENUM(ELogFamily,
    (PlainText)
    (Structured)
);

////////////////////////////////////////////////////////////////////////////////

struct TLoggingCategory;
struct TLoggingAnchor;
struct TLogEvent;
struct TLoggingContext;

class TLogger;
struct ILogManager;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
