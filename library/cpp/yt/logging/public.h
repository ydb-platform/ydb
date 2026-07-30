#pragma once

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/strong_typedef.h>

#include <library/cpp/yt/memory/ref.h>

#include <variant>

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

//! Opaque payload of a plain-text log event: a message plus optional key/value tags,
//! framed by #TTaggedPayloadWriter. Produced by the YT_LOG_*/YT_TLOG_* macros.
YT_DEFINE_STRONG_TYPEDEF(TTaggedLogEventPayload, TSharedRef);

//! Opaque payload of a structured log event: a raw YSON map fragment.
//! Produced by #LogStructuredEvent.
YT_DEFINE_STRONG_TYPEDEF(TStructuredLogEventPayload, TSharedRef);

//! The payload carried by a #TLogEvent: exactly one of the encodings above. The active
//! alternative both identifies the event kind and determines how the payload is decoded.
using TLogEventPayload = std::variant<TTaggedLogEventPayload, TStructuredLogEventPayload>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
