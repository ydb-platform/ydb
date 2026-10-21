#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

// Structured events carry the raw YSON map fragment as their opaque payload, with no
// framing. These helpers isolate that representation from callers.

//! Producer: wraps #message into a payload (zero-copy).
TStructuredLogEventPayload MakeStructuredPayloadFromYson(const NYson::TYsonString& message);

//! Consumer: views the payload as the YSON map fragment it carries. The result views
//! into #payload, which must outlive it.
NYson::TYsonStringBuf GetYsonFromStructuredPayload(const TStructuredLogEventPayload& payload);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
