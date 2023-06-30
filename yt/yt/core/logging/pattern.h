#pragma once

#include "public.h"

#include <library/cpp/yt/string/raw_formatter.h>

#include <util/generic/size_literals.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

constexpr int DateTimeBufferSize = 64;
constexpr int MessageBufferSize = 64_KB;
constexpr int MessageBufferWatermarkSize = 256;

void FormatMessage(TBaseFormatter* out, TStringBuf message);
void FormatDateTime(TBaseFormatter* out, TInstant dateTime);
void FormatMilliseconds(TBaseFormatter* out, TInstant dateTime);
void FormatMicroseconds(TBaseFormatter* out, TInstant dateTime);
void FormatLevel(TBaseFormatter* out, ELogLevel level);

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging::NYT
