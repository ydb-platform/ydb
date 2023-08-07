#pragma once

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/logging//logger.h>

namespace NYT::NLogging {

/////////////////////////////////////////////////////////////π///////////////////

constexpr int DateTimeBufferSize = 64;
constexpr int MessageBufferSize = 64_KB;

void FormatMilliseconds(TBaseFormatter* out, TInstant dateTime);
void FormatMicroseconds(TBaseFormatter* out, TInstant dateTime);
void FormatLevel(TBaseFormatter* out, ELogLevel level);
void FormatMessage(TBaseFormatter* out, TStringBuf message);

/////////////////////////////////////////////////////////////π///////////////////

class TCachingDateFormatter
{
public:
    void Format(TBaseFormatter* buffer, TInstant dateTime, bool printMicroseconds = false);

private:
    ui64 CachedSecond_ = 0;
    TRawFormatter<DateTimeBufferSize> Cached_;
};

////////////////////////////////////////////////////////////////////////////////

class TPlainTextEventFormatter
{
public:
    explicit TPlainTextEventFormatter(bool enableSourceLocation);

    void Format(TBaseFormatter* buffer, const TLogEvent& event);

private:
    const bool EnableSourceLocation_;

    TCachingDateFormatter CachingDateFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
