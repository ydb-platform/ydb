#pragma once

#include "config.h"

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/logging/plain_text_formatter/formatter.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct ILogFormatter
{
    virtual ~ILogFormatter() = default;

    virtual i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) = 0;
    virtual void WriteLogReopenSeparator(IOutputStream* outputStream) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TLogFormatterBaseOptions
{
    bool EnableSourceLocation = false;
};

class TLogFormatterBase
    : public ILogFormatter
{
protected:
    explicit TLogFormatterBase(TLogFormatterBaseOptions options);

    bool IsSourceLocationEnabled() const;

private:
    const TLogFormatterBaseOptions Options_;
};

////////////////////////////////////////////////////////////////////////////////

struct TPlainTextLogFormatterOptions
{
    bool EnableSourceLocation = false;
};

class TPlainTextLogFormatter
    : public TLogFormatterBase
{
public:
    explicit TPlainTextLogFormatter(TPlainTextLogFormatterOptions options = {});

    i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) override;
    void WriteLogReopenSeparator(IOutputStream* outputStream) override;

private:
    TRawFormatter<MessageBufferSize> Buffer_;
    TPlainTextEventFormatter EventFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

struct TStructuredLogFormatterOptions
{
    ELogFormat Format = ELogFormat::Json;
    //! Fields injected into every event as |key -> raw YSON value|.
    THashMap<std::string, NYson::TYsonString> CommonFields;
    bool EnableSourceLocation = false;
    bool EnableSystemFields = true;
    bool EnableHostField = false;
    bool EnableNativeTags = false;
    NJson::TJsonFormatConfigPtr JsonFormat;
    NYson::EYsonFormat YsonFormat = NYson::EYsonFormat::Text;
};

class TStructuredLogFormatter
    : public TLogFormatterBase
{
public:
    explicit TStructuredLogFormatter(TStructuredLogFormatterOptions options);

    i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) override;
    void WriteLogReopenSeparator(IOutputStream* outputStream) override;

private:
    const TStructuredLogFormatterOptions Options_;

    TCachingDateFormatter CachingDateFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
