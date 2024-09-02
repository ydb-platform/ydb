#pragma once

#include "config.h"

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/logging/plain_text_formatter/formatter.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct ILogFormatter
{
    virtual ~ILogFormatter() = default;

    virtual i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) = 0;
    virtual void WriteLogReopenSeparator(IOutputStream* outputStream) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TLogFormatterBase
    : public ILogFormatter
{
protected:
    TLogFormatterBase(bool enableSourceLocation);

    bool IsSourceLocationEnabled() const;

private:
    const bool EnableSourceLocation_;
};

////////////////////////////////////////////////////////////////////////////////

class TPlainTextLogFormatter
    : public TLogFormatterBase
{
public:
    explicit TPlainTextLogFormatter(bool enableSourceLocation = false);

    i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) override;
    void WriteLogReopenSeparator(IOutputStream* outputStream) override;

private:
    TRawFormatter<MessageBufferSize> Buffer_;
    TPlainTextEventFormatter EventFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

class TStructuredLogFormatter
    : public TLogFormatterBase
{
public:
    TStructuredLogFormatter(
        ELogFormat format,
        THashMap<TString, NYTree::INodePtr> commonFields,
        bool enableSourceLocation = false,
        bool enableSystemFields = true,
        bool enableHostField = false,
        NJson::TJsonFormatConfigPtr jsonFormat = nullptr);

    i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) override;
    void WriteLogReopenSeparator(IOutputStream* outputStream) override;

private:
    const ELogFormat Format_;
    const THashMap<TString, NYTree::INodePtr> CommonFields_;
    const bool EnableSystemFields_;
    const bool EnableHostField_;
    const NJson::TJsonFormatConfigPtr JsonFormat_;

    TCachingDateFormatter CachingDateFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
