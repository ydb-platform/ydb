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
    virtual void WriteLogStartEvent(IOutputStream* outputStream) = 0;
    virtual void WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TPlainTextLogFormatter
    : public ILogFormatter
{
public:
    explicit TPlainTextLogFormatter(
        bool enableControlMessages = true,
        bool enableSourceLocation = false);

    i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) override;
    void WriteLogReopenSeparator(IOutputStream* outputStream) override;
    void WriteLogStartEvent(IOutputStream* outputStream) override;
    void WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) override;

private:
    const bool EnableSystemMessages_;

    TRawFormatter<MessageBufferSize> Buffer_;
    TPlainTextEventFormatter EventFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

class TStructuredLogFormatter
    : public ILogFormatter
{
public:
    TStructuredLogFormatter(
        ELogFormat format,
        THashMap<TString, NYTree::INodePtr> commonFields,
        bool enableControlMessages = true,
        bool enableSourceLocation = false,
        NJson::TJsonFormatConfigPtr jsonFormat = nullptr);

    i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) override;
    void WriteLogReopenSeparator(IOutputStream* outputStream) override;
    void WriteLogStartEvent(IOutputStream* outputStream) override;
    void WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) override;

private:
    const ELogFormat Format_;
    const THashMap<TString, NYTree::INodePtr> CommonFields_;
    const bool EnableSystemMessages_;
    const bool EnableSourceLocation_;
    const NJson::TJsonFormatConfigPtr JsonFormat_;

    TCachingDateFormatter CachingDateFormatter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
