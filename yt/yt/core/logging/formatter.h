#pragma once

#include "config.h"
#include "pattern.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TCachingDateFormatter
{
public:
    void Format(TBaseFormatter* buffer, TInstant dateTime, bool printMicroseconds = false);

private:
    ui64 CachedSecond_ = 0;
    TRawFormatter<DateTimeBufferSize> Cached_;
};

////////////////////////////////////////////////////////////////////////////////

struct ILogFormatter
{
    virtual ~ILogFormatter() = default;

    virtual i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const = 0;
    virtual void WriteLogReopenSeparator(IOutputStream* outputStream) const = 0;
    virtual void WriteLogStartEvent(IOutputStream* outputStream) const = 0;
    virtual void WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TPlainTextLogFormatter
    : public ILogFormatter
{
public:
    explicit TPlainTextLogFormatter(
        bool enableControlMessages = true,
        bool enableSourceLocation = false);

    i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const override;
    void WriteLogReopenSeparator(IOutputStream* outputStream) const override;
    void WriteLogStartEvent(IOutputStream* outputStream) const override;
    void WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) const override;

private:
    const std::unique_ptr<TRawFormatter<MessageBufferSize>> Buffer_;
    const std::unique_ptr<TCachingDateFormatter> CachingDateFormatter_;
    const bool EnableSystemMessages_;
    const bool EnableSourceLocation_;
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
        NJson::TJsonFormatConfigPtr jsonFormat = nullptr);

    i64 WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const override;
    void WriteLogReopenSeparator(IOutputStream* outputStream) const override;
    void WriteLogStartEvent(IOutputStream* outputStream) const override;
    void WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) const override;

private:
    const ELogFormat Format_;
    const std::unique_ptr<TCachingDateFormatter> CachingDateFormatter_;
    const THashMap<TString, NYTree::INodePtr> CommonFields_;
    const bool EnableSystemMessages_;
    const NJson::TJsonFormatConfigPtr JsonFormat_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
