#include "formatter.h"
#include "private.h"
#include "log.h"

#include <yt/yt/build/build.h>

#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/writer.h>

#include <util/stream/length.h>

namespace NYT::NLogging {

using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger(SystemLoggingCategoryName);

namespace {

TLogEvent GetStartLogEvent()
{
    TLogEvent event;
    event.Instant = GetCpuInstant();
    event.Category = Logger.GetCategory();
    event.Level = ELogLevel::Info;
    event.MessageRef = TSharedRef::FromString(Format("Logging started (Version: %v, BuildHost: %v, BuildTime: %v)",
        GetVersion(),
        GetBuildHost(),
        GetBuildTime()));
    event.MessageKind = ELogMessageKind::Unstructured;
    return event;
}

TLogEvent GetStartLogStructuredEvent()
{
    TLogEvent event;
    event.Instant = GetCpuInstant();
    event.Category = Logger.GetCategory();
    event.Level = ELogLevel::Info;
    event.MessageRef = BuildYsonStringFluently<NYson::EYsonType::MapFragment>()
        .Item("message").Value("Logging started")
        .Item("version").Value(GetVersion())
        .Item("build_host").Value(GetBuildHost())
        .Item("build_time").Value(GetBuildTime())
        .Finish()
        .ToSharedRef();
    event.MessageKind = ELogMessageKind::Structured;
    return event;
}

TLogEvent GetSkippedLogEvent(i64 count, TStringBuf skippedBy)
{
    TLogEvent event;
    event.Instant = GetCpuInstant();
    event.Category = Logger.GetCategory();
    event.Level = ELogLevel::Info;
    event.MessageRef = TSharedRef::FromString(Format("Skipped log records in last second (Count: %v, SkippedBy: %v)",
        count,
        skippedBy));
    event.MessageKind = ELogMessageKind::Unstructured;
    return event;
}

TLogEvent GetSkippedLogStructuredEvent(i64 count, TStringBuf skippedBy)
{
    TLogEvent event;
    event.Instant = GetCpuInstant();
    event.Category = Logger.GetCategory();
    event.Level = ELogLevel::Info;
    event.MessageRef = BuildYsonStringFluently<NYson::EYsonType::MapFragment>()
        .Item("message").Value("Events skipped")
        .Item("skipped_by").Value(skippedBy)
        .Item("events_skipped").Value(count)
        .Finish()
        .ToSharedRef();
    event.MessageKind = ELogMessageKind::Structured;
    return event;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TCachingDateFormatter::Format(TBaseFormatter* buffer, TInstant dateTime, bool printMicroseconds)
{
    auto currentSecond = dateTime.Seconds();
    if (CachedSecond_ != currentSecond) {
        Cached_.Reset();
        FormatDateTime(&Cached_, dateTime);
        CachedSecond_ = currentSecond;
    }

    buffer->AppendString(Cached_.GetBuffer());
    buffer->AppendChar(',');
    if (printMicroseconds) {
        FormatMicroseconds(buffer, dateTime);
    } else {
        FormatMilliseconds(buffer, dateTime);
    }
}

////////////////////////////////////////////////////////////////////////////////

TPlainTextLogFormatter::TPlainTextLogFormatter(
    bool enableSystemMessages,
    bool enableSourceLocation)
    : Buffer_(std::make_unique<TRawFormatter<MessageBufferSize>>())
    , CachingDateFormatter_(std::make_unique<TCachingDateFormatter>())
    , EnableSystemMessages_(enableSystemMessages && Logger)
    , EnableSourceLocation_(enableSourceLocation)
{ }

i64 TPlainTextLogFormatter::WriteFormatted(IOutputStream* outputStream, const TLogEvent& event) const
{
    if (!outputStream) {
        return 0;
    }

    auto* buffer = Buffer_.get();
    buffer->Reset();

    CachingDateFormatter_->Format(buffer, CpuInstantToInstant(event.Instant), true);

    buffer->AppendChar('\t');

    FormatLevel(buffer, event.Level);

    buffer->AppendChar('\t');

    buffer->AppendString(event.Category->Name);

    buffer->AppendChar('\t');

    FormatMessage(buffer, event.MessageRef.ToStringBuf());

    buffer->AppendChar('\t');

    if (event.ThreadName.Length > 0) {
        buffer->AppendString(TStringBuf(event.ThreadName.Buffer.data(), event.ThreadName.Length));
    } else if (event.ThreadId != NThreading::InvalidThreadId) {
        buffer->AppendNumber(event.ThreadId, 16);
    }

    buffer->AppendChar('\t');

    if (event.FiberId != NConcurrency::InvalidFiberId) {
        buffer->AppendNumber(event.FiberId, 16);
    }

    buffer->AppendChar('\t');

    if (event.TraceId != NTracing::InvalidTraceId) {
        buffer->AppendGuid(event.TraceId);
    }

    if (EnableSourceLocation_) {
        buffer->AppendChar('\t');
        if (event.SourceFile) {
            auto sourceFile = event.SourceFile;
            buffer->AppendString(sourceFile.RNextTok(LOCSLASH_C));
            buffer->AppendChar(':');
            buffer->AppendNumber(event.SourceLine);
        }
    }

    buffer->AppendChar('\n');

    outputStream->Write(buffer->GetData(), buffer->GetBytesWritten());

    return buffer->GetBytesWritten();
}

void TPlainTextLogFormatter::WriteLogReopenSeparator(IOutputStream* outputStream) const
{
    *outputStream << Endl;
}

void TPlainTextLogFormatter::WriteLogStartEvent(IOutputStream* outputStream) const
{
    if (EnableSystemMessages_) {
        WriteFormatted(outputStream, GetStartLogEvent());
    }
}

void TPlainTextLogFormatter::WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) const
{
    if (EnableSystemMessages_) {
        WriteFormatted(outputStream, GetSkippedLogEvent(count, skippedBy));
    }
}

////////////////////////////////////////////////////////////////////////////////

TStructuredLogFormatter::TStructuredLogFormatter(
    ELogFormat format,
    THashMap<TString, NYTree::INodePtr> commonFields,
    bool enableSystemMessages,
    NJson::TJsonFormatConfigPtr jsonFormat)
    : Format_(format)
    , CachingDateFormatter_(std::make_unique<TCachingDateFormatter>())
    , CommonFields_(std::move(commonFields))
    , EnableSystemMessages_(enableSystemMessages)
    , JsonFormat_(!jsonFormat && (Format_ == ELogFormat::Json)
        ? New<NJson::TJsonFormatConfig>()
        : std::move(jsonFormat))
{ }

i64 TStructuredLogFormatter::WriteFormatted(IOutputStream* stream, const TLogEvent& event) const
{
    if (!stream) {
        return 0;
    }

    auto countingStream = TCountingOutput(stream);
    std::unique_ptr<IFlushableYsonConsumer> consumer;

    switch (Format_) {
        case ELogFormat::Json:
            YT_VERIFY(JsonFormat_);
            consumer = NJson::CreateJsonConsumer(&countingStream, EYsonType::Node, JsonFormat_);
            break;
        case ELogFormat::Yson:
            consumer = std::make_unique<TYsonWriter>(&countingStream, EYsonFormat::Text);
            break;
        default:
            YT_ABORT();
    }

    TRawFormatter<DateTimeBufferSize> dateTimeBuffer;
    CachingDateFormatter_->Format(&dateTimeBuffer, CpuInstantToInstant(event.Instant));

    BuildYsonFluently(consumer.get())
        .BeginMap()
            .DoFor(CommonFields_, [] (auto fluent, auto item) {
                fluent.Item(item.first).Value(item.second);
            })
            .DoIf(event.MessageKind == ELogMessageKind::Structured, [&] (auto fluent) {
                fluent.Items(TYsonString(event.MessageRef, EYsonType::MapFragment));
            })
            .DoIf(event.MessageKind == ELogMessageKind::Unstructured, [&] (auto fluent) {
                fluent.Item("message").Value(event.MessageRef.ToStringBuf());
            })
            .Item("instant").Value(dateTimeBuffer.GetBuffer())
            .Item("level").Value(FormatEnum(event.Level))
            .Item("category").Value(event.Category->Name)
        .EndMap();
    consumer->Flush();

    if (Format_ == ELogFormat::Yson) {
        // In order to obtain proper list fragment, we must manually insert trailing semicolon in each line.
        countingStream.Write(';');
    }

    countingStream.Write('\n');

    return countingStream.Counter();
}

void TStructuredLogFormatter::WriteLogReopenSeparator(IOutputStream* /*outputStream*/) const
{ }

void TStructuredLogFormatter::WriteLogStartEvent(IOutputStream* outputStream) const
{
    if (EnableSystemMessages_) {
        WriteFormatted(outputStream, GetStartLogStructuredEvent());
    }
}

void TStructuredLogFormatter::WriteLogSkippedEvent(IOutputStream* outputStream, i64 count, TStringBuf skippedBy) const
{
    if (EnableSystemMessages_) {
        WriteFormatted(outputStream, GetSkippedLogStructuredEvent(count, skippedBy));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
