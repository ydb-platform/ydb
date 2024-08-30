#include "formatter.h"

#include "private.h"

#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/net/local_address.h>

#include <util/stream/length.h>

namespace NYT::NLogging {

using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TPlainTextLogFormatter::TPlainTextLogFormatter(bool enableSourceLocation)
    : TLogFormatterBase(enableSourceLocation)
    , EventFormatter_(enableSourceLocation)
{ }

i64 TPlainTextLogFormatter::WriteFormatted(IOutputStream* outputStream, const TLogEvent& event)
{
    if (!outputStream) {
        return 0;
    }

    Buffer_.Reset();

    EventFormatter_.Format(&Buffer_, event);

    outputStream->Write(Buffer_.GetData(), Buffer_.GetBytesWritten());

    return Buffer_.GetBytesWritten();
}

void TPlainTextLogFormatter::WriteLogReopenSeparator(IOutputStream* outputStream)
{
    *outputStream << Endl;
}

////////////////////////////////////////////////////////////////////////////////

TLogFormatterBase::TLogFormatterBase(bool enableSourceLocation)
    : EnableSourceLocation_(enableSourceLocation)
{ }

bool TLogFormatterBase::IsSourceLocationEnabled() const
{
    return EnableSourceLocation_;
}

////////////////////////////////////////////////////////////////////////////////

TStructuredLogFormatter::TStructuredLogFormatter(
    ELogFormat format,
    THashMap<TString, INodePtr> commonFields,
    bool enableSourceLocation,
    bool enableSystemFields,
    bool enableHostField,
    NJson::TJsonFormatConfigPtr jsonFormat)
    : TLogFormatterBase(enableSourceLocation)
    , Format_(format)
    , CommonFields_(std::move(commonFields))
    , EnableSystemFields_(enableSystemFields)
    , EnableHostField_(enableHostField)
    , JsonFormat_(!jsonFormat && (Format_ == ELogFormat::Json)
        ? New<NJson::TJsonFormatConfig>()
        : std::move(jsonFormat))
{ }

i64 TStructuredLogFormatter::WriteFormatted(IOutputStream* stream, const TLogEvent& event)
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
    CachingDateFormatter_.Format(&dateTimeBuffer, CpuInstantToInstant(event.Instant));

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
            .DoIf(EnableSystemFields_, [&] (auto fluent) {
                fluent
                    .Item("instant").Value(dateTimeBuffer.GetBuffer())
                    .Item("level").Value(FormatEnum(event.Level))
                    .Item("category").Value(event.Category->Name);
            })
            // TODO(achulkov2): The presence of different system fields should be controller by a flag enum instead of multiple boolean options.
            .DoIf(EnableHostField_, [&] (auto fluent) {
                fluent.Item("host").Value(NNet::GetLocalHostName());
            })
            .DoIf(event.Family == ELogFamily::PlainText, [&] (auto fluent) {
                if (event.FiberId != TFiberId()) {
                    fluent.Item("fiber_id").Value(Format("%x", event.FiberId));
                }
                if (event.TraceId != TTraceId()) {
                    fluent.Item("trace_id").Value(event.TraceId);
                }
                if (IsSourceLocationEnabled() && event.SourceFile) {
                    auto sourceFile = event.SourceFile;
                    fluent.Item("source_file").Value(Format("%v:%v", sourceFile.RNextTok(LOCSLASH_C), event.SourceLine));
                }
            })
        .EndMap();
    consumer->Flush();

    if (Format_ == ELogFormat::Yson) {
        // In order to obtain proper list fragment, we must manually insert trailing semicolon in each line.
        countingStream.Write(';');
    }

    countingStream.Write('\n');

    return countingStream.Counter();
}

void TStructuredLogFormatter::WriteLogReopenSeparator(IOutputStream* /*outputStream*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
