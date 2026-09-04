#include "formatter.h"

#include "private.h"

#include <library/cpp/yt/logging/tagged_payload.h>
#include <library/cpp/yt/logging/structured_payload.h>

#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/net/local_address.h>

#include <util/stream/length.h>

namespace NYT::NLogging {

using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TPlainTextLogFormatter::TPlainTextLogFormatter(TPlainTextLogFormatterOptions options)
    : TLogFormatterBase(TLogFormatterBaseOptions{.EnableSourceLocation = options.EnableSourceLocation})
    , EventFormatter_(options.EnableSourceLocation)
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

TLogFormatterBase::TLogFormatterBase(TLogFormatterBaseOptions options)
    : Options_(std::move(options))
{ }

bool TLogFormatterBase::IsSourceLocationEnabled() const
{
    return Options_.EnableSourceLocation;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TStructuredLogFormatterOptions NormalizeStructuredOptions(TStructuredLogFormatterOptions options)
{
    if (!options.JsonFormat && options.Format == ELogFormat::Json) {
        options.JsonFormat = New<NJson::TJsonFormatConfig>();
    }
    return options;
}

} // namespace

TStructuredLogFormatter::TStructuredLogFormatter(TStructuredLogFormatterOptions options)
    : TLogFormatterBase(TLogFormatterBaseOptions{.EnableSourceLocation = options.EnableSourceLocation})
    , Options_(NormalizeStructuredOptions(std::move(options)))
{ }

i64 TStructuredLogFormatter::WriteFormatted(IOutputStream* stream, const TLogEvent& event)
{
    if (!stream) {
        return 0;
    }

    auto countingStream = TCountingOutput(stream);
    std::unique_ptr<IFlushableYsonConsumer> consumer;

    switch (Options_.Format) {
        case ELogFormat::Json:
            YT_VERIFY(Options_.JsonFormat);
            consumer = NJson::CreateJsonConsumer(&countingStream, EYsonType::Node, Options_.JsonFormat);
            break;
        case ELogFormat::Yson:
            consumer = std::make_unique<TYsonWriter>(
                &countingStream,
                Options_.YsonFormat,
                EYsonType::Node,
                /*enableRaw*/ Options_.YsonFormat == EYsonFormat::Binary);
            break;
        default:
            YT_ABORT();
    }

    TRawFormatter<DateTimeBufferSize> dateTimeBuffer;
    CachingDateFormatter_.Format(&dateTimeBuffer, CpuInstantToInstant(event.Instant));

    BuildYsonFluently(consumer.get())
        .BeginMap()
            .DoFor(Options_.CommonFields, [] (auto fluent, auto item) {
                fluent.Item(item.first).Value(item.second);
            })
            .DoIf(std::holds_alternative<TStructuredLogEventPayload>(event.Payload), [&] (auto fluent) {
                fluent.Items(TYsonString(GetYsonFromStructuredPayload(std::get<TStructuredLogEventPayload>(event.Payload))));
            })
            .DoIf(std::holds_alternative<TTaggedLogEventPayload>(event.Payload), [&] (auto fluent) {
                const auto& taggedPayload = std::get<TTaggedLogEventPayload>(event.Payload);
                if (Options_.EnableNativeTags) {
                    TTaggedPayloadReader reader(taggedPayload);
                    auto message = reader.ReadMessage();
                    auto firstTag = reader.TryReadTag();
                    fluent
                        .Item("message").Value(message)
                        .DoIf(firstTag.has_value(), [&] (auto fluent) {
                            fluent.Item("tags").DoMap([&] (auto fluent) {
                                for (auto tag = firstTag; tag; tag = reader.TryReadTag()) {
                                    fluent.Item(tag->Key).Value(tag->Value);
                                }
                            });
                        });
                } else {
                    // Fold the tags into the message: |Message (Key: Value, ...)|.
                    fluent.Item("message").Value(FormatTaggedPayload(taggedPayload));
                }
            })
            .DoIf(Options_.EnableSystemFields, [&] (auto fluent) {
                fluent
                    .Item("instant").Value(dateTimeBuffer.GetBuffer())
                    .Item("level").Value(FormatEnum(event.Level))
                    .Item("category").Value(event.Category->Name);
            })
            // TODO(achulkov2): The presence of different system fields should be controller by a flag enum instead of multiple boolean options.
            .DoIf(Options_.EnableHostField, [&] (auto fluent) {
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

    if (Options_.Format == ELogFormat::Yson) {
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
