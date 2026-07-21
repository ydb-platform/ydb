#include "formatter.h"

#include <library/cpp/yt/logging/structured_payload.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/misc/port.h>

#include <variant>

#ifdef YT_USE_SSE42
    #include <emmintrin.h>
    #include <pmmintrin.h>
#endif

namespace NYT::NLogging {

constexpr int MessageBufferWatermarkSize = 256;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Ultra-fast specialized versions of AppendNumber.
void AppendDigit(TBaseFormatter* out, ui32 value)
{
    out->AppendChar('0' + value);
}

void AppendNumber2(TBaseFormatter* out, ui32 value)
{
    AppendDigit(out, value / 10);
    AppendDigit(out, value % 10);
}

void AppendNumber3(TBaseFormatter* out, ui32 value)
{
    AppendDigit(out, value / 100);
    AppendDigit(out, (value / 10) % 10);
    AppendDigit(out, value % 10);
}

void AppendNumber4(TBaseFormatter* out, ui32 value)
{
    AppendDigit(out, value / 1000);
    AppendDigit(out, (value / 100) % 10);
    AppendDigit(out, (value / 10) % 10);
    AppendDigit(out, value % 10);
}

void AppendNumber6(TBaseFormatter* out, ui32 value)
{
    AppendDigit(out, value / 100000);
    AppendDigit(out, (value / 10000) % 10);
    AppendDigit(out, (value / 1000) % 10);
    AppendDigit(out, (value / 100) % 10);
    AppendDigit(out, (value / 10) % 10);
    AppendDigit(out, value % 10);
}

} // namespace

void FormatDateTime(TBaseFormatter* out, TInstant dateTime)
{
    tm localTime;
    dateTime.LocalTime(&localTime);
    AppendNumber4(out, localTime.tm_year + 1900);
    out->AppendChar('-');
    AppendNumber2(out, localTime.tm_mon + 1);
    out->AppendChar('-');
    AppendNumber2(out, localTime.tm_mday);
    out->AppendChar(' ');
    AppendNumber2(out, localTime.tm_hour);
    out->AppendChar(':');
    AppendNumber2(out, localTime.tm_min);
    out->AppendChar(':');
    AppendNumber2(out, localTime.tm_sec);
}

void FormatMilliseconds(TBaseFormatter* out, TInstant dateTime)
{
    AppendNumber3(out, dateTime.MilliSecondsOfSecond());
}

void FormatMicroseconds(TBaseFormatter* out, TInstant dateTime)
{
    AppendNumber6(out, dateTime.MicroSecondsOfSecond());
}

void FormatLevel(TBaseFormatter* out, ELogLevel level)
{
    static char chars[] = "?TDIWEAF?";
    out->AppendChar(chars[static_cast<int>(level)]);
}

void FormatMessage(TBaseFormatter* out, TStringBuf message)
{
    auto current = message.begin();

#ifdef YT_USE_SSE42
    auto vectorLow = _mm_set1_epi8(PrintableASCIILow);
    auto vectorHigh = _mm_set1_epi8(PrintableASCIIHigh);
#endif

    auto appendChar = [&] {
        char ch = *current;
        if (ch == '\n') {
            out->AppendString("\\n");
        } else if (ch == '\t') {
            out->AppendString("\\t");
        } else if (ch < PrintableASCIILow || ch > PrintableASCIIHigh) {
            unsigned char unsignedCh = ch;
            out->AppendString("\\x");
            out->AppendChar(IntToHexLowercase[unsignedCh >> 4]);
            out->AppendChar(IntToHexLowercase[unsignedCh & 15]);
        } else {
            out->AppendChar(ch);
        }
        ++current;
    };

    while (current < message.end()) {
        if (out->GetBytesRemaining() < MessageBufferWatermarkSize) {
            out->AppendString(TStringBuf("...<message truncated>"));
            break;
        }
#ifdef YT_USE_SSE42
        // Use SSE for optimization.
        if (current + 16 > message.end()) {
            appendChar();
        } else {
            const void* inPtr = &(*current);
            void* outPtr = out->GetCursor();
            auto value = _mm_lddqu_si128(static_cast<const __m128i*>(inPtr));
            if (_mm_movemask_epi8(_mm_cmplt_epi8(value, vectorLow)) ||
                _mm_movemask_epi8(_mm_cmpgt_epi8(value, vectorHigh))) {
                for (int index = 0; index < 16; ++index) {
                    appendChar();
                }
            } else {
                _mm_storeu_si128(static_cast<__m128i*>(outPtr), value);
                out->Advance(16);
                current += 16;
            }
        }
#else
        // Unoptimized version.
        appendChar();
#endif
    }
}

// Formats |Message (Key: Value, ...)|, with well-known tags (e.g. an error) appended
// after the |(...)| group. Well-known tags are always written last, so a single pass
// suffices. Every piece -- message, tag keys/values, and the newline separating a
// well-known tag -- goes through FormatMessage and is escaped, so the rendered payload
// stays on a single physical line (a newline is emitted as the literal "\n").
void FormatPayload(TBaseFormatter* out, const TTaggedLogEventPayload& payload)
{
    TTaggedPayloadReader reader(payload);
    FormatMessage(out, reader.ReadMessage());
    bool parenOpen = false;
    while (auto tag = reader.TryReadTag()) {
        if (tag->IsWellKnown) {
            if (parenOpen) {
                out->AppendChar(')');
                parenOpen = false;
            }
            FormatMessage(out, "\n"_sb);
            FormatMessage(out, tag->Value);
        } else {
            out->AppendString(parenOpen ? ", "_sb : " ("_sb);
            parenOpen = true;
            FormatMessage(out, tag->Key);
            out->AppendString(": "_sb);
            FormatMessage(out, tag->Value);
        }
    }
    if (parenOpen) {
        out->AppendChar(')');
    }
}

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

TPlainTextEventFormatter::TPlainTextEventFormatter(bool enableSourceLocation)
    : EnableSourceLocation_(enableSourceLocation)
{ }

void TPlainTextEventFormatter::Format(TBaseFormatter* buffer, const TLogEvent& event)
{
    CachingDateFormatter_.Format(buffer, CpuInstantToInstant(event.Instant), true);

    buffer->AppendChar('\t');

    FormatLevel(buffer, event.Level);

    buffer->AppendChar('\t');

    buffer->AppendString(event.Category->Name);

    buffer->AppendChar('\t');

    if (const auto* tagged = std::get_if<TTaggedLogEventPayload>(&event.Payload)) {
        FormatPayload(buffer, *tagged);
    } else {
        // A structured event routed to a plain-text writer: emit its raw YSON fragment
        // (escaped, so the record stays a single physical line).
        FormatMessage(buffer, GetYsonFromStructuredPayload(std::get<TStructuredLogEventPayload>(event.Payload)).AsStringBuf());
    }

    buffer->AppendChar('\t');

    if (event.ThreadName.Length > 0) {
        buffer->AppendString(TStringBuf(event.ThreadName.Buffer.data(), event.ThreadName.Length));
    } else if (event.ThreadId != TThreadId()) {
        buffer->AppendNumber(event.ThreadId, 16);
    }

    buffer->AppendChar('\t');

    if (event.FiberId != TFiberId()) {
        buffer->AppendNumber(event.FiberId, 16);
    }

    buffer->AppendChar('\t');

    if (event.TraceId != TTraceId()) {
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
