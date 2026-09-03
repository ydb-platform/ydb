#include "formatter.h"

#include <library/cpp/yt/logging/structured_payload.h>
#include <library/cpp/yt/logging/tag.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/misc/port.h>

#include <bit>
#include <variant>

#ifdef __SSE4_2__
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

#ifdef __SSE4_2__
    auto vectorLow = _mm_set1_epi8(PrintableASCIILow);
    auto vectorHigh = _mm_set1_epi8(PrintableASCIIHigh);
#endif

    auto appendCharRaw = [&] (char* cursor, unsigned char ch) {
        if (ch == '\n') {
            *cursor++ = '\\';
            *cursor++ = 'n';
        } else if (ch == '\t') {
            *cursor++ = '\\';
            *cursor++ = 't';
        } else if (ch < PrintableASCIILow || ch > PrintableASCIIHigh) {
            *cursor++ = '\\';
            *cursor++ = 'x';
            *cursor++ = IntToHexLowercase[ch >> 4];
            *cursor++ = IntToHexLowercase[ch & 15];
        } else {
            *cursor++ = ch;
        }

        return cursor;
    };

    while (current < message.end()) {
        // Guarantee there is enough space so that per-character bounds checks can be skipped.
        if (out->GetBytesRemaining() < MessageBufferWatermarkSize) {
            out->AppendString(TStringBuf("...<message truncated>"));
            break;
        }

        char* cursor = out->GetCursor();

#ifdef __SSE4_2__
        if (current + 16 <= message.end()) {
            auto value = _mm_lddqu_si128(reinterpret_cast<const __m128i*>(current));
            int mask = _mm_movemask_epi8(_mm_cmplt_epi8(value, vectorLow)) |
                _mm_movemask_epi8(_mm_cmpgt_epi8(value, vectorHigh));

            if (mask == 0) {
                // Fast path: perfect 16 chars
                _mm_storeu_si128(reinterpret_cast<__m128i*>(cursor), value);
                out->Advance(16);
                current += 16;
                continue;
            }

            int processed = 0;
            while (mask != 0) {
                int badCharIndex = std::countr_zero(static_cast<ui32>(mask));

                while (processed < badCharIndex) {
                    *cursor++ = current[processed++];
                }

                cursor = appendCharRaw(cursor, current[processed++]);

                mask &= mask - 1;
            }

            while (processed < 16) {
                *cursor++ = current[processed++];
            }

            out->Advance(cursor - out->GetCursor());
            current += 16;
            continue;
        }
#endif
        // Unoptimized tail
        cursor = appendCharRaw(cursor, *current++);
        out->Advance(cursor - out->GetCursor());
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
