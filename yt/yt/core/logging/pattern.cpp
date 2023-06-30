#include "pattern.h"

#ifdef YT_USE_SSE42
    #include <emmintrin.h>
    #include <pmmintrin.h>
#endif

namespace NYT::NLogging {

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
#ifdef YT_USE_SSE42
        // Use SSE for optimization.
        if (current + 16 > message.end()) {
            appendChar();
        } else if (out->GetBytesRemaining() < MessageBufferWatermarkSize) {
            out->AppendString(TStringBuf("...<message truncated>"));
            break;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
