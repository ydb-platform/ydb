#include "escape.h"

#ifdef YT_USE_SSE42
    #include <util/system/cpu_id.h>
#endif

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

namespace {

#ifdef YT_USE_SSE42

const char _m128i_shift_right[31] = {
     0,  1,  2,  3,  4,  5,  6,  7,
     8,  9, 10, 11, 12, 13, 14, 15,
    -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1
};

// This method performs an "aligned" load of |p| into 128-bit register.
// If |p| is not aligned then the returned value will contain a (byte-)prefix
// of memory region pointed by |p| truncated at the first 16-byte boundary.
// The length of the result is stored into |length|.
//
// Note that real motivation for this method is to avoid accidental page faults
// with direct unaligned reads. I. e., if you have 4 bytes at the end of a page
// then unaligned read will read 16 - 4 = 12 bytes from the next page causing
// a page fault; if the next page is unmapped this will incur a segmentation
// fault and terminate the process.
YT_ATTRIBUTE_NO_SANITIZE_ADDRESS inline __m128i AlignedPrefixLoad(
    const void* p,
    int* length) Y_NO_SANITIZE("memory")
{
    int offset = (size_t)p & 15; *length = 16 - offset;

    if (offset) {
        // Load and shift to the right.
        // (Kudos to glibc authors for fast implementation).
        return _mm_shuffle_epi8(
            _mm_load_si128((__m128i*)((char*)p - offset)),
            _mm_loadu_si128((__m128i*)(_m128i_shift_right + offset)));
    } else {
        // Just load.
        return _mm_load_si128((__m128i*)p);
    }
}

YT_ATTRIBUTE_NO_SANITIZE_ADDRESS inline const char* FindNextSymbol(
    const char* begin,
    const char* end,
    __m128i symbols,
    int count) Y_NO_SANITIZE("memory")
{
    const char* current = begin;
    int length = end - begin;
    int result, result2, tmp;

    __m128i value = AlignedPrefixLoad(current, &tmp);
    tmp = Min(tmp, length);

    do {
        // In short, PCMPxSTRx instruction takes two 128-bit registers with
        // packed bytes and performs string comparison on them with user-defined
        // strategy. As the result PCMPxSTRx produces a lot of stuff, i. e.
        // match bit-mask, LSB or MSB of that bit-mask, and a few flags.
        //
        // See http://software.intel.com/sites/default/files/m/0/3/c/d/4/18187-d9156103.pdf
        //
        // In our case we are doing the following:
        //   - _SIDD_UBYTE_OPS - matching unsigned bytes,
        //   - _SIDD_CMP_EQUAL_ANY - comparing any byte from %xmm0 with any byte of %xmm1,
        //   - _SIDD_MASKED_POSITIVE_POLARITY - are interested only in proper bytes with positive matches,
        //   - _SIDD_LEAST_SIGNIFICANT - are interested in the index of least significant match position.
        //
        // In human terms this means "find position of first occurrence of
        // any byte from %xmm0 in %xmm1".
        //
        // XXX(sandello): These intrinsics compile down to a single
        // "pcmpestri $0x20,%xmm0,%xmm1" instruction, because |result| is written
        // to %ecx and |result2| is (simultaneously) written to CFlag.
        // We are interested in CFlag because it is cheaper to check.
        result = _mm_cmpestri(
            symbols,
            count,
            value,
            tmp,
            _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY |
            _SIDD_MASKED_POSITIVE_POLARITY | _SIDD_LEAST_SIGNIFICANT);
        result2 = _mm_cmpestrc(
            symbols,
            count,
            value,
            tmp,
            _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY |
            _SIDD_MASKED_POSITIVE_POLARITY | _SIDD_LEAST_SIGNIFICANT);

        if (result2) {
            return current + result;
        } else {
            current += tmp;
            length -= tmp;
        }

        if (length > 0) {
            // We may load more bytes than needed (but within memory page, due to 16-byte alignment)
            // but subsequent call to _mm_cmpestri compares only appropriate bytes.
            value = _mm_load_si128((__m128i*)current);
            tmp = Min(16, length);
        } else {
            break;
        }
    } while (true);

    YT_VERIFY(current == end);
    return current;
}

#endif

static inline const char* FindNextSymbol(
    const char* begin,
    const char* end,
    const bool* bitmap)
{
    // XXX(sandello): Manual loop unrolling saves about 8% CPU.
    const char* current = begin;
#define DO_1  if (bitmap[static_cast<ui8>(*current)]) { return current; } ++current;
#define DO_4  DO_1 DO_1 DO_1 DO_1
#define DO_16 DO_4 DO_4 DO_4 DO_4
    while (current + 16 < end) { DO_16; }
    while (current + 4  < end) { DO_4;  }
    while (current      < end) { DO_1;  }
#undef DO_1
#undef DO_4
#undef DO_16
    YT_ASSERT(current == end);
    return current;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TEscapeTable::TEscapeTable()
{ }

void TEscapeTable::FillStops(const std::vector<char>& stopSymbols)
{
    YT_VERIFY(stopSymbols.size() <= 16);

#ifdef YT_USE_SSE42
    if (NX86::CachedHaveSSE42()) {
        char storage[16] = {0};

        SymbolCount = stopSymbols.size();
        for (int i = 0; i < SymbolCount; ++i) {
            storage[i] = stopSymbols[i]; // :) C-style!
        }

        Symbols = _mm_setr_epi8(
            storage[0],  storage[1],  storage[2],  storage[3],
            storage[4],  storage[5],  storage[6],  storage[7],
            storage[8],  storage[9],  storage[10], storage[11],
            storage[12], storage[13], storage[14], storage[15]);
    } else
#endif
    {
        for (int i = 0; i < 256; ++i) {
            Bitmap[i] = false;
        }

        for (char stopSymbol : stopSymbols) {
            Bitmap[static_cast<ui8>(stopSymbol)] = true;
        }
    }
}

const char* TEscapeTable::FindNext(const char* begin, const char* end) const
{
    if (begin == end) {
        return end;
    }
#ifdef YT_USE_SSE42
    if (NX86::CachedHaveSSE42()) {
        return FindNextSymbol(begin, end, Symbols, SymbolCount);
    } else
#endif
    {
        return FindNextSymbol(begin, end, Bitmap);
    }
}

////////////////////////////////////////////////////////////////////////////////

void EscapeAndWrite(
    TStringBuf string,
    IOutputStream* stream,
    const TEscapeTable& escapeTable)
{
    if (escapeTable.EscapingSymbol) {
        auto* begin = string.begin();
        auto* end = string.end();
        auto* next = begin;
        for (; begin != end; begin = next) {
            next = escapeTable.FindNext(begin, end);

            stream->Write(begin, next - begin);
            if (next != end) {
                stream->Write(escapeTable.EscapingSymbol);
                stream->Write(EscapeForward[static_cast<ui8>(*next)]);
                ++next;
            }
        }
    } else {
        stream->Write(string);
    }
}

TString Escape(
    TStringBuf string,
    const TEscapeTable& escapeTable)
{
    if (escapeTable.EscapingSymbol) {
        TString result;
        // In worst case result length will be twice the original length.
        result.reserve(2 * string.length());
        auto* begin = string.begin();
        auto* end = string.end();
        auto* next = begin;
        for (; begin != end; begin = next) {
            next = escapeTable.FindNext(begin, end);
            result.append(begin, next);
            if (next != end) {
                result.append(escapeTable.EscapingSymbol);
                result.append(EscapeForward[static_cast<ui8>(*next)]);
                ++next;
            }
        }
        return result;
    } else {
        return TString(string);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
