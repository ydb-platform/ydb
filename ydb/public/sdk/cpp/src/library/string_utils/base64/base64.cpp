#include "base64.h"

#include <libbase64.h>
#include <util/generic/yexception.h>
#include <util/system/cpu_id.h>
#include <util/system/platform.h>

#include <cstdlib>

static const char base64_etab_std[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const char base64_bkw[] = {
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',                // 0..15
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',                // 16..31
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\76', '\0', '\76', '\0', '\77',             // 32.47
    '\64', '\65', '\66', '\67', '\70', '\71', '\72', '\73', '\74', '\75', '\0', '\0', '\0', '\0', '\0', '\0',      // 48..63
    '\0', '\0', '\1', '\2', '\3', '\4', '\5', '\6', '\7', '\10', '\11', '\12', '\13', '\14', '\15', '\16',         // 64..79
    '\17', '\20', '\21', '\22', '\23', '\24', '\25', '\26', '\27', '\30', '\31', '\0', '\0', '\0', '\0', '\77',    // 80..95
    '\0', '\32', '\33', '\34', '\35', '\36', '\37', '\40', '\41', '\42', '\43', '\44', '\45', '\46', '\47', '\50', // 96..111
    '\51', '\52', '\53', '\54', '\55', '\56', '\57', '\60', '\61', '\62', '\63', '\0', '\0', '\0', '\0', '\0',     // 112..127
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',                // 128..143
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',
    '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};

static_assert(Y_ARRAY_SIZE(base64_bkw) == 256, "wrong size");

// Base64 for url encoding, RFC3548
static const char base64_etab_url[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

static inline unsigned char GetBase64EncodedIndex0(unsigned char octet0) {
    return (octet0 >> 2);
}

static inline unsigned char GetBase64EncodedIndex1(unsigned char octet0, unsigned char octet1) {
    return (((octet0 << 4) & 0x30) | ((octet1 >> 4) & 0x0f));
}

static inline unsigned char GetBase64EncodedIndex2(unsigned char octet1, unsigned char octet2) {
    return (((octet1 << 2) & 0x3c) | ((octet2 >> 6) & 0x03));
}

static inline unsigned char GetBase64EncodedIndex3(unsigned char octet2) {
    return (octet2 & 0x3f);
}

template <bool urlVersion, bool usePadding = true>
static inline char* Base64EncodeImpl(char* outstr, const unsigned char* instr, size_t len) {
    const char* const base64_etab = (urlVersion ? base64_etab_url : base64_etab_std);
    const char pad = (urlVersion ? ',' : '=');

    size_t idx = 0;

    while (idx + 2 < len) {
        *outstr++ = base64_etab[GetBase64EncodedIndex0(instr[idx])];
        *outstr++ = base64_etab[GetBase64EncodedIndex1(instr[idx], instr[idx + 1])];
        *outstr++ = base64_etab[GetBase64EncodedIndex2(instr[idx + 1], instr[idx + 2])];
        *outstr++ = base64_etab[GetBase64EncodedIndex3(instr[idx + 2])];
        idx += 3;
    }
    if (idx < len) {
        *outstr++ = base64_etab[GetBase64EncodedIndex0(instr[idx])];
        if (idx + 1 < len) {
            *outstr++ = base64_etab[GetBase64EncodedIndex1(instr[idx], instr[idx + 1])];
            *outstr++ = base64_etab[GetBase64EncodedIndex2(instr[idx + 1], '\0')];
        } else {
            *outstr++ = base64_etab[GetBase64EncodedIndex1(instr[idx], '\0')];
            if (usePadding) {
                *outstr++ = pad;
            }
        }
        if (usePadding) {
            *outstr++ = pad;
        }
    }
    *outstr = 0;

    return outstr;
}

static char* Base64EncodePlain(char* outstr, const unsigned char* instr, size_t len) {
    return Base64EncodeImpl<false>(outstr, instr, len);
}

char* Base64EncodeUrl(char* outstr, const unsigned char* instr, size_t len) {
    return Base64EncodeImpl<true>(outstr, instr, len);
}

char* Base64EncodeUrlNoPadding(char* outstr, const unsigned char* instr, size_t len) {
    return Base64EncodeImpl<true, false>(outstr, instr, len);
}

inline void uudecode_1(char* dst, unsigned char* src) {
    dst[0] = char((base64_bkw[src[0]] << 2) | (base64_bkw[src[1]] >> 4));
    dst[1] = char((base64_bkw[src[1]] << 4) | (base64_bkw[src[2]] >> 2));
    dst[2] = char((base64_bkw[src[2]] << 6) | base64_bkw[src[3]]);
}

static size_t Base64DecodePlain(void* dst, const char* b, const char* e) {
    size_t n = 0;
    while (b < e) {
        uudecode_1((char*)dst + n, (unsigned char*)b);

        b += 4;
        n += 3;
    }

    if (n > 0) {
        if (b[-1] == ',' || b[-1] == '=') {
            n--;

            if (b[-2] == ',' || b[-2] == '=') {
                n--;
            }
        }
    }

    return n;
}

// Table for Base64StrictDecode
static const char base64_bkw_strict[] =
    "\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100"
    "\100\100\100\100\100\100\100\100\100\100\100\76\101\76\100\77\64\65\66\67\70\71\72\73\74\75\100\100\100\101\100\100"
    "\100\0\1\2\3\4\5\6\7\10\11\12\13\14\15\16\17\20\21\22\23\24\25\26\27\30\31\100\100\100\100\77"
    "\100\32\33\34\35\36\37\40\41\42\43\44\45\46\47\50\51\52\53\54\55\56\57\60\61\62\63\100\100\100\100\100"
    "\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100"
    "\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100"
    "\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100"
    "\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100\100";

size_t Base64StrictDecode(void* out, const char* b, const char* e) {
    char* dst = (char*)out;
    const unsigned char* src = (unsigned char*)b;
    const unsigned char* const end = (unsigned char*)e;

    Y_ENSURE(!((e - b) % 4), "incorrect input length for base64 decode");

    while (src < end) {
        const char zeroth = base64_bkw_strict[src[0]];
        const char first = base64_bkw_strict[src[1]];
        const char second = base64_bkw_strict[src[2]];
        const char third = base64_bkw_strict[src[3]];

        constexpr char invalid = 64;
        constexpr char padding = 65;
        if (Y_UNLIKELY(zeroth == invalid || first == invalid ||
                       second == invalid || third == invalid ||
                       zeroth == padding || first == padding))
        {
            ythrow yexception() << "invalid character in input";
        }

        dst[0] = char((zeroth << 2) | (first >> 4));
        dst[1] = char((first << 4) | (second >> 2));
        dst[2] = char((second << 6) | third);

        src += 4;
        dst += 3;

        if (src[-1] == ',' || src[-1] == '=') {
            --dst;

            if (src[-2] == ',' || src[-2] == '=') {
                --dst;
            }
        } else if (Y_UNLIKELY(src[-2] == ',' || src[-2] == '=')) {
            ythrow yexception() << "incorrect padding";
        }
    }

    return dst - (char*)out;
}

size_t Base64Decode(void* dst, const char* b, const char* e) {
    const auto size = e - b;
    Y_ENSURE(!(size % 4), "incorrect input length for base64 decode");
    if (Y_LIKELY(size < 8)) {
        return Base64DecodePlain(dst, b, e);
    }

    size_t outLen;
    base64_decode(b, size, (char*)dst, &outLen, 0);

    return outLen;
}

size_t Base64DecodeUneven(void* dst, const std::string_view s) {
    const size_t tailSize = s.length() % 4;
    if (tailSize == 0) {
        return Base64Decode(dst, s.begin(), s.end());
    }

    // divide s into even part and tail and decode in two step, to avoid memory allocation
    char tail[4] = {'=', '=', '=', '='};
    memcpy(tail, s.end() - tailSize, tailSize);
    size_t decodedEven = s.length() > 4 ? Base64Decode(dst, s.begin(), s.end() - tailSize) : 0;
    // there should not be tail of size 1 it's incorrect for 8-bit bytes
    size_t decodedTail = tailSize != 1 ? Base64Decode(static_cast<char*>(dst) + decodedEven, tail, tail + 4) : 0;
    return decodedEven + decodedTail;
}

std::string Base64DecodeUneven(const std::string_view s) {
    std::string ret;
    ret.resize(Base64DecodeBufSize(s.size()));
    size_t size = Base64DecodeUneven(const_cast<char*>(ret.data()), s);
    ret.resize(size);
    return ret;
}

char* Base64Encode(char* outstr, const unsigned char* instr, size_t len) {
    if (Y_LIKELY(len < 8)) {
        return Base64EncodePlain(outstr, instr, len);
    }

    size_t outLen;
    base64_encode((char*)instr, len, outstr, &outLen, 0);

    *(outstr + outLen) = '\0';
    return outstr + outLen;
}
