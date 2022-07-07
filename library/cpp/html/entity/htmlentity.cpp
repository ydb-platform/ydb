#include "htmlentity.h"

#include <util/string/util.h>
#include <util/system/defaults.h>
#include <library/cpp/charset/recyr.hh>
#include <library/cpp/charset/codepage.h>
#include <util/charset/utf8.h>
#include <util/string/strspn.h>
#include <util/string/hex.h>
#include <util/generic/hash_set.h>

#define isalpha(c) ('a' <= (c) && (c) <= 'z' || 'A' <= (c) && (c) <= 'Z')
#define isdigit(c) ('0' <= (c) && (c) <= '9')
#define isalnum(c) (isalpha(c) || isdigit(c))

#define TEST_CHAR_AT_IMPL(condition, i, len) ((i < (len)) && (condition(s[i])))
#define TEST_CHAR_AT(condition, i) TEST_CHAR_AT_IMPL(condition, i, len)

static const ui32 UNICODE_BORDER = 0x10FFFF;

enum EPureType {
    PT_SEMIC, // Semicolumn shoud always present
    PT_HTML5,
    PT_HTML5_ATTR
};

// http://www.whatwg.org/specs/web-apps/current-work/multipage/tokenization.html#consume-a-character-reference (anything else comments)
template <EPureType PURE>
static inline bool PureCondition(const char* afterEntityStr, size_t len) {
    if (PURE == PT_HTML5)
        return true;

    const char* s = afterEntityStr;
    if (PURE == PT_SEMIC) {
        return TEST_CHAR_AT(';' ==, 0);
    } else {
        return TEST_CHAR_AT(';' ==, 0) || !(TEST_CHAR_AT('=' ==, 1) || TEST_CHAR_AT(isalnum, 1));
    }
}

template <EPureType PURE>
inline static bool DetectEntity(const unsigned char* const str, size_t len, TEntity* entity) {
    if (len == 0)
        return 0;

    Y_ASSERT(str[0] == '&');

    if (DecodeNamedEntity(str + 1, len - 1, entity)) { // exclude '&'
        if (PureCondition<PURE>((const char*)str + entity->Len, len - entity->Len)) {
            entity->Len += 1; // add '&'
            Y_ASSERT(entity->Len <= len);
            return true;
        }
    }

    return false;
}

static size_t DetectNumber(const char* inputStr, size_t len, wchar32* codepoint) {
    if (len < 2)
        return 0;

    Y_ASSERT(inputStr[0] == '#');

    static TCompactStrSpn DIGITS("0123456789");

    const char* digitEnd = DIGITS.FindFirstNotOf<const char*>(inputStr + 1, inputStr + len);

    if (digitEnd == inputStr + 1)
        return 0;

    *codepoint = inputStr[1] - '0';
    for (auto sym = inputStr + 2; sym != digitEnd; ++sym) {
        if (*codepoint < UNICODE_BORDER)
            *codepoint = *codepoint * 10 + (*sym - '0');
    }

    return digitEnd - inputStr;
}

static size_t DetectXNumber(const char* inputStr, size_t len, wchar32* codepoint) {
    if (len < 3)
        return 0;

    Y_ASSERT(inputStr[0] == '#');
    Y_ASSERT(inputStr[1] == 'x' || inputStr[1] == 'X');

    static TCompactStrSpn XDIGITS("0123456789ABCDEFabcdef");

    const char* digitEnd = XDIGITS.FindFirstNotOf<const char*>(inputStr + 2, inputStr + len);

    if (digitEnd == inputStr + 2)
        return 0;

    *codepoint = Char2Digit(inputStr[2]);
    for (const char* sym = inputStr + 3; sym != digitEnd; ++sym) {
        if (*codepoint < UNICODE_BORDER)
            *codepoint = *codepoint * 16 + Char2Digit(*sym);
    }

    return digitEnd - inputStr;
}

///////////////////////////////////////////////////////////////////////////////

static inline void FixBadNumber(wchar32* c) {
    if (*c == 0)
        *c = BROKEN_RUNE;

    if ((0xD800 <= *c && *c <= 0xDFFF) || *c > UNICODE_BORDER) {
        *c = BROKEN_RUNE;
    }

    if (128 <= *c && *c < 160)
        *c = CodePageByCharset(CODES_ASCII)->unicode[*c];

    // I don't know what does it mean and what the reason.
    if (0xF000 <= *c && *c < 0xF100) // UNKNOWN PLANE
        *c = '\x20';
}

template <EPureType PURE>
static inline size_t DoNumber(const unsigned char* const s, size_t len, wchar32* c) {
    Y_ASSERT(s[0] == '#');

    size_t clen = 0;

    if (s[1] == 'x' || s[1] == 'X')
        clen = DetectXNumber((const char*)s, len, c);
    else
        clen = DetectNumber((const char*)s, len, c);

    if (clen != 0) {
        if (!PureCondition<PURE>((const char*)s + clen, len - clen)) {
            return 0;
        }

        FixBadNumber(c);
        return clen + TEST_CHAR_AT(';' ==, clen);
    }

    return 0;
}

static inline size_t DoSymbol(ECharset cp, const unsigned char* const s, size_t len, wchar32* c) {
    size_t written = 0;
    size_t clen = 0;
    RECODE_RESULT res = RecodeToUnicode(cp, (const char*)s, c, len, 1, clen, written);
    bool error = !(res == RECODE_OK || res == RECODE_EOOUTPUT);
    if (error || clen == 0)
        clen = 1;
    if (error || written == 0)
        *c = BROKEN_RUNE;

    return clen;
}

///////////////////////////////////////////////////////////////////////////////

template <EPureType PURE>
inline bool HtTryDecodeEntityT(const unsigned char* const s, size_t len, TEntity* entity) {
    Y_ASSERT(len != 0);
    Y_ASSERT(s[0] == '&');

    if (len > 2) {
        if (isalpha(s[1])) {
            return DetectEntity<PURE>(s, len, entity);
        }

        if (s[1] == '#') {
            entity->Codepoint2 = 0;
            entity->Len = DoNumber<PURE>(s + 1, len - 1, &(entity->Codepoint1));
            if (entity->Len != 0) {
                entity->Len += 1; // Add '&'
                Y_ASSERT(entity->Len <= len);
                return true;
            }
        }
    }

    return false;
}

template <EPureType PURE>
inline bool HtTryDecodeEntityT(const TStringBuf& str, TEntity* entity) {
    return HtTryDecodeEntityT<PURE>((const unsigned char*)str.data(), str.length(), entity);
}

bool HtTryDecodeEntity(const char* str, size_t len, TEntity* entity) {
    return HtTryDecodeEntityT<PT_HTML5>((const unsigned char*)str, len, entity);
}

///////////////////////////////////////////////////////////////////////////////

// the string is in ASCII-compatible encoding, so entities are found as-is
TStringBuf HtTryEntDecodeAsciiCompat(const TStringBuf& src, char* dst, size_t dstlen, ECharset cpsrc) {
    const char* const dstbeg = dst;
    const char* const dstend = dstbeg + dstlen;

    TStringBuf out;
    TStringBuf str(src);

    for (size_t curpos = 0, nwr = 0;;) {
        const size_t nxtpos = str.find('&', curpos);
        const TStringBuf tail = str.SubStr(nxtpos);

        if (tail.empty()) {
            if (dstbeg == dst) { // we haven't written anything
                out = src;
                break;
            }
            if (dst + str.length() <= dstend) { // sufficient space
                memmove(dst, str.data(), str.length());
                out = TStringBuf(dstbeg, dst - dstbeg + str.length());
            }
            break;
        }

        if (dst + nxtpos >= dstend) // insufficient space
            break;

        TEntity entity;
        if (!HtTryDecodeEntityT<PT_HTML5>(tail, &entity)) {
            ++curpos;
            continue;
        }

        memmove(dst, str.data(), nxtpos);
        dst += nxtpos;

        if (RECODE_OK != RecodeFromUnicode(cpsrc, entity.Codepoint1, dst, dstend - dst, nwr))
            break;

        dst += nwr;

        if (entity.Codepoint2 != 0) {
            if (RECODE_OK != RecodeFromUnicode(cpsrc, entity.Codepoint2, dst, dstend - dst, nwr))
                break;
            dst += nwr;
        }

        str = tail.SubStr(entity.Len);
        curpos = 0;
    }

    return out;
}

// the string is in ASCII-compatible encoding, so entities are found as-is
// however, the target encoding is potentially different
TStringBuf HtTryEntDecodeAsciiCompat(const TStringBuf& src, char* dst, size_t dstlen, ECharset cpsrc, ECharset cpdst) {
    if (cpsrc == cpdst)
        return HtTryEntDecodeAsciiCompat(src, dst, dstlen, cpsrc);

    const char* const dstbeg = dst;
    const char* const dstend = dstbeg + dstlen;

    TStringBuf out;
    TStringBuf str(src);

    for (size_t curpos = 0, nrd, nwr;;) {
        const size_t nxtpos = str.find('&', curpos);
        const TStringBuf tail = str.SubStr(nxtpos);

        if (tail.empty()) {
            if (RECODE_OK == Recode(cpsrc, cpdst, str.data(), dst, str.length(), dstend - dst, nrd, nwr))
                out = TStringBuf(dstbeg, dst - dstbeg + nwr);
            break;
        }

        TEntity entity;
        if (!HtTryDecodeEntityT<PT_HTML5>(tail, &entity)) {
            ++curpos;
            continue;
        }

        if (RECODE_OK != Recode(cpsrc, cpdst, str.data(), dst, nxtpos, dstend - dst, nrd, nwr))
            break;
        dst += nwr;

        if (RECODE_OK != RecodeFromUnicode(cpsrc, entity.Codepoint1, dst, dstend - dst, nwr))
            break;

        dst += nwr;

        if (entity.Codepoint2 != 0) {
            if (RECODE_OK != RecodeFromUnicode(cpsrc, entity.Codepoint2, dst, dstend - dst, nwr))
                break;
            dst += nwr;
        }

        str = tail.SubStr(entity.Len);
        curpos = 0;
    }

    return out;
}

///////////////////////////////////////////////////////////////////////////////

template <EPureType PURE>
inline static std::pair<wchar32, wchar32> HtEntDecodeStepT(ECharset cp, const unsigned char*& s, size_t len, unsigned char** map, bool old = false) {
    if (len == 0)
        return std::make_pair(0, 0);

    TEntity entity = {0, 0, 0};
    if (s[0] == '&') {
        if (!HtTryDecodeEntityT<PURE>(s, len, &entity) || (entity.Codepoint2 != 0 && old)) {
            entity.Len = 1;
            entity.Codepoint1 = '&';
        }
    } else {
        entity.Len = DoSymbol(cp, s, len, &(entity.Codepoint1));
    }

    Y_ASSERT(entity.Len <= len);
    s += entity.Len;

    if (map && *map)
        *(*map)++ = (unsigned char)entity.Len;

    return std::make_pair(entity.Codepoint1, entity.Codepoint2);
}

std::pair<wchar32, wchar32> HtEntDecodeStep(ECharset cp, const unsigned char*& str, size_t len, unsigned char** map) {
    return HtEntDecodeStepT<PT_HTML5>(cp, str, len, map);
}

std::pair<wchar32, wchar32> HtEntPureDecodeStep(ECharset cp, const unsigned char*& str, size_t len, unsigned char** map) {
    return HtEntDecodeStepT<PT_SEMIC>(cp, str, len, map);
}

wchar32 HtEntOldDecodeStep(ECharset cp, const unsigned char*& str, size_t len, unsigned char** map) {
    return HtEntDecodeStepT<PT_HTML5>(cp, str, len, map, true).first;
}

wchar32 HtEntOldPureDecodeStep(ECharset cp, const unsigned char*& str, size_t len, unsigned char** map) {
    return HtEntDecodeStepT<PT_SEMIC>(cp, str, len, map, true).first;
}

///////////////////////////////////////////////////////////////////////////////

size_t HtEntDecode(ECharset cp, const char* str, size_t len, wchar32* buf, size_t buflen, unsigned char* map) {
    const unsigned char* s = (const unsigned char*)str;
    const unsigned char* end = (const unsigned char*)(str + len);
    size_t ret = 0;
    while (s < end & ret < buflen) {
        const auto codepoints = HtEntDecodeStep(cp, s, end - s, &map);
        *buf++ = codepoints.first;
        ret++;
        if (codepoints.second != 0 && ret < buflen) {
            *buf++ = codepoints.second;
            ret++;
        }
    }
    return ret;
}

static const THashSet<ECharset> nonCompliant = {
    CODES_UNKNOWNPLANE,
    CODES_CP864,
    CODES_ISO646_CN,
    CODES_ISO646_JP,
    CODES_JISX0201,
    CODES_TCVN,
    CODES_TDS565,
    CODES_VISCII};

static bool IsAsciiCompliant(ECharset dc) {
    return nonCompliant.count(dc) == 0 && (SingleByteCodepage(dc) || dc == CODES_UTF8);
}

const ui32 LOW_CHAR_COUNT = 0x80;

class TNotRecoded {
public:
    bool Flags[LOW_CHAR_COUNT << 1];
    bool AsciiCharsets[CODES_MAX];

public:
    TNotRecoded() {
        memset(&Flags[0], true, LOW_CHAR_COUNT * sizeof(bool));
        memset(&Flags[LOW_CHAR_COUNT], false, LOW_CHAR_COUNT * sizeof(bool));
        Flags[(ui8)'&'] = false;
        Flags[0x7E] = false;
        Flags[0x5C] = false;
        for (ui32 c = 0; c < CODES_MAX; c++) {
            AsciiCharsets[c] = IsAsciiCompliant((ECharset)c);
        }
    }

    bool NotRecoded(unsigned char c) const noexcept {
        return Flags[static_cast<ui8>(c)];
    }

    bool AsciiComliant(ECharset c) const noexcept {
        return (static_cast<int>(c) >= 0) ? AsciiCharsets[c] : false;
    }
};

const TNotRecoded NotRecoded;

template <EPureType PURE>
static size_t HtEntDecodeToUtf8T(ECharset cp,
                                 const char* src, size_t srclen,
                                 char* dst, size_t dstlen) {
    const unsigned char* srcptr = reinterpret_cast<const unsigned char*>(src);
    unsigned char* dstptr = reinterpret_cast<unsigned char*>(dst);
    const unsigned char* const dstbeg = dstptr;
    const unsigned char* const srcend = srcptr + srclen;
    const unsigned char* const dstend = dstbeg + dstlen;
    bool asciiCompl = NotRecoded.AsciiComliant(cp);
    for (size_t len = 0; srcptr < srcend;) {
        if (asciiCompl && NotRecoded.NotRecoded(*srcptr)) {
            if (Y_UNLIKELY(dstptr >= dstend)) {
                return 0;
            }
            *dstptr++ = *srcptr++;
            continue;
        }
        const auto runes = HtEntDecodeStepT<PURE>(cp, srcptr, srcend - srcptr, nullptr);
        if (RECODE_OK != SafeWriteUTF8Char(runes.first, len, dstptr, dstend))
            return 0;
        dstptr += len;

        if (runes.second != 0) {
            if (RECODE_OK != SafeWriteUTF8Char(runes.second, len, dstptr, dstend))
                return 0;
            dstptr += len;
        }
    }
    return dstptr - dstbeg;
}

size_t HtEntDecodeToUtf8(ECharset cp,
                         const char* src, size_t srclen,
                         char* dst, size_t dstlen) {
    return HtEntDecodeToUtf8T<PT_HTML5>(cp, src, srclen, dst, dstlen);
}

size_t HtDecodeAttrToUtf8(ECharset cp,
                          const char* src, size_t srclen,
                          char* dst, size_t dstlen) {
    return HtEntDecodeToUtf8T<PT_HTML5_ATTR>(cp, src, srclen, dst, dstlen);
}

size_t HtEntDecodeToChar(ECharset cp, const char* str, size_t len, wchar16* dst, unsigned char* m) {
    const unsigned char* s = reinterpret_cast<const unsigned char*>(str);
    const unsigned char* end = reinterpret_cast<const unsigned char*>(str + len);
    wchar16* startDst = dst;
    bool asciiCompl = NotRecoded.AsciiComliant(cp);
    while (s < end) {
        if (asciiCompl && NotRecoded.NotRecoded(*s)) {
            *dst++ = *s++;
            continue;
        }
        const auto codepoints = HtEntDecodeStep(cp, s, end - s, &m);
        const size_t len2 = WriteSymbol(codepoints.first, dst);
        if (codepoints.second != 0)
            WriteSymbol(codepoints.second, dst);

        if (m != nullptr && len2 > 1)
            *(m++) = 0;
    }
    return dst - startDst;
}

bool HtLinkDecode(const char* in, char* out, size_t buflen, size_t& written, ECharset cp) {
    return HtLinkDecode(TStringBuf(in, strlen(in)), out, buflen, written, cp);
}

bool HtLinkDecode(const TStringBuf& in, char* out, size_t buflen, size_t& written, ECharset cp) {
    static const char XDIGIT[] = "0123456789ABCDEFabcdef";

    written = 0;
    size_t elen = 0;
    const char* inpEnd = in.data() + in.size();
    bool asciiCompl = NotRecoded.AsciiComliant(cp);

    for (const char* p = in.data(); p < inpEnd && *p; p += elen) {
        bool isEntity = false;
        wchar32 charval = (unsigned char)*p;
        elen = 1;

        if (*p == '&') {
            TEntity entity;
            if (HtTryDecodeEntityT<PT_SEMIC>((const unsigned char*)p, inpEnd - p, &entity) && entity.Codepoint2 == 0) {
                elen = entity.Len;
                charval = entity.Codepoint1;
                isEntity = true;
            } else {
                charval = '&';
                elen = 1;
            }
        }

        if (cp != CODES_UNKNOWN && !isEntity) {
            if (asciiCompl && NotRecoded.NotRecoded(*p)) {
                charval = *p;
            } else {
                DoSymbol(cp, reinterpret_cast<const unsigned char*>(p), 6, &charval);
                if (charval == BROKEN_RUNE)
                    return false;
            }
            isEntity = true;
        }

        if (charval <= 0x20 || charval >= 0x7F) {
            if (isEntity && charval >= 0x7F) {
                const size_t BUFLEN = 4; // 4 max length of UTF8 encoded character
                unsigned char buf[BUFLEN];
                size_t len = 0;
                if (SafeWriteUTF8Char(charval, len, buf, buf + BUFLEN) != RECODE_OK) // actually always OK
                    return false;
                const size_t n = len * 3;
                if (written + n < buflen) {
                    for (size_t i = 0; i < len; ++i) {
                        out[written++] = '%';
                        out[written++] = XDIGIT[buf[i] >> 4];
                        out[written++] = XDIGIT[buf[i] & 15];
                    }
                } else
                    return false; // ERROR_SMALL_BUFFER
            } else {
                if (written + 3 > buflen)
                    return false; // ERROR_SMALL_BUFFER

                unsigned char ch = *p;
                if (isEntity) {
                    ch = charval;
                }
                out[written++] = '%';
                out[written++] = XDIGIT[ch >> 4];
                out[written++] = XDIGIT[ch & 15];
            }
        } else {
            if (written + 1 < buflen) {
                out[written++] = (unsigned char)charval;
            } else {
                return false; // ERROR_SMALL_BUFFER
            }
        }
    }

    return true;
}
