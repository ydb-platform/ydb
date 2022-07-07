#pragma once

#include "decoder.h"

#include <util/system/defaults.h>
#include <library/cpp/charset/doccodes.h>
#include <util/generic/strbuf.h>
#include <utility>

/******************************************************/
/*           direct decoding actions                  */
/******************************************************/

//! Try decode named or numeric entity using general html5 standard rules.
//! @param str - string started with '&'.
bool HtTryDecodeEntity(const char* str, size_t len, TEntity* entity);

/******************************************************/
/*           step by step actions                     */
/******************************************************/

// NOTE: Some entities have two codepoinst, if entity has one codepoint
// then the second wchar32 in pair is zero.
// Decodes with html5 standard rules.
std::pair<wchar32, wchar32> HtEntDecodeStep(ECharset cp, const unsigned char*& s, size_t len, unsigned char** map);

// Decodes assuming that ';' should always present after entity.
std::pair<wchar32, wchar32> HtEntPureDecodeStep(ECharset cp, const unsigned char*& s, size_t len, unsigned char** map);

// Similar with HtEntDecodeStep, but do not decodes named entities with two codepoints.
// Use HtEntDecodeStep and HtEntPureDecodeStep instead.
wchar32 HtEntOldDecodeStep(ECharset cp, const unsigned char*& s, size_t len, unsigned char** map);
wchar32 HtEntOldPureDecodeStep(ECharset cp, const unsigned char*& s, size_t len, unsigned char** map);

/******************************************************/
/*           complete actions                         */
/******************************************************/

// Try decode str using general html5 standard rules.
// Stops when str or buffer finish.
size_t HtEntDecode(ECharset cp, const char* str, size_t len, wchar32* buffer, size_t buflen, unsigned char* char_lengthes = nullptr);

size_t HtEntDecodeToUtf8(ECharset cp, const char* src, size_t srclen, char* dst, size_t dstlen);

// Special rules for attributes decoding
// http://www.whatwg.org/specs/web-apps/current-work/multipage/syntax.html#character-reference-in-attribute-value-state
size_t HtDecodeAttrToUtf8(ECharset cp, const char* src, size_t srclen, char* dst, size_t dstlen);

size_t HtEntDecodeToChar(ECharset cp, const char* str, size_t len, wchar16* buffer, unsigned char* char_lengthes = nullptr);

/**
 * decode HTML entities if any
 * @param src      input buffer
 * @param dst      output buffer
 * @param dstlen   output buffer length
 * @param cpsrc    input buffer encoding, ascii-compatible
 * @param cpdst    output buffer encoding, if different from cpsrc
 * @return         src if no entities and encodings are the same (dst remains untouched)
 *                 NULL if dst was not sufficiently long
 *                 dst-based output buffer with decoded string
 * @note           entities must be pure, with the terminating ";"
 */
TStringBuf HtTryEntDecodeAsciiCompat(const TStringBuf& src, char* dst, size_t dstlen, ECharset cpsrc = CODES_UTF8);
TStringBuf HtTryEntDecodeAsciiCompat(const TStringBuf& src, char* dst, size_t dstlen, ECharset cpsrc, ECharset cpdst);

//! decodes HTML entities and converts non-ASCII characters to unicode, then converts unicode to UTF8 and percent-encodes
//! @param text     zero-terminated text of link
//! @param buffer   buffer receiving UTF8 percent-encoded text of link
//! @param buflen   length of output buffer
//! @param cp       code page object used to convert non-ASCII characters
//! @note HTML entities directly converted into unicode characters, non-ASCII characters
//!       converted into unicode using code page object if it is passed to the function,
//!       then unicode characters converted to UTF8 and percent-encoded,
//!       percent-encoded text in the link copied into output buffer as is
bool HtLinkDecode(const char* text, char* buffer, size_t buflen, size_t& written, ECharset cp = CODES_UNKNOWN);
bool HtLinkDecode(const TStringBuf& text, char* buffer, size_t buflen, size_t& written, ECharset cp = CODES_UNKNOWN);

static inline bool HtLinkDecode(const char* text, char* buffer, size_t buflen, ECharset cp = CODES_UNKNOWN) {
    size_t written;
    const bool ok = HtLinkDecode(text, buffer, buflen, written, cp);
    if (ok)
        buffer[written] = '\x00';
    return ok;
}
