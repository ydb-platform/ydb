/*
 * Copyright (c) 2014-2021 Steven G. Johnson, Jiahao Chen, Peter Colberg, Tony Kelman, Scott P. Jones, and other contributors.
 * Copyright (c) 2009 Public Software Group e. V., Berlin, Germany
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */


/**
 * @mainpage
 *
 * utf8proc is a free/open-source (MIT/expat licensed) C library
 * providing Unicode normalization, case-folding, and other operations
 * for strings in the UTF-8 encoding, supporting up-to-date Unicode versions.
 * See the utf8proc home page (http://julialang.org/utf8proc/)
 * for downloads and other information, or the source code on github
 * (https://github.com/JuliaLang/utf8proc).
 *
 * For the utf8proc API documentation, see: @ref utf8proc.h
 *
 * The features of utf8proc include:
 *
 * - Transformation of strings (utf8proc_map()) to:
 *    - decompose (@ref UTF8PROC_DECOMPOSE) or compose (@ref UTF8PROC_COMPOSE) Unicode combining characters (http://en.wikipedia.org/wiki/Combining_character)
 *    - canonicalize Unicode compatibility characters (@ref UTF8PROC_COMPAT)
 *    - strip "ignorable" (@ref UTF8PROC_IGNORE) characters, control characters (@ref UTF8PROC_STRIPCC), or combining characters such as accents (@ref UTF8PROC_STRIPMARK)
 *    - case-folding (@ref UTF8PROC_CASEFOLD)
 * - Unicode normalization: utf8proc_NFD(), utf8proc_NFC(), utf8proc_NFKD(), utf8proc_NFKC()
 * - Detecting grapheme boundaries (utf8proc_grapheme_break() and @ref UTF8PROC_CHARBOUND)
 * - Character-width computation: utf8proc_charwidth()
 * - Classification of characters by Unicode category: utf8proc_category() and utf8proc_category_string()
 * - Encode (utf8proc_encode_char()) and decode (utf8proc_iterate()) Unicode codepoints to/from UTF-8.
 */

/** @file */

#ifndef UTF8PROC_H
#define UTF8PROC_H

/** @name API version
 *
 * The utf8proc API version MAJOR.MINOR.PATCH, following
 * semantic-versioning rules (http://semver.org) based on API
 * compatibility.
 *
 * This is also returned at runtime by utf8proc_version(); however, the
 * runtime version may append a string like "-dev" to the version number
 * for prerelease versions.
 *
 * @note The shared-library version number in the Makefile
 *       (and CMakeLists.txt, and MANIFEST) may be different,
 *       being based on ABI compatibility rather than API compatibility.
 */
/** @{ */
/** The MAJOR version number (increased when backwards API compatibility is broken). */
#define UTF8PROC_VERSION_MAJOR 2
/** The MINOR version number (increased when new functionality is added in a backwards-compatible manner). */
#define UTF8PROC_VERSION_MINOR 9
/** The PATCH version (increased for fixes that do not change the API). */
#define UTF8PROC_VERSION_PATCH 0
/** @} */

#include <stdlib.h>
#include "ncexternl.h"

#if defined(_MSC_VER) && _MSC_VER < 1800
// MSVC prior to 2013 lacked stdbool.h and stdint.h
typedef signed char nc_utf8proc_int8_t;
typedef unsigned char nc_utf8proc_uint8_t;
typedef short nc_utf8proc_int16_t;
typedef unsigned short nc_utf8proc_uint16_t;
typedef int nc_utf8proc_int32_t;
typedef unsigned int nc_utf8proc_uint32_t;
#  ifdef _WIN64
typedef __int64 nc_utf8proc_ssize_t;
typedef unsigned __int64 nc_utf8proc_size_t;
#  else
typedef int nc_utf8proc_ssize_t;
typedef unsigned int nc_utf8proc_size_t;
#  endif
#  ifndef __cplusplus
// emulate C99 bool
typedef unsigned char nc_utf8proc_bool;
#    ifndef __bool_true_false_are_defined
#      define false 0
#      define true 1
#      define __bool_true_false_are_defined 1
#    endif
#  else
typedef bool nc_utf8proc_bool;
#  endif
#else
#  include <stddef.h>
#  include <stdbool.h>
#  include <stdint.h>
typedef int8_t nc_utf8proc_int8_t;
typedef uint8_t nc_utf8proc_uint8_t;
typedef int16_t nc_utf8proc_int16_t;
typedef uint16_t nc_utf8proc_uint16_t;
typedef int32_t nc_utf8proc_int32_t;
typedef uint32_t nc_utf8proc_uint32_t;
typedef size_t nc_utf8proc_size_t;
typedef ptrdiff_t nc_utf8proc_ssize_t;
typedef bool nc_utf8proc_bool;
#endif
#include <limits.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Option flags used by several functions in the library.
 */
typedef enum {
  /** The given UTF-8 input is NULL terminated. */
  UTF8PROC_NULLTERM  = (1<<0),
  /** Unicode Versioning Stability has to be respected. */
  UTF8PROC_STABLE    = (1<<1),
  /** Compatibility decomposition (i.e. formatting information is lost). */
  UTF8PROC_COMPAT    = (1<<2),
  /** Return a result with decomposed characters. */
  UTF8PROC_COMPOSE   = (1<<3),
  /** Return a result with decomposed characters. */
  UTF8PROC_DECOMPOSE = (1<<4),
  /** Strip "default ignorable characters" such as SOFT-HYPHEN or ZERO-WIDTH-SPACE. */
  UTF8PROC_IGNORE    = (1<<5),
  /** Return an error, if the input contains unassigned codepoints. */
  UTF8PROC_REJECTNA  = (1<<6),
  /**
   * Indicating that NLF-sequences (LF, CRLF, CR, NEL) are representing a
   * line break, and should be converted to the codepoint for line
   * separation (LS).
   */
  UTF8PROC_NLF2LS    = (1<<7),
  /**
   * Indicating that NLF-sequences are representing a paragraph break, and
   * should be converted to the codepoint for paragraph separation
   * (PS).
   */
  UTF8PROC_NLF2PS    = (1<<8),
  /** Indicating that the meaning of NLF-sequences is unknown. */
  UTF8PROC_NLF2LF    = (UTF8PROC_NLF2LS | UTF8PROC_NLF2PS),
  /** Strips and/or converts control characters.
   *
   * NLF-sequences are transformed into space, except if one of the
   * NLF2LS/PS/LF options is given. HorizontalTab (HT) and FormFeed (FF)
   * are treated as a NLF-sequence in this case.  All other control
   * characters are simply removed.
   */
  UTF8PROC_STRIPCC   = (1<<9),
  /**
   * Performs unicode case folding, to be able to do a case-insensitive
   * string comparison.
   */
  UTF8PROC_CASEFOLD  = (1<<10),
  /**
   * Inserts 0xFF bytes at the beginning of each sequence which is
   * representing a single grapheme cluster (see UAX#29).
   */
  UTF8PROC_CHARBOUND = (1<<11),
  /** Lumps certain characters together.
   *
   * E.g. HYPHEN U+2010 and MINUS U+2212 to ASCII "-". See lump.md for details.
   *
   * If NLF2LF is set, this includes a transformation of paragraph and
   * line separators to ASCII line-feed (LF).
   */
  UTF8PROC_LUMP      = (1<<12),
  /** Strips all character markings.
   *
   * This includes non-spacing, spacing and enclosing (i.e. accents).
   * @note This option works only with @ref UTF8PROC_COMPOSE or
   *       @ref UTF8PROC_DECOMPOSE
   */
  UTF8PROC_STRIPMARK = (1<<13),
  /**
   * Strip unassigned codepoints.
   */
  UTF8PROC_STRIPNA    = (1<<14),
} nc_utf8proc_option_t;

/** @name Error codes
 * Error codes being returned by almost all functions.
 */
/** @{ */
/** Memory could not be allocated. */
#define UTF8PROC_ERROR_NOMEM -1
/** The given string is too long to be processed. */
#define UTF8PROC_ERROR_OVERFLOW -2
/** The given string is not a legal UTF-8 string. */
#define UTF8PROC_ERROR_INVALIDUTF8 -3
/** The @ref UTF8PROC_REJECTNA flag was set and an unassigned codepoint was found. */
#define UTF8PROC_ERROR_NOTASSIGNED -4
/** Invalid options have been used. */
#define UTF8PROC_ERROR_INVALIDOPTS -5
/** @} */

/* @name Types */

/** Holds the value of a property. */
typedef nc_utf8proc_int16_t nc_utf8proc_propval_t;

/** Struct containing information about a codepoint. */
typedef struct nc_utf8proc_property_struct {
  /**
   * Unicode category.
   * @see nc_utf8proc_category_t.
   */
  nc_utf8proc_propval_t category;
  nc_utf8proc_propval_t combining_class;
  /**
   * Bidirectional class.
   * @see nc_utf8proc_bidi_class_t.
   */
  nc_utf8proc_propval_t bidi_class;
  /**
   * @anchor Decomposition type.
   * @see nc_utf8proc_decomp_type_t.
   */
  nc_utf8proc_propval_t decomp_type;
  nc_utf8proc_uint16_t decomp_seqindex;
  nc_utf8proc_uint16_t casefold_seqindex;
  nc_utf8proc_uint16_t uppercase_seqindex;
  nc_utf8proc_uint16_t lowercase_seqindex;
  nc_utf8proc_uint16_t titlecase_seqindex;
  nc_utf8proc_uint16_t comb_index;
  unsigned bidi_mirrored:1;
  unsigned comp_exclusion:1;
  /**
   * Can this codepoint be ignored?
   *
   * Used by nc_utf8proc_decompose_char() when @ref UTF8PROC_IGNORE is
   * passed as an option.
   */
  unsigned ignorable:1;
  unsigned control_boundary:1;
  /** The width of the codepoint. */
  unsigned charwidth:2;
  unsigned pad:2;
  /**
   * Boundclass.
   * @see nc_utf8proc_boundclass_t.
   */
  unsigned boundclass:6;
  unsigned indic_conjunct_break:2;
} nc_utf8proc_property_t;

/** Unicode categories. */
typedef enum {
  UTF8PROC_CATEGORY_CN  = 0, /**< Other, not assigned */
  UTF8PROC_CATEGORY_LU  = 1, /**< Letter, uppercase */
  UTF8PROC_CATEGORY_LL  = 2, /**< Letter, lowercase */
  UTF8PROC_CATEGORY_LT  = 3, /**< Letter, titlecase */
  UTF8PROC_CATEGORY_LM  = 4, /**< Letter, modifier */
  UTF8PROC_CATEGORY_LO  = 5, /**< Letter, other */
  UTF8PROC_CATEGORY_MN  = 6, /**< Mark, nonspacing */
  UTF8PROC_CATEGORY_MC  = 7, /**< Mark, spacing combining */
  UTF8PROC_CATEGORY_ME  = 8, /**< Mark, enclosing */
  UTF8PROC_CATEGORY_ND  = 9, /**< Number, decimal digit */
  UTF8PROC_CATEGORY_NL = 10, /**< Number, letter */
  UTF8PROC_CATEGORY_NO = 11, /**< Number, other */
  UTF8PROC_CATEGORY_PC = 12, /**< Punctuation, connector */
  UTF8PROC_CATEGORY_PD = 13, /**< Punctuation, dash */
  UTF8PROC_CATEGORY_PS = 14, /**< Punctuation, open */
  UTF8PROC_CATEGORY_PE = 15, /**< Punctuation, close */
  UTF8PROC_CATEGORY_PI = 16, /**< Punctuation, initial quote */
  UTF8PROC_CATEGORY_PF = 17, /**< Punctuation, final quote */
  UTF8PROC_CATEGORY_PO = 18, /**< Punctuation, other */
  UTF8PROC_CATEGORY_SM = 19, /**< Symbol, math */
  UTF8PROC_CATEGORY_SC = 20, /**< Symbol, currency */
  UTF8PROC_CATEGORY_SK = 21, /**< Symbol, modifier */
  UTF8PROC_CATEGORY_SO = 22, /**< Symbol, other */
  UTF8PROC_CATEGORY_ZS = 23, /**< Separator, space */
  UTF8PROC_CATEGORY_ZL = 24, /**< Separator, line */
  UTF8PROC_CATEGORY_ZP = 25, /**< Separator, paragraph */
  UTF8PROC_CATEGORY_CC = 26, /**< Other, control */
  UTF8PROC_CATEGORY_CF = 27, /**< Other, format */
  UTF8PROC_CATEGORY_CS = 28, /**< Other, surrogate */
  UTF8PROC_CATEGORY_CO = 29, /**< Other, private use */
} nc_utf8proc_category_t;

/** Bidirectional character classes. */
typedef enum {
  UTF8PROC_BIDI_CLASS_L     = 1, /**< Left-to-Right */
  UTF8PROC_BIDI_CLASS_LRE   = 2, /**< Left-to-Right Embedding */
  UTF8PROC_BIDI_CLASS_LRO   = 3, /**< Left-to-Right Override */
  UTF8PROC_BIDI_CLASS_R     = 4, /**< Right-to-Left */
  UTF8PROC_BIDI_CLASS_AL    = 5, /**< Right-to-Left Arabic */
  UTF8PROC_BIDI_CLASS_RLE   = 6, /**< Right-to-Left Embedding */
  UTF8PROC_BIDI_CLASS_RLO   = 7, /**< Right-to-Left Override */
  UTF8PROC_BIDI_CLASS_PDF   = 8, /**< Pop Directional Format */
  UTF8PROC_BIDI_CLASS_EN    = 9, /**< European Number */
  UTF8PROC_BIDI_CLASS_ES   = 10, /**< European Separator */
  UTF8PROC_BIDI_CLASS_ET   = 11, /**< European Number Terminator */
  UTF8PROC_BIDI_CLASS_AN   = 12, /**< Arabic Number */
  UTF8PROC_BIDI_CLASS_CS   = 13, /**< Common Number Separator */
  UTF8PROC_BIDI_CLASS_NSM  = 14, /**< Nonspacing Mark */
  UTF8PROC_BIDI_CLASS_BN   = 15, /**< Boundary Neutral */
  UTF8PROC_BIDI_CLASS_B    = 16, /**< Paragraph Separator */
  UTF8PROC_BIDI_CLASS_S    = 17, /**< Segment Separator */
  UTF8PROC_BIDI_CLASS_WS   = 18, /**< Whitespace */
  UTF8PROC_BIDI_CLASS_ON   = 19, /**< Other Neutrals */
  UTF8PROC_BIDI_CLASS_LRI  = 20, /**< Left-to-Right Isolate */
  UTF8PROC_BIDI_CLASS_RLI  = 21, /**< Right-to-Left Isolate */
  UTF8PROC_BIDI_CLASS_FSI  = 22, /**< First Strong Isolate */
  UTF8PROC_BIDI_CLASS_PDI  = 23, /**< Pop Directional Isolate */
} nc_utf8proc_bidi_class_t;

/** Decomposition type. */
typedef enum {
  UTF8PROC_DECOMP_TYPE_FONT      = 1, /**< Font */
  UTF8PROC_DECOMP_TYPE_NOBREAK   = 2, /**< Nobreak */
  UTF8PROC_DECOMP_TYPE_INITIAL   = 3, /**< Initial */
  UTF8PROC_DECOMP_TYPE_MEDIAL    = 4, /**< Medial */
  UTF8PROC_DECOMP_TYPE_FINAL     = 5, /**< Final */
  UTF8PROC_DECOMP_TYPE_ISOLATED  = 6, /**< Isolated */
  UTF8PROC_DECOMP_TYPE_CIRCLE    = 7, /**< Circle */
  UTF8PROC_DECOMP_TYPE_SUPER     = 8, /**< Super */
  UTF8PROC_DECOMP_TYPE_SUB       = 9, /**< Sub */
  UTF8PROC_DECOMP_TYPE_VERTICAL = 10, /**< Vertical */
  UTF8PROC_DECOMP_TYPE_WIDE     = 11, /**< Wide */
  UTF8PROC_DECOMP_TYPE_NARROW   = 12, /**< Narrow */
  UTF8PROC_DECOMP_TYPE_SMALL    = 13, /**< Small */
  UTF8PROC_DECOMP_TYPE_SQUARE   = 14, /**< Square */
  UTF8PROC_DECOMP_TYPE_FRACTION = 15, /**< Fraction */
  UTF8PROC_DECOMP_TYPE_COMPAT   = 16, /**< Compat */
} nc_utf8proc_decomp_type_t;

/** Boundclass property. (TR29) */
typedef enum {
  UTF8PROC_BOUNDCLASS_START              =  0, /**< Start */
  UTF8PROC_BOUNDCLASS_OTHER              =  1, /**< Other */
  UTF8PROC_BOUNDCLASS_CR                 =  2, /**< Cr */
  UTF8PROC_BOUNDCLASS_LF                 =  3, /**< Lf */
  UTF8PROC_BOUNDCLASS_CONTROL            =  4, /**< Control */
  UTF8PROC_BOUNDCLASS_EXTEND             =  5, /**< Extend */
  UTF8PROC_BOUNDCLASS_L                  =  6, /**< L */
  UTF8PROC_BOUNDCLASS_V                  =  7, /**< V */
  UTF8PROC_BOUNDCLASS_T                  =  8, /**< T */
  UTF8PROC_BOUNDCLASS_LV                 =  9, /**< Lv */
  UTF8PROC_BOUNDCLASS_LVT                = 10, /**< Lvt */
  UTF8PROC_BOUNDCLASS_REGIONAL_INDICATOR = 11, /**< Regional indicator */
  UTF8PROC_BOUNDCLASS_SPACINGMARK        = 12, /**< Spacingmark */
  UTF8PROC_BOUNDCLASS_PREPEND            = 13, /**< Prepend */
  UTF8PROC_BOUNDCLASS_ZWJ                = 14, /**< Zero Width Joiner */

  /* the following are no longer used in Unicode 11, but we keep
     the constants here for backward compatibility */
  UTF8PROC_BOUNDCLASS_E_BASE             = 15, /**< Emoji Base */
  UTF8PROC_BOUNDCLASS_E_MODIFIER         = 16, /**< Emoji Modifier */
  UTF8PROC_BOUNDCLASS_GLUE_AFTER_ZWJ     = 17, /**< Glue_After_ZWJ */
  UTF8PROC_BOUNDCLASS_E_BASE_GAZ         = 18, /**< E_BASE + GLUE_AFTER_ZJW */

  /* the Extended_Pictographic property is used in the Unicode 11
     grapheme-boundary rules, so we store it in the boundclass field */
  UTF8PROC_BOUNDCLASS_EXTENDED_PICTOGRAPHIC = 19,
  UTF8PROC_BOUNDCLASS_E_ZWG = 20, /* UTF8PROC_BOUNDCLASS_EXTENDED_PICTOGRAPHIC + ZWJ */
} nc_utf8proc_boundclass_t;

/** Indic_Conjunct_Break property. (TR44) */
typedef enum {
  UTF8PROC_INDIC_CONJUNCT_BREAK_NONE = 0,
  UTF8PROC_INDIC_CONJUNCT_BREAK_LINKER = 1,
  UTF8PROC_INDIC_CONJUNCT_BREAK_CONSONANT = 2,
  UTF8PROC_INDIC_CONJUNCT_BREAK_EXTEND = 3,
} nc_utf8proc_indic_conjunct_break_t;

/**
 * Function pointer type passed to nc_utf8proc_map_custom() and
 * nc_utf8proc_decompose_custom(), which is used to specify a user-defined
 * mapping of codepoints to be applied in conjunction with other mappings.
 */
typedef nc_utf8proc_int32_t (*nc_utf8proc_custom_func)(nc_utf8proc_int32_t codepoint, void *data);

/**
 * Array containing the byte lengths of a UTF-8 encoded codepoint based
 * on the first byte.
 */
extern const nc_utf8proc_int8_t nc_utf8proc_utf8class[256];

/**
 * Returns the utf8proc API version as a string MAJOR.MINOR.PATCH
 * (http://semver.org format), possibly with a "-dev" suffix for
 * development versions.
 */
const char *nc_utf8proc_version(void);

/**
 * Returns the utf8proc supported Unicode version as a string MAJOR.MINOR.PATCH.
 */
const char *nc_utf8proc_unicode_version(void);

/**
 * Returns an informative error string for the given utf8proc error code
 * (e.g. the error codes returned by nc_utf8proc_map()).
 */
const char *nc_utf8proc_errmsg(nc_utf8proc_ssize_t errcode);

/**
 * Reads a single codepoint from the UTF-8 sequence being pointed to by `str`.
 * The maximum number of bytes read is `strlen`, unless `strlen` is
 * negative (in which case up to 4 bytes are read).
 *
 * If a valid codepoint could be read, it is stored in the variable
 * pointed to by `codepoint_ref`, otherwise that variable will be set to -1.
 * In case of success, the number of bytes read is returned; otherwise, a
 * negative error code is returned.
 */
EXTERNL nc_utf8proc_ssize_t nc_utf8proc_iterate(const nc_utf8proc_uint8_t *str, nc_utf8proc_ssize_t strlen, nc_utf8proc_int32_t *codepoint_ref);

/**
 * Check if a codepoint is valid (regardless of whether it has been
 * assigned a value by the current Unicode standard).
 *
 * @return 1 if the given `codepoint` is valid and otherwise return 0.
 */
nc_utf8proc_bool nc_utf8proc_codepoint_valid(nc_utf8proc_int32_t codepoint);

/**
 * Encodes the codepoint as an UTF-8 string in the byte array pointed
 * to by `dst`. This array must be at least 4 bytes long.
 *
 * In case of success the number of bytes written is returned, and
 * otherwise 0 is returned.
 *
 * This function does not check whether `codepoint` is valid Unicode.
 */
nc_utf8proc_ssize_t nc_utf8proc_encode_char(nc_utf8proc_int32_t codepoint, nc_utf8proc_uint8_t *dst);

/**
 * Look up the properties for a given codepoint.
 *
 * @param codepoint The Unicode codepoint.
 *
 * @returns
 * A pointer to a (constant) struct containing information about
 * the codepoint.
 * @par
 * If the codepoint is unassigned or invalid, a pointer to a special struct is
 * returned in which `category` is 0 (@ref UTF8PROC_CATEGORY_CN).
 */
const nc_utf8proc_property_t *nc_utf8proc_get_property(nc_utf8proc_int32_t codepoint);

/** Decompose a codepoint into an array of codepoints.
 *
 * @param codepoint the codepoint.
 * @param dst the destination buffer.
 * @param bufsize the size of the destination buffer.
 * @param options one or more of the following flags:
 * - @ref UTF8PROC_REJECTNA  - return an error `codepoint` is unassigned
 * - @ref UTF8PROC_IGNORE    - strip "default ignorable" codepoints
 * - @ref UTF8PROC_CASEFOLD  - apply Unicode casefolding
 * - @ref UTF8PROC_COMPAT    - replace certain codepoints with their
 *                             compatibility decomposition
 * - @ref UTF8PROC_CHARBOUND - insert 0xFF bytes before each grapheme cluster
 * - @ref UTF8PROC_LUMP      - lump certain different codepoints together
 * - @ref UTF8PROC_STRIPMARK - remove all character marks
 * - @ref UTF8PROC_STRIPNA   - remove unassigned codepoints
 * @param last_boundclass
 * Pointer to an integer variable containing
 * the previous codepoint's (boundclass + indic_conjunct_break << 1) if the @ref UTF8PROC_CHARBOUND
 * option is used.  If the string is being processed in order, this can be initialized to 0 for
 * the beginning of the string, and is thereafter updated automatically.  Otherwise, this parameter is ignored.
 *
 * @return
 * In case of success, the number of codepoints written is returned; in case
 * of an error, a negative error code is returned (nc_utf8proc_errmsg()).
 * @par
 * If the number of written codepoints would be bigger than `bufsize`, the
 * required buffer size is returned, while the buffer will be overwritten with
 * undefined data.
 */
nc_utf8proc_ssize_t nc_utf8proc_decompose_char(
  nc_utf8proc_int32_t codepoint, nc_utf8proc_int32_t *dst, nc_utf8proc_ssize_t bufsize,
  nc_utf8proc_option_t options, int *last_boundclass
);

/**
 * The same as nc_utf8proc_decompose_char(), but acts on a whole UTF-8
 * string and orders the decomposed sequences correctly.
 *
 * If the @ref UTF8PROC_NULLTERM flag in `options` is set, processing
 * will be stopped, when a NULL byte is encountered, otherwise `strlen`
 * bytes are processed.  The result (in the form of 32-bit unicode
 * codepoints) is written into the buffer being pointed to by
 * `buffer` (which must contain at least `bufsize` entries).  In case of
 * success, the number of codepoints written is returned; in case of an
 * error, a negative error code is returned (nc_utf8proc_errmsg()).
 * See nc_utf8proc_decompose_custom() to supply additional transformations.
 *
 * If the number of written codepoints would be bigger than `bufsize`, the
 * required buffer size is returned, while the buffer will be overwritten with
 * undefined data.
 */
nc_utf8proc_ssize_t nc_utf8proc_decompose(
  const nc_utf8proc_uint8_t *str, nc_utf8proc_ssize_t strlen,
  nc_utf8proc_int32_t *buffer, nc_utf8proc_ssize_t bufsize, nc_utf8proc_option_t options
);

/**
 * The same as nc_utf8proc_decompose(), but also takes a `custom_func` mapping function
 * that is called on each codepoint in `str` before any other transformations
 * (along with a `custom_data` pointer that is passed through to `custom_func`).
 * The `custom_func` argument is ignored if it is `NULL`.  See also nc_utf8proc_map_custom().
 */
nc_utf8proc_ssize_t nc_utf8proc_decompose_custom(
  const nc_utf8proc_uint8_t *str, nc_utf8proc_ssize_t strlen,
  nc_utf8proc_int32_t *buffer, nc_utf8proc_ssize_t bufsize, nc_utf8proc_option_t options,
  nc_utf8proc_custom_func custom_func, void *custom_data
);

/**
 * Normalizes the sequence of `length` codepoints pointed to by `buffer`
 * in-place (i.e., the result is also stored in `buffer`).
 *
 * @param buffer the (native-endian UTF-32) unicode codepoints to re-encode.
 * @param length the length (in codepoints) of the buffer.
 * @param options a bitwise or (`|`) of one or more of the following flags:
 * - @ref UTF8PROC_NLF2LS  - convert LF, CRLF, CR and NEL into LS
 * - @ref UTF8PROC_NLF2PS  - convert LF, CRLF, CR and NEL into PS
 * - @ref UTF8PROC_NLF2LF  - convert LF, CRLF, CR and NEL into LF
 * - @ref UTF8PROC_STRIPCC - strip or convert all non-affected control characters
 * - @ref UTF8PROC_COMPOSE - try to combine decomposed codepoints into composite
 *                           codepoints
 * - @ref UTF8PROC_STABLE  - prohibit combining characters that would violate
 *                           the unicode versioning stability
 *
 * @return
 * In case of success, the length (in codepoints) of the normalized UTF-32 string is
 * returned; otherwise, a negative error code is returned (nc_utf8proc_errmsg()).
 *
 * @warning The entries of the array pointed to by `str` have to be in the
 *          range `0x0000` to `0x10FFFF`. Otherwise, the program might crash!
 */
nc_utf8proc_ssize_t nc_utf8proc_normalize_utf32(nc_utf8proc_int32_t *buffer, nc_utf8proc_ssize_t length, nc_utf8proc_option_t options);

/**
 * Reencodes the sequence of `length` codepoints pointed to by `buffer`
 * UTF-8 data in-place (i.e., the result is also stored in `buffer`).
 * Can optionally normalize the UTF-32 sequence prior to UTF-8 conversion.
 *
 * @param buffer the (native-endian UTF-32) unicode codepoints to re-encode.
 * @param length the length (in codepoints) of the buffer.
 * @param options a bitwise or (`|`) of one or more of the following flags:
 * - @ref UTF8PROC_NLF2LS  - convert LF, CRLF, CR and NEL into LS
 * - @ref UTF8PROC_NLF2PS  - convert LF, CRLF, CR and NEL into PS
 * - @ref UTF8PROC_NLF2LF  - convert LF, CRLF, CR and NEL into LF
 * - @ref UTF8PROC_STRIPCC - strip or convert all non-affected control characters
 * - @ref UTF8PROC_COMPOSE - try to combine decomposed codepoints into composite
 *                           codepoints
 * - @ref UTF8PROC_STABLE  - prohibit combining characters that would violate
 *                           the unicode versioning stability
 * - @ref UTF8PROC_CHARBOUND - insert 0xFF bytes before each grapheme cluster
 *
 * @return
 * In case of success, the length (in bytes) of the resulting nul-terminated
 * UTF-8 string is returned; otherwise, a negative error code is returned
 * (nc_utf8proc_errmsg()).
 *
 * @warning The amount of free space pointed to by `buffer` must
 *          exceed the amount of the input data by one byte, and the
 *          entries of the array pointed to by `str` have to be in the
 *          range `0x0000` to `0x10FFFF`. Otherwise, the program might crash!
 */
nc_utf8proc_ssize_t nc_utf8proc_reencode(nc_utf8proc_int32_t *buffer, nc_utf8proc_ssize_t length, nc_utf8proc_option_t options);

/**
 * Given a pair of consecutive codepoints, return whether a grapheme break is
 * permitted between them (as defined by the extended grapheme clusters in UAX#29).
 *
 * @param codepoint1 The first codepoint.
 * @param codepoint2 The second codepoint, occurring consecutively after `codepoint1`.
 * @param state Beginning with Version 29 (Unicode 9.0.0), this algorithm requires
 *              state to break graphemes. This state can be passed in as a pointer
 *              in the `state` argument and should initially be set to 0. If the
 *              state is not passed in (i.e. a null pointer is passed), UAX#29 rules
 *              GB10/12/13 which require this state will not be applied, essentially
 *              matching the rules in Unicode 8.0.0.
 *
 * @warning If the state parameter is used, `nc_utf8proc_grapheme_break_stateful` must
 *          be called IN ORDER on ALL potential breaks in a string.  However, it
 *          is safe to reset the state to zero after a grapheme break.
 */
nc_utf8proc_bool nc_utf8proc_grapheme_break_stateful(
    nc_utf8proc_int32_t codepoint1, nc_utf8proc_int32_t codepoint2, nc_utf8proc_int32_t *state);

/**
 * Same as nc_utf8proc_grapheme_break_stateful(), except without support for the
 * Unicode 9 additions to the algorithm. Supported for legacy reasons.
 */
nc_utf8proc_bool nc_utf8proc_grapheme_break(
    nc_utf8proc_int32_t codepoint1, nc_utf8proc_int32_t codepoint2);


/**
 * Given a codepoint `c`, return the codepoint of the corresponding
 * lower-case character, if any; otherwise (if there is no lower-case
 * variant, or if `c` is not a valid codepoint) return `c`.
 */
nc_utf8proc_int32_t nc_utf8proc_tolower(nc_utf8proc_int32_t c);

/**
 * Given a codepoint `c`, return the codepoint of the corresponding
 * upper-case character, if any; otherwise (if there is no upper-case
 * variant, or if `c` is not a valid codepoint) return `c`.
 */
nc_utf8proc_int32_t nc_utf8proc_toupper(nc_utf8proc_int32_t c);

/**
 * Given a codepoint `c`, return the codepoint of the corresponding
 * title-case character, if any; otherwise (if there is no title-case
 * variant, or if `c` is not a valid codepoint) return `c`.
 */
nc_utf8proc_int32_t nc_utf8proc_totitle(nc_utf8proc_int32_t c);

/**
 * Given a codepoint `c`, return `1` if the codepoint corresponds to a lower-case character
 * and `0` otherwise.
 */
int nc_utf8proc_islower(nc_utf8proc_int32_t c);

/**
 * Given a codepoint `c`, return `1` if the codepoint corresponds to an upper-case character
 * and `0` otherwise.
 */
int nc_utf8proc_isupper(nc_utf8proc_int32_t c);

/**
 * Given a codepoint, return a character width analogous to `wcwidth(codepoint)`,
 * except that a width of 0 is returned for non-printable codepoints
 * instead of -1 as in `wcwidth`.
 *
 * @note
 * If you want to check for particular types of non-printable characters,
 * (analogous to `isprint` or `iscntrl`), use nc_utf8proc_category(). */
int nc_utf8proc_charwidth(nc_utf8proc_int32_t codepoint);

/**
 * Return the Unicode category for the codepoint (one of the
 * @ref nc_utf8proc_category_t constants.)
 */
nc_utf8proc_category_t nc_utf8proc_category(nc_utf8proc_int32_t codepoint);

/**
 * Return the two-letter (nul-terminated) Unicode category string for
 * the codepoint (e.g. `"Lu"` or `"Co"`).
 */
const char *nc_utf8proc_category_string(nc_utf8proc_int32_t codepoint);

/**
 * Maps the given UTF-8 string pointed to by `str` to a new UTF-8
 * string, allocated dynamically by `malloc` and returned via `dstptr`.
 *
 * If the @ref UTF8PROC_NULLTERM flag in the `options` field is set,
 * the length is determined by a NULL terminator, otherwise the
 * parameter `strlen` is evaluated to determine the string length, but
 * in any case the result will be NULL terminated (though it might
 * contain NULL characters with the string if `str` contained NULL
 * characters). Other flags in the `options` field are passed to the
 * functions defined above, and regarded as described.  See also
 * nc_utf8proc_map_custom() to supply a custom codepoint transformation.
 *
 * In case of success the length of the new string is returned,
 * otherwise a negative error code is returned.
 *
 * @note The memory of the new UTF-8 string will have been allocated
 * with `malloc`, and should therefore be deallocated with `free`.
 */
nc_utf8proc_ssize_t nc_utf8proc_map(
  const nc_utf8proc_uint8_t *str, nc_utf8proc_ssize_t strlen, nc_utf8proc_uint8_t **dstptr, nc_utf8proc_option_t options
);

/**
 * Like nc_utf8proc_map(), but also takes a `custom_func` mapping function
 * that is called on each codepoint in `str` before any other transformations
 * (along with a `custom_data` pointer that is passed through to `custom_func`).
 * The `custom_func` argument is ignored if it is `NULL`.
 */
nc_utf8proc_ssize_t nc_utf8proc_map_custom(
  const nc_utf8proc_uint8_t *str, nc_utf8proc_ssize_t strlen, nc_utf8proc_uint8_t **dstptr, nc_utf8proc_option_t options,
  nc_utf8proc_custom_func custom_func, void *custom_data
);

/** @name Unicode normalization
 *
 * Returns a pointer to newly allocated memory of a NFD, NFC, NFKD, NFKC or
 * NFKC_Casefold normalized version of the null-terminated string `str`.  These
 * are shortcuts to calling nc_utf8proc_map() with @ref UTF8PROC_NULLTERM
 * combined with @ref UTF8PROC_STABLE and flags indicating the normalization.
 */
/** @{ */
/** NFD normalization (@ref UTF8PROC_DECOMPOSE). */
nc_utf8proc_uint8_t *nc_utf8proc_NFD(const nc_utf8proc_uint8_t *str);
/** NFC normalization (@ref UTF8PROC_COMPOSE). */
nc_utf8proc_uint8_t *nc_utf8proc_NFC(const nc_utf8proc_uint8_t *str);
/** NFKD normalization (@ref UTF8PROC_DECOMPOSE and @ref UTF8PROC_COMPAT). */
nc_utf8proc_uint8_t *nc_utf8proc_NFKD(const nc_utf8proc_uint8_t *str);
/** NFKC normalization (@ref UTF8PROC_COMPOSE and @ref UTF8PROC_COMPAT). */
nc_utf8proc_uint8_t *nc_utf8proc_NFKC(const nc_utf8proc_uint8_t *str);
/**
 * NFKC_Casefold normalization (@ref UTF8PROC_COMPOSE and @ref UTF8PROC_COMPAT
 * and @ref UTF8PROC_CASEFOLD and @ref UTF8PROC_IGNORE).
 **/
nc_utf8proc_uint8_t *nc_utf8proc_NFKC_Casefold(const nc_utf8proc_uint8_t *str);
/** @} */

#ifdef __cplusplus
}
#endif

#endif
