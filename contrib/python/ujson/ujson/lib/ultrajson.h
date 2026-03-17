/*
Developed by ESN, an Electronic Arts Inc. studio.
Copyright (c) 2014, Electronic Arts Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
* Neither the name of ESN, Electronic Arts Inc. nor the
names of its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL ELECTRONIC ARTS INC. BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


Portions of code from MODP_ASCII - Ascii transformations (upper/lower, etc)
https://github.com/client9/stringencoders
Copyright (c) 2007  Nick Galbreath -- nickg [at] modp [dot] com. All rights reserved.

Numeric decoder derived from from TCL library
https://opensource.apple.com/source/tcl/tcl-14/tcl/license.terms
 * Copyright (c) 1988-1993 The Regents of the University of California.
 * Copyright (c) 1994 Sun Microsystems, Inc.
*/

/*
Ultra fast JSON encoder and decoder
Developed by Jonas Tarnstrom (jonas@esn.me).

Encoder notes:
------------------

:: Cyclic references ::
Cyclic referenced objects are not detected.
Set JSONObjectEncoder.recursionMax to suitable value or make sure input object
tree doesn't have cyclic references.

*/

#ifndef __ULTRAJSON_H__
#define __ULTRAJSON_H__

#include <stdio.h>

// Max decimals to encode double floating point numbers with
#ifndef JSON_DOUBLE_MAX_DECIMALS
    #define JSON_DOUBLE_MAX_DECIMALS 15
#endif

// Max recursion depth, default for encoder
#ifndef JSON_MAX_RECURSION_DEPTH
    #define JSON_MAX_RECURSION_DEPTH 1024
#endif

// Max recursion depth, default for decoder
#ifndef JSON_MAX_OBJECT_DEPTH
    #define JSON_MAX_OBJECT_DEPTH 1024
#endif

/*
Dictates and limits how much stack space for buffers UltraJSON will use before resorting to provided heap functions */
#ifndef JSON_MAX_STACK_BUFFER_SIZE
    #define JSON_MAX_STACK_BUFFER_SIZE 1024
#endif

#ifdef _WIN32

    typedef __int64 JSINT64;
    typedef unsigned __int64 JSUINT64;

    typedef __int32 JSINT32;
    typedef unsigned __int32 JSUINT32;
    typedef unsigned __int8 JSUINT8;
    typedef unsigned __int16 JSUTF16;
    typedef unsigned __int32 JSUTF32;
    typedef __int64 JSLONG;

    #define EXPORTFUNCTION __declspec(dllexport)

    #define FASTCALL_MSVC __fastcall
    #define FASTCALL_ATTR
    #define INLINE_PREFIX __inline

#else

    #include <stdint.h>
    typedef int64_t JSINT64;
    typedef uint64_t JSUINT64;

    typedef int32_t JSINT32;
    typedef uint32_t JSUINT32;

    #define FASTCALL_MSVC

    #if !defined __x86_64__
        #define FASTCALL_ATTR __attribute__((fastcall))
    #else
        #define FASTCALL_ATTR
    #endif

    #define INLINE_PREFIX inline

    typedef uint8_t JSUINT8;
    typedef uint16_t JSUTF16;
    typedef uint32_t JSUTF32;

    typedef int64_t JSLONG;

    #define EXPORTFUNCTION
#endif

#ifdef __GNUC__
    #define LIKELY(x)       __builtin_expect(!!(x), 1)
    #define UNLIKELY(x)     __builtin_expect(!!(x), 0)
#else
    #define LIKELY(x)       (x)
    #define UNLIKELY(x)     (x)
#endif

#if !(defined(__LITTLE_ENDIAN__) || defined(__BIG_ENDIAN__))

    #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        #define __LITTLE_ENDIAN__
    #else

    #if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        #define __BIG_ENDIAN__
    #endif

#endif

#endif

#if !defined(__LITTLE_ENDIAN__) && !defined(__BIG_ENDIAN__)
    #error "Endianness not supported"
#endif

enum JSTYPES
{
  JT_NULL,      // NULL
  JT_TRUE,      // boolean true
  JT_FALSE,     // boolean false
  JT_INT,       // (JSINT32 (signed 32-bit))
  JT_LONG,      // (JSINT64 (signed 64-bit))
  JT_ULONG,     // (JSUINT64 (unsigned 64-bit))
  JT_DOUBLE,    // (double)
  JT_UTF8,      // (char 8-bit)
  JT_RAW,       // (raw char 8-bit)
  JT_ARRAY,     // Array structure
  JT_OBJECT,    // Key/Value structure
  JT_INVALID,   // Internal, do not return nor expect
  JT_NAN,       // Not A Number
  JT_POS_INF,   // Positive infinity
  JT_NEG_INF,   // Negative infinity
};

typedef void * JSOBJ;
typedef void * JSITER;

typedef struct __JSONTypeContext
{
  int type;
  void *prv;
  void *encoder_prv;
} JSONTypeContext;

/*
Function pointer declarations, suitable for implementing UltraJSON */
typedef int (*JSPFN_ITERNEXT)(JSOBJ obj, JSONTypeContext *tc);
typedef void (*JSPFN_ITEREND)(JSOBJ obj, JSONTypeContext *tc);
typedef JSOBJ (*JSPFN_ITERGETVALUE)(JSOBJ obj, JSONTypeContext *tc);
typedef char *(*JSPFN_ITERGETNAME)(JSOBJ obj, JSONTypeContext *tc, size_t *outLen);
typedef void *(*JSPFN_MALLOC)(size_t size);
typedef void (*JSPFN_FREE)(void *pptr);
typedef void *(*JSPFN_REALLOC)(void *base, size_t size);


struct __JSONObjectEncoder;

typedef struct __JSONObjectEncoder
{
  void (*beginTypeContext)(JSOBJ obj, JSONTypeContext *tc, struct __JSONObjectEncoder *enc);
  void (*endTypeContext)(JSOBJ obj, JSONTypeContext *tc);
  const char *(*getStringValue)(JSOBJ obj, JSONTypeContext *tc, size_t *_outLen);
  JSINT64 (*getLongValue)(JSOBJ obj, JSONTypeContext *tc);
  JSUINT64 (*getUnsignedLongValue)(JSOBJ obj, JSONTypeContext *tc);
  double (*getDoubleValue)(JSOBJ obj, JSONTypeContext *tc);

  /*
  Retrieve next object in an iteration. Should return 0 to indicate iteration has reached end or 1 if there are more items.
  Implementer is responsible for keeping state of the iteration. Use ti->prv fields for this
  */
  JSPFN_ITERNEXT iterNext;

  /*
  Ends the iteration of an iterable object.
  Any iteration state stored in ti->prv can be freed here
  */
  JSPFN_ITEREND iterEnd;

  /*
  Returns a reference to the value object of an iterator
  The is responsible for the life-cycle of the returned string. Use iterNext/iterEnd and ti->prv to keep track of current object
  */
  JSPFN_ITERGETVALUE iterGetValue;

  /*
  Return name of iterator.
  The is responsible for the life-cycle of the returned string. Use iterNext/iterEnd and ti->prv to keep track of current object
  */
  JSPFN_ITERGETNAME iterGetName;

  /*
  Release a value as indicated by setting ti->release = 1 in the previous getValue call.
  The ti->prv array should contain the necessary context to release the value
  */
  void (*releaseObject)(JSOBJ obj);

  /* Library functions
  Set to NULL to use STDLIB malloc,realloc,free */
  JSPFN_MALLOC malloc;
  JSPFN_REALLOC realloc;
  JSPFN_FREE free;

  /*
  Configuration for max recursion, set to 0 to use default (see JSON_MAX_RECURSION_DEPTH)*/
  int recursionMax;

  /*
  If true output will be ASCII with all characters above 127 encoded as \uXXXX. If false output will be UTF-8 or what ever charset strings are brought as */
  int forceASCII;

  /*
  If true, '<', '>', and '&' characters will be encoded as \u003c, \u003e, and \u0026, respectively. If false, no special encoding will be used. */
  int encodeHTMLChars;

  /*
  If true, '/' will be encoded as \/. If false, no escaping. */
  int escapeForwardSlashes;

  /*
  If true, dictionaries are iterated through in sorted key order. */
  int sortKeys;

  /*
  Configuration for spaces of indent */
  int indent;

  /*
  If true, NaN will be encoded as a string matching the Python standard library's JSON behavior.
  This is not valid JSON. */
  int allowNan;

  /*
  If true, bytes are rejected. */
  int rejectBytes;

  /*
  Configuration for item and key separators, e.g. "," and ":" for a compact representation or ", " and ": " to match the Python standard library's defaults. */
  size_t itemSeparatorLength;
  const char *itemSeparatorChars;
  size_t keySeparatorLength;
  const char *keySeparatorChars;

  /*
  Private pointer to be used by the caller. Passed as encoder_prv in JSONTypeContext */
  void *prv;

  /*
  Pointer to the DoubleToStringConverter instance */
  void *d2s;

  /*
  Set to an error message if error occurred */
  const char *errorMsg;
  JSOBJ errorObj;

  /* Buffer stuff */
  char *start;
  char *offset;
  char *end;
  int heap;
  int level;

} JSONObjectEncoder;


/*
Encode an object structure into JSON.

Arguments:
obj - An anonymous type representing the object
enc - Function definitions for querying JSOBJ type
buffer - Preallocated buffer to store result in. If NULL function allocates own buffer
cbBuffer - Length of buffer (ignored if buffer is NULL)
outLen - Will store the length of the encoded string

Returns:
Encoded JSON object as a char string.

NOTE:
If the supplied buffer wasn't enough to hold the result the function will allocate a new buffer.
Life cycle of the provided buffer must still be handled by caller.

If the return value doesn't equal the specified buffer caller must release the memory using
JSONObjectEncoder.free or free() as specified when calling this function.

If an error occurs during encoding, NULL is returned and no outLen is stored.
*/
EXPORTFUNCTION char *JSON_EncodeObject(JSOBJ obj, JSONObjectEncoder *enc, char *buffer, size_t cbBuffer, size_t *outLen);

typedef struct __JSONObjectDecoder
{
  JSOBJ (*newString)(void *prv, JSUINT32 *start, JSUINT32 *end);
  void (*objectAddKey)(void *prv, JSOBJ obj, JSOBJ name, JSOBJ value);
  void (*arrayAddItem)(void *prv, JSOBJ obj, JSOBJ value);
  JSOBJ (*newTrue)(void *prv);
  JSOBJ (*newFalse)(void *prv);
  JSOBJ (*newNull)(void *prv);
  JSOBJ (*newNaN)(void *prv);
  JSOBJ (*newPosInf)(void *prv);
  JSOBJ (*newNegInf)(void *prv);
  JSOBJ (*newObject)(void *prv);
  JSOBJ (*newArray)(void *prv);
  JSOBJ (*newInt)(void *prv, JSINT32 value);
  JSOBJ (*newLong)(void *prv, JSINT64 value);
  JSOBJ (*newUnsignedLong)(void *prv, JSUINT64 value);
  JSOBJ (*newIntegerFromString)(void *prv, char *value, size_t length);
  JSOBJ (*newDouble)(void *prv, double value);
  void (*releaseObject)(void *prv, JSOBJ obj);
  JSPFN_MALLOC malloc;
  JSPFN_FREE free;
  JSPFN_REALLOC realloc;
  char *errorStr;
  char *errorOffset;
  void *prv;
  void *s2d;
} JSONObjectDecoder;

EXPORTFUNCTION JSOBJ JSON_DecodeObject(JSONObjectDecoder *dec, const char *buffer, size_t cbBuffer);

#define DCONV_DECIMAL_IN_SHORTEST_LOW -4
#define DCONV_DECIMAL_IN_SHORTEST_HIGH 16

enum dconv_d2s_flags {
  DCONV_D2S_NO_FLAGS = 0,
  DCONV_D2S_EMIT_POSITIVE_EXPONENT_SIGN = 1,
  DCONV_D2S_EMIT_TRAILING_DECIMAL_POINT = 2,
  DCONV_D2S_EMIT_TRAILING_ZERO_AFTER_POINT = 4,
  DCONV_D2S_UNIQUE_ZERO = 8
};

enum dconv_s2d_flags
{
  DCONV_S2D_NO_FLAGS = 0,
  DCONV_S2D_ALLOW_HEX = 1,
  DCONV_S2D_ALLOW_OCTALS = 2,
  DCONV_S2D_ALLOW_TRAILING_JUNK = 4,
  DCONV_S2D_ALLOW_LEADING_SPACES = 8,
  DCONV_S2D_ALLOW_TRAILING_SPACES = 16,
  DCONV_S2D_ALLOW_SPACES_AFTER_SIGN = 32
};

void dconv_d2s_init(void **d2s,
                    int flags,
                    const char* infinity_symbol,
                    const char* nan_symbol,
                    char exponent_character,
                    int decimal_in_shortest_low,
                    int decimal_in_shortest_high,
                    int max_leading_padding_zeroes_in_precision_mode,
                    int max_trailing_padding_zeroes_in_precision_mode);
int dconv_d2s(void *d2s, double value, char* buf, int buflen, int* strlength);
void dconv_d2s_free(void **d2s);

void dconv_s2d_init(void **s2d, int flags, double empty_string_value,
                    double junk_string_value, const char* infinity_symbol,
                    const char* nan_symbol);
double dconv_s2d(void *s2d, const char* buffer, int length, int* processed_characters_count);
void dconv_s2d_free(void **s2d);

#endif
