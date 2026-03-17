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

#include "ultrajson.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <math.h>

#include <float.h>

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#if ( (defined(_WIN32) || defined(WIN32) ) && ( defined(_MSC_VER) ) )
#define snprintf sprintf_s
#endif

/*
Worst cases being:

Control characters (ASCII < 32)
0x00 (1 byte) input => \u0000 output (6 bytes)
1 * 6 => 6 (6 bytes required)

or UTF-16 surrogate pairs
4 bytes input in UTF-8 => \uXXXX\uYYYY (12 bytes).

4 * 6 => 24 bytes (12 bytes required)

The extra 2 bytes are for the quotes around the string

*/
#define RESERVE_STRING(_len) (2 + ((_len) * 6))

static const char g_hexChars[] = "0123456789abcdef";
static const char g_escapeChars[] = "0123456789\\b\\t\\n\\f\\r\\\"\\\\\\/";

/*
FIXME: While this is fine dandy and working it's a magic value mess which probably only the author understands.
Needs a cleanup and more documentation */

/*
Table for pure ascii output escaping all characters above 127 to \uXXXX */
static const JSUINT8 g_asciiOutputTable[256] =
{
/* 0x00 */ 0, 30, 30, 30, 30, 30, 30, 30, 10, 12, 14, 30, 16, 18, 30, 30,
/* 0x10 */ 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30,
/* 0x20 */ 1, 1, 20, 1, 1, 1, 29, 1, 1, 1, 1, 1, 1, 1, 1, 24,
/* 0x30 */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 29, 1, 29, 1,
/* 0x40 */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
/* 0x50 */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 22, 1, 1, 1,
/* 0x60 */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
/* 0x70 */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
/* 0x80 */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
/* 0x90 */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
/* 0xa0 */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
/* 0xb0 */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
/* 0xc0 */ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
/* 0xd0 */ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
/* 0xe0 */ 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
/* 0xf0 */ 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 1, 1
};

static void SetError (JSOBJ obj, JSONObjectEncoder *enc, const char *message)
{
  enc->errorMsg = message;
  enc->errorObj = obj;
}

/*
FIXME: Keep track of how big these get across several encoder calls and try to make an estimate
That way we won't run our head into the wall each call */
static void Buffer_Realloc (JSONObjectEncoder *enc, size_t cbNeeded)
{
  size_t free_space = enc->end - enc->offset;
  if (free_space >= cbNeeded)
  {
    return;
  }
  size_t curSize = enc->end - enc->start;
  size_t newSize = curSize;
  size_t offset = enc->offset - enc->start;

#ifdef DEBUG
  // In debug mode, allocate only what is requested so that any miscalculation
  // shows up plainly as a crash.
  newSize = (enc->offset - enc->start) + cbNeeded;
#else
  while (newSize < curSize + cbNeeded)
  {
    newSize *= 2;
  }
#endif

  if (enc->heap)
  {
    enc->start = (char *) enc->realloc (enc->start, newSize);
    if (!enc->start)
    {
      SetError (NULL, enc, "Could not reserve memory block");
      return;
    }
  }
  else
  {
    char *oldStart = enc->start;
    enc->heap = 1;
    enc->start = (char *) enc->malloc (newSize);
    if (!enc->start)
    {
      SetError (NULL, enc, "Could not reserve memory block");
      return;
    }
    memcpy (enc->start, oldStart, offset);
  }
  enc->offset = enc->start + offset;
  enc->end = enc->start + newSize;
}

#define Buffer_Reserve(__enc, __len) \
    if ( (size_t) ((__enc)->end - (__enc)->offset) < (size_t) (__len))  \
    {   \
      Buffer_Realloc((__enc), (__len));\
    }   \

static void *Buffer_memcpy (JSONObjectEncoder *enc, const void *src, size_t n)
{
  void *out;
#ifdef DEBUG
  if ((size_t) (enc->end - enc->offset) < n) {
    fprintf(stderr, "Ran out of buffer space during Buffer_memcpy()\n");
    abort();
  }
#endif
  out = memcpy(enc->offset, src, n);
  enc->offset += n;
  return out;
}

static FASTCALL_ATTR INLINE_PREFIX void FASTCALL_MSVC Buffer_AppendShortHexUnchecked (char *outputOffset, unsigned short value)
{
  *(outputOffset++) = g_hexChars[(value & 0xf000) >> 12];
  *(outputOffset++) = g_hexChars[(value & 0x0f00) >> 8];
  *(outputOffset++) = g_hexChars[(value & 0x00f0) >> 4];
  *(outputOffset++) = g_hexChars[(value & 0x000f) >> 0];
}

static int Buffer_EscapeStringUnvalidated (JSONObjectEncoder *enc, const char *io, const char *end)
{
  char *of = (char *) enc->offset;

  for (;;)
  {
#ifdef DEBUG
    // 6 is the maximum length of a single character (cf. RESERVE_STRING).
    if ((io < end) && (enc->end - of < 6)) {
      fprintf(stderr, "Ran out of buffer space during Buffer_EscapeStringUnvalidated()\n");
      abort();
    }
#endif
    switch (*io)
    {
      case 0x00:
      {
        if (io < end)
        {
          *(of++) = '\\';
          *(of++) = 'u';
          *(of++) = '0';
          *(of++) = '0';
          *(of++) = '0';
          *(of++) = '0';
          break;
        }
        else
        {
          enc->offset += (of - enc->offset);
          return TRUE;
        }
      }
      case '\"': (*of++) = '\\'; (*of++) = '\"'; break;
      case '\\': (*of++) = '\\'; (*of++) = '\\'; break;
      case '\b': (*of++) = '\\'; (*of++) = 'b'; break;
      case '\f': (*of++) = '\\'; (*of++) = 'f'; break;
      case '\n': (*of++) = '\\'; (*of++) = 'n'; break;
      case '\r': (*of++) = '\\'; (*of++) = 'r'; break;
      case '\t': (*of++) = '\\'; (*of++) = 't'; break;

      case '/':
      {
        if (enc->escapeForwardSlashes)
        {
          (*of++) = '\\';
          (*of++) = '/';
        }
        else
        {
          // Same as default case below.
          (*of++) = (*io);
        }
        break;
      }
      case 0x26: // '&'
      case 0x3c: // '<'
      case 0x3e: // '>'
      {
        if (enc->encodeHTMLChars)
        {
          // Fall through to \u00XX case below.
        }
        else
        {
          // Same as default case below.
          (*of++) = (*io);
          break;
        }
      }
      case 0x01:
      case 0x02:
      case 0x03:
      case 0x04:
      case 0x05:
      case 0x06:
      case 0x07:
      case 0x0b:
      case 0x0e:
      case 0x0f:
      case 0x10:
      case 0x11:
      case 0x12:
      case 0x13:
      case 0x14:
      case 0x15:
      case 0x16:
      case 0x17:
      case 0x18:
      case 0x19:
      case 0x1a:
      case 0x1b:
      case 0x1c:
      case 0x1d:
      case 0x1e:
      case 0x1f:
      {
        *(of++) = '\\';
        *(of++) = 'u';
        *(of++) = '0';
        *(of++) = '0';
        *(of++) = g_hexChars[ (unsigned char) (((*io) & 0xf0) >> 4)];
        *(of++) = g_hexChars[ (unsigned char) ((*io) & 0x0f)];
        break;
      }
      default: (*of++) = (*io); break;
    }
    io++;
  }
}

static int Buffer_EscapeStringValidated (JSOBJ obj, JSONObjectEncoder *enc, const char *io, const char *end)
{
  JSUTF32 ucs;
  char *of = (char *) enc->offset;

  for (;;)
  {
#ifdef DEBUG
    /*
    6 is the maximum length of a single character (cf. RESERVE_STRING).
    Note that the loop below may consume more than one input char and produce a UTF-16 surrogate pair.
    In that case, more than 6 characters would be needed on the output buffer.
    So this calculates the maximum length of the entire remaining input buffer instead. */
    if (enc->end - enc->offset < 6 * (end - io)) {
      fprintf(stderr, "Ran out of buffer space during Buffer_EscapeStringValidated()\n");
      abort();
    }
#endif
    JSUINT8 utflen = g_asciiOutputTable[(unsigned char) *io];

    switch (utflen)
    {
      case 0:
      {
        if (io < end)
        {
          *(of++) = '\\';
          *(of++) = 'u';
          *(of++) = '0';
          *(of++) = '0';
          *(of++) = '0';
          *(of++) = '0';
          io ++;
          continue;
        }
        else
        {
          enc->offset += (of - enc->offset);
          return TRUE;
        }
      }

      case 1:
      {
        *(of++)= (*io++);
        continue;
      }

      case 2:
      {
        JSUTF32 in;
        JSUTF16 in16;

        if (end - io < 1)
        {
          enc->offset += (of - enc->offset);
          SetError (obj, enc, "Unterminated UTF-8 sequence when encoding string");
          return FALSE;
        }

        memcpy(&in16, io, sizeof(JSUTF16));
        in = (JSUTF32) in16;

#ifdef __LITTLE_ENDIAN__
        ucs = ((in & 0x1f) << 6) | ((in >> 8) & 0x3f);
#else
        ucs = ((in & 0x1f00) >> 2) | (in & 0x3f);
#endif

        if (ucs < 0x80)
        {
          enc->offset += (of - enc->offset);
          SetError (obj, enc, "Overlong 2 byte UTF-8 sequence detected when encoding string");
          return FALSE;
        }

        io += 2;
        break;
      }

      case 3:
      {
        JSUTF32 in;
        JSUTF16 in16;
        JSUINT8 in8;

        if (end - io < 2)
        {
          enc->offset += (of - enc->offset);
          SetError (obj, enc, "Unterminated UTF-8 sequence when encoding string");
          return FALSE;
        }

        memcpy(&in16, io, sizeof(JSUTF16));
        memcpy(&in8, io + 2, sizeof(JSUINT8));
#ifdef __LITTLE_ENDIAN__
        in = (JSUTF32) in16;
        in |= in8 << 16;
        ucs = ((in & 0x0f) << 12) | ((in & 0x3f00) >> 2) | ((in & 0x3f0000) >> 16);
#else
        in = in16 << 8;
        in |= in8;
        ucs = ((in & 0x0f0000) >> 4) | ((in & 0x3f00) >> 2) | (in & 0x3f);
#endif

        if (ucs < 0x800)
        {
          enc->offset += (of - enc->offset);
          SetError (obj, enc, "Overlong 3 byte UTF-8 sequence detected when encoding string");
          return FALSE;
        }

        io += 3;
        break;
      }
      case 4:
      {
        JSUTF32 in;

        if (end - io < 3)
        {
          enc->offset += (of - enc->offset);
          SetError (obj, enc, "Unterminated UTF-8 sequence when encoding string");
          return FALSE;
        }

        memcpy(&in, io, sizeof(JSUTF32));
#ifdef __LITTLE_ENDIAN__
        ucs = ((in & 0x07) << 18) | ((in & 0x3f00) << 4) | ((in & 0x3f0000) >> 10) | ((in & 0x3f000000) >> 24);
#else
        ucs = ((in & 0x07000000) >> 6) | ((in & 0x3f0000) >> 4) | ((in & 0x3f00) >> 2) | (in & 0x3f);
#endif
        if (ucs < 0x10000)
        {
          enc->offset += (of - enc->offset);
          SetError (obj, enc, "Overlong 4 byte UTF-8 sequence detected when encoding string");
          return FALSE;
        }

        io += 4;
        break;
      }


      case 5:
      case 6:
      {
        enc->offset += (of - enc->offset);
        SetError (obj, enc, "Unsupported UTF-8 sequence length when encoding string");
        return FALSE;
      }

      case 29:
      {
        if (enc->encodeHTMLChars)
        {
          // Fall through to \u00XX case 30 below.
        }
        else
        {
          // Same as case 1 above.
          *(of++) = (*io++);
          continue;
        }
      }

      case 30:
      {
        // \uXXXX encode
        *(of++) = '\\';
        *(of++) = 'u';
        *(of++) = '0';
        *(of++) = '0';
        *(of++) = g_hexChars[ (unsigned char) (((*io) & 0xf0) >> 4)];
        *(of++) = g_hexChars[ (unsigned char) ((*io) & 0x0f)];
        io ++;
        continue;
      }
      case 10:
      case 12:
      case 14:
      case 16:
      case 18:
      case 20:
      case 22:
      {
        *(of++) = *( (char *) (g_escapeChars + utflen + 0));
        *(of++) = *( (char *) (g_escapeChars + utflen + 1));
        io ++;
        continue;
      }
      case 24:
      {
        if (enc->escapeForwardSlashes)
        {
          *(of++) = *( (char *) (g_escapeChars + utflen + 0));
          *(of++) = *( (char *) (g_escapeChars + utflen + 1));
          io ++;
        }
        else
        {
          // Same as case 1 above.
          *(of++) = (*io++);
        }
        continue;
      }
      // This can never happen, it's here to make L4 VC++ happy
      default:
      {
        ucs = 0;
        break;
      }
    }

    /*
    If the character is a UTF8 sequence of length > 1 we end up here */
    if (ucs >= 0x10000)
    {
      ucs -= 0x10000;
      *(of++) = '\\';
      *(of++) = 'u';
      Buffer_AppendShortHexUnchecked(of, (unsigned short) (ucs >> 10) + 0xd800);
      of += 4;

      *(of++) = '\\';
      *(of++) = 'u';
      Buffer_AppendShortHexUnchecked(of, (unsigned short) (ucs & 0x3ff) + 0xdc00);
      of += 4;
    }
    else
    {
      *(of++) = '\\';
      *(of++) = 'u';
      Buffer_AppendShortHexUnchecked(of, (unsigned short) ucs);
      of += 4;
    }
  }
}


static FASTCALL_ATTR INLINE_PREFIX void FASTCALL_MSVC Buffer_AppendCharUnchecked(JSONObjectEncoder *enc, char chr)
{
#ifdef DEBUG
  if (enc->end <= enc->offset)
  {
    fprintf(stderr, "Overflow writing byte %d '%c'. The last few characters were:\n'''", chr, chr);
    char * recent = enc->offset - 1000;
    if (enc->start > recent)
    {
      recent = enc->start;
    }
    for (; recent < enc->offset; recent++)
    {
      fprintf(stderr, "%c", *recent);
    }
    fprintf(stderr, "'''\n");
    abort();
  }
#endif
  *(enc->offset++) = chr;
}

static FASTCALL_ATTR INLINE_PREFIX void FASTCALL_MSVC strreverse(char* begin, char* end)
{
  char aux;
  while (end > begin)
  aux = *end, *end-- = *begin, *begin++ = aux;
}

static void Buffer_AppendIndentNewlineUnchecked(JSONObjectEncoder *enc)
{
  if (enc->indent > 0) Buffer_AppendCharUnchecked(enc, '\n');
}

static void Buffer_AppendIndentUnchecked(JSONObjectEncoder *enc, JSINT32 value)
{
  int i;
  if (enc->indent > 0)
    while (value-- > 0)
      for (i = 0; i < enc->indent; i++)
        Buffer_AppendCharUnchecked(enc, ' ');
}

static void Buffer_AppendLongUnchecked(JSONObjectEncoder *enc, JSINT64 value)
{
  char* wstr;
  JSUINT64 uvalue;

  if (value == INT64_MIN) {
    uvalue = INT64_MAX + UINT64_C(1);
  } else {
    uvalue = (value < 0) ? -value : value;
  }

  wstr = enc->offset;
#ifdef DEBUG
  // 20 is the maximum length of a JSINT64 (minus sign plus 19 digits)
  if (enc->end - enc->offset < 20) {
    fprintf(stderr, "Ran out of buffer space during Buffer_AppendLongUnchecked()\n");
    abort();
  }
#endif
  // Conversion. Number is reversed.

  do *wstr++ = (char)(48 + (uvalue % 10ULL)); while(uvalue /= 10ULL);
  if (value < 0) *wstr++ = '-';

  // Reverse string
  strreverse(enc->offset,wstr - 1);
  enc->offset += (wstr - (enc->offset));
}

static void Buffer_AppendUnsignedLongUnchecked(JSONObjectEncoder *enc, JSUINT64 value)
{
  char* wstr;
  JSUINT64 uvalue = value;

  wstr = enc->offset;
#ifdef DEBUG
  // 21 is the maximum length of a JSUINT64 (minus sign plus 20 digits)
  if (enc->end - enc->offset < 21) {
    fprintf(stderr, "Ran out of buffer space during Buffer_AppendUnsignedLongUnchecked()\n");
    abort();
  }
#endif
  // Conversion. Number is reversed.

  do *wstr++ = (char)(48 + (uvalue % 10ULL)); while(uvalue /= 10ULL);

  // Reverse string
  strreverse(enc->offset,wstr - 1);
  enc->offset += (wstr - (enc->offset));
}

static int Buffer_AppendDoubleDconv(JSOBJ obj, JSONObjectEncoder *enc, double value)
{
  char buf[128];
  int strlength;
#ifdef DEBUG
  if ((size_t) (enc->end - enc->offset) < sizeof(buf)) {
    fprintf(stderr, "Ran out of buffer space during Buffer_AppendDoubleDconv()\n");
    abort();
  }
#endif
  if(!dconv_d2s(enc->d2s, value, buf, sizeof(buf), &strlength))
  {
    SetError (obj, enc, "Invalid value when encoding double");
    return FALSE;
  }

  Buffer_memcpy(enc, buf, strlength);

  return TRUE;
}

/*
FIXME:
Handle integration functions returning NULL here */

/*
FIXME:
Perhaps implement recursion detection */

static void encode(JSOBJ obj, JSONObjectEncoder *enc, const char *name, size_t cbName)
{
  const char *value;
  char *objName;
  int count, res;
  JSOBJ iterObj;
  size_t szlen;
  JSONTypeContext tc;

  if (enc->level > enc->recursionMax)
  {
    SetError (obj, enc, "Maximum recursion level reached");
    return;
  }

  if (enc->errorMsg)
  {
    return;
  }

  if (name)
  {
    Buffer_Reserve(enc, RESERVE_STRING(cbName) + enc->keySeparatorLength);
    Buffer_AppendCharUnchecked(enc, '\"');

    if (enc->forceASCII)
    {
      if (!Buffer_EscapeStringValidated(obj, enc, name, name + cbName))
      {
        return;
      }
    }
    else
    {
      if (!Buffer_EscapeStringUnvalidated(enc, name, name + cbName))
      {
        return;
      }
    }

    Buffer_AppendCharUnchecked(enc, '\"');

    Buffer_memcpy(enc, enc->keySeparatorChars, enc->keySeparatorLength);
  }

  tc.encoder_prv = enc->prv;
  enc->beginTypeContext(obj, &tc, enc);

  /*
  This reservation covers any additions on non-variable parts below, specifically:
  - Opening brackets for JT_ARRAY and JT_OBJECT
  - Number representation for JT_LONG, JT_ULONG, JT_INT, and JT_DOUBLE
  - Constant value for JT_TRUE, JT_FALSE, JT_NULL

  The length of 128 is the worst case length of the Buffer_AppendDoubleDconv addition.
  The other types above all have smaller representations.
  */
  Buffer_Reserve (enc, 128);

  switch (tc.type)
  {
    case JT_INVALID:
    {
      /*
      There should already be an exception at the Python level.
      This however sets the errorMsg so recursion on arrays and objects stops.
      endTypeContext must not be called here as beginTypeContext already cleans up in the INVALID case.
      */
      SetError (obj, enc, "Invalid type");
      enc->level--;
      return;
    }

    case JT_ARRAY:
    {
      count = 0;

      Buffer_AppendCharUnchecked (enc, '[');

      while (enc->iterNext(obj, &tc))
      {
        // The extra 1 byte covers the optional newline.
        Buffer_Reserve (enc, enc->indent * (enc->level + 1) + enc->itemSeparatorLength + 1);

        if (count > 0)
        {
          Buffer_memcpy(enc, enc->itemSeparatorChars, enc->itemSeparatorLength);
        }
        Buffer_AppendIndentNewlineUnchecked (enc);

        iterObj = enc->iterGetValue(obj, &tc);

        enc->level ++;
        Buffer_AppendIndentUnchecked (enc, enc->level);
        encode (iterObj, enc, NULL, 0);
        if (enc->errorMsg)
        {
          enc->iterEnd(obj, &tc);
          enc->endTypeContext(obj, &tc);
          enc->level--;
          return;
        }
        count ++;
      }

      enc->iterEnd(obj, &tc);

      if (count > 0) {
        // Reserve space for the indentation plus the newline.
        Buffer_Reserve (enc, enc->indent * enc->level + 1);
        Buffer_AppendIndentNewlineUnchecked (enc);
        Buffer_AppendIndentUnchecked (enc, enc->level);
      }
      Buffer_Reserve (enc, 1);
      Buffer_AppendCharUnchecked (enc, ']');
      break;
    }

    case JT_OBJECT:
    {
      count = 0;

      Buffer_AppendCharUnchecked (enc, '{');

      while ((res = enc->iterNext(obj, &tc)))
      {
        // The extra 1 byte covers the optional newline.
        Buffer_Reserve (enc, enc->indent * (enc->level + 1) + enc->itemSeparatorLength + 1);

        if(res < 0)
        {
          enc->iterEnd(obj, &tc);
          enc->endTypeContext(obj, &tc);
          enc->level--;
          return;
        }

        if (count > 0)
        {
          Buffer_memcpy(enc, enc->itemSeparatorChars, enc->itemSeparatorLength);
        }
        Buffer_AppendIndentNewlineUnchecked (enc);

        iterObj = enc->iterGetValue(obj, &tc);
        objName = enc->iterGetName(obj, &tc, &szlen);

        enc->level ++;
        Buffer_AppendIndentUnchecked (enc, enc->level);
        encode (iterObj, enc, objName, szlen);
        if (enc->errorMsg)
        {
          enc->iterEnd(obj, &tc);
          enc->endTypeContext(obj, &tc);
          enc->level--;
          return;
        }
        count ++;
      }

      enc->iterEnd(obj, &tc);

      if (count > 0) {
        Buffer_Reserve (enc, enc->indent * enc->level + 1);
        Buffer_AppendIndentNewlineUnchecked (enc);
        Buffer_AppendIndentUnchecked (enc, enc->level);
      }
      Buffer_Reserve (enc, 1);
      Buffer_AppendCharUnchecked (enc, '}');
      break;
    }

    case JT_LONG:
    {
      Buffer_AppendLongUnchecked (enc, enc->getLongValue(obj, &tc));
      break;
    }

    case JT_ULONG:
    {
      Buffer_AppendUnsignedLongUnchecked (enc, enc->getUnsignedLongValue(obj, &tc));
      break;
    }

    case JT_TRUE:
    {
      Buffer_AppendCharUnchecked (enc, 't');
      Buffer_AppendCharUnchecked (enc, 'r');
      Buffer_AppendCharUnchecked (enc, 'u');
      Buffer_AppendCharUnchecked (enc, 'e');
      break;
    }

    case JT_FALSE:
    {
      Buffer_AppendCharUnchecked (enc, 'f');
      Buffer_AppendCharUnchecked (enc, 'a');
      Buffer_AppendCharUnchecked (enc, 'l');
      Buffer_AppendCharUnchecked (enc, 's');
      Buffer_AppendCharUnchecked (enc, 'e');
      break;
    }

    case JT_NULL:
    {
      Buffer_AppendCharUnchecked (enc, 'n');
      Buffer_AppendCharUnchecked (enc, 'u');
      Buffer_AppendCharUnchecked (enc, 'l');
      Buffer_AppendCharUnchecked (enc, 'l');
      break;
    }

    case JT_DOUBLE:
    {
      if (!Buffer_AppendDoubleDconv(obj, enc, enc->getDoubleValue(obj, &tc)))
      {
        enc->endTypeContext(obj, &tc);
        enc->level--;
        return;
      }
      break;
    }

    case JT_UTF8:
    {
      value = enc->getStringValue(obj, &tc, &szlen);
      if(!value)
      {
        SetError(obj, enc, "utf-8 encoding error");
        return;
      }

      Buffer_Reserve(enc, RESERVE_STRING(szlen));
      if (enc->errorMsg)
      {
        enc->endTypeContext(obj, &tc);
        return;
      }
      Buffer_AppendCharUnchecked (enc, '\"');

      if (enc->forceASCII)
      {
        if (!Buffer_EscapeStringValidated(obj, enc, value, value + szlen))
        {
          enc->endTypeContext(obj, &tc);
          enc->level--;
          return;
        }
      }
      else
      {
        if (!Buffer_EscapeStringUnvalidated(enc, value, value + szlen))
        {
          enc->endTypeContext(obj, &tc);
          enc->level--;
          return;
        }
      }

      Buffer_AppendCharUnchecked (enc, '\"');
      break;
    }

    case JT_RAW:
    {
      value = enc->getStringValue(obj, &tc, &szlen);
      if(!value)
      {
        SetError(obj, enc, "utf-8 encoding error");
        return;
      }

      Buffer_Reserve(enc, szlen);
      if (enc->errorMsg)
      {
        enc->endTypeContext(obj, &tc);
        return;
      }

      Buffer_memcpy(enc, value, szlen);

      break;
    }
  }

  enc->endTypeContext(obj, &tc);
  enc->level--;
}

char *JSON_EncodeObject(JSOBJ obj, JSONObjectEncoder *enc, char *_buffer, size_t _cbBuffer, size_t *_outLen)
{
  enc->malloc = enc->malloc ? enc->malloc : malloc;
  enc->free =  enc->free ? enc->free : free;
  enc->realloc = enc->realloc ? enc->realloc : realloc;
  enc->errorMsg = NULL;
  enc->errorObj = NULL;
  enc->level = 0;

  if (enc->recursionMax < 1)
  {
    enc->recursionMax = JSON_MAX_RECURSION_DEPTH;
  }

  if (_buffer == NULL)
  {
    _cbBuffer = 32768;
    enc->start = (char *) enc->malloc (_cbBuffer);
    if (!enc->start)
    {
      SetError(obj, enc, "Could not reserve memory block");
      return NULL;
    }
    enc->heap = 1;
  }
  else
  {
    enc->start = _buffer;
    enc->heap = 0;
  }

  enc->end = enc->start + _cbBuffer;
  enc->offset = enc->start;

  encode (obj, enc, NULL, 0);

  if (enc->errorMsg)
  {
    if (enc->heap == 1)
    {
      // Buffer was realloc'd at some point, or no initial buffer was provided.
      enc->free(enc->start);
    }
    return NULL;
  }

  *_outLen = enc->offset - enc->start;
  return enc->start;
}
