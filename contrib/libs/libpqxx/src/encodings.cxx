/** Implementation of string encodings support
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include "pqxx/except.hxx"
#include "pqxx/internal/encodings.hxx"

#include <cstring>
#include <iomanip>
#include <map>
#include <sstream>

using namespace pqxx::internal;

extern "C"
{
#include "libpq-fe.h"
}


// Internal helper functions
namespace
{
/// Extract byte from buffer, return as unsigned char.
unsigned char get_byte(const char buffer[], std::string::size_type offset)
{
  return static_cast<unsigned char>(buffer[offset]);
}


[[noreturn]] void throw_for_encoding_error(
  const char* encoding_name,
  const char buffer[],
  std::string::size_type start,
  std::string::size_type count
)
{
  std::stringstream s;
  s
    << "Invalid byte sequence for encoding "
    << encoding_name
    << " at byte "
    << start
    << ": "
    << std::hex
    << std::setw(2)
    << std::setfill('0')
  ;
  for (std::string::size_type i{0}; i < count; ++i)
  {
    s << "0x" << static_cast<unsigned int>(get_byte(buffer, start + i));
    if (i + 1 < count) s << " ";
  }
  throw pqxx::argument_error{s.str()};
}


/// Does value lie between bottom and top, inclusive?
constexpr bool between_inc(unsigned char value, unsigned bottom, unsigned top)
{
  return value >= bottom and value <= top;
}


/*
EUC-JP and EUC-JIS-2004 represent slightly different code points but iterate
the same:
 * https://en.wikipedia.org/wiki/Extended_Unix_Code#EUC-JP
 * http://x0213.org/codetable/index.en.html
*/
std::string::size_type next_seq_for_euc_jplike(
	const char buffer[],
	std::string::size_type buffer_len,
	std::string::size_type start,
	const char encoding_name[])
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (start + 2 > buffer_len)
    throw_for_encoding_error(encoding_name, buffer, start, 1);

  const auto byte2 = get_byte(buffer, start + 1);
  if (byte1 == 0x8e)
  {
    if (not between_inc(byte2, 0xa1, 0xfe))
      throw_for_encoding_error(encoding_name, buffer, start, 2);

    return start + 2;
  }

  if (between_inc(byte1, 0xa1, 0xfe))
  {
    if (not between_inc(byte2, 0xa1, 0xfe))
      throw_for_encoding_error(encoding_name, buffer, start, 2);

    return start + 2;
  }

  if (byte1 == 0x8f and start + 3 <= buffer_len)
  {
    const auto byte3 = get_byte(buffer, start + 2);
    if (
	not between_inc(byte2, 0xa1, 0xfe) or
        not between_inc(byte3, 0xa1, 0xfe)
      )
      throw_for_encoding_error(encoding_name, buffer, start, 3);

    return start + 3;
  }

  throw_for_encoding_error(encoding_name, buffer, start, 1);
}

/*
As far as I can tell, for the purposes of iterating the only difference between
SJIS and SJIS-2004 is increased range in the first byte of two-byte sequences
(0xEF increased to 0xFC).  Officially, that is; apparently the version of SJIS
used by Postgres has the same range as SJIS-2004.  They both have increased
range over the documented versions, not having the even/odd restriction for the
first byte in 2-byte sequences.
*/
// https://en.wikipedia.org/wiki/Shift_JIS#Shift_JIS_byte_map
// http://x0213.org/codetable/index.en.html
std::string::size_type next_seq_for_sjislike(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start,
  const char* encoding_name
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80 or between_inc(byte1, 0xa1, 0xdf)) return start + 1;

  if (
	not between_inc(byte1, 0x81, 0x9f) and
	not between_inc(byte1, 0xe0, 0xfc)
  )
    throw_for_encoding_error(encoding_name, buffer, start, 1);

  if (start + 2 > buffer_len)
    throw_for_encoding_error(
	encoding_name,
	buffer,
	start,
	buffer_len - start);

  const auto byte2 = get_byte(buffer, start + 1);
  if (byte2 == 0x7f) throw_for_encoding_error(encoding_name, buffer, start, 2);

  if (between_inc(byte2, 0x40, 0x9e) or between_inc(byte2, 0x9f, 0xfc))
    return start + 2;

  throw_for_encoding_error(encoding_name, buffer, start, 2);
}
} // namespace


// Implement template specializations first
namespace pqxx
{
namespace internal
{
template<encoding_group> struct glyph_scanner
{
  static std::string::size_type call(
	const char buffer[],
	std::string::size_type buffer_len,
	std::string::size_type start);
};

template<>
std::string::size_type glyph_scanner<encoding_group::MONOBYTE>::call(
  const char /* buffer */[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;
  else return start + 1;
}

// https://en.wikipedia.org/wiki/Big5#Organization
template<> std::string::size_type glyph_scanner<encoding_group::BIG5>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (not between_inc(byte1, 0x81, 0xfe) or (start + 2 > buffer_len))
    throw_for_encoding_error("BIG5", buffer, start, 1);

  const auto byte2 = get_byte(buffer, start + 1);
  if (
	not between_inc(byte2, 0x40, 0x7e) and
	not between_inc(byte2, 0xa1, 0xfe))
    throw_for_encoding_error("BIG5", buffer, start, 2);

  return start + 2;
}

/*
The PostgreSQL documentation claims that the EUC_* encodings are 1-3 bytes each,
but other documents explain that the EUC sets can contain 1-(2,3,4) bytes
depending on the specific extension:
    EUC_CN      : 1-2
    EUC_JP      : 1-3
    EUC_JIS_2004: 1-2
    EUC_KR      : 1-2
    EUC_TW      : 1-4
*/

// https://en.wikipedia.org/wiki/GB_2312#EUC-CN
template<> std::string::size_type glyph_scanner<encoding_group::EUC_CN>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (not between_inc(byte1, 0xa1, 0xf7) or start + 2 > buffer_len)
    throw_for_encoding_error("EUC_CN", buffer, start, 1);

  const auto byte2 = get_byte(buffer, start + 1);
  if (not between_inc(byte2, 0xa1, 0xfe))
    throw_for_encoding_error("EUC_CN", buffer, start, 2);

  return start + 2;
}

template<> std::string::size_type glyph_scanner<encoding_group::EUC_JP>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  return next_seq_for_euc_jplike(buffer, buffer_len, start, "EUC_JP");
}

template<>
std::string::size_type glyph_scanner<encoding_group::EUC_JIS_2004>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  return next_seq_for_euc_jplike(buffer, buffer_len, start, "EUC_JIS_2004");
}

// https://en.wikipedia.org/wiki/Extended_Unix_Code#EUC-KR
template<> std::string::size_type glyph_scanner<encoding_group::EUC_KR>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (not between_inc(byte1, 0xa1, 0xfe) or start + 2 > buffer_len)
    throw_for_encoding_error("EUC_KR", buffer, start, 1);

  const auto byte2 = get_byte(buffer, start + 1);
  if (not between_inc(byte2, 0xa1, 0xfe))
    throw_for_encoding_error("EUC_KR", buffer, start, 1);

  return start + 2;
}

// https://en.wikipedia.org/wiki/Extended_Unix_Code#EUC-TW
template<> std::string::size_type glyph_scanner<encoding_group::EUC_TW>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (start + 2 > buffer_len)
    throw_for_encoding_error("EUC_KR", buffer, start, 1);

  const auto byte2 = get_byte(buffer, start + 1);
  if (between_inc(byte1, 0xa1, 0xfe))
  {
    if (not between_inc(byte2, 0xa1, 0xfe))
      throw_for_encoding_error("EUC_KR", buffer, start, 2);

    return start + 2;
  }

  if (byte1 != 0x8e or start + 4 > buffer_len)
    throw_for_encoding_error("EUC_KR", buffer, start, 1);

  if (
        between_inc(byte2, 0xa1, 0xb0) and
        between_inc(get_byte(buffer, start + 2), 0xa1, 0xfe) and
        between_inc(get_byte(buffer, start + 3), 0xa1, 0xfe)
  )
    return start + 4;

  throw_for_encoding_error("EUC_KR", buffer, start, 4);
}

// https://en.wikipedia.org/wiki/GB_18030#Mapping
template<> std::string::size_type glyph_scanner<encoding_group::GB18030>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (between_inc(byte1, 0x80, 0xff)) return start + 1;

  if (start + 2 > buffer_len)
    throw_for_encoding_error("GB18030", buffer, start, buffer_len - start);

  const auto byte2 = get_byte(buffer, start + 1);
  if (between_inc(byte2, 0x40, 0xfe))
  {
    if (byte2 == 0x7f)
      throw_for_encoding_error("GB18030", buffer, start, 2);

    return start + 2;
  }

  if (start + 4 > buffer_len)
    throw_for_encoding_error("GB18030", buffer, start, buffer_len - start);

  if (
	between_inc(byte2, 0x30, 0x39) and
	between_inc(get_byte(buffer, start + 2), 0x81, 0xfe) and
	between_inc(get_byte(buffer, start + 3), 0x30, 0x39)
  )
    return start + 4;

  throw_for_encoding_error("GB18030", buffer, start, 4);
}

// https://en.wikipedia.org/wiki/GBK_(character_encoding)#Encoding
template<> std::string::size_type glyph_scanner<encoding_group::GBK>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (start + 2 > buffer_len)
    throw_for_encoding_error("GBK", buffer, start, 1);

  const auto byte2 = get_byte(buffer, start + 1);
  if (
    (between_inc(byte1, 0xa1, 0xa9) and between_inc(byte2, 0xa1, 0xfe))
    or
    (between_inc(byte1, 0xb0, 0xf7) and between_inc(byte2, 0xa1, 0xfe))
    or
    (
      between_inc(byte1, 0x81, 0xa0) and
      between_inc(byte2, 0x40, 0xfe) and
      byte2 != 0x7f
    )
    or
    (
      between_inc(byte1, 0xaa, 0xfe) and
      between_inc(byte2, 0x40, 0xa0) and
      byte2 != 0x7f
    )
    or
    (
      between_inc(byte1, 0xa8, 0xa9) and
      between_inc(byte2, 0x40, 0xa0) and
      byte2 != 0x7f
    )
    or
    (between_inc(byte1, 0xaa, 0xaf) and between_inc(byte2, 0xa1, 0xfe))
    or
    (between_inc(byte1, 0xf8, 0xfe) and between_inc(byte2, 0xa1, 0xfe))
    or
    (
      between_inc(byte1, 0xa1, 0xa7) and
      between_inc(byte2, 0x40, 0xa0) and
      byte2 != 0x7f
    )
  )
    return start + 2;

  throw_for_encoding_error("GBK", buffer, start, 2);
}

/*
The PostgreSQL documentation claims that the JOHAB encoding is 1-3 bytes, but
"CJKV Information Processing" describes it (actually just the Hangul portion)
as "three five-bit segments" that reside inside 16 bits (2 bytes).

CJKV Information Processing by Ken Lunde, pg. 269:

  https://bit.ly/2BEOu5V
*/
template<> std::string::size_type glyph_scanner<encoding_group::JOHAB>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (start + 2 > buffer_len)
    throw_for_encoding_error("JOHAB", buffer, start, 1);

  const auto byte2 = get_byte(buffer, start);
  if (
    (
      between_inc(byte1, 0x84, 0xd3) and
      (between_inc(byte2, 0x41, 0x7e) or between_inc(byte2, 0x81, 0xfe))
    )
    or
    (
      (between_inc(byte1, 0xd8, 0xde) or between_inc(byte1, 0xe0, 0xf9)) and
      (between_inc(byte2, 0x31, 0x7e) or between_inc(byte2, 0x91, 0xfe))
    )
  )
    return start + 2;

  throw_for_encoding_error("JOHAB", buffer, start, 2);
}

/*
PostgreSQL's MULE_INTERNAL is the emacs rather than Xemacs implementation;
see the server/mb/pg_wchar.h PostgreSQL header file.
This is implemented according to the description in said header file, but I was
unable to get it to successfully iterate a MULE-encoded test CSV generated using
PostgreSQL 9.2.23.  Use this at your own risk.
*/
template<>
std::string::size_type glyph_scanner<encoding_group::MULE_INTERNAL>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (start + 2 > buffer_len)
    throw_for_encoding_error("MULE_INTERNAL", buffer, start, 1);

  const auto byte2 = get_byte(buffer, start + 1);
  if (between_inc(byte1, 0x81, 0x8d) and byte2 >= 0xA0)
    return start + 2;

  if (start + 3 > buffer_len)
    throw_for_encoding_error("MULE_INTERNAL", buffer, start, 2);

  if (
    (
      (byte1 == 0x9A and between_inc(byte2, 0xa0, 0xdf)) or
      (byte1 == 0x9B and between_inc(byte2, 0xe0, 0xef)) or
      (between_inc(byte1, 0x90, 0x99) and byte2 >= 0xa0)
    )
    and
    (
      byte2 >= 0xA0
    )
  )
    return start + 3;

  if (start + 4 > buffer_len)
    throw_for_encoding_error("MULE_INTERNAL", buffer, start, 3);

  if (
    (
      (byte1 == 0x9C and between_inc(byte2, 0xf0, 0xf4)) or
      (byte1 == 0x9D and between_inc(byte2, 0xf5, 0xfe))
    )
    and
    get_byte(buffer, start + 2) >= 0xa0 and
    get_byte(buffer, start + 4) >= 0xa0
  )
    return start + 4;

  throw_for_encoding_error("MULE_INTERNAL", buffer, start, 4);
}

template<> std::string::size_type glyph_scanner<encoding_group::SJIS>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  return next_seq_for_sjislike(buffer, buffer_len, start, "SJIS");
}

template<>
std::string::size_type glyph_scanner<encoding_group::SHIFT_JIS_2004>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  return next_seq_for_sjislike(buffer, buffer_len, start, "SHIFT_JIS_2004");
}

// https://en.wikipedia.org/wiki/Unified_Hangul_Code
template<> std::string::size_type glyph_scanner<encoding_group::UHC>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (start + 2 > buffer_len)
    throw_for_encoding_error("UHC", buffer, start, buffer_len - start);

  const auto byte2 = get_byte(buffer, start + 1);
  if (between_inc(byte1, 0x80, 0xc6))
  {
    if (
      between_inc(byte2, 0x41, 0x5a) or
      between_inc(byte2, 0x61, 0x7a) or
      between_inc(byte2, 0x80, 0xfe)
    )
      return start + 2;

    throw_for_encoding_error("UHC", buffer, start, 2);
  }

  if (between_inc(byte1, 0xa1, 0xfe))
  {
    if (not between_inc(byte2, 0xa1, 0xfe))
      throw_for_encoding_error("UHC", buffer, start, 2);

   return start + 2;
  }

  throw_for_encoding_error("UHC", buffer, start, 1);
}

// https://en.wikipedia.org/wiki/UTF-8#Description
template<> std::string::size_type glyph_scanner<encoding_group::UTF8>::call(
  const char buffer[],
  std::string::size_type buffer_len,
  std::string::size_type start
)
{
  if (start >= buffer_len) return std::string::npos;

  const auto byte1 = get_byte(buffer, start);
  if (byte1 < 0x80) return start + 1;

  if (start + 2 > buffer_len)
      throw_for_encoding_error("UTF8", buffer, start, buffer_len - start);

  const auto byte2 = get_byte(buffer, start + 1);
  if (between_inc(byte1, 0xc0, 0xdf))
  {
    if (not between_inc(byte2, 0x80, 0xbf))
      throw_for_encoding_error("UTF8", buffer, start, 2);

    return start + 2;
  }

  if (start + 3 > buffer_len)
      throw_for_encoding_error("UTF8", buffer, start, buffer_len - start);

  const auto byte3 = get_byte(buffer, start + 2);
  if (between_inc(byte1, 0xe0, 0xef))
  {
    if (between_inc(byte2, 0x80, 0xbf) and between_inc(byte3, 0x80, 0xbf))
      return start + 3;

    throw_for_encoding_error("UTF8", buffer, start, 3);
  }

  if (start + 4 > buffer_len)
      throw_for_encoding_error("UTF8", buffer, start, buffer_len - start);

  if (between_inc(byte1, 0xf0, 0xf7))
  {
    if (
      between_inc(byte2, 0x80, 0xbf) and
      between_inc(byte3, 0x80, 0xbf) and
      between_inc(get_byte(buffer, start + 3), 0x80, 0xbf)
    )
      return start + 4;

    throw_for_encoding_error("UTF8", buffer, start, 4);
  }

  throw_for_encoding_error("UTF8", buffer, start, 1);
}


const char *name_encoding(int encoding_id)
{
  return pg_encoding_to_char(encoding_id);
}


encoding_group enc_group(int libpq_enc_id)
{
  return enc_group(name_encoding(libpq_enc_id));
}


encoding_group enc_group(const std::string& encoding_name)
{
  static const std::map<std::string, encoding_group> encoding_map{
    {"BIG5", encoding_group::BIG5},
    {"EUC_CN", encoding_group::EUC_CN},
    {"EUC_JP", encoding_group::EUC_JP},
    {"EUC_JIS_2004", encoding_group::EUC_JIS_2004},
    {"EUC_KR", encoding_group::EUC_KR},
    {"EUC_TW", encoding_group::EUC_TW},
    {"GB18030", encoding_group::GB18030},
    {"GBK", encoding_group::GBK},
    {"ISO_8859_5", encoding_group::MONOBYTE},
    {"ISO_8859_6", encoding_group::MONOBYTE},
    {"ISO_8859_7", encoding_group::MONOBYTE},
    {"ISO_8859_8", encoding_group::MONOBYTE},
    {"JOHAB", encoding_group::JOHAB},
    {"KOI8R", encoding_group::MONOBYTE},
    {"KOI8U", encoding_group::MONOBYTE},
    {"LATIN1", encoding_group::MONOBYTE},
    {"LATIN2", encoding_group::MONOBYTE},
    {"LATIN3", encoding_group::MONOBYTE},
    {"LATIN4", encoding_group::MONOBYTE},
    {"LATIN5", encoding_group::MONOBYTE},
    {"LATIN6", encoding_group::MONOBYTE},
    {"LATIN7", encoding_group::MONOBYTE},
    {"LATIN8", encoding_group::MONOBYTE},
    {"LATIN9", encoding_group::MONOBYTE},
    {"LATIN10", encoding_group::MONOBYTE},
    {"MULE_INTERNAL", encoding_group::MULE_INTERNAL},
    {"SJIS", encoding_group::SJIS},
    {"SHIFT_JIS_2004", encoding_group::SHIFT_JIS_2004},
    {"SQL_ASCII", encoding_group::MONOBYTE},
    {"UHC", encoding_group::UHC},
    {"UTF8", encoding_group::UTF8},
    {"WIN866", encoding_group::MONOBYTE},
    {"WIN874", encoding_group::MONOBYTE},
    {"WIN1250", encoding_group::MONOBYTE},
    {"WIN1251", encoding_group::MONOBYTE},
    {"WIN1252", encoding_group::MONOBYTE},
    {"WIN1253", encoding_group::MONOBYTE},
    {"WIN1254", encoding_group::MONOBYTE},
    {"WIN1255", encoding_group::MONOBYTE},
    {"WIN1256", encoding_group::MONOBYTE},
    {"WIN1257", encoding_group::MONOBYTE},
    {"WIN1258", encoding_group::MONOBYTE},
  };
  
  const auto found_encoding_group = encoding_map.find(encoding_name);
  if (found_encoding_group == encoding_map.end())
    throw std::invalid_argument{
      "unrecognized encoding '" + encoding_name + "'"
    };
  return found_encoding_group->second;
}


/// Look up instantiation @c T<enc>::call at runtime.
/** Here, "T" is a struct template with a static member function "call", whose
 * type is "F".
 *
 * The return value is a pointer to the "call" member function for the
 * instantiation of T for encoding group enc.
 */
template<template<encoding_group> class T, typename F>
inline F *for_encoding(encoding_group enc)
{

#define CASE_GROUP(ENC) \
	case encoding_group::ENC: return T<encoding_group::ENC>::call

  switch (enc)
  {
  CASE_GROUP(MONOBYTE);
  CASE_GROUP(BIG5);
  CASE_GROUP(EUC_CN);
  CASE_GROUP(EUC_JP);
  CASE_GROUP(EUC_JIS_2004);
  CASE_GROUP(EUC_KR);
  CASE_GROUP(EUC_TW);
  CASE_GROUP(GB18030);
  CASE_GROUP(GBK);
  CASE_GROUP(JOHAB);
  CASE_GROUP(MULE_INTERNAL);
  CASE_GROUP(SJIS);
  CASE_GROUP(SHIFT_JIS_2004);
  CASE_GROUP(UHC);
  CASE_GROUP(UTF8);
  }
  throw pqxx::usage_error{
	"Unsupported encoding group code " + to_string(int(enc)) + "."};

#undef CASE_GROUP
}


glyph_scanner_func *get_glyph_scanner(encoding_group enc)
{
  return for_encoding<glyph_scanner, glyph_scanner_func>(enc);
}


template<encoding_group E> struct char_finder
{
  static std::string::size_type call(
	const std::string &haystack,
	char needle,
	std::string::size_type start)
  {
    const auto buffer = haystack.c_str();
    const auto size = haystack.size();
    for (
	auto here = start;
	here + 1 <= size;
	here = glyph_scanner<E>::call(buffer, size, here)
    )
    {
      if (haystack[here] == needle) return here;
    }
    return std::string::npos;
  }
};


template<encoding_group E> struct string_finder
{
  static std::string::size_type call(
	const std::string &haystack,
	const std::string &needle,
	std::string::size_type start)
  {
    const auto buffer = haystack.c_str();
    const auto size = haystack.size();
    const auto needle_size = needle.size();
    for (
	auto here = start;
	here + needle_size <= size;
	here = glyph_scanner<E>::call(buffer, size, here)
    )
    {
      if (std::memcmp(buffer + here, needle.c_str(), needle_size) == 0)
        return here;
    }
    return std::string::npos;
  }
};


std::string::size_type find_with_encoding(
	encoding_group enc,
	const std::string& haystack,
	char needle,
	std::string::size_type start
)
{
  using finder_func =
    std::string::size_type(
	const std::string &haystack,
	char needle,
	std::string::size_type start);
  const auto finder = for_encoding<char_finder, finder_func>(enc);
  return finder(haystack, needle, start);
}


std::string::size_type find_with_encoding(
	encoding_group enc,
	const std::string& haystack,
	const std::string& needle,
	std::string::size_type start
)
{
  using finder_func =
    std::string::size_type(
	const std::string &haystack,
	const std::string &needle,
	std::string::size_type start);
  const auto finder = for_encoding<string_finder, finder_func>(enc);
  return finder(haystack, needle, start);
}

#undef DISPATCH_ENCODING_OPERATION

} // namespace pqxx::internal
} // namespace pqxx
