/** Internal string encodings support for libpqxx
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_ENCODINGS
#define PQXX_H_ENCODINGS

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"
#include "pqxx/internal/encoding_group.hxx"

#include <string>


namespace pqxx
{
namespace internal
{
const char *name_encoding(int encoding_id);

/// Convert libpq encoding enum or encoding name to its libpqxx group.
encoding_group enc_group(int /* libpq encoding ID */);
encoding_group enc_group(const std::string&);


/// Function type: "find the end of the current glyph."
/** This type of function takes a text buffer, and a location in that buffer,
 * and returns the location one byte past the end of the current glyph.
 *
 * The start offset marks the beginning of the current glyph.  It must fall
 * within the buffer.
 *
 * There are multiple different glyph scnaner implementations, for different
 * kinds of encodings.
 */
using glyph_scanner_func =
  std::string::size_type(
	const char buffer[],
	std::string::size_type buffer_len,
	std::string::size_type start);


/// Look up the glyph scanner function for a given encoding group.
/** To identify the glyph boundaries in a buffer, call this to obtain the
 * scanner function appropriate for the buffer's encoding.  Then, repeatedly
 * call the scanner function to find the glyphs.
 */
PQXX_LIBEXPORT glyph_scanner_func *get_glyph_scanner(encoding_group);


/// Find a single-byte "needle" character in a "haystack" text buffer.
std::string::size_type find_with_encoding(
  encoding_group enc,
  const std::string& haystack,
  char needle,
  std::string::size_type start = 0
);


PQXX_LIBEXPORT std::string::size_type find_with_encoding(
  encoding_group enc,
  const std::string& haystack,
  const std::string& needle,
  std::string::size_type start = 0
);


/// Iterate over the glyphs in a buffer.
/** Scans the glyphs in the buffer, and for each, passes its begin and its
 * one-past-end pointers to @c callback.
 */
template<typename CALLABLE> PQXX_LIBEXPORT inline void for_glyphs(
        encoding_group enc,
        CALLABLE callback,
        const char buffer[],
        std::string::size_type buffer_len,
        std::string::size_type start = 0
)
{
  const auto scan = get_glyph_scanner(enc);
  for (
        std::string::size_type here = start, next;
        here < buffer_len;
        here = next
  )
  {
    next = scan(buffer, buffer_len, here);
    callback(buffer + here, buffer + next);
  }
}
} // namespace pqxx::internal
} // namespace pqxx

#include "pqxx/compiler-internal-post.hxx"
#endif
