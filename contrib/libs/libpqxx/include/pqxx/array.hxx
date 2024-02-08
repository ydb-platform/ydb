/** Handling of SQL arrays.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/field instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_ARRAY
#define PQXX_H_ARRAY

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include "pqxx/internal/encoding_group.hxx"
#include "pqxx/internal/encodings.hxx"

#include <stdexcept>
#include <string>
#include <utility>


namespace pqxx
{
/// Low-level array parser.
/** Use this to read an array field retrieved from the database.
 *
 * This parser will only work reliably if your client encoding is UTF-8, ASCII,
 * or a single-byte encoding which is a superset of ASCII (such as Latin-1).
 *
 * Also, the parser only supports array element types which use either a comma
 * or a semicolon ("," or ";") as the separator between array elements.  All
 * built-in types use comma, except for one which uses semicolon, but some
 * custom types may not work.
 *
 * The input is a C-style string containing the textual representation of an
 * array, as returned by the database.  The parser reads this representation
 * on the fly.  The string must remain in memory until parsing is done.
 *
 * Parse the array by making calls to @c get_next until it returns a
 * @c juncture of "done".  The @c juncture tells you what the parser found in
 * that step: did the array "nest" to a deeper level, or "un-nest" back up?
 */
class PQXX_LIBEXPORT array_parser
{
public:
  /// What's the latest thing found in the array?
  enum juncture
  {
    /// Starting a new row.
    row_start,
    /// Ending the current row.
    row_end,
    /// Found a NULL value.
    null_value,
    /// Found a string value.
    string_value,
    /// Parsing has completed.
    done,
  };

// XXX: Actually _pass_ encoding group!
  /// Constructor.  You don't need this; use @c field::as_array instead.
  explicit array_parser(
	const char input[],
	internal::encoding_group=internal::encoding_group::MONOBYTE);

  /// Parse the next step in the array.
  /** Returns what it found.  If the juncture is @c string_value, the string
   * will contain the value.  Otherwise, it will be empty.
   *
   * Call this until the @c juncture it returns is @c done.
   */
  std::pair<juncture, std::string> get_next();

private:
  const char *const m_input;
  const std::string::size_type m_end;
  internal::glyph_scanner_func *const m_scan;

  /// Current parsing position in the input.
  std::string::size_type m_pos;

  std::string::size_type scan_single_quoted_string() const;
  std::string parse_single_quoted_string(std::string::size_type end) const;
  std::string::size_type scan_double_quoted_string() const;
  std::string parse_double_quoted_string(std::string::size_type end) const;
  std::string::size_type scan_unquoted_string() const;
  std::string parse_unquoted_string(std::string::size_type end) const;

  std::string::size_type scan_glyph(std::string::size_type pos) const;
  std::string::size_type scan_glyph(
	std::string::size_type pos,
	std::string::size_type end) const;
};
} // namespace pqxx

#include "pqxx/compiler-internal-post.hxx"
#endif
