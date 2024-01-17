/** Handling of SQL arrays.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <utility>

#include "pqxx/array"
#include "pqxx/except"


namespace pqxx
{
/// Scan to next glyph in the buffer.  Assumes there is one.
std::string::size_type array_parser::scan_glyph(
	std::string::size_type pos) const
{
  assert(pos < m_end);
  return m_scan(m_input, m_end, pos);
}


/// Scan to next glyph in a substring.  Assumes there is one.
std::string::size_type array_parser::scan_glyph(
	std::string::size_type pos,
	std::string::size_type end) const
{
  assert(pos < end);
  assert(end <= m_end);
  return m_scan(m_input, end, pos);
}


/// Find the end of a single-quoted SQL string in an SQL array.
/** Returns the offset of the first character after the closing quote.
 */
std::string::size_type array_parser::scan_single_quoted_string() const
{
  auto here = m_pos, next = scan_glyph(here);
  assert(next < m_end);
  assert(next - here == 1);
  assert(m_input[here] == '\'');
  for (
	here = next, next = scan_glyph(here);
	here < m_end;
	here = next, next = scan_glyph(here)
  )
  {
    if (next - here == 1) switch (m_input[here])
    {
    case '\'':
      // SQL escapes single quotes by doubling them.  Terrible idea, but it's
      // what we have.  Inspect the next character to find out whether this is
      // the closing quote, or an escaped one inside the string.
      here = next;
      // (We can read beyond this quote because the array will always end in
      // a closing brace.)
      next = scan_glyph(here);

      if ((here + 1 < next) or (m_input[here] != '\''))
      {
        // Our lookahead character is not an escaped quote.  It's the first
        // character outside our string.  So, return it.
        return here;
      }

      // We've just scanned an escaped quote.  Keep going.
      break;

    case '\\':
      // Backslash escape.  Skip ahead by one more character.
      here = next;
      next = scan_glyph(here);
      break;
    }
  }
  throw argument_error{"Null byte in SQL string: " + std::string{m_input}};
}


/// Parse a single-quoted SQL string: un-quote it and un-escape it.
std::string array_parser::parse_single_quoted_string(
	std::string::size_type end) const
{
  // There have to be at least 2 characters: the opening and closing quotes.
  assert(m_pos + 1 < end);
  assert(m_input[m_pos] == '\'');
  assert(m_input[end - 1] == '\'');

  std::string output;
  // Maximum output size is same as the input size, minus the opening and
  // closing quotes.  In the worst case, the real number could be half that.
  // Usually it'll be a pretty close estimate.
  output.reserve(end - m_pos - 2);
  for (
	auto here = m_pos + 1, next = scan_glyph(here, end);
	here < end - 1;
	here = next, next = scan_glyph(here, end)
  )
  {
    if (
	next - here == 1 and
        (m_input[here] == '\'' or m_input[here] == '\\')
    )
    {
      // Skip escape.
      here = next;
      next = scan_glyph(here, end);
    }

    output.append(m_input + here, m_input + next);
  }

  return output;
}


/// Find the end of a double-quoted SQL string in an SQL array.
std::string::size_type array_parser::scan_double_quoted_string() const
{
  auto here = m_pos;
  assert(here < m_end);
  auto next = scan_glyph(here);
  assert(next - here == 1);
  assert(m_input[here] == '"');
  for (
	here = next, next = scan_glyph(here);
	here < m_end;
	here = next, next = scan_glyph(here)
  )
  {
    if (next - here == 1) switch (m_input[here])
    {
    case '\\':
      // Backslash escape.  Skip ahead by one more character.
      here = next;
      next = scan_glyph(here);
      break;

    case '"':
      // Closing quote.  Return the position right after.
      return next;
    }
  }
  throw argument_error{"Null byte in SQL string: " + std::string{m_input}};
}


/// Parse a double-quoted SQL string: un-quote it and un-escape it.
std::string array_parser::parse_double_quoted_string(
	std::string::size_type end) const
{
  // There have to be at least 2 characters: the opening and closing quotes.
  assert(m_pos + 1 < end);
  assert(m_input[m_pos] == '"');
  assert(m_input[end - 1] == '"');

  std::string output;
  // Maximum output size is same as the input size, minus the opening and
  // closing quotes.  In the worst case, the real number could be half that.
  // Usually it'll be a pretty close estimate.
  output.reserve(std::size_t(end - m_pos - 2));

  for (
	auto here = scan_glyph(m_pos, end), next = scan_glyph(here, end);
	here < end - 1;
	here = next, next = scan_glyph(here, end)
  )
  {
    if ((next - here == 1) and (m_input[here] == '\\'))
    {
      // Skip escape.
      here = next;
      next = scan_glyph(here, end);
    }

    output.append(m_input + here, m_input + next);
  }

  return output;
}


/// Find the end of an unquoted string in an SQL array.
/** Assumes UTF-8 or an ASCII-superset single-byte encoding.
 */
std::string::size_type array_parser::scan_unquoted_string() const
{
  auto here = m_pos, next = scan_glyph(here);
  assert(here < m_end);
  assert((next - here > 1) or (m_input[here] != '\''));
  assert((next - here > 1) or (m_input[here] != '"'));

  while (
        (next - here) > 1 or
        (
	  m_input[here] != ',' and
	  m_input[here] != ';' and
	  m_input[here] != '}'
        )
  )
  {
    here = next;
    next = scan_glyph(here);
  }
  return here;
}


/// Parse an unquoted SQL string.
/** Here, the special unquoted value NULL means a null value, not a string
 * that happens to spell "NULL".
 */
std::string array_parser::parse_unquoted_string(
	std::string::size_type end) const
{
  return std::string{m_input + m_pos, m_input + end};
}


array_parser::array_parser(
	const char input[],
	internal::encoding_group enc) :
  m_input(input),
  m_end(input == nullptr ? 0 : std::strlen(input)),
  m_scan(internal::get_glyph_scanner(enc)),
  m_pos(0)
{
}


std::pair<array_parser::juncture, std::string>
array_parser::get_next()
{
  juncture found;
  std::string value;
  std::string::size_type end;

  if (m_input == nullptr or (m_pos >= m_end))
    return std::make_pair(juncture::done, value);

  if (scan_glyph(m_pos) - m_pos > 1)
  {
    // Non-ASCII unquoted string.
    end = scan_unquoted_string();
    value = parse_unquoted_string(end);
    found = juncture::string_value;
  }
  else switch (m_input[m_pos])
  {
  case '\0':
    // TODO: Maybe just raise an error?
    found = juncture::done;
    end = m_pos;
    break;
  case '{':
    found = juncture::row_start;
    end = scan_glyph(m_pos);
    break;
  case '}':
    found = juncture::row_end;
    end = scan_glyph(m_pos);
    break;
  case '\'':
    found = juncture::string_value;
    end = scan_single_quoted_string();
    value = parse_single_quoted_string(end);
    break;
  case '"':
    found = juncture::string_value;
    end = scan_double_quoted_string();
    value = parse_double_quoted_string(end);
    break;
  default:
    end = scan_unquoted_string();
    value = parse_unquoted_string(end);
    if (value == "NULL")
    {
      // In this one situation, as a special case, NULL means a null field,
      // not a string that happens to spell "NULL".
      value.clear();
      found = juncture::null_value;
    }
    else
    {
      // The normal case: we just parsed an unquoted string.  The value is
      // what we need.
      found = juncture::string_value;
    }
    break;
  }

  // Skip a trailing field separator, if present.
  if (end < m_end)
  {
    auto next = scan_glyph(end);
    if (next - end == 1 and (m_input[end] == ',' or m_input[end] == ';'))
      end = next;
  }

  m_pos = end;
  return std::make_pair(found, value);
}
} // namespace pqxx
