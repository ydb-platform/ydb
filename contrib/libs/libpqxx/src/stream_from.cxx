/** Implementation of the pqxx::stream_from class.
 *
 * pqxx::stream_from enables optimized batch reads from a database table.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include "pqxx/stream_from"

#include "pqxx/internal/encodings.hxx"
#include "pqxx/internal/gates/transaction-stream_from.hxx"


namespace
{
  // bool is_octalchar(char o) noexcept
  // {
  //   return (o>='0') and (o<='7');
  // }

  /// Find first tab character at or after start position in string
  /** If not found, returns line.size() rather than string::npos.
   */
  std::string::size_type find_tab(
    pqxx::internal::encoding_group enc,
    const std::string &line,
    std::string::size_type start
  )
  {
    auto here = pqxx::internal::find_with_encoding(enc, line, '\t', start);
    return (here == std::string::npos) ? line.size() : here;
  }
} // namespace


pqxx::stream_from::stream_from(
  transaction_base &tb,
  const std::string &table_name
) :
  namedclass{"stream_from", table_name},
  stream_base{tb},
  m_retry_line{false}
{
  set_up(tb, table_name);
}


pqxx::stream_from::~stream_from() noexcept
{
  try
  {
    complete();
  }
  catch (const std::exception &e)
  {
    reg_pending_error(e.what());
  }
}


void pqxx::stream_from::complete()
{
  close();
}


bool pqxx::stream_from::get_raw_line(std::string &line)
{
  internal::gate::transaction_stream_from gate{m_trans};
  if (*this)
    try
    {
      if (not gate.read_copy_line(line)) close();
    }
    catch (const std::exception &)
    {
      close();
      throw;
    }
  return *this;
}


void pqxx::stream_from::set_up(
  transaction_base &tb,
  const std::string &table_name
)
{
  set_up(tb, table_name, "");
}


void pqxx::stream_from::set_up(
  transaction_base &tb,
  const std::string &table_name,
  const std::string &columns
)
{
  // Get the encoding before starting the COPY, otherwise reading the the
  // variable will interrupt it
  m_copy_encoding = internal::enc_group(m_trans.conn().encoding_id());
  internal::gate::transaction_stream_from{tb}.BeginCopyRead(
    table_name,
    columns
  );
  register_me();
}


void pqxx::stream_from::close()
{
  pqxx::stream_base::close();
  try
  {
    // Flush any remaining lines
    std::string s;
    while (get_raw_line(s));
  }
  catch (const broken_connection &)
  {
    try
    {
      pqxx::stream_base::close();
    }
    catch (const std::exception &) {}
    throw;
  }
  catch (const std::exception &e)
  {
    reg_pending_error(e.what());
  }
}


bool pqxx::stream_from::extract_field(
	const std::string &line,
	std::string::size_type &i,
	std::string &s
) const
{
  using namespace pqxx::internal;

  const auto next_seq = get_glyph_scanner(m_copy_encoding);
  s.clear();
  bool is_null{false};
  auto stop = find_tab(m_copy_encoding, line, i);
  while (i < stop)
  {
    auto glyph_end = next_seq(line.c_str(), line.size(), i);
    auto seq_len = glyph_end - i;
    if (seq_len == 1)
    {
      switch (line[i])
      {
      case '\n':
        // End-of-row; shouldn't happen, but we may get old-style
        // newline-terminated lines.
        i = stop;
        break;

      case '\\':
        {
        // Escape sequence.
          if (glyph_end >= line.size())
            throw failure{"Row ends in backslash"};
          char n = line[glyph_end++];
          
          /*
           * "Presently, COPY TO will never emit an octal or hex-digits
           * backslash sequence [...]"
           *  - https://www.postgresql.org/docs/10/sql-copy.html
           */
          // if (is_octalchar(n))
          // {
          //   if (here.end_byte+2 >= line.size())
          //     throw failure{"Row ends in middle of octal value"};
          //   char n1 = line[here.end_byte++];
          //   char n2 = line[here.end_byte++];
          //   if (not is_octalchar(n1) or not is_octalchar(n2))
          //     throw failure{
          //       "Invalid octal in encoded table stream"
          //     };
          //   s += (
          //     (digit_to_number(n)<<6) |
          //     (digit_to_number(n1)<<3) |
          //     digit_to_number(n2)
          //   );
          //   break;
          // }
          // else
            switch (n)
            {
            case 'N':
              // Null value
              if (not s.empty())
                throw failure{
                  "Null sequence found in nonempty field"
                };
              is_null = true;
              break;

            case 'b': // Backspace
              s += '\b'; break;
            case 'f': // Vertical tab
              s += '\f'; break;
            case 'n': // Form feed
              s += '\n'; break;
            case 'r': // Newline
              s += '\r'; break;
            case 't': // Tab
              s += '\t'; break;
            case 'v': // Carriage return
              s += '\v'; break;

            default:
              // Self-escaped character
              s += n;
              break;
            }
        }
        break;

      default:
        s += line[i];
        break;
      }
    }
    else
    {
      // Multi-byte sequence; never treated specially, so just append
      s.insert(s.size(), line.c_str() + i, seq_len);
    }

    i = glyph_end;
  }

  // Skip field separator
  i += 1;

  return not is_null;
}

template<> void pqxx::stream_from::extract_value<std::nullptr_t>(
  const std::string &line,
  std::nullptr_t&,
  std::string::size_type &here,
  std::string &workspace
) const
{
  if (extract_field(line, here, workspace))
    throw pqxx::conversion_error{
      "Attempt to convert non-null '"
      + workspace
      + "' to null"
    };
}
