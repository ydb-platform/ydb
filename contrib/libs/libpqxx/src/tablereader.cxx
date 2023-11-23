/** Implementation of the pqxx::tablereader class.
 *
 * pqxx::tablereader enables optimized batch reads from a database table.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include "pqxx/tablereader"
#include "pqxx/transaction"

#include "pqxx/internal/gates/transaction-tablereader.hxx"

using namespace pqxx::internal;


pqxx::tablereader::tablereader(
	transaction_base &T,
	const std::string &Name,
	const std::string &Null) :
  namedclass{"tablereader", Name},
  tablestream(T, Null),
  m_done{true}
{
  set_up(T, Name);
}

void pqxx::tablereader::set_up(
	transaction_base &T,
	const std::string &Name,
	const std::string &Columns)
{
  gate::transaction_tablereader{T}.BeginCopyRead(Name, Columns);
  register_me();
  m_done = false;
}

pqxx::tablereader::~tablereader() noexcept
{
  try
  {
    reader_close();
  }
  catch (const std::exception &e)
  {
    reg_pending_error(e.what());
  }
}


bool pqxx::tablereader::get_raw_line(std::string &Line)
{
  if (not m_done) try
  {
    m_done = not gate::transaction_tablereader{m_trans}.read_copy_line(Line);
  }
  catch (const std::exception &)
  {
    m_done = true;
    throw;
  }
  return not m_done;
}


void pqxx::tablereader::complete()
{
  reader_close();
}


void pqxx::tablereader::reader_close()
{
  if (not is_finished())
  {
    base_close();

    // If any lines remain to be read, consume them to not confuse PQendcopy()
    if (not m_done)
    {
      try
      {
        std::string Dummy;
        while (get_raw_line(Dummy)) ;
      }
      catch (const broken_connection &)
      {
	try { base_close(); } catch (const std::exception &) {}
	throw;
      }
      catch (const std::exception &e)
      {
        reg_pending_error(e.what());
      }
    }
  }
}


namespace
{
inline bool is_octalchar(char o) noexcept
{
  return (o>='0') and (o<='7');
}

/// Find first tab character at or after start position in string
/** If not found, returns Line.size() rather than string::npos.
 */
std::string::size_type findtab(
	const std::string &Line,
	std::string::size_type start)
{
  // TODO: Fix for multibyte encodings?
  const auto here = Line.find('\t', start);
  return (here == std::string::npos) ? Line.size() : here;
}
} // namespace


std::string pqxx::tablereader::extract_field(
	const std::string &Line,
	std::string::size_type &i) const
{
  // TODO: Pick better exception types
  std::string R;
  bool isnull=false;
  auto stop = findtab(Line, i);
  for (; i < stop; ++i)
  {
    const char c = Line[i];
    switch (c)
    {
    case '\n':			// End of row
      // Shouldn't happen, but we may get old-style, newline-terminated lines
      i = stop;
      break;

    case '\\':			// Escape sequence
      {
        const char n = Line[++i];
        if (i >= Line.size())
          throw failure{"Row ends in backslash."};

	switch (n)
	{
	case 'N':	// Null value
	  if (not R.empty())
	    throw failure{"Null sequence found in nonempty field."};
	  R = NullStr();
	  isnull = true;
	  break;

	case '0':	// Octal sequence (3 digits)
	case '1':
	case '2':
	case '3':
	case '4':
	case '5':
	case '6':
	case '7':
          {
	    if ((i+2) >= Line.size())
	      throw failure{"Row ends in middle of octal value."};
	    const char n1 = Line[++i];
	    const char n2 = Line[++i];
	    if (not is_octalchar(n1) or not is_octalchar(n2))
	      throw failure{"Invalid octal in encoded table stream."};
	    R += char(
		(digit_to_number(n)<<6) |
		(digit_to_number(n1)<<3) |
		digit_to_number(n2));
          }
	  break;

	case 'b':
	  // TODO: Escape code?
	  R += char(8);
	  break;	// Backspace
	case 'v':
	  // TODO: Escape code?
	  R += char(11);
	  break;	// Vertical tab
	case 'f':
	  // TODO: Escape code?
	  R += char(12);
	  break;	// Form feed
	case 'n':
	  R += '\n';
	  break;	// Newline
	case 't':
	  R += '\t';
	  break;	// Tab
	case 'r':
	  R += '\r';
	  break;	// Carriage return;

	default:	// Self-escaped character
	  R += n;
	  // This may be a self-escaped tab that we thought was a terminator...
	  if (i == stop)
	  {
	    if ((i+1) >= Line.size())
	      throw internal_error{"COPY line ends in backslash."};
	    stop = findtab(Line, i+1);
	  }
	  break;
	}
      }
      break;

    default:
      R += c;
      break;
    }
  }
  ++i;

  if (isnull and (R.size() != NullStr().size()))
    throw failure{"Field contains data behind null sequence."};

  return R;
}
