/** Implementation of the pqxx::stream_to class.
 *
 * pqxx::stream_to enables optimized batch updates to a database table.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include "pqxx/stream_to.hxx"

#include "pqxx/internal/gates/transaction-stream_to.hxx"


pqxx::stream_to::stream_to(
  transaction_base &tb,
  const std::string &table_name
) :
  namedclass{"stream_to", table_name},
  stream_base{tb}
{
  set_up(tb, table_name);
}


pqxx::stream_to::~stream_to() noexcept
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


void pqxx::stream_to::complete()
{
  close();
}


void pqxx::stream_to::write_raw_line(const std::string &line)
{
  internal::gate::transaction_stream_to{m_trans}.write_copy_line(line);
}


pqxx::stream_to & pqxx::stream_to::operator<<(stream_from &tr)
{
  std::string line;
  while (tr)
  {
    tr.get_raw_line(line);
    write_raw_line(line);
  }
  return *this;
}


void pqxx::stream_to::set_up(
  transaction_base &tb,
  const std::string &table_name
)
{
  set_up(tb, table_name, "");
}


void pqxx::stream_to::set_up(
  transaction_base &tb,
  const std::string &table_name,
  const std::string &columns
)
{
  internal::gate::transaction_stream_to{tb}.BeginCopyWrite(
    table_name,
    columns
  );
  register_me();
}


void pqxx::stream_to::close()
{
  if (*this)
  {
    stream_base::close();
    try
    {
      internal::gate::transaction_stream_to{m_trans}.end_copy_write();
    }
    catch (const std::exception &)
    {
      try
      {
        stream_base::close();
      }
      catch (const std::exception &) {}
      throw;
    }
  }
}


std::string pqxx::internal::TypedCopyEscaper::escape(const std::string &s)
{
  if (s.empty())
    return s;

  std::string escaped;
  escaped.reserve(s.size()+1);

  for (auto c : s)
    switch (c)
    {
    case '\b': escaped += "\\b";  break; // Backspace
    case '\f': escaped += "\\f";  break; // Vertical tab
    case '\n': escaped += "\\n";  break; // Form feed
    case '\r': escaped += "\\r";  break; // Newline
    case '\t': escaped += "\\t";  break; // Tab
    case '\v': escaped += "\\v";  break; // Carriage return
    case '\\': escaped += "\\\\"; break; // Backslash
    default:
      if (c < ' ' or c > '~')
      {
        escaped += "\\";
        for (auto i = 2; i >= 0; --i)
          escaped += number_to_digit((c >> (3*i)) & 0x07);
      }
      else
        escaped += c;
      break;
    }

  return escaped;
}
