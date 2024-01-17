/** Implementation of the pqxx::result class and support classes.
 *
 * pqxx::result represents the set of result rows from a database query
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <cstdlib>
#include <cstring>
#include <stdexcept>

extern "C"
{
#include "libpq-fe.h"
}

#include "pqxx/except"
#include "pqxx/result"


const std::string pqxx::result::s_empty_string;


/// C++ wrapper for libpq's PQclear.
void pqxx::internal::clear_result(const pq::PGresult *data)
{
  PQclear(const_cast<pq::PGresult *>(data));
}


pqxx::result::result(
	pqxx::internal::pq::PGresult *rhs,
	const std::string &Query,
        internal::encoding_group enc) :
  m_data{make_data_pointer(rhs)},
  m_query{std::make_shared<std::string>(Query)},
  m_encoding(enc)
{
}


bool pqxx::result::operator==(const result &rhs) const noexcept
{
  if (&rhs == this) return true;
  const auto s = size();
  if (rhs.size() != s) return false;
  for (size_type i=0; i<s; ++i)
    if ((*this)[i] != rhs[i]) return false;
  return true;
}


pqxx::result::const_reverse_iterator pqxx::result::rbegin() const
{
  return const_reverse_iterator{end()};
}


pqxx::result::const_reverse_iterator pqxx::result::crbegin() const
{
  return rbegin();
}


pqxx::result::const_reverse_iterator pqxx::result::rend() const
{
  return const_reverse_iterator{begin()};
}


pqxx::result::const_reverse_iterator pqxx::result::crend() const
{
  return rend();
}


pqxx::result::const_iterator pqxx::result::begin() const noexcept
{
  return const_iterator{this, 0};
}


pqxx::result::const_iterator pqxx::result::cbegin() const noexcept
{
  return begin();
}


pqxx::result::size_type pqxx::result::size() const noexcept
{
  return m_data.get() ? size_type(PQntuples(m_data.get())) : 0;
}


bool pqxx::result::empty() const noexcept
{
  return (m_data.get() == nullptr) or (PQntuples(m_data.get()) == 0);
}


pqxx::result::reference pqxx::result::front() const noexcept
{
  return row{*this, 0};
}


pqxx::result::reference pqxx::result::back() const noexcept
{
  return row{*this, size() - 1};
}


void pqxx::result::swap(result &rhs) noexcept
{
  m_data.swap(rhs.m_data);
  m_query.swap(rhs.m_query);
}


const pqxx::row pqxx::result::operator[](result_size_type i) const noexcept
{
  return row{*this, i};
}


const pqxx::row pqxx::result::at(pqxx::result::size_type i) const
{
  if (i >= size()) throw range_error{"Row number out of range."};
  return operator[](i);
}


namespace
{
/// C string comparison.
inline bool equal(const char lhs[], const char rhs[])
{
  return strcmp(lhs, rhs) == 0;
}
} // namespace

void pqxx::result::ThrowSQLError(
	const std::string &Err,
	const std::string &Query) const
{
  // Try to establish more precise error type, and throw corresponding
  // type of exception.
  const char *const code = PQresultErrorField(m_data.get(), PG_DIAG_SQLSTATE);
  if (code) switch (code[0])
  {
  case '0':
    switch (code[1])
    {
    case '8':
      throw broken_connection{Err};
    case 'A':
      throw feature_not_supported{Err, Query, code};
    }
    break;
  case '2':
    switch (code[1])
    {
    case '2':
      throw data_exception{Err, Query, code};
    case '3':
      if (equal(code,"23001")) throw restrict_violation{Err, Query, code};
      if (equal(code,"23502")) throw not_null_violation{Err, Query, code};
      if (equal(code,"23503"))
        throw foreign_key_violation{Err, Query, code};
      if (equal(code,"23505")) throw unique_violation{Err, Query, code};
      if (equal(code,"23514")) throw check_violation{Err, Query, code};
      throw integrity_constraint_violation{Err, Query, code};
    case '4':
      throw invalid_cursor_state{Err, Query, code};
    case '6':
      throw invalid_sql_statement_name{Err, Query, code};
    }
    break;
  case '3':
    switch (code[1])
    {
    case '4':
      throw invalid_cursor_name{Err, Query, code};
    }
    break;
  case '4':
    switch (code[1])
    {
    case '0':
      if (equal(code, "40000")) throw transaction_rollback{Err};
      if (equal(code, "40001")) throw serialization_failure{Err};
      if (equal(code, "40003")) throw statement_completion_unknown{Err};
      if (equal(code, "40P01")) throw deadlock_detected{Err};
      break;
    case '2':
      if (equal(code,"42501")) throw insufficient_privilege{Err, Query};
      if (equal(code,"42601"))
        throw syntax_error{Err, Query, code, errorposition()};
      if (equal(code,"42703")) throw undefined_column{Err, Query, code};
      if (equal(code,"42883")) throw undefined_function{Err, Query, code};
      if (equal(code,"42P01")) throw undefined_table{Err, Query, code};
    }
    break;
  case '5':
    switch (code[1])
    {
    case '3':
      if (equal(code,"53100")) throw disk_full{Err, Query, code};
      if (equal(code,"53200")) throw out_of_memory{Err, Query, code};
      if (equal(code,"53300")) throw too_many_connections{Err};
      throw insufficient_resources{Err, Query, code};
    }
    break;

  case 'P':
    if (equal(code, "P0001")) throw plpgsql_raise{Err, Query, code};
    if (equal(code, "P0002"))
      throw plpgsql_no_data_found{Err, Query, code};
    if (equal(code, "P0003"))
      throw plpgsql_too_many_rows{Err, Query, code};
    throw plpgsql_error{Err, Query, code};
  }
  // Fallback: No error code.
  throw sql_error{Err, Query, code};
}

void pqxx::result::check_status() const
{
  const std::string Err = StatusError();
  if (not Err.empty()) ThrowSQLError(Err, query());
}


std::string pqxx::result::StatusError() const
{
  if (m_data.get() == nullptr) throw failure{"No result set given."};

  std::string Err;

  switch (PQresultStatus(m_data.get()))
  {
  case PGRES_EMPTY_QUERY: // The string sent to the backend was empty.
  case PGRES_COMMAND_OK: // Successful completion of a command returning no data
  case PGRES_TUPLES_OK: // The query successfully executed
    break;

  case PGRES_COPY_OUT: // Copy Out (from server) data transfer started
  case PGRES_COPY_IN: // Copy In (to server) data transfer started
    break;

  case PGRES_BAD_RESPONSE: // The server's response was not understood
  case PGRES_NONFATAL_ERROR:
  case PGRES_FATAL_ERROR:
    Err = PQresultErrorMessage(m_data.get());
    break;

  default:
    throw internal_error{
	"pqxx::result: Unrecognized response code " +
	to_string(int(PQresultStatus(m_data.get())))};
  }
  return Err;
}


const char *pqxx::result::cmd_status() const noexcept
{
  return PQcmdStatus(const_cast<internal::pq::PGresult *>(m_data.get()));
}


const std::string &pqxx::result::query() const noexcept
{
  return m_query ? *m_query : s_empty_string;
}


pqxx::oid pqxx::result::inserted_oid() const
{
  if (m_data.get() == nullptr)
    throw usage_error{
	"Attempt to read oid of inserted row without an INSERT result"};
  return PQoidValue(const_cast<internal::pq::PGresult *>(m_data.get()));
}


pqxx::result::size_type pqxx::result::affected_rows() const
{
  const char *const RowsStr = PQcmdTuples(
	const_cast<internal::pq::PGresult *>(m_data.get()));
  return RowsStr[0] ? size_type(atoi(RowsStr)) : 0;
}


const char *pqxx::result::GetValue(
	pqxx::result::size_type Row,
	pqxx::row::size_type Col) const
{
  return PQgetvalue(m_data.get(), int(Row), int(Col));
}


bool pqxx::result::get_is_null(
	pqxx::result::size_type Row,
	pqxx::row::size_type Col) const
{
  return PQgetisnull(m_data.get(), int(Row), int(Col)) != 0;
}

pqxx::field::size_type pqxx::result::get_length(
	pqxx::result::size_type Row,
        pqxx::row::size_type Col) const noexcept
{
  return field::size_type(PQgetlength(m_data.get(), int(Row), int(Col)));
}


pqxx::oid pqxx::result::column_type(row::size_type ColNum) const
{
  const oid T = PQftype(m_data.get(), int(ColNum));
  if (T == oid_none)
    throw argument_error{
	"Attempt to retrieve type of nonexistent column " +
	to_string(ColNum) + " of query result."};
  return T;
}


pqxx::oid pqxx::result::column_table(row::size_type ColNum) const
{
  const oid T = PQftable(m_data.get(), int(ColNum));

  /* If we get oid_none, it may be because the column is computed, or because we
   * got an invalid row number.
   */
  if (T == oid_none and ColNum >= columns())
    throw argument_error{
	"Attempt to retrieve table ID for column " + to_string(ColNum) +
	" out of " + to_string(columns())};

  return T;
}


pqxx::row::size_type pqxx::result::table_column(row::size_type ColNum) const
{
  const auto n = row::size_type(PQftablecol(m_data.get(), int(ColNum)));
  if (n != 0) return n-1;

  // Failed.  Now find out why, so we can throw a sensible exception.
  const std::string col_num = to_string(ColNum);
  if (ColNum > columns())
    throw range_error{"Invalid column index in table_column(): " + col_num};

  if (m_data.get() == nullptr)
    throw usage_error{
      "Can't query origin of column " + col_num + ": "
      "result is not initialized."};

  throw usage_error{
    "Can't query origin of column " + col_num + ": "
    "not derived from table column."};
}

int pqxx::result::errorposition() const
{
  int pos = -1;
  if (m_data.get())
  {
    const char *p = PQresultErrorField(
	const_cast<internal::pq::PGresult *>(m_data.get()),
	PG_DIAG_STATEMENT_POSITION);
    if (p) from_string(p, pos);
  }
  return pos;
}


const char *pqxx::result::column_name(pqxx::row::size_type Number) const
{
  const char *const N = PQfname(m_data.get(), int(Number));
  if (N == nullptr)
  {
    if (m_data.get() == nullptr)
      throw usage_error{"Queried column name on null result."};
    throw range_error{
	"Invalid column number: " + to_string(Number) +
	" (maximum is " + to_string(columns() - 1) + ")."};
  }
  return N;
}


pqxx::row::size_type pqxx::result::columns() const noexcept
{
  auto ptr = const_cast<internal::pq::PGresult *>(m_data.get());
  return ptr ? row::size_type(PQnfields(ptr)) : 0;
}


// const_result_iterator

pqxx::const_result_iterator pqxx::const_result_iterator::operator++(int)
{
  const_result_iterator old{*this};
  m_index++;
  return old;
}


pqxx::const_result_iterator pqxx::const_result_iterator::operator--(int)
{
  const_result_iterator old{*this};
  m_index--;
  return old;
}


pqxx::result::const_iterator
pqxx::result::const_reverse_iterator::base() const noexcept
{
  iterator_type tmp{*this};
  return ++tmp;
}


pqxx::const_reverse_result_iterator
pqxx::const_reverse_result_iterator::operator++(int)
{
  const_reverse_result_iterator tmp{*this};
  iterator_type::operator--();
  return tmp;
}


pqxx::const_reverse_result_iterator
pqxx::const_reverse_result_iterator::operator--(int)
{
  const_reverse_result_iterator tmp{*this};
  iterator_type::operator++();
  return tmp;
}


template<>
std::string pqxx::to_string(const field &Obj)
{
  return std::string{Obj.c_str(), Obj.size()};
}
