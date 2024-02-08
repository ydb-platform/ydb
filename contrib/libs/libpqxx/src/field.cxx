/** Implementation of the pqxx::field class.
 *
 * pqxx::field refers to a field in a query result.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <cstring>

#include "pqxx/internal/libpq-forward.hxx"

#include "pqxx/result"


pqxx::field::field(const pqxx::row &R, pqxx::row::size_type C) noexcept :
  m_col{static_cast<decltype(m_col)>(C)},
  m_home{R.m_result},
  m_row{pqxx::result_size_type(R.m_index)}
{
}


bool pqxx::field::operator==(const field &rhs) const
{
  if (is_null() != rhs.is_null()) return false;
  // TODO: Verify null handling decision
  const size_type s = size();
  if (s != rhs.size()) return false;
  return std::memcmp(c_str(), rhs.c_str(), s) == 0;
}


const char *pqxx::field::name() const
{
  return home().column_name(col());
}


pqxx::oid pqxx::field::type() const
{
  return home().column_type(col());
}


pqxx::oid pqxx::field::table() const
{
  return home().column_table(col());
}


pqxx::row::size_type pqxx::field::table_column() const
{
  return home().table_column(col());
}


const char *pqxx::field::c_str() const
{
  return home().GetValue(idx(), col());
}


bool pqxx::field::is_null() const noexcept
{
  return home().get_is_null(idx(), col());
}


pqxx::field::size_type pqxx::field::size() const noexcept
{
  return home().get_length(idx(), col());
}
