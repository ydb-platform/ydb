/** Implementation of the pqxx::result class and support classes.
 *
 * pqxx::result represents the set of result rows from a database query.
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

extern "C"
{
#include "libpq-fe.h"
}

#include "pqxx/except"
#include "pqxx/result"

#include "pqxx/internal/gates/result-row.hxx"


pqxx::row::row(result r, size_t i) noexcept :
  m_result{r},
  m_index{long(i)},
  m_end{internal::gate::result_row(r) ? r.columns() : 0}
{
}


pqxx::row::const_iterator pqxx::row::begin() const noexcept
{
  return const_iterator{*this, m_begin};
}


pqxx::row::const_iterator pqxx::row::cbegin() const noexcept
{
  return begin();
}


pqxx::row::const_iterator pqxx::row::end() const noexcept
{
  return const_iterator{*this, m_end};
}


pqxx::row::const_iterator pqxx::row::cend() const noexcept
{
  return end();
}


pqxx::row::reference pqxx::row::front() const noexcept
{
  return field{*this, m_begin};
}


pqxx::row::reference pqxx::row::back() const noexcept
{
  return field{*this, m_end - 1};
}


pqxx::row::const_reverse_iterator pqxx::row::rbegin() const
{
  return const_reverse_row_iterator{end()};
}


pqxx::row::const_reverse_iterator pqxx::row::crbegin() const
{
  return rbegin();
}


pqxx::row::const_reverse_iterator pqxx::row::rend() const
{
  return const_reverse_row_iterator{begin()};
}


pqxx::row::const_reverse_iterator pqxx::row::crend() const
{
  return rend();
}


bool pqxx::row::operator==(const row &rhs) const noexcept
{
  if (&rhs == this) return true;
  const auto s = size();
  if (rhs.size() != s) return false;
  // TODO: Depends on how null is handled!
  for (size_type i=0; i<s; ++i) if ((*this)[i] != rhs[i]) return false;
  return true;
}


pqxx::row::reference pqxx::row::operator[](size_type i) const noexcept
{
  return field{*this, m_begin + i};
}


pqxx::row::reference pqxx::row::operator[](int i) const noexcept
{
  return operator[](size_type(i));
}


pqxx::row::reference pqxx::row::operator[](const char f[]) const
{
  return at(f);
}


pqxx::row::reference pqxx::row::operator[](const std::string &s) const
{
  return operator[](s.c_str());
}


pqxx::row::reference pqxx::row::at(int i) const
{
  return at(size_type(i));
}


pqxx::row::reference pqxx::row::at(const std::string &s) const
{
  return at(s.c_str());
}


void pqxx::row::swap(row &rhs) noexcept
{
  const auto i = m_index;
  const auto b= m_begin;
  const auto e = m_end;
  m_result.swap(rhs.m_result);
  m_index = rhs.m_index;
  m_begin = rhs.m_begin;
  m_end = rhs.m_end;
  rhs.m_index = i;
  rhs.m_begin = b;
  rhs.m_end = e;
}


pqxx::field pqxx::row::at(const char f[]) const
{
  return field{*this, m_begin + column_number(f)};
}


pqxx::field pqxx::row::at(pqxx::row::size_type i) const
{
  if (i >= size())
    throw range_error{"Invalid field number."};

  return operator[](i);
}


pqxx::oid pqxx::row::column_type(size_type ColNum) const
{
  return m_result.column_type(m_begin + ColNum);
}


pqxx::oid pqxx::row::column_table(size_type ColNum) const
{
  return m_result.column_table(m_begin + ColNum);
}


pqxx::row::size_type pqxx::row::table_column(size_type ColNum) const
{
  return m_result.table_column(m_begin + ColNum);
}


pqxx::row::size_type pqxx::row::column_number(const char ColName[]) const
{
  const auto n = m_result.column_number(ColName);
  if (n >= m_end)
    return result{}.column_number(ColName);
  if (n >= m_begin)
    return n - m_begin;

  const char *const AdaptedColName = m_result.column_name(n);
  for (auto i = m_begin; i < m_end; ++i)
    if (strcmp(AdaptedColName, m_result.column_name(i)) == 0)
      return i - m_begin;

  return result{}.column_number(ColName);
}


pqxx::row::size_type pqxx::result::column_number(const char ColName[]) const
{
  const int N = PQfnumber(
	const_cast<internal::pq::PGresult *>(m_data.get()), ColName);
  if (N == -1)
    throw argument_error{
	"Unknown column name: '" + std::string{ColName} + "'."};

  return row::size_type(N);
}


pqxx::row pqxx::row::slice(size_type Begin, size_type End) const
{
  if (Begin > End or End > size())
    throw range_error{"Invalid field range."};

  row result{*this};
  result.m_begin = m_begin + Begin;
  result.m_end = m_begin + End;
  return result;
}


bool pqxx::row::empty() const noexcept
{
  return m_begin == m_end;
}


pqxx::const_row_iterator pqxx::const_row_iterator::operator++(int)
{
  const_row_iterator old{*this};
  m_col++;
  return old;
}


pqxx::const_row_iterator pqxx::const_row_iterator::operator--(int)
{
  const_row_iterator old{*this};
  m_col--;
  return old;
}


pqxx::const_row_iterator
pqxx::const_reverse_row_iterator::base() const noexcept
{
  iterator_type tmp{*this};
  return ++tmp;
}


pqxx::const_reverse_row_iterator
pqxx::const_reverse_row_iterator::operator++(int)
{
  const_reverse_row_iterator tmp{*this};
  operator++();
  return tmp;
}


pqxx::const_reverse_row_iterator
pqxx::const_reverse_row_iterator::operator--(int)
{
  const_reverse_row_iterator tmp{*this};
  operator--();
  return tmp;
}
