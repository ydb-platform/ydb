/** Implementation of libpqxx STL-style cursor classes.
 *
 * These classes wrap SQL cursors in STL-like interfaces.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <iterator>

#include "pqxx/cursor"
#include "pqxx/result"
#include "pqxx/strconv"
#include "pqxx/transaction"

#include "pqxx/internal/gates/icursor_iterator-icursorstream.hxx"
#include "pqxx/internal/gates/icursorstream-icursor_iterator.hxx"

using namespace pqxx;
using namespace pqxx::internal;


pqxx::cursor_base::difference_type pqxx::cursor_base::all() noexcept
{
  // Implemented out-of-line so we don't fall afoul of Visual Studio defining
  // min() and max() macros, which turn this expression into malformed code:
  return std::numeric_limits<int>::max() - 1;
}


pqxx::cursor_base::difference_type cursor_base::backward_all() noexcept
{
  // Implemented out-of-line so we don't fall afoul of Visual Studio defining
  // min() and max() macros, which turn this expression into malformed code:
  return std::numeric_limits<int>::min() + 1;
}


pqxx::cursor_base::cursor_base(
	connection_base &context,
	const std::string &Name,
	bool embellish_name) :
  m_name{embellish_name ? context.adorn_name(Name) : Name}
{
}


result::size_type pqxx::internal::obtain_stateless_cursor_size(sql_cursor &cur)
{
  if (cur.endpos() == -1) cur.move(cursor_base::all());
  return result::size_type(cur.endpos() - 1);
}


result pqxx::internal::stateless_cursor_retrieve(
	sql_cursor &cur,
	result::difference_type size,
	result::difference_type begin_pos,
	result::difference_type end_pos)
{
  if (begin_pos < 0 or begin_pos > size)
    throw range_error{"Starting position out of range"};

  if (end_pos < -1) end_pos = -1;
  else if (end_pos > size) end_pos = size;

  if (begin_pos == end_pos) return cur.empty_result();

  const int direction = ((begin_pos < end_pos) ? 1 : -1);
  cur.move((begin_pos-direction) - (cur.pos()-1));
  return cur.fetch(end_pos - begin_pos);
}


pqxx::icursorstream::icursorstream(
    transaction_base &context,
    const std::string &query,
    const std::string &basename,
    difference_type sstride) :
  m_cur{context,
	query,
	basename,
	cursor_base::forward_only,
	cursor_base::read_only,
	cursor_base::owned,
	false},
  m_stride{sstride},
  m_realpos{0},
  m_reqpos{0},
  m_iterators{nullptr},
  m_done{false}
{
  set_stride(sstride);
}


pqxx::icursorstream::icursorstream(
    transaction_base &context,
    const field &cname,
    difference_type sstride,
    cursor_base::ownershippolicy op) :
  m_cur{context, cname.c_str(), op},
  m_stride{sstride},
  m_realpos{0},
  m_reqpos{0},
  m_iterators{nullptr},
  m_done{false}
{
  set_stride(sstride);
}


void pqxx::icursorstream::set_stride(difference_type n)
{
  if (n < 1)
    throw argument_error{"Attempt to set cursor stride to " + to_string(n)};
  m_stride = n;
}

result pqxx::icursorstream::fetchblock()
{
  const result r{m_cur.fetch(m_stride)};
  m_realpos += r.size();
  if (r.empty()) m_done = true;
  return r;
}


icursorstream &pqxx::icursorstream::ignore(std::streamsize n)
{
  auto offset = m_cur.move(difference_type(n));
  m_realpos += offset;
  if (offset < n) m_done = true;
  return *this;
}


icursorstream::size_type pqxx::icursorstream::forward(size_type n)
{
  m_reqpos += difference_type(n) * m_stride;
  return icursorstream::size_type(m_reqpos);
}


void pqxx::icursorstream::insert_iterator(icursor_iterator *i) noexcept
{
  gate::icursor_iterator_icursorstream{*i}.set_next(m_iterators);
  if (m_iterators)
    gate::icursor_iterator_icursorstream{*m_iterators}.set_prev(i);
  m_iterators = i;
}


void pqxx::icursorstream::remove_iterator(icursor_iterator *i) const noexcept
{
  gate::icursor_iterator_icursorstream igate{*i};
  if (i == m_iterators)
  {
    m_iterators = igate.get_next();
    if (m_iterators)
      gate::icursor_iterator_icursorstream{*m_iterators}.set_prev(nullptr);
  }
  else
  {
    auto prev = igate.get_prev(), next = igate.get_next();
    gate::icursor_iterator_icursorstream{*prev}.set_next(next);
    if (next) gate::icursor_iterator_icursorstream{*next}.set_prev(prev);
  }
  igate.set_prev(nullptr);
  igate.set_next(nullptr);
}


void pqxx::icursorstream::service_iterators(difference_type topos)
{
  if (topos < m_realpos) return;

  using todolist = std::multimap<difference_type,icursor_iterator*>;
  todolist todo;
  for (icursor_iterator *i = m_iterators, *next; i; i = next)
  {
    gate::icursor_iterator_icursorstream gate{*i};
    const auto ipos = gate.pos();
    if (ipos >= m_realpos and ipos <= topos)
      todo.insert(todolist::value_type(ipos, i));
    next = gate.get_next();
  }
  const auto todo_end = std::end(todo);
  for (auto i = std::begin(todo); i != todo_end; )
  {
    const auto readpos = i->first;
    if (readpos > m_realpos) ignore(readpos - m_realpos);
    const result r = fetchblock();
    for ( ; i != todo_end and i->first == readpos; ++i)
      gate::icursor_iterator_icursorstream{*i->second}.fill(r);
  }
}


pqxx::icursor_iterator::icursor_iterator() noexcept :
  m_pos{0}
{
}


pqxx::icursor_iterator::icursor_iterator(istream_type &s) noexcept :
  m_stream{&s},
  m_pos{difference_type(gate::icursorstream_icursor_iterator(s).forward(0))}
{
  gate::icursorstream_icursor_iterator{*m_stream}.insert_iterator(this);
}


pqxx::icursor_iterator::icursor_iterator(const icursor_iterator &rhs)
	noexcept :
  m_stream{rhs.m_stream},
  m_here{rhs.m_here},
  m_pos{rhs.m_pos}
{
  if (m_stream)
    gate::icursorstream_icursor_iterator{*m_stream}.insert_iterator(this);
}


pqxx::icursor_iterator::~icursor_iterator() noexcept
{
  if (m_stream)
    gate::icursorstream_icursor_iterator{*m_stream}.remove_iterator(this);
}


icursor_iterator pqxx::icursor_iterator::operator++(int)
{
  icursor_iterator old{*this};
  m_pos = difference_type(
	gate::icursorstream_icursor_iterator{*m_stream}.forward());
  m_here.clear();
  return old;
}


icursor_iterator &pqxx::icursor_iterator::operator++()
{
  m_pos = difference_type(
	gate::icursorstream_icursor_iterator{*m_stream}.forward());
  m_here.clear();
  return *this;
}


icursor_iterator &pqxx::icursor_iterator::operator+=(difference_type n)
{
  if (n <= 0)
  {
    if (n == 0) return *this;
    throw argument_error{"Advancing icursor_iterator by negative offset."};
  }
  m_pos = difference_type(
	gate::icursorstream_icursor_iterator{*m_stream}.forward(
		icursorstream::size_type(n)));
  m_here.clear();
  return *this;
}


icursor_iterator &
pqxx::icursor_iterator::operator=(const icursor_iterator &rhs) noexcept
{
  if (rhs.m_stream == m_stream)
  {
    m_here = rhs.m_here;
    m_pos = rhs.m_pos;
  }
  else
  {
    if (m_stream)
      gate::icursorstream_icursor_iterator{*m_stream}.remove_iterator(this);
    m_here = rhs.m_here;
    m_pos = rhs.m_pos;
    m_stream = rhs.m_stream;
    if (m_stream)
      gate::icursorstream_icursor_iterator{*m_stream}.insert_iterator(this);
  }
  return *this;
}


bool pqxx::icursor_iterator::operator==(const icursor_iterator &rhs) const
{
  if (m_stream == rhs.m_stream) return pos() == rhs.pos();
  if (m_stream and rhs.m_stream) return false;
  refresh();
  rhs.refresh();
  return m_here.empty() and rhs.m_here.empty();
}


bool pqxx::icursor_iterator::operator<(const icursor_iterator &rhs) const
{
  if (m_stream == rhs.m_stream) return pos() < rhs.pos();
  refresh();
  rhs.refresh();
  return not m_here.empty();
}


void pqxx::icursor_iterator::refresh() const
{
  if (m_stream)
    gate::icursorstream_icursor_iterator{*m_stream}.service_iterators(pos());
}


void pqxx::icursor_iterator::fill(const result &r)
{
  m_here = r;
}
