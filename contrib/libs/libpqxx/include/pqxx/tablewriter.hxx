/** Definition of the pqxx::tablewriter class.
 *
 * pqxx::tablewriter enables optimized batch updates to a database table.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/tablewriter.hxx instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_TABLEWRITER
#define PQXX_H_TABLEWRITER

#include <iterator>

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include "pqxx/tablestream.hxx"


namespace pqxx
{
/// @deprecated Use stream_to instead.
/** Efficiently write data directly to a database table.
 * @warning This class does not work reliably with multibyte encodings.  Using
 * it with some multi-byte encodings may pose a security risk.
 */
class PQXX_LIBEXPORT tablewriter : public tablestream
{
public:
  PQXX_DEPRECATED tablewriter(
	transaction_base &,
	const std::string &WName,
	const std::string &Null=std::string{});
  template<typename ITER>
        PQXX_DEPRECATED tablewriter(
	transaction_base &,
	const std::string &WName,
	ITER begincolumns,
	ITER endcolumns);
  template<typename ITER>
        PQXX_DEPRECATED tablewriter(
	transaction_base &T,
	const std::string &WName,
	ITER begincolumns,
	ITER endcolumns,
	const std::string &Null);
  ~tablewriter() noexcept;
  template<typename IT> void insert(IT Begin, IT End);
  template<typename TUPLE> void insert(const TUPLE &);
  template<typename IT> void push_back(IT Begin, IT End);
  template<typename TUPLE> void push_back(const TUPLE &);
  template<typename SIZE> void reserve(SIZE) {}
  template<typename TUPLE> tablewriter &operator<<(const TUPLE &);
  tablewriter &operator<<(tablereader &);
  template<typename IT> std::string generate(IT Begin, IT End) const;
  template<typename TUPLE> std::string generate(const TUPLE &) const;
  virtual void complete() override;
  void write_raw_line(const std::string &);
private:
  void set_up(
	transaction_base &,
	const std::string &WName,
	const std::string &Columns = std::string{});
  PQXX_PRIVATE void writer_close();
};
} // namespace pqxx


namespace std
{
template<>
  class back_insert_iterator<pqxx::tablewriter>
{
public:
  using iterator_category = output_iterator_tag;

  explicit back_insert_iterator(pqxx::tablewriter &W) noexcept :
    m_writer{&W} {}

  back_insert_iterator &
    operator=(const back_insert_iterator &rhs) noexcept
  {
    m_writer = rhs.m_writer;
    return *this;
  }

  template<typename TUPLE>
  back_insert_iterator &operator=(const TUPLE &T)
  {
    m_writer->insert(T);
    return *this;
  }

  back_insert_iterator &operator++() { return *this; }
  back_insert_iterator &operator++(int) { return *this; }
  back_insert_iterator &operator*() { return *this; }

private:
  pqxx::tablewriter *m_writer;
};
} // namespace std


namespace pqxx
{
template<typename ITER> inline tablewriter::tablewriter(
	transaction_base &T,
	const std::string &WName,
	ITER begincolumns,
	ITER endcolumns) :
  namedclass{"tablewriter", WName},
  tablestream{T, std::string{}}
{
  set_up(T, WName, columnlist(begincolumns, endcolumns));
}


template<typename ITER> inline tablewriter::tablewriter(
	transaction_base &T,
	const std::string &WName,
	ITER begincolumns,
	ITER endcolumns,
	const std::string &Null) :
  namedclass{"tablewriter", WName},
  tablestream{T, Null}
{
  set_up(T, WName, columnlist(begincolumns, endcolumns));
}


namespace internal
{
PQXX_LIBEXPORT std::string escape(
	const std::string &s,
	const std::string &null);

inline std::string escape_any(
	const std::string &s,
	const std::string &null)
{ return escape(s, null); }

inline std::string escape_any(
	const char s[],
	const std::string &null)
{ return s ? escape(std::string{s}, null) : "\\N"; }

template<typename T> inline std::string escape_any(
	const T &t,
	const std::string &null)
{ return escape(to_string(t), null); }


template<typename IT> class Escaper
{
  const std::string &m_null;
public:
  explicit Escaper(const std::string &null) : m_null{null} {}
  std::string operator()(IT i) const { return escape_any(*i, m_null); }
};
}


template<typename IT>
inline std::string tablewriter::generate(IT Begin, IT End) const
{
  return separated_list("\t", Begin, End, internal::Escaper<IT>{NullStr()});
}
template<typename TUPLE>
inline std::string tablewriter::generate(const TUPLE &T) const
{
  return generate(std::begin(T), std::end(T));
}

template<typename IT> inline void tablewriter::insert(IT Begin, IT End)
{
  write_raw_line(generate(Begin, End));
}

template<typename TUPLE> inline void tablewriter::insert(const TUPLE &T)
{
  insert(std::begin(T), std::end(T));
}

template<typename IT>
inline void tablewriter::push_back(IT Begin, IT End)
{
  insert(Begin, End);
}

template<typename TUPLE>
inline void tablewriter::push_back(const TUPLE &T)
{
  insert(std::begin(T), std::end(T));
}

template<typename TUPLE>
inline tablewriter &tablewriter::operator<<(const TUPLE &T)
{
  insert(T);
  return *this;
}

} // namespace pqxx
#include "pqxx/compiler-internal-post.hxx"
#endif
