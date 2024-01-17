/** Definition of the pqxx::tablereader class.
 *
 * pqxx::tablereader enables optimized batch reads from a database table.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/tablereader instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_TABLEREADER
#define PQXX_H_TABLEREADER

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"
#include "pqxx/result.hxx"
#include "pqxx/tablestream.hxx"


namespace pqxx
{
/// @deprecated Use stream_from instead.
/** Efficiently pull data directly out of a table.
 * @warning This class does not work reliably with multibyte encodings.  Using
 * it with some multi-byte encodings may pose a security risk.
 */
class PQXX_LIBEXPORT tablereader : public tablestream
{
public:
  PQXX_DEPRECATED tablereader(
	transaction_base &,
	const std::string &Name,
	const std::string &Null=std::string{});
  template<typename ITER>
  PQXX_DEPRECATED tablereader(
	transaction_base &,
	const std::string &Name,
	ITER begincolumns,
	ITER endcolumns);
  template<typename ITER>
  PQXX_DEPRECATED tablereader(
	transaction_base &,
	const std::string &Name,
	ITER begincolumns,
	ITER endcolumns,
	const std::string &Null);
  ~tablereader() noexcept;
  template<typename TUPLE> tablereader &operator>>(TUPLE &);
  operator bool() const noexcept { return not m_done; }
  bool operator!() const noexcept { return m_done; }
  bool get_raw_line(std::string &Line);
  template<typename TUPLE>
  void tokenize(std::string, TUPLE &) const;
  virtual void complete() override;
private:
  void set_up(
	transaction_base &T,
	const std::string &RName,
	const std::string &Columns=std::string{});
  PQXX_PRIVATE void reader_close();
  std::string extract_field(
	const std::string &,
	std::string::size_type &) const;
  bool m_done;
};


template<typename ITER> inline
tablereader::tablereader(
	transaction_base &T,
	const std::string &Name,
	ITER begincolumns,
	ITER endcolumns) :
  namedclass{Name, "tablereader"},
  tablestream{T, std::string{}},
  m_done{true}
{
  set_up(T, Name, columnlist(begincolumns, endcolumns));
}


template<typename ITER> inline
tablereader::tablereader(
	transaction_base &T,
	const std::string &Name,
	ITER begincolumns,
	ITER endcolumns,
	const std::string &Null) :
  namedclass{Name, "tablereader"},
  tablestream{T, Null},
  m_done{true}
{
  set_up(T, Name, columnlist(begincolumns, endcolumns));
}


template<typename TUPLE>
inline void tablereader::tokenize(std::string Line, TUPLE &T) const
{
  std::back_insert_iterator<TUPLE> ins = std::back_inserter(T);
  std::string::size_type here = 0;
  while (here < Line.size()) *ins++ = extract_field(Line, here);
}


template<typename TUPLE>
inline tablereader &pqxx::tablereader::operator>>(TUPLE &T)
{
  std::string Line;
  if (get_raw_line(Line)) tokenize(Line, T);
  return *this;
}
} // namespace pqxx

#include "pqxx/compiler-internal-post.hxx"
#endif
