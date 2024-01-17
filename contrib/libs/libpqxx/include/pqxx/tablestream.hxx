/** Definition of the pqxx::tablestream class.
 *
 * pqxx::tablestream provides optimized batch access to a database table.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/tablestream instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_TABLESTREAM
#define PQXX_H_TABLESTREAM

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"
#include "pqxx/transaction_base.hxx"


namespace pqxx
{
/// Base class for obsolete tablereader/tablewriter classes.
/** @deprecated Use stream_from and stream_to instead.
 */
class PQXX_LIBEXPORT PQXX_NOVTABLE tablestream :
  public internal::transactionfocus
{
public:
  explicit tablestream(
	transaction_base &Trans,
	const std::string &Null=std::string{});
  virtual ~tablestream() noexcept =0;
  virtual void complete() =0;
protected:
  const std::string &NullStr() const { return m_null; }
  bool is_finished() const noexcept { return m_finished; }
  void base_close();
  template<typename ITER>
  static std::string columnlist(ITER colbegin, ITER colend);
private:
  std::string m_null;
  bool m_finished = false;

  tablestream() =delete;
  tablestream(const tablestream &) =delete;
  tablestream &operator=(const tablestream &) =delete;
};


template<typename ITER> inline
std::string tablestream::columnlist(ITER colbegin, ITER colend)
{
  return separated_list(",", colbegin, colend);
}
} // namespace pqxx

#include "pqxx/compiler-internal-post.hxx"
#endif
