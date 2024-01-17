/** Definition of the pqxx::subtransaction class.
 *
 * pqxx::subtransaction is a nested transaction, i.e. one within a transaction.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/subtransaction instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_SUBTRANSACTION
#define PQXX_H_SUBTRANSACTION

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include "pqxx/dbtransaction.hxx"


/* Methods tested in eg. self-test program test1 are marked with "//[t01]"
 */


namespace pqxx
{

/**
 * @ingroup transaction
 */
/// "Transaction" nested within another transaction
/** A subtransaction can be executed inside a backend transaction, or inside
 * another subtransaction.  This can be useful when, for example, statements in
 * a transaction may harmlessly fail and you don't want them to abort the entire
 * transaction.  Here's an example of how a temporary table may be dropped
 * before re-creating it, without failing if the table did not exist:
 *
 * @code
 * void do_job(connection_base &C)
 * {
 *   const string temptable = "fleetingtable";
 *
 *   // Since we're dealing with a temporary table here, disallow automatic
 *   // recovery of the connection in case it breaks.
 *   C.inhibit_reactivation(true);
 *
 *   work W(C, "do_job");
 *   do_firstpart(W);
 *
 *   // Attempt to delete our temporary table if it already existed
 *   try
 *   {
 *     subtransaction S(W, "droptemp");
 *     S.exec0("DROP TABLE " + temptable);
 *     S.commit();
 *   }
 *   catch (const undefined_table &)
 *   {
 *     // Table did not exist.  Which is what we were hoping to achieve anyway.
 *     // Carry on without regrets.
 *   }
 *
 *   // S may have gone into a failed state and been destroyed, but the
 *   // upper-level transaction W is still fine.  We can continue to use it.
 *   W.exec("CREATE TEMP TABLE " + temptable + "(bar integer, splat varchar)");
 *
 *   do_lastpart(W);
 * }
 * @endcode
 *
 * (This is just an example.  If you really wanted to do drop a table without an
 * error if it doesn't exist, you'd use DROP TABLE IF EXISTS.)
 *
 * There are no isolation levels inside a transaction.  They are not needed
 * because all actions within the same backend transaction are always performed
 * sequentially anyway.
 */
class PQXX_LIBEXPORT subtransaction :
  public internal::transactionfocus,
  public dbtransaction
{
public:
  /// Nest a subtransaction nested in another transaction.
  explicit subtransaction(						//[t88]
	dbtransaction &T, const std::string &Name=std::string{});

  /// Nest a subtransaction in another subtransaction.
  explicit subtransaction(
	subtransaction &T, const std::string &Name=std::string{});

  virtual ~subtransaction() noexcept
	{ End(); }

private:
  virtual void do_begin() override;					//[t88]
  virtual void do_commit() override;					//[t88]
  virtual void do_abort() override;					//[t88]

  dbtransaction &m_parent;
};
}

#include "pqxx/compiler-internal-post.hxx"
#endif
