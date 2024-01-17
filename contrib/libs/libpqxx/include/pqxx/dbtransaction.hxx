/** Definition of the pqxx::dbtransaction abstract base class.
 *
 * pqxx::dbransaction defines a real transaction on the database.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/dbtransaction instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_DBTRANSACTION
#define PQXX_H_DBTRANSACTION

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include "pqxx/transaction_base.hxx"

namespace pqxx
{

enum readwrite_policy
{
  read_only,
  read_write
};


/// Abstract base class responsible for bracketing a backend transaction.
/**
 * @ingroup transaction
 *
 * Use a dbtransaction-derived object such as "work" (transaction<>) to enclose
 * operations on a database in a single "unit of work."  This ensures that the
 * whole series of operations either succeeds as a whole or fails completely.
 * In no case will it leave half-finished work behind in the database.
 *
 * Once processing on a transaction has succeeded and any changes should be
 * allowed to become permanent in the database, call commit().  If something
 * has gone wrong and the changes should be forgotten, call abort() instead.
 * If you do neither, an implicit abort() is executed at destruction time.
 *
 * It is an error to abort a transaction that has already been committed, or to
 * commit a transaction that has already been aborted.  Aborting an already
 * aborted transaction or committing an already committed one has been allowed
 * to make errors easier to deal with.  Repeated aborts or commits have no
 * effect after the first one.
 *
 * Database transactions are not suitable for guarding long-running processes.
 * If your transaction code becomes too long or too complex, please consider
 * ways to break it up into smaller ones.  There's no easy, general way to do
 * this since application-specific considerations become important at this
 * point.
 *
 * The actual operations for beginning and committing/aborting the backend
 * transaction are implemented by a derived class.  The implementing concrete
 * class must also call Begin() and End() from its constructors and destructors,
 * respectively, and implement do_exec().
 */
class PQXX_LIBEXPORT PQXX_NOVTABLE dbtransaction : public transaction_base
{
public:
  virtual ~dbtransaction();

protected:
  dbtransaction(
	connection_base &,
	const std::string &IsolationString,
	readwrite_policy rw=read_write);

  explicit dbtransaction(
	connection_base &,
	bool direct=true,
	readwrite_policy rw=read_write);


  /// Start a transaction on the backend and set desired isolation level
  void start_backend_transaction();

  /// Sensible default implemented here: begin backend transaction
  virtual void do_begin() override;					//[t01]
  /// Sensible default implemented here: perform query
  virtual result do_exec(const char Query[]) override;
  /// To be implemented by derived class: commit backend transaction
  virtual void do_commit() override =0;
  /// Sensible default implemented here: abort backend transaction
  /** Default implementation does two things:
   * <ol>
   * <li>Clears the "connection reactivation avoidance counter"</li>
   * <li>Executes a ROLLBACK statement</li>
   * </ol>
   */
  virtual void do_abort() override;				//[t13]

  static std::string fullname(const std::string &ttype,
	const std::string &isolation);

private:
  /// Precomputed SQL command to run at start of this transaction
  std::string m_start_cmd;
};

} // namespace pqxx

#include "pqxx/compiler-internal-post.hxx"

#endif
