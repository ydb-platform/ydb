/** Definition of the pqxx::transaction class.
 * pqxx::transaction represents a standard database transaction.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/transaction instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_TRANSACTION
#define PQXX_H_TRANSACTION

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include "pqxx/dbtransaction.hxx"


/* Methods tested in eg. self-test program test1 are marked with "//[t01]"
 */


namespace pqxx
{

namespace internal
{
/// Helper base class for the @c transaction class template.
class PQXX_LIBEXPORT basic_transaction : public dbtransaction
{
protected:
  basic_transaction(							//[t01]
	connection_base &C,
	const std::string &IsolationLevel,
	readwrite_policy);

private:
  virtual void do_commit() override;					//[t01]
};
} // namespace internal


/**
 * @ingroup transaction
 */
//@{

/// Standard back-end transaction, templatized on isolation level
/** This is the type you'll normally want to use to represent a transaction on
 * the database.
 *
 * While you may choose to create your own transaction object to interface to
 * the database backend, it is recommended that you wrap your transaction code
 * into a transactor code instead and let the transaction be created for you.
 * @see pqxx/transactor.hxx
 *
 * If you should find that using a transactor makes your code less portable or
 * too complex, go ahead, create your own transaction anyway.
 *
 * Usage example: double all wages
 *
 * @code
 * extern connection C;
 * work T(C);
 * try
 * {
 *   T.exec("UPDATE employees SET wage=wage*2");
 *   T.commit();	// NOTE: do this inside try block
 * }
 * catch (const exception &e)
 * {
 *   cerr << e.what() << endl;
 *   T.abort();		// Usually not needed; same happens when T's life ends.
 * }
 * @endcode
 */
template<
	isolation_level ISOLATIONLEVEL=read_committed,
	readwrite_policy READWRITE=read_write>
class transaction : public internal::basic_transaction
{
public:
  using isolation_tag = isolation_traits<ISOLATIONLEVEL>;

  /// Create a transaction.
  /**
   * @param C Connection for this transaction to operate on
   * @param TName Optional name for transaction; must begin with a letter and
   * may contain letters and digits only
   */
  explicit transaction(connection_base &C, const std::string &TName):	//[t01]
    namedclass{fullname("transaction", isolation_tag::name()), TName},
    internal::basic_transaction(C, isolation_tag::name(), READWRITE)
	{ Begin(); }

  explicit transaction(connection_base &C) :				//[t01]
    transaction(C, "") {}

  virtual ~transaction() noexcept
	{ End(); }
};


/// The default transaction type.
using work = transaction<>;

/// Read-only transaction.
using read_transaction = transaction<read_committed, read_only>;

//@}
}

#include "pqxx/compiler-internal-post.hxx"
#endif
