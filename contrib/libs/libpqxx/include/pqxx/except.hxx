/** Definition of libpqxx exception classes.
 *
 * pqxx::sql_error, pqxx::broken_connection, pqxx::in_doubt_error, ...
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/except instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_EXCEPT
#define PQXX_H_EXCEPT

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include <stdexcept>

#include "pqxx/util.hxx"


namespace pqxx
{

/**
 * @addtogroup exception Exception classes
 *
 * These exception classes follow, roughly, the two-level hierarchy defined by
 * the PostgreSQL error codes (see Appendix A of the PostgreSQL documentation
 * corresponding to your server version).  The hierarchy given here is, as yet,
 * not a complete mirror of the error codes.  There are some other differences
 * as well, e.g. the error code statement_completion_unknown has a separate
 * status in libpqxx as in_doubt_error, and too_many_connections is classified
 * as a broken_connection rather than a subtype of insufficient_resources.
 *
 * @see http://www.postgresql.org/docs/9.4/interactive/errcodes-appendix.html
 *
 * @{
 */

/// Mixin base class to identify libpqxx-specific exception types
/**
 * If you wish to catch all exception types specific to libpqxx for some reason,
 * catch this type.  All of libpqxx's exception classes are derived from it
 * through multiple-inheritance (they also fit into the standard library's
 * exception hierarchy in more fitting places).
 *
 * This class is not derived from std::exception, since that could easily lead
 * to exception classes with multiple std::exception base-class objects.  As
 * Bart Samwel points out, "catch" is subject to some nasty fineprint in such
 * cases.
 */
class PQXX_LIBEXPORT PQXX_NOVTABLE pqxx_exception
{
public:
  /// Support run-time polymorphism, and keep this class abstract
  virtual ~pqxx_exception() noexcept =0;

  /// Return std::exception base-class object
  /** Use this to get at the exception's what() function, or to downcast to a
   * more specific type using dynamic_cast.
   *
   * Casting directly from pqxx_exception to a specific exception type is not
   * likely to work since pqxx_exception is not (and could not safely be)
   * derived from std::exception.
   *
   * For example, to test dynamically whether an exception is an sql_error:
   *
   * @code
   * try
   * {
   *   // ...
   * }
   * catch (const pqxx::pqxx_exception &e)
   * {
   *   std::cerr << e.base().what() << std::endl;
   *   const pqxx::sql_error *s=dynamic_cast<const pqxx::sql_error*>(&e.base());
   *   if (s) std::cerr << "Query was: " << s->query() << std::endl;
   * }
   * @endcode
   */
  PQXX_CONST virtual const std::exception &base() const noexcept =0;	//[t00]
};


/// Run-time failure encountered by libpqxx, similar to std::runtime_error
class PQXX_LIBEXPORT failure :
  public pqxx_exception, public std::runtime_error
{
  virtual const std::exception &base() const noexcept override
	{ return *this; }
public:
  explicit failure(const std::string &);
};


/// Exception class for lost or failed backend connection.
/**
 * @warning When this happens on Unix-like systems, you may also get a SIGPIPE
 * signal.  That signal aborts the program by default, so if you wish to be able
 * to continue after a connection breaks, be sure to disarm this signal.
 *
 * If you're working on a Unix-like system, see the manual page for
 * @c signal (2) on how to deal with SIGPIPE.  The easiest way to make this
 * signal harmless is to make your program ignore it:
 *
 * @code
 * #include <signal.h>
 *
 * int main()
 * {
 *   signal(SIGPIPE, SIG_IGN);
 *   // ...
 * @endcode
 */
class PQXX_LIBEXPORT broken_connection : public failure
{
public:
  broken_connection();
  explicit broken_connection(const std::string &);
};


/// Exception class for failed queries.
/** Carries, in addition to a regular error message, a copy of the failed query
 * and (if available) the SQLSTATE value accompanying the error.
 */
class PQXX_LIBEXPORT sql_error : public failure
{
  /// Query string.  Empty if unknown.
  const std::string m_query;
  /// SQLSTATE string describing the error type, if known; or empty string.
  const std::string m_sqlstate;

public:
  explicit sql_error(
	const std::string &msg="",
	const std::string &Q="",
	const char sqlstate[]=nullptr);
  virtual ~sql_error() noexcept;

  /// The query whose execution triggered the exception
  PQXX_PURE const std::string &query() const noexcept;			//[t56]

  /// SQLSTATE error code if known, or empty string otherwise.
  PQXX_PURE const std::string &sqlstate() const noexcept;
};


/// "Help, I don't know whether transaction was committed successfully!"
/** Exception that might be thrown in rare cases where the connection to the
 * database is lost while finishing a database transaction, and there's no way
 * of telling whether it was actually executed by the backend.  In this case
 * the database is left in an indeterminate (but consistent) state, and only
 * manual inspection will tell which is the case.
 */
class PQXX_LIBEXPORT in_doubt_error : public failure
{
public:
  explicit in_doubt_error(const std::string &);
};


/// The backend saw itself forced to roll back the ongoing transaction.
class PQXX_LIBEXPORT transaction_rollback : public failure
{
public:
  explicit transaction_rollback(const std::string &);
};


/// Transaction failed to serialize.  Please retry it.
/** Can only happen at transaction isolation levels REPEATABLE READ and
 * SERIALIZABLE.
 *
 * The current transaction cannot be committed without violating the guarantees
 * made by its isolation level.  This is the effect of a conflict with another
 * ongoing transaction.  The transaction may still succeed if you try to
 * perform it again.
 */
class PQXX_LIBEXPORT serialization_failure : public transaction_rollback
{
public:
  explicit serialization_failure(const std::string &);
};


/// We can't tell whether our last statement succeeded.
class PQXX_LIBEXPORT statement_completion_unknown : public transaction_rollback
{
public:
  explicit statement_completion_unknown(const std::string &);
};


/// The ongoing transaction has deadlocked.  Retrying it may help.
class PQXX_LIBEXPORT deadlock_detected : public transaction_rollback
{
public:
  explicit deadlock_detected(const std::string &);
};


/// Internal error in libpqxx library
class PQXX_LIBEXPORT internal_error :
  public pqxx_exception, public std::logic_error
{
  virtual const std::exception &base() const noexcept override
	{ return *this; }
public:
  explicit internal_error(const std::string &);
};


/// Error in usage of libpqxx library, similar to std::logic_error
class PQXX_LIBEXPORT usage_error :
  public pqxx_exception, public std::logic_error
{
  virtual const std::exception &base() const noexcept override
	{ return *this; }
public:
  explicit usage_error(const std::string &);
};


/// Invalid argument passed to libpqxx, similar to std::invalid_argument
class PQXX_LIBEXPORT argument_error :
  public pqxx_exception, public std::invalid_argument
{
  virtual const std::exception &base() const noexcept override
	{ return *this; }
public:
  explicit argument_error(const std::string &);
};


/// Value conversion failed, e.g. when converting "Hello" to int.
class PQXX_LIBEXPORT conversion_error :
  public pqxx_exception, public std::domain_error
{
  virtual const std::exception &base() const noexcept override
	{ return *this; }
public:
  explicit conversion_error(const std::string &);
};


/// Something is out of range, similar to std::out_of_range
class PQXX_LIBEXPORT range_error :
  public pqxx_exception, public std::out_of_range
{
  virtual const std::exception &base() const noexcept override
	{ return *this; }
public:
  explicit range_error(const std::string &);
};


/// Query returned an unexpected number of rows.
class PQXX_LIBEXPORT unexpected_rows : public range_error
{
  virtual const std::exception &base() const noexcept override
	{ return *this; }
public:
  explicit unexpected_rows(const std::string &msg) : range_error{msg} {}
};


/// Database feature not supported in current setup
class PQXX_LIBEXPORT feature_not_supported : public sql_error
{
public:
  explicit feature_not_supported(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    sql_error{err, Q, sqlstate} {}
};

/// Error in data provided to SQL statement
class PQXX_LIBEXPORT data_exception : public sql_error
{
public:
  explicit data_exception(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    sql_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT integrity_constraint_violation : public sql_error
{
public:
  explicit integrity_constraint_violation(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    sql_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT restrict_violation :
  public integrity_constraint_violation
{
public:
  explicit restrict_violation(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    integrity_constraint_violation{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT not_null_violation :
  public integrity_constraint_violation
{
public:
  explicit not_null_violation(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    integrity_constraint_violation{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT foreign_key_violation :
  public integrity_constraint_violation
{
public:
  explicit foreign_key_violation(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    integrity_constraint_violation{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT unique_violation :
  public integrity_constraint_violation
{
public:
  explicit unique_violation(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    integrity_constraint_violation{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT check_violation :
  public integrity_constraint_violation
{
public:
  explicit check_violation(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    integrity_constraint_violation{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT invalid_cursor_state : public sql_error
{
public:
  explicit invalid_cursor_state(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    sql_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT invalid_sql_statement_name : public sql_error
{
public:
  explicit invalid_sql_statement_name(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    sql_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT invalid_cursor_name : public sql_error
{
public:
  explicit invalid_cursor_name(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    sql_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT syntax_error : public sql_error
{
public:
  /// Approximate position in string where error occurred, or -1 if unknown.
  const int error_position;

  explicit syntax_error(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr,
	int pos=-1) :
    sql_error{err, Q, sqlstate}, error_position{pos} {}
};

class PQXX_LIBEXPORT undefined_column : public syntax_error
{
public:
  explicit undefined_column(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    syntax_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT undefined_function : public syntax_error
{
public:
  explicit undefined_function(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    syntax_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT undefined_table : public syntax_error
{
public:
  explicit undefined_table(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    syntax_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT insufficient_privilege : public sql_error
{
public:
  explicit insufficient_privilege(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    sql_error{err, Q, sqlstate} {}
};

/// Resource shortage on the server
class PQXX_LIBEXPORT insufficient_resources : public sql_error
{
public:
  explicit insufficient_resources(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    sql_error{err,Q, sqlstate} {}
};

class PQXX_LIBEXPORT disk_full : public insufficient_resources
{
public:
  explicit disk_full(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    insufficient_resources{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT out_of_memory : public insufficient_resources
{
public:
  explicit out_of_memory(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    insufficient_resources{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT too_many_connections : public broken_connection
{
public:
  explicit too_many_connections(const std::string &err) :
	broken_connection{err} {}
};

/// PL/pgSQL error
/** Exceptions derived from this class are errors from PL/pgSQL procedures.
 */
class PQXX_LIBEXPORT plpgsql_error : public sql_error
{
public:
  explicit plpgsql_error(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    sql_error{err, Q, sqlstate} {}
};

/// Exception raised in PL/pgSQL procedure
class PQXX_LIBEXPORT plpgsql_raise : public plpgsql_error
{
public:
  explicit plpgsql_raise(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    plpgsql_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT plpgsql_no_data_found : public plpgsql_error
{
public:
  explicit plpgsql_no_data_found(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    plpgsql_error{err, Q, sqlstate} {}
};

class PQXX_LIBEXPORT plpgsql_too_many_rows : public plpgsql_error
{
public:
  explicit plpgsql_too_many_rows(
	const std::string &err,
	const std::string &Q="",
	const char sqlstate[]=nullptr) :
    plpgsql_error{err, Q, sqlstate} {}
};

/**
 * @}
 */

}

#include "pqxx/compiler-internal-post.hxx"

#endif
