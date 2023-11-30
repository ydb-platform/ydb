/** Definition of the pqxx::connection_base abstract base class.
 *
 * pqxx::connection_base encapsulates a frontend to backend connection
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/connection_base instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_CONNECTION_BASE
#define PQXX_H_CONNECTION_BASE

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include <bitset>
#include <list>
#include <map>
#include <memory>

#include "pqxx/errorhandler.hxx"
#include "pqxx/except.hxx"
#include "pqxx/prepared_statement.hxx"
#include "pqxx/strconv.hxx"
#include "pqxx/util.hxx"
#include "pqxx/version.hxx"


/* Use of the libpqxx library starts here.
 *
 * Everything that can be done with a database through libpqxx must go through
 * a connection object derived from connection_base.
 */

/* Methods tested in eg. self-test program test1 are marked with "//[t01]"
 */

namespace pqxx
{
namespace internal
{
class reactivation_avoidance_exemption;
class sql_cursor;

class reactivation_avoidance_counter
{
public:
  reactivation_avoidance_counter() =default;

  void add(int n) noexcept { m_counter += n; }
  void clear() noexcept { m_counter = 0; }
  int get() const noexcept { return m_counter; }

private:
  int m_counter = 0;
};

}


/// Encrypt password for given user.
/** Use this when setting a new password for the user if password encryption is
 * enabled.  Inputs are the username the password is for, and the plaintext
 * password.
 *
 * @return encrypted version of the password, suitable for encrypted PostgreSQL
 * authentication.
 *
 * Thus the password for a user can be changed with:
 * @code
 * void setpw(transaction_base &t, const string &user, const string &pw)
 * {
 *   t.exec("ALTER USER " + user + " "
 *   	"PASSWORD '" + encrypt_password(user,pw) + "'");
 * }
 * @endcode
 */
std::string PQXX_LIBEXPORT encrypt_password(				//[t00]
	const std::string &user,
	const std::string &password);


namespace internal
{
namespace gate
{
class connection_dbtransaction;
class connection_errorhandler;
class connection_largeobject;
class connection_notification_receiver;
class connection_parameterized_invocation;
class connection_pipeline;
class connection_prepare_invocation;
class connection_reactivation_avoidance_exemption;
class connection_sql_cursor;
class connection_transaction;
class const_connection_largeobject;
} // namespace pqxx::internal::gate
} // namespace pqxx::internal


/// connection_base abstract base class; represents a connection to a database.
/** This is the first class to look at when you wish to work with a database
 * through libpqxx.  Depending on the implementing concrete child class, a
 * connection can be automatically opened when it is constructed, or when it is
 * first used, or somewhere inbetween.  The connection is automatically closed
 * upon destruction (if it hasn't been closed already).
 *
 * To query or manipulate the database once connected, use one of the
 * transaction classes (see pqxx/transaction_base.hxx) or preferably the
 * transactor framework (see pqxx/transactor.hxx).
 *
 * If a network connection to the database server fails, the connection will be
 * restored automatically (although any transaction going on at the time will
 * have to be aborted).  This also means that any information set in previous
 * transactions that is not stored in the database, such as temp tables or
 * connection-local variables defined with PostgreSQL's SET command, will be
 * lost.  Whenever you create such state, either keept it local to one
 * transaction, where possible, or inhibit automatic reactivation of the
 * connection using the inhibit_reactivation() method.
 *
 * When a connection breaks, you will typically get a broken_connection
 * exception.  This can happen at almost any point, and the details may depend
 * on which connection class (all derived from this one) you use.
 *
 * As a general rule, always avoid raw queries if libpqxx offers a dedicated
 * function for the same purpose.  There may be hidden logic to hide certain
 * complications from you, such as reinstating session variables when a
 * broken or disabled connection is reactivated.
 *
 * @warning On Unix-like systems, including GNU and BSD systems, your program
 * may receive the SIGPIPE signal when the connection to the backend breaks.  By
 * default this signal will abort your program.  Use "signal(SIGPIPE, SIG_IGN)"
 * if you want your program to continue running after a connection fails.
 */
class PQXX_LIBEXPORT connection_base
{
public:
  /// Explicitly close connection.
  void disconnect() noexcept;						//[t02]

   /// Is this connection open at the moment?
  /** @warning This function is @b not needed in most code.  Resist the
   * temptation to check it after opening a connection; instead, rely on the
   * broken_connection exception that will be thrown on connection failure.
   */
  bool PQXX_PURE is_open() const noexcept;				//[t01]

 /**
   * @name Activation
   *
   * @warning Connection deactivation/reactivation will probably be removed in
   * libpqxx 7.  If your application relies on an ability to "put connections
   * to sleep" and reactivate them later, you'll need to wrap them in some way
   * to handle this.
   *
   * Connections can be temporarily deactivated, or they can break because of
   * overly impatient firewalls dropping TCP connections.  Where possible,
   * libpqxx will try to re-activate these when resume using them, or you can
   * wake them up explicitly.  You probably won't need this feature, but you
   * should be aware of it.
   */
  //@{
  /// @deprecated Explicitly activate deferred or deactivated connection.
  /** Use of this method is entirely optional.  Whenever a connection is used
   * while in a deferred or deactivated state, it will transparently try to
   * bring itself into an activated state.  This function is best viewed as an
   * explicit hint to the connection that "if you're not in an active state, now
   * would be a good time to get into one."  Whether a connection is currently
   * in an active state or not makes no real difference to its functionality.
   * There is also no particular need to match calls to activate() with calls to
   * deactivate().  A good time to call activate() might be just before you
   * first open a transaction on a lazy connection.
   */
  PQXX_DEPRECATED void activate();					//[t12]

  /// @deprecated Explicitly deactivate connection.
  /** Like its counterpart activate(), this method is entirely optional.
   * Calling this function really only makes sense if you won't be using this
   * connection for a while and want to reduce the number of open connections on
   * the database server.
   * There is no particular need to match or pair calls to deactivate() with
   * calls to activate(), but calling deactivate() during a transaction is an
   * error.
   */
  PQXX_DEPRECATED void deactivate();					//[t12]

  /// @deprecated Disallow (or permit) connection recovery
  /** A connection whose underlying socket is not currently connected to the
   * server will normally (re-)establish communication with the server whenever
   * needed, or when the client program requests it (although for reasons of
   * integrity, never inside a transaction; but retrying the whole transaction
   * may implicitly cause the connection to be restored).  In normal use this is
   * quite a convenient thing to have and presents a simple, safe, predictable
   * interface.
   *
   * There is at least one situation where this feature is not desirable,
   * however.  Although most session state (prepared statements, session
   * variables) is automatically restored to its working state upon connection
   * reactivation, temporary tables and so-called WITH HOLD cursors (which can
   * live outside transactions) are not.
   *
   * Cursors that live outside transactions are automatically handled, and the
   * library will quietly ignore requests to deactivate or reactivate
   * connections while they exist; it does not want to give you the illusion of
   * being back in your transaction when in reality you just dropped a cursor.
   * With temporary tables this is not so easy: there is no easy way for the
   * library to detect their creation or track their lifetimes.
   *
   * So if your program uses temporary tables, and any part of this use happens
   * outside of any database transaction (or spans multiple transactions), some
   * of the work you have done on these tables may unexpectedly be undone if the
   * connection is broken or deactivated while any of these tables exists, and
   * then reactivated or implicitly restored before you are finished with it.
   *
   * If this describes any part of your program, guard it against unexpected
   * reconnections by inhibiting reconnection at the beginning.  And if you want
   * to continue doing work on the connection afterwards that no longer requires
   * the temp tables, you can permit it again to get the benefits of connection
   * reactivation for the remainder of the program.
   *
   * @param inhibit should reactivation be inhibited from here on?
   *
   * @warning Some connection types (the lazy and asynchronous types) defer
   * completion of the socket-level connection until it is actually needed by
   * the client program.  Inhibiting reactivation before this connection is
   * really established will prevent these connection types from doing their
   * work.  For those connection types, if you are sure that reactivation needs
   * to be inhibited before any query goes across the connection, activate() the
   * connection first.  This will ensure that definite activation happens before
   * you inhibit it.
   */
  PQXX_DEPRECATED void inhibit_reactivation(bool inhibit)		//[t86]
	{ m_inhibit_reactivation=inhibit; }

  /// Make the connection fail.  @warning Do not use this except for testing!
  /** Breaks the connection in some unspecified, horrible, dirty way to enable
   * failure testing.
   *
   * Do not use this in normal programs.  This is only meant for testing.
   */
  void simulate_failure();						//[t94]
  //@}

  /// Invoke notice processor function.  The message should end in newline.
  void process_notice(const char[]) noexcept;				//[t14]
  /// Invoke notice processor function.  Newline at end is recommended.
  void process_notice(const std::string &) noexcept;			//[t14]

  /// Enable tracing to a given output stream, or nullptr to disable.
  void trace(std::FILE *) noexcept;					//[t03]

  /**
   * @name Connection properties
   *
   * These are probably not of great interest, since most are derived from
   * information supplied by the client program itself, but they are included
   * for completeness.
   */
  //@{
  /// Name of database we're connected to, if any.
  /** @warning This activates the connection, which may fail with a
   * broken_connection exception.
   */
  const char *dbname();							//[t01]

  /// Database user ID we're connected under, if any.
  /** @warning This activates the connection, which may fail with a
   * broken_connection exception.
   */
  const char *username();						//[t01]

  /// Address of server, or nullptr if none specified (i.e. default or local)
  /** @warning This activates the connection, which may fail with a
   * broken_connection exception.
   */
  const char *hostname();						//[t01]

  /// Server port number we're connected to.
  /** @warning This activates the connection, which may fail with a
   * broken_connection exception.
   */
  const char *port();							//[t01]

  /// Process ID for backend process.
  /** Use with care: connections may be lost and automatically re-established
   * without your knowledge, in which case this process ID may no longer be
   * correct.  You may, however, assume that this number remains constant and
   * reliable within the span of a successful backend transaction.  If the
   * transaction fails, which may be due to a lost connection, then this number
   * will have become invalid at some point within the transaction.
   *
   * @return Process identifier, or 0 if not currently connected.
   */
  int PQXX_PURE backendpid() const noexcept;				//[t01]

  /// Socket currently used for connection, or -1 for none.  Use with care!
  /** Query the current socket number.  This is intended for event loops based
   * on functions such as select() or poll(), where multiple file descriptors
   * are watched.
   *
   * Please try to stay away from this function.  It is really only meant for
   * event loops that need to wait on more than one file descriptor.  If all you
   * need is to block until a notification arrives, for instance, use
   * await_notification().  If you want to issue queries and retrieve results in
   * nonblocking fashion, check out the pipeline class.
   *
   * @warning Don't store this value anywhere, and always be prepared for the
   * possibility that, at any given time, there may not be a socket!  The
   * socket may change or even go away or be established during any invocation
   * of libpqxx code on the connection, no matter how trivial.
   */
  int PQXX_PURE sock() const noexcept;					//[t87]

  /**
   * @name Capabilities
   *
   * Some functionality may only be available in certain versions of the
   * backend, or only when speaking certain versions of the communications
   * protocol that connects us to the backend.
   */
  //@{

  /// Session capabilities.
  /** No capabilities are defined at the moment: all capabilities that older
   * versions checked for are now always supported.
   */
  enum capability
  {
    /// Not a capability value; end-of-enumeration marker
    cap_end,
  };


  /// Does this connection seem to support the given capability?
  /** Don't try to be smart by caching this information anywhere.  Obtaining it
   * is quite fast (especially after the first time) and what's more, a
   * capability may "suddenly" appear or disappear if the connection is broken
   * or deactivated, and then restored.  This may happen silently any time no
   * backend transaction is active; if it turns out that the server was upgraded
   * or restored from an older backup, or the new connection goes to a different
   * backend, then the restored session may have different capabilities than
   * were available previously.
   *
   * Some guesswork is involved in establishing the presence of any capability;
   * try not to rely on this function being exactly right.
   *
   * @warning Make sure your connection is active before calling this function,
   * or the answer will always be "no."  In particular, if you are using this
   * function on a newly-created lazyconnection, activate the connection first.
   */
  bool supports(capability c) const noexcept				//[t88]
	{ return m_caps.test(c); }

  /// What version of the PostgreSQL protocol is this connection using?
  /** The answer can be 0 (when there is no connection); 3 for protocol 3.0; or
   * possibly higher values as newer protocol versions are taken into use.
   *
   * If the connection is broken and restored, the restored connection could
   * possibly use a different server and protocol version.  This would normally
   * happen if the server is upgraded without shutting down the client program,
   * for example.
   */
  int PQXX_PURE protocol_version() const noexcept;			//[t01]

  /// What version of the PostgreSQL server are we connected to?
  /** The result is a bit complicated: each of the major, medium, and minor
   * release numbers is written as a two-digit decimal number, and the three
   * are then concatenated.  Thus server version 9.4.2 will be returned as the
   * decimal number 90402.  If there is no connection to the server, this
   * returns zero.
   *
   * @warning When writing version numbers in your code, don't add zero at the
   * beginning!  Numbers beginning with zero are interpreted as octal (base-8)
   * in C++.  Thus, 070402 is not the same as 70402, and 080000 is not a number
   * at all because there is no digit "8" in octal notation.  Use strictly
   * decimal notation when it comes to these version numbers.
   */
  int PQXX_PURE server_version() const noexcept;			//[t01]
  //@}

  /// @name Text encoding
  /**
   * Each connection is governed by a "client encoding," which dictates how
   * strings and other text is represented in bytes.  The database server will
   * send text data to you in this encoding, and you should use it for the
   * queries and data which you send to the server.
   *
   * Search the PostgreSQL documentation for "character set encodings" to find
   * out more about the available encodings, how to extend them, and how to use
   * them.  Not all server-side encodings are compatible with all client-side
   * encodings or vice versa.
   *
   * Encoding names are case-insensitive, so e.g. "UTF8" is equivalent to
   * "utf8".
   *
   * You can change the client encoding, but this may not work when the
   * connection is in a special state, such as when streaming a table.  It's
   * not clear what happens if you change the encoding during a transaction,
   * and then abort the transaction.
   */
  //@{
  /// Get client-side character encoding, by name.
  std::string get_client_encoding() const;

  /// Set client-side character encoding, by name.
  /**
   * @param Encoding Name of the character set encoding to use.
   */
  void set_client_encoding(const std::string &encoding);		//[t07]

  /// Set client-side character encoding, by name.
  /**
   * @param Encoding Name of the character set encoding to use.
   */
  void set_client_encoding(const char encoding[]);			//[t07]

  /// Get the connection's encoding, as a PostgreSQL-defined code.
  int PQXX_PRIVATE encoding_id() const;

  //@}

  /// Set session variable
  /** Set a session variable for this connection, using the SET command.  If the
   * connection to the database is lost and recovered, the last-set value will
   * be restored automatically.  See the PostgreSQL documentation for a list of
   * variables that can be set and their permissible values.
   * If a transaction is currently in progress, aborting that transaction will
   * normally discard the newly set value.  However nontransaction (which
   * doesn't start a real backend transaction) is an exception.
   *
   * @warning Do not mix the set_variable interface with manual setting of
   * variables by executing the corresponding SQL commands, and do not get or
   * set variables while a tablestream or pipeline is active on the same
   * connection.
   * @param Var Variable to set
   * @param Value Value vor Var to assume: an identifier, a quoted string, or a
   * number.
   */
  void set_variable(							//[t60]
	const std::string &Var,
	const std::string &Value);

  /// Read session variable
  /** Will try to read the value locally, from the list of variables set with
   * the set_variable function.  If that fails, the database is queried.
   * @warning Do not mix the set_variable interface with manual setting of
   * variables by executing the corresponding SQL commands, and do not get or
   * set variables while a tablestream or pipeline is active on the same
   * connection.
   */
  std::string get_variable(const std::string &);			//[t60]
  //@}


  /**
   * @name Notifications and Receivers
   */
  //@{
  /// Check for pending notifications and take appropriate action.
  /**
   * All notifications found pending at call time are processed by finding
   * any matching receivers and invoking those.  If no receivers matched the
   * notification string, none are invoked but the notification is considered
   * processed.
   *
   * Exceptions thrown by client-registered receivers are reported using the
   * connection's errorhandlers, but the exceptions themselves are not passed
   * on outside this function.
   *
   * @return Number of notifications processed
   */
  int get_notifs();							//[t04]


  /// Wait for a notification to come in
  /** The wait may also be terminated by other events, such as the connection
   * to the backend failing.  Any pending or received notifications are
   * processed as part of the call.
   *
   * @return Number of notifications processed
   */
  int await_notification();						//[t78]

  /// Wait for a notification to come in, or for given timeout to pass
  /** The wait may also be terminated by other events, such as the connection
   * to the backend failing.  Any pending or received notifications are
   * processed as part of the call.

   * @return Number of notifications processed
   */
  int await_notification(long seconds, long microseconds);		//[t79]
  //@}


  /**
   * @name Prepared statements
   *
   * PostgreSQL supports prepared SQL statements, i.e. statements that can be
   * registered under a client-provided name, optimized once by the backend, and
   * executed any number of times under the given name.
   *
   * Prepared statement definitions are not sensitive to transaction boundaries;
   * a statement defined inside a transaction will remain defined outside that
   * transaction, even if the transaction itself is subsequently aborted.  Once
   * a statement has been prepared, only closing the connection or explicitly
   * "unpreparing" it can make it go away.
   *
   * Use the @c pqxx::transaction_base::exec_prepared functions to execute a
   * prepared statement.  Use @c prepared().exists() to find out whether a
   * statement has been prepared under a given name.  See \ref prepared for a
   * full discussion.
   *
   * Never try to prepare, execute, or unprepare a prepared statement manually
   * using direct SQL queries.  Always use the functions provided by libpqxx.
   *
   * @{
   */

  /// Define a prepared statement.
  /**
   * The statement's definition can refer to a parameter using the parameter's
   * positional number n in the definition.  For example, the first parameter
   * can be used as a variable "$1", the second as "$2" and so on.
   *
   * Here's an example of how to use prepared statements.  Note the unusual
   * syntax for passing parameters: every new argument is a parenthesized
   * expression that is simply tacked onto the end of the statement!
   *
   * @code
   * using namespace pqxx;
   * void foo(connection_base &C)
   * {
   *   C.prepare("findtable", "select * from pg_tables where name=$1");
   *   work W{C};
   *   result R = W.exec_prepared("findtable", "mytable");
   *   if (R.empty()) throw runtime_error{"mytable not found!"};
   * }
   * @endcode
   *
   * To save time, prepared statements aren't really registered with the backend
   * until they are first used.  If this is not what you want, e.g. because you
   * have very specific realtime requirements, you can use the @c prepare_now()
   * function to force immediate preparation.
   *
   * The statement may not be registered with the backend until it is actually
   * used.  So if, for example, the statement is syntactically incorrect, you
   * may see a syntax_error here, or later when you try to call the statement,
   * or during a @c prepare_now() call.
   *
   * @param name unique name for the new prepared statement.
   * @param definition SQL statement to prepare.
   */
  void prepare(const std::string &name, const std::string &definition);

  /// Define a nameless prepared statement.
  /**
   * This can be useful if you merely want to pass large binary parameters to a
   * statement without otherwise wishing to prepare it.  If you use this
   * feature, always keep the definition and the use close together to avoid
   * the nameless statement being redefined unexpectedly by code somewhere else.
   */
  void prepare(const std::string &definition);

  /// Drop prepared statement.
  void unprepare(const std::string &name);

  /// Request that prepared statement be registered with the server.
  /** If the statement had already been fully prepared, this will do nothing.
   *
   * If the connection should break and be transparently restored, then the new
   * connection will again defer registering the statement with the server.
   * Since connections are never restored inside backend transactions, doing
   * this once at the beginning of your transaction ensures that the statement
   * will not be re-registered during that transaction.  In most cases, however,
   * it's probably better not to use this and let the connection decide when and
   * whether to register prepared statements that you've defined.
   */
  void prepare_now(const std::string &name);

  /**
   * @}
   */

  /// @deprecated Pre-C++11 transactor function.
  /**
   * This has been superseded by the new transactor framework and
   * @c pqxx::perform.
   *
   * Invokes the given transactor, making at most Attempts attempts to perform
   * the encapsulated code.  If the code throws any exception other than
   * broken_connection, it will be aborted right away.
   *
   * @param T The transactor to be executed.
   * @param Attempts Maximum number of attempts to be made to execute T.
   */
  template<typename TRANSACTOR>
  PQXX_DEPRECATED void perform(const TRANSACTOR &T, int Attempts);	//[t04]

  /// @deprecated Pre-C++11 transactor function.  Use @c pqxx::perform instead.
  /**
   * This has been superseded by the new transactor framework and
   * @c pqxx::perform.
   *
   * @param T The transactor to be executed.
   */
  template<typename TRANSACTOR>
  PQXX_DEPRECATED void perform(const TRANSACTOR &T)
  {
#include "pqxx/internal/ignore-deprecated-pre.hxx"
    perform(T, 3);
#include "pqxx/internal/ignore-deprecated-post.hxx"
  }

  /// Suffix unique number to name to make it unique within session context
  /** Used internally to generate identifiers for SQL objects (such as cursors
   * and nested transactions) based on a given human-readable base name.
   */
  std::string adorn_name(const std::string &);				//[90]

  /**
   * @defgroup escaping-functions String-escaping functions
   */
  //@{
  /// Escape string for use as SQL string literal on this connection
  std::string esc(const char str[]);

  /// Escape string for use as SQL string literal on this connection
  std::string esc(const char str[], size_t maxlen);

  /// Escape string for use as SQL string literal on this connection
  std::string esc(const std::string &str);

  /// Escape binary string for use as SQL string literal on this connection
  std::string esc_raw(const unsigned char str[], size_t len);

  /// Unescape binary data, e.g. from a table field or notification payload.
  /** Takes a binary string as escaped by PostgreSQL, and returns a restored
   * copy of the original binary data.
   */
  std::string unesc_raw(const std::string &text)
					     { return unesc_raw(text.c_str()); }

  /// Unescape binary data, e.g. from a table field or notification payload.
  /** Takes a binary string as escaped by PostgreSQL, and returns a restored
   * copy of the original binary data.
   */
  std::string unesc_raw(const char *text);

  /// Escape and quote a string of binary data.
  std::string quote_raw(const unsigned char str[], size_t len);

  /// Escape and quote an SQL identifier for use in a query.
  std::string quote_name(const std::string &identifier);

  /// Represent object as SQL string, including quoting & escaping.
  /** Nulls are recognized and represented as SQL nulls. */
  template<typename T>
  std::string quote(const T &t)
  {
    if (string_traits<T>::is_null(t)) return "NULL";
    return "'" + this->esc(to_string(t)) + "'";
  }

  std::string quote(const binarystring &);

  /// Escape string for literal LIKE match.
  /** Use this when part of an SQL "LIKE" pattern should match only as a
   * literal string, not as a pattern, even if it contains "%" or "_"
   * characters that would normally act as wildcards.
   *
   * The string does not get string-escaped or quoted.  You do that later.
   *
   * For instance, let's say you have a string @c name entered by the user,
   * and you're searching a @c file column for items that match @c name
   * followed by a dot and three letters.  Even if @c name contains wildcard
   * characters "%" or "_", you only want those to match literally, so "_"
   * only matches "_" and "%" only matches a single "%".
   *
   * You do that by "like-escaping" @c name, appending the wildcard pattern
   * @c ".___", and finally, escaping and quoting the result for inclusion in
   * your query:
   *
   *    tx.exec(
   *        "SELECT file FROM item WHERE file LIKE " +
   *        tx.quote(tx.esc_like(name) + ".___"));
   *
   * The SQL "LIKE" operator also lets you choose your own escape character.
   * This is supported, but must be a single-byte character.
   */
  std::string esc_like(const std::string &str, char escape_char='\\') const;
  //@}

  /// Attempt to cancel the ongoing query, if any.
  void cancel_query();

  /// Error verbosity levels.
  enum error_verbosity
  {
      // These values must match those in libpq's PGVerbosity enum.
      terse=0,
      normal=1,
      verbose=2
  };

  /// Set session verbosity.
  /** Set the verbosity of error messages to "terse", "normal" (i.e. default) or
   * "verbose."
   *
   *  If "terse", returned messages include severity, primary text, and position
   *  only; this will normally fit on a single line. "normal" produces messages
   *  that include the above plus any detail, hint, or context fields (these
   *  might span multiple lines).  "verbose" includes all available fields.
   */
  void set_verbosity(error_verbosity verbosity) noexcept;
   /// Retrieve current error verbosity
  error_verbosity get_verbosity() const noexcept {return m_verbosity;}

  /// Return pointers to the active errorhandlers.
  /** The entries are ordered from oldest to newest handler.
   *
   * You may use this to find errorhandlers that your application wants to
   * delete when destroying the connection.  Be aware, however, that libpqxx
   * may also add errorhandlers of its own, and those will be included in the
   * list.  If this is a problem for you, derive your errorhandlers from a
   * custom base class derived from pqxx::errorhandler.  Then use dynamic_cast
   * to find which of the error handlers are yours.
   *
   * The pointers point to the real errorhandlers.  The container it returns
   * however is a copy of the one internal to the connection, not a reference.
   */
  std::vector<errorhandler *> get_errorhandlers() const;

protected:
  explicit connection_base(connectionpolicy &pol) :
	m_policy{pol}
  {
    // Check library version.  The check_library_version template is declared
    // for any library version, but only actually defined for the version of
    // the libpqxx binary against which the code is linked.
    //
    // If the library binary is a different version than the one declared in
    // these headers, then this call will fail to link: there will be no
    // definition for the function with these exact template parameter values.
    // There will be a definition, but the version in the parameter values will
    // be different.
    //
    // There is no particular reason to do this here in this constructor, except
    // to ensure that every meaningful libpqxx client will execute it.  The call
    // must be in the execution path somewhere or the compiler won't try to link
    // it.  We can't use it to initialise a global or class-static variable,
    // because a smart compiler might resolve it at compile time.
    // 
    // On the other hand, we don't want to make a useless function call too
    // often for performance reasons.  A local static variable is initialised
    // only on the definition's first execution.  Compilers will be well
    // optimised for this behaviour, so there's a minimal one-time cost.
    static const auto version_ok =
      internal::check_library_version<PQXX_VERSION_MAJOR, PQXX_VERSION_MINOR>();
    ignore_unused(version_ok);

    clearcaps();
  }
  void init();

  void close() noexcept;
  void wait_read() const;
  void wait_read(long seconds, long microseconds) const;
  void wait_write() const;

private:

  result make_result(internal::pq::PGresult *rhs, const std::string &query);

  void clearcaps() noexcept;
  void PQXX_PRIVATE set_up_state();
  void PQXX_PRIVATE check_result(const result &);

  void PQXX_PRIVATE internal_set_trace() noexcept;
  int PQXX_PRIVATE PQXX_PURE status() const noexcept;

  friend class internal::gate::const_connection_largeobject;
  const char * PQXX_PURE err_msg() const noexcept;

  void PQXX_PRIVATE reset();
  std::string PQXX_PRIVATE raw_get_var(const std::string &);
  void PQXX_PRIVATE process_notice_raw(const char msg[]) noexcept;

  void read_capabilities();

  prepare::internal::prepared_def &find_prepared(const std::string &);

  prepare::internal::prepared_def &register_prepared(const std::string &);

  friend class internal::gate::connection_prepare_invocation;
  /// @deprecated Use exec_prepared instead.
  PQXX_DEPRECATED result prepared_exec(
	const std::string &,
	const char *const[],
	const int[],
	const int[],
	int,
	result_format format);
  result exec_prepared(const std::string &statement, const internal::params &, result_format format = result_format::text);
  bool prepared_exists(const std::string &) const;

  /// Connection handle.
  internal::pq::PGconn *m_conn = nullptr;

  connectionpolicy &m_policy;

  /// Active transaction on connection, if any.
  internal::unique<transaction_base> m_trans;

  /// Set libpq notice processor to call connection's error handlers chain.
  void set_notice_processor();
  /// Clear libpq notice processor.
  void clear_notice_processor();
  std::list<errorhandler *> m_errorhandlers;

  /// File to trace to, if any
  std::FILE *m_trace = nullptr;

  using receiver_list =
	std::multimap<std::string, pqxx::notification_receiver *>;
  /// Notification receivers.
  receiver_list m_receivers;

  /// Variables set in this session
  std::map<std::string, std::string> m_vars;

  using PSMap = std::map<std::string, prepare::internal::prepared_def>;
  /// Prepared statements existing in this section
  PSMap m_prepared;

  /// Server version
  int m_serverversion = 0;

  /// Stacking counter: known objects that can't be auto-reactivated
  internal::reactivation_avoidance_counter m_reactivation_avoidance;

  /// Unique number to use as suffix for identifiers (see adorn_name())
  int m_unique_id = 0;

  /// Have we successfully established this connection?
  bool m_completed = false;

  /// Is reactivation currently inhibited?
  bool m_inhibit_reactivation = false;

  /// Set of session capabilities
  std::bitset<cap_end> m_caps;

  /// Current verbosity level
  error_verbosity m_verbosity = normal;

  friend class internal::gate::connection_errorhandler;
  void PQXX_PRIVATE register_errorhandler(errorhandler *);
  void PQXX_PRIVATE unregister_errorhandler(errorhandler *) noexcept;

  friend class internal::gate::connection_transaction;
  result PQXX_PRIVATE exec(const char[], int Retries);
  void PQXX_PRIVATE register_transaction(transaction_base *);
  void PQXX_PRIVATE unregister_transaction(transaction_base *) noexcept;
  bool PQXX_PRIVATE read_copy_line(std::string &);
  void PQXX_PRIVATE write_copy_line(const std::string &);
  void PQXX_PRIVATE end_copy_write();
  void PQXX_PRIVATE raw_set_var(const std::string &, const std::string &);
  void PQXX_PRIVATE add_variables(const std::map<std::string, std::string> &);

  friend class internal::gate::connection_largeobject;
  internal::pq::PGconn *raw_connection() const { return m_conn; }

  friend class internal::gate::connection_notification_receiver;
  void add_receiver(notification_receiver *);
  void remove_receiver(notification_receiver *) noexcept;

  friend class internal::gate::connection_pipeline;
  void PQXX_PRIVATE start_exec(const std::string &);
  bool PQXX_PRIVATE consume_input() noexcept;
  bool PQXX_PRIVATE is_busy() const noexcept;
  internal::pq::PGresult *get_result();

  friend class internal::gate::connection_dbtransaction;

  friend class internal::gate::connection_sql_cursor;
  void add_reactivation_avoidance_count(int);

  friend class internal::gate::connection_reactivation_avoidance_exemption;

  friend class internal::gate::connection_parameterized_invocation;
  /// @deprecated Use exec_params instead.
  PQXX_DEPRECATED result parameterized_exec(
	const std::string &query,
	const char *const params[],
	const int paramlengths[],
	const int binaries[],
	int nparams);

  result exec_params(
	const std::string &query,
	const internal::params &args);

  connection_base(const connection_base &) =delete;
  connection_base &operator=(const connection_base &) =delete;
};


namespace internal
{

/// Scoped exemption to reactivation avoidance
class PQXX_LIBEXPORT reactivation_avoidance_exemption
{
public:
  explicit reactivation_avoidance_exemption(connection_base &C);
  ~reactivation_avoidance_exemption();

  void close_connection() noexcept { m_open = false; }

private:
  connection_base &m_home;
  int m_count;
  bool m_open;
};


void wait_read(const internal::pq::PGconn *);
void wait_read(const internal::pq::PGconn *, long seconds, long microseconds);
void wait_write(const internal::pq::PGconn *);
} // namespace pqxx::internal

} // namespace pqxx

#include "pqxx/compiler-internal-post.hxx"

#endif
