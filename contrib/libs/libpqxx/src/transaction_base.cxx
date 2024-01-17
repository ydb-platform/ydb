/** Common code and definitions for the transaction classes.
 *
 * pqxx::transaction_base defines the interface for any abstract class that
 * represents a database transaction.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <cstring>
#include <stdexcept>

#include "pqxx/connection_base"
#include "pqxx/result"
#include "pqxx/transaction_base"

#include "pqxx/internal/gates/connection-transaction.hxx"
#include "pqxx/internal/gates/connection-parameterized_invocation.hxx"
#include "pqxx/internal/gates/transaction-transactionfocus.hxx"

#include "pqxx/internal/encodings.hxx"


using namespace pqxx::internal;


pqxx::internal::parameterized_invocation::parameterized_invocation(
	connection_base &c,
	const std::string &query) :
  m_home{c},
  m_query{query}
{
}


pqxx::result pqxx::internal::parameterized_invocation::exec()
{
  std::vector<const char *> values;
  std::vector<int> lengths;
  std::vector<int> binaries;
  const int elements = marshall(values, lengths, binaries);

  return gate::connection_parameterized_invocation{m_home}.parameterized_exec(
	m_query,
	values.data(),
	lengths.data(),
	binaries.data(),
	elements);
}


pqxx::transaction_base::transaction_base(connection_base &C, bool direct) :
  namedclass{"transaction_base"},
  m_conn{C}
{
  if (direct)
  {
    gate::connection_transaction gate{conn()};
    gate.register_transaction(this);
    m_registered = true;
  }
}


pqxx::transaction_base::~transaction_base()
{
  try
  {
    reactivation_avoidance_clear();
    if (not m_pending_error.empty())
      process_notice("UNPROCESSED ERROR: " + m_pending_error + "\n");

    if (m_registered)
    {
      m_conn.process_notice(description() + " was never closed properly!\n");
      gate::connection_transaction gate{conn()};
      gate.unregister_transaction(this);
    }
  }
  catch (const std::exception &e)
  {
    try
    {
      process_notice(std::string{e.what()} + "\n");
    }
    catch (const std::exception &)
    {
      process_notice(e.what());
    }
  }
}


void pqxx::transaction_base::commit()
{
  CheckPendingError();

  // Check previous status code.  Caller should only call this function if
  // we're in "implicit" state, but multiple commits are silently accepted.
  switch (m_status)
  {
  case st_nascent:	// Empty transaction.  No skin off our nose.
    return;

  case st_active:	// Just fine.  This is what we expect.
    break;

  case st_aborted:
    throw usage_error{"Attempt to commit previously aborted " + description()};

  case st_committed:
    // Transaction has been committed already.  This is not exactly proper
    // behaviour, but throwing an exception here would only give the impression
    // that an abort is needed--which would only confuse things further at this
    // stage.
    // Therefore, multiple commits are accepted, though under protest.
    m_conn.process_notice(description() + " committed more than once.\n");
    return;

  case st_in_doubt:
    // Transaction may or may not have been committed.  The only thing we can
    // really do is keep telling the caller that the transaction is in doubt.
    throw in_doubt_error{
	description() + " committed again while in an indeterminate state."};

  default:
    throw internal_error{"pqxx::transaction: invalid status code."};
  }

  // Tricky one.  If stream is nested in transaction but inside the same scope,
  // the commit() will come before the stream is closed.  Which means the
  // commit is premature.  Punish this swiftly and without fail to discourage
  // the habit from forming.
  if (m_focus.get())
    throw failure{
	"Attempt to commit " + description() + " with " +
	m_focus.get()->description() + " still open."};

  // Check that we're still connected (as far as we know--this is not an
  // absolute thing!) before trying to commit.  If the connection was broken
  // already, the commit would fail anyway but this way at least we don't remain
  // in-doubt as to whether the backend got the commit order at all.
  if (not m_conn.is_open())
    throw broken_connection{
	"Broken connection to backend; cannot complete transaction."};

  try
  {
    do_commit();
    m_status = st_committed;
  }
  catch (const in_doubt_error &)
  {
    m_status = st_in_doubt;
    throw;
  }
  catch (const std::exception &)
  {
    m_status = st_aborted;
    throw;
  }

  gate::connection_transaction gate{conn()};
  gate.add_variables(m_vars);

  End();
}


void pqxx::transaction_base::abort()
{
  // Check previous status code.  Quietly accept multiple aborts to
  // simplify emergency bailout code.
  switch (m_status)
  {
  case st_nascent:	// Never began transaction.  No need to issue rollback.
    break;

  case st_active:
    try { do_abort(); } catch (const std::exception &) { }
    break;

  case st_aborted:
    return;

  case st_committed:
    throw usage_error{"Attempt to abort previously committed " + description()};

  case st_in_doubt:
    // Aborting an in-doubt transaction is probably a reasonably sane response
    // to an insane situation.  Log it, but do not complain.
    m_conn.process_notice(
	"Warning: " + description() + " aborted after going into "
	"indeterminate state; it may have been executed anyway.\n");
    return;

  default:
    throw internal_error{"Invalid transaction status."};
  }

  m_status = st_aborted;
  End();
}


std::string pqxx::transaction_base::esc_raw(const std::string &str) const
{
  const unsigned char *p = reinterpret_cast<const unsigned char *>(str.c_str());
  return conn().esc_raw(p, str.size());
}


std::string pqxx::transaction_base::quote_raw(const std::string &str) const
{
  const unsigned char *p = reinterpret_cast<const unsigned char *>(str.c_str());
  return conn().quote_raw(p, str.size());
}


void pqxx::transaction_base::activate()
{
  switch (m_status)
  {
  case st_nascent:
    // Make sure transaction has begun before executing anything
    Begin();
    break;

  case st_active:
    break;

  case st_committed:
  case st_aborted:
  case st_in_doubt:
    throw usage_error{
	"Attempt to activate " + description() + " "
	"which is already closed."};

  default:
    throw internal_error{"pqxx::transaction: invalid status code."};
  }
}


pqxx::result pqxx::transaction_base::exec(
	const std::string &Query,
	const std::string &Desc)
{
  CheckPendingError();

  const std::string N = (Desc.empty() ? "" : "'" + Desc + "' ");

  if (m_focus.get())
    throw usage_error{
	"Attempt to execute query " + N +
	"on " + description() + " "
	"with " + m_focus.get()->description() + " still open."};

  try
  {
    activate();
  }
  catch (const usage_error &e)
  {
    throw usage_error{"Error executing query " + N + ".  " + e.what()};
  }

  // TODO: Pass Desc to do_exec(), and from there on down
  return do_exec(Query.c_str());
}


pqxx::result pqxx::transaction_base::exec_n(
	size_t rows,
	const std::string &Query,
	const std::string &Desc)
{
  const result r = exec(Query, Desc);
  if (r.size() != rows)
  {
    const std::string N = (Desc.empty() ? "" : "'" + Desc + "'");
    throw unexpected_rows{
	"Expected " + to_string(rows) + " row(s) of data "
        "from query " + N + ", got " + to_string(r.size()) + "."};
  }
  return r;
}


void pqxx::transaction_base::check_rowcount_prepared(
	const std::string &statement,
	size_t expected_rows,
	size_t actual_rows)
{
  if (actual_rows != expected_rows)
  {
    throw unexpected_rows{
	"Expected " + to_string(expected_rows) + " row(s) of data "
	"from prepared statement '" + statement + "', got " +
	to_string(actual_rows) + "."};
  }
}


void pqxx::transaction_base::check_rowcount_params(
	size_t expected_rows,
	size_t actual_rows)
{
  if (actual_rows != expected_rows)
  {
    throw unexpected_rows{
	"Expected " + to_string(expected_rows) + " row(s) of data "
	"from parameterised query, got " + to_string(actual_rows) + "."};
  }
}


pqxx::internal::parameterized_invocation
pqxx::transaction_base::parameterized(const std::string &query)
{
#include "pqxx/internal/ignore-deprecated-pre.hxx"
  return internal::parameterized_invocation{conn(), query};
#include "pqxx/internal/ignore-deprecated-post.hxx"
}


pqxx::prepare::invocation
pqxx::transaction_base::prepared(const std::string &statement)
{
  try
  {
    activate();
  }
  catch (const usage_error &e)
  {
    throw usage_error{
	"Error executing prepared statement " + statement + ".  " + e.what()};
  }
#include "pqxx/internal/ignore-deprecated-pre.hxx"
  return prepare::invocation{*this, statement};
#include "pqxx/internal/ignore-deprecated-post.hxx"
}


pqxx::result pqxx::transaction_base::internal_exec_prepared(
	const std::string &statement,
	const internal::params &args,
	result_format format)
{
  gate::connection_transaction gate{conn()};
  return gate.exec_prepared(statement, args, format);
}


pqxx::result pqxx::transaction_base::internal_exec_params(
	const std::string &query,
	const internal::params &args)
{
  gate::connection_transaction gate{conn()};
  return gate.exec_params(query, args);
}


void pqxx::transaction_base::set_variable(
	const std::string &Var,
	const std::string &Value)
{
  // Before committing to this new value, see what the backend thinks about it
  gate::connection_transaction gate{conn()};
  gate.raw_set_var(Var, Value);
  m_vars[Var] = Value;
}


std::string pqxx::transaction_base::get_variable(const std::string &Var)
{
  const std::map<std::string,std::string>::const_iterator i = m_vars.find(Var);
  if (i != m_vars.end()) return i->second;
  return gate::connection_transaction{conn()}.raw_get_var(Var);
}


void pqxx::transaction_base::Begin()
{
  if (m_status != st_nascent)
    throw internal_error{
	"pqxx::transaction: Begin() called while not in nascent state."};

  try
  {
    // Better handle any pending notifications before we begin
    m_conn.get_notifs();

    do_begin();
    m_status = st_active;
  }
  catch (const std::exception &)
  {
    End();
    throw;
  }
}


void pqxx::transaction_base::End() noexcept
{
  try
  {
    try { CheckPendingError(); }
    catch (const std::exception &e) { m_conn.process_notice(e.what()); }

    gate::connection_transaction gate{conn()};
    if (m_registered)
    {
      m_registered = false;
      gate.unregister_transaction(this);
    }

    if (m_status != st_active) return;

    if (m_focus.get())
      m_conn.process_notice(
	"Closing " + description() + "  with " +
	m_focus.get()->description() + " still open.\n");

    try { abort(); }
    catch (const std::exception &e) { m_conn.process_notice(e.what()); }

    gate.take_reactivation_avoidance(m_reactivation_avoidance.get());
    m_reactivation_avoidance.clear();
  }
  catch (const std::exception &e)
  {
    try { m_conn.process_notice(e.what()); } catch (const std::exception &) {}
  }
}


void pqxx::transaction_base::register_focus(internal::transactionfocus *S)
{
  m_focus.register_guest(S);
}


void pqxx::transaction_base::unregister_focus(internal::transactionfocus *S)
	noexcept
{
  try
  {
    m_focus.unregister_guest(S);
  }
  catch (const std::exception &e)
  {
    m_conn.process_notice(std::string{e.what()} + "\n");
  }
}


pqxx::result pqxx::transaction_base::direct_exec(const char C[], int Retries)
{
  CheckPendingError();
  return gate::connection_transaction{conn()}.exec(C, Retries);
}


void pqxx::transaction_base::register_pending_error(const std::string &Err)
	noexcept
{
  if (m_pending_error.empty() and not Err.empty())
  {
    try
    {
      m_pending_error = Err;
    }
    catch (const std::exception &e)
    {
      try
      {
        process_notice("UNABLE TO PROCESS ERROR\n");
        process_notice(e.what());
        process_notice("ERROR WAS:");
        process_notice(Err);
      }
      catch (...)
      {
      }
    }
  }
}


void pqxx::transaction_base::CheckPendingError()
{
  if (not m_pending_error.empty())
  {
    const std::string Err{m_pending_error};
    m_pending_error.clear();
    throw failure{Err};
  }
}


namespace
{
std::string MakeCopyString(
        const std::string &Table,
        const std::string &Columns)
{
  std::string Q = "COPY " + Table + " ";
  if (not Columns.empty()) Q += "(" + Columns + ") ";
  return Q;
}
} // namespace


void pqxx::transaction_base::BeginCopyRead(
	const std::string &Table,
	const std::string &Columns)
{
  exec(MakeCopyString(Table, Columns) + "TO STDOUT");
}


void pqxx::transaction_base::BeginCopyWrite(
	const std::string &Table,
	const std::string &Columns)
{
  exec(MakeCopyString(Table, Columns) + "FROM STDIN");
}


bool pqxx::transaction_base::read_copy_line(std::string &line)
{
  return gate::connection_transaction{conn()}.read_copy_line(line);
}


void pqxx::transaction_base::write_copy_line(const std::string &line)
{
  gate::connection_transaction gate{conn()};
  gate.write_copy_line(line);
}


void pqxx::transaction_base::end_copy_write()
{
  gate::connection_transaction gate{conn()};
  gate.end_copy_write();
}


void pqxx::internal::transactionfocus::register_me()
{
  gate::transaction_transactionfocus gate{m_trans};
  gate.register_focus(this);
  m_registered = true;
}


void pqxx::internal::transactionfocus::unregister_me() noexcept
{
  gate::transaction_transactionfocus gate{m_trans};
  gate.unregister_focus(this);
  m_registered = false;
}

void
pqxx::internal::transactionfocus::reg_pending_error(const std::string &err)
	noexcept
{
  gate::transaction_transactionfocus gate{m_trans};
  gate.register_pending_error(err);
}
