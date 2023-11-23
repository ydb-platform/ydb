/** Implementation of the pqxx::dbtransaction class.
 *
 * pqxx::dbtransaction represents a real backend transaction.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include "pqxx/dbtransaction"

#include "pqxx/internal/gates/connection-dbtransaction.hxx"

using namespace pqxx::internal;


namespace
{
std::string generate_set_transaction(
	pqxx::readwrite_policy rw,
	const std::string &IsolationString=std::string{})
{
  std::string args;

  if (not IsolationString.empty())
    if (IsolationString != pqxx::isolation_traits<pqxx::read_committed>::name())
      args += " ISOLATION LEVEL " + IsolationString;

  if (rw != pqxx::read_write) args += " READ ONLY";

  return args.empty() ? "BEGIN" : ("BEGIN; SET TRANSACTION" + args);
}
} // namespace


pqxx::dbtransaction::dbtransaction(
	connection_base &C,
	const std::string &IsolationString,
	readwrite_policy rw) :
  namedclass{"dbtransaction"},
  transaction_base{C},
  m_start_cmd{generate_set_transaction(rw, IsolationString)}
{
}


pqxx::dbtransaction::dbtransaction(
	connection_base &C,
	bool direct,
	readwrite_policy rw) :
  namedclass{"dbtransaction"},
  transaction_base(C, direct),
  m_start_cmd{generate_set_transaction(rw)}
{
}


pqxx::dbtransaction::~dbtransaction()
{
}


void pqxx::dbtransaction::do_begin()
{
  const gate::connection_dbtransaction gate(conn());
  const int avoidance_counter = gate.get_reactivation_avoidance_count();
  direct_exec(m_start_cmd.c_str(), avoidance_counter ? 0 : 2);
}


pqxx::result pqxx::dbtransaction::do_exec(const char Query[])
{
  try
  {
    return direct_exec(Query);
  }
  catch (const std::exception &)
  {
    try { abort(); } catch (const std::exception &) {}
    throw;
  }
}


void pqxx::dbtransaction::do_abort()
{
  reactivation_avoidance_clear();
  direct_exec("ROLLBACK");
}


std::string pqxx::dbtransaction::fullname(const std::string &ttype,
	const std::string &isolation)
{
  return ttype + "<" + isolation + ">";
}
