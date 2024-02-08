/** Implementation of the pqxx::subtransaction class.
 *
 * pqxx::transaction is a nested transaction, i.e. one within a transaction
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include <stdexcept>

#include "pqxx/connection_base"
#include "pqxx/subtransaction"

#include "pqxx/internal/gates/transaction-subtransaction.hxx"

using namespace pqxx::internal;


pqxx::subtransaction::subtransaction(
	dbtransaction &T,
	const std::string &Name) :
  namedclass{"subtransaction", T.conn().adorn_name(Name)},
  transactionfocus{T},
  dbtransaction(T.conn(), false),
  m_parent{T}
{
}


namespace
{
using dbtransaction_ref = pqxx::dbtransaction &;
}


pqxx::subtransaction::subtransaction(
	subtransaction &T,
	const std::string &Name) :
  subtransaction(dbtransaction_ref(T), Name)
{
}


void pqxx::subtransaction::do_begin()
{
  try
  {
    direct_exec(("SAVEPOINT " + quote_name(name())).c_str());
  }
  catch (const sql_error &)
  {
    throw;
  }
}


void pqxx::subtransaction::do_commit()
{
  const int ra = m_reactivation_avoidance.get();
  m_reactivation_avoidance.clear();
  direct_exec(("RELEASE SAVEPOINT " + quote_name(name())).c_str());
  gate::transaction_subtransaction{m_parent}.add_reactivation_avoidance_count(
	ra);
}


void pqxx::subtransaction::do_abort()
{
  direct_exec(("ROLLBACK TO SAVEPOINT " + quote_name(name())).c_str());
}
