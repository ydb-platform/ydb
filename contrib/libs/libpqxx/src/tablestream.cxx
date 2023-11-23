/** Implementation of the pqxx::tablestream class.
 *
 * pqxx::tablestream provides optimized batch access to a database table.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include "pqxx/tablestream"
#include "pqxx/transaction"


pqxx::tablestream::tablestream(transaction_base &STrans,
	const std::string &Null) :
  internal::namedclass{"tablestream"},
  internal::transactionfocus{STrans},
  m_null{Null}
{
}


pqxx::tablestream::~tablestream() noexcept
{
}


void pqxx::tablestream::base_close()
{
  if (not is_finished())
  {
    m_finished = true;
    unregister_me();
  }
}
