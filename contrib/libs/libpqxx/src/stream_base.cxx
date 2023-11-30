/** Implementation of the pqxx::stream_base class.
 *
 * pqxx::stream_base provides optimized batch access to a database table.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include "pqxx/stream_base.hxx"
#include "pqxx/transaction"


pqxx::stream_base::stream_base(transaction_base &tb) :
  internal::namedclass("stream_base"),
  internal::transactionfocus{tb},
  m_finished{false}
{}


pqxx::stream_base::operator bool() const noexcept
{
  return not m_finished;
}


bool pqxx::stream_base::operator!() const noexcept
{
  return not static_cast<bool>(*this);
}


void pqxx::stream_base::close()
{
  if (*this)
  {
    m_finished = true;
    unregister_me();
  }
}
