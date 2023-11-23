/** Helper classes for defining and executing prepared statements>
 *
 * See the connection_base hierarchy for more about prepared statements.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#include "pqxx/compiler-internal.hxx"

#include "pqxx/connection_base"
#include "pqxx/prepared_statement"
#include "pqxx/result"
#include "pqxx/transaction_base"

#include "pqxx/internal/gates/connection-prepare-invocation.hxx"


using namespace pqxx;
using namespace pqxx::internal;


pqxx::prepare::invocation::invocation(
	transaction_base &home,
	const std::string &statement) :
  m_home{home},
  m_statement{statement}
{
}

pqxx::result pqxx::prepare::invocation::exec() const
{
    return internal_exec(result_format::text);
}

pqxx::result pqxx::prepare::invocation::exec_binary() const
{
    return internal_exec(result_format::binary);
}

pqxx::result pqxx::prepare::invocation::internal_exec(result_format format) const
{
  std::vector<const char *> ptrs;
  std::vector<int> lens;
  std::vector<int> binaries;
  const int elts = marshall(ptrs, lens, binaries);

  return gate::connection_prepare_invocation{m_home.conn()}.prepared_exec(
	m_statement,
	ptrs.data(),
	lens.data(),
	binaries.data(),
	elts,
	format);
}

bool pqxx::prepare::invocation::exists() const
{
  return gate::connection_prepare_invocation{m_home.conn()}.prepared_exists(
	m_statement);
}


pqxx::prepare::internal::prepared_def::prepared_def(const std::string &def) :
  definition{def}
{
}
