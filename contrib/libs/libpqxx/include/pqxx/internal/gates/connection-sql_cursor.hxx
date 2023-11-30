#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace internal
{
class sql_cursor;

namespace gate
{
class PQXX_PRIVATE connection_sql_cursor : callgate<connection_base>
{
  friend class pqxx::internal::sql_cursor;

  connection_sql_cursor(reference x) : super(x) {}

  result exec(const char query[], int retries)
	{ return home().exec(query, retries); }

  void add_reactivation_avoidance_count(int n)
	{ home().add_reactivation_avoidance_count(n); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
