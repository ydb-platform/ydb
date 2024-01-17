#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
class dbtransaction;

namespace internal
{
namespace gate
{
class PQXX_PRIVATE connection_dbtransaction : callgate<connection_base>
{
  friend class pqxx::dbtransaction;

  connection_dbtransaction(reference x) : super(x) {}

  int get_reactivation_avoidance_count() const noexcept
	{ return home().m_reactivation_avoidance.get(); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
