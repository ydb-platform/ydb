#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace internal
{
namespace gate
{
class PQXX_PRIVATE transaction_subtransaction : callgate<transaction_base>
{
  friend class pqxx::subtransaction;

  transaction_subtransaction(reference x) : super(x) {}

  void add_reactivation_avoidance_count(int n)
	{ home().m_reactivation_avoidance.add(n); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
