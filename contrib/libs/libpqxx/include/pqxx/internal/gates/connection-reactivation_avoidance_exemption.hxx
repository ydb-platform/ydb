#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace internal
{
class reactivation_avoidance_exemption;

namespace gate
{
class PQXX_PRIVATE connection_reactivation_avoidance_exemption :
  callgate<connection_base>
{
  friend class pqxx::internal::reactivation_avoidance_exemption;

  connection_reactivation_avoidance_exemption(reference x) : super(x) {}

  int get_counter() const { return home().m_reactivation_avoidance.get(); }
  void add_counter(int x) const { home().m_reactivation_avoidance.add(x); }
  void clear_counter() { home().m_reactivation_avoidance.clear(); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
