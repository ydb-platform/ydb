#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace internal
{
namespace gate
{
class PQXX_PRIVATE errorhandler_connection_base : callgate<errorhandler>
{
  friend class pqxx::connection_base;

  errorhandler_connection_base(reference x) : super(x) {}

  void unregister() noexcept { home().unregister(); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
