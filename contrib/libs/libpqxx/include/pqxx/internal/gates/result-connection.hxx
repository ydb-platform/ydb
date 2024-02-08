#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace internal
{
namespace gate
{
class PQXX_PRIVATE result_connection : callgate<const result>
{
  friend class pqxx::connection_base;

  result_connection(reference x) : super(x) {}

  operator bool() const { return bool(home()); }
  bool operator!() const { return not home(); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
