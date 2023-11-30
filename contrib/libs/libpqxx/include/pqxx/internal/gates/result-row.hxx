#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace internal
{
class row;

namespace gate
{
class PQXX_PRIVATE result_row : callgate<result>
{
  friend class pqxx::row;

  result_row(reference x) : super(x) {}

  operator bool()
	{ return bool(home()); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
