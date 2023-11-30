#include <string>

#include <pqxx/internal/callgate.hxx>
#include <pqxx/internal/libpq-forward.hxx>

namespace pqxx
{
class largeobject;

namespace internal
{
namespace gate
{
class PQXX_PRIVATE connection_largeobject : callgate<connection_base>
{
  friend class pqxx::largeobject;

  connection_largeobject(reference x) : super(x) {}

  pq::PGconn *raw_connection() const { return home().raw_connection(); }
};


class PQXX_PRIVATE const_connection_largeobject :
	callgate<const connection_base>
{
  friend class pqxx::largeobject;

  const_connection_largeobject(reference x) : super(x) {}

  std::string error_message() const { return home().err_msg(); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
