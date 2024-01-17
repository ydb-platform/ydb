#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
class connection_base;
class errorhandler;

namespace internal
{
namespace gate
{
class PQXX_PRIVATE connection_errorhandler : callgate<connection_base>
{
  friend class pqxx::errorhandler;

  connection_errorhandler(reference x) : super(x) {}

  void register_errorhandler(errorhandler *h)
					    { home().register_errorhandler(h); }
  void unregister_errorhandler(errorhandler *h)
					  { home().unregister_errorhandler(h); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
