#include "pqxx/compiler-internal.hxx"

#include "pqxx/version"

namespace pqxx
{
namespace internal
{
// One, single definition of this function.  If a call fails to link, then the
// libpqxx binary was built against a different libpqxx version than the code
// which is being linked against it.
template<> PQXX_LIBEXPORT
int check_library_version<PQXX_VERSION_MAJOR, PQXX_VERSION_MINOR>() noexcept
{
  return 0;
}
}
}
