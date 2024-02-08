#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
class connection_base;

namespace internal
{
namespace gate
{
class PQXX_PRIVATE connection_parameterized_invocation :
  callgate<connection_base>
{
  friend class pqxx::internal::parameterized_invocation;

  connection_parameterized_invocation(reference x) : super(x) {}

  result parameterized_exec(
	const std::string &query,
	const char *const params[],
	const int paramlengths[],
	const int binaries[],
	int nparams)
  {
#include <pqxx/internal/ignore-deprecated-pre.hxx>
    return home().parameterized_exec(
	query,
	params,
	paramlengths,
	binaries,
	nparams);
#include <pqxx/internal/ignore-deprecated-post.hxx>
  }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
