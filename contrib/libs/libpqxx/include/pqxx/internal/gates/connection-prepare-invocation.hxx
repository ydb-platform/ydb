#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace prepare
{
class invocation;
} // namespace pqxx::prepare

namespace internal
{
namespace gate
{
class PQXX_PRIVATE connection_prepare_invocation : callgate<connection_base>
{
  friend class pqxx::prepare::invocation;

  connection_prepare_invocation(reference x) : super(x) {}

  /// @deprecated To be replaced by exec_prepared.
  result prepared_exec(
	const std::string &statement,
	const char *const params[],
	const int paramlengths[],
	const int binary[],
	int nparams,
	result_format format)
  {
#include <pqxx/internal/ignore-deprecated-pre.hxx>
    return home().prepared_exec(
	statement,
	params,
	paramlengths,
	binary,
	nparams,
	format);
#include <pqxx/internal/ignore-deprecated-post.hxx>
  }

  bool prepared_exists(const std::string &statement) const
	{ return home().prepared_exists(statement); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
