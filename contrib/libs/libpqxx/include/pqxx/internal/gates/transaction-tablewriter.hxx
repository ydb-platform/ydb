#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace internal
{
namespace gate
{
class PQXX_PRIVATE transaction_tablewriter : callgate<transaction_base>
{
  friend class pqxx::tablewriter;

  transaction_tablewriter(reference x) : super(x) {}

  void BeginCopyWrite(
	const std::string &table,
	const std::string &columns = std::string{})
	{ home().BeginCopyWrite(table, columns); }

  void write_copy_line(const std::string &line)
	{ home().write_copy_line(line); }

  void end_copy_write() { home().end_copy_write(); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
