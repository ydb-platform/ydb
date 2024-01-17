#include <pqxx/internal/callgate.hxx>

namespace pqxx
{
namespace internal
{
namespace gate
{
class PQXX_PRIVATE transaction_transactionfocus : callgate<transaction_base>
{
  friend class pqxx::internal::transactionfocus;

  transaction_transactionfocus(reference x) : super(x) {}

  void register_focus(transactionfocus *focus) { home().register_focus(focus); }
  void unregister_focus(transactionfocus *focus) noexcept
	{ home().unregister_focus(focus); }
  void register_pending_error(const std::string &error)
	{ home().register_pending_error(error); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
