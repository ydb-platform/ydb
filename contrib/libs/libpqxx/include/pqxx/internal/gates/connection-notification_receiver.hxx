#include <pqxx/internal/callgate.hxx>

#include <pqxx/connection_base>


namespace pqxx
{
class notification_receiver;

namespace internal
{
namespace gate
{
class PQXX_PRIVATE connection_notification_receiver : callgate<connection_base>
{
  friend class pqxx::notification_receiver;

  connection_notification_receiver(reference x) : super(x) {}

  void add_receiver(notification_receiver *receiver)
	{ home().add_receiver(receiver); }
  void remove_receiver(notification_receiver *receiver) noexcept
	{ home().remove_receiver(receiver); }
};
} // namespace pqxx::internal::gate
} // namespace pqxx::internal
} // namespace pqxx
