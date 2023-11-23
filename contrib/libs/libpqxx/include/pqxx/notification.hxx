/** Definition of the pqxx::notification_receiver functor interface.
 *
 * pqxx::notification_receiver handles incoming notifications.
 *
 * DO NOT INCLUDE THIS FILE DIRECTLY; include pqxx/notification instead.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_NOTIFICATION
#define PQXX_H_NOTIFICATION

#include "pqxx/compiler-public.hxx"
#include "pqxx/compiler-internal-pre.hxx"

#include <string>

#include "pqxx/types.hxx"


namespace pqxx
{
/// "Observer" base class for notifications.
/** @addtogroup notification Notifications and Receivers
 *
 * To listen on a notification issued using the NOTIFY command, derive your own
 * class from notification_receiver and define its function-call operator to
 * perform whatever action you wish to take when the given notification arrives.
 * Then create an object of that class and pass it to your connection.  DO NOT
 * use raw SQL to listen for notifications, or your attempts to listen won't be
 * resumed when a connection fails--and you'll have no way to notice.
 *
 * Notifications never arrive inside a transaction, not even in a
 * nontransaction.  Therefore, you are free to open a transaction of your own
 * inside your receiver's function invocation operator.
 *
 * Notifications you are listening for may arrive anywhere within libpqxx code,
 * but be aware that @b PostgreSQL @b defers @b notifications @b occurring
 * @b inside @b transactions.  (This was done for excellent reasons; just think
 * about what happens if the transaction where you happen to handle an incoming
 * notification is later rolled back for other reasons).  So if you're keeping a
 * transaction open, don't expect any of your receivers on the same connection
 * to be notified.
 *
 * (For very similar reasons, outgoing notifications are also not sent until the
 * transaction that sends them commits.)
 *
 * Multiple receivers on the same connection may listen on a notification of the
 * same name.  An incoming notification is processed by invoking all receivers
 * (zero or more) of the same name.
 */
class PQXX_LIBEXPORT PQXX_NOVTABLE notification_receiver
{
public:
  /// Register the receiver with a connection.
  /**
   * @param c Connnection to operate on.
   * @param channel Name of the notification to listen for.
   */
  notification_receiver(connection_base &c, const std::string &channel);
  notification_receiver(const notification_receiver &) =delete;
  notification_receiver &operator=(const notification_receiver &) =delete;
  virtual ~notification_receiver();

  /// The channel that this receiver listens on.
  const std::string &channel() const { return m_channel; }

  /// Overridable: action to invoke when notification arrives.
  /**
   * @param payload On PostgreSQL 9.0 or later, an optional string that may have
   * been passed to the NOTIFY command.
   * @param backend_pid Process ID of the database backend process that served
   * our connection when the notification arrived.  The actual process ID behind
   * the connection may have changed by the time this method is called.
   */
  virtual void operator()(const std::string &payload, int backend_pid) =0;

protected:
  connection_base &conn() const noexcept { return m_conn; }

private:
  connection_base &m_conn;
  std::string m_channel;
};
}

#include "pqxx/compiler-internal-post.hxx"
#endif
