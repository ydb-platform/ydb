/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright © 2009 Codethink Limited
 * Copyright © 2009 Red Hat, Inc
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with this library; if not, see <http://www.gnu.org/licenses/>.
 *
 * Authors: Ryan Lortie <desrt@desrt.ca>
 *          Alexander Larsson <alexl@redhat.com>
 */

/**
 * SECTION:gthreadedsocketservice
 * @title: GThreadedSocketService
 * @short_description: A threaded GSocketService
 * @include: gio/gio.h
 * @see_also: #GSocketService.
 *
 * A #GThreadedSocketService is a simple subclass of #GSocketService
 * that handles incoming connections by creating a worker thread and
 * dispatching the connection to it by emitting the
 * #GThreadedSocketService::run signal in the new thread.
 *
 * The signal handler may perform blocking IO and need not return
 * until the connection is closed.
 *
 * The service is implemented using a thread pool, so there is a
 * limited amount of threads available to serve incoming requests.
 * The service automatically stops the #GSocketService from accepting
 * new connections when all threads are busy.
 *
 * As with #GSocketService, you may connect to #GThreadedSocketService::run,
 * or subclass and override the default handler.
 */

#include <contrib/restricted/glib/config.h>
#include "gsocketconnection.h"
#include "gthreadedsocketservice.h"
#include "glibintl.h"
#include "gmarshal-internal.h"

struct _GThreadedSocketServicePrivate
{
  GThreadPool *thread_pool;
  int max_threads;
  gint job_count;
};

static guint g_threaded_socket_service_run_signal;

G_DEFINE_TYPE_WITH_PRIVATE (GThreadedSocketService,
                            g_threaded_socket_service,
                            G_TYPE_SOCKET_SERVICE)

typedef enum
{
  PROP_MAX_THREADS = 1,
} GThreadedSocketServiceProperty;

G_LOCK_DEFINE_STATIC(job_count);

typedef struct
{
  GThreadedSocketService *service;  /* (owned) */
  GSocketConnection *connection;  /* (owned) */
  GObject *source_object;  /* (owned) (nullable) */
} GThreadedSocketServiceData;

static void
g_threaded_socket_service_data_free (GThreadedSocketServiceData *data)
{
  g_clear_object (&data->service);
  g_clear_object (&data->connection);
  g_clear_object (&data->source_object);
  g_slice_free (GThreadedSocketServiceData, data);
}

static void
g_threaded_socket_service_func (gpointer job_data,
                                gpointer user_data)
{
  GThreadedSocketServiceData *data = job_data;
  gboolean result;

  g_signal_emit (data->service, g_threaded_socket_service_run_signal,
                 0, data->connection, data->source_object, &result);

  G_LOCK (job_count);
  if (data->service->priv->job_count-- == data->service->priv->max_threads)
    g_socket_service_start (G_SOCKET_SERVICE (data->service));
  G_UNLOCK (job_count);

  g_threaded_socket_service_data_free (data);
}

static gboolean
g_threaded_socket_service_incoming (GSocketService    *service,
                                    GSocketConnection *connection,
                                    GObject           *source_object)
{
  GThreadedSocketService *threaded;
  GThreadedSocketServiceData *data;
  GError *local_error = NULL;

  threaded = G_THREADED_SOCKET_SERVICE (service);

  data = g_slice_new0 (GThreadedSocketServiceData);
  data->service = g_object_ref (threaded);
  data->connection = g_object_ref (connection);
  data->source_object = (source_object != NULL) ? g_object_ref (source_object) : NULL;

  G_LOCK (job_count);
  if (++threaded->priv->job_count == threaded->priv->max_threads)
    g_socket_service_stop (service);
  G_UNLOCK (job_count);

  if (!g_thread_pool_push (threaded->priv->thread_pool, data, &local_error))
    {
      g_warning ("Error handling incoming socket: %s", local_error->message);
      g_threaded_socket_service_data_free (data);
    }

  g_clear_error (&local_error);

  return FALSE;
}

static void
g_threaded_socket_service_init (GThreadedSocketService *service)
{
  service->priv = g_threaded_socket_service_get_instance_private (service);
  service->priv->max_threads = 10;
}

static void
g_threaded_socket_service_constructed (GObject *object)
{
  GThreadedSocketService *service = G_THREADED_SOCKET_SERVICE (object);

  service->priv->thread_pool =
    g_thread_pool_new  (g_threaded_socket_service_func,
			NULL,
			service->priv->max_threads,
			FALSE,
			NULL);
}


static void
g_threaded_socket_service_finalize (GObject *object)
{
  GThreadedSocketService *service = G_THREADED_SOCKET_SERVICE (object);

  /* All jobs in the pool hold a reference to this #GThreadedSocketService, so
   * this should only be called once the pool is empty: */
  g_thread_pool_free (service->priv->thread_pool, FALSE, FALSE);

  G_OBJECT_CLASS (g_threaded_socket_service_parent_class)
    ->finalize (object);
}

static void
g_threaded_socket_service_get_property (GObject    *object,
					guint       prop_id,
					GValue     *value,
					GParamSpec *pspec)
{
  GThreadedSocketService *service = G_THREADED_SOCKET_SERVICE (object);

  switch ((GThreadedSocketServiceProperty) prop_id)
    {
      case PROP_MAX_THREADS:
	g_value_set_int (value, service->priv->max_threads);
	break;

      default:
	G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
    }
}

static void
g_threaded_socket_service_set_property (GObject      *object,
					guint         prop_id,
					const GValue *value,
					GParamSpec   *pspec)
{
  GThreadedSocketService *service = G_THREADED_SOCKET_SERVICE (object);

  switch ((GThreadedSocketServiceProperty) prop_id)
    {
      case PROP_MAX_THREADS:
	service->priv->max_threads = g_value_get_int (value);
	break;

      default:
	G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
    }
}


static void
g_threaded_socket_service_class_init (GThreadedSocketServiceClass *class)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (class);
  GSocketServiceClass *ss_class = &class->parent_class;

  gobject_class->constructed = g_threaded_socket_service_constructed;
  gobject_class->finalize = g_threaded_socket_service_finalize;
  gobject_class->set_property = g_threaded_socket_service_set_property;
  gobject_class->get_property = g_threaded_socket_service_get_property;

  ss_class->incoming = g_threaded_socket_service_incoming;

  /**
   * GThreadedSocketService::run:
   * @service: the #GThreadedSocketService.
   * @connection: a new #GSocketConnection object.
   * @source_object: (nullable): the source_object passed to g_socket_listener_add_address().
   *
   * The ::run signal is emitted in a worker thread in response to an
   * incoming connection. This thread is dedicated to handling
   * @connection and may perform blocking IO. The signal handler need
   * not return until the connection is closed.
   *
   * Returns: %TRUE to stop further signal handlers from being called
   */
  g_threaded_socket_service_run_signal =
    g_signal_new (I_("run"), G_TYPE_FROM_CLASS (class), G_SIGNAL_RUN_LAST,
		  G_STRUCT_OFFSET (GThreadedSocketServiceClass, run),
		  g_signal_accumulator_true_handled, NULL,
		  _g_cclosure_marshal_BOOLEAN__OBJECT_OBJECT,
		  G_TYPE_BOOLEAN,
		  2, G_TYPE_SOCKET_CONNECTION, G_TYPE_OBJECT);
  g_signal_set_va_marshaller (g_threaded_socket_service_run_signal,
			      G_TYPE_FROM_CLASS (class),
			      _g_cclosure_marshal_BOOLEAN__OBJECT_OBJECTv);

  g_object_class_install_property (gobject_class, PROP_MAX_THREADS,
				   g_param_spec_int ("max-threads",
						     P_("Max threads"),
						     P_("The max number of threads handling clients for this service"),
						     -1,
						     G_MAXINT,
						     10,
						     G_PARAM_CONSTRUCT_ONLY | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
}

/**
 * g_threaded_socket_service_new:
 * @max_threads: the maximal number of threads to execute concurrently
 *   handling incoming clients, -1 means no limit
 *
 * Creates a new #GThreadedSocketService with no listeners. Listeners
 * must be added with one of the #GSocketListener "add" methods.
 *
 * Returns: a new #GSocketService.
 *
 * Since: 2.22
 */
GSocketService *
g_threaded_socket_service_new (int max_threads)
{
  return g_object_new (G_TYPE_THREADED_SOCKET_SERVICE,
		       "max-threads", max_threads,
		       NULL);
}
