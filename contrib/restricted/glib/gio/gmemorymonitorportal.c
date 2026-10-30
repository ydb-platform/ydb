/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright 2019 Red Hat, Inc.
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
 */

#include <contrib/restricted/glib/config.h>

#include "gmemorymonitor.h"
#include "gmemorymonitorportal.h"
#include "ginitable.h"
#include "giomodule-priv.h"
#include "xdp-dbus.h"
#include "gportalsupport.h"

#define G_MEMORY_MONITOR_PORTAL_GET_INITABLE_IFACE(o) (G_TYPE_INSTANCE_GET_INTERFACE ((o), G_TYPE_INITABLE, GInitable))

static void g_memory_monitor_portal_iface_init (GMemoryMonitorInterface *iface);
static void g_memory_monitor_portal_initable_iface_init (GInitableIface *iface);

struct _GMemoryMonitorPortal
{
  GObject parent_instance;

  GDBusProxy *proxy;
  gulong signal_id;
};

G_DEFINE_TYPE_WITH_CODE (GMemoryMonitorPortal, g_memory_monitor_portal, G_TYPE_OBJECT,
                         G_IMPLEMENT_INTERFACE (G_TYPE_INITABLE,
                                                g_memory_monitor_portal_initable_iface_init)
                         G_IMPLEMENT_INTERFACE (G_TYPE_MEMORY_MONITOR,
                                                g_memory_monitor_portal_iface_init)
                         _g_io_modules_ensure_extension_points_registered ();
                         g_io_extension_point_implement (G_MEMORY_MONITOR_EXTENSION_POINT_NAME,
                                                         g_define_type_id,
                                                         "portal",
                                                         40))

static void
g_memory_monitor_portal_init (GMemoryMonitorPortal *portal)
{
}

static void
proxy_signal (GDBusProxy            *proxy,
              const char            *sender,
              const char            *signal,
              GVariant              *parameters,
              GMemoryMonitorPortal *portal)
{
  guint8 level;

  if (strcmp (signal, "LowMemoryWarning") != 0)
    return;
  if (!parameters)
    return;

  g_variant_get (parameters, "(y)", &level);
  g_signal_emit_by_name (portal, "low-memory-warning", level);
}

static gboolean
g_memory_monitor_portal_initable_init (GInitable     *initable,
                                        GCancellable  *cancellable,
                                        GError       **error)
{
  GMemoryMonitorPortal *portal = G_MEMORY_MONITOR_PORTAL (initable);
  GDBusProxy *proxy;
  gchar *name_owner = NULL;

  if (!glib_should_use_portal ())
    {
      g_set_error (error, G_IO_ERROR, G_IO_ERROR_FAILED, "Not using portals");
      return FALSE;
    }

  proxy = g_dbus_proxy_new_for_bus_sync (G_BUS_TYPE_SESSION,
                                         G_DBUS_PROXY_FLAGS_DO_NOT_LOAD_PROPERTIES,
                                         NULL,
                                         "org.freedesktop.portal.Desktop",
                                         "/org/freedesktop/portal/desktop",
                                         "org.freedesktop.portal.MemoryMonitor",
                                         cancellable,
                                         error);
  if (!proxy)
    return FALSE;

  name_owner = g_dbus_proxy_get_name_owner (proxy);

  if (name_owner == NULL)
    {
      g_object_unref (proxy);
      g_set_error (error,
                   G_DBUS_ERROR,
                   G_DBUS_ERROR_NAME_HAS_NO_OWNER,
                   "Desktop portal not found");
      return FALSE;
    }

  g_free (name_owner);

  portal->signal_id = g_signal_connect (proxy, "g-signal",
                                        G_CALLBACK (proxy_signal), portal);

  portal->proxy = proxy;

  return TRUE;
}

static void
g_memory_monitor_portal_finalize (GObject *object)
{
  GMemoryMonitorPortal *portal = G_MEMORY_MONITOR_PORTAL (object);

  if (portal->proxy != NULL)
    g_clear_signal_handler (&portal->signal_id, portal->proxy);
  g_clear_object (&portal->proxy);

  G_OBJECT_CLASS (g_memory_monitor_portal_parent_class)->finalize (object);
}

static void
g_memory_monitor_portal_class_init (GMemoryMonitorPortalClass *nl_class)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (nl_class);

  gobject_class->finalize  = g_memory_monitor_portal_finalize;
}

static void
g_memory_monitor_portal_iface_init (GMemoryMonitorInterface *monitor_iface)
{
}

static void
g_memory_monitor_portal_initable_iface_init (GInitableIface *iface)
{
  iface->init = g_memory_monitor_portal_initable_init;
}
