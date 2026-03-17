/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright 2021 Red Hat, Inc.
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

#include "gpowerprofilemonitor.h"
#include "gpowerprofilemonitorportal.h"
#include "gdbuserror.h"
#include "gdbusproxy.h"
#include "ginitable.h"
#include "gioerror.h"
#include "giomodule-priv.h"
#include "gportalsupport.h"

#define G_POWER_PROFILE_MONITOR_PORTAL_GET_INITABLE_IFACE(o) (G_TYPE_INSTANCE_GET_INTERFACE ((o), G_TYPE_INITABLE, GInitable))

static void g_power_profile_monitor_portal_iface_init (GPowerProfileMonitorInterface *iface);
static void g_power_profile_monitor_portal_initable_iface_init (GInitableIface *iface);

typedef enum
{
  PROP_POWER_SAVER_ENABLED = 1,
} GPowerProfileMonitorPortalProperty;

struct _GPowerProfileMonitorPortal
{
  GObject parent_instance;

  GDBusProxy *proxy;
  gulong signal_id;
  gboolean power_saver_enabled;
};

G_DEFINE_TYPE_WITH_CODE (GPowerProfileMonitorPortal, g_power_profile_monitor_portal, G_TYPE_OBJECT,
                         G_IMPLEMENT_INTERFACE (G_TYPE_INITABLE,
                                                g_power_profile_monitor_portal_initable_iface_init)
                         G_IMPLEMENT_INTERFACE (G_TYPE_POWER_PROFILE_MONITOR,
                                                g_power_profile_monitor_portal_iface_init)
                         _g_io_modules_ensure_extension_points_registered ();
                         g_io_extension_point_implement (G_POWER_PROFILE_MONITOR_EXTENSION_POINT_NAME,
                                                         g_define_type_id,
                                                         "portal",
                                                         40))

static void
g_power_profile_monitor_portal_init (GPowerProfileMonitorPortal *portal)
{
}

static void
proxy_properties_changed (GDBusProxy *proxy,
                          GVariant   *changed_properties,
                          GStrv       invalidated_properties,
                          gpointer    user_data)
{
  GPowerProfileMonitorPortal *ppm = user_data;
  gboolean power_saver_enabled;

  if (!g_variant_lookup (changed_properties, "power-saver-enabled", "b", &power_saver_enabled))
    return;

  if (power_saver_enabled == ppm->power_saver_enabled)
    return;

  ppm->power_saver_enabled = power_saver_enabled;
  g_object_notify (G_OBJECT (ppm), "power-saver-enabled");
}

static void
g_power_profile_monitor_portal_get_property (GObject    *object,
                                             guint       prop_id,
                                             GValue     *value,
                                             GParamSpec *pspec)
{
  GPowerProfileMonitorPortal *ppm = G_POWER_PROFILE_MONITOR_PORTAL (object);

  switch ((GPowerProfileMonitorPortalProperty) prop_id)
    {
    case PROP_POWER_SAVER_ENABLED:
      g_value_set_boolean (value, ppm->power_saver_enabled);
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
    }
}

static gboolean
g_power_profile_monitor_portal_initable_init (GInitable     *initable,
                                              GCancellable  *cancellable,
                                              GError       **error)
{
  GPowerProfileMonitorPortal *ppm = G_POWER_PROFILE_MONITOR_PORTAL (initable);
  GDBusProxy *proxy;
  gchar *name_owner;
  GVariant *power_saver_enabled_v = NULL;

  if (!glib_should_use_portal ())
    {
      g_set_error (error, G_IO_ERROR, G_IO_ERROR_FAILED, "Not using portals");
      return FALSE;
    }

  proxy = g_dbus_proxy_new_for_bus_sync (G_BUS_TYPE_SESSION,
                                         G_DBUS_PROXY_FLAGS_NONE,
                                         NULL,
                                         "org.freedesktop.portal.Desktop",
                                         "/org/freedesktop/portal/desktop",
                                         "org.freedesktop.portal.PowerProfileMonitor",
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

  ppm->signal_id = g_signal_connect (proxy, "g-properties-changed",
                                     G_CALLBACK (proxy_properties_changed), ppm);

  power_saver_enabled_v = g_dbus_proxy_get_cached_property (proxy, "power-saver-enabled");
  if (power_saver_enabled_v != NULL &&
      g_variant_is_of_type (power_saver_enabled_v, G_VARIANT_TYPE_BOOLEAN))
    ppm->power_saver_enabled = g_variant_get_boolean (power_saver_enabled_v);
  g_clear_pointer (&power_saver_enabled_v, g_variant_unref);

  ppm->proxy = g_steal_pointer (&proxy);

  return TRUE;
}

static void
g_power_profile_monitor_portal_finalize (GObject *object)
{
  GPowerProfileMonitorPortal *ppm = G_POWER_PROFILE_MONITOR_PORTAL (object);

  g_clear_signal_handler (&ppm->signal_id, ppm->proxy);
  g_clear_object (&ppm->proxy);

  G_OBJECT_CLASS (g_power_profile_monitor_portal_parent_class)->finalize (object);
}

static void
g_power_profile_monitor_portal_class_init (GPowerProfileMonitorPortalClass *nl_class)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (nl_class);

  gobject_class->get_property = g_power_profile_monitor_portal_get_property;
  gobject_class->finalize  = g_power_profile_monitor_portal_finalize;

  g_object_class_override_property (gobject_class, PROP_POWER_SAVER_ENABLED, "power-saver-enabled");
}

static void
g_power_profile_monitor_portal_iface_init (GPowerProfileMonitorInterface *monitor_iface)
{
}

static void
g_power_profile_monitor_portal_initable_iface_init (GInitableIface *iface)
{
  iface->init = g_power_profile_monitor_portal_initable_init;
}
