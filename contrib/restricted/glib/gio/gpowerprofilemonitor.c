/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright 2019 Red Hat, Inc
 * Copyright 2021 Igalia S.L.
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
#include "glib.h"
#include "glibintl.h"

#include "gpowerprofilemonitor.h"
#include "ginetaddress.h"
#include "ginetsocketaddress.h"
#include "ginitable.h"
#include "gioenumtypes.h"
#include "giomodule-priv.h"
#include "gtask.h"

/**
 * SECTION:gpowerprofilemonitor
 * @title: GPowerProfileMonitor
 * @short_description: Power profile monitor
 * @include: gio/gio.h
 *
 * #GPowerProfileMonitor makes it possible for applications as well as OS components
 * to monitor system power profiles and act upon them. It currently only exports
 * whether the system is in “Power Saver” mode (known as “Low Power” mode on
 * some systems).
 *
 * When in “Low Power” mode, it is recommended that applications:
 * - disable automatic downloads;
 * - reduce the rate of refresh from online sources such as calendar or
 *   email synchronisation;
 * - reduce the use of expensive visual effects.
 *
 * It is also likely that OS components providing services to applications will
 * lower their own background activity, for the sake of the system.
 *
 * There are a variety of tools that exist for power consumption analysis, but those
 * usually depend on the OS and hardware used. On Linux, one could use `upower` to
 * monitor the battery discharge rate, `powertop` to check on the background activity
 * or activity at all), `sysprof` to inspect CPU usage, and `intel_gpu_time` to
 * profile GPU usage.
 *
 * Don't forget to disconnect the #GPowerProfileMonitor::notify::power-saver-enabled
 * signal, and unref the #GPowerProfileMonitor itself when exiting.
 *
 * Since: 2.70
 */

/**
 * GPowerProfileMonitor:
 *
 * #GPowerProfileMonitor monitors system power profile and notifies on
 * changes.
 *
 * Since: 2.70
 */

/**
 * GPowerProfileMonitorInterface:
 * @g_iface: The parent interface.
 *
 * The virtual function table for #GPowerProfileMonitor.
 *
 * Since: 2.70
 */

G_DEFINE_INTERFACE_WITH_CODE (GPowerProfileMonitor, g_power_profile_monitor, G_TYPE_OBJECT,
                              g_type_interface_add_prerequisite (g_define_type_id, G_TYPE_INITABLE))


/**
 * g_power_profile_monitor_dup_default:
 *
 * Gets a reference to the default #GPowerProfileMonitor for the system.
 *
 * Returns: (not nullable) (transfer full): a new reference to the default #GPowerProfileMonitor
 *
 * Since: 2.70
 */
GPowerProfileMonitor *
g_power_profile_monitor_dup_default (void)
{
  return g_object_ref (_g_io_module_get_default (G_POWER_PROFILE_MONITOR_EXTENSION_POINT_NAME,
                                                 "GIO_USE_POWER_PROFILE_MONITOR",
                                                 NULL));
}

/**
 * g_power_profile_monitor_get_power_saver_enabled:
 * @monitor: a #GPowerProfileMonitor
 *
 * Gets whether the system is in “Power Saver” mode.
 *
 * You are expected to listen to the
 * #GPowerProfileMonitor::notify::power-saver-enabled signal to know when the profile has
 * changed.
 *
 * Returns: Whether the system is in “Power Saver” mode.
 *
 * Since: 2.70
 */
gboolean
g_power_profile_monitor_get_power_saver_enabled (GPowerProfileMonitor *monitor)
{
  gboolean enabled;
  g_object_get (monitor, "power-saver-enabled", &enabled, NULL);
  return enabled;
}

static void
g_power_profile_monitor_default_init (GPowerProfileMonitorInterface *iface)
{
  /**
   * GPowerProfileMonitor:power-saver-enabled:
   *
   * Whether “Power Saver” mode is enabled on the system.
   *
   * Since: 2.70
   */
  g_object_interface_install_property (iface,
                                       g_param_spec_boolean ("power-saver-enabled",
                                                             "power-saver-enabled",
                                                             "Power Saver Enabled",
                                                             FALSE,
                                                             G_PARAM_READABLE | G_PARAM_STATIC_STRINGS | G_PARAM_EXPLICIT_NOTIFY));
}
