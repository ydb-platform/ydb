/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright 2019 Red Hat, Inc
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

#include "gmemorymonitor.h"
#include "ginetaddress.h"
#include "ginetsocketaddress.h"
#include "ginitable.h"
#include "gioenumtypes.h"
#include "giomodule-priv.h"
#include "gtask.h"

/**
 * SECTION:gmemorymonitor
 * @title: GMemoryMonitor
 * @short_description: Memory usage monitor
 * @include: gio/gio.h
 *
 * #GMemoryMonitor will monitor system memory and suggest to the application
 * when to free memory so as to leave more room for other applications.
 * It is implemented on Linux using the [Low Memory Monitor](https://gitlab.freedesktop.org/hadess/low-memory-monitor/)
 * ([API documentation](https://hadess.pages.freedesktop.org/low-memory-monitor/)).
 *
 * There is also an implementation for use inside Flatpak sandboxes.
 *
 * Possible actions to take when the signal is received are:
 *
 *  - Free caches
 *  - Save files that haven't been looked at in a while to disk, ready to be reopened when needed
 *  - Run a garbage collection cycle
 *  - Try and compress fragmented allocations
 *  - Exit on idle if the process has no reason to stay around
 *  - Call [`malloc_trim(3)`](man:malloc_trim) to return cached heap pages to
 *    the kernel (if supported by your libc)
 *
 * Note that some actions may not always improve system performance, and so
 * should be profiled for your application. `malloc_trim()`, for example, may
 * make future heap allocations slower (due to releasing cached heap pages back
 * to the kernel).
 *
 * See #GMemoryMonitorWarningLevel for details on the various warning levels.
 *
 * |[<!-- language="C" -->
 * static void
 * warning_cb (GMemoryMonitor *m, GMemoryMonitorWarningLevel level)
 * {
 *   g_debug ("Warning level: %d", level);
 *   if (warning_level > G_MEMORY_MONITOR_WARNING_LEVEL_LOW)
 *     drop_caches ();
 * }
 *
 * static GMemoryMonitor *
 * monitor_low_memory (void)
 * {
 *   GMemoryMonitor *m;
 *   m = g_memory_monitor_dup_default ();
 *   g_signal_connect (G_OBJECT (m), "low-memory-warning",
 *                     G_CALLBACK (warning_cb), NULL);
 *   return m;
 * }
 * ]|
 *
 * Don't forget to disconnect the #GMemoryMonitor::low-memory-warning
 * signal, and unref the #GMemoryMonitor itself when exiting.
 *
 * Since: 2.64
 */

/**
 * GMemoryMonitor:
 *
 * #GMemoryMonitor monitors system memory and indicates when
 * the system is low on memory.
 *
 * Since: 2.64
 */

/**
 * GMemoryMonitorInterface:
 * @g_iface: The parent interface.
 * @low_memory_warning: the virtual function pointer for the
 *  #GMemoryMonitor::low-memory-warning signal.
 *
 * The virtual function table for #GMemoryMonitor.
 *
 * Since: 2.64
 */

G_DEFINE_INTERFACE_WITH_CODE (GMemoryMonitor, g_memory_monitor, G_TYPE_OBJECT,
                              g_type_interface_add_prerequisite (g_define_type_id, G_TYPE_INITABLE))

enum {
  LOW_MEMORY_WARNING,
  LAST_SIGNAL
};

static guint signals[LAST_SIGNAL] = { 0 };

/**
 * g_memory_monitor_dup_default:
 *
 * Gets a reference to the default #GMemoryMonitor for the system.
 *
 * Returns: (not nullable) (transfer full): a new reference to the default #GMemoryMonitor
 *
 * Since: 2.64
 */
GMemoryMonitor *
g_memory_monitor_dup_default (void)
{
  return g_object_ref (_g_io_module_get_default (G_MEMORY_MONITOR_EXTENSION_POINT_NAME,
                                                 "GIO_USE_MEMORY_MONITOR",
                                                 NULL));
}

static void
g_memory_monitor_default_init (GMemoryMonitorInterface *iface)
{
  /**
   * GMemoryMonitor::low-memory-warning:
   * @monitor: a #GMemoryMonitor
   * @level: the #GMemoryMonitorWarningLevel warning level
   *
   * Emitted when the system is running low on free memory. The signal
   * handler should then take the appropriate action depending on the
   * warning level. See the #GMemoryMonitorWarningLevel documentation for
   * details.
   *
   * Since: 2.64
   */
  signals[LOW_MEMORY_WARNING] =
    g_signal_new (I_("low-memory-warning"),
                  G_TYPE_MEMORY_MONITOR,
                  G_SIGNAL_RUN_LAST,
                  G_STRUCT_OFFSET (GMemoryMonitorInterface, low_memory_warning),
                  NULL, NULL,
                  NULL,
                  G_TYPE_NONE, 1,
                  G_TYPE_MEMORY_MONITOR_WARNING_LEVEL);
}
