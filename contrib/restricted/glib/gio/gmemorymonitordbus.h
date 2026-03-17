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

#ifndef __G_MEMORY_MONITOR_DBUS_H__
#define __G_MEMORY_MONITOR_DBUS_H__

#include <glib-object.h>

G_BEGIN_DECLS

#define G_TYPE_MEMORY_MONITOR_DBUS         (g_memory_monitor_dbus_get_type ())
G_DECLARE_FINAL_TYPE (GMemoryMonitorDBus, g_memory_monitor_dbus, G, MEMORY_MONITOR_DBUS, GObject)

G_END_DECLS

#endif /* __G_MEMORY_MONITOR_DBUS_H__ */
