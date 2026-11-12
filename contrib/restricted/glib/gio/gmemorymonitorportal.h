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

#ifndef __G_MEMORY_MONITOR_PORTAL_H__
#define __G_MEMORY_MONITOR_PORTAL_H__

#include <glib-object.h>

G_BEGIN_DECLS

#define G_TYPE_MEMORY_MONITOR_PORTAL         (g_memory_monitor_portal_get_type ())
G_DECLARE_FINAL_TYPE (GMemoryMonitorPortal, g_memory_monitor_portal, G, MEMORY_MONITOR_PORTAL, GObject)

G_END_DECLS

#endif /* __G_MEMORY_MONITOR_PORTAL_H__ */
