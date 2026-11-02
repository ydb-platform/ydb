/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright (C) 2008 Red Hat, Inc.
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

#ifndef __G_THREADED_RESOLVER_H__
#define __G_THREADED_RESOLVER_H__

#include <gio/gresolver.h>

G_BEGIN_DECLS

#define G_TYPE_THREADED_RESOLVER         (g_threaded_resolver_get_type ())
#define G_THREADED_RESOLVER(o)           (G_TYPE_CHECK_INSTANCE_CAST ((o), G_TYPE_THREADED_RESOLVER, GThreadedResolver))
#define G_THREADED_RESOLVER_CLASS(k)     (G_TYPE_CHECK_CLASS_CAST((k), G_TYPE_THREADED_RESOLVER, GThreadedResolverClass))
#define G_IS_THREADED_RESOLVER(o)        (G_TYPE_CHECK_INSTANCE_TYPE ((o), G_TYPE_THREADED_RESOLVER))
#define G_IS_THREADED_RESOLVER_CLASS(k)  (G_TYPE_CHECK_CLASS_TYPE ((k), G_TYPE_THREADED_RESOLVER))
#define G_THREADED_RESOLVER_GET_CLASS(o) (G_TYPE_INSTANCE_GET_CLASS ((o), G_TYPE_THREADED_RESOLVER, GThreadedResolverClass))

typedef struct {
  GResolver parent_instance;
} GThreadedResolver;

typedef struct {
  GResolverClass parent_class;

} GThreadedResolverClass;

GLIB_AVAILABLE_IN_ALL
GType g_threaded_resolver_get_type (void) G_GNUC_CONST;

/* Used for a private test API */
#ifdef G_OS_UNIX
GLIB_AVAILABLE_IN_ALL
GList *g_resolver_records_from_res_query (const gchar      *rrname,
                                          gint              rrtype,
                                          const guint8     *answer,
                                          gssize            len,
                                          gint              herr,
                                          GError          **error);
GLIB_AVAILABLE_IN_ALL
gint g_resolver_record_type_to_rrtype (GResolverRecordType type);
#endif

G_END_DECLS

#endif /* __G_RESOLVER_H__ */
