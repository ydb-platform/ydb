/* gconvertprivate.h - Private GLib gconvert functions
 *
 * Copyright 2020 Frederic Martinsons
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
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __G_CONVERTPRIVATE_H__
#define __G_CONVERTPRIVATE_H__

G_BEGIN_DECLS

#include "glib.h"

gchar *_g_time_locale_to_utf8 (const gchar *opsysstring,
                               gssize len,
                               gsize *bytes_read,
                               gsize *bytes_written,
                               GError **error) G_GNUC_MALLOC;

gchar *_g_ctype_locale_to_utf8 (const gchar *opsysstring,
                                gssize len,
                                gsize *bytes_read,
                                gsize *bytes_written,
                                GError **error) G_GNUC_MALLOC;

G_END_DECLS

#endif /* __G_CONVERTPRIVATE_H__ */
