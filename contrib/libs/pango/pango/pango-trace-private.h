/* Pango
 * pango-trace-private.h:
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once


#ifdef HAVE_SYSPROF
#error #include <sysprof-capture.h>
#endif

#include <glib.h>

G_BEGIN_DECLS

#ifdef HAVE_SYSPROF
#define PANGO_TRACE_CURRENT_TIME SYSPROF_CAPTURE_CURRENT_TIME
#else
#define PANGO_TRACE_CURRENT_TIME 0
#endif

void pango_trace_mark (gint64       begin_time,
                       const gchar *name,
                       const gchar *message_format,
                       ...) G_GNUC_PRINTF (3, 4);

#ifndef HAVE_SYSPROF
/* Optimise the whole call out */
#if defined(G_HAVE_ISO_VARARGS)
#define pango_trace_mark(b, n, m, ...) G_STMT_START { } G_STMT_END
#elif defined(G_HAVE_GNUC_VARARGS)
#define pango_trace_mark(b, n, m...) G_STMT_START { } G_STMT_END
#else
/* no varargs macro support; the call will have to be optimised out by the compiler */
#endif
#endif

G_END_DECLS
