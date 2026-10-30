/*
 * Copyright © 2020 Endless Mobile, Inc.
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
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 *
 * Author: Philip Withnall <withnall@endlessm.com>
 */

#pragma once

#ifdef HAVE_SYSPROF
#error #include <sysprof-capture.h>
#endif

#include "glib.h"

G_BEGIN_DECLS

/*
 * G_TRACE_CURRENT_TIME:
 *
 * Get the current time, in nanoseconds since the tracing epoch. This (and only
 * this) is suitable for passing to tracing functions like g_trace_mark(). It is
 * not suitable for other timekeeping.
 *
 * The tracing epoch is implementation defined, but is guaranteed to be
 * unchanged within the lifetime of each thread. It is not comparable across
 * threads or process instances.
 *
 * If tracing support is disabled, this evaluates to `0`.
 *
 * Since: 2.66
 */
#ifdef HAVE_SYSPROF
#define G_TRACE_CURRENT_TIME SYSPROF_CAPTURE_CURRENT_TIME
#else
#define G_TRACE_CURRENT_TIME 0
#endif

void (g_trace_mark) (gint64       begin_time_nsec,
                     gint64       duration_nsec,
                     const gchar *group,
                     const gchar *name,
                     const gchar *message_format,
                     ...) G_GNUC_PRINTF (5, 6);

#ifndef HAVE_SYSPROF
/* Optimise the whole call out */
#if defined(G_HAVE_ISO_VARARGS)
#define g_trace_mark(b, d, g, n, m, ...)
#elif defined(G_HAVE_GNUC_VARARGS)
#define g_trace_mark(b, d, g, n, m...)
#else
/* no varargs macro support; the call will have to be optimised out by the compiler */
#endif
#endif

guint   (g_trace_define_int64_counter) (const char *group,
                                        const char *name,
                                        const char *description);
void    (g_trace_set_int64_counter)    (guint       id,
                                        gint64      value);

#ifndef HAVE_SYSPROF
#define g_trace_define_int64_counter(g, n, d) ((guint) -1)
#define g_trace_set_int64_counter(i,v)
#endif

G_END_DECLS
