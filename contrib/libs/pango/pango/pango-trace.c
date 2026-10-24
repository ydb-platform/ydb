/* Pango
 * pango-trace.c:
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

#include "config.h"

#include "pango-trace-private.h"

#include <stdarg.h>

void
(pango_trace_mark) (gint64       begin_time,
                    const gchar *name,
                    const gchar *message_format,
                    ...)
{
#ifdef HAVE_SYSPROF
  gint64 end_time = PANGO_TRACE_CURRENT_TIME;
  va_list args;

  va_start (args, message_format);
  sysprof_collector_mark_vprintf (begin_time, end_time - begin_time, "Pango", name, message_format, args);
  va_end (args);
#endif  /* HAVE_SYSPROF */
}
