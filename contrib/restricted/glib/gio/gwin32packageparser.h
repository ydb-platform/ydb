/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright (C) 2020 Руслан Ижбулатов <lrn1986@gmail.com>
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
 *
 */
#ifndef __G_WIN32_PACKAGE_PARSER_H__
#define __G_WIN32_PACKAGE_PARSER_H__

#include <gio/gio.h>

#ifdef G_PLATFORM_WIN32

typedef struct _GWin32PackageExtGroup GWin32PackageExtGroup;

struct _GWin32PackageExtGroup
{
  GPtrArray *verbs;
  GPtrArray *extensions;
};

typedef gboolean (*GWin32PackageParserCallback)(gpointer         user_data,
                                                const gunichar2 *full_package_name,
                                                const gunichar2 *package_name,
                                                const gunichar2 *app_user_model_id,
                                                gboolean         show_in_applist,
                                                GPtrArray       *supported_extgroups,
                                                GPtrArray       *supported_protocols);

gboolean g_win32_package_parser_enum_packages (GWin32PackageParserCallback   callback,
                                               gpointer                      user_data,
                                               GError                      **error);

#endif /* G_PLATFORM_WIN32 */

#endif /* __G_WIN32_PACKAGE_PARSER_H__ */