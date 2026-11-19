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
#ifndef __G_WIN32_FILE_SYNC_STREAM_H__
#define __G_WIN32_FILE_SYNC_STREAM_H__

#include <gio/gio.h>

#ifdef G_PLATFORM_WIN32

#include <objidl.h>

typedef struct _GWin32FileSyncStream GWin32FileSyncStream;

struct _GWin32FileSyncStream
{
  IStream  self;
  ULONG    ref_count;
  HANDLE   file_handle;
  gboolean owns_handle;
  DWORD    stgm_mode;
};

IStream *g_win32_file_sync_stream_new (HANDLE    file_handle,
                                       gboolean  owns_handle,
                                       DWORD     stgm_mode,
                                       HRESULT  *output_hresult);

#endif /* G_PLATFORM_WIN32 */

#endif /* __G_WIN32_FILE_SYNC_STREAM_H__ */