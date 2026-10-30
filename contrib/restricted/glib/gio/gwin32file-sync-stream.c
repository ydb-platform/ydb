/* gwin32file-sync-stream.c - a simple IStream implementation
 *
 * Copyright 2020 Руслан Ижбулатов
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

/* A COM object that implements an IStream backed by a file HANDLE.
 * Works just like `SHCreateStreamOnFileEx()`, but does not
 * support locking, and doesn't force us to link to libshlwapi.
 * Only supports synchronous access.
 */
#include <contrib/restricted/glib/config.h>
#define COBJMACROS
#define INITGUID
#include <windows.h>

#include "gwin32file-sync-stream.h"

static HRESULT STDMETHODCALLTYPE _file_sync_stream_query_interface (IStream         *self_ptr,
                                                                    REFIID           ref_interface_guid,
                                                                    LPVOID          *output_object_ptr);
static ULONG STDMETHODCALLTYPE   _file_sync_stream_release         (IStream         *self_ptr);
static ULONG STDMETHODCALLTYPE   _file_sync_stream_add_ref         (IStream         *self_ptr);

static HRESULT STDMETHODCALLTYPE _file_sync_stream_read            (IStream         *self_ptr,
                                                                    void            *output_data,
                                                                    ULONG            bytes_to_read,
                                                                    ULONG           *output_bytes_read);

static HRESULT STDMETHODCALLTYPE _file_sync_stream_write           (IStream         *self_ptr,
                                                                    const void      *data,
                                                                    ULONG            bytes_to_write,
                                                                    ULONG           *output_bytes_written);


static HRESULT STDMETHODCALLTYPE _file_sync_stream_clone           (IStream         *self_ptr,
                                                                    IStream        **output_clone_ptr);
static HRESULT STDMETHODCALLTYPE _file_sync_stream_commit          (IStream         *self_ptr,
                                                                    DWORD            commit_flags);
static HRESULT STDMETHODCALLTYPE _file_sync_stream_copy_to         (IStream         *self_ptr,
                                                                    IStream         *output_stream,
                                                                    ULARGE_INTEGER   bytes_to_copy,
                                                                    ULARGE_INTEGER  *output_bytes_read,
                                                                    ULARGE_INTEGER  *output_bytes_written);
static HRESULT STDMETHODCALLTYPE _file_sync_stream_lock_region     (IStream         *self_ptr,
                                                                    ULARGE_INTEGER   lock_offset,
                                                                    ULARGE_INTEGER   lock_bytes,
                                                                    DWORD            lock_type);
static HRESULT STDMETHODCALLTYPE _file_sync_stream_revert          (IStream         *self_ptr);
static HRESULT STDMETHODCALLTYPE _file_sync_stream_seek            (IStream         *self_ptr,
                                                                    LARGE_INTEGER    move_distance,
                                                                    DWORD            origin,
                                                                    ULARGE_INTEGER  *output_new_position);
static HRESULT STDMETHODCALLTYPE _file_sync_stream_set_size        (IStream         *self_ptr,
                                                                    ULARGE_INTEGER   new_size);
static HRESULT STDMETHODCALLTYPE _file_sync_stream_stat            (IStream         *self_ptr,
                                                                    STATSTG         *output_stat,
                                                                    DWORD            flags);
static HRESULT STDMETHODCALLTYPE _file_sync_stream_unlock_region   (IStream         *self_ptr,
                                                                    ULARGE_INTEGER   lock_offset,
                                                                    ULARGE_INTEGER   lock_bytes,
                                                                    DWORD            lock_type);

static void _file_sync_stream_free (GWin32FileSyncStream *self);

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_query_interface (IStream *self_ptr,
                                   REFIID   ref_interface_guid,
                                   LPVOID  *output_object_ptr)
{
  *output_object_ptr = NULL;

  if (IsEqualGUID (ref_interface_guid, &IID_IUnknown))
    {
      IUnknown_AddRef ((IUnknown *) self_ptr);
      *output_object_ptr = self_ptr;
      return S_OK;
    }
  else if (IsEqualGUID (ref_interface_guid, &IID_IStream))
    {
      IStream_AddRef (self_ptr);
      *output_object_ptr = self_ptr;
      return S_OK;
    }

  return E_NOINTERFACE;
}

static ULONG STDMETHODCALLTYPE
_file_sync_stream_add_ref (IStream *self_ptr)
{
  GWin32FileSyncStream *self = (GWin32FileSyncStream *) self_ptr;

  return ++self->ref_count;
}

static ULONG STDMETHODCALLTYPE
_file_sync_stream_release (IStream *self_ptr)
{
  GWin32FileSyncStream *self = (GWin32FileSyncStream *) self_ptr;

  int ref_count = --self->ref_count;

  if (ref_count == 0)
    _file_sync_stream_free (self);

  return ref_count;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_read (IStream *self_ptr,
                        void    *output_data,
                        ULONG    bytes_to_read,
                        ULONG   *output_bytes_read)
{
  GWin32FileSyncStream *self = (GWin32FileSyncStream *) self_ptr;
  DWORD bytes_read;

  if (!ReadFile (self->file_handle, output_data, bytes_to_read, &bytes_read, NULL))
    {
      DWORD error = GetLastError ();
      return __HRESULT_FROM_WIN32 (error);
    }

  if (output_bytes_read)
    *output_bytes_read = bytes_read;

  return S_OK;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_write (IStream    *self_ptr,
                         const void *data,
                         ULONG       bytes_to_write,
                         ULONG      *output_bytes_written)
{
  GWin32FileSyncStream *self = (GWin32FileSyncStream *) self_ptr;
  DWORD bytes_written;

  if (!WriteFile (self->file_handle, data, bytes_to_write, &bytes_written, NULL))
    {
      DWORD error = GetLastError ();
      return __HRESULT_FROM_WIN32 (error);
    }

  if (output_bytes_written)
    *output_bytes_written = bytes_written;

  return S_OK;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_seek (IStream        *self_ptr,
                        LARGE_INTEGER   move_distance,
                        DWORD           origin,
                        ULARGE_INTEGER *output_new_position)
{
  GWin32FileSyncStream *self = (GWin32FileSyncStream *) self_ptr;
  LARGE_INTEGER new_position;
  DWORD move_method;

  switch (origin)
    {
    case STREAM_SEEK_SET:
      move_method = FILE_BEGIN;
      break;
    case STREAM_SEEK_CUR:
      move_method = FILE_CURRENT;
      break;
    case STREAM_SEEK_END:
      move_method = FILE_END;
      break;
    default:
      return E_INVALIDARG;
    }

  if (!SetFilePointerEx (self->file_handle, move_distance, &new_position, move_method))
    {
      DWORD error = GetLastError ();
      return __HRESULT_FROM_WIN32 (error);
    }

  (*output_new_position).QuadPart = new_position.QuadPart;

  return S_OK;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_set_size (IStream        *self_ptr,
                            ULARGE_INTEGER  new_size)
{
  GWin32FileSyncStream *self = (GWin32FileSyncStream *) self_ptr;
  FILE_END_OF_FILE_INFO info;

  info.EndOfFile.QuadPart = new_size.QuadPart;

  if (SetFileInformationByHandle (self->file_handle, FileEndOfFileInfo, &info, sizeof (info)))
    {
      DWORD error = GetLastError ();
      return __HRESULT_FROM_WIN32 (error);
    }

  return S_OK;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_copy_to (IStream        *self_ptr,
                           IStream        *output_stream,
                           ULARGE_INTEGER  bytes_to_copy,
                           ULARGE_INTEGER *output_bytes_read,
                           ULARGE_INTEGER *output_bytes_written)
{
  ULARGE_INTEGER counter;
  ULARGE_INTEGER written_counter;
  ULARGE_INTEGER read_counter;

  counter.QuadPart = bytes_to_copy.QuadPart;
  written_counter.QuadPart = 0;
  read_counter.QuadPart = 0;

  while (counter.QuadPart > 0)
    {
      HRESULT hr;
      ULONG bytes_read;
      ULONG bytes_written;
      ULONG bytes_index;
#define _INTERNAL_BUFSIZE 1024
      BYTE buffer[_INTERNAL_BUFSIZE];
#undef _INTERNAL_BUFSIZE
      ULONG buffer_size = sizeof (buffer);
      ULONG to_read = buffer_size;

      if (counter.QuadPart < buffer_size)
        to_read = (ULONG) counter.QuadPart;

      /* Because MS SDK has a function IStream_Read() with 3 arguments */
      hr = self_ptr->lpVtbl->Read (self_ptr, buffer, to_read, &bytes_read);
      if (!SUCCEEDED (hr))
        return hr;

      read_counter.QuadPart += bytes_read;

      if (bytes_read == 0)
        break;

      bytes_index = 0;

      while (bytes_index < bytes_read)
        {
          /* Because MS SDK has a function IStream_Write() with 3 arguments */
          hr = output_stream->lpVtbl->Write (output_stream, &buffer[bytes_index], bytes_read - bytes_index, &bytes_written);
          if (!SUCCEEDED (hr))
            return hr;

          if (bytes_written == 0)
            return __HRESULT_FROM_WIN32 (ERROR_WRITE_FAULT);

          bytes_index += bytes_written;
          written_counter.QuadPart += bytes_written;
        }
    }

  if (output_bytes_read)
    output_bytes_read->QuadPart = read_counter.QuadPart;
  if (output_bytes_written)
    output_bytes_written->QuadPart = written_counter.QuadPart;

  return S_OK;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_commit (IStream *self_ptr,
                          DWORD    commit_flags)
{
  GWin32FileSyncStream *self = (GWin32FileSyncStream *) self_ptr;

  if (!FlushFileBuffers (self->file_handle))
    {
      DWORD error = GetLastError ();
      return __HRESULT_FROM_WIN32 (error);
    }

  return S_OK;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_revert (IStream *self_ptr)
{
  return E_NOTIMPL;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_lock_region (IStream        *self_ptr,
                               ULARGE_INTEGER  lock_offset,
                               ULARGE_INTEGER  lock_bytes,
                               DWORD           lock_type)
{
  return STG_E_INVALIDFUNCTION;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_unlock_region (IStream        *self_ptr,
                                 ULARGE_INTEGER  lock_offset,
                                 ULARGE_INTEGER  lock_bytes,
                                 DWORD           lock_type)
{
  return STG_E_INVALIDFUNCTION;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_stat (IStream *self_ptr,
                        STATSTG *output_stat,
                        DWORD    flags)
{
  GWin32FileSyncStream *self = (GWin32FileSyncStream *) self_ptr;
  BOOL get_name = FALSE;
  FILE_BASIC_INFO bi;
  FILE_STANDARD_INFO si;

  if (output_stat == NULL)
    return STG_E_INVALIDPOINTER;

  switch (flags)
    {
    case STATFLAG_DEFAULT:
      get_name = TRUE;
      break;
    case STATFLAG_NONAME:
      get_name = FALSE;
      break;
    default:
      return STG_E_INVALIDFLAG;
    }

  if (!GetFileInformationByHandleEx (self->file_handle, FileBasicInfo, &bi, sizeof (bi)) ||
      !GetFileInformationByHandleEx (self->file_handle, FileStandardInfo, &si, sizeof (si)))
    {
      DWORD error = GetLastError ();
      return __HRESULT_FROM_WIN32 (error);
    }

  output_stat->type = STGTY_STREAM;
  output_stat->mtime.dwLowDateTime = bi.LastWriteTime.LowPart;
  output_stat->mtime.dwHighDateTime = bi.LastWriteTime.HighPart;
  output_stat->ctime.dwLowDateTime = bi.CreationTime.LowPart;
  output_stat->ctime.dwHighDateTime = bi.CreationTime.HighPart;
  output_stat->atime.dwLowDateTime = bi.LastAccessTime.LowPart;
  output_stat->atime.dwHighDateTime = bi.LastAccessTime.HighPart;
  output_stat->grfLocksSupported = 0;
  memset (&output_stat->clsid, 0, sizeof (CLSID));
  output_stat->grfStateBits = 0;
  output_stat->reserved = 0;
  output_stat->cbSize.QuadPart = si.EndOfFile.QuadPart;
  output_stat->grfMode = self->stgm_mode;

  if (get_name)
    {
      DWORD tries;
      wchar_t *buffer;

      /* Nothing in the documentation guarantees that the name
       * won't change between two invocations (one - to get the
       * buffer size, the other - to fill the buffer).
       * Re-try up to 5 times in case the required buffer size
       * doesn't match.
       */
      for (tries = 5; tries > 0; tries--)
        {
          DWORD buffer_size;
          DWORD buffer_size2;
          DWORD error;

          buffer_size = GetFinalPathNameByHandleW (self->file_handle, NULL, 0, 0);

          if (buffer_size == 0)
            {
              DWORD error = GetLastError ();
              return __HRESULT_FROM_WIN32 (error);
            }

          buffer = CoTaskMemAlloc (buffer_size);
          buffer[buffer_size - 1] = 0;
          buffer_size2 = GetFinalPathNameByHandleW (self->file_handle, buffer, buffer_size, 0);

          if (buffer_size2 < buffer_size)
            break;

          error = GetLastError ();
          CoTaskMemFree (buffer);
          if (buffer_size2 == 0)
            return __HRESULT_FROM_WIN32 (error);
        }

      if (tries == 0)
        return __HRESULT_FROM_WIN32 (ERROR_BAD_LENGTH);

      output_stat->pwcsName = buffer;
    }
  else
    output_stat->pwcsName = NULL;

  return S_OK;
}

static HRESULT STDMETHODCALLTYPE
_file_sync_stream_clone (IStream  *self_ptr,
                         IStream **output_clone_ptr)
{
  return E_NOTIMPL;
}

static IStreamVtbl _file_sync_stream_vtbl = {
  _file_sync_stream_query_interface,
  _file_sync_stream_add_ref,
  _file_sync_stream_release,
  _file_sync_stream_read,
  _file_sync_stream_write,
  _file_sync_stream_seek,
  _file_sync_stream_set_size,
  _file_sync_stream_copy_to,
  _file_sync_stream_commit,
  _file_sync_stream_revert,
  _file_sync_stream_lock_region,
  _file_sync_stream_unlock_region,
  _file_sync_stream_stat,
  _file_sync_stream_clone,
};

static void
_file_sync_stream_free (GWin32FileSyncStream *self)
{
  if (self->owns_handle)
    CloseHandle (self->file_handle);

  g_free (self);
}

/**
 * g_win32_file_sync_stream_new:
 * @handle: a Win32 HANDLE for a file.
 * @owns_handle: %TRUE if newly-created stream owns the handle
 *               (and closes it when destroyed)
 * @stgm_mode: a combination of [STGM constants](https://docs.microsoft.com/en-us/windows/win32/stg/stgm-constants)
 *             that specify the mode with which the stream
 *             is opened.
 * @output_hresult: (out) (optional): a HRESULT from the internal COM calls.
 *                                    Will be `S_OK` on success.
 *
 * Creates an IStream object backed by a HANDLE.
 *
 * @stgm_mode should match the mode of the @handle, otherwise the stream might
 * attempt to perform operations that the @handle does not allow. The implementation
 * itself ignores these flags completely, they are only used to report
 * the mode of the stream to third parties.
 *
 * The stream only does synchronous access and will never return `E_PENDING` on I/O.
 *
 * The returned stream object should be treated just like any other
 * COM object, and released via `IUnknown_Release()`.
 * its elements have been unreffed with g_object_unref().
 *
 * Returns: (nullable) (transfer full): a new IStream object on success, %NULL on failure.
 **/
IStream *
g_win32_file_sync_stream_new (HANDLE    file_handle,
                              gboolean  owns_handle,
                              DWORD     stgm_mode,
                              HRESULT  *output_hresult)
{
  GWin32FileSyncStream *new_stream;
  IStream *result;
  HRESULT hr;

  new_stream = g_new0 (GWin32FileSyncStream, 1);
  new_stream->self.lpVtbl = &_file_sync_stream_vtbl;

  hr = IUnknown_QueryInterface ((IUnknown *) new_stream, &IID_IStream, (void **) &result);
  if (!SUCCEEDED (hr))
    {
      g_free (new_stream);

      if (output_hresult)
        *output_hresult = hr;

      return NULL;
    }

  new_stream->stgm_mode = stgm_mode;
  new_stream->file_handle = file_handle;
  new_stream->owns_handle = owns_handle;

  if (output_hresult)
    *output_hresult = S_OK;

  return result;
}
