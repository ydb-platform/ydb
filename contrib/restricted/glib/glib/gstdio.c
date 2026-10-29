/* gstdio.c - wrappers for C library functions
 *
 * Copyright 2004 Tor Lillqvist
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

#include <contrib/restricted/glib/config.h>
#include "glibconfig.h"

/* Don’t redefine (for example) g_open() to open(), since we actually want to
 * define g_open() in this file and export it as a symbol. See gstdio.h. */
#define G_STDIO_WRAP_ON_UNIX

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef G_OS_UNIX
#include <unistd.h>
#endif

#ifdef G_OS_WIN32
#include <windows.h>
#include <winioctl.h>
#include <errno.h>
#include <wchar.h>
#include <direct.h>
#include <io.h>
#include <sys/utime.h>
#include <stdlib.h> /* for MB_CUR_MAX */
#else
#include <utime.h>
#include <errno.h>
#endif

#include "gstdio.h"
#include "gstdioprivate.h"

#if !defined (G_OS_UNIX) && !defined (G_OS_WIN32)
#error Please port this to your operating system
#endif

#if defined (_MSC_VER) && !defined(_WIN64)
#undef _wstat
#define _wstat _wstat32
#endif

#if defined (G_OS_WIN32)

/* We can't include Windows DDK and Windows SDK simultaneously,
 * so let's copy this here from MinGW-w64 DDK.
 * The structure is ultimately documented here:
 * https://msdn.microsoft.com/en-us/library/ff552012(v=vs.85).aspx
 */
typedef struct _REPARSE_DATA_BUFFER
{
  ULONG  ReparseTag;
  USHORT ReparseDataLength;
  USHORT Reserved;
  union
  {
    struct
    {
      USHORT SubstituteNameOffset;
      USHORT SubstituteNameLength;
      USHORT PrintNameOffset;
      USHORT PrintNameLength;
      ULONG  Flags;
      WCHAR  PathBuffer[1];
    } SymbolicLinkReparseBuffer;
    struct
    {
      USHORT SubstituteNameOffset;
      USHORT SubstituteNameLength;
      USHORT PrintNameOffset;
      USHORT PrintNameLength;
      WCHAR  PathBuffer[1];
    } MountPointReparseBuffer;
    struct
    {
      UCHAR  DataBuffer[1];
    } GenericReparseBuffer;
  };
} REPARSE_DATA_BUFFER, *PREPARSE_DATA_BUFFER;

static int
w32_error_to_errno (DWORD error_code)
{
  switch (error_code)
    {
    case ERROR_ACCESS_DENIED:
      return EACCES;
      break;
    case ERROR_ALREADY_EXISTS:
    case ERROR_FILE_EXISTS:
      return EEXIST;
    case ERROR_FILE_NOT_FOUND:
      return ENOENT;
      break;
    case ERROR_INVALID_FUNCTION:
      return EFAULT;
      break;
    case ERROR_INVALID_HANDLE:
      return EBADF;
      break;
    case ERROR_INVALID_PARAMETER:
      return EINVAL;
      break;
    case ERROR_LOCK_VIOLATION:
    case ERROR_SHARING_VIOLATION:
      return EACCES;
      break;
    case ERROR_NOT_ENOUGH_MEMORY:
    case ERROR_OUTOFMEMORY:
      return ENOMEM;
      break;
    case ERROR_NOT_SAME_DEVICE:
      return EXDEV;
      break;
    case ERROR_PATH_NOT_FOUND:
      return ENOENT; /* or ELOOP, or ENAMETOOLONG */
      break;
    default:
      return EIO;
      break;
    }
}

#include "gstdio-private.c"

/* Windows implementation of fopen() does not accept modes such as
 * "wb+". The 'b' needs to be appended to "w+", i.e. "w+b". Note
 * that otherwise these 2 modes are supposed to be aliases, hence
 * swappable at will. TODO: Is this still true?
 */
static void
_g_win32_fix_mode (wchar_t *mode)
{
  wchar_t *ptr;
  wchar_t temp;

  ptr = wcschr (mode, L'+');
  if (ptr != NULL && (ptr - mode) > 1)
    {
      temp = mode[1];
      mode[1] = *ptr;
      *ptr = temp;
    }
}

/* From
 * https://support.microsoft.com/en-ca/help/167296/how-to-convert-a-unix-time-t-to-a-win32-filetime-or-systemtime
 * FT = UT * 10000000 + 116444736000000000.
 * Therefore:
 * UT = (FT - 116444736000000000) / 10000000.
 * Converts FILETIME to unix epoch time in form
 * of a signed 64-bit integer (can be negative).
 *
 * The function that does the reverse can be found in
 * gio/glocalfileinfo.c.
 */
static gint64
_g_win32_filetime_to_unix_time (const FILETIME *ft,
                                gint32         *nsec)
{
  gint64 result;
  /* 1 unit of FILETIME is 100ns */
  const gint64 hundreds_of_usec_per_sec = 10000000;
  /* The difference between January 1, 1601 UTC (FILETIME epoch) and UNIX epoch
   * in hundreds of nanoseconds.
   */
  const gint64 filetime_unix_epoch_offset = 116444736000000000;

  result = ((gint64) ft->dwLowDateTime) | (((gint64) ft->dwHighDateTime) << 32);
  result -= filetime_unix_epoch_offset;

  if (nsec)
    *nsec = (result % hundreds_of_usec_per_sec) * 100;

  return result / hundreds_of_usec_per_sec;
}

#  ifdef _MSC_VER
#    ifndef S_IXUSR
#      define _S_IRUSR _S_IREAD
#      define _S_IWUSR _S_IWRITE
#      define _S_IXUSR _S_IEXEC
#      define S_IRUSR _S_IRUSR
#      define S_IWUSR _S_IWUSR
#      define S_IXUSR _S_IXUSR
#      define S_IRGRP (S_IRUSR >> 3)
#      define S_IWGRP (S_IWUSR >> 3)
#      define S_IXGRP (S_IXUSR >> 3)
#      define S_IROTH (S_IRGRP >> 3)
#      define S_IWOTH (S_IWGRP >> 3)
#      define S_IXOTH (S_IXGRP >> 3)
#    endif
#    ifndef S_ISDIR
#      define S_ISDIR(m) (((m) & _S_IFMT) == _S_IFDIR)
#    endif
#  endif

/* Uses filename and BHFI to fill a stat64 structure.
 * Tries to reproduce the behaviour and quirks of MS C runtime stat().
 */
static int
_g_win32_fill_statbuf_from_handle_info (const wchar_t                    *filename,
                                        const wchar_t                    *filename_target,
                                        const BY_HANDLE_FILE_INFORMATION *handle_info,
                                        struct __stat64                  *statbuf)
{
  wchar_t drive_letter_w = 0;
  size_t drive_letter_size = MB_CUR_MAX;
  char *drive_letter = _alloca (drive_letter_size);

  /* If filename (target or link) is absolute,
   * then use the drive letter from it as-is.
   */
  if (filename_target != NULL &&
      filename_target[0] != L'\0' &&
      filename_target[1] == L':')
    drive_letter_w = filename_target[0];
  else if (filename[0] != L'\0' &&
           filename[1] == L':')
    drive_letter_w = filename[0];

  if (drive_letter_w > 0 &&
      iswalpha (drive_letter_w) &&
      iswascii (drive_letter_w) &&
      wctomb (drive_letter, drive_letter_w) == 1)
    statbuf->st_dev = toupper (drive_letter[0]) - 'A'; /* 0 means A: drive */
  else
    /* Otherwise use the PWD drive.
     * Return value of 0 gives us 0 - 1 = -1,
     * which is the "no idea" value for st_dev.
     */
    statbuf->st_dev = _getdrive () - 1;

  statbuf->st_rdev = statbuf->st_dev;
  /* Theoretically, it's possible to set it for ext-FS. No idea how.
   * Meaningless for all filesystems that Windows normally uses.
   */
  statbuf->st_ino = 0;
  statbuf->st_mode = 0;

  if ((handle_info->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == FILE_ATTRIBUTE_DIRECTORY)
    statbuf->st_mode |= S_IFDIR | S_IXUSR | S_IXGRP | S_IXOTH;
  else
    statbuf->st_mode |= S_IFREG;
  /* No idea what S_IFCHR means here. */
  /* S_IFIFO is not even mentioned in MSDN */
  /* S_IFBLK is also not mentioned */

  /* The aim here is to reproduce MS stat() behaviour,
   * even if it's braindead.
   */
  statbuf->st_mode |= S_IRUSR | S_IRGRP | S_IROTH;
  if ((handle_info->dwFileAttributes & FILE_ATTRIBUTE_READONLY) != FILE_ATTRIBUTE_READONLY)
    statbuf->st_mode |= S_IWUSR | S_IWGRP | S_IWOTH;

  if (!S_ISDIR (statbuf->st_mode))
    {
      const wchar_t *name;
      const wchar_t *dot = NULL;

      if (filename_target != NULL)
        name = filename_target;
      else
        name = filename;

      do
        {
          wchar_t *last_dot = wcschr (name, L'.');
          if (last_dot == NULL)
            break;
          dot = last_dot;
          name = &last_dot[1];
        }
      while (TRUE);

      if ((dot != NULL &&
          (wcsicmp (dot, L".exe") == 0 ||
           wcsicmp (dot, L".com") == 0 ||
           wcsicmp (dot, L".bat") == 0 ||
           wcsicmp (dot, L".cmd") == 0)))
        statbuf->st_mode |= S_IXUSR | S_IXGRP | S_IXOTH;
    }

  statbuf->st_nlink = handle_info->nNumberOfLinks;
  statbuf->st_uid = statbuf->st_gid = 0;
  statbuf->st_size = (((guint64) handle_info->nFileSizeHigh) << 32) | handle_info->nFileSizeLow;
  statbuf->st_ctime = _g_win32_filetime_to_unix_time (&handle_info->ftCreationTime, NULL);
  statbuf->st_mtime = _g_win32_filetime_to_unix_time (&handle_info->ftLastWriteTime, NULL);
  statbuf->st_atime = _g_win32_filetime_to_unix_time (&handle_info->ftLastAccessTime, NULL);

  return 0;
}

/* Fills our private stat-like structure using data from
 * a normal stat64 struct, BHFI, FSI and a reparse tag.
 */
static void
_g_win32_fill_privatestat (const struct __stat64            *statbuf,
                           const BY_HANDLE_FILE_INFORMATION *handle_info,
                           const FILE_STANDARD_INFO         *std_info,
                           DWORD                             reparse_tag,
                           GWin32PrivateStat                *buf)
{
  buf->st_dev = statbuf->st_dev;
  buf->st_ino = statbuf->st_ino;
  buf->st_mode = statbuf->st_mode;
  buf->volume_serial = handle_info->dwVolumeSerialNumber;
  buf->file_index = (((guint64) handle_info->nFileIndexHigh) << 32) | handle_info->nFileIndexLow;
  buf->attributes = handle_info->dwFileAttributes;
  buf->st_nlink = handle_info->nNumberOfLinks;
  buf->st_size = (((guint64) handle_info->nFileSizeHigh) << 32) | handle_info->nFileSizeLow;
  buf->allocated_size = std_info->AllocationSize.QuadPart;

  buf->reparse_tag = reparse_tag;

  buf->st_ctim.tv_sec = _g_win32_filetime_to_unix_time (&handle_info->ftCreationTime, &buf->st_ctim.tv_nsec);
  buf->st_mtim.tv_sec = _g_win32_filetime_to_unix_time (&handle_info->ftLastWriteTime, &buf->st_mtim.tv_nsec);
  buf->st_atim.tv_sec = _g_win32_filetime_to_unix_time (&handle_info->ftLastAccessTime, &buf->st_atim.tv_nsec);
}

/* Read the link data from a symlink/mountpoint represented
 * by the handle. Also reads reparse tag.
 * @reparse_tag receives the tag. Can be %NULL if @buf or @alloc_buf
 *              is non-NULL.
 * @buf receives the link data. Can be %NULL if reparse_tag is non-%NULL.
 *      Mutually-exclusive with @alloc_buf.
 * @buf_size is the size of the @buf, in bytes.
 * @alloc_buf points to a location where internally-allocated buffer
 *            pointer will be written. That buffer receives the
 *            link data. Mutually-exclusive with @buf.
 * @terminate ensures that the buffer is NUL-terminated if
 *            it isn't already. Note that this can erase useful
 *            data if @buf is provided and @buf_size is too small.
 *            Specifically, with @buf_size <= 2 the buffer will
 *            receive an empty string, even if there is some
 *            data in the reparse point.
 * The contents of @buf or @alloc_buf are presented as-is - could
 * be non-NUL-terminated (unless @terminate is %TRUE) or even malformed.
 * Returns the number of bytes (!) placed into @buf or @alloc_buf,
 * including NUL-terminator (if any).
 *
 * Returned value of 0 means that there's no recognizable data in the
 * reparse point. @alloc_buf will not be allocated in that case,
 * and @buf will be left unmodified.
 *
 * If @buf and @alloc_buf are %NULL, returns 0 to indicate success.
 * Returns -1 to indicate an error, sets errno.
 */
static int
_g_win32_readlink_handle_raw (HANDLE      h,
                              DWORD      *reparse_tag,
                              gunichar2  *buf,
                              gsize       buf_size,
                              gunichar2 **alloc_buf,
                              gboolean    terminate)
{
  DWORD error_code;
  DWORD returned_bytes = 0;
  BYTE *data = NULL;
  gsize to_copy;
  /* This is 16k. It's impossible to make DeviceIoControl() tell us
   * the required size. NtFsControlFile() does have such a feature,
   * but for some reason it doesn't work with CreateFile()-returned handles.
   * The only alternative is to repeatedly call DeviceIoControl()
   * with bigger and bigger buffers, until it succeeds.
   * We choose to sacrifice stack space for speed.
   */
  BYTE max_buffer[sizeof (REPARSE_DATA_BUFFER) + MAXIMUM_REPARSE_DATA_BUFFER_SIZE] = {0,};
  DWORD max_buffer_size = sizeof (REPARSE_DATA_BUFFER) + MAXIMUM_REPARSE_DATA_BUFFER_SIZE;
  REPARSE_DATA_BUFFER *rep_buf;

  g_return_val_if_fail ((buf != NULL || alloc_buf != NULL || reparse_tag != NULL) &&
                        (buf == NULL || alloc_buf == NULL),
                        -1);

  if (!DeviceIoControl (h, FSCTL_GET_REPARSE_POINT, NULL, 0,
                        max_buffer,
                        max_buffer_size,
                        &returned_bytes, NULL))
    {
      error_code = GetLastError ();
      errno = w32_error_to_errno (error_code);
      return -1;
    }

  rep_buf = (REPARSE_DATA_BUFFER *) max_buffer;

  if (reparse_tag != NULL)
    *reparse_tag = rep_buf->ReparseTag;

  if (buf == NULL && alloc_buf == NULL)
    return 0;

  if (rep_buf->ReparseTag == IO_REPARSE_TAG_SYMLINK)
    {
      data = &((BYTE *) rep_buf->SymbolicLinkReparseBuffer.PathBuffer)[rep_buf->SymbolicLinkReparseBuffer.SubstituteNameOffset];

      to_copy = rep_buf->SymbolicLinkReparseBuffer.SubstituteNameLength;
    }
  else if (rep_buf->ReparseTag == IO_REPARSE_TAG_MOUNT_POINT)
    {
      data = &((BYTE *) rep_buf->MountPointReparseBuffer.PathBuffer)[rep_buf->MountPointReparseBuffer.SubstituteNameOffset];

      to_copy = rep_buf->MountPointReparseBuffer.SubstituteNameLength;
    }
  else
    to_copy = 0;

  return _g_win32_copy_and_maybe_terminate (data, to_copy, buf, buf_size, alloc_buf, terminate);
}

/* Read the link data from a symlink/mountpoint represented
 * by the @filename.
 * @filename is the name of the file.
 * @reparse_tag receives the tag. Can be %NULL if @buf or @alloc_buf
 *              is non-%NULL.
 * @buf receives the link data. Mutually-exclusive with @alloc_buf.
 * @buf_size is the size of the @buf, in bytes.
 * @alloc_buf points to a location where internally-allocated buffer
 *            pointer will be written. That buffer receives the
 *            link data. Mutually-exclusive with @buf.
 * @terminate ensures that the buffer is NUL-terminated if
 *            it isn't already
 * The contents of @buf or @alloc_buf are presented as-is - could
 * be non-NUL-terminated (unless @terminate is TRUE) or even malformed.
 * Returns the number of bytes (!) placed into @buf or @alloc_buf.
 * Returned value of 0 means that there's no recognizable data in the
 * reparse point. @alloc_buf will not be allocated in that case,
 * and @buf will be left unmodified.
 * If @buf and @alloc_buf are %NULL, returns 0 to indicate success.
 * Returns -1 to indicate an error, sets errno.
 */
static int
_g_win32_readlink_utf16_raw (const gunichar2  *filename,
                             DWORD            *reparse_tag,
                             gunichar2        *buf,
                             gsize             buf_size,
                             gunichar2       **alloc_buf,
                             gboolean          terminate)
{
  HANDLE h;
  DWORD attributes;
  DWORD to_copy;
  DWORD error_code;

  if ((attributes = GetFileAttributesW (filename)) == 0)
    {
      error_code = GetLastError ();
      errno = w32_error_to_errno (error_code);
      return -1;
    }

  if ((attributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0)
    {
      errno = EINVAL;
      return -1;
    }

  /* To read symlink target we need to open the file as a reparse
   * point and use DeviceIoControl() on it.
   */
  h = CreateFileW (filename,
                   FILE_READ_EA,
                   FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
                   NULL, OPEN_EXISTING,
                   FILE_ATTRIBUTE_NORMAL
                   | FILE_FLAG_OPEN_REPARSE_POINT
                   | (attributes & FILE_ATTRIBUTE_DIRECTORY ? FILE_FLAG_BACKUP_SEMANTICS : 0),
                   NULL);

  if (h == INVALID_HANDLE_VALUE)
    {
      error_code = GetLastError ();
      errno = w32_error_to_errno (error_code);
      return -1;
    }

  to_copy = _g_win32_readlink_handle_raw (h, reparse_tag, buf, buf_size, alloc_buf, terminate);

  CloseHandle (h);

  return to_copy;
}

/* Read the link data from a symlink/mountpoint represented
 * by a UTF-16 filename or a file handle.
 * @filename is the name of the file. Mutually-exclusive with @file_handle.
 * @file_handle is the handle of the file. Mutually-exclusive with @filename.
 * @reparse_tag receives the tag. Can be %NULL if @buf or @alloc_buf
 *              is non-%NULL.
 * @buf receives the link data. Mutually-exclusive with @alloc_buf.
 * @buf_size is the size of the @buf, in bytes.
 * @alloc_buf points to a location where internally-allocated buffer
 *            pointer will be written. That buffer receives the
 *            link data. Mutually-exclusive with @buf.
 * @terminate ensures that the buffer is NUL-terminated if
 *            it isn't already
 * The contents of @buf or @alloc_buf are adjusted
 * (extended or nt object manager prefix is stripped),
 * but otherwise they are presented as-is - could be non-NUL-terminated
 * (unless @terminate is TRUE) or even malformed.
 * Returns the number of bytes (!) placed into @buf or @alloc_buf.
 * Returned value of 0 means that there's no recognizable data in the
 * reparse point. @alloc_buf will not be allocated in that case,
 * and @buf will be left unmodified.
 * Returns -1 to indicate an error, sets errno.
 */
static int
_g_win32_readlink_utf16_handle (const gunichar2  *filename,
                                HANDLE            file_handle,
                                DWORD            *reparse_tag,
                                gunichar2        *buf,
                                gsize             buf_size,
                                gunichar2       **alloc_buf,
                                gboolean          terminate)
{
  int   result;
  gsize string_size;

  g_return_val_if_fail ((buf != NULL || alloc_buf != NULL || reparse_tag != NULL) &&
                        (filename != NULL || file_handle != NULL) &&
                        (buf == NULL || alloc_buf == NULL) &&
                        (filename == NULL || file_handle == NULL),
                        -1);

  if (filename)
    result = _g_win32_readlink_utf16_raw (filename, reparse_tag, buf, buf_size, alloc_buf, terminate);
  else
    result = _g_win32_readlink_handle_raw (file_handle, reparse_tag, buf, buf_size, alloc_buf, terminate);

  if (result <= 0)
    return result;

  /* Ensure that output is a multiple of sizeof (gunichar2),
   * cutting any trailing partial gunichar2, if present.
   */
  result -= result % sizeof (gunichar2);

  if (result <= 0)
    return result;

  /* DeviceIoControl () tends to return filenames as NT Object Manager
   * names , i.e. "\\??\\C:\\foo\\bar".
   * Remove the leading 4-byte "\\??\\" prefix, as glib (as well as many W32 API
   * functions) is unprepared to deal with it. Unless it has no 'x:' drive
   * letter part after the prefix, in which case we leave everything
   * as-is, because the path could be "\\??\\Volume{GUID}" - stripping
   * the prefix will allow it to be confused with relative links
   * targeting "Volume{GUID}".
   */
  string_size = result / sizeof (gunichar2);
  _g_win32_strip_extended_ntobjm_prefix (buf ? buf : *alloc_buf, &string_size);

  return string_size * sizeof (gunichar2);
}

/* Works like stat() or lstat(), depending on the value of @for_symlink,
 * but accepts filename in UTF-16 and fills our custom stat structure.
 * The @filename must not have trailing slashes.
 */
static int
_g_win32_stat_utf16_no_trailing_slashes (const gunichar2    *filename,
                                         GWin32PrivateStat  *buf,
                                         gboolean            for_symlink)
{
  struct __stat64 statbuf;
  BY_HANDLE_FILE_INFORMATION handle_info;
  FILE_STANDARD_INFO std_info;
  gboolean is_symlink = FALSE;
  wchar_t *filename_target = NULL;
  DWORD immediate_attributes;
  DWORD open_flags;
  gboolean is_directory;
  DWORD reparse_tag = 0;
  DWORD error_code;
  BOOL succeeded_so_far;
  HANDLE file_handle;

  immediate_attributes = GetFileAttributesW (filename);

  if (immediate_attributes == INVALID_FILE_ATTRIBUTES)
    {
      error_code = GetLastError ();
      errno = w32_error_to_errno (error_code);

      return -1;
    }

  is_symlink = (immediate_attributes & FILE_ATTRIBUTE_REPARSE_POINT) == FILE_ATTRIBUTE_REPARSE_POINT;
  is_directory = (immediate_attributes & FILE_ATTRIBUTE_DIRECTORY) == FILE_ATTRIBUTE_DIRECTORY;

  open_flags = FILE_ATTRIBUTE_NORMAL;

  if (for_symlink && is_symlink)
    open_flags |= FILE_FLAG_OPEN_REPARSE_POINT;

  if (is_directory)
    open_flags |= FILE_FLAG_BACKUP_SEMANTICS;

  file_handle = CreateFileW (filename, FILE_READ_ATTRIBUTES | FILE_READ_EA,
                             FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
                             NULL, OPEN_EXISTING,
                             open_flags,
                             NULL);

  if (file_handle == INVALID_HANDLE_VALUE)
    {
      error_code = GetLastError ();
      errno = w32_error_to_errno (error_code);
      return -1;
    }

  succeeded_so_far = GetFileInformationByHandle (file_handle,
                                                 &handle_info);
  error_code = GetLastError ();

  if (succeeded_so_far)
    {
      succeeded_so_far = GetFileInformationByHandleEx (file_handle,
                                                       FileStandardInfo,
                                                       &std_info,
                                                       sizeof (std_info));
      error_code = GetLastError ();
    }

  if (!succeeded_so_far)
    {
      CloseHandle (file_handle);
      errno = w32_error_to_errno (error_code);
      return -1;
    }

  /* It's tempting to use GetFileInformationByHandleEx(FileAttributeTagInfo),
   * but it always reports that the ReparseTag is 0.
   * We already have a handle open for symlink, use that.
   * For the target we have to specify a filename, and the function
   * will open another handle internally.
   */
  if (is_symlink &&
      _g_win32_readlink_utf16_handle (for_symlink ? NULL : filename,
                                      for_symlink ? file_handle : NULL,
                                      &reparse_tag,
                                      NULL, 0,
                                      for_symlink ? NULL : &filename_target,
                                      TRUE) < 0)
    {
      CloseHandle (file_handle);
      return -1;
    }

  CloseHandle (file_handle);

  _g_win32_fill_statbuf_from_handle_info (filename,
                                          filename_target,
                                          &handle_info,
                                          &statbuf);
  g_free (filename_target);
  _g_win32_fill_privatestat (&statbuf,
                             &handle_info,
                             &std_info,
                             reparse_tag,
                             buf);

  return 0;
}

/* Works like fstat(), but fills our custom stat structure. */
static int
_g_win32_stat_fd (int                 fd,
                  GWin32PrivateStat  *buf)
{
  HANDLE file_handle;
  gboolean succeeded_so_far;
  DWORD error_code;
  struct __stat64 statbuf;
  BY_HANDLE_FILE_INFORMATION handle_info;
  FILE_STANDARD_INFO std_info;
  DWORD reparse_tag = 0;
  gboolean is_symlink = FALSE;

  file_handle = (HANDLE) _get_osfhandle (fd);

  if (file_handle == INVALID_HANDLE_VALUE)
    return -1;

  succeeded_so_far = GetFileInformationByHandle (file_handle,
                                                 &handle_info);
  error_code = GetLastError ();

  if (succeeded_so_far)
    {
      succeeded_so_far = GetFileInformationByHandleEx (file_handle,
                                                       FileStandardInfo,
                                                       &std_info,
                                                       sizeof (std_info));
      error_code = GetLastError ();
    }

  if (!succeeded_so_far)
    {
      errno = w32_error_to_errno (error_code);
      return -1;
    }

  is_symlink = (handle_info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == FILE_ATTRIBUTE_REPARSE_POINT;

  if (is_symlink &&
      _g_win32_readlink_handle_raw (file_handle, &reparse_tag, NULL, 0, NULL, FALSE) < 0)
    return -1;

  if (_fstat64 (fd, &statbuf) != 0)
    return -1;

  _g_win32_fill_privatestat (&statbuf,
                             &handle_info,
                             &std_info,
                             reparse_tag,
                             buf);

  return 0;
}

/* Works like stat() or lstat(), depending on the value of @for_symlink,
 * but accepts filename in UTF-8 and fills our custom stat structure.
 */
static int
_g_win32_stat_utf8 (const gchar       *filename,
                    GWin32PrivateStat *buf,
                    gboolean           for_symlink)
{
  wchar_t *wfilename;
  int result;
  gsize len;

  if (filename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  len = strlen (filename);

  while (len > 0 && G_IS_DIR_SEPARATOR (filename[len - 1]))
    len--;

  if (len <= 0 ||
      (g_path_is_absolute (filename) && len <= (gsize) (g_path_skip_root (filename) - filename)))
    len = strlen (filename);

  wfilename = g_utf8_to_utf16 (filename, len, NULL, NULL, NULL);

  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  result = _g_win32_stat_utf16_no_trailing_slashes (wfilename, buf, for_symlink);

  g_free (wfilename);

  return result;
}

/* Works like stat(), but accepts filename in UTF-8
 * and fills our custom stat structure.
 */
int
g_win32_stat_utf8 (const gchar       *filename,
                   GWin32PrivateStat *buf)
{
  return _g_win32_stat_utf8 (filename, buf, FALSE);
}

/* Works like lstat(), but accepts filename in UTF-8
 * and fills our custom stat structure.
 */
int
g_win32_lstat_utf8 (const gchar       *filename,
                    GWin32PrivateStat *buf)
{
  return _g_win32_stat_utf8 (filename, buf, TRUE);
}

/* Works like fstat(), but accepts filename in UTF-8
 * and fills our custom stat structure.
 */
int
g_win32_fstat (int                fd,
               GWin32PrivateStat *buf)
{
  return _g_win32_stat_fd (fd, buf);
}

/**
 * g_win32_readlink_utf8:
 * @filename: (type filename): a pathname in UTF-8
 * @buf: (array length=buf_size) : a buffer to receive the reparse point
 *                                 target path. Mutually-exclusive
 *                                 with @alloc_buf.
 * @buf_size: size of the @buf, in bytes
 * @alloc_buf: points to a location where internally-allocated buffer
 *             pointer will be written. That buffer receives the
 *             link data. Mutually-exclusive with @buf.
 * @terminate: ensures that the buffer is NUL-terminated if
 *             it isn't already. If %FALSE, the returned string
 *             might not be NUL-terminated (depends entirely on
 *             what the contents of the filesystem are).
 *
 * Tries to read the reparse point indicated by @filename, filling
 * @buf or @alloc_buf with the path that the reparse point redirects to.
 * The path will be UTF-8-encoded, and an extended path prefix
 * or a NT object manager prefix will be removed from it, if
 * possible, but otherwise the path is returned as-is. Specifically,
 * it could be a "\\\\Volume{GUID}\\" path. It also might use
 * backslashes as path separators.
 *
 * Returns: -1 on error (sets errno), 0 if there's no (recognizable)
 * path in the reparse point (@alloc_buf will not be allocated in that case,
 * and @buf will be left unmodified),
 * or the number of bytes placed into @buf otherwise,
 * including NUL-terminator (if present or if @terminate is TRUE).
 * The buffer returned via @alloc_buf should be freed with g_free().
 *
 * Since: 2.60
 */
int
g_win32_readlink_utf8 (const gchar  *filename,
                       gchar        *buf,
                       gsize         buf_size,
                       gchar       **alloc_buf,
                       gboolean      terminate)
{
  wchar_t *wfilename;
  int result;
  wchar_t *buf_utf16;
  glong tmp_len;
  gchar *tmp;

  g_return_val_if_fail ((buf != NULL || alloc_buf != NULL) &&
                        (buf == NULL || alloc_buf == NULL),
                        -1);

  wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);

  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  result = _g_win32_readlink_utf16_handle (wfilename, NULL, NULL,
                                           NULL, 0, &buf_utf16, terminate);

  g_free (wfilename);

  if (result <= 0)
    return result;

  tmp = g_utf16_to_utf8 (buf_utf16,
                         result / sizeof (gunichar2),
                         NULL,
                         &tmp_len,
                         NULL);

  g_free (buf_utf16);

  if (tmp == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  if (alloc_buf)
    {
      *alloc_buf = tmp;
      return tmp_len;
    }

  if ((gsize) tmp_len > buf_size)
    tmp_len = buf_size;

  memcpy (buf, tmp, tmp_len);
  g_free (tmp);

  return tmp_len;
}

#endif

/**
 * g_access:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @mode: as in access()
 *
 * A wrapper for the POSIX access() function. This function is used to
 * test a pathname for one or several of read, write or execute
 * permissions, or just existence.
 *
 * On Windows, the file protection mechanism is not at all POSIX-like,
 * and the underlying function in the C library only checks the
 * FAT-style READONLY attribute, and does not look at the ACL of a
 * file at all. This function is this in practise almost useless on
 * Windows. Software that needs to handle file permissions on Windows
 * more exactly should use the Win32 API.
 *
 * See your C library manual for more details about access().
 *
 * Returns: zero if the pathname refers to an existing file system
 *     object that has all the tested permissions, or -1 otherwise
 *     or on error.
 * 
 * Since: 2.8
 */
int
g_access (const gchar *filename,
	  int          mode)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;
    
  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

#ifndef X_OK
#define X_OK 1
#endif

  retval = _waccess (wfilename, mode & ~X_OK);
  save_errno = errno;

  g_free (wfilename);

  errno = save_errno;
  return retval;
#else
  return access (filename, mode);
#endif
}

/**
 * g_chmod:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @mode: as in chmod()
 *
 * A wrapper for the POSIX chmod() function. The chmod() function is
 * used to set the permissions of a file system object.
 * 
 * On Windows the file protection mechanism is not at all POSIX-like,
 * and the underlying chmod() function in the C library just sets or
 * clears the FAT-style READONLY attribute. It does not touch any
 * ACL. Software that needs to manage file permissions on Windows
 * exactly should use the Win32 API.
 *
 * See your C library manual for more details about chmod().
 *
 * Returns: 0 if the operation succeeded, -1 on error
 * 
 * Since: 2.8
 */
int
g_chmod (const gchar *filename,
	 int          mode)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;
    
  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  retval = _wchmod (wfilename, mode);
  save_errno = errno;

  g_free (wfilename);

  errno = save_errno;
  return retval;
#else
  return chmod (filename, mode);
#endif
}
/**
 * g_open:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @flags: as in open()
 * @mode: as in open()
 *
 * A wrapper for the POSIX open() function. The open() function is
 * used to convert a pathname into a file descriptor.
 *
 * On POSIX systems file descriptors are implemented by the operating
 * system. On Windows, it's the C library that implements open() and
 * file descriptors. The actual Win32 API for opening files is quite
 * different, see MSDN documentation for CreateFile(). The Win32 API
 * uses file handles, which are more randomish integers, not small
 * integers like file descriptors.
 *
 * Because file descriptors are specific to the C library on Windows,
 * the file descriptor returned by this function makes sense only to
 * functions in the same C library. Thus if the GLib-using code uses a
 * different C library than GLib does, the file descriptor returned by
 * this function cannot be passed to C library functions like write()
 * or read().
 *
 * See your C library manual for more details about open().
 *
 * Returns: a new file descriptor, or -1 if an error occurred.
 *     The return value can be used exactly like the return value
 *     from open().
 * 
 * Since: 2.6
 */
int
g_open (const gchar *filename,
	int          flags,
	int          mode)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;
    
  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  retval = _wopen (wfilename, flags, mode);
  save_errno = errno;

  g_free (wfilename);

  errno = save_errno;
  return retval;
#else
  int fd;
  do
    fd = open (filename, flags, mode);
  while (G_UNLIKELY (fd == -1 && errno == EINTR));
  return fd;
#endif
}

/**
 * g_creat:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @mode: as in creat()
 *
 * A wrapper for the POSIX creat() function. The creat() function is
 * used to convert a pathname into a file descriptor, creating a file
 * if necessary.
 *
 * On POSIX systems file descriptors are implemented by the operating
 * system. On Windows, it's the C library that implements creat() and
 * file descriptors. The actual Windows API for opening files is
 * different, see MSDN documentation for CreateFile(). The Win32 API
 * uses file handles, which are more randomish integers, not small
 * integers like file descriptors.
 *
 * Because file descriptors are specific to the C library on Windows,
 * the file descriptor returned by this function makes sense only to
 * functions in the same C library. Thus if the GLib-using code uses a
 * different C library than GLib does, the file descriptor returned by
 * this function cannot be passed to C library functions like write()
 * or read().
 *
 * See your C library manual for more details about creat().
 *
 * Returns: a new file descriptor, or -1 if an error occurred.
 *     The return value can be used exactly like the return value
 *     from creat().
 * 
 * Since: 2.8
 */
int
g_creat (const gchar *filename,
	 int          mode)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;
    
  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  retval = _wcreat (wfilename, mode);
  save_errno = errno;

  g_free (wfilename);

  errno = save_errno;
  return retval;
#else
  return creat (filename, mode);
#endif
}

/**
 * g_rename:
 * @oldfilename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @newfilename: (type filename): a pathname in the GLib file name encoding
 *
 * A wrapper for the POSIX rename() function. The rename() function 
 * renames a file, moving it between directories if required.
 * 
 * See your C library manual for more details about how rename() works
 * on your system. It is not possible in general on Windows to rename
 * a file that is open to some process.
 *
 * Returns: 0 if the renaming succeeded, -1 if an error occurred
 * 
 * Since: 2.6
 */
int
g_rename (const gchar *oldfilename,
	  const gchar *newfilename)
{
#ifdef G_OS_WIN32
  wchar_t *woldfilename = g_utf8_to_utf16 (oldfilename, -1, NULL, NULL, NULL);
  wchar_t *wnewfilename;
  int retval;
  int save_errno = 0;

  if (woldfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  wnewfilename = g_utf8_to_utf16 (newfilename, -1, NULL, NULL, NULL);

  if (wnewfilename == NULL)
    {
      g_free (woldfilename);
      errno = EINVAL;
      return -1;
    }

  if (MoveFileExW (woldfilename, wnewfilename, MOVEFILE_REPLACE_EXISTING))
    retval = 0;
  else
    {
      retval = -1;
      save_errno = w32_error_to_errno (GetLastError ());
    }

  g_free (woldfilename);
  g_free (wnewfilename);
    
  errno = save_errno;
  return retval;
#else
  return rename (oldfilename, newfilename);
#endif
}

/**
 * g_mkdir: 
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @mode: permissions to use for the newly created directory
 *
 * A wrapper for the POSIX mkdir() function. The mkdir() function 
 * attempts to create a directory with the given name and permissions.
 * The mode argument is ignored on Windows.
 * 
 * See your C library manual for more details about mkdir().
 *
 * Returns: 0 if the directory was successfully created, -1 if an error 
 *    occurred
 * 
 * Since: 2.6
 */
int
g_mkdir (const gchar *filename,
	 int          mode)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;

  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  retval = _wmkdir (wfilename);
  save_errno = errno;

  g_free (wfilename);
    
  errno = save_errno;
  return retval;
#else
  return mkdir (filename, mode);
#endif
}

/**
 * g_chdir: 
 * @path: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 *
 * A wrapper for the POSIX chdir() function. The function changes the
 * current directory of the process to @path.
 * 
 * See your C library manual for more details about chdir().
 *
 * Returns: 0 on success, -1 if an error occurred.
 * 
 * Since: 2.8
 */
int
g_chdir (const gchar *path)
{
#ifdef G_OS_WIN32
  wchar_t *wpath = g_utf8_to_utf16 (path, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;

  if (wpath == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  retval = _wchdir (wpath);
  save_errno = errno;

  g_free (wpath);
    
  errno = save_errno;
  return retval;
#else
  return chdir (path);
#endif
}

/**
 * GStatBuf:
 *
 * A type corresponding to the appropriate struct type for the stat()
 * system call, depending on the platform and/or compiler being used.
 *
 * See g_stat() for more information.
 */
/**
 * g_stat: 
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @buf: a pointer to a stat struct, which will be filled with the file
 *     information
 *
 * A wrapper for the POSIX stat() function. The stat() function
 * returns information about a file. On Windows the stat() function in
 * the C library checks only the FAT-style READONLY attribute and does
 * not look at the ACL at all. Thus on Windows the protection bits in
 * the @st_mode field are a fabrication of little use.
 * 
 * On Windows the Microsoft C libraries have several variants of the
 * stat struct and stat() function with names like _stat(), _stat32(),
 * _stat32i64() and _stat64i32(). The one used here is for 32-bit code
 * the one with 32-bit size and time fields, specifically called _stat32().
 *
 * In Microsoft's compiler, by default struct stat means one with
 * 64-bit time fields while in MinGW struct stat is the legacy one
 * with 32-bit fields. To hopefully clear up this messs, the gstdio.h
 * header defines a type #GStatBuf which is the appropriate struct type
 * depending on the platform and/or compiler being used. On POSIX it
 * is just struct stat, but note that even on POSIX platforms, stat()
 * might be a macro.
 *
 * See your C library manual for more details about stat().
 *
 * Returns: 0 if the information was successfully retrieved,
 *     -1 if an error occurred
 * 
 * Since: 2.6
 */
int
g_stat (const gchar *filename,
	GStatBuf    *buf)
{
#ifdef G_OS_WIN32
  GWin32PrivateStat w32_buf;
  int retval = g_win32_stat_utf8 (filename, &w32_buf);

  buf->st_dev = w32_buf.st_dev;
  buf->st_ino = w32_buf.st_ino;
  buf->st_mode = w32_buf.st_mode;
  buf->st_nlink = w32_buf.st_nlink;
  buf->st_uid = w32_buf.st_uid;
  buf->st_gid = w32_buf.st_gid;
  buf->st_rdev = w32_buf.st_dev;
  buf->st_size = w32_buf.st_size;
  buf->st_atime = w32_buf.st_atim.tv_sec;
  buf->st_mtime = w32_buf.st_mtim.tv_sec;
  buf->st_ctime = w32_buf.st_ctim.tv_sec;

  return retval;
#else
  return stat (filename, buf);
#endif
}

/**
 * g_lstat: 
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @buf: a pointer to a stat struct, which will be filled with the file
 *     information
 *
 * A wrapper for the POSIX lstat() function. The lstat() function is
 * like stat() except that in the case of symbolic links, it returns
 * information about the symbolic link itself and not the file that it
 * refers to. If the system does not support symbolic links g_lstat()
 * is identical to g_stat().
 * 
 * See your C library manual for more details about lstat().
 *
 * Returns: 0 if the information was successfully retrieved,
 *     -1 if an error occurred
 * 
 * Since: 2.6
 */
int
g_lstat (const gchar *filename,
	 GStatBuf    *buf)
{
#ifdef HAVE_LSTAT
  /* This can't be Win32, so don't do the widechar dance. */
  return lstat (filename, buf);
#elif defined (G_OS_WIN32)
  GWin32PrivateStat w32_buf;
  int retval = g_win32_lstat_utf8 (filename, &w32_buf);

  buf->st_dev = w32_buf.st_dev;
  buf->st_ino = w32_buf.st_ino;
  buf->st_mode = w32_buf.st_mode;
  buf->st_nlink = w32_buf.st_nlink;
  buf->st_uid = w32_buf.st_uid;
  buf->st_gid = w32_buf.st_gid;
  buf->st_rdev = w32_buf.st_dev;
  buf->st_size = w32_buf.st_size;
  buf->st_atime = w32_buf.st_atim.tv_sec;
  buf->st_mtime = w32_buf.st_mtim.tv_sec;
  buf->st_ctime = w32_buf.st_ctim.tv_sec;

  return retval;
#else
  return g_stat (filename, buf);
#endif
}

/**
 * g_unlink:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 *
 * A wrapper for the POSIX unlink() function. The unlink() function 
 * deletes a name from the filesystem. If this was the last link to the 
 * file and no processes have it opened, the diskspace occupied by the
 * file is freed.
 * 
 * See your C library manual for more details about unlink(). Note
 * that on Windows, it is in general not possible to delete files that
 * are open to some process, or mapped into memory.
 *
 * Returns: 0 if the name was successfully deleted, -1 if an error 
 *    occurred
 * 
 * Since: 2.6
 */
int
g_unlink (const gchar *filename)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;

  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  retval = _wunlink (wfilename);
  save_errno = errno;

  g_free (wfilename);

  errno = save_errno;
  return retval;
#else
  return unlink (filename);
#endif
}

/**
 * g_remove:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 *
 * A wrapper for the POSIX remove() function. The remove() function
 * deletes a name from the filesystem.
 * 
 * See your C library manual for more details about how remove() works
 * on your system. On Unix, remove() removes also directories, as it
 * calls unlink() for files and rmdir() for directories. On Windows,
 * although remove() in the C library only works for files, this
 * function tries first remove() and then if that fails rmdir(), and
 * thus works for both files and directories. Note however, that on
 * Windows, it is in general not possible to remove a file that is
 * open to some process, or mapped into memory.
 *
 * If this function fails on Windows you can't infer too much from the
 * errno value. rmdir() is tried regardless of what caused remove() to
 * fail. Any errno value set by remove() will be overwritten by that
 * set by rmdir().
 *
 * Returns: 0 if the file was successfully removed, -1 if an error 
 *    occurred
 * 
 * Since: 2.6
 */
int
g_remove (const gchar *filename)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;

  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }

  retval = _wremove (wfilename);
  if (retval == -1)
    retval = _wrmdir (wfilename);
  save_errno = errno;

  g_free (wfilename);

  errno = save_errno;
  return retval;
#else
  return remove (filename);
#endif
}

/**
 * g_rmdir:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 *
 * A wrapper for the POSIX rmdir() function. The rmdir() function
 * deletes a directory from the filesystem.
 * 
 * See your C library manual for more details about how rmdir() works
 * on your system.
 *
 * Returns: 0 if the directory was successfully removed, -1 if an error 
 *    occurred
 * 
 * Since: 2.6
 */
int
g_rmdir (const gchar *filename)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;

  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  
  retval = _wrmdir (wfilename);
  save_errno = errno;

  g_free (wfilename);

  errno = save_errno;
  return retval;
#else
  return rmdir (filename);
#endif
}

/**
 * g_fopen:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @mode: a string describing the mode in which the file should be opened
 *
 * A wrapper for the stdio `fopen()` function. The `fopen()` function
 * opens a file and associates a new stream with it.
 * 
 * Because file descriptors are specific to the C library on Windows,
 * and a file descriptor is part of the `FILE` struct, the `FILE*` returned
 * by this function makes sense only to functions in the same C library.
 * Thus if the GLib-using code uses a different C library than GLib does,
 * the FILE* returned by this function cannot be passed to C library
 * functions like `fprintf()` or `fread()`.
 *
 * See your C library manual for more details about `fopen()`.
 *
 * As `close()` and `fclose()` are part of the C library, this implies that it is
 * currently impossible to close a file if the application C library and the C library
 * used by GLib are different. Convenience functions like g_file_set_contents_full()
 * avoid this problem.
 *
 * Returns: A `FILE*` if the file was successfully opened, or %NULL if
 *     an error occurred
 * 
 * Since: 2.6
 */
FILE *
g_fopen (const gchar *filename,
	 const gchar *mode)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  wchar_t *wmode;
  FILE *retval;
  int save_errno;

  if (wfilename == NULL)
    {
      errno = EINVAL;
      return NULL;
    }

  wmode = g_utf8_to_utf16 (mode, -1, NULL, NULL, NULL);

  if (wmode == NULL)
    {
      g_free (wfilename);
      errno = EINVAL;
      return NULL;
    }

  _g_win32_fix_mode (wmode);
  retval = _wfopen (wfilename, wmode);
  save_errno = errno;

  g_free (wfilename);
  g_free (wmode);

  errno = save_errno;
  return retval;
#else
  return fopen (filename, mode);
#endif
}

/**
 * g_freopen:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @mode: a string describing the mode in which the file should be  opened
 * @stream: (nullable): an existing stream which will be reused, or %NULL
 *
 * A wrapper for the POSIX freopen() function. The freopen() function
 * opens a file and associates it with an existing stream.
 * 
 * See your C library manual for more details about freopen().
 *
 * Returns: A FILE* if the file was successfully opened, or %NULL if
 *     an error occurred.
 * 
 * Since: 2.6
 */
FILE *
g_freopen (const gchar *filename,
	   const gchar *mode,
	   FILE        *stream)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  wchar_t *wmode;
  FILE *retval;
  int save_errno;

  if (wfilename == NULL)
    {
      errno = EINVAL;
      return NULL;
    }

  wmode = g_utf8_to_utf16 (mode, -1, NULL, NULL, NULL);

  if (wmode == NULL)
    {
      g_free (wfilename);
      errno = EINVAL;
      return NULL;
    }

  _g_win32_fix_mode (wmode);
  retval = _wfreopen (wfilename, wmode, stream);
  save_errno = errno;

  g_free (wfilename);
  g_free (wmode);

  errno = save_errno;
  return retval;
#else
  return freopen (filename, mode, stream);
#endif
}

/**
 * g_fsync:
 * @fd: a file descriptor
 *
 * A wrapper for the POSIX `fsync()` function. On Windows, `_commit()` will be
 * used. On macOS, `fcntl(F_FULLFSYNC)` will be used.
 * The `fsync()` function is used to synchronize a file's in-core
 * state with that of the disk.
 *
 * This wrapper will handle retrying on `EINTR`.
 *
 * See the C library manual for more details about fsync().
 *
 * Returns: 0 on success, or -1 if an error occurred.
 * The return value can be used exactly like the return value from fsync().
 *
 * Since: 2.64
 */
gint
g_fsync (gint fd)
{
#ifdef G_OS_WIN32
  return _commit (fd);
#elif defined(HAVE_FSYNC) || defined(HAVE_FCNTL_F_FULLFSYNC)
  int retval;
  do
#ifdef HAVE_FCNTL_F_FULLFSYNC
    retval = fcntl (fd, F_FULLFSYNC, 0);
#else
    retval = fsync (fd);
#endif
  while (G_UNLIKELY (retval < 0 && errno == EINTR));
  return retval;
#else
  return 0;
#endif
}

/**
 * g_utime:
 * @filename: (type filename): a pathname in the GLib file name encoding
 *     (UTF-8 on Windows)
 * @utb: a pointer to a struct utimbuf.
 *
 * A wrapper for the POSIX utime() function. The utime() function
 * sets the access and modification timestamps of a file.
 * 
 * See your C library manual for more details about how utime() works
 * on your system.
 *
 * Returns: 0 if the operation was successful, -1 if an error occurred
 * 
 * Since: 2.18
 */
int
g_utime (const gchar    *filename,
	 struct utimbuf *utb)
{
#ifdef G_OS_WIN32
  wchar_t *wfilename = g_utf8_to_utf16 (filename, -1, NULL, NULL, NULL);
  int retval;
  int save_errno;

  if (wfilename == NULL)
    {
      errno = EINVAL;
      return -1;
    }
  
  retval = _wutime (wfilename, (struct _utimbuf*) utb);
  save_errno = errno;

  g_free (wfilename);

  errno = save_errno;
  return retval;
#else
  return utime (filename, utb);
#endif
}

/**
 * g_close:
 * @fd: A file descriptor
 * @error: a #GError
 *
 * This wraps the close() call; in case of error, %errno will be
 * preserved, but the error will also be stored as a #GError in @error.
 *
 * Besides using #GError, there is another major reason to prefer this
 * function over the call provided by the system; on Unix, it will
 * attempt to correctly handle %EINTR, which has platform-specific
 * semantics.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 2.36
 */
gboolean
g_close (gint       fd,
         GError   **error)
{
  int res;
  res = close (fd);
  /* Just ignore EINTR for now; a retry loop is the wrong thing to do
   * on Linux at least.  Anyone who wants to add a conditional check
   * for e.g. HP-UX is welcome to do so later...
   *
   * http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html
   * https://bugzilla.gnome.org/show_bug.cgi?id=682819
   * http://utcc.utoronto.ca/~cks/space/blog/unix/CloseEINTR
   * https://sites.google.com/site/michaelsafyan/software-engineering/checkforeintrwheninvokingclosethinkagain
   */
  if (G_UNLIKELY (res == -1 && errno == EINTR))
    return TRUE;
  else if (res == -1)
    {
      int errsv = errno;
      g_set_error_literal (error, G_FILE_ERROR,
                           g_file_error_from_errno (errsv),
                           g_strerror (errsv));
      errno = errsv;
      return FALSE;
    }
  return TRUE;
}
