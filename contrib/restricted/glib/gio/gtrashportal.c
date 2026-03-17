/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright 2018, Red Hat, Inc.
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

#include <contrib/restricted/glib/config.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "gtrashportal.h"
#include "xdp-dbus.h"
#include "gstdio.h"

#ifdef G_OS_UNIX
#include "gunixfdlist.h"
#endif

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#else
#define HAVE_O_CLOEXEC 1
#endif

#ifndef O_PATH
#define O_PATH 0
#endif

static GXdpTrash *
ensure_trash_portal (void)
{
  static GXdpTrash *trash = NULL;

  if (g_once_init_enter (&trash))
    {
      GDBusConnection *connection = g_bus_get_sync (G_BUS_TYPE_SESSION, NULL, NULL);
      GXdpTrash *proxy = NULL;

      if (connection != NULL)
        {
          proxy = gxdp_trash_proxy_new_sync (connection, 0,
                                             "org.freedesktop.portal.Desktop",
                                             "/org/freedesktop/portal/desktop",
                                             NULL, NULL);
          g_object_unref (connection);
        }

      g_once_init_leave (&trash, proxy);
    }

  return trash;
}

gboolean
g_trash_portal_trash_file (GFile   *file,
                           GError **error)
{
  char *path = NULL;
  GUnixFDList *fd_list = NULL;
  int fd, fd_in, errsv;
  gboolean ret = FALSE;
  guint portal_result = 0;
  GXdpTrash *proxy;

  proxy = ensure_trash_portal ();
  if (proxy == NULL)
    {
      g_set_error (error, G_IO_ERROR, G_IO_ERROR_NOT_INITIALIZED,
                   "Trash portal is not available");
      goto out;
    }

  path = g_file_get_path (file);

  fd = g_open (path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
  if (fd == -1 && errno == EISDIR)
    /* If it is a directory, fall back to O_PATH.
     * Remove O_NOFOLLOW since
     * a) we know it is a directory, not a symlink, and
     * b) the portal reject this combination
     */
    fd = g_open (path, O_PATH | O_CLOEXEC | O_RDONLY);

  errsv = errno;

  if (fd == -1)
    {
      g_set_error (error, G_IO_ERROR, g_io_error_from_errno (errsv),
                   "Failed to open %s", path);
      goto out;
    }

#ifndef HAVE_O_CLOEXEC
  fcntl (fd, F_SETFD, FD_CLOEXEC);
#endif

  fd_list = g_unix_fd_list_new ();
  fd_in = g_unix_fd_list_append (fd_list, fd, error);
  g_close (fd, NULL);

  if (fd_in == -1)
    goto out;

  ret = gxdp_trash_call_trash_file_sync (proxy,
                                         g_variant_new_handle (fd_in),
                                         fd_list,
                                         &portal_result,
                                         NULL,
                                         NULL,
                                         error);

  if (ret && portal_result != 1)
    {
      g_set_error (error, G_IO_ERROR, G_IO_ERROR_FAILED, "Trash portal failed on %s", path);
      ret = FALSE;
    }

 out:
  g_clear_object (&fd_list);
  g_free (path);

  return ret;
}
