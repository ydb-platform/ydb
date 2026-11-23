/* GLIB - Library of useful routines for C programming
 * Copyright (C) 2011 Red Hat, Inc.
 *
 * glib-unix.c: UNIX specific API wrappers and convenience functions
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
 * Authors: Colin Walters <walters@verbum.org>
 */

#include <contrib/restricted/glib/config.h>

/* To make bionic export pipe2() */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#include "glib-unix.h"
#include "gmain-internal.h"

#include <string.h>
#include <sys/types.h>
#include <pwd.h>

G_STATIC_ASSERT (sizeof (ssize_t) == GLIB_SIZEOF_SSIZE_T);
G_STATIC_ASSERT (G_ALIGNOF (gssize) == G_ALIGNOF (ssize_t));

G_STATIC_ASSERT (sizeof (GPid) == sizeof (pid_t));
G_STATIC_ASSERT (G_ALIGNOF (GPid) == G_ALIGNOF (pid_t));

/**
 * SECTION:gunix
 * @title: UNIX-specific utilities and integration
 * @short_description: pipes, signal handling
 * @include: glib-unix.h
 *
 * Most of GLib is intended to be portable; in contrast, this set of
 * functions is designed for programs which explicitly target UNIX,
 * or are using it to build higher level abstractions which would be
 * conditionally compiled if the platform matches %G_OS_UNIX.
 *
 * To use these functions, you must explicitly include the
 * "glib-unix.h" header.
 */

G_DEFINE_QUARK (g-unix-error-quark, g_unix_error)

static gboolean
g_unix_set_error_from_errno (GError **error,
                             gint     saved_errno)
{
  g_set_error_literal (error,
                       G_UNIX_ERROR,
                       0,
                       g_strerror (saved_errno));
  errno = saved_errno;
  return FALSE;
}

/**
 * g_unix_open_pipe:
 * @fds: Array of two integers
 * @flags: Bitfield of file descriptor flags, as for fcntl()
 * @error: a #GError
 *
 * Similar to the UNIX pipe() call, but on modern systems like Linux
 * uses the pipe2() system call, which atomically creates a pipe with
 * the configured flags. The only supported flag currently is
 * %FD_CLOEXEC. If for example you want to configure %O_NONBLOCK, that
 * must still be done separately with fcntl().
 *
 * This function does not take %O_CLOEXEC, it takes %FD_CLOEXEC as if
 * for fcntl(); these are different on Linux/glibc.
 *
 * Returns: %TRUE on success, %FALSE if not (and errno will be set).
 *
 * Since: 2.30
 */
gboolean
g_unix_open_pipe (int     *fds,
                  int      flags,
                  GError **error)
{
  int ecode;

  /* We only support FD_CLOEXEC */
  g_return_val_if_fail ((flags & (FD_CLOEXEC)) == flags, FALSE);

#ifdef HAVE_PIPE2
  {
    int pipe2_flags = 0;
    if (flags & FD_CLOEXEC)
      pipe2_flags |= O_CLOEXEC;
    /* Atomic */
    ecode = pipe2 (fds, pipe2_flags);
    if (ecode == -1 && errno != ENOSYS)
      return g_unix_set_error_from_errno (error, errno);
    else if (ecode == 0)
      return TRUE;
    /* Fall through on -ENOSYS, we must be running on an old kernel */
  }
#endif
  ecode = pipe (fds);
  if (ecode == -1)
    return g_unix_set_error_from_errno (error, errno);

  if (flags == 0)
    return TRUE;

  ecode = fcntl (fds[0], F_SETFD, flags);
  if (ecode == -1)
    {
      int saved_errno = errno;
      close (fds[0]);
      close (fds[1]);
      return g_unix_set_error_from_errno (error, saved_errno);
    }
  ecode = fcntl (fds[1], F_SETFD, flags);
  if (ecode == -1)
    {
      int saved_errno = errno;
      close (fds[0]);
      close (fds[1]);
      return g_unix_set_error_from_errno (error, saved_errno);
    }
  return TRUE;
}

/**
 * g_unix_set_fd_nonblocking:
 * @fd: A file descriptor
 * @nonblock: If %TRUE, set the descriptor to be non-blocking
 * @error: a #GError
 *
 * Control the non-blocking state of the given file descriptor,
 * according to @nonblock. On most systems this uses %O_NONBLOCK, but
 * on some older ones may use %O_NDELAY.
 *
 * Returns: %TRUE if successful
 *
 * Since: 2.30
 */
gboolean
g_unix_set_fd_nonblocking (gint       fd,
                           gboolean   nonblock,
                           GError   **error)
{
#ifdef F_GETFL
  glong fcntl_flags;
  fcntl_flags = fcntl (fd, F_GETFL);

  if (fcntl_flags == -1)
    return g_unix_set_error_from_errno (error, errno);

  if (nonblock)
    {
#ifdef O_NONBLOCK
      fcntl_flags |= O_NONBLOCK;
#else
      fcntl_flags |= O_NDELAY;
#endif
    }
  else
    {
#ifdef O_NONBLOCK
      fcntl_flags &= ~O_NONBLOCK;
#else
      fcntl_flags &= ~O_NDELAY;
#endif
    }

  if (fcntl (fd, F_SETFL, fcntl_flags) == -1)
    return g_unix_set_error_from_errno (error, errno);
  return TRUE;
#else
  return g_unix_set_error_from_errno (error, EINVAL);
#endif
}

/**
 * g_unix_signal_source_new:
 * @signum: A signal number
 *
 * Create a #GSource that will be dispatched upon delivery of the UNIX
 * signal @signum.  In GLib versions before 2.36, only `SIGHUP`, `SIGINT`,
 * `SIGTERM` can be monitored.  In GLib 2.36, `SIGUSR1` and `SIGUSR2`
 * were added. In GLib 2.54, `SIGWINCH` was added.
 *
 * Note that unlike the UNIX default, all sources which have created a
 * watch will be dispatched, regardless of which underlying thread
 * invoked g_unix_signal_source_new().
 *
 * For example, an effective use of this function is to handle `SIGTERM`
 * cleanly; flushing any outstanding files, and then calling
 * g_main_loop_quit ().  It is not safe to do any of this a regular
 * UNIX signal handler; your handler may be invoked while malloc() or
 * another library function is running, causing reentrancy if you
 * attempt to use it from the handler.  None of the GLib/GObject API
 * is safe against this kind of reentrancy.
 *
 * The interaction of this source when combined with native UNIX
 * functions like sigprocmask() is not defined.
 *
 * The source will not initially be associated with any #GMainContext
 * and must be added to one with g_source_attach() before it will be
 * executed.
 *
 * Returns: A newly created #GSource
 *
 * Since: 2.30
 */
GSource *
g_unix_signal_source_new (int signum)
{
  g_return_val_if_fail (signum == SIGHUP || signum == SIGINT || signum == SIGTERM ||
                        signum == SIGUSR1 || signum == SIGUSR2 || signum == SIGWINCH,
                        NULL);

  return _g_main_create_unix_signal_watch (signum);
}

/**
 * g_unix_signal_add_full: (rename-to g_unix_signal_add)
 * @priority: the priority of the signal source. Typically this will be in
 *            the range between %G_PRIORITY_DEFAULT and %G_PRIORITY_HIGH.
 * @signum: Signal number
 * @handler: Callback
 * @user_data: Data for @handler
 * @notify: #GDestroyNotify for @handler
 *
 * A convenience function for g_unix_signal_source_new(), which
 * attaches to the default #GMainContext.  You can remove the watch
 * using g_source_remove().
 *
 * Returns: An ID (greater than 0) for the event source
 *
 * Since: 2.30
 */
guint
g_unix_signal_add_full (int            priority,
                        int            signum,
                        GSourceFunc    handler,
                        gpointer       user_data,
                        GDestroyNotify notify)
{
  guint id;
  GSource *source;

  source = g_unix_signal_source_new (signum);

  if (priority != G_PRIORITY_DEFAULT)
    g_source_set_priority (source, priority);

  g_source_set_callback (source, handler, user_data, notify);
  id = g_source_attach (source, NULL);
  g_source_unref (source);

  return id;
}

/**
 * g_unix_signal_add:
 * @signum: Signal number
 * @handler: Callback
 * @user_data: Data for @handler
 *
 * A convenience function for g_unix_signal_source_new(), which
 * attaches to the default #GMainContext.  You can remove the watch
 * using g_source_remove().
 *
 * Returns: An ID (greater than 0) for the event source
 *
 * Since: 2.30
 */
guint
g_unix_signal_add (int         signum,
                   GSourceFunc handler,
                   gpointer    user_data)
{
  return g_unix_signal_add_full (G_PRIORITY_DEFAULT, signum, handler, user_data, NULL);
}

typedef struct
{
  GSource source;

  gint     fd;
  gpointer tag;
} GUnixFDSource;

static gboolean
g_unix_fd_source_dispatch (GSource     *source,
                           GSourceFunc  callback,
                           gpointer     user_data)
{
  GUnixFDSource *fd_source = (GUnixFDSource *) source;
  GUnixFDSourceFunc func = (GUnixFDSourceFunc) callback;

  if (!callback)
    {
      g_warning ("GUnixFDSource dispatched without callback. "
                 "You must call g_source_set_callback().");
      return FALSE;
    }

  return (* func) (fd_source->fd, g_source_query_unix_fd (source, fd_source->tag), user_data);
}

GSourceFuncs g_unix_fd_source_funcs = {
  NULL, NULL, g_unix_fd_source_dispatch, NULL, NULL, NULL
};

/**
 * g_unix_fd_source_new:
 * @fd: a file descriptor
 * @condition: IO conditions to watch for on @fd
 *
 * Creates a #GSource to watch for a particular IO condition on a file
 * descriptor.
 *
 * The source will never close the fd -- you must do it yourself.
 *
 * Returns: the newly created #GSource
 *
 * Since: 2.36
 **/
GSource *
g_unix_fd_source_new (gint         fd,
                      GIOCondition condition)
{
  GUnixFDSource *fd_source;
  GSource *source;

  source = g_source_new (&g_unix_fd_source_funcs, sizeof (GUnixFDSource));
  fd_source = (GUnixFDSource *) source;

  fd_source->fd = fd;
  fd_source->tag = g_source_add_unix_fd (source, fd, condition);

  return source;
}

/**
 * g_unix_fd_add_full:
 * @priority: the priority of the source
 * @fd: a file descriptor
 * @condition: IO conditions to watch for on @fd
 * @function: a #GUnixFDSourceFunc
 * @user_data: data to pass to @function
 * @notify: function to call when the idle is removed, or %NULL
 *
 * Sets a function to be called when the IO condition, as specified by
 * @condition becomes true for @fd.
 *
 * This is the same as g_unix_fd_add(), except that it allows you to
 * specify a non-default priority and a provide a #GDestroyNotify for
 * @user_data.
 *
 * Returns: the ID (greater than 0) of the event source
 *
 * Since: 2.36
 **/
guint
g_unix_fd_add_full (gint              priority,
                    gint              fd,
                    GIOCondition      condition,
                    GUnixFDSourceFunc function,
                    gpointer          user_data,
                    GDestroyNotify    notify)
{
  GSource *source;
  guint id;

  g_return_val_if_fail (function != NULL, 0);

  source = g_unix_fd_source_new (fd, condition);

  if (priority != G_PRIORITY_DEFAULT)
    g_source_set_priority (source, priority);

  g_source_set_callback (source, (GSourceFunc) function, user_data, notify);
  id = g_source_attach (source, NULL);
  g_source_unref (source);

  return id;
}

/**
 * g_unix_fd_add:
 * @fd: a file descriptor
 * @condition: IO conditions to watch for on @fd
 * @function: a #GUnixFDSourceFunc
 * @user_data: data to pass to @function
 *
 * Sets a function to be called when the IO condition, as specified by
 * @condition becomes true for @fd.
 *
 * @function will be called when the specified IO condition becomes
 * %TRUE.  The function is expected to clear whatever event caused the
 * IO condition to become true and return %TRUE in order to be notified
 * when it happens again.  If @function returns %FALSE then the watch
 * will be cancelled.
 *
 * The return value of this function can be passed to g_source_remove()
 * to cancel the watch at any time that it exists.
 *
 * The source will never close the fd -- you must do it yourself.
 *
 * Returns: the ID (greater than 0) of the event source
 *
 * Since: 2.36
 **/
guint
g_unix_fd_add (gint              fd,
               GIOCondition      condition,
               GUnixFDSourceFunc function,
               gpointer          user_data)
{
  return g_unix_fd_add_full (G_PRIORITY_DEFAULT, fd, condition, function, user_data, NULL);
}

/**
 * g_unix_get_passwd_entry:
 * @user_name: the username to get the passwd file entry for
 * @error: return location for a #GError, or %NULL
 *
 * Get the `passwd` file entry for the given @user_name using `getpwnam_r()`.
 * This can fail if the given @user_name doesn’t exist.
 *
 * The returned `struct passwd` has been allocated using g_malloc() and should
 * be freed using g_free(). The strings referenced by the returned struct are
 * included in the same allocation, so are valid until the `struct passwd` is
 * freed.
 *
 * This function is safe to call from multiple threads concurrently.
 *
 * You will need to include `pwd.h` to get the definition of `struct passwd`.
 *
 * Returns: (transfer full): passwd entry, or %NULL on error; free the returned
 *    value with g_free()
 * Since: 2.64
 */
struct passwd *
g_unix_get_passwd_entry (const gchar  *user_name,
                         GError      **error)
{
  struct passwd *passwd_file_entry;
  struct
    {
      struct passwd pwd;
      char string_buffer[];
    } *buffer = NULL;
  gsize string_buffer_size = 0;
  GError *local_error = NULL;

  g_return_val_if_fail (user_name != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

#ifdef _SC_GETPW_R_SIZE_MAX
    {
      /* Get the recommended buffer size */
      glong string_buffer_size_long = sysconf (_SC_GETPW_R_SIZE_MAX);
      if (string_buffer_size_long > 0)
        string_buffer_size = string_buffer_size_long;
    }
#endif /* _SC_GETPW_R_SIZE_MAX */

  /* Default starting size. */
  if (string_buffer_size == 0)
    string_buffer_size = 64;

  do
    {
      int retval;

      g_free (buffer);
      /* Allocate space for the `struct passwd`, and then a buffer for all its
       * strings (whose size is @string_buffer_size, which increases in this
       * loop until it’s big enough). Add 6 extra bytes to work around a bug in
       * macOS < 10.3. See #156446.
       */
      buffer = g_malloc0 (sizeof (*buffer) + string_buffer_size + 6);

      retval = getpwnam_r (user_name, &buffer->pwd, buffer->string_buffer,
                           string_buffer_size, &passwd_file_entry);

      /* Bail out if: the lookup was successful, or if the user id can't be
       * found (should be pretty rare case actually), or if the buffer should be
       * big enough and yet lookups are still not successful.
       */
      if (passwd_file_entry != NULL)
        {
          /* Success. */
          break;
        }
      else if (retval == 0 ||
          retval == ENOENT || retval == ESRCH ||
          retval == EBADF || retval == EPERM)
        {
          /* Username not found. */
          g_unix_set_error_from_errno (&local_error, retval);
          break;
        }
      else if (retval == ERANGE)
        {
          /* Can’t allocate enough string buffer space. */
          if (string_buffer_size > 32 * 1024)
            {
              g_unix_set_error_from_errno (&local_error, retval);
              break;
            }

          string_buffer_size *= 2;
          continue;
        }
      else
        {
          g_unix_set_error_from_errno (&local_error, retval);
          break;
        }
    }
  while (passwd_file_entry == NULL);

  g_assert (passwd_file_entry == NULL ||
            (gpointer) passwd_file_entry == (gpointer) buffer);

  /* Success or error. */
  if (local_error != NULL)
    {
      g_clear_pointer (&buffer, g_free);
      g_propagate_error (error, g_steal_pointer (&local_error));
    }

  return (struct passwd *) g_steal_pointer (&buffer);
}
