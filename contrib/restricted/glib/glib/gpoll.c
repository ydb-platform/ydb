/* GLIB - Library of useful routines for C programming
 * Copyright (C) 1995-1997  Peter Mattis, Spencer Kimball and Josh MacDonald
 *
 * gpoll.c: poll(2) abstraction
 * Copyright 1998 Owen Taylor
 * Copyright 2008 Red Hat, Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Modified by the GLib Team and others 1997-2000.  See the AUTHORS
 * file for a list of people on the GLib Team.  See the ChangeLog
 * files for a list of changes.  These files are distributed with
 * GLib at ftp://ftp.gtk.org/pub/gtk/.
 */

/*
 * MT safe
 */

#include <contrib/restricted/glib/config.h>
#include "glibconfig.h"
#include "giochannel.h"

/* Uncomment the next line (and the corresponding line in gmain.c) to
 * enable debugging printouts if the environment variable
 * G_MAIN_POLL_DEBUG is set to some value.
 */
/* #define G_MAIN_POLL_DEBUG */

#ifdef _WIN32
/* Always enable debugging printout on Windows, as it is more often
 * needed there...
 */
#define G_MAIN_POLL_DEBUG
#endif

#include <sys/types.h>
#include <time.h>
#include <stdlib.h>
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif /* HAVE_SYS_TIME_H */
#ifdef HAVE_POLL
#  include <poll.h>

/* The poll() emulation on OS/X doesn't handle fds=NULL, nfds=0,
 * so we prefer our own poll emulation.
 */
#if defined(_POLL_EMUL_H_) || defined(BROKEN_POLL)
#undef HAVE_POLL
#endif

#endif /* GLIB_HAVE_SYS_POLL_H */
#ifdef G_OS_UNIX
#include <unistd.h>
#endif /* G_OS_UNIX */
#include <errno.h>

#ifdef G_OS_WIN32
#define STRICT
#include <windows.h>
#include <process.h>
#endif /* G_OS_WIN32 */

#include "gpoll.h"

#ifdef G_OS_WIN32
#include "gprintf.h"
#endif

#ifdef G_MAIN_POLL_DEBUG
extern gboolean _g_main_poll_debug;
#endif

#ifdef HAVE_POLL

/**
 * g_poll:
 * @fds: file descriptors to poll
 * @nfds: the number of file descriptors in @fds
 * @timeout: amount of time to wait, in milliseconds, or -1 to wait forever
 *
 * Polls @fds, as with the poll() system call, but portably. (On
 * systems that don't have poll(), it is emulated using select().)
 * This is used internally by #GMainContext, but it can be called
 * directly if you need to block until a file descriptor is ready, but
 * don't want to run the full main loop.
 *
 * Each element of @fds is a #GPollFD describing a single file
 * descriptor to poll. The @fd field indicates the file descriptor,
 * and the @events field indicates the events to poll for. On return,
 * the @revents fields will be filled with the events that actually
 * occurred.
 *
 * On POSIX systems, the file descriptors in @fds can be any sort of
 * file descriptor, but the situation is much more complicated on
 * Windows. If you need to use g_poll() in code that has to run on
 * Windows, the easiest solution is to construct all of your
 * #GPollFDs with g_io_channel_win32_make_pollfd().
 *
 * Returns: the number of entries in @fds whose @revents fields
 * were filled in, or 0 if the operation timed out, or -1 on error or
 * if the call was interrupted.
 *
 * Since: 2.20
 **/
gint
g_poll (GPollFD *fds,
	guint    nfds,
	gint     timeout)
{
  return poll ((struct pollfd *)fds, nfds, timeout);
}

#else	/* !HAVE_POLL */

#ifdef G_OS_WIN32

static int
poll_rest (GPollFD *msg_fd,
           GPollFD *stop_fd,
           HANDLE  *handles,
           GPollFD *handle_to_fd[],
           gint     nhandles,
           DWORD    timeout_ms)
{
  DWORD ready;
  GPollFD *f;
  int recursed_result;

  if (msg_fd != NULL)
    {
      /* Wait for either messages or handles
       * -> Use MsgWaitForMultipleObjectsEx
       */
      if (_g_main_poll_debug)
	g_print ("  MsgWaitForMultipleObjectsEx(%d, %lu)\n", nhandles, timeout_ms);

      ready = MsgWaitForMultipleObjectsEx (nhandles, handles, timeout_ms,
					   QS_ALLINPUT, MWMO_ALERTABLE);

      if (ready == WAIT_FAILED)
	{
	  gchar *emsg = g_win32_error_message (GetLastError ());
	  g_warning ("MsgWaitForMultipleObjectsEx failed: %s", emsg);
	  g_free (emsg);
	}
    }
  else if (nhandles == 0)
    {
      /* No handles to wait for, just the timeout */
      if (timeout_ms == INFINITE)
	ready = WAIT_FAILED;
      else
        {
          /* Wait for the current process to die, more efficient than SleepEx(). */
          WaitForSingleObjectEx (GetCurrentProcess (), timeout_ms, TRUE);
          ready = WAIT_TIMEOUT;
        }
    }
  else
    {
      /* Wait for just handles
       * -> Use WaitForMultipleObjectsEx
       */
      if (_g_main_poll_debug)
	g_print ("  WaitForMultipleObjectsEx(%d, %lu)\n", nhandles, timeout_ms);

      ready = WaitForMultipleObjectsEx (nhandles, handles, FALSE, timeout_ms, TRUE);
      if (ready == WAIT_FAILED)
	{
	  gchar *emsg = g_win32_error_message (GetLastError ());
	  g_warning ("WaitForMultipleObjectsEx failed: %s", emsg);
	  g_free (emsg);
	}
    }

  if (_g_main_poll_debug)
    g_print ("  wait returns %ld%s\n",
	     ready,
	     (ready == WAIT_FAILED ? " (WAIT_FAILED)" :
	      (ready == WAIT_TIMEOUT ? " (WAIT_TIMEOUT)" :
	       (msg_fd != NULL && ready == WAIT_OBJECT_0 + nhandles ? " (msg)" : ""))));

  if (ready == WAIT_FAILED)
    return -1;
  else if (ready == WAIT_TIMEOUT ||
	   ready == WAIT_IO_COMPLETION)
    return 0;
  else if (msg_fd != NULL && ready == WAIT_OBJECT_0 + nhandles)
    {
      msg_fd->revents |= G_IO_IN;

      /* If we have a timeout, or no handles to poll, be satisfied
       * with just noticing we have messages waiting.
       */
      if (timeout_ms != 0 || nhandles == 0)
	return 1;

      /* If no timeout and handles to poll, recurse to poll them,
       * too.
       */
      recursed_result = poll_rest (NULL, stop_fd, handles, handle_to_fd, nhandles, 0);
      return (recursed_result == -1) ? -1 : 1 + recursed_result;
    }
  else if (ready < WAIT_OBJECT_0 + nhandles)
    {
      int retval;

      f = handle_to_fd[ready - WAIT_OBJECT_0];
      f->revents = f->events;
      if (_g_main_poll_debug)
        g_print ("  got event %p\n", (HANDLE) f->fd);

      /* Do not count the stop_fd */
      retval = (f != stop_fd) ? 1 : 0;

      /* If no timeout and polling several handles, recurse to poll
       * the rest of them.
       */
      if (timeout_ms == 0 && nhandles > 1)
        {
          /* Poll the handles with index > ready */
          HANDLE *shorter_handles;
          GPollFD **shorter_handle_to_fd;
          gint shorter_nhandles;

          shorter_handles = &handles[ready - WAIT_OBJECT_0 + 1];
          shorter_handle_to_fd = &handle_to_fd[ready - WAIT_OBJECT_0 + 1];
          shorter_nhandles = nhandles - (ready - WAIT_OBJECT_0 + 1);

          recursed_result = poll_rest (NULL, stop_fd, shorter_handles, shorter_handle_to_fd, shorter_nhandles, 0);
          return (recursed_result == -1) ? -1 : retval + recursed_result;
        }
      return retval;
    }

  return 0;
}

typedef struct
{
  HANDLE handles[MAXIMUM_WAIT_OBJECTS];
  GPollFD *handle_to_fd[MAXIMUM_WAIT_OBJECTS];
  GPollFD *msg_fd;
  GPollFD *stop_fd;
  gint nhandles;
  DWORD    timeout_ms;
} GWin32PollThreadData;

static gint
poll_single_thread (GWin32PollThreadData *data)
{
  int retval;

  /* Polling for several things? */
  if (data->nhandles > 1 || (data->nhandles > 0 && data->msg_fd != NULL))
    {
      /* First check if one or several of them are immediately
       * available
       */
      retval = poll_rest (data->msg_fd, data->stop_fd, data->handles, data->handle_to_fd, data->nhandles, 0);

      /* If not, and we have a significant timeout, poll again with
       * timeout then. Note that this will return indication for only
       * one event, or only for messages.
       */
      if (retval == 0 && (data->timeout_ms == INFINITE || data->timeout_ms > 0))
        retval = poll_rest (data->msg_fd, data->stop_fd, data->handles, data->handle_to_fd, data->nhandles, data->timeout_ms);
    }
  else
    {
      /* Just polling for one thing, so no need to check first if
       * available immediately
       */
      retval = poll_rest (data->msg_fd, data->stop_fd, data->handles, data->handle_to_fd, data->nhandles, data->timeout_ms);
    }

  return retval;
}

static void
fill_poll_thread_data (GPollFD              *fds,
                       guint                 nfds,
                       DWORD                 timeout_ms,
                       GPollFD              *stop_fd,
                       GWin32PollThreadData *data)
{
  GPollFD *f;

  data->timeout_ms = timeout_ms;

  if (stop_fd != NULL)
    {
      if (_g_main_poll_debug)
        g_print (" Stop FD: %p", (HANDLE) stop_fd->fd);

      g_assert (data->nhandles < MAXIMUM_WAIT_OBJECTS);

      data->stop_fd = stop_fd;
      data->handle_to_fd[data->nhandles] = stop_fd;
      data->handles[data->nhandles++] = (HANDLE) stop_fd->fd;
    }

  for (f = fds; f < &fds[nfds]; ++f)
    {
      if ((data->nhandles == MAXIMUM_WAIT_OBJECTS) ||
          (data->msg_fd != NULL && (data->nhandles == MAXIMUM_WAIT_OBJECTS - 1)))
        {
          g_warning ("Too many handles to wait for!");
          break;
        }

      if (f->fd == G_WIN32_MSG_HANDLE && (f->events & G_IO_IN))
        {
          if (_g_main_poll_debug && data->msg_fd == NULL)
            g_print (" MSG");
          data->msg_fd = f;
        }
      else if (f->fd > 0)
        {
          if (_g_main_poll_debug)
            g_print (" %p", (HANDLE) f->fd);
          data->handle_to_fd[data->nhandles] = f;
          data->handles[data->nhandles++] = (HANDLE) f->fd;
        }

      f->revents = 0;
    }
}

static guint __stdcall
poll_thread_run (gpointer user_data)
{
  GWin32PollThreadData *data = user_data;

  /* Docs say that it is safer to call _endthreadex by our own:
   * https://docs.microsoft.com/en-us/cpp/c-runtime-library/reference/endthread-endthreadex
   */
  _endthreadex (poll_single_thread (data));

  g_assert_not_reached ();

  return 0;
}

/* One slot for a possible msg object or the stop event */
#define MAXIMUM_WAIT_OBJECTS_PER_THREAD (MAXIMUM_WAIT_OBJECTS - 1)

gint
g_poll (GPollFD *fds,
	guint    nfds,
	gint     timeout)
{
  guint nthreads, threads_remain;
  HANDLE thread_handles[MAXIMUM_WAIT_OBJECTS];
  GWin32PollThreadData *threads_data;
  GPollFD stop_event = { 0, };
  GPollFD *f;
  guint i, fds_idx = 0;
  DWORD ready;
  DWORD thread_retval;
  int retval;
  GPollFD *msg_fd = NULL;

  if (timeout == -1)
    timeout = INFINITE;

  /* Simple case without extra threads */
  if (nfds <= MAXIMUM_WAIT_OBJECTS)
    {
      GWin32PollThreadData data = { 0, };

      if (_g_main_poll_debug)
        g_print ("g_poll: waiting for");

      fill_poll_thread_data (fds, nfds, timeout, NULL, &data);

      if (_g_main_poll_debug)
        g_print ("\n");

      retval = poll_single_thread (&data);
      if (retval == -1)
        for (f = fds; f < &fds[nfds]; ++f)
          f->revents = 0;

      return retval;
    }

  if (_g_main_poll_debug)
    g_print ("g_poll: polling with threads\n");

  nthreads = nfds / MAXIMUM_WAIT_OBJECTS_PER_THREAD;
  threads_remain = nfds % MAXIMUM_WAIT_OBJECTS_PER_THREAD;
  if (threads_remain > 0)
    nthreads++;

  if (nthreads > MAXIMUM_WAIT_OBJECTS_PER_THREAD)
    {
      g_warning ("Too many handles to wait for in threads!");
      nthreads = MAXIMUM_WAIT_OBJECTS_PER_THREAD;
    }

#if GLIB_SIZEOF_VOID_P == 8
  stop_event.fd = (gint64)CreateEventW (NULL, TRUE, FALSE, NULL);
#else
  stop_event.fd = (gint)CreateEventW (NULL, TRUE, FALSE, NULL);
#endif
  stop_event.events = G_IO_IN;

  threads_data = g_new0 (GWin32PollThreadData, nthreads);
  for (i = 0; i < nthreads; i++)
    {
      guint thread_fds;
      guint ignore;

      if (i == (nthreads - 1) && threads_remain > 0)
        thread_fds = threads_remain;
      else
        thread_fds = MAXIMUM_WAIT_OBJECTS_PER_THREAD;

      fill_poll_thread_data (fds + fds_idx, thread_fds, timeout, &stop_event, &threads_data[i]);
      fds_idx += thread_fds;

      /* We must poll for messages from the same thread, so poll it along with the threads */
      if (threads_data[i].msg_fd != NULL)
        {
          msg_fd = threads_data[i].msg_fd;
          threads_data[i].msg_fd = NULL;
        }

      thread_handles[i] = (HANDLE) _beginthreadex (NULL, 0, poll_thread_run, &threads_data[i], 0, &ignore);
    }

  /* Wait for at least one thread to return */
  if (msg_fd != NULL)
    ready = MsgWaitForMultipleObjectsEx (nthreads, thread_handles, timeout,
                                         QS_ALLINPUT, MWMO_ALERTABLE);
  else
    ready = WaitForMultipleObjects (nthreads, thread_handles, FALSE, timeout);

  /* Signal the stop in case any of the threads did not stop yet */
  if (!SetEvent ((HANDLE)stop_event.fd))
    {
      gchar *emsg = g_win32_error_message (GetLastError ());
      g_warning ("gpoll: failed to signal the stop event: %s", emsg);
      g_free (emsg);
    }

  /* Wait for the rest of the threads to finish */
  WaitForMultipleObjects (nthreads, thread_handles, TRUE, INFINITE);

  /* The return value of all the threads give us all the fds that changed state */
  retval = 0;
  if (msg_fd != NULL && ready == WAIT_OBJECT_0 + nthreads)
    {
      msg_fd->revents |= G_IO_IN;
      retval = 1;
    }

  for (i = 0; i < nthreads; i++)
    {
      if (GetExitCodeThread (thread_handles[i], &thread_retval))
        retval = (retval == -1) ? -1 : ((thread_retval == (DWORD) -1) ? -1 : (int) (retval + thread_retval));

      CloseHandle (thread_handles[i]);
    }

  if (retval == -1)
    for (f = fds; f < &fds[nfds]; ++f)
      f->revents = 0;

  g_free (threads_data);
  CloseHandle ((HANDLE)stop_event.fd);

  return retval;
}

#else  /* !G_OS_WIN32 */

/* The following implementation of poll() comes from the GNU C Library.
 * Copyright (C) 1994, 1996, 1997 Free Software Foundation, Inc.
 */

#include <string.h> /* for bzero on BSD systems */

#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif /* HAVE_SYS_SELECT_H */

gint
g_poll (GPollFD *fds,
	guint    nfds,
	gint     timeout)
{
  struct timeval tv;
  fd_set rset, wset, xset;
  GPollFD *f;
  int ready;
  int maxfd = 0;

  FD_ZERO (&rset);
  FD_ZERO (&wset);
  FD_ZERO (&xset);

  for (f = fds; f < &fds[nfds]; ++f)
    if (f->fd >= 0)
      {
	if (f->events & G_IO_IN)
	  FD_SET (f->fd, &rset);
	if (f->events & G_IO_OUT)
	  FD_SET (f->fd, &wset);
	if (f->events & G_IO_PRI)
	  FD_SET (f->fd, &xset);
	if (f->fd > maxfd && (f->events & (G_IO_IN|G_IO_OUT|G_IO_PRI)))
	  maxfd = f->fd;
      }

  tv.tv_sec = timeout / 1000;
  tv.tv_usec = (timeout % 1000) * 1000;

  ready = select (maxfd + 1, &rset, &wset, &xset,
		  timeout == -1 ? NULL : &tv);
  if (ready > 0)
    for (f = fds; f < &fds[nfds]; ++f)
      {
	f->revents = 0;
	if (f->fd >= 0)
	  {
	    if (FD_ISSET (f->fd, &rset))
	      f->revents |= G_IO_IN;
	    if (FD_ISSET (f->fd, &wset))
	      f->revents |= G_IO_OUT;
	    if (FD_ISSET (f->fd, &xset))
	      f->revents |= G_IO_PRI;
	  }
      }

  return ready;
}

#endif /* !G_OS_WIN32 */

#endif	/* !HAVE_POLL */
