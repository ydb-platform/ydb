/*
 * Copyright © 2021 Ole André Vadla Ravnås
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
 */

#include <contrib/restricted/glib/config.h>

#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined (HAVE_EPOLL_CREATE)
#include <sys/epoll.h>
#elif defined (HAVE_KQUEUE)
#include <sys/event.h>
#include <sys/time.h>
#endif

#include "giounix-private.h"

#define G_TEMP_FAILURE_RETRY(expression)      \
  ({                                          \
    gssize __result;                          \
                                              \
    do                                        \
      __result = (gssize) (expression);       \
    while (__result == -1 && errno == EINTR); \
                                              \
    __result;                                 \
  })

static gboolean g_fd_is_regular_file (int fd) G_GNUC_UNUSED;

gboolean
_g_fd_is_pollable (int fd)
{
  /*
   * Determining whether a file-descriptor (FD) is pollable turns out to be
   * quite hard.
   *
   * We used to detect this by attempting to lseek() and check if it failed with
   * ESPIPE, and if so we'd consider the FD pollable. But this turned out to not
   * work on e.g. PTYs and other devices that are pollable.
   *
   * Another approach that was considered was to call fstat() and if it failed
   * we'd assume that the FD is pollable, and if it succeeded we'd consider it
   * pollable as long as it's not a regular file. This seemed to work alright
   * except for FDs backed by simple devices, such as /dev/null.
   *
   * There are however OS-specific methods that allow us to figure this out with
   * absolute certainty:
   */

#if defined (HAVE_EPOLL_CREATE)
  /*
   * Linux
   *
   * The answer we seek is provided by the kernel's file_can_poll():
   * https://github.com/torvalds/linux/blob/2ab38c17aac10bf55ab3efde4c4db3893d8691d2/include/linux/poll.h#L81-L84
   * But we cannot probe that by using poll() as the returned events for
   * non-pollable FDs are always IN | OUT.
   *
   * The best option then seems to be using epoll, as it will refuse to add FDs
   * where file_can_poll() returns FALSE.
   */

  int efd;
  struct epoll_event ev = { 0, };
  gboolean add_succeeded;

  efd = epoll_create (1);
  if (efd == -1)
    g_error ("epoll_create () failed: %s", g_strerror (errno));

  ev.events = EPOLLIN;

  add_succeeded = epoll_ctl (efd, EPOLL_CTL_ADD, fd, &ev) == 0;

  close (efd);

  return add_succeeded;
#elif defined (HAVE_KQUEUE)
  /*
   * Apple OSes and BSDs
   *
   * Like on Linux, we cannot use poll() to do the probing, but kqueue does
   * the trick as it will refuse to add non-pollable FDs. (Except for regular
   * files, which we need to special-case. Even though kqueue does support them,
   * poll() does not.)
   */

  int kfd;
  struct kevent ev;
  gboolean add_succeeded;

  if (g_fd_is_regular_file (fd))
    return FALSE;

  kfd = kqueue ();
  if (kfd == -1)
    g_error ("kqueue () failed: %s", g_strerror (errno));

  EV_SET (&ev, fd, EVFILT_READ, EV_ADD, 0, 0, NULL);

  add_succeeded =
      G_TEMP_FAILURE_RETRY (kevent (kfd, &ev, 1, NULL, 0, NULL)) == 0;

  close (kfd);

  return add_succeeded;
#else
  /*
   * Other UNIXes (AIX, QNX, Solaris, etc.)
   *
   * We can rule out regular files, but devices such as /dev/null will be
   * reported as pollable even though they're not. This is hopefully good
   * enough for most use-cases, but easy to expand on later if needed.
   */

  return !g_fd_is_regular_file (fd);
#endif
}

static gboolean
g_fd_is_regular_file (int fd)
{
  struct stat st;

  if (G_TEMP_FAILURE_RETRY (fstat (fd, &st)) == -1)
    return FALSE;

  return S_ISREG (st.st_mode);
}
