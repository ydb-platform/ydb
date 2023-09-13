/* io_submit
   libaio Linux async I/O interface
   Copyright 2002 Red Hat, Inc.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with this library; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
#define NO_SYSCALL_ERRNO
#include <sys/types.h>
#include <libaio.h>
#include <errno.h>
#include "syscall.h"

struct timespec;

int io_queue_wait(io_context_t ctx, struct timespec *timeout)
{
	return io_getevents(ctx, 0, 0, NULL, timeout);
}
