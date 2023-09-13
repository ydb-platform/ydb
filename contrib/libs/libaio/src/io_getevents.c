/* io_getevents.c
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
#include <libaio.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include "syscall.h"
#include "aio_ring.h"

io_syscall5(int, __io_getevents_0_4, io_getevents, io_context_t, ctx, long, min_nr, long, nr, struct io_event *, events, struct timespec *, timeout)

int io_getevents(io_context_t ctx, long min_nr, long nr, struct io_event * events, struct timespec * timeout)
{
	if (aio_ring_is_empty(ctx, timeout))
		return 0;
	return __io_getevents_0_4(ctx, min_nr, nr, events, timeout);
}

// DEFSYMVER(io_getevents_0_4, io_getevents, 0.4)
