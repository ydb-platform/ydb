/*
   libaio Linux async I/O interface
   Copyright 2018 Christoph Hellwig.

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
#include <signal.h>
#include "syscall.h"
#include "aio_ring.h"

#ifdef __NR_io_pgetevents
io_syscall6(int, __io_pgetevents, io_pgetevents, io_context_t, ctx, long,
		min_nr, long, nr, struct io_event *, events,
		struct timespec *, timeout, void *, sigmask);

int io_pgetevents(io_context_t ctx, long min_nr, long nr,
		struct io_event *events, struct timespec *timeout,
		sigset_t *sigmask)
{
	struct {
		unsigned long ss;
		unsigned long ss_len;
	} data;

	if (aio_ring_is_empty(ctx, timeout))
		return 0;

	data.ss = (unsigned long)sigmask;
	data.ss_len = _NSIG / 8;
	return __io_pgetevents(ctx, min_nr, nr, events, timeout, &data);
}
#else
int io_pgetevents(io_context_t ctx, long min_nr, long nr,
		struct io_event *events, struct timespec *timeout,
		sigset_t *sigmask)

{
	return -ENOSYS;
}
#endif /* __NR_io_pgetevents */
