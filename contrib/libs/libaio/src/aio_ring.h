/*
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
#ifndef _AIO_RING_H
#define _AIO_RING_H

#define AIO_RING_MAGIC                  0xa10a10a1

struct aio_ring {
	unsigned        id;     /* kernel internal index number */
	unsigned        nr;     /* number of io_events */
	unsigned        head;
	unsigned        tail;

	unsigned        magic;
	unsigned        compat_features;
	unsigned        incompat_features;
	unsigned        header_length;  /* size of aio_ring */
};

static inline int aio_ring_is_empty(io_context_t ctx, struct timespec *timeout)
{
	struct aio_ring *ring = (struct aio_ring *)ctx;

	if (!ring || ring->magic != AIO_RING_MAGIC)
		return 0;
	if (!timeout || timeout->tv_sec || timeout->tv_nsec)
		return 0;
	if (ring->head != ring->tail)
		return 0;
	return 1;
}

#endif /* _AIO_RING_H */
