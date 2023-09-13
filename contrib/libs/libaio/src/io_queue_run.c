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
#include <libaio.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>

int io_queue_run(io_context_t ctx)
{
	static struct timespec timeout = { 0, 0 };
	struct io_event event;
	int ret;

	/* FIXME: batch requests? */
	while (1 == (ret = io_getevents(ctx, 0, 1, &event, &timeout))) {
		io_callback_t cb = (io_callback_t)event.data;
		struct iocb *iocb = event.obj;

		cb(ctx, iocb, event.res, event.res2);
	}

	return ret;
}
