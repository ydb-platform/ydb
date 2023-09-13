/* io_queue_init.c
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
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "syscall.h"

int io_queue_init(int maxevents, io_context_t *ctxp)
{
	if (maxevents > 0) {
		*ctxp = NULL;
		return io_setup(maxevents, ctxp);
	}
	return -EINVAL;
}
