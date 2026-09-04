/* state.c -- Create the backtrace state.
   Copyright (C) 2012-2026 Free Software Foundation, Inc.
   Written by Ian Lance Taylor, Google.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    (1) Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

    (2) Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in
    the documentation and/or other materials provided with the
    distribution.

    (3) The name of the author may not be used to
    endorse or promote products derived from this software without
    specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.  */

#include "config.h"

#include <string.h>
#include <sys/types.h>

#include "backtrace.h"
#include "backtrace-supported.h"
#include "internal.h"

/* Create the backtrace state.  This will then be passed to all the
   other routines.  */

struct backtrace_state *
backtrace_create_state (const char *filename, int flags,
			backtrace_error_callback error_callback,
			void *data)
{
  int threaded;
  int moredata;
  struct backtrace_state init_state;
  struct backtrace_state *state;

  threaded = (flags & 1) != 0;
  moredata = (flags & 2) != 0;
  if ((flags & ~3) != 0)
    {
      error_callback (data, "backtrace_create_state: unsupported flag", 0);
      return NULL;
    }

#ifndef HAVE_SYNC_FUNCTIONS
  if (threaded)
    {
      error_callback (data, "backtrace library does not support threads", 0);
      return NULL;
    }
#endif

  memset (&init_state, 0, sizeof init_state);
  init_state.filename = filename;
  init_state.threaded = threaded;
  init_state.moredata = moredata;

  state = ((struct backtrace_state *)
	   backtrace_alloc (&init_state, sizeof *state, error_callback, data));
  if (state == NULL)
    return NULL;
  *state = init_state;

  return state;
}
