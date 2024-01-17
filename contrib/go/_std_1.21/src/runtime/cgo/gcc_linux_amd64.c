// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include <pthread.h>
#include <errno.h>
#include <string.h> // strerror
#include <signal.h>
#include <stdlib.h>
#include "libcgo.h"
#include "libcgo_unix.h"

static void* threadentry(void*);
static void (*setg_gcc)(void*);

// This will be set in gcc_android.c for android-specific customization.
void (*x_cgo_inittls)(void **tlsg, void **tlsbase) __attribute__((common));

void
x_cgo_init(G *g, void (*setg)(void*), void **tlsg, void **tlsbase)
{
	pthread_attr_t *attr;
	size_t size;

	/* The memory sanitizer distributed with versions of clang
	   before 3.8 has a bug: if you call mmap before malloc, mmap
	   may return an address that is later overwritten by the msan
	   library.  Avoid this problem by forcing a call to malloc
	   here, before we ever call malloc.

	   This is only required for the memory sanitizer, so it's
	   unfortunate that we always run it.  It should be possible
	   to remove this when we no longer care about versions of
	   clang before 3.8.  The test for this is
	   misc/cgo/testsanitizers.

	   GCC works hard to eliminate a seemingly unnecessary call to
	   malloc, so we actually use the memory we allocate.  */

	setg_gcc = setg;
	attr = (pthread_attr_t*)malloc(sizeof *attr);
	if (attr == NULL) {
		fatalf("malloc failed: %s", strerror(errno));
	}
	pthread_attr_init(attr);
	pthread_attr_getstacksize(attr, &size);
	g->stacklo = (uintptr)__builtin_frame_address(0) - size + 4096;
	if (g->stacklo >= g->stackhi)
		fatalf("bad stack bounds: lo=%p hi=%p\n", g->stacklo, g->stackhi);
	pthread_attr_destroy(attr);
	free(attr);

	if (x_cgo_inittls) {
		x_cgo_inittls(tlsg, tlsbase);
	}
}


void
_cgo_sys_thread_start(ThreadStart *ts)
{
	pthread_attr_t attr;
	sigset_t ign, oset;
	pthread_t p;
	size_t size;
	int err;

	sigfillset(&ign);
	pthread_sigmask(SIG_SETMASK, &ign, &oset);

	pthread_attr_init(&attr);
	pthread_attr_getstacksize(&attr, &size);
	// Leave stacklo=0 and set stackhi=size; mstart will do the rest.
	ts->g->stackhi = size;
	err = _cgo_try_pthread_create(&p, &attr, threadentry, ts);

	pthread_sigmask(SIG_SETMASK, &oset, nil);

	if (err != 0) {
		fatalf("pthread_create failed: %s", strerror(err));
	}
}

static void*
threadentry(void *v)
{
	ThreadStart ts;

	ts = *(ThreadStart*)v;
	_cgo_tsan_acquire();
	free(v);
	_cgo_tsan_release();

	crosscall_amd64(ts.fn, setg_gcc, (void*)ts.g);
	return nil;
}
