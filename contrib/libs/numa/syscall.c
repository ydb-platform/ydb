/* Copyright (C) 2003,2004 Andi Kleen, SuSE Labs.

   libnuma is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; version
   2.1.

   libnuma is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should find a copy of v2.1 of the GNU Lesser General Public License
   somewhere on your Linux system; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA */
#include <unistd.h>
#include <sys/types.h>
#include <asm/unistd.h>
#include <errno.h>
#include "numa.h"
#include "numaif.h"
#include "numaint.h"
#include "config.h"
#include "util.h"

#define WEAK __attribute__((weak))

#if !defined(__NR_mbind) || !defined(__NR_set_mempolicy) || \
    !defined(__NR_get_mempolicy) || !defined(__NR_migrate_pages) || \
    !defined(__NR_move_pages)

#if defined(__x86_64__)

#define __NR_sched_setaffinity    203
#define __NR_sched_getaffinity     204

/* Official allocation */

#define __NR_mbind 237
#define __NR_set_mempolicy 238
#define __NR_get_mempolicy 239
#define __NR_migrate_pages 256
#define __NR_move_pages 279

#elif defined(__ia64__)
#define __NR_sched_setaffinity    1231
#define __NR_sched_getaffinity    1232
#define __NR_migrate_pages	1280
#define __NR_move_pages 1276

/* Official allocation */

#define __NR_mbind 1259
#define __NR_get_mempolicy 1260
#define __NR_set_mempolicy 1261

#elif defined(__i386__)

#define __NR_mbind 274
#define __NR_get_mempolicy 275
#define __NR_set_mempolicy 276
#define __NR_migrate_pages 294
#define __NR_move_pages 317

#elif defined(__powerpc__)

#define __NR_mbind 259
#define __NR_get_mempolicy 260
#define __NR_set_mempolicy 261
#define __NR_migrate_pages 258
/* FIXME: powerpc is missing move pages!!!
#define __NR_move_pages xxx
*/

#elif defined(__loongarch__)

//reference to /usr/include/asm-generic/unistd.h

#define __NR_mbind 235
#define __NR_get_mempolicy 236
#define __NR_set_mempolicy 237
#define __NR_migrate_pages 238
#define __NR_move_pages 239

#elif defined(__mips__)

#if _MIPS_SIM == _ABIO32
/*
 * Linux o32 style syscalls are in the range from 4000 to 4999.
 */
#define __NR_Linux 4000
#define __NR_mbind (__NR_Linux + 268)
#define __NR_get_mempolicy (__NR_Linux + 269)
#define __NR_set_mempolicy (__NR_Linux + 270)
#define __NR_migrate_pages (__NR_Linux + 287)
#endif

#if _MIPS_SIM == _ABI64
/*
 * Linux 64-bit syscalls are in the range from 5000 to 5999.
 */
#define __NR_Linux 5000
#define __NR_mbind (__NR_Linux + 227)
#define __NR_get_mempolicy (__NR_Linux + 228)
#define __NR_set_mempolicy (__NR_Linux + 229)
#define __NR_migrate_pages (__NR_Linux + 246)
#endif

#if _MIPS_SIM == _ABIN32
/*
 * Linux N32 syscalls are in the range from 6000 to 6999.
 */
#define __NR_Linux 6000
#define __NR_mbind (__NR_Linux + 231)
#define __NR_get_mempolicy (__NR_Linux + 232)
#define __NR_set_mempolicy (__NR_Linux + 233)
#define __NR_migrate_pages (__NR_Linux + 250)
#endif

#elif defined(__hppa__)

#define __NR_migrate_pages	272

#elif defined(__arm__)
/* https://bugs.debian.org/796802 */
#warning "ARM does not implement the migrate_pages() syscall"

#elif defined(__s390x__)

#define __NR_mbind 268
#define __NR_get_mempolicy 269
#define __NR_set_mempolicy 270
#define __NR_migrate_pages 287
#define __NR_move_pages    310

#elif !defined(DEPS_RUN)
#error "Add syscalls for your architecture or update kernel headers"
#endif

#endif

#ifndef __GLIBC_PREREQ
# define __GLIBC_PREREQ(x,y) 0
#endif

#if defined(__GLIBC__) && __GLIBC_PREREQ(2, 11)

/* glibc 2.11 seems to have working 6 argument sycall. Use the
   glibc supplied syscall in this case.
   The version cut-off is rather arbitrary and could be probably
   earlier. */

#define syscall6 syscall
#elif defined(__x86_64__)
/* 6 argument calls on x86-64 are often buggy in both glibc and
   asm/unistd.h. Add a working version here. */
long syscall6(long call, long a, long b, long c, long d, long e, long f)
{
       long res;
       asm volatile ("movq %[d],%%r10 ; movq %[e],%%r8 ; movq %[f],%%r9 ; syscall"
		     : "=a" (res)
		     : "0" (call),"D" (a),"S" (b), "d" (c),
		       [d] "g" (d), [e] "g" (e), [f] "g" (f) :
		     "r11","rcx","r8","r10","r9","memory" );
       if (res < 0) {
	       errno = -res;
	       res = -1;
       }
       return res;
}
#elif defined(__i386__)

/* i386 has buggy syscall6 in glibc too. This is tricky to do
   in inline assembly because it clobbers so many registers. Do it
   out of line. */
asm(
"__syscall6:\n"
"	pushl %ebp\n"
"	pushl %edi\n"
"	pushl %esi\n"
"	pushl %ebx\n"
"	movl  (0+5)*4(%esp),%eax\n"
"	movl  (1+5)*4(%esp),%ebx\n"
"	movl  (2+5)*4(%esp),%ecx\n"
"	movl  (3+5)*4(%esp),%edx\n"
"	movl  (4+5)*4(%esp),%esi\n"
"	movl  (5+5)*4(%esp),%edi\n"
"	movl  (6+5)*4(%esp),%ebp\n"
"	int $0x80\n"
"	popl %ebx\n"
"	popl %esi\n"
"	popl %edi\n"
"	popl %ebp\n"
"	ret"
);
extern long __syscall6(long n, long a, long b, long c, long d, long e, long f);

long syscall6(long call, long a, long b, long c, long d, long e, long f)
{
       long res = __syscall6(call,a,b,c,d,e,f);
       if (res < 0) {
	       errno = -res;
	       res = -1;
       }
       return res;
}

#else
#define syscall6 syscall
#endif

long WEAK get_mempolicy(int *policy, unsigned long *nmask,
				unsigned long maxnode, void *addr,
				unsigned flags)
{
	return syscall(__NR_get_mempolicy, policy, nmask,
					maxnode, addr, flags);
}

long WEAK mbind(void *start, unsigned long len, int mode,
	const unsigned long *nmask, unsigned long maxnode, unsigned flags)
{
	return syscall6(__NR_mbind, (long)start, len, mode, (long)nmask,
				maxnode, flags);
}

long WEAK set_mempolicy(int mode, const unsigned long *nmask,
                                   unsigned long maxnode)
{
	long i;
	i = syscall(__NR_set_mempolicy,mode,nmask,maxnode);
	return i;
}

long WEAK migrate_pages(int pid, unsigned long maxnode,
	const unsigned long *frommask, const unsigned long *tomask)
{
#if defined(__NR_migrate_pages)
	return syscall(__NR_migrate_pages, pid, maxnode, frommask, tomask);
#else
    errno = ENOSYS;
    return -1;
#endif
}

long WEAK move_pages(int pid, unsigned long count,
	void **pages, const int *nodes, int *status, int flags)
{
	return syscall(__NR_move_pages, pid, count, pages, nodes, status, flags);
}

/* SLES8 glibc doesn't define those */
SYMVER("numa_sched_setaffinity_v1", "numa_sched_setaffinity@libnuma_1.1")
int numa_sched_setaffinity_v1(pid_t pid, unsigned len, const unsigned long *mask)
{
	return syscall(__NR_sched_setaffinity,pid,len,mask);
}

SYMVER("numa_sched_setaffinity_v2", "numa_sched_setaffinity@@libnuma_1.2")
int numa_sched_setaffinity_v2(pid_t pid, struct bitmask *mask)
{
	return syscall(__NR_sched_setaffinity, pid, numa_bitmask_nbytes(mask),
								mask->maskp);
}

SYMVER("numa_sched_getaffinity_v1", "numa_sched_getaffinity@libnuma_1.1")
int numa_sched_getaffinity_v1(pid_t pid, unsigned len, const unsigned long *mask)
{
	return syscall(__NR_sched_getaffinity,pid,len,mask);
}

SYMVER("numa_sched_getaffinity_v2", "numa_sched_getaffinity@@libnuma_1.2")
int numa_sched_getaffinity_v2(pid_t pid, struct bitmask *mask)
{
	/* len is length in bytes */
	return syscall(__NR_sched_getaffinity, pid, numa_bitmask_nbytes(mask),
								mask->maskp);
	/* sched_getaffinity returns sizeof(cpumask_t) */
}

make_internal_alias(numa_sched_getaffinity_v1);
make_internal_alias(numa_sched_getaffinity_v2);
make_internal_alias(numa_sched_setaffinity_v1);
make_internal_alias(numa_sched_setaffinity_v2);
