/*
 * SPDX-License-Identifier: BSD-3-Clause
 * Copyright © 2009 CNRS
 * Copyright © 2009-2026 Inria.  All rights reserved.
 * Copyright © 2009-2013, 2015, 2020 Université Bordeaux
 * Copyright © 2009-2018 Cisco Systems, Inc.  All rights reserved.
 * Copyright © 2015 Intel, Inc.  All rights reserved.
 * Copyright © 2010 IBM
 * See COPYING in top-level directory.
 */

#include "private/autogen/config.h"
#include "hwloc.h"
#include "hwloc/linux.h"
#include "private/misc.h"
#include "private/private.h"
#include "private/misc.h"
#include "private/debug.h"

#include <limits.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HWLOC_HAVE_LIBUDEV
#error #include <libudev.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <sched.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <mntent.h>
#include <stddef.h>
#include <endian.h>

struct hwloc_linux_backend_data_s {
  char *root_path; /* NULL if unused */
  int root_fd; /* The file descriptor for the file system root, used when browsing, e.g., Linux' sysfs and procfs. */
  int is_real_fsroot; /* Boolean saying whether root_fd points to the real filesystem root of the system */
#ifdef HWLOC_HAVE_LIBUDEV
  struct udev *udev; /* Global udev context */
#endif
  const char *dumped_hwdata_dirname;
  enum {
    HWLOC_LINUX_ARCH_X86, /* x86 32 or 64bits, including k1om (KNC) */
    HWLOC_LINUX_ARCH_IA64,
    HWLOC_LINUX_ARCH_ARM,
    HWLOC_LINUX_ARCH_POWER,
    HWLOC_LINUX_ARCH_S390,
    HWLOC_LINUX_ARCH_LOONGARCH,
    HWLOC_LINUX_ARCH_UNKNOWN
  } arch;
  int is_knl;
  int is_amd_with_CU;
  int is_amd_homogeneous;
  int is_fake_numa_uniform; /* 0 if not fake, -1 if fake non-uniform, N if fake=<N>U */
  int use_numa_distances;
  int use_numa_distances_for_cpuless;
  int use_numa_initiators;
  struct utsname utsname; /* fields contain \0 when unknown */
  int fallback_nbprocessors; /* only used in hwloc_linux_fallback_pu_level(), maybe be <= 0 (error) earlier */
  unsigned pagesize;
};



/***************************
 * Misc Abstraction layers *
 ***************************/

#if !(defined HWLOC_HAVE_SCHED_SETAFFINITY) && (defined HWLOC_HAVE_SYSCALL)
/* libc doesn't have support for sched_setaffinity, make system call
 * ourselves: */
#    ifndef __NR_sched_setaffinity
#       ifdef __i386__
#         define __NR_sched_setaffinity 241
#       elif defined(__x86_64__)
#         define __NR_sched_setaffinity 203
#       elif defined(__ia64__)
#         define __NR_sched_setaffinity 1231
#       elif defined(__hppa__)
#         define __NR_sched_setaffinity 211
#       elif defined(__alpha__)
#         define __NR_sched_setaffinity 395
#       elif defined(__s390__) || defined(__s390x__)
#         define __NR_sched_setaffinity 239
#       elif defined(__sparc__)
#         define __NR_sched_setaffinity 261
#       elif defined(__m68k__)
#         define __NR_sched_setaffinity 311
#       elif defined(__powerpc__) || defined(__ppc__) || defined(__PPC__) || defined(__powerpc64__) || defined(__ppc64__)
#         define __NR_sched_setaffinity 222
#       elif defined(__aarch64__)
#         define __NR_sched_setaffinity 122
#       elif defined(__riscv)
#         define __NR_sched_setaffinity 122
#       elif defined(__arm__)
#         define __NR_sched_setaffinity 241
#       elif defined(__cris__)
#         define __NR_sched_setaffinity 241
#       elif defined(__loongarch__)
#         define __NR_sched_setaffinity 122
#       elif defined(__mips__) && _MIPS_SIM == _ABI64
#         define __NR_sched_setaffinity 5195
#       elif defined(__mips__) && _MIPS_SIM == _ABIN32
#         define __NR_sched_setaffinity 6195
#       elif defined(__mips__) && _MIPS_SIM == _ABIO32
#         define __NR_sched_setaffinity 4239
#       else
#         warning "don't know the syscall number for sched_setaffinity on this architecture, will not support binding"
#         define sched_setaffinity(pid, lg, mask) (errno = ENOSYS, -1)
#       endif
#    endif
#    ifndef sched_setaffinity
#      define sched_setaffinity(pid, lg, mask) syscall(__NR_sched_setaffinity, pid, lg, mask)
#    endif
#    ifndef __NR_sched_getaffinity
#       ifdef __i386__
#         define __NR_sched_getaffinity 242
#       elif defined(__x86_64__)
#         define __NR_sched_getaffinity 204
#       elif defined(__ia64__)
#         define __NR_sched_getaffinity 1232
#       elif defined(__hppa__)
#         define __NR_sched_getaffinity 212
#       elif defined(__alpha__)
#         define __NR_sched_getaffinity 396
#       elif defined(__s390__) || defined(__s390x__)
#         define __NR_sched_getaffinity 240
#       elif defined(__sparc__)
#         define __NR_sched_getaffinity 260
#       elif defined(__m68k__)
#         define __NR_sched_getaffinity 312
#       elif defined(__powerpc__) || defined(__ppc__) || defined(__PPC__) || defined(__powerpc64__) || defined(__ppc64__)
#         define __NR_sched_getaffinity 223
#       elif defined(__aarch64__)
#         define __NR_sched_getaffinity 123
#       elif defined(__riscv)
#         define __NR_sched_getaffinity 123
#       elif defined(__arm__)
#         define __NR_sched_getaffinity 242
#       elif defined(__cris__)
#         define __NR_sched_getaffinity 242
#       elif defined(__loongarch__)
#         define __NR_sched_getaffinity 123
#       elif defined(__mips__) && _MIPS_SIM == _ABI64
#         define __NR_sched_getaffinity 5196
#       elif defined(__mips__) && _MIPS_SIM == _ABIN32
#         define __NR_sched_getaffinity 6196
#       elif defined(__mips__) && _MIPS_SIM == _ABIO32
#         define __NR_sched_getaffinity 4240
#       else
#         warning "don't know the syscall number for sched_getaffinity on this architecture, will not support getting binding"
#         define sched_getaffinity(pid, lg, mask) (errno = ENOSYS, -1)
#       endif
#    endif
#    ifndef sched_getaffinity
#      define sched_getaffinity(pid, lg, mask) (syscall(__NR_sched_getaffinity, pid, lg, mask) < 0 ? -1 : 0)
#    endif
#endif

/* numa syscalls are only in libnuma, but libnuma devel headers aren't widely installed.
 * just redefine these syscalls to avoid requiring libnuma devel headers just because of these missing syscalls.
 * __NR_foo should be defined in headers in all modern platforms.
 * Just redefine the basic ones on important platform when not to hard to detect/define.
 */

#ifndef MPOL_DEFAULT
# define MPOL_DEFAULT 0
#endif
#ifndef MPOL_PREFERRED
# define MPOL_PREFERRED 1
#endif
#ifndef MPOL_BIND
# define MPOL_BIND 2
#endif
#ifndef MPOL_INTERLEAVE
# define MPOL_INTERLEAVE 3
#endif
#ifndef MPOL_LOCAL
# define MPOL_LOCAL 4
#endif
#ifndef MPOL_PREFERRED_MANY
# define MPOL_PREFERRED_MANY 5
#endif
#ifndef MPOL_WEIGHTED_INTERLEAVE
# define MPOL_WEIGHTED_INTERLEAVE 6
#endif
#ifndef MPOL_F_ADDR
# define  MPOL_F_ADDR (1<<1)
#endif
#ifndef MPOL_MF_STRICT
# define MPOL_MF_STRICT (1<<0)
#endif
#ifndef MPOL_MF_MOVE
# define MPOL_MF_MOVE (1<<1)
#endif

#ifndef __NR_mbind
# ifdef __i386__
#  define __NR_mbind 274
# elif defined(__x86_64__)
#  define __NR_mbind 237
# elif defined(__ia64__)
#  define __NR_mbind 1259
# elif defined(__hppa__)
#  define __NR_mbind 260
# elif defined(__alpha__)
   /* sys_ni_syscall */
# elif defined(__s390) || defined(__s390x__)
#  define __NR_mbind 268
# elif defined(__sparc__)
#  define __NR_mbind 353
# elif defined(__m68k__)
#  define __NR_mbind 268
# elif defined(__powerpc__) || defined(__ppc__) || defined(__PPC__) || defined(__powerpc64__) || defined(__ppc64__)
#  define __NR_mbind 259
# elif defined(__aarch64__)
#  define __NR_mbind 235
# elif defined(__riscv)
#  define __NR_mbind 235
# elif defined(__arm__)
#  define __NR_mbind 319
# elif defined(__cris__)
   /* sys_ni_syscall when CRIS removed in 4.17 */
# elif defined(__loongarch__)
#  define __NR_mbind 235
# elif defined(__mips__) && _MIPS_SIM == _ABI64
#  define __NR_mbind 5227
# elif defined(__mips__) && _MIPS_SIM == _ABIN32
#  define __NR_mbind 6231
# elif defined(__mips__) && _MIPS_SIM == _ABIO32
#  define __NR_mbind 4268
# endif
#endif
static __hwloc_inline long hwloc_mbind(void *addr __hwloc_attribute_unused,
				       unsigned long len __hwloc_attribute_unused,
				       int mode __hwloc_attribute_unused,
				       const unsigned long *nodemask __hwloc_attribute_unused,
				       unsigned long maxnode __hwloc_attribute_unused,
				       unsigned flags __hwloc_attribute_unused)
{
#if (defined __NR_mbind) && (defined HWLOC_HAVE_SYSCALL)
  return syscall(__NR_mbind, (long) addr, len, mode, (long)nodemask, maxnode, flags);
#else
#warning Couldn't find __NR_mbind syscall number, memory binding won't be supported
  errno = ENOSYS;
  return -1;
#endif
}

#ifndef __NR_set_mempolicy
# ifdef __i386__
#  define __NR_set_mempolicy 276
# elif defined(__x86_64__)
#  define __NR_set_mempolicy 238
# elif defined(__ia64__)
#  define __NR_set_mempolicy 1261
# elif defined(__hppa__)
#  define __NR_set_mempolicy 262
# elif defined(__alpha__)
   /* sys_ni_syscall */
# elif defined(__s390) || defined(__s390x__)
#  define __NR_set_mempolicy 270
# elif defined(__sparc__)
#  define __NR_set_mempolicy 305
# elif defined(__m68k__)
#  define __NR_set_mempolicy 270
# elif defined(__powerpc__) || defined(__ppc__) || defined(__PPC__) || defined(__powerpc64__) || defined(__ppc64__)
#  define __NR_set_mempolicy 261
# elif defined(__aarch64__)
#  define __NR_set_mempolicy 237
# elif defined(__riscv)
#  define __NR_set_mempolicy 237
# elif defined(__arm__)
#  define __NR_set_mempolicy 321
# elif defined(__cris__)
   /* sys_ni_syscall when CRIS removed in 4.17 */
# elif defined(__loongarch__)
#  define __NR_set_mempolicy 237
# elif defined(__mips__) && _MIPS_SIM == _ABI64
#  define __NR_set_mempolicy 5229
# elif defined(__mips__) && _MIPS_SIM == _ABIN32
#  define __NR_set_mempolicy 6233
# elif defined(__mips__) && _MIPS_SIM == _ABIO32
#  define __NR_set_mempolicy 4270
# endif
#endif
static __hwloc_inline long hwloc_set_mempolicy(int mode __hwloc_attribute_unused,
					       const unsigned long *nodemask __hwloc_attribute_unused,
					       unsigned long maxnode __hwloc_attribute_unused)
{
#if (defined __NR_set_mempolicy) && (defined HWLOC_HAVE_SYSCALL)
  return syscall(__NR_set_mempolicy, mode, nodemask, maxnode);
#else
#warning Couldn't find __NR_set_mempolicy syscall number, memory binding won't be supported
  errno = ENOSYS;
  return -1;
#endif
}

#ifndef __NR_get_mempolicy
# ifdef __i386__
#  define __NR_get_mempolicy 275
# elif defined(__x86_64__)
#  define __NR_get_mempolicy 239
# elif defined(__ia64__)
#  define __NR_get_mempolicy 1260
# elif defined(__hppa__)
#  define __NR_get_mempolicy 261
# elif defined(__alpha__)
   /* sys_ni_syscall */
# elif defined(__s390) || defined(__s390x__)
#  define __NR_get_mempolicy 269
# elif defined(__sparc__)
#  define __NR_get_mempolicy 304
# elif defined(__m68k__)
#  define __NR_get_mempolicy 269
# elif defined(__powerpc__) || defined(__ppc__) || defined(__PPC__) || defined(__powerpc64__) || defined(__ppc64__)
#  define __NR_get_mempolicy 260
# elif defined(__aarch64__)
#  define __NR_get_mempolicy 236
# elif defined(__riscv)
#  define __NR_get_mempolicy 236
# elif defined(__arm__)
#  define __NR_get_mempolicy 320
# elif defined(__cris__)
   /* sys_ni_syscall when CRIS removed in 4.17 */
# elif defined(__loongarch__)
#  define __NR_get_mempolicy 236
# elif defined(__mips__) && _MIPS_SIM == _ABI64
#  define __NR_get_mempolicy 5228
# elif defined(__mips__) && _MIPS_SIM == _ABIN32
#  define __NR_get_mempolicy 6232
# elif defined(__mips__) && _MIPS_SIM == _ABIO32
#  define __NR_get_mempolicy 4269
# endif
#endif
static __hwloc_inline long hwloc_get_mempolicy(int *mode __hwloc_attribute_unused,
					       unsigned long *nodemask __hwloc_attribute_unused,
					       unsigned long maxnode __hwloc_attribute_unused,
					       void *addr __hwloc_attribute_unused,
					       int flags __hwloc_attribute_unused)
{
#if (defined __NR_get_mempolicy) && (defined HWLOC_HAVE_SYSCALL)
  return syscall(__NR_get_mempolicy, mode, nodemask, maxnode, addr, flags);
#else
#warning Couldn't find __NR_get_mempolicy syscall number, memory binding won't be supported
  errno = ENOSYS;
  return -1;
#endif
}

#ifndef __NR_migrate_pages
# ifdef __i386__
#  define __NR_migrate_pages 294
# elif defined(__x86_64__)
#  define __NR_migrate_pages 256
# elif defined(__ia64__)
#  define __NR_migrate_pages 1280
# elif defined(__hppa__)
#  define __NR_migrate_pages 272
# elif defined(__alpha__)
   /* sys_ni_syscall */
# elif defined(__s390) || defined(__s390x__)
#  define __NR_migrate_pages 287
# elif defined(__sparc__)
#  define __NR_migrate_pages 302
# elif defined(__m68k__)
#  define __NR_migrate_pages 287
# elif defined(__powerpc__) || defined(__ppc__) || defined(__PPC__) || defined(__powerpc64__) || defined(__ppc64__)
#  define __NR_migrate_pages 258
# elif defined(__aarch64__)
#  define __NR_migrate_pages 238
# elif defined(__riscv)
#  define __NR_migrate_pages 238
# elif defined(__arm__)
#  define __NR_migrate_pages 400
# elif defined(__cris__)
#  define __NR_migrate_pages 294
# elif defined(__loongarch__)
#  define __NR_migrate_pages 238
# elif defined(__mips__) && _MIPS_SIM == _ABI64
#  define __NR_migrate_pages 5246
# elif defined(__mips__) && _MIPS_SIM == _ABIN32
#  define __NR_migrate_pages 6250
# elif defined(__mips__) && _MIPS_SIM == _ABIO32
#  define __NR_migrate_pages 4287
# endif
#endif
static __hwloc_inline long hwloc_migrate_pages(int pid __hwloc_attribute_unused,
					       unsigned long maxnode __hwloc_attribute_unused,
					       const unsigned long *oldnodes __hwloc_attribute_unused,
					       const unsigned long *newnodes __hwloc_attribute_unused)
{
#if (defined __NR_migrate_pages) && (defined HWLOC_HAVE_SYSCALL)
  return syscall(__NR_migrate_pages, pid, maxnode, oldnodes, newnodes);
#else
#warning Couldn't find __NR_migrate_pages syscall number, memory migration won't be supported
  errno = ENOSYS;
  return -1;
#endif
}

#ifndef __NR_move_pages
# ifdef __i386__
#  define __NR_move_pages 317
# elif defined(__x86_64__)
#  define __NR_move_pages 279
# elif defined(__ia64__)
#  define __NR_move_pages 1276
# elif defined(__hppa__)
#  define __NR_move_pages 295
# elif defined(__alpha__)
   /* sys_ni_syscall */
# elif defined(__s390__) || defined(__s390x__)
#  define __NR_move_pages 310
# elif defined(__sparc__)
#  define __NR_move_pages 307
# elif defined(__m68k__)
#  define __NR_move_pages 310
# elif defined(__powerpc__) || defined(__ppc__) || defined(__PPC__) || defined(__powerpc64__) || defined(__ppc64__)
#  define __NR_move_pages 301
# elif defined(__aarch64__)
#  define __NR_move_pages 239
# elif defined(__riscv)
#  define __NR_move_pages 239
# elif defined(__arm__)
#  define __NR_move_pages 344
# elif defined(__cris__)
#  define __NR_migrate_pages 317
# elif defined(__loongarch__)
#  define __NR_move_pages 239
# elif defined(__mips__) && _MIPS_SIM == _ABI64
#  define __NR_move_pages 5267
# elif defined(__mips__) && _MIPS_SIM == _ABIN32
#  define __NR_move_pages 6271
# elif defined(__mips__) && _MIPS_SIM == _ABIO32
#  define __NR_move_pages 4308
# endif
#endif
static __hwloc_inline long hwloc_move_pages(int pid __hwloc_attribute_unused,
					    unsigned long count __hwloc_attribute_unused,
					    void **pages __hwloc_attribute_unused,
					    const int *nodes __hwloc_attribute_unused,
					    int *status __hwloc_attribute_unused,
					    int flags __hwloc_attribute_unused)
{
#if (defined __NR_move_pages) && (defined HWLOC_HAVE_SYSCALL)
  return syscall(__NR_move_pages, pid, count, pages, nodes, status, flags);
#else
#warning Couldn't find __NR_move_pages syscall number, getting memory location won't be supported
  errno = ENOSYS;
  return -1;
#endif
}


/* Added for ntohl() */
#include <arpa/inet.h>

#ifdef HAVE_OPENAT
/* Use our own filesystem functions if we have openat */

static const char *
hwloc_checkat(const char *path, int fsroot_fd)
{
  const char *relative_path = path;

  if (fsroot_fd >= 0)
    /* Skip leading slashes.  */
    for (; *relative_path == '/'; relative_path++);

  return relative_path;
}

static int
hwloc_openat(const char *path, int fsroot_fd)
{
  const char *relative_path;

  relative_path = hwloc_checkat(path, fsroot_fd);
  if (!relative_path)
    return -1;

  return openat (fsroot_fd, relative_path, O_RDONLY);
}

static FILE *
hwloc_fopenat(const char *path, const char *mode, int fsroot_fd)
{
  int fd;

  if (strcmp(mode, "r")) {
    errno = ENOTSUP;
    return NULL;
  }

  fd = hwloc_openat (path, fsroot_fd);
  if (fd == -1)
    return NULL;

  return fdopen(fd, mode);
}

static int
hwloc_accessat(const char *path, int mode, int fsroot_fd)
{
  const char *relative_path;

  relative_path = hwloc_checkat(path, fsroot_fd);
  if (!relative_path)
    return -1;

  return faccessat(fsroot_fd, relative_path, mode, 0);
}

static int
hwloc_fstatat(const char *path, struct stat *st, int flags, int fsroot_fd)
{
  const char *relative_path;

  relative_path = hwloc_checkat(path, fsroot_fd);
  if (!relative_path)
    return -1;

  return fstatat(fsroot_fd, relative_path, st, flags);
}

static DIR*
hwloc_opendirat(const char *path, int fsroot_fd)
{
  int dir_fd;
  const char *relative_path;

  relative_path = hwloc_checkat(path, fsroot_fd);
  if (!relative_path)
    return NULL;

  dir_fd = openat(fsroot_fd, relative_path, O_RDONLY | O_DIRECTORY);
  if (dir_fd < 0)
    return NULL;

  return fdopendir(dir_fd);
}

static int
hwloc_readlinkat(const char *path, char *buf, size_t buflen, int fsroot_fd)
{
  const char *relative_path;

  relative_path = hwloc_checkat(path, fsroot_fd);
  if (!relative_path)
    return -1;

  return readlinkat(fsroot_fd, relative_path, buf, buflen);
}

#endif /* HAVE_OPENAT */

/* Static inline version of fopen so that we can use openat if we have
   it, but still preserve compiler parameter checking */
static __hwloc_inline int
hwloc_open(const char *p, int d __hwloc_attribute_unused)
{
#ifdef HAVE_OPENAT
    return hwloc_openat(p, d);
#else
    return open(p, O_RDONLY);
#endif
}

static __hwloc_inline FILE *
hwloc_fopen(const char *p, const char *m, int d __hwloc_attribute_unused)
{
#ifdef HAVE_OPENAT
    return hwloc_fopenat(p, m, d);
#else
    return fopen(p, m);
#endif
}

/* Static inline version of access so that we can use openat if we have
   it, but still preserve compiler parameter checking */
static __hwloc_inline int
hwloc_access(const char *p, int m, int d __hwloc_attribute_unused)
{
#ifdef HAVE_OPENAT
    return hwloc_accessat(p, m, d);
#else
    return access(p, m);
#endif
}

static __hwloc_inline int
hwloc_stat(const char *p, struct stat *st, int d __hwloc_attribute_unused)
{
#ifdef HAVE_OPENAT
    return hwloc_fstatat(p, st, 0, d);
#else
    return stat(p, st);
#endif
}

/* Static inline version of opendir so that we can use openat if we have
   it, but still preserve compiler parameter checking */
static __hwloc_inline DIR *
hwloc_opendir(const char *p, int d __hwloc_attribute_unused)
{
#ifdef HAVE_OPENAT
    return hwloc_opendirat(p, d);
#else
    return opendir(p);
#endif
}

static __hwloc_inline int
hwloc_readlink(const char *p, char *l, size_t ll, int d __hwloc_attribute_unused)
{
  ssize_t err;
  /* readlink doesn't put the ending \0. read ll-1 and add it. */
#ifdef HAVE_OPENAT
  err = hwloc_readlinkat(p, l, ll-1, d);
#else
  err = readlink(p, l, ll-1);
#endif
  if (err >= 0)
    l[err] = '\0';
  return err;
}


/*****************************************
 ******* Helpers for reading files *******
 *****************************************/

/* Read up to length-1 bytes in path and add an ending \0.
 * Return read bytes (without counting the ending \0), 0 for empty file, or -1 on error.
 */
static __hwloc_inline ssize_t
hwloc_read_path_by_length(const char *path, char *string, size_t length, int fsroot_fd)
{
  int fd;
  ssize_t ret;

  fd = hwloc_open(path, fsroot_fd);
  if (fd < 0)
    return -1;

  ret = read(fd, string, length-1); /* read -1 to put the ending \0 */
  close(fd);

  if (ret <= 0)
    return -1;

  string[ret] = 0;
  return ret;
}

static __hwloc_inline int
hwloc_read_path_as_int(const char *path, int *value, int fsroot_fd)
{
  char string[11];
  if (hwloc_read_path_by_length(path, string, sizeof(string), fsroot_fd) <= 0)
    return -1;
  *value = atoi(string);
  return 0;
}

static __hwloc_inline int
hwloc_read_path_as_uint(const char *path, unsigned *value, int fsroot_fd)
{
  char string[11];
  if (hwloc_read_path_by_length(path, string, sizeof(string), fsroot_fd) <= 0)
    return -1;
  *value = (unsigned) strtoul(string, NULL, 10);
  return 0;
}

static __hwloc_inline int
hwloc_read_path_as_uint64(const char *path, uint64_t *value, int fsroot_fd)
{
  char string[22];
  if (hwloc_read_path_by_length(path, string, sizeof(string), fsroot_fd) <= 0)
    return -1;
  *value = (uint64_t) strtoull(string, NULL, 10);
  return 0;
}

/* Read everything from fd and save it into a newly allocated buffer
 * returned in bufferp. Use sizep as a default buffer size, and return
 * the actually needed size in sizep.
 */
static __hwloc_inline int
hwloc__read_fd(int fd, char **bufferp, size_t *sizep)
{
  char *buffer;
  size_t toread, filesize, totalread;
  ssize_t ret;

  toread = filesize = *sizep;

  /* Alloc and read +1 so that we get EOF on 2^n without reading once more */
  buffer = malloc(filesize+1);
  if (!buffer)
    return -1;

  ret = read(fd, buffer, toread+1);
  if (ret < 0) {
    free(buffer);
    return -1;
  }

  totalread = (size_t) ret;

  if (totalread < toread + 1)
    /* Normal case, a single read got EOF */
    goto done;

  /* Unexpected case, must extend the buffer and read again.
   * Only occurs on first invocation and if the kernel ever uses multiple page for a single mask.
   */
  do {
    char *tmp;

    toread = filesize;
    filesize *= 2;

    tmp = realloc(buffer, filesize+1);
    if (!tmp) {
      free(buffer);
      return -1;
    }
    buffer = tmp;

    ret = read(fd, buffer+toread+1, toread);
    if (ret < 0) {
      free(buffer);
      return -1;
    }

    totalread += ret;
  } while ((size_t) ret == toread);

 done:
  buffer[totalread] = '\0';
  *bufferp = buffer;
  *sizep = filesize;
  return 0;
}

/* kernel cpumaps are composed of an array of 32bits cpumasks */
#define KERNEL_CPU_MASK_BITS 32
#define KERNEL_CPU_MAP_LEN (KERNEL_CPU_MASK_BITS/4+2)

static __hwloc_inline int
hwloc__read_path_as_cpumask(const char *path, hwloc_bitmap_t set, int fsroot_fd)
{
  static size_t _filesize = 0; /* will be dynamically initialized to hwloc_get_pagesize(), and increased later if needed */
  size_t filesize;
  unsigned long *maps;
  unsigned long map;
  int nr_maps = 0;
  static int _nr_maps_allocated = 8; /* Only compute the power-of-two above the kernel cpumask size once.
				      * Actually, it may increase multiple times if first read cpumaps start with zeroes.
				      */
  int nr_maps_allocated = _nr_maps_allocated;
  char *buffer, *tmpbuf;
  int fd, err;
  int i;

  fd = hwloc_open(path, fsroot_fd);
  if (fd < 0)
    goto out;

  /* Kernel sysfs files are usually at most one page. 4kB may contain 455 32-bit
   * masks (followed by comma), enough for 14k PUs. So allocate a page by default for now.
   *
   * If we ever need a larger buffer, we'll realloc() the buffer during the first
   * invocation of this function so that others directly allocate the right size
   * (all cpumask files have the exact same size).
   */
  filesize = _filesize;
  if (!filesize)
    filesize = hwloc_getpagesize();
  err = hwloc__read_fd(fd, &buffer, &filesize);
  close(fd);
  if (err < 0)
    goto out;
  /* Only update the static value with the final one,
   * to avoid sharing intermediate values that we modify,
   * in case there's ever multiple concurrent calls.
   */
  _filesize = filesize;

  maps = malloc(nr_maps_allocated * sizeof(*maps));
  if (!maps)
    goto out_with_buffer;

  /* reset to zero first */
  hwloc_bitmap_zero(set);

  /* parse the whole mask */
  tmpbuf = buffer;
  while (sscanf(tmpbuf, "%lx", &map) == 1) {
    /* read one kernel cpu mask and the ending comma */
    if (nr_maps == nr_maps_allocated) {
      unsigned long *tmp = realloc(maps, 2*nr_maps_allocated * sizeof(*maps));
      if (!tmp)
        goto out_with_maps;
      maps = tmp;
      nr_maps_allocated *= 2;
    }

    tmpbuf = strchr(tmpbuf, ',');
    if (!tmpbuf) {
      maps[nr_maps++] = map;
      break;
    } else
      tmpbuf++;

    if (!map && !nr_maps)
      /* ignore the first map if it's empty */
      continue;

    maps[nr_maps++] = map;
  }

  free(buffer);

  /* convert into a set */
#if KERNEL_CPU_MASK_BITS == HWLOC_BITS_PER_LONG
  for(i=0; i<nr_maps; i++)
    hwloc_bitmap_set_ith_ulong(set, i, maps[nr_maps-1-i]);
#else
  for(i=0; i<(nr_maps+1)/2; i++) {
    unsigned long mask;
    mask = maps[nr_maps-2*i-1];
    if (2*i+1<nr_maps)
      mask |= maps[nr_maps-2*i-2] << KERNEL_CPU_MASK_BITS;
    hwloc_bitmap_set_ith_ulong(set, i, mask);
  }
#endif

  free(maps);

  /* Only update the static value with the final one,
   * to avoid sharing intermediate values that we modify,
   * in case there's ever multiple concurrent calls.
   */
  if (nr_maps_allocated > _nr_maps_allocated)
    _nr_maps_allocated = nr_maps_allocated;
  return 0;

 out_with_maps:
  free(maps);
 out_with_buffer:
  free(buffer);
 out:
  return -1;
}

static __hwloc_inline hwloc_bitmap_t
hwloc__alloc_read_path_as_cpumask(const char *maskpath, int fsroot_fd)
{
  hwloc_bitmap_t set;
  int err;
  set = hwloc_bitmap_alloc();
  if (!set)
    return NULL;
  err = hwloc__read_path_as_cpumask(maskpath, set, fsroot_fd);
  if (err < 0) {
    hwloc_bitmap_free(set);
    return NULL;
  } else
    return set;
}

int
hwloc_linux_read_path_as_cpumask(const char *maskpath, hwloc_bitmap_t set)
{
  return hwloc__read_path_as_cpumask(maskpath, set, -1);
}

/* on failure, the content of set is undefined */
static __hwloc_inline int
hwloc__read_path_as_cpulist(const char *path, hwloc_bitmap_t set, int fsroot_fd)
{
  /* Kernel sysfs files are usually at most one page.
   * But cpulists can be of very different sizes depending on the fragmentation,
   * so don't bother remember the actual read size between invocations.
   * We don't have many invocations anyway.
   */
  size_t filesize = hwloc_getpagesize();
  char *buffer, *current, *comma, *tmp;
  int prevlast, nextfirst, nextlast; /* beginning/end of enabled-segments */
  int fd, err;

  fd = hwloc_open(path, fsroot_fd);
  if (fd < 0)
    return -1;
  err = hwloc__read_fd(fd, &buffer, &filesize);
  close(fd);
  if (err < 0)
    return -1;

  hwloc_bitmap_fill(set);

  current = buffer;
  prevlast = -1;

  while (1) {
    /* save a pointer to the next comma and erase it to simplify things */
    comma = strchr(current, ',');
    if (comma)
      *comma = '\0';

    /* find current enabled-segment bounds */
    nextfirst = strtoul(current, &tmp, 0);
    if (*tmp == '-')
      nextlast = strtoul(tmp+1, NULL, 0);
    else
      nextlast = nextfirst;
    if (prevlast+1 <= nextfirst-1)
      hwloc_bitmap_clr_range(set, prevlast+1, nextfirst-1);

    /* switch to next enabled-segment */
    prevlast = nextlast;
    if (!comma)
      break;
    current = comma+1;
  }

  hwloc_bitmap_clr_range(set, prevlast+1, -1);
  free(buffer);
  return 0;
}

/* on failure, the content of set is undefined */
static __hwloc_inline hwloc_bitmap_t
hwloc__alloc_read_path_as_cpulist(const char *maskpath, int fsroot_fd)
{
  hwloc_bitmap_t set;
  int err;
  set = hwloc_bitmap_alloc_full();
  if (!set)
    return NULL;
  err = hwloc__read_path_as_cpulist(maskpath, set, fsroot_fd);
  if (err < 0) {
    hwloc_bitmap_free(set);
    return NULL;
  } else
    return set;
}


/*****************************
 ******* CpuBind Hooks *******
 *****************************/

int
hwloc_linux_set_tid_cpubind(hwloc_topology_t topology __hwloc_attribute_unused, pid_t tid __hwloc_attribute_unused, hwloc_const_bitmap_t hwloc_set __hwloc_attribute_unused)
{
  /* The resulting binding is always strict */

#if defined(HWLOC_HAVE_CPU_SET_S) && !defined(HWLOC_HAVE_OLD_SCHED_SETAFFINITY)
  cpu_set_t *plinux_set;
  unsigned cpu;
  int last;
  size_t setsize;
  int err;

  last = hwloc_bitmap_last(hwloc_set);
  if (last == -1) {
    errno = EINVAL;
    return -1;
  }

  setsize = CPU_ALLOC_SIZE(last+1);
  plinux_set = CPU_ALLOC(last+1);
  if (!plinux_set)
    return -1;

  CPU_ZERO_S(setsize, plinux_set);
  hwloc_bitmap_foreach_begin(cpu, hwloc_set)
    CPU_SET_S(cpu, setsize, plinux_set);
  hwloc_bitmap_foreach_end();

  err = sched_setaffinity(tid, setsize, plinux_set);

  CPU_FREE(plinux_set);
  return err;
#elif defined(HWLOC_HAVE_CPU_SET)
  cpu_set_t linux_set;
  unsigned cpu;

  CPU_ZERO(&linux_set);
  hwloc_bitmap_foreach_begin(cpu, hwloc_set)
    CPU_SET(cpu, &linux_set);
  hwloc_bitmap_foreach_end();

#ifdef HWLOC_HAVE_OLD_SCHED_SETAFFINITY
  return sched_setaffinity(tid, &linux_set);
#else /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
  return sched_setaffinity(tid, sizeof(linux_set), &linux_set);
#endif /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
#elif defined(HWLOC_HAVE_SYSCALL)
  unsigned long mask = hwloc_bitmap_to_ulong(hwloc_set);

#ifdef HWLOC_HAVE_OLD_SCHED_SETAFFINITY
  return sched_setaffinity(tid, (void*) &mask);
#else /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
  return sched_setaffinity(tid, sizeof(mask), (void*) &mask);
#endif /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
#else /* !SYSCALL */
  errno = ENOSYS;
  return -1;
#endif /* !SYSCALL */
}

#if defined(HWLOC_HAVE_CPU_SET_S) && !defined(HWLOC_HAVE_OLD_SCHED_SETAFFINITY)
/*
 * On some kernels, sched_getaffinity requires the output size to be larger
 * than the kernel cpu_set size (defined by CONFIG_NR_CPUS).
 * Try sched_affinity on ourself until we find a nr_cpus value that makes
 * the kernel happy.
 */
static int
hwloc_linux_find_kernel_nr_cpus(hwloc_topology_t topology)
{
  static int _nr_cpus = -1;
  int nr_cpus = _nr_cpus;
  hwloc_bitmap_t possible_bitmap;

  if (nr_cpus != -1)
    /* already computed */
    return nr_cpus;

  if (topology->levels[0][0]->complete_cpuset)
    /* start with a nr_cpus that may contain the whole topology */
    nr_cpus = hwloc_bitmap_last(topology->levels[0][0]->complete_cpuset) + 1;
  if (nr_cpus <= 0)
    /* start from scratch, the topology isn't ready yet (complete_cpuset is missing (-1) or empty (0))*/
    nr_cpus = 1;

  /* reading /sys/devices/system/cpu/kernel_max would be easier (single value to parse instead of a list),
   * but its value may be way too large (5119 on CentOS7).
   * /sys/devices/system/cpu/possible is better because it matches the current hardware.
   */

  possible_bitmap = hwloc__alloc_read_path_as_cpulist("/sys/devices/system/cpu/possible", -1); /* binding only supported in real fsroot, no need for data->root_fd */
  if (possible_bitmap) {
    int max_possible = hwloc_bitmap_last(possible_bitmap);
    hwloc_debug_bitmap("possible CPUs are %s\n", possible_bitmap);
    if (nr_cpus < max_possible + 1)
      nr_cpus = max_possible + 1;
    hwloc_bitmap_free(possible_bitmap);
  }

  while (1) {
    cpu_set_t *set = CPU_ALLOC(nr_cpus);
    size_t setsize = CPU_ALLOC_SIZE(nr_cpus);
    int err;
    if (!set)
      return -1; /* caller will return an error, and we'll try again later */
    err = sched_getaffinity(0, setsize, set); /* always works, unless setsize is too small */
    CPU_FREE(set);
    nr_cpus = setsize * 8; /* that's the value that was actually tested */
    if (!err)
      /* Found it. Only update the static value with the final one,
       * to avoid sharing intermediate values that we modify,
       * in case there's ever multiple concurrent calls.
       */
      return _nr_cpus = nr_cpus;
    nr_cpus *= 2;
  }
}
#endif

int
hwloc_linux_get_tid_cpubind(hwloc_topology_t topology __hwloc_attribute_unused, pid_t tid __hwloc_attribute_unused, hwloc_bitmap_t hwloc_set __hwloc_attribute_unused)
{
  int err __hwloc_attribute_unused;

#if defined(HWLOC_HAVE_CPU_SET_S) && !defined(HWLOC_HAVE_OLD_SCHED_SETAFFINITY)
  cpu_set_t *plinux_set;
  unsigned cpu;
  int last;
  size_t setsize;
  int kernel_nr_cpus;

  /* find the kernel nr_cpus so as to use a large enough cpu_set size */
  kernel_nr_cpus = hwloc_linux_find_kernel_nr_cpus(topology);
  if (kernel_nr_cpus < 0)
    return -1;
  setsize = CPU_ALLOC_SIZE(kernel_nr_cpus);
  plinux_set = CPU_ALLOC(kernel_nr_cpus);
  if (!plinux_set)
    return -1;

  err = sched_getaffinity(tid, setsize, plinux_set);

  if (err < 0) {
    CPU_FREE(plinux_set);
    return -1;
  }

  last = -1;
  if (topology->levels[0][0]->complete_cpuset)
    last = hwloc_bitmap_last(topology->levels[0][0]->complete_cpuset);
  if (last == -1)
    /* round the maximal support number, the topology isn't ready yet (complete_cpuset is missing or empty)*/
    last = kernel_nr_cpus-1;

  hwloc_bitmap_zero(hwloc_set);
  for(cpu=0; cpu<=(unsigned) last; cpu++)
    if (CPU_ISSET_S(cpu, setsize, plinux_set))
      hwloc_bitmap_set(hwloc_set, cpu);

  CPU_FREE(plinux_set);
#elif defined(HWLOC_HAVE_CPU_SET)
  cpu_set_t linux_set;
  unsigned cpu;

#ifdef HWLOC_HAVE_OLD_SCHED_SETAFFINITY
  err = sched_getaffinity(tid, &linux_set);
#else /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
  err = sched_getaffinity(tid, sizeof(linux_set), &linux_set);
#endif /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
  if (err < 0)
    return -1;

  hwloc_bitmap_zero(hwloc_set);
  for(cpu=0; cpu<CPU_SETSIZE; cpu++)
    if (CPU_ISSET(cpu, &linux_set))
      hwloc_bitmap_set(hwloc_set, cpu);
#elif defined(HWLOC_HAVE_SYSCALL)
  unsigned long mask;

#ifdef HWLOC_HAVE_OLD_SCHED_SETAFFINITY
  err = sched_getaffinity(tid, (void*) &mask);
#else /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
  err = sched_getaffinity(tid, sizeof(mask), (void*) &mask);
#endif /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
  if (err < 0)
    return -1;

  hwloc_bitmap_from_ulong(hwloc_set, mask);
#else /* !SYSCALL */
  errno = ENOSYS;
  return -1;
#endif /* !SYSCALL */

  return 0;
}

/* Get the array of tids of a process from the task directory in /proc */
static int
hwloc_linux_get_proc_tids(DIR *taskdir, unsigned *nr_tidsp, pid_t ** tidsp)
{
  struct dirent *dirent;
  unsigned nr_tids = 0;
  unsigned max_tids = 32;
  pid_t *tids;
  struct stat sb;

  /* take the number of links as a good estimate for the number of tids */
  if (fstat(dirfd(taskdir), &sb) == 0)
    max_tids = sb.st_nlink;

  tids = malloc(max_tids*sizeof(pid_t));
  if (!tids) {
    errno = ENOMEM;
    return -1;
  }

  rewinddir(taskdir);

  while ((dirent = readdir(taskdir)) != NULL) {
    if (nr_tids == max_tids) {
      pid_t *newtids;
      max_tids += 8;
      newtids = realloc(tids, max_tids*sizeof(pid_t));
      if (!newtids) {
        free(tids);
        errno = ENOMEM;
        return -1;
      }
      tids = newtids;
    }
    if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
      continue;
    tids[nr_tids++] = atoi(dirent->d_name);
  }

  *nr_tidsp = nr_tids;
  *tidsp = tids;
  return 0;
}

/* Per-tid callbacks */
typedef int (*hwloc_linux_foreach_proc_tid_cb_t)(hwloc_topology_t topology, pid_t tid, void *data, int idx);

static int
hwloc_linux_foreach_proc_tid(hwloc_topology_t topology,
			     pid_t pid, hwloc_linux_foreach_proc_tid_cb_t cb,
			     void *data)
{
  char taskdir_path[128];
  DIR *taskdir;
  pid_t *tids, *newtids;
  unsigned i, nr, newnr, failed = 0, failed_errno = 0;
  unsigned retrynr = 0;
  int err;

  if (pid)
    snprintf(taskdir_path, sizeof(taskdir_path), "/proc/%u/task", (unsigned) pid);
  else
    snprintf(taskdir_path, sizeof(taskdir_path), "/proc/self/task");

  taskdir = opendir(taskdir_path);
  if (!taskdir) {
    if (errno == ENOENT)
      errno = EINVAL;
    err = -1;
    goto out;
  }

  /* read the current list of threads */
  err = hwloc_linux_get_proc_tids(taskdir, &nr, &tids);
  if (err < 0)
    goto out_with_dir;

 retry:
  /* apply the callback to all threads */
  failed=0;
  for(i=0; i<nr; i++) {
    err = cb(topology, tids[i], data, i);
    if (err < 0) {
      failed++;
      failed_errno = errno;
    }
  }

  /* re-read the list of thread */
  err = hwloc_linux_get_proc_tids(taskdir, &newnr, &newtids);
  if (err < 0)
    goto out_with_tids;
  /* retry if the list changed in the meantime, or we failed for *some* threads only.
   * if we're really unlucky, all threads changed but we got the same set of tids. no way to support this.
   */
  if (newnr != nr || memcmp(newtids, tids, nr*sizeof(pid_t)) || (failed && failed != nr)) {
    free(tids);
    tids = newtids;
    nr = newnr;
    if (++retrynr > 10) {
      /* we tried 10 times, it didn't work, the application is probably creating/destroying many threads, stop trying */
      errno = EAGAIN;
      err = -1;
      goto out_with_tids;
    }
    goto retry;
  } else {
    free(newtids);
  }

  /* if all threads failed, return the last errno. */
  if (failed) {
    err = -1;
    errno = failed_errno;
    goto out_with_tids;
  }

  err = 0;
 out_with_tids:
  free(tids);
 out_with_dir:
  closedir(taskdir);
 out:
  return err;
}

/* Per-tid proc_set_cpubind callback and caller.
 * Callback data is a hwloc_bitmap_t. */
static int
hwloc_linux_foreach_proc_tid_set_cpubind_cb(hwloc_topology_t topology, pid_t tid, void *data, int idx __hwloc_attribute_unused)
{
  return hwloc_linux_set_tid_cpubind(topology, tid, (hwloc_bitmap_t) data);
}

static int
hwloc_linux_set_pid_cpubind(hwloc_topology_t topology, pid_t pid, hwloc_const_bitmap_t hwloc_set, int flags __hwloc_attribute_unused)
{
  return hwloc_linux_foreach_proc_tid(topology, pid,
				      hwloc_linux_foreach_proc_tid_set_cpubind_cb,
				      (void*) hwloc_set);
}

/* Per-tid proc_get_cpubind callback data, callback function and caller */
struct hwloc_linux_foreach_proc_tid_get_cpubind_cb_data_s {
  hwloc_bitmap_t cpuset;
  hwloc_bitmap_t tidset;
  int flags;
};

static int
hwloc_linux_foreach_proc_tid_get_cpubind_cb(hwloc_topology_t topology, pid_t tid, void *_data, int idx)
{
  struct hwloc_linux_foreach_proc_tid_get_cpubind_cb_data_s *data = _data;
  hwloc_bitmap_t cpuset = data->cpuset;
  hwloc_bitmap_t tidset = data->tidset;
  int flags = data->flags;

  if (hwloc_linux_get_tid_cpubind(topology, tid, tidset))
    return -1;

  /* reset the cpuset on first iteration */
  if (!idx)
    hwloc_bitmap_zero(cpuset);

  if (flags & HWLOC_CPUBIND_STRICT) {
    /* if STRICT, we want all threads to have the same binding */
    if (!idx) {
      /* this is the first thread, copy its binding */
      hwloc_bitmap_copy(cpuset, tidset);
    } else if (!hwloc_bitmap_isequal(cpuset, tidset)) {
      /* this is not the first thread, and it's binding is different */
      errno = EXDEV;
      return -1;
    }
  } else {
    /* if not STRICT, just OR all thread bindings */
    hwloc_bitmap_or(cpuset, cpuset, tidset);
  }
  return 0;
}

static int
hwloc_linux_get_pid_cpubind(hwloc_topology_t topology, pid_t pid, hwloc_bitmap_t hwloc_set, int flags)
{
  struct hwloc_linux_foreach_proc_tid_get_cpubind_cb_data_s data;
  hwloc_bitmap_t tidset = hwloc_bitmap_alloc();
  int ret;

  data.cpuset = hwloc_set;
  data.tidset = tidset;
  data.flags = flags;
  ret = hwloc_linux_foreach_proc_tid(topology, pid,
				     hwloc_linux_foreach_proc_tid_get_cpubind_cb,
				     (void*) &data);
  hwloc_bitmap_free(tidset);
  return ret;
}

static int
hwloc_linux_set_proc_cpubind(hwloc_topology_t topology, pid_t pid, hwloc_const_bitmap_t hwloc_set, int flags)
{
  if (pid == 0)
    pid = topology->pid;
  if (flags & HWLOC_CPUBIND_THREAD)
    return hwloc_linux_set_tid_cpubind(topology, pid, hwloc_set);
  else
    return hwloc_linux_set_pid_cpubind(topology, pid, hwloc_set, flags);
}

static int
hwloc_linux_get_proc_cpubind(hwloc_topology_t topology, pid_t pid, hwloc_bitmap_t hwloc_set, int flags)
{
  if (pid == 0)
    pid = topology->pid;
  if (flags & HWLOC_CPUBIND_THREAD)
    return hwloc_linux_get_tid_cpubind(topology, pid, hwloc_set);
  else
    return hwloc_linux_get_pid_cpubind(topology, pid, hwloc_set, flags);
}

static int
hwloc_linux_set_thisproc_cpubind(hwloc_topology_t topology, hwloc_const_bitmap_t hwloc_set, int flags)
{
  return hwloc_linux_set_pid_cpubind(topology, topology->pid, hwloc_set, flags);
}

static int
hwloc_linux_get_thisproc_cpubind(hwloc_topology_t topology, hwloc_bitmap_t hwloc_set, int flags)
{
  return hwloc_linux_get_pid_cpubind(topology, topology->pid, hwloc_set, flags);
}

static int
hwloc_linux_set_thisthread_cpubind(hwloc_topology_t topology, hwloc_const_bitmap_t hwloc_set, int flags __hwloc_attribute_unused)
{
  if (topology->pid) {
    errno = ENOSYS;
    return -1;
  }
  return hwloc_linux_set_tid_cpubind(topology, 0, hwloc_set);
}

static int
hwloc_linux_get_thisthread_cpubind(hwloc_topology_t topology, hwloc_bitmap_t hwloc_set, int flags __hwloc_attribute_unused)
{
  if (topology->pid) {
    errno = ENOSYS;
    return -1;
  }
  return hwloc_linux_get_tid_cpubind(topology, 0, hwloc_set);
}

#if HAVE_DECL_PTHREAD_SETAFFINITY_NP
#pragma weak pthread_setaffinity_np
#pragma weak pthread_self

static int
hwloc_linux_set_thread_cpubind(hwloc_topology_t topology, pthread_t tid, hwloc_const_bitmap_t hwloc_set, int flags __hwloc_attribute_unused)
{
  int err;

  if (topology->pid) {
    errno = ENOSYS;
    return -1;
  }

  if (!pthread_self) {
    /* ?! Application uses set_thread_cpubind, but doesn't link against libpthread ?! */
    errno = ENOSYS;
    return -1;
  }
  if (tid == pthread_self())
    return hwloc_linux_set_tid_cpubind(topology, 0, hwloc_set);

  if (!pthread_setaffinity_np) {
    errno = ENOSYS;
    return -1;
  }

#if defined(HWLOC_HAVE_CPU_SET_S) && !defined(HWLOC_HAVE_OLD_SCHED_SETAFFINITY)
  /* Use a separate block so that we can define specific variable
     types here */
  {
     cpu_set_t *plinux_set;
     unsigned cpu;
     int last;
     size_t setsize;

     last = hwloc_bitmap_last(hwloc_set);
     if (last == -1) {
       errno = EINVAL;
       return -1;
     }

     setsize = CPU_ALLOC_SIZE(last+1);
     plinux_set = CPU_ALLOC(last+1);
     if (!plinux_set)
       return -1;

     CPU_ZERO_S(setsize, plinux_set);
     hwloc_bitmap_foreach_begin(cpu, hwloc_set)
         CPU_SET_S(cpu, setsize, plinux_set);
     hwloc_bitmap_foreach_end();

     err = pthread_setaffinity_np(tid, setsize, plinux_set);

     CPU_FREE(plinux_set);
  }
#elif defined(HWLOC_HAVE_CPU_SET)
  /* Use a separate block so that we can define specific variable
     types here */
  {
     cpu_set_t linux_set;
     unsigned cpu;

     CPU_ZERO(&linux_set);
     hwloc_bitmap_foreach_begin(cpu, hwloc_set)
         CPU_SET(cpu, &linux_set);
     hwloc_bitmap_foreach_end();

#ifdef HWLOC_HAVE_OLD_SCHED_SETAFFINITY
     err = pthread_setaffinity_np(tid, &linux_set);
#else /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
     err = pthread_setaffinity_np(tid, sizeof(linux_set), &linux_set);
#endif /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
  }
#else /* CPU_SET */
  /* Use a separate block so that we can define specific variable
     types here */
  {
      unsigned long mask = hwloc_bitmap_to_ulong(hwloc_set);

#ifdef HWLOC_HAVE_OLD_SCHED_SETAFFINITY
      err = pthread_setaffinity_np(tid, (void*) &mask);
#else /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
      err = pthread_setaffinity_np(tid, sizeof(mask), (void*) &mask);
#endif /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
  }
#endif /* CPU_SET */

  if (err) {
    errno = err;
    return -1;
  }
  return 0;
}
#endif /* HAVE_DECL_PTHREAD_SETAFFINITY_NP */

#if HAVE_DECL_PTHREAD_GETAFFINITY_NP
#pragma weak pthread_getaffinity_np
#pragma weak pthread_self

static int
hwloc_linux_get_thread_cpubind(hwloc_topology_t topology, pthread_t tid, hwloc_bitmap_t hwloc_set, int flags __hwloc_attribute_unused)
{
  int err;

  if (topology->pid) {
    errno = ENOSYS;
    return -1;
  }

  if (!pthread_self) {
    /* ?! Application uses set_thread_cpubind, but doesn't link against libpthread ?! */
    errno = ENOSYS;
    return -1;
  }
  if (tid == pthread_self())
    return hwloc_linux_get_tid_cpubind(topology, 0, hwloc_set);

  if (!pthread_getaffinity_np) {
    errno = ENOSYS;
    return -1;
  }

#if defined(HWLOC_HAVE_CPU_SET_S) && !defined(HWLOC_HAVE_OLD_SCHED_SETAFFINITY)
  /* Use a separate block so that we can define specific variable
     types here */
  {
     cpu_set_t *plinux_set;
     unsigned cpu;
     int last;
     size_t setsize;

     last = hwloc_bitmap_last(topology->levels[0][0]->complete_cpuset);
     assert (last != -1);

     setsize = CPU_ALLOC_SIZE(last+1);
     plinux_set = CPU_ALLOC(last+1);
     if (!plinux_set)
       return -1;

     err = pthread_getaffinity_np(tid, setsize, plinux_set);
     if (err) {
        CPU_FREE(plinux_set);
        errno = err;
        return -1;
     }

     hwloc_bitmap_zero(hwloc_set);
     for(cpu=0; cpu<=(unsigned) last; cpu++)
       if (CPU_ISSET_S(cpu, setsize, plinux_set))
	 hwloc_bitmap_set(hwloc_set, cpu);

     CPU_FREE(plinux_set);
  }
#elif defined(HWLOC_HAVE_CPU_SET)
  /* Use a separate block so that we can define specific variable
     types here */
  {
     cpu_set_t linux_set;
     unsigned cpu;

#ifdef HWLOC_HAVE_OLD_SCHED_SETAFFINITY
     err = pthread_getaffinity_np(tid, &linux_set);
#else /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
     err = pthread_getaffinity_np(tid, sizeof(linux_set), &linux_set);
#endif /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
     if (err) {
        errno = err;
        return -1;
     }

     hwloc_bitmap_zero(hwloc_set);
     for(cpu=0; cpu<CPU_SETSIZE; cpu++)
       if (CPU_ISSET(cpu, &linux_set))
	 hwloc_bitmap_set(hwloc_set, cpu);
  }
#else /* CPU_SET */
  /* Use a separate block so that we can define specific variable
     types here */
  {
      unsigned long mask;

#ifdef HWLOC_HAVE_OLD_SCHED_SETAFFINITY
      err = pthread_getaffinity_np(tid, (void*) &mask);
#else /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
      err = pthread_getaffinity_np(tid, sizeof(mask), (void*) &mask);
#endif /* HWLOC_HAVE_OLD_SCHED_SETAFFINITY */
      if (err) {
        errno = err;
        return -1;
      }

     hwloc_bitmap_from_ulong(hwloc_set, mask);
  }
#endif /* CPU_SET */

  return 0;
}
#endif /* HAVE_DECL_PTHREAD_GETAFFINITY_NP */

int
hwloc_linux_get_tid_last_cpu_location(hwloc_topology_t topology __hwloc_attribute_unused, pid_t tid, hwloc_bitmap_t set)
{
  /* read /proc/pid/stat.
   * its second field contains the command name between parentheses,
   * and the command itself may contain parentheses,
   * so read the whole line and find the last closing parenthesis to find the third field.
   */
  char buf[1024] = "";
  char name[64];
  char *tmp;
  int i, err;

  /* TODO: find a way to use sched_getcpu().
   * either compare tid with gettid() in all callbacks.
   * or pass gettid() in the callback data.
   */

  if (!tid) {
#ifdef SYS_gettid
    tid = syscall(SYS_gettid);
#else
    errno = ENOSYS;
    return -1;
#endif
  }

  snprintf(name, sizeof(name), "/proc/%lu/stat", (unsigned long) tid);
  err = hwloc_read_path_by_length(name, buf, sizeof(buf), -1); /* no fsroot for real /proc */
  if (err <= 0) {
    errno = ENOSYS;
    return -1;
  }

  tmp = strrchr(buf, ')');
  if (!tmp) {
    errno = ENOSYS;
    return -1;
  }
  /* skip ') ' to find the actual third argument */
  tmp += 2;

  /* skip 35 fields */
  for(i=0; i<36; i++) {
    tmp = strchr(tmp, ' ');
    if (!tmp) {
      errno = ENOSYS;
      return -1;
    }
    /* skip the ' ' itself */
    tmp++;
  }

  /* read the last cpu in the 38th field now */
  if (sscanf(tmp, "%d ", &i) != 1) {
    errno = ENOSYS;
    return -1;
  }

  hwloc_bitmap_only(set, i);
  return 0;
}

/* Per-tid proc_get_last_cpu_location callback data, callback function and caller */
struct hwloc_linux_foreach_proc_tid_get_last_cpu_location_cb_data_s {
  hwloc_bitmap_t cpuset;
  hwloc_bitmap_t tidset;
};

static int
hwloc_linux_foreach_proc_tid_get_last_cpu_location_cb(hwloc_topology_t topology, pid_t tid, void *_data, int idx)
{
  struct hwloc_linux_foreach_proc_tid_get_last_cpu_location_cb_data_s *data = _data;
  hwloc_bitmap_t cpuset = data->cpuset;
  hwloc_bitmap_t tidset = data->tidset;

  if (hwloc_linux_get_tid_last_cpu_location(topology, tid, tidset))
    return -1;

  /* reset the cpuset on first iteration */
  if (!idx)
    hwloc_bitmap_zero(cpuset);

  hwloc_bitmap_or(cpuset, cpuset, tidset);
  return 0;
}

static int
hwloc_linux_get_pid_last_cpu_location(hwloc_topology_t topology, pid_t pid, hwloc_bitmap_t hwloc_set, int flags __hwloc_attribute_unused)
{
  struct hwloc_linux_foreach_proc_tid_get_last_cpu_location_cb_data_s data;
  hwloc_bitmap_t tidset = hwloc_bitmap_alloc();
  int ret;

  data.cpuset = hwloc_set;
  data.tidset = tidset;
  ret = hwloc_linux_foreach_proc_tid(topology, pid,
				     hwloc_linux_foreach_proc_tid_get_last_cpu_location_cb,
				     &data);
  hwloc_bitmap_free(tidset);
  return ret;
}

static int
hwloc_linux_get_proc_last_cpu_location(hwloc_topology_t topology, pid_t pid, hwloc_bitmap_t hwloc_set, int flags)
{
  if (pid == 0)
    pid = topology->pid;
  if (flags & HWLOC_CPUBIND_THREAD)
    return hwloc_linux_get_tid_last_cpu_location(topology, pid, hwloc_set);
  else
    return hwloc_linux_get_pid_last_cpu_location(topology, pid, hwloc_set, flags);
}

static int
hwloc_linux_get_thisproc_last_cpu_location(hwloc_topology_t topology, hwloc_bitmap_t hwloc_set, int flags)
{
  return hwloc_linux_get_pid_last_cpu_location(topology, topology->pid, hwloc_set, flags);
}

static int
hwloc_linux_get_thisthread_last_cpu_location(hwloc_topology_t topology, hwloc_bitmap_t hwloc_set, int flags __hwloc_attribute_unused)
{
  if (topology->pid) {
    errno = ENOSYS;
    return -1;
  }

#if HAVE_DECL_SCHED_GETCPU
  {
    int pu = sched_getcpu();
    if (pu >= 0) {
      hwloc_bitmap_only(hwloc_set, pu);
      return 0;
    }
  }
#endif

  return hwloc_linux_get_tid_last_cpu_location(topology, 0, hwloc_set);
}



/***************************
 ****** Membind hooks ******
 ***************************/

static int
hwloc_linux_membind_policy_from_hwloc(int *linuxpolicy, hwloc_membind_policy_t policy, int flags)
{
  switch (policy) {
  case HWLOC_MEMBIND_DEFAULT:
    *linuxpolicy = MPOL_DEFAULT;
    break;
  case HWLOC_MEMBIND_FIRSTTOUCH:
    *linuxpolicy = MPOL_LOCAL;
    break;
  case HWLOC_MEMBIND_BIND:
    if (flags & HWLOC_MEMBIND_STRICT)
      *linuxpolicy = MPOL_BIND;
    else
      *linuxpolicy = MPOL_PREFERRED_MANY; /* will be converted to MPOL_PREFERRED by the caller if not supported */
    break;
  case HWLOC_MEMBIND_INTERLEAVE:
    *linuxpolicy = MPOL_INTERLEAVE;
    break;
  case HWLOC_MEMBIND_WEIGHTED_INTERLEAVE:
    *linuxpolicy = MPOL_WEIGHTED_INTERLEAVE;
    break;
  /* TODO: next-touch when (if?) patch applied upstream */
  default:
    errno = ENOSYS;
    return -1;
  }
  return 0;
}

static int
hwloc_linux_membind_mask_from_nodeset(hwloc_topology_t topology __hwloc_attribute_unused,
				      hwloc_const_nodeset_t nodeset,
				      unsigned *max_os_index_p, unsigned long **linuxmaskp)
{
  unsigned max_os_index = 0; /* highest os_index + 1 */
  unsigned long *linuxmask;
  unsigned i;
  hwloc_nodeset_t linux_nodeset = NULL;

  if (hwloc_bitmap_isfull(nodeset)) {
    linux_nodeset = hwloc_bitmap_alloc();
    hwloc_bitmap_only(linux_nodeset, 0);
    nodeset = linux_nodeset;
  }

  max_os_index = hwloc_bitmap_last(nodeset);
  if (max_os_index == (unsigned) -1)
    max_os_index = 0;
  /* add 1 to convert the last os_index into a max_os_index,
   * and round up to the nearest multiple of BITS_PER_LONG */
  max_os_index = (max_os_index + 1 + HWLOC_BITS_PER_LONG - 1) & ~(HWLOC_BITS_PER_LONG - 1);

  linuxmask = calloc(max_os_index/HWLOC_BITS_PER_LONG, sizeof(unsigned long));
  if (!linuxmask) {
    hwloc_bitmap_free(linux_nodeset);
    errno = ENOMEM;
    return -1;
  }

  for(i=0; i<max_os_index/HWLOC_BITS_PER_LONG; i++)
    linuxmask[i] = hwloc_bitmap_to_ith_ulong(nodeset, i);

  if (linux_nodeset)
    hwloc_bitmap_free(linux_nodeset);

  *max_os_index_p = max_os_index;
  *linuxmaskp = linuxmask;
  return 0;
}

static void
hwloc_linux_membind_mask_to_nodeset(hwloc_topology_t topology __hwloc_attribute_unused,
				    hwloc_nodeset_t nodeset,
				    unsigned max_os_index, const unsigned long *linuxmask)
{
  unsigned i;

#ifdef HWLOC_DEBUG
  /* max_os_index comes from hwloc_linux_find_kernel_max_numnodes() so it's a multiple of HWLOC_BITS_PER_LONG */
  assert(!(max_os_index%HWLOC_BITS_PER_LONG));
#endif

  hwloc_bitmap_zero(nodeset);
  for(i=0; i<max_os_index/HWLOC_BITS_PER_LONG; i++)
    hwloc_bitmap_set_ith_ulong(nodeset, i, linuxmask[i]);
}

static __hwloc_inline void
warn_preferred_many_fallback(hwloc_const_bitmap_t nodeset)
{
  static int warned = 0;
  static int checked_binding_error_env = 0;
  static int warn_binding_errors = 0;
  if (!checked_binding_error_env) {
    char *env = getenv("HWLOC_SHOW_ERRORS");
    if (env) {
      if (strstr(env, "bind"))
        warn_binding_errors = 1;
    }
    checked_binding_error_env = 1;
  }
  if (!warned && (warn_binding_errors || HWLOC_SHOW_ALL_ERRORS()) && hwloc_bitmap_weight(nodeset) > 1) {
    fprintf(stderr, "[hwloc/membind] MPOL_PREFERRED_MANY not supported by the kernel.\n");
    fprintf(stderr, "If *all* given nodes must be used, use strict binding or the interleave policy.\n");
    fprintf(stderr, "Otherwise the old MPOL_PREFERRED will only use the first given node.\n");
    warned = 1;
  }
}

static int
hwloc_linux_set_area_membind(hwloc_topology_t topology, const void *addr, size_t len, hwloc_const_nodeset_t nodeset, hwloc_membind_policy_t policy, int flags)
{
  unsigned max_os_index; /* highest os_index + 1 */
  unsigned long *linuxmask;
  size_t remainder;
  static int preferred_many_notsupported = -1; /* -1 = MPOL_PREFERRED not tested, 0 = ok, 1 = not supported */
  int linuxpolicy;
  unsigned linuxflags = 0;
  int err;

  remainder = (uintptr_t) addr & (hwloc_getpagesize()-1);
  addr = (char*) addr - remainder;
  len += remainder;

  err = hwloc_linux_membind_policy_from_hwloc(&linuxpolicy, policy, flags);
  if (err < 0)
    return err;

  if (preferred_many_notsupported == 1 && linuxpolicy == MPOL_PREFERRED_MANY)
    linuxpolicy = MPOL_PREFERRED;

  if (linuxpolicy == MPOL_DEFAULT) {
    /* Some Linux kernels don't like being passed a set */
    return hwloc_mbind((void *) addr, len, linuxpolicy, NULL, 0, 0);

  } else if (linuxpolicy == MPOL_LOCAL) {
    if (!hwloc_bitmap_isequal(nodeset, hwloc_topology_get_complete_nodeset(topology))) {
      errno = EXDEV;
      return -1;
    }
    /* MPOL_LOCAL isn't supported before 3.8, and it's identical to PREFERRED with no nodeset, which was supported way before */
    return hwloc_mbind((void *) addr, len, MPOL_PREFERRED, NULL, 0, 0);
  }

  err = hwloc_linux_membind_mask_from_nodeset(topology, nodeset, &max_os_index, &linuxmask);
  if (err < 0)
    goto out;

  if (flags & HWLOC_MEMBIND_MIGRATE) {
    linuxflags = MPOL_MF_MOVE;
    if (flags & HWLOC_MEMBIND_STRICT)
      linuxflags |= MPOL_MF_STRICT;
  }

  err = hwloc_mbind((void *) addr, len, linuxpolicy, linuxmask, max_os_index+1, linuxflags);

  if (linuxpolicy == MPOL_PREFERRED_MANY && preferred_many_notsupported == -1) {
    if (!err) {
      /* MPOL_PREFERRED_MANY is supported */
      preferred_many_notsupported = 0;
    } else if (errno == EINVAL) {
      /* failed, try with MPOL_PREFERRED */
      err = hwloc_mbind((void *) addr, len, MPOL_PREFERRED, linuxmask, max_os_index+1, linuxflags);
      if (!err) {
        /* worked fine, MPOL_PREFERRED_MANY isn't supported */
        warn_preferred_many_fallback(nodeset);
        preferred_many_notsupported = 1;
      }
    }
  }

  if (err < 0)
    goto out_with_mask;

  free(linuxmask);
  return 0;

 out_with_mask:
  free(linuxmask);
 out:
  return -1;
}

static void *
hwloc_linux_alloc_membind(hwloc_topology_t topology, size_t len, hwloc_const_nodeset_t nodeset, hwloc_membind_policy_t policy, int flags)
{
  void *buffer;
  int err;

  buffer = hwloc_alloc_mmap(topology, len);
  if (!buffer)
    return NULL;

  err = hwloc_linux_set_area_membind(topology, buffer, len, nodeset, policy, flags);
  if (err < 0 && (flags & HWLOC_MEMBIND_STRICT)) {
    munmap(buffer, len);
    return NULL;
  }

  return buffer;
}

static int
hwloc_linux_set_thisthread_membind(hwloc_topology_t topology, hwloc_const_nodeset_t nodeset, hwloc_membind_policy_t policy, int flags)
{
  unsigned max_os_index; /* highest os_index + 1 */
  unsigned long *linuxmask;
  static int preferred_many_notsupported = -1; /* -1 = MPOL_PREFERRED not tested, 0 = ok, 1 = not supported */
  int linuxpolicy;
  int err;

  err = hwloc_linux_membind_policy_from_hwloc(&linuxpolicy, policy, flags);
  if (err < 0)
    return err;

  if (preferred_many_notsupported == 1 && linuxpolicy == MPOL_PREFERRED_MANY)
    linuxpolicy = MPOL_PREFERRED;

  if (linuxpolicy == MPOL_DEFAULT) {
    /* Some Linux kernels don't like being passed a set */
    return hwloc_set_mempolicy(linuxpolicy, NULL, 0);

  } else if (linuxpolicy == MPOL_LOCAL) {
    if (!hwloc_bitmap_isequal(nodeset, hwloc_topology_get_complete_nodeset(topology))) {
      errno = EXDEV;
      return -1;
    }
    /* MPOL_LOCAL isn't supported before 3.8, and it's identical to PREFERRED with no nodeset, which was supported way before */
    return hwloc_set_mempolicy(MPOL_PREFERRED, NULL, 0);
  }

  err = hwloc_linux_membind_mask_from_nodeset(topology, nodeset, &max_os_index, &linuxmask);
  if (err < 0)
    goto out;

  if (flags & HWLOC_MEMBIND_MIGRATE) {
    unsigned long *fullmask;
    fullmask = malloc(max_os_index/HWLOC_BITS_PER_LONG * sizeof(*fullmask));
    if (!fullmask)
      goto out_with_mask;
    memset(fullmask, 0xf, max_os_index/HWLOC_BITS_PER_LONG * sizeof(unsigned long));
    err = hwloc_migrate_pages(0, max_os_index+1, fullmask, linuxmask); /* returns the (positive) number of non-migrated pages on success */
    free(fullmask);
    if (err < 0 && (flags & HWLOC_MEMBIND_STRICT))
      goto out_with_mask;
  }

  err = hwloc_set_mempolicy(linuxpolicy, linuxmask, max_os_index+1);

  if (linuxpolicy == MPOL_PREFERRED_MANY && preferred_many_notsupported == -1) {
    if (!err) {
      /* MPOL_PREFERRED_MANY is supported */
      preferred_many_notsupported = 0;
    } else if (errno == EINVAL) {
      /* failed, try with MPOL_PREFERRED */
      err = hwloc_set_mempolicy(MPOL_PREFERRED, linuxmask, max_os_index+1);
      if (!err) {
        /* worked fine, MPOL_PREFERRED_MANY isn't supported */
        warn_preferred_many_fallback(nodeset);
        preferred_many_notsupported = 1;
      }
    }
  }

  if (err < 0)
    goto out_with_mask;

  free(linuxmask);
  return 0;

 out_with_mask:
  free(linuxmask);
 out:
  return -1;
}

/*
 * On some kernels, get_mempolicy requires the output size to be larger
 * than the kernel MAX_NUMNODES (defined by CONFIG_NODES_SHIFT).
 * Try get_mempolicy on ourself until we find a max_os_index value that
 * makes the kernel happy.
 */
static int
hwloc_linux_find_kernel_max_numnodes(hwloc_topology_t topology __hwloc_attribute_unused)
{
  static int _max_numnodes = -1, max_numnodes;
  int linuxpolicy;
  hwloc_bitmap_t possible_bitmap;

  if (_max_numnodes != -1)
    /* already computed */
    return _max_numnodes;

  /* start with a single ulong, it's the minimal and it's enough for most machines */
  max_numnodes = HWLOC_BITS_PER_LONG;

  /* try to get the max from sysfs */
  possible_bitmap = hwloc__alloc_read_path_as_cpulist("/sys/devices/system/node/possible", -1); /* binding only supported in real fsroot, no need for data->root_fd */
  if (possible_bitmap) {
    int max_possible = hwloc_bitmap_last(possible_bitmap);
    hwloc_debug_bitmap("possible NUMA nodes are %s\n", possible_bitmap);
    if (max_numnodes < max_possible + 1)
      max_numnodes = max_possible + 1;
    hwloc_bitmap_free(possible_bitmap);
  }

  while (1) {
    unsigned long *mask;
    int err;
    mask = malloc(max_numnodes / HWLOC_BITS_PER_LONG * sizeof(*mask));
    if (!mask)
      /* we can't return anything sane, assume the default size will work */
      return _max_numnodes = max_numnodes;

    err = hwloc_get_mempolicy(&linuxpolicy, mask, max_numnodes, 0, 0);
    free(mask);
    if (!err || errno != EINVAL)
      /* Found it. Only update the static value with the final one,
       * to avoid sharing intermediate values that we modify,
       * in case there's ever multiple concurrent calls.
       */
      return _max_numnodes = max_numnodes;
    max_numnodes *= 2;
  }
}

static int
hwloc_linux_membind_policy_to_hwloc(int linuxpolicy, hwloc_membind_policy_t *policy)
{
  switch (linuxpolicy) {
  case MPOL_DEFAULT:
  case MPOL_LOCAL: /* converted from MPOL_PREFERRED + empty nodeset by the caller */
    *policy = HWLOC_MEMBIND_FIRSTTOUCH;
    return 0;
  case MPOL_PREFERRED:
  case MPOL_PREFERRED_MANY:
  case MPOL_BIND:
    *policy = HWLOC_MEMBIND_BIND;
    return 0;
  case MPOL_INTERLEAVE:
    *policy = HWLOC_MEMBIND_INTERLEAVE;
    return 0;
  case MPOL_WEIGHTED_INTERLEAVE:
    *policy = HWLOC_MEMBIND_WEIGHTED_INTERLEAVE;
    return 0;
  default:
    errno = EINVAL;
    return -1;
  }
}

static int hwloc_linux_mask_is_empty(unsigned max_os_index, unsigned long *linuxmask)
{
  unsigned i;
  for(i=0; i<max_os_index/HWLOC_BITS_PER_LONG; i++)
    if (linuxmask[i])
      return 0;
  return 1;
}

static int
hwloc_linux_get_thisthread_membind(hwloc_topology_t topology, hwloc_nodeset_t nodeset, hwloc_membind_policy_t *policy, int flags __hwloc_attribute_unused)
{
  unsigned max_os_index;
  unsigned long *linuxmask;
  int linuxpolicy;
  int err;

  max_os_index = hwloc_linux_find_kernel_max_numnodes(topology);

  linuxmask = malloc(max_os_index/HWLOC_BITS_PER_LONG * sizeof(*linuxmask));;
  if (!linuxmask)
    goto out;

  err = hwloc_get_mempolicy(&linuxpolicy, linuxmask, max_os_index, 0, 0);
  if (err < 0)
    goto out_with_linuxmask;

  /* MPOL_PREFERRED with empty mask is MPOL_LOCAL */
  if (linuxpolicy == MPOL_PREFERRED && hwloc_linux_mask_is_empty(max_os_index, linuxmask))
    linuxpolicy = MPOL_LOCAL;

  if (linuxpolicy == MPOL_DEFAULT || linuxpolicy == MPOL_LOCAL) {
    hwloc_bitmap_copy(nodeset, hwloc_topology_get_topology_nodeset(topology));
  } else {
    hwloc_linux_membind_mask_to_nodeset(topology, nodeset, max_os_index, linuxmask);
  }

  err = hwloc_linux_membind_policy_to_hwloc(linuxpolicy, policy);
  if (err < 0)
    goto out_with_linuxmask;

  free(linuxmask);
  return 0;

 out_with_linuxmask:
  free(linuxmask);
 out:
  return -1;
}

static int
hwloc_linux_get_area_membind(hwloc_topology_t topology, const void *addr, size_t len, hwloc_nodeset_t nodeset, hwloc_membind_policy_t *policy, int flags __hwloc_attribute_unused)
{
  unsigned max_os_index;
  unsigned long *linuxmask;
  unsigned long *globallinuxmask;
  int linuxpolicy = 0, globallinuxpolicy = 0; /* shut-up the compiler */
  int mixed = 0;
  int full = 0;
  int first = 1;
  int pagesize = hwloc_getpagesize();
  char *tmpaddr;
  int err;
  unsigned i;

  max_os_index = hwloc_linux_find_kernel_max_numnodes(topology);

  linuxmask = malloc(max_os_index/HWLOC_BITS_PER_LONG * sizeof(*linuxmask));
  globallinuxmask = malloc(max_os_index/HWLOC_BITS_PER_LONG * sizeof(*globallinuxmask));
  if (!linuxmask || !globallinuxmask)
    goto out_with_linuxmasks;

  memset(globallinuxmask, 0, sizeof(*globallinuxmask));

  for(tmpaddr = (char *)((unsigned long)addr & ~(pagesize-1));
      tmpaddr < (char *)addr + len;
      tmpaddr += pagesize) {
    err = hwloc_get_mempolicy(&linuxpolicy, linuxmask, max_os_index, tmpaddr, MPOL_F_ADDR);
    if (err < 0)
      goto out_with_linuxmasks;

    /* MPOL_PREFERRED with empty mask is MPOL_LOCAL */
    if (linuxpolicy == MPOL_PREFERRED && hwloc_linux_mask_is_empty(max_os_index, linuxmask))
      linuxpolicy = MPOL_LOCAL;

    /* use the first found policy. if we find a different one later, set mixed to 1 */
    if (first)
      globallinuxpolicy = linuxpolicy;
    else if (globallinuxpolicy != linuxpolicy)
      mixed = 1;

    /* agregate masks, and set full to 1 if we ever find DEFAULT or LOCAL */
    if (full || linuxpolicy == MPOL_DEFAULT || linuxpolicy == MPOL_LOCAL) {
      full = 1;
    } else {
      for(i=0; i<max_os_index/HWLOC_BITS_PER_LONG; i++)
        globallinuxmask[i] |= linuxmask[i];
    }

    first = 0;
  }

  if (mixed) {
    *policy = HWLOC_MEMBIND_MIXED;
  } else {
    err = hwloc_linux_membind_policy_to_hwloc(linuxpolicy, policy);
    if (err < 0)
      goto out_with_linuxmasks;
  }

  if (full) {
    hwloc_bitmap_copy(nodeset, hwloc_topology_get_topology_nodeset(topology));
  } else {
    hwloc_linux_membind_mask_to_nodeset(topology, nodeset, max_os_index, globallinuxmask);
  }

  free(linuxmask);
  free(globallinuxmask);
  return 0;

 out_with_linuxmasks:
  free(linuxmask);
  free(globallinuxmask);
  return -1;
}

static int
hwloc_linux_get_area_memlocation(hwloc_topology_t topology __hwloc_attribute_unused, const void *addr, size_t len, hwloc_nodeset_t nodeset, int flags __hwloc_attribute_unused)
{
  unsigned offset;
  unsigned long count;
  void **pages;
  int *status;
  int pagesize = hwloc_getpagesize();
  int ret;
  unsigned i;

  offset = ((unsigned long) addr) & (pagesize-1);
  addr = ((char*) addr) - offset;
  len += offset;
  count = (len + pagesize-1)/pagesize;
  pages = malloc(count*sizeof(*pages));
  status = malloc(count*sizeof(*status));
  if (!pages || !status) {
    ret = -1;
    goto out_with_pages;
  }

  for(i=0; i<count; i++)
    pages[i] = ((char*)addr) + i*pagesize;

  ret = hwloc_move_pages(0, count, pages, NULL, status, 0);
  if (ret  < 0)
    goto out_with_pages;

  hwloc_bitmap_zero(nodeset);
  for(i=0; i<count; i++)
    if (status[i] >= 0)
      hwloc_bitmap_set(nodeset, status[i]);
  ret = 0; /* not really useful since move_pages never returns > 0 */

 out_with_pages:
  free(pages);
  free(status);
  return ret;
}

static void hwloc_linux__get_allowed_resources(hwloc_topology_t topology, const char *root_path, int root_fd, char **cpuset_namep);

static int hwloc_linux_get_allowed_resources_hook(hwloc_topology_t topology)
{
  const char *fsroot_path;
  char *cpuset_name = NULL;
  int root_fd = -1;

  fsroot_path = getenv("HWLOC_FSROOT");
  if (!fsroot_path)
    fsroot_path = "/";

  if (strcmp(fsroot_path, "/")) {
#ifdef HAVE_OPENAT
    root_fd = open(fsroot_path, O_RDONLY | O_DIRECTORY);
    if (root_fd < 0)
      goto out;
#else
    errno = ENOSYS;
    goto out;
#endif
  }

  /* we could also error-out if the current topology doesn't actually match the system,
   * at least for PUs and NUMA nodes. But it would increase the overhead of loading XMLs.
   *
   * Just trust the user when he sets THISSYSTEM=1. It enables hacky
   * tests such as restricting random XML or synthetic to the current
   * machine (uses the default cgroup).
   */

  hwloc_linux__get_allowed_resources(topology, fsroot_path, root_fd, &cpuset_name);
  if (cpuset_name) {
    hwloc__add_info_nodup(&topology->levels[0][0]->infos, &topology->levels[0][0]->infos_count,
			  "LinuxCgroup", cpuset_name, 1 /* replace */);
    free(cpuset_name);
  }
  if (root_fd != -1)
    close(root_fd);

 out:
  return -1;
}

void
hwloc_set_linuxfs_hooks(struct hwloc_binding_hooks *hooks,
			struct hwloc_topology_support *support)
{
  hooks->set_thisthread_cpubind = hwloc_linux_set_thisthread_cpubind;
  hooks->get_thisthread_cpubind = hwloc_linux_get_thisthread_cpubind;
  hooks->set_thisproc_cpubind = hwloc_linux_set_thisproc_cpubind;
  hooks->get_thisproc_cpubind = hwloc_linux_get_thisproc_cpubind;
  hooks->set_proc_cpubind = hwloc_linux_set_proc_cpubind;
  hooks->get_proc_cpubind = hwloc_linux_get_proc_cpubind;
#if HAVE_DECL_PTHREAD_SETAFFINITY_NP
  hooks->set_thread_cpubind = hwloc_linux_set_thread_cpubind;
#endif /* HAVE_DECL_PTHREAD_SETAFFINITY_NP */
#if HAVE_DECL_PTHREAD_GETAFFINITY_NP
  hooks->get_thread_cpubind = hwloc_linux_get_thread_cpubind;
#endif /* HAVE_DECL_PTHREAD_GETAFFINITY_NP */
  hooks->get_thisthread_last_cpu_location = hwloc_linux_get_thisthread_last_cpu_location;
  hooks->get_thisproc_last_cpu_location = hwloc_linux_get_thisproc_last_cpu_location;
  hooks->get_proc_last_cpu_location = hwloc_linux_get_proc_last_cpu_location;
#ifndef ANDROID /* get_mempolicy crashes on some Android */
  hooks->set_thisthread_membind = hwloc_linux_set_thisthread_membind;
  hooks->get_thisthread_membind = hwloc_linux_get_thisthread_membind;
  hooks->get_area_membind = hwloc_linux_get_area_membind;
  hooks->set_area_membind = hwloc_linux_set_area_membind;
  hooks->get_area_memlocation = hwloc_linux_get_area_memlocation;
  hooks->alloc_membind = hwloc_linux_alloc_membind;
  hooks->alloc = hwloc_alloc_mmap;
  hooks->free_membind = hwloc_free_mmap;
  support->membind->firsttouch_membind = 1;
  support->membind->bind_membind = 1;
  support->membind->interleave_membind = 1;
  support->membind->migrate_membind = 1;
  /* if weighted interleave is supported, weights are exposed in sysfs */
  if (access("/sys/kernel/mm/mempolicy/weighted_interleave", F_OK) == 0)
    support->membind->weighted_interleave_membind = 1;
#endif
  hooks->get_allowed_resources = hwloc_linux_get_allowed_resources_hook;

  /* The get_allowed_resources() hook also works in the !thissystem case
   * (it just reads fsroot files) but hooks are only setup if thissystem.
   * Not an issue because this hook isn't used unless THISSYSTEM_ALLOWED_RESOURCES
   * which also requires THISSYSTEM which means this functions is called.
   */
}


/*******************************************
 *** Misc Helpers for Topology Discovery ***
 *******************************************/

/* cpuinfo array */
struct hwloc_linux_cpuinfo_proc {
  /* set during hwloc_linux_parse_cpuinfo */
  unsigned long Pproc;

  /* custom info, set during hwloc_linux_parse_cpuinfo */
  struct hwloc_info_s *infos;
  unsigned infos_count;
};

enum hwloc_linux_cgroup_type_e {
      HWLOC_LINUX_CGROUP2,
      HWLOC_LINUX_CGROUP1,
      HWLOC_LINUX_CPUSET
};

static void
hwloc_find_linux_cgroup_mntpnt(enum hwloc_linux_cgroup_type_e *cgtype, char **mntpnt, const char *root_path, int fsroot_fd)
{
  char *mount_path;
  struct mntent mntent;
  char *buf;
  FILE *fd;
  int err;
  size_t bufsize;

  /* try standard mount points */
  if (!hwloc_access("/sys/fs/cgroup/cpuset.cpus.effective", R_OK, fsroot_fd)) {
    hwloc_debug("Found standard cgroup2/cpuset mount point at /sys/fs/cgroup/\n");
    *cgtype = HWLOC_LINUX_CGROUP2;
    *mntpnt = strdup("/sys/fs/cgroup");
    return;
  } else if (!hwloc_access("/sys/fs/cgroup/cpuset/cpuset.cpus", R_OK, fsroot_fd)) {
    hwloc_debug("Found standard cgroup1/cpuset mount point at /sys/fs/cgroup/cpuset/\n");
    *cgtype = HWLOC_LINUX_CGROUP1;
    *mntpnt = strdup("/sys/fs/cgroup/cpuset");
    return;
  } else if (!hwloc_access("/dev/cpuset/cpus", R_OK, fsroot_fd)) {
    hwloc_debug("Found standard cpuset mount point at /dev/cpuset/\n");
    *cgtype = HWLOC_LINUX_CPUSET;
    *mntpnt = strdup("/dev/cpuset");
    return;
  }
  hwloc_debug("Couldn't find any standard cgroup or cpuset mount point, looking in /proc/mounts...\n");

  /* try to manually find the mount point */
  *mntpnt = NULL;

  if (root_path) {
    /* setmntent() doesn't support openat(), so use the root_path directly */
    err = asprintf(&mount_path, "%s/proc/mounts", root_path);
    if (err < 0)
      return;
    fd = setmntent(mount_path, "r");
    free(mount_path);
  } else {
    fd = setmntent("/proc/mounts", "r");
  }
  if (!fd)
    return;

  /* getmntent_r() doesn't actually report an error when the buffer
   * is too small. It just silently truncates things. So we can't
   * dynamically resize things.
   *
   * Linux limits mount type, string, and options to one page each.
   * getmntent() limits the line size to 4kB.
   * so use 4*pagesize to be far above both.
   */
  bufsize = hwloc_getpagesize()*4;
  buf = malloc(bufsize);
  if (!buf) {
    endmntent(fd);
    return;
  }

  while (getmntent_r(fd, &mntent, buf, bufsize)) {

    if (!strcmp(mntent.mnt_type, "cgroup2")) {
      char ctrls[1024]; /* there are about ten controllers with 10-char names */
      char ctrlpath[256];
      hwloc_debug("Found cgroup2 mount point on %s\n", mntent.mnt_dir);
      /* read controllers */
      snprintf(ctrlpath, sizeof(ctrlpath), "%s/cgroup.controllers", mntent.mnt_dir);
      err = hwloc_read_path_by_length(ctrlpath, ctrls, sizeof(ctrls), fsroot_fd);
      if (err > 0) {
	/* look for cpuset separated by spaces */
	char *ctrl, *_ctrls = ctrls;
	char *tmp;
	int cpuset_ctrl = 0;
	tmp = strchr(ctrls, '\n');
	if (tmp)
	  *tmp = '\0';
	hwloc_debug("Looking for `cpuset' controller in list `%s'\n", ctrls);
	while ((ctrl = strsep(&_ctrls, " ")) != NULL) {
	  if (!strcmp(ctrl, "cpuset")) {
	    cpuset_ctrl = 1;
	    break;
	  }
	}
	if (cpuset_ctrl) {
	  hwloc_debug("Found cgroup2/cpuset mount point on %s\n", mntent.mnt_dir);
	  *cgtype = HWLOC_LINUX_CGROUP2;
	  *mntpnt = strdup(mntent.mnt_dir);
	  break;
	}
      } else {
	hwloc_debug("Failed to read cgroup2 controllers from `%s'\n", ctrlpath);
      }

    } else if (!strcmp(mntent.mnt_type, "cpuset")) {
      hwloc_debug("Found cpuset mount point on %s\n", mntent.mnt_dir);
      *cgtype = HWLOC_LINUX_CPUSET;
      *mntpnt = strdup(mntent.mnt_dir);
      break;

    } else if (!strcmp(mntent.mnt_type, "cgroup")) {
      /* found a cgroup mntpnt */
      char *opt, *opts = mntent.mnt_opts;
      int cpuset_opt = 0;
      int noprefix_opt = 0;
      /* look at options */
      while ((opt = strsep(&opts, ",")) != NULL) {
	if (!strcmp(opt, "cpuset"))
	  cpuset_opt = 1;
	else if (!strcmp(opt, "noprefix"))
	  noprefix_opt = 1;
      }
      if (!cpuset_opt)
	continue;
      if (noprefix_opt) {
	hwloc_debug("Found cgroup1 emulating a cpuset mount point on %s\n", mntent.mnt_dir);
	*cgtype = HWLOC_LINUX_CPUSET;
	*mntpnt = strdup(mntent.mnt_dir);
	break;
      } else {
	hwloc_debug("Found cgroup1/cpuset mount point on %s\n", mntent.mnt_dir);
	*cgtype = HWLOC_LINUX_CGROUP1;
	*mntpnt = strdup(mntent.mnt_dir);
	break;
      }
    }
  }

  endmntent(fd);
  free(buf);
}

/*
 * Linux cpusets may be managed directly or through cgroup.
 * If cgroup is used, tasks get a /proc/pid/cgroup which may contain a
 * single line %d:cpuset:<name>. If cpuset are used they get /proc/pid/cpuset
 * containing <name>.
 */
static char *
hwloc_read_linux_cgroup_name(int fsroot_fd, hwloc_pid_t pid)
{
#define CPUSET_NAME_LEN 128
  char cpuset_name[CPUSET_NAME_LEN];
  FILE *file;
  int err;
  char *tmp;

  /* try to read from /proc/XXXX/cpuset */
  if (!pid)
    err = hwloc_read_path_by_length("/proc/self/cpuset", cpuset_name, sizeof(cpuset_name), fsroot_fd);
  else {
    char path[] = "/proc/XXXXXXXXXXX/cpuset";
    snprintf(path, sizeof(path), "/proc/%d/cpuset", pid);
    err = hwloc_read_path_by_length(path, cpuset_name, sizeof(cpuset_name), fsroot_fd);
  }
  if (err > 0) {
    /* found a cpuset, return the name */
    tmp = strchr(cpuset_name, '\n');
    if (tmp)
      *tmp = '\0';
    hwloc_debug("Found cgroup name `%s'\n", cpuset_name);
    return strdup(cpuset_name);
  }

  /* try to read from /proc/XXXX/cgroup */
  if (!pid)
    file = hwloc_fopen("/proc/self/cgroup", "r", fsroot_fd);
  else {
    char path[] = "/proc/XXXXXXXXXXX/cgroup";
    snprintf(path, sizeof(path), "/proc/%d/cgroup", pid);
    file = hwloc_fopen(path, "r", fsroot_fd);
  }
  if (file) {
    /* find a cpuset line */
#define CGROUP_LINE_LEN 256
    char line[CGROUP_LINE_LEN];
    while (fgets(line, sizeof(line), file)) {
      char *end, *path, *colon;
      colon = strchr(line, ':');
      if (!colon)
	continue;
      if (!strncmp(colon, ":cpuset:", 8)) /* cgroup v1 cpuset-specific hierarchy */
	path = colon + 8;
      else if (!strncmp(colon, "::", 2)) /* cgroup v2 unified hierarchy */
	path = colon + 2;
      else
	continue;

      /* found a cgroup with cpusets, return the name */
      fclose(file);
      end = strchr(path, '\n');
      if (end)
	*end = '\0';
      hwloc_debug("Found cgroup-cpuset %s\n", path);
      return strdup(path);
    }
    fclose(file);
  }

  /* found nothing */
  hwloc_debug("%s", "No cgroup or cpuset found\n");
  return NULL;
}

static void
hwloc_admin_disable_set_from_cgroup(int root_fd,
				    enum hwloc_linux_cgroup_type_e cgtype,
				    const char *mntpnt,
				    const char *cpuset_name,
				    const char *attr_name,
				    hwloc_bitmap_t admin_enabled_set)
{
#define CPUSET_FILENAME_LEN 256
  char cpuset_filename[CPUSET_FILENAME_LEN];
  int err;

  switch (cgtype) {
  case HWLOC_LINUX_CGROUP2:
    /* try to read the cpuset from cgroup2. use the last "effective" mask to get a AND of parent masks */
    snprintf(cpuset_filename, CPUSET_FILENAME_LEN, "%s%s/cpuset.%s.effective", mntpnt, cpuset_name, attr_name);
    hwloc_debug("Trying to read cgroup2 file <%s>\n", cpuset_filename);
    break;
  case HWLOC_LINUX_CGROUP1:
    /* try to read the cpuset from cgroup1. no need to use "effective_cpus/mems" since we'll remove offline CPUs in the core */
    snprintf(cpuset_filename, CPUSET_FILENAME_LEN, "%s%s/cpuset.%s", mntpnt, cpuset_name, attr_name);
    hwloc_debug("Trying to read cgroup1 file <%s>\n", cpuset_filename);
    break;
  case HWLOC_LINUX_CPUSET:
    /* try to read the cpuset directly */
    snprintf(cpuset_filename, CPUSET_FILENAME_LEN, "%s%s/%s", mntpnt, cpuset_name, attr_name);
    hwloc_debug("Trying to read cpuset file <%s>\n", cpuset_filename);
    break;
  }

  err = hwloc__read_path_as_cpulist(cpuset_filename, admin_enabled_set, root_fd);
  if (err < 0) {
    hwloc_debug("failed to read cpuset '%s' attribute '%s'\n", cpuset_name, attr_name);
    hwloc_bitmap_fill(admin_enabled_set);
    return;
  }
  hwloc_debug_bitmap("cpuset includes %s\n", admin_enabled_set);
}

static void
hwloc_parse_meminfo_info(struct hwloc_linux_backend_data_s *data,
			 const char *path,
			 uint64_t *local_memory)
{
  char *tmp;
  char buffer[4096];
  unsigned long long number;

  if (hwloc_read_path_by_length(path, buffer, sizeof(buffer), data->root_fd) <= 0)
    return;

  tmp = strstr(buffer, "MemTotal: "); /* MemTotal: %llu kB */
  if (tmp) {
    number = strtoull(tmp+10, NULL, 10);
    *local_memory = number << 10;
  }
}

#define SYSFS_NUMA_NODE_PATH_LEN 128

static void
hwloc_parse_hugepages_info(struct hwloc_linux_backend_data_s *data,
			   const char *dirpath,
			   struct hwloc_numanode_attr_s *memory,
			   unsigned allocated_page_types,
			   uint64_t *remaining_local_memory)
{
  DIR *dir;
  struct dirent *dirent;
  unsigned long index_ = 1; /* slot 0 is for normal pages */
  char line[64];
  char path[SYSFS_NUMA_NODE_PATH_LEN];

  dir = hwloc_opendir(dirpath, data->root_fd);
  if (dir) {
    while ((dirent = readdir(dir)) != NULL) {
      int err;
      if (strncmp(dirent->d_name, "hugepages-", 10))
        continue;
      if (index_ >= allocated_page_types) {
	/* we must increase the page_types array */
	struct hwloc_memory_page_type_s *tmp = realloc(memory->page_types, allocated_page_types * 2 * sizeof(*tmp));
	if (!tmp)
	  break;
	memory->page_types = tmp;
	allocated_page_types *= 2;
      }
      memory->page_types[index_].size = strtoul(dirent->d_name+10, NULL, 0) * 1024ULL;
      err = snprintf(path, sizeof(path), "%s/%s/nr_hugepages", dirpath, dirent->d_name);
      if ((size_t) err < sizeof(path)
	  && hwloc_read_path_by_length(path, line, sizeof(line), data->root_fd) > 0) {
	/* these are the actual total amount of huge pages */
	memory->page_types[index_].count = strtoull(line, NULL, 0);
	*remaining_local_memory -= memory->page_types[index_].count * memory->page_types[index_].size;
	index_++;
      }
    }
    closedir(dir);
    memory->page_types_len = index_;
  }
}

static void
hwloc_get_machine_meminfo(struct hwloc_linux_backend_data_s *data,
			  struct hwloc_numanode_attr_s *memory)
{
  struct stat st;
  int has_sysfs_hugepages = 0;
  int types = 1; /* only normal pages by default */
  uint64_t remaining_local_memory;
  int err;

  err = hwloc_stat("/sys/kernel/mm/hugepages", &st, data->root_fd);
  if (!err) {
    types = 1 /* normal non-huge size */ + st.st_nlink - 2 /* ignore . and .. */;
    if (types < 3)
      /* some buggy filesystems (e.g. btrfs when reading from fsroot)
       * return wrong st_nlink for directories (always 1 for btrfs).
       * use 3 as a sane default (default page + 2 huge sizes).
       * hwloc_parse_hugepages_info() will extend it if needed.
       */
      types = 3;
    has_sysfs_hugepages = 1;
  }

  memory->page_types = calloc(types, sizeof(*memory->page_types));
  if (!memory->page_types) {
    memory->page_types_len = 0;
    return;
  }
  memory->page_types_len = 1; /* we'll increase it when successfully getting hugepage info */

  /* get the total memory */
  hwloc_parse_meminfo_info(data, "/proc/meminfo",
			   &memory->local_memory);
  remaining_local_memory = memory->local_memory;

  if (has_sysfs_hugepages) {
    /* read from node%d/hugepages/hugepages-%skB/nr_hugepages */
    hwloc_parse_hugepages_info(data, "/sys/kernel/mm/hugepages", memory, types, &remaining_local_memory);
    /* memory->page_types_len may have changed */
  }

  /* use remaining memory as normal pages */
  memory->page_types[0].size = data->pagesize;
  memory->page_types[0].count = remaining_local_memory / memory->page_types[0].size;
}

static void
hwloc_get_sysfs_node_meminfo(struct hwloc_linux_backend_data_s *data,
			     int node,
			     struct hwloc_numanode_attr_s *memory)
{
  char path[SYSFS_NUMA_NODE_PATH_LEN];
  char meminfopath[SYSFS_NUMA_NODE_PATH_LEN];
  struct stat st;
  int has_sysfs_hugepages = 0;
  int types = 1; /* only normal pages by default */
  uint64_t remaining_local_memory;
  int err;

  sprintf(path, "/sys/devices/system/node/node%d/hugepages", node);
  err = hwloc_stat(path, &st, data->root_fd);
  if (!err) {
    types = 1 /* normal non-huge size */ + st.st_nlink - 2 /* ignore . and .. */;
    if (types < 3)
      /* some buggy filesystems (e.g. btrfs when reading from fsroot)
       * return wrong st_nlink for directories (always 1 for btrfs).
       * use 3 as a sane default (default page + 2 huge sizes).
       * hwloc_parse_hugepages_info() will extend it if needed.
       */
      types = 3;
    has_sysfs_hugepages = 1;
  }

  memory->page_types = calloc(types, sizeof(*memory->page_types));
  if (!memory->page_types) {
    memory->page_types_len = 0;
    return;
  }
  memory->page_types_len = 1; /* we'll increase it when successfully getting hugepage info */

  /* get the total memory */
  sprintf(meminfopath, "/sys/devices/system/node/node%d/meminfo", node);
  hwloc_parse_meminfo_info(data, meminfopath,
			   &memory->local_memory);
  remaining_local_memory = memory->local_memory;

  if (has_sysfs_hugepages) {
    /* read from node%d/hugepages/hugepages-%skB/nr_hugepages */
    hwloc_parse_hugepages_info(data, path, memory, types, &remaining_local_memory);
    /* memory->page_types_len may have changed */
  }

  /* use remaining memory as normal pages */
  memory->page_types[0].size = data->pagesize;
  memory->page_types[0].count = remaining_local_memory / memory->page_types[0].size;
}

static int
hwloc_parse_nodes_distances(unsigned nbnodes, unsigned *indexes, uint64_t *distances, int fsroot_fd)
{
  size_t len = (10+1)*nbnodes;
  uint64_t *curdist = distances;
  char *string;
  unsigned i;

  string = malloc(len); /* space-separated %d */
  if (!string)
    goto out;

  for(i=0; i<nbnodes; i++) {
    unsigned osnode = indexes[i];
    char distancepath[SYSFS_NUMA_NODE_PATH_LEN];
    char *tmp, *next;
    unsigned found;

    /* Linux nodeX/distance file contains distance from X to other localities (from ACPI SLIT table or so),
     * store them in slots X*N...X*N+N-1 */
    sprintf(distancepath, "/sys/devices/system/node/node%u/distance", osnode);
    if (hwloc_read_path_by_length(distancepath, string, len, fsroot_fd) <= 0)
      goto out_with_string;

    tmp = string;
    found = 0;
    while (tmp) {
      unsigned distance = strtoul(tmp, &next, 0); /* stored as a %d */
      if (next == tmp)
	break;
      *curdist = (uint64_t) distance;
      curdist++;
      found++;
      if (found == nbnodes)
	break;
      tmp = next+1;
    }
    if (found != nbnodes)
      goto out_with_string;
  }

  free(string);
  return 0;

 out_with_string:
  free(string);
 out:
  return -1;
}

static void
hwloc__get_dmi_id_one_info(struct hwloc_linux_backend_data_s *data,
			   hwloc_obj_t obj,
			   char *path, unsigned pathlen,
			   const char *dmi_name, const char *hwloc_name)
{
  char dmi_line[64];

  strcpy(path+pathlen, dmi_name);
  if (hwloc_read_path_by_length(path, dmi_line, sizeof(dmi_line), data->root_fd) <= 0)
    return;

  if (dmi_line[0] != '\0') {
    char *tmp = strchr(dmi_line, '\n');
    if (tmp)
      *tmp = '\0';
    hwloc_debug("found %s '%s'\n", hwloc_name, dmi_line);
    hwloc_obj_add_info(obj, hwloc_name, dmi_line);
  }
}

static void
hwloc__get_dmi_id_info(struct hwloc_linux_backend_data_s *data, hwloc_obj_t obj)
{
  char path[128];
  unsigned pathlen;
  DIR *dir;

  strcpy(path, "/sys/devices/virtual/dmi/id");
  dir = hwloc_opendir(path, data->root_fd);
  if (dir) {
    pathlen = 27;
  } else {
    strcpy(path, "/sys/class/dmi/id");
    dir = hwloc_opendir(path, data->root_fd);
    if (dir)
      pathlen = 17;
    else
      return;
  }
  closedir(dir);

  path[pathlen++] = '/';

  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "product_name", "DMIProductName");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "product_version", "DMIProductVersion");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "product_serial", "DMIProductSerial");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "product_uuid", "DMIProductUUID");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "board_vendor", "DMIBoardVendor");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "board_name", "DMIBoardName");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "board_version", "DMIBoardVersion");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "board_serial", "DMIBoardSerial");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "board_asset_tag", "DMIBoardAssetTag");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "chassis_vendor", "DMIChassisVendor");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "chassis_type", "DMIChassisType");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "chassis_version", "DMIChassisVersion");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "chassis_serial", "DMIChassisSerial");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "chassis_asset_tag", "DMIChassisAssetTag");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "bios_vendor", "DMIBIOSVendor");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "bios_version", "DMIBIOSVersion");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "bios_date", "DMIBIOSDate");
  hwloc__get_dmi_id_one_info(data, obj, path, pathlen, "sys_vendor", "DMISysVendor");
}

static void
hwloc__get_soc_one_info(struct hwloc_linux_backend_data_s *data,
                        hwloc_obj_t obj,
                        char *path, int n, const char *info_suffix)
{
  char soc_line[64];
  char infoname[64];

  if (hwloc_read_path_by_length(path, soc_line, sizeof(soc_line), data->root_fd) <= 0)
    return;

  if (soc_line[0] != '\0') {
    char *tmp = strchr(soc_line, '\n');
    if (tmp)
      *tmp = '\0';
    snprintf(infoname, sizeof(infoname), "SoC%d%s", n, info_suffix);
    hwloc_obj_add_info(obj, infoname, soc_line);
  }
}

static void
hwloc__get_soc_info(struct hwloc_linux_backend_data_s *data, hwloc_obj_t obj)
{
  char path[128];
  struct dirent *dirent;
  DIR *dir;

  /* single SoC, add topology info */
  strcpy(path, "/sys/bus/soc/devices");
  dir = hwloc_opendir(path, data->root_fd);
  if (!dir)
    return;

  while ((dirent = readdir(dir)) != NULL) {
    int i;
    if (sscanf(dirent->d_name, "soc%d", &i) != 1)
      continue;

    snprintf(path, sizeof(path), "/sys/bus/soc/devices/soc%d/soc_id", i);
    hwloc__get_soc_one_info(data, obj, path, i, "ID");
    snprintf(path, sizeof(path), "/sys/bus/soc/devices/soc%d/family", i);
    hwloc__get_soc_one_info(data, obj, path, i, "Family");
    snprintf(path, sizeof(path), "/sys/bus/soc/devices/soc%d/revision", i);
    hwloc__get_soc_one_info(data, obj, path, i, "Revision");
  }
  closedir(dir);
}


/***************************************
 * KNL NUMA quirks
 */

struct knl_hwdata {
  char memory_mode[32];
  char cluster_mode[32];
  long long int mcdram_cache_size; /* mcdram_cache_* is valid only if size > 0 */
  int mcdram_cache_associativity;
  int mcdram_cache_inclusiveness;
  int mcdram_cache_line_size;
};

struct knl_distances_summary {
  unsigned nb_values; /* number of different values found in the matrix */
  struct knl_distances_value {
    unsigned occurences;
    uint64_t value;
  } values[4]; /* sorted by occurences */
};

static int hwloc_knl_distances_value_compar(const void *_v1, const void *_v2)
{
  const struct knl_distances_value *v1 = _v1, *v2 = _v2;
  return v1->occurences - v2->occurences;
}

static int
hwloc_linux_knl_parse_numa_distances(unsigned nbnodes,
				     uint64_t *distances,
				     struct knl_distances_summary *summary)
{
  unsigned i, j, k;

  summary->nb_values = 1;
  summary->values[0].value = 10;
  summary->values[0].occurences = nbnodes;

  if (nbnodes == 1)
    /* nothing else needed */
    return 0;

  if (nbnodes != 2 && nbnodes != 4 && nbnodes != 8) {
    if (HWLOC_SHOW_CRITICAL_ERRORS())
      fprintf(stderr, "hwloc/linux: Ignoring KNL NUMA quirk, nbnodes (%u) isn't 2, 4 or 8.\n", nbnodes);
    return -1;
  }

  if (!distances) {
    if (HWLOC_SHOW_CRITICAL_ERRORS())
      fprintf(stderr, "hwloc/linux: Ignoring KNL NUMA quirk, distance matrix missing.\n");
    return -1;
  }

  for(i=0; i<nbnodes; i++) {
    /* check we have 10 on the diagonal */
    if (distances[i*nbnodes+i] != 10) {
      if (HWLOC_SHOW_CRITICAL_ERRORS())
        fprintf(stderr, "hwloc/linux: hwloc/linux: Ignoring KNL NUMA quirk, distance matrix does not contain 10 on the diagonal.\n");
      return -1;
    }
    for(j=i+1; j<nbnodes; j++) {
      uint64_t distance = distances[i*nbnodes+j];
      /* check things are symmetric */
      if (distance != distances[i+j*nbnodes]) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "hwloc/linux: Ignoring KNL NUMA quirk, distance matrix isn't symmetric.\n");
	return -1;
      }
      /* check everything outside the diagonal is > 10 */
      if (distance <= 10) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "hwloc/linux: Ignoring KNL NUMA quirk, distance matrix contains values <= 10.\n");
	return -1;
      }
      /* did we already see this value? */
      for(k=0; k<summary->nb_values; k++)
	if (distance == summary->values[k].value) {
	  summary->values[k].occurences++;
	  break;
	}
      if (k == summary->nb_values) {
	/* add a new value */
	if (k == 4) {
          if (HWLOC_SHOW_CRITICAL_ERRORS())
            fprintf(stderr, "hwloc/linux: Ignoring KNL NUMA quirk, distance matrix contains more than 4 different values.\n");
	  return -1;
	}
	summary->values[k].value = distance;
	summary->values[k].occurences = 1;
	summary->nb_values++;
      }
    }
  }

  qsort(summary->values, summary->nb_values, sizeof(struct knl_distances_value), hwloc_knl_distances_value_compar);

  if (nbnodes == 2) {
    if (summary->nb_values != 2) {
      if (HWLOC_SHOW_CRITICAL_ERRORS())
        fprintf(stderr, "hwloc/linux: Ignoring KNL NUMA quirk, distance matrix for 2 nodes cannot contain %u different values instead of 2.\n",
                summary->nb_values);
      return -1;
    }

  } else if (nbnodes == 4) {
    if (summary->nb_values != 2 && summary->nb_values != 4) {
      if (HWLOC_SHOW_CRITICAL_ERRORS())
        fprintf(stderr, "hwloc/linux: Ignoring KNL NUMA quirk, distance matrix for 8 nodes cannot contain %u different values instead of 2 or 4.\n",
                summary->nb_values);
      return -1;
    }

  } else if (nbnodes == 8) {
    if (summary->nb_values != 4) {
      if (HWLOC_SHOW_CRITICAL_ERRORS())
        fprintf(stderr, "hwloc/linux: Ignoring KNL NUMA quirk, distance matrix for 8 nodes cannot contain %u different values instead of 4.\n",
                summary->nb_values);
      return -1;
    }

  } else {
    abort(); /* checked above */
  }

  hwloc_debug("Summary of KNL distance matrix:\n");
  for(k=0; k<summary->nb_values; k++)
    hwloc_debug("  Found %u times distance %llu\n", summary->values[k].occurences, (unsigned long long) summary->values[k].value);
  return 0;
}

static int
hwloc_linux_knl_identify_4nodes(uint64_t *distances,
				struct knl_distances_summary *distsum,
				unsigned *ddr, unsigned *mcdram) /* ddr and mcdram arrays must be 2-long */
{
  uint64_t value;
  unsigned i;

  hwloc_debug("Trying to identify 4 KNL NUMA nodes in SNC-2 cluster mode...\n");

  /* The SNC2-Flat/Hybrid matrix should be something like
   * 10 21 31 41
   * 21 10 41 31
   * 31 41 10 41
   * 41 31 41 10
   * which means there are (above 4*10 on the diagonal):
   * 1 unique value for DDR to other DDR,
   * 2 identical values for DDR to local MCDRAM
   * 3 identical values for everything else.
   */
  if (distsum->nb_values != 4
      || distsum->values[0].occurences != 1 /* DDR to DDR */
      || distsum->values[1].occurences != 2 /* DDR to local MCDRAM */
      || distsum->values[2].occurences != 3 /* others */
      || distsum->values[3].occurences != 4 /* local */ )
    return -1;

  /* DDR:0 is always first */
  ddr[0] = 0;

  /* DDR:1 is at distance distsum->values[0].value from ddr[0] */
  value = distsum->values[0].value;
  ddr[1] = 0;
  hwloc_debug("  DDR#0 is NUMAnode#0\n");
  for(i=0; i<4; i++)
    if (distances[i] == value) {
      ddr[1] = i;
      hwloc_debug("  DDR#1 is NUMAnode#%u\n", i);
      break;
    }
  if (!ddr[1])
    return -1;

  /* MCDRAMs are at distance distsum->values[1].value from their local DDR */
  value = distsum->values[1].value;
  mcdram[0] = mcdram[1] = 0;
  for(i=1; i<4; i++) {
    if (distances[i] == value) {
      hwloc_debug("  MCDRAM#0 is NUMAnode#%u\n", i);
      mcdram[0] = i;
    } else if (distances[ddr[1]*4+i] == value) {
      hwloc_debug("  MCDRAM#1 is NUMAnode#%u\n", i);
      mcdram[1] = i;
    }
  }
  if (!mcdram[0] || !mcdram[1])
    return -1;

  return 0;
}

static int
hwloc_linux_knl_identify_8nodes(uint64_t *distances,
				struct knl_distances_summary *distsum,
				unsigned *ddr, unsigned *mcdram) /* ddr and mcdram arrays must be 4-long */
{
  uint64_t value;
  unsigned i, nb;

  hwloc_debug("Trying to identify 8 KNL NUMA nodes in SNC-4 cluster mode...\n");

  /* The SNC4-Flat/Hybrid matrix should be something like
   * 10 21 21 21 31 41 41 41
   * 21 10 21 21 41 31 41 41
   * 21 21 10 21 41 41 31 41
   * 21 21 21 10 41 41 41 31
   * 31 41 41 41 10 41 41 41
   * 41 31 41 41 41 10 41 41
   * 41 41 31 41 41 41 31 41
   * 41 41 41 31 41 41 41 41
   * which means there are (above 8*10 on the diagonal):
   * 4 identical values for DDR to local MCDRAM
   * 6 identical values for DDR to other DDR,
   * 18 identical values for everything else.
   */
  if (distsum->nb_values != 4
      || distsum->values[0].occurences != 4 /* DDR to local MCDRAM */
      || distsum->values[1].occurences != 6 /* DDR to DDR */
      || distsum->values[2].occurences != 8 /* local */
      || distsum->values[3].occurences != 18 /* others */ )
    return -1;

  /* DDR:0 is always first */
  ddr[0] = 0;
  hwloc_debug("  DDR#0 is NUMAnode#0\n");

  /* DDR:[1-3] are at distance distsum->values[1].value from ddr[0] */
  value = distsum->values[1].value;
  ddr[1] = ddr[2] = ddr[3] = 0;
  nb = 1;
  for(i=0; i<8; i++)
    if (distances[i] == value) {
      hwloc_debug("  DDR#%u is NUMAnode#%u\n", nb, i);
      ddr[nb++] = i;
      if (nb == 4)
	break;
    }
  if (nb != 4 || !ddr[1] || !ddr[2] || !ddr[3])
    return -1;

  /* MCDRAMs are at distance distsum->values[0].value from their local DDR */
  value = distsum->values[0].value;
  mcdram[0] = mcdram[1] = mcdram[2] = mcdram[3] = 0;
  for(i=1; i<8; i++) {
    if (distances[i] == value) {
      hwloc_debug("  MCDRAM#0 is NUMAnode#%u\n", i);
      mcdram[0] = i;
    } else if (distances[ddr[1]*8+i] == value) {
      hwloc_debug("  MCDRAM#1 is NUMAnode#%u\n", i);
      mcdram[1] = i;
    } else if (distances[ddr[2]*8+i] == value) {
      hwloc_debug("  MCDRAM#2 is NUMAnode#%u\n", i);
      mcdram[2] = i;
    } else if (distances[ddr[3]*8+i] == value) {
      hwloc_debug("  MCDRAM#3 is NUMAnode#%u\n", i);
      mcdram[3] = i;
    }
  }
  if (!mcdram[0] || !mcdram[1] || !mcdram[2] || !mcdram[3])
    return -1;

  return 0;
}

/* Try to handle knl hwdata properties
 * Returns 0 on success and -1 otherwise */
static int
hwloc_linux_knl_read_hwdata_properties(struct hwloc_linux_backend_data_s *data,
				       struct knl_hwdata *hwdata)
{
  char *knl_cache_file;
  int version = 0;
  char buffer[512] = {0};
  char *data_beg = NULL;

  if (asprintf(&knl_cache_file, "%s/knl_memoryside_cache", data->dumped_hwdata_dirname) < 0)
    return -1;

  hwloc_debug("Reading knl cache data from: %s\n", knl_cache_file);
  if (hwloc_read_path_by_length(knl_cache_file, buffer, sizeof(buffer), data->root_fd) <= 0) {
    hwloc_debug("Unable to open KNL data file `%s' (%s)\n", knl_cache_file, strerror(errno));
    free(knl_cache_file);
    return -1;
  }
  free(knl_cache_file);

  data_beg = &buffer[0];

  /* file must start with version information */
  if (sscanf(data_beg, "version: %d", &version) != 1) {
    fprintf(stderr, "hwloc/linux/hwdata: Invalid knl_memoryside_cache header, expected \"version: <int>\".\n");
    return -1;
  }

  while (1) {
    char *line_end = strstr(data_beg, "\n");
    if (!line_end)
        break;
    if (version >= 1) {
      if (!strncmp("cache_size:", data_beg, strlen("cache_size"))) {
          sscanf(data_beg, "cache_size: %lld", &hwdata->mcdram_cache_size);
          hwloc_debug("read cache_size=%lld\n", hwdata->mcdram_cache_size);
      } else if (!strncmp("line_size:", data_beg, strlen("line_size:"))) {
          sscanf(data_beg, "line_size: %d", &hwdata->mcdram_cache_line_size);
          hwloc_debug("read line_size=%d\n", hwdata->mcdram_cache_line_size);
      } else if (!strncmp("inclusiveness:", data_beg, strlen("inclusiveness:"))) {
          sscanf(data_beg, "inclusiveness: %d", &hwdata->mcdram_cache_inclusiveness);
          hwloc_debug("read inclusiveness=%d\n", hwdata->mcdram_cache_inclusiveness);
      } else if (!strncmp("associativity:", data_beg, strlen("associativity:"))) {
          sscanf(data_beg, "associativity: %d\n", &hwdata->mcdram_cache_associativity);
          hwloc_debug("read associativity=%d\n", hwdata->mcdram_cache_associativity);
      }
    }
    if (version >= 2) {
      if (!strncmp("cluster_mode: ", data_beg, strlen("cluster_mode: "))) {
	size_t length;
	data_beg += strlen("cluster_mode: ");
	length = line_end-data_beg;
	if (length > sizeof(hwdata->cluster_mode)-1)
	  length = sizeof(hwdata->cluster_mode)-1;
	memcpy(hwdata->cluster_mode, data_beg, length);
	hwdata->cluster_mode[length] = '\0';
        hwloc_debug("read cluster_mode=%s\n", hwdata->cluster_mode);
      } else if (!strncmp("memory_mode: ", data_beg, strlen("memory_mode: "))) {
	size_t length;
	data_beg += strlen("memory_mode: ");
	length = line_end-data_beg;
	if (length > sizeof(hwdata->memory_mode)-1)
	  length = sizeof(hwdata->memory_mode)-1;
	memcpy(hwdata->memory_mode, data_beg, length);
	hwdata->memory_mode[length] = '\0';
        hwloc_debug("read memory_mode=%s\n", hwdata->memory_mode);
      }
    }

    data_beg = line_end + 1;
  }

  if (hwdata->mcdram_cache_size == -1
      || hwdata->mcdram_cache_line_size == -1
      || hwdata->mcdram_cache_associativity == -1
      || hwdata->mcdram_cache_inclusiveness == -1) {
    hwloc_debug("Incorrect file format cache_size=%lld line_size=%d associativity=%d inclusiveness=%d\n",
		hwdata->mcdram_cache_size,
		hwdata->mcdram_cache_line_size,
		hwdata->mcdram_cache_associativity,
		hwdata->mcdram_cache_inclusiveness);
    hwdata->mcdram_cache_size = -1; /* mark cache as invalid */
  }

  return 0;
}

static void
hwloc_linux_knl_guess_hwdata_properties(struct knl_hwdata *hwdata,
					hwloc_obj_t *nodes, unsigned nbnodes,
					struct knl_distances_summary *distsum)
{
  /* Try to guess KNL configuration (Cluster mode, Memory mode, and MCDRAM cache info)
   * from the NUMA configuration (number of nodes, CPUless or not, distances).
   * Keep in mind that some CPUs might be offline (hence DDR could be CPUless too.
   * Keep in mind that part of the memory might be offline (hence MCDRAM could contain less than 16GB total).
   */

  hwloc_debug("Trying to guess missing KNL configuration information...\n");

  /* These MCDRAM cache attributes are always valid.
   * We'll only use them if mcdram_cache_size > 0
   */
  hwdata->mcdram_cache_associativity = 1;
  hwdata->mcdram_cache_inclusiveness = 1;
  hwdata->mcdram_cache_line_size = 64;
  /* All commercial KNL/KNM have 16GB of MCDRAM, we'll divide that in the number of SNC */

  if (hwdata->mcdram_cache_size > 0
      && hwdata->cluster_mode[0]
      && hwdata->memory_mode[0])
    /* Nothing to guess */
    return;

  /* Quadrant/All2All/Hemisphere are basically identical from the application point-of-view,
   * and Quadrant is recommended (except if underpopulating DIMMs).
   * Hence we'll assume Quadrant when unknown.
   */

  /* Flat/Hybrid25/Hybrid50 cannot be distinguished unless we know the Cache size
   * (if running a old hwloc-dump-hwdata that reports Cache size without modes)
   * or we're sure MCDRAM NUMAnode size was not decreased by offlining some memory.
   * Hence we'll assume Flat when unknown.
   */

  if (nbnodes == 1) {
    /* Quadrant-Cache */
    if (!hwdata->cluster_mode[0])
      strcpy(hwdata->cluster_mode, "Quadrant");
    if (!hwdata->memory_mode[0])
      strcpy(hwdata->memory_mode, "Cache");
    if (hwdata->mcdram_cache_size <= 0)
      hwdata->mcdram_cache_size = 16UL*1024*1024*1024;

  } else if (nbnodes == 2) {
    /* most likely Quadrant-Flat/Hybrid,
     * or SNC2/Cache (unlikely)
     */

    if (!strcmp(hwdata->memory_mode, "Cache")
	|| !strcmp(hwdata->cluster_mode, "SNC2")
	|| !hwloc_bitmap_iszero(nodes[1]->cpuset)) { /* MCDRAM cannot be nodes[0], and its cpuset is always empty */
      /* SNC2-Cache */
      if (!hwdata->cluster_mode[0])
	strcpy(hwdata->cluster_mode, "SNC2");
      if (!hwdata->memory_mode[0])
	strcpy(hwdata->memory_mode, "Cache");
      if (hwdata->mcdram_cache_size <= 0)
	hwdata->mcdram_cache_size = 8UL*1024*1024*1024;

    } else {
      /* Assume Quadrant-Flat/Hybrid.
       * Could also be SNC2-Cache with offline CPUs in nodes[1] (unlikely).
       */
      if (!hwdata->cluster_mode[0])
	strcpy(hwdata->cluster_mode, "Quadrant");
      if (!hwdata->memory_mode[0]) {
	if (hwdata->mcdram_cache_size == 4UL*1024*1024*1024)
	  strcpy(hwdata->memory_mode, "Hybrid25");
	else if (hwdata->mcdram_cache_size == 8UL*1024*1024*1024)
	  strcpy(hwdata->memory_mode, "Hybrid50");
	else
	  strcpy(hwdata->memory_mode, "Flat");
      } else {
	if (hwdata->mcdram_cache_size <= 0) {
	  if (!strcmp(hwdata->memory_mode, "Hybrid25"))
	    hwdata->mcdram_cache_size = 4UL*1024*1024*1024;
	  else if (!strcmp(hwdata->memory_mode, "Hybrid50"))
	    hwdata->mcdram_cache_size = 8UL*1024*1024*1024;
	}
      }
    }

  } else if (nbnodes == 4) {
    /* most likely SNC4-Cache
     * or SNC2-Flat/Hybrid (unlikely)
     *
     * SNC2-Flat/Hybrid has 4 different values in distsum,
     * while SNC4-Cache only has 2.
     */

    if (!strcmp(hwdata->cluster_mode, "SNC2") || distsum->nb_values == 4) {
      /* SNC2-Flat/Hybrid */
      if (!hwdata->cluster_mode[0])
	strcpy(hwdata->cluster_mode, "SNC2");
      if (!hwdata->memory_mode[0]) {
	if (hwdata->mcdram_cache_size == 2UL*1024*1024*1024)
	  strcpy(hwdata->memory_mode, "Hybrid25");
	else if (hwdata->mcdram_cache_size == 4UL*1024*1024*1024)
	  strcpy(hwdata->memory_mode, "Hybrid50");
	else
	  strcpy(hwdata->memory_mode, "Flat");
      } else {
	if (hwdata->mcdram_cache_size <= 0) {
	  if (!strcmp(hwdata->memory_mode, "Hybrid25"))
	    hwdata->mcdram_cache_size = 2UL*1024*1024*1024;
	  else if (!strcmp(hwdata->memory_mode, "Hybrid50"))
	    hwdata->mcdram_cache_size = 4UL*1024*1024*1024;
	}
      }

    } else {
      /* Assume SNC4-Cache.
       * SNC2 is unlikely.
       */
      if (!hwdata->cluster_mode[0])
	strcpy(hwdata->cluster_mode, "SNC4");
      if (!hwdata->memory_mode[0])
	strcpy(hwdata->memory_mode, "Cache");
      if (hwdata->mcdram_cache_size <= 0)
	hwdata->mcdram_cache_size = 4UL*1024*1024*1024;
    }

  } else if (nbnodes == 8) {
    /* SNC4-Flat/Hybrid */

    if (!hwdata->cluster_mode[0])
      strcpy(hwdata->cluster_mode, "SNC4");
    if (!hwdata->memory_mode[0]) {
      if (hwdata->mcdram_cache_size == 1UL*1024*1024*1024)
	strcpy(hwdata->memory_mode, "Hybrid25");
      else if (hwdata->mcdram_cache_size == 2UL*1024*1024*1024)
	strcpy(hwdata->memory_mode, "Hybrid50");
      else
	strcpy(hwdata->memory_mode, "Flat");
    } else {
      if (hwdata->mcdram_cache_size <= 0) {
	if (!strcmp(hwdata->memory_mode, "Hybrid25"))
	  hwdata->mcdram_cache_size = 1UL*1024*1024*1024;
	else if (!strcmp(hwdata->memory_mode, "Hybrid50"))
	  hwdata->mcdram_cache_size = 2UL*1024*1024*1024;
      }
    }
  }

  hwloc_debug("  Found cluster=%s memory=%s cache=%lld\n",
	      hwdata->cluster_mode, hwdata->memory_mode,
	      hwdata->mcdram_cache_size);
}

static void
hwloc_linux_knl_add_cluster(struct hwloc_topology *topology,
			    hwloc_obj_t ddr, hwloc_obj_t mcdram,
			    struct knl_hwdata *knl_hwdata,
			    int mscache_as_l3,
                            int snclevel,
			    unsigned *failednodes)
{
  hwloc_obj_t cluster = NULL;

  if (mcdram) {
    mcdram->subtype = strdup("MCDRAM");
    /* Change MCDRAM cpuset to DDR cpuset for clarity.
     * Not actually useful if we insert with hwloc__attach_memory_object() below.
     * The cpuset will be updated by the core later anyway.
     */
    hwloc_bitmap_copy(mcdram->cpuset, ddr->cpuset);

    /* also mark ddr as DRAM to match what we do in memattrs.c */
    assert(ddr);
    ddr->subtype = strdup("DRAM");

    /* Add a Group for Cluster containing this MCDRAM + DDR */
    cluster = hwloc_alloc_setup_object(topology, HWLOC_OBJ_GROUP, HWLOC_UNKNOWN_INDEX);
    hwloc_obj_add_other_obj_sets(cluster, ddr);
    hwloc_obj_add_other_obj_sets(cluster, mcdram);
    cluster->subtype = strdup("Cluster");
    cluster->attr->group.kind = HWLOC_GROUP_KIND_INTEL_KNL_SUBNUMA_CLUSTER;
    cluster = hwloc__insert_object_by_cpuset(topology, NULL, cluster, "linux:knl:snc:group");
  }

  if (cluster) {
    /* Now insert NUMA nodes below this cluster */
    hwloc_obj_t res;
    res = hwloc__attach_memory_object(topology, cluster, ddr, "linux:knl:snc:ddr");
    if (res != ddr) {
      (*failednodes)++;
      ddr = NULL;
    }
    res = hwloc__attach_memory_object(topology, cluster, mcdram, "linux:knl:snc:mcdram");
    if (res != mcdram) {
      (*failednodes)++;
      mcdram = NULL;
    }

  } else {
    /* we don't know where to attach, let the core find or insert if needed */
    hwloc_obj_t res;
    res = hwloc__insert_object_by_cpuset(topology, NULL, ddr, "linux:knl:ddr");
    if (res != ddr) {
      (*failednodes)++;
      ddr = NULL;
    }
    if (mcdram) {
      res = hwloc__insert_object_by_cpuset(topology, NULL, mcdram, "linux:knl:mcdram");
      if (res != mcdram) {
	(*failednodes)++;
        mcdram = NULL;
      }
    }
  }

  if (ddr && mcdram && !(topology->flags & HWLOC_TOPOLOGY_FLAG_NO_MEMATTRS)) {
    /* add memattrs to distinguish DDR and MCDRAM */
    struct hwloc_internal_location_s loc;
    hwloc_uint64_t ddrbw;
    hwloc_uint64_t mcdrambw;
    ddrbw = 90000/snclevel;
    mcdrambw = 360000/snclevel;
    loc.type = HWLOC_LOCATION_TYPE_CPUSET;
    loc.location.cpuset = ddr->cpuset;
    hwloc_internal_memattr_set_value(topology, HWLOC_MEMATTR_ID_BANDWIDTH, HWLOC_OBJ_NUMANODE, (hwloc_uint64_t)-1, ddr->os_index, &loc, ddrbw);
    hwloc_internal_memattr_set_value(topology, HWLOC_MEMATTR_ID_BANDWIDTH, HWLOC_OBJ_NUMANODE, (hwloc_uint64_t)-1, mcdram->os_index, &loc, mcdrambw);
  }

  if (ddr && knl_hwdata->mcdram_cache_size > 0) {
    /* Now insert the mscache if any */
    hwloc_obj_t cache = hwloc_alloc_setup_object(topology, HWLOC_OBJ_L3CACHE, HWLOC_UNKNOWN_INDEX);
    if (!cache)
      /* failure is harmless */
      return;
    cache->attr->cache.depth = 3;
    cache->attr->cache.type = HWLOC_OBJ_CACHE_UNIFIED;
    cache->attr->cache.size = knl_hwdata->mcdram_cache_size;
    cache->attr->cache.linesize = knl_hwdata->mcdram_cache_line_size;
    cache->attr->cache.associativity = knl_hwdata->mcdram_cache_associativity;
    hwloc_obj_add_info(cache, "Inclusive", knl_hwdata->mcdram_cache_inclusiveness ? "1" : "0");
    cache->cpuset = hwloc_bitmap_dup(ddr->cpuset);
    cache->nodeset = hwloc_bitmap_dup(ddr->nodeset); /* only applies to DDR */
    if (mscache_as_l3) {
      /* make it a L3 */
      cache->subtype = strdup("MemorySideCache");
      hwloc__insert_object_by_cpuset(topology, NULL, cache, "linux:knl:memcache:l3cache");
    } else {
      /* make it a real mscache */
      cache->type = HWLOC_OBJ_MEMCACHE;
      cache->depth = 1;
      if (cluster)
	hwloc__attach_memory_object(topology, cluster, cache, "linux:knl:snc:memcache");
      else
	hwloc__insert_object_by_cpuset(topology, NULL, cache, "linux:knl:memcache");
    }
  }
}

static void
hwloc_linux_knl_numa_quirk(struct hwloc_topology *topology,
			   struct hwloc_linux_backend_data_s *data,
			   hwloc_obj_t *nodes, unsigned nbnodes,
			   uint64_t * distances,
			   unsigned *failednodes)
{
  struct knl_hwdata hwdata;
  struct knl_distances_summary dist;
  unsigned i;
  char * fallback_env = getenv("HWLOC_KNL_HDH_FALLBACK");
  int fallback = fallback_env ? atoi(fallback_env) : -1; /* by default, only fallback if needed */
  char * mscache_as_l3_env = getenv("HWLOC_KNL_MSCACHE_L3");
  int mscache_as_l3 = mscache_as_l3_env ? atoi(mscache_as_l3_env) : 1; /* L3 by default, for backward compat */

  if (*failednodes)
    goto error;

  if (hwloc_linux_knl_parse_numa_distances(nbnodes, distances, &dist) < 0)
    goto error;

  hwdata.memory_mode[0] = '\0';
  hwdata.cluster_mode[0] = '\0';
  hwdata.mcdram_cache_size = -1;
  hwdata.mcdram_cache_associativity = -1;
  hwdata.mcdram_cache_inclusiveness = -1;
  hwdata.mcdram_cache_line_size = -1;
  if (fallback == 1)
    hwloc_debug("KNL dumped hwdata ignored, forcing fallback to heuristics\n");
  else
    hwloc_linux_knl_read_hwdata_properties(data, &hwdata);
  if (fallback != 0)
    hwloc_linux_knl_guess_hwdata_properties(&hwdata, nodes, nbnodes, &dist);

  if (strcmp(hwdata.cluster_mode, "All2All")
      && strcmp(hwdata.cluster_mode, "Hemisphere")
      && strcmp(hwdata.cluster_mode, "Quadrant")
      && strcmp(hwdata.cluster_mode, "SNC2")
      && strcmp(hwdata.cluster_mode, "SNC4")) {
    if (HWLOC_SHOW_CRITICAL_ERRORS())
      fprintf(stderr, "hwloc/linux: Failed to find a usable KNL cluster mode (%s)\n", hwdata.cluster_mode);
    goto error;
  }
  if (strcmp(hwdata.memory_mode, "Cache")
      && strcmp(hwdata.memory_mode, "Flat")
      && strcmp(hwdata.memory_mode, "Hybrid25")
      && strcmp(hwdata.memory_mode, "Hybrid50")) {
    if (HWLOC_SHOW_CRITICAL_ERRORS())
      fprintf(stderr, "hwloc/linux: Failed to find a usable KNL memory mode (%s)\n", hwdata.memory_mode);
    goto error;
  }

  if (mscache_as_l3) {
    if (!hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_L3CACHE))
      hwdata.mcdram_cache_size = 0;
  } else {
    if (!hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_MEMCACHE))
      hwdata.mcdram_cache_size = 0;
  }

  hwloc_obj_add_info(topology->levels[0][0], "ClusterMode", hwdata.cluster_mode);
  hwloc_obj_add_info(topology->levels[0][0], "MemoryMode", hwdata.memory_mode);

  if (!strcmp(hwdata.cluster_mode, "All2All")
      || !strcmp(hwdata.cluster_mode, "Hemisphere")
      || !strcmp(hwdata.cluster_mode, "Quadrant")) {
    if (!strcmp(hwdata.memory_mode, "Cache")) {
      /* Quadrant-Cache */
      if (nbnodes != 1) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "hwloc/linux: Found %u NUMA nodes instead of 1 in mode %s-%s\n", nbnodes, hwdata.cluster_mode, hwdata.memory_mode);
	goto error;
      }
      hwloc_linux_knl_add_cluster(topology, nodes[0], NULL, &hwdata, mscache_as_l3, 1, failednodes);

    } else {
      /* Quadrant-Flat/Hybrid */
      if (nbnodes != 2) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "hwloc/linux: Found %u NUMA nodes instead of 2 in mode %s-%s\n", nbnodes, hwdata.cluster_mode, hwdata.memory_mode);
	goto error;
      }
      if (!strcmp(hwdata.memory_mode, "Flat"))
	hwdata.mcdram_cache_size = 0;
      hwloc_linux_knl_add_cluster(topology, nodes[0], nodes[1], &hwdata, mscache_as_l3, 1, failednodes);
    }

  } else if (!strcmp(hwdata.cluster_mode, "SNC2")) {
    if (!strcmp(hwdata.memory_mode, "Cache")) {
      /* SNC2-Cache */
      if (nbnodes != 2) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "hwloc/linux: Found %u NUMA nodes instead of 2 in mode %s-%s\n", nbnodes, hwdata.cluster_mode, hwdata.memory_mode);
	goto error;
      }
      hwloc_linux_knl_add_cluster(topology, nodes[0], NULL, &hwdata, mscache_as_l3, 2, failednodes);
      hwloc_linux_knl_add_cluster(topology, nodes[1], NULL, &hwdata, mscache_as_l3, 2, failednodes);

    } else {
      /* SNC2-Flat/Hybrid */
      unsigned ddr[2], mcdram[2];
      if (nbnodes != 4) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "hwloc/linux: Found %u NUMA nodes instead of 2 in mode %s-%s\n", nbnodes, hwdata.cluster_mode, hwdata.memory_mode);
	goto error;
      }
      if (hwloc_linux_knl_identify_4nodes(distances, &dist, ddr, mcdram) < 0) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "Uhwloc/linux: nexpected distance layout for mode %s-%s\n", hwdata.cluster_mode, hwdata.memory_mode);
	goto error;
      }
      if (!strcmp(hwdata.memory_mode, "Flat"))
	hwdata.mcdram_cache_size = 0;
      hwloc_linux_knl_add_cluster(topology, nodes[ddr[0]], nodes[mcdram[0]], &hwdata, mscache_as_l3, 2, failednodes);
      hwloc_linux_knl_add_cluster(topology, nodes[ddr[1]], nodes[mcdram[1]], &hwdata, mscache_as_l3, 2, failednodes);
    }

  } else if (!strcmp(hwdata.cluster_mode, "SNC4")) {
    if (!strcmp(hwdata.memory_mode, "Cache")) {
      /* SNC4-Cache */
      if (nbnodes != 4) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "hwloc/linux: Found %u NUMA nodes instead of 4 in mode %s-%s\n", nbnodes, hwdata.cluster_mode, hwdata.memory_mode);
	goto error;
      }
      hwloc_linux_knl_add_cluster(topology, nodes[0], NULL, &hwdata, mscache_as_l3, 4, failednodes);
      hwloc_linux_knl_add_cluster(topology, nodes[1], NULL, &hwdata, mscache_as_l3, 4, failednodes);
      hwloc_linux_knl_add_cluster(topology, nodes[2], NULL, &hwdata, mscache_as_l3, 4, failednodes);
      hwloc_linux_knl_add_cluster(topology, nodes[3], NULL, &hwdata, mscache_as_l3, 4, failednodes);

    } else {
      /* SNC4-Flat/Hybrid */
      unsigned ddr[4], mcdram[4];
      if (nbnodes != 8) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "hwloc/linux: Found %u NUMA nodes instead of 2 in mode %s-%s\n", nbnodes, hwdata.cluster_mode, hwdata.memory_mode);
	goto error;
      }
      if (hwloc_linux_knl_identify_8nodes(distances, &dist, ddr, mcdram) < 0) {
        if (HWLOC_SHOW_CRITICAL_ERRORS())
          fprintf(stderr, "hwloc/linux: Unexpected distance layout for mode %s-%s\n", hwdata.cluster_mode, hwdata.memory_mode);
	goto error;
      }
      if (!strcmp(hwdata.memory_mode, "Flat"))
	hwdata.mcdram_cache_size = 0;
      hwloc_linux_knl_add_cluster(topology, nodes[ddr[0]], nodes[mcdram[0]], &hwdata, mscache_as_l3, 4, failednodes);
      hwloc_linux_knl_add_cluster(topology, nodes[ddr[1]], nodes[mcdram[1]], &hwdata, mscache_as_l3, 4, failednodes);
      hwloc_linux_knl_add_cluster(topology, nodes[ddr[2]], nodes[mcdram[2]], &hwdata, mscache_as_l3, 4, failednodes);
      hwloc_linux_knl_add_cluster(topology, nodes[ddr[3]], nodes[mcdram[3]], &hwdata, mscache_as_l3, 4, failednodes);
    }
  }

  return;

 error:
  /* just insert nodes basically */
  for (i = 0; i < nbnodes; i++) {
    hwloc_obj_t node = nodes[i];
    if (node) {
      hwloc_obj_t res_obj = hwloc__insert_object_by_cpuset(topology, NULL, node, "linux:knl:basic:numa");
      if (res_obj != node)
	/* This NUMA node got merged somehow, could be a buggy BIOS reporting wrong NUMA node cpuset.
	 * This object disappeared, we'll ignore distances */
	(*failednodes)++;
    }
  }
}


/**************************************
 ****** Sysfs Topology Discovery ******
 **************************************/

/* try to find locality of CPU-less NUMA nodes by looking at their distances */
static int
fixup_cpuless_node_locality_from_distances(unsigned i,
					   unsigned nbnodes, hwloc_obj_t *nodes, uint64_t *distances)
{
  unsigned min = UINT_MAX;
  unsigned nb = 0, j;

  for(j=0; j<nbnodes; j++) {
    if (j==i || !nodes[j])
      continue;
    if (distances[i*nbnodes+j] < min) {
      min = distances[i*nbnodes+j];
      nb = 1;
    } else if (distances[i*nbnodes+j] == min) {
      nb++;
    }
  }

  if (min <= distances[i*nbnodes+i] || min == UINT_MAX || nb == nbnodes-1)
    return -1;

  /* not local, but closer to *some* other nodes */
  for(j=0; j<nbnodes; j++)
    if (j!=i && nodes[j] && distances[i*nbnodes+j] == min)
      hwloc_bitmap_or(nodes[i]->cpuset, nodes[i]->cpuset, nodes[j]->cpuset);
  return 0;
}

/* try to find locality of CPU-less NUMA nodes by looking at HMAT initiators.
 *
 * In theory, we may have HMAT info only for some nodes.
 * In practice, if this never occurs, we may want to assume HMAT for either all or no nodes.
 */
static int
read_node_initiators(struct hwloc_linux_backend_data_s *data,
		     hwloc_obj_t node, unsigned nbnodes, hwloc_obj_t *nodes)
{
  char accesspath[SYSFS_NUMA_NODE_PATH_LEN];
  DIR *dir;
  struct dirent *dirent;

  /* starting with Linux 5.10, Generic Initiators may be preferred to CPU initiators.
   * access0 contains the fastest of GI and CPU. access1 contains the fastest of CPU.
   * Try access1 to avoid GI if any, or fallback to access0 otherwise.
   */
  sprintf(accesspath, "/sys/devices/system/node/node%u/access1/initiators", node->os_index);
  dir = hwloc_opendir(accesspath, data->root_fd);
  if (!dir) {
    sprintf(accesspath, "/sys/devices/system/node/node%u/access0/initiators", node->os_index);
    dir = hwloc_opendir(accesspath, data->root_fd);
    if (!dir)
      return -1;
  }

  while ((dirent = readdir(dir)) != NULL) {
    unsigned initiator_os_index;
    if (sscanf(dirent->d_name, "node%u", &initiator_os_index) == 1
	&& initiator_os_index != node->os_index) {
      /* we found an initiator that's not ourself,
       * find the corresponding node and add its cpuset
       */
      unsigned j;
      for(j=0; j<nbnodes; j++)
	if (nodes[j] && nodes[j]->os_index == initiator_os_index) {
	  hwloc_bitmap_or(node->cpuset, node->cpuset, nodes[j]->cpuset);
	  break;
	}
    }
  }
  closedir(dir);
  return 0;
}

static int
read_node_local_memattrs(struct hwloc_topology *topology,
                         struct hwloc_linux_backend_data_s *data,
                         hwloc_obj_t node)
{
  char accessdirpath[SYSFS_NUMA_NODE_PATH_LEN];
  char accesspath[SYSFS_NUMA_NODE_PATH_LEN+20];
  unsigned rbw = 0, wbw = 0, rlat = 0, wlat = 0;
  struct hwloc_internal_location_s loc;

  /* starting with Linux 5.10, Generic Initiators may be preferred to CPU initiators.
   * access0 contains the fastest of GI and CPU. access1 contains the fastest of CPU.
   * Try access1 to avoid GI if any, or fallback to access0 otherwise.
   */
  sprintf(accessdirpath, "/sys/devices/system/node/node%u/access1/initiators", node->os_index);
  if (hwloc_access(accessdirpath, X_OK, data->root_fd) < 0)
    sprintf(accessdirpath, "/sys/devices/system/node/node%u/access0/initiators", node->os_index);

  loc.type = HWLOC_LOCATION_TYPE_CPUSET;
  loc.location.cpuset = node->cpuset;

  /* bandwidth in MiB/s and latency in ns, just like in our memattrs API */

  sprintf(accesspath, "%s/read_bandwidth", accessdirpath);
  if (hwloc_read_path_as_uint(accesspath, &rbw, data->root_fd) == 0 && rbw > 0) {
    hwloc_internal_memattr_set_value(topology, HWLOC_MEMATTR_ID_READ_BANDWIDTH, HWLOC_OBJ_NUMANODE, (hwloc_uint64_t)-1, node->os_index, &loc, rbw);
  }
  sprintf(accesspath, "%s/write_bandwidth", accessdirpath);
  if (hwloc_read_path_as_uint(accesspath, &wbw, data->root_fd) == 0 && wbw > 0) {
    hwloc_internal_memattr_set_value(topology, HWLOC_MEMATTR_ID_WRITE_BANDWIDTH, HWLOC_OBJ_NUMANODE, (hwloc_uint64_t)-1, node->os_index, &loc, wbw);
  }
  /* main BW is average if both are known, or just read if known, or just write */
  if (rbw > 0 && wbw > 0)
    hwloc_internal_memattr_set_value(topology, HWLOC_MEMATTR_ID_BANDWIDTH, HWLOC_OBJ_NUMANODE, (hwloc_uint64_t)-1, node->os_index, &loc, (rbw+wbw)/2);

  sprintf(accesspath, "%s/read_latency", accessdirpath);
  if (hwloc_read_path_as_uint(accesspath, &rlat, data->root_fd) == 0 && rlat > 0) {
    hwloc_internal_memattr_set_value(topology, HWLOC_MEMATTR_ID_READ_LATENCY, HWLOC_OBJ_NUMANODE, (hwloc_uint64_t)-1, node->os_index, &loc, rlat);
  }
  sprintf(accesspath, "%s/write_latency", accessdirpath);
  if (hwloc_read_path_as_uint(accesspath, &wlat, data->root_fd) == 0 && wlat > 0) {
    hwloc_internal_memattr_set_value(topology, HWLOC_MEMATTR_ID_WRITE_LATENCY, HWLOC_OBJ_NUMANODE, (hwloc_uint64_t)-1, node->os_index, &loc, wlat);
  }
  /* main latency is average if both are known, or just read if known, or just write */
  if (rlat > 0 && wlat > 0)
    hwloc_internal_memattr_set_value(topology, HWLOC_MEMATTR_ID_LATENCY, HWLOC_OBJ_NUMANODE, (hwloc_uint64_t)-1, node->os_index, &loc, (rlat+wlat)/2);

  return 0;
}

/* return -1 if the kernel doesn't support mscache,
 * or update tree (containing only the node on input) with caches (if any)
 */
static int
read_node_mscaches(struct hwloc_topology *topology,
		   struct hwloc_linux_backend_data_s *data,
		   hwloc_obj_t *treep)
{
  hwloc_obj_t tree = *treep, node = tree;
  unsigned osnode = node->os_index;
  char mscpath[SYSFS_NUMA_NODE_PATH_LEN];
  DIR *mscdir;
  struct dirent *dirent;

  sprintf(mscpath, "/sys/devices/system/node/node%u/memory_side_cache", osnode);
  mscdir = hwloc_opendir(mscpath, data->root_fd);
  if (!mscdir)
    return -1;

  while ((dirent = readdir(mscdir)) != NULL) {
    unsigned depth;
    uint64_t size;
    unsigned line_size;
    unsigned associativity;
    hwloc_obj_t cache;

    if (strncmp(dirent->d_name, "index", 5))
      continue;

    depth = atoi(dirent->d_name+5);

    sprintf(mscpath, "/sys/devices/system/node/node%u/memory_side_cache/index%u/size", osnode, depth);
    if (hwloc_read_path_as_uint64(mscpath, &size, data->root_fd) < 0)
      continue;

    sprintf(mscpath, "/sys/devices/system/node/node%u/memory_side_cache/index%u/line_size", osnode, depth);
    if (hwloc_read_path_as_uint(mscpath, &line_size, data->root_fd) < 0)
      continue;

    sprintf(mscpath, "/sys/devices/system/node/node%u/memory_side_cache/index%u/indexing", osnode, depth);
    if (hwloc_read_path_as_uint(mscpath, &associativity, data->root_fd) < 0)
      continue;
    /* 0 for direct-mapped, 1 for indexed (don't know how many ways), 2 for custom/other */

    cache = hwloc_alloc_setup_object(topology, HWLOC_OBJ_MEMCACHE, HWLOC_UNKNOWN_INDEX);
    if (cache) {
      cache->nodeset = hwloc_bitmap_dup(node->nodeset);
      cache->cpuset = hwloc_bitmap_dup(node->cpuset);
      cache->attr->cache.size = size;
      cache->attr->cache.depth = depth;
      cache->attr->cache.linesize = line_size;
      cache->attr->cache.type = HWLOC_OBJ_CACHE_UNIFIED;
      cache->attr->cache.associativity = !associativity ? 1 /* direct-mapped */ : 0 /* unknown */;
      hwloc_debug_1arg_bitmap("mscache %s has nodeset %s\n",
			      dirent->d_name, cache->nodeset);

      cache->memory_first_child = tree;
      tree = cache;
    }
  }
  closedir(mscdir);
  *treep = tree;
  return 0;
}

static int
annotate_cxl_dax(hwloc_obj_t obj, unsigned region, int root_fd)
{
  char path[300];
  char bdfs[(12+1)*16]; /* 16 interleaved devices max, 12 chars par BDF, comma-separated + ending \0 */
  char *curbdfptr = bdfs;
  unsigned interleave_ways = 0;
  unsigned i;
  *curbdfptr = '\0';

  for(i=0; ; i++) {
    char decoder[20]; /* "decoderX.Y" */
    char decoderpath[256], *endpoint;
    char uportpath[256], *pcirootbus, *pcibdf;
    unsigned pcidomain, pcibus, pcidevice, pcifunc;
    char *slash, *end;
    int err;

    /* read the i-th decoder name from file target<i> */
    snprintf(path, sizeof(path), "/sys/bus/cxl/devices/region%u/target%u", region, i);
    if (hwloc_read_path_by_length(path, decoder, sizeof(decoder), root_fd) < 0)
      break;
    end = strchr(decoder, '\n');
    if (end)
      *end = '\0';
    hwloc_debug("hwloc/dax/cxl: found decoder `%s' for region#%u target#%u\n", decoder, region, i);

    /* get the endpoint symlink which ends with "/portT/endpointX/decoderY.X/" */
    snprintf(path, sizeof(path), "/sys/bus/cxl/devices/%s", decoder);
    err = hwloc_readlink(path, decoderpath, sizeof(decoderpath), root_fd);
    if (err < 0)
      break;
    endpoint = strstr(decoderpath, "endpoint");
    if (!endpoint)
      break;
    slash = strchr(endpoint, '/');
    if (!slash)
      break;
    *slash = '\0';
    hwloc_debug("hwloc/dax/cxl: found endpoint `%s'\n", endpoint);

    /* get the PCI in the endpointX/uport symlink "../../../pci<busid>/<BDFs>../memX" */
    snprintf(path, sizeof(path), "/sys/bus/cxl/devices/%s/uport", endpoint);
    err = hwloc_readlink(path, uportpath, sizeof(uportpath), root_fd);
    if (err < 0)
      break;
    hwloc_debug("hwloc/dax/cxl: lookind for BDF at the end of uport `%s'\n", uportpath);
    pcirootbus = strstr(uportpath, "/pci");
    if (!pcirootbus)
      break;
    slash = pcirootbus + 11; /* "/pciXXXX:YY/" */
    if (*slash != '/')
      break;
    pcibdf = NULL;
    while (sscanf(slash, "/%x:%x:%x.%x/", &pcidomain, &pcibus, &pcidevice, &pcifunc) == 4) {
      pcibdf = slash+1;
      slash += 13;
    }
    *slash = '\0';
    if (pcibdf) {
      if (interleave_ways) {
        if (interleave_ways >= 16) {
          if (HWLOC_SHOW_CRITICAL_ERRORS())
            fprintf(stderr, "hwloc/linux/cxl: Found more than 16 interleaved devices for region%u, ignoring the last ones.\n", region);
          break;
        }
        *(curbdfptr++) = ',';
      }
      strcpy(curbdfptr, pcibdf);
      curbdfptr += 12;
      interleave_ways++;
    }
  }

  if (interleave_ways) {
    if (interleave_ways > 1) {
      char tmp[12]; /* interleave ways is 16 max */
      snprintf(tmp, sizeof(tmp), "%u", interleave_ways);
      hwloc_obj_add_info(obj, "CXLDeviceInterleaveWays", tmp);
    }
    hwloc_obj_add_info(obj, "CXLDevice", bdfs);
  }

  return 0;
}

static int
dax_is_kmem(const char *name, int fsroot_fd)
{
  char path[300];
  struct stat stbuf;

  snprintf(path, sizeof(path), "/sys/bus/dax/drivers/kmem/%s", name);
  return hwloc_stat(path, &stbuf, fsroot_fd) == 0;
}

static void
annotate_dax_parent(hwloc_obj_t obj, const char *name, int fsroot_fd)
{
  char daxpath[300];
  char link[PATH_MAX];
  char *begin, *end, *region;
  const char *type;
  int err;

  snprintf(daxpath, sizeof(daxpath), "/sys/bus/dax/devices/%s", name);
  err = hwloc_readlink(daxpath, link, sizeof(link), fsroot_fd);
  if (err < 0)
    /* this isn't a symlink? we won't be to find what memory this is, ignore */
    return;

  /* usually the link is one of these:
   * ../../../devices/LNXSYSTM:00/LNXSYBUS:00/ACPI0012:00/ndbus0/region2/dax2.0/dax2.0/ for NVDIMMs
   * ../../../devices/platform/e820_pmem/ndbus0/region0/dax0.0/dax0.0/ for fake NVM (memmap=size!start kernel parameter)
   * ../../../devices/platform/hmem.0/dax0.0/ for "soft-reserved" specific-purpose memory
   * ../../../devices/platform/ACPI0017:00/root0/decoder0.0/region0/dax_region0/dax0.0/ for CXL RAM
   * ../../../devices/platform/ACPI0017:00/root0/nvdimm-bridge0/ndbus0/region0/dax0.0/dax0.0/ for CXL PMEM
   */

  /* remove beginning and end of link to populate DAXParent */
  begin = link;
  /* remove the starting ".." (likely multiple) */
  while (!strncmp(begin, "../", 3))
    begin += 3;
  /* remove the starting "devices/" and "platform/" */
  if (!strncmp(begin, "devices/", 8))
    begin += 8;
  if (!strncmp(begin, "platform/", 9))
    begin += 9;
  /* stop at the ending "/daxX.Y" */
  end = strstr(begin, name);
  if (end && end != begin && end[-1] == '/') {
    *end = '\0';
    if (end != begin && end[-1] == '/')
      end[-1] = '\0';
  }

  /* we'll convert SPM (specific-purpose memory) into a HBM subtype later by looking at memattrs */
  type = strstr(begin, "ndbus") ? "NVM" : "SPM";
  hwloc_obj_add_info(obj, "DAXType", type);

  /* try to get some CXL info from the region */
  region = strstr(begin, "/region");
  if (region) {
    unsigned i = strtoul(region+7, &end, 10);
    if (end != region+7)
      annotate_cxl_dax(obj, i, fsroot_fd);
  }

  /* insert DAXParent last because it's likely less useful than others */
  hwloc_obj_add_info(obj, "DAXParent", begin);

  /*
   * Note:
   * "ndbus" or "ndctl" in the path should be enough since these are specifically for NVDIMMs.
   * For additional information about the nv hardware:
   * /sys/class/nd/ndctl%u/device/region%u/mapping%u starts with "nmem%u,"
   * /sys/class/nd/ndctl%u/device/nmem%u/devtype currently contains "nvdimm"
   */
}

static void
annotate_dax_nodes(struct hwloc_topology *topology __hwloc_attribute_unused,
                   unsigned nbnodes, hwloc_obj_t *nodes,
                   int fsroot_fd)
{
  DIR *dir;
  struct dirent *dirent;

  /* try to find DAX devices of KMEM NUMA nodes */
  dir = hwloc_opendir("/sys/bus/dax/devices/",fsroot_fd);
  if (!dir)
    return;

  while ((dirent = readdir(dir)) != NULL) {
    char daxpath[300];
    unsigned target_node, i;
    int tmp;

    if (!dax_is_kmem(dirent->d_name, fsroot_fd))
      continue;

    snprintf(daxpath, sizeof(daxpath), "/sys/bus/dax/devices/%s/target_node", dirent->d_name);
    if (hwloc_read_path_as_int(daxpath, &tmp, fsroot_fd) < 0) /* contains %d when added in 5.1 */
      continue;
    if (tmp < 0)
      /* no NUMA node, ignore this DAX, we cannot know which target_node to annotate */
      continue;
    target_node = (unsigned) tmp;

    /* iterate over NUMA nodes and annotate target_node */
    for(i=0; i<nbnodes; i++) {
      hwloc_obj_t node = nodes[i];
      if (node && node->os_index == target_node) {
        hwloc_obj_add_info(node, "DAXDevice", dirent->d_name);
        annotate_dax_parent(node, dirent->d_name, fsroot_fd);
        break;
      }
    }
  }
  closedir(dir);
}


static unsigned *
list_sysfsnode(struct hwloc_topology *topology,
	       struct hwloc_linux_backend_data_s *data,
	       unsigned *nbnodesp)
{
  DIR *dir;
  unsigned osnode, nbnodes = 0;
  unsigned *indexes, index_;
  hwloc_bitmap_t nodeset;
  struct dirent *dirent;

  /* try to get the list of NUMA nodes at once.
   * otherwise we'll list the entire directory.
   *
   * offline nodes don't exist at all under /sys (they are in "possible", we may ignore them).
   */
  nodeset = hwloc__alloc_read_path_as_cpulist("/sys/devices/system/node/online", data->root_fd);
  if (nodeset) {
    int _nbnodes = hwloc_bitmap_weight(nodeset);
    assert(_nbnodes >= 1);
    nbnodes = (unsigned)_nbnodes;
    hwloc_debug_bitmap("possible NUMA nodes %s\n", nodeset);
    goto found;
  }

  /* Get the list of nodes first */
  dir = hwloc_opendir("/sys/devices/system/node", data->root_fd);
  if (!dir)
    return NULL;

  nodeset = hwloc_bitmap_alloc();
  if (!nodeset) {
    closedir(dir);
    return NULL;
  }

  while ((dirent = readdir(dir)) != NULL) {
    char *end;
    if (strncmp(dirent->d_name, "node", 4))
      continue;
    osnode = strtoul(dirent->d_name+4, &end, 0);
    if (end == dirent->d_name+4)
      continue;
    hwloc_bitmap_set(nodeset, osnode);
    nbnodes++;
  }
  closedir(dir);

  assert(nbnodes >= 1); /* linux cannot have a "node/" subdirectory without at least one "node%d" */

  /* we don't know if sysfs returns nodes in order, we can't merge above and below loops */

 found:
  /* if there are already some nodes, we'll annotate them. make sure the indexes match */
  if (!hwloc_bitmap_iszero(topology->levels[0][0]->nodeset)
      && !hwloc_bitmap_isequal(nodeset, topology->levels[0][0]->nodeset)) {
    char *sn, *tn;
    hwloc_bitmap_asprintf(&sn, nodeset);
    hwloc_bitmap_asprintf(&tn, topology->levels[0][0]->nodeset);
    if (HWLOC_SHOW_CRITICAL_ERRORS())
      fprintf(stderr, "hwloc/linux: ignoring nodes because nodeset %s doesn't match existing nodeset %s.\n", tn, sn);
    free(sn);
    free(tn);
    hwloc_bitmap_free(nodeset);
    return NULL;
  }

  indexes = calloc(nbnodes, sizeof(*indexes));
  if (!indexes) {
    hwloc_bitmap_free(nodeset);
    return NULL;
  }

  /* Unsparsify node indexes.
   * We'll need them later because Linux groups sparse distances
   * and keeps them in order in the sysfs distance files.
   * It'll simplify things in the meantime.
   */
  index_ = 0;
  hwloc_bitmap_foreach_begin (osnode, nodeset) {
    indexes[index_] = osnode;
    index_++;
  } hwloc_bitmap_foreach_end();

  hwloc_bitmap_free(nodeset);

#ifdef HWLOC_DEBUG
  hwloc_debug("%s", "NUMA indexes: ");
  for (index_ = 0; index_ < nbnodes; index_++)
    hwloc_debug(" %u", indexes[index_]);
  hwloc_debug("%s", "\n");
#endif

  *nbnodesp = nbnodes;
  return indexes;
}

static int
annotate_sysfsnode(struct hwloc_topology *topology,
		   struct hwloc_linux_backend_data_s *data,
		   unsigned *found)
{
  unsigned nbnodes;
  hwloc_obj_t * nodes; /* the array of NUMA node objects, to be used for inserting distances */
  hwloc_obj_t node;
  unsigned * indexes;
  uint64_t * distances;
  unsigned i;

  /* NUMA nodes cannot be filtered out */
  indexes = list_sysfsnode(topology, data, &nbnodes);
  if (!indexes)
    return 0;

  nodes = calloc(nbnodes, sizeof(hwloc_obj_t));
  distances = malloc(nbnodes*nbnodes*sizeof(*distances));
  if (NULL == nodes || NULL == distances) {
    free(nodes);
    free(indexes);
    free(distances);
    return 0;
  }

  for(node=hwloc_get_next_obj_by_type(topology, HWLOC_OBJ_NUMANODE, NULL);
      node != NULL;
      node = hwloc_get_next_obj_by_type(topology, HWLOC_OBJ_NUMANODE, node)) {
    assert(node); /* list_sysfsnode() ensured that sysfs nodes and existing nodes match */

    /* hwloc_parse_nodes_distances() requires nodes in physical index order,
     * and inserting distances requires nodes[] and indexes[] in same order.
     */
    for(i=0; i<nbnodes; i++)
      if (indexes[i] == node->os_index) {
	nodes[i] = node;
	break;
      }

    hwloc_get_sysfs_node_meminfo(data, node->os_index, &node->attr->numanode);
  }

  topology->support.discovery->numa = 1;
  topology->support.discovery->numa_memory = 1;
  topology->support.discovery->disallowed_numa = 1;

  if (nbnodes >= 2
      && data->use_numa_distances
      && !hwloc_parse_nodes_distances(nbnodes, indexes, distances, data->root_fd)
      && !(topology->flags & HWLOC_TOPOLOGY_FLAG_NO_DISTANCES)) {
    hwloc_internal_distances_add(topology, "NUMALatency", nbnodes, nodes, distances,
				 HWLOC_DISTANCES_KIND_FROM_OS|HWLOC_DISTANCES_KIND_MEANS_LATENCY,
				 HWLOC_DISTANCES_ADD_FLAG_GROUP);
  } else {
    free(nodes);
    free(distances);
  }

  free(indexes);
  *found = nbnodes;
  return 0;
}

static int
look_sysfsnode(struct hwloc_topology *topology,
	       struct hwloc_linux_backend_data_s *data,
	       unsigned *found)
{
  unsigned osnode;
  unsigned nbnodes;
  hwloc_obj_t * nodes; /* the array of NUMA node objects, to be used for inserting distances */
  unsigned nr_trees;
  hwloc_obj_t * trees; /* the array of memory hierarchies to insert */
  unsigned *indexes;
  uint64_t * distances;
  hwloc_bitmap_t nodes_cpuset;
  unsigned failednodes = 0;
  unsigned i;
  DIR *dir;
  char *env;
  int allow_overlapping_node_cpusets = 0;
  int need_memcaches = hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_MEMCACHE);
  int need_memattrs = !(topology->flags & HWLOC_TOPOLOGY_FLAG_NO_MEMATTRS);

  hwloc_debug("\n\n * Topology extraction from /sys/devices/system/node *\n\n");

  if (data->is_fake_numa_uniform) {
    hwloc_debug("Disabling memory-side caches, memory attributes and HMAT initiators because of fake numa\n");
    need_memcaches = 0;
    need_memattrs = 0;
    data->use_numa_initiators = 0;
    allow_overlapping_node_cpusets = 2; /* accept without warning */
  }

  env = getenv("HWLOC_DEBUG_ALLOW_OVERLAPPING_NODE_CPUSETS");
  if (env) {
    allow_overlapping_node_cpusets = atoi(env); /* 0 drop non-first overlapping nodes, 1 allows with warning, 2 allows without warning */
  }

  /* NUMA nodes cannot be filtered out */
  indexes = list_sysfsnode(topology, data, &nbnodes);
  if (!indexes)
    return 0;

  nodes = calloc(nbnodes, sizeof(hwloc_obj_t));
  trees = calloc(nbnodes, sizeof(hwloc_obj_t));
  distances = malloc(nbnodes*nbnodes*sizeof(*distances));
  nodes_cpuset  = hwloc_bitmap_alloc();
  if (NULL == nodes || NULL == trees || NULL == distances || NULL == nodes_cpuset) {
    free(nodes);
    free(trees);
    free(indexes);
    free(distances);
    hwloc_bitmap_free(nodes_cpuset);
    nbnodes = 0;
    goto out;
  }

  topology->support.discovery->numa = 1;
  topology->support.discovery->numa_memory = 1;
  topology->support.discovery->disallowed_numa = 1;

  /* Create NUMA objects */
  for (i = 0; i < nbnodes; i++) {
    hwloc_obj_t node;
    char nodepath[SYSFS_NUMA_NODE_PATH_LEN];
    hwloc_bitmap_t cpuset;

    osnode = indexes[i];
    sprintf(nodepath, "/sys/devices/system/node/node%u/cpumap", osnode);
    cpuset = hwloc__alloc_read_path_as_cpumask(nodepath, data->root_fd);
    if (!cpuset) {
      /* This NUMA object won't be inserted, we'll ignore distances */
      failednodes++;
      continue;
    }
    if (hwloc_bitmap_intersects(nodes_cpuset, cpuset)) {
      /* Buggy BIOS with overlapping NUMA node cpusets, impossible on Linux so far, we should ignore them.
       * But it may be useful for debugging the core.
       */
      if (!allow_overlapping_node_cpusets) {
	hwloc_debug_1arg_bitmap("node P#%u cpuset %s intersects with previous nodes, ignoring that node.\n", osnode, cpuset);
	hwloc_bitmap_free(cpuset);
	failednodes++;
	continue;
      }
      if (allow_overlapping_node_cpusets < 2 && HWLOC_SHOW_CRITICAL_ERRORS())
        fprintf(stderr, "hwloc/linux: node P#%u cpuset intersects with previous nodes, forcing its acceptance\n", osnode);
    }
    hwloc_bitmap_or(nodes_cpuset, nodes_cpuset, cpuset);

    node = hwloc_alloc_setup_object(topology, HWLOC_OBJ_NUMANODE, osnode);
    node->cpuset = cpuset;
    node->nodeset = hwloc_bitmap_alloc();
    hwloc_bitmap_set(node->nodeset, osnode);
    hwloc_get_sysfs_node_meminfo(data, osnode, &node->attr->numanode);

    nodes[i] = node;
    hwloc_debug_1arg_bitmap("os node %u has cpuset %s\n",
			  osnode, node->cpuset);
  }

      /* try to find NUMA nodes that correspond to NVIDIA GPU memory */
      dir = hwloc_opendir("/proc/driver/nvidia/gpus", data->root_fd);
      if (dir) {
	struct dirent *dirent;
	int keep;
	env = getenv("HWLOC_KEEP_NVIDIA_GPU_NUMA_NODES");
        /* NVIDIA GPU NUMA nodes hidden by default on POWER */
        keep = (data->arch != HWLOC_LINUX_ARCH_POWER);
        if (env)
          keep = atoi(env);
	while ((dirent = readdir(dir)) != NULL) {
	  char nvgpunumapath[300], line[256];
          int err;
	  snprintf(nvgpunumapath, sizeof(nvgpunumapath), "/proc/driver/nvidia/gpus/%s/numa_status", dirent->d_name);
          err = hwloc_read_path_by_length(nvgpunumapath, line, sizeof(line), data->root_fd);
          if (err > 0) {
              const char *nvgpu_node_line = strstr(line, "Node:");
	      if (nvgpu_node_line) {
		unsigned nvgpu_node;
		const char *value = nvgpu_node_line+5;
		while (*value == ' ' || *value == '\t')
		  value++;
		nvgpu_node = atoi(value);
		hwloc_debug("os node %u is NVIDIA GPU %s integrated memory\n", nvgpu_node, dirent->d_name);
		for(i=0; i<nbnodes; i++) {
		  hwloc_obj_t node = nodes[i];
		  if (node && node->os_index == nvgpu_node) {
		    if (keep) {
		      /* keep this NUMA node but fixed its locality and add an info about the GPU */
		      char nvgpulocalcpuspath[300];
		      node->subtype = strdup("GPUMemory");
		      hwloc_obj_add_info(node, "PCIBusID", dirent->d_name);
		      snprintf(nvgpulocalcpuspath, sizeof(nvgpulocalcpuspath), "/sys/bus/pci/devices/%s/local_cpus", dirent->d_name);
		      err = hwloc__read_path_as_cpumask(nvgpulocalcpuspath, node->cpuset, data->root_fd);
		      if (err)
			/* the core will attach to the root */
			hwloc_bitmap_zero(node->cpuset);
		    } else {
		      /* drop this NUMA node */
		      hwloc_free_unlinked_object(node);
		      nodes[i] = NULL;
		    }
		    break;
		  }
		}
	      }
	  }
	}
	closedir(dir);
      }

      annotate_dax_nodes(topology, nbnodes, nodes, data->root_fd);

      topology->support.discovery->numa = 1;
      topology->support.discovery->numa_memory = 1;
      topology->support.discovery->disallowed_numa = 1;

      hwloc_bitmap_free(nodes_cpuset);

      if (nbnodes <= 1) {
	/* failed to read/create some nodes, don't bother reading/fixing
	 * a distance matrix that would likely be wrong anyway.
	 */
	data->use_numa_distances = 0;
      }

      if (!data->use_numa_distances) {
	free(distances);
	distances = NULL;
      }

      if (distances && hwloc_parse_nodes_distances(nbnodes, indexes, distances, data->root_fd) < 0) {
	free(distances);
	distances = NULL;
      }

      free(indexes);

      if (data->is_knl) {
	/* apply KNL quirks */
	int noquirk;
	env = getenv("HWLOC_KNL_NUMA_QUIRK");
        noquirk = (env && !atoi(env));
	if (!noquirk) {
	  hwloc_linux_knl_numa_quirk(topology, data, nodes, nbnodes, distances, &failednodes);
	  free(distances);
	  free(nodes);
	  free(trees);
	  goto out;
	}
      }

      /* Fill the array of trees */
      nr_trees = 0;
      /* First list nodes that have a non-empty cpumap.
       * They are likely the default nodes where we want to allocate from (DDR),
       * make sure they are listed first in their parent memory subtree.
       */
      for (i = 0; i < nbnodes; i++) {
	hwloc_obj_t node = nodes[i];
	if (node && !hwloc_bitmap_iszero(node->cpuset)) {
	  hwloc_obj_t tree;
	  /* update from HMAT initiators if any */
	  if (data->use_numa_initiators)
	    read_node_initiators(data, node, nbnodes, nodes);

	  tree = node;
	  if (need_memcaches)
	    read_node_mscaches(topology, data, &tree);
	  trees[nr_trees++] = tree;
	}
      }
      /* Now look for empty-cpumap nodes.
       * Those may be the non-default nodes for allocation.
       * Hence we don't want them to be listed first,
       * especially if we end up fixing their actual cpumap.
       */
      for (i = 0; i < nbnodes; i++) {
	hwloc_obj_t node = nodes[i];
	if (!node)
          continue;
        if (hwloc_bitmap_iszero(node->cpuset)) {
	  hwloc_obj_t tree;
	  /* update from HMAT initiators if any */
	  if (data->use_numa_initiators)
	    if (!read_node_initiators(data, node, nbnodes, nodes))
	      if (!hwloc_bitmap_iszero(node->cpuset))
		goto fixed;

	  /* if HMAT didn't help, try to find locality of CPU-less NUMA nodes by looking at their distances */
	  if (distances && data->use_numa_distances_for_cpuless)
	    fixup_cpuless_node_locality_from_distances(i, nbnodes, nodes, distances);

	fixed:
	  tree = node;
	  if (need_memcaches)
	    read_node_mscaches(topology, data, &tree);
	  trees[nr_trees++] = tree;
	}
        /* By the way, get their memattrs now that cpuset is fixed */
        if (need_memattrs)
          read_node_local_memattrs(topology, data, node);
      }

      /* insert memory trees for real */
      for (i = 0; i < nr_trees; i++) {
	hwloc_obj_t tree = trees[i];
	while (tree) {
	  hwloc_obj_t cur_obj;
	  hwloc_obj_t res_obj;
	  hwloc_obj_type_t cur_type;
	  cur_obj = tree;
	  cur_type = cur_obj->type;
	  tree = cur_obj->memory_first_child;
	  assert(!cur_obj->next_sibling);
	  res_obj = hwloc__insert_object_by_cpuset(topology, NULL, cur_obj, "linux:sysfs:numa");
	  if (res_obj != cur_obj && cur_type == HWLOC_OBJ_NUMANODE) {
	    /* This NUMA node got merged somehow, could be a buggy BIOS reporting wrong NUMA node cpuset.
	     * Update it in the array for the distance matrix. */
	    unsigned j;
	    for(j=0; j<nbnodes; j++)
	      if (nodes[j] == cur_obj)
		nodes[j] = res_obj;
	    failednodes++;
	  }
	}
      }
      free(trees);

      if (topology->flags & HWLOC_TOPOLOGY_FLAG_NO_DISTANCES) {
        free(distances);
        distances = NULL;
      }

      /* Inserted distances now that nodes are properly inserted */
      if (distances)
	hwloc_internal_distances_add(topology, "NUMALatency", nbnodes, nodes, distances,
				     HWLOC_DISTANCES_KIND_FROM_OS|HWLOC_DISTANCES_KIND_MEANS_LATENCY,
				     HWLOC_DISTANCES_ADD_FLAG_GROUP);
      else
	free(nodes);

 out:
  *found = nbnodes - failednodes;
  return 0;
}


/*************************************
 * sysfs CPU frequencies for cpukinds
 */

struct hwloc_linux_cpukinds_by_pu {
  unsigned pu;
  unsigned long max_freq; /* kHz */
  unsigned long base_freq; /* kHz */
  unsigned long capacity;
  int done; /* temporary bit to identify PU that were processed by the current algorithm
             * (only hwloc_linux_cpukinds_adjust_maxfreqs() for now)
             */
};

struct hwloc_linux_cpukinds {
  struct hwloc_linux_cpukind {
    unsigned long value;
    hwloc_bitmap_t cpuset;
  } *sets;
  unsigned nr_sets, nr_sets_allocated;
};

static void
hwloc_linux_cpukinds_init(struct hwloc_linux_cpukinds *cpukinds)
{
  cpukinds->nr_sets = 0;
  cpukinds->nr_sets_allocated = 4; /* enough for vast majority of cases */
  cpukinds->sets = malloc(cpukinds->nr_sets_allocated * sizeof(*cpukinds->sets));
}

static void
hwloc_linux_cpukinds_add(struct hwloc_linux_cpukinds *cpukinds,
                         unsigned pu, unsigned long value)
{
  unsigned i;

  /* try to add to existing value */
  for(i=0; i<cpukinds->nr_sets; i++) {
    if (cpukinds->sets[i].value == value) {
      hwloc_bitmap_set(cpukinds->sets[i].cpuset, pu);
      return;
    }
  }

  /* do we need to enlarge the array before adding a new value? */
  if (cpukinds->nr_sets == cpukinds->nr_sets_allocated) {
    struct hwloc_linux_cpukind *new = realloc(cpukinds->sets, 2 * cpukinds->nr_sets_allocated * sizeof(*cpukinds->sets));
    if (!new)
      /* failed, ignore this PU */
      return;
    cpukinds->sets = new;
    cpukinds->nr_sets_allocated *= 2;
  }

  /* add a new value for real */
  cpukinds->sets[cpukinds->nr_sets].cpuset = hwloc_bitmap_alloc();
  if (!cpukinds->sets[cpukinds->nr_sets].cpuset)
    /* failed, ignore this PU */
    return;

  cpukinds->sets[cpukinds->nr_sets].value = value;
  hwloc_bitmap_set(cpukinds->sets[cpukinds->nr_sets].cpuset, pu);
  cpukinds->nr_sets++;
}

static int
hwloc_linux_cpukinds_compar(const void *_a, const void *_b)
{
  const struct hwloc_linux_cpukind *a = _a, *b = _b;
  return a->value - b->value;
}

static void
hwloc_linux_cpukinds_register_one(struct hwloc_topology *topology,
                                  hwloc_bitmap_t cpuset,
                                  int efficiency,
                                  char *infoname,
                                  char *infovalue)
{
  struct hwloc_info_s infoattr;
  infoattr.name = infoname;
  infoattr.value = infovalue;
  hwloc_internal_cpukinds_register(topology, cpuset, efficiency, &infoattr, 1, 0);
  /* the cpuset is given to the callee */
}

static void
hwloc_linux_cpukinds_register(struct hwloc_linux_cpukinds *cpukinds,
                              struct hwloc_topology *topology,
                              const char *name,
                              int forced_efficiency)
{
  unsigned i;
  int use_index_for_efficiency = 0;

  /* sort by value, lower frequency and lower capacity likely means lower performance */
  qsort(cpukinds->sets, cpukinds->nr_sets, sizeof(*cpukinds->sets), hwloc_linux_cpukinds_compar);

  /* values likely fit in integers (cpu_capacity is 1024 max as of linux 6.15).
   * otherwise we'll replace them with their index.
   */
  for(i=0; i<cpukinds->nr_sets; i++) {
    if (cpukinds->sets[i].value > INT_MAX) {
      use_index_for_efficiency = 1;
      break;
    }
  }

  for(i=0; i<cpukinds->nr_sets; i++) {
    char value[32];
    unsigned long efficiency = cpukinds->sets[i].value;
    snprintf(value, sizeof(value), "%lu", efficiency);
    hwloc_linux_cpukinds_register_one(topology, cpukinds->sets[i].cpuset,
                                      forced_efficiency ? (use_index_for_efficiency ? (int) i : (int) efficiency) : HWLOC_CPUKIND_EFFICIENCY_UNKNOWN,
                                      (char *) name, value);
    /* the cpuset is given to the callee */
    cpukinds->sets[i].cpuset = NULL;
  }

  if (cpukinds->nr_sets)
    topology->support.discovery->cpukind_efficiency = 1;
}

static void
hwloc_linux_cpukinds_destroy(struct hwloc_linux_cpukinds *cpukinds)
{
  unsigned i;
  for(i=0; i<cpukinds->nr_sets; i++)
    hwloc_bitmap_free(cpukinds->sets[i].cpuset);
  cpukinds->nr_sets = 0;
  cpukinds->nr_sets_allocated = 0;
  free (cpukinds->sets);
}

/* for each set of PUs with the same base frequency,
 * adjust max frequencies by up to adjust_max percents,
 * and uniformize to the min capacity
 */
static void
hwloc_linux_cpukinds_adjust_maxfreqs(unsigned nr_pus,
                                     struct hwloc_linux_cpukinds_by_pu *by_pu,
                                     unsigned adjust_max)
{
  unsigned i, next = 0, done = 0;
  while (done < nr_pus) {
    /* start a new group of same base_frequency at next */
    unsigned first = next;
    unsigned long cur_base_freq = by_pu[first].base_freq;
    unsigned long min_maxfreq = by_pu[first].max_freq;
    unsigned long max_maxfreq = by_pu[first].max_freq;
    unsigned long min_capacity = by_pu[first].capacity;
    unsigned long max_capacity = by_pu[first].capacity;
    by_pu[first].done = 1;
    done++;
    next = 0;
    for(i=first+1; i<nr_pus; i++) {
      if (by_pu[i].done)
        continue;
      if (by_pu[i].base_freq == cur_base_freq) {
        if (by_pu[i].max_freq > max_maxfreq)
          max_maxfreq = by_pu[i].max_freq;
        else if (by_pu[i].max_freq < min_maxfreq)
          min_maxfreq = by_pu[i].max_freq;
        if (by_pu[i].capacity > max_capacity)
          max_capacity = by_pu[i].capacity;
        else if (by_pu[i].capacity < min_capacity)
          min_capacity = by_pu[i].capacity;
        by_pu[i].done = 1;
        done++;
      } else {
        if (!next)
          next = i;
      }
    }

    if (min_maxfreq == max_maxfreq) {
      hwloc_debug("linux/cpufreq: max frequencies always %lu when base=%lu\n",
                  min_maxfreq, cur_base_freq);
      hwloc_debug("linux/cpufreq: %lu <= capacity <= %lu\n",
                  min_capacity, max_capacity);
    } else {
      float ratio = ((float)(max_maxfreq-min_maxfreq)/(float)min_maxfreq);
      hwloc_debug("linux/cpufreq: max frequencies in [%lu-%lu] when base=%lu\n",
                  min_maxfreq, max_maxfreq, cur_base_freq);
      if (ratio*100 < (float)adjust_max) {
        hwloc_debug("linux/cpufreq: max frequencies overrated up to %u%% < %u%%, adjust all to %lu\n",
                    (unsigned)(ratio*100), adjust_max, min_maxfreq);
        /* update max_freq and capacity of all PUs with this base_freq */
        for(i=first; i<nr_pus; i++)
          if (by_pu[i].base_freq == cur_base_freq) {
            by_pu[i].max_freq = min_maxfreq;
            by_pu[i].capacity = min_capacity;
          }
      }
    }
  }
}

static void
hwloc_linux_cpukinds_force_homogeneous(struct hwloc_topology *topology,
                                       unsigned nr_pus,
                                       struct hwloc_linux_cpukinds_by_pu *by_pu)
{
  unsigned i;
  unsigned long base_freq = ULONG_MAX;
  unsigned long max_freq = 0;
  unsigned long capacity = 0;
  for(i=0; i<nr_pus; i++) {
    /* use the lowest base_freq for all cores */
    if (by_pu[i].base_freq && by_pu[i].base_freq < base_freq)
      base_freq = by_pu[i].base_freq;
    /* use the highest max_freq for all cores */
    if (by_pu[i].max_freq > max_freq)
      max_freq = by_pu[i].max_freq;
    /* use the highest capacity for all cores */
    if (by_pu[i].capacity > capacity)
      capacity = by_pu[i].capacity;
  }
  hwloc_debug("linux/cpukinds: forcing homogeneous max_freq %lu base_freq %lu capacity %lu\n",
              max_freq, base_freq, capacity);

  if (max_freq) {
    hwloc_bitmap_t rootset = hwloc_bitmap_dup(topology->levels[0][0]->cpuset);
    if (rootset) {
      char value[64];
      snprintf(value, sizeof(value), "%lu", max_freq/1000);
      hwloc_linux_cpukinds_register_one(topology, rootset,
                                        HWLOC_CPUKIND_EFFICIENCY_UNKNOWN,
                                        (char *) "FrequencyMaxMHz", value);
      /* the cpuset is given to the callee */
    }
  }
  if (base_freq != ULONG_MAX) {
    hwloc_bitmap_t rootset = hwloc_bitmap_dup(topology->levels[0][0]->cpuset);
    if (rootset) {
      char value[64];
      snprintf(value, sizeof(value), "%lu", base_freq/1000);
      hwloc_linux_cpukinds_register_one(topology, rootset,
                                        HWLOC_CPUKIND_EFFICIENCY_UNKNOWN,
                                        (char *) "FrequencyBaseMHz", value);
      /* the cpuset is given to the callee */
    }
  }
  if (capacity) {
    hwloc_bitmap_t rootset = hwloc_bitmap_dup(topology->levels[0][0]->cpuset);
    if (rootset) {
      char value[64];
      snprintf(value, sizeof(value), "%lu", capacity);
      hwloc_linux_cpukinds_register_one(topology, rootset,
                                        HWLOC_CPUKIND_EFFICIENCY_UNKNOWN,
                                        (char *) "LinuxCapacity", value);
      /* the cpuset is given to the callee */
    }
  }
}

static int
look_sysfscpukinds(struct hwloc_topology *topology,
                   struct hwloc_linux_backend_data_s *data)
{
  int enabled = -1; /* not decided yet */
  int nr_pus;
  struct hwloc_linux_cpukinds_by_pu *by_pu;
  struct hwloc_linux_cpukinds cpufreqs_max, cpufreqs_base, cpu_capacity;
  int max_without_basefreq = 0; /* any cpu where we have maxfreq without basefreq? */
  char str[293];
  char *env;
  hwloc_bitmap_t atom_pmu_set, core_pmu_set, lowp_pmu_set;
  int maxfreq_enabled = -1; /* -1 means adjust (default), 0 means ignore, 1 means enforce */
  int use_cppc_nominal_freq = -1; /* -1 means try, 0 no, 1 yes */
  unsigned adjust_max = 10;
  int force_homogeneous;
  const char *info;
  int pu, i;

  env = getenv("HWLOC_LINUX_CPUKINDS");
  if (env) {
    if (!strcmp(env, "none") || !strcmp(env, "0"))
      enabled = 0;
    else {
      /* if variable is given, assume anything else means enabled */
      enabled = 1;
      if (!strncmp(env, "cppc=", 5))
        use_cppc_nominal_freq = atoi(env+5);
    }
  }
  if (enabled == -1 && data->is_amd_homogeneous) {
    /* If not disabled but on pre-Zen5 AMD, disable since useless.
     * This will avoid looking at AMD CPPC which may be slow on Zen2/3 (see #756)
     */
    hwloc_debug("ignoring linux sysfs CPU kind detection on pre-Zen5 AMD CPUs\n");
    enabled = 0;
  }
  if (!enabled)
    return 0;

  env = getenv("HWLOC_CPUKINDS_MAXFREQ");
  if (env) {
    if (!strcmp(env, "0")) {
      maxfreq_enabled = 0;
    } else if (!strcmp(env, "1")) {
      maxfreq_enabled = 1;
    } else if (!strncmp(env, "adjust=", 7)) {
      adjust_max = atoi(env+7);
    }
  }
  if (maxfreq_enabled == 1)
    hwloc_debug("linux/cpufreq: max frequency values are enforced even if it makes CPUs unexpectedly hybrid\n");
  else if (maxfreq_enabled == 0)
    hwloc_debug("linux/cpufreq: max frequency values are ignored\n");
  else
    hwloc_debug("linux/cpufreq: max frequency values will be adjusted by up to %u%%\n",
                adjust_max);

  nr_pus = hwloc_bitmap_weight(topology->levels[0][0]->cpuset);
  assert(nr_pus > 0);
  by_pu = calloc(nr_pus, sizeof(*by_pu));
  if (!by_pu)
    return -1;

  /* gather all sysfs info in the by_pu array */
  i = 0;
  hwloc_bitmap_foreach_begin(pu, topology->levels[0][0]->cpuset) {
    unsigned maxfreq = 0, basefreq = 0, capacity = 0;;
    by_pu[i].pu = pu;

    /* cpuinfo_max_freq is the hardware max. scaling_max_freq is the software policy current max */
    sprintf(str, "/sys/devices/system/cpu/cpu%d/cpufreq/cpuinfo_max_freq", pu);
    if (hwloc_read_path_as_uint(str, &maxfreq, data->root_fd) >= 0)
      by_pu[i].max_freq = maxfreq;
    if (use_cppc_nominal_freq != 1) {
      /* base_frequency is in intel_pstate and works fine */
      sprintf(str, "/sys/devices/system/cpu/cpu%d/cpufreq/base_frequency", pu);
      if (hwloc_read_path_as_uint(str, &basefreq, data->root_fd) >= 0) {
        by_pu[i].base_freq = basefreq;
        use_cppc_nominal_freq = 0;
      }
    }
    /* try acpi_cppc/nominal_freq only if cpufreq/base_frequency failed
     * acpi_cppc/nominal_freq is widely available, but it returns 0 on some Intel SPR,
     * same freq for all cores on RPL,
     * maxfreq for E-cores and LP-E-cores but basefreq for P-cores on MTL.
     */
    if (use_cppc_nominal_freq != 0) {
      sprintf(str, "/sys/devices/system/cpu/cpu%d/acpi_cppc/nominal_freq", pu);
      if (hwloc_read_path_as_uint(str, &basefreq, data->root_fd) >= 0 && basefreq > 0) {
        by_pu[i].base_freq = basefreq * 1000; /* nominal_freq is already in MHz */
        use_cppc_nominal_freq = 1;
      } else {
        use_cppc_nominal_freq = 0;
      }
    }
    if (maxfreq && !basefreq)
      max_without_basefreq = 1;
    /* capacity */
    sprintf(str, "/sys/devices/system/cpu/cpu%d/cpu_capacity", i);
    if (hwloc_read_path_as_uint(str, &capacity, data->root_fd) >= 0)
      by_pu[i].capacity = capacity;
    i++;
  } hwloc_bitmap_foreach_end();
  assert(i == nr_pus);

  /* NVIDIA Grace is homogeneous with slight variations of max frequency, ignore those */
  info = hwloc_obj_get_info_by_name(topology->levels[0][0], "SoC0ID");
  force_homogeneous = info && !strcmp(info, "jep106:036b:0241");
  /* force homogeneity ? */
  env = getenv("HWLOC_CPUKINDS_HOMOGENEOUS");
  if (env)
    force_homogeneous = atoi(env);
  if (force_homogeneous) {
    hwloc_linux_cpukinds_force_homogeneous(topology, (unsigned) nr_pus, by_pu);
    free(by_pu);
    return 0;
  }

  if (maxfreq_enabled == -1 && !max_without_basefreq)
    /* we have basefreq, check maxfreq and ignore/fix it if turboboost 3.0 makes the max different on different cores */
    hwloc_linux_cpukinds_adjust_maxfreqs(nr_pus, by_pu, adjust_max);

  /* now store base+max frequency */
  hwloc_linux_cpukinds_init(&cpufreqs_max);
  hwloc_linux_cpukinds_init(&cpufreqs_base);
  for(i=0; i<nr_pus; i++) {
    if (by_pu[i].max_freq)
      hwloc_linux_cpukinds_add(&cpufreqs_max, by_pu[i].pu, by_pu[i].max_freq/1000);
    if (by_pu[i].base_freq)
      hwloc_linux_cpukinds_add(&cpufreqs_base, by_pu[i].pu, by_pu[i].base_freq/1000);
  }

  if (maxfreq_enabled != 0)
    /* only expose maxfreq info if we miss some basefreq info */
    hwloc_linux_cpukinds_register(&cpufreqs_max, topology, "FrequencyMaxMHz", 0);
  hwloc_linux_cpukinds_register(&cpufreqs_base, topology, "FrequencyBaseMHz", 0);
  hwloc_linux_cpukinds_destroy(&cpufreqs_max);
  hwloc_linux_cpukinds_destroy(&cpufreqs_base);

  /* look at the PU capacity */
  hwloc_linux_cpukinds_init(&cpu_capacity);
  for(i=0; i<nr_pus; i++) {
    if (by_pu[i].capacity)
      hwloc_linux_cpukinds_add(&cpu_capacity, by_pu[i].pu, by_pu[i].capacity);
  }
  hwloc_linux_cpukinds_register(&cpu_capacity, topology, "LinuxCapacity", 1);
  hwloc_linux_cpukinds_destroy(&cpu_capacity);

  free(by_pu);

  /* look at Intel core/atom PMUs */
  atom_pmu_set = hwloc__alloc_read_path_as_cpulist("/sys/devices/cpu_atom/cpus", data->root_fd);
  core_pmu_set = hwloc__alloc_read_path_as_cpulist("/sys/devices/cpu_core/cpus", data->root_fd);
  lowp_pmu_set = hwloc__alloc_read_path_as_cpulist("/sys/devices/cpu_lowpower/cpus", data->root_fd);
  if (atom_pmu_set) {
    hwloc_linux_cpukinds_register_one(topology, atom_pmu_set,
                                      HWLOC_CPUKIND_EFFICIENCY_UNKNOWN,
                                      (char *) "CoreType", (char *) "IntelAtom");
    /* the cpuset is given to the callee */
  } else {
    hwloc_bitmap_free(atom_pmu_set);
  }
  if (core_pmu_set) {
    hwloc_linux_cpukinds_register_one(topology, core_pmu_set,
                                      HWLOC_CPUKIND_EFFICIENCY_UNKNOWN,
                                      (char *) "CoreType", (char *) "IntelCore");
    /* the cpuset is given to the callee */
  } else {
    hwloc_bitmap_free(core_pmu_set);
  }
  if (lowp_pmu_set) {
    hwloc_linux_cpukinds_register_one(topology, lowp_pmu_set,
                                      HWLOC_CPUKIND_EFFICIENCY_UNKNOWN,
                                      (char *) "CoreType", (char *) "IntelLowPower");
    /* the cpuset is given to the callee */
  } else {
    hwloc_bitmap_free(lowp_pmu_set);
  }

  return 0;
}


/**********************************************
 * sysfs CPU discovery
 */

static int
look_sysfscpu(struct hwloc_topology *topology,
	      struct hwloc_linux_backend_data_s *data,
	      int old_filenames,
	      struct hwloc_linux_cpuinfo_proc * cpuinfo_Lprocs, unsigned cpuinfo_numprocs)
{
  hwloc_bitmap_t cpuset; /* Set of cpus for which we have topology information */
  hwloc_bitmap_t online_set; /* Set of online CPUs if easily available, or NULL */
#define CPU_TOPOLOGY_STR_LEN 512
  char str[CPU_TOPOLOGY_STR_LEN];
  DIR *dir;
  int i,j;
  int threadwithcoreid = data->is_amd_with_CU ? -1 : 0; /* -1 means we don't know yet if threads have their own coreids within thread_siblings */
  int dont_merge_cluster_groups;
  const char *env;

  hwloc_debug("\n\n * Topology extraction from /sys/devices/system/cpu/ *\n\n");

  /* try to get the list of online CPUs at once.
   * otherwise we'll use individual per-CPU "online" files.
   */
  online_set = hwloc__alloc_read_path_as_cpulist("/sys/devices/system/cpu/online", data->root_fd);
  if (online_set)
    hwloc_debug_bitmap("online CPUs %s\n", online_set);

  /* fill the cpuset of interesting cpus */
  dir = hwloc_opendir("/sys/devices/system/cpu", data->root_fd);
  if (!dir) {
    hwloc_debug("failed to open sysfscpu path /sys/devices/system/cpu (%d)\n", errno);
    hwloc_bitmap_free(online_set);
    return -1;
  } else {
    struct dirent *dirent;
    cpuset = hwloc_bitmap_alloc();

    while ((dirent = readdir(dir)) != NULL) {
      unsigned long cpu;
      char online[2];
      char *end;

      if (strncmp(dirent->d_name, "cpu", 3))
	continue;
      cpu = strtoul(dirent->d_name+3, &end, 0);
      if (end == dirent->d_name+3)
        continue;

      /* Maybe we don't have topology information but at least it exists */
      hwloc_bitmap_set(topology->levels[0][0]->complete_cpuset, cpu);

      /* check whether this processor is online */
      if (online_set) {
	if (!hwloc_bitmap_isset(online_set, cpu)) {
	  hwloc_debug("os proc %lu is offline\n", cpu);
	  continue;
	}
      } else {
	/* /sys/devices/system/cpu/online unavailable, check the cpu online file */
	sprintf(str, "/sys/devices/system/cpu/cpu%lu/online", cpu);
	if (hwloc_read_path_by_length(str, online, sizeof(online), data->root_fd) > 0) {
	  if (!atoi(online)) {
	    hwloc_debug("os proc %lu is offline\n", cpu);
	    continue;
	  }
	}
      }

      /* check whether the kernel exports topology information for this cpu */
      sprintf(str, "/sys/devices/system/cpu/cpu%lu/topology", cpu);
      if (hwloc_access(str, X_OK, data->root_fd) < 0 && errno == ENOENT) {
	hwloc_debug("os proc %lu has no accessible /sys/devices/system/cpu/cpu%lu/topology\n",
		   cpu, cpu);
	continue;
      }

      hwloc_bitmap_set(cpuset, cpu);
    }
    closedir(dir);
  }

  topology->support.discovery->pu = 1;
  topology->support.discovery->disallowed_pu = 1;
  hwloc_debug_1arg_bitmap("found %d cpu topologies, cpuset %s\n",
	     hwloc_bitmap_weight(cpuset), cpuset);

  env = getenv("HWLOC_DONT_MERGE_CLUSTER_GROUPS");
  dont_merge_cluster_groups = env && atoi(env);

  hwloc_bitmap_foreach_begin(i, cpuset) {
    int tmpint;
    int notfirstofcore = 0; /* set if we have core info and if we're not the first PU of our core */
    int notfirstofcluster = 0; /* set if we have cluster info and if we're not the first PU of our cluster */
    int notfirstofdie = 0; /* set if we have die info and if we're not the first PU of our die */
    hwloc_bitmap_t dieset = NULL;
    hwloc_bitmap_t clusterset = NULL;

    if (hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_CORE)) {
      /* look at the core */
      hwloc_bitmap_t coreset;
      if (old_filenames)
	sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/thread_siblings", i);
      else
	sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/core_cpus", i);
      coreset = hwloc__alloc_read_path_as_cpumask(str, data->root_fd);
      if (coreset) {
        unsigned mycoreid = (unsigned) -1;
	int gotcoreid = 0; /* to avoid reading the coreid twice */
	hwloc_bitmap_and(coreset, coreset, cpuset);
	if (hwloc_bitmap_weight(coreset) > 1 && threadwithcoreid == -1) {
	  /* check if this is hyper-threading or different coreids */
	  unsigned siblingid, siblingcoreid;

	  mycoreid = (unsigned) -1;
	  sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/core_id", i); /* contains %d at least up to 4.19 */
	  if (hwloc_read_path_as_int(str, &tmpint, data->root_fd) == 0)
	    mycoreid = (unsigned) tmpint;
	  gotcoreid = 1;

	  siblingid = hwloc_bitmap_first(coreset);
	  if (siblingid == (unsigned) i)
	    siblingid = hwloc_bitmap_next(coreset, i);
	  siblingcoreid = (unsigned) -1;
	  sprintf(str, "/sys/devices/system/cpu/cpu%u/topology/core_id", siblingid); /* contains %d at least up to 4.19 */
	  if (hwloc_read_path_as_int(str, &tmpint, data->root_fd) == 0)
	    siblingcoreid = (unsigned) tmpint;
	  threadwithcoreid = (siblingcoreid != mycoreid);
	}
	if (hwloc_bitmap_first(coreset) != i)
	  notfirstofcore = notfirstofcluster = notfirstofdie = 1;
	if (!notfirstofcore || threadwithcoreid) {
	  /* regular core */
	  struct hwloc_obj *core;

	  if (!gotcoreid) {
	    mycoreid = (unsigned) -1;
	    sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/core_id", i); /* contains %d at least up to 4.19 */
	    if (hwloc_read_path_as_int(str, &tmpint, data->root_fd) == 0)
	      mycoreid = (unsigned) tmpint;
	  }

	  core = hwloc_alloc_setup_object(topology, HWLOC_OBJ_CORE, mycoreid);
	  if (threadwithcoreid)
	    /* amd multicore compute-unit, create one core per thread */
	    hwloc_bitmap_only(coreset, i);
	  core->cpuset = coreset;
	  hwloc_debug_1arg_bitmap("os core %u has cpuset %s\n",
				  mycoreid, core->cpuset);
	  hwloc__insert_object_by_cpuset(topology, NULL, core, "linux:sysfs:core");
	  coreset = NULL; /* don't free it */
	} else

	hwloc_bitmap_free(coreset);
      }
    }

    if (!notfirstofcore /* don't look at the cluster unless we are the first of the core */
	&& hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_GROUP)) {
      /* look at the cluster */
      sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/cluster_cpus", i);
      clusterset = hwloc__alloc_read_path_as_cpumask(str, data->root_fd);
      if (clusterset) {
	hwloc_bitmap_and(clusterset, clusterset, cpuset);
        if (hwloc_bitmap_weight(clusterset) == 1) {
          /* cluster with single PU, ignore the cluster */
          hwloc_bitmap_free(clusterset);
          clusterset = NULL;
        } else if (hwloc_bitmap_first(clusterset) != i) {
	  /* not first cpu in this cluster, ignore the cluster */
	  hwloc_bitmap_free(clusterset);
	  clusterset = NULL;
	  notfirstofcluster = notfirstofdie = 1;
	}
        /* we don't have coreset anymore for ignoring clusters if equal to cores,
         * the group will be merged by the core.
         */
	/* look at dies and packages before deciding whether we keep that cluster or not */
      }
    }

    if (!notfirstofcluster /* don't look at the die unless we are the first of the core */
	&& hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_DIE)) {
      /* look at the die */
      sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/die_cpus", i);
      dieset = hwloc__alloc_read_path_as_cpumask(str, data->root_fd);
      if (dieset) {
	hwloc_bitmap_and(dieset, dieset, cpuset);
        if (hwloc_bitmap_weight(dieset) == 1) {
          /* die with single PU (non-x86 arch using default die sysfs values), ignore the die */
          hwloc_bitmap_free(dieset);
          dieset = NULL;
        } else if (hwloc_bitmap_first(dieset) != i) {
	  /* not first cpu in this die, ignore the die */
	  hwloc_bitmap_free(dieset);
	  dieset = NULL;
	  notfirstofdie = 1;
	}
	if (clusterset && dieset && hwloc_bitmap_isequal(dieset, clusterset)) {
	  /* cluster is identical to die, ignore it */
	  hwloc_bitmap_free(clusterset);
	  clusterset = NULL;
	}
	/* look at packages before deciding whether we keep that die or not */
      }
    }

    if (!notfirstofdie /* don't look at the package unless we are the first of the die */
	&& hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_PACKAGE)) {
      /* look at the package */
      hwloc_bitmap_t packageset;
      if (old_filenames)
	sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/core_siblings", i);
      else
	sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/package_cpus", i);
      packageset = hwloc__alloc_read_path_as_cpumask(str, data->root_fd);
      if (packageset) {
	hwloc_bitmap_and(packageset, packageset, cpuset);
	if (clusterset && hwloc_bitmap_isequal(packageset, clusterset)) {
	  /* cluster is identical to package, ignore it */
	  hwloc_bitmap_free(clusterset);
	  clusterset = NULL;
	}
	if (hwloc_bitmap_first(packageset) == i) {
	  /* first cpu in this package, add the package */
	  struct hwloc_obj *package;
	  unsigned mypackageid;
	  mypackageid = (unsigned) -1;
	  sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/physical_package_id", i); /* contains %d at least up to 4.19 */
	  if (hwloc_read_path_as_int(str, &tmpint, data->root_fd) == 0)
	    mypackageid = (unsigned) tmpint;

	  package = hwloc_alloc_setup_object(topology, HWLOC_OBJ_PACKAGE, mypackageid);
	  package->cpuset = packageset;
	  hwloc_debug_1arg_bitmap("os package %u has cpuset %s\n",
				  mypackageid, packageset);
	  /* add cpuinfo */
	  if (cpuinfo_Lprocs) {
	    for(j=0; j<(int) cpuinfo_numprocs; j++)
	      if ((int) cpuinfo_Lprocs[j].Pproc == i) {
		hwloc__move_infos(&package->infos, &package->infos_count,
				  &cpuinfo_Lprocs[j].infos, &cpuinfo_Lprocs[j].infos_count);
	      }
	  }
	  hwloc__insert_object_by_cpuset(topology, NULL, package, "linux:sysfs:package");
	  packageset = NULL; /* don't free it */
	}
	hwloc_bitmap_free(packageset);
      }
    }

    if (clusterset) {
      struct hwloc_obj *cluster;
      unsigned myclusterid;
      myclusterid = (unsigned) -1;
      sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/cluster_id", i); /* contains %d when added in 5.16 */
      if (hwloc_read_path_as_int(str, &tmpint, data->root_fd) == 0)
	myclusterid = (unsigned) tmpint;

      cluster = hwloc_alloc_setup_object(topology, HWLOC_OBJ_GROUP, myclusterid);
      cluster->cpuset = clusterset;
      cluster->subtype = strdup("Cluster");
      cluster->attr->group.kind = HWLOC_GROUP_KIND_LINUX_CLUSTER;
      cluster->attr->group.dont_merge = dont_merge_cluster_groups;
      hwloc_debug_1arg_bitmap("os cluster %u has cpuset %s\n",
			      myclusterid, clusterset);
      hwloc__insert_object_by_cpuset(topology, NULL, cluster, "linux:sysfs:cluster");
    }

    if (dieset) {
      struct hwloc_obj *die;
      unsigned mydieid;
      mydieid = (unsigned) -1;
      sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/die_id", i); /* contains %d when added in 5.2 */
      if (hwloc_read_path_as_int(str, &tmpint, data->root_fd) == 0)
	mydieid = (unsigned) tmpint;

      die = hwloc_alloc_setup_object(topology, HWLOC_OBJ_DIE, mydieid);
      die->cpuset = dieset;
      hwloc_debug_1arg_bitmap("os die %u has cpuset %s\n",
			      mydieid, dieset);
      hwloc__insert_object_by_cpuset(topology, NULL, die, "linux:sysfs:die");
    }

    if (data->arch == HWLOC_LINUX_ARCH_S390
	&& hwloc_filter_check_keep_object_type(topology, HWLOC_OBJ_GROUP)) {
      /* look at the books */
      hwloc_bitmap_t bookset, drawerset;
      sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/book_siblings", i);
      bookset = hwloc__alloc_read_path_as_cpumask(str, data->root_fd);
      if (bookset) {
	hwloc_bitmap_and(bookset, bookset, cpuset);
	if (hwloc_bitmap_first(bookset) == i) {
	  struct hwloc_obj *book;
	  unsigned mybookid;
	  mybookid = (unsigned) -1;
	  sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/book_id", i); /* contains %d at least up to 4.19 */
	  if (hwloc_read_path_as_int(str, &tmpint, data->root_fd) == 0) {
	    mybookid = (unsigned) tmpint;

	    book = hwloc_alloc_setup_object(topology, HWLOC_OBJ_GROUP, mybookid);
	    book->cpuset = bookset;
	    hwloc_debug_1arg_bitmap("os book %u has cpuset %s\n",
				    mybookid, bookset);
	    book->subtype = strdup("Book");
	    book->attr->group.kind = HWLOC_GROUP_KIND_S390_BOOK;
	    book->attr->group.subkind = 0;
	    hwloc__insert_object_by_cpuset(topology, NULL, book, "linux:sysfs:group:book");
	    bookset = NULL; /* don't free it */
	  }
        }
	hwloc_bitmap_free(bookset);

	sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/drawer_siblings", i);
	drawerset = hwloc__alloc_read_path_as_cpumask(str, data->root_fd);
	if (drawerset) {
	  hwloc_bitmap_and(drawerset, drawerset, cpuset);
	  if (hwloc_bitmap_first(drawerset) == i) {
	    struct hwloc_obj *drawer;
	    unsigned mydrawerid;
	    mydrawerid = (unsigned) -1;
	    sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/drawer_id", i); /* contains %d at least up to 4.19 */
	    if (hwloc_read_path_as_int(str, &tmpint, data->root_fd) == 0) {
	      mydrawerid = (unsigned) tmpint;

	      drawer = hwloc_alloc_setup_object(topology, HWLOC_OBJ_GROUP, mydrawerid);
	      drawer->cpuset = drawerset;
	      hwloc_debug_1arg_bitmap("os drawer %u has cpuset %s\n",
				      mydrawerid, drawerset);
	      drawer->subtype = strdup("Drawer");
	      drawer->attr->group.kind = HWLOC_GROUP_KIND_S390_BOOK;
	      drawer->attr->group.subkind = 1;
	      hwloc__insert_object_by_cpuset(topology, NULL, drawer, "linux:sysfs:group:drawer");
	      drawerset = NULL; /* don't free it */
	    }
	  }
	  hwloc_bitmap_free(drawerset);
	}
      }
    }

    /* PU cannot be filtered-out */
    {
      /* look at the thread */
      hwloc_bitmap_t threadset;
      struct hwloc_obj *thread = hwloc_alloc_setup_object(topology, HWLOC_OBJ_PU, (unsigned) i);
      threadset = hwloc_bitmap_alloc();
      hwloc_bitmap_only(threadset, i);
      thread->cpuset = threadset;
      hwloc_debug_1arg_bitmap("thread %d has cpuset %s\n",
		 i, threadset);
      hwloc__insert_object_by_cpuset(topology, NULL, thread, "linux:sysfs:pu");
    }

    /* look at the caches */
    if (topology->want_some_cpu_caches) {
     for(j=0; j<10; j++) {
      char str2[20]; /* enough for a level number (one digit) or a type (Data/Instruction/Unified) */
      hwloc_bitmap_t cacheset;

      sprintf(str, "/sys/devices/system/cpu/cpu%d/cache/index%d/shared_cpu_map", i, j);
      cacheset = hwloc__alloc_read_path_as_cpumask(str, data->root_fd);
      if (cacheset) {
	if (hwloc_bitmap_iszero(cacheset)) {
	  /* ia64 returning empty L3 and L2i? use the core set instead */
	  hwloc_bitmap_t tmpset;
	  if (old_filenames)
	    sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/thread_siblings", i);
	  else
	    sprintf(str, "/sys/devices/system/cpu/cpu%d/topology/core_cpus", i);
	  tmpset = hwloc__alloc_read_path_as_cpumask(str, data->root_fd);
	  /* only use it if we actually got something */
	  if (tmpset) {
	    hwloc_bitmap_free(cacheset);
	    cacheset = tmpset;
	  }
	}
	hwloc_bitmap_and(cacheset, cacheset, cpuset);

	if (hwloc_bitmap_first(cacheset) == i) {
	  unsigned kB;
	  unsigned linesize;
	  unsigned sets, lines_per_tag;
	  unsigned depth; /* 1 for L1, .... */
	  hwloc_obj_cache_type_t ctype = HWLOC_OBJ_CACHE_UNIFIED; /* default */
	  hwloc_obj_type_t otype;
          unsigned id = HWLOC_UNKNOWN_INDEX;
	  struct hwloc_obj *cache;

	  /* get the cache level depth */
	  sprintf(str, "/sys/devices/system/cpu/cpu%d/cache/index%d/level", i, j); /* contains %u at least up to 4.19 */
	  if (hwloc_read_path_as_uint(str, &depth, data->root_fd) < 0) {
	    hwloc_bitmap_free(cacheset);
	    continue;
	  }

	  /* cache type */
	  sprintf(str, "/sys/devices/system/cpu/cpu%d/cache/index%d/type", i, j);
	  if (hwloc_read_path_by_length(str, str2, sizeof(str2), data->root_fd) > 0) {
	    if (!strncmp(str2, "Data", 4))
	      ctype = HWLOC_OBJ_CACHE_DATA;
	    else if (!strncmp(str2, "Unified", 7))
	      ctype = HWLOC_OBJ_CACHE_UNIFIED;
	    else if (!strncmp(str2, "Instruction", 11))
	      ctype = HWLOC_OBJ_CACHE_INSTRUCTION;
	  }

          /* cache id */
          sprintf(str, "/sys/devices/system/cpu/cpu%d/cache/index%d/id", i, j);
          hwloc_read_path_as_uint(str, &id, data->root_fd);

	  otype = hwloc_cache_type_by_depth_type(depth, ctype);
	  if (otype == HWLOC_OBJ_TYPE_NONE
	      || !hwloc_filter_check_keep_object_type(topology, otype)) {
	    hwloc_bitmap_free(cacheset);
	    continue;
	  }

	  /* FIXME: if Bulldozer/Piledriver, add compute unit Groups when L2/L1i filtered-out */
	  /* FIXME: if KNL, add tile Groups when L2/L1i filtered-out */

	  /* get the cache size */
	  kB = 0;
	  sprintf(str, "/sys/devices/system/cpu/cpu%d/cache/index%d/size", i, j); /* contains %uK at least up to 4.19 */
	  hwloc_read_path_as_uint(str, &kB, data->root_fd);
	  /* KNL reports L3 with size=0 and full cpuset in cpuid.
	   * Let hwloc_linux_try_add_knl_mcdram_cache() detect it better.
	   */
	  if (!kB && otype == HWLOC_OBJ_L3CACHE && data->is_knl) {
	    hwloc_bitmap_free(cacheset);
	    continue;
	  }

	  /* get the line size */
	  linesize = 0;
	  sprintf(str, "/sys/devices/system/cpu/cpu%d/cache/index%d/coherency_line_size", i, j); /* contains %u at least up to 4.19 */
	  hwloc_read_path_as_uint(str, &linesize, data->root_fd);

	  /* get the number of sets and lines per tag.
	   * don't take the associativity directly in "ways_of_associativity" because
	   * some archs (ia64, ppc) put 0 there when fully-associative, while others (x86) put something like -1 there.
	   */
	  sets = 0;
	  sprintf(str, "/sys/devices/system/cpu/cpu%d/cache/index%d/number_of_sets", i, j); /* contains %u at least up to 4.19 */
	  hwloc_read_path_as_uint(str, &sets, data->root_fd);

	  lines_per_tag = 1;
	  sprintf(str, "/sys/devices/system/cpu/cpu%d/cache/index%d/physical_line_partition", i, j); /* contains %u at least up to 4.19 */
	  hwloc_read_path_as_uint(str, &lines_per_tag, data->root_fd);

	  /* first cpu in this cache, add the cache */
	  cache = hwloc_alloc_setup_object(topology, otype, id);
	  cache->attr->cache.size = ((uint64_t)kB) << 10;
	  cache->attr->cache.depth = depth;
	  cache->attr->cache.linesize = linesize;
	  cache->attr->cache.type = ctype;
	  if (!linesize || !lines_per_tag || !sets)
	    cache->attr->cache.associativity = 0; /* unknown */
	  else if (sets == 1)
	    cache->attr->cache.associativity = 0; /* likely wrong, make it unknown */
	  else
	    cache->attr->cache.associativity = (kB << 10) / linesize / lines_per_tag / sets;
	  cache->cpuset = cacheset;
	  hwloc_debug_1arg_bitmap("cache depth %u has cpuset %s\n",
				  depth, cacheset);
	  hwloc__insert_object_by_cpuset(topology, NULL, cache, "linux:sysfs:cache");
	  cacheset = NULL; /* don't free it */
	}
      }
      hwloc_bitmap_free(cacheset);
     }
    }

  } hwloc_bitmap_foreach_end();

  hwloc_bitmap_free(cpuset);
  hwloc_bitmap_free(online_set);

  return 0;
}



/****************************************
 ****** cpuinfo Topology Discovery ******
 ****************************************/

static int
hwloc_linux_parse_cpuinfo_x86(const char *prefix, const char *value,
			      struct hwloc_info_s **infos, unsigned *infos_count,
			      int is_global __hwloc_attribute_unused)
{
  if (!strcmp("vendor_id", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUVendor", value);
  } else if (!strcmp("model name", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUModel", value);
  } else if (!strcmp("model", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUModelNumber", value);
  } else if (!strcmp("cpu family", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUFamilyNumber", value);
  } else if (!strcmp("stepping", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUStepping", value);
  }
  return 0;
}

static int
hwloc_linux_parse_cpuinfo_ia64(const char *prefix, const char *value,
			       struct hwloc_info_s **infos, unsigned *infos_count,
			       int is_global __hwloc_attribute_unused)
{
  if (!strcmp("vendor", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUVendor", value);
  } else if (!strcmp("model name", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUModel", value);
  } else if (!strcmp("model", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUModelNumber", value);
  } else if (!strcmp("family", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUFamilyNumber", value);
  }
  return 0;
}

static int
hwloc_linux_parse_cpuinfo_arm(const char *prefix, const char *value,
			      struct hwloc_info_s **infos, unsigned *infos_count,
			      int is_global __hwloc_attribute_unused)
{
  if (!strcmp("Processor", prefix) /* old kernels with one Processor header */
      || !strcmp("model name", prefix) /* new kernels with one model name per core */) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUModel", value);
  } else if (!strcmp("CPU implementer", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUImplementer", value);
  } else if (!strcmp("CPU architecture", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUArchitecture", value);
  } else if (!strcmp("CPU variant", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUVariant", value);
  } else if (!strcmp("CPU part", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUPart", value);
  } else if (!strcmp("CPU revision", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPURevision", value);
  } else if (!strcmp("Hardware", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "HardwareName", value);
  } else if (!strcmp("Revision", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "HardwareRevision", value);
  } else if (!strcmp("Serial", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "HardwareSerial", value);
  }
  return 0;
}

static int
hwloc_linux_parse_cpuinfo_ppc(const char *prefix, const char *value,
			      struct hwloc_info_s **infos, unsigned *infos_count,
			      int is_global)
{
  /* common fields */
  if (!strcmp("cpu", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUModel", value);
  } else if (!strcmp("platform", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "PlatformName", value);
  } else if (!strcmp("model", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "PlatformModel", value);
  }
  /* platform-specific fields */
  else if (!strcasecmp("vendor", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "PlatformVendor", value);
  } else if (!strcmp("Board ID", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "PlatformBoardID", value);
  } else if (!strcmp("Board", prefix)
	     || !strcasecmp("Machine", prefix)) {
    /* machine and board are similar (and often more precise) than model above */
    if (value[0])
      hwloc__add_info_nodup(infos, infos_count, "PlatformModel", value, 1);
  } else if (!strcasecmp("Revision", prefix)
	     || !strcmp("Hardware rev", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, is_global ? "PlatformRevision" : "CPURevision", value);
  } else if (!strcmp("SVR", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "SystemVersionRegister", value);
  } else if (!strcmp("PVR", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "ProcessorVersionRegister", value);
  }
  /* don't match 'board*' because there's also "board l2" on some platforms */
  return 0;
}

static int
hwloc_linux_parse_cpuinfo_loongarch(const char *prefix, const char *value,
                                    struct hwloc_info_s **infos, unsigned *infos_count,
                                    int is_global __hwloc_attribute_unused)
{
  if (!strcmp("Model Name", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUModel", value);
  } else if (!strcmp("CPU Family", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPUFamily", value);
  } else if (!strcmp("CPU Revision", prefix)) {
    if (value[0])
      hwloc__add_info(infos, infos_count, "CPURevision", value);
  }
  return 0;
}

/*
 * avr32: "chip type\t:"			=> OK
 * blackfin: "model name\t:"			=> OK
 * h8300: "CPU:"				=> OK
 * m68k: "CPU:"					=> OK
 * mips: "cpu model\t\t:"			=> OK
 * openrisc: "CPU:"				=> OK
 * sparc: "cpu\t\t:"				=> OK
 * tile: "model name\t:"			=> OK
 * unicore32: "Processor\t:"			=> OK
 * alpha: "cpu\t\t\t: Alpha" + "cpu model\t\t:"	=> "cpu" overwritten by "cpu model", no processor indexes
 * cris: "cpu\t\t:" + "cpu model\t:"		=> only "cpu"
 * frv: "CPU-Core:" + "CPU:"			=> only "CPU"
 * mn10300: "cpu core   :" + "model name :"	=> only "model name"
 * parisc: "cpu family\t:" + "cpu\t\t:"		=> only "cpu"
 *
 * not supported because of conflicts with other arch minor lines:
 * m32r: "cpu family\t:"			=> KO (adding "cpu family" would break "blackfin")
 * microblaze: "CPU-Family:"			=> KO
 * sh: "cpu family\t:" + "cpu type\t:"		=> KO
 * xtensa: "model\t\t:"				=> KO
 */
static int
hwloc_linux_parse_cpuinfo_generic(const char *prefix, const char *value,
				  struct hwloc_info_s **infos, unsigned *infos_count,
				  int is_global __hwloc_attribute_unused)
{
  if (!strcmp("model name", prefix)
      || !strcmp("Processor", prefix)
      || !strcmp("chip type", prefix)
      || !strcmp("cpu model", prefix)
      || !strcasecmp("cpu", prefix)) {
    /* keep the last one, assume it's more precise than the first one.
     * we should have the Architecture keypair for basic information anyway.
     */
    if (value[0])
      hwloc__add_info_nodup(infos, infos_count, "CPUModel", value, 1);
  }
  return 0;
}

/* Lprocs_p set to NULL unless returns > 0 */
static int
hwloc_linux_parse_cpuinfo(struct hwloc_linux_backend_data_s *data,
			  const char *path,
			  struct hwloc_linux_cpuinfo_proc ** Lprocs_p,
			  struct hwloc_info_s **global_infos, unsigned *global_infos_count)
{
  /* FIXME: only parse once per package and once for globals? */
  FILE *fd;
  char str[128]; /* vendor/model can be very long */
  char *endptr;
  unsigned allocated_Lprocs = 0;
  struct hwloc_linux_cpuinfo_proc * Lprocs = NULL;
  unsigned numprocs = 0;
  int curproc = -1;
  int (*parse_cpuinfo_func)(const char *, const char *, struct hwloc_info_s **, unsigned *, int) = NULL;

  if (!(fd=hwloc_fopen(path,"r", data->root_fd)))
    {
      hwloc_debug("could not open %s\n", path);
      return -1;
    }

  /* architecture specific or default routine for parsing cpumodel */
  switch (data->arch) {
  case HWLOC_LINUX_ARCH_X86:
    parse_cpuinfo_func = hwloc_linux_parse_cpuinfo_x86;
    break;
  case HWLOC_LINUX_ARCH_ARM:
    parse_cpuinfo_func = hwloc_linux_parse_cpuinfo_arm;
    break;
  case HWLOC_LINUX_ARCH_POWER:
    parse_cpuinfo_func = hwloc_linux_parse_cpuinfo_ppc;
    break;
  case HWLOC_LINUX_ARCH_IA64:
    parse_cpuinfo_func = hwloc_linux_parse_cpuinfo_ia64;
    break;
  case HWLOC_LINUX_ARCH_LOONGARCH:
    parse_cpuinfo_func = hwloc_linux_parse_cpuinfo_loongarch;
    break;
  default:
    parse_cpuinfo_func = hwloc_linux_parse_cpuinfo_generic;
  }

#      define PROCESSOR	"processor"
  hwloc_debug("\n\n * Topology extraction from %s *\n\n", path);
  while (fgets(str, sizeof(str), fd)!=NULL) {
    unsigned long Pproc;
    char *end, *dot, *prefix, *value;
    int noend = 0;

    /* remove the ending \n */
    end = strchr(str, '\n');
    if (end)
      *end = 0;
    else
      noend = 1;
    /* if empty line, skip and reset curproc */
    if (!*str) {
      curproc = -1;
      continue;
    }
    /* skip lines with no dot */
    dot = strchr(str, ':');
    if (!dot)
      continue;
    /* skip lines not starting with a letter */
    if ((*str > 'z' || *str < 'a')
	&& (*str > 'Z' || *str < 'A'))
      continue;

    /* mark the end of the prefix */
    prefix = str;
    end = dot;
    while (end[-1] == ' ' || end[-1] == '\t') end--; /* need a strrspn() */
    *end = 0;
    /* find beginning of value, its end is already marked */
    value = dot+1 + strspn(dot+1, " \t");

    /* defines for parsing numbers */
#   define getprocnb_begin(field, var)					\
    if (!strcmp(field,prefix)) {					\
      var = strtoul(value,&endptr,0);					\
      if (endptr==value) {						\
	hwloc_debug("no number in "field" field of %s\n", path);	\
	goto err;							\
      } else if (var==ULONG_MAX) {					\
	hwloc_debug("too big "field" number in %s\n", path); 		\
	goto err;							\
      }									\
      hwloc_debug(field " %lu\n", var)
#   define getprocnb_end()						\
    }
    /* actually parse numbers */
    getprocnb_begin(PROCESSOR, Pproc);
    curproc = numprocs++;
    if (numprocs > allocated_Lprocs) {
      struct hwloc_linux_cpuinfo_proc * tmp;
      if (!allocated_Lprocs)
	allocated_Lprocs = 8;
      else
        allocated_Lprocs *= 2;
      tmp = realloc(Lprocs, allocated_Lprocs * sizeof(*Lprocs));
      if (!tmp)
	goto err;
      Lprocs = tmp;
    }
    Lprocs[curproc].Pproc = Pproc;
    Lprocs[curproc].infos = NULL;
    Lprocs[curproc].infos_count = 0;
    getprocnb_end() else {

      /* we can't assume that we already got a processor index line:
       * alpha/frv/h8300/m68k/microblaze/sparc have no processor lines at all, only a global entry.
       * tile has a global section with model name before the list of processor lines.
       */
      parse_cpuinfo_func(prefix, value,
			 curproc >= 0 ? &Lprocs[curproc].infos : global_infos,
			 curproc >= 0 ? &Lprocs[curproc].infos_count : global_infos_count,
			 curproc < 0);
    }

    if (noend) {
      /* ignore end of line */
      if (fscanf(fd,"%*[^\n]") == EOF)
	break;
      getc(fd);
    }
  }
  fclose(fd);

  *Lprocs_p = Lprocs;
  return numprocs;

 err:
  fclose(fd);
  free(Lprocs);
  *Lprocs_p = NULL;
  return -1;
}

static void
hwloc_linux_free_cpuinfo(struct hwloc_linux_cpuinfo_proc * Lprocs, unsigned numprocs,
			 struct hwloc_info_s *global_infos, unsigned global_infos_count)
{
  if (Lprocs) {
    unsigned i;
    for(i=0; i<numprocs; i++) {
      hwloc__free_infos(Lprocs[i].infos, Lprocs[i].infos_count);
    }
    free(Lprocs);
  }
  hwloc__free_infos(global_infos, global_infos_count);
}



/*************************************
 ****** Main Topology Discovery ******
 *************************************/

static void
hwloc_gather_system_info(struct hwloc_topology *topology,
			 struct hwloc_linux_backend_data_s *data)
{
  FILE *file;
  char line[128]; /* enough for utsname fields */
  const char *env;

  /* initialize to something sane, in case !is_thissystem and we can't find things in /proc/hwloc-nofile-info */
  memset(&data->utsname, 0, sizeof(data->utsname));
  data->fallback_nbprocessors = -1; /* unknown yet */
  data->pagesize = 4096;

  /* read thissystem info */
  if (topology->is_thissystem) {
    uname(&data->utsname);
    data->fallback_nbprocessors = hwloc_fallback_nbprocessors(0); /* errors managed in hwloc_linux_fallback_pu_level() */
    data->pagesize = hwloc_getpagesize();
  }

  if (!data->is_real_fsroot) {
   /* overwrite with optional /proc/hwloc-nofile-info */
   file = hwloc_fopen("/proc/hwloc-nofile-info", "r", data->root_fd);
   if (file) {
    while (fgets(line, sizeof(line), file)) {
      char *tmp = strchr(line, '\n');
      if (!strncmp("OSName: ", line, 8)) {
	if (tmp)
	  *tmp = '\0';
	strncpy(data->utsname.sysname, line+8, sizeof(data->utsname.sysname));
	data->utsname.sysname[sizeof(data->utsname.sysname)-1] = '\0';
      } else if (!strncmp("OSRelease: ", line, 11)) {
	if (tmp)
	  *tmp = '\0';
	strncpy(data->utsname.release, line+11, sizeof(data->utsname.release));
	data->utsname.release[sizeof(data->utsname.release)-1] = '\0';
      } else if (!strncmp("OSVersion: ", line, 11)) {
	if (tmp)
	  *tmp = '\0';
	strncpy(data->utsname.version, line+11, sizeof(data->utsname.version));
	data->utsname.version[sizeof(data->utsname.version)-1] = '\0';
      } else if (!strncmp("HostName: ", line, 10)) {
	if (tmp)
	  *tmp = '\0';
	strncpy(data->utsname.nodename, line+10, sizeof(data->utsname.nodename));
	data->utsname.nodename[sizeof(data->utsname.nodename)-1] = '\0';
      } else if (!strncmp("Architecture: ", line, 14)) {
	if (tmp)
	  *tmp = '\0';
	strncpy(data->utsname.machine, line+14, sizeof(data->utsname.machine));
	data->utsname.machine[sizeof(data->utsname.machine)-1] = '\0';
      } else if (!strncmp("FallbackNbProcessors: ", line, 22)) {
	if (tmp)
	  *tmp = '\0';
	data->fallback_nbprocessors = atoi(line+22);
      } else if (!strncmp("PageSize: ", line, 10)) {
	if (tmp)
	 *tmp = '\0';
	data->pagesize = strtoull(line+10, NULL, 10);
      } else {
	hwloc_debug("ignored /proc/hwloc-nofile-info line %s\n", line);
	/* ignored */
      }
    }
    fclose(file);
   }
  }

  env = getenv("HWLOC_DUMP_NOFILE_INFO");
  if (env && *env) {
    file = fopen(env, "w");
    if (file) {
      if (*data->utsname.sysname)
	fprintf(file, "OSName: %s\n", data->utsname.sysname);
      if (*data->utsname.release)
	fprintf(file, "OSRelease: %s\n", data->utsname.release);
      if (*data->utsname.version)
	fprintf(file, "OSVersion: %s\n", data->utsname.version);
      if (*data->utsname.nodename)
	fprintf(file, "HostName: %s\n", data->utsname.nodename);
      if (*data->utsname.machine)
	fprintf(file, "Architecture: %s\n", data->utsname.machine);
      fprintf(file, "FallbackNbProcessors: %d\n", data->fallback_nbprocessors);
      fprintf(file, "PageSize: %llu\n", (unsigned long long) data->pagesize);
      fclose(file);
    }
  }

  /* detect arch for quirks, using configure #defines if possible, or uname */
#if (defined HWLOC_X86_32_ARCH) || (defined HWLOC_X86_64_ARCH) /* does not cover KNC */
  if (topology->is_thissystem)
    data->arch = HWLOC_LINUX_ARCH_X86;
#endif
  if (data->arch == HWLOC_LINUX_ARCH_UNKNOWN && *data->utsname.machine) {
    if (!strcmp(data->utsname.machine, "x86_64")
	|| (data->utsname.machine[0] == 'i' && !strcmp(data->utsname.machine+2, "86"))
	|| !strcmp(data->utsname.machine, "k1om"))
      data->arch = HWLOC_LINUX_ARCH_X86;
    else if (!strcmp(data->utsname.machine, "aarch64")
             || !strncmp(data->utsname.machine, "arm", 3))
      data->arch = HWLOC_LINUX_ARCH_ARM;
    else if (!strncmp(data->utsname.machine, "ppc", 3)
	     || !strncmp(data->utsname.machine, "power", 5))
      data->arch = HWLOC_LINUX_ARCH_POWER;
    else if (!strncmp(data->utsname.machine, "s390", 4))
      data->arch = HWLOC_LINUX_ARCH_S390;
    else if (!strncmp(data->utsname.machine, "loongarch", 9))
      data->arch = HWLOC_LINUX_ARCH_LOONGARCH;
    else if (!strcmp(data->utsname.machine, "ia64"))
      data->arch = HWLOC_LINUX_ARCH_IA64;
  }
}

/* returns 0 on success, -1 on non-match or error during hardwired load */
static int
hwloc_linux_try_hardwired_cpuinfo(struct hwloc_backend *backend)
{
  struct hwloc_topology *topology = backend->topology;
  struct hwloc_linux_backend_data_s *data = backend->private_data;

  if (getenv("HWLOC_NO_HARDWIRED_TOPOLOGY"))
    return -1;

  if (!strcmp(data->utsname.machine, "s64fx")) {
    char line[128];
    /* Fujistu K-computer, FX10, and FX100 use specific processors
     * whose Linux topology support is broken until 4.1 (acc455cffa75070d55e74fc7802b49edbc080e92and)
     * and existing machines will likely never be fixed by kernel upgrade.
     */

    /* /proc/cpuinfo starts with one of these lines:
     * "cpu             : Fujitsu SPARC64 VIIIfx"
     * "cpu             : Fujitsu SPARC64 XIfx"
     * "cpu             : Fujitsu SPARC64 IXfx"
     */
    if (hwloc_read_path_by_length("/proc/cpuinfo", line, sizeof(line), data->root_fd) <= 0)
      return -1;

    if (strncmp(line, "cpu\t", 4))
      return -1;

    if (strstr(line, "Fujitsu SPARC64 VIIIfx"))
      return hwloc_look_hardwired_fujitsu_k(topology);
    else if (strstr(line, "Fujitsu SPARC64 IXfx"))
      return hwloc_look_hardwired_fujitsu_fx10(topology);
    else if (strstr(line, "FUJITSU SPARC64 XIfx"))
      return hwloc_look_hardwired_fujitsu_fx100(topology);
  }
  return -1;
}

static void hwloc_linux__get_allowed_resources(hwloc_topology_t topology, const char *root_path, int root_fd, char **cpuset_namep)
{
  enum hwloc_linux_cgroup_type_e cgtype;
  char *mntpnt, *cpuset_name = NULL;

  hwloc_find_linux_cgroup_mntpnt(&cgtype, &mntpnt, root_path, root_fd);
  if (mntpnt) {
    cpuset_name = hwloc_read_linux_cgroup_name(root_fd, topology->pid);
    if (cpuset_name) {
      hwloc_admin_disable_set_from_cgroup(root_fd, cgtype, mntpnt, cpuset_name, "cpus", topology->allowed_cpuset);
      hwloc_admin_disable_set_from_cgroup(root_fd, cgtype, mntpnt, cpuset_name, "mems", topology->allowed_nodeset);
    }
    free(mntpnt);
  }
  *cpuset_namep = cpuset_name;
}

static void
hwloc_linux_fallback_pu_level(struct hwloc_backend *backend)
{
  struct hwloc_topology *topology = backend->topology;
  struct hwloc_linux_backend_data_s *data = backend->private_data;

  if (data->fallback_nbprocessors >= 1)
    topology->support.discovery->pu = 1;
  else
    data->fallback_nbprocessors = 1;
  hwloc_setup_pu_level(topology, data->fallback_nbprocessors);
}

static int check_sysfs_cpu_path(int root_fd, int *old_filenames)
{
  unsigned first;
  int err;

  if (!hwloc_access("/sys/devices/system/cpu", R_OK|X_OK, root_fd)) {
    if (!hwloc_access("/sys/devices/system/cpu/cpu0/topology/package_cpus", R_OK, root_fd)
	|| !hwloc_access("/sys/devices/system/cpu/cpu0/topology/core_cpus", R_OK, root_fd)) {
      return 0;
    }

    if (!hwloc_access("/sys/devices/system/cpu/cpu0/topology/core_siblings", R_OK, root_fd)
	|| !hwloc_access("/sys/devices/system/cpu/cpu0/topology/thread_siblings", R_OK, root_fd)) {
      *old_filenames = 1;
      return 0;
    }
  }

  /* cpu0 might be offline, fallback to looking at the first online cpu.
   * online contains comma-separated ranges, just read the first number.
   */
  hwloc_debug("Failed to find sysfs cpu files using cpu0, looking at online CPUs...\n");
  err = hwloc_read_path_as_uint("/sys/devices/system/cpu/online", &first, root_fd);
  if (err) {
    hwloc_debug("Failed to find read /sys/devices/system/cpu/online.\n");
  } else {
    char path[PATH_MAX];
    hwloc_debug("Found CPU#%u as first online CPU\n", first);

    if (!hwloc_access("/sys/devices/system/cpu", R_OK|X_OK, root_fd)) {
      snprintf(path, sizeof(path), "/sys/devices/system/cpu/cpu%u/topology/package_cpus", first);
      if (!hwloc_access(path, R_OK, root_fd))
        return 0;
      snprintf(path, sizeof(path), "/sys/devices/system/cpu/cpu%u/topology/core_cpus", first);
      if (!hwloc_access(path, R_OK, root_fd))
        return 0;

      snprintf(path, sizeof(path), "/sys/devices/system/cpu/cpu%u/topology/core_siblings", first);
      if (!hwloc_access(path, R_OK, root_fd)) {
        *old_filenames = 1;
        return 0;
      }
      snprintf(path, sizeof(path), "/sys/devices/system/cpu/cpu%u/topology/thread_siblings", first);
      if (!hwloc_access(path, R_OK, root_fd)) {
        *old_filenames = 1;
        return 0;
      }
    }
  }

  return -1;
}

static void
hwloc_linuxfs_check_kernel_cmdline(struct hwloc_linux_backend_data_s *data)
{
  FILE *file;
  char cmdline[4096];
  char *fakenuma;

  file = hwloc_fopen("/proc/cmdline", "r", data->root_fd);
  if (!file)
    return;

  cmdline[0] = 0;
  if (!fgets(cmdline, sizeof(cmdline), file))
    goto out;

  fakenuma = strstr(cmdline, "numa=fake=");
  if (fakenuma) {
    /* in fake numa emulation, SLIT is updated but HMAT isn't, hence we need to disable/fix things later */
    unsigned width = 0;
    char type = 0;
    if (sscanf(fakenuma+10, "%u%c", &width, &type) == 2 && type == 'U') {
      /* if <N>U, each node is split in 8 nodes, we can still do things in this case */
      data->is_fake_numa_uniform = width;
    } else {
      /* otherwise fake nodes are created by just dividing the entire RAM,
       * without respecting locality at all
       */
      data->is_fake_numa_uniform = -1;
    }
    hwloc_debug("Found fake numa %d\n", data->is_fake_numa_uniform);
  }

 out:
  fclose(file);
}

static int
hwloc_linuxfs_look_cpu(struct hwloc_backend *backend, struct hwloc_disc_status *dstatus)
{
  /*
   * This backend may be used with topology->is_thissystem set (default)
   * or not (modified fsroot path).
   */

  struct hwloc_topology *topology = backend->topology;
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  unsigned nbnodes;
  char *cpuset_name = NULL;
  struct hwloc_linux_cpuinfo_proc * Lprocs = NULL;
  struct hwloc_info_s *global_infos = NULL;
  unsigned global_infos_count = 0;
  int numprocs;
  int already_pus;
  int already_numanodes;
  int old_siblings_filenames = 0;
  int err;

  /* check whether sysfs contains old or new cpu topology files */
  err = check_sysfs_cpu_path(data->root_fd, &old_siblings_filenames);
  hwloc_debug("Found sysfs cpu files under /sys/devices/system/cpu with %s topology filenames\n",
	      old_siblings_filenames ? "old" : "new");
  if (err < 0) {
    if (HWLOC_SHOW_CRITICAL_ERRORS())
      fprintf(stderr, "hwloc/linux: failed to find sysfs cpu topology directory, aborting linux discovery.\n");
    return -1;
  }

  already_pus = (topology->levels[0][0]->complete_cpuset != NULL
		 && !hwloc_bitmap_iszero(topology->levels[0][0]->complete_cpuset));
  /* if there are PUs, still look at memory information
   * since x86 misses NUMA node information (unless we forced AMD topoext NUMA nodes)
   * memory size.
   */
  already_numanodes = (topology->levels[0][0]->complete_nodeset != NULL
		       && !hwloc_bitmap_iszero(topology->levels[0][0]->complete_nodeset));
  /* if there are already NUMA nodes, we'll just annotate them with memory information,
   * which requires the NUMA level to be connected.
   */
  if (already_numanodes)
    hwloc__reconnect(topology, 0);

  hwloc_alloc_root_sets(topology->levels[0][0]);

  /*********************************
   * Platform information for later
   */
  hwloc_gather_system_info(topology, data);
  /* soc info needed for cpukinds quirks in look_sysfscpukinds() */
  hwloc__get_soc_info(data, topology->levels[0][0]);

  /**********************************
   * Detect things in /proc/cmdline
   */
  hwloc_linuxfs_check_kernel_cmdline(data);

  /**********************
   * /proc/cpuinfo
   */
  numprocs = hwloc_linux_parse_cpuinfo(data, "/proc/cpuinfo", &Lprocs, &global_infos, &global_infos_count);
  if (numprocs < 0)
    numprocs = 0;

  /**************************
   * detect model for quirks
   */
  if (data->arch == HWLOC_LINUX_ARCH_X86 && numprocs > 0) {
      unsigned i;
      const char *cpuvendor = NULL, *cpufamilynumber = NULL, *cpumodelnumber = NULL;
      for(i=0; i<Lprocs[0].infos_count; i++) {
	if (!strcmp(Lprocs[0].infos[i].name, "CPUVendor")) {
	  cpuvendor = Lprocs[0].infos[i].value;
	} else if (!strcmp(Lprocs[0].infos[i].name, "CPUFamilyNumber")) {
	  cpufamilynumber = Lprocs[0].infos[i].value;
	} else if (!strcmp(Lprocs[0].infos[i].name, "CPUModelNumber")) {
	  cpumodelnumber = Lprocs[0].infos[i].value;
	}
      }
      if (cpuvendor && !strcmp(cpuvendor, "GenuineIntel")
	  && cpufamilynumber && !strcmp(cpufamilynumber, "6")
	  && cpumodelnumber && (!strcmp(cpumodelnumber, "87")
	  || !strcmp(cpumodelnumber, "133")))
	data->is_knl = 1;
      if (cpuvendor && !strcmp(cpuvendor, "AuthenticAMD")) {
        if (cpufamilynumber && (!strcmp(cpufamilynumber, "21")
                                || !strcmp(cpufamilynumber, "22")))
          data->is_amd_with_CU = 1;
        else if (cpufamilynumber && (atoi(cpufamilynumber) < 0x1a))
          data->is_amd_homogeneous = 1; /* hybrid CPUs started with Zen5 = family 0x1a */
      }
  }

  /**********************
   * Gather the list of admin-disabled cpus and mems
   */
  if (!(dstatus->flags & HWLOC_DISC_STATUS_FLAG_GOT_ALLOWED_RESOURCES)) {
    hwloc_linux__get_allowed_resources(topology, data->root_path, data->root_fd, &cpuset_name);
    dstatus->flags |= HWLOC_DISC_STATUS_FLAG_GOT_ALLOWED_RESOURCES;
  }

  /**********************
   * CPU information
   */

  /* Don't rediscover CPU resources if already done */
  if (already_pus)
    goto cpudone;

  /* Gather the list of cpus now */
  err = hwloc_linux_try_hardwired_cpuinfo(backend);
  if (!err)
    goto cpudone;

  /* setup root info */
  hwloc__move_infos(&hwloc_get_root_obj(topology)->infos, &hwloc_get_root_obj(topology)->infos_count,
		    &global_infos, &global_infos_count);

  /* sysfs */
  if (look_sysfscpu(topology, data, old_siblings_filenames, Lprocs, numprocs) < 0)
    /* sysfs but we failed to read cpu topology, fallback */
    hwloc_linux_fallback_pu_level(backend);

 cpudone:
  if (!(topology->flags & HWLOC_TOPOLOGY_FLAG_NO_CPUKINDS))
    look_sysfscpukinds(topology, data);

  /*********************
   * Memory information
   */

  /* Get the machine memory attributes */
  hwloc_get_machine_meminfo(data, &topology->machine_memory);

  /* Gather NUMA information if enabled in the kernel. */
  if (!hwloc_access("/sys/devices/system/node", R_OK|X_OK, data->root_fd)) {
    if (hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_NUMANODE) > 0)
      annotate_sysfsnode(topology, data, &nbnodes);
    else
      look_sysfsnode(topology, data, &nbnodes);
  } else
    nbnodes = 0;

  /**********************
   * Misc
   */

  /* Gather DMI info */
  hwloc__get_dmi_id_info(data, topology->levels[0][0]);

  hwloc_obj_add_info(topology->levels[0][0], "Backend", "Linux");
  if (cpuset_name) {
    hwloc_obj_add_info(topology->levels[0][0], "LinuxCgroup", cpuset_name);
    free(cpuset_name);
  }

  /* data->utsname was filled with real uname or \0, we can safely pass it */
  hwloc_add_uname_info(topology, &data->utsname);

  hwloc_linux_free_cpuinfo(Lprocs, numprocs, global_infos, global_infos_count);
  return 0;
}



/****************************************
 ***** Linux PCI backend callbacks ******
 ****************************************/

/*
 * backend callback for retrieving the location of a pci device
 */
static int
hwloc_linux_backend_get_pci_busid_cpuset(struct hwloc_backend *backend,
					 struct hwloc_pcidev_attr_s *busid, hwloc_bitmap_t cpuset)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  char path[256];
  int err;

  snprintf(path, sizeof(path), "/sys/bus/pci/devices/%04x:%02x:%02x.%01x/local_cpus",
	   busid->domain, busid->bus,
	   busid->dev, busid->func);
  err = hwloc__read_path_as_cpumask(path, cpuset, data->root_fd);
  if (!err && !hwloc_bitmap_iszero(cpuset))
    return 0;
  return -1;
}



#ifdef HWLOC_HAVE_LINUXIO

/***********************************
 ******* Linux I/O discovery *******
 ***********************************/

#define HWLOC_LINUXFS_OSDEV_FLAG_FIND_VIRTUAL (1U<<0)
#define HWLOC_LINUXFS_OSDEV_FLAG_FIND_USB (1U<<1)
#define HWLOC_LINUXFS_OSDEV_FLAG_BLOCK_WITH_SECTORS (1U<<2)
#define HWLOC_LINUXFS_OSDEV_FLAG_USE_PARENT_ATTRS (1U<<30) /* DAX devices have some attributes in their parent */
#define HWLOC_LINUXFS_OSDEV_FLAG_UNDER_BUS (1U<<31) /* bus devices are actual hardware devices, while class devices point to hardware devices through the "device" symlink */

static hwloc_obj_t
hwloc_linuxfs_read_osdev_numa_node(struct hwloc_topology *topology, int root_fd,
                                   const char *osdevpath, unsigned osdev_flags)
{
  char path[256];
  int node, err;

  if (!(osdev_flags & HWLOC_LINUXFS_OSDEV_FLAG_UNDER_BUS)) {
    /* class device have numa_node under the actual device pointed by "device" */
    snprintf(path, sizeof(path), "%s/device/numa_node", osdevpath);
    err = hwloc_read_path_as_int(path, &node, root_fd);
    if (!err && node >= 0)
      return hwloc_get_numanode_obj_by_os_index(topology, (unsigned) node);
    return NULL;
  }

  /* bus devices are actual hardware devices, they should have numa_node directly */
  snprintf(path, sizeof(path), "%s/numa_node", osdevpath);
  err = hwloc_read_path_as_int(path, &node, root_fd);
  if (!err && node >= 0)
    return hwloc_get_numanode_obj_by_os_index(topology, (unsigned) node);

  if (osdev_flags & HWLOC_LINUXFS_OSDEV_FLAG_USE_PARENT_ATTRS) {
    /* before 5.5, nvdimm dax had numa_node only in parent */
    snprintf(path, sizeof(path), "%s/../numa_node", osdevpath);
    err = hwloc_read_path_as_int(path, &node, root_fd);
    if (!err && node >= 0)
      return hwloc_get_numanode_obj_by_os_index(topology, (unsigned) node);
  }

  return NULL;
}

static hwloc_obj_t
hwloc_linuxfs_find_osdev_parent(struct hwloc_backend *backend, int root_fd,
				const char *osdevpath, unsigned osdev_flags)
{
  struct hwloc_topology *topology = backend->topology;
  char path[256];
  int foundpci;
  unsigned pcidomain = 0, pcibus = 0, pcidev = 0, pcifunc = 0;
  unsigned _pcidomain, _pcibus, _pcidev, _pcifunc;
  const char *tmp;
  hwloc_obj_t parent;
  int err;

  err = hwloc_readlink(osdevpath, path, sizeof(path), root_fd);
  if (err < 0) {
    /* /sys/class/<class>/<name> is a directory instead of a symlink on old kernels (at least around 2.6.18 and 2.6.25).
     * The link to parse can be found in /sys/class/<class>/<name>/device instead, at least for "/pci..."
     */
    char olddevpath[256];
    snprintf(olddevpath, sizeof(olddevpath), "%s/device", osdevpath);
    err = hwloc_readlink(olddevpath, path, sizeof(path), root_fd);
    if (err < 0)
      return NULL;
  }

  if (!(osdev_flags & HWLOC_LINUXFS_OSDEV_FLAG_FIND_VIRTUAL)) {
    if (strstr(path, "/virtual/"))
      return NULL;
  }

  if (!(osdev_flags & HWLOC_LINUXFS_OSDEV_FLAG_FIND_USB)) {
    if (strstr(path, "/usb"))
      return NULL;
  }

  tmp = strstr(path, "/pci");
  if (!tmp)
    goto nopci;
  tmp = strchr(tmp+4, '/');
  if (!tmp)
    goto nopci;
  tmp++;

  /* iterate through busid to find the last one (previous ones are bridges) */
  foundpci = 0;
 nextpci:
  /* tmp points to a PCI [domain:]bus:device.function */
  if (sscanf(tmp, "%x:%x:%x.%x", &_pcidomain, &_pcibus, &_pcidev, &_pcifunc) == 4) {
    foundpci = 1;
    pcidomain = _pcidomain;
    pcibus = _pcibus;
    pcidev = _pcidev;
    pcifunc = _pcifunc;
    tmp = strchr(tmp+4, ':')+9; /* tmp points to at least 4 digits for domain, then a ':' */
    goto nextpci;
  }
  if (sscanf(tmp, "%x:%x.%x", &_pcibus, &_pcidev, &_pcifunc) == 3) {
    foundpci = 1;
    pcidomain = 0;
    pcibus = _pcibus;
    pcidev = _pcidev;
    pcifunc = _pcifunc;
    tmp += 8;
    goto nextpci;
  }

  if (foundpci) {
    /* attach to a PCI parent or to a normal (non-I/O) parent found by PCI affinity */
    parent = hwloc_pci_find_parent_by_busid(topology, pcidomain, pcibus, pcidev, pcifunc);
    if (parent)
      return parent;
  }

 nopci:
  /* attach directly near the right NUMA node */
  parent = hwloc_linuxfs_read_osdev_numa_node(topology, root_fd, osdevpath, osdev_flags);
  if (parent) {
    /* don't attach I/O under numa node, attach to the same normal parent */
    while (hwloc__obj_type_is_memory(parent->type))
      parent = parent->parent;
    return parent;
  }

  /* don't use local_cpus, it's only available for PCI sysfs device, not for our osdevs */

  /* FIXME: {numa_node,local_cpus} may be missing when the device link points to a subdirectory.
   * For instance, device of scsi blocks may point to foo/ata1/host0/target0:0:0/0:0:0:0/ instead of foo/
   * In such case, we should look for device/../../../../{numa_node,local_cpus} instead of device/{numa_node,local_cpus}
   * Not needed yet since scsi blocks use the PCI locality above.
   */

  /* fallback to the root object */
  return hwloc_get_root_obj(topology);
}

static hwloc_obj_t
hwloc_linux_add_os_device(struct hwloc_backend *backend, struct hwloc_obj *pcidev, hwloc_obj_osdev_type_t type, const char *name)
{
  struct hwloc_topology *topology = backend->topology;
  struct hwloc_obj *obj = hwloc_alloc_setup_object(topology, HWLOC_OBJ_OS_DEVICE, HWLOC_UNKNOWN_INDEX);
  obj->name = strdup(name);
  obj->attr->osdev.type = type;

  hwloc_insert_object_by_parent(topology, pcidev, obj);
  /* insert_object_by_parent() doesn't merge during insert, so obj is still valid */

  return obj;
}

static void
hwloc_linuxfs_block_class_fillinfos(struct hwloc_backend *backend __hwloc_attribute_unused, int root_fd,
				    struct hwloc_obj *obj, const char *osdevpath, unsigned osdev_flags)
{
#ifdef HWLOC_HAVE_LIBUDEV
  struct hwloc_linux_backend_data_s *data = backend->private_data;
#endif
  FILE *file;
  char path[296]; /* osdevpath <= 256 */
  char line[128];
  char vendor[64] = "";
  char model[64] = "";
  char serial[64] = "";
  char revision[64] = "";
  char blocktype[128] = "";
  unsigned sectorsize = 0;
  unsigned major_id, minor_id;
  int is_nvm = 0;
  const char *daxtype;
  char *tmp;

  snprintf(path, sizeof(path), "%s/size", osdevpath);
  if (hwloc_read_path_by_length(path, line, sizeof(line), root_fd) > 0) {
    unsigned long long value = strtoull(line, NULL, 10);
    /* linux always reports size in 512-byte units for blocks, and bytes for dax, we want kB */
    snprintf(line, sizeof(line), "%llu",
	     (osdev_flags & HWLOC_LINUXFS_OSDEV_FLAG_BLOCK_WITH_SECTORS) ? value / 2 : value >> 10);
    hwloc_obj_add_info(obj, "Size", line);
  }

  snprintf(path, sizeof(path), "%s/queue/hw_sector_size", osdevpath);
  if (hwloc_read_path_by_length(path, line, sizeof(line), root_fd) > 0) {
    sectorsize = strtoul(line, NULL, 10);
  }
  if (sectorsize) {
    snprintf(line, sizeof(line), "%u", sectorsize);
    hwloc_obj_add_info(obj, "SectorSize", line);
  }

  if (osdev_flags & HWLOC_LINUXFS_OSDEV_FLAG_UNDER_BUS) {
    /* "bus" devices are the actual hardware devices. */
    if (osdev_flags & HWLOC_LINUXFS_OSDEV_FLAG_USE_PARENT_ATTRS)
      /* some bus devices (DAX) only have devtype in the parent. */
      snprintf(path, sizeof(path), "%s/../devtype", osdevpath);
    else
      /* currently unused since DAX is the only "bus" we look at */
      snprintf(path, sizeof(path), "%s/devtype", osdevpath);
  } else {
    /* "class" devices are not the actual hardware device, we need to follow their "device" symlibk first. */
    snprintf(path, sizeof(path), "%s/device/devtype", osdevpath);
  }
  if (hwloc_read_path_by_length(path, line, sizeof(line), root_fd) > 0) {
    /* non-volatile devices use the following subtypes:
     * nd_namespace_pmem for pmem/raw (/dev/pmemX)
     * nd_btt for pmem/sector (/dev/pmemXs)
     * nd_pfn for pmem/fsdax (/dev/pmemX)
     * nd_dax for pmem/devdax (/dev/daxX) but it's not a block device anyway
     * nd_namespace_blk for blk/raw and blk/sector (/dev/ndblkX) ?
     *
     * Note that device/sector_size in btt devices includes integrity metadata
     * (512/4096 block + 0/N) while queue/hw_sector_size above is the user sectorsize
     * without metadata.
     */
    if (!strncmp(line, "nd_", 3))
      is_nvm = 1;
  }

  snprintf(path, sizeof(path), "%s/dev", osdevpath);
  if (hwloc_read_path_by_length(path, line, sizeof(line), root_fd) <= 0)
    goto done;
  if (sscanf(line, "%u:%u", &major_id, &minor_id) != 2)
    goto done;
  tmp = strchr(line, '\n');
  if (tmp)
    *tmp = '\0';
  hwloc_obj_add_info(obj, "LinuxDeviceID", line);

#ifdef HWLOC_HAVE_LIBUDEV
  if (data->udev) {
    struct udev_device *dev;
    const char *prop;
    dev = udev_device_new_from_subsystem_sysname(data->udev, "block", obj->name);
    if (!dev)
      goto done;
    prop = udev_device_get_property_value(dev, "ID_VENDOR");
    if (prop) {
      strncpy(vendor, prop, sizeof(vendor));
      vendor[sizeof(vendor)-1] = '\0';
    }
    prop = udev_device_get_property_value(dev, "ID_MODEL");
    if (prop) {
      strncpy(model, prop, sizeof(model));
      model[sizeof(model)-1] = '\0';
    }
    prop = udev_device_get_property_value(dev, "ID_REVISION");
    if (prop) {
      strncpy(revision, prop, sizeof(revision));
      revision[sizeof(revision)-1] = '\0';
    }
    prop = udev_device_get_property_value(dev, "ID_SERIAL_SHORT");
    if (prop) {
      strncpy(serial, prop, sizeof(serial));
      serial[sizeof(serial)-1] = '\0';
    }
    prop = udev_device_get_property_value(dev, "ID_TYPE");
    if (prop) {
      strncpy(blocktype, prop, sizeof(blocktype));
      blocktype[sizeof(blocktype)-1] = '\0';
    }

    udev_device_unref(dev);
  } else
    /* fallback to reading files, works with any fsroot */
#endif
 {
  snprintf(path, sizeof(path), "/run/udev/data/b%u:%u", major_id, minor_id);
  file = hwloc_fopen(path, "r", root_fd);
  if (!file)
    goto done;

  while (NULL != fgets(line, sizeof(line), file)) {
    tmp = strchr(line, '\n');
    if (tmp)
      *tmp = '\0';
    if (!strncmp(line, "E:ID_VENDOR=", strlen("E:ID_VENDOR="))) {
      strncpy(vendor, line+strlen("E:ID_VENDOR="), sizeof(vendor));
      vendor[sizeof(vendor)-1] = '\0';
    } else if (!strncmp(line, "E:ID_MODEL=", strlen("E:ID_MODEL="))) {
      strncpy(model, line+strlen("E:ID_MODEL="), sizeof(model));
      model[sizeof(model)-1] = '\0';
    } else if (!strncmp(line, "E:ID_REVISION=", strlen("E:ID_REVISION="))) {
      strncpy(revision, line+strlen("E:ID_REVISION="), sizeof(revision));
      revision[sizeof(revision)-1] = '\0';
    } else if (!strncmp(line, "E:ID_SERIAL_SHORT=", strlen("E:ID_SERIAL_SHORT="))) {
      strncpy(serial, line+strlen("E:ID_SERIAL_SHORT="), sizeof(serial));
      serial[sizeof(serial)-1] = '\0';
    } else if (!strncmp(line, "E:ID_TYPE=", strlen("E:ID_TYPE="))) {
      strncpy(blocktype, line+strlen("E:ID_TYPE="), sizeof(blocktype));
      blocktype[sizeof(blocktype)-1] = '\0';
    }
  }
  fclose(file);
 }

 done:
  /* clear fake "ATA" vendor name */
  if (!strcasecmp(vendor, "ATA"))
    *vendor = '\0';
  /* overwrite vendor name from model when possible */
  if (!*vendor) {
    if (!strncasecmp(model, "wd", 2))
      strcpy(vendor, "Western Digital");
    else if (!strncasecmp(model, "st", 2))
      strcpy(vendor, "Seagate");
    else if (!strncasecmp(model, "samsung", 7))
      strcpy(vendor, "Samsung");
    else if (!strncasecmp(model, "sandisk", 7))
      strcpy(vendor, "SanDisk");
    else if (!strncasecmp(model, "toshiba", 7))
      strcpy(vendor, "Toshiba");
  }

  if (*vendor)
    hwloc_obj_add_info(obj, "Vendor", vendor);
  if (*model)
    hwloc_obj_add_info(obj, "Model", model);
  if (*revision)
    hwloc_obj_add_info(obj, "Revision", revision);
  if (*serial)
    hwloc_obj_add_info(obj, "SerialNumber", serial);

  daxtype = hwloc_obj_get_info_by_name(obj, "DAXType");
  if (daxtype)
    obj->subtype = strdup(daxtype); /* SPM or NVM */
  else if (is_nvm)
    obj->subtype = strdup("NVM");
  else if (!strcmp(blocktype, "disk") || !strncmp(obj->name, "nvme", 4))
    obj->subtype = strdup("Disk");
  else if (!strcmp(blocktype, "tape"))
    obj->subtype = strdup("Tape");
  else if (!strcmp(blocktype, "cd") || !strcmp(blocktype, "floppy") || !strcmp(blocktype, "optical"))
    obj->subtype = strdup("Removable Media Device");
  else {
    /* generic, usb mass storage/rbc, usb mass storage/scsi */
  }
}

static int
hwloc_linuxfs_lookup_block_class(struct hwloc_backend *backend, unsigned osdev_flags)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  dir = hwloc_opendir("/sys/class/block", root_fd);
  if (!dir)
    return 0;

  osdev_flags |= HWLOC_LINUXFS_OSDEV_FLAG_BLOCK_WITH_SECTORS; /* uses 512B sectors */

  while ((dirent = readdir(dir)) != NULL) {
    char path[256];
    struct stat stbuf;
    hwloc_obj_t obj, parent;
    int err;

    if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
      continue;

    /* ignore partitions */
    err = snprintf(path, sizeof(path), "/sys/class/block/%s/partition", dirent->d_name);
    if ((size_t) err < sizeof(path)
	&& hwloc_stat(path, &stbuf, root_fd) >= 0)
      continue;

    err = snprintf(path, sizeof(path), "/sys/class/block/%s", dirent->d_name);
    if ((size_t) err >= sizeof(path))
      continue;
    parent = hwloc_linuxfs_find_osdev_parent(backend, root_fd, path, osdev_flags);
    if (!parent)
      continue;

    /* USB device are created here but removed later when USB PCI devices get filtered out
     * (unless WHOLE_IO is enabled).
     */

    obj = hwloc_linux_add_os_device(backend, parent, HWLOC_OBJ_OSDEV_BLOCK, dirent->d_name);

    hwloc_linuxfs_block_class_fillinfos(backend, root_fd, obj, path, osdev_flags);
  }

  closedir(dir);

  return 0;
}

static int
hwloc_linuxfs_lookup_dax_class(struct hwloc_backend *backend, unsigned osdev_flags)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  /* old kernels with /sys/class/dax aren't supported anymore */

  dir = hwloc_opendir("/sys/bus/dax/devices", root_fd);
  if (dir) {
    while ((dirent = readdir(dir)) != NULL) {
      char path[300];
      hwloc_obj_t obj, parent;

      if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
	continue;

      /* ignore kmem-device, those appear as additional NUMA nodes */
      if (dax_is_kmem(dirent->d_name, root_fd))
        continue;

      /* FIXME: target_node could be better than numa_node for finding the locality, but it's not possible yet, see #529 */
      snprintf(path, sizeof(path), "/sys/bus/dax/devices/%s", dirent->d_name);
      parent = hwloc_linuxfs_find_osdev_parent(backend, root_fd, path, osdev_flags | HWLOC_LINUXFS_OSDEV_FLAG_UNDER_BUS | HWLOC_LINUXFS_OSDEV_FLAG_USE_PARENT_ATTRS);
      if (!parent)
	continue;

      obj = hwloc_linux_add_os_device(backend, parent, HWLOC_OBJ_OSDEV_BLOCK, dirent->d_name);

      annotate_dax_parent(obj, dirent->d_name, root_fd);

      hwloc_linuxfs_block_class_fillinfos(backend, root_fd, obj, path, osdev_flags | HWLOC_LINUXFS_OSDEV_FLAG_UNDER_BUS | HWLOC_LINUXFS_OSDEV_FLAG_USE_PARENT_ATTRS);
    }
    closedir(dir);
  }

  return 0;
}

static void
hwloc_linuxfs_net_class_fillinfos(int root_fd,
				  struct hwloc_obj *obj, const char *osdevpath)
{
  struct stat st;
  char path[296]; /* osdevpath <= 256 */
  char address[128];
  int err;
  snprintf(path, sizeof(path), "%s/address", osdevpath);
  if (hwloc_read_path_by_length(path, address, sizeof(address), root_fd) > 0) {
    char *eol = strchr(address, '\n');
    if (eol)
      *eol = 0;
    hwloc_obj_add_info(obj, "Address", address);
  }
  snprintf(path, sizeof(path), "%s/device/infiniband", osdevpath);
  if (!hwloc_stat(path, &st, root_fd)) {
    char hexid[16];
    snprintf(path, sizeof(path), "%s/dev_port", osdevpath);
    err = hwloc_read_path_by_length(path, hexid, sizeof(hexid), root_fd);
    if (err < 0) {
      /* fallback t dev_id for old kernels/drivers */
      snprintf(path, sizeof(path), "%s/dev_id", osdevpath);
      err = hwloc_read_path_by_length(path, hexid, sizeof(hexid), root_fd);
    }
    if (err > 0) {
      char *eoid;
      unsigned long port;
      port = strtoul(hexid, &eoid, 0);
      if (eoid != hexid) {
	char portstr[21];
	snprintf(portstr, sizeof(portstr), "%lu", port+1);
	hwloc_obj_add_info(obj, "Port", portstr);
      }
    }
  }
  if (!strncmp(obj->name, "hs", 2)) {
    /* Cray Cassini HSN for Slingshot networks are Ethernet-based,
     * named "hsnX" or "hsiX" with a "cxi" (and "cxi_user") class.
     */
    snprintf(path, sizeof(path), "%s/device/cxi", osdevpath);
    if (!hwloc_access(path, R_OK|X_OK, root_fd)) {
      obj->subtype = strdup("Slingshot");
    }
  }
}

static int
hwloc_linuxfs_lookup_net_class(struct hwloc_backend *backend, unsigned osdev_flags)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  dir = hwloc_opendir("/sys/class/net", root_fd);
  if (!dir)
    return 0;

  while ((dirent = readdir(dir)) != NULL) {
    char path[256];
    hwloc_obj_t obj, parent;
    int err;

    if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
      continue;

    err = snprintf(path, sizeof(path), "/sys/class/net/%s", dirent->d_name);
    if ((size_t) err >= sizeof(path))
      continue;
    parent = hwloc_linuxfs_find_osdev_parent(backend, root_fd, path, osdev_flags);
    if (!parent)
      continue;

    obj = hwloc_linux_add_os_device(backend, parent, HWLOC_OBJ_OSDEV_NETWORK, dirent->d_name);

    hwloc_linuxfs_net_class_fillinfos(root_fd, obj, path);
  }

  closedir(dir);

  return 0;
}

static void
hwloc_linuxfs_infiniband_class_fillinfos(int root_fd,
					 struct hwloc_obj *obj, const char *osdevpath)
{
  char path[296]; /* osdevpath <= 256 */
  char guidvalue[20];
  unsigned i,j;

  snprintf(path, sizeof(path), "%s/node_guid", osdevpath);
  if (hwloc_read_path_by_length(path, guidvalue, sizeof(guidvalue), root_fd) > 0) {
    size_t len;
    len = strspn(guidvalue, "0123456789abcdefx:");
    guidvalue[len] = '\0';
    hwloc_obj_add_info(obj, "NodeGUID", guidvalue);
  }

  snprintf(path, sizeof(path), "%s/sys_image_guid", osdevpath);
  if (hwloc_read_path_by_length(path, guidvalue, sizeof(guidvalue), root_fd) > 0) {
    size_t len;
    len = strspn(guidvalue, "0123456789abcdefx:");
    guidvalue[len] = '\0';
    hwloc_obj_add_info(obj, "SysImageGUID", guidvalue);
  }

  for(i=1; ; i++) {
    char statevalue[2];
    char lidvalue[11];
    char gidvalue[40];

    snprintf(path, sizeof(path), "%s/ports/%u/state", osdevpath, i);
    if (hwloc_read_path_by_length(path, statevalue, sizeof(statevalue), root_fd) > 0) {
      char statename[32];
      statevalue[1] = '\0'; /* only keep the first byte/digit */
      snprintf(statename, sizeof(statename), "Port%uState", i);
      hwloc_obj_add_info(obj, statename, statevalue);
    } else {
      /* no such port */
      break;
    }

    snprintf(path, sizeof(path), "%s/ports/%u/lid", osdevpath, i);
    if (hwloc_read_path_by_length(path, lidvalue, sizeof(lidvalue), root_fd) > 0) {
      char lidname[32];
      size_t len;
      len = strspn(lidvalue, "0123456789abcdefx");
      lidvalue[len] = '\0';
      snprintf(lidname, sizeof(lidname), "Port%uLID", i);
      hwloc_obj_add_info(obj, lidname, lidvalue);
    }

    snprintf(path, sizeof(path), "%s/ports/%u/lid_mask_count", osdevpath, i);
    if (hwloc_read_path_by_length(path, lidvalue, sizeof(lidvalue), root_fd) > 0) {
      char lidname[32];
      size_t len;
      len = strspn(lidvalue, "0123456789");
      lidvalue[len] = '\0';
      snprintf(lidname, sizeof(lidname), "Port%uLMC", i);
      hwloc_obj_add_info(obj, lidname, lidvalue);
    }

    for(j=0; ; j++) {
      snprintf(path, sizeof(path), "%s/ports/%u/gids/%u", osdevpath, i, j);
      if (hwloc_read_path_by_length(path, gidvalue, sizeof(gidvalue), root_fd) > 0) {
	char gidname[32];
	size_t len;
	len = strspn(gidvalue, "0123456789abcdefx:");
	gidvalue[len] = '\0';
	if (strncmp(gidvalue+20, "0000:0000:0000:0000", 19)) {
	  /* only keep initialized GIDs */
	  snprintf(gidname, sizeof(gidname), "Port%uGID%u", i, j);
	  hwloc_obj_add_info(obj, gidname, gidvalue);
	}
      } else {
	/* no such port */
	break;
      }
    }
  }
}

static int
hwloc_linuxfs_lookup_infiniband_class(struct hwloc_backend *backend, unsigned osdev_flags)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  dir = hwloc_opendir("/sys/class/infiniband", root_fd);
  if (!dir)
    return 0;

  while ((dirent = readdir(dir)) != NULL) {
    char path[256];
    hwloc_obj_t obj, parent;
    int err;

    if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
      continue;

    /* blocklist scif* fake devices */
    if (!strncmp(dirent->d_name, "scif", 4))
      continue;

    err = snprintf(path, sizeof(path), "/sys/class/infiniband/%s", dirent->d_name);
    if ((size_t) err > sizeof(path))
      continue;
    parent = hwloc_linuxfs_find_osdev_parent(backend, root_fd, path, osdev_flags);
    if (!parent)
      continue;

    obj = hwloc_linux_add_os_device(backend, parent, HWLOC_OBJ_OSDEV_OPENFABRICS, dirent->d_name);

    hwloc_linuxfs_infiniband_class_fillinfos(root_fd, obj, path);
  }

  closedir(dir);

  return 0;
}

static void
hwloc_linuxfs_bxi_class_fillinfos(int root_fd,
                                  struct hwloc_obj *obj, const char *osdevpath)
{
  char path[296]; /* osdevpath <= 256 */
  char tmp[64];
  obj->subtype = strdup("BXI");

  snprintf(path, sizeof(path), "%s/uuid", osdevpath);
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    char *end = strchr(tmp, '\n');
    if (end)
      *end = '\0';
    hwloc_obj_add_info(obj, "BXIUUID", tmp);
  }
}

static int
hwloc_linuxfs_lookup_bxi_class(struct hwloc_backend *backend, unsigned osdev_flags)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  dir = hwloc_opendir("/sys/class/bxi", root_fd);
  if (!dir)
    return 0;

  while ((dirent = readdir(dir)) != NULL) {
    char path[256];
    hwloc_obj_t obj, parent;
    int err;

    if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
      continue;

    err = snprintf(path, sizeof(path), "/sys/class/bxi/%s", dirent->d_name);
    if ((size_t) err > sizeof(path))
      continue;
    parent = hwloc_linuxfs_find_osdev_parent(backend, root_fd, path, osdev_flags);
    if (!parent)
      continue;

    obj = hwloc_linux_add_os_device(backend, parent, HWLOC_OBJ_OSDEV_OPENFABRICS, dirent->d_name);

    hwloc_linuxfs_bxi_class_fillinfos(root_fd, obj, path);
  }

  closedir(dir);

  return 0;
}

static void
hwloc_linuxfs_ve_class_fillinfos(int root_fd,
                                 struct hwloc_obj *obj, const char *osdevpath)
{
  char path[296]; /* osdevpath <= 256 */
  char tmp[64];
  unsigned val;
  obj->subtype = strdup("VectorEngine");

  snprintf(path, sizeof(path), "%s/model", osdevpath); /* in GB */
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    char *end = strchr(tmp, '\n');
    if (end)
      *end = '\0';
    hwloc_obj_add_info(obj, "VectorEngineModel", tmp);
  }

  snprintf(path, sizeof(path), "%s/serial", osdevpath); /* in GB */
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    char *end = strchr(tmp, '\n');
    if (end)
      *end = '\0';
    hwloc_obj_add_info(obj, "VectorEngineSerialNumber", tmp);
  }

  snprintf(path, sizeof(path), "%s/partitioning_mode", osdevpath); /* in GB */
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    if (atoi(tmp) > 0)
      hwloc_obj_add_info(obj, "VectorEngineNUMAPartitioned", "1");
  }

  snprintf(path, sizeof(path), "%s/num_of_core", osdevpath);
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    size_t len;
    len = strspn(tmp, "0123456789");
    tmp[len] = '\0';
    hwloc_obj_add_info(obj, "VectorEngineCores", tmp);
  }

  snprintf(path, sizeof(path), "%s/memory_size", osdevpath); /* in GB */
  if (!hwloc_read_path_as_uint(path, &val, root_fd)) {
    snprintf(tmp, sizeof(tmp), "%llu", ((unsigned long long) val) * 1024*1024); /* convert from GB to kB */
    hwloc_obj_add_info(obj, "VectorEngineMemorySize", tmp);
  }
  snprintf(path, sizeof(path), "%s/cache_llc", osdevpath); /* in kB */
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    size_t len;
    len = strspn(tmp, "0123456789");
    tmp[len] = '\0';
    hwloc_obj_add_info(obj, "VectorEngineLLCSize", tmp);
  }
  snprintf(path, sizeof(path), "%s/cache_l2", osdevpath); /* in kB */
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    size_t len;
    len = strspn(tmp, "0123456789");
    tmp[len] = '\0';
    hwloc_obj_add_info(obj, "VectorEngineL2Size", tmp);
  }
  snprintf(path, sizeof(path), "%s/cache_l1d", osdevpath); /* in kB */
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    size_t len;
    len = strspn(tmp, "0123456789");
    tmp[len] = '\0';
    hwloc_obj_add_info(obj, "VectorEngineL1dSize", tmp);
  }
  snprintf(path, sizeof(path), "%s/cache_l1i", osdevpath); /* in kB */
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    size_t len;
    len = strspn(tmp, "0123456789");
    tmp[len] = '\0';
    hwloc_obj_add_info(obj, "VectorEngineL1iSize", tmp);
  }
}

static int
hwloc_linuxfs_lookup_ve_class(struct hwloc_backend *backend, unsigned osdev_flags)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  dir = hwloc_opendir("/sys/class/ve", root_fd);
  if (!dir)
    return 0;

  while ((dirent = readdir(dir)) != NULL) {
    char path[256];
    hwloc_obj_t obj, parent;
    int err;

    if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
      continue;

    err = snprintf(path, sizeof(path), "/sys/class/ve/%s", dirent->d_name);
    if ((size_t) err > sizeof(path))
      continue;
    parent = hwloc_linuxfs_find_osdev_parent(backend, root_fd, path, osdev_flags);
    if (!parent)
      continue;

    obj = hwloc_linux_add_os_device(backend, parent, HWLOC_OBJ_OSDEV_COPROC, dirent->d_name);

    hwloc_linuxfs_ve_class_fillinfos(root_fd, obj, path);
  }

  closedir(dir);

  return 0;
}

static int
hwloc_linuxfs_lookup_drm_class(struct hwloc_backend *backend, unsigned osdev_flags)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  dir = hwloc_opendir("/sys/class/drm", root_fd);
  if (!dir)
    return 0;

  while ((dirent = readdir(dir)) != NULL) {
    char path[256];
    hwloc_obj_t parent;
    struct stat stbuf;
    int err;

    if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
      continue;

    /* only keep main devices, not subdevices for outputs */
    err = snprintf(path, sizeof(path), "/sys/class/drm/%s/dev", dirent->d_name);
    if ((size_t) err < sizeof(path)
	&& hwloc_stat(path, &stbuf, root_fd) < 0)
      continue;

    /* Most drivers expose a card%d device.
     * Some (free?) drivers also expose render%d.
     * Old kernels also have a controlD%d. On recent kernels, it's a symlink to card%d (deprecated?).
     * There can also exist some output-specific files such as card0-DP-1.
     *
     * All these aren't very useful compared to CUDA/OpenCL/...
     * Hence the DRM class is only enabled when KEEP_ALL.
     *
     * FIXME: We might want to filter everything out but card%d.
     * Maybe look at the driver (read the end of /sys/class/drm/<name>/device/driver symlink),
     * to decide whether card%d could be useful (likely not for NVIDIA).
     */

    err = snprintf(path, sizeof(path), "/sys/class/drm/%s", dirent->d_name);
    if ((size_t) err >= sizeof(path))
      continue;
    parent = hwloc_linuxfs_find_osdev_parent(backend, root_fd, path, osdev_flags);
    if (!parent)
      continue;

    hwloc_linux_add_os_device(backend, parent, HWLOC_OBJ_OSDEV_GPU, dirent->d_name);
  }

  closedir(dir);

  return 0;
}

static int
hwloc_linuxfs_lookup_dma_class(struct hwloc_backend *backend, unsigned osdev_flags)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  dir = hwloc_opendir("/sys/class/dma", root_fd);
  if (!dir)
    return 0;

  while ((dirent = readdir(dir)) != NULL) {
    char path[256];
    hwloc_obj_t parent;
    int err;

    if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
      continue;

    err = snprintf(path, sizeof(path), "/sys/class/dma/%s", dirent->d_name);
    if ((size_t) err >= sizeof(path))
      continue;
    parent = hwloc_linuxfs_find_osdev_parent(backend, root_fd, path, osdev_flags);
    if (!parent)
      continue;

    hwloc_linux_add_os_device(backend, parent, HWLOC_OBJ_OSDEV_DMA, dirent->d_name);
  }

  closedir(dir);

  return 0;
}

static void
hwloc_linuxfs_cxlmem_fillinfos(int root_fd,
                               struct hwloc_obj *obj, const char *osdevpath)
{
  char path[310];
  char tmp[64];
  obj->subtype = strdup("CXLMem");

  snprintf(path, sizeof(path), "%s/ram/size", osdevpath);
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    unsigned long long value = strtoull(tmp, NULL, 0);
    if (value)  {
      snprintf(tmp, sizeof(tmp), "%llu", value / 1024);
      hwloc_obj_add_info(obj, "CXLRAMSize", tmp);
    }
  }
  snprintf(path, sizeof(path), "%s/pmem/size", osdevpath);
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    unsigned long long value = strtoull(tmp, NULL, 0);
    if (value)  {
      snprintf(tmp, sizeof(tmp), "%llu", value / 1024);
      hwloc_obj_add_info(obj, "CXLPMEMSize", tmp);
    }
  }

  snprintf(path, sizeof(path), "%s/serial", osdevpath);
  if (hwloc_read_path_by_length(path, tmp, sizeof(tmp), root_fd) > 0) {
    char *end = strchr(tmp, '\n');
    if (end)
      *end = '\0';
    hwloc_obj_add_info(obj, "SerialNumber", tmp);
  }
}

static int
hwloc_linuxfs_lookup_cxlmem(struct hwloc_backend *backend, unsigned osdev_flags)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  dir = hwloc_opendir("/sys/bus/cxl/devices", root_fd);
  if (dir) {
    while ((dirent = readdir(dir)) != NULL) {
      char path[300];
      hwloc_obj_t obj, parent;

      if (strncmp(dirent->d_name, "mem", 3))
	continue;

      snprintf(path, sizeof(path), "/sys/bus/cxl/devices/%s", dirent->d_name);
      parent = hwloc_linuxfs_find_osdev_parent(backend, root_fd, path, osdev_flags | HWLOC_LINUXFS_OSDEV_FLAG_UNDER_BUS | HWLOC_LINUXFS_OSDEV_FLAG_USE_PARENT_ATTRS);
      if (!parent)
	continue;

      obj = hwloc_linux_add_os_device(backend, parent, HWLOC_OBJ_OSDEV_BLOCK, dirent->d_name);

      hwloc_linuxfs_cxlmem_fillinfos(root_fd, obj, path);
    }
    closedir(dir);
  }

  return 0;
}

struct hwloc_firmware_dmi_mem_device_header {
  unsigned char type;
  unsigned char length;
  unsigned char handle[2];
  unsigned char phy_mem_handle[2];
  unsigned char mem_err_handle[2];
  unsigned char tot_width[2];
  unsigned char dat_width[2];
  unsigned char size[2];
  unsigned char ff;
  unsigned char dev_set;
  unsigned char dev_loc_str_num;
  unsigned char bank_loc_str_num;
  unsigned char mem_type;
  unsigned char type_detail[2];
  unsigned char speed[2];
  unsigned char manuf_str_num;
  unsigned char serial_str_num;
  unsigned char asset_tag_str_num;
  unsigned char part_num_str_num;
  /* Here is the end of SMBIOS 2.3 fields (27 bytes),
   * those are required for hwloc.
   * Anything below (SMBIOS 2.6+) is optional for hwloc,
   * we must to check header->length before reading them.
   */
  unsigned char attributes;
  unsigned char extended_size[4];
};

static int check_dmi_entry(const char *buffer)
{
  /* reject empty strings */
  if (!*buffer)
    return 0;
  /* reject strings of spaces (at least Dell use this for empty memory slots) */
  if (strspn(buffer, " ") == strlen(buffer))
    return 0;
  return 1;
}

static const char *dmi_memory_device_form_factor(uint8_t code)
{
  static const char *form_factor[] = {
    "Other", /* 0x01 */
    "Unknown",
    "SIMM",
    "SIP",
    "Chip",
    "DIP",
    "ZIP",
    "Proprietary Card",
    "DIMM",
    "TSOP",
    "Row Of Chips",
    "RIMM",
    "SODIMM",
    "SRIMM",
    "FB-DIMM",
    "Die", /* 0x10 */
    /* updated for SMBIOS 3.7.0 20230721 */
  };

  if (code >= 1 && code <= sizeof(form_factor)/sizeof(form_factor[0]))
    return form_factor[code - 1];
  return NULL; /* return NULL to distinguish unsupported values from the official "Unknown" value above */
}

static const char *dmi_memory_device_type(uint8_t code)
{
  static const char *type[] = {
    "Other", /* 0x01 */
    "Unknown",
    "DRAM",
    "EDRAM",
    "VRAM",
    "SRAM",
    "RAM",
    "ROM",
    "Flash",
    "EEPROM",
    "FEPROM",
    "EPROM",
    "CDRAM",
    "3DRAM",
    "SDRAM",
    "SGRAM",
    "RDRAM",
    "DDR",
    "DDR2",
    "DDR2 FB-DIMM",
    "Reserved",
    "Reserved",
    "Reserved",
    "DDR3",
    "FBD2",
    "DDR4",
    "LPDDR",
    "LPDDR2",
    "LPDDR3",
    "LPDDR4",
    "Logical non-volatile device",
    "HBM",
    "HBM2",
    "DDR5",
    "LPDDR5",
    "HBM3" /* 0x24 */
    /* updated for SMBIOS 3.7.0 20230721 */
  };

  if (code >= 1 && code <= sizeof(type)/sizeof(type[0]))
    return type[code - 1];
  return NULL; /* return NULL to distinguish unsupported values from the official "Unknown" value above */
}

/* SMBIOS structures are stored in little-endian, at least since 2.8.
 * Only used for memory size and extended_size so far.
 */
#define get_smbios_uint16_t(x) htole16(*(uint16_t*)(x))
#define get_smbios_uint32_t(x) htole32(*(uint32_t*)(x))

static int dmi_memory_device_size(char *buffer, size_t len,
                                  const struct hwloc_firmware_dmi_mem_device_header *header)
{
  uint64_t memory_size = 0;
  uint16_t code = get_smbios_uint16_t(header->size);

  if (code == 0xFFFF)
    return -1;

  if (header->length >= offsetof(struct hwloc_firmware_dmi_mem_device_header, extended_size) + sizeof(header->extended_size) && code == 0x7FFF) {
    memory_size = get_smbios_uint32_t(header->extended_size) & 0x7FFFFFFF; /* MiB */
    memory_size <<= 10;
  } else {
    memory_size = code & 0x7FFF;
    if (!(code & 0x8000)) /* MiB (otherwise KiB) */
      memory_size <<= 10;
  }
  snprintf(buffer, len, "%llu", (unsigned long long) memory_size);
  return 0;
}

static int dmi_memory_device_rank(char *buffer, size_t len,
                                  const struct hwloc_firmware_dmi_mem_device_header *header)
{
  uint8_t code;
  if (header->length < offsetof(struct hwloc_firmware_dmi_mem_device_header, attributes) + sizeof(header->attributes))
    return -1;
  code = header->attributes;
  if (!code)
    return -1;
  snprintf(buffer, len, "%u", code & 0x0F);
  return 0;
}

static int
hwloc__get_firmware_dmi_memory_info_one(struct hwloc_topology *topology,
					unsigned idx, const char *path, FILE *fd,
					struct hwloc_firmware_dmi_mem_device_header *header)
{
  unsigned slen;
  char buffer[256]; /* enough for memory device strings, or at least for each of them */
  const char *retbuf;
  unsigned foff; /* offset in raw file */
  unsigned boff; /* offset in buffer read from raw file */
  unsigned i;
  struct hwloc_info_s *infos = NULL;
  unsigned infos_count = 0;
  hwloc_obj_t misc;
  int foundinfo = 0;

  /* start after the header */
  foff = header->length;
  i = 1;
  while (1) {
    /* read one buffer */
    if (fseek(fd, foff, SEEK_SET) < 0)
      break;
    if (!fgets(buffer, sizeof(buffer), fd))
      break;
    /* read string at the beginning of the buffer */
    boff = 0;
    while (1) {
      /* stop on empty string */
      if (!buffer[boff])
        goto done;
      /* stop if this string goes to the end of the buffer */
      slen = strlen(buffer+boff);
      if (boff + slen+1 == sizeof(buffer))
        break;
      /* string didn't get truncated, should be OK */
      if (i == header->manuf_str_num) {
	if (check_dmi_entry(buffer+boff)) {
	  hwloc__add_info(&infos, &infos_count, "Vendor", buffer+boff);
	  foundinfo = 1;
	}
      }	else if (i == header->serial_str_num) {
	if (check_dmi_entry(buffer+boff)) {
	  hwloc__add_info(&infos, &infos_count, "SerialNumber", buffer+boff);
	  foundinfo = 1;
	}
      } else if (i == header->asset_tag_str_num) {
	if (check_dmi_entry(buffer+boff)) {
	  hwloc__add_info(&infos, &infos_count, "AssetTag", buffer+boff);
	  foundinfo = 1;
	}
      } else if (i == header->part_num_str_num) {
	if (check_dmi_entry(buffer+boff)) {
	  hwloc__add_info(&infos, &infos_count, "PartNumber", buffer+boff);
	  foundinfo = 1;
	}
      } else if (i == header->dev_loc_str_num) {
	if (check_dmi_entry(buffer+boff)) {
	  hwloc__add_info(&infos, &infos_count, "DeviceLocation", buffer+boff);
	  /* only a location, not an actual info about the device */
	}
      } else if (i == header->bank_loc_str_num) {
	if (check_dmi_entry(buffer+boff)) {
	  hwloc__add_info(&infos, &infos_count, "BankLocation", buffer+boff);
	  /* only a location, not an actual info about the device */
	}
      } else {
	goto done;
      }
      /* next string in buffer */
      boff += slen+1;
      i++;
    }
    /* couldn't read a single full string from that buffer, we're screwed */
    if (!boff) {
      if (HWLOC_SHOW_CRITICAL_ERRORS())
        fprintf(stderr, "hwloc/linux: hwloc couldn't read a DMI firmware entry #%u in %s\n",
                i, path);
      break;
    }
    /* reread buffer after previous string */
    foff += boff;
  }

done:
  if (!foundinfo) {
    /* found no actual info about the device. if there's only location info, the slot may be empty */
    goto out_with_infos;
  }

  retbuf = dmi_memory_device_form_factor(header->ff);
  if (retbuf)
    hwloc__add_info(&infos, &infos_count, "FormFactor", retbuf);
  retbuf = dmi_memory_device_type(header->mem_type);
  if (retbuf)
    hwloc__add_info(&infos, &infos_count, "Type", retbuf);
  if (!dmi_memory_device_size(buffer, sizeof(buffer), header))
    hwloc__add_info(&infos, &infos_count, "Size", buffer);
  if (!dmi_memory_device_rank(buffer, sizeof(buffer), header))
    hwloc__add_info(&infos, &infos_count, "Rank", buffer);

  misc = hwloc_alloc_setup_object(topology, HWLOC_OBJ_MISC, idx);
  if (!misc)
    goto out_with_infos;

  misc->subtype = strdup("MemoryModule");

  hwloc__move_infos(&misc->infos, &misc->infos_count, &infos, &infos_count);
  /* FIXME: find a way to identify the corresponding NUMA node and attach these objects there.
   * but it means we need to parse DeviceLocation=DIMM_B4 but these vary significantly
   * with the vendor, and it's hard to be 100% sure 'B' is second socket.
   * Examples at http://sourceforge.net/p/edac-utils/code/HEAD/tree/trunk/src/etc/labels.db
   * or https://github.com/grondo/edac-utils/blob/master/src/etc/labels.db
   */
  hwloc_insert_object_by_parent(topology, hwloc_get_root_obj(topology), misc);
  return 1;

 out_with_infos:
  hwloc__free_infos(infos, infos_count);
  return 0;
}

static int
hwloc__get_firmware_dmi_memory_info(struct hwloc_topology *topology,
				    struct hwloc_linux_backend_data_s *data)
{
  char path[128];
  unsigned i;

  for(i=0; ; i++) {
    FILE *fd;
    struct hwloc_firmware_dmi_mem_device_header header;
    int err;

    snprintf(path, sizeof(path), "/sys/firmware/dmi/entries/17-%u/raw", i);
    fd = hwloc_fopen(path, "r", data->root_fd);
    if (!fd)
      break;

    err = fread(&header, sizeof(header), 1, fd);
    if (err != 1) {
      fclose(fd);
      break;
    }

    HWLOC_BUILD_ASSERT(offsetof(struct hwloc_firmware_dmi_mem_device_header, part_num_str_num) + sizeof(header.part_num_str_num) == 27);
    if (header.length < 27) {
      /* invalid, or too old entry/spec that doesn't contain what we need */
      fclose(fd);
      break;
    }

    hwloc__get_firmware_dmi_memory_info_one(topology, i, path, fd, &header);

    fclose(fd);
  }

  return 0;
}

#ifdef HWLOC_HAVE_LINUXPCI

#define HWLOC_PCI_REVISION_ID 0x08
#define HWLOC_PCI_CAP_ID_EXP 0x10
#define HWLOC_PCI_CLASS_NOT_DEFINED 0x0000

static int
hwloc_linuxfs_pci_look_pcidevices(struct hwloc_backend *backend)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  struct hwloc_topology *topology = backend->topology;
  hwloc_obj_t tree = NULL;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  /* We could lookup /sys/devices/pci.../.../busid1/.../busid2 recursively
   * to build the hierarchy of bridges/devices directly.
   * But that would require readdirs in all bridge sysfs subdirectories.
   * Do a single readdir in the linear list in /sys/bus/pci/devices/...
   * and build the hierarchy manually instead.
   */
  dir = hwloc_opendir("/sys/bus/pci/devices/", root_fd);
  if (!dir)
    return 0;

  while ((dirent = readdir(dir)) != NULL) {
#define CONFIG_SPACE_CACHESIZE 256
    unsigned char config_space_cache[CONFIG_SPACE_CACHESIZE+1]; /* one more byte for the ending \0 in hwloc_read_path_by_length() */
    unsigned domain, bus, dev, func;
    unsigned secondary_bus, subordinate_bus;
    unsigned short class_id;
    hwloc_obj_type_t type;
    hwloc_obj_t obj;
    struct hwloc_pcidev_attr_s *attr;
    unsigned offset;
    char path[64];
    char value[16];
    int err;

    if (sscanf(dirent->d_name, "%x:%02x:%02x.%01x", &domain, &bus, &dev, &func) != 4)
      continue;

#ifndef HWLOC_HAVE_32BITS_PCI_DOMAIN
    if (domain > 0xffff) {
      static int warned = 0;
      if (!warned && HWLOC_SHOW_ALL_ERRORS())
	fprintf(stderr, "hwloc/linux: Ignoring PCI device with non-16bit domain.\nPass --enable-32bits-pci-domain to configure to support such devices\n(warning: it would break the library ABI, don't enable unless really needed).\n");
      warned = 1;
      continue;
    }
#endif

    /* initialize the config space in case we fail to read it (missing permissions, etc). */
    memset(config_space_cache, 0xff, CONFIG_SPACE_CACHESIZE);
    err = snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/config", dirent->d_name);
    if ((size_t) err < sizeof(path)) {
      hwloc_read_path_by_length(path, (char *) config_space_cache, sizeof(config_space_cache), root_fd);
      /* we have CONFIG_SPACE_CACHESIZE bytes + the ending \0 */
    }

    class_id = HWLOC_PCI_CLASS_NOT_DEFINED;
    err = snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/class", dirent->d_name);
    if ((size_t) err < sizeof(path)
	&& hwloc_read_path_by_length(path, value, sizeof(value), root_fd) > 0)
      class_id = strtoul(value, NULL, 16) >> 8;

    type = hwloc_pcidisc_check_bridge_type(class_id, config_space_cache);
    /* only HWLOC_OBJ_BRIDGE for bridges to-PCI */
    if (type == HWLOC_OBJ_BRIDGE) {
      /* since 4.13, there's secondary_bus_number and subordinate_bus_number in sysfs,
       * but reading them from the config-space is easy anyway.
       */
      if (hwloc_pcidisc_find_bridge_buses(domain, bus, dev, func,
					  &secondary_bus, &subordinate_bus,
					  config_space_cache) < 0)
	continue;
    }

    /* filtered? */
    if (type == HWLOC_OBJ_PCI_DEVICE) {
      enum hwloc_type_filter_e filter;
      hwloc_topology_get_type_filter(topology, HWLOC_OBJ_PCI_DEVICE, &filter);
      if (filter == HWLOC_TYPE_FILTER_KEEP_NONE)
	continue;
      if (filter == HWLOC_TYPE_FILTER_KEEP_IMPORTANT
	  && !hwloc_filter_check_pcidev_subtype_important(class_id))
	continue;
    } else if (type == HWLOC_OBJ_BRIDGE) {
      enum hwloc_type_filter_e filter;
      hwloc_topology_get_type_filter(topology, HWLOC_OBJ_BRIDGE, &filter);
      if (filter == HWLOC_TYPE_FILTER_KEEP_NONE)
	continue;
      /* HWLOC_TYPE_FILTER_KEEP_IMPORTANT filtered later in the core */
    }

    obj = hwloc_alloc_setup_object(topology, type, HWLOC_UNKNOWN_INDEX);
    if (!obj)
      break;
    attr = &obj->attr->pcidev;

    attr->domain = domain;
    attr->bus = bus;
    attr->dev = dev;
    attr->func = func;

    /* bridge specific attributes */
    if (type == HWLOC_OBJ_BRIDGE) {
      /* assumes this is a Bridge to-PCI */
      struct hwloc_bridge_attr_s *battr = &obj->attr->bridge;
      battr->upstream_type = HWLOC_OBJ_BRIDGE_PCI;
      battr->downstream_type = HWLOC_OBJ_BRIDGE_PCI;
      battr->downstream.pci.domain = domain;
      battr->downstream.pci.secondary_bus = secondary_bus;
      battr->downstream.pci.subordinate_bus = subordinate_bus;
    }

    /* default (unknown) values */
    attr->vendor_id = 0;
    attr->device_id = 0;
    attr->class_id = class_id;
    attr->revision = 0;
    attr->subvendor_id = 0;
    attr->subdevice_id = 0;
    attr->linkspeed = 0;

    err = snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/vendor", dirent->d_name);
    if ((size_t) err < sizeof(path)
	&& hwloc_read_path_by_length(path, value, sizeof(value), root_fd) > 0)
      attr->vendor_id = strtoul(value, NULL, 16);

    err = snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/device", dirent->d_name);
    if ((size_t) err < sizeof(path)
	&& hwloc_read_path_by_length(path, value, sizeof(value), root_fd) > 0)
      attr->device_id = strtoul(value, NULL, 16);

    err = snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/subsystem_vendor", dirent->d_name);
    if ((size_t) err < sizeof(path)
	&& hwloc_read_path_by_length(path, value, sizeof(value), root_fd) > 0)
      attr->subvendor_id = strtoul(value, NULL, 16);

    err = snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/subsystem_device", dirent->d_name);
    if ((size_t) err < sizeof(path)
	&& hwloc_read_path_by_length(path, value, sizeof(value), root_fd) > 0)
      attr->subdevice_id = strtoul(value, NULL, 16);

    /* get the revision */
    attr->revision = config_space_cache[HWLOC_PCI_REVISION_ID];

    /* try to get the link speed */
    offset = hwloc_pcidisc_find_cap(config_space_cache, HWLOC_PCI_CAP_ID_EXP);
    if (offset > 0 && offset + 20 /* size of PCI express block up to link status */ <= CONFIG_SPACE_CACHESIZE) {
      hwloc_pcidisc_find_linkspeed(config_space_cache, offset, &attr->linkspeed);
    } else {
      /* if not available from config-space (extended part is root-only), look in sysfs files added in 4.13 */
      float speed = 0.f;
      unsigned width = 0;
      err = snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/current_link_speed", dirent->d_name);
      if ((size_t) err < sizeof(path)
	  && hwloc_read_path_by_length(path, value, sizeof(value), root_fd) > 0)
	speed = hwloc_linux_pci_link_speed_from_string(value);
      err = snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/current_link_width", dirent->d_name);
      if ((size_t) err < sizeof(path)
	  && hwloc_read_path_by_length(path, value, sizeof(value), root_fd) > 0)
	width = atoi(value);
      attr->linkspeed = speed*width/8;
    }

    hwloc_pcidisc_tree_insert_by_busid(&tree, obj);
  }

  closedir(dir);

  hwloc_pcidisc_tree_attach(backend->topology, tree);
  return 0;
}

static int
hwloc_linuxfs_pci_look_pcislots(struct hwloc_backend *backend)
{
  struct hwloc_topology *topology = backend->topology;
  struct hwloc_linux_backend_data_s *data = backend->private_data;
  int root_fd = data->root_fd;
  DIR *dir;
  struct dirent *dirent;

  dir = hwloc_opendir("/sys/bus/pci/slots/", root_fd);
  if (dir) {
    while ((dirent = readdir(dir)) != NULL) {
      char path[64];
      char buf[64];
      unsigned domain, bus, dev;
      int err;

      if (dirent->d_name[0] == '.')
	continue;
      err = snprintf(path, sizeof(path), "/sys/bus/pci/slots/%s/address", dirent->d_name);
      if ((size_t) err < sizeof(path)
	  && hwloc_read_path_by_length(path, buf, sizeof(buf), root_fd) > 0
	  && sscanf(buf, "%x:%x:%x", &domain, &bus, &dev) == 3) {
	/* may also be %x:%x without a device number but that's only for hotplug when nothing is plugged, ignore those */
	hwloc_obj_t obj = hwloc_pci_find_by_busid(topology, domain, bus, dev, 0);
	/* obj may be higher in the hierarchy that requested (if that exact bus didn't exist),
	 * we'll check below whether the bus ID is correct.
	 */
	while (obj) {
	  /* Apply the slot to that device and its siblings with same domain/bus/dev ID.
	   * Make sure that siblings are still PCI and on the same bus
	   * (optional bridge filtering can put different things together).
	   */
	  if (obj->type != HWLOC_OBJ_PCI_DEVICE &&
	      (obj->type != HWLOC_OBJ_BRIDGE || obj->attr->bridge.upstream_type != HWLOC_OBJ_BRIDGE_PCI))
	    break;
	  if (obj->attr->pcidev.domain != domain
	      || obj->attr->pcidev.bus != bus
	      || obj->attr->pcidev.dev != dev)
	    break;

	  hwloc_obj_add_info(obj, "PCISlot", dirent->d_name);
	  obj = obj->next_sibling;
	}
      }
    }
    closedir(dir);
  }

  return 0;
}
#endif /* HWLOC_HAVE_LINUXPCI */
#endif /* HWLOC_HAVE_LINUXIO */

static int
hwloc_look_linuxfs(struct hwloc_backend *backend, struct hwloc_disc_status *dstatus)
{
  /*
   * This backend may be used with topology->is_thissystem set (default)
   * or not (modified fsroot path).
   */

  struct hwloc_topology *topology = backend->topology;
#ifdef HWLOC_HAVE_LINUXIO
  enum hwloc_type_filter_e pfilter, bfilter, ofilter, mfilter;
#endif /* HWLOC_HAVE_LINUXIO */

  if (dstatus->phase == HWLOC_DISC_PHASE_CPU) {
    hwloc_linuxfs_look_cpu(backend, dstatus);
    return 0;
  }

#ifdef HWLOC_HAVE_LINUXIO
  hwloc_topology_get_type_filter(topology, HWLOC_OBJ_PCI_DEVICE, &pfilter);
  hwloc_topology_get_type_filter(topology, HWLOC_OBJ_BRIDGE, &bfilter);
  hwloc_topology_get_type_filter(topology, HWLOC_OBJ_OS_DEVICE, &ofilter);
  hwloc_topology_get_type_filter(topology, HWLOC_OBJ_MISC, &mfilter);

  if (dstatus->phase == HWLOC_DISC_PHASE_PCI
      && (bfilter != HWLOC_TYPE_FILTER_KEEP_NONE
	  || pfilter != HWLOC_TYPE_FILTER_KEEP_NONE)) {
#ifdef HWLOC_HAVE_LINUXPCI
    hwloc_linuxfs_pci_look_pcidevices(backend);
    /* no need to run another PCI phase */
    dstatus->excluded_phases |= HWLOC_DISC_PHASE_PCI;
#endif /* HWLOC_HAVE_LINUXPCI */
  }

  if (dstatus->phase == HWLOC_DISC_PHASE_ANNOTATE
      && (bfilter != HWLOC_TYPE_FILTER_KEEP_NONE
	  || pfilter != HWLOC_TYPE_FILTER_KEEP_NONE)) {
    /* Doesn't work when annotating XML/synthetic because hwloc_pci_find_by_busid()
     * doesn't have any PCI localities.
     * That's good news because we don't want to annotate a random XML with local PCI slot info.
     * We should disable this phase when XML/synthetic is used but this is not enabled by default anyway.
     */
#ifdef HWLOC_HAVE_LINUXPCI
    hwloc_linuxfs_pci_look_pcislots(backend);
#endif /* HWLOC_HAVE_LINUXPCI */
  }

  if (dstatus->phase == HWLOC_DISC_PHASE_IO
      && ofilter != HWLOC_TYPE_FILTER_KEEP_NONE) {
    unsigned osdev_flags = 0;
    if (getenv("HWLOC_VIRTUAL_LINUX_OSDEV"))
      osdev_flags |= HWLOC_LINUXFS_OSDEV_FLAG_FIND_VIRTUAL;
    if (ofilter == HWLOC_TYPE_FILTER_KEEP_ALL)
      osdev_flags |= HWLOC_LINUXFS_OSDEV_FLAG_FIND_USB;

    hwloc_linuxfs_lookup_block_class(backend, osdev_flags);
    hwloc_linuxfs_lookup_dax_class(backend, osdev_flags);
    hwloc_linuxfs_lookup_net_class(backend, osdev_flags);
    hwloc_linuxfs_lookup_infiniband_class(backend, osdev_flags);
    hwloc_linuxfs_lookup_ve_class(backend, osdev_flags);
    hwloc_linuxfs_lookup_bxi_class(backend, osdev_flags);
    hwloc_linuxfs_lookup_cxlmem(backend, osdev_flags);
    if (ofilter != HWLOC_TYPE_FILTER_KEEP_IMPORTANT) {
      hwloc_linuxfs_lookup_drm_class(backend, osdev_flags);
      hwloc_linuxfs_lookup_dma_class(backend, osdev_flags);
    }
  }

  if (dstatus->phase == HWLOC_DISC_PHASE_MISC
      && mfilter != HWLOC_TYPE_FILTER_KEEP_NONE) {
    hwloc__get_firmware_dmi_memory_info(topology, backend->private_data);
  }
#endif /* HWLOC_HAVE_LINUXIO */

  return 0;
}

/*******************************
 ******* Linux component *******
 *******************************/

static void
hwloc_linux_backend_disable(struct hwloc_backend *backend)
{
  struct hwloc_linux_backend_data_s *data = backend->private_data;
#ifdef HAVE_OPENAT
  if (data->root_fd >= 0) {
    free(data->root_path);
    close(data->root_fd);
  }
#endif
#ifdef HWLOC_HAVE_LIBUDEV
  if (data->udev)
    udev_unref(data->udev);
#endif
  free(data);
}

static struct hwloc_backend *
hwloc_linux_component_instantiate(struct hwloc_topology *topology,
				  struct hwloc_disc_component *component,
				  unsigned excluded_phases __hwloc_attribute_unused,
				  const void *_data1 __hwloc_attribute_unused,
				  const void *_data2 __hwloc_attribute_unused,
				  const void *_data3 __hwloc_attribute_unused)
{
  struct hwloc_backend *backend;
  struct hwloc_linux_backend_data_s *data;
  const char * fsroot_path;
  int root = -1;
  char *env;

  backend = hwloc_backend_alloc(topology, component);
  if (!backend)
    goto out;

  data = malloc(sizeof(*data));
  if (!data) {
    errno = ENOMEM;
    goto out_with_backend;
  }

  backend->private_data = data;
  backend->discover = hwloc_look_linuxfs;
  backend->get_pci_busid_cpuset = hwloc_linux_backend_get_pci_busid_cpuset;
  backend->disable = hwloc_linux_backend_disable;

  /* default values */
  data->arch = HWLOC_LINUX_ARCH_UNKNOWN;
  data->is_knl = 0;
  data->is_amd_with_CU = 0;
  data->is_amd_homogeneous = 0;
  data->is_fake_numa_uniform = 0;
  data->is_real_fsroot = 1;
  data->root_path = NULL;
  fsroot_path = getenv("HWLOC_FSROOT");
  if (!fsroot_path)
    fsroot_path = "/";

  if (strcmp(fsroot_path, "/")) {
#ifdef HAVE_OPENAT
    int flags;

    root = open(fsroot_path, O_RDONLY | O_DIRECTORY);
    if (root < 0)
      goto out_with_data;

    backend->is_thissystem = 0;
    data->is_real_fsroot = 0;
    data->root_path = strdup(fsroot_path);

    /* Since this fd stays open after hwloc returns, mark it as
       close-on-exec so that children don't inherit it.  Stevens says
       that we should GETFD before we SETFD, so we do. */
    flags = fcntl(root, F_GETFD, 0);
    if (-1 == flags ||
	-1 == fcntl(root, F_SETFD, FD_CLOEXEC | flags)) {
      close(root);
      root = -1;
      goto out_with_data;
    }
#else
    if (HWLOC_SHOW_CRITICAL_ERRORS())
      fprintf(stderr, "hwloc/linux: Cannot change fsroot without openat() support.\n");
    errno = ENOSYS;
    goto out_with_data;
#endif
  }
  data->root_fd = root;

#ifdef HWLOC_HAVE_LIBUDEV
  data->udev = NULL;
  if (data->is_real_fsroot) {
    data->udev = udev_new();
  }
#endif

  data->dumped_hwdata_dirname = getenv("HWLOC_DUMPED_HWDATA_DIR");
  if (!data->dumped_hwdata_dirname)
    data->dumped_hwdata_dirname = RUNSTATEDIR "/hwloc/";

  data->use_numa_distances = 1;
  data->use_numa_distances_for_cpuless = 1;
  data->use_numa_initiators = 1;
  env = getenv("HWLOC_USE_NUMA_DISTANCES");
  if (env) {
    unsigned val = atoi(env);
    data->use_numa_distances = !!(val & 3); /* 2 implies 1 */
    data->use_numa_distances_for_cpuless = !!(val & 2);
    data->use_numa_initiators = !!(val & 4);
  }

  return backend;

 out_with_data:
#ifdef HAVE_OPENAT
  free(data->root_path);
#endif
  free(data);
 out_with_backend:
  free(backend);
 out:
  return NULL;
}

static struct hwloc_disc_component hwloc_linux_disc_component = {
  "linux",
  HWLOC_DISC_PHASE_CPU | HWLOC_DISC_PHASE_PCI | HWLOC_DISC_PHASE_IO | HWLOC_DISC_PHASE_MISC | HWLOC_DISC_PHASE_ANNOTATE,
  HWLOC_DISC_PHASE_GLOBAL,
  hwloc_linux_component_instantiate,
  50,
  1,
  NULL
};

const struct hwloc_component hwloc_linux_component = {
  HWLOC_COMPONENT_ABI,
  NULL, NULL,
  HWLOC_COMPONENT_TYPE_DISC,
  0,
  &hwloc_linux_disc_component
};
