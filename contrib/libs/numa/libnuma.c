/* Simple NUMA library.
   Copyright (C) 2003,2004,2005,2008 Andi Kleen,SuSE Labs and
   Cliff Wickman,SGI.

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
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

   All calls are undefined when numa_available returns an error. */
#define _GNU_SOURCE 1
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sched.h>
#include <dirent.h>
#include <errno.h>
#include <stdarg.h>
#include <ctype.h>
#include <assert.h>

#include <sys/mman.h>
#include <limits.h>

#include "config.h"
#include "numa.h"
#include "numaif.h"
#include "numaint.h"
#include "util.h"
#include "affinity.h"

#define WEAK __attribute__((weak))

#define CPU_BUFFER_SIZE 4096     /* This limits you to 32768 CPUs */

/* these are the old (version 1) masks */
nodemask_t numa_no_nodes;
nodemask_t numa_all_nodes;
/* these are now the default bitmask (pointers to) (version 2) */
struct bitmask *numa_no_nodes_ptr = NULL;
struct bitmask *numa_all_nodes_ptr = NULL;
struct bitmask *numa_possible_nodes_ptr = NULL;
struct bitmask *numa_all_cpus_ptr = NULL;
struct bitmask *numa_possible_cpus_ptr = NULL;
/* I would prefer to use symbol versioning to create v1 and v2 versions
   of numa_no_nodes and numa_all_nodes, but the loader does not correctly
   handle versioning of BSS versus small data items */

struct bitmask *numa_nodes_ptr = NULL;
static struct bitmask *numa_memnode_ptr = NULL;
static unsigned long *node_cpu_mask_v1[NUMA_NUM_NODES];
static char node_cpu_mask_v1_stale = 1;
static struct bitmask **node_cpu_mask_v2;
static char node_cpu_mask_v2_stale = 1;

WEAK void numa_error(char *where);

#ifndef TLS
#warning "not threadsafe"
#define __thread
#endif

static __thread int bind_policy = MPOL_BIND;
static __thread unsigned int mbind_flags = 0;
static int sizes_set=0;
static int maxconfigurednode = -1;
static int maxconfiguredcpu = -1;
static int numprocnode = -1;
static int numproccpu = -1;
static int nodemask_sz = 0;
static int cpumask_sz = 0;

static int has_preferred_many = 0;

int numa_exit_on_error = 0;
int numa_exit_on_warn = 0;
static void set_sizes(void);

/*
 * There are two special functions, _init(void) and _fini(void), which
 * are called automatically by the dynamic loader whenever a library is loaded.
 *
 * The v1 library depends upon nodemask_t's of all nodes and no nodes.
 */
void __attribute__((constructor))
numa_init(void)
{
	int max,i;

	if (sizes_set)
		return;

	set_sizes();
	/* numa_all_nodes should represent existing nodes on this system */
        max = numa_num_configured_nodes();
        for (i = 0; i < max; i++)
                nodemask_set_compat((nodemask_t *)&numa_all_nodes, i);
	memset(&numa_no_nodes, 0, sizeof(numa_no_nodes));
}

static void cleanup_node_cpu_mask_v2(void);

#define FREE_AND_ZERO(x) if (x) {	\
		numa_bitmask_free(x);	\
		x = NULL;		\
	}

void __attribute__((destructor))
numa_fini(void)
{
	FREE_AND_ZERO(numa_all_cpus_ptr);
	FREE_AND_ZERO(numa_possible_cpus_ptr);
	FREE_AND_ZERO(numa_all_nodes_ptr);
	FREE_AND_ZERO(numa_possible_nodes_ptr);
	FREE_AND_ZERO(numa_no_nodes_ptr);
	FREE_AND_ZERO(numa_memnode_ptr);
	FREE_AND_ZERO(numa_nodes_ptr);
	cleanup_node_cpu_mask_v2();
}

static int numa_find_first(struct bitmask *mask)
{
	int i;
	for (i = 0; i < mask->size; i++)
		if (numa_bitmask_isbitset(mask, i))
			return i;
	return -1;
}

/*
 * The following bitmask declarations, bitmask_*() routines, and associated
 * _setbit() and _getbit() routines are:
 * Copyright (c) 2004_2007 Silicon Graphics, Inc. (SGI) All rights reserved.
 * SGI publishes it under the terms of the GNU General Public License, v2,
 * as published by the Free Software Foundation.
 */
static unsigned int
_getbit(const struct bitmask *bmp, unsigned int n)
{
	if (n < bmp->size)
		return (bmp->maskp[n/bitsperlong] >> (n % bitsperlong)) & 1;
	else
		return 0;
}

static void
_setbit(struct bitmask *bmp, unsigned int n, unsigned int v)
{
	if (n < bmp->size) {
		if (v)
			bmp->maskp[n/bitsperlong] |= 1UL << (n % bitsperlong);
		else
			bmp->maskp[n/bitsperlong] &= ~(1UL << (n % bitsperlong));
	}
}

int
numa_bitmask_isbitset(const struct bitmask *bmp, unsigned int i)
{
	return _getbit(bmp, i);
}

struct bitmask *
numa_bitmask_setall(struct bitmask *bmp)
{
	unsigned int i;
	for (i = 0; i < bmp->size; i++)
		_setbit(bmp, i, 1);
	return bmp;
}

struct bitmask *
numa_bitmask_clearall(struct bitmask *bmp)
{
	unsigned int i;
	for (i = 0; i < bmp->size; i++)
		_setbit(bmp, i, 0);
	return bmp;
}

struct bitmask *
numa_bitmask_setbit(struct bitmask *bmp, unsigned int i)
{
	_setbit(bmp, i, 1);
	return bmp;
}

struct bitmask *
numa_bitmask_clearbit(struct bitmask *bmp, unsigned int i)
{
	_setbit(bmp, i, 0);
	return bmp;
}

unsigned int
numa_bitmask_nbytes(struct bitmask *bmp)
{
	return longsperbits(bmp->size) * sizeof(unsigned long);
}

/* where n is the number of bits in the map */
/* This function should not exit on failure, but right now we cannot really
   recover from this. */
struct bitmask *
numa_bitmask_alloc(unsigned int n)
{
	struct bitmask *bmp;

	if (n < 1) {
		errno = EINVAL;
		numa_error("request to allocate mask for invalid number");
		exit(1);
	}
	bmp = malloc(sizeof(*bmp));
	if (bmp == 0)
		goto oom;
	bmp->size = n;
	bmp->maskp = calloc(longsperbits(n), sizeof(unsigned long));
	if (bmp->maskp == 0) {
		free(bmp);
		goto oom;
	}
	return bmp;

oom:
	numa_error("Out of memory allocating bitmask");
	exit(1);
}

void
numa_bitmask_free(struct bitmask *bmp)
{
	if (bmp == 0)
		return;
	free(bmp->maskp);
	bmp->maskp = (unsigned long *)0xdeadcdef;  /* double free tripwire */
	free(bmp);
	return;
}

/* True if two bitmasks are equal */
int
numa_bitmask_equal(const struct bitmask *bmp1, const struct bitmask *bmp2)
{
	unsigned int i;
	for (i = 0; i < bmp1->size || i < bmp2->size; i++)
		if (_getbit(bmp1, i) != _getbit(bmp2, i))
			return 0;
	return 1;
}

/* Hamming Weight: number of set bits */
unsigned int numa_bitmask_weight(const struct bitmask *bmp)
{
	unsigned int i;
	unsigned int w = 0;
	for (i = 0; i < bmp->size; i++)
		if (_getbit(bmp, i))
			w++;
	return w;
}

/* *****end of bitmask_  routines ************ */

/* Next two can be overwritten by the application for different error handling */
WEAK void numa_error(char *where)
{
	int olde = errno;
	perror(where);
	if (numa_exit_on_error)
		exit(1);
	errno = olde;
}

WEAK void numa_warn(int num, char *fmt, ...)
{
	static unsigned warned;
	va_list ap;
	int olde = errno;

	/* Give each warning only once */
	if ((1<<num) & warned)
		return;
	warned |= (1<<num);

	va_start(ap,fmt);
	fprintf(stderr, "libnuma: Warning: ");
	vfprintf(stderr, fmt, ap);
	fputc('\n', stderr);
	va_end(ap);

	errno = olde;
}

static void setpol(int policy, struct bitmask *bmp)
{
	if (set_mempolicy(policy, bmp->maskp, bmp->size + 1) < 0)
		numa_error("set_mempolicy");
}

static void getpol(int *oldpolicy, struct bitmask *bmp)
{
	if (get_mempolicy(oldpolicy, bmp->maskp, bmp->size + 1, 0, 0) < 0)
		numa_error("get_mempolicy");
}

static void dombind(void *mem, size_t size, int pol, struct bitmask *bmp)
{
	if (mbind(mem, size, pol, bmp ? bmp->maskp : NULL, bmp ? bmp->size + 1 : 0,
		  mbind_flags) < 0)
		numa_error("mbind");
}

/* (undocumented) */
/* gives the wrong answer for hugetlbfs mappings. */
int numa_pagesize(void)
{
	static int pagesize;
	if (pagesize > 0)
		return pagesize;
	pagesize = getpagesize();
	return pagesize;
}

make_internal_alias(numa_pagesize);

/*
 * Find nodes (numa_nodes_ptr), nodes with memory (numa_memnode_ptr)
 * and the highest numbered existing node (maxconfigurednode).
 */
static void
set_configured_nodes(void)
{
	DIR *d;
	struct dirent *de;
	long long freep;

	numa_memnode_ptr = numa_allocate_nodemask();
	numa_nodes_ptr = numa_allocate_nodemask();

	d = opendir("/sys/devices/system/node");
	if (!d) {
		maxconfigurednode = 0;
	} else {
		while ((de = readdir(d)) != NULL) {
			int nd;
			if (strncmp(de->d_name, "node", 4))
				continue;
			nd = strtoul(de->d_name+4, NULL, 0);
			numa_bitmask_setbit(numa_nodes_ptr, nd);
			if (numa_node_size64(nd, &freep) > 0)
				numa_bitmask_setbit(numa_memnode_ptr, nd);
			if (maxconfigurednode < nd)
				maxconfigurednode = nd;
		}
		closedir(d);
	}
}

/*
 * Convert the string length of an ascii hex mask to the number
 * of bits represented by that mask.
 */
static int s2nbits(const char *s)
{
	return strlen(s) * 32 / 9;
}

/* Is string 'pre' a prefix of string 's'? */
static int strprefix(const char *s, const char *pre)
{
	return strncmp(s, pre, strlen(pre)) == 0;
}

static const char *mask_size_file = "/proc/self/status";
static const char *nodemask_prefix = "Mems_allowed:\t";
/*
 * (do this the way Paul Jackson's libcpuset does it)
 * The nodemask values in /proc/self/status are in an
 * ascii format that uses 9 characters for each 32 bits of mask.
 * (this could also be used to find the cpumask size)
 */
static void
set_nodemask_size(void)
{
	FILE *fp;
	char *buf = NULL;
	size_t bufsize = 0;

	if ((fp = fopen(mask_size_file, "r")) == NULL)
		goto done;

	while (getline(&buf, &bufsize, fp) > 0) {
		if (strprefix(buf, nodemask_prefix)) {
			nodemask_sz = s2nbits(buf + strlen(nodemask_prefix));
			break;
		}
	}
	free(buf);
	fclose(fp);
done:
	if (nodemask_sz == 0) {/* fall back on error */
		int pol;
		unsigned long *mask = NULL;
		nodemask_sz = 16;
		do {
			nodemask_sz <<= 1;
			mask = realloc(mask, nodemask_sz / 8);
			if (!mask)
				return;
		} while (get_mempolicy(&pol, mask, nodemask_sz + 1, 0, 0) < 0 && errno == EINVAL &&
				nodemask_sz < 4096*8);
		free(mask);
	}
}

/*
 * Read a mask consisting of a sequence of hexadecimal longs separated by
 * commas. Order them correctly and return the number of bits set.
 */
static int
read_mask(char *s, struct bitmask *bmp)
{
	char *end = s;
	int tmplen = (bmp->size + bitsperint - 1) / bitsperint;
	unsigned int tmp[tmplen];
	unsigned int *start = tmp;
	unsigned int i, n = 0, m = 0;

	if (!s)
		return 0;	/* shouldn't happen */

	i = strtoul(s, &end, 16);

	/* Skip leading zeros */
	while (!i && *end++ == ',') {
		i = strtoul(end, &end, 16);
	}

	if (!i)
		/* End of string. No mask */
		return -1;

	start[n++] = i;
	/* Read sequence of ints */
	while (*end++ == ',') {
		i = strtoul(end, &end, 16);
		start[n++] = i;

		/* buffer overflow */
		if (n > tmplen)
			return -1;
	}

	/*
	 * Invert sequence of ints if necessary since the first int
	 * is the highest and we put it first because we read it first.
	 */
	while (n) {
		int w;
		unsigned long x = 0;
		/* read into long values in an endian-safe way */
		for (w = 0; n && w < bitsperlong; w += bitsperint)
			x |= ((unsigned long)start[n-- - 1] << w);

		bmp->maskp[m++] = x;
	}
	/*
	 * Return the number of bits set
	 */
	return numa_bitmask_weight(bmp);
}

/*
 * Read a processes constraints in terms of nodes and cpus from
 * /proc/self/status.
 */
static void
set_task_constraints(void)
{
	int hicpu = maxconfiguredcpu;
	int i;
	char *buffer = NULL;
	size_t buflen = 0;
	FILE *f;

	numa_all_cpus_ptr = numa_allocate_cpumask();
	numa_possible_cpus_ptr = numa_allocate_cpumask();
	numa_all_nodes_ptr = numa_allocate_nodemask();
	numa_possible_nodes_ptr = numa_allocate_cpumask();
	numa_no_nodes_ptr = numa_allocate_nodemask();

	f = fopen(mask_size_file, "r");
	if (!f) {
		//numa_warn(W_cpumap, "Cannot parse %s", mask_size_file);
		return;
	}

	while (getline(&buffer, &buflen, f) > 0) {
		/* mask starts after [last] tab */
		char  *mask = strrchr(buffer,'\t');

		if (strncmp(buffer,"Cpus_allowed:",13) == 0)
			numproccpu = read_mask(mask + 1, numa_all_cpus_ptr);

		if (strncmp(buffer,"Mems_allowed:",13) == 0) {
			numprocnode = read_mask(mask + 1, numa_all_nodes_ptr);
		}
	}
	fclose(f);
	free(buffer);

	for (i = 0; i <= hicpu; i++)
		numa_bitmask_setbit(numa_possible_cpus_ptr, i);
	for (i = 0; i <= maxconfigurednode; i++)
		numa_bitmask_setbit(numa_possible_nodes_ptr, i);

	/*
	 * Cpus_allowed in the kernel can be defined to all f's
	 * i.e. it may be a superset of the actual available processors.
	 * As such let's reduce numproccpu to the number of actual
	 * available cpus.
	 */
	if (numproccpu <= 0) {
		for (i = 0; i <= hicpu; i++)
			numa_bitmask_setbit(numa_all_cpus_ptr, i);
		numproccpu = hicpu+1;
	}

	if (numproccpu > hicpu+1) {
		numproccpu = hicpu+1;
		for (i=hicpu+1; i<numa_all_cpus_ptr->size; i++) {
			numa_bitmask_clearbit(numa_all_cpus_ptr, i);
		}
	}

	if (numprocnode <= 0) {
		for (i = 0; i <= maxconfigurednode; i++)
			numa_bitmask_setbit(numa_all_nodes_ptr, i);
		numprocnode = maxconfigurednode + 1;
	}

	return;
}

/*
 * Find the highest cpu number possible (in other words the size
 * of a kernel cpumask_t (in bits) - 1)
 */
static void
set_numa_max_cpu(void)
{
	int len = 4096;
	int n;
	int olde = errno;
	struct bitmask *buffer;

	do {
		buffer = numa_bitmask_alloc(len);
		n = numa_sched_getaffinity_v2_int(0, buffer);
		/* on success, returns size of kernel cpumask_t, in bytes */
		if (n < 0) {
			if (errno == EINVAL) {
				if (len >= 1024*1024)
					break;
				len *= 2;
				numa_bitmask_free(buffer);
				continue;
			} else {
				numa_warn(W_numcpus, "Unable to determine max cpu"
					  " (sched_getaffinity: %s); guessing...",
					  strerror(errno));
				n = sizeof(cpu_set_t);
				break;
			}
		}
	} while (n < 0);
	numa_bitmask_free(buffer);
	errno = olde;
	cpumask_sz = n*8;
}

/*
 * get the total (configured) number of cpus - both online and offline
 */
static void
set_configured_cpus(void)
{
	maxconfiguredcpu = sysconf(_SC_NPROCESSORS_CONF) - 1;
	if (maxconfiguredcpu == -1)
		numa_error("sysconf(NPROCESSORS_CONF) failed");
}

static void
set_kernel_abi()
{
	int oldp;
	struct bitmask *bmp, *tmp;
	bmp = numa_allocate_nodemask();
	tmp = numa_allocate_nodemask();

	if (get_mempolicy(&oldp, bmp->maskp, bmp->size + 1, 0, 0) < 0)
		goto out;

	/* Assumes there's always a node 0, and it's online */
	numa_bitmask_setbit(tmp, 0);
	if (set_mempolicy(MPOL_PREFERRED_MANY, tmp->maskp, tmp->size) == 0) {
		has_preferred_many++;
		/* reset the old memory policy */
		setpol(oldp, bmp);
	}

out:
	numa_bitmask_free(tmp);
	numa_bitmask_free(bmp);
}

/*
 * Initialize all the sizes.
 */
static void
set_sizes(void)
{
	sizes_set++;
	set_nodemask_size();	/* size of kernel nodemask_t */
	set_configured_nodes();	/* configured nodes listed in /sys */
	set_numa_max_cpu();	/* size of kernel cpumask_t */
	set_configured_cpus();	/* cpus listed in /sys/devices/system/cpu */
	set_task_constraints(); /* cpus and nodes for current task */
	set_kernel_abi();	/* man policy supported */
}

int
numa_num_configured_nodes(void)
{
	/*
	* NOTE: this function's behavior matches the documentation (ie: it
	* returns a count of nodes with memory) despite the poor function
	* naming.  We also cannot use the similarly poorly named
	* numa_all_nodes_ptr as it only tracks nodes with memory from which
	* the calling process can allocate.  Think sparse nodes, memory-less
	* nodes, cpusets...
	*/
	int memnodecount=0, i;

	for (i=0; i <= maxconfigurednode; i++) {
		if (numa_bitmask_isbitset(numa_memnode_ptr, i))
			memnodecount++;
	}
	return memnodecount;
}

int
numa_num_configured_cpus(void)
{

	return maxconfiguredcpu+1;
}

int
numa_num_possible_nodes(void)
{
	return nodemask_sz;
}

int
numa_num_possible_cpus(void)
{
	return cpumask_sz;
}

int
numa_num_task_nodes(void)
{
	return numprocnode;
}

/*
 * for backward compatibility
 */
int
numa_num_thread_nodes(void)
{
	return numa_num_task_nodes();
}

int
numa_num_task_cpus(void)
{
	return numproccpu;
}

/*
 * for backward compatibility
 */
int
numa_num_thread_cpus(void)
{
	return numa_num_task_cpus();
}

/*
 * Return the number of the highest node in this running system,
 */
int
numa_max_node(void)
{
	return maxconfigurednode;
}

make_internal_alias(numa_max_node);

/*
 * Return the number of the highest possible node in a system,
 * which for v1 is the size of a numa.h nodemask_t(in bits)-1.
 * but for v2 is the size of a kernel nodemask_t(in bits)-1.
 */
SYMVER("numa_max_possible_node_v1", "numa_max_possible_node@libnuma_1.1")
int
numa_max_possible_node_v1(void)
{
	return ((sizeof(nodemask_t)*8)-1);
}

SYMVER("numa_max_possible_node_v2", "numa_max_possible_node@@libnuma_1.2")
int
numa_max_possible_node_v2(void)
{
	return numa_num_possible_nodes()-1;
}

make_internal_alias(numa_max_possible_node_v1);
make_internal_alias(numa_max_possible_node_v2);

/*
 * Allocate a bitmask for cpus, of a size large enough to
 * match the kernel's cpumask_t.
 */
struct bitmask *
numa_allocate_cpumask()
{
	int ncpus = numa_num_possible_cpus();

	return numa_bitmask_alloc(ncpus);
}

/*
 * Allocate a bitmask the size of a libnuma nodemask_t
 */
static struct bitmask *
allocate_nodemask_v1(void)
{
	int nnodes = numa_max_possible_node_v1_int()+1;

	return numa_bitmask_alloc(nnodes);
}

/*
 * Allocate a bitmask for nodes, of a size large enough to
 * match the kernel's nodemask_t.
 */
struct bitmask *
numa_allocate_nodemask(void)
{
	struct bitmask *bmp;
	int nnodes = numa_max_possible_node_v2_int() + 1;

	bmp = numa_bitmask_alloc(nnodes);
	return bmp;
}

/* (cache the result?) */
long long numa_node_size64(int node, long long *freep)
{
	size_t len = 0;
	char *line = NULL;
	long long size = -1;
	FILE *f;
	char fn[64];
	int ok = 0;
	int required = freep ? 2 : 1;

	if (freep)
		*freep = -1;
	sprintf(fn,"/sys/devices/system/node/node%d/meminfo", node);
	f = fopen(fn, "r");
	if (!f)
		return -1;
	while (getdelim(&line, &len, '\n', f) > 0) {
		char *end;
		char *s = strcasestr(line, "kB");
		if (!s)
			continue;
		--s;
		while (s > line && isspace(*s))
			--s;
		while (s > line && isdigit(*s))
			--s;
		if (strstr(line, "MemTotal")) {
			size = strtoull(s,&end,0) << 10;
			if (end == s)
				size = -1;
			else
				ok++;
		}
		if (freep && strstr(line, "MemFree")) {
			*freep = strtoull(s,&end,0) << 10;
			if (end == s)
				*freep = -1;
			else
				ok++;
		}
	}
	fclose(f);
	free(line);
	if (ok != required)
		numa_warn(W_badmeminfo, "Cannot parse sysfs meminfo (%d)", ok);
	return size;
}

make_internal_alias(numa_node_size64);

long numa_node_size(int node, long *freep)
{
	long long f2;
	long sz = numa_node_size64_int(node, &f2);
	if (freep)
		*freep = f2;
	return sz;
}

int numa_available(void)
{
	if (get_mempolicy(NULL, NULL, 0, 0, 0) < 0 && errno == ENOSYS)
		return -1;
	return 0;
}

SYMVER("numa_interleave_memory_v1", "numa_interleave_memory@libnuma_1.1")
void
numa_interleave_memory_v1(void *mem, size_t size, const nodemask_t *mask)
{
	struct bitmask bitmask;

	bitmask.size = sizeof(nodemask_t) * 8;
	bitmask.maskp = (unsigned long *)mask;
	dombind(mem, size, MPOL_INTERLEAVE, &bitmask);
}

SYMVER("numa_interleave_memory_v2", "numa_interleave_memory@@libnuma_1.2")
void
numa_interleave_memory_v2(void *mem, size_t size, struct bitmask *bmp)
{
	dombind(mem, size, MPOL_INTERLEAVE, bmp);
}

void numa_tonode_memory(void *mem, size_t size, int node)
{
	struct bitmask *nodes;

	nodes = numa_allocate_nodemask();
	numa_bitmask_setbit(nodes, node);
	dombind(mem, size, bind_policy, nodes);
	numa_bitmask_free(nodes);
}

SYMVER("numa_tonodemask_memory_v1", "numa_tonodemask_memory@libnuma_1.1")
void
numa_tonodemask_memory_v1(void *mem, size_t size, const nodemask_t *mask)
{
	struct bitmask bitmask;

	bitmask.maskp = (unsigned long *)mask;
	bitmask.size  = sizeof(nodemask_t);
	dombind(mem, size,  bind_policy, &bitmask);
}

SYMVER("numa_tonodemask_memory_v2", "numa_tonodemask_memory@@libnuma_1.2")
void
numa_tonodemask_memory_v2(void *mem, size_t size, struct bitmask *bmp)
{
	dombind(mem, size,  bind_policy, bmp);
}

void numa_setlocal_memory(void *mem, size_t size)
{
	dombind(mem, size, MPOL_LOCAL, NULL);
}

void numa_police_memory(void *mem, size_t size)
{
	int pagesize = numa_pagesize_int();
	unsigned long i;
	char *p = mem;
	for (i = 0; i < size; i += pagesize, p += pagesize)
		__atomic_and_fetch(p, 0xff, __ATOMIC_RELAXED);

}

make_internal_alias(numa_police_memory);

void *numa_alloc(size_t size)
{
	char *mem;
	mem = mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,
		   0, 0);
	if (mem == (char *)-1)
		return NULL;
	numa_police_memory_int(mem, size);
	return mem;
}

void *numa_realloc(void *old_addr, size_t old_size, size_t new_size)
{
	char *mem;
	mem = mremap(old_addr, old_size, new_size, MREMAP_MAYMOVE);
	if (mem == (char *)-1)
		return NULL;
	/*
	 *	The memory policy of the allocated pages is preserved by mremap(), so
	 *	there is no need to (re)set it here. If the policy of the original
	 *	allocation is not set, the new pages will be allocated according to the
	 *	process' mempolicy. Trying to allocate explicitly the new pages on the
	 *	same node as the original ones would require changing the policy of the
	 *	newly allocated pages, which violates the numa_realloc() semantics.
	 */
	return mem;
}

SYMVER("numa_alloc_interleaved_subset_v1", "numa_alloc_interleaved_subset@libnuma_1.1")
void *numa_alloc_interleaved_subset_v1(size_t size, const nodemask_t *mask)
{
	char *mem;
	struct bitmask bitmask;

	mem = mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,
			0, 0);
	if (mem == (char *)-1)
		return NULL;
	bitmask.maskp = (unsigned long *)mask;
	bitmask.size  = sizeof(nodemask_t);
	dombind(mem, size, MPOL_INTERLEAVE, &bitmask);
	return mem;
}

SYMVER("numa_alloc_interleaved_subset_v2", "numa_alloc_interleaved_subset@@libnuma_1.2")
void *numa_alloc_interleaved_subset_v2(size_t size, struct bitmask *bmp)
{
	char *mem;

	mem = mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,
		   0, 0);
	if (mem == (char *)-1)
		return NULL;
	dombind(mem, size, MPOL_INTERLEAVE, bmp);
	return mem;
}

make_internal_alias(numa_alloc_interleaved_subset_v1);
make_internal_alias(numa_alloc_interleaved_subset_v2);

void *
numa_alloc_interleaved(size_t size)
{
	return numa_alloc_interleaved_subset_v2_int(size, numa_all_nodes_ptr);
}

/*
 * given a user node mask, set memory policy to use those nodes
 */
SYMVER("numa_set_interleave_mask_v1", "numa_set_interleave_mask@libnuma_1.1")
void
numa_set_interleave_mask_v1(nodemask_t *mask)
{
	struct bitmask *bmp;
	int nnodes = numa_max_possible_node_v1_int()+1;

	bmp = numa_bitmask_alloc(nnodes);
	copy_nodemask_to_bitmask(mask, bmp);
	if (numa_bitmask_equal(bmp, numa_no_nodes_ptr))
		setpol(MPOL_DEFAULT, bmp);
	else
		setpol(MPOL_INTERLEAVE, bmp);
	numa_bitmask_free(bmp);
}


SYMVER("numa_set_interleave_mask_v2", "numa_set_interleave_mask@@libnuma_1.2")
void
numa_set_interleave_mask_v2(struct bitmask *bmp)
{
	if (numa_bitmask_equal(bmp, numa_no_nodes_ptr))
		setpol(MPOL_DEFAULT, bmp);
	else
		setpol(MPOL_INTERLEAVE, bmp);
}

SYMVER("numa_get_interleave_mask_v1", "numa_get_interleave_mask@libnuma_1.1")
nodemask_t
numa_get_interleave_mask_v1(void)
{
	int oldpolicy;
	struct bitmask *bmp;
	nodemask_t mask;

	bmp = allocate_nodemask_v1();
	getpol(&oldpolicy, bmp);
	if (oldpolicy == MPOL_INTERLEAVE)
		copy_bitmask_to_nodemask(bmp, &mask);
	else
		copy_bitmask_to_nodemask(numa_no_nodes_ptr, &mask);
	numa_bitmask_free(bmp);
	return mask;
}

SYMVER("numa_get_interleave_mask_v2", "numa_get_interleave_mask@@libnuma_1.2")
struct bitmask *
numa_get_interleave_mask_v2(void)
{
	int oldpolicy;
	struct bitmask *bmp;

	bmp = numa_allocate_nodemask();
	getpol(&oldpolicy, bmp);
	if (oldpolicy != MPOL_INTERLEAVE)
		copy_bitmask_to_bitmask(numa_no_nodes_ptr, bmp);
	return bmp;
}

/* (undocumented) */
int numa_get_interleave_node(void)
{
	int nd;
	if (get_mempolicy(&nd, NULL, 0, 0, MPOL_F_NODE) == 0)
		return nd;
	return 0;
}

void *numa_alloc_onnode(size_t size, int node)
{
	char *mem;
	struct bitmask *bmp;

	bmp = numa_allocate_nodemask();
	numa_bitmask_setbit(bmp, node);
	mem = mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,
		   0, 0);
	if (mem == (char *)-1)
		mem = NULL;
	else
		dombind(mem, size, bind_policy, bmp);
	numa_bitmask_free(bmp);
	return mem;
}

void *numa_alloc_local(size_t size)
{
	char *mem;
	mem = mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS,
		   0, 0);
	if (mem == (char *)-1)
		mem =  NULL;
	else
		dombind(mem, size, MPOL_LOCAL, NULL);
	return mem;
}

void numa_set_bind_policy(int strict)
{
	if (strict)
		bind_policy = MPOL_BIND;
	else if (has_preferred_many)
		bind_policy = MPOL_PREFERRED_MANY;
	else
		bind_policy = MPOL_PREFERRED;
}

SYMVER("numa_set_membind_v1", "numa_set_membind@libnuma_1.1")
void
numa_set_membind_v1(const nodemask_t *mask)
{
	struct bitmask bitmask;

	bitmask.maskp = (unsigned long *)mask;
	bitmask.size  = sizeof(nodemask_t);
	setpol(MPOL_BIND, &bitmask);
}

SYMVER("numa_set_membind_v2", "numa_set_membind@@libnuma_1.2")
void
numa_set_membind_v2(struct bitmask *bmp)
{
	setpol(MPOL_BIND, bmp);
}

make_internal_alias(numa_set_membind_v2);

void
numa_set_membind_balancing(struct bitmask *bmp)
{
	/* MPOL_F_NUMA_BALANCING: ignore if unsupported */
	if (set_mempolicy(MPOL_BIND | MPOL_F_NUMA_BALANCING,
			  bmp->maskp, bmp->size + 1) < 0) {
		if (errno == EINVAL) {
			errno = 0;
			numa_set_membind_v2(bmp);
		} else
			numa_error("set_mempolicy");
	}
}

/*
 * copy a bitmask map body to a numa.h nodemask_t structure
 */
void
copy_bitmask_to_nodemask(struct bitmask *bmp, nodemask_t *nmp)
{
	int max, i;

	memset(nmp, 0, sizeof(nodemask_t));
        max = (sizeof(nodemask_t)*8);
	for (i=0; i<bmp->size; i++) {
		if (i >= max)
			break;
		if (numa_bitmask_isbitset(bmp, i))
			nodemask_set_compat((nodemask_t *)nmp, i);
	}
}

/*
 * copy a bitmask map body to another bitmask body
 * fill a larger destination with zeroes
 */
void
copy_bitmask_to_bitmask(struct bitmask *bmpfrom, struct bitmask *bmpto)
{
	int bytes;

	if (bmpfrom->size >= bmpto->size) {
		memcpy(bmpto->maskp, bmpfrom->maskp, CPU_BYTES(bmpto->size));
	} else if (bmpfrom->size < bmpto->size) {
		bytes = CPU_BYTES(bmpfrom->size);
		memcpy(bmpto->maskp, bmpfrom->maskp, bytes);
		memset(((char *)bmpto->maskp)+bytes, 0,
					CPU_BYTES(bmpto->size)-bytes);
	}
}

/*
 * copy a numa.h nodemask_t structure to a bitmask map body
 */
void
copy_nodemask_to_bitmask(nodemask_t *nmp, struct bitmask *bmp)
{
	int max, i;

	numa_bitmask_clearall(bmp);
        max = (sizeof(nodemask_t)*8);
	if (max > bmp->size)
		max = bmp->size;
	for (i=0; i<max; i++) {
		if (nodemask_isset_compat(nmp, i))
			numa_bitmask_setbit(bmp, i);
	}
}

SYMVER("numa_get_membind_v1", "numa_get_membind@libnuma_1.1")
nodemask_t
numa_get_membind_v1(void)
{
	int oldpolicy;
	struct bitmask *bmp;
	nodemask_t nmp;

	bmp = allocate_nodemask_v1();
	getpol(&oldpolicy, bmp);
	if (oldpolicy == MPOL_BIND) {
		copy_bitmask_to_nodemask(bmp, &nmp);
	} else {
		/* copy the body of the map to numa_all_nodes */
		copy_bitmask_to_nodemask(bmp, &numa_all_nodes);
		nmp = numa_all_nodes;
	}
	numa_bitmask_free(bmp);
	return nmp;
}

SYMVER("numa_get_membind_v2", "numa_get_membind@@libnuma_1.2")
struct bitmask *
numa_get_membind_v2(void)
{
	int oldpolicy;
	struct bitmask *bmp;

	bmp = numa_allocate_nodemask();
	getpol(&oldpolicy, bmp);
	if (oldpolicy != MPOL_BIND)
		copy_bitmask_to_bitmask(numa_all_nodes_ptr, bmp);
	return bmp;
}

//TODO:  do we need a v1 nodemask_t version?
struct bitmask *numa_get_mems_allowed(void)
{
	struct bitmask *bmp;

	/*
	 * can change, so query on each call.
	 */
	bmp = numa_allocate_nodemask();
	if (get_mempolicy(NULL, bmp->maskp, bmp->size + 1, 0,
				MPOL_F_MEMS_ALLOWED) < 0)
		numa_error("get_mempolicy");
	return bmp;
}
make_internal_alias(numa_get_mems_allowed);

void numa_free(void *mem, size_t size)
{
	munmap(mem, size);
}

SYMVER("numa_parse_bitmap_v1", "numa_parse_bitmap@libnuma_1.1")
int
numa_parse_bitmap_v1(char *line, unsigned long *mask, int ncpus)
{
	int i;
	char *p = strchr(line, '\n');
	if (!p)
		return -1;

	for (i = 0; p > line;i++) {
		char *oldp, *endp;
		oldp = p;
		if (*p == ',')
			--p;
		while (p > line && *p != ',')
			--p;
		/* Eat two 32bit fields at a time to get longs */
		if (p > line && sizeof(unsigned long) == 8) {
			oldp--;
			memmove(p, p+1, oldp-p+1);
			while (p > line && *p != ',')
				--p;
		}
		if (*p == ',')
			p++;
		if (i >= CPU_LONGS(ncpus))
			return -1;
		mask[i] = strtoul(p, &endp, 16);
		if (endp != oldp)
			return -1;
		p--;
	}
	return 0;
}

SYMVER("numa_parse_bitmap_v2", "numa_parse_bitmap@@libnuma_1.2")
int
numa_parse_bitmap_v2(char *line, struct bitmask *mask)
{
	int i, ncpus;
	char *p = strchr(line, '\n');
	if (!p)
		return -1;
	ncpus = mask->size;

	for (i = 0; p > line;i++) {
		char *oldp, *endp;
		oldp = p;
		if (*p == ',')
			--p;
		while (p > line && *p != ',')
			--p;
		/* Eat two 32bit fields at a time to get longs */
		if (p > line && sizeof(unsigned long) == 8) {
			oldp--;
			memmove(p, p+1, oldp-p+1);
			while (p > line && *p != ',')
				--p;
		}
		if (*p == ',')
			p++;
		if (i >= CPU_LONGS(ncpus))
			return -1;
		mask->maskp[i] = strtoul(p, &endp, 16);
		if (endp != oldp)
			return -1;
		p--;
	}
	return 0;
}

static void init_node_cpu_mask_v2(void)
{
	int nnodes = numa_max_possible_node_v2_int() + 1;
	node_cpu_mask_v2 = calloc (nnodes, sizeof(struct bitmask *));
}

static void cleanup_node_cpu_mask_v2(void)
{
	if (node_cpu_mask_v2) {
		int i;
		int nnodes;
		nnodes = numa_max_possible_node_v2_int() + 1;
		for (i = 0; i < nnodes; i++) {
			FREE_AND_ZERO(node_cpu_mask_v2[i]);
		}
		free(node_cpu_mask_v2);
		node_cpu_mask_v2 = NULL;
	}
}

/* This would be better with some locking, but I don't want to make libnuma
   dependent on pthreads right now. The races are relatively harmless. */
SYMVER("numa_node_to_cpus_v1", "numa_node_to_cpus@libnuma_1.1")
int
numa_node_to_cpus_v1(int node, unsigned long *buffer, int bufferlen)
{
	int err = 0;
	char fn[64];
	FILE *f;
	char update;
	char *line = NULL;
	size_t len = 0;
	struct bitmask bitmask;
	int buflen_needed;
	unsigned long *mask;
	int ncpus = numa_num_possible_cpus();
	int maxnode = numa_max_node_int();

	buflen_needed = CPU_BYTES(ncpus);
	if ((unsigned)node > maxnode || bufferlen < buflen_needed) {
		errno = ERANGE;
		return -1;
	}
	if (bufferlen > buflen_needed)
		memset(buffer, 0, bufferlen);
	update = __atomic_fetch_and(&node_cpu_mask_v1_stale, 0, __ATOMIC_RELAXED);
	if (node_cpu_mask_v1[node] && !update) {
		memcpy(buffer, node_cpu_mask_v1[node], buflen_needed);
		return 0;
	}

	mask = malloc(buflen_needed);
	if (!mask)
		mask = (unsigned long *)buffer;
	memset(mask, 0, buflen_needed);

	sprintf(fn, "/sys/devices/system/node/node%d/cpumap", node);
	f = fopen(fn, "r");
	if (!f || getdelim(&line, &len, '\n', f) < 1) {
		if (numa_bitmask_isbitset(numa_nodes_ptr, node)) {
			numa_warn(W_nosysfs2,
			   "/sys not mounted or invalid. Assuming one node: %s",
				  strerror(errno));
			numa_warn(W_nosysfs2,
			   "(cannot open or correctly parse %s)", fn);
		}
		bitmask.maskp = (unsigned long *)mask;
		bitmask.size  = buflen_needed * 8;
		numa_bitmask_setall(&bitmask);
		err = -1;
	}
	if (f)
		fclose(f);

	if (line && (numa_parse_bitmap_v1(line, mask, ncpus) < 0)) {
		numa_warn(W_cpumap, "Cannot parse cpumap. Assuming one node");
		bitmask.maskp = (unsigned long *)mask;
		bitmask.size  = buflen_needed * 8;
		numa_bitmask_setall(&bitmask);
		err = -1;
	}

	free(line);
	memcpy(buffer, mask, buflen_needed);

	/* slightly racy, see above */
	if (node_cpu_mask_v1[node]) {
		if (update) {
			/*
			 * There may be readers on node_cpu_mask_v1[], hence it can not
			 * be freed.
			 */
			memcpy(node_cpu_mask_v1[node], mask, buflen_needed);
			free(mask);
			mask = NULL;
		} else if (mask != buffer)
			free(mask);
	} else {
		node_cpu_mask_v1[node] = mask;
	}
	return err;
}

/*
 * test whether a node has cpus
 */
/* This would be better with some locking, but I don't want to make libnuma
   dependent on pthreads right now. The races are relatively harmless. */
/*
 * deliver a bitmask of cpus representing the cpus on a given node
 */
SYMVER("numa_node_to_cpus_v2", "numa_node_to_cpus@@libnuma_1.2")
int
numa_node_to_cpus_v2(int node, struct bitmask *buffer)
{
	int err = 0;
	int nnodes = numa_max_node();
	char fn[64], *line = NULL;
	FILE *f;
	char update;
	size_t len = 0;
	struct bitmask *mask;

	if (!node_cpu_mask_v2)
		init_node_cpu_mask_v2();

	if (node > nnodes) {
		errno = ERANGE;
		return -1;
	}
	numa_bitmask_clearall(buffer);

	update = __atomic_fetch_and(&node_cpu_mask_v2_stale, 0, __ATOMIC_RELAXED);
	if (node_cpu_mask_v2[node] && !update) {
		/* have already constructed a mask for this node */
		if (buffer->size < node_cpu_mask_v2[node]->size) {
			errno = EINVAL;
			numa_error("map size mismatch");
			return -1;
		}
		copy_bitmask_to_bitmask(node_cpu_mask_v2[node], buffer);
		return 0;
	}

	/* need a new mask for this node */
	mask = numa_allocate_cpumask();

	/* this is a kernel cpumask_t (see node_read_cpumap()) */
	sprintf(fn, "/sys/devices/system/node/node%d/cpumap", node);
	f = fopen(fn, "r");
	if (!f || getdelim(&line, &len, '\n', f) < 1) {
		if (numa_bitmask_isbitset(numa_nodes_ptr, node)) {
			numa_warn(W_nosysfs2,
			   "/sys not mounted or invalid. Assuming one node: %s",
				  strerror(errno));
			numa_warn(W_nosysfs2,
			   "(cannot open or correctly parse %s)", fn);
		}
		numa_bitmask_setall(mask);
		err = -1;
	}
	if (f)
		fclose(f);

	if (line && (numa_parse_bitmap_v2(line, mask) < 0)) {
		numa_warn(W_cpumap, "Cannot parse cpumap. Assuming one node");
		numa_bitmask_setall(mask);
		err = -1;
	}

	free(line);
	copy_bitmask_to_bitmask(mask, buffer);

	/* slightly racy, see above */
	/* save the mask we created */
	if (node_cpu_mask_v2[node]) {
		if (update) {
			copy_bitmask_to_bitmask(mask, node_cpu_mask_v2[node]);
			numa_bitmask_free(mask);
			mask = NULL;
		/* how could this be? */
		} else if (mask != buffer)
			numa_bitmask_free(mask);
	} else {
		/* we don't want to cache faulty result */
		if (!err)
			node_cpu_mask_v2[node] = mask;
		else
			numa_bitmask_free(mask);
	}
	return err;
}

make_internal_alias(numa_node_to_cpus_v1);
make_internal_alias(numa_node_to_cpus_v2);

void numa_node_to_cpu_update(void)
{
	__atomic_store_n(&node_cpu_mask_v1_stale, 1, __ATOMIC_RELAXED);
	__atomic_store_n(&node_cpu_mask_v2_stale, 1, __ATOMIC_RELAXED);
}

/* report the node of the specified cpu */
int numa_node_of_cpu(int cpu)
{
	struct bitmask *bmp;
	int ncpus, nnodes, node, ret;

	ncpus = numa_num_possible_cpus();
	if (cpu > ncpus){
		errno = EINVAL;
		return -1;
	}
	bmp = numa_bitmask_alloc(ncpus);
	nnodes = numa_max_node();
	for (node = 0; node <= nnodes; node++){
		if (numa_node_to_cpus_v2_int(node, bmp) < 0) {
			/* It's possible for the node to not exist */
			continue;
		}
		if (numa_bitmask_isbitset(bmp, cpu)){
			ret = node;
			goto end;
		}
	}
	ret = -1;
	errno = EINVAL;
end:
	numa_bitmask_free(bmp);
	return ret;
}

SYMVER("numa_run_on_node_mask_v1", "numa_run_on_node_mask@libnuma_1.1")
int
numa_run_on_node_mask_v1(const nodemask_t *mask)
{
	int ncpus = numa_num_possible_cpus();
	int i, k, err;
	unsigned long cpus[CPU_LONGS(ncpus)], nodecpus[CPU_LONGS(ncpus)];
	memset(cpus, 0, CPU_BYTES(ncpus));
	for (i = 0; i < NUMA_NUM_NODES; i++) {
		if (mask->n[i / BITS_PER_LONG] == 0)
			continue;
		if (nodemask_isset_compat(mask, i)) {
			if (numa_node_to_cpus_v1_int(i, nodecpus, CPU_BYTES(ncpus)) < 0) {
				numa_warn(W_noderunmask,
					  "Cannot read node cpumask from sysfs");
				continue;
			}
			for (k = 0; k < CPU_LONGS(ncpus); k++)
				cpus[k] |= nodecpus[k];
		}
	}
	err = numa_sched_setaffinity_v1(0, CPU_BYTES(ncpus), cpus);

	/* The sched_setaffinity API is broken because it expects
	   the user to guess the kernel cpuset size. Do this in a
	   brute force way. */
	if (err < 0 && errno == EINVAL) {
		int savederrno = errno;
		char *bigbuf;
		static int size = -1;
		if (size == -1)
			size = CPU_BYTES(ncpus) * 2;
		bigbuf = malloc(CPU_BUFFER_SIZE);
		if (!bigbuf) {
			errno = ENOMEM;
			return -1;
		}
		errno = savederrno;
		while (size <= CPU_BUFFER_SIZE) {
			memcpy(bigbuf, cpus, CPU_BYTES(ncpus));
			memset(bigbuf + CPU_BYTES(ncpus), 0,
			       CPU_BUFFER_SIZE - CPU_BYTES(ncpus));
			err = numa_sched_setaffinity_v1_int(0, size, (unsigned long *)bigbuf);
			if (err == 0 || errno != EINVAL)
				break;
			size *= 2;
		}
		savederrno = errno;
		free(bigbuf);
		errno = savederrno;
	}
	return err;
}

/*
 * Given a node mask (size of a kernel nodemask_t) (probably populated by
 * a user argument list) set up a map of cpus (map "cpus") on those nodes.
 * Then set affinity to those cpus.
 */
SYMVER("numa_run_on_node_mask_v2", "numa_run_on_node_mask@@libnuma_1.2")
int
numa_run_on_node_mask_v2(struct bitmask *bmp)
{
	int ncpus, i, k, err;
	struct bitmask *cpus, *nodecpus;

	cpus = numa_allocate_cpumask();
	ncpus = cpus->size;
	nodecpus = numa_allocate_cpumask();

	for (i = 0; i < bmp->size; i++) {
		if (bmp->maskp[i / BITS_PER_LONG] == 0)
			continue;
		if (numa_bitmask_isbitset(bmp, i)) {
			/*
			 * numa_all_nodes_ptr is cpuset aware; use only
			 * these nodes
			 */
			if (!numa_bitmask_isbitset(numa_all_nodes_ptr, i)) {
				numa_warn(W_noderunmask,
					"node %d not allowed", i);
				continue;
			}
			if (numa_node_to_cpus_v2_int(i, nodecpus) < 0) {
				numa_warn(W_noderunmask,
					"Cannot read node cpumask from sysfs");
				continue;
			}
			for (k = 0; k < CPU_LONGS(ncpus); k++)
				cpus->maskp[k] |= nodecpus->maskp[k];
		}
	}
	err = numa_sched_setaffinity_v2_int(0, cpus);

	numa_bitmask_free(cpus);
	numa_bitmask_free(nodecpus);

	/* used to have to consider that this could fail - it shouldn't now */
	if (err < 0) {
		numa_error("numa_sched_setaffinity_v2_int() failed");
	}

	return err;
}

make_internal_alias(numa_run_on_node_mask_v2);

/*
 * Given a node mask (size of a kernel nodemask_t) (probably populated by
 * a user argument list) set up a map of cpus (map "cpus") on those nodes
 * without any cpuset awareness. Then set affinity to those cpus.
 */
int
numa_run_on_node_mask_all(struct bitmask *bmp)
{
	int ncpus, i, k, err;
	struct bitmask *cpus, *nodecpus;

	cpus = numa_allocate_cpumask();
	ncpus = cpus->size;
	nodecpus = numa_allocate_cpumask();

	for (i = 0; i < bmp->size; i++) {
		if (bmp->maskp[i / BITS_PER_LONG] == 0)
			continue;
		if (numa_bitmask_isbitset(bmp, i)) {
			if (!numa_bitmask_isbitset(numa_possible_nodes_ptr, i)) {
				numa_warn(W_noderunmask,
					"node %d not allowed", i);
				continue;
			}
			if (numa_node_to_cpus_v2_int(i, nodecpus) < 0) {
				numa_warn(W_noderunmask,
					"Cannot read node cpumask from sysfs");
				continue;
			}
			for (k = 0; k < CPU_LONGS(ncpus); k++)
				cpus->maskp[k] |= nodecpus->maskp[k];
		}
	}
	err = numa_sched_setaffinity_v2_int(0, cpus);

	numa_bitmask_free(cpus);
	numa_bitmask_free(nodecpus);

	/* With possible nodes freedom it can happen easily now */
	if (err < 0) {
		numa_error("numa_sched_setaffinity_v2_int() failed");
	}

	return err;
}

SYMVER("numa_get_run_node_mask_v1", "numa_get_run_node_mask@libnuma_1.1")
nodemask_t
numa_get_run_node_mask_v1(void)
{
	int ncpus = numa_num_configured_cpus();
	int i, k;
	int max = numa_max_node_int();
	struct bitmask *bmp, *cpus, *nodecpus;
	nodemask_t nmp;

	cpus = numa_allocate_cpumask();
	if (numa_sched_getaffinity_v2_int(0, cpus) < 0){
		nmp = numa_no_nodes;
		goto free_cpus;
	}

	nodecpus = numa_allocate_cpumask();
	bmp = allocate_nodemask_v1(); /* the size of a nodemask_t */
	for (i = 0; i <= max; i++) {
		if (numa_node_to_cpus_v2_int(i, nodecpus) < 0) {
			/* It's possible for the node to not exist */
			continue;
		}
		for (k = 0; k < CPU_LONGS(ncpus); k++) {
			if (nodecpus->maskp[k] & cpus->maskp[k])
				numa_bitmask_setbit(bmp, i);
		}
	}
	copy_bitmask_to_nodemask(bmp, &nmp);
	numa_bitmask_free(bmp);
	numa_bitmask_free(nodecpus);
free_cpus:
	numa_bitmask_free(cpus);
	return nmp;
}

SYMVER("numa_get_run_node_mask_v2", "numa_get_run_node_mask@@libnuma_1.2")
struct bitmask *
numa_get_run_node_mask_v2(void)
{
	int i, k;
	int ncpus = numa_num_configured_cpus();
	int max = numa_max_node_int();
	struct bitmask *bmp, *cpus, *nodecpus;

	bmp = numa_allocate_cpumask();
	cpus = numa_allocate_cpumask();
	if (numa_sched_getaffinity_v2_int(0, cpus) < 0){
		copy_bitmask_to_bitmask(numa_no_nodes_ptr, bmp);
		goto free_cpus;
	}

	nodecpus = numa_allocate_cpumask();
	for (i = 0; i <= max; i++) {
		/*
		 * numa_all_nodes_ptr is cpuset aware; show only
		 * these nodes
		 */
		if (!numa_bitmask_isbitset(numa_all_nodes_ptr, i)) {
			continue;
		}
		if (numa_node_to_cpus_v2_int(i, nodecpus) < 0) {
			/* It's possible for the node to not exist */
			continue;
		}
		for (k = 0; k < CPU_LONGS(ncpus); k++) {
			if (nodecpus->maskp[k] & cpus->maskp[k])
				numa_bitmask_setbit(bmp, i);
		}
	}
	numa_bitmask_free(nodecpus);
free_cpus:
	numa_bitmask_free(cpus);
	return bmp;
}

int
numa_migrate_pages(int pid, struct bitmask *fromnodes, struct bitmask *tonodes)
{
	int numa_num_nodes = numa_num_possible_nodes();

	return migrate_pages(pid, numa_num_nodes + 1, fromnodes->maskp,
							tonodes->maskp);
}

int numa_move_pages(int pid, unsigned long count,
	void **pages, const int *nodes, int *status, int flags)
{
	return move_pages(pid, count, pages, nodes, status, flags);
}

int numa_run_on_node(int node)
{
	int numa_num_nodes = numa_num_possible_nodes();
	int ret = -1;
	struct bitmask *cpus;

	if (node >= numa_num_nodes){
		errno = EINVAL;
		goto out;
	}

	cpus = numa_allocate_cpumask();

	if (node == -1)
		numa_bitmask_setall(cpus);
	else if (numa_node_to_cpus_v2_int(node, cpus) < 0){
		numa_warn(W_noderunmask, "Cannot read node cpumask from sysfs");
		goto free;
	}

	ret = numa_sched_setaffinity_v2_int(0, cpus);
free:
	numa_bitmask_free(cpus);
out:
	return ret;
}

static struct bitmask *__numa_preferred(void)
{
	int policy;
	struct bitmask *bmp;

	bmp = numa_allocate_nodemask();
	/* could read the current CPU from /proc/self/status. Probably
	   not worth it. */
	numa_bitmask_clearall(bmp);
	getpol(&policy, bmp);

	if (policy != MPOL_PREFERRED &&
			policy != MPOL_PREFERRED_MANY &&
			policy != MPOL_BIND)
		return bmp;

	if (numa_bitmask_weight(bmp) > 1)
		numa_error(__FILE__);

	return bmp;
}

int numa_preferred(void)
{
	int first_node = 0;
	struct bitmask *bmp;

	bmp = __numa_preferred();
	first_node = numa_find_first(bmp);
	numa_bitmask_free(bmp);
	
	return first_node;
}

static void __numa_set_preferred(struct bitmask *bmp)
{
	int nodes = numa_bitmask_weight(bmp);
	if (nodes > 1)
		numa_error(__FILE__);
	setpol(nodes ? MPOL_PREFERRED : MPOL_LOCAL, bmp);
}

void numa_set_preferred(int node)
{
	struct bitmask *bmp = numa_allocate_nodemask();
	numa_bitmask_setbit(bmp, node);
	__numa_set_preferred(bmp);
	numa_bitmask_free(bmp);
}

int numa_has_preferred_many(void)
{
	return has_preferred_many;
}

void numa_set_preferred_many(struct bitmask *bitmask)
{
	int first_node = 0;

	if (!has_preferred_many) {
		numa_warn(W_nodeparse,
			"Unable to handle MANY preferred nodes. Falling back to first node\n");
		first_node = numa_find_first(bitmask);
		numa_set_preferred(first_node);
		return;
	}
	setpol(MPOL_PREFERRED_MANY, bitmask);
}

struct bitmask *numa_preferred_many()
{
	return __numa_preferred();
}

void numa_set_localalloc(void)
{
	setpol(MPOL_LOCAL, numa_no_nodes_ptr);
}

SYMVER("numa_bind_v1", "numa_bind@libnuma_1.1")
void numa_bind_v1(const nodemask_t *nodemask)
{
	struct bitmask bitmask;

	bitmask.maskp = (unsigned long *)nodemask;
	bitmask.size  = sizeof(nodemask_t);
	numa_run_on_node_mask_v2_int(&bitmask);
	numa_set_membind_v2_int(&bitmask);
}

SYMVER("numa_bind_v2", "numa_bind@@libnuma_1.2")
void numa_bind_v2(struct bitmask *bmp)
{
	numa_run_on_node_mask_v2_int(bmp);
	numa_set_membind_v2_int(bmp);
}

void numa_set_strict(int flag)
{
	if (flag)
		mbind_flags |= MPOL_MF_STRICT;
	else
		mbind_flags &= ~MPOL_MF_STRICT;
}

/*
 * Extract a node or processor number from the given string.
 * Allow a relative node / processor specification within the allowed
 * set if "relative" is nonzero
 */
static unsigned long get_nr(const char *s, char **end, struct bitmask *bmp, int relative)
{
	long i, nr;

	if (!relative)
		return strtoul(s, end, 0);

	nr = strtoul(s, end, 0);
	if (s == *end)
		return nr;
	/* Find the nth set bit */
	for (i = 0; nr >= 0 && i <= bmp->size; i++)
		if (numa_bitmask_isbitset(bmp, i))
			nr--;
	return i-1;
}

/*
 * __numa_parse_nodestring() is called to create a node mask, given
 * an ascii string such as 25 or 12-15 or 1,3,5-7 or +6-10.
 * (the + indicates that the numbers are nodeset-relative)
 *
 * The nodes may be specified as absolute, or relative to the current nodeset.
 * The list of available nodes is in a map pointed to by "allowed_nodes_ptr",
 * which may represent all nodes or the nodes in the current nodeset.
 *
 * The caller must free the returned bitmask.
 */
static struct bitmask *
__numa_parse_nodestring(const char *s, struct bitmask *allowed_nodes_ptr)
{
	int invert = 0, relative = 0;
	int conf_nodes = numa_num_configured_nodes();
	char *end;
	struct bitmask *mask;

	mask = numa_allocate_nodemask();

	if (s[0] == 0){
		copy_bitmask_to_bitmask(numa_no_nodes_ptr, mask);
		return mask; /* return freeable mask */
	}
	if (*s == '!') {
		invert = 1;
		s++;
	}
	if (*s == '+') {
		relative++;
		s++;
	}
	do {
		unsigned long arg;
		int i;
		if (isalpha(*s)) {
			int n;
			if (!strcmp(s,"all")) {
				copy_bitmask_to_bitmask(allowed_nodes_ptr,
							mask);
				s+=4;
				break;
			}
			n = resolve_affinity(s, mask);
			if (n != NO_IO_AFFINITY) {
				if (n < 0)
					goto err;
				s += strlen(s) + 1;
				break;
			}
		}
		arg = get_nr(s, &end, allowed_nodes_ptr, relative);
		if (end == s) {
			numa_warn(W_nodeparse, "unparseable node description `%s'\n", s);
			goto err;
		}
		if (!numa_bitmask_isbitset(allowed_nodes_ptr, arg)) {
			numa_warn(W_nodeparse, "node argument %d is out of range\n", arg);
			goto err;
		}
		i = arg;
		numa_bitmask_setbit(mask, i);
		s = end;
		if (*s == '-') {
			char *end2;
			unsigned long arg2;
			arg2 = get_nr(++s, &end2, allowed_nodes_ptr, relative);
			if (end2 == s) {
				numa_warn(W_nodeparse, "missing node argument %s\n", s);
				goto err;
			}
			if (!numa_bitmask_isbitset(allowed_nodes_ptr, arg2)) {
				numa_warn(W_nodeparse, "node argument %d out of range\n", arg2);
				goto err;
			}
			while (arg <= arg2) {
				i = arg;
				if (numa_bitmask_isbitset(allowed_nodes_ptr,i))
					numa_bitmask_setbit(mask, i);
				arg++;
			}
			s = end2;
		}
	} while (*s++ == ',');
	if (s[-1] != '\0')
		goto err;
	if (invert) {
		int i;
		for (i = 0; i < conf_nodes; i++) {
			if (numa_bitmask_isbitset(mask, i))
				numa_bitmask_clearbit(mask, i);
			else
				numa_bitmask_setbit(mask, i);
		}
	}
	return mask;

err:
	numa_bitmask_free(mask);
	return NULL;
}

/*
 * numa_parse_nodestring() is called to create a bitmask from nodes available
 * for this task.
 */

struct bitmask * numa_parse_nodestring(const char *s)
{
	return __numa_parse_nodestring(s, numa_all_nodes_ptr);
}

/*
 * numa_parse_nodestring_all() is called to create a bitmask from all nodes
 * available.
 */

struct bitmask * numa_parse_nodestring_all(const char *s)
{
	return __numa_parse_nodestring(s, numa_possible_nodes_ptr);
}

/*
 * __numa_parse_cpustring() is called to create a bitmask, given
 * an ascii string such as 25 or 12-15 or 1,3,5-7 or +6-10.
 * (the + indicates that the numbers are cpuset-relative)
 *
 * The cpus may be specified as absolute, or relative to the current cpuset.
 * The list of available cpus for this task is in the map pointed to by
 * "allowed_cpus_ptr", which may represent all cpus or the cpus in the
 * current cpuset.
 *
 * The caller must free the returned bitmask.
 */
static struct bitmask *
__numa_parse_cpustring(const char *s, struct bitmask *allowed_cpus_ptr)
{
	int invert = 0, relative=0;
	int conf_cpus = numa_num_configured_cpus();
	char *end;
	struct bitmask *mask;
	int i;

	mask = numa_allocate_cpumask();

	if (s[0] == 0)
		return mask;
	if (*s == '!') {
		invert = 1;
		s++;
	}
	if (*s == '+') {
		relative++;
		s++;
	}
	do {
		unsigned long arg;

		if (!strcmp(s,"all")) {
			copy_bitmask_to_bitmask(allowed_cpus_ptr, mask);
			s+=4;
			break;
		}
		arg = get_nr(s, &end, allowed_cpus_ptr, relative);
		if (end == s) {
			numa_warn(W_cpuparse, "unparseable cpu description `%s'\n", s);
			goto err;
		}
		if (!numa_bitmask_isbitset(allowed_cpus_ptr, arg)) {
			numa_warn(W_cpuparse, "cpu argument %s is out of range\n", s);
			goto err;
		}
		i = arg;
		numa_bitmask_setbit(mask, i);
		s = end;
		if (*s == '-') {
			char *end2;
			unsigned long arg2;
			arg2 = get_nr(++s, &end2, allowed_cpus_ptr, relative);
			if (end2 == s) {
				numa_warn(W_cpuparse, "missing cpu argument %s\n", s);
				goto err;
			}
			if (!numa_bitmask_isbitset(allowed_cpus_ptr, arg2)) {
				numa_warn(W_cpuparse, "cpu argument %s out of range\n", s);
				goto err;
			}
			while (arg <= arg2) {
				i = arg;
				if (numa_bitmask_isbitset(allowed_cpus_ptr, i))
					numa_bitmask_setbit(mask, i);
				arg++;
			}
			s = end2;
		}
	} while (*s++ == ',');
	if (s[-1] != '\0')
		goto err;
	if (invert) {
		for (i = 0; i < conf_cpus; i++) {
			if (numa_bitmask_isbitset(mask, i))
				numa_bitmask_clearbit(mask, i);
			else
				numa_bitmask_setbit(mask, i);
		}
	}
	return mask;

err:
	numa_bitmask_free(mask);
	return NULL;
}

/*
 * numa_parse_cpustring() is called to create a bitmask from cpus available
 * for this task.
 */

struct bitmask * numa_parse_cpustring(const char *s)
{
	return __numa_parse_cpustring(s, numa_all_cpus_ptr);
}

/*
 * numa_parse_cpustring_all() is called to create a bitmask from all cpus
 * available.
 */

struct bitmask * numa_parse_cpustring_all(const char *s)
{
	return __numa_parse_cpustring(s, numa_possible_cpus_ptr);
}
