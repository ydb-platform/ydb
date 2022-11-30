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

#ifndef _NUMA_H
#define _NUMA_H 1

/* allow an application to test for the current programming interface: */
#define LIBNUMA_API_VERSION 2

/* Simple NUMA policy library */

#include <stddef.h>
#include <string.h>
#include <sys/types.h>
#include <stdlib.h>

#if defined(__x86_64__) || defined(__i386__)
#define NUMA_NUM_NODES  128
#else
#define NUMA_NUM_NODES  2048
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
        unsigned long n[NUMA_NUM_NODES/(sizeof(unsigned long)*8)];
} nodemask_t;

struct bitmask {
	unsigned long size; /* number of bits in the map */
	unsigned long *maskp;
};

/* operations on struct bitmask */
int numa_bitmask_isbitset(const struct bitmask *, unsigned int);
struct bitmask *numa_bitmask_setall(struct bitmask *);
struct bitmask *numa_bitmask_clearall(struct bitmask *);
struct bitmask *numa_bitmask_setbit(struct bitmask *, unsigned int);
struct bitmask *numa_bitmask_clearbit(struct bitmask *, unsigned int);
unsigned int numa_bitmask_nbytes(struct bitmask *);
unsigned int numa_bitmask_weight(const struct bitmask *);
struct bitmask *numa_bitmask_alloc(unsigned int);
void numa_bitmask_free(struct bitmask *);
int numa_bitmask_equal(const struct bitmask *, const struct bitmask *);
void copy_nodemask_to_bitmask(nodemask_t *, struct bitmask *);
void copy_bitmask_to_nodemask(struct bitmask *, nodemask_t *);
void copy_bitmask_to_bitmask(struct bitmask *, struct bitmask *);

/* compatibility for codes that used them: */

static inline void nodemask_zero(nodemask_t *mask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)mask;
	tmp.size = sizeof(nodemask_t) * 8;
	numa_bitmask_clearall(&tmp);
}

static inline void nodemask_zero_compat(nodemask_t *mask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)mask;
	tmp.size = sizeof(nodemask_t) * 8;
	numa_bitmask_clearall(&tmp);
}

static inline void nodemask_set_compat(nodemask_t *mask, int node)
{
	mask->n[node / (8*sizeof(unsigned long))] |=
		(1UL<<(node%(8*sizeof(unsigned long))));
}

static inline void nodemask_clr_compat(nodemask_t *mask, int node)
{
	mask->n[node / (8*sizeof(unsigned long))] &=
		~(1UL<<(node%(8*sizeof(unsigned long))));
}

static inline int nodemask_isset_compat(const nodemask_t *mask, int node)
{
	if ((unsigned)node >= NUMA_NUM_NODES)
		return 0;
	if (mask->n[node / (8*sizeof(unsigned long))] &
		(1UL<<(node%(8*sizeof(unsigned long)))))
		return 1;
	return 0;
}

static inline int nodemask_equal(const nodemask_t *a, const nodemask_t *b)
{
	struct bitmask tmp_a, tmp_b;

	tmp_a.maskp = (unsigned long *)a;
	tmp_a.size = sizeof(nodemask_t) * 8;

	tmp_b.maskp = (unsigned long *)b;
	tmp_b.size = sizeof(nodemask_t) * 8;

	return numa_bitmask_equal(&tmp_a, &tmp_b);
}

static inline int nodemask_equal_compat(const nodemask_t *a, const nodemask_t *b)
{
	struct bitmask tmp_a, tmp_b;

	tmp_a.maskp = (unsigned long *)a;
	tmp_a.size = sizeof(nodemask_t) * 8;

	tmp_b.maskp = (unsigned long *)b;
	tmp_b.size = sizeof(nodemask_t) * 8;

	return numa_bitmask_equal(&tmp_a, &tmp_b);
}

/* NUMA support available. If this returns a negative value all other function
   in this library are undefined. */
int numa_available(void);

/* Basic NUMA state */

/* Get max available node */
int numa_max_node(void);
int numa_max_possible_node(void);
/* Return preferred node */
int numa_preferred(void);

/* Return node size and free memory */
long long numa_node_size64(int node, long long *freep);
long numa_node_size(int node, long *freep);

int numa_pagesize(void);

/* Set with all nodes from which the calling process may allocate memory.
   Only valid after numa_available. */
extern struct bitmask *numa_all_nodes_ptr;

/* Set with all nodes the kernel has exposed to userspace */
extern struct bitmask *numa_nodes_ptr;

/* For source compatibility */
extern nodemask_t numa_all_nodes;

/* Set with all cpus. */
extern struct bitmask *numa_all_cpus_ptr;

/* Set with no nodes */
extern struct bitmask *numa_no_nodes_ptr;

/* Source compatibility */
extern nodemask_t numa_no_nodes;

/* Only run and allocate memory from a specific set of nodes. */
void numa_bind(struct bitmask *nodes);

/* Set the NUMA node interleaving mask. 0 to turn off interleaving */
void numa_set_interleave_mask(struct bitmask *nodemask);

/* Return the current interleaving mask */
struct bitmask *numa_get_interleave_mask(void);

/* allocate a bitmask big enough for all nodes */
struct bitmask *numa_allocate_nodemask(void);

static inline void numa_free_nodemask(struct bitmask *b)
{
	numa_bitmask_free(b);
}

/* Some node to preferably allocate memory from for task. */
void numa_set_preferred(int node);

/* Returns whether or not the platform supports MPOL_PREFERRED_MANY */
int numa_has_preferred_many(void);

/* Set of nodes to preferably allocate memory from for task. */
void numa_set_preferred_many(struct bitmask *bitmask);

/* Return preferred nodes */
struct bitmask *numa_preferred_many(void);

/* Set local memory allocation policy for task */
void numa_set_localalloc(void);

/* Only allocate memory from the nodes set in mask. 0 to turn off */
void numa_set_membind(struct bitmask *nodemask);

/* Only allocate memory from the nodes set in mask. Optimize page
   placement with Linux kernel NUMA balancing if possible. 0 to turn off */
void numa_set_membind_balancing(struct bitmask *bmp);

/* Return current membind */
struct bitmask *numa_get_membind(void);

/* Return allowed memories [nodes] */
struct bitmask *numa_get_mems_allowed(void);

int numa_get_interleave_node(void);

/* NUMA memory allocation. These functions always round to page size
   and are relatively slow. */

/* Alloc memory page interleaved on nodes in mask */
void *numa_alloc_interleaved_subset(size_t size, struct bitmask *nodemask);
/* Alloc memory page interleaved on all nodes. */
void *numa_alloc_interleaved(size_t size);
/* Alloc memory located on node */
void *numa_alloc_onnode(size_t size, int node);
/* Alloc memory on local node */
void *numa_alloc_local(size_t size);
/* Allocation with current policy */
void *numa_alloc(size_t size);
/* Change the size of a memory area preserving the memory policy */
void *numa_realloc(void *old_addr, size_t old_size, size_t new_size);
/* Free memory allocated by the functions above */
void numa_free(void *mem, size_t size);

/* Low level functions, primarily for shared memory. All memory
   processed by these must not be touched yet */

/* Interleave a memory area. */
void numa_interleave_memory(void *mem, size_t size, struct bitmask *mask);

/* Allocate a memory area on a specific node. */
void numa_tonode_memory(void *start, size_t size, int node);

/* Allocate memory on a mask of nodes. */
void numa_tonodemask_memory(void *mem, size_t size, struct bitmask *mask);

/* Allocate a memory area on the current node. */
void numa_setlocal_memory(void *start, size_t size);

/* Allocate memory area with current memory policy */
void numa_police_memory(void *start, size_t size);

/* Run current task only on nodes in mask */
int numa_run_on_node_mask(struct bitmask *mask);
/* Run current task on nodes in mask without any cpuset awareness */
int numa_run_on_node_mask_all(struct bitmask *mask);
/* Run current task only on node */
int numa_run_on_node(int node);
/* Return current mask of nodes the task can run on */
struct bitmask * numa_get_run_node_mask(void);

/* When strict fail allocation when memory cannot be allocated in target node(s). */
void numa_set_bind_policy(int strict);

/* Fail when existing memory has incompatible policy */
void numa_set_strict(int flag);

/* maximum nodes (size of kernel nodemask_t) */
int numa_num_possible_nodes(void);

/* maximum cpus (size of kernel cpumask_t) */
int numa_num_possible_cpus(void);

/* nodes in the system */
int numa_num_configured_nodes(void);

/* maximum cpus */
int numa_num_configured_cpus(void);

/* maximum cpus allowed to current task */
int numa_num_task_cpus(void);
int numa_num_thread_cpus(void); /* backward compatibility */

/* maximum nodes allowed to current task */
int numa_num_task_nodes(void);
int numa_num_thread_nodes(void); /* backward compatibility */

/* allocate a bitmask the size of the kernel cpumask_t */
struct bitmask *numa_allocate_cpumask(void);

static inline void numa_free_cpumask(struct bitmask *b)
{
	numa_bitmask_free(b);
}

/* Convert node to CPU mask. -1/errno on failure, otherwise 0. */
int numa_node_to_cpus(int, struct bitmask *);

void numa_node_to_cpu_update(void);

/* report the node of the specified cpu. -1/errno on invalid cpu. */
int numa_node_of_cpu(int cpu);

/* Report distance of node1 from node2. 0 on error.*/
int numa_distance(int node1, int node2);

/* Error handling. */
/* This is an internal function in libnuma that can be overwritten by an user
   program. Default is to print an error to stderr and exit if numa_exit_on_error
   is true. */
void numa_error(char *where);

/* When true exit the program when a NUMA system call (except numa_available)
   fails */
extern int numa_exit_on_error;
/* Warning function. Can also be overwritten. Default is to print on stderr
   once. */
void numa_warn(int num, char *fmt, ...);

/* When true exit the program on a numa_warn() call */
extern int numa_exit_on_warn;

int numa_migrate_pages(int pid, struct bitmask *from, struct bitmask *to);

int numa_move_pages(int pid, unsigned long count, void **pages,
		const int *nodes, int *status, int flags);

int numa_sched_getaffinity(pid_t, struct bitmask *);
int numa_sched_setaffinity(pid_t, struct bitmask *);

/* Convert an ascii list of nodes to a bitmask */
struct bitmask *numa_parse_nodestring(const char *);

/* Convert an ascii list of nodes to a bitmask without current nodeset
 * dependency */
struct bitmask *numa_parse_nodestring_all(const char *);

/* Convert an ascii list of cpu to a bitmask */
struct bitmask *numa_parse_cpustring(const char *);

/* Convert an ascii list of cpu to a bitmask without current taskset
 * dependency */
struct bitmask *numa_parse_cpustring_all(const char *);

/*
 * The following functions are for source code compatibility
 * with releases prior to version 2.
 * Such codes should be compiled with NUMA_VERSION1_COMPATIBILITY defined.
 */

static inline void numa_set_interleave_mask_compat(nodemask_t *nodemask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)nodemask;
	tmp.size = sizeof(nodemask_t) * 8;
	numa_set_interleave_mask(&tmp);
}

static inline nodemask_t numa_get_interleave_mask_compat(void)
{
	struct bitmask *tp;
	nodemask_t mask;

	tp = numa_get_interleave_mask();
	copy_bitmask_to_nodemask(tp, &mask);
	numa_bitmask_free(tp);
	return mask;
}

static inline void numa_bind_compat(nodemask_t *mask)
{
	struct bitmask *tp;

	tp = numa_allocate_nodemask();
	copy_nodemask_to_bitmask(mask, tp);
	numa_bind(tp);
	numa_bitmask_free(tp);
}

static inline void numa_set_membind_compat(nodemask_t *mask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)mask;
	tmp.size = sizeof(nodemask_t) * 8;
	numa_set_membind(&tmp);
}

static inline nodemask_t numa_get_membind_compat(void)
{
	struct bitmask *tp;
	nodemask_t mask;

	tp = numa_get_membind();
	copy_bitmask_to_nodemask(tp, &mask);
	numa_bitmask_free(tp);
	return mask;
}

static inline void *numa_alloc_interleaved_subset_compat(size_t size,
					const nodemask_t *mask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)mask;
	tmp.size = sizeof(nodemask_t) * 8;
	return numa_alloc_interleaved_subset(size, &tmp);
}

static inline int numa_run_on_node_mask_compat(const nodemask_t *mask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)mask;
	tmp.size = sizeof(nodemask_t) * 8;
	return numa_run_on_node_mask(&tmp);
}

static inline nodemask_t numa_get_run_node_mask_compat(void)
{
	struct bitmask *tp;
	nodemask_t mask;

	tp = numa_get_run_node_mask();
	copy_bitmask_to_nodemask(tp, &mask);
	numa_bitmask_free(tp);
	return mask;
}

static inline void numa_interleave_memory_compat(void *mem, size_t size,
						const nodemask_t *mask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)mask;
	tmp.size = sizeof(nodemask_t) * 8;
	numa_interleave_memory(mem, size, &tmp);
}

static inline void numa_tonodemask_memory_compat(void *mem, size_t size,
						const nodemask_t *mask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)mask;
	tmp.size = sizeof(nodemask_t) * 8;
	numa_tonodemask_memory(mem, size, &tmp);
}

static inline int numa_sched_getaffinity_compat(pid_t pid, unsigned len,
						unsigned long *mask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)mask;
	tmp.size = len * 8;
	return numa_sched_getaffinity(pid, &tmp);
}

static inline int numa_sched_setaffinity_compat(pid_t pid, unsigned len,
						unsigned long *mask)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)mask;
	tmp.size = len * 8;
	return numa_sched_setaffinity(pid, &tmp);
}

static inline int numa_node_to_cpus_compat(int node, unsigned long *buffer,
							int buffer_len)
{
	struct bitmask tmp;

	tmp.maskp = (unsigned long *)buffer;
	tmp.size = buffer_len * 8;
	return numa_node_to_cpus(node, &tmp);
}

/* end of version 1 compatibility functions */

/*
 * To compile an application that uses libnuma version 1:
 *   add -DNUMA_VERSION1_COMPATIBILITY to your Makefile's CFLAGS
 */
#ifdef NUMA_VERSION1_COMPATIBILITY
#error #include <numacompat1.h>
#endif

#ifdef __cplusplus
}
#endif

#endif
