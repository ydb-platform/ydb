#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017-2018 Intel Corporation
 */

#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>
#include <sys/file.h>
#include <unistd.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <signal.h>
#include <setjmp.h>
#ifdef F_ADD_SEALS /* if file sealing is supported, so is memfd */
#include <linux/memfd.h>
#define MEMFD_SUPPORTED
#endif
#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
#include <numa.h>
#include <numaif.h>
#endif
#include <linux/falloc.h>
#include <linux/mman.h> /* for hugetlb-related mmap flags */

#include <rte_common.h>
#include <rte_log.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_memory.h>
#include <rte_spinlock.h>

#include "eal_filesystem.h"
#include "eal_internal_cfg.h"
#include "eal_memalloc.h"
#include "eal_memcfg.h"
#include "eal_private.h"

const int anonymous_hugepages_supported =
#ifdef MAP_HUGE_SHIFT
		1;
#define RTE_MAP_HUGE_SHIFT MAP_HUGE_SHIFT
#else
		0;
#define RTE_MAP_HUGE_SHIFT 26
#endif

/*
 * we've already checked memfd support at compile-time, but we also need to
 * check if we can create hugepage files with memfd.
 *
 * also, this is not a constant, because while we may be *compiled* with memfd
 * hugetlbfs support, we might not be *running* on a system that supports memfd
 * and/or memfd with hugetlbfs, so we need to be able to adjust this flag at
 * runtime, and fall back to anonymous memory.
 */
static int memfd_create_supported =
#ifdef MFD_HUGETLB
		1;
#define RTE_MFD_HUGETLB MFD_HUGETLB
#else
		0;
#define RTE_MFD_HUGETLB 4U
#endif

/*
 * not all kernel version support fallocate on hugetlbfs, so fall back to
 * ftruncate and disallow deallocation if fallocate is not supported.
 */
static int fallocate_supported = -1; /* unknown */

/*
 * we have two modes - single file segments, and file-per-page mode.
 *
 * for single-file segments, we use memseg_list_fd to store the segment fd,
 * while the fds[] will not be allocated, and len will be set to 0.
 *
 * for file-per-page mode, each page will have its own fd, so 'memseg_list_fd'
 * will be invalid (set to -1), and we'll use 'fds' to keep track of page fd's.
 *
 * we cannot know how many pages a system will have in advance, but we do know
 * that they come in lists, and we know lengths of these lists. so, simply store
 * a malloc'd array of fd's indexed by list and segment index.
 *
 * they will be initialized at startup, and filled as we allocate/deallocate
 * segments.
 */
static struct {
	int *fds; /**< dynamically allocated array of segment lock fd's */
	int memseg_list_fd; /**< memseg list fd */
	int len; /**< total length of the array */
	int count; /**< entries used in an array */
} fd_list[RTE_MAX_MEMSEG_LISTS];

/** local copy of a memory map, used to synchronize memory hotplug in MP */
static struct rte_memseg_list local_memsegs[RTE_MAX_MEMSEG_LISTS];

static sigjmp_buf huge_jmpenv;

static void __rte_unused huge_sigbus_handler(int signo __rte_unused)
{
	siglongjmp(huge_jmpenv, 1);
}

/* Put setjmp into a wrap method to avoid compiling error. Any non-volatile,
 * non-static local variable in the stack frame calling sigsetjmp might be
 * clobbered by a call to longjmp.
 */
static int __rte_unused huge_wrap_sigsetjmp(void)
{
	return sigsetjmp(huge_jmpenv, 1);
}

static struct sigaction huge_action_old;
static int huge_need_recover;

static void __rte_unused
huge_register_sigbus(void)
{
	sigset_t mask;
	struct sigaction action;

	sigemptyset(&mask);
	sigaddset(&mask, SIGBUS);
	action.sa_flags = 0;
	action.sa_mask = mask;
	action.sa_handler = huge_sigbus_handler;

	huge_need_recover = !sigaction(SIGBUS, &action, &huge_action_old);
}

static void __rte_unused
huge_recover_sigbus(void)
{
	if (huge_need_recover) {
		sigaction(SIGBUS, &huge_action_old, NULL);
		huge_need_recover = 0;
	}
}

#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
static bool
check_numa(void)
{
	bool ret = true;
	/* Check if kernel supports NUMA. */
	if (numa_available() != 0) {
		RTE_LOG(DEBUG, EAL, "NUMA is not supported.\n");
		ret = false;
	}
	return ret;
}

static void
prepare_numa(int *oldpolicy, struct bitmask *oldmask, int socket_id)
{
	RTE_LOG(DEBUG, EAL, "Trying to obtain current memory policy.\n");
	if (get_mempolicy(oldpolicy, oldmask->maskp,
			  oldmask->size + 1, 0, 0) < 0) {
		RTE_LOG(ERR, EAL,
			"Failed to get current mempolicy: %s. "
			"Assuming MPOL_DEFAULT.\n", strerror(errno));
		*oldpolicy = MPOL_DEFAULT;
	}
	RTE_LOG(DEBUG, EAL,
		"Setting policy MPOL_PREFERRED for socket %d\n",
		socket_id);
	numa_set_preferred(socket_id);
}

static void
restore_numa(int *oldpolicy, struct bitmask *oldmask)
{
	RTE_LOG(DEBUG, EAL,
		"Restoring previous memory policy: %d\n", *oldpolicy);
	if (*oldpolicy == MPOL_DEFAULT) {
		numa_set_localalloc();
	} else if (set_mempolicy(*oldpolicy, oldmask->maskp,
				 oldmask->size + 1) < 0) {
		RTE_LOG(ERR, EAL, "Failed to restore mempolicy: %s\n",
			strerror(errno));
		numa_set_localalloc();
	}
	numa_free_cpumask(oldmask);
}
#endif

/*
 * uses fstat to report the size of a file on disk
 */
static off_t
get_file_size(int fd)
{
	struct stat st;
	if (fstat(fd, &st) < 0)
		return 0;
	return st.st_size;
}

static int
pagesz_flags(uint64_t page_sz)
{
	/* as per mmap() manpage, all page sizes are log2 of page size
	 * shifted by MAP_HUGE_SHIFT
	 */
	int log2 = rte_log2_u64(page_sz);
	return log2 << RTE_MAP_HUGE_SHIFT;
}

/* returns 1 on successful lock, 0 on unsuccessful lock, -1 on error */
static int lock(int fd, int type)
{
	int ret;

	/* flock may be interrupted */
	do {
		ret = flock(fd, type | LOCK_NB);
	} while (ret && errno == EINTR);

	if (ret && errno == EWOULDBLOCK) {
		/* couldn't lock */
		return 0;
	} else if (ret) {
		RTE_LOG(ERR, EAL, "%s(): error calling flock(): %s\n",
			__func__, strerror(errno));
		return -1;
	}
	/* lock was successful */
	return 1;
}

static int
get_seg_memfd(struct hugepage_info *hi __rte_unused,
		unsigned int list_idx __rte_unused,
		unsigned int seg_idx __rte_unused)
{
#ifdef MEMFD_SUPPORTED
	int fd;
	char segname[250]; /* as per manpage, limit is 249 bytes plus null */

	int flags = RTE_MFD_HUGETLB | pagesz_flags(hi->hugepage_sz);
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (internal_conf->single_file_segments) {
		fd = fd_list[list_idx].memseg_list_fd;

		if (fd < 0) {
			snprintf(segname, sizeof(segname), "seg_%i", list_idx);
			fd = memfd_create(segname, flags);
			if (fd < 0) {
				RTE_LOG(DEBUG, EAL, "%s(): memfd create failed: %s\n",
					__func__, strerror(errno));
				return -1;
			}
			fd_list[list_idx].memseg_list_fd = fd;
		}
	} else {
		fd = fd_list[list_idx].fds[seg_idx];

		if (fd < 0) {
			snprintf(segname, sizeof(segname), "seg_%i-%i",
					list_idx, seg_idx);
			fd = memfd_create(segname, flags);
			if (fd < 0) {
				RTE_LOG(DEBUG, EAL, "%s(): memfd create failed: %s\n",
					__func__, strerror(errno));
				return -1;
			}
			fd_list[list_idx].fds[seg_idx] = fd;
		}
	}
	return fd;
#endif
	return -1;
}

static int
get_seg_fd(char *path, int buflen, struct hugepage_info *hi,
		unsigned int list_idx, unsigned int seg_idx)
{
	int fd;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* for in-memory mode, we only make it here when we're sure we support
	 * memfd, and this is a special case.
	 */
	if (internal_conf->in_memory)
		return get_seg_memfd(hi, list_idx, seg_idx);

	if (internal_conf->single_file_segments) {
		/* create a hugepage file path */
		eal_get_hugefile_path(path, buflen, hi->hugedir, list_idx);

		fd = fd_list[list_idx].memseg_list_fd;

		if (fd < 0) {
			fd = open(path, O_CREAT | O_RDWR, 0600);
			if (fd < 0) {
				RTE_LOG(ERR, EAL, "%s(): open failed: %s\n",
					__func__, strerror(errno));
				return -1;
			}
			/* take out a read lock and keep it indefinitely */
			if (lock(fd, LOCK_SH) < 0) {
				RTE_LOG(ERR, EAL, "%s(): lock failed: %s\n",
					__func__, strerror(errno));
				close(fd);
				return -1;
			}
			fd_list[list_idx].memseg_list_fd = fd;
		}
	} else {
		/* create a hugepage file path */
		eal_get_hugefile_path(path, buflen, hi->hugedir,
				list_idx * RTE_MAX_MEMSEG_PER_LIST + seg_idx);

		fd = fd_list[list_idx].fds[seg_idx];

		if (fd < 0) {
			/* A primary process is the only one creating these
			 * files. If there is a leftover that was not cleaned
			 * by clear_hugedir(), we must *now* make sure to drop
			 * the file or we will remap old stuff while the rest
			 * of the code is built on the assumption that a new
			 * page is clean.
			 */
			if (rte_eal_process_type() == RTE_PROC_PRIMARY &&
					unlink(path) == -1 &&
					errno != ENOENT) {
				RTE_LOG(DEBUG, EAL, "%s(): could not remove '%s': %s\n",
					__func__, path, strerror(errno));
				return -1;
			}

			fd = open(path, O_CREAT | O_RDWR, 0600);
			if (fd < 0) {
				RTE_LOG(DEBUG, EAL, "%s(): open failed: %s\n",
					__func__, strerror(errno));
				return -1;
			}
			/* take out a read lock */
			if (lock(fd, LOCK_SH) < 0) {
				RTE_LOG(ERR, EAL, "%s(): lock failed: %s\n",
					__func__, strerror(errno));
				close(fd);
				return -1;
			}
			fd_list[list_idx].fds[seg_idx] = fd;
		}
	}
	return fd;
}

static int
resize_hugefile_in_memory(int fd, uint64_t fa_offset,
		uint64_t page_sz, bool grow)
{
	int flags = grow ? 0 : FALLOC_FL_PUNCH_HOLE |
			FALLOC_FL_KEEP_SIZE;
	int ret;

	/* grow or shrink the file */
	ret = fallocate(fd, flags, fa_offset, page_sz);

	if (ret < 0) {
		RTE_LOG(DEBUG, EAL, "%s(): fallocate() failed: %s\n",
				__func__,
				strerror(errno));
		return -1;
	}
	return 0;
}

static int
resize_hugefile_in_filesystem(int fd, uint64_t fa_offset, uint64_t page_sz,
		bool grow)
{
	bool again = false;

	do {
		if (fallocate_supported == 0) {
			/* we cannot deallocate memory if fallocate() is not
			 * supported, and hugepage file is already locked at
			 * creation, so no further synchronization needed.
			 */

			if (!grow) {
				RTE_LOG(DEBUG, EAL, "%s(): fallocate not supported, not freeing page back to the system\n",
					__func__);
				return -1;
			}
			uint64_t new_size = fa_offset + page_sz;
			uint64_t cur_size = get_file_size(fd);

			/* fallocate isn't supported, fall back to ftruncate */
			if (new_size > cur_size &&
					ftruncate(fd, new_size) < 0) {
				RTE_LOG(DEBUG, EAL, "%s(): ftruncate() failed: %s\n",
					__func__, strerror(errno));
				return -1;
			}
		} else {
			int flags = grow ? 0 : FALLOC_FL_PUNCH_HOLE |
					FALLOC_FL_KEEP_SIZE;
			int ret;

			/*
			 * technically, it is perfectly safe for both primary
			 * and secondary to grow and shrink the page files:
			 * growing the file repeatedly has no effect because
			 * a page can only be allocated once, while mmap ensures
			 * that secondaries hold on to the page even after the
			 * page itself is removed from the filesystem.
			 *
			 * however, leaving growing/shrinking to the primary
			 * tends to expose bugs in fdlist page count handling,
			 * so leave this here just in case.
			 */
			if (rte_eal_process_type() != RTE_PROC_PRIMARY)
				return 0;

			/* grow or shrink the file */
			ret = fallocate(fd, flags, fa_offset, page_sz);

			if (ret < 0) {
				if (fallocate_supported == -1 &&
						errno == ENOTSUP) {
					RTE_LOG(ERR, EAL, "%s(): fallocate() not supported, hugepage deallocation will be disabled\n",
						__func__);
					again = true;
					fallocate_supported = 0;
				} else {
					RTE_LOG(DEBUG, EAL, "%s(): fallocate() failed: %s\n",
						__func__,
						strerror(errno));
					return -1;
				}
			} else
				fallocate_supported = 1;
		}
	} while (again);

	return 0;
}

static void
close_hugefile(int fd, char *path, int list_idx)
{
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();
	/*
	 * primary process must unlink the file, but only when not in in-memory
	 * mode (as in that case there is no file to unlink).
	 */
	if (!internal_conf->in_memory &&
			rte_eal_process_type() == RTE_PROC_PRIMARY &&
			unlink(path))
		RTE_LOG(ERR, EAL, "%s(): unlinking '%s' failed: %s\n",
			__func__, path, strerror(errno));

	close(fd);
	fd_list[list_idx].memseg_list_fd = -1;
}

static int
resize_hugefile(int fd, uint64_t fa_offset, uint64_t page_sz, bool grow)
{
	/* in-memory mode is a special case, because we can be sure that
	 * fallocate() is supported.
	 */
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (internal_conf->in_memory)
		return resize_hugefile_in_memory(fd, fa_offset,
				page_sz, grow);

	return resize_hugefile_in_filesystem(fd, fa_offset, page_sz,
				grow);
}

static int
alloc_seg(struct rte_memseg *ms, void *addr, int socket_id,
		struct hugepage_info *hi, unsigned int list_idx,
		unsigned int seg_idx)
{
#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
	int cur_socket_id = 0;
#endif
	uint64_t map_offset;
	rte_iova_t iova;
	void *va;
	char path[PATH_MAX];
	int ret = 0;
	int fd;
	size_t alloc_sz;
	int flags;
	void *new_addr;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	alloc_sz = hi->hugepage_sz;

	/* these are checked at init, but code analyzers don't know that */
	if (internal_conf->in_memory && !anonymous_hugepages_supported) {
		RTE_LOG(ERR, EAL, "Anonymous hugepages not supported, in-memory mode cannot allocate memory\n");
		return -1;
	}
	if (internal_conf->in_memory && !memfd_create_supported &&
			internal_conf->single_file_segments) {
		RTE_LOG(ERR, EAL, "Single-file segments are not supported without memfd support\n");
		return -1;
	}

	/* in-memory without memfd is a special case */
	int mmap_flags;

	if (internal_conf->in_memory && !memfd_create_supported) {
		const int in_memory_flags = MAP_HUGETLB | MAP_FIXED |
				MAP_PRIVATE | MAP_ANONYMOUS;
		int pagesz_flag;

		pagesz_flag = pagesz_flags(alloc_sz);
		fd = -1;
		mmap_flags = in_memory_flags | pagesz_flag;

		/* single-file segments codepath will never be active
		 * here because in-memory mode is incompatible with the
		 * fallback path, and it's stopped at EAL initialization
		 * stage.
		 */
		map_offset = 0;
	} else {
		/* takes out a read lock on segment or segment list */
		fd = get_seg_fd(path, sizeof(path), hi, list_idx, seg_idx);
		if (fd < 0) {
			RTE_LOG(ERR, EAL, "Couldn't get fd on hugepage file\n");
			return -1;
		}

		if (internal_conf->single_file_segments) {
			map_offset = seg_idx * alloc_sz;
			ret = resize_hugefile(fd, map_offset, alloc_sz, true);
			if (ret < 0)
				goto resized;

			fd_list[list_idx].count++;
		} else {
			map_offset = 0;
			if (ftruncate(fd, alloc_sz) < 0) {
				RTE_LOG(DEBUG, EAL, "%s(): ftruncate() failed: %s\n",
					__func__, strerror(errno));
				goto resized;
			}
			if (internal_conf->hugepage_unlink &&
					!internal_conf->in_memory) {
				if (unlink(path)) {
					RTE_LOG(DEBUG, EAL, "%s(): unlink() failed: %s\n",
						__func__, strerror(errno));
					goto resized;
				}
			}
		}
		mmap_flags = MAP_SHARED | MAP_POPULATE | MAP_FIXED;
	}

	/*
	 * map the segment, and populate page tables, the kernel fills
	 * this segment with zeros if it's a new page.
	 */
	va = mmap(addr, alloc_sz, PROT_READ | PROT_WRITE, mmap_flags, fd,
			map_offset);

	if (va == MAP_FAILED) {
		RTE_LOG(DEBUG, EAL, "%s(): mmap() failed: %s\n", __func__,
			strerror(errno));
		/* mmap failed, but the previous region might have been
		 * unmapped anyway. try to remap it
		 */
		goto unmapped;
	}
	if (va != addr) {
		RTE_LOG(DEBUG, EAL, "%s(): wrong mmap() address\n", __func__);
		munmap(va, alloc_sz);
		goto resized;
	}

	/* In linux, hugetlb limitations, like cgroup, are
	 * enforced at fault time instead of mmap(), even
	 * with the option of MAP_POPULATE. Kernel will send
	 * a SIGBUS signal. To avoid to be killed, save stack
	 * environment here, if SIGBUS happens, we can jump
	 * back here.
	 */
	if (huge_wrap_sigsetjmp()) {
		RTE_LOG(DEBUG, EAL, "SIGBUS: Cannot mmap more hugepages of size %uMB\n",
			(unsigned int)(alloc_sz >> 20));
		goto mapped;
	}

	/* we need to trigger a write to the page to enforce page fault and
	 * ensure that page is accessible to us, but we can't overwrite value
	 * that is already there, so read the old value, and write itback.
	 * kernel populates the page with zeroes initially.
	 */
	*(volatile int *)addr = *(volatile int *)addr;

	iova = rte_mem_virt2iova(addr);
	if (iova == RTE_BAD_PHYS_ADDR) {
		RTE_LOG(DEBUG, EAL, "%s(): can't get IOVA addr\n",
			__func__);
		goto mapped;
	}

#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
	/*
	 * If the kernel has been built without NUMA support, get_mempolicy()
	 * will return an error. If check_numa() returns false, memory
	 * allocation is not NUMA aware and the socket_id should not be
	 * checked.
	 */
	if (check_numa()) {
		ret = get_mempolicy(&cur_socket_id, NULL, 0, addr,
					MPOL_F_NODE | MPOL_F_ADDR);
		if (ret < 0) {
			RTE_LOG(DEBUG, EAL, "%s(): get_mempolicy: %s\n",
				__func__, strerror(errno));
			goto mapped;
		} else if (cur_socket_id != socket_id) {
			RTE_LOG(DEBUG, EAL,
					"%s(): allocation happened on wrong socket (wanted %d, got %d)\n",
				__func__, socket_id, cur_socket_id);
			goto mapped;
		}
	}
#else
	if (rte_socket_count() > 1)
		RTE_LOG(DEBUG, EAL, "%s(): not checking hugepage NUMA node.\n",
				__func__);
#endif

	ms->addr = addr;
	ms->hugepage_sz = alloc_sz;
	ms->len = alloc_sz;
	ms->nchannel = rte_memory_get_nchannel();
	ms->nrank = rte_memory_get_nrank();
	ms->iova = iova;
	ms->socket_id = socket_id;

	return 0;

mapped:
	munmap(addr, alloc_sz);
unmapped:
	flags = EAL_RESERVE_FORCE_ADDRESS;
	new_addr = eal_get_virtual_area(addr, &alloc_sz, alloc_sz, 0, flags);
	if (new_addr != addr) {
		if (new_addr != NULL)
			munmap(new_addr, alloc_sz);
		/* we're leaving a hole in our virtual address space. if
		 * somebody else maps this hole now, we could accidentally
		 * override it in the future.
		 */
		RTE_LOG(CRIT, EAL, "Can't mmap holes in our virtual address space\n");
	}
	/* roll back the ref count */
	if (internal_conf->single_file_segments)
		fd_list[list_idx].count--;
resized:
	/* some codepaths will return negative fd, so exit early */
	if (fd < 0)
		return -1;

	if (internal_conf->single_file_segments) {
		resize_hugefile(fd, map_offset, alloc_sz, false);
		/* ignore failure, can't make it any worse */

		/* if refcount is at zero, close the file */
		if (fd_list[list_idx].count == 0)
			close_hugefile(fd, path, list_idx);
	} else {
		/* only remove file if we can take out a write lock */
		if (internal_conf->hugepage_unlink == 0 &&
				internal_conf->in_memory == 0 &&
				lock(fd, LOCK_EX) == 1)
			unlink(path);
		close(fd);
		fd_list[list_idx].fds[seg_idx] = -1;
	}
	return -1;
}

static int
free_seg(struct rte_memseg *ms, struct hugepage_info *hi,
		unsigned int list_idx, unsigned int seg_idx)
{
	uint64_t map_offset;
	char path[PATH_MAX];
	int fd, ret = 0;
	bool exit_early;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* erase page data */
	memset(ms->addr, 0, ms->len);

	if (mmap(ms->addr, ms->len, PROT_NONE,
			MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0) ==
				MAP_FAILED) {
		RTE_LOG(DEBUG, EAL, "couldn't unmap page\n");
		return -1;
	}

	eal_mem_set_dump(ms->addr, ms->len, false);

	exit_early = false;

	/* if we're using anonymous hugepages, nothing to be done */
	if (internal_conf->in_memory && !memfd_create_supported)
		exit_early = true;

	/* if we've already unlinked the page, nothing needs to be done */
	if (!internal_conf->in_memory && internal_conf->hugepage_unlink)
		exit_early = true;

	if (exit_early) {
		memset(ms, 0, sizeof(*ms));
		return 0;
	}

	/* if we are not in single file segments mode, we're going to unmap the
	 * segment and thus drop the lock on original fd, but hugepage dir is
	 * now locked so we can take out another one without races.
	 */
	fd = get_seg_fd(path, sizeof(path), hi, list_idx, seg_idx);
	if (fd < 0)
		return -1;

	if (internal_conf->single_file_segments) {
		map_offset = seg_idx * ms->len;
		if (resize_hugefile(fd, map_offset, ms->len, false))
			return -1;

		if (--(fd_list[list_idx].count) == 0)
			close_hugefile(fd, path, list_idx);

		ret = 0;
	} else {
		/* if we're able to take out a write lock, we're the last one
		 * holding onto this page.
		 */
		if (!internal_conf->in_memory) {
			ret = lock(fd, LOCK_EX);
			if (ret >= 0) {
				/* no one else is using this page */
				if (ret == 1)
					unlink(path);
			}
		}
		/* closing fd will drop the lock */
		close(fd);
		fd_list[list_idx].fds[seg_idx] = -1;
	}

	memset(ms, 0, sizeof(*ms));

	return ret < 0 ? -1 : 0;
}

struct alloc_walk_param {
	struct hugepage_info *hi;
	struct rte_memseg **ms;
	size_t page_sz;
	unsigned int segs_allocated;
	unsigned int n_segs;
	int socket;
	bool exact;
};
static int
alloc_seg_walk(const struct rte_memseg_list *msl, void *arg)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct alloc_walk_param *wa = arg;
	struct rte_memseg_list *cur_msl;
	size_t page_sz;
	int cur_idx, start_idx, j, dir_fd = -1;
	unsigned int msl_idx, need, i;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (msl->page_sz != wa->page_sz)
		return 0;
	if (msl->socket_id != wa->socket)
		return 0;

	page_sz = (size_t)msl->page_sz;

	msl_idx = msl - mcfg->memsegs;
	cur_msl = &mcfg->memsegs[msl_idx];

	need = wa->n_segs;

	/* try finding space in memseg list */
	if (wa->exact) {
		/* if we require exact number of pages in a list, find them */
		cur_idx = rte_fbarray_find_next_n_free(&cur_msl->memseg_arr, 0,
				need);
		if (cur_idx < 0)
			return 0;
		start_idx = cur_idx;
	} else {
		int cur_len;

		/* we don't require exact number of pages, so we're going to go
		 * for best-effort allocation. that means finding the biggest
		 * unused block, and going with that.
		 */
		cur_idx = rte_fbarray_find_biggest_free(&cur_msl->memseg_arr,
				0);
		if (cur_idx < 0)
			return 0;
		start_idx = cur_idx;
		/* adjust the size to possibly be smaller than original
		 * request, but do not allow it to be bigger.
		 */
		cur_len = rte_fbarray_find_contig_free(&cur_msl->memseg_arr,
				cur_idx);
		need = RTE_MIN(need, (unsigned int)cur_len);
	}

	/* do not allow any page allocations during the time we're allocating,
	 * because file creation and locking operations are not atomic,
	 * and we might be the first or the last ones to use a particular page,
	 * so we need to ensure atomicity of every operation.
	 *
	 * during init, we already hold a write lock, so don't try to take out
	 * another one.
	 */
	if (wa->hi->lock_descriptor == -1 && !internal_conf->in_memory) {
		dir_fd = open(wa->hi->hugedir, O_RDONLY);
		if (dir_fd < 0) {
			RTE_LOG(ERR, EAL, "%s(): Cannot open '%s': %s\n",
				__func__, wa->hi->hugedir, strerror(errno));
			return -1;
		}
		/* blocking writelock */
		if (flock(dir_fd, LOCK_EX)) {
			RTE_LOG(ERR, EAL, "%s(): Cannot lock '%s': %s\n",
				__func__, wa->hi->hugedir, strerror(errno));
			close(dir_fd);
			return -1;
		}
	}

	for (i = 0; i < need; i++, cur_idx++) {
		struct rte_memseg *cur;
		void *map_addr;

		cur = rte_fbarray_get(&cur_msl->memseg_arr, cur_idx);
		map_addr = RTE_PTR_ADD(cur_msl->base_va,
				cur_idx * page_sz);

		if (alloc_seg(cur, map_addr, wa->socket, wa->hi,
				msl_idx, cur_idx)) {
			RTE_LOG(DEBUG, EAL, "attempted to allocate %i segments, but only %i were allocated\n",
				need, i);

			/* if exact number wasn't requested, stop */
			if (!wa->exact)
				goto out;

			/* clean up */
			for (j = start_idx; j < cur_idx; j++) {
				struct rte_memseg *tmp;
				struct rte_fbarray *arr =
						&cur_msl->memseg_arr;

				tmp = rte_fbarray_get(arr, j);
				rte_fbarray_set_free(arr, j);

				/* free_seg may attempt to create a file, which
				 * may fail.
				 */
				if (free_seg(tmp, wa->hi, msl_idx, j))
					RTE_LOG(DEBUG, EAL, "Cannot free page\n");
			}
			/* clear the list */
			if (wa->ms)
				memset(wa->ms, 0, sizeof(*wa->ms) * wa->n_segs);

			if (dir_fd >= 0)
				close(dir_fd);
			return -1;
		}
		if (wa->ms)
			wa->ms[i] = cur;

		rte_fbarray_set_used(&cur_msl->memseg_arr, cur_idx);
	}
out:
	wa->segs_allocated = i;
	if (i > 0)
		cur_msl->version++;
	if (dir_fd >= 0)
		close(dir_fd);
	/* if we didn't allocate any segments, move on to the next list */
	return i > 0;
}

struct free_walk_param {
	struct hugepage_info *hi;
	struct rte_memseg *ms;
};
static int
free_seg_walk(const struct rte_memseg_list *msl, void *arg)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct rte_memseg_list *found_msl;
	struct free_walk_param *wa = arg;
	uintptr_t start_addr, end_addr;
	int msl_idx, seg_idx, ret, dir_fd = -1;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	start_addr = (uintptr_t) msl->base_va;
	end_addr = start_addr + msl->len;

	if ((uintptr_t)wa->ms->addr < start_addr ||
			(uintptr_t)wa->ms->addr >= end_addr)
		return 0;

	msl_idx = msl - mcfg->memsegs;
	seg_idx = RTE_PTR_DIFF(wa->ms->addr, start_addr) / msl->page_sz;

	/* msl is const */
	found_msl = &mcfg->memsegs[msl_idx];

	/* do not allow any page allocations during the time we're freeing,
	 * because file creation and locking operations are not atomic,
	 * and we might be the first or the last ones to use a particular page,
	 * so we need to ensure atomicity of every operation.
	 *
	 * during init, we already hold a write lock, so don't try to take out
	 * another one.
	 */
	if (wa->hi->lock_descriptor == -1 && !internal_conf->in_memory) {
		dir_fd = open(wa->hi->hugedir, O_RDONLY);
		if (dir_fd < 0) {
			RTE_LOG(ERR, EAL, "%s(): Cannot open '%s': %s\n",
				__func__, wa->hi->hugedir, strerror(errno));
			return -1;
		}
		/* blocking writelock */
		if (flock(dir_fd, LOCK_EX)) {
			RTE_LOG(ERR, EAL, "%s(): Cannot lock '%s': %s\n",
				__func__, wa->hi->hugedir, strerror(errno));
			close(dir_fd);
			return -1;
		}
	}

	found_msl->version++;

	rte_fbarray_set_free(&found_msl->memseg_arr, seg_idx);

	ret = free_seg(wa->ms, wa->hi, msl_idx, seg_idx);

	if (dir_fd >= 0)
		close(dir_fd);

	if (ret < 0)
		return -1;

	return 1;
}

int
eal_memalloc_alloc_seg_bulk(struct rte_memseg **ms, int n_segs, size_t page_sz,
		int socket, bool exact)
{
	int i, ret = -1;
#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
	bool have_numa = false;
	int oldpolicy;
	struct bitmask *oldmask;
#endif
	struct alloc_walk_param wa;
	struct hugepage_info *hi = NULL;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	memset(&wa, 0, sizeof(wa));

	/* dynamic allocation not supported in legacy mode */
	if (internal_conf->legacy_mem)
		return -1;

	for (i = 0; i < (int) RTE_DIM(internal_conf->hugepage_info); i++) {
		if (page_sz ==
				internal_conf->hugepage_info[i].hugepage_sz) {
			hi = &internal_conf->hugepage_info[i];
			break;
		}
	}
	if (!hi) {
		RTE_LOG(ERR, EAL, "%s(): can't find relevant hugepage_info entry\n",
			__func__);
		return -1;
	}

#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
	if (check_numa()) {
		oldmask = numa_allocate_nodemask();
		prepare_numa(&oldpolicy, oldmask, socket);
		have_numa = true;
	}
#endif

	wa.exact = exact;
	wa.hi = hi;
	wa.ms = ms;
	wa.n_segs = n_segs;
	wa.page_sz = page_sz;
	wa.socket = socket;
	wa.segs_allocated = 0;

	/* memalloc is locked, so it's safe to use thread-unsafe version */
	ret = rte_memseg_list_walk_thread_unsafe(alloc_seg_walk, &wa);
	if (ret == 0) {
		RTE_LOG(ERR, EAL, "%s(): couldn't find suitable memseg_list\n",
			__func__);
		ret = -1;
	} else if (ret > 0) {
		ret = (int)wa.segs_allocated;
	}

#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
	if (have_numa)
		restore_numa(&oldpolicy, oldmask);
#endif
	return ret;
}

struct rte_memseg *
eal_memalloc_alloc_seg(size_t page_sz, int socket)
{
	struct rte_memseg *ms;
	if (eal_memalloc_alloc_seg_bulk(&ms, 1, page_sz, socket, true) < 0)
		return NULL;
	/* return pointer to newly allocated memseg */
	return ms;
}

int
eal_memalloc_free_seg_bulk(struct rte_memseg **ms, int n_segs)
{
	int seg, ret = 0;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* dynamic free not supported in legacy mode */
	if (internal_conf->legacy_mem)
		return -1;

	for (seg = 0; seg < n_segs; seg++) {
		struct rte_memseg *cur = ms[seg];
		struct hugepage_info *hi = NULL;
		struct free_walk_param wa;
		int i, walk_res;

		/* if this page is marked as unfreeable, fail */
		if (cur->flags & RTE_MEMSEG_FLAG_DO_NOT_FREE) {
			RTE_LOG(DEBUG, EAL, "Page is not allowed to be freed\n");
			ret = -1;
			continue;
		}

		memset(&wa, 0, sizeof(wa));

		for (i = 0; i < (int)RTE_DIM(internal_conf->hugepage_info);
				i++) {
			hi = &internal_conf->hugepage_info[i];
			if (cur->hugepage_sz == hi->hugepage_sz)
				break;
		}
		if (i == (int)RTE_DIM(internal_conf->hugepage_info)) {
			RTE_LOG(ERR, EAL, "Can't find relevant hugepage_info entry\n");
			ret = -1;
			continue;
		}

		wa.ms = cur;
		wa.hi = hi;

		/* memalloc is locked, so it's safe to use thread-unsafe version
		 */
		walk_res = rte_memseg_list_walk_thread_unsafe(free_seg_walk,
				&wa);
		if (walk_res == 1)
			continue;
		if (walk_res == 0)
			RTE_LOG(ERR, EAL, "Couldn't find memseg list\n");
		ret = -1;
	}
	return ret;
}

int
eal_memalloc_free_seg(struct rte_memseg *ms)
{
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* dynamic free not supported in legacy mode */
	if (internal_conf->legacy_mem)
		return -1;

	return eal_memalloc_free_seg_bulk(&ms, 1);
}

static int
sync_chunk(struct rte_memseg_list *primary_msl,
		struct rte_memseg_list *local_msl, struct hugepage_info *hi,
		unsigned int msl_idx, bool used, int start, int end)
{
	struct rte_fbarray *l_arr, *p_arr;
	int i, ret, chunk_len, diff_len;

	l_arr = &local_msl->memseg_arr;
	p_arr = &primary_msl->memseg_arr;

	/* we need to aggregate allocations/deallocations into bigger chunks,
	 * as we don't want to spam the user with per-page callbacks.
	 *
	 * to avoid any potential issues, we also want to trigger
	 * deallocation callbacks *before* we actually deallocate
	 * memory, so that the user application could wrap up its use
	 * before it goes away.
	 */

	chunk_len = end - start;

	/* find how many contiguous pages we can map/unmap for this chunk */
	diff_len = used ?
			rte_fbarray_find_contig_free(l_arr, start) :
			rte_fbarray_find_contig_used(l_arr, start);

	/* has to be at least one page */
	if (diff_len < 1)
		return -1;

	diff_len = RTE_MIN(chunk_len, diff_len);

	/* if we are freeing memory, notify the application */
	if (!used) {
		struct rte_memseg *ms;
		void *start_va;
		size_t len, page_sz;

		ms = rte_fbarray_get(l_arr, start);
		start_va = ms->addr;
		page_sz = (size_t)primary_msl->page_sz;
		len = page_sz * diff_len;

		eal_memalloc_mem_event_notify(RTE_MEM_EVENT_FREE,
				start_va, len);
	}

	for (i = 0; i < diff_len; i++) {
		struct rte_memseg *p_ms, *l_ms;
		int seg_idx = start + i;

		l_ms = rte_fbarray_get(l_arr, seg_idx);
		p_ms = rte_fbarray_get(p_arr, seg_idx);

		if (l_ms == NULL || p_ms == NULL)
			return -1;

		if (used) {
			ret = alloc_seg(l_ms, p_ms->addr,
					p_ms->socket_id, hi,
					msl_idx, seg_idx);
			if (ret < 0)
				return -1;
			rte_fbarray_set_used(l_arr, seg_idx);
		} else {
			ret = free_seg(l_ms, hi, msl_idx, seg_idx);
			rte_fbarray_set_free(l_arr, seg_idx);
			if (ret < 0)
				return -1;
		}
	}

	/* if we just allocated memory, notify the application */
	if (used) {
		struct rte_memseg *ms;
		void *start_va;
		size_t len, page_sz;

		ms = rte_fbarray_get(l_arr, start);
		start_va = ms->addr;
		page_sz = (size_t)primary_msl->page_sz;
		len = page_sz * diff_len;

		eal_memalloc_mem_event_notify(RTE_MEM_EVENT_ALLOC,
				start_va, len);
	}

	/* calculate how much we can advance until next chunk */
	diff_len = used ?
			rte_fbarray_find_contig_used(l_arr, start) :
			rte_fbarray_find_contig_free(l_arr, start);
	ret = RTE_MIN(chunk_len, diff_len);

	return ret;
}

static int
sync_status(struct rte_memseg_list *primary_msl,
		struct rte_memseg_list *local_msl, struct hugepage_info *hi,
		unsigned int msl_idx, bool used)
{
	struct rte_fbarray *l_arr, *p_arr;
	int p_idx, l_chunk_len, p_chunk_len, ret;
	int start, end;

	/* this is a little bit tricky, but the basic idea is - walk both lists
	 * and spot any places where there are discrepancies. walking both lists
	 * and noting discrepancies in a single go is a hard problem, so we do
	 * it in two passes - first we spot any places where allocated segments
	 * mismatch (i.e. ensure that everything that's allocated in the primary
	 * is also allocated in the secondary), and then we do it by looking at
	 * free segments instead.
	 *
	 * we also need to aggregate changes into chunks, as we have to call
	 * callbacks per allocation, not per page.
	 */
	l_arr = &local_msl->memseg_arr;
	p_arr = &primary_msl->memseg_arr;

	if (used)
		p_idx = rte_fbarray_find_next_used(p_arr, 0);
	else
		p_idx = rte_fbarray_find_next_free(p_arr, 0);

	while (p_idx >= 0) {
		int next_chunk_search_idx;

		if (used) {
			p_chunk_len = rte_fbarray_find_contig_used(p_arr,
					p_idx);
			l_chunk_len = rte_fbarray_find_contig_used(l_arr,
					p_idx);
		} else {
			p_chunk_len = rte_fbarray_find_contig_free(p_arr,
					p_idx);
			l_chunk_len = rte_fbarray_find_contig_free(l_arr,
					p_idx);
		}
		/* best case scenario - no differences (or bigger, which will be
		 * fixed during next iteration), look for next chunk
		 */
		if (l_chunk_len >= p_chunk_len) {
			next_chunk_search_idx = p_idx + p_chunk_len;
			goto next_chunk;
		}

		/* if both chunks start at the same point, skip parts we know
		 * are identical, and sync the rest. each call to sync_chunk
		 * will only sync contiguous segments, so we need to call this
		 * until we are sure there are no more differences in this
		 * chunk.
		 */
		start = p_idx + l_chunk_len;
		end = p_idx + p_chunk_len;
		do {
			ret = sync_chunk(primary_msl, local_msl, hi, msl_idx,
					used, start, end);
			start += ret;
		} while (start < end && ret >= 0);
		/* if ret is negative, something went wrong */
		if (ret < 0)
			return -1;

		next_chunk_search_idx = p_idx + p_chunk_len;
next_chunk:
		/* skip to end of this chunk */
		if (used) {
			p_idx = rte_fbarray_find_next_used(p_arr,
					next_chunk_search_idx);
		} else {
			p_idx = rte_fbarray_find_next_free(p_arr,
					next_chunk_search_idx);
		}
	}
	return 0;
}

static int
sync_existing(struct rte_memseg_list *primary_msl,
		struct rte_memseg_list *local_msl, struct hugepage_info *hi,
		unsigned int msl_idx)
{
	int ret, dir_fd;

	/* do not allow any page allocations during the time we're allocating,
	 * because file creation and locking operations are not atomic,
	 * and we might be the first or the last ones to use a particular page,
	 * so we need to ensure atomicity of every operation.
	 */
	dir_fd = open(hi->hugedir, O_RDONLY);
	if (dir_fd < 0) {
		RTE_LOG(ERR, EAL, "%s(): Cannot open '%s': %s\n", __func__,
			hi->hugedir, strerror(errno));
		return -1;
	}
	/* blocking writelock */
	if (flock(dir_fd, LOCK_EX)) {
		RTE_LOG(ERR, EAL, "%s(): Cannot lock '%s': %s\n", __func__,
			hi->hugedir, strerror(errno));
		close(dir_fd);
		return -1;
	}

	/* ensure all allocated space is the same in both lists */
	ret = sync_status(primary_msl, local_msl, hi, msl_idx, true);
	if (ret < 0)
		goto fail;

	/* ensure all unallocated space is the same in both lists */
	ret = sync_status(primary_msl, local_msl, hi, msl_idx, false);
	if (ret < 0)
		goto fail;

	/* update version number */
	local_msl->version = primary_msl->version;

	close(dir_fd);

	return 0;
fail:
	close(dir_fd);
	return -1;
}

static int
sync_walk(const struct rte_memseg_list *msl, void *arg __rte_unused)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct rte_memseg_list *primary_msl, *local_msl;
	struct hugepage_info *hi = NULL;
	unsigned int i;
	int msl_idx;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (msl->external)
		return 0;

	msl_idx = msl - mcfg->memsegs;
	primary_msl = &mcfg->memsegs[msl_idx];
	local_msl = &local_memsegs[msl_idx];

	for (i = 0; i < RTE_DIM(internal_conf->hugepage_info); i++) {
		uint64_t cur_sz =
			internal_conf->hugepage_info[i].hugepage_sz;
		uint64_t msl_sz = primary_msl->page_sz;
		if (msl_sz == cur_sz) {
			hi = &internal_conf->hugepage_info[i];
			break;
		}
	}
	if (!hi) {
		RTE_LOG(ERR, EAL, "Can't find relevant hugepage_info entry\n");
		return -1;
	}

	/* if versions don't match, synchronize everything */
	if (local_msl->version != primary_msl->version &&
			sync_existing(primary_msl, local_msl, hi, msl_idx))
		return -1;
	return 0;
}


int
eal_memalloc_sync_with_primary(void)
{
	/* nothing to be done in primary */
	if (rte_eal_process_type() == RTE_PROC_PRIMARY)
		return 0;

	/* memalloc is locked, so it's safe to call thread-unsafe version */
	if (rte_memseg_list_walk_thread_unsafe(sync_walk, NULL))
		return -1;
	return 0;
}

static int
secondary_msl_create_walk(const struct rte_memseg_list *msl,
		void *arg __rte_unused)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct rte_memseg_list *primary_msl, *local_msl;
	char name[PATH_MAX];
	int msl_idx, ret;

	if (msl->external)
		return 0;

	msl_idx = msl - mcfg->memsegs;
	primary_msl = &mcfg->memsegs[msl_idx];
	local_msl = &local_memsegs[msl_idx];

	/* create distinct fbarrays for each secondary */
	snprintf(name, RTE_FBARRAY_NAME_LEN, "%s_%i",
		primary_msl->memseg_arr.name, getpid());

	ret = rte_fbarray_init(&local_msl->memseg_arr, name,
		primary_msl->memseg_arr.len,
		primary_msl->memseg_arr.elt_sz);
	if (ret < 0) {
		RTE_LOG(ERR, EAL, "Cannot initialize local memory map\n");
		return -1;
	}
	local_msl->base_va = primary_msl->base_va;
	local_msl->len = primary_msl->len;

	return 0;
}

static int
alloc_list(int list_idx, int len)
{
	int *data;
	int i;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* single-file segments mode does not need fd list */
	if (!internal_conf->single_file_segments) {
		/* ensure we have space to store fd per each possible segment */
		data = malloc(sizeof(int) * len);
		if (data == NULL) {
			RTE_LOG(ERR, EAL, "Unable to allocate space for file descriptors\n");
			return -1;
		}
		/* set all fd's as invalid */
		for (i = 0; i < len; i++)
			data[i] = -1;
		fd_list[list_idx].fds = data;
		fd_list[list_idx].len = len;
	} else {
		fd_list[list_idx].fds = NULL;
		fd_list[list_idx].len = 0;
	}

	fd_list[list_idx].count = 0;
	fd_list[list_idx].memseg_list_fd = -1;

	return 0;
}

static int
fd_list_create_walk(const struct rte_memseg_list *msl,
		void *arg __rte_unused)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	unsigned int len;
	int msl_idx;

	if (msl->external)
		return 0;

	msl_idx = msl - mcfg->memsegs;
	len = msl->memseg_arr.len;

	return alloc_list(msl_idx, len);
}

int
eal_memalloc_set_seg_fd(int list_idx, int seg_idx, int fd)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* single file segments mode doesn't support individual segment fd's */
	if (internal_conf->single_file_segments)
		return -ENOTSUP;

	/* if list is not allocated, allocate it */
	if (fd_list[list_idx].len == 0) {
		int len = mcfg->memsegs[list_idx].memseg_arr.len;

		if (alloc_list(list_idx, len) < 0)
			return -ENOMEM;
	}
	fd_list[list_idx].fds[seg_idx] = fd;

	return 0;
}

int
eal_memalloc_set_seg_list_fd(int list_idx, int fd)
{
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* non-single file segment mode doesn't support segment list fd's */
	if (!internal_conf->single_file_segments)
		return -ENOTSUP;

	fd_list[list_idx].memseg_list_fd = fd;

	return 0;
}

int
eal_memalloc_get_seg_fd(int list_idx, int seg_idx)
{
	int fd;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (internal_conf->in_memory || internal_conf->no_hugetlbfs) {
#ifndef MEMFD_SUPPORTED
		/* in in-memory or no-huge mode, we rely on memfd support */
		return -ENOTSUP;
#endif
		/* memfd supported, but hugetlbfs memfd may not be */
		if (!internal_conf->no_hugetlbfs && !memfd_create_supported)
			return -ENOTSUP;
	}

	if (internal_conf->single_file_segments) {
		fd = fd_list[list_idx].memseg_list_fd;
	} else if (fd_list[list_idx].len == 0) {
		/* list not initialized */
		fd = -1;
	} else {
		fd = fd_list[list_idx].fds[seg_idx];
	}
	if (fd < 0)
		return -ENODEV;
	return fd;
}

static int
test_memfd_create(void)
{
#ifdef MEMFD_SUPPORTED
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();
	unsigned int i;
	for (i = 0; i < internal_conf->num_hugepage_sizes; i++) {
		uint64_t pagesz = internal_conf->hugepage_info[i].hugepage_sz;
		int pagesz_flag = pagesz_flags(pagesz);
		int flags;

		flags = pagesz_flag | RTE_MFD_HUGETLB;
		int fd = memfd_create("test", flags);
		if (fd < 0) {
			/* we failed - let memalloc know this isn't working */
			if (errno == EINVAL) {
				memfd_create_supported = 0;
				return 0; /* not supported */
			}

			/* we got other error - something's wrong */
			return -1; /* error */
		}
		close(fd);
		return 1; /* supported */
	}
#endif
	return 0; /* not supported */
}

int
eal_memalloc_get_seg_fd_offset(int list_idx, int seg_idx, size_t *offset)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (internal_conf->in_memory || internal_conf->no_hugetlbfs) {
#ifndef MEMFD_SUPPORTED
		/* in in-memory or no-huge mode, we rely on memfd support */
		return -ENOTSUP;
#endif
		/* memfd supported, but hugetlbfs memfd may not be */
		if (!internal_conf->no_hugetlbfs && !memfd_create_supported)
			return -ENOTSUP;
	}

	if (internal_conf->single_file_segments) {
		size_t pgsz = mcfg->memsegs[list_idx].page_sz;

		/* segment not active? */
		if (fd_list[list_idx].memseg_list_fd < 0)
			return -ENOENT;
		*offset = pgsz * seg_idx;
	} else {
		/* fd_list not initialized? */
		if (fd_list[list_idx].len == 0)
			return -ENODEV;

		/* segment not active? */
		if (fd_list[list_idx].fds[seg_idx] < 0)
			return -ENOENT;
		*offset = 0;
	}
	return 0;
}

int
eal_memalloc_init(void)
{
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (rte_eal_process_type() == RTE_PROC_SECONDARY)
		if (rte_memseg_list_walk(secondary_msl_create_walk, NULL) < 0)
			return -1;
	if (rte_eal_process_type() == RTE_PROC_PRIMARY &&
			internal_conf->in_memory) {
		int mfd_res = test_memfd_create();

		if (mfd_res < 0) {
			RTE_LOG(ERR, EAL, "Unable to check if memfd is supported\n");
			return -1;
		}
		if (mfd_res == 1)
			RTE_LOG(DEBUG, EAL, "Using memfd for anonymous memory\n");
		else
			RTE_LOG(INFO, EAL, "Using memfd is not supported, falling back to anonymous hugepages\n");

		/* we only support single-file segments mode with in-memory mode
		 * if we support hugetlbfs with memfd_create. this code will
		 * test if we do.
		 */
		if (internal_conf->single_file_segments &&
				mfd_res != 1) {
			RTE_LOG(ERR, EAL, "Single-file segments mode cannot be used without memfd support\n");
			return -1;
		}
		/* this cannot ever happen but better safe than sorry */
		if (!anonymous_hugepages_supported) {
			RTE_LOG(ERR, EAL, "Using anonymous memory is not supported\n");
			return -1;
		}
	}

	/* initialize all of the fd lists */
	if (rte_memseg_list_walk(fd_list_create_walk, NULL))
		return -1;
	return 0;
}
