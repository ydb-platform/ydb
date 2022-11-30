#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation.
 * Copyright(c) 2013 6WIND S.A.
 */

#include <errno.h>
#include <fcntl.h>
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
#include <sys/resource.h>
#include <unistd.h>
#include <limits.h>
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

#include <rte_errno.h>
#include <rte_log.h>
#include <rte_memory.h>
#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_common.h>
#include <rte_string_fns.h>

#include "eal_private.h"
#include "eal_memalloc.h"
#include "eal_memcfg.h"
#include "eal_internal_cfg.h"
#include "eal_filesystem.h"
#include "eal_hugepages.h"
#include "eal_options.h"

#define PFN_MASK_SIZE	8

/**
 * @file
 * Huge page mapping under linux
 *
 * To reserve a big contiguous amount of memory, we use the hugepage
 * feature of linux. For that, we need to have hugetlbfs mounted. This
 * code will create many files in this directory (one per page) and
 * map them in virtual memory. For each page, we will retrieve its
 * physical address and remap it in order to have a virtual contiguous
 * zone as well as a physical contiguous zone.
 */

static int phys_addrs_available = -1;

#define RANDOMIZE_VA_SPACE_FILE "/proc/sys/kernel/randomize_va_space"

uint64_t eal_get_baseaddr(void)
{
	/*
	 * Linux kernel uses a really high address as starting address for
	 * serving mmaps calls. If there exists addressing limitations and IOVA
	 * mode is VA, this starting address is likely too high for those
	 * devices. However, it is possible to use a lower address in the
	 * process virtual address space as with 64 bits there is a lot of
	 * available space.
	 *
	 * Current known limitations are 39 or 40 bits. Setting the starting
	 * address at 4GB implies there are 508GB or 1020GB for mapping the
	 * available hugepages. This is likely enough for most systems, although
	 * a device with addressing limitations should call
	 * rte_mem_check_dma_mask for ensuring all memory is within supported
	 * range.
	 */
	return 0x100000000ULL;
}

/*
 * Get physical address of any mapped virtual address in the current process.
 */
phys_addr_t
rte_mem_virt2phy(const void *virtaddr)
{
	int fd, retval;
	uint64_t page, physaddr;
	unsigned long virt_pfn;
	int page_size;
	off_t offset;

	if (phys_addrs_available == 0)
		return RTE_BAD_IOVA;

	/* standard page size */
	page_size = getpagesize();

	fd = open("/proc/self/pagemap", O_RDONLY);
	if (fd < 0) {
		RTE_LOG(INFO, EAL, "%s(): cannot open /proc/self/pagemap: %s\n",
			__func__, strerror(errno));
		return RTE_BAD_IOVA;
	}

	virt_pfn = (unsigned long)virtaddr / page_size;
	offset = sizeof(uint64_t) * virt_pfn;
	if (lseek(fd, offset, SEEK_SET) == (off_t) -1) {
		RTE_LOG(INFO, EAL, "%s(): seek error in /proc/self/pagemap: %s\n",
				__func__, strerror(errno));
		close(fd);
		return RTE_BAD_IOVA;
	}

	retval = read(fd, &page, PFN_MASK_SIZE);
	close(fd);
	if (retval < 0) {
		RTE_LOG(INFO, EAL, "%s(): cannot read /proc/self/pagemap: %s\n",
				__func__, strerror(errno));
		return RTE_BAD_IOVA;
	} else if (retval != PFN_MASK_SIZE) {
		RTE_LOG(INFO, EAL, "%s(): read %d bytes from /proc/self/pagemap "
				"but expected %d:\n",
				__func__, retval, PFN_MASK_SIZE);
		return RTE_BAD_IOVA;
	}

	/*
	 * the pfn (page frame number) are bits 0-54 (see
	 * pagemap.txt in linux Documentation)
	 */
	if ((page & 0x7fffffffffffffULL) == 0)
		return RTE_BAD_IOVA;

	physaddr = ((page & 0x7fffffffffffffULL) * page_size)
		+ ((unsigned long)virtaddr % page_size);

	return physaddr;
}

rte_iova_t
rte_mem_virt2iova(const void *virtaddr)
{
	if (rte_eal_iova_mode() == RTE_IOVA_VA)
		return (uintptr_t)virtaddr;
	return rte_mem_virt2phy(virtaddr);
}

/*
 * For each hugepage in hugepg_tbl, fill the physaddr value. We find
 * it by browsing the /proc/self/pagemap special file.
 */
static int
find_physaddrs(struct hugepage_file *hugepg_tbl, struct hugepage_info *hpi)
{
	unsigned int i;
	phys_addr_t addr;

	for (i = 0; i < hpi->num_pages[0]; i++) {
		addr = rte_mem_virt2phy(hugepg_tbl[i].orig_va);
		if (addr == RTE_BAD_PHYS_ADDR)
			return -1;
		hugepg_tbl[i].physaddr = addr;
	}
	return 0;
}

/*
 * For each hugepage in hugepg_tbl, fill the physaddr value sequentially.
 */
static int
set_physaddrs(struct hugepage_file *hugepg_tbl, struct hugepage_info *hpi)
{
	unsigned int i;
	static phys_addr_t addr;

	for (i = 0; i < hpi->num_pages[0]; i++) {
		hugepg_tbl[i].physaddr = addr;
		addr += hugepg_tbl[i].size;
	}
	return 0;
}

/*
 * Check whether address-space layout randomization is enabled in
 * the kernel. This is important for multi-process as it can prevent
 * two processes mapping data to the same virtual address
 * Returns:
 *    0 - address space randomization disabled
 *    1/2 - address space randomization enabled
 *    negative error code on error
 */
static int
aslr_enabled(void)
{
	char c;
	int retval, fd = open(RANDOMIZE_VA_SPACE_FILE, O_RDONLY);
	if (fd < 0)
		return -errno;
	retval = read(fd, &c, 1);
	close(fd);
	if (retval < 0)
		return -errno;
	if (retval == 0)
		return -EIO;
	switch (c) {
		case '0' : return 0;
		case '1' : return 1;
		case '2' : return 2;
		default: return -EINVAL;
	}
}

static sigjmp_buf huge_jmpenv;

static void huge_sigbus_handler(int signo __rte_unused)
{
	siglongjmp(huge_jmpenv, 1);
}

/* Put setjmp into a wrap method to avoid compiling error. Any non-volatile,
 * non-static local variable in the stack frame calling sigsetjmp might be
 * clobbered by a call to longjmp.
 */
static int huge_wrap_sigsetjmp(void)
{
	return sigsetjmp(huge_jmpenv, 1);
}

#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
/* Callback for numa library. */
void numa_error(char *where)
{
	RTE_LOG(ERR, EAL, "%s failed: %s\n", where, strerror(errno));
}
#endif

/*
 * Mmap all hugepages of hugepage table: it first open a file in
 * hugetlbfs, then mmap() hugepage_sz data in it. If orig is set, the
 * virtual address is stored in hugepg_tbl[i].orig_va, else it is stored
 * in hugepg_tbl[i].final_va. The second mapping (when orig is 0) tries to
 * map contiguous physical blocks in contiguous virtual blocks.
 */
static unsigned
map_all_hugepages(struct hugepage_file *hugepg_tbl, struct hugepage_info *hpi,
		  uint64_t *essential_memory __rte_unused)
{
	int fd;
	unsigned i;
	void *virtaddr;
#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
	int node_id = -1;
	int essential_prev = 0;
	int oldpolicy;
	struct bitmask *oldmask = NULL;
	bool have_numa = true;
	unsigned long maxnode = 0;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* Check if kernel supports NUMA. */
	if (numa_available() != 0) {
		RTE_LOG(DEBUG, EAL, "NUMA is not supported.\n");
		have_numa = false;
	}

	if (have_numa) {
		RTE_LOG(DEBUG, EAL, "Trying to obtain current memory policy.\n");
		oldmask = numa_allocate_nodemask();
		if (get_mempolicy(&oldpolicy, oldmask->maskp,
				  oldmask->size + 1, 0, 0) < 0) {
			RTE_LOG(ERR, EAL,
				"Failed to get current mempolicy: %s. "
				"Assuming MPOL_DEFAULT.\n", strerror(errno));
			oldpolicy = MPOL_DEFAULT;
		}
		for (i = 0; i < RTE_MAX_NUMA_NODES; i++)
			if (internal_conf->socket_mem[i])
				maxnode = i + 1;
	}
#endif

	for (i = 0; i < hpi->num_pages[0]; i++) {
		struct hugepage_file *hf = &hugepg_tbl[i];
		uint64_t hugepage_sz = hpi->hugepage_sz;

#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
		if (maxnode) {
			unsigned int j;

			for (j = 0; j < maxnode; j++)
				if (essential_memory[j])
					break;

			if (j == maxnode) {
				node_id = (node_id + 1) % maxnode;
				while (!internal_conf->socket_mem[node_id]) {
					node_id++;
					node_id %= maxnode;
				}
				essential_prev = 0;
			} else {
				node_id = j;
				essential_prev = essential_memory[j];

				if (essential_memory[j] < hugepage_sz)
					essential_memory[j] = 0;
				else
					essential_memory[j] -= hugepage_sz;
			}

			RTE_LOG(DEBUG, EAL,
				"Setting policy MPOL_PREFERRED for socket %d\n",
				node_id);
			numa_set_preferred(node_id);
		}
#endif

		hf->file_id = i;
		hf->size = hugepage_sz;
		eal_get_hugefile_path(hf->filepath, sizeof(hf->filepath),
				hpi->hugedir, hf->file_id);
		hf->filepath[sizeof(hf->filepath) - 1] = '\0';

		/* try to create hugepage file */
		fd = open(hf->filepath, O_CREAT | O_RDWR, 0600);
		if (fd < 0) {
			RTE_LOG(DEBUG, EAL, "%s(): open failed: %s\n", __func__,
					strerror(errno));
			goto out;
		}

		/* map the segment, and populate page tables,
		 * the kernel fills this segment with zeros. we don't care where
		 * this gets mapped - we already have contiguous memory areas
		 * ready for us to map into.
		 */
		virtaddr = mmap(NULL, hugepage_sz, PROT_READ | PROT_WRITE,
				MAP_SHARED | MAP_POPULATE, fd, 0);
		if (virtaddr == MAP_FAILED) {
			RTE_LOG(DEBUG, EAL, "%s(): mmap failed: %s\n", __func__,
					strerror(errno));
			close(fd);
			goto out;
		}

		hf->orig_va = virtaddr;

		/* In linux, hugetlb limitations, like cgroup, are
		 * enforced at fault time instead of mmap(), even
		 * with the option of MAP_POPULATE. Kernel will send
		 * a SIGBUS signal. To avoid to be killed, save stack
		 * environment here, if SIGBUS happens, we can jump
		 * back here.
		 */
		if (huge_wrap_sigsetjmp()) {
			RTE_LOG(DEBUG, EAL, "SIGBUS: Cannot mmap more "
				"hugepages of size %u MB\n",
				(unsigned int)(hugepage_sz / 0x100000));
			munmap(virtaddr, hugepage_sz);
			close(fd);
			unlink(hugepg_tbl[i].filepath);
#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
			if (maxnode)
				essential_memory[node_id] =
					essential_prev;
#endif
			goto out;
		}
		*(int *)virtaddr = 0;

		/* set shared lock on the file. */
		if (flock(fd, LOCK_SH) < 0) {
			RTE_LOG(DEBUG, EAL, "%s(): Locking file failed:%s \n",
				__func__, strerror(errno));
			close(fd);
			goto out;
		}

		close(fd);
	}

out:
#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
	if (maxnode) {
		RTE_LOG(DEBUG, EAL,
			"Restoring previous memory policy: %d\n", oldpolicy);
		if (oldpolicy == MPOL_DEFAULT) {
			numa_set_localalloc();
		} else if (set_mempolicy(oldpolicy, oldmask->maskp,
					 oldmask->size + 1) < 0) {
			RTE_LOG(ERR, EAL, "Failed to restore mempolicy: %s\n",
				strerror(errno));
			numa_set_localalloc();
		}
	}
	if (oldmask != NULL)
		numa_free_cpumask(oldmask);
#endif
	return i;
}

/*
 * Parse /proc/self/numa_maps to get the NUMA socket ID for each huge
 * page.
 */
static int
find_numasocket(struct hugepage_file *hugepg_tbl, struct hugepage_info *hpi)
{
	int socket_id;
	char *end, *nodestr;
	unsigned i, hp_count = 0;
	uint64_t virt_addr;
	char buf[BUFSIZ];
	char hugedir_str[PATH_MAX];
	FILE *f;

	f = fopen("/proc/self/numa_maps", "r");
	if (f == NULL) {
		RTE_LOG(NOTICE, EAL, "NUMA support not available"
			" consider that all memory is in socket_id 0\n");
		return 0;
	}

	snprintf(hugedir_str, sizeof(hugedir_str),
			"%s/%s", hpi->hugedir, eal_get_hugefile_prefix());

	/* parse numa map */
	while (fgets(buf, sizeof(buf), f) != NULL) {

		/* ignore non huge page */
		if (strstr(buf, " huge ") == NULL &&
				strstr(buf, hugedir_str) == NULL)
			continue;

		/* get zone addr */
		virt_addr = strtoull(buf, &end, 16);
		if (virt_addr == 0 || end == buf) {
			RTE_LOG(ERR, EAL, "%s(): error in numa_maps parsing\n", __func__);
			goto error;
		}

		/* get node id (socket id) */
		nodestr = strstr(buf, " N");
		if (nodestr == NULL) {
			RTE_LOG(ERR, EAL, "%s(): error in numa_maps parsing\n", __func__);
			goto error;
		}
		nodestr += 2;
		end = strstr(nodestr, "=");
		if (end == NULL) {
			RTE_LOG(ERR, EAL, "%s(): error in numa_maps parsing\n", __func__);
			goto error;
		}
		end[0] = '\0';
		end = NULL;

		socket_id = strtoul(nodestr, &end, 0);
		if ((nodestr[0] == '\0') || (end == NULL) || (*end != '\0')) {
			RTE_LOG(ERR, EAL, "%s(): error in numa_maps parsing\n", __func__);
			goto error;
		}

		/* if we find this page in our mappings, set socket_id */
		for (i = 0; i < hpi->num_pages[0]; i++) {
			void *va = (void *)(unsigned long)virt_addr;
			if (hugepg_tbl[i].orig_va == va) {
				hugepg_tbl[i].socket_id = socket_id;
				hp_count++;
#ifdef RTE_EAL_NUMA_AWARE_HUGEPAGES
				RTE_LOG(DEBUG, EAL,
					"Hugepage %s is on socket %d\n",
					hugepg_tbl[i].filepath, socket_id);
#endif
			}
		}
	}

	if (hp_count < hpi->num_pages[0])
		goto error;

	fclose(f);
	return 0;

error:
	fclose(f);
	return -1;
}

static int
cmp_physaddr(const void *a, const void *b)
{
#ifndef RTE_ARCH_PPC_64
	const struct hugepage_file *p1 = a;
	const struct hugepage_file *p2 = b;
#else
	/* PowerPC needs memory sorted in reverse order from x86 */
	const struct hugepage_file *p1 = b;
	const struct hugepage_file *p2 = a;
#endif
	if (p1->physaddr < p2->physaddr)
		return -1;
	else if (p1->physaddr > p2->physaddr)
		return 1;
	else
		return 0;
}

/*
 * Uses mmap to create a shared memory area for storage of data
 * Used in this file to store the hugepage file map on disk
 */
static void *
create_shared_memory(const char *filename, const size_t mem_size)
{
	void *retval;
	int fd;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* if no shared files mode is used, create anonymous memory instead */
	if (internal_conf->no_shconf) {
		retval = mmap(NULL, mem_size, PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (retval == MAP_FAILED)
			return NULL;
		return retval;
	}

	fd = open(filename, O_CREAT | O_RDWR, 0600);
	if (fd < 0)
		return NULL;
	if (ftruncate(fd, mem_size) < 0) {
		close(fd);
		return NULL;
	}
	retval = mmap(NULL, mem_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);
	if (retval == MAP_FAILED)
		return NULL;
	return retval;
}

/*
 * this copies *active* hugepages from one hugepage table to another.
 * destination is typically the shared memory.
 */
static int
copy_hugepages_to_shared_mem(struct hugepage_file * dst, int dest_size,
		const struct hugepage_file * src, int src_size)
{
	int src_pos, dst_pos = 0;

	for (src_pos = 0; src_pos < src_size; src_pos++) {
		if (src[src_pos].orig_va != NULL) {
			/* error on overflow attempt */
			if (dst_pos == dest_size)
				return -1;
			memcpy(&dst[dst_pos], &src[src_pos], sizeof(struct hugepage_file));
			dst_pos++;
		}
	}
	return 0;
}

static int
unlink_hugepage_files(struct hugepage_file *hugepg_tbl,
		unsigned num_hp_info)
{
	unsigned socket, size;
	int page, nrpages = 0;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* get total number of hugepages */
	for (size = 0; size < num_hp_info; size++)
		for (socket = 0; socket < RTE_MAX_NUMA_NODES; socket++)
			nrpages +=
			internal_conf->hugepage_info[size].num_pages[socket];

	for (page = 0; page < nrpages; page++) {
		struct hugepage_file *hp = &hugepg_tbl[page];

		if (hp->orig_va != NULL && unlink(hp->filepath)) {
			RTE_LOG(WARNING, EAL, "%s(): Removing %s failed: %s\n",
				__func__, hp->filepath, strerror(errno));
		}
	}
	return 0;
}

/*
 * unmaps hugepages that are not going to be used. since we originally allocate
 * ALL hugepages (not just those we need), additional unmapping needs to be done.
 */
static int
unmap_unneeded_hugepages(struct hugepage_file *hugepg_tbl,
		struct hugepage_info *hpi,
		unsigned num_hp_info)
{
	unsigned socket, size;
	int page, nrpages = 0;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* get total number of hugepages */
	for (size = 0; size < num_hp_info; size++)
		for (socket = 0; socket < RTE_MAX_NUMA_NODES; socket++)
			nrpages += internal_conf->hugepage_info[size].num_pages[socket];

	for (size = 0; size < num_hp_info; size++) {
		for (socket = 0; socket < RTE_MAX_NUMA_NODES; socket++) {
			unsigned pages_found = 0;

			/* traverse until we have unmapped all the unused pages */
			for (page = 0; page < nrpages; page++) {
				struct hugepage_file *hp = &hugepg_tbl[page];

				/* find a page that matches the criteria */
				if ((hp->size == hpi[size].hugepage_sz) &&
						(hp->socket_id == (int) socket)) {

					/* if we skipped enough pages, unmap the rest */
					if (pages_found == hpi[size].num_pages[socket]) {
						uint64_t unmap_len;

						unmap_len = hp->size;

						/* get start addr and len of the remaining segment */
						munmap(hp->orig_va,
							(size_t)unmap_len);

						hp->orig_va = NULL;
						if (unlink(hp->filepath) == -1) {
							RTE_LOG(ERR, EAL, "%s(): Removing %s failed: %s\n",
									__func__, hp->filepath, strerror(errno));
							return -1;
						}
					} else {
						/* lock the page and skip */
						pages_found++;
					}

				} /* match page */
			} /* foreach page */
		} /* foreach socket */
	} /* foreach pagesize */

	return 0;
}

static int
remap_segment(struct hugepage_file *hugepages, int seg_start, int seg_end)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct rte_memseg_list *msl;
	struct rte_fbarray *arr;
	int cur_page, seg_len;
	unsigned int msl_idx;
	int ms_idx;
	uint64_t page_sz;
	size_t memseg_len;
	int socket_id;
#ifndef RTE_ARCH_64
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();
#endif
	page_sz = hugepages[seg_start].size;
	socket_id = hugepages[seg_start].socket_id;
	seg_len = seg_end - seg_start;

	RTE_LOG(DEBUG, EAL, "Attempting to map %" PRIu64 "M on socket %i\n",
			(seg_len * page_sz) >> 20ULL, socket_id);

	/* find free space in memseg lists */
	for (msl_idx = 0; msl_idx < RTE_MAX_MEMSEG_LISTS; msl_idx++) {
		bool empty;
		msl = &mcfg->memsegs[msl_idx];
		arr = &msl->memseg_arr;

		if (msl->page_sz != page_sz)
			continue;
		if (msl->socket_id != socket_id)
			continue;

		/* leave space for a hole if array is not empty */
		empty = arr->count == 0;
		ms_idx = rte_fbarray_find_next_n_free(arr, 0,
				seg_len + (empty ? 0 : 1));

		/* memseg list is full? */
		if (ms_idx < 0)
			continue;

		/* leave some space between memsegs, they are not IOVA
		 * contiguous, so they shouldn't be VA contiguous either.
		 */
		if (!empty)
			ms_idx++;
		break;
	}
	if (msl_idx == RTE_MAX_MEMSEG_LISTS) {
		RTE_LOG(ERR, EAL, "Could not find space for memseg. Please increase %s and/or %s in configuration.\n",
				RTE_STR(RTE_MAX_MEMSEG_PER_TYPE),
				RTE_STR(RTE_MAX_MEM_MB_PER_TYPE));
		return -1;
	}

#ifdef RTE_ARCH_PPC_64
	/* for PPC64 we go through the list backwards */
	for (cur_page = seg_end - 1; cur_page >= seg_start;
			cur_page--, ms_idx++) {
#else
	for (cur_page = seg_start; cur_page < seg_end; cur_page++, ms_idx++) {
#endif
		struct hugepage_file *hfile = &hugepages[cur_page];
		struct rte_memseg *ms = rte_fbarray_get(arr, ms_idx);
		void *addr;
		int fd;

		fd = open(hfile->filepath, O_RDWR);
		if (fd < 0) {
			RTE_LOG(ERR, EAL, "Could not open '%s': %s\n",
					hfile->filepath, strerror(errno));
			return -1;
		}
		/* set shared lock on the file. */
		if (flock(fd, LOCK_SH) < 0) {
			RTE_LOG(DEBUG, EAL, "Could not lock '%s': %s\n",
					hfile->filepath, strerror(errno));
			close(fd);
			return -1;
		}
		memseg_len = (size_t)page_sz;
		addr = RTE_PTR_ADD(msl->base_va, ms_idx * memseg_len);

		/* we know this address is already mmapped by memseg list, so
		 * using MAP_FIXED here is safe
		 */
		addr = mmap(addr, page_sz, PROT_READ | PROT_WRITE,
				MAP_SHARED | MAP_POPULATE | MAP_FIXED, fd, 0);
		if (addr == MAP_FAILED) {
			RTE_LOG(ERR, EAL, "Couldn't remap '%s': %s\n",
					hfile->filepath, strerror(errno));
			close(fd);
			return -1;
		}

		/* we have a new address, so unmap previous one */
#ifndef RTE_ARCH_64
		/* in 32-bit legacy mode, we have already unmapped the page */
		if (!internal_conf->legacy_mem)
			munmap(hfile->orig_va, page_sz);
#else
		munmap(hfile->orig_va, page_sz);
#endif

		hfile->orig_va = NULL;
		hfile->final_va = addr;

		/* rewrite physical addresses in IOVA as VA mode */
		if (rte_eal_iova_mode() == RTE_IOVA_VA)
			hfile->physaddr = (uintptr_t)addr;

		/* set up memseg data */
		ms->addr = addr;
		ms->hugepage_sz = page_sz;
		ms->len = memseg_len;
		ms->iova = hfile->physaddr;
		ms->socket_id = hfile->socket_id;
		ms->nchannel = rte_memory_get_nchannel();
		ms->nrank = rte_memory_get_nrank();

		rte_fbarray_set_used(arr, ms_idx);

		/* store segment fd internally */
		if (eal_memalloc_set_seg_fd(msl_idx, ms_idx, fd) < 0)
			RTE_LOG(ERR, EAL, "Could not store segment fd: %s\n",
				rte_strerror(rte_errno));
	}
	RTE_LOG(DEBUG, EAL, "Allocated %" PRIu64 "M on socket %i\n",
			(seg_len * page_sz) >> 20, socket_id);
	return 0;
}

static uint64_t
get_mem_amount(uint64_t page_sz, uint64_t max_mem)
{
	uint64_t area_sz, max_pages;

	/* limit to RTE_MAX_MEMSEG_PER_LIST pages or RTE_MAX_MEM_MB_PER_LIST */
	max_pages = RTE_MAX_MEMSEG_PER_LIST;
	max_mem = RTE_MIN((uint64_t)RTE_MAX_MEM_MB_PER_LIST << 20, max_mem);

	area_sz = RTE_MIN(page_sz * max_pages, max_mem);

	/* make sure the list isn't smaller than the page size */
	area_sz = RTE_MAX(area_sz, page_sz);

	return RTE_ALIGN(area_sz, page_sz);
}

static int
memseg_list_free(struct rte_memseg_list *msl)
{
	if (rte_fbarray_destroy(&msl->memseg_arr)) {
		RTE_LOG(ERR, EAL, "Cannot destroy memseg list\n");
		return -1;
	}
	memset(msl, 0, sizeof(*msl));
	return 0;
}

/*
 * Our VA space is not preallocated yet, so preallocate it here. We need to know
 * how many segments there are in order to map all pages into one address space,
 * and leave appropriate holes between segments so that rte_malloc does not
 * concatenate them into one big segment.
 *
 * we also need to unmap original pages to free up address space.
 */
static int __rte_unused
prealloc_segments(struct hugepage_file *hugepages, int n_pages)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	int cur_page, seg_start_page, end_seg, new_memseg;
	unsigned int hpi_idx, socket, i;
	int n_contig_segs, n_segs;
	int msl_idx;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* before we preallocate segments, we need to free up our VA space.
	 * we're not removing files, and we already have information about
	 * PA-contiguousness, so it is safe to unmap everything.
	 */
	for (cur_page = 0; cur_page < n_pages; cur_page++) {
		struct hugepage_file *hpi = &hugepages[cur_page];
		munmap(hpi->orig_va, hpi->size);
		hpi->orig_va = NULL;
	}

	/* we cannot know how many page sizes and sockets we have discovered, so
	 * loop over all of them
	 */
	for (hpi_idx = 0; hpi_idx < internal_conf->num_hugepage_sizes;
			hpi_idx++) {
		uint64_t page_sz =
			internal_conf->hugepage_info[hpi_idx].hugepage_sz;

		for (i = 0; i < rte_socket_count(); i++) {
			struct rte_memseg_list *msl;

			socket = rte_socket_id_by_idx(i);
			n_contig_segs = 0;
			n_segs = 0;
			seg_start_page = -1;

			for (cur_page = 0; cur_page < n_pages; cur_page++) {
				struct hugepage_file *prev, *cur;
				int prev_seg_start_page = -1;

				cur = &hugepages[cur_page];
				prev = cur_page == 0 ? NULL :
						&hugepages[cur_page - 1];

				new_memseg = 0;
				end_seg = 0;

				if (cur->size == 0)
					end_seg = 1;
				else if (cur->socket_id != (int) socket)
					end_seg = 1;
				else if (cur->size != page_sz)
					end_seg = 1;
				else if (cur_page == 0)
					new_memseg = 1;
#ifdef RTE_ARCH_PPC_64
				/* On PPC64 architecture, the mmap always start
				 * from higher address to lower address. Here,
				 * physical addresses are in descending order.
				 */
				else if ((prev->physaddr - cur->physaddr) !=
						cur->size)
					new_memseg = 1;
#else
				else if ((cur->physaddr - prev->physaddr) !=
						cur->size)
					new_memseg = 1;
#endif
				if (new_memseg) {
					/* if we're already inside a segment,
					 * new segment means end of current one
					 */
					if (seg_start_page != -1) {
						end_seg = 1;
						prev_seg_start_page =
								seg_start_page;
					}
					seg_start_page = cur_page;
				}

				if (end_seg) {
					if (prev_seg_start_page != -1) {
						/* we've found a new segment */
						n_contig_segs++;
						n_segs += cur_page -
							prev_seg_start_page;
					} else if (seg_start_page != -1) {
						/* we didn't find new segment,
						 * but did end current one
						 */
						n_contig_segs++;
						n_segs += cur_page -
								seg_start_page;
						seg_start_page = -1;
						continue;
					} else {
						/* we're skipping this page */
						continue;
					}
				}
				/* segment continues */
			}
			/* check if we missed last segment */
			if (seg_start_page != -1) {
				n_contig_segs++;
				n_segs += cur_page - seg_start_page;
			}

			/* if no segments were found, do not preallocate */
			if (n_segs == 0)
				continue;

			/* we now have total number of pages that we will
			 * allocate for this segment list. add separator pages
			 * to the total count, and preallocate VA space.
			 */
			n_segs += n_contig_segs - 1;

			/* now, preallocate VA space for these segments */

			/* first, find suitable memseg list for this */
			for (msl_idx = 0; msl_idx < RTE_MAX_MEMSEG_LISTS;
					msl_idx++) {
				msl = &mcfg->memsegs[msl_idx];

				if (msl->base_va != NULL)
					continue;
				break;
			}
			if (msl_idx == RTE_MAX_MEMSEG_LISTS) {
				RTE_LOG(ERR, EAL, "Not enough space in memseg lists, please increase %s\n",
					RTE_STR(RTE_MAX_MEMSEG_LISTS));
				return -1;
			}

			/* now, allocate fbarray itself */
			if (eal_memseg_list_init(msl, page_sz, n_segs,
					socket, msl_idx, true) < 0)
				return -1;

			/* finally, allocate VA space */
			if (eal_memseg_list_alloc(msl, 0) < 0) {
				RTE_LOG(ERR, EAL, "Cannot preallocate 0x%"PRIx64"kB hugepages\n",
					page_sz >> 10);
				return -1;
			}
		}
	}
	return 0;
}

/*
 * We cannot reallocate memseg lists on the fly because PPC64 stores pages
 * backwards, therefore we have to process the entire memseg first before
 * remapping it into memseg list VA space.
 */
static int
remap_needed_hugepages(struct hugepage_file *hugepages, int n_pages)
{
	int cur_page, seg_start_page, new_memseg, ret;

	seg_start_page = 0;
	for (cur_page = 0; cur_page < n_pages; cur_page++) {
		struct hugepage_file *prev, *cur;

		new_memseg = 0;

		cur = &hugepages[cur_page];
		prev = cur_page == 0 ? NULL : &hugepages[cur_page - 1];

		/* if size is zero, no more pages left */
		if (cur->size == 0)
			break;

		if (cur_page == 0)
			new_memseg = 1;
		else if (cur->socket_id != prev->socket_id)
			new_memseg = 1;
		else if (cur->size != prev->size)
			new_memseg = 1;
#ifdef RTE_ARCH_PPC_64
		/* On PPC64 architecture, the mmap always start from higher
		 * address to lower address. Here, physical addresses are in
		 * descending order.
		 */
		else if ((prev->physaddr - cur->physaddr) != cur->size)
			new_memseg = 1;
#else
		else if ((cur->physaddr - prev->physaddr) != cur->size)
			new_memseg = 1;
#endif

		if (new_memseg) {
			/* if this isn't the first time, remap segment */
			if (cur_page != 0) {
				ret = remap_segment(hugepages, seg_start_page,
						cur_page);
				if (ret != 0)
					return -1;
			}
			/* remember where we started */
			seg_start_page = cur_page;
		}
		/* continuation of previous memseg */
	}
	/* we were stopped, but we didn't remap the last segment, do it now */
	if (cur_page != 0) {
		ret = remap_segment(hugepages, seg_start_page,
				cur_page);
		if (ret != 0)
			return -1;
	}
	return 0;
}

static inline size_t
eal_get_hugepage_mem_size(void)
{
	uint64_t size = 0;
	unsigned i, j;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	for (i = 0; i < internal_conf->num_hugepage_sizes; i++) {
		struct hugepage_info *hpi = &internal_conf->hugepage_info[i];
		if (strnlen(hpi->hugedir, sizeof(hpi->hugedir)) != 0) {
			for (j = 0; j < RTE_MAX_NUMA_NODES; j++) {
				size += hpi->hugepage_sz * hpi->num_pages[j];
			}
		}
	}

	return (size < SIZE_MAX) ? (size_t)(size) : SIZE_MAX;
}

static struct sigaction huge_action_old;
static int huge_need_recover;

static void
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

static void
huge_recover_sigbus(void)
{
	if (huge_need_recover) {
		sigaction(SIGBUS, &huge_action_old, NULL);
		huge_need_recover = 0;
	}
}

/*
 * Prepare physical memory mapping: fill configuration structure with
 * these infos, return 0 on success.
 *  1. map N huge pages in separate files in hugetlbfs
 *  2. find associated physical addr
 *  3. find associated NUMA socket ID
 *  4. sort all huge pages by physical address
 *  5. remap these N huge pages in the correct order
 *  6. unmap the first mapping
 *  7. fill memsegs in configuration with contiguous zones
 */
static int
eal_legacy_hugepage_init(void)
{
	struct rte_mem_config *mcfg;
	struct hugepage_file *hugepage = NULL, *tmp_hp = NULL;
	struct hugepage_info used_hp[MAX_HUGEPAGE_SIZES];
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	uint64_t memory[RTE_MAX_NUMA_NODES];

	unsigned hp_offset;
	int i, j;
	int nr_hugefiles, nr_hugepages = 0;
	void *addr;

	memset(used_hp, 0, sizeof(used_hp));

	/* get pointer to global configuration */
	mcfg = rte_eal_get_configuration()->mem_config;

	/* hugetlbfs can be disabled */
	if (internal_conf->no_hugetlbfs) {
		void *prealloc_addr;
		size_t mem_sz;
		struct rte_memseg_list *msl;
		int n_segs, fd, flags;
#ifdef MEMFD_SUPPORTED
		int memfd;
#endif
		uint64_t page_sz;

		/* nohuge mode is legacy mode */
		internal_conf->legacy_mem = 1;

		/* nohuge mode is single-file segments mode */
		internal_conf->single_file_segments = 1;

		/* create a memseg list */
		msl = &mcfg->memsegs[0];

		mem_sz = internal_conf->memory;
		page_sz = RTE_PGSIZE_4K;
		n_segs = mem_sz / page_sz;

		if (eal_memseg_list_init_named(
				msl, "nohugemem", page_sz, n_segs, 0, true)) {
			return -1;
		}

		/* set up parameters for anonymous mmap */
		fd = -1;
		flags = MAP_PRIVATE | MAP_ANONYMOUS;

#ifdef MEMFD_SUPPORTED
		/* create a memfd and store it in the segment fd table */
		memfd = memfd_create("nohuge", 0);
		if (memfd < 0) {
			RTE_LOG(DEBUG, EAL, "Cannot create memfd: %s\n",
					strerror(errno));
			RTE_LOG(DEBUG, EAL, "Falling back to anonymous map\n");
		} else {
			/* we got an fd - now resize it */
			if (ftruncate(memfd, internal_conf->memory) < 0) {
				RTE_LOG(ERR, EAL, "Cannot resize memfd: %s\n",
						strerror(errno));
				RTE_LOG(ERR, EAL, "Falling back to anonymous map\n");
				close(memfd);
			} else {
				/* creating memfd-backed file was successful.
				 * we want changes to memfd to be visible to
				 * other processes (such as vhost backend), so
				 * map it as shared memory.
				 */
				RTE_LOG(DEBUG, EAL, "Using memfd for anonymous memory\n");
				fd = memfd;
				flags = MAP_SHARED;
			}
		}
#endif
		/* preallocate address space for the memory, so that it can be
		 * fit into the DMA mask.
		 */
		if (eal_memseg_list_alloc(msl, 0)) {
			RTE_LOG(ERR, EAL, "Cannot preallocate VA space for hugepage memory\n");
			return -1;
		}

		prealloc_addr = msl->base_va;
		addr = mmap(prealloc_addr, mem_sz, PROT_READ | PROT_WRITE,
				flags | MAP_FIXED, fd, 0);
		if (addr == MAP_FAILED || addr != prealloc_addr) {
			RTE_LOG(ERR, EAL, "%s: mmap() failed: %s\n", __func__,
					strerror(errno));
			munmap(prealloc_addr, mem_sz);
			return -1;
		}

		/* we're in single-file segments mode, so only the segment list
		 * fd needs to be set up.
		 */
		if (fd != -1) {
			if (eal_memalloc_set_seg_list_fd(0, fd) < 0) {
				RTE_LOG(ERR, EAL, "Cannot set up segment list fd\n");
				/* not a serious error, proceed */
			}
		}

		eal_memseg_list_populate(msl, addr, n_segs);

		if (mcfg->dma_maskbits &&
		    rte_mem_check_dma_mask_thread_unsafe(mcfg->dma_maskbits)) {
			RTE_LOG(ERR, EAL,
				"%s(): couldn't allocate memory due to IOVA exceeding limits of current DMA mask.\n",
				__func__);
			if (rte_eal_iova_mode() == RTE_IOVA_VA &&
			    rte_eal_using_phys_addrs())
				RTE_LOG(ERR, EAL,
					"%s(): Please try initializing EAL with --iova-mode=pa parameter.\n",
					__func__);
			goto fail;
		}
		return 0;
	}

	/* calculate total number of hugepages available. at this point we haven't
	 * yet started sorting them so they all are on socket 0 */
	for (i = 0; i < (int) internal_conf->num_hugepage_sizes; i++) {
		/* meanwhile, also initialize used_hp hugepage sizes in used_hp */
		used_hp[i].hugepage_sz = internal_conf->hugepage_info[i].hugepage_sz;

		nr_hugepages += internal_conf->hugepage_info[i].num_pages[0];
	}

	/*
	 * allocate a memory area for hugepage table.
	 * this isn't shared memory yet. due to the fact that we need some
	 * processing done on these pages, shared memory will be created
	 * at a later stage.
	 */
	tmp_hp = malloc(nr_hugepages * sizeof(struct hugepage_file));
	if (tmp_hp == NULL)
		goto fail;

	memset(tmp_hp, 0, nr_hugepages * sizeof(struct hugepage_file));

	hp_offset = 0; /* where we start the current page size entries */

	huge_register_sigbus();

	/* make a copy of socket_mem, needed for balanced allocation. */
	for (i = 0; i < RTE_MAX_NUMA_NODES; i++)
		memory[i] = internal_conf->socket_mem[i];

	/* map all hugepages and sort them */
	for (i = 0; i < (int)internal_conf->num_hugepage_sizes; i++) {
		unsigned pages_old, pages_new;
		struct hugepage_info *hpi;

		/*
		 * we don't yet mark hugepages as used at this stage, so
		 * we just map all hugepages available to the system
		 * all hugepages are still located on socket 0
		 */
		hpi = &internal_conf->hugepage_info[i];

		if (hpi->num_pages[0] == 0)
			continue;

		/* map all hugepages available */
		pages_old = hpi->num_pages[0];
		pages_new = map_all_hugepages(&tmp_hp[hp_offset], hpi, memory);
		if (pages_new < pages_old) {
			RTE_LOG(DEBUG, EAL,
				"%d not %d hugepages of size %u MB allocated\n",
				pages_new, pages_old,
				(unsigned)(hpi->hugepage_sz / 0x100000));

			int pages = pages_old - pages_new;

			nr_hugepages -= pages;
			hpi->num_pages[0] = pages_new;
			if (pages_new == 0)
				continue;
		}

		if (rte_eal_using_phys_addrs() &&
				rte_eal_iova_mode() != RTE_IOVA_VA) {
			/* find physical addresses for each hugepage */
			if (find_physaddrs(&tmp_hp[hp_offset], hpi) < 0) {
				RTE_LOG(DEBUG, EAL, "Failed to find phys addr "
					"for %u MB pages\n",
					(unsigned int)(hpi->hugepage_sz / 0x100000));
				goto fail;
			}
		} else {
			/* set physical addresses for each hugepage */
			if (set_physaddrs(&tmp_hp[hp_offset], hpi) < 0) {
				RTE_LOG(DEBUG, EAL, "Failed to set phys addr "
					"for %u MB pages\n",
					(unsigned int)(hpi->hugepage_sz / 0x100000));
				goto fail;
			}
		}

		if (find_numasocket(&tmp_hp[hp_offset], hpi) < 0){
			RTE_LOG(DEBUG, EAL, "Failed to find NUMA socket for %u MB pages\n",
					(unsigned)(hpi->hugepage_sz / 0x100000));
			goto fail;
		}

		qsort(&tmp_hp[hp_offset], hpi->num_pages[0],
		      sizeof(struct hugepage_file), cmp_physaddr);

		/* we have processed a num of hugepages of this size, so inc offset */
		hp_offset += hpi->num_pages[0];
	}

	huge_recover_sigbus();

	if (internal_conf->memory == 0 && internal_conf->force_sockets == 0)
		internal_conf->memory = eal_get_hugepage_mem_size();

	nr_hugefiles = nr_hugepages;


	/* clean out the numbers of pages */
	for (i = 0; i < (int) internal_conf->num_hugepage_sizes; i++)
		for (j = 0; j < RTE_MAX_NUMA_NODES; j++)
			internal_conf->hugepage_info[i].num_pages[j] = 0;

	/* get hugepages for each socket */
	for (i = 0; i < nr_hugefiles; i++) {
		int socket = tmp_hp[i].socket_id;

		/* find a hugepage info with right size and increment num_pages */
		const int nb_hpsizes = RTE_MIN(MAX_HUGEPAGE_SIZES,
				(int)internal_conf->num_hugepage_sizes);
		for (j = 0; j < nb_hpsizes; j++) {
			if (tmp_hp[i].size ==
					internal_conf->hugepage_info[j].hugepage_sz) {
				internal_conf->hugepage_info[j].num_pages[socket]++;
			}
		}
	}

	/* make a copy of socket_mem, needed for number of pages calculation */
	for (i = 0; i < RTE_MAX_NUMA_NODES; i++)
		memory[i] = internal_conf->socket_mem[i];

	/* calculate final number of pages */
	nr_hugepages = eal_dynmem_calc_num_pages_per_socket(memory,
			internal_conf->hugepage_info, used_hp,
			internal_conf->num_hugepage_sizes);

	/* error if not enough memory available */
	if (nr_hugepages < 0)
		goto fail;

	/* reporting in! */
	for (i = 0; i < (int) internal_conf->num_hugepage_sizes; i++) {
		for (j = 0; j < RTE_MAX_NUMA_NODES; j++) {
			if (used_hp[i].num_pages[j] > 0) {
				RTE_LOG(DEBUG, EAL,
					"Requesting %u pages of size %uMB"
					" from socket %i\n",
					used_hp[i].num_pages[j],
					(unsigned)
					(used_hp[i].hugepage_sz / 0x100000),
					j);
			}
		}
	}

	/* create shared memory */
	hugepage = create_shared_memory(eal_hugepage_data_path(),
			nr_hugefiles * sizeof(struct hugepage_file));

	if (hugepage == NULL) {
		RTE_LOG(ERR, EAL, "Failed to create shared memory!\n");
		goto fail;
	}
	memset(hugepage, 0, nr_hugefiles * sizeof(struct hugepage_file));

	/*
	 * unmap pages that we won't need (looks at used_hp).
	 * also, sets final_va to NULL on pages that were unmapped.
	 */
	if (unmap_unneeded_hugepages(tmp_hp, used_hp,
			internal_conf->num_hugepage_sizes) < 0) {
		RTE_LOG(ERR, EAL, "Unmapping and locking hugepages failed!\n");
		goto fail;
	}

	/*
	 * copy stuff from malloc'd hugepage* to the actual shared memory.
	 * this procedure only copies those hugepages that have orig_va
	 * not NULL. has overflow protection.
	 */
	if (copy_hugepages_to_shared_mem(hugepage, nr_hugefiles,
			tmp_hp, nr_hugefiles) < 0) {
		RTE_LOG(ERR, EAL, "Copying tables to shared memory failed!\n");
		goto fail;
	}

#ifndef RTE_ARCH_64
	/* for legacy 32-bit mode, we did not preallocate VA space, so do it */
	if (internal_conf->legacy_mem &&
			prealloc_segments(hugepage, nr_hugefiles)) {
		RTE_LOG(ERR, EAL, "Could not preallocate VA space for hugepages\n");
		goto fail;
	}
#endif

	/* remap all pages we do need into memseg list VA space, so that those
	 * pages become first-class citizens in DPDK memory subsystem
	 */
	if (remap_needed_hugepages(hugepage, nr_hugefiles)) {
		RTE_LOG(ERR, EAL, "Couldn't remap hugepage files into memseg lists\n");
		goto fail;
	}

	/* free the hugepage backing files */
	if (internal_conf->hugepage_unlink &&
		unlink_hugepage_files(tmp_hp, internal_conf->num_hugepage_sizes) < 0) {
		RTE_LOG(ERR, EAL, "Unlinking hugepage files failed!\n");
		goto fail;
	}

	/* free the temporary hugepage table */
	free(tmp_hp);
	tmp_hp = NULL;

	munmap(hugepage, nr_hugefiles * sizeof(struct hugepage_file));
	hugepage = NULL;

	/* we're not going to allocate more pages, so release VA space for
	 * unused memseg lists
	 */
	for (i = 0; i < RTE_MAX_MEMSEG_LISTS; i++) {
		struct rte_memseg_list *msl = &mcfg->memsegs[i];
		size_t mem_sz;

		/* skip inactive lists */
		if (msl->base_va == NULL)
			continue;
		/* skip lists where there is at least one page allocated */
		if (msl->memseg_arr.count > 0)
			continue;
		/* this is an unused list, deallocate it */
		mem_sz = msl->len;
		munmap(msl->base_va, mem_sz);
		msl->base_va = NULL;
		msl->heap = 0;

		/* destroy backing fbarray */
		rte_fbarray_destroy(&msl->memseg_arr);
	}

	if (mcfg->dma_maskbits &&
	    rte_mem_check_dma_mask_thread_unsafe(mcfg->dma_maskbits)) {
		RTE_LOG(ERR, EAL,
			"%s(): couldn't allocate memory due to IOVA exceeding limits of current DMA mask.\n",
			__func__);
		goto fail;
	}

	return 0;

fail:
	huge_recover_sigbus();
	free(tmp_hp);
	if (hugepage != NULL)
		munmap(hugepage, nr_hugefiles * sizeof(struct hugepage_file));

	return -1;
}

/*
 * uses fstat to report the size of a file on disk
 */
static off_t
getFileSize(int fd)
{
	struct stat st;
	if (fstat(fd, &st) < 0)
		return 0;
	return st.st_size;
}

/*
 * This creates the memory mappings in the secondary process to match that of
 * the server process. It goes through each memory segment in the DPDK runtime
 * configuration and finds the hugepages which form that segment, mapping them
 * in order to form a contiguous block in the virtual memory space
 */
static int
eal_legacy_hugepage_attach(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct hugepage_file *hp = NULL;
	unsigned int num_hp = 0;
	unsigned int i = 0;
	unsigned int cur_seg;
	off_t size = 0;
	int fd, fd_hugepage = -1;

	if (aslr_enabled() > 0) {
		RTE_LOG(WARNING, EAL, "WARNING: Address Space Layout Randomization "
				"(ASLR) is enabled in the kernel.\n");
		RTE_LOG(WARNING, EAL, "   This may cause issues with mapping memory "
				"into secondary processes\n");
	}

	fd_hugepage = open(eal_hugepage_data_path(), O_RDONLY);
	if (fd_hugepage < 0) {
		RTE_LOG(ERR, EAL, "Could not open %s\n",
				eal_hugepage_data_path());
		goto error;
	}

	size = getFileSize(fd_hugepage);
	hp = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd_hugepage, 0);
	if (hp == MAP_FAILED) {
		RTE_LOG(ERR, EAL, "Could not mmap %s\n",
				eal_hugepage_data_path());
		goto error;
	}

	num_hp = size / sizeof(struct hugepage_file);
	RTE_LOG(DEBUG, EAL, "Analysing %u files\n", num_hp);

	/* map all segments into memory to make sure we get the addrs. the
	 * segments themselves are already in memseg list (which is shared and
	 * has its VA space already preallocated), so we just need to map
	 * everything into correct addresses.
	 */
	for (i = 0; i < num_hp; i++) {
		struct hugepage_file *hf = &hp[i];
		size_t map_sz = hf->size;
		void *map_addr = hf->final_va;
		int msl_idx, ms_idx;
		struct rte_memseg_list *msl;
		struct rte_memseg *ms;

		/* if size is zero, no more pages left */
		if (map_sz == 0)
			break;

		fd = open(hf->filepath, O_RDWR);
		if (fd < 0) {
			RTE_LOG(ERR, EAL, "Could not open %s: %s\n",
				hf->filepath, strerror(errno));
			goto error;
		}

		map_addr = mmap(map_addr, map_sz, PROT_READ | PROT_WRITE,
				MAP_SHARED | MAP_FIXED, fd, 0);
		if (map_addr == MAP_FAILED) {
			RTE_LOG(ERR, EAL, "Could not map %s: %s\n",
				hf->filepath, strerror(errno));
			goto fd_error;
		}

		/* set shared lock on the file. */
		if (flock(fd, LOCK_SH) < 0) {
			RTE_LOG(DEBUG, EAL, "%s(): Locking file failed: %s\n",
				__func__, strerror(errno));
			goto mmap_error;
		}

		/* find segment data */
		msl = rte_mem_virt2memseg_list(map_addr);
		if (msl == NULL) {
			RTE_LOG(DEBUG, EAL, "%s(): Cannot find memseg list\n",
				__func__);
			goto mmap_error;
		}
		ms = rte_mem_virt2memseg(map_addr, msl);
		if (ms == NULL) {
			RTE_LOG(DEBUG, EAL, "%s(): Cannot find memseg\n",
				__func__);
			goto mmap_error;
		}

		msl_idx = msl - mcfg->memsegs;
		ms_idx = rte_fbarray_find_idx(&msl->memseg_arr, ms);
		if (ms_idx < 0) {
			RTE_LOG(DEBUG, EAL, "%s(): Cannot find memseg idx\n",
				__func__);
			goto mmap_error;
		}

		/* store segment fd internally */
		if (eal_memalloc_set_seg_fd(msl_idx, ms_idx, fd) < 0)
			RTE_LOG(ERR, EAL, "Could not store segment fd: %s\n",
				rte_strerror(rte_errno));
	}
	/* unmap the hugepage config file, since we are done using it */
	munmap(hp, size);
	close(fd_hugepage);
	return 0;

mmap_error:
	munmap(hp[i].final_va, hp[i].size);
fd_error:
	close(fd);
error:
	/* unwind mmap's done so far */
	for (cur_seg = 0; cur_seg < i; cur_seg++)
		munmap(hp[cur_seg].final_va, hp[cur_seg].size);

	if (hp != NULL && hp != MAP_FAILED)
		munmap(hp, size);
	if (fd_hugepage >= 0)
		close(fd_hugepage);
	return -1;
}

static int
eal_hugepage_attach(void)
{
	if (eal_memalloc_sync_with_primary()) {
		RTE_LOG(ERR, EAL, "Could not map memory from primary process\n");
		if (aslr_enabled() > 0)
			RTE_LOG(ERR, EAL, "It is recommended to disable ASLR in the kernel and retry running both primary and secondary processes\n");
		return -1;
	}
	return 0;
}

int
rte_eal_hugepage_init(void)
{
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	return internal_conf->legacy_mem ?
			eal_legacy_hugepage_init() :
			eal_dynmem_hugepage_init();
}

int
rte_eal_hugepage_attach(void)
{
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	return internal_conf->legacy_mem ?
			eal_legacy_hugepage_attach() :
			eal_hugepage_attach();
}

int
rte_eal_using_phys_addrs(void)
{
	if (phys_addrs_available == -1) {
		uint64_t tmp = 0;

		if (rte_eal_has_hugepages() != 0 &&
		    rte_mem_virt2phy(&tmp) != RTE_BAD_PHYS_ADDR)
			phys_addrs_available = 1;
		else
			phys_addrs_available = 0;
	}
	return phys_addrs_available;
}

static int __rte_unused
memseg_primary_init_32(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	int active_sockets, hpi_idx, msl_idx = 0;
	unsigned int socket_id, i;
	struct rte_memseg_list *msl;
	uint64_t extra_mem_per_socket, total_extra_mem, total_requested_mem;
	uint64_t max_mem;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* no-huge does not need this at all */
	if (internal_conf->no_hugetlbfs)
		return 0;

	/* this is a giant hack, but desperate times call for desperate
	 * measures. in legacy 32-bit mode, we cannot preallocate VA space,
	 * because having upwards of 2 gigabytes of VA space already mapped will
	 * interfere with our ability to map and sort hugepages.
	 *
	 * therefore, in legacy 32-bit mode, we will be initializing memseg
	 * lists much later - in eal_memory.c, right after we unmap all the
	 * unneeded pages. this will not affect secondary processes, as those
	 * should be able to mmap the space without (too many) problems.
	 */
	if (internal_conf->legacy_mem)
		return 0;

	/* 32-bit mode is a very special case. we cannot know in advance where
	 * the user will want to allocate their memory, so we have to do some
	 * heuristics.
	 */
	active_sockets = 0;
	total_requested_mem = 0;
	if (internal_conf->force_sockets)
		for (i = 0; i < rte_socket_count(); i++) {
			uint64_t mem;

			socket_id = rte_socket_id_by_idx(i);
			mem = internal_conf->socket_mem[socket_id];

			if (mem == 0)
				continue;

			active_sockets++;
			total_requested_mem += mem;
		}
	else
		total_requested_mem = internal_conf->memory;

	max_mem = (uint64_t)RTE_MAX_MEM_MB << 20;
	if (total_requested_mem > max_mem) {
		RTE_LOG(ERR, EAL, "Invalid parameters: 32-bit process can at most use %uM of memory\n",
				(unsigned int)(max_mem >> 20));
		return -1;
	}
	total_extra_mem = max_mem - total_requested_mem;
	extra_mem_per_socket = active_sockets == 0 ? total_extra_mem :
			total_extra_mem / active_sockets;

	/* the allocation logic is a little bit convoluted, but here's how it
	 * works, in a nutshell:
	 *  - if user hasn't specified on which sockets to allocate memory via
	 *    --socket-mem, we allocate all of our memory on main core socket.
	 *  - if user has specified sockets to allocate memory on, there may be
	 *    some "unused" memory left (e.g. if user has specified --socket-mem
	 *    such that not all memory adds up to 2 gigabytes), so add it to all
	 *    sockets that are in use equally.
	 *
	 * page sizes are sorted by size in descending order, so we can safely
	 * assume that we dispense with bigger page sizes first.
	 */

	/* create memseg lists */
	for (i = 0; i < rte_socket_count(); i++) {
		int hp_sizes = (int) internal_conf->num_hugepage_sizes;
		uint64_t max_socket_mem, cur_socket_mem;
		unsigned int main_lcore_socket;
		struct rte_config *cfg = rte_eal_get_configuration();
		bool skip;

		socket_id = rte_socket_id_by_idx(i);

#ifndef RTE_EAL_NUMA_AWARE_HUGEPAGES
		/* we can still sort pages by socket in legacy mode */
		if (!internal_conf->legacy_mem && socket_id > 0)
			break;
#endif

		/* if we didn't specifically request memory on this socket */
		skip = active_sockets != 0 &&
				internal_conf->socket_mem[socket_id] == 0;
		/* ...or if we didn't specifically request memory on *any*
		 * socket, and this is not main lcore
		 */
		main_lcore_socket = rte_lcore_to_socket_id(cfg->main_lcore);
		skip |= active_sockets == 0 && socket_id != main_lcore_socket;

		if (skip) {
			RTE_LOG(DEBUG, EAL, "Will not preallocate memory on socket %u\n",
					socket_id);
			continue;
		}

		/* max amount of memory on this socket */
		max_socket_mem = (active_sockets != 0 ?
					internal_conf->socket_mem[socket_id] :
					internal_conf->memory) +
					extra_mem_per_socket;
		cur_socket_mem = 0;

		for (hpi_idx = 0; hpi_idx < hp_sizes; hpi_idx++) {
			uint64_t max_pagesz_mem, cur_pagesz_mem = 0;
			uint64_t hugepage_sz;
			struct hugepage_info *hpi;
			int type_msl_idx, max_segs, total_segs = 0;

			hpi = &internal_conf->hugepage_info[hpi_idx];
			hugepage_sz = hpi->hugepage_sz;

			/* check if pages are actually available */
			if (hpi->num_pages[socket_id] == 0)
				continue;

			max_segs = RTE_MAX_MEMSEG_PER_TYPE;
			max_pagesz_mem = max_socket_mem - cur_socket_mem;

			/* make it multiple of page size */
			max_pagesz_mem = RTE_ALIGN_FLOOR(max_pagesz_mem,
					hugepage_sz);

			RTE_LOG(DEBUG, EAL, "Attempting to preallocate "
					"%" PRIu64 "M on socket %i\n",
					max_pagesz_mem >> 20, socket_id);

			type_msl_idx = 0;
			while (cur_pagesz_mem < max_pagesz_mem &&
					total_segs < max_segs) {
				uint64_t cur_mem;
				unsigned int n_segs;

				if (msl_idx >= RTE_MAX_MEMSEG_LISTS) {
					RTE_LOG(ERR, EAL,
						"No more space in memseg lists, please increase %s\n",
						RTE_STR(RTE_MAX_MEMSEG_LISTS));
					return -1;
				}

				msl = &mcfg->memsegs[msl_idx];

				cur_mem = get_mem_amount(hugepage_sz,
						max_pagesz_mem);
				n_segs = cur_mem / hugepage_sz;

				if (eal_memseg_list_init(msl, hugepage_sz,
						n_segs, socket_id, type_msl_idx,
						true)) {
					/* failing to allocate a memseg list is
					 * a serious error.
					 */
					RTE_LOG(ERR, EAL, "Cannot allocate memseg list\n");
					return -1;
				}

				if (eal_memseg_list_alloc(msl, 0)) {
					/* if we couldn't allocate VA space, we
					 * can try with smaller page sizes.
					 */
					RTE_LOG(ERR, EAL, "Cannot allocate VA space for memseg list, retrying with different page size\n");
					/* deallocate memseg list */
					if (memseg_list_free(msl))
						return -1;
					break;
				}

				total_segs += msl->memseg_arr.len;
				cur_pagesz_mem = total_segs * hugepage_sz;
				type_msl_idx++;
				msl_idx++;
			}
			cur_socket_mem += cur_pagesz_mem;
		}
		if (cur_socket_mem == 0) {
			RTE_LOG(ERR, EAL, "Cannot allocate VA space on socket %u\n",
				socket_id);
			return -1;
		}
	}

	return 0;
}

static int __rte_unused
memseg_primary_init(void)
{
	return eal_dynmem_memseg_lists_init();
}

static int
memseg_secondary_init(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	int msl_idx = 0;
	struct rte_memseg_list *msl;

	for (msl_idx = 0; msl_idx < RTE_MAX_MEMSEG_LISTS; msl_idx++) {

		msl = &mcfg->memsegs[msl_idx];

		/* skip empty memseg lists */
		if (msl->memseg_arr.len == 0)
			continue;

		if (rte_fbarray_attach(&msl->memseg_arr)) {
			RTE_LOG(ERR, EAL, "Cannot attach to primary process memseg lists\n");
			return -1;
		}

		/* preallocate VA space */
		if (eal_memseg_list_alloc(msl, 0)) {
			RTE_LOG(ERR, EAL, "Cannot preallocate VA space for hugepage memory\n");
			return -1;
		}
	}

	return 0;
}

int
rte_eal_memseg_init(void)
{
	/* increase rlimit to maximum */
	struct rlimit lim;

#ifndef RTE_EAL_NUMA_AWARE_HUGEPAGES
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();
#endif
	if (getrlimit(RLIMIT_NOFILE, &lim) == 0) {
		/* set limit to maximum */
		lim.rlim_cur = lim.rlim_max;

		if (setrlimit(RLIMIT_NOFILE, &lim) < 0) {
			RTE_LOG(DEBUG, EAL, "Setting maximum number of open files failed: %s\n",
					strerror(errno));
		} else {
			RTE_LOG(DEBUG, EAL, "Setting maximum number of open files to %"
					PRIu64 "\n",
					(uint64_t)lim.rlim_cur);
		}
	} else {
		RTE_LOG(ERR, EAL, "Cannot get current resource limits\n");
	}
#ifndef RTE_EAL_NUMA_AWARE_HUGEPAGES
	if (!internal_conf->legacy_mem && rte_socket_count() > 1) {
		RTE_LOG(WARNING, EAL, "DPDK is running on a NUMA system, but is compiled without NUMA support.\n");
		RTE_LOG(WARNING, EAL, "This will have adverse consequences for performance and usability.\n");
		RTE_LOG(WARNING, EAL, "Please use --"OPT_LEGACY_MEM" option, or recompile with NUMA support.\n");
	}
#endif

	return rte_eal_process_type() == RTE_PROC_PRIMARY ?
#ifndef RTE_ARCH_64
			memseg_primary_init_32() :
#else
			memseg_primary_init() :
#endif
			memseg_secondary_init();
}
