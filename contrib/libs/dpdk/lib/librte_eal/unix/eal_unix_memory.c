#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2020 Dmitry Kozlyuk
 */

#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <rte_eal_paging.h>
#include <rte_errno.h>
#include <rte_log.h>

#include "eal_private.h"

#ifdef RTE_EXEC_ENV_LINUX
#define EAL_DONTDUMP MADV_DONTDUMP
#define EAL_DODUMP   MADV_DODUMP
#elif defined RTE_EXEC_ENV_FREEBSD
#define EAL_DONTDUMP MADV_NOCORE
#define EAL_DODUMP   MADV_CORE
#else
#error "madvise doesn't support this OS"
#endif

static void *
mem_map(void *requested_addr, size_t size, int prot, int flags,
	int fd, size_t offset)
{
	void *virt = mmap(requested_addr, size, prot, flags, fd, offset);
	if (virt == MAP_FAILED) {
		RTE_LOG(DEBUG, EAL,
			"Cannot mmap(%p, 0x%zx, 0x%x, 0x%x, %d, 0x%zx): %s\n",
			requested_addr, size, prot, flags, fd, offset,
			strerror(errno));
		rte_errno = errno;
		return NULL;
	}
	return virt;
}

static int
mem_unmap(void *virt, size_t size)
{
	int ret = munmap(virt, size);
	if (ret < 0) {
		RTE_LOG(DEBUG, EAL, "Cannot munmap(%p, 0x%zx): %s\n",
			virt, size, strerror(errno));
		rte_errno = errno;
	}
	return ret;
}

void *
eal_mem_reserve(void *requested_addr, size_t size, int flags)
{
	int sys_flags = MAP_PRIVATE | MAP_ANONYMOUS;

	if (flags & EAL_RESERVE_HUGEPAGES) {
#ifdef MAP_HUGETLB
		sys_flags |= MAP_HUGETLB;
#else
		rte_errno = ENOTSUP;
		return NULL;
#endif
	}

	if (flags & EAL_RESERVE_FORCE_ADDRESS)
		sys_flags |= MAP_FIXED;

	return mem_map(requested_addr, size, PROT_NONE, sys_flags, -1, 0);
}

void
eal_mem_free(void *virt, size_t size)
{
	mem_unmap(virt, size);
}

int
eal_mem_set_dump(void *virt, size_t size, bool dump)
{
	int flags = dump ? EAL_DODUMP : EAL_DONTDUMP;
	int ret = madvise(virt, size, flags);
	if (ret) {
		RTE_LOG(DEBUG, EAL, "madvise(%p, %#zx, %d) failed: %s\n",
				virt, size, flags, strerror(rte_errno));
		rte_errno = errno;
	}
	return ret;
}

static int
mem_rte_to_sys_prot(int prot)
{
	int sys_prot = PROT_NONE;

	if (prot & RTE_PROT_READ)
		sys_prot |= PROT_READ;
	if (prot & RTE_PROT_WRITE)
		sys_prot |= PROT_WRITE;
	if (prot & RTE_PROT_EXECUTE)
		sys_prot |= PROT_EXEC;

	return sys_prot;
}

void *
rte_mem_map(void *requested_addr, size_t size, int prot, int flags,
	int fd, size_t offset)
{
	int sys_flags = 0;
	int sys_prot;

	sys_prot = mem_rte_to_sys_prot(prot);

	if (flags & RTE_MAP_SHARED)
		sys_flags |= MAP_SHARED;
	if (flags & RTE_MAP_ANONYMOUS)
		sys_flags |= MAP_ANONYMOUS;
	if (flags & RTE_MAP_PRIVATE)
		sys_flags |= MAP_PRIVATE;
	if (flags & RTE_MAP_FORCE_ADDRESS)
		sys_flags |= MAP_FIXED;

	return mem_map(requested_addr, size, sys_prot, sys_flags, fd, offset);
}

int
rte_mem_unmap(void *virt, size_t size)
{
	return mem_unmap(virt, size);
}

size_t
rte_mem_page_size(void)
{
	static size_t page_size;

	if (!page_size)
		page_size = sysconf(_SC_PAGESIZE);

	return page_size;
}

int
rte_mem_lock(const void *virt, size_t size)
{
	int ret = mlock(virt, size);
	if (ret)
		rte_errno = errno;
	return ret;
}
