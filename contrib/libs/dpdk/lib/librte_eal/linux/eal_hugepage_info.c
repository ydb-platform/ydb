#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <string.h>
#include <sys/types.h>
#include <sys/file.h>
#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <fnmatch.h>
#include <inttypes.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/queue.h>
#include <sys/stat.h>

#include <linux/mman.h> /* for hugetlb-related flags */

#include <rte_memory.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_debug.h>
#include <rte_log.h>
#include <rte_common.h>
#include "rte_string_fns.h"

#include "eal_private.h"
#include "eal_internal_cfg.h"
#include "eal_hugepages.h"
#include "eal_filesystem.h"

static const char sys_dir_path[] = "/sys/kernel/mm/hugepages";
static const char sys_pages_numa_dir_path[] = "/sys/devices/system/node";

/*
 * Uses mmap to create a shared memory area for storage of data
 * Used in this file to store the hugepage file map on disk
 */
static void *
map_shared_memory(const char *filename, const size_t mem_size, int flags)
{
	void *retval;
	int fd = open(filename, flags, 0600);
	if (fd < 0)
		return NULL;
	if (ftruncate(fd, mem_size) < 0) {
		close(fd);
		return NULL;
	}
	retval = mmap(NULL, mem_size, PROT_READ | PROT_WRITE,
			MAP_SHARED, fd, 0);
	close(fd);
	return retval;
}

static void *
open_shared_memory(const char *filename, const size_t mem_size)
{
	return map_shared_memory(filename, mem_size, O_RDWR);
}

static void *
create_shared_memory(const char *filename, const size_t mem_size)
{
	return map_shared_memory(filename, mem_size, O_RDWR | O_CREAT);
}

static int get_hp_sysfs_value(const char *subdir, const char *file, unsigned long *val)
{
	char path[PATH_MAX];

	snprintf(path, sizeof(path), "%s/%s/%s",
			sys_dir_path, subdir, file);
	return eal_parse_sysfs_value(path, val);
}

/* this function is only called from eal_hugepage_info_init which itself
 * is only called from a primary process */
static uint32_t
get_num_hugepages(const char *subdir)
{
	unsigned long resv_pages, num_pages, over_pages, surplus_pages;
	const char *nr_hp_file = "free_hugepages";
	const char *nr_rsvd_file = "resv_hugepages";
	const char *nr_over_file = "nr_overcommit_hugepages";
	const char *nr_splus_file = "surplus_hugepages";

	/* first, check how many reserved pages kernel reports */
	if (get_hp_sysfs_value(subdir, nr_rsvd_file, &resv_pages) < 0)
		return 0;

	if (get_hp_sysfs_value(subdir, nr_hp_file, &num_pages) < 0)
		return 0;

	if (get_hp_sysfs_value(subdir, nr_over_file, &over_pages) < 0)
		over_pages = 0;

	if (get_hp_sysfs_value(subdir, nr_splus_file, &surplus_pages) < 0)
		surplus_pages = 0;

	/* adjust num_pages */
	if (num_pages >= resv_pages)
		num_pages -= resv_pages;
	else if (resv_pages)
		num_pages = 0;

	if (over_pages >= surplus_pages)
		over_pages -= surplus_pages;
	else
		over_pages = 0;

	if (num_pages == 0 && over_pages == 0)
		RTE_LOG(WARNING, EAL, "No available hugepages reported in %s\n",
				subdir);

	num_pages += over_pages;
	if (num_pages < over_pages) /* overflow */
		num_pages = UINT32_MAX;

	/* we want to return a uint32_t and more than this looks suspicious
	 * anyway ... */
	if (num_pages > UINT32_MAX)
		num_pages = UINT32_MAX;

	return num_pages;
}

static uint32_t
get_num_hugepages_on_node(const char *subdir, unsigned int socket)
{
	char path[PATH_MAX], socketpath[PATH_MAX];
	DIR *socketdir;
	unsigned long num_pages = 0;
	const char *nr_hp_file = "free_hugepages";

	snprintf(socketpath, sizeof(socketpath), "%s/node%u/hugepages",
		sys_pages_numa_dir_path, socket);

	socketdir = opendir(socketpath);
	if (socketdir) {
		/* Keep calm and carry on */
		closedir(socketdir);
	} else {
		/* Can't find socket dir, so ignore it */
		return 0;
	}

	snprintf(path, sizeof(path), "%s/%s/%s",
			socketpath, subdir, nr_hp_file);
	if (eal_parse_sysfs_value(path, &num_pages) < 0)
		return 0;

	if (num_pages == 0)
		RTE_LOG(WARNING, EAL, "No free hugepages reported in %s\n",
				subdir);

	/*
	 * we want to return a uint32_t and more than this looks suspicious
	 * anyway ...
	 */
	if (num_pages > UINT32_MAX)
		num_pages = UINT32_MAX;

	return num_pages;
}

static uint64_t
get_default_hp_size(void)
{
	const char proc_meminfo[] = "/proc/meminfo";
	const char str_hugepagesz[] = "Hugepagesize:";
	unsigned hugepagesz_len = sizeof(str_hugepagesz) - 1;
	char buffer[256];
	unsigned long long size = 0;

	FILE *fd = fopen(proc_meminfo, "r");
	if (fd == NULL)
		rte_panic("Cannot open %s\n", proc_meminfo);
	while(fgets(buffer, sizeof(buffer), fd)){
		if (strncmp(buffer, str_hugepagesz, hugepagesz_len) == 0){
			size = rte_str_to_size(&buffer[hugepagesz_len]);
			break;
		}
	}
	fclose(fd);
	if (size == 0)
		rte_panic("Cannot get default hugepage size from %s\n", proc_meminfo);
	return size;
}

static int
get_hugepage_dir(uint64_t hugepage_sz, char *hugedir, int len)
{
	enum proc_mount_fieldnames {
		DEVICE = 0,
		MOUNTPT,
		FSTYPE,
		OPTIONS,
		_FIELDNAME_MAX
	};
	static uint64_t default_size = 0;
	const char proc_mounts[] = "/proc/mounts";
	const char hugetlbfs_str[] = "hugetlbfs";
	const size_t htlbfs_str_len = sizeof(hugetlbfs_str) - 1;
	const char pagesize_opt[] = "pagesize=";
	const size_t pagesize_opt_len = sizeof(pagesize_opt) - 1;
	const char split_tok = ' ';
	char *splitstr[_FIELDNAME_MAX];
	char buf[BUFSIZ];
	int retval = -1;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	FILE *fd = fopen(proc_mounts, "r");
	if (fd == NULL)
		rte_panic("Cannot open %s\n", proc_mounts);

	if (default_size == 0)
		default_size = get_default_hp_size();

	while (fgets(buf, sizeof(buf), fd)){
		if (rte_strsplit(buf, sizeof(buf), splitstr, _FIELDNAME_MAX,
				split_tok) != _FIELDNAME_MAX) {
			RTE_LOG(ERR, EAL, "Error parsing %s\n", proc_mounts);
			break; /* return NULL */
		}

		/* we have a specified --huge-dir option, only examine that dir */
		if (internal_conf->hugepage_dir != NULL &&
				strcmp(splitstr[MOUNTPT], internal_conf->hugepage_dir) != 0)
			continue;

		if (strncmp(splitstr[FSTYPE], hugetlbfs_str, htlbfs_str_len) == 0){
			const char *pagesz_str = strstr(splitstr[OPTIONS], pagesize_opt);

			/* if no explicit page size, the default page size is compared */
			if (pagesz_str == NULL){
				if (hugepage_sz == default_size){
					strlcpy(hugedir, splitstr[MOUNTPT], len);
					retval = 0;
					break;
				}
			}
			/* there is an explicit page size, so check it */
			else {
				uint64_t pagesz = rte_str_to_size(&pagesz_str[pagesize_opt_len]);
				if (pagesz == hugepage_sz) {
					strlcpy(hugedir, splitstr[MOUNTPT], len);
					retval = 0;
					break;
				}
			}
		} /* end if strncmp hugetlbfs */
	} /* end while fgets */

	fclose(fd);
	return retval;
}

/*
 * Clear the hugepage directory of whatever hugepage files
 * there are. Checks if the file is locked (i.e.
 * if it's in use by another DPDK process).
 */
static int
clear_hugedir(const char * hugedir)
{
	DIR *dir;
	struct dirent *dirent;
	int dir_fd, fd, lck_result;
	const char filter[] = "*map_*"; /* matches hugepage files */

	/* open directory */
	dir = opendir(hugedir);
	if (!dir) {
		RTE_LOG(ERR, EAL, "Unable to open hugepage directory %s\n",
				hugedir);
		goto error;
	}
	dir_fd = dirfd(dir);

	dirent = readdir(dir);
	if (!dirent) {
		RTE_LOG(ERR, EAL, "Unable to read hugepage directory %s\n",
				hugedir);
		goto error;
	}

	while(dirent != NULL){
		/* skip files that don't match the hugepage pattern */
		if (fnmatch(filter, dirent->d_name, 0) > 0) {
			dirent = readdir(dir);
			continue;
		}

		/* try and lock the file */
		fd = openat(dir_fd, dirent->d_name, O_RDONLY);

		/* skip to next file */
		if (fd == -1) {
			dirent = readdir(dir);
			continue;
		}

		/* non-blocking lock */
		lck_result = flock(fd, LOCK_EX | LOCK_NB);

		/* if lock succeeds, remove the file */
		if (lck_result != -1)
			unlinkat(dir_fd, dirent->d_name, 0);
		close (fd);
		dirent = readdir(dir);
	}

	closedir(dir);
	return 0;

error:
	if (dir)
		closedir(dir);

	RTE_LOG(ERR, EAL, "Error while clearing hugepage dir: %s\n",
		strerror(errno));

	return -1;
}

static int
compare_hpi(const void *a, const void *b)
{
	const struct hugepage_info *hpi_a = a;
	const struct hugepage_info *hpi_b = b;

	return hpi_b->hugepage_sz - hpi_a->hugepage_sz;
}

static void
calc_num_pages(struct hugepage_info *hpi, struct dirent *dirent)
{
	uint64_t total_pages = 0;
	unsigned int i;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/*
	 * first, try to put all hugepages into relevant sockets, but
	 * if first attempts fails, fall back to collecting all pages
	 * in one socket and sorting them later
	 */
	total_pages = 0;
	/* we also don't want to do this for legacy init */
	if (!internal_conf->legacy_mem)
		for (i = 0; i < rte_socket_count(); i++) {
			int socket = rte_socket_id_by_idx(i);
			unsigned int num_pages =
					get_num_hugepages_on_node(
						dirent->d_name, socket);
			hpi->num_pages[socket] = num_pages;
			total_pages += num_pages;
		}
	/*
	 * we failed to sort memory from the get go, so fall
	 * back to old way
	 */
	if (total_pages == 0) {
		hpi->num_pages[0] = get_num_hugepages(dirent->d_name);

#ifndef RTE_ARCH_64
		/* for 32-bit systems, limit number of hugepages to
		 * 1GB per page size */
		hpi->num_pages[0] = RTE_MIN(hpi->num_pages[0],
				RTE_PGSIZE_1G / hpi->hugepage_sz);
#endif
	}
}

static int
hugepage_info_init(void)
{	const char dirent_start_text[] = "hugepages-";
	const size_t dirent_start_len = sizeof(dirent_start_text) - 1;
	unsigned int i, num_sizes = 0;
	DIR *dir;
	struct dirent *dirent;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	dir = opendir(sys_dir_path);
	if (dir == NULL) {
		RTE_LOG(ERR, EAL,
			"Cannot open directory %s to read system hugepage info\n",
			sys_dir_path);
		return -1;
	}

	for (dirent = readdir(dir); dirent != NULL; dirent = readdir(dir)) {
		struct hugepage_info *hpi;

		if (strncmp(dirent->d_name, dirent_start_text,
			    dirent_start_len) != 0)
			continue;

		if (num_sizes >= MAX_HUGEPAGE_SIZES)
			break;

		hpi = &internal_conf->hugepage_info[num_sizes];
		hpi->hugepage_sz =
			rte_str_to_size(&dirent->d_name[dirent_start_len]);

		/* first, check if we have a mountpoint */
		if (get_hugepage_dir(hpi->hugepage_sz,
			hpi->hugedir, sizeof(hpi->hugedir)) < 0) {
			uint32_t num_pages;

			num_pages = get_num_hugepages(dirent->d_name);
			if (num_pages > 0)
				RTE_LOG(NOTICE, EAL,
					"%" PRIu32 " hugepages of size "
					"%" PRIu64 " reserved, but no mounted "
					"hugetlbfs found for that size\n",
					num_pages, hpi->hugepage_sz);
			/* if we have kernel support for reserving hugepages
			 * through mmap, and we're in in-memory mode, treat this
			 * page size as valid. we cannot be in legacy mode at
			 * this point because we've checked this earlier in the
			 * init process.
			 */
#ifdef MAP_HUGE_SHIFT
			if (internal_conf->in_memory) {
				RTE_LOG(DEBUG, EAL, "In-memory mode enabled, "
					"hugepages of size %" PRIu64 " bytes "
					"will be allocated anonymously\n",
					hpi->hugepage_sz);
				calc_num_pages(hpi, dirent);
				num_sizes++;
			}
#endif
			continue;
		}

		/* try to obtain a writelock */
		hpi->lock_descriptor = open(hpi->hugedir, O_RDONLY);

		/* if blocking lock failed */
		if (flock(hpi->lock_descriptor, LOCK_EX) == -1) {
			RTE_LOG(CRIT, EAL,
				"Failed to lock hugepage directory!\n");
			break;
		}
		/* clear out the hugepages dir from unused pages */
		if (clear_hugedir(hpi->hugedir) == -1)
			break;

		calc_num_pages(hpi, dirent);

		num_sizes++;
	}
	closedir(dir);

	/* something went wrong, and we broke from the for loop above */
	if (dirent != NULL)
		return -1;

	internal_conf->num_hugepage_sizes = num_sizes;

	/* sort the page directory entries by size, largest to smallest */
	qsort(&internal_conf->hugepage_info[0], num_sizes,
	      sizeof(internal_conf->hugepage_info[0]), compare_hpi);

	/* now we have all info, check we have at least one valid size */
	for (i = 0; i < num_sizes; i++) {
		/* pages may no longer all be on socket 0, so check all */
		unsigned int j, num_pages = 0;
		struct hugepage_info *hpi = &internal_conf->hugepage_info[i];

		for (j = 0; j < RTE_MAX_NUMA_NODES; j++)
			num_pages += hpi->num_pages[j];
		if (num_pages > 0)
			return 0;
	}

	/* no valid hugepage mounts available, return error */
	return -1;
}

/*
 * when we initialize the hugepage info, everything goes
 * to socket 0 by default. it will later get sorted by memory
 * initialization procedure.
 */
int
eal_hugepage_info_init(void)
{
	struct hugepage_info *hpi, *tmp_hpi;
	unsigned int i;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (hugepage_info_init() < 0)
		return -1;

	/* for no shared files mode, we're done */
	if (internal_conf->no_shconf)
		return 0;

	hpi = &internal_conf->hugepage_info[0];

	tmp_hpi = create_shared_memory(eal_hugepage_info_path(),
			sizeof(internal_conf->hugepage_info));
	if (tmp_hpi == NULL) {
		RTE_LOG(ERR, EAL, "Failed to create shared memory!\n");
		return -1;
	}

	memcpy(tmp_hpi, hpi, sizeof(internal_conf->hugepage_info));

	/* we've copied file descriptors along with everything else, but they
	 * will be invalid in secondary process, so overwrite them
	 */
	for (i = 0; i < RTE_DIM(internal_conf->hugepage_info); i++) {
		struct hugepage_info *tmp = &tmp_hpi[i];
		tmp->lock_descriptor = -1;
	}

	if (munmap(tmp_hpi, sizeof(internal_conf->hugepage_info)) < 0) {
		RTE_LOG(ERR, EAL, "Failed to unmap shared memory!\n");
		return -1;
	}
	return 0;
}

int eal_hugepage_info_read(void)
{
	struct internal_config *internal_conf =
		eal_get_internal_configuration();
	struct hugepage_info *hpi = &internal_conf->hugepage_info[0];
	struct hugepage_info *tmp_hpi;

	tmp_hpi = open_shared_memory(eal_hugepage_info_path(),
				  sizeof(internal_conf->hugepage_info));
	if (tmp_hpi == NULL) {
		RTE_LOG(ERR, EAL, "Failed to open shared memory!\n");
		return -1;
	}

	memcpy(hpi, tmp_hpi, sizeof(internal_conf->hugepage_info));

	if (munmap(tmp_hpi, sizeof(internal_conf->hugepage_info)) < 0) {
		RTE_LOG(ERR, EAL, "Failed to unmap shared memory!\n");
		return -1;
	}
	return 0;
}
