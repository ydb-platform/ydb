#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "env_internal.h"

#include "spdk/version.h"
#include "spdk/env_dpdk.h"
#include "spdk/log.h"

#include <rte_config.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_vfio.h>

#define SPDK_ENV_DPDK_DEFAULT_NAME		"spdk"
#define SPDK_ENV_DPDK_DEFAULT_SHM_ID		-1
#define SPDK_ENV_DPDK_DEFAULT_MEM_SIZE		-1
#define SPDK_ENV_DPDK_DEFAULT_MAIN_CORE		-1
#define SPDK_ENV_DPDK_DEFAULT_MEM_CHANNEL	-1
#define SPDK_ENV_DPDK_DEFAULT_CORE_MASK		"0x1"
#define SPDK_ENV_DPDK_DEFAULT_BASE_VIRTADDR	0x200000000000

#if RTE_VERSION < RTE_VERSION_NUM(20, 11, 0, 0)
#define DPDK_ALLOW_PARAM	"--pci-whitelist"
#define DPDK_BLOCK_PARAM	"--pci-blacklist"
#define DPDK_MAIN_CORE_PARAM	"--master-lcore"
#else
#define DPDK_ALLOW_PARAM	"--allow"
#define DPDK_BLOCK_PARAM	"--block"
#define DPDK_MAIN_CORE_PARAM	"--main-lcore"
#endif

static char **g_eal_cmdline;
static int g_eal_cmdline_argcount;
static bool g_external_init = true;

static char *
_sprintf_alloc(const char *format, ...)
{
	va_list args;
	va_list args_copy;
	char *buf;
	size_t bufsize;
	int rc;

	va_start(args, format);

	/* Try with a small buffer first. */
	bufsize = 32;

	/* Limit maximum buffer size to something reasonable so we don't loop forever. */
	while (bufsize <= 1024 * 1024) {
		buf = malloc(bufsize);
		if (buf == NULL) {
			va_end(args);
			return NULL;
		}

		va_copy(args_copy, args);
		rc = vsnprintf(buf, bufsize, format, args_copy);
		va_end(args_copy);

		/*
		 * If vsnprintf() returned a count within our current buffer size, we are done.
		 * The count does not include the \0 terminator, so rc == bufsize is not OK.
		 */
		if (rc >= 0 && (size_t)rc < bufsize) {
			va_end(args);
			return buf;
		}

		/*
		 * vsnprintf() should return the required space, but some libc versions do not
		 * implement this correctly, so just double the buffer size and try again.
		 *
		 * We don't need the data in buf, so rather than realloc(), use free() and malloc()
		 * again to avoid a copy.
		 */
		free(buf);
		bufsize *= 2;
	}

	va_end(args);
	return NULL;
}

void
spdk_env_opts_init(struct spdk_env_opts *opts)
{
	if (!opts) {
		return;
	}

	memset(opts, 0, sizeof(*opts));

	opts->name = SPDK_ENV_DPDK_DEFAULT_NAME;
	opts->core_mask = SPDK_ENV_DPDK_DEFAULT_CORE_MASK;
	opts->shm_id = SPDK_ENV_DPDK_DEFAULT_SHM_ID;
	opts->mem_size = SPDK_ENV_DPDK_DEFAULT_MEM_SIZE;
	opts->main_core = SPDK_ENV_DPDK_DEFAULT_MAIN_CORE;
	opts->mem_channel = SPDK_ENV_DPDK_DEFAULT_MEM_CHANNEL;
	opts->base_virtaddr = SPDK_ENV_DPDK_DEFAULT_BASE_VIRTADDR;
}

static void
free_args(char **args, int argcount)
{
	int i;

	if (args == NULL) {
		return;
	}

	for (i = 0; i < argcount; i++) {
		free(args[i]);
	}

	if (argcount) {
		free(args);
	}
}

static char **
push_arg(char *args[], int *argcount, char *arg)
{
	char **tmp;

	if (arg == NULL) {
		SPDK_ERRLOG("%s: NULL arg supplied\n", __func__);
		free_args(args, *argcount);
		return NULL;
	}

	tmp = realloc(args, sizeof(char *) * (*argcount + 1));
	if (tmp == NULL) {
		free(arg);
		free_args(args, *argcount);
		return NULL;
	}

	tmp[*argcount] = arg;
	(*argcount)++;

	return tmp;
}

#if defined(__linux__) && defined(__x86_64__)

/* TODO: Can likely get this value from rlimits in the future */
#define SPDK_IOMMU_VA_REQUIRED_WIDTH 48
#define VTD_CAP_MGAW_SHIFT 16
#define VTD_CAP_MGAW_MASK (0x3F << VTD_CAP_MGAW_SHIFT)

static int
get_iommu_width(void)
{
	DIR *dir;
	FILE *file;
	struct dirent *entry;
	char mgaw_path[64];
	char buf[64];
	char *end;
	long long int val;
	int width, tmp;

	dir = opendir("/sys/devices/virtual/iommu/");
	if (dir == NULL) {
		return -EINVAL;
	}

	width = 0;

	while ((entry = readdir(dir)) != NULL) {
		/* Find directories named "dmar0", "dmar1", etc */
		if (strncmp(entry->d_name, "dmar", sizeof("dmar") - 1) != 0) {
			continue;
		}

		tmp = snprintf(mgaw_path, sizeof(mgaw_path), "/sys/devices/virtual/iommu/%s/intel-iommu/cap",
			       entry->d_name);
		if ((unsigned)tmp >= sizeof(mgaw_path)) {
			continue;
		}

		file = fopen(mgaw_path, "r");
		if (file == NULL) {
			continue;
		}

		if (fgets(buf, sizeof(buf), file) == NULL) {
			fclose(file);
			continue;
		}

		val = strtoll(buf, &end, 16);
		if (val == LLONG_MIN || val == LLONG_MAX) {
			fclose(file);
			continue;
		}

		tmp = ((val & VTD_CAP_MGAW_MASK) >> VTD_CAP_MGAW_SHIFT) + 1;
		if (width == 0 || tmp < width) {
			width = tmp;
		}

		fclose(file);
	}

	closedir(dir);

	return width;
}

#endif

static int
build_eal_cmdline(const struct spdk_env_opts *opts)
{
	int argcount = 0;
	char **args;

	args = NULL;

	/* set the program name */
	args = push_arg(args, &argcount, _sprintf_alloc("%s", opts->name));
	if (args == NULL) {
		return -1;
	}

	/* disable shared configuration files when in single process mode. This allows for cleaner shutdown */
	if (opts->shm_id < 0) {
		args = push_arg(args, &argcount, _sprintf_alloc("%s", "--no-shconf"));
		if (args == NULL) {
			return -1;
		}
	}

	/* set the coremask */
	/* NOTE: If coremask starts with '[' and ends with ']' it is a core list
	 */
	if (opts->core_mask[0] == '[') {
		char *l_arg = _sprintf_alloc("-l %s", opts->core_mask + 1);

		if (l_arg != NULL) {
			int len = strlen(l_arg);

			if (l_arg[len - 1] == ']') {
				l_arg[len - 1] = '\0';
			}
		}
		args = push_arg(args, &argcount, l_arg);
	} else {
		args = push_arg(args, &argcount, _sprintf_alloc("-c %s", opts->core_mask));
	}

	if (args == NULL) {
		return -1;
	}

	/* set the memory channel number */
	if (opts->mem_channel > 0) {
		args = push_arg(args, &argcount, _sprintf_alloc("-n %d", opts->mem_channel));
		if (args == NULL) {
			return -1;
		}
	}

	/* set the memory size */
	if (opts->mem_size >= 0) {
		args = push_arg(args, &argcount, _sprintf_alloc("-m %d", opts->mem_size));
		if (args == NULL) {
			return -1;
		}
	}

	/* set the main core */
	if (opts->main_core > 0) {
		args = push_arg(args, &argcount, _sprintf_alloc("%s=%d",
				DPDK_MAIN_CORE_PARAM, opts->main_core));
		if (args == NULL) {
			return -1;
		}
	}

	/* set no pci  if enabled */
	if (opts->no_pci) {
		args = push_arg(args, &argcount, _sprintf_alloc("--no-pci"));
		if (args == NULL) {
			return -1;
		}
	}

	/* create just one hugetlbfs file */
	if (opts->hugepage_single_segments) {
		args = push_arg(args, &argcount, _sprintf_alloc("--single-file-segments"));
		if (args == NULL) {
			return -1;
		}
	}

	/* unlink hugepages after initialization */
	if (opts->unlink_hugepage) {
		args = push_arg(args, &argcount, _sprintf_alloc("--huge-unlink"));
		if (args == NULL) {
			return -1;
		}
	}

	/* use a specific hugetlbfs mount */
	if (opts->hugedir) {
		args = push_arg(args, &argcount, _sprintf_alloc("--huge-dir=%s", opts->hugedir));
		if (args == NULL) {
			return -1;
		}
	}

	if (opts->num_pci_addr) {
		size_t i;
		char bdf[32];
		struct spdk_pci_addr *pci_addr =
				opts->pci_blocked ? opts->pci_blocked : opts->pci_allowed;

		for (i = 0; i < opts->num_pci_addr; i++) {
			spdk_pci_addr_fmt(bdf, 32, &pci_addr[i]);
			args = push_arg(args, &argcount, _sprintf_alloc("%s=%s",
					(opts->pci_blocked ? DPDK_BLOCK_PARAM : DPDK_ALLOW_PARAM),
					bdf));
			if (args == NULL) {
				return -1;
			}
		}
	}

	/* Lower default EAL loglevel to RTE_LOG_NOTICE - normal, but significant messages.
	 * This can be overridden by specifying the same option in opts->env_context
	 */
	args = push_arg(args, &argcount, strdup("--log-level=lib.eal:6"));
	if (args == NULL) {
		return -1;
	}

	/* Lower default CRYPTO loglevel to RTE_LOG_ERR to avoid a ton of init msgs.
	 * This can be overridden by specifying the same option in opts->env_context
	 */
	args = push_arg(args, &argcount, strdup("--log-level=lib.cryptodev:5"));
	if (args == NULL) {
		return -1;
	}

	/* `user1` log type is used by rte_vhost, which prints an INFO log for each received
	 * vhost user message. We don't want that. The same log type is also used by a couple
	 * of other DPDK libs, but none of which we make use right now. If necessary, this can
	 * be overridden via opts->env_context.
	 */
	args = push_arg(args, &argcount, strdup("--log-level=user1:6"));
	if (args == NULL) {
		return -1;
	}

	if (opts->env_context) {
		args = push_arg(args, &argcount, strdup(opts->env_context));
		if (args == NULL) {
			return -1;
		}
	}

#ifdef __linux__

	if (opts->iova_mode) {
		args = push_arg(args, &argcount, _sprintf_alloc("--iova-mode=%s", opts->iova_mode));
		if (args == NULL) {
			return -1;
		}
	} else {
		/* When using vfio with enable_unsafe_noiommu_mode=Y, we need iova-mode=pa,
		 * but DPDK guesses it should be iova-mode=va. Add a check and force
		 * iova-mode=pa here. */
		if (rte_vfio_noiommu_is_enabled()) {
			args = push_arg(args, &argcount, _sprintf_alloc("--iova-mode=pa"));
			if (args == NULL) {
				return -1;
			}
		}

#if defined(__x86_64__)
		/* DPDK by default guesses that it should be using iova-mode=va so that it can
		 * support running as an unprivileged user. However, some systems (especially
		 * virtual machines) don't have an IOMMU capable of handling the full virtual
		 * address space and DPDK doesn't currently catch that. Add a check in SPDK
		 * and force iova-mode=pa here. */
		if (get_iommu_width() < SPDK_IOMMU_VA_REQUIRED_WIDTH) {
			args = push_arg(args, &argcount, _sprintf_alloc("--iova-mode=pa"));
			if (args == NULL) {
				return -1;
			}
		}
#elif defined(__PPC64__)
		/* On Linux + PowerPC, DPDK doesn't support VA mode at all. Unfortunately, it doesn't correctly
		 * auto-detect at the moment, so we'll just force it here. */
		args = push_arg(args, &argcount, _sprintf_alloc("--iova-mode=pa"));
		if (args == NULL) {
			return -1;
		}
#endif
	}


	/* Set the base virtual address - it must be an address that is not in the
	 * ASAN shadow region, otherwise ASAN-enabled builds will ignore the
	 * mmap hint.
	 *
	 * Ref: https://github.com/google/sanitizers/wiki/AddressSanitizerAlgorithm
	 */
	args = push_arg(args, &argcount, _sprintf_alloc("--base-virtaddr=0x%" PRIx64, opts->base_virtaddr));
	if (args == NULL) {
		return -1;
	}

	/* --match-allocation prevents DPDK from merging or splitting system memory allocations under the hood.
	 * This is critical for RDMA when attempting to use an rte_mempool based buffer pool. If DPDK merges two
	 * physically or IOVA contiguous memory regions, then when we go to allocate a buffer pool, it can split
	 * the memory for a buffer over two allocations meaning the buffer will be split over a memory region.
	 */
	if (!opts->env_context || strstr(opts->env_context, "--legacy-mem") == NULL) {
		args = push_arg(args, &argcount, _sprintf_alloc("%s", "--match-allocations"));
		if (args == NULL) {
			return -1;
		}
	}

	if (opts->shm_id < 0) {
		args = push_arg(args, &argcount, _sprintf_alloc("--file-prefix=spdk_pid%d",
				getpid()));
		if (args == NULL) {
			return -1;
		}
	} else {
		args = push_arg(args, &argcount, _sprintf_alloc("--file-prefix=spdk%d",
				opts->shm_id));
		if (args == NULL) {
			return -1;
		}

		/* set the process type */
		args = push_arg(args, &argcount, _sprintf_alloc("--proc-type=auto"));
		if (args == NULL) {
			return -1;
		}
	}
#endif

	g_eal_cmdline = args;
	g_eal_cmdline_argcount = argcount;
	return argcount;
}

int
spdk_env_dpdk_post_init(bool legacy_mem)
{
	int rc;

	pci_env_init();

	rc = mem_map_init(legacy_mem);
	if (rc < 0) {
		SPDK_ERRLOG("Failed to allocate mem_map\n");
		return rc;
	}

	rc = vtophys_init();
	if (rc < 0) {
		SPDK_ERRLOG("Failed to initialize vtophys\n");
		return rc;
	}

	return 0;
}

void
spdk_env_dpdk_post_fini(void)
{
	vtophys_fini();

	pci_env_fini();

	free_args(g_eal_cmdline, g_eal_cmdline_argcount);
	g_eal_cmdline = NULL;
	g_eal_cmdline_argcount = 0;
}

int
spdk_env_init(const struct spdk_env_opts *opts)
{
	char **dpdk_args = NULL;
	int i, rc;
	int orig_optind;
	bool legacy_mem;

	/* If SPDK env has been initialized before, then only pci env requires
	 * reinitialization.
	 */
	if (g_external_init == false) {
		if (opts != NULL) {
			fprintf(stderr, "Invalid arguments to reinitialize SPDK env\n");
			return -EINVAL;
		}

		printf("Starting %s / %s reinitialization...\n", SPDK_VERSION_STRING, rte_version());
		pci_env_reinit();

		return 0;
	}

	if (opts == NULL) {
		fprintf(stderr, "NULL arguments to initialize DPDK\n");
		return -EINVAL;
	}

	rc = build_eal_cmdline(opts);
	if (rc < 0) {
		SPDK_ERRLOG("Invalid arguments to initialize DPDK\n");
		return -EINVAL;
	}

	SPDK_PRINTF("Starting %s / %s initialization...\n", SPDK_VERSION_STRING, rte_version());
	SPDK_PRINTF("[ DPDK EAL parameters: ");
	for (i = 0; i < g_eal_cmdline_argcount; i++) {
		SPDK_PRINTF("%s ", g_eal_cmdline[i]);
	}
	SPDK_PRINTF("]\n");

	/* DPDK rearranges the array we pass to it, so make a copy
	 * before passing so we can still free the individual strings
	 * correctly.
	 */
	dpdk_args = calloc(g_eal_cmdline_argcount, sizeof(char *));
	if (dpdk_args == NULL) {
		SPDK_ERRLOG("Failed to allocate dpdk_args\n");
		return -ENOMEM;
	}
	memcpy(dpdk_args, g_eal_cmdline, sizeof(char *) * g_eal_cmdline_argcount);

	fflush(stdout);
	orig_optind = optind;
	optind = 1;
	rc = rte_eal_init(g_eal_cmdline_argcount, dpdk_args);
	optind = orig_optind;

	free(dpdk_args);

	if (rc < 0) {
		if (rte_errno == EALREADY) {
			SPDK_ERRLOG("DPDK already initialized\n");
		} else {
			SPDK_ERRLOG("Failed to initialize DPDK\n");
		}
		return -rte_errno;
	}

	legacy_mem = false;
	if (opts->env_context && strstr(opts->env_context, "--legacy-mem") != NULL) {
		legacy_mem = true;
	}

	rc = spdk_env_dpdk_post_init(legacy_mem);
	if (rc == 0) {
		g_external_init = false;
	}

	return rc;
}

void
spdk_env_fini(void)
{
	spdk_env_dpdk_post_fini();
}

bool
spdk_env_dpdk_external_init(void)
{
	return g_external_init;
}
