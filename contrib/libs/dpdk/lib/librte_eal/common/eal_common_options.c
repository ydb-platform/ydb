#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation.
 * Copyright(c) 2014 6WIND S.A.
 */

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#ifndef RTE_EXEC_ENV_WINDOWS
#include <syslog.h>
#endif
#include <ctype.h>
#include <limits.h>
#include <errno.h>
#include <getopt.h>
#ifndef RTE_EXEC_ENV_WINDOWS
#include <dlfcn.h>
#include <libgen.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#ifndef RTE_EXEC_ENV_WINDOWS
#include <dirent.h>
#endif

#include <rte_string_fns.h>
#include <rte_eal.h>
#include <rte_log.h>
#include <rte_lcore.h>
#include <rte_memory.h>
#include <rte_tailq.h>
#include <rte_version.h>
#include <rte_devargs.h>
#include <rte_memcpy.h>
#ifndef RTE_EXEC_ENV_WINDOWS
#include <rte_telemetry.h>
#endif
#include <rte_vect.h>

#include "eal_internal_cfg.h"
#include "eal_options.h"
#include "eal_filesystem.h"
#include "eal_private.h"
#ifndef RTE_EXEC_ENV_WINDOWS
#include "eal_trace.h"
#endif

#define BITS_PER_HEX 4
#define LCORE_OPT_LST 1
#define LCORE_OPT_MSK 2
#define LCORE_OPT_MAP 3

const char
eal_short_options[] =
	"a:" /* allow */
	"b:" /* block */
	"c:" /* coremask */
	"s:" /* service coremask */
	"d:" /* driver */
	"h"  /* help */
	"l:" /* corelist */
	"S:" /* service corelist */
	"m:" /* memory size */
	"n:" /* memory channels */
	"r:" /* memory ranks */
	"v"  /* version */
	"w:" /* pci-whitelist (deprecated) */
	;

const struct option
eal_long_options[] = {
	{OPT_BASE_VIRTADDR,     1, NULL, OPT_BASE_VIRTADDR_NUM    },
	{OPT_CREATE_UIO_DEV,    0, NULL, OPT_CREATE_UIO_DEV_NUM   },
	{OPT_FILE_PREFIX,       1, NULL, OPT_FILE_PREFIX_NUM      },
	{OPT_HELP,              0, NULL, OPT_HELP_NUM             },
	{OPT_HUGE_DIR,          1, NULL, OPT_HUGE_DIR_NUM         },
	{OPT_HUGE_UNLINK,       0, NULL, OPT_HUGE_UNLINK_NUM      },
	{OPT_IOVA_MODE,	        1, NULL, OPT_IOVA_MODE_NUM        },
	{OPT_LCORES,            1, NULL, OPT_LCORES_NUM           },
	{OPT_LOG_LEVEL,         1, NULL, OPT_LOG_LEVEL_NUM        },
	{OPT_TRACE,             1, NULL, OPT_TRACE_NUM            },
	{OPT_TRACE_DIR,         1, NULL, OPT_TRACE_DIR_NUM        },
	{OPT_TRACE_BUF_SIZE,    1, NULL, OPT_TRACE_BUF_SIZE_NUM   },
	{OPT_TRACE_MODE,        1, NULL, OPT_TRACE_MODE_NUM       },
	{OPT_MASTER_LCORE,      1, NULL, OPT_MASTER_LCORE_NUM     },
	{OPT_MAIN_LCORE,        1, NULL, OPT_MAIN_LCORE_NUM       },
	{OPT_MBUF_POOL_OPS_NAME, 1, NULL, OPT_MBUF_POOL_OPS_NAME_NUM},
	{OPT_NO_HPET,           0, NULL, OPT_NO_HPET_NUM          },
	{OPT_NO_HUGE,           0, NULL, OPT_NO_HUGE_NUM          },
	{OPT_NO_PCI,            0, NULL, OPT_NO_PCI_NUM           },
	{OPT_NO_SHCONF,         0, NULL, OPT_NO_SHCONF_NUM        },
	{OPT_IN_MEMORY,         0, NULL, OPT_IN_MEMORY_NUM        },
	{OPT_DEV_BLOCK,         1, NULL, OPT_DEV_BLOCK_NUM        },
	{OPT_DEV_ALLOW,		1, NULL, OPT_DEV_ALLOW_NUM	  },
	{OPT_PROC_TYPE,         1, NULL, OPT_PROC_TYPE_NUM        },
	{OPT_SOCKET_MEM,        1, NULL, OPT_SOCKET_MEM_NUM       },
	{OPT_SOCKET_LIMIT,      1, NULL, OPT_SOCKET_LIMIT_NUM     },
	{OPT_SYSLOG,            1, NULL, OPT_SYSLOG_NUM           },
	{OPT_VDEV,              1, NULL, OPT_VDEV_NUM             },
	{OPT_VFIO_INTR,         1, NULL, OPT_VFIO_INTR_NUM        },
	{OPT_VFIO_VF_TOKEN,     1, NULL, OPT_VFIO_VF_TOKEN_NUM    },
	{OPT_VMWARE_TSC_MAP,    0, NULL, OPT_VMWARE_TSC_MAP_NUM   },
	{OPT_LEGACY_MEM,        0, NULL, OPT_LEGACY_MEM_NUM       },
	{OPT_SINGLE_FILE_SEGMENTS, 0, NULL, OPT_SINGLE_FILE_SEGMENTS_NUM},
	{OPT_MATCH_ALLOCATIONS, 0, NULL, OPT_MATCH_ALLOCATIONS_NUM},
	{OPT_TELEMETRY,         0, NULL, OPT_TELEMETRY_NUM        },
	{OPT_NO_TELEMETRY,      0, NULL, OPT_NO_TELEMETRY_NUM     },
	{OPT_FORCE_MAX_SIMD_BITWIDTH, 1, NULL, OPT_FORCE_MAX_SIMD_BITWIDTH_NUM},

	/* legacy options that will be removed in future */
	{OPT_PCI_BLACKLIST,     1, NULL, OPT_PCI_BLACKLIST_NUM    },
	{OPT_PCI_WHITELIST,     1, NULL, OPT_PCI_WHITELIST_NUM    },

	{0,                     0, NULL, 0                        }
};

TAILQ_HEAD(shared_driver_list, shared_driver);

/* Definition for shared object drivers. */
struct shared_driver {
	TAILQ_ENTRY(shared_driver) next;

	char    name[PATH_MAX];
	void*   lib_handle;
};

/* List of external loadable drivers */
static struct shared_driver_list solib_list =
TAILQ_HEAD_INITIALIZER(solib_list);

#ifndef RTE_EXEC_ENV_WINDOWS
/* Default path of external loadable drivers */
static const char *default_solib_dir = RTE_EAL_PMD_PATH;
#endif

/*
 * Stringified version of solib path used by dpdk-pmdinfo.py
 * Note: PLEASE DO NOT ALTER THIS without making a corresponding
 * change to usertools/dpdk-pmdinfo.py
 */
static const char dpdk_solib_path[] __rte_used =
"DPDK_PLUGIN_PATH=" RTE_EAL_PMD_PATH;

TAILQ_HEAD(device_option_list, device_option);

struct device_option {
	TAILQ_ENTRY(device_option) next;

	enum rte_devtype type;
	char arg[];
};

static struct device_option_list devopt_list =
TAILQ_HEAD_INITIALIZER(devopt_list);

static int main_lcore_parsed;
static int mem_parsed;
static int core_parsed;

/* Allow the application to print its usage message too if set */
static rte_usage_hook_t rte_application_usage_hook;

/* Returns rte_usage_hook_t */
rte_usage_hook_t
eal_get_application_usage_hook(void)
{
	return rte_application_usage_hook;
}

/* Set a per-application usage message */
rte_usage_hook_t
rte_set_application_usage_hook(rte_usage_hook_t usage_func)
{
	rte_usage_hook_t old_func;

	/* Will be NULL on the first call to denote the last usage routine. */
	old_func = rte_application_usage_hook;
	rte_application_usage_hook = usage_func;

	return old_func;
}

#ifndef RTE_EXEC_ENV_WINDOWS
static char **eal_args;
static char **eal_app_args;

#define EAL_PARAM_REQ "/eal/params"
#define EAL_APP_PARAM_REQ "/eal/app_params"

/* callback handler for telemetry library to report out EAL flags */
int
handle_eal_info_request(const char *cmd, const char *params __rte_unused,
		struct rte_tel_data *d)
{
	char **args;
	int used = 0;
	int i = 0;

	if (strcmp(cmd, EAL_PARAM_REQ) == 0)
		args = eal_args;
	else
		args = eal_app_args;

	rte_tel_data_start_array(d, RTE_TEL_STRING_VAL);
	if (args == NULL || args[0] == NULL)
		return 0;

	for ( ; args[i] != NULL; i++)
		used = rte_tel_data_add_array_string(d, args[i]);
	return used;
}

int
eal_save_args(int argc, char **argv)
{
	int i, j;

	rte_telemetry_register_cmd(EAL_PARAM_REQ, handle_eal_info_request,
			"Returns EAL commandline parameters used. Takes no parameters");
	rte_telemetry_register_cmd(EAL_APP_PARAM_REQ, handle_eal_info_request,
			"Returns app commandline parameters used. Takes no parameters");

	/* clone argv to report out later. We overprovision, but
	 * this does not waste huge amounts of memory
	 */
	eal_args = calloc(argc + 1, sizeof(*eal_args));
	if (eal_args == NULL)
		return -1;

	for (i = 0; i < argc; i++) {
		eal_args[i] = strdup(argv[i]);
		if (strcmp(argv[i], "--") == 0)
			break;
	}
	eal_args[i++] = NULL; /* always finish with NULL */

	/* allow reporting of any app args we know about too */
	if (i >= argc)
		return 0;

	eal_app_args = calloc(argc - i + 1, sizeof(*eal_args));
	if (eal_app_args == NULL)
		return -1;

	for (j = 0; i < argc; j++, i++)
		eal_app_args[j] = strdup(argv[i]);
	eal_app_args[j] = NULL;

	return 0;
}
#endif

static int
eal_option_device_add(enum rte_devtype type, const char *optarg)
{
	struct device_option *devopt;
	size_t optlen;
	int ret;

	optlen = strlen(optarg) + 1;
	devopt = calloc(1, sizeof(*devopt) + optlen);
	if (devopt == NULL) {
		RTE_LOG(ERR, EAL, "Unable to allocate device option\n");
		return -ENOMEM;
	}

	devopt->type = type;
	ret = strlcpy(devopt->arg, optarg, optlen);
	if (ret < 0) {
		RTE_LOG(ERR, EAL, "Unable to copy device option\n");
		free(devopt);
		return -EINVAL;
	}
	TAILQ_INSERT_TAIL(&devopt_list, devopt, next);
	return 0;
}

int
eal_option_device_parse(void)
{
	struct device_option *devopt;
	void *tmp;
	int ret = 0;

	TAILQ_FOREACH_SAFE(devopt, &devopt_list, next, tmp) {
		if (ret == 0) {
			ret = rte_devargs_add(devopt->type, devopt->arg);
			if (ret)
				RTE_LOG(ERR, EAL, "Unable to parse device '%s'\n",
					devopt->arg);
		}
		TAILQ_REMOVE(&devopt_list, devopt, next);
		free(devopt);
	}
	return ret;
}

const char *
eal_get_hugefile_prefix(void)
{
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (internal_conf->hugefile_prefix != NULL)
		return internal_conf->hugefile_prefix;
	return HUGEFILE_PREFIX_DEFAULT;
}

void
eal_reset_internal_config(struct internal_config *internal_cfg)
{
	int i;

	internal_cfg->memory = 0;
	internal_cfg->force_nrank = 0;
	internal_cfg->force_nchannel = 0;
	internal_cfg->hugefile_prefix = NULL;
	internal_cfg->hugepage_dir = NULL;
	internal_cfg->force_sockets = 0;
	/* zero out the NUMA config */
	for (i = 0; i < RTE_MAX_NUMA_NODES; i++)
		internal_cfg->socket_mem[i] = 0;
	internal_cfg->force_socket_limits = 0;
	/* zero out the NUMA limits config */
	for (i = 0; i < RTE_MAX_NUMA_NODES; i++)
		internal_cfg->socket_limit[i] = 0;
	/* zero out hugedir descriptors */
	for (i = 0; i < MAX_HUGEPAGE_SIZES; i++) {
		memset(&internal_cfg->hugepage_info[i], 0,
				sizeof(internal_cfg->hugepage_info[0]));
		internal_cfg->hugepage_info[i].lock_descriptor = -1;
	}
	internal_cfg->base_virtaddr = 0;

#ifdef LOG_DAEMON
	internal_cfg->syslog_facility = LOG_DAEMON;
#endif

	/* if set to NONE, interrupt mode is determined automatically */
	internal_cfg->vfio_intr_mode = RTE_INTR_MODE_NONE;
	memset(internal_cfg->vfio_vf_token, 0,
			sizeof(internal_cfg->vfio_vf_token));

#ifdef RTE_LIBEAL_USE_HPET
	internal_cfg->no_hpet = 0;
#else
	internal_cfg->no_hpet = 1;
#endif
	internal_cfg->vmware_tsc_map = 0;
	internal_cfg->create_uio_dev = 0;
	internal_cfg->iova_mode = RTE_IOVA_DC;
	internal_cfg->user_mbuf_pool_ops_name = NULL;
	CPU_ZERO(&internal_cfg->ctrl_cpuset);
	internal_cfg->init_complete = 0;
	internal_cfg->max_simd_bitwidth.bitwidth = RTE_VECT_DEFAULT_SIMD_BITWIDTH;
	internal_cfg->max_simd_bitwidth.forced = 0;
}

static int
eal_plugin_add(const char *path)
{
	struct shared_driver *solib;

	solib = malloc(sizeof(*solib));
	if (solib == NULL) {
		RTE_LOG(ERR, EAL, "malloc(solib) failed\n");
		return -1;
	}
	memset(solib, 0, sizeof(*solib));
	strlcpy(solib->name, path, PATH_MAX);
	TAILQ_INSERT_TAIL(&solib_list, solib, next);

	return 0;
}

#ifdef RTE_EXEC_ENV_WINDOWS
int
eal_plugins_init(void)
{
	return 0;
}
#else

static int
eal_plugindir_init(const char *path)
{
	DIR *d = NULL;
	struct dirent *dent = NULL;
	char sopath[PATH_MAX];

	if (path == NULL || *path == '\0')
		return 0;

	d = opendir(path);
	if (d == NULL) {
		RTE_LOG(ERR, EAL, "failed to open directory %s: %s\n",
			path, strerror(errno));
		return -1;
	}

	while ((dent = readdir(d)) != NULL) {
		struct stat sb;
		int nlen = strnlen(dent->d_name, sizeof(dent->d_name));

		/* check if name ends in .so or .so.ABI_VERSION */
		if (strcmp(&dent->d_name[nlen - 3], ".so") != 0 &&
		    strcmp(&dent->d_name[nlen - 4 - strlen(ABI_VERSION)],
			   ".so."ABI_VERSION) != 0)
			continue;

		snprintf(sopath, sizeof(sopath), "%s/%s", path, dent->d_name);

		/* if a regular file, add to list to load */
		if (!(stat(sopath, &sb) == 0 && S_ISREG(sb.st_mode)))
			continue;

		if (eal_plugin_add(sopath) == -1)
			break;
	}

	closedir(d);
	/* XXX this ignores failures from readdir() itself */
	return (dent == NULL) ? 0 : -1;
}

static int
verify_perms(const char *dirpath)
{
	struct stat st;

	/* if not root, check down one level first */
	if (strcmp(dirpath, "/") != 0) {
		static __thread char last_dir_checked[PATH_MAX];
		char copy[PATH_MAX];
		const char *dir;

		strlcpy(copy, dirpath, PATH_MAX);
		dir = dirname(copy);
		if (strncmp(dir, last_dir_checked, PATH_MAX) != 0) {
			if (verify_perms(dir) != 0)
				return -1;
			strlcpy(last_dir_checked, dir, PATH_MAX);
		}
	}

	/* call stat to check for permissions and ensure not world writable */
	if (stat(dirpath, &st) != 0) {
		RTE_LOG(ERR, EAL, "Error with stat on %s, %s\n",
				dirpath, strerror(errno));
		return -1;
	}
	if (st.st_mode & S_IWOTH) {
		RTE_LOG(ERR, EAL,
				"Error, directory path %s is world-writable and insecure\n",
				dirpath);
		return -1;
	}

	return 0;
}

static void *
eal_dlopen(const char *pathname)
{
	void *retval = NULL;
	char *realp = realpath(pathname, NULL);

	if (realp == NULL && errno == ENOENT) {
		/* not a full or relative path, try a load from system dirs */
		retval = dlopen(pathname, RTLD_NOW);
		if (retval == NULL)
			RTE_LOG(ERR, EAL, "%s\n", dlerror());
		return retval;
	}
	if (realp == NULL) {
		RTE_LOG(ERR, EAL, "Error with realpath for %s, %s\n",
				pathname, strerror(errno));
		goto out;
	}
	if (strnlen(realp, PATH_MAX) == PATH_MAX) {
		RTE_LOG(ERR, EAL, "Error, driver path greater than PATH_MAX\n");
		goto out;
	}

	/* do permissions checks */
	if (verify_perms(realp) != 0)
		goto out;

	retval = dlopen(realp, RTLD_NOW);
	if (retval == NULL)
		RTE_LOG(ERR, EAL, "%s\n", dlerror());
out:
	free(realp);
	return retval;
}

int
eal_plugins_init(void)
{
	struct shared_driver *solib = NULL;
	struct stat sb;

	/* If we are not statically linked, add default driver loading
	 * path if it exists as a directory.
	 * (Using dlopen with NOLOAD flag on EAL, will return NULL if the EAL
	 * shared library is not already loaded i.e. it's statically linked.)
	 */
	if (dlopen("librte_eal.so."ABI_VERSION, RTLD_LAZY | RTLD_NOLOAD) != NULL &&
			*default_solib_dir != '\0' &&
			stat(default_solib_dir, &sb) == 0 &&
			S_ISDIR(sb.st_mode))
		eal_plugin_add(default_solib_dir);

	TAILQ_FOREACH(solib, &solib_list, next) {

		if (stat(solib->name, &sb) == 0 && S_ISDIR(sb.st_mode)) {
			if (eal_plugindir_init(solib->name) == -1) {
				RTE_LOG(ERR, EAL,
					"Cannot init plugin directory %s\n",
					solib->name);
				return -1;
			}
		} else {
			RTE_LOG(DEBUG, EAL, "open shared lib %s\n",
				solib->name);
			solib->lib_handle = eal_dlopen(solib->name);
			if (solib->lib_handle == NULL)
				return -1;
		}

	}
	return 0;
}
#endif

/*
 * Parse the coremask given as argument (hexadecimal string) and fill
 * the global configuration (core role and core count) with the parsed
 * value.
 */
static int xdigit2val(unsigned char c)
{
	int val;

	if (isdigit(c))
		val = c - '0';
	else if (isupper(c))
		val = c - 'A' + 10;
	else
		val = c - 'a' + 10;
	return val;
}

static int
eal_parse_service_coremask(const char *coremask)
{
	struct rte_config *cfg = rte_eal_get_configuration();
	int i, j, idx = 0;
	unsigned int count = 0;
	char c;
	int val;
	uint32_t taken_lcore_count = 0;

	if (coremask == NULL)
		return -1;
	/* Remove all blank characters ahead and after .
	 * Remove 0x/0X if exists.
	 */
	while (isblank(*coremask))
		coremask++;
	if (coremask[0] == '0' && ((coremask[1] == 'x')
		|| (coremask[1] == 'X')))
		coremask += 2;
	i = strlen(coremask);
	while ((i > 0) && isblank(coremask[i - 1]))
		i--;

	if (i == 0)
		return -1;

	for (i = i - 1; i >= 0 && idx < RTE_MAX_LCORE; i--) {
		c = coremask[i];
		if (isxdigit(c) == 0) {
			/* invalid characters */
			return -1;
		}
		val = xdigit2val(c);
		for (j = 0; j < BITS_PER_HEX && idx < RTE_MAX_LCORE;
				j++, idx++) {
			if ((1 << j) & val) {
				/* handle main lcore already parsed */
				uint32_t lcore = idx;
				if (main_lcore_parsed &&
						cfg->main_lcore == lcore) {
					RTE_LOG(ERR, EAL,
						"lcore %u is main lcore, cannot use as service core\n",
						idx);
					return -1;
				}

				if (eal_cpu_detected(idx) == 0) {
					RTE_LOG(ERR, EAL,
						"lcore %u unavailable\n", idx);
					return -1;
				}

				if (cfg->lcore_role[idx] == ROLE_RTE)
					taken_lcore_count++;

				lcore_config[idx].core_role = ROLE_SERVICE;
				count++;
			}
		}
	}

	for (; i >= 0; i--)
		if (coremask[i] != '0')
			return -1;

	for (; idx < RTE_MAX_LCORE; idx++)
		lcore_config[idx].core_index = -1;

	if (count == 0)
		return -1;

	if (core_parsed && taken_lcore_count != count) {
		RTE_LOG(WARNING, EAL,
			"Not all service cores are in the coremask. "
			"Please ensure -c or -l includes service cores\n");
	}

	cfg->service_lcore_count = count;
	return 0;
}

static int
eal_service_cores_parsed(void)
{
	int idx;
	for (idx = 0; idx < RTE_MAX_LCORE; idx++) {
		if (lcore_config[idx].core_role == ROLE_SERVICE)
			return 1;
	}
	return 0;
}

static int
update_lcore_config(int *cores)
{
	struct rte_config *cfg = rte_eal_get_configuration();
	unsigned int count = 0;
	unsigned int i;
	int ret = 0;

	for (i = 0; i < RTE_MAX_LCORE; i++) {
		if (cores[i] != -1) {
			if (eal_cpu_detected(i) == 0) {
				RTE_LOG(ERR, EAL, "lcore %u unavailable\n", i);
				ret = -1;
				continue;
			}
			cfg->lcore_role[i] = ROLE_RTE;
			count++;
		} else {
			cfg->lcore_role[i] = ROLE_OFF;
		}
		lcore_config[i].core_index = cores[i];
	}
	if (!ret)
		cfg->lcore_count = count;
	return ret;
}

static int
eal_parse_coremask(const char *coremask, int *cores)
{
	unsigned count = 0;
	int i, j, idx;
	int val;
	char c;

	for (idx = 0; idx < RTE_MAX_LCORE; idx++)
		cores[idx] = -1;
	idx = 0;

	/* Remove all blank characters ahead and after .
	 * Remove 0x/0X if exists.
	 */
	while (isblank(*coremask))
		coremask++;
	if (coremask[0] == '0' && ((coremask[1] == 'x')
		|| (coremask[1] == 'X')))
		coremask += 2;
	i = strlen(coremask);
	while ((i > 0) && isblank(coremask[i - 1]))
		i--;
	if (i == 0)
		return -1;

	for (i = i - 1; i >= 0 && idx < RTE_MAX_LCORE; i--) {
		c = coremask[i];
		if (isxdigit(c) == 0) {
			/* invalid characters */
			return -1;
		}
		val = xdigit2val(c);
		for (j = 0; j < BITS_PER_HEX && idx < RTE_MAX_LCORE; j++, idx++)
		{
			if ((1 << j) & val) {
				cores[idx] = count;
				count++;
			}
		}
	}
	for (; i >= 0; i--)
		if (coremask[i] != '0')
			return -1;
	if (count == 0)
		return -1;
	return 0;
}

static int
eal_parse_service_corelist(const char *corelist)
{
	struct rte_config *cfg = rte_eal_get_configuration();
	int i, idx = 0;
	unsigned count = 0;
	char *end = NULL;
	int min, max;
	uint32_t taken_lcore_count = 0;

	if (corelist == NULL)
		return -1;

	/* Remove all blank characters ahead and after */
	while (isblank(*corelist))
		corelist++;
	i = strlen(corelist);
	while ((i > 0) && isblank(corelist[i - 1]))
		i--;

	/* Get list of cores */
	min = RTE_MAX_LCORE;
	do {
		while (isblank(*corelist))
			corelist++;
		if (*corelist == '\0')
			return -1;
		errno = 0;
		idx = strtoul(corelist, &end, 10);
		if (errno || end == NULL)
			return -1;
		while (isblank(*end))
			end++;
		if (*end == '-') {
			min = idx;
		} else if ((*end == ',') || (*end == '\0')) {
			max = idx;
			if (min == RTE_MAX_LCORE)
				min = idx;
			for (idx = min; idx <= max; idx++) {
				if (cfg->lcore_role[idx] != ROLE_SERVICE) {
					/* handle main lcore already parsed */
					uint32_t lcore = idx;
					if (cfg->main_lcore == lcore &&
							main_lcore_parsed) {
						RTE_LOG(ERR, EAL,
							"Error: lcore %u is main lcore, cannot use as service core\n",
							idx);
						return -1;
					}
					if (cfg->lcore_role[idx] == ROLE_RTE)
						taken_lcore_count++;

					lcore_config[idx].core_role =
							ROLE_SERVICE;
					count++;
				}
			}
			min = RTE_MAX_LCORE;
		} else
			return -1;
		corelist = end + 1;
	} while (*end != '\0');

	if (count == 0)
		return -1;

	if (core_parsed && taken_lcore_count != count) {
		RTE_LOG(WARNING, EAL,
			"Not all service cores were in the coremask. "
			"Please ensure -c or -l includes service cores\n");
	}

	return 0;
}

static int
eal_parse_corelist(const char *corelist, int *cores)
{
	unsigned count = 0;
	char *end = NULL;
	int min, max;
	int idx;

	for (idx = 0; idx < RTE_MAX_LCORE; idx++)
		cores[idx] = -1;

	/* Remove all blank characters ahead */
	while (isblank(*corelist))
		corelist++;

	/* Get list of cores */
	min = RTE_MAX_LCORE;
	do {
		while (isblank(*corelist))
			corelist++;
		if (*corelist == '\0')
			return -1;
		errno = 0;
		idx = strtol(corelist, &end, 10);
		if (errno || end == NULL)
			return -1;
		if (idx < 0 || idx >= RTE_MAX_LCORE)
			return -1;
		while (isblank(*end))
			end++;
		if (*end == '-') {
			min = idx;
		} else if ((*end == ',') || (*end == '\0')) {
			max = idx;
			if (min == RTE_MAX_LCORE)
				min = idx;
			for (idx = min; idx <= max; idx++) {
				if (cores[idx] == -1) {
					cores[idx] = count;
					count++;
				}
			}
			min = RTE_MAX_LCORE;
		} else
			return -1;
		corelist = end + 1;
	} while (*end != '\0');

	if (count == 0)
		return -1;
	return 0;
}

/* Changes the lcore id of the main thread */
static int
eal_parse_main_lcore(const char *arg)
{
	char *parsing_end;
	struct rte_config *cfg = rte_eal_get_configuration();

	errno = 0;
	cfg->main_lcore = (uint32_t) strtol(arg, &parsing_end, 0);
	if (errno || parsing_end[0] != 0)
		return -1;
	if (cfg->main_lcore >= RTE_MAX_LCORE)
		return -1;
	main_lcore_parsed = 1;

	/* ensure main core is not used as service core */
	if (lcore_config[cfg->main_lcore].core_role == ROLE_SERVICE) {
		RTE_LOG(ERR, EAL,
			"Error: Main lcore is used as a service core\n");
		return -1;
	}

	return 0;
}

/*
 * Parse elem, the elem could be single number/range or '(' ')' group
 * 1) A single number elem, it's just a simple digit. e.g. 9
 * 2) A single range elem, two digits with a '-' between. e.g. 2-6
 * 3) A group elem, combines multiple 1) or 2) with '( )'. e.g (0,2-4,6)
 *    Within group elem, '-' used for a range separator;
 *                       ',' used for a single number.
 */
static int
eal_parse_set(const char *input, rte_cpuset_t *set)
{
	unsigned idx;
	const char *str = input;
	char *end = NULL;
	unsigned min, max;

	CPU_ZERO(set);

	while (isblank(*str))
		str++;

	/* only digit or left bracket is qualify for start point */
	if ((!isdigit(*str) && *str != '(') || *str == '\0')
		return -1;

	/* process single number or single range of number */
	if (*str != '(') {
		errno = 0;
		idx = strtoul(str, &end, 10);
		if (errno || end == NULL || idx >= CPU_SETSIZE)
			return -1;
		else {
			while (isblank(*end))
				end++;

			min = idx;
			max = idx;
			if (*end == '-') {
				/* process single <number>-<number> */
				end++;
				while (isblank(*end))
					end++;
				if (!isdigit(*end))
					return -1;

				errno = 0;
				idx = strtoul(end, &end, 10);
				if (errno || end == NULL || idx >= CPU_SETSIZE)
					return -1;
				max = idx;
				while (isblank(*end))
					end++;
				if (*end != ',' && *end != '\0')
					return -1;
			}

			if (*end != ',' && *end != '\0' &&
			    *end != '@')
				return -1;

			for (idx = RTE_MIN(min, max);
			     idx <= RTE_MAX(min, max); idx++)
				CPU_SET(idx, set);

			return end - input;
		}
	}

	/* process set within bracket */
	str++;
	while (isblank(*str))
		str++;
	if (*str == '\0')
		return -1;

	min = RTE_MAX_LCORE;
	do {

		/* go ahead to the first digit */
		while (isblank(*str))
			str++;
		if (!isdigit(*str))
			return -1;

		/* get the digit value */
		errno = 0;
		idx = strtoul(str, &end, 10);
		if (errno || end == NULL || idx >= CPU_SETSIZE)
			return -1;

		/* go ahead to separator '-',',' and ')' */
		while (isblank(*end))
			end++;
		if (*end == '-') {
			if (min == RTE_MAX_LCORE)
				min = idx;
			else /* avoid continuous '-' */
				return -1;
		} else if ((*end == ',') || (*end == ')')) {
			max = idx;
			if (min == RTE_MAX_LCORE)
				min = idx;
			for (idx = RTE_MIN(min, max);
			     idx <= RTE_MAX(min, max); idx++)
				CPU_SET(idx, set);

			min = RTE_MAX_LCORE;
		} else
			return -1;

		str = end + 1;
	} while (*end != '\0' && *end != ')');

	/*
	 * to avoid failure that tail blank makes end character check fail
	 * in eal_parse_lcores( )
	 */
	while (isblank(*str))
		str++;

	return str - input;
}

static int
check_cpuset(rte_cpuset_t *set)
{
	unsigned int idx;

	for (idx = 0; idx < CPU_SETSIZE; idx++) {
		if (!CPU_ISSET(idx, set))
			continue;

		if (eal_cpu_detected(idx) == 0) {
			RTE_LOG(ERR, EAL, "core %u "
				"unavailable\n", idx);
			return -1;
		}
	}
	return 0;
}

/*
 * The format pattern: --lcores='<lcores[@cpus]>[<,lcores[@cpus]>...]'
 * lcores, cpus could be a single digit/range or a group.
 * '(' and ')' are necessary if it's a group.
 * If not supply '@cpus', the value of cpus uses the same as lcores.
 * e.g. '1,2@(5-7),(3-5)@(0,2),(0,6),7-8' means start 9 EAL thread as below
 *   lcore 0 runs on cpuset 0x41 (cpu 0,6)
 *   lcore 1 runs on cpuset 0x2 (cpu 1)
 *   lcore 2 runs on cpuset 0xe0 (cpu 5,6,7)
 *   lcore 3,4,5 runs on cpuset 0x5 (cpu 0,2)
 *   lcore 6 runs on cpuset 0x41 (cpu 0,6)
 *   lcore 7 runs on cpuset 0x80 (cpu 7)
 *   lcore 8 runs on cpuset 0x100 (cpu 8)
 */
static int
eal_parse_lcores(const char *lcores)
{
	struct rte_config *cfg = rte_eal_get_configuration();
	rte_cpuset_t lcore_set;
	unsigned int set_count;
	unsigned idx = 0;
	unsigned count = 0;
	const char *lcore_start = NULL;
	const char *end = NULL;
	int offset;
	rte_cpuset_t cpuset;
	int lflags;
	int ret = -1;

	if (lcores == NULL)
		return -1;

	/* Remove all blank characters ahead and after */
	while (isblank(*lcores))
		lcores++;

	CPU_ZERO(&cpuset);

	/* Reset lcore config */
	for (idx = 0; idx < RTE_MAX_LCORE; idx++) {
		cfg->lcore_role[idx] = ROLE_OFF;
		lcore_config[idx].core_index = -1;
		CPU_ZERO(&lcore_config[idx].cpuset);
	}

	/* Get list of cores */
	do {
		while (isblank(*lcores))
			lcores++;
		if (*lcores == '\0')
			goto err;

		lflags = 0;

		/* record lcore_set start point */
		lcore_start = lcores;

		/* go across a complete bracket */
		if (*lcore_start == '(') {
			lcores += strcspn(lcores, ")");
			if (*lcores++ == '\0')
				goto err;
		}

		/* scan the separator '@', ','(next) or '\0'(finish) */
		lcores += strcspn(lcores, "@,");

		if (*lcores == '@') {
			/* explicit assign cpuset and update the end cursor */
			offset = eal_parse_set(lcores + 1, &cpuset);
			if (offset < 0)
				goto err;
			end = lcores + 1 + offset;
		} else { /* ',' or '\0' */
			/* haven't given cpuset, current loop done */
			end = lcores;

			/* go back to check <number>-<number> */
			offset = strcspn(lcore_start, "(-");
			if (offset < (end - lcore_start) &&
			    *(lcore_start + offset) != '(')
				lflags = 1;
		}

		if (*end != ',' && *end != '\0')
			goto err;

		/* parse lcore_set from start point */
		if (eal_parse_set(lcore_start, &lcore_set) < 0)
			goto err;

		/* without '@', by default using lcore_set as cpuset */
		if (*lcores != '@')
			rte_memcpy(&cpuset, &lcore_set, sizeof(cpuset));

		set_count = CPU_COUNT(&lcore_set);
		/* start to update lcore_set */
		for (idx = 0; idx < RTE_MAX_LCORE; idx++) {
			if (!CPU_ISSET(idx, &lcore_set))
				continue;
			set_count--;

			if (cfg->lcore_role[idx] != ROLE_RTE) {
				lcore_config[idx].core_index = count;
				cfg->lcore_role[idx] = ROLE_RTE;
				count++;
			}

			if (lflags) {
				CPU_ZERO(&cpuset);
				CPU_SET(idx, &cpuset);
			}

			if (check_cpuset(&cpuset) < 0)
				goto err;
			rte_memcpy(&lcore_config[idx].cpuset, &cpuset,
				   sizeof(rte_cpuset_t));
		}

		/* some cores from the lcore_set can't be handled by EAL */
		if (set_count != 0)
			goto err;

		lcores = end + 1;
	} while (*end != '\0');

	if (count == 0)
		goto err;

	cfg->lcore_count = count;
	ret = 0;

err:

	return ret;
}

#ifndef RTE_EXEC_ENV_WINDOWS
static int
eal_parse_syslog(const char *facility, struct internal_config *conf)
{
	int i;
	static const struct {
		const char *name;
		int value;
	} map[] = {
		{ "auth", LOG_AUTH },
		{ "cron", LOG_CRON },
		{ "daemon", LOG_DAEMON },
		{ "ftp", LOG_FTP },
		{ "kern", LOG_KERN },
		{ "lpr", LOG_LPR },
		{ "mail", LOG_MAIL },
		{ "news", LOG_NEWS },
		{ "syslog", LOG_SYSLOG },
		{ "user", LOG_USER },
		{ "uucp", LOG_UUCP },
		{ "local0", LOG_LOCAL0 },
		{ "local1", LOG_LOCAL1 },
		{ "local2", LOG_LOCAL2 },
		{ "local3", LOG_LOCAL3 },
		{ "local4", LOG_LOCAL4 },
		{ "local5", LOG_LOCAL5 },
		{ "local6", LOG_LOCAL6 },
		{ "local7", LOG_LOCAL7 },
		{ NULL, 0 }
	};

	for (i = 0; map[i].name; i++) {
		if (!strcmp(facility, map[i].name)) {
			conf->syslog_facility = map[i].value;
			return 0;
		}
	}
	return -1;
}
#endif

static int
eal_parse_log_priority(const char *level)
{
	static const char * const levels[] = {
		[RTE_LOG_EMERG]   = "emergency",
		[RTE_LOG_ALERT]   = "alert",
		[RTE_LOG_CRIT]    = "critical",
		[RTE_LOG_ERR]     = "error",
		[RTE_LOG_WARNING] = "warning",
		[RTE_LOG_NOTICE]  = "notice",
		[RTE_LOG_INFO]    = "info",
		[RTE_LOG_DEBUG]   = "debug",
	};
	size_t len = strlen(level);
	unsigned long tmp;
	char *end;
	unsigned int i;

	if (len == 0)
		return -1;

	/* look for named values, skip 0 which is not a valid level */
	for (i = 1; i < RTE_DIM(levels); i++) {
		if (strncmp(levels[i], level, len) == 0)
			return i;
	}

	/* not a string, maybe it is numeric */
	errno = 0;
	tmp = strtoul(level, &end, 0);

	/* check for errors */
	if (errno != 0 || end == NULL || *end != '\0' ||
	    tmp >= UINT32_MAX)
		return -1;

	return tmp;
}

static int
eal_parse_log_level(const char *arg)
{
	const char *pattern = NULL;
	const char *regex = NULL;
	char *str, *level;
	int priority;

	str = strdup(arg);
	if (str == NULL)
		return -1;

	if ((level = strchr(str, ','))) {
		regex = str;
		*level++ = '\0';
	} else if ((level = strchr(str, ':'))) {
		pattern = str;
		*level++ = '\0';
	} else {
		level = str;
	}

	priority = eal_parse_log_priority(level);
	if (priority < 0) {
		fprintf(stderr, "invalid log priority: %s\n", level);
		goto fail;
	}

	if (regex) {
		if (rte_log_set_level_regexp(regex, priority) < 0) {
			fprintf(stderr, "cannot set log level %s,%d\n",
				regex, priority);
			goto fail;
		}
		if (rte_log_save_regexp(regex, priority) < 0)
			goto fail;
	} else if (pattern) {
		if (rte_log_set_level_pattern(pattern, priority) < 0) {
			fprintf(stderr, "cannot set log level %s:%d\n",
				pattern, priority);
			goto fail;
		}
		if (rte_log_save_pattern(pattern, priority) < 0)
			goto fail;
	} else {
		rte_log_set_global_level(priority);
	}

	free(str);
	return 0;

fail:
	free(str);
	return -1;
}

static enum rte_proc_type_t
eal_parse_proc_type(const char *arg)
{
	if (strncasecmp(arg, "primary", sizeof("primary")) == 0)
		return RTE_PROC_PRIMARY;
	if (strncasecmp(arg, "secondary", sizeof("secondary")) == 0)
		return RTE_PROC_SECONDARY;
	if (strncasecmp(arg, "auto", sizeof("auto")) == 0)
		return RTE_PROC_AUTO;

	return RTE_PROC_INVALID;
}

static int
eal_parse_iova_mode(const char *name)
{
	int mode;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (name == NULL)
		return -1;

	if (!strcmp("pa", name))
		mode = RTE_IOVA_PA;
	else if (!strcmp("va", name))
		mode = RTE_IOVA_VA;
	else
		return -1;

	internal_conf->iova_mode = mode;
	return 0;
}

static int
eal_parse_simd_bitwidth(const char *arg)
{
	char *end;
	unsigned long bitwidth;
	int ret;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (arg == NULL || arg[0] == '\0')
		return -1;

	errno = 0;
	bitwidth = strtoul(arg, &end, 0);

	/* check for errors */
	if (errno != 0 || end == NULL || *end != '\0' || bitwidth > RTE_VECT_SIMD_MAX)
		return -1;

	if (bitwidth == 0)
		bitwidth = (unsigned long) RTE_VECT_SIMD_MAX;
	ret = rte_vect_set_max_simd_bitwidth(bitwidth);
	if (ret < 0)
		return -1;
	internal_conf->max_simd_bitwidth.forced = 1;
	return 0;
}

static int
eal_parse_base_virtaddr(const char *arg)
{
	char *end;
	uint64_t addr;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	errno = 0;
	addr = strtoull(arg, &end, 16);

	/* check for errors */
	if ((errno != 0) || (arg[0] == '\0') || end == NULL || (*end != '\0'))
		return -1;

	/* make sure we don't exceed 32-bit boundary on 32-bit target */
#ifndef RTE_ARCH_64
	if (addr >= UINTPTR_MAX)
		return -1;
#endif

	/* align the addr on 16M boundary, 16MB is the minimum huge page
	 * size on IBM Power architecture. If the addr is aligned to 16MB,
	 * it can align to 2MB for x86. So this alignment can also be used
	 * on x86 and other architectures.
	 */
	internal_conf->base_virtaddr =
		RTE_PTR_ALIGN_CEIL((uintptr_t)addr, (size_t)RTE_PGSIZE_16M);

	return 0;
}

/* caller is responsible for freeing the returned string */
static char *
available_cores(void)
{
	char *str = NULL;
	int previous;
	int sequence;
	char *tmp;
	int idx;

	/* find the first available cpu */
	for (idx = 0; idx < RTE_MAX_LCORE; idx++) {
		if (eal_cpu_detected(idx) == 0)
			continue;
		break;
	}
	if (idx >= RTE_MAX_LCORE)
		return NULL;

	/* first sequence */
	if (asprintf(&str, "%d", idx) < 0)
		return NULL;
	previous = idx;
	sequence = 0;

	for (idx++ ; idx < RTE_MAX_LCORE; idx++) {
		if (eal_cpu_detected(idx) == 0)
			continue;

		if (idx == previous + 1) {
			previous = idx;
			sequence = 1;
			continue;
		}

		/* finish current sequence */
		if (sequence) {
			if (asprintf(&tmp, "%s-%d", str, previous) < 0) {
				free(str);
				return NULL;
			}
			free(str);
			str = tmp;
		}

		/* new sequence */
		if (asprintf(&tmp, "%s,%d", str, idx) < 0) {
			free(str);
			return NULL;
		}
		free(str);
		str = tmp;
		previous = idx;
		sequence = 0;
	}

	/* finish last sequence */
	if (sequence) {
		if (asprintf(&tmp, "%s-%d", str, previous) < 0) {
			free(str);
			return NULL;
		}
		free(str);
		str = tmp;
	}

	return str;
}

int
eal_parse_common_option(int opt, const char *optarg,
			struct internal_config *conf)
{
	static int b_used;
	static int a_used;

	switch (opt) {
	case OPT_PCI_BLACKLIST_NUM:
		fprintf(stderr,
			"Option --pci-blacklist is deprecated, use -b, --block instead\n");
		/* fallthrough */
	case 'b':
		if (a_used)
			goto ba_conflict;
		if (eal_option_device_add(RTE_DEVTYPE_BLOCKED, optarg) < 0)
			return -1;
		b_used = 1;
		break;

	case 'w':
		fprintf(stderr,
			"Option -w, --pci-whitelist is deprecated, use -a, --allow option instead\n");
		/* fallthrough */
	case 'a':
		if (b_used)
			goto ba_conflict;
		if (eal_option_device_add(RTE_DEVTYPE_ALLOWED, optarg) < 0)
			return -1;
		a_used = 1;
		break;
	/* coremask */
	case 'c': {
		int lcore_indexes[RTE_MAX_LCORE];

		if (eal_service_cores_parsed())
			RTE_LOG(WARNING, EAL,
				"Service cores parsed before dataplane cores. Please ensure -c is before -s or -S\n");
		if (eal_parse_coremask(optarg, lcore_indexes) < 0) {
			RTE_LOG(ERR, EAL, "invalid coremask syntax\n");
			return -1;
		}
		if (update_lcore_config(lcore_indexes) < 0) {
			char *available = available_cores();

			RTE_LOG(ERR, EAL,
				"invalid coremask, please check specified cores are part of %s\n",
				available);
			free(available);
			return -1;
		}

		if (core_parsed) {
			RTE_LOG(ERR, EAL, "Option -c is ignored, because (%s) is set!\n",
				(core_parsed == LCORE_OPT_LST) ? "-l" :
				(core_parsed == LCORE_OPT_MAP) ? "--lcore" :
				"-c");
			return -1;
		}

		core_parsed = LCORE_OPT_MSK;
		break;
	}
	/* corelist */
	case 'l': {
		int lcore_indexes[RTE_MAX_LCORE];

		if (eal_service_cores_parsed())
			RTE_LOG(WARNING, EAL,
				"Service cores parsed before dataplane cores. Please ensure -l is before -s or -S\n");

		if (eal_parse_corelist(optarg, lcore_indexes) < 0) {
			RTE_LOG(ERR, EAL, "invalid core list syntax\n");
			return -1;
		}
		if (update_lcore_config(lcore_indexes) < 0) {
			char *available = available_cores();

			RTE_LOG(ERR, EAL,
				"invalid core list, please check specified cores are part of %s\n",
				available);
			free(available);
			return -1;
		}

		if (core_parsed) {
			RTE_LOG(ERR, EAL, "Option -l is ignored, because (%s) is set!\n",
				(core_parsed == LCORE_OPT_MSK) ? "-c" :
				(core_parsed == LCORE_OPT_MAP) ? "--lcore" :
				"-l");
			return -1;
		}

		core_parsed = LCORE_OPT_LST;
		break;
	}
	/* service coremask */
	case 's':
		if (eal_parse_service_coremask(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid service coremask\n");
			return -1;
		}
		break;
	/* service corelist */
	case 'S':
		if (eal_parse_service_corelist(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid service core list\n");
			return -1;
		}
		break;
	/* size of memory */
	case 'm':
		conf->memory = atoi(optarg);
		conf->memory *= 1024ULL;
		conf->memory *= 1024ULL;
		mem_parsed = 1;
		break;
	/* force number of channels */
	case 'n':
		conf->force_nchannel = atoi(optarg);
		if (conf->force_nchannel == 0) {
			RTE_LOG(ERR, EAL, "invalid channel number\n");
			return -1;
		}
		break;
	/* force number of ranks */
	case 'r':
		conf->force_nrank = atoi(optarg);
		if (conf->force_nrank == 0 ||
		    conf->force_nrank > 16) {
			RTE_LOG(ERR, EAL, "invalid rank number\n");
			return -1;
		}
		break;
	/* force loading of external driver */
	case 'd':
		if (eal_plugin_add(optarg) == -1)
			return -1;
		break;
	case 'v':
		/* since message is explicitly requested by user, we
		 * write message at highest log level so it can always
		 * be seen
		 * even if info or warning messages are disabled */
		RTE_LOG(CRIT, EAL, "RTE Version: '%s'\n", rte_version());
		break;

	/* long options */
	case OPT_HUGE_UNLINK_NUM:
		conf->hugepage_unlink = 1;
		break;

	case OPT_NO_HUGE_NUM:
		conf->no_hugetlbfs = 1;
		/* no-huge is legacy mem */
		conf->legacy_mem = 1;
		break;

	case OPT_NO_PCI_NUM:
		conf->no_pci = 1;
		break;

	case OPT_NO_HPET_NUM:
		conf->no_hpet = 1;
		break;

	case OPT_VMWARE_TSC_MAP_NUM:
		conf->vmware_tsc_map = 1;
		break;

	case OPT_NO_SHCONF_NUM:
		conf->no_shconf = 1;
		break;

	case OPT_IN_MEMORY_NUM:
		conf->in_memory = 1;
		/* in-memory is a superset of noshconf and huge-unlink */
		conf->no_shconf = 1;
		conf->hugepage_unlink = 1;
		break;

	case OPT_PROC_TYPE_NUM:
		conf->process_type = eal_parse_proc_type(optarg);
		break;

	case OPT_MASTER_LCORE_NUM:
		fprintf(stderr,
			"Option --" OPT_MASTER_LCORE
			" is deprecated use " OPT_MAIN_LCORE "\n");
		/* fallthrough */
	case OPT_MAIN_LCORE_NUM:
		if (eal_parse_main_lcore(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameter for --"
					OPT_MAIN_LCORE "\n");
			return -1;
		}
		break;

	case OPT_VDEV_NUM:
		if (eal_option_device_add(RTE_DEVTYPE_VIRTUAL,
				optarg) < 0) {
			return -1;
		}
		break;

#ifndef RTE_EXEC_ENV_WINDOWS
	case OPT_SYSLOG_NUM:
		if (eal_parse_syslog(optarg, conf) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameters for --"
					OPT_SYSLOG "\n");
			return -1;
		}
		break;
#endif

	case OPT_LOG_LEVEL_NUM: {
		if (eal_parse_log_level(optarg) < 0) {
			RTE_LOG(ERR, EAL,
				"invalid parameters for --"
				OPT_LOG_LEVEL "\n");
			return -1;
		}
		break;
	}

#ifndef RTE_EXEC_ENV_WINDOWS
	case OPT_TRACE_NUM: {
		if (eal_trace_args_save(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameters for --"
				OPT_TRACE "\n");
			return -1;
		}
		break;
	}

	case OPT_TRACE_DIR_NUM: {
		if (eal_trace_dir_args_save(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameters for --"
				OPT_TRACE_DIR "\n");
			return -1;
		}
		break;
	}

	case OPT_TRACE_BUF_SIZE_NUM: {
		if (eal_trace_bufsz_args_save(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameters for --"
				OPT_TRACE_BUF_SIZE "\n");
			return -1;
		}
		break;
	}

	case OPT_TRACE_MODE_NUM: {
		if (eal_trace_mode_args_save(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameters for --"
				OPT_TRACE_MODE "\n");
			return -1;
		}
		break;
	}
#endif /* !RTE_EXEC_ENV_WINDOWS */

	case OPT_LCORES_NUM:
		if (eal_parse_lcores(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameter for --"
				OPT_LCORES "\n");
			return -1;
		}

		if (core_parsed) {
			RTE_LOG(ERR, EAL, "Option --lcore is ignored, because (%s) is set!\n",
				(core_parsed == LCORE_OPT_LST) ? "-l" :
				(core_parsed == LCORE_OPT_MSK) ? "-c" :
				"--lcore");
			return -1;
		}

		core_parsed = LCORE_OPT_MAP;
		break;
	case OPT_LEGACY_MEM_NUM:
		conf->legacy_mem = 1;
		break;
	case OPT_SINGLE_FILE_SEGMENTS_NUM:
		conf->single_file_segments = 1;
		break;
	case OPT_IOVA_MODE_NUM:
		if (eal_parse_iova_mode(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameters for --"
				OPT_IOVA_MODE "\n");
			return -1;
		}
		break;
	case OPT_BASE_VIRTADDR_NUM:
		if (eal_parse_base_virtaddr(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameter for --"
					OPT_BASE_VIRTADDR "\n");
			return -1;
		}
		break;
	case OPT_TELEMETRY_NUM:
		break;
	case OPT_NO_TELEMETRY_NUM:
		conf->no_telemetry = 1;
		break;
	case OPT_FORCE_MAX_SIMD_BITWIDTH_NUM:
		if (eal_parse_simd_bitwidth(optarg) < 0) {
			RTE_LOG(ERR, EAL, "invalid parameter for --"
					OPT_FORCE_MAX_SIMD_BITWIDTH "\n");
			return -1;
		}
		break;

	/* don't know what to do, leave this to caller */
	default:
		return 1;

	}

	return 0;

ba_conflict:
	RTE_LOG(ERR, EAL,
		"Options allow (-a) and block (-b) can't be used at the same time\n");
	return -1;
}

static void
eal_auto_detect_cores(struct rte_config *cfg)
{
	unsigned int lcore_id;
	unsigned int removed = 0;
	rte_cpuset_t affinity_set;

	if (pthread_getaffinity_np(pthread_self(), sizeof(rte_cpuset_t),
				&affinity_set))
		CPU_ZERO(&affinity_set);

	for (lcore_id = 0; lcore_id < RTE_MAX_LCORE; lcore_id++) {
		if (cfg->lcore_role[lcore_id] == ROLE_RTE &&
		    !CPU_ISSET(lcore_id, &affinity_set)) {
			cfg->lcore_role[lcore_id] = ROLE_OFF;
			removed++;
		}
	}

	cfg->lcore_count -= removed;
}

static void
compute_ctrl_threads_cpuset(struct internal_config *internal_cfg)
{
	rte_cpuset_t *cpuset = &internal_cfg->ctrl_cpuset;
	rte_cpuset_t default_set;
	unsigned int lcore_id;

	for (lcore_id = 0; lcore_id < RTE_MAX_LCORE; lcore_id++) {
		if (rte_lcore_has_role(lcore_id, ROLE_OFF))
			continue;
		RTE_CPU_OR(cpuset, cpuset, &lcore_config[lcore_id].cpuset);
	}
	RTE_CPU_NOT(cpuset, cpuset);

	if (pthread_getaffinity_np(pthread_self(), sizeof(rte_cpuset_t),
				&default_set))
		CPU_ZERO(&default_set);

	RTE_CPU_AND(cpuset, cpuset, &default_set);

	/* if no remaining cpu, use main lcore cpu affinity */
	if (!CPU_COUNT(cpuset)) {
		memcpy(cpuset, &lcore_config[rte_get_main_lcore()].cpuset,
			sizeof(*cpuset));
	}
}

int
eal_cleanup_config(struct internal_config *internal_cfg)
{
	if (internal_cfg->hugefile_prefix != NULL)
		free(internal_cfg->hugefile_prefix);
	if (internal_cfg->hugepage_dir != NULL)
		free(internal_cfg->hugepage_dir);
	if (internal_cfg->user_mbuf_pool_ops_name != NULL)
		free(internal_cfg->user_mbuf_pool_ops_name);

	return 0;
}

int
eal_adjust_config(struct internal_config *internal_cfg)
{
	int i;
	struct rte_config *cfg = rte_eal_get_configuration();
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (!core_parsed)
		eal_auto_detect_cores(cfg);

	if (internal_conf->process_type == RTE_PROC_AUTO)
		internal_conf->process_type = eal_proc_type_detect();

	/* default main lcore is the first one */
	if (!main_lcore_parsed) {
		cfg->main_lcore = rte_get_next_lcore(-1, 0, 0);
		if (cfg->main_lcore >= RTE_MAX_LCORE)
			return -1;
		lcore_config[cfg->main_lcore].core_role = ROLE_RTE;
	}

	compute_ctrl_threads_cpuset(internal_cfg);

	/* if no memory amounts were requested, this will result in 0 and
	 * will be overridden later, right after eal_hugepage_info_init() */
	for (i = 0; i < RTE_MAX_NUMA_NODES; i++)
		internal_cfg->memory += internal_cfg->socket_mem[i];

	return 0;
}

int
eal_check_common_options(struct internal_config *internal_cfg)
{
	struct rte_config *cfg = rte_eal_get_configuration();
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (cfg->lcore_role[cfg->main_lcore] != ROLE_RTE) {
		RTE_LOG(ERR, EAL, "Main lcore is not enabled for DPDK\n");
		return -1;
	}

	if (internal_cfg->process_type == RTE_PROC_INVALID) {
		RTE_LOG(ERR, EAL, "Invalid process type specified\n");
		return -1;
	}
	if (internal_cfg->hugefile_prefix != NULL &&
			strlen(internal_cfg->hugefile_prefix) < 1) {
		RTE_LOG(ERR, EAL, "Invalid length of --" OPT_FILE_PREFIX " option\n");
		return -1;
	}
	if (internal_cfg->hugepage_dir != NULL &&
			strlen(internal_cfg->hugepage_dir) < 1) {
		RTE_LOG(ERR, EAL, "Invalid length of --" OPT_HUGE_DIR" option\n");
		return -1;
	}
	if (internal_cfg->user_mbuf_pool_ops_name != NULL &&
			strlen(internal_cfg->user_mbuf_pool_ops_name) < 1) {
		RTE_LOG(ERR, EAL, "Invalid length of --" OPT_MBUF_POOL_OPS_NAME" option\n");
		return -1;
	}
	if (index(eal_get_hugefile_prefix(), '%') != NULL) {
		RTE_LOG(ERR, EAL, "Invalid char, '%%', in --"OPT_FILE_PREFIX" "
			"option\n");
		return -1;
	}
	if (mem_parsed && internal_cfg->force_sockets == 1) {
		RTE_LOG(ERR, EAL, "Options -m and --"OPT_SOCKET_MEM" cannot "
			"be specified at the same time\n");
		return -1;
	}
	if (internal_cfg->no_hugetlbfs && internal_cfg->force_sockets == 1) {
		RTE_LOG(ERR, EAL, "Option --"OPT_SOCKET_MEM" cannot "
			"be specified together with --"OPT_NO_HUGE"\n");
		return -1;
	}
	if (internal_cfg->no_hugetlbfs && internal_cfg->hugepage_unlink &&
			!internal_cfg->in_memory) {
		RTE_LOG(ERR, EAL, "Option --"OPT_HUGE_UNLINK" cannot "
			"be specified together with --"OPT_NO_HUGE"\n");
		return -1;
	}
	if (internal_conf->force_socket_limits && internal_conf->legacy_mem) {
		RTE_LOG(ERR, EAL, "Option --"OPT_SOCKET_LIMIT
			" is only supported in non-legacy memory mode\n");
	}
	if (internal_cfg->single_file_segments &&
			internal_cfg->hugepage_unlink &&
			!internal_cfg->in_memory) {
		RTE_LOG(ERR, EAL, "Option --"OPT_SINGLE_FILE_SEGMENTS" is "
			"not compatible with --"OPT_HUGE_UNLINK"\n");
		return -1;
	}
	if (internal_cfg->legacy_mem &&
			internal_cfg->in_memory) {
		RTE_LOG(ERR, EAL, "Option --"OPT_LEGACY_MEM" is not compatible "
				"with --"OPT_IN_MEMORY"\n");
		return -1;
	}
	if (internal_cfg->legacy_mem && internal_cfg->match_allocations) {
		RTE_LOG(ERR, EAL, "Option --"OPT_LEGACY_MEM" is not compatible "
				"with --"OPT_MATCH_ALLOCATIONS"\n");
		return -1;
	}
	if (internal_cfg->no_hugetlbfs && internal_cfg->match_allocations) {
		RTE_LOG(ERR, EAL, "Option --"OPT_NO_HUGE" is not compatible "
				"with --"OPT_MATCH_ALLOCATIONS"\n");
		return -1;
	}
	if (internal_cfg->legacy_mem && internal_cfg->memory == 0) {
		RTE_LOG(NOTICE, EAL, "Static memory layout is selected, "
			"amount of reserved memory can be adjusted with "
			"-m or --"OPT_SOCKET_MEM"\n");
	}

	return 0;
}

uint16_t
rte_vect_get_max_simd_bitwidth(void)
{
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();
	return internal_conf->max_simd_bitwidth.bitwidth;
}

int
rte_vect_set_max_simd_bitwidth(uint16_t bitwidth)
{
	struct internal_config *internal_conf =
		eal_get_internal_configuration();
	if (internal_conf->max_simd_bitwidth.forced) {
		RTE_LOG(NOTICE, EAL, "Cannot set max SIMD bitwidth - user runtime override enabled");
		return -EPERM;
	}

	if (bitwidth < RTE_VECT_SIMD_DISABLED || !rte_is_power_of_2(bitwidth)) {
		RTE_LOG(ERR, EAL, "Invalid bitwidth value!\n");
		return -EINVAL;
	}
	internal_conf->max_simd_bitwidth.bitwidth = bitwidth;
	return 0;
}

void
eal_common_usage(void)
{
	printf("[options]\n\n"
	       "EAL common options:\n"
	       "  -c COREMASK         Hexadecimal bitmask of cores to run on\n"
	       "  -l CORELIST         List of cores to run on\n"
	       "                      The argument format is <c1>[-c2][,c3[-c4],...]\n"
	       "                      where c1, c2, etc are core indexes between 0 and %d\n"
	       "  --"OPT_LCORES" COREMAP    Map lcore set to physical cpu set\n"
	       "                      The argument format is\n"
	       "                            '<lcores[@cpus]>[<,lcores[@cpus]>...]'\n"
	       "                      lcores and cpus list are grouped by '(' and ')'\n"
	       "                      Within the group, '-' is used for range separator,\n"
	       "                      ',' is used for single number separator.\n"
	       "                      '( )' can be omitted for single element group,\n"
	       "                      '@' can be omitted if cpus and lcores have the same value\n"
	       "  -s SERVICE COREMASK Hexadecimal bitmask of cores to be used as service cores\n"
	       "  --"OPT_MAIN_LCORE" ID     Core ID that is used as main\n"
	       "  --"OPT_MBUF_POOL_OPS_NAME" Pool ops name for mbuf to use\n"
	       "  -n CHANNELS         Number of memory channels\n"
	       "  -m MB               Memory to allocate (see also --"OPT_SOCKET_MEM")\n"
	       "  -r RANKS            Force number of memory ranks (don't detect)\n"
	       "  -b, --block         Add a device to the blocked list.\n"
	       "                      Prevent EAL from using this device. The argument\n"
	       "                      format for PCI devices is <domain:bus:devid.func>.\n"
	       "  -a, --allow         Add a device to the allow list.\n"
	       "                      Only use the specified devices. The argument format\n"
	       "                      for PCI devices is <[domain:]bus:devid.func>.\n"
	       "                      This option can be present several times.\n"
	       "                      [NOTE: " OPT_DEV_ALLOW " cannot be used with "OPT_DEV_BLOCK" option]\n"
	       "  --"OPT_VDEV"              Add a virtual device.\n"
	       "                      The argument format is <driver><id>[,key=val,...]\n"
	       "                      (ex: --vdev=net_pcap0,iface=eth2).\n"
	       "  --"OPT_IOVA_MODE"   Set IOVA mode. 'pa' for IOVA_PA\n"
	       "                      'va' for IOVA_VA\n"
	       "  -d LIB.so|DIR       Add a driver or driver directory\n"
	       "                      (can be used multiple times)\n"
	       "  --"OPT_VMWARE_TSC_MAP"    Use VMware TSC map instead of native RDTSC\n"
	       "  --"OPT_PROC_TYPE"         Type of this process (primary|secondary|auto)\n"
#ifndef RTE_EXEC_ENV_WINDOWS
	       "  --"OPT_SYSLOG"            Set syslog facility\n"
#endif
	       "  --"OPT_LOG_LEVEL"=<int>   Set global log level\n"
	       "  --"OPT_LOG_LEVEL"=<type-match>:<int>\n"
	       "                      Set specific log level\n"
#ifndef RTE_EXEC_ENV_WINDOWS
	       "  --"OPT_TRACE"=<regex-match>\n"
	       "                      Enable trace based on regular expression trace name.\n"
	       "                      By default, the trace is disabled.\n"
	       "		      User must specify this option to enable trace.\n"
	       "  --"OPT_TRACE_DIR"=<directory path>\n"
	       "                      Specify trace directory for trace output.\n"
	       "                      By default, trace output will created at\n"
	       "                      $HOME directory and parameter must be\n"
	       "                      specified once only.\n"
	       "  --"OPT_TRACE_BUF_SIZE"=<int>\n"
	       "                      Specify maximum size of allocated memory\n"
	       "                      for trace output for each thread. Valid\n"
	       "                      unit can be either 'B|K|M' for 'Bytes',\n"
	       "                      'KBytes' and 'MBytes' respectively.\n"
	       "                      Default is 1MB and parameter must be\n"
	       "                      specified once only.\n"
	       "  --"OPT_TRACE_MODE"=<o[verwrite] | d[iscard]>\n"
	       "                      Specify the mode of update of trace\n"
	       "                      output file. Either update on a file can\n"
	       "                      be wrapped or discarded when file size\n"
	       "                      reaches its maximum limit.\n"
	       "                      Default mode is 'overwrite' and parameter\n"
	       "                      must be specified once only.\n"
#endif /* !RTE_EXEC_ENV_WINDOWS */
	       "  -v                  Display version information on startup\n"
	       "  -h, --help          This help\n"
	       "  --"OPT_IN_MEMORY"   Operate entirely in memory. This will\n"
	       "                      disable secondary process support\n"
	       "  --"OPT_BASE_VIRTADDR"     Base virtual address\n"
	       "  --"OPT_TELEMETRY"   Enable telemetry support (on by default)\n"
	       "  --"OPT_NO_TELEMETRY"   Disable telemetry support\n"
	       "  --"OPT_FORCE_MAX_SIMD_BITWIDTH" Force the max SIMD bitwidth\n"
	       "\nEAL options for DEBUG use only:\n"
	       "  --"OPT_HUGE_UNLINK"       Unlink hugepage files after init\n"
	       "  --"OPT_NO_HUGE"           Use malloc instead of hugetlbfs\n"
	       "  --"OPT_NO_PCI"            Disable PCI\n"
	       "  --"OPT_NO_HPET"           Disable HPET\n"
	       "  --"OPT_NO_SHCONF"         No shared config (mmap'd files)\n"
	       "\n", RTE_MAX_LCORE);
}
