/*
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2017      Inria.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#define OPAL_HWLOC_WANT_SHMEM 1

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>
#include <sys/mman.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#if HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "opal/class/opal_list.h"
#include "opal/dss/dss_types.h"
#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/mca/pmix/pmix_types.h"
#include "opal/util/argv.h"
#include "opal/util/fd.h"
#include "opal/util/opal_environ.h"
#include "opal/util/path.h"

#include "orte/util/show_help.h"
#include "orte/util/error_strings.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/rmaps_types.h"

#include "orte/mca/rtc/base/base.h"
#include "rtc_hwloc.h"

static int init(void);
static void finalize(void);
static void assign(orte_job_t *jdata);
static void set(orte_job_t *jdata,
                orte_proc_t *proc,
                char ***environ_copy,
                int write_fd);

orte_rtc_base_module_t orte_rtc_hwloc_module = {
    .init = init,
    .finalize = finalize,
    .assign = assign,
    .set = set
};

#if HWLOC_API_VERSION >= 0x20000
static size_t shmemsize = 0;
static size_t shmemaddr;
static char *shmemfile = NULL;
static int shmemfd = -1;

static int parse_map_line(const char *line,
                          unsigned long *beginp,
                          unsigned long *endp,
                          orte_rtc_hwloc_vm_map_kind_t *kindp);
static int use_hole(unsigned long holebegin,
                    unsigned long holesize,
                    unsigned long *addrp,
                    unsigned long size);
static int find_hole(orte_rtc_hwloc_vm_hole_kind_t hkind,
                     size_t *addrp,
                     size_t size);
static int enough_space(const char *filename,
                        size_t space_req,
                        uint64_t *space_avail,
                        bool *result);
#endif

static int init(void)
{
#if HWLOC_API_VERSION >= 0x20000
    int rc;
    bool space_available = false;
    uint64_t amount_space_avail = 0;

    /* ensure we have the topology */
    if (OPAL_SUCCESS != (rc = opal_hwloc_base_get_topology())) {
        return rc;
    }

    if (VM_HOLE_NONE == mca_rtc_hwloc_component.kind) {
        return ORTE_SUCCESS;
    }

    /* get the size of the topology shared memory segment */
    if (0 != hwloc_shmem_topology_get_length(opal_hwloc_topology, &shmemsize, 0)) {
        opal_output_verbose(2, orte_rtc_base_framework.framework_output,
                            "%s hwloc topology shmem not available",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_SUCCESS;
    }

    if (ORTE_SUCCESS != (rc = find_hole(mca_rtc_hwloc_component.kind,
                                        &shmemaddr, shmemsize))) {
        /* we couldn't find a hole, so don't use the shmem support */
        if (4 < opal_output_get_verbosity(orte_rtc_base_framework.framework_output)) {
            FILE *file = fopen("/proc/self/maps", "r");
            if (file) {
                char line[256];
                opal_output(0, "%s Dumping /proc/self/maps",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                while (fgets(line, sizeof(line), file) != NULL) {
                    char *end = strchr(line, '\n');
                    if (end) {
                       *end = '\0';
                    }
                    opal_output(0, "%s", line);
                }
                fclose(file);
            }
        }
        return ORTE_SUCCESS;
    }
    /* create the shmem file in our session dir so it
     * will automatically get cleaned up */
    asprintf(&shmemfile, "%s/hwloc.sm", orte_process_info.jobfam_session_dir);
    /* let's make sure we have enough space for the backing file */
    if (OPAL_SUCCESS != (rc = enough_space(shmemfile, shmemsize,
                                           &amount_space_avail,
                                           &space_available))) {
        opal_output_verbose(2, orte_rtc_base_framework.framework_output,
                            "%s an error occurred while determining "
                            "whether or not %s could be created for topo shmem.",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), shmemfile);
        free(shmemfile);
        shmemfile = NULL;
        return ORTE_SUCCESS;
    }
    if (!space_available) {
        if (1 < opal_output_get_verbosity(orte_rtc_base_framework.framework_output)) {
            orte_show_help("help-orte-rtc-hwloc.txt", "target full", true,
                           shmemfile, orte_process_info.nodename,
                           (unsigned long)shmemsize,
                           (unsigned long long)amount_space_avail);
        }
        free(shmemfile);
        shmemfile = NULL;
        return ORTE_SUCCESS;
    }
    /* enough space is available, so create the segment */
    if (-1 == (shmemfd = open(shmemfile, O_CREAT | O_RDWR, 0600))) {
        int err = errno;
        if (1 < opal_output_get_verbosity(orte_rtc_base_framework.framework_output)) {
            orte_show_help("help-orte-rtc-hwloc.txt", "sys call fail", true,
                           orte_process_info.nodename,
                           "open(2)", "", strerror(err), err);
        }
        free(shmemfile);
        shmemfile = NULL;
        return ORTE_SUCCESS;
    }
    /* ensure nobody inherits this fd */
    opal_fd_set_cloexec(shmemfd);
    /* populate the shmem segment with the topology */
    if (0 != (rc = hwloc_shmem_topology_write(opal_hwloc_topology, shmemfd, 0,
                                              (void*)shmemaddr, shmemsize, 0))) {
        opal_output_verbose(2, orte_rtc_base_framework.framework_output,
                            "%s an error occurred while writing topology to %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), shmemfile);
        unlink(shmemfile);
        free(shmemfile);
        shmemfile = NULL;
        close(shmemfd);
        shmemfd = -1;
        return ORTE_SUCCESS;
    }
#endif

    return ORTE_SUCCESS;
}

static void finalize(void)
{
#if HWLOC_API_VERSION >= 0x20000
    if (NULL != shmemfile) {
        unlink(shmemfile);
        free(shmemfile);
    }
    if (0 <= shmemfd) {
        close(shmemfd);
    }
#endif
    return;
}

static void assign(orte_job_t *jdata)
{
#if HWLOC_API_VERSION >= 0x20000
    opal_list_t *cache;
    opal_value_t *kv;

    if (VM_HOLE_NONE == mca_rtc_hwloc_component.kind ||
        NULL == shmemfile) {
        return;
    }
    /* add the shmem address and size to the job-level info that
     * will be provided to the proc upon registration */
    cache = NULL;
    if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_INFO_CACHE, (void**)&cache, OPAL_PTR) ||
        NULL == cache) {
        cache = OBJ_NEW(opal_list_t);
        orte_set_attribute(&jdata->attributes, ORTE_JOB_INFO_CACHE, ORTE_ATTR_LOCAL, cache, OPAL_PTR);
    }
    opal_output_verbose(2, orte_rtc_base_framework.framework_output,
                        "FILE %s ADDR %lx SIZE %lx", shmemfile,
                        (unsigned long)shmemaddr,
                        (unsigned long)shmemsize);

    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_HWLOC_SHMEM_FILE);
    kv->type = OPAL_STRING;
    kv->data.string = strdup(shmemfile);
    opal_list_append(cache, &kv->super);

    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_HWLOC_SHMEM_ADDR);
    kv->type = OPAL_SIZE;
    kv->data.size = shmemaddr;
    opal_list_append(cache, &kv->super);

    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_HWLOC_SHMEM_SIZE);
    kv->type = OPAL_SIZE;
    kv->data.size = shmemsize;
    opal_list_append(cache, &kv->super);
#endif
}

static void set(orte_job_t *jobdat,
                orte_proc_t *child,
                char ***environ_copy,
                int write_fd)
{
    hwloc_cpuset_t cpuset;
    hwloc_obj_t root;
    opal_hwloc_topo_data_t *sum;
    orte_app_context_t *context;
    int rc=ORTE_ERROR;
    char *msg, *param;
    char *cpu_bitmap;

    opal_output_verbose(2, orte_rtc_base_framework.framework_output,
                        "%s hwloc:set on child %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (NULL == child) ? "NULL" : ORTE_NAME_PRINT(&child->name));

    if (NULL == jobdat || NULL == child) {
        /* nothing for us to do */
        opal_output_verbose(2, orte_rtc_base_framework.framework_output,
                            "%s hwloc:set jobdat %s child %s - nothing to do",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            (NULL == jobdat) ? "NULL" : ORTE_JOBID_PRINT(jobdat->jobid),
                            (NULL == child) ? "NULL" : ORTE_NAME_PRINT(&child->name));
        return;
    }

    context = (orte_app_context_t*)opal_pointer_array_get_item(jobdat->apps, child->app_idx);

    /* Set process affinity, if given */
    cpu_bitmap = NULL;
    if (!orte_get_attribute(&child->attributes, ORTE_PROC_CPU_BITMAP, (void**)&cpu_bitmap, OPAL_STRING) ||
        NULL == cpu_bitmap || 0 == strlen(cpu_bitmap)) {
        /* if the daemon is bound, then we need to "free" this proc */
        if (NULL != orte_daemon_cores) {
            root = hwloc_get_root_obj(opal_hwloc_topology);
            if (NULL == root->userdata) {
                orte_rtc_base_send_warn_show_help(write_fd,
                                                  "help-orte-odls-default.txt", "incorrectly bound",
                                                  orte_process_info.nodename, context->app,
                                                  __FILE__, __LINE__);
            }
            sum = (opal_hwloc_topo_data_t*)root->userdata;
            /* bind this proc to all available processors */
            rc = hwloc_set_cpubind(opal_hwloc_topology, sum->available, 0);
            /* if we got an error and this wasn't a default binding policy, then report it */
            if (rc < 0  && OPAL_BINDING_POLICY_IS_SET(jobdat->map->binding)) {
                if (errno == ENOSYS) {
                    msg = "hwloc indicates cpu binding not supported";
                } else if (errno == EXDEV) {
                    msg = "hwloc indicates cpu binding cannot be enforced";
                } else {
                    char *tmp;
                    (void)hwloc_bitmap_list_asprintf(&tmp, sum->available);
                    asprintf(&msg, "hwloc_set_cpubind returned \"%s\" for bitmap \"%s\"",
                             opal_strerror(rc), tmp);
                    free(tmp);
                }
                if (OPAL_BINDING_REQUIRED(jobdat->map->binding)) {
                    /* If binding is required, send an error up the pipe (which exits
                       -- it doesn't return). */
                    orte_rtc_base_send_error_show_help(write_fd, 1, "help-orte-odls-default.txt",
                                                       "binding generic error",
                                                       orte_process_info.nodename, context->app, msg,
                                                       __FILE__, __LINE__);
                } else {
                    orte_rtc_base_send_warn_show_help(write_fd,
                                                      "help-orte-odls-default.txt", "not bound",
                                                      orte_process_info.nodename, context->app, msg,
                                                      __FILE__, __LINE__);
                    return;
                }
            }
        }
        if (0 == rc && opal_hwloc_report_bindings) {
            opal_output(0, "MCW rank %d is not bound (or bound to all available processors)", child->name.vpid);
            /* avoid reporting it twice */
            (void) mca_base_var_env_name ("hwloc_base_report_bindings", &param);
            opal_unsetenv(param, environ_copy);
            free(param);
        }
    } else {
        /* convert the list to a cpuset */
        cpuset = hwloc_bitmap_alloc();
        if (0 != (rc = hwloc_bitmap_list_sscanf(cpuset, cpu_bitmap))) {
            /* See comment above about "This may be a small memory leak" */
            asprintf(&msg, "hwloc_bitmap_sscanf returned \"%s\" for the string \"%s\"",
                     opal_strerror(rc), cpu_bitmap);
            if (NULL == msg) {
                msg = "failed to convert bitmap list to hwloc bitmap";
            }
            if (OPAL_BINDING_REQUIRED(jobdat->map->binding) &&
                OPAL_BINDING_POLICY_IS_SET(jobdat->map->binding)) {
                /* If binding is required and a binding directive was explicitly
                 * given (i.e., we are not binding due to a default policy),
                 * send an error up the pipe (which exits -- it doesn't return).
                 */
                orte_rtc_base_send_error_show_help(write_fd, 1, "help-orte-odls-default.txt",
                                                   "binding generic error",
                                                   orte_process_info.nodename,
                                                   context->app, msg,
                                                   __FILE__, __LINE__);
            } else {
                orte_rtc_base_send_warn_show_help(write_fd,
                                                  "help-orte-odls-default.txt", "not bound",
                                                  orte_process_info.nodename, context->app, msg,
                                                  __FILE__, __LINE__);
                free(cpu_bitmap);
                return;
            }
        }
        /* bind as specified */
        rc = hwloc_set_cpubind(opal_hwloc_topology, cpuset, 0);
        /* if we got an error and this wasn't a default binding policy, then report it */
        if (rc < 0  && OPAL_BINDING_POLICY_IS_SET(jobdat->map->binding)) {
            char *tmp = NULL;
            if (errno == ENOSYS) {
                msg = "hwloc indicates cpu binding not supported";
            } else if (errno == EXDEV) {
                msg = "hwloc indicates cpu binding cannot be enforced";
            } else {
                asprintf(&msg, "hwloc_set_cpubind returned \"%s\" for bitmap \"%s\"",
                         opal_strerror(rc), cpu_bitmap);
            }
            if (OPAL_BINDING_REQUIRED(jobdat->map->binding)) {
                /* If binding is required, send an error up the pipe (which exits
                   -- it doesn't return). */
                orte_rtc_base_send_error_show_help(write_fd, 1, "help-orte-odls-default.txt",
                                                   "binding generic error",
                                                   orte_process_info.nodename, context->app, msg,
                                                   __FILE__, __LINE__);
            } else {
                orte_rtc_base_send_warn_show_help(write_fd,
                                                  "help-orte-odls-default.txt", "not bound",
                                                  orte_process_info.nodename, context->app, msg,
                                                  __FILE__, __LINE__);
                if (NULL != tmp) {
                    free(tmp);
                    free(msg);
                }
                return;
            }
            if (NULL != tmp) {
                free(tmp);
                free(msg);
            }
        }
        if (0 == rc && opal_hwloc_report_bindings) {
            char tmp1[1024], tmp2[1024];
            hwloc_cpuset_t mycpus;
            /* get the cpus we are bound to */
            mycpus = hwloc_bitmap_alloc();
            if (hwloc_get_cpubind(opal_hwloc_topology,
                                  mycpus,
                                  HWLOC_CPUBIND_PROCESS) < 0) {
                opal_output(0, "MCW rank %d is not bound",
                            child->name.vpid);
            } else {
                if (OPAL_ERR_NOT_BOUND == opal_hwloc_base_cset2str(tmp1, sizeof(tmp1), opal_hwloc_topology, mycpus)) {
                    opal_output(0, "MCW rank %d is not bound (or bound to all available processors)", child->name.vpid);
                } else {
                    opal_hwloc_base_cset2mapstr(tmp2, sizeof(tmp2), opal_hwloc_topology, mycpus);
                    opal_output(0, "MCW rank %d bound to %s: %s",
                                child->name.vpid, tmp1, tmp2);
                }
            }
            hwloc_bitmap_free(mycpus);
            /* avoid reporting it twice */
            (void) mca_base_var_env_name ("hwloc_base_report_bindings", &param);
            opal_unsetenv(param, environ_copy);
            free(param);
        }
        /* set memory affinity policy - if we get an error, don't report
         * anything unless the user actually specified the binding policy
         */
        rc = opal_hwloc_base_set_process_membind_policy();
        if (ORTE_SUCCESS != rc  && OPAL_BINDING_POLICY_IS_SET(jobdat->map->binding)) {
            if (errno == ENOSYS) {
                msg = "hwloc indicates memory binding not supported";
            } else if (errno == EXDEV) {
                msg = "hwloc indicates memory binding cannot be enforced";
            } else {
                msg = "failed to bind memory";
            }
            if (OPAL_HWLOC_BASE_MBFA_ERROR == opal_hwloc_base_mbfa) {
                /* If binding is required, send an error up the pipe (which exits
                   -- it doesn't return). */
                orte_rtc_base_send_error_show_help(write_fd, 1, "help-orte-odls-default.txt",
                                                   "memory binding error",
                                                   orte_process_info.nodename, context->app, msg,
                                                   __FILE__, __LINE__);
            } else {
                orte_rtc_base_send_warn_show_help(write_fd,
                                                  "help-orte-odls-default.txt", "memory not bound",
                                                  orte_process_info.nodename, context->app, msg,
                                                  __FILE__, __LINE__);
                free(cpu_bitmap);
                return;
            }
        }
    }
    if (NULL != cpu_bitmap) {
        free(cpu_bitmap);
    }
}

#if HWLOC_API_VERSION >= 0x20000

static int parse_map_line(const char *line,
                          unsigned long *beginp,
                          unsigned long *endp,
                          orte_rtc_hwloc_vm_map_kind_t *kindp)
{
    const char *tmp = line, *next;
    unsigned long value;

    /* "beginaddr-endaddr " */
    value = strtoull(tmp, (char **) &next, 16);
    if (next == tmp) {
        return ORTE_ERROR;
    }

    *beginp = (unsigned long) value;

    if (*next != '-') {
        return ORTE_ERROR;
    }

     tmp = next + 1;

    value = strtoull(tmp, (char **) &next, 16);
    if (next == tmp) {
        return ORTE_ERROR;
    }
    *endp = (unsigned long) value;
    tmp = next;

    if (*next != ' ') {
        return ORTE_ERROR;
    }
    tmp = next + 1;

    /* look for ending absolute path */
    next = strchr(tmp, '/');
    if (next) {
        *kindp = VM_MAP_FILE;
    } else {
        /* look for ending special tag [foo] */
        next = strchr(tmp, '[');
        if (next) {
            if (!strncmp(next, "[heap]", 6)) {
                *kindp = VM_MAP_HEAP;
            } else if (!strncmp(next, "[stack]", 7)) {
                *kindp = VM_MAP_STACK;
            } else {
                char *end;
                if ((end = strchr(next, '\n')) != NULL) {
                    *end = '\0';
                }
                opal_output_verbose(80, orte_rtc_base_framework.framework_output,
                                    "Found special VMA \"%s\" before stack", next);
                *kindp = VM_MAP_OTHER;
            }
        } else {
            *kindp = VM_MAP_ANONYMOUS;
        }
    }

    return ORTE_SUCCESS;
}

#define ALIGN2MB (2*1024*1024UL)

static int use_hole(unsigned long holebegin,
                    unsigned long holesize,
                    unsigned long *addrp,
                    unsigned long size)
{
    unsigned long aligned;
    unsigned long middle = holebegin+holesize/2;

    opal_output_verbose(80, orte_rtc_base_framework.framework_output,
                        "looking in hole [0x%lx-0x%lx] size %lu (%lu MB) for %lu (%lu MB)\n",
                        holebegin, holebegin+holesize, holesize, holesize>>20, size, size>>20);

    if (holesize < size) {
        return ORTE_ERROR;
    }

    /* try to align the middle of the hole on 64MB for POWER's 64k-page PMD */
    #define ALIGN64MB (64*1024*1024UL)
    aligned = (middle + ALIGN64MB) & ~(ALIGN64MB-1);
    if (aligned + size <= holebegin + holesize) {
        opal_output_verbose(80, orte_rtc_base_framework.framework_output,
                            "aligned [0x%lx-0x%lx] (middle 0x%lx) to 0x%lx for 64MB\n",
                            holebegin, holebegin+holesize, middle, aligned);
        opal_output_verbose(80, orte_rtc_base_framework.framework_output,
                            " there are %lu MB free before and %lu MB free after\n",
                            (aligned-holebegin)>>20, (holebegin+holesize-aligned-size)>>20);

        *addrp = aligned;
        return ORTE_SUCCESS;
    }

    /* try to align the middle of the hole on 2MB for x86 PMD */
    aligned = (middle + ALIGN2MB) & ~(ALIGN2MB-1);
    if (aligned + size <= holebegin + holesize) {
        opal_output_verbose(80, orte_rtc_base_framework.framework_output,
                            "aligned [0x%lx-0x%lx] (middle 0x%lx) to 0x%lx for 2MB\n",
                            holebegin, holebegin+holesize, middle, aligned);
        opal_output_verbose(80, orte_rtc_base_framework.framework_output,
                            " there are %lu MB free before and %lu MB free after\n",
                            (aligned-holebegin)>>20, (holebegin+holesize-aligned-size)>>20);
        *addrp = aligned;
        return ORTE_SUCCESS;
    }

    /* just use the end of the hole */
    *addrp = holebegin + holesize - size;
    opal_output_verbose(80, orte_rtc_base_framework.framework_output,
                        "using the end of hole starting at 0x%lx\n", *addrp);
    opal_output_verbose(80, orte_rtc_base_framework.framework_output,
                        " there are %lu MB free before\n", (*addrp-holebegin)>>20);
    return ORTE_SUCCESS;
}

static int find_hole(orte_rtc_hwloc_vm_hole_kind_t hkind,
                     size_t *addrp, size_t size)
{
    unsigned long biggestbegin = 0;
    unsigned long biggestsize = 0;
    unsigned long prevend = 0;
    orte_rtc_hwloc_vm_map_kind_t prevmkind = VM_MAP_OTHER;
    int in_libs = 0;
    FILE *file;
    char line[96];

    file = fopen("/proc/self/maps", "r");
    if (!file) {
        return ORTE_ERROR;
    }

    while (fgets(line, sizeof(line), file) != NULL) {
        unsigned long begin=0, end=0;
        orte_rtc_hwloc_vm_map_kind_t mkind=VM_MAP_OTHER;

        if (!parse_map_line(line, &begin, &end, &mkind)) {
            opal_output_verbose(90, orte_rtc_base_framework.framework_output,
                                "found %s from 0x%lx to 0x%lx\n",
                                mkind == VM_MAP_HEAP ? "HEAP" :
                                mkind == VM_MAP_STACK ? "STACK" :
                                mkind == VM_MAP_OTHER ? "OTHER" :
                                mkind == VM_MAP_FILE ? "FILE" :
                                mkind == VM_MAP_ANONYMOUS ? "ANON" : "unknown",
                                begin, end);

            switch (hkind) {
                case VM_HOLE_BEGIN:
                    fclose(file);
                    return use_hole(0, begin, addrp, size);

                case VM_HOLE_AFTER_HEAP:
                    if (prevmkind == VM_MAP_HEAP && mkind != VM_MAP_HEAP) {
                        /* only use HEAP when there's no other HEAP after it
                         * (there can be several of them consecutively).
                         */
                        fclose(file);
                        return use_hole(prevend, begin-prevend, addrp, size);
                    }
                    break;

                case VM_HOLE_BEFORE_STACK:
                    if (mkind == VM_MAP_STACK) {
                        fclose(file);
                        return use_hole(prevend, begin-prevend, addrp, size);
                    }
                    break;

                case VM_HOLE_IN_LIBS:
                    /* see if we are between heap and stack */
                    if (prevmkind == VM_MAP_HEAP) {
                        in_libs = 1;
                    }
                    if (mkind == VM_MAP_STACK) {
                        in_libs = 0;
                    }
                    if (!in_libs) {
                        /* we're not in libs, ignore this entry */
                        break;
                    }
                    /* we're in libs, consider this entry for searching the biggest hole below */
                    /* fallthrough */

                case VM_HOLE_BIGGEST:
                    if (begin-prevend > biggestsize) {
                        opal_output_verbose(90, orte_rtc_base_framework.framework_output,
                                            "new biggest 0x%lx - 0x%lx = %lu (%lu MB)\n",
                                            prevend, begin, begin-prevend, (begin-prevend)>>20);
                        biggestbegin = prevend;
                        biggestsize = begin-prevend;
                    }
                    break;

                    default:
                        assert(0);
            }
        }

        while (!strchr(line, '\n')) {
            if (!fgets(line, sizeof(line), file)) {
                goto done;
            }
        }

        if (mkind == VM_MAP_STACK) {
          /* Don't go beyond the stack. Other VMAs are special (vsyscall, vvar, vdso, etc),
           * There's no spare room there. And vsyscall is even above the userspace limit.
           */
          break;
        }

        prevend = end;
        prevmkind = mkind;

    }

  done:
    fclose(file);
    if (hkind == VM_HOLE_IN_LIBS || hkind == VM_HOLE_BIGGEST) {
        return use_hole(biggestbegin, biggestsize, addrp, size);
    }

    return ORTE_ERROR;
}

static int enough_space(const char *filename,
                        size_t space_req,
                        uint64_t *space_avail,
                        bool *result)
{
    uint64_t avail = 0;
    size_t fluff = (size_t)(.05 * space_req);
    bool enough = false;
    char *last_sep = NULL;
    /* the target file name is passed here, but we need to check the parent
     * directory. store it so we can extract that info later. */
    char *target_dir = strdup(filename);
    int rc;

    if (NULL == target_dir) {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto out;
    }
    /* get the parent directory */
    last_sep = strrchr(target_dir, OPAL_PATH_SEP[0]);
    *last_sep = '\0';
    /* now check space availability */
    if (OPAL_SUCCESS != (rc = opal_path_df(target_dir, &avail))) {
        OPAL_OUTPUT_VERBOSE(
            (70, orte_rtc_base_framework.framework_output,
             "WARNING: opal_path_df failure!")
        );
        goto out;
    }
    /* do we have enough space? */
    if (avail >= space_req + fluff) {
        enough = true;
    }
    else {
        OPAL_OUTPUT_VERBOSE(
            (70, orte_rtc_base_framework.framework_output,
             "WARNING: not enough space on %s to meet request!"
             "available: %"PRIu64 "requested: %lu", target_dir,
             avail, (unsigned long)space_req + fluff)
        );
    }

out:
    if (NULL != target_dir) {
        free(target_dir);
    }
    *result = enough;
    *space_avail = avail;
    return rc;
}
#endif
