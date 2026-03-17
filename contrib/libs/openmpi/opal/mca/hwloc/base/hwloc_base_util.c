/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2012-2017 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (C) 2018      Mellanox Technologies, Ltd. 
 *                         All rights reserved.
 * Copyright (c) 2018      Amazon.com, Inc. or its affiliates.  All Rights reserved.
 * Copyright (c) 2019 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#define OPAL_HWLOC_WANT_SHMEM 1

#include "opal_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_ENDIAN_H
#include <endian.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#if HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "opal/runtime/opal.h"
#include "opal/constants.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/os_dirpath.h"
#include "opal/util/show_help.h"
#include "opal/threads/tsd.h"
#include "opal/mca/pmix/pmix.h"

#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/mca/hwloc/base/base.h"

static bool topo_in_shmem = false;

/*
 * Provide the hwloc object that corresponds to the given
 * processor id of the given type.  Remember: "processor" here [usually] means "core" --
 * except that on some platforms, hwloc won't find any cores; it'll
 * only find PUs (!).  On such platforms, then do the same calculation
 * but with PUs instead of COREs.
 */
hwloc_obj_t opal_hwloc_base_get_pu(hwloc_topology_t topo,
                                   int lid,
                                   opal_hwloc_resource_type_t rtype)
{
    hwloc_obj_type_t obj_type = HWLOC_OBJ_CORE;
    hwloc_obj_t obj;

    /* hwloc isn't able to find cores on all platforms.  Example:
       PPC64 running RHEL 5.4 (linux kernel 2.6.18) only reports NUMA
       nodes and PU's.  Fine.

       However, note that hwloc_get_obj_by_type() will return NULL in
       2 (effectively) different cases:

       - no objects of the requested type were found
       - the Nth object of the requested type was not found

       So first we have to see if we can find *any* cores by looking
       for the 0th core.  If we find it, then try to find the Nth
       core.  Otherwise, try to find the Nth PU. */
    if (opal_hwloc_use_hwthreads_as_cpus || (NULL == hwloc_get_obj_by_type(topo, HWLOC_OBJ_CORE, 0))) {
        obj_type = HWLOC_OBJ_PU;
    }

    if (OPAL_HWLOC_PHYSICAL == rtype) {
        /* find the pu - note that we can only find physical PUs
         * as cores do not have unique physical numbers (they are
         * numbered within their sockets instead). So we find the
         * specified PU, and then return the core object that contains it */
        obj = hwloc_get_pu_obj_by_os_index(topo, lid);
        OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                             "physical cpu %d %s found in cpuset %s",
                             lid, (NULL == obj) ? "not" : "is",
                             (NULL == opal_hwloc_base_cpu_list) ? "None" : opal_hwloc_base_cpu_list));
        /* we now need to shift upward to the core including this PU */
        if (NULL != obj && HWLOC_OBJ_CORE == obj_type) {
            obj = obj->parent;
        }
        return obj;
    }

    opal_output_verbose(5, opal_hwloc_base_framework.framework_output,
                        "Searching for %d LOGICAL PU", lid);

    /* Now do the actual lookup. */
    obj = hwloc_get_obj_by_type(topo, obj_type, lid);
    OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                         "logical cpu %d %s found in cpuset %s",
                         lid, (NULL == obj) ? "not" : "is",
                         (NULL == opal_hwloc_base_cpu_list) ? "None" : opal_hwloc_base_cpu_list));

    /* Found the right core (or PU). Return the object */
    return obj;
}

/* determine the node-level available cpuset based on
 * online vs allowed vs user-specified cpus
 */
int opal_hwloc_base_filter_cpus(hwloc_topology_t topo)
{
    hwloc_obj_t root, pu;
    hwloc_cpuset_t avail = NULL, pucpus, res;
    opal_hwloc_topo_data_t *sum;
    opal_hwloc_obj_data_t *data;
    char **ranges=NULL, **range=NULL;
    int idx, cpu, start, end;

    root = hwloc_get_root_obj(topo);

    if (NULL == root->userdata) {
        root->userdata = (void*)OBJ_NEW(opal_hwloc_topo_data_t);
    }
    sum = (opal_hwloc_topo_data_t*)root->userdata;

    /* should only ever enter here once, but check anyway */
    if (NULL != sum->available) {
        return OPAL_SUCCESS;
    }

    /* process any specified default cpu set against this topology */
    if (NULL == opal_hwloc_base_cpu_list) {
        /* get the root available cpuset */
        #if HWLOC_API_VERSION < 0x20000
            avail = hwloc_bitmap_alloc();
            hwloc_bitmap_and(avail, root->online_cpuset, root->allowed_cpuset);
        #else
            avail = hwloc_bitmap_dup(root->cpuset);
        #endif
        OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                             "hwloc:base: no cpus specified - using root available cpuset"));
    } else {
        OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                             "hwloc:base: filtering cpuset"));
        /* find the specified logical cpus */
        ranges = opal_argv_split(opal_hwloc_base_cpu_list, ',');
        avail = hwloc_bitmap_alloc();
        hwloc_bitmap_zero(avail);
        res = hwloc_bitmap_alloc();
        pucpus = hwloc_bitmap_alloc();
        for (idx=0; idx < opal_argv_count(ranges); idx++) {
            range = opal_argv_split(ranges[idx], '-');
            switch (opal_argv_count(range)) {
            case 1:
                /* only one cpu given - get that object */
                cpu = strtoul(range[0], NULL, 10);
                if (NULL != (pu = opal_hwloc_base_get_pu(topo, cpu, OPAL_HWLOC_LOGICAL))) {
                    #if HWLOC_API_VERSION < 0x20000
                        hwloc_bitmap_and(pucpus, pu->online_cpuset, pu->allowed_cpuset);
                    #else
                        hwloc_bitmap_free(pucpus);
                        pucpus = hwloc_bitmap_dup(pu->cpuset);
                    #endif
                    hwloc_bitmap_or(res, avail, pucpus);
                    hwloc_bitmap_copy(avail, res);
                    data = (opal_hwloc_obj_data_t*)pu->userdata;
                    if (NULL == data) {
                        pu->userdata = (void*)OBJ_NEW(opal_hwloc_obj_data_t);
                        data = (opal_hwloc_obj_data_t*)pu->userdata;
                    }
                    data->npus++;
                }
                break;
            case 2:
                /* range given */
                start = strtoul(range[0], NULL, 10);
                end = strtoul(range[1], NULL, 10);
                for (cpu=start; cpu <= end; cpu++) {
                    if (NULL != (pu = opal_hwloc_base_get_pu(topo, cpu, OPAL_HWLOC_LOGICAL))) {
                        #if HWLOC_API_VERSION < 0x20000
                            hwloc_bitmap_and(pucpus, pu->online_cpuset, pu->allowed_cpuset);
                        #else
                            hwloc_bitmap_free(pucpus);
                            pucpus = hwloc_bitmap_dup(pu->cpuset);
                        #endif
                        hwloc_bitmap_or(res, avail, pucpus);
                        hwloc_bitmap_copy(avail, res);
                        data = (opal_hwloc_obj_data_t*)pu->userdata;
                        if (NULL == data) {
                            pu->userdata = (void*)OBJ_NEW(opal_hwloc_obj_data_t);
                            data = (opal_hwloc_obj_data_t*)pu->userdata;
                        }
                        data->npus++;
                    }
                }
                break;
            default:
                break;
            }
            opal_argv_free(range);
        }
        if (NULL != ranges) {
            opal_argv_free(ranges);
        }
        hwloc_bitmap_free(res);
        hwloc_bitmap_free(pucpus);
    }

    /* cache this info */
    sum->available = avail;

    return OPAL_SUCCESS;
}

static void fill_cache_line_size(void)
{
    int i = 0, cache_level = 2;
    unsigned size;
    unsigned int cache_object = HWLOC_OBJ_L2CACHE;
    hwloc_obj_t obj;
    bool found = false;

    /* Look for the smallest L2 cache size */
    size = 4096;
    while (cache_level > 0 && !found) {
        i=0;
        while (1) {
            obj = opal_hwloc_base_get_obj_by_type(opal_hwloc_topology,
                                                  cache_object, cache_level,
                                                  i, OPAL_HWLOC_LOGICAL);
            if (NULL == obj) {
                --cache_level;
                cache_object = HWLOC_OBJ_L1CACHE;
                break;
            } else {
                if (NULL != obj->attr &&
                    obj->attr->cache.linesize > 0 &&
                    size > obj->attr->cache.linesize) {
                    size = obj->attr->cache.linesize;
                    found = true;
                }
            }
            ++i;
        }
    }

    /* If we found an L2 cache size in the hwloc data, save it in
       opal_cache_line_size.  Otherwise, we'll leave whatever default
       was set in opal_init.c */
    if (found) {
        opal_cache_line_size = (int) size;
    }
}

int opal_hwloc_base_get_topology(void)
{
    int rc;
    opal_process_name_t wildcard_rank;
    char *val = NULL;
#if HWLOC_API_VERSION >= 0x20000
    int rc2, rc3, fd;
    uint64_t addr, *aptr, size, *sptr;
    char *shmemfile;
#endif

    opal_output_verbose(2, opal_hwloc_base_framework.framework_output,
                         "hwloc:base:get_topology");

    /* see if we already have it */
    if (NULL != opal_hwloc_topology) {
        return OPAL_SUCCESS;
    }
    wildcard_rank.jobid = OPAL_PROC_MY_NAME.jobid;
    wildcard_rank.vpid = OPAL_VPID_WILDCARD;

    if (NULL != opal_pmix.get) {
#if HWLOC_API_VERSION >= 0x20000
        opal_output_verbose(2, opal_hwloc_base_framework.framework_output,
                             "hwloc:base: looking for topology in shared memory");

        /* first try to get the shmem link, if available */
        aptr = &addr;
        sptr = &size;
        OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, OPAL_PMIX_HWLOC_SHMEM_FILE,
                                       &wildcard_rank, (void**)&shmemfile, OPAL_STRING);
        OPAL_MODEX_RECV_VALUE_OPTIONAL(rc2, OPAL_PMIX_HWLOC_SHMEM_ADDR,
                                       &wildcard_rank, (void**)&aptr, OPAL_SIZE);
        OPAL_MODEX_RECV_VALUE_OPTIONAL(rc3, OPAL_PMIX_HWLOC_SHMEM_SIZE,
                                       &wildcard_rank, (void**)&sptr, OPAL_SIZE);
        if (OPAL_SUCCESS == rc && OPAL_SUCCESS == rc2 && OPAL_SUCCESS == rc3) {
            if (0 > (fd = open(shmemfile, O_RDONLY))) {
                free(shmemfile);
                OPAL_ERROR_LOG(OPAL_ERR_FILE_OPEN_FAILURE)
                return OPAL_ERR_FILE_OPEN_FAILURE;
            }
            free(shmemfile);
            if (0 != hwloc_shmem_topology_adopt(&opal_hwloc_topology, fd,
                                                0, (void*)addr, size, 0)) {
                if (4 < opal_output_get_verbosity(opal_hwloc_base_framework.framework_output)) {
                    FILE *file = fopen("/proc/self/maps", "r");
                    if (file) {
                        char line[256];
                        opal_output(0, "Dumping /proc/self/maps");

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
                /* failed to adopt from shmem, fallback to other ways to get the topology */
            } else {
                opal_output_verbose(2, opal_hwloc_base_framework.framework_output,
                                    "hwloc:base: topology in shared memory");
                topo_in_shmem = true;
                return OPAL_SUCCESS;
            }
        }
#endif
        /* if that isn't available, then try to retrieve
         * the xml representation from the PMIx data store */
        opal_output_verbose(1, opal_hwloc_base_framework.framework_output,
                            "hwloc:base[%s:%d] getting topology XML string",
                            __FILE__, __LINE__);
#if HWLOC_API_VERSION >= 0x20000
        OPAL_MODEX_RECV_VALUE_IMMEDIATE(rc, OPAL_PMIX_HWLOC_XML_V2,
                                        &wildcard_rank, &val, OPAL_STRING);
#else
        OPAL_MODEX_RECV_VALUE_IMMEDIATE(rc, OPAL_PMIX_HWLOC_XML_V1,
                                        &wildcard_rank, &val, OPAL_STRING);
#endif
    } else {
        opal_output_verbose(1, opal_hwloc_base_framework.framework_output,
                            "hwloc:base PMIx not available");
        rc = OPAL_ERR_NOT_SUPPORTED;
    }

    if (OPAL_SUCCESS == rc && NULL != val) {
        opal_output_verbose(1, opal_hwloc_base_framework.framework_output,
                            "hwloc:base loading topology from XML");
        /* load the topology */
        if (0 != hwloc_topology_init(&opal_hwloc_topology)) {
            free(val);
            return OPAL_ERROR;
        }
        if (0 != hwloc_topology_set_xmlbuffer(opal_hwloc_topology, val, strlen(val)+1)) {
            free(val);
            hwloc_topology_destroy(opal_hwloc_topology);
            return OPAL_ERROR;
        }
        /* since we are loading this from an external source, we have to
         * explicitly set a flag so hwloc sets things up correctly
         */
        if (0 != opal_hwloc_base_topology_set_flags(opal_hwloc_topology,
                                                    HWLOC_TOPOLOGY_FLAG_IS_THISSYSTEM,
                                                    true)) {
            hwloc_topology_destroy(opal_hwloc_topology);
            free(val);
            return OPAL_ERROR;
        }
        /* now load the topology */
        if (0 != hwloc_topology_load(opal_hwloc_topology)) {
            hwloc_topology_destroy(opal_hwloc_topology);
            free(val);
            return OPAL_ERROR;
        }
        free(val);
        /* filter the cpus thru any default cpu set */
        if (OPAL_SUCCESS != (rc = opal_hwloc_base_filter_cpus(opal_hwloc_topology))) {
            hwloc_topology_destroy(opal_hwloc_topology);
            return rc;
        }
    } else if (NULL == opal_hwloc_base_topo_file) {
        opal_output_verbose(1, opal_hwloc_base_framework.framework_output,
                            "hwloc:base discovering topology");
        if (0 != hwloc_topology_init(&opal_hwloc_topology) ||
            0 != opal_hwloc_base_topology_set_flags(opal_hwloc_topology, 0, true) ||
            0 != hwloc_topology_load(opal_hwloc_topology)) {
            OPAL_ERROR_LOG(OPAL_ERR_NOT_SUPPORTED);
            return OPAL_ERR_NOT_SUPPORTED;
        }
        /* filter the cpus thru any default cpu set */
        if (OPAL_SUCCESS != (rc = opal_hwloc_base_filter_cpus(opal_hwloc_topology))) {
            hwloc_topology_destroy(opal_hwloc_topology);
            return rc;
        }
    } else {
        opal_output_verbose(1, opal_hwloc_base_framework.framework_output,
                            "hwloc:base loading topology from file %s",
                            opal_hwloc_base_topo_file);
        if (OPAL_SUCCESS != (rc = opal_hwloc_base_set_topology(opal_hwloc_base_topo_file))) {
            return rc;
        }
    }

    /* fill opal_cache_line_size global with the smallest L1 cache
       line size */
    fill_cache_line_size();

    /* get or update our local cpuset - it will get used multiple
     * times, so it's more efficient to keep a global copy
     */
    opal_hwloc_base_get_local_cpuset();

    return OPAL_SUCCESS;
}

int opal_hwloc_base_set_topology(char *topofile)
{
    struct hwloc_topology_support *support;

     OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                          "hwloc:base:set_topology %s", topofile));

   if (NULL != opal_hwloc_topology) {
        hwloc_topology_destroy(opal_hwloc_topology);
    }
    if (0 != hwloc_topology_init(&opal_hwloc_topology)) {
        return OPAL_ERR_NOT_SUPPORTED;
    }
    if (0 != hwloc_topology_set_xml(opal_hwloc_topology, topofile)) {
        hwloc_topology_destroy(opal_hwloc_topology);
        OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                             "hwloc:base:set_topology bad topo file"));
        return OPAL_ERR_NOT_SUPPORTED;
    }
    /* since we are loading this from an external source, we have to
     * explicitly set a flag so hwloc sets things up correctly
     */
    if (0 != opal_hwloc_base_topology_set_flags(opal_hwloc_topology,
                                                HWLOC_TOPOLOGY_FLAG_IS_THISSYSTEM,
                                                true)) {
        hwloc_topology_destroy(opal_hwloc_topology);
        return OPAL_ERR_NOT_SUPPORTED;
    }
    if (0 != hwloc_topology_load(opal_hwloc_topology)) {
        hwloc_topology_destroy(opal_hwloc_topology);
        OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                             "hwloc:base:set_topology failed to load"));
        return OPAL_ERR_NOT_SUPPORTED;
    }

    /* unfortunately, hwloc does not include support info in its
     * xml output :-(( We default to assuming it is present as
     * systems that use this option are likely to provide
     * binding support
     */
    support = (struct hwloc_topology_support*)hwloc_topology_get_support(opal_hwloc_topology);
    support->cpubind->set_thisproc_cpubind = true;
    support->membind->set_thisproc_membind = true;

    /* fill opal_cache_line_size global with the smallest L1 cache
       line size */
    fill_cache_line_size();

    /* all done */
    return OPAL_SUCCESS;
}

static void free_object(hwloc_obj_t obj)
{
    opal_hwloc_obj_data_t *data;
    unsigned k;

    /* free any data hanging on this object */
    if (NULL != obj->userdata) {
        data = (opal_hwloc_obj_data_t*)obj->userdata;
        OBJ_RELEASE(data);
        obj->userdata = NULL;
    }

    /* loop thru our children */
    for (k=0; k < obj->arity; k++) {
        free_object(obj->children[k]);
    }
}

void opal_hwloc_base_free_topology(hwloc_topology_t topo)
{
    hwloc_obj_t obj;
    opal_hwloc_topo_data_t *rdata;
    unsigned k;

    if (!topo_in_shmem) {
        obj = hwloc_get_root_obj(topo);
        /* release the root-level userdata */
        if (NULL != obj->userdata) {
            rdata = (opal_hwloc_topo_data_t*)obj->userdata;
            OBJ_RELEASE(rdata);
            obj->userdata = NULL;
        }
        /* now recursively descend and release userdata
         * in the rest of the objects
         */
        for (k=0; k < obj->arity; k++) {
            free_object(obj->children[k]);
        }
    }
    hwloc_topology_destroy(topo);
}

void opal_hwloc_base_get_local_cpuset(void)
{
    hwloc_obj_t root;

    if (NULL != opal_hwloc_topology) {
        if (NULL == opal_hwloc_my_cpuset) {
            opal_hwloc_my_cpuset = hwloc_bitmap_alloc();
        }

        /* get the cpus we are bound to */
        if (hwloc_get_cpubind(opal_hwloc_topology,
                              opal_hwloc_my_cpuset,
                              HWLOC_CPUBIND_PROCESS) < 0) {
            /* we are not bound - use the root's available cpuset */
            root = hwloc_get_root_obj(opal_hwloc_topology);
            hwloc_bitmap_copy(opal_hwloc_my_cpuset, root->cpuset);
        }
    }
}

int opal_hwloc_base_report_bind_failure(const char *file,
                                        int line,
                                        const char *msg, int rc)
{
    static int already_reported = 0;

    if (!already_reported &&
        OPAL_HWLOC_BASE_MBFA_SILENT != opal_hwloc_base_mbfa) {
        char hostname[OPAL_MAXHOSTNAMELEN];
        gethostname(hostname, sizeof(hostname));

        opal_show_help("help-opal-hwloc-base.txt", "mbind failure", true,
                       hostname, getpid(), file, line, msg,
                       (OPAL_HWLOC_BASE_MBFA_WARN == opal_hwloc_base_mbfa) ?
                       "Warning -- your job will continue, but possibly with degraded performance" :
                       "ERROR -- your job may abort or behave erraticly");
        already_reported = 1;
        return rc;
    }

    return OPAL_SUCCESS;
}

/* determine if there is a single cpu in a bitmap */
bool opal_hwloc_base_single_cpu(hwloc_cpuset_t cpuset)
{
    int i;
    bool one=false;

    /* count the number of bits that are set - there is
     * one bit for each available pu. We could just
     * subtract the first and last indices, but there
     * may be "holes" in the bitmap corresponding to
     * offline or unallowed cpus - so we have to
     * search for them. Return false if we anything
     * other than one
     */
    for (i=hwloc_bitmap_first(cpuset);
         i <= hwloc_bitmap_last(cpuset);
         i++) {
        if (hwloc_bitmap_isset(cpuset, i)) {
            if (one) {
                return false;
            }
            one = true;
        }
    }

    return one;
}

/* get the number of pu's under a given hwloc object */
unsigned int opal_hwloc_base_get_npus(hwloc_topology_t topo,
                                      hwloc_obj_t obj)
{
    opal_hwloc_obj_data_t *data;
    unsigned int cnt = 0;

    data = (opal_hwloc_obj_data_t*)obj->userdata;
    if (NULL == data || !data->npus_calculated) {
        if (!opal_hwloc_use_hwthreads_as_cpus) {
            /* if we are treating cores as cpus, then we really
             * want to know how many cores are in this object.
             * hwloc sets a bit for each "pu", so we can't just
             * count bits in this case as there may be more than
             * one hwthread/core. Instead, find the number of cores
             * in the system
             */
            cnt = hwloc_get_nbobjs_inside_cpuset_by_type(topo, obj->cpuset, HWLOC_OBJ_CORE);
        } else {
            hwloc_cpuset_t cpuset;

            /* if we are treating cores as cpus, or the system can't detect
             * "cores", then get the available cpuset for this object - this will
             * create and store the data
             */
            if (NULL == (cpuset = obj->cpuset)) {
                return 0;
            }
            /* count the number of bits that are set - there is
             * one bit for each available pu. We could just
             * subtract the first and last indices, but there
             * may be "holes" in the bitmap corresponding to
             * offline or unallowed cpus - so we count them with
             * the bitmap "weight" (a.k.a. population count) function
             */
            cnt = hwloc_bitmap_weight(cpuset);
        }
        /* cache the info */
        data = (opal_hwloc_obj_data_t*)obj->userdata;  // in case it was added
        if (NULL == data) {
            data = OBJ_NEW(opal_hwloc_obj_data_t);
            obj->userdata = (void*)data;
        }
        data->npus = cnt;
        data->npus_calculated = true;
    }

    return data->npus;
}

unsigned int opal_hwloc_base_get_obj_idx(hwloc_topology_t topo,
                                         hwloc_obj_t obj,
                                         opal_hwloc_resource_type_t rtype)
{
    unsigned cache_level=0;
    opal_hwloc_obj_data_t *data;
    hwloc_obj_t ptr;
    unsigned int nobjs, i;

    OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                         "hwloc:base:get_idx"));

    /* see if we already have the info */
    data = (opal_hwloc_obj_data_t*)obj->userdata;

    if (NULL == data) {
        data = OBJ_NEW(opal_hwloc_obj_data_t);
        obj->userdata = (void*)data;
    }

    if (data->idx < UINT_MAX) {
        OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                             "hwloc:base:get_idx already have data: %u",
                             data->idx));
        return data->idx;
    }

#if HWLOC_API_VERSION < 0x20000
    /* determine the number of objects of this type */
    if (HWLOC_OBJ_CACHE == obj->type) {
        cache_level = obj->attr->cache.depth;
    }
#endif

    nobjs = opal_hwloc_base_get_nbobjs_by_type(topo, obj->type, cache_level, rtype);

    OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                         "hwloc:base:get_idx found %u objects of type %s:%u",
                         nobjs, hwloc_obj_type_string(obj->type), cache_level));

    /* find this object */
    for (i=0; i < nobjs; i++) {
        ptr = opal_hwloc_base_get_obj_by_type(topo, obj->type, cache_level, i, rtype);
        if (ptr == obj) {
            data->idx = i;
            return i;
        }
    }
    /* if we get here, it wasn't found */
    opal_show_help("help-opal-hwloc-base.txt",
                   "obj-idx-failed", true,
                   hwloc_obj_type_string(obj->type), cache_level);
    return UINT_MAX;
}

/* hwloc treats cache objects as special
 * cases. Instead of having a unique type for each cache level,
 * there is a single cache object type, and the level is encoded
 * in an attribute union. So looking for cache objects involves
 * a multi-step test :-(
 */
static hwloc_obj_t df_search(hwloc_topology_t topo,
                             hwloc_obj_t start,
                             hwloc_obj_type_t target,
                             unsigned cache_level,
                             unsigned int nobj,
                             opal_hwloc_resource_type_t rtype,
                             unsigned int *num_objs)
{
    hwloc_obj_t obj;
    int search_depth;

    search_depth = hwloc_get_type_depth(topo, target);
    if (HWLOC_TYPE_DEPTH_MULTIPLE == search_depth) {
        /* either v1.x Cache, or Groups */
#if HWLOC_API_VERSION >= 0x20000
        return NULL;
#else
        if (cache_level != HWLOC_OBJ_CACHE)
            return NULL;
        search_depth = hwloc_get_cache_type_depth(topo, cache_level, (hwloc_obj_cache_type_t) -1);
#endif
    }
    if (HWLOC_TYPE_DEPTH_UNKNOWN == search_depth)
        return NULL;

    if (OPAL_HWLOC_LOGICAL == rtype) {
        if (num_objs)
            *num_objs = hwloc_get_nbobjs_by_depth(topo, search_depth);
        return hwloc_get_obj_by_depth(topo, search_depth, nobj);
    }
    if (OPAL_HWLOC_PHYSICAL == rtype) {
        /* the PHYSICAL object number is stored as the os_index. When
         * counting physical objects, we can't just count the number
         * that are in the hwloc tree as the only entries in the tree
         * are LOGICAL objects - i.e., any physical gaps won't show. So
         * we instead return the MAX os_index, as this is the best we
         * can do to tell you how many PHYSICAL objects are in the system.
         *
         * NOTE: if the last PHYSICAL object is not present (e.g., the last
         * socket on the node is empty), then the count we return will
         * be wrong!
         */
        hwloc_obj_t found = NULL;
        obj = NULL;
        if (num_objs)
            *num_objs = 0;
        while ((obj = hwloc_get_next_obj_by_depth(topo, search_depth, obj)) != NULL) {
            if (num_objs && obj->os_index > *num_objs)
                *num_objs = obj->os_index;
            if (obj->os_index == nobj)
                found = obj;
        }
        return found;
    }
    if (OPAL_HWLOC_AVAILABLE == rtype) {
        unsigned idx = 0;
        if (num_objs)
            *num_objs = hwloc_get_nbobjs_inside_cpuset_by_depth(topo, start->cpuset, search_depth);
        obj = NULL;
        while ((obj = hwloc_get_next_obj_inside_cpuset_by_depth(topo, start->cpuset, search_depth, obj)) != NULL) {
            if (idx == nobj)
                return obj;
            idx++;
        }
        return NULL;
    }
    return NULL;
}

unsigned int opal_hwloc_base_get_nbobjs_by_type(hwloc_topology_t topo,
                                                hwloc_obj_type_t target,
                                                unsigned cache_level,
                                                opal_hwloc_resource_type_t rtype)
{
    unsigned int num_objs;
    hwloc_obj_t obj;
    opal_hwloc_summary_t *sum;
    opal_hwloc_topo_data_t *data;
    int rc;

    /* bozo check */
    if (NULL == topo) {
        OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                             "hwloc:base:get_nbobjs NULL topology"));
        return 0;
    }

    /* if we want the number of LOGICAL objects, we can just
     * use the hwloc accessor to get it, unless it is a CACHE
     * as these are treated as special cases
     */
    if (OPAL_HWLOC_LOGICAL == rtype
#if HWLOC_API_VERSION < 0x20000
        && HWLOC_OBJ_CACHE != target
#endif
       ) {
        /* we should not get an error back, but just in case... */
        if (0 > (rc = hwloc_get_nbobjs_by_type(topo, target))) {
            opal_output(0, "UNKNOWN HWLOC ERROR");
            return 0;
        }
        return rc;
    }

    /* for everything else, we have to do some work */
    num_objs = 0;
    obj = hwloc_get_root_obj(topo);

    /* first see if the topology already has this summary */
    data = (opal_hwloc_topo_data_t*)obj->userdata;
    if (NULL == data) {
        data = OBJ_NEW(opal_hwloc_topo_data_t);
        obj->userdata = (void*)data;
    } else {
        OPAL_LIST_FOREACH(sum, &data->summaries, opal_hwloc_summary_t) {
            if (target == sum->type &&
                cache_level == sum->cache_level &&
                rtype == sum->rtype) {
                /* yep - return the value */
                OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                                     "hwloc:base:get_nbojbs pre-existing data %u of %s:%u",
                                     sum->num_objs, hwloc_obj_type_string(target), cache_level));
                return sum->num_objs;
            }
        }
    }

    /* don't already know it - go get it */
    df_search(topo, obj, target, cache_level, 0, rtype, &num_objs);

    /* cache the results for later */
    sum = OBJ_NEW(opal_hwloc_summary_t);
    sum->type = target;
    sum->cache_level = cache_level;
    sum->num_objs = num_objs;
    sum->rtype = rtype;
    opal_list_append(&data->summaries, &sum->super);

    OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                         "hwloc:base:get_nbojbs computed data %u of %s:%u",
                         num_objs, hwloc_obj_type_string(target), cache_level));

    return num_objs;
}

/* as above, only return the Nth instance of the specified object
 * type from inside the topology
 */
hwloc_obj_t opal_hwloc_base_get_obj_by_type(hwloc_topology_t topo,
                                            hwloc_obj_type_t target,
                                            unsigned cache_level,
                                            unsigned int instance,
                                            opal_hwloc_resource_type_t rtype)
{
    hwloc_obj_t obj;

    /* bozo check */
    if (NULL == topo) {
        return NULL;
    }

    /* if we want the nth LOGICAL object, we can just
     * use the hwloc accessor to get it, unless it is a CACHE
     * as these are treated as special cases
     */
    if (OPAL_HWLOC_LOGICAL == rtype
#if HWLOC_API_VERSION < 0x20000
        && HWLOC_OBJ_CACHE != target
#endif
       ) {
        return hwloc_get_obj_by_type(topo, target, instance);
    }

    /* for everything else, we have to do some work */
    obj = hwloc_get_root_obj(topo);
    return df_search(topo, obj, target, cache_level, instance, rtype, NULL);
}

static void df_clear(hwloc_topology_t topo,
                     hwloc_obj_t start)
{
    unsigned k;
    opal_hwloc_obj_data_t *data;

    /* see how many procs are bound to us */
    data = (opal_hwloc_obj_data_t*)start->userdata;
    if (NULL != data) {
        data->num_bound = 0;
    }

    for (k=0; k < start->arity; k++) {
        df_clear(topo, start->children[k]);
    }
}

void opal_hwloc_base_clear_usage(hwloc_topology_t topo)
{
    hwloc_obj_t root;
    unsigned k;

    /* bozo check */
    if (NULL == topo) {
        OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                             "hwloc:base:clear_usage: NULL topology"));
        return;
    }

    root = hwloc_get_root_obj(topo);
    /* must not start at root as the root object has
     * a different userdata attached to it
     */
    for (k=0; k < root->arity; k++) {
        df_clear(topo, root->children[k]);
    }
}

/* The current slot_list notation only goes to the core level - i.e., the location
 * is specified as socket:core. Thus, the code below assumes that all locations
 * are to be parsed under that notation.
 */

static int socket_to_cpu_set(char *cpus,
                             hwloc_topology_t topo,
                             opal_hwloc_resource_type_t rtype,
                             hwloc_bitmap_t cpumask)
{
    char **range;
    int range_cnt;
    int lower_range, upper_range;
    int socket_id;
    hwloc_obj_t obj;

    if ('*' == cpus[0]) {
        /* requesting cpumask for ALL sockets */
        obj = hwloc_get_root_obj(topo);
        /* set to all available processors - essentially,
         * this specification equates to unbound
         */
        hwloc_bitmap_or(cpumask, cpumask, obj->cpuset);
        return OPAL_SUCCESS;
    }

    range = opal_argv_split(cpus,'-');
    range_cnt = opal_argv_count(range);
    switch (range_cnt) {
    case 1:  /* no range was present, so just one socket given */
        socket_id = atoi(range[0]);
        obj = opal_hwloc_base_get_obj_by_type(topo, HWLOC_OBJ_SOCKET, 0, socket_id, rtype);
        /* get the available cpus for this socket */
        hwloc_bitmap_or(cpumask, cpumask, obj->cpuset);
        break;

    case 2:  /* range of sockets was given */
        lower_range = atoi(range[0]);
        upper_range = atoi(range[1]);
        /* cycle across the range of sockets */
        for (socket_id=lower_range; socket_id<=upper_range; socket_id++) {
            obj = opal_hwloc_base_get_obj_by_type(topo, HWLOC_OBJ_SOCKET, 0, socket_id, rtype);
            /* set the available cpus for this socket bits in the bitmask */
            hwloc_bitmap_or(cpumask, cpumask, obj->cpuset);
        }
        break;
    default:
        opal_argv_free(range);
        return OPAL_ERROR;
    }
    opal_argv_free(range);

    return OPAL_SUCCESS;
}

static int socket_core_to_cpu_set(char *socket_core_list,
                                  hwloc_topology_t topo,
                                  opal_hwloc_resource_type_t rtype,
                                  hwloc_bitmap_t cpumask)
{
    int rc=OPAL_SUCCESS, i, j;
    char **socket_core, *corestr;
    char **range, **list;
    int range_cnt;
    int lower_range, upper_range;
    int socket_id, core_id;
    hwloc_obj_t socket, core;
    hwloc_obj_type_t obj_type = HWLOC_OBJ_CORE;

    socket_core = opal_argv_split(socket_core_list, ':');
    socket_id = atoi(socket_core[0]);

    /* get the object for this socket id */
    if (NULL == (socket = opal_hwloc_base_get_obj_by_type(topo, HWLOC_OBJ_SOCKET, 0,
                                                          socket_id, rtype))) {
        opal_argv_free(socket_core);
        return OPAL_ERR_NOT_FOUND;
    }

    /* as described in comment near top of file, hwloc isn't able
     * to find cores on all platforms. Adjust the type here if
     * required
     */
    if (NULL == hwloc_get_obj_by_type(topo, HWLOC_OBJ_CORE, 0)) {
        obj_type = HWLOC_OBJ_PU;
    }

    for (i=1; NULL != socket_core[i]; i++) {
        if ('C' == socket_core[i][0] ||
            'c' == socket_core[i][0]) {
            corestr = &socket_core[i][1];
        } else {
            corestr = socket_core[i];
        }
        if ('*' == corestr[0]) {
            /* set to all cpus on this socket */
            hwloc_bitmap_or(cpumask, cpumask, socket->cpuset);
            /* we are done - already assigned all cores! */
            rc = OPAL_SUCCESS;
            break;
        } else {
            range = opal_argv_split(corestr, '-');
            range_cnt = opal_argv_count(range);
            /* see if a range was set or not */
            switch (range_cnt) {
            case 1:  /* only one core, or a list of cores, specified */
                list = opal_argv_split(range[0], ',');
                for (j=0; NULL != list[j]; j++) {
                    core_id = atoi(list[j]);
                    /* get that object */
                    if (NULL == (core = df_search(topo, socket, obj_type, 0,
                                                  core_id, OPAL_HWLOC_AVAILABLE,
                                                  NULL))) {
                        opal_argv_free(list);
                        opal_argv_free(range);
                        opal_argv_free(socket_core);
                        return OPAL_ERR_NOT_FOUND;
                    }
                    /* get the cpus */
                    hwloc_bitmap_or(cpumask, cpumask, core->cpuset);
                }
                opal_argv_free(list);
                break;

            case 2:  /* range of core id's was given */
                opal_output_verbose(5, opal_hwloc_base_framework.framework_output,
                                    "range of cores given: start %s stop %s",
                                    range[0], range[1]);
                lower_range = atoi(range[0]);
                upper_range = atoi(range[1]);
                for (core_id=lower_range; core_id <= upper_range; core_id++) {
                    /* get that object */
                    if (NULL == (core = df_search(topo, socket, obj_type, 0,
                                                  core_id, OPAL_HWLOC_AVAILABLE,
                                                  NULL))) {
                        opal_argv_free(range);
                        opal_argv_free(socket_core);
                        return OPAL_ERR_NOT_FOUND;
                    }
                    /* get the cpus add them into the result */
                    hwloc_bitmap_or(cpumask, cpumask, core->cpuset);
                }
                break;

            default:
                opal_argv_free(range);
                opal_argv_free(socket_core);
                return OPAL_ERROR;
            }
            opal_argv_free(range);
        }
    }
    opal_argv_free(socket_core);

    return rc;
}

int opal_hwloc_base_cpu_list_parse(const char *slot_str,
                                    hwloc_topology_t topo,
                                    opal_hwloc_resource_type_t rtype,
                                    hwloc_cpuset_t cpumask)
{
    char **item, **rngs;
    int rc, i, j, k;
    hwloc_obj_t pu;
    char **range, **list;
    size_t range_cnt;
    int core_id, lower_range, upper_range;

    /* bozo checks */
    if (NULL == opal_hwloc_topology) {
        return OPAL_ERR_NOT_SUPPORTED;
    }
    if (NULL == slot_str || 0 == strlen(slot_str)) {
        return OPAL_ERR_BAD_PARAM;
    }

    opal_output_verbose(5, opal_hwloc_base_framework.framework_output,
                        "slot assignment: slot_list == %s",
                        slot_str);

    /* split at ';' */
    item = opal_argv_split(slot_str, ';');

    /* start with a clean mask */
    hwloc_bitmap_zero(cpumask);
    /* loop across the items and accumulate the mask */
    for (i=0; NULL != item[i]; i++) {
        opal_output_verbose(5, opal_hwloc_base_framework.framework_output,
                            "working assignment %s",
                            item[i]);
        /* if they specified "socket" by starting with an S/s,
         * or if they use socket:core notation, then parse the
         * socket/core info
         */
        if ('S' == item[i][0] ||
            's' == item[i][0] ||
            NULL != strchr(item[i], ':')) {
            /* specified a socket */
            if (NULL == strchr(item[i], ':')) {
                /* binding just to the socket level, though
                 * it could specify multiple sockets
                 * Skip the S and look for a ranges
                 */
                rngs = opal_argv_split(&item[i][1], ',');
                for (j=0; NULL != rngs[j]; j++) {
                    if (OPAL_SUCCESS != (rc = socket_to_cpu_set(rngs[j], topo, rtype, cpumask))) {
                        opal_argv_free(rngs);
                        opal_argv_free(item);
                        return rc;
                    }
                }
                opal_argv_free(rngs);
            } else {
                /* binding to a socket/whatever specification */
                if ('S' == item[i][0] ||
                    's' == item[i][0]) {
                    rngs = opal_argv_split(&item[i][1], ',');
                    for (j=0; NULL != rngs[j]; j++) {
                        if (OPAL_SUCCESS != (rc = socket_core_to_cpu_set(rngs[j], topo, rtype, cpumask))) {
                            opal_argv_free(rngs);
                            opal_argv_free(item);
                            return rc;
                        }
                    }
                    opal_argv_free(rngs);
                } else {
                    rngs = opal_argv_split(item[i], ',');
                    for (j=0; NULL != rngs[j]; j++) {
                        if (OPAL_SUCCESS != (rc = socket_core_to_cpu_set(rngs[j], topo, rtype, cpumask))) {
                            opal_argv_free(rngs);
                            opal_argv_free(item);
                            return rc;
                        }
                    }
                    opal_argv_free(rngs);
                }
            }
        } else {
            rngs = opal_argv_split(item[i], ',');
            for (k=0; NULL != rngs[k]; k++) {
                /* just a core specification - see if one or a range was given */
                range = opal_argv_split(rngs[k], '-');
                range_cnt = opal_argv_count(range);
                /* see if a range was set or not */
                switch (range_cnt) {
                case 1:  /* only one core, or a list of cores, specified */
                    list = opal_argv_split(range[0], ',');
                    for (j=0; NULL != list[j]; j++) {
                        core_id = atoi(list[j]);
                        /* find the specified available cpu */
                        if (NULL == (pu = opal_hwloc_base_get_pu(topo, core_id, rtype))) {
                            opal_argv_free(range);
                            opal_argv_free(item);
                            opal_argv_free(rngs);
                            opal_argv_free(list);
                            return OPAL_ERR_SILENT;
                        }
                        /* get the cpus for that object and set them in the massk*/
                        hwloc_bitmap_or(cpumask, cpumask, pu->cpuset);
                    }
                    opal_argv_free(list);
                    break;

                case 2:  /* range of core id's was given */
                    lower_range = atoi(range[0]);
                    upper_range = atoi(range[1]);
                    for (core_id=lower_range; core_id <= upper_range; core_id++) {
                        /* find the specified logical available cpu */
                        if (NULL == (pu = opal_hwloc_base_get_pu(topo, core_id, rtype))) {
                            opal_argv_free(range);
                            opal_argv_free(item);
                            opal_argv_free(rngs);
                            return OPAL_ERR_SILENT;
                        }
                        /* get the cpus for that object and set them in the mask*/
                        hwloc_bitmap_or(cpumask, cpumask, pu->cpuset);
                    }
                    break;

                default:
                    opal_argv_free(range);
                    opal_argv_free(item);
                    opal_argv_free(rngs);
                    return OPAL_ERROR;
                }
                opal_argv_free(range);
            }
            opal_argv_free(rngs);
        }
    }
    opal_argv_free(item);
    return OPAL_SUCCESS;
}

opal_hwloc_locality_t opal_hwloc_base_get_relative_locality(hwloc_topology_t topo,
                                                            char *cpuset1, char *cpuset2)
{
    opal_hwloc_locality_t locality;
    hwloc_obj_t obj;
    unsigned depth, d, width, w;
    bool shared;
    hwloc_obj_type_t type;
    int sect1, sect2;
    hwloc_cpuset_t loc1, loc2;

    /* start with what we know - they share a node on a cluster
     * NOTE: we may alter that latter part as hwloc's ability to
     * sense multi-cu, multi-cluster systems grows
     */
    locality = OPAL_PROC_ON_NODE | OPAL_PROC_ON_HOST | OPAL_PROC_ON_CU | OPAL_PROC_ON_CLUSTER;

    /* if either cpuset is NULL, then that isn't bound */
    if (NULL == cpuset1 || NULL == cpuset2) {
        return locality;
    }

    /* get the max depth of the topology */
    depth = hwloc_topology_get_depth(topo);

    /* convert the strings to cpusets */
    loc1 = hwloc_bitmap_alloc();
    hwloc_bitmap_list_sscanf(loc1, cpuset1);
    loc2 = hwloc_bitmap_alloc();
    hwloc_bitmap_list_sscanf(loc2, cpuset2);

    /* start at the first depth below the top machine level */
    for (d=1; d < depth; d++) {
        shared = false;
        /* get the object type at this depth */
        type = hwloc_get_depth_type(topo, d);
        /* if it isn't one of interest, then ignore it */
        if (HWLOC_OBJ_NODE != type &&
            HWLOC_OBJ_SOCKET != type &&
#if HWLOC_API_VERSION < 0x20000
            HWLOC_OBJ_CACHE != type &&
#else
            HWLOC_OBJ_L3CACHE != type &&
            HWLOC_OBJ_L2CACHE != type &&
            HWLOC_OBJ_L1CACHE != type &&
#endif
            HWLOC_OBJ_CORE != type &&
            HWLOC_OBJ_PU != type) {
            continue;
        }
        /* get the width of the topology at this depth */
        width = hwloc_get_nbobjs_by_depth(topo, d);

        /* scan all objects at this depth to see if
         * our locations overlap with them
         */
        for (w=0; w < width; w++) {
            /* get the object at this depth/index */
            obj = hwloc_get_obj_by_depth(topo, d, w);
            /* see if our locations intersect with the cpuset for this obj */
            sect1 = hwloc_bitmap_intersects(obj->cpuset, loc1);
            sect2 = hwloc_bitmap_intersects(obj->cpuset, loc2);
            /* if both intersect, then we share this level */
            if (sect1 && sect2) {
                shared = true;
                switch(obj->type) {
                case HWLOC_OBJ_NODE:
                    locality |= OPAL_PROC_ON_NUMA;
                    break;
                case HWLOC_OBJ_SOCKET:
                    locality |= OPAL_PROC_ON_SOCKET;
                    break;
#if HWLOC_API_VERSION < 0x20000
                case HWLOC_OBJ_CACHE:
                    if (3 == obj->attr->cache.depth) {
                        locality |= OPAL_PROC_ON_L3CACHE;
                    } else if (2 == obj->attr->cache.depth) {
                        locality |= OPAL_PROC_ON_L2CACHE;
                    } else {
                        locality |= OPAL_PROC_ON_L1CACHE;
                    }
                    break;
#else
                case HWLOC_OBJ_L3CACHE:
                    locality |= OPAL_PROC_ON_L3CACHE;
                    break;
                case HWLOC_OBJ_L2CACHE:
                    locality |= OPAL_PROC_ON_L2CACHE;
                    break;
                case HWLOC_OBJ_L1CACHE:
                    locality |= OPAL_PROC_ON_L1CACHE;
                    break;
#endif
                case HWLOC_OBJ_CORE:
                    locality |= OPAL_PROC_ON_CORE;
                    break;
                case HWLOC_OBJ_PU:
                    locality |= OPAL_PROC_ON_HWTHREAD;
                    break;
                default:
                    /* just ignore it */
                    break;
                }
                break;
            }
            /* otherwise, we don't share this
             * object - but we still might share another object
             * on this level, so we have to keep searching
             */
        }
        /* if we spanned the entire width without finding
         * a point of intersection, then no need to go
         * deeper
         */
        if (!shared) {
            break;
        }
    }

    opal_output_verbose(5, opal_hwloc_base_framework.framework_output,
                        "locality: %s",
                        opal_hwloc_base_print_locality(locality));
    hwloc_bitmap_free(loc1);
    hwloc_bitmap_free(loc2);

    return locality;
}

/* searches the given topology for coprocessor objects and returns
 * their serial numbers as a comma-delimited string, or NULL
 * if no coprocessors are found
 */
char* opal_hwloc_base_find_coprocessors(hwloc_topology_t topo)
{
    hwloc_obj_t osdev;
    unsigned i;
    char **cps = NULL;
    char *cpstring = NULL;
    int depth;

    /* coprocessors are recorded under OS_DEVICEs, so first
     * see if we have any of those
     */
    if (HWLOC_TYPE_DEPTH_UNKNOWN == (depth = hwloc_get_type_depth(topo, HWLOC_OBJ_OS_DEVICE))) {
        OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                             "hwloc:base:find_coprocessors: NONE FOUND IN TOPO"));
        return NULL;
    }
#if HAVE_DECL_HWLOC_OBJ_OSDEV_COPROC
    /* check the device objects for coprocessors */
    osdev = hwloc_get_obj_by_depth(topo, depth, 0);
    while (NULL != osdev) {
        if (HWLOC_OBJ_OSDEV_COPROC == osdev->attr->osdev.type) {
            /* got one! find and save its serial number */
            for (i=0; i < osdev->infos_count; i++) {
                if (0 == strncmp(osdev->infos[i].name, "MICSerialNumber", strlen("MICSerialNumber"))) {
                    OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                                         "hwloc:base:find_coprocessors: coprocessor %s found",
                                         osdev->infos[i].value));
                    opal_argv_append_nosize(&cps, osdev->infos[i].value);
                }
            }
        }
        osdev = osdev->next_cousin;
    }
    if (NULL != cps) {
        cpstring = opal_argv_join(cps, ',');
        opal_argv_free(cps);
    }
    OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                         "hwloc:base:find_coprocessors: hosting coprocessors %s",
                         (NULL == cpstring) ? "NONE" : cpstring));
#else
    OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                         "hwloc:base:find_coprocessors: the version of hwloc that Open MPI was built against (v%d.%d.%d) does not support detecting coprocessors",
                         (HWLOC_API_VERSION>>16)&&0xFF, (HWLOC_API_VERSION>>8)&0xFF, HWLOC_API_VERSION&&0xFF));
#endif
    return cpstring;
}

#define OPAL_HWLOC_MAX_ELOG_LINE 1024

static char *hwloc_getline(FILE *fp)
{
    char *ret, *buff;
    char input[OPAL_HWLOC_MAX_ELOG_LINE];

    ret = fgets(input, OPAL_HWLOC_MAX_ELOG_LINE, fp);
    if (NULL != ret) {
           input[strlen(input)-1] = '\0';  /* remove newline */
           buff = strdup(input);
           return buff;
    }

    return NULL;
}

/* checks local environment to determine if this process
 * is on a coprocessor - if so, it returns the serial number
 * as a string, or NULL if it isn't on a coprocessor
 */
char* opal_hwloc_base_check_on_coprocessor(void)
{
    /* this support currently is limited to Intel Phi processors
     * but will hopefully be extended as we get better, more
     * generalized ways of identifying coprocessors
     */
    FILE *fp;
    char *t, *cptr, *e, *cp=NULL;

    if (OPAL_SUCCESS != opal_os_dirpath_access("/proc/elog", S_IRUSR)) {
        /* if the file isn't there, or we don't have permission
         * to read it, then we are not on a coprocessor so far
         * as we can tell
         */
        return NULL;
    }
    if (NULL == (fp = fopen("/proc/elog", "r"))) {
        /* nothing we can do */
        return NULL;
    }
    /* look for the line containing the serial number of this
     * card - usually the first line in the file
     */
    while (NULL != (cptr = hwloc_getline(fp))) {
        if (NULL != (t = strstr(cptr, "Card"))) {
            /* we want the string right after this - delimited by
             * a colon at the end
             */
            t += 5;  // move past "Card "
            if (NULL == (e = strchr(t, ':'))) {
                /* not what we were expecting */
                free(cptr);
                continue;
            }
            *e = '\0';
            cp = strdup(t);
            free(cptr);
            break;
        }
        free(cptr);
    }
    fclose(fp);
    OPAL_OUTPUT_VERBOSE((5, opal_hwloc_base_framework.framework_output,
                         "hwloc:base:check_coprocessor: on coprocessor %s",
                         (NULL == cp) ? "NONE" : cp));
    return cp;
}

char* opal_hwloc_base_print_binding(opal_binding_policy_t binding)
{
    char *ret, *bind;
    opal_hwloc_print_buffers_t *ptr;

    switch(OPAL_GET_BINDING_POLICY(binding)) {
    case OPAL_BIND_TO_NONE:
        bind = "NONE";
        break;
    case OPAL_BIND_TO_BOARD:
        bind = "BOARD";
        break;
    case OPAL_BIND_TO_NUMA:
        bind = "NUMA";
        break;
    case OPAL_BIND_TO_SOCKET:
        bind = "SOCKET";
        break;
    case OPAL_BIND_TO_L3CACHE:
        bind = "L3CACHE";
        break;
    case OPAL_BIND_TO_L2CACHE:
        bind = "L2CACHE";
        break;
    case OPAL_BIND_TO_L1CACHE:
        bind = "L1CACHE";
        break;
    case OPAL_BIND_TO_CORE:
        bind = "CORE";
        break;
    case OPAL_BIND_TO_HWTHREAD:
        bind = "HWTHREAD";
        break;
    case OPAL_BIND_TO_CPUSET:
        bind = "CPUSET";
        break;
    default:
        bind = "UNKNOWN";
    }
    ptr = opal_hwloc_get_print_buffer();
    if (NULL == ptr) {
        return opal_hwloc_print_null;
    }
    /* cycle around the ring */
    if (OPAL_HWLOC_PRINT_NUM_BUFS == ptr->cntr) {
        ptr->cntr = 0;
    }
    if (!OPAL_BINDING_REQUIRED(binding) &&
        OPAL_BIND_OVERLOAD_ALLOWED(binding)) {
        snprintf(ptr->buffers[ptr->cntr], OPAL_HWLOC_PRINT_MAX_SIZE,
                 "%s:IF-SUPPORTED:OVERLOAD-ALLOWED", bind);
    } else if (OPAL_BIND_OVERLOAD_ALLOWED(binding)) {
        snprintf(ptr->buffers[ptr->cntr], OPAL_HWLOC_PRINT_MAX_SIZE,
                 "%s:OVERLOAD-ALLOWED", bind);
    } else if (!OPAL_BINDING_REQUIRED(binding)) {
        snprintf(ptr->buffers[ptr->cntr], OPAL_HWLOC_PRINT_MAX_SIZE,
                 "%s:IF-SUPPORTED", bind);
    } else {
        snprintf(ptr->buffers[ptr->cntr], OPAL_HWLOC_PRINT_MAX_SIZE, "%s", bind);
    }
    ret = ptr->buffers[ptr->cntr];
    ptr->cntr++;

    return ret;
}

/*
 * Turn an int bitmap to a "a-b,c" range kind of string
 */
static char *bitmap2rangestr(int bitmap)
{
    size_t i;
    int range_start, range_end;
    bool first, isset;
    char tmp[BUFSIZ];
    const int stmp = sizeof(tmp) - 1;
    static char ret[BUFSIZ];

    memset(ret, 0, sizeof(ret));

    first = true;
    range_start = -999;
    for (i = 0; i < sizeof(int) * 8; ++i) {
        isset = (bitmap & (1 << i));

        /* Do we have a running range? */
        if (range_start >= 0) {
            if (isset) {
                continue;
            } else {
                /* A range just ended; output it */
                if (!first) {
                    strncat(ret, ",", sizeof(ret) - strlen(ret) - 1);
                } else {
                    first = false;
                }

                range_end = i - 1;
                if (range_start == range_end) {
                    snprintf(tmp, stmp, "%d", range_start);
                } else {
                    snprintf(tmp, stmp, "%d-%d", range_start, range_end);
                }
                strncat(ret, tmp, sizeof(ret) - strlen(ret) - 1);

                range_start = -999;
            }
        }

        /* No running range */
        else {
            if (isset) {
                range_start = i;
            }
        }
    }

    /* If we ended the bitmap with a range open, output it */
    if (range_start >= 0) {
        if (!first) {
            strncat(ret, ",", sizeof(ret) - strlen(ret) - 1);
            first = false;
        }

        range_end = i - 1;
        if (range_start == range_end) {
            snprintf(tmp, stmp, "%d", range_start);
        } else {
            snprintf(tmp, stmp, "%d-%d", range_start, range_end);
        }
        strncat(ret, tmp, sizeof(ret) - strlen(ret) - 1);
    }

    return ret;
}

/*
 * Make a map of socket/core/hwthread tuples
 */
static int build_map(int *num_sockets_arg, int *num_cores_arg,
                     hwloc_cpuset_t cpuset, int ***map, hwloc_topology_t topo)
{
    int num_sockets, num_cores;
    int socket_index, core_index, pu_index;
    hwloc_obj_t socket, core, pu;
    int **data;

    /* Find out how many sockets we have */
    num_sockets = hwloc_get_nbobjs_by_type(topo, HWLOC_OBJ_SOCKET);
    /* some systems (like the iMac) only have one
     * socket and so don't report a socket
     */
    if (0 == num_sockets) {
        num_sockets = 1;
    }
    /* Lazy: take the total number of cores that we have in the
       topology; that'll be more than the max number of cores
       under any given socket */
    num_cores = hwloc_get_nbobjs_by_type(topo, HWLOC_OBJ_CORE);
    *num_sockets_arg = num_sockets;
    *num_cores_arg = num_cores;

    /* Alloc a 2D array: sockets x cores. */
    data = malloc(num_sockets * sizeof(int *));
    if (NULL == data) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    data[0] = calloc(num_sockets * num_cores, sizeof(int));
    if (NULL == data[0]) {
        free(data);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    for (socket_index = 1; socket_index < num_sockets; ++socket_index) {
        data[socket_index] = data[socket_index - 1] + num_cores;
    }

    /* Iterate the PUs in this cpuset; fill in the data[][] array with
       the socket/core/pu triples */
    for (pu_index = 0,
             pu = hwloc_get_obj_inside_cpuset_by_type(topo,
                                                      cpuset, HWLOC_OBJ_PU,
                                                      pu_index);
         NULL != pu;
         pu = hwloc_get_obj_inside_cpuset_by_type(topo,
                                                  cpuset, HWLOC_OBJ_PU,
                                                  ++pu_index)) {
        /* Go upward and find the core this PU belongs to */
        core = pu;
        while (NULL != core && core->type != HWLOC_OBJ_CORE) {
            core = core->parent;
        }
        core_index = 0;
        if (NULL != core) {
            core_index = core->logical_index;
        }

        /* Go upward and find the socket this PU belongs to */
        socket = pu;
        while (NULL != socket && socket->type != HWLOC_OBJ_SOCKET) {
            socket = socket->parent;
        }
        socket_index = 0;
        if (NULL != socket) {
            socket_index = socket->logical_index;
        }

        /* Save this socket/core/pu combo.  LAZY: Assuming that we
           won't have more PU's per core than (sizeof(int)*8). */
        data[socket_index][core_index] |= (1 << pu->sibling_rank);
    }

    *map = data;
    return OPAL_SUCCESS;
}

/*
 * Make a prettyprint string for a hwloc_cpuset_t
 */
int opal_hwloc_base_cset2str(char *str, int len,
                             hwloc_topology_t topo,
                             hwloc_cpuset_t cpuset)
{
    bool first;
    int num_sockets, num_cores;
    int ret, socket_index, core_index;
    char tmp[BUFSIZ];
    const int stmp = sizeof(tmp) - 1;
    int **map=NULL;
    hwloc_obj_t root;
    opal_hwloc_topo_data_t *sum;

    str[0] = tmp[stmp] = '\0';

    /* if the cpuset is all zero, then not bound */
    if (hwloc_bitmap_iszero(cpuset)) {
        return OPAL_ERR_NOT_BOUND;
    }

    /* if the cpuset includes all available cpus, then we are unbound */
    root = hwloc_get_root_obj(topo);
    if (NULL != root->userdata) {
        sum = (opal_hwloc_topo_data_t*)root->userdata;
        if (NULL == sum->available) {
           return OPAL_ERROR;
        }
        if (0 != hwloc_bitmap_isincluded(sum->available, cpuset)) {
            return OPAL_ERR_NOT_BOUND;
        }
    }

    if (OPAL_SUCCESS != (ret = build_map(&num_sockets, &num_cores, cpuset, &map, topo))) {
        return ret;
    }
    /* Iterate over the data matrix and build up the string */
    first = true;
    for (socket_index = 0; socket_index < num_sockets; ++socket_index) {
        for (core_index = 0; core_index < num_cores; ++core_index) {
            if (map[socket_index][core_index] > 0) {
                if (!first) {
                    strncat(str, ", ", len - strlen(str) - 1);
                }
                first = false;

                snprintf(tmp, stmp, "socket %d[core %d[hwt %s]]",
                         socket_index, core_index,
                         bitmap2rangestr(map[socket_index][core_index]));
                strncat(str, tmp, len - strlen(str) - 1);
            }
        }
    }
    if (NULL != map) {
        if (NULL != map[0]) {
            free(map[0]);
        }
        free(map);
    }

    return OPAL_SUCCESS;
}

/*
 * Make a prettyprint string for a cset in a map format.
 * Example: [B./..]
 * Key:  [] - signifies socket
 *        / - divider between cores
 *        . - signifies PU a process not bound to
 *        B - signifies PU a process is bound to
 */
int opal_hwloc_base_cset2mapstr(char *str, int len,
                                hwloc_topology_t topo,
                                hwloc_cpuset_t cpuset)
{
    char tmp[BUFSIZ];
    int core_index, pu_index;
    const int stmp = sizeof(tmp) - 1;
    hwloc_obj_t socket, core, pu;
    hwloc_obj_t root;
    opal_hwloc_topo_data_t *sum;

    str[0] = tmp[stmp] = '\0';

    /* if the cpuset is all zero, then not bound */
    if (hwloc_bitmap_iszero(cpuset)) {
        return OPAL_ERR_NOT_BOUND;
    }

    /* if the cpuset includes all available cpus, then we are unbound */
    root = hwloc_get_root_obj(topo);
    if (NULL != root->userdata) {
        sum = (opal_hwloc_topo_data_t*)root->userdata;
        if (NULL == sum->available) {
           return OPAL_ERROR;
        }
        if (0 != hwloc_bitmap_isincluded(sum->available, cpuset)) {
            return OPAL_ERR_NOT_BOUND;
        }
    }

    /* Iterate over all existing sockets */
    for (socket = hwloc_get_obj_by_type(topo, HWLOC_OBJ_SOCKET, 0);
         NULL != socket;
         socket = socket->next_cousin) {
        strncat(str, "[", len - strlen(str) - 1);

        /* Iterate over all existing cores in this socket */
        core_index = 0;
        for (core = hwloc_get_obj_inside_cpuset_by_type(topo,
                                                        socket->cpuset,
                                                        HWLOC_OBJ_CORE, core_index);
             NULL != core;
             core = hwloc_get_obj_inside_cpuset_by_type(topo,
                                                        socket->cpuset,
                                                        HWLOC_OBJ_CORE, ++core_index)) {
            if (core_index > 0) {
                strncat(str, "/", len - strlen(str) - 1);
            }

            /* Iterate over all existing PUs in this core */
            pu_index = 0;
            for (pu = hwloc_get_obj_inside_cpuset_by_type(topo,
                                                          core->cpuset,
                                                          HWLOC_OBJ_PU, pu_index);
                 NULL != pu;
                 pu = hwloc_get_obj_inside_cpuset_by_type(topo,
                                                          core->cpuset,
                                                          HWLOC_OBJ_PU, ++pu_index)) {

                /* Is this PU in the cpuset? */
                if (hwloc_bitmap_isset(cpuset, pu->os_index)) {
                    strncat(str, "B", len - strlen(str) - 1);
                } else {
                    strncat(str, ".", len - strlen(str) - 1);
                }
            }
        }
        strncat(str, "]", len - strlen(str) - 1);
    }

    return OPAL_SUCCESS;
}

static int dist_cmp_fn (opal_list_item_t **a, opal_list_item_t **b)
{
    opal_rmaps_numa_node_t *aitem = *((opal_rmaps_numa_node_t **) a);
    opal_rmaps_numa_node_t *bitem = *((opal_rmaps_numa_node_t **) b);

    if (aitem->dist_from_closed > bitem->dist_from_closed) {
        return 1;
    } else if( aitem->dist_from_closed == bitem->dist_from_closed ) {
        return 0;
    } else {
        return -1;
    }
}

static void sort_by_dist(hwloc_topology_t topo, char* device_name, opal_list_t *sorted_list)
{
    hwloc_obj_t device_obj = NULL;
    hwloc_obj_t obj = NULL;
    struct hwloc_distances_s* distances;
    opal_rmaps_numa_node_t *numa_node;
    int close_node_index;
    float latency;
    unsigned int j;
#if HWLOC_API_VERSION < 0x20000
    hwloc_obj_t root = NULL;
    int depth;
    unsigned i;
#endif
    unsigned distances_nr = 0;

    for (device_obj = hwloc_get_obj_by_type(topo, HWLOC_OBJ_OS_DEVICE, 0); device_obj; device_obj = hwloc_get_next_osdev(topo, device_obj)) {
        if (device_obj->attr->osdev.type == HWLOC_OBJ_OSDEV_OPENFABRICS
                || device_obj->attr->osdev.type == HWLOC_OBJ_OSDEV_NETWORK) {
            if (!strcmp(device_obj->name, device_name)) {
                /* find numa node containing this device */
                obj = device_obj->parent;
#if HWLOC_API_VERSION < 0x20000
                while ((obj != NULL) && (obj->type != HWLOC_OBJ_NODE)) {
                    obj = obj->parent;
                }
#else
                while (obj && !obj->memory_arity) {
                    obj = obj->parent; /* no memory child, walk up */
                }
                if (obj != NULL) {
                    obj = obj->memory_first_child;
                }
#endif
                if (obj == NULL) {
                    opal_output_verbose(5, opal_hwloc_base_framework.framework_output,
                            "hwloc:base:get_sorted_numa_list: NUMA node closest to %s wasn't found.",
                            device_name);
                    return;
                } else {
                    close_node_index = obj->logical_index;
                }

                /* find distance matrix for all numa nodes */
#if HWLOC_API_VERSION < 0x20000
                distances = hwloc_get_whole_distance_matrix_by_type(topo, HWLOC_OBJ_NODE);
                if (NULL ==  distances) {
                    /* we can try to find distances under group object. This info can be there. */
                    depth = hwloc_get_type_depth(topo, HWLOC_OBJ_NODE);
                    if (HWLOC_TYPE_DEPTH_UNKNOWN == depth) {
                        opal_output_verbose(5, opal_hwloc_base_framework.framework_output,
                                "hwloc:base:get_sorted_numa_list: There is no information about distances on the node.");
                        return;
                    }
                    root = hwloc_get_root_obj(topo);
                    for (i = 0; i < root->arity; i++) {
                        obj = root->children[i];
                        if (obj->distances_count > 0) {
                            for(j = 0; j < obj->distances_count; j++) {
                                if (obj->distances[j]->relative_depth + 1 == (unsigned) depth) {
                                    distances = obj->distances[j];
                                    break;
                                }
                            }
                        }
                    }
                }
                /* find all distances for our close node with logical index = close_node_index as close_node_index + nbobjs*j */
                if ((NULL == distances) || (0 == distances->nbobjs)) {
                    opal_output_verbose(5, opal_hwloc_base_framework.framework_output,
                            "hwloc:base:get_sorted_numa_list: There is no information about distances on the node.");
                    return;
                }
                /* fill list of numa nodes */
                for (j = 0; j < distances->nbobjs; j++) {
                    latency = distances->latency[close_node_index + distances->nbobjs * j];
                    numa_node = OBJ_NEW(opal_rmaps_numa_node_t);
                    numa_node->index = j;
                    numa_node->dist_from_closed = latency;
                    opal_list_append(sorted_list, &numa_node->super);
                }
#else
                distances_nr = 1;
                if (0 != hwloc_distances_get_by_type(topo, HWLOC_OBJ_NODE, &distances_nr, &distances,
                                                     HWLOC_DISTANCES_KIND_MEANS_LATENCY, 0) || 0 == distances_nr) {
                    opal_output_verbose(5, opal_hwloc_base_framework.framework_output,
                            "hwloc:base:get_sorted_numa_list: There is no information about distances on the node.");
                    return;
                }
                /* fill list of numa nodes */
                for (j = 0; j < distances->nbobjs; j++) {
                    latency = distances->values[close_node_index + distances->nbobjs * j];
                    numa_node = OBJ_NEW(opal_rmaps_numa_node_t);
                    numa_node->index = j;
                    numa_node->dist_from_closed = latency;
                    opal_list_append(sorted_list, &numa_node->super);
                }
                hwloc_distances_release(topo, distances);
#endif
                /* sort numa nodes by distance from the closest one to PCI */
                opal_list_sort(sorted_list, dist_cmp_fn);
                return;
            }
        }
    }
}

static int find_devices(hwloc_topology_t topo, char** device_name)
{
    hwloc_obj_t device_obj = NULL;
    int count = 0;
    for (device_obj = hwloc_get_obj_by_type(topo, HWLOC_OBJ_OS_DEVICE, 0); device_obj; device_obj = hwloc_get_next_osdev(topo, device_obj)) {
        if (device_obj->attr->osdev.type == HWLOC_OBJ_OSDEV_OPENFABRICS) {
            count++;
            free(*device_name);
            *device_name = strdup(device_obj->name);
        }
    }
    return count;
}

int opal_hwloc_get_sorted_numa_list(hwloc_topology_t topo, char* device_name, opal_list_t *sorted_list)
{
    hwloc_obj_t obj;
    opal_hwloc_summary_t *sum;
    opal_hwloc_topo_data_t *data;
    opal_rmaps_numa_node_t *numa, *copy_numa;
    int count;

    obj = hwloc_get_root_obj(topo);

    /* first see if the topology already has this info */
    /* we call opal_hwloc_base_get_nbobjs_by_type() before it to fill summary object so it should exist*/
    data = (opal_hwloc_topo_data_t*)obj->userdata;
    if (NULL != data) {
        OPAL_LIST_FOREACH(sum, &data->summaries, opal_hwloc_summary_t) {
            if (HWLOC_OBJ_NODE == sum->type) {
                if (opal_list_get_size(&sum->sorted_by_dist_list) > 0) {
                    OPAL_LIST_FOREACH(numa, &(sum->sorted_by_dist_list), opal_rmaps_numa_node_t) {
                        copy_numa = OBJ_NEW(opal_rmaps_numa_node_t);
                        copy_numa->index = numa->index;
                        copy_numa->dist_from_closed = numa->dist_from_closed;
                        opal_list_append(sorted_list, &copy_numa->super);
                    }
                    return OPAL_SUCCESS;
                }else {
                    /* don't already know it - go get it */
                    /* firstly we check if we need to autodetect OpenFabrics  devices or we have the specified one */
                    bool free_device_name = false;
                    if (!strcmp(device_name, "auto")) {
                        count = find_devices(topo, &device_name);
                        if (count > 1) {
                            free(device_name);
                            return count;
                        }
                        free_device_name = true;
                    }
                    if (!device_name) {
                        return OPAL_ERR_NOT_FOUND;
                    } else if (free_device_name && (0 == strlen(device_name))) {
                        free(device_name);
                        return OPAL_ERR_NOT_FOUND;
                    }
                    sort_by_dist(topo, device_name, sorted_list);
                    if (free_device_name) {
                        free(device_name);
                    }
                    /* store this info in summary object for later usage */
                    OPAL_LIST_FOREACH(numa, sorted_list, opal_rmaps_numa_node_t) {
                        copy_numa = OBJ_NEW(opal_rmaps_numa_node_t);
                        copy_numa->index = numa->index;
                        copy_numa->dist_from_closed = numa->dist_from_closed;
                        opal_list_append(&(sum->sorted_by_dist_list), &copy_numa->super);
                    }
                    return OPAL_SUCCESS;
                }
            }
        }
    }
    return OPAL_ERR_NOT_FOUND;
}

char* opal_hwloc_base_get_topo_signature(hwloc_topology_t topo)
{
    int nnuma, nsocket, nl3, nl2, nl1, ncore, nhwt;
    char *sig=NULL, *arch = NULL, *endian;
    hwloc_obj_t obj;
    unsigned i;

    nnuma = opal_hwloc_base_get_nbobjs_by_type(topo, HWLOC_OBJ_NODE, 0, OPAL_HWLOC_AVAILABLE);
    nsocket = opal_hwloc_base_get_nbobjs_by_type(topo, HWLOC_OBJ_SOCKET, 0, OPAL_HWLOC_AVAILABLE);
    nl3 = opal_hwloc_base_get_nbobjs_by_type(topo, HWLOC_OBJ_L3CACHE, 3, OPAL_HWLOC_AVAILABLE);
    nl2 = opal_hwloc_base_get_nbobjs_by_type(topo, HWLOC_OBJ_L2CACHE, 2, OPAL_HWLOC_AVAILABLE);
    nl1 = opal_hwloc_base_get_nbobjs_by_type(topo, HWLOC_OBJ_L1CACHE, 1, OPAL_HWLOC_AVAILABLE);
    ncore = opal_hwloc_base_get_nbobjs_by_type(topo, HWLOC_OBJ_CORE, 0, OPAL_HWLOC_AVAILABLE);
    nhwt = opal_hwloc_base_get_nbobjs_by_type(topo, HWLOC_OBJ_PU, 0, OPAL_HWLOC_AVAILABLE);

    /* get the root object so we can add the processor architecture */
    obj = hwloc_get_root_obj(topo);
    for (i=0; i < obj->infos_count; i++) {
        if (0 == strcmp(obj->infos[i].name, "Architecture")) {
            arch = obj->infos[i].value;
            break;
        }
    }
    if (NULL == arch) {
        arch = "unknown";
    }

#ifdef __BYTE_ORDER
#if __BYTE_ORDER == __LITTLE_ENDIAN
    endian = "le";
#else
    endian = "be";
#endif
#else
    endian = "unknown";
#endif

    asprintf(&sig, "%dN:%dS:%dL3:%dL2:%dL1:%dC:%dH:%s:%s",
             nnuma, nsocket, nl3, nl2, nl1, ncore, nhwt, arch, endian);
    return sig;
}

char* opal_hwloc_base_get_locality_string(hwloc_topology_t topo,
                                          char *bitmap)
{
    hwloc_obj_t obj;
    char *locality=NULL, *tmp, *t2;
    unsigned depth, d, width, w;
    hwloc_cpuset_t cpuset, result;
    hwloc_obj_type_t type;

    /* if this proc is not bound, then there is no locality. We
     * know it isn't bound if the cpuset is NULL, or if it is
     * all 1's */
    if (NULL == bitmap) {
        return NULL;
    }
    cpuset = hwloc_bitmap_alloc();
    hwloc_bitmap_list_sscanf(cpuset, bitmap);
    if (hwloc_bitmap_isfull(cpuset)) {
        hwloc_bitmap_free(cpuset);
        return NULL;
    }

    /* we are going to use a bitmap to save the results so
     * that we can use a hwloc utility to print them */
    result = hwloc_bitmap_alloc();

    /* get the max depth of the topology */
    depth = hwloc_topology_get_depth(topo);

    /* start at the first depth below the top machine level */
    for (d=1; d < depth; d++) {
        /* get the object type at this depth */
        type = hwloc_get_depth_type(topo, d);
        /* if it isn't one of interest, then ignore it */
        if (HWLOC_OBJ_NODE != type &&
            HWLOC_OBJ_SOCKET != type &&
#if HWLOC_API_VERSION < 0x20000
            HWLOC_OBJ_CACHE != type &&
#else
            HWLOC_OBJ_L1CACHE != type &&
            HWLOC_OBJ_L2CACHE != type &&
            HWLOC_OBJ_L3CACHE != type &&
#endif
            HWLOC_OBJ_CORE != type &&
            HWLOC_OBJ_PU != type) {
            continue;
        }

        /* get the width of the topology at this depth */
        width = hwloc_get_nbobjs_by_depth(topo, d);
        if (0 == width) {
            continue;
        }

        /* scan all objects at this depth to see if
         * the location overlaps with them
         */
        for (w=0; w < width; w++) {
            /* get the object at this depth/index */
            obj = hwloc_get_obj_by_depth(topo, d, w);
            /* see if the location intersects with it */
            if (hwloc_bitmap_intersects(obj->cpuset, cpuset)) {
                hwloc_bitmap_set(result, w);
            }
        }
        /* it should be impossible, but allow for the possibility
         * that we came up empty at this depth */
        if (!hwloc_bitmap_iszero(result)) {
            hwloc_bitmap_list_asprintf(&tmp, result);
            switch(obj->type) {
                case HWLOC_OBJ_NODE:
                    asprintf(&t2, "%sNM%s:", (NULL == locality) ? "" : locality, tmp);
                    if (NULL != locality) {
                        free(locality);
                    }
                    locality = t2;
                    break;
                case HWLOC_OBJ_SOCKET:
                    asprintf(&t2, "%sSK%s:", (NULL == locality) ? "" : locality, tmp);
                    if (NULL != locality) {
                        free(locality);
                    }
                    locality = t2;
                    break;
#if HWLOC_API_VERSION < 0x20000
                case HWLOC_OBJ_CACHE:
                    if (3 == obj->attr->cache.depth) {
                        asprintf(&t2, "%sL3%s:", (NULL == locality) ? "" : locality, tmp);
                        if (NULL != locality) {
                            free(locality);
                        }
                        locality = t2;
                        break;
                    } else if (2 == obj->attr->cache.depth) {
                        asprintf(&t2, "%sL2%s:", (NULL == locality) ? "" : locality, tmp);
                        if (NULL != locality) {
                            free(locality);
                        }
                        locality = t2;
                        break;
                    } else {
                        asprintf(&t2, "%sL1%s:", (NULL == locality) ? "" : locality, tmp);
                        if (NULL != locality) {
                            free(locality);
                        }
                        locality = t2;
                        break;
                    }
                    break;
#else
                case HWLOC_OBJ_L3CACHE:
                    asprintf(&t2, "%sL3%s:", (NULL == locality) ? "" : locality, tmp);
                    if (NULL != locality) {
                        free(locality);
                    }
                    locality = t2;
                    break;
                case HWLOC_OBJ_L2CACHE:
                    asprintf(&t2, "%sL2%s:", (NULL == locality) ? "" : locality, tmp);
                    if (NULL != locality) {
                        free(locality);
                    }
                    locality = t2;
                    break;
                case HWLOC_OBJ_L1CACHE:
                    asprintf(&t2, "%sL1%s:", (NULL == locality) ? "" : locality, tmp);
                    if (NULL != locality) {
                        free(locality);
                    }
                    locality = t2;
                    break;
#endif
                case HWLOC_OBJ_CORE:
                    asprintf(&t2, "%sCR%s:", (NULL == locality) ? "" : locality, tmp);
                    if (NULL != locality) {
                        free(locality);
                    }
                    locality = t2;
                    break;
                case HWLOC_OBJ_PU:
                    asprintf(&t2, "%sHT%s:", (NULL == locality) ? "" : locality, tmp);
                    if (NULL != locality) {
                        free(locality);
                    }
                    locality = t2;
                    break;
                default:
                    /* just ignore it */
                    break;
            }
            free(tmp);
        }
        hwloc_bitmap_zero(result);
    }
    hwloc_bitmap_free(result);
    hwloc_bitmap_free(cpuset);

    /* remove the trailing colon */
    if (NULL != locality) {
        locality[strlen(locality)-1] = '\0';
    }
    return locality;
}

char* opal_hwloc_base_get_location(char *locality,
                                   hwloc_obj_type_t type,
                                   unsigned index)
{
    char **loc;
    char *srch, *ans = NULL;
    size_t n;

    if (NULL == locality) {
        return NULL;
    }
    switch(type) {
        case HWLOC_OBJ_NODE:
            srch = "NM";
            break;
        case HWLOC_OBJ_SOCKET:
            srch = "SK";
            break;
#if HWLOC_API_VERSION < 0x20000
        case HWLOC_OBJ_CACHE:
            if (3 == index) {
                srch = "L3";
            } else if (2 == index) {
                srch = "L2";
            } else {
                srch = "L0";
            }
            break;
#else
        case HWLOC_OBJ_L3CACHE:
            srch = "L3";
            break;
        case HWLOC_OBJ_L2CACHE:
            srch = "L2";
            break;
        case HWLOC_OBJ_L1CACHE:
            srch = "L0";
            break;
#endif
        case HWLOC_OBJ_CORE:
            srch = "CR";
            break;
        case HWLOC_OBJ_PU:
            srch = "HT";
            break;
        default:
            return NULL;
    }
    loc = opal_argv_split(locality, ':');
    for (n=0; NULL != loc[n]; n++) {
        if (0 == strncmp(loc[n], srch, 2)) {
            ans = strdup(&loc[n][2]);
            break;
        }
    }
    opal_argv_free(loc);

    return ans;
}

opal_hwloc_locality_t opal_hwloc_compute_relative_locality(char *loc1, char *loc2)
{
    opal_hwloc_locality_t locality;
    char **set1, **set2;
    hwloc_bitmap_t bit1, bit2;
    size_t n1, n2;

    /* start with what we know - they share a node on a cluster
     * NOTE: we may alter that latter part as hwloc's ability to
     * sense multi-cu, multi-cluster systems grows
     */
    locality = OPAL_PROC_ON_NODE | OPAL_PROC_ON_HOST | OPAL_PROC_ON_CU | OPAL_PROC_ON_CLUSTER;

    /* if either location is NULL, then that isn't bound */
    if (NULL == loc1 || NULL == loc2) {
        return locality;
    }

    set1 = opal_argv_split(loc1, ':');
    set2 = opal_argv_split(loc2, ':');
    bit1 = hwloc_bitmap_alloc();
    bit2 = hwloc_bitmap_alloc();

    /* check each matching type */
    for (n1=0; NULL != set1[n1]; n1++) {
        /* convert the location into bitmap */
        hwloc_bitmap_list_sscanf(bit1, &set1[n1][2]);
        /* find the matching type in set2 */
        for (n2=0; NULL != set2[n2]; n2++) {
            if (0 == strncmp(set1[n1], set2[n2], 2)) {
                /* convert the location into bitmap */
                hwloc_bitmap_list_sscanf(bit2, &set2[n2][2]);
                /* see if they intersect */
                if (hwloc_bitmap_intersects(bit1, bit2)) {
                    /* set the corresponding locality bit */
                    if (0 == strncmp(set1[n1], "NM", 2)) {
                        locality |= OPAL_PROC_ON_NUMA;
                    } else if (0 == strncmp(set1[n1], "SK", 2)) {
                        locality |= OPAL_PROC_ON_SOCKET;
                    } else if (0 == strncmp(set1[n1], "L3", 2)) {
                        locality |= OPAL_PROC_ON_L3CACHE;
                    } else if (0 == strncmp(set1[n1], "L2", 2)) {
                        locality |= OPAL_PROC_ON_L2CACHE;
                    } else if (0 == strncmp(set1[n1], "L1", 2)) {
                        locality |= OPAL_PROC_ON_L1CACHE;
                    } else if (0 == strncmp(set1[n1], "CR", 2)) {
                        locality |= OPAL_PROC_ON_CORE;
                    } else if (0 == strncmp(set1[n1], "HT", 2)) {
                        locality |= OPAL_PROC_ON_HWTHREAD;
                    } else {
                        /* should never happen */
                        opal_output(0, "UNRECOGNIZED LOCALITY %s", set1[n1]);
                    }
                }
                break;
            }
        }
    }
    opal_argv_free(set1);
    opal_argv_free(set2);
    hwloc_bitmap_free(bit1);
    hwloc_bitmap_free(bit2);
    return locality;
}

int opal_hwloc_base_topology_export_xmlbuffer(hwloc_topology_t topology, char **xmlpath, int *buflen) {
#if HWLOC_API_VERSION < 0x20000
    return hwloc_topology_export_xmlbuffer(topology, xmlpath, buflen);
#else
    return hwloc_topology_export_xmlbuffer(topology, xmlpath, buflen, 0);
#endif
}

int opal_hwloc_base_topology_set_flags (hwloc_topology_t topology, unsigned long flags, bool io) {
    if (io) {
#if HWLOC_API_VERSION < 0x20000
        flags |= HWLOC_TOPOLOGY_FLAG_IO_DEVICES;
#else
        int ret = hwloc_topology_set_io_types_filter(topology, HWLOC_TYPE_FILTER_KEEP_IMPORTANT);
        if (0 != ret) return ret;
#endif
    }
    return hwloc_topology_set_flags(topology, flags);
}
