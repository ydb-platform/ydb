/*
 * Copyright (c) 2004-2009 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2010-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * Simple routine to expose three things to the MPI process:
 *
 * 1. What processor(s) Open MPI bound this process to
 * 2. What processor(s) this process is bound to
 * 3. What processor(s) exist on this host
 *
 * Note that 1 and 2 may be different!
 */

#include "ompi_config.h"

#include <stdio.h>
#include <string.h>

#include "opal/mca/hwloc/base/base.h"
#include "opal/runtime/opal.h"

#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mca/rte/rte.h"
#include "ompi/mpi/c/bindings.h"
#include "ompi/mpiext/affinity/c/mpiext_affinity_c.h"

static const char FUNC_NAME[] = "OMPI_Affinity";
static const char ompi_nobind_str[] = "Open MPI did not bind this process";
static const char not_bound_str[] = "Not bound (i.e., bound to all processors)";


static int get_rsrc_ompi_bound(char str[OMPI_AFFINITY_STRING_MAX]);
static int get_rsrc_current_binding(char str[OMPI_AFFINITY_STRING_MAX]);
static int get_rsrc_exists(char str[OMPI_AFFINITY_STRING_MAX]);
static int get_layout_ompi_bound(char str[OMPI_AFFINITY_STRING_MAX]);
static int get_layout_current_binding(char str[OMPI_AFFINITY_STRING_MAX]);
static int get_layout_exists(char str[OMPI_AFFINITY_STRING_MAX]);


int OMPI_Affinity_str(ompi_affinity_fmt_t fmt_type,
                      char ompi_bound[OMPI_AFFINITY_STRING_MAX],
                      char current_binding[OMPI_AFFINITY_STRING_MAX],
                      char exists[OMPI_AFFINITY_STRING_MAX])
{
    int ret;

    memset(ompi_bound, 0, OMPI_AFFINITY_STRING_MAX);
    memset(current_binding, 0, OMPI_AFFINITY_STRING_MAX);
    memset(exists, 0, OMPI_AFFINITY_STRING_MAX);

    /* If we have no hwloc support, return nothing */
    if (NULL == opal_hwloc_topology) {
        return MPI_SUCCESS;
    }

    /* Otherwise, return useful information */
    switch (fmt_type) {
    case OMPI_AFFINITY_RSRC_STRING_FMT:
    if (OMPI_SUCCESS != (ret = get_rsrc_ompi_bound(ompi_bound)) ||
        OMPI_SUCCESS != (ret = get_rsrc_current_binding(current_binding)) ||
        OMPI_SUCCESS != (ret = get_rsrc_exists(exists))) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, ret, FUNC_NAME);
    }
    break;
    case OMPI_AFFINITY_LAYOUT_FMT:
    if (OMPI_SUCCESS != (ret = get_layout_ompi_bound(ompi_bound)) ||
        OMPI_SUCCESS != (ret = get_layout_current_binding(current_binding)) ||
        OMPI_SUCCESS != (ret = get_layout_exists(exists))) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, ret, FUNC_NAME);
    }
    break;
    default:
    return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
    }

    return MPI_SUCCESS;
}

/*---------------------------------------------------------------------------*/

/*
 * Where did OMPI bind this process? (prettyprint)
 */
static int get_rsrc_ompi_bound(char str[OMPI_AFFINITY_STRING_MAX])
{
    int ret;

    /* If OMPI did not bind, indicate that */
    if (!ompi_rte_proc_is_bound) {
        strncpy(str, ompi_nobind_str, OMPI_AFFINITY_STRING_MAX - 1);
        return OMPI_SUCCESS;
    }

    if (NULL == ompi_proc_applied_binding) {
        ret = OPAL_ERR_NOT_BOUND;
    } else {
        ret = opal_hwloc_base_cset2str(str, OMPI_AFFINITY_STRING_MAX,
                                       opal_hwloc_topology,
                                       ompi_proc_applied_binding);
    }
    if (OPAL_ERR_NOT_BOUND == ret) {
        strncpy(str, not_bound_str, OMPI_AFFINITY_STRING_MAX - 1);
        ret = OMPI_SUCCESS;
    }
    return ret;
}


/*
 * Where is this process currently bound? (prettyprint)
 */
static int get_rsrc_current_binding(char str[OMPI_AFFINITY_STRING_MAX])
{
    int ret;
    hwloc_obj_t root;
    hwloc_cpuset_t boundset, rootset;
    bool bound = false;

    /* get our root object */
    root = hwloc_get_root_obj(opal_hwloc_topology);
    rootset = root->cpuset;

    /* get our bindings */
    boundset = hwloc_bitmap_alloc();
    if (hwloc_get_cpubind(opal_hwloc_topology, boundset,
                          HWLOC_CPUBIND_PROCESS) < 0) {
        /* we are NOT bound if get_cpubind fails, nor can we be bound
           - the environment does not support it */
        bound = false;
    } else {
        /* we are bound if the two cpusets are not equal, or if there
           is only ONE PU available to us */
        if (0 != hwloc_bitmap_compare(boundset, rootset) ||
            opal_hwloc_base_single_cpu(rootset) ||
            opal_hwloc_base_single_cpu(boundset)) {
            bound = true;
        }
    }

    /* If we are not bound, indicate that */
    if (!bound) {
        strncat(str, not_bound_str, OMPI_AFFINITY_STRING_MAX - 1);
        ret = OMPI_SUCCESS;
    }

    /* If we are bound, print it out */
    else {
        ret = opal_hwloc_base_cset2str(str, OMPI_AFFINITY_STRING_MAX,
                                       opal_hwloc_topology,
                                       boundset);
        if (OPAL_ERR_NOT_BOUND == ret) {
            strncpy(str, not_bound_str, OMPI_AFFINITY_STRING_MAX - 1);
            ret = OMPI_SUCCESS;
        }
    }
    hwloc_bitmap_free(boundset);

    return ret;
}


/*
 * Prettyprint a list of all available sockets and cores.  Note that
 * this is *everything* -- not just the ones that are available to
 * this process.
 */
static int get_rsrc_exists(char str[OMPI_AFFINITY_STRING_MAX])
{
    bool first = true;
    int i, num_cores, num_pus;
    char tmp[BUFSIZ];
    const int stmp = sizeof(tmp) - 1;
    hwloc_obj_t socket, core, c2;

    str[0] = '\0';
    for (socket = hwloc_get_obj_by_type(opal_hwloc_topology,
                                        HWLOC_OBJ_SOCKET, 0);
         NULL != socket; socket = socket->next_cousin) {
        /* If this isn't the first socket, add a delimiter */
        if (!first) {
            strncat(str, "; ", OMPI_AFFINITY_STRING_MAX - strlen(str));
        }
        first = false;

        snprintf(tmp, stmp, "socket %d has ", socket->os_index);
        strncat(str, tmp, OMPI_AFFINITY_STRING_MAX - strlen(str));

        /* Find out how many cores are inside this socket, and get an
           object pointing to the first core.  Also count how many PUs
           are in the first core. */
        num_cores = hwloc_get_nbobjs_inside_cpuset_by_type(opal_hwloc_topology,
                                                           socket->cpuset,
                                                           HWLOC_OBJ_CORE);
        core = hwloc_get_obj_inside_cpuset_by_type(opal_hwloc_topology,
                                                   socket->cpuset,
                                                   HWLOC_OBJ_CORE, 0);
        if (NULL != core) {
            num_pus =
                hwloc_get_nbobjs_inside_cpuset_by_type(opal_hwloc_topology,
                                                       core->cpuset,
                                                       HWLOC_OBJ_PU);

            /* Only 1 core */
            if (1 == num_cores) {
                strncat(str, "1 core with ",
                        OMPI_AFFINITY_STRING_MAX - strlen(str));
                if (1 == num_pus) {
                    strncat(str, "1 hwt",
                            OMPI_AFFINITY_STRING_MAX - strlen(str));
                } else {
                    snprintf(tmp, stmp, "%d hwts", num_pus);
                    strncat(str, tmp, OMPI_AFFINITY_STRING_MAX - strlen(str));
                }
            }

            /* Multiple cores */
            else {
                bool same = true;

                snprintf(tmp, stmp, "%d cores", num_cores);
                strncat(str, tmp, OMPI_AFFINITY_STRING_MAX - strlen(str));

                /* Do all the cores have the same number of PUs? */
                for (c2 = core; NULL != c2; c2 = c2->next_cousin) {
                    if (hwloc_get_nbobjs_inside_cpuset_by_type(opal_hwloc_topology,
                                                               core->cpuset,
                                                               HWLOC_OBJ_PU) !=
                        num_pus) {
                        same = false;
                        break;
                    }
                }

                /* Yes, they all have the same number of PUs */
                if (same) {
                    snprintf(tmp, stmp, ", each with %d hwt", num_pus);
                    strncat(str, tmp, OMPI_AFFINITY_STRING_MAX - strlen(str));
                    if (num_pus != 1) {
                        strncat(str, "s", OMPI_AFFINITY_STRING_MAX - strlen(str));
                    }
                }

                /* No, they have differing numbers of PUs */
                else {
                    bool first = true;

                    strncat(str, "with (", OMPI_AFFINITY_STRING_MAX - strlen(str));
                    for (c2 = core; NULL != c2; c2 = c2->next_cousin) {
                        if (!first) {
                            strncat(str, ", ",
                                    OMPI_AFFINITY_STRING_MAX - strlen(str));
                        }
                        first = false;

                        i = hwloc_get_nbobjs_inside_cpuset_by_type(opal_hwloc_topology,
                                                                   core->cpuset,
                                                                   HWLOC_OBJ_PU);
                        snprintf(tmp, stmp, "%d", i);
                        strncat(str, tmp, OMPI_AFFINITY_STRING_MAX - strlen(str));
                    }
                    strncat(str, ") hwts",
                            OMPI_AFFINITY_STRING_MAX - strlen(str));
                }
            }
        }
    }

    return OMPI_SUCCESS;
}

/*---------------------------------------------------------------------------*/

/*
 * Where did OMPI bind this process? (layout string)
 */
static int get_layout_ompi_bound(char str[OMPI_AFFINITY_STRING_MAX])
{
    int ret;

    /* If OMPI did not bind, indicate that */
    if (!ompi_rte_proc_is_bound) {
        strncpy(str, ompi_nobind_str, OMPI_AFFINITY_STRING_MAX - 1);
        return OMPI_SUCCESS;
    }

    /* Find out what OMPI bound us to and prettyprint it */
    if (NULL == ompi_proc_applied_binding) {
        ret = OPAL_ERR_NOT_BOUND;
    } else {
        ret = opal_hwloc_base_cset2mapstr(str, OMPI_AFFINITY_STRING_MAX,
                                          opal_hwloc_topology,
                                          ompi_proc_applied_binding);
    }
    if (OPAL_ERR_NOT_BOUND == ret) {
        strncpy(str, not_bound_str, OMPI_AFFINITY_STRING_MAX - 1);
        ret = OMPI_SUCCESS;
    }

    return ret;
}

/*
 * Where is this process currently bound? (layout string)
 */
static int get_layout_current_binding(char str[OMPI_AFFINITY_STRING_MAX])
{
    int ret;
    hwloc_obj_t root;
    hwloc_cpuset_t boundset, rootset;
    bool bound = false;

    /* get our root object */
    root = hwloc_get_root_obj(opal_hwloc_topology);
    rootset = root->cpuset;

    /* get our bindings */
    boundset = hwloc_bitmap_alloc();
    if (hwloc_get_cpubind(opal_hwloc_topology, boundset,
                          HWLOC_CPUBIND_PROCESS) < 0) {
        /* we are NOT bound if get_cpubind fails, nor can we be bound
           - the environment does not support it */
        bound = false;
    } else {
        /* we are bound if the two cpusets are not equal, or if there
           is only ONE PU available to us */
        if (0 != hwloc_bitmap_compare(boundset, rootset) ||
            opal_hwloc_base_single_cpu(rootset) ||
            opal_hwloc_base_single_cpu(boundset)) {
            bound = true;
        }
    }

    /* If we are not bound, indicate that */
    if (!bound) {
        strncat(str, not_bound_str, OMPI_AFFINITY_STRING_MAX - 1);
        ret = OMPI_SUCCESS;
    }

    /* If we are bound, print it out */
    else {
        ret = opal_hwloc_base_cset2mapstr(str, OMPI_AFFINITY_STRING_MAX,
                                          opal_hwloc_topology,
                                          boundset);
        if (OPAL_ERR_NOT_BOUND == ret) {
            strncpy(str, not_bound_str, OMPI_AFFINITY_STRING_MAX - 1);
            ret = OMPI_SUCCESS;
        }
    }
    hwloc_bitmap_free(boundset);

    return ret;
}

/*
 * Make a layout string of all available sockets and cores.  Note that
 * this is *everything* -- not just the ones that are available to
 * this process.
 *
 * Example: [../..]
 * Key:  [] - signifies socket
 *        / - signifies core
 *        . - signifies PU
 */
static int get_layout_exists(char str[OMPI_AFFINITY_STRING_MAX])
{
    int core_index, pu_index;
    int len = OMPI_AFFINITY_STRING_MAX;
    hwloc_obj_t socket, core, pu;

    str[0] = '\0';

    /* Iterate over all existing sockets */
    for (socket = hwloc_get_obj_by_type(opal_hwloc_topology,
                                        HWLOC_OBJ_SOCKET, 0);
         NULL != socket;
         socket = socket->next_cousin) {
        strncat(str, "[", len - strlen(str));

        /* Iterate over all existing cores in this socket */
        core_index = 0;
        for (core = hwloc_get_obj_inside_cpuset_by_type(opal_hwloc_topology,
                                                        socket->cpuset,
                                                        HWLOC_OBJ_CORE, core_index);
             NULL != core;
             core = hwloc_get_obj_inside_cpuset_by_type(opal_hwloc_topology,
                                                        socket->cpuset,
                                                        HWLOC_OBJ_CORE, ++core_index)) {
            if (core_index > 0) {
                strncat(str, "/", len - strlen(str));
            }

            /* Iterate over all existing PUs in this core */
            pu_index = 0;
            for (pu = hwloc_get_obj_inside_cpuset_by_type(opal_hwloc_topology,
                                                          core->cpuset,
                                                          HWLOC_OBJ_PU, pu_index);
                 NULL != pu;
                 pu = hwloc_get_obj_inside_cpuset_by_type(opal_hwloc_topology,
                                                          core->cpuset,
                                                          HWLOC_OBJ_PU, ++pu_index)) {
                strncat(str, ".", len - strlen(str));
            }
        }
        strncat(str, "]", len - strlen(str));
    }

    return OMPI_SUCCESS;
}
