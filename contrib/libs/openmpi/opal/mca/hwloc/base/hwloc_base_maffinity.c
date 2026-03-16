/*
 * Copyright (c) 2011-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"

#include "opal/constants.h"

#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/mca/hwloc/base/base.h"


/*
 * Don't use show_help() here (or print any error message at all).
 * Let the upper layer output a relevant message, because doing so may
 * be complicated (e.g., this might be called from the ORTE ODLS,
 * which has to do some extra steps to get error messages to be
 * displayed).
 */
int opal_hwloc_base_set_process_membind_policy(void)
{
    int rc = 0, flags;
    hwloc_membind_policy_t policy;
    hwloc_cpuset_t cpuset;

    /* Make sure opal_hwloc_topology has been set by the time we've
       been called */
    if (OPAL_SUCCESS != opal_hwloc_base_get_topology()) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* Set the default memory allocation policy according to MCA
       param */
    switch (opal_hwloc_base_map) {
    case OPAL_HWLOC_BASE_MAP_LOCAL_ONLY:
        policy = HWLOC_MEMBIND_BIND;
        flags = HWLOC_MEMBIND_STRICT;
        break;

    case OPAL_HWLOC_BASE_MAP_NONE:
    default:
        policy = HWLOC_MEMBIND_DEFAULT;
        flags = 0;
        break;
    }

    cpuset = hwloc_bitmap_alloc();
    if (NULL == cpuset) {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
    } else {
        int e;
        hwloc_get_cpubind(opal_hwloc_topology, cpuset, 0);
        rc = hwloc_set_membind(opal_hwloc_topology,
                               cpuset, policy, flags);
        e = errno;
        hwloc_bitmap_free(cpuset);

        /* See if hwloc was able to do it.  If hwloc failed due to
           ENOSYS, but the base_map == NONE, then it's not really an
           error. */
        if (0 != rc && ENOSYS == e &&
            OPAL_HWLOC_BASE_MAP_NONE == opal_hwloc_base_map) {
            rc = 0;
        }
    }

    return (0 == rc) ? OPAL_SUCCESS : OPAL_ERROR;
}

int opal_hwloc_base_memory_set(opal_hwloc_base_memory_segment_t *segments,
                               size_t num_segments)
{
    int rc = OPAL_SUCCESS;
    char *msg = NULL;
    size_t i;
    hwloc_cpuset_t cpuset = NULL;

    /* bozo check */
    if (OPAL_SUCCESS != opal_hwloc_base_get_topology()) {
        msg = "hwloc_set_area_membind() failure - topology not available";
        return opal_hwloc_base_report_bind_failure(__FILE__, __LINE__,
                                                   msg, rc);
    }

    /* This module won't be used unless the process is already
       processor-bound.  So find out where we're processor bound, and
       bind our memory there, too. */
    cpuset = hwloc_bitmap_alloc();
    if (NULL == cpuset) {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        msg = "hwloc_bitmap_alloc() failure";
        goto out;
    }
    hwloc_get_cpubind(opal_hwloc_topology, cpuset, 0);
    for (i = 0; i < num_segments; ++i) {
        if (0 != hwloc_set_area_membind(opal_hwloc_topology,
                                        segments[i].mbs_start_addr,
                                        segments[i].mbs_len, cpuset,
                                        HWLOC_MEMBIND_BIND,
                                        HWLOC_MEMBIND_STRICT)) {
            rc = OPAL_ERROR;
            msg = "hwloc_set_area_membind() failure";
            goto out;
        }
    }

 out:
    if (NULL != cpuset) {
        hwloc_bitmap_free(cpuset);
    }
    if (OPAL_SUCCESS != rc) {
        return opal_hwloc_base_report_bind_failure(__FILE__, __LINE__, msg, rc);
    }
    return OPAL_SUCCESS;
}

int opal_hwloc_base_node_name_to_id(char *node_name, int *id)
{
    /* GLB: fix me */
    *id = atoi(node_name + 3);

    return OPAL_SUCCESS;
}

int opal_hwloc_base_membind(opal_hwloc_base_memory_segment_t *segs,
                            size_t count, int node_id)
{
    size_t i;
    int rc = OPAL_SUCCESS;
    char *msg = NULL;
    hwloc_cpuset_t cpuset = NULL;

    /* bozo check */
    if (OPAL_SUCCESS != opal_hwloc_base_get_topology()) {
        msg = "hwloc_set_area_membind() failure - topology not available";
        return opal_hwloc_base_report_bind_failure(__FILE__, __LINE__,
                                                   msg, rc);
    }

    cpuset = hwloc_bitmap_alloc();
    if (NULL == cpuset) {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        msg = "hwloc_bitmap_alloc() failure";
        goto out;
    }
    hwloc_bitmap_set(cpuset, node_id);
    for(i = 0; i < count; i++) {
        if (0 != hwloc_set_area_membind(opal_hwloc_topology,
                                        segs[i].mbs_start_addr,
                                        segs[i].mbs_len, cpuset,
                                        HWLOC_MEMBIND_BIND,
                                        HWLOC_MEMBIND_STRICT)) {
            rc = OPAL_ERROR;
            msg = "hwloc_set_area_membind() failure";
            goto out;
        }
    }

 out:
    if (NULL != cpuset) {
        hwloc_bitmap_free(cpuset);
    }
    if (OPAL_SUCCESS != rc) {
        return opal_hwloc_base_report_bind_failure(__FILE__, __LINE__, msg, rc);
    }
    return OPAL_SUCCESS;
}
