/*
 * Copyright (c) 2011-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"

#include "opal/constants.h"
#include "opal/dss/dss.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/threads/tsd.h"

#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/mca/hwloc/base/base.h"


/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "opal/mca/hwloc/base/static-components.h"


/*
 * Globals
 */
bool opal_hwloc_base_inited = false;
hwloc_topology_t opal_hwloc_topology=NULL;
hwloc_cpuset_t opal_hwloc_my_cpuset=NULL;
hwloc_cpuset_t opal_hwloc_base_given_cpus=NULL;
opal_hwloc_base_map_t opal_hwloc_base_map = OPAL_HWLOC_BASE_MAP_NONE;
opal_hwloc_base_mbfa_t opal_hwloc_base_mbfa = OPAL_HWLOC_BASE_MBFA_WARN;
opal_binding_policy_t opal_hwloc_binding_policy=0;
char *opal_hwloc_base_cpu_list=NULL;
bool opal_hwloc_report_bindings=false;
hwloc_obj_type_t opal_hwloc_levels[] = {
    HWLOC_OBJ_MACHINE,
    HWLOC_OBJ_NODE,
    HWLOC_OBJ_SOCKET,
    HWLOC_OBJ_L3CACHE,
    HWLOC_OBJ_L2CACHE,
    HWLOC_OBJ_L1CACHE,
    HWLOC_OBJ_CORE,
    HWLOC_OBJ_PU
};
bool opal_hwloc_use_hwthreads_as_cpus = false;
char *opal_hwloc_base_topo_file = NULL;

static mca_base_var_enum_value_t hwloc_base_map[] = {
    {OPAL_HWLOC_BASE_MAP_NONE, "none"},
    {OPAL_HWLOC_BASE_MAP_LOCAL_ONLY, "local_only"},
    {0, NULL}
};

static mca_base_var_enum_value_t hwloc_failure_action[] = {
    {OPAL_HWLOC_BASE_MBFA_SILENT, "silent"},
    {OPAL_HWLOC_BASE_MBFA_WARN, "warn"},
    {OPAL_HWLOC_BASE_MBFA_ERROR, "error"},
    {0, NULL}
};

static int opal_hwloc_base_register(mca_base_register_flag_t flags);
static int opal_hwloc_base_open(mca_base_open_flag_t flags);
static int opal_hwloc_base_close(void);

MCA_BASE_FRAMEWORK_DECLARE(opal, hwloc, NULL, opal_hwloc_base_register, opal_hwloc_base_open, opal_hwloc_base_close,
                           mca_hwloc_base_static_components, 0);

static char *opal_hwloc_base_binding_policy = NULL;
static bool opal_hwloc_base_bind_to_core = false;
static bool opal_hwloc_base_bind_to_socket = false;

static int opal_hwloc_base_register(mca_base_register_flag_t flags)
{
    mca_base_var_enum_t *new_enum;
    int ret, varid;

    /* hwloc_base_mbind_policy */

    opal_hwloc_base_map = OPAL_HWLOC_BASE_MAP_NONE;
    mca_base_var_enum_create("hwloc memory allocation policy", hwloc_base_map, &new_enum);
    ret = mca_base_var_register("opal", "hwloc", "base", "mem_alloc_policy",
                                "General memory allocations placement policy (this is not memory binding). "
                                "\"none\" means that no memory policy is applied. \"local_only\" means that a process' memory allocations will be restricted to its local NUMA node. "
                                "If using direct launch, this policy will not be in effect until after MPI_INIT. "
                                "Note that operating system paging policies are unaffected by this setting. For example, if \"local_only\" is used and local NUMA node memory is exhausted, a new memory allocation may cause paging.",
                                MCA_BASE_VAR_TYPE_INT, new_enum, 0, 0, OPAL_INFO_LVL_9,
                                MCA_BASE_VAR_SCOPE_READONLY, &opal_hwloc_base_map);
    OBJ_RELEASE(new_enum);
    if (0 > ret) {
        return ret;
    }

    /* hwloc_base_bind_failure_action */
    opal_hwloc_base_mbfa = OPAL_HWLOC_BASE_MBFA_WARN;
    mca_base_var_enum_create("hwloc memory bind failure action", hwloc_failure_action, &new_enum);
    ret = mca_base_var_register("opal", "hwloc", "base", "mem_bind_failure_action",
                                "What Open MPI will do if it explicitly tries to bind memory to a specific NUMA location, and fails.  Note that this is a different case than the general allocation policy described by hwloc_base_alloc_policy.  A value of \"silent\" means that Open MPI will proceed without comment. A value of \"warn\" means that Open MPI will warn the first time this happens, but allow the job to continue (possibly with degraded performance).  A value of \"error\" means that Open MPI will abort the job if this happens.",
                                MCA_BASE_VAR_TYPE_INT, new_enum, 0, 0, OPAL_INFO_LVL_9,
                                MCA_BASE_VAR_SCOPE_READONLY, &opal_hwloc_base_mbfa);
    OBJ_RELEASE(new_enum);
    if (0 > ret) {
        return ret;
    }

    opal_hwloc_base_binding_policy = NULL;
    (void) mca_base_var_register("opal", "hwloc", "base", "binding_policy",
                                 "Policy for binding processes. Allowed values: none, hwthread, core, l1cache, l2cache, "
                                 "l3cache, socket, numa, board, cpu-list (\"none\" is the default when oversubscribed, \"core\" is "
                                 "the default when np<=2, and \"numa\" is the default when np>2). Allowed qualifiers: "
                                 "overload-allowed, if-supported, ordered",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &opal_hwloc_base_binding_policy);

    /* backward compatibility */
    opal_hwloc_base_bind_to_core = false;
    (void) mca_base_var_register("opal", "hwloc", "base", "bind_to_core", "Bind processes to cores",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &opal_hwloc_base_bind_to_core);

    opal_hwloc_base_bind_to_socket = false;
    (void) mca_base_var_register("opal", "hwloc", "base", "bind_to_socket", "Bind processes to sockets",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &opal_hwloc_base_bind_to_socket);

    opal_hwloc_report_bindings = false;
    (void) mca_base_var_register("opal", "hwloc", "base", "report_bindings", "Report bindings to stderr",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &opal_hwloc_report_bindings);

    opal_hwloc_base_cpu_list = NULL;
    varid = mca_base_var_register("opal", "hwloc", "base", "cpu_list",
                                  "Comma-separated list of ranges specifying logical cpus to be used by these processes [default: none]",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_9,
                                  MCA_BASE_VAR_SCOPE_READONLY, &opal_hwloc_base_cpu_list);
    mca_base_var_register_synonym (varid, "opal", "hwloc", "base", "slot_list", MCA_BASE_VAR_SYN_FLAG_DEPRECATED);
    mca_base_var_register_synonym (varid, "opal", "hwloc", "base", "cpu_set", MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    /* declare hwthreads as independent cpus */
    opal_hwloc_use_hwthreads_as_cpus = false;
    (void) mca_base_var_register("opal", "hwloc", "base", "use_hwthreads_as_cpus",
                                 "Use hardware threads as independent cpus",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &opal_hwloc_use_hwthreads_as_cpus);

    opal_hwloc_base_topo_file = NULL;
    (void) mca_base_var_register("opal", "hwloc", "base", "topo_file",
                                 "Read local topology from file instead of directly sensing it",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &opal_hwloc_base_topo_file);

    /* register parameters */
    return OPAL_SUCCESS;
}

static int opal_hwloc_base_open(mca_base_open_flag_t flags)
{
    int rc;
    opal_data_type_t tmp;

    if (opal_hwloc_base_inited) {
        return OPAL_SUCCESS;
    }
    opal_hwloc_base_inited = true;

    if (OPAL_SUCCESS != (rc = opal_hwloc_base_set_binding_policy(&opal_hwloc_binding_policy,
                                                                 opal_hwloc_base_binding_policy))) {
        return rc;
    }

    if (opal_hwloc_base_bind_to_core) {
        opal_show_help("help-opal-hwloc-base.txt", "deprecated", true,
                       "--bind-to-core", "--bind-to core",
                       "hwloc_base_bind_to_core", "hwloc_base_binding_policy=core");
        /* set binding policy to core - error if something else already set */
        if (OPAL_BINDING_POLICY_IS_SET(opal_hwloc_binding_policy) &&
            OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy) != OPAL_BIND_TO_CORE) {
            /* error - cannot redefine the default ranking policy */
            opal_show_help("help-opal-hwloc-base.txt", "redefining-policy", true,
                           "core", opal_hwloc_base_print_binding(opal_hwloc_binding_policy));
            return OPAL_ERR_BAD_PARAM;
        }
        OPAL_SET_BINDING_POLICY(opal_hwloc_binding_policy, OPAL_BIND_TO_CORE);
    }

    if (opal_hwloc_base_bind_to_socket) {
        opal_show_help("help-opal-hwloc-base.txt", "deprecated", true,
                       "--bind-to-socket", "--bind-to socket",
                       "hwloc_base_bind_to_socket", "hwloc_base_binding_policy=socket");
        /* set binding policy to socket - error if something else already set */
        if (OPAL_BINDING_POLICY_IS_SET(opal_hwloc_binding_policy) &&
            OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy) != OPAL_BIND_TO_SOCKET) {
            /* error - cannot redefine the default ranking policy */
            opal_show_help("help-opal-hwloc-base.txt", "redefining-policy", true,
                           "socket", opal_hwloc_base_print_binding(opal_hwloc_binding_policy));
            return OPAL_ERR_SILENT;
        }
        OPAL_SET_BINDING_POLICY(opal_hwloc_binding_policy, OPAL_BIND_TO_SOCKET);
    }

    /* did the user provide a slot list? */
    if (NULL != opal_hwloc_base_cpu_list) {
        /* it is okay if a binding policy was already given - just ensure that
         * we do bind to the given cpus if provided, otherwise this would be
         * ignored if someone didn't also specify a binding policy
         */
        OPAL_SET_BINDING_POLICY(opal_hwloc_binding_policy, OPAL_BIND_TO_CPUSET);
    }

    /* if we are binding to hwthreads, then we must use hwthreads as cpus */
    if (OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy) == OPAL_BIND_TO_HWTHREAD) {
        opal_hwloc_use_hwthreads_as_cpus = true;
    }

    /* to support tools such as ompi_info, add the components
     * to a list
     */
    if (OPAL_SUCCESS !=
        mca_base_framework_components_open(&opal_hwloc_base_framework, flags)) {
        return OPAL_ERROR;
    }

    /* declare the hwloc data types */
    tmp = OPAL_HWLOC_TOPO;
    if (OPAL_SUCCESS != (rc = opal_dss.register_type(opal_hwloc_pack,
                                                     opal_hwloc_unpack,
                                                     (opal_dss_copy_fn_t)opal_hwloc_copy,
                                                     (opal_dss_compare_fn_t)opal_hwloc_compare,
                                                     (opal_dss_print_fn_t)opal_hwloc_print,
                                                     OPAL_DSS_STRUCTURED,
                                                     "OPAL_HWLOC_TOPO", &tmp))) {
        return rc;
    }

    return OPAL_SUCCESS;
}

static int opal_hwloc_base_close(void)
{
    int ret;
    if (!opal_hwloc_base_inited) {
        return OPAL_SUCCESS;
    }

    /* no need to close the component as it was statically opened */

    /* for support of tools such as ompi_info */
    ret = mca_base_framework_components_close (&opal_hwloc_base_framework, NULL);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    /* free memory */
    if (NULL != opal_hwloc_my_cpuset) {
        hwloc_bitmap_free(opal_hwloc_my_cpuset);
        opal_hwloc_my_cpuset = NULL;
    }

    /* destroy the topology */
    if (NULL != opal_hwloc_topology) {
        opal_hwloc_base_free_topology(opal_hwloc_topology);
        opal_hwloc_topology = NULL;
    }


    /* All done */
    opal_hwloc_base_inited = false;
    return OPAL_SUCCESS;
}

static bool fns_init=false;
static opal_tsd_key_t print_tsd_key;
char* opal_hwloc_print_null = "NULL";

static void buffer_cleanup(void *value)
{
    int i;
    opal_hwloc_print_buffers_t *ptr;

    if (NULL != value) {
        ptr = (opal_hwloc_print_buffers_t*)value;
        for (i=0; i < OPAL_HWLOC_PRINT_NUM_BUFS; i++) {
            free(ptr->buffers[i]);
        }
        free(ptr);
    }
}

opal_hwloc_print_buffers_t *opal_hwloc_get_print_buffer(void)
{
    opal_hwloc_print_buffers_t *ptr;
    int ret, i;

    if (!fns_init) {
        /* setup the print_args function */
        if (OPAL_SUCCESS != (ret = opal_tsd_key_create(&print_tsd_key, buffer_cleanup))) {
            return NULL;
        }
        fns_init = true;
    }

    ret = opal_tsd_getspecific(print_tsd_key, (void**)&ptr);
    if (OPAL_SUCCESS != ret) return NULL;

    if (NULL == ptr) {
        ptr = (opal_hwloc_print_buffers_t*)malloc(sizeof(opal_hwloc_print_buffers_t));
        for (i=0; i < OPAL_HWLOC_PRINT_NUM_BUFS; i++) {
            ptr->buffers[i] = (char *) malloc((OPAL_HWLOC_PRINT_MAX_SIZE+1) * sizeof(char));
        }
        ptr->cntr = 0;
        ret = opal_tsd_setspecific(print_tsd_key, (void*)ptr);
    }

    return (opal_hwloc_print_buffers_t*) ptr;
}

char* opal_hwloc_base_print_locality(opal_hwloc_locality_t locality)
{
    opal_hwloc_print_buffers_t *ptr;
    int idx;

    ptr = opal_hwloc_get_print_buffer();
    if (NULL == ptr) {
        return opal_hwloc_print_null;
    }
    /* cycle around the ring */
    if (OPAL_HWLOC_PRINT_NUM_BUFS == ptr->cntr) {
        ptr->cntr = 0;
    }

    idx = 0;

    if (OPAL_PROC_ON_LOCAL_CLUSTER(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'C';
        ptr->buffers[ptr->cntr][idx++] = 'L';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_CU(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'C';
        ptr->buffers[ptr->cntr][idx++] = 'U';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_NODE(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'N';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_BOARD(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'B';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_NUMA(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'N';
        ptr->buffers[ptr->cntr][idx++] = 'u';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_SOCKET(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'S';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_L3CACHE(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'L';
        ptr->buffers[ptr->cntr][idx++] = '3';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_L2CACHE(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'L';
        ptr->buffers[ptr->cntr][idx++] = '2';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_L1CACHE(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'L';
        ptr->buffers[ptr->cntr][idx++] = '1';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_CORE(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'C';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (OPAL_PROC_ON_LOCAL_HWTHREAD(locality)) {
        ptr->buffers[ptr->cntr][idx++] = 'H';
        ptr->buffers[ptr->cntr][idx++] = 'w';
        ptr->buffers[ptr->cntr][idx++] = 't';
        ptr->buffers[ptr->cntr][idx++] = ':';
    }
    if (0 < idx) {
        ptr->buffers[ptr->cntr][idx-1] = '\0';
    } else if (OPAL_PROC_NON_LOCAL & locality) {
        ptr->buffers[ptr->cntr][idx++] = 'N';
        ptr->buffers[ptr->cntr][idx++] = 'O';
        ptr->buffers[ptr->cntr][idx++] = 'N';
        ptr->buffers[ptr->cntr][idx++] = '\0';
    } else {
        /* must be an unknown locality */
        ptr->buffers[ptr->cntr][idx++] = 'U';
        ptr->buffers[ptr->cntr][idx++] = 'N';
        ptr->buffers[ptr->cntr][idx++] = 'K';
        ptr->buffers[ptr->cntr][idx++] = '\0';
    }

    return ptr->buffers[ptr->cntr];
}

static void obj_data_const(opal_hwloc_obj_data_t *ptr)
{
    ptr->npus_calculated = false;
    ptr->npus = 0;
    ptr->idx = UINT_MAX;
    ptr->num_bound = 0;
}
OBJ_CLASS_INSTANCE(opal_hwloc_obj_data_t,
                   opal_object_t,
                   obj_data_const, NULL);

static void sum_const(opal_hwloc_summary_t *ptr)
{
    ptr->num_objs = 0;
    ptr->rtype = 0;
    OBJ_CONSTRUCT(&ptr->sorted_by_dist_list, opal_list_t);
}
static void sum_dest(opal_hwloc_summary_t *ptr)
{
    opal_list_item_t *item;
    while (NULL != (item = opal_list_remove_first(&ptr->sorted_by_dist_list))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&ptr->sorted_by_dist_list);
}
OBJ_CLASS_INSTANCE(opal_hwloc_summary_t,
                   opal_list_item_t,
                   sum_const, sum_dest);
static void topo_data_const(opal_hwloc_topo_data_t *ptr)
{
    ptr->available = NULL;
    OBJ_CONSTRUCT(&ptr->summaries, opal_list_t);
    ptr->userdata = NULL;
}
static void topo_data_dest(opal_hwloc_topo_data_t *ptr)
{
    opal_list_item_t *item;

    if (NULL != ptr->available) {
        hwloc_bitmap_free(ptr->available);
    }
    while (NULL != (item = opal_list_remove_first(&ptr->summaries))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&ptr->summaries);
    ptr->userdata = NULL;
}
OBJ_CLASS_INSTANCE(opal_hwloc_topo_data_t,
                   opal_object_t,
                   topo_data_const,
                   topo_data_dest);

OBJ_CLASS_INSTANCE(opal_rmaps_numa_node_t,
        opal_list_item_t,
        NULL,
        NULL);

int opal_hwloc_base_set_binding_policy(opal_binding_policy_t *policy, char *spec)
{
    int i;
    opal_binding_policy_t tmp;
    char **tmpvals, **quals;

    /* set default */
    tmp = 0;

    /* binding specification */
    if (NULL == spec) {
        if (opal_hwloc_use_hwthreads_as_cpus) {
            /* default to bind-to hwthread */
            OPAL_SET_DEFAULT_BINDING_POLICY(tmp, OPAL_BIND_TO_HWTHREAD);
        } else {
            /* default to bind-to core */
            OPAL_SET_DEFAULT_BINDING_POLICY(tmp, OPAL_BIND_TO_CORE);
        }
    } else if (0 == strncasecmp(spec, "none", strlen("none"))) {
        OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_NONE);
    } else {
        tmpvals = opal_argv_split(spec, ':');
        if (1 < opal_argv_count(tmpvals) || ':' == spec[0]) {
            if (':' == spec[0]) {
                quals = opal_argv_split(&spec[1], ',');
            } else {
                quals = opal_argv_split(tmpvals[1], ',');
            }
            for (i=0; NULL != quals[i]; i++) {
                if (0 == strncasecmp(quals[i], "if-supported", strlen(quals[i]))) {
                    tmp |= OPAL_BIND_IF_SUPPORTED;
                } else if (0 == strncasecmp(quals[i], "overload-allowed", strlen(quals[i])) ||
                           0 == strncasecmp(quals[i], "oversubscribe-allowed", strlen(quals[i]))) {
                    tmp |= OPAL_BIND_ALLOW_OVERLOAD;
                } else if (0 == strncasecmp(quals[i], "ordered", strlen(quals[i]))) {
                    tmp |= OPAL_BIND_ORDERED;
                } else {
                    /* unknown option */
                    opal_output(0, "Unknown qualifier to binding policy: %s", spec);
                    opal_argv_free(quals);
                    opal_argv_free(tmpvals);
                    return OPAL_ERR_BAD_PARAM;
                }
            }
            opal_argv_free(quals);
        }
        if (NULL == tmpvals[0] || ':' == spec[0]) {
            OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_CORE);
            tmp &= ~OPAL_BIND_GIVEN;
        } else {
            if (0 == strcasecmp(tmpvals[0], "hwthread")) {
                OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_HWTHREAD);
            } else if (0 == strcasecmp(tmpvals[0], "core")) {
                OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_CORE);
            } else if (0 == strcasecmp(tmpvals[0], "l1cache")) {
                OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_L1CACHE);
            } else if (0 == strcasecmp(tmpvals[0], "l2cache")) {
                OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_L2CACHE);
            } else if (0 == strcasecmp(tmpvals[0], "l3cache")) {
                OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_L3CACHE);
            } else if (0 == strcasecmp(tmpvals[0], "socket")) {
                OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_SOCKET);
            } else if (0 == strcasecmp(tmpvals[0], "numa")) {
                OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_NUMA);
            } else if (0 == strcasecmp(tmpvals[0], "board")) {
                OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_BOARD);
            } else if (0 == strcasecmp(tmpvals[0], "cpu-list") ||
                       0 == strcasecmp(tmpvals[0], "cpulist")) {
                // Accept both "cpu-list" (which matches the
                // "--cpu-list" CLI option) and "cpulist" (because
                // people will be lazy)
                OPAL_SET_BINDING_POLICY(tmp, OPAL_BIND_TO_CPUSET);
            } else {
                opal_show_help("help-opal-hwloc-base.txt", "invalid binding_policy", true, "binding", spec);
                opal_argv_free(tmpvals);
                return OPAL_ERR_BAD_PARAM;
            }
        }
        opal_argv_free(tmpvals);
    }

    *policy = tmp;
    return OPAL_SUCCESS;
}
