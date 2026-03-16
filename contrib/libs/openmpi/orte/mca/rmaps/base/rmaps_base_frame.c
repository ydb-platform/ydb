/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>

#include "orte/mca/mca.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "orte/runtime/orte_globals.h"
#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"

#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rmaps/base/base.h"
/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "orte/mca/rmaps/base/static-components.h"

/*
 * Global variables
 */
orte_rmaps_base_t orte_rmaps_base = {{{0}}};
bool orte_rmaps_base_pernode = false;
int orte_rmaps_base_n_pernode = 0;
int orte_rmaps_base_n_persocket = 0;

/*
 * Local variables
 */
static char *rmaps_base_mapping_policy = NULL;
static char *rmaps_base_ranking_policy = NULL;
static bool rmaps_base_bycore = false;
static bool rmaps_base_byslot = false;
static bool rmaps_base_bynode = false;
static bool rmaps_base_no_schedule_local = false;
static bool rmaps_base_no_oversubscribe = false;
static bool rmaps_base_oversubscribe = false;
static bool rmaps_base_display_devel_map = false;
static bool rmaps_base_display_diffable_map = false;
static char *rmaps_base_topo_file = NULL;
static char *rmaps_dist_device = NULL;
static bool rmaps_base_inherit = false;

static int orte_rmaps_base_register(mca_base_register_flag_t flags)
{
    int var_id;

    orte_rmaps_base_pernode = false;
    var_id = mca_base_var_register("orte", "rmaps", "base", "pernode",
                                 "Launch one ppn as directed",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_rmaps_base_pernode);
    (void) mca_base_var_register_synonym(var_id, "orte", "rmaps", "ppr", "pernode", 0);

    orte_rmaps_base_n_pernode = 0;
    var_id = mca_base_var_register("orte", "rmaps", "base", "n_pernode",
                                 "Launch n procs/node", MCA_BASE_VAR_TYPE_INT,
                                 NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &orte_rmaps_base_n_pernode);
    (void) mca_base_var_register_synonym(var_id, "orte", "rmaps","ppr", "n_pernode", 0);

    orte_rmaps_base_n_persocket = 0;
    var_id = mca_base_var_register("orte", "rmaps", "base", "n_persocket",
                                 "Launch n procs/socket", MCA_BASE_VAR_TYPE_INT,
                                 NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &orte_rmaps_base_n_persocket);
    (void) mca_base_var_register_synonym(var_id, "orte", "rmaps","ppr", "n_persocket", 0);

    orte_rmaps_base.ppr = NULL;
    var_id = mca_base_var_register("orte", "rmaps", "base", "pattern",
                                 "Comma-separated list of number of processes on a given resource type [default: none]",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &orte_rmaps_base.ppr);
    (void) mca_base_var_register_synonym(var_id, "orte", "rmaps","ppr", "pattern", 0);

    /* define default mapping policy */
    rmaps_base_mapping_policy = NULL;
    var_id = mca_base_var_register("orte", "rmaps", "base", "mapping_policy",
                                   "Mapping Policy [slot | hwthread | core (default:np<=2) | l1cache | l2cache | l3cache | socket (default:np>2) | numa | board | node | seq | dist | ppr], with allowed modifiers :PE=y,SPAN,OVERSUBSCRIBE,NOOVERSUBSCRIBE",
                                   MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                   OPAL_INFO_LVL_9,
                                   MCA_BASE_VAR_SCOPE_READONLY,
                                   &rmaps_base_mapping_policy);
    (void) mca_base_var_register_synonym(var_id, "orte", "rmaps", "base", "schedule_policy",
                                         MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    /* define default ranking policy */
    rmaps_base_ranking_policy = NULL;
    (void) mca_base_var_register("orte", "rmaps", "base", "ranking_policy",
                                           "Ranking Policy [slot (default:np<=2) | hwthread | core | l1cache | l2cache | l3cache | socket (default:np>2) | numa | board | node], with modifier :SPAN or :FILL",
                                MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                OPAL_INFO_LVL_9,
                                MCA_BASE_VAR_SCOPE_READONLY,
                                &rmaps_base_ranking_policy);

    /* backward compatibility */
    rmaps_base_bycore = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "bycore",
                                 "Whether to map and rank processes round-robin by core",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_bycore);

    rmaps_base_byslot = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "byslot",
                                 "Whether to map and rank processes round-robin by slot",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_byslot);

    rmaps_base_bynode = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "bynode",
                                 "Whether to map and rank processes round-robin by node",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_bynode);

    /* #cpus/rank to use */
    orte_rmaps_base.cpus_per_rank = 0;
    var_id = mca_base_var_register("orte", "rmaps", "base", "cpus_per_proc",
                                   "Number of cpus to use for each rank [1-2**15 (default=1)]",
                                   MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                   OPAL_INFO_LVL_9,
                                   MCA_BASE_VAR_SCOPE_READONLY, &orte_rmaps_base.cpus_per_rank);
    mca_base_var_register_synonym(var_id, "orte", "rmaps", "base", "cpus_per_rank", 0);

    rmaps_dist_device = NULL;
    var_id = mca_base_var_register("orte", "rmaps", NULL, "dist_device",
                                   "If specified, map processes near to this device. Any device name that is identified by the lstopo hwloc utility as Net or OpenFabrics (for example eth0, mlx4_0, etc) or special name as auto ",
                                   MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                   OPAL_INFO_LVL_9,
                                   MCA_BASE_VAR_SCOPE_READONLY,
                                   &rmaps_dist_device);

    rmaps_base_no_schedule_local = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "no_schedule_local",
                                 "If false, allow scheduling MPI applications on the same node as mpirun (default).  If true, do not schedule any MPI applications on the same node as mpirun",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_no_schedule_local);

    /** default condition that allows oversubscription */
    rmaps_base_no_oversubscribe = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "no_oversubscribe",
                                 "If true, then do not allow oversubscription of nodes - mpirun will return an error if there aren't enough nodes to launch all processes without oversubscribing",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_no_oversubscribe);

    rmaps_base_oversubscribe = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "oversubscribe",
                                 "If true, then allow oversubscription of nodes and overloading of processing elements",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_oversubscribe);

    /* should we display the map after determining it? */
    orte_rmaps_base.display_map = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "display_map",
                                 "Whether to display the process map after it is computed",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &orte_rmaps_base.display_map);

    rmaps_base_display_devel_map = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "display_devel_map",
                                 "Whether to display a developer-detail process map after it is computed",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_display_devel_map);

    /* should we display the topology along with the map? */
    orte_display_topo_with_map = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "display_topo_with_map",
                                 "Whether to display the topology with the map",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &orte_display_topo_with_map);

    rmaps_base_display_diffable_map = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "display_diffable_map",
                                 "Whether to display a diffable process map after it is computed",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_display_diffable_map);

    rmaps_base_topo_file = NULL;
    (void) mca_base_var_register("orte", "rmaps", "base", "topology",
                                 "hwloc topology file (xml format) describing the topology of the compute nodes [default: none]",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_topo_file);

    rmaps_base_inherit = false;
    (void) mca_base_var_register("orte", "rmaps", "base", "inherit",
                                 "Whether child jobs shall inherit launch directives",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &rmaps_base_inherit);

    return ORTE_SUCCESS;
}

static int orte_rmaps_base_close(void)
{
    opal_list_item_t *item;

    /* cleanup globals */
    while (NULL != (item = opal_list_remove_first(&orte_rmaps_base.selected_modules))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&orte_rmaps_base.selected_modules);

    return mca_base_framework_components_close(&orte_rmaps_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int orte_rmaps_base_open(mca_base_open_flag_t flags)
{
    int rc;

    /* init the globals */
    OBJ_CONSTRUCT(&orte_rmaps_base.selected_modules, opal_list_t);
    orte_rmaps_base.slot_list = NULL;
    orte_rmaps_base.mapping = 0;
    orte_rmaps_base.ranking = 0;
    orte_rmaps_base.device = NULL;
    orte_rmaps_base.inherit = rmaps_base_inherit;

    /* if a topology file was given, then set our topology
     * from it. Even though our actual topology may differ,
     * mpirun only needs to see the compute node topology
     * for mapping purposes
     */
    if (NULL != rmaps_base_topo_file) {
        if (OPAL_SUCCESS != (rc = opal_hwloc_base_set_topology(rmaps_base_topo_file))) {
            orte_show_help("help-orte-rmaps-base.txt", "topo-file", true, rmaps_base_topo_file);
            return ORTE_ERR_SILENT;
        }
    }

    /* check for violations that has to be detected before we parse the mapping option */
    if (NULL != orte_rmaps_base.ppr) {
        orte_show_help("help-orte-rmaps-base.txt", "deprecated", true,
                       "--ppr, -ppr", "--map-by ppr:<pattern>",
                       "rmaps_base_pattern, rmaps_ppr_pattern",
                       "rmaps_base_mapping_policy=ppr:<pattern>");
        /* if the mapping policy is NULL, then we can proceed */
        if (NULL == rmaps_base_mapping_policy) {
            asprintf(&rmaps_base_mapping_policy, "ppr:%s", orte_rmaps_base.ppr);
        } else {
            return ORTE_ERR_SILENT;
        }
    }

    if (0 < orte_rmaps_base.cpus_per_rank) {
        orte_show_help("help-orte-rmaps-base.txt", "deprecated", true,
                       "--cpus-per-proc, -cpus-per-proc, --cpus-per-rank, -cpus-per-rank",
                       "--map-by <obj>:PE=N, default <obj>=NUMA",
                       "rmaps_base_cpus_per_proc", "rmaps_base_mapping_policy=<obj>:PE=N, default <obj>=NUMA");
    }

    if (ORTE_SUCCESS != (rc = orte_rmaps_base_set_mapping_policy(NULL, &orte_rmaps_base.mapping,
                                                                 &orte_rmaps_base.device,
                                                                 rmaps_base_mapping_policy))) {
        return rc;
    }

    if (ORTE_SUCCESS != (rc = orte_rmaps_base_set_ranking_policy(&orte_rmaps_base.ranking,
                                                                 orte_rmaps_base.mapping,
                                                                 rmaps_base_ranking_policy))) {
        return rc;
    }

    if (rmaps_base_bycore) {
        orte_show_help("help-orte-rmaps-base.txt", "deprecated", true,
                       "--bycore, -bycore", "--map-by core",
                       "rmaps_base_bycore", "rmaps_base_mapping_policy=core");
        /* set mapping policy to bycore - error if something else already set */
        if ((ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) &&
            ORTE_GET_MAPPING_POLICY(orte_rmaps_base.mapping) != ORTE_MAPPING_BYCORE) {
            /* error - cannot redefine the default mapping policy */
            orte_show_help("help-orte-rmaps-base.txt", "redefining-policy", true, "mapping",
                           "bycore", orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
            return ORTE_ERR_SILENT;
        }
        ORTE_SET_MAPPING_POLICY(orte_rmaps_base.mapping, ORTE_MAPPING_BYCORE);
        ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_GIVEN);
        /* set ranking policy to bycore - error if something else already set */
        if ((ORTE_RANKING_GIVEN & ORTE_GET_RANKING_DIRECTIVE(orte_rmaps_base.ranking)) &&
            ORTE_GET_RANKING_POLICY(orte_rmaps_base.ranking) != ORTE_RANK_BY_CORE) {
            /* error - cannot redefine the default ranking policy */
            orte_show_help("help-orte-rmaps-base.txt", "redefining-policy", true, "ranking",
                           "bycore", orte_rmaps_base_print_ranking(orte_rmaps_base.ranking));
            return ORTE_ERR_SILENT;
        }
        ORTE_SET_RANKING_POLICY(orte_rmaps_base.ranking, ORTE_RANK_BY_CORE);
        ORTE_SET_RANKING_DIRECTIVE(orte_rmaps_base.ranking, ORTE_RANKING_GIVEN);
    }

    if (rmaps_base_byslot) {
        orte_show_help("help-orte-rmaps-base.txt", "deprecated", true,
                       "--byslot, -byslot", "--map-by slot",
                       "rmaps_base_byslot", "rmaps_base_mapping_policy=slot");
        /* set mapping policy to byslot - error if something else already set */
        if ((ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) &&
            ORTE_GET_MAPPING_POLICY(orte_rmaps_base.mapping) != ORTE_MAPPING_BYSLOT) {
            /* error - cannot redefine the default mapping policy */
            orte_show_help("help-orte-rmaps-base.txt", "redefining-policy", true, "mapping",
                           "byslot", orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
            return ORTE_ERR_SILENT;
        }
        ORTE_SET_MAPPING_POLICY(orte_rmaps_base.mapping, ORTE_MAPPING_BYSLOT);
        ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_GIVEN);
        /* set ranking policy to byslot - error if something else already set */
        if ((ORTE_RANKING_GIVEN & ORTE_GET_RANKING_DIRECTIVE(orte_rmaps_base.ranking)) &&
            ORTE_GET_RANKING_POLICY(orte_rmaps_base.ranking) != ORTE_RANK_BY_SLOT) {
            /* error - cannot redefine the default ranking policy */
            orte_show_help("help-orte-rmaps-base.txt", "redefining-policy", true, "ranking",
                           "byslot", orte_rmaps_base_print_ranking(orte_rmaps_base.ranking));
            return ORTE_ERR_SILENT;
        }
        ORTE_SET_RANKING_POLICY(orte_rmaps_base.ranking, ORTE_RANK_BY_SLOT);
        ORTE_SET_RANKING_DIRECTIVE(orte_rmaps_base.ranking, ORTE_RANKING_GIVEN);
    }

    if (rmaps_base_bynode) {
        orte_show_help("help-orte-rmaps-base.txt", "deprecated", true,
                       "--bynode, -bynode", "--map-by node",
                       "rmaps_base_bynode", "rmaps_base_mapping_policy=node");
        /* set mapping policy to bynode - error if something else already set */
        if ((ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) &&
            ORTE_GET_MAPPING_POLICY(orte_rmaps_base.mapping) != ORTE_MAPPING_BYNODE) {
            orte_show_help("help-orte-rmaps-base.txt", "redefining-policy", true, "mapping",
                           "bynode", orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
            return ORTE_ERR_SILENT;
        }
        ORTE_SET_MAPPING_POLICY(orte_rmaps_base.mapping, ORTE_MAPPING_BYNODE);
        ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_GIVEN);
        /* set ranking policy to bynode - error if something else already set */
        if ((ORTE_RANKING_GIVEN & ORTE_GET_RANKING_DIRECTIVE(orte_rmaps_base.ranking)) &&
            ORTE_GET_RANKING_POLICY(orte_rmaps_base.ranking) != ORTE_RANK_BY_NODE) {
            /* error - cannot redefine the default ranking policy */
            orte_show_help("help-orte-rmaps-base.txt", "redefining-policy", true, "ranking",
                           "bynode", orte_rmaps_base_print_ranking(orte_rmaps_base.ranking));
            return ORTE_ERR_SILENT;
        }
        ORTE_SET_RANKING_POLICY(orte_rmaps_base.ranking, ORTE_RANK_BY_NODE);
        ORTE_SET_RANKING_DIRECTIVE(orte_rmaps_base.ranking, ORTE_RANKING_GIVEN);
    }

    if (0 < orte_rmaps_base.cpus_per_rank) {
        /* if we were asked for cpus/proc, then we have to
         * bind to those cpus - any other binding policy is an
         * error
         */
        if (OPAL_BINDING_POLICY_IS_SET(opal_hwloc_binding_policy)) {
            if (opal_hwloc_use_hwthreads_as_cpus) {
                if (OPAL_BIND_TO_HWTHREAD != OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy) &&
                    OPAL_BIND_TO_NONE != OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy)) {
                    orte_show_help("help-orte-rmaps-base.txt", "mismatch-binding", true,
                                   orte_rmaps_base.cpus_per_rank, "use-hwthreads-as-cpus",
                                   opal_hwloc_base_print_binding(opal_hwloc_binding_policy),
                                   "bind-to hwthread");
                    return ORTE_ERR_SILENT;
                }
            } else if (OPAL_BIND_TO_CORE != OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy) &&
                       OPAL_BIND_TO_NONE != OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy)) {
                orte_show_help("help-orte-rmaps-base.txt", "mismatch-binding", true,
                               orte_rmaps_base.cpus_per_rank, "cores as cpus",
                               opal_hwloc_base_print_binding(opal_hwloc_binding_policy),
                               "bind-to core");
                return ORTE_ERR_SILENT;
            }
        } else {
            if (opal_hwloc_use_hwthreads_as_cpus) {
                OPAL_SET_BINDING_POLICY(opal_hwloc_binding_policy, OPAL_BIND_TO_HWTHREAD);
            } else {
                OPAL_SET_BINDING_POLICY(opal_hwloc_binding_policy, OPAL_BIND_TO_CORE);
            }
        }
        if (1 < orte_rmaps_base.cpus_per_rank) {
            /* we need to ensure we are mapping to a high-enough level to have
             * multiple cpus beneath it - by default, we'll go to the NUMA level */
            if (ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) {
                if (ORTE_GET_MAPPING_POLICY(orte_rmaps_base.mapping) == ORTE_MAPPING_BYHWTHREAD ||
                  (ORTE_GET_MAPPING_POLICY(orte_rmaps_base.mapping) == ORTE_MAPPING_BYCORE &&
                  !opal_hwloc_use_hwthreads_as_cpus)) {
                    orte_show_help("help-orte-rmaps-base.txt", "mapping-too-low-init", true);
                    return ORTE_ERR_SILENT;
                }
            } else {
                opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "%s rmaps:base pe/rank set - setting mapping to BYNUMA",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                ORTE_SET_MAPPING_POLICY(orte_rmaps_base.mapping, ORTE_MAPPING_BYNUMA);
                ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_GIVEN);
            }
        }
    }

    if (orte_rmaps_base_pernode) {
        /* if the user didn't specify a mapping directive, then match it */
        if (!(ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping))) {
            /* ensure we set the mapping policy to ppr */
            ORTE_SET_MAPPING_POLICY(orte_rmaps_base.mapping, ORTE_MAPPING_PPR);
            ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_GIVEN);
            /* define the ppr */
            orte_rmaps_base.ppr = strdup("1:node");
        }
    }

    if (0 < orte_rmaps_base_n_pernode) {
         /* if the user didn't specify a mapping directive, then match it */
         if (!(ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping))) {
             /* ensure we set the mapping policy to ppr */
             ORTE_SET_MAPPING_POLICY(orte_rmaps_base.mapping, ORTE_MAPPING_PPR);
             ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_GIVEN);
             /* define the ppr */
             asprintf(&orte_rmaps_base.ppr, "%d:node", orte_rmaps_base_n_pernode);
         }
    }

    if (0 < orte_rmaps_base_n_persocket) {
        /* if the user didn't specify a mapping directive, then match it */
        if (!(ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping))) {
            /* ensure we set the mapping policy to ppr */
            ORTE_SET_MAPPING_POLICY(orte_rmaps_base.mapping, ORTE_MAPPING_PPR);
            ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_GIVEN);
            /* define the ppr */
            asprintf(&orte_rmaps_base.ppr, "%d:socket", orte_rmaps_base_n_persocket);
        }
    }

    /* Should we schedule on the local node or not? */
    if (rmaps_base_no_schedule_local) {
        orte_rmaps_base.mapping |= ORTE_MAPPING_NO_USE_LOCAL;
    }

    /* Should we oversubscribe or not? */
    if (rmaps_base_no_oversubscribe) {
        if ((ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) &&
            !(ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping))) {
            /* error - cannot redefine the default mapping policy */
            orte_show_help("help-orte-rmaps-base.txt", "redefining-policy", true, "mapping",
                           "no-oversubscribe", orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
            return ORTE_ERR_SILENT;
        }
        ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
        ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_SUBSCRIBE_GIVEN);
    }

    /** force oversubscription permission */
    if (rmaps_base_oversubscribe) {
        if ((ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) &&
            (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping))) {
            /* error - cannot redefine the default mapping policy */
            orte_show_help("help-orte-rmaps-base.txt", "redefining-policy", true, "mapping",
                           "oversubscribe", orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
            return ORTE_ERR_SILENT;
        }
        ORTE_UNSET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
        ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_SUBSCRIBE_GIVEN);
        /* also set the overload allowed flag */
        opal_hwloc_binding_policy |= OPAL_BIND_ALLOW_OVERLOAD;
    }

    /* should we display a detailed (developer-quality) version of the map after determining it? */
    if (rmaps_base_display_devel_map) {
        orte_rmaps_base.display_map = true;
        orte_devel_level_output = true;
    }

    /* should we display a diffable report of proc locations after determining it? */
    if (rmaps_base_display_diffable_map) {
        orte_rmaps_base.display_map = true;
        orte_display_diffable_output = true;
    }

    /* Open up all available components */
    rc = mca_base_framework_components_open(&orte_rmaps_base_framework, flags);

    /* check to see if any component indicated a problem */
    if (ORTE_MAPPING_CONFLICTED & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) {
        /* the component would have already reported the error, so
         * tell the rest of the chain to shut up
         */
        return ORTE_ERR_SILENT;
    }

    /* All done */
    return rc;
}

MCA_BASE_FRAMEWORK_DECLARE(orte, rmaps, "ORTE Mapping Subsystem",
                           orte_rmaps_base_register, orte_rmaps_base_open, orte_rmaps_base_close,
                           mca_rmaps_base_static_components, 0);

OBJ_CLASS_INSTANCE(orte_rmaps_base_selected_module_t,
                   opal_list_item_t,
                   NULL, NULL);


static int check_modifiers(char *ck, orte_mapping_policy_t *tmp)
{
    char **ck2, *ptr;
    int i;
    bool found = false;

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "%s rmaps:base check modifiers with %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (NULL == ck) ? "NULL" : ck);

    if (NULL == ck) {
        return ORTE_SUCCESS;
    }

    ck2 = opal_argv_split(ck, ',');
    for (i=0; NULL != ck2[i]; i++) {
        if (0 == strncasecmp(ck2[i], "span", strlen(ck2[i]))) {
            ORTE_SET_MAPPING_DIRECTIVE(*tmp, ORTE_MAPPING_SPAN);
            found = true;
        } else if (0 == strncasecmp(ck2[i], "pe", strlen("pe"))) {
            /* break this at the = sign to get the number */
            if (NULL == (ptr = strchr(ck2[i], '='))) {
                /* missing the value */
                orte_show_help("help-orte-rmaps-base.txt", "missing-value", true, "pe", ck2[i]);
                opal_argv_free(ck2);
                return ORTE_ERR_SILENT;
            }
            ptr++;
            orte_rmaps_base.cpus_per_rank = strtol(ptr, NULL, 10);
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "%s rmaps:base setting pe/rank to %d",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                orte_rmaps_base.cpus_per_rank);
            found = true;
        } else if (0 == strncasecmp(ck2[i], "oversubscribe", strlen(ck2[i]))) {
            ORTE_UNSET_MAPPING_DIRECTIVE(*tmp, ORTE_MAPPING_NO_OVERSUBSCRIBE);
            ORTE_SET_MAPPING_DIRECTIVE(*tmp, ORTE_MAPPING_SUBSCRIBE_GIVEN);
            found = true;
        } else if (0 == strncasecmp(ck2[i], "nooversubscribe", strlen(ck2[i]))) {
            ORTE_SET_MAPPING_DIRECTIVE(*tmp, ORTE_MAPPING_NO_OVERSUBSCRIBE);
            ORTE_SET_MAPPING_DIRECTIVE(*tmp, ORTE_MAPPING_SUBSCRIBE_GIVEN);
            found = true;
        } else {
            /* unrecognized modifier */
            opal_argv_free(ck2);
            return ORTE_ERR_BAD_PARAM;
        }
    }
    opal_argv_free(ck2);
    if (found) {
        return ORTE_SUCCESS;
    }
    return ORTE_ERR_TAKE_NEXT_OPTION;
}

int orte_rmaps_base_set_mapping_policy(orte_job_t *jdata,
                                       orte_mapping_policy_t *policy,
                                       char **device, char *inspec)
{
    char *ck;
    char *ptr, *cptr;
    orte_mapping_policy_t tmp;
    int rc;
    size_t len;
    char *spec;
    char *pch;

    /* set defaults */
    tmp = 0;
    if (NULL != device) {
        *device = NULL;
    }

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "%s rmaps:base set policy with %s device %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (NULL == inspec) ? "NULL" : inspec,
                        (NULL == device) ? "NULL" : "NONNULL");

    if (NULL == inspec) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYSOCKET);
        goto setpolicy;
    }

    spec = strdup(inspec);  // protect the input string
    /* see if a colon was included - if so, then we have a policy + modifier */
    ck = strchr(spec, ':');
    if (NULL != ck) {
        /* if the colon is the first character of the string, then we
         * just have modifiers on the default mapping policy */
        if (ck == spec) {
            ck++;  // step over the colon
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "%s rmaps:base only modifiers %s provided - assuming bysocket mapping",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ck);
            ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYSOCKET);
            if (ORTE_ERR_SILENT == (rc = check_modifiers(ck, &tmp)) &&
                ORTE_ERR_BAD_PARAM != rc) {
                free(spec);
                return ORTE_ERR_SILENT;
            }
            free(spec);
            goto setpolicy;
        }
        *ck = '\0';  // terminate spec where the colon was
        ck++;    // step past the colon
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "%s rmaps:base policy %s modifiers %s provided",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), spec, ck);

        if (0 == strncasecmp(spec, "ppr", strlen(spec))) {
            /* at this point, ck points to a string that contains at least
             * two fields (specifying the #procs/obj and the object we are
             * to map by). we have to allow additional modifiers here - e.g.,
             * specifying #pe's/proc or oversubscribe - so check for modifiers. if
             * they are present, ck will look like "N:obj:mod1,mod2,mod3"
             */
            if (NULL == (ptr = strchr(ck, ':'))) {
                /* this is an error - there had to be at least one
                 * colon to delimit the number from the object type
                 */
                orte_show_help("help-orte-rmaps-base.txt", "invalid-pattern", true, inspec);
                free(spec);
                return ORTE_ERR_SILENT;
            }
            ptr++; // move past the colon
            /* at this point, ptr is pointing to the beginning of the string that describes
             * the object plus any modifiers (i.e., "obj:mod1,mod2". We first check to see if there
             * is another colon indicating that there are modifiers to the request */
            if (NULL != (cptr = strchr(ptr, ':'))) {
                /* there are modifiers, so we terminate the object string
                 * at the location of the colon */
                *cptr = '\0';
                /* step over that colon */
                cptr++;
                /* now check for modifiers  - may be none, so
                 * don't emit an error message if the modifier
                 * isn't recognized */
                if (ORTE_ERR_SILENT == (rc = check_modifiers(cptr, &tmp)) &&
                    ORTE_ERR_BAD_PARAM != rc) {
                    free(spec);
                    return ORTE_ERR_SILENT;
                }
            }
            /* now save the pattern */
            if (NULL == jdata || NULL == jdata->map) {
                orte_rmaps_base.ppr = strdup(ck);
            } else {
                jdata->map->ppr = strdup(ck);
            }
            ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_PPR);
            ORTE_SET_MAPPING_DIRECTIVE(tmp, ORTE_MAPPING_GIVEN);
            free(spec);
            goto setpolicy;
        }
        if (ORTE_SUCCESS != (rc = check_modifiers(ck, &tmp)) &&
            ORTE_ERR_TAKE_NEXT_OPTION != rc) {
            if (ORTE_ERR_BAD_PARAM == rc) {
                orte_show_help("help-orte-rmaps-base.txt", "unrecognized-modifier", true, inspec);
            }
            free(spec);
            return rc;
        }
    }
    len = strlen(spec);
    if (0 == strncasecmp(spec, "slot", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYSLOT);
    } else if (0 == strncasecmp(spec, "node", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYNODE);
    } else if (0 == strncasecmp(spec, "seq", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_SEQ);
    } else if (0 == strncasecmp(spec, "core", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYCORE);
    } else if (0 == strncasecmp(spec, "l1cache", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYL1CACHE);
    } else if (0 == strncasecmp(spec, "l2cache", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYL2CACHE);
    } else if (0 == strncasecmp(spec, "l3cache", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYL3CACHE);
    } else if (0 == strncasecmp(spec, "socket", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYSOCKET);
    } else if (0 == strncasecmp(spec, "numa", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYNUMA);
    } else if (0 == strncasecmp(spec, "board", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYBOARD);
    } else if (0 == strncasecmp(spec, "hwthread", len)) {
        ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYHWTHREAD);
        /* if we are mapping processes to individual hwthreads, then
         * we need to treat those hwthreads as separate cpus
         */
        opal_hwloc_use_hwthreads_as_cpus = true;
    } else if (0 == strncasecmp(spec, "dist", len)) {
        if (NULL != rmaps_dist_device) {
            if (NULL != (pch = strchr(rmaps_dist_device, ':'))) {
                *pch = '\0';
            }
            if (NULL != device) {
                *device = strdup(rmaps_dist_device);
            }
            ORTE_SET_MAPPING_POLICY(tmp, ORTE_MAPPING_BYDIST);
        } else {
            orte_show_help("help-orte-rmaps-base.txt", "device-not-specified", true);
            free(spec);
            return ORTE_ERR_SILENT;
        }
    } else {
        orte_show_help("help-orte-rmaps-base.txt", "unrecognized-policy", true, "mapping", spec);
        free(spec);
        return ORTE_ERR_SILENT;
    }
    free(spec);
    ORTE_SET_MAPPING_DIRECTIVE(tmp, ORTE_MAPPING_GIVEN);

 setpolicy:
    if (NULL == jdata || NULL == jdata->map) {
        *policy = tmp;
    } else {
        jdata->map->mapping = tmp;
    }

    return ORTE_SUCCESS;
}

int orte_rmaps_base_set_ranking_policy(orte_ranking_policy_t *policy,
                                       orte_mapping_policy_t mapping,
                                       char *spec)
{
    orte_mapping_policy_t map;
    orte_ranking_policy_t tmp;
    char **ck;
    size_t len;

    /* set default */
    tmp = 0;

    if (NULL == spec) {
        /* check for map-by object directives - we set the
         * ranking to match if one was given
         */
        if (ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(mapping)) {
            map = ORTE_GET_MAPPING_POLICY(mapping);
            switch (map) {
            case ORTE_MAPPING_BYSLOT:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_SLOT);
                break;
            case ORTE_MAPPING_BYNODE:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_NODE);
                break;
            case ORTE_MAPPING_BYCORE:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_CORE);
                break;
            case ORTE_MAPPING_BYL1CACHE:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_L1CACHE);
                break;
            case ORTE_MAPPING_BYL2CACHE:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_L2CACHE);
                break;
            case ORTE_MAPPING_BYL3CACHE:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_L3CACHE);
                break;
            case ORTE_MAPPING_BYSOCKET:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_SOCKET);
                break;
            case ORTE_MAPPING_BYNUMA:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_NUMA);
                break;
            case ORTE_MAPPING_BYBOARD:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_BOARD);
                break;
            case ORTE_MAPPING_BYHWTHREAD:
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_HWTHREAD);
                break;
            default:
                /* anything not tied to a specific hw obj can rank by slot */
                ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_SLOT);
                break;
            }
        } else {
            /* if no map-by was given, default to by-slot */
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_SLOT);
        }
    } else {
        ck = opal_argv_split(spec, ':');
        if (2 < opal_argv_count(ck)) {
            /* incorrect format */
            orte_show_help("help-orte-rmaps-base.txt", "unrecognized-policy", true, "ranking", policy);
            opal_argv_free(ck);
            return ORTE_ERR_SILENT;
        }
        if (2 == opal_argv_count(ck)) {
            if (0 == strncasecmp(ck[1], "span", strlen(ck[1]))) {
                ORTE_SET_RANKING_DIRECTIVE(tmp, ORTE_RANKING_SPAN);
            } else if (0 == strncasecmp(ck[1], "fill", strlen(ck[1]))) {
                ORTE_SET_RANKING_DIRECTIVE(tmp, ORTE_RANKING_FILL);
            } else {
                /* unrecognized modifier */
                orte_show_help("help-orte-rmaps-base.txt", "unrecognized-modifier", true, ck[1]);
                opal_argv_free(ck);
                return ORTE_ERR_SILENT;
            }
        }
        len = strlen(ck[0]);
        if (0 == strncasecmp(ck[0], "slot", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_SLOT);
        } else if (0 == strncasecmp(ck[0], "node", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_NODE);
        } else if (0 == strncasecmp(ck[0], "hwthread", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_HWTHREAD);
        } else if (0 == strncasecmp(ck[0], "core", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_CORE);
        } else if (0 == strncasecmp(ck[0], "l1cache", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_L1CACHE);
        } else if (0 == strncasecmp(ck[0], "l2cache", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_L2CACHE);
        } else if (0 == strncasecmp(ck[0], "l3cache", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_L3CACHE);
        } else if (0 == strncasecmp(ck[0], "socket", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_SOCKET);
        } else if (0 == strncasecmp(ck[0], "numa", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_NUMA);
        } else if (0 == strncasecmp(ck[0], "board", len)) {
            ORTE_SET_RANKING_POLICY(tmp, ORTE_RANK_BY_BOARD);
        } else {
            orte_show_help("help-orte-rmaps-base.txt", "unrecognized-policy", true, "ranking", rmaps_base_ranking_policy);
            opal_argv_free(ck);
            return ORTE_ERR_SILENT;
        }
        opal_argv_free(ck);
        ORTE_SET_RANKING_DIRECTIVE(tmp, ORTE_RANKING_GIVEN);
    }

    *policy = tmp;
    return ORTE_SUCCESS;
}
