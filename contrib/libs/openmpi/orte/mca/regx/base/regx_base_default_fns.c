/*
 * Copyright (c) 2016-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"
#include "orte/types.h"
#include "opal/types.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <ctype.h>

#include "opal/util/argv.h"
#include "opal/util/basename.h"
#include "opal/util/opal_environ.h"

#include "orte/runtime/orte_globals.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/regx/base/base.h"

static void range_construct(orte_regex_range_t *ptr)
{
    ptr->vpid = 0;
    ptr->cnt = 0;
}
OBJ_CLASS_INSTANCE(orte_regex_range_t,
                   opal_list_item_t,
                   range_construct, NULL);

static void orte_regex_node_construct(orte_regex_node_t *ptr)
{
    ptr->prefix = NULL;
    ptr->suffix = NULL;
    ptr->num_digits = 0;
    OBJ_CONSTRUCT(&ptr->ranges, opal_list_t);
}

static void orte_regex_node_destruct(orte_regex_node_t *ptr)
{
    opal_list_item_t *item;

    if (NULL != ptr->prefix) {
        free(ptr->prefix);
    }
    if (NULL != ptr->suffix) {
        free(ptr->suffix);
    }

    while (NULL != (item = opal_list_remove_first(&ptr->ranges))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&ptr->ranges);
}

OBJ_CLASS_INSTANCE(orte_regex_node_t,
                   opal_list_item_t,
                   orte_regex_node_construct,
                   orte_regex_node_destruct);

int orte_regx_base_nidmap_parse(char *regex)
{
    char *nodelist, *vpids, *ptr;
    char **nodes, **dvpids;
    int rc, n, cnt;
    orte_regex_range_t *rng;
    opal_list_t dids;
    orte_job_t *daemons;
    orte_node_t *nd;
    orte_proc_t *proc;

    /* if we are the HNP, we don't need to parse this */
    if (ORTE_PROC_IS_HNP) {
        return ORTE_SUCCESS;
    }

    /* split the regex into its node and vpid parts */
    nodelist = regex;
    vpids = strchr(regex, '@');
    if (NULL == vpids) {
        /* indicates the regex got mangled somewhere */
        return ORTE_ERR_BAD_PARAM;
    }
    *vpids = '\0';  // terminate the nodelist string
    ++vpids;  // step over the separator
    if (NULL == vpids || '\0' == *vpids) {
        /* indicates the regex got mangled somewhere */
        return ORTE_ERR_BAD_PARAM;
    }

    /* decompress the nodes regex */
    nodes = NULL;
    if (ORTE_SUCCESS != (rc = orte_regx.extract_node_names(nodelist, &nodes))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    if (NULL == nodes) {
        /* should not happen */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }

    /* decompress the vpids */
    OBJ_CONSTRUCT(&dids, opal_list_t);
    dvpids = opal_argv_split(vpids, ',');
    for (n=0; NULL != dvpids[n]; n++) {
        rng = OBJ_NEW(orte_regex_range_t);
        opal_list_append(&dids, &rng->super);
        /* check for a count */
        if (NULL != (ptr = strchr(dvpids[n], '('))) {
            dvpids[n][strlen(dvpids[n])-1] = '\0';  // remove trailing paren
            *ptr = '\0';
            ++ptr;
            rng->cnt = strtoul(ptr, NULL, 10);
        } else {
            rng->cnt = 1;
        }
        /* convert the number */
        rng->vpid = strtoul(dvpids[n], NULL, 10);
    }
    opal_argv_free(dvpids);

    /* get the daemon job object */
    daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

    /* create the node pool array - this will include
     * _all_ nodes known to the allocation */
    rng = (orte_regex_range_t*)opal_list_get_first(&dids);
    cnt = 0;
    for (n=0; NULL != nodes[n]; n++) {
        nd = OBJ_NEW(orte_node_t);
        nd->name = nodes[n];
        opal_pointer_array_set_item(orte_node_pool, n, nd);
        /* see if it has a daemon on it */
        if (-1 != rng->vpid) {
            /* we have a daemon, so let's create the tracker for it */
            if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(daemons->procs, rng->vpid+cnt))) {
                proc = OBJ_NEW(orte_proc_t);
                proc->name.jobid = ORTE_PROC_MY_NAME->jobid;
                proc->name.vpid = rng->vpid + cnt;
                proc->state = ORTE_PROC_STATE_RUNNING;
                ORTE_FLAG_SET(proc, ORTE_PROC_FLAG_ALIVE);
                daemons->num_procs++;
                opal_pointer_array_set_item(daemons->procs, proc->name.vpid, proc);
            }
            nd->index = proc->name.vpid;
            OBJ_RETAIN(nd);
            proc->node = nd;
            OBJ_RETAIN(proc);
            nd->daemon = proc;
        }
        ++cnt;
        if (rng->cnt <= cnt) {
            rng = (orte_regex_range_t*)opal_list_get_next(&rng->super);
            if (NULL == rng) {
                ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                return ORTE_ERR_NOT_FOUND;
            }
            cnt = 0;
        }
    }

    /* update num procs */
    if (orte_process_info.num_procs != daemons->num_procs) {
        orte_process_info.num_procs = daemons->num_procs;
        /* need to update the routing plan */
        orte_routed.update_routing_plan(NULL);
    }

    if (orte_process_info.max_procs < orte_process_info.num_procs) {
        orte_process_info.max_procs = orte_process_info.num_procs;
    }

    if (0 < opal_output_get_verbosity(orte_regx_base_framework.framework_output)) {
        int i;
        for (i=0; i < orte_node_pool->size; i++) {
            if (NULL == (nd = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                continue;
            }
            opal_output(0, "%s node[%d].name %s daemon %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i,
                        (NULL == nd->name) ? "NULL" : nd->name,
                        (NULL == nd->daemon) ? "NONE" : ORTE_VPID_PRINT(nd->daemon->name.vpid));
        }
    }

    return ORTE_SUCCESS;
}

int orte_regx_base_encode_nodemap(opal_buffer_t *buffer)
{
    int n;
    bool test;
    orte_regex_range_t *rng, *slt, *tp, *flg;
    opal_list_t slots, topos, flags;
    opal_list_item_t *item;
    char *tmp, *tmp2;
    orte_node_t *nptr;
    int rc;
    uint8_t ui8;
    orte_topology_t *ortetopo;

    /* setup the list of results */
    OBJ_CONSTRUCT(&slots, opal_list_t);
    OBJ_CONSTRUCT(&topos, opal_list_t);
    OBJ_CONSTRUCT(&flags, opal_list_t);

    slt = NULL;
    tp = NULL;
    flg = NULL;

    /* pack a flag indicating if the HNP was included in the allocation */
    if (orte_hnp_is_allocated) {
        ui8 = 1;
    } else {
        ui8 = 0;
    }
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &ui8, 1, OPAL_UINT8))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    /* pack a flag indicating if we are in a managed allocation */
    if (orte_managed_allocation) {
        ui8 = 1;
    } else {
        ui8 = 0;
    }
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &ui8, 1, OPAL_UINT8))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    /* handle the topologies - as the most common case by far
     * is to have homogeneous topologies, we only send them
     * if something is different. We know that the HNP is
     * the first topology, and that any differing topology
     * on the compute nodes must follow. So send the topologies
     * if and only if:
     *
     * (a) the HNP is being used to house application procs and
     *     there is more than one topology on our list; or
     *
     * (b) the HNP is not being used, but there are more than
     *     two topologies on our list, thus indicating that
     *     there are multiple topologies on the compute nodes
     */
    nptr = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0);
    if (!orte_hnp_is_allocated || (ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping) & ORTE_MAPPING_NO_USE_LOCAL)) {
        /* assign a NULL topology so we still account for our presence,
         * but don't cause us to send topology info when not needed */
        tp = OBJ_NEW(orte_regex_range_t);
        tp->t = NULL;
        tp->cnt = 1;
    } else {
        /* there is always one topology - our own - so start with it */
        tp = OBJ_NEW(orte_regex_range_t);
        tp->t = nptr->topology;
        tp->cnt = 1;
    }
    opal_list_append(&topos, &tp->super);

    opal_output_verbose(5, orte_regx_base_framework.framework_output,
                        "%s STARTING WITH TOPOLOGY FOR NODE %s: %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        nptr->name, (NULL == tp->t) ? "NULL" : tp->t->sig);

    /* likewise, we have slots */
    slt = OBJ_NEW(orte_regex_range_t);
    slt->slots = nptr->slots;
    slt->cnt = 1;
    opal_list_append(&slots, &slt->super);

    /* and flags */
    flg = OBJ_NEW(orte_regex_range_t);
    if (ORTE_FLAG_TEST(nptr, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
        flg->slots = 1;
    } else {
        flg->slots = 0;
    }
    flg->cnt = 1;
    opal_list_append(&flags, &flg->super);

    for (n=1; n < orte_node_pool->size; n++) {
        if (NULL == (nptr = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, n))) {
            continue;
        }
        /* check the #slots */
        /* is this the next in line */
        if (nptr->slots == slt->slots) {
            slt->cnt++;
        } else {
            /* need to start another range */
            slt = OBJ_NEW(orte_regex_range_t);
            slt->slots = nptr->slots;
            slt->cnt = 1;
            opal_list_append(&slots, &slt->super);
        }
        /* check the topologies */
        if (NULL != tp->t && NULL == nptr->topology) {
            /* we don't know this topology, likely because
             * we don't have a daemon on the node */
            tp = OBJ_NEW(orte_regex_range_t);
            tp->t = NULL;
            tp->cnt = 1;
            opal_output_verbose(5, orte_regx_base_framework.framework_output,
                                "%s ADD TOPOLOGY FOR NODE %s: NULL",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), nptr->name);
            opal_list_append(&topos, &tp->super);
        } else {
            /* is this the next in line */
            if (tp->t == nptr->topology) {
                tp->cnt++;
                opal_output_verbose(5, orte_regx_base_framework.framework_output,
                                    "%s CONTINUE TOPOLOGY RANGE (%d) WITH NODE %s: %s",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    tp->cnt, nptr->name,
                                    (NULL == tp->t) ? "N/A" : tp->t->sig);
            } else {
                /* need to start another range */
                tp = OBJ_NEW(orte_regex_range_t);
                tp->t = nptr->topology;
                tp->cnt = 1;
                opal_output_verbose(5, orte_regx_base_framework.framework_output,
                                    "%s STARTING NEW TOPOLOGY RANGE WITH NODE %s: %s",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    nptr->name, tp->t->sig);
                opal_list_append(&topos, &tp->super);
            }
        }
        /* check the flags */
        test = ORTE_FLAG_TEST(nptr, ORTE_NODE_FLAG_SLOTS_GIVEN);
        /* is this the next in line */
         if ((test && 1 == flg->slots) ||
             (!test && 0 == flg->slots)) {
            flg->cnt++;
        } else {
            /* need to start another range */
            flg = OBJ_NEW(orte_regex_range_t);
            if (test) {
                flg->slots = 1;
            } else {
                flg->slots = 0;
            }
            flg->cnt = 1;
            opal_list_append(&flags, &flg->super);
        }
    }

    /* pass #slots on each node */
    tmp = NULL;
    while (NULL != (item = opal_list_remove_first(&slots))) {
        rng = (orte_regex_range_t*)item;
        if (NULL == tmp) {
            asprintf(&tmp, "%d[%d]", rng->cnt, rng->slots);
        } else {
            asprintf(&tmp2, "%s,%d[%d]", tmp, rng->cnt, rng->slots);
            free(tmp);
            tmp = tmp2;
        }
        OBJ_RELEASE(rng);
    }
    OPAL_LIST_DESTRUCT(&slots);
    opal_output_verbose(1, orte_regx_base_framework.framework_output,
                        "%s SLOT ASSIGNMENTS: %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp);
    /* pack the string */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &tmp, 1, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    if (NULL != tmp) {
        free(tmp);
    }

    /* do the same to pass the flags for each node */
    tmp = NULL;
    while (NULL != (item = opal_list_remove_first(&flags))) {
        rng = (orte_regex_range_t*)item;
        if (NULL == tmp) {
            asprintf(&tmp, "%d[%d]", rng->cnt, rng->slots);
        } else {
            asprintf(&tmp2, "%s,%d[%d]", tmp, rng->cnt, rng->slots);
            free(tmp);
            tmp = tmp2;
        }
        OBJ_RELEASE(rng);
    }
    OPAL_LIST_DESTRUCT(&flags);

    /* pack the string */
    opal_output_verbose(1, orte_regx_base_framework.framework_output,
                        "%s FLAG ASSIGNMENTS: %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp);
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &tmp, 1, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    if (NULL != tmp) {
        free(tmp);
    }

    /* don't try to be cute - there aren't going to be that many
     * topologies, so just scan the list and see if they are the
     * same, excluding any NULL values */
    ortetopo = NULL;
    test = false;
    OPAL_LIST_FOREACH(rng, &topos, orte_regex_range_t) {
        if (NULL == rng->t) {
            continue;
        }
        if (NULL == ortetopo) {
            ortetopo = rng->t;
        } else if (0 != strcmp(ortetopo->sig, rng->t->sig)) {
            /* we have a difference, so send them */
            test = true;
        }
    }
    tmp = NULL;
    if (test) {
        opal_buffer_t bucket, *bptr;
        OBJ_CONSTRUCT(&bucket, opal_buffer_t);
        while (NULL != (item = opal_list_remove_first(&topos))) {
            rng = (orte_regex_range_t*)item;
            opal_output_verbose(5, orte_regx_base_framework.framework_output,
                                "%s PASSING TOPOLOGY %s RANGE %d",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                (NULL == rng->t) ? "NULL" : rng->t->sig, rng->cnt);
            if (NULL == tmp) {
                asprintf(&tmp, "%d", rng->cnt);
            } else {
                asprintf(&tmp2, "%s,%d", tmp, rng->cnt);
                free(tmp);
                tmp = tmp2;
            }
            if (NULL == rng->t) {
                /* need to account for NULL topology */
                opal_output_verbose(1, orte_regx_base_framework.framework_output,
                                    "%s PACKING NULL TOPOLOGY",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                tmp2 = NULL;
                if (ORTE_SUCCESS != (rc = opal_dss.pack(&bucket, &tmp2, 1, OPAL_STRING))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(rng);
                    OPAL_LIST_DESTRUCT(&topos);
                    OBJ_DESTRUCT(&bucket);
                    free(tmp);
                    return rc;
                }
            } else {
                opal_output_verbose(1, orte_regx_base_framework.framework_output,
                                    "%s PACKING TOPOLOGY: %s",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rng->t->sig);
                /* pack this topology string */
                if (ORTE_SUCCESS != (rc = opal_dss.pack(&bucket, &rng->t->sig, 1, OPAL_STRING))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(rng);
                    OPAL_LIST_DESTRUCT(&topos);
                    OBJ_DESTRUCT(&bucket);
                    free(tmp);
                    return rc;
                }
                /* pack the topology itself */
                if (ORTE_SUCCESS != (rc = opal_dss.pack(&bucket, &rng->t->topo, 1, OPAL_HWLOC_TOPO))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(rng);
                    OPAL_LIST_DESTRUCT(&topos);
                    OBJ_DESTRUCT(&bucket);
                    free(tmp);
                    return rc;
                }
            }
            OBJ_RELEASE(rng);
        }
        OPAL_LIST_DESTRUCT(&topos);
        /* pack the string */
        opal_output_verbose(1, orte_regx_base_framework.framework_output,
                            "%s TOPOLOGY ASSIGNMENTS: %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp);
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &tmp, 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            OBJ_DESTRUCT(&bucket);
            free(tmp);
            return rc;
        }
        free(tmp);

        /* now pack the topologies */
        bptr = &bucket;
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &bptr, 1, OPAL_BUFFER))) {
            ORTE_ERROR_LOG(rc);
            OBJ_DESTRUCT(&bucket);
            return rc;
        }
        OBJ_DESTRUCT(&bucket);
    } else {
        opal_output_verbose(1, orte_regx_base_framework.framework_output,
                            "%s NOT PASSING TOPOLOGIES",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        /* need to pack the NULL just to terminate the region */
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &tmp, 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }

    return ORTE_SUCCESS;
}

int orte_regx_base_decode_daemon_nodemap(opal_buffer_t *buffer)
{
      int n, nn, rc, cnt, offset;
      orte_node_t *node;
      char *slots=NULL, *topos=NULL, *flags=NULL;
      char *rmndr, **tmp;
      opal_list_t slts, flgs;;
      opal_buffer_t *bptr=NULL;
      orte_topology_t *t2;
      orte_regex_range_t *rng, *srng, *frng;
      uint8_t ui8;

      OBJ_CONSTRUCT(&slts, opal_list_t);
      OBJ_CONSTRUCT(&flgs, opal_list_t);

      /* unpack the flag indicating if the HNP was allocated */
      n = 1;
      if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &ui8, &n, OPAL_UINT8))) {
          ORTE_ERROR_LOG(rc);
          goto cleanup;
      }
      if (0 == ui8) {
          orte_hnp_is_allocated = false;
      } else {
          orte_hnp_is_allocated = true;
      }

      /* unpack the flag indicating we are in a managed allocation */
      n = 1;
      if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &ui8, &n, OPAL_UINT8))) {
          ORTE_ERROR_LOG(rc);
          goto cleanup;
      }
      if (0 == ui8) {
          orte_managed_allocation = false;
      } else {
          orte_managed_allocation = true;
      }

      /* unpack the slots regex */
      n = 1;
      if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &slots, &n, OPAL_STRING))) {
          ORTE_ERROR_LOG(rc);
          goto cleanup;
      }
      /* this is not allowed to be NULL */
      if (NULL == slots) {
          ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
          rc = ORTE_ERR_BAD_PARAM;
          goto cleanup;
      }

      /* unpack the flags regex */
      n = 1;
      if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &flags, &n, OPAL_STRING))) {
          ORTE_ERROR_LOG(rc);
          goto cleanup;
      }
      /* this is not allowed to be NULL */
      if (NULL == flags) {
          ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
          rc = ORTE_ERR_BAD_PARAM;
          goto cleanup;
      }

      /* unpack the topos regex - this may not have been
       * provided (e.g., for a homogeneous machine) */
      n = 1;
      if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &topos, &n, OPAL_STRING))) {
          ORTE_ERROR_LOG(rc);
          goto cleanup;
      }
      if (NULL != topos) {
          /* need to unpack the topologies */
          n = 1;
          if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &bptr, &n, OPAL_BUFFER))) {
              ORTE_ERROR_LOG(rc);
              goto cleanup;
          }
      }

      /* if we are the HNP, then we just discard these strings as we already
       * have a complete picture - but we needed to unpack them in order to
       * maintain sync in the unpacking order */
      if (ORTE_PROC_IS_HNP) {
          rc = ORTE_SUCCESS;
          goto cleanup;
      }

      /* decompress the slots */
      tmp = opal_argv_split(slots, ',');
      for (n=0; NULL != tmp[n]; n++) {
          rng = OBJ_NEW(orte_regex_range_t);
          opal_list_append(&slts, &rng->super);
          /* find the '[' as that delimits the value */
          rmndr = strchr(tmp[n], '[');
          if (NULL == rmndr) {
              ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
              rc = ORTE_ERR_BAD_PARAM;
              opal_argv_free(tmp);
              goto cleanup;
          }
          *rmndr = '\0';
          ++rmndr;
          /* convert that number as this is the number of
           * slots for this range */
          rng->slots = strtoul(rmndr, NULL, 10);
          /* convert the initial number as that is the cnt */
          rng->cnt = strtoul(tmp[n], NULL, 10);
      }
      opal_argv_free(tmp);

      /* decompress the flags */
      tmp = opal_argv_split(flags, ',');
      for (n=0; NULL != tmp[n]; n++) {
          rng = OBJ_NEW(orte_regex_range_t);
          opal_list_append(&flgs, &rng->super);
          /* find the '[' as that delimits the value */
          rmndr = strchr(tmp[n], '[');
          if (NULL == rmndr) {
              ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
              opal_argv_free(tmp);
              rc = ORTE_ERR_BAD_PARAM;
              goto cleanup;
          }
          *rmndr = '\0';
          ++rmndr;
          /* check the value - it is just one character */
          if ('1' == *rmndr) {
              rng->slots = 1;
          } else {
              rng->slots = 0;
          }
          /* convert the initial number as that is the cnt */
          rng->cnt = strtoul(tmp[n], NULL, 10);
      }
      opal_argv_free(tmp);
      free(flags);

      /* update the node array */
      srng = (orte_regex_range_t*)opal_list_get_first(&slts);
      frng = (orte_regex_range_t*)opal_list_get_first(&flgs);
      for (n=0; n < orte_node_pool->size; n++) {
          if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, n))) {
              continue;
          }
          /* set the number of slots */
          node->slots = srng->slots;
          srng->cnt--;
          if (0 == srng->cnt) {
              srng = (orte_regex_range_t*)opal_list_get_next(&srng->super);
          }
          /* set the flags */
          if (0 == frng->slots) {
              ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
          } else {
              ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
          }
          frng->cnt--;
          if (0 == frng->cnt) {
              frng = (orte_regex_range_t*)opal_list_get_next(&frng->super);
          }
      }

      /* if no topology info was passed, then everyone shares our topology */
      if (NULL == bptr) {
          /* our topology is first in the array */
          t2 = (orte_topology_t*)opal_pointer_array_get_item(orte_node_topologies, 0);
          opal_output_verbose(1, orte_regx_base_framework.framework_output,
                              "%s ASSIGNING ALL TOPOLOGIES TO: %s",
                              ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), t2->sig);
          for (n=0; n < orte_node_pool->size; n++) {
              if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, n))) {
                  if (NULL == node->topology) {
                      OBJ_RETAIN(t2);
                      node->topology = t2;
                  }
              }
          }
      } else {
          char *sig;
          hwloc_topology_t topo;
          /* decompress the topology regex */
          tmp = opal_argv_split(topos, ',');
          /* there must be a topology definition for each range */
          offset = 0;
          for (nn=0; NULL != tmp[nn]; nn++) {
              cnt = strtoul(tmp[nn], NULL, 10);
              /* unpack the signature */
              n = 1;
              if (ORTE_SUCCESS != (rc = opal_dss.unpack(bptr, &sig, &n, OPAL_STRING))) {
                  ORTE_ERROR_LOG(rc);
                  opal_argv_free(tmp);
                  OBJ_RELEASE(bptr);
                  goto cleanup;
              }
              if (NULL == sig) {
                  /* the nodes in this range have not reported a topology,
                   * so skip them */
                  offset += cnt;
                  continue;
              }
              n = 1;
              if (ORTE_SUCCESS != (rc = opal_dss.unpack(bptr, &topo, &n, OPAL_HWLOC_TOPO))) {
                  ORTE_ERROR_LOG(rc);
                  opal_argv_free(tmp);
                  OBJ_RELEASE(bptr);
                  free(sig);
                  goto cleanup;
              }
              /* see if we already have this topology - could be an update */
              t2 = NULL;
              for (n=0; n < orte_node_topologies->size; n++) {
                  if (NULL == (t2 = (orte_topology_t*)opal_pointer_array_get_item(orte_node_topologies, n))) {
                      continue;
                  }
                  if (0 == strcmp(t2->sig, sig)) {
                      /* found a match */
                      free(sig);
                      opal_hwloc_base_free_topology(topo);
                      sig = NULL;
                      break;
                  }
              }
              if (NULL != sig || NULL == t2) {
                  /* new topology - record it */
                  t2 = OBJ_NEW(orte_topology_t);
                  t2->sig = sig;
                  t2->topo = topo;
                  opal_pointer_array_add(orte_node_topologies, t2);
              }
              /* point each of the nodes in this range to this topology */
              n=0;
              while (n < cnt && (n+offset) < orte_node_pool->size) {
                  if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, n+offset))) {
                      continue;
                  }
                  opal_output_verbose(1, orte_regx_base_framework.framework_output,
                                      "%s ASSIGNING NODE %s WITH TOPO: %s",
                                      ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                      node->name, t2->sig);
                  if (NULL == node->topology) {
                      OBJ_RETAIN(t2);
                      node->topology = t2;
                  }
                  ++n;
              }
              offset += cnt;
          }
          OBJ_RELEASE(bptr);
          opal_argv_free(tmp);
      }

  cleanup:
    OPAL_LIST_DESTRUCT(&slts);
    OPAL_LIST_DESTRUCT(&flgs);
    return rc;
}

int orte_regx_base_generate_ppn(orte_job_t *jdata, char **ppn)
{
    orte_nidmap_regex_t *prng, **actives;
    opal_list_t *prk;
    orte_node_t *nptr;
    orte_proc_t *proc;
    size_t n;
    int *cnt, i, k;
    char *tmp2, *ptmp, **cache = NULL;

    /* create an array of lists to handle the number of app_contexts in this job */
    prk = (opal_list_t*)malloc(jdata->num_apps * sizeof(opal_list_t));
    cnt = (int*)malloc(jdata->num_apps * sizeof(int));
    actives = (orte_nidmap_regex_t**)malloc(jdata->num_apps * sizeof(orte_nidmap_regex_t*));
    for (n=0; n < jdata->num_apps; n++) {
        OBJ_CONSTRUCT(&prk[n], opal_list_t);
        actives[n] = NULL;
    }

    /* we provide a complete map in the regex, with an entry for every
     * node in the pool */
    for (i=0; i < orte_node_pool->size; i++) {
        if (NULL == (nptr = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
            continue;
        }
        /* if a daemon has been assigned, then count how many procs
         * for each app_context from the specified job are assigned to this node */
        memset(cnt, 0, jdata->num_apps * sizeof(int));
        if (NULL != nptr->daemon) {
            for (k=0; k < nptr->procs->size; k++) {
                if (NULL != (proc = (orte_proc_t*)opal_pointer_array_get_item(nptr->procs, k))) {
                    if (proc->name.jobid == jdata->jobid) {
                        ++cnt[proc->app_idx];
                    }
                }
            }
        }
        /* track the #procs on this node */
        for (n=0; n < jdata->num_apps; n++) {
            if (NULL == actives[n]) {
                /* just starting */
                actives[n] = OBJ_NEW(orte_nidmap_regex_t);
                actives[n]->nprocs = cnt[n];
                actives[n]->cnt = 1;
                opal_list_append(&prk[n], &actives[n]->super);
            } else {
                /* is this the next in line */
                if (cnt[n] == actives[n]->nprocs) {
                    actives[n]->cnt++;
                } else {
                    /* need to start another range */
                    actives[n] = OBJ_NEW(orte_nidmap_regex_t);
                    actives[n]->nprocs = cnt[n];
                    actives[n]->cnt = 1;
                    opal_list_append(&prk[n], &actives[n]->super);
                }
            }
        }
    }

    /* construct the regex from the found ranges for each app_context */
    ptmp = NULL;
    for (n=0; n < jdata->num_apps; n++) {
        OPAL_LIST_FOREACH(prng, &prk[n], orte_nidmap_regex_t) {
            if (1 < prng->cnt) {
                if (NULL == ptmp) {
                    asprintf(&ptmp, "%u(%u)", prng->nprocs, prng->cnt);
                } else {
                    asprintf(&tmp2, "%s,%u(%u)", ptmp, prng->nprocs, prng->cnt);
                    free(ptmp);
                    ptmp = tmp2;
                }
            } else {
                if (NULL == ptmp) {
                    asprintf(&ptmp, "%u", prng->nprocs);
                } else {
                    asprintf(&tmp2, "%s,%u", ptmp, prng->nprocs);
                    free(ptmp);
                    ptmp = tmp2;
                }
            }
        }
        OPAL_LIST_DESTRUCT(&prk[n]);  // releases all the actives objects
        if (NULL != ptmp) {
            opal_argv_append_nosize(&cache, ptmp);
            free(ptmp);
            ptmp = NULL;
        }
    }
    free(prk);
    free(cnt);
    free(actives);

    *ppn = opal_argv_join(cache, '@');
    opal_argv_free(cache);

    return ORTE_SUCCESS;
}

int orte_regx_base_parse_ppn(orte_job_t *jdata, char *regex)
{
    orte_node_t *node;
    orte_proc_t *proc;
    int n, k, m, cnt;
    char **tmp, *ptr, **ppn;
    orte_nidmap_regex_t *rng;
    opal_list_t trk;
    int rc = ORTE_SUCCESS;

    /* split the regex by app_context */
    tmp = opal_argv_split(regex, '@');

    /* for each app_context, set the ppn */
    for (n=0; NULL != tmp[n]; n++) {
        ppn = opal_argv_split(tmp[n], ',');
        /* decompress the ppn */
        OBJ_CONSTRUCT(&trk, opal_list_t);
        for (m=0; NULL != ppn[m]; m++) {
            rng = OBJ_NEW(orte_nidmap_regex_t);
            opal_list_append(&trk, &rng->super);
            /* check for a count */
            if (NULL != (ptr = strchr(ppn[m], '('))) {
                ppn[m][strlen(ppn[m])-1] = '\0';  // remove trailing paren
                *ptr = '\0';
                ++ptr;
                rng->cnt = strtoul(ptr, NULL, 10);
            } else {
                rng->cnt = 1;
            }
            /* convert the number */
            rng->nprocs = strtoul(ppn[m], NULL, 10);
        }
        opal_argv_free(ppn);

        /* cycle thru our node pool and add the indicated number of procs
         * to each node */
        rng = (orte_nidmap_regex_t*)opal_list_get_first(&trk);
        cnt = 0;
        for (m=0; m < orte_node_pool->size; m++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, m))) {
                continue;
            }
            /* see if it has any procs for this job and app_context */
            if (0 < rng->nprocs) {
                /* add this node to the job map if it isn't already there */
                if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_MAPPED)) {
                    OBJ_RETAIN(node);
                    ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
                    opal_pointer_array_add(jdata->map->nodes, node);
                }
                /* create a proc object for each one */
                for (k=0; k < rng->nprocs; k++) {
                    proc = OBJ_NEW(orte_proc_t);
                    proc->name.jobid = jdata->jobid;
                    /* leave the vpid undefined as this will be determined
                     * later when we do the overall ranking */
                    proc->app_idx = n;
                    proc->parent = node->daemon->name.vpid;
                    OBJ_RETAIN(node);
                    proc->node = node;
                    /* flag the proc as ready for launch */
                    proc->state = ORTE_PROC_STATE_INIT;
                    opal_pointer_array_add(node->procs, proc);
                    /* we will add the proc to the jdata array when we
                     * compute its rank */
                }
                node->num_procs += rng->nprocs;
            }
            ++cnt;
            if (rng->cnt <= cnt) {
                rng = (orte_nidmap_regex_t*)opal_list_get_next(&rng->super);
                if (NULL == rng) {
                    ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                    opal_argv_free(tmp);
                    rc = ORTE_ERR_NOT_FOUND;
                    goto complete;
                }
                cnt = 0;
            }
        }
        OPAL_LIST_DESTRUCT(&trk);
    }
    opal_argv_free(tmp);

  complete:
    /* reset any node map flags we used so the next job will start clean */
    for (n=0; n < jdata->map->nodes->size; n++) {
        if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(jdata->map->nodes, n))) {
            ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
        }
    }

    return rc;
}


static int regex_parse_node_range(char *base, char *range, int num_digits, char *suffix, char ***names);

/*
 * Parse one or more ranges in a set
 *
 * @param base     The base text of the node name
 * @param *ranges  A pointer to a range. This can contain multiple ranges
 *                 (i.e. "1-3,10" or "5" or "9,0100-0130,250")
 * @param ***names An argv array to add the newly discovered nodes to
 */
static int regex_parse_node_ranges(char *base, char *ranges, int num_digits, char *suffix, char ***names)
{
    int i, len, ret;
    char *start, *orig;

    /* Look for commas, the separator between ranges */

    len = strlen(ranges);
    for (orig = start = ranges, i = 0; i < len; ++i) {
        if (',' == ranges[i]) {
            ranges[i] = '\0';
            ret = regex_parse_node_range(base, start, num_digits, suffix, names);
            if (ORTE_SUCCESS != ret) {
                ORTE_ERROR_LOG(ret);
                return ret;
            }
            start = ranges + i + 1;
        }
    }

    /* Pick up the last range, if it exists */

    if (start < orig + len) {

        OPAL_OUTPUT_VERBOSE((1, orte_debug_output,
                             "%s regex:parse:ranges: parse range %s (2)",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), start));

        ret = regex_parse_node_range(base, start, num_digits, suffix, names);
        if (ORTE_SUCCESS != ret) {
            ORTE_ERROR_LOG(ret);
            return ret;
        }
    }

    /* All done */
    return ORTE_SUCCESS;
}


/*
 * Parse a single range in a set and add the full names of the nodes
 * found to the names argv
 *
 * @param base     The base text of the node name
 * @param *ranges  A pointer to a single range. (i.e. "1-3" or "5")
 * @param ***names An argv array to add the newly discovered nodes to
 */
static int regex_parse_node_range(char *base, char *range, int num_digits, char *suffix, char ***names)
{
    char *str, tmp[132];
    size_t i, k, start, end;
    size_t base_len, len;
    bool found;
    int ret;

    if (NULL == base || NULL == range) {
        return ORTE_ERROR;
    }

    len = strlen(range);
    base_len = strlen(base);
    /* Silence compiler warnings; start and end are always assigned
     properly, below */
    start = end = 0;

    /* Look for the beginning of the first number */

    for (found = false, i = 0; i < len; ++i) {
        if (isdigit((int) range[i])) {
            if (!found) {
                start = atoi(range + i);
                found = true;
                break;
            }
        }
    }
    if (!found) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }

    /* Look for the end of the first number */

    for (found = false; i < len; ++i) {
        if (!isdigit(range[i])) {
            break;
        }
    }

    /* Was there no range, just a single number? */

    if (i >= len) {
        end = start;
        found = true;
    } else {
        /* Nope, there was a range.  Look for the beginning of the second
         * number
         */
        for (; i < len; ++i) {
            if (isdigit(range[i])) {
                end = strtol(range + i, NULL, 10);
                found = true;
                break;
            }
        }
    }
    if (!found) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }

    /* Make strings for all values in the range */

    len = base_len + num_digits + 32;
    if (NULL != suffix) {
        len += strlen(suffix);
    }
    str = (char *) malloc(len);
    if (NULL == str) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    for (i = start; i <= end; ++i) {
        memset(str, 0, len);
        strcpy(str, base);
        /* we need to zero-pad the digits */
        for (k=0; k < (size_t)num_digits; k++) {
            str[k+base_len] = '0';
        }
        memset(tmp, 0, 132);
        snprintf(tmp, 132, "%lu", (unsigned long)i);
        for (k=0; k < strlen(tmp); k++) {
            str[base_len + num_digits - k - 1] = tmp[strlen(tmp)-k-1];
        }
        /* if there is a suffix, add it */
        if (NULL != suffix) {
            strcat(str, suffix);
        }
        ret = opal_argv_append_nosize(names, str);
        if(ORTE_SUCCESS != ret) {
            ORTE_ERROR_LOG(ret);
            free(str);
            return ret;
        }
    }
    free(str);

    /* All done */
    return ORTE_SUCCESS;
}

static int regex_parse_node_range(char *base, char *range, int num_digits, char *suffix, char ***names);

int orte_regx_base_extract_node_names(char *regexp, char ***names)
{
    int i, j, k, len, ret;
    char *base;
    char *orig, *suffix;
    bool found_range = false;
    bool more_to_come = false;
    int num_digits;

    if (NULL == regexp) {
        *names = NULL;
        return ORTE_SUCCESS;
    }

    orig = base = strdup(regexp);
    if (NULL == base) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_debug_output,
                         "%s regex:extract:nodenames: checking nodelist: %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         regexp));

    do {
        /* Find the base */
        len = strlen(base);
        for (i = 0; i <= len; ++i) {
            if (base[i] == '[') {
                /* we found a range. this gets dealt with below */
                base[i] = '\0';
                found_range = true;
                break;
            }
            if (base[i] == ',') {
                /* we found a singleton node, and there are more to come */
                base[i] = '\0';
                found_range = false;
                more_to_come = true;
                break;
            }
            if (base[i] == '\0') {
                /* we found a singleton node */
                found_range = false;
                more_to_come = false;
                break;
            }
        }
        if (i == 0 && !found_range) {
            /* we found a special character at the beginning of the string */
            orte_show_help("help-regex.txt", "regex:special-char", true, regexp);
            free(orig);
            return ORTE_ERR_BAD_PARAM;
        }

        if (found_range) {
            /* If we found a range, get the number of digits in the numbers */
            i++;  /* step over the [ */
            for (j=i; j < len; j++) {
                if (base[j] == ':') {
                    base[j] = '\0';
                    break;
                }
            }
            if (j >= len) {
                /* we didn't find the number of digits */
                orte_show_help("help-regex.txt", "regex:num-digits-missing", true, regexp);
                free(orig);
                return ORTE_ERR_BAD_PARAM;
            }
            num_digits = strtol(&base[i], NULL, 10);
            i = j + 1;  /* step over the : */
            /* now find the end of the range */
            for (j = i; j < len; ++j) {
                if (base[j] == ']') {
                    base[j] = '\0';
                    break;
                }
            }
            if (j >= len) {
                /* we didn't find the end of the range */
                orte_show_help("help-regex.txt", "regex:end-range-missing", true, regexp);
                free(orig);
                return ORTE_ERR_BAD_PARAM;
            }
            /* check for a suffix */
            if (j+1 < len && base[j+1] != ',') {
                /* find the next comma, if present */
                for (k=j+1; k < len && base[k] != ','; k++);
                if (k < len) {
                    base[k] = '\0';
                }
                suffix = strdup(&base[j+1]);
                if (k < len) {
                    base[k] = ',';
                }
                j = k-1;
            } else {
                suffix = NULL;
            }
            OPAL_OUTPUT_VERBOSE((1, orte_debug_output,
                                 "%s regex:extract:nodenames: parsing range %s %s %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 base, base + i, suffix));

            ret = regex_parse_node_ranges(base, base + i, num_digits, suffix, names);
            if (NULL != suffix) {
                free(suffix);
            }
            if (ORTE_SUCCESS != ret) {
                orte_show_help("help-regex.txt", "regex:bad-value", true, regexp);
                free(orig);
                return ret;
            }
            if (j+1 < len && base[j + 1] == ',') {
                more_to_come = true;
                base = &base[j + 2];
            } else {
                more_to_come = false;
            }
        } else {
            /* If we didn't find a range, just add the node */
            if(ORTE_SUCCESS != (ret = opal_argv_append_nosize(names, base))) {
                ORTE_ERROR_LOG(ret);
                free(orig);
                return ret;
            }
            /* step over the comma */
            i++;
            /* set base equal to the (possible) next base to look at */
            base = &base[i];
        }
    } while(more_to_come);

    free(orig);

    /* All done */
    return ret;
}
