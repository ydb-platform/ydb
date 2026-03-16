/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2016 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <stdio.h>
#include <stddef.h>
#include <ctype.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <time.h>


#include "opal/mca/event/event.h"
#include "opal/mca/base/base.h"
#include "opal/mca/pstat/pstat.h"
#include "opal/util/output.h"
#include "opal/util/opal_environ.h"
#include "opal/util/path.h"
#include "opal/runtime/opal.h"
#include "opal/runtime/opal_progress.h"
#include "opal/dss/dss.h"

#include "orte/util/proc_info.h"
#include "orte/util/session_dir.h"
#include "orte/util/name_fns.h"
#include "orte/util/compress.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/iof/base/base.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/odls/odls.h"
#include "orte/mca/odls/base/base.h"
#include "orte/mca/oob/base/base.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/state/state.h"

#include "orte/mca/odls/base/odls_private.h"

#include "orte/runtime/runtime.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_quit.h"

#include "orte/orted/orted.h"

/*
 * Globals
 */
static char *get_orted_comm_cmd_str(int command);

static opal_pointer_array_t *procs_prev_ordered_to_terminate = NULL;

void orte_daemon_recv(int status, orte_process_name_t* sender,
                      opal_buffer_t *buffer, orte_rml_tag_t tag,
                      void* cbdata)
{
    orte_daemon_cmd_flag_t command;
    opal_buffer_t *relay_msg;
    int ret;
    orte_std_cntr_t n;
    int32_t signal;
    orte_jobid_t job;
    char *contact_info;
    opal_buffer_t data, *answer;
    orte_job_t *jdata;
    orte_process_name_t proc, proc2;
    orte_process_name_t *return_addr;
    int32_t i, num_replies;
    bool hnp_accounted_for;
    opal_pointer_array_t procarray;
    orte_proc_t *proct;
    char *cmd_str = NULL;
    opal_pointer_array_t *procs_to_kill = NULL;
    orte_std_cntr_t num_procs, num_new_procs = 0, p;
    orte_proc_t *cur_proc = NULL, *prev_proc = NULL;
    bool found = false;
    orte_node_t *node;
    orte_grpcomm_signature_t *sig;
    FILE *fp;
    char gscmd[256], path[1035], *pathptr;
    char string[256], *string_ptr = string;
    float pss;
    opal_pstats_t pstat;
    char *rtmod;
    char *coprocessors;
    orte_job_map_t *map;
    int8_t flag;
    uint8_t *cmpdata;
    size_t cmplen;

    /* unpack the command */
    n = 1;
    if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &command, &n, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(ret);
        return;
    }

    cmd_str = get_orted_comm_cmd_str(command);
    OPAL_OUTPUT_VERBOSE((1, orte_debug_output,
                         "%s orted:comm:process_commands() Processing Command: %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), cmd_str));
    free(cmd_str);
    cmd_str = NULL;

    /* now process the command locally */
    switch(command) {

        /****    NULL    ****/
    case ORTE_DAEMON_NULL_CMD:
        ret = ORTE_SUCCESS;
        break;

        /****    KILL_LOCAL_PROCS   ****/
    case ORTE_DAEMON_KILL_LOCAL_PROCS:
        num_replies = 0;

        /* construct the pointer array */
        OBJ_CONSTRUCT(&procarray, opal_pointer_array_t);
        opal_pointer_array_init(&procarray, num_replies, ORTE_GLOBAL_ARRAY_MAX_SIZE, 16);

        /* unpack the proc names into the array */
        while (ORTE_SUCCESS == (ret = opal_dss.unpack(buffer, &proc, &n, ORTE_NAME))) {
            proct = OBJ_NEW(orte_proc_t);
            proct->name.jobid = proc.jobid;
            proct->name.vpid = proc.vpid;

            opal_pointer_array_add(&procarray, proct);
            num_replies++;
        }
        if (ORTE_ERR_UNPACK_READ_PAST_END_OF_BUFFER != ret) {
            ORTE_ERROR_LOG(ret);
            goto KILL_PROC_CLEANUP;
        }

        if (0 == num_replies) {
            /* kill everything */
            if (ORTE_SUCCESS != (ret = orte_odls.kill_local_procs(NULL))) {
                ORTE_ERROR_LOG(ret);
            }
            break;
        } else {
            /* kill the procs */
            if (ORTE_SUCCESS != (ret = orte_odls.kill_local_procs(&procarray))) {
                ORTE_ERROR_LOG(ret);
            }
        }

        /* cleanup */
    KILL_PROC_CLEANUP:
        for (i=0; i < procarray.size; i++) {
            if (NULL != (proct = (orte_proc_t*)opal_pointer_array_get_item(&procarray, i))) {
                free(proct);
            }
        }
        OBJ_DESTRUCT(&procarray);
        break;

        /****    SIGNAL_LOCAL_PROCS   ****/
    case ORTE_DAEMON_SIGNAL_LOCAL_PROCS:
        /* unpack the jobid */
        n = 1;
        if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &job, &n, ORTE_JOBID))) {
            ORTE_ERROR_LOG(ret);
            goto CLEANUP;
        }

        /* look up job data object */
        jdata = orte_get_job_data_object(job);

        /* get the signal */
        n = 1;
        if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &signal, &n, OPAL_INT32))) {
            ORTE_ERROR_LOG(ret);
            goto CLEANUP;
        }

        /* Convert SIGTSTP to SIGSTOP so we can suspend a.out */
        if (SIGTSTP == signal) {
            if (orte_debug_daemons_flag) {
                opal_output(0, "%s orted_cmd: converted SIGTSTP to SIGSTOP before delivering",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            }
            signal = SIGSTOP;
            if (NULL != jdata) {
                jdata->state |= ORTE_JOB_STATE_SUSPENDED;
            }
        } else if (SIGCONT == signal && NULL != jdata) {
            jdata->state &= ~ORTE_JOB_STATE_SUSPENDED;
        }

        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received signal_local_procs, delivering signal %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        signal);
        }

        /* signal them */
        if (ORTE_SUCCESS != (ret = orte_odls.signal_local_procs(NULL, signal))) {
            ORTE_ERROR_LOG(ret);
        }
        break;

        /****    ADD_LOCAL_PROCS   ****/
    case ORTE_DAEMON_ADD_LOCAL_PROCS:
    case ORTE_DAEMON_DVM_ADD_PROCS:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received add_local_procs",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }

        /* launch the processes */
        if (ORTE_SUCCESS != (ret = orte_odls.launch_local_procs(buffer))) {
            OPAL_OUTPUT_VERBOSE((1, orte_debug_output,
                                 "%s orted:comm:add_procs failed to launch on error %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_ERROR_NAME(ret)));
        }
        break;

    case ORTE_DAEMON_ABORT_PROCS_CALLED:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received abort_procs report",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }

        /* Number of processes */
        n = 1;
        if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &num_procs, &n, ORTE_STD_CNTR)) ) {
            ORTE_ERROR_LOG(ret);
            goto CLEANUP;
        }

        /* Retrieve list of processes */
        procs_to_kill = OBJ_NEW(opal_pointer_array_t);
        opal_pointer_array_init(procs_to_kill, num_procs, INT32_MAX, 2);

        /* Keep track of previously terminated, so we don't keep ordering the
         * same processes to die.
         */
        if( NULL == procs_prev_ordered_to_terminate ) {
            procs_prev_ordered_to_terminate = OBJ_NEW(opal_pointer_array_t);
            opal_pointer_array_init(procs_prev_ordered_to_terminate, num_procs+1, INT32_MAX, 8);
        }

        num_new_procs = 0;
        for( i = 0; i < num_procs; ++i) {
            cur_proc = OBJ_NEW(orte_proc_t);

            n = 1;
            if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &(cur_proc->name), &n, ORTE_NAME)) ) {
                ORTE_ERROR_LOG(ret);
                goto CLEANUP;
            }

            /* See if duplicate */
            found = false;
            for( p = 0; p < procs_prev_ordered_to_terminate->size; ++p) {
                if( NULL == (prev_proc = (orte_proc_t*)opal_pointer_array_get_item(procs_prev_ordered_to_terminate, p))) {
                    continue;
                }
                if(OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL,
                                                               &cur_proc->name,
                                                               &prev_proc->name) ) {
                    found = true;
                    break;
                }
            }

            OPAL_OUTPUT_VERBOSE((2, orte_debug_output,
                                 "%s orted:comm:abort_procs Application %s requests term. of %s (%2d of %2d) %3s.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(sender),
                                 ORTE_NAME_PRINT(&(cur_proc->name)), i, num_procs,
                                 (found ? "Dup" : "New") ));

            /* If not a duplicate, then add to the to_kill list */
            if( !found ) {
                opal_pointer_array_add(procs_to_kill, (void*)cur_proc);
                OBJ_RETAIN(cur_proc);
                opal_pointer_array_add(procs_prev_ordered_to_terminate, (void*)cur_proc);
                num_new_procs++;
            }
        }

        /*
         * Send the request to terminate
         */
        if( num_new_procs > 0 ) {
            OPAL_OUTPUT_VERBOSE((2, orte_debug_output,
                                 "%s orted:comm:abort_procs Terminating application requested processes (%2d / %2d).",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 num_new_procs, num_procs));
            orte_plm.terminate_procs(procs_to_kill);
        } else {
            OPAL_OUTPUT_VERBOSE((2, orte_debug_output,
                                 "%s orted:comm:abort_procs No new application processes to terminating from request (%2d / %2d).",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 num_new_procs, num_procs));
        }

        break;

        /****    EXIT COMMAND    ****/
    case ORTE_DAEMON_EXIT_CMD:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received exit cmd",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }
        if (orte_do_not_launch) {
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            return;
        }
        /* kill the local procs */
        orte_odls.kill_local_procs(NULL);
        /* flag that orteds were ordered to terminate */
        orte_orteds_term_ordered = true;
        /* if all my routes and local children are gone, then terminate ourselves */
        rtmod = orte_rml.get_routed(orte_mgmt_conduit);
        if (0 == (ret = orte_routed.num_routes(rtmod))) {
            for (i=0; i < orte_local_children->size; i++) {
                if (NULL != (proct = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i)) &&
                    ORTE_FLAG_TEST(proct, ORTE_PROC_FLAG_ALIVE)) {
                    /* at least one is still alive */
                    if (orte_debug_daemons_flag) {
                        opal_output(0, "%s orted_cmd: exit cmd, but proc %s is alive",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    ORTE_NAME_PRINT(&proct->name));
                    }
                    return;
                }
            }
            /* call our appropriate exit procedure */
            if (orte_debug_daemons_flag) {
                opal_output(0, "%s orted_cmd: all routes and children gone - exiting",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            }
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
        } else if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: exit cmd, %d routes still exist",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret);
        }
        return;
        break;

        /****    HALT VM COMMAND    ****/
    case ORTE_DAEMON_HALT_VM_CMD:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received halt_vm cmd",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }
        if (orte_do_not_launch) {
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            return;
        }
        /* kill the local procs */
        orte_odls.kill_local_procs(NULL);
        /* flag that orteds were ordered to terminate */
        orte_orteds_term_ordered = true;
        if (ORTE_PROC_IS_HNP) {
            /* if all my routes and local children are gone, then terminate ourselves */
            rtmod = orte_rml.get_routed(orte_mgmt_conduit);
            if (0 == orte_routed.num_routes(rtmod)) {
                for (i=0; i < orte_local_children->size; i++) {
                    if (NULL != (proct = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i)) &&
                        ORTE_FLAG_TEST(proct, ORTE_PROC_FLAG_ALIVE)) {
                        /* at least one is still alive */
                        return;
                    }
                }
                /* call our appropriate exit procedure */
                if (orte_debug_daemons_flag) {
                    opal_output(0, "%s orted_cmd: all routes and children gone - exiting",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                }
                ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            }
        } else {
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
        }
        return;
        break;

        /****    HALT DVM COMMAND    ****/
    case ORTE_DAEMON_HALT_DVM_CMD:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received halt_dvm cmd",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }
        /* we just need to xcast the HALT_VM cmd out, which will send
         * it back into us */
        answer = OBJ_NEW(opal_buffer_t);
        command = ORTE_DAEMON_HALT_VM_CMD;
        opal_dss.pack(answer, &command, 1, ORTE_DAEMON_CMD);
        sig = OBJ_NEW(orte_grpcomm_signature_t);
        sig->signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
        sig->signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
        sig->signature[0].vpid = ORTE_VPID_WILDCARD;
        orte_grpcomm.xcast(sig, ORTE_RML_TAG_DAEMON, answer);
        OBJ_RELEASE(answer);
        OBJ_RELEASE(sig);
        return;
        break;

        /****    SPAWN JOB COMMAND    ****/
    case ORTE_DAEMON_SPAWN_JOB_CMD:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received spawn job",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }
        /* can only process this if we are the HNP */
        if (ORTE_PROC_IS_HNP) {
            /* unpack the job data */
            n = 1;
            if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &jdata, &n, ORTE_JOB))) {
                ORTE_ERROR_LOG(ret);
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
                break;
            }
            /* point the originator to the sender */
            jdata->originator = *sender;
            /* assign a jobid to it */
            if (ORTE_SUCCESS != (ret = orte_plm_base_create_jobid(jdata))) {
                ORTE_ERROR_LOG(ret);
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
                break;
            }
            /* store it on the global job data pool */
            opal_hash_table_set_value_uint32(orte_job_data, jdata->jobid, jdata);
            /* before we launch it, tell the IOF to forward all output exclusively
             * to the requestor */
            {
                orte_iof_tag_t ioftag;
                opal_buffer_t *iofbuf;
                orte_process_name_t source;

                ioftag = ORTE_IOF_EXCLUSIVE | ORTE_IOF_STDOUTALL | ORTE_IOF_PULL;
                iofbuf = OBJ_NEW(opal_buffer_t);
                /* pack the tag */
                if (ORTE_SUCCESS != (ret = opal_dss.pack(iofbuf, &ioftag, 1, ORTE_IOF_TAG))) {
                    ORTE_ERROR_LOG(ret);
                    OBJ_RELEASE(iofbuf);
                    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
                    break;
                }
                /* pack the name of the source */
                source.jobid = jdata->jobid;
                source.vpid = ORTE_VPID_WILDCARD;
                if (ORTE_SUCCESS != (ret = opal_dss.pack(iofbuf, &source, 1, ORTE_NAME))) {
                    ORTE_ERROR_LOG(ret);
                    OBJ_RELEASE(iofbuf);
                    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
                    break;
                }
                /* pack the sender as the sink */
                if (ORTE_SUCCESS != (ret = opal_dss.pack(iofbuf, sender, 1, ORTE_NAME))) {
                    ORTE_ERROR_LOG(ret);
                    OBJ_RELEASE(iofbuf);
                    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
                    break;
                }
                /* send the buffer to our IOF */
                orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                        ORTE_PROC_MY_NAME, iofbuf, ORTE_RML_TAG_IOF_HNP,
                                        orte_rml_send_callback, NULL);
            }
            for (i=1; i < orte_node_pool->size; i++) {
                if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                    node->state = ORTE_NODE_STATE_ADDED;
                }
            }
            /* now launch the job - this will just push it into our state machine */
            if (ORTE_SUCCESS != (ret = orte_plm.spawn(jdata))) {
                ORTE_ERROR_LOG(ret);
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
                break;
            }
        }
        break;

        /****    TERMINATE JOB COMMAND    ****/
    case ORTE_DAEMON_TERMINATE_JOB_CMD:

        /* unpack the jobid */
        n = 1;
        if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &job, &n, ORTE_JOBID))) {
            ORTE_ERROR_LOG(ret);
            goto CLEANUP;
        }

        /* look up job data object */
        if (NULL == (jdata = orte_get_job_data_object(job))) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            goto CLEANUP;
        }

        /* mark the job as (being) cancelled so that we can distinguish it later */
        if (ORTE_SUCCESS != (ret = orte_set_attribute(&jdata->attributes, ORTE_JOB_CANCELLED,
                                                      ORTE_ATTR_LOCAL, NULL, OPAL_BOOL))) {
            ORTE_ERROR_LOG(ret);
            goto CLEANUP;
        }

        if (ORTE_SUCCESS != (ret = orte_plm.terminate_job(job))) {
            ORTE_ERROR_LOG(ret);
            goto CLEANUP;
        }
        break;


        /****     DVM CLEANUP JOB COMMAND    ****/
    case ORTE_DAEMON_DVM_CLEANUP_JOB_CMD:
        /* unpack the jobid */
        n = 1;
        if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &job, &n, ORTE_JOBID))) {
            ORTE_ERROR_LOG(ret);
            goto CLEANUP;
        }

        /* look up job data object */
        if (NULL == (jdata = orte_get_job_data_object(job))) {
            /* we can safely ignore this request as the job
             * was already cleaned up, or it was a tool */
            goto CLEANUP;
        }

        /* if we have any local children for this job, then we
         * can ignore this request as we would have already
         * dealt with it */
        if (0 < jdata->num_local_procs) {
            goto CLEANUP;
        }

        /* release all resources (even those on other nodes) that we
         * assigned to this job */
        if (NULL != jdata->map) {
            map = (orte_job_map_t*)jdata->map;
            for (n = 0; n < map->nodes->size; n++) {
                if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, n))) {
                    continue;
                }
                for (i = 0; i < node->procs->size; i++) {
                    if (NULL == (proct = (orte_proc_t*)opal_pointer_array_get_item(node->procs, i))) {
                        continue;
                    }
                    if (proct->name.jobid != jdata->jobid) {
                        /* skip procs from another job */
                        continue;
                    }
                    node->slots_inuse--;
                    node->num_procs--;
                    /* set the entry in the node array to NULL */
                    opal_pointer_array_set_item(node->procs, i, NULL);
                    /* release the proc once for the map entry */
                    OBJ_RELEASE(proct);
                }
                /* set the node location to NULL */
                opal_pointer_array_set_item(map->nodes, n, NULL);
                /* maintain accounting */
                OBJ_RELEASE(node);
                /* flag that the node is no longer in a map */
                ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
            }
            OBJ_RELEASE(map);
            jdata->map = NULL;
        }
        break;


        /****     REPORT TOPOLOGY COMMAND    ****/
    case ORTE_DAEMON_REPORT_TOPOLOGY_CMD:
        OBJ_CONSTRUCT(&data, opal_buffer_t);
        /* pack the topology signature */
        if (ORTE_SUCCESS != (ret = opal_dss.pack(&data, &orte_topo_signature, 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(ret);
            OBJ_DESTRUCT(&data);
            goto CLEANUP;
        }
        /* pack the topology */
        if (ORTE_SUCCESS != (ret = opal_dss.pack(&data, &opal_hwloc_topology, 1, OPAL_HWLOC_TOPO))) {
            ORTE_ERROR_LOG(ret);
            OBJ_DESTRUCT(&data);
            goto CLEANUP;
        }

        /* detect and add any coprocessors */
        coprocessors = opal_hwloc_base_find_coprocessors(opal_hwloc_topology);
        if (ORTE_SUCCESS != (ret = opal_dss.pack(&data, &coprocessors, 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(ret);
        }
        if (NULL != coprocessors) {
            free(coprocessors);
        }
        /* see if I am on a coprocessor */
        coprocessors = opal_hwloc_base_check_on_coprocessor();
        if (ORTE_SUCCESS != (ret = opal_dss.pack(&data, &coprocessors, 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(ret);
        }
        if (NULL!= coprocessors) {
            free(coprocessors);
        }
        answer = OBJ_NEW(opal_buffer_t);
        if (orte_util_compress_block((uint8_t*)data.base_ptr, data.bytes_used,
                             &cmpdata, &cmplen)) {
            /* the data was compressed - mark that we compressed it */
            flag = 1;
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &flag, 1, OPAL_INT8))) {
                ORTE_ERROR_LOG(ret);
                free(cmpdata);
                OBJ_DESTRUCT(&data);
                OBJ_RELEASE(answer);
                goto CLEANUP;
            }
            /* pack the compressed length */
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &cmplen, 1, OPAL_SIZE))) {
                ORTE_ERROR_LOG(ret);
                free(cmpdata);
                OBJ_DESTRUCT(&data);
                OBJ_RELEASE(answer);
                goto CLEANUP;
            }
            /* pack the uncompressed length */
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &data.bytes_used, 1, OPAL_SIZE))) {
                ORTE_ERROR_LOG(ret);
                free(cmpdata);
                OBJ_DESTRUCT(&data);
                OBJ_RELEASE(answer);
                goto CLEANUP;
            }
            /* pack the compressed info */
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, cmpdata, cmplen, OPAL_UINT8))) {
                ORTE_ERROR_LOG(ret);
                free(cmpdata);
                OBJ_DESTRUCT(&data);
                OBJ_RELEASE(answer);
                goto CLEANUP;
            }
            OBJ_DESTRUCT(&data);
            free(cmpdata);
        } else {
            /* mark that it was not compressed */
            flag = 0;
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &flag, 1, OPAL_INT8))) {
                ORTE_ERROR_LOG(ret);
                OBJ_DESTRUCT(&data);
                free(cmpdata);
                OBJ_RELEASE(answer);
                goto CLEANUP;
            }
            /* transfer the payload across */
            opal_dss.copy_payload(answer, &data);
            OBJ_DESTRUCT(&data);
        }
        /* send the data */
        if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                               sender, answer, ORTE_RML_TAG_TOPOLOGY_REPORT,
                                               orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(ret);
            OBJ_RELEASE(answer);
        }
        break;

        /****     CONTACT QUERY COMMAND    ****/
    case ORTE_DAEMON_CONTACT_QUERY_CMD:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received contact query",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }
        /* send back contact info */
        orte_oob_base_get_addr(&contact_info);

        if (NULL == contact_info) {
            ORTE_ERROR_LOG(ORTE_ERROR);
            ret = ORTE_ERROR;
            goto CLEANUP;
        }

        /* setup buffer with answer */
        answer = OBJ_NEW(opal_buffer_t);
        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &contact_info, 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(ret);
            OBJ_RELEASE(answer);
            goto CLEANUP;
        }

        if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                               sender, answer, ORTE_RML_TAG_TOOL,
                                               orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(ret);
            OBJ_RELEASE(answer);
        }
        break;

        /****     REPORT_JOB_INFO_CMD COMMAND    ****/
    case ORTE_DAEMON_REPORT_JOB_INFO_CMD:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received job info query",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }
        /* if we are not the HNP, we can do nothing - report
         * back 0 procs so the tool won't hang
         */
        if (!ORTE_PROC_IS_HNP) {
            int32_t zero=0;

            answer = OBJ_NEW(opal_buffer_t);
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &zero, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
                goto CLEANUP;
            }
            if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                   sender, answer, ORTE_RML_TAG_TOOL,
                                                   orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
            }
        } else {
            /* if we are the HNP, process the request */
            int32_t rc, num_jobs;
            orte_job_t *jobdat;

            /* unpack the jobid */
            n = 1;
            if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &job, &n, ORTE_JOBID))) {
                ORTE_ERROR_LOG(ret);
                goto CLEANUP;
            }

            /* setup return */
            answer = OBJ_NEW(opal_buffer_t);

            /* if they asked for a specific job, then just get that info */
            if (ORTE_JOBID_WILDCARD != job) {
                job = ORTE_CONSTRUCT_LOCAL_JOBID(ORTE_PROC_MY_NAME->jobid, job);
                if (NULL != (jobdat = orte_get_job_data_object(job))) {
                    num_jobs = 1;
                    if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &num_jobs, 1, OPAL_INT32))) {
                        ORTE_ERROR_LOG(ret);
                        OBJ_RELEASE(answer);
                        goto CLEANUP;
                    }
                    if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &jobdat, 1, ORTE_JOB))) {
                        ORTE_ERROR_LOG(ret);
                        OBJ_RELEASE(answer);
                        goto CLEANUP;
                    }
                } else {
                    /* if we get here, then send a zero answer */
                    num_jobs = 0;
                    if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &num_jobs, 1, OPAL_INT32))) {
                        ORTE_ERROR_LOG(ret);
                        OBJ_RELEASE(answer);
                        goto CLEANUP;
                    }
                }
            } else {
                uint32_t u32;
                void *nptr;
                num_jobs = opal_hash_table_get_size(orte_job_data);
                /* pack the number of jobs */
                if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &num_jobs, 1, OPAL_INT32))) {
                    ORTE_ERROR_LOG(ret);
                    OBJ_RELEASE(answer);
                    goto CLEANUP;
                }
                /* now pack the data, one at a time */
                rc = opal_hash_table_get_first_key_uint32(orte_job_data, &u32, (void **)&jobdat, &nptr);
                while (OPAL_SUCCESS == rc) {
                    if (NULL != jobdat) {
                        /* pack the job struct */
                        if (ORTE_SUCCESS != (rc = opal_dss.pack(answer, &jobdat, 1, ORTE_JOB))) {
                            ORTE_ERROR_LOG(ret);
                            OBJ_RELEASE(answer);
                            goto CLEANUP;
                        }
                        ++num_jobs;
                    }
                    rc = opal_hash_table_get_next_key_uint32(orte_job_data, &u32, (void **)&jobdat, nptr, &nptr);
                }
            }
            if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                   sender, answer, ORTE_RML_TAG_TOOL,
                                                   orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
            }
        }
        break;

        /****     REPORT_NODE_INFO_CMD COMMAND    ****/
    case ORTE_DAEMON_REPORT_NODE_INFO_CMD:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received node info query",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }
        /* if we are not the HNP, we can do nothing - report
         * back 0 nodes so the tool won't hang
         */
        if (!ORTE_PROC_IS_HNP) {
            int32_t zero=0;

            answer = OBJ_NEW(opal_buffer_t);
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &zero, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
                goto CLEANUP;
            }
            if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                   sender, answer, ORTE_RML_TAG_TOOL,
                                                   orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
            }
        } else {
            /* if we are the HNP, process the request */
            int32_t i, num_nodes;
            orte_node_t *node;
            char *nid;

            /* unpack the nodename */
            n = 1;
            if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &nid, &n, OPAL_STRING))) {
                ORTE_ERROR_LOG(ret);
                goto CLEANUP;
            }

            /* setup return */
            answer = OBJ_NEW(opal_buffer_t);
            num_nodes = 0;

            /* if they asked for a specific node, then just get that info */
            if (NULL != nid) {
                /* find this node */
                for (i=0; i < orte_node_pool->size; i++) {
                    if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                        continue;
                    }
                    if (0 == strcmp(nid, node->name)) {
                        num_nodes = 1;
                        break;
                    }
                }
                if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &num_nodes, 1, OPAL_INT32))) {
                    ORTE_ERROR_LOG(ret);
                    OBJ_RELEASE(answer);
                    goto CLEANUP;
                }
                if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &node, 1, ORTE_NODE))) {
                    ORTE_ERROR_LOG(ret);
                    OBJ_RELEASE(answer);
                    goto CLEANUP;
                }
            } else {
                /* count number of nodes */
                for (i=0; i < orte_node_pool->size; i++) {
                    if (NULL != opal_pointer_array_get_item(orte_node_pool, i)) {
                        num_nodes++;
                    }
                }
                /* pack the answer */
                if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &num_nodes, 1, OPAL_INT32))) {
                    ORTE_ERROR_LOG(ret);
                    OBJ_RELEASE(answer);
                    goto CLEANUP;
                }
                /* pack each node separately */
                for (i=0; i < orte_node_pool->size; i++) {
                    if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &node, 1, ORTE_NODE))) {
                            ORTE_ERROR_LOG(ret);
                            OBJ_RELEASE(answer);
                            goto CLEANUP;
                        }
                    }
                }
            }
            /* send the info */
            if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                   sender, answer, ORTE_RML_TAG_TOOL,
                                                   orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
            }
        }
        break;

        /****     REPORT_PROC_INFO_CMD COMMAND    ****/
    case ORTE_DAEMON_REPORT_PROC_INFO_CMD:
        if (orte_debug_daemons_flag) {
            opal_output(0, "%s orted_cmd: received proc info query",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }
        /* if we are not the HNP, we can do nothing - report
         * back 0 procs so the tool won't hang
         */
        if (!ORTE_PROC_IS_HNP) {
            int32_t zero=0;

            answer = OBJ_NEW(opal_buffer_t);
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &zero, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
                goto CLEANUP;
            }
            if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                   sender, answer, ORTE_RML_TAG_TOOL,
                                                   orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
            }
        } else {
            /* if we are the HNP, process the request */
            orte_job_t *jdata;
            orte_proc_t *proc;
            orte_vpid_t vpid;
            int32_t i, num_procs;
            char *nid;

            /* setup the answer */
            answer = OBJ_NEW(opal_buffer_t);

            /* unpack the jobid */
            n = 1;
            if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &job, &n, ORTE_JOBID))) {
                ORTE_ERROR_LOG(ret);
                goto CLEANUP;
            }

            /* look up job data object */
            job = ORTE_CONSTRUCT_LOCAL_JOBID(ORTE_PROC_MY_NAME->jobid, job);
            if (NULL == (jdata = orte_get_job_data_object(job))) {
                ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                goto CLEANUP;
            }

            /* unpack the vpid */
            n = 1;
            if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &vpid, &n, ORTE_VPID))) {
                ORTE_ERROR_LOG(ret);
                goto CLEANUP;
            }


            /* if they asked for a specific proc, then just get that info */
            if (ORTE_VPID_WILDCARD != vpid) {
                /* find this proc */
                for (i=0; i < jdata->procs->size; i++) {
                    if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
                        continue;
                    }
                    if (vpid == proc->name.vpid) {
                        num_procs = 1;
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &num_procs, 1, OPAL_INT32))) {
                            ORTE_ERROR_LOG(ret);
                            goto CLEANUP;
                        }
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &proc, 1, ORTE_PROC))) {
                            ORTE_ERROR_LOG(ret);
                            goto CLEANUP;
                        }
                        /* the vpid and nodename for this proc are no longer packed
                         * in the ORTE_PROC packing routines to save space for other
                         * uses, so we have to pack them separately
                         */
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &proc->pid, 1, OPAL_PID))) {
                            ORTE_ERROR_LOG(ret);
                            goto CLEANUP;
                        }
                        if (NULL == proc->node) {
                            nid = "UNKNOWN";
                        } else {
                            nid = proc->node->name;
                        }
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &nid, 1, OPAL_STRING))) {
                            ORTE_ERROR_LOG(ret);
                            goto CLEANUP;
                        }
                        break;
                    }
                }
            } else {
                /* count number of procs */
                num_procs = 0;
                for (i=0; i < jdata->procs->size; i++) {
                    if (NULL != opal_pointer_array_get_item(jdata->procs, i)) {
                        num_procs++;
                    }
                }
                /* pack the answer */
                if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &num_procs, 1, OPAL_INT32))) {
                    ORTE_ERROR_LOG(ret);
                    OBJ_RELEASE(answer);
                    goto CLEANUP;
                }
                /* pack each proc separately */
                for (i=0; i < jdata->procs->size; i++) {
                    if (NULL != (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &proc, 1, ORTE_PROC))) {
                            ORTE_ERROR_LOG(ret);
                            OBJ_RELEASE(answer);
                            goto CLEANUP;
                        }
                        /* the vpid and nodename for this proc are no longer packed
                         * in the ORTE_PROC packing routines to save space for other
                         * uses, so we have to pack them separately
                         */
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &proc->pid, 1, OPAL_PID))) {
                            ORTE_ERROR_LOG(ret);
                            goto CLEANUP;
                        }
                        if (NULL == proc->node) {
                            nid = "UNKNOWN";
                        } else {
                            nid = proc->node->name;
                        }
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &nid, 1, OPAL_STRING))) {
                            ORTE_ERROR_LOG(ret);
                            goto CLEANUP;
                        }
                    }
                }
            }
            /* send the info */
            if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                   sender, answer, ORTE_RML_TAG_TOOL,
                                                   orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
            }
        }
        break;

        /****     HEARTBEAT COMMAND    ****/
    case ORTE_DAEMON_HEARTBEAT_CMD:
        ORTE_ERROR_LOG(ORTE_ERR_NOT_IMPLEMENTED);
        ret = ORTE_ERR_NOT_IMPLEMENTED;
        break;

        /****     TOP COMMAND     ****/
    case ORTE_DAEMON_TOP_CMD:
        /* setup the answer */
        answer = OBJ_NEW(opal_buffer_t);
        num_replies = 0;
        hnp_accounted_for = false;

        n = 1;
        return_addr = NULL;
        while (ORTE_SUCCESS == opal_dss.unpack(buffer, &proc, &n, ORTE_NAME)) {
            /* the jobid provided will, of course, have the job family of
             * the requestor. We need to convert that to our own job family
             */
            proc.jobid = ORTE_CONSTRUCT_LOCAL_JOBID(ORTE_PROC_MY_NAME->jobid, proc.jobid);
            if (ORTE_PROC_IS_HNP) {
                return_addr = sender;
                proc2.jobid = ORTE_PROC_MY_NAME->jobid;
                /* if the request is for a wildcard vpid, then it goes to every
                 * daemon. For scalability, we should probably xcast this some
                 * day - but for now, we just loop
                 */
                if (ORTE_VPID_WILDCARD == proc.vpid) {
                    /* loop across all daemons */
                    for (proc2.vpid=1; proc2.vpid < orte_process_info.num_procs; proc2.vpid++) {

                        /* setup the cmd */
                        relay_msg = OBJ_NEW(opal_buffer_t);
                        command = ORTE_DAEMON_TOP_CMD;
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(relay_msg, &command, 1, ORTE_DAEMON_CMD))) {
                            ORTE_ERROR_LOG(ret);
                            OBJ_RELEASE(relay_msg);
                            goto SEND_TOP_ANSWER;
                        }
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(relay_msg, &proc, 1, ORTE_NAME))) {
                            ORTE_ERROR_LOG(ret);
                            OBJ_RELEASE(relay_msg);
                            goto SEND_TOP_ANSWER;
                        }
                        if (ORTE_SUCCESS != (ret = opal_dss.pack(relay_msg, sender, 1, ORTE_NAME))) {
                            ORTE_ERROR_LOG(ret);
                            OBJ_RELEASE(relay_msg);
                            goto SEND_TOP_ANSWER;
                        }
                        /* the callback function will release relay_msg buffer */
                        if (0 > orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                        &proc2, relay_msg,
                                                        ORTE_RML_TAG_DAEMON,
                                                        orte_rml_send_callback, NULL)) {
                            ORTE_ERROR_LOG(ORTE_ERR_COMM_FAILURE);
                            OBJ_RELEASE(relay_msg);
                            ret = ORTE_ERR_COMM_FAILURE;
                        }
                        num_replies++;
                    }
                    /* account for our own reply */
                    if (!hnp_accounted_for) {
                        hnp_accounted_for = true;
                        num_replies++;
                    }
                    /* now get the data for my own procs */
                    goto GET_TOP;
                } else {
                    /* this is for a single proc - see which daemon
                     * this rank is on
                     */
                    if (ORTE_VPID_INVALID == (proc2.vpid = orte_get_proc_daemon_vpid(&proc))) {
                        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                        goto SEND_TOP_ANSWER;
                    }
                    /* if the vpid is me, then just handle this myself */
                    if (proc2.vpid == ORTE_PROC_MY_NAME->vpid) {
                        if (!hnp_accounted_for) {
                            hnp_accounted_for = true;
                            num_replies++;
                        }
                        goto GET_TOP;
                    }
                    /* otherwise, forward the cmd on to the appropriate daemon */
                    relay_msg = OBJ_NEW(opal_buffer_t);
                    command = ORTE_DAEMON_TOP_CMD;
                    if (ORTE_SUCCESS != (ret = opal_dss.pack(relay_msg, &command, 1, ORTE_DAEMON_CMD))) {
                        ORTE_ERROR_LOG(ret);
                        OBJ_RELEASE(relay_msg);
                        goto SEND_TOP_ANSWER;
                    }
                    if (ORTE_SUCCESS != (ret = opal_dss.pack(relay_msg, &proc, 1, ORTE_NAME))) {
                        ORTE_ERROR_LOG(ret);
                        OBJ_RELEASE(relay_msg);
                        goto SEND_TOP_ANSWER;
                    }
                    if (ORTE_SUCCESS != (ret = opal_dss.pack(relay_msg, sender, 1, ORTE_NAME))) {
                        ORTE_ERROR_LOG(ret);
                        OBJ_RELEASE(relay_msg);
                        goto SEND_TOP_ANSWER;
                    }
                    /* the callback function will release relay_msg buffer */
                    if (0 > orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                    &proc2, relay_msg,
                                                    ORTE_RML_TAG_DAEMON,
                                                    orte_rml_send_callback, NULL)) {
                        ORTE_ERROR_LOG(ORTE_ERR_COMM_FAILURE);
                        OBJ_RELEASE(relay_msg);
                        ret = ORTE_ERR_COMM_FAILURE;
                    }
                }
                /* end if HNP */
            } else {
                /* this came from the HNP, but needs to go back to the original
                 * requestor. Unpack the name of that entity first
                 */
                n = 1;
                if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &proc2, &n, ORTE_NAME))) {
                    ORTE_ERROR_LOG(ret);
                    /* in this case, we are helpless - we have no idea who to send an
                     * error message TO! All we can do is return - the tool that sent
                     * this request is going to hang, but there isn't anything we can
                     * do about it
                     */
                    goto CLEANUP;
                }
                return_addr = &proc2;
            GET_TOP:
                /* this rank must be local to me, or the HNP wouldn't
                 * have sent it to me - process the request
                 */
                if (ORTE_SUCCESS != (ret = orte_odls_base_get_proc_stats(answer, &proc))) {
                    ORTE_ERROR_LOG(ret);
                    goto SEND_TOP_ANSWER;
                }
            }
        }
    SEND_TOP_ANSWER:
        /* send the answer back to requester */
        if (ORTE_PROC_IS_HNP) {
            /* if I am the HNP, I need to also provide the number of
             * replies the caller should recv and the sample time
             */
            time_t mytime;
            char *cptr;

            relay_msg = OBJ_NEW(opal_buffer_t);
            if (ORTE_SUCCESS != (ret = opal_dss.pack(relay_msg, &num_replies, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(ret);
            }
            time(&mytime);
            cptr = ctime(&mytime);
            cptr[strlen(cptr)-1] = '\0';  /* remove trailing newline */
            if (ORTE_SUCCESS != (ret = opal_dss.pack(relay_msg, &cptr, 1, OPAL_STRING))) {
                ORTE_ERROR_LOG(ret);
            }
            /* copy the stats payload */
            opal_dss.copy_payload(relay_msg, answer);
            OBJ_RELEASE(answer);
            answer = relay_msg;
        }
        /* if we don't have a return address, then we are helpless */
        if (NULL == return_addr) {
            ORTE_ERROR_LOG(ORTE_ERR_COMM_FAILURE);
            ret = ORTE_ERR_COMM_FAILURE;
            break;
        }
        if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                               return_addr, answer, ORTE_RML_TAG_TOOL,
                                               orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(ret);
            OBJ_RELEASE(answer);
        }
        break;

    case ORTE_DAEMON_GET_STACK_TRACES:
        /* prep the response */
        answer = OBJ_NEW(opal_buffer_t);
        pathptr = path;

        // Try to find the "gstack" executable.  Failure to find the
        // executable will be handled below, because the receiver
        // expects to have the process name, hostname, and PID in the
        // buffer before finding an error message.
        char *gstack_exec;
        gstack_exec = opal_find_absolute_path("gstack");

        /* hit each local process with a gstack command */
        for (i=0; i < orte_local_children->size; i++) {
            if (NULL != (proct = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i)) &&
                ORTE_FLAG_TEST(proct, ORTE_PROC_FLAG_ALIVE)) {
                relay_msg = OBJ_NEW(opal_buffer_t);
                if (OPAL_SUCCESS != opal_dss.pack(relay_msg, &proct->name, 1, ORTE_NAME) ||
                    OPAL_SUCCESS != opal_dss.pack(relay_msg, &proct->node->name, 1, OPAL_STRING) ||
                    OPAL_SUCCESS != opal_dss.pack(relay_msg, &proct->pid, 1, OPAL_PID)) {
                    OBJ_RELEASE(relay_msg);
                    break;
                }

                // If we were able to find the gstack executable,
                // above, then run the command here.
                fp = NULL;
                if (NULL != gstack_exec) {
                    (void) snprintf(gscmd, sizeof(gscmd), "%s %lu",
                                    gstack_exec, (unsigned long) proct->pid);
                    fp = popen(gscmd, "r");
                }

                // If either we weren't able to find or run the gstack
                // exectuable, send back a nice error message here.
                if (NULL == gstack_exec || NULL == fp) {
                    (void) snprintf(string, sizeof(string),
                                    "Failed to %s \"%s\" on %s to obtain stack traces",
                                    (NULL == gstack_exec) ? "find" : "run",
                                    (NULL == gstack_exec) ? "gstack" : gstack_exec,
                                    proct->node->name);
                    if (OPAL_SUCCESS ==
                        opal_dss.pack(relay_msg, &string_ptr, 1, OPAL_STRING)) {
                        opal_dss.pack(answer, &relay_msg, 1, OPAL_BUFFER);
                    }
                    OBJ_RELEASE(relay_msg);
                    break;
                }
                /* Read the output a line at a time and pack it for transmission */
                memset(path, 0, sizeof(path));
                while (fgets(path, sizeof(path)-1, fp) != NULL) {
                    if (OPAL_SUCCESS != opal_dss.pack(relay_msg, &pathptr, 1, OPAL_STRING)) {
                        OBJ_RELEASE(relay_msg);
                        break;
                    }
                    memset(path, 0, sizeof(path));
                }
                /* close */
                pclose(fp);
                /* transfer this load */
                if (OPAL_SUCCESS != opal_dss.pack(answer, &relay_msg, 1, OPAL_BUFFER)) {
                    OBJ_RELEASE(relay_msg);
                    break;
                }
                OBJ_RELEASE(relay_msg);
            }
        }
        if (NULL != gstack_exec) {
            free(gstack_exec);
        }
        /* always send our response */
        if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                               ORTE_PROC_MY_HNP, answer,
                                               ORTE_RML_TAG_STACK_TRACE,
                                               orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(ret);
            OBJ_RELEASE(answer);
        }
        break;

    case ORTE_DAEMON_GET_MEMPROFILE:
        answer = OBJ_NEW(opal_buffer_t);
        /* pack our hostname so they know where it came from */
        opal_dss.pack(answer, &orte_process_info.nodename, 1, OPAL_STRING);
        /* collect my memory usage */
        OBJ_CONSTRUCT(&pstat, opal_pstats_t);
        opal_pstat.query(orte_process_info.pid, &pstat, NULL);
        opal_dss.pack(answer, &pstat.pss, 1, OPAL_FLOAT);
        OBJ_DESTRUCT(&pstat);
        /* collect the memory usage of all my children */
        pss = 0.0;
        num_replies = 0;
        for (i=0; i < orte_local_children->size; i++) {
            if (NULL != (proct = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i)) &&
                ORTE_FLAG_TEST(proct, ORTE_PROC_FLAG_ALIVE)) {
                /* collect the stats on this proc */
                OBJ_CONSTRUCT(&pstat, opal_pstats_t);
                if (OPAL_SUCCESS == opal_pstat.query(proct->pid, &pstat, NULL)) {
                    pss += pstat.pss;
                    ++num_replies;
                }
                OBJ_DESTRUCT(&pstat);
            }
        }
        /* compute the average value */
        if (0 < num_replies) {
            pss /= (float)num_replies;
        }
        opal_dss.pack(answer, &pss, 1, OPAL_FLOAT);
        /* send it back */
        if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                               ORTE_PROC_MY_HNP, answer,
                                               ORTE_RML_TAG_MEMPROFILE,
                                               orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(ret);
            OBJ_RELEASE(answer);
        }
        break;

    default:
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
    }

 CLEANUP:
    return;
}

static char *get_orted_comm_cmd_str(int command)
{
    switch(command) {
    case ORTE_DAEMON_CONTACT_QUERY_CMD:
        return strdup("ORTE_DAEMON_CONTACT_QUERY_CMD");
    case ORTE_DAEMON_KILL_LOCAL_PROCS:
        return strdup("ORTE_DAEMON_KILL_LOCAL_PROCS");
    case ORTE_DAEMON_SIGNAL_LOCAL_PROCS:
        return strdup("ORTE_DAEMON_SIGNAL_LOCAL_PROCS");
    case ORTE_DAEMON_ADD_LOCAL_PROCS:
        return strdup("ORTE_DAEMON_ADD_LOCAL_PROCS");

    case ORTE_DAEMON_HEARTBEAT_CMD:
        return strdup("ORTE_DAEMON_HEARTBEAT_CMD");
    case ORTE_DAEMON_EXIT_CMD:
        return strdup("ORTE_DAEMON_EXIT_CMD");
    case ORTE_DAEMON_PROCESS_AND_RELAY_CMD:
        return strdup("ORTE_DAEMON_PROCESS_AND_RELAY_CMD");
    case ORTE_DAEMON_NULL_CMD:
        return strdup("NULL");

    case ORTE_DAEMON_REPORT_JOB_INFO_CMD:
        return strdup("ORTE_DAEMON_REPORT_JOB_INFO_CMD");
    case ORTE_DAEMON_REPORT_NODE_INFO_CMD:
        return strdup("ORTE_DAEMON_REPORT_NODE_INFO_CMD");
    case ORTE_DAEMON_REPORT_PROC_INFO_CMD:
        return strdup("ORTE_DAEMON_REPORT_PROC_INFO_CMD");
    case ORTE_DAEMON_SPAWN_JOB_CMD:
        return strdup("ORTE_DAEMON_SPAWN_JOB_CMD");
    case ORTE_DAEMON_TERMINATE_JOB_CMD:
        return strdup("ORTE_DAEMON_TERMINATE_JOB_CMD");

    case ORTE_DAEMON_HALT_VM_CMD:
        return strdup("ORTE_DAEMON_HALT_VM_CMD");
    case ORTE_DAEMON_HALT_DVM_CMD:
        return strdup("ORTE_DAEMON_HALT_DVM_CMD");
    case ORTE_DAEMON_REPORT_JOB_COMPLETE:
        return strdup("ORTE_DAEMON_REPORT_JOB_COMPLETE");

    case ORTE_DAEMON_TOP_CMD:
        return strdup("ORTE_DAEMON_TOP_CMD");
    case ORTE_DAEMON_NAME_REQ_CMD:
        return strdup("ORTE_DAEMON_NAME_REQ_CMD");
    case ORTE_DAEMON_CHECKIN_CMD:
        return strdup("ORTE_DAEMON_CHECKIN_CMD");

    case ORTE_TOOL_CHECKIN_CMD:
        return strdup("ORTE_TOOL_CHECKIN_CMD");
    case ORTE_DAEMON_PROCESS_CMD:
        return strdup("ORTE_DAEMON_PROCESS_CMD");
    case ORTE_DAEMON_ABORT_PROCS_CALLED:
        return strdup("ORTE_DAEMON_ABORT_PROCS_CALLED");

    case ORTE_DAEMON_DVM_NIDMAP_CMD:
        return strdup("ORTE_DAEMON_DVM_NIDMAP_CMD");
    case ORTE_DAEMON_DVM_ADD_PROCS:
        return strdup("ORTE_DAEMON_DVM_ADD_PROCS");

    case ORTE_DAEMON_GET_STACK_TRACES:
        return strdup("ORTE_DAEMON_GET_STACK_TRACES");

    case ORTE_DAEMON_GET_MEMPROFILE:
        return strdup("ORTE_DAEMON_GET_MEMPROFILE");

    case ORTE_DAEMON_DVM_CLEANUP_JOB_CMD:
        return strdup("ORTE_DAEMON_DVM_CLEANUP_JOB_CMD");

    default:
        return strdup("Unknown Command!");
    }
}
