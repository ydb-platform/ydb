/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2007      The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC. All
 *                         rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include <string.h>

#include "opal/dss/dss.h"
#include "opal/class/opal_list.h"
#include "opal/mca/pmix/pmix.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/regx/regx.h"
#include "orte/mca/rml/base/base.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/routed/base/base.h"
#include "orte/mca/state/state.h"
#include "orte/util/compress.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"

#include "orte/mca/grpcomm/base/base.h"
#include "grpcomm_direct.h"


/* Static API's */
static int init(void);
static void finalize(void);
static int xcast(orte_vpid_t *vpids,
                 size_t nprocs,
                 opal_buffer_t *buf);
static int allgather(orte_grpcomm_coll_t *coll,
                     opal_buffer_t *buf);

/* Module def */
orte_grpcomm_base_module_t orte_grpcomm_direct_module = {
    init,
    finalize,
    xcast,
    allgather
};

/* internal functions */
static void xcast_recv(int status, orte_process_name_t* sender,
                       opal_buffer_t* buffer, orte_rml_tag_t tag,
                       void* cbdata);
static void allgather_recv(int status, orte_process_name_t* sender,
                           opal_buffer_t* buffer, orte_rml_tag_t tag,
                           void* cbdata);
static void barrier_release(int status, orte_process_name_t* sender,
                            opal_buffer_t* buffer, orte_rml_tag_t tag,
                            void* cbdata);

/* internal variables */
static opal_list_t tracker;

/**
 * Initialize the module
 */
static int init(void)
{
    OBJ_CONSTRUCT(&tracker, opal_list_t);

    /* post the receives */
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                            ORTE_RML_TAG_XCAST,
                            ORTE_RML_PERSISTENT,
                            xcast_recv, NULL);
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                            ORTE_RML_TAG_ALLGATHER_DIRECT,
                            ORTE_RML_PERSISTENT,
                            allgather_recv, NULL);
    /* setup recv for barrier release */
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                            ORTE_RML_TAG_COLL_RELEASE,
                            ORTE_RML_PERSISTENT,
                            barrier_release, NULL);

    return OPAL_SUCCESS;
}

/**
 * Finalize the module
 */
static void finalize(void)
{
    OPAL_LIST_DESTRUCT(&tracker);
    return;
}

static int xcast(orte_vpid_t *vpids,
                 size_t nprocs,
                 opal_buffer_t *buf)
{
    int rc;

    /* send it to the HNP (could be myself) for relay */
    OBJ_RETAIN(buf);  // we'll let the RML release it
    if (0 > (rc = orte_rml.send_buffer_nb(orte_coll_conduit,
                                          ORTE_PROC_MY_HNP, buf, ORTE_RML_TAG_XCAST,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }
    return ORTE_SUCCESS;
}

static int allgather(orte_grpcomm_coll_t *coll,
                     opal_buffer_t *buf)
{
    int rc;
    opal_buffer_t *relay;

    OPAL_OUTPUT_VERBOSE((1, orte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:direct: allgather",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* the base functions pushed us into the event library
     * before calling us, so we can safely access global data
     * at this point */

    relay = OBJ_NEW(opal_buffer_t);
    /* pack the signature */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(relay, &coll->sig, 1, ORTE_SIGNATURE))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(relay);
        return rc;
    }

    /* pass along the payload */
    opal_dss.copy_payload(relay, buf);

    /* send this to ourselves for processing */
    OPAL_OUTPUT_VERBOSE((1, orte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:direct:allgather sending to ourself",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* send the info to ourselves for tracking */
    rc = orte_rml.send_buffer_nb(orte_coll_conduit,
                                 ORTE_PROC_MY_NAME, relay,
                                 ORTE_RML_TAG_ALLGATHER_DIRECT,
                                 orte_rml_send_callback, NULL);
    return rc;
}

static void allgather_recv(int status, orte_process_name_t* sender,
                           opal_buffer_t* buffer, orte_rml_tag_t tag,
                           void* cbdata)
{
    int32_t cnt;
    int rc, ret;
    orte_grpcomm_signature_t *sig;
    opal_buffer_t *reply;
    orte_grpcomm_coll_t *coll;

    OPAL_OUTPUT_VERBOSE((1, orte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:direct allgather recvd from %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(sender)));

    /* unpack the signature */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &sig, &cnt, ORTE_SIGNATURE))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* check for the tracker and create it if not found */
    if (NULL == (coll = orte_grpcomm_base_get_tracker(sig, true))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        OBJ_RELEASE(sig);
        return;
    }

    /* increment nprocs reported for collective */
    coll->nreported++;
    /* capture any provided content */
    opal_dss.copy_payload(&coll->bucket, buffer);

    OPAL_OUTPUT_VERBOSE((1, orte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:direct allgather recv nexpected %d nrep %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (int)coll->nexpected, (int)coll->nreported));

    /* see if everyone has reported */
    if (coll->nreported == coll->nexpected) {
        if (ORTE_PROC_IS_HNP) {
            OPAL_OUTPUT_VERBOSE((1, orte_grpcomm_base_framework.framework_output,
                                 "%s grpcomm:direct allgather HNP reports complete",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            /* the allgather is complete - send the xcast */
            reply = OBJ_NEW(opal_buffer_t);
            /* pack the signature */
            if (OPAL_SUCCESS != (rc = opal_dss.pack(reply, &sig, 1, ORTE_SIGNATURE))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(reply);
                OBJ_RELEASE(sig);
                return;
            }
            /* pack the status - success since the allgather completed. This
             * would be an error if we timeout instead */
            ret = ORTE_SUCCESS;
            if (OPAL_SUCCESS != (rc = opal_dss.pack(reply, &ret, 1, OPAL_INT))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(reply);
                OBJ_RELEASE(sig);
                return;
            }
            /* transfer the collected bucket */
            opal_dss.copy_payload(reply, &coll->bucket);
            /* send the release via xcast */
            (void)orte_grpcomm.xcast(sig, ORTE_RML_TAG_COLL_RELEASE, reply);
            OBJ_RELEASE(reply);
        } else {
            OPAL_OUTPUT_VERBOSE((1, orte_grpcomm_base_framework.framework_output,
                                 "%s grpcomm:direct allgather rollup complete - sending to %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_PARENT)));
            /* relay the bucket upward */
            reply = OBJ_NEW(opal_buffer_t);
            /* pack the signature */
            if (OPAL_SUCCESS != (rc = opal_dss.pack(reply, &sig, 1, ORTE_SIGNATURE))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(reply);
                OBJ_RELEASE(sig);
                return;
            }
            /* transfer the collected bucket */
            opal_dss.copy_payload(reply, &coll->bucket);
            /* send the info to our parent */
            rc = orte_rml.send_buffer_nb(orte_coll_conduit,
                                         ORTE_PROC_MY_PARENT, reply,
                                         ORTE_RML_TAG_ALLGATHER_DIRECT,
                                         orte_rml_send_callback, NULL);
        }
    }
    OBJ_RELEASE(sig);
}

static void xcast_recv(int status, orte_process_name_t* sender,
                       opal_buffer_t* buffer, orte_rml_tag_t tg,
                       void* cbdata)
{
    opal_list_item_t *item;
    orte_namelist_t *nm;
    int ret, cnt;
    opal_buffer_t *relay=NULL, *rly;
    orte_daemon_cmd_flag_t command = ORTE_DAEMON_NULL_CMD;
    opal_buffer_t wireup, datbuf, *data;
    opal_byte_object_t *bo;
    int8_t flag;
    orte_job_t *jdata;
    orte_proc_t *rec;
    opal_list_t coll;
    orte_grpcomm_signature_t *sig;
    orte_rml_tag_t tag;
    char *rtmod, *nidmap;
    size_t inlen, cmplen;
    uint8_t *packed_data, *cmpdata;
    int32_t nvals, i;
    opal_value_t kv, *kval;
    orte_process_name_t dmn;

    OPAL_OUTPUT_VERBOSE((1, orte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:direct:xcast:recv: with %d bytes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (int)buffer->bytes_used));

    /* we need a passthru buffer to send to our children - we leave it
     * as compressed data */
    rly = OBJ_NEW(opal_buffer_t);
    opal_dss.copy_payload(rly, buffer);
    OBJ_CONSTRUCT(&datbuf, opal_buffer_t);
    /* setup the relay list */
    OBJ_CONSTRUCT(&coll, opal_list_t);

    /* unpack the flag to see if this payload is compressed */
    cnt=1;
    if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &flag, &cnt, OPAL_INT8))) {
        ORTE_ERROR_LOG(ret);
        ORTE_FORCED_TERMINATE(ret);
        OBJ_DESTRUCT(&datbuf);
        OBJ_DESTRUCT(&coll);
        OBJ_RELEASE(rly);
        return;
    }
    if (flag) {
        /* unpack the data size */
        cnt=1;
        if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &inlen, &cnt, OPAL_SIZE))) {
            ORTE_ERROR_LOG(ret);
            ORTE_FORCED_TERMINATE(ret);
            OBJ_DESTRUCT(&datbuf);
            OBJ_DESTRUCT(&coll);
            OBJ_RELEASE(rly);
            return;
        }
        /* unpack the unpacked data size */
        cnt=1;
        if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &cmplen, &cnt, OPAL_SIZE))) {
            ORTE_ERROR_LOG(ret);
            ORTE_FORCED_TERMINATE(ret);
            OBJ_DESTRUCT(&datbuf);
            OBJ_DESTRUCT(&coll);
            OBJ_RELEASE(rly);
            return;
        }
        /* allocate the space */
        packed_data = (uint8_t*)malloc(inlen);
        /* unpack the data blob */
        cnt = inlen;
        if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, packed_data, &cnt, OPAL_UINT8))) {
            ORTE_ERROR_LOG(ret);
            free(packed_data);
            ORTE_FORCED_TERMINATE(ret);
            OBJ_DESTRUCT(&datbuf);
            OBJ_DESTRUCT(&coll);
            OBJ_RELEASE(rly);
            return;
        }
        /* decompress the data */
        if (orte_util_uncompress_block(&cmpdata, cmplen,
                                       packed_data, inlen)) {
            /* the data has been uncompressed */
            opal_dss.load(&datbuf, cmpdata, cmplen);
            data = &datbuf;
        } else {
            data = buffer;
        }
        free(packed_data);
    } else {
        data = buffer;
    }

    /* get the signature that we do not need */
    cnt=1;
    if (ORTE_SUCCESS != (ret = opal_dss.unpack(data, &sig, &cnt, ORTE_SIGNATURE))) {
        ORTE_ERROR_LOG(ret);
        OBJ_DESTRUCT(&datbuf);
        OBJ_DESTRUCT(&coll);
        OBJ_RELEASE(rly);
        ORTE_FORCED_TERMINATE(ret);
        return;
    }
    OBJ_RELEASE(sig);

    /* get the target tag */
    cnt=1;
    if (ORTE_SUCCESS != (ret = opal_dss.unpack(data, &tag, &cnt, ORTE_RML_TAG))) {
        ORTE_ERROR_LOG(ret);
        OBJ_DESTRUCT(&datbuf);
        OBJ_DESTRUCT(&coll);
        OBJ_RELEASE(rly);
        ORTE_FORCED_TERMINATE(ret);
        return;
    }

    /* get our conduit's routed module name */
    rtmod = orte_rml.get_routed(orte_coll_conduit);

    /* if this is headed for the daemon command processor,
     * then we first need to check for add_local_procs
     * as that command includes some needed wireup info */
    if (ORTE_RML_TAG_DAEMON == tag) {
        /* peek at the command */
        cnt=1;
        if (ORTE_SUCCESS == (ret = opal_dss.unpack(data, &command, &cnt, ORTE_DAEMON_CMD))) {
            /* if it is an exit cmd, then flag that we are quitting so we will properly
             * handle connection losses from our downstream peers */
            if (ORTE_DAEMON_EXIT_CMD == command ||
                ORTE_DAEMON_HALT_VM_CMD == command) {
                orte_orteds_term_ordered = true;
                if (ORTE_DAEMON_HALT_VM_CMD == command) {
                    /* this is an abnormal termination */
                    orte_abnormal_term_ordered = true;
                }
                /* copy the msg for relay to ourselves */
                relay = OBJ_NEW(opal_buffer_t);
                /* repack the command */
                if (OPAL_SUCCESS != (ret = opal_dss.pack(relay, &command, 1, ORTE_DAEMON_CMD))) {
                    ORTE_ERROR_LOG(ret);
                    goto relay;
                }
                opal_dss.copy_payload(relay, data);
            } else if (ORTE_DAEMON_ADD_LOCAL_PROCS == command ||
                       ORTE_DAEMON_DVM_NIDMAP_CMD == command ||
                       ORTE_DAEMON_DVM_ADD_PROCS == command) {
                /* setup our internal relay buffer */
                relay = OBJ_NEW(opal_buffer_t);
                /* repack the command */
                if (OPAL_SUCCESS != (ret = opal_dss.pack(relay, &command, 1, ORTE_DAEMON_CMD))) {
                    ORTE_ERROR_LOG(ret);
                    goto relay;
                }
                /* unpack the nidmap string - may be NULL */
                cnt = 1;
                if (OPAL_SUCCESS != (ret = opal_dss.unpack(data, &nidmap, &cnt, OPAL_STRING))) {
                    ORTE_ERROR_LOG(ret);
                    goto relay;
                }
                if (NULL != nidmap) {
                    if (ORTE_SUCCESS != (ret = orte_regx.nidmap_parse(nidmap))) {
                        ORTE_ERROR_LOG(ret);
                        goto relay;
                    }
                    free(nidmap);
                }
                /* see if they included info on node capabilities */
                cnt = 1;
                if (OPAL_SUCCESS != (ret = opal_dss.unpack(data, &flag, &cnt, OPAL_INT8))) {
                    ORTE_ERROR_LOG(ret);
                    goto relay;
                }
                if (0 != flag) {
                    /* update our local nidmap, if required - the decode function
                     * knows what to do
                     */
                    OPAL_OUTPUT_VERBOSE((5, orte_grpcomm_base_framework.framework_output,
                                         "%s grpcomm:direct:xcast updating daemon nidmap",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

                    if (ORTE_SUCCESS != (ret = orte_regx.decode_daemon_nodemap(data))) {
                        ORTE_ERROR_LOG(ret);
                        goto relay;
                    }

                    if (!ORTE_PROC_IS_HNP) {
                        /* update the routing plan - the HNP already did
                         * it when it computed the VM, so don't waste time
                         * re-doing it here */
                        orte_routed.update_routing_plan(rtmod);
                    }
                    /* routing is now possible */
                    orte_routed_base.routing_enabled = true;

                    /* unpack the byte object */
                    cnt=1;
                    if (ORTE_SUCCESS != (ret = opal_dss.unpack(data, &bo, &cnt, OPAL_BYTE_OBJECT))) {
                        ORTE_ERROR_LOG(ret);
                        goto relay;
                    }
                    if (0 < bo->size) {
                        /* load it into a buffer */
                        OBJ_CONSTRUCT(&wireup, opal_buffer_t);
                        opal_dss.load(&wireup, bo->bytes, bo->size);
                        /* decode it, pushing the info into our database */
                        if (opal_pmix.legacy_get()) {
                            OBJ_CONSTRUCT(&kv, opal_value_t);
                            kv.key = OPAL_PMIX_PROC_URI;
                            kv.type = OPAL_STRING;
                            cnt=1;
                            while (OPAL_SUCCESS == (ret = opal_dss.unpack(&wireup, &dmn, &cnt, ORTE_NAME))) {
                                cnt = 1;
                                if (ORTE_SUCCESS != (ret = opal_dss.unpack(&wireup, &kv.data.string, &cnt, OPAL_STRING))) {
                                    ORTE_ERROR_LOG(ret);
                                    break;
                                }
                                if (OPAL_SUCCESS != (ret = opal_pmix.store_local(&dmn, &kv))) {
                                    ORTE_ERROR_LOG(ret);
                                    free(kv.data.string);
                                    break;
                                }
                                free(kv.data.string);
                                kv.data.string = NULL;
                            }
                            if (ORTE_ERR_UNPACK_READ_PAST_END_OF_BUFFER != ret) {
                                ORTE_ERROR_LOG(ret);
                            }
                        } else {
                           cnt=1;
                           while (OPAL_SUCCESS == (ret = opal_dss.unpack(&wireup, &dmn, &cnt, ORTE_NAME))) {
                               cnt = 1;
                               if (ORTE_SUCCESS != (ret = opal_dss.unpack(&wireup, &nvals, &cnt, OPAL_INT32))) {
                                   ORTE_ERROR_LOG(ret);
                                   break;
                               }
                               for (i=0; i < nvals; i++) {
                                cnt = 1;
                                if (ORTE_SUCCESS != (ret = opal_dss.unpack(&wireup, &kval, &cnt, OPAL_VALUE))) {
                                    ORTE_ERROR_LOG(ret);
                                    break;
                                }
                                OPAL_OUTPUT_VERBOSE((5, orte_grpcomm_base_framework.framework_output,
                                                     "%s STORING MODEX DATA FOR PROC %s KEY %s",
                                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                                     ORTE_NAME_PRINT(&dmn), kval->key));
                                if (OPAL_SUCCESS != (ret = opal_pmix.store_local(&dmn, kval))) {
                                    ORTE_ERROR_LOG(ret);
                                    OBJ_RELEASE(kval);
                                    break;
                                }
                                OBJ_RELEASE(kval);
                            }
                            }
                            if (ORTE_ERR_UNPACK_READ_PAST_END_OF_BUFFER != ret) {
                                ORTE_ERROR_LOG(ret);
                            }
                        }
                        /* done with the wireup buffer - dump it */
                        OBJ_DESTRUCT(&wireup);
                    }
                    free(bo);
                }
                /* copy the remainder of the payload - we don't pass wiring info
                 * to the odls */
                opal_dss.copy_payload(relay, data);
            } else {
                relay = OBJ_NEW(opal_buffer_t);
                /* repack the command */
                if (OPAL_SUCCESS != (ret = opal_dss.pack(relay, &command, 1, ORTE_DAEMON_CMD))) {
                    ORTE_ERROR_LOG(ret);
                    goto relay;
                }
                /* copy the msg for relay to ourselves */
                opal_dss.copy_payload(relay, data);
            }
        } else {
            ORTE_ERROR_LOG(ret);
            goto CLEANUP;
        }
    } else {
        /* copy the msg for relay to ourselves */
        relay = OBJ_NEW(opal_buffer_t);
        opal_dss.copy_payload(relay, data);
    }

  relay:
    if (!orte_do_not_launch) {
        /* get the list of next recipients from the routed module */
        orte_routed.get_routing_list(rtmod, &coll);

        /* if list is empty, no relay is required */
        if (opal_list_is_empty(&coll)) {
            OPAL_OUTPUT_VERBOSE((5, orte_grpcomm_base_framework.framework_output,
                                 "%s grpcomm:direct:send_relay - recipient list is empty!",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            goto CLEANUP;
        }

        /* send the message to each recipient on list, deconstructing it as we go */
        while (NULL != (item = opal_list_remove_first(&coll))) {
            nm = (orte_namelist_t*)item;

            OPAL_OUTPUT_VERBOSE((5, orte_grpcomm_base_framework.framework_output,
                                 "%s grpcomm:direct:send_relay sending relay msg of %d bytes to %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (int)rly->bytes_used,
                                 ORTE_NAME_PRINT(&nm->name)));
            OBJ_RETAIN(rly);
            /* check the state of the recipient - no point
             * sending to someone not alive
             */
            jdata = orte_get_job_data_object(nm->name.jobid);
            if (NULL == (rec = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, nm->name.vpid))) {
                if (!orte_abnormal_term_ordered && !orte_orteds_term_ordered) {
                    opal_output(0, "%s grpcomm:direct:send_relay proc %s not found - cannot relay",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&nm->name));
                }
                OBJ_RELEASE(rly);
                OBJ_RELEASE(item);
                ORTE_FORCED_TERMINATE(ORTE_ERR_UNREACH);
                continue;
            }
            if ((ORTE_PROC_STATE_RUNNING < rec->state &&
                ORTE_PROC_STATE_CALLED_ABORT != rec->state) ||
                !ORTE_FLAG_TEST(rec, ORTE_PROC_FLAG_ALIVE)) {
                if (!orte_abnormal_term_ordered && !orte_orteds_term_ordered) {
                    opal_output(0, "%s grpcomm:direct:send_relay proc %s not running - cannot relay: %s ",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&nm->name),
                                ORTE_FLAG_TEST(rec, ORTE_PROC_FLAG_ALIVE) ? orte_proc_state_to_str(rec->state) : "NOT ALIVE");
                }
                OBJ_RELEASE(rly);
                OBJ_RELEASE(item);
                ORTE_FORCED_TERMINATE(ORTE_ERR_UNREACH);
                continue;
            }
            if (ORTE_SUCCESS != (ret = orte_rml.send_buffer_nb(orte_coll_conduit,
                                                               &nm->name, rly, ORTE_RML_TAG_XCAST,
                                                               orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(rly);
                OBJ_RELEASE(item);
                ORTE_FORCED_TERMINATE(ORTE_ERR_UNREACH);
                continue;
            }
            OBJ_RELEASE(item);
        }
    }

 CLEANUP:
    /* cleanup */
    OPAL_LIST_DESTRUCT(&coll);
    OBJ_RELEASE(rly);  // retain accounting

    /* now pass the relay buffer to myself for processing - don't
     * inject it into the RML system via send as that will compete
     * with the relay messages down in the OOB. Instead, pass it
     * directly to the RML message processor */
    if (ORTE_DAEMON_DVM_NIDMAP_CMD != command) {
        ORTE_RML_POST_MESSAGE(ORTE_PROC_MY_NAME, tag, 1,
                              relay->base_ptr, relay->bytes_used);
        relay->base_ptr = NULL;
        relay->bytes_used = 0;
    }
    if (NULL != relay) {
        OBJ_RELEASE(relay);
    }
    OBJ_DESTRUCT(&datbuf);
}

static void barrier_release(int status, orte_process_name_t* sender,
                            opal_buffer_t* buffer, orte_rml_tag_t tag,
                            void* cbdata)
{
    int32_t cnt;
    int rc, ret;
    orte_grpcomm_signature_t *sig;
    orte_grpcomm_coll_t *coll;

    OPAL_OUTPUT_VERBOSE((5, orte_grpcomm_base_framework.framework_output,
                         "%s grpcomm:direct: barrier release called with %d bytes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (int)buffer->bytes_used));

    /* unpack the signature */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &sig, &cnt, ORTE_SIGNATURE))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* unpack the return status */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &ret, &cnt, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* check for the tracker - it is not an error if not
     * found as that just means we wre not involved
     * in the collective */
    if (NULL == (coll = orte_grpcomm_base_get_tracker(sig, false))) {
        OBJ_RELEASE(sig);
        return;
    }

    /* execute the callback */
    if (NULL != coll->cbfunc) {
        coll->cbfunc(ret, buffer, coll->cbdata);
    }
    opal_list_remove_item(&orte_grpcomm_base.ongoing, &coll->super);
    OBJ_RELEASE(coll);
    OBJ_RELEASE(sig);
}
