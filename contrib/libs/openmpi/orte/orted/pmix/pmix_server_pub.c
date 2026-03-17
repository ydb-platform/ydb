/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/dss/dss.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_data_server.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/base/rml_contact.h"

#include "pmix_server_internal.h"

static int init_server(void)
{
    char *server;
    opal_value_t val;
    char input[1024], *filename;
    FILE *fp;
    int rc;

    /* only do this once */
    orte_pmix_server_globals.pubsub_init = true;

    /* if the universal server wasn't specified, then we use
     * our own HNP for that purpose */
    if (NULL == orte_data_server_uri) {
        orte_pmix_server_globals.server = *ORTE_PROC_MY_HNP;
    } else {
        if (0 == strncmp(orte_data_server_uri, "file", strlen("file")) ||
            0 == strncmp(orte_data_server_uri, "FILE", strlen("FILE"))) {
            /* it is a file - get the filename */
            filename = strchr(orte_data_server_uri, ':');
            if (NULL == filename) {
                /* filename is not correctly formatted */
                orte_show_help("help-orterun.txt", "orterun:ompi-server-filename-bad", true,
                               orte_basename, orte_data_server_uri);
                return ORTE_ERR_BAD_PARAM;
            }
            ++filename; /* space past the : */

            if (0 >= strlen(filename)) {
                /* they forgot to give us the name! */
                orte_show_help("help-orterun.txt", "orterun:ompi-server-filename-missing", true,
                               orte_basename, orte_data_server_uri);
                return ORTE_ERR_BAD_PARAM;
            }

            /* open the file and extract the uri */
            fp = fopen(filename, "r");
            if (NULL == fp) { /* can't find or read file! */
                orte_show_help("help-orterun.txt", "orterun:ompi-server-filename-access", true,
                               orte_basename, orte_data_server_uri);
                return ORTE_ERR_BAD_PARAM;
            }
            if (NULL == fgets(input, 1024, fp)) {
                /* something malformed about file */
                fclose(fp);
                orte_show_help("help-orterun.txt", "orterun:ompi-server-file-bad", true,
                               orte_basename, orte_data_server_uri,
                               orte_basename);
                return ORTE_ERR_BAD_PARAM;
            }
            fclose(fp);
            input[strlen(input)-1] = '\0';  /* remove newline */
            server = strdup(input);
        } else {
            server = strdup(orte_data_server_uri);
        }
        /* parse the URI to get the server's name */
        if (ORTE_SUCCESS != (rc = orte_rml_base_parse_uris(server, &orte_pmix_server_globals.server, NULL))) {
            ORTE_ERROR_LOG(rc);
            free(server);
            return rc;
        }
        /* setup our route to the server */
        OBJ_CONSTRUCT(&val, opal_value_t);
        val.key = OPAL_PMIX_PROC_URI;
        val.type = OPAL_STRING;
        val.data.string = server;
        if (OPAL_SUCCESS != (rc = opal_pmix.store_local(&orte_pmix_server_globals.server, &val))) {
            ORTE_ERROR_LOG(rc);
            val.key = NULL;
            OBJ_DESTRUCT(&val);
            return rc;
        }
        val.key = NULL;
        OBJ_DESTRUCT(&val);

        /* check if we are to wait for the server to start - resolves
         * a race condition that can occur when the server is run
         * as a background job - e.g., in scripts
         */
        if (orte_pmix_server_globals.wait_for_server) {
            /* ping the server */
            struct timeval timeout;
            timeout.tv_sec = orte_pmix_server_globals.timeout;
            timeout.tv_usec = 0;
            if (ORTE_SUCCESS != (rc = orte_rml.ping(orte_mgmt_conduit, server, &timeout))) {
                /* try it one more time */
                if (ORTE_SUCCESS != (rc = orte_rml.ping(orte_mgmt_conduit, server, &timeout))) {
                    /* okay give up */
                    orte_show_help("help-orterun.txt", "orterun:server-not-found", true,
                                   orte_basename, server,
                                   (long)orte_pmix_server_globals.timeout,
                                   ORTE_ERROR_NAME(rc));
                    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    return rc;
                }
            }
        }
    }

    return ORTE_SUCCESS;
}

static void execute(int sd, short args, void *cbdata)
{
    pmix_server_req_t *req = (pmix_server_req_t*)cbdata;
    int rc;
    opal_buffer_t *xfer;
    orte_process_name_t *target;

    ORTE_ACQUIRE_OBJECT(req);

    if (!orte_pmix_server_globals.pubsub_init) {
        /* we need to initialize our connection to the server */
        if (ORTE_SUCCESS != (rc = init_server())) {
            orte_show_help("help-orted.txt", "noserver", true,
                           (NULL == orte_data_server_uri) ?
                           "NULL" : orte_data_server_uri);
            goto callback;
        }
    }

    /* add this request to our tracker hotel */
    if (OPAL_SUCCESS != (rc = opal_hotel_checkin(&orte_pmix_server_globals.reqs, req, &req->room_num))) {
        orte_show_help("help-orted.txt", "noroom", true, req->operation, orte_pmix_server_globals.num_rooms);
        goto callback;
    }

    /* setup the xfer */
    xfer = OBJ_NEW(opal_buffer_t);
    /* pack the room number */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(xfer, &req->room_num, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(xfer);
        goto callback;
    }
    opal_dss.copy_payload(xfer, &req->msg);

    /* if the range is SESSION, then set the target to the global server */
    if (OPAL_PMIX_RANGE_SESSION == req->range) {
        opal_output_verbose(1, orte_pmix_server_globals.output,
                            "%s orted:pmix:server range SESSION",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        target = &orte_pmix_server_globals.server;
    } else if (OPAL_PMIX_RANGE_LOCAL == req->range) {
        /* if the range is local, send it to myself */
        opal_output_verbose(1, orte_pmix_server_globals.output,
                            "%s orted:pmix:server range LOCAL",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        target = ORTE_PROC_MY_NAME;
    } else {
        opal_output_verbose(1, orte_pmix_server_globals.output,
                            "%s orted:pmix:server range GLOBAL",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        target = ORTE_PROC_MY_HNP;
    }

    /* send the request to the target */
    rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                 target, xfer,
                                 ORTE_RML_TAG_DATA_SERVER,
                                 orte_rml_send_callback, NULL);
    if (ORTE_SUCCESS == rc) {
        return;
    }

  callback:
    /* execute the callback to avoid having the client hang */
    if (NULL != req->opcbfunc) {
        req->opcbfunc(rc, req->cbdata);
    } else if (NULL != req->lkcbfunc) {
        req->lkcbfunc(rc, NULL, req->cbdata);
    }
    opal_hotel_checkout(&orte_pmix_server_globals.reqs, req->room_num);
    OBJ_RELEASE(req);
}

int pmix_server_publish_fn(opal_process_name_t *proc,
                           opal_list_t *info,
                           opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_server_req_t *req;
    int rc;
    uint8_t cmd = ORTE_PMIX_PUBLISH_CMD;
    opal_value_t *iptr;
    opal_pmix_persistence_t persist = OPAL_PMIX_PERSIST_APP;
    bool rset, pset;

    opal_output_verbose(1, orte_pmix_server_globals.output,
                        "%s orted:pmix:server PUBLISH",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    /* create the caddy */
    req = OBJ_NEW(pmix_server_req_t);
    (void)asprintf(&req->operation, "PUBLISH: %s:%d", __FILE__, __LINE__);
    req->opcbfunc = cbfunc;
    req->cbdata = cbdata;

    /* load the command */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &cmd, 1, OPAL_UINT8))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* pack the name of the publisher */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, proc, 1, OPAL_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* no help for it - need to search for range/persistence */
    rset = false;
    pset = false;
    OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
        if (0 == strcmp(iptr->key, OPAL_PMIX_RANGE)) {
            req->range = (opal_pmix_data_range_t)iptr->data.uint;
            if (pset) {
                break;
            }
            rset = true;
        } else if (0 == strcmp(iptr->key, OPAL_PMIX_PERSISTENCE)) {
            persist = (opal_pmix_persistence_t)iptr->data.integer;
            if (rset) {
                break;
            }
            pset = true;
        }
    }

    /* pack the range */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &req->range, 1, OPAL_PMIX_DATA_RANGE))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* pack the persistence */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &persist, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* if we have items, pack those too - ignore persistence, timeout
     * and range values */
    OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
        if (0 == strcmp(iptr->key, OPAL_PMIX_RANGE) ||
            0 == strcmp(iptr->key, OPAL_PMIX_PERSISTENCE)) {
            continue;
        }
        if (0 == strcmp(iptr->key, OPAL_PMIX_TIMEOUT)) {
            /* record the timeout value, but don't pack it */
            req->timeout = iptr->data.integer;
            continue;
        }
        opal_output_verbose(5, orte_pmix_server_globals.output,
                            "%s publishing data %s of type %d from source %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), iptr->key, iptr->type,
                            ORTE_NAME_PRINT(proc));
        if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &iptr, 1, OPAL_VALUE))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(req);
            return rc;
        }
    }

    /* thread-shift so we can store the tracker */
    opal_event_set(orte_event_base, &(req->ev),
                   -1, OPAL_EV_WRITE, execute, req);
    opal_event_set_priority(&(req->ev), ORTE_MSG_PRI);
    ORTE_POST_OBJECT(req);
    opal_event_active(&(req->ev), OPAL_EV_WRITE, 1);

    return OPAL_SUCCESS;

}

int pmix_server_lookup_fn(opal_process_name_t *proc, char **keys,
                          opal_list_t *info,
                          opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
    pmix_server_req_t *req;
    int rc;
    uint8_t cmd = ORTE_PMIX_LOOKUP_CMD;
    int32_t nkeys, i;
    opal_value_t *iptr;

    /* the list of info objects are directives for us - they include
     * things like timeout constraints, so there is no reason to
     * forward them to the server */

    /* create the caddy */
    req = OBJ_NEW(pmix_server_req_t);
    (void)asprintf(&req->operation, "LOOKUP: %s:%d", __FILE__, __LINE__);
    req->lkcbfunc = cbfunc;
    req->cbdata = cbdata;

    /* load the command */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &cmd, 1, OPAL_UINT8))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* pack the requesting process jobid */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &proc->jobid, 1, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* no help for it - need to search for range */
    OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
        if (0 == strcmp(iptr->key, OPAL_PMIX_RANGE)) {
            req->range = (opal_pmix_data_range_t)iptr->data.uint;
            break;
        }
    }

    /* pack the range */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &req->range, 1, OPAL_PMIX_DATA_RANGE))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* pack the number of keys */
    nkeys = opal_argv_count(keys);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &nkeys, 1, OPAL_UINT32))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* pack the keys too */
    for (i=0; i < nkeys; i++) {
        opal_output_verbose(5, orte_pmix_server_globals.output,
                            "%s lookup data %s for proc %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), keys[i],
                            ORTE_NAME_PRINT(proc));
        if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &keys[i], 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(req);
            return rc;
        }
    }

    /* if we have items, pack those too - ignore range and timeout value */
    OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
        if (0 == strcmp(iptr->key, OPAL_PMIX_RANGE)) {
            continue;
        }
        if (0 == strcmp(iptr->key, OPAL_PMIX_TIMEOUT)) {
            /* record the timeout value, but don't pack it */
            req->timeout = iptr->data.integer;
            continue;
        }
        opal_output_verbose(2, orte_pmix_server_globals.output,
                            "%s lookup directive %s for proc %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), iptr->key,
                            ORTE_NAME_PRINT(proc));
        if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &iptr, 1, OPAL_VALUE))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(req);
            return rc;
        }
    }

    /* thread-shift so we can store the tracker */
    opal_event_set(orte_event_base, &(req->ev),
                   -1, OPAL_EV_WRITE, execute, req);
    opal_event_set_priority(&(req->ev), ORTE_MSG_PRI);
    ORTE_POST_OBJECT(req);
    opal_event_active(&(req->ev), OPAL_EV_WRITE, 1);

    return OPAL_SUCCESS;
}

int pmix_server_unpublish_fn(opal_process_name_t *proc, char **keys,
                             opal_list_t *info,
                             opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_server_req_t *req;
    int rc;
    uint8_t cmd = ORTE_PMIX_UNPUBLISH_CMD;
    uint32_t nkeys, n;
    opal_value_t *iptr;

    /* create the caddy */
    req = OBJ_NEW(pmix_server_req_t);
    (void)asprintf(&req->operation, "UNPUBLISH: %s:%d", __FILE__, __LINE__);
    req->opcbfunc = cbfunc;
    req->cbdata = cbdata;

    /* load the command */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &cmd, 1, OPAL_UINT8))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* pack the name of the publisher */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, proc, 1, OPAL_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* no help for it - need to search for range */
    OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
        if (0 == strcmp(iptr->key, OPAL_PMIX_RANGE)) {
            req->range = (opal_pmix_data_range_t)iptr->data.integer;
            break;
        }
    }

    /* pack the range */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &req->range, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* pack the number of keys */
    nkeys = opal_argv_count(keys);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &nkeys, 1, OPAL_UINT32))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    /* pack the keys too */
    for (n=0; n < nkeys; n++) {
        if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &keys[n], 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(req);
            return rc;
        }
    }

    /* if we have items, pack those too - ignore range and timeout value */
    OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
        if (0 == strcmp(iptr->key, OPAL_PMIX_RANGE)) {
            continue;
        }
        if (0 == strcmp(iptr->key, OPAL_PMIX_TIMEOUT)) {
            /* record the timeout value, but don't pack it */
            req->timeout = iptr->data.integer;
            continue;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(&req->msg, &iptr, 1, OPAL_VALUE))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(req);
            return rc;
        }
    }

    /* thread-shift so we can store the tracker */
    opal_event_set(orte_event_base, &(req->ev),
                   -1, OPAL_EV_WRITE, execute, req);
    opal_event_set_priority(&(req->ev), ORTE_MSG_PRI);
    ORTE_POST_OBJECT(req);
    opal_event_active(&(req->ev), OPAL_EV_WRITE, 1);

    return OPAL_SUCCESS;
}

void pmix_server_keyval_client(int status, orte_process_name_t* sender,
                               opal_buffer_t *buffer,
                               orte_rml_tag_t tg, void *cbdata)
{
    int rc, ret, room_num = -1;
    int32_t cnt;
    pmix_server_req_t *req=NULL;
    opal_list_t info;
    opal_value_t *iptr;
    opal_pmix_pdata_t *pdata;
    opal_process_name_t source;

    opal_output_verbose(1, orte_pmix_server_globals.output,
                        "%s recvd lookup data return",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    OBJ_CONSTRUCT(&info, opal_list_t);
    /* unpack the room number of the request tracker */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &room_num, &cnt, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* unpack the status */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &ret, &cnt, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        ret = rc;
        goto release;
    }

    opal_output_verbose(5, orte_pmix_server_globals.output,
                        "%s recvd lookup returned status %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret);

    if (ORTE_SUCCESS == ret) {
        /* see if any data was included - not an error if the answer is no */
        cnt = 1;
        while (OPAL_SUCCESS == opal_dss.unpack(buffer, &source, &cnt, OPAL_NAME)) {
            pdata = OBJ_NEW(opal_pmix_pdata_t);
            pdata->proc = source;
            if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &iptr, &cnt, OPAL_VALUE))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(pdata);
                continue;
            }
            opal_output_verbose(5, orte_pmix_server_globals.output,
                                "%s recvd lookup returned data %s of type %d from source %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), iptr->key, iptr->type,
                                ORTE_NAME_PRINT(&source));
            if (OPAL_SUCCESS != (rc = opal_value_xfer(&pdata->value, iptr))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(pdata);
                OBJ_RELEASE(iptr);
                continue;
            }
            OBJ_RELEASE(iptr);
            opal_list_append(&info, &pdata->super);
        }
    }

  release:
    if (0 <= room_num) {
        /* retrieve the tracker */
        opal_hotel_checkout_and_return_occupant(&orte_pmix_server_globals.reqs, room_num, (void**)&req);
    }

    if (NULL != req) {
        /* pass down the response */
        if (NULL != req->opcbfunc) {
            req->opcbfunc(ret, req->cbdata);
        } else if (NULL != req->lkcbfunc) {
            req->lkcbfunc(ret, &info, req->cbdata);
        } else {
            /* should not happen */
            ORTE_ERROR_LOG(ORTE_ERR_NOT_SUPPORTED);
        }

        /* cleanup */
        OPAL_LIST_DESTRUCT(&info);
        OBJ_RELEASE(req);
    }
}
