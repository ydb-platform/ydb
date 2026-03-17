/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/types.h"
#include "orte/constants.h"

#include "opal/dss/dss.h"
#include "opal/util/output.h"

#include "orte/mca/errmgr/errmgr.h"

#include "orte/util/attr.h"

#define MAX_CONVERTERS 5
#define MAX_CONVERTER_PROJECT_LEN 10

typedef struct {
    int init;
    char project[MAX_CONVERTER_PROJECT_LEN];
    orte_attribute_key_t key_base;
    orte_attribute_key_t key_max;
    orte_attr2str_fn_t converter;
} orte_attr_converter_t;

/* all default to NULL */
static orte_attr_converter_t converters[MAX_CONVERTERS];

bool orte_get_attribute(opal_list_t *attributes,
                        orte_attribute_key_t key,
                        void **data, opal_data_type_t type)
{
    orte_attribute_t *kv;
    int rc;

    OPAL_LIST_FOREACH(kv, attributes, orte_attribute_t) {
        if (key == kv->key) {
            if (kv->type != type) {
                ORTE_ERROR_LOG(ORTE_ERR_TYPE_MISMATCH);
                return false;
            }
            if (NULL != data) {
                if (ORTE_SUCCESS != (rc = orte_attr_unload(kv, data, type))) {
                    ORTE_ERROR_LOG(rc);
                }
            }
            return true;
        }
    }
    /* not found */
    return false;
}

int orte_set_attribute(opal_list_t *attributes,
                       orte_attribute_key_t key, bool local,
                       void *data, opal_data_type_t type)
{
    orte_attribute_t *kv;
    int rc;

    OPAL_LIST_FOREACH(kv, attributes, orte_attribute_t) {
        if (key == kv->key) {
            if (kv->type != type) {
                return ORTE_ERR_TYPE_MISMATCH;
            }
            if (ORTE_SUCCESS != (rc = orte_attr_load(kv, data, type))) {
                ORTE_ERROR_LOG(rc);
            }
            return rc;
        }
    }
    /* not found - add it */
    kv = OBJ_NEW(orte_attribute_t);
    kv->key = key;
    kv->local = local;
    if (OPAL_SUCCESS != (rc = orte_attr_load(kv, data, type))) {
        OBJ_RELEASE(kv);
        return rc;
    }
    opal_list_append(attributes, &kv->super);
    return ORTE_SUCCESS;
}

orte_attribute_t* orte_fetch_attribute(opal_list_t *attributes,
                                       orte_attribute_t *prev,
                                       orte_attribute_key_t key)
{
    orte_attribute_t *kv, *end, *next;

    /* if prev is NULL, then find the first attr on the list
     * that matches the key */
    if (NULL == prev) {
        OPAL_LIST_FOREACH(kv, attributes, orte_attribute_t) {
            if (key == kv->key) {
                return kv;
            }
        }
        /* if we get, then the key isn't on the list */
        return NULL;
    }

    /* if we are at the end of the list, then nothing to do */
    end = (orte_attribute_t*)opal_list_get_end(attributes);
    if (prev == end || end == (orte_attribute_t*)opal_list_get_next(&prev->super) ||
        NULL == opal_list_get_next(&prev->super)) {
        return NULL;
    }

    /* starting with the next item on the list, search
     * for the next attr with the matching key */
    next = (orte_attribute_t*)opal_list_get_next(&prev->super);
    while (NULL != next) {
        if (next->key == key) {
            return next;
        }
        next = (orte_attribute_t*)opal_list_get_next(&next->super);
    }

    /* if we get here, then no matching key was found */
    return NULL;
}

int orte_add_attribute(opal_list_t *attributes,
                       orte_attribute_key_t key, bool local,
                       void *data, opal_data_type_t type)
{
    orte_attribute_t *kv;
    int rc;

    kv = OBJ_NEW(orte_attribute_t);
    kv->key = key;
    kv->local = local;
    if (OPAL_SUCCESS != (rc = orte_attr_load(kv, data, type))) {
        OBJ_RELEASE(kv);
        return rc;
    }
    opal_list_append(attributes, &kv->super);
    return ORTE_SUCCESS;
}

int orte_prepend_attribute(opal_list_t *attributes,
                           orte_attribute_key_t key, bool local,
                           void *data, opal_data_type_t type)
{
    orte_attribute_t *kv;
    int rc;

    kv = OBJ_NEW(orte_attribute_t);
    kv->key = key;
    kv->local = local;
    if (OPAL_SUCCESS != (rc = orte_attr_load(kv, data, type))) {
        OBJ_RELEASE(kv);
        return rc;
    }
    opal_list_prepend(attributes, &kv->super);
    return ORTE_SUCCESS;
}

void orte_remove_attribute(opal_list_t *attributes, orte_attribute_key_t key)
{
    orte_attribute_t *kv;

    OPAL_LIST_FOREACH(kv, attributes, orte_attribute_t) {
        if (key == kv->key) {
            opal_list_remove_item(attributes, &kv->super);
            OBJ_RELEASE(kv);
            return;
        }
    }
}

int orte_attr_register(const char *project,
                       orte_attribute_key_t key_base,
                       orte_attribute_key_t key_max,
                       orte_attr2str_fn_t converter)
{
    int i;

    for (i = 0 ; i < MAX_CONVERTERS ; ++i) {
        if (0 == converters[i].init) {
            converters[i].init = 1;
            strncpy(converters[i].project, project, MAX_CONVERTER_PROJECT_LEN);
            converters[i].project[MAX_CONVERTER_PROJECT_LEN-1] = '\0';
            converters[i].key_base = key_base;
            converters[i].key_max = key_max;
            converters[i].converter = converter;
            return ORTE_SUCCESS;
        }
    }

    return ORTE_ERR_OUT_OF_RESOURCE;
}

const char *orte_attr_key_to_str(orte_attribute_key_t key)
{
    int i;

    if (ORTE_ATTR_KEY_BASE < key &&
        key < ORTE_ATTR_KEY_MAX) {
        /* belongs to ORTE, so we handle it */
        switch(key) {
        case ORTE_APP_HOSTFILE:
            return "APP-HOSTFILE";
        case ORTE_APP_ADD_HOSTFILE:
            return "APP-ADD-HOSTFILE";
        case ORTE_APP_DASH_HOST:
            return "APP-DASH-HOST";
        case ORTE_APP_ADD_HOST:
            return "APP-ADD-HOST";
        case ORTE_APP_USER_CWD:
            return "APP-USER-CWD";
        case ORTE_APP_SSNDIR_CWD:
            return "APP-USE-SESSION-DIR-AS-CWD";
        case ORTE_APP_PRELOAD_BIN:
            return "APP-PRELOAD-BIN";
        case ORTE_APP_PRELOAD_FILES:
            return "APP-PRELOAD-FILES";
        case ORTE_APP_SSTORE_LOAD:
            return "APP-SSTORE-LOAD";
        case ORTE_APP_RECOV_DEF:
            return "APP-RECOVERY-DEFINED";
        case ORTE_APP_MAX_RESTARTS:
            return "APP-MAX-RESTARTS";
        case ORTE_APP_MIN_NODES:
            return "APP-MIN-NODES";
        case ORTE_APP_MANDATORY:
            return "APP-NODES-MANDATORY";
        case ORTE_APP_MAX_PPN:
            return "APP-MAX-PPN";
        case ORTE_APP_PREFIX_DIR:
            return "APP-PREFIX-DIR";
        case ORTE_APP_NO_CACHEDIR:
            return "ORTE_APP_NO_CACHEDIR";
        case ORTE_APP_SET_ENVAR:
            return "ORTE_APP_SET_ENVAR";
        case ORTE_APP_UNSET_ENVAR:
            return "ORTE_APP_UNSET_ENVAR";
        case ORTE_APP_PREPEND_ENVAR:
            return "ORTE_APP_PREPEND_ENVAR";
        case ORTE_APP_APPEND_ENVAR:
            return "ORTE_APP_APPEND_ENVAR";
        case ORTE_APP_ADD_ENVAR:
            return "ORTE_APP_ADD_ENVAR";

        case ORTE_NODE_USERNAME:
            return "NODE-USERNAME";
        case ORTE_NODE_PORT:
            return "NODE-PORT";
        case ORTE_NODE_LAUNCH_ID:
            return "NODE-LAUNCHID";
        case ORTE_NODE_HOSTID:
            return "NODE-HOSTID";
        case ORTE_NODE_ALIAS:
            return "NODE-ALIAS";
        case ORTE_NODE_SERIAL_NUMBER:
            return "NODE-SERIAL-NUM";

        case ORTE_JOB_LAUNCH_MSG_SENT:
            return "JOB-LAUNCH-MSG-SENT";
        case ORTE_JOB_LAUNCH_MSG_RECVD:
            return "JOB-LAUNCH-MSG-RECVD";
        case ORTE_JOB_MAX_LAUNCH_MSG_RECVD:
            return "JOB-MAX-LAUNCH-MSG-RECVD";
        case ORTE_JOB_FILE_MAPS:
            return "JOB-FILE-MAPS";
        case ORTE_JOB_CKPT_STATE:
            return "JOB-CKPT-STATE";
        case ORTE_JOB_SNAPSHOT_REF:
            return "JOB-SNAPSHOT-REF";
        case ORTE_JOB_SNAPSHOT_LOC:
            return "JOB-SNAPSHOT-LOC";
        case ORTE_JOB_SNAPC_INIT_BAR:
            return "JOB-SNAPC-INIT-BARRIER-ID";
        case ORTE_JOB_SNAPC_FINI_BAR:
            return "JOB-SNAPC-FINI-BARRIER-ID";
        case ORTE_JOB_NUM_NONZERO_EXIT:
            return "JOB-NUM-NONZERO-EXIT";
        case ORTE_JOB_FAILURE_TIMER_EVENT:
            return "JOB-FAILURE-TIMER-EVENT";
        case ORTE_JOB_ABORTED_PROC:
            return "JOB-ABORTED-PROC";
        case ORTE_JOB_MAPPER:
            return "JOB-MAPPER";
        case ORTE_JOB_REDUCER:
            return "JOB-REDUCER";
        case ORTE_JOB_COMBINER:
            return "JOB-COMBINER";
        case ORTE_JOB_INDEX_ARGV:
            return "JOB-INDEX-ARGV";
        case ORTE_JOB_NO_VM:
            return "JOB-NO-VM";
        case ORTE_JOB_SPIN_FOR_DEBUG:
            return "JOB-SPIN-FOR-DEBUG";
        case ORTE_JOB_CONTINUOUS_OP:
            return "JOB-CONTINUOUS-OP";
        case ORTE_JOB_RECOVER_DEFINED:
            return "JOB-RECOVERY-DEFINED";
        case ORTE_JOB_NON_ORTE_JOB:
            return "JOB-NON-ORTE-JOB";
        case ORTE_JOB_STDOUT_TARGET:
            return "JOB-STDOUT-TARGET";
        case ORTE_JOB_POWER:
            return "JOB-POWER";
        case ORTE_JOB_MAX_FREQ:
            return "JOB-MAX_FREQ";
        case ORTE_JOB_MIN_FREQ:
            return "JOB-MIN_FREQ";
        case ORTE_JOB_GOVERNOR:
            return "JOB-FREQ-GOVERNOR";
        case ORTE_JOB_FAIL_NOTIFIED:
            return "JOB-FAIL-NOTIFIED";
        case ORTE_JOB_TERM_NOTIFIED:
            return "JOB-TERM-NOTIFIED";
        case ORTE_JOB_PEER_MODX_ID:
            return "JOB-PEER-MODX-ID";
        case ORTE_JOB_INIT_BAR_ID:
            return "JOB-INIT-BAR-ID";
        case ORTE_JOB_FINI_BAR_ID:
            return "JOB-FINI-BAR-ID";
        case ORTE_JOB_FWDIO_TO_TOOL:
            return "JOB-FWD-IO-TO-TOOL";
        case ORTE_JOB_PHYSICAL_CPUIDS:
            return "JOB-PHYSICAL-CPUIDS";
        case ORTE_JOB_LAUNCHED_DAEMONS:
            return "JOB-LAUNCHED-DAEMONS";
        case ORTE_JOB_REPORT_BINDINGS:
            return "JOB-REPORT-BINDINGS";
        case ORTE_JOB_CPU_LIST:
            return "JOB-CPU-LIST";
        case ORTE_JOB_NOTIFICATIONS:
            return "JOB-NOTIFICATIONS";
        case ORTE_JOB_ROOM_NUM:
            return "JOB-ROOM-NUM";
        case ORTE_JOB_LAUNCH_PROXY:
            return "JOB-LAUNCH-PROXY";
        case ORTE_JOB_NSPACE_REGISTERED:
            return "JOB-NSPACE-REGISTERED";
        case ORTE_JOB_FIXED_DVM:
            return "ORTE-JOB-FIXED-DVM";
        case ORTE_JOB_DVM_JOB:
            return "ORTE-JOB-DVM-JOB";
        case ORTE_JOB_CANCELLED:
            return "ORTE-JOB-CANCELLED";
        case ORTE_JOB_OUTPUT_TO_FILE:
            return "ORTE-JOB-OUTPUT-TO-FILE";
        case ORTE_JOB_MERGE_STDERR_STDOUT:
            return "ORTE-JOB-MERGE-STDERR-STDOUT";
        case ORTE_JOB_TAG_OUTPUT:
            return "ORTE-JOB-TAG-OUTPUT";
        case ORTE_JOB_TIMESTAMP_OUTPUT:
            return "ORTE-JOB-TIMESTAMP-OUTPUT";
        case ORTE_JOB_MULTI_DAEMON_SIM:
            return "ORTE_JOB_MULTI_DAEMON_SIM";
        case ORTE_JOB_NOTIFY_COMPLETION:
            return "ORTE_JOB_NOTIFY_COMPLETION";
        case ORTE_JOB_TRANSPORT_KEY:
            return "ORTE_JOB_TRANSPORT_KEY";
        case ORTE_JOB_INFO_CACHE:
            return "ORTE_JOB_INFO_CACHE";
        case ORTE_JOB_FULLY_DESCRIBED:
            return "ORTE_JOB_FULLY_DESCRIBED";
        case ORTE_JOB_SILENT_TERMINATION:
            return "ORTE_JOB_SILENT_TERMINATION";
        case ORTE_JOB_SET_ENVAR:
            return "ORTE_JOB_SET_ENVAR";
        case ORTE_JOB_UNSET_ENVAR:
            return "ORTE_JOB_UNSET_ENVAR";
        case ORTE_JOB_PREPEND_ENVAR:
            return "ORTE_JOB_PREPEND_ENVAR";
        case ORTE_JOB_APPEND_ENVAR:
            return "ORTE_JOB_APPEND_ENVAR";
        case ORTE_JOB_ADD_ENVAR:
            return "ORTE_APP_ADD_ENVAR";
        case ORTE_JOB_APP_SETUP_DATA:
            return "ORTE_JOB_APP_SETUP_DATA";

        case ORTE_PROC_NOBARRIER:
            return "PROC-NOBARRIER";
        case ORTE_PROC_CPU_BITMAP:
            return "PROC-CPU-BITMAP";
        case ORTE_PROC_HWLOC_LOCALE:
            return "PROC-HWLOC-LOCALE";
        case ORTE_PROC_HWLOC_BOUND:
            return "PROC-HWLOC-BOUND";
        case ORTE_PROC_PRIOR_NODE:
            return "PROC-PRIOR-NODE";
        case ORTE_PROC_NRESTARTS:
            return "PROC-NUM-RESTARTS";
        case ORTE_PROC_RESTART_TIME:
            return "PROC-RESTART-TIME";
        case ORTE_PROC_FAST_FAILS:
            return "PROC-FAST-FAILS";
        case ORTE_PROC_CKPT_STATE:
            return "PROC-CKPT-STATE";
        case ORTE_PROC_SNAPSHOT_REF:
            return "PROC-SNAPHOT-REF";
        case ORTE_PROC_SNAPSHOT_LOC:
            return "PROC-SNAPSHOT-LOC";
        case ORTE_PROC_NODENAME:
            return "PROC-NODENAME";
        case ORTE_PROC_CGROUP:
            return "PROC-CGROUP";
        case ORTE_PROC_NBEATS:
            return "PROC-NBEATS";

        case ORTE_RML_TRANSPORT_TYPE:
            return "RML-TRANSPORT-TYPE";
        case ORTE_RML_PROTOCOL_TYPE:
            return "RML-PROTOCOL-TYPE";
        case ORTE_RML_CONDUIT_ID:
            return "RML-CONDUIT-ID";
        case ORTE_RML_INCLUDE_COMP_ATTRIB:
            return "RML-INCLUDE";
        case ORTE_RML_EXCLUDE_COMP_ATTRIB:
            return "RML-EXCLUDE";
        case ORTE_RML_TRANSPORT_ATTRIB:
            return "RML-TRANSPORT";
        case ORTE_RML_QUALIFIER_ATTRIB:
            return "RML-QUALIFIER";
        case ORTE_RML_PROVIDER_ATTRIB:
            return "RML-DESIRED-PROVIDERS";
        case ORTE_RML_PROTOCOL_ATTRIB:
            return "RML-DESIRED-PROTOCOLS";
        case ORTE_RML_ROUTED_ATTRIB:
            return "RML-DESIRED-ROUTED-MODULES";
        default:
            return "UNKNOWN-KEY";
        }
    }

    /* see if one of the converters can handle it */
    for (i = 0 ; i < MAX_CONVERTERS ; ++i) {
        if (0 != converters[i].init) {
            if (converters[i].key_base < key &&
                key < converters[i].key_max) {
                return converters[i].converter(key);
            }
        }
    }

    /* get here if nobody know what to do */
    return "UNKNOWN-KEY";
}


int orte_attr_load(orte_attribute_t *kv,
                   void *data, opal_data_type_t type)
{
    opal_byte_object_t *boptr;
    struct timeval *tv;
    opal_envar_t *envar;

    kv->type = type;
    if (NULL == data) {
        /* if the type is BOOL, then the user wanted to
         * use the presence of the attribute to indicate
         * "true" - so let's mark it that way just in
         * case a subsequent test looks for the value */
        if (OPAL_BOOL == type) {
            kv->data.flag = true;
        } else {
            /* otherwise, check to see if this type has storage
             * that is already allocated, and free it if so */
            if (OPAL_STRING == type && NULL != kv->data.string) {
                free(kv->data.string);
            } else if (OPAL_BYTE_OBJECT == type && NULL != kv->data.bo.bytes) {
                free(kv->data.bo.bytes);
            }
            /* just set the fields to zero */
            memset(&kv->data, 0, sizeof(kv->data));
        }
        return OPAL_SUCCESS;
    }

    switch (type) {
    case OPAL_BOOL:
        kv->data.flag = *(bool*)(data);
        break;
    case OPAL_BYTE:
        kv->data.byte = *(uint8_t*)(data);
        break;
    case OPAL_STRING:
        if (NULL != kv->data.string) {
            free(kv->data.string);
        }
        if (NULL != data) {
            kv->data.string = strdup( (const char *) data);
        } else {
            kv->data.string = NULL;
        }
        break;
    case OPAL_SIZE:
        kv->data.size = *(size_t*)(data);
        break;
    case OPAL_PID:
        kv->data.pid = *(pid_t*)(data);
        break;

    case OPAL_INT:
        kv->data.integer = *(int*)(data);
        break;
    case OPAL_INT8:
        kv->data.int8 = *(int8_t*)(data);
        break;
    case OPAL_INT16:
        kv->data.int16 = *(int16_t*)(data);
        break;
    case OPAL_INT32:
        kv->data.int32 = *(int32_t*)(data);
        break;
    case OPAL_INT64:
        kv->data.int64 = *(int64_t*)(data);
        break;

    case OPAL_UINT:
        kv->data.uint = *(unsigned int*)(data);
        break;
    case OPAL_UINT8:
        kv->data.uint8 = *(uint8_t*)(data);
        break;
    case OPAL_UINT16:
        kv->data.uint16 = *(uint16_t*)(data);
        break;
    case OPAL_UINT32:
        kv->data.uint32 = *(uint32_t*)data;
        break;
    case OPAL_UINT64:
        kv->data.uint64 = *(uint64_t*)(data);
        break;

    case OPAL_BYTE_OBJECT:
        if (NULL != kv->data.bo.bytes) {
            free(kv->data.bo.bytes);
        }
        boptr = (opal_byte_object_t*)data;
        if (NULL != boptr && NULL != boptr->bytes && 0 < boptr->size) {
            kv->data.bo.bytes = (uint8_t *) malloc(boptr->size);
            memcpy(kv->data.bo.bytes, boptr->bytes, boptr->size);
            kv->data.bo.size = boptr->size;
        } else {
            kv->data.bo.bytes = NULL;
            kv->data.bo.size = 0;
        }
        break;

    case OPAL_FLOAT:
        kv->data.fval = *(float*)(data);
        break;

    case OPAL_TIMEVAL:
        tv = (struct timeval*)data;
        kv->data.tv.tv_sec = tv->tv_sec;
        kv->data.tv.tv_usec = tv->tv_usec;
        break;

    case OPAL_PTR:
        kv->data.ptr = data;
        break;

    case OPAL_VPID:
        kv->data.vpid = *(orte_vpid_t *)data;
        break;

    case OPAL_JOBID:
        kv->data.jobid = *(orte_jobid_t *)data;
        break;

    case OPAL_NAME:
        kv->data.name = *(opal_process_name_t *)data;
        break;

    case OPAL_ENVAR:
        OBJ_CONSTRUCT(&kv->data.envar, opal_envar_t);
        envar = (opal_envar_t*)data;
        if (NULL != envar->envar) {
            kv->data.envar.envar = strdup(envar->envar);
        }
        if (NULL != envar->value) {
            kv->data.envar.value = strdup(envar->value);
        }
        kv->data.envar.separator = envar->separator;
        break;

    default:
        OPAL_ERROR_LOG(OPAL_ERR_NOT_SUPPORTED);
        return OPAL_ERR_NOT_SUPPORTED;
    }
    return OPAL_SUCCESS;
}

int orte_attr_unload(orte_attribute_t *kv,
                     void **data, opal_data_type_t type)
{
    opal_byte_object_t *boptr;
    opal_envar_t *envar;

    if (type != kv->type) {
        return OPAL_ERR_TYPE_MISMATCH;
    }
    if (NULL == data  ||
        (OPAL_STRING != type && OPAL_BYTE_OBJECT != type &&
         OPAL_BUFFER != type && OPAL_PTR != type && NULL == *data)) {
        assert(0);
        OPAL_ERROR_LOG(OPAL_ERR_BAD_PARAM);
        return OPAL_ERR_BAD_PARAM;
    }

    switch (type) {
    case OPAL_BOOL:
        memcpy(*data, &kv->data.flag, sizeof(bool));
        break;
    case OPAL_BYTE:
        memcpy(*data, &kv->data.byte, sizeof(uint8_t));
        break;
    case OPAL_STRING:
        if (NULL != kv->data.string) {
            *data = strdup(kv->data.string);
        } else {
            *data = NULL;
        }
        break;
    case OPAL_SIZE:
        memcpy(*data, &kv->data.size, sizeof(size_t));
        break;
    case OPAL_PID:
        memcpy(*data, &kv->data.pid, sizeof(pid_t));
        break;

    case OPAL_INT:
        memcpy(*data, &kv->data.integer, sizeof(int));
        break;
    case OPAL_INT8:
        memcpy(*data, &kv->data.int8, sizeof(int8_t));
        break;
    case OPAL_INT16:
        memcpy(*data, &kv->data.int16, sizeof(int16_t));
        break;
    case OPAL_INT32:
        memcpy(*data, &kv->data.int32, sizeof(int32_t));
        break;
    case OPAL_INT64:
        memcpy(*data, &kv->data.int64, sizeof(int64_t));
        break;

    case OPAL_UINT:
        memcpy(*data, &kv->data.uint, sizeof(unsigned int));
        break;
    case OPAL_UINT8:
        memcpy(*data, &kv->data.uint8, 1);
        break;
    case OPAL_UINT16:
        memcpy(*data, &kv->data.uint16, 2);
        break;
    case OPAL_UINT32:
        memcpy(*data, &kv->data.uint32, 4);
        break;
    case OPAL_UINT64:
        memcpy(*data, &kv->data.uint64, 8);
        break;

    case OPAL_BYTE_OBJECT:
        boptr = (opal_byte_object_t*)malloc(sizeof(opal_byte_object_t));
        if (NULL != kv->data.bo.bytes && 0 < kv->data.bo.size) {
            boptr->bytes = (uint8_t *) malloc(kv->data.bo.size);
            memcpy(boptr->bytes, kv->data.bo.bytes, kv->data.bo.size);
            boptr->size = kv->data.bo.size;
        } else {
            boptr->bytes = NULL;
            boptr->size = 0;
        }
        *data = boptr;
        break;

    case OPAL_BUFFER:
        *data = OBJ_NEW(opal_buffer_t);
        opal_dss.copy_payload(*data, &kv->data.buf);
        break;

    case OPAL_FLOAT:
        memcpy(*data, &kv->data.fval, sizeof(float));
        break;

    case OPAL_TIMEVAL:
        memcpy(*data, &kv->data.tv, sizeof(struct timeval));
        break;

    case OPAL_PTR:
        *data = kv->data.ptr;
        break;

    case OPAL_VPID:
        memcpy(*data, &kv->data.vpid, sizeof(orte_vpid_t));
        break;

    case OPAL_JOBID:
        memcpy(*data, &kv->data.jobid, sizeof(orte_jobid_t));
        break;

    case OPAL_NAME:
        memcpy(*data, &kv->data.name, sizeof(orte_process_name_t));
        break;

    case OPAL_ENVAR:
        envar = OBJ_NEW(opal_envar_t);
        if (NULL != kv->data.envar.envar) {
            envar->envar = strdup(kv->data.envar.envar);
        }
        if (NULL != kv->data.envar.value) {
            envar->value = strdup(kv->data.envar.value);
        }
        envar->separator = kv->data.envar.separator;
        *data = envar;
        break;

    default:
        OPAL_ERROR_LOG(OPAL_ERR_NOT_SUPPORTED);
        return OPAL_ERR_NOT_SUPPORTED;
    }
    return OPAL_SUCCESS;
}
