/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "opal_config.h"
#include "opal/constants.h"


#include <regex.h>

#include <time.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal_stdint.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"
#include "opal/util/show_help.h"

#include "opal/mca/pmix/base/base.h"
#include "opal/mca/pmix/base/pmix_base_fns.h"
#include "opal/mca/pmix/base/pmix_base_hash.h"

#define OPAL_PMI_PAD  10

void opal_pmix_base_set_evbase(opal_event_base_t *evbase)
{
    opal_pmix_base.evbase = evbase;
}

/********     ERRHANDLER SUPPORT FOR COMPONENTS THAT
 ********     DO NOT NATIVELY SUPPORT IT
 ********/
static opal_pmix_notification_fn_t evhandler = NULL;

void opal_pmix_base_register_handler(opal_list_t *event_codes,
                                     opal_list_t *info,
                                     opal_pmix_notification_fn_t err,
                                     opal_pmix_evhandler_reg_cbfunc_t cbfunc,
                                     void *cbdata)
{
    evhandler = err;
    if (NULL != cbfunc) {
        cbfunc(OPAL_SUCCESS, 0, cbdata);
    }
}

void opal_pmix_base_evhandler(int status,
                              const opal_process_name_t *source,
                              opal_list_t *info, opal_list_t *results,
                              opal_pmix_notification_complete_fn_t cbfunc, void *cbdata)
{
    if (NULL != evhandler) {
        evhandler(status, source, info, results, cbfunc, cbdata);
    }
}

void opal_pmix_base_deregister_handler(size_t errid,
                                       opal_pmix_op_cbfunc_t cbfunc,
                                       void *cbdata)
{
    evhandler = NULL;
    if (NULL != cbfunc) {
        cbfunc(OPAL_SUCCESS, cbdata);
    }
}

int opal_pmix_base_notify_event(int status,
                                const opal_process_name_t *source,
                                opal_pmix_data_range_t range,
                                opal_list_t *info,
                                opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    return OPAL_SUCCESS;
}

int opal_pmix_base_exchange(opal_value_t *indat,
                            opal_pmix_pdata_t *outdat,
                            int timeout)
{
    int rc;
    opal_list_t ilist, mlist;
    opal_value_t *info;
    opal_pmix_pdata_t *pdat;

    /* protect the incoming value */
    opal_dss.copy((void**)&info, indat, OPAL_VALUE);
    OBJ_CONSTRUCT(&ilist, opal_list_t);
    opal_list_append(&ilist, &info->super);
    /* tell the server to delete upon read */
    info = OBJ_NEW(opal_value_t);
    info->key = strdup(OPAL_PMIX_PERSISTENCE);
    info->type = OPAL_PERSIST;
    info->data.integer = OPAL_PMIX_PERSIST_FIRST_READ;
    opal_list_append(&ilist, &info->super);

    /* publish it with "session" scope */
    rc = opal_pmix.publish(&ilist);
    OPAL_LIST_DESTRUCT(&ilist);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    /* lookup the other side's info - if a non-blocking form
     * of lookup isn't available, then we use the blocking
     * form and trust that the underlying system will WAIT
     * until the other side publishes its data */
    pdat = OBJ_NEW(opal_pmix_pdata_t);
    pdat->value.key = strdup(outdat->value.key);
    pdat->value.type = outdat->value.type;
    /* setup the constraints */
    OBJ_CONSTRUCT(&mlist, opal_list_t);
    /* tell it to wait for the data to arrive */
    info = OBJ_NEW(opal_value_t);
    info->key = strdup(OPAL_PMIX_WAIT);
    info->type = OPAL_BOOL;
    info->data.flag = true;
    opal_list_append(&mlist, &info->super);
    /* pass along the given timeout as we don't know when
     * the other side will publish - it doesn't
     * have to be simultaneous */
    info = OBJ_NEW(opal_value_t);
    info->key = strdup(OPAL_PMIX_TIMEOUT);
    info->type = OPAL_INT;
    if (0 < opal_pmix_base.timeout) {
        /* the user has overridden the default */
        info->data.integer = opal_pmix_base.timeout;
    } else {
        info->data.integer = timeout;
    }
    opal_list_append(&mlist, &info->super);

    /* if a non-blocking version of lookup isn't
     * available, then use the blocking version */
    OBJ_CONSTRUCT(&ilist, opal_list_t);
    opal_list_append(&ilist, &pdat->super);
    rc = opal_pmix.lookup(&ilist, &mlist);
    OPAL_LIST_DESTRUCT(&mlist);
    if (OPAL_SUCCESS != rc) {
        OPAL_LIST_DESTRUCT(&ilist);
        return rc;
    }

    /* pass back the result */
    outdat->proc = pdat->proc;
    free(outdat->value.key);
    rc = opal_value_xfer(&outdat->value, &pdat->value);
    OPAL_LIST_DESTRUCT(&ilist);
    return rc;
}


/********     DATA CONSOLIDATION     ********/

static char* setup_key(const opal_process_name_t* name, const char *key, int pmix_keylen_max);
static char *pmi_encode(const void *val, size_t vallen);
static uint8_t *pmi_decode (const char *data, size_t *retlen);

int opal_pmix_base_store_encoded(const char *key, const void *data,
                                 opal_data_type_t type, char** buffer, int* length)
{
    opal_byte_object_t *bo;
    size_t data_len = 0;
    size_t needed;

    int pmi_packed_data_off = *length;
    char* pmi_packed_data = *buffer;

    switch (type) {
        case OPAL_STRING:
        {
            char *ptr = *(char **)data;
            data_len = ptr ? strlen(ptr) + 1 : 0;
            data = ptr;
            break;
        }
        case OPAL_INT:
        case OPAL_UINT:
            data_len = sizeof (int);
            break;
        case OPAL_INT16:
        case OPAL_UINT16:
            data_len = sizeof (int16_t);
            break;
        case OPAL_INT32:
        case OPAL_UINT32:
            data_len = sizeof (int32_t);
            break;
        case OPAL_INT64:
        case OPAL_UINT64:
            data_len = sizeof (int64_t);
            break;
        case OPAL_BYTE_OBJECT:
            bo = (opal_byte_object_t *) data;
            data = bo->bytes;
            data_len = bo->size;
    }

    needed = 10 + data_len + strlen (key);

    if (NULL == pmi_packed_data) {
        pmi_packed_data = calloc (needed, 1);
    } else {
        /* grow the region */
        pmi_packed_data = realloc (pmi_packed_data, pmi_packed_data_off + needed);
    }

    /* special length meaning NULL */
    if (NULL == data) {
        data_len = 0xffff;
    }

    /* serialize the opal datatype */
    pmi_packed_data_off += sprintf (pmi_packed_data + pmi_packed_data_off,
                                    "%s%c%02x%c%04x%c", key, '\0', type, '\0',
                                    (int) data_len, '\0');
    if (NULL != data) {
        memmove (pmi_packed_data + pmi_packed_data_off, data, data_len);
        pmi_packed_data_off += data_len;
    }

    *length = pmi_packed_data_off;
    *buffer = pmi_packed_data;
    return OPAL_SUCCESS;
}

int opal_pmix_base_commit_packed( char** data, int* data_offset,
                                  char** enc_data, int* enc_data_offset,
                                  int max_key, int* pack_key, kvs_put_fn fn)
{
    int rc;
    char *pmikey = NULL, *tmp;
    char tmp_key[32];
    char *encoded_data;
    int encoded_data_len;
    int data_len;
    int pkey;

    pkey = *pack_key;

    if (NULL == (tmp = malloc(max_key))) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    data_len = *data_offset;
    if (NULL == (encoded_data = pmi_encode(*data, data_len))) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        free(tmp);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    *data = NULL;
    *data_offset = 0;

    encoded_data_len = (int)strlen(encoded_data);
    while (encoded_data_len+*enc_data_offset > max_key - 2) {
        memcpy(tmp, *enc_data, *enc_data_offset);
        memcpy(tmp+*enc_data_offset, encoded_data, max_key-*enc_data_offset-1);
        tmp[max_key-1] = 0;

        sprintf (tmp_key, "key%d", pkey);

        if (NULL == (pmikey = setup_key(&OPAL_PROC_MY_NAME, tmp_key, max_key))) {
            OPAL_ERROR_LOG(OPAL_ERR_BAD_PARAM);
            rc = OPAL_ERR_BAD_PARAM;
            break;
        }

        rc = fn(pmikey, tmp);
        free(pmikey);
        if (OPAL_SUCCESS != rc) {
            *pack_key = pkey;
            free(tmp);
            free(encoded_data);
            return rc;
        }

        pkey++;
        memmove(encoded_data, encoded_data+max_key-1-*enc_data_offset, encoded_data_len - max_key + *enc_data_offset + 2);
        *enc_data_offset = 0;
        encoded_data_len = (int)strlen(encoded_data);
    }
    memcpy(tmp, *enc_data, *enc_data_offset);
    memcpy(tmp+*enc_data_offset, encoded_data, encoded_data_len+1);
    tmp[*enc_data_offset+encoded_data_len+1] = '\0';
    tmp[*enc_data_offset+encoded_data_len] = '-';
    free(encoded_data);

    sprintf (tmp_key, "key%d", pkey);

    if (NULL == (pmikey = setup_key(&OPAL_PROC_MY_NAME, tmp_key, max_key))) {
        OPAL_ERROR_LOG(OPAL_ERR_BAD_PARAM);
        rc = OPAL_ERR_BAD_PARAM;
        free(tmp);
        return rc;
    }

    rc = fn(pmikey, tmp);
    free(pmikey);
    if (OPAL_SUCCESS != rc) {
        *pack_key = pkey;
        free(tmp);
        return rc;
    }

    pkey++;
    free(*data);
    *data = NULL;
    *data_offset = 0;
    free(tmp);
    if (NULL != *enc_data) {
        free(*enc_data);
        *enc_data = NULL;
        *enc_data_offset = 0;
    }
    *pack_key = pkey;
    return OPAL_SUCCESS;
}

int opal_pmix_base_partial_commit_packed( char** data, int* data_offset,
                                          char** enc_data, int* enc_data_offset,
                                          int max_key, int* pack_key, kvs_put_fn fn)
{
    int rc;
    char *pmikey = NULL, *tmp;
    char tmp_key[32];
    char *encoded_data;
    int encoded_data_len;
    int data_len;
    int pkey;

    pkey = *pack_key;

    if (NULL == (tmp = malloc(max_key))) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    data_len = *data_offset - (*data_offset%3);
    if (NULL == (encoded_data = pmi_encode(*data, data_len))) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        free(tmp);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    if (*data_offset == data_len) {
        *data = NULL;
        *data_offset = 0;
    } else {
        memmove(*data, *data+data_len, *data_offset - data_len);
        *data = realloc(*data, *data_offset - data_len);
        *data_offset -= data_len;
    }

    encoded_data_len = (int)strlen(encoded_data);
    while (encoded_data_len+*enc_data_offset > max_key - 2) {
        memcpy(tmp, *enc_data, *enc_data_offset);
        memcpy(tmp+*enc_data_offset, encoded_data, max_key-*enc_data_offset-1);
        tmp[max_key-1] = 0;

        sprintf (tmp_key, "key%d", pkey);

        if (NULL == (pmikey = setup_key(&OPAL_PROC_MY_NAME, tmp_key, max_key))) {
            OPAL_ERROR_LOG(OPAL_ERR_BAD_PARAM);
            rc = OPAL_ERR_BAD_PARAM;
            break;
        }

        rc = fn(pmikey, tmp);
        free(pmikey);
        if (OPAL_SUCCESS != rc) {
            *pack_key = pkey;
            free(tmp);
            free(encoded_data);
            return rc;
        }

        pkey++;
        memmove(encoded_data, encoded_data+max_key-1-*enc_data_offset, encoded_data_len - max_key + *enc_data_offset + 2);
        *enc_data_offset = 0;
        encoded_data_len = (int)strlen(encoded_data);
    }
    free(tmp);
    if (NULL != *enc_data) {
        free(*enc_data);
    }
    *enc_data = realloc(encoded_data, strlen(encoded_data)+1);
    *enc_data_offset = strlen(encoded_data);
    *pack_key = pkey;
    return OPAL_SUCCESS;
}

int opal_pmix_base_get_packed(const opal_process_name_t* proc, char **packed_data,
                              size_t *len, int vallen, kvs_get_fn fn)
{
    char *tmp_encoded = NULL, *pmikey, *pmi_tmp;
    int remote_key, size;
    size_t bytes_read;
    int rc = OPAL_ERR_NOT_FOUND;

    /* set default */
    *packed_data = NULL;
    *len = 0;

    pmi_tmp = calloc (vallen, 1);
    if (NULL == pmi_tmp) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* read all of the packed data from this proc */
    for (remote_key = 0, bytes_read = 0 ; ; ++remote_key) {
        char tmp_key[32];

        sprintf (tmp_key, "key%d", remote_key);

        if (NULL == (pmikey = setup_key(proc, tmp_key, vallen))) {
            rc = OPAL_ERR_OUT_OF_RESOURCE;
            OPAL_ERROR_LOG(rc);
            free(pmi_tmp);
            if (NULL != tmp_encoded) {
                free(tmp_encoded);
            }
            return rc;
        }

        OPAL_OUTPUT_VERBOSE((10, opal_pmix_base_framework.framework_output,
                             "GETTING KEY %s", pmikey));

        rc = fn(pmikey, pmi_tmp, vallen);
        free (pmikey);
        if (OPAL_SUCCESS != rc) {
            break;
        }

        size = strlen (pmi_tmp);

        if (NULL == tmp_encoded) {
            tmp_encoded = malloc (size + 1);
        } else {
            tmp_encoded = realloc (tmp_encoded, bytes_read + size + 1);
        }

        strcpy (tmp_encoded + bytes_read, pmi_tmp);
        bytes_read += size;

        /* is the string terminator present? */
        if ('-' == tmp_encoded[bytes_read-1]) {
            break;
        }
    }

    free (pmi_tmp);

    OPAL_OUTPUT_VERBOSE((10, opal_pmix_base_framework.framework_output,
                         "Read data %s\n",
                         (NULL == tmp_encoded) ? "NULL" : tmp_encoded));

    if (NULL != tmp_encoded) {
        *packed_data = (char *) pmi_decode (tmp_encoded, len);
        free (tmp_encoded);
        if (NULL == *packed_data) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
    }

    return rc;
}

int opal_pmix_base_cache_keys_locally(const opal_process_name_t* id, const char* key,
                                      opal_value_t **out_kv, char* kvs_name,
                                      int vallen, kvs_get_fn fn)
{
    char *tmp, *tmp2, *tmp3, *tmp_val;
    opal_data_type_t stored_type;
    size_t len, offset;
    int rc, size;
    opal_value_t *kv, *knew;
    opal_list_t values;

    /* set the default */
    *out_kv = NULL;

    /* first try to fetch data from data storage */
    OBJ_CONSTRUCT(&values, opal_list_t);
    rc = opal_pmix_base_fetch(id, key, &values);
    if (OPAL_SUCCESS == rc) {
        kv = (opal_value_t*)opal_list_get_first(&values);
        /* create the copy */
        if (OPAL_SUCCESS != (rc = opal_dss.copy((void**)&knew, kv, OPAL_VALUE))) {
            OPAL_ERROR_LOG(rc);
        } else {
            *out_kv = knew;
        }
        OPAL_LIST_DESTRUCT(&values);
        return rc;
    }
    OPAL_LIST_DESTRUCT(&values);

    OPAL_OUTPUT_VERBOSE((1, opal_pmix_base_framework.framework_output,
                         "pmix: get all keys for proc %s in KVS %s",
                         OPAL_NAME_PRINT(*id), kvs_name));

    rc = opal_pmix_base_get_packed(id, &tmp_val, &len, vallen, fn);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    /* search for each key in the decoded data */
    for (offset = 0 ; offset < len ; ) {
        /* type */
        tmp = tmp_val + offset + strlen (tmp_val + offset) + 1;
        /* size */
        tmp2 = tmp + strlen (tmp) + 1;
        /* data */
        tmp3 = tmp2 + strlen (tmp2) + 1;

        stored_type = (opal_data_type_t) strtol (tmp, NULL, 16);
        size = strtol (tmp2, NULL, 16);
        /* cache value locally so we don't have to look it up via pmi again */
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup(tmp_val + offset);
        kv->type = stored_type;

        switch (stored_type) {
            case OPAL_BYTE:
                kv->data.byte = *tmp3;
                break;
            case OPAL_STRING:
                kv->data.string = strdup(tmp3);
                break;
            case OPAL_PID:
                kv->data.pid = strtoul(tmp3, NULL, 10);
                break;
            case OPAL_INT:
                kv->data.integer = strtol(tmp3, NULL, 10);
                break;
            case OPAL_INT8:
                kv->data.int8 = strtol(tmp3, NULL, 10);
                break;
            case OPAL_INT16:
                kv->data.int16 = strtol(tmp3, NULL, 10);
                break;
            case OPAL_INT32:
                kv->data.int32 = strtol(tmp3, NULL, 10);
                break;
            case OPAL_INT64:
                kv->data.int64 = strtol(tmp3, NULL, 10);
                break;
            case OPAL_UINT:
                kv->data.uint = strtoul(tmp3, NULL, 10);
                break;
            case OPAL_UINT8:
                kv->data.uint8 = strtoul(tmp3, NULL, 10);
                break;
            case OPAL_UINT16:
                kv->data.uint16 = strtoul(tmp3, NULL, 10);
                break;
            case OPAL_UINT32:
                kv->data.uint32 = strtoul(tmp3, NULL, 10);
                break;
            case OPAL_UINT64:
                kv->data.uint64 = strtoull(tmp3, NULL, 10);
                break;
            case OPAL_BYTE_OBJECT:
                if (size == 0xffff) {
                    kv->data.bo.bytes = NULL;
                    kv->data.bo.size = 0;
                    size = 0;
                } else {
                    kv->data.bo.bytes = malloc(size);
                    memcpy(kv->data.bo.bytes, tmp3, size);
                    kv->data.bo.size = size;
                }
                break;
            default:
                opal_output(0, "UNSUPPORTED TYPE %d", stored_type);
                return OPAL_ERROR;
        }
        /* store data in local hash table */
        if (OPAL_SUCCESS != (rc = opal_pmix_base_store(id, kv))) {
            OPAL_ERROR_LOG(rc);
        }
        /* keep going and cache everything locally */
        offset = (size_t) (tmp3 - tmp_val) + size;
        if (0 == strcmp(kv->key, key)) {
            /* create the copy */
            if (OPAL_SUCCESS != (rc = opal_dss.copy((void**)&knew, kv, OPAL_VALUE))) {
                OPAL_ERROR_LOG(rc);
            } else {
                *out_kv = knew;
            }
        }
    }
    free (tmp_val);
    /* if there was no issue with unpacking the message, but
     * we didn't find the requested info, then indicate that
     * the info wasn't found */
    if (OPAL_SUCCESS == rc && NULL == *out_kv) {
        return OPAL_ERR_NOT_FOUND;
    }
    return rc;
}

static char* setup_key(const opal_process_name_t* name, const char *key, int pmix_keylen_max)
{
    char *pmi_kvs_key;

    if (pmix_keylen_max <= asprintf(&pmi_kvs_key, "%" PRIu32 "-%" PRIu32 "-%s",
                                    name->jobid, name->vpid, key)) {
        free(pmi_kvs_key);
        return NULL;
    }

    return pmi_kvs_key;
}

/* base64 encoding with illegal (to Cray PMI) characters removed ('=' is replaced by ' ') */
static inline unsigned char pmi_base64_encsym (unsigned char value) {
    assert (value < 64);

    if (value < 26) {
        return 'A' + value;
    } else if (value < 52) {
        return 'a' + (value - 26);
    } else if (value < 62) {
        return '0' + (value - 52);
    }

    return (62 == value) ? '+' : '/';
}

static inline unsigned char pmi_base64_decsym (unsigned char value) {
    if ('+' == value) {
        return 62;
    } else if ('/' == value) {
        return 63;
    } else if (' ' == value) {
        return 64;
    } else if (value <= '9') {
        return (value - '0') + 52;
    } else if (value <= 'Z') {
        return (value - 'A');
    } else if (value <= 'z') {
        return (value - 'a') + 26;
    }
    return 64;
}

static inline void pmi_base64_encode_block (const unsigned char in[3], char out[4], int len) {
    out[0] = pmi_base64_encsym (in[0] >> 2);
    out[1] = pmi_base64_encsym (((in[0] & 0x03) << 4) | ((in[1] & 0xf0) >> 4));
    /* Cray PMI doesn't allow = in PMI attributes so pad with spaces */
    out[2] = 1 < len ? pmi_base64_encsym(((in[1] & 0x0f) << 2) | ((in[2] & 0xc0) >> 6)) : ' ';
    out[3] = 2 < len ? pmi_base64_encsym(in[2] & 0x3f) : ' ';
}

static inline int pmi_base64_decode_block (const char in[4], unsigned char out[3]) {
    char in_dec[4];

    in_dec[0] = pmi_base64_decsym (in[0]);
    in_dec[1] = pmi_base64_decsym (in[1]);
    in_dec[2] = pmi_base64_decsym (in[2]);
    in_dec[3] = pmi_base64_decsym (in[3]);

    out[0] = in_dec[0] << 2 | in_dec[1] >> 4;
    if (64 == in_dec[2]) {
        return 1;
    }

    out[1] = in_dec[1] << 4 | in_dec[2] >> 2;
    if (64 == in_dec[3]) {
        return 2;
    }

    out[2] = ((in_dec[2] << 6) & 0xc0) | in_dec[3];
    return 3;
}


/* PMI only supports strings. For now, do a simple base64. */
static char *pmi_encode(const void *val, size_t vallen)
{
    char *outdata, *tmp;
    size_t i;

    outdata = calloc (((2 + vallen) * 4) / 3 + 2, 1);
    if (NULL == outdata) {
        return NULL;
    }

    for (i = 0, tmp = outdata ; i < vallen ; i += 3, tmp += 4) {
        pmi_base64_encode_block((unsigned char *) val + i, tmp, vallen - i);
    }

    tmp[0] = (unsigned char)'\0';

    return outdata;
}

static uint8_t *pmi_decode (const char *data, size_t *retlen)
{
    size_t input_len = strlen (data) / 4;
    unsigned char *ret;
    int out_len;
    size_t i;

    /* default */
    *retlen = 0;

    ret = calloc (1, 3 * input_len);
    if (NULL == ret) {
        return ret;
    }
    for (i = 0, out_len = 0 ; i < input_len ; i++, data += 4) {
        out_len += pmi_base64_decode_block(data, ret + 3 * i);
    }
    *retlen = out_len;
    return ret;
}
