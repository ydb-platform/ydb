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
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "orte_config.h"
#include "orte/types.h"
#include "orte/constants.h"

#include <stdio.h>
#include <string.h>

#include "opal/util/printf.h"
#include "opal/threads/tsd.h"

#include "orte/mca/errmgr/errmgr.h"

#include "orte/util/name_fns.h"

#define ORTE_PRINT_NAME_ARGS_MAX_SIZE   50
#define ORTE_PRINT_NAME_ARG_NUM_BUFS    16

#define ORTE_SCHEMA_DELIMITER_CHAR      '.'
#define ORTE_SCHEMA_WILDCARD_CHAR       '*'
#define ORTE_SCHEMA_WILDCARD_STRING     "*"
#define ORTE_SCHEMA_INVALID_CHAR        '$'
#define ORTE_SCHEMA_INVALID_STRING      "$"

/* constructor - used to initialize namelist instance */
static void orte_namelist_construct(orte_namelist_t* list)
{
    list->name.jobid = ORTE_JOBID_INVALID;
    list->name.vpid = ORTE_VPID_INVALID;
}

/* destructor - used to free any resources held by instance */
static void orte_namelist_destructor(orte_namelist_t* list)
{
}

/* define instance of opal_class_t */
OBJ_CLASS_INSTANCE(orte_namelist_t,              /* type name */
                   opal_list_item_t,             /* parent "class" name */
                   orte_namelist_construct,      /* constructor */
                   orte_namelist_destructor);    /* destructor */

static bool fns_init=false;

static opal_tsd_key_t print_args_tsd_key;
char* orte_print_args_null = "NULL";
typedef struct {
    char *buffers[ORTE_PRINT_NAME_ARG_NUM_BUFS];
    int cntr;
} orte_print_args_buffers_t;

static void
buffer_cleanup(void *value)
{
    int i;
    orte_print_args_buffers_t *ptr;

    if (NULL != value) {
        ptr = (orte_print_args_buffers_t*)value;
        for (i=0; i < ORTE_PRINT_NAME_ARG_NUM_BUFS; i++) {
            free(ptr->buffers[i]);
        }
        free (ptr);
    }
}

static orte_print_args_buffers_t*
get_print_name_buffer(void)
{
    orte_print_args_buffers_t *ptr;
    int ret, i;

    if (!fns_init) {
        /* setup the print_args function */
        if (ORTE_SUCCESS != (ret = opal_tsd_key_create(&print_args_tsd_key, buffer_cleanup))) {
            ORTE_ERROR_LOG(ret);
            return NULL;
        }
        fns_init = true;
    }

    ret = opal_tsd_getspecific(print_args_tsd_key, (void**)&ptr);
    if (OPAL_SUCCESS != ret) return NULL;

    if (NULL == ptr) {
        ptr = (orte_print_args_buffers_t*)malloc(sizeof(orte_print_args_buffers_t));
        for (i=0; i < ORTE_PRINT_NAME_ARG_NUM_BUFS; i++) {
            ptr->buffers[i] = (char *) malloc((ORTE_PRINT_NAME_ARGS_MAX_SIZE+1) * sizeof(char));
        }
        ptr->cntr = 0;
        ret = opal_tsd_setspecific(print_args_tsd_key, (void*)ptr);
    }

    return (orte_print_args_buffers_t*) ptr;
}

char* orte_util_print_name_args(const orte_process_name_t *name)
{
    orte_print_args_buffers_t *ptr;
    char *job, *vpid;

    /* protect against NULL names */
    if (NULL == name) {
        /* get the next buffer */
        ptr = get_print_name_buffer();
        if (NULL == ptr) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return orte_print_args_null;
        }
        /* cycle around the ring */
        if (ORTE_PRINT_NAME_ARG_NUM_BUFS == ptr->cntr) {
            ptr->cntr = 0;
        }
        snprintf(ptr->buffers[ptr->cntr++], ORTE_PRINT_NAME_ARGS_MAX_SIZE, "[NO-NAME]");
        return ptr->buffers[ptr->cntr-1];
    }

    /* get the jobid, vpid strings first - this will protect us from
     * stepping on each other's buffer. This also guarantees
     * that the print_args function has been initialized, so
     * we don't need to duplicate that here
     */
    job = orte_util_print_jobids(name->jobid);
    vpid = orte_util_print_vpids(name->vpid);

    /* get the next buffer */
    ptr = get_print_name_buffer();

    if (NULL == ptr) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return orte_print_args_null;
    }

    /* cycle around the ring */
    if (ORTE_PRINT_NAME_ARG_NUM_BUFS == ptr->cntr) {
        ptr->cntr = 0;
    }

    snprintf(ptr->buffers[ptr->cntr++],
             ORTE_PRINT_NAME_ARGS_MAX_SIZE,
             "[%s,%s]", job, vpid);

    return ptr->buffers[ptr->cntr-1];
}

char* orte_util_print_jobids(const orte_jobid_t job)
{
    orte_print_args_buffers_t *ptr;
    unsigned long tmp1, tmp2;

    ptr = get_print_name_buffer();

    if (NULL == ptr) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return orte_print_args_null;
    }

    /* cycle around the ring */
    if (ORTE_PRINT_NAME_ARG_NUM_BUFS == ptr->cntr) {
        ptr->cntr = 0;
    }

    if (ORTE_JOBID_INVALID == job) {
        snprintf(ptr->buffers[ptr->cntr++], ORTE_PRINT_NAME_ARGS_MAX_SIZE, "[INVALID]");
    } else if (ORTE_JOBID_WILDCARD == job) {
        snprintf(ptr->buffers[ptr->cntr++], ORTE_PRINT_NAME_ARGS_MAX_SIZE, "[WILDCARD]");
    } else {
        tmp1 = ORTE_JOB_FAMILY((unsigned long)job);
        tmp2 = ORTE_LOCAL_JOBID((unsigned long)job);
        snprintf(ptr->buffers[ptr->cntr++],
                 ORTE_PRINT_NAME_ARGS_MAX_SIZE,
                 "[%lu,%lu]", tmp1, tmp2);
    }
    return ptr->buffers[ptr->cntr-1];
}

char* orte_util_print_job_family(const orte_jobid_t job)
{
    orte_print_args_buffers_t *ptr;
    unsigned long tmp1;

    ptr = get_print_name_buffer();

    if (NULL == ptr) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return orte_print_args_null;
    }

    /* cycle around the ring */
    if (ORTE_PRINT_NAME_ARG_NUM_BUFS == ptr->cntr) {
        ptr->cntr = 0;
    }

    if (ORTE_JOBID_INVALID == job) {
        snprintf(ptr->buffers[ptr->cntr++], ORTE_PRINT_NAME_ARGS_MAX_SIZE, "INVALID");
    } else if (ORTE_JOBID_WILDCARD == job) {
        snprintf(ptr->buffers[ptr->cntr++], ORTE_PRINT_NAME_ARGS_MAX_SIZE, "WILDCARD");
    } else {
        tmp1 = ORTE_JOB_FAMILY((unsigned long)job);
        snprintf(ptr->buffers[ptr->cntr++],
                 ORTE_PRINT_NAME_ARGS_MAX_SIZE,
                 "%lu", tmp1);
    }
    return ptr->buffers[ptr->cntr-1];
}

char* orte_util_print_local_jobid(const orte_jobid_t job)
{
    orte_print_args_buffers_t *ptr;
    unsigned long tmp1;

    ptr = get_print_name_buffer();

    if (NULL == ptr) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return orte_print_args_null;
    }

    /* cycle around the ring */
    if (ORTE_PRINT_NAME_ARG_NUM_BUFS == ptr->cntr) {
        ptr->cntr = 0;
    }

    if (ORTE_JOBID_INVALID == job) {
        snprintf(ptr->buffers[ptr->cntr++], ORTE_PRINT_NAME_ARGS_MAX_SIZE, "INVALID");
    } else if (ORTE_JOBID_WILDCARD == job) {
        snprintf(ptr->buffers[ptr->cntr++], ORTE_PRINT_NAME_ARGS_MAX_SIZE, "WILDCARD");
    } else {
        tmp1 = (unsigned long)job & 0x0000ffff;
        snprintf(ptr->buffers[ptr->cntr++],
                 ORTE_PRINT_NAME_ARGS_MAX_SIZE,
                 "%lu", tmp1);
    }
    return ptr->buffers[ptr->cntr-1];
}

char* orte_util_print_vpids(const orte_vpid_t vpid)
{
    orte_print_args_buffers_t *ptr;

    ptr = get_print_name_buffer();

    if (NULL == ptr) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return orte_print_args_null;
    }

    /* cycle around the ring */
    if (ORTE_PRINT_NAME_ARG_NUM_BUFS == ptr->cntr) {
        ptr->cntr = 0;
    }

    if (ORTE_VPID_INVALID == vpid) {
        snprintf(ptr->buffers[ptr->cntr++], ORTE_PRINT_NAME_ARGS_MAX_SIZE, "INVALID");
    } else if (ORTE_VPID_WILDCARD == vpid) {
        snprintf(ptr->buffers[ptr->cntr++], ORTE_PRINT_NAME_ARGS_MAX_SIZE, "WILDCARD");
    } else {
        snprintf(ptr->buffers[ptr->cntr++],
                 ORTE_PRINT_NAME_ARGS_MAX_SIZE,
                 "%ld", (long)vpid);
    }
    return ptr->buffers[ptr->cntr-1];
}



/***   STRING FUNCTIONS   ***/

int orte_util_snprintf_jobid(char *jobid_string, size_t size, const orte_jobid_t jobid)
{
    int rc;

    /* check for wildcard value - handle appropriately */
    if (ORTE_JOBID_WILDCARD == jobid) {
        (void)strncpy(jobid_string, ORTE_SCHEMA_WILDCARD_STRING, size);
    } else {
        rc = snprintf(jobid_string, size, "%ld", (long) jobid);
        if (0 > rc) {
            return ORTE_ERROR;
        }
    }

    return ORTE_SUCCESS;
}

int orte_util_convert_jobid_to_string(char **jobid_string, const orte_jobid_t jobid)
{
    int rc;
    char str[256];
    rc = orte_util_snprintf_jobid(str, 255, jobid);
    if (0 > rc) {
        *jobid_string = NULL;
        return rc;
    }
    *jobid_string = strdup(str);
    if (NULL == *jobid_string) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    return ORTE_SUCCESS;
}


int orte_util_convert_string_to_jobid(orte_jobid_t *jobid, const char* jobidstring)
{
    if (NULL == jobidstring) {  /* got an error */
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        *jobid = ORTE_JOBID_INVALID;
        return ORTE_ERR_BAD_PARAM;
    }

    /** check for wildcard character - handle appropriately */
    if (0 == strcmp(ORTE_SCHEMA_WILDCARD_STRING, jobidstring)) {
        *jobid = ORTE_JOBID_WILDCARD;
        return ORTE_SUCCESS;
    }

    /* check for invalid value */
    if (0 == strcmp(ORTE_SCHEMA_INVALID_STRING, jobidstring)) {
        *jobid = ORTE_JOBID_INVALID;
        return ORTE_SUCCESS;
    }

    *jobid = strtoul(jobidstring, NULL, 10);

    return ORTE_SUCCESS;
}

int orte_util_convert_vpid_to_string(char **vpid_string, const orte_vpid_t vpid)
{
    /* check for wildcard value - handle appropriately */
    if (ORTE_VPID_WILDCARD == vpid) {
        *vpid_string = strdup(ORTE_SCHEMA_WILDCARD_STRING);
        return ORTE_SUCCESS;
    }

    /* check for invalid value - handle appropriately */
    if (ORTE_VPID_INVALID == vpid) {
        *vpid_string = strdup(ORTE_SCHEMA_INVALID_STRING);
        return ORTE_SUCCESS;
    }

    if (0 > asprintf(vpid_string, "%ld", (long) vpid)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    return ORTE_SUCCESS;
}


int orte_util_convert_string_to_vpid(orte_vpid_t *vpid, const char* vpidstring)
{
    if (NULL == vpidstring) {  /* got an error */
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        *vpid = ORTE_VPID_INVALID;
        return ORTE_ERR_BAD_PARAM;
    }

    /** check for wildcard character - handle appropriately */
    if (0 == strcmp(ORTE_SCHEMA_WILDCARD_STRING, vpidstring)) {
        *vpid = ORTE_VPID_WILDCARD;
        return ORTE_SUCCESS;
    }

    /* check for invalid value */
    if (0 == strcmp(ORTE_SCHEMA_INVALID_STRING, vpidstring)) {
        *vpid = ORTE_VPID_INVALID;
        return ORTE_SUCCESS;
    }

    *vpid = strtol(vpidstring, NULL, 10);

    return ORTE_SUCCESS;
}

int orte_util_convert_string_to_process_name(orte_process_name_t *name,
                                             const char* name_string)
{
    char *temp, *token;
    orte_jobid_t job;
    orte_vpid_t vpid;
    int return_code=ORTE_SUCCESS;

    /* set default */
    name->jobid = ORTE_JOBID_INVALID;
    name->vpid = ORTE_VPID_INVALID;

    /* check for NULL string - error */
    if (NULL == name_string) {
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return ORTE_ERR_BAD_PARAM;
    }

    temp = strdup(name_string);  /** copy input string as the strtok process is destructive */
    token = strchr(temp, ORTE_SCHEMA_DELIMITER_CHAR); /** get first field -> jobid */

    /* check for error */
    if (NULL == token) {
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        free(temp);
        return ORTE_ERR_BAD_PARAM;
    }
    *token = '\0';
    token++;

    /* check for WILDCARD character - assign
     * value accordingly, if found
     */
    if (0 == strcmp(temp, ORTE_SCHEMA_WILDCARD_STRING)) {
        job = ORTE_JOBID_WILDCARD;
    } else if (0 == strcmp(temp, ORTE_SCHEMA_INVALID_STRING)) {
        job = ORTE_JOBID_INVALID;
    } else {
        job = strtoul(temp, NULL, 10);
    }

    /* check for WILDCARD character - assign
     * value accordingly, if found
     */
    if (0 == strcmp(token, ORTE_SCHEMA_WILDCARD_STRING)) {
        vpid = ORTE_VPID_WILDCARD;
    } else if (0 == strcmp(token, ORTE_SCHEMA_INVALID_STRING)) {
        vpid = ORTE_VPID_INVALID;
    } else {
        vpid = strtoul(token, NULL, 10);
    }

    name->jobid = job;
    name->vpid = vpid;

    free(temp);

    return return_code;
}

int orte_util_convert_process_name_to_string(char **name_string,
                                             const orte_process_name_t* name)
{
    char *tmp, *tmp2;

    if (NULL == name) { /* got an error */
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return ORTE_ERR_BAD_PARAM;
    }

    /* check for wildcard and invalid values - where encountered, insert the
     * corresponding string so we can correctly parse the name string when
     * it is passed back to us later
     */
    if (ORTE_JOBID_WILDCARD == name->jobid) {
        asprintf(&tmp, "%s", ORTE_SCHEMA_WILDCARD_STRING);
    } else if (ORTE_JOBID_INVALID == name->jobid) {
        asprintf(&tmp, "%s", ORTE_SCHEMA_INVALID_STRING);
    } else {
        asprintf(&tmp, "%lu", (unsigned long)name->jobid);
    }

    if (ORTE_VPID_WILDCARD == name->vpid) {
        asprintf(&tmp2, "%s%c%s", tmp, ORTE_SCHEMA_DELIMITER_CHAR, ORTE_SCHEMA_WILDCARD_STRING);
    } else if (ORTE_VPID_INVALID == name->vpid) {
        asprintf(&tmp2, "%s%c%s", tmp, ORTE_SCHEMA_DELIMITER_CHAR, ORTE_SCHEMA_INVALID_STRING);
    } else {
        asprintf(&tmp2, "%s%c%lu", tmp, ORTE_SCHEMA_DELIMITER_CHAR, (unsigned long)name->vpid);
    }

    asprintf(name_string, "%s", tmp2);

    free(tmp);
    free(tmp2);

    return ORTE_SUCCESS;
}


/****    CREATE PROCESS NAME    ****/
int orte_util_create_process_name(orte_process_name_t **name,
                                  orte_jobid_t job,
                                  orte_vpid_t vpid
                                  )
{
    *name = NULL;

    *name = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
    if (NULL == *name) { /* got an error */
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    (*name)->jobid = job;
    (*name)->vpid = vpid;

    return ORTE_SUCCESS;
}

/****    COMPARE NAME FIELDS     ****/
int orte_util_compare_name_fields(orte_ns_cmp_bitmask_t fields,
                                  const orte_process_name_t* name1,
                                  const orte_process_name_t* name2)
{
    /* handle the NULL pointer case */
    if (NULL == name1 && NULL == name2) {
        return OPAL_EQUAL;
    } else if (NULL == name1) {
        return OPAL_VALUE2_GREATER;
    } else if (NULL == name2) {
        return OPAL_VALUE1_GREATER;
    }

    /* in this comparison function, we check for exact equalities.
     * In the case of wildcards, we check to ensure that the fields
     * actually match those values - thus, a "wildcard" in this
     * function does not actually stand for a wildcard value, but
     * rather a specific value - UNLESS the CMP_WILD bitmask value
     * is set
     */

    /* check job id */
    if (ORTE_NS_CMP_JOBID & fields) {
        if (ORTE_NS_CMP_WILD & fields &&
            (ORTE_JOBID_WILDCARD == name1->jobid ||
             ORTE_JOBID_WILDCARD == name2->jobid)) {
            goto check_vpid;
        }
        if (name1->jobid < name2->jobid) {
            return OPAL_VALUE2_GREATER;
        } else if (name1->jobid > name2->jobid) {
            return OPAL_VALUE1_GREATER;
        }
    }

    /* get here if jobid's are equal, or not being checked
     * now check vpid
     */
 check_vpid:
    if (ORTE_NS_CMP_VPID & fields) {
        if (ORTE_NS_CMP_WILD & fields &&
            (ORTE_VPID_WILDCARD == name1->vpid ||
             ORTE_VPID_WILDCARD == name2->vpid)) {
            return OPAL_EQUAL;
        }
        if (name1->vpid < name2->vpid) {
            return OPAL_VALUE2_GREATER;
        } else if (name1->vpid > name2->vpid) {
            return OPAL_VALUE1_GREATER;
        }
    }

    /* only way to get here is if all fields are being checked and are equal,
    * or jobid not checked, but vpid equal,
    * only vpid being checked, and equal
    * return that fact
    */
    return OPAL_EQUAL;
}

/* hash a vpid based on Robert Jenkin's algorithm - note
 * that the precise values of the constants in the algo are
 * irrelevant.
 */
uint32_t  orte_util_hash_vpid(orte_vpid_t vpid) {
    uint32_t hash;

    hash = vpid;
    hash = (hash + 0x7ed55d16) + (hash<<12);
    hash = (hash ^ 0xc761c23c) ^ (hash>>19);
    hash = (hash + 0x165667b1) + (hash<<5);
    hash = (hash + 0xd3a2646c) ^ (hash<<9);
    hash = (hash + 0xfd7046c5) + (hash<<3);
    hash = (hash ^ 0xb55a4f09) ^ (hash>>16);
    return hash;
}

/* sysinfo conversion to and from string */
int orte_util_convert_string_to_sysinfo(char **cpu_type, char **cpu_model,
					const char* sysinfo_string)
{
    char *temp, *token;
    int return_code=ORTE_SUCCESS;

    /* check for NULL string - error */
    if (NULL == sysinfo_string) {
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return ORTE_ERR_BAD_PARAM;
    }

    temp = strdup(sysinfo_string);  /** copy input string as the strtok process is destructive */
    token = strchr(temp, ORTE_SCHEMA_DELIMITER_CHAR); /** get first field -> cpu_type */

    /* check for error */
    if (NULL == token) {
        free(temp);
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return ORTE_ERR_BAD_PARAM;
    }
    *token = '\0';
    token++;

    /* If type is a valid string get the value otherwise leave cpu_type untouched.
     */
    if (0 != strcmp(temp, ORTE_SCHEMA_INVALID_STRING)) {
        *cpu_type = strdup(temp);
    }

    /* If type is a valid string get the value otherwise leave cpu_type untouched.
     */
    if (0 != strcmp(token, ORTE_SCHEMA_INVALID_STRING)) {
        *cpu_model = strdup(token);
    }

    free(temp);

    return return_code;
}

int orte_util_convert_sysinfo_to_string(char **sysinfo_string,
					const char *cpu_type, const char *cpu_model)
{
    char *tmp;

    /* check for no sysinfo values (like empty cpu_type) - where encountered, insert the
     * invalid string so we can correctly parse the name string when
     * it is passed back to us later
     */
    if (NULL == cpu_type) {
        asprintf(&tmp, "%s", ORTE_SCHEMA_INVALID_STRING);
    } else {
        asprintf(&tmp, "%s", cpu_type);
    }

    if (NULL == cpu_model) {
        asprintf(sysinfo_string, "%s%c%s", tmp, ORTE_SCHEMA_DELIMITER_CHAR, ORTE_SCHEMA_INVALID_STRING);
    } else {
        asprintf(sysinfo_string, "%s%c%s", tmp, ORTE_SCHEMA_DELIMITER_CHAR, cpu_model);
    }
    free(tmp);
    return ORTE_SUCCESS;
}

char *orte_pretty_print_timing(int64_t secs, int64_t usecs)
{
    unsigned long minutes, seconds;
    float fsecs;
    char *timestring;

    seconds = secs + (usecs / 1000000l);
    minutes = seconds / 60l;
    seconds = seconds % 60l;
    if (0 == minutes && 0 == seconds) {
        fsecs = ((float)(secs)*1000000.0 + (float)usecs) / 1000.0;
        asprintf(&timestring, "%8.2f millisecs", fsecs);
    } else {
        asprintf(&timestring, "%3lu:%02lu min:sec", minutes, seconds);
    }

    return timestring;
}


