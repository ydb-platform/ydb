/*
 *
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
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * $Id: orte_universe_setup_file I/O functions $
 *
 */
#include "orte_config.h"
#include "orte/constants.h"

#include <stdio.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <stdarg.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif  /* HAVE_DIRENT_H */

#include "opal/util/os_path.h"
#include "opal/util/output.h"
#include "opal/util/os_dirpath.h"
#include "opal/mca/pmix/pmix.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/oob/base/base.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/routed/routed.h"

#include "orte/util/proc_info.h"
#include "orte/util/hnp_contact.h"

#define ORTE_HNP_CONTACT_FILE_MAX_LINE_LENGTH 1024

/* instantiate the hnp_contact object */
static void orte_hnp_contact_construct(orte_hnp_contact_t *ptr)
{
    ptr->name.jobid = ORTE_JOBID_INVALID;
    ptr->name.vpid = ORTE_VPID_INVALID;

    ptr->rml_uri = NULL;
}
static void orte_hnp_contact_destruct(orte_hnp_contact_t *ptr)
{
    if (NULL != ptr->rml_uri) free(ptr->rml_uri);
}
OBJ_CLASS_INSTANCE(orte_hnp_contact_t,
                   opal_list_item_t,
                   orte_hnp_contact_construct,
                   orte_hnp_contact_destruct);


static char *orte_getline(FILE *fp);

int orte_write_hnp_contact_file(char *filename)
{
    FILE *fp;
    char *my_uri;

    orte_oob_base_get_addr(&my_uri);
    if (NULL == my_uri) {
        return ORTE_ERROR;
    }

    fp = fopen(filename, "w");
    if (NULL == fp) {
        opal_output( 0, "Impossible to open the file %s in write mode\n",
                     filename );
        ORTE_ERROR_LOG(ORTE_ERR_FILE_OPEN_FAILURE);
        return ORTE_ERR_FILE_OPEN_FAILURE;
    }

    fprintf(fp, "%s\n", my_uri);
    free(my_uri);

    fprintf(fp, "%lu\n", (unsigned long)orte_process_info.pid);
    fclose(fp);

    return ORTE_SUCCESS;
}

int orte_read_hnp_contact_file(char *filename, orte_hnp_contact_t *hnp, bool connect)
{
    char *hnp_uri, *pidstr;
    FILE *fp;
    int rc;
    opal_value_t val;

    fp = fopen(filename, "r");
    if (NULL == fp) { /* failed on first read - wait and try again */
        fp = fopen(filename, "r");
        if (NULL == fp) { /* failed twice - give up */
            return ORTE_ERR_FILE_OPEN_FAILURE;
        }
    }

    hnp_uri = orte_getline(fp);
    if (NULL == hnp_uri) {
        ORTE_ERROR_LOG(ORTE_ERR_FILE_READ_FAILURE);
        fclose(fp);
        return ORTE_ERR_FILE_READ_FAILURE;
    }

    /* get the pid */
    pidstr = orte_getline(fp);
    if (NULL == pidstr) {
        ORTE_ERROR_LOG(ORTE_ERR_FILE_READ_FAILURE);
        fclose(fp);
        free(hnp_uri);
        return ORTE_ERR_FILE_READ_FAILURE;
    }
    hnp->pid = (pid_t)atol(pidstr);
    free(pidstr);
    fclose(fp);

    if (connect) {
        /* extract the HNP's name and store it */
        if (ORTE_SUCCESS != (rc = orte_rml_base_parse_uris(hnp_uri, &hnp->name, NULL))) {
            ORTE_ERROR_LOG(rc);
            free(hnp_uri);
            return rc;
        }

        /* set the contact info into the comm hash tables*/
        OBJ_CONSTRUCT(&val, opal_value_t);
        val.key = OPAL_PMIX_PROC_URI;
        val.type = OPAL_STRING;
        val.data.string = hnp_uri;
        if (OPAL_SUCCESS != (rc = opal_pmix.store_local(&hnp->name, &val))) {
            ORTE_ERROR_LOG(rc);
            val.key = NULL;
            val.data.string = NULL;
            OBJ_DESTRUCT(&val);
            free(hnp_uri);
            return rc;
        }
        val.key = NULL;
        val.data.string = NULL;
        OBJ_DESTRUCT(&val);

        /* set the route to be direct */
        if (ORTE_SUCCESS != (rc = orte_routed.update_route(NULL, &hnp->name, &hnp->name))) {
            ORTE_ERROR_LOG(rc);
            free(hnp_uri);
            return rc;
        }
    }
    hnp->rml_uri = hnp_uri;

    return ORTE_SUCCESS;
}

static char *orte_getline(FILE *fp)
{
    char *ret, *buff;
    char input[ORTE_HNP_CONTACT_FILE_MAX_LINE_LENGTH];

    ret = fgets(input, ORTE_HNP_CONTACT_FILE_MAX_LINE_LENGTH, fp);
    if (NULL != ret) {
       input[strlen(input)-1] = '\0';  /* remove newline */
       buff = strdup(input);
       return buff;
    }

    return NULL;
}


int orte_list_local_hnps(opal_list_t *hnps, bool connect)
{
    int ret;
    DIR *cur_dirp = NULL;
    struct dirent * dir_entry;
    char *contact_filename = NULL;
    orte_hnp_contact_t *hnp;
    char *headdir;

    /*
     * Check to make sure we have access to the top-level directory
     */
    headdir = orte_process_info.top_session_dir;

    if( ORTE_SUCCESS != (ret = opal_os_dirpath_access(headdir, 0) )) {
        /* it is okay not to find this as there may not be any
         * HNP's present, and we don't write our own session dir
         */
        if (ORTE_ERR_NOT_FOUND != ret) {
            ORTE_ERROR_LOG(ret);
        }
        goto cleanup;
    }

    /*
     * Open up the base directory so we can get a listing
     */
    if( NULL == (cur_dirp = opendir(headdir)) ) {
        goto cleanup;
    }
    /*
     * For each directory
     */
    while( NULL != (dir_entry = readdir(cur_dirp)) ) {

        /*
         * Skip the obvious
         */
        if( 0 == strncmp(dir_entry->d_name, ".", strlen(".")) ||
            0 == strncmp(dir_entry->d_name, "..", strlen("..")) ) {
            continue;
        }

        /*
         * See if a contact file exists in this directory and read it
         */
        contact_filename = opal_os_path( false, headdir,
                                         dir_entry->d_name, "contact.txt", NULL );

        hnp = OBJ_NEW(orte_hnp_contact_t);
        if (ORTE_SUCCESS == (ret = orte_read_hnp_contact_file(contact_filename, hnp, connect))) {
            opal_list_append(hnps, &(hnp->super));
        } else {
            OBJ_RELEASE(hnp);
        }
        free(contact_filename);
     }

cleanup:
    if( NULL != cur_dirp )
        closedir(cur_dirp);

    return (opal_list_is_empty(hnps) ? ORTE_ERR_NOT_FOUND : ORTE_SUCCESS);
}
