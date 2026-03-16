/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <string.h>

#include "opal/class/opal_list.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_vari.h"
#include "opal/util/keyval_parse.h"
#include "opal/util/output.h"
static void save_value(const char *name, const char *value);

static char * file_being_read;
static opal_list_t * _param_list;

int mca_base_parse_paramfile(const char *paramfile, opal_list_t *list)
{
    file_being_read = (char*)paramfile;
    _param_list = list;

    return opal_util_keyval_parse(paramfile, save_value);
}

int mca_base_internal_env_store(void)
{
    return opal_util_keyval_save_internal_envars(save_value);
}

static void save_value(const char *name, const char *value)
{
    mca_base_var_file_value_t *fv;
    bool found = false;

    /* First traverse through the list and ensure that we don't
       already have a param of this name.  If we do, just replace the
       value. */

    OPAL_LIST_FOREACH(fv, _param_list, mca_base_var_file_value_t) {
        if (0 == strcmp(name, fv->mbvfv_var)) {
            if (NULL != fv->mbvfv_value) {
                free (fv->mbvfv_value);
            }
            found = true;
            break;
        }
    }

    if (!found) {
        /* We didn't already have the param, so append it to the list */
        fv = OBJ_NEW(mca_base_var_file_value_t);
        if (NULL == fv) {
            return;
        }

        fv->mbvfv_var = strdup(name);
        opal_list_append(_param_list, &fv->super);
    }

    fv->mbvfv_value = value ? strdup(value) : NULL;
    fv->mbvfv_file  = file_being_read;
    fv->mbvfv_lineno = opal_util_keyval_parse_lineno;
}
