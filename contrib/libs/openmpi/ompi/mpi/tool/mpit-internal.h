/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2011      UT-Battelle, LLC. All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(MPIT_INTERNAL_H)
#define MPIT_INTERNAL_H

#include "ompi/include/ompi_config.h"
#include "opal/mca/base/mca_base_var.h"
#include "opal/mca/base/mca_base_pvar.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"

#include "mpi.h"

#include <string.h>

typedef struct ompi_mpit_cvar_handle_t {
    const mca_base_var_t *var;
    /* XXX -- TODO -- allow binding objects */
    void           *bound_object;
} ompi_mpit_cvar_handle_t;

void ompi_mpit_lock (void);
void ompi_mpit_unlock (void);

extern volatile uint32_t ompi_mpit_init_count;

int ompit_var_type_to_datatype (mca_base_var_type_t type, MPI_Datatype *datatype);
int ompit_opal_to_mpit_error (int rc);

static inline int mpit_is_initialized (void)
{
    return !!ompi_mpit_init_count;
}

static inline void mpit_copy_string (char *dest, int *len, const char *source)
{
    if (NULL == len)
        return;

    if (NULL == source) {
        *len = 0;
        if (NULL != dest) {
            dest[0] = '\0';
        }

        return;
    }

    if (0 != *len && NULL != dest) {
        if ((int) strlen (source) < *len) {
            *len = strlen (source) + 1;
        }

        strncpy (dest, source, *len);
        dest[*len - 1] = '\0';
    } else {
        *len = strlen (source) + 1;
    }
}

#endif /* !defined(MPIT_INTERNAL_H) */
