/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include <stdio.h>

#include "ompi/constants.h"
#include "ompi/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"


#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/coll/base/coll_base_functions.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "ompi/mca/coll/base/static-components.h"

/*
 * Ensure all function pointers are NULL'ed out to start with
 */
static void coll_base_module_construct(mca_coll_base_module_t *m)
{
    /* zero out all functions */
    memset ((char *) m + sizeof (m->super), 0, sizeof (*m) - sizeof (m->super));
    m->coll_module_disable = NULL;
    m->base_data = NULL;
}

static void
coll_base_module_destruct(mca_coll_base_module_t *module)
{
    if (NULL != module->base_data) {
        OBJ_RELEASE(module->base_data);
    }
}

OBJ_CLASS_INSTANCE(mca_coll_base_module_t, opal_object_t,
                   coll_base_module_construct, coll_base_module_destruct);


static void
coll_base_comm_construct(mca_coll_base_comm_t *data)
{
    memset ((char *) data + sizeof (data->super), 0, sizeof (*data) - sizeof (data->super));
}

static void
coll_base_comm_destruct(mca_coll_base_comm_t *data)
{
    if( NULL != data->mcct_reqs ) {
        ompi_coll_base_free_reqs( data->mcct_reqs, data->mcct_num_reqs );
        free(data->mcct_reqs);
        data->mcct_reqs = NULL;
        data->mcct_num_reqs = 0;
    }
    assert(0 == data->mcct_num_reqs);

    /* free any cached information that has been allocated */
    if (data->cached_ntree) { /* destroy general tree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_ntree);
    }
    if (data->cached_bintree) { /* destroy bintree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_bintree);
    }
    if (data->cached_bmtree) { /* destroy bmtree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_bmtree);
    }
    if (data->cached_in_order_bmtree) { /* destroy bmtree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_in_order_bmtree);
    }
    if (data->cached_kmtree) { /* destroy kmtree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_kmtree);
    }
    if (data->cached_chain) { /* destroy general chain if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_chain);
    }
    if (data->cached_pipeline) { /* destroy pipeline if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_pipeline);
    }
    if (data->cached_in_order_bintree) { /* destroy in order bintree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_in_order_bintree);
    }
}

OBJ_CLASS_INSTANCE(mca_coll_base_comm_t, opal_object_t,
                   coll_base_comm_construct, coll_base_comm_destruct);

ompi_request_t** ompi_coll_base_comm_get_reqs(mca_coll_base_comm_t* data, int nreqs)
{
    if( 0 == nreqs ) return NULL;

    if( data->mcct_num_reqs < nreqs ) {
        data->mcct_reqs = (ompi_request_t**)realloc(data->mcct_reqs, sizeof(ompi_request_t*) * nreqs);

        if( NULL != data->mcct_reqs ) {
            for( int i = data->mcct_num_reqs; i < nreqs; i++ )
                data->mcct_reqs[i] = MPI_REQUEST_NULL;
            data->mcct_num_reqs = nreqs;
        } else
            data->mcct_num_reqs = 0;  /* nothing to return */
    }
    return data->mcct_reqs;
}

MCA_BASE_FRAMEWORK_DECLARE(ompi, coll, "Collectives", NULL, NULL, NULL,
                           mca_coll_base_static_components, 0);
