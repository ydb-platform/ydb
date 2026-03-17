/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/constants.h"
#include "ompi/types.h"

#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/base.h"

#include "ompi/mca/bml/base/base.h"

int mca_pml_base_ft_event(int state)
{
    int ret;

#if 0
    opal_output(0, "pml:base: ft_event: Called (%d)!!\n", state);
#endif

    if(OPAL_CRS_CHECKPOINT == state) {
        ;
    }
    else if(OPAL_CRS_CONTINUE == state) {
        ;
    }
    else if(OPAL_CRS_RESTART == state) {
        ;
    }
    else if(OPAL_CRS_TERM == state ) {
        ;
    }
    else {
        ;
    }

    /* Call the BML
     * BML is expected to call ft_event in
     * - BTL(s)
     * - MPool(s)
     */
    if( OMPI_SUCCESS != (ret = mca_bml.bml_ft_event(state))) {
        opal_output(0, "pml:base: ft_event: BML ft_event function failed: %d\n",
                    ret);
    }

    if(OPAL_CRS_CHECKPOINT == state) {
        ;
    }
    else if(OPAL_CRS_CONTINUE == state) {
        ;
    }
    else if(OPAL_CRS_RESTART == state) {
        ;
    }
    else if(OPAL_CRS_TERM == state ) {
        ;
    }
    else {
        ;
    }

    return OMPI_SUCCESS;
}
