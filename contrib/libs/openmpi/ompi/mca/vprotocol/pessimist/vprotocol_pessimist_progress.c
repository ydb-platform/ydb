/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "vprotocol_pessimist_sender_based.h"
#include "vprotocol_pessimist.h"

#ifdef SB_USE_PROGRESS_METHOD

int mca_vprotocol_pessimist_progress(void)
{
    int ret;

    printf("PROGRESS\n");
    /* First let the real progress take place */
    ret = mca_pml_v.host_pml.pml_progress();
    /* Then progess the sender_based copies */
    vprotocol_pessimist_sb_progress_all_reqs();
    return ret;
}

#endif
