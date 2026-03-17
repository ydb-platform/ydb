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
 * Copyright (c) 2006-2007 University of Houston. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/group/group.h"
#include "ompi/constants.h"

/*
 * Set group rank in a group structure.
 */
void ompi_set_group_rank(ompi_group_t *group, struct ompi_proc_t *proc_pointer)
{
    /* local variables */
    int proc;

    /* set the rank to MPI_UNDEFINED, just in case this process is not
     *   in this group
     */
    group->grp_my_rank = MPI_UNDEFINED;
    if (NULL != proc_pointer) {
        /* loop over all procs in the group */
        for (proc = 0; proc < group->grp_proc_count; proc++) {
            /* check and see if this proc pointer matches proc_pointer
             */
	    if (ompi_group_peer_lookup_existing (group, proc) == proc_pointer) {
                group->grp_my_rank = proc;
		break;
	    }
        }                       /* end proc loop */
    }
}
