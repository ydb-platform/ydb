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
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "coll_self.h"


/*
 *	gatherv_intra
 *
 *	Function:	- gatherv
 *	Accepts:	- same arguments as MPI_Gatherv()
 *	Returns:	- MPI_SUCCESS or error code
 */
int mca_coll_self_gatherv_intra(const void *sbuf, int scount,
                                struct ompi_datatype_t *sdtype,
                                void *rbuf, const int *rcounts, const int *disps,
                                struct ompi_datatype_t *rdtype, int root,
                                struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module)
{
    if (MPI_IN_PLACE == sbuf) {
        return MPI_SUCCESS;
    } else {
        int err;
        ptrdiff_t lb, extent;
        err = ompi_datatype_get_extent(rdtype, &lb, &extent);
        if (OMPI_SUCCESS != err) {
            return OMPI_ERROR;
        }
        return ompi_datatype_sndrcv(sbuf, scount, sdtype,
                               ((char *) rbuf) + disps[0]*extent,
                               rcounts[0], rdtype);
    }
}
