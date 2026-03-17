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
 * Copyright (c) 2013      FUJITSU LIMITED.  All rights reserved.
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
 *	alltoallv_intra
 *
 *	Function:	- MPI_Alltoallv
 *	Accepts:	- same as MPI_Alltoallv()
 *	Returns:	- MPI_SUCCESS or an MPI error code
 */
int
mca_coll_self_alltoallv_intra(const void *sbuf, const int *scounts, const int *sdisps,
                              struct ompi_datatype_t *sdtype,
                              void *rbuf, const int *rcounts, const int *rdisps,
                              struct ompi_datatype_t *rdtype,
                              struct ompi_communicator_t *comm,
                              mca_coll_base_module_t *module)
{
    int err;
    ptrdiff_t lb, rextent, sextent;

    if (MPI_IN_PLACE == sbuf) {
        return MPI_SUCCESS;
    }

    err = ompi_datatype_get_extent(sdtype, &lb, &sextent);
    if (OMPI_SUCCESS != err) {
        return OMPI_ERROR;
    }
    err = ompi_datatype_get_extent(rdtype, &lb, &rextent);
    if (OMPI_SUCCESS != err) {
        return OMPI_ERROR;
    }
    return ompi_datatype_sndrcv(((char *) sbuf) + sdisps[0] * sextent,
                           scounts[0], sdtype,
                           ((char *) rbuf) + rdisps[0] * rextent,
                           rcounts[0], rdtype);
}
