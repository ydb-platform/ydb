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
 * Copyright (c) 2008-2015 University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "fcoll_individual.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/fcoll/fcoll.h"
#include "ompi/mca/common/ompio/common_ompio.h"
#include "math.h"
#include <unistd.h>


/* this component just directly calls the individual I/O operations */

int
mca_fcoll_individual_file_read_all (ompio_file_t *fh,
                                    void *buf,
                                    int count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_status_public_t *status)
{
    return mca_common_ompio_file_read( fh, buf, count, datatype, status);
}
