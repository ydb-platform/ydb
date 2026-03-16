/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2016 University of Houston. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_FCOLL_BASE_COLL_ARRAY_H
#define MCA_FCOLL_BASE_COLL_ARRAY_H

#include "mpi.h"
#include "opal/class/opal_list.h"
#include "ompi/communicator/communicator.h"
#include "ompi/info/info.h"
#include "opal/datatype/opal_convertor.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/request/request.h"

#define FCOLL_TAG_GATHER              100
#define FCOLL_TAG_GATHERV             101
#define FCOLL_TAG_BCAST               102
#define FCOLL_TAG_SCATTERV            103


/*
 * Modified versions of Collective operations
 * Based on an array of procs in group
 */
OMPI_DECLSPEC int ompi_fcoll_base_coll_gatherv_array (void *sbuf,
                                                 int scount,
                                                 ompi_datatype_t *sdtype,
                                                 void *rbuf,
                                                 int *rcounts,
                                                 int *disps,
                                                 ompi_datatype_t *rdtype,
                                                 int root_index,
                                                 int *procs_in_group,
                                                 int procs_per_group,
                                                 ompi_communicator_t *comm);
OMPI_DECLSPEC int ompi_fcoll_base_coll_scatterv_array (void *sbuf,
                                                  int *scounts,
                                                  int *disps,
                                                  ompi_datatype_t *sdtype,
                                                  void *rbuf,
                                                  int rcount,
                                                  ompi_datatype_t *rdtype,
                                                  int root_index,
                                                  int *procs_in_group,
                                                  int procs_per_group,
                                                  ompi_communicator_t *comm);
OMPI_DECLSPEC int ompi_fcoll_base_coll_allgather_array (void *sbuf,
                                                   int scount,
                                                   ompi_datatype_t *sdtype,
                                                   void *rbuf,
                                                   int rcount,
                                                   ompi_datatype_t *rdtype,
                                                   int root_index,
                                                   int *procs_in_group,
                                                   int procs_per_group,
                                                   ompi_communicator_t *comm);

OMPI_DECLSPEC int ompi_fcoll_base_coll_allgatherv_array (void *sbuf,
                                                    int scount,
                                                    ompi_datatype_t *sdtype,
                                                    void *rbuf,
                                                    int *rcounts,
                                                    int *disps,
                                                    ompi_datatype_t *rdtype,
                                                    int root_index,
                                                    int *procs_in_group,
                                                    int procs_per_group,
                                                    ompi_communicator_t *comm);
OMPI_DECLSPEC int ompi_fcoll_base_coll_gather_array (void *sbuf,
                                                int scount,
                                                ompi_datatype_t *sdtype,
                                                void *rbuf,
                                                int rcount,
                                                ompi_datatype_t *rdtype,
                                                int root_index,
                                                int *procs_in_group,
                                                int procs_per_group,
                                                ompi_communicator_t *comm);
OMPI_DECLSPEC int ompi_fcoll_base_coll_bcast_array (void *buff,
                                               int count,
                                               ompi_datatype_t *datatype,
                                               int root_index,
                                               int *procs_in_group,
                                               int procs_per_group,
                                               ompi_communicator_t *comm);

END_C_DECLS

#endif /* MCA_FCOLL_BASE_COLL_ARRAY_H */
