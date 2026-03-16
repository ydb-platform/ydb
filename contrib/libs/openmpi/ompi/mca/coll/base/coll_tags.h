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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COLL_BASE_TAGS_H
#define MCA_COLL_BASE_TAGS_H

/*
 * Tags that can be used for MPI point-to-point functions when
 * implementing collectives via point-to-point.
 */

#define MCA_COLL_BASE_TAG_ALLGATHER -10
#define MCA_COLL_BASE_TAG_ALLGATHERV -11
#define MCA_COLL_BASE_TAG_ALLREDUCE -12
#define MCA_COLL_BASE_TAG_ALLTOALL -13
#define MCA_COLL_BASE_TAG_ALLTOALLV -14
#define MCA_COLL_BASE_TAG_ALLTOALLW -15
#define MCA_COLL_BASE_TAG_BARRIER -16
#define MCA_COLL_BASE_TAG_BCAST -17
#define MCA_COLL_BASE_TAG_EXSCAN -18
#define MCA_COLL_BASE_TAG_GATHER -19
#define MCA_COLL_BASE_TAG_GATHERV -20
#define MCA_COLL_BASE_TAG_REDUCE -21
#define MCA_COLL_BASE_TAG_REDUCE_SCATTER -22
#define MCA_COLL_BASE_TAG_REDUCE_SCATTER_BLOCK -23
#define MCA_COLL_BASE_TAG_SCAN -24
#define MCA_COLL_BASE_TAG_SCATTER -25
#define MCA_COLL_BASE_TAG_SCATTERV -26
#define MCA_COLL_BASE_TAG_NONBLOCKING_BASE -27
#define MCA_COLL_BASE_TAG_NONBLOCKING_END ((-1 * INT_MAX/2) + 1)
#define MCA_COLL_BASE_TAG_HCOLL_BASE (-1 * INT_MAX/2)
#define MCA_COLL_BASE_TAG_HCOLL_END (-1 * INT_MAX)
#endif /* MCA_COLL_BASE_TAGS_H */
