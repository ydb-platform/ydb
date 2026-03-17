/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2006      The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2006      The Technical University of Chemnitz. All
 *                         rights reserved.
 *
 * Author(s): Torsten Hoefler <htor@cs.indiana.edu>
 *
 * Copyright (c) 2012      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 */
#include "nbc_internal.h"

/* an allgatherv schedule can not be cached easily because the contents
 * ot the recvcounts array may change, so a comparison of the address
 * would not be sufficient ... we simply do not cache it */

/* simple linear MPI_Iallgatherv
 * the algorithm uses p-1 rounds
 * first round:
 *   each node sends to it's left node (rank+1)%p sendcount elements
 *   each node begins with it's right node (rank-11)%p and receives from it recvcounts[(rank+1)%p] elements
 * second round:
 *   each node sends to node (rank+2)%p sendcount elements
 *   each node receives from node (rank-2)%p recvcounts[(rank+2)%p] elements */
static int nbc_allgatherv_init(const void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, const int *recvcounts, const int *displs,
                               MPI_Datatype recvtype, struct ompi_communicator_t *comm, ompi_request_t ** request,
                               struct mca_coll_base_module_2_3_0_t *module, bool persistent)
{
  int rank, p, res, speer, rpeer;
  MPI_Aint rcvext;
  NBC_Schedule *schedule;
  char *rbuf, *sbuf, inplace;
  ompi_coll_libnbc_module_t *libnbc_module = (ompi_coll_libnbc_module_t*) module;

  NBC_IN_PLACE(sendbuf, recvbuf, inplace);

  rank = ompi_comm_rank (comm);
  p = ompi_comm_size (comm);

  res = ompi_datatype_type_extent (recvtype, &rcvext);
  if (OPAL_UNLIKELY(MPI_SUCCESS != res)) {
    NBC_Error ("MPI Error in ompi_datatype_type_extent() (%i)", res);
    return res;
  }

  if (inplace) {
      sendtype = recvtype;
      sendcount = recvcounts[rank];
  } else if (!persistent) { /* for persistent, the copy must be scheduled */
    /* copy my data to receive buffer */
    rbuf = (char *) recvbuf + displs[rank] * rcvext;
    res = NBC_Copy (sendbuf, sendcount, sendtype, rbuf, recvcounts[rank], recvtype, comm);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      return res;
    }
  }

  schedule = OBJ_NEW(NBC_Schedule);
  if (NULL == schedule) {
    return OMPI_ERR_OUT_OF_RESOURCE;
  }

  sbuf = (char *) recvbuf + displs[rank] * rcvext;

  if (persistent && !inplace) { /* for nonblocking, data has been copied already */
    /* copy my data to receive buffer (= send buffer of NBC_Sched_send) */
    res = NBC_Sched_copy ((void *)sendbuf, false, sendcount, sendtype,
                          sbuf, false, recvcounts[rank], recvtype, schedule, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      OBJ_RELEASE(schedule);
      return res;
    }
  }

  /* do p-1 rounds */
  for (int r = 1 ; r < p ; ++r) {
    speer = (rank + r) % p;
    rpeer = (rank - r + p) % p;
    rbuf = (char *)recvbuf + displs[rpeer] * rcvext;

    res = NBC_Sched_recv (rbuf, false, recvcounts[rpeer], recvtype, rpeer, schedule, false);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      OBJ_RELEASE(schedule);
      return res;
    }

    /* send to rank r - not from the sendbuf to optimize MPI_IN_PLACE */
    res = NBC_Sched_send (sbuf, false, recvcounts[rank], recvtype, speer, schedule, false);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      OBJ_RELEASE(schedule);
      return res;
    }
  }

  res = NBC_Sched_commit (schedule);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    OBJ_RELEASE(schedule);
    return res;
  }

  res = NBC_Schedule_request (schedule, comm, libnbc_module, persistent, request, NULL);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    OBJ_RELEASE(schedule);
    return res;
  }

  return OMPI_SUCCESS;
}

int ompi_coll_libnbc_iallgatherv(const void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, const int *recvcounts, const int *displs,
                                 MPI_Datatype recvtype, struct ompi_communicator_t *comm, ompi_request_t ** request,
                                 struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_allgatherv_init(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype,
                                  comm, request, module, false);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
    }
  
    res = NBC_Start(*(ompi_coll_libnbc_request_t **)request);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        NBC_Return_handle (*(ompi_coll_libnbc_request_t **)request);
        *request = &ompi_request_null.request;
        return res;
    }

    return OMPI_SUCCESS;
}

static int nbc_allgatherv_inter_init(const void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, const int *recvcounts, const int *displs,
                                     MPI_Datatype recvtype, struct ompi_communicator_t *comm, ompi_request_t ** request,
                                     struct mca_coll_base_module_2_3_0_t *module, bool persistent)
{
  int res, rsize;
  MPI_Aint rcvext;
  NBC_Schedule *schedule;
  ompi_coll_libnbc_module_t *libnbc_module = (ompi_coll_libnbc_module_t*) module;

  rsize = ompi_comm_remote_size (comm);

  res = ompi_datatype_type_extent(recvtype, &rcvext);
  if (OPAL_UNLIKELY(MPI_SUCCESS != res)) {
    NBC_Error ("MPI Error in ompi_datatype_type_extent() (%i)", res);
    return res;
  }

  schedule = OBJ_NEW(NBC_Schedule);
  if (NULL == schedule) {
    return OMPI_ERR_OUT_OF_RESOURCE;
  }

  /* do rsize  rounds */
  for (int r = 0 ; r < rsize ; ++r) {
    char *rbuf = (char *) recvbuf + displs[r] * rcvext;

    if (recvcounts[r]) {
      res = NBC_Sched_recv (rbuf, false, recvcounts[r], recvtype, r, schedule, false);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        OBJ_RELEASE(schedule);
        return res;
      }
    }
  }

  if (sendcount) {
    for (int r = 0 ; r < rsize ; ++r) {
      res = NBC_Sched_send (sendbuf, false, sendcount, sendtype, r, schedule, false);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        OBJ_RELEASE(schedule);
        return res;
      }
    }
  }

  res = NBC_Sched_commit (schedule);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    OBJ_RELEASE(schedule);
    return res;
  }

  res = NBC_Schedule_request(schedule, comm, libnbc_module, persistent, request, NULL);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    OBJ_RELEASE(schedule);
    return res;
  }

  return OMPI_SUCCESS;
}

int ompi_coll_libnbc_iallgatherv_inter(const void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, const int *recvcounts, const int *displs,
                                       MPI_Datatype recvtype, struct ompi_communicator_t *comm, ompi_request_t ** request,
                                       struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_allgatherv_inter_init(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype,
                                        comm, request, module, false);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
    }
  
    res = NBC_Start(*(ompi_coll_libnbc_request_t **)request);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        NBC_Return_handle (*(ompi_coll_libnbc_request_t **)request);
        *request = &ompi_request_null.request;
        return res;
    }

    return OMPI_SUCCESS;
}

int ompi_coll_libnbc_allgatherv_init(const void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, const int *recvcounts, const int *displs,
                                     MPI_Datatype recvtype, struct ompi_communicator_t *comm, MPI_Info info, ompi_request_t ** request,
                                     struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_allgatherv_init(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype,
                                  comm, request, module, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
    }

    return OMPI_SUCCESS;
}

int ompi_coll_libnbc_allgatherv_inter_init(const void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, const int *recvcounts, const int *displs,
                                           MPI_Datatype recvtype, struct ompi_communicator_t *comm, MPI_Info info, ompi_request_t ** request,
                                           struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_allgatherv_inter_init(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype,
                                        comm, request, module, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
    }

    return OMPI_SUCCESS;
}
