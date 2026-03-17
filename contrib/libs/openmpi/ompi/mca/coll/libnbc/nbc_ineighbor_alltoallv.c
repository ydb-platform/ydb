/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2006      The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2006      The Technical University of Chemnitz. All
 *                         rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * Author(s): Torsten Hoefler <htor@cs.indiana.edu>
 *
 */
#include "nbc_internal.h"

/* cannot cache schedules because one cannot check locally if the pattern is the same!! */
#undef NBC_CACHE_SCHEDULE

#ifdef NBC_CACHE_SCHEDULE
/* tree comparison function for schedule cache */
int NBC_Ineighbor_alltoallv_args_compare(NBC_Ineighbor_alltoallv_args *a, NBC_Ineighbor_alltoallv_args *b, void *param) {
  if ((a->sbuf == b->sbuf) &&
      (a->scount == b->scount) &&
      (a->stype == b->stype) &&
      (a->rbuf == b->rbuf) &&
      (a->rcount == b->rcount) &&
      (a->rtype == b->rtype) ) {
    return 0;
  }

  if (a->sbuf < b->sbuf) {
    return -1;
  }

  return 1;
}
#endif


static int nbc_neighbor_alltoallv_init(const void *sbuf, const int *scounts, const int *sdispls, MPI_Datatype stype,
                                       void *rbuf, const int *rcounts, const int *rdispls, MPI_Datatype rtype,
                                       struct ompi_communicator_t *comm, ompi_request_t ** request,
                                       struct mca_coll_base_module_2_3_0_t *module, bool persistent) {
  int res, indegree, outdegree, *srcs, *dsts;
  MPI_Aint sndext, rcvext;
  ompi_coll_libnbc_module_t *libnbc_module = (ompi_coll_libnbc_module_t*) module;
  NBC_Schedule *schedule;

  res = ompi_datatype_type_extent (stype, &sndext);
  if (MPI_SUCCESS != res) {
    NBC_Error("MPI Error in ompi_datatype_type_extent() (%i)", res);
    return res;
  }

  res = ompi_datatype_type_extent (rtype, &rcvext);
  if (MPI_SUCCESS != res) {
    NBC_Error("MPI Error in ompi_datatype_type_extent() (%i)", res);
    return res;
  }

#ifdef NBC_CACHE_SCHEDULE
  NBC_Ineighbor_alltoallv_args *args, *found, search;

  /* search schedule in communicator specific tree */
  search.sbuf = sbuf;
  search.scount = scount;
  search.stype = stype;
  search.rbuf = rbuf;
  search.rcount = rcount;
  search.rtype = rtype;
  found = (NBC_Ineighbor_alltoallv_args *) hb_tree_search ((hb_tree *) libnbc_module->NBC_Dict[NBC_NEIGHBOR_ALLTOALLV],
                                                           &search);
  if (NULL == found) {
#endif
    schedule = OBJ_NEW(NBC_Schedule);
    if (OPAL_UNLIKELY(NULL == schedule)) {
      return OMPI_ERR_OUT_OF_RESOURCE;
    }

    res = NBC_Comm_neighbors (comm, &srcs, &indegree, &dsts, &outdegree);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      OBJ_RELEASE(schedule);
      return res;
    }

    /* simply loop over neighbors and post send/recv operations */
    for (int i = 0 ; i < indegree ; ++i) {
      if (srcs[i] != MPI_PROC_NULL) {
        res = NBC_Sched_recv ((char *) rbuf + rdispls[i] * rcvext, false, rcounts[i], rtype, srcs[i], schedule, false);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          break;
        }
      }
    }

    free (srcs);

    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      OBJ_RELEASE(schedule);
      free (dsts);
      return res;
    }

    for (int i = 0 ; i < outdegree ; ++i) {
      if (dsts[i] != MPI_PROC_NULL) {
        res = NBC_Sched_send ((char *) sbuf + sdispls[i] * sndext, false, scounts[i], stype, dsts[i], schedule, false);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          break;
        }
      }
    }

    free (dsts);

    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      OBJ_RELEASE(schedule);
      return res;
    }

    res = NBC_Sched_commit (schedule);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      OBJ_RELEASE(schedule);
      return res;
    }

#ifdef NBC_CACHE_SCHEDULE
    /* save schedule to tree */
    args = (NBC_Ineighbor_alltoallv_args *) malloc (sizeof (args));
    if (NULL != args) {
      args->sbuf = sbuf;
      args->scount = scount;
      args->stype = stype;
      args->rbuf = rbuf;
      args->rcount = rcount;
      args->rtype = rtype;
      args->schedule = schedule;
      res = hb_tree_insert ((hb_tree *) libnbc_module->NBC_Dict[NBC_NEIGHBOR_ALLTOALLV], args, args, 0);
      if (0 == res) {
        OBJ_RETAIN(schedule);

        /* increase number of elements for A2A */
        if (++libnbc_module->NBC_Dict_size[NBC_NEIGHBOR_ALLTOALLV] > NBC_SCHED_DICT_UPPER) {
          NBC_SchedCache_dictwipe ((hb_tree *) libnbc_module->NBC_Dict[NBC_NEIGHBOR_ALLTOALLV],
                                   &libnbc_module->NBC_Dict_size[NBC_NEIGHBOR_ALLTOALLV]);
        }
      } else {
        NBC_Error("error in dict_insert() (%i)", res);
        free (args);
      }
    }
  } else {
    /* found schedule */
    schedule = found->schedule;
    OBJ_RETAIN(schedule);
  }
#endif

  res = NBC_Schedule_request(schedule, comm, libnbc_module, persistent, request, NULL);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    OBJ_RELEASE(schedule);
    return res;
  }

  return OMPI_SUCCESS;
}

int ompi_coll_libnbc_ineighbor_alltoallv(const void *sbuf, const int *scounts, const int *sdispls, MPI_Datatype stype,
                                         void *rbuf, const int *rcounts, const int *rdispls, MPI_Datatype rtype,
                                         struct ompi_communicator_t *comm, ompi_request_t ** request,
                                         struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_neighbor_alltoallv_init(sbuf, scounts, sdispls, stype, rbuf, rcounts, rdispls, rtype,
                                          comm, request, module, false);
    if (OPAL_LIKELY(OMPI_SUCCESS != res)) {
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

int ompi_coll_libnbc_neighbor_alltoallv_init(const void *sbuf, const int *scounts, const int *sdispls, MPI_Datatype stype,
                                             void *rbuf, const int *rcounts, const int *rdispls, MPI_Datatype rtype,
                                             struct ompi_communicator_t *comm, MPI_Info info, ompi_request_t ** request,
                                             struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_neighbor_alltoallv_init(sbuf, scounts, sdispls, stype, rbuf, rcounts, rdispls, rtype,
                                          comm, request, module, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
    }

    return OMPI_SUCCESS;
}
