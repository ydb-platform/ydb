/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2006      The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2006      The Technical University of Chemnitz. All
 *                         rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * Author(s): Torsten Hoefler <htor@cs.indiana.edu>
 *
 */
#include "nbc_internal.h"

static inline int bcast_sched_binomial(int rank, int p, int root, NBC_Schedule *schedule, void *buffer, int count,
                                       MPI_Datatype datatype);
static inline int bcast_sched_linear(int rank, int p, int root, NBC_Schedule *schedule, void *buffer, int count,
                                     MPI_Datatype datatype);
static inline int bcast_sched_chain(int rank, int p, int root, NBC_Schedule *schedule, void *buffer, int count,
                                    MPI_Datatype datatype, int fragsize, size_t size);

#ifdef NBC_CACHE_SCHEDULE
/* tree comparison function for schedule cache */
int NBC_Bcast_args_compare(NBC_Bcast_args *a, NBC_Bcast_args *b, void *param) {
  if ((a->buffer == b->buffer) &&
      (a->count == b->count) &&
      (a->datatype == b->datatype) &&
      (a->root == b->root) ) {
    return 0;
  }

  if( a->buffer < b->buffer ) {
    return -1;
  }

  return 1;
}
#endif

static int nbc_bcast_init(void *buffer, int count, MPI_Datatype datatype, int root,
                          struct ompi_communicator_t *comm, ompi_request_t ** request,
                          struct mca_coll_base_module_2_3_0_t *module, bool persistent)
{
  int rank, p, res, segsize;
  size_t size;
  NBC_Schedule *schedule;
#ifdef NBC_CACHE_SCHEDULE
  NBC_Bcast_args *args, *found, search;
#endif
  enum { NBC_BCAST_LINEAR, NBC_BCAST_BINOMIAL, NBC_BCAST_CHAIN } alg;
  ompi_coll_libnbc_module_t *libnbc_module = (ompi_coll_libnbc_module_t*) module;

  rank = ompi_comm_rank (comm);
  p = ompi_comm_size (comm);

  if (1 == p) {
    return nbc_get_noop_request(persistent, request);
  }

  res = ompi_datatype_type_size(datatype, &size);
  if (MPI_SUCCESS != res) {
    NBC_Error("MPI Error in ompi_datatype_type_size() (%i)", res);
    return res;
  }

  segsize = 16384;
  /* algorithm selection */
  if( libnbc_ibcast_skip_dt_decision ) {
    if (p <= 4) {
      alg = NBC_BCAST_LINEAR;
    }
    else {
      alg = NBC_BCAST_BINOMIAL;
    }
  }
  else {
    if (p <= 4) {
      alg = NBC_BCAST_LINEAR;
    } else if (size * count < 65536) {
      alg = NBC_BCAST_BINOMIAL;
    } else if (size * count < 524288) {
      alg = NBC_BCAST_CHAIN;
      segsize = 8192;
    } else {
      alg = NBC_BCAST_CHAIN;
      segsize = 32768;
    }
  }

#ifdef NBC_CACHE_SCHEDULE
  /* search schedule in communicator specific tree */
  search.buffer = buffer;
  search.count = count;
  search.datatype = datatype;
  search.root = root;
  found = (NBC_Bcast_args *) hb_tree_search ((hb_tree *) libnbc_module->NBC_Dict[NBC_BCAST], &search);
  if (NULL == found) {
#endif
    schedule = OBJ_NEW(NBC_Schedule);
    if (OPAL_UNLIKELY(NULL == schedule)) {
      return OMPI_ERR_OUT_OF_RESOURCE;
    }

    switch(alg) {
      case NBC_BCAST_LINEAR:
        res = bcast_sched_linear(rank, p, root, schedule, buffer, count, datatype);
        break;
      case NBC_BCAST_BINOMIAL:
        res = bcast_sched_binomial(rank, p, root, schedule, buffer, count, datatype);
        break;
      case NBC_BCAST_CHAIN:
        res = bcast_sched_chain(rank, p, root, schedule, buffer, count, datatype, segsize, size);
        break;
    }

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
    args = (NBC_Bcast_args *) malloc (sizeof (args));
    if (NULL != args) {
      args->buffer = buffer;
      args->count = count;
      args->datatype = datatype;
      args->root = root;
      args->schedule = schedule;
      res = hb_tree_insert ((hb_tree *) libnbc_module->NBC_Dict[NBC_BCAST], args, args, 0);
      if (0 == res) {
        OBJ_RETAIN (schedule);

        /* increase number of elements for A2A */
        if (++libnbc_module->NBC_Dict_size[NBC_BCAST] > NBC_SCHED_DICT_UPPER) {
          NBC_SchedCache_dictwipe ((hb_tree *) libnbc_module->NBC_Dict[NBC_BCAST],
                                   &libnbc_module->NBC_Dict_size[NBC_BCAST]);
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

int ompi_coll_libnbc_ibcast(void *buffer, int count, MPI_Datatype datatype, int root,
                            struct ompi_communicator_t *comm, ompi_request_t ** request,
                            struct mca_coll_base_module_2_3_0_t *module)
{
    int res = nbc_bcast_init(buffer, count, datatype, root,
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

/* better binomial bcast
 * working principle:
 * - each node gets a virtual rank vrank
 * - the 'root' node get vrank 0
 * - node 0 gets the vrank of the 'root'
 * - all other ranks stay identical (they do not matter)
 *
 * Algorithm:
 * - each node with vrank > 2^r and vrank < 2^r+1 receives from node
 *   vrank - 2^r (vrank=1 receives from 0, vrank 0 receives never)
 * - each node sends each round r to node vrank + 2^r
 * - a node stops to send if 2^r > commsize
 */
#define RANK2VRANK(rank, vrank, root) \
{ \
  vrank = rank; \
  if (rank == 0) vrank = root; \
  if (rank == root) vrank = 0; \
}
#define VRANK2RANK(rank, vrank, root) \
{ \
  rank = vrank; \
  if (vrank == 0) rank = root; \
  if (vrank == root) rank = 0; \
}
static inline int bcast_sched_binomial(int rank, int p, int root, NBC_Schedule *schedule, void *buffer, int count, MPI_Datatype datatype) {
  int maxr, vrank, peer, res;

  maxr = (int)ceil((log((double)p)/LOG2));

  RANK2VRANK(rank, vrank, root);

  /* receive from the right hosts  */
  if (vrank != 0) {
    for (int r = 0 ; r < maxr ; ++r) {
      if ((vrank >= (1 << r)) && (vrank < (1 << (r + 1)))) {
        VRANK2RANK(peer, vrank - (1 << r), root);
        res = NBC_Sched_recv (buffer, false, count, datatype, peer, schedule, false);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          return res;
        }
      }
    }

    res = NBC_Sched_barrier (schedule);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      return res;
    }
  }

  /* now send to the right hosts */
  for (int r = 0 ; r < maxr ; ++r) {
    if (((vrank + (1 << r) < p) && (vrank < (1 << r))) || (vrank == 0)) {
      VRANK2RANK(peer, vrank + (1 << r), root);
      res = NBC_Sched_send (buffer, false, count, datatype, peer, schedule, false);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
      }
    }
  }

  return OMPI_SUCCESS;
}

/* simple linear MPI_Ibcast */
static inline int bcast_sched_linear(int rank, int p, int root, NBC_Schedule *schedule, void *buffer, int count, MPI_Datatype datatype) {
  int res;

  /* send to all others */
  if(rank == root) {
    for (int peer = 0 ; peer < p ; ++peer) {
      if (peer != root) {
        /* send msg to peer */
        res = NBC_Sched_send (buffer, false, count, datatype, peer, schedule, false);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          return res;
        }
      }
    }
  } else {
    /* recv msg from root */
    res = NBC_Sched_recv (buffer, false, count, datatype, root, schedule, false);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      return res;
    }
  }

  return OMPI_SUCCESS;
}

/* simple chained MPI_Ibcast */
static inline int bcast_sched_chain(int rank, int p, int root, NBC_Schedule *schedule, void *buffer, int count, MPI_Datatype datatype, int fragsize, size_t size) {
  int res, vrank, rpeer, speer, numfrag, fragcount, thiscount;
  MPI_Aint ext;
  char *buf;

  RANK2VRANK(rank, vrank, root);
  VRANK2RANK(rpeer, vrank-1, root);
  VRANK2RANK(speer, vrank+1, root);
  res = ompi_datatype_type_extent(datatype, &ext);
  if (MPI_SUCCESS != res) {
    NBC_Error("MPI Error in ompi_datatype_type_extent() (%i)", res);
    return res;
  }

  if (count == 0) {
    return OMPI_SUCCESS;
  }

  numfrag = count * size/fragsize;
  if ((count * size) % fragsize != 0) {
    numfrag++;
  }

  fragcount = count/numfrag;

  for (int fragnum = 0 ; fragnum < numfrag ; ++fragnum) {
    buf = (char *) buffer + fragnum * fragcount * ext;
    thiscount = fragcount;
    if (fragnum == numfrag-1) {
      /* last fragment may not be full */
      thiscount = count - fragcount * fragnum;
    }

    /* root does not receive */
    if (vrank != 0) {
      res = NBC_Sched_recv (buf, false, thiscount, datatype, rpeer, schedule, true);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
      }
    }

    /* last rank does not send */
    if (vrank != p-1) {
      res = NBC_Sched_send (buf, false, thiscount, datatype, speer, schedule, false);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
      }

      /* this barrier here seems awaward but isn't!!!! */
      if (vrank == 0)  {
        res = NBC_Sched_barrier (schedule);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          return res;
        }
      }
    }
  }

  return OMPI_SUCCESS;
}

static int nbc_bcast_inter_init(void *buffer, int count, MPI_Datatype datatype, int root,
                                struct ompi_communicator_t *comm, ompi_request_t ** request,
                                struct mca_coll_base_module_2_3_0_t *module, bool persistent) {
  int res;
  NBC_Schedule *schedule;
  ompi_coll_libnbc_module_t *libnbc_module = (ompi_coll_libnbc_module_t*) module;

  schedule = OBJ_NEW(NBC_Schedule);
  if (OPAL_UNLIKELY(NULL == schedule)) {
    return OMPI_ERR_OUT_OF_RESOURCE;
  }

  if (root != MPI_PROC_NULL) {
    /* send to all others */
    if (root == MPI_ROOT) {
      int remsize;

      remsize = ompi_comm_remote_size (comm);

      for (int peer = 0 ; peer < remsize ; ++peer) {
        /* send msg to peer */
        res = NBC_Sched_send (buffer, false, count, datatype, peer, schedule, false);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          OBJ_RELEASE(schedule);
          return res;
        }
      }
    } else {
      /* recv msg from root */
      res = NBC_Sched_recv (buffer, false, count, datatype, root, schedule, false);
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

int ompi_coll_libnbc_ibcast_inter(void *buffer, int count, MPI_Datatype datatype, int root,
                                  struct ompi_communicator_t *comm, ompi_request_t ** request,
                                  struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_bcast_inter_init(buffer, count, datatype, root,
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

int ompi_coll_libnbc_bcast_init(void *buffer, int count, MPI_Datatype datatype, int root,
                                struct ompi_communicator_t *comm, MPI_Info info, ompi_request_t ** request,
                                struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_bcast_init(buffer, count, datatype, root,
                             comm, request, module, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
    }

    return OMPI_SUCCESS;
}

int ompi_coll_libnbc_bcast_inter_init(void *buffer, int count, MPI_Datatype datatype, int root,
                                      struct ompi_communicator_t *comm, MPI_Info info, ompi_request_t ** request,
                                      struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_bcast_inter_init(buffer, count, datatype, root,
                                   comm, request, module, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
    }

    return OMPI_SUCCESS;
}
