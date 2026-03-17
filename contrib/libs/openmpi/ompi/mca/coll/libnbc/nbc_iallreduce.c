/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2006      The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2006      The Technical University of Chemnitz. All
 *                         rights reserved.
 * Copyright (c) 2013-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
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
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/op/op.h"

#include <assert.h>

static inline int allred_sched_diss(int rank, int p, int count, MPI_Datatype datatype, ptrdiff_t gap, const void *sendbuf,
                                    void *recvbuf, MPI_Op op, char inplace, NBC_Schedule *schedule, void *tmpbuf);
static inline int allred_sched_ring(int rank, int p, int count, MPI_Datatype datatype, const void *sendbuf,
                                    void *recvbuf, MPI_Op op, int size, int ext, NBC_Schedule *schedule,
                                    void *tmpbuf);
static inline int allred_sched_linear(int rank, int p, const void *sendbuf, void *recvbuf, int count,
                                      MPI_Datatype datatype, ptrdiff_t gap, MPI_Op op, int ext, int size,
                                      NBC_Schedule *schedule, void *tmpbuf);

#ifdef NBC_CACHE_SCHEDULE
/* tree comparison function for schedule cache */
int NBC_Allreduce_args_compare(NBC_Allreduce_args *a, NBC_Allreduce_args *b, void *param) {
  if ((a->sendbuf == b->sendbuf) &&
      (a->recvbuf == b->recvbuf) &&
      (a->count == b->count) &&
      (a->datatype == b->datatype) &&
      (a->op == b->op)) {
    return 0;
  }

  if( a->sendbuf < b->sendbuf ) {
    return -1;
  }

  return 1;
}
#endif

static int nbc_allreduce_init(const void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
                              struct ompi_communicator_t *comm, ompi_request_t ** request,
                              struct mca_coll_base_module_2_3_0_t *module, bool persistent)
{
  int rank, p, res;
  ptrdiff_t ext, lb;
  NBC_Schedule *schedule;
  size_t size;
#ifdef NBC_CACHE_SCHEDULE
  NBC_Allreduce_args *args, *found, search;
#endif
  enum { NBC_ARED_BINOMIAL, NBC_ARED_RING } alg;
  char inplace;
  void *tmpbuf = NULL;
  ompi_coll_libnbc_module_t *libnbc_module = (ompi_coll_libnbc_module_t*) module;
  ptrdiff_t span, gap;

  NBC_IN_PLACE(sendbuf, recvbuf, inplace);

  rank = ompi_comm_rank (comm);
  p = ompi_comm_size (comm);

  res = ompi_datatype_get_extent(datatype, &lb, &ext);
  if (OMPI_SUCCESS != res) {
    NBC_Error ("MPI Error in ompi_datatype_type_extent() (%i)", res);
    return res;
  }

  res = ompi_datatype_type_size (datatype, &size);
  if (OMPI_SUCCESS != res) {
    NBC_Error ("MPI Error in ompi_datatype_type_size() (%i)", res);
    return res;
  }

  if (1 == p && (!persistent || inplace)) {
    if (!inplace) {
      /* for a single node - copy data to receivebuf */
      res = NBC_Copy(sendbuf, count, datatype, recvbuf, count, datatype, comm);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
      }
    }
    return nbc_get_noop_request(persistent, request);
  }

  span = opal_datatype_span(&datatype->super, count, &gap);
  tmpbuf = malloc (span);
  if (OPAL_UNLIKELY(NULL == tmpbuf)) {
    return OMPI_ERR_OUT_OF_RESOURCE;
  }

  /* algorithm selection */
  if(p < 4 || size*count < 65536 || !ompi_op_is_commute(op) || inplace) {
    alg = NBC_ARED_BINOMIAL;
  } else {
    alg = NBC_ARED_RING;
  }

#ifdef NBC_CACHE_SCHEDULE
  /* search schedule in communicator specific tree */
  search.sendbuf = sendbuf;
  search.recvbuf = recvbuf;
  search.count = count;
  search.datatype = datatype;
  search.op = op;
  found = (NBC_Allreduce_args *) hb_tree_search ((hb_tree *) libnbc_module->NBC_Dict[NBC_ALLREDUCE], &search);
  if (NULL == found) {
#endif
    schedule = OBJ_NEW(NBC_Schedule);
    if (NULL == schedule) {
      free(tmpbuf);
      return OMPI_ERR_OUT_OF_RESOURCE;
    }

    if (p == 1) {
      res = NBC_Sched_copy((void *)sendbuf, false, count, datatype,
                           recvbuf, false, count, datatype, schedule, false);
    } else {
      switch(alg) {
        case NBC_ARED_BINOMIAL:
          res = allred_sched_diss(rank, p, count, datatype, gap, sendbuf, recvbuf, op, inplace, schedule, tmpbuf);
          break;
        case NBC_ARED_RING:
          res = allred_sched_ring(rank, p, count, datatype, sendbuf, recvbuf, op, size, ext, schedule, tmpbuf);
          break;
      }
    }

    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      OBJ_RELEASE(schedule);
      free(tmpbuf);
      return res;
    }

    res = NBC_Sched_commit(schedule);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      OBJ_RELEASE(schedule);
      free(tmpbuf);
      return res;
    }

#ifdef NBC_CACHE_SCHEDULE
    /* save schedule to tree */
    args = (NBC_Allreduce_args *) malloc (sizeof(args));
    if (NULL != args) {
      args->sendbuf = sendbuf;
      args->recvbuf = recvbuf;
      args->count = count;
      args->datatype = datatype;
      args->op = op;
      args->schedule = schedule;
      res = hb_tree_insert ((hb_tree *) libnbc_module->NBC_Dict[NBC_ALLREDUCE], args, args, 0);
      if (0 == res) {
        OBJ_RETAIN(schedule);

        /* increase number of elements for A2A */
        if (++libnbc_module->NBC_Dict_size[NBC_ALLREDUCE] > NBC_SCHED_DICT_UPPER) {
          NBC_SchedCache_dictwipe ((hb_tree *) libnbc_module->NBC_Dict[NBC_ALLREDUCE],
                                   &libnbc_module->NBC_Dict_size[NBC_ALLREDUCE]);
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

  res = NBC_Schedule_request (schedule, comm, libnbc_module, persistent, request, tmpbuf);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    OBJ_RELEASE(schedule);
    free(tmpbuf);
    return res;
  }

  return OMPI_SUCCESS;
}

int ompi_coll_libnbc_iallreduce(const void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
                                struct ompi_communicator_t *comm, ompi_request_t ** request,
                                struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_allreduce_init(sendbuf, recvbuf, count, datatype, op,
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

static int nbc_allreduce_inter_init(const void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
                                    struct ompi_communicator_t *comm, ompi_request_t ** request,
                                    struct mca_coll_base_module_2_3_0_t *module, bool persistent)
{
  int rank, res, rsize;
  size_t size;
  MPI_Aint ext;
  NBC_Schedule *schedule;
  void *tmpbuf = NULL;
  ompi_coll_libnbc_module_t *libnbc_module = (ompi_coll_libnbc_module_t*) module;
  ptrdiff_t span, gap;

  rank = ompi_comm_rank (comm);
  rsize = ompi_comm_remote_size (comm);

  res = ompi_datatype_type_extent(datatype, &ext);
  if (MPI_SUCCESS != res) {
    NBC_Error("MPI Error in ompi_datatype_type_extent() (%i)", res);
    return res;
  }

  res = ompi_datatype_type_size(datatype, &size);
  if (MPI_SUCCESS != res) {
    NBC_Error("MPI Error in ompi_datatype_type_size() (%i)", res);
    return res;
  }

  span = opal_datatype_span(&datatype->super, count, &gap);
  tmpbuf = malloc (span);
  if (OPAL_UNLIKELY(NULL == tmpbuf)) {
    return OMPI_ERR_OUT_OF_RESOURCE;
  }

  schedule = OBJ_NEW(NBC_Schedule);
  if (OPAL_UNLIKELY(NULL == schedule)) {
    free(tmpbuf);
    return OMPI_ERR_OUT_OF_RESOURCE;
  }

  res = allred_sched_linear (rank, rsize, sendbuf, recvbuf, count, datatype, gap, op,
                             ext, size, schedule, tmpbuf);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    OBJ_RELEASE(schedule);
    free(tmpbuf);
    return res;
  }

  res = NBC_Sched_commit(schedule);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    OBJ_RELEASE(schedule);
    free(tmpbuf);
    return res;
  }

  res = NBC_Schedule_request(schedule, comm, libnbc_module, persistent, request, tmpbuf);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    OBJ_RELEASE(schedule);
    free(tmpbuf);
    return res;
  }

  return OMPI_SUCCESS;
}

int ompi_coll_libnbc_iallreduce_inter(const void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
                                      struct ompi_communicator_t *comm, ompi_request_t ** request,
                                      struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_allreduce_inter_init(sendbuf, recvbuf, count, datatype, op,
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

/* binomial allreduce (binomial tree up and binomial bcast down)
 * working principle:
 * - each node gets a virtual rank vrank
 * - the 'root' node get vrank 0
 * - node 0 gets the vrank of the 'root'
 * - all other ranks stay identical (they do not matter)
 *
 * Algorithm:
 * pairwise exchange
 * round r:
 *  grp = rank % 2^r
 *  if grp == 0: receive from rank + 2^(r-1) if it exists and reduce value
 *  if grp == 1: send to rank - 2^(r-1) and exit function
 *
 * do this for R=log_2(p) rounds
 * followed by a Bcast:
 * Algorithm:
 * - each node with vrank > 2^r and vrank < 2^r+1 receives from node
 *   vrank - 2^r (vrank=1 receives from 0, vrank 0 receives never)
 * - each node sends each round r to node vrank + 2^r
 * - a node stops to send if 2^r > commsize
 *
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
static inline int allred_sched_diss(int rank, int p, int count, MPI_Datatype datatype, ptrdiff_t gap, const void *sendbuf, void *recvbuf,
                                    MPI_Op op, char inplace, NBC_Schedule *schedule, void *tmpbuf) {
  int root, vrank, maxr, vpeer, peer, res;
  char *rbuf, *lbuf, *buf;
  int tmprbuf, tmplbuf;

  root = 0; /* this makes the code for ireduce and iallreduce nearly identical - could be changed to improve performance */
  RANK2VRANK(rank, vrank, root);
  maxr = (int)ceil((log((double)p)/LOG2));
  /* ensure the result ends up in recvbuf on vrank 0 */
  if (0 == (maxr%2)) {
    rbuf = (void *)(-gap);
    tmprbuf = true;
    lbuf = recvbuf;
    tmplbuf = false;
  } else {
    lbuf = (void *)(-gap);
    tmplbuf = true;
    rbuf = recvbuf;
    tmprbuf = false;
    if (inplace) {
        res = NBC_Sched_copy(rbuf, false, count, datatype,
                             ((char *)tmpbuf) - gap, false, count, datatype,
                             schedule, true);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          return res;
        }
    }
  }

  for (int r = 1, firstred = 1 ; r <= maxr ; ++r) {
    if ((vrank % (1 << r)) == 0) {
      /* we have to receive this round */
      vpeer = vrank + (1 << (r - 1));
      VRANK2RANK(peer, vpeer, root)
      if (peer < p) {
        /* we have to wait until we have the data */
        res = NBC_Sched_recv (rbuf, tmprbuf, count, datatype, peer, schedule, true);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          return res;
        }

        /* this cannot be done until tmpbuf is unused :-( so barrier after the op */
        if (firstred && !inplace) {
          /* perform the reduce with the senbuf */
          res = NBC_Sched_op (sendbuf, false, rbuf, tmprbuf, count, datatype, op, schedule, true);
          firstred = 0;
        } else {
          /* perform the reduce in my local buffer */
          res = NBC_Sched_op (lbuf, tmplbuf, rbuf, tmprbuf, count, datatype, op, schedule, true);
        }
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          return res;
        }
        /* swap left and right buffers */
        buf = rbuf; rbuf = lbuf ; lbuf = buf;
        tmprbuf ^= 1; tmplbuf ^= 1;
      }
    } else {
      /* we have to send this round */
      vpeer = vrank - (1 << (r - 1));
      VRANK2RANK(peer, vpeer, root)
      if (firstred && !inplace) {
        /* we have to use the sendbuf in the first round .. */
        res = NBC_Sched_send (sendbuf, false, count, datatype, peer, schedule, false);
      } else {
        /* and the recvbuf in all remaining rounds */
        res = NBC_Sched_send (lbuf, tmplbuf, count, datatype, peer, schedule, false);
      }

      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
      }

      /* leave the game */
      break;
    }
  }

  /* this is the Bcast part - copied with minor changes from nbc_ibcast.c
   * changed: buffer -> recvbuf  */
  RANK2VRANK(rank, vrank, root);

  /* receive from the right hosts  */
  if (vrank != 0) {
    for (int r = 0; r < maxr ; ++r) {
      if ((vrank >= (1 << r)) && (vrank < (1 << (r + 1)))) {
        VRANK2RANK(peer, vrank - (1 << r), root);
        res = NBC_Sched_recv (recvbuf, false, count, datatype, peer, schedule, false);
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

  if (0 == vrank) assert(lbuf == recvbuf);
  /* now send to the right hosts */
  for (int r = 0; r < maxr; ++r) {
    if (((vrank + (1 << r) < p) && (vrank < (1 << r))) || (vrank == 0)) {
      VRANK2RANK(peer, vrank + (1 << r), root);
      res = NBC_Sched_send (recvbuf, false, count, datatype, peer, schedule, false);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
      }
    }
  }

  /* end of the bcast */
  return OMPI_SUCCESS;
}

static inline int allred_sched_ring (int r, int p, int count, MPI_Datatype datatype, const void *sendbuf, void *recvbuf, MPI_Op op,
                                     int size, int ext, NBC_Schedule *schedule, void *tmpbuf) {
  int segsize, *segsizes, *segoffsets; /* segment sizes and offsets per segment (number of segments == number of nodes */
  int speer, rpeer; /* send and recvpeer */
  int res = OMPI_SUCCESS;

  if (count == 0) {
    return OMPI_SUCCESS;
  }

  segsizes = (int *) malloc (sizeof (int) * p);
  segoffsets = (int *) malloc (sizeof (int) * p);
  if (NULL == segsizes || NULL == segoffsets) {
    free (segsizes);
    free (segoffsets);
    return OMPI_ERR_OUT_OF_RESOURCE;
  }

  segsize = (count + p - 1) / p; /* size of the segments */

  segoffsets[0] = 0;
  for (int i = 0, mycount = count ; i < p ; ++i) {
    mycount -= segsize;
    segsizes[i] = segsize;
    if (mycount < 0) {
      segsizes[i] = segsize + mycount;
      mycount = 0;
    }

    if (i) {
      segoffsets[i] = segoffsets[i-1] + segsizes[i-1];
    }
  }

  /* reduce peers */
  speer = (r + 1) % p;
  rpeer = (r - 1 + p) % p;

  /*  + -> reduced this round
   *  / -> sum (reduced in a previous step)
   *
   *     *** round 0 ***
   *    0        1        2
   *
   *   00       10       20      0: [1] -> 1
   *   01       11       21      1: [2] -> 2
   *   02       12       22      2: [0] -> 0  --> send element (r+1)%p to node (r+1)%p
   *
   *      *** round 1 ***
   *    0        1        2
   *
   *   00+20    10       20     0: red(0), [0] -> 1
   *   01       11+01    21     1: red(1), [1] -> 2
   *   02       12       22+12  2: red(2), [2] -> 0 --> reduce and send element (r+0)%p to node (r+1)%p
   *
   *      *** round 2 ***
   *    0        1        2
   *
   *   00/20    all      20     0: red(2), [2] -> 1
   *   01       11/01    all    1: red(0), [0] -> 2
   *   all      12       22/12  2: red(1), [1] -> 0 --> reduce and send (r-1)%p to node (r+1)%p
   *
   *      *** round 3 ***
   *    0        1        2
   *
   *   00/20    all      all    0: [1] -> 1
   *   all      11/01    all    1: [2] -> 2
   *   all      all      22/12  2: [0] -> 0 --> send element (r-2)%p to node (r+1)%p
   *
   *      *** round 4 ***
   *    0        1        2
   *
   *   all      all      all    0: done
   *   all      all      all    1: done
   *   all      all      all    2: done
   *
   * -> 4
   *     *** round 0 ***
   *    0        1        2        3
   *
   *   00       10       20       30       0: [1] -> 1
   *   01       11       21       31       1: [2] -> 2
   *   02       12       22       32       2: [3] -> 3
   *   03       13       23       33       3: [0] -> 0 --> send element (r+1)%p to node (r+1)%p
   *
   *      *** round 1 ***
   *    0        1        2        3
   *
   *   00+30    10       20       30       0: red(0), [0] -> 1
   *   01       11+01    21       31       1: red(1), [1] -> 2
   *   02       12       22+12    32       2: red(2), [2] -> 3
   *   03       13       23       33+23    3: red(3), [3] -> 0 --> reduce and send element (r+0)%p to node (r+1)%p
   *
   *      *** round 2 ***
   *    0        1        2        3
   *
   *   00/30    10+00/30 20       30       0: red(3), [3] -> 1
   *   01       11/01    21+11/01 31       1: red(0), [0] -> 2
   *   02       12       22/12    32+22/12 2: red(1), [1] -> 3
   *   03+33/23 13       23       33/23    3: red(2), [2] -> 0 --> reduce and send (r-1)%p to node (r+1)%p
   *
   *      *** round 3 ***
   *    0        1        2        3
   *
   *   00/30    10/00/30 all      30       0: red(2), [2] -> 1
   *   01       11/01    21/11/01 all      1: red(3), [3] -> 2
   *   all      12       22/12    32/22/12 2: red(0), [0] -> 3
   *   03/33/23 all      23       33/23    3: red(1), [1] -> 0 --> reduce and send (r-2)%p to node (r+1)%p
   *
   *      *** round 4 ***
   *    0        1        2        3
   *
   *   00/30    10/00/30 all      all      0: [1] -> 1
   *   all      11/01    21/11/01 all      1: [2] -> 2
   *   all      all      22/12    32/22/12 2: [3] -> 3
   *   03/33/23 all      all      33/23    3: [0] -> 0 --> receive and send element (r+1)%p to node (r+1)%p
   *
   *      *** round 5 ***
   *    0        1        2        3
   *
   *   all      10/00/30 all      all      0: [0] -> 1
   *   all      all      21/11/01 all      1: [1] -> 2
   *   all      all      all      32/22/12 2: [3] -> 3
   *   03/33/23 all      all      all      3: [4] -> 4 --> receive and send element (r-0)%p to node (r+1)%p
   *
   *      *** round 6 ***
   *    0        1        2        3
   *
   *   all      all      all      all
   *   all      all      all      all
   *   all      all      all      all
   *   all      all      all      all     receive element (r-1)%p
   *
   *   2p-2 rounds ... every node does p-1 reductions and p-1 sends
   *
   */
  /* first p-1 rounds are reductions */
  for (int round = 0 ; round < p - 1 ; ++round) {
    int selement = (r+1-round + 2*p /*2*p avoids negative mod*/)%p; /* the element I am sending */
    int soffset = segoffsets[selement]*ext;
    int relement = (r-round + 2*p /*2*p avoids negative mod*/)%p; /* the element that I receive from my neighbor */
    int roffset = segoffsets[relement]*ext;

    /* first message come out of sendbuf */
    if (round == 0) {
      res = NBC_Sched_send ((char *) sendbuf + soffset, false, segsizes[selement], datatype, speer,
                            schedule, false);
    } else {
      res = NBC_Sched_send ((char *) recvbuf + soffset, false, segsizes[selement], datatype, speer,
                            schedule, false);
    }

    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      break;
    }

    res = NBC_Sched_recv ((char *) recvbuf + roffset, false, segsizes[relement], datatype, rpeer,
                          schedule, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      break;
    }

    res = NBC_Sched_op ((char *) sendbuf + roffset, false, (char *) recvbuf + roffset, false,
                         segsizes[relement], datatype, op, schedule, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      break;
    }
  }

  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    free (segsizes);
    free (segoffsets);
    return res;
  }

  for (int round = p - 1 ; round < 2 * p - 2 ; ++round) {
    int selement = (r+1-round + 2*p /*2*p avoids negative mod*/)%p; /* the element I am sending */
    int soffset = segoffsets[selement]*ext;
    int relement = (r-round + 2*p /*2*p avoids negative mod*/)%p; /* the element that I receive from my neighbor */
    int roffset = segoffsets[relement]*ext;

    res = NBC_Sched_send ((char *) recvbuf + soffset, false, segsizes[selement], datatype, speer,
                          schedule, false);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      break;
    }

    res = NBC_Sched_recv ((char *) recvbuf + roffset, false, segsizes[relement], datatype, rpeer,
                          schedule, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      break;
    }
  }

  free (segsizes);
  free (segoffsets);

  return res;
}

static inline int allred_sched_linear(int rank, int rsize, const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype,
				      ptrdiff_t gap, MPI_Op op, int ext, int size, NBC_Schedule *schedule, void *tmpbuf) {
  int res;

  if (0 == count) {
    return OMPI_SUCCESS;
  }

  /* send my data to the remote root */
  res = NBC_Sched_send (sendbuf, false, count, datatype, 0, schedule, false);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    return res;
  }

  /* recv my data to the remote root */
  if (0 != rank || 1 ==(rsize%2)) {
    res = NBC_Sched_recv (recvbuf, false, count, datatype, 0, schedule, false);
  } else {
    res = NBC_Sched_recv ((void *)(-gap), true, count, datatype, 0, schedule, false);
  }
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    return res;
  }

  if (0 == rank) {
    char *rbuf, *lbuf, *buf;
    int tmprbuf, tmplbuf;

    res = NBC_Sched_barrier (schedule);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      return res;
    }

    /* ensure the result ends up in recvbuf */
    if (0 == (rsize%2)) {
      lbuf = (void *)(-gap);
      tmplbuf = true;
      rbuf = recvbuf;
      tmprbuf = false;
    } else {
      rbuf = (void *)(-gap);
      tmprbuf = true;
      lbuf = recvbuf;
      tmplbuf = false;
    }

    /* get data from remote peers and reduce */
    for (int rpeer = 1 ; rpeer < rsize ; ++rpeer) {
      res = NBC_Sched_recv (rbuf, tmprbuf, count, datatype, rpeer, schedule, true);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
      }

      res = NBC_Sched_op (lbuf, tmplbuf, rbuf, tmprbuf, count, datatype, op, schedule, true);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
      }
      /* swap left and right buffers */
      buf = rbuf; rbuf = lbuf ; lbuf = buf;
      tmprbuf ^= 1; tmplbuf ^= 1;
    }

    /* exchange our result with the remote root (each root will broadcast to the other's peers) */
    res = NBC_Sched_recv ((void *)(-gap), true, count, datatype, 0, schedule, false);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      return res;
    }

    /* wait for data from remote root */
    res = NBC_Sched_send (recvbuf, false, count, datatype, 0, schedule, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      return res;
    }

    /* broadcast the result to all remote peers */
    for (int rpeer = 1 ; rpeer < rsize ; ++rpeer) {
      res = NBC_Sched_send ((void *)(-gap), true, count, datatype, rpeer, schedule, false);
      if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
      }
    }
  }

  return OMPI_SUCCESS;
}

int ompi_coll_libnbc_allreduce_init(const void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
                                    struct ompi_communicator_t *comm, MPI_Info info, ompi_request_t ** request,
                                    struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_allreduce_init(sendbuf, recvbuf, count, datatype, op,
                                 comm, request, module, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
    }

    return OMPI_SUCCESS;
}

int ompi_coll_libnbc_allreduce_inter_init(const void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
                                          struct ompi_communicator_t *comm, MPI_Info info, ompi_request_t ** request,
                                          struct mca_coll_base_module_2_3_0_t *module) {
    int res = nbc_allreduce_inter_init(sendbuf, recvbuf, count, datatype, op,
                                       comm, request, module, true);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
        return res;
    }

    return OMPI_SUCCESS;
}

