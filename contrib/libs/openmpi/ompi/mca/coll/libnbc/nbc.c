/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2006      The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2013      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2006      The Technical University of Chemnitz. All
 *                         rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * Author(s): Torsten Hoefler <htor@cs.indiana.edu>
 *
 * Copyright (c) 2012      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2017      Ian Bradley Morgan and Anthony Skjellum. All
 *                         rights reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 */
#include "nbc_internal.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/op/op.h"
#include "ompi/mca/pml/pml.h"

/* only used in this file */
static inline int NBC_Start_round(NBC_Handle *handle);

/* #define NBC_TIMING */

#ifdef NBC_TIMING
static double Isend_time=0, Irecv_time=0, Wait_time=0, Test_time=0;
void NBC_Reset_times() {
  Isend_time=Irecv_time=Wait_time=Test_time=0;
}
void NBC_Print_times(double div) {
  printf("*** NBC_TIMES: Isend: %lf, Irecv: %lf, Wait: %lf, Test: %lf\n", Isend_time*1e6/div, Irecv_time*1e6/div, Wait_time*1e6/div, Test_time*1e6/div);
}
#endif

static void nbc_schedule_constructor (NBC_Schedule *schedule) {
  /* initial total size of the schedule */
  schedule->size = sizeof (int);
  schedule->current_round_offset = 0;
  schedule->data = calloc (1, schedule->size);
}

static void nbc_schedule_destructor (NBC_Schedule *schedule) {
  free (schedule->data);
  schedule->data = NULL;
}

OBJ_CLASS_INSTANCE(NBC_Schedule, opal_object_t, nbc_schedule_constructor,
                   nbc_schedule_destructor);

static int nbc_schedule_grow (NBC_Schedule *schedule, int additional) {
  void *tmp;
  int size;

  /* get current size of schedule */
  size = nbc_schedule_get_size (schedule);

  tmp = realloc (schedule->data, size + additional);
  if (NULL == tmp) {
    NBC_Error ("Could not increase the size of NBC schedule");
    return OMPI_ERR_OUT_OF_RESOURCE;
  }

  schedule->data = tmp;
  return OMPI_SUCCESS;
}

static int nbc_schedule_round_append (NBC_Schedule *schedule, void *data, int data_size, bool barrier) {
  int ret, size = nbc_schedule_get_size (schedule);

  if (barrier) {
    ret = nbc_schedule_grow (schedule, data_size + 1 + sizeof (int));
  } else {
    ret = nbc_schedule_grow (schedule, data_size);
  }
  if (OMPI_SUCCESS != ret) {
    return ret;
  }

  /* append to the round-schedule */
  if (data_size) {
    memcpy (schedule->data + size, data, data_size);

    /* increase number of elements in round-schedule */
    nbc_schedule_inc_round (schedule);

    /* increase size of schedule */
    nbc_schedule_inc_size (schedule, data_size);
  }

  if (barrier) {
    /* add the barrier */
    schedule->data[size + data_size] = 1;
    /* set next round counter to 0 */
    memset (schedule->data + size + data_size + 1, 0, sizeof (int));

    NBC_DEBUG(10, "ended round at byte %i\n", size + data_size + 1);

    schedule->current_round_offset = size + data_size + 1;

    /* increase size of schedule */
    nbc_schedule_inc_size (schedule, sizeof (int) + 1);
  }

  return OMPI_SUCCESS;
}

/* this function puts a send into the schedule */
static int NBC_Sched_send_internal (const void* buf, char tmpbuf, int count, MPI_Datatype datatype, int dest, bool local, NBC_Schedule *schedule, bool barrier) {
  NBC_Args_send send_args;
  int ret;

  /* store the passed arguments */
  send_args.type = SEND;
  send_args.buf = buf;
  send_args.tmpbuf = tmpbuf;
  send_args.count = count;
  send_args.datatype = datatype;
  send_args.dest = dest;
  send_args.local = local;

  /* append to the round-schedule */
  ret = nbc_schedule_round_append (schedule, &send_args, sizeof (send_args), barrier);
  if (OMPI_SUCCESS != ret) {
    return ret;
  }

  NBC_DEBUG(10, "added send - ends at byte %i\n", nbc_schedule_get_size (schedule));

  return OMPI_SUCCESS;
}

int NBC_Sched_send (const void* buf, char tmpbuf, int count, MPI_Datatype datatype, int dest, NBC_Schedule *schedule, bool barrier) {
  return NBC_Sched_send_internal (buf, tmpbuf, count, datatype, dest, false, schedule, barrier);
}

int NBC_Sched_local_send (const void* buf, char tmpbuf, int count, MPI_Datatype datatype, int dest, NBC_Schedule *schedule, bool barrier) {
  return NBC_Sched_send_internal (buf, tmpbuf, count, datatype, dest, true, schedule, barrier);
}

/* this function puts a receive into the schedule */
static int NBC_Sched_recv_internal (void* buf, char tmpbuf, int count, MPI_Datatype datatype, int source, bool local, NBC_Schedule *schedule, bool barrier) {
  NBC_Args_recv recv_args;
  int ret;

  /* store the passed arguments */
  recv_args.type = RECV;
  recv_args.buf = buf;
  recv_args.tmpbuf = tmpbuf;
  recv_args.count = count;
  recv_args.datatype = datatype;
  recv_args.source = source;
  recv_args.local = local;

  /* append to the round-schedule */
  ret = nbc_schedule_round_append (schedule, &recv_args, sizeof (recv_args), barrier);
  if (OMPI_SUCCESS != ret) {
    return ret;
  }

  NBC_DEBUG(10, "added receive - ends at byte %d\n", nbc_schedule_get_size (schedule));

  return OMPI_SUCCESS;
}

int NBC_Sched_recv (void* buf, char tmpbuf, int count, MPI_Datatype datatype, int source, NBC_Schedule *schedule, bool barrier) {
  return NBC_Sched_recv_internal(buf, tmpbuf, count, datatype, source, false, schedule, barrier);
}

int NBC_Sched_local_recv (void* buf, char tmpbuf, int count, MPI_Datatype datatype, int source, NBC_Schedule *schedule, bool barrier) {
  return NBC_Sched_recv_internal(buf, tmpbuf, count, datatype, source, true, schedule, barrier);
}

/* this function puts an operation into the schedule */
int NBC_Sched_op (const void* buf1, char tmpbuf1, void* buf2, char tmpbuf2, int count, MPI_Datatype datatype,
                  MPI_Op op, NBC_Schedule *schedule, bool barrier) {
  NBC_Args_op op_args;
  int ret;

  /* store the passed arguments */
  op_args.type = OP;
  op_args.buf1 = buf1;
  op_args.buf2 = buf2;
  op_args.tmpbuf1 = tmpbuf1;
  op_args.tmpbuf2 = tmpbuf2;
  op_args.count = count;
  op_args.op = op;
  op_args.datatype = datatype;

  /* append to the round-schedule */
  ret = nbc_schedule_round_append (schedule, &op_args, sizeof (op_args), barrier);
  if (OMPI_SUCCESS != ret) {
    return ret;
  }

  NBC_DEBUG(10, "added op2 - ends at byte %i\n", nbc_schedule_get_size (schedule));

  return OMPI_SUCCESS;
}

/* this function puts a copy into the schedule */
int NBC_Sched_copy (void *src, char tmpsrc, int srccount, MPI_Datatype srctype, void *tgt, char tmptgt, int tgtcount,
                    MPI_Datatype tgttype, NBC_Schedule *schedule, bool barrier) {
  NBC_Args_copy copy_args;
  int ret;

  /* store the passed arguments */
  copy_args.type = COPY;
  copy_args.src = src;
  copy_args.tmpsrc = tmpsrc;
  copy_args.srccount = srccount;
  copy_args.srctype = srctype;
  copy_args.tgt = tgt;
  copy_args.tmptgt = tmptgt;
  copy_args.tgtcount = tgtcount;
  copy_args.tgttype = tgttype;

  /* append to the round-schedule */
  ret = nbc_schedule_round_append (schedule, &copy_args, sizeof (copy_args), barrier);
  if (OMPI_SUCCESS != ret) {
    return ret;
  }

  NBC_DEBUG(10, "added copy - ends at byte %i\n", nbc_schedule_get_size (schedule));

  return OMPI_SUCCESS;
}

/* this function puts a unpack into the schedule */
int NBC_Sched_unpack (void *inbuf, char tmpinbuf, int count, MPI_Datatype datatype, void *outbuf, char tmpoutbuf,
                      NBC_Schedule *schedule, bool barrier) {
  NBC_Args_unpack unpack_args;
  int ret;

  /* store the passed arguments */
  unpack_args.type = UNPACK;
  unpack_args.inbuf = inbuf;
  unpack_args.tmpinbuf = tmpinbuf;
  unpack_args.count = count;
  unpack_args.datatype = datatype;
  unpack_args.outbuf = outbuf;
  unpack_args.tmpoutbuf = tmpoutbuf;

  /* append to the round-schedule */
  ret = nbc_schedule_round_append (schedule, &unpack_args, sizeof (unpack_args), barrier);
  if (OMPI_SUCCESS != ret) {
    return ret;
  }

  NBC_DEBUG(10, "added unpack - ends at byte %i\n", nbc_schedule_get_size (schedule));

  return OMPI_SUCCESS;
}

/* this function ends a round of a schedule */
int NBC_Sched_barrier (NBC_Schedule *schedule) {
  return nbc_schedule_round_append (schedule, NULL, 0, true);
}

/* this function ends a schedule */
int NBC_Sched_commit(NBC_Schedule *schedule) {
  int size = nbc_schedule_get_size (schedule);
  char *ptr;
  int ret;

  ret = nbc_schedule_grow (schedule, 1);
  if (OMPI_SUCCESS != ret) {
    return ret;
  }

  /* add the barrier char (0) because this is the last round */
  ptr = schedule->data + size;
  *((char *) ptr) = 0;

  /* increase size of schedule */
  nbc_schedule_inc_size (schedule, 1);

  NBC_DEBUG(10, "closed schedule %p at byte %i\n", schedule, (int)(size + 1));

  return OMPI_SUCCESS;
}

/* finishes a request
 *
 * to be called *only* from the progress thread !!! */
static inline void NBC_Free (NBC_Handle* handle) {

  if (NULL != handle->schedule) {
    /* release schedule */
    OBJ_RELEASE (handle->schedule);
    handle->schedule = NULL;
  }

  /* if the nbc_I<collective> attached some data */
  /* problems with schedule cache here, see comment (TODO) in
   * nbc_internal.h */
  if (NULL != handle->tmpbuf) {
    free((void*)handle->tmpbuf);
    handle->tmpbuf = NULL;
  }
}

/* progresses a request
 *
 * to be called *only* from the progress thread !!! */
int NBC_Progress(NBC_Handle *handle) {
  int res, ret=NBC_CONTINUE;
  bool flag;
  unsigned long size = 0;
  char *delim;
  int i;
  ompi_status_public_t status;

  if (handle->nbc_complete) {
    return NBC_OK;
  }

  flag = true;

  if ((handle->req_count > 0) && (handle->req_array != NULL)) {
    NBC_DEBUG(50, "NBC_Progress: testing for %i requests\n", handle->req_count);
#ifdef NBC_TIMING
    Test_time -= MPI_Wtime();
#endif
    /* don't call ompi_request_test_all as it causes a recursive call into opal_progress */
    while (handle->req_count) {
        ompi_request_t *subreq = handle->req_array[handle->req_count - 1];
        if (REQUEST_COMPLETE(subreq)) {
            ompi_request_free(&subreq);
            handle->req_count--;
        } else {
            flag = false;
            break;
        }
    }
#ifdef NBC_TIMING
    Test_time += MPI_Wtime();
#endif
  }

  /* a round is finished */
  if (flag) {
    /* adjust delim to start of current round */
    NBC_DEBUG(5, "NBC_Progress: going in schedule %p to row-offset: %li\n", handle->schedule, handle->row_offset);
    delim = handle->schedule->data + handle->row_offset;
    NBC_DEBUG(10, "delim: %p\n", delim);
    nbc_get_round_size(delim, &size);
    NBC_DEBUG(10, "size: %li\n", size);
    /* adjust delim to end of current round -> delimiter */
    delim = delim + size;

    if (NULL != handle->req_array) {
      /* free request array */
      free (handle->req_array);
      handle->req_array = NULL;
    }

    handle->req_count = 0;

    if (*delim == 0) {
      /* this was the last round - we're done */
      NBC_DEBUG(5, "NBC_Progress last round finished - we're done\n");

      handle->nbc_complete = true;
      if (!handle->super.req_persistent) {
        NBC_Free(handle);
      }

      return NBC_OK;
    }

    NBC_DEBUG(5, "NBC_Progress round finished - goto next round\n");
    /* move delim to start of next round */
    /* initializing handle for new virgin round */
    handle->row_offset = (intptr_t) (delim + 1) - (intptr_t) handle->schedule->data;
    /* kick it off */
    res = NBC_Start_round(handle);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
      NBC_Error ("Error in NBC_Start_round() (%i)", res);
      return res;
    }
  }

  return ret;
}

static inline int NBC_Start_round(NBC_Handle *handle) {
  int num; /* number of operations */
  int res;
  char* ptr;
  MPI_Request *tmp;
  NBC_Fn_type type;
  NBC_Args_send     sendargs;
  NBC_Args_recv     recvargs;
  NBC_Args_op         opargs;
  NBC_Args_copy     copyargs;
  NBC_Args_unpack unpackargs;
  void *buf1,  *buf2;

  /* get round-schedule address */
  ptr = handle->schedule->data + handle->row_offset;

  NBC_GET_BYTES(ptr,num);
  NBC_DEBUG(10, "start_round round at offset %d : posting %i operations\n", handle->row_offset, num);

  for (int i = 0 ; i < num ; ++i) {
    int offset = (intptr_t)(ptr - handle->schedule->data);

    memcpy (&type, ptr, sizeof (type));
    switch(type) {
      case SEND:
        NBC_DEBUG(5,"  SEND (offset %li) ", offset);
        NBC_GET_BYTES(ptr,sendargs);
        NBC_DEBUG(5,"*buf: %p, count: %i, type: %p, dest: %i, tag: %i)\n", sendargs.buf,
                  sendargs.count, sendargs.datatype, sendargs.dest, handle->tag);
        /* get an additional request */
        handle->req_count++;
        /* get buffer */
        if(sendargs.tmpbuf) {
          buf1=(char*)handle->tmpbuf+(long)sendargs.buf;
        } else {
          buf1=(void *)sendargs.buf;
        }
#ifdef NBC_TIMING
        Isend_time -= MPI_Wtime();
#endif
        tmp = (MPI_Request *) realloc ((void *) handle->req_array, handle->req_count * sizeof (MPI_Request));
        if (NULL == tmp) {
          return OMPI_ERR_OUT_OF_RESOURCE;
        }

        handle->req_array = tmp;

        res = MCA_PML_CALL(isend(buf1, sendargs.count, sendargs.datatype, sendargs.dest, handle->tag,
                                 MCA_PML_BASE_SEND_STANDARD, sendargs.local?handle->comm->c_local_comm:handle->comm,
                                 handle->req_array+handle->req_count - 1));
        if (OMPI_SUCCESS != res) {
          NBC_Error ("Error in MPI_Isend(%lu, %i, %p, %i, %i, %lu) (%i)", (unsigned long)buf1, sendargs.count,
                     sendargs.datatype, sendargs.dest, handle->tag, (unsigned long)handle->comm, res);
          return res;
        }
#ifdef NBC_TIMING
        Isend_time += MPI_Wtime();
#endif
        break;
      case RECV:
        NBC_DEBUG(5, "  RECV (offset %li) ", offset);
        NBC_GET_BYTES(ptr,recvargs);
        NBC_DEBUG(5, "*buf: %p, count: %i, type: %p, source: %i, tag: %i)\n", recvargs.buf, recvargs.count,
                  recvargs.datatype, recvargs.source, handle->tag);
        /* get an additional request - TODO: req_count NOT thread safe */
        handle->req_count++;
        /* get buffer */
        if(recvargs.tmpbuf) {
          buf1=(char*)handle->tmpbuf+(long)recvargs.buf;
        } else {
          buf1=recvargs.buf;
        }
#ifdef NBC_TIMING
        Irecv_time -= MPI_Wtime();
#endif
        tmp = (MPI_Request *) realloc ((void *) handle->req_array, handle->req_count * sizeof (MPI_Request));
        if (NULL == tmp) {
          return OMPI_ERR_OUT_OF_RESOURCE;
        }

        handle->req_array = tmp;

        res = MCA_PML_CALL(irecv(buf1, recvargs.count, recvargs.datatype, recvargs.source, handle->tag, recvargs.local?handle->comm->c_local_comm:handle->comm,
                                 handle->req_array+handle->req_count-1));
        if (OMPI_SUCCESS != res) {
          NBC_Error("Error in MPI_Irecv(%lu, %i, %p, %i, %i, %lu) (%i)", (unsigned long)buf1, recvargs.count,
                    recvargs.datatype, recvargs.source, handle->tag, (unsigned long)handle->comm, res);
          return res;
        }
#ifdef NBC_TIMING
        Irecv_time += MPI_Wtime();
#endif
        break;
      case OP:
        NBC_DEBUG(5, "  OP2  (offset %li) ", offset);
        NBC_GET_BYTES(ptr,opargs);
        NBC_DEBUG(5, "*buf1: %p, buf2: %p, count: %i, type: %p)\n", opargs.buf1, opargs.buf2,
                  opargs.count, opargs.datatype);
        /* get buffers */
        if(opargs.tmpbuf1) {
          buf1=(char*)handle->tmpbuf+(long)opargs.buf1;
        } else {
          buf1=(void *)opargs.buf1;
        }
        if(opargs.tmpbuf2) {
          buf2=(char*)handle->tmpbuf+(long)opargs.buf2;
        } else {
          buf2=opargs.buf2;
        }
        ompi_op_reduce(opargs.op, buf1, buf2, opargs.count, opargs.datatype);
        break;
      case COPY:
        NBC_DEBUG(5, "  COPY   (offset %li) ", offset);
        NBC_GET_BYTES(ptr,copyargs);
        NBC_DEBUG(5, "*src: %lu, srccount: %i, srctype: %p, *tgt: %lu, tgtcount: %i, tgttype: %p)\n",
                  (unsigned long) copyargs.src, copyargs.srccount, copyargs.srctype,
                  (unsigned long) copyargs.tgt, copyargs.tgtcount, copyargs.tgttype);
        /* get buffers */
        if(copyargs.tmpsrc) {
          buf1=(char*)handle->tmpbuf+(long)copyargs.src;
        } else {
          buf1=copyargs.src;
        }
        if(copyargs.tmptgt) {
          buf2=(char*)handle->tmpbuf+(long)copyargs.tgt;
        } else {
          buf2=copyargs.tgt;
        }
        res = NBC_Copy (buf1, copyargs.srccount, copyargs.srctype, buf2, copyargs.tgtcount, copyargs.tgttype,
                        handle->comm);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
          return res;
        }
        break;
      case UNPACK:
        NBC_DEBUG(5, "  UNPACK   (offset %li) ", offset);
        NBC_GET_BYTES(ptr,unpackargs);
        NBC_DEBUG(5, "*src: %lu, srccount: %i, srctype: %p, *tgt: %lu\n", (unsigned long) unpackargs.inbuf,
                  unpackargs.count, unpackargs.datatype, (unsigned long) unpackargs.outbuf);
        /* get buffers */
        if(unpackargs.tmpinbuf) {
          buf1=(char*)handle->tmpbuf+(long)unpackargs.inbuf;
        } else {
          buf1=unpackargs.inbuf;
        }
        if(unpackargs.tmpoutbuf) {
          buf2=(char*)handle->tmpbuf+(long)unpackargs.outbuf;
        } else {
          buf2=unpackargs.outbuf;
        }
        res = NBC_Unpack (buf1, unpackargs.count, unpackargs.datatype, buf2, handle->comm);
        if (OMPI_SUCCESS != res) {
          NBC_Error ("NBC_Unpack() failed (code: %i)", res);
          return res;
        }

        break;
      default:
        NBC_Error ("NBC_Start_round: bad type %li at offset %li", (long)type, offset);
        return OMPI_ERROR;
    }
  }

  /* check if we can make progress - not in the first round, this allows us to leave the
   * initialization faster and to reach more overlap
   *
   * threaded case: calling progress in the first round can lead to a
   * deadlock if NBC_Free is called in this round :-( */
  if (handle->row_offset) {
    res = NBC_Progress(handle);
    if ((NBC_OK != res) && (NBC_CONTINUE != res)) {
      return OMPI_ERROR;
    }
  }

  return OMPI_SUCCESS;
}

void NBC_Return_handle(ompi_coll_libnbc_request_t *request) {
  NBC_Free (request);
  OMPI_COLL_LIBNBC_REQUEST_RETURN(request);
}

int  NBC_Init_comm(MPI_Comm comm, NBC_Comminfo *comminfo) {
  comminfo->tag= MCA_COLL_BASE_TAG_NONBLOCKING_BASE;

#ifdef NBC_CACHE_SCHEDULE
  /* initialize the NBC_ALLTOALL SchedCache tree */
  comminfo->NBC_Dict[NBC_ALLTOALL] = hb_tree_new((dict_cmp_func)NBC_Alltoall_args_compare, NBC_SchedCache_args_delete_key_dummy, NBC_SchedCache_args_delete);
  if(comminfo->NBC_Dict[NBC_ALLTOALL] == NULL) { printf("Error in hb_tree_new()\n"); return OMPI_ERROR;; }
  NBC_DEBUG(1, "added tree at address %lu\n", (unsigned long)comminfo->NBC_Dict[NBC_ALLTOALL]);
  comminfo->NBC_Dict_size[NBC_ALLTOALL] = 0;
  /* initialize the NBC_ALLGATHER SchedCache tree */
  comminfo->NBC_Dict[NBC_ALLGATHER] = hb_tree_new((dict_cmp_func)NBC_Allgather_args_compare, NBC_SchedCache_args_delete_key_dummy, NBC_SchedCache_args_delete);
  if(comminfo->NBC_Dict[NBC_ALLGATHER] == NULL) { printf("Error in hb_tree_new()\n"); return OMPI_ERROR;; }
  NBC_DEBUG(1, "added tree at address %lu\n", (unsigned long)comminfo->NBC_Dict[NBC_ALLGATHER]);
  comminfo->NBC_Dict_size[NBC_ALLGATHER] = 0;
  /* initialize the NBC_ALLREDUCE SchedCache tree */
  comminfo->NBC_Dict[NBC_ALLREDUCE] = hb_tree_new((dict_cmp_func)NBC_Allreduce_args_compare, NBC_SchedCache_args_delete_key_dummy, NBC_SchedCache_args_delete);
  if(comminfo->NBC_Dict[NBC_ALLREDUCE] == NULL) { printf("Error in hb_tree_new()\n"); return OMPI_ERROR;; }
  NBC_DEBUG(1, "added tree at address %lu\n", (unsigned long)comminfo->NBC_Dict[NBC_ALLREDUCE]);
  comminfo->NBC_Dict_size[NBC_ALLREDUCE] = 0;
  /* initialize the NBC_BARRIER SchedCache tree - is not needed -
   * schedule is hung off directly */
  comminfo->NBC_Dict_size[NBC_BARRIER] = 0;
  /* initialize the NBC_BCAST SchedCache tree */
  comminfo->NBC_Dict[NBC_BCAST] = hb_tree_new((dict_cmp_func)NBC_Bcast_args_compare, NBC_SchedCache_args_delete_key_dummy, NBC_SchedCache_args_delete);
  if(comminfo->NBC_Dict[NBC_BCAST] == NULL) { printf("Error in hb_tree_new()\n"); return OMPI_ERROR;; }
  NBC_DEBUG(1, "added tree at address %lu\n", (unsigned long)comminfo->NBC_Dict[NBC_BCAST]);
  comminfo->NBC_Dict_size[NBC_BCAST] = 0;
  /* initialize the NBC_GATHER SchedCache tree */
  comminfo->NBC_Dict[NBC_GATHER] = hb_tree_new((dict_cmp_func)NBC_Gather_args_compare, NBC_SchedCache_args_delete_key_dummy, NBC_SchedCache_args_delete);
  if(comminfo->NBC_Dict[NBC_GATHER] == NULL) { printf("Error in hb_tree_new()\n"); return OMPI_ERROR;; }
  NBC_DEBUG(1, "added tree at address %lu\n", (unsigned long)comminfo->NBC_Dict[NBC_GATHER]);
  comminfo->NBC_Dict_size[NBC_GATHER] = 0;
  /* initialize the NBC_REDUCE SchedCache tree */
  comminfo->NBC_Dict[NBC_REDUCE] = hb_tree_new((dict_cmp_func)NBC_Reduce_args_compare, NBC_SchedCache_args_delete_key_dummy, NBC_SchedCache_args_delete);
  if(comminfo->NBC_Dict[NBC_REDUCE] == NULL) { printf("Error in hb_tree_new()\n"); return OMPI_ERROR;; }
  NBC_DEBUG(1, "added tree at address %lu\n", (unsigned long)comminfo->NBC_Dict[NBC_REDUCE]);
  comminfo->NBC_Dict_size[NBC_REDUCE] = 0;
  /* initialize the NBC_SCAN SchedCache tree */
  comminfo->NBC_Dict[NBC_SCAN] = hb_tree_new((dict_cmp_func)NBC_Scan_args_compare, NBC_SchedCache_args_delete_key_dummy, NBC_SchedCache_args_delete);
  if(comminfo->NBC_Dict[NBC_SCAN] == NULL) { printf("Error in hb_tree_new()\n"); return OMPI_ERROR;; }
  NBC_DEBUG(1, "added tree at address %lu\n", (unsigned long)comminfo->NBC_Dict[NBC_SCAN]);
  comminfo->NBC_Dict_size[NBC_SCAN] = 0;
  /* initialize the NBC_SCATTER SchedCache tree */
  comminfo->NBC_Dict[NBC_SCATTER] = hb_tree_new((dict_cmp_func)NBC_Scatter_args_compare, NBC_SchedCache_args_delete_key_dummy, NBC_SchedCache_args_delete);
  if(comminfo->NBC_Dict[NBC_SCATTER] == NULL) { printf("Error in hb_tree_new()\n"); return OMPI_ERROR;; }
  NBC_DEBUG(1, "added tree at address %lu\n", (unsigned long)comminfo->NBC_Dict[NBC_SCATTER]);
  comminfo->NBC_Dict_size[NBC_SCATTER] = 0;
#endif

  return OMPI_SUCCESS;
}

int NBC_Start(NBC_Handle *handle) {
  int res;

  /* bozo case */
  if ((ompi_request_t *)handle == &ompi_request_empty) {
    return OMPI_SUCCESS;
  }

  /* kick off first round */
  handle->super.req_state = OMPI_REQUEST_ACTIVE;
  res = NBC_Start_round(handle);
  if (OPAL_UNLIKELY(OMPI_SUCCESS != res)) {
    return res;
  }

  OPAL_THREAD_LOCK(&mca_coll_libnbc_component.lock);
  opal_list_append(&mca_coll_libnbc_component.active_requests, &(handle->super.super.super));
  OPAL_THREAD_UNLOCK(&mca_coll_libnbc_component.lock);

  return OMPI_SUCCESS;
}

int NBC_Schedule_request(NBC_Schedule *schedule, ompi_communicator_t *comm,
                         ompi_coll_libnbc_module_t *module, bool persistent,
                         ompi_request_t **request, void *tmpbuf) {
  int ret, tmp_tag;
  bool need_register = false;
  ompi_coll_libnbc_request_t *handle;

  /* no operation (e.g. one process barrier)? */
  if (((int *)schedule->data)[0] == 0 && schedule->data[sizeof(int)] == 0) {
    ret = nbc_get_noop_request(persistent, request);
    if (OMPI_SUCCESS != ret) {
      return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* update the module->tag here because other processes may have operations
     * and they may update the module->tag */
    OPAL_THREAD_LOCK(&module->mutex);
    tmp_tag = module->tag--;
    if (tmp_tag == MCA_COLL_BASE_TAG_NONBLOCKING_END) {
      tmp_tag = module->tag = MCA_COLL_BASE_TAG_NONBLOCKING_BASE;
      NBC_DEBUG(2,"resetting tags ...\n");
    }
    OPAL_THREAD_UNLOCK(&module->mutex);

    OBJ_RELEASE(schedule);
    free(tmpbuf);

    return OMPI_SUCCESS;
  }

  OMPI_COLL_LIBNBC_REQUEST_ALLOC(comm, persistent, handle);
  if (NULL == handle) return OMPI_ERR_OUT_OF_RESOURCE;

  handle->tmpbuf = NULL;
  handle->req_count = 0;
  handle->req_array = NULL;
  handle->comm = comm;
  handle->schedule = NULL;
  handle->row_offset = 0;
  handle->nbc_complete = persistent ? true : false;

  /******************** Do the tag and shadow comm administration ...  ***************/

  OPAL_THREAD_LOCK(&module->mutex);
  tmp_tag = module->tag--;
  if (tmp_tag == MCA_COLL_BASE_TAG_NONBLOCKING_END) {
      tmp_tag = module->tag = MCA_COLL_BASE_TAG_NONBLOCKING_BASE;
      NBC_DEBUG(2,"resetting tags ...\n");
  }

  if (true != module->comm_registered) {
      module->comm_registered = true;
      need_register = true;
  }
  OPAL_THREAD_UNLOCK(&module->mutex);

  handle->tag = tmp_tag;

  /* register progress */
  if (need_register) {
      int32_t tmp =
          OPAL_THREAD_ADD_FETCH32(&mca_coll_libnbc_component.active_comms, 1);
      if (tmp == 1) {
          opal_progress_register(ompi_coll_libnbc_progress);
      }
  }

  handle->comm=comm;
  /*printf("got module: %lu tag: %i\n", module, module->tag);*/

  /******************** end of tag and shadow comm administration ...  ***************/
  handle->comminfo = module;

  NBC_DEBUG(3, "got tag %i\n", handle->tag);

  handle->tmpbuf = tmpbuf;
  handle->schedule = schedule;
  *request = (ompi_request_t *) handle;

  return OMPI_SUCCESS;
}

#ifdef NBC_CACHE_SCHEDULE
void NBC_SchedCache_args_delete_key_dummy(void *k) {
    /* do nothing because the key and the data element are identical :-)
     * both (the single one :) is freed in NBC_<COLLOP>_args_delete() */
}

void NBC_SchedCache_args_delete(void *entry) {
  struct NBC_dummyarg *tmp = (struct NBC_dummyarg*)entry;
  OBJ_RELEASE(tmp->schedule);
  free(entry);
}
#endif
