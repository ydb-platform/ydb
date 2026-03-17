/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2006 The Trustees of Indiana University and Indiana
 *                    University Research and Technology
 *                    Corporation.  All rights reserved.
 * Copyright (c) 2006 The Technical University of Chemnitz. All
 *                    rights reserved.
 *
 * Author(s): Torsten Hoefler <htor@cs.indiana.edu>
 *
 * Copyright (c) 2012      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2014      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 */
#ifndef __NBC_INTERNAL_H__
#define __NBC_INTERNAL_H__
#include "ompi_config.h"

/* correct fortran bindings */
#define NBC_F77_FUNC_ F77_FUNC_

#include "mpi.h"

#include "coll_libnbc.h"
#if OPAL_CUDA_SUPPORT
#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_datatype_cuda.h"
#endif /* OPAL_CUDA_SUPPORT */
#include "ompi/include/ompi/constants.h"
#include "ompi/request/request.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <assert.h>
#include <math.h>
#include <string.h>
#include "libdict/dict.h"

#ifdef __cplusplus
extern "C" {
#endif

/* log(2) */
#define LOG2 0.69314718055994530941

/* true/false */
#define true 1
#define false 0

/* all collectives */
#define NBC_ALLGATHER 0
#define NBC_ALLGATHERV 1
#define NBC_ALLREDUCE 2
#define NBC_ALLTOALL 3
#define NBC_ALLTOALLV 4
#define NBC_ALLTOALLW 5
#define NBC_BARRIER 6
#define NBC_BCAST 7
#define NBC_EXSCAN 8
#define NBC_GATHER 9
#define NBC_GATHERV 10
#define NBC_REDUCE 11
#define NBC_REDUCESCAT 12
#define NBC_SCAN 13
#define NBC_SCATTER 14
#define NBC_SCATTERV 15
/* set the number of collectives in nbc.h !!!! */

/* several typedefs for NBC */

/* the function type enum */
typedef enum {
  SEND,
  RECV,
  OP,
  COPY,
  UNPACK
} NBC_Fn_type;

/* the send argument struct */
typedef struct {
  NBC_Fn_type type;
  int count;
  const void *buf;
  MPI_Datatype datatype;
  int dest;
  char tmpbuf;
  bool local;
} NBC_Args_send;

/* the receive argument struct */
typedef struct {
  NBC_Fn_type type;
  int count;
  void *buf;
  MPI_Datatype datatype;
  char tmpbuf;
  int source;
  bool local;
} NBC_Args_recv;

/* the operation argument struct */
typedef struct {
  NBC_Fn_type type;
  char tmpbuf1;
  char tmpbuf2;
  const void *buf1;
  void *buf2;
  MPI_Op op;
  MPI_Datatype datatype;
  int count;
} NBC_Args_op;

/* the copy argument struct */
typedef struct {
  NBC_Fn_type type;
  int srccount;
  void *src;
  void *tgt;
  MPI_Datatype srctype;
  MPI_Datatype tgttype;
  int tgtcount;
  char tmpsrc;
  char tmptgt;
} NBC_Args_copy;

/* unpack operation arguments */
typedef struct {
  NBC_Fn_type type;
  int count;
  void *inbuf;
  void *outbuf;
  MPI_Datatype datatype;
  char tmpinbuf;
  char tmpoutbuf;
} NBC_Args_unpack;

/* internal function prototypes */
int NBC_Sched_send (const void* buf, char tmpbuf, int count, MPI_Datatype datatype, int dest, NBC_Schedule *schedule, bool barrier);
int NBC_Sched_local_send (const void* buf, char tmpbuf, int count, MPI_Datatype datatype, int dest,NBC_Schedule *schedule, bool barrier);
int NBC_Sched_recv (void* buf, char tmpbuf, int count, MPI_Datatype datatype, int source, NBC_Schedule *schedule, bool barrier);
int NBC_Sched_local_recv (void* buf, char tmpbuf, int count, MPI_Datatype datatype, int source, NBC_Schedule *schedule, bool barrier);
int NBC_Sched_op (const void* buf1, char tmpbuf1, void* buf2, char tmpbuf2, int count, MPI_Datatype datatype,
                  MPI_Op op, NBC_Schedule *schedule, bool barrier);
int NBC_Sched_copy (void *src, char tmpsrc, int srccount, MPI_Datatype srctype, void *tgt, char tmptgt, int tgtcount,
                    MPI_Datatype tgttype, NBC_Schedule *schedule, bool barrier);
int NBC_Sched_unpack (void *inbuf, char tmpinbuf, int count, MPI_Datatype datatype, void *outbuf, char tmpoutbuf,
                      NBC_Schedule *schedule, bool barrier);

int NBC_Sched_barrier (NBC_Schedule *schedule);
int NBC_Sched_commit (NBC_Schedule *schedule);

#ifdef NBC_CACHE_SCHEDULE
/* this is a dummy structure which is used to get the schedule out of
 * the collop specific structure. The schedule pointer HAS to be at the
 * first position and should NOT BE REORDERED by the compiler (C
 * guarantees that */
struct NBC_dummyarg {
  NBC_Schedule *schedule;
};

typedef struct {
  NBC_Schedule *schedule;
  void *sendbuf;
  int sendcount;
  MPI_Datatype sendtype;
  void* recvbuf;
  int recvcount;
  MPI_Datatype recvtype;
} NBC_Alltoall_args;
int NBC_Alltoall_args_compare(NBC_Alltoall_args *a, NBC_Alltoall_args *b, void *param);

typedef struct {
  NBC_Schedule *schedule;
  void *sendbuf;
  int sendcount;
  MPI_Datatype sendtype;
  void* recvbuf;
  int recvcount;
  MPI_Datatype recvtype;
} NBC_Allgather_args;
int NBC_Allgather_args_compare(NBC_Allgather_args *a, NBC_Allgather_args *b, void *param);

typedef struct {
  NBC_Schedule *schedule;
  void *sendbuf;
  void* recvbuf;
  int count;
  MPI_Datatype datatype;
  MPI_Op op;
} NBC_Allreduce_args;
int NBC_Allreduce_args_compare(NBC_Allreduce_args *a, NBC_Allreduce_args *b, void *param);

typedef struct {
  NBC_Schedule *schedule;
  void *buffer;
  int count;
  MPI_Datatype datatype;
  int root;
} NBC_Bcast_args;
int NBC_Bcast_args_compare(NBC_Bcast_args *a, NBC_Bcast_args *b, void *param);

typedef struct {
  NBC_Schedule *schedule;
  void *sendbuf;
  int sendcount;
  MPI_Datatype sendtype;
  void* recvbuf;
  int recvcount;
  MPI_Datatype recvtype;
  int root;
} NBC_Gather_args;
int NBC_Gather_args_compare(NBC_Gather_args *a, NBC_Gather_args *b, void *param);

typedef struct {
  NBC_Schedule *schedule;
  void *sendbuf;
  void* recvbuf;
  int count;
  MPI_Datatype datatype;
  MPI_Op op;
  int root;
} NBC_Reduce_args;
int NBC_Reduce_args_compare(NBC_Reduce_args *a, NBC_Reduce_args *b, void *param);

typedef struct {
  NBC_Schedule *schedule;
  void *sendbuf;
  void* recvbuf;
  int count;
  MPI_Datatype datatype;
  MPI_Op op;
} NBC_Scan_args;
int NBC_Scan_args_compare(NBC_Scan_args *a, NBC_Scan_args *b, void *param);

typedef struct {
  NBC_Schedule *schedule;
  void *sendbuf;
  int sendcount;
  MPI_Datatype sendtype;
  void* recvbuf;
  int recvcount;
  MPI_Datatype recvtype;
  int root;
} NBC_Scatter_args;
int NBC_Scatter_args_compare(NBC_Scatter_args *a, NBC_Scatter_args *b, void *param);

/* Schedule cache structures/functions */
void NBC_SchedCache_args_delete(void *entry);
void NBC_SchedCache_args_delete_key_dummy(void *k);

#endif


int NBC_Start(NBC_Handle *handle);
int NBC_Schedule_request(NBC_Schedule *schedule, ompi_communicator_t *comm,
                         ompi_coll_libnbc_module_t *module, bool persistent,
                         ompi_request_t **request, void *tmpbuf);
void NBC_Return_handle(ompi_coll_libnbc_request_t *request);
static inline int NBC_Type_intrinsic(MPI_Datatype type);
int NBC_Create_fortran_handle(int *fhandle, NBC_Handle **handle);

/* some macros */

static inline void NBC_Error (char *format, ...) {
  va_list args;

  va_start (args, format);
  vfprintf (stderr, format, args);
  fprintf (stderr, "\n");
  va_end (args);
}

/* a schedule has the following format:
 * [schedule] ::= [size][round-schedule][delimiter][round-schedule][delimiter]...[end]
 * [size] ::= size of the schedule (int)
 * [round-schedule] ::= [num][type][type-args][type][type-args]...
 * [num] ::= number of elements in round (int)
 * [type] ::= function type (NBC_Fn_type)
 * [type-args] ::= type specific arguments (NBC_Args_send, NBC_Args_recv or, NBC_Args_op)
 * [delimiter] ::= 1 (char) - indicates that a round follows
 * [end] ::= 0 (char) - indicates that this is the last round
 */

/*
 * The addresses of components of a round-schedule may be poorly aligned.
 * E.g., single-char delimiters can push addresses to odd-byte boundaries.
 * Or even ints can push 8-byte pointers to 4-byte boundaries.
 * So, for greater portability, we access components of a round-schedule with memcpy.
 */
#define NBC_GET_BYTES(ptr,x) {memcpy(&x,ptr,sizeof(x)); ptr += sizeof(x);}
#define NBC_PUT_BYTES(ptr,x) {memcpy(ptr,&x,sizeof(x)); ptr += sizeof(x);}

/* NBC_GET_ROUND_SIZE returns the size in bytes of a round of a NBC_Schedule
 * schedule. A round has the format:
 * [num]{[type][type-args]}
 * e.g. [(int)2][(NBC_Fn_type)SEND][(NBC_Args_send)SEND-ARGS][(NBC_Fn_type)RECV][(NBC_Args_recv)RECV-ARGS] */
static inline void nbc_get_round_size (char *p, unsigned long *size) {
  NBC_Fn_type type;
  unsigned long offset = 0;
  int num;

  NBC_GET_BYTES(p,num);
  /*NBC_DEBUG(10, "GET_ROUND_SIZE got %i elements\n", num); */
  for (int i = 0 ; i < num ; ++i) {
    memcpy (&type, p + offset, sizeof (type));
    switch(type) {
    case SEND:
      /*printf("found a SEND at offset %li\n", (long)p-(long)schedule); */
      offset += sizeof(NBC_Args_send);
      break;
    case RECV:
      /*printf("found a RECV at offset %li\n", (long)p-(long)schedule); */
      offset += sizeof(NBC_Args_recv);
      break;
    case OP:
      /*printf("found a OP at offset %li\n", (long)p-(long)schedule); */
      offset += sizeof(NBC_Args_op);            \
      break;
    case COPY:
      /*printf("found a COPY at offset %li\n", (long)p-(long)schedule); */
      offset += sizeof(NBC_Args_copy);
      break;
    case UNPACK:
      /*printf("found a UNPACK at offset %li\n", (long)p-(long)schedule); */
      offset += sizeof(NBC_Args_unpack);
      break;
    default:
      NBC_Error("NBC_GET_ROUND_SIZE: bad type %i at offset %li", type, offset);
      return;
    }
  }

  *size = offset + sizeof (int);
}


/* returns the size of a schedule in bytes */
static inline int nbc_schedule_get_size (NBC_Schedule *schedule) {
  return schedule->size;
}

/* increase the size of a schedule by size bytes */
static inline void nbc_schedule_inc_size (NBC_Schedule *schedule, int size) {
  schedule->size += size;
}

/* increments the number of operations in the last round */
static inline void nbc_schedule_inc_round (NBC_Schedule *schedule) {
  int last_round_num;
  char *lastround;

  lastround = schedule->data + schedule->current_round_offset;

  /* increment the count in the last round of the schedule (memcpy is used
   * to protect against unaligned access) */
  memcpy (&last_round_num, lastround, sizeof (last_round_num));
  ++last_round_num;
  memcpy (lastround, &last_round_num, sizeof (last_round_num));
}

/* returns a no-operation request (e.g. for one process barrier) */
static inline int nbc_get_noop_request(bool persistent, ompi_request_t **request) {
  if (persistent) {
    return ompi_request_persistent_noop_create(request);
  } else {
    *request = &ompi_request_empty;
    return OMPI_SUCCESS;
  }
}

/* NBC_PRINT_ROUND prints a round in a schedule. A round has the format:
 * [num]{[type][type-args]} types: [int]{[enum][args-type]}
 * e.g. [2][SEND][SEND-ARGS][RECV][RECV-ARGS] */
#define NBC_PRINT_ROUND(schedule) \
 {  \
   int myrank, i, num; \
   char *p = (char*) schedule; \
   NBC_Fn_type type; \
   NBC_Args_send     sendargs; \
   NBC_Args_recv     recvargs; \
   NBC_Args_op         opargs; \
   NBC_Args_copy     copyargs; \
   NBC_Args_unpack unpackargs; \
     \
   NBC_GET_BYTES(p,num); \
   MPI_Comm_rank(MPI_COMM_WORLD, &myrank); \
   printf("[%i] has %i actions: \n", myrank, num); \
   for (i=0; i<num; i++) { \
     NBC_GET_BYTES(p,type); \
     switch(type) { \
       case SEND: \
         printf("[%i]  SEND (offset %li) ", myrank, (long)p-(long)schedule); \
         NBC_GET_BYTES(p,sendargs); \
         printf("*buf: %lu, count: %i, type: %lu, dest: %i)\n", (unsigned long)sendargs.buf, sendargs.count, (unsigned long)sendargs.datatype, sendargs.dest); \
         break; \
       case RECV: \
         printf("[%i]  RECV (offset %li) ", myrank, (long)p-(long)schedule); \
         NBC_GET_BYTES(p,recvargs); \
         printf("*buf: %lu, count: %i, type: %lu, source: %i)\n", (unsigned long)recvargs.buf, recvargs.count, (unsigned long)recvargs.datatype, recvargs.source); \
         break; \
       case OP: \
         printf("[%i]  OP   (offset %li) ", myrank, (long)p-(long)schedule); \
         NBC_GET_BYTES(p,opargs); \
         printf("*buf1: %lu, buf2: %lu, count: %i, type: %lu)\n", (unsigned long)opargs.buf1, (unsigned long)opargs.buf2, opargs.count, (unsigned long)opargs.datatype); \
         break; \
       case COPY: \
         printf("[%i]  COPY   (offset %li) ", myrank, (long)p-(long)schedule); \
         NBC_GET_BYTES(p,copyargs); \
         printf("*src: %lu, srccount: %i, srctype: %lu, *tgt: %lu, tgtcount: %i, tgttype: %lu)\n", (unsigned long)copyargs.src, copyargs.srccount, (unsigned long)copyargs.srctype, (unsigned long)copyargs.tgt, copyargs.tgtcount, (unsigned long)copyargs.tgttype); \
         break; \
       case UNPACK: \
         printf("[%i]  UNPACK   (offset %li) ", myrank, (long)p-(long)schedule); \
         NBC_GET_BYTES(p,unpackargs); \
         printf("*src: %lu, srccount: %i, srctype: %lu, *tgt: %lu\n",(unsigned long)unpackargs.inbuf, unpackargs.count, (unsigned long)unpackargs.datatype, (unsigned long)unpackargs.outbuf); \
         break; \
       default: \
         printf("[%i] NBC_PRINT_ROUND: bad type %i at offset %li\n", myrank, type, (long)p-sizeof(type)-(long)schedule); \
         return NBC_BAD_SCHED; \
     } \
   } \
   printf("\n"); \
 }

#define NBC_PRINT_SCHED(schedule) \
{ \
  int size, myrank; \
  long round_size; \
  char *ptr; \
 \
  NBC_GET_SIZE(schedule, size); \
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank); \
  printf("[%i] printing schedule of size %i\n", myrank, size); \
 \
  /* ptr begins at first round (first int is overall size) */ \
  ptr = (char*)schedule+sizeof(int); \
  while ((long)ptr-(long)schedule < size) { \
    NBC_GET_ROUND_SIZE(ptr, round_size); \
    printf("[%i] Round at byte %li (size %li) ", myrank, (long)ptr-(long)schedule, round_size); \
    NBC_PRINT_ROUND(ptr); \
    ptr += round_size; \
    ptr += sizeof(char); /* barrier delimiter */ \
  } \
}

/*
#define NBC_DEBUG(level, ...) {}
*/

static inline void NBC_DEBUG(int level, const char *fmt, ...)
{
#if NBC_DLEVEL > 0
  va_list ap;
  int rank;

  if(NBC_DLEVEL >= level) {
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    printf("[LibNBC - %i] ", rank);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end (ap);
  }
#endif
}

/* returns true (1) or false (0) if type is intrinsic or not */
static inline int NBC_Type_intrinsic(MPI_Datatype type) {

  if( ( type == MPI_INT ) ||
      ( type == MPI_LONG ) ||
      ( type == MPI_SHORT ) ||
      ( type == MPI_UNSIGNED ) ||
      ( type == MPI_UNSIGNED_SHORT ) ||
      ( type == MPI_UNSIGNED_LONG ) ||
      ( type == MPI_FLOAT ) ||
      ( type == MPI_DOUBLE ) ||
      ( type == MPI_LONG_DOUBLE ) ||
      ( type == MPI_BYTE ) ||
      ( type == MPI_FLOAT_INT) ||
      ( type == MPI_DOUBLE_INT) ||
      ( type == MPI_LONG_INT) ||
      ( type == MPI_2INT) ||
      ( type == MPI_SHORT_INT) ||
      ( type == MPI_LONG_DOUBLE_INT))
    return 1;
  else
    return 0;
}

/* let's give a try to inline functions */
static inline int NBC_Copy(const void *src, int srccount, MPI_Datatype srctype, void *tgt, int tgtcount, MPI_Datatype tgttype, MPI_Comm comm) {
  int res;

  res = ompi_datatype_sndrcv(src, srccount, srctype, tgt, tgtcount, tgttype);
  if (OMPI_SUCCESS != res) {
    NBC_Error ("MPI Error in ompi_datatype_sndrcv() (%i)", res);
    return res;
  }

  return OMPI_SUCCESS;
}

static inline int NBC_Unpack(void *src, int srccount, MPI_Datatype srctype, void *tgt, MPI_Comm comm) {
  MPI_Aint size, pos;
  int res;
  ptrdiff_t ext, lb;

#if OPAL_CUDA_SUPPORT
  if(NBC_Type_intrinsic(srctype) && !(opal_cuda_check_bufs((char *)tgt, (char *)src))) {
#else
  if(NBC_Type_intrinsic(srctype)) {
#endif /* OPAL_CUDA_SUPPORT */
    /* if we have the same types and they are contiguous (intrinsic
     * types are contiguous), we can just use a single memcpy */
    res = ompi_datatype_pack_external_size("external32", srccount, srctype, &size);
    res = ompi_datatype_get_extent (srctype, &lb, &ext);
    if (OMPI_SUCCESS != res) {
      NBC_Error ("MPI Error in MPI_Type_extent() (%i)", res);
      return res;
    }

    memcpy(tgt, src, srccount * ext);

  } else {
    /* we have to unpack */
    pos = 0;
    res = ompi_datatype_unpack_external("external32", src, size, &pos, tgt, srccount, srctype);
    if (MPI_SUCCESS != res) {
      NBC_Error ("MPI Error in ompi_datatype_unpack_external() (%i)", res);
      return res;
    }
  }

  return OMPI_SUCCESS;
}

/* deletes elements from dict until low watermark is reached */
static inline void NBC_SchedCache_dictwipe(hb_tree *dict, int *size) {
  hb_itor *itor;

  itor = hb_itor_new(dict);
  for (; hb_itor_valid(itor) && (*size>NBC_SCHED_DICT_LOWER); hb_itor_next(itor)) {
    hb_tree_remove(dict, hb_itor_key(itor), 0);
    *size = *size-1;
  }
  hb_itor_destroy(itor);
}

#define NBC_IN_PLACE(sendbuf, recvbuf, inplace) \
{ \
  inplace = 0; \
  if(recvbuf == sendbuf) { \
    inplace = 1; \
  } else \
  if(sendbuf == MPI_IN_PLACE) { \
    sendbuf = recvbuf; \
    inplace = 1; \
  } else \
  if(recvbuf == MPI_IN_PLACE) { \
    recvbuf = (void *)sendbuf; \
    inplace = 1; \
  } \
}

int NBC_Comm_neighbors_count (ompi_communicator_t *comm, int *indegree, int *outdegree);
int NBC_Comm_neighbors (ompi_communicator_t *comm, int **sources, int *source_count, int **destinations, int *dest_count);

#ifdef __cplusplus
}
#endif

#endif


