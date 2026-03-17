/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**********************************************************************
 * Copyright (C) 2000-2004 by Etnus, LLC.
 * Copyright (C) 1999 by Etnus, Inc.
 * Copyright (C) 1997-1998 Dolphin Interconnect Solutions Inc.
 *
 * Permission is hereby granted to use, reproduce, prepare derivative
 * works, and to redistribute to others.
 *
 *				  DISCLAIMER
 *
 * Neither Dolphin Interconnect Solutions, Etnus LLC, nor any of their
 * employees, makes any warranty express or implied, or assumes any
 * legal liability or responsibility for the accuracy, completeness,
 * or usefulness of any information, apparatus, product, or process
 * disclosed, or represents that its use would not infringe privately
 * owned rights.
 *
 * This code was written by
 * James Cownie: Dolphin Interconnect Solutions. <jcownie@dolphinics.com>
 *               Etnus LLC <jcownie@etnus.com>
 **********************************************************************/

/* Update log
 *
 * May 19 1998 JHC: Changed the names of the structs now that we don't
 *              include this directly in mpi_interface.h
 * Oct 27 1997 JHC: Structure definitions for structures used to hold MPICH
 *              info required by the DLL for dumping message queues.
 */

#ifndef OMPI_MSGQ_DLL_DEFS_H
#define OMPI_MSGQ_DLL_DEFS_H

#include "ompi_common_dll_defs.h"

/***********************************************************************
 * Information associated with a specific process
 */

typedef struct group_t
{
    mqs_taddr_t group_base;          /* Where was it in the process  */
    int         ref_count;           /* How many references to us */
    int         entries;             /* How many entries */
    int*        local_to_global;     /* The translation table */
} group_t;

/* Internal structure we hold for each communicator */
typedef struct communicator_t
{
    struct communicator_t * next;
    group_t *               group;		/* Translations */
    mqs_taddr_t             comm_ptr;   /* pointer to the communicator in the process memory */
    int                     present;    /* validation marker */
    mqs_communicator        comm_info;  /* Info needed at the higher level */
} communicator_t;

typedef struct mqs_ompi_opal_list_t_pos {
    mqs_taddr_t current_item;
    mqs_taddr_t list;
    mqs_taddr_t sentinel;
} mqs_opal_list_t_pos;

typedef struct {
    mqs_opal_list_t_pos opal_list_t_pos;
    mqs_taddr_t current_item;
    mqs_taddr_t upper_bound;
    mqs_tword_t header_space;
    mqs_taddr_t free_list;
    mqs_tword_t fl_frag_class;         /* opal_class_t* */
    mqs_tword_t fl_mpool;              /* struct mca_mpool_base_module_t* */
    mqs_tword_t fl_frag_size;          /* size_t */
    mqs_tword_t fl_frag_alignment;     /* size_t */
    mqs_tword_t fl_num_per_alloc;      /* size_t */
    mqs_tword_t fl_num_allocated;      /* size_t */
    mqs_tword_t fl_num_initial_alloc;  /* size_t */
} mqs_opal_free_list_t_pos;


/* Information for a single process, a list of communicators, some
 * useful addresses, and the state of the iterators.
 */
typedef struct
{
  struct communicator_t *communicator_list;	/* List of communicators in the process */

  /* Addresses in the target process */
  mqs_taddr_t send_queue_base;			/* Where to find the send message queues */
  mqs_taddr_t recv_queue_base;			/* Where to find the recv message queues */
  mqs_taddr_t sendq_base;			/* Where to find the send queue */
  mqs_taddr_t commlist_base;			/* Where to find the list of communicators */
  /* Other info we need to remember about it */
  mqs_tword_t comm_number_free;         /* the number of available positions in
                                         * the communicator array. */
  mqs_tword_t comm_lowest_free;         /* the lowest free communicator */
  mqs_tword_t show_internal_requests;   /* show or not the Open MPI internal requests */
  /* State for the iterators */
  struct communicator_t *current_communicator;	/* Easy, we're walking a simple list */

  int world_proc_array_entries;
  mqs_taddr_t* world_proc_array;

  mqs_opal_free_list_t_pos next_msg;            /* And state for the message iterator */
  mqs_op_class  what;				/* What queue are we looking on */
} mpi_process_info_extra;

#endif
