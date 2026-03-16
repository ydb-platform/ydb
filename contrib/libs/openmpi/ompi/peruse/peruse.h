/*
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _PERUSE_H_
#define _PERUSE_H_

#include "mpi.h"

/* PERUSE type declarations */
typedef void* peruse_event_h;    /* Opaque event handle XXX */

typedef struct _peruse_comm_spec_t {
    MPI_Comm      comm;
    void *        buf;
    int           count;
    MPI_Datatype  datatype;
    int           peer;
    int           tag;
    int           operation;
} peruse_comm_spec_t;

typedef int (peruse_comm_callback_f)(peruse_event_h event_h,
              MPI_Aint unique_id, peruse_comm_spec_t * spec, void * param);

/* PERUSE constants */
enum {
    PERUSE_SUCCESS = 0,       /* Success *//* XXX Devation from 1.11 */
    PERUSE_ERR_INIT,          /* PERUSE initialization failure */
    PERUSE_ERR_GENERIC,       /* Generic unspecified error */
    PERUSE_ERR_MALLOC,        /* Memory-related error */
    PERUSE_ERR_EVENT,         /* Invalid event descriptor */
    PERUSE_ERR_EVENT_HANDLE,  /* Invalid event handle */
    PERUSE_ERR_PARAMETER,     /* Invalid input parameter */
    PERUSE_ERR_MPI_INIT,      /* MPI has not been initialized */
    PERUSE_ERR_COMM,          /* MPI_ERR_COMM class */
    PERUSE_ERR_MPI_OBJECT     /* Error with associated MPI object */
};

enum {
    PERUSE_EVENT_INVALID = -1, /* Must differ in value from PERUSE_SUCCESS. Devation from 1.11 */

    /* Point-to-point request events */
    PERUSE_COMM_REQ_ACTIVATE,
    PERUSE_COMM_REQ_MATCH_UNEX,
    PERUSE_COMM_REQ_INSERT_IN_POSTED_Q,
    PERUSE_COMM_REQ_REMOVE_FROM_POSTED_Q,
    PERUSE_COMM_REQ_XFER_BEGIN,
    PERUSE_COMM_REQ_XFER_CONTINUE,      /* Open MPI extension */
    PERUSE_COMM_REQ_XFER_END,
    PERUSE_COMM_REQ_COMPLETE,
    PERUSE_COMM_REQ_NOTIFY,
    PERUSE_COMM_MSG_ARRIVED,
    PERUSE_COMM_MSG_INSERT_IN_UNEX_Q,
    PERUSE_COMM_MSG_REMOVE_FROM_UNEX_Q,
    PERUSE_COMM_MSG_MATCH_POSTED_REQ,

    /* Queue events*/
    PERUSE_COMM_SEARCH_POSTED_Q_BEGIN,
    PERUSE_COMM_SEARCH_POSTED_Q_END,
    PERUSE_COMM_SEARCH_UNEX_Q_BEGIN,    /* XXX Deviation from 1.11 */
    PERUSE_COMM_SEARCH_UNEX_Q_END,

    /* Collective events */
    /* IO events */
    /* One-sided events */
    PERUSE_CUSTOM_EVENT
};

/* Scope of message queues */
enum {
    PERUSE_PER_COMM=0,                  /* XXX Devation from 1.11 */
    PERUSE_GLOBAL
};

/* Operation values */
enum {
    PERUSE_SEND=0,                      /* XXX Devation from 1.11 */
    PERUSE_RECV
};

#define PERUSE_EVENT_HANDLE_NULL ((peruse_event_h)0)

/*
 * I. Environment
 */

/* PERUSE initialization */
OMPI_DECLSPEC int PERUSE_Init( void );

/* Query all implemented events */
OMPI_DECLSPEC int PERUSE_Query_supported_events( int* num_supported,
                                                 char*** event_names,
                                                 int** events );

/* Query supported events */
OMPI_DECLSPEC int PERUSE_Query_event( const char* event_name, int* event );

/* Query event name */
OMPI_DECLSPEC int PERUSE_Query_event_name( int event, char** event_name );

/* Get environment variables that affect MPI library behavior */
OMPI_DECLSPEC int PERUSE_Query_environment( int* env_size, char*** env );

/* Query the scope of queue metrics - global or per communicator */
OMPI_DECLSPEC int PERUSE_Query_queue_event_scope( int* scope );

/*
 * II. Events, objects initialization and manipulation
 */
/* Initialize event associated with an MPI communicator */
OMPI_DECLSPEC int PERUSE_Event_comm_register( int                       event,
                                              MPI_Comm                  comm,
                                              peruse_comm_callback_f *  callback_fn,
                                              void *                    param,
                                              peruse_event_h *          event_h );

/* Start collecting data (activate event) */
OMPI_DECLSPEC int PERUSE_Event_activate( peruse_event_h event_h );

/* Stop collecting data (deactivate event) */
OMPI_DECLSPEC int PERUSE_Event_deactivate( peruse_event_h event_h );

/* Free event handle */
OMPI_DECLSPEC int PERUSE_Event_release( peruse_event_h* event_h );

/* Set a new comm callback */
OMPI_DECLSPEC int PERUSE_Event_comm_callback_set( peruse_event_h            event_h,
                                                  peruse_comm_callback_f*   callback_fn,
                                                  void *                    param);

/* Get the current comm callback */
OMPI_DECLSPEC int PERUSE_Event_comm_callback_get( peruse_event_h            event_h,
                                                  peruse_comm_callback_f**  callback_fn,
                                                  void **                   param );

/* Obtain event descriptor from an event handle (reverse lookup) */
OMPI_DECLSPEC int PERUSE_Event_get( peruse_event_h event_h, int* event );

/* Obtain MPI object associated with event handle */
OMPI_DECLSPEC int PERUSE_Event_object_get( peruse_event_h event_h, void** mpi_object );

/* Propagation mode */
OMPI_DECLSPEC int PERUSE_Event_propagate( peruse_event_h event_h, int mode );

#endif
