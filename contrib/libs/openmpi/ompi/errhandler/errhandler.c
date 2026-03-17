/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2016 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>

#include "ompi/communicator/communicator.h"
#include "ompi/win/win.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/errhandler/errhandler_predefined.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/mca/pmix/pmix.h"


/*
 * Table for Fortran <-> C errhandler handle conversion
 */
opal_pointer_array_t ompi_errhandler_f_to_c_table = {{0}};

/*
 * default errhandler id
 */
static size_t default_errhandler_id = SIZE_MAX;

/*
 * Class information
 */
static void ompi_errhandler_construct(ompi_errhandler_t *eh);
static void ompi_errhandler_destruct(ompi_errhandler_t *eh);


/*
 * Class instance
 */
OBJ_CLASS_INSTANCE(ompi_errhandler_t, opal_object_t, ompi_errhandler_construct,
                   ompi_errhandler_destruct);


/*
 * _addr flavors are for F03 bindings
 */
ompi_predefined_errhandler_t ompi_mpi_errhandler_null = {{{0}}};
ompi_predefined_errhandler_t *ompi_mpi_errhandler_null_addr =
    &ompi_mpi_errhandler_null;
ompi_predefined_errhandler_t ompi_mpi_errors_are_fatal = {{{0}}};
ompi_predefined_errhandler_t *ompi_mpi_errors_are_fatal_addr =
    &ompi_mpi_errors_are_fatal;
ompi_predefined_errhandler_t ompi_mpi_errors_return = {{{0}}};
ompi_predefined_errhandler_t *ompi_mpi_errors_return_addr =
    &ompi_mpi_errors_return;
ompi_predefined_errhandler_t ompi_mpi_errors_throw_exceptions = {{{0}}};
ompi_predefined_errhandler_t *ompi_mpi_errors_throw_exceptions_addr =
    &ompi_mpi_errors_throw_exceptions;


/*
 * Initialize OMPI errhandler infrastructure
 */
int ompi_errhandler_init(void)
{
  /* initialize ompi_errhandler_f_to_c_table */

  OBJ_CONSTRUCT( &ompi_errhandler_f_to_c_table, opal_pointer_array_t);
  if( OPAL_SUCCESS != opal_pointer_array_init(&ompi_errhandler_f_to_c_table, 8,
                                              OMPI_FORTRAN_HANDLE_MAX, 16) ) {
    return OMPI_ERROR;
  }

  /* Initialize the predefined error handlers */
  OBJ_CONSTRUCT( &ompi_mpi_errhandler_null.eh, ompi_errhandler_t );
  if( ompi_mpi_errhandler_null.eh.eh_f_to_c_index != OMPI_ERRHANDLER_NULL_FORTRAN )
      return OMPI_ERROR;
  ompi_mpi_errhandler_null.eh.eh_mpi_object_type = OMPI_ERRHANDLER_TYPE_PREDEFINED;
  ompi_mpi_errhandler_null.eh.eh_lang = OMPI_ERRHANDLER_LANG_C;
  ompi_mpi_errhandler_null.eh.eh_comm_fn = NULL;
  ompi_mpi_errhandler_null.eh.eh_file_fn = NULL;
  ompi_mpi_errhandler_null.eh.eh_win_fn  = NULL ;
  ompi_mpi_errhandler_null.eh.eh_fort_fn = NULL;
  strncpy (ompi_mpi_errhandler_null.eh.eh_name, "MPI_ERRHANDLER_NULL",
	   strlen("MPI_ERRHANDLER_NULL")+1 );


  OBJ_CONSTRUCT( &ompi_mpi_errors_are_fatal.eh, ompi_errhandler_t );
  if( ompi_mpi_errors_are_fatal.eh.eh_f_to_c_index != OMPI_ERRORS_ARE_FATAL_FORTRAN )
      return OMPI_ERROR;
  ompi_mpi_errors_are_fatal.eh.eh_mpi_object_type = OMPI_ERRHANDLER_TYPE_PREDEFINED;
  ompi_mpi_errors_are_fatal.eh.eh_lang = OMPI_ERRHANDLER_LANG_C;
  ompi_mpi_errors_are_fatal.eh.eh_comm_fn = ompi_mpi_errors_are_fatal_comm_handler;
  ompi_mpi_errors_are_fatal.eh.eh_file_fn = ompi_mpi_errors_are_fatal_file_handler;
  ompi_mpi_errors_are_fatal.eh.eh_win_fn  = ompi_mpi_errors_are_fatal_win_handler ;
  ompi_mpi_errors_are_fatal.eh.eh_fort_fn = NULL;
  strncpy (ompi_mpi_errors_are_fatal.eh.eh_name, "MPI_ERRORS_ARE_FATAL",
	   strlen("MPI_ERRORS_ARE_FATAL")+1 );

  OBJ_CONSTRUCT( &ompi_mpi_errors_return.eh, ompi_errhandler_t );
  if( ompi_mpi_errors_return.eh.eh_f_to_c_index != OMPI_ERRORS_RETURN_FORTRAN )
      return OMPI_ERROR;
  ompi_mpi_errors_return.eh.eh_mpi_object_type  = OMPI_ERRHANDLER_TYPE_PREDEFINED;
  ompi_mpi_errors_return.eh.eh_lang = OMPI_ERRHANDLER_LANG_C;
  ompi_mpi_errors_return.eh.eh_comm_fn = ompi_mpi_errors_return_comm_handler;
  ompi_mpi_errors_return.eh.eh_file_fn = ompi_mpi_errors_return_file_handler;
  ompi_mpi_errors_return.eh.eh_win_fn  = ompi_mpi_errors_return_win_handler;
  ompi_mpi_errors_return.eh.eh_fort_fn = NULL;
  strncpy (ompi_mpi_errors_return.eh.eh_name, "MPI_ERRORS_RETURN",
	   strlen("MPI_ERRORS_RETURN")+1 );

  /* If we're going to use C++, functions will be fixed up during
     MPI::Init.  Note that it is proper to use ERRHANDLER_LANG_C here;
     the dispatch function is in C (although in libmpi_cxx); the
     conversion from C handles to C++ handles happens in that dispatch
     function -- not the errhandler_invoke.c stuff here in libmpi. */
  OBJ_CONSTRUCT( &ompi_mpi_errors_throw_exceptions.eh, ompi_errhandler_t );
  ompi_mpi_errors_throw_exceptions.eh.eh_mpi_object_type =
      OMPI_ERRHANDLER_TYPE_PREDEFINED;
  ompi_mpi_errors_throw_exceptions.eh.eh_lang = OMPI_ERRHANDLER_LANG_C;
  ompi_mpi_errors_throw_exceptions.eh.eh_comm_fn =
      ompi_mpi_errors_are_fatal_comm_handler;
  ompi_mpi_errors_throw_exceptions.eh.eh_file_fn =
      ompi_mpi_errors_are_fatal_file_handler;
  ompi_mpi_errors_throw_exceptions.eh.eh_win_fn  =
      ompi_mpi_errors_are_fatal_win_handler ;
  ompi_mpi_errors_throw_exceptions.eh.eh_fort_fn = NULL;
  strncpy (ompi_mpi_errors_throw_exceptions.eh.eh_name, "MPI_ERRORS_THROW_EXCEPTIONS",
	   strlen("MPI_ERRORS_THROW_EXCEPTIONS")+1 );

  /* All done */

  return OMPI_SUCCESS;
}


/*
 * Clean up the errorhandler resources
 */
int ompi_errhandler_finalize(void)
{
    OBJ_DESTRUCT(&ompi_mpi_errhandler_null.eh);
    OBJ_DESTRUCT(&ompi_mpi_errors_return.eh);
    OBJ_DESTRUCT(&ompi_mpi_errors_throw_exceptions.eh);
    OBJ_DESTRUCT(&ompi_mpi_errors_are_fatal.eh);

    /* JMS Add stuff here checking for unreleased errorhandlers,
       similar to communicators, info handles, etc. */
    opal_pmix.deregister_evhandler(default_errhandler_id, NULL, NULL);

    /* Remove errhandler F2C table */

    OBJ_DESTRUCT(&ompi_errhandler_f_to_c_table);

    /* All done */

    return OMPI_SUCCESS;
}


ompi_errhandler_t *ompi_errhandler_create(ompi_errhandler_type_t object_type,
					                      ompi_errhandler_generic_handler_fn_t *func,
                                          ompi_errhandler_lang_t lang)
{
  ompi_errhandler_t *new_errhandler;

  /* Create a new object and ensure that it's valid */

  new_errhandler = OBJ_NEW(ompi_errhandler_t);
  if (NULL != new_errhandler) {
    if (0 > new_errhandler->eh_f_to_c_index) {
      OBJ_RELEASE(new_errhandler);
      new_errhandler = NULL;
    } else {

      /* We cast the user's callback function to any one of the
         function pointer types in the union; it doesn't matter which.
         It only matters that we dereference/use the right member when
         invoking the callback. */

      new_errhandler->eh_mpi_object_type = object_type;
      new_errhandler->eh_lang = lang;
      switch (object_type ) {
	  case (OMPI_ERRHANDLER_TYPE_COMM):
	      new_errhandler->eh_comm_fn = (MPI_Comm_errhandler_function *)func;
	      break;
	  case (OMPI_ERRHANDLER_TYPE_FILE):
	      new_errhandler->eh_file_fn = (ompi_file_errhandler_fn *)func;
	      break;
	  case (OMPI_ERRHANDLER_TYPE_WIN):
	      new_errhandler->eh_win_fn = (MPI_Win_errhandler_function *)func;
	      break;
	  default:
	      break;
      }

      new_errhandler->eh_fort_fn = (ompi_errhandler_fortran_handler_fn_t *)func;
    }
  }

  /* All done */

  return new_errhandler;
}

/* registration callback */
void ompi_errhandler_registration_callback(int status,
                                           size_t errhandler_ref,
                                           void *cbdata)
{
    ompi_errhandler_errtrk_t *errtrk = (ompi_errhandler_errtrk_t*)cbdata;

    default_errhandler_id = errhandler_ref;
    errtrk->status = status;
    errtrk->active = false;
}

/**
 * Default errhandler callback
 */
void ompi_errhandler_callback(int status,
                              const opal_process_name_t *source,
                              opal_list_t *info, opal_list_t *results,
                              opal_pmix_notification_complete_fn_t cbfunc,
                              void *cbdata)
{
    /* tell the event chain engine to go no further - we
     * will handle this */
    if (NULL != cbfunc) {
        cbfunc(OMPI_ERR_HANDLERS_COMPLETE, NULL, NULL, NULL, cbdata);
    }
    /* our default action is to abort */
    ompi_mpi_abort(MPI_COMM_WORLD, status);
}

/**************************************************************************
 *
 * Static functions
 *
 **************************************************************************/

/**
 * Errhandler constructor
 */
static void ompi_errhandler_construct(ompi_errhandler_t *new_errhandler)
{
  int ret_val;

  /* assign entry in fortran <-> c translation array */

  ret_val = opal_pointer_array_add(&ompi_errhandler_f_to_c_table,
                                   new_errhandler);
  new_errhandler->eh_f_to_c_index = ret_val;

  new_errhandler->eh_lang = OMPI_ERRHANDLER_LANG_C;

  new_errhandler->eh_comm_fn      = NULL;
  new_errhandler->eh_win_fn       = NULL;
  new_errhandler->eh_file_fn      = NULL;
  new_errhandler->eh_fort_fn      = NULL;

  new_errhandler->eh_cxx_dispatch_fn = NULL;

  memset (new_errhandler->eh_name, 0, MPI_MAX_OBJECT_NAME);
}


/**
 * Errhandler destructor
 */
static void ompi_errhandler_destruct(ompi_errhandler_t *errhandler)
{
  /* reset the ompi_errhandler_f_to_c_table entry - make sure that the
     entry is in the table */

  if (NULL!= opal_pointer_array_get_item(&ompi_errhandler_f_to_c_table,
                                        errhandler->eh_f_to_c_index)) {
    opal_pointer_array_set_item(&ompi_errhandler_f_to_c_table,
                                errhandler->eh_f_to_c_index, NULL);
  }
}
