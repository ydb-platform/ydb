/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      University of Houston. All rights reserved.
 * Copyright (c) 2008-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include <stdlib.h>
#include <stdarg.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#include "opal/util/show_help.h"
#include "ompi/mca/rte/rte.h"
#include "ompi/errhandler/errhandler_predefined.h"
#include "ompi/errhandler/errcode.h"
#include "ompi/communicator/communicator.h"
#include "ompi/file/file.h"
#include "ompi/win/win.h"
#include "opal/util/printf.h"
#include "opal/util/output.h"

/*
 * Local functions
 */
static void backend_fatal(char *type, struct ompi_communicator_t *comm,
                          char *name, int *error_code, va_list arglist);
static void out(char *str, char *arg);


void ompi_mpi_errors_are_fatal_comm_handler(struct ompi_communicator_t **comm,
                                            int *error_code, ...)
{
  char *name;
  struct ompi_communicator_t *abort_comm;
  va_list arglist;

  va_start(arglist, error_code);

  if (NULL != comm) {
      name = (*comm)->c_name;
      abort_comm = *comm;
  } else {
      name = NULL;
      abort_comm = NULL;
  }
  backend_fatal("communicator", abort_comm, name, error_code, arglist);
  va_end(arglist);
}


void ompi_mpi_errors_are_fatal_file_handler(struct ompi_file_t **file,
                                            int *error_code, ...)
{
  char *name;
  struct ompi_communicator_t *abort_comm;
  va_list arglist;

  va_start(arglist, error_code);

  if (NULL != file) {
      name = (*file)->f_filename;
      abort_comm = (*file)->f_comm;
  } else {
      name = NULL;
      abort_comm = NULL;
  }
  backend_fatal("file", abort_comm, name, error_code, arglist);
  va_end(arglist);
}


void ompi_mpi_errors_are_fatal_win_handler(struct ompi_win_t **win,
                                           int *error_code, ...)
{
  char *name;
  struct ompi_communicator_t *abort_comm = NULL;
  va_list arglist;

  va_start(arglist, error_code);

  if (NULL != win) {
      name = (*win)->w_name;
  } else {
      name = NULL;
  }
  backend_fatal("win", abort_comm, name, error_code, arglist);
  va_end(arglist);
}

void ompi_mpi_errors_return_comm_handler(struct ompi_communicator_t **comm,
                                         int *error_code, ...)
{
    /* Don't need anything more -- just need this function to exist */
    /* Silence some compiler warnings */

    va_list arglist;
    va_start(arglist, error_code);
    va_end(arglist);
}


void ompi_mpi_errors_return_file_handler(struct ompi_file_t **file,
                                         int *error_code, ...)
{
    /* Don't need anything more -- just need this function to exist */
    /* Silence some compiler warnings */

    va_list arglist;
    va_start(arglist, error_code);
    va_end(arglist);
}


void ompi_mpi_errors_return_win_handler(struct ompi_win_t **win,
                                        int *error_code, ...)
{
    /* Don't need anything more -- just need this function to exist */
    /* Silence some compiler warnings */

    va_list arglist;
    va_start(arglist, error_code);
    va_end(arglist);
}


static void out(char *str, char *arg)
{
    if (ompi_rte_initialized &&
        ompi_mpi_state < OMPI_MPI_STATE_FINALIZE_PAST_COMM_SELF_DESTRUCT) {
        if (NULL != arg) {
            opal_output(0, str, arg);
        } else {
            opal_output(0, "%s", str);
        }
    } else {
        if (NULL != arg) {
            fprintf(stderr, str, arg);
        } else {
            fprintf(stderr, "%s", str);
        }
    }
}

/*
 * Use opal_show_help() to aggregate the error messages (i.e., show it
 * once rather than N times).
 *
 * Note that this function will only be invoked for errors during the
 * MPI application (i.e., after MPI_INIT and before MPI_FINALIZE).  So
 * there's no need to handle the pre-MPI_INIT and post-MPI_FINALIZE
 * errors here.
 */
static void backend_fatal_aggregate(char *type,
                                    struct ompi_communicator_t *comm,
                                    char *name, int *error_code,
                                    va_list arglist)
{
    char *arg = NULL, *prefix = NULL, *err_msg = NULL;
    const char* const unknown_error_code = "Error code: %d (no associated error message)";
    const char* const unknown_error = "Unknown error";
    const char* const unknown_prefix = "[?:?]";
    bool generated = false;

    // these do not own what they point to; they're
    // here to avoid repeating expressions such as
    // (NULL == foo) ? unknown_foo : foo
    const char* usable_prefix = unknown_prefix;
    const char* usable_err_msg = unknown_error;

    arg = va_arg(arglist, char*);
    va_end(arglist);

    if (asprintf(&prefix, "[%s:%05d]",
                 ompi_process_info.nodename,
                 (int) ompi_process_info.pid) == -1) {
        prefix = NULL;
        // non-fatal, we could still go on to give useful information here...
        opal_output(0, "%s", "Could not write node and PID to prefix");
        opal_output(0, "Node: %s", ompi_process_info.nodename);
        opal_output(0, "PID: %d", (int) ompi_process_info.pid);
    }

    if (NULL != error_code) {
        err_msg = ompi_mpi_errnum_get_string(*error_code);
        if (NULL == err_msg) {
            if (asprintf(&err_msg, unknown_error_code,
                         *error_code) == -1) {
                err_msg = NULL;
                opal_output(0, "%s", "Could not write to err_msg");
                opal_output(0, unknown_error_code, *error_code);
            } else {
                generated = true;
            }
        }
    }

    usable_prefix  = (NULL == prefix)  ? unknown_prefix : prefix;
    usable_err_msg = (NULL == err_msg) ? unknown_error  : err_msg;

    if (NULL != name) {
        opal_show_help("help-mpi-errors.txt",
                       "mpi_errors_are_fatal",
                       false,
                       usable_prefix,
                       (NULL == arg) ? "" : "in",
                       (NULL == arg) ? "" : arg,
                       usable_prefix,
                       OMPI_PROC_MY_NAME->jobid,
                       OMPI_PROC_MY_NAME->vpid,
                       usable_prefix,
                       type,
                       name,
                       usable_prefix,
                       usable_err_msg,
                       usable_prefix,
                       type,
                       usable_prefix);
    } else {
        opal_show_help("help-mpi-errors.txt",
                       "mpi_errors_are_fatal unknown handle",
                       false,
                       usable_prefix,
                       (NULL == arg) ? "" : "in",
                       (NULL == arg) ? "" : arg,
                       usable_prefix,
                       OMPI_PROC_MY_NAME->jobid,
                       OMPI_PROC_MY_NAME->vpid,
                       usable_prefix,
                       type,
                       usable_prefix,
                       usable_err_msg,
                       usable_prefix,
                       type,
                       usable_prefix);
    }

    free(prefix);
    if (generated) {
        free(err_msg);
    }
}

/*
 * Note that this function has to handle pre-MPI_INIT and
 * post-MPI_FINALIZE errors, which backend_fatal_aggregate() does not
 * have to handle.
 *
 * This function also intentionally does not call malloc(), just in
 * case we're being called due to some kind of stack/memory error --
 * we *might* be able to get a message out if we're not further
 * corrupting the stack by calling malloc()...
 */
static void backend_fatal_no_aggregate(char *type,
                                       struct ompi_communicator_t *comm,
                                       char *name, int *error_code,
                                       va_list arglist)
{
    char *arg;

    int32_t state = ompi_mpi_state;
    assert(state < OMPI_MPI_STATE_INIT_COMPLETED ||
           state >= OMPI_MPI_STATE_FINALIZE_PAST_COMM_SELF_DESTRUCT);

    fflush(stdout);
    fflush(stderr);

    arg = va_arg(arglist, char*);

    /* Per #2152, print out in plain english if something was invoked
       before MPI_INIT* or after MPI_FINALIZE */
    if (state < OMPI_MPI_STATE_INIT_STARTED) {
        if (NULL != arg) {
            out("*** The %s() function was called before MPI_INIT was invoked.\n"
                "*** This is disallowed by the MPI standard.\n", arg);
        } else {
            out("*** An MPI function was called before MPI_INIT was invoked.\n"
                "*** This is disallowed by the MPI standard.\n"
                "*** Unfortunately, no further information is available on *which* MPI\n"
                "*** function was invoked, sorry.  :-(\n", NULL);
        }
        out("*** Your MPI job will now abort.\n", NULL);
    } else if (state >= OMPI_MPI_STATE_FINALIZE_PAST_COMM_SELF_DESTRUCT) {
        if (NULL != arg) {
            out("*** The %s() function was called after MPI_FINALIZE was invoked.\n"
                "*** This is disallowed by the MPI standard.\n", arg);
        } else {
            out("*** An MPI function was called after MPI_FINALIZE was invoked.\n"
                "*** This is disallowed by the MPI standard.\n"
                "*** Unfortunately, no further information is available on *which* MPI\n"
                "*** function was invoked, sorry.  :-(\n", NULL);
        }
        out("*** Your MPI job will now abort.\n", NULL);
    }

    else {
        int len;
        char str[MPI_MAX_PROCESSOR_NAME * 2];

        /* THESE MESSAGES ARE COORDINATED WITH FIXED STRINGS IN
           help-mpi-errors.txt!  Do not change these messages without
           also changing help-mpi-errors.txt! */

        /* This is after MPI_INIT* and before MPI_FINALIZE, so print
           the error message normally */
        if (NULL != arg) {
            out("*** An error occurred in %s\n", arg);
        } else {
            out("*** An error occurred\n", NULL);
        }

        if (NULL != name) {
            /* Don't use asprintf() here because there may be stack /
               heap corruption by the time we're invoked, so just do
               it on the stack */
            str[0] = '\0';
            len = sizeof(str) - 1;
            strncat(str, type, len);

            len -= strlen(type);
            if (len > 0) {
                strncat(str, " ", len);

                --len;
                if (len > 0) {
                    strncat(str, name, len);
                }
            }
            out("*** on %s", str);
        } else if (NULL == name) {
            out("*** on a NULL %s\n", type);
        }

        if (NULL != error_code) {
            char *tmp = ompi_mpi_errnum_get_string(*error_code);
            if (NULL != tmp) {
                out("*** %s\n", tmp);
            } else {
                char intbuf[32];
                snprintf(intbuf, 32, "%d", *error_code);
                out("*** Error code: %d (no associated error message)\n", intbuf);
            }
        }
        /* out("*** MPI_ERRORS_ARE_FATAL: your MPI job will now abort\n", NULL); */
        out("*** MPI_ERRORS_ARE_FATAL (processes in this %s will now abort,\n", type);
        out("***    and potentially your MPI job)\n", NULL);

    }
    va_end(arglist);
}

static void backend_fatal(char *type, struct ompi_communicator_t *comm,
                          char *name, int *error_code,
                          va_list arglist)
{
    /* We only want aggregation while the rte is initialized */
    if (ompi_rte_initialized) {
        backend_fatal_aggregate(type, comm, name, error_code, arglist);
    } else {
        backend_fatal_no_aggregate(type, comm, name, error_code, arglist);
    }

    /* In most instances the communicator will be valid. If not, we are either early in
     * the initialization or we are dealing with a window. Thus, it is good enough to abort
     * on MPI_COMM_SELF, the error will propagate.
     */
    if (comm == NULL) {
        comm = &ompi_mpi_comm_self.comm;
    }

    if (NULL != error_code) {
        ompi_mpi_abort(comm, *error_code);
    } else {
        ompi_mpi_abort(comm, 1);
    }
}
