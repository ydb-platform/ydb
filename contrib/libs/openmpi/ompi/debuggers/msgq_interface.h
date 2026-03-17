/*
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/* $Header: /home/tv/src/mpi/src/mpi_interface.h,v 1.13 2003/03/12 14:03:42 jcownie Exp $ */
/* $Locker:  $ */

/**********************************************************************
 * Copyright (C) 2000-2004 by Etnus, LLC
 *
 * Permission is hereby granted to use, reproduce, prepare derivative
 * works, and to redistribute to others.
 *
 *				  DISCLAIMER
 *
 * Neither Etnus, nor any of their employees, makes any warranty
 * express or implied, or assumes any legal liability or
 * responsibility for the accuracy, completeness, or usefulness of any
 * information, apparatus, product, or process disclosed, or
 * represents that its use would not infringe privately owned rights.
 *
 * This code was written by
 * James Cownie: Etnus, LLC. <jcownie@etnus.com>
 **********************************************************************/

/**********************************************************************
 * Copyright (C) 1999 by Etnus, Inc.
 * Copyright (C) 1997-1998 Dolphin Interconnect Solutions Inc.
 *
 * Permission is hereby granted to use, reproduce, prepare derivative
 * works, and to redistribute to others.
 *
 *				  DISCLAIMER
 *
 * Neither Dolphin Interconnect Solutions, nor any of their employees,
 * makes any warranty express or implied, or assumes any legal
 * liability or responsibility for the accuracy, completeness, or
 * usefulness of any information, apparatus, product, or process
 * disclosed, or represents that its use would not infringe privately
 * owned rights.
 *
 * This code was written by
 * James Cownie: Dolphin Interconnect Solutions. <jcownie@dolphinics.com>
 * And now we're Etnus :-
 * James Cownie: Etnus LLC <jcownie@etnus.com>
 **********************************************************************/


/* Update log
 *
 * Aug  5 2002 CLG: Tiny fix to 64 bit taddr_t definition on sun.
 * Oct  6 2000 JHC: Add all of the MPI-2 relevant types and functions.
 *              This does need a compatibility number change to
 *              ensure new libraries can't get loaded into old debuggers.
 *              New debuggers can continue to use old libraries, though.
 *              New functions under control of FOR_MPI2
 * Oct  2 2000 JHC: Add the mqs_get_comm_group function to support
 *              partial acquisition.
 * Mar 21 2000 JHC: Lower the version compatibility again, but add
 *              a new DLL provided call to tell us the width it
 *              believes mqs_taddr_t is. If the DLL doesn't provide this
 *              call, we assume it's old and use the appropriate old width.
 *              This lets us gracefully handle widening the interface on AIX.
 * Mar 17 2000 JHC: Add FORCE_32BIT_MPI conditional compilation flag.
 * Mar  3 2000 JHC: Widen the tword_t and taddr_t on AIX, now that IBM
 *              has 64 bit machines. Increment the version compatibility
 *              number on AIX (only) since this is an incompatible change in
 *              the interface.
 * Oct  1 1998 JHC: Change MQS_INVALID_PROCESS to -1, TV would never generate
 *              the old value anyway.
 * May 26 1998 JHC: Change interface compatibility, add extra strings in the
 *              mqs_pending_operation.
 * Apr 23 1998 JHC: Make mqs_tword_t and mqs_taddr_t into 64 bit entities on
 *              SGI. Expand the comment above their definition.
 * Mar  9 1998 JHC: Added mqs_sizeof_ft. Of course we always needed this !
 * Nov  6 1997 JHC: Worked over somewhat to keep John happy :-)
 * Oct 27 1997 James Cownie <jcownie@dolphinics.com>: Created.
 */

/***********************************************************************
 * This header file defines the interface between a debugger and a
 * dynamically loaded library use to implement access to MPI message
 * queues.
 *
 * The interface is specified at the C level, to avoid C++ compiler issues.
 *
 * The interface allows code in the DLL to
 * 1) find named types from the debugger's type system and look up fields in them
 * 2) find the address of named external variables
 * 3) access objects at absolute addresses in the target process.
 * 4) convert objects from target format to host format.
 *
 * A number of different objects are passed backwards and forwards
 * between the debugger and the DLL :-
 *
 * executable images
 * processes
 * communicators
 *
 * Many of these are opaque to the DLL, in such cases they will be
 * defined in the interface as pointers to opaque structures, since
 * this provides type checking while maintaining information hiding.
 *
 * All named entities in here start with the prefix "mqs_" (for
 * Message Queue Support), all the debugger callbacks are made via
 * callback tables, so the real (linkage) names of the functions are
 * not visible to the DLL.
 */

#ifndef _MPI_INTERFACE_INCLUDED
#define _MPI_INTERFACE_INCLUDED

#include "ompi_config.h"
#include <stdio.h>				/* For FILENAME_MAX */

/* No MPI2 support yet */
#define FOR_MPI2 0

BEGIN_C_DECLS

/***********************************************************************
 * Version of the interface this header represents
 */
enum
{
#if (FOR_MPI2)
  MQS_INTERFACE_COMPATIBILITY = 3		/* Has MPI-2 functions */
#else
  MQS_INTERFACE_COMPATIBILITY = 2
#endif
};

/***********************************************************************
 * type definitions.
 */

/* Opaque types are used here to provide a degree of type checking
 * through the prototypes of the interface functions.
 *
 * Only pointers to these types are ever passed across the interface.
 * Internally to the debugger, or the DLL you should immediately cast
 * these pointers to pointers to the concrete types that you actually
 * want to use.
 *
 * (An alternative would be to use void * for all of these arguments,
 * but that would remove a useful degree of type checking, assuming
 * that you are thinking while typing the casts :-)
 */

/* Types which will be (cast to) concrete types in the DLL */
typedef struct _mqs_image_info   mqs_image_info;
typedef struct _mqs_process_info mqs_process_info;
#if (FOR_MPI2)
typedef struct _mqs_job_info     mqs_job_info;
#endif

/* Types which will be (cast to) concrete types in the debugger */
typedef struct mqs_image_    mqs_image;
#if (FOR_MPI2)
typedef struct mqs_job_      mqs_job;
#endif
typedef struct mqs_process_  mqs_process;
typedef struct mqs_type_     mqs_type;

/* *** BEWARE ***
 * On machines with two pointer lengths (such as SGI -n32, -64 compilations,
 * and AIX and Solaris soon), it is quite likely that TotalView and the DLL
 * will have been compiled with the 32 bit model, *but* will need to debug
 * code compiled with the 64 bit one. The mqs_taddr_t and mqs_tword_t need
 * to be a type which even when compiled with the 32 bit model compiler can
 * hold the longer (64 bit) pointer.
 *
 * You may need to add your host to this #if, if you have a machine with
 * two compilation models.
 * *** END BEWARE ***
 *
 * It would be better not to have this target dependence in the
 * interface, but it is unreasonable to require a 64 bit interface on
 * 32 bit machines, and we need the 64 bit interface in some places.
 */

#if !defined (FORCE_32BIT_MPI) && (defined (__sgi) || defined (__hpux) || defined (_AIX) || defined(__sun))
typedef unsigned long long mqs_taddr_t;		/* Something long enough for a target address */
typedef long long          mqs_tword_t;		/* Something long enough for a word    */
#else
typedef unsigned long mqs_taddr_t;		/* Something long enough for a target address */
typedef long          mqs_tword_t;		/* Something long enough for a word    */
#endif

/***********************************************************************
 * Defined structures which form part of the interface.
 */

/* A structure for (target) architectural information */
typedef struct
{
    int short_size;	/* sizeof (short) */
    int int_size;	/* sizeof (int)   */
    int long_size;	/* sizeof (long)  */
    int long_long_size;	/* sizeof (long long) */
    int pointer_size;	/* sizeof (void *) */
    int bool_size;      /* sizeof(bool) */
    int size_t_size;    /* sizeof(size_t) */
} mqs_target_type_sizes;

/* Result codes.
 * mqs_ok is returned for success.
 * Anything else implies a failure of some sort.
 *
 * Most of the functions actually return one of these, however to avoid
 * any potential issues with different compilers implementing enums as
 * different sized objects, we actually use int as the result type.
 *
 * Note that both the DLL and the debugger will use values starting at
 * mqs_first_user_code, since you always know which side you were calling,
 * this shouldn't be a problem.
 *
 * See below for functions to convert codes to strings.
 */
enum {
  mqs_ok = 0,
  mqs_no_information,
  mqs_end_of_list,
  mqs_first_user_code = 100			/* Allow for more pre-defines */
};

#if (FOR_MPI2)
/* For handling attachment to new processes in MPI-2 we need to know
 * where they are.
 */
typedef struct mqs_process_location {
  long pid;
  char image_name [FILENAME_MAX];
  char host_name  [64];
} mqs_process_location;
#endif

/* Languages */
typedef enum {
  mqs_lang_c     = 'c',
  mqs_lang_cplus = 'C',
  mqs_lang_f77   = 'f',
  mqs_lang_f90   = 'F'
} mqs_lang_code;

/* Which queue are we interested in ? */
typedef enum
{
  mqs_pending_sends,
  mqs_pending_receives,
  mqs_unexpected_messages
} mqs_op_class;

/* A value to represent an invalid process index. */
enum
{
  MQS_INVALID_PROCESS = -1
};

enum mqs_status
{
  mqs_st_pending, mqs_st_matched, mqs_st_complete
};

/* Additional error codes and error string conversion. */
enum {
    err_silent_failure  = mqs_first_user_code,

    err_no_current_communicator,
    err_bad_request,
    err_no_store,

    err_failed_qhdr,
    err_unexpected,
    err_posted,

    err_failed_queue,
    err_first,

    err_context_id,
    err_tag,
    err_tagmask,
    err_lsrc,
    err_srcmask,
    err_next,
    err_ptr,

    err_missing_type,
    err_missing_symbol,

    err_db_shandle,
    err_db_comm,
    err_db_target,
    err_db_tag,
    err_db_data,
    err_db_byte_length,
    err_db_next,

    err_failed_rhandle,
    err_is_complete,
    err_buf,
    err_len,
    err_s,

    err_failed_status,
    err_count,
    err_MPI_SOURCE,
    err_MPI_TAG,

    err_failed_commlist,
    err_sequence_number,
    err_comm_first,

    err_failed_communicator,
    err_lrank_to_grank,
    err_send_context,
    err_recv_context,
    err_comm_next,
    err_comm_name,

    err_all_communicators,
    err_mpid_sends,
    err_mpid_recvs,
    err_group_corrupt
};

/* A structure to represent a communicator */
typedef struct
{
  mqs_taddr_t unique_id;			/* A unique tag for the communicator */
  mqs_tword_t local_rank;			/* The rank of this process Comm_rank */
  mqs_tword_t size;				/* Comm_size  */
  char    name[64];				/* the name if it has one */
} mqs_communicator;

/*
 * We currently assume that all messages are flattened into contiguous buffers.
 * This is potentially incorrect, but let's leave that complication for a while.
 */

typedef struct
{
  /* Fields for all messages */
  int status;					/* Status of the message (really enum mqs_status) */
  mqs_tword_t desired_local_rank;		/* Rank of target/source -1 for ANY */
  mqs_tword_t desired_global_rank;		/* As above but in COMM_WORLD  */
  int tag_wild;					/* Flag for wildcard receive  */
  mqs_tword_t desired_tag;			/* Only if !tag_wild */
  mqs_tword_t desired_length;			/* Length of the message buffer */
  int system_buffer;				/* Is it a system or user buffer ? */
  mqs_taddr_t buffer;				/* Where data is */

  /* Fields valid if status >= matched or it's a send */
  mqs_tword_t actual_local_rank;		/* Actual local rank */
  mqs_tword_t actual_global_rank;		/* As above but in COMM_WORLD */
  mqs_tword_t actual_tag;
  mqs_tword_t actual_length;

  /* Additional strings which can be filled in if the DLL has more
   * info.  (Uninterpreted by the debugger, simply displayed to the
   * user).
   *
   * Can be used to give the name of the function causing this request,
   * for instance.
   *
   * Up to five lines each of 64 characters.
   */
  char extra_text[5][64];
} mqs_pending_operation;

/***********************************************************************
 * Callbacks from the DLL into the debugger.
 ***********************************************************************
 * These are all made via a table of function pointers.
 */

/* Hang information on the image */
typedef void (*mqs_put_image_info_ft) (mqs_image *, mqs_image_info *);
/* Get it back */
typedef mqs_image_info * (*mqs_get_image_info_ft) (mqs_image *);

#if (FOR_MPI2)
/* Given a job and an index return the process object */
typedef mqs_process * (*mqs_get_process_ft) (mqs_job *, int);

/* Hang information on the job */
typedef void (*mqs_put_job_info_ft) (mqs_job *, mqs_job_info *);
/* Get it back */
typedef mqs_job_info * (*mqs_get_job_info_ft) (mqs_job *);
#endif

/* Given a process return the image it is an instance of */
typedef mqs_image * (*mqs_get_image_ft) (mqs_process *);

/* Given a process return its rank in comm_world */
typedef int (*mqs_get_global_rank_ft) (mqs_process *);

/* Given an image look up the specified function */
typedef int (*mqs_find_function_ft) (mqs_image *, char *, mqs_lang_code, mqs_taddr_t * );

/* Given an image look up the specified symbol */
typedef int (*mqs_find_symbol_ft) (mqs_image *, char *, mqs_taddr_t * );

/* Hang information on the process */
typedef void (*mqs_put_process_info_ft) (mqs_process *, mqs_process_info *);
/* Get it back */
typedef mqs_process_info * (*mqs_get_process_info_ft) (mqs_process *);

#if (FOR_MPI2)
/* Given a process, return the job it belongs to */
typedef mqs_job * (*mqs_get_process_job_ft) (mqs_process *);
/* Given a process, return its identity (index in the job's universe of processes) */
typedef int       (*mqs_get_process_identity_ft) (mqs_job *);
#endif

/* Allocate store */
typedef void * (*mqs_malloc_ft) (size_t);
/* Free it again */
typedef void   (*mqs_free_ft)   (void *);

/***********************************************************************
 * Type access functions
 */

/* Given an executable image look up a named type in it.
 * Returns a type handle, or the null pointer if the type could not be
 * found.  Since the debugger may load debug information lazily, the
 * MPI run time library should ensure that the type definitions
 * required occur in a file whose debug information will already have
 * been loaded, for instance by placing them in the same file as the
 * startup breakpoint function.
 */
typedef mqs_type * (*mqs_find_type_ft)(mqs_image *, char *, mqs_lang_code);

/* Given the handle for a type (assumed to be a structure) return the
 * byte offset of the named field. If the field cannot be found
 * the result will be -1.
 */
typedef int (*mqs_field_offset_ft) (mqs_type *, char *);

/* Given the handle for a type return the size of the type in bytes.
 * (Just like sizeof ())
 */
typedef int (*mqs_sizeof_ft) (mqs_type *);

/* Fill in the sizes of target types for this process */
typedef void (*mqs_get_type_sizes_ft) (mqs_process *, mqs_target_type_sizes *);

/***********************************************************************
 * Target store access functions
 */

/* Fetch data from the process into a buffer into a specified buffer.
 * N.B.
 * The data is the same as that in the target process when accessed
 * as a byte array. You *must* use mqs_target_to_host to do any
 * necessary byte flipping if you want to look at it at larger
 * granularity.
 */
typedef int (*mqs_fetch_data_ft) (mqs_process *, mqs_taddr_t, int, void *);

/* Convert data into host format */
typedef void (*mqs_target_to_host_ft) (mqs_process *, const void *, void *, int);

/***********************************************************************
 * Miscellaneous functions.
 */
/* Print a message (intended for debugging use *ONLY*). */
typedef void (*mqs_dprints_ft) (const char *);
/* Convert an error code from the debugger into an error message */
typedef char * (*mqs_errorstring_ft) (int);

/***********************************************************************
 * Call back tables
 */
typedef struct mqs_basic_callbacks
{
  mqs_malloc_ft           mqs_malloc_fp;
  mqs_free_ft             mqs_free_fp;
  mqs_dprints_ft          mqs_dprints_fp;
  mqs_errorstring_ft      mqs_errorstring_fp;
  mqs_put_image_info_ft   mqs_put_image_info_fp;
  mqs_get_image_info_ft	  mqs_get_image_info_fp;
  mqs_put_process_info_ft mqs_put_process_info_fp;
  mqs_get_process_info_ft mqs_get_process_info_fp;
#if (FOR_MPI2)
  mqs_put_job_info_ft     mqs_put_job_info_fp;
  mqs_get_job_info_ft     mqs_get_job_info_fp;
#endif
} mqs_basic_callbacks;

#if (FOR_MPI2)
typedef struct mqs_job_callbacks {
  mqs_get_process_ft	mqs_get_process_fp;
} mqs_job_callbacks;
#endif

typedef struct mqs_image_callbacks
{
  mqs_get_type_sizes_ft	  mqs_get_type_sizes_fp;
  mqs_find_function_ft	  mqs_find_function_fp;
  mqs_find_symbol_ft      mqs_find_symbol_fp;
  mqs_find_type_ft        mqs_find_type_fp;
  mqs_field_offset_ft	  mqs_field_offset_fp;
  mqs_sizeof_ft	          mqs_sizeof_fp;
} mqs_image_callbacks;

typedef struct mqs_process_callbacks
{
  mqs_get_global_rank_ft       mqs_get_global_rank_fp;
  mqs_get_image_ft             mqs_get_image_fp;
  mqs_fetch_data_ft            mqs_fetch_data_fp;
  mqs_target_to_host_ft        mqs_target_to_host_fp;
#if (FOR_MPI2)
  mqs_get_process_job_ft       mqs_get_process_job_fp;
  mqs_get_process_identity_ft  mqs_get_process_identity_fp;
#endif
} mqs_process_callbacks;

/***********************************************************************
 * Calls from the debugger into the DLL.
 ***********************************************************************/

/* Provide the library with the pointers to the the debugger functions
 * it needs The DLL need only save the pointer, the debugger promises
 * to maintain the table of functions valid for as long as
 * needed. (The table remains the property of the debugger, and should
 * not be messed with, or deallocated by the DLL). This applies to
 * all of the callback tables.
 */
OMPI_DECLSPEC extern void mqs_setup_basic_callbacks (const mqs_basic_callbacks *);

/* Version handling */
OMPI_DECLSPEC extern char *mqs_version_string (void);
OMPI_DECLSPEC extern int   mqs_version_compatibility(void);
/* This gives the width which has been compiled into the DLL, it is
 * _not_ the width of a specific process, which could be smaller than
 * this.
 */
OMPI_DECLSPEC extern int   mqs_dll_taddr_width(void);

/* Provide a text string for an error value */
OMPI_DECLSPEC extern char * mqs_dll_error_string (int);

/***********************************************************************
 * Calls related to an executable image.
 */

/* Setup debug information for a specific image, this must save
 * the callbacks (probably in the mqs_image_info), and use those
 * functions for accessing this image.
 *
 * The DLL should use the mqs_put_image_info and mqs_get_image_info functions
 * to associate whatever information it wants to keep with the image.
 * (For instance all of the type offsets it needs could be kept here).
 * the debugger will call mqs_destroy_image_info when it no longer wants to
 * keep information about the given executable.
 *
 * This will be called once for each executable image in the parallel
 * program.
 */
OMPI_DECLSPEC extern int mqs_setup_image (mqs_image *, const mqs_image_callbacks *);

/* Does this image have the necessary symbols to allow access to the message
 * queues ?
 *
 * This function will be called once for each image, and the information
 * cached inside the debugger.
 *
 * Returns an error enumeration to show whether the image has queues
 * or not, and an error string to be used in a pop-up complaint to the
 * user, as if in printf (error_string, name_of_image);
 *
 * The pop-up display is independent of the result. (So you can silently
 * disable things, or loudly enable them).
 */

OMPI_DECLSPEC extern int mqs_image_has_queues (mqs_image *, char **);

/* This will be called by the debugger to let you tidy up whatever is
 * required when the mqs_image_info is no longer needed.
 */
OMPI_DECLSPEC extern void mqs_destroy_image_info (mqs_image_info *);

#if (FOR_MPI2)
/***********************************************************************
 * Calls related to a specific job, which owns a universe of processes.
 */
extern int mqs_setup_job (mqs_job *, const mqs_job_callbacks *);
extern int mqs_destroy_job_info (mqs_job_info *);
#endif

/***********************************************************************
 * Calls related to a specific process. These will only be called if the
 * image which this is an instance of passes the has_message_queues tests.
 *
 * If you can't tell whether the process will have valid message queues
 * just by looking at the image, then you should return mqs_ok from
 * mqs_image_has_queues and let mqs_process_has_queues handle it.
 */

/* Set up whatever process specific information we need.
 * For instance addresses of global variables should be handled here,
 * rather than in the image information if anything is a dynamic library
 * which could end up mapped differently in different processes.
 */
OMPI_DECLSPEC extern int mqs_setup_process (mqs_process *, const mqs_process_callbacks *);
OMPI_DECLSPEC extern void mqs_destroy_process_info (mqs_process_info *);

/* Like the mqs_has_message_queues function, but will only be called
 * if the image claims to have message queues. This lets you actually
 * delve inside the process to look at variables before deciding if
 * the process really can support message queue extraction.
 */
OMPI_DECLSPEC extern int mqs_process_has_queues (mqs_process *, char **);

/***********************************************************************
 * The functions which actually extract the information we need !
 *
 * The model here is that the debugger calls down to the library to initialise
 * an iteration over a specific class of things, and then keeps calling
 * the "next" function until it returns mqs_false.
 *
 * For communicators we separate stepping from extracting information,
 * because we want to use the state of the communicator iterator to qualify
 * the selections of the operation iterator.
 *
 * Whenever mqs_true is returned the description has been updated,
 * mqs_false means there is no more information to return, and
 * therefore the description contains no useful information.
 *
 * We will only have one of each type of iteration running at once, so
 * the library should save the iteration state in the
 * mqs_process_info.
 */


/* Check that the DLL's model of the communicators in the process is
 * up to date, ideally by checking the sequence number.
 */
OMPI_DECLSPEC extern int mqs_update_communicator_list (mqs_process *);

/* Prepare to iterate over all of the communicators in the process. */
OMPI_DECLSPEC extern int mqs_setup_communicator_iterator (mqs_process *);

/* Extract information about the current communicator */
OMPI_DECLSPEC extern int mqs_get_communicator (mqs_process *, mqs_communicator *);

/* Extract the group from the current communicator.
 * The debugger already knows comm_size, so can allocate a
 * suitably sized array for the result. The result is the
 * rank in COMM_WORLD of the index'th element in the current
 * communicator.
 */
OMPI_DECLSPEC extern int mqs_get_comm_group (mqs_process *, int *);

/* Move on to the next communicator in this process. */
OMPI_DECLSPEC extern int mqs_next_communicator (mqs_process *);

/* Prepare to iterate over the pending operations in the currently
 * active communicator in this process.
 *
 * The int is *really* mqs_op_class
 */
OMPI_DECLSPEC extern int mqs_setup_operation_iterator (mqs_process *, int);

/* Return information about the next appropriate pending operation in
 * the current communicator, mqs_false when we've seen them all.
 */
OMPI_DECLSPEC extern int mqs_next_operation (mqs_process *, mqs_pending_operation *);

#if (FOR_MPI2)
/* Information about newly created (or connected to) processes.
 * This is how we pick up processes created with MPI_Spawn (and friends),
 * or attached to with MPI_Comm_connect or MPI_Comm_join.
 */
extern int mqs_setup_new_process_iterator (mqs_process *);
extern int mqs_next_new_process (mqs_process *, mqs_process_location *);

/* Once the debugger has attached to a newly created process it will
 * set it up in the normal way, and then set its identity.
 */
extern int mqs_set_process_identity (mqs_process *, int);
#endif

END_C_DECLS

#endif /* defined (_MPI_INTERFACE_INCLUDED) */
