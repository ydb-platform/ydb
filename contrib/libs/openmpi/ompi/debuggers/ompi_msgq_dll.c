/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2016 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2008-2009 Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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
 * Jul 12 2001 FNW: Add a meaningful ID to the communicator name, and switch
 *                  to using the recv_context as the unique_id field.
 * Mar  6 2001 JHC: Add mqs_get_comm_group to allow a debugger to acquire
 *                  processes less eagerly.
 * Dec 13 2000 JHC: totalview/2514: Modify image_has_queues to return
 *                  a silent FALSE if none of the expected data is
 *                  present. This way you won't get complaints when
 *                  you try this on non MPICH processes.
 * Sep  8 2000 JVD: #include <string.h> to silence Linux Alpha compiler warnings.
 * Mar 21 2000 JHC: Add the new entrypoint mqs_dll_taddr_width
 * Nov 26 1998 JHC: Fix the problem that we weren't handling
 *                  MPIR_Ignore_queues properly.
 * Oct 22 1998 JHC: Fix a zero allocation problem
 * Aug 19 1998 JHC: Fix some problems in our use of target_to_host on
 *                  big endian machines.
 * May 28 1998 JHC: Use the extra information we can return to say
 *                  explicitly that sends are only showing non-blocking ops
 * May 19 1998 JHC: Changed the names of the structs and added casts
 *                  where needed to reflect the change to the way we handle
 *                  type safety across the interface.
 * Oct 27 1997 JHC: Created by exploding db_message_state_mpich.cxx
 */

/*
   The following was added by William Gropp to improve the portability
   to systems with non-ANSI C compilers
 */

#include "ompi_config.h"

#ifdef HAVE_NO_C_CONST
#define const
#endif
#include <string.h>
#include <stdlib.h>

/* Notice to developers!!!!
 * The following include files with _dbg.h suffixes contains definitions
 * that are shared between the debuggger plugins and the OMPI code base.
 * This is done instead of including the non-_dbg suffixed files because
 * of the different way compilers may handle extern definitions. The
 * particular case that is causing problems is when there is an extern
 * variable or function that is accessed in a static inline function.
 * For example, here is the code we often see in a header file.
 *
 * extern int request_complete;
 * static inline check_request(void) {
 *    request_complete = 1;
 * }
 *
 * If this code exists in a header file and gets included in a source
 * file, then some compilers expect to have request_complete defined
 * somewhere even if request_complete is never referenced and
 * check_request is never called. Other compilers do not need them defined
 * if they are never referenced in the source file.
 *
 * In the case of extern functions we something like the following:
 *
 * extern int foo();
 * static inline bar(void) {
 *     foo();
 * }
 *
 * If this code exists it actually compiles fine however an undefined symbol
 * is kept for foo() and in the case of some tools that load in plugins with
 * RTLD_NOW this undefined symbol causes the dlopen to fail since we do not
 * have (nor really need) the supporting library containing foo().
 *
 * Therefore, to handle cases like the above with compilers that require the
 * symbols (like Sun Studio) instead of  pulling in all of OMPI into the
 * plugins or defining dummy symbols here we separate the definitions used by
 * both sets of code into the _dbg.h files.
 *
 * This means if one needs to add another definition that the plugins must see
 * one should either move the definition into one of the existing _dbg.h file or
 * create a new _dbg.h file.
 */
#include "ompi/group/group_dbg.h"
#include "ompi/request/request_dbg.h"
#include "ompi/mca/pml/base/pml_base_request_dbg.h"
#include "mpi.h" /* needed for MPI_ANY_TAG */

#include "msgq_interface.h"
#include "ompi_msgq_dll_defs.h"

/*
   End of inclusion
 */


/* Essential macros for C */
#ifndef NULL
#define NULL ((void *)0)
#endif
#ifndef TRUE
#define TRUE (0==0)
#endif
#ifndef FALSE
#define FALSE (0==1)
#endif

#ifdef OLD_STYLE_CPP_CONCAT
#define concat(a,b) a/**/b
#define stringize(a) "a"
#else
#define concat(a,b) a##b
#define stringize(a) #a
#endif

#define OPAL_ALIGN(x,a,t) (((x)+((t)(a)-1)) & ~(((t)(a)-1)))

/**
 * The internal debugging interface.
 */
#define VERBOSE_GENERAL  0x00000001
#define VERBOSE_GROUP    0x00000002
#define VERBOSE_COMM     0x00000004
#define VERBOSE_LISTS    0x00000008
#define VERBOSE_REQ      0x00000010
#define VERBOSE_REQ_DUMP 0x00000020

#define VERBOSE 0x00000000

#if VERBOSE
#define DEBUG(LEVEL, WHAT) if(LEVEL & VERBOSE) { printf WHAT; }
#else
#define DEBUG(LEVEL,WHAT)
#endif  /* VERBOSE */

/**********************************************************************/
/* Set up the basic callbacks into the debugger */

void mqs_setup_basic_callbacks (const mqs_basic_callbacks * cb)
{
    mqs_basic_entrypoints = cb;
} /* mqs_setup_callbacks */


/**********************************************************************/
/* Version handling functions.
 * This one should never be changed.
 */
int mqs_version_compatibility (void)
{
    return MQS_INTERFACE_COMPATIBILITY;
} /* mqs_version_compatibility */

static char mqs_version_str[OMPI_MAX_VER_SIZE];

/* This one can say what you like */
char *mqs_version_string (void)
{
    int offset;
    offset = snprintf(mqs_version_str, OMPI_MAX_VER_SIZE-1,  
                      "Open MPI message queue support for parallel debuggers ");
    ompi_get_lib_version(mqs_version_str+offset, OMPI_MAX_VER_SIZE-offset);
    return mqs_version_str;
} /* mqs_version_string */

/* So the debugger can tell what interface width the library was compiled with */
int mqs_dll_taddr_width (void)
{
    return sizeof (mqs_taddr_t);
} /* mqs_dll_taddr_width */

/**********************************************************************/
/* Functions to handle translation groups.
 * We have a list of these on the process info, so that we can
 * share the group between multiple communicators.
 */
/**********************************************************************/
/* Translate a process number */
static int translate (group_t *this, int index)
{
    if (index == MQS_INVALID_PROCESS ||
        ((unsigned int)index) >= ((unsigned int) this->entries))
        return MQS_INVALID_PROCESS;
    return this->local_to_global[index];
} /* translate */

/**********************************************************************/
/* Search the group list for this group, if not found create it.
 */
static group_t * find_or_create_group( mqs_process *proc,
                                       mqs_taddr_t group_base )
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;
    mqs_image * image        = mqs_get_image (proc);
    mpi_image_info *i_info   = (mpi_image_info *)mqs_get_image_info (image);
    communicator_t *comm     = extra->communicator_list;
    int *tr;
    char *trbuffer;
    int i, np, is_dense;
    group_t *group;
    mqs_taddr_t value;
    mqs_taddr_t tablep;

    np = ompi_fetch_int( proc,
                         group_base + i_info->ompi_group_t.offset.grp_proc_count,
                         p_info );
    if( np < 0 ) {
        DEBUG(VERBOSE_COMM, ("Get a size for the communicator = %d\n", np));
        return NULL;  /* Makes no sense ! */
    }
    is_dense =
        ompi_fetch_int( proc,
                        group_base + i_info->ompi_group_t.offset.grp_flags,
                        p_info );
    is_dense = (0 != (is_dense & OMPI_GROUP_DENSE));

    /* Iterate over each communicator seeing if we can find this group */
    for (;comm; comm = comm->next) {
        group = comm->group;
        if( group && (group->group_base == group_base) ) {
            group->ref_count++;			/* Someone else is interested */
            DEBUG(VERBOSE_GROUP, ("Increase refcount for group 0x%p to %d\n",
                                  (void*)group, group->ref_count) );
            return group;
        }
    }

    /* Hmm, couldn't find one, so fetch it */
    group = (group_t *)mqs_malloc (sizeof (group_t));
    tr = (int *)mqs_malloc (np*sizeof(int));
    trbuffer = (char *)mqs_malloc (np*sizeof(mqs_taddr_t));
    group->local_to_global = tr;
    group->group_base = group_base;
    DEBUG(VERBOSE_GROUP, ("Create a new group 0x%p with %d members\n",
                          (void*)group, np) );

    tablep = ompi_fetch_pointer( proc,
                                 group_base + i_info->ompi_group_t.offset.grp_proc_pointers,
                                 p_info);

    if( (0 != np) &&
        (mqs_ok != mqs_fetch_data(proc, tablep, np * p_info->sizes.pointer_size,
                                  trbuffer)) ) {
        DEBUG(VERBOSE_GROUP,("Failed to read the proc data. Destroy group %p\n",
                             (void*)group));
        mqs_free (group);
        mqs_free (tr);
        mqs_free (trbuffer);
        return NULL;
    }

    /**
     * Now convert the process representation into the local representation.
     * We will endup with an array of Open MPI internal pointers to proc
     * structure. By comparing this pointers to the MPI_COMM_WORLD group
     * we can figure out the global rank in the MPI_COMM_WORLD of the process.
     *
     * Note that this only works for dense groups.  Someday we may
     * support more than dense groups, but that's what we've got for
     * today.
     */
     if( NULL == extra->world_proc_array ) {
         extra->world_proc_array = mqs_malloc( np * sizeof(mqs_taddr_t) );
         for( i = 0; i < np; i++ ) {
             mqs_target_to_host( proc, trbuffer + p_info->sizes.pointer_size*i,
                                 &value, p_info->sizes.pointer_size );
             extra->world_proc_array[i] = value;
             group->local_to_global[i] = is_dense ? i : -1;
         }
         extra->world_proc_array_entries = np;
     } else {
         int j;

         for( i = 0; i < np; i++ ) {
             mqs_target_to_host( proc, trbuffer + p_info->sizes.pointer_size*i,
                                 &value, p_info->sizes.pointer_size );
             if (is_dense) {
                 /* get the global rank this MPI process */
                 for( j = 0; j < extra->world_proc_array_entries; j++ ) {
                     if( value == extra->world_proc_array[j] ) {
                         group->local_to_global[i] = j;
                         break;
                     }
                 }
             } else {
                 group->local_to_global[i] = -1;
             }
         }
     }

    mqs_free(trbuffer);

    group->entries = np;
    group->ref_count = 1;
    return group;
} /* find_or_create_group */

/***********************************************************************/
static void group_decref (group_t * group)
{
    DEBUG(VERBOSE_GROUP, ("Decrement reference count for group %p to %d\n", (void*)group,
                          (group->ref_count - 1)));
    if (--(group->ref_count) == 0) {
        mqs_free (group->local_to_global);
        DEBUG(VERBOSE_GROUP, ("Destroy group %p\n", (void*)group));
        mqs_free (group);
    }
} /* group_decref */

/***********************************************************************
 * Perform basic setup for the image, we just allocate and clear
 * our info.
 */
int mqs_setup_image (mqs_image *image, const mqs_image_callbacks *icb)
{
    mpi_image_info *i_info = (mpi_image_info *)mqs_malloc (sizeof (mpi_image_info));

    if (!i_info)
        return err_no_store;

    memset ((void *)i_info, 0, sizeof (mpi_image_info));
    i_info->image_callbacks = icb;		/* Before we do *ANYTHING* */
    i_info->extra = NULL;

    mqs_put_image_info (image, (mqs_image_info *)i_info);

    return mqs_ok;
} /* mqs_setup_image */


/***********************************************************************
 * Check for all the information we require to access the Open MPI message queues.
 * Stash it into our structure on the image if we're successful.
 */

int mqs_image_has_queues (mqs_image *image, char **message)
{
    mpi_image_info * i_info = (mpi_image_info *)mqs_get_image_info (image);

    i_info->extra = NULL;

    /* Default failure message ! */
    *message = "The symbols and types in the Open MPI library used by the debugger\n"
        "to extract the message queues are not as expected in\n"
        "the image '%s'\n"
        "No message queue display is possible.\n"
        "This is probably an Open MPI version or configuration problem.";

    /* Force in the file containing our breakpoint function, to ensure
     * that types have been read from there before we try to look them
     * up.
     */
    mqs_find_function (image, "ompi_debugger_setup_dlls", mqs_lang_c, NULL);

    /* Are we supposed to ignore this ? (e.g. it's really an HPF
     * runtime using the Open MPI process acquisition, but not wanting
     * queue display)
     */
    if (mqs_find_symbol (image, "MPIR_Ignore_queues", NULL) == mqs_ok) {
        *message = NULL;				/* Fail silently */
        return err_silent_failure;
    }

    /* Fill in the type information */
    return ompi_fill_in_type_info(image, message);
} /* mqs_image_has_queues */

/***********************************************************************
 * Setup information needed for a specific process.
 * TV assumes that this will hang something onto the process,
 * if nothing is attached to it, then TV will believe that this process
 * has no message queue information.
 */
int mqs_setup_process (mqs_process *process, const mqs_process_callbacks *pcb)
{
    /* Extract the addresses of the global variables we need and save them away */
    mpi_process_info *p_info = (mpi_process_info *)mqs_malloc (sizeof (mpi_process_info));

    if (p_info) {
        mqs_image        *image;
        mpi_image_info   *i_info;
        mpi_process_info_extra *extra;

        p_info->process_callbacks = pcb;

        p_info->extra = mqs_malloc(sizeof(mpi_process_info_extra));
        extra = (mpi_process_info_extra*) p_info->extra;

        /* Now we can get the rest of the info ! */
        image  = mqs_get_image (process);
        i_info   = (mpi_image_info *)mqs_get_image_info (image);

        /* We have no communicators yet */
        extra->communicator_list = NULL;
        /* Enforce the generation of the communicators list */
        extra->comm_lowest_free  = 0;
        extra->comm_number_free  = 0;
        /* By default we don't show our internal requests*/
        extra->show_internal_requests = 0;

        extra->world_proc_array_entries = 0;
        extra->world_proc_array = NULL;

        mqs_get_type_sizes (process, &p_info->sizes);
        /*
         * Before going any further make sure we know exactly how the
         * Open MPI library was compiled. This means we know the size
         * of each of the basic types as stored in the
         * MPIR_debug_typedefs_sizeof array.
         */
        {
            mqs_taddr_t typedefs_sizeof;

            if (mqs_find_symbol (image, "MPIR_debug_typedefs_sizeof", &typedefs_sizeof) != mqs_ok) {
                return err_no_store;
            }
            p_info->sizes.short_size = ompi_fetch_int( process, /* sizeof (short) */
                                                       typedefs_sizeof,
                                                       p_info );
            typedefs_sizeof += p_info->sizes.int_size;
            p_info->sizes.int_size = ompi_fetch_int( process, /* sizeof (int) */
                                                     typedefs_sizeof,
                                                     p_info );
            typedefs_sizeof += p_info->sizes.int_size;
             p_info->sizes.long_size = ompi_fetch_int( process, /* sizeof (long) */
                                                       typedefs_sizeof,
                                                       p_info );
            typedefs_sizeof += p_info->sizes.int_size;
            p_info->sizes.long_long_size = ompi_fetch_int( process, /* sizeof (long long) */
                                                           typedefs_sizeof,
                                                           p_info );
            typedefs_sizeof += p_info->sizes.int_size;
            p_info->sizes.pointer_size = ompi_fetch_int( process, /* sizeof (void *) */
                                                         typedefs_sizeof,
                                                         p_info );
            typedefs_sizeof += p_info->sizes.int_size;
            p_info->sizes.bool_size = ompi_fetch_int( process, /* sizeof (bool) */
                                                      typedefs_sizeof,
                                                      p_info );
            typedefs_sizeof += p_info->sizes.int_size;
            p_info->sizes.size_t_size = ompi_fetch_int( process, /* sizeof (size_t) */
                                                        typedefs_sizeof,
                                                        p_info );
            DEBUG( VERBOSE_GENERAL,
                   ("sizes short = %d int = %d long = %d long long = %d "
                    "void* = %d bool = %d size_t = %d\n",
                    p_info->sizes.short_size, p_info->sizes.int_size,
                    p_info->sizes.long_size, p_info->sizes.long_long_size,
                    p_info->sizes.pointer_size, p_info->sizes.bool_size,
                    p_info->sizes.size_t_size) );
        }

        mqs_put_process_info (process, (mqs_process_info *)p_info);

        return mqs_ok;
    }
    return err_no_store;
} /* mqs_setup_process */

/***********************************************************************
 * Check the process for message queues.
 */
int mqs_process_has_queues (mqs_process *proc, char **msg)
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;
    mqs_image * image        = mqs_get_image (proc);
    mpi_image_info   *i_info = (mpi_image_info *)mqs_get_image_info (image);

    /* Don't bother with a pop up here, it's unlikely to be helpful */
    *msg = 0;
    DEBUG(VERBOSE_GENERAL,("checking the status of the OMPI dll\n"));
    if (mqs_find_symbol (image, "ompi_mpi_communicators", &extra->commlist_base) != mqs_ok)
        return err_all_communicators;

    if (mqs_find_symbol (image, "mca_pml_base_send_requests", &extra->send_queue_base) != mqs_ok)
        return err_mpid_sends;

    if (mqs_find_symbol (image, "mca_pml_base_recv_requests", &extra->recv_queue_base) != mqs_ok)
        return err_mpid_recvs;
    DEBUG(VERBOSE_GENERAL,("process_has_queues returned success\n"));
    return mqs_ok;
} /* mqs_process_has_queues */

/***********************************************************************
 * Check if the communicators have changed by looking at the
 * pointer array values for lowest_free and number_free.
 */
static int communicators_changed (mqs_process *proc)
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;
    mqs_image * image          = mqs_get_image (proc);
    mpi_image_info *i_info   = (mpi_image_info *)mqs_get_image_info (image);
    mqs_tword_t number_free;         /* the number of available positions in
                                      * the communicator array. */
    mqs_tword_t lowest_free;         /* the lowest free communicator */

    lowest_free = ompi_fetch_int( proc,
                                  extra->commlist_base + i_info->opal_pointer_array_t.offset.lowest_free,
                                  p_info );
    number_free = ompi_fetch_int( proc,
                                  extra->commlist_base + i_info->opal_pointer_array_t.offset.number_free,
                                  p_info );
    if( (lowest_free != extra->comm_lowest_free) ||
        (number_free != extra->comm_number_free) ) {
        DEBUG(VERBOSE_COMM, ("Recreate the communicator list\n"
                             "    lowest_free [current] %d != [stored] %d\n"
                             "    number_free [current] %d != [stored] %d\n",
                             (int)lowest_free, (int)extra->comm_lowest_free,
                             (int)number_free, (int)extra->comm_number_free) );
        return 1;
    }
    DEBUG(VERBOSE_COMM, ("Communicator list not modified\n") );
    return 0;
} /* mqs_communicators_changed */

/***********************************************************************
 * Find a matching communicator on our list. We check the recv context
 * as well as the address since the communicator structures may be
 * being re-allocated from a free list, in which case the same
 * address will be re-used a lot, which could confuse us.
 */
static communicator_t * find_communicator( mpi_process_info *p_info,
                                           int recv_ctx )
{
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;
    communicator_t * comm = extra->communicator_list;

    for( ; comm; comm = comm->next ) {
        if( comm->comm_info.unique_id == (mqs_taddr_t)recv_ctx )
            return comm;
    }

    return NULL;
} /* find_communicator */

/***********************************************************************
 * Comparison function for sorting communicators.
 */
static int compare_comms (const void *a, const void *b)
{
    communicator_t * ca = *(communicator_t **)a;
    communicator_t * cb = *(communicator_t **)b;

    return cb->comm_info.unique_id - ca->comm_info.unique_id;
} /* compare_comms */

/***********************************************************************
 * Rebuild our list of communicators because something has changed
 */
static int rebuild_communicator_list (mqs_process *proc)
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;
    mqs_image * image        = mqs_get_image (proc);
    mpi_image_info *i_info   = (mpi_image_info *)mqs_get_image_info (image);
    communicator_t **commp, *old;
    int i, commcount = 0, context_id;
    mqs_tword_t comm_size, lowest_free, number_free;
    mqs_taddr_t comm_addr_base;
    mqs_taddr_t comm_ptr;

    DEBUG(VERBOSE_COMM,("rebuild_communicator_list called "
                        "(commlist_base %llx, array offset %ld array size %d)\n",
                        (long long)extra->commlist_base,
                        (long)i_info->opal_pointer_array_t.offset.addr,
                        i_info->opal_pointer_array_t.size));
    /**
     * Start by getting the number of registered communicators in the
     * global communicator array.
     */
    comm_size = ompi_fetch_int( proc,
                                extra->commlist_base + i_info->opal_pointer_array_t.offset.size,
                                p_info );
    lowest_free = ompi_fetch_int( proc,
                                  extra->commlist_base + i_info->opal_pointer_array_t.offset.lowest_free,
                                  p_info );
    number_free = ompi_fetch_int( proc,
                                  extra->commlist_base + i_info->opal_pointer_array_t.offset.number_free,
                                  p_info );
    extra->comm_lowest_free = lowest_free;
    extra->comm_number_free = number_free;

    DEBUG(VERBOSE_COMM,("Number of coms %d lowest_free %d number_free %d\n",
                        (int)comm_size, (int)lowest_free, (int)number_free));
    /* In Open MPI the MPI_COMM_WORLD is always at index 0. By default, the
     * MPI_COMM_WORLD will never get modified. Except, when the fault tolerance
     * features are enabled in Open MPI. Therefore, we will regenerate the
     * list of proc pointers every time we rescan the communicators list.
     * We can use the fact that MPI_COMM_WORLD is at index 0 to force the
     * creation of the world_proc_array.
     */
    extra->world_proc_array_entries = 0;
    mqs_free( extra->world_proc_array );
    extra->world_proc_array = NULL;

    /* Now get the pointer to the array of pointers to communicators */
    comm_addr_base =
        ompi_fetch_pointer( proc,
                            extra->commlist_base + i_info->opal_pointer_array_t.offset.addr,
                            p_info );
    DEBUG(VERBOSE_COMM,("Array of communicators starting at 0x%llx (sizeof(mqs_taddr_t*) = %d)\n",
                        (long long)comm_addr_base, (int)sizeof(mqs_taddr_t)));
    for( i = 0; (commcount < (comm_size - number_free)) && (i < comm_size); i++ ) {
        /* Get the communicator pointer */
        comm_ptr =
            ompi_fetch_pointer( proc,
                                comm_addr_base + i * p_info->sizes.pointer_size,
                                p_info );
        DEBUG(VERBOSE_GENERAL,("Fetch communicator pointer 0x%llx\n", (long long)comm_ptr));
        if( 0 == comm_ptr ) continue;
        commcount++;
        /* Now let's grab the data we want from inside */
        DEBUG(VERBOSE_GENERAL, ("Retrieve context_id from 0x%llx and local_rank from 0x%llx\n",
                                (long long)(comm_ptr + i_info->ompi_communicator_t.offset.c_contextid),
                                (long long)(comm_ptr + i_info->ompi_communicator_t.offset.c_my_rank)));
        context_id = ompi_fetch_int( proc,
                                     comm_ptr + i_info->ompi_communicator_t.offset.c_contextid,
                                     p_info );
        /* Do we already have this communicator ? */
        old = find_communicator(p_info, context_id);
        if( NULL == old ) {
            mqs_taddr_t group_base;

            old = (communicator_t *)mqs_malloc (sizeof (communicator_t));
            /* Save the results */
            old->next                 = extra->communicator_list;
            extra->communicator_list = old;
            old->comm_ptr             = comm_ptr;
            old->comm_info.unique_id  = context_id;
            old->comm_info.local_rank = ompi_fetch_int(proc,
                                                       comm_ptr + i_info->ompi_communicator_t.offset.c_my_rank,
                                                       p_info);
            old->group = NULL;

            DEBUG(VERBOSE_COMM,("Create new communicator 0x%lx with context_id %d and local_rank %d\n",
                                (long)old, context_id, local_rank));
            /* Now get the information about the group */
            group_base =
                ompi_fetch_pointer( proc, comm_ptr + i_info->ompi_communicator_t.offset.c_local_group,
                                    p_info );
            old->group = find_or_create_group( proc, group_base );
        }
        mqs_fetch_data( proc, comm_ptr + i_info->ompi_communicator_t.offset.c_name,
                        64, old->comm_info.name );

        if( NULL != old->group ) {
            old->comm_info.size = old->group->entries;
        }
        old->present = TRUE;
        DEBUG(VERBOSE_COMM,("Communicator 0x%llx %d local_rank %d name %s group %p\n",
                            (long long)old->comm_ptr, (int)old->comm_info.unique_id,
                            (int)old->comm_info.local_rank, old->comm_info.name,
                            (void*)old->group));
    }

    /* Now iterate over the list tidying up any communicators which
     * no longer exist, and cleaning the flags on any which do.
     */
    commp = &extra->communicator_list;
    commcount = 0;
    for (; *commp; ) {
        communicator_t *comm = *commp;
        if (comm->present) {
            comm->present = FALSE;
            commcount++;
            DEBUG(VERBOSE_COMM, ("Keep communicator 0x%llx name %s\n",
                                 (long long)comm->comm_ptr, comm->comm_info.name));
            commp = &(*commp)->next;        /* go to the next communicator */
        } else { /* It needs to be deleted */
            *commp = comm->next;			/* Remove from the list, *commp now points to the next */
            DEBUG(VERBOSE_COMM, ("Remove communicator 0x%llx name %s (group %p)\n",
                                 (long long)comm->comm_ptr, comm->comm_info.name,
                                 (void*)comm->group));
            group_decref (comm->group);		/* Group is no longer referenced from here */
            mqs_free (comm);
        }
    }

    if (commcount) {
        /* Sort the list so that it is displayed in some semi-sane order. */
        communicator_t ** comm_array =
            (communicator_t **) mqs_malloc(commcount * sizeof (communicator_t *));
        communicator_t *comm = extra->communicator_list;

        for (i=0; i<commcount; i++, comm=comm->next)
            comm_array [i] = comm;

        /* Do the sort */
        qsort (comm_array, commcount, sizeof (communicator_t *), compare_comms);

        /* Rebuild the list */
        extra->communicator_list = NULL;
        for (i=0; i<commcount; i++) {
            comm = comm_array[i];
            comm->next = extra->communicator_list;
            extra->communicator_list = comm;
        }

        mqs_free (comm_array);
    }

    return mqs_ok;
} /* rebuild_communicator_list */

/***********************************************************************
 * Update the list of communicators in the process if it has changed.
 */
int mqs_update_communicator_list (mqs_process *proc)
{
    if (communicators_changed (proc))
        return rebuild_communicator_list (proc);
    return mqs_ok;
} /* mqs_update_communicator_list */

/***********************************************************************
 * Setup to iterate over communicators.
 * This is where we check whether our internal communicator list needs
 * updating and if so do it.
 */
int mqs_setup_communicator_iterator (mqs_process *proc)
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;

    /* Start at the front of the list again */
    extra->current_communicator = extra->communicator_list;
    /* Reset the operation iterator too */
    extra->next_msg.free_list            = 0;
    extra->next_msg.current_item         = 0;
    extra->next_msg.opal_list_t_pos.list = 0;

    DEBUG(VERBOSE_COMM,("mqs_setup_communicator_iterator called\n"));
    return extra->current_communicator == NULL ? mqs_end_of_list : mqs_ok;
} /* mqs_setup_communicator_iterator */

/***********************************************************************
 * Fetch information about the current communicator.
 */
int mqs_get_communicator (mqs_process *proc, mqs_communicator *comm)
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;

    if (extra->current_communicator) {
        *comm = extra->current_communicator->comm_info;
        DEBUG(VERBOSE_COMM,("mqs_get_communicator %d local_rank %d name %s\n",
                            (int)comm->unique_id, (int)comm->local_rank,
                            comm->name));
        return mqs_ok;
    }
    DEBUG(VERBOSE_COMM,("No more communicators for this iteration\n"));
    return err_no_current_communicator;
} /* mqs_get_communicator */

/***********************************************************************
 * Get the group information about the current communicator.
 */
int mqs_get_comm_group (mqs_process *proc, int *group_members)
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;
    communicator_t     *comm   = extra->current_communicator;

    if (comm && comm->group) {
        group_t * g = comm->group;
        int i;

        for (i=0; i<g->entries; i++)
            group_members[i] = g->local_to_global[i];

        return mqs_ok;
    }
    return err_no_current_communicator;
} /* mqs_get_comm_group */

/***********************************************************************
 * Step to the next communicator.
 */
int mqs_next_communicator (mqs_process *proc)
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;

    extra->current_communicator = extra->current_communicator->next;
    return (extra->current_communicator != NULL) ? mqs_ok : mqs_end_of_list;
} /* mqs_next_communicator */

/**
 * Parsing the opal_list_t.
 */
static int opal_list_t_init_parser( mqs_process *proc, mpi_process_info *p_info,
                                    mqs_opal_list_t_pos* position, mqs_taddr_t list )
{
    mqs_image * image        = mqs_get_image (proc);
    mpi_image_info *i_info   = (mpi_image_info *)mqs_get_image_info (image);

    position->list = list;
    position->sentinel = position->list + i_info->opal_list_t.offset.opal_list_sentinel;
    position->current_item =
        ompi_fetch_pointer( proc, position->sentinel + i_info->opal_list_item_t.offset.opal_list_next,
                            p_info );
    if( position->current_item == position->sentinel )
        position->current_item = 0;
    DEBUG(VERBOSE_LISTS,("opal_list_t_init_parser list = 0x%llx, sentinel = 0x%llx, "
                         "current_item = 0x%llx\n", (long long)position->list,
                         (long long)position->sentinel, (long long)position->current_item));
    return mqs_ok;
}

static int next_item_opal_list_t( mqs_process *proc, mpi_process_info *p_info,
                                  mqs_opal_list_t_pos* position, mqs_taddr_t* active_item )
{
    mqs_image * image        = mqs_get_image (proc);
    mpi_image_info *i_info   = (mpi_image_info *)mqs_get_image_info (image);

    *active_item = position->current_item;
    if( 0 == position->current_item )
        return mqs_end_of_list;

    position->current_item =
        ompi_fetch_pointer( proc,
                            position->current_item + i_info->opal_list_item_t.offset.opal_list_next,
                            p_info );
    if( position->current_item == position->sentinel )
        position->current_item = 0;
    return mqs_ok;
}

#if defined(CODE_NOT_USED)
/**
 * Parsing the opal_free_list lists.
 */
static void opal_free_list_t_dump_position( mqs_opal_free_list_t_pos* position )
{
    printf( "position->opal_list_t_pos.current_item = 0x%llx\n", (long long)position->opal_list_t_pos.current_item );
    printf( "position->opal_list_t_pos.list         = 0x%llx\n", (long long)position->opal_list_t_pos.list );
    printf( "position->opal_list_t_pos.sentinel     = 0x%llx\n", (long long)position->opal_list_t_pos.sentinel );
    printf( "position->current_item                 = 0x%llx\n", (long long)position->current_item );
    printf( "position->upper_bound                  = 0x%llx\n", (long long)position->upper_bound );
    printf( "position->header_space                 = %llx\n", (long long)position->header_space );
    printf( "position->free_list                    = 0x%llx\n", (long long)position->free_list );
    printf( "position->fl_frag_class                = 0x%llx\n", (long long)position->fl_frag_class );
    printf( "position->fl_mpool                     = 0x%llx\n", (long long)position->fl_mpool );
    printf( "position->fl_frag_size                 = %llx\n", (long long)position->fl_frag_size );
    printf( "position->fl_frag_alignment            = %llx\n", (long long)position->fl_frag_alignment );
    printf( "position->fl_num_per_alloc             = %llx\n", (long long)position->fl_num_per_alloc );
    printf( "position->fl_num_allocated             = %llx\n", (long long)position->fl_num_allocated );
    printf( "position->fl_num_initial_alloc         = %llx\n", (long long)position->fl_num_initial_alloc );
}
#endif  /* CODE_NOT_USED */

static int opal_free_list_t_init_parser( mqs_process *proc, mpi_process_info *p_info,
                                         mqs_opal_free_list_t_pos* position, mqs_taddr_t free_list )
{
    mqs_image * image          = mqs_get_image (proc);
    mpi_image_info *i_info   = (mpi_image_info *)mqs_get_image_info (image);
    mqs_taddr_t active_allocation;

    position->free_list = free_list;

    position->fl_frag_size =
        ompi_fetch_size_t( proc, position->free_list + i_info->opal_free_list_t.offset.fl_frag_size,
                           p_info );
    position->fl_frag_alignment =
        ompi_fetch_size_t( proc, position->free_list + i_info->opal_free_list_t.offset.fl_frag_alignment,
                           p_info );
    position->fl_frag_class =
        ompi_fetch_pointer( proc, position->free_list + i_info->opal_free_list_t.offset.fl_frag_class,
                            p_info );
    position->fl_mpool =
        ompi_fetch_pointer( proc, position->free_list + i_info->opal_free_list_t.offset.fl_mpool,
                            p_info );
    position->fl_num_per_alloc =
        ompi_fetch_size_t( proc, position->free_list + i_info->opal_free_list_t.offset.fl_num_per_alloc,
                           p_info );
    position->fl_num_allocated =
        ompi_fetch_size_t( proc, position->free_list + i_info->opal_free_list_t.offset.fl_num_allocated,
                           p_info );

    if( 0 == position->fl_mpool ) {
        position->header_space = position->fl_frag_size;
    } else {
        DEBUG(VERBOSE_GENERAL, ("BLAH !!! (CORRECT ME)\n"));
        position->header_space = position->fl_frag_size;
    }
    position->header_space = OPAL_ALIGN( position->header_space,
                                         position->fl_frag_alignment, mqs_taddr_t );

    /**
     * Work around the strange opal_free_list_t way to allocate elements. The first chunk is
     * not required to have the same size as the others.
     * A similar work around should be set for the last chunk of allocations too !!! But how
     * can we solve ONE equation with 2 unknowns ?
     */
    if( position->fl_num_allocated <= position->fl_num_per_alloc ) {
        position->fl_num_initial_alloc = position->fl_num_allocated;
    } else {
        position->fl_num_initial_alloc = position->fl_num_allocated % position->fl_num_per_alloc;
        if( 0 == position->fl_num_initial_alloc )
            position->fl_num_initial_alloc = position->fl_num_per_alloc;
    }
    DEBUG(VERBOSE_LISTS,("opal_free_list_t fl_frag_size = %lld fl_header_space = %lld\n"
                         "                 fl_frag_alignment = %lld fl_num_per_alloc = %lld\n"
                         "                 fl_num_allocated = %lld fl_num_initial_alloc = %lld\n"
                         "                 header_space = %lld\n",
                         (long long)position->fl_frag_size, (long long)position->header_space,
                         (long long)position->fl_frag_alignment, (long long)position->fl_num_per_alloc,
                         (long long)position->fl_num_allocated, (long long)position->fl_num_initial_alloc,
                         (long long)position->header_space));

    /**
     * Initialize the pointer to the opal_list_t.
     */
    opal_list_t_init_parser( proc, p_info, &position->opal_list_t_pos,
                             position->free_list + i_info->opal_free_list_t.offset.fl_allocations );
    next_item_opal_list_t( proc, p_info, &position->opal_list_t_pos, &active_allocation );
    DEBUG(VERBOSE_LISTS,("active_allocation 0x%llx header_space %d\n",
                         (long long)active_allocation, (int)position->header_space));
    if( 0 == active_allocation ) {  /* the end of the list */
        position->upper_bound = 0;
    } else {
        /**
         * Handle alignment issues...
         */
        active_allocation += i_info->opal_free_list_item_t.size;
        active_allocation = OPAL_ALIGN( active_allocation,
                                        position->fl_frag_alignment, mqs_taddr_t );
        /**
         * Now let's try to compute the upper bound ...
         */
        position->upper_bound =
            position->fl_num_initial_alloc * position->header_space + active_allocation;
        DEBUG(VERBOSE_LISTS,("there are some elements in the list "
                             "active_allocation = %llx upper_bound = %llx\n",
                             (long long)active_allocation, (long long)position->upper_bound));
    }
    position->current_item = active_allocation;

    /*opal_free_list_t_dump_position( position );*/
    return mqs_ok;
}

/**
 * Return the current position and move the internal counter to the next element.
 */
static int opal_free_list_t_next_item( mqs_process *proc, mpi_process_info *p_info,
                                       mqs_opal_free_list_t_pos* position, mqs_taddr_t* active_item )
{
    mqs_image * image          = mqs_get_image (proc);
    mpi_image_info *i_info   = (mpi_image_info *)mqs_get_image_info (image);
    mqs_taddr_t active_allocation;

    *active_item = position->current_item;
    if( 0 == position->current_item )  /* the end ... */
        return mqs_ok;

    position->current_item += position->header_space;
    if( position->current_item >= position->upper_bound ) {
        DEBUG(VERBOSE_LISTS,("Reach the end of one of the opal_free_list_t "
                             "allocations. Go to the next one\n"));
        /* we should go to the next allocation */
        next_item_opal_list_t( proc, p_info,
                               &position->opal_list_t_pos, &active_allocation );
        if( 0 == active_allocation ) { /* we're at the end */
            position->current_item = 0;
            return mqs_ok;
        }
        /**
         * Handle alignment issues...
         */
        active_allocation += i_info->opal_free_list_item_t.size;
        active_allocation = OPAL_ALIGN( active_allocation,
                                        position->fl_frag_alignment, mqs_taddr_t );
        /**
         * Now let's try to compute the upper bound ...
         */
        position->upper_bound =
            position->fl_num_per_alloc * position->header_space + active_allocation;
        position->current_item = active_allocation;
        DEBUG(VERBOSE_LISTS,("there are more elements in the list "
                             "active_allocation = %llx upper_bound = %llx\n",
                             (long long)active_allocation, (long long)position->upper_bound));
        /*opal_free_list_t_dump_position( position );*/
    }
    DEBUG(VERBOSE_LISTS,("Free list actual position 0x%llx next element at 0x%llx\n",
                         (long long)*active_item, (long long)position->current_item));
    return mqs_ok;
}

static void dump_request( mqs_taddr_t current_item, mqs_pending_operation *res )
{
    if(!(VERBOSE_REQ_DUMP & VERBOSE)) return;
    printf( "\n+===============================================+\n"
            "|Request 0x%llx contain \n"
            "|    res->status              = %d\n"
            "|    res->desired_local_rank  = %ld\n"
            "|    res->desired_global_rank = %ld\n"
            "|    res->tag_wild            = %ld\n"
            "|    res->desired_tag         = %ld\n"
            "|    res->system_buffer       = %s\n"
            "|    res->buffer              = 0x%llx\n"
            "|    res->desired_length      = %ld\n",
        (long long)current_item, res->status, (long)res->desired_local_rank,
        (long)res->desired_global_rank, (long)res->tag_wild, (long)res->desired_tag,
        (TRUE == res->system_buffer ? "TRUE" : "FALSE"), (long long)res->buffer,
        (long)res->desired_length );

    if( res->status > mqs_st_pending ) {
        printf( "|    res->actual_length       = %ld\n"
                "|    res->actual_tag          = %ld\n"
                "|    res->actual_local_rank   = %ld\n"
                "|    res->actual_global_rank  = %ld\n",
                (long)res->actual_length, (long)res->actual_tag,
                (long)res->actual_local_rank, (long)res->actual_global_rank );
    }
    if( '\0' != res->extra_text[0][0] )
        printf( "|    extra[0] = %s\n", res->extra_text[0] );
    if( '\0' != res->extra_text[1][0] )
        printf( "|    extra[1] = %s\n", res->extra_text[1] );
    if( '\0' != res->extra_text[2][0] )
        printf( "|    extra[2] = %s\n", res->extra_text[2] );
    if( '\0' != res->extra_text[3][0] )
        printf( "|    extra[3] = %s\n", res->extra_text[3] );
    if( '\0' != res->extra_text[4][0] )
        printf( "|    extra[4] = %s\n", res->extra_text[4] );
    printf( "+===============================================+\n\n" );
}

/**
 * Handle the send queue as well as the receive queue. The unexpected queue
 * is a whole different story ...
 */
static int fetch_request( mqs_process *proc, mpi_process_info *p_info,
                          mqs_pending_operation *res, int look_for_user_buffer )
{
    mqs_image * image        = mqs_get_image (proc);
    mpi_image_info *i_info   = (mpi_image_info *)mqs_get_image_info (image);
    mqs_taddr_t current_item;
    mqs_tword_t req_complete, req_pml_complete, req_valid, req_type;
    mqs_taddr_t req_buffer, req_comm;
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;

    /* If we get a PML request with an internal tag we will jump back here */
  rescan_requests:
    while( 1 ) {
        opal_free_list_t_next_item( proc, p_info,
                                    &extra->next_msg, &current_item );
        if( 0 == current_item ) {
            DEBUG(VERBOSE_REQ,("no more items in the %s request queue\n",
                               look_for_user_buffer ? "receive" : "send" ));
            return mqs_end_of_list;
        }
        req_valid = ompi_fetch_int( proc, current_item + i_info->ompi_request_t.offset.req_state, p_info );
        if( OMPI_REQUEST_INVALID == req_valid ) continue;
        req_comm = ompi_fetch_pointer( proc, current_item + i_info->mca_pml_base_request_t.offset.req_comm, p_info );
        if( extra->current_communicator->comm_ptr == req_comm ) break;
        DEBUG(VERBOSE_REQ,("unmatched request (0x%llx) req_comm = %llx current_com = %llx\n",
                           (long long)current_item, (long long)req_comm,
                           (long long)extra->current_communicator->comm_ptr));
    }

    res->extra_text[0][0] = 0; res->extra_text[1][0] = 0; res->extra_text[2][0] = 0;
    res->extra_text[3][0] = 0; res->extra_text[4][0] = 0;

    req_type = ompi_fetch_int( proc, current_item + i_info->ompi_request_t.offset.req_type, p_info );
    if( OMPI_REQUEST_PML == req_type ) {
        mqs_taddr_t ompi_datatype;
        char data_name[64];

        /**
         * First retrieve the tag. If the tag is negative and the user didn't
         * request the internal requests information then move along.
         */
        res->desired_tag =
            ompi_fetch_int( proc, current_item + i_info->mca_pml_base_request_t.offset.req_tag, p_info );
        if( MPI_ANY_TAG == (int)res->desired_tag ) {
            res->tag_wild = TRUE;
        } else {
            /* Don't allow negative tags to show up */
            if( ((int)res->desired_tag < 0) && (0 == extra->show_internal_requests) )
                goto rescan_requests;
            res->tag_wild = FALSE;
        }

        req_type =
            ompi_fetch_int( proc, current_item + i_info->mca_pml_base_request_t.offset.req_type,
                            p_info);
        req_complete =
            ompi_fetch_bool( proc,
                             current_item + i_info->ompi_request_t.offset.req_complete,
                             p_info );
        req_pml_complete =
            ompi_fetch_bool( proc,
                             current_item + i_info->mca_pml_base_request_t.offset.req_pml_complete,
                             p_info );
        res->status = (0 == req_complete ? mqs_st_pending : mqs_st_complete);

        res->desired_local_rank  = ompi_fetch_int( proc, current_item + i_info->mca_pml_base_request_t.offset.req_peer, p_info );
        res->desired_global_rank = translate( extra->current_communicator->group,
                                              res->desired_local_rank );

        res->buffer = ompi_fetch_pointer( proc, current_item + i_info->mca_pml_base_request_t.offset.req_addr,
                                     p_info );
        /* Set this to true if it's a buffered request */
        res->system_buffer = FALSE;

        /* The pointer to the request datatype */
        ompi_datatype =
            ompi_fetch_pointer( proc,
                                current_item + i_info->mca_pml_base_request_t.offset.req_datatype, p_info );
        /* Retrieve the count as specified by the user */
        res->desired_length =
            ompi_fetch_size_t( proc,
                               ompi_datatype + i_info->ompi_datatype_t.offset.size,
                               p_info );
        /* Be user friendly, show the datatype name */
        mqs_fetch_data( proc, ompi_datatype + i_info->ompi_datatype_t.offset.name,
                        64, data_name );
        if( '\0' != data_name[0] ) {
            snprintf( (char*)res->extra_text[1], 64, "Data: %d * %s",
                      (int)res->desired_length, data_name );
        }
        /* And now compute the real length as specified by the user */
        res->desired_length *=
            ompi_fetch_size_t( proc,
                               current_item + i_info->mca_pml_base_request_t.offset.req_count,
                               p_info );

        if( MCA_PML_REQUEST_SEND == req_type ) {
            snprintf( (char *)res->extra_text[0], 64, "Send: 0x%llx", (long long)current_item );
            req_buffer =
                ompi_fetch_pointer( proc,
                                    current_item + i_info->mca_pml_base_send_request_t.offset.req_addr,
                                    p_info );
            res->system_buffer = ( req_buffer == res->buffer ? FALSE : TRUE );
            res->actual_length =
                ompi_fetch_size_t( proc,
                                   current_item + i_info->mca_pml_base_send_request_t.offset.req_bytes_packed, p_info );
            res->actual_tag         = res->desired_tag;
            res->actual_local_rank  = res->desired_local_rank;
            res->actual_global_rank = res->actual_local_rank;
        } else if( MCA_PML_REQUEST_RECV == req_type ) {
            snprintf( (char *)res->extra_text[0], 64, "Receive: 0x%llx", (long long)current_item );
            /**
             * There is a trick with the MPI_TAG. All receive requests set it to MPI_ANY_TAG
             * when the request get initialized, and to the real tag once the request
             * is matched.
             */
            res->actual_tag =
                ompi_fetch_int( proc, current_item + i_info->ompi_request_t.offset.req_status +
                                i_info->ompi_status_public_t.offset.MPI_TAG, p_info );
            if( MPI_ANY_TAG != (int)res->actual_tag ) {
                res->status = mqs_st_matched;
                res->desired_length =
                    ompi_fetch_size_t( proc,
                                       current_item + i_info->mca_pml_base_recv_request_t.offset.req_bytes_packed,
                                       p_info );
                res->actual_local_rank =
                    ompi_fetch_int( proc, current_item + i_info->ompi_request_t.offset.req_status +
                                    i_info->ompi_status_public_t.offset.MPI_SOURCE, p_info );
                res->actual_global_rank = translate( extra->current_communicator->group,
                                                  res->actual_local_rank );
            }
        } else {
            snprintf( (char *)res->extra_text[0], 64, "Unknown type of request 0x%llx", (long long)current_item );
        }
        if( 0 != req_pml_complete ) {
			snprintf( (char *)res->extra_text[1], 64, "Data transfer completed" );
        }

        /* If the length we're looking for is the count ... */
        /*res->desired_length      =
          ompi_fetch_int( proc, current_item + i_info->mca_pml_base_request_t.offset.req_count, p_info );*/

        if( (mqs_st_pending < res->status) && (MCA_PML_REQUEST_SEND != req_type) ) {  /* The real data from the status */
            res->actual_length       =
                ompi_fetch_size_t( proc, current_item + i_info->ompi_request_t.offset.req_status +
                                   i_info->ompi_status_public_t.offset._ucount, p_info );
            res->actual_tag          =
                ompi_fetch_int( proc, current_item + i_info->ompi_request_t.offset.req_status +
                                i_info->ompi_status_public_t.offset.MPI_TAG, p_info );
            res->actual_local_rank   =
                ompi_fetch_int( proc, current_item + i_info->ompi_request_t.offset.req_status +
                                i_info->ompi_status_public_t.offset.MPI_SOURCE, p_info );
            res->actual_global_rank  = translate( extra->current_communicator->group,
                                                  res->actual_local_rank );
        }
        dump_request( current_item, res );
    }
    return mqs_ok;
}

/***********************************************************************
 * Setup to iterate over pending operations
 */
int mqs_setup_operation_iterator (mqs_process *proc, int op)
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;

    extra->what = (mqs_op_class)op;

    switch (op) {
    case mqs_pending_sends:
        DEBUG(VERBOSE_REQ,("setup the send queue iterator\n"));
        opal_free_list_t_init_parser( proc, p_info, &extra->next_msg, extra->send_queue_base );
        return mqs_ok;

    case mqs_pending_receives:
        DEBUG(VERBOSE_REQ,("setup the receive queue iterator\n"));
        opal_free_list_t_init_parser( proc, p_info, &extra->next_msg, extra->recv_queue_base );
        return mqs_ok;

    case mqs_unexpected_messages:  /* TODO */
        return mqs_no_information;

    default:
        return err_bad_request;
    }
} /* mqs_setup_operation_iterator */

/***********************************************************************
 * Fetch the next valid operation.
 * Since Open MPI only maintains a single queue of each type of operation,
 * we have to run over it and filter out the operations which
 * match the active communicator.
 */
int mqs_next_operation (mqs_process *proc, mqs_pending_operation *op)
{
    mpi_process_info *p_info = (mpi_process_info *)mqs_get_process_info (proc);
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;

    switch (extra->what) {
    case mqs_pending_receives:
        DEBUG(VERBOSE_REQ,("digging for the receive queue\n"));
        return fetch_request( proc, p_info, op, TRUE );
    case mqs_unexpected_messages:
        /* TODO: not handled yet */
        return err_bad_request;
    case mqs_pending_sends:
        DEBUG(VERBOSE_REQ,("digging for the send queue\n"));
        return fetch_request( proc, p_info, op, FALSE );
    default: return err_bad_request;
    }
} /* mqs_next_operation */

/***********************************************************************
 * Destroy the info.
 */
void mqs_destroy_process_info (mqs_process_info *mp_info)
{
    mpi_process_info *p_info = (mpi_process_info *)mp_info;
    mpi_process_info_extra *extra = (mpi_process_info_extra*) p_info->extra;
    /* Need to handle the communicators and groups too */
    communicator_t *comm;

    if( NULL != extra) {
        comm = extra->communicator_list;
        while (comm) {
            communicator_t *next = comm->next;

            if( NULL != comm->group )
                group_decref (comm->group);  /* Group is no longer referenced from here */
            mqs_free (comm);

            comm = next;
        }
        if (NULL != extra) {
            mqs_free(extra);
        }
    }
    mqs_free (p_info);
} /* mqs_destroy_process_info */

/***********************************************************************
 * Free off the data we associated with an image. Since we malloced it
 * we just free it.
 */
void mqs_destroy_image_info (mqs_image_info *info)
{
    mqs_free (info);
} /* mqs_destroy_image_info */

/***********************************************************************/
/* Convert an error code into a printable string */
char * mqs_dll_error_string (int errcode)
{
    switch (errcode) {
    case err_silent_failure:
        return "";
    case err_no_current_communicator:
        return "No current communicator in the communicator iterator";
    case err_bad_request:
        return "Attempting to setup to iterate over an unknown queue of operations";
    case err_no_store:
        return "Unable to allocate store";
    case err_failed_qhdr:
        return "Failed to find type MPID_QHDR";
    case err_unexpected:
        return "Failed to find field 'unexpected' in MPID_QHDR";
    case err_posted:
        return "Failed to find field 'posted' in MPID_QHDR";
    case err_failed_queue:
        return "Failed to find type MPID_QUEUE";
    case err_first:
        return "Failed to find field 'first' in MPID_QUEUE";
    case err_context_id:
        return "Failed to find field 'context_id' in MPID_QEL";
    case err_tag:
        return "Failed to find field 'tag' in MPID_QEL";
    case err_tagmask:
        return "Failed to find field 'tagmask' in MPID_QEL";
    case err_lsrc:
        return "Failed to find field 'lsrc' in MPID_QEL";
    case err_srcmask:
        return "Failed to find field 'srcmask' in MPID_QEL";
    case err_next:
        return "Failed to find field 'next' in MPID_QEL";
    case err_ptr:
        return "Failed to find field 'ptr' in MPID_QEL";
    case err_missing_type:
        return "Failed to find some type";
    case err_missing_symbol:
        return "Failed to find field the global symbol";
    case err_db_shandle:
        return "Failed to find field 'db_shandle' in MPIR_SQEL";
    case err_db_comm:
        return "Failed to find field 'db_comm' in MPIR_SQEL";
    case err_db_target:
        return "Failed to find field 'db_target' in MPIR_SQEL";
    case err_db_tag:
        return "Failed to find field 'db_tag' in MPIR_SQEL";
    case err_db_data:
        return "Failed to find field 'db_data' in MPIR_SQEL";
    case err_db_byte_length:
        return "Failed to find field 'db_byte_length' in MPIR_SQEL";
    case err_db_next:
        return "Failed to find field 'db_next' in MPIR_SQEL";
    case err_failed_rhandle:
        return "Failed to find type MPIR_RHANDLE";
    case err_is_complete:
        return "Failed to find field 'is_complete' in MPIR_RHANDLE";
    case err_buf:
        return "Failed to find field 'buf' in MPIR_RHANDLE";
    case err_len:
        return "Failed to find field 'len' in MPIR_RHANDLE";
    case err_s:
        return "Failed to find field 's' in MPIR_RHANDLE";
    case err_failed_status:
        return "Failed to find type MPI_Status";
    case err_count:
        return "Failed to find field 'count' in MPIR_Status";
    case err_MPI_SOURCE:
        return "Failed to find field 'MPI_SOURCE' in MPIR_Status";
    case err_MPI_TAG:
        return "Failed to find field 'MPI_TAG' in MPIR_Status";
    case err_failed_commlist:
        return "Failed to find type MPIR_Comm_list";
    case err_sequence_number:
        return "Failed to find field 'sequence_number' in MPIR_Comm_list";
    case err_comm_first:
        return "Failed to find field 'comm_first' in MPIR_Comm_list";
    case err_failed_communicator:
        return "Failed to find type MPIR_Communicator";
    case err_lrank_to_grank:
        return "Failed to find field 'lrank_to_grank' in MPIR_Communicator";
    case err_send_context:
        return "Failed to find field 'send_context' in MPIR_Communicator";
    case err_recv_context:
        return "Failed to find field 'recv_context' in MPIR_Communicator";
    case err_comm_next:
        return "Failed to find field 'comm_next' in MPIR_Communicator";
    case err_comm_name:
        return "Failed to find field 'comm_name' in MPIR_Communicator";
    case err_all_communicators:
        return "Failed to find the global symbol MPIR_All_communicators";
    case err_mpid_sends:
        return "Failed to access the global send requests list";
    case err_mpid_recvs:
        return "Failed to access the global receive requests list";
    case err_group_corrupt:
        return "Could not read a communicator's group from the process (probably a store corruption)";

    default: return "Unknown error code";
    }
} /* mqs_dll_error_string */
