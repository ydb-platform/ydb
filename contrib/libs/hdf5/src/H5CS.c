/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:	Provides internal function tracing in the form of a stack.
 *		The FUNC_ENTER() macro adds the function name to the function
 *              stack whenever a function is entered.
 *		As the functions return with FUNC_LEAVE,
 *		entries are removed from the stack.
 *
 *		A function stack has a fixed maximum size.  If this size is
 *		exceeded then the stack will be truncated and only the
 *		first called functions will have entries on the stack. This is
 *		expected to be a rare condition.
 *
 */

#include "H5private.h"   /* Generic Functions			*/
#include "H5CSprivate.h" /* Function stack			*/
#include "H5Eprivate.h"  /* Error handling		  	*/

#ifdef H5_HAVE_CODESTACK

#define H5CS_MIN_NSLOTS 16 /* Minimum number of records in an function stack	*/

/* A function stack */
typedef struct H5CS_t {
    unsigned     nused;  /* Number of records currently used in stack */
    unsigned     nalloc; /* Number of records current allocated for stack */
    const char **rec;    /* Array of function records */
} H5CS_t;

#ifdef H5_HAVE_THREADSAFE
/*
 * The per-thread function stack. pthread_once() initializes a special
 * key that will be used by all threads to create a stack specific to
 * each thread individually. The association of stacks to threads will
 * be handled by the pthread library.
 *
 * In order for this macro to work, H5CS_get_my_stack() must be preceded
 * by "H5CS_t *fstack =".
 */
static H5CS_t *H5CS__get_stack(void);
#define H5CS_get_my_stack() H5CS__get_stack()
#else /* H5_HAVE_THREADSAFE */
/*
 * The function stack.  Eventually we'll have some sort of global table so each
 * thread has it's own stack.  The stacks will be created on demand when the
 * thread first calls H5CS_push().  */
H5CS_t H5CS_stack_g[1];
#define H5CS_get_my_stack() (H5CS_stack_g + 0)
#endif /* H5_HAVE_THREADSAFE */

#ifdef H5_HAVE_THREADSAFE
/*-------------------------------------------------------------------------
 * Function:	H5CS__get_stack
 *
 * Purpose:	Support function for H5CS_get_my_stack() to initialize and
 *              acquire per-thread function stack.
 *
 * Return:	Success:	function stack (H5CS_t *)
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static H5CS_t *
H5CS__get_stack(void)
{
    H5CS_t *fstack;

    FUNC_ENTER_PACKAGE_NOERR_NOFS

    fstack = H5TS_get_thread_local_value(H5TS_funcstk_key_g);
    if (!fstack) {
        /* No associated value with current thread - create one */
#ifdef H5_HAVE_WIN_THREADS
        fstack = (H5CS_t *)LocalAlloc(
            LPTR, sizeof(H5CS_t)); /* Win32 has to use LocalAlloc to match the LocalFree in DllMain */
#else
        fstack =
            (H5CS_t *)malloc(sizeof(H5CS_t)); /* Don't use H5MM_malloc() here, it causes infinite recursion */
#endif /* H5_HAVE_WIN_THREADS */
        assert(fstack);

        /* Set the thread-specific info */
        fstack->nused  = 0;
        fstack->nalloc = 0;
        fstack->rec    = NULL;

        /* (It's not necessary to release this in this API, it is
         *      released by the "key destructor" set up in the H5TS
         *      routines.  See calls to pthread_key_create() in H5TS.c -QAK)
         */
        H5TS_set_thread_local_value(H5TS_funcstk_key_g, (void *)fstack);
    } /* end if */

    FUNC_LEAVE_NOAPI_NOFS(fstack)
} /* end H5CS__get_stack() */
#endif /* H5_HAVE_THREADSAFE */

/*-------------------------------------------------------------------------
 * Function:	H5CS_print_stack
 *
 * Purpose:	Prints a function stack.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CS_print_stack(const H5CS_t *fstack, FILE *stream)
{
    const int indent = 2; /* Indentation level */
    int       i;          /* Local index ariable */

    /* Don't push this function on the function stack... :-) */
    FUNC_ENTER_NOAPI_NOERR_NOFS

    /* Sanity check */
    assert(fstack);

    /* Default to outputting information to stderr */
    if (!stream)
        stream = stderr;

    fprintf(stream, "HDF5-DIAG: Function stack from %s ", H5_lib_vers_info_g);
    /* try show the process or thread id in multiple processes cases*/
    fprintf(stream, "thread %" PRIu64 ".", H5TS_thread_id());
    if (fstack && fstack->nused > 0)
        fprintf(stream, "  Back trace follows.");
    fputc('\n', stream);

    for (i = fstack->nused - 1; i >= 0; --i)
        fprintf(stream, "%*s#%03d: Routine: %s\n", indent, "", i, fstack->rec[i]);

    FUNC_LEAVE_NOAPI_NOFS(SUCCEED)
} /* end H5CS_print_stack() */

/*-------------------------------------------------------------------------
 * Function:	H5CS_push
 *
 * Purpose:	Pushes a new record onto function stack for the current
 *		thread.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CS_push(const char *func_name)
{
    H5CS_t *fstack = H5CS_get_my_stack(); /* Current function stack for library */

    /* Don't push this function on the function stack... :-) */
    FUNC_ENTER_NOAPI_NOERR_NOFS

    /* Sanity check */
    assert(fstack);
    assert(fstack->nused <= fstack->nalloc);
    assert(func_name);

    /* Check if we need to expand the stack of records */
    if (fstack->nused == fstack->nalloc) {
        size_t na = MAX((fstack->nalloc * 2), H5CS_MIN_NSLOTS);

        /* Don't use H5MM_realloc here */
        const char **x = (const char **)realloc(fstack->rec, na * sizeof(const char *));

        /* (Avoid returning an error from this routine, currently -QAK) */
        assert(x);
        fstack->rec    = x;
        fstack->nalloc = na;
    } /* end if */

    /* Push the function name */
    fstack->rec[fstack->nused] = func_name;
    fstack->nused++;

    FUNC_LEAVE_NOAPI_NOFS(SUCCEED)
} /* end H5CS_push() */

/*-------------------------------------------------------------------------
 * Function:	H5CS_pop
 *
 * Purpose:	Pops a record off function stack for the current thread.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CS_pop(void)
{
    H5CS_t *fstack = H5CS_get_my_stack();

    /* Don't push this function on the function stack... :-) */
    FUNC_ENTER_NOAPI_NOERR_NOFS

    /* Sanity check */
    assert(fstack);
    assert(fstack->nused > 0);

    /* Pop the function. */
    fstack->nused--;

    FUNC_LEAVE_NOAPI_NOFS(SUCCEED)
} /* end H5CS_pop() */

/*-------------------------------------------------------------------------
 * Function:	H5CS_copy_stack
 *
 * Purpose:	Makes a copy of the current stack
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5CS_t *
H5CS_copy_stack(void)
{
    H5CS_t *old_stack = H5CS_get_my_stack(); /* Existing function stack for library */
    H5CS_t *new_stack;                       /* New function stack, for copy */
    H5CS_t *ret_value = NULL;                /* Return value */

    /* Don't push this function on the function stack... :-) */
    FUNC_ENTER_NOAPI_NOFS

    /* Sanity check */
    assert(old_stack);

    /* Allocate a new stack */
    /* (Don't use library allocate code, since this code stack supports it) */
    if (NULL == (new_stack = calloc(1, sizeof(H5CS_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate function stack");
    if (NULL == (new_stack->rec = calloc(old_stack->nused, sizeof(const char *))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate function stack records");

    /* Copy pointers on old stack to new one */
    /* (Strings don't need to be duplicated, they are statically allocated) */
    H5MM_memcpy(new_stack->rec, old_stack->rec, sizeof(char *) * old_stack->nused);
    new_stack->nused = new_stack->nalloc = old_stack->nused;

    /* Set the return value */
    ret_value = new_stack;

done:
    FUNC_LEAVE_NOAPI_NOFS(ret_value)
} /* end H5CS_copy_stack() */

/*-------------------------------------------------------------------------
 * Function:	H5CS_close_stack
 *
 * Purpose:	Closes and frees a copy of a stack
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CS_close_stack(H5CS_t *stack)
{
    /* Don't push this function on the function stack... :-) */
    FUNC_ENTER_NOAPI_NOERR_NOFS

    /* Sanity check */
    assert(stack);

    /* Free stack */
    /* The function name string are statically allocated (by the compiler)
     * and are not allocated, so there's no need to free them.
     */
    if (stack->rec) {
        free(stack->rec);
        stack->rec = NULL;
    } /* end if */
    if (stack)
        free(stack);

    FUNC_LEAVE_NOAPI_NOFS(SUCCEED)
} /* end H5CS_close_stack() */

#endif /* H5_HAVE_CODESTACK */
