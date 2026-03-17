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

/****************/
/* Module Setup */
/****************/
#include "H5module.h" /* This source code file is part of the H5 module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dprivate.h"  /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FLprivate.h" /* Free lists                               */
#include "H5FSprivate.h" /* File free space                          */
#include "H5Lprivate.h"  /* Links                                    */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Pprivate.h"  /* Property lists                           */
#include "H5PLprivate.h" /* Plugins                                  */
#include "H5SLprivate.h" /* Skip lists                               */
#include "H5Tprivate.h"  /* Datatypes                                */

#include "H5FDsec2.h" /* for H5FD_sec2_init() */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/* Node for list of 'atclose' routines to invoke at library shutdown */
typedef struct H5_atclose_node_t {
    H5_atclose_func_t         func; /* Function to invoke */
    void                     *ctx;  /* Context to pass to function */
    struct H5_atclose_node_t *next; /* Pointer to next node in list */
} H5_atclose_node_t;

/********************/
/* Local Prototypes */
/********************/
static void H5__debug_mask(const char *);
#ifdef H5_HAVE_PARALLEL
static int H5__mpi_delete_cb(MPI_Comm comm, int keyval, void *attr_val, int *flag);
#endif /*H5_HAVE_PARALLEL*/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/* Library incompatible release versions, develop releases are incompatible by design */
static const unsigned VERS_RELEASE_EXCEPTIONS[]    = {0};
static const unsigned VERS_RELEASE_EXCEPTIONS_SIZE = 0;

/* statically initialize block for pthread_once call used in initializing */
/* the first global mutex                                                 */
#ifdef H5_HAVE_THREADSAFE
H5_api_t H5_g;
#else
bool H5_libinit_g = false; /* Library hasn't been initialized */
bool H5_libterm_g = false; /* Library isn't being shutdown */
#endif

char        H5_lib_vers_info_g[] = H5_VERS_INFO;
static bool H5_dont_atexit_g     = false;
H5_debug_t  H5_debug_g; /* debugging info */

/*******************/
/* Local Variables */
/*******************/

/* Linked list of registered 'atclose' functions to invoke at library shutdown */
static H5_atclose_node_t *H5_atclose_head = NULL;

/* Declare a free list to manage the H5_atclose_node_t struct */
H5FL_DEFINE_STATIC(H5_atclose_node_t);

/*-------------------------------------------------------------------------
 * Function:    H5_default_vfd_init
 *
 * Purpose:     Initialize the default VFD.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *-------------------------------------------------------------------------
 */
static herr_t
H5_default_vfd_init(void)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)
    /* Load the hid_t for the default VFD for the side effect
     * it has of initializing the default VFD.
     */
    if (H5FD_sec2_init() == H5I_INVALID_HID) {
        HGOTO_ERROR(H5E_FUNC, H5E_CANTINIT, FAIL, "unable to load default VFD ID");
    }
done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*--------------------------------------------------------------------------
 * NAME
 *   H5_init_library -- Initialize library-global information
 * USAGE
 *    herr_t H5_init_library()
 *
 * RETURNS
 *    Non-negative on success/Negative on failure
 *
 * DESCRIPTION
 *    Initializes any library-global data or routines.
 *
 *--------------------------------------------------------------------------
 */
herr_t
H5_init_library(void)
{
    size_t i;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Run the library initialization routine, if it hasn't already run */
    if (H5_INIT_GLOBAL || H5_TERM_GLOBAL)
        HGOTO_DONE(SUCCEED);

    /* Set the 'library initialized' flag as early as possible, to avoid
     * possible re-entrancy.
     */
    H5_INIT_GLOBAL = true;

#ifdef H5_HAVE_PARALLEL
    {
        int mpi_initialized;
        int mpi_finalized;
        int mpi_code;

        MPI_Initialized(&mpi_initialized);
        MPI_Finalized(&mpi_finalized);

        /* add an attribute on MPI_COMM_SELF to call H5_term_library
           when it is destroyed, i.e. on MPI_Finalize */
        if (mpi_initialized && !mpi_finalized) {
            int key_val;

            if (MPI_SUCCESS != (mpi_code = MPI_Comm_create_keyval(
                                    MPI_COMM_NULL_COPY_FN, (MPI_Comm_delete_attr_function *)H5__mpi_delete_cb,
                                    &key_val, NULL)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Comm_create_keyval failed", mpi_code)

            if (MPI_SUCCESS != (mpi_code = MPI_Comm_set_attr(MPI_COMM_SELF, key_val, NULL)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Comm_set_attr failed", mpi_code)

            if (MPI_SUCCESS != (mpi_code = MPI_Comm_free_keyval(&key_val)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Comm_free_keyval failed", mpi_code)
        }
    }
#endif /*H5_HAVE_PARALLEL*/

    /*
     * Make sure the package information is updated.
     */
    memset(&H5_debug_g, 0, sizeof H5_debug_g);
    H5_debug_g.pkg[H5_PKG_A].name  = "a";
    H5_debug_g.pkg[H5_PKG_AC].name = "ac";
    H5_debug_g.pkg[H5_PKG_B].name  = "b";
    H5_debug_g.pkg[H5_PKG_D].name  = "d";
    H5_debug_g.pkg[H5_PKG_E].name  = "e";
    H5_debug_g.pkg[H5_PKG_F].name  = "f";
    H5_debug_g.pkg[H5_PKG_G].name  = "g";
    H5_debug_g.pkg[H5_PKG_HG].name = "hg";
    H5_debug_g.pkg[H5_PKG_HL].name = "hl";
    H5_debug_g.pkg[H5_PKG_I].name  = "i";
    H5_debug_g.pkg[H5_PKG_M].name  = "m";
    H5_debug_g.pkg[H5_PKG_MF].name = "mf";
    H5_debug_g.pkg[H5_PKG_MM].name = "mm";
    H5_debug_g.pkg[H5_PKG_O].name  = "o";
    H5_debug_g.pkg[H5_PKG_P].name  = "p";
    H5_debug_g.pkg[H5_PKG_S].name  = "s";
    H5_debug_g.pkg[H5_PKG_T].name  = "t";
    H5_debug_g.pkg[H5_PKG_V].name  = "v";
    H5_debug_g.pkg[H5_PKG_VL].name = "vl";
    H5_debug_g.pkg[H5_PKG_Z].name  = "z";

    /*
     * Install atexit() library cleanup routines unless the H5dont_atexit()
     * has been called.  Once we add something to the atexit() list it stays
     * there permanently, so we set H5_dont_atexit_g after we add it to prevent
     * adding it again later if the library is closed and reopened.
     */
    if (!H5_dont_atexit_g) {

#if defined(H5_HAVE_THREADSAFE) && defined(H5_HAVE_WIN_THREADS)
        /* Clean up Win32 thread resources. Pthreads automatically cleans up.
         * This must be entered before the library cleanup code so it's
         * executed in LIFO order (i.e., last).
         */
        (void)atexit(H5TS_win32_process_exit);
#endif /* H5_HAVE_THREADSAFE && H5_HAVE_WIN_THREADS */

        /* Normal library termination code */
        (void)atexit(H5_term_library);

        H5_dont_atexit_g = true;
    } /* end if */

    /*
     * Initialize interfaces that might not be able to initialize themselves
     * soon enough.  The file & dataset interfaces must be initialized because
     * calling H5P_create() might require the file/dataset property classes to be
     * initialized.  The property interface must be initialized before the file
     * & dataset interfaces though, in order to provide them with the proper
     * property classes.
     * The link interface needs to be initialized so that link property lists
     * have their properties registered.
     * The FS module needs to be initialized as a result of the fix for HDFFV-10160:
     *   It might not be initialized during normal file open.
     *   When the application does not close the file, routines in the module might
     *   be called via H5_term_library() when shutting down the file.
     * The dataspace interface needs to be initialized so that future IDs for
     *   dataspaces work.
     */
    {
        /* clang-format off */
        struct {
            herr_t (*func)(void);
            const char *descr;
        } initializer[] = {
            {H5E_init, "error"}
        ,   {H5VL_init_phase1, "VOL"}
        ,   {H5SL_init, "skip lists"}
        ,   {H5FD_init, "VFD"}
        ,   {H5_default_vfd_init, "default VFD"}
        ,   {H5P_init_phase1, "property list"}
        ,   {H5AC_init, "metadata caching"}
        ,   {H5L_init, "link"}
        ,   {H5S_init, "dataspace"}
        ,   {H5PL_init, "plugins"}
        /* Finish initializing interfaces that depend on the interfaces above */
        ,   {H5P_init_phase2, "property list"}
        ,   {H5VL_init_phase2, "VOL"}
        };

        for (i = 0; i < NELMTS(initializer); i++) {
            if (initializer[i].func() < 0) {
                HGOTO_ERROR(H5E_FUNC, H5E_CANTINIT, FAIL,
                    "unable to initialize %s interface", initializer[i].descr);
            }
        }
        /* clang-format on */
    }

    /* Debugging? */
    H5__debug_mask("-all");
    H5__debug_mask(getenv("HDF5_DEBUG"));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_init_library() */

/*-------------------------------------------------------------------------
 * Function:    H5_term_library
 *
 * Purpose:    Terminate interfaces in a well-defined order due to
 *        dependencies among the interfaces, then terminate
 *        library-specific data.
 *
 * Return:    void
 *
 *-------------------------------------------------------------------------
 */
void
H5_term_library(void)
{
    int         pending, ntries   = 0;
    char        loop[1024], *next = loop;
    size_t      i;
    size_t      nleft = sizeof(loop);
    int         nprinted;
    H5E_auto2_t func;

#ifdef H5_HAVE_THREADSAFE
    /* explicit locking of the API */
    H5_FIRST_THREAD_INIT
    H5_API_LOCK
#endif

    /* Don't do anything if the library is already closed */
    if (!(H5_INIT_GLOBAL))
        goto done;

    /* Indicate that the library is being shut down */
    H5_TERM_GLOBAL = true;

    /* Push the API context without checking for errors */
    H5CX_push_special();

    /* Check if we should display error output */
    (void)H5Eget_auto2(H5E_DEFAULT, &func, NULL);

    /* Iterate over the list of 'atclose' callbacks that have been registered */
    if (H5_atclose_head) {
        H5_atclose_node_t *curr_atclose; /* Current 'atclose' node */

        /* Iterate over all 'atclose' nodes, making callbacks */
        curr_atclose = H5_atclose_head;
        while (curr_atclose) {
            H5_atclose_node_t *tmp_atclose; /* Temporary pointer to 'atclose' node */

            /* Invoke callback, providing context */
            (*curr_atclose->func)(curr_atclose->ctx);

            /* Advance to next node and free this one */
            tmp_atclose  = curr_atclose;
            curr_atclose = curr_atclose->next;
            H5FL_FREE(H5_atclose_node_t, tmp_atclose);
        } /* end while */

        /* Reset list head, in case library is re-initialized */
        H5_atclose_head = NULL;
    } /* end if */

    /* clang-format off */

    /*
     * Terminate each interface. The termination functions return a positive
     * value if they do something that might affect some other interface in a
     * way that would necessitate some cleanup work in the other interface.
     */

    {
#define TERMINATOR(module, wait) {          \
          .func = H5##module##_term_package \
        , .name = #module                   \
        , .completed = false                \
        , .await_prior = wait               \
        }

        /*
         * Termination is ordered by the `terminator` table so the "higher" level
         * packages are shut down before "lower" level packages that they
         * rely on:
         */
        struct {
            int (*func)(void);       /* function to terminate the module; returns 0
                                      * on success, >0 if termination was not
                                      * completed and we should try to terminate
                                      * some dependent modules, first.
                                      */
            const char *name;        /* name of the module */
            bool     completed;   /* true iff this terminator was already
                                      * completed
                                      */
            const bool await_prior;  /* true iff all prior terminators in the
                                         * list must complete before this
                                         * terminator is attempted
                                         */
        } terminator[] = {
            /* Close the event sets first, so that all asynchronous operations
             * complete before anything else attempts to shut down.
             */
            TERMINATOR(ES, false)
            /* Do not attempt to close down package L until after event sets
             * have finished closing down.
             */
        ,   TERMINATOR(L, true)
            /* Close the "top" of various interfaces (IDs, etc) but don't shut
             * down the whole interface yet, so that the object header messages
             * get serialized correctly for entries in the metadata cache and the
             * symbol table entry in the superblock gets serialized correctly, etc.
             * all of which is performed in the 'F' shutdown.
             *
             * The tops of packages A, D, G, M, S, T do not need to wait for L
             * or previous packages to finish closing down.
             */
        ,   TERMINATOR(A_top, false)
        ,   TERMINATOR(D_top, false)
        ,   TERMINATOR(G_top, false)
        ,   TERMINATOR(M_top, false)
        ,   TERMINATOR(S_top, false)
        ,   TERMINATOR(T_top, false)
            /* Don't shut down the file code until objects in files are shut down */
        ,   TERMINATOR(F, true)
            /* Don't shut down the property list code until all objects that might
             * use property lists are shut down
             */
        ,   TERMINATOR(P, true)
            /* Wait to shut down the "bottom" of various interfaces until the
             * files are closed, so pieces of the file can be serialized
             * correctly.
             *
             * Shut down the "bottom" of the attribute, dataset, group,
             * reference, dataspace, and datatype interfaces, fully closing
             * out the interfaces now.
             */
        ,   TERMINATOR(A, true)
        ,   TERMINATOR(D, false)
        ,   TERMINATOR(G, false)
        ,   TERMINATOR(M, false)
        ,   TERMINATOR(S, false)
        ,   TERMINATOR(T, false)
            /* Wait to shut down low-level packages like AC until after
             * the preceding high-level packages have shut down.  This prevents
             * low-level objects from closing "out from underneath" their
             * reliant high-level objects.
             */
        ,   TERMINATOR(AC, true)
            /* Shut down the "pluggable" interfaces, before the plugin framework */
        ,   TERMINATOR(Z, false)
        ,   TERMINATOR(FD, false)
        ,   TERMINATOR(VL, false)
            /* Don't shut down the plugin code until all "pluggable" interfaces
             * (Z, FD, PL) are shut down
             */
        ,   TERMINATOR(PL, true)
            /* Shut down the following packages in strictly the order given
             * by the table.
             */
        ,   TERMINATOR(E, true)
        ,   TERMINATOR(I, true)
        ,   TERMINATOR(SL, true)
        ,   TERMINATOR(FL, true)
        ,   TERMINATOR(CX, true)
        };

        do {
            pending = 0;
            for (i = 0; i < NELMTS(terminator); i++) {
                if (terminator[i].completed)
                    continue;
                if (pending != 0 && terminator[i].await_prior)
                    break;
                if (terminator[i].func() == 0) {
                    terminator[i].completed = true;
                    continue;
                }

                /* log a package when its terminator needs to be retried */
                pending++;
                nprinted = snprintf(next, nleft, "%s%s",
                    (next != loop) ? "," : "", terminator[i].name);
                if (nprinted < 0)
                    continue;
                if ((size_t)nprinted >= nleft)
                    nprinted = snprintf(next, nleft, "...");
                if (nprinted < 0 || (size_t)nprinted >= nleft)
                    continue;
                nleft -= (size_t)nprinted;
                next += nprinted;
            }
        } while (pending && ntries++ < 100);

        /* clang-format on */

        if (pending) {
            /* Only display the error message if the user is interested in them. */
            if (func) {
                fprintf(stderr, "HDF5: infinite loop closing library\n");
                fprintf(stderr, "      %s\n", loop);
#ifndef NDEBUG
                HDabort();
#endif        /* NDEBUG */
            } /* end if */
        }     /* end if */
    }

    /* Free open debugging streams */
    while (H5_debug_g.open_stream) {
        H5_debug_open_stream_t *tmp_open_stream;

        tmp_open_stream = H5_debug_g.open_stream;
        (void)fclose(H5_debug_g.open_stream->stream);
        H5_debug_g.open_stream = H5_debug_g.open_stream->next;
        (void)H5MM_free(tmp_open_stream);
    } /* end while */

    /* Reset flag indicating that the library is being shut down */
    H5_TERM_GLOBAL = false;

    /* Mark library as closed */
    H5_INIT_GLOBAL = false;

    /* Don't pop the API context (i.e. H5CX_pop), since it's been shut down already */

done:
#ifdef H5_HAVE_THREADSAFE
    H5_API_UNLOCK
#endif /* H5_HAVE_THREADSAFE */

    return;
} /* end H5_term_library() */

/*-------------------------------------------------------------------------
 * Function:    H5dont_atexit
 *
 * Purpose:    Indicates that the library is not to clean up after itself
 *        when the application exits by calling exit() or returning
 *        from main().  This function must be called before any other
 *        HDF5 function or constant is used or it will have no effect.
 *
 *        If this function is used then certain memory buffers will not
 *        be de-allocated nor will open files be flushed automatically.
 *        The application may still call H5close() explicitly to
 *        accomplish these things.
 *
 * Return:    Success:    non-negative
 *
 *        Failure:    negative if this function is called more than
 *                once or if it is called too late.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5dont_atexit(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API_NOINIT_NOERR_NOFS
    H5TRACE0("e", "");

    if (H5_dont_atexit_g)
        ret_value = FAIL;
    else
        H5_dont_atexit_g = true;

    FUNC_LEAVE_API_NOFS(ret_value)
} /* end H5dont_atexit() */

/*-------------------------------------------------------------------------
 * Function:    H5garbage_collect
 *
 * Purpose:    Walks through all the garbage collection routines for the
 *        library, which are supposed to free any unused memory they have
 *        allocated.
 *
 *      These should probably be registered dynamically in a linked list of
 *          functions to call, but there aren't that many right now, so we
 *          hard-wire them...
 *
 * Return:    Success:    non-negative
 *
 *        Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5garbage_collect(void)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE0("e", "");

    /* Call the garbage collection routines in the library */
    if (H5FL_garbage_coll() < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "can't garbage collect objects");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5garbage_collect() */

/*-------------------------------------------------------------------------
 * Function:    H5set_free_list_limits
 *
 * Purpose:    Sets limits on the different kinds of free lists.  Setting a value
 *      of -1 for a limit means no limit of that type.  These limits are global
 *      for the entire library.  Each "global" limit only applies to free lists
 *      of that type, so if an application sets a limit of 1 MB on each of the
 *      global lists, up to 3 MB of total storage might be allocated (1MB on
 *      each of regular, array and block type lists).
 *
 *      The settings for block free lists are duplicated to factory free lists.
 *      Factory free list limits cannot be set independently currently.
 *
 * Parameters:
 *  int reg_global_lim;  IN: The limit on all "regular" free list memory used
 *  int reg_list_lim;    IN: The limit on memory used in each "regular" free list
 *  int arr_global_lim;  IN: The limit on all "array" free list memory used
 *  int arr_list_lim;    IN: The limit on memory used in each "array" free list
 *  int blk_global_lim;  IN: The limit on all "block" free list memory used
 *  int blk_list_lim;    IN: The limit on memory used in each "block" free list
 *
 * Return:    Success:    non-negative
 *
 *        Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5set_free_list_limits(int reg_global_lim, int reg_list_lim, int arr_global_lim, int arr_list_lim,
                       int blk_global_lim, int blk_list_lim)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "IsIsIsIsIsIs", reg_global_lim, reg_list_lim, arr_global_lim, arr_list_lim, blk_global_lim,
             blk_list_lim);

    /* Call the free list function to actually set the limits */
    if (H5FL_set_free_list_limits(reg_global_lim, reg_list_lim, arr_global_lim, arr_list_lim, blk_global_lim,
                                  blk_list_lim, blk_global_lim, blk_list_lim) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTSET, FAIL, "can't set garbage collection limits");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5set_free_list_limits() */

/*-------------------------------------------------------------------------
 * Function:    H5get_free_list_sizes
 *
 * Purpose:    Gets the current size of the different kinds of free lists that
 *    the library uses to manage memory.  The free list sizes can be set with
 *    H5set_free_list_limits and garbage collected with H5garbage_collect.
 *      These lists are global for the entire library.
 *
 * Parameters:
 *  size_t *reg_size;    OUT: The current size of all "regular" free list memory used
 *  size_t *arr_size;    OUT: The current size of all "array" free list memory used
 *  size_t *blk_size;    OUT: The current size of all "block" free list memory used
 *  size_t *fac_size;    OUT: The current size of all "factory" free list memory used
 *
 * Return:    Success:    non-negative
 *        Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5get_free_list_sizes(size_t *reg_size /*out*/, size_t *arr_size /*out*/, size_t *blk_size /*out*/,
                      size_t *fac_size /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "xxxx", reg_size, arr_size, blk_size, fac_size);

    /* Call the free list function to actually get the sizes */
    if (H5FL_get_free_list_sizes(reg_size, arr_size, blk_size, fac_size) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "can't get garbage collection sizes");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5get_free_list_sizes() */

/*-------------------------------------------------------------------------
 * Function:    H5__debug_mask
 *
 * Purpose:     Set runtime debugging flags according to the string S.  The
 *              string should contain file numbers and package names
 *              separated by other characters. A file number applies to all
 *              following package names up to the next file number. The
 *              initial file number is `2' (the standard error stream). Each
 *              package name can be preceded by a `+' or `-' to add or remove
 *              the package from the debugging list (`+' is the default). The
 *              special name `all' means all packages.
 *
 *              The name `trace' indicates that API tracing is to be turned
 *              on or off.
 *
 *              The name 'ttop' indicates that only top-level API calls
 *              should be shown. This also turns on tracing as if the
 *              'trace' word was shown.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
static void
H5__debug_mask(const char *s)
{
    FILE  *stream = stderr;
    char   pkg_name[32], *rest;
    size_t i;
    bool   clear;

    while (s && *s) {

        if (isalpha(*s) || '-' == *s || '+' == *s) {

            /* Enable or Disable debugging? */
            if ('-' == *s) {
                clear = true;
                s++;
            }
            else if ('+' == *s) {
                clear = false;
                s++;
            }
            else {
                clear = false;
            } /* end if */

            /* Get the name */
            for (i = 0; isalpha(*s); i++, s++)
                if (i < sizeof pkg_name)
                    pkg_name[i] = *s;
            pkg_name[MIN(sizeof(pkg_name) - 1, i)] = '\0';

            /* Trace, all, or one? */
            if (!strcmp(pkg_name, "trace")) {
                H5_debug_g.trace = clear ? NULL : stream;
            }
            else if (!strcmp(pkg_name, "ttop")) {
                H5_debug_g.trace = stream;
                H5_debug_g.ttop  = (bool)!clear;
            }
            else if (!strcmp(pkg_name, "ttimes")) {
                H5_debug_g.trace  = stream;
                H5_debug_g.ttimes = (bool)!clear;
            }
            else if (!strcmp(pkg_name, "all")) {
                for (i = 0; i < (size_t)H5_NPKGS; i++)
                    H5_debug_g.pkg[i].stream = clear ? NULL : stream;
            }
            else {
                for (i = 0; i < (size_t)H5_NPKGS; i++) {
                    if (!strcmp(H5_debug_g.pkg[i].name, pkg_name)) {
                        H5_debug_g.pkg[i].stream = clear ? NULL : stream;
                        break;
                    } /* end if */
                }     /* end for */
                if (i >= (size_t)H5_NPKGS)
                    fprintf(stderr, "HDF5_DEBUG: ignored %s\n", pkg_name);
            } /* end if-else */
        }
        else if (isdigit(*s)) {
            int                     fd = (int)strtol(s, &rest, 0);
            H5_debug_open_stream_t *open_stream;

            if ((stream = HDfdopen(fd, "w")) != NULL) {
                (void)HDsetvbuf(stream, NULL, _IOLBF, (size_t)0);

                if (NULL ==
                    (open_stream = (H5_debug_open_stream_t *)H5MM_malloc(sizeof(H5_debug_open_stream_t)))) {
                    (void)fclose(stream);
                    return;
                } /* end if */

                open_stream->stream    = stream;
                open_stream->next      = H5_debug_g.open_stream;
                H5_debug_g.open_stream = open_stream;
            } /* end if */

            s = rest;
        }
        else {
            s++;
        } /* end if-else */
    }     /* end while */
} /* end H5__debug_mask() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:    H5__mpi_delete_cb
 *
 * Purpose:    Callback attribute on MPI_COMM_SELF to terminate the HDF5
 *              library when the communicator is destroyed, i.e. on MPI_Finalize.
 *
 * Return:    MPI_SUCCESS
 *
 *-------------------------------------------------------------------------
 */
static int
H5__mpi_delete_cb(MPI_Comm H5_ATTR_UNUSED comm, int H5_ATTR_UNUSED keyval, void H5_ATTR_UNUSED *attr_val,
                  int H5_ATTR_UNUSED *flag)
{
    H5_term_library();
    return MPI_SUCCESS;
}
#endif /*H5_HAVE_PARALLEL*/

/*-------------------------------------------------------------------------
 * Function:    H5get_libversion
 *
 * Purpose:    Returns the library version numbers through arguments. MAJNUM
 *        will be the major revision number of the library, MINNUM the
 *        minor revision number, and RELNUM the release revision number.
 *
 * Note:    When printing an HDF5 version number it should be printed as
 *
 *         printf("%u.%u.%u", maj, min, rel)        or
 *        printf("version %u.%u release %u", maj, min, rel)
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5get_libversion(unsigned *majnum /*out*/, unsigned *minnum /*out*/, unsigned *relnum /*out*/)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "xxx", majnum, minnum, relnum);

    /* Set the version information */
    if (majnum)
        *majnum = H5_VERS_MAJOR;
    if (minnum)
        *minnum = H5_VERS_MINOR;
    if (relnum)
        *relnum = H5_VERS_RELEASE;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5get_libversion() */

/*-------------------------------------------------------------------------
 * Function:    H5check_version
 *
 * Purpose:    Verifies that the arguments match the version numbers
 *        compiled into the library.  This function is intended to be
 *        called from user to verify that the versions of header files
 *        compiled into the application match the version of the hdf5
 *        library.
 *        Within major.minor.release version, the expectation
 *        is that all release versions are compatible, exceptions to
 *        this rule must be added to the VERS_RELEASE_EXCEPTIONS list.
 *
 * Return:    Success:    SUCCEED
 *
 *        Failure:    abort()
 *
 *-------------------------------------------------------------------------
 */
#define VERSION_MISMATCH_WARNING                                                                             \
    "Warning! ***HDF5 library version mismatched error***\n"                                                 \
    "The HDF5 header files used to compile this application do not match\n"                                  \
    "the version used by the HDF5 library to which this application is linked.\n"                            \
    "Data corruption or segmentation faults may occur if the application continues.\n"                       \
    "This can happen when an application was compiled by one version of HDF5 but\n"                          \
    "linked with a different version of static or shared HDF5 library.\n"                                    \
    "You should recompile the application or check your shared library related\n"                            \
    "settings such as 'LD_LIBRARY_PATH'.\n"
#define RELEASE_MISMATCH_WARNING                                                                             \
    "Warning! ***HDF5 library release mismatched error***\n"                                                 \
    "The HDF5 header files used to compile this application are not compatible with\n"                       \
    "the version used by the HDF5 library to which this application is linked.\n"                            \
    "Data corruption or segmentation faults may occur if the application continues.\n"                       \
    "This can happen when an application was compiled by one version of HDF5 but\n"                          \
    "linked with an incompatible version of static or shared HDF5 library.\n"                                \
    "You should recompile the application or check your shared library related\n"                            \
    "settings such as 'LD_LIBRARY_PATH'.\n"

herr_t
H5check_version(unsigned majnum, unsigned minnum, unsigned relnum)
{
    char                lib_str[256];
    char                substr[]                 = H5_VERS_SUBRELEASE;
    static int          checked                  = 0; /* If we've already checked the version info */
    static unsigned int disable_version_check    = 0; /* Set if the version check should be disabled */
    static const char  *version_mismatch_warning = VERSION_MISMATCH_WARNING;
    static const char  *release_mismatch_warning = RELEASE_MISMATCH_WARNING;
    herr_t              ret_value                = SUCCEED; /* Return value */

    FUNC_ENTER_API_NOINIT_NOERR_NOFS
    H5TRACE3("e", "IuIuIu", majnum, minnum, relnum);

    /* Don't check again, if we already have */
    if (checked)
        HGOTO_DONE(SUCCEED);

    {
        const char *s; /* Environment string for disabling version check */

        /* Allow different versions of the header files and library? */
        s = getenv("HDF5_DISABLE_VERSION_CHECK");

        if (s && isdigit(*s))
            disable_version_check = (unsigned int)strtol(s, NULL, 0);
    }

    /* H5_VERS_MAJOR and H5_VERS_MINOR must match */
    if (H5_VERS_MAJOR != majnum || H5_VERS_MINOR != minnum) {
        switch (disable_version_check) {
            case 0:
                fprintf(stderr, "%s%s", version_mismatch_warning,
                        "You can, at your own risk, disable this warning by setting the environment\n"
                        "variable 'HDF5_DISABLE_VERSION_CHECK' to a value of '1'.\n"
                        "Setting it to 2 or higher will suppress the warning messages totally.\n");
                /* Mention the versions we are referring to */
                fprintf(stderr, "Headers are %u.%u.%u, library is %u.%u.%u\n", majnum, minnum, relnum,
                        (unsigned)H5_VERS_MAJOR, (unsigned)H5_VERS_MINOR, (unsigned)H5_VERS_RELEASE);
                /* Show library build settings if available */
                fprintf(stderr, "%s", H5build_settings);

                /* Bail out now. */
                fputs("Bye...\n", stderr);
                HDabort();
            case 1:
                /* continue with a warning */
                /* Note that the warning message is embedded in the format string.*/
                fprintf(stderr,
                        "%s'HDF5_DISABLE_VERSION_CHECK' "
                        "environment variable is set to %d, application will\n"
                        "continue at your own risk.\n",
                        version_mismatch_warning, disable_version_check);
                /* Mention the versions we are referring to */
                fprintf(stderr, "Headers are %u.%u.%u, library is %u.%u.%u\n", majnum, minnum, relnum,
                        (unsigned)H5_VERS_MAJOR, (unsigned)H5_VERS_MINOR, (unsigned)H5_VERS_RELEASE);
                /* Show library build settings if available */
                fprintf(stderr, "%s", H5build_settings);
                break;
            default:
                /* 2 or higher: continue silently */
                break;
        } /* end switch */

    } /* end if (H5_VERS_MAJOR != majnum || H5_VERS_MINOR != minnum) */

    /* H5_VERS_RELEASE should be compatible, we will only add checks for exceptions */
    /* Library develop release versions are incompatible by design */
    if (H5_VERS_RELEASE != relnum) {
        for (unsigned i = 0; i < VERS_RELEASE_EXCEPTIONS_SIZE; i++) {
            /* Check for incompatible headers or incompatible library */
            if (VERS_RELEASE_EXCEPTIONS[i] == relnum || VERS_RELEASE_EXCEPTIONS[i] == H5_VERS_RELEASE) {
                switch (disable_version_check) {
                    case 0:
                        fprintf(stderr, "%s%s", release_mismatch_warning,
                                "You can, at your own risk, disable this warning by setting the environment\n"
                                "variable 'HDF5_DISABLE_VERSION_CHECK' to a value of '1'.\n"
                                "Setting it to 2 or higher will suppress the warning messages totally.\n");
                        /* Mention the versions we are referring to */
                        fprintf(stderr, "Headers are %u.%u.%u, library is %u.%u.%u\n", majnum, minnum, relnum,
                                (unsigned)H5_VERS_MAJOR, (unsigned)H5_VERS_MINOR, (unsigned)H5_VERS_RELEASE);

                        /* Bail out now. */
                        fputs("Bye...\n", stderr);
                        HDabort();
                    case 1:
                        /* continue with a warning */
                        /* Note that the warning message is embedded in the format string.*/
                        fprintf(stderr,
                                "%s'HDF5_DISABLE_VERSION_CHECK' "
                                "environment variable is set to %d, application will\n"
                                "continue at your own risk.\n",
                                release_mismatch_warning, disable_version_check);
                        /* Mention the versions we are referring to */
                        fprintf(stderr, "Headers are %u.%u.%u, library is %u.%u.%u\n", majnum, minnum, relnum,
                                (unsigned)H5_VERS_MAJOR, (unsigned)H5_VERS_MINOR, (unsigned)H5_VERS_RELEASE);
                        break;
                    default:
                        /* 2 or higher: continue silently */
                        break;
                } /* end switch */

            } /* end if */

        } /* end for */

    } /* end if (H5_VERS_RELEASE != relnum) */

    /* Indicate that the version check has been performed */
    checked = 1;

    if (!disable_version_check) {
        /*
         * Verify if H5_VERS_INFO is consistent with the other version information.
         * Check only the first sizeof(lib_str) char.  Assume the information
         * will fit within this size or enough significance.
         */
        snprintf(lib_str, sizeof(lib_str), "HDF5 library version: %d.%d.%d%s%s", H5_VERS_MAJOR, H5_VERS_MINOR,
                 H5_VERS_RELEASE, (*substr ? "-" : ""), substr);

        if (strcmp(lib_str, H5_lib_vers_info_g) != 0) {
            fputs("Warning!  Library version information error.\n"
                  "The HDF5 library version information are not "
                  "consistent in its source code.\nThis is NOT a fatal error "
                  "but should be corrected.  Setting the environment\n"
                  "variable 'HDF5_DISABLE_VERSION_CHECK' to a value of 1 "
                  "will suppress\nthis warning.\n",
                  stderr);
            fprintf(stderr,
                    "Library version information are:\n"
                    "H5_VERS_MAJOR=%d, H5_VERS_MINOR=%d, H5_VERS_RELEASE=%d, "
                    "H5_VERS_SUBRELEASE=%s,\nH5_VERS_INFO=%s\n",
                    H5_VERS_MAJOR, H5_VERS_MINOR, H5_VERS_RELEASE, H5_VERS_SUBRELEASE, H5_VERS_INFO);
        } /* end if */
    }

done:
    FUNC_LEAVE_API_NOFS(ret_value)
} /* end H5check_version() */

/*-------------------------------------------------------------------------
 * Function:    H5open
 *
 * Purpose:     Initialize the library.  This is normally called
 *              automatically, but if you find that an HDF5 library function
 *              is failing inexplicably, then try calling this function
 *              first.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5open(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API_NOPUSH(FAIL)
    /*NO TRACE*/

    /* all work is done by FUNC_ENTER() */

done:
    FUNC_LEAVE_API_NOPUSH(ret_value)
} /* end H5open() */

/*-------------------------------------------------------------------------
 * Function:    H5atclose
 *
 * Purpose:    Register a callback for the library to invoke when it's
 *        closing.  Callbacks are invoked in LIFO order.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5atclose(H5_atclose_func_t func, void *ctx)
{
    H5_atclose_node_t *new_atclose;         /* New 'atclose' node */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "Hc*x", func, ctx);

    /* Check arguments */
    if (NULL == func)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL func pointer");

    /* Allocate space for the 'atclose' node */
    if (NULL == (new_atclose = H5FL_MALLOC(H5_atclose_node_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate 'atclose' node");

    /* Set up 'atclose' node */
    new_atclose->func = func;
    new_atclose->ctx  = ctx;

    /* Connector to linked-list of 'atclose' nodes */
    new_atclose->next = H5_atclose_head;
    H5_atclose_head   = new_atclose;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5atclose() */

/*-------------------------------------------------------------------------
 * Function:    H5close
 *
 * Purpose:    Terminate the library and release all resources.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5close(void)
{
    /*
     * Don't call normal FUNC_ENTER() since we don't want to initialize the
     * whole library just to release it all right away.  It is safe to call
     * this function for an uninitialized library.
     */
    FUNC_ENTER_API_NOINIT_NOERR_NOFS
    H5TRACE0("e", "");

    H5_term_library();

    FUNC_LEAVE_API_NOFS(SUCCEED)
} /* end H5close() */

/*-------------------------------------------------------------------------
 * Function:    H5allocate_memory
 *
 * Purpose:        Allocate a memory buffer with the semantics of malloc().
 *
 *              NOTE: This function is intended for use with filter
 *              plugins so that all allocation and free operations
 *              use the same memory allocator. It is not intended for
 *              use as a general memory allocator in applications.
 *
 * Parameters:
 *
 *      size:   The size of the buffer.
 *
 *      clear:  Whether or not to memset the buffer to 0.
 *
 * Return:
 *
 *      Success:    A pointer to the allocated buffer or NULL if the size
 *                  parameter is zero.
 *
 *      Failure:    NULL (but may also be NULL w/ size 0!)
 *
 *-------------------------------------------------------------------------
 */
void *H5_ATTR_MALLOC
H5allocate_memory(size_t size, bool clear)
{
    void *ret_value = NULL;

    FUNC_ENTER_API_NOINIT
    H5TRACE2("*x", "zb", size, clear);

    if (0 == size)
        return NULL;

    if (clear)
        ret_value = H5MM_calloc(size);
    else
        ret_value = H5MM_malloc(size);

    FUNC_LEAVE_API_NOINIT(ret_value)
} /* end H5allocate_memory() */

/*-------------------------------------------------------------------------
 * Function:    H5resize_memory
 *
 * Purpose:        Resize a memory buffer with the semantics of realloc().
 *
 *              NOTE: This function is intended for use with filter
 *              plugins so that all allocation and free operations
 *              use the same memory allocator. It is not intended for
 *              use as a general memory allocator in applications.
 *
 * Parameters:
 *
 *      mem:    The buffer to be resized.
 *
 *      size:   The size of the buffer.
 *
 * Return:
 *
 *      Success:    A pointer to the resized buffer.
 *
 *      Failure:    NULL (the input buffer will be unchanged)
 *
 *-------------------------------------------------------------------------
 */
void *
H5resize_memory(void *mem, size_t size)
{
    void *ret_value = NULL;

    FUNC_ENTER_API_NOINIT
    H5TRACE2("*x", "*xz", mem, size);

    ret_value = H5MM_realloc(mem, size);

    FUNC_LEAVE_API_NOINIT(ret_value)
} /* end H5resize_memory() */

/*-------------------------------------------------------------------------
 * Function:    H5free_memory
 *
 * Purpose:        Frees memory allocated by the library that it is the user's
 *              responsibility to free.  Ensures that the same library
 *              that was used to allocate the memory frees it.  Passing
 *              NULL pointers is allowed.
 *
 * Return:        SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5free_memory(void *mem)
{
    FUNC_ENTER_API_NOINIT
    H5TRACE1("e", "*x", mem);

    /* At this time, it is impossible for this to fail. */
    H5MM_xfree(mem);

    FUNC_LEAVE_API_NOINIT(SUCCEED)
} /* end H5free_memory() */

/*-------------------------------------------------------------------------
 * Function:    H5is_library_threadsafe
 *
 * Purpose:        Checks to see if the library was built with thread-safety
 *              enabled.
 *
 * Return:        SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5is_library_threadsafe(bool *is_ts /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API_NOINIT
    H5TRACE1("e", "x", is_ts);

    if (is_ts) {
#ifdef H5_HAVE_THREADSAFE
        *is_ts = true;
#else  /* H5_HAVE_THREADSAFE */
        *is_ts = false;
#endif /* H5_HAVE_THREADSAFE */
    }
    else
        ret_value = FAIL;

    FUNC_LEAVE_API_NOINIT(ret_value)
} /* end H5is_library_threadsafe() */

/*-------------------------------------------------------------------------
 * Function:    H5is_library_terminating
 *
 * Purpose:    Checks to see if the library is shutting down.
 *
 * Note:    Useful for plugins to detect when the library is terminating.
 *        For example, a VOL connector could check if a "file close"
 *        callback was the result of the library shutdown process, or
 *        an API action from the application.
 *
 * Return:    SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5is_library_terminating(bool *is_terminating /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API_NOINIT
    H5TRACE1("e", "x", is_terminating);

    assert(is_terminating);

    if (is_terminating)
        *is_terminating = H5_TERM_GLOBAL;
    else
        ret_value = FAIL;

    FUNC_LEAVE_API_NOINIT(ret_value)
} /* end H5is_library_terminating() */

#if defined(H5_HAVE_THREADSAFE) && defined(H5_BUILT_AS_DYNAMIC_LIB) && defined(H5_HAVE_WIN32_API) &&         \
    defined(H5_HAVE_WIN_THREADS)
/*-------------------------------------------------------------------------
 * Function:    DllMain
 *
 * Purpose:     Handles various conditions in the library on Windows.
 *
 *    NOTE:     The main purpose of this is for handling Win32 thread cleanup
 *              on thread/process detach.
 *
 *              Only enabled when the shared Windows library is built with
 *              thread safety enabled.
 *
 * Return:      true on success, false on failure
 *
 *-------------------------------------------------------------------------
 */
BOOL WINAPI
DllMain(_In_ HINSTANCE hinstDLL, _In_ DWORD fdwReason, _In_ LPVOID lpvReserved)
{
    /* Don't add our function enter/leave macros since this function will be
     * called before the library is initialized.
     *
     * NOTE: Do NOT call any CRT functions in DllMain!
     * This includes any functions that are called by from here!
     */

    BOOL fOkay = true;

    switch (fdwReason) {
        case DLL_PROCESS_ATTACH:
            break;

        case DLL_PROCESS_DETACH:
            break;

        case DLL_THREAD_ATTACH:
#ifdef H5_HAVE_WIN_THREADS
            if (H5TS_win32_thread_enter() < 0)
                fOkay = false;
#endif /* H5_HAVE_WIN_THREADS */
            break;

        case DLL_THREAD_DETACH:
#ifdef H5_HAVE_WIN_THREADS
            if (H5TS_win32_thread_exit() < 0)
                fOkay = false;
#endif /* H5_HAVE_WIN_THREADS */
            break;

        default:
            /* Shouldn't get here */
            fOkay = false;
            break;
    }

    return fOkay;
}
#endif /* H5_HAVE_WIN32_API && H5_BUILT_AS_DYNAMIC_LIB && H5_HAVE_WIN_THREADS && H5_HAVE_THREADSAFE*/
