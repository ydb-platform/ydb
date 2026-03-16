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

#include "H5Zmodule.h" /* This source code file is part of the H5Z module */

#include "H5private.h"   /* Generic Functions   */
#include "H5CXprivate.h" /* API Contexts        */
#include "H5Dprivate.h"  /* Dataset functions   */
#include "H5Eprivate.h"  /* Error handling      */
#include "H5Fprivate.h"  /* File                */
#include "H5Iprivate.h"  /* IDs                 */
#include "H5MMprivate.h" /* Memory management   */
#include "H5Oprivate.h"  /* Object headers      */
#include "H5Pprivate.h"  /* Property lists      */
#include "H5PLprivate.h" /* Plugins             */
#include "H5Sprivate.h"  /* Dataspace functions */
#include "H5Zpkg.h"      /* Data filters        */

#ifdef H5_HAVE_SZLIB_H
#error #include "szlib.h"
#endif

/* Local typedefs */
#ifdef H5Z_DEBUG
typedef struct H5Z_stats_t {
    struct {
        hsize_t       total;  /* total number of bytes processed */
        hsize_t       errors; /* bytes of total attributable to errors */
        H5_timevals_t times;  /* execution time including errors */
    } stats[2];               /* 0 = output, 1 = input */
} H5Z_stats_t;
#endif /* H5Z_DEBUG */

typedef struct H5Z_object_t {
    H5Z_filter_t filter_id; /* ID of the filter we're looking for */
    htri_t       found;     /* Whether we find an object using the filter */
#ifdef H5_HAVE_PARALLEL
    bool sanity_checked; /* Whether the sanity check for collectively calling H5Zunregister has been done */
#endif                   /* H5_HAVE_PARALLEL */
} H5Z_object_t;

/* Enumerated type for dataset creation prelude callbacks */
typedef enum {
    H5Z_PRELUDE_CAN_APPLY, /* Call "can apply" callback */
    H5Z_PRELUDE_SET_LOCAL  /* Call "set local" callback */
} H5Z_prelude_type_t;

/* Local variables */
static size_t        H5Z_table_alloc_g = 0;
static size_t        H5Z_table_used_g  = 0;
static H5Z_class2_t *H5Z_table_g       = NULL;
#ifdef H5Z_DEBUG
static H5Z_stats_t *H5Z_stat_table_g = NULL;
#endif /* H5Z_DEBUG */

/* Local functions */
static int H5Z__find_idx(H5Z_filter_t id);
static int H5Z__check_unregister_dset_cb(void *obj_ptr, hid_t obj_id, void *key);
static int H5Z__check_unregister_group_cb(void *obj_ptr, hid_t obj_id, void *key);
static int H5Z__flush_file_cb(void *obj_ptr, hid_t obj_id, void *key);

/*-------------------------------------------------------------------------
 * Function:    H5Z_init
 *
 * Purpose:     Initialize the interface from some other layer.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_init(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (H5_TERM_GLOBAL)
        HGOTO_DONE(SUCCEED);

    /* Internal filters */
    if (H5Z_register(H5Z_SHUFFLE) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to register shuffle filter");
    if (H5Z_register(H5Z_FLETCHER32) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to register fletcher32 filter");
    if (H5Z_register(H5Z_NBIT) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to register nbit filter");
    if (H5Z_register(H5Z_SCALEOFFSET) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to register scaleoffset filter");

        /* External filters */
#ifdef H5_HAVE_FILTER_DEFLATE
    if (H5Z_register(H5Z_DEFLATE) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to register deflate filter");
#endif /* H5_HAVE_FILTER_DEFLATE */
#ifdef H5_HAVE_FILTER_SZIP
    {
        int encoder_enabled = SZ_encoder_enabled();
        if (encoder_enabled < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "check for szip encoder failed");

        H5Z_SZIP->encoder_present = (unsigned)encoder_enabled;
        if (H5Z_register(H5Z_SZIP) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to register szip filter");
    }
#endif /* H5_HAVE_FILTER_SZIP */

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function: H5Z_term_package
 *
 * Purpose:  Terminate the H5Z layer.
 *
 * Return:   void
 *-------------------------------------------------------------------------
 */
int
H5Z_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

#ifdef H5Z_DEBUG
    char   comment[16], bandwidth[32];
    int    dir, nprint = 0;
    size_t i;

    if (H5DEBUG(Z)) {
        for (i = 0; i < H5Z_table_used_g; i++) {
            for (dir = 0; dir < 2; dir++) {
                struct {
                    char *user;
                    char *system;
                    char *elapsed;
                } timestrs = {H5_timer_get_time_string(H5Z_stat_table_g[i].stats[dir].times.user),
                              H5_timer_get_time_string(H5Z_stat_table_g[i].stats[dir].times.system),
                              H5_timer_get_time_string(H5Z_stat_table_g[i].stats[dir].times.elapsed)};
                if (0 == H5Z_stat_table_g[i].stats[dir].total)
                    goto next;

                if (0 == nprint++) {
                    /* Print column headers */
                    fprintf(H5DEBUG(Z), "H5Z: filter statistics "
                                        "accumulated over life of library:\n");
                    fprintf(H5DEBUG(Z), "   %-16s %10s %10s %8s %8s %8s %10s\n", "Filter", "Total", "Errors",
                            "User", "System", "Elapsed", "Bandwidth");
                    fprintf(H5DEBUG(Z), "   %-16s %10s %10s %8s %8s %8s %10s\n", "------", "-----", "------",
                            "----", "------", "-------", "---------");
                } /* end if */

                /* Truncate the comment to fit in the field */
                strncpy(comment, H5Z_table_g[i].name, sizeof comment);
                comment[sizeof(comment) - 1] = '\0';

                /*
                 * Format bandwidth to have four significant digits and
                 * units of `B/s', `kB/s', `MB/s', `GB/s', or `TB/s' or
                 * the word `Inf' if the elapsed time is zero.
                 */
                H5_bandwidth(bandwidth, sizeof(bandwidth), (double)(H5Z_stat_table_g[i].stats[dir].total),
                             H5Z_stat_table_g[i].stats[dir].times.elapsed);

                /* Print the statistics */
                fprintf(H5DEBUG(Z), "   %s%-15s %10" PRIdHSIZE " %10" PRIdHSIZE " %8s %8s %8s %10s\n",
                        (dir ? "<" : ">"), comment, H5Z_stat_table_g[i].stats[dir].total,
                        H5Z_stat_table_g[i].stats[dir].errors, timestrs.user, timestrs.system,
                        timestrs.elapsed, bandwidth);
next:
                free(timestrs.user);
                free(timestrs.system);
                free(timestrs.elapsed);
            } /* end for */
        }     /* end for */
    }         /* end if */
#endif        /* H5Z_DEBUG */

    /* Free the table of filters */
    if (H5Z_table_g) {
        H5Z_table_g = (H5Z_class2_t *)H5MM_xfree(H5Z_table_g);

#ifdef H5Z_DEBUG
        H5Z_stat_table_g = (H5Z_stats_t *)H5MM_xfree(H5Z_stat_table_g);
#endif /* H5Z_DEBUG */
        H5Z_table_used_g = H5Z_table_alloc_g = 0;

        n++;
    } /* end if */

    FUNC_LEAVE_NOAPI(n)
} /* end H5Z_term_package() */

/*-------------------------------------------------------------------------
 * Function: H5Zregister
 *
 * Purpose:  This function registers new filter.
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Zregister(const void *cls)
{
    const H5Z_class2_t *cls_real  = (const H5Z_class2_t *)cls; /* "Real" class pointer */
    herr_t              ret_value = SUCCEED;                   /* Return value */
#ifndef H5_NO_DEPRECATED_SYMBOLS
    H5Z_class2_t cls_new; /* Translated class struct */
#endif

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "*x", cls);

    /* Check args */
    if (cls_real == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid filter class");

    /* Check H5Z_class_t version number; this is where a function to convert
     * from an outdated version should be called.
     *
     * If the version number is invalid, we assume that the target of cls is the
     * old style "H5Z_class1_t" structure, which did not contain a version
     * field.  In this structure, the first field is the id.  Since both version
     * and id are integers they will have the same value, and since id must be
     * at least 256, there should be no overlap and the version of the struct
     * can be determined by the value of the first field.
     */
    if (cls_real->version != H5Z_CLASS_T_VERS) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
        /* Assume it is an old "H5Z_class1_t" instead */
        const H5Z_class1_t *cls_old = (const H5Z_class1_t *)cls;

        /* Translate to new H5Z_class2_t */
        cls_new.version         = H5Z_CLASS_T_VERS;
        cls_new.id              = cls_old->id;
        cls_new.encoder_present = 1;
        cls_new.decoder_present = 1;
        cls_new.name            = cls_old->name;
        cls_new.can_apply       = cls_old->can_apply;
        cls_new.set_local       = cls_old->set_local;
        cls_new.filter          = cls_old->filter;

        /* Set cls_real to point to the translated structure */
        cls_real = &cls_new;

#else  /* H5_NO_DEPRECATED_SYMBOLS */
        /* Deprecated symbols not allowed, throw an error */
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid H5Z_class_t version number");
#endif /* H5_NO_DEPRECATED_SYMBOLS */
    }  /* end if */

    if (cls_real->id < 0 || cls_real->id > H5Z_FILTER_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid filter identification number");
    if (cls_real->id < H5Z_FILTER_RESERVED)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to modify predefined filters");
    if (cls_real->filter == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no filter function specified");

    /* Do it */
    if (H5Z_register(cls_real) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to register filter");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function: H5Z_register
 *
 * Purpose:  Same as the public version except this one allows filters
 *           to be set for predefined method numbers < H5Z_FILTER_RESERVED
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_register(const H5Z_class2_t *cls)
{
    size_t i;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(cls);
    assert(cls->id >= 0 && cls->id <= H5Z_FILTER_MAX);

    /* Is the filter already registered? */
    for (i = 0; i < H5Z_table_used_g; i++)
        if (H5Z_table_g[i].id == cls->id)
            break;

    /* Filter not already registered */
    if (i >= H5Z_table_used_g) {
        if (H5Z_table_used_g >= H5Z_table_alloc_g) {
            size_t        n     = MAX(H5Z_MAX_NFILTERS, 2 * H5Z_table_alloc_g);
            H5Z_class2_t *table = (H5Z_class2_t *)H5MM_realloc(H5Z_table_g, n * sizeof(H5Z_class2_t));
#ifdef H5Z_DEBUG
            H5Z_stats_t *stat_table = (H5Z_stats_t *)H5MM_realloc(H5Z_stat_table_g, n * sizeof(H5Z_stats_t));
#endif /* H5Z_DEBUG */
            if (!table)
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to extend filter table");
            H5Z_table_g = table;
#ifdef H5Z_DEBUG
            if (!stat_table)
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to extend filter statistics table");
            H5Z_stat_table_g = stat_table;
#endif /* H5Z_DEBUG */
            H5Z_table_alloc_g = n;
        } /* end if */

        /* Initialize */
        i = H5Z_table_used_g++;
        H5MM_memcpy(H5Z_table_g + i, cls, sizeof(H5Z_class2_t));
#ifdef H5Z_DEBUG
        memset(H5Z_stat_table_g + i, 0, sizeof(H5Z_stats_t));
#endif /* H5Z_DEBUG */
    }  /* end if */
    /* Filter already registered */
    else {
        /* Replace old contents */
        H5MM_memcpy(H5Z_table_g + i, cls, sizeof(H5Z_class2_t));
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Zunregister
 *
 * Purpose:     This function unregisters a filter.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Zunregister(H5Z_filter_t id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "Zf", id);

    /* Check args */
    if (id < 0 || id > H5Z_FILTER_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid filter identification number");
    if (id < H5Z_FILTER_RESERVED)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to modify predefined filters");

    /* Do it */
    if (H5Z__unregister(id) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to unregister filter");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Zunregister() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__unregister
 *
 * Purpose:     Same as the public version except this one allows filters
 *               to be unset for predefined method numbers <H5Z_FILTER_RESERVED
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Z__unregister(H5Z_filter_t filter_id)
{
    size_t       filter_index;        /* Local index variable for filter */
    H5Z_object_t object;              /* Object to pass to callbacks */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(filter_id >= 0 && filter_id <= H5Z_FILTER_MAX);

    /* Is the filter already registered? */
    for (filter_index = 0; filter_index < H5Z_table_used_g; filter_index++)
        if (H5Z_table_g[filter_index].id == filter_id)
            break;

    /* Fail if filter not found */
    if (filter_index >= H5Z_table_used_g)
        HGOTO_ERROR(H5E_PLINE, H5E_NOTFOUND, FAIL, "filter is not registered");

    /* Initialize the structure object for iteration */
    object.filter_id = filter_id;
    object.found     = false;
#ifdef H5_HAVE_PARALLEL
    object.sanity_checked = false;
#endif /* H5_HAVE_PARALLEL */

    /* Iterate through all opened datasets, returns a failure if any of them uses the filter */
    if (H5I_iterate(H5I_DATASET, H5Z__check_unregister_dset_cb, &object, false) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "iteration failed");

    if (object.found)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTRELEASE, FAIL,
                    "can't unregister filter because a dataset is still using it");

    /* Iterate through all opened groups, returns a failure if any of them uses the filter */
    if (H5I_iterate(H5I_GROUP, H5Z__check_unregister_group_cb, &object, false) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "iteration failed");

    if (object.found)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTRELEASE, FAIL,
                    "can't unregister filter because a group is still using it");

    /* Iterate through all opened files and flush them */
    if (H5I_iterate(H5I_FILE, H5Z__flush_file_cb, &object, false) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_BADITER, FAIL, "iteration failed");

    /* Remove filter from table */
    /* Don't worry about shrinking table size (for now) */
    memmove(&H5Z_table_g[filter_index], &H5Z_table_g[filter_index + 1],
            sizeof(H5Z_class2_t) * ((H5Z_table_used_g - 1) - filter_index));
#ifdef H5Z_DEBUG
    memmove(&H5Z_stat_table_g[filter_index], &H5Z_stat_table_g[filter_index + 1],
            sizeof(H5Z_stats_t) * ((H5Z_table_used_g - 1) - filter_index));
#endif /* H5Z_DEBUG */
    H5Z_table_used_g--;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__unregister() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__check_unregister
 *
 * Purpose:     Check if an object uses the filter to be unregistered.
 *
 * Return:      true if the object uses the filter
 *              false if not
 *              NEGATIVE on error
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5Z__check_unregister(hid_t ocpl_id, H5Z_filter_t filter_id)
{
    H5P_genplist_t *plist;             /* Property list */
    htri_t          ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get the plist structure of object creation */
    if (NULL == (plist = H5P_object_verify(ocpl_id, H5P_OBJECT_CREATE)))
        HGOTO_ERROR(H5E_PLINE, H5E_BADID, FAIL, "can't find object for ID");

    /* Check if the object creation property list uses the filter */
    if ((ret_value = H5P_filter_in_pline(plist, filter_id)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't check filter in pipeline");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__check_unregister() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__check_unregister_group_cb
 *
 * Purpose:     The callback function for H5Z__unregister. It iterates
 *              through all opened objects.  If the object is a dataset
 *              or a group and it uses the filter to be unregistered, the
 *              function returns true.
 *
 * Return:      true if the object uses the filter
 *              false if not
 *              NEGATIVE on error
 *
 *-------------------------------------------------------------------------
 */
static int
H5Z__check_unregister_group_cb(void *obj_ptr, hid_t H5_ATTR_UNUSED obj_id, void *key)
{
    hid_t         ocpl_id         = -1;
    H5Z_object_t *object          = (H5Z_object_t *)key;
    htri_t        filter_in_pline = false;
    int           ret_value       = false; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(obj_ptr);

    /* Get the group creation property */
    if ((ocpl_id = H5G_get_create_plist((H5G_t *)obj_ptr)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't get group creation property list");

    /* Check if the filter is in the group creation property list */
    if ((filter_in_pline = H5Z__check_unregister(ocpl_id, object->filter_id)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't check filter in pipeline");

    /* H5I_iterate expects true to stop the loop over objects. Stop the loop and
     * let H5Z__unregister return failure.
     */
    if (filter_in_pline) {
        object->found = true;
        ret_value     = true;
    }

done:
    if (ocpl_id > 0)
        if (H5I_dec_app_ref(ocpl_id) < 0)
            HDONE_ERROR(H5E_PLINE, H5E_CANTDEC, FAIL, "can't release plist");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__check_unregister_group_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__check_unregister_dset_cb
 *
 * Purpose:     The callback function for H5Z__unregister. It iterates
 *              through all opened objects.  If the object is a dataset
 *              or a group and it uses the filter to be unregistered, the
 *              function returns true.
 *
 * Return:      true if the object uses the filter
 *              false if not
 *              NEGATIVE on error
 *
 *-------------------------------------------------------------------------
 */
static int
H5Z__check_unregister_dset_cb(void *obj_ptr, hid_t H5_ATTR_UNUSED obj_id, void *key)
{
    hid_t         ocpl_id         = -1;
    H5Z_object_t *object          = (H5Z_object_t *)key;
    htri_t        filter_in_pline = false;
    int           ret_value       = false; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(obj_ptr);

    /* Get the dataset creation property */
    if ((ocpl_id = H5D_get_create_plist((H5D_t *)obj_ptr)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't get dataset creation property list");

    /* Check if the filter is in the dataset creation property list */
    if ((filter_in_pline = H5Z__check_unregister(ocpl_id, object->filter_id)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't check filter in pipeline");

    /* H5I_iterate expects true to stop the loop over objects. Stop the loop and
     * let H5Z__unregister return failure.
     */
    if (filter_in_pline) {
        object->found = true;
        ret_value     = true;
    }

done:
    if (ocpl_id > 0)
        if (H5I_dec_app_ref(ocpl_id) < 0)
            HDONE_ERROR(H5E_PLINE, H5E_CANTDEC, FAIL, "can't release plist");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__check_unregister_dset_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__flush_file_cb
 *
 * Purpose:     The callback function for H5Z__unregister. It iterates
 *              through all opened files and flush them.
 *
 * Return:      NON-NEGATIVE if finishes flushing and moves on
 *              NEGATIVE if there is an error
 *-------------------------------------------------------------------------
 */
static int
H5Z__flush_file_cb(void *obj_ptr, hid_t H5_ATTR_UNUSED obj_id, void H5_ATTR_PARALLEL_USED *key)
{
    H5F_t *f = (H5F_t *)obj_ptr; /* File object for operations */
#ifdef H5_HAVE_PARALLEL
    H5Z_object_t *object = (H5Z_object_t *)key;
#endif                     /* H5_HAVE_PARALLEL */
    int ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(obj_ptr);
    assert(key);

    /* Do a global flush if the file is opened for write */
    if (H5F_ACC_RDWR & H5F_INTENT(f)) {

#ifdef H5_HAVE_PARALLEL
        /* Check if MPIO driver is used */
        if (H5F_HAS_FEATURE(f, H5FD_FEAT_HAS_MPI)) {
            /* Sanity check for collectively calling H5Zunregister, if requested */
            /* (Sanity check assumes that a barrier on one file's comm
             *  is sufficient (i.e. that there aren't different comms for
             *  different files).  -QAK, 2018/02/14)
             */
            if (H5_coll_api_sanity_check_g && !object->sanity_checked) {
                MPI_Comm mpi_comm; /* File's communicator */

                /* Retrieve the file communicator */
                if (MPI_COMM_NULL == (mpi_comm = H5F_mpi_get_comm(f)))
                    HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't get MPI communicator");

                /* Issue the barrier */
                if (mpi_comm != MPI_COMM_NULL)
                    MPI_Barrier(mpi_comm);

                /* Set the "sanity checked" flag */
                object->sanity_checked = true;
            } /* end if */
        }     /* end if */
#endif        /* H5_HAVE_PARALLEL */

        /* Call the flush routine for mounted file hierarchies */
        if (H5F_flush_mounts((H5F_t *)obj_ptr) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTFLUSH, FAIL, "unable to flush file hierarchy");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__flush_file_cb() */

/*-------------------------------------------------------------------------
 * Function: H5Zfilter_avail
 *
 * Purpose:  Check if a filter is available
 *
 * Return:   Non-negative (true/false) on success/Negative on failure
 *-------------------------------------------------------------------------
 */
htri_t
H5Zfilter_avail(H5Z_filter_t id)
{
    htri_t ret_value = false; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "Zf", id);

    /* Check args */
    if (id < 0 || id > H5Z_FILTER_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid filter identification number");

    if ((ret_value = H5Z_filter_avail(id)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_NOTFOUND, FAIL, "unable to check the availability of the filter");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Zfilter_avail() */

/*-------------------------------------------------------------------------
 * Function: H5Z_filter_avail
 *
 * Purpose:  Private function to check if a filter is available
 *
 * Return:   Non-negative (true/false) on success/Negative on failure
 *-------------------------------------------------------------------------
 */
htri_t
H5Z_filter_avail(H5Z_filter_t id)
{
    H5PL_key_t          key;               /* Key for finding a plugin     */
    const H5Z_class2_t *filter_info;       /* Filter information           */
    size_t              i;                 /* Local index variable         */
    htri_t              ret_value = false; /* Return value                 */

    FUNC_ENTER_NOAPI(FAIL)

    /* Is the filter already registered? */
    for (i = 0; i < H5Z_table_used_g; i++)
        if (H5Z_table_g[i].id == id)
            HGOTO_DONE(true);

    key.id = (int)id;
    if (NULL != (filter_info = (const H5Z_class2_t *)H5PL_load(H5PL_TYPE_FILTER, &key))) {
        if (H5Z_register(filter_info) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to register loaded filter");
        HGOTO_DONE(true);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_filter_avail() */

/*-------------------------------------------------------------------------
 * Function: H5Z__prelude_callback
 *
 * Purpose:  Makes a dataset creation "prelude" callback for the "can_apply"
 *           or "set_local" routines.
 *
 * Return:   Non-negative on success/Negative on failure
 *
 * Notes:    The chunk dimensions are used to create a dataspace, instead
 *           of passing in the dataset's dataspace, since the chunk
 *           dimensions are what the I/O filter will actually see
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__prelude_callback(const H5O_pline_t *pline, hid_t dcpl_id, hid_t type_id, hid_t space_id,
                      H5Z_prelude_type_t prelude_type)
{
    H5Z_class2_t *fclass;           /* Individual filter information */
    size_t        u;                /* Local index variable */
    htri_t        ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(pline->nused > 0);

    /* Iterate over filters */
    for (u = 0; u < pline->nused; u++) {
        /* Get filter information */
        if (NULL == (fclass = H5Z_find(pline->filter[u].id))) {
            /* Ignore errors from optional filters */
            if (pline->filter[u].flags & H5Z_FLAG_OPTIONAL)
                H5E_clear_stack(NULL);
            else
                HGOTO_ERROR(H5E_PLINE, H5E_NOTFOUND, FAIL, "required filter was not located");
        } /* end if */
        else {
            /* Make correct callback */
            switch (prelude_type) {
                case H5Z_PRELUDE_CAN_APPLY:
                    /* Check if filter is configured to be able to encode */
                    if (!fclass->encoder_present)
                        HGOTO_ERROR(H5E_PLINE, H5E_NOENCODER, FAIL,
                                    "Filter present but encoding is disabled.");

                    /* Check if there is a "can apply" callback */
                    if (fclass->can_apply) {
                        /* Make callback to filter's "can apply" function */
                        htri_t status = (fclass->can_apply)(dcpl_id, type_id, space_id);

                        /* Indicate error during filter callback */
                        if (status < 0)
                            HGOTO_ERROR(H5E_PLINE, H5E_CANAPPLY, FAIL, "error during user callback");

                        /* Indicate filter can't apply to this combination of parameters.
                         * If the filter is NOT optional, returns failure. */
                        if (status == false && !(pline->filter[u].flags & H5Z_FLAG_OPTIONAL))
                            HGOTO_ERROR(H5E_PLINE, H5E_CANAPPLY, FAIL, "filter parameters not appropriate");
                    } /* end if */
                    break;

                case H5Z_PRELUDE_SET_LOCAL:
                    /* Check if there is a "set local" callback */
                    if (fclass->set_local) {
                        /* Make callback to filter's "set local" function */
                        if ((fclass->set_local)(dcpl_id, type_id, space_id) < 0)
                            /* Indicate error during filter callback */
                            HGOTO_ERROR(H5E_PLINE, H5E_SETLOCAL, FAIL, "error during user callback");
                    } /* end if */
                    break;

                default:
                    assert("invalid prelude type" && 0);
            } /* end switch */
        }     /* end else */
    }         /* end for */

done:

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__prelude_callback() */

/*-------------------------------------------------------------------------
 * Function: H5Z__prepare_prelude_callback_dcpl
 *
 * Purpose:  Prepares to make a dataset creation "prelude" callback
 *           for the "can_apply" or "set_local" routines.
 *
 * Return:   Non-negative on success/Negative on failure
 *
 * Notes:    The chunk dimensions are used to create a dataspace, instead
 *           of passing in the dataset's dataspace, since the chunk
 *           dimensions are what the I/O filter will actually see
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__prepare_prelude_callback_dcpl(hid_t dcpl_id, hid_t type_id, H5Z_prelude_type_t prelude_type)
{
    hid_t         space_id    = -1;      /* ID for dataspace describing chunk */
    H5O_layout_t *dcpl_layout = NULL;    /* Dataset's layout information */
    herr_t        ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(H5I_GENPROP_LST == H5I_get_type(dcpl_id));
    assert(H5I_DATATYPE == H5I_get_type(type_id));

    /* Check if the property list is non-default */
    if (dcpl_id != H5P_DATASET_CREATE_DEFAULT) {
        H5P_genplist_t *dc_plist; /* Dataset creation property list object */

        /* Get memory for the layout */
        if (NULL == (dcpl_layout = (H5O_layout_t *)H5MM_calloc(sizeof(H5O_layout_t))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to allocate dcpl layout buffer");

        /* Get dataset creation property list object */
        if (NULL == (dc_plist = (H5P_genplist_t *)H5I_object(dcpl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't get dataset creation property list");

        /* Peek at the layout information */
        if (H5P_peek(dc_plist, H5D_CRT_LAYOUT_NAME, dcpl_layout) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't retrieve layout");

        /* Check if the dataset is chunked */
        if (H5D_CHUNKED == dcpl_layout->type) {
            H5O_pline_t dcpl_pline; /* Object's I/O pipeline information */

            /* Get I/O pipeline information */
            if (H5P_peek(dc_plist, H5O_CRT_PIPELINE_NAME, &dcpl_pline) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't retrieve pipeline filter");

            /* Check if the chunks have filters */
            if (dcpl_pline.nused > 0) {
                hsize_t chunk_dims[H5O_LAYOUT_NDIMS]; /* Size of chunk dimensions */
                H5S_t  *space;                        /* Dataspace describing chunk */
                size_t  u;                            /* Local index variable */

                /* Create a dataspace for a chunk & set the extent */
                for (u = 0; u < dcpl_layout->u.chunk.ndims; u++)
                    chunk_dims[u] = dcpl_layout->u.chunk.dim[u];
                if (NULL == (space = H5S_create_simple(dcpl_layout->u.chunk.ndims, chunk_dims, NULL)))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "can't create simple dataspace");

                /* Get ID for dataspace to pass to filter routines */
                if ((space_id = H5I_register(H5I_DATASPACE, space, false)) < 0) {
                    (void)H5S_close(space);
                    HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register dataspace ID");
                }

                /* Make the callbacks */
                if (H5Z__prelude_callback(&dcpl_pline, dcpl_id, type_id, space_id, prelude_type) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_CANAPPLY, FAIL, "unable to apply filter");
            }
        }
    }

done:
    if (space_id > 0 && H5I_dec_ref(space_id) < 0)
        HDONE_ERROR(H5E_PLINE, H5E_CANTRELEASE, FAIL, "unable to close dataspace");

    if (dcpl_layout)
        dcpl_layout = (H5O_layout_t *)H5MM_xfree(dcpl_layout);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__prepare_prelude_callback_dcpl() */

/*-------------------------------------------------------------------------
 * Function: H5Z_can_apply
 *
 * Purpose:  Checks if all the filters defined in the dataset creation
 *           property list can be applied to a particular combination of
 *           datatype and dataspace for a dataset.
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *
 * Notes:    The chunk dimensions are used to create a dataspace, instead
 *           of passing in the dataset's dataspace, since the chunk
 *           dimensions are what the I/O filter will actually see
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_can_apply(hid_t dcpl_id, hid_t type_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Make "can apply" callbacks for filters in pipeline */
    if (H5Z__prepare_prelude_callback_dcpl(dcpl_id, type_id, H5Z_PRELUDE_CAN_APPLY) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANAPPLY, FAIL, "unable to apply filter");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_can_apply() */

/*-------------------------------------------------------------------------
 * Function: H5Z_set_local
 *
 * Purpose:  Makes callbacks to modify dataset creation list property
 *           settings for filters on a new dataset, based on the datatype
 *           and dataspace of that dataset (chunk).
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *
 * Notes:    The chunk dimensions are used to create a dataspace, instead
 *           of passing in the dataset's dataspace, since the chunk
 *           dimensions are what the I/O filter will actually see
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_set_local(hid_t dcpl_id, hid_t type_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Make "set local" callbacks for filters in pipeline */
    if (H5Z__prepare_prelude_callback_dcpl(dcpl_id, type_id, H5Z_PRELUDE_SET_LOCAL) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_SETLOCAL, FAIL, "local filter parameters not set");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_set_local() */

/*-------------------------------------------------------------------------
 * Function: H5Z_can_apply_direct
 *
 * Purpose:  Checks if all the filters defined in the pipeline can be
 *           applied to an opaque byte stream (currently only a group).
 *           The pipeline is assumed to have at least one filter.
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_can_apply_direct(const H5O_pline_t *pline)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(pline->nused > 0);

    /* Make "can apply" callbacks for filters in pipeline */
    if (H5Z__prelude_callback(pline, (hid_t)-1, (hid_t)-1, (hid_t)-1, H5Z_PRELUDE_CAN_APPLY) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANAPPLY, FAIL, "unable to apply filter");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_can_apply_direct() */

/*-------------------------------------------------------------------------
 * Function: H5Z_set_local_direct
 *
 * Purpose:  Makes callbacks to modify local settings for filters on a
 *           new opaque object.  The pipeline is assumed to have at
 *           least one filter.
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *
 * Notes:    This callback will almost certainly not do anything
 *           useful, other than to make certain that the filter will
 *           accept opaque data.
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_set_local_direct(const H5O_pline_t *pline)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(pline->nused > 0);

    /* Make "set local" callbacks for filters in pipeline */
    if (H5Z__prelude_callback(pline, (hid_t)-1, (hid_t)-1, (hid_t)-1, H5Z_PRELUDE_SET_LOCAL) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_SETLOCAL, FAIL, "local filter parameters not set");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_set_local_direct() */

/*-------------------------------------------------------------------------
 * Function: H5Z_ignore_filters
 *
 * Purpose:  Determine whether filters can be ignored.
 *
 * Description:
 *      When the filters are optional (i.e., H5Z_FLAG_OPTIONAL is provided,)
 *      if any of the following conditions is met, the filters will be ignored:
 *          - dataspace is either H5S_NULL or H5S_SCALAR
 *          - datatype is variable-length (string or non-string)
 *      However, if any of these conditions exists and a filter is not
 *      optional, the function will produce an error.
 *
 * Return:   Non-negative(true/false) on success
 *           Negative on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Z_ignore_filters(hid_t dcpl_id, const H5T_t *type, const H5S_t *space)
{
    H5P_genplist_t *dc_plist;                /* Dataset creation property list object */
    H5O_pline_t     pline;                   /* Object's I/O pipeline information */
    H5S_class_t     space_class;             /* To check class of space */
    H5T_class_t     type_class;              /* To check if type is VL */
    bool            bad_for_filters = false; /* Suitable to have filters */
    htri_t          ret_value       = false; /* true for ignoring filters */

    FUNC_ENTER_NOAPI(FAIL)

    if (NULL == (dc_plist = (H5P_genplist_t *)H5I_object(dcpl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't get dataset creation property list");

    /* Get pipeline information */
    if (H5P_peek(dc_plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't retrieve pipeline filter");

    /* Get datatype and dataspace classes for quick access */
    space_class = H5S_GET_EXTENT_TYPE(space);
    type_class  = H5T_get_class(type, false);

    /* These conditions are not suitable for filters */
    bad_for_filters = (H5S_NULL == space_class || H5S_SCALAR == space_class || H5T_VLEN == type_class ||
                       (H5T_STRING == type_class && true == H5T_is_variable_str(type)));

    /* When these conditions occur, if there are required filters in pline,
       then report a failure, otherwise, set flag that they can be ignored */
    if (bad_for_filters) {
        size_t ii;
        if (pline.nused > 0) {
            for (ii = 0; ii < pline.nused; ii++) {
                if (!(pline.filter[ii].flags & H5Z_FLAG_OPTIONAL))
                    HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, FAIL, "not suitable for filters");
            }

            /* All filters are optional, we can ignore them */
            ret_value = true;
        }
    } /* bad for filters */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_ignore_filters() */

/*-------------------------------------------------------------------------
 * Function: H5Z_modify
 *
 * Purpose:  Modify filter parameters for specified pipeline.
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_modify(const H5O_pline_t *pline, H5Z_filter_t filter, unsigned flags, size_t cd_nelmts,
           const unsigned int cd_values[/*cd_nelmts*/])
{
    size_t idx;                 /* Index of filter in pipeline */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(pline);
    assert(filter >= 0 && filter <= H5Z_FILTER_MAX);
    assert(0 == (flags & ~((unsigned)H5Z_FLAG_DEFMASK)));
    assert(0 == cd_nelmts || cd_values);

    /* Locate the filter in the pipeline */
    for (idx = 0; idx < pline->nused; idx++)
        if (pline->filter[idx].id == filter)
            break;

    /* Check if the filter was not already in the pipeline */
    if (idx > pline->nused)
        HGOTO_ERROR(H5E_PLINE, H5E_NOTFOUND, FAIL, "filter not in pipeline");

    /* Change parameters for filter */
    pline->filter[idx].flags     = flags;
    pline->filter[idx].cd_nelmts = cd_nelmts;

    /* Free any existing parameters */
    if (pline->filter[idx].cd_values != NULL && pline->filter[idx].cd_values != pline->filter[idx]._cd_values)
        H5MM_xfree(pline->filter[idx].cd_values);

    /* Set parameters */
    if (cd_nelmts > 0) {
        size_t i; /* Local index variable */

        /* Allocate memory or point at internal buffer */
        if (cd_nelmts > H5Z_COMMON_CD_VALUES) {
            pline->filter[idx].cd_values = (unsigned *)H5MM_malloc(cd_nelmts * sizeof(unsigned));
            if (NULL == pline->filter[idx].cd_values)
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                            "memory allocation failed for filter parameters");
        } /* end if */
        else
            pline->filter[idx].cd_values = pline->filter[idx]._cd_values;

        /* Copy client data values */
        for (i = 0; i < cd_nelmts; i++)
            pline->filter[idx].cd_values[i] = cd_values[i];
    } /* end if */
    else
        pline->filter[idx].cd_values = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_modify() */

/*-------------------------------------------------------------------------
 * Function: H5Z_append
 *
 * Purpose:  Append another filter to the specified pipeline.
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_append(H5O_pline_t *pline, H5Z_filter_t filter, unsigned flags, size_t cd_nelmts,
           const unsigned int cd_values[/*cd_nelmts*/])
{
    size_t idx;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(pline);
    assert(filter >= 0 && filter <= H5Z_FILTER_MAX);
    assert(0 == (flags & ~((unsigned)H5Z_FLAG_DEFMASK)));
    assert(0 == cd_nelmts || cd_values);

    /*
     * Check filter limit.  We do it here for early warnings although we may
     * decide to relax this restriction in the future.
     */
    if (pline->nused >= H5Z_MAX_NFILTERS)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "too many filters in pipeline");

    /* Check for freshly allocated filter pipeline */
    if (pline->version == 0)
        pline->version = H5O_PLINE_VERSION_1;

    /* Allocate additional space in the pipeline if it's full */
    if (pline->nused >= pline->nalloc) {
        H5O_pline_t x;
        size_t      n;

        /* Each filter's data may be stored internally or may be
         * a separate block of memory.
         * For each filter, if cd_values points to the internal array
         * _cd_values, the pointer will need to be updated when the
         * filter struct is reallocated.  Set these pointers to ~NULL
         * so that we can reset them after reallocating the filters array.
         */
        for (n = 0; n < pline->nalloc; ++n)
            if (pline->filter[n].cd_values == pline->filter[n]._cd_values)
                pline->filter[n].cd_values = (unsigned *)((void *)~((size_t)NULL));

        x.nalloc = MAX(H5Z_MAX_NFILTERS, 2 * pline->nalloc);
        x.filter = (H5Z_filter_info_t *)H5MM_realloc(pline->filter, x.nalloc * sizeof(x.filter[0]));
        if (NULL == x.filter)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for filter pipeline");

        /* Fix pointers in previous filters that need to point to their own
         *      internal data.
         */
        for (n = 0; n < pline->nalloc; ++n)
            if (x.filter[n].cd_values == (void *)~((size_t)NULL))
                x.filter[n].cd_values = x.filter[n]._cd_values;

        /* Point to newly allocated buffer */
        pline->nalloc = x.nalloc;
        pline->filter = x.filter;
    } /* end if */

    /* Add the new filter to the pipeline */
    idx                          = pline->nused;
    pline->filter[idx].id        = filter;
    pline->filter[idx].flags     = flags;
    pline->filter[idx].name      = NULL; /*we'll pick it up later*/
    pline->filter[idx].cd_nelmts = cd_nelmts;
    if (cd_nelmts > 0) {
        size_t i; /* Local index variable */

        /* Allocate memory or point at internal buffer */
        if (cd_nelmts > H5Z_COMMON_CD_VALUES) {
            pline->filter[idx].cd_values = (unsigned *)H5MM_malloc(cd_nelmts * sizeof(unsigned));
            if (NULL == pline->filter[idx].cd_values)
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for filter");
        } /* end if */
        else
            pline->filter[idx].cd_values = pline->filter[idx]._cd_values;

        /* Copy client data values */
        for (i = 0; i < cd_nelmts; i++)
            pline->filter[idx].cd_values[i] = cd_values[i];
    } /* end if */
    else
        pline->filter[idx].cd_values = NULL;

    pline->nused++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_append() */

/*-------------------------------------------------------------------------
 * Function: H5Z__find_idx
 *
 * Purpose:  Given a filter ID return the offset in the global array
 *           that holds all the registered filters.
 *
 * Return:   Success:    Non-negative index of entry in global filter table.
 *           Failure:    Negative
 *-------------------------------------------------------------------------
 */
static int
H5Z__find_idx(H5Z_filter_t id)
{
    size_t i;                /* Local index variable */
    int    ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    for (i = 0; i < H5Z_table_used_g; i++)
        if (H5Z_table_g[i].id == id)
            HGOTO_DONE((int)i);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__find_idx() */

/*-------------------------------------------------------------------------
 * Function: H5Z_find
 *
 * Purpose:  Given a filter ID return a pointer to a global struct that
 *           defines the filter.
 *
 * Return:   Success:    Ptr to entry in global filter table.
 *           Failure:    NULL
 *-------------------------------------------------------------------------
 */
H5Z_class2_t *
H5Z_find(H5Z_filter_t id)
{
    int           idx;              /* Filter index in global table */
    H5Z_class2_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Get the index in the global table */
    if ((idx = H5Z__find_idx(id)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_NOTFOUND, NULL, "required filter %d is not registered", id);

    /* Set return value */
    ret_value = H5Z_table_g + idx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5Z_find() */

/*-------------------------------------------------------------------------
 * Function: H5Z_pipeline
 *
 * Purpose:  Process data through the filter pipeline.  The FLAGS argument
 *           is the filter invocation flags (definition flags come from
 *           the PLINE->filter[].flags).  The filters are processed in
 *           definition order unless the H5Z_FLAG_REVERSE is set.  The
 *           FILTER_MASK is a bit-mask to indicate which filters to skip
 *           and on exit will indicate which filters failed.  Each
 *           filter has an index number in the pipeline and that index
 *           number is the filter's bit in the FILTER_MASK.  NBYTES is the
 *           number of bytes of data to filter and on exit should be the
 *           number of resulting bytes while BUF_SIZE holds the total
 *           allocated size of the buffer, which is pointed to BUF.
 *
 *           If the buffer must grow during processing of the pipeline
 *           then the pipeline function should free the original buffer
 *           and return a fresh buffer, adjusting BUF_SIZE accordingly.
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_pipeline(const H5O_pline_t *pline, unsigned flags, unsigned *filter_mask /*in,out*/, H5Z_EDC_t edc_read,
             H5Z_cb_t cb_struct, size_t *nbytes /*in,out*/, size_t *buf_size /*in,out*/,
             void **buf /*in,out*/)
{
    size_t        idx;
    size_t        new_nbytes;
    int           fclass_idx;    /* Index of filter class in global table */
    H5Z_class2_t *fclass = NULL; /* Filter class pointer */
#ifdef H5Z_DEBUG
    H5Z_stats_t  *fstats = NULL; /* Filter stats pointer */
    H5_timer_t    timer;         /* Timer for filter operations */
    H5_timevals_t times;         /* Elapsed time for each operation */
#endif
    unsigned failed = 0;
    unsigned tmp_flags;
    size_t   i;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(0 == (flags & ~((unsigned)H5Z_FLAG_INVMASK)));
    assert(filter_mask);
    assert(nbytes && *nbytes > 0);
    assert(buf_size && *buf_size > 0);
    assert(buf && *buf);
    assert(!pline || pline->nused < H5Z_MAX_NFILTERS);

#ifdef H5Z_DEBUG
    H5_timer_init(&timer);
#endif
    if (pline && (flags & H5Z_FLAG_REVERSE)) { /* Read */
        for (i = pline->nused; i > 0; --i) {
            idx = i - 1;
            if (*filter_mask & ((unsigned)1 << idx)) {
                failed |= (unsigned)1 << idx;
                continue; /* filter excluded */
            }

            /* If the filter isn't registered and the application doesn't
             * indicate no plugin through HDF5_PRELOAD_PLUG (using the symbol "::"),
             * try to load it dynamically and register it.  Otherwise, return failure
             */
            if ((fclass_idx = H5Z__find_idx(pline->filter[idx].id)) < 0) {
                H5PL_key_t          key;
                const H5Z_class2_t *filter_info;
                bool                issue_error = false;

                /* Try loading the filter */
                key.id = (int)(pline->filter[idx].id);
                if (NULL != (filter_info = (const H5Z_class2_t *)H5PL_load(H5PL_TYPE_FILTER, &key))) {
                    /* Register the filter we loaded */
                    if (H5Z_register(filter_info) < 0)
                        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to register filter");

                    /* Search in the table of registered filters again to find the dynamic filter just loaded
                     * and registered */
                    if ((fclass_idx = H5Z__find_idx(pline->filter[idx].id)) < 0)
                        issue_error = true;
                }
                else
                    issue_error = true;

                /* Check for error */
                if (issue_error) {
                    /* Print out the filter name to give more info.  But the name is optional for
                     * the filter */
                    if (pline->filter[idx].name)
                        HGOTO_ERROR(H5E_PLINE, H5E_READERROR, FAIL, "required filter '%s' is not registered",
                                    pline->filter[idx].name);
                    else
                        HGOTO_ERROR(H5E_PLINE, H5E_READERROR, FAIL,
                                    "required filter (name unavailable) is not registered");
                }
            } /* end if */

            fclass = &H5Z_table_g[fclass_idx];

#ifdef H5Z_DEBUG
            fstats = &H5Z_stat_table_g[fclass_idx];
            H5_timer_start(&timer);
#endif

            tmp_flags = flags | (pline->filter[idx].flags);
            tmp_flags |= (edc_read == H5Z_DISABLE_EDC) ? H5Z_FLAG_SKIP_EDC : 0;
            new_nbytes = (fclass->filter)(tmp_flags, pline->filter[idx].cd_nelmts,
                                          pline->filter[idx].cd_values, *nbytes, buf_size, buf);

#ifdef H5Z_DEBUG
            H5_timer_stop(&timer);
            H5_timer_get_times(timer, &times);
            fstats->stats[1].times.elapsed += times.elapsed;
            fstats->stats[1].times.system += times.system;
            fstats->stats[1].times.user += times.user;

            fstats->stats[1].total += MAX(*nbytes, new_nbytes);
            if (0 == new_nbytes)
                fstats->stats[1].errors += *nbytes;
#endif

            if (0 == new_nbytes) {
                if ((cb_struct.func && (H5Z_CB_FAIL == cb_struct.func(pline->filter[idx].id, *buf, *buf_size,
                                                                      cb_struct.op_data))) ||
                    !cb_struct.func)
                    HGOTO_ERROR(H5E_PLINE, H5E_READERROR, FAIL, "filter returned failure during read");

                *nbytes = *buf_size;
                failed |= (unsigned)1 << idx;
                H5E_clear_stack(NULL);
            }
            else
                *nbytes = new_nbytes;
        }
    }
    else if (pline) { /* Write */
        for (idx = 0; idx < pline->nused; idx++) {
            if (*filter_mask & ((unsigned)1 << idx)) {
                failed |= (unsigned)1 << idx;
                continue; /* filter excluded */
            }
            if ((fclass_idx = H5Z__find_idx(pline->filter[idx].id)) < 0) {
                /* Check if filter is optional -- If it isn't, then error */
                if ((pline->filter[idx].flags & H5Z_FLAG_OPTIONAL) == 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_WRITEERROR, FAIL, "required filter is not registered");
                failed |= (unsigned)1 << idx;
                H5E_clear_stack(NULL);
                continue; /* filter excluded */
            }             /* end if */

            fclass = &H5Z_table_g[fclass_idx];

#ifdef H5Z_DEBUG
            fstats = &H5Z_stat_table_g[fclass_idx];
            H5_timer_start(&timer);
#endif

            new_nbytes = (fclass->filter)(flags | (pline->filter[idx].flags), pline->filter[idx].cd_nelmts,
                                          pline->filter[idx].cd_values, *nbytes, buf_size, buf);

#ifdef H5Z_DEBUG
            H5_timer_stop(&timer);
            H5_timer_get_times(timer, &times);
            fstats->stats[0].times.elapsed += times.elapsed;
            fstats->stats[0].times.system += times.system;
            fstats->stats[0].times.user += times.user;

            fstats->stats[0].total += MAX(*nbytes, new_nbytes);
            if (0 == new_nbytes)
                fstats->stats[0].errors += *nbytes;
#endif

            if (0 == new_nbytes) {
                if (0 == (pline->filter[idx].flags & H5Z_FLAG_OPTIONAL)) {
                    if ((cb_struct.func && (H5Z_CB_FAIL == cb_struct.func(pline->filter[idx].id, *buf,
                                                                          *nbytes, cb_struct.op_data))) ||
                        !cb_struct.func)
                        HGOTO_ERROR(H5E_PLINE, H5E_WRITEERROR, FAIL, "filter returned failure");

                    *nbytes = *buf_size;
                }
                failed |= (unsigned)1 << idx;
                H5E_clear_stack(NULL);
            }
            else
                *nbytes = new_nbytes;
        } /* end for */
    }

    *filter_mask = failed;

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function: H5Z_filter_info
 *
 * Purpose:  Get pointer to filter info for pipeline
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *-------------------------------------------------------------------------
 */
H5Z_filter_info_t *
H5Z_filter_info(const H5O_pline_t *pline, H5Z_filter_t filter)
{
    size_t             idx;              /* Index of filter in pipeline */
    H5Z_filter_info_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    assert(pline);
    assert(filter >= 0 && filter <= H5Z_FILTER_MAX);

    /* Locate the filter in the pipeline */
    for (idx = 0; idx < pline->nused; idx++)
        if (pline->filter[idx].id == filter)
            break;

    /* Check if the filter was not already in the pipeline */
    if (idx >= pline->nused)
        HGOTO_ERROR(H5E_PLINE, H5E_NOTFOUND, NULL, "filter not in pipeline");

    /* Set return value */
    ret_value = &pline->filter[idx];

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_filter_info() */

/*-------------------------------------------------------------------------
 * Function: H5Z_filter_in_pline
 *
 * Purpose:  Check whether a filter is in the filter pipeline using the
 *           filter ID.  This function is very similar to H5Z_filter_info
 *
 * Return:   true   - found filter
 *           false  - not found
 *           FAIL   - error
 *-------------------------------------------------------------------------
 */
htri_t
H5Z_filter_in_pline(const H5O_pline_t *pline, H5Z_filter_t filter)
{
    size_t idx;              /* Index of filter in pipeline */
    htri_t ret_value = true; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    assert(pline);
    assert(filter >= 0 && filter <= H5Z_FILTER_MAX);

    /* Locate the filter in the pipeline */
    for (idx = 0; idx < pline->nused; idx++)
        if (pline->filter[idx].id == filter)
            break;

    /* Check if the filter was not already in the pipeline */
    if (idx >= pline->nused)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_filter_in_pline() */

/*-------------------------------------------------------------------------
 * Function: H5Z_all_filters_avail
 *
 * Purpose:  Verify that all the filters in a pipeline are currently
 *           available (i.e. registered)
 *
 * Return:   Non-negative (true/false) on success
 *           Negative on failure
 *-------------------------------------------------------------------------
 */
htri_t
H5Z_all_filters_avail(const H5O_pline_t *pline)
{
    size_t i, j;             /* Local index variable */
    htri_t ret_value = true; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Check args */
    assert(pline);

    /* Iterate through all the filters in pipeline */
    for (i = 0; i < pline->nused; i++) {
        /* Look for each filter in the list of registered filters */
        for (j = 0; j < H5Z_table_used_g; j++)
            if (H5Z_table_g[j].id == pline->filter[i].id)
                break;

        /* Check if we didn't find the filter */
        if (j == H5Z_table_used_g)
            HGOTO_DONE(false);
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_all_filters_avail() */

/*-------------------------------------------------------------------------
 * Function: H5Z_delete
 *
 * Purpose:  Delete filter FILTER from pipeline PLINE;
 *           deletes all filters if FILTER is H5Z_FILTER_ALL
 *
 * Return:   Non-negative on success
 *           Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_delete(H5O_pline_t *pline, H5Z_filter_t filter)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(pline);
    assert(filter >= 0 && filter <= H5Z_FILTER_MAX);

    /* if the pipeline has no filters, just return */
    if (pline->nused == 0)
        HGOTO_DONE(SUCCEED);

    /* Delete all filters */
    if (H5Z_FILTER_ALL == filter) {
        if (H5O_msg_reset(H5O_PLINE_ID, pline) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTFREE, FAIL, "can't release pipeline info");
    }
    /* Delete filter */
    else {
        size_t idx;           /* Index of filter in pipeline */
        bool   found = false; /* Indicate filter was found in pipeline */

        /* Locate the filter in the pipeline */
        for (idx = 0; idx < pline->nused; idx++)
            if (pline->filter[idx].id == filter) {
                found = true;
                break;
            }

        /* filter was not found in the pipeline */
        if (!found)
            HGOTO_ERROR(H5E_PLINE, H5E_NOTFOUND, FAIL, "filter not in pipeline");

        /* Free information for deleted filter */
        if (pline->filter[idx].name && pline->filter[idx].name != pline->filter[idx]._name)
            assert((strlen(pline->filter[idx].name) + 1) > H5Z_COMMON_NAME_LEN);
        if (pline->filter[idx].name != pline->filter[idx]._name)
            pline->filter[idx].name = (char *)H5MM_xfree(pline->filter[idx].name);
        if (pline->filter[idx].cd_values && pline->filter[idx].cd_values != pline->filter[idx]._cd_values)
            assert(pline->filter[idx].cd_nelmts > H5Z_COMMON_CD_VALUES);
        if (pline->filter[idx].cd_values != pline->filter[idx]._cd_values)
            pline->filter[idx].cd_values = (unsigned *)H5MM_xfree(pline->filter[idx].cd_values);

        /* Remove filter from pipeline array */
        if ((idx + 1) < pline->nused) {
            /* Copy filters down & fix up any client data value arrays using internal storage */
            for (; (idx + 1) < pline->nused; idx++) {
                pline->filter[idx] = pline->filter[idx + 1];
                if (pline->filter[idx].name && (strlen(pline->filter[idx].name) + 1) <= H5Z_COMMON_NAME_LEN)
                    pline->filter[idx].name = pline->filter[idx]._name;
                if (pline->filter[idx].cd_nelmts <= H5Z_COMMON_CD_VALUES)
                    pline->filter[idx].cd_values = pline->filter[idx]._cd_values;
            }
        }

        /* Decrement number of used filters */
        pline->nused--;

        /* Reset information for previous last filter in pipeline */
        memset(&pline->filter[pline->nused], 0, sizeof(H5Z_filter_info_t));
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_delete() */

/*-------------------------------------------------------------------------
 * Function: H5Zget_filter_info
 *
 * Purpose:  Gets information about a pipeline data filter and stores it
 *           in filter_config_flags.
 *
 * Return:   zero on success
 *           negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Zget_filter_info(H5Z_filter_t filter, unsigned *filter_config_flags /*out*/)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "Zfx", filter, filter_config_flags);

    /* Get the filter info */
    if (H5Z_get_filter_info(filter, filter_config_flags) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "Filter info not retrieved");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Zget_filter_info() */

/*-------------------------------------------------------------------------
 * Function: H5Z_get_filter_info
 *
 * Purpose:  Gets information about a pipeline data filter and stores it
 *           in filter_config_flags.
 *
 * Return:   zero on success
 *           negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Z_get_filter_info(H5Z_filter_t filter, unsigned int *filter_config_flags)
{
    H5Z_class2_t *fclass;
    herr_t        ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Look up the filter class info */
    if (NULL == (fclass = H5Z_find(filter)))
        HGOTO_ERROR(H5E_PLINE, H5E_BADVALUE, FAIL, "Filter not defined");

    /* Set the filter config flags for the application */
    if (filter_config_flags != NULL) {
        *filter_config_flags = 0;

        if (fclass->encoder_present)
            *filter_config_flags |= H5Z_FILTER_CONFIG_ENCODE_ENABLED;
        if (fclass->decoder_present)
            *filter_config_flags |= H5Z_FILTER_CONFIG_DECODE_ENABLED;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_get_filter_info() */
