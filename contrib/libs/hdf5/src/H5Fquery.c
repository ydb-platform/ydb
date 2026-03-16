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

/*-------------------------------------------------------------------------
 *
 * Created:             H5Fquery.c
 *
 * Purpose:             File structure query routines.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Fmodule.h" /* This source code file is part of the H5F module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fpkg.h"      /* File access                              */
#include "H5FDprivate.h" /* File drivers                             */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function: H5F_shared_get_intent
 *
 * Purpose:  Quick and dirty routine to retrieve the file's 'intent' flags
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_shared_t data structure)
 *
 * Return:   'intent' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_shared_get_intent(const H5F_shared_t *f_sh)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f_sh);

    FUNC_LEAVE_NOAPI(f_sh->flags)
} /* end H5F_shared_get_intent() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_intent
 *
 * Purpose:  Quick and dirty routine to retrieve the file's 'intent' flags
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   'intent' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_get_intent(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->shared->flags)
} /* end H5F_get_intent() */

/*-------------------------------------------------------------------------
 * Function:    H5F_get_low_bound
 *
 * Purpose: Quick and dirty routine to retrieve the file's low_bound.
 *          (Mainly added to stop non-file routines from poking about in the
 *          H5F_t data structure)
 *
 * Return:  low_bound on success/abort on failure (shouldn't fail)
 *
 *-------------------------------------------------------------------------
 */
H5F_libver_t
H5F_get_low_bound(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->shared->low_bound)
} /* end H5F_get_low_bound() */

/*-------------------------------------------------------------------------
 * Function:    H5F_get_high_bound
 *
 * Purpose: Quick and dirty routine to retrieve the file's high_bound.
 *          (Mainly added to stop non-file routines from poking about in the
 *          H5F_t data structure)
 *
 * Return:  high_bound on success/abort on failure (shouldn't fail)
 *
 *-------------------------------------------------------------------------
 */
H5F_libver_t
H5F_get_high_bound(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->shared->high_bound)
} /* end H5F_get_high_bound() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_open_name
 *
 * Purpose:  Retrieve the name used to open a file.
 *
 * Return:   Success:    The name of the file.
 *           Failure:    ? (should not happen)
 *-------------------------------------------------------------------------
 */
char *
H5F_get_open_name(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->open_name);

    FUNC_LEAVE_NOAPI(f->open_name)
} /* end H5F_get_open_name() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_actual_name
 *
 * Purpose:  Retrieve the actual name of a file, after resolving symlinks, etc.
 *
 * Return:   Success:    The name of the file.
 *           Failure:    ? (should not happen)
 *-------------------------------------------------------------------------
 */
char *
H5F_get_actual_name(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->actual_name);

    FUNC_LEAVE_NOAPI(f->actual_name)
} /* end H5F_get_actual_name() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_extpath
 *
 * Purpose:  Retrieve the file's 'extpath' flags
 *           This is used by H5L_extern_traverse() and H5D_build_file_prefix()
 *           to retrieve the main file's location when searching the target file.
 *
 * Return:   'extpath' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
char *
H5F_get_extpath(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);
    assert(f->shared->extpath);

    FUNC_LEAVE_NOAPI(f->shared->extpath)
} /* end H5F_get_extpath() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_shared
 *
 * Purpose:  Retrieve the file's 'shared' pointer
 *
 * Return:   'shared' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
H5F_shared_t *
H5F_get_shared(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->shared)
} /* end H5F_get_shared() */

/*-------------------------------------------------------------------------
 * Function: H5F_same_shared
 *
 * Purpose:  Determine if two files have the same shared file pointer
 *
 * Return:   true/false on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
bool
H5F_same_shared(const H5F_t *f1, const H5F_t *f2)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f1);
    assert(f1->shared);
    assert(f2);
    assert(f2->shared);

    FUNC_LEAVE_NOAPI(f1->shared == f2->shared)
} /* end H5F_same_shared() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_nopen_objs
 *
 * Purpose:  Retrieve the file's 'nopen_objs' value
 *
 * Return:   'nopen_objs' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_get_nopen_objs(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->nopen_objs)
} /* end H5F_get_nopen_objs() */

/*-------------------------------------------------------------------------
 * Function:    H5F_file_id_exists
 *
 * Purpose:     Determines if a file ID exists for this file struct
 *
 * Return:      true/false
 *-------------------------------------------------------------------------
 */
bool
H5F_file_id_exists(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->id_exists)
} /* end H5F_file_id_exists() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_parent
 *
 * Purpose:  Retrieve the file's 'parent' pointer
 *
 * Return:   'parent' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
H5F_t *
H5F_get_parent(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->parent)
} /* end H5F_get_parent() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_nmounts
 *
 * Purpose:  Retrieve the file's 'nmounts' value
 *
 * Return:   'nmounts' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_get_nmounts(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->nmounts)
} /* end H5F_get_nmounts() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_read_attempts
 *
 * Purpose:  Retrieve the file's 'read_attempts' value
 *
 * Return:   '# of read attempts' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_get_read_attempts(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->shared->read_attempts)
} /* end H5F_get_read_attempts() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_fcpl
 *
 * Purpose:  Retrieve the value of a file's FCPL.
 *
 * Return:   Success:    The FCPL for the file.
 *           Failure:    ? (should not happen)
 *-------------------------------------------------------------------------
 */
hid_t
H5F_get_fcpl(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->fcpl_id)
} /* end H5F_get_fcpl() */

/*-------------------------------------------------------------------------
 * Function: H5F_sizeof_addr
 *
 * Purpose:  Quick and dirty routine to retrieve the size of the file's size_t
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   'sizeof_addr' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
uint8_t
H5F_sizeof_addr(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->sizeof_addr)
} /* end H5F_sizeof_addr() */

/*-------------------------------------------------------------------------
 * Function: H5F_sizeof_size
 *
 * Purpose:  Quick and dirty routine to retrieve the size of the file's off_t
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   'sizeof_size' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
uint8_t
H5F_sizeof_size(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->sizeof_size)
} /* H5F_sizeof_size() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_sohm_addr
 *
 * Purpose:  Retrieve the file's 'sohm_addr' value
 *
 * Return:   'sohm_addr' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
haddr_t
H5F_get_sohm_addr(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->sohm_addr)
} /* end H5F_get_sohm_addr() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_sohm_vers
 *
 * Purpose:  Retrieve the file's 'sohm_vers' value
 *
 * Return:   'sohm_vers' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_get_sohm_vers(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->sohm_vers)
} /* end H5F_get_sohm_vers() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_sohm_nindexes
 *
 * Purpose:  Retrieve the file's 'sohm_nindexes' value
 *
 * Return:   'sohm_nindexes' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_get_sohm_nindexes(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->sohm_nindexes)
} /* end H5F_get_sohm_nindexes() */

/*-------------------------------------------------------------------------
 * Function: H5F_sym_leaf_k
 *
 * Purpose:  Replaced a macro to retrieve the symbol table leaf size,
 *           now that the generic properties are being used to store
 *           the values.
 *
 * Return:   Success:    Non-negative, and the symbol table leaf size is
 *                              returned.
 *           Failure:    Negative (should not happen)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_sym_leaf_k(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);
    assert(f->shared->sblock);

    FUNC_LEAVE_NOAPI(f->shared->sblock->sym_leaf_k)
} /* end H5F_sym_leaf_k() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_min_dset_ohdr
 *
 * Purpose:  Get the setting flag for minimized dataset object headers
 *
 * Return:   true/false as set in file
 *-------------------------------------------------------------------------
 */
bool
H5F_get_min_dset_ohdr(const H5F_t *f)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->shared->crt_dset_min_ohdr_flag)
} /* end H5F_get_min_dset_ohdr */

/*-------------------------------------------------------------------------
 * Function: H5F_Kvalue
 *
 * Purpose:  Replaced a macro to retrieve a B-tree key value for a certain
 *           type, now that the generic properties are being used to store
 *           the B-tree values.
 *
 * Return:   Success:    Non-negative, and the B-tree key value is
 *                              returned.
 *           Failure:    Negative (should not happen)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_Kvalue(const H5F_t *f, const H5B_class_t *type)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);
    assert(f->shared->sblock);
    assert(type);

    FUNC_LEAVE_NOAPI(f->shared->sblock->btree_k[type->id])
} /* end H5F_Kvalue() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_nrefs
 *
 * Purpose:  Retrieve the file's 'nrefs' value
 *
 * Return:   'nrefs' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
unsigned
H5F_get_nrefs(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->nrefs)
} /* end H5F_get_nrefs() */

/*-------------------------------------------------------------------------
 * Function: H5F_rdcc_nslots
 *
 * Purpose:  Replaced a macro to retrieve the raw data cache number of slots,
 *           now that the generic properties are being used to store
 *           the values.
 *
 * Return:   Success:    Non-negative, and the raw data cache number of
 *                              of slots is returned.
 *           Failure:    Negative (should not happen)
 *-------------------------------------------------------------------------
 */
size_t
H5F_rdcc_nslots(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->rdcc_nslots)
} /* end H5F_rdcc_nelmts() */

/*-------------------------------------------------------------------------
 * Function: H5F_rdcc_nbytes
 *
 * Purpose:  Replaced a macro to retrieve the raw data cache number of bytes,
 *           now that the generic properties are being used to store
 *           the values.
 *
 * Return:   Success:    Non-negative, and the raw data cache number of
 *                              of bytes is returned.
 *           Failure:    Negative (should not happen)
 *-------------------------------------------------------------------------
 */
size_t
H5F_rdcc_nbytes(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->rdcc_nbytes)
} /* end H5F_rdcc_nbytes() */

/*-------------------------------------------------------------------------
 * Function: H5F_rdcc_w0
 *
 * Purpose:  Replaced a macro to retrieve the raw data cache 'w0' value
 *           now that the generic properties are being used to store
 *           the values.
 *
 * Return:   Success:    Non-negative, and the raw data cache 'w0' value
 *                              is returned.
 *           Failure:    Negative (should not happen)
 *-------------------------------------------------------------------------
 */
double
H5F_rdcc_w0(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->rdcc_w0)
} /* end H5F_rdcc_w0() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_base_addr
 *
 * Purpose:  Quick and dirty routine to retrieve the file's 'base_addr' value
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
haddr_t
H5F_get_base_addr(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);
    assert(f->shared->sblock);

    FUNC_LEAVE_NOAPI(f->shared->sblock->base_addr)
} /* end H5F_get_base_addr() */

/*-------------------------------------------------------------------------
 * Function: H5F_grp_btree_shared
 *
 * Purpose:  Replaced a macro to retrieve the shared B-tree node info
 *           now that the generic properties are being used to store
 *           the values.
 *
 * Return:   Success:    Non-void, and the shared B-tree node info
 *                              is returned.
 *           Failure:    void (should not happen)
 *-------------------------------------------------------------------------
 */
H5UC_t *
H5F_grp_btree_shared(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->grp_btree_shared)
} /* end H5F_grp_btree_shared() */

/*-------------------------------------------------------------------------
 * Function: H5F_sieve_buf_size
 *
 * Purpose:  Replaced a macro to retrieve the dataset sieve buffer size
 *           now that the generic properties are being used to store
 *           the values.
 *
 * Return:   Success:    Non-void, and the dataset sieve buffer size
 *                              is returned.
 *           Failure:    void (should not happen)
 *-------------------------------------------------------------------------
 */
size_t
H5F_sieve_buf_size(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->sieve_buf_size)
} /* end H5F_sieve_buf_size() */

/*-------------------------------------------------------------------------
 * Function: H5F_gc_ref
 *
 * Purpose:  Replaced a macro to retrieve the "garbage collect
 *           references flag" now that the generic properties are being used
 *           to store the values.
 *
 * Return:  Success:    The "garbage collect references flag" is returned.
 *          Failure:    (should not happen)
 *
 *-------------------------------------------------------------------------
 */
unsigned
H5F_gc_ref(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->gc_ref)
} /* end H5F_gc_ref() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_fc_degree
 *
 * Purpose:  Retrieve the 'file close degree' for the file.
 *
 * Return:   Success:    Non-negative, the 'file close degree'
 *           Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
H5F_close_degree_t
H5F_get_fc_degree(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->fc_degree)
} /* end H5F_get_fc_degree() */

/*-------------------------------------------------------------------------
 * Function:    H5F_get_evict_on_close
 *
 * Purpose:     Checks if evict-on-close is desired for objects in the
 *              file.
 *
 * Return:      Success:    Flag indicating whether the evict-on-close
 *                          property was set for the file.
 *              Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
bool
H5F_get_evict_on_close(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->evict_on_close)
} /* end H5F_get_evict_on_close() */

/*-------------------------------------------------------------------------
 * Function: H5F_store_msg_crt_idx
 *
 * Purpose:  Retrieve the 'store message creation index' flag for the file.
 *
 * Return:   Success:    Non-negative, the 'store message creation index' flag
 *           Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
bool
H5F_store_msg_crt_idx(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->store_msg_crt_idx)
} /* end H5F_store_msg_crt_idx() */

/*-------------------------------------------------------------------------
 * Function: H5F_shared_has_feature
 *
 * Purpose:  Check if a file has a particular feature enabled
 *
 * Return:   Success:    Non-negative - true or false
 *           Failure:    Negative (should not happen)
 *-------------------------------------------------------------------------
 */
bool
H5F_shared_has_feature(const H5F_shared_t *f_sh, unsigned feature)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f_sh);

    FUNC_LEAVE_NOAPI((bool)(f_sh->lf->feature_flags & feature))
} /* end H5F_shared_has_feature() */

/*-------------------------------------------------------------------------
 * Function: H5F_has_feature
 *
 * Purpose:  Check if a file has a particular feature enabled
 *
 * Return:   Success:    Non-negative - true or false
 *           Failure:    Negative (should not happen)
 *-------------------------------------------------------------------------
 */
bool
H5F_has_feature(const H5F_t *f, unsigned feature)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI((bool)(f->shared->lf->feature_flags & feature))
} /* end H5F_has_feature() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_driver_id
 *
 * Purpose:  Quick and dirty routine to retrieve the file's 'driver_id' value
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   'driver_id' on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
hid_t
H5F_get_driver_id(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);
    assert(f->shared->lf);

    FUNC_LEAVE_NOAPI(f->shared->lf->driver_id)
} /* end H5F_get_driver_id() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_fileno
 *
 * Purpose:  Quick and dirty routine to retrieve the file's 'fileno' value
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5F_get_fileno(const H5F_t *f, unsigned long *filenum)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(f);
    assert(f->shared);
    assert(f->shared->lf);
    assert(filenum);

    /* Retrieve the file's serial number */
    if (H5FD_get_fileno(f->shared->lf, filenum) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_BADRANGE, FAIL, "can't retrieve fileno");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_get_fileno() */

/*-------------------------------------------------------------------------
 * Function: H5F_shared_get_eoa
 *
 * Purpose:  Quick and dirty routine to retrieve the file's 'eoa' value
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
haddr_t
H5F_shared_get_eoa(const H5F_shared_t *f_sh, H5FD_mem_t type)
{
    haddr_t ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_NOAPI(HADDR_UNDEF)

    assert(f_sh);

    /* Dispatch to driver */
    if (HADDR_UNDEF == (ret_value = H5FD_get_eoa(f_sh->lf, type)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, HADDR_UNDEF, "driver get_eoa request failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_shared_get_eoa() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_eoa
 *
 * Purpose:  Quick and dirty routine to retrieve the file's 'eoa' value
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
haddr_t
H5F_get_eoa(const H5F_t *f, H5FD_mem_t type)
{
    haddr_t ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_NOAPI(HADDR_UNDEF)

    assert(f);
    assert(f->shared);

    /* Dispatch to driver */
    if (HADDR_UNDEF == (ret_value = H5FD_get_eoa(f->shared->lf, type)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, HADDR_UNDEF, "driver get_eoa request failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5F_shared_get_file_driver
 *
 * Purpose:     Returns a pointer to the file driver structure of the
 *              file's 'shared' structure.
 *
 * Return:      file handle on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
herr_t
H5F_shared_get_file_driver(const H5F_shared_t *f_sh, H5FD_t **file_handle)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f_sh);
    assert(file_handle);

    *file_handle = f_sh->lf;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5F_shared_get_file_driver() */

/*-------------------------------------------------------------------------
 * Function:    H5F_get_vfd_handle
 *
 * Purpose:     Returns a pointer to the file handle of the low-level file
 *              driver.  This is the private function for H5Fget_vfd_handle.
 *
 * Return:      Success:        Non-negative.
 *              Failure:        negative.
 *-------------------------------------------------------------------------
 */
herr_t
H5F_get_vfd_handle(const H5F_t *file, hid_t fapl, void **file_handle)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(file);
    assert(file_handle);

    /* Get the VFD handle */
    if (H5FD_get_vfd_handle(file->shared->lf, fapl, file_handle) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get file handle for file driver");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_get_vfd_handle() */

/*-------------------------------------------------------------------------
 * Function: H5F_is_tmp_addr
 *
 * Purpose:  Quick and dirty routine to determine if an address is in
 *           the 'temporary' file space.
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   true/false on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
bool
H5F_is_tmp_addr(const H5F_t *f, haddr_t addr)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(H5_addr_le(f->shared->tmp_addr, addr))
} /* end H5F_is_tmp_addr() */

/*-------------------------------------------------------------------------
 * Function: H5F_use_tmp_space
 *
 * Purpose:  Quick and dirty routine to determine if using temporary
 *           file space is allowed for this file.
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   true/false on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
bool
H5F_use_tmp_space(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->use_tmp_space)
} /* end H5F_use_tmp_space() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function: H5F_coll_md_read
 *
 * Purpose:  Retrieve the 'collective metadata reads' flag for the file.
 *
 * Return:   Success:    Non-negative, the 'collective metadata reads' flag
 *           Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
H5P_coll_md_read_flag_t
H5F_coll_md_read(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f && f->shared);

    FUNC_LEAVE_NOAPI(f->shared->coll_md_read)
} /* end H5F_coll_md_read() */

/*-------------------------------------------------------------------------
 * Function: H5F_shared_coll_md_read
 *
 * Purpose:  Retrieve the 'collective metadata reads' flag for the file.
 *
 * Return:   Success:    Non-negative, the 'collective metadata reads' flag
 *           Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
H5P_coll_md_read_flag_t
H5F_shared_coll_md_read(const H5F_shared_t *f_sh)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f_sh);

    FUNC_LEAVE_NOAPI(f_sh->coll_md_read)
} /* end H5F_shared_coll_md_read() */

/*-------------------------------------------------------------------------
 * Function:    H5F_shared_get_mpi_file_sync_required
 *
 * Purpose:     Returns the mpi_file_sync_required flag
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_shared_get_mpi_file_sync_required(const H5F_shared_t *f_sh, bool *flag /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(f_sh);
    assert(flag);

    /* Dispatch to driver */
    if ((ret_value = H5FD_mpi_get_file_sync_required(f_sh->lf, flag)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "driver get_file_sync_required request failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_shared_get_mpi_file_sync_required() */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function: H5F_use_mdc_logging
 *
 * Purpose:  Quick and dirty routine to determine if using MDC logging
 *           is enabled for this file.
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   true/false on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
bool
H5F_use_mdc_logging(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->use_mdc_logging)
} /* end H5F_use_mdc_logging() */

/*-------------------------------------------------------------------------
 * Function: H5F_start_mdc_log_on_access
 *
 * Purpose:  Quick and dirty routine to determine if we should start MDC
 *           logging on access for this file.
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   true/false on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
bool
H5F_start_mdc_log_on_access(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->start_mdc_log_on_access)
} /* end H5F_start_mdc_log_on_access() */

/*-------------------------------------------------------------------------
 * Function: H5F_mdc_log_location
 *
 * Purpose:  Quick and dirty routine to retrieve the MDC log location
 *           for this file.
 *           (Mainly added to stop non-file routines from poking about in the
 *           H5F_t data structure)
 *
 * Return:   true/false on success/abort on failure (shouldn't fail)
 *-------------------------------------------------------------------------
 */
char *
H5F_mdc_log_location(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->mdc_log_location)
} /* end H5F_mdc_log_location() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_alignment
 *
 * Purpose:  Retrieve the 'alignment' for the file.
 *
 * Return:   Success:    Non-negative, the 'alignment'
 *           Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
hsize_t
H5F_get_alignment(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->alignment)
} /* end H5F_get_alignment() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_threshold
 *
 * Purpose:  Retrieve the 'threshold' for alignment in the file.
 *
 * Return:   Success:    Non-negative, the 'threshold'
 *           Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
hsize_t
H5F_get_threshold(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->threshold)
} /* end H5F_get_threshold() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_pgend_meta_thres
 *
 * Purpose:  Retrieve the 'page end meta threshold size' for the file.
 *
 * Return:   Success:    Non-negative, the 'pgend_meta_thres'
 *           Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
hsize_t
H5F_get_pgend_meta_thres(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->pgend_meta_thres)
} /* end H5F_get_pgend_meta_thres() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_point_of_no_return
 *
 * Purpose:  Retrieve the 'point of no return' value for the file.
 *
 * Return:   Success:    Non-negative, the 'point_of_no_return'
 *           Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
bool
H5F_get_point_of_no_return(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->point_of_no_return)
} /* end H5F_get_point_of_no_return() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_null_fsm_addr
 *
 * Purpose:  Retrieve the 'null_fsm_addr' value for the file.
 *
 * Return:   Success:    Non-negative, the 'null_fsm_addr'
 *           Failure:    (can't happen)
 *-------------------------------------------------------------------------
 */
bool
H5F_get_null_fsm_addr(const H5F_t *f)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->null_fsm_addr)
} /* end H5F_get_null_fsm_addr() */

/*-------------------------------------------------------------------------
 * Function: H5F_get_vol_cls
 *
 * Purpose:  Get the VOL class for the file
 *
 * Return:   VOL class pointer for file, can't fail
 *
 *-------------------------------------------------------------------------
 */
const H5VL_class_t *
H5F_get_vol_cls(const H5F_t *f)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->vol_cls)
} /* end H5F_get_vol_cls */

/*-------------------------------------------------------------------------
 * Function: H5F_get_vol_obj
 *
 * Purpose:  Get the VOL object for the file
 *
 * Return:   VOL object pointer for file, can't fail
 *
 *-------------------------------------------------------------------------
 */
H5VL_object_t *
H5F_get_vol_obj(const H5F_t *f)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);

    FUNC_LEAVE_NOAPI(f->vol_obj)
} /* end H5F_get_vol_obj */

/*-------------------------------------------------------------------------
 * Function:    H5F_get_cont_info
 *
 * Purpose:     Get the VOL container info for the file
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__get_cont_info(const H5F_t *f, H5VL_file_cont_info_t *info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);

    /* Verify structure version */
    if (info->version != H5VL_CONTAINER_INFO_VERSION)
        HGOTO_ERROR(H5E_FILE, H5E_VERSION, FAIL, "wrong container info version #");

    /* Set the container info fields */
    info->feature_flags = 0; /* None currently defined */
    info->token_size    = H5F_SIZEOF_ADDR(f);
    info->blob_id_size  = H5HG_HEAP_ID_SIZE(f);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_get_cont_info */

/*-------------------------------------------------------------------------
 * Function: H5F_get_file_locking
 *
 * Purpose:  Get the file locking flag for the file
 *
 * Return:   true/false
 *
 *-------------------------------------------------------------------------
 */
bool
H5F_get_file_locking(const H5F_t *f)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    FUNC_LEAVE_NOAPI(f->shared->use_file_locking)
} /* end H5F_get_file_locking */

/*-------------------------------------------------------------------------
 * Function: H5F_has_vector_select_io
 *
 * Purpose:  Determine if vector or selection I/O is supported by this file
 *
 * Return:   true/false
 *
 *-------------------------------------------------------------------------
 */
bool
H5F_has_vector_select_io(const H5F_t *f, bool is_write)
{
    bool ret_value; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(f);
    assert(f->shared);

    if (is_write)
        ret_value = (f->shared->lf->cls->write_vector != NULL || f->shared->lf->cls->write_selection != NULL);
    else
        ret_value = (f->shared->lf->cls->read_vector != NULL || f->shared->lf->cls->read_selection != NULL);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_has_vector_select_io */
