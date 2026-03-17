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
 * Created:		H5Fspace.c
 *
 * Purpose:		Space allocation routines for the file.
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
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fpkg.h"      /* File access				*/
#include "H5FDprivate.h" /* File drivers				*/

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
 * Function:    H5F__alloc
 *
 * Purpose:     Wrapper for H5FD_alloc, to make certain EOA changes are
 *		reflected in superblock.
 *
 * Note:	When the metadata cache routines are updated to allow
 *		marking an entry dirty without a H5F_t*, this routine should
 *		be changed to take a H5F_super_t* directly.
 *
 * Return:      Success:    The format address of the new file memory.
 *              Failure:    The undefined address HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5F__alloc(H5F_t *f, H5F_mem_t type, hsize_t size, haddr_t *frag_addr, hsize_t *frag_size)
{
    haddr_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(f->shared);
    assert(f->shared->lf);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(size > 0);

    /* Check whether the file can use temporary addresses */
    if (f->shared->use_tmp_space) {
        haddr_t eoa; /* Current EOA for the file */

        /* Get the EOA for the file */
        if (HADDR_UNDEF == (eoa = H5F_get_eoa(f, type)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, HADDR_UNDEF, "Unable to get eoa");

        /* Check for overlapping into file's temporary allocation space */
        if (H5_addr_gt((eoa + size), f->shared->tmp_addr))
            HGOTO_ERROR(H5E_FILE, H5E_BADRANGE, HADDR_UNDEF,
                        "'normal' file space allocation request will overlap into 'temporary' file space");
    } /* end if */

    /* Call the file driver 'alloc' routine */
    ret_value = H5FD_alloc(f->shared->lf, type, f, size, frag_addr, frag_size);
    if (!H5_addr_defined(ret_value))
        HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, HADDR_UNDEF, "file driver 'alloc' request failed");

    /* Mark EOA dirty */
    if (H5F_eoa_dirty(f) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTMARKDIRTY, HADDR_UNDEF, "unable to mark EOA as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5F__free
 *
 * Purpose:     Wrapper for H5FD_free, to make certain EOA changes are
 *		reflected in superblock.
 *
 * Note:	When the metadata cache routines are updated to allow
 *		marking an entry dirty without a H5F_t*, this routine should
 *		be changed to take a H5F_super_t* directly.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__free(H5F_t *f, H5FD_mem_t type, haddr_t addr, hsize_t size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f);
    assert(f->shared);
    assert(f->shared->lf);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(size > 0);

    /* Call the file driver 'free' routine */
    if (H5FD_free(f->shared->lf, type, f, addr, size) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "file driver 'free' request failed");

    /* Mark EOA dirty */
    if (H5F_eoa_dirty(f) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTMARKDIRTY, FAIL, "unable to mark EOA as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__free() */

/*-------------------------------------------------------------------------
 * Function:    H5F__try_extend
 *
 * Purpose:	Extend a block at the end of the file, if possible.
 *
 * Note:	When the metadata cache routines are updated to allow
 *		marking an entry dirty without a H5F_t*, this routine should
 *		be changed to take a H5F_super_t* directly.
 *
 * Return:	Success:	true(1)  - Block was extended
 *                              false(0) - Block could not be extended
 * 		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5F__try_extend(H5F_t *f, H5FD_mem_t type, haddr_t blk_end, hsize_t extra_requested)
{
    htri_t ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(f->shared);
    assert(f->shared->lf);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(extra_requested > 0);

    /* Extend the object by extending the underlying file */
    if ((ret_value = H5FD_try_extend(f->shared->lf, type, f, blk_end, extra_requested)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTEXTEND, FAIL, "driver try extend request failed");

    /* H5FD_try_extend() updates driver message and marks the superblock
     * dirty, so no need to do it again here.
     */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__try_extend() */
