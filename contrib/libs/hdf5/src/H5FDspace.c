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
 * Created:		H5FDspace.c
 *
 * Purpose:		Space allocation routines for the file driver code.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5FDmodule.h" /* This source code file is part of the H5FD module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5FDpkg.h"     /* File Drivers				*/
#include "H5FDmulti.h"   /* Usage-partitioned file family	*/
#include "H5FLprivate.h" /* Free lists                               */

/****************/
/* Local Macros */
/****************/

/* Define this to display information about file allocations */
/* #define H5FD_ALLOC_DEBUG */

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

/* Declare a free list to manage the H5FD_free_t struct */
H5FL_DEFINE(H5FD_free_t);

/*-------------------------------------------------------------------------
 * Function:    H5FD__extend
 *
 * Purpose:     Extend the EOA space of a file.
 *
 * NOTE:        Returns absolute file offset
 *
 * Return:      Success:    The address of the previous EOA.
 *              Failure:    The undefined address HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__extend(H5FD_t *file, H5FD_mem_t type, hsize_t size)
{
    haddr_t eoa;                     /* Address of end-of-allocated space */
    haddr_t ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(file);
    assert(file->cls);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(size > 0);

    /* Get current end-of-allocated space address */
    eoa = file->cls->get_eoa(file, type);

    /* Check for overflow when extending */
    if (H5_addr_overflow(eoa, size) || (eoa + size) > file->maxaddr)
        HGOTO_ERROR(H5E_VFL, H5E_NOSPACE, HADDR_UNDEF, "file allocation request failed");

    /* Set the [NOT aligned] address to return */
    ret_value = eoa;

    /* Extend the end-of-allocated space address */
    eoa += size;
    if (file->cls->set_eoa(file, type, eoa) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_NOSPACE, HADDR_UNDEF, "file allocation request failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__extend() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__alloc_real
 *
 * Purpose:     Allocate space in the file with the VFD
 *              Note: the handling of alignment is moved up from each driver to
 *              this routine.
 *
 * Return:      Success:    The format address of the new file memory.
 *              Failure:    The undefined address HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5FD__alloc_real(H5FD_t *file, H5FD_mem_t type, hsize_t size, haddr_t *frag_addr, hsize_t *frag_size)
{
    hsize_t       orig_size = size;        /* Original allocation size */
    haddr_t       eoa;                     /* Address of end-of-allocated space */
    hsize_t       extra;                   /* Extra space to allocate, to align request */
    unsigned long flags = 0;               /* Driver feature flags */
    bool          use_alloc_size;          /* Just pass alloc size to the driver */
    haddr_t       ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_PACKAGE
#ifdef H5FD_ALLOC_DEBUG
    fprintf(stderr, "%s: type = %u, size = %" PRIuHSIZE "\n", __func__, (unsigned)type, size);
#endif /* H5FD_ALLOC_DEBUG */

    /* check args */
    assert(file);
    assert(file->cls);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(size > 0);

    /* Check for query driver and call it */
    if (file->cls->query)
        (file->cls->query)(file, &flags);

    /* Check for the driver feature flag */
    use_alloc_size = flags & H5FD_FEAT_USE_ALLOC_SIZE;

    /* Get current end-of-allocated space address */
    eoa = file->cls->get_eoa(file, type);

    /* Compute extra space to allocate, if this should be aligned */
    extra = 0;
    if (!file->paged_aggr && file->alignment > 1 && orig_size >= file->threshold) {
        hsize_t mis_align; /* Amount EOA is misaligned */

        /* Check for EOA already aligned */
        if ((mis_align = (eoa % file->alignment)) > 0) {
            extra = file->alignment - mis_align;
            if (frag_addr)
                *frag_addr = eoa - file->base_addr; /* adjust for file's base address */
            if (frag_size)
                *frag_size = extra;
        } /* end if */
    }     /* end if */

    /* Dispatch to driver `alloc' callback or extend the end-of-address marker */
    /* For the multi/split driver: the size passed down to the alloc callback is the original size from
     * H5FD_alloc() */
    /* For all other drivers: the size passed down to the alloc callback is the size + [possibly] alignment
     * size */
    if (file->cls->alloc) {
        ret_value = (file->cls->alloc)(file, type, H5CX_get_dxpl(), use_alloc_size ? size : size + extra);
        if (!H5_addr_defined(ret_value))
            HGOTO_ERROR(H5E_VFL, H5E_NOSPACE, HADDR_UNDEF, "driver allocation request failed");
    } /* end if */
    else {
        ret_value = H5FD__extend(file, type, size + extra);
        if (!H5_addr_defined(ret_value))
            HGOTO_ERROR(H5E_VFL, H5E_NOSPACE, HADDR_UNDEF, "driver eoa update request failed");
    } /* end else */

    /* Set the [possibly aligned] address to return */
    if (!use_alloc_size)
        ret_value += extra;

    /* Post-condition sanity check */
    if (!file->paged_aggr && file->alignment > 1 && orig_size >= file->threshold)
        assert(!(ret_value % file->alignment));

    /* Convert absolute file offset to relative address */
    ret_value -= file->base_addr;

done:
#ifdef H5FD_ALLOC_DEBUG
    fprintf(stderr, "%s: ret_value = %" PRIuHADDR "\n", __func__, ret_value);
#endif /* H5FD_ALLOC_DEBUG */
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__alloc_real() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_alloc
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
H5FD_alloc(H5FD_t *file, H5FD_mem_t type, H5F_t *f, hsize_t size, haddr_t *frag_addr, hsize_t *frag_size)
{
    haddr_t ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_NOAPI(HADDR_UNDEF)

    /* check args */
    assert(file);
    assert(file->cls);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(size > 0);

    /* Call the real 'alloc' routine */
    ret_value = H5FD__alloc_real(file, type, size, frag_addr, frag_size);
    if (!H5_addr_defined(ret_value))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, HADDR_UNDEF, "real 'alloc' request failed");

    /* Mark EOA info dirty in cache, so change will get encoded */
    if (H5F_eoa_dirty(f) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTMARKDIRTY, HADDR_UNDEF, "unable to mark EOA info as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__free_real
 *
 * Purpose:     Release space back to the VFD
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FD__free_real(H5FD_t *file, H5FD_mem_t type, haddr_t addr, hsize_t size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(file);
    assert(file->cls);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(size > 0);

#ifdef H5FD_ALLOC_DEBUG
    fprintf(stderr, "%s: type = %u, addr = %" PRIuHADDR ", size = %" PRIuHSIZE "\n", __func__, (unsigned)type,
            addr, size);
#endif /* H5FD_ALLOC_DEBUG */

    /* Sanity checking */
    if (!H5_addr_defined(addr))
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid file offset");

    /* Convert address to absolute file offset */
    addr += file->base_addr;

    /* More sanity checking */
    if (addr > file->maxaddr || H5_addr_overflow(addr, size) || (addr + size) > file->maxaddr)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid file free space region to free");

    /* Check for file driver 'free' callback and call it if available */
    if (file->cls->free) {
#ifdef H5FD_ALLOC_DEBUG
        fprintf(stderr, "%s: Letting VFD free space\n", __func__);
#endif /* H5FD_ALLOC_DEBUG */
        if ((file->cls->free)(file, type, H5CX_get_dxpl(), addr, size) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTFREE, FAIL, "driver free request failed");
    } /* end if */
    /* Check if this free block is at the end of file allocated space.
     * Truncate it if this is true.
     */
    else if (file->cls->get_eoa) {
        haddr_t eoa;

        eoa = file->cls->get_eoa(file, type);
#ifdef H5FD_ALLOC_DEBUG
        fprintf(stderr, "%s: eoa = %" PRIuHADDR "\n", __func__, eoa);
#endif /* H5FD_ALLOC_DEBUG */
        if (eoa == (addr + size)) {
#ifdef H5FD_ALLOC_DEBUG
            fprintf(stderr, "%s: Reducing file size to = %" PRIuHADDR "\n", __func__, addr);
#endif /* H5FD_ALLOC_DEBUG */
            if (file->cls->set_eoa(file, type, addr) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "set end of space allocation request failed");
        } /* end if */
    }     /* end else-if */
    else {
        /* leak memory */
#ifdef H5FD_ALLOC_DEBUG
        fprintf(stderr, "%s: LEAKED MEMORY!!! type = %u, addr = %" PRIuHADDR ", size = %" PRIuHSIZE "\n",
                __func__, (unsigned)type, addr, size);
#endif /* H5FD_ALLOC_DEBUG */
    }  /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__free_real() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_free
 *
 * Purpose:     Wrapper for H5FD__free_real, to make certain EOA changes are
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
H5FD_free(H5FD_t *file, H5FD_mem_t type, H5F_t *f, haddr_t addr, hsize_t size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(file);
    assert(file->cls);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(size > 0);

    /* Call the real 'free' routine */
    if (H5FD__free_real(file, type, addr, size) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTFREE, FAIL, "real 'free' request failed");

    /* Mark EOA info dirty in cache, so change will get encoded */
    if (H5F_eoa_dirty(f) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTMARKDIRTY, FAIL, "unable to mark EOA info as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_free() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_try_extend
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
H5FD_try_extend(H5FD_t *file, H5FD_mem_t type, H5F_t *f, haddr_t blk_end, hsize_t extra_requested)
{
    haddr_t eoa;               /* End of allocated space in file */
    htri_t  ret_value = false; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(file);
    assert(file->cls);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(extra_requested > 0);
    assert(f);

    /* Retrieve the end of the address space */
    if (HADDR_UNDEF == (eoa = file->cls->get_eoa(file, type)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "driver get_eoa request failed");

    /* Adjust block end by base address of the file, to create absolute address */
    blk_end += file->base_addr;

    /* Check if the block is exactly at the end of the file */
    if (H5_addr_eq(blk_end, eoa)) {
        /* Extend the object by extending the underlying file */
        if (HADDR_UNDEF == H5FD__extend(file, type, extra_requested))
            HGOTO_ERROR(H5E_VFL, H5E_CANTEXTEND, FAIL, "driver extend request failed");

        /* Mark EOA info dirty in cache, so change will get encoded */
        if (H5F_eoa_dirty(f) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTMARKDIRTY, FAIL, "unable to mark EOA info as dirty");

        /* Indicate success */
        HGOTO_DONE(true);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_try_extend() */
