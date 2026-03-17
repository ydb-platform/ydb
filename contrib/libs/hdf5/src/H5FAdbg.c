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
 * Created:     H5FAdbg.c
 *
 * Purpose:     Dump debugging information about a fixed array.
 *
 *-------------------------------------------------------------------------
 */

/**********************/
/* Module Declaration */
/**********************/

#include "H5FAmodule.h" /* This source code file is part of the H5FA module */

/***********************/
/* Other Packages Used */
/***********************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FApkg.h"     /* Fixed Arrays				*/
#include "H5Oprivate.h"  /* Object Header 			*/
#include "H5VMprivate.h" /* Vector functions                     */

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
 * Function:    H5FA__hdr_debug
 *
 * Purpose:     Prints debugging info about a fixed array header.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA__hdr_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, const H5FA_class_t *cls,
                haddr_t obj_addr)
{
    /* Local variables */
    H5FA_hdr_t *hdr       = NULL;    /* Shared fixed array header */
    void       *dbg_ctx   = NULL;    /* Fixed array debugging context */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(H5_addr_defined(obj_addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);
    assert(cls);

    /* Check for debugging context callback available */
    if (cls->crt_dbg_ctx)
        /* Create debugging context */
        if (NULL == (dbg_ctx = cls->crt_dbg_ctx(f, obj_addr)))
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTGET, FAIL, "unable to create fixed array debugging context");

    /* Load the fixed array header */
    if (NULL == (hdr = H5FA__hdr_protect(f, addr, dbg_ctx, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL, "unable to load fixed array header");

    /* Print opening message */
    fprintf(stream, "%*sFixed Array Header...\n", indent, "");

    /* Print the values */
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Array class ID:", hdr->cparam.cls->name);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Header size:", hdr->size);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Raw Element Size:", (unsigned)hdr->cparam.raw_elmt_size);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
            "Native Element Size (on this platform):", hdr->cparam.cls->nat_elmt_size);

    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Max. # of elements in data block page:",
            (unsigned)((size_t)1 << hdr->cparam.max_dblk_page_nelmts_bits));

    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth,
            "Number of elements in Fixed Array:", hdr->stats.nelmts);

    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "Fixed Array Data Block Address:", hdr->dblk_addr);

done:
    if (dbg_ctx && cls->dst_dbg_ctx(dbg_ctx) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTRELEASE, FAIL, "unable to release fixed array debugging context");
    if (hdr && H5FA__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release fixed array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__hdr_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblock_debug
 *
 * Purpose:     Prints debugging info about a fixed array data block.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA__dblock_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, const H5FA_class_t *cls,
                   haddr_t hdr_addr, haddr_t obj_addr)
{
    /* Local variables */
    H5FA_hdr_t    *hdr     = NULL;      /* Shared fixed array header */
    H5FA_dblock_t *dblock  = NULL;      /* Fixed array data block */
    void          *dbg_ctx = NULL;      /* Fixed array context */
    size_t         u;                   /* Local index variable */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);
    assert(cls);
    assert(H5_addr_defined(hdr_addr));
    assert(H5_addr_defined(obj_addr));

    /* Check for debugging context callback available */
    if (cls->crt_dbg_ctx)
        /* Create debugging context */
        if (NULL == (dbg_ctx = cls->crt_dbg_ctx(f, obj_addr)))
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTGET, FAIL, "unable to create fixed array debugging context");

    /* Load the fixed array header */
    if (NULL == (hdr = H5FA__hdr_protect(f, hdr_addr, dbg_ctx, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL, "unable to load fixed array header");

    /* Protect data block */
    if (NULL == (dblock = H5FA__dblock_protect(hdr, addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL,
                    "unable to protect fixed array data block, address = %llu", (unsigned long long)addr);

    /* Print opening message */
    fprintf(stream, "%*sFixed Array data Block...\n", indent, "");

    /* Print the values */
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Array class ID:", hdr->cparam.cls->name);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, "Address of Data Block:", dblock->addr);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth, "Data Block size:", dblock->size);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth,
            "Number of elements in Data Block:", hdr->cparam.nelmts);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Number of pages in Data Block:", dblock->npages);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
            "Number of elements per Data Block page:", dblock->dblk_page_nelmts);

    if (dblock->npages) {         /* paging */
        size_t  dblk_page_nelmts; /* # of elements in a data block page */
        haddr_t dblk_page_addr;   /* Address of a data block page */
        size_t  page_idx;         /* Page index within data block */

        fprintf(stream, "%*sPaging:\n", indent, "");

        /* Iterate over the pages */
        dblk_page_addr   = dblock->addr + H5FA_DBLOCK_PREFIX_SIZE(dblock);
        dblk_page_nelmts = dblock->dblk_page_nelmts;

        /* Read and print each page's elements in the data block */
        for (page_idx = 0; page_idx < dblock->npages; page_idx++) {
            if (!H5VM_bit_get(dblock->dblk_page_init, page_idx)) {
                fprintf(stream, "%*s%-*s %zu %s\n", indent, "", fwidth, "Page %zu:", page_idx, "empty");

            }                                  /* end if */
            else {                             /* get the page */
                H5FA_dblk_page_t *dblk_page;   /* Pointer to a data block page */
                hsize_t           nelmts_left; /* Remaining elements in the last data block page */

                /* Check for last page */
                if (((page_idx + 1) == dblock->npages) &&
                    (nelmts_left = hdr->cparam.nelmts % dblock->dblk_page_nelmts))
                    dblk_page_nelmts = (size_t)nelmts_left;

                if (NULL == (dblk_page = H5FA__dblk_page_protect(hdr, dblk_page_addr, dblk_page_nelmts,
                                                                 H5AC__READ_ONLY_FLAG)))
                    HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL,
                                "unable to protect fixed array data block page, address = %llu",
                                (unsigned long long)dblk_page_addr);

                fprintf(stream, "%*sElements in page %zu:\n", indent, "", page_idx);
                for (u = 0; u < dblk_page_nelmts; u++) {
                    /* Call the class's 'debug' callback */
                    if ((hdr->cparam.cls->debug)(stream, (indent + 3), MAX(0, (fwidth - 3)), (hsize_t)u,
                                                 ((uint8_t *)dblk_page->elmts) +
                                                     (hdr->cparam.cls->nat_elmt_size * u)) < 0)
                        HGOTO_ERROR(H5E_FARRAY, H5E_CANTGET, FAIL, "can't get element for debugging");
                } /* end for */
                if (H5FA__dblk_page_unprotect(dblk_page, H5AC__NO_FLAGS_SET) < 0)
                    HGOTO_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL,
                                "unable to release fixed array data block page");

                /* Advance to next page address */
                dblk_page_addr += dblock->dblk_page_size;
            } /* paging */
        }     /* end for npages */
    }         /* end if */
    else {    /* not paging */
        /* Print the elements in the data block */
        fprintf(stream, "%*sElements:\n", indent, "");
        for (u = 0; u < hdr->cparam.nelmts; u++) {
            /* Call the class's 'debug' callback */
            if ((hdr->cparam.cls->debug)(stream, (indent + 3), MAX(0, (fwidth - 3)), (hsize_t)u,
                                         ((uint8_t *)dblock->elmts) + (hdr->cparam.cls->nat_elmt_size * u)) <
                0)
                HGOTO_ERROR(H5E_FARRAY, H5E_CANTGET, FAIL, "can't get element for debugging");
        } /* end for */
    }     /* end else */

done:
    if (dbg_ctx && cls->dst_dbg_ctx(dbg_ctx) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTRELEASE, FAIL, "unable to release fixed array debugging context");
    if (dblock && H5FA__dblock_unprotect(dblock, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release fixed array data block");
    if (hdr && H5FA__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release fixed array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblock_debug() */
