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
 * Created:         H5EAdbg.c
 *
 * Purpose:        Dump debugging information about an extensible array.
 *
 *-------------------------------------------------------------------------
 */

/**********************/
/* Module Declaration */
/**********************/

#include "H5EAmodule.h" /* This source code file is part of the H5EA module */

/***********************/
/* Other Packages Used */
/***********************/

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions            */
#include "H5Eprivate.h" /* Error handling               */
#include "H5EApkg.h"    /* Extensible Arrays            */

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
 * Function:    H5EA__hdr_debug
 *
 * Purpose:     Prints debugging info about a extensible array header.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, const H5EA_class_t *cls,
                haddr_t obj_addr)
{
    /* Local variables */
    H5EA_hdr_t *hdr       = NULL;    /* Shared extensible array header */
    void       *dbg_ctx   = NULL;    /* Extensible array debugging context */
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
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTGET, FAIL, "unable to create fixed array debugging context");

    /* Load the extensible array header */
    if (NULL == (hdr = H5EA__hdr_protect(f, addr, dbg_ctx, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, FAIL, "unable to load extensible array header");

    /* Print opening message */
    fprintf(stream, "%*sExtensible Array Header...\n", indent, "");

    /* Print the values */
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Array class ID:", hdr->cparam.cls->name);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Header size:", hdr->size);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Raw Element Size:", (unsigned)hdr->cparam.raw_elmt_size);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
            "Native Element Size (on this platform):", hdr->cparam.cls->nat_elmt_size);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Log2(Max. # of elements in array):", (unsigned)hdr->cparam.max_nelmts_bits);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "# of elements in index block:", (unsigned)hdr->cparam.idx_blk_elmts);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Min. # of elements per data block:", (unsigned)hdr->cparam.data_blk_min_elmts);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Min. # of data block pointers for a super block:", (unsigned)hdr->cparam.sup_blk_min_data_ptrs);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Log2(Max. # of elements in data block page):", (unsigned)hdr->cparam.max_dblk_page_nelmts_bits);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth,
            "Highest element index stored (+1):", hdr->stats.stored.max_idx_set);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth,
            "Number of super blocks created:", hdr->stats.stored.nsuper_blks);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth,
            "Number of data blocks created:", hdr->stats.stored.ndata_blks);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth,
            "Number of elements 'realized':", hdr->stats.stored.nelmts);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "Index Block Address:", hdr->idx_blk_addr);

done:
    if (dbg_ctx && cls->dst_dbg_ctx(dbg_ctx) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTRELEASE, FAIL,
                    "unable to release extensible array debugging context");
    if (hdr && H5EA__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__iblock_debug
 *
 * Purpose:     Prints debugging info about a extensible array index block.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__iblock_debug(H5F_t *f, haddr_t H5_ATTR_UNUSED addr, FILE *stream, int indent, int fwidth,
                   const H5EA_class_t *cls, haddr_t hdr_addr, haddr_t obj_addr)
{
    /* Local variables */
    H5EA_hdr_t    *hdr       = NULL;    /* Shared extensible array header */
    H5EA_iblock_t *iblock    = NULL;    /* Extensible array index block */
    void          *dbg_ctx   = NULL;    /* Extensible array context */
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
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTGET, FAIL, "unable to create extensible array debugging context");

    /* Load the extensible array header */
    if (NULL == (hdr = H5EA__hdr_protect(f, hdr_addr, dbg_ctx, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, FAIL, "unable to load extensible array header");

    /* Sanity check */
    assert(H5_addr_eq(hdr->idx_blk_addr, addr));

    /* Protect index block */
    if (NULL == (iblock = H5EA__iblock_protect(hdr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, FAIL,
                    "unable to protect extensible array index block, address = %llu",
                    (unsigned long long)hdr->idx_blk_addr);

    /* Print opening message */
    fprintf(stream, "%*sExtensible Array Index Block...\n", indent, "");

    /* Print the values */
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Array class ID:", hdr->cparam.cls->name);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Index Block size:", iblock->size);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
            "# of data block addresses in index block:", iblock->ndblk_addrs);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
            "# of super block addresses in index block:", iblock->nsblk_addrs);

    /* Check if there are any elements in index block */
    if (hdr->cparam.idx_blk_elmts > 0) {
        unsigned u; /* Local index variable */

        /* Print the elements in the index block */
        fprintf(stream, "%*sElements in Index Block:\n", indent, "");
        for (u = 0; u < hdr->cparam.idx_blk_elmts; u++) {
            /* Call the class's 'debug' callback */
            if ((hdr->cparam.cls->debug)(stream, (indent + 3), MAX(0, (fwidth - 3)), (hsize_t)u,
                                         ((uint8_t *)iblock->elmts) + (hdr->cparam.cls->nat_elmt_size * u)) <
                0)
                HGOTO_ERROR(H5E_EARRAY, H5E_CANTGET, FAIL, "can't get element for debugging");
        } /* end for */
    }     /* end if */

    /* Check if there are any data block addresses in index block */
    if (iblock->ndblk_addrs > 0) {
        char     temp_str[128]; /* Temporary string, for formatting */
        unsigned u;             /* Local index variable */

        /* Print the data block addresses in the index block */
        fprintf(stream, "%*sData Block Addresses in Index Block:\n", indent, "");
        for (u = 0; u < iblock->ndblk_addrs; u++) {
            /* Print address */
            snprintf(temp_str, sizeof(temp_str), "Address #%u:", u);
            fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", (indent + 3), "", MAX(0, (fwidth - 3)), temp_str,
                    iblock->dblk_addrs[u]);
        } /* end for */
    }     /* end if */

    /* Check if there are any super block addresses in index block */
    if (iblock->nsblk_addrs > 0) {
        char     temp_str[128]; /* Temporary string, for formatting */
        unsigned u;             /* Local index variable */

        /* Print the super block addresses in the index block */
        fprintf(stream, "%*sSuper Block Addresses in Index Block:\n", indent, "");
        for (u = 0; u < iblock->nsblk_addrs; u++) {
            /* Print address */
            snprintf(temp_str, sizeof(temp_str), "Address #%u:", u);
            fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", (indent + 3), "", MAX(0, (fwidth - 3)), temp_str,
                    iblock->sblk_addrs[u]);
        } /* end for */
    }     /* end if */

done:
    if (dbg_ctx && cls->dst_dbg_ctx(dbg_ctx) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTRELEASE, FAIL,
                    "unable to release extensible array debugging context");
    if (iblock && H5EA__iblock_unprotect(iblock, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array index block");
    if (hdr && H5EA__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__iblock_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__sblock_debug
 *
 * Purpose:     Prints debugging info about a extensible array super block.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__sblock_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, const H5EA_class_t *cls,
                   haddr_t hdr_addr, unsigned sblk_idx, haddr_t obj_addr)
{
    /* Local variables */
    H5EA_hdr_t    *hdr       = NULL;    /* Shared extensible array header */
    H5EA_sblock_t *sblock    = NULL;    /* Extensible array super block */
    void          *dbg_ctx   = NULL;    /* Extensible array context */
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
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTGET, FAIL, "unable to create extensible array debugging context");

    /* Load the extensible array header */
    if (NULL == (hdr = H5EA__hdr_protect(f, hdr_addr, dbg_ctx, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, FAIL, "unable to load extensible array header");

    /* Protect super block */
    /* (Note: setting parent of super block to 'hdr' for this operation should be OK -QAK) */
    if (NULL ==
        (sblock = H5EA__sblock_protect(hdr, (H5EA_iblock_t *)hdr, addr, sblk_idx, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, FAIL,
                    "unable to protect extensible array super block, address = %llu",
                    (unsigned long long)addr);

    /* Print opening message */
    fprintf(stream, "%*sExtensible Array Super Block...\n", indent, "");

    /* Print the values */
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Array class ID:", hdr->cparam.cls->name);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Super Block size:", sblock->size);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
            "# of data block addresses in super block:", sblock->ndblks);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
            "# of elements in data blocks from this super block:", sblock->dblk_nelmts);

    /* Check if there are any data block addresses in super block */
    if (sblock->ndblks > 0) {
        char     temp_str[128]; /* Temporary string, for formatting */
        unsigned u;             /* Local index variable */

        /* Print the data block addresses in the super block */
        fprintf(stream, "%*sData Block Addresses in Super Block:\n", indent, "");
        for (u = 0; u < sblock->ndblks; u++) {
            /* Print address */
            snprintf(temp_str, sizeof(temp_str), "Address #%u:", u);
            fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", (indent + 3), "", MAX(0, (fwidth - 3)), temp_str,
                    sblock->dblk_addrs[u]);
        } /* end for */
    }     /* end if */

done:
    if (dbg_ctx && cls->dst_dbg_ctx(dbg_ctx) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTRELEASE, FAIL,
                    "unable to release extensible array debugging context");
    if (sblock && H5EA__sblock_unprotect(sblock, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array super block");
    if (hdr && H5EA__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__sblock_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__dblock_debug
 *
 * Purpose:     Prints debugging info about a extensible array data block.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__dblock_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, const H5EA_class_t *cls,
                   haddr_t hdr_addr, size_t dblk_nelmts, haddr_t obj_addr)
{
    /* Local variables */
    H5EA_hdr_t    *hdr     = NULL;      /* Shared extensible array header */
    H5EA_dblock_t *dblock  = NULL;      /* Extensible array data block */
    void          *dbg_ctx = NULL;      /* Extensible array context */
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
    assert(dblk_nelmts > 0);

    /* Check for debugging context callback available */
    if (cls->crt_dbg_ctx)
        /* Create debugging context */
        if (NULL == (dbg_ctx = cls->crt_dbg_ctx(f, obj_addr)))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTGET, FAIL, "unable to create extensible array debugging context");

    /* Load the extensible array header */
    if (NULL == (hdr = H5EA__hdr_protect(f, hdr_addr, dbg_ctx, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, FAIL, "unable to load extensible array header");

    /* Protect data block */
    /* (Note: setting parent of data block to 'hdr' for this operation should be OK -QAK) */
    if (NULL == (dblock = H5EA__dblock_protect(hdr, hdr, addr, dblk_nelmts, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, FAIL,
                    "unable to protect extensible array data block, address = %" PRIuHADDR, addr);

    /* Print opening message */
    fprintf(stream, "%*sExtensible Array data Block...\n", indent, "");

    /* Print the values */
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Array class ID:", hdr->cparam.cls->name);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Data Block size:", dblock->size);

    /* Print the elements in the index block */
    fprintf(stream, "%*sElements:\n", indent, "");
    for (u = 0; u < dblk_nelmts; u++) {
        /* Call the class's 'debug' callback */
        if ((hdr->cparam.cls->debug)(stream, (indent + 3), MAX(0, (fwidth - 3)), (hsize_t)u,
                                     ((uint8_t *)dblock->elmts) + (hdr->cparam.cls->nat_elmt_size * u)) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTGET, FAIL, "can't get element for debugging");
    } /* end for */

done:
    if (dbg_ctx && cls->dst_dbg_ctx(dbg_ctx) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTRELEASE, FAIL,
                    "unable to release extensible array debugging context");
    if (dblock && H5EA__dblock_unprotect(dblock, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array data block");
    if (hdr && H5EA__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__dblock_debug() */
