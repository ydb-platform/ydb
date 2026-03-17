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
 * Created:		H5HFdbg.c
 *
 * Purpose:		Dump debugging information about a fractal heap
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5HFmodule.h" /* This source code file is part of the H5HF module */
#define H5HF_DEBUGGING  /* Need access to fractal heap debugging routines */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5HFpkg.h"     /* Fractal heaps			*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* User data for direct block debugging iterator callback */
typedef struct {
    FILE    *stream;      /* Stream for output */
    int      indent;      /* Indentation amount */
    int      fwidth;      /* Field width mount */
    haddr_t  dblock_addr; /* Direct block's address */
    hsize_t  dblock_size; /* Direct block's size */
    uint8_t *marker;      /* 'Marker' array for free space */
    size_t   sect_count;  /* Number of free space sections in block */
    size_t   amount_free; /* Amount of free space in block */
} H5HF_debug_iter_ud1_t;

/* User data for free space section iterator callback */
typedef struct {
    H5FS_t *fspace; /* Free space manager */
    FILE   *stream; /* Stream for output */
    int     indent; /* Indentation amount */
    int     fwidth; /* Field width mount */
} H5HF_debug_iter_ud2_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

static herr_t H5HF__dtable_debug(const H5HF_dtable_t *dtable, FILE *stream, int indent, int fwidth);

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
 * Function:	H5HF_id_print
 *
 * Purpose:	Prints a fractal heap ID.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_id_print(H5HF_t *fh, const void *_id, FILE *stream, int indent, int fwidth)
{
    const uint8_t *id = (const uint8_t *)_id; /* Object ID */
    uint8_t        id_flags;                  /* Heap ID flag bits */
    hsize_t        obj_off;                   /* Offset of object */
    size_t         obj_len;                   /* Length of object */
    char           id_type;                   /* Character for the type of heap ID */
    herr_t         ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /*
     * Check arguments.
     */
    assert(fh);
    assert(id);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Get the ID flags */
    id_flags = *id;

    /* Check for correct heap ID version */
    if ((id_flags & H5HF_ID_VERS_MASK) != H5HF_ID_VERS_CURR)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, FAIL, "incorrect heap ID version");

    /* Check type of object in heap */
    if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_MAN) {
        id_type = 'M';
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_HUGE) {
        id_type = 'H';
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_TINY) {
        id_type = 'T';
    } /* end if */
    else {
        fprintf(stderr, "%s: Heap ID type not supported yet!\n", __func__);
        HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "heap ID type not supported yet");
    } /* end else */

    /* Get the length of the heap object */
    if (H5HF_get_obj_len(fh, id, &obj_len) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't retrieve heap ID length");

    /* Get the offset of the heap object */
    if (H5HF_get_obj_off(fh, id, &obj_off) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't retrieve heap ID length");

    /* Display the heap ID */
    fprintf(stream, "%*s%-*s (%c, %" PRIuHSIZE " , %zu)\n", indent, "", fwidth,
            "Heap ID info: (type, offset, length)", id_type, obj_off, obj_len);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_id_print() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__dtable_debug
 *
 * Purpose:	Prints debugging info about a doubling table
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__dtable_debug(const H5HF_dtable_t *dtable, FILE *stream, int indent, int fwidth)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(dtable);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /*
     * Print the values.
     */
    /* Creation parameter values */
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Doubling table width:", dtable->cparam.width);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
            "Starting block size:", dtable->cparam.start_block_size);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
            "Max. direct block size:", dtable->cparam.max_direct_size);
    fprintf(stream, "%*s%-*s %u (bits)\n", indent, "", fwidth, "Max. index size:", dtable->cparam.max_index);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Starting # of rows in root indirect block:", dtable->cparam.start_root_rows);

    /* Run-time varying parameter values */
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "Table's root address:", dtable->table_addr);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Current # of rows in root indirect block:", dtable->curr_root_rows);

    /* Computed values */
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Max. # of rows in root indirect block:", dtable->max_root_rows);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Max. # of direct rows in any indirect block:", dtable->max_direct_rows);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "# of bits for IDs in first row:", dtable->first_row_bits);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "# of IDs in first row:", dtable->num_id_first_row);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__dtable_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_hdr_print
 *
 * Purpose:	Prints info about a fractal heap header.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
void
H5HF_hdr_print(const H5HF_hdr_t *hdr, bool dump_internal, FILE *stream, int indent, int fwidth)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Print opening message */
    fprintf(stream, "%*sFractal Heap Header...\n", indent, "");

    /*
     * Print the values.
     */
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Heap is:", hdr->man_dtable.curr_root_rows > 0 ? "Indirect" : "Direct");
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Objects stored in 'debugging' format:", hdr->debug_objs ? "TRUE" : "FALSE");
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "'Write once' flag:", hdr->write_once ? "TRUE" : "FALSE");
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "'Huge' object IDs have wrapped:", hdr->huge_ids_wrapped ? "TRUE" : "FALSE");
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "Free space in managed blocks:", hdr->total_man_free);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "Managed space data block size:", hdr->man_size);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "Total managed space allocated:", hdr->man_alloc_size);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "Offset of managed space iterator:", hdr->man_iter_off);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "Number of managed objects in heap:", hdr->man_nobjs);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "Address of free space manager for managed blocks:", hdr->fs_addr);
    fprintf(stream, "%*s%-*s %lu\n", indent, "", fwidth,
            "Max. size of managed object:", (unsigned long)hdr->max_man_size);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "'Huge' object space used:", hdr->huge_size);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "Number of 'huge' objects in heap:", hdr->huge_nobjs);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "ID of next 'huge' object:", hdr->huge_next_id);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "Address of v2 B-tree for 'huge' objects:", hdr->huge_bt2_addr);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "'Tiny' object space used:", hdr->tiny_size);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "Number of 'tiny' objects in heap:", hdr->tiny_nobjs);

    fprintf(stream, "%*sManaged Objects Doubling-Table Info...\n", indent, "");
    H5HF__dtable_debug(&hdr->man_dtable, stream, indent + 3, MAX(0, fwidth - 3));

    /* Print information about I/O filters */
    if (hdr->filter_len > 0) {
        fprintf(stream, "%*sI/O filter Info...\n", indent, "");
        if (hdr->man_dtable.curr_root_rows == 0) {
            fprintf(stream, "%*s%-*s %zu\n", indent + 3, "", MAX(0, fwidth - 3),
                    "Compressed size of root direct block:", hdr->pline_root_direct_size);
            fprintf(stream, "%*s%-*s %x\n", indent + 3, "", MAX(0, fwidth - 3),
                    "Filter mask for root direct block:", hdr->pline_root_direct_filter_mask);
        } /* end if */
        H5O_debug_id(H5O_PLINE_ID, hdr->f, &(hdr->pline), stream, indent + 3, MAX(0, fwidth - 3));
    } /* end if */

    /* Print internal (runtime) information, if requested */
    if (dump_internal) {
        fprintf(stream, "%*sFractal Heap Header Internal Information:\n", indent, "");

        /* Dump root iblock, if there is one */
        fprintf(stream, "%*s%-*s %x\n", indent + 3, "", MAX(0, fwidth - 3),
                "Root indirect block flags:", hdr->root_iblock_flags);
        fprintf(stream, "%*s%-*s %p\n", indent + 3, "", MAX(0, fwidth - 3),
                "Root indirect block pointer:", (void *)hdr->root_iblock);
        if (hdr->root_iblock)
            H5HF_iblock_print(hdr->root_iblock, dump_internal, stream, indent + 3, fwidth);
    } /* end if */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5HF_hdr_print() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_hdr_debug
 *
 * Purpose:	Prints debugging info about a fractal heap header.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_hdr_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth)
{
    H5HF_hdr_t *hdr       = NULL;    /* Fractal heap header info */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Load the fractal heap header */
    if (NULL == (hdr = H5HF__hdr_protect(f, addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap header");

    /* Print the information about the heap's header */
    H5HF_hdr_print(hdr, false, stream, indent, fwidth);

done:
    if (hdr && H5AC_unprotect(f, H5AC_FHEAP_HDR, addr, hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_PROTECT, FAIL, "unable to release fractal heap header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_hdr_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_dblock_debug_cb
 *
 * Purpose:	Detect free space within a direct block
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF_dblock_debug_cb(H5FS_section_info_t *_sect, void *_udata)
{
    H5HF_free_section_t   *sect  = (H5HF_free_section_t *)_sect;    /* Section to dump info */
    H5HF_debug_iter_ud1_t *udata = (H5HF_debug_iter_ud1_t *)_udata; /* User data for callbacks */
    haddr_t                sect_start, sect_end;     /* Section's beginning and ending offsets */
    haddr_t                dblock_start, dblock_end; /* Direct block's beginning and ending offsets */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /*
     * Check arguments.
     */
    assert(sect);
    assert(udata);

    /* Set up some local variables, for convenience */
    sect_start = sect->sect_info.addr;
    sect_end   = (sect->sect_info.addr + sect->sect_info.size) - 1;
    assert(sect_end >= sect_start);
    dblock_start = udata->dblock_addr;
    dblock_end   = (udata->dblock_addr + udata->dblock_size) - 1;
    assert(dblock_end >= dblock_start);

    /* Check for overlap between free space section & direct block */
    if ((sect_start <= dblock_end &&
         sect_end >= dblock_start) || /* section within or overlaps w/beginning of direct block*/
        (sect_start <= dblock_end && sect_end >= dblock_end)) { /* section overlaps w/end of direct block */
        char   temp_str[32];                                    /* Temporary string for formatting */
        size_t start, end;                                      /* Start & end of the overlapping area */
        size_t len;                                             /* Length of the overlapping area */
        size_t overlap;                                         /* Track any overlaps */
        size_t u;                                               /* Local index variable */

        /* Calculate the starting & ending */
        if (sect_start < dblock_start)
            start = 0;
        else
            H5_CHECKED_ASSIGN(start, size_t, (sect_start - dblock_start), hsize_t);
        if (sect_end > dblock_end)
            H5_CHECKED_ASSIGN(end, size_t, udata->dblock_size, hsize_t);
        else
            H5_CHECKED_ASSIGN(end, size_t, ((sect_end - dblock_start) + 1), hsize_t);

        /* Calculate the length */
        len = end - start;

        snprintf(temp_str, sizeof(temp_str), "Section #%u:", (unsigned)udata->sect_count);
        fprintf(udata->stream, "%*s%-*s %8zu, %8zu\n", udata->indent + 3, "", MAX(0, udata->fwidth - 9),
                temp_str, start, len);
        udata->sect_count++;

        /* Mark this node's free space & check for overlaps w/other sections */
        overlap = 0;
        for (u = start; u < end; u++) {
            if (udata->marker[u])
                overlap++;
            udata->marker[u] = 1;
        } /* end for */

        /* Flag overlaps */
        if (overlap)
            fprintf(udata->stream, "***THAT FREE BLOCK OVERLAPPED A PREVIOUS ONE!\n");
        else
            udata->amount_free += len;
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF_dblock_debug_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_dblock_debug
 *
 * Purpose:	Prints debugging info about a fractal heap direct block.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_dblock_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, haddr_t hdr_addr,
                  size_t block_size)
{
    H5HF_hdr_t    *hdr    = NULL;       /* Fractal heap header info */
    H5HF_direct_t *dblock = NULL;       /* Fractal heap direct block info */
    size_t         blk_prefix_size;     /* Size of prefix for block */
    size_t         amount_free;         /* Amount of free space in block */
    uint8_t       *marker    = NULL;    /* Track free space for block */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);
    assert(H5_addr_defined(hdr_addr));
    assert(block_size > 0);

    /* Load the fractal heap header */
    if (NULL == (hdr = H5HF__hdr_protect(f, hdr_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap header");

    /*
     * Load the heap direct block
     */
    if (NULL == (dblock = H5HF__man_dblock_protect(hdr, addr, block_size, NULL, 0, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTLOAD, FAIL, "unable to load fractal heap direct block");

    /* Print opening message */
    fprintf(stream, "%*sFractal Heap Direct Block...\n", indent, "");

    /*
     * Print the values.
     */
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "Address of fractal heap that owns this block:", hdr->heap_addr);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "Offset of direct block in heap:", dblock->block_off);
    blk_prefix_size = H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Size of block header:", blk_prefix_size);

    /* Allocate space for the free space markers */
    if (NULL == (marker = (uint8_t *)H5MM_calloc(dblock->size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Initialize the free space information for the heap */
    if (H5HF__space_start(hdr, false) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize heap free space");

    /* If there is a free space manager for the heap, check for sections that overlap this block */
    if (hdr->fspace) {
        H5HF_debug_iter_ud1_t udata; /* User data for callbacks */

        /* Prepare user data for section iteration callback */
        udata.stream      = stream;
        udata.indent      = indent;
        udata.fwidth      = fwidth;
        udata.dblock_addr = dblock->block_off;
        udata.dblock_size = block_size;
        udata.marker      = marker;
        udata.sect_count  = 0;
        udata.amount_free = 0;

        /* Print header */
        fprintf(stream, "%*sFree Blocks (offset, size):\n", indent, "");

        /* Iterate over the free space sections, to detect overlaps with this block */
        if (H5FS_sect_iterate(f, hdr->fspace, H5HF_dblock_debug_cb, &udata) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_BADITER, FAIL, "can't iterate over heap's free space");

        /* Close the free space information */
        if (H5HF__space_close(hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't release free space info");

        /* Keep the amount of space free */
        amount_free = udata.amount_free;

        /* Check for no free space */
        if (amount_free == 0)
            fprintf(stream, "%*s<none>\n", indent + 3, "");
    } /* end if */
    else
        amount_free = 0;

    fprintf(stream, "%*s%-*s %.2f%%\n", indent, "", fwidth, "Percent of available space for data used:",
            (100.0 * (double)((dblock->size - blk_prefix_size) - amount_free) /
             (double)(dblock->size - blk_prefix_size)));

    /*
     * Print the data in a VMS-style octal dump.
     */
    H5_buffer_dump(stream, indent, dblock->blk, marker, (size_t)0, dblock->size);

done:
    if (dblock && H5AC_unprotect(f, H5AC_FHEAP_DBLOCK, addr, dblock, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_PROTECT, FAIL, "unable to release fractal heap direct block");
    if (hdr && H5AC_unprotect(f, H5AC_FHEAP_HDR, hdr_addr, hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_PROTECT, FAIL, "unable to release fractal heap header");
    H5MM_xfree(marker);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_dblock_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_iblock_print
 *
 * Purpose:	Prints debugging info about a fractal heap indirect block.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
void
H5HF_iblock_print(const H5HF_indirect_t *iblock, bool dump_internal, FILE *stream, int indent, int fwidth)
{
    const H5HF_hdr_t *hdr;          /* Pointer to heap's header */
    char              temp_str[64]; /* Temporary string, for formatting */
    size_t            u, v;         /* Local index variable */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /*
     * Check arguments.
     */
    assert(iblock);
    assert(iblock->hdr);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Set up convenience variables */
    hdr = iblock->hdr;

    /* Print opening message */
    fprintf(stream, "%*sFractal Heap Indirect Block...\n", indent, "");

    /*
     * Print the values.
     */
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "Address of fractal heap that owns this block:", hdr->heap_addr);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " \n", indent, "", fwidth,
            "Offset of indirect block in heap:", iblock->block_off);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Size of indirect block:", iblock->size);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Current # of rows:", iblock->nrows);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Max. # of rows:", iblock->max_rows);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Max direct block rows:", hdr->man_dtable.max_direct_rows);

    /* Print the entry tables */
    if (hdr->filter_len > 0)
        fprintf(stream, "%*sDirect Block Entries: (address/compressed size/filter mask)\n", indent, "");
    else
        fprintf(stream, "%*sDirect Block Entries: (address)\n", indent, "");
    for (u = 0; u < hdr->man_dtable.max_direct_rows && u < iblock->nrows; u++) {
        snprintf(temp_str, sizeof(temp_str), "Row #%u: (block size: %lu)", (unsigned)u,
                 (unsigned long)hdr->man_dtable.row_block_size[u]);
        fprintf(stream, "%*s%-*s\n", indent + 3, "", MAX(0, fwidth - 3), temp_str);
        for (v = 0; v < hdr->man_dtable.cparam.width; v++) {
            size_t off = (u * hdr->man_dtable.cparam.width) + v;

            snprintf(temp_str, sizeof(temp_str), "Col #%u:", (unsigned)v);
            if (hdr->filter_len > 0)
                fprintf(stream, "%*s%-*s %9" PRIuHADDR "/%6zu/%x\n", indent + 6, "", MAX(0, fwidth - 6),
                        temp_str, iblock->ents[off].addr, iblock->filt_ents[off].size,
                        iblock->filt_ents[off].filter_mask);
            else
                fprintf(stream, "%*s%-*s %9" PRIuHADDR "\n", indent + 6, "", MAX(0, fwidth - 6), temp_str,
                        iblock->ents[off].addr);
        } /* end for */
    }     /* end for */
    fprintf(stream, "%*sIndirect Block Entries:\n", indent, "");
    if (iblock->nrows > hdr->man_dtable.max_direct_rows) {
        unsigned first_row_bits;    /* Number of bits used bit addresses in first row */
        unsigned num_indirect_rows; /* Number of rows of blocks in each indirect block */

        first_row_bits = H5VM_log2_of2((uint32_t)hdr->man_dtable.cparam.start_block_size) +
                         H5VM_log2_of2(hdr->man_dtable.cparam.width);
        for (u = hdr->man_dtable.max_direct_rows; u < iblock->nrows; u++) {
            num_indirect_rows = (H5VM_log2_gen(hdr->man_dtable.row_block_size[u]) - first_row_bits) + 1;
            snprintf(temp_str, sizeof(temp_str), "Row #%u: (# of rows: %u)", (unsigned)u, num_indirect_rows);
            fprintf(stream, "%*s%-*s\n", indent + 3, "", MAX(0, fwidth - 3), temp_str);
            for (v = 0; v < hdr->man_dtable.cparam.width; v++) {
                size_t off = (u * hdr->man_dtable.cparam.width) + v;

                snprintf(temp_str, sizeof(temp_str), "Col #%u:", (unsigned)v);
                fprintf(stream, "%*s%-*s %9" PRIuHADDR "\n", indent + 6, "", MAX(0, fwidth - 6), temp_str,
                        iblock->ents[off].addr);
            } /* end for */
        }     /* end for */
    }         /* end if */
    else
        fprintf(stream, "%*s%-*s\n", indent + 3, "", MAX(0, fwidth - 3), "<none>");

    /* Print internal (runtime) information, if requested */
    if (dump_internal) {
        fprintf(stream, "%*sFractal Indirect Block Internal Information:\n", indent, "");

        /* Print general information */
        fprintf(stream, "%*s%-*s %zu\n", indent + 3, "", MAX(0, fwidth - 3), "Reference count:", iblock->rc);

        /* Print parent's information */
        fprintf(stream, "%*s%-*s %p\n", indent + 3, "", MAX(0, fwidth - 3),
                "Parent indirect block address:", (void *)iblock->parent);
        if (iblock->parent)
            H5HF_iblock_print(iblock->parent, true, stream, indent + 6, fwidth);
    } /* end if */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5HF_iblock_print() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_iblock_debug
 *
 * Purpose:	Prints debugging info about a fractal heap indirect block.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_iblock_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, haddr_t hdr_addr,
                  unsigned nrows)
{
    H5HF_hdr_t      *hdr         = NULL;    /* Fractal heap header info */
    H5HF_indirect_t *iblock      = NULL;    /* Fractal heap direct block info */
    bool             did_protect = false;   /* Whether we protected the indirect block or not */
    herr_t           ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);
    assert(H5_addr_defined(hdr_addr));
    assert(nrows > 0);

    /* Load the fractal heap header */
    if (NULL == (hdr = H5HF__hdr_protect(f, hdr_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap header");

    /*
     * Load the heap indirect block
     */
    if (NULL == (iblock = H5HF__man_iblock_protect(hdr, addr, nrows, NULL, 0, false, H5AC__READ_ONLY_FLAG,
                                                   &did_protect)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTLOAD, FAIL, "unable to load fractal heap indirect block");

    /* Print the information about the heap's indirect block */
    H5HF_iblock_print(iblock, false, stream, indent, fwidth);

done:
    if (iblock && H5HF__man_iblock_unprotect(iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_PROTECT, FAIL, "unable to release fractal heap direct block");
    if (hdr && H5AC_unprotect(f, H5AC_FHEAP_HDR, hdr_addr, hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_PROTECT, FAIL, "unable to release fractal heap header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_iblock_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_sects_debug_cb
 *
 * Purpose:	Prints debugging info about a free space section for a fractal heap.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF_sects_debug_cb(H5FS_section_info_t *_sect, void *_udata)
{
    H5HF_free_section_t   *sect      = (H5HF_free_section_t *)_sect;    /* Section to dump info */
    H5HF_debug_iter_ud2_t *udata     = (H5HF_debug_iter_ud2_t *)_udata; /* User data for callbacks */
    herr_t                 ret_value = SUCCEED;                         /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /*
     * Check arguments.
     */
    assert(sect);
    assert(udata);

    /* Print generic section information */
    fprintf(udata->stream, "%*s%-*s %s\n", udata->indent, "", udata->fwidth, "Section type:",
            (sect->sect_info.type == H5HF_FSPACE_SECT_SINGLE
                 ? "single"
                 : (sect->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW
                        ? "first row"
                        : (sect->sect_info.type == H5HF_FSPACE_SECT_NORMAL_ROW ? "normal row" : "unknown"))));
    fprintf(udata->stream, "%*s%-*s %" PRIuHADDR "\n", udata->indent, "", udata->fwidth,
            "Section address:", sect->sect_info.addr);
    fprintf(udata->stream, "%*s%-*s %" PRIuHSIZE "\n", udata->indent, "", udata->fwidth,
            "Section size:", sect->sect_info.size);

    /* Dump section-specific debugging information */
    if (H5FS_sect_debug(udata->fspace, _sect, udata->stream, udata->indent + 3, MAX(0, udata->fwidth - 3)) <
        0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADITER, FAIL, "can't dump section's debugging info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_sects_debug_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_sects_debug
 *
 * Purpose:	Prints debugging info about free space sections for a fractal heap.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_sects_debug(H5F_t *f, haddr_t fh_addr, FILE *stream, int indent, int fwidth)
{
    H5HF_hdr_t *hdr       = NULL;    /* Fractal heap header info */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(fh_addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Load the fractal heap header */
    if (NULL == (hdr = H5HF__hdr_protect(f, fh_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap header");

    /* Initialize the free space information for the heap */
    if (H5HF__space_start(hdr, false) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize heap free space");

    /* If there is a free space manager for the heap, iterate over them */
    if (hdr->fspace) {
        H5HF_debug_iter_ud2_t udata; /* User data for callbacks */

        /* Prepare user data for section iteration callback */
        udata.fspace = hdr->fspace;
        udata.stream = stream;
        udata.indent = indent;
        udata.fwidth = fwidth;

        /* Iterate over all the free space sections */
        if (H5FS_sect_iterate(f, hdr->fspace, H5HF_sects_debug_cb, &udata) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_BADITER, FAIL, "can't iterate over heap's free space");

        /* Close the free space information */
        if (H5HF__space_close(hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't release free space info");
    } /* end if */

done:
    if (hdr && H5AC_unprotect(f, H5AC_FHEAP_HDR, fh_addr, hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_PROTECT, FAIL, "unable to release fractal heap header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_sects_debug() */
