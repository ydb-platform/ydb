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
 * Purpose:     Local Heap object debugging functions.
 */

/****************/
/* Module Setup */
/****************/

#include "H5HLmodule.h" /* This source code file is part of the H5HL module */

/***********/
/* Headers */
/***********/

#include "H5private.h"   /* Generic Functions            */
#include "H5Eprivate.h"  /* Error handling               */
#include "H5HLpkg.h"     /* Local heaps                  */
#include "H5MMprivate.h" /* Memory management            */

/*-------------------------------------------------------------------------
 * Function:    H5HL_debug
 *
 * Purpose:     Prints debugging information about a heap.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth)
{
    H5HL_t      *h = NULL;
    int          free_block;
    H5HL_free_t *freelist;
    uint8_t     *marker      = NULL;
    size_t       amount_free = 0;
    herr_t       ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check arguments */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    if (NULL == (h = (H5HL_t *)H5HL_protect(f, addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to load/protect local heap");

    fprintf(stream, "%*sLocal Heap...\n", indent, "");
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Header size (in bytes):", h->prfx_size);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, "Address of heap data:", h->dblk_addr);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Data bytes allocated for heap:", h->dblk_size);

    /* Traverse the free list and check that all free blocks fall within
     * the heap and that no two free blocks point to the same region of
     * the heap.
     */
    if (NULL == (marker = (uint8_t *)H5MM_calloc(h->dblk_size)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "memory allocation failed");

    fprintf(stream, "%*sFree Blocks (offset, size):\n", indent, "");
    for (free_block = 0, freelist = h->freelist; freelist; freelist = freelist->next, free_block++) {
        char temp_str[32];

        snprintf(temp_str, sizeof(temp_str), "Block #%d:", free_block);
        fprintf(stream, "%*s%-*s %8zu, %8zu\n", indent + 3, "", MAX(0, fwidth - 9), temp_str,
                freelist->offset, freelist->size);
        if ((freelist->offset + freelist->size) > h->dblk_size)
            fprintf(stream, "***THAT FREE BLOCK IS OUT OF BOUNDS!\n");
        else {
            int    overlap = 0;
            size_t i;

            for (i = 0; i < freelist->size; i++) {
                if (marker[freelist->offset + i])
                    overlap++;
                marker[freelist->offset + i] = 1;
            }
            if (overlap)
                fprintf(stream, "***THAT FREE BLOCK OVERLAPPED A PREVIOUS ONE!\n");
            else
                amount_free += freelist->size;
        }
    }

    if (h->dblk_size)
        fprintf(stream, "%*s%-*s %.2f%%\n", indent, "", fwidth, "Percent of heap used:",
                (100.0 * (double)(h->dblk_size - amount_free) / (double)h->dblk_size));

    /* Print the data in a VMS-style octal dump */
    H5_buffer_dump(stream, indent, h->dblk_image, marker, (size_t)0, h->dblk_size);

done:
    if (h && FAIL == H5HL_unprotect(h))
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release/unprotect local heap");

    if (marker && NULL != (marker = (uint8_t *)H5MM_xfree(marker)))
        HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "can't free marker buffer");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL_debug() */
