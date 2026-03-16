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
 * Created:             H5Faccum.c
 *
 * Purpose:             File metadata "accumulator" routines.  (Used to
 *                      cache small metadata I/Os and group them into a
 *                      single larger I/O)
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
#include "H5FDprivate.h" /* File drivers			*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/

/****************/
/* Local Macros */
/****************/

/* Metadata accumulator controls */
#define H5F_ACCUM_THROTTLE  8
#define H5F_ACCUM_THRESHOLD 2048
#define H5F_ACCUM_MAX_SIZE  (1024 * 1024) /* Max. accum. buf size (max. I/Os will be 1/2 this size) */

/******************/
/* Local Typedefs */
/******************/

/* Enumerated type to indicate how data will be added to accumulator */
typedef enum {
    H5F_ACCUM_PREPEND, /* Data will be prepended to accumulator */
    H5F_ACCUM_APPEND   /* Data will be appended to accumulator */
} H5F_accum_adjust_t;

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

/* Declare a PQ free list to manage the metadata accumulator buffer */
H5FL_BLK_DEFINE_STATIC(meta_accum);

/*-------------------------------------------------------------------------
 * Function:	H5F__accum_read
 *
 * Purpose:	Attempts to read some data from the metadata accumulator for
 *              a file into a buffer.
 *
 * Note:	We can't change (or add to) the metadata accumulator, because
 *		this might be a speculative read and could possibly read raw
 *		data into the metadata accumulator.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__accum_read(H5F_shared_t *f_sh, H5FD_mem_t map_type, haddr_t addr, size_t size, void *buf /*out*/)
{
    H5FD_t *file;                /* File driver pointer */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f_sh);
    assert(buf);

    /* Translate to file driver I/O info object */
    file = f_sh->lf;

    /* Check if this information is in the metadata accumulator */
    if ((f_sh->feature_flags & H5FD_FEAT_ACCUMULATE_METADATA) && map_type != H5FD_MEM_DRAW) {
        H5F_meta_accum_t *accum; /* Alias for file's metadata accumulator */

        /* Set up alias for file's metadata accumulator info */
        accum = &f_sh->accum;

        if (size < H5F_ACCUM_MAX_SIZE) {
            /* Sanity check */
            assert(!accum->buf || (accum->alloc_size >= accum->size));

            /* Current read adjoins or overlaps with metadata accumulator */
            if (H5_addr_defined(accum->loc) &&
                (H5_addr_overlap(addr, size, accum->loc, accum->size) || ((addr + size) == accum->loc) ||
                 (accum->loc + accum->size) == addr)) {
                size_t  amount_before; /* Amount to read before current accumulator */
                haddr_t new_addr;      /* New address of the accumulator buffer */
                size_t  new_size;      /* New size of the accumulator buffer */

                /* Compute new values for accumulator */
                new_addr = MIN(addr, accum->loc);
                new_size = (size_t)(MAX((addr + size), (accum->loc + accum->size)) - new_addr);

                /* Check if we need more buffer space */
                if (new_size > accum->alloc_size) {
                    size_t new_alloc_size; /* New size of accumulator */

                    /* Adjust the buffer size to be a power of 2 that is large enough to hold data */
                    new_alloc_size = (size_t)1 << (1 + H5VM_log2_gen((uint64_t)(new_size - 1)));

                    /* Reallocate the metadata accumulator buffer */
                    if (NULL == (accum->buf = H5FL_BLK_REALLOC(meta_accum, accum->buf, new_alloc_size)))
                        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                    "unable to allocate metadata accumulator buffer");

                    /* Note the new buffer size */
                    accum->alloc_size = new_alloc_size;

                    /* Clear the memory */
                    memset(accum->buf + accum->size, 0, (accum->alloc_size - accum->size));
                } /* end if */

                /* Read the part before the metadata accumulator */
                if (addr < accum->loc) {
                    /* Set the amount to read */
                    H5_CHECKED_ASSIGN(amount_before, size_t, (accum->loc - addr), hsize_t);

                    /* Make room for the metadata to read in */
                    memmove(accum->buf + amount_before, accum->buf, accum->size);

                    /* Adjust dirty region tracking info, if present */
                    if (accum->dirty)
                        accum->dirty_off += amount_before;

                    /* Dispatch to driver */
                    if (H5FD_read(file, map_type, addr, amount_before, accum->buf) < 0)
                        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "driver read request failed");
                } /* end if */
                else
                    amount_before = 0;

                /* Read the part after the metadata accumulator */
                if ((addr + size) > (accum->loc + accum->size)) {
                    size_t amount_after; /* Amount to read at a time */

                    /* Set the amount to read */
                    H5_CHECKED_ASSIGN(amount_after, size_t, ((addr + size) - (accum->loc + accum->size)),
                                      hsize_t);

                    /* Dispatch to driver */
                    if (H5FD_read(file, map_type, (accum->loc + accum->size), amount_after,
                                  (accum->buf + accum->size + amount_before)) < 0)
                        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "driver read request failed");
                } /* end if */

                /* Copy the data out of the buffer */
                H5MM_memcpy(buf, accum->buf + (addr - new_addr), size);

                /* Adjust the accumulator address & size */
                accum->loc  = new_addr;
                accum->size = new_size;
            } /* end if */
            /* Current read doesn't overlap with metadata accumulator, read it from file */
            else {
                /* Dispatch to driver */
                if (H5FD_read(file, map_type, addr, size, buf) < 0)
                    HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "driver read request failed");
            } /* end else */
        }     /* end if */
        else {
            /* Read the data */
            if (H5FD_read(file, map_type, addr, size, buf) < 0)
                HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "driver read request failed");

            /* Check for overlap w/dirty accumulator */
            /* (Note that this could be improved by updating the non-dirty
             *  information in the accumulator with [some of] the information
             *  just read in. -QAK)
             */
            if (accum->dirty &&
                H5_addr_overlap(addr, size, accum->loc + accum->dirty_off, accum->dirty_len)) {
                haddr_t dirty_loc = accum->loc + accum->dirty_off; /* File offset of dirty information */
                size_t  buf_off;                                   /* Offset of dirty region in buffer */
                size_t  dirty_off;                                 /* Offset within dirty region */
                size_t  overlap_size;                              /* Size of overlap with dirty region */

                /* Check for read starting before beginning dirty region */
                if (H5_addr_le(addr, dirty_loc)) {
                    /* Compute offset of dirty region within buffer */
                    buf_off = (size_t)(dirty_loc - addr);

                    /* Compute offset within dirty region */
                    dirty_off = 0;

                    /* Check for read ending within dirty region */
                    if (H5_addr_lt(addr + size, dirty_loc + accum->dirty_len))
                        overlap_size = (size_t)((addr + size) - buf_off);
                    else /* Access covers whole dirty region */
                        overlap_size = accum->dirty_len;
                }      /* end if */
                else { /* Read starts after beginning of dirty region */
                    /* Compute dirty offset within buffer and overlap size */
                    buf_off      = 0;
                    dirty_off    = (size_t)(addr - dirty_loc);
                    overlap_size = (size_t)((dirty_loc + accum->dirty_len) - addr);
                } /* end else */

                /* Copy the dirty region to buffer */
                H5MM_memcpy((unsigned char *)buf + buf_off,
                            (unsigned char *)accum->buf + accum->dirty_off + dirty_off, overlap_size);
            } /* end if */
        }     /* end else */
    }         /* end if */
    else {
        /* Read the data */
        if (H5FD_read(file, map_type, addr, size, buf) < 0)
            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "driver read request failed");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__accum_read() */

/*-------------------------------------------------------------------------
 * Function:	H5F__accum_adjust
 *
 * Purpose:	Adjust accumulator size, if necessary
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__accum_adjust(H5F_meta_accum_t *accum, H5FD_t *file, H5F_accum_adjust_t adjust, size_t size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(accum);
    assert(file);
    assert(H5F_ACCUM_APPEND == adjust || H5F_ACCUM_PREPEND == adjust);
    assert(size > 0);
    assert(size <= H5F_ACCUM_MAX_SIZE);

    /* Check if we need more buffer space */
    if ((size + accum->size) > accum->alloc_size) {
        size_t new_size; /* New size of accumulator */

        /* Adjust the buffer size to be a power of 2 that is large enough to hold data */
        new_size = (size_t)1 << (1 + H5VM_log2_gen((uint64_t)((size + accum->size) - 1)));

        /* Check for accumulator getting too big */
        if (new_size > H5F_ACCUM_MAX_SIZE) {
            size_t shrink_size;  /* Amount to shrink accumulator by */
            size_t remnant_size; /* Amount left in accumulator */

            /* Cap the accumulator's growth, leaving some room */

            /* Determine the amounts to work with */
            if (size > (H5F_ACCUM_MAX_SIZE / 2)) {
                new_size     = H5F_ACCUM_MAX_SIZE;
                shrink_size  = accum->size;
                remnant_size = 0;
            } /* end if */
            else {
                if (H5F_ACCUM_PREPEND == adjust) {
                    new_size     = (H5F_ACCUM_MAX_SIZE / 2);
                    shrink_size  = (H5F_ACCUM_MAX_SIZE / 2);
                    remnant_size = accum->size - shrink_size;
                } /* end if */
                else {
                    size_t adjust_size = size + accum->dirty_len;

                    /* Check if we can slide the dirty region down, to accommodate the request */
                    if (accum->dirty && (adjust_size <= H5F_ACCUM_MAX_SIZE)) {
                        if ((ssize_t)(H5F_ACCUM_MAX_SIZE - (accum->dirty_off + adjust_size)) >=
                            (ssize_t)(2 * size))
                            shrink_size = accum->dirty_off / 2;
                        else
                            shrink_size = accum->dirty_off;
                        remnant_size = accum->size - shrink_size;
                        new_size     = remnant_size + size;
                    } /* end if */
                    else {
                        new_size     = (H5F_ACCUM_MAX_SIZE / 2);
                        shrink_size  = (H5F_ACCUM_MAX_SIZE / 2);
                        remnant_size = accum->size - shrink_size;
                    } /* end else */
                }     /* end else */
            }         /* end else */

            /* Check if we need to flush accumulator data to file */
            if (accum->dirty) {
                /* Check whether to accumulator will be prepended or appended */
                if (H5F_ACCUM_PREPEND == adjust) {
                    /* Check if the dirty region overlaps the region to eliminate from the accumulator */
                    if ((accum->size - shrink_size) < (accum->dirty_off + accum->dirty_len)) {
                        /* Write out the dirty region from the metadata accumulator, with dispatch to driver
                         */
                        if (H5FD_write(file, H5FD_MEM_DEFAULT, (accum->loc + accum->dirty_off),
                                       accum->dirty_len, (accum->buf + accum->dirty_off)) < 0)
                            HGOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "file write failed");

                        /* Reset accumulator dirty flag */
                        accum->dirty = false;
                    } /* end if */
                }     /* end if */
                else {
                    /* Check if the dirty region overlaps the region to eliminate from the accumulator */
                    if (shrink_size > accum->dirty_off) {
                        /* Write out the dirty region from the metadata accumulator, with dispatch to driver
                         */
                        if (H5FD_write(file, H5FD_MEM_DEFAULT, (accum->loc + accum->dirty_off),
                                       accum->dirty_len, (accum->buf + accum->dirty_off)) < 0)
                            HGOTO_ERROR(H5E_FILE, H5E_WRITEERROR, FAIL, "file write failed");

                        /* Reset accumulator dirty flag */
                        accum->dirty = false;
                    } /* end if */

                    /* Adjust dirty region tracking info */
                    accum->dirty_off -= shrink_size;
                } /* end else */
            }     /* end if */

            /* Trim the accumulator's use of its buffer */
            accum->size = remnant_size;

            /* When appending, need to adjust location of accumulator */
            if (H5F_ACCUM_APPEND == adjust) {
                /* Move remnant of accumulator down */
                memmove(accum->buf, (accum->buf + shrink_size), remnant_size);

                /* Adjust accumulator's location */
                accum->loc += shrink_size;
            } /* end if */
        }     /* end if */

        /* Check for accumulator needing to be reallocated */
        if (new_size > accum->alloc_size) {
            unsigned char *new_buf; /* New buffer to hold the accumulated metadata */

            /* Reallocate the metadata accumulator buffer */
            if (NULL == (new_buf = H5FL_BLK_REALLOC(meta_accum, accum->buf, new_size)))
                HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, FAIL, "unable to allocate metadata accumulator buffer");

            /* Update accumulator info */
            accum->buf        = new_buf;
            accum->alloc_size = new_size;

            /* Clear the memory */
            memset(accum->buf + accum->size, 0, (accum->alloc_size - (accum->size + size)));
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__accum_adjust() */

/*-------------------------------------------------------------------------
 * Function:	H5F__accum_write
 *
 * Purpose:	Attempts to write some data to the metadata accumulator for
 *              a file from a buffer.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__accum_write(H5F_shared_t *f_sh, H5FD_mem_t map_type, haddr_t addr, size_t size, const void *buf)
{
    H5FD_t *file;                /* File driver pointer */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f_sh);
    assert(H5F_SHARED_INTENT(f_sh) & H5F_ACC_RDWR);
    assert(buf);

    /* Translate to file driver pointer */
    file = f_sh->lf;

    /* Check for accumulating metadata */
    if ((f_sh->feature_flags & H5FD_FEAT_ACCUMULATE_METADATA) && map_type != H5FD_MEM_DRAW) {
        H5F_meta_accum_t *accum; /* Alias for file's metadata accumulator */

        /* Set up alias for file's metadata accumulator info */
        accum = &f_sh->accum;

        if (size < H5F_ACCUM_MAX_SIZE) {
            /* Sanity check */
            assert(!accum->buf || (accum->alloc_size >= accum->size));

            /* Check if there is already metadata in the accumulator */
            if (accum->size > 0) {
                /* Check if the new metadata adjoins the beginning of the current accumulator */
                if (H5_addr_defined(accum->loc) && (addr + size) == accum->loc) {
                    /* Check if we need to adjust accumulator size */
                    if (H5F__accum_adjust(accum, file, H5F_ACCUM_PREPEND, size) < 0)
                        HGOTO_ERROR(H5E_IO, H5E_CANTRESIZE, FAIL, "can't adjust metadata accumulator");

                    /* Move the existing metadata to the proper location */
                    memmove(accum->buf + size, accum->buf, accum->size);

                    /* Copy the new metadata at the front */
                    H5MM_memcpy(accum->buf, buf, size);

                    /* Set the new size & location of the metadata accumulator */
                    accum->loc = addr;
                    accum->size += size;

                    /* Adjust the dirty region and mark accumulator dirty */
                    if (accum->dirty)
                        accum->dirty_len = size + accum->dirty_off + accum->dirty_len;
                    else {
                        accum->dirty_len = size;
                        accum->dirty     = true;
                    } /* end else */
                    accum->dirty_off = 0;
                } /* end if */
                /* Check if the new metadata adjoins the end of the current accumulator */
                else if (H5_addr_defined(accum->loc) && addr == (accum->loc + accum->size)) {
                    /* Check if we need to adjust accumulator size */
                    if (H5F__accum_adjust(accum, file, H5F_ACCUM_APPEND, size) < 0)
                        HGOTO_ERROR(H5E_IO, H5E_CANTRESIZE, FAIL, "can't adjust metadata accumulator");

                    /* Copy the new metadata to the end */
                    H5MM_memcpy(accum->buf + accum->size, buf, size);

                    /* Adjust the dirty region and mark accumulator dirty */
                    if (accum->dirty)
                        accum->dirty_len = size + (accum->size - accum->dirty_off);
                    else {
                        accum->dirty_off = accum->size;
                        accum->dirty_len = size;
                        accum->dirty     = true;
                    } /* end else */

                    /* Set the new size of the metadata accumulator */
                    accum->size += size;
                } /* end if */
                /* Check if the piece of metadata being written overlaps the metadata accumulator */
                else if (H5_addr_defined(accum->loc) &&
                         H5_addr_overlap(addr, size, accum->loc, accum->size)) {
                    size_t add_size; /* New size of the accumulator buffer */

                    /* Check if the new metadata is entirely within the current accumulator */
                    if (addr >= accum->loc && (addr + size) <= (accum->loc + accum->size)) {
                        size_t dirty_off = (size_t)(addr - accum->loc);

                        /* Copy the new metadata to the proper location within the accumulator */
                        H5MM_memcpy(accum->buf + dirty_off, buf, size);

                        /* Adjust the dirty region and mark accumulator dirty */
                        if (accum->dirty) {
                            /* Check for new metadata starting before current dirty region */
                            if (dirty_off <= accum->dirty_off) {
                                if ((dirty_off + size) <= (accum->dirty_off + accum->dirty_len))
                                    accum->dirty_len = (accum->dirty_off + accum->dirty_len) - dirty_off;
                                else
                                    accum->dirty_len = size;
                                accum->dirty_off = dirty_off;
                            } /* end if */
                            else {
                                if ((dirty_off + size) <= (accum->dirty_off + accum->dirty_len))
                                    ; /* accum->dirty_len doesn't change */
                                else
                                    accum->dirty_len = (dirty_off + size) - accum->dirty_off;
                            } /* end else */
                        }     /* end if */
                        else {
                            accum->dirty_off = dirty_off;
                            accum->dirty_len = size;
                            accum->dirty     = true;
                        } /* end else */
                    }     /* end if */
                    /* Check if the new metadata overlaps the beginning of the current accumulator */
                    else if (addr < accum->loc && (addr + size) <= (accum->loc + accum->size)) {
                        size_t old_offset; /* Offset of old data within the accumulator buffer */

                        /* Calculate the amount we will need to add to the accumulator size, based on the
                         * amount of overlap */
                        H5_CHECKED_ASSIGN(add_size, size_t, (accum->loc - addr), hsize_t);

                        /* Check if we need to adjust accumulator size */
                        if (H5F__accum_adjust(accum, file, H5F_ACCUM_PREPEND, add_size) < 0)
                            HGOTO_ERROR(H5E_IO, H5E_CANTRESIZE, FAIL, "can't adjust metadata accumulator");

                        /* Calculate the proper offset of the existing metadata */
                        H5_CHECKED_ASSIGN(old_offset, size_t, (addr + size) - accum->loc, hsize_t);

                        /* Move the existing metadata to the proper location */
                        memmove(accum->buf + size, accum->buf + old_offset, (accum->size - old_offset));

                        /* Copy the new metadata at the front */
                        H5MM_memcpy(accum->buf, buf, size);

                        /* Set the new size & location of the metadata accumulator */
                        accum->loc = addr;
                        accum->size += add_size;

                        /* Adjust the dirty region and mark accumulator dirty */
                        if (accum->dirty) {
                            size_t curr_dirty_end = add_size + accum->dirty_off + accum->dirty_len;

                            accum->dirty_off = 0;
                            if (size <= curr_dirty_end)
                                accum->dirty_len = curr_dirty_end;
                            else
                                accum->dirty_len = size;
                        } /* end if */
                        else {
                            accum->dirty_off = 0;
                            accum->dirty_len = size;
                            accum->dirty     = true;
                        } /* end else */
                    }     /* end if */
                    /* Check if the new metadata overlaps the end of the current accumulator */
                    else if (addr >= accum->loc && (addr + size) > (accum->loc + accum->size)) {
                        size_t dirty_off; /* Offset of dirty region */

                        /* Calculate the amount we will need to add to the accumulator size, based on the
                         * amount of overlap */
                        H5_CHECKED_ASSIGN(add_size, size_t, (addr + size) - (accum->loc + accum->size),
                                          hsize_t);

                        /* Check if we need to adjust accumulator size */
                        if (H5F__accum_adjust(accum, file, H5F_ACCUM_APPEND, add_size) < 0)
                            HGOTO_ERROR(H5E_IO, H5E_CANTRESIZE, FAIL, "can't adjust metadata accumulator");

                        /* Compute offset of dirty region (after adjusting accumulator) */
                        dirty_off = (size_t)(addr - accum->loc);

                        /* Copy the new metadata to the end */
                        H5MM_memcpy(accum->buf + dirty_off, buf, size);

                        /* Set the new size of the metadata accumulator */
                        accum->size += add_size;

                        /* Adjust the dirty region and mark accumulator dirty */
                        if (accum->dirty) {
                            /* Check for new metadata starting before current dirty region */
                            if (dirty_off <= accum->dirty_off) {
                                accum->dirty_off = dirty_off;
                                accum->dirty_len = size;
                            } /* end if */
                            else {
                                accum->dirty_len = (dirty_off + size) - accum->dirty_off;
                            } /* end else */
                        }     /* end if */
                        else {
                            accum->dirty_off = dirty_off;
                            accum->dirty_len = size;
                            accum->dirty     = true;
                        } /* end else */
                    }     /* end if */
                    /* New metadata overlaps both ends of the current accumulator */
                    else {
                        /* Check if we need more buffer space */
                        if (size > accum->alloc_size) {
                            size_t new_alloc_size; /* New size of accumulator */

                            /* Adjust the buffer size to be a power of 2 that is large enough to hold data */
                            new_alloc_size = (size_t)1 << (1 + H5VM_log2_gen((uint64_t)(size - 1)));

                            /* Reallocate the metadata accumulator buffer */
                            if (NULL ==
                                (accum->buf = H5FL_BLK_REALLOC(meta_accum, accum->buf, new_alloc_size)))
                                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                            "unable to allocate metadata accumulator buffer");

                            /* Note the new buffer size */
                            accum->alloc_size = new_alloc_size;

                            /* Clear the memory */
                            memset(accum->buf + size, 0, (accum->alloc_size - size));
                        } /* end if */

                        /* Copy the new metadata to the buffer */
                        H5MM_memcpy(accum->buf, buf, size);

                        /* Set the new size & location of the metadata accumulator */
                        accum->loc  = addr;
                        accum->size = size;

                        /* Adjust the dirty region and mark accumulator dirty */
                        accum->dirty_off = 0;
                        accum->dirty_len = size;
                        accum->dirty     = true;
                    } /* end else */
                }     /* end if */
                /* New piece of metadata doesn't adjoin or overlap the existing accumulator */
                else {
                    /* Write out the existing metadata accumulator, with dispatch to driver */
                    if (accum->dirty) {
                        if (H5FD_write(file, H5FD_MEM_DEFAULT, accum->loc + accum->dirty_off,
                                       accum->dirty_len, accum->buf + accum->dirty_off) < 0)
                            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed");

                        /* Reset accumulator dirty flag */
                        accum->dirty = false;
                    } /* end if */

                    /* Cache the new piece of metadata */
                    /* Check if we need to resize the buffer */
                    if (size > accum->alloc_size) {
                        size_t new_size;   /* New size of accumulator */
                        size_t clear_size; /* Size of memory that needs clearing */

                        /* Adjust the buffer size to be a power of 2 that is large enough to hold data */
                        new_size = (size_t)1 << (1 + H5VM_log2_gen((uint64_t)(size - 1)));

                        /* Grow the metadata accumulator buffer */
                        if (NULL == (accum->buf = H5FL_BLK_REALLOC(meta_accum, accum->buf, new_size)))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                        "unable to allocate metadata accumulator buffer");

                        /* Note the new buffer size */
                        accum->alloc_size = new_size;

                        /* Clear the memory */
                        clear_size = MAX(accum->size, size);
                        memset(accum->buf + clear_size, 0, (accum->alloc_size - clear_size));
                    } /* end if */
                    else {
                        /* Check if we should shrink the accumulator buffer */
                        if (size < (accum->alloc_size / H5F_ACCUM_THROTTLE) &&
                            accum->alloc_size > H5F_ACCUM_THRESHOLD) {
                            size_t tmp_size =
                                (accum->alloc_size / H5F_ACCUM_THROTTLE); /* New size of accumulator buffer */

                            /* Shrink the accumulator buffer */
                            if (NULL == (accum->buf = H5FL_BLK_REALLOC(meta_accum, accum->buf, tmp_size)))
                                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                            "unable to allocate metadata accumulator buffer");

                            /* Note the new buffer size */
                            accum->alloc_size = tmp_size;
                        } /* end if */
                    }     /* end else */

                    /* Update the metadata accumulator information */
                    accum->loc  = addr;
                    accum->size = size;

                    /* Store the piece of metadata in the accumulator */
                    H5MM_memcpy(accum->buf, buf, size);

                    /* Adjust the dirty region and mark accumulator dirty */
                    accum->dirty_off = 0;
                    accum->dirty_len = size;
                    accum->dirty     = true;
                } /* end else */
            }     /* end if */
            /* No metadata in the accumulator, grab this piece and keep it */
            else {
                /* Check if we need to reallocate the buffer */
                if (size > accum->alloc_size) {
                    size_t new_size; /* New size of accumulator */

                    /* Adjust the buffer size to be a power of 2 that is large enough to hold data */
                    new_size = (size_t)1 << (1 + H5VM_log2_gen((uint64_t)(size - 1)));

                    /* Reallocate the metadata accumulator buffer */
                    if (NULL == (accum->buf = H5FL_BLK_REALLOC(meta_accum, accum->buf, new_size)))
                        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                    "unable to allocate metadata accumulator buffer");

                    /* Note the new buffer size */
                    accum->alloc_size = new_size;

                    /* Clear the memory */
                    memset(accum->buf + size, 0, (accum->alloc_size - size));
                } /* end if */

                /* Update the metadata accumulator information */
                accum->loc  = addr;
                accum->size = size;

                /* Store the piece of metadata in the accumulator */
                H5MM_memcpy(accum->buf, buf, size);

                /* Adjust the dirty region and mark accumulator dirty */
                accum->dirty_off = 0;
                accum->dirty_len = size;
                accum->dirty     = true;
            } /* end else */
        }     /* end if */
        else {
            /* Make certain that data in accumulator is visible before new write */
            if ((H5F_SHARED_INTENT(f_sh) & H5F_ACC_SWMR_WRITE) > 0)
                /* Flush if dirty and reset accumulator */
                if (H5F__accum_reset(f_sh, true) < 0)
                    HGOTO_ERROR(H5E_IO, H5E_CANTRESET, FAIL, "can't reset accumulator");

            /* Write the data */
            if (H5FD_write(file, map_type, addr, size, buf) < 0)
                HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed");

            /* Check for overlap w/accumulator */
            /* (Note that this could be improved by updating the accumulator
             *  with [some of] the information just read in. -QAK)
             */
            if (H5_addr_defined(accum->loc) && H5_addr_overlap(addr, size, accum->loc, accum->size)) {
                /* Check for write starting before beginning of accumulator */
                if (H5_addr_le(addr, accum->loc)) {
                    /* Check for write ending within accumulator */
                    if (H5_addr_le(addr + size, accum->loc + accum->size)) {
                        size_t overlap_size; /* Size of overlapping region */

                        /* Compute overlap size */
                        overlap_size = (size_t)((addr + size) - accum->loc);

                        /* Check for dirty region */
                        if (accum->dirty) {
                            haddr_t dirty_start =
                                accum->loc + accum->dirty_off; /* File address of start of dirty region */
                            haddr_t dirty_end =
                                dirty_start + accum->dirty_len; /* File address of end of dirty region */

                            /* Check if entire dirty region is overwritten */
                            if (H5_addr_le(dirty_end, addr + size)) {
                                accum->dirty     = false;
                                accum->dirty_len = 0;
                            } /* end if */
                            else {
                                /* Check for dirty region falling after write */
                                if (H5_addr_le(addr + size, dirty_start))
                                    accum->dirty_off = overlap_size;
                                else { /* Dirty region overlaps w/written region */
                                    accum->dirty_off = 0;
                                    accum->dirty_len -= (size_t)((addr + size) - dirty_start);
                                } /* end else */
                            }     /* end if */
                        }         /* end if */

                        /* Trim bottom of accumulator off */
                        accum->loc += overlap_size;
                        accum->size -= overlap_size;
                        memmove(accum->buf, accum->buf + overlap_size, accum->size);
                    }      /* end if */
                    else { /* Access covers whole accumulator */
                        /* Reset accumulator, but don't flush */
                        if (H5F__accum_reset(f_sh, false) < 0)
                            HGOTO_ERROR(H5E_IO, H5E_CANTRESET, FAIL, "can't reset accumulator");
                    }                    /* end else */
                }                        /* end if */
                else {                   /* Write starts after beginning of accumulator */
                    size_t overlap_size; /* Size of overlapping region */

                    /* Sanity check */
                    assert(H5_addr_gt(addr + size, accum->loc + accum->size));

                    /* Compute overlap size */
                    overlap_size = (size_t)((accum->loc + accum->size) - addr);

                    /* Check for dirty region */
                    if (accum->dirty) {
                        haddr_t dirty_start =
                            accum->loc + accum->dirty_off; /* File address of start of dirty region */
                        haddr_t dirty_end =
                            dirty_start + accum->dirty_len; /* File address of end of dirty region */

                        /* Check if entire dirty region is overwritten */
                        if (H5_addr_ge(dirty_start, addr)) {
                            accum->dirty     = false;
                            accum->dirty_len = 0;
                        } /* end if */
                        else {
                            /* Check for dirty region falling before write */
                            if (H5_addr_le(dirty_end, addr))
                                ; /* noop */
                            else  /* Dirty region overlaps w/written region */
                                accum->dirty_len = (size_t)(addr - dirty_start);
                        } /* end if */
                    }     /* end if */

                    /* Trim top of accumulator off */
                    accum->size -= overlap_size;
                } /* end else */
            }     /* end if */
        }         /* end else */
    }             /* end if */
    else {
        /* Write the data */
        if (H5FD_write(file, map_type, addr, size, buf) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__accum_write() */

/*-------------------------------------------------------------------------
 * Function:    H5F__accum_free
 *
 * Purpose:     Check for free space invalidating [part of] a metadata
 *              accumulator.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__accum_free(H5F_shared_t *f_sh, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr, hsize_t size)
{
    H5F_meta_accum_t *accum;               /* Alias for file's metadata accumulator */
    H5FD_t           *file;                /* File driver pointer */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(f_sh);

    /* Set up alias for file's metadata accumulator info */
    accum = &f_sh->accum;

    /* Translate to file driver pointer */
    file = f_sh->lf;

    /* Adjust the metadata accumulator to remove the freed block, if it overlaps */
    if ((f_sh->feature_flags & H5FD_FEAT_ACCUMULATE_METADATA) && H5_addr_defined(accum->loc) &&
        H5_addr_overlap(addr, size, accum->loc, accum->size)) {
        size_t overlap_size; /* Size of overlap with accumulator */

        /* Sanity check */
        /* (The metadata accumulator should not intersect w/raw data */
        assert(H5FD_MEM_DRAW != type);
        assert(H5FD_MEM_GHEAP != type); /* (global heap data is being treated as raw data currently) */

        /* Check for overlapping the beginning of the accumulator */
        if (H5_addr_le(addr, accum->loc)) {
            /* Check for completely overlapping the accumulator */
            if (H5_addr_ge(addr + size, accum->loc + accum->size)) {
                /* Reset the accumulator, but don't free buffer */
                accum->loc   = HADDR_UNDEF;
                accum->size  = 0;
                accum->dirty = false;
            } /* end if */
            /* Block to free must end within the accumulator */
            else {
                size_t new_accum_size; /* Size of new accumulator buffer */

                /* Calculate the size of the overlap with the accumulator, etc. */
                H5_CHECKED_ASSIGN(overlap_size, size_t, (addr + size) - accum->loc, haddr_t);
                new_accum_size = accum->size - overlap_size;

                /* Move the accumulator buffer information to eliminate the freed block */
                memmove(accum->buf, accum->buf + overlap_size, new_accum_size);

                /* Adjust the accumulator information */
                accum->loc += overlap_size;
                accum->size = new_accum_size;

                /* Adjust the dirty region and possibly mark accumulator clean */
                if (accum->dirty) {
                    /* Check if block freed is entirely before dirty region */
                    if (overlap_size < accum->dirty_off)
                        accum->dirty_off -= overlap_size;
                    else {
                        /* Check if block freed ends within dirty region */
                        if (overlap_size < (accum->dirty_off + accum->dirty_len)) {
                            accum->dirty_len = (accum->dirty_off + accum->dirty_len) - overlap_size;
                            accum->dirty_off = 0;
                        } /* end if */
                        /* Block freed encompasses dirty region */
                        else
                            accum->dirty = false;
                    } /* end else */
                }     /* end if */
            }         /* end else */
        }             /* end if */
        /* Block to free must start within the accumulator */
        else {
            haddr_t dirty_end   = accum->loc + accum->dirty_off + accum->dirty_len;
            haddr_t dirty_start = accum->loc + accum->dirty_off;

            /* Calculate the size of the overlap with the accumulator */
            H5_CHECKED_ASSIGN(overlap_size, size_t, (accum->loc + accum->size) - addr, haddr_t);

            /* Check if block to free begins before end of dirty region */
            if (accum->dirty && H5_addr_lt(addr, dirty_end)) {
                haddr_t tail_addr;

                /* Calculate the address of the tail to write */
                tail_addr = addr + size;

                /* Check if the block to free begins before dirty region */
                if (H5_addr_lt(addr, dirty_start)) {
                    /* Check if block to free is entirely before dirty region */
                    if (H5_addr_le(tail_addr, dirty_start)) {
                        /* Write out the entire dirty region of the accumulator */
                        if (H5FD_write(file, H5FD_MEM_DEFAULT, dirty_start, accum->dirty_len,
                                       accum->buf + accum->dirty_off) < 0)
                            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed");
                    } /* end if */
                    /* Block to free overlaps with some/all of dirty region */
                    /* Check for unfreed dirty region to write */
                    else if (H5_addr_lt(tail_addr, dirty_end)) {
                        size_t write_size;
                        size_t dirty_delta;

                        write_size  = (size_t)(dirty_end - tail_addr);
                        dirty_delta = accum->dirty_len - write_size;

                        assert(write_size > 0);

                        /* Write out the unfreed dirty region of the accumulator */
                        if (H5FD_write(file, H5FD_MEM_DEFAULT, dirty_start + dirty_delta, write_size,
                                       accum->buf + accum->dirty_off + dirty_delta) < 0)
                            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed");
                    } /* end if */

                    /* Reset dirty flag */
                    accum->dirty = false;
                } /* end if */
                /* Block to free begins at beginning of or in middle of dirty region */
                else {
                    /* Check if block to free ends before end of dirty region */
                    if (H5_addr_lt(tail_addr, dirty_end)) {
                        size_t write_size;
                        size_t dirty_delta;

                        write_size  = (size_t)(dirty_end - tail_addr);
                        dirty_delta = accum->dirty_len - write_size;

                        assert(write_size > 0);

                        /* Write out the unfreed end of the dirty region of the accumulator */
                        if (H5FD_write(file, H5FD_MEM_DEFAULT, dirty_start + dirty_delta, write_size,
                                       accum->buf + accum->dirty_off + dirty_delta) < 0)
                            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed");
                    } /* end if */

                    /* Check for block to free beginning at same location as dirty region */
                    if (H5_addr_eq(addr, dirty_start)) {
                        /* Reset dirty flag */
                        accum->dirty = false;
                    } /* end if */
                    /* Block to free eliminates end of dirty region */
                    else {
                        accum->dirty_len = (size_t)(addr - dirty_start);
                    } /* end else */
                }     /* end else */

            } /* end if */

            /* Adjust the accumulator information */
            accum->size = accum->size - overlap_size;
        } /* end else */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__accum_free() */

/*-------------------------------------------------------------------------
 * Function:	H5F__accum_flush
 *
 * Purpose:	Flush the metadata accumulator to the file
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__accum_flush(H5F_shared_t *f_sh)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f_sh);

    /* Check if we need to flush out the metadata accumulator */
    if ((f_sh->feature_flags & H5FD_FEAT_ACCUMULATE_METADATA) && f_sh->accum.dirty) {
        H5FD_t *file; /* File driver pointer */

        /* Translate to file driver pointer */
        file = f_sh->lf;

        /* Flush the metadata contents */
        if (H5FD_write(file, H5FD_MEM_DEFAULT, f_sh->accum.loc + f_sh->accum.dirty_off, f_sh->accum.dirty_len,
                       f_sh->accum.buf + f_sh->accum.dirty_off) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed");

        /* Reset the dirty flag */
        f_sh->accum.dirty = false;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__accum_flush() */

/*-------------------------------------------------------------------------
 * Function:	H5F__accum_reset
 *
 * Purpose:	Reset the metadata accumulator for the file
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__accum_reset(H5F_shared_t *f_sh, bool flush)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f_sh);

    /* Flush any dirty data in accumulator, if requested */
    if (flush)
        if (H5F__accum_flush(f_sh) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "can't flush metadata accumulator");

    /* Check if we need to reset the metadata accumulator information */
    if (f_sh->feature_flags & H5FD_FEAT_ACCUMULATE_METADATA) {
        /* Free the buffer */
        if (f_sh->accum.buf)
            f_sh->accum.buf = H5FL_BLK_FREE(meta_accum, f_sh->accum.buf);

        /* Reset the buffer sizes & location */
        f_sh->accum.alloc_size = f_sh->accum.size = 0;
        f_sh->accum.loc                           = HADDR_UNDEF;
        f_sh->accum.dirty                         = false;
        f_sh->accum.dirty_len                     = 0;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__accum_reset() */
