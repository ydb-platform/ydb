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
 * Created:             H5Fio.c
 *
 * Purpose:             File I/O routines.
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
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5PBprivate.h" /* Page Buffer				*/

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
 * Function:	H5F_shared_block_read
 *
 * Purpose:	Reads some data from a file/server/etc into a buffer.
 *		The data is contiguous.	 The address is relative to the base
 *		address for the file.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_shared_block_read(H5F_shared_t *f_sh, H5FD_mem_t type, haddr_t addr, size_t size, void *buf /*out*/)
{
    H5FD_mem_t map_type;            /* Mapped memory type */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f_sh);
    assert(buf);
    assert(H5_addr_defined(addr));

    /* Check for attempting I/O on 'temporary' file address */
    if (H5_addr_le(f_sh->tmp_addr, (addr + size)))
        HGOTO_ERROR(H5E_IO, H5E_BADRANGE, FAIL, "attempting I/O in temporary file space");

    /* Treat global heap as raw data */
    map_type = (type == H5FD_MEM_GHEAP) ? H5FD_MEM_DRAW : type;

    /* Pass through page buffer layer */
    if (H5PB_read(f_sh, map_type, addr, size, buf) < 0)
        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "read through page buffer failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_shared_block_read() */

/*-------------------------------------------------------------------------
 * Function:	H5F_block_read
 *
 * Purpose:	Reads some data from a file/server/etc into a buffer.
 *		The data is contiguous.	 The address is relative to the base
 *		address for the file.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_block_read(H5F_t *f, H5FD_mem_t type, haddr_t addr, size_t size, void *buf /*out*/)
{
    H5FD_mem_t map_type;            /* Mapped memory type */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(buf);
    assert(H5_addr_defined(addr));

    /* Check for attempting I/O on 'temporary' file address */
    if (H5_addr_le(f->shared->tmp_addr, (addr + size)))
        HGOTO_ERROR(H5E_IO, H5E_BADRANGE, FAIL, "attempting I/O in temporary file space");

    /* Treat global heap as raw data */
    map_type = (type == H5FD_MEM_GHEAP) ? H5FD_MEM_DRAW : type;

    /* Pass through page buffer layer */
    if (H5PB_read(f->shared, map_type, addr, size, buf) < 0)
        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "read through page buffer failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_block_read() */

/*-------------------------------------------------------------------------
 * Function:	H5F_shared_block_write
 *
 * Purpose:	Writes some data from memory to a file/server/etc.  The
 *		data is contiguous.  The address is relative to the base
 *		address.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_shared_block_write(H5F_shared_t *f_sh, H5FD_mem_t type, haddr_t addr, size_t size, const void *buf)
{
    H5FD_mem_t map_type;            /* Mapped memory type */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f_sh);
    assert(H5F_SHARED_INTENT(f_sh) & H5F_ACC_RDWR);
    assert(buf);
    assert(H5_addr_defined(addr));

    /* Check for attempting I/O on 'temporary' file address */
    if (H5_addr_le(f_sh->tmp_addr, (addr + size)))
        HGOTO_ERROR(H5E_IO, H5E_BADRANGE, FAIL, "attempting I/O in temporary file space");

    /* Treat global heap as raw data */
    map_type = (type == H5FD_MEM_GHEAP) ? H5FD_MEM_DRAW : type;

    /* Pass through page buffer layer */
    if (H5PB_write(f_sh, map_type, addr, size, buf) < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "write through page buffer failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_shared_block_write() */

/*-------------------------------------------------------------------------
 * Function:	H5F_block_write
 *
 * Purpose:	Writes some data from memory to a file/server/etc.  The
 *		data is contiguous.  The address is relative to the base
 *		address.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_block_write(H5F_t *f, H5FD_mem_t type, haddr_t addr, size_t size, const void *buf)
{
    H5FD_mem_t map_type;            /* Mapped memory type */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(H5F_INTENT(f) & H5F_ACC_RDWR);
    assert(buf);
    assert(H5_addr_defined(addr));

    /* Check for attempting I/O on 'temporary' file address */
    if (H5_addr_le(f->shared->tmp_addr, (addr + size)))
        HGOTO_ERROR(H5E_IO, H5E_BADRANGE, FAIL, "attempting I/O in temporary file space");

    /* Treat global heap as raw data */
    map_type = (type == H5FD_MEM_GHEAP) ? H5FD_MEM_DRAW : type;

    /* Pass through page buffer layer */
    if (H5PB_write(f->shared, map_type, addr, size, buf) < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "write through page buffer failed");
done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_block_write() */

/*-------------------------------------------------------------------------
 * Function:    H5F_shared_select_read
 *
 * Purpose:     Reads some data from a file/server/etc into a buffer.
 *              The location of the data is defined by the mem_spaces and
 *              file_spaces dataspace arrays, along with the offsets
 *              array.  The addresses is relative to the base address for
 *              the file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_shared_select_read(H5F_shared_t *f_sh, H5FD_mem_t type, uint32_t count, H5S_t **mem_spaces,
                       H5S_t **file_spaces, haddr_t offsets[], size_t element_sizes[], void *bufs[] /* out */)
{
    H5FD_mem_t map_type;            /* Mapped memory type */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f_sh);
    assert((mem_spaces) || (count == 0));
    assert((file_spaces) || (count == 0));
    assert((offsets) || (count == 0));
    assert((element_sizes) || (count == 0));
    assert((bufs) || (count == 0));

    /* Treat global heap as raw data */
    map_type = (type == H5FD_MEM_GHEAP) ? H5FD_MEM_DRAW : type;

    /* Pass down to file driver layer (bypass page buffer for now) */
    if (H5FD_read_selection(f_sh->lf, map_type, count, mem_spaces, file_spaces, offsets, element_sizes,
                            bufs) < 0)
        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "selection read through file driver failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_shared_select_read() */

/*-------------------------------------------------------------------------
 * Function:    H5F_shared_select_write
 *
 * Purpose:     Writes some data from a buffer to a file/server/etc.
 *              The location of the data is defined by the mem_spaces and
 *              file_spaces dataspace arrays, along with the offsets
 *              array.  The addresses is relative to the base address for
 *              the file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_shared_select_write(H5F_shared_t *f_sh, H5FD_mem_t type, uint32_t count, H5S_t **mem_spaces,
                        H5S_t **file_spaces, haddr_t offsets[], size_t element_sizes[], const void *bufs[])
{
    H5FD_mem_t map_type;            /* Mapped memory type */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f_sh);
    assert((mem_spaces) || (count == 0));
    assert((file_spaces) || (count == 0));
    assert((offsets) || (count == 0));
    assert((element_sizes) || (count == 0));
    assert((bufs) || (count == 0));

    /* Treat global heap as raw data */
    map_type = (type == H5FD_MEM_GHEAP) ? H5FD_MEM_DRAW : type;

    /* Pass down to file driver layer (bypass page buffer for now) */
    if (H5FD_write_selection(f_sh->lf, map_type, count, mem_spaces, file_spaces, offsets, element_sizes,
                             bufs) < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "selection write through file driver failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_shared_select_write() */

herr_t
H5F_shared_vector_read(H5F_shared_t *f_sh, uint32_t count, H5FD_mem_t types[], haddr_t addrs[],
                       size_t sizes[], void *bufs[])
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f_sh);
    assert((types) || (count == 0));
    assert((addrs) || (count == 0));
    assert((sizes) || (count == 0));
    assert((bufs) || (count == 0));

    /*
     * Note that we don't try to map global heap data to raw
     * data here, as it may become expensive to check for when
     * I/O vectors are large. This may change in the future, but,
     * for now, assume the caller has done this already.
     */
#ifndef NDEBUG
    for (uint32_t i = 0; i < count; i++) {
        /* Break early if H5FD_MEM_NOLIST was specified
         * since a full 'count'-sized array may not
         * have been passed for 'types'
         */
        if (i > 0 && types[i] == H5FD_MEM_NOLIST)
            break;

        assert(types[i] != H5FD_MEM_GHEAP);
    }
#endif

    /* Pass down to file driver layer (bypass page buffer for now) */
    if (H5FD_read_vector(f_sh->lf, count, types, addrs, sizes, bufs) < 0)
        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "vector read through file driver failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5F_shared_vector_write
 *
 * Purpose:     Writes data from `count` buffers (from the `bufs` array) to
 *              a file/server/etc. at the offsets provided in the `addrs`
 *              array, with the data sizes specified in the `sizes` array
 *              and data memory types specified in the `types` array. The
 *              addresses are relative to the base address for the file.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_shared_vector_write(H5F_shared_t *f_sh, uint32_t count, H5FD_mem_t types[], haddr_t addrs[],
                        size_t sizes[], const void *bufs[])
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f_sh);
    assert((types) || (count == 0));
    assert((addrs) || (count == 0));
    assert((sizes) || (count == 0));
    assert((bufs) || (count == 0));

    /*
     * Note that we don't try to map global heap data to raw
     * data here, as it may become expensive to check for when
     * I/O vectors are large. This may change in the future, but,
     * for now, assume the caller has done this already.
     */
#ifndef NDEBUG
    for (uint32_t i = 0; i < count; i++) {
        /* Break early if H5FD_MEM_NOLIST was specified
         * since a full 'count'-sized array may not
         * have been passed for 'types'
         */
        if (i > 0 && types[i] == H5FD_MEM_NOLIST)
            break;

        assert(types[i] != H5FD_MEM_GHEAP);
    }
#endif

    /* Pass down to file driver layer (bypass page buffer for now) */
    if (H5FD_write_vector(f_sh->lf, count, types, addrs, sizes, bufs) < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "vector write through file driver failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5F_flush_tagged_metadata
 *
 * Purpose:     Flushes metadata with specified tag in the metadata cache
 *              to disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_flush_tagged_metadata(H5F_t *f, haddr_t tag)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Use tag to search for and flush associated metadata */
    if (H5AC_flush_tagged_metadata(f, tag) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to flush tagged metadata");

    /* Flush and reset the accumulator */
    if (H5F__accum_reset(f->shared, true) < 0)
        HGOTO_ERROR(H5E_IO, H5E_CANTRESET, FAIL, "can't reset accumulator");

    /* Flush file buffers to disk. */
    if (H5FD_flush(f->shared->lf, false) < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "low level flush failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_flush_tagged_metadata */

/*-------------------------------------------------------------------------
 * Function:    H5F__evict_cache_entries
 *
 * Purpose:     To evict all cache entries except the pinned superblock entry
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__evict_cache_entries(H5F_t *f)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(f->shared);

    /* Evict all except pinned entries in the cache */
    if (H5AC_evict(f) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTEXPUNGE, FAIL, "unable to evict all except pinned entries");

#ifndef NDEBUG
    {
        unsigned status = 0;
        uint32_t cur_num_entries;

        /* Retrieve status of the superblock */
        if (H5AC_get_entry_status(f, (haddr_t)0, &status) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "unable to get entry status");

        /* Verify status of the superblock entry in the cache */
        if (!(status & H5AC_ES__IN_CACHE) || !(status & H5AC_ES__IS_PINNED))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "unable to get entry status");

        /* Get the number of cache entries */
        if (H5AC_get_cache_size(f->shared->cache, NULL, NULL, NULL, &cur_num_entries) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5AC_get_cache_size() failed.");

        /* Should be the only one left in the cache (the superblock) */
        if (cur_num_entries != 1)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "number of cache entries is not correct");
    }
#endif /* NDEBUG */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__evict_cache_entries() */

/*-------------------------------------------------------------------------
 * Function:    H5F_get_checksums
 *
 * Purpose:   	Decode checksum stored in the buffer
 *		Calculate checksum for the data in the buffer
 *
 * Note:	Assumes that the checksum is the last data in the buffer
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_get_checksums(const uint8_t *buf, size_t buf_size, uint32_t *s_chksum /*out*/, uint32_t *c_chksum /*out*/)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments */
    assert(buf);
    assert(buf_size);

    /* Return the stored checksum */
    if (s_chksum) {
        const uint8_t *chk_p; /* Pointer into raw data buffer */

        /* Offset to the checksum in the buffer */
        chk_p = buf + buf_size - H5_SIZEOF_CHKSUM;

        /* Decode the checksum stored in the buffer */
        UINT32DECODE(chk_p, *s_chksum);
    } /* end if */

    /* Return the computed checksum for the buffer */
    if (c_chksum)
        *c_chksum = H5_checksum_metadata(buf, buf_size - H5_SIZEOF_CHKSUM, 0);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5F_get_chksums() */
