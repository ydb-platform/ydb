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

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Dpkg.h"      /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5HLprivate.h" /* Local Heaps                              */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5VMprivate.h" /* Vector and array functions               */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Callback info for readvv operation */
typedef struct H5D_efl_readvv_ud_t {
    const H5O_efl_t *efl;  /* Pointer to efl info */
    const H5D_t     *dset; /* The dataset */
    unsigned char   *rbuf; /* Read buffer */
} H5D_efl_readvv_ud_t;

/* Callback info for writevv operation */
typedef struct H5D_efl_writevv_ud_t {
    const H5O_efl_t     *efl;  /* Pointer to efl info */
    const H5D_t         *dset; /* The dataset */
    const unsigned char *wbuf; /* Write buffer */
} H5D_efl_writevv_ud_t;

/********************/
/* Local Prototypes */
/********************/

/* Layout operation callbacks */
static herr_t  H5D__efl_construct(H5F_t *f, H5D_t *dset);
static herr_t  H5D__efl_io_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static ssize_t H5D__efl_readvv(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                               size_t dset_max_nseq, size_t *dset_curr_seq, size_t dset_len_arr[],
                               hsize_t dset_offset_arr[], size_t mem_max_nseq, size_t *mem_curr_seq,
                               size_t mem_len_arr[], hsize_t mem_offset_arr[]);
static ssize_t H5D__efl_writevv(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                                size_t dset_max_nseq, size_t *dset_curr_seq, size_t dset_len_arr[],
                                hsize_t dset_offset_arr[], size_t mem_max_nseq, size_t *mem_curr_seq,
                                size_t mem_len_arr[], hsize_t mem_offset_arr[]);

/* Helper routines */
static herr_t H5D__efl_read(const H5O_efl_t *efl, const H5D_t *dset, haddr_t addr, size_t size, uint8_t *buf);
static herr_t H5D__efl_write(const H5O_efl_t *efl, const H5D_t *dset, haddr_t addr, size_t size,
                             const uint8_t *buf);

/*********************/
/* Package Variables */
/*********************/

/* External File List (EFL) storage layout I/O ops */
const H5D_layout_ops_t H5D_LOPS_EFL[1] = {{
    H5D__efl_construct,      /* construct */
    NULL,                    /* init */
    H5D__efl_is_space_alloc, /* is_space_alloc */
    NULL,                    /* is_data_cached */
    H5D__efl_io_init,        /* io_init */
    NULL,                    /* mdio_init */
    H5D__contig_read,        /* ser_read */
    H5D__contig_write,       /* ser_write */
    H5D__efl_readvv,         /* readvv */
    H5D__efl_writevv,        /* writevv */
    NULL,                    /* flush */
    NULL,                    /* io_term */
    NULL                     /* dest */
}};

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5D__efl_construct
 *
 * Purpose:	Constructs new EFL layout information for dataset
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__efl_construct(H5F_t *f, H5D_t *dset)
{
    size_t   dt_size;             /* Size of datatype */
    hssize_t stmp_size;           /* Temporary holder for raw data size */
    hsize_t  tmp_size;            /* Temporary holder for raw data size */
    hsize_t  max_points;          /* Maximum elements */
    hsize_t  max_storage;         /* Maximum storage size */
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(dset);

    /*
     * The maximum size of the dataset cannot exceed the storage size.
     * Also, only the slowest varying dimension of a simple dataspace
     * can be extendible (currently only for external data storage).
     */

    /* Check for invalid dataset dimensions */
    for (u = 1; u < dset->shared->ndims; u++)
        if (dset->shared->max_dims[u] > dset->shared->curr_dims[u])
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "only the first dimension can be extendible");

    /* Retrieve the size of the dataset's datatype */
    if (0 == (dt_size = H5T_get_size(dset->shared->type)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to determine datatype size");

    /* Check for storage overflows */
    max_points  = H5S_get_npoints_max(dset->shared->space);
    max_storage = H5O_efl_total_size(&dset->shared->dcpl_cache.efl);
    if (H5S_UNLIMITED == max_points) {
        if (H5O_EFL_UNLIMITED != max_storage)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unlimited dataspace but finite storage");
    } /* end if */
    else if ((max_points * dt_size) < max_points)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "dataspace * type size overflowed");
    else if ((max_points * dt_size) > max_storage)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "dataspace size exceeds external storage size");

    /* Compute the total size of dataset */
    stmp_size = H5S_GET_EXTENT_NPOINTS(dset->shared->space);
    assert(stmp_size >= 0);
    tmp_size = (hsize_t)stmp_size * dt_size;
    H5_CHECKED_ASSIGN(dset->shared->layout.storage.u.contig.size, hsize_t, tmp_size, hssize_t);

    /* Get the sieve buffer size for this dataset */
    dset->shared->cache.contig.sieve_buf_size = H5F_SIEVE_BUF_SIZE(f);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__efl_construct() */

/*-------------------------------------------------------------------------
 * Function:	H5D__efl_is_space_alloc
 *
 * Purpose:	Query if space is allocated for layout
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
bool
H5D__efl_is_space_alloc(const H5O_storage_t H5_ATTR_UNUSED *storage)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(storage);

    /* EFL storage is currently always treated as allocated */
    FUNC_LEAVE_NOAPI(true)
} /* end H5D__efl_is_space_alloc() */

/*-------------------------------------------------------------------------
 * Function:	H5D__efl_io_init
 *
 * Purpose:	Performs initialization before any sort of I/O on the raw data
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__efl_io_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo)
{
    FUNC_ENTER_PACKAGE_NOERR

    H5MM_memcpy(&dinfo->store->efl, &(dinfo->dset->shared->dcpl_cache.efl), sizeof(H5O_efl_t));

    /* No "pieces" selected */
    dinfo->layout_io_info.contig_piece_info = NULL;

    /* Disable selection I/O */
    io_info->use_select_io = H5D_SELECTION_IO_MODE_OFF;
    io_info->no_selection_io_cause |= H5D_SEL_IO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__efl_io_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__efl_read
 *
 * Purpose:     Reads data from an external file list.  It is an error to
 *              read past the logical end of file, but reading past the end
 *              of any particular member of the external file list results in
 *              zeros.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__efl_read(const H5O_efl_t *efl, const H5D_t *dset, haddr_t addr, size_t size, uint8_t *buf)
{
    int    fd = -1;
    size_t to_read;
#ifndef NDEBUG
    hsize_t tempto_read;
#endif /* NDEBUG */
    hsize_t skip = 0;
    haddr_t cur;
    ssize_t n;
    size_t  u;                   /* Local index variable */
    char   *full_name = NULL;    /* File name with prefix */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(efl && efl->nused > 0);
    assert(H5_addr_defined(addr));
    assert(size < SIZE_MAX);
    assert(buf || 0 == size);

    /* Find the first efl member from which to read */
    for (u = 0, cur = 0; u < efl->nused; u++) {
        if (H5O_EFL_UNLIMITED == efl->slot[u].size || addr < cur + efl->slot[u].size) {
            skip = addr - cur;
            break;
        } /* end if */
        cur += efl->slot[u].size;
    } /* end for */

    /* Read the data */
    while (size) {
        assert(buf);
        if (u >= efl->nused)
            HGOTO_ERROR(H5E_EFL, H5E_OVERFLOW, FAIL, "read past logical end of file");
        if (H5F_OVERFLOW_HSIZET2OFFT((hsize_t)efl->slot[u].offset + skip))
            HGOTO_ERROR(H5E_EFL, H5E_OVERFLOW, FAIL, "external file address overflowed");
        if (H5_combine_path(dset->shared->extfile_prefix, efl->slot[u].name, &full_name) < 0)
            HGOTO_ERROR(H5E_EFL, H5E_NOSPACE, FAIL, "can't build external file name");
        if ((fd = HDopen(full_name, O_RDONLY)) < 0)
            HGOTO_ERROR(H5E_EFL, H5E_CANTOPENFILE, FAIL, "unable to open external raw data file");
        if (HDlseek(fd, (HDoff_t)(efl->slot[u].offset + (HDoff_t)skip), SEEK_SET) < 0)
            HGOTO_ERROR(H5E_EFL, H5E_SEEKERROR, FAIL, "unable to seek in external raw data file");
#ifndef NDEBUG
        tempto_read = MIN((size_t)(efl->slot[u].size - skip), (hsize_t)size);
        H5_CHECK_OVERFLOW(tempto_read, hsize_t, size_t);
        to_read = (size_t)tempto_read;
#else  /* NDEBUG */
        to_read  = MIN((size_t)(efl->slot[u].size - skip), (hsize_t)size);
#endif /* NDEBUG */
        if ((n = HDread(fd, buf, to_read)) < 0)
            HGOTO_ERROR(H5E_EFL, H5E_READERROR, FAIL, "read error in external raw data file");
        else if ((size_t)n < to_read)
            memset(buf + n, 0, to_read - (size_t)n);
        full_name = (char *)H5MM_xfree(full_name);
        HDclose(fd);
        fd = -1;
        size -= to_read;
        buf += to_read;
        skip = 0;
        u++;
    } /* end while */

done:
    if (full_name)
        full_name = (char *)H5MM_xfree(full_name);
    if (fd >= 0)
        HDclose(fd);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__efl_read() */

/*-------------------------------------------------------------------------
 * Function:	H5D__efl_write
 *
 * Purpose:	Writes data to an external file list.  It is an error to
 *		write past the logical end of file, but writing past the end
 *		of any particular member of the external file list just
 *		extends that file.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__efl_write(const H5O_efl_t *efl, const H5D_t *dset, haddr_t addr, size_t size, const uint8_t *buf)
{
    int    fd = -1;
    size_t to_write;
#ifndef NDEBUG
    hsize_t tempto_write;
#endif /* NDEBUG */
    haddr_t cur;
    hsize_t skip = 0;
    size_t  u;                   /* Local index variable */
    char   *full_name = NULL;    /* File name with prefix */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(efl && efl->nused > 0);
    assert(H5_addr_defined(addr));
    assert(size < SIZE_MAX);
    assert(buf || 0 == size);

    /* Find the first efl member in which to write */
    for (u = 0, cur = 0; u < efl->nused; u++) {
        if (H5O_EFL_UNLIMITED == efl->slot[u].size || addr < cur + efl->slot[u].size) {
            skip = addr - cur;
            break;
        } /* end if */
        cur += efl->slot[u].size;
    } /* end for */

    /* Write the data */
    while (size) {
        assert(buf);
        if (u >= efl->nused)
            HGOTO_ERROR(H5E_EFL, H5E_OVERFLOW, FAIL, "write past logical end of file");
        if (H5F_OVERFLOW_HSIZET2OFFT((hsize_t)efl->slot[u].offset + skip))
            HGOTO_ERROR(H5E_EFL, H5E_OVERFLOW, FAIL, "external file address overflowed");
        if (H5_combine_path(dset->shared->extfile_prefix, efl->slot[u].name, &full_name) < 0)
            HGOTO_ERROR(H5E_EFL, H5E_NOSPACE, FAIL, "can't build external file name");
        if ((fd = HDopen(full_name, O_CREAT | O_RDWR, H5_POSIX_CREATE_MODE_RW)) < 0) {
            if (HDaccess(full_name, F_OK) < 0)
                HGOTO_ERROR(H5E_EFL, H5E_CANTOPENFILE, FAIL, "external raw data file does not exist");
            else
                HGOTO_ERROR(H5E_EFL, H5E_CANTOPENFILE, FAIL, "unable to open external raw data file");
        } /* end if */
        if (HDlseek(fd, (HDoff_t)(efl->slot[u].offset + (HDoff_t)skip), SEEK_SET) < 0)
            HGOTO_ERROR(H5E_EFL, H5E_SEEKERROR, FAIL, "unable to seek in external raw data file");
#ifndef NDEBUG
        tempto_write = MIN(efl->slot[u].size - skip, (hsize_t)size);
        H5_CHECK_OVERFLOW(tempto_write, hsize_t, size_t);
        to_write = (size_t)tempto_write;
#else  /* NDEBUG */
        to_write = MIN((size_t)(efl->slot[u].size - skip), size);
#endif /* NDEBUG */
        if ((size_t)HDwrite(fd, buf, to_write) != to_write)
            HGOTO_ERROR(H5E_EFL, H5E_READERROR, FAIL, "write error in external raw data file");
        full_name = (char *)H5MM_xfree(full_name);
        HDclose(fd);
        fd = -1;
        size -= to_write;
        buf += to_write;
        skip = 0;
        u++;
    } /* end while */

done:
    if (full_name)
        full_name = (char *)H5MM_xfree(full_name);
    if (fd >= 0)
        HDclose(fd);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__efl_write() */

/*-------------------------------------------------------------------------
 * Function:	H5D__efl_readvv_cb
 *
 * Purpose:	Callback operator for H5D__efl_readvv().
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__efl_readvv_cb(hsize_t dst_off, hsize_t src_off, size_t len, void *_udata)
{
    H5D_efl_readvv_ud_t *udata     = (H5D_efl_readvv_ud_t *)_udata; /* User data for H5VM_opvv() operator */
    herr_t               ret_value = SUCCEED;                       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Read data */
    if (H5D__efl_read(udata->efl, udata->dset, dst_off, len, (udata->rbuf + src_off)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "EFL read failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__efl_readvv_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5D__efl_readvv
 *
 * Purpose:	Reads data from an external file list.  It is an error to
 *		read past the logical end of file, but reading past the end
 *		of any particular member of the external file list results in
 *		zeros.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5D__efl_readvv(const H5D_io_info_t H5_ATTR_NDEBUG_UNUSED *io_info, const H5D_dset_io_info_t *dset_info,
                size_t dset_max_nseq, size_t *dset_curr_seq, size_t dset_len_arr[], hsize_t dset_off_arr[],
                size_t mem_max_nseq, size_t *mem_curr_seq, size_t mem_len_arr[], hsize_t mem_off_arr[])
{
    H5D_efl_readvv_ud_t udata;          /* User data for H5VM_opvv() operator */
    ssize_t             ret_value = -1; /* Return value (Total size of sequence in bytes) */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(io_info);
    assert(dset_info);
    assert(dset_info->store->efl.nused > 0);
    assert(dset_info->buf.vp);
    assert(dset_info->dset);
    assert(dset_info->dset->shared);
    assert(dset_curr_seq);
    assert(dset_len_arr);
    assert(dset_off_arr);
    assert(mem_curr_seq);
    assert(mem_len_arr);
    assert(mem_off_arr);

    /* Set up user data for H5VM_opvv() */
    udata.efl  = &(dset_info->store->efl);
    udata.dset = dset_info->dset;
    udata.rbuf = (unsigned char *)dset_info->buf.vp;

    /* Call generic sequence operation routine */
    if ((ret_value = H5VM_opvv(dset_max_nseq, dset_curr_seq, dset_len_arr, dset_off_arr, mem_max_nseq,
                               mem_curr_seq, mem_len_arr, mem_off_arr, H5D__efl_readvv_cb, &udata)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL, "can't perform vectorized EFL read");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__efl_readvv() */

/*-------------------------------------------------------------------------
 * Function:	H5D__efl_writevv_cb
 *
 * Purpose:	Callback operator for H5D__efl_writevv().
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__efl_writevv_cb(hsize_t dst_off, hsize_t src_off, size_t len, void *_udata)
{
    H5D_efl_writevv_ud_t *udata     = (H5D_efl_writevv_ud_t *)_udata; /* User data for H5VM_opvv() operator */
    herr_t                ret_value = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Write data */
    if (H5D__efl_write(udata->efl, udata->dset, dst_off, len, (udata->wbuf + src_off)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "EFL write failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__efl_writevv_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5D__efl_writevv
 *
 * Purpose:	Writes data to an external file list.  It is an error to
 *		write past the logical end of file, but writing past the end
 *		of any particular member of the external file list just
 *		extends that file.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5D__efl_writevv(const H5D_io_info_t H5_ATTR_NDEBUG_UNUSED *io_info, const H5D_dset_io_info_t *dset_info,
                 size_t dset_max_nseq, size_t *dset_curr_seq, size_t dset_len_arr[], hsize_t dset_off_arr[],
                 size_t mem_max_nseq, size_t *mem_curr_seq, size_t mem_len_arr[], hsize_t mem_off_arr[])
{
    H5D_efl_writevv_ud_t udata;          /* User data for H5VM_opvv() operator */
    ssize_t              ret_value = -1; /* Return value (Total size of sequence in bytes) */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(io_info);
    assert(dset_info);
    assert(dset_info->store->efl.nused > 0);
    assert(dset_info->buf.cvp);
    assert(dset_info->dset);
    assert(dset_info->dset->shared);
    assert(dset_curr_seq);
    assert(dset_len_arr);
    assert(dset_off_arr);
    assert(mem_curr_seq);
    assert(mem_len_arr);
    assert(mem_off_arr);

    /* Set up user data for H5VM_opvv() */
    udata.efl  = &(dset_info->store->efl);
    udata.dset = dset_info->dset;
    udata.wbuf = (const unsigned char *)dset_info->buf.cvp;

    /* Call generic sequence operation routine */
    if ((ret_value = H5VM_opvv(dset_max_nseq, dset_curr_seq, dset_len_arr, dset_off_arr, mem_max_nseq,
                               mem_curr_seq, mem_len_arr, mem_off_arr, H5D__efl_writevv_cb, &udata)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL, "can't perform vectorized EFL write");
done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__efl_writevv() */

/*-------------------------------------------------------------------------
 * Function:    H5D__efl_bh_info
 *
 * Purpose:     Retrieve the amount of heap storage used for External File
 *		List message
 *
 * Return:      Success:        Non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__efl_bh_info(H5F_t *f, H5O_efl_t *efl, hsize_t *heap_size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f);
    assert(efl);
    assert(H5_addr_defined(efl->heap_addr));
    assert(heap_size);

    /* Get the size of the local heap for EFL's file list */
    if (H5HL_heapsize(f, efl->heap_addr, heap_size) < 0)
        HGOTO_ERROR(H5E_EFL, H5E_CANTINIT, FAIL, "unable to retrieve local heap info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__efl_bh_info() */
