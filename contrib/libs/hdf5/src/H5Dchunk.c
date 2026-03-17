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

/* Purpose: Abstract indexed (chunked) I/O functions.  The logical
 *          multi-dimensional dataspace is regularly partitioned into
 *          same-sized "chunks", the first of which is aligned with the
 *          logical origin.  The chunks are indexed by different methods,
 *          that map a chunk index to disk address.  Each chunk can be
 *          compressed independently and the chunks may move around in the
 *          file as their storage requirements change.
 *
 * Cache:   Disk I/O is performed in units of chunks and H5MF_alloc()
 *          contains code to optionally align chunks on disk block
 *          boundaries for performance.
 *
 *          The chunk cache is an extendible hash indexed by a function
 *          of storage B-tree address and chunk N-dimensional offset
 *          within the dataset.  Collisions are not resolved -- one of
 *          the two chunks competing for the hash slot must be preempted
 *          from the cache.  All entries in the hash also participate in
 *          a doubly-linked list and entries are penalized by moving them
 *          toward the front of the list.  When a new chunk is about to
 *          be added to the cache the heap is pruned by preempting
 *          entries near the front of the list to make room for the new
 *          entry which is added to the end of the list.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h" /* Generic Functions            */
#ifdef H5_HAVE_PARALLEL
#include "H5ACprivate.h" /* Metadata cache            */
#endif                   /* H5_HAVE_PARALLEL */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Dpkg.h"      /* Dataset functions            */
#include "H5Eprivate.h"  /* Error handling              */
#include "H5Fprivate.h"  /* File functions            */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5MMprivate.h" /* Memory management            */
#include "H5MFprivate.h" /* File memory management               */
#include "H5PBprivate.h" /* Page Buffer	                         */
#include "H5VMprivate.h" /* Vector and array functions        */

/****************/
/* Local Macros */
/****************/

/* Macros for iterating over chunks to operate on */
#define H5D_CHUNK_GET_FIRST_NODE(dinfo)                                                                      \
    (dinfo->layout_io_info.chunk_map->use_single                                                             \
         ? (H5SL_node_t *)(1)                                                                                \
         : H5SL_first(dinfo->layout_io_info.chunk_map->dset_sel_pieces))
#define H5D_CHUNK_GET_NODE_INFO(dinfo, node)                                                                 \
    (dinfo->layout_io_info.chunk_map->use_single ? dinfo->layout_io_info.chunk_map->single_piece_info        \
                                                 : (H5D_piece_info_t *)H5SL_item(node))
#define H5D_CHUNK_GET_NEXT_NODE(dinfo, node)                                                                 \
    (dinfo->layout_io_info.chunk_map->use_single ? (H5SL_node_t *)NULL : H5SL_next(node))
#define H5D_CHUNK_GET_NODE_COUNT(dinfo)                                                                      \
    (dinfo->layout_io_info.chunk_map->use_single                                                             \
         ? (size_t)1                                                                                         \
         : H5SL_count(dinfo->layout_io_info.chunk_map->dset_sel_pieces))

/* Sanity check on chunk index types: commonly used by a lot of routines in this file */
#define H5D_CHUNK_STORAGE_INDEX_CHK(storage)                                                                 \
    do {                                                                                                     \
        assert((H5D_CHUNK_IDX_EARRAY == (storage)->idx_type && H5D_COPS_EARRAY == (storage)->ops) ||         \
               (H5D_CHUNK_IDX_FARRAY == (storage)->idx_type && H5D_COPS_FARRAY == (storage)->ops) ||         \
               (H5D_CHUNK_IDX_BT2 == (storage)->idx_type && H5D_COPS_BT2 == (storage)->ops) ||               \
               (H5D_CHUNK_IDX_BTREE == (storage)->idx_type && H5D_COPS_BTREE == (storage)->ops) ||           \
               (H5D_CHUNK_IDX_SINGLE == (storage)->idx_type && H5D_COPS_SINGLE == (storage)->ops) ||         \
               (H5D_CHUNK_IDX_NONE == (storage)->idx_type && H5D_COPS_NONE == (storage)->ops));              \
    } while (0)
/*
 * Feature: If this constant is defined then every cache preemption and load
 *        causes a character to be printed on the standard error stream:
 *
 *     `.': Entry was preempted because it has been completely read or
 *        completely written but not partially read and not partially
 *        written. This is often a good reason for preemption because such
 *        a chunk will be unlikely to be referenced in the near future.
 *
 *     `:': Entry was preempted because it hasn't been used recently.
 *
 *     `#': Entry was preempted because another chunk collided with it. This
 *        is usually a relatively bad thing.  If there are too many of
 *        these then the number of entries in the cache can be increased.
 *
 *       c: Entry was preempted because the file is closing.
 *
 *     w: A chunk read operation was eliminated because the library is
 *        about to write new values to the entire chunk.  This is a good
 *        thing, especially on files where the chunk size is the same as
 *        the disk block size, chunks are aligned on disk block boundaries,
 *        and the operating system can also eliminate a read operation.
 */

/*#define H5D_CHUNK_DEBUG */

/* Flags for the "edge_chunk_state" field below */
#define H5D_RDCC_DISABLE_FILTERS 0x01U /* Disable filters on this chunk */
#define H5D_RDCC_NEWLY_DISABLED_FILTERS                                                                      \
    0x02U /* Filters have been disabled since                                                                \
           * the last flush */

/******************/
/* Local Typedefs */
/******************/

/* Raw data chunks are cached.  Each entry in the cache is: */
typedef struct H5D_rdcc_ent_t {
    bool                   locked;                   /*entry is locked in cache        */
    bool                   dirty;                    /*needs to be written to disk?        */
    bool                   deleted;                  /*chunk about to be deleted        */
    unsigned               edge_chunk_state;         /*states related to edge chunks (see above) */
    hsize_t                scaled[H5O_LAYOUT_NDIMS]; /*scaled chunk 'name' (coordinates) */
    uint32_t               rd_count;                 /*bytes remaining to be read        */
    uint32_t               wr_count;                 /*bytes remaining to be written        */
    H5F_block_t            chunk_block;              /*offset/length of chunk in file        */
    hsize_t                chunk_idx;                /*index of chunk in dataset             */
    uint8_t               *chunk;                    /*the unfiltered chunk data        */
    unsigned               idx;                      /*index in hash table            */
    struct H5D_rdcc_ent_t *next;                     /*next item in doubly-linked list    */
    struct H5D_rdcc_ent_t *prev;                     /*previous item in doubly-linked list    */
    struct H5D_rdcc_ent_t *tmp_next;                 /*next item in temporary doubly-linked list */
    struct H5D_rdcc_ent_t *tmp_prev;                 /*previous item in temporary doubly-linked list */
} H5D_rdcc_ent_t;
typedef H5D_rdcc_ent_t *H5D_rdcc_ent_ptr_t; /* For free lists */

/* Callback info for iteration to prune chunks */
typedef struct H5D_chunk_it_ud1_t {
    H5D_chunk_common_ud_t     common;          /* Common info for B-tree user data (must be first) */
    const H5D_chk_idx_info_t *idx_info;        /* Chunked index info */
    const H5D_io_info_t      *io_info;         /* I/O info for dataset operation */
    const H5D_dset_io_info_t *dset_info;       /* Dataset specific I/O info */
    const hsize_t            *space_dim;       /* New dataset dimensions    */
    const bool               *shrunk_dim;      /* Dimensions which have been shrunk */
    H5S_t                    *chunk_space;     /* Dataspace for a chunk */
    uint32_t                  elmts_per_chunk; /* Elements in chunk */
    hsize_t                  *hyper_start;     /* Starting location of hyperslab */
    H5D_fill_buf_info_t       fb_info;         /* Dataset's fill buffer info */
    bool                      fb_info_init;    /* Whether the fill value buffer has been initialized */
} H5D_chunk_it_ud1_t;

/* Callback info for iteration to obtain chunk address and the index of the chunk for all chunks in the
 * B-tree. */
typedef struct H5D_chunk_it_ud2_t {
    /* down */
    H5D_chunk_common_ud_t common; /* Common info for B-tree user data (must be first) */

    /* up */
    haddr_t *chunk_addr; /* Array of chunk addresses to fill in */
} H5D_chunk_it_ud2_t;

/* Callback info for iteration to copy data */
typedef struct H5D_chunk_it_ud3_t {
    H5D_chunk_common_ud_t common;       /* Common info for B-tree user data (must be first) */
    H5F_t                *file_src;     /* Source file for copy */
    H5D_chk_idx_info_t   *idx_info_dst; /* Dest. chunk index info object */
    void                 *buf;          /* Buffer to hold chunk data for read/write */
    void                 *bkg;          /* Buffer for background information during type conversion */
    size_t                buf_size;     /* Buffer size */
    bool                  do_convert;   /* Whether to perform type conversions */

    /* needed for converting variable-length data */
    hid_t        tid_src;          /* Datatype ID for source datatype */
    hid_t        tid_dst;          /* Datatype ID for destination datatype */
    hid_t        tid_mem;          /* Datatype ID for memory datatype */
    const H5T_t *dt_src;           /* Source datatype */
    H5T_path_t  *tpath_src_mem;    /* Datatype conversion path from source file to memory */
    H5T_path_t  *tpath_mem_dst;    /* Datatype conversion path from memory to dest. file */
    void        *reclaim_buf;      /* Buffer for reclaiming data */
    size_t       reclaim_buf_size; /* Reclaim buffer size */
    uint32_t     nelmts;           /* Number of elements in buffer */
    H5S_t       *buf_space;        /* Dataspace describing buffer */

    /* needed for compressed variable-length data */
    const H5O_pline_t *pline;      /* Filter pipeline */
    unsigned           dset_ndims; /* Number of dimensions in dataset */
    const hsize_t     *dset_dims;  /* Dataset dimensions */

    /* needed for copy object pointed by refs */
    H5O_copy_t *cpy_info; /* Copy options */

    /* needed for getting raw data from chunk cache */
    bool     chunk_in_cache;
    uint8_t *chunk; /* the unfiltered chunk data        */
} H5D_chunk_it_ud3_t;

/* Callback info for iteration to dump index */
typedef struct H5D_chunk_it_ud4_t {
    FILE     *stream;           /* Output stream    */
    bool      header_displayed; /* Node's header is displayed? */
    unsigned  ndims;            /* Number of dimensions for chunk/dataset */
    uint32_t *chunk_dim;        /* Chunk dimensions */
} H5D_chunk_it_ud4_t;

/* Callback info for iteration to format convert chunks */
typedef struct H5D_chunk_it_ud5_t {
    H5D_chk_idx_info_t *new_idx_info; /* Dest. chunk index info object */
    unsigned            dset_ndims;   /* Number of dimensions in dataset */
    hsize_t            *dset_dims;    /* Dataset dimensions */
} H5D_chunk_it_ud5_t;

/* Callback info for nonexistent readvv operation */
typedef struct H5D_chunk_readvv_ud_t {
    unsigned char *rbuf; /* Read buffer to initialize */
    const H5D_t   *dset; /* Dataset to operate on */
} H5D_chunk_readvv_ud_t;

/* Typedef for chunk info iterator callback */
typedef struct H5D_chunk_info_iter_ud_t {
    hsize_t  scaled[H5O_LAYOUT_NDIMS]; /* Logical offset of the chunk */
    hsize_t  ndims;                    /* Number of dimensions in the dataset */
    uint32_t nbytes;                   /* Size of stored data in the chunk */
    unsigned filter_mask;              /* Excluded filters */
    haddr_t  chunk_addr;               /* Address of the chunk in file */
    hsize_t  chunk_idx;                /* Chunk index, where the iteration needs to stop */
    hsize_t  curr_idx;                 /* Current index, where the iteration is */
    unsigned idx_hint;                 /* Index of chunk in cache, if present */
    bool     found;                    /* Whether the chunk was found */
} H5D_chunk_info_iter_ud_t;

#ifdef H5_HAVE_PARALLEL
/* information to construct a collective I/O operation for filling chunks */
typedef struct H5D_chunk_coll_fill_info_t {
    size_t num_chunks; /* Number of chunks in the write operation */
    struct chunk_coll_fill_info {
        haddr_t addr;       /* File address of the chunk */
        size_t  chunk_size; /* Size of the chunk in the file */
        bool    unfiltered_partial_chunk;
    } * chunk_info;
} H5D_chunk_coll_fill_info_t;
#endif /* H5_HAVE_PARALLEL */

typedef struct H5D_chunk_iter_ud_t {
    H5D_chunk_iter_op_t op;      /* User defined callback */
    void               *op_data; /* User data for user defined callback */
    H5O_layout_chunk_t *chunk;   /* Chunk layout */
} H5D_chunk_iter_ud_t;

/********************/
/* Local Prototypes */
/********************/

/* Chunked layout operation callbacks */
static herr_t H5D__chunk_construct(H5F_t *f, H5D_t *dset);
static herr_t H5D__chunk_init(H5F_t *f, const H5D_t *dset, hid_t dapl_id);
static herr_t H5D__chunk_io_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static herr_t H5D__chunk_io_init_selections(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static herr_t H5D__chunk_mdio_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static herr_t H5D__chunk_read(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static herr_t H5D__chunk_write(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static herr_t H5D__chunk_flush(H5D_t *dset);
static herr_t H5D__chunk_io_term(H5D_io_info_t *io_info, H5D_dset_io_info_t *di);
static herr_t H5D__chunk_dest(H5D_t *dset);

/* Chunk query operation callbacks */
static int H5D__get_num_chunks_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata);
static int H5D__get_chunk_info_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata);
static int H5D__get_chunk_info_by_coord_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata);
static int H5D__chunk_iter_cb(const H5D_chunk_rec_t *chunk_rec, void *udata);

/* "Nonexistent" layout operation callback */
static ssize_t H5D__nonexistent_readvv(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                                       size_t chunk_max_nseq, size_t *chunk_curr_seq, size_t chunk_len_arr[],
                                       hsize_t chunk_offset_arr[], size_t mem_max_nseq, size_t *mem_curr_seq,
                                       size_t mem_len_arr[], hsize_t mem_offset_arr[]);

/* Format convert cb */
static int H5D__chunk_format_convert_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata);

/* Helper routines */
static herr_t   H5D__chunk_set_info_real(H5O_layout_chunk_t *layout, unsigned ndims, const hsize_t *curr_dims,
                                         const hsize_t *max_dims);
static herr_t   H5D__chunk_cinfo_cache_reset(H5D_chunk_cached_t *last);
static herr_t   H5D__chunk_cinfo_cache_update(H5D_chunk_cached_t *last, const H5D_chunk_ud_t *udata);
static bool     H5D__chunk_cinfo_cache_found(const H5D_chunk_cached_t *last, H5D_chunk_ud_t *udata);
static herr_t   H5D__create_piece_map_single(H5D_dset_io_info_t *di, H5D_io_info_t *io_info);
static herr_t   H5D__create_piece_file_map_all(H5D_dset_io_info_t *di, H5D_io_info_t *io_info);
static herr_t   H5D__create_piece_file_map_hyper(H5D_dset_io_info_t *di, H5D_io_info_t *io_info);
static herr_t   H5D__create_piece_mem_map_1d(const H5D_dset_io_info_t *di);
static herr_t   H5D__create_piece_mem_map_hyper(const H5D_dset_io_info_t *di);
static herr_t   H5D__piece_file_cb(void *elem, const H5T_t *type, unsigned ndims, const hsize_t *coords,
                                   void *_opdata);
static herr_t   H5D__piece_mem_cb(void *elem, const H5T_t *type, unsigned ndims, const hsize_t *coords,
                                  void *_opdata);
static herr_t   H5D__chunk_may_use_select_io(H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info);
static unsigned H5D__chunk_hash_val(const H5D_shared_t *shared, const hsize_t *scaled);
static herr_t   H5D__chunk_flush_entry(const H5D_t *dset, H5D_rdcc_ent_t *ent, bool reset);
static herr_t   H5D__chunk_cache_evict(const H5D_t *dset, H5D_rdcc_ent_t *ent, bool flush);
static void    *H5D__chunk_lock(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                                H5D_chunk_ud_t *udata, bool relax, bool prev_unfilt_chunk);
static herr_t   H5D__chunk_unlock(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                                  const H5D_chunk_ud_t *udata, bool dirty, void *chunk, uint32_t naccessed);
static herr_t   H5D__chunk_cache_prune(const H5D_t *dset, size_t size);
static herr_t   H5D__chunk_prune_fill(H5D_chunk_it_ud1_t *udata, bool new_unfilt_chunk);
#ifdef H5_HAVE_PARALLEL
static herr_t H5D__chunk_collective_fill(const H5D_t *dset, H5D_chunk_coll_fill_info_t *chunk_fill_info,
                                         const void *fill_buf, const void *partial_chunk_fill_buf);
static int    H5D__chunk_cmp_coll_fill_info(const void *_entry1, const void *_entry2);
#endif /* H5_HAVE_PARALLEL */

/* Debugging helper routine callback */
static int H5D__chunk_dump_index_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata);

/*********************/
/* Package Variables */
/*********************/

/* Chunked storage layout I/O ops */
const H5D_layout_ops_t H5D_LOPS_CHUNK[1] = {{
    H5D__chunk_construct,      /* construct */
    H5D__chunk_init,           /* init */
    H5D__chunk_is_space_alloc, /* is_space_alloc */
    H5D__chunk_is_data_cached, /* is_data_cached */
    H5D__chunk_io_init,        /* io_init */
    H5D__chunk_mdio_init,      /* mdio_init */
    H5D__chunk_read,           /* ser_read */
    H5D__chunk_write,          /* ser_write */
    NULL,                      /* readvv */
    NULL,                      /* writevv */
    H5D__chunk_flush,          /* flush */
    H5D__chunk_io_term,        /* io_term */
    H5D__chunk_dest            /* dest */
}};

/*******************/
/* Local Variables */
/*******************/

/* "nonexistent" storage layout I/O ops */
static const H5D_layout_ops_t H5D_LOPS_NONEXISTENT[1] = {
    {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, H5D__nonexistent_readvv, NULL, NULL, NULL, NULL}};

/* Declare a free list to manage the H5F_rdcc_ent_ptr_t sequence information */
H5FL_SEQ_DEFINE_STATIC(H5D_rdcc_ent_ptr_t);

/* Declare a free list to manage H5D_rdcc_ent_t objects */
H5FL_DEFINE_STATIC(H5D_rdcc_ent_t);

/* Declare a free list to manage the H5D_chunk_info_t struct */
H5FL_DEFINE_STATIC(H5D_chunk_map_t);

/* Declare a free list to manage the H5D_piece_info_t struct */
H5FL_DEFINE(H5D_piece_info_t);

/* Declare a free list to manage the chunk sequence information */
H5FL_BLK_DEFINE_STATIC(chunk);

/* Declare extern free list to manage the H5S_sel_iter_t struct */
H5FL_EXTERN(H5S_sel_iter_t);

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_direct_write
 *
 * Purpose:    Internal routine to write a chunk directly into the file.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_direct_write(H5D_t *dset, uint32_t filters, hsize_t *offset, uint32_t data_size, const void *buf)
{
    const H5O_layout_t *layout = &(dset->shared->layout); /* Dataset layout */
    H5D_chunk_ud_t      udata;                            /* User data for querying chunk info */
    H5F_block_t         old_chunk;                        /* Offset/length of old chunk */
    H5D_chk_idx_info_t  idx_info;                         /* Chunked index info */
    hsize_t             scaled[H5S_MAX_RANK];             /* Scaled coordinates for this chunk */
    bool                need_insert = false;   /* Whether the chunk needs to be inserted into the index */
    herr_t              ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    /* Sanity checks */
    assert(layout->type == H5D_CHUNKED);

    /* Allocate dataspace and initialize it if it hasn't been. */
    if (!H5D__chunk_is_space_alloc(&layout->storage))
        if (H5D__alloc_storage(dset, H5D_ALLOC_WRITE, false, NULL) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize storage");

    /* Calculate the index of this chunk */
    H5VM_chunk_scaled(dset->shared->ndims, offset, layout->u.chunk.dim, scaled);
    scaled[dset->shared->ndims] = 0;

    /* Find out the file address of the chunk (if any) */
    if (H5D__chunk_lookup(dset, scaled, &udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

    /* Sanity check */
    assert((H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length > 0) ||
           (!H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length == 0));

    /* Set the file block information for the old chunk */
    /* (Which is only defined when overwriting an existing chunk) */
    old_chunk.offset = udata.chunk_block.offset;
    old_chunk.length = udata.chunk_block.length;

    /* Check if the chunk needs to be inserted (it also could exist already
     *      and the chunk allocate operation could resize it)
     */

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &(dset->shared->dcpl_cache.pline);
    idx_info.layout  = &(dset->shared->layout.u.chunk);
    idx_info.storage = &(dset->shared->layout.storage.u.chunk);

    /* Set up the size of chunk for user data */
    udata.chunk_block.length = data_size;

    if (0 == idx_info.pline->nused && H5_addr_defined(old_chunk.offset))
        /* If there are no filters and we are overwriting the chunk we can just set values */
        need_insert = false;
    else {
        /* Otherwise, create the chunk it if it doesn't exist, or reallocate the chunk
         * if its size has changed.
         */
        if (H5D__chunk_file_alloc(&idx_info, &old_chunk, &udata.chunk_block, &need_insert, scaled) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "unable to allocate chunk");

        /* Cache the new chunk information */
        H5D__chunk_cinfo_cache_update(&dset->shared->cache.chunk.last, &udata);
    } /* end else */

    /* Make sure the address of the chunk is returned. */
    if (!H5_addr_defined(udata.chunk_block.offset))
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk address isn't defined");

    /* Evict the (old) entry from the cache if present, but do not flush
     * it to disk */
    if (UINT_MAX != udata.idx_hint) {
        const H5D_rdcc_t *rdcc = &(dset->shared->cache.chunk); /*raw data chunk cache */

        if (H5D__chunk_cache_evict(dset, rdcc->slot[udata.idx_hint], false) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTREMOVE, FAIL, "unable to evict chunk");
    } /* end if */

    /* Write the data to the file */
    if (H5F_shared_block_write(H5F_SHARED(dset->oloc.file), H5FD_MEM_DRAW, udata.chunk_block.offset,
                               data_size, buf) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to write raw data to file");

    /* Insert the chunk record into the index */
    if (need_insert && layout->storage.u.chunk.ops->insert) {
        /* Set the chunk's filter mask to the new settings */
        udata.filter_mask = filters;

        if ((layout->storage.u.chunk.ops->insert)(&idx_info, &udata, dset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "unable to insert chunk addr into index");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__chunk_direct_write() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_direct_read
 *
 * Purpose:     Internal routine to read a chunk directly from the file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_direct_read(const H5D_t *dset, hsize_t *offset, uint32_t *filters, void *buf)
{
    const H5O_layout_t *layout = &(dset->shared->layout);      /* Dataset layout */
    const H5D_rdcc_t   *rdcc   = &(dset->shared->cache.chunk); /* raw data chunk cache */
    H5D_chunk_ud_t      udata;                                 /* User data for querying chunk info */
    hsize_t             scaled[H5S_MAX_RANK];                  /* Scaled coordinates for this chunk */
    herr_t              ret_value = SUCCEED;                   /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    /* Check args */
    assert(dset && H5D_CHUNKED == layout->type);
    assert(offset);
    assert(filters);
    assert(buf);

    *filters = 0;

    /* Allocate dataspace and initialize it if it hasn't been. */
    if (!H5D__chunk_is_space_alloc(&layout->storage) && !H5D__chunk_is_data_cached(dset->shared))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "storage is not initialized");

    /* Calculate the index of this chunk */
    H5VM_chunk_scaled(dset->shared->ndims, offset, layout->u.chunk.dim, scaled);
    scaled[dset->shared->ndims] = 0;

    /* Reset fields about the chunk we are looking for */
    udata.filter_mask        = 0;
    udata.chunk_block.offset = HADDR_UNDEF;
    udata.chunk_block.length = 0;
    udata.idx_hint           = UINT_MAX;

    /* Find out the file address of the chunk */
    if (H5D__chunk_lookup(dset, scaled, &udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

    /* Sanity check */
    assert((H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length > 0) ||
           (!H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length == 0));

    /* Check if the requested chunk exists in the chunk cache */
    if (UINT_MAX != udata.idx_hint) {
        H5D_rdcc_ent_t *ent = rdcc->slot[udata.idx_hint];
        bool            flush;

        /* Sanity checks  */
        assert(udata.idx_hint < rdcc->nslots);
        assert(rdcc->slot[udata.idx_hint]);

        flush = (ent->dirty == true) ? true : false;

        /* Flush the chunk to disk and clear the cache entry */
        if (H5D__chunk_cache_evict(dset, rdcc->slot[udata.idx_hint], flush) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTREMOVE, FAIL, "unable to evict chunk");

        /* Reset fields about the chunk we are looking for */
        udata.filter_mask        = 0;
        udata.chunk_block.offset = HADDR_UNDEF;
        udata.chunk_block.length = 0;
        udata.idx_hint           = UINT_MAX;

        /* Get the new file address / chunk size after flushing */
        if (H5D__chunk_lookup(dset, scaled, &udata) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");
    }

    /* Make sure the address of the chunk is returned. */
    if (!H5_addr_defined(udata.chunk_block.offset))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "chunk address isn't defined");

    /* Read the chunk data into the supplied buffer */
    if (H5F_shared_block_read(H5F_SHARED(dset->oloc.file), H5FD_MEM_DRAW, udata.chunk_block.offset,
                              udata.chunk_block.length, buf) < 0)
        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "unable to read raw data chunk");

    /* Return the filter mask */
    *filters = udata.filter_mask;

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__chunk_direct_read() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_chunk_storage_size
 *
 * Purpose:     Internal routine to read the storage size of a chunk on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__get_chunk_storage_size(H5D_t *dset, const hsize_t *offset, hsize_t *storage_size)
{
    const H5O_layout_t *layout = &(dset->shared->layout);      /* Dataset layout */
    const H5D_rdcc_t   *rdcc   = &(dset->shared->cache.chunk); /* raw data chunk cache */
    hsize_t             scaled[H5S_MAX_RANK];                  /* Scaled coordinates for this chunk */
    H5D_chunk_ud_t      udata;                                 /* User data for querying chunk info */
    herr_t              ret_value = SUCCEED;                   /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    /* Check args */
    assert(dset && H5D_CHUNKED == layout->type);
    assert(offset);
    assert(storage_size);

    /* Allocate dataspace and initialize it if it hasn't been. */
    if (!(*layout->ops->is_space_alloc)(&layout->storage))
        HGOTO_DONE(SUCCEED);

    /* Calculate the index of this chunk */
    H5VM_chunk_scaled(dset->shared->ndims, offset, layout->u.chunk.dim, scaled);
    scaled[dset->shared->ndims] = 0;

    /* Reset fields about the chunk we are looking for */
    udata.chunk_block.offset = HADDR_UNDEF;
    udata.chunk_block.length = 0;
    udata.idx_hint           = UINT_MAX;

    /* Find out the file address of the chunk */
    if (H5D__chunk_lookup(dset, scaled, &udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

    /* Sanity check */
    assert((H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length > 0) ||
           (!H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length == 0));

    /* The requested chunk is not in cache or on disk */
    if (!H5_addr_defined(udata.chunk_block.offset) && UINT_MAX == udata.idx_hint)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "chunk storage is not allocated");

    /* Check if there are filters registered to the dataset */
    if (dset->shared->dcpl_cache.pline.nused > 0) {
        /* Check if the requested chunk exists in the chunk cache */
        if (UINT_MAX != udata.idx_hint) {
            H5D_rdcc_ent_t *ent = rdcc->slot[udata.idx_hint];

            /* Sanity checks  */
            assert(udata.idx_hint < rdcc->nslots);
            assert(rdcc->slot[udata.idx_hint]);

            /* If the cached chunk is dirty, it must be flushed to get accurate size */
            if (ent->dirty == true) {
                /* Flush the chunk to disk and clear the cache entry */
                if (H5D__chunk_cache_evict(dset, rdcc->slot[udata.idx_hint], true) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTREMOVE, FAIL, "unable to evict chunk");

                /* Reset fields about the chunk we are looking for */
                udata.chunk_block.offset = HADDR_UNDEF;
                udata.chunk_block.length = 0;
                udata.idx_hint           = UINT_MAX;

                /* Get the new file address / chunk size after flushing */
                if (H5D__chunk_lookup(dset, scaled, &udata) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");
            }
        }

        /* Make sure the address of the chunk is returned. */
        if (!H5_addr_defined(udata.chunk_block.offset))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "chunk address isn't defined");

        /* Return the chunk size on disk */
        *storage_size = udata.chunk_block.length;
    }
    /* There are no filters registered, return the chunk size from the storage layout */
    else
        *storage_size = dset->shared->layout.u.chunk.size;

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* H5D__get_chunk_storage_size */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_set_info_real
 *
 * Purpose:     Internal routine to set the information about chunks for a dataset
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_set_info_real(H5O_layout_chunk_t *layout, unsigned ndims, const hsize_t *curr_dims,
                         const hsize_t *max_dims)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(layout);
    assert(curr_dims);

    /* Can happen when corrupt files are parsed */
    if (ndims == 0)
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "number of dimensions cannot be zero");

    /* Compute the # of chunks in dataset dimensions */
    layout->nchunks     = 1;
    layout->max_nchunks = 1;
    for (unsigned u = 0; u < ndims; u++) {
        /* Round up to the next integer # of chunks, to accommodate partial chunks */
        layout->chunks[u] = ((curr_dims[u] + layout->dim[u]) - 1) / layout->dim[u];
        if (H5S_UNLIMITED == max_dims[u])
            layout->max_chunks[u] = H5S_UNLIMITED;
        else {
            /* Sanity check */
            if (layout->dim[u] == 0)
                HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "dimension size must be > 0, dim = %u ", u);

            layout->max_chunks[u] = ((max_dims[u] + layout->dim[u]) - 1) / layout->dim[u];
        }

        /* Accumulate the # of chunks */
        layout->nchunks *= layout->chunks[u];
        layout->max_nchunks *= layout->max_chunks[u];
    }

    /* Get the "down" sizes for each dimension */
    H5VM_array_down(ndims, layout->chunks, layout->down_chunks);
    H5VM_array_down(ndims, layout->max_chunks, layout->max_down_chunks);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_set_info_real() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_set_info
 *
 * Purpose:    Sets the information about chunks for a dataset
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_set_info(const H5D_t *dset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dset);

    /* Set the base layout information */
    if (H5D__chunk_set_info_real(&dset->shared->layout.u.chunk, dset->shared->ndims, dset->shared->curr_dims,
                                 dset->shared->max_dims) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set layout's chunk info");

    /* Call the index's "resize" callback */
    if (dset->shared->layout.storage.u.chunk.ops->resize &&
        (dset->shared->layout.storage.u.chunk.ops->resize)(&dset->shared->layout.u.chunk) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to resize chunk index information");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_set_info() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_set_sizes
 *
 * Purpose:     Sets chunk and type sizes.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_set_sizes(H5D_t *dset)
{
    uint64_t chunk_size;            /* Size of chunk in bytes */
    unsigned max_enc_bytes_per_dim; /* Max. number of bytes required to encode this dimension */
    unsigned u;                     /* Iterator */
    herr_t   ret_value = SUCCEED;   /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(dset);

    /* Increment # of chunk dimensions, to account for datatype size as last element */
    dset->shared->layout.u.chunk.ndims++;

    /* Set the last dimension of the chunk size to the size of the datatype */
    dset->shared->layout.u.chunk.dim[dset->shared->layout.u.chunk.ndims - 1] =
        (uint32_t)H5T_GET_SIZE(dset->shared->type);

    /* Compute number of bytes to use for encoding chunk dimensions */
    max_enc_bytes_per_dim = 0;
    for (u = 0; u < (unsigned)dset->shared->layout.u.chunk.ndims; u++) {
        unsigned enc_bytes_per_dim; /* Number of bytes required to encode this dimension */

        /* Get encoded size of dim, in bytes */
        enc_bytes_per_dim = (H5VM_log2_gen(dset->shared->layout.u.chunk.dim[u]) + 8) / 8;

        /* Check if this is the largest value so far */
        if (enc_bytes_per_dim > max_enc_bytes_per_dim)
            max_enc_bytes_per_dim = enc_bytes_per_dim;
    } /* end for */
    assert(max_enc_bytes_per_dim > 0 && max_enc_bytes_per_dim <= 8);
    dset->shared->layout.u.chunk.enc_bytes_per_dim = max_enc_bytes_per_dim;

    /* Compute and store the total size of a chunk */
    /* (Use 64-bit value to ensure that we can detect >4GB chunks) */
    for (u = 1, chunk_size = (uint64_t)dset->shared->layout.u.chunk.dim[0];
         u < dset->shared->layout.u.chunk.ndims; u++)
        chunk_size *= (uint64_t)dset->shared->layout.u.chunk.dim[u];

    /* Check for chunk larger than can be represented in 32-bits */
    /* (Chunk size is encoded in 32-bit value in v1 B-tree records) */
    if (chunk_size > (uint64_t)0xffffffff)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "chunk size must be < 4GB");

    H5_CHECKED_ASSIGN(dset->shared->layout.u.chunk.size, uint32_t, chunk_size, uint64_t);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_set_sizes */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_construct
 *
 * Purpose:    Constructs new chunked layout information for dataset
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_construct(H5F_t H5_ATTR_UNUSED *f, H5D_t *dset)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(dset);

    /* Check for invalid chunk dimension rank */
    if (0 == dset->shared->layout.u.chunk.ndims)
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "no chunk information set?");
    if (dset->shared->layout.u.chunk.ndims != dset->shared->ndims)
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "dimensionality of chunks doesn't match the dataspace");

    /* Set chunk sizes */
    if (H5D__chunk_set_sizes(dset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "unable to set chunk sizes");
    assert((unsigned)(dset->shared->layout.u.chunk.ndims) <= NELMTS(dset->shared->layout.u.chunk.dim));

    /* Chunked storage is not compatible with external storage (currently) */
    if (dset->shared->dcpl_cache.efl.nused > 0)
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "external storage not supported with chunked layout");

    /* Sanity check dimensions */
    for (u = 0; u < dset->shared->layout.u.chunk.ndims - 1; u++) {
        /* Don't allow zero-sized chunk dimensions */
        if (0 == dset->shared->layout.u.chunk.dim[u])
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "chunk size must be > 0, dim = %u ", u);

        /*
         * The chunk size of a dimension with a fixed size cannot exceed
         * the maximum dimension size. If any dimension size is zero, there
         * will be no such restriction.
         */
        if (dset->shared->curr_dims[u] && dset->shared->max_dims[u] != H5S_UNLIMITED &&
            dset->shared->max_dims[u] < dset->shared->layout.u.chunk.dim[u])
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                        "chunk size must be <= maximum dimension size for fixed-sized dimensions");
    } /* end for */

    /* Reset address and pointer of the array struct for the chunked storage index */
    if (H5D_chunk_idx_reset(&dset->shared->layout.storage.u.chunk, true) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to reset chunked storage index");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_construct() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_init
 *
 * Purpose:    Initialize the raw data chunk cache for a dataset.  This is
 *        called when the dataset is initialized.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_init(H5F_t *f, const H5D_t *const dset, hid_t dapl_id)
{
    H5D_chk_idx_info_t idx_info;                            /* Chunked index info */
    H5D_rdcc_t        *rdcc = &(dset->shared->cache.chunk); /* Convenience pointer to dataset's chunk cache */
    H5P_genplist_t    *dapl;                                /* Data access property list object pointer */
    H5O_storage_chunk_t *sc        = &(dset->shared->layout.storage.u.chunk);
    bool                 idx_init  = false;
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);
    assert(dset);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);

    if (NULL == (dapl = (H5P_genplist_t *)H5I_object(dapl_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for fapl ID");

    /* Use the properties in dapl_id if they have been set, otherwise use the properties from the file */
    if (H5P_get(dapl, H5D_ACS_DATA_CACHE_NUM_SLOTS_NAME, &rdcc->nslots) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get data cache number of slots");
    if (rdcc->nslots == H5D_CHUNK_CACHE_NSLOTS_DEFAULT)
        rdcc->nslots = H5F_RDCC_NSLOTS(f);

    if (H5P_get(dapl, H5D_ACS_DATA_CACHE_BYTE_SIZE_NAME, &rdcc->nbytes_max) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get data cache byte size");
    if (rdcc->nbytes_max == H5D_CHUNK_CACHE_NBYTES_DEFAULT)
        rdcc->nbytes_max = H5F_RDCC_NBYTES(f);

    if (H5P_get(dapl, H5D_ACS_PREEMPT_READ_CHUNKS_NAME, &rdcc->w0) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get preempt read chunks");
    if (rdcc->w0 < 0)
        rdcc->w0 = H5F_RDCC_W0(f);

    /* If nbytes_max or nslots is 0, set them both to 0 and avoid allocating space */
    if (!rdcc->nbytes_max || !rdcc->nslots)
        rdcc->nbytes_max = rdcc->nslots = 0;
    else {
        rdcc->slot = H5FL_SEQ_CALLOC(H5D_rdcc_ent_ptr_t, rdcc->nslots);
        if (NULL == rdcc->slot)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

        /* Reset any cached chunk info for this dataset */
        H5D__chunk_cinfo_cache_reset(&(rdcc->last));
    } /* end else */

    /* Compute scaled dimension info, if dataset dims > 1 */
    if (dset->shared->ndims > 1) {
        unsigned u; /* Local index value */

        for (u = 0; u < dset->shared->ndims; u++) {
            hsize_t scaled_power2up; /* Scaled value, rounded to next power of 2 */

            /* Initial scaled dimension sizes */
            if (dset->shared->layout.u.chunk.dim[u] == 0)
                HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk size must be > 0, dim = %u ", u);

            /* Round up to the next integer # of chunks, to accommodate partial chunks */
            rdcc->scaled_dims[u] = (dset->shared->curr_dims[u] + dset->shared->layout.u.chunk.dim[u] - 1) /
                                   dset->shared->layout.u.chunk.dim[u];

            if (!(scaled_power2up = H5VM_power2up(rdcc->scaled_dims[u])))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get the next power of 2");

            /* Initial 'power2up' values for scaled dimensions */
            rdcc->scaled_power2up[u] = scaled_power2up;

            /* Number of bits required to encode scaled dimension size */
            rdcc->scaled_encode_bits[u] = H5VM_log2_gen(rdcc->scaled_power2up[u]);
        } /* end for */
    }     /* end if */

    /* Compose chunked index info struct */
    idx_info.f       = f;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = sc;

    /* Allocate any indexing structures */
    if (sc->ops->init && (sc->ops->init)(&idx_info, dset->shared->space, dset->oloc.addr) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize indexing information");
    idx_init = true;

    /* Set the number of chunks in dataset, etc. */
    if (H5D__chunk_set_info(dset) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set # of chunks for dataset");

done:
    if (FAIL == ret_value) {
        if (rdcc->slot)
            rdcc->slot = H5FL_SEQ_FREE(H5D_rdcc_ent_ptr_t, rdcc->slot);

        if (idx_init && sc->ops->dest && (sc->ops->dest)(&idx_info) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to release chunk index info");
    }
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_is_space_alloc
 *
 * Purpose:    Query if space is allocated for layout
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
bool
H5D__chunk_is_space_alloc(const H5O_storage_t *storage)
{
    const H5O_storage_chunk_t *sc        = &(storage->u.chunk);
    bool                       ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(storage);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);

    /* Query index layer */
    ret_value = (sc->ops->is_space_alloc)(sc);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_is_space_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_is_data_cached
 *
 * Purpose:     Query if raw data is cached for dataset
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
bool
H5D__chunk_is_data_cached(const H5D_shared_t *shared_dset)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(shared_dset);

    FUNC_LEAVE_NOAPI(shared_dset->cache.chunk.nused > 0)
} /* end H5D__chunk_is_data_cached() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_io_init
 *
 * Purpose:    Performs initialization before any sort of I/O on the raw data
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_io_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo)
{
    const H5D_t     *dataset = dinfo->dset;         /* Local pointer to dataset info */
    H5D_chunk_map_t *fm;                            /* Convenience pointer to chunk map */
    hssize_t         old_offset[H5O_LAYOUT_NDIMS];  /* Old selection offset */
    htri_t           file_space_normalized = false; /* File dataspace was normalized */
    unsigned         f_ndims;                       /* The number of dimensions of the file's dataspace */
    int              sm_ndims; /* The number of dimensions of the memory buffer's dataspace (signed) */
    unsigned         u;        /* Local index variable */
    herr_t           ret_value = SUCCEED; /* Return value        */

    FUNC_ENTER_PACKAGE

    /* Allocate chunk map */
    if (NULL == (dinfo->layout_io_info.chunk_map = H5FL_MALLOC(H5D_chunk_map_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "unable to allocate chunk map");
    fm = dinfo->layout_io_info.chunk_map;

    /* Get layout for dataset */
    dinfo->layout = &(dataset->shared->layout);

    /* Initialize "last chunk" information */
    fm->last_index      = (hsize_t)-1;
    fm->last_piece_info = NULL;

    /* Clear other fields */
    fm->mchunk_tmpl       = NULL;
    fm->dset_sel_pieces   = NULL;
    fm->single_space      = NULL;
    fm->single_piece_info = NULL;

    /* Check if the memory space is scalar & make equivalent memory space */
    if ((sm_ndims = H5S_GET_EXTENT_NDIMS(dinfo->mem_space)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "unable to get dimension number");
    /* Set the number of dimensions for the memory dataspace */
    H5_CHECKED_ASSIGN(fm->m_ndims, unsigned, sm_ndims, int);

    /* Get rank for file dataspace */
    fm->f_ndims = f_ndims = dataset->shared->layout.u.chunk.ndims - 1;

    /* Normalize hyperslab selections by adjusting them by the offset */
    /* (It might be worthwhile to normalize both the file and memory dataspaces
     * before any (contiguous, chunked, etc) file I/O operation, in order to
     * speed up hyperslab calculations by removing the extra checks and/or
     * additions involving the offset and the hyperslab selection -QAK)
     */
    if ((file_space_normalized = H5S_hyper_normalize_offset(dinfo->file_space, old_offset)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to normalize selection");

    /* Decide the number of chunks in each dimension */
    for (u = 0; u < f_ndims; u++)
        /* Keep the size of the chunk dimensions as hsize_t for various routines */
        fm->chunk_dim[u] = dinfo->layout->u.chunk.dim[u];

    if (H5D__chunk_io_init_selections(io_info, dinfo) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create file and memory chunk selections");

    /* Check if we're performing selection I/O and save the result if it hasn't
     * been disabled already */
    if (io_info->use_select_io != H5D_SELECTION_IO_MODE_OFF)
        if (H5D__chunk_may_use_select_io(io_info, dinfo) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't check if selection I/O is possible");

    /* Calculate type conversion buffer size if necessary.  Currently only implemented for selection I/O. */
    if (io_info->use_select_io != H5D_SELECTION_IO_MODE_OFF &&
        !(dinfo->type_info.is_xform_noop && dinfo->type_info.is_conv_noop)) {
        H5SL_node_t *chunk_node; /* Current node in chunk skip list */

        /* Iterate through nodes in chunk skip list */
        chunk_node = H5D_CHUNK_GET_FIRST_NODE(dinfo);
        while (chunk_node) {
            H5D_piece_info_t *piece_info; /* Chunk information */

            /* Get the actual chunk information from the skip list node */
            piece_info = H5D_CHUNK_GET_NODE_INFO(dinfo, chunk_node);

            /* Handle type conversion buffer */
            H5D_INIT_PIECE_TCONV(io_info, dinfo, piece_info)

            /* Advance to next chunk in list */
            chunk_node = H5D_CHUNK_GET_NEXT_NODE(dinfo, chunk_node);
        }
    }

#ifdef H5_HAVE_PARALLEL
    /*
     * If collective metadata reads are enabled, ensure all ranks
     * have the dataset's chunk index open (if it was created) to
     * prevent possible metadata inconsistency issues or unintentional
     * independent metadata reads later on.
     */
    if (H5F_SHARED_HAS_FEATURE(io_info->f_sh, H5FD_FEAT_HAS_MPI) &&
        H5F_shared_get_coll_metadata_reads(io_info->f_sh) &&
        H5D__chunk_is_space_alloc(&dataset->shared->layout.storage)) {
        H5O_storage_chunk_t *sc = &(dataset->shared->layout.storage.u.chunk);
        H5D_chk_idx_info_t   idx_info;
        bool                 index_is_open;

        idx_info.f       = dataset->oloc.file;
        idx_info.pline   = &dataset->shared->dcpl_cache.pline;
        idx_info.layout  = &dataset->shared->layout.u.chunk;
        idx_info.storage = sc;

        assert(sc && sc->ops && sc->ops->is_open);
        if (sc->ops->is_open(&idx_info, &index_is_open) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to check if dataset chunk index is open");

        if (!index_is_open) {
            assert(sc->ops->open);
            if (sc->ops->open(&idx_info) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to open dataset chunk index");
        }

        /*
         * Load any other chunk index metadata that we can,
         * such as fixed array data blocks, while we know all
         * MPI ranks will do so with collective metadata reads
         * enabled
         */
        if (sc->ops->load_metadata && sc->ops->load_metadata(&idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to load additional chunk index metadata");
    }
#endif

done:
    if (file_space_normalized == true)
        if (H5S_hyper_denormalize_offset(dinfo->file_space, old_offset) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't denormalize selection");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_io_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_io_init_selections
 *
 * Purpose:        Initialize the chunk mappings
 *
 * Return:        Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_io_init_selections(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo)
{
    H5D_chunk_map_t   *fm;                 /* Convenience pointer to chunk map */
    const H5D_t       *dataset;            /* Local pointer to dataset info */
    const H5T_t       *mem_type;           /* Local pointer to memory datatype */
    H5S_t             *tmp_mspace = NULL;  /* Temporary memory dataspace */
    H5T_t             *file_type  = NULL;  /* Temporary copy of file datatype for iteration */
    bool               iter_init  = false; /* Selection iteration info has been initialized */
    char               bogus;              /* "bogus" buffer to pass to selection iterator */
    H5D_io_info_wrap_t io_info_wrap;
    herr_t             ret_value = SUCCEED; /* Return value        */

    FUNC_ENTER_PACKAGE

    assert(io_info);
    assert(dinfo);

    /* Set convenience pointers */
    fm = dinfo->layout_io_info.chunk_map;
    assert(fm);
    dataset  = dinfo->dset;
    mem_type = dinfo->type_info.mem_type;

    /* Special case for only one element in selection */
    /* (usually appending a record) */
    if (dinfo->nelmts == 1
#ifdef H5_HAVE_PARALLEL
        && !(io_info->using_mpi_vfd)
#endif /* H5_HAVE_PARALLEL */
        && H5S_SEL_ALL != H5S_GET_SELECT_TYPE(dinfo->file_space)) {
        /* Initialize skip list for chunk selections */
        fm->use_single = true;

        /* Initialize single chunk dataspace */
        if (NULL == dataset->shared->cache.chunk.single_space) {
            /* Make a copy of the dataspace for the dataset */
            if ((dataset->shared->cache.chunk.single_space = H5S_copy(dinfo->file_space, true, false)) ==
                NULL)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy file space");

            /* Resize chunk's dataspace dimensions to size of chunk */
            if (H5S_set_extent_real(dataset->shared->cache.chunk.single_space, fm->chunk_dim) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't adjust chunk dimensions");

            /* Set the single chunk dataspace to 'all' selection */
            if (H5S_select_all(dataset->shared->cache.chunk.single_space, true) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL, "unable to set all selection");
        } /* end if */
        fm->single_space = dataset->shared->cache.chunk.single_space;
        assert(fm->single_space);

        /* Allocate the single chunk information */
        if (NULL == dataset->shared->cache.chunk.single_piece_info)
            if (NULL == (dataset->shared->cache.chunk.single_piece_info = H5FL_MALLOC(H5D_piece_info_t)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "can't allocate chunk info");
        fm->single_piece_info = dataset->shared->cache.chunk.single_piece_info;
        assert(fm->single_piece_info);

        /* Reset chunk template information */
        fm->mchunk_tmpl = NULL;

        /* Set up chunk mapping for single element */
        if (H5D__create_piece_map_single(dinfo, io_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                        "unable to create chunk selections for single element");
    } /* end if */
    else {
        bool sel_hyper_flag; /* Whether file selection is a hyperslab */

        /* Initialize skip list for chunk selections */
        if (NULL == dataset->shared->cache.chunk.sel_chunks)
            if (NULL == (dataset->shared->cache.chunk.sel_chunks = H5SL_create(H5SL_TYPE_HSIZE, NULL)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, FAIL, "can't create skip list for chunk selections");
        fm->dset_sel_pieces = dataset->shared->cache.chunk.sel_chunks;
        assert(fm->dset_sel_pieces);

        /* We are not using single element mode */
        fm->use_single = false;

        /* Get type of selection on disk & in memory */
        if ((fm->fsel_type = H5S_GET_SELECT_TYPE(dinfo->file_space)) < H5S_SEL_NONE)
            HGOTO_ERROR(H5E_DATASET, H5E_BADSELECT, FAIL, "unable to get type of selection");
        if ((fm->msel_type = H5S_GET_SELECT_TYPE(dinfo->mem_space)) < H5S_SEL_NONE)
            HGOTO_ERROR(H5E_DATASET, H5E_BADSELECT, FAIL, "unable to get type of selection");

        /* If the selection is NONE or POINTS, set the flag to false */
        if (fm->fsel_type == H5S_SEL_POINTS || fm->fsel_type == H5S_SEL_NONE)
            sel_hyper_flag = false;
        else
            sel_hyper_flag = true;

        /* Check if file selection is a not a hyperslab selection */
        if (sel_hyper_flag) {
            /* Build the file selection for each chunk */
            if (H5S_SEL_ALL == fm->fsel_type) {
                if (H5D__create_piece_file_map_all(dinfo, io_info) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create file chunk selections");
            } /* end if */
            else {
                /* Sanity check */
                assert(fm->fsel_type == H5S_SEL_HYPERSLABS);

                if (H5D__create_piece_file_map_hyper(dinfo, io_info) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create file chunk selections");
            } /* end else */
        }     /* end if */
        else {
            H5S_sel_iter_op_t iter_op; /* Operator for iteration */

            /* Create temporary datatypes for selection iteration */
            if (NULL == (file_type = H5T_copy(dataset->shared->type, H5T_COPY_ALL)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "unable to copy file datatype");

            /* set opdata for H5D__piece_mem_cb */
            io_info_wrap.io_info = io_info;
            io_info_wrap.dinfo   = dinfo;
            iter_op.op_type      = H5S_SEL_ITER_OP_LIB;
            iter_op.u.lib_op     = H5D__piece_file_cb;

            /* Spaces might not be the same shape, iterate over the file selection directly */
            if (H5S_select_iterate(&bogus, file_type, dinfo->file_space, &iter_op, &io_info_wrap) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create file chunk selections");

            /* Reset "last piece" info */
            fm->last_index      = (hsize_t)-1;
            fm->last_piece_info = NULL;
        } /* end else */

        /* Build the memory selection for each chunk */
        if (sel_hyper_flag && H5S_SELECT_SHAPE_SAME(dinfo->file_space, dinfo->mem_space) == true) {
            /* Reset chunk template information */
            fm->mchunk_tmpl = NULL;

            /* If the selections are the same shape, use the file chunk
             * information to generate the memory chunk information quickly.
             */
            if (H5D__create_piece_mem_map_hyper(dinfo) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create memory chunk selections");
        } /* end if */
        else if (sel_hyper_flag && fm->f_ndims == 1 && fm->m_ndims == 1 &&
                 H5S_SELECT_IS_REGULAR(dinfo->mem_space) && H5S_SELECT_IS_SINGLE(dinfo->mem_space)) {
            if (H5D__create_piece_mem_map_1d(dinfo) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create file chunk selections");
        } /* end else-if */
        else {
            H5S_sel_iter_op_t iter_op;   /* Operator for iteration */
            size_t            elmt_size; /* Memory datatype size */

            /* Make a copy of equivalent memory space */
            if ((tmp_mspace = H5S_copy(dinfo->mem_space, true, false)) == NULL)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy memory space");

            /* De-select the mem space copy */
            if (H5S_select_none(tmp_mspace) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to de-select memory space");

            /* Save chunk template information */
            fm->mchunk_tmpl = tmp_mspace;

            /* Create temporary datatypes for selection iteration */
            if (!file_type)
                if (NULL == (file_type = H5T_copy(dataset->shared->type, H5T_COPY_ALL)))
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "unable to copy file datatype");

            /* Create selection iterator for memory selection */
            if (0 == (elmt_size = H5T_get_size(mem_type)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_BADSIZE, FAIL, "datatype size invalid");
            if (H5S_select_iter_init(&(fm->mem_iter), dinfo->mem_space, elmt_size, 0) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
            iter_init = true; /* Selection iteration info has been initialized */

            /* set opdata for H5D__piece_mem_cb */
            io_info_wrap.io_info = io_info;
            io_info_wrap.dinfo   = dinfo;
            iter_op.op_type      = H5S_SEL_ITER_OP_LIB;
            iter_op.u.lib_op     = H5D__piece_mem_cb;

            /* Spaces aren't the same shape, iterate over the memory selection directly */
            if (H5S_select_iterate(&bogus, file_type, dinfo->file_space, &iter_op, &io_info_wrap) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to create memory chunk selections");
        } /* end else */
    }     /* end else */

done:
    /* Release the [potentially partially built] chunk mapping information if an error occurs */
    if (ret_value < 0) {
        if (tmp_mspace && !fm->mchunk_tmpl)
            if (H5S_close(tmp_mspace) < 0)
                HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL,
                            "can't release memory chunk dataspace template");
        if (H5D__chunk_io_term(io_info, dinfo) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release chunk mapping");
    } /* end if */

    if (iter_init && H5S_SELECT_ITER_RELEASE(&(fm->mem_iter)) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");
    if (file_type && (H5T_close_real(file_type) < 0))
        HDONE_ERROR(H5E_DATATYPE, H5E_CANTFREE, FAIL, "Can't free temporary datatype");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_io_init_selections() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_mem_alloc
 *
 * Purpose:    Allocate space for a chunk in memory.  This routine allocates
 *              memory space for non-filtered chunks from a block free list
 *              and uses malloc()/free() for filtered chunks.
 *
 * Return:    Pointer to memory for chunk on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
void *
H5D__chunk_mem_alloc(size_t size, void *pline)
{
    H5O_pline_t *_pline    = (H5O_pline_t *)pline;
    void        *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(size);

    if (_pline && _pline->nused)
        ret_value = H5MM_malloc(size);
    else
        ret_value = H5FL_BLK_MALLOC(chunk, size);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_mem_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_mem_xfree
 *
 * Purpose:    Free space for a chunk in memory.  This routine releases
 *              memory space for non-filtered chunks from a block free list
 *              and uses malloc()/free() for filtered chunks.
 *
 * Return:    NULL (never fails)
 *
 *-------------------------------------------------------------------------
 */
void *
H5D__chunk_mem_xfree(void *chk, const void *pline)
{
    const H5O_pline_t *_pline = (const H5O_pline_t *)pline;

    FUNC_ENTER_PACKAGE_NOERR

    if (chk) {
        if (_pline && _pline->nused)
            H5MM_xfree(chk);
        else
            chk = H5FL_BLK_FREE(chunk, chk);
    } /* end if */

    FUNC_LEAVE_NOAPI(NULL)
} /* H5D__chunk_mem_xfree() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_mem_free
 *
 * Purpose:    Wrapper with H5MM_free_t-compatible signature that just
 *             calls H5D__chunk_mem_xfree and discards the return value.
 *-------------------------------------------------------------------------
 */
void
H5D__chunk_mem_free(void *chk, void *pline)
{
    (void)H5D__chunk_mem_xfree(chk, pline);
}

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_mem_realloc
 *
 * Purpose:     Reallocate space for a chunk in memory.  This routine allocates
 *              memory space for non-filtered chunks from a block free list
 *              and uses malloc()/free() for filtered chunks.
 *
 * Return:      Pointer to memory for chunk on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
void *
H5D__chunk_mem_realloc(void *chk, size_t size, const H5O_pline_t *pline)
{
    void *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(size);
    assert(pline);

    if (pline->nused > 0)
        ret_value = H5MM_realloc(chk, size);
    else
        ret_value = H5FL_BLK_REALLOC(chunk, chk, size);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_mem_realloc() */

/*--------------------------------------------------------------------------
 NAME
    H5D__free_piece_info
 PURPOSE
    Performs initialization before any sort of I/O on the raw data
    This was derived from H5D__free_chunk_info for multi-dset work.
 USAGE
    herr_t H5D__free_piece_info(chunk_info, key, opdata)
        void *chunk_info;    IN: Pointer to chunk info to destroy
        void *key;           Unused
        void *opdata;        Unused
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Releases all the memory for a chunk info node.  Called by H5SL_free
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5D__free_piece_info(void *item, void H5_ATTR_UNUSED *key, void H5_ATTR_UNUSED *opdata)
{
    H5D_piece_info_t *piece_info = (H5D_piece_info_t *)item;

    FUNC_ENTER_PACKAGE_NOERR

    assert(piece_info);

    /* Close the piece's file dataspace, if it's not shared */
    if (!piece_info->fspace_shared)
        (void)H5S_close(piece_info->fspace);
    else
        H5S_select_all(piece_info->fspace, true);

    /* Close the piece's memory dataspace, if it's not shared */
    if (!piece_info->mspace_shared && piece_info->mspace)
        (void)H5S_close((H5S_t *)piece_info->mspace);

    /* Free the actual piece info */
    piece_info = H5FL_FREE(H5D_piece_info_t, piece_info);

    FUNC_LEAVE_NOAPI(0)
} /* H5D__free_piece_info() */

/*-------------------------------------------------------------------------
 * Function:    H5D__create_piece_map_single
 *
 * Purpose:    Create piece selections when appending a single record
 *             This was derived from H5D__create_chunk_map_single for
 *             multi-dset work.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__create_piece_map_single(H5D_dset_io_info_t *di, H5D_io_info_t *io_info)
{
    H5D_chunk_map_t  *fm;                          /* Convenience pointer to chunk map */
    H5D_piece_info_t *piece_info;                  /* Piece information to insert into skip list */
    hsize_t           coords[H5O_LAYOUT_NDIMS];    /* Coordinates of chunk */
    hsize_t           sel_start[H5O_LAYOUT_NDIMS]; /* Offset of low bound of file selection */
    hsize_t           sel_end[H5O_LAYOUT_NDIMS];   /* Offset of high bound of file selection */
    unsigned          u;                           /* Local index variable */
    herr_t            ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set convenience pointer */
    fm = di->layout_io_info.chunk_map;

    /* Sanity checks */
    assert(fm);
    assert(fm->f_ndims > 0);

    /* Get coordinate for selection */
    if (H5S_SELECT_BOUNDS(di->file_space, sel_start, sel_end) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file selection bound info");

    /* Initialize the 'single piece' file & memory piece information */
    piece_info               = fm->single_piece_info;
    piece_info->piece_points = 1;

    /* Set chunk location & hyperslab size */
    for (u = 0; u < fm->f_ndims; u++) {
        /* Validate this chunk dimension */
        if (di->layout->u.chunk.dim[u] == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk size must be > 0, dim = %u ", u);
        assert(sel_start[u] == sel_end[u]);
        piece_info->scaled[u] = sel_start[u] / di->layout->u.chunk.dim[u];
        coords[u]             = piece_info->scaled[u] * di->layout->u.chunk.dim[u];
    } /* end for */
    piece_info->scaled[fm->f_ndims] = 0;

    /* Calculate the index of this chunk */
    piece_info->index =
        H5VM_array_offset_pre(fm->f_ndims, di->layout->u.chunk.down_chunks, piece_info->scaled);

    /* Copy selection for file's dataspace into chunk dataspace */
    if (H5S_select_copy(fm->single_space, di->file_space, false) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy file selection");

    /* Move selection back to have correct offset in chunk */
    if (H5S_SELECT_ADJUST_U(fm->single_space, coords) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't adjust chunk selection");

    /* Set the file dataspace for the chunk to the shared 'single' dataspace */
    piece_info->fspace = fm->single_space;

    /* Indicate that the chunk's file dataspace is shared */
    piece_info->fspace_shared = true;

    /* Just point at the memory dataspace & selection */
    piece_info->mspace = di->mem_space;

    /* Indicate that the chunk's memory dataspace is shared */
    piece_info->mspace_shared = true;

    /* Initialize in-place type conversion info. Start with it disabled. */
    piece_info->in_place_tconv = false;
    piece_info->buf_off        = 0;

    /* Check if chunk is in a dataset with filters applied */
    piece_info->filtered_dset = di->dset->shared->dcpl_cache.pline.nused > 0;

    /* make connection to related dset info from this piece_info */
    piece_info->dset_info = di;

    /* Add piece to global piece_count */
    io_info->piece_count++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__create_piece_map_single() */

/*-------------------------------------------------------------------------
 * Function:    H5D__create_piece_file_map_all
 *
 * Purpose:    Create all chunk selections in file, for an "all" selection.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__create_piece_file_map_all(H5D_dset_io_info_t *di, H5D_io_info_t *io_info)
{
    H5D_chunk_map_t *fm;                      /* Convenience pointer to chunk map */
    H5S_t           *tmp_fchunk = NULL;       /* Temporary file dataspace */
    hsize_t          file_dims[H5S_MAX_RANK]; /* File dataspace dims */
    hsize_t          sel_points;              /* Number of elements in file selection */
    hsize_t zeros[H5S_MAX_RANK];   /* All zero vector (for start parameter to setting hyperslab on partial
                                      chunks) */
    hsize_t  coords[H5S_MAX_RANK]; /* Current coordinates of chunk */
    hsize_t  end[H5S_MAX_RANK];    /* Final coordinates of chunk */
    hsize_t  scaled[H5S_MAX_RANK]; /* Scaled coordinates for this chunk */
    hsize_t  chunk_index;          /* "Index" of chunk */
    hsize_t  curr_partial_clip[H5S_MAX_RANK]; /* Current partial dimension sizes to clip against */
    hsize_t  partial_dim_size[H5S_MAX_RANK];  /* Size of a partial dimension */
    bool     is_partial_dim[H5S_MAX_RANK];    /* Whether a dimension is currently a partial chunk */
    bool     filtered_dataset;                /* Whether the dataset in question has filters applied */
    unsigned num_partial_dims;                /* Current number of partial dimensions */
    unsigned u;                               /* Local index variable */
    herr_t   ret_value = SUCCEED;             /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set convenience pointer */
    fm = di->layout_io_info.chunk_map;

    /* Sanity checks */
    assert(fm);
    assert(fm->f_ndims > 0);

    /* Get number of elements selected in file */
    sel_points = di->nelmts;

    /* Get dataspace dimensions */
    if (H5S_get_simple_extent_dims(di->file_space, file_dims, NULL) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file selection bound info");

    /* Set initial chunk location, partial dimensions, etc */
    num_partial_dims = 0;
    memset(zeros, 0, sizeof(zeros));
    for (u = 0; u < fm->f_ndims; u++) {
        /* Validate this chunk dimension */
        if (di->layout->u.chunk.dim[u] == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk size must be > 0, dim = %u ", u);

        /* Set up start / end coordinates for first chunk */
        scaled[u] = 0;
        coords[u] = 0;
        end[u]    = fm->chunk_dim[u] - 1;

        /* Initialize partial chunk dimension information */
        partial_dim_size[u] = file_dims[u] % fm->chunk_dim[u];
        if (file_dims[u] < fm->chunk_dim[u]) {
            curr_partial_clip[u] = partial_dim_size[u];
            is_partial_dim[u]    = true;
            num_partial_dims++;
        } /* end if */
        else {
            curr_partial_clip[u] = fm->chunk_dim[u];
            is_partial_dim[u]    = false;
        } /* end else */
    }     /* end for */

    /* Set the index of this chunk */
    chunk_index = 0;

    /* Check whether dataset has filters applied */
    filtered_dataset = di->dset->shared->dcpl_cache.pline.nused > 0;

    /* Create "temporary" chunk for selection operations (copy file space) */
    if (NULL == (tmp_fchunk = H5S_create_simple(fm->f_ndims, fm->chunk_dim, NULL)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, FAIL, "unable to create dataspace for chunk");

    /* Iterate through each chunk in the dataset */
    while (sel_points) {
        H5D_piece_info_t *new_piece_info; /* Piece information to insert into skip list */
        hsize_t           chunk_points;   /* Number of elements in chunk selection */

        /* Add temporary chunk to the list of pieces */

        /* Allocate the file & memory chunk information */
        if (NULL == (new_piece_info = H5FL_MALLOC(H5D_piece_info_t)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate piece info");

        /* Initialize the chunk information */

        /* Set the chunk index */
        new_piece_info->index = chunk_index;

        /* Set the file chunk dataspace */
        if (NULL == (new_piece_info->fspace = H5S_copy(tmp_fchunk, true, false)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy chunk dataspace");
        new_piece_info->fspace_shared = false;

        /* If there are partial dimensions for this chunk, set the hyperslab for them */
        if (num_partial_dims > 0)
            if (H5S_select_hyperslab(new_piece_info->fspace, H5S_SELECT_SET, zeros, NULL, curr_partial_clip,
                                     NULL) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL, "can't create chunk selection");

        /* Set the memory chunk dataspace */
        new_piece_info->mspace        = NULL;
        new_piece_info->mspace_shared = false;

        /* Copy the chunk's scaled coordinates */
        H5MM_memcpy(new_piece_info->scaled, scaled, sizeof(hsize_t) * fm->f_ndims);
        new_piece_info->scaled[fm->f_ndims] = 0;

        /* make connection to related dset info from this piece_info */
        new_piece_info->dset_info = di;

        /* Initialize in-place type conversion info. Start with it disabled. */
        new_piece_info->in_place_tconv = false;
        new_piece_info->buf_off        = 0;

        new_piece_info->filtered_dset = filtered_dataset;

        /* Insert the new chunk into the skip list */
        if (H5SL_insert(fm->dset_sel_pieces, new_piece_info, &new_piece_info->index) < 0) {
            H5D__free_piece_info(new_piece_info, NULL, NULL);
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't insert chunk into skip list");
        } /* end if */

        /* Add piece to global piece_count*/
        io_info->piece_count++;

        /* Get number of elements selected in chunk */
        chunk_points                 = H5S_GET_SELECT_NPOINTS(new_piece_info->fspace);
        new_piece_info->piece_points = chunk_points;

        /* Decrement # of points left in file selection */
        sel_points -= chunk_points;

        /* Advance to next chunk if we are not done */
        if (sel_points > 0) {
            int curr_dim; /* Current dimension to increment */

            /* Increment chunk index */
            chunk_index++;

            /* Set current increment dimension */
            curr_dim = (int)fm->f_ndims - 1;

            /* Increment chunk location in fastest changing dimension */
            coords[curr_dim] += fm->chunk_dim[curr_dim];
            scaled[curr_dim]++;
            end[curr_dim] += fm->chunk_dim[curr_dim];

            /* Bring chunk location back into bounds, if necessary */
            if (coords[curr_dim] >= file_dims[curr_dim]) {
                do {
                    /* Reset current dimension's location to 0 */
                    coords[curr_dim] = 0;
                    scaled[curr_dim] = 0;
                    end[curr_dim]    = fm->chunk_dim[curr_dim] - 1;

                    /* Check for previous partial chunk in this dimension */
                    if (is_partial_dim[curr_dim] && end[curr_dim] < file_dims[curr_dim]) {
                        /* Sanity check */
                        assert(num_partial_dims > 0);

                        /* Reset partial chunk information for this dimension */
                        curr_partial_clip[curr_dim] = fm->chunk_dim[curr_dim];
                        is_partial_dim[curr_dim]    = false;
                        num_partial_dims--;
                    } /* end if */

                    /* Decrement current dimension */
                    curr_dim--;

                    /* Check for valid current dim */
                    if (curr_dim >= 0) {
                        /* Increment chunk location in current dimension */
                        coords[curr_dim] += fm->chunk_dim[curr_dim];
                        scaled[curr_dim]++;
                        end[curr_dim] = (coords[curr_dim] + fm->chunk_dim[curr_dim]) - 1;
                    } /* end if */
                } while (curr_dim >= 0 && (coords[curr_dim] >= file_dims[curr_dim]));
            } /* end if */

            /* Check for valid current dim */
            if (curr_dim >= 0) {
                /* Check for partial chunk in this dimension */
                if (!is_partial_dim[curr_dim] && file_dims[curr_dim] <= end[curr_dim]) {
                    /* Set partial chunk information for this dimension */
                    curr_partial_clip[curr_dim] = partial_dim_size[curr_dim];
                    is_partial_dim[curr_dim]    = true;
                    num_partial_dims++;

                    /* Sanity check */
                    assert(num_partial_dims <= fm->f_ndims);
                } /* end if */
            }     /* end if */
        }         /* end if */
    }             /* end while */

done:
    /* Clean up */
    if (tmp_fchunk && H5S_close(tmp_fchunk) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "can't release temporary dataspace");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__create_chunk_file_map_all() */

/*-------------------------------------------------------------------------
 * Function:    H5D__create_piece_file_map_hyper
 *
 * Purpose:     Create all chunk selections in file.
 *              This was derived from H5D__create_chunk_file_map_hyper for
 *              multi-dset work.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__create_piece_file_map_hyper(H5D_dset_io_info_t *dinfo, H5D_io_info_t *io_info)
{
    H5D_chunk_map_t *fm;                             /* Convenience pointer to chunk map */
    H5S_t           *tmp_fchunk = NULL;              /* Temporary file dataspace */
    hsize_t          sel_start[H5O_LAYOUT_NDIMS];    /* Offset of low bound of file selection */
    hsize_t          sel_end[H5O_LAYOUT_NDIMS];      /* Offset of high bound of file selection */
    hsize_t          sel_points;                     /* Number of elements in file selection */
    hsize_t          start_coords[H5O_LAYOUT_NDIMS]; /* Starting coordinates of selection */
    hsize_t          coords[H5O_LAYOUT_NDIMS];       /* Current coordinates of chunk */
    hsize_t          end[H5O_LAYOUT_NDIMS];          /* Final coordinates of chunk */
    hsize_t          chunk_index;                    /* Index of chunk */
    hsize_t          start_scaled[H5S_MAX_RANK];     /* Starting scaled coordinates of selection */
    hsize_t          scaled[H5S_MAX_RANK];           /* Scaled coordinates for this chunk */
    bool             filtered_dataset;               /* Whether the dataset in question has filters applied */
    int              curr_dim;                       /* Current dimension to increment */
    unsigned         u;                              /* Local index variable */
    herr_t           ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set convenience pointer */
    fm = dinfo->layout_io_info.chunk_map;

    /* Sanity checks */
    assert(fm);
    assert(fm->f_ndims > 0);

    /* Get number of elements selected in file */
    sel_points = dinfo->nelmts;

    /* Get bounding box for selection (to reduce the number of chunks to iterate over) */
    if (H5S_SELECT_BOUNDS(dinfo->file_space, sel_start, sel_end) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file selection bound info");

    /* Set initial chunk location & hyperslab size */
    for (u = 0; u < fm->f_ndims; u++) {
        /* Validate this chunk dimension */
        if (dinfo->layout->u.chunk.dim[u] == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk size must be > 0, dim = %u ", u);
        scaled[u] = start_scaled[u] = sel_start[u] / dinfo->layout->u.chunk.dim[u];
        coords[u] = start_coords[u] = scaled[u] * dinfo->layout->u.chunk.dim[u];
        end[u]                      = (coords[u] + fm->chunk_dim[u]) - 1;
    } /* end for */

    /* Calculate the index of this chunk */
    chunk_index = H5VM_array_offset_pre(fm->f_ndims, dinfo->layout->u.chunk.down_chunks, scaled);

    /* Check whether dataset has filters applied */
    filtered_dataset = dinfo->dset->shared->dcpl_cache.pline.nused > 0;

    /* Iterate through each chunk in the dataset */
    while (sel_points) {
        /* Check for intersection of current chunk and file selection */
        if (true == H5S_SELECT_INTERSECT_BLOCK(dinfo->file_space, coords, end)) {
            H5D_piece_info_t *new_piece_info; /* chunk information to insert into skip list */
            hsize_t           chunk_points;   /* Number of elements in chunk selection */

            /* Create dataspace for chunk, 'AND'ing the overall selection with
             *  the current chunk.
             */
            if (H5S_combine_hyperslab(dinfo->file_space, H5S_SELECT_AND, coords, NULL, fm->chunk_dim, NULL,
                                      &tmp_fchunk) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL,
                            "unable to combine file space selection with chunk block");

            /* Resize chunk's dataspace dimensions to size of chunk */
            if (H5S_set_extent_real(tmp_fchunk, fm->chunk_dim) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL, "can't adjust chunk dimensions");

            /* Move selection back to have correct offset in chunk */
            if (H5S_SELECT_ADJUST_U(tmp_fchunk, coords) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL, "can't adjust chunk selection");

            /* Add temporary chunk to the list of chunks */

            /* Allocate the file & memory chunk information */
            if (NULL == (new_piece_info = H5FL_MALLOC(H5D_piece_info_t)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate chunk info");

            /* Initialize the chunk information */

            /* Set the chunk index */
            new_piece_info->index = chunk_index;

            /* Set the file chunk dataspace */
            new_piece_info->fspace        = tmp_fchunk;
            new_piece_info->fspace_shared = false;
            tmp_fchunk                    = NULL;

            /* Set the memory chunk dataspace */
            new_piece_info->mspace        = NULL;
            new_piece_info->mspace_shared = false;

            /* Copy the chunk's scaled coordinates */
            H5MM_memcpy(new_piece_info->scaled, scaled, sizeof(hsize_t) * fm->f_ndims);
            new_piece_info->scaled[fm->f_ndims] = 0;

            /* make connection to related dset info from this piece_info */
            new_piece_info->dset_info = dinfo;

            /* Initialize in-place type conversion info. Start with it disabled. */
            new_piece_info->in_place_tconv = false;
            new_piece_info->buf_off        = 0;

            new_piece_info->filtered_dset = filtered_dataset;

            /* Add piece to global piece_count */
            io_info->piece_count++;

            /* Insert the new piece into the skip list */
            if (H5SL_insert(fm->dset_sel_pieces, new_piece_info, &new_piece_info->index) < 0) {
                H5D__free_piece_info(new_piece_info, NULL, NULL);
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't insert piece into skip list");
            } /* end if */

            /* Get number of elements selected in chunk */
            chunk_points                 = H5S_GET_SELECT_NPOINTS(new_piece_info->fspace);
            new_piece_info->piece_points = chunk_points;

            /* Decrement # of points left in file selection */
            sel_points -= chunk_points;

            /* Leave if we are done */
            if (sel_points == 0)
                HGOTO_DONE(SUCCEED);
        } /* end if */

        /* Increment chunk index */
        chunk_index++;

        /* Set current increment dimension */
        curr_dim = (int)fm->f_ndims - 1;

        /* Increment chunk location in fastest changing dimension */
        coords[curr_dim] += fm->chunk_dim[curr_dim];
        end[curr_dim] += fm->chunk_dim[curr_dim];
        scaled[curr_dim]++;

        /* Bring chunk location back into bounds, if necessary */
        if (coords[curr_dim] > sel_end[curr_dim]) {
            do {
                /* Reset current dimension's location to 0 */
                scaled[curr_dim] = start_scaled[curr_dim];
                coords[curr_dim] =
                    start_coords[curr_dim]; /*lint !e771 The start_coords will always be initialized */
                end[curr_dim] = (coords[curr_dim] + fm->chunk_dim[curr_dim]) - 1;

                /* Decrement current dimension */
                curr_dim--;

                /* Check for valid current dim */
                if (curr_dim >= 0) {
                    /* Increment chunk location in current dimension */
                    scaled[curr_dim]++;
                    coords[curr_dim] += fm->chunk_dim[curr_dim];
                    end[curr_dim] = (coords[curr_dim] + fm->chunk_dim[curr_dim]) - 1;
                } /* end if */
            } while (curr_dim >= 0 && (coords[curr_dim] > sel_end[curr_dim]));

            /* Re-calculate the index of this chunk */
            chunk_index = H5VM_array_offset_pre(fm->f_ndims, dinfo->layout->u.chunk.down_chunks, scaled);
        } /* end if */
    }     /* end while */

done:
    /* Clean up on failure */
    if (ret_value < 0)
        if (tmp_fchunk && H5S_close(tmp_fchunk) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "can't release temporary dataspace");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__create_piece_file_map_hyper() */

/*-------------------------------------------------------------------------
 * Function:    H5D__create_piece_mem_map_hyper
 *
 * Purpose:     Create all chunk selections in memory by copying the file
 *              chunk selections and adjusting their offsets to be correct
 *              or the memory.
 *              This was derived from H5D__create_chunk_mem_map_hyper for
 *              multi-dset work.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Assumptions: That the file and memory selections are the same shape.
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__create_piece_mem_map_hyper(const H5D_dset_io_info_t *dinfo)
{
    H5D_chunk_map_t  *fm;                           /* Convenience pointer to chunk map */
    H5D_piece_info_t *piece_info;                   /* Pointer to piece information */
    H5SL_node_t      *curr_node;                    /* Current node in skip list */
    hsize_t           file_sel_start[H5S_MAX_RANK]; /* Offset of low bound of file selection */
    hsize_t           file_sel_end[H5S_MAX_RANK];   /* Offset of high bound of file selection */
    hsize_t           mem_sel_start[H5S_MAX_RANK];  /* Offset of low bound of file selection */
    hsize_t           mem_sel_end[H5S_MAX_RANK];    /* Offset of high bound of file selection */
    hssize_t          adjust[H5S_MAX_RANK];         /* Adjustment to make to all file chunks */
    unsigned          u;                            /* Local index variable */
    herr_t            ret_value = SUCCEED;          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dinfo->layout_io_info.chunk_map->f_ndims > 0);

    /* Set convenience pointer */
    fm = dinfo->layout_io_info.chunk_map;

    /* Check for all I/O going to a single chunk */
    if (H5SL_count(fm->dset_sel_pieces) == 1) {
        /* Get the node */
        curr_node = H5SL_first(fm->dset_sel_pieces);

        /* Get pointer to piece's information */
        piece_info = (H5D_piece_info_t *)H5SL_item(curr_node);
        assert(piece_info);

        /* Just point at the memory dataspace & selection */
        piece_info->mspace = dinfo->mem_space;

        /* Indicate that the piece's memory space is shared */
        piece_info->mspace_shared = true;
    } /* end if */
    else {
        /* Get bounding box for file selection */
        if (H5S_SELECT_BOUNDS(dinfo->file_space, file_sel_start, file_sel_end) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file selection bound info");

        /* Get bounding box for memory selection */
        if (H5S_SELECT_BOUNDS(dinfo->mem_space, mem_sel_start, mem_sel_end) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file selection bound info");

        /* Calculate the adjustment for memory selection from file selection */
        assert(fm->m_ndims == fm->f_ndims);
        for (u = 0; u < fm->f_ndims; u++) {
            H5_CHECK_OVERFLOW(file_sel_start[u], hsize_t, hssize_t);
            H5_CHECK_OVERFLOW(mem_sel_start[u], hsize_t, hssize_t);
            adjust[u] = (hssize_t)file_sel_start[u] - (hssize_t)mem_sel_start[u];
        } /* end for */

        /* Iterate over each chunk in the chunk list */
        assert(fm->dset_sel_pieces);
        curr_node = H5SL_first(fm->dset_sel_pieces);
        while (curr_node) {
            hsize_t      coords[H5S_MAX_RANK];       /* Current coordinates of chunk */
            hssize_t     piece_adjust[H5S_MAX_RANK]; /* Adjustment to make to a particular chunk */
            H5S_sel_type chunk_sel_type;             /* Chunk's selection type */

            /* Get pointer to piece's information */
            piece_info = (H5D_piece_info_t *)H5SL_item(curr_node);
            assert(piece_info);

            /* Compute the chunk coordinates from the scaled coordinates */
            for (u = 0; u < fm->f_ndims; u++)
                coords[u] = piece_info->scaled[u] * dinfo->layout->u.chunk.dim[u];

            /* Copy the information */

            /* Copy the memory dataspace */
            if ((piece_info->mspace = H5S_copy(dinfo->mem_space, true, false)) == NULL)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy memory space");

            /* Get the chunk's selection type */
            if ((chunk_sel_type = H5S_GET_SELECT_TYPE(piece_info->fspace)) < H5S_SEL_NONE)
                HGOTO_ERROR(H5E_DATASET, H5E_BADSELECT, FAIL, "unable to get type of selection");

            /* Set memory selection for "all" chunk selections */
            if (H5S_SEL_ALL == chunk_sel_type) {
                /* Adjust the chunk coordinates */
                for (u = 0; u < fm->f_ndims; u++)
                    coords[u] = (hsize_t)((hssize_t)coords[u] - adjust[u]);

                /* Set to same shape as chunk */
                if (H5S_select_hyperslab(piece_info->mspace, H5S_SELECT_SET, coords, NULL, fm->chunk_dim,
                                         NULL) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL, "can't create chunk memory selection");
            } /* end if */
            else {
                /* Sanity check */
                assert(H5S_SEL_HYPERSLABS == chunk_sel_type);

                /* Copy the file chunk's selection */
                if (H5S_SELECT_COPY(piece_info->mspace, piece_info->fspace, false) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy selection");

                /* Compute the adjustment for this chunk */
                for (u = 0; u < fm->f_ndims; u++) {
                    /* Compensate for the chunk offset */
                    H5_CHECK_OVERFLOW(coords[u], hsize_t, hssize_t);
                    piece_adjust[u] = adjust[u] - (hssize_t)coords[u];
                } /* end for */

                /* Adjust the selection */
                if (H5S_SELECT_ADJUST_S(piece_info->mspace, piece_adjust) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to adjust selection");
            } /* end else */

            /* Get the next piece node in the skip list */
            curr_node = H5SL_next(curr_node);
        } /* end while */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__create_piece_mem_map_hyper() */

/*-------------------------------------------------------------------------
 * Function:    H5D__create_piece_mem_map_1d
 *
 * Purpose:    Create all chunk selections for 1-dimensional regular memory space
 *          that has only one single block in the selection
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__create_piece_mem_map_1d(const H5D_dset_io_info_t *dinfo)
{
    H5D_chunk_map_t  *fm;                  /* Convenience pointer to chunk map */
    H5D_piece_info_t *piece_info;          /* Pointer to chunk information */
    H5SL_node_t      *curr_node;           /* Current node in skip list */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dinfo->layout_io_info.chunk_map->f_ndims > 0);

    /* Set convenience pointer */
    fm = dinfo->layout_io_info.chunk_map;
    assert(fm);

    /* Check for all I/O going to a single chunk */
    if (H5SL_count(fm->dset_sel_pieces) == 1) {
        /* Get the node */
        curr_node = H5SL_first(fm->dset_sel_pieces);

        /* Get pointer to chunk's information */
        piece_info = (H5D_piece_info_t *)H5SL_item(curr_node);
        assert(piece_info);

        /* Just point at the memory dataspace & selection */
        piece_info->mspace = dinfo->mem_space;

        /* Indicate that the chunk's memory space is shared */
        piece_info->mspace_shared = true;
    } /* end if */
    else {
        hsize_t mem_sel_start[H5S_MAX_RANK]; /* Offset of low bound of file selection */
        hsize_t mem_sel_end[H5S_MAX_RANK];   /* Offset of high bound of file selection */

        assert(fm->m_ndims == 1);

        if (H5S_SELECT_BOUNDS(dinfo->mem_space, mem_sel_start, mem_sel_end) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get file selection bound info");

        /* Iterate over each chunk in the chunk list */
        curr_node = H5SL_first(fm->dset_sel_pieces);
        while (curr_node) {
            hsize_t chunk_points; /* Number of elements in chunk selection */
            hsize_t tmp_count = 1;

            /* Get pointer to chunk's information */
            piece_info = (H5D_piece_info_t *)H5SL_item(curr_node);
            assert(piece_info);

            /* Copy the memory dataspace */
            if ((piece_info->mspace = H5S_copy(dinfo->mem_space, true, false)) == NULL)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy memory space");

            chunk_points = H5S_GET_SELECT_NPOINTS(piece_info->fspace);

            if (H5S_select_hyperslab(piece_info->mspace, H5S_SELECT_SET, mem_sel_start, NULL, &tmp_count,
                                     &chunk_points) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL, "can't create chunk memory selection");

            mem_sel_start[0] += chunk_points;

            /* Get the next chunk node in the skip list */
            curr_node = H5SL_next(curr_node);
        } /* end while */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__create_piece_mem_map_1d() */

/*-------------------------------------------------------------------------
 * Function:    H5D__piece_file_cb
 *
 * Purpose:     Callback routine for file selection iterator.  Used when
 *              creating selections in file for each point selected.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__piece_file_cb(void H5_ATTR_UNUSED *elem, const H5T_t H5_ATTR_UNUSED *type, unsigned ndims,
                   const hsize_t *coords, void *_opdata)
{
    H5D_io_info_wrap_t *opdata  = (H5D_io_info_wrap_t *)_opdata;
    H5D_io_info_t      *io_info = (H5D_io_info_t *)opdata->io_info;    /* io info for multi dset */
    H5D_dset_io_info_t *dinfo   = (H5D_dset_io_info_t *)opdata->dinfo; /* File<->memory piece mapping info */
    H5D_chunk_map_t    *fm;                                            /* Convenience pointer to chunk map */
    H5D_piece_info_t   *piece_info;                        /* Chunk information for current piece */
    hsize_t             coords_in_chunk[H5O_LAYOUT_NDIMS]; /* Coordinates of element in chunk */
    hsize_t             chunk_index;                       /* Chunk index */
    hsize_t             scaled[H5S_MAX_RANK];              /* Scaled coordinates for this chunk */
    unsigned            u;                                 /* Local index variable */
    herr_t              ret_value = SUCCEED;               /* Return value        */

    FUNC_ENTER_PACKAGE

    /* Set convenience pointer */
    fm = dinfo->layout_io_info.chunk_map;

    /* Calculate the index of this chunk */
    chunk_index = H5VM_chunk_index_scaled(ndims, coords, dinfo->layout->u.chunk.dim,
                                          dinfo->layout->u.chunk.down_chunks, scaled);

    /* Find correct chunk in file & memory skip list */
    if (chunk_index == fm->last_index) {
        /* If the chunk index is the same as the last chunk index we used,
         * get the cached info to operate on.
         */
        piece_info = fm->last_piece_info;
    } /* end if */
    else {
        /* If the chunk index is not the same as the last chunk index we used,
         * find the chunk in the skip list.  If we do not find it, create
         * a new node. */
        if (NULL == (piece_info = (H5D_piece_info_t *)H5SL_search(fm->dset_sel_pieces, &chunk_index))) {
            H5S_t *fspace; /* Memory chunk's dataspace */

            /* Allocate the file & memory chunk information */
            if (NULL == (piece_info = H5FL_MALLOC(H5D_piece_info_t)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "can't allocate chunk info");

            /* Initialize the chunk information */

            /* Set the chunk index */
            piece_info->index = chunk_index;

            /* Create a dataspace for the chunk */
            if ((fspace = H5S_create_simple(fm->f_ndims, fm->chunk_dim, NULL)) == NULL) {
                piece_info = H5FL_FREE(H5D_piece_info_t, piece_info);
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "unable to create dataspace for chunk");
            } /* end if */

            /* De-select the chunk space */
            if (H5S_select_none(fspace) < 0) {
                (void)H5S_close(fspace);
                piece_info = H5FL_FREE(H5D_piece_info_t, piece_info);
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to de-select dataspace");
            } /* end if */

            /* Set the file chunk dataspace */
            piece_info->fspace        = fspace;
            piece_info->fspace_shared = false;

            /* Set the memory chunk dataspace */
            piece_info->mspace        = NULL;
            piece_info->mspace_shared = false;

            /* Set the number of selected elements in chunk to zero */
            piece_info->piece_points = 0;

            /* Set the chunk's scaled coordinates */
            H5MM_memcpy(piece_info->scaled, scaled, sizeof(hsize_t) * fm->f_ndims);
            piece_info->scaled[fm->f_ndims] = 0;

            /* Initialize in-place type conversion info. Start with it disabled. */
            piece_info->in_place_tconv = false;
            piece_info->buf_off        = 0;

            piece_info->filtered_dset = dinfo->dset->shared->dcpl_cache.pline.nused > 0;

            /* Make connection to related dset info from this piece_info */
            piece_info->dset_info = dinfo;

            /* Insert the new chunk into the skip list */
            if (H5SL_insert(fm->dset_sel_pieces, piece_info, &piece_info->index) < 0) {
                H5D__free_piece_info(piece_info, NULL, NULL);
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't insert chunk into dataset skip list");
            } /* end if */

            /* Add piece to global piece_count */
            io_info->piece_count++;
        } /* end if */

        /* Update the "last chunk seen" information */
        fm->last_index      = chunk_index;
        fm->last_piece_info = piece_info;
    } /* end else */

    /* Get the offset of the element within the chunk */
    for (u = 0; u < fm->f_ndims; u++)
        coords_in_chunk[u] = coords[u] - (scaled[u] * dinfo->layout->u.chunk.dim[u]);

    /* Add point to file selection for chunk */
    if (H5S_select_elements(piece_info->fspace, H5S_SELECT_APPEND, (size_t)1, coords_in_chunk) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "unable to select element");

    /* Increment the number of elemented selected in chunk */
    piece_info->piece_points++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__piece_file_cb */

/*-------------------------------------------------------------------------
 * Function:    H5D__piece_mem_cb
 *
 * Purpose:     Callback routine for file selection iterator.  Used when
 *              creating selections in memory for each piece.
 *              This was derived from H5D__chunk_mem_cb for multi-dset
 *              work.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__piece_mem_cb(void H5_ATTR_UNUSED *elem, const H5T_t H5_ATTR_UNUSED *type, unsigned ndims,
                  const hsize_t *coords, void *_opdata)
{
    H5D_io_info_wrap_t *opdata = (H5D_io_info_wrap_t *)_opdata;
    H5D_dset_io_info_t *dinfo  = (H5D_dset_io_info_t *)opdata->dinfo; /* File<->memory chunk mapping info */
    H5D_piece_info_t   *piece_info;                  /* Chunk information for current chunk */
    H5D_chunk_map_t    *fm;                          /* Convenience pointer to chunk map */
    hsize_t             coords_in_mem[H5S_MAX_RANK]; /* Coordinates of element in memory */
    hsize_t             chunk_index;                 /* Chunk index */
    herr_t              ret_value = SUCCEED;         /* Return value        */

    FUNC_ENTER_PACKAGE

    /* Set convenience pointer */
    fm = dinfo->layout_io_info.chunk_map;

    /* Calculate the index of this chunk */
    chunk_index =
        H5VM_chunk_index(ndims, coords, dinfo->layout->u.chunk.dim, dinfo->layout->u.chunk.down_chunks);

    /* Find correct chunk in file & memory skip list */
    if (chunk_index == fm->last_index) {
        /* If the chunk index is the same as the last chunk index we used,
         * get the cached spaces to operate on.
         */
        piece_info = fm->last_piece_info;
    } /* end if */
    else {
        /* If the chunk index is not the same as the last chunk index we used,
         * find the chunk in the dataset skip list.
         */
        /* Get the chunk node from the skip list */
        if (NULL == (piece_info = (H5D_piece_info_t *)H5SL_search(fm->dset_sel_pieces, &chunk_index)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_NOTFOUND, H5_ITER_ERROR,
                        "can't locate piece in dataset skip list");

        /* Check if the chunk already has a memory space */
        if (NULL == piece_info->mspace)
            /* Copy the template memory chunk dataspace */
            if (NULL == (piece_info->mspace = H5S_copy(fm->mchunk_tmpl, false, false)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, H5_ITER_ERROR, "unable to copy file space");

        /* Update the "last chunk seen" information */
        fm->last_index      = chunk_index;
        fm->last_piece_info = piece_info;
    } /* end else */

    /* Get coordinates of selection iterator for memory */
    if (H5S_SELECT_ITER_COORDS(&fm->mem_iter, coords_in_mem) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, H5_ITER_ERROR, "unable to get iterator coordinates");

    /* Add point to memory selection for chunk */
    if (fm->msel_type == H5S_SEL_POINTS) {
        if (H5S_select_elements(piece_info->mspace, H5S_SELECT_APPEND, (size_t)1, coords_in_mem) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, H5_ITER_ERROR, "unable to select element");
    } /* end if */
    else {
        if (H5S_hyper_add_span_element(piece_info->mspace, fm->m_ndims, coords_in_mem) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, H5_ITER_ERROR, "unable to select element");
    } /* end else */

    /* Move memory selection iterator to next element in selection */
    if (H5S_SELECT_ITER_NEXT(&fm->mem_iter, (size_t)1) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTNEXT, H5_ITER_ERROR, "unable to move to next iterator location");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__piece_mem_cb() */

/*-------------------------------------------------------------------------
 * Function:   H5D__chunk_mdio_init
 *
 * Purpose:    Performs second phase of initialization for multi-dataset
 *             I/O.  Currently looks up chunk addresses and adds chunks to
 *             sel_pieces.
 *
 * Return:     Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_mdio_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo)
{
    H5SL_node_t      *piece_node;          /* Current node in chunk skip list */
    H5D_piece_info_t *piece_info;          /* Piece information for current piece */
    H5D_chunk_ud_t    udata;               /* Chunk data from index */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get first node in skip list.  Note we don't check for failure since NULL
     * simply indicates an empty skip list. */
    piece_node = H5D_CHUNK_GET_FIRST_NODE(dinfo);

    /* Iterate over skip list */
    while (piece_node) {
        /* Get piece info */
        if (NULL == (piece_info = (H5D_piece_info_t *)H5D_CHUNK_GET_NODE_INFO(dinfo, piece_node)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "couldn't get piece info from list");

        /* Get the info for the chunk in the file */
        if (H5D__chunk_lookup(dinfo->dset, piece_info->scaled, &udata) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

        /* Save chunk file address */
        piece_info->faddr = udata.chunk_block.offset;

        /* Add piece to MDIO operation if it has a file address */
        if (H5_addr_defined(piece_info->faddr)) {
            assert(io_info->sel_pieces);
            assert(io_info->pieces_added < io_info->piece_count);

            /* Add to sel_pieces and update pieces_added */
            io_info->sel_pieces[io_info->pieces_added++] = piece_info;

            if (piece_info->filtered_dset)
                io_info->filtered_pieces_added++;
        }

        /* Advance to next skip list node */
        piece_node = H5D_CHUNK_GET_NEXT_NODE(dinfo, piece_node);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_mdio_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_cacheable
 *
 * Purpose:    A small internal function to if it's possible to load the
 *              chunk into cache.
 *
 * Return:    true or false
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5D__chunk_cacheable(const H5D_io_info_t H5_ATTR_PARALLEL_USED *io_info, H5D_dset_io_info_t *dset_info,
                     haddr_t caddr, bool write_op)
{
    const H5D_t *dataset     = NULL;  /* Local pointer to dataset info */
    bool         has_filters = false; /* Whether there are filters on the chunk or not */
    htri_t       ret_value   = FAIL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset_info);
    dataset = dset_info->dset;
    assert(dataset);

    /* Must bring the whole chunk in if there are any filters on the chunk.
     * Make sure to check if filters are on the dataset but disabled for the
     * chunk because it is a partial edge chunk. */
    if (dataset->shared->dcpl_cache.pline.nused > 0) {
        if (dataset->shared->layout.u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS) {
            has_filters =
                !H5D__chunk_is_partial_edge_chunk(dataset->shared->ndims, dataset->shared->layout.u.chunk.dim,
                                                  dset_info->store->chunk.scaled, dataset->shared->curr_dims);
        } /* end if */
        else
            has_filters = true;
    } /* end if */

    if (has_filters)
        ret_value = true;
    else {
#ifdef H5_HAVE_PARALLEL
        /* If MPI based VFD is used and the file is opened for write access, must
         *         bypass the chunk-cache scheme because other MPI processes could
         *         be writing to other elements in the same chunk.  Do a direct
         *         write-through of only the elements requested.
         */
        if (io_info->using_mpi_vfd && (H5F_ACC_RDWR & H5F_INTENT(dataset->oloc.file)))
            ret_value = false;
        else {
#endif /* H5_HAVE_PARALLEL */
            /* If the chunk is too large to keep in the cache and if we don't
             * need to write the fill value, then don't load the chunk into the
             * cache, just write the data to it directly.
             */
            H5_CHECK_OVERFLOW(dataset->shared->layout.u.chunk.size, uint32_t, size_t);
            if ((size_t)dataset->shared->layout.u.chunk.size > dataset->shared->cache.chunk.nbytes_max) {
                if (write_op && !H5_addr_defined(caddr)) {
                    const H5O_fill_t *fill = &(dataset->shared->dcpl_cache.fill); /* Fill value info */
                    H5D_fill_value_t  fill_status;                                /* Fill value status */

                    /* Revtrieve the fill value status */
                    if (H5P_is_fill_value_defined(fill, &fill_status) < 0)
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't tell if fill value defined");

                    /* If the fill value needs to be written then we will need
                     * to use the cache to write the fill value */
                    if (fill->fill_time == H5D_FILL_TIME_ALLOC ||
                        (fill->fill_time == H5D_FILL_TIME_IFSET &&
                         (fill_status == H5D_FILL_VALUE_USER_DEFINED ||
                          fill_status == H5D_FILL_VALUE_DEFAULT)))
                        ret_value = true;
                    else
                        ret_value = false;
                }
                else
                    ret_value = false;
            }
            else
                ret_value = true;
#ifdef H5_HAVE_PARALLEL
        } /* end else */
#endif    /* H5_HAVE_PARALLEL */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_cacheable() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_may_use_select_io
 *
 * Purpose:    A small internal function to if it may be possible to use
 *             selection I/O.
 *
 * Return:    true or false
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_may_use_select_io(H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info)
{
    const H5D_t *dataset   = NULL;    /* Local pointer to dataset info */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(dset_info);

    dataset = dset_info->dset;
    assert(dataset);

    /* Don't use selection I/O if there are filters on the dataset (for now) */
    if (dataset->shared->dcpl_cache.pline.nused > 0) {
        io_info->use_select_io = H5D_SELECTION_IO_MODE_OFF;
        io_info->no_selection_io_cause |= H5D_SEL_IO_DATASET_FILTER;
    }
    else {
        bool page_buf_enabled;

        /* Check if the page buffer is enabled */
        if (H5PB_enabled(io_info->f_sh, H5FD_MEM_DRAW, &page_buf_enabled) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't check if page buffer is enabled");
        if (page_buf_enabled) {
            /* Note that page buffer is disabled in parallel */
            io_info->use_select_io = H5D_SELECTION_IO_MODE_OFF;
            io_info->no_selection_io_cause |= H5D_SEL_IO_PAGE_BUFFER;
        }
        else {
            /* Check if chunks in this dataset may be cached, if so don't use
             * selection I/O (for now).  Note that chunks temporarily cached for
             * the purpose of writing the fill value don't count, since they are
             * immediately evicted. */
#ifdef H5_HAVE_PARALLEL
            /* If MPI based VFD is used and the file is opened for write access,
             * must bypass the chunk-cache scheme because other MPI processes
             * could be writing to other elements in the same chunk.
             */
            if (!(io_info->using_mpi_vfd && (H5F_ACC_RDWR & H5F_INTENT(dataset->oloc.file)))) {
#endif /* H5_HAVE_PARALLEL */
                /* Check if the chunk is too large to keep in the cache */
                H5_CHECK_OVERFLOW(dataset->shared->layout.u.chunk.size, uint32_t, size_t);
                if ((size_t)dataset->shared->layout.u.chunk.size <= dataset->shared->cache.chunk.nbytes_max) {
                    io_info->use_select_io = H5D_SELECTION_IO_MODE_OFF;
                    io_info->no_selection_io_cause |= H5D_SEL_IO_CHUNK_CACHE;
                }
#ifdef H5_HAVE_PARALLEL
            } /* end else */
#endif        /* H5_HAVE_PARALLEL */
        }     /* end else */
    }         /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_may_use_select_io() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_read
 *
 * Purpose:    Read from a chunked dataset.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_read(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_info)
{
    H5SL_node_t       *chunk_node;                  /* Current node in chunk skip list */
    H5D_io_info_t      nonexistent_io_info;         /* "nonexistent" I/O info object */
    H5D_dset_io_info_t nonexistent_dset_info;       /* "nonexistent" I/O dset info object */
    H5D_dset_io_info_t ctg_dset_info;               /* Contiguous I/O dset info object */
    H5D_dset_io_info_t cpt_dset_info;               /* Compact I/O dset info object */
    uint32_t           src_accessed_bytes  = 0;     /* Total accessed size in a chunk */
    bool               skip_missing_chunks = false; /* Whether to skip missing chunks */
    H5S_t            **chunk_mem_spaces    = NULL;  /* Array of chunk memory spaces */
    H5S_t             *chunk_mem_spaces_local[8];   /* Local buffer for chunk_mem_spaces */
    H5S_t            **chunk_file_spaces = NULL;    /* Array of chunk file spaces */
    H5S_t             *chunk_file_spaces_local[8];  /* Local buffer for chunk_file_spaces */
    haddr_t           *chunk_addrs = NULL;          /* Array of chunk addresses */
    haddr_t            chunk_addrs_local[8];        /* Local buffer for chunk_addrs */
    herr_t             ret_value = SUCCEED;         /*return value        */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(dset_info);
    assert(dset_info->buf.vp);

    /* Set up "nonexistent" I/O info object */
    H5MM_memcpy(&nonexistent_io_info, io_info, sizeof(nonexistent_io_info));
    H5MM_memcpy(&nonexistent_dset_info, dset_info, sizeof(nonexistent_dset_info));
    nonexistent_dset_info.layout_ops = *H5D_LOPS_NONEXISTENT;
    nonexistent_io_info.dsets_info   = &nonexistent_dset_info;
    nonexistent_io_info.count        = 1;

    {
        const H5O_fill_t *fill = &(dset_info->dset->shared->dcpl_cache.fill); /* Fill value info */
        H5D_fill_value_t  fill_status;                                        /* Fill value status */

        /* Check the fill value status */
        if (H5P_is_fill_value_defined(fill, &fill_status) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't tell if fill value defined");

        /* If we are never to return fill values, or if we would return them
         * but they aren't set, set the flag to skip missing chunks.
         */
        if (fill->fill_time == H5D_FILL_TIME_NEVER ||
            (fill->fill_time == H5D_FILL_TIME_IFSET && fill_status != H5D_FILL_VALUE_USER_DEFINED &&
             fill_status != H5D_FILL_VALUE_DEFAULT))
            skip_missing_chunks = true;
    }

    /* Different blocks depending on whether we're using selection I/O */
    if (io_info->use_select_io == H5D_SELECTION_IO_MODE_ON) {
        size_t num_chunks       = 0;
        size_t element_sizes[2] = {dset_info->type_info.src_type_size, 0};
        void  *bufs[2]          = {dset_info->buf.vp, NULL};

        /* Only create selection I/O arrays if not performing multi dataset I/O,
         * otherwise the higher level will handle it */
        if (H5D_LAYOUT_CB_PERFORM_IO(io_info)) {
            /* Cache number of chunks */
            num_chunks = H5D_CHUNK_GET_NODE_COUNT(dset_info);

            /* Allocate arrays of dataspaces and offsets for use with selection I/O,
             * or point to local buffers */
            assert(sizeof(chunk_mem_spaces_local) / sizeof(chunk_mem_spaces_local[0]) ==
                   sizeof(chunk_file_spaces_local) / sizeof(chunk_file_spaces_local[0]));
            assert(sizeof(chunk_mem_spaces_local) / sizeof(chunk_mem_spaces_local[0]) ==
                   sizeof(chunk_addrs_local) / sizeof(chunk_addrs_local[0]));
            if (num_chunks > (sizeof(chunk_mem_spaces_local) / sizeof(chunk_mem_spaces_local[0]))) {
                if (NULL == (chunk_mem_spaces = H5MM_malloc(num_chunks * sizeof(H5S_t *))))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                "memory allocation failed for memory space list");
                if (NULL == (chunk_file_spaces = H5MM_malloc(num_chunks * sizeof(H5S_t *))))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                "memory allocation failed for file space list");
                if (NULL == (chunk_addrs = H5MM_malloc(num_chunks * sizeof(haddr_t))))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                "memory allocation failed for chunk address list");
            } /* end if */
            else {
                chunk_mem_spaces  = chunk_mem_spaces_local;
                chunk_file_spaces = chunk_file_spaces_local;
                chunk_addrs       = chunk_addrs_local;
            } /* end else */

            /* Reset num_chunks */
            num_chunks = 0;
        } /* end if */

        /* Iterate through nodes in chunk skip list */
        chunk_node = H5D_CHUNK_GET_FIRST_NODE(dset_info);
        while (chunk_node) {
            H5D_piece_info_t *chunk_info; /* Chunk information */
            H5D_chunk_ud_t    udata;      /* Chunk index pass-through    */

            /* Get the actual chunk information from the skip list node */
            chunk_info = H5D_CHUNK_GET_NODE_INFO(dset_info, chunk_node);

            /* Get the info for the chunk in the file */
            if (H5D__chunk_lookup(dset_info->dset, chunk_info->scaled, &udata) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

            /* There should be no chunks cached */
            assert(UINT_MAX == udata.idx_hint);

            /* Sanity check */
            assert((H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length > 0) ||
                   (!H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length == 0));

            /* Check for non-existent chunk & skip it if appropriate */
            if (H5_addr_defined(udata.chunk_block.offset)) {
                /* Add chunk to list for selection I/O, if not performing multi dataset I/O */
                if (H5D_LAYOUT_CB_PERFORM_IO(io_info)) {
                    chunk_mem_spaces[num_chunks]  = chunk_info->mspace;
                    chunk_file_spaces[num_chunks] = chunk_info->fspace;
                    chunk_addrs[num_chunks]       = udata.chunk_block.offset;
                    num_chunks++;
                } /* end if */
                else {
                    /* Add to mdset selection I/O arrays */
                    assert(io_info->mem_spaces);
                    assert(io_info->file_spaces);
                    assert(io_info->addrs);
                    assert(io_info->element_sizes);
                    assert(io_info->rbufs);
                    assert(io_info->pieces_added < io_info->piece_count);

                    io_info->mem_spaces[io_info->pieces_added]    = chunk_info->mspace;
                    io_info->file_spaces[io_info->pieces_added]   = chunk_info->fspace;
                    io_info->addrs[io_info->pieces_added]         = udata.chunk_block.offset;
                    io_info->element_sizes[io_info->pieces_added] = element_sizes[0];
                    io_info->rbufs[io_info->pieces_added]         = bufs[0];
                    if (io_info->sel_pieces)
                        io_info->sel_pieces[io_info->pieces_added] = chunk_info;
                    io_info->pieces_added++;

                    if (io_info->sel_pieces && chunk_info->filtered_dset)
                        io_info->filtered_pieces_added++;
                }
            } /* end if */
            else if (!skip_missing_chunks) {
                /* Set up nonexistent dataset info for (fill value) read from nonexistent chunk */
                nonexistent_dset_info.layout_io_info.contig_piece_info = chunk_info;
                nonexistent_dset_info.file_space                       = chunk_info->fspace;
                nonexistent_dset_info.mem_space                        = chunk_info->mspace;
                nonexistent_dset_info.nelmts                           = chunk_info->piece_points;

                /* Set request_nelmts.  This is not normally set by the upper layers because selection I/O
                 * usually does not use strip mining (H5D__scatgath_write), and instead allocates buffers
                 * large enough for the entire I/O.  Set request_nelmts to be large enough for all selected
                 * elements in this chunk because it must be at least that large */
                nonexistent_dset_info.type_info.request_nelmts = nonexistent_dset_info.nelmts;

                /* Perform the actual read operation from the nonexistent chunk
                 */
                if ((dset_info->io_ops.single_read)(&nonexistent_io_info, &nonexistent_dset_info) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "chunked read failed");
            } /* end if */

            /* Advance to next chunk in list */
            chunk_node = H5D_CHUNK_GET_NEXT_NODE(dset_info, chunk_node);
        } /* end while */

        /* Only perform I/O if not performing multi dataset I/O or type conversion, otherwise the
         * higher level will handle it after all datasets have been processed */
        if (H5D_LAYOUT_CB_PERFORM_IO(io_info)) {
            /* Issue selection I/O call (we can skip the page buffer because we've
             * already verified it won't be used, and the metadata accumulator
             * because this is raw data) */
            H5_CHECK_OVERFLOW(num_chunks, size_t, uint32_t);
            if (H5F_shared_select_read(H5F_SHARED(dset_info->dset->oloc.file), H5FD_MEM_DRAW,
                                       (uint32_t)num_chunks, chunk_mem_spaces, chunk_file_spaces, chunk_addrs,
                                       element_sizes, bufs) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "chunk selection read failed");

            /* Clean up memory */
            if (chunk_mem_spaces != chunk_mem_spaces_local) {
                assert(chunk_file_spaces != chunk_file_spaces_local);
                assert(chunk_addrs != chunk_addrs_local);
                chunk_mem_spaces  = H5MM_xfree(chunk_mem_spaces);
                chunk_file_spaces = H5MM_xfree(chunk_file_spaces);
                chunk_addrs       = H5MM_xfree(chunk_addrs);
            } /* end if */
        }     /* end if */

#ifdef H5_HAVE_PARALLEL
        /* Report that collective chunk I/O was used (will only be set on the DXPL if collective I/O was
         * requested) */
        io_info->actual_io_mode |= H5D_MPIO_CHUNK_COLLECTIVE;
#endif /* H5_HAVE_PARALLEL */
    }  /* end if */
    else {
        H5D_io_info_t ctg_io_info; /* Contiguous I/O info object */
        H5D_storage_t ctg_store;   /* Chunk storage information as contiguous dataset */
        H5D_io_info_t cpt_io_info; /* Compact I/O info object */
        H5D_storage_t cpt_store;   /* Chunk storage information as compact dataset */
        bool          cpt_dirty;   /* Temporary placeholder for compact storage "dirty" flag */

        /* Set up contiguous I/O info object */
        H5MM_memcpy(&ctg_io_info, io_info, sizeof(ctg_io_info));
        H5MM_memcpy(&ctg_dset_info, dset_info, sizeof(ctg_dset_info));
        ctg_dset_info.store      = &ctg_store;
        ctg_dset_info.layout_ops = *H5D_LOPS_CONTIG;
        ctg_io_info.dsets_info   = &ctg_dset_info;
        ctg_io_info.count        = 1;

        /* Initialize temporary contiguous storage info */
        H5_CHECKED_ASSIGN(ctg_store.contig.dset_size, hsize_t, dset_info->dset->shared->layout.u.chunk.size,
                          uint32_t);

        /* Set up compact I/O info object */
        H5MM_memcpy(&cpt_io_info, io_info, sizeof(cpt_io_info));
        H5MM_memcpy(&cpt_dset_info, dset_info, sizeof(cpt_dset_info));
        cpt_dset_info.store      = &cpt_store;
        cpt_dset_info.layout_ops = *H5D_LOPS_COMPACT;
        cpt_io_info.dsets_info   = &cpt_dset_info;
        cpt_io_info.count        = 1;

        /* Initialize temporary compact storage info */
        cpt_store.compact.dirty = &cpt_dirty;

        /* Iterate through nodes in chunk skip list */
        chunk_node = H5D_CHUNK_GET_FIRST_NODE(dset_info);
        while (chunk_node) {
            H5D_piece_info_t *chunk_info; /* Chunk information */
            H5D_chunk_ud_t    udata;      /* Chunk index pass-through    */
            htri_t            cacheable;  /* Whether the chunk is cacheable */

            /* Get the actual chunk information from the skip list node */
            chunk_info = H5D_CHUNK_GET_NODE_INFO(dset_info, chunk_node);

            /* Get the info for the chunk in the file */
            if (H5D__chunk_lookup(dset_info->dset, chunk_info->scaled, &udata) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

            /* Sanity check */
            assert((H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length > 0) ||
                   (!H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length == 0));

            /* Check for non-existent chunk & skip it if appropriate */
            if (H5_addr_defined(udata.chunk_block.offset) || UINT_MAX != udata.idx_hint ||
                !skip_missing_chunks) {
                H5D_io_info_t *chk_io_info;  /* Pointer to I/O info object for this chunk */
                void          *chunk = NULL; /* Pointer to locked chunk buffer */

                /* Set chunk's [scaled] coordinates */
                dset_info->store->chunk.scaled = chunk_info->scaled;

                /* Determine if we should use the chunk cache */
                if ((cacheable = H5D__chunk_cacheable(io_info, dset_info, udata.chunk_block.offset, false)) <
                    0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't tell if chunk is cacheable");
                if (cacheable) {
                    /* Load the chunk into cache and lock it. */

                    /* Compute # of bytes accessed in chunk */
                    H5_CHECK_OVERFLOW(dset_info->type_info.src_type_size, /*From:*/ size_t, /*To:*/ uint32_t);
                    H5_CHECK_OVERFLOW(chunk_info->piece_points, /*From:*/ size_t, /*To:*/ uint32_t);
                    src_accessed_bytes =
                        (uint32_t)chunk_info->piece_points * (uint32_t)dset_info->type_info.src_type_size;

                    /* Lock the chunk into the cache */
                    if (NULL == (chunk = H5D__chunk_lock(io_info, dset_info, &udata, false, false)))
                        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "unable to read raw data chunk");

                    /* Set up the storage buffer information for this chunk */
                    cpt_store.compact.buf = chunk;

                    /* Point I/O info at contiguous I/O info for this chunk */
                    chk_io_info = &cpt_io_info;
                } /* end if */
                else if (H5_addr_defined(udata.chunk_block.offset)) {
                    /* Set up the storage address information for this chunk */
                    ctg_store.contig.dset_addr = udata.chunk_block.offset;

                    /* Point I/O info at temporary I/O info for this chunk */
                    chk_io_info = &ctg_io_info;
                } /* end else if */
                else {
                    /* Point I/O info at "nonexistent" I/O info for this chunk */
                    chk_io_info = &nonexistent_io_info;
                } /* end else */

                /* Perform the actual read operation */
                assert(chk_io_info->count == 1);
                chk_io_info->dsets_info[0].layout_io_info.contig_piece_info = chunk_info;
                chk_io_info->dsets_info[0].file_space                       = chunk_info->fspace;
                chk_io_info->dsets_info[0].mem_space                        = chunk_info->mspace;
                chk_io_info->dsets_info[0].nelmts                           = chunk_info->piece_points;
                if ((dset_info->io_ops.single_read)(chk_io_info, &chk_io_info->dsets_info[0]) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "chunked read failed");

                /* Release the cache lock on the chunk. */
                if (chunk &&
                    H5D__chunk_unlock(io_info, dset_info, &udata, false, chunk, src_accessed_bytes) < 0)
                    HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "unable to unlock raw data chunk");
            } /* end if */

            /* Advance to next chunk in list */
            chunk_node = H5D_CHUNK_GET_NEXT_NODE(dset_info, chunk_node);
        } /* end while */
    }     /* end else */

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        if (chunk_mem_spaces != chunk_mem_spaces_local)
            chunk_mem_spaces = H5MM_xfree(chunk_mem_spaces);
        if (chunk_file_spaces != chunk_file_spaces_local)
            chunk_file_spaces = H5MM_xfree(chunk_file_spaces);
        if (chunk_addrs != chunk_addrs_local)
            chunk_addrs = H5MM_xfree(chunk_addrs);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!chunk_mem_spaces || chunk_mem_spaces == chunk_mem_spaces_local);
    assert(!chunk_file_spaces || chunk_file_spaces == chunk_file_spaces_local);
    assert(!chunk_addrs || chunk_addrs == chunk_addrs_local);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_read() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_write
 *
 * Purpose:    Writes to a chunked dataset.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_write(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_info)
{
    H5SL_node_t       *chunk_node;                /* Current node in chunk skip list */
    H5D_io_info_t      ctg_io_info;               /* Contiguous I/O info object */
    H5D_dset_io_info_t ctg_dset_info;             /* Contiguous I/O dset info object */
    H5D_storage_t      ctg_store;                 /* Chunk storage information as contiguous dataset */
    H5D_io_info_t      cpt_io_info;               /* Compact I/O info object */
    H5D_dset_io_info_t cpt_dset_info;             /* Compact I/O dset info object */
    H5D_storage_t      cpt_store;                 /* Chunk storage information as compact dataset */
    bool               cpt_dirty;                 /* Temporary placeholder for compact storage "dirty" flag */
    uint32_t           dst_accessed_bytes = 0;    /* Total accessed size in a chunk */
    H5S_t            **chunk_mem_spaces   = NULL; /* Array of chunk memory spaces */
    H5S_t             *chunk_mem_spaces_local[8]; /* Local buffer for chunk_mem_spaces */
    H5S_t            **chunk_file_spaces = NULL;  /* Array of chunk file spaces */
    H5S_t             *chunk_file_spaces_local[8]; /* Local buffer for chunk_file_spaces */
    haddr_t           *chunk_addrs = NULL;         /* Array of chunk addresses */
    haddr_t            chunk_addrs_local[8];       /* Local buffer for chunk_addrs */
    herr_t             ret_value = SUCCEED;        /* Return value        */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(dset_info);
    assert(dset_info->buf.cvp);

    /* Set up contiguous I/O info object */
    H5MM_memcpy(&ctg_io_info, io_info, sizeof(ctg_io_info));
    H5MM_memcpy(&ctg_dset_info, dset_info, sizeof(ctg_dset_info));
    ctg_dset_info.store      = &ctg_store;
    ctg_dset_info.layout_ops = *H5D_LOPS_CONTIG;
    ctg_io_info.dsets_info   = &ctg_dset_info;
    ctg_io_info.count        = 1;

    /* Initialize temporary contiguous storage info */
    H5_CHECKED_ASSIGN(ctg_store.contig.dset_size, hsize_t, dset_info->dset->shared->layout.u.chunk.size,
                      uint32_t);

    /* Set up compact I/O info object */
    H5MM_memcpy(&cpt_io_info, io_info, sizeof(cpt_io_info));
    H5MM_memcpy(&cpt_dset_info, dset_info, sizeof(cpt_dset_info));
    cpt_dset_info.store      = &cpt_store;
    cpt_dset_info.layout_ops = *H5D_LOPS_COMPACT;
    cpt_io_info.dsets_info   = &cpt_dset_info;
    cpt_io_info.count        = 1;

    /* Initialize temporary compact storage info */
    cpt_store.compact.dirty = &cpt_dirty;

    /* Different blocks depending on whether we're using selection I/O */
    if (io_info->use_select_io == H5D_SELECTION_IO_MODE_ON) {
        size_t      num_chunks       = 0;
        size_t      element_sizes[2] = {dset_info->type_info.dst_type_size, 0};
        const void *bufs[2]          = {dset_info->buf.cvp, NULL};

        /* Only create selection I/O arrays if not performing multi dataset I/O,
         * otherwise the higher level will handle it */
        if (H5D_LAYOUT_CB_PERFORM_IO(io_info)) {
            /* Cache number of chunks */
            num_chunks = H5D_CHUNK_GET_NODE_COUNT(dset_info);

            /* Allocate arrays of dataspaces and offsets for use with selection I/O,
             * or point to local buffers */
            assert(sizeof(chunk_mem_spaces_local) / sizeof(chunk_mem_spaces_local[0]) ==
                   sizeof(chunk_file_spaces_local) / sizeof(chunk_file_spaces_local[0]));
            assert(sizeof(chunk_mem_spaces_local) / sizeof(chunk_mem_spaces_local[0]) ==
                   sizeof(chunk_addrs_local) / sizeof(chunk_addrs_local[0]));
            if (num_chunks > (sizeof(chunk_mem_spaces_local) / sizeof(chunk_mem_spaces_local[0]))) {
                if (NULL == (chunk_mem_spaces = H5MM_malloc(num_chunks * sizeof(H5S_t *))))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                "memory allocation failed for memory space list");
                if (NULL == (chunk_file_spaces = H5MM_malloc(num_chunks * sizeof(H5S_t *))))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                "memory allocation failed for file space list");
                if (NULL == (chunk_addrs = H5MM_malloc(num_chunks * sizeof(haddr_t))))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                "memory allocation failed for chunk address list");
            } /* end if */
            else {
                chunk_mem_spaces  = chunk_mem_spaces_local;
                chunk_file_spaces = chunk_file_spaces_local;
                chunk_addrs       = chunk_addrs_local;
            } /* end else */

            /* Reset num_chunks */
            num_chunks = 0;
        } /* end if */

        /* Iterate through nodes in chunk skip list */
        chunk_node = H5D_CHUNK_GET_FIRST_NODE(dset_info);
        while (chunk_node) {
            H5D_piece_info_t  *chunk_info; /* Chunk information */
            H5D_chk_idx_info_t idx_info;   /* Chunked index info */
            H5D_chunk_ud_t     udata;      /* Index pass-through    */
            htri_t             cacheable;  /* Whether the chunk is cacheable */
            bool need_insert = false;      /* Whether the chunk needs to be inserted into the index */

            /* Get the actual chunk information from the skip list node */
            chunk_info = H5D_CHUNK_GET_NODE_INFO(dset_info, chunk_node);

            /* Get the info for the chunk in the file */
            if (H5D__chunk_lookup(dset_info->dset, chunk_info->scaled, &udata) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

            /* There should be no chunks cached */
            assert(UINT_MAX == udata.idx_hint);

            /* Sanity check */
            assert((H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length > 0) ||
                   (!H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length == 0));

            /* Set chunk's [scaled] coordinates */
            dset_info->store->chunk.scaled = chunk_info->scaled;

            /* Determine if we should use the chunk cache */
            if ((cacheable = H5D__chunk_cacheable(io_info, dset_info, udata.chunk_block.offset, true)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't tell if chunk is cacheable");
            if (cacheable) {
                /* Load the chunk into cache.  But if the whole chunk is written,
                 * simply allocate space instead of load the chunk. */
                void *chunk;               /* Pointer to locked chunk buffer */
                bool  entire_chunk = true; /* Whether whole chunk is selected */

                /* Compute # of bytes accessed in chunk */
                H5_CHECK_OVERFLOW(dset_info->type_info.dst_type_size, /*From:*/ size_t, /*To:*/ uint32_t);
                H5_CHECK_OVERFLOW(chunk_info->piece_points, /*From:*/ size_t, /*To:*/ uint32_t);
                dst_accessed_bytes =
                    (uint32_t)chunk_info->piece_points * (uint32_t)dset_info->type_info.dst_type_size;

                /* Determine if we will access all the data in the chunk */
                if (dst_accessed_bytes != ctg_store.contig.dset_size ||
                    (chunk_info->piece_points * dset_info->type_info.src_type_size) !=
                        ctg_store.contig.dset_size ||
                    dset_info->layout_io_info.chunk_map->fsel_type == H5S_SEL_POINTS)
                    entire_chunk = false;

                /* Lock the chunk into the cache */
                if (NULL == (chunk = H5D__chunk_lock(io_info, dset_info, &udata, entire_chunk, false)))
                    HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "unable to read raw data chunk");

                /* Set up the storage buffer information for this chunk */
                cpt_store.compact.buf = chunk;

                /* Set up compact dataset info for write to cached chunk */
                cpt_dset_info.layout_io_info.contig_piece_info = chunk_info;
                cpt_dset_info.file_space                       = chunk_info->fspace;
                cpt_dset_info.mem_space                        = chunk_info->mspace;
                cpt_dset_info.nelmts                           = chunk_info->piece_points;

                /* Set request_nelmts.  This is not normally set by the upper layers because selection I/O
                 * usually does not use strip mining (H5D__scatgath_write), and instead allocates buffers
                 * large enough for the entire I/O.  Set request_nelmts to be large enough for all selected
                 * elements in this chunk because it must be at least that large */
                cpt_dset_info.type_info.request_nelmts = cpt_dset_info.nelmts;

                /* Perform the actual write operation */
                if ((dset_info->io_ops.single_write)(&cpt_io_info, &cpt_dset_info) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "chunked write failed");

                /* Release the cache lock on the chunk */
                if (H5D__chunk_unlock(io_info, dset_info, &udata, true, chunk, dst_accessed_bytes) < 0)
                    HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "unable to unlock raw data chunk");
            } /* end if */
            else {
                /* If the chunk hasn't been allocated on disk, do so now. */
                if (!H5_addr_defined(udata.chunk_block.offset)) {
                    /* Compose chunked index info struct */
                    idx_info.f       = dset_info->dset->oloc.file;
                    idx_info.pline   = &(dset_info->dset->shared->dcpl_cache.pline);
                    idx_info.layout  = &(dset_info->dset->shared->layout.u.chunk);
                    idx_info.storage = &(dset_info->dset->shared->layout.storage.u.chunk);

                    /* Set up the size of chunk for user data */
                    udata.chunk_block.length = dset_info->dset->shared->layout.u.chunk.size;

                    /* Allocate the chunk */
                    if (H5D__chunk_file_alloc(&idx_info, NULL, &udata.chunk_block, &need_insert,
                                              chunk_info->scaled) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL,
                                    "unable to insert/resize chunk on chunk level");

                    /* Make sure the address of the chunk is returned. */
                    if (!H5_addr_defined(udata.chunk_block.offset))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "chunk address isn't defined");

                    /* Cache the new chunk information */
                    H5D__chunk_cinfo_cache_update(&dset_info->dset->shared->cache.chunk.last, &udata);

                    /* Insert chunk into index */
                    if (need_insert && dset_info->dset->shared->layout.storage.u.chunk.ops->insert)
                        if ((dset_info->dset->shared->layout.storage.u.chunk.ops->insert)(&idx_info, &udata,
                                                                                          NULL) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL,
                                        "unable to insert chunk addr into index");
                } /* end if */

                /* Add chunk to list for selection I/O, if not performing multi dataset I/O */
                if (H5D_LAYOUT_CB_PERFORM_IO(io_info)) {
                    chunk_mem_spaces[num_chunks]  = chunk_info->mspace;
                    chunk_file_spaces[num_chunks] = chunk_info->fspace;
                    chunk_addrs[num_chunks]       = udata.chunk_block.offset;
                    num_chunks++;
                } /* end if */
                else {
                    /* Add to mdset selection I/O arrays */
                    assert(io_info->mem_spaces);
                    assert(io_info->file_spaces);
                    assert(io_info->addrs);
                    assert(io_info->element_sizes);
                    assert(io_info->wbufs);
                    assert(io_info->pieces_added < io_info->piece_count);

                    io_info->mem_spaces[io_info->pieces_added]    = chunk_info->mspace;
                    io_info->file_spaces[io_info->pieces_added]   = chunk_info->fspace;
                    io_info->addrs[io_info->pieces_added]         = udata.chunk_block.offset;
                    io_info->element_sizes[io_info->pieces_added] = element_sizes[0];
                    io_info->wbufs[io_info->pieces_added]         = bufs[0];
                    if (io_info->sel_pieces)
                        io_info->sel_pieces[io_info->pieces_added] = chunk_info;
                    io_info->pieces_added++;

                    if (io_info->sel_pieces && chunk_info->filtered_dset)
                        io_info->filtered_pieces_added++;
                }
            } /* end else */

            /* Advance to next chunk in list */
            chunk_node = H5D_CHUNK_GET_NEXT_NODE(dset_info, chunk_node);
        } /* end while */

        /* Only perform I/O if not performing multi dataset I/O or type conversion, otherwise the
         * higher level will handle it after all datasets have been processed */
        if (H5D_LAYOUT_CB_PERFORM_IO(io_info)) {
            /* Issue selection I/O call (we can skip the page buffer because we've
             * already verified it won't be used, and the metadata accumulator
             * because this is raw data) */
            H5_CHECK_OVERFLOW(num_chunks, size_t, uint32_t);
            if (H5F_shared_select_write(H5F_SHARED(dset_info->dset->oloc.file), H5FD_MEM_DRAW,
                                        (uint32_t)num_chunks, chunk_mem_spaces, chunk_file_spaces,
                                        chunk_addrs, element_sizes, bufs) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "chunk selection write failed");

            /* Clean up memory */
            if (chunk_mem_spaces != chunk_mem_spaces_local) {
                assert(chunk_file_spaces != chunk_file_spaces_local);
                assert(chunk_addrs != chunk_addrs_local);
                chunk_mem_spaces  = H5MM_xfree(chunk_mem_spaces);
                chunk_file_spaces = H5MM_xfree(chunk_file_spaces);
                chunk_addrs       = H5MM_xfree(chunk_addrs);
            } /* end if */
        }     /* end if */

#ifdef H5_HAVE_PARALLEL
        /* Report that collective chunk I/O was used (will only be set on the DXPL if collective I/O was
         * requested) */
        io_info->actual_io_mode |= H5D_MPIO_CHUNK_COLLECTIVE;
#endif /* H5_HAVE_PARALLEL */
    }  /* end if */
    else {
        /* Iterate through nodes in chunk skip list */
        chunk_node = H5D_CHUNK_GET_FIRST_NODE(dset_info);
        while (chunk_node) {
            H5D_piece_info_t  *chunk_info;  /* Chunk information */
            H5D_chk_idx_info_t idx_info;    /* Chunked index info */
            H5D_io_info_t     *chk_io_info; /* Pointer to I/O info object for this chunk */
            void              *chunk;       /* Pointer to locked chunk buffer */
            H5D_chunk_ud_t     udata;       /* Index pass-through    */
            htri_t             cacheable;   /* Whether the chunk is cacheable */
            bool need_insert = false;       /* Whether the chunk needs to be inserted into the index */

            /* Get the actual chunk information from the skip list node */
            chunk_info = H5D_CHUNK_GET_NODE_INFO(dset_info, chunk_node);

            /* Look up the chunk */
            if (H5D__chunk_lookup(dset_info->dset, chunk_info->scaled, &udata) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

            /* Sanity check */
            assert((H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length > 0) ||
                   (!H5_addr_defined(udata.chunk_block.offset) && udata.chunk_block.length == 0));

            /* Set chunk's [scaled] coordinates */
            dset_info->store->chunk.scaled = chunk_info->scaled;

            /* Determine if we should use the chunk cache */
            if ((cacheable = H5D__chunk_cacheable(io_info, dset_info, udata.chunk_block.offset, true)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't tell if chunk is cacheable");
            if (cacheable) {
                /* Load the chunk into cache.  But if the whole chunk is written,
                 * simply allocate space instead of load the chunk. */
                bool entire_chunk = true; /* Whether whole chunk is selected */

                /* Compute # of bytes accessed in chunk */
                H5_CHECK_OVERFLOW(dset_info->type_info.dst_type_size, /*From:*/ size_t, /*To:*/ uint32_t);
                H5_CHECK_OVERFLOW(chunk_info->piece_points, /*From:*/ size_t, /*To:*/ uint32_t);
                dst_accessed_bytes =
                    (uint32_t)chunk_info->piece_points * (uint32_t)dset_info->type_info.dst_type_size;

                /* Determine if we will access all the data in the chunk */
                if (dst_accessed_bytes != ctg_store.contig.dset_size ||
                    (chunk_info->piece_points * dset_info->type_info.src_type_size) !=
                        ctg_store.contig.dset_size ||
                    dset_info->layout_io_info.chunk_map->fsel_type == H5S_SEL_POINTS)
                    entire_chunk = false;

                /* Lock the chunk into the cache */
                if (NULL == (chunk = H5D__chunk_lock(io_info, dset_info, &udata, entire_chunk, false)))
                    HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "unable to read raw data chunk");

                /* Set up the storage buffer information for this chunk */
                cpt_store.compact.buf = chunk;

                /* Point I/O info at main I/O info for this chunk */
                chk_io_info = &cpt_io_info;
            } /* end if */
            else {
                /* If the chunk hasn't been allocated on disk, do so now. */
                if (!H5_addr_defined(udata.chunk_block.offset)) {
                    /* Compose chunked index info struct */
                    idx_info.f       = dset_info->dset->oloc.file;
                    idx_info.pline   = &(dset_info->dset->shared->dcpl_cache.pline);
                    idx_info.layout  = &(dset_info->dset->shared->layout.u.chunk);
                    idx_info.storage = &(dset_info->dset->shared->layout.storage.u.chunk);

                    /* Set up the size of chunk for user data */
                    udata.chunk_block.length = dset_info->dset->shared->layout.u.chunk.size;

                    /* Allocate the chunk */
                    if (H5D__chunk_file_alloc(&idx_info, NULL, &udata.chunk_block, &need_insert,
                                              chunk_info->scaled) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL,
                                    "unable to insert/resize chunk on chunk level");

                    /* Make sure the address of the chunk is returned. */
                    if (!H5_addr_defined(udata.chunk_block.offset))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "chunk address isn't defined");

                    /* Cache the new chunk information */
                    H5D__chunk_cinfo_cache_update(&dset_info->dset->shared->cache.chunk.last, &udata);
                } /* end if */

                /* Set up the storage address information for this chunk */
                ctg_store.contig.dset_addr = udata.chunk_block.offset;

                /* No chunk cached */
                chunk = NULL;

                /* Point I/O info at temporary I/O info for this chunk */
                chk_io_info = &ctg_io_info;
            } /* end else */

            /* Perform the actual write operation */
            assert(chk_io_info->count == 1);
            chk_io_info->dsets_info[0].layout_io_info.contig_piece_info = chunk_info;
            chk_io_info->dsets_info[0].file_space                       = chunk_info->fspace;
            chk_io_info->dsets_info[0].mem_space                        = chunk_info->mspace;
            chk_io_info->dsets_info[0].nelmts                           = chunk_info->piece_points;
            if ((dset_info->io_ops.single_write)(chk_io_info, &chk_io_info->dsets_info[0]) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "chunked write failed");

            /* Release the cache lock on the chunk, or insert chunk into index. */
            if (chunk) {
                if (H5D__chunk_unlock(io_info, dset_info, &udata, true, chunk, dst_accessed_bytes) < 0)
                    HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "unable to unlock raw data chunk");
            } /* end if */
            else {
                if (need_insert && dset_info->dset->shared->layout.storage.u.chunk.ops->insert)
                    if ((dset_info->dset->shared->layout.storage.u.chunk.ops->insert)(&idx_info, &udata,
                                                                                      NULL) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL,
                                    "unable to insert chunk addr into index");
            } /* end else */

            /* Advance to next chunk in list */
            chunk_node = H5D_CHUNK_GET_NEXT_NODE(dset_info, chunk_node);
        } /* end while */
    }     /* end else */

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        if (chunk_mem_spaces != chunk_mem_spaces_local)
            chunk_mem_spaces = H5MM_xfree(chunk_mem_spaces);
        if (chunk_file_spaces != chunk_file_spaces_local)
            chunk_file_spaces = H5MM_xfree(chunk_file_spaces);
        if (chunk_addrs != chunk_addrs_local)
            chunk_addrs = H5MM_xfree(chunk_addrs);
    } /* end if */

    /* Make sure we cleaned up */
    assert(!chunk_mem_spaces || chunk_mem_spaces == chunk_mem_spaces_local);
    assert(!chunk_file_spaces || chunk_file_spaces == chunk_file_spaces_local);
    assert(!chunk_addrs || chunk_addrs == chunk_addrs_local);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_write() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_flush
 *
 * Purpose:    Writes all dirty chunks to disk and optionally preempts them
 *        from the cache.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_flush(H5D_t *dset)
{
    H5D_rdcc_t     *rdcc = &(dset->shared->cache.chunk);
    H5D_rdcc_ent_t *ent, *next;
    unsigned        nerrors   = 0;       /* Count of any errors encountered when flushing chunks */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);

    /* Loop over all entries in the chunk cache */
    for (ent = rdcc->head; ent; ent = next) {
        next = ent->next;
        if (H5D__chunk_flush_entry(dset, ent, false) < 0)
            nerrors++;
    } /* end for */
    if (nerrors)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to flush one or more raw data chunks");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_io_term
 *
 * Purpose:    Destroy I/O operation information.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_io_term(H5D_io_info_t H5_ATTR_UNUSED *io_info, H5D_dset_io_info_t *di)
{
    H5D_chunk_map_t *fm;                  /* Convenience pointer to chunk map */
    herr_t           ret_value = SUCCEED; /*return value        */

    FUNC_ENTER_PACKAGE

    assert(di);

    /* Set convenience pointer */
    fm = di->layout_io_info.chunk_map;

    /* Single element I/O vs. multiple element I/O cleanup */
    if (fm->use_single) {
        /* Sanity checks */
        assert(fm->dset_sel_pieces == NULL);
        assert(fm->last_piece_info == NULL);
        assert(fm->single_piece_info);
        assert(fm->single_piece_info->fspace_shared);
        assert(fm->single_piece_info->mspace_shared);

        /* Reset the selection for the single element I/O */
        H5S_select_all(fm->single_space, true);
    } /* end if */
    else {
        /* Release the nodes on the list of selected pieces, or the last (only)
         * piece if the skiplist is not available */
        if (fm->dset_sel_pieces) {
            if (H5SL_free(fm->dset_sel_pieces, H5D__free_piece_info, NULL) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTNEXT, FAIL, "can't free dataset skip list");
        } /* end if */
        else if (fm->last_piece_info) {
            if (H5D__free_piece_info(fm->last_piece_info, NULL, NULL) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't free piece info");
            fm->last_piece_info = NULL;
        } /* end if */
    }     /* end else */

    /* Free the memory piece dataspace template */
    if (fm->mchunk_tmpl)
        if (H5S_close(fm->mchunk_tmpl) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL,
                        "can't release memory chunk dataspace template");

    /* Free chunk map */
    di->layout_io_info.chunk_map = H5FL_FREE(H5D_chunk_map_t, di->layout_io_info.chunk_map);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_io_term() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_dest
 *
 * Purpose:    Destroy the entire chunk cache by flushing dirty entries,
 *        preempting all entries, and freeing the cache itself.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_dest(H5D_t *dset)
{
    H5D_chk_idx_info_t   idx_info;                            /* Chunked index info */
    H5D_rdcc_t          *rdcc = &(dset->shared->cache.chunk); /* Dataset's chunk cache */
    H5D_rdcc_ent_t      *ent = NULL, *next = NULL;            /* Pointer to current & next cache entries */
    int                  nerrors   = 0;                       /* Accumulated count of errors */
    H5O_storage_chunk_t *sc        = &(dset->shared->layout.storage.u.chunk);
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    /* Sanity checks */
    assert(dset);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);

    /* Flush all the cached chunks */
    for (ent = rdcc->head; ent; ent = next) {
        next = ent->next;
        if (H5D__chunk_cache_evict(dset, ent, true) < 0)
            nerrors++;
    } /* end for */

    /* Continue even if there are failures. */
    if (nerrors)
        HDONE_ERROR(H5E_IO, H5E_CANTFLUSH, FAIL, "unable to flush one or more raw data chunks");

    /* Release cache structures */
    if (rdcc->slot)
        rdcc->slot = H5FL_SEQ_FREE(H5D_rdcc_ent_ptr_t, rdcc->slot);
    memset(rdcc, 0, sizeof(H5D_rdcc_t));

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = sc;

    /* Free any index structures */
    if (sc->ops->dest && (sc->ops->dest)(&idx_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to release chunk index info");

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__chunk_dest() */

/*-------------------------------------------------------------------------
 * Function:    H5D_chunk_idx_reset
 *
 * Purpose:    Reset index information
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D_chunk_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(storage);
    assert(storage->ops);
    H5D_CHUNK_STORAGE_INDEX_CHK(storage);

    /* Reset index structures */
    if ((storage->ops->reset)(storage, reset_addr) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to reset chunk index info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_chunk_idx_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_cinfo_cache_reset
 *
 * Purpose:    Reset the cached chunk info
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_cinfo_cache_reset(H5D_chunk_cached_t *last)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(last);

    /* Indicate that the cached info is not valid */
    last->valid = false;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__chunk_cinfo_cache_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_cinfo_cache_update
 *
 * Purpose:    Update the cached chunk info
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_cinfo_cache_update(H5D_chunk_cached_t *last, const H5D_chunk_ud_t *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(last);
    assert(udata);
    assert(udata->common.layout);
    assert(udata->common.scaled);

    /* Stored the information to cache */
    H5MM_memcpy(last->scaled, udata->common.scaled, sizeof(hsize_t) * udata->common.layout->ndims);
    last->addr = udata->chunk_block.offset;
    H5_CHECKED_ASSIGN(last->nbytes, uint32_t, udata->chunk_block.length, hsize_t);
    last->chunk_idx   = udata->chunk_idx;
    last->filter_mask = udata->filter_mask;

    /* Indicate that the cached info is valid */
    last->valid = true;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__chunk_cinfo_cache_update() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_cinfo_cache_found
 *
 * Purpose:    Look for chunk info in cache
 *
 * Return:    true/false/FAIL
 *
 *-------------------------------------------------------------------------
 */
static bool
H5D__chunk_cinfo_cache_found(const H5D_chunk_cached_t *last, H5D_chunk_ud_t *udata)
{
    bool ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(last);
    assert(udata);
    assert(udata->common.layout);
    assert(udata->common.scaled);

    /* Check if the cached information is what is desired */
    if (last->valid) {
        unsigned u; /* Local index variable */

        /* Check that the scaled offset is the same */
        for (u = 0; u < udata->common.layout->ndims; u++)
            if (last->scaled[u] != udata->common.scaled[u])
                HGOTO_DONE(false);

        /* Retrieve the information from the cache */
        udata->chunk_block.offset = last->addr;
        udata->chunk_block.length = last->nbytes;
        udata->chunk_idx          = last->chunk_idx;
        udata->filter_mask        = last->filter_mask;

        /* Indicate that the data was found */
        HGOTO_DONE(true);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_cinfo_cache_found() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_create
 *
 * Purpose:    Creates a new chunked storage index and initializes the
 *        layout information with information about the storage.  The
 *        layout info should be immediately written to the object header.
 *
 * Return:    Non-negative on success (with the layout information initialized
 *        and ready to write to an object header). Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_create(const H5D_t *dset /*in,out*/)
{
    H5D_chk_idx_info_t   idx_info; /* Chunked index info */
    H5O_storage_chunk_t *sc        = &(dset->shared->layout.storage.u.chunk);
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dset);
    assert(H5D_CHUNKED == dset->shared->layout.type);
    assert(dset->shared->layout.u.chunk.ndims > 0 && dset->shared->layout.u.chunk.ndims <= H5O_LAYOUT_NDIMS);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);

#ifndef NDEBUG
    {
        unsigned u; /* Local index variable */

        for (u = 0; u < dset->shared->layout.u.chunk.ndims; u++)
            assert(dset->shared->layout.u.chunk.dim[u] > 0);
    }
#endif

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = sc;

    /* Create the index for the chunks */
    if ((sc->ops->create)(&idx_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create chunk index");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_create() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_hash_val
 *
 * Purpose:     To calculate an index based on the dataset's scaled
 *              coordinates and sizes of the faster dimensions.
 *
 * Return:    Hash value index
 *
 *-------------------------------------------------------------------------
 */
static unsigned
H5D__chunk_hash_val(const H5D_shared_t *shared, const hsize_t *scaled)
{
    hsize_t  val;                   /* Intermediate value */
    unsigned ndims = shared->ndims; /* Rank of dataset */
    unsigned ret   = 0;             /* Value to return */
    unsigned u;                     /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(shared);
    assert(scaled);

    /* If the fastest changing dimension doesn't have enough entropy, use
     *  other dimensions too
     */
    val = scaled[0];
    for (u = 1; u < ndims; u++) {
        val <<= shared->cache.chunk.scaled_encode_bits[u];
        val ^= scaled[u];
    } /* end for */

    /* Modulo value against the number of array slots */
    ret = (unsigned)(val % shared->cache.chunk.nslots);

    FUNC_LEAVE_NOAPI(ret)
} /* H5D__chunk_hash_val() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_lookup
 *
 * Purpose:    Loops up a chunk in cache and on disk, and retrieves
 *              information about that chunk.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_lookup(const H5D_t *dset, const hsize_t *scaled, H5D_chunk_ud_t *udata)
{
    H5D_rdcc_ent_t      *ent   = NULL; /* Cache entry */
    H5O_storage_chunk_t *sc    = &(dset->shared->layout.storage.u.chunk);
    unsigned             idx   = 0;     /* Index of chunk in cache, if present */
    bool                 found = false; /* In cache? */
#ifdef H5_HAVE_PARALLEL
    H5P_coll_md_read_flag_t md_reads_file_flag;
    bool                    md_reads_context_flag;
    bool                    restore_md_reads_state = false;
#endif
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dset);
    assert(dset->shared->layout.u.chunk.ndims > 0);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);
    assert(scaled);
    assert(udata);

    /* Initialize the query information about the chunk we are looking for */
    udata->common.layout  = &(dset->shared->layout.u.chunk);
    udata->common.storage = sc;
    udata->common.scaled  = scaled;

    /* Reset information about the chunk we are looking for */
    udata->chunk_block.offset = HADDR_UNDEF;
    udata->chunk_block.length = 0;
    udata->filter_mask        = 0;
    udata->new_unfilt_chunk   = false;

    /* Check for chunk in cache */
    if (dset->shared->cache.chunk.nslots > 0) {
        /* Determine the chunk's location in the hash table */
        idx = H5D__chunk_hash_val(dset->shared, scaled);

        /* Get the chunk cache entry for that location */
        ent = dset->shared->cache.chunk.slot[idx];
        if (ent) {
            unsigned u; /* Counter */

            /* Speculatively set the 'found' flag */
            found = true;

            /* Verify that the cache entry is the correct chunk */
            for (u = 0; u < dset->shared->ndims; u++)
                if (scaled[u] != ent->scaled[u]) {
                    found = false;
                    break;
                } /* end if */
        }         /* end if */
    }             /* end if */

    /* Retrieve chunk addr */
    if (found) {
        udata->idx_hint           = idx;
        udata->chunk_block.offset = ent->chunk_block.offset;
        udata->chunk_block.length = ent->chunk_block.length;
        udata->chunk_idx          = ent->chunk_idx;
    } /* end if */
    else {
        /* Invalidate idx_hint, to signal that the chunk is not in cache */
        udata->idx_hint = UINT_MAX;

        /* Check for cached information */
        if (!H5D__chunk_cinfo_cache_found(&dset->shared->cache.chunk.last, udata)) {
            H5D_chk_idx_info_t idx_info; /* Chunked index info */

            /* Compose chunked index info struct */
            idx_info.f       = dset->oloc.file;
            idx_info.pline   = &dset->shared->dcpl_cache.pline;
            idx_info.layout  = &dset->shared->layout.u.chunk;
            idx_info.storage = sc;

#ifdef H5_HAVE_PARALLEL
            if (H5F_HAS_FEATURE(idx_info.f, H5FD_FEAT_HAS_MPI)) {
                /* Disable collective metadata read for chunk indexes as it is
                 * highly unlikely that users would read the same chunks from all
                 * processes.
                 */
                if (H5F_get_coll_metadata_reads(idx_info.f)) {
#ifndef NDEBUG
                    bool index_is_open;

                    /*
                     * The dataset's chunk index should be open at this point.
                     * Otherwise, we will end up reading it in independently,
                     * which may not be desired.
                     */
                    sc->ops->is_open(&idx_info, &index_is_open);
                    assert(index_is_open);
#endif

                    md_reads_file_flag    = H5P_FORCE_FALSE;
                    md_reads_context_flag = false;
                    H5F_set_coll_metadata_reads(idx_info.f, &md_reads_file_flag, &md_reads_context_flag);
                    restore_md_reads_state = true;
                }
            }
#endif /* H5_HAVE_PARALLEL */

            /* Go get the chunk information */
            if ((sc->ops->get_addr)(&idx_info, udata) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't query chunk address");

                /*
                 * Cache the information retrieved.
                 *
                 * Note that if we are writing to the dataset in parallel and filters
                 * are involved, we skip caching this information as it is highly likely
                 * that the chunk information will be invalidated as a result of the
                 * filter operation (e.g. the chunk gets re-allocated to a different
                 * address in the file and/or gets re-allocated with a different size).
                 * If we were to cache this information, subsequent reads/writes would
                 * retrieve the invalid information and cause a variety of issues.
                 *
                 * It has been verified that in the serial library, when writing to chunks
                 * with the real chunk cache disabled and with filters involved, the
                 * functions within this file are correctly called in such a manner that
                 * this single chunk cache is always updated correctly. Therefore, this
                 * check is not needed for the serial library.
                 *
                 * This is an ugly and potentially frail check, but the
                 * H5D__chunk_cinfo_cache_reset() function is not currently available
                 * to functions outside of this file, so outside functions can not
                 * invalidate this single chunk cache. Even if the function were available,
                 * this check prevents us from doing the work of going through and caching
                 * each chunk in the write operation, when we're only going to invalidate
                 * the cache at the end of a parallel write anyway.
                 *
                 *  - JTH (7/13/2018)
                 */
#ifdef H5_HAVE_PARALLEL
            if (!((H5F_HAS_FEATURE(idx_info.f, H5FD_FEAT_HAS_MPI)) &&
                  (H5F_INTENT(dset->oloc.file) & H5F_ACC_RDWR) && dset->shared->dcpl_cache.pline.nused))
#endif
                H5D__chunk_cinfo_cache_update(&dset->shared->cache.chunk.last, udata);
        } /* end if */
    }     /* end else */

done:
#ifdef H5_HAVE_PARALLEL
    /* Re-enable collective metadata reads if we disabled them */
    if (restore_md_reads_state)
        H5F_set_coll_metadata_reads(dset->oloc.file, &md_reads_file_flag, &md_reads_context_flag);
#endif /* H5_HAVE_PARALLEL */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_lookup() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_flush_entry
 *
 * Purpose:    Writes a chunk to disk.  If RESET is non-zero then the
 *        entry is cleared -- it's slightly faster to flush a chunk if
 *        the RESET flag is turned on because it results in one fewer
 *        memory copy.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_flush_entry(const H5D_t *dset, H5D_rdcc_ent_t *ent, bool reset)
{
    void                *buf                = NULL; /* Temporary buffer        */
    bool                 point_of_no_return = false;
    H5O_storage_chunk_t *sc                 = &(dset->shared->layout.storage.u.chunk);
    herr_t               ret_value          = SUCCEED; /* Return value            */

    FUNC_ENTER_PACKAGE

    assert(dset);
    assert(dset->shared);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);
    assert(ent);
    assert(!ent->locked);

    buf = ent->chunk;
    if (ent->dirty) {
        H5D_chk_idx_info_t idx_info;            /* Chunked index info */
        H5D_chunk_ud_t     udata;               /* pass through B-tree        */
        bool               must_alloc  = false; /* Whether the chunk must be allocated */
        bool               need_insert = false; /* Whether the chunk needs to be inserted into the index */

        /* Set up user data for index callbacks */
        udata.common.layout      = &dset->shared->layout.u.chunk;
        udata.common.storage     = sc;
        udata.common.scaled      = ent->scaled;
        udata.chunk_block.offset = ent->chunk_block.offset;
        udata.chunk_block.length = dset->shared->layout.u.chunk.size;
        udata.filter_mask        = 0;
        udata.chunk_idx          = ent->chunk_idx;

        /* Should the chunk be filtered before writing it to disk? */
        if (dset->shared->dcpl_cache.pline.nused && !(ent->edge_chunk_state & H5D_RDCC_DISABLE_FILTERS)) {
            H5Z_EDC_t err_detect;                       /* Error detection info */
            H5Z_cb_t  filter_cb;                        /* I/O filter callback function */
            size_t    alloc = udata.chunk_block.length; /* Bytes allocated for BUF    */
            size_t    nbytes;                           /* Chunk size (in bytes) */

            /* Retrieve filter settings from API context */
            if (H5CX_get_err_detect(&err_detect) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get error detection info");
            if (H5CX_get_filter_cb(&filter_cb) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get I/O filter callback function");

            if (!reset) {
                /*
                 * Copy the chunk to a new buffer before running it through
                 * the pipeline because we'll want to save the original buffer
                 * for later.
                 */
                if (NULL == (buf = H5MM_malloc(alloc)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for pipeline");
                H5MM_memcpy(buf, ent->chunk, alloc);
            } /* end if */
            else {
                /*
                 * If we are resetting and something goes wrong after this
                 * point then it's too late to recover because we may have
                 * destroyed the original data by calling H5Z_pipeline().
                 * The only safe option is to continue with the reset
                 * even if we can't write the data to disk.
                 */
                point_of_no_return = true;
                ent->chunk         = NULL;
            } /* end else */
            H5_CHECKED_ASSIGN(nbytes, size_t, udata.chunk_block.length, hsize_t);
            if (H5Z_pipeline(&(dset->shared->dcpl_cache.pline), 0, &(udata.filter_mask), err_detect,
                             filter_cb, &nbytes, &alloc, &buf) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFILTER, FAIL, "output pipeline failed");
#if H5_SIZEOF_SIZE_T > 4
            /* Check for the chunk expanding too much to encode in a 32-bit value */
            if (nbytes > ((size_t)0xffffffff))
                HGOTO_ERROR(H5E_DATASET, H5E_BADRANGE, FAIL, "chunk too large for 32-bit length");
#endif /* H5_SIZEOF_SIZE_T > 4 */
            H5_CHECKED_ASSIGN(udata.chunk_block.length, hsize_t, nbytes, size_t);

            /* Indicate that the chunk must be allocated */
            must_alloc = true;
        } /* end if */
        else if (!H5_addr_defined(udata.chunk_block.offset)) {
            /* Indicate that the chunk must be allocated */
            must_alloc = true;

            /* This flag could be set for this chunk, just remove and ignore it
             */
            ent->edge_chunk_state &= ~H5D_RDCC_NEWLY_DISABLED_FILTERS;
        } /* end else */
        else if (ent->edge_chunk_state & H5D_RDCC_NEWLY_DISABLED_FILTERS) {
            /* Chunk on disk is still filtered, must insert to allocate correct
             * size */
            must_alloc = true;

            /* Set the disable filters field back to the standard disable
             * filters setting, as it no longer needs to be inserted with every
             * flush */
            ent->edge_chunk_state &= ~H5D_RDCC_NEWLY_DISABLED_FILTERS;
        } /* end else */

        assert(!(ent->edge_chunk_state & H5D_RDCC_NEWLY_DISABLED_FILTERS));

        /* Check if the chunk needs to be allocated (it also could exist already
         *      and the chunk alloc operation could resize it)
         */
        if (must_alloc) {
            /* Compose chunked index info struct */
            idx_info.f       = dset->oloc.file;
            idx_info.pline   = &dset->shared->dcpl_cache.pline;
            idx_info.layout  = &dset->shared->layout.u.chunk;
            idx_info.storage = sc;

            /* Create the chunk it if it doesn't exist, or reallocate the chunk
             *  if its size changed.
             */
            if (H5D__chunk_file_alloc(&idx_info, &(ent->chunk_block), &udata.chunk_block, &need_insert,
                                      ent->scaled) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL,
                            "unable to insert/resize chunk on chunk level");

            /* Update the chunk entry's info, in case it was allocated or relocated */
            ent->chunk_block.offset = udata.chunk_block.offset;
            ent->chunk_block.length = udata.chunk_block.length;
        } /* end if */

        /* Write the data to the file */
        assert(H5_addr_defined(udata.chunk_block.offset));
        H5_CHECK_OVERFLOW(udata.chunk_block.length, hsize_t, size_t);
        if (H5F_shared_block_write(H5F_SHARED(dset->oloc.file), H5FD_MEM_DRAW, udata.chunk_block.offset,
                                   (size_t)udata.chunk_block.length, buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to write raw data to file");

        /* Insert the chunk record into the index */
        if (need_insert && sc->ops->insert)
            if ((sc->ops->insert)(&idx_info, &udata, dset) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "unable to insert chunk addr into index");

        /* Cache the chunk's info, in case it's accessed again shortly */
        H5D__chunk_cinfo_cache_update(&dset->shared->cache.chunk.last, &udata);

        /* Mark cache entry as clean */
        ent->dirty = false;

        /* Increment # of flushed entries */
        dset->shared->cache.chunk.stats.nflushes++;
    } /* end if */

    /* Reset, but do not free or removed from list */
    if (reset) {
        point_of_no_return = false;
        if (buf == ent->chunk)
            buf = NULL;
        if (ent->chunk != NULL)
            ent->chunk = (uint8_t *)H5D__chunk_mem_xfree(ent->chunk,
                                                         ((ent->edge_chunk_state & H5D_RDCC_DISABLE_FILTERS)
                                                              ? NULL
                                                              : &(dset->shared->dcpl_cache.pline)));
    } /* end if */

done:
    /* Free the temp buffer only if it's different than the entry chunk */
    if (buf != ent->chunk)
        H5MM_xfree(buf);

    /*
     * If we reached the point of no return then we have no choice but to
     * reset the entry.  This can only happen if RESET is true but the
     * output pipeline failed.  Do not free the entry or remove it from the
     * list.
     */
    if (ret_value < 0 && point_of_no_return)
        if (ent->chunk)
            ent->chunk = (uint8_t *)H5D__chunk_mem_xfree(ent->chunk,
                                                         ((ent->edge_chunk_state & H5D_RDCC_DISABLE_FILTERS)
                                                              ? NULL
                                                              : &(dset->shared->dcpl_cache.pline)));

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_flush_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_cache_evict
 *
 * Purpose:     Preempts the specified entry from the cache, flushing it to
 *              disk if necessary.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_cache_evict(const H5D_t *dset, H5D_rdcc_ent_t *ent, bool flush)
{
    H5D_rdcc_t *rdcc      = &(dset->shared->cache.chunk);
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dset);
    assert(ent);
    assert(!ent->locked);
    assert(ent->idx < rdcc->nslots);

    if (flush) {
        /* Flush */
        if (H5D__chunk_flush_entry(dset, ent, true) < 0)
            HDONE_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "cannot flush indexed storage buffer");
    } /* end if */
    else {
        /* Don't flush, just free chunk */
        if (ent->chunk != NULL)
            ent->chunk = (uint8_t *)H5D__chunk_mem_xfree(ent->chunk,
                                                         ((ent->edge_chunk_state & H5D_RDCC_DISABLE_FILTERS)
                                                              ? NULL
                                                              : &(dset->shared->dcpl_cache.pline)));
    } /* end else */

    /* Unlink from list */
    if (ent->prev)
        ent->prev->next = ent->next;
    else
        rdcc->head = ent->next;
    if (ent->next)
        ent->next->prev = ent->prev;
    else
        rdcc->tail = ent->prev;
    ent->prev = ent->next = NULL;

    /* Unlink from temporary list */
    if (ent->tmp_prev) {
        assert(rdcc->tmp_head->tmp_next);
        ent->tmp_prev->tmp_next = ent->tmp_next;
        if (ent->tmp_next) {
            ent->tmp_next->tmp_prev = ent->tmp_prev;
            ent->tmp_next           = NULL;
        } /* end if */
        ent->tmp_prev = NULL;
    } /* end if */
    else
        /* Only clear hash table slot if the chunk was not on the temporary list
         */
        rdcc->slot[ent->idx] = NULL;

    /* Remove from cache */
    assert(rdcc->slot[ent->idx] != ent);
    ent->idx = UINT_MAX;
    rdcc->nbytes_used -= dset->shared->layout.u.chunk.size;
    --rdcc->nused;

    /* Free */
    ent = H5FL_FREE(H5D_rdcc_ent_t, ent);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_cache_evict() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_cache_prune
 *
 * Purpose:    Prune the cache by preempting some things until the cache has
 *        room for something which is SIZE bytes.  Only unlocked
 *        entries are considered for preemption.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_cache_prune(const H5D_t *dset, size_t size)
{
    const H5D_rdcc_t *rdcc  = &(dset->shared->cache.chunk);
    size_t            total = rdcc->nbytes_max;
    const int         nmeth = 2;           /* Number of methods */
    int               w[1];                /* Weighting as an interval */
    H5D_rdcc_ent_t   *p[2], *cur;          /* List pointers */
    H5D_rdcc_ent_t   *n[2];                /* List next pointers */
    int               nerrors   = 0;       /* Accumulated error count during preemptions */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Preemption is accomplished by having multiple pointers (currently two)
     * slide down the list beginning at the head. Pointer p(N+1) will start
     * traversing the list when pointer pN reaches wN percent of the original
     * list.  In other words, preemption method N gets to consider entries in
     * approximate least recently used order w0 percent before method N+1
     * where 100% means the method N will run to completion before method N+1
     * begins.  The pointers participating in the list traversal are each
     * given a chance at preemption before any of the pointers are advanced.
     */
    w[0] = (int)(rdcc->nused * rdcc->w0);
    p[0] = rdcc->head;
    p[1] = NULL;

    while ((p[0] || p[1]) && (rdcc->nbytes_used + size) > total) {
        int i; /* Local index variable */

        /* Introduce new pointers */
        for (i = 0; i < nmeth - 1; i++)
            if (0 == w[i])
                p[i + 1] = rdcc->head;

        /* Compute next value for each pointer */
        for (i = 0; i < nmeth; i++)
            n[i] = p[i] ? p[i]->next : NULL;

        /* Give each method a chance */
        for (i = 0; i < nmeth && (rdcc->nbytes_used + size) > total; i++) {
            if (0 == i && p[0] && !p[0]->locked &&
                ((0 == p[0]->rd_count && 0 == p[0]->wr_count) ||
                 (0 == p[0]->rd_count && dset->shared->layout.u.chunk.size == p[0]->wr_count) ||
                 (dset->shared->layout.u.chunk.size == p[0]->rd_count && 0 == p[0]->wr_count))) {
                /*
                 * Method 0: Preempt entries that have been completely written
                 * and/or completely read but not entries that are partially
                 * written or partially read.
                 */
                cur = p[0];
            }
            else if (1 == i && p[1] && !p[1]->locked) {
                /*
                 * Method 1: Preempt the entry without regard to
                 * considerations other than being locked.  This is the last
                 * resort preemption.
                 */
                cur = p[1];
            }
            else {
                /* Nothing to preempt at this point */
                cur = NULL;
            }

            if (cur) {
                int j; /* Local index variable */

                for (j = 0; j < nmeth; j++) {
                    if (p[j] == cur)
                        p[j] = NULL;
                    if (n[j] == cur)
                        n[j] = cur->next;
                } /* end for */
                if (H5D__chunk_cache_evict(dset, cur, true) < 0)
                    nerrors++;
            } /* end if */
        }     /* end for */

        /* Advance pointers */
        for (i = 0; i < nmeth; i++)
            p[i] = n[i];
        for (i = 0; i < nmeth - 1; i++)
            w[i] -= 1;
    } /* end while */

    if (nerrors)
        HGOTO_ERROR(H5E_IO, H5E_CANTFLUSH, FAIL, "unable to preempt one or more raw data cache entry");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_cache_prune() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_lock
 *
 * Purpose:    Return a pointer to a dataset chunk.  The pointer points
 *        directly into the chunk cache and should not be freed
 *        by the caller but will be valid until it is unlocked.  The
 *        input value IDX_HINT is used to speed up cache lookups and
 *        it's output value should be given to H5D__chunk_unlock().
 *        IDX_HINT is ignored if it is out of range, and if it points
 *        to the wrong entry then we fall back to the normal search
 *        method.
 *
 *        If RELAX is non-zero and the chunk isn't in the cache then
 *        don't try to read it from the file, but just allocate an
 *        uninitialized buffer to hold the result.  This is intended
 *        for output functions that are about to overwrite the entire
 *        chunk.
 *
 * Return:    Success:    Ptr to a file chunk.
 *
 *        Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5D__chunk_lock(const H5D_io_info_t H5_ATTR_NDEBUG_UNUSED *io_info, const H5D_dset_io_info_t *dset_info,
                H5D_chunk_ud_t *udata, bool relax, bool prev_unfilt_chunk)
{
    const H5D_t *dset;      /* Convenience pointer to the dataset */
    H5O_pline_t *pline;     /* I/O pipeline info - always equal to the pline passed to H5D__chunk_mem_alloc */
    H5O_pline_t *old_pline; /* Old pipeline, i.e. pipeline used to read the chunk */
    const H5O_layout_t *layout;                  /* Dataset layout */
    const H5O_fill_t   *fill;                    /* Fill value info */
    H5D_fill_buf_info_t fb_info;                 /* Dataset's fill buffer info */
    bool                fb_info_init = false;    /* Whether the fill value buffer has been initialized */
    H5D_rdcc_t         *rdcc;                    /*raw data chunk cache*/
    H5D_rdcc_ent_t     *ent;                     /*cache entry        */
    size_t              chunk_size;              /*size of a chunk    */
    bool                disable_filters = false; /* Whether to disable filters (when adding to cache) */
    void               *chunk           = NULL;  /*the file chunk    */
    void               *ret_value       = NULL;  /* Return value         */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(io_info);
    assert(dset_info);
    assert(dset_info->store);
    dset = dset_info->dset;
    assert(dset);
    assert(udata);
    assert(!(udata->new_unfilt_chunk && prev_unfilt_chunk));

    /* Set convenience pointers */
    pline     = &(dset->shared->dcpl_cache.pline);
    old_pline = pline;
    layout    = &(dset->shared->layout);
    fill      = &(dset->shared->dcpl_cache.fill);
    rdcc      = &(dset->shared->cache.chunk);

    assert(!rdcc->tmp_head);

    /* Get the chunk's size */
    assert(layout->u.chunk.size > 0);
    H5_CHECKED_ASSIGN(chunk_size, size_t, layout->u.chunk.size, uint32_t);

    /* Check if the chunk is in the cache */
    if (UINT_MAX != udata->idx_hint) {
        /* Sanity check */
        assert(udata->idx_hint < rdcc->nslots);
        assert(rdcc->slot[udata->idx_hint]);

        /* Get the entry */
        ent = rdcc->slot[udata->idx_hint];

#ifndef NDEBUG
        {
            unsigned u; /*counters        */

            /* Make sure this is the right chunk */
            for (u = 0; u < layout->u.chunk.ndims - 1; u++)
                assert(dset_info->store->chunk.scaled[u] == ent->scaled[u]);
        }
#endif /* NDEBUG */

        /*
         * Already in the cache.  Count a hit.
         */
        rdcc->stats.nhits++;

        /* Make adjustments if the edge chunk status changed recently */
        if (pline->nused) {
            /* If the chunk recently became an unfiltered partial edge chunk
             * while in cache, we must make some changes to the entry */
            if (udata->new_unfilt_chunk) {
                /* If this flag is set then partial chunk filters must be
                 * disabled, and the chunk must not have previously been a
                 * partial chunk (with disabled filters) */
                assert(layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS);
                assert(!(ent->edge_chunk_state & H5D_RDCC_DISABLE_FILTERS));
                assert(old_pline->nused);

                /* Disable filters.  Set pline to NULL instead of just the
                 * default pipeline to make a quick failure more likely if the
                 * code is changed in an inappropriate/incomplete way. */
                pline = NULL;

                /* Reallocate the chunk so H5D__chunk_mem_xfree doesn't get confused
                 */
                if (NULL == (chunk = H5D__chunk_mem_alloc(chunk_size, pline)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                                "memory allocation failed for raw data chunk");
                H5MM_memcpy(chunk, ent->chunk, chunk_size);
                ent->chunk = (uint8_t *)H5D__chunk_mem_xfree(ent->chunk, old_pline);
                ent->chunk = (uint8_t *)chunk;
                chunk      = NULL;

                /* Mark the chunk as having filters disabled as well as "newly
                 * disabled" so it is inserted on flush */
                ent->edge_chunk_state |= H5D_RDCC_DISABLE_FILTERS;
                ent->edge_chunk_state |= H5D_RDCC_NEWLY_DISABLED_FILTERS;
            } /* end if */
            else if (prev_unfilt_chunk) {
                /* If this flag is set then partial chunk filters must be
                 * disabled, and the chunk must have previously been a partial
                 * chunk (with disabled filters) */
                assert(layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS);
                assert((ent->edge_chunk_state & H5D_RDCC_DISABLE_FILTERS));
                assert(pline->nused);

                /* Mark the old pipeline as having been disabled */
                old_pline = NULL;

                /* Reallocate the chunk so H5D__chunk_mem_xfree doesn't get confused
                 */
                if (NULL == (chunk = H5D__chunk_mem_alloc(chunk_size, pline)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                                "memory allocation failed for raw data chunk");
                H5MM_memcpy(chunk, ent->chunk, chunk_size);

                ent->chunk = (uint8_t *)H5D__chunk_mem_xfree(ent->chunk, old_pline);
                ent->chunk = (uint8_t *)chunk;
                chunk      = NULL;

                /* Mark the chunk as having filters enabled */
                ent->edge_chunk_state &= ~(H5D_RDCC_DISABLE_FILTERS | H5D_RDCC_NEWLY_DISABLED_FILTERS);
            } /* end else */
        }     /* end if */

        /*
         * If the chunk is not at the beginning of the cache; move it backward
         * by one slot.  This is how we implement the LRU preemption
         * algorithm.
         */
        if (ent->next) {
            if (ent->next->next)
                ent->next->next->prev = ent;
            else
                rdcc->tail = ent;
            ent->next->prev = ent->prev;
            if (ent->prev)
                ent->prev->next = ent->next;
            else
                rdcc->head = ent->next;
            ent->prev       = ent->next;
            ent->next       = ent->next->next;
            ent->prev->next = ent;
        } /* end if */
    }     /* end if */
    else {
        haddr_t chunk_addr;  /* Address of chunk on disk */
        hsize_t chunk_alloc; /* Length of chunk on disk */

        /* Save the chunk info so the cache stays consistent */
        chunk_addr  = udata->chunk_block.offset;
        chunk_alloc = udata->chunk_block.length;

        /* Check if we should disable filters on this chunk */
        if (pline->nused) {
            if (udata->new_unfilt_chunk) {
                assert(layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS);

                /* Disable the filters for writing */
                disable_filters = true;
                pline           = NULL;
            } /* end if */
            else if (prev_unfilt_chunk) {
                assert(layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS);

                /* Mark the filters as having been previously disabled (for the
                 * chunk as currently on disk) - disable the filters for reading
                 */
                old_pline = NULL;
            } /* end if */
            else if (layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS) {
                /* Check if this is an edge chunk */
                if (H5D__chunk_is_partial_edge_chunk(dset->shared->ndims, layout->u.chunk.dim,
                                                     dset_info->store->chunk.scaled,
                                                     dset->shared->curr_dims)) {
                    /* Disable the filters for both writing and reading */
                    disable_filters = true;
                    old_pline       = NULL;
                    pline           = NULL;
                } /* end if */
            }     /* end if */
        }         /* end if */

        if (relax) {
            /*
             * Not in the cache, but we're about to overwrite the whole thing
             * anyway, so just allocate a buffer for it but don't initialize that
             * buffer with the file contents. Count this as a hit instead of a
             * miss because we saved ourselves lots of work.
             */
            rdcc->stats.nhits++;

            if (NULL == (chunk = H5D__chunk_mem_alloc(chunk_size, pline)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for raw data chunk");

            /* In the case that some dataset functions look through this data,
             * clear it to all 0s. */
            memset(chunk, 0, chunk_size);
        } /* end if */
        else {
            /*
             * Not in the cache.  Count this as a miss if it's in the file
             *      or an init if it isn't.
             */

            /* Check if the chunk exists on disk */
            if (H5_addr_defined(chunk_addr)) {
                size_t my_chunk_alloc = chunk_alloc; /* Allocated buffer size */
                size_t buf_alloc      = chunk_alloc; /* [Re-]allocated buffer size */

                /* Chunk size on disk isn't [likely] the same size as the final chunk
                 * size in memory, so allocate memory big enough. */
                if (NULL == (chunk = H5D__chunk_mem_alloc(my_chunk_alloc,
                                                          (udata->new_unfilt_chunk ? old_pline : pline))))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                                "memory allocation failed for raw data chunk");
                if (H5F_shared_block_read(H5F_SHARED(dset->oloc.file), H5FD_MEM_DRAW, chunk_addr,
                                          my_chunk_alloc, chunk) < 0)
                    HGOTO_ERROR(H5E_IO, H5E_READERROR, NULL, "unable to read raw data chunk");

                if (old_pline && old_pline->nused) {
                    H5Z_EDC_t err_detect; /* Error detection info */
                    H5Z_cb_t  filter_cb;  /* I/O filter callback function */

                    /* Retrieve filter settings from API context */
                    if (H5CX_get_err_detect(&err_detect) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get error detection info");
                    if (H5CX_get_filter_cb(&filter_cb) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get I/O filter callback function");

                    if (H5Z_pipeline(old_pline, H5Z_FLAG_REVERSE, &(udata->filter_mask), err_detect,
                                     filter_cb, &my_chunk_alloc, &buf_alloc, &chunk) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTFILTER, NULL, "data pipeline read failed");

                    /* Reallocate chunk if necessary */
                    if (udata->new_unfilt_chunk) {
                        void *tmp_chunk = chunk;

                        if (NULL == (chunk = H5D__chunk_mem_alloc(my_chunk_alloc, pline))) {
                            (void)H5D__chunk_mem_xfree(tmp_chunk, old_pline);
                            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                                        "memory allocation failed for raw data chunk");
                        } /* end if */
                        H5MM_memcpy(chunk, tmp_chunk, chunk_size);
                        (void)H5D__chunk_mem_xfree(tmp_chunk, old_pline);
                    } /* end if */
                }     /* end if */

                /* Increment # of cache misses */
                rdcc->stats.nmisses++;
            } /* end if */
            else {
                H5D_fill_value_t fill_status;

                /* Sanity check */
                assert(fill->alloc_time != H5D_ALLOC_TIME_EARLY);

                /* Chunk size on disk isn't [likely] the same size as the final chunk
                 * size in memory, so allocate memory big enough. */
                if (NULL == (chunk = H5D__chunk_mem_alloc(chunk_size, pline)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                                "memory allocation failed for raw data chunk");

                if (H5P_is_fill_value_defined(fill, &fill_status) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't tell if fill value defined");

                if (fill->fill_time == H5D_FILL_TIME_ALLOC ||
                    (fill->fill_time == H5D_FILL_TIME_IFSET &&
                     (fill_status == H5D_FILL_VALUE_USER_DEFINED || fill_status == H5D_FILL_VALUE_DEFAULT))) {
                    /*
                     * The chunk doesn't exist in the file.  Replicate the fill
                     * value throughout the chunk, if the fill value is defined.
                     */

                    /* Initialize the fill value buffer */
                    /* (use the compact dataset storage buffer as the fill value buffer) */
                    if (H5D__fill_init(&fb_info, chunk, NULL, NULL, NULL, NULL,
                                       &dset->shared->dcpl_cache.fill, dset->shared->type,
                                       dset->shared->type_id, (size_t)0, chunk_size) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, NULL, "can't initialize fill buffer info");
                    fb_info_init = true;

                    /* Check for VL datatype & non-default fill value */
                    if (fb_info.has_vlen_fill_type)
                        /* Fill the buffer with VL datatype fill values */
                        if (H5D__fill_refill_vl(&fb_info, fb_info.elmts_per_buf) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, NULL, "can't refill fill value buffer");
                } /* end if */
                else
                    memset(chunk, 0, chunk_size);

                /* Increment # of creations */
                rdcc->stats.ninits++;
            } /* end else */
        }     /* end else */

        /* See if the chunk can be cached */
        if (rdcc->nslots > 0 && chunk_size <= rdcc->nbytes_max) {
            /* Calculate the index */
            udata->idx_hint = H5D__chunk_hash_val(dset->shared, udata->common.scaled);

            /* Add the chunk to the cache only if the slot is not already locked */
            ent = rdcc->slot[udata->idx_hint];
            if (!ent || !ent->locked) {
                /* Preempt enough things from the cache to make room */
                if (ent) {
                    if (H5D__chunk_cache_evict(dset, ent, true) < 0)
                        HGOTO_ERROR(H5E_IO, H5E_CANTINIT, NULL, "unable to preempt chunk from cache");
                } /* end if */
                if (H5D__chunk_cache_prune(dset, chunk_size) < 0)
                    HGOTO_ERROR(H5E_IO, H5E_CANTINIT, NULL, "unable to preempt chunk(s) from cache");

                /* Create a new entry */
                if (NULL == (ent = H5FL_CALLOC(H5D_rdcc_ent_t)))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, NULL, "can't allocate raw data chunk entry");

                ent->edge_chunk_state = disable_filters ? H5D_RDCC_DISABLE_FILTERS : 0;
                if (udata->new_unfilt_chunk)
                    ent->edge_chunk_state |= H5D_RDCC_NEWLY_DISABLED_FILTERS;

                /* Initialize the new entry */
                ent->chunk_block.offset = chunk_addr;
                ent->chunk_block.length = chunk_alloc;
                ent->chunk_idx          = udata->chunk_idx;
                H5MM_memcpy(ent->scaled, udata->common.scaled, sizeof(hsize_t) * layout->u.chunk.ndims);
                H5_CHECKED_ASSIGN(ent->rd_count, uint32_t, chunk_size, size_t);
                H5_CHECKED_ASSIGN(ent->wr_count, uint32_t, chunk_size, size_t);
                ent->chunk = (uint8_t *)chunk;

                /* Add it to the cache */
                assert(NULL == rdcc->slot[udata->idx_hint]);
                rdcc->slot[udata->idx_hint] = ent;
                ent->idx                    = udata->idx_hint;
                rdcc->nbytes_used += chunk_size;
                rdcc->nused++;

                /* Add it to the linked list */
                if (rdcc->tail) {
                    rdcc->tail->next = ent;
                    ent->prev        = rdcc->tail;
                    rdcc->tail       = ent;
                } /* end if */
                else
                    rdcc->head = rdcc->tail = ent;
                ent->tmp_next = NULL;
                ent->tmp_prev = NULL;

            } /* end if */
            else
                /* We did not add the chunk to cache */
                ent = NULL;
        }    /* end else */
        else /* No cache set up, or chunk is too large: chunk is uncacheable */
            ent = NULL;
    } /* end else */

    /* Lock the chunk into the cache */
    if (ent) {
        assert(!ent->locked);
        ent->locked = true;
        chunk       = ent->chunk;
    } /* end if */
    else
        /*
         * The chunk cannot be placed in cache so we don't cache it. This is the
         * reason all those arguments have to be repeated for the unlock
         * function.
         */
        udata->idx_hint = UINT_MAX;

    /* Set return value */
    ret_value = chunk;

done:
    /* Release the fill buffer info, if it's been initialized */
    if (fb_info_init && H5D__fill_term(&fb_info) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, NULL, "Can't release fill buffer info");

    /* Release the chunk allocated, on error */
    if (!ret_value)
        if (chunk)
            chunk = H5D__chunk_mem_xfree(chunk, pline);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_unlock
 *
 * Purpose:    Unlocks a previously locked chunk. The LAYOUT, COMP, and
 *        OFFSET arguments should be the same as for H5D__chunk_lock().
 *        The DIRTY argument should be set to non-zero if the chunk has
 *        been modified since it was locked. The IDX_HINT argument is
 *        the returned index hint from the lock operation and BUF is
 *        the return value from the lock.
 *
 *        The NACCESSED argument should be the number of bytes accessed
 *        for reading or writing (depending on the value of DIRTY).
 *        It's only purpose is to provide additional information to the
 *        preemption policy.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_unlock(const H5D_io_info_t H5_ATTR_NDEBUG_UNUSED *io_info, const H5D_dset_io_info_t *dset_info,
                  const H5D_chunk_ud_t *udata, bool dirty, void *chunk, uint32_t naccessed)
{
    const H5O_layout_t *layout; /* Dataset layout */
    const H5D_rdcc_t   *rdcc;
    const H5D_t        *dset;                /* Local pointer to the dataset info */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(dset_info);
    assert(udata);

    /* Set convenience pointers */
    layout = &(dset_info->dset->shared->layout);
    rdcc   = &(dset_info->dset->shared->cache.chunk);
    dset   = dset_info->dset;

    if (UINT_MAX == udata->idx_hint) {
        /*
         * It's not in the cache, probably because it's too big.  If it's
         * dirty then flush it to disk.  In any case, free the chunk.
         */
        bool is_unfiltered_edge_chunk = false; /* Whether the chunk is an unfiltered edge chunk */

        /* Check if we should disable filters on this chunk */
        if (udata->new_unfilt_chunk) {
            assert(layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS);

            is_unfiltered_edge_chunk = true;
        } /* end if */
        else if (layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS) {
            /* Check if the chunk is an edge chunk, and disable filters if so */
            is_unfiltered_edge_chunk =
                H5D__chunk_is_partial_edge_chunk(dset->shared->ndims, layout->u.chunk.dim,
                                                 dset_info->store->chunk.scaled, dset->shared->curr_dims);
        } /* end if */

        if (dirty) {
            H5D_rdcc_ent_t fake_ent; /* "fake" chunk cache entry */

            memset(&fake_ent, 0, sizeof(fake_ent));
            fake_ent.dirty = true;
            if (is_unfiltered_edge_chunk)
                fake_ent.edge_chunk_state = H5D_RDCC_DISABLE_FILTERS;
            if (udata->new_unfilt_chunk)
                fake_ent.edge_chunk_state |= H5D_RDCC_NEWLY_DISABLED_FILTERS;
            H5MM_memcpy(fake_ent.scaled, udata->common.scaled, sizeof(hsize_t) * layout->u.chunk.ndims);
            assert(layout->u.chunk.size > 0);
            fake_ent.chunk_idx          = udata->chunk_idx;
            fake_ent.chunk_block.offset = udata->chunk_block.offset;
            fake_ent.chunk_block.length = udata->chunk_block.length;
            fake_ent.chunk              = (uint8_t *)chunk;

            if (H5D__chunk_flush_entry(dset, &fake_ent, true) < 0)
                HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "cannot flush indexed storage buffer");
        } /* end if */
        else {
            if (chunk)
                chunk = H5D__chunk_mem_xfree(
                    chunk, (is_unfiltered_edge_chunk ? NULL : &(dset->shared->dcpl_cache.pline)));
        } /* end else */
    }     /* end if */
    else {
        H5D_rdcc_ent_t *ent; /* Chunk's entry in the cache */

        /* Sanity check */
        assert(udata->idx_hint < rdcc->nslots);
        assert(rdcc->slot[udata->idx_hint]);
        assert(rdcc->slot[udata->idx_hint]->chunk == chunk);

        /*
         * It's in the cache so unlock it.
         */
        ent = rdcc->slot[udata->idx_hint];
        assert(ent->locked);
        if (dirty) {
            ent->dirty = true;
            ent->wr_count -= MIN(ent->wr_count, naccessed);
        } /* end if */
        else
            ent->rd_count -= MIN(ent->rd_count, naccessed);
        ent->locked = false;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_unlock() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_allocated_cb
 *
 * Purpose:    Simply counts the number of chunks for a dataset.
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__chunk_allocated_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata)
{
    hsize_t *nbytes = (hsize_t *)_udata;

    FUNC_ENTER_PACKAGE_NOERR

    *(hsize_t *)nbytes += chunk_rec->nbytes;

    FUNC_LEAVE_NOAPI(H5_ITER_CONT)
} /* H5D__chunk_allocated_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_allocated
 *
 * Purpose:    Return the number of bytes allocated in the file for storage
 *        of raw data in the chunked dataset
 *
 * Return:    Success:    Number of bytes stored in all chunks.
 *        Failure:    0
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_allocated(const H5D_t *dset, hsize_t *nbytes)
{
    H5D_chk_idx_info_t   idx_info;                            /* Chunked index info */
    const H5D_rdcc_t    *rdcc = &(dset->shared->cache.chunk); /* Raw data chunk cache */
    H5D_rdcc_ent_t      *ent;                                 /* Cache entry  */
    hsize_t              chunk_bytes = 0;                     /* Number of bytes allocated for chunks */
    H5O_storage_chunk_t *sc          = &(dset->shared->layout.storage.u.chunk);
    herr_t               ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(dset);
    assert(dset->shared);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);

    /* Search for cached chunks that haven't been written out */
    for (ent = rdcc->head; ent; ent = ent->next)
        /* Flush the chunk out to disk, to make certain the size is correct later */
        if (H5D__chunk_flush_entry(dset, ent, false) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "cannot flush indexed storage buffer");

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = sc;

    /* Iterate over the chunks */
    if ((sc->ops->iterate)(&idx_info, H5D__chunk_allocated_cb, &chunk_bytes) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                    "unable to retrieve allocated chunk information from index");

    /* Set number of bytes for caller */
    *nbytes = chunk_bytes;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_allocated() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_allocate
 *
 * Purpose:    Allocate file space for all chunks that are not allocated yet.
 *        Return SUCCEED if all needed allocation succeed, otherwise
 *        FAIL.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_allocate(const H5D_t *dset, bool full_overwrite, const hsize_t old_dim[])
{
    H5D_chk_idx_info_t     idx_info;                                       /* Chunked index info */
    const H5D_chunk_ops_t *ops = dset->shared->layout.storage.u.chunk.ops; /* Chunk operations */
    hsize_t min_unalloc[H5O_LAYOUT_NDIMS]; /* First chunk in each dimension that is unallocated (in scaled
                                              coordinates) */
    hsize_t max_unalloc[H5O_LAYOUT_NDIMS]; /* Last chunk in each dimension that is unallocated (in scaled
                                              coordinates) */
    hsize_t           scaled[H5O_LAYOUT_NDIMS]; /* Offset of current chunk (in scaled coordinates) */
    size_t            orig_chunk_size;          /* Original size of chunk in bytes */
    size_t            chunk_size;               /* Actual size of chunk in bytes, possibly filtered */
    unsigned          filter_mask = 0;          /* Filter mask for chunks that have them */
    H5O_layout_t     *layout      = &(dset->shared->layout);           /* Dataset layout */
    H5O_pline_t      *pline       = &(dset->shared->dcpl_cache.pline); /* I/O pipeline info */
    H5O_pline_t       def_pline   = H5O_CRT_PIPELINE_DEF;              /* Default pipeline */
    const H5O_fill_t *fill        = &(dset->shared->dcpl_cache.fill);  /* Fill value info */
    H5D_fill_value_t  fill_status;                                     /* The fill value status */
    bool              should_fill     = false; /* Whether fill values should be written */
    void             *unfilt_fill_buf = NULL;  /* Unfiltered fill value buffer */
    void            **fill_buf        = NULL;  /* Pointer to the fill buffer to use for a chunk */
#ifdef H5_HAVE_PARALLEL
    bool blocks_written = false; /* Flag to indicate that chunk was actually written */
    bool using_mpi =
        false; /* Flag to indicate that the file is being accessed with an MPI-capable file driver */
    H5D_chunk_coll_fill_info_t chunk_fill_info; /* chunk address information for doing I/O */
#endif                                          /* H5_HAVE_PARALLEL */
    bool                carry; /* Flag to indicate that chunk increment carrys to higher dimension (sorta) */
    unsigned            space_ndims;                     /* Dataset's space rank */
    const hsize_t      *space_dim;                       /* Dataset's dataspace dimensions */
    const uint32_t     *chunk_dim = layout->u.chunk.dim; /* Convenience pointer to chunk dimensions */
    unsigned            op_dim;                          /* Current operating dimension */
    H5D_fill_buf_info_t fb_info;                         /* Dataset's fill buffer info */
    bool                fb_info_init = false; /* Whether the fill value buffer has been initialized */
    bool has_unfilt_edge_chunks = false; /* Whether there are partial edge chunks with disabled filters */
    bool unfilt_edge_chunk_dim[H5O_LAYOUT_NDIMS]; /* Whether there are unfiltered edge chunks at the edge
                                                        of each dimension */
    hsize_t edge_chunk_scaled[H5O_LAYOUT_NDIMS];  /* Offset of the unfiltered edge chunks at the edge of each
                                                     dimension */
    unsigned             nunfilt_edge_chunk_dims = 0; /* Number of dimensions on an edge */
    H5O_storage_chunk_t *sc                      = &(layout->storage.u.chunk); /* Convenience variable */
    herr_t               ret_value               = SUCCEED;                    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dset && H5D_CHUNKED == layout->type);
    assert(layout->u.chunk.ndims > 0 && layout->u.chunk.ndims <= H5O_LAYOUT_NDIMS);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);

    /* Retrieve the dataset dimensions */
    space_dim   = dset->shared->curr_dims;
    space_ndims = dset->shared->ndims;

    /* The last dimension in scaled chunk coordinates is always 0 */
    scaled[space_ndims] = (hsize_t)0;

    /* Check if any space dimensions are 0, if so we do not have to do anything
     */
    for (op_dim = 0; op_dim < (unsigned)space_ndims; op_dim++)
        if (space_dim[op_dim] == 0) {
            /* Reset any cached chunk info for this dataset */
            H5D__chunk_cinfo_cache_reset(&dset->shared->cache.chunk.last);
            HGOTO_DONE(SUCCEED);
        } /* end if */

#ifdef H5_HAVE_PARALLEL
    /* Retrieve MPI parameters */
    if (H5F_HAS_FEATURE(dset->oloc.file, H5FD_FEAT_HAS_MPI)) {
        /* Set the MPI-capable file driver flag */
        using_mpi = true;

        /* init chunk info stuff for collective I/O */
        chunk_fill_info.num_chunks = 0;
        chunk_fill_info.chunk_info = NULL;
    }  /* end if */
#endif /* H5_HAVE_PARALLEL */

    /* Calculate the minimum and maximum chunk offsets in each dimension, and
     * determine if there are any unfiltered partial edge chunks.  Note that we
     * assume here that all elements of space_dim are > 0.  This is checked at
     * the top of this function. */
    for (op_dim = 0; op_dim < space_ndims; op_dim++) {
        /* Validate this chunk dimension */
        if (chunk_dim[op_dim] == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk size must be > 0, dim = %u ", op_dim);
        min_unalloc[op_dim] = (old_dim[op_dim] + chunk_dim[op_dim] - 1) / chunk_dim[op_dim];
        max_unalloc[op_dim] = (space_dim[op_dim] - 1) / chunk_dim[op_dim];

        /* Calculate if there are unfiltered edge chunks at the edge of this
         * dimension.  Note the edge_chunk_scaled is uninitialized for
         * dimensions where unfilt_edge_chunk_dim is false.  Also  */
        if ((layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS) && pline->nused > 0 &&
            space_dim[op_dim] % chunk_dim[op_dim] != 0) {
            has_unfilt_edge_chunks        = true;
            unfilt_edge_chunk_dim[op_dim] = true;
            edge_chunk_scaled[op_dim]     = max_unalloc[op_dim];
        } /* end if */
        else
            unfilt_edge_chunk_dim[op_dim] = false;
    } /* end for */

    /* Get original chunk size */
    H5_CHECKED_ASSIGN(orig_chunk_size, size_t, layout->u.chunk.size, uint32_t);

    /* Check the dataset's fill-value status */
    if (H5P_is_fill_value_defined(fill, &fill_status) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't tell if fill value defined");

    /* If we are filling the dataset on allocation or "if set" and
     * the fill value _is_ set, _and_ we are not overwriting the new blocks,
     * or if there are any pipeline filters defined,
     * set the "should fill" flag
     */
    if ((!full_overwrite &&
         (fill->fill_time == H5D_FILL_TIME_ALLOC ||
          (fill->fill_time == H5D_FILL_TIME_IFSET &&
           (fill_status == H5D_FILL_VALUE_USER_DEFINED || fill_status == H5D_FILL_VALUE_DEFAULT)))) ||
        pline->nused > 0)
        should_fill = true;

    /* Check if fill values should be written to chunks */
    if (should_fill) {
        /* Initialize the fill value buffer */
        /* (delay allocating fill buffer for VL datatypes until refilling) */
        if (H5D__fill_init(&fb_info, NULL, H5D__chunk_mem_alloc, pline, H5D__chunk_mem_free, pline,
                           &dset->shared->dcpl_cache.fill, dset->shared->type, dset->shared->type_id,
                           (size_t)0, orig_chunk_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize fill buffer info");
        fb_info_init = true;

        /* Initialize the fill_buf pointer to the buffer in fb_info.  If edge
         * chunk filters are disabled, we will switch the buffer as appropriate
         * for each chunk. */
        fill_buf = &fb_info.fill_buf;

        /* Check if there are filters which need to be applied to the chunk */
        /* (only do this in advance when the chunk info can be re-used (i.e.
         *      it doesn't contain any non-default VL datatype fill values)
         */
        if (!fb_info.has_vlen_fill_type && pline->nused > 0) {
            H5Z_EDC_t err_detect; /* Error detection info */
            H5Z_cb_t  filter_cb;  /* I/O filter callback function */
            size_t    buf_size = orig_chunk_size;

            /* If the dataset has disabled partial chunk filters, create a copy
             * of the unfiltered fill_buf to use for partial chunks */
            if (has_unfilt_edge_chunks) {
                if (NULL == (unfilt_fill_buf = H5D__chunk_mem_alloc(orig_chunk_size, &def_pline)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                "memory allocation failed for raw data chunk");
                H5MM_memcpy(unfilt_fill_buf, fb_info.fill_buf, orig_chunk_size);
            } /* end if */

            /* Retrieve filter settings from API context */
            if (H5CX_get_err_detect(&err_detect) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get error detection info");
            if (H5CX_get_filter_cb(&filter_cb) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get I/O filter callback function");

            /* Push the chunk through the filters */
            if (H5Z_pipeline(pline, 0, &filter_mask, err_detect, filter_cb, &orig_chunk_size, &buf_size,
                             &fb_info.fill_buf) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_WRITEERROR, FAIL, "output pipeline failed");
#if H5_SIZEOF_SIZE_T > 4
            /* Check for the chunk expanding too much to encode in a 32-bit value */
            if (orig_chunk_size > ((size_t)0xffffffff))
                HGOTO_ERROR(H5E_DATASET, H5E_BADRANGE, FAIL, "chunk too large for 32-bit length");
#endif    /* H5_SIZEOF_SIZE_T > 4 */
        } /* end if */
    }     /* end if */

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = sc;

    /* Loop over all chunks */
    /* The algorithm is:
     *  For each dimension:
     *   -Allocate all chunks in the new dataspace that are beyond the original
     *    dataspace in the operating dimension, except those that have already
     *    been allocated.
     *
     * This is accomplished mainly using the min_unalloc and max_unalloc arrays.
     * min_unalloc represents the lowest offset in each dimension of chunks that
     * have not been allocated (whether or not they need to be).  max_unalloc
     * represents the highest offset in each dimension of chunks in the new
     * dataset that have not been allocated by this routine (they may have been
     * allocated previously).
     *
     * Every time the algorithm finishes allocating chunks allocated beyond a
     * certain dimension, max_unalloc is updated in order to avoid allocating
     * those chunks again.
     *
     * Note that min_unalloc & max_unalloc are in scaled coordinates.
     *
     */
    chunk_size = orig_chunk_size;
    for (op_dim = 0; op_dim < space_ndims; op_dim++) {
        H5D_chunk_ud_t udata; /* User data for querying chunk info */
        unsigned       u;     /* Local index variable */
        int            i;     /* Local index variable */

        /* Check if allocation along this dimension is really necessary */
        if (min_unalloc[op_dim] > max_unalloc[op_dim])
            continue;
        else {
            /* Reset the chunk offset indices */
            memset(scaled, 0, (space_ndims * sizeof(scaled[0])));
            scaled[op_dim] = min_unalloc[op_dim];

            if (has_unfilt_edge_chunks) {
                /* Initialize nunfilt_edge_chunk_dims */
                nunfilt_edge_chunk_dims = 0;
                for (u = 0; u < space_ndims; u++)
                    if (unfilt_edge_chunk_dim[u] && scaled[u] == edge_chunk_scaled[u])
                        nunfilt_edge_chunk_dims++;

                /* Initialize chunk_size and fill_buf */
                if (should_fill && !fb_info.has_vlen_fill_type) {
                    assert(fb_info_init);
                    assert(unfilt_fill_buf);
                    if (nunfilt_edge_chunk_dims) {
                        fill_buf   = &unfilt_fill_buf;
                        chunk_size = layout->u.chunk.size;
                    } /* end if */
                    else {
                        fill_buf   = &fb_info.fill_buf;
                        chunk_size = orig_chunk_size;
                    } /* end else */
                }     /* end if */
            }         /* end if */

            carry = false;
        } /* end else */

        while (!carry) {
            bool need_insert = false; /* Whether the chunk needs to be inserted into the index */

            /* Look up this chunk */
            if (H5D__chunk_lookup(dset, scaled, &udata) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");
#ifndef NDEBUG
            /* None of the chunks should be allocated */
            if (H5D_CHUNK_IDX_NONE != sc->idx_type)
                assert(!H5_addr_defined(udata.chunk_block.offset));

            /* Make sure the chunk is really in the dataset and outside the
             * original dimensions */
            {
                unsigned v; /* Local index variable */
                bool     outside_orig = false;

                for (v = 0; v < space_ndims; v++) {
                    assert((scaled[v] * chunk_dim[v]) < space_dim[v]);
                    if ((scaled[v] * chunk_dim[v]) >= old_dim[v])
                        outside_orig = true;
                } /* end for */
                assert(outside_orig);
            } /* end block */
#endif        /* NDEBUG */

            /* Check for VL datatype & non-default fill value */
            if (fb_info_init && fb_info.has_vlen_fill_type) {
                /* Sanity check */
                assert(should_fill);
                assert(!unfilt_fill_buf);
#ifdef H5_HAVE_PARALLEL
                assert(!using_mpi); /* Can't write VL datatypes in parallel currently */
#endif

                /* Check to make sure the buffer is large enough.  It is
                 * possible (though ill-advised) for the filter to shrink the
                 * buffer.
                 */
                if (fb_info.fill_buf_size < orig_chunk_size) {
                    if (NULL ==
                        (fb_info.fill_buf = H5D__chunk_mem_realloc(fb_info.fill_buf, orig_chunk_size, pline)))
                        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                    "memory reallocation failed for raw data chunk");
                    fb_info.fill_buf_size = orig_chunk_size;
                } /* end if */

                /* Fill the buffer with VL datatype fill values */
                if (H5D__fill_refill_vl(&fb_info, fb_info.elmts_per_buf) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't refill fill value buffer");

                /* Check if there are filters which need to be applied to the chunk */
                if ((pline->nused > 0) && !nunfilt_edge_chunk_dims) {
                    H5Z_EDC_t err_detect; /* Error detection info */
                    H5Z_cb_t  filter_cb;  /* I/O filter callback function */
                    size_t    nbytes = orig_chunk_size;

                    /* Retrieve filter settings from API context */
                    if (H5CX_get_err_detect(&err_detect) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get error detection info");
                    if (H5CX_get_filter_cb(&filter_cb) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get I/O filter callback function");

                    /* Push the chunk through the filters */
                    if (H5Z_pipeline(pline, 0, &filter_mask, err_detect, filter_cb, &nbytes,
                                     &fb_info.fill_buf_size, &fb_info.fill_buf) < 0)
                        HGOTO_ERROR(H5E_PLINE, H5E_WRITEERROR, FAIL, "output pipeline failed");

#if H5_SIZEOF_SIZE_T > 4
                    /* Check for the chunk expanding too much to encode in a 32-bit value */
                    if (nbytes > ((size_t)0xffffffff))
                        HGOTO_ERROR(H5E_DATASET, H5E_BADRANGE, FAIL, "chunk too large for 32-bit length");
#endif /* H5_SIZEOF_SIZE_T > 4 */

                    /* Keep the number of bytes the chunk turned in to */
                    chunk_size = nbytes;
                } /* end if */
                else
                    chunk_size = layout->u.chunk.size;

                assert(*fill_buf == fb_info.fill_buf);
            } /* end if */

            /* Initialize the chunk information */
            udata.common.layout      = &layout->u.chunk;
            udata.common.storage     = sc;
            udata.common.scaled      = scaled;
            udata.chunk_block.offset = HADDR_UNDEF;
            H5_CHECKED_ASSIGN(udata.chunk_block.length, uint32_t, chunk_size, size_t);
            udata.filter_mask = filter_mask;

            /* Allocate the chunk (with all processes) */
            if (H5D__chunk_file_alloc(&idx_info, NULL, &udata.chunk_block, &need_insert, scaled) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL,
                            "unable to insert/resize chunk on chunk level");
            assert(H5_addr_defined(udata.chunk_block.offset));

            /* Check if fill values should be written to chunks */
            if (should_fill) {
                /* Sanity check */
                assert(fb_info_init);
                assert(udata.chunk_block.length == chunk_size);

#ifdef H5_HAVE_PARALLEL
                /* Check if this file is accessed with an MPI-capable file driver */
                if (using_mpi) {
                    /* collect all chunk addresses to be written to
                       write collectively at the end */

                    /* allocate/resize chunk info array if no more space left */
                    if (0 == chunk_fill_info.num_chunks % 1024) {
                        void *tmp_realloc;

                        if (NULL == (tmp_realloc = H5MM_realloc(chunk_fill_info.chunk_info,
                                                                (chunk_fill_info.num_chunks + 1024) *
                                                                    sizeof(struct chunk_coll_fill_info))))
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                                        "memory allocation failed for chunk fill info");

                        chunk_fill_info.chunk_info = tmp_realloc;
                    }

                    /* Store info about the chunk for later */
                    chunk_fill_info.chunk_info[chunk_fill_info.num_chunks].addr = udata.chunk_block.offset;
                    chunk_fill_info.chunk_info[chunk_fill_info.num_chunks].chunk_size = chunk_size;
                    chunk_fill_info.chunk_info[chunk_fill_info.num_chunks].unfiltered_partial_chunk =
                        (*fill_buf == unfilt_fill_buf);
                    chunk_fill_info.num_chunks++;

                    /* Indicate that blocks will be written */
                    blocks_written = true;
                } /* end if */
                else {
#endif /* H5_HAVE_PARALLEL */
                    if (H5F_shared_block_write(H5F_SHARED(dset->oloc.file), H5FD_MEM_DRAW,
                                               udata.chunk_block.offset, chunk_size, *fill_buf) < 0)
                        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "unable to write raw data to file");
#ifdef H5_HAVE_PARALLEL
                } /* end else */
#endif            /* H5_HAVE_PARALLEL */
            }     /* end if */

            /* Insert the chunk record into the index */
            if (need_insert && ops->insert)
                if ((ops->insert)(&idx_info, &udata, dset) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "unable to insert chunk addr into index");

            /* Increment indices and adjust the edge chunk state */
            carry = true;
            for (i = ((int)space_ndims - 1); i >= 0; --i) {
                scaled[i]++;
                if (scaled[i] > max_unalloc[i]) {
                    if ((unsigned)i == op_dim)
                        scaled[i] = min_unalloc[i];
                    else
                        scaled[i] = 0;

                    /* Check if we just left the edge in this dimension */
                    if (unfilt_edge_chunk_dim[i] && edge_chunk_scaled[i] == max_unalloc[i] &&
                        scaled[i] < edge_chunk_scaled[i]) {
                        nunfilt_edge_chunk_dims--;
                        if (should_fill && nunfilt_edge_chunk_dims == 0 && !fb_info.has_vlen_fill_type) {
                            assert(
                                !H5D__chunk_is_partial_edge_chunk(space_ndims, chunk_dim, scaled, space_dim));
                            fill_buf   = &fb_info.fill_buf;
                            chunk_size = orig_chunk_size;
                        } /* end if */
                    }     /* end if */
                }         /* end if */
                else {
                    /* Check if we just entered the edge in this dimension */
                    if (unfilt_edge_chunk_dim[i] && scaled[i] == edge_chunk_scaled[i]) {
                        assert(edge_chunk_scaled[i] == max_unalloc[i]);
                        nunfilt_edge_chunk_dims++;
                        if (should_fill && nunfilt_edge_chunk_dims == 1 && !fb_info.has_vlen_fill_type) {
                            assert(
                                H5D__chunk_is_partial_edge_chunk(space_ndims, chunk_dim, scaled, space_dim));
                            fill_buf   = &unfilt_fill_buf;
                            chunk_size = layout->u.chunk.size;
                        } /* end if */
                    }     /* end if */

                    carry = false;
                    break;
                } /* end else */
            }     /* end for */
        }         /* end while(!carry) */

        /* Adjust max_unalloc so we don't allocate the same chunk twice.  Also
         * check if this dimension started from 0 (and hence allocated all of
         * the chunks. */
        if (min_unalloc[op_dim] == 0)
            break;
        else
            max_unalloc[op_dim] = min_unalloc[op_dim] - 1;
    } /* end for(op_dim=0...) */

#ifdef H5_HAVE_PARALLEL
    /* do final collective I/O */
    if (using_mpi && blocks_written)
        if (H5D__chunk_collective_fill(dset, &chunk_fill_info, fb_info.fill_buf, unfilt_fill_buf) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "unable to write raw data to file");
#endif /* H5_HAVE_PARALLEL */

    /* Reset any cached chunk info for this dataset */
    H5D__chunk_cinfo_cache_reset(&dset->shared->cache.chunk.last);

done:
    /* Release the fill buffer info, if it's been initialized */
    if (fb_info_init && H5D__fill_term(&fb_info) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release fill buffer info");

    /* Free the unfiltered fill value buffer */
    unfilt_fill_buf = H5D__chunk_mem_xfree(unfilt_fill_buf, &def_pline);

#ifdef H5_HAVE_PARALLEL
    if (using_mpi && chunk_fill_info.chunk_info)
        H5MM_free(chunk_fill_info.chunk_info);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_allocate() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_update_old_edge_chunks
 *
 * Purpose:     Update all chunks which were previously partial edge
 *              chunks and are now complete.  Determines exactly which
 *              chunks need to be updated and locks each into cache using
 *              the 'prev_unfilt_chunk' flag, then unlocks it, causing
 *              filters to be applied as necessary.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_update_old_edge_chunks(H5D_t *dset, hsize_t old_dim[])
{
    hsize_t old_edge_chunk_sc[H5O_LAYOUT_NDIMS]; /* Offset of first previously incomplete chunk in each
                                                    dimension */
    hsize_t max_edge_chunk_sc[H5O_LAYOUT_NDIMS]; /* largest offset of chunks that might need to be modified in
                                                    each dimension */
    bool new_full_dim[H5O_LAYOUT_NDIMS];         /* Whether the plane of chunks in this dimension needs to be
                                                       modified */
    const H5O_layout_t *layout = &(dset->shared->layout); /* Dataset layout */
    hsize_t             chunk_sc[H5O_LAYOUT_NDIMS];       /* Offset of current chunk */
    const uint32_t     *chunk_dim = layout->u.chunk.dim;  /* Convenience pointer to chunk dimensions */
    unsigned            space_ndims;                      /* Dataset's space rank */
    const hsize_t      *space_dim;                        /* Dataset's dataspace dimensions */
    unsigned            op_dim;                           /* Current operationg dimension */
    H5D_io_info_t       chk_io_info;                      /* Chunked I/O info object */
    H5D_chunk_ud_t      chk_udata;                        /* User data for locking chunk */
    H5D_storage_t       chk_store;                        /* Chunk storage information */
    H5D_dset_io_info_t  chk_dset_info;                    /* Chunked I/O dset info object */
    void               *chunk;                            /* The file chunk  */
    bool                carry; /* Flag to indicate that chunk increment carrys to higher dimension (sorta) */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dset && H5D_CHUNKED == layout->type);
    assert(layout->u.chunk.ndims > 0 && layout->u.chunk.ndims <= H5O_LAYOUT_NDIMS);
    H5D_CHUNK_STORAGE_INDEX_CHK(&layout->storage.u.chunk);
    assert(dset->shared->dcpl_cache.pline.nused > 0);
    assert(layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS);

    /* Retrieve the dataset dimensions */
    space_dim   = dset->shared->curr_dims;
    space_ndims = dset->shared->ndims;

    /* The last dimension in chunk_offset is always 0 */
    chunk_sc[space_ndims] = (hsize_t)0;

    /* Check if any current dimensions are smaller than the chunk size, or if
     * any old dimensions are 0.  If so we do not have to do anything. */
    for (op_dim = 0; op_dim < space_ndims; op_dim++)
        if ((space_dim[op_dim] < chunk_dim[op_dim]) || old_dim[op_dim] == 0) {
            /* Reset any cached chunk info for this dataset */
            H5D__chunk_cinfo_cache_reset(&dset->shared->cache.chunk.last);
            HGOTO_DONE(SUCCEED);
        } /* end if */

    /* Set up chunked I/O info object, for operations on chunks (in callback).
     * Note that we only need to set chunk_offset once, as the array's address
     * will never change. */
    chk_store.chunk.scaled = chunk_sc;

    chk_io_info.op_type = H5D_IO_OP_READ;

    chk_dset_info.dset     = dset;
    chk_dset_info.store    = &chk_store;
    chk_dset_info.buf.vp   = NULL;
    chk_io_info.dsets_info = &chk_dset_info;

    /*
     * Determine the edges of the dataset which need to be modified
     */
    for (op_dim = 0; op_dim < space_ndims; op_dim++) {
        /* Start off with this dimension marked as not needing to be modified */
        new_full_dim[op_dim] = false;

        /* Validate this chunk dimension */
        if (chunk_dim[op_dim] == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk size must be > 0, dim = %u ", op_dim);

        /* Calculate offset of first previously incomplete chunk in this
         * dimension */
        old_edge_chunk_sc[op_dim] = (old_dim[op_dim] / chunk_dim[op_dim]);

        /* Calculate the largest offset of chunks that might need to be
         * modified in this dimension */
        max_edge_chunk_sc[op_dim] = MIN((old_dim[op_dim] - 1) / chunk_dim[op_dim],
                                        MAX((space_dim[op_dim] / chunk_dim[op_dim]), 1) - 1);

        /* Check for old_dim aligned with chunk boundary in this dimension, if
         * so we do not need to modify chunks along the edge in this dimension
         */
        if (old_dim[op_dim] % chunk_dim[op_dim] == 0)
            continue;

        /* Check if the dataspace expanded enough to cause the old edge chunks
         * in this dimension to become full */
        if ((space_dim[op_dim] / chunk_dim[op_dim]) >= (old_edge_chunk_sc[op_dim] + 1))
            new_full_dim[op_dim] = true;
    } /* end for */

    /* Main loop: fix old edge chunks */
    for (op_dim = 0; op_dim < space_ndims; op_dim++) {
        /* Check if allocation along this dimension is really necessary */
        if (!new_full_dim[op_dim])
            continue;
        else {
            assert(max_edge_chunk_sc[op_dim] == old_edge_chunk_sc[op_dim]);

            /* Reset the chunk offset indices */
            memset(chunk_sc, 0, (space_ndims * sizeof(chunk_sc[0])));
            chunk_sc[op_dim] = old_edge_chunk_sc[op_dim];

            carry = false;
        } /* end if */

        while (!carry) {
            int i; /* Local index variable */

            /* Make sure the chunk is really a former edge chunk */
            assert(H5D__chunk_is_partial_edge_chunk(space_ndims, chunk_dim, chunk_sc, old_dim) &&
                   !H5D__chunk_is_partial_edge_chunk(space_ndims, chunk_dim, chunk_sc, space_dim));

            /* Lookup the chunk */
            if (H5D__chunk_lookup(dset, chunk_sc, &chk_udata) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");

            /* If this chunk does not exist in cache or on disk, no need to do
             * anything */
            if (H5_addr_defined(chk_udata.chunk_block.offset) || (UINT_MAX != chk_udata.idx_hint)) {
                /* Lock the chunk into cache.  H5D__chunk_lock will take care of
                 * updating the chunk to no longer be an edge chunk. */
                if (NULL ==
                    (chunk = (void *)H5D__chunk_lock(&chk_io_info, &chk_dset_info, &chk_udata, false, true)))
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "unable to lock raw data chunk");

                /* Unlock the chunk */
                if (H5D__chunk_unlock(&chk_io_info, &chk_dset_info, &chk_udata, true, chunk, (uint32_t)0) < 0)
                    HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "unable to unlock raw data chunk");
            } /* end if */

            /* Increment indices */
            carry = true;
            for (i = ((int)space_ndims - 1); i >= 0; --i) {
                if ((unsigned)i != op_dim) {
                    ++chunk_sc[i];
                    if (chunk_sc[i] > (hsize_t)max_edge_chunk_sc[i])
                        chunk_sc[i] = 0;
                    else {
                        carry = false;
                        break;
                    } /* end else */
                }     /* end if */
            }         /* end for */
        }             /* end while(!carry) */

        /* Adjust max_edge_chunk_sc so we don't modify the same chunk twice.
         * Also check if this dimension started from 0 (and hence modified all
         * of the old edge chunks. */
        if (old_edge_chunk_sc[op_dim] == 0)
            break;
        else
            --max_edge_chunk_sc[op_dim];
    } /* end for(op_dim=0...) */

    /* Reset any cached chunk info for this dataset */
    H5D__chunk_cinfo_cache_reset(&dset->shared->cache.chunk.last);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_update_old_edge_chunks() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_collective_fill
 *
 * Purpose:     Use MPIO collective write to fill the chunks (if number of
 *              chunks to fill is greater than the number of MPI procs;
 *              otherwise use independent I/O).
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_collective_fill(const H5D_t *dset, H5D_chunk_coll_fill_info_t *chunk_fill_info,
                           const void *fill_buf, const void *partial_chunk_fill_buf)
{
    MPI_Comm         mpi_comm = MPI_COMM_NULL; /* MPI communicator for file */
    int              mpi_rank = (-1);          /* This process's rank  */
    int              mpi_size = (-1);          /* MPI Comm size  */
    int              mpi_code;                 /* MPI return code */
    size_t           num_blocks;               /* Number of blocks between processes. */
    size_t           leftover_blocks;          /* Number of leftover blocks to handle */
    int              blocks, leftover;         /* converted to int for MPI */
    MPI_Aint        *chunk_disp_array = NULL;
    MPI_Aint        *block_disps      = NULL;
    int             *block_lens       = NULL;
    MPI_Datatype     mem_type = MPI_BYTE, file_type = MPI_BYTE;
    H5FD_mpio_xfer_t prev_xfer_mode;         /* Previous data xfer mode */
    bool             have_xfer_mode = false; /* Whether the previous xffer mode has been retrieved */
    bool             need_sort      = false;
    size_t           i;                   /* Local index variable */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * If a separate fill buffer is provided for partial chunks, ensure
     * that the "don't filter partial edge chunks" flag is set.
     */
    if (partial_chunk_fill_buf)
        assert(dset->shared->layout.u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS);

    /* Get the MPI communicator */
    if (MPI_COMM_NULL == (mpi_comm = H5F_mpi_get_comm(dset->oloc.file)))
        HGOTO_ERROR(H5E_INTERNAL, H5E_MPI, FAIL, "Can't retrieve MPI communicator");

    /* Get the MPI rank */
    if ((mpi_rank = H5F_mpi_get_rank(dset->oloc.file)) < 0)
        HGOTO_ERROR(H5E_INTERNAL, H5E_MPI, FAIL, "Can't retrieve MPI rank");

    /* Get the MPI size */
    if ((mpi_size = H5F_mpi_get_size(dset->oloc.file)) < 0)
        HGOTO_ERROR(H5E_INTERNAL, H5E_MPI, FAIL, "Can't retrieve MPI size");

    /* Distribute evenly the number of blocks between processes. */
    if (mpi_size == 0)
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "Resulted in division by zero");
    num_blocks =
        (size_t)(chunk_fill_info->num_chunks / (size_t)mpi_size); /* value should be the same on all procs */

    /* After evenly distributing the blocks between processes, are there any
     * leftover blocks for each individual process (round-robin)?
     */
    leftover_blocks = (size_t)(chunk_fill_info->num_chunks % (size_t)mpi_size);

    /* Cast values to types needed by MPI */
    H5_CHECKED_ASSIGN(blocks, int, num_blocks, size_t);
    H5_CHECKED_ASSIGN(leftover, int, leftover_blocks, size_t);

    /* Check if we have any chunks to write on this rank */
    if (num_blocks > 0 || (leftover && leftover > mpi_rank)) {
        MPI_Aint partial_fill_buf_disp = 0;
        bool     all_same_block_len    = true;

        /* Allocate buffers */
        if (NULL == (chunk_disp_array = (MPI_Aint *)H5MM_malloc((size_t)(blocks + 1) * sizeof(MPI_Aint))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate chunk file displacement buffer");

        if (partial_chunk_fill_buf) {
            MPI_Aint fill_buf_addr;
            MPI_Aint partial_fill_buf_addr;

            /* Calculate the displacement between the fill buffer and partial chunk fill buffer */
            if (MPI_SUCCESS != (mpi_code = MPI_Get_address(fill_buf, &fill_buf_addr)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Get_address failed", mpi_code)
            if (MPI_SUCCESS != (mpi_code = MPI_Get_address(partial_chunk_fill_buf, &partial_fill_buf_addr)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Get_address failed", mpi_code)

#if H5_CHECK_MPI_VERSION(3, 1)
            partial_fill_buf_disp = MPI_Aint_diff(partial_fill_buf_addr, fill_buf_addr);
#else
            partial_fill_buf_disp = partial_fill_buf_addr - fill_buf_addr;
#endif

            /*
             * Allocate all-zero block displacements array. If a block's displacement
             * is left as zero, that block will be written to from the regular fill
             * buffer. If a block represents an unfiltered partial edge chunk, its
             * displacement will be set so that the block is written to from the
             * unfiltered fill buffer.
             */
            if (NULL == (block_disps = (MPI_Aint *)H5MM_calloc((size_t)(blocks + 1) * sizeof(MPI_Aint))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate block displacements buffer");
        }

        /*
         * Perform initial scan of chunk info list to:
         *  - make sure that chunk addresses are monotonically non-decreasing
         *  - check if all blocks have the same length
         */
        for (i = 1; i < chunk_fill_info->num_chunks; i++) {
            if (chunk_fill_info->chunk_info[i].addr < chunk_fill_info->chunk_info[i - 1].addr)
                need_sort = true;

            if (chunk_fill_info->chunk_info[i].chunk_size != chunk_fill_info->chunk_info[i - 1].chunk_size)
                all_same_block_len = false;
        }

        if (need_sort)
            qsort(chunk_fill_info->chunk_info, chunk_fill_info->num_chunks,
                  sizeof(struct chunk_coll_fill_info), H5D__chunk_cmp_coll_fill_info);

        /* Allocate buffer for block lengths if necessary */
        if (!all_same_block_len)
            if (NULL == (block_lens = (int *)H5MM_malloc((size_t)(blocks + 1) * sizeof(int))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate chunk lengths buffer");

        for (i = 0; i < (size_t)blocks; i++) {
            size_t idx = i + (size_t)(mpi_rank * blocks);

            /* store the chunk address as an MPI_Aint */
            chunk_disp_array[i] = (MPI_Aint)(chunk_fill_info->chunk_info[idx].addr);

            if (!all_same_block_len)
                H5_CHECKED_ASSIGN(block_lens[i], int, chunk_fill_info->chunk_info[idx].chunk_size, size_t);

            if (chunk_fill_info->chunk_info[idx].unfiltered_partial_chunk) {
                assert(partial_chunk_fill_buf);
                block_disps[i] = partial_fill_buf_disp;
            }
        } /* end for */

        /* Calculate if there are any leftover blocks after evenly
         * distributing. If there are, then round-robin the distribution
         * to processes 0 -> leftover.
         */
        if (leftover && leftover > mpi_rank) {
            chunk_disp_array[blocks] =
                (MPI_Aint)chunk_fill_info->chunk_info[(blocks * mpi_size) + mpi_rank].addr;

            if (!all_same_block_len)
                H5_CHECKED_ASSIGN(block_lens[blocks], int,
                                  chunk_fill_info->chunk_info[(blocks * mpi_size) + mpi_rank].chunk_size,
                                  size_t);

            if (chunk_fill_info->chunk_info[(blocks * mpi_size) + mpi_rank].unfiltered_partial_chunk) {
                assert(partial_chunk_fill_buf);
                block_disps[blocks] = partial_fill_buf_disp;
            }

            blocks++;
        }

        /* Create file and memory types for the write operation */
        if (all_same_block_len) {
            int block_len;

            H5_CHECKED_ASSIGN(block_len, int, chunk_fill_info->chunk_info[0].chunk_size, size_t);

            mpi_code =
                MPI_Type_create_hindexed_block(blocks, block_len, chunk_disp_array, MPI_BYTE, &file_type);
            if (mpi_code != MPI_SUCCESS)
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed_block failed", mpi_code)

            if (partial_chunk_fill_buf) {
                /*
                 * If filters are disabled for partial edge chunks, those chunks could
                 * potentially have the same block length as the other chunks, but still
                 * need to be written to using the unfiltered fill buffer. Use an hindexed
                 * block type rather than an hvector.
                 */
                mpi_code =
                    MPI_Type_create_hindexed_block(blocks, block_len, block_disps, MPI_BYTE, &mem_type);
                if (mpi_code != MPI_SUCCESS)
                    HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed_block failed", mpi_code)
            }
            else {
                mpi_code = MPI_Type_create_hvector(blocks, block_len, 0, MPI_BYTE, &mem_type);
                if (mpi_code != MPI_SUCCESS)
                    HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hvector failed", mpi_code)
            }
        }
        else {
            /*
             * Currently, different block lengths implies that there are partial
             * edge chunks and the "don't filter partial edge chunks" flag is set.
             */
            assert(partial_chunk_fill_buf);
            assert(block_lens);
            assert(block_disps);

            mpi_code = MPI_Type_create_hindexed(blocks, block_lens, chunk_disp_array, MPI_BYTE, &file_type);
            if (mpi_code != MPI_SUCCESS)
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed failed", mpi_code)

            mpi_code = MPI_Type_create_hindexed(blocks, block_lens, block_disps, MPI_BYTE, &mem_type);
            if (mpi_code != MPI_SUCCESS)
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed failed", mpi_code)
        }

        if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(&file_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)
        if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(&mem_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)
    } /* end if */

    /* Set MPI-IO VFD properties */

    /* Set MPI datatypes for operation */
    if (H5CX_set_mpi_coll_datatypes(mem_type, file_type) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set MPI-I/O properties");

    /* Get current transfer mode */
    if (H5CX_get_io_xfer_mode(&prev_xfer_mode) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set transfer mode");
    have_xfer_mode = true;

    /* Set transfer mode */
    if (H5CX_set_io_xfer_mode(H5FD_MPIO_COLLECTIVE) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set transfer mode");

    /* Low-level write (collective) */
    if (H5F_shared_block_write(H5F_SHARED(dset->oloc.file), H5FD_MEM_DRAW, (haddr_t)0,
                               (blocks) ? (size_t)1 : (size_t)0, fill_buf) < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "unable to write raw data to file");

    /* Barrier so processes don't race ahead */
    if (MPI_SUCCESS != (mpi_code = MPI_Barrier(mpi_comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_code)

done:
    if (have_xfer_mode)
        /* Set transfer mode */
        if (H5CX_set_io_xfer_mode(prev_xfer_mode) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set transfer mode");

    /* free things */
    if (MPI_BYTE != file_type)
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&file_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    if (MPI_BYTE != mem_type)
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&mem_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
    H5MM_xfree(chunk_disp_array);
    H5MM_xfree(block_disps);
    H5MM_xfree(block_lens);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_collective_fill() */

static int
H5D__chunk_cmp_coll_fill_info(const void *_entry1, const void *_entry2)
{
    const struct chunk_coll_fill_info *entry1;
    const struct chunk_coll_fill_info *entry2;

    FUNC_ENTER_PACKAGE_NOERR

    entry1 = (const struct chunk_coll_fill_info *)_entry1;
    entry2 = (const struct chunk_coll_fill_info *)_entry2;

    FUNC_LEAVE_NOAPI(H5_addr_cmp(entry1->addr, entry2->addr))
} /* end H5D__chunk_cmp_coll_fill_info() */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_prune_fill
 *
 * Purpose:    Write the fill value to the parts of the chunk that are no
 *              longer part of the dataspace
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__chunk_prune_fill(H5D_chunk_it_ud1_t *udata, bool new_unfilt_chunk)
{
    const H5D_io_info_t *io_info         = udata->io_info;          /* Local pointer to I/O info */
    const H5D_t         *dset            = udata->dset_info->dset;  /* Local pointer to the dataset info */
    const H5O_layout_t  *layout          = &(dset->shared->layout); /* Dataset's layout */
    unsigned             rank            = udata->common.layout->ndims - 1; /* Dataset rank */
    const hsize_t       *scaled          = udata->common.scaled;            /* Scaled chunk offset */
    H5S_sel_iter_t      *chunk_iter      = NULL;  /* Memory selection iteration info */
    bool                 chunk_iter_init = false; /* Whether the chunk iterator has been initialized */
    hsize_t              sel_nelmts;              /* Number of elements in selection */
    hsize_t              count[H5O_LAYOUT_NDIMS]; /* Element count of hyperslab */
    size_t               chunk_size;              /*size of a chunk       */
    void                *chunk;                   /* The file chunk  */
    H5D_chunk_ud_t       chk_udata;               /* User data for locking chunk */
    uint32_t             bytes_accessed;          /* Bytes accessed in chunk */
    unsigned             u;                       /* Local index variable */
    herr_t               ret_value = SUCCEED;     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get the chunk's size */
    assert(layout->u.chunk.size > 0);
    H5_CHECKED_ASSIGN(chunk_size, size_t, layout->u.chunk.size, uint32_t);

    /* Get the info for the chunk in the file */
    if (H5D__chunk_lookup(dset, scaled, &chk_udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk address");
    chk_udata.new_unfilt_chunk = new_unfilt_chunk;

    /* If this chunk does not exist in cache or on disk, no need to do anything */
    if (!H5_addr_defined(chk_udata.chunk_block.offset) && UINT_MAX == chk_udata.idx_hint)
        HGOTO_DONE(SUCCEED);

    /* Initialize the fill value buffer, if necessary */
    if (!udata->fb_info_init) {
        H5_CHECK_OVERFLOW(udata->elmts_per_chunk, uint32_t, size_t);
        if (H5D__fill_init(&udata->fb_info, NULL, NULL, NULL, NULL, NULL, &dset->shared->dcpl_cache.fill,
                           dset->shared->type, dset->shared->type_id, (size_t)udata->elmts_per_chunk,
                           chunk_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize fill buffer info");
        udata->fb_info_init = true;
    } /* end if */

    /* Compute the # of elements to leave with existing value, in each dimension */
    for (u = 0; u < rank; u++) {
        count[u] = MIN(layout->u.chunk.dim[u], (udata->space_dim[u] - (scaled[u] * layout->u.chunk.dim[u])));
        assert(count[u] > 0);
    } /* end for */

    /* Select all elements in chunk, to begin with */
    if (H5S_select_all(udata->chunk_space, true) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL, "unable to select space");

    /* "Subtract out" the elements to keep */
    if (H5S_select_hyperslab(udata->chunk_space, H5S_SELECT_NOTB, udata->hyper_start, NULL, count, NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSELECT, FAIL, "unable to select hyperslab");

    /* Lock the chunk into the cache, to get a pointer to the chunk buffer */
    if (NULL == (chunk = (void *)H5D__chunk_lock(io_info, udata->dset_info, &chk_udata, false, false)))
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "unable to lock raw data chunk");

    /* Fill the selection in the memory buffer */
    /* Use the size of the elements in the chunk directly instead of */
    /* relying on the fill.size, which might be set to 0 if there is */
    /* no fill-value defined for the dataset -QAK */

    /* Get the number of elements in the selection */
    sel_nelmts = H5S_GET_SELECT_NPOINTS(udata->chunk_space);
    H5_CHECK_OVERFLOW(sel_nelmts, hsize_t, size_t);

    /* Check for VL datatype & non-default fill value */
    if (udata->fb_info.has_vlen_fill_type)
        /* Re-fill the buffer to use for this I/O operation */
        if (H5D__fill_refill_vl(&udata->fb_info, (size_t)sel_nelmts) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't refill fill value buffer");

    /* Allocate the chunk selection iterator */
    if (NULL == (chunk_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate chunk selection iterator");

    /* Create a selection iterator for scattering the elements to memory buffer */
    if (H5S_select_iter_init(chunk_iter, udata->chunk_space, layout->u.chunk.dim[rank], 0) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize chunk selection information");
    chunk_iter_init = true;

    /* Scatter the data into memory */
    if (H5D__scatter_mem(udata->fb_info.fill_buf, chunk_iter, (size_t)sel_nelmts, chunk /*out*/) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "scatter failed");

    /* The number of bytes accessed in the chunk */
    /* (i.e. the bytes replaced with fill values) */
    H5_CHECK_OVERFLOW(sel_nelmts, hsize_t, uint32_t);
    bytes_accessed = (uint32_t)sel_nelmts * layout->u.chunk.dim[rank];

    /* Release lock on chunk */
    if (H5D__chunk_unlock(io_info, udata->dset_info, &chk_udata, true, chunk, bytes_accessed) < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "unable to unlock raw data chunk");

done:
    /* Release the selection iterator */
    if (chunk_iter_init && H5S_SELECT_ITER_RELEASE(chunk_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (chunk_iter)
        chunk_iter = H5FL_FREE(H5S_sel_iter_t, chunk_iter);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_prune_fill */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_prune_by_extent
 *
 * Purpose:    This function searches for chunks that are no longer necessary
 *              both in the raw data cache and in the chunk index.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 * The algorithm is:
 *
 *  For chunks that are no longer necessary:
 *
 *  1. Search in the raw data cache for each chunk
 *  2. If found then preempt it from the cache
 *  3. Search in the B-tree for each chunk
 *  4. If found then remove it from the B-tree and deallocate file storage for the chunk
 *
 * This example shows a 2d dataset of 90x90 with a chunk size of 20x20.
 *
 *
 *     0         20        40        60        80    90   100
 *    0 +---------+---------+---------+---------+-----+...+
 *      |:::::X::::::::::::::         :         :     |   :
 *      |:::::::X::::::::::::         :         :     |   :   Key
 *      |::::::::::X:::::::::         :         :     |   :   --------
 *      |::::::::::::X:::::::         :         :     |   :  +-+ Dataset
 *    20+::::::::::::::::::::.........:.........:.....+...:  | | Extent
 *      |         :::::X:::::         :         :     |   :  +-+
 *      |         :::::::::::         :         :     |   :
 *      |         :::::::::::         :         :     |   :  ... Chunk
 *      |         :::::::X:::         :         :     |   :  : : Boundary
 *    40+.........:::::::::::.........:.........:.....+...:  :.:
 *      |         :         :         :         :     |   :
 *      |         :         :         :         :     |   :  ... Allocated
 *      |         :         :         :         :     |   :  ::: & Filled
 *      |         :         :         :         :     |   :  ::: Chunk
 *    60+.........:.........:.........:.........:.....+...:
 *      |         :         :::::::X:::         :     |   :   X  Element
 *      |         :         :::::::::::         :     |   :      Written
 *      |         :         :::::::::::         :     |   :
 *      |         :         :::::::::::         :     |   :
 *    80+.........:.........:::::::::::.........:.....+...:   O  Fill Val
 *      |         :         :         :::::::::::     |   :      Explicitly
 *      |         :         :         ::::::X::::     |   :      Written
 *    90+---------+---------+---------+---------+-----+   :
 *      :         :         :         :::::::::::         :
 *   100:.........:.........:.........:::::::::::.........:
 *
 *
 * We have 25 total chunks for this dataset, 5 of which have space
 * allocated in the file because they were written to one or more
 * elements. These five chunks (and only these five) also have entries in
 * the storage B-tree for this dataset.
 *
 * Now lets say we want to shrink the dataset down to 70x70:
 *
 *
 *      0         20        40        60   70   80    90   100
 *    0 +---------+---------+---------+----+----+-----+...+
 *      |:::::X::::::::::::::         :    |    :     |   :
 *      |:::::::X::::::::::::         :    |    :     |   :    Key
 *      |::::::::::X:::::::::         :    |    :     |   :    --------
 *      |::::::::::::X:::::::         :    |    :     |   :   +-+ Dataset
 *    20+::::::::::::::::::::.........:....+....:.....|...:   | | Extent
 *      |         :::::X:::::         :    |    :     |   :   +-+
 *      |         :::::::::::         :    |    :     |   :
 *      |         :::::::::::         :    |    :     |   :   ... Chunk
 *      |         :::::::X:::         :    |    :     |   :   : : Boundary
 *    40+.........:::::::::::.........:....+....:.....|...:   :.:
 *      |         :         :         :    |    :     |   :
 *      |         :         :         :    |    :     |   :   ... Allocated
 *      |         :         :         :    |    :     |   :   ::: & Filled
 *      |         :         :         :    |    :     |   :   ::: Chunk
 *    60+.........:.........:.........:....+....:.....|...:
 *      |         :         :::::::X:::    |    :     |   :    X  Element
 *      |         :         :::::::::::    |    :     |   :       Written
 *      +---------+---------+---------+----+    :     |   :
 *      |         :         :::::::::::         :     |   :
 *    80+.........:.........:::::::::X:.........:.....|...:    O  Fill Val
 *      |         :         :         :::::::::::     |   :       Explicitly
 *      |         :         :         ::::::X::::     |   :       Written
 *    90+---------+---------+---------+---------+-----+   :
 *      :         :         :         :::::::::::         :
 *   100:.........:.........:.........:::::::::::.........:
 *
 *
 * That means that the nine chunks along the bottom and right side should
 * no longer exist. Of those nine chunks, (0,80), (20,80), (40,80),
 * (60,80), (80,80), (80,60), (80,40), (80,20), and (80,0), one is actually allocated
 * that needs to be released.
 * To release the chunks, we traverse the B-tree to obtain a list of unused
 * allocated chunks, and then call H5B_remove() for each chunk.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_prune_by_extent(H5D_t *dset, const hsize_t *old_dim)
{
    hsize_t min_mod_chunk_sc[H5O_LAYOUT_NDIMS]; /* Scaled offset of first chunk to modify in each dimension */
    hsize_t max_mod_chunk_sc[H5O_LAYOUT_NDIMS]; /* Scaled offset of last chunk to modify in each dimension */
    hssize_t max_fill_chunk_sc[H5O_LAYOUT_NDIMS]; /* Scaled offset of last chunk that might be filled in each
                                                     dimension */
    bool fill_dim[H5O_LAYOUT_NDIMS]; /* Whether the plane of edge chunks in this dimension needs to be
                                           filled */
    hsize_t min_partial_chunk_sc[H5O_LAYOUT_NDIMS]; /* Offset of first partial (or empty) chunk in each
                                                       dimension */
    bool new_unfilt_dim[H5O_LAYOUT_NDIMS]; /* Whether the plane of edge chunks in this dimension are newly
                                                 unfiltered */
    H5D_chk_idx_info_t  idx_info;          /* Chunked index info */
    H5D_io_info_t       chk_io_info;       /* Chunked I/O info object */
    H5D_dset_io_info_t  chk_dset_info;     /* Chunked I/O dset info object */
    H5D_storage_t       chk_store;         /* Chunk storage information */
    const H5O_layout_t *layout = &(dset->shared->layout);      /* Dataset's layout */
    const H5D_rdcc_t   *rdcc   = &(dset->shared->cache.chunk); /*raw data chunk cache */
    unsigned            space_ndims;                           /* Dataset's space rank */
    const hsize_t      *space_dim;                             /* Current dataspace dimensions */
    unsigned            op_dim;                                /* Current operating dimension */
    bool                shrunk_dim[H5O_LAYOUT_NDIMS];          /* Dimensions which have shrunk */
    H5D_chunk_it_ud1_t  udata;                                 /* Chunk index iterator user data */
    bool udata_init = false;         /* Whether the chunk index iterator user data has been initialized */
    H5D_chunk_common_ud_t idx_udata; /* User data for index removal routine */
    H5S_t                *chunk_space = NULL;            /* Dataspace for a chunk */
    hsize_t               chunk_dim[H5O_LAYOUT_NDIMS];   /* Chunk dimensions */
    hsize_t               scaled[H5O_LAYOUT_NDIMS];      /* Scaled offset of current chunk */
    hsize_t               hyper_start[H5O_LAYOUT_NDIMS]; /* Starting location of hyperslab */
    uint32_t              elmts_per_chunk;               /* Elements in chunk */
    bool     disable_edge_filters = false; /* Whether to disable filters on partial edge chunks */
    bool     new_unfilt_chunk     = false; /* Whether the chunk is newly unfiltered */
    unsigned u;                            /* Local index variable */
    const H5O_storage_chunk_t *sc        = &(layout->storage.u.chunk);
    herr_t                     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dset && H5D_CHUNKED == layout->type);
    assert(layout->u.chunk.ndims > 0 && layout->u.chunk.ndims <= H5O_LAYOUT_NDIMS);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);

    /* Go get the rank & dimensions (including the element size) */
    space_dim   = dset->shared->curr_dims;
    space_ndims = dset->shared->ndims;

    /* The last dimension in scaled is always 0 */
    scaled[space_ndims] = (hsize_t)0;

    /* Check if any old dimensions are 0, if so we do not have to do anything */
    for (op_dim = 0; op_dim < (unsigned)space_ndims; op_dim++)
        if (old_dim[op_dim] == 0) {
            /* Reset any cached chunk info for this dataset */
            H5D__chunk_cinfo_cache_reset(&dset->shared->cache.chunk.last);
            HGOTO_DONE(SUCCEED);
        } /* end if */

    /* Round up to the next integer # of chunks, to accommodate partial chunks */
    /* Use current dims because the indices have already been updated! -NAF */
    /* (also compute the number of elements per chunk) */
    /* (also copy the chunk dimensions into 'hsize_t' array for creating dataspace) */
    /* (also compute the dimensions which have been shrunk) */
    elmts_per_chunk = 1;
    for (u = 0; u < space_ndims; u++) {
        elmts_per_chunk *= layout->u.chunk.dim[u];
        chunk_dim[u]  = layout->u.chunk.dim[u];
        shrunk_dim[u] = (space_dim[u] < old_dim[u]);
    } /* end for */

    /* Create a dataspace for a chunk & set the extent */
    if (NULL == (chunk_space = H5S_create_simple(space_ndims, chunk_dim, NULL)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "can't create simple dataspace");

    /* Reset hyperslab start array */
    /* (hyperslabs will always start from origin) */
    memset(hyper_start, 0, sizeof(hyper_start));

    /* Set up chunked I/O info object, for operations on chunks (in callback)
     * Note that we only need to set scaled once, as the array's address
     * will never change. */
    chk_store.chunk.scaled = scaled;

    chk_io_info.op_type = H5D_IO_OP_READ;

    chk_dset_info.dset     = dset;
    chk_dset_info.store    = &chk_store;
    chk_dset_info.buf.vp   = NULL;
    chk_io_info.dsets_info = &chk_dset_info;
    chk_io_info.count      = 1;

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = &dset->shared->layout.storage.u.chunk;

    /* Initialize the user data for the iteration */
    memset(&udata, 0, sizeof udata);
    udata.common.layout   = &layout->u.chunk;
    udata.common.storage  = sc;
    udata.common.scaled   = scaled;
    udata.io_info         = &chk_io_info;
    udata.dset_info       = &chk_dset_info;
    udata.idx_info        = &idx_info;
    udata.space_dim       = space_dim;
    udata.shrunk_dim      = shrunk_dim;
    udata.elmts_per_chunk = elmts_per_chunk;
    udata.chunk_space     = chunk_space;
    udata.hyper_start     = hyper_start;
    udata_init            = true;

    /* Initialize user data for removal */
    idx_udata.layout  = &layout->u.chunk;
    idx_udata.storage = sc;

    /* Determine if partial edge chunk filters are disabled */
    disable_edge_filters = (layout->u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS) &&
                           (idx_info.pline->nused > 0);

    /*
     * Determine the chunks which need to be filled or removed
     */
    memset(min_mod_chunk_sc, 0, sizeof(min_mod_chunk_sc));
    memset(max_mod_chunk_sc, 0, sizeof(max_mod_chunk_sc));
    for (op_dim = 0; op_dim < (unsigned)space_ndims; op_dim++) {
        /* Validate this chunk dimension */
        if (chunk_dim[op_dim] == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk size must be > 0, dim = %u ", op_dim);

        /* Calculate the largest offset of chunks that might need to be
         * modified in this dimension */
        max_mod_chunk_sc[op_dim] = (old_dim[op_dim] - 1) / chunk_dim[op_dim];

        /* Calculate the largest offset of chunks that might need to be
         * filled in this dimension */
        if (0 == space_dim[op_dim])
            max_fill_chunk_sc[op_dim] = -1;
        else
            max_fill_chunk_sc[op_dim] =
                (hssize_t)(((MIN(space_dim[op_dim], old_dim[op_dim]) - 1) / chunk_dim[op_dim]));

        if (shrunk_dim[op_dim]) {
            /* Calculate the smallest offset of chunks that might need to be
             * modified in this dimension.  Note that this array contains
             * garbage for all dimensions which are not shrunk.  These locations
             * must not be read from! */
            min_mod_chunk_sc[op_dim] = space_dim[op_dim] / chunk_dim[op_dim];

            /* Determine if we need to fill chunks in this dimension */
            if ((hssize_t)min_mod_chunk_sc[op_dim] == max_fill_chunk_sc[op_dim]) {
                fill_dim[op_dim] = true;

                /* If necessary, check if chunks in this dimension that need to
                 * be filled are new partial edge chunks */
                if (disable_edge_filters && old_dim[op_dim] >= (min_mod_chunk_sc[op_dim] + 1))
                    new_unfilt_dim[op_dim] = true;
                else
                    new_unfilt_dim[op_dim] = false;
            } /* end if */
            else {
                fill_dim[op_dim]       = false;
                new_unfilt_dim[op_dim] = false;
            } /* end else */
        }     /* end if */
        else {
            fill_dim[op_dim]       = false;
            new_unfilt_dim[op_dim] = false;
        } /* end else */

        /* If necessary, calculate the smallest offset of non-previously full
         * chunks in this dimension, so we know these chunks were previously
         * unfiltered */
        if (disable_edge_filters)
            min_partial_chunk_sc[op_dim] = old_dim[op_dim] / chunk_dim[op_dim];
    } /* end for */

    /* Main loop: fill or remove chunks */
    for (op_dim = 0; op_dim < (unsigned)space_ndims; op_dim++) {
        bool dims_outside_fill[H5O_LAYOUT_NDIMS]; /* Dimensions in chunk offset outside fill dimensions */
        int  ndims_outside_fill; /* Number of dimensions in chunk offset outside fill dimensions */
        bool carry; /* Flag to indicate that chunk increment carrys to higher dimension (sorta) */

        /* Check if modification along this dimension is really necessary */
        if (!shrunk_dim[op_dim])
            continue;
        else {
            assert(max_mod_chunk_sc[op_dim] >= min_mod_chunk_sc[op_dim]);

            /* Reset the chunk offset indices */
            memset(scaled, 0, (space_ndims * sizeof(scaled[0])));
            scaled[op_dim] = min_mod_chunk_sc[op_dim];

            /* Initialize "dims_outside_fill" array */
            ndims_outside_fill = 0;
            for (u = 0; u < space_ndims; u++)
                if ((hssize_t)scaled[u] > max_fill_chunk_sc[u]) {
                    dims_outside_fill[u] = true;
                    ndims_outside_fill++;
                } /* end if */
                else
                    dims_outside_fill[u] = false;
        } /* end else */

        carry = false;
        while (!carry) {
            int i; /* Local index variable */

            udata.common.scaled = scaled;

            if (0 == ndims_outside_fill) {
                assert(fill_dim[op_dim]);
                assert(scaled[op_dim] == min_mod_chunk_sc[op_dim]);

                /* Make sure this is an edge chunk */
                assert(H5D__chunk_is_partial_edge_chunk(space_ndims, layout->u.chunk.dim, scaled, space_dim));

                /* Determine if the chunk just became an unfiltered chunk */
                if (new_unfilt_dim[op_dim]) {
                    new_unfilt_chunk = true;
                    for (u = 0; u < space_ndims; u++)
                        if (scaled[u] == min_partial_chunk_sc[u]) {
                            new_unfilt_chunk = false;
                            break;
                        } /* end if */
                }         /* end if */

                /* Make sure that, if we think this is a new unfiltered chunk,
                 * it was previously not an edge chunk */
                assert(!new_unfilt_dim[op_dim] ||
                       (!new_unfilt_chunk != !H5D__chunk_is_partial_edge_chunk(
                                                 space_ndims, layout->u.chunk.dim, scaled, old_dim)));
                assert(!new_unfilt_chunk || new_unfilt_dim[op_dim]);

                /* Fill the unused parts of the chunk */
                if (H5D__chunk_prune_fill(&udata, new_unfilt_chunk) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "unable to write fill value");
            } /* end if */
            else {
                H5D_chunk_ud_t chk_udata; /* User data for getting chunk info */

#ifndef NDEBUG
                /* Make sure this chunk is really outside the new dimensions */
                {
                    bool outside_dim = false;

                    for (u = 0; u < space_ndims; u++)
                        if ((scaled[u] * chunk_dim[u]) >= space_dim[u]) {
                            outside_dim = true;
                            break;
                        } /* end if */
                    assert(outside_dim);
                } /* end block */
#endif            /* NDEBUG */

                /* Check if the chunk exists in cache or on disk */
                if (H5D__chunk_lookup(dset, scaled, &chk_udata) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "error looking up chunk");

                /* Evict the entry from the cache if present, but do not flush
                 * it to disk */
                if (UINT_MAX != chk_udata.idx_hint)
                    if (H5D__chunk_cache_evict(dset, rdcc->slot[chk_udata.idx_hint], false) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTREMOVE, FAIL, "unable to evict chunk");

                /* Remove the chunk from disk, if present */
                if (H5_addr_defined(chk_udata.chunk_block.offset)) {
                    /* Update the offset in idx_udata */
                    idx_udata.scaled = udata.common.scaled;

                    /* Remove the chunk from disk */
                    if ((sc->ops->remove)(&idx_info, &idx_udata) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTDELETE, FAIL,
                                    "unable to remove chunk entry from index");
                } /* end if */
            }     /* end else */

            /* Increment indices */
            carry = true;
            for (i = (int)(space_ndims - 1); i >= 0; --i) {
                scaled[i]++;
                if (scaled[i] > max_mod_chunk_sc[i]) {
                    /* Left maximum dimensions, "wrap around" and check if this
                     * dimension is no longer outside the fill dimension */
                    if ((unsigned)i == op_dim) {
                        scaled[i] = min_mod_chunk_sc[i];
                        if (dims_outside_fill[i] && fill_dim[i]) {
                            dims_outside_fill[i] = false;
                            ndims_outside_fill--;
                        } /* end if */
                    }     /* end if */
                    else {
                        scaled[i] = 0;
                        if (dims_outside_fill[i] && max_fill_chunk_sc[i] >= 0) {
                            dims_outside_fill[i] = false;
                            ndims_outside_fill--;
                        } /* end if */
                    }     /* end else */
                }         /* end if */
                else {
                    /* Check if we just went outside the fill dimension */
                    if (!dims_outside_fill[i] && (hssize_t)scaled[i] > max_fill_chunk_sc[i]) {
                        dims_outside_fill[i] = true;
                        ndims_outside_fill++;
                    } /* end if */

                    /* We found the next chunk, so leave the loop */
                    carry = false;
                    break;
                } /* end else */
            }     /* end for */
        }         /* end while(!carry) */

        /* Adjust max_mod_chunk_sc so we don't modify the same chunk twice.
         * Also check if this dimension started from 0 (and hence removed all
         * of the chunks). */
        if (min_mod_chunk_sc[op_dim] == 0)
            break;
        else
            max_mod_chunk_sc[op_dim] = min_mod_chunk_sc[op_dim] - 1;
    } /* end for(op_dim=0...) */

    /* Reset any cached chunk info for this dataset */
    H5D__chunk_cinfo_cache_reset(&dset->shared->cache.chunk.last);

done:
    /* Release resources */
    if (chunk_space && H5S_close(chunk_space) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release dataspace");
    if (udata_init)
        if (udata.fb_info_init && H5D__fill_term(&udata.fb_info) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release fill buffer info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_prune_by_extent() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_addrmap_cb
 *
 * Purpose:     Callback when obtaining the chunk addresses for all existing chunks
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__chunk_addrmap_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata)
{
    H5D_chunk_it_ud2_t *udata = (H5D_chunk_it_ud2_t *)_udata;    /* User data for callback */
    unsigned            rank  = udata->common.layout->ndims - 1; /* # of dimensions of dataset */
    hsize_t             chunk_index;

    FUNC_ENTER_PACKAGE_NOERR

    /* Compute the index for this chunk */
    chunk_index = H5VM_array_offset_pre(rank, udata->common.layout->down_chunks, chunk_rec->scaled);

    /* Set it in the userdata to return */
    udata->chunk_addr[chunk_index] = chunk_rec->chunk_addr;

    FUNC_LEAVE_NOAPI(H5_ITER_CONT)
} /* H5D__chunk_addrmap_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_addrmap
 *
 * Purpose:     Obtain the chunk addresses for all existing chunks
 *
 * Return:    Success:    Non-negative on succeed.
 *        Failure:    negative value
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_addrmap(const H5D_t *dset, haddr_t chunk_addr[])
{
    H5D_chk_idx_info_t   idx_info; /* Chunked index info */
    H5D_chunk_it_ud2_t   udata;    /* User data for iteration callback */
    H5O_storage_chunk_t *sc;
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(dset);
    assert(dset->shared);
    sc = &(dset->shared->layout.storage.u.chunk);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);
    assert(chunk_addr);

    /* Set up user data for B-tree callback */
    memset(&udata, 0, sizeof(udata));
    udata.common.layout  = &dset->shared->layout.u.chunk;
    udata.common.storage = sc;
    udata.chunk_addr     = chunk_addr;

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = sc;

    /* Iterate over chunks to build mapping of chunk addresses */
    if ((sc->ops->iterate)(&idx_info, H5D__chunk_addrmap_cb, &udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                    "unable to iterate over chunk index to build address map");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_addrmap() */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_delete
 *
 * Purpose:    Delete raw data storage for entire dataset (i.e. all chunks)
 *
 * Return:    Success:    Non-negative
 *        Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_delete(H5F_t *f, H5O_t *oh, H5O_storage_t *storage)
{
    H5D_chk_idx_info_t idx_info;            /* Chunked index info */
    H5O_layout_t       layout;              /* Dataset layout  message */
    bool               layout_read = false; /* Whether the layout message was read from the file */
    H5O_pline_t        pline;               /* I/O pipeline message */
    bool               pline_read = false;  /* Whether the I/O pipeline message was read from the file */
    htri_t             exists;              /* Flag if header message of interest exists */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);
    assert(oh);
    assert(storage);
    H5D_CHUNK_STORAGE_INDEX_CHK(&storage->u.chunk);

    /* Check for I/O pipeline message */
    if ((exists = H5O_msg_exists_oh(oh, H5O_PLINE_ID)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to check for object header message");
    else if (exists) {
        if (NULL == H5O_msg_read_oh(f, oh, H5O_PLINE_ID, &pline))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get I/O pipeline message");
        pline_read = true;
    } /* end else if */
    else
        memset(&pline, 0, sizeof(pline));

    /* Retrieve dataset layout message */
    if ((exists = H5O_msg_exists_oh(oh, H5O_LAYOUT_ID)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to check for object header message");
    else if (exists) {
        if (NULL == H5O_msg_read_oh(f, oh, H5O_LAYOUT_ID, &layout))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get layout message");
        layout_read = true;
    } /* end else if */
    else
        HGOTO_ERROR(H5E_DATASET, H5E_NOTFOUND, FAIL, "can't find layout message");

    /* Compose chunked index info struct */
    idx_info.f       = f;
    idx_info.pline   = &pline;
    idx_info.layout  = &layout.u.chunk;
    idx_info.storage = &storage->u.chunk;

    /* Delete the chunked storage information in the file */
    if ((storage->u.chunk.ops->idx_delete)(&idx_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDELETE, FAIL, "unable to delete chunk index");

done:
    /* Clean up any messages read in */
    if (pline_read)
        if (H5O_msg_reset(H5O_PLINE_ID, &pline) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTRESET, FAIL, "unable to reset I/O pipeline message");
    if (layout_read)
        if (H5O_msg_reset(H5O_LAYOUT_ID, &layout) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTRESET, FAIL, "unable to reset layout message");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_update_cache
 *
 * Purpose:    Update any cached chunks index values after the dataspace
 *              size has changed
 *
 * Return:    Success:    Non-negative
 *        Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_update_cache(H5D_t *dset)
{
    H5D_rdcc_t     *rdcc = &(dset->shared->cache.chunk); /*raw data chunk cache */
    H5D_rdcc_ent_t *ent, *next;                          /*cache entry  */
    H5D_rdcc_ent_t  tmp_head;                            /* Sentinel entry for temporary entry list */
    H5D_rdcc_ent_t *tmp_tail;                            /* Tail pointer for temporary entry list */
    herr_t          ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dset && H5D_CHUNKED == dset->shared->layout.type);
    assert(dset->shared->layout.u.chunk.ndims > 0 && dset->shared->layout.u.chunk.ndims <= H5O_LAYOUT_NDIMS);

    /* Check the rank */
    assert((dset->shared->layout.u.chunk.ndims - 1) > 1);

    /* Add temporary entry list to rdcc */
    (void)memset(&tmp_head, 0, sizeof(tmp_head));
    rdcc->tmp_head = &tmp_head;
    tmp_tail       = &tmp_head;

    /* Recompute the index for each cached chunk that is in a dataset */
    for (ent = rdcc->head; ent; ent = next) {
        unsigned old_idx; /* Previous index number    */

        /* Get the pointer to the next cache entry */
        next = ent->next;

        /* Compute the index for the chunk entry */
        old_idx  = ent->idx; /* Save for later */
        ent->idx = H5D__chunk_hash_val(dset->shared, ent->scaled);

        if (old_idx != ent->idx) {
            H5D_rdcc_ent_t *old_ent; /* Old cache entry  */

            /* Check if there is already a chunk at this chunk's new location */
            old_ent = rdcc->slot[ent->idx];
            if (old_ent != NULL) {
                assert(old_ent->locked == false);
                assert(old_ent->deleted == false);

                /* Insert the old entry into the temporary list, but do not
                 * evict (yet).  Make sure we do not make any calls to the index
                 * until all chunks have updated indices! */
                assert(!old_ent->tmp_next);
                assert(!old_ent->tmp_prev);
                tmp_tail->tmp_next = old_ent;
                old_ent->tmp_prev  = tmp_tail;
                tmp_tail           = old_ent;
            } /* end if */

            /* Insert this chunk into correct location in hash table */
            rdcc->slot[ent->idx] = ent;

            /* If this chunk was previously on the temporary list and therefore
             * not in the hash table, remove it from the temporary list.
             * Otherwise clear the old hash table slot. */
            if (ent->tmp_prev) {
                assert(tmp_head.tmp_next);
                assert(tmp_tail != &tmp_head);
                ent->tmp_prev->tmp_next = ent->tmp_next;
                if (ent->tmp_next) {
                    ent->tmp_next->tmp_prev = ent->tmp_prev;
                    ent->tmp_next           = NULL;
                } /* end if */
                else {
                    assert(tmp_tail == ent);
                    tmp_tail = ent->tmp_prev;
                } /* end else */
                ent->tmp_prev = NULL;
            } /* end if */
            else
                rdcc->slot[old_idx] = NULL;
        } /* end if */
    }     /* end for */

    /* tmp_tail is no longer needed, and will be invalidated by
     * H5D_chunk_cache_evict anyways. */
    tmp_tail = NULL;

    /* Evict chunks that are still on the temporary list */
    while (tmp_head.tmp_next) {
        ent = tmp_head.tmp_next;

        /* Remove the old entry from the cache */
        if (H5D__chunk_cache_evict(dset, ent, true) < 0)
            HGOTO_ERROR(H5E_IO, H5E_CANTFLUSH, FAIL, "unable to flush one or more raw data chunks");
    } /* end while */

done:
    /* Remove temporary list from rdcc */
    rdcc->tmp_head = NULL;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_update_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_copy_cb
 *
 * Purpose:     Copy chunked raw data from source file and insert to the
 *              index in the destination file
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__chunk_copy_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata)
{
    H5D_chunk_it_ud3_t *udata = (H5D_chunk_it_ud3_t *)_udata; /* User data for callback */
    H5D_chunk_ud_t      udata_dst;                            /* User data about new destination chunk */
    bool                is_vlen     = false;                  /* Whether datatype is variable-length */
    bool                fix_ref     = false; /* Whether to fix up references in the dest. file */
    bool                need_insert = false; /* Whether the chunk needs to be inserted into the index */

    /* General information about chunk copy */
    void              *bkg      = udata->bkg;      /* Background buffer for datatype conversion */
    void              *buf      = udata->buf;      /* Chunk buffer for I/O & datatype conversions */
    size_t             buf_size = udata->buf_size; /* Size of chunk buffer */
    const H5O_pline_t *pline    = udata->pline;    /* I/O pipeline for applying filters */

    /* needed for compressed variable length data */
    bool     must_filter = false;      /* Whether chunk must be filtered during copy */
    size_t   nbytes;                   /* Size of chunk in file (in bytes) */
    H5Z_cb_t filter_cb;                /* Filter failure callback struct */
    int      ret_value = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get 'size_t' local value for number of bytes in chunk */
    H5_CHECKED_ASSIGN(nbytes, size_t, chunk_rec->nbytes, uint32_t);

    /* Initialize the filter callback struct */
    filter_cb.op_data = NULL;
    filter_cb.func    = NULL; /* no callback function when failed */

    /* Check for filtered chunks */
    /* Check for an edge chunk that is not filtered */
    if (pline && pline->nused) {
        must_filter = true;
        if ((udata->common.layout->flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS) &&
            H5D__chunk_is_partial_edge_chunk(udata->dset_ndims, udata->common.layout->dim, chunk_rec->scaled,
                                             udata->dset_dims))
            must_filter = false;
    }

    /* Check parameter for type conversion */
    if (udata->do_convert) {
        if (H5T_detect_class(udata->dt_src, H5T_VLEN, false) > 0)
            is_vlen = true;
        else if ((H5T_get_class(udata->dt_src, false) == H5T_REFERENCE) &&
                 (udata->file_src != udata->idx_info_dst->f))
            fix_ref = true;
        else
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, H5_ITER_ERROR, "unable to copy dataset elements");
    } /* end if */

    /* Resize the buf if it is too small to hold the data */
    if (nbytes > buf_size) {
        void *new_buf; /* New buffer for data */

        /* Re-allocate memory for copying the chunk */
        if (NULL == (new_buf = H5MM_realloc(udata->buf, nbytes)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, H5_ITER_ERROR,
                        "memory allocation failed for raw data chunk");
        udata->buf = new_buf;
        if (udata->bkg) {
            if (NULL == (new_buf = H5MM_realloc(udata->bkg, nbytes)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, H5_ITER_ERROR,
                            "memory allocation failed for raw data chunk");
            udata->bkg = new_buf;
            if (!udata->cpy_info->expand_ref)
                memset((uint8_t *)udata->bkg + buf_size, 0, (size_t)(nbytes - buf_size));

            bkg = udata->bkg;
        } /* end if */

        buf             = udata->buf;
        udata->buf_size = buf_size = nbytes;
    } /* end if */

    if (udata->chunk_in_cache && udata->chunk) {
        assert(!H5_addr_defined(chunk_rec->chunk_addr));
        H5MM_memcpy(buf, udata->chunk, nbytes);
        udata->chunk = NULL;
    }
    else {
        H5D_rdcc_ent_t *ent = NULL; /* Cache entry */
        unsigned        idx;        /* Index of chunk in cache, if present */
        unsigned        u;          /* Counter */
        H5D_shared_t   *shared_fo = (H5D_shared_t *)udata->cpy_info->shared_fo;

        /* See if the written chunk is in the chunk cache */
        if (shared_fo && shared_fo->cache.chunk.nslots > 0) {
            /* Determine the chunk's location in the hash table */
            idx = H5D__chunk_hash_val(shared_fo, chunk_rec->scaled);

            /* Get the chunk cache entry for that location */
            ent = shared_fo->cache.chunk.slot[idx];
            if (ent) {
                /* Speculatively set the 'found' flag */
                udata->chunk_in_cache = true;

                /* Verify that the cache entry is the correct chunk */
                for (u = 0; u < shared_fo->ndims; u++)
                    if (chunk_rec->scaled[u] != ent->scaled[u]) {
                        udata->chunk_in_cache = false;
                        break;
                    } /* end if */
            }         /* end if */
        }             /* end if */

        if (udata->chunk_in_cache) {

            if (NULL == ent)
                HGOTO_ERROR(H5E_IO, H5E_BADVALUE, H5_ITER_ERROR, "NULL chunk entry pointer");

            assert(H5_addr_defined(chunk_rec->chunk_addr));
            assert(H5_addr_defined(ent->chunk_block.offset));

            H5_CHECKED_ASSIGN(nbytes, size_t, shared_fo->layout.u.chunk.size, uint32_t);
            H5MM_memcpy(buf, ent->chunk, nbytes);
        }
        else {
            /* read chunk data from the source file */
            if (H5F_block_read(udata->file_src, H5FD_MEM_DRAW, chunk_rec->chunk_addr, nbytes, buf) < 0)
                HGOTO_ERROR(H5E_IO, H5E_READERROR, H5_ITER_ERROR, "unable to read raw data chunk");
        }
    }

    /* Need to uncompress filtered variable-length & reference data elements that are not found in chunk cache
     */
    if (must_filter && (is_vlen || fix_ref) && !udata->chunk_in_cache) {
        unsigned filter_mask = chunk_rec->filter_mask;

        if (H5Z_pipeline(pline, H5Z_FLAG_REVERSE, &filter_mask, H5Z_NO_EDC, filter_cb, &nbytes, &buf_size,
                         &buf) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, H5_ITER_ERROR, "data pipeline read failed");
    } /* end if */

    /* Perform datatype conversion, if necessary */
    if (is_vlen) {
        H5T_path_t *tpath_src_mem    = udata->tpath_src_mem;
        H5T_path_t *tpath_mem_dst    = udata->tpath_mem_dst;
        H5S_t      *buf_space        = udata->buf_space;
        hid_t       tid_src          = udata->tid_src;
        hid_t       tid_dst          = udata->tid_dst;
        hid_t       tid_mem          = udata->tid_mem;
        void       *reclaim_buf      = udata->reclaim_buf;
        size_t      reclaim_buf_size = udata->reclaim_buf_size;

        /* Convert from source file to memory */
        H5_CHECK_OVERFLOW(udata->nelmts, uint32_t, size_t);
        if (H5T_convert(tpath_src_mem, tid_src, tid_mem, (size_t)udata->nelmts, (size_t)0, (size_t)0, buf,
                        bkg) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, H5_ITER_ERROR, "datatype conversion failed");

        /* Copy into another buffer, to reclaim memory later */
        H5MM_memcpy(reclaim_buf, buf, reclaim_buf_size);

        /* Set background buffer to all zeros */
        memset(bkg, 0, buf_size);

        /* Convert from memory to destination file */
        if (H5T_convert(tpath_mem_dst, tid_mem, tid_dst, udata->nelmts, (size_t)0, (size_t)0, buf, bkg) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, H5_ITER_ERROR, "datatype conversion failed");

        /* Reclaim space from variable length data */
        if (H5T_reclaim(tid_mem, buf_space, reclaim_buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADITER, H5_ITER_ERROR, "unable to reclaim variable-length data");
    } /* end if */
    else if (fix_ref) {
        /* Check for expanding references */
        /* (background buffer has already been zeroed out, if not expanding) */
        if (udata->cpy_info->expand_ref) {
            /* Copy the reference elements */
            if (H5O_copy_expand_ref(udata->file_src, udata->tid_src, udata->dt_src, buf, nbytes,
                                    udata->idx_info_dst->f, bkg, udata->cpy_info) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, H5_ITER_ERROR, "unable to copy reference attribute");
        } /* end if */

        /* After fix ref, copy the new reference elements to the buffer to write out */
        H5MM_memcpy(buf, bkg, buf_size);
    } /* end if */

    /* Set up destination chunk callback information for insertion */
    udata_dst.common.layout      = udata->idx_info_dst->layout;
    udata_dst.common.storage     = udata->idx_info_dst->storage;
    udata_dst.common.scaled      = chunk_rec->scaled;
    udata_dst.chunk_block.offset = HADDR_UNDEF;
    udata_dst.chunk_block.length = chunk_rec->nbytes;
    udata_dst.filter_mask        = chunk_rec->filter_mask;

    /* Need to compress variable-length or reference data elements or a chunk found in cache before writing to
     * file */
    if (must_filter && (is_vlen || fix_ref || udata->chunk_in_cache)) {
        if (H5Z_pipeline(pline, 0, &(udata_dst.filter_mask), H5Z_NO_EDC, filter_cb, &nbytes, &buf_size,
                         &buf) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, H5_ITER_ERROR, "output pipeline failed");
#if H5_SIZEOF_SIZE_T > 4
        /* Check for the chunk expanding too much to encode in a 32-bit value */
        if (nbytes > ((size_t)0xffffffff))
            HGOTO_ERROR(H5E_DATASET, H5E_BADRANGE, H5_ITER_ERROR, "chunk too large for 32-bit length");
#endif /* H5_SIZEOF_SIZE_T > 4 */
        H5_CHECKED_ASSIGN(udata_dst.chunk_block.length, uint32_t, nbytes, size_t);
        udata->buf      = buf;
        udata->buf_size = buf_size;
    } /* end if */

    udata->chunk_in_cache = false;

    udata_dst.chunk_idx =
        H5VM_array_offset_pre(udata_dst.common.layout->ndims - 1, udata_dst.common.layout->max_down_chunks,
                              udata_dst.common.scaled);

    /* Allocate chunk in the file */
    if (H5D__chunk_file_alloc(udata->idx_info_dst, NULL, &udata_dst.chunk_block, &need_insert,
                              udata_dst.common.scaled) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "unable to insert/resize chunk on chunk level");

    /* Write chunk data to destination file */
    assert(H5_addr_defined(udata_dst.chunk_block.offset));
    if (H5F_block_write(udata->idx_info_dst->f, H5FD_MEM_DRAW, udata_dst.chunk_block.offset, nbytes, buf) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, H5_ITER_ERROR, "unable to write raw data to file");

    /* Set metadata tag in API context */
    H5_BEGIN_TAG(H5AC__COPIED_TAG)

    /* Insert chunk record into index */
    if (need_insert && udata->idx_info_dst->storage->ops->insert)
        if ((udata->idx_info_dst->storage->ops->insert)(udata->idx_info_dst, &udata_dst, NULL) < 0)
            HGOTO_ERROR_TAG(H5E_DATASET, H5E_CANTINSERT, H5_ITER_ERROR,
                            "unable to insert chunk addr into index");

    /* Reset metadata tag in API context */
    H5_END_TAG

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_copy_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_copy
 *
 * Purpose:    Copy chunked storage from SRC file to DST file.
 *
 * Return:    Success:    Non-negative
 *        Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_copy(H5F_t *f_src, H5O_storage_chunk_t *storage_src, H5O_layout_chunk_t *layout_src, H5F_t *f_dst,
                H5O_storage_chunk_t *storage_dst, const H5S_extent_t *ds_extent_src, const H5T_t *dt_src,
                const H5O_pline_t *pline_src, H5O_copy_t *cpy_info)
{
    H5D_chunk_it_ud3_t udata;                                       /* User data for iteration callback */
    H5D_chk_idx_info_t idx_info_dst;                                /* Dest. chunked index info */
    H5D_chk_idx_info_t idx_info_src;                                /* Source chunked index info */
    int                sndims;                                      /* Rank of dataspace */
    hsize_t            curr_dims[H5O_LAYOUT_NDIMS];                 /* Curr. size of dataset dimensions */
    hsize_t            max_dims[H5O_LAYOUT_NDIMS];                  /* Curr. size of dataset dimensions */
    H5O_pline_t        _pline;                                      /* Temporary pipeline info */
    const H5O_pline_t *pline;                                       /* Pointer to pipeline info to use */
    H5T_path_t        *tpath_src_mem = NULL, *tpath_mem_dst = NULL; /* Datatype conversion paths */
    hid_t              tid_src = -1;                                /* Datatype ID for source datatype */
    hid_t              tid_dst = -1;                                /* Datatype ID for destination datatype */
    hid_t              tid_mem = -1;                                /* Datatype ID for memory datatype */
    size_t             buf_size;                                    /* Size of copy buffer */
    size_t             reclaim_buf_size;                            /* Size of reclaim buffer */
    void              *buf             = NULL;                      /* Buffer for copying data */
    void              *bkg             = NULL;    /* Buffer for background during type conversion */
    void              *reclaim_buf     = NULL;    /* Buffer for reclaiming data */
    H5S_t             *buf_space       = NULL;    /* Dataspace describing buffer */
    hid_t              sid_buf         = -1;      /* ID for buffer dataspace */
    uint32_t           nelmts          = 0;       /* Number of elements in buffer */
    bool               do_convert      = false;   /* Indicate that type conversions should be performed */
    bool               copy_setup_done = false;   /* Indicate that 'copy setup' is done */
    herr_t             ret_value       = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f_src);
    assert(storage_src);
    H5D_CHUNK_STORAGE_INDEX_CHK(storage_src);
    assert(layout_src);
    assert(f_dst);
    assert(storage_dst);
    H5D_CHUNK_STORAGE_INDEX_CHK(storage_dst);
    assert(ds_extent_src);
    assert(dt_src);

    /* Initialize the temporary pipeline info */
    if (NULL == pline_src) {
        memset(&_pline, 0, sizeof(_pline));
        pline = &_pline;
    } /* end if */
    else
        pline = pline_src;

    /* Layout is not created in the destination file, reset index address */
    if (H5D_chunk_idx_reset(storage_dst, true) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to reset chunked storage index in dest");

    /* Initialize layout information */
    {
        unsigned ndims; /* Rank of dataspace */

        /* Get the dim info for dataset */
        if ((sndims = H5S_extent_get_dims(ds_extent_src, curr_dims, max_dims)) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataspace dimensions");
        H5_CHECKED_ASSIGN(ndims, unsigned, sndims, int);

        /* Set the source layout chunk information */
        if (H5D__chunk_set_info_real(layout_src, ndims, curr_dims, max_dims) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set layout's chunk info");
    } /* end block */

    /* Compose source & dest chunked index info structs */
    idx_info_src.f       = f_src;
    idx_info_src.pline   = pline;
    idx_info_src.layout  = layout_src;
    idx_info_src.storage = storage_src;

    idx_info_dst.f       = f_dst;
    idx_info_dst.pline   = pline; /* Use same I/O filter pipeline for dest. */
    idx_info_dst.layout  = layout_src /* Use same layout for dest. */;
    idx_info_dst.storage = storage_dst;

    /* Call the index-specific "copy setup" routine */
    if ((storage_src->ops->copy_setup)(&idx_info_src, &idx_info_dst) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                    "unable to set up index-specific chunk copying information");
    copy_setup_done = true;

    /* Create datatype ID for src datatype */
    if ((tid_src = H5I_register(H5I_DATATYPE, dt_src, false)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL, "unable to register source file datatype");

    /* If there's a VLEN source datatype, set up type conversion information */
    if (H5T_detect_class(dt_src, H5T_VLEN, false) > 0) {
        H5T_t   *dt_dst;      /* Destination datatype */
        H5T_t   *dt_mem;      /* Memory datatype */
        size_t   mem_dt_size; /* Memory datatype size */
        size_t   tmp_dt_size; /* Temp. datatype size */
        size_t   max_dt_size; /* Max atatype size */
        hsize_t  buf_dim;     /* Dimension for buffer */
        unsigned u;

        /* create a memory copy of the variable-length datatype */
        if (NULL == (dt_mem = H5T_copy(dt_src, H5T_COPY_TRANSIENT)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to copy");
        if ((tid_mem = H5I_register(H5I_DATATYPE, dt_mem, false)) < 0) {
            (void)H5T_close_real(dt_mem);
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL, "unable to register memory datatype");
        } /* end if */

        /* create variable-length datatype at the destination file */
        if (NULL == (dt_dst = H5T_copy(dt_src, H5T_COPY_TRANSIENT)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to copy");
        if (H5T_set_loc(dt_dst, H5F_VOL_OBJ(f_dst), H5T_LOC_DISK) < 0) {
            (void)H5T_close_real(dt_dst);
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "cannot mark datatype on disk");
        } /* end if */
        if ((tid_dst = H5I_register(H5I_DATATYPE, dt_dst, false)) < 0) {
            (void)H5T_close_real(dt_dst);
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL, "unable to register destination file datatype");
        } /* end if */

        /* Set up the conversion functions */
        if (NULL == (tpath_src_mem = H5T_path_find(dt_src, dt_mem)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to convert between src and mem datatypes");
        if (NULL == (tpath_mem_dst = H5T_path_find(dt_mem, dt_dst)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to convert between mem and dst datatypes");

        /* Determine largest datatype size */
        if (0 == (max_dt_size = H5T_get_size(dt_src)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to determine datatype size");
        if (0 == (mem_dt_size = H5T_get_size(dt_mem)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to determine datatype size");
        max_dt_size = MAX(max_dt_size, mem_dt_size);
        if (0 == (tmp_dt_size = H5T_get_size(dt_dst)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to determine datatype size");
        max_dt_size = MAX(max_dt_size, tmp_dt_size);

        /* Compute the number of elements per chunk */
        nelmts = 1;
        for (u = 0; u < (layout_src->ndims - 1); u++)
            nelmts *= layout_src->dim[u];

        /* Create the space and set the initial extent */
        buf_dim = nelmts;
        if (NULL == (buf_space = H5S_create_simple((unsigned)1, &buf_dim, NULL)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "can't create simple dataspace");

        /* Register */
        if ((sid_buf = H5I_register(H5I_DATASPACE, buf_space, false)) < 0) {
            (void)H5S_close(buf_space);
            HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register dataspace ID");
        } /* end if */

        /* Set initial buffer sizes */
        buf_size         = nelmts * max_dt_size;
        reclaim_buf_size = nelmts * mem_dt_size;

        /* Allocate memory for reclaim buf */
        if (NULL == (reclaim_buf = H5MM_malloc(reclaim_buf_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for raw data chunk");

        /* Indicate that type conversion should be performed */
        do_convert = true;
    } /* end if */
    else {
        if (H5T_get_class(dt_src, false) == H5T_REFERENCE) {
            /* Indicate that type conversion should be performed */
            do_convert = true;
        } /* end if */

        H5_CHECKED_ASSIGN(buf_size, size_t, layout_src->size, uint32_t);
        reclaim_buf_size = 0;
    } /* end else */

    /* Set up conversion buffer, if appropriate */
    if (do_convert) {
        /* Allocate background memory for converting the chunk */
        if (NULL == (bkg = H5MM_malloc(buf_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for raw data chunk");

        /* Check for reference datatype and no expanding references & clear background buffer */
        if (!cpy_info->expand_ref && ((H5T_get_class(dt_src, false) == H5T_REFERENCE) && (f_src != f_dst)))
            /* Reset value to zero */
            memset(bkg, 0, buf_size);
    } /* end if */

    /* Allocate memory for copying the chunk */
    if (NULL == (buf = H5MM_malloc(buf_size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for raw data chunk");

    /* Initialize the callback structure for the source */
    memset(&udata, 0, sizeof udata);
    udata.common.layout    = layout_src;
    udata.common.storage   = storage_src;
    udata.file_src         = f_src;
    udata.idx_info_dst     = &idx_info_dst;
    udata.buf              = buf;
    udata.bkg              = bkg;
    udata.buf_size         = buf_size;
    udata.tid_src          = tid_src;
    udata.tid_mem          = tid_mem;
    udata.tid_dst          = tid_dst;
    udata.dt_src           = dt_src;
    udata.do_convert       = do_convert;
    udata.tpath_src_mem    = tpath_src_mem;
    udata.tpath_mem_dst    = tpath_mem_dst;
    udata.reclaim_buf      = reclaim_buf;
    udata.reclaim_buf_size = reclaim_buf_size;
    udata.buf_space        = buf_space;
    udata.nelmts           = nelmts;
    udata.pline            = pline;
    udata.dset_ndims       = (unsigned)sndims;
    udata.dset_dims        = curr_dims;
    udata.cpy_info         = cpy_info;
    udata.chunk_in_cache   = false;
    udata.chunk            = NULL;

    /* Iterate over chunks to copy data */
    if ((storage_src->ops->iterate)(&idx_info_src, H5D__chunk_copy_cb, &udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "unable to iterate over chunk index to copy data");

    /* Iterate over the chunk cache to copy data for chunks with undefined address */
    if (udata.cpy_info->shared_fo) {
        H5D_rdcc_ent_t *ent, *next;
        H5D_chunk_rec_t chunk_rec;
        H5D_shared_t   *shared_fo = (H5D_shared_t *)udata.cpy_info->shared_fo;

        chunk_rec.nbytes      = layout_src->size;
        chunk_rec.filter_mask = 0;
        chunk_rec.chunk_addr  = HADDR_UNDEF;

        for (ent = shared_fo->cache.chunk.head; ent; ent = next) {
            if (!H5_addr_defined(ent->chunk_block.offset)) {
                H5MM_memcpy(chunk_rec.scaled, ent->scaled, sizeof(chunk_rec.scaled));
                udata.chunk          = ent->chunk;
                udata.chunk_in_cache = true;
                if (H5D__chunk_copy_cb(&chunk_rec, &udata) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy chunk data in cache");
            }
            next = ent->next;
        } /* end for */
    }

    /* I/O buffers may have been re-allocated */
    buf = udata.buf;
    bkg = udata.bkg;

done:
    if (sid_buf > 0 && H5I_dec_ref(sid_buf) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't decrement temporary dataspace ID");
    if (tid_src > 0 && H5I_dec_ref(tid_src) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't decrement temporary datatype ID");
    if (tid_dst > 0 && H5I_dec_ref(tid_dst) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't decrement temporary datatype ID");
    if (tid_mem > 0 && H5I_dec_ref(tid_mem) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't decrement temporary datatype ID");
    if (buf)
        H5MM_xfree(buf);
    if (bkg)
        H5MM_xfree(bkg);
    if (reclaim_buf)
        H5MM_xfree(reclaim_buf);

    /* Clean up any index information */
    if (copy_setup_done)
        if (storage_src->ops->copy_shutdown &&
            (storage_src->ops->copy_shutdown)(storage_src, storage_dst) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to shut down index copying info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_bh_info
 *
 * Purpose:     Retrieve the amount of index storage for chunked dataset
 *
 * Return:      Success:        Non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_bh_info(const H5O_loc_t *loc, H5O_t *oh, H5O_layout_t *layout, hsize_t *index_size)
{
    H5D_chk_idx_info_t   idx_info;     /* Chunked index info */
    H5S_t               *space = NULL; /* Dataset's dataspace */
    H5O_pline_t          pline;        /* I/O pipeline message */
    H5O_storage_chunk_t *sc = &(layout->storage.u.chunk);
    htri_t               exists;                  /* Flag if header message of interest exists */
    bool                 idx_info_init = false;   /* Whether the chunk index info has been initialized */
    bool                 pline_read    = false;   /* Whether the I/O pipeline message was read */
    herr_t               ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(loc);
    assert(loc->file);
    assert(H5_addr_defined(loc->addr));
    assert(layout);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);
    assert(index_size);

    /* Check for I/O pipeline message */
    if ((exists = H5O_msg_exists_oh(oh, H5O_PLINE_ID)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to read object header");
    else if (exists) {
        if (NULL == H5O_msg_read_oh(loc->file, oh, H5O_PLINE_ID, &pline))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't find I/O pipeline message");
        pline_read = true;
    } /* end else if */
    else
        memset(&pline, 0, sizeof(pline));

    /* Compose chunked index info struct */
    idx_info.f       = loc->file;
    idx_info.pline   = &pline;
    idx_info.layout  = &layout->u.chunk;
    idx_info.storage = sc;

    /* Get the dataspace for the dataset */
    if (NULL == (space = H5S_read(loc)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to load dataspace info from dataset header");

    /* Allocate any indexing structures */
    if (sc->ops->init && (sc->ops->init)(&idx_info, space, loc->addr) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize indexing information");
    idx_info_init = true;

    /* Get size of index structure */
    if (sc->ops->size && (sc->ops->size)(&idx_info, index_size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to retrieve chunk index info");

done:
    /* Free resources, if they've been initialized */
    if (idx_info_init && sc->ops->dest && (sc->ops->dest)(&idx_info) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to release chunk index info");
    if (pline_read && H5O_msg_reset(H5O_PLINE_ID, &pline) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRESET, FAIL, "unable to reset I/O pipeline message");
    if (space && H5S_close(space) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release dataspace");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_bh_info() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_dump_index_cb
 *
 * Purpose:    If the UDATA.STREAM member is non-null then debugging
 *              information is written to that stream.
 *
 * Return:    Success:    Non-negative
 *
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__chunk_dump_index_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata)
{
    H5D_chunk_it_ud4_t *udata = (H5D_chunk_it_ud4_t *)_udata; /* User data from caller */

    FUNC_ENTER_PACKAGE_NOERR

    if (udata->stream) {
        unsigned u; /* Local index variable */

        /* Print header if not already displayed */
        if (!udata->header_displayed) {
            fprintf(udata->stream, "           Flags    Bytes     Address          Logical Offset\n");
            fprintf(udata->stream, "        ========== ======== ========== ==============================\n");

            /* Set flag that the headers has been printed */
            udata->header_displayed = true;
        } /* end if */

        /* Print information about this chunk */
        fprintf(udata->stream, "        0x%08x %8" PRIu32 " %10" PRIuHADDR " [", chunk_rec->filter_mask,
                chunk_rec->nbytes, chunk_rec->chunk_addr);
        for (u = 0; u < udata->ndims; u++)
            fprintf(udata->stream, "%s%" PRIuHSIZE, (u ? ", " : ""),
                    (chunk_rec->scaled[u] * udata->chunk_dim[u]));
        fputs("]\n", udata->stream);
    } /* end if */

    FUNC_LEAVE_NOAPI(H5_ITER_CONT)
} /* H5D__chunk_dump_index_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_dump_index
 *
 * Purpose:    Prints information about the storage index to the specified
 *        stream.
 *
 * Return:    Success:    Non-negative
 *        Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_dump_index(H5D_t *dset, FILE *stream)
{
    H5O_storage_chunk_t *sc        = &(dset->shared->layout.storage.u.chunk);
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);
    H5D_CHUNK_STORAGE_INDEX_CHK(sc);

    /* Only display info if stream is defined */
    if (stream) {
        H5D_chk_idx_info_t idx_info; /* Chunked index info */
        H5D_chunk_it_ud4_t udata;    /* User data for callback */

        /* Display info for index */
        if ((sc->ops->dump)(sc, stream) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "unable to dump chunk index info");

        /* Compose chunked index info struct */
        idx_info.f       = dset->oloc.file;
        idx_info.pline   = &dset->shared->dcpl_cache.pline;
        idx_info.layout  = &dset->shared->layout.u.chunk;
        idx_info.storage = sc;

        /* Set up user data for callback */
        udata.stream           = stream;
        udata.header_displayed = false;
        udata.ndims            = dset->shared->layout.u.chunk.ndims;
        udata.chunk_dim        = dset->shared->layout.u.chunk.dim;

        /* Iterate over index and dump chunk info */
        if ((sc->ops->iterate)(&idx_info, H5D__chunk_dump_index_cb, &udata) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL,
                        "unable to iterate over chunk index to dump chunk info");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_dump_index() */

#ifdef H5D_CHUNK_DEBUG

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_stats
 *
 * Purpose:    Print raw data cache statistics to the debug stream.  If
 *        HEADERS is non-zero then print table column headers,
 *        otherwise assume that the H5AC layer has already printed them.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_stats(const H5D_t *dset, bool headers)
{
    H5D_rdcc_t *rdcc = &(dset->shared->cache.chunk);
    double      miss_rate;
    char        ascii[32];
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    if (!H5DEBUG(AC))
        HGOTO_DONE(SUCCEED);

    if (headers) {
        fprintf(H5DEBUG(AC), "H5D: raw data cache statistics\n");
        fprintf(H5DEBUG(AC), "   %-18s %8s %8s %8s %8s+%-8s\n", "Layer", "Hits", "Misses", "MissRate",
                "Inits", "Flushes");
        fprintf(H5DEBUG(AC), "   %-18s %8s %8s %8s %8s-%-8s\n", "-----", "----", "------", "--------",
                "-----", "-------");
    }

#ifdef H5AC_DEBUG
    if (H5DEBUG(AC))
        headers = true;
#endif

    if (headers) {
        if (rdcc->stats.nhits > 0 || rdcc->stats.nmisses > 0) {
            miss_rate = 100.0 * rdcc->stats.nmisses / (rdcc->stats.nhits + rdcc->stats.nmisses);
        }
        else {
            miss_rate = 0.0;
        }
        if (miss_rate > 100) {
            snprintf(ascii, sizeof(ascii), "%7d%%", (int)(miss_rate + 0.5));
        }
        else {
            snprintf(ascii, sizeof(ascii), "%7.2f%%", miss_rate);
        }

        fprintf(H5DEBUG(AC), "   %-18s %8u %8u %7s %8d+%-9ld\n", "raw data chunks", rdcc->stats.nhits,
                rdcc->stats.nmisses, ascii, rdcc->stats.ninits,
                (long)(rdcc->stats.nflushes) - (long)(rdcc->stats.ninits));
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_stats() */
#endif /* H5D_CHUNK_DEBUG */

/*-------------------------------------------------------------------------
 * Function:    H5D__nonexistent_readvv_cb
 *
 * Purpose:    Callback operation for performing fill value I/O operation
 *              on memory buffer.
 *
 * Note:    This algorithm is pretty inefficient about initializing and
 *              terminating the fill buffer info structure and it would be
 *              faster to refactor this into a "real" initialization routine,
 *              and a "vectorized fill" routine. -QAK
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__nonexistent_readvv_cb(hsize_t H5_ATTR_UNUSED dst_off, hsize_t src_off, size_t len, void *_udata)
{
    H5D_chunk_readvv_ud_t *udata = (H5D_chunk_readvv_ud_t *)_udata; /* User data for H5VM_opvv() operator */
    H5D_fill_buf_info_t    fb_info;                                 /* Dataset's fill buffer info */
    bool                   fb_info_init = false;   /* Whether the fill value buffer has been initialized */
    herr_t                 ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Initialize the fill value buffer */
    if (H5D__fill_init(&fb_info, (udata->rbuf + src_off), NULL, NULL, NULL, NULL,
                       &udata->dset->shared->dcpl_cache.fill, udata->dset->shared->type,
                       udata->dset->shared->type_id, (size_t)0, len) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize fill buffer info");
    fb_info_init = true;

    /* Check for VL datatype & fill the buffer with VL datatype fill values */
    if (fb_info.has_vlen_fill_type && H5D__fill_refill_vl(&fb_info, fb_info.elmts_per_buf) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't refill fill value buffer");

done:
    /* Release the fill buffer info, if it's been initialized */
    if (fb_info_init && H5D__fill_term(&fb_info) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release fill buffer info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__nonexistent_readvv_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__nonexistent_readvv
 *
 * Purpose:    When the chunk doesn't exist on disk and the chunk is bigger
 *              than the cache size, performs fill value I/O operation on
 *              memory buffer, advancing through two I/O vectors, until one
 *              runs out.
 *
 * Note:    This algorithm is pretty inefficient about initializing and
 *              terminating the fill buffer info structure and it would be
 *              faster to refactor this into a "real" initialization routine,
 *              and a "vectorized fill" routine. -QAK
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5D__nonexistent_readvv(const H5D_io_info_t H5_ATTR_NDEBUG_UNUSED *io_info,
                        const H5D_dset_io_info_t *dset_info, size_t chunk_max_nseq, size_t *chunk_curr_seq,
                        size_t chunk_len_arr[], hsize_t chunk_off_arr[], size_t mem_max_nseq,
                        size_t *mem_curr_seq, size_t mem_len_arr[], hsize_t mem_off_arr[])
{
    H5D_chunk_readvv_ud_t udata;          /* User data for H5VM_opvv() operator */
    ssize_t               ret_value = -1; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(io_info);
    assert(chunk_curr_seq);
    assert(chunk_len_arr);
    assert(chunk_off_arr);
    assert(mem_curr_seq);
    assert(mem_len_arr);
    assert(mem_off_arr);

    /* Set up user data for H5VM_opvv() */
    udata.rbuf = (unsigned char *)dset_info->buf.vp;
    udata.dset = dset_info->dset;

    /* Call generic sequence operation routine */
    if ((ret_value = H5VM_opvv(chunk_max_nseq, chunk_curr_seq, chunk_len_arr, chunk_off_arr, mem_max_nseq,
                               mem_curr_seq, mem_len_arr, mem_off_arr, H5D__nonexistent_readvv_cb, &udata)) <
        0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPERATE, FAIL, "can't perform vectorized fill value init");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__nonexistent_readvv() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_is_partial_edge_chunk
 *
 * Purpose:     Checks to see if the chunk is a partial edge chunk.
 *              Either dset or (dset_dims and dset_ndims) must be
 *              provided.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
bool
H5D__chunk_is_partial_edge_chunk(unsigned dset_ndims, const uint32_t *chunk_dims, const hsize_t scaled[],
                                 const hsize_t *dset_dims)
{
    unsigned u;                 /* Local index variable */
    bool     ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(scaled);
    assert(dset_ndims > 0);
    assert(dset_dims);
    assert(chunk_dims);

    /* check if this is a partial edge chunk */
    for (u = 0; u < dset_ndims; u++)
        if (((scaled[u] + 1) * chunk_dims[u]) > dset_dims[u])
            HGOTO_DONE(true);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_is_partial_edge_chunk() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_file_alloc()
 *
 * Purpose:     Chunk allocation:
 *          Create the chunk if it doesn't exist, or reallocate the
 *                chunk if its size changed.
 *          The coding is moved and modified from each index structure.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_file_alloc(const H5D_chk_idx_info_t *idx_info, const H5F_block_t *old_chunk,
                      H5F_block_t *new_chunk, bool *need_insert, const hsize_t *scaled)
{
    bool   alloc_chunk = false;   /* Whether to allocate chunk */
    herr_t ret_value   = SUCCEED; /* Return value         */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(new_chunk);
    assert(need_insert);

    *need_insert = false;

    /* Check for filters on chunks */
    if (idx_info->pline->nused > 0) {
        /* Sanity/error checking block */
        assert(idx_info->storage->idx_type != H5D_CHUNK_IDX_NONE);
        {
            unsigned allow_chunk_size_len; /* Allowed size of encoded chunk size */
            unsigned new_chunk_size_len;   /* Size of encoded chunk size */

            /* Compute the size required for encoding the size of a chunk, allowing
             * for an extra byte, in case the filter makes the chunk larger.
             */
            allow_chunk_size_len = 1 + ((H5VM_log2_gen((uint64_t)(idx_info->layout->size)) + 8) / 8);
            if (allow_chunk_size_len > 8)
                allow_chunk_size_len = 8;

            /* Compute encoded size of chunk */
            new_chunk_size_len = (H5VM_log2_gen((uint64_t)(new_chunk->length)) + 8) / 8;
            if (new_chunk_size_len > 8)
                HGOTO_ERROR(H5E_DATASET, H5E_BADRANGE, FAIL, "encoded chunk size is more than 8 bytes?!?");

            /* Check if the chunk became too large to be encoded */
            if (new_chunk_size_len > allow_chunk_size_len)
                HGOTO_ERROR(H5E_DATASET, H5E_BADRANGE, FAIL, "chunk size can't be encoded");
        } /* end block */

        if (old_chunk && H5_addr_defined(old_chunk->offset)) {
            /* Sanity check */
            assert(!H5_addr_defined(new_chunk->offset) || H5_addr_eq(new_chunk->offset, old_chunk->offset));

            /* Check for chunk being same size */
            if (new_chunk->length != old_chunk->length) {
                /* Release previous chunk */
                /* Only free the old location if not doing SWMR writes - otherwise
                 * we must keep the old chunk around in case a reader has an
                 * outdated version of the B-tree node
                 */
                if (!(H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE))
                    if (H5MF_xfree(idx_info->f, H5FD_MEM_DRAW, old_chunk->offset, old_chunk->length) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to free chunk");
                alloc_chunk = true;
            } /* end if */
            else {
                /* Don't need to reallocate chunk, but send its address back up */
                if (!H5_addr_defined(new_chunk->offset))
                    new_chunk->offset = old_chunk->offset;
            } /* end else */
        }     /* end if */
        else {
            assert(!H5_addr_defined(new_chunk->offset));
            alloc_chunk = true;
        } /* end else */
    }     /* end if */
    else {
        assert(!H5_addr_defined(new_chunk->offset));
        assert(new_chunk->length == idx_info->layout->size);
        alloc_chunk = true;
    } /* end else */

    /* Actually allocate space for the chunk in the file */
    if (alloc_chunk) {
        switch (idx_info->storage->idx_type) {
            case H5D_CHUNK_IDX_NONE: {
                H5D_chunk_ud_t udata;

                udata.common.scaled = scaled;
                if ((idx_info->storage->ops->get_addr)(idx_info, &udata) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't query chunk address");
                new_chunk->offset = udata.chunk_block.offset;
                assert(new_chunk->length == udata.chunk_block.length);
                break;
            }

            case H5D_CHUNK_IDX_EARRAY:
            case H5D_CHUNK_IDX_FARRAY:
            case H5D_CHUNK_IDX_BT2:
            case H5D_CHUNK_IDX_BTREE:
            case H5D_CHUNK_IDX_SINGLE:
                assert(new_chunk->length > 0);
                H5_CHECK_OVERFLOW(new_chunk->length, /*From: */ uint32_t, /*To: */ hsize_t);
                new_chunk->offset = H5MF_alloc(idx_info->f, H5FD_MEM_DRAW, (hsize_t)new_chunk->length);
                if (!H5_addr_defined(new_chunk->offset))
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "file allocation failed");
                *need_insert = true;
                break;

            case H5D_CHUNK_IDX_NTYPES:
            default:
                assert(0 && "This should never be executed!");
                break;
        } /* end switch */
    }     /* end if */

    assert(H5_addr_defined(new_chunk->offset));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_file_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_format_convert_cb
 *
 * Purpose:     Callback routine to insert chunk address into v1 B-tree
 *              chunk index.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__chunk_format_convert_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata)
{
    H5D_chunk_it_ud5_t *udata = (H5D_chunk_it_ud5_t *)_udata; /* User data */
    H5D_chk_idx_info_t *new_idx_info;                         /* The new chunk index information */
    H5D_chunk_ud_t      insert_udata;                         /* Chunk information to be inserted */
    haddr_t             chunk_addr;                           /* Chunk address */
    size_t              nbytes;                               /* Chunk size */
    void               *buf       = NULL;                     /* Pointer to buffer of chunk data */
    int                 ret_value = H5_ITER_CONT;             /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set up */
    new_idx_info = udata->new_idx_info;
    H5_CHECKED_ASSIGN(nbytes, size_t, chunk_rec->nbytes, uint32_t);
    chunk_addr = chunk_rec->chunk_addr;

    if (new_idx_info->pline->nused &&
        (new_idx_info->layout->flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS) &&
        (H5D__chunk_is_partial_edge_chunk(udata->dset_ndims, new_idx_info->layout->dim, chunk_rec->scaled,
                                          udata->dset_dims))) {

        /* This is a partial non-filtered edge chunk */
        /* Convert the chunk to a filtered edge chunk for v1 B-tree chunk index */

        unsigned filter_mask = chunk_rec->filter_mask;
        H5Z_cb_t filter_cb;          /* Filter failure callback struct */
        size_t   read_size = nbytes; /* Bytes to read */

        assert(read_size == new_idx_info->layout->size);

        /* Initialize the filter callback struct */
        filter_cb.op_data = NULL;
        filter_cb.func    = NULL; /* no callback function when failed */

        /* Allocate buffer for chunk data */
        if (NULL == (buf = H5MM_malloc(read_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, H5_ITER_ERROR,
                        "memory allocation failed for raw data chunk");

        /* Read the non-filtered edge chunk */
        if (H5F_block_read(new_idx_info->f, H5FD_MEM_DRAW, chunk_addr, read_size, buf) < 0)
            HGOTO_ERROR(H5E_IO, H5E_READERROR, H5_ITER_ERROR, "unable to read raw data chunk");

        /* Pass the chunk through the pipeline */
        if (H5Z_pipeline(new_idx_info->pline, 0, &filter_mask, H5Z_NO_EDC, filter_cb, &nbytes, &read_size,
                         &buf) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, H5_ITER_ERROR, "output pipeline failed");

#if H5_SIZEOF_SIZE_T > 4
        /* Check for the chunk expanding too much to encode in a 32-bit value */
        if (nbytes > ((size_t)0xffffffff))
            HGOTO_ERROR(H5E_DATASET, H5E_BADRANGE, H5_ITER_ERROR, "chunk too large for 32-bit length");
#endif /* H5_SIZEOF_SIZE_T > 4 */

        /* Allocate space for the filtered chunk */
        if ((chunk_addr = H5MF_alloc(new_idx_info->f, H5FD_MEM_DRAW, (hsize_t)nbytes)) == HADDR_UNDEF)
            HGOTO_ERROR(H5E_DATASET, H5E_NOSPACE, H5_ITER_ERROR, "file allocation failed for filtered chunk");
        assert(H5_addr_defined(chunk_addr));

        /* Write the filtered chunk to disk */
        if (H5F_block_write(new_idx_info->f, H5FD_MEM_DRAW, chunk_addr, nbytes, buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, H5_ITER_ERROR, "unable to write raw data to file");
    } /* end if */

    /* Set up chunk information for insertion to chunk index */
    insert_udata.chunk_block.offset = chunk_addr;
    insert_udata.chunk_block.length = nbytes;
    insert_udata.filter_mask        = chunk_rec->filter_mask;
    insert_udata.common.scaled      = chunk_rec->scaled;
    insert_udata.common.layout      = new_idx_info->layout;
    insert_udata.common.storage     = new_idx_info->storage;

    /* Insert chunk into the v1 B-tree chunk index */
    if ((new_idx_info->storage->ops->insert)(new_idx_info, &insert_udata, NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, H5_ITER_ERROR, "unable to insert chunk addr into index");

done:
    if (buf)
        H5MM_xfree(buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__chunk_format_convert_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_format_convert
 *
 * Purpose:     Iterate over the chunks for the current chunk index and insert the
 *        the chunk addresses into v1 B-tree chunk index via callback.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_format_convert(H5D_t *dset, H5D_chk_idx_info_t *idx_info, H5D_chk_idx_info_t *new_idx_info)
{
    H5D_chunk_it_ud5_t udata;               /* User data */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dset);

    /* Set up user data */
    udata.new_idx_info = new_idx_info;
    udata.dset_ndims   = dset->shared->ndims;
    udata.dset_dims    = dset->shared->curr_dims;

    /* Iterate over the chunks in the current index and insert the chunk addresses into version 1 B-tree index
     */
    if ((idx_info->storage->ops->iterate)(idx_info, H5D__chunk_format_convert_cb, &udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "unable to iterate over chunk index to chunk info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_format_convert() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_index_empty_cb
 *
 * Purpose:     Callback function that simply stops iteration and sets the
 *              `empty` parameter to false if called. If this callback is
 *              entered, it means that the chunk index contains at least
 *              one chunk, so is not empty.
 *
 * Return:      H5_ITER_STOP
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__chunk_index_empty_cb(const H5D_chunk_rec_t H5_ATTR_UNUSED *chunk_rec, void *_udata)
{
    bool *empty     = (bool *)_udata;
    int   ret_value = H5_ITER_STOP;

    FUNC_ENTER_PACKAGE_NOERR

    *empty = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_index_empty_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_index_empty
 *
 * Purpose:     Determines whether a chunk index is empty (has no chunks
 *              inserted into it yet).
 *
 * Note:        This routine is meant to be a little more performant than
 *              just counting the number of chunks in the index. In the
 *              future, this is probably a callback that the chunk index
 *              ops structure should provide.
 *
 * Return:      Non-negative on Success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_index_empty(const H5D_t *dset, bool *empty)
{
    H5D_chk_idx_info_t idx_info;            /* Chunked index info */
    H5D_rdcc_ent_t    *ent;                 /* Cache entry  */
    const H5D_rdcc_t  *rdcc      = NULL;    /* Raw data chunk cache */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    assert(dset);
    assert(dset->shared);
    assert(empty);

    rdcc = &(dset->shared->cache.chunk); /* raw data chunk cache */
    assert(rdcc);

    /* Search for cached chunks that haven't been written out */
    for (ent = rdcc->head; ent; ent = ent->next)
        /* Flush the chunk out to disk, to make certain the size is correct later */
        if (H5D__chunk_flush_entry(dset, ent, false) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "cannot flush indexed storage buffer");

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = &dset->shared->layout.storage.u.chunk;

    *empty = true;

    if (H5_addr_defined(idx_info.storage->idx_addr)) {
        /* Iterate over the allocated chunks */
        if ((dset->shared->layout.storage.u.chunk.ops->iterate)(&idx_info, H5D__chunk_index_empty_cb, empty) <
            0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                        "unable to retrieve allocated chunk information from index");
    }

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__chunk_index_empty() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_num_chunks_cb
 *
 * Purpose:     Callback function that increments the number of written
 *              chunks in the dataset.
 *
 * Note:        Currently, this function only gets the number of all written
 *              chunks, regardless the dataspace.
 *
 * Return:      H5_ITER_CONT
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__get_num_chunks_cb(const H5D_chunk_rec_t H5_ATTR_UNUSED *chunk_rec, void *_udata)
{
    hsize_t *num_chunks = (hsize_t *)_udata;
    int      ret_value  = H5_ITER_CONT; /* Callback return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(num_chunks);

    (*num_chunks)++;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__get_num_chunks_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_num_chunks
 *
 * Purpose:     Gets the number of written chunks in a dataset.
 *
 * Note:        Currently, this function only gets the number of all written
 *              chunks, regardless the dataspace.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__get_num_chunks(const H5D_t *dset, const H5S_t H5_ATTR_UNUSED *space, hsize_t *nchunks)
{
    H5D_chk_idx_info_t idx_info;            /* Chunked index info */
    hsize_t            num_chunks = 0;      /* Number of written chunks */
    H5D_rdcc_ent_t    *ent;                 /* Cache entry  */
    const H5D_rdcc_t  *rdcc      = NULL;    /* Raw data chunk cache */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    assert(dset);
    assert(dset->shared);
    assert(space);
    assert(nchunks);

    rdcc = &(dset->shared->cache.chunk); /* raw data chunk cache */
    assert(rdcc);

    /* Search for cached chunks that haven't been written out */
    for (ent = rdcc->head; ent; ent = ent->next)
        /* Flush the chunk out to disk, to make certain the size is correct later */
        if (H5D__chunk_flush_entry(dset, ent, false) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "cannot flush indexed storage buffer");

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = &dset->shared->layout.storage.u.chunk;

    /* If the dataset is not written, number of chunks will be 0 */
    if (!H5_addr_defined(idx_info.storage->idx_addr))
        *nchunks = 0;
    else {
        /* Iterate over the allocated chunks */
        if ((dset->shared->layout.storage.u.chunk.ops->iterate)(&idx_info, H5D__get_num_chunks_cb,
                                                                &num_chunks) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                        "unable to retrieve allocated chunk information from index");
        *nchunks = num_chunks;
    }

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__get_num_chunks() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_chunk_info_cb
 *
 * Purpose:     Get the chunk info of the queried chunk, given by its index.
 *
 * Return:      Success:    H5_ITER_CONT or H5_ITER_STOP
 *                          H5_ITER_STOP indicates the queried chunk is found
 *              Failure:    Negative (H5_ITER_ERROR)
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__get_chunk_info_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata)
{
    H5D_chunk_info_iter_ud_t *chunk_info = (H5D_chunk_info_iter_ud_t *)_udata;
    int                       ret_value  = H5_ITER_CONT; /* Callback return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(chunk_rec);
    assert(chunk_info);

    /* If this is the queried chunk, retrieve its info and stop iterating */
    if (chunk_info->curr_idx == chunk_info->chunk_idx) {
        hsize_t ii = 0; /* Dimension index */

        /* Copy info */
        chunk_info->filter_mask = chunk_rec->filter_mask;
        chunk_info->chunk_addr  = chunk_rec->chunk_addr;
        chunk_info->nbytes      = chunk_rec->nbytes;
        for (ii = 0; ii < chunk_info->ndims; ii++)
            chunk_info->scaled[ii] = chunk_rec->scaled[ii];
        chunk_info->found = true;

        /* Stop iterating */
        ret_value = H5_ITER_STOP;
    }
    /* Go to the next chunk */
    else
        chunk_info->curr_idx++;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__get_chunk_info_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_chunk_info
 *
 * Purpose:     Iterate over the chunks in the dataset to get the info
 *              of the desired chunk.
 *
 * Note:        Currently, the domain of the index in this function is of all
 *              the written chunks, regardless the dataspace.
 *
 * Return:      Success: SUCCEED
 *              Failure: FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__get_chunk_info(const H5D_t *dset, const H5S_t H5_ATTR_UNUSED *space, hsize_t chk_index, hsize_t *offset,
                    unsigned *filter_mask, haddr_t *addr, hsize_t *size)
{
    H5D_chk_idx_info_t       idx_info;            /* Chunked index info */
    H5D_chunk_info_iter_ud_t udata;               /* User data for callback */
    const H5D_rdcc_t        *rdcc = NULL;         /* Raw data chunk cache */
    H5D_rdcc_ent_t          *ent;                 /* Cache entry index */
    hsize_t                  ii        = 0;       /* Dimension index */
    herr_t                   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    assert(dset);
    assert(dset->shared);
    assert(space);

    /* Get the raw data chunk cache */
    rdcc = &(dset->shared->cache.chunk);
    assert(rdcc);

    /* Search for cached chunks that haven't been written out */
    for (ent = rdcc->head; ent; ent = ent->next)
        /* Flush the chunk out to disk, to make certain the size is correct later */
        if (H5D__chunk_flush_entry(dset, ent, false) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "cannot flush indexed storage buffer");

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = &dset->shared->layout.storage.u.chunk;

    /* Set addr & size for when dset is not written or queried chunk is not found */
    if (addr)
        *addr = HADDR_UNDEF;
    if (size)
        *size = 0;

    /* If the chunk is written, get its info, otherwise, return without error */
    if (H5_addr_defined(idx_info.storage->idx_addr)) {
        /* Initialize before iteration */
        udata.chunk_idx   = chk_index;
        udata.curr_idx    = 0;
        udata.ndims       = dset->shared->ndims;
        udata.nbytes      = 0;
        udata.filter_mask = 0;
        udata.chunk_addr  = HADDR_UNDEF;
        udata.found       = false;

        /* Iterate over the allocated chunks */
        if ((dset->shared->layout.storage.u.chunk.ops->iterate)(&idx_info, H5D__get_chunk_info_cb, &udata) <
            0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                        "unable to retrieve allocated chunk information from index");

        /* Obtain requested info if the chunk is found */
        if (udata.found) {
            if (filter_mask)
                *filter_mask = udata.filter_mask;
            if (addr)
                *addr = udata.chunk_addr;
            if (size)
                *size = udata.nbytes;
            if (offset)
                for (ii = 0; ii < udata.ndims; ii++)
                    offset[ii] = udata.scaled[ii] * dset->shared->layout.u.chunk.dim[ii];
        } /* end if */
    }     /* end if H5_addr_defined */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__get_chunk_info() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_chunk_info_by_coord_cb
 *
 * Purpose:     Get the chunk info of the desired chunk, given its offset
 *              coordinates.
 *
 * Return:      Success:    H5_ITER_CONT or H5_ITER_STOP
 *              Failure:    Negative (H5_ITER_ERROR)
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__get_chunk_info_by_coord_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata)
{
    H5D_chunk_info_iter_ud_t *chunk_info = (H5D_chunk_info_iter_ud_t *)_udata;
    bool                      different  = false;       /* true when a scaled value pair mismatch */
    hsize_t                   ii;                       /* Local index value */
    int                       ret_value = H5_ITER_CONT; /* Callback return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(chunk_rec);
    assert(chunk_info);

    /* Going through the scaled, stop when a mismatch is found */
    for (ii = 0; ii < chunk_info->ndims && !different; ii++)
        if (chunk_info->scaled[ii] != chunk_rec->scaled[ii])
            different = true;

    /* Same scaled coords means the chunk is found, copy the chunk info */
    if (!different) {
        chunk_info->nbytes      = chunk_rec->nbytes;
        chunk_info->filter_mask = chunk_rec->filter_mask;
        chunk_info->chunk_addr  = chunk_rec->chunk_addr;
        chunk_info->found       = true;

        /* Stop iterating */
        ret_value = H5_ITER_STOP;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__get_chunk_info_by_coord_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_chunk_info_by_coord
 *
 * Purpose:     Iterate over the chunks in the dataset to get the info
 *              of the desired chunk, given by its offset coordinates.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__get_chunk_info_by_coord(const H5D_t *dset, const hsize_t *offset, unsigned *filter_mask, haddr_t *addr,
                             hsize_t *size)
{
    const H5O_layout_t      *layout = NULL;       /* Dataset layout */
    const H5D_rdcc_t        *rdcc   = NULL;       /* Raw data chunk cache */
    H5D_rdcc_ent_t          *ent;                 /* Cache entry index */
    H5D_chk_idx_info_t       idx_info;            /* Chunked index info */
    H5D_chunk_info_iter_ud_t udata;               /* User data for callback */
    herr_t                   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    /* Check args */
    assert(dset);
    assert(dset->shared);
    assert(offset);

    /* Get dataset layout and raw data chunk cache */
    layout = &(dset->shared->layout);
    rdcc   = &(dset->shared->cache.chunk);
    assert(layout);
    assert(rdcc);
    assert(H5D_CHUNKED == layout->type);

    /* Search for cached chunks that haven't been written out */
    for (ent = rdcc->head; ent; ent = ent->next)
        /* Flush the chunk out to disk, to make certain the size is correct later */
        if (H5D__chunk_flush_entry(dset, ent, false) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "cannot flush indexed storage buffer");

    /* Set addr & size for when dset is not written or queried chunk is not found */
    if (addr)
        *addr = HADDR_UNDEF;
    if (size)
        *size = 0;

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &dset->shared->layout.u.chunk;
    idx_info.storage = &dset->shared->layout.storage.u.chunk;

    /* If the dataset is not written, return without errors */
    if (H5_addr_defined(idx_info.storage->idx_addr)) {
        /* Calculate the scaled of this chunk */
        H5VM_chunk_scaled(dset->shared->ndims, offset, layout->u.chunk.dim, udata.scaled);
        udata.scaled[dset->shared->ndims] = 0;

        /* Initialize before iteration */
        udata.ndims       = dset->shared->ndims;
        udata.nbytes      = 0;
        udata.filter_mask = 0;
        udata.chunk_addr  = HADDR_UNDEF;
        udata.found       = false;

        /* Iterate over the allocated chunks to find the requested chunk */
        if ((dset->shared->layout.storage.u.chunk.ops->iterate)(&idx_info, H5D__get_chunk_info_by_coord_cb,
                                                                &udata) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                        "unable to retrieve information of the chunk by its scaled coordinates");

        /* Obtain requested info if the chunk is found */
        if (udata.found) {
            if (filter_mask)
                *filter_mask = udata.filter_mask;
            if (addr)
                *addr = udata.chunk_addr;
            if (size)
                *size = udata.nbytes;
        } /* end if */
    }     /* end if H5_addr_defined */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__get_chunk_info_by_coord() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_iter_cb
 *
 * Purpose:     Call the user-defined function with the chunk data. The iterator continues if
 *              the user-defined function returns H5_ITER_CONT, and stops if H5_ITER_STOP is
 *              returned.
 *
 * Return:      Success:    H5_ITER_CONT or H5_ITER_STOP
 *              Failure:    Negative (H5_ITER_ERROR)
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__chunk_iter_cb(const H5D_chunk_rec_t *chunk_rec, void *udata)
{
    const H5D_chunk_iter_ud_t *data      = (H5D_chunk_iter_ud_t *)udata;
    const H5O_layout_chunk_t  *chunk     = data->chunk;
    int                        ret_value = H5_ITER_CONT;
    hsize_t                    offset[H5O_LAYOUT_NDIMS];
    unsigned                   ii; /* Match H5O_layout_chunk_t.ndims */

    /* Similar to H5D__get_chunk_info */
    for (ii = 0; ii < chunk->ndims; ii++)
        offset[ii] = chunk_rec->scaled[ii] * chunk->dim[ii];

    FUNC_ENTER_PACKAGE_NOERR

    /* Check for callback failure and pass along return value */
    if ((ret_value = (data->op)(offset, (unsigned)chunk_rec->filter_mask, chunk_rec->chunk_addr,
                                (hsize_t)chunk_rec->nbytes, data->op_data)) < 0)
        HERROR(H5E_DATASET, H5E_CANTNEXT, "iteration operator failed");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_iter_cb */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_iter
 *
 * Purpose:     Iterate over all the chunks in the dataset with given callback.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_iter(H5D_t *dset, H5D_chunk_iter_op_t op, void *op_data)
{
    const H5D_rdcc_t  *rdcc   = NULL;       /* Raw data chunk cache */
    H5O_layout_t      *layout = NULL;       /* Dataset layout */
    H5D_rdcc_ent_t    *ent;                 /* Cache entry index */
    H5D_chk_idx_info_t idx_info;            /* Chunked index info */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(dset->oloc.addr)

    /* Check args */
    assert(dset);
    assert(dset->shared);

    /* Get dataset layout and raw data chunk cache */
    layout = &(dset->shared->layout);
    rdcc   = &(dset->shared->cache.chunk);
    assert(layout);
    assert(rdcc);
    assert(H5D_CHUNKED == layout->type);

    /* Search for cached chunks that haven't been written out */
    for (ent = rdcc->head; ent; ent = ent->next)
        /* Flush the chunk out to disk, to make certain the size is correct later */
        if (H5D__chunk_flush_entry(dset, ent, false) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "cannot flush indexed storage buffer");

    /* Compose chunked index info struct */
    idx_info.f       = dset->oloc.file;
    idx_info.pline   = &dset->shared->dcpl_cache.pline;
    idx_info.layout  = &layout->u.chunk;
    idx_info.storage = &layout->storage.u.chunk;

    /* If the dataset is not written, return without errors */
    if (H5_addr_defined(idx_info.storage->idx_addr)) {
        H5D_chunk_iter_ud_t ud;

        /* Set up info for iteration callback */
        ud.op      = op;
        ud.op_data = op_data;
        ud.chunk   = &dset->shared->layout.u.chunk;

        /* Iterate over the allocated chunks calling the iterator callback */
        if ((ret_value = (layout->storage.u.chunk.ops->iterate)(&idx_info, H5D__chunk_iter_cb, &ud)) < 0)
            HERROR(H5E_DATASET, H5E_CANTNEXT, "chunk iteration failed");
    } /* end if H5_addr_defined */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__chunk_iter() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_get_offset_copy
 *
 * Purpose:     Copies an offset buffer and performs bounds checks on the
 *              values.
 *
 *              This helper function ensures that the offset buffer given
 *              by the user is suitable for use with the rest of the library.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__chunk_get_offset_copy(const H5D_t *dset, const hsize_t *offset, hsize_t *offset_copy)
{
    unsigned u;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(dset);
    assert(offset);
    assert(offset_copy);

    /* The library's chunking code requires the offset to terminate with a zero.
     * So transfer the offset array to an internal offset array that we
     * can properly terminate (handled via the memset call).
     */
    memset(offset_copy, 0, H5O_LAYOUT_NDIMS * sizeof(hsize_t));

    for (u = 0; u < dset->shared->ndims; u++) {
        /* Make sure the offset doesn't exceed the dataset's dimensions */
        if (offset[u] > dset->shared->curr_dims[u])
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "offset exceeds dimensions of dataset");

        /* Make sure the offset fall right on a chunk's boundary */
        if (offset[u] % dset->shared->layout.u.chunk.dim[u])
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "offset doesn't fall on chunks's boundary");

        offset_copy[u] = offset[u];
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_get_offset_copy() */
