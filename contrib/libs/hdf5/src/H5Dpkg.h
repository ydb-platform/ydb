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
 * Purpose: This file contains declarations which are visible only within
 *          the H5D package.  Source files outside the H5D package should
 *          include H5Dprivate.h instead.
 */
#if !(defined H5D_FRIEND || defined H5D_MODULE)
#error "Do not include this file outside the H5D package!"
#endif

#ifndef H5Dpkg_H
#define H5Dpkg_H

/* Get package's private header */
#include "H5Dprivate.h"

/* Other private headers needed by this file */
#include "H5ACprivate.h" /* Metadata cache            */
#include "H5B2private.h" /* v2 B-trees                */
#include "H5Fprivate.h"  /* File access               */
#include "H5FLprivate.h" /* Free Lists                */
#include "H5Gprivate.h"  /* Groups                    */
#include "H5SLprivate.h" /* Skip lists                */
#include "H5Tprivate.h"  /* Datatypes                 */

/**************************/
/* Package Private Macros */
/**************************/

/* Set the minimum object header size to create objects with */
#define H5D_MINHDR_SIZE 256

/* Flags for marking aspects of a dataset dirty */
#define H5D_MARK_SPACE  0x01
#define H5D_MARK_LAYOUT 0x02

/* Default creation parameters for chunk index data structures */
/* See H5O_layout_chunk_t */

/* Fixed array creation values */
#define H5D_FARRAY_CREATE_PARAM_SIZE         1  /* Size of the creation parameters in bytes */
#define H5D_FARRAY_MAX_DBLK_PAGE_NELMTS_BITS 10 /* i.e. 1024 elements per data block page */

/* Extensible array creation values */
#define H5D_EARRAY_CREATE_PARAM_SIZE           5  /* Size of the creation parameters in bytes */
#define H5D_EARRAY_MAX_NELMTS_BITS             32 /* i.e. 4 giga-elements */
#define H5D_EARRAY_IDX_BLK_ELMTS               4
#define H5D_EARRAY_SUP_BLK_MIN_DATA_PTRS       4
#define H5D_EARRAY_DATA_BLK_MIN_ELMTS          16
#define H5D_EARRAY_MAX_DBLOCK_PAGE_NELMTS_BITS 10 /* i.e. 1024 elements per data block page */

/* v2 B-tree creation values for raw meta_size */
#define H5D_BT2_CREATE_PARAM_SIZE 6 /* Size of the creation parameters in bytes */
#define H5D_BT2_NODE_SIZE         2048
#define H5D_BT2_SPLIT_PERC        100
#define H5D_BT2_MERGE_PERC        40

/* Macro to determine if the layout I/O callback should perform I/O */
#define H5D_LAYOUT_CB_PERFORM_IO(IO_INFO)                                                                    \
    (((IO_INFO)->use_select_io == H5D_SELECTION_IO_MODE_OFF) ||                                              \
     ((IO_INFO)->count == 1 && (IO_INFO)->max_tconv_type_size == 0))

/* Macro to check if in-place type conversion will be used for a piece and add it to the global type
 * conversion size if it won't be used */
#define H5D_INIT_PIECE_TCONV(IO_INFO, DINFO, PIECE_INFO)                                                     \
    {                                                                                                        \
        /* Check for potential in-place conversion */                                                        \
        if ((IO_INFO)->may_use_in_place_tconv) {                                                             \
            size_t mem_type_size  = ((IO_INFO)->op_type == H5D_IO_OP_READ)                                   \
                                        ? (DINFO)->type_info.dst_type_size                                   \
                                        : (DINFO)->type_info.src_type_size;                                  \
            size_t file_type_size = ((IO_INFO)->op_type == H5D_IO_OP_READ)                                   \
                                        ? (DINFO)->type_info.src_type_size                                   \
                                        : (DINFO)->type_info.dst_type_size;                                  \
                                                                                                             \
            /* Make sure the memory type is not smaller than the file type, otherwise the memory buffer      \
             * won't be big enough to serve as the type conversion buffer */                                 \
            if (mem_type_size >= file_type_size) {                                                           \
                bool    is_contig;                                                                           \
                hsize_t sel_off;                                                                             \
                                                                                                             \
                /* Check if the space is contiguous */                                                       \
                if (H5S_select_contig_block((PIECE_INFO)->mspace, &is_contig, &sel_off, NULL) < 0)           \
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't check if dataspace is contiguous");  \
                                                                                                             \
                /* If the first sequence includes all the elements selected in this piece, it it contiguous  \
                 */                                                                                          \
                if (is_contig) {                                                                             \
                    H5_CHECK_OVERFLOW(sel_off, hsize_t, size_t);                                             \
                    (PIECE_INFO)->in_place_tconv = true;                                                     \
                    (PIECE_INFO)->buf_off        = (size_t)sel_off * mem_type_size;                          \
                }                                                                                            \
            }                                                                                                \
        }                                                                                                    \
                                                                                                             \
        /* If we're not using in-place type conversion, add this piece to global type conversion buffer      \
         * size.  This will only be used if we must allocate a type conversion buffer for the entire I/O. */ \
        if (!(PIECE_INFO)->in_place_tconv)                                                                   \
            (IO_INFO)->tconv_buf_size += (PIECE_INFO)->piece_points * MAX((DINFO)->type_info.src_type_size,  \
                                                                          (DINFO)->type_info.dst_type_size); \
    }

/****************************/
/* Package Private Typedefs */
/****************************/

/* Typedef for datatype information for raw data I/O operation */
typedef struct H5D_type_info_t {
    /* Initial values */
    const H5T_t *mem_type;    /* Pointer to memory datatype */
    const H5T_t *dset_type;   /* Pointer to dataset datatype */
    H5T_path_t  *tpath;       /* Datatype conversion path */
    hid_t        src_type_id; /* Source datatype ID */
    hid_t        dst_type_id; /* Destination datatype ID */

    /* Computed/derived values */
    size_t                   src_type_size;  /* Size of source type */
    size_t                   dst_type_size;  /* Size of destination type */
    bool                     is_conv_noop;   /* Whether the type conversion is a NOOP */
    bool                     is_xform_noop;  /* Whether the data transform is a NOOP */
    const H5T_subset_info_t *cmpd_subset;    /* Info related to the compound subset conversion functions */
    H5T_bkg_t                need_bkg;       /* Type of background buf needed */
    size_t                   request_nelmts; /* Requested strip mine */
} H5D_type_info_t;

/* Forward declaration of structs used below */
struct H5D_io_info_t;
struct H5D_dset_io_info_t;
typedef struct H5D_shared_t H5D_shared_t;

/* Function pointers for I/O on particular types of dataset layouts */
typedef herr_t (*H5D_layout_construct_func_t)(H5F_t *f, H5D_t *dset);
typedef herr_t (*H5D_layout_init_func_t)(H5F_t *f, const H5D_t *dset, hid_t dapl_id);
typedef bool (*H5D_layout_is_space_alloc_func_t)(const H5O_storage_t *storage);
typedef bool (*H5D_layout_is_data_cached_func_t)(const H5D_shared_t *shared_dset);
typedef herr_t (*H5D_layout_io_init_func_t)(struct H5D_io_info_t *io_info, struct H5D_dset_io_info_t *dinfo);
typedef herr_t (*H5D_layout_mdio_init_func_t)(struct H5D_io_info_t      *io_info,
                                              struct H5D_dset_io_info_t *dinfo);
typedef herr_t (*H5D_layout_read_func_t)(struct H5D_io_info_t *io_info, struct H5D_dset_io_info_t *dinfo);
typedef herr_t (*H5D_layout_write_func_t)(struct H5D_io_info_t *io_info, struct H5D_dset_io_info_t *dinfo);
typedef herr_t (*H5D_layout_read_md_func_t)(struct H5D_io_info_t *io_info);
typedef herr_t (*H5D_layout_write_md_func_t)(struct H5D_io_info_t *io_info);
typedef ssize_t (*H5D_layout_readvv_func_t)(const struct H5D_io_info_t      *io_info,
                                            const struct H5D_dset_io_info_t *dset_info, size_t dset_max_nseq,
                                            size_t *dset_curr_seq, size_t dset_len_arr[],
                                            hsize_t dset_offset_arr[], size_t mem_max_nseq,
                                            size_t *mem_curr_seq, size_t mem_len_arr[],
                                            hsize_t mem_offset_arr[]);
typedef ssize_t (*H5D_layout_writevv_func_t)(const struct H5D_io_info_t      *io_info,
                                             const struct H5D_dset_io_info_t *dset_info, size_t dset_max_nseq,
                                             size_t *dset_curr_seq, size_t dset_len_arr[],
                                             hsize_t dset_offset_arr[], size_t mem_max_nseq,
                                             size_t *mem_curr_seq, size_t mem_len_arr[],
                                             hsize_t mem_offset_arr[]);
typedef herr_t (*H5D_layout_flush_func_t)(H5D_t *dataset);
typedef herr_t (*H5D_layout_io_term_func_t)(struct H5D_io_info_t *io_info, struct H5D_dset_io_info_t *di);
typedef herr_t (*H5D_layout_dest_func_t)(H5D_t *dataset);

/* Typedef for grouping layout I/O routines */
typedef struct H5D_layout_ops_t {
    H5D_layout_construct_func_t      construct;      /* Layout constructor for new datasets */
    H5D_layout_init_func_t           init;           /* Layout initializer for dataset */
    H5D_layout_is_space_alloc_func_t is_space_alloc; /* Query routine to determine if storage is allocated */
    H5D_layout_is_data_cached_func_t
        is_data_cached; /* Query routine to determine if any raw data is cached.  If routine is not present
                           then the layout type never caches raw data. */
    H5D_layout_io_init_func_t   io_init;   /* I/O initialization routine */
    H5D_layout_mdio_init_func_t mdio_init; /* Multi Dataset I/O initialization routine - called after all
                                              datasets have done io_init and sel_pieces has been allocated */
    H5D_layout_read_func_t    ser_read;    /* High-level I/O routine for reading data in serial */
    H5D_layout_write_func_t   ser_write;   /* High-level I/O routine for writing data in serial */
    H5D_layout_readvv_func_t  readvv;      /* Low-level I/O routine for reading data */
    H5D_layout_writevv_func_t writevv;     /* Low-level I/O routine for writing data */
    H5D_layout_flush_func_t   flush;       /* Low-level I/O routine for flushing raw data */
    H5D_layout_io_term_func_t io_term;     /* I/O shutdown routine for multi-dset */
    H5D_layout_dest_func_t    dest;        /* Destroy layout info */
} H5D_layout_ops_t;

/* Function pointers for either multiple or single block I/O access */
typedef herr_t (*H5D_io_single_read_func_t)(const struct H5D_io_info_t      *io_info,
                                            const struct H5D_dset_io_info_t *dset_info);
typedef herr_t (*H5D_io_single_write_func_t)(const struct H5D_io_info_t      *io_info,
                                             const struct H5D_dset_io_info_t *dset_info);

typedef herr_t (*H5D_io_single_read_md_func_t)(const struct H5D_io_info_t *io_info, hsize_t nelmts,
                                               H5S_t *file_space, H5S_t *mem_space);
typedef herr_t (*H5D_io_single_write_md_func_t)(const struct H5D_io_info_t *io_info, hsize_t nelmts,
                                                H5S_t *file_space, H5S_t *mem_space);

/* Typedef for raw data I/O framework info */
typedef struct H5D_io_ops_t {
    H5D_layout_read_func_t     multi_read;   /* High-level I/O routine for reading data */
    H5D_layout_write_func_t    multi_write;  /* High-level I/O routine for writing data */
    H5D_io_single_read_func_t  single_read;  /* I/O routine for reading single block */
    H5D_io_single_write_func_t single_write; /* I/O routine for writing single block */
} H5D_io_ops_t;

/* Typedef for raw data I/O framework info (multi-dataset I/O) */
typedef struct H5D_md_io_ops_t {
    H5D_layout_read_md_func_t     multi_read_md;  /* High-level I/O routine for reading data for multi-dset */
    H5D_layout_write_md_func_t    multi_write_md; /* High-level I/O routine for writing data for multi-dset */
    H5D_io_single_read_md_func_t  single_read_md; /* I/O routine for reading single block for multi-dset */
    H5D_io_single_write_md_func_t single_write_md; /* I/O routine for writing single block for multi-dset */
} H5D_md_io_ops_t;

/* Typedefs for dataset storage information */
typedef struct {
    haddr_t dset_addr; /* Address of dataset in file */
    hsize_t dset_size; /* Total size of dataset in file */
} H5D_contig_storage_t;

typedef struct {
    hsize_t *scaled; /* Scaled coordinates for a chunk */
} H5D_chunk_storage_t;

typedef struct {
    void *buf;   /* Buffer for compact dataset */
    bool *dirty; /* Pointer to dirty flag to mark */
} H5D_compact_storage_t;

typedef union H5D_storage_t {
    H5D_contig_storage_t  contig;  /* Contiguous information for dataset */
    H5D_chunk_storage_t   chunk;   /* Chunk information for dataset */
    H5D_compact_storage_t compact; /* Compact information for dataset */
    H5O_efl_t             efl;     /* External file list information for dataset */
} H5D_storage_t;

/* Typedef for raw data I/O operation info */
typedef enum H5D_io_op_type_t {
    H5D_IO_OP_READ, /* Read operation */
    H5D_IO_OP_WRITE /* Write operation */
} H5D_io_op_type_t;

/* Piece info for a data chunk/block during I/O */
typedef struct H5D_piece_info_t {
    haddr_t  faddr;                    /* File address */
    hsize_t  index;                    /* "Index" of chunk in dataset */
    hsize_t  piece_points;             /* Number of elements selected in piece */
    hsize_t  scaled[H5O_LAYOUT_NDIMS]; /* Scaled coordinates of chunk (in file dataset's dataspace) */
    H5S_t   *fspace;                   /* Dataspace describing chunk & selection in it */
    unsigned fspace_shared;  /* Indicate that the file space for a chunk is shared and shouldn't be freed */
    H5S_t   *mspace;         /* Dataspace describing selection in memory corresponding to this chunk */
    unsigned mspace_shared;  /* Indicate that the memory space for a chunk is shared and shouldn't be freed */
    bool     in_place_tconv; /* Whether to perform type conversion in-place */
    size_t   buf_off;        /* Buffer offset for in-place type conversion */
    bool     filtered_dset;  /* Whether the dataset this chunk is in has filters applied */
    struct H5D_dset_io_info_t *dset_info; /* Pointer to dset_info */
} H5D_piece_info_t;

/* I/O info for a single dataset */
typedef struct H5D_dset_io_info_t {
    H5D_t                  *dset;       /* Pointer to dataset being operated on */
    H5D_storage_t          *store;      /* Dataset storage info */
    H5D_layout_ops_t        layout_ops; /* Dataset layout I/O operation function pointers */
    H5_flexible_const_ptr_t buf;        /* Buffer pointer */

    H5D_io_ops_t io_ops; /* I/O operations for this dataset */

    H5O_layout_t *layout; /* Dataset layout information*/
    hsize_t       nelmts; /* Number of elements selected in file & memory dataspaces */

    H5S_t *file_space; /* Pointer to the file dataspace */
    H5S_t *mem_space;  /* Pointer to the memory dataspace */

    union {
        struct H5D_chunk_map_t *chunk_map;         /* Chunk specific I/O info */
        H5D_piece_info_t       *contig_piece_info; /* Piece info for contiguous dataset */
    } layout_io_info;

    hid_t           mem_type_id; /* memory datatype ID */
    H5D_type_info_t type_info;
    bool            skip_io; /* Whether to skip I/O for this dataset */
} H5D_dset_io_info_t;

/* I/O info for entire I/O operation */
typedef struct H5D_io_info_t {
    /* QAK: Delete the f_sh field when oloc has a shared file pointer? */
    H5F_shared_t *f_sh; /* Pointer to shared file struct that dataset is within */
#ifdef H5_HAVE_PARALLEL
    MPI_Comm comm;                     /* MPI communicator for file */
    bool     using_mpi_vfd;            /* Whether the file is using an MPI-based VFD */
#endif                                 /* H5_HAVE_PARALLEL */
    H5D_md_io_ops_t         md_io_ops; /* Multi dataset I/O operation function pointers */
    H5D_io_op_type_t        op_type;
    size_t                  count;          /* Number of datasets in I/O request */
    size_t                  filtered_count; /* Number of datasets with filters applied in I/O request */
    H5D_dset_io_info_t     *dsets_info;     /* dsets info where I/O is done to/from */
    size_t                  piece_count;    /* Number of pieces in I/O request */
    size_t                  pieces_added;   /* Number of pieces added so far to arrays */
    size_t                  filtered_pieces_added; /* Number of filtered pieces in I/O request */
    H5D_piece_info_t      **sel_pieces;            /* Array of info struct for all pieces in I/O */
    H5S_t                 **mem_spaces;            /* Array of chunk memory spaces */
    H5S_t                 **file_spaces;           /* Array of chunk file spaces */
    haddr_t                *addrs;                 /* Array of chunk addresses */
    size_t                 *element_sizes;         /* Array of element sizes */
    void                  **rbufs;                 /* Array of read buffers */
    const void            **wbufs;                 /* Array of write buffers */
    haddr_t                 store_faddr;           /* lowest file addr for read/write */
    H5_flexible_const_ptr_t base_maddr;            /* starting mem address */
    H5D_selection_io_mode_t use_select_io;         /* Whether to use selection I/O */
    uint8_t                *tconv_buf;             /* Datatype conv buffer */
    bool                    tconv_buf_allocated;   /* Whether the type conversion buffer was allocated */
    size_t                  tconv_buf_size;        /* Size of type conversion buffer */
    uint8_t                *bkg_buf;               /* Background buffer */
    bool                    bkg_buf_allocated;     /* Whether the background buffer was allocated */
    size_t                  bkg_buf_size;          /* Size of background buffer */
    size_t max_tconv_type_size; /* Largest of all source and destination type sizes involved in type
                                   conversion */
    bool must_fill_bkg; /* Whether any datasets need a background buffer filled with destination contents */
    bool may_use_in_place_tconv; /* Whether datasets in this I/O could potentially use in-place type
                                       conversion if the type sizes are compatible with it */
#ifdef H5_HAVE_PARALLEL
    H5D_mpio_actual_io_mode_t actual_io_mode; /* Actual type of collective or independent I/O */
#endif                                        /* H5_HAVE_PARALLEL */
    unsigned no_selection_io_cause;           /* "No selection I/O cause" flags */
} H5D_io_info_t;

/* Created to pass both at once for callback func */
typedef struct H5D_io_info_wrap_t {
    H5D_io_info_t      *io_info;
    H5D_dset_io_info_t *dinfo;
} H5D_io_info_wrap_t;

/******************/
/* Chunk typedefs */
/******************/

/* Typedef for chunked dataset index operation info */
typedef struct H5D_chk_idx_info_t {
    H5F_t               *f;       /* File pointer for operation */
    const H5O_pline_t   *pline;   /* I/O pipeline info */
    H5O_layout_chunk_t  *layout;  /* Chunk layout description */
    H5O_storage_chunk_t *storage; /* Chunk storage description */
} H5D_chk_idx_info_t;

/*
 * "Generic" chunk record.  Each chunk is keyed by the minimum logical
 * N-dimensional coordinates and the datatype size of the chunk.
 * The fastest-varying dimension is assumed to reference individual bytes of
 * the array, so a 100-element 1-D array of 4-byte integers would really be a
 * 2-D array with the slow varying dimension of size 100 and the fast varying
 * dimension of size 4 (the storage dimensionality has very little to do with
 * the real dimensionality).
 *
 * The chunk's file address, filter mask and size on disk are not key values.
 */
typedef struct H5D_chunk_rec_t {
    hsize_t  scaled[H5O_LAYOUT_NDIMS]; /* Logical offset to start */
    uint32_t nbytes;                   /* Size of stored data */
    uint32_t filter_mask;              /* Excluded filters */
    haddr_t  chunk_addr;               /* Address of chunk in file */
} H5D_chunk_rec_t;

/*
 * Common data exchange structure for indexed storage nodes.  This structure is
 * passed through the indexing layer to the methods for the objects
 * to which the index points.
 */
typedef struct H5D_chunk_common_ud_t {
    const H5O_layout_chunk_t  *layout;  /* Chunk layout description */
    const H5O_storage_chunk_t *storage; /* Chunk storage description */
    const hsize_t             *scaled;  /* Scaled coordinates for a chunk */
} H5D_chunk_common_ud_t;

/* B-tree callback info for various operations */
typedef struct H5D_chunk_ud_t {
    /* Downward */
    H5D_chunk_common_ud_t common; /* Common info for B-tree user data (must be first) */

    /* Upward */
    unsigned    idx_hint;         /* Index of chunk in cache, if present */
    H5F_block_t chunk_block;      /* Offset/length of chunk in file */
    unsigned    filter_mask;      /* Excluded filters */
    bool        new_unfilt_chunk; /* Whether the chunk just became unfiltered */
    hsize_t     chunk_idx;        /* Chunk index for EA, FA indexing */
} H5D_chunk_ud_t;

/* Typedef for "generic" chunk callbacks */
typedef int (*H5D_chunk_cb_func_t)(const H5D_chunk_rec_t *chunk_rec, void *udata);

/* Typedefs for chunk operations */
typedef herr_t (*H5D_chunk_init_func_t)(const H5D_chk_idx_info_t *idx_info, const H5S_t *space,
                                        haddr_t dset_ohdr_addr);
typedef herr_t (*H5D_chunk_create_func_t)(const H5D_chk_idx_info_t *idx_info);
typedef herr_t (*H5D_chunk_open_func_t)(const H5D_chk_idx_info_t *idx_info);
typedef herr_t (*H5D_chunk_close_func_t)(const H5D_chk_idx_info_t *idx_info);
typedef herr_t (*H5D_chunk_is_open_func_t)(const H5D_chk_idx_info_t *idx_info, bool *is_open);
typedef bool (*H5D_chunk_is_space_alloc_func_t)(const H5O_storage_chunk_t *storage);
typedef herr_t (*H5D_chunk_insert_func_t)(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata,
                                          const H5D_t *dset);
typedef herr_t (*H5D_chunk_get_addr_func_t)(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata);
typedef herr_t (*H5D_chunk_load_metadata_func_t)(const H5D_chk_idx_info_t *idx_info);
typedef herr_t (*H5D_chunk_resize_func_t)(H5O_layout_chunk_t *layout);
typedef int (*H5D_chunk_iterate_func_t)(const H5D_chk_idx_info_t *idx_info, H5D_chunk_cb_func_t chunk_cb,
                                        void *chunk_udata);
typedef herr_t (*H5D_chunk_remove_func_t)(const H5D_chk_idx_info_t *idx_info, H5D_chunk_common_ud_t *udata);
typedef herr_t (*H5D_chunk_delete_func_t)(const H5D_chk_idx_info_t *idx_info);
typedef herr_t (*H5D_chunk_copy_setup_func_t)(const H5D_chk_idx_info_t *idx_info_src,
                                              const H5D_chk_idx_info_t *idx_info_dst);
typedef herr_t (*H5D_chunk_copy_shutdown_func_t)(H5O_storage_chunk_t *storage_src,
                                                 H5O_storage_chunk_t *storage_dst);
typedef herr_t (*H5D_chunk_size_func_t)(const H5D_chk_idx_info_t *idx_info, hsize_t *idx_size);
typedef herr_t (*H5D_chunk_reset_func_t)(H5O_storage_chunk_t *storage, bool reset_addr);
typedef herr_t (*H5D_chunk_dump_func_t)(const H5O_storage_chunk_t *storage, FILE *stream);
typedef herr_t (*H5D_chunk_dest_func_t)(const H5D_chk_idx_info_t *idx_info);

/* Typedef for grouping chunk I/O routines */
typedef struct H5D_chunk_ops_t {
    bool                     can_swim; /* Flag to indicate that the index supports SWMR access */
    H5D_chunk_init_func_t    init;     /* Routine to initialize indexing information in memory */
    H5D_chunk_create_func_t  create;   /* Routine to create chunk index */
    H5D_chunk_open_func_t    open;     /* Routine to open chunk index */
    H5D_chunk_close_func_t   close;    /* Routine to close chunk index */
    H5D_chunk_is_open_func_t is_open;  /* Query routine to determine if index is open or not */
    H5D_chunk_is_space_alloc_func_t
                              is_space_alloc; /* Query routine to determine if storage/index is allocated */
    H5D_chunk_insert_func_t   insert;         /* Routine to insert a chunk into an index */
    H5D_chunk_get_addr_func_t get_addr;       /* Routine to retrieve address of chunk in file */
    H5D_chunk_load_metadata_func_t
        load_metadata; /* Routine to load additional chunk index metadata, such as fixed array data blocks */
    H5D_chunk_resize_func_t     resize;     /* Routine to update chunk index info after resizing dataset */
    H5D_chunk_iterate_func_t    iterate;    /* Routine to iterate over chunks */
    H5D_chunk_remove_func_t     remove;     /* Routine to remove a chunk from an index */
    H5D_chunk_delete_func_t     idx_delete; /* Routine to delete index & all chunks from file*/
    H5D_chunk_copy_setup_func_t copy_setup; /* Routine to perform any necessary setup for copying chunks */
    H5D_chunk_copy_shutdown_func_t
                           copy_shutdown; /* Routine to perform any necessary shutdown for copying chunks */
    H5D_chunk_size_func_t  size;          /* Routine to get size of indexing information */
    H5D_chunk_reset_func_t reset;         /* Routine to reset indexing information */
    H5D_chunk_dump_func_t  dump;          /* Routine to dump indexing information */
    H5D_chunk_dest_func_t  dest;          /* Routine to destroy indexing information in memory */
} H5D_chunk_ops_t;

/* Main structure holding the mapping between file chunks and memory */
typedef struct H5D_chunk_map_t {
    unsigned f_ndims; /* Number of dimensions for file dataspace */

    H5S_t         *mchunk_tmpl; /* Dataspace template for new memory chunks */
    H5S_sel_iter_t mem_iter;    /* Iterator for elements in memory selection */
    unsigned       m_ndims;     /* Number of dimensions for memory dataspace */
    H5S_sel_type   msel_type;   /* Selection type in memory */
    H5S_sel_type   fsel_type;   /* Selection type in file */

    H5SL_t *dset_sel_pieces; /* Skip list containing information for each chunk selected */

    H5S_t            *single_space;      /* Dataspace for single chunk */
    H5D_piece_info_t *single_piece_info; /* Pointer to single chunk's info */
    bool              use_single;        /* Whether I/O is on a single element */

    hsize_t           last_index;      /* Index of last chunk operated on */
    H5D_piece_info_t *last_piece_info; /* Pointer to last chunk's info */

    hsize_t chunk_dim[H5O_LAYOUT_NDIMS]; /* Size of chunk in each dimension */
} H5D_chunk_map_t;

/* Cached information about a particular chunk */
typedef struct H5D_chunk_cached_t {
    bool     valid;                    /*whether cache info is valid*/
    hsize_t  scaled[H5O_LAYOUT_NDIMS]; /*scaled offset of chunk*/
    haddr_t  addr;                     /*file address of chunk */
    uint32_t nbytes;                   /*size of stored data */
    hsize_t  chunk_idx;                /*index of chunk in dataset */
    unsigned filter_mask;              /*excluded filters */
} H5D_chunk_cached_t;

/****************************/
/* Virtual dataset typedefs */
/****************************/

/* List of files held open during refresh operations */
typedef struct H5D_virtual_held_file_t {
    H5F_t                          *file; /* Pointer to file held open */
    struct H5D_virtual_held_file_t *next; /* Pointer to next node in list */
} H5D_virtual_held_file_t;

/* The raw data chunk cache */
struct H5D_rdcc_ent_t; /* Forward declaration of struct used below */
typedef struct H5D_rdcc_t {
    struct {
        unsigned ninits;   /* Number of chunk creations        */
        unsigned nhits;    /* Number of cache hits            */
        unsigned nmisses;  /* Number of cache misses        */
        unsigned nflushes; /* Number of cache flushes        */
    } stats;
    size_t                 nbytes_max; /* Maximum cached raw data in bytes    */
    size_t                 nslots;     /* Number of chunk slots allocated    */
    double                 w0;         /* Chunk preemption policy          */
    struct H5D_rdcc_ent_t *head;       /* Head of doubly linked list        */
    struct H5D_rdcc_ent_t *tail;       /* Tail of doubly linked list        */
    struct H5D_rdcc_ent_t
        *tmp_head; /* Head of temporary doubly linked list.  Chunks on this list are not in the hash table
                      (slot).  The head entry is a sentinel (does not refer to an actual chunk). */
    size_t                  nbytes_used;       /* Current cached raw data in bytes */
    int                     nused;             /* Number of chunk slots in use        */
    H5D_chunk_cached_t      last;              /* Cached copy of last chunk information */
    struct H5D_rdcc_ent_t **slot;              /* Chunk slots, each points to a chunk*/
    H5SL_t                 *sel_chunks;        /* Skip list containing information for each chunk selected */
    H5S_t                  *single_space;      /* Dataspace for single element I/O on chunks */
    H5D_piece_info_t       *single_piece_info; /* Pointer to single piece's info */

    /* Cached information about scaled dataspace dimensions */
    hsize_t  scaled_dims[H5S_MAX_RANK];        /* The scaled dim sizes */
    hsize_t  scaled_power2up[H5S_MAX_RANK];    /* The scaled dim sizes, rounded up to next power of 2 */
    unsigned scaled_encode_bits[H5S_MAX_RANK]; /* The number of bits needed to encode the scaled dim sizes */
} H5D_rdcc_t;

/* The raw data contiguous data cache */
typedef struct H5D_rdcdc_t {
    unsigned char *sieve_buf;      /* Buffer to hold data sieve buffer */
    haddr_t        sieve_loc;      /* File location (offset) of the data sieve buffer */
    size_t         sieve_size;     /* Size of the data sieve buffer used (in bytes) */
    size_t         sieve_buf_size; /* Size of the data sieve buffer allocated (in bytes) */
    bool           sieve_dirty;    /* Flag to indicate that the data sieve buffer is dirty */
} H5D_rdcdc_t;

/*
 * A dataset is made of two layers, an H5D_t struct that is unique to
 * each instance of an opened dataset, and a shared struct that is only
 * created once for a given dataset.  Thus, if a dataset is opened twice,
 * there will be two IDs and two H5D_t structs, both sharing one H5D_shared_t.
 */
struct H5D_shared_t {
    size_t           fo_count;        /* Reference count */
    bool             closing;         /* Flag to indicate dataset is closing */
    hid_t            type_id;         /* ID for dataset's datatype    */
    H5T_t           *type;            /* Datatype for this dataset     */
    H5S_t           *space;           /* Dataspace of this dataset    */
    hid_t            dcpl_id;         /* Dataset creation property id */
    hid_t            dapl_id;         /* Dataset access property id */
    H5D_dcpl_cache_t dcpl_cache;      /* Cached DCPL values */
    H5O_layout_t     layout;          /* Data layout                  */
    bool             checked_filters; /* true if dataset passes can_apply check */

    /* Cached dataspace info */
    unsigned ndims;                       /* The dataset's dataspace rank */
    hsize_t  curr_dims[H5S_MAX_RANK];     /* The curr. size of dataset dimensions */
    hsize_t  curr_power2up[H5S_MAX_RANK]; /* The curr. dim sizes, rounded up to next power of 2 */
    hsize_t  max_dims[H5S_MAX_RANK];      /* The max. size of dataset dimensions */

    /* Buffered/cached information for types of raw data storage*/
    struct {
        H5D_rdcdc_t contig; /* Information about contiguous data */
                            /* (Note that the "contig" cache
                             * information can be used by a chunked
                             * dataset in certain circumstances)
                             */
        H5D_rdcc_t chunk;   /* Information about chunked data */
    } cache;

    H5D_append_flush_t append_flush;   /* Append flush property information */
    char              *extfile_prefix; /* expanded external file prefix */
    char              *vds_prefix;     /* expanded vds prefix */
};

struct H5D_t {
    H5O_loc_t     oloc;   /* Object header location       */
    H5G_name_t    path;   /* Group hierarchy path         */
    H5D_shared_t *shared; /* cached information from file */
};

/* Enumerated type for allocating dataset's storage */
typedef enum {
    H5D_ALLOC_CREATE, /* Dataset is being created */
    H5D_ALLOC_OPEN,   /* Dataset is being opened */
    H5D_ALLOC_EXTEND, /* Dataset's dataspace is being extended */
    H5D_ALLOC_WRITE   /* Dataset is being extended */
} H5D_time_alloc_t;

/* Typedef for dataset creation operation */
typedef struct {
    hid_t        type_id; /* Datatype for dataset */
    const H5S_t *space;   /* Dataspace for dataset */
    hid_t        dcpl_id; /* Dataset creation property list */
    hid_t        dapl_id; /* Dataset access property list */
} H5D_obj_create_t;

/* Typedef for filling a buffer with a fill value */
typedef struct H5D_fill_buf_info_t {
    H5MM_allocate_t fill_alloc_func; /* Routine to call for allocating fill buffer */
    void           *fill_alloc_info; /* Extra info for allocation routine */
    H5MM_free_t     fill_free_func;  /* Routine to call for freeing fill buffer */
    void           *fill_free_info;  /* Extra info for free routine */
    H5T_path_t
        *fill_to_mem_tpath; /* Datatype conversion path for converting the fill value to the memory buffer */
    H5T_path_t *mem_to_dset_tpath; /* Datatype conversion path for converting the memory buffer to the dataset
                                      elements */
    const H5O_fill_t *fill;        /* Pointer to fill value */
    void             *fill_buf;    /* Fill buffer */
    size_t            fill_buf_size;                 /* Size of fill buffer */
    bool              use_caller_fill_buf;           /* Whether the caller provided the fill buffer */
    void             *bkg_buf;                       /* Background conversion buffer */
    size_t            bkg_buf_size;                  /* Size of background buffer */
    H5T_t            *mem_type;                      /* Pointer to memory datatype */
    const H5T_t      *file_type;                     /* Pointer to file datatype */
    hid_t             mem_tid;                       /* ID for memory version of disk datatype */
    hid_t             file_tid;                      /* ID for disk datatype */
    size_t            mem_elmt_size, file_elmt_size; /* Size of element in memory and on disk */
    size_t            max_elmt_size;                 /* Max. size of memory or file datatype */
    size_t            elmts_per_buf;                 /* # of elements that fit into a buffer */
    bool has_vlen_fill_type; /* Whether the datatype for the fill value has a variable-length component */
} H5D_fill_buf_info_t;

/*****************************/
/* Package Private Variables */
/*****************************/

/* Storage layout class I/O operations */
H5_DLLVAR const H5D_layout_ops_t H5D_LOPS_CONTIG[1];
H5_DLLVAR const H5D_layout_ops_t H5D_LOPS_EFL[1];
H5_DLLVAR const H5D_layout_ops_t H5D_LOPS_COMPACT[1];
H5_DLLVAR const H5D_layout_ops_t H5D_LOPS_CHUNK[1];
H5_DLLVAR const H5D_layout_ops_t H5D_LOPS_VIRTUAL[1];

/* Chunked layout operations */
H5_DLLVAR const H5D_chunk_ops_t H5D_COPS_BTREE[1];
H5_DLLVAR const H5D_chunk_ops_t H5D_COPS_NONE[1];
H5_DLLVAR const H5D_chunk_ops_t H5D_COPS_SINGLE[1];
H5_DLLVAR const H5D_chunk_ops_t H5D_COPS_EARRAY[1];
H5_DLLVAR const H5D_chunk_ops_t H5D_COPS_FARRAY[1];
H5_DLLVAR const H5D_chunk_ops_t H5D_COPS_BT2[1];

/* The v2 B-tree class for indexing chunked datasets with >1 unlimited dimensions */
H5_DLLVAR const H5B2_class_t H5D_BT2[1];
H5_DLLVAR const H5B2_class_t H5D_BT2_FILT[1];

/*  Array of versions for Layout */
H5_DLLVAR const unsigned H5O_layout_ver_bounds[H5F_LIBVER_NBOUNDS];

/******************************/
/* Package Private Prototypes */
/******************************/

H5_DLL H5D_t  *H5D__create(H5F_t *file, hid_t type_id, const H5S_t *space, hid_t dcpl_id, hid_t dapl_id);
H5_DLL H5D_t  *H5D__create_named(const H5G_loc_t *loc, const char *name, hid_t type_id, const H5S_t *space,
                                 hid_t lcpl_id, hid_t dcpl_id, hid_t dapl_id);
H5_DLL H5D_t  *H5D__open_name(const H5G_loc_t *loc, const char *name, hid_t dapl_id);
H5_DLL hid_t   H5D__get_space(const H5D_t *dset);
H5_DLL hid_t   H5D__get_type(const H5D_t *dset);
H5_DLL herr_t  H5D__get_space_status(const H5D_t *dset, H5D_space_status_t *allocation);
H5_DLL herr_t  H5D__alloc_storage(H5D_t *dset, H5D_time_alloc_t time_alloc, bool full_overwrite,
                                  hsize_t old_dim[]);
H5_DLL herr_t  H5D__get_storage_size(const H5D_t *dset, hsize_t *storage_size);
H5_DLL herr_t  H5D__get_chunk_storage_size(H5D_t *dset, const hsize_t *offset, hsize_t *storage_size);
H5_DLL herr_t  H5D__chunk_index_empty(const H5D_t *dset, bool *empty);
H5_DLL herr_t  H5D__get_num_chunks(const H5D_t *dset, const H5S_t *space, hsize_t *nchunks);
H5_DLL herr_t  H5D__get_chunk_info(const H5D_t *dset, const H5S_t *space, hsize_t chk_idx, hsize_t *coord,
                                   unsigned *filter_mask, haddr_t *offset, hsize_t *size);
H5_DLL herr_t  H5D__get_chunk_info_by_coord(const H5D_t *dset, const hsize_t *coord, unsigned *filter_mask,
                                            haddr_t *addr, hsize_t *size);
H5_DLL herr_t  H5D__chunk_iter(H5D_t *dset, H5D_chunk_iter_op_t cb, void *op_data);
H5_DLL haddr_t H5D__get_offset(const H5D_t *dset);
H5_DLL herr_t  H5D__vlen_get_buf_size(H5D_t *dset, hid_t type_id, hid_t space_id, hsize_t *size);
H5_DLL herr_t  H5D__vlen_get_buf_size_gen(H5VL_object_t *vol_obj, hid_t type_id, hid_t space_id,
                                          hsize_t *size);
H5_DLL herr_t  H5D__set_extent(H5D_t *dataset, const hsize_t *size);
H5_DLL herr_t  H5D__flush_sieve_buf(H5D_t *dataset);
H5_DLL herr_t  H5D__flush_real(H5D_t *dataset);
H5_DLL herr_t  H5D__flush(H5D_t *dset, hid_t dset_id);
H5_DLL herr_t  H5D__mark(const H5D_t *dataset, unsigned flags);
H5_DLL herr_t  H5D__refresh(H5D_t *dataset, hid_t dset_id);

/* To convert a dataset's chunk indexing type to v1 B-tree */
H5_DLL herr_t H5D__format_convert(H5D_t *dataset);

/* Internal I/O routines */
H5_DLL herr_t H5D__read(size_t count, H5D_dset_io_info_t *dset_info);
H5_DLL herr_t H5D__write(size_t count, H5D_dset_io_info_t *dset_info);

/* Functions that perform direct serial I/O operations */
H5_DLL herr_t H5D__select_read(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info);
H5_DLL herr_t H5D__select_write(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info);

/* Functions that perform direct copying between memory buffers */
H5_DLL herr_t H5D_select_io_mem(void *dst_buf, H5S_t *dst_space, const void *src_buf, H5S_t *src_space,
                                size_t elmt_size, size_t nelmts);

/* Functions that perform scatter-gather I/O operations */
H5_DLL herr_t H5D__scatter_mem(const void *_tscat_buf, H5S_sel_iter_t *iter, size_t nelmts, void *_buf);
H5_DLL size_t H5D__gather_mem(const void *_buf, H5S_sel_iter_t *iter, size_t nelmts,
                              void *_tgath_buf /*out*/);
H5_DLL herr_t H5D__scatgath_read(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info);
H5_DLL herr_t H5D__scatgath_write(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info);
H5_DLL herr_t H5D__scatgath_read_select(H5D_io_info_t *io_info);
H5_DLL herr_t H5D__scatgath_write_select(H5D_io_info_t *io_info);

/* Functions that operate on dataset's layout information */
H5_DLL herr_t H5D__layout_set_io_ops(const H5D_t *dataset);
H5_DLL size_t H5D__layout_meta_size(const H5F_t *f, const H5O_layout_t *layout, bool include_compact_data);
H5_DLL herr_t H5D__layout_set_version(H5F_t *f, H5O_layout_t *layout);
H5_DLL herr_t H5D__layout_set_latest_indexing(H5O_layout_t *layout, const H5S_t *space,
                                              const H5D_dcpl_cache_t *dcpl_cache);
H5_DLL herr_t H5D__layout_oh_create(H5F_t *file, H5O_t *oh, H5D_t *dset, hid_t dapl_id);
H5_DLL herr_t H5D__layout_oh_read(H5D_t *dset, hid_t dapl_id, H5P_genplist_t *plist);
H5_DLL herr_t H5D__layout_oh_write(const H5D_t *dataset, H5O_t *oh, unsigned update_flags);

/* Functions that operate on contiguous storage */
H5_DLL herr_t H5D__contig_alloc(H5F_t *f, H5O_storage_contig_t *storage);
H5_DLL bool   H5D__contig_is_space_alloc(const H5O_storage_t *storage);
H5_DLL bool   H5D__contig_is_data_cached(const H5D_shared_t *shared_dset);
H5_DLL herr_t H5D__contig_fill(H5D_t *dset);
H5_DLL herr_t H5D__contig_read(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
H5_DLL herr_t H5D__contig_write(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
H5_DLL herr_t H5D__contig_copy(H5F_t *f_src, const H5O_storage_contig_t *storage_src, H5F_t *f_dst,
                               H5O_storage_contig_t *storage_dst, H5T_t *src_dtype, H5O_copy_t *cpy_info);
H5_DLL herr_t H5D__contig_delete(H5F_t *f, const H5O_storage_t *store);

/* Functions that operate on chunked dataset storage */
H5_DLL htri_t H5D__chunk_cacheable(const H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_info, haddr_t caddr,
                                   bool write_op);
H5_DLL herr_t H5D__chunk_create(const H5D_t *dset /*in,out*/);
H5_DLL herr_t H5D__chunk_set_info(const H5D_t *dset);
H5_DLL bool   H5D__chunk_is_space_alloc(const H5O_storage_t *storage);
H5_DLL bool   H5D__chunk_is_data_cached(const H5D_shared_t *shared_dset);
H5_DLL herr_t H5D__chunk_lookup(const H5D_t *dset, const hsize_t *scaled, H5D_chunk_ud_t *udata);
H5_DLL herr_t H5D__chunk_allocated(const H5D_t *dset, hsize_t *nbytes);
H5_DLL herr_t H5D__chunk_allocate(const H5D_t *dset, bool full_overwrite, const hsize_t old_dim[]);
H5_DLL herr_t H5D__chunk_file_alloc(const H5D_chk_idx_info_t *idx_info, const H5F_block_t *old_chunk,
                                    H5F_block_t *new_chunk, bool *need_insert, const hsize_t *scaled);
H5_DLL void  *H5D__chunk_mem_alloc(size_t size, void *pline);
H5_DLL void   H5D__chunk_mem_free(void *chk, void *pline);
H5_DLL void  *H5D__chunk_mem_xfree(void *chk, const void *pline);
H5_DLL void  *H5D__chunk_mem_realloc(void *chk, size_t size, const H5O_pline_t *pline);
H5_DLL herr_t H5D__chunk_update_old_edge_chunks(H5D_t *dset, hsize_t old_dim[]);
H5_DLL bool   H5D__chunk_is_partial_edge_chunk(unsigned dset_ndims, const uint32_t *chunk_dims,
                                               const hsize_t *chunk_scaled, const hsize_t *dset_dims);
H5_DLL herr_t H5D__chunk_prune_by_extent(H5D_t *dset, const hsize_t *old_dim);
H5_DLL herr_t H5D__chunk_set_sizes(H5D_t *dset);
#ifdef H5_HAVE_PARALLEL
H5_DLL herr_t H5D__chunk_addrmap(const H5D_t *dset, haddr_t chunk_addr[]);
#endif /* H5_HAVE_PARALLEL */
H5_DLL herr_t H5D__chunk_update_cache(H5D_t *dset);
H5_DLL herr_t H5D__chunk_copy(H5F_t *f_src, H5O_storage_chunk_t *storage_src, H5O_layout_chunk_t *layout_src,
                              H5F_t *f_dst, H5O_storage_chunk_t *storage_dst,
                              const H5S_extent_t *ds_extent_src, const H5T_t *dt_src,
                              const H5O_pline_t *pline_src, H5O_copy_t *cpy_info);
H5_DLL herr_t H5D__chunk_bh_info(const H5O_loc_t *loc, H5O_t *oh, H5O_layout_t *layout, hsize_t *btree_size);
H5_DLL herr_t H5D__chunk_dump_index(H5D_t *dset, FILE *stream);
H5_DLL herr_t H5D__chunk_delete(H5F_t *f, H5O_t *oh, H5O_storage_t *store);
H5_DLL herr_t H5D__chunk_get_offset_copy(const H5D_t *dset, const hsize_t *offset, hsize_t *offset_copy);
H5_DLL herr_t H5D__chunk_direct_write(H5D_t *dset, uint32_t filters, hsize_t *offset, uint32_t data_size,
                                      const void *buf);
H5_DLL herr_t H5D__chunk_direct_read(const H5D_t *dset, hsize_t *offset, uint32_t *filters, void *buf);
#ifdef H5D_CHUNK_DEBUG
H5_DLL herr_t H5D__chunk_stats(const H5D_t *dset, bool headers);
#endif /* H5D_CHUNK_DEBUG */

/* format convert */
H5_DLL herr_t H5D__chunk_format_convert(H5D_t *dset, H5D_chk_idx_info_t *idx_info,
                                        H5D_chk_idx_info_t *new_idx_info);

/* Functions that operate on compact dataset storage */
H5_DLL herr_t H5D__compact_fill(const H5D_t *dset);
H5_DLL herr_t H5D__compact_copy(H5F_t *f_src, H5O_storage_compact_t *storage_src, H5F_t *f_dst,
                                H5O_storage_compact_t *storage_dst, H5T_t *src_dtype, H5O_copy_t *cpy_info);

/* Functions that operate on virtual dataset storage */
H5_DLL herr_t H5D__virtual_store_layout(H5F_t *f, H5O_layout_t *layout);
H5_DLL herr_t H5D__virtual_copy_layout(H5O_layout_t *layout);
H5_DLL herr_t H5D__virtual_set_extent_unlim(const H5D_t *dset);
H5_DLL herr_t H5D__virtual_reset_layout(H5O_layout_t *layout);
H5_DLL herr_t H5D__virtual_delete(H5F_t *f, H5O_storage_t *storage);
H5_DLL herr_t H5D__virtual_copy(H5F_t *f_src, H5O_layout_t *layout_dst);
H5_DLL herr_t H5D__virtual_init(H5F_t *f, const H5D_t *dset, hid_t dapl_id);
H5_DLL bool   H5D__virtual_is_space_alloc(const H5O_storage_t *storage);
H5_DLL herr_t H5D__virtual_hold_source_dset_files(const H5D_t *dset, H5D_virtual_held_file_t **head);
H5_DLL herr_t H5D__virtual_refresh_source_dsets(H5D_t *dset);
H5_DLL herr_t H5D__virtual_release_source_dset_files(H5D_virtual_held_file_t *head);

/* Functions that operate on EFL (External File List)*/
H5_DLL bool   H5D__efl_is_space_alloc(const H5O_storage_t *storage);
H5_DLL herr_t H5D__efl_bh_info(H5F_t *f, H5O_efl_t *efl, hsize_t *heap_size);

/* Functions that perform fill value operations on datasets */
H5_DLL herr_t H5D__fill(const void *fill, const H5T_t *fill_type, void *buf, const H5T_t *buf_type,
                        H5S_t *space);
H5_DLL herr_t H5D__fill_init(H5D_fill_buf_info_t *fb_info, void *caller_fill_buf, H5MM_allocate_t alloc_func,
                             void *alloc_info, H5MM_free_t free_func, void *free_info, const H5O_fill_t *fill,
                             const H5T_t *dset_type, hid_t dset_type_id, size_t nelmts, size_t min_buf_size);
H5_DLL herr_t H5D__fill_refill_vl(H5D_fill_buf_info_t *fb_info, size_t nelmts);
H5_DLL herr_t H5D__fill_term(H5D_fill_buf_info_t *fb_info);

#ifdef H5_HAVE_PARALLEL

#ifdef H5D_DEBUG
#ifndef H5Dmpio_DEBUG
#define H5Dmpio_DEBUG
#endif /*H5Dmpio_DEBUG*/
#endif /*H5D_DEBUG*/
/* MPI-IO function to read multi-dsets (Chunk, Contig), it will select either
 * regular or irregular read */
H5_DLL herr_t H5D__mpio_select_read(const H5D_io_info_t *io_info, hsize_t nelmts, H5S_t *file_space,
                                    H5S_t *mem_space);

/* MPI-IO function to write multi-dsets (Chunk, Contig), it will select either
 * regular or irregular write */
H5_DLL herr_t H5D__mpio_select_write(const H5D_io_info_t *io_info, hsize_t nelmts, H5S_t *file_space,
                                     H5S_t *mem_space);

/* MPI-IO functions to handle collective IO for multiple dsets (CONTIG, CHUNK) */
H5_DLL herr_t H5D__collective_read(H5D_io_info_t *io_info);
H5_DLL herr_t H5D__collective_write(H5D_io_info_t *io_info);

/* MPI-IO function to check if a direct I/O transfer is possible between
 * memory and the file */
H5_DLL htri_t H5D__mpio_opt_possible(H5D_io_info_t *io_info);
H5_DLL herr_t H5D__mpio_get_no_coll_cause_strings(char *local_cause, size_t local_cause_len,
                                                  char *global_cause, size_t global_cause_len);

#endif /* H5_HAVE_PARALLEL */

/* Free a piece (chunk or contiguous dataset data block) info struct */
H5_DLL herr_t H5D__free_piece_info(void *item, void *key, void *opdata);

/* Testing functions */
#ifdef H5D_TESTING
H5_DLL herr_t H5D__layout_version_test(hid_t did, unsigned *version);
H5_DLL herr_t H5D__layout_contig_size_test(hid_t did, hsize_t *size);
H5_DLL herr_t H5D__layout_compact_dirty_test(hid_t did, bool *dirty);
H5_DLL herr_t H5D__layout_idx_type_test(hid_t did, H5D_chunk_index_t *idx_type);
H5_DLL herr_t H5D__layout_type_test(hid_t did, H5D_layout_t *layout_type);
H5_DLL herr_t H5D__current_cache_size_test(hid_t did, size_t *nbytes_used, int *nused);
#endif /* H5D_TESTING */

#endif /*H5Dpkg_H*/
