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
 * This file contains private information about the H5D module
 */
#ifndef H5Dprivate_H
#define H5Dprivate_H

/* Include package's public header */
#include "H5Dpublic.h"

/* Private headers needed by this file */
#include "H5FDprivate.h" /* File drivers                */
#include "H5Oprivate.h"  /* Object headers              */
#include "H5Sprivate.h"  /* Dataspaces                  */
#include "H5Zprivate.h"  /* Data filters                */

/**************************/
/* Library Private Macros */
/**************************/

/*
 * Feature: Define H5D_DEBUG on the compiler command line if you want to
 *        debug dataset I/O. NDEBUG must not be defined in order for this
 *        to have any effect.
 */
#ifdef NDEBUG
#undef H5D_DEBUG
#endif

/* ========  Dataset creation property names ======== */
#define H5D_CRT_LAYOUT_NAME            "layout"           /* Storage layout */
#define H5D_CRT_FILL_VALUE_NAME        "fill_value"       /* Fill value */
#define H5D_CRT_ALLOC_TIME_STATE_NAME  "alloc_time_state" /* Space allocation time state */
#define H5D_CRT_EXT_FILE_LIST_NAME     "efl"              /* External file list */
#define H5D_CRT_MIN_DSET_HDR_SIZE_NAME "dset_oh_minimize" /* Minimize object header */

/* ========  Dataset access property names ======== */
#define H5D_ACS_DATA_CACHE_NUM_SLOTS_NAME "rdcc_nslots"          /* Size of raw data chunk cache(slots) */
#define H5D_ACS_DATA_CACHE_BYTE_SIZE_NAME "rdcc_nbytes"          /* Size of raw data chunk cache(bytes) */
#define H5D_ACS_PREEMPT_READ_CHUNKS_NAME  "rdcc_w0"              /* Preemption read chunks first */
#define H5D_ACS_VDS_VIEW_NAME             "vds_view"             /* VDS view option */
#define H5D_ACS_VDS_PRINTF_GAP_NAME       "vds_printf_gap"       /* VDS printf gap size */
#define H5D_ACS_VDS_PREFIX_NAME           "vds_prefix"           /* VDS file prefix */
#define H5D_ACS_APPEND_FLUSH_NAME         "append_flush"         /* Append flush actions */
#define H5D_ACS_EFILE_PREFIX_NAME         "external file prefix" /* External file prefix */

/* ======== Data transfer properties ======== */
#define H5D_XFER_MAX_TEMP_BUF_NAME          "max_temp_buf"        /* Maximum temp buffer size */
#define H5D_XFER_TCONV_BUF_NAME             "tconv_buf"           /* Type conversion buffer */
#define H5D_XFER_BKGR_BUF_NAME              "bkgr_buf"            /* Background buffer */
#define H5D_XFER_BKGR_BUF_TYPE_NAME         "bkgr_buf_type"       /* Background buffer type */
#define H5D_XFER_BTREE_SPLIT_RATIO_NAME     "btree_split_ratio"   /* B-tree node splitting ratio */
#define H5D_XFER_VLEN_ALLOC_NAME            "vlen_alloc"          /* Vlen allocation function */
#define H5D_XFER_VLEN_ALLOC_INFO_NAME       "vlen_alloc_info"     /* Vlen allocation info */
#define H5D_XFER_VLEN_FREE_NAME             "vlen_free"           /* Vlen free function */
#define H5D_XFER_VLEN_FREE_INFO_NAME        "vlen_free_info"      /* Vlen free info */
#define H5D_XFER_VFL_ID_NAME                "vfl_id"              /* File driver ID */
#define H5D_XFER_VFL_INFO_NAME              "vfl_info"            /* File driver info */
#define H5D_XFER_HYPER_VECTOR_SIZE_NAME     "vec_size"            /* Hyperslab vector size */
#define H5D_XFER_IO_XFER_MODE_NAME          "io_xfer_mode"        /* I/O transfer mode */
#define H5D_XFER_MPIO_COLLECTIVE_OPT_NAME   "mpio_collective_opt" /* Optimization of MPI-IO transfer mode */
#define H5D_XFER_MPIO_CHUNK_OPT_HARD_NAME   "mpio_chunk_opt_hard"
#define H5D_XFER_MPIO_CHUNK_OPT_NUM_NAME    "mpio_chunk_opt_num"
#define H5D_XFER_MPIO_CHUNK_OPT_RATIO_NAME  "mpio_chunk_opt_ratio"
#define H5D_MPIO_ACTUAL_CHUNK_OPT_MODE_NAME "actual_chunk_opt_mode"
#define H5D_MPIO_ACTUAL_IO_MODE_NAME        "actual_io_mode"
#define H5D_MPIO_LOCAL_NO_COLLECTIVE_CAUSE_NAME                                                              \
    "local_no_collective_cause" /* cause of broken collective I/O in each process */
#define H5D_MPIO_GLOBAL_NO_COLLECTIVE_CAUSE_NAME                                                             \
    "global_no_collective_cause" /* cause of broken collective I/O in all processes */
#define H5D_XFER_EDC_NAME                      "err_detect"            /* EDC */
#define H5D_XFER_FILTER_CB_NAME                "filter_cb"             /* Filter callback function */
#define H5D_XFER_CONV_CB_NAME                  "type_conv_cb"          /* Type conversion callback function */
#define H5D_XFER_XFORM_NAME                    "data_transform"        /* Data transform */
#define H5D_XFER_DSET_IO_SEL_NAME              "dset_io_selection"     /* Dataset I/O selection */
#define H5D_XFER_SELECTION_IO_MODE_NAME        "selection_io_mode"     /* Selection I/O mode */
#define H5D_XFER_NO_SELECTION_IO_CAUSE_NAME    "no_selection_io_cause" /* Cause for no selection I/O */
#define H5D_XFER_ACTUAL_SELECTION_IO_MODE_NAME "actual_selection_io_mode" /* Actual selection I/O mode */
#define H5D_XFER_MODIFY_WRITE_BUF_NAME         "modify_write_buf"         /* Modify write buffers */
#ifdef H5_HAVE_INSTRUMENTED_LIBRARY
/* Collective chunk instrumentation properties */
#define H5D_XFER_COLL_CHUNK_LINK_HARD_NAME        "coll_chunk_link_hard"
#define H5D_XFER_COLL_CHUNK_MULTI_HARD_NAME       "coll_chunk_multi_hard"
#define H5D_XFER_COLL_CHUNK_LINK_NUM_TRUE_NAME    "coll_chunk_link_true"
#define H5D_XFER_COLL_CHUNK_LINK_NUM_FALSE_NAME   "coll_chunk_link_false"
#define H5D_XFER_COLL_CHUNK_MULTI_RATIO_COLL_NAME "coll_chunk_multi_coll"
#define H5D_XFER_COLL_CHUNK_MULTI_RATIO_IND_NAME  "coll_chunk_multi_ind"

/* Definitions for all collective chunk instrumentation properties */
#define H5D_XFER_COLL_CHUNK_SIZE sizeof(unsigned)
#define H5D_XFER_COLL_CHUNK_DEF  1

/* General collective I/O instrumentation properties */
#define H5D_XFER_COLL_RANK0_BCAST_NAME "coll_rank0_bcast"

/* Definitions for general collective I/O instrumentation properties */
#define H5D_XFER_COLL_RANK0_BCAST_SIZE sizeof(bool)
#define H5D_XFER_COLL_RANK0_BCAST_DEF  false
#endif /* H5_HAVE_INSTRUMENTED_LIBRARY */

/* Default temporary buffer size */
#define H5D_TEMP_BUF_SIZE (1024 * 1024)

/* Default I/O vector size */
#define H5D_IO_VECTOR_SIZE 1024

/* Default VL allocation & free info */
#define H5D_VLEN_ALLOC      NULL
#define H5D_VLEN_ALLOC_INFO NULL
#define H5D_VLEN_FREE       NULL
#define H5D_VLEN_FREE_INFO  NULL

/* Default virtual dataset list size */
#define H5D_VIRTUAL_DEF_LIST_SIZE 8

/****************************/
/* Library Private Typedefs */
/****************************/

/* Typedef for dataset in memory (defined in H5Dpkg.h) */
typedef struct H5D_t H5D_t;

/* Typedef for cached dataset creation property list information */
typedef struct H5D_dcpl_cache_t {
    H5O_fill_t  fill;  /* Fill value info (H5D_CRT_FILL_VALUE_NAME) */
    H5O_pline_t pline; /* I/O pipeline info (H5O_CRT_PIPELINE_NAME) */
    H5O_efl_t   efl;   /* External file list info (H5D_CRT_EXT_FILE_LIST_NAME) */
} H5D_dcpl_cache_t;

/* Callback information for copying datasets */
typedef struct H5D_copy_file_ud_t {
    H5O_copy_file_ud_common_t common;           /* Shared information (must be first) */
    struct H5S_extent_t      *src_space_extent; /* Copy of dataspace extent for dataset */
    H5T_t                    *src_dtype;        /* Copy of datatype for dataset */
} H5D_copy_file_ud_t;

/* Structure for dataset append flush property (H5Pset_append_flush) */
typedef struct H5D_append_flush_t {
    unsigned        ndims;                  /* The # of dimensions for "boundary" */
    hsize_t         boundary[H5S_MAX_RANK]; /* The dimension sizes for determining boundary */
    H5D_append_cb_t func;                   /* The callback function */
    void           *udata;                  /* User data */
} H5D_append_flush_t;

/*****************************/
/* Library Private Variables */
/*****************************/

/******************************/
/* Library Private Prototypes */
/******************************/

H5_DLL herr_t      H5D_init(void);
H5_DLL H5D_t      *H5D_open(const H5G_loc_t *loc, hid_t dapl_id);
H5_DLL herr_t      H5D_close(H5D_t *dataset);
H5_DLL herr_t      H5D_mult_refresh_close(hid_t dset_id);
H5_DLL herr_t      H5D_mult_refresh_reopen(H5D_t *dataset);
H5_DLL H5O_loc_t  *H5D_oloc(H5D_t *dataset);
H5_DLL H5G_name_t *H5D_nameof(H5D_t *dataset);
H5_DLL herr_t      H5D_flush_all(H5F_t *f);
H5_DLL hid_t       H5D_get_create_plist(const H5D_t *dset);
H5_DLL hid_t       H5D_get_access_plist(const H5D_t *dset);

/* Functions that operate on chunked storage */
H5_DLL herr_t H5D_chunk_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr);

/* Functions that operate on virtual storage */
H5_DLL herr_t H5D_virtual_check_mapping_pre(const H5S_t *vspace, const H5S_t *src_space,
                                            H5O_virtual_space_status_t space_status);
H5_DLL herr_t H5D_virtual_check_mapping_post(const H5O_storage_virtual_ent_t *ent);
H5_DLL herr_t H5D_virtual_check_min_dims(const H5D_t *dset);
H5_DLL herr_t H5D_virtual_update_min_dims(H5O_layout_t *layout, size_t idx);
H5_DLL herr_t H5D_virtual_parse_source_name(const char                      *source_name,
                                            H5O_storage_virtual_name_seg_t **parsed_name,
                                            size_t *static_strlen, size_t *nsubs);
H5_DLL herr_t H5D_virtual_free_parsed_name(H5O_storage_virtual_name_seg_t *name_seg);

/* Functions that operate on indexed storage */
H5_DLL herr_t H5D_btree_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, unsigned ndims,
                              const uint32_t *dim);

#endif /* H5Dprivate_H */
