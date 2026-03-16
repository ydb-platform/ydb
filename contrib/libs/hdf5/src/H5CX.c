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
 * Purpose:
 *      Keep a set of "psuedo-global" information for an API call.  This
 *      general corresponds to the DXPL for the call, along with cached
 *      information from them.
 */

/****************/
/* Module Setup */
/****************/

#include "H5CXmodule.h" /* This source code file is part of the H5CX module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                    */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Dprivate.h"  /* Datasets                             */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Iprivate.h"  /* IDs                                  */
#include "H5Lprivate.h"  /* Links                                */
#include "H5MMprivate.h" /* Memory management                    */
#include "H5Pprivate.h"  /* Property lists                       */

/****************/
/* Local Macros */
/****************/

#ifdef H5_HAVE_THREADSAFE
/*
 * The per-thread API context. pthread_once() initializes a special
 * key that will be used by all threads to create a stack specific to
 * each thread individually. The association of contexts to threads will
 * be handled by the pthread library.
 *
 * In order for this macro to work, H5CX_get_my_context() must be preceded
 * by "H5CX_node_t *ctx =".
 */
#define H5CX_get_my_context() H5CX__get_context()
#else /* H5_HAVE_THREADSAFE */
/*
 * The current API context.
 */
#define H5CX_get_my_context() (&H5CX_head_g)
#endif /* H5_HAVE_THREADSAFE */

/* Common macro for the retrieving the pointer to a property list */
#define H5CX_RETRIEVE_PLIST(PL, FAILVAL)                                                                     \
    /* Check if the property list is already available */                                                    \
    if (NULL == (*head)->ctx.PL)                                                                             \
        /* Get the property list pointer */                                                                  \
        if (NULL == ((*head)->ctx.PL = (H5P_genplist_t *)H5I_object((*head)->ctx.H5_GLUE(PL, _id))))         \
            HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, (FAILVAL), "can't get property list");

/* Common macro for the duplicated code to retrieve properties from a property list */
#define H5CX_RETRIEVE_PROP_COMMON(PL, DEF_PL, PROP_NAME, PROP_FIELD)                                         \
    /* Check for default property list */                                                                    \
    if ((*head)->ctx.H5_GLUE(PL, _id) == (DEF_PL))                                                           \
        H5MM_memcpy(&(*head)->ctx.PROP_FIELD, &H5_GLUE3(H5CX_def_, PL, _cache).PROP_FIELD,                   \
                    sizeof(H5_GLUE3(H5CX_def_, PL, _cache).PROP_FIELD));                                     \
    else {                                                                                                   \
        /* Retrieve the property list */                                                                     \
        H5CX_RETRIEVE_PLIST(PL, FAIL)                                                                        \
                                                                                                             \
        /* Get the property */                                                                               \
        if (H5P_get((*head)->ctx.PL, (PROP_NAME), &(*head)->ctx.PROP_FIELD) < 0)                             \
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "can't retrieve value from API context");            \
    } /* end else */                                                                                         \
                                                                                                             \
    /* Mark the field as valid */                                                                            \
    (*head)->ctx.H5_GLUE(PROP_FIELD, _valid) = true;

/* Macro for the duplicated code to retrieve properties from a property list */
#define H5CX_RETRIEVE_PROP_VALID(PL, DEF_PL, PROP_NAME, PROP_FIELD)                                          \
    /* Check if the value has been retrieved already */                                                      \
    if (!(*head)->ctx.H5_GLUE(PROP_FIELD, _valid)) {                                                         \
        H5CX_RETRIEVE_PROP_COMMON(PL, DEF_PL, PROP_NAME, PROP_FIELD)                                         \
    } /* end if */

/* Macro for the duplicated code to retrieve possibly set properties from a property list */
#define H5CX_RETRIEVE_PROP_VALID_SET(PL, DEF_PL, PROP_NAME, PROP_FIELD)                                      \
    /* Check if the value has been retrieved already */                                                      \
    if (!((*head)->ctx.H5_GLUE(PROP_FIELD, _valid) || (*head)->ctx.H5_GLUE(PROP_FIELD, _set))) {             \
        H5CX_RETRIEVE_PROP_COMMON(PL, DEF_PL, PROP_NAME, PROP_FIELD)                                         \
    } /* end if */

#if defined(H5_HAVE_PARALLEL) && defined(H5_HAVE_INSTRUMENTED_LIBRARY)
/* Macro for the duplicated code to test and set properties for a property list */
#define H5CX_TEST_SET_PROP(PROP_NAME, PROP_FIELD)                                                            \
    {                                                                                                        \
        htri_t check_prop = 0; /* Whether the property exists in the API context's DXPL */                   \
                                                                                                             \
        /* Check if property exists in DXPL */                                                               \
        if (!(*head)->ctx.H5_GLUE(PROP_FIELD, _set)) {                                                       \
            /* Retrieve the dataset transfer property list */                                                \
            H5CX_RETRIEVE_PLIST(dxpl, FAIL)                                                                  \
                                                                                                             \
            if ((check_prop = H5P_exist_plist((*head)->ctx.dxpl, PROP_NAME)) < 0)                            \
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "error checking for property");                  \
        } /* end if */                                                                                       \
                                                                                                             \
        /* If property was already set or exists (for first set), update it */                               \
        if ((*head)->ctx.H5_GLUE(PROP_FIELD, _set) || check_prop > 0) {                                      \
            /* Cache the value for later, marking it to set in DXPL when context popped */                   \
            (*head)->ctx.PROP_FIELD                = PROP_FIELD;                                             \
            (*head)->ctx.H5_GLUE(PROP_FIELD, _set) = true;                                                   \
        } /* end if */                                                                                       \
    }
#endif /* defined(H5_HAVE_PARALLEL) && defined(H5_HAVE_INSTRUMENTED_LIBRARY) */

/* Macro for the duplicated code to test and set properties for a property list */
#define H5CX_SET_PROP(PROP_NAME, PROP_FIELD)                                                                 \
    if ((*head)->ctx.H5_GLUE(PROP_FIELD, _set)) {                                                            \
        /* Retrieve the dataset transfer property list */                                                    \
        H5CX_RETRIEVE_PLIST(dxpl, NULL)                                                                      \
                                                                                                             \
        /* Set the property */                                                                               \
        if (H5P_set((*head)->ctx.dxpl, PROP_NAME, &(*head)->ctx.PROP_FIELD) < 0)                             \
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTSET, NULL, "error setting data xfer property");                 \
    } /* end if */

/******************/
/* Local Typedefs */
/******************/

/* Typedef for context about each API call, as it proceeds */
/* Fields in this struct are of several types:
 * - The DXPL & LAPL ID are either library default ones (from the API context
 *      initialization) or passed in from the application via an API call
 *      parameter.  The corresponding H5P_genplist_t* is just the underlying
 *      property list struct for the ID, to optimize retrieving properties
 *      from the list multiple times.
 *
 * - Internal fields, used and set only within the library, for managing the
 *      operation under way.  These do not correspond to properties in the
 *      DXPL or LAPL and can have any name.
 *
 * - Cached fields, which are not returned to the application, for managing
 *      the operation under way.  These correspond to properties in the DXPL
 *      or LAPL, and are retrieved either from the (global) cache for a
 *      default property list, or from the corresponding property in the
 *      application's (non-default) property list.  Getting / setting these
 *      properties within the library does _not_ affect the application's
 *      property list.  Note that the naming of these fields, <foo> and
 *      <foo>_valid, is important for the H5CX_RETRIEVE_PROP_VALID ahd
 *      H5CX_RETRIEVE_PROP_VALID_SET macros to work properly.
 *
 * - "Return-only"" properties that are returned to the application, mainly
 *      for sending out "introspection" information ("Why did collective I/O
 *      get broken for this operation?", "Which filters are set on the chunk I
 *      just directly read in?", etc) Setting these fields will cause the
 *      corresponding property in the property list to be set when the API
 *      context is popped, when returning from the API routine.  Note that the
 *      naming of these fields, <foo> and <foo>_set, is important for the
 *      H5CX_TEST_SET_PROP and H5CX_SET_PROP macros to work properly.
 */
typedef struct H5CX_t {
    /* DXPL */
    hid_t           dxpl_id; /* DXPL ID for API operation */
    H5P_genplist_t *dxpl;    /* Dataset Transfer Property List */

    /* LCPL */
    hid_t           lcpl_id; /* LCPL ID for API operation */
    H5P_genplist_t *lcpl;    /* Link Creation Property List */

    /* LAPL */
    hid_t           lapl_id; /* LAPL ID for API operation */
    H5P_genplist_t *lapl;    /* Link Access Property List */

    /* DCPL */
    hid_t           dcpl_id; /* DCPL ID for API operation */
    H5P_genplist_t *dcpl;    /* Dataset Creation Property List */

    /* DAPL */
    hid_t           dapl_id; /* DAPL ID for API operation */
    H5P_genplist_t *dapl;    /* Dataset Access Property List */

    /* FAPL */
    hid_t           fapl_id; /* FAPL ID for API operation */
    H5P_genplist_t *fapl;    /* File Access Property List */

    /* Internal: Object tagging info */
    haddr_t tag; /* Current object's tag (ohdr chunk #0 address) */

    /* Internal: Metadata cache info */
    H5AC_ring_t ring; /* Current metadata cache ring for entries */

#ifdef H5_HAVE_PARALLEL
    /* Internal: Parallel I/O settings */
    bool         coll_metadata_read; /* Whether to use collective I/O for metadata read */
    MPI_Datatype btype;              /* MPI datatype for buffer, when using collective I/O */
    MPI_Datatype ftype;              /* MPI datatype for file, when using collective I/O */
    bool         mpi_file_flushing;  /* Whether an MPI-opened file is being flushed */
    bool         rank0_bcast;        /* Whether a dataset meets read-with-rank0-and-bcast requirements */
#endif                               /* H5_HAVE_PARALLEL */

    /* Cached DXPL properties */
    size_t    max_temp_buf;            /* Maximum temporary buffer size */
    bool      max_temp_buf_valid;      /* Whether maximum temporary buffer size is valid */
    void     *tconv_buf;               /* Temporary conversion buffer (H5D_XFER_TCONV_BUF_NAME) */
    bool      tconv_buf_valid;         /* Whether temporary conversion buffer is valid */
    void     *bkgr_buf;                /* Background conversion buffer (H5D_XFER_BKGR_BUF_NAME) */
    bool      bkgr_buf_valid;          /* Whether background conversion buffer is valid */
    H5T_bkg_t bkgr_buf_type;           /* Background buffer type (H5D_XFER_BKGR_BUF_NAME) */
    bool      bkgr_buf_type_valid;     /* Whether background buffer type is valid */
    double    btree_split_ratio[3];    /* B-tree split ratios */
    bool      btree_split_ratio_valid; /* Whether B-tree split ratios are valid */
    size_t    vec_size;                /* Size of hyperslab vector (H5D_XFER_HYPER_VECTOR_SIZE_NAME) */
    bool      vec_size_valid;          /* Whether hyperslab vector is valid */
#ifdef H5_HAVE_PARALLEL
    H5FD_mpio_xfer_t io_xfer_mode; /* Parallel transfer mode for this request (H5D_XFER_IO_XFER_MODE_NAME) */
    bool             io_xfer_mode_valid;      /* Whether parallel transfer mode is valid */
    H5FD_mpio_collective_opt_t mpio_coll_opt; /* Parallel transfer with independent IO or collective IO with
                                                 this mode (H5D_XFER_MPIO_COLLECTIVE_OPT_NAME) */
    bool mpio_coll_opt_valid;                 /* Whether parallel transfer option is valid */
    H5FD_mpio_chunk_opt_t
             mpio_chunk_opt_mode;        /* Collective chunk option (H5D_XFER_MPIO_CHUNK_OPT_HARD_NAME) */
    bool     mpio_chunk_opt_mode_valid;  /* Whether collective chunk option is valid */
    unsigned mpio_chunk_opt_num;         /* Collective chunk threshold (H5D_XFER_MPIO_CHUNK_OPT_NUM_NAME) */
    bool     mpio_chunk_opt_num_valid;   /* Whether collective chunk threshold is valid */
    unsigned mpio_chunk_opt_ratio;       /* Collective chunk ratio (H5D_XFER_MPIO_CHUNK_OPT_RATIO_NAME) */
    bool     mpio_chunk_opt_ratio_valid; /* Whether collective chunk ratio is valid */
#endif                                   /* H5_HAVE_PARALLEL */
    H5Z_EDC_t               err_detect;  /* Error detection info (H5D_XFER_EDC_NAME) */
    bool                    err_detect_valid;     /* Whether error detection info is valid */
    H5Z_cb_t                filter_cb;            /* Filter callback function (H5D_XFER_FILTER_CB_NAME) */
    bool                    filter_cb_valid;      /* Whether filter callback function is valid */
    H5Z_data_xform_t       *data_transform;       /* Data transform info (H5D_XFER_XFORM_NAME) */
    bool                    data_transform_valid; /* Whether data transform info is valid */
    H5T_vlen_alloc_info_t   vl_alloc_info;        /* VL datatype alloc info (H5D_XFER_VLEN_*_NAME) */
    bool                    vl_alloc_info_valid;  /* Whether VL datatype alloc info is valid */
    H5T_conv_cb_t           dt_conv_cb;           /* Datatype conversion struct (H5D_XFER_CONV_CB_NAME) */
    bool                    dt_conv_cb_valid;     /* Whether datatype conversion struct is valid */
    H5D_selection_io_mode_t selection_io_mode;    /* Selection I/O mode (H5D_XFER_SELECTION_IO_MODE_NAME) */
    bool                    selection_io_mode_valid; /* Whether selection I/O mode is valid */
    bool                    modify_write_buf;        /* Whether the library can modify write buffers */
    bool                    modify_write_buf_valid;  /* Whether the modify_write_buf field is valid */

    /* Return-only DXPL properties to return to application */
#ifdef H5_HAVE_PARALLEL
    H5D_mpio_actual_chunk_opt_mode_t mpio_actual_chunk_opt; /* Chunk optimization mode used for parallel I/O
                                                               (H5D_MPIO_ACTUAL_CHUNK_OPT_MODE_NAME) */
    bool mpio_actual_chunk_opt_set; /* Whether chunk optimization mode used for parallel I/O is set */
    H5D_mpio_actual_io_mode_t
             mpio_actual_io_mode; /* Actual I/O mode used for parallel I/O (H5D_MPIO_ACTUAL_IO_MODE_NAME) */
    bool     mpio_actual_io_mode_set;        /* Whether actual I/O mode used for parallel I/O is set */
    uint32_t mpio_local_no_coll_cause;       /* Local reason for breaking collective I/O
                                                (H5D_MPIO_LOCAL_NO_COLLECTIVE_CAUSE_NAME) */
    bool     mpio_local_no_coll_cause_set;   /* Whether local reason for breaking collective I/O is set */
    bool     mpio_local_no_coll_cause_valid; /* Whether local reason for breaking collective I/O is valid */
    uint32_t mpio_global_no_coll_cause;      /* Global reason for breaking collective I/O
                                                (H5D_MPIO_GLOBAL_NO_COLLECTIVE_CAUSE_NAME) */
    bool mpio_global_no_coll_cause_set;      /* Whether global reason for breaking collective I/O is set */
    bool mpio_global_no_coll_cause_valid;    /* Whether global reason for breaking collective I/O is valid */
#ifdef H5_HAVE_INSTRUMENTED_LIBRARY
    int mpio_coll_chunk_link_hard;       /* Instrumented "collective chunk link hard" value
                                            (H5D_XFER_COLL_CHUNK_LINK_HARD_NAME) */
    bool mpio_coll_chunk_link_hard_set;  /* Whether instrumented "collective chunk link hard" value is set */
    int  mpio_coll_chunk_multi_hard;     /* Instrumented "collective chunk multi hard" value
                                            (H5D_XFER_COLL_CHUNK_MULTI_HARD_NAME) */
    bool mpio_coll_chunk_multi_hard_set; /* Whether instrumented "collective chunk multi hard" value is set */
    int  mpio_coll_chunk_link_num_true;  /* Instrumented "collective chunk link num true" value
                                            (H5D_XFER_COLL_CHUNK_LINK_NUM_TRUE_NAME) */
    bool mpio_coll_chunk_link_num_true_set;    /* Whether instrumented "collective chunk link num true" value
                                                     is set */
    int mpio_coll_chunk_link_num_false;        /* Instrumented "collective chunk link num false" value
                                                  (H5D_XFER_COLL_CHUNK_LINK_NUM_FALSE_NAME) */
    bool mpio_coll_chunk_link_num_false_set;   /* Whether instrumented "collective chunk link num false"
                                                     value is set */
    int mpio_coll_chunk_multi_ratio_coll;      /* Instrumented "collective chunk multi ratio coll" value
                                                  (H5D_XFER_COLL_CHUNK_MULTI_RATIO_COLL_NAME) */
    bool mpio_coll_chunk_multi_ratio_coll_set; /* Whether instrumented "collective chunk multi ratio coll"
                                                     value is set */
    int mpio_coll_chunk_multi_ratio_ind;       /* Instrumented "collective chunk multi ratio ind" value
                                                  (H5D_XFER_COLL_CHUNK_MULTI_RATIO_IND_NAME) */
    bool mpio_coll_chunk_multi_ratio_ind_set;  /* Whether instrumented "collective chunk multi ratio ind"
                                                     value is set */
    bool mpio_coll_rank0_bcast;                /* Instrumented "collective chunk multi ratio ind" value
                                                     (H5D_XFER_COLL_CHUNK_MULTI_RATIO_IND_NAME) */
    bool mpio_coll_rank0_bcast_set; /* Whether instrumented "collective chunk multi ratio ind" value is set */
#endif                              /* H5_HAVE_INSTRUMENTED_LIBRARY */
#endif                              /* H5_HAVE_PARALLEL */
    uint32_t no_selection_io_cause; /* Reason for not performing selection I/O
                                          (H5D_XFER_NO_SELECTION_IO_CAUSE_NAME) */
    bool no_selection_io_cause_set; /* Whether reason for not performing selection I/O is set */
    bool no_selection_io_cause_valid; /* Whether reason for not performing selection I/O is valid */

    uint32_t
            actual_selection_io_mode; /* Actual selection I/O mode used (H5D_ACTUAL_SELECTION_IO_MODE_NAME) */
    hbool_t actual_selection_io_mode_set;   /* Whether actual selection I/O mode is set */
    hbool_t actual_selection_io_mode_valid; /* Whether actual selection I/O mode is valid */

    /* Cached LCPL properties */
    H5T_cset_t encoding;                 /* Link name character encoding */
    bool       encoding_valid;           /* Whether link name character encoding is valid */
    unsigned   intermediate_group;       /* Whether to create intermediate groups */
    bool       intermediate_group_valid; /* Whether create intermediate group flag is valid */

    /* Cached LAPL properties */
    size_t nlinks;       /* Number of soft / UD links to traverse (H5L_ACS_NLINKS_NAME) */
    bool   nlinks_valid; /* Whether number of soft / UD links to traverse is valid */

    /* Cached DCPL properties */
    bool    do_min_dset_ohdr;       /* Whether to minimize dataset object header */
    bool    do_min_dset_ohdr_valid; /* Whether minimize dataset object header flag is valid */
    uint8_t ohdr_flags;             /* Object header flags */
    bool    ohdr_flags_valid;       /* Whether the object headers flags are valid */

    /* Cached DAPL properties */
    const char *extfile_prefix;       /* Prefix for external file                      */
    bool        extfile_prefix_valid; /* Whether the prefix for external file is valid */
    const char *vds_prefix;           /* Prefix for VDS                                */
    bool        vds_prefix_valid;     /* Whether the prefix for VDS is valid           */

    /* Cached FAPL properties */
    H5F_libver_t low_bound;        /* low_bound property for H5Pset_libver_bounds() */
    bool         low_bound_valid;  /* Whether low_bound property is valid */
    H5F_libver_t high_bound;       /* high_bound property for H5Pset_libver_bounds */
    bool         high_bound_valid; /* Whether high_bound property is valid */

    /* Cached VOL settings */
    H5VL_connector_prop_t vol_connector_prop; /* Property for VOL connector ID & info */
    bool  vol_connector_prop_valid;           /* Whether property for VOL connector ID & info is valid */
    void *vol_wrap_ctx;                       /* VOL connector's "wrap context" for creating IDs */
    bool  vol_wrap_ctx_valid; /* Whether VOL connector's "wrap context" for creating IDs is valid */
} H5CX_t;

/* Typedef for nodes on the API context stack */
/* Each entry into the library through an API routine invokes H5CX_push()
 * in a FUNC_ENTER_API* macro, which pushes an H5CX_node_t on the API
 * context [thread-local] stack, after initializing it with default values
 * in H5CX__push_common().
 */
typedef struct H5CX_node_t {
    H5CX_t              ctx;  /* Context for current API call */
    struct H5CX_node_t *next; /* Pointer to previous context, on stack */
} H5CX_node_t;

/* Typedef for cached default dataset transfer property list information */
/* This is initialized to the values in the default DXPL during package
 * initialization and then remains constant for the rest of the library's
 * operation.  When a field in H5CX_t is retrieved from an API context that
 * uses a default DXPL, this value is copied instead of spending time looking
 * up the property in the DXPL.
 */
typedef struct H5CX_dxpl_cache_t {
    size_t    max_temp_buf;         /* Maximum temporary buffer size (H5D_XFER_MAX_TEMP_BUF_NAME) */
    void     *tconv_buf;            /* Temporary conversion buffer (H5D_XFER_TCONV_BUF_NAME) */
    void     *bkgr_buf;             /* Background conversion buffer (H5D_XFER_BKGR_BUF_NAME) */
    H5T_bkg_t bkgr_buf_type;        /* Background buffer type (H5D_XFER_BKGR_BUF_NAME) */
    double    btree_split_ratio[3]; /* B-tree split ratios (H5D_XFER_BTREE_SPLIT_RATIO_NAME) */
    size_t    vec_size;             /* Size of hyperslab vector (H5D_XFER_HYPER_VECTOR_SIZE_NAME) */
#ifdef H5_HAVE_PARALLEL
    H5FD_mpio_xfer_t io_xfer_mode; /* Parallel transfer mode for this request (H5D_XFER_IO_XFER_MODE_NAME) */
    H5FD_mpio_collective_opt_t mpio_coll_opt; /* Parallel transfer with independent IO or collective IO with
                                                 this mode (H5D_XFER_MPIO_COLLECTIVE_OPT_NAME) */
    uint32_t mpio_local_no_coll_cause;        /* Local reason for breaking collective I/O
                                                 (H5D_MPIO_LOCAL_NO_COLLECTIVE_CAUSE_NAME) */
    uint32_t mpio_global_no_coll_cause;       /* Global reason for breaking collective I/O
                                                 (H5D_MPIO_GLOBAL_NO_COLLECTIVE_CAUSE_NAME) */
    H5FD_mpio_chunk_opt_t
             mpio_chunk_opt_mode;       /* Collective chunk option (H5D_XFER_MPIO_CHUNK_OPT_HARD_NAME) */
    unsigned mpio_chunk_opt_num;        /* Collective chunk threshold (H5D_XFER_MPIO_CHUNK_OPT_NUM_NAME) */
    unsigned mpio_chunk_opt_ratio;      /* Collective chunk ratio (H5D_XFER_MPIO_CHUNK_OPT_RATIO_NAME) */
#endif                                  /* H5_HAVE_PARALLEL */
    H5Z_EDC_t               err_detect; /* Error detection info (H5D_XFER_EDC_NAME) */
    H5Z_cb_t                filter_cb;  /* Filter callback function (H5D_XFER_FILTER_CB_NAME) */
    H5Z_data_xform_t       *data_transform;        /* Data transform info (H5D_XFER_XFORM_NAME) */
    H5T_vlen_alloc_info_t   vl_alloc_info;         /* VL datatype alloc info (H5D_XFER_VLEN_*_NAME) */
    H5T_conv_cb_t           dt_conv_cb;            /* Datatype conversion struct (H5D_XFER_CONV_CB_NAME) */
    H5D_selection_io_mode_t selection_io_mode;     /* Selection I/O mode (H5D_XFER_SELECTION_IO_MODE_NAME) */
    uint32_t                no_selection_io_cause; /* Reasons for not performing selection I/O
                                                            (H5D_XFER_NO_SELECTION_IO_CAUSE_NAME) */
    uint32_t actual_selection_io_mode;             /* Actual selection I/O mode
                                                         (H5D_XFER_ACTUAL_SELECTION_IO_MODE_NAME) */
    bool modify_write_buf;                         /* Whether the library can modify write buffers */
} H5CX_dxpl_cache_t;

/* Typedef for cached default link creation property list information */
/* (Same as the cached DXPL struct, above, except for the default LCPL) */
typedef struct H5CX_lcpl_cache_t {
    H5T_cset_t encoding;           /* Link name character encoding */
    unsigned   intermediate_group; /* Whether to create intermediate groups  */
} H5CX_lcpl_cache_t;

/* Typedef for cached default link access property list information */
/* (Same as the cached DXPL struct, above, except for the default LAPL) */
typedef struct H5CX_lapl_cache_t {
    size_t nlinks; /* Number of soft / UD links to traverse (H5L_ACS_NLINKS_NAME) */
} H5CX_lapl_cache_t;

/* Typedef for cached default dataset creation property list information */
/* (Same as the cached DXPL struct, above, except for the default DCPL) */
typedef struct H5CX_dcpl_cache_t {
    bool    do_min_dset_ohdr; /* Whether to minimize dataset object header */
    uint8_t ohdr_flags;       /* Object header flags */
} H5CX_dcpl_cache_t;

/* Typedef for cached default dataset access property list information */
/* (Same as the cached DXPL struct, above, except for the default DXPL) */
typedef struct H5CX_dapl_cache_t {
    const char *extfile_prefix; /* Prefix for external file */
    const char *vds_prefix;     /* Prefix for VDS           */
} H5CX_dapl_cache_t;

/* Typedef for cached default file access property list information */
/* (Same as the cached DXPL struct, above, except for the default DCPL) */
typedef struct H5CX_fapl_cache_t {
    H5F_libver_t low_bound;  /* low_bound property for H5Pset_libver_bounds() */
    H5F_libver_t high_bound; /* high_bound property for H5Pset_libver_bounds */
} H5CX_fapl_cache_t;

/********************/
/* Local Prototypes */
/********************/
#ifdef H5_HAVE_THREADSAFE
static H5CX_node_t **H5CX__get_context(void);
#endif /* H5_HAVE_THREADSAFE */
static void         H5CX__push_common(H5CX_node_t *cnode);
static H5CX_node_t *H5CX__pop_common(bool update_dxpl_props);

/*********************/
/* Package Variables */
/*********************/

/*******************/
/* Local Variables */
/*******************/

#ifndef H5_HAVE_THREADSAFE
static H5CX_node_t *H5CX_head_g = NULL; /* Pointer to head of context stack */
#endif                                  /* H5_HAVE_THREADSAFE */

/* Define a "default" dataset transfer property list cache structure to use for default DXPLs */
static H5CX_dxpl_cache_t H5CX_def_dxpl_cache;

/* Define a "default" link creation property list cache structure to use for default LCPLs */
static H5CX_lcpl_cache_t H5CX_def_lcpl_cache;

/* Define a "default" link access property list cache structure to use for default LAPLs */
static H5CX_lapl_cache_t H5CX_def_lapl_cache;

/* Define a "default" dataset creation property list cache structure to use for default DCPLs */
static H5CX_dcpl_cache_t H5CX_def_dcpl_cache;

/* Define a "default" dataset access property list cache structure to use for default DAPLs */
static H5CX_dapl_cache_t H5CX_def_dapl_cache;

/* Define a "default" file access property list cache structure to use for default FAPLs */
static H5CX_fapl_cache_t H5CX_def_fapl_cache;

/* Declare a static free list to manage H5CX_node_t structs */
H5FL_DEFINE_STATIC(H5CX_node_t);

/* Declare a static free list to manage H5CX_state_t structs */
H5FL_DEFINE_STATIC(H5CX_state_t);

/*-------------------------------------------------------------------------
 * Function:    H5CX_init
 *
 * Purpose:     Initialize the interface from some other layer.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_init(void)
{
    H5P_genplist_t *dx_plist;            /* Data transfer property list */
    H5P_genplist_t *lc_plist;            /* Link creation property list */
    H5P_genplist_t *la_plist;            /* Link access property list */
    H5P_genplist_t *dc_plist;            /* Dataset creation property list */
    H5P_genplist_t *da_plist;            /* Dataset access property list */
    H5P_genplist_t *fa_plist;            /* File access property list */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Reset the "default DXPL cache" information */
    memset(&H5CX_def_dxpl_cache, 0, sizeof(H5CX_dxpl_cache_t));

    /* Get the default DXPL cache information */

    /* Get the default dataset transfer property list */
    if (NULL == (dx_plist = (H5P_genplist_t *)H5I_object(H5P_DATASET_XFER_DEFAULT)))
        HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL, "not a dataset transfer property list");

    /* Get B-tree split ratios */
    if (H5P_get(dx_plist, H5D_XFER_BTREE_SPLIT_RATIO_NAME, &H5CX_def_dxpl_cache.btree_split_ratio) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve B-tree split ratios");

    /* Get maximum temporary buffer size value */
    if (H5P_get(dx_plist, H5D_XFER_MAX_TEMP_BUF_NAME, &H5CX_def_dxpl_cache.max_temp_buf) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve maximum temporary buffer size");

    /* Get temporary buffer pointer */
    if (H5P_get(dx_plist, H5D_XFER_TCONV_BUF_NAME, &H5CX_def_dxpl_cache.tconv_buf) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve temporary buffer pointer");

    /* Get background buffer pointer */
    if (H5P_get(dx_plist, H5D_XFER_BKGR_BUF_NAME, &H5CX_def_dxpl_cache.bkgr_buf) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve background buffer pointer");

    /* Get background buffer type */
    if (H5P_get(dx_plist, H5D_XFER_BKGR_BUF_TYPE_NAME, &H5CX_def_dxpl_cache.bkgr_buf_type) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve background buffer type");

    /* Get I/O vector size */
    if (H5P_get(dx_plist, H5D_XFER_HYPER_VECTOR_SIZE_NAME, &H5CX_def_dxpl_cache.vec_size) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve I/O vector size");

#ifdef H5_HAVE_PARALLEL
    /* Collect Parallel I/O information for possible later use */
    if (H5P_get(dx_plist, H5D_XFER_IO_XFER_MODE_NAME, &H5CX_def_dxpl_cache.io_xfer_mode) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve parallel transfer method");
    if (H5P_get(dx_plist, H5D_XFER_MPIO_COLLECTIVE_OPT_NAME, &H5CX_def_dxpl_cache.mpio_coll_opt) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve collective transfer option");
    if (H5P_get(dx_plist, H5D_XFER_MPIO_CHUNK_OPT_HARD_NAME, &H5CX_def_dxpl_cache.mpio_chunk_opt_mode) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve chunk optimization option");
    if (H5P_get(dx_plist, H5D_XFER_MPIO_CHUNK_OPT_NUM_NAME, &H5CX_def_dxpl_cache.mpio_chunk_opt_num) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve chunk optimization threshold");
    if (H5P_get(dx_plist, H5D_XFER_MPIO_CHUNK_OPT_RATIO_NAME, &H5CX_def_dxpl_cache.mpio_chunk_opt_ratio) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve chunk optimization ratio");

    /* Get the local & global reasons for breaking collective I/O values */
    if (H5P_get(dx_plist, H5D_MPIO_LOCAL_NO_COLLECTIVE_CAUSE_NAME,
                &H5CX_def_dxpl_cache.mpio_local_no_coll_cause) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve local cause for breaking collective I/O");
    if (H5P_get(dx_plist, H5D_MPIO_GLOBAL_NO_COLLECTIVE_CAUSE_NAME,
                &H5CX_def_dxpl_cache.mpio_global_no_coll_cause) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL,
                    "Can't retrieve global cause for breaking collective I/O");
#endif /* H5_HAVE_PARALLEL */

    /* Get error detection properties */
    if (H5P_get(dx_plist, H5D_XFER_EDC_NAME, &H5CX_def_dxpl_cache.err_detect) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve error detection info");

    /* Get filter callback function */
    if (H5P_get(dx_plist, H5D_XFER_FILTER_CB_NAME, &H5CX_def_dxpl_cache.filter_cb) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve filter callback function");

    /* Look at the data transform property */
    /* (Note: 'peek', not 'get' - if this turns out to be a problem, we may need
     *          to copy it and free this in the H5CX terminate routine. -QAK)
     */
    if (H5P_peek(dx_plist, H5D_XFER_XFORM_NAME, &H5CX_def_dxpl_cache.data_transform) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve data transform info");

    /* Get VL datatype alloc info */
    if (H5P_get(dx_plist, H5D_XFER_VLEN_ALLOC_NAME, &H5CX_def_dxpl_cache.vl_alloc_info.alloc_func) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve VL datatype alloc info");
    if (H5P_get(dx_plist, H5D_XFER_VLEN_ALLOC_INFO_NAME, &H5CX_def_dxpl_cache.vl_alloc_info.alloc_info) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve VL datatype alloc info");
    if (H5P_get(dx_plist, H5D_XFER_VLEN_FREE_NAME, &H5CX_def_dxpl_cache.vl_alloc_info.free_func) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve VL datatype alloc info");
    if (H5P_get(dx_plist, H5D_XFER_VLEN_FREE_INFO_NAME, &H5CX_def_dxpl_cache.vl_alloc_info.free_info) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve VL datatype alloc info");

    /* Get datatype conversion struct */
    if (H5P_get(dx_plist, H5D_XFER_CONV_CB_NAME, &H5CX_def_dxpl_cache.dt_conv_cb) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve datatype conversion exception callback");

    /* Get the selection I/O mode */
    if (H5P_get(dx_plist, H5D_XFER_SELECTION_IO_MODE_NAME, &H5CX_def_dxpl_cache.selection_io_mode) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve selection I/O mode");

    /* Get the local & global reasons for breaking selection I/O values */
    if (H5P_get(dx_plist, H5D_XFER_NO_SELECTION_IO_CAUSE_NAME, &H5CX_def_dxpl_cache.no_selection_io_cause) <
        0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve cause for no selection I/O");

    /* Get the actual selection I/O mode */
    if (H5P_get(dx_plist, H5D_XFER_ACTUAL_SELECTION_IO_MODE_NAME,
                &H5CX_def_dxpl_cache.actual_selection_io_mode) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve actual selection I/O mode");

    /* Get the modify write buffer property */
    if (H5P_get(dx_plist, H5D_XFER_MODIFY_WRITE_BUF_NAME, &H5CX_def_dxpl_cache.modify_write_buf) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve modify write buffer property");

    /* Reset the "default LCPL cache" information */
    memset(&H5CX_def_lcpl_cache, 0, sizeof(H5CX_lcpl_cache_t));

    /* Get the default LCPL cache information */

    /* Get the default link creation property list */
    if (NULL == (lc_plist = (H5P_genplist_t *)H5I_object(H5P_LINK_CREATE_DEFAULT)))
        HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL, "not a link creation property list");

    /* Get link name character encoding */
    if (H5P_get(lc_plist, H5P_STRCRT_CHAR_ENCODING_NAME, &H5CX_def_lcpl_cache.encoding) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve link name encoding");

    /* Get flag whether to create intermediate groups */
    if (H5P_get(lc_plist, H5L_CRT_INTERMEDIATE_GROUP_NAME, &H5CX_def_lcpl_cache.intermediate_group) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve intermediate group creation flag");

    /* Reset the "default LAPL cache" information */
    memset(&H5CX_def_lapl_cache, 0, sizeof(H5CX_lapl_cache_t));

    /* Get the default LAPL cache information */

    /* Get the default link access property list */
    if (NULL == (la_plist = (H5P_genplist_t *)H5I_object(H5P_LINK_ACCESS_DEFAULT)))
        HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL, "not a link access property list");

    /* Get number of soft / UD links to traverse */
    if (H5P_get(la_plist, H5L_ACS_NLINKS_NAME, &H5CX_def_lapl_cache.nlinks) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve number of soft / UD links to traverse");

    /* Reset the "default DCPL cache" information */
    memset(&H5CX_def_dcpl_cache, 0, sizeof(H5CX_dcpl_cache_t));

    /* Get the default DCPL cache information */

    /* Get the default dataset creation property list */
    if (NULL == (dc_plist = (H5P_genplist_t *)H5I_object(H5P_DATASET_CREATE_DEFAULT)))
        HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL, "not a dataset create property list");

    /* Get flag to indicate whether to minimize dataset object header */
    if (H5P_get(dc_plist, H5D_CRT_MIN_DSET_HDR_SIZE_NAME, &H5CX_def_dcpl_cache.do_min_dset_ohdr) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve dataset minimize flag");

    /* Get object header flags */
    if (H5P_get(dc_plist, H5O_CRT_OHDR_FLAGS_NAME, &H5CX_def_dcpl_cache.ohdr_flags) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve object header flags");

    /* Reset the "default DAPL cache" information */
    memset(&H5CX_def_dapl_cache, 0, sizeof(H5CX_dapl_cache_t));

    /* Get the default DAPL cache information */

    /* Get the default dataset access property list */
    if (NULL == (da_plist = (H5P_genplist_t *)H5I_object(H5P_DATASET_ACCESS_DEFAULT)))
        HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL, "not a dataset create property list");

    /* Get the prefix for the external file */
    if (H5P_peek(da_plist, H5D_ACS_EFILE_PREFIX_NAME, &H5CX_def_dapl_cache.extfile_prefix) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve prefix for external file");

    /* Get the prefix for the VDS file */
    if (H5P_peek(da_plist, H5D_ACS_VDS_PREFIX_NAME, &H5CX_def_dapl_cache.vds_prefix) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve prefix for VDS");

    /* Reset the "default FAPL cache" information */
    memset(&H5CX_def_fapl_cache, 0, sizeof(H5CX_fapl_cache_t));

    /* Get the default FAPL cache information */

    /* Get the default file access property list */
    if (NULL == (fa_plist = (H5P_genplist_t *)H5I_object(H5P_FILE_ACCESS_DEFAULT)))
        HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL, "not a dataset create property list");

    /* Get low_bound */
    if (H5P_get(fa_plist, H5F_ACS_LIBVER_LOW_BOUND_NAME, &H5CX_def_fapl_cache.low_bound) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve dataset minimize flag");

    if (H5P_get(fa_plist, H5F_ACS_LIBVER_HIGH_BOUND_NAME, &H5CX_def_fapl_cache.high_bound) < 0)
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve dataset minimize flag");
done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function: H5CX_term_package
 *
 * Purpose:  Terminate this interface.
 *
 * Return:   Success:    Positive if anything was done that might
 *                affect other interfaces; zero otherwise.
 *            Failure:    Negative.
 *
 *-------------------------------------------------------------------------
 */
int
H5CX_term_package(void)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    H5CX_node_t *cnode; /* Context node */

    /* Pop the top context node from the stack */
    /* (Can't check for errors, as rest of library is shut down) */
    cnode = H5CX__pop_common(false);

    /* Free the context node */
    /* (Allocated with malloc() in H5CX_push_special() ) */
    free(cnode);

#ifndef H5_HAVE_THREADSAFE
    H5CX_head_g = NULL;
#endif /* H5_HAVE_THREADSAFE */

    FUNC_LEAVE_NOAPI(0)
} /* end H5CX_term_package() */

#ifdef H5_HAVE_THREADSAFE
/*-------------------------------------------------------------------------
 * Function:	H5CX__get_context
 *
 * Purpose:	Support function for H5CX_get_my_context() to initialize and
 *              acquire per-thread API context stack.
 *
 * Return:	Success: Non-NULL pointer to head pointer of API context stack for thread
 *		Failure: NULL
 *
 *-------------------------------------------------------------------------
 */
static H5CX_node_t **
H5CX__get_context(void)
{
    H5CX_node_t **ctx = NULL;

    FUNC_ENTER_PACKAGE_NOERR

    ctx = (H5CX_node_t **)H5TS_get_thread_local_value(H5TS_apictx_key_g);

    if (!ctx) {
        /* No associated value with current thread - create one */
#ifdef H5_HAVE_WIN_THREADS
        /* Win32 has to use LocalAlloc to match the LocalFree in DllMain */
        ctx = (H5CX_node_t **)LocalAlloc(LPTR, sizeof(H5CX_node_t *));
#else
        /* Use malloc here since this has to match the free in the
         * destructor and we want to avoid the codestack there.
         */
        ctx = (H5CX_node_t **)malloc(sizeof(H5CX_node_t *));
#endif /* H5_HAVE_WIN_THREADS */
        assert(ctx);

        /* Reset the thread-specific info */
        *ctx = NULL;

        /* (It's not necessary to release this in this API, it is
         *      released by the "key destructor" set up in the H5TS
         *      routines.  See calls to pthread_key_create() in H5TS.c -QAK)
         */
        H5TS_set_thread_local_value(H5TS_apictx_key_g, (void *)ctx);
    } /* end if */

    /* Set return value */
    FUNC_LEAVE_NOAPI(ctx)
} /* end H5CX__get_context() */
#endif /* H5_HAVE_THREADSAFE */

/*-------------------------------------------------------------------------
 * Function:    H5CX__push_common
 *
 * Purpose:     Internal routine to push a context for an API call.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static void
H5CX__push_common(H5CX_node_t *cnode)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(cnode);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head);

    /* Set non-zero context info */
    cnode->ctx.dxpl_id = H5P_DATASET_XFER_DEFAULT;
    cnode->ctx.dcpl_id = H5P_DATASET_CREATE_DEFAULT;
    cnode->ctx.dapl_id = H5P_DATASET_ACCESS_DEFAULT;
    cnode->ctx.lcpl_id = H5P_LINK_CREATE_DEFAULT;
    cnode->ctx.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    cnode->ctx.fapl_id = H5P_FILE_ACCESS_DEFAULT;
    cnode->ctx.tag     = H5AC__INVALID_TAG;
    cnode->ctx.ring    = H5AC_RING_USER;

#ifdef H5_HAVE_PARALLEL
    cnode->ctx.btype = MPI_BYTE;
    cnode->ctx.ftype = MPI_BYTE;
#endif

    /* Push context node onto stack */
    cnode->next = *head;
    *head       = cnode;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX__push_common() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_push
 *
 * Purpose:     Pushes a context for an API call.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_push(void)
{
    H5CX_node_t *cnode     = NULL;    /* Context node */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Allocate & clear API context node */
    if (NULL == (cnode = H5FL_CALLOC(H5CX_node_t)))
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTALLOC, FAIL, "unable to allocate new struct");

    /* Set context info */
    H5CX__push_common(cnode);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_push() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_push_special
 *
 * Purpose:     Pushes a context for an API call, without using library routines.
 *
 * Note:	This should only be called in special circumstances, like H5close.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_push_special(void)
{
    H5CX_node_t *cnode = NULL; /* Context node */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Allocate & clear API context node, without using library API routines */
    cnode = (H5CX_node_t *)calloc(1, sizeof(H5CX_node_t));
    assert(cnode);

    /* Set context info */
    H5CX__push_common(cnode);

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_push_special() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_retrieve_state
 *
 * Purpose:     Retrieve the state of an API context, for later resumption.
 *
 * Note:	This routine _only_ tracks the state of API context information
 *		set before the VOL callback is invoked, not values that are
 *		set internal to the library.  It's main purpose is to provide
 *		API context state to VOL connectors.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_retrieve_state(H5CX_state_t **api_state)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(api_state);

    /* Allocate & clear API context state */
    if (NULL == (*api_state = H5FL_CALLOC(H5CX_state_t)))
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTALLOC, FAIL, "unable to allocate new API context state");

    /* Check for non-default DCPL */
    if (H5P_DATASET_CREATE_DEFAULT != (*head)->ctx.dcpl_id) {
        /* Retrieve the DCPL property list */
        H5CX_RETRIEVE_PLIST(dcpl, FAIL)

        /* Copy the DCPL ID */
        if (((*api_state)->dcpl_id = H5P_copy_plist((H5P_genplist_t *)(*head)->ctx.dcpl, false)) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTCOPY, FAIL, "can't copy property list");
    } /* end if */
    else
        (*api_state)->dcpl_id = H5P_DATASET_CREATE_DEFAULT;

    /* Check for non-default DXPL */
    if (H5P_DATASET_XFER_DEFAULT != (*head)->ctx.dxpl_id) {
        /* Retrieve the DXPL property list */
        H5CX_RETRIEVE_PLIST(dxpl, FAIL)

        /* Copy the DXPL ID */
        if (((*api_state)->dxpl_id = H5P_copy_plist((H5P_genplist_t *)(*head)->ctx.dxpl, false)) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTCOPY, FAIL, "can't copy property list");
    } /* end if */
    else
        (*api_state)->dxpl_id = H5P_DATASET_XFER_DEFAULT;

    /* Check for non-default LAPL */
    if (H5P_LINK_ACCESS_DEFAULT != (*head)->ctx.lapl_id) {
        /* Retrieve the LAPL property list */
        H5CX_RETRIEVE_PLIST(lapl, FAIL)

        /* Copy the LAPL ID */
        if (((*api_state)->lapl_id = H5P_copy_plist((H5P_genplist_t *)(*head)->ctx.lapl, false)) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTCOPY, FAIL, "can't copy property list");
    } /* end if */
    else
        (*api_state)->lapl_id = H5P_LINK_ACCESS_DEFAULT;

    /* Check for non-default LCPL */
    if (H5P_LINK_CREATE_DEFAULT != (*head)->ctx.lcpl_id) {
        /* Retrieve the LCPL property list */
        H5CX_RETRIEVE_PLIST(lcpl, FAIL)

        /* Copy the LCPL ID */
        if (((*api_state)->lcpl_id = H5P_copy_plist((H5P_genplist_t *)(*head)->ctx.lcpl, false)) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTCOPY, FAIL, "can't copy property list");
    } /* end if */
    else
        (*api_state)->lcpl_id = H5P_LINK_CREATE_DEFAULT;

    /* Keep a reference to the current VOL wrapping context */
    (*api_state)->vol_wrap_ctx = (*head)->ctx.vol_wrap_ctx;
    if (NULL != (*api_state)->vol_wrap_ctx) {
        assert((*head)->ctx.vol_wrap_ctx_valid);
        if (H5VL_inc_vol_wrapper((*api_state)->vol_wrap_ctx) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTINC, FAIL, "can't increment refcount on VOL wrapping context");
    } /* end if */

    /* Keep a copy of the VOL connector property, if there is one */
    if ((*head)->ctx.vol_connector_prop_valid && (*head)->ctx.vol_connector_prop.connector_id > 0) {
        /* Get the connector property */
        H5MM_memcpy(&(*api_state)->vol_connector_prop, &(*head)->ctx.vol_connector_prop,
                    sizeof(H5VL_connector_prop_t));

        /* Check for actual VOL connector property */
        if ((*api_state)->vol_connector_prop.connector_id) {
            /* Copy connector info, if it exists */
            if ((*api_state)->vol_connector_prop.connector_info) {
                H5VL_class_t *connector;                 /* Pointer to connector */
                void         *new_connector_info = NULL; /* Copy of connector info */

                /* Retrieve the connector for the ID */
                if (NULL ==
                    (connector = (H5VL_class_t *)H5I_object((*api_state)->vol_connector_prop.connector_id)))
                    HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL, "not a VOL connector ID");

                /* Allocate and copy connector info */
                if (H5VL_copy_connector_info(connector, &new_connector_info,
                                             (*api_state)->vol_connector_prop.connector_info) < 0)
                    HGOTO_ERROR(H5E_CONTEXT, H5E_CANTCOPY, FAIL, "connector info copy failed");
                (*api_state)->vol_connector_prop.connector_info = new_connector_info;
            } /* end if */

            /* Increment the refcount on the connector ID */
            if (H5I_inc_ref((*api_state)->vol_connector_prop.connector_id, false) < 0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTINC, FAIL, "incrementing VOL connector ID failed");
        } /* end if */
    }     /* end if */

#ifdef H5_HAVE_PARALLEL
    /* Save parallel I/O settings */
    (*api_state)->coll_metadata_read = (*head)->ctx.coll_metadata_read;
#endif /* H5_HAVE_PARALLEL */

done:
    /* Cleanup on error */
    if (ret_value < 0) {
        if (*api_state) {
            /* Release the (possibly partially allocated) API state struct */
            if (H5CX_free_state(*api_state) < 0)
                HDONE_ERROR(H5E_CONTEXT, H5E_CANTRELEASE, FAIL, "unable to release API state");
            *api_state = NULL;
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_retrieve_state() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_restore_state
 *
 * Purpose:     Restore an API context, from a previously retrieved state.
 *
 * Note:	This routine _only_ resets the state of API context information
 *		set before the VOL callback is invoked, not values that are
 *		set internal to the library.  It's main purpose is to restore
 *		API context state from VOL connectors.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_restore_state(const H5CX_state_t *api_state)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(api_state);

    /* Restore the DCPL info */
    (*head)->ctx.dcpl_id = api_state->dcpl_id;
    (*head)->ctx.dcpl    = NULL;

    /* Restore the DXPL info */
    (*head)->ctx.dxpl_id = api_state->dxpl_id;
    (*head)->ctx.dxpl    = NULL;

    /* Restore the LAPL info */
    (*head)->ctx.lapl_id = api_state->lapl_id;
    (*head)->ctx.lapl    = NULL;

    /* Restore the LCPL info */
    (*head)->ctx.lcpl_id = api_state->lcpl_id;
    (*head)->ctx.lcpl    = NULL;

    /* Restore the VOL wrapper context */
    (*head)->ctx.vol_wrap_ctx = api_state->vol_wrap_ctx;
    if (NULL != (*head)->ctx.vol_wrap_ctx)
        (*head)->ctx.vol_wrap_ctx_valid = true;

    /* Restore the VOL connector info */
    if (api_state->vol_connector_prop.connector_id) {
        H5MM_memcpy(&(*head)->ctx.vol_connector_prop, &api_state->vol_connector_prop,
                    sizeof(H5VL_connector_prop_t));
        (*head)->ctx.vol_connector_prop_valid = true;
    } /* end if */

#ifdef H5_HAVE_PARALLEL
    /* Restore parallel I/O settings */
    (*head)->ctx.coll_metadata_read = api_state->coll_metadata_read;
#endif /* H5_HAVE_PARALLEL */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5CX_restore_state() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_free_state
 *
 * Purpose:     Free a previously retrievedAPI context state
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_free_state(H5CX_state_t *api_state)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(api_state);

    /* Release the DCPL */
    if (0 != api_state->dcpl_id && H5P_DATASET_CREATE_DEFAULT != api_state->dcpl_id)
        if (H5I_dec_ref(api_state->dcpl_id) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTDEC, FAIL, "can't decrement refcount on DCPL");

    /* Release the DXPL */
    if (0 != api_state->dxpl_id && H5P_DATASET_XFER_DEFAULT != api_state->dxpl_id)
        if (H5I_dec_ref(api_state->dxpl_id) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTDEC, FAIL, "can't decrement refcount on DXPL");

    /* Release the LAPL */
    if (0 != api_state->lapl_id && H5P_LINK_ACCESS_DEFAULT != api_state->lapl_id)
        if (H5I_dec_ref(api_state->lapl_id) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTDEC, FAIL, "can't decrement refcount on LAPL");

    /* Release the LCPL */
    if (0 != api_state->lcpl_id && H5P_LINK_CREATE_DEFAULT != api_state->lcpl_id)
        if (H5I_dec_ref(api_state->lcpl_id) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTDEC, FAIL, "can't decrement refcount on LCPL");

    /* Release the VOL wrapper context */
    if (api_state->vol_wrap_ctx)
        if (H5VL_dec_vol_wrapper(api_state->vol_wrap_ctx) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTDEC, FAIL, "can't decrement refcount on VOL wrapping context");

    /* Release the VOL connector property, if it was set */
    if (api_state->vol_connector_prop.connector_id) {
        /* Clean up any VOL connector info */
        if (api_state->vol_connector_prop.connector_info)
            if (H5VL_free_connector_info(api_state->vol_connector_prop.connector_id,
                                         api_state->vol_connector_prop.connector_info) < 0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTRELEASE, FAIL,
                            "unable to release VOL connector info object");
        /* Decrement connector ID */
        if (H5I_dec_ref(api_state->vol_connector_prop.connector_id) < 0)
            HDONE_ERROR(H5E_CONTEXT, H5E_CANTDEC, FAIL, "can't close VOL connector ID");
    } /* end if */

    /* Free the state */
    api_state = H5FL_FREE(H5CX_state_t, api_state);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_free_state() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_is_def_dxpl
 *
 * Purpose:     Checks if the API context is using the library's default DXPL
 *
 * Return:      true / false (can't fail)
 *
 *-------------------------------------------------------------------------
 */
bool
H5CX_is_def_dxpl(void)
{
    H5CX_node_t **head        = NULL;  /* Pointer to head of API context list */
    bool          is_def_dxpl = false; /* Flag to indicate DXPL is default */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set return value */
    is_def_dxpl = ((*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT);

    FUNC_LEAVE_NOAPI(is_def_dxpl)
} /* end H5CX_is_def_dxpl() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_dxpl
 *
 * Purpose:     Sets the DXPL for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_dxpl(hid_t dxpl_id)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context's DXPL to a new value */
    (*head)->ctx.dxpl_id = dxpl_id;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_dxpl() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_dcpl
 *
 * Purpose:     Sets the DCPL for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_dcpl(hid_t dcpl_id)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context's DCPL to a new value */
    (*head)->ctx.dcpl_id = dcpl_id;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_dcpl() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_libver_bounds
 *
 * Purpose:     Sets the low/high bounds according to "f" for the current API call context.
 *              When "f" is NULL, the low/high bounds are set to latest format.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_libver_bounds(H5F_t *f)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context value */
    (*head)->ctx.low_bound  = (f == NULL) ? H5F_LIBVER_LATEST : H5F_LOW_BOUND(f);
    (*head)->ctx.high_bound = (f == NULL) ? H5F_LIBVER_LATEST : H5F_HIGH_BOUND(f);

    /* Mark the values as valid */
    (*head)->ctx.low_bound_valid  = true;
    (*head)->ctx.high_bound_valid = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_set_libver_bounds() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_lcpl
 *
 * Purpose:     Sets the LCPL for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_lcpl(hid_t lcpl_id)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context's LCPL to a new value */
    (*head)->ctx.lcpl_id = lcpl_id;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_lcpl() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_lapl
 *
 * Purpose:     Sets the LAPL for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_lapl(hid_t lapl_id)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context's LAPL to a new value */
    (*head)->ctx.lapl_id = lapl_id;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_lapl() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_apl
 *
 * Purpose:     Validaties an access property list, and sanity checking &
 *              setting up collective operations.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_apl(hid_t *acspl_id, const H5P_libclass_t *libclass,
             hid_t
#ifndef H5_HAVE_PARALLEL
                 H5_ATTR_UNUSED
#endif /* H5_HAVE_PARALLEL */
                     loc_id,
             bool
#ifndef H5_HAVE_PARALLEL
                 H5_ATTR_UNUSED
#endif /* H5_HAVE_PARALLEL */
                     is_collective)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(acspl_id);
    assert(libclass);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set access plist to the default property list of the appropriate class if it's the generic default */
    if (H5P_DEFAULT == *acspl_id)
        *acspl_id = *libclass->def_plist_id;
    else {
        htri_t is_lapl; /* Whether the access property list is (or is derived from) a link access property
                           list */
        htri_t is_dapl; /* Whether the access property list is (or is derived from) a dataset access property
                           list */
        htri_t is_fapl; /* Whether the access property list is (or is derived from) a file access property
                           list */

#ifdef H5CX_DEBUG
        /* Sanity check the access property list class */
        if (true != H5P_isa_class(*acspl_id, *libclass->class_id))
            HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL, "not the required access property list");
#endif /* H5CX_DEBUG*/

        /* Check for link access property and set API context if so */
        if ((is_lapl = H5P_class_isa(*libclass->pclass, *H5P_CLS_LACC->pclass)) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "can't check for link access class");
        else if (is_lapl)
            (*head)->ctx.lapl_id = *acspl_id;

        /* Check for dataset access property and set API context if so */
        if ((is_dapl = H5P_class_isa(*libclass->pclass, *H5P_CLS_DACC->pclass)) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "can't check for dataset access class");
        else if (is_dapl)
            (*head)->ctx.dapl_id = *acspl_id;

        /* Check for file access property and set API context if so */
        if ((is_fapl = H5P_class_isa(*libclass->pclass, *H5P_CLS_FACC->pclass)) < 0)
            HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "can't check for file access class");
        else if (is_fapl)
            (*head)->ctx.fapl_id = *acspl_id;

#ifdef H5_HAVE_PARALLEL
        /* If this routine is not guaranteed to be collective (i.e. it doesn't
         * modify the structural metadata in a file), check if the application
         * specified a collective metadata read for just this operation.
         */
        if (!is_collective) {
            H5P_genplist_t         *plist;        /* Property list pointer */
            H5P_coll_md_read_flag_t md_coll_read; /* Collective metadata read flag */

            /* Get the plist structure for the access property list */
            if (NULL == (plist = (H5P_genplist_t *)H5I_object(*acspl_id)))
                HGOTO_ERROR(H5E_CONTEXT, H5E_BADID, FAIL, "can't find object for ID");

            /* Get the collective metadata read flag */
            if (H5P_peek(plist, H5_COLL_MD_READ_FLAG_NAME, &md_coll_read) < 0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "can't get core collective metadata read flag");

            /* If collective metadata read requested, set collective metadata read flag */
            if (H5P_USER_TRUE == md_coll_read)
                is_collective = true;
        } /* end if */
#endif    /* H5_HAVE_PARALLEL */
    }     /* end else */

#ifdef H5_HAVE_PARALLEL
    /* Check for collective operation */
    if (is_collective) {
        /* Set collective metadata read flag */
        (*head)->ctx.coll_metadata_read = true;

        /* If parallel is enabled and the file driver used is the MPI-IO
         * VFD, issue an MPI barrier for easier debugging if the API function
         * calling this is supposed to be called collectively.
         */
        if (H5_coll_api_sanity_check_g) {
            MPI_Comm mpi_comm; /* File communicator */

            /* Retrieve the MPI communicator from the loc_id or the fapl_id */
            if (H5F_mpi_retrieve_comm(loc_id, *acspl_id, &mpi_comm) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get MPI communicator");

            /* issue the barrier */
            if (mpi_comm != MPI_COMM_NULL)
                MPI_Barrier(mpi_comm);
        } /* end if */
    }     /* end if */
#endif    /* H5_HAVE_PARALLEL */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_set_apl() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_loc
 *
 * Purpose:     Sanity checks and sets up collective operations.
 *
 * Note:        Should be called for all API routines that modify file
 *              metadata but don't pass in an access property list.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_loc(hid_t
#ifndef H5_HAVE_PARALLEL
                 H5_ATTR_UNUSED
#endif /* H5_HAVE_PARALLEL */
                     loc_id)
{
#ifdef H5_HAVE_PARALLEL
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set collective metadata read flag */
    (*head)->ctx.coll_metadata_read = true;

    /* If parallel is enabled and the file driver used is the MPI-IO
     * VFD, issue an MPI barrier for easier debugging if the API function
     * calling this is supposed to be called collectively.
     */
    if (H5_coll_api_sanity_check_g) {
        MPI_Comm mpi_comm; /* File communicator */

        /* Retrieve the MPI communicator from the loc_id or the fapl_id */
        if (H5F_mpi_retrieve_comm(loc_id, H5P_DEFAULT, &mpi_comm) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get MPI communicator");

        /* issue the barrier */
        if (mpi_comm != MPI_COMM_NULL)
            MPI_Barrier(mpi_comm);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
#else  /* H5_HAVE_PARALLEL */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(SUCCEED)
#endif /* H5_HAVE_PARALLEL */
} /* end H5CX_set_loc() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_vol_wrap_ctx
 *
 * Purpose:     Sets the VOL object wrapping context for an operation.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_vol_wrap_ctx(void *vol_wrap_ctx)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context value */
    (*head)->ctx.vol_wrap_ctx = vol_wrap_ctx;

    /* Mark the value as valid */
    (*head)->ctx.vol_wrap_ctx_valid = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_set_vol_wrap_ctx() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_vol_connector_prop
 *
 * Purpose:     Sets the VOL connector ID & info for an operation.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_vol_connector_prop(const H5VL_connector_prop_t *vol_connector_prop)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context value */
    H5MM_memcpy(&(*head)->ctx.vol_connector_prop, vol_connector_prop, sizeof(H5VL_connector_prop_t));

    /* Mark the value as valid */
    (*head)->ctx.vol_connector_prop_valid = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_set_vol_connector_prop() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_dxpl
 *
 * Purpose:     Retrieves the DXPL ID for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5CX_get_dxpl(void)
{
    H5CX_node_t **head    = NULL;            /* Pointer to head of API context list */
    hid_t         dxpl_id = H5I_INVALID_HID; /* DXPL ID for API operation */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set return value */
    dxpl_id = (*head)->ctx.dxpl_id;

    FUNC_LEAVE_NOAPI(dxpl_id)
} /* end H5CX_get_dxpl() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_lapl
 *
 * Purpose:     Retrieves the LAPL ID for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5CX_get_lapl(void)
{
    H5CX_node_t **head    = NULL;            /* Pointer to head of API context list */
    hid_t         lapl_id = H5I_INVALID_HID; /* LAPL ID for API operation */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set return value */
    lapl_id = (*head)->ctx.lapl_id;

    FUNC_LEAVE_NOAPI(lapl_id)
} /* end H5CX_get_lapl() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_vol_wrap_ctx
 *
 * Purpose:     Retrieves the VOL object wrapping context for an operation.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_vol_wrap_ctx(void **vol_wrap_ctx)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(vol_wrap_ctx);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */

    /* No error is expected at this point.  But in case an application calls H5VLwrap_register
     * which doesn't reset the API context and there is no context, returns a relevant error here
     */
    if (!head)
        HGOTO_ERROR(H5E_CONTEXT, H5E_UNINITIALIZED, FAIL, "the API context isn't available");

    if (!(*head))
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "unable to get the current API context");

    /* Check for value that was set */
    if ((*head)->ctx.vol_wrap_ctx_valid)
        /* Get the value */
        *vol_wrap_ctx = (*head)->ctx.vol_wrap_ctx;
    else
        *vol_wrap_ctx = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_vol_wrap_ctx() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_vol_connector_prop
 *
 * Purpose:     Retrieves the VOL connector ID & info for an operation.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_vol_connector_prop(H5VL_connector_prop_t *vol_connector_prop)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    assert(vol_connector_prop);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Check for value that was set */
    if ((*head)->ctx.vol_connector_prop_valid)
        /* Get the value */
        H5MM_memcpy(vol_connector_prop, &(*head)->ctx.vol_connector_prop, sizeof(H5VL_connector_prop_t));
    else
        memset(vol_connector_prop, 0, sizeof(H5VL_connector_prop_t));

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_vol_connector_prop() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_tag
 *
 * Purpose:     Retrieves the object tag for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5CX_get_tag(void)
{
    H5CX_node_t **head = NULL;        /* Pointer to head of API context list */
    haddr_t       tag  = HADDR_UNDEF; /* Current object's tag (ohdr chunk #0 address) */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set return value */
    tag = (*head)->ctx.tag;

    FUNC_LEAVE_NOAPI(tag)
} /* end H5CX_get_tag() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_ring
 *
 * Purpose:     Retrieves the metadata cache ring for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5AC_ring_t
H5CX_get_ring(void)
{
    H5CX_node_t **head = NULL;          /* Pointer to head of API context list */
    H5AC_ring_t   ring = H5AC_RING_INV; /* Current metadata cache ring for entries */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set return value */
    ring = (*head)->ctx.ring;

    FUNC_LEAVE_NOAPI(ring)
} /* end H5CX_get_ring() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_coll_metadata_read
 *
 * Purpose:     Retrieves the "do collective metadata reads" flag for the current API call context.
 *
 * Return:      true / false on success / <can't fail>
 *
 *-------------------------------------------------------------------------
 */
bool
H5CX_get_coll_metadata_read(void)
{
    H5CX_node_t **head         = NULL; /* Pointer to head of API context list */
    bool          coll_md_read = false;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set return value */
    coll_md_read = (*head)->ctx.coll_metadata_read;

    FUNC_LEAVE_NOAPI(coll_md_read)
} /* end H5CX_get_coll_metadata_read() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_mpi_coll_datatypes
 *
 * Purpose:     Retrieves the MPI datatypes for collective I/O for the current API call context.
 *
 * Note:	This is only a shallow copy, the datatypes are not duplicated.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_mpi_coll_datatypes(MPI_Datatype *btype, MPI_Datatype *ftype)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    assert(btype);
    assert(ftype);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context values */
    *btype = (*head)->ctx.btype;
    *ftype = (*head)->ctx.ftype;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_mpi_coll_datatypes() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_mpi_file_flushing
 *
 * Purpose:     Retrieves the "flushing an MPI-opened file" flag for the current API call context.
 *
 * Return:      true / false on success / <can't fail>
 *
 *-------------------------------------------------------------------------
 */
bool
H5CX_get_mpi_file_flushing(void)
{
    H5CX_node_t **head     = NULL; /* Pointer to head of API context list */
    bool          flushing = false;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set return value */
    flushing = (*head)->ctx.mpi_file_flushing;

    FUNC_LEAVE_NOAPI(flushing)
} /* end H5CX_get_mpi_file_flushing() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_mpio_rank0_bcast
 *
 * Purpose:     Retrieves if the dataset meets read-with-rank0-and-bcast requirements for the current API call
 *context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
bool
H5CX_get_mpio_rank0_bcast(void)
{
    H5CX_node_t **head           = NULL; /* Pointer to head of API context list */
    bool          do_rank0_bcast = false;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set return value */
    do_rank0_bcast = (*head)->ctx.rank0_bcast;

    FUNC_LEAVE_NOAPI(do_rank0_bcast)
} /* end H5CX_get_mpio_rank0_bcast() */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_btree_split_ratios
 *
 * Purpose:     Retrieves the B-tree split ratios for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_btree_split_ratios(double split_ratio[3])
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(split_ratio);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_BTREE_SPLIT_RATIO_NAME,
                             btree_split_ratio)

    /* Get the B-tree split ratio values */
    H5MM_memcpy(split_ratio, &(*head)->ctx.btree_split_ratio, sizeof((*head)->ctx.btree_split_ratio));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_btree_split_ratios() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_max_temp_buf
 *
 * Purpose:     Retrieves the maximum temporary buffer size for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_max_temp_buf(size_t *max_temp_buf)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(max_temp_buf);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_MAX_TEMP_BUF_NAME, max_temp_buf)

    /* Get the value */
    *max_temp_buf = (*head)->ctx.max_temp_buf;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_max_temp_buf() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_tconv_buf
 *
 * Purpose:     Retrieves the temporary buffer pointer for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_tconv_buf(void **tconv_buf)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(tconv_buf);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_TCONV_BUF_NAME, tconv_buf)

    /* Get the value */
    *tconv_buf = (*head)->ctx.tconv_buf;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_tconv_buf() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_bkgr_buf
 *
 * Purpose:     Retrieves the background buffer pointer for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_bkgr_buf(void **bkgr_buf)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(bkgr_buf);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_BKGR_BUF_NAME, bkgr_buf)

    /* Get the value */
    *bkgr_buf = (*head)->ctx.bkgr_buf;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_bkgr_buf() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_bkgr_buf_type
 *
 * Purpose:     Retrieves the background buffer type for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_bkgr_buf_type(H5T_bkg_t *bkgr_buf_type)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(bkgr_buf_type);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_BKGR_BUF_TYPE_NAME, bkgr_buf_type)

    /* Get the value */
    *bkgr_buf_type = (*head)->ctx.bkgr_buf_type;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_bkgr_buf_type() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_vec_size
 *
 * Purpose:     Retrieves the hyperslab vector size for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_vec_size(size_t *vec_size)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(vec_size);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_HYPER_VECTOR_SIZE_NAME, vec_size)

    /* Get the value */
    *vec_size = (*head)->ctx.vec_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_vec_size() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_io_xfer_mode
 *
 * Purpose:     Retrieves the parallel transfer mode for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_io_xfer_mode(H5FD_mpio_xfer_t *io_xfer_mode)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(io_xfer_mode);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_IO_XFER_MODE_NAME, io_xfer_mode)

    /* Get the value */
    *io_xfer_mode = (*head)->ctx.io_xfer_mode;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_io_xfer_mode() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_mpio_coll_opt
 *
 * Purpose:     Retrieves the collective / independent parallel I/O option for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_mpio_coll_opt(H5FD_mpio_collective_opt_t *mpio_coll_opt)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(mpio_coll_opt);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_MPIO_COLLECTIVE_OPT_NAME, mpio_coll_opt)

    /* Get the value */
    *mpio_coll_opt = (*head)->ctx.mpio_coll_opt;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_mpio_coll_opt() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_mpio_local_no_coll_cause
 *
 * Purpose:     Retrieves the local cause for breaking collective I/O for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_mpio_local_no_coll_cause(uint32_t *mpio_local_no_coll_cause)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(mpio_local_no_coll_cause);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID_SET(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_MPIO_LOCAL_NO_COLLECTIVE_CAUSE_NAME,
                                 mpio_local_no_coll_cause)

    /* Get the value */
    *mpio_local_no_coll_cause = (*head)->ctx.mpio_local_no_coll_cause;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_mpio_local_no_coll_cause() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_mpio_global_no_coll_cause
 *
 * Purpose:     Retrieves the global cause for breaking collective I/O for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_mpio_global_no_coll_cause(uint32_t *mpio_global_no_coll_cause)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(mpio_global_no_coll_cause);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID_SET(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_MPIO_GLOBAL_NO_COLLECTIVE_CAUSE_NAME,
                                 mpio_global_no_coll_cause)

    /* Get the value */
    *mpio_global_no_coll_cause = (*head)->ctx.mpio_global_no_coll_cause;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_mpio_global_no_coll_cause() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_mpio_chunk_opt_mode
 *
 * Purpose:     Retrieves the collective chunk optimization mode for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_mpio_chunk_opt_mode(H5FD_mpio_chunk_opt_t *mpio_chunk_opt_mode)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(mpio_chunk_opt_mode);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_MPIO_CHUNK_OPT_HARD_NAME,
                             mpio_chunk_opt_mode)

    /* Get the value */
    *mpio_chunk_opt_mode = (*head)->ctx.mpio_chunk_opt_mode;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_mpio_chunk_opt_mode() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_mpio_chunk_opt_num
 *
 * Purpose:     Retrieves the collective chunk optimization threshold for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_mpio_chunk_opt_num(unsigned *mpio_chunk_opt_num)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(mpio_chunk_opt_num);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_MPIO_CHUNK_OPT_NUM_NAME,
                             mpio_chunk_opt_num)

    /* Get the value */
    *mpio_chunk_opt_num = (*head)->ctx.mpio_chunk_opt_num;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_mpio_chunk_opt_num() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_mpio_chunk_opt_ratio
 *
 * Purpose:     Retrieves the collective chunk optimization ratio for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_mpio_chunk_opt_ratio(unsigned *mpio_chunk_opt_ratio)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(mpio_chunk_opt_ratio);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_MPIO_CHUNK_OPT_RATIO_NAME,
                             mpio_chunk_opt_ratio)

    /* Get the value */
    *mpio_chunk_opt_ratio = (*head)->ctx.mpio_chunk_opt_ratio;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_mpio_chunk_opt_ratio() */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_err_detect
 *
 * Purpose:     Retrieves the error detection info for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_err_detect(H5Z_EDC_t *err_detect)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(err_detect);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_EDC_NAME, err_detect)

    /* Get the value */
    *err_detect = (*head)->ctx.err_detect;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_err_detect() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_filter_cb
 *
 * Purpose:     Retrieves the I/O filter callback function for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_filter_cb(H5Z_cb_t *filter_cb)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(filter_cb);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_FILTER_CB_NAME, filter_cb)

    /* Get the value */
    *filter_cb = (*head)->ctx.filter_cb;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_filter_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_data_transform
 *
 * Purpose:     Retrieves the I/O filter callback function for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_data_transform(H5Z_data_xform_t **data_transform)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(data_transform);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    /* Check if the value has been retrieved already */
    if (!(*head)->ctx.data_transform_valid) {
        /* Check for default DXPL */
        if ((*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT)
            (*head)->ctx.data_transform = H5CX_def_dxpl_cache.data_transform;
        else {
            /* Check if the property list is already available */
            if (NULL == (*head)->ctx.dxpl)
                /* Get the dataset transfer property list pointer */
                if (NULL == ((*head)->ctx.dxpl = (H5P_genplist_t *)H5I_object((*head)->ctx.dxpl_id)))
                    HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL,
                                "can't get default dataset transfer property list");

            /* Get data transform info value */
            /* (Note: 'peek', not 'get' - if this turns out to be a problem, we may need
             *          to copy it and free this in the H5CX pop routine. -QAK)
             */
            if (H5P_peek((*head)->ctx.dxpl, H5D_XFER_XFORM_NAME, &(*head)->ctx.data_transform) < 0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve data transform info");
        } /* end else */

        /* Mark the value as valid */
        (*head)->ctx.data_transform_valid = true;
    } /* end if */

    /* Get the value */
    *data_transform = (*head)->ctx.data_transform;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_data_transform() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_vlen_alloc_info
 *
 * Purpose:     Retrieves the VL datatype alloc info for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_vlen_alloc_info(H5T_vlen_alloc_info_t *vl_alloc_info)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(vl_alloc_info);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    /* Check if the value has been retrieved already */
    if (!(*head)->ctx.vl_alloc_info_valid) {
        /* Check for default DXPL */
        if ((*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT)
            (*head)->ctx.vl_alloc_info = H5CX_def_dxpl_cache.vl_alloc_info;
        else {
            /* Check if the property list is already available */
            if (NULL == (*head)->ctx.dxpl)
                /* Get the dataset transfer property list pointer */
                if (NULL == ((*head)->ctx.dxpl = (H5P_genplist_t *)H5I_object((*head)->ctx.dxpl_id)))
                    HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL,
                                "can't get default dataset transfer property list");

            /* Get VL datatype alloc info values */
            if (H5P_get((*head)->ctx.dxpl, H5D_XFER_VLEN_ALLOC_NAME, &(*head)->ctx.vl_alloc_info.alloc_func) <
                0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve VL datatype alloc info");
            if (H5P_get((*head)->ctx.dxpl, H5D_XFER_VLEN_ALLOC_INFO_NAME,
                        &(*head)->ctx.vl_alloc_info.alloc_info) < 0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve VL datatype alloc info");
            if (H5P_get((*head)->ctx.dxpl, H5D_XFER_VLEN_FREE_NAME, &(*head)->ctx.vl_alloc_info.free_func) <
                0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve VL datatype alloc info");
            if (H5P_get((*head)->ctx.dxpl, H5D_XFER_VLEN_FREE_INFO_NAME,
                        &(*head)->ctx.vl_alloc_info.free_info) < 0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve VL datatype alloc info");
        } /* end else */

        /* Mark the value as valid */
        (*head)->ctx.vl_alloc_info_valid = true;
    } /* end if */

    /* Get the value */
    *vl_alloc_info = (*head)->ctx.vl_alloc_info;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_vlen_alloc_info() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_dt_conv_cb
 *
 * Purpose:     Retrieves the datatype conversion exception callback for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_dt_conv_cb(H5T_conv_cb_t *dt_conv_cb)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(dt_conv_cb);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_CONV_CB_NAME, dt_conv_cb)

    /* Get the value */
    *dt_conv_cb = (*head)->ctx.dt_conv_cb;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_dt_conv_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_selection_io_mode
 *
 * Purpose:     Retrieves the selection I/O mode for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_selection_io_mode(H5D_selection_io_mode_t *selection_io_mode)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(selection_io_mode);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_SELECTION_IO_MODE_NAME,
                             selection_io_mode)

    /* Get the value */
    *selection_io_mode = (*head)->ctx.selection_io_mode;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_selection_io_mode() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_no_selection_io_cause
 *
 * Purpose:     Retrieves the cause for not performing selection I/O
 *              for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_no_selection_io_cause(uint32_t *no_selection_io_cause)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(no_selection_io_cause);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID_SET(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_NO_SELECTION_IO_CAUSE_NAME,
                                 no_selection_io_cause)

    /* Get the value */
    *no_selection_io_cause = (*head)->ctx.no_selection_io_cause;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_no_selection_io_cause() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_actual_selection_io_mode
 *
 * Purpose:     Retrieves the actual I/O mode (scalar, vector, and/or selection) for the current API call
 *context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_actual_selection_io_mode(uint32_t *actual_selection_io_mode)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(actual_selection_io_mode);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    /* This property is a special case - we want to wipe out any previous setting.  Copy the default setting
     * if it has not been set yet. */
    if ((*head)->ctx.dxpl_id != H5P_DATASET_XFER_DEFAULT && !(*head)->ctx.actual_selection_io_mode_set &&
        !(*head)->ctx.actual_selection_io_mode_valid) {
        (*head)->ctx.actual_selection_io_mode     = H5CX_def_dxpl_cache.actual_selection_io_mode;
        (*head)->ctx.actual_selection_io_mode_set = true;
    }
    H5CX_RETRIEVE_PROP_VALID_SET(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_ACTUAL_SELECTION_IO_MODE_NAME,
                                 actual_selection_io_mode)

    /* Get the value */
    *actual_selection_io_mode = (*head)->ctx.actual_selection_io_mode;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_actual_selection_io_mode() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_modify_write_buf
 *
 * Purpose:     Retrieves the modify write buffer property for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_modify_write_buf(bool *modify_write_buf)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(modify_write_buf);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(dxpl, H5P_DATASET_XFER_DEFAULT, H5D_XFER_MODIFY_WRITE_BUF_NAME, modify_write_buf)

    /* Get the value */
    *modify_write_buf = (*head)->ctx.modify_write_buf;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_selection_io_mode() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_encoding
 *
 * Purpose:     Retrieves the character encoding for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_encoding(H5T_cset_t *encoding)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(encoding);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.lcpl_id);

    H5CX_RETRIEVE_PROP_VALID(lcpl, H5P_LINK_CREATE_DEFAULT, H5P_STRCRT_CHAR_ENCODING_NAME, encoding)

    /* Get the value */
    *encoding = (*head)->ctx.encoding;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_encoding() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_intermediate_group
 *
 * Purpose:     Retrieves the create intermediate group flag for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_intermediate_group(unsigned *crt_intermed_group)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(crt_intermed_group);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.lcpl_id);

    H5CX_RETRIEVE_PROP_VALID(lcpl, H5P_LINK_CREATE_DEFAULT, H5L_CRT_INTERMEDIATE_GROUP_NAME,
                             intermediate_group)

    /* Get the value */
    *crt_intermed_group = (*head)->ctx.intermediate_group;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_create_intermediate_group() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_nlinks
 *
 * Purpose:     Retrieves the # of soft / UD links to traverse for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_nlinks(size_t *nlinks)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(nlinks);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dxpl_id);

    H5CX_RETRIEVE_PROP_VALID(lapl, H5P_LINK_ACCESS_DEFAULT, H5L_ACS_NLINKS_NAME, nlinks)

    /* Get the value */
    *nlinks = (*head)->ctx.nlinks;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_nlinks() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_libver_bounds
 *
 * Purpose:     Retrieves the low/high bounds for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_libver_bounds(H5F_libver_t *low_bound, H5F_libver_t *high_bound)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(low_bound);
    assert(high_bound);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.fapl_id);

    H5CX_RETRIEVE_PROP_VALID(fapl, H5P_FILE_ACCESS_DEFAULT, H5F_ACS_LIBVER_LOW_BOUND_NAME, low_bound)
    H5CX_RETRIEVE_PROP_VALID(fapl, H5P_FILE_ACCESS_DEFAULT, H5F_ACS_LIBVER_HIGH_BOUND_NAME, high_bound)

    /* Get the values */
    *low_bound  = (*head)->ctx.low_bound;
    *high_bound = (*head)->ctx.high_bound;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_libver_bounds() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_dset_min_ohdr_flag
 *
 * Purpose:     Retrieves the flag that indicates whether the dataset object
 *		header should be minimized
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_dset_min_ohdr_flag(bool *dset_min_ohdr_flag)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(dset_min_ohdr_flag);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dcpl_id);

    H5CX_RETRIEVE_PROP_VALID(dcpl, H5P_DATASET_CREATE_DEFAULT, H5D_CRT_MIN_DSET_HDR_SIZE_NAME,
                             do_min_dset_ohdr)

    /* Get the value */
    *dset_min_ohdr_flag = (*head)->ctx.do_min_dset_ohdr;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_dset_min_ohdr_flag() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_ext_file_prefix
 *
 * Purpose:     Retrieves the prefix for external file
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_ext_file_prefix(const char **extfile_prefix)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(extfile_prefix);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dapl_id);

    /* Check if the value has been retrieved already */
    if (!(*head)->ctx.extfile_prefix_valid) {
        /* Check for default DAPL */
        if ((*head)->ctx.dapl_id == H5P_DATASET_ACCESS_DEFAULT)
            (*head)->ctx.extfile_prefix = H5CX_def_dapl_cache.extfile_prefix;
        else {
            /* Check if the property list is already available */
            if (NULL == (*head)->ctx.dapl)
                /* Get the dataset access property list pointer */
                if (NULL == ((*head)->ctx.dapl = (H5P_genplist_t *)H5I_object((*head)->ctx.dapl_id)))
                    HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL,
                                "can't get default dataset access property list");

            /* Get the prefix for the external file */
            /* (Note: 'peek', not 'get' - if this turns out to be a problem, we may need
             *          to copy it and free this in the H5CX pop routine. -QAK)
             */
            if (H5P_peek((*head)->ctx.dapl, H5D_ACS_EFILE_PREFIX_NAME, &(*head)->ctx.extfile_prefix) < 0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve external file prefix");
        } /* end else */

        /* Mark the value as valid */
        (*head)->ctx.extfile_prefix_valid = true;
    } /* end if */

    /* Get the value */
    *extfile_prefix = (*head)->ctx.extfile_prefix;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_ext_file_prefix() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_vds_prefix
 *
 * Purpose:     Retrieves the prefix for VDS
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_vds_prefix(const char **vds_prefix)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(vds_prefix);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dapl_id);

    /* Check if the value has been retrieved already */
    if (!(*head)->ctx.vds_prefix_valid) {
        /* Check for default DAPL */
        if ((*head)->ctx.dapl_id == H5P_DATASET_ACCESS_DEFAULT)
            (*head)->ctx.vds_prefix = H5CX_def_dapl_cache.vds_prefix;
        else {
            /* Check if the property list is already available */
            if (NULL == (*head)->ctx.dapl)
                /* Get the dataset access property list pointer */
                if (NULL == ((*head)->ctx.dapl = (H5P_genplist_t *)H5I_object((*head)->ctx.dapl_id)))
                    HGOTO_ERROR(H5E_CONTEXT, H5E_BADTYPE, FAIL,
                                "can't get default dataset access property list");

            /* Get the prefix for the VDS */
            /* (Note: 'peek', not 'get' - if this turns out to be a problem, we may need
             *          to copy it and free this in the H5CX pop routine. -QAK)
             */
            if (H5P_peek((*head)->ctx.dapl, H5D_ACS_VDS_PREFIX_NAME, &(*head)->ctx.vds_prefix) < 0)
                HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "Can't retrieve VDS prefix");
        } /* end else */

        /* Mark the value as valid */
        (*head)->ctx.vds_prefix_valid = true;
    } /* end if */

    /* Get the value */
    *vds_prefix = (*head)->ctx.vds_prefix;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_get_vds_prefix() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_tag
 *
 * Purpose:     Sets the object tag for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_tag(haddr_t tag)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    (*head)->ctx.tag = tag;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_tag() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_ring
 *
 * Purpose:     Sets the metadata cache ring for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_ring(H5AC_ring_t ring)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    (*head)->ctx.ring = ring;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_ring() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_coll_metadata_read
 *
 * Purpose:     Sets the "do collective metadata reads" flag for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_coll_metadata_read(bool cmdr)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    (*head)->ctx.coll_metadata_read = cmdr;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_coll_metadata_read() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_mpi_coll_datatypes
 *
 * Purpose:     Sets the MPI datatypes for collective I/O for the current API call context.
 *
 * Note:	This is only a shallow copy, the datatypes are not duplicated.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_mpi_coll_datatypes(MPI_Datatype btype, MPI_Datatype ftype)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context values */
    (*head)->ctx.btype = btype;
    (*head)->ctx.ftype = ftype;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_set_mpi_coll_datatypes() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_io_xfer_mode
 *
 * Purpose:     Sets the parallel transfer mode for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_io_xfer_mode(H5FD_mpio_xfer_t io_xfer_mode)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context value */
    (*head)->ctx.io_xfer_mode = io_xfer_mode;

    /* Mark the value as valid */
    (*head)->ctx.io_xfer_mode_valid = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_set_io_xfer_mode() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_mpio_coll_opt
 *
 * Purpose:     Sets the parallel transfer mode for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_mpio_coll_opt(H5FD_mpio_collective_opt_t mpio_coll_opt)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context value */
    (*head)->ctx.mpio_coll_opt = mpio_coll_opt;

    /* Mark the value as valid */
    (*head)->ctx.mpio_coll_opt_valid = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_set_mpio_coll_opt() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_mpi_file_flushing
 *
 * Purpose:     Sets the "flushing an MPI-opened file" flag for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_mpi_file_flushing(bool flushing)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    (*head)->ctx.mpi_file_flushing = flushing;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_mpi_file_flushing() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_mpio_rank0_bcast
 *
 * Purpose:     Sets the "dataset meets read-with-rank0-and-bcast requirements" flag for the current API call
 *context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_mpio_rank0_bcast(bool rank0_bcast)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    (*head)->ctx.rank0_bcast = rank0_bcast;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_mpio_rank0_bcast() */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_vlen_alloc_info
 *
 * Purpose:     Sets the VL datatype alloc info for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_vlen_alloc_info(H5MM_allocate_t alloc_func, void *alloc_info, H5MM_free_t free_func, void *free_info)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context value */
    (*head)->ctx.vl_alloc_info.alloc_func = alloc_func;
    (*head)->ctx.vl_alloc_info.alloc_info = alloc_info;
    (*head)->ctx.vl_alloc_info.free_func  = free_func;
    (*head)->ctx.vl_alloc_info.free_info  = free_info;

    /* Mark the value as valid */
    (*head)->ctx.vl_alloc_info_valid = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_set_vlen_alloc_info() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_nlinks
 *
 * Purpose:     Sets the # of soft / UD links to traverse for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_set_nlinks(size_t nlinks)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Set the API context value */
    (*head)->ctx.nlinks = nlinks;

    /* Mark the value as valid */
    (*head)->ctx.nlinks_valid = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_set_nlinks() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_mpio_actual_chunk_opt
 *
 * Purpose:     Sets the actual chunk optimization used for parallel I/O for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_mpio_actual_chunk_opt(H5D_mpio_actual_chunk_opt_mode_t mpio_actual_chunk_opt)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(!((*head)->ctx.dxpl_id == H5P_DEFAULT || (*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT));

    /* Cache the value for later, marking it to set in DXPL when context popped */
    (*head)->ctx.mpio_actual_chunk_opt     = mpio_actual_chunk_opt;
    (*head)->ctx.mpio_actual_chunk_opt_set = true;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_mpio_actual_chunk_opt() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_mpio_actual_io_mode
 *
 * Purpose:     Sets the actual I/O mode used for parallel I/O for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_mpio_actual_io_mode(H5D_mpio_actual_io_mode_t mpio_actual_io_mode)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(!((*head)->ctx.dxpl_id == H5P_DEFAULT || (*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT));

    /* Cache the value for later, marking it to set in DXPL when context popped */
    (*head)->ctx.mpio_actual_io_mode     = mpio_actual_io_mode;
    (*head)->ctx.mpio_actual_io_mode_set = true;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_mpio_actual_chunk_opt() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_mpio_local_no_coll_cause
 *
 * Purpose:     Sets the local reason for breaking collective I/O for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_mpio_local_no_coll_cause(uint32_t mpio_local_no_coll_cause)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert((*head)->ctx.dxpl_id != H5P_DEFAULT);

    /* If we're using the default DXPL, don't modify it */
    if ((*head)->ctx.dxpl_id != H5P_DATASET_XFER_DEFAULT) {
        /* Cache the value for later, marking it to set in DXPL when context popped */
        (*head)->ctx.mpio_local_no_coll_cause     = mpio_local_no_coll_cause;
        (*head)->ctx.mpio_local_no_coll_cause_set = true;
    } /* end if */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_mpio_local_no_coll_cause() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_mpio_global_no_coll_cause
 *
 * Purpose:     Sets the global reason for breaking collective I/O for the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_mpio_global_no_coll_cause(uint32_t mpio_global_no_coll_cause)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert((*head)->ctx.dxpl_id != H5P_DEFAULT);

    /* If we're using the default DXPL, don't modify it */
    if ((*head)->ctx.dxpl_id != H5P_DATASET_XFER_DEFAULT) {
        /* Cache the value for later, marking it to set in DXPL when context popped */
        (*head)->ctx.mpio_global_no_coll_cause     = mpio_global_no_coll_cause;
        (*head)->ctx.mpio_global_no_coll_cause_set = true;
    } /* end if */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_mpio_global_no_coll_cause() */

#ifdef H5_HAVE_INSTRUMENTED_LIBRARY

/*-------------------------------------------------------------------------
 * Function:    H5CX_test_set_mpio_coll_chunk_link_hard
 *
 * Purpose:     Sets the instrumented "collective chunk link hard" value for the current API call context.
 *
 * Note:        Only sets value if property set in DXPL
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_test_set_mpio_coll_chunk_link_hard(int mpio_coll_chunk_link_hard)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(!((*head)->ctx.dxpl_id == H5P_DEFAULT || (*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT));

    H5CX_TEST_SET_PROP(H5D_XFER_COLL_CHUNK_LINK_HARD_NAME, mpio_coll_chunk_link_hard)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_test_set_mpio_coll_chunk_link_hard() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_test_set_mpio_coll_chunk_multi_hard
 *
 * Purpose:     Sets the instrumented "collective chunk multi hard" value for the current API call context.
 *
 * Note:        Only sets value if property set in DXPL
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_test_set_mpio_coll_chunk_multi_hard(int mpio_coll_chunk_multi_hard)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(!((*head)->ctx.dxpl_id == H5P_DEFAULT || (*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT));

    H5CX_TEST_SET_PROP(H5D_XFER_COLL_CHUNK_MULTI_HARD_NAME, mpio_coll_chunk_multi_hard)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_test_set_mpio_coll_chunk_multi_hard() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_test_set_mpio_coll_chunk_link_num_true
 *
 * Purpose:     Sets the instrumented "collective chunk link num tru" value for the current API call context.
 *
 * Note:        Only sets value if property set in DXPL
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_test_set_mpio_coll_chunk_link_num_true(int mpio_coll_chunk_link_num_true)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(!((*head)->ctx.dxpl_id == H5P_DEFAULT || (*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT));

    H5CX_TEST_SET_PROP(H5D_XFER_COLL_CHUNK_LINK_NUM_TRUE_NAME, mpio_coll_chunk_link_num_true)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_test_set_mpio_coll_chunk_link_num_true() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_test_set_mpio_coll_chunk_link_num_false
 *
 * Purpose:     Sets the instrumented "collective chunk link num false" value for the current API call
 *context.
 *
 * Note:        Only sets value if property set in DXPL
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_test_set_mpio_coll_chunk_link_num_false(int mpio_coll_chunk_link_num_false)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(!((*head)->ctx.dxpl_id == H5P_DEFAULT || (*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT));

    H5CX_TEST_SET_PROP(H5D_XFER_COLL_CHUNK_LINK_NUM_FALSE_NAME, mpio_coll_chunk_link_num_false)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_test_set_mpio_coll_chunk_link_num_false() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_test_set_mpio_coll_chunk_multi_ratio_coll
 *
 * Purpose:     Sets the instrumented "collective chunk multi ratio coll" value for the current API call
 *context.
 *
 * Note:        Only sets value if property set in DXPL
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_test_set_mpio_coll_chunk_multi_ratio_coll(int mpio_coll_chunk_multi_ratio_coll)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(!((*head)->ctx.dxpl_id == H5P_DEFAULT || (*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT));

    H5CX_TEST_SET_PROP(H5D_XFER_COLL_CHUNK_MULTI_RATIO_COLL_NAME, mpio_coll_chunk_multi_ratio_coll)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_test_set_mpio_coll_chunk_multi_ratio_coll() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_test_set_mpio_coll_chunk_multi_ratio_ind
 *
 * Purpose:     Sets the instrumented "collective chunk multi ratio ind" value for the current API call
 *context.
 *
 * Note:        Only sets value if property set in DXPL
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_test_set_mpio_coll_chunk_multi_ratio_ind(int mpio_coll_chunk_multi_ratio_ind)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(!((*head)->ctx.dxpl_id == H5P_DEFAULT || (*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT));

    H5CX_TEST_SET_PROP(H5D_XFER_COLL_CHUNK_MULTI_RATIO_IND_NAME, mpio_coll_chunk_multi_ratio_ind)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_test_set_mpio_coll_chunk_multi_ratio_ind() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_test_set_mpio_coll_rank0_bcast
 *
 * Purpose:     Sets the instrumented "read-with-rank0-bcast" flag for the current API call context.
 *
 * Note:        Only sets value if property set in DXPL
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_test_set_mpio_coll_rank0_bcast(bool mpio_coll_rank0_bcast)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(!((*head)->ctx.dxpl_id == H5P_DEFAULT || (*head)->ctx.dxpl_id == H5P_DATASET_XFER_DEFAULT));

    H5CX_TEST_SET_PROP(H5D_XFER_COLL_RANK0_BCAST_NAME, mpio_coll_rank0_bcast)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_test_set_mpio_coll_rank0_bcast() */
#endif /* H5_HAVE_INSTRUMENTED_LIBRARY */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_no_selection_io_cause
 *
 * Purpose:     Sets the reason for not performing selection I/O for
 *              the current API call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_no_selection_io_cause(uint32_t no_selection_io_cause)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert((*head)->ctx.dxpl_id != H5P_DEFAULT);

    /* If we're using the default DXPL, don't modify it */
    if ((*head)->ctx.dxpl_id != H5P_DATASET_XFER_DEFAULT) {
        /* Cache the value for later, marking it to set in DXPL when context popped */
        (*head)->ctx.no_selection_io_cause     = no_selection_io_cause;
        (*head)->ctx.no_selection_io_cause_set = true;
    } /* end if */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_no_selection_io_cause() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_set_actual_selection_io_mode
 *
 * Purpose:     Sets the actual selection I/O mode for the current API
 *              call context.
 *
 * Return:      <none>
 *
 *-------------------------------------------------------------------------
 */
void
H5CX_set_actual_selection_io_mode(uint32_t actual_selection_io_mode)
{
    H5CX_node_t **head = NULL; /* Pointer to head of API context list */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert((*head)->ctx.dxpl_id != H5P_DEFAULT);

    /* If we're using the default DXPL, don't modify it */
    if ((*head)->ctx.dxpl_id != H5P_DATASET_XFER_DEFAULT) {
        /* Cache the value for later, marking it to set in DXPL when context popped */
        (*head)->ctx.actual_selection_io_mode     = actual_selection_io_mode;
        (*head)->ctx.actual_selection_io_mode_set = true;
    }

    FUNC_LEAVE_NOAPI_VOID
} /* end H5CX_set_actual_selection_io_mode() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_get_ohdr_flags
 *
 * Purpose:     Retrieves the object header flags for the current API call context.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_get_ohdr_flags(uint8_t *ohdr_flags)
{
    H5CX_node_t **head      = NULL;    /* Pointer to head of API context list */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(ohdr_flags);
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);
    assert(H5P_DEFAULT != (*head)->ctx.dcpl_id);

    H5CX_RETRIEVE_PROP_VALID(dcpl, H5P_DATASET_CREATE_DEFAULT, H5O_CRT_OHDR_FLAGS_NAME, ohdr_flags)

    /* Get the value */
    *ohdr_flags = (*head)->ctx.ohdr_flags;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* End H5CX_get_ohdr_flags() */

/*-------------------------------------------------------------------------
 * Function:    H5CX__pop_common
 *
 * Purpose:     Common code for popping the context for an API call.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static H5CX_node_t *
H5CX__pop_common(bool update_dxpl_props)
{
    H5CX_node_t **head      = NULL; /* Pointer to head of API context list */
    H5CX_node_t  *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    head = H5CX_get_my_context(); /* Get the pointer to the head of the API context, for this thread */
    assert(head && *head);

    /* Check for cached DXPL properties to return to application */
    if (update_dxpl_props) {
        /* actual_selection_io_mode is a special case - we always want to set it in the property list even if
         * it was never set by the library, in that case it indicates no I/O was performed and we don't want
         * to leave the (possibly incorrect) old value in the property list, so set from the default property
         * list */
        if ((*head)->ctx.dxpl_id != H5P_DATASET_XFER_DEFAULT && !(*head)->ctx.actual_selection_io_mode_set) {
            (*head)->ctx.actual_selection_io_mode     = H5CX_def_dxpl_cache.actual_selection_io_mode;
            (*head)->ctx.actual_selection_io_mode_set = true;
        }

        H5CX_SET_PROP(H5D_XFER_NO_SELECTION_IO_CAUSE_NAME, no_selection_io_cause)
        H5CX_SET_PROP(H5D_XFER_ACTUAL_SELECTION_IO_MODE_NAME, actual_selection_io_mode)
#ifdef H5_HAVE_PARALLEL
        H5CX_SET_PROP(H5D_MPIO_ACTUAL_CHUNK_OPT_MODE_NAME, mpio_actual_chunk_opt)
        H5CX_SET_PROP(H5D_MPIO_ACTUAL_IO_MODE_NAME, mpio_actual_io_mode)
        H5CX_SET_PROP(H5D_MPIO_LOCAL_NO_COLLECTIVE_CAUSE_NAME, mpio_local_no_coll_cause)
        H5CX_SET_PROP(H5D_MPIO_GLOBAL_NO_COLLECTIVE_CAUSE_NAME, mpio_global_no_coll_cause)
#ifdef H5_HAVE_INSTRUMENTED_LIBRARY
        H5CX_SET_PROP(H5D_XFER_COLL_CHUNK_LINK_HARD_NAME, mpio_coll_chunk_link_hard)
        H5CX_SET_PROP(H5D_XFER_COLL_CHUNK_MULTI_HARD_NAME, mpio_coll_chunk_multi_hard)
        H5CX_SET_PROP(H5D_XFER_COLL_CHUNK_LINK_NUM_TRUE_NAME, mpio_coll_chunk_link_num_true)
        H5CX_SET_PROP(H5D_XFER_COLL_CHUNK_LINK_NUM_FALSE_NAME, mpio_coll_chunk_link_num_false)
        H5CX_SET_PROP(H5D_XFER_COLL_CHUNK_MULTI_RATIO_COLL_NAME, mpio_coll_chunk_multi_ratio_coll)
        H5CX_SET_PROP(H5D_XFER_COLL_CHUNK_MULTI_RATIO_IND_NAME, mpio_coll_chunk_multi_ratio_ind)
        H5CX_SET_PROP(H5D_XFER_COLL_RANK0_BCAST_NAME, mpio_coll_rank0_bcast)
#endif /* H5_HAVE_INSTRUMENTED_LIBRARY */
#endif /* H5_HAVE_PARALLEL */
    }  /* end if */

    /* Pop the top context node from the stack */
    ret_value = (*head);
    (*head)   = (*head)->next;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX__pop_common() */

/*-------------------------------------------------------------------------
 * Function:    H5CX_pop
 *
 * Purpose:     Pops the context for an API call.
 *
 * Return:      Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5CX_pop(bool update_dxpl_props)
{
    H5CX_node_t *cnode;               /* Context node */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Perform common operations and get top context from stack */
    if (NULL == (cnode = H5CX__pop_common(update_dxpl_props)))
        HGOTO_ERROR(H5E_CONTEXT, H5E_CANTGET, FAIL, "error getting API context node");

    /* Free the context node */
    cnode = H5FL_FREE(H5CX_node_t, cnode);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5CX_pop() */
