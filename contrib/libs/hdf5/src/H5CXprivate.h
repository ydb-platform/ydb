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
 *  Header file for API contexts, etc.
 */
#ifndef H5CXprivate_H
#define H5CXprivate_H

/* Private headers needed by this file */
#include "H5private.h"   /* Generic Functions                    */
#include "H5ACprivate.h" /* Metadata cache                       */
#ifdef H5_HAVE_PARALLEL
#include "H5FDprivate.h" /* File drivers                         */
#endif                   /* H5_HAVE_PARALLEL */
#include "H5Zprivate.h"  /* Data filters                         */

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/* API context state */
typedef struct H5CX_state_t {
    hid_t                 dcpl_id;            /* DCPL for operation */
    hid_t                 dxpl_id;            /* DXPL for operation */
    hid_t                 lapl_id;            /* LAPL for operation */
    hid_t                 lcpl_id;            /* LCPL for operation */
    void                 *vol_wrap_ctx;       /* VOL connector's "wrap context" for creating IDs */
    H5VL_connector_prop_t vol_connector_prop; /* VOL connector property */

#ifdef H5_HAVE_PARALLEL
    /* Internal: Parallel I/O settings */
    bool coll_metadata_read; /* Whether to use collective I/O for metadata read */
#endif                       /* H5_HAVE_PARALLEL */
} H5CX_state_t;

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/

/* Library private routines */
#ifndef H5private_H
H5_DLL herr_t H5CX_push(void);
H5_DLL herr_t H5CX_pop(bool update_dxpl_props);
#endif /* H5private_H */
H5_DLL void H5CX_push_special(void);
H5_DLL bool H5CX_is_def_dxpl(void);

/* API context state routines */
H5_DLL herr_t H5CX_retrieve_state(H5CX_state_t **api_state);
H5_DLL herr_t H5CX_restore_state(const H5CX_state_t *api_state);
H5_DLL herr_t H5CX_free_state(H5CX_state_t *api_state);

/* "Setter" routines for API context info */
H5_DLL void   H5CX_set_dxpl(hid_t dxpl_id);
H5_DLL void   H5CX_set_lcpl(hid_t lcpl_id);
H5_DLL void   H5CX_set_lapl(hid_t lapl_id);
H5_DLL void   H5CX_set_dcpl(hid_t dcpl_id);
H5_DLL herr_t H5CX_set_libver_bounds(H5F_t *f);
H5_DLL herr_t H5CX_set_apl(hid_t *acspl_id, const H5P_libclass_t *libclass, hid_t loc_id, bool is_collective);
H5_DLL herr_t H5CX_set_loc(hid_t loc_id);
H5_DLL herr_t H5CX_set_vol_wrap_ctx(void *wrap_ctx);
H5_DLL herr_t H5CX_set_vol_connector_prop(const H5VL_connector_prop_t *vol_connector_prop);

/* "Getter" routines for API context info */
H5_DLL hid_t       H5CX_get_dxpl(void);
H5_DLL hid_t       H5CX_get_lapl(void);
H5_DLL herr_t      H5CX_get_vol_wrap_ctx(void **wrap_ctx);
H5_DLL herr_t      H5CX_get_vol_connector_prop(H5VL_connector_prop_t *vol_connector_prop);
H5_DLL haddr_t     H5CX_get_tag(void);
H5_DLL H5AC_ring_t H5CX_get_ring(void);
#ifdef H5_HAVE_PARALLEL
H5_DLL bool   H5CX_get_coll_metadata_read(void);
H5_DLL herr_t H5CX_get_mpi_coll_datatypes(MPI_Datatype *btype, MPI_Datatype *ftype);
H5_DLL bool   H5CX_get_mpi_file_flushing(void);
H5_DLL bool   H5CX_get_mpio_rank0_bcast(void);
#endif /* H5_HAVE_PARALLEL */

/* "Getter" routines for DXPL properties cached in API context */
H5_DLL herr_t H5CX_get_btree_split_ratios(double split_ratio[3]);
H5_DLL herr_t H5CX_get_max_temp_buf(size_t *max_temp_buf);
H5_DLL herr_t H5CX_get_tconv_buf(void **tconv_buf);
H5_DLL herr_t H5CX_get_bkgr_buf(void **bkgr_buf);
H5_DLL herr_t H5CX_get_bkgr_buf_type(H5T_bkg_t *bkgr_buf_type);
H5_DLL herr_t H5CX_get_vec_size(size_t *vec_size);
#ifdef H5_HAVE_PARALLEL
H5_DLL herr_t H5CX_get_io_xfer_mode(H5FD_mpio_xfer_t *io_xfer_mode);
H5_DLL herr_t H5CX_get_mpio_coll_opt(H5FD_mpio_collective_opt_t *mpio_coll_opt);
H5_DLL herr_t H5CX_get_mpio_local_no_coll_cause(uint32_t *mpio_local_no_coll_cause);
H5_DLL herr_t H5CX_get_mpio_global_no_coll_cause(uint32_t *mpio_global_no_coll_cause);
H5_DLL herr_t H5CX_get_mpio_chunk_opt_mode(H5FD_mpio_chunk_opt_t *mpio_chunk_opt_mode);
H5_DLL herr_t H5CX_get_mpio_chunk_opt_num(unsigned *mpio_chunk_opt_num);
H5_DLL herr_t H5CX_get_mpio_chunk_opt_ratio(unsigned *mpio_chunk_opt_ratio);
#endif /* H5_HAVE_PARALLEL */
H5_DLL herr_t H5CX_get_err_detect(H5Z_EDC_t *err_detect);
H5_DLL herr_t H5CX_get_filter_cb(H5Z_cb_t *filter_cb);
H5_DLL herr_t H5CX_get_data_transform(H5Z_data_xform_t **data_transform);
H5_DLL herr_t H5CX_get_vlen_alloc_info(H5T_vlen_alloc_info_t *vl_alloc_info);
H5_DLL herr_t H5CX_get_dt_conv_cb(H5T_conv_cb_t *cb_struct);
H5_DLL herr_t H5CX_get_selection_io_mode(H5D_selection_io_mode_t *selection_io_mode);
H5_DLL herr_t H5CX_get_no_selection_io_cause(uint32_t *no_selection_io_cause);
H5_DLL herr_t H5CX_get_actual_selection_io_mode(uint32_t *actual_selection_io_mode);
H5_DLL herr_t H5CX_get_modify_write_buf(bool *modify_write_buf);

/* "Getter" routines for LCPL properties cached in API context */
H5_DLL herr_t H5CX_get_encoding(H5T_cset_t *encoding);
H5_DLL herr_t H5CX_get_intermediate_group(unsigned *crt_intermed_group);

/* "Getter" routines for LAPL properties cached in API context */
H5_DLL herr_t H5CX_get_nlinks(size_t *nlinks);

/* "Getter" routines for DCPL properties cached in API context */
H5_DLL herr_t H5CX_get_dset_min_ohdr_flag(bool *dset_min_ohdr_flag);
H5_DLL herr_t H5CX_get_ohdr_flags(uint8_t *ohdr_flags);

/* "Getter" routines for DAPL properties cached in API context */
H5_DLL herr_t H5CX_get_ext_file_prefix(const char **prefix_extfile);
H5_DLL herr_t H5CX_get_vds_prefix(const char **prefix_vds);

/* "Getter" routines for FAPL properties cached in API context */
H5_DLL herr_t H5CX_get_libver_bounds(H5F_libver_t *low_bound, H5F_libver_t *high_bound);

/* "Setter" routines for API context info */
H5_DLL void H5CX_set_tag(haddr_t tag);
H5_DLL void H5CX_set_ring(H5AC_ring_t ring);
#ifdef H5_HAVE_PARALLEL
H5_DLL void   H5CX_set_coll_metadata_read(bool cmdr);
H5_DLL herr_t H5CX_set_mpi_coll_datatypes(MPI_Datatype btype, MPI_Datatype ftype);
H5_DLL herr_t H5CX_set_mpio_coll_opt(H5FD_mpio_collective_opt_t mpio_coll_opt);
H5_DLL void   H5CX_set_mpi_file_flushing(bool flushing);
H5_DLL void   H5CX_set_mpio_rank0_bcast(bool rank0_bcast);
#endif /* H5_HAVE_PARALLEL */

/* "Setter" routines for DXPL properties cached in API context */
#ifdef H5_HAVE_PARALLEL
H5_DLL herr_t H5CX_set_io_xfer_mode(H5FD_mpio_xfer_t io_xfer_mode);
#endif /* H5_HAVE_PARALLEL */
H5_DLL herr_t H5CX_set_vlen_alloc_info(H5MM_allocate_t alloc_func, void *alloc_info, H5MM_free_t free_func,
                                       void *free_info);

/* "Setter" routines for LAPL properties cached in API context */
H5_DLL herr_t H5CX_set_nlinks(size_t nlinks);

H5_DLL herr_t H5CX_init(void);

/* "Setter" routines for cached DXPL properties that must be returned to application */

H5_DLL void H5CX_set_no_selection_io_cause(uint32_t no_selection_io_cause);
H5_DLL void H5CX_set_actual_selection_io_mode(uint32_t actual_selection_io_mode);

#ifdef H5_HAVE_PARALLEL
H5_DLL void H5CX_set_mpio_actual_chunk_opt(H5D_mpio_actual_chunk_opt_mode_t chunk_opt);
H5_DLL void H5CX_set_mpio_actual_io_mode(H5D_mpio_actual_io_mode_t actual_io_mode);
H5_DLL void H5CX_set_mpio_local_no_coll_cause(uint32_t mpio_local_no_coll_cause);
H5_DLL void H5CX_set_mpio_global_no_coll_cause(uint32_t mpio_global_no_coll_cause);
#ifdef H5_HAVE_INSTRUMENTED_LIBRARY
H5_DLL herr_t H5CX_test_set_mpio_coll_chunk_link_hard(int mpio_coll_chunk_link_hard);
H5_DLL herr_t H5CX_test_set_mpio_coll_chunk_multi_hard(int mpio_coll_chunk_multi_hard);
H5_DLL herr_t H5CX_test_set_mpio_coll_chunk_link_num_true(int mpio_coll_chunk_link_num_true);
H5_DLL herr_t H5CX_test_set_mpio_coll_chunk_link_num_false(int mpio_coll_chunk_link_num_false);
H5_DLL herr_t H5CX_test_set_mpio_coll_chunk_multi_ratio_coll(int mpio_coll_chunk_multi_ratio_coll);
H5_DLL herr_t H5CX_test_set_mpio_coll_chunk_multi_ratio_ind(int mpio_coll_chunk_multi_ratio_ind);
H5_DLL herr_t H5CX_test_set_mpio_coll_rank0_bcast(bool rank0_bcast);
#endif /* H5_HAVE_INSTRUMENTED_LIBRARY */
#endif /* H5_HAVE_PARALLEL */

#endif /* H5CXprivate_H */
