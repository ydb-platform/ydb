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
 * Created:		H5Pdxpl.c
 *
 * Purpose:		Data transfer property list class routines
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Pmodule.h" /* This source code file is part of the H5P module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Cache                                    */
#include "H5Dprivate.h"  /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FDprivate.h" /* File drivers                             */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Ppkg.h"      /* Property lists                           */
#include "H5VMprivate.h" /* Vector Functions                         */

/****************/
/* Local Macros */
/****************/

/* ======== Data transfer properties ======== */
/* Definitions for maximum temp buffer size property */
#define H5D_XFER_MAX_TEMP_BUF_SIZE sizeof(size_t)
#define H5D_XFER_MAX_TEMP_BUF_DEF  H5D_TEMP_BUF_SIZE
#define H5D_XFER_MAX_TEMP_BUF_ENC  H5P__encode_size_t
#define H5D_XFER_MAX_TEMP_BUF_DEC  H5P__decode_size_t
/* Definitions for type conversion buffer property */
#define H5D_XFER_TCONV_BUF_SIZE sizeof(void *)
#define H5D_XFER_TCONV_BUF_DEF  NULL
/* Definitions for background buffer property */
#define H5D_XFER_BKGR_BUF_SIZE sizeof(void *)
#define H5D_XFER_BKGR_BUF_DEF  NULL
/* Definitions for background buffer type property */
#define H5D_XFER_BKGR_BUF_TYPE_SIZE sizeof(H5T_bkg_t)
#define H5D_XFER_BKGR_BUF_TYPE_DEF  H5T_BKG_NO
#define H5D_XFER_BKGR_BUF_TYPE_ENC  H5P__dxfr_bkgr_buf_type_enc
#define H5D_XFER_BKGR_BUF_TYPE_DEC  H5P__dxfr_bkgr_buf_type_dec
/* Definitions for B-tree node splitting ratio property */
/* (These default B-tree node splitting ratios are also used for splitting
 * group's B-trees as well as chunked dataset's B-trees - QAK)
 */
#define H5D_XFER_BTREE_SPLIT_RATIO_SIZE sizeof(double[3])
#define H5D_XFER_BTREE_SPLIT_RATIO_DEF                                                                       \
    {                                                                                                        \
        0.1, 0.5, 0.9                                                                                        \
    }
#define H5D_XFER_BTREE_SPLIT_RATIO_ENC H5P__dxfr_btree_split_ratio_enc
#define H5D_XFER_BTREE_SPLIT_RATIO_DEC H5P__dxfr_btree_split_ratio_dec
/* Definitions for vlen allocation function property */
#define H5D_XFER_VLEN_ALLOC_SIZE sizeof(H5MM_allocate_t)
#define H5D_XFER_VLEN_ALLOC_DEF  H5D_VLEN_ALLOC
/* Definitions for vlen allocation info property */
#define H5D_XFER_VLEN_ALLOC_INFO_SIZE sizeof(void *)
#define H5D_XFER_VLEN_ALLOC_INFO_DEF  H5D_VLEN_ALLOC_INFO
/* Definitions for vlen free function property */
#define H5D_XFER_VLEN_FREE_SIZE sizeof(H5MM_free_t)
#define H5D_XFER_VLEN_FREE_DEF  H5D_VLEN_FREE
/* Definitions for vlen free info property */
#define H5D_XFER_VLEN_FREE_INFO_SIZE sizeof(void *)
#define H5D_XFER_VLEN_FREE_INFO_DEF  H5D_VLEN_FREE_INFO
/* Definitions for hyperslab vector size property */
/* (Be cautious about increasing the default size, there are arrays allocated
 *      on the stack which depend on it - QAK)
 */
#define H5D_XFER_HYPER_VECTOR_SIZE_SIZE sizeof(size_t)
#define H5D_XFER_HYPER_VECTOR_SIZE_DEF  H5D_IO_VECTOR_SIZE
#define H5D_XFER_HYPER_VECTOR_SIZE_ENC  H5P__encode_size_t
#define H5D_XFER_HYPER_VECTOR_SIZE_DEC  H5P__decode_size_t

/* Parallel I/O properties */
/* Note: Some of these are registered with the DXPL class even when parallel
 *      is disabled, so that property list comparisons of encoded property
 *      lists (between parallel & non-parallel builds) work properly. -QAK
 */

/* Definitions for I/O transfer mode property */
#define H5D_XFER_IO_XFER_MODE_SIZE sizeof(H5FD_mpio_xfer_t)
#define H5D_XFER_IO_XFER_MODE_DEF  H5FD_MPIO_INDEPENDENT
#define H5D_XFER_IO_XFER_MODE_ENC  H5P__dxfr_io_xfer_mode_enc
#define H5D_XFER_IO_XFER_MODE_DEC  H5P__dxfr_io_xfer_mode_dec
/* Definitions for optimization of MPI-IO transfer mode property */
#define H5D_XFER_MPIO_COLLECTIVE_OPT_SIZE  sizeof(H5FD_mpio_collective_opt_t)
#define H5D_XFER_MPIO_COLLECTIVE_OPT_DEF   H5FD_MPIO_COLLECTIVE_IO
#define H5D_XFER_MPIO_COLLECTIVE_OPT_ENC   H5P__dxfr_mpio_collective_opt_enc
#define H5D_XFER_MPIO_COLLECTIVE_OPT_DEC   H5P__dxfr_mpio_collective_opt_dec
#define H5D_XFER_MPIO_CHUNK_OPT_HARD_SIZE  sizeof(H5FD_mpio_chunk_opt_t)
#define H5D_XFER_MPIO_CHUNK_OPT_HARD_DEF   H5FD_MPIO_CHUNK_DEFAULT
#define H5D_XFER_MPIO_CHUNK_OPT_HARD_ENC   H5P__dxfr_mpio_chunk_opt_hard_enc
#define H5D_XFER_MPIO_CHUNK_OPT_HARD_DEC   H5P__dxfr_mpio_chunk_opt_hard_dec
#define H5D_XFER_MPIO_CHUNK_OPT_NUM_SIZE   sizeof(unsigned)
#define H5D_XFER_MPIO_CHUNK_OPT_NUM_DEF    H5D_ONE_LINK_CHUNK_IO_THRESHOLD
#define H5D_XFER_MPIO_CHUNK_OPT_NUM_ENC    H5P__encode_unsigned
#define H5D_XFER_MPIO_CHUNK_OPT_NUM_DEC    H5P__decode_unsigned
#define H5D_XFER_MPIO_CHUNK_OPT_RATIO_SIZE sizeof(unsigned)
#define H5D_XFER_MPIO_CHUNK_OPT_RATIO_DEF  H5D_MULTI_CHUNK_IO_COL_THRESHOLD
#define H5D_XFER_MPIO_CHUNK_OPT_RATIO_ENC  H5P__encode_unsigned
#define H5D_XFER_MPIO_CHUNK_OPT_RATIO_DEC  H5P__decode_unsigned
/* Definitions for chunk opt mode property. */
#define H5D_MPIO_ACTUAL_CHUNK_OPT_MODE_SIZE sizeof(H5D_mpio_actual_chunk_opt_mode_t)
#define H5D_MPIO_ACTUAL_CHUNK_OPT_MODE_DEF  H5D_MPIO_NO_CHUNK_OPTIMIZATION
/* Definitions for chunk io mode property. */
#define H5D_MPIO_ACTUAL_IO_MODE_SIZE sizeof(H5D_mpio_actual_io_mode_t)
#define H5D_MPIO_ACTUAL_IO_MODE_DEF  H5D_MPIO_NO_COLLECTIVE
/* Definitions for cause of broken collective io property */
#define H5D_MPIO_NO_COLLECTIVE_CAUSE_SIZE sizeof(uint32_t)
#define H5D_MPIO_NO_COLLECTIVE_CAUSE_DEF  H5D_MPIO_COLLECTIVE

/* Definitions for EDC property */
#define H5D_XFER_EDC_SIZE sizeof(H5Z_EDC_t)
#define H5D_XFER_EDC_DEF  H5Z_ENABLE_EDC
#define H5D_XFER_EDC_ENC  H5P__dxfr_edc_enc
#define H5D_XFER_EDC_DEC  H5P__dxfr_edc_dec
/* Definitions for filter callback function property */
#define H5D_XFER_FILTER_CB_SIZE sizeof(H5Z_cb_t)
#define H5D_XFER_FILTER_CB_DEF                                                                               \
    {                                                                                                        \
        NULL, NULL                                                                                           \
    }
/* Definitions for type conversion callback function property */
#define H5D_XFER_CONV_CB_SIZE sizeof(H5T_conv_cb_t)
#define H5D_XFER_CONV_CB_DEF                                                                                 \
    {                                                                                                        \
        NULL, NULL                                                                                           \
    }
/* Definitions for data transform property */
#define H5D_XFER_XFORM_SIZE  sizeof(void *)
#define H5D_XFER_XFORM_DEF   NULL
#define H5D_XFER_XFORM_SET   H5P__dxfr_xform_set
#define H5D_XFER_XFORM_GET   H5P__dxfr_xform_get
#define H5D_XFER_XFORM_ENC   H5P__dxfr_xform_enc
#define H5D_XFER_XFORM_DEC   H5P__dxfr_xform_dec
#define H5D_XFER_XFORM_DEL   H5P__dxfr_xform_del
#define H5D_XFER_XFORM_COPY  H5P__dxfr_xform_copy
#define H5D_XFER_XFORM_CMP   H5P__dxfr_xform_cmp
#define H5D_XFER_XFORM_CLOSE H5P__dxfr_xform_close
/* Definitions for dataset I/O selection property */
#define H5D_XFER_DSET_IO_SEL_SIZE  sizeof(H5S_t *)
#define H5D_XFER_DSET_IO_SEL_DEF   NULL
#define H5D_XFER_DSET_IO_SEL_COPY  H5P__dxfr_dset_io_hyp_sel_copy
#define H5D_XFER_DSET_IO_SEL_CMP   H5P__dxfr_dset_io_hyp_sel_cmp
#define H5D_XFER_DSET_IO_SEL_CLOSE H5P__dxfr_dset_io_hyp_sel_close
#ifdef QAK
#define H5D_XFER_DSET_IO_SEL_ENC H5P__dxfr_edc_enc
#define H5D_XFER_DSET_IO_SEL_DEC H5P__dxfr_edc_dec
#endif /* QAK */
/* Definition for selection I/O mode property */
#define H5D_XFER_SELECTION_IO_MODE_SIZE sizeof(H5D_selection_io_mode_t)
#define H5D_XFER_SELECTION_IO_MODE_DEF  H5D_SELECTION_IO_MODE_DEFAULT
#define H5D_XFER_SELECTION_IO_MODE_ENC  H5P__dxfr_selection_io_mode_enc
#define H5D_XFER_SELECTION_IO_MODE_DEC  H5P__dxfr_selection_io_mode_dec
/* Definitions for cause of no selection I/O property */
#define H5D_XFER_NO_SELECTION_IO_CAUSE_SIZE sizeof(uint32_t)
#define H5D_XFER_NO_SELECTION_IO_CAUSE_DEF  0
/* Definitions for actual selection I/O mode property */
#define H5D_XFER_ACTUAL_SELECTION_IO_MODE_SIZE sizeof(uint32_t)
#define H5D_XFER_ACTUAL_SELECTION_IO_MODE_DEF  0
/* Definitions for modify write buffer property */
#define H5D_XFER_MODIFY_WRITE_BUF_SIZE sizeof(bool)
#define H5D_XFER_MODIFY_WRITE_BUF_DEF  false
#define H5D_XFER_MODIFY_WRITE_BUF_ENC  H5P__dxfr_modify_write_buf_enc
#define H5D_XFER_MODIFY_WRITE_BUF_DEC  H5P__dxfr_modify_write_buf_dec

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* Property class callbacks */
static herr_t H5P__dxfr_reg_prop(H5P_genclass_t *pclass);

/* Property list callbacks */
static herr_t H5P__dxfr_bkgr_buf_type_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dxfr_bkgr_buf_type_dec(const void **pp, void *value);
static herr_t H5P__dxfr_btree_split_ratio_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dxfr_btree_split_ratio_dec(const void **pp, void *value);
static herr_t H5P__dxfr_io_xfer_mode_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dxfr_io_xfer_mode_dec(const void **pp, void *value);
static herr_t H5P__dxfr_mpio_collective_opt_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dxfr_mpio_collective_opt_dec(const void **pp, void *value);
static herr_t H5P__dxfr_mpio_chunk_opt_hard_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dxfr_mpio_chunk_opt_hard_dec(const void **pp, void *value);
static herr_t H5P__dxfr_edc_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dxfr_edc_dec(const void **pp, void *value);
static herr_t H5P__dxfr_xform_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dxfr_xform_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dxfr_xform_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dxfr_xform_dec(const void **pp, void *value);
static herr_t H5P__dxfr_xform_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dxfr_xform_copy(const char *name, size_t size, void *value);
static int    H5P__dxfr_xform_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__dxfr_xform_close(const char *name, size_t size, void *value);
static herr_t H5P__dxfr_dset_io_hyp_sel_copy(const char *name, size_t size, void *value);
static int    H5P__dxfr_dset_io_hyp_sel_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__dxfr_dset_io_hyp_sel_close(const char *name, size_t size, void *value);
static herr_t H5P__dxfr_selection_io_mode_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dxfr_selection_io_mode_dec(const void **pp, void *value);
static herr_t H5P__dxfr_modify_write_buf_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dxfr_modify_write_buf_dec(const void **pp, void *value);

/*********************/
/* Package Variables */
/*********************/

/* Data transfer property list class library initialization object */
const H5P_libclass_t H5P_CLS_DXFR[1] = {{
    "data transfer",       /* Class name for debugging     */
    H5P_TYPE_DATASET_XFER, /* Class type                   */

    &H5P_CLS_ROOT_g,            /* Parent class                 */
    &H5P_CLS_DATASET_XFER_g,    /* Pointer to class             */
    &H5P_CLS_DATASET_XFER_ID_g, /* Pointer to class ID          */
    &H5P_LST_DATASET_XFER_ID_g, /* Pointer to default property list ID */
    H5P__dxfr_reg_prop,         /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/***************************/
/* Local Private Variables */
/***************************/

/* Property value defaults */
static const size_t H5D_def_max_temp_buf_g =
    H5D_XFER_MAX_TEMP_BUF_DEF; /* Default value for maximum temp buffer size */
static const void *H5D_def_tconv_buf_g =
    H5D_XFER_TCONV_BUF_DEF; /* Default value for type conversion buffer */
static const void     *H5D_def_bkgr_buf_g = H5D_XFER_BKGR_BUF_DEF; /* Default value for background buffer */
static const H5T_bkg_t H5D_def_bkgr_buf_type_g = H5D_XFER_BKGR_BUF_TYPE_DEF;
static const double    H5D_def_btree_split_ratio_g[3] =
    H5D_XFER_BTREE_SPLIT_RATIO_DEF; /* Default value for B-tree node split ratios */
static const H5MM_allocate_t H5D_def_vlen_alloc_g =
    H5D_XFER_VLEN_ALLOC_DEF; /* Default value for vlen allocation function */
static const void *H5D_def_vlen_alloc_info_g =
    H5D_XFER_VLEN_ALLOC_INFO_DEF; /* Default value for vlen allocation information */
static const H5MM_free_t H5D_def_vlen_free_g =
    H5D_XFER_VLEN_FREE_DEF; /* Default value for vlen free function */
static const void *H5D_def_vlen_free_info_g =
    H5D_XFER_VLEN_FREE_INFO_DEF; /* Default value for vlen free information */
static const size_t H5D_def_hyp_vec_size_g =
    H5D_XFER_HYPER_VECTOR_SIZE_DEF; /* Default value for vector size */
static const H5FD_mpio_xfer_t H5D_def_io_xfer_mode_g =
    H5D_XFER_IO_XFER_MODE_DEF; /* Default value for I/O transfer mode */
static const H5FD_mpio_chunk_opt_t      H5D_def_mpio_chunk_opt_mode_g      = H5D_XFER_MPIO_CHUNK_OPT_HARD_DEF;
static const H5FD_mpio_collective_opt_t H5D_def_mpio_collective_opt_mode_g = H5D_XFER_MPIO_COLLECTIVE_OPT_DEF;
static const unsigned                   H5D_def_mpio_chunk_opt_num_g       = H5D_XFER_MPIO_CHUNK_OPT_NUM_DEF;
static const unsigned                   H5D_def_mpio_chunk_opt_ratio_g = H5D_XFER_MPIO_CHUNK_OPT_RATIO_DEF;
static const H5D_mpio_actual_chunk_opt_mode_t H5D_def_mpio_actual_chunk_opt_mode_g =
    H5D_MPIO_ACTUAL_CHUNK_OPT_MODE_DEF;
static const H5D_mpio_actual_io_mode_t      H5D_def_mpio_actual_io_mode_g = H5D_MPIO_ACTUAL_IO_MODE_DEF;
static const H5D_mpio_no_collective_cause_t H5D_def_mpio_no_collective_cause_g =
    H5D_MPIO_NO_COLLECTIVE_CAUSE_DEF;
static const H5Z_EDC_t H5D_def_enable_edc_g = H5D_XFER_EDC_DEF;       /* Default value for EDC property */
static const H5Z_cb_t  H5D_def_filter_cb_g  = H5D_XFER_FILTER_CB_DEF; /* Default value for filter callback */
static const H5T_conv_cb_t H5D_def_conv_cb_g =
    H5D_XFER_CONV_CB_DEF; /* Default value for datatype conversion callback */
static const void  *H5D_def_xfer_xform_g = H5D_XFER_XFORM_DEF; /* Default value for data transform */
static const H5S_t *H5D_def_dset_io_sel_g =
    H5D_XFER_DSET_IO_SEL_DEF; /* Default value for dataset I/O selection */
static const H5D_selection_io_mode_t H5D_def_selection_io_mode_g     = H5D_XFER_SELECTION_IO_MODE_DEF;
static const uint32_t                H5D_def_no_selection_io_cause_g = H5D_XFER_NO_SELECTION_IO_CAUSE_DEF;
static const uint32_t H5D_def_actual_selection_io_mode_g             = H5D_XFER_ACTUAL_SELECTION_IO_MODE_DEF;
static const bool     H5D_def_modify_write_buf_g                     = H5D_XFER_MODIFY_WRITE_BUF_DEF;

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_reg_prop
 *
 * Purpose:     Register the data transfer property list class's properties
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_reg_prop(H5P_genclass_t *pclass)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Register the max. temp buffer size property */
    if (H5P__register_real(pclass, H5D_XFER_MAX_TEMP_BUF_NAME, H5D_XFER_MAX_TEMP_BUF_SIZE,
                           &H5D_def_max_temp_buf_g, NULL, NULL, NULL, H5D_XFER_MAX_TEMP_BUF_ENC,
                           H5D_XFER_MAX_TEMP_BUF_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the type conversion buffer property */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_XFER_TCONV_BUF_NAME, H5D_XFER_TCONV_BUF_SIZE, &H5D_def_tconv_buf_g,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the background buffer property */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_XFER_BKGR_BUF_NAME, H5D_XFER_BKGR_BUF_SIZE, &H5D_def_bkgr_buf_g, NULL,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the background buffer type property */
    if (H5P__register_real(pclass, H5D_XFER_BKGR_BUF_TYPE_NAME, H5D_XFER_BKGR_BUF_TYPE_SIZE,
                           &H5D_def_bkgr_buf_type_g, NULL, NULL, NULL, H5D_XFER_BKGR_BUF_TYPE_ENC,
                           H5D_XFER_BKGR_BUF_TYPE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the B-Tree node splitting ratios property */
    if (H5P__register_real(pclass, H5D_XFER_BTREE_SPLIT_RATIO_NAME, H5D_XFER_BTREE_SPLIT_RATIO_SIZE,
                           H5D_def_btree_split_ratio_g, NULL, NULL, NULL, H5D_XFER_BTREE_SPLIT_RATIO_ENC,
                           H5D_XFER_BTREE_SPLIT_RATIO_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the vlen allocation function property */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_XFER_VLEN_ALLOC_NAME, H5D_XFER_VLEN_ALLOC_SIZE, &H5D_def_vlen_alloc_g,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the vlen allocation information property */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_XFER_VLEN_ALLOC_INFO_NAME, H5D_XFER_VLEN_ALLOC_INFO_SIZE,
                           &H5D_def_vlen_alloc_info_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the vlen free function property */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_XFER_VLEN_FREE_NAME, H5D_XFER_VLEN_FREE_SIZE, &H5D_def_vlen_free_g,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the vlen free information property */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_XFER_VLEN_FREE_INFO_NAME, H5D_XFER_VLEN_FREE_INFO_SIZE,
                           &H5D_def_vlen_free_info_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the vector size property */
    if (H5P__register_real(pclass, H5D_XFER_HYPER_VECTOR_SIZE_NAME, H5D_XFER_HYPER_VECTOR_SIZE_SIZE,
                           &H5D_def_hyp_vec_size_g, NULL, NULL, NULL, H5D_XFER_HYPER_VECTOR_SIZE_ENC,
                           H5D_XFER_HYPER_VECTOR_SIZE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the I/O transfer mode properties */
    if (H5P__register_real(pclass, H5D_XFER_IO_XFER_MODE_NAME, H5D_XFER_IO_XFER_MODE_SIZE,
                           &H5D_def_io_xfer_mode_g, NULL, NULL, NULL, H5D_XFER_IO_XFER_MODE_ENC,
                           H5D_XFER_IO_XFER_MODE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");
    if (H5P__register_real(pclass, H5D_XFER_MPIO_COLLECTIVE_OPT_NAME, H5D_XFER_MPIO_COLLECTIVE_OPT_SIZE,
                           &H5D_def_mpio_collective_opt_mode_g, NULL, NULL, NULL,
                           H5D_XFER_MPIO_COLLECTIVE_OPT_ENC, H5D_XFER_MPIO_COLLECTIVE_OPT_DEC, NULL, NULL,
                           NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");
    if (H5P__register_real(pclass, H5D_XFER_MPIO_CHUNK_OPT_HARD_NAME, H5D_XFER_MPIO_CHUNK_OPT_HARD_SIZE,
                           &H5D_def_mpio_chunk_opt_mode_g, NULL, NULL, NULL, H5D_XFER_MPIO_CHUNK_OPT_HARD_ENC,
                           H5D_XFER_MPIO_CHUNK_OPT_HARD_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");
    if (H5P__register_real(pclass, H5D_XFER_MPIO_CHUNK_OPT_NUM_NAME, H5D_XFER_MPIO_CHUNK_OPT_NUM_SIZE,
                           &H5D_def_mpio_chunk_opt_num_g, NULL, NULL, NULL, H5D_XFER_MPIO_CHUNK_OPT_NUM_ENC,
                           H5D_XFER_MPIO_CHUNK_OPT_NUM_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");
    if (H5P__register_real(pclass, H5D_XFER_MPIO_CHUNK_OPT_RATIO_NAME, H5D_XFER_MPIO_CHUNK_OPT_RATIO_SIZE,
                           &H5D_def_mpio_chunk_opt_ratio_g, NULL, NULL, NULL,
                           H5D_XFER_MPIO_CHUNK_OPT_RATIO_ENC, H5D_XFER_MPIO_CHUNK_OPT_RATIO_DEC, NULL, NULL,
                           NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the chunk optimization mode property. */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_MPIO_ACTUAL_CHUNK_OPT_MODE_NAME, H5D_MPIO_ACTUAL_CHUNK_OPT_MODE_SIZE,
                           &H5D_def_mpio_actual_chunk_opt_mode_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the actual I/O mode property. */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_MPIO_ACTUAL_IO_MODE_NAME, H5D_MPIO_ACTUAL_IO_MODE_SIZE,
                           &H5D_def_mpio_actual_io_mode_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the local cause of broken collective I/O */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_MPIO_LOCAL_NO_COLLECTIVE_CAUSE_NAME, H5D_MPIO_NO_COLLECTIVE_CAUSE_SIZE,
                           &H5D_def_mpio_no_collective_cause_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the global cause of broken collective I/O */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_MPIO_GLOBAL_NO_COLLECTIVE_CAUSE_NAME,
                           H5D_MPIO_NO_COLLECTIVE_CAUSE_SIZE, &H5D_def_mpio_no_collective_cause_g, NULL, NULL,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the EDC property */
    if (H5P__register_real(pclass, H5D_XFER_EDC_NAME, H5D_XFER_EDC_SIZE, &H5D_def_enable_edc_g, NULL, NULL,
                           NULL, H5D_XFER_EDC_ENC, H5D_XFER_EDC_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the filter callback property */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_XFER_FILTER_CB_NAME, H5D_XFER_FILTER_CB_SIZE, &H5D_def_filter_cb_g,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the type conversion callback property */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_XFER_CONV_CB_NAME, H5D_XFER_CONV_CB_SIZE, &H5D_def_conv_cb_g, NULL,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the data transform property */
    if (H5P__register_real(pclass, H5D_XFER_XFORM_NAME, H5D_XFER_XFORM_SIZE, &H5D_def_xfer_xform_g, NULL,
                           H5D_XFER_XFORM_SET, H5D_XFER_XFORM_GET, H5D_XFER_XFORM_ENC, H5D_XFER_XFORM_DEC,
                           H5D_XFER_XFORM_DEL, H5D_XFER_XFORM_COPY, H5D_XFER_XFORM_CMP,
                           H5D_XFER_XFORM_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the dataset I/O selection property */
    if (H5P__register_real(pclass, H5D_XFER_DSET_IO_SEL_NAME, H5D_XFER_DSET_IO_SEL_SIZE,
                           &H5D_def_dset_io_sel_g, NULL, NULL, NULL, NULL, NULL, NULL,
                           H5D_XFER_DSET_IO_SEL_COPY, H5D_XFER_DSET_IO_SEL_CMP,
                           H5D_XFER_DSET_IO_SEL_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    if (H5P__register_real(pclass, H5D_XFER_SELECTION_IO_MODE_NAME, H5D_XFER_SELECTION_IO_MODE_SIZE,
                           &H5D_def_selection_io_mode_g, NULL, NULL, NULL, H5D_XFER_SELECTION_IO_MODE_ENC,
                           H5D_XFER_SELECTION_IO_MODE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the cause of no selection I/O property */
    /* (Note: this property should not have an encode/decode callback) */
    if (H5P__register_real(pclass, H5D_XFER_NO_SELECTION_IO_CAUSE_NAME, H5D_XFER_NO_SELECTION_IO_CAUSE_SIZE,
                           &H5D_def_no_selection_io_cause_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the actual selection I/O mode property */
    /* (Note: this property should not have an encode/decode callback) */
    if (H5P__register_real(pclass, H5D_XFER_ACTUAL_SELECTION_IO_MODE_NAME,
                           H5D_XFER_ACTUAL_SELECTION_IO_MODE_SIZE, &H5D_def_actual_selection_io_mode_g, NULL,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the modify write buffer property */
    if (H5P__register_real(pclass, H5D_XFER_MODIFY_WRITE_BUF_NAME, H5D_XFER_MODIFY_WRITE_BUF_SIZE,
                           &H5D_def_modify_write_buf_g, NULL, NULL, NULL, H5D_XFER_MODIFY_WRITE_BUF_ENC,
                           H5D_XFER_MODIFY_WRITE_BUF_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_reg_prop() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_bkgr_buf_type_enc
 *
 * Purpose:        Callback routine which is called whenever the background
 *                 buffer type property in the dataset transfer property list
 *                 is encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_bkgr_buf_type_enc(const void *value, void **_pp, size_t *size)
{
    const H5T_bkg_t *bkgr_buf_type = (const H5T_bkg_t *)value; /* Create local alias for values */
    uint8_t        **pp            = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(bkgr_buf_type);
    assert(size);

    if (NULL != *pp)
        /* Encode background buffer type */
        *(*pp)++ = (uint8_t)*bkgr_buf_type;

    /* Size of background buffer type */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_bkgr_buf_type_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_bkgr_buf_type_dec
 *
 * Purpose:        Callback routine which is called whenever the background
 *                 buffer type property in the dataset transfer property list
 *                 is decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_bkgr_buf_type_dec(const void **_pp, void *_value)
{
    H5T_bkg_t      *bkgr_buf_type = (H5T_bkg_t *)_value; /* Background buffer type */
    const uint8_t **pp            = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(bkgr_buf_type);

    /* Decode background buffer type */
    *bkgr_buf_type = (H5T_bkg_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_bkgr_buf_type_dec() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_btree_split_ratio_enc
 *
 * Purpose:        Callback routine which is called whenever the B-tree split
 *                 ratio property in the dataset transfer property list
 *                 is encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_btree_split_ratio_enc(const void *value, void **_pp, size_t *size)
{
    const double *btree_split_ratio = (const double *)value; /* Create local alias for values */
    uint8_t     **pp                = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(btree_split_ratio);
    assert(size);

    if (NULL != *pp) {
        /* Encode the size of a double*/
        *(*pp)++ = (uint8_t)sizeof(double);

        /* Encode the left split value */
        H5_ENCODE_DOUBLE(*pp, *(const double *)btree_split_ratio);
        btree_split_ratio++;

        /* Encode the middle split value */
        H5_ENCODE_DOUBLE(*pp, *(const double *)btree_split_ratio);
        btree_split_ratio++;

        /* Encode the right split value */
        H5_ENCODE_DOUBLE(*pp, *(const double *)btree_split_ratio);
    } /* end if */

    /* Size of B-tree split ratio values */
    *size += 1 + (3 * sizeof(double));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_btree_split_ratio_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_btree_split_ratio_dec
 *
 * Purpose:        Callback routine which is called whenever the B-tree split
 *                 ratio property in the dataset transfer property list
 *                 is decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_btree_split_ratio_dec(const void **_pp, void *_value)
{
    double         *btree_split_ratio = (double *)_value; /* B-tree split ratio */
    unsigned        enc_size;                             /* Size of encoded property */
    const uint8_t **pp        = (const uint8_t **)_pp;
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(btree_split_ratio);

    /* Decode the size */
    enc_size = *(*pp)++;
    if (enc_size != sizeof(double))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "double value can't be decoded");

    /* Decode the left, middle & left B-tree split ratios */
    H5_DECODE_DOUBLE(*pp, btree_split_ratio[0]);
    H5_DECODE_DOUBLE(*pp, btree_split_ratio[1]);
    H5_DECODE_DOUBLE(*pp, btree_split_ratio[2]);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_btree_split_ratio_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_xform_set
 *
 * Purpose:     Copies a data transform property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_xform_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size,
                    void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of data transform */
    if (H5Z_xform_copy((H5Z_data_xform_t **)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "error copying the data transform info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_xform_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_xform_get
 *
 * Purpose:     Copies a data transform property when it's retrieved for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_xform_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size,
                    void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of data transform */
    if (H5Z_xform_copy((H5Z_data_xform_t **)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "error copying the data transform info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_xform_get() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_xform_enc
 *
 * Purpose:        Callback routine which is called whenever the data transform
 *                 property in the dataset transfer property list
 *                 is encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_xform_enc(const void *value, void **_pp, size_t *size)
{
    const H5Z_data_xform_t *data_xform_prop =
        *(const H5Z_data_xform_t *const *)value; /* Create local alias for values */
    const char *pexp      = NULL;                /* Pointer to transform expression */
    size_t      len       = 0;                   /* Length of transform expression */
    uint8_t   **pp        = (uint8_t **)_pp;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    assert(size);

    /* Check for data transform set */
    if (NULL != data_xform_prop) {
        /* Get the transform expression */
        if (NULL == (pexp = H5Z_xform_extract_xform_str(data_xform_prop)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "failed to retrieve transform expression");

        /* Get the transform string expression size */
        len = strlen(pexp) + 1;
    } /* end if */

    if (NULL != *pp) {
        uint64_t enc_value;
        unsigned enc_size;

        /* encode the length of the prefix */
        enc_value = (uint64_t)len;
        enc_size  = H5VM_limit_enc_size(enc_value);
        assert(enc_size < 256);
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        if (NULL != data_xform_prop) {
            /* Sanity check */
            assert(pexp);

            /* Copy the expression into the buffer */
            H5MM_memcpy(*pp, (const uint8_t *)pexp, len);
            *pp += len;
            *pp[0] = '\0';
        } /* end if */
    }     /* end if */

    /* Size of encoded data transform */
    *size += (1 + H5VM_limit_enc_size((uint64_t)len));
    if (NULL != pexp)
        *size += len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_xform_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_xform_dec
 *
 * Purpose:        Callback routine which is called whenever the data transform
 *                 property in the dataset transfer property list
 *                 is decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_xform_dec(const void **_pp, void *_value)
{
    H5Z_data_xform_t **data_xform_prop = (H5Z_data_xform_t **)_value; /* New data xform property */
    size_t             len;                                           /* Length of encoded string */
    const uint8_t    **pp = (const uint8_t **)_pp;
    unsigned           enc_size;
    uint64_t           enc_value;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(data_xform_prop);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* Decode the length of xform expression */
    enc_size = *(*pp)++;
    assert(enc_size < 256);
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    len = (size_t)enc_value;

    if (0 != len) {
        if (NULL == (*data_xform_prop = H5Z_xform_create((const char *)*pp)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, FAIL, "unable to create data transform info");
        *pp += len;
    } /* end if */
    else
        *data_xform_prop = H5D_XFER_XFORM_DEF;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_xform_dec() */

/*-------------------------------------------------------------------------
 * Function: H5P__dxfr_xform_del
 *
 * Purpose: Frees memory allocated by H5P_dxfr_xform_set
 *
 * Return: Success: SUCCEED, Failure: FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_xform_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size,
                    void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(value);

    if (H5Z_xform_destroy(*(H5Z_data_xform_t **)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCLOSEOBJ, FAIL, "error closing the parse tree");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_xform_del() */

/*-------------------------------------------------------------------------
 * Function: H5P__dxfr_xform_copy
 *
 * Purpose: Creates a copy of the user's data transform string and its
 *              associated parse tree.
 *
 * Return: Success: SUCCEED, Failure: FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_xform_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of data transform */
    if (H5Z_xform_copy((H5Z_data_xform_t **)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "error copying the data transform info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_xform_copy() */

/*-------------------------------------------------------------------------
 * Function: H5P__dxfr_xform_cmp
 *
 * Purpose: Compare two data transforms.
 *
 * Return: positive if VALUE1 is greater than VALUE2, negative if VALUE2 is
 *		greater than VALUE1 and zero if VALUE1 and VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__dxfr_xform_cmp(const void *_xform1, const void *_xform2, size_t H5_ATTR_UNUSED size)
{
    const H5Z_data_xform_t *const *xform1 =
        (const H5Z_data_xform_t *const *)_xform1; /* Create local aliases for values */
    const H5Z_data_xform_t *const *xform2 =
        (const H5Z_data_xform_t *const *)_xform2; /* Create local aliases for values */
    const char *pexp1, *pexp2;                    /* Pointers to transform expressions */
    herr_t      ret_value = 0;                    /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(xform1);
    assert(xform2);
    assert(size == sizeof(H5Z_data_xform_t *));

    /* Check for a property being set */
    if (*xform1 == NULL && *xform2 != NULL)
        HGOTO_DONE(-1);
    if (*xform1 != NULL && *xform2 == NULL)
        HGOTO_DONE(1);

    if (*xform1) {
        assert(*xform2);

        /* Get the transform expressions */
        pexp1 = H5Z_xform_extract_xform_str(*xform1);
        pexp2 = H5Z_xform_extract_xform_str(*xform2);

        /* Check for property expressions */
        if (pexp1 == NULL && pexp2 != NULL)
            HGOTO_DONE(-1);
        if (pexp1 != NULL && pexp2 == NULL)
            HGOTO_DONE(1);

        if (pexp1) {
            assert(pexp2);
            ret_value = strcmp(pexp1, pexp2);
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_xform_cmp() */

/*-------------------------------------------------------------------------
 * Function: H5P__dxfr_xform_close
 *
 * Purpose: Frees memory allocated by H5P_dxfr_xform_set
 *
 * Return: Success: SUCCEED, Failure: FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_xform_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(value);

    if (H5Z_xform_destroy(*(H5Z_data_xform_t **)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCLOSEOBJ, FAIL, "error closing the parse tree");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_xform_close() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_data_transform
 *
 * Purpose:	Sets data transform expression.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_data_transform(hid_t plist_id, const char *expression)
{
    H5P_genplist_t   *plist;                     /* Property list pointer */
    H5Z_data_xform_t *data_xform_prop = NULL;    /* New data xform property */
    herr_t            ret_value       = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", plist_id, expression);

    /* Check arguments */
    if (expression == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "expression cannot be NULL");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* See if a data transform is already set, and free it if it is */
    if (H5P_peek(plist, H5D_XFER_XFORM_NAME, &data_xform_prop) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "error getting data transform expression");

    /* Destroy previous data transform property */
    if (H5Z_xform_destroy(data_xform_prop) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "unable to release data transform expression");

    /* Create data transform info from expression */
    if (NULL == (data_xform_prop = H5Z_xform_create(expression)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOSPACE, FAIL, "unable to create data transform info");

    /* Update property list (takes ownership of transform) */
    if (H5P_poke(plist, H5D_XFER_XFORM_NAME, &data_xform_prop) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "Error setting data transform expression");

done:
    if (ret_value < 0)
        if (data_xform_prop && H5Z_xform_destroy(data_xform_prop) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "unable to release data transform expression");

    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_data_transform() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_data_transform
 *
 * Purpose:	Gets data transform expression.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 * Comments:
 *  If `expression' is non-NULL then write up to `size' bytes into that
 *  buffer and always return the length of the transform name.
 *  Otherwise `size' is ignored and the function does not store the expression,
 *  just returning the number of characters required to store the expression.
 *  If an error occurs then the buffer pointed to by `expression' (NULL or non-NULL)
 *  is unchanged and the function returns a negative value.
 *  If a zero is returned for the name's length, then there is no name
 *  associated with the ID.
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Pget_data_transform(hid_t plist_id, char *expression /*out*/, size_t size)
{
    H5P_genplist_t   *plist;                  /* Property list pointer */
    H5Z_data_xform_t *data_xform_prop = NULL; /* New data xform property */
    size_t            len;
    const char       *pexp;
    ssize_t           ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("Zs", "ixz", plist_id, expression, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (H5P_peek(plist, H5D_XFER_XFORM_NAME, &data_xform_prop) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "error getting data transform expression");

    if (NULL == data_xform_prop)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "data transform has not been set");

    /* Get the data transform string */
    if (NULL == (pexp = H5Z_xform_extract_xform_str(data_xform_prop)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "failed to retrieve transform expression");

    /* Copy into application buffer */
    len = strlen(pexp);
    if (expression) {
        strncpy(expression, pexp, size);
        if (len >= size)
            expression[size - 1] = '\0';
    } /* end if */

    ret_value = (ssize_t)len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_data_transform() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_buffer
 *
 * Purpose:	Given a dataset transfer property list, set the maximum size
 *		for the type conversion buffer and background buffer and
 *		optionally supply pointers to application-allocated buffers.
 *		If the buffer size is smaller than the entire amount of data
 *		being transferred between application and file, and a type
 *		conversion buffer or background buffer is required then
 *		strip mining will be used.
 *
 *		If TCONV and/or BKG are null pointers then buffers will be
 *		allocated and freed during the data transfer.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_buffer(hid_t plist_id, size_t size, void *tconv, void *bkg)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iz*x*x", plist_id, size, tconv, bkg);

    /* Check arguments */
    if (size == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buffer size must not be zero");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Update property list */
    if (H5P_set(plist, H5D_XFER_MAX_TEMP_BUF_NAME, &size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "Can't set transfer buffer size");
    if (H5P_set(plist, H5D_XFER_TCONV_BUF_NAME, &tconv) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "Can't set transfer type conversion buffer");
    if (H5P_set(plist, H5D_XFER_BKGR_BUF_NAME, &bkg) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "Can't set background type conversion buffer");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_buffer() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_buffer
 *
 * Purpose:	Reads values previously set with H5Pset_buffer().
 *
 * Return:	Success:	Buffer size.
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
size_t
H5Pget_buffer(hid_t plist_id, void **tconv /*out*/, void **bkg /*out*/)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    size_t          size;      /* Type conversion buffer size */
    size_t          ret_value; /* Return value */

    FUNC_ENTER_API(0)
    H5TRACE3("z", "ixx", plist_id, tconv, bkg);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, 0, "can't find object for ID");

    /* Return values */
    if (tconv)
        if (H5P_get(plist, H5D_XFER_TCONV_BUF_NAME, tconv) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, 0, "Can't get transfer type conversion buffer");
    if (bkg)
        if (H5P_get(plist, H5D_XFER_BKGR_BUF_NAME, bkg) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, 0, "Can't get background type conversion buffer");

    /* Get the size */
    if (H5P_get(plist, H5D_XFER_MAX_TEMP_BUF_NAME, &size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, 0, "Can't set transfer buffer size");

    /* Set the return value */
    ret_value = size;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_buffer() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_preserve
 *
 * Purpose:	When reading or writing compound data types and the
 *		destination is partially initialized and the read/write is
 *		intended to initialize the other members, one must set this
 *		property to true.  Otherwise the I/O pipeline treats the
 *		destination datapoints as completely uninitialized.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_preserve(hid_t plist_id, hbool_t status)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5T_bkg_t       need_bkg;            /* Value for background buffer type */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ib", plist_id, status);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Update property list */
    need_bkg = status ? H5T_BKG_YES : H5T_BKG_NO;
    if (H5P_set(plist, H5D_XFER_BKGR_BUF_TYPE_NAME, &need_bkg) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_preserve() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_preserve
 *
 * Purpose:	The inverse of H5Pset_preserve()
 *
 * Return:	Success:	true or false
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5Pget_preserve(hid_t plist_id)
{
    H5T_bkg_t       need_bkg;  /* Background value */
    H5P_genplist_t *plist;     /* Property list pointer */
    int             ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("Is", "i", plist_id);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get value */
    if (H5P_get(plist, H5D_XFER_BKGR_BUF_TYPE_NAME, &need_bkg) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

    /* Set return value */
    ret_value = need_bkg ? true : false;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_preserve() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_edc_check
 *
 * Purpose:     Enable or disable error-detecting for a dataset reading
 *              process.  This error-detecting algorithm is whichever
 *              user chooses earlier.  This function cannot control
 *              writing process.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_edc_check(hid_t plist_id, H5Z_EDC_t check)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iZe", plist_id, check);

    /* Check argument */
    if (check != H5Z_ENABLE_EDC && check != H5Z_DISABLE_EDC)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid value");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Update property list */
    if (H5P_set(plist, H5D_XFER_EDC_NAME, &check) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_edc_check() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_edc_check
 *
 * Purpose:     Enable or disable error-detecting for a dataset reading
 *              process.  This error-detecting algorithm is whichever
 *              user chooses earlier.  This function cannot control
 *              writing process.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5Z_EDC_t
H5Pget_edc_check(hid_t plist_id)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    H5Z_EDC_t       ret_value; /* Return value */

    FUNC_ENTER_API(H5Z_ERROR_EDC)
    H5TRACE1("Ze", "i", plist_id);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, H5Z_ERROR_EDC, "can't find object for ID");

    /* Update property list */
    if (H5P_get(plist, H5D_XFER_EDC_NAME, &ret_value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, H5Z_ERROR_EDC, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_edc_check() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_filter_callback
 *
 * Purpose:     Sets user's callback function for dataset transfer property
 *              list.  This callback function defines what user wants to do
 *              if certain filter fails.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_filter_callback(hid_t plist_id, H5Z_filter_func_t func, void *op_data)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */
    H5Z_cb_t        cb_struct;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iZF*x", plist_id, func, op_data);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Update property list */
    cb_struct.func    = func;
    cb_struct.op_data = op_data;

    if (H5P_set(plist, H5D_XFER_FILTER_CB_NAME, &cb_struct) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Pset_type_conv_cb
 *
 * Purpose:     Sets user's callback function for dataset transfer property
 *              list.  This callback function defines what user wants to do
 *              if there's exception during datatype conversion.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_type_conv_cb(hid_t plist_id, H5T_conv_except_func_t op, void *operate_data)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */
    H5T_conv_cb_t   cb_struct;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iTE*x", plist_id, op, operate_data);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Update property list */
    cb_struct.func      = op;
    cb_struct.user_data = operate_data;

    if (H5P_set(plist, H5D_XFER_CONV_CB_NAME, &cb_struct) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Pget_type_conv_cb
 *
 * Purpose:     Gets callback function for dataset transfer property
 *              list.  This callback function defines what user wants to do
 *              if there's exception during datatype conversion.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_type_conv_cb(hid_t plist_id, H5T_conv_except_func_t *op /*out*/, void **operate_data /*out*/)
{
    H5P_genplist_t *plist; /* Property list pointer */
    H5T_conv_cb_t   cb_struct;
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", plist_id, op, operate_data);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get property */
    if (H5P_get(plist, H5D_XFER_CONV_CB_NAME, &cb_struct) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

    /* Assign return value */
    *op           = cb_struct.func;
    *operate_data = cb_struct.user_data;

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Pget_btree_ratios
 *
 * Purpose:	Queries B-tree split ratios.  See H5Pset_btree_ratios().
 *
 * Return:	Success:	Non-negative with split ratios returned through
 *				the non-null arguments.
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_btree_ratios(hid_t plist_id, double *left /*out*/, double *middle /*out*/, double *right /*out*/)
{
    H5P_genplist_t *plist;                /* Property list pointer */
    double          btree_split_ratio[3]; /* B-tree node split ratios */
    herr_t          ret_value = SUCCEED;  /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "ixxx", plist_id, left, middle, right);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the split ratios */
    if (H5P_get(plist, H5D_XFER_BTREE_SPLIT_RATIO_NAME, &btree_split_ratio) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

    /* Get values */
    if (left)
        *left = btree_split_ratio[0];
    if (middle)
        *middle = btree_split_ratio[1];
    if (right)
        *right = btree_split_ratio[2];

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_btree_ratios() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_btree_ratios
 *
 * Purpose:	Sets B-tree split ratios for a dataset transfer property
 *		list. The split ratios determine what percent of children go
 *		in the first node when a node splits.  The LEFT ratio is
 *		used when the splitting node is the left-most node at its
 *		level in the tree; the RIGHT ratio is when the splitting node
 *		is the right-most node at its level; and the MIDDLE ratio for
 *		all other cases.  A node which is the only node at its level
 *		in the tree uses the RIGHT ratio when it splits.  All ratios
 *		are real numbers between 0 and 1, inclusive.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_btree_ratios(hid_t plist_id, double left, double middle, double right)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    double          split_ratio[3];      /* B-tree node split ratios */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iddd", plist_id, left, middle, right);

    /* Check arguments */
    if (left < 0.0 || left > 1.0 || middle < 0.0 || middle > 1.0 || right < 0.0 || right > 1.0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "split ratio must satisfy 0.0 <= X <= 1.0");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    split_ratio[0] = left;
    split_ratio[1] = middle;
    split_ratio[2] = right;

    /* Set the split ratios */
    if (H5P_set(plist, H5D_XFER_BTREE_SPLIT_RATIO_NAME, &split_ratio) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_btree_ratios() */

/*-------------------------------------------------------------------------
 * Function:	H5P_set_vlen_mem_manager
 *
 * Purpose:	Sets the memory allocate/free pair for VL datatypes.  The
 *		allocation routine is called when data is read into a new
 *		array and the free routine is called when H5Treclaim is
 *		called.  The alloc_info and free_info are user parameters
 *		which are passed to the allocation and freeing functions
 *		respectively.  To reset the allocate/free functions to the
 *		default setting of using the system's malloc/free functions,
 *		call this routine with alloc_func and free_func set to NULL.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_set_vlen_mem_manager(H5P_genplist_t *plist, H5MM_allocate_t alloc_func, void *alloc_info,
                         H5MM_free_t free_func, void *free_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(plist);

    /* Update property list */
    if (H5P_set(plist, H5D_XFER_VLEN_ALLOC_NAME, &alloc_func) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");
    if (H5P_set(plist, H5D_XFER_VLEN_ALLOC_INFO_NAME, &alloc_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");
    if (H5P_set(plist, H5D_XFER_VLEN_FREE_NAME, &free_func) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");
    if (H5P_set(plist, H5D_XFER_VLEN_FREE_INFO_NAME, &free_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_set_vlen_mem_manager() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_vlen_mem_manager
 *
 * Purpose:	Sets the memory allocate/free pair for VL datatypes.  The
 *		allocation routine is called when data is read into a new
 *		array and the free routine is called when H5Treclaim is
 *		called.  The alloc_info and free_info are user parameters
 *		which are passed to the allocation and freeing functions
 *		respectively.  To reset the allocate/free functions to the
 *		default setting of using the system's malloc/free functions,
 *		call this routine with alloc_func and free_func set to NULL.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_vlen_mem_manager(hid_t plist_id, H5MM_allocate_t alloc_func, void *alloc_info, H5MM_free_t free_func,
                        void *free_info)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "iMa*xMf*x", plist_id, alloc_func, alloc_info, free_func, free_info);

    /* Check arguments */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset transfer property list");

    /* Update property list */
    if (H5P_set_vlen_mem_manager(plist, alloc_func, alloc_info, free_func, free_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set values");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_vlen_mem_manager() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_vlen_mem_manager
 *
 * Purpose:	The inverse of H5Pset_vlen_mem_manager()
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_vlen_mem_manager(hid_t plist_id, H5MM_allocate_t *alloc_func /*out*/, void **alloc_info /*out*/,
                        H5MM_free_t *free_func /*out*/, void **free_info /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "ixxxx", plist_id, alloc_func, alloc_info, free_func, free_info);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (alloc_func)
        if (H5P_get(plist, H5D_XFER_VLEN_ALLOC_NAME, alloc_func) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");
    if (alloc_info)
        if (H5P_get(plist, H5D_XFER_VLEN_ALLOC_INFO_NAME, alloc_info) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");
    if (free_func)
        if (H5P_get(plist, H5D_XFER_VLEN_FREE_NAME, free_func) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");
    if (free_info)
        if (H5P_get(plist, H5D_XFER_VLEN_FREE_INFO_NAME, free_info) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_vlen_mem_manager() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_hyper_vector_size
 *
 * Purpose:	Given a dataset transfer property list, set the number of
 *              "I/O vectors" (offset and length pairs) which are to be
 *              accumulated in memory before being issued to the lower levels
 *              of the library for reading or writing the actual data.
 *              Increasing the number should give better performance, but use
 *              more memory during hyperslab I/O.  The vector size must be
 *              greater than 1.
 *
 *		The default is to use 1024 vectors for I/O during hyperslab
 *              reading/writing.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_hyper_vector_size(hid_t plist_id, size_t vector_size)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iz", plist_id, vector_size);

    /* Check arguments */
    if (vector_size < 1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "vector size too small");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Update property list */
    if (H5P_set(plist, H5D_XFER_HYPER_VECTOR_SIZE_NAME, &vector_size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_hyper_vector_size() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_hyper_vector_size
 *
 * Purpose:	Reads values previously set with H5Pset_hyper_vector_size().
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_hyper_vector_size(hid_t plist_id, size_t *vector_size /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, vector_size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Return values */
    if (vector_size)
        if (H5P_get(plist, H5D_XFER_HYPER_VECTOR_SIZE_NAME, vector_size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_hyper_vector_size() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_io_xfer_mode_enc
 *
 * Purpose:        Callback routine which is called whenever the I/O transfer
 *                 mode property in the dataset transfer property list
 *                 is encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_io_xfer_mode_enc(const void *value, void **_pp, size_t *size)
{
    const H5FD_mpio_xfer_t *xfer_mode = (const H5FD_mpio_xfer_t *)value; /* Create local alias for values */
    uint8_t               **pp        = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(xfer_mode);
    assert(size);

    if (NULL != *pp)
        /* Encode I/O transfer mode */
        *(*pp)++ = (uint8_t)*xfer_mode;

    /* Size of I/O transfer mode */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_io_xfer_mode_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_io_xfer_mode_dec
 *
 * Purpose:        Callback routine which is called whenever the I/O transfer
 *                 mode property in the dataset transfer property list
 *                 is decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_io_xfer_mode_dec(const void **_pp, void *_value)
{
    H5FD_mpio_xfer_t *xfer_mode = (H5FD_mpio_xfer_t *)_value; /* I/O transfer mode */
    const uint8_t   **pp        = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(xfer_mode);

    /* Decode I/O transfer mode */
    *xfer_mode = (H5FD_mpio_xfer_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_io_xfer_mode_dec() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_mpio_collective_opt_enc
 *
 * Purpose:        Callback routine which is called whenever the MPI-I/O
 *                 collective optimization property in the dataset transfer
 *		   property list is encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_mpio_collective_opt_enc(const void *value, void **_pp, size_t *size)
{
    const H5FD_mpio_collective_opt_t *coll_opt =
        (const H5FD_mpio_collective_opt_t *)value; /* Create local alias for values */
    uint8_t **pp = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(coll_opt);
    assert(size);

    if (NULL != *pp)
        /* Encode MPI-I/O collective optimization property */
        *(*pp)++ = (uint8_t)*coll_opt;

    /* Size of MPI-I/O collective optimization property */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_mpio_collective_opt_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_mpio_collective_opt_dec
 *
 * Purpose:        Callback routine which is called whenever the MPI-I/O
 *                 collective optimization property in the dataset transfer
 *		   property list is decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_mpio_collective_opt_dec(const void **_pp, void *_value)
{
    H5FD_mpio_collective_opt_t *coll_opt =
        (H5FD_mpio_collective_opt_t *)_value; /* MPI-I/O collective optimization mode */
    const uint8_t **pp = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(coll_opt);

    /* Decode MPI-I/O collective optimization mode */
    *coll_opt = (H5FD_mpio_collective_opt_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_mpio_collective_opt_dec() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_mpio_chunk_opt_hard_enc
 *
 * Purpose:        Callback routine which is called whenever the MPI-I/O
 *                 chunk optimization property in the dataset transfer
 *		   property list is encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_mpio_chunk_opt_hard_enc(const void *value, void **_pp, size_t *size)
{
    const H5FD_mpio_chunk_opt_t *chunk_opt =
        (const H5FD_mpio_chunk_opt_t *)value; /* Create local alias for values */
    uint8_t **pp = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(chunk_opt);
    assert(size);

    if (NULL != *pp)
        /* Encode MPI-I/O chunk optimization property */
        *(*pp)++ = (uint8_t)*chunk_opt;

    /* Size of MPI-I/O chunk optimization property */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_mpio_chunk_opt_hard_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_mpio_chunk_opt_hard_enc
 *
 * Purpose:        Callback routine which is called whenever the MPI-I/O
 *                 chunk collective optimization property in the dataset transfer
 *		   property list is decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_mpio_chunk_opt_hard_dec(const void **_pp, void *_value)
{
    H5FD_mpio_chunk_opt_t *chunk_opt = (H5FD_mpio_chunk_opt_t *)_value; /* MPI-I/O chunk optimization mode */
    const uint8_t        **pp        = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(chunk_opt);

    /* Decode MPI-I/O chunk optimization mode */
    *chunk_opt = (H5FD_mpio_chunk_opt_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_mpio_chunk_opt_hard_dec() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:	H5Pget_mpio_actual_chunk_opt_mode
 *
 * Purpose:	Retrieves the chunked io optimization scheme that library chose
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_mpio_actual_chunk_opt_mode(hid_t                             plist_id,
                                  H5D_mpio_actual_chunk_opt_mode_t *actual_chunk_opt_mode /*out*/)
{
    H5P_genplist_t *plist;
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, actual_chunk_opt_mode);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Return values */
    if (actual_chunk_opt_mode)
        if (H5P_get(plist, H5D_MPIO_ACTUAL_CHUNK_OPT_MODE_NAME, actual_chunk_opt_mode) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_mpio_actual_chunk_opt_mode() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_mpio_actual_io_mode
 *
 * Purpose:	Retrieves the type of I/O actually performed when collective I/O
 *		is requested.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_mpio_actual_io_mode(hid_t plist_id, H5D_mpio_actual_io_mode_t *actual_io_mode /*out*/)
{
    H5P_genplist_t *plist;
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, actual_io_mode);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Return values */
    if (actual_io_mode)
        if (H5P_get(plist, H5D_MPIO_ACTUAL_IO_MODE_NAME, actual_io_mode) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_mpio_actual_io_mode() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_mpio_no_collective_cause
 *
 * Purpose:	Retrieves cause for the broke collective I/O
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_mpio_no_collective_cause(hid_t plist_id, uint32_t *local_no_collective_cause /*out*/,
                                uint32_t *global_no_collective_cause /*out*/)
{
    H5P_genplist_t *plist;
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", plist_id, local_no_collective_cause, global_no_collective_cause);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Return values */
    if (local_no_collective_cause)
        if (H5P_get(plist, H5D_MPIO_LOCAL_NO_COLLECTIVE_CAUSE_NAME, local_no_collective_cause) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get local value");
    if (global_no_collective_cause)
        if (H5P_get(plist, H5D_MPIO_GLOBAL_NO_COLLECTIVE_CAUSE_NAME, global_no_collective_cause) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get global value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_mpio_no_collective_cause() */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_edc_enc
 *
 * Purpose:        Callback routine which is called whenever the error detect
 *                 property in the dataset transfer property list
 *                 is encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_edc_enc(const void *value, void **_pp, size_t *size)
{
    const H5Z_EDC_t *check = (const H5Z_EDC_t *)value; /* Create local alias for values */
    uint8_t        **pp    = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(check);
    assert(size);

    if (NULL != *pp)
        /* Encode EDC property */
        *(*pp)++ = (uint8_t)*check;

    /* Size of EDC property */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_edc_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dxfr_edc_dec
 *
 * Purpose:        Callback routine which is called whenever the error detect
 *                 property in the dataset transfer property list
 *                 is decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_edc_dec(const void **_pp, void *_value)
{
    H5Z_EDC_t      *check = (H5Z_EDC_t *)_value; /* EDC property */
    const uint8_t **pp    = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(check);

    /* Decode EDC property */
    *check = (H5Z_EDC_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_edc_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_dset_io_hyp_sel_copy
 *
 * Purpose:     Creates a copy of the dataset I/O selection.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_dset_io_hyp_sel_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    H5S_t *orig_space = *(H5S_t **)value; /* Original dataspace for property */
    H5S_t *new_space  = NULL;             /* New dataspace for property */
    herr_t ret_value  = SUCCEED;          /* Return value */

    FUNC_ENTER_PACKAGE

    /* If there's a dataspace I/O selection set, copy it */
    if (orig_space) {
        /* Make copy of dataspace */
        if (NULL == (new_space = H5S_copy(orig_space, false, true)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "error copying the dataset I/O selection");

        /* Set new value for property */
        *(void **)value = new_space;
    } /* end if */

done:
    /* Cleanup on error */
    if (ret_value < 0)
        if (new_space && H5S_close(new_space) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCLOSEOBJ, FAIL, "error closing dataset I/O selection dataspace");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_dset_io_hyp_sel_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_dset_io_hyp_sel_cmp
 *
 * Purpose:     Compare two dataset I/O selections.
 *
 * Return:      positive if VALUE1 is greater than VALUE2, negative if VALUE2 is
 *		greater than VALUE1 and zero if VALUE1 and VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__dxfr_dset_io_hyp_sel_cmp(const void *_space1, const void *_space2, size_t H5_ATTR_UNUSED size)
{
    const H5S_t *const *space1    = (const H5S_t *const *)_space1; /* Create local aliases for values */
    const H5S_t *const *space2    = (const H5S_t *const *)_space2; /* Create local aliases for values */
    herr_t              ret_value = 0;                             /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(space1);
    assert(space1);
    assert(size == sizeof(H5S_t *));

    /* Check for a property being set */
    if (*space1 == NULL && *space2 != NULL)
        HGOTO_DONE(-1);
    if (*space1 != NULL && *space2 == NULL)
        HGOTO_DONE(1);

    if (*space1) {
        assert(*space2);

        /* Compare the extents of the dataspaces */
        /* (Error & not-equal count the same) */
        if (true != H5S_extent_equal(*space1, *space2))
            HGOTO_DONE(-1);

        /* Compare the selection "shape" of the dataspaces
         * (Error & not-equal count the same)
         *
         * Since H5S_select_shape_same() can result in the dataspaces being
         * rebuilt, the parameters are not const which makes it impossible
         * to match the cmp prototype. Since we need to compare them,
         * we quiet the const warning.
         */
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        if (true != H5S_select_shape_same((H5S_t *)*space1, (H5S_t *)*space2))
            HGOTO_DONE(-1);
        H5_GCC_CLANG_DIAG_ON("cast-qual")
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_dset_io_hyp_sel_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_dset_io_hyp_sel_close
 *
 * Purpose:     Frees resources for dataset I/O selection
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_dset_io_hyp_sel_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *_value)
{
    H5S_t *space     = *(H5S_t **)_value; /* Dataspace for property */
    herr_t ret_value = SUCCEED;           /* Return value */

    FUNC_ENTER_PACKAGE

    /* Release any dataspace */
    if (space && H5S_close(space) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCLOSEOBJ, FAIL, "error closing dataset I/O selection dataspace");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dxfr_dset_io_hyp_sel_close() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_selection_io_mode_enc
 *
 * Purpose:     Callback routine which is called whenever the selection
 *              I/O mode property in the dataset transfer property list
 *              is encoded.
 *
 * Return:      Success:	Non-negative
 *		        Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_selection_io_mode_enc(const void *value, void **_pp, size_t *size)
{
    const H5D_selection_io_mode_t *select_io_mode =
        (const H5D_selection_io_mode_t *)value; /* Create local alias for values */
    uint8_t **pp = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(select_io_mode);
    assert(size);

    if (NULL != *pp)
        /* Encode selection I/O mode property */
        *(*pp)++ = (uint8_t)*select_io_mode;

    /* Size of selection I/O mode property */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_selection_io_mode_enc() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_selection_io_mode_dec
 *
 * Purpose:     Callback routine which is called whenever the selection
 *              I/O mode property in the dataset transfer property list
 *              is decoded.
 *
 * Return:      Success:	Non-negative
 *		        Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_selection_io_mode_dec(const void **_pp, void *_value)
{
    H5D_selection_io_mode_t *select_io_mode = (H5D_selection_io_mode_t *)_value; /* Selection I/O mode */
    const uint8_t          **pp             = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(select_io_mode);

    /* Decode selection I/O mode property */
    *select_io_mode = (H5D_selection_io_mode_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_selection_io_dec() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_dataset_io_hyperslab_selection
 *
 * Purpose:	H5Pset_dataset_io_hyperslab_selection() is designed to be used
 *              in conjunction with using H5S_PLIST for the file dataspace
 *              ID when making a call to H5Dread() or H5Dwrite().  When used
 *              with H5S_PLIST, the selection created by one or more calls to
 *              this routine is used for determining which dataset elements to
 *              access.
 *
 *              'rank' is the dimensionality of the selection and determines
 *              the size of the 'start', 'stride', 'count', and 'block' arrays.
 *              'rank' must be between 1 and H5S_MAX_RANK, inclusive.
 *
 *              The 'op', 'start', 'stride', 'count', and 'block' parameters
 *              behave identically to their behavior for H5Sselect_hyperslab(),
 *              please see the documentation for that routine for details about
 *              their use.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_dataset_io_hyperslab_selection(hid_t plist_id, unsigned rank, H5S_seloper_t op, const hsize_t start[],
                                      const hsize_t stride[], const hsize_t count[], const hsize_t block[])
{
    H5P_genplist_t *plist               = NULL;    /* Property list pointer */
    H5S_t          *space               = NULL;    /* Dataspace to hold selection */
    bool            space_created       = false;   /* Whether a new dataspace has been created */
    bool            reset_prop_on_error = false;   /* Whether to reset the property on failure */
    herr_t          ret_value           = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "iIuSs*h*h*h*h", plist_id, rank, op, start, stride, count, block);

    /* Check arguments */
    if (rank < 1 || rank > H5S_MAX_RANK)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid rank value: %u", rank);
    if (!(op > H5S_SELECT_NOOP && op < H5S_SELECT_INVALID))
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
    if (start == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "'count' pointer is NULL");
    if (stride != NULL) {
        unsigned u; /* Local index variable */

        /* Check for 0-sized strides */
        for (u = 0; u < rank; u++)
            if (stride[u] == 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid value - stride[%u]==0", u);
    } /* end if */
    if (count == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "'start' pointer is NULL");
    /* block is allowed to be NULL, and will be assumed to be all '1's when NULL */

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* See if a dataset I/O selection is already set, and free it if it is */
    if (H5P_peek(plist, H5D_XFER_DSET_IO_SEL_NAME, &space) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "error getting dataset I/O selection");

    /* Check for operation on existing dataspace selection */
    if (NULL != space) {
        int sndims; /* Rank of existing dataspace */

        /* Get dimensions from current dataspace for selection */
        if ((sndims = H5S_GET_EXTENT_NDIMS(space)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get selection's dataspace rank");

        /* Check for different # of dimensions */
        if ((unsigned)sndims != rank) {
            /* Set up new dataspace for 'set' operation, otherwise fail */
            if (op == H5S_SELECT_SET) {
                /* Close previous dataspace */
                if (H5S_close(space) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "unable to release dataspace");

                /* Reset 'space' pointer, so it's re-created */
                space = NULL;

                /* Set flag to reset property list on error */
                reset_prop_on_error = true;
            } /* end if */
            else
                HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "different rank for previous and new selections");
        } /* end if */
    }     /* end if */

    /* Check for first time called */
    if (NULL == space) {
        hsize_t  dims[H5S_MAX_RANK]; /* Dimensions for new dataspace */
        unsigned u;                  /* Local index variable */

        /* Initialize dimensions to largest possible actual size */
        for (u = 0; u < rank; u++)
            dims[u] = (H5S_UNLIMITED - 1);

        /* Create dataspace of the correct dimensionality, with maximum dimensions */
        if (NULL == (space = H5S_create_simple(rank, dims, NULL)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, FAIL, "unable to create dataspace for selection");
        space_created = true;
    } /* end if */

    /* Set selection for dataspace */
    if (H5S_select_hyperslab(space, op, start, stride, count, block) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSELECT, FAIL, "can't create selection");

    /* Update property list (takes ownership of dataspace, if new) */
    if (H5P_poke(plist, H5D_XFER_DSET_IO_SEL_NAME, &space) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "error setting dataset I/O selection");
    space_created = false; /* Reset now that property owns the dataspace */

done:
    /* Cleanup on failure */
    if (ret_value < 0) {
        if (reset_prop_on_error && plist && H5P_poke(plist, H5D_XFER_DSET_IO_SEL_NAME, &space) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "error setting dataset I/O selection");
        if (space_created && H5S_close(space) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "unable to release dataspace");
    } /* end if */

    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_dataset_io_hyperslab_selection() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_selection_io
 *
 * Purpose:     To set the selection I/O mode in the dataset
 *              transfer property list.
 *
 * Note:        The library may not perform selection I/O as it asks for if
 *              the layout callback determines that it is not feasible to do so.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_selection_io(hid_t plist_id, H5D_selection_io_mode_t selection_io_mode)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iDC", plist_id, selection_io_mode);

    /* Check arguments */
    if (plist_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");

    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");

    /* Set the selection I/O mode */
    if (H5P_set(plist, H5D_XFER_SELECTION_IO_MODE_NAME, &selection_io_mode) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_selection_io() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_selection_io
 *
 * Purpose:     To retrieve the selection I/O mode that is set in
 *              the dataset transfer property list.
 *
 * Note:        The library may not perform selection I/O as it asks for if
 *              the layout callback determines that it is not feasible to do so.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_selection_io(hid_t plist_id, H5D_selection_io_mode_t *selection_io_mode /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, selection_io_mode);

    /* Check arguments */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");

    /* Get the selection I/O mode */
    if (selection_io_mode)
        if (H5P_get(plist, H5D_XFER_SELECTION_IO_MODE_NAME, selection_io_mode) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_selection_io() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_no_selection_io_cause
 *
 * Purpose:	    Retrieves causes for not performing selection I/O
 *
 * Return:	    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_no_selection_io_cause(hid_t plist_id, uint32_t *no_selection_io_cause /*out*/)
{
    H5P_genplist_t *plist;
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, no_selection_io_cause);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Return values */
    if (no_selection_io_cause)
        if (H5P_get(plist, H5D_XFER_NO_SELECTION_IO_CAUSE_NAME, no_selection_io_cause) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get no_selection_io_cause value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_no_selection_io_cause() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_actual_selection_io_mode
 *
 * Purpose:	    Retrieves actual selection I/O mode
 *
 * Return:	    Non-negative on success/Negative on failure
 *
 * Programmer:	Vailin Choi
 *              April 27, 2023
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_actual_selection_io_mode(hid_t plist_id, uint32_t *actual_selection_io_mode /*out*/)
{
    H5P_genplist_t *plist;
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, actual_selection_io_mode);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Return values */
    if (actual_selection_io_mode)
        if (H5P_get(plist, H5D_XFER_ACTUAL_SELECTION_IO_MODE_NAME, actual_selection_io_mode) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get actual_selection_io_mode value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_actual_selection_io_mode() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_modify_write_buf_enc
 *
 * Purpose:     Callback routine which is called whenever the modify write
 *              buffer property in the dataset transfer property list is
 *              encoded.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_modify_write_buf_enc(const void *value, void **_pp /*out*/, size_t *size /*out*/)
{
    const bool *modify_write_buf = (const bool *)value; /* Create local alias for values */
    uint8_t   **pp               = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(modify_write_buf);
    assert(size);

    if (NULL != *pp)
        /* Encode modify write buf property.  Use "!!" so we always get 0 or 1 */
        *(*pp)++ = (uint8_t)(!!(*modify_write_buf));

    /* Size of modify write buf property */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_modify_write_buf_enc() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dxfr_modify_write_buf_dec
 *
 * Purpose:     Callback routine which is called whenever the modify write
 *              buffer property in the dataset transfer property list is
 *              decoded.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dxfr_modify_write_buf_dec(const void **_pp, void *_value /*out*/)
{
    bool           *modify_write_buf = (bool *)_value; /* Modify write buffer */
    const uint8_t **pp               = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(modify_write_buf);

    /* Decode selection I/O mode property */
    *modify_write_buf = (bool)*(*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dxfr_modify_write_buf_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_modify_write_buf
 *
 * Purpose:     Allows the library to modify the contents of the write
 *              buffer
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_modify_write_buf(hid_t plist_id, hbool_t modify_write_buf)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ib", plist_id, modify_write_buf);

    /* Check arguments */
    if (plist_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");

    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");

    /* Set the selection I/O mode */
    if (H5P_set(plist, H5D_XFER_MODIFY_WRITE_BUF_NAME, &modify_write_buf) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_modify_write_buf() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_modify_write_buf
 *
 * Purpose:     Retrieves the "modify write buffer" property
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_modify_write_buf(hid_t plist_id, hbool_t *modify_write_buf /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, modify_write_buf);

    /* Check arguments */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");

    /* Get the selection I/O mode */
    if (modify_write_buf)
        if (H5P_get(plist, H5D_XFER_MODIFY_WRITE_BUF_NAME, modify_write_buf) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_modify_write_buf() */
