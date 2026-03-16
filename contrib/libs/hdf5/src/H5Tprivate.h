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
 * This file contains private information about the H5T module
 */
#ifndef H5Tprivate_H
#define H5Tprivate_H

/* Early typedefs to avoid circular dependencies */
typedef struct H5T_t H5T_t;

/* Include package's public headers */
#include "H5Tpublic.h"
#include "H5Tdevelop.h"

/* Other public headers needed by this file */
#include "H5MMpublic.h" /* Memory management                        */

/* Private headers needed by this file */
#include "H5private.h"   /* Generic Functions                        */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Rprivate.h"  /* References                               */
#include "H5VLprivate.h" /* VOL Drivers                              */

/* Macro for size of temporary buffers to contain a single element */
#define H5T_ELEM_BUF_SIZE 256

/* If the module using this macro is allowed access to the private variables, access them directly */
#ifdef H5T_MODULE
#define H5T_GET_SIZE(T)             ((T)->shared->size)
#define H5T_GET_SHARED(T)           ((T)->shared)
#define H5T_GET_MEMBER_OFFSET(T, I) ((T)->u.compnd.memb[I].offset)
#define H5T_GET_MEMBER_SIZE(T, I)   ((T)->u.compnd.memb[I].shared->size)
#define H5T_GET_FORCE_CONV(T)       ((T)->shared->force_conv)
#else /* H5T_MODULE */
#define H5T_GET_SIZE(T)             (H5T_get_size(T))
#define H5T_GET_SHARED(T)           (H5T_get_shared(T))
#define H5T_GET_MEMBER_OFFSET(T, I) (H5T_get_member_offset((T), (I)))
#define H5T_GET_MEMBER_SIZE(T, I)   (H5T_get_member_size((T), (I)))
#define H5T_GET_FORCE_CONV(T)       (H5T_get_force_conv(T))
#endif /* H5T_MODULE */

/* Forward references of package typedefs (declared in H5Tpkg.h) */
typedef struct H5T_stats_t H5T_stats_t;
typedef struct H5T_path_t  H5T_path_t;

/* Forward reference of H5S_t */
struct H5S_t;

/* How to copy a datatype */
typedef enum H5T_copy_t { H5T_COPY_TRANSIENT, H5T_COPY_ALL } H5T_copy_t;

/* Location of datatype information */
typedef enum {
    H5T_LOC_BADLOC = 0, /* invalid datatype location */
    H5T_LOC_MEMORY,     /* data stored in memory */
    H5T_LOC_DISK,       /* data stored on disk */
    H5T_LOC_MAXLOC      /* highest value (Invalid as true value) */
} H5T_loc_t;

/* VL allocation information */
typedef struct {
    H5MM_allocate_t alloc_func; /* Allocation function */
    void           *alloc_info; /* Allocation information */
    H5MM_free_t     free_func;  /* Free function */
    void           *free_info;  /* Free information */
} H5T_vlen_alloc_info_t;

/* Structure for conversion callback property */
typedef struct H5T_conv_cb_t {
    H5T_conv_except_func_t func;
    void                  *user_data;
} H5T_conv_cb_t;

/* Values for the optimization of compound data reading and writing.  They indicate
 * whether the fields of the source and destination are subset of each other and
 * there is no conversion needed.
 */
typedef enum {
    H5T_SUBSET_BADVALUE = -1, /* Invalid value */
    H5T_SUBSET_FALSE    = 0,  /* Source and destination aren't subset of each other */
    H5T_SUBSET_SRC,           /* Source is the subset of dest and no conversion is needed */
    H5T_SUBSET_DST,           /* Dest is the subset of source and no conversion is needed */
    H5T_SUBSET_CAP            /* Must be the last value */
} H5T_subset_t;

typedef struct H5T_subset_info_t {
    H5T_subset_t subset;    /* See above */
    size_t       copy_size; /* Size in bytes, to copy for each element */
} H5T_subset_info_t;

/* Forward declarations for prototype arguments */
struct H5G_loc_t;
struct H5G_name_t;
struct H5O_shared_t;

/* The native endianness of the platform */
H5_DLLVAR H5T_order_t H5T_native_order_g;

/* Private functions */
H5_DLL herr_t             H5T_init(void);
H5_DLL H5T_t             *H5T_copy(const H5T_t *old_dt, H5T_copy_t method);
H5_DLL H5T_t             *H5T_copy_reopen(H5T_t *old_dt);
H5_DLL herr_t             H5T_lock(H5T_t *dt, bool immutable);
H5_DLL herr_t             H5T_close(H5T_t *dt);
H5_DLL herr_t             H5T_close_real(H5T_t *dt);
H5_DLL H5T_t             *H5T_get_super(const H5T_t *dt);
H5_DLL H5T_class_t        H5T_get_class(const H5T_t *dt, htri_t internal);
H5_DLL htri_t             H5T_detect_class(const H5T_t *dt, H5T_class_t cls, bool from_api);
H5_DLL size_t             H5T_get_size(const H5T_t *dt);
H5_DLL bool               H5T_get_force_conv(const H5T_t *dt);
H5_DLL int                H5T_cmp(const H5T_t *dt1, const H5T_t *dt2, bool superset);
H5_DLL herr_t             H5T_encode(H5T_t *obj, unsigned char *buf, size_t *nalloc);
H5_DLL H5T_t             *H5T_decode(size_t buf_size, const unsigned char *buf);
H5_DLL herr_t             H5T_debug(const H5T_t *dt, FILE *stream);
H5_DLL struct H5O_loc_t  *H5T_oloc(H5T_t *dt);
H5_DLL struct H5G_name_t *H5T_nameof(H5T_t *dt);
H5_DLL htri_t             H5T_is_immutable(const H5T_t *dt);
H5_DLL htri_t             H5T_is_named(const H5T_t *dt);
H5_DLL herr_t             H5T_convert_committed_datatype(H5T_t *dt, H5F_t *f);
H5_DLL htri_t             H5T_is_relocatable(const H5T_t *dt);
H5_DLL H5T_path_t        *H5T_path_find(const H5T_t *src, const H5T_t *dst);
H5_DLL bool               H5T_path_noop(const H5T_path_t *p);
H5_DLL H5T_bkg_t          H5T_path_bkg(const H5T_path_t *p);
H5_DLL H5T_subset_info_t *H5T_path_compound_subset(const H5T_path_t *p);
H5_DLL herr_t H5T_convert(H5T_path_t *tpath, hid_t src_id, hid_t dst_id, size_t nelmts, size_t buf_stride,
                          size_t bkg_stride, void *buf, void *bkg);
H5_DLL herr_t H5T_reclaim(hid_t type_id, struct H5S_t *space, void *buf);
H5_DLL herr_t H5T_reclaim_cb(void *elem, const H5T_t *dt, unsigned ndim, const hsize_t *point, void *op_data);
H5_DLL herr_t H5T_vlen_reclaim_elmt(void *elem, H5T_t *dt);
H5_DLL htri_t H5T_set_loc(H5T_t *dt, H5VL_object_t *file, H5T_loc_t loc);
H5_DLL htri_t H5T_is_sensible(const H5T_t *dt);
H5_DLL uint32_t       H5T_hash(H5F_t *file, const H5T_t *dt);
H5_DLL herr_t         H5T_set_version(H5F_t *f, H5T_t *dt);
H5_DLL herr_t         H5T_patch_file(H5T_t *dt, H5F_t *f);
H5_DLL herr_t         H5T_patch_vlen_file(H5T_t *dt, H5VL_object_t *file);
H5_DLL herr_t         H5T_own_vol_obj(H5T_t *dt, H5VL_object_t *vol_obj);
H5_DLL htri_t         H5T_is_variable_str(const H5T_t *dt);
H5_DLL H5T_t         *H5T_construct_datatype(H5VL_object_t *dt_obj);
H5_DLL H5VL_object_t *H5T_get_named_type(const H5T_t *dt);
H5_DLL H5T_t         *H5T_get_actual_type(H5T_t *dt);
H5_DLL herr_t         H5T_save_refresh_state(hid_t tid, struct H5O_shared_t *cached_H5O_shared);
H5_DLL herr_t         H5T_restore_refresh_state(hid_t tid, struct H5O_shared_t *cached_H5O_shared);
H5_DLL bool           H5T_already_vol_managed(const H5T_t *dt);
H5_DLL htri_t         H5T_is_vl_storage(const H5T_t *dt);
H5_DLL herr_t H5T_invoke_vol_optional(H5T_t *dt, H5VL_optional_args_t *args, hid_t dxpl_id, void **req,
                                      H5VL_object_t **vol_obj_ptr);

/* Reference specific functions */
H5_DLL H5R_type_t H5T_get_ref_type(const H5T_t *dt);

/* Operations on named datatypes */
H5_DLL H5T_t *H5T_open(const struct H5G_loc_t *loc);
H5_DLL int    H5T_link(const H5T_t *type, int adjust);
H5_DLL herr_t H5T_update_shared(H5T_t *type);

/* Field functions (for both compound & enumerated types) */
H5_DLL int    H5T_get_nmembers(const H5T_t *dt);
H5_DLL H5T_t *H5T_get_member_type(const H5T_t *dt, unsigned membno);
H5_DLL size_t H5T_get_member_offset(const H5T_t *dt, unsigned membno);

/* Atomic functions */
H5_DLL H5T_order_t H5T_get_order(const H5T_t *dt);
H5_DLL size_t      H5T_get_precision(const H5T_t *dt);
H5_DLL int         H5T_get_offset(const H5T_t *dt);

/* Fixed-point functions */
H5_DLL H5T_sign_t H5T_get_sign(H5T_t const *dt);

#endif /* H5Tprivate_H */
