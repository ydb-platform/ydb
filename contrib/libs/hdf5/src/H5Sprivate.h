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
 * This file contains private information about the H5S module
 */
#ifndef H5Sprivate_H
#define H5Sprivate_H

/* Include package's public header */
#include "H5Spublic.h"

/* Public headers needed by this file */
#include "H5Dpublic.h" /* Datasets				*/

/* Private headers needed by this file */
#include "H5private.h"  /* Generic Functions			*/
#include "H5Fprivate.h" /* Files				*/
#include "H5Gprivate.h" /* Groups				*/
#include "H5Pprivate.h" /* Property lists			*/
#include "H5Tprivate.h" /* Datatypes				*/

/* Forward references of package typedefs */
typedef struct H5S_extent_t          H5S_extent_t;
typedef struct H5S_pnt_node_t        H5S_pnt_node_t;
typedef struct H5S_pnt_list_t        H5S_pnt_list_t;
typedef struct H5S_hyper_span_t      H5S_hyper_span_t;
typedef struct H5S_hyper_span_info_t H5S_hyper_span_info_t;

/* Information about one dimension in a hyperslab selection */
typedef struct H5S_hyper_dim_t {
    hsize_t start;
    hsize_t stride;
    hsize_t count;
    hsize_t block;
} H5S_hyper_dim_t;

/* Point selection iteration container */
typedef struct {
    H5S_pnt_list_t *pnt_lst; /* Pointer to point list */
    H5S_pnt_node_t *curr;    /* Pointer to next node to output */
} H5S_point_iter_t;

/* Hyperslab selection iteration container */
typedef struct {
    /* Common fields for all hyperslab selections */
    hsize_t  off[H5S_MAX_RANK];  /* Offset in span node (used as position for regular hyperslabs) */
    hsize_t  slab[H5S_MAX_RANK]; /* Cumulative size of each dimension in bytes */
    unsigned iter_rank;          /* Rank of iterator information */
                                 /* (This should always be the same as the dataspace
                                  *  rank, except for regular hyperslab selections in
                                  *  which there are contiguous regions in the lower
                                  *  dimensions that have been "flattened")
                                  */
    bool diminfo_valid;          /* Whether the dimension information is valid */

    /* "Flattened" regular hyperslab selection fields */
    H5S_hyper_dim_t diminfo[H5S_MAX_RANK];   /* "Flattened" regular selection information */
    hsize_t         size[H5S_MAX_RANK];      /* "Flattened" dataspace extent information */
    hssize_t        sel_off[H5S_MAX_RANK];   /* "Flattened" selection offset information */
    bool            flattened[H5S_MAX_RANK]; /* Whether this dimension has been flattened */

    /* Irregular hyperslab selection fields */
    hsize_t loc_off[H5S_MAX_RANK]; /* Byte offset in buffer, for each dimension's current offset */
    H5S_hyper_span_info_t *spans;  /* Pointer to copy of the span tree */
    H5S_hyper_span_t      *span[H5S_MAX_RANK]; /* Array of pointers to span nodes */
} H5S_hyper_iter_t;

/* "All" selection iteration container */
typedef struct {
    hsize_t elmt_offset; /* Next element to output */
    hsize_t byte_offset; /* Next byte to output */
} H5S_all_iter_t;

/* Forward declaration of selection iteration class */
struct H5S_sel_iter_class_t;

/* Selection iteration container */
typedef struct H5S_sel_iter_t {
    /* Selection class */
    const struct H5S_sel_iter_class_t *type; /* Selection iteration class info */

    /* Information common to all iterators */
    unsigned rank;                  /* Rank of dataspace the selection iterator is operating on */
    hsize_t  dims[H5S_MAX_RANK];    /* Dimensions of dataspace the selection is operating on */
    hssize_t sel_off[H5S_MAX_RANK]; /* Selection offset in dataspace */
    hsize_t  elmt_left;             /* Number of elements left to iterate over */
    size_t   elmt_size;             /* Size of elements to iterate over */
    unsigned flags;                 /* Flags controlling iterator behavior */

    /* Information specific to each type of iterator */
    union {
        H5S_point_iter_t pnt; /* Point selection iteration information */
        H5S_hyper_iter_t hyp; /* New Hyperslab selection iteration information */
        H5S_all_iter_t   all; /* "All" selection iteration information */
    } u;
} H5S_sel_iter_t;

/* Selection iteration operator for internal library callbacks */
typedef herr_t (*H5S_sel_iter_lib_op_t)(void *elem, const H5T_t *type, unsigned ndim, const hsize_t *point,
                                        void *op_data);

/* Describe kind of callback to make */
typedef enum H5S_sel_iter_op_type_t {
    H5S_SEL_ITER_OP_APP, /* Application callback */
    H5S_SEL_ITER_OP_LIB  /* Library internal callback */
} H5S_sel_iter_op_type_t;

typedef struct H5S_sel_iter_app_op_t {
    H5D_operator_t op;      /* Callback */
    hid_t          type_id; /* Type ID to be passed to callback */
} H5S_sel_iter_app_op_t;

typedef struct H5S_sel_iter_op_t {
    H5S_sel_iter_op_type_t op_type;
    union {
        H5S_sel_iter_app_op_t app_op; /* Application callback */
        H5S_sel_iter_lib_op_t lib_op; /* Library internal callback */
    } u;
} H5S_sel_iter_op_t;

/* If the module using this macro is allowed access to the private variables, access them directly */
#ifdef H5S_MODULE
#define H5S_GET_EXTENT_TYPE(S)                  ((S)->extent.type)
#define H5S_GET_EXTENT_NDIMS(S)                 ((S)->extent.rank)
#define H5S_GET_EXTENT_NPOINTS(S)               ((S)->extent.nelem)
#define H5S_GET_SELECT_NPOINTS(S)               ((S)->select.num_elem)
#define H5S_GET_SELECT_TYPE(S)                  ((S)->select.type->type)
#define H5S_SELECT_VALID(S)                     ((*(S)->select.type->is_valid)(S))
#define H5S_SELECT_SERIAL_SIZE(S)               ((*(S)->select.type->serial_size)(S))
#define H5S_SELECT_SERIALIZE(S, BUF)            ((*(S)->select.type->serialize)(S, BUF))
#define H5S_SELECT_BOUNDS(S, START, END)        ((*(S)->select.type->bounds)(S, START, END))
#define H5S_SELECT_OFFSET(S, OFFSET)            ((*(S)->select.type->offset)(S, OFFSET))
#define H5S_SELECT_IS_CONTIGUOUS(S)             ((*(S)->select.type->is_contiguous)(S))
#define H5S_SELECT_IS_SINGLE(S)                 ((*(S)->select.type->is_single)(S))
#define H5S_SELECT_IS_REGULAR(S)                ((*(S)->select.type->is_regular)(S))
#define H5S_SELECT_ADJUST_U(S, O)               ((*(S)->select.type->adjust_u)(S, O))
#define H5S_SELECT_ADJUST_S(S, O)               ((*(S)->select.type->adjust_s)(S, O))
#define H5S_SELECT_PROJECT_SCALAR(S, O)         ((*(S)->select.type->project_scalar)(S, O))
#define H5S_SELECT_PROJECT_SIMPLE(S, NS, O)     ((*(S)->select.type->project_simple)(S, NS, O))
#define H5S_SELECT_ITER_COORDS(ITER, COORDS)    ((*(ITER)->type->iter_coords)(ITER, COORDS))
#define H5S_SELECT_ITER_BLOCK(ITER, START, END) ((*(ITER)->type->iter_block)(ITER, START, END))
#define H5S_SELECT_ITER_NELMTS(ITER)            ((*(ITER)->type->iter_nelmts)(ITER))
#define H5S_SELECT_ITER_HAS_NEXT_BLOCK(ITER)    ((*(ITER)->type->iter_has_next_block)(ITER))
#define H5S_SELECT_ITER_NEXT(ITER, NELEM)       ((*(ITER)->type->iter_next)(ITER, NELEM))
#define H5S_SELECT_ITER_NEXT_BLOCK(ITER)        ((*(ITER)->type->iter_next_block)(ITER))
#define H5S_SELECT_ITER_GET_SEQ_LIST(ITER, MAXSEQ, MAXBYTES, NSEQ, NBYTES, OFF, LEN)                         \
    ((*(ITER)->type->iter_get_seq_list)(ITER, MAXSEQ, MAXBYTES, NSEQ, NBYTES, OFF, LEN))
#define H5S_SELECT_ITER_RELEASE(ITER) ((*(ITER)->type->iter_release)(ITER))
#else /* H5S_MODULE */
#define H5S_GET_EXTENT_TYPE(S)                  (H5S_get_simple_extent_type(S))
#define H5S_GET_EXTENT_NDIMS(S)                 (H5S_get_simple_extent_ndims(S))
#define H5S_GET_EXTENT_NPOINTS(S)               (H5S_get_simple_extent_npoints(S))
#define H5S_GET_SELECT_NPOINTS(S)               (H5S_get_select_npoints(S))
#define H5S_GET_SELECT_TYPE(S)                  (H5S_get_select_type(S))
#define H5S_SELECT_VALID(S)                     (H5S_select_valid(S))
#define H5S_SELECT_SERIAL_SIZE(S)               (H5S_select_serial_size(S))
#define H5S_SELECT_SERIALIZE(S, BUF)            (H5S_select_serialize(S, BUF))
#define H5S_SELECT_BOUNDS(S, START, END)        (H5S_get_select_bounds(S, START, END))
#define H5S_SELECT_OFFSET(S, OFFSET)            (H5S_get_select_offset(S, OFFSET))
#define H5S_SELECT_IS_CONTIGUOUS(S)             (H5S_select_is_contiguous(S))
#define H5S_SELECT_IS_SINGLE(S)                 (H5S_select_is_single(S))
#define H5S_SELECT_IS_REGULAR(S)                (H5S_select_is_regular(S))
#define H5S_SELECT_ADJUST_U(S, O)               (H5S_select_adjust_u(S, O))
#define H5S_SELECT_ADJUST_S(S, O)               (H5S_select_adjust_s(S, O))
#define H5S_SELECT_PROJECT_SCALAR(S, O)         (H5S_select_project_scalar(S, O))
#define H5S_SELECT_PROJECT_SIMPLE(S, NS, O)     (H5S_select_project_simple(S, NS, O))
#define H5S_SELECT_ITER_COORDS(ITER, COORDS)    (H5S_select_iter_coords(ITER, COORDS))
#define H5S_SELECT_ITER_BLOCK(ITER, START, END) (H5S_select_iter_block(ITER, START, END))
#define H5S_SELECT_ITER_NELMTS(ITER)            (H5S_select_iter_nelmts(ITER))
#define H5S_SELECT_ITER_HAS_NEXT_BLOCK(ITER)    (H5S_select_iter_has_next_block(ITER))
#define H5S_SELECT_ITER_NEXT(ITER, NELEM)       (H5S_select_iter_next(ITER, NELEM))
#define H5S_SELECT_ITER_NEXT_BLOCK(ITER)        (H5S_select_iter_next_block(ITER))
#define H5S_SELECT_ITER_GET_SEQ_LIST(ITER, MAXSEQ, MAXBYTES, NSEQ, NBYTES, OFF, LEN)                         \
    (H5S_select_iter_get_seq_list(ITER, MAXSEQ, MAXBYTES, NSEQ, NBYTES, OFF, LEN))
#define H5S_SELECT_ITER_RELEASE(ITER) (H5S_select_iter_release(ITER))
#endif /* H5S_MODULE */

/* Handle these callbacks in a special way, since they have prologs that need to be executed */
#define H5S_SELECT_COPY(DST, SRC, SHARE)          (H5S_select_copy(DST, SRC, SHARE))
#define H5S_SELECT_SHAPE_SAME(S1, S2)             (H5S_select_shape_same(S1, S2))
#define H5S_SELECT_INTERSECT_BLOCK(S, START, END) (H5S_select_intersect_block(S, START, END))
#define H5S_SELECT_RELEASE(S)                     (H5S_select_release(S))
#define H5S_SELECT_DESERIALIZE(S, BUF, BUF_SIZE)  (H5S_select_deserialize(S, BUF, BUF_SIZE))

/* Forward declaration of structs used below */
struct H5O_t;
struct H5O_loc_t;

/* Early typedef to avoid circular dependencies */
typedef struct H5S_t H5S_t;

/* Operations on dataspaces */
H5_DLL herr_t      H5S_init(void);
H5_DLL H5S_t      *H5S_copy(const H5S_t *src, bool share_selection, bool copy_max);
H5_DLL herr_t      H5S_close(H5S_t *ds);
H5_DLL H5S_class_t H5S_get_simple_extent_type(const H5S_t *ds);
H5_DLL hssize_t    H5S_get_simple_extent_npoints(const H5S_t *ds);
H5_DLL hsize_t     H5S_get_npoints_max(const H5S_t *ds);
H5_DLL bool        H5S_has_extent(const H5S_t *ds);
H5_DLL int         H5S_get_simple_extent_ndims(const H5S_t *ds);
H5_DLL int    H5S_get_simple_extent_dims(const H5S_t *ds, hsize_t dims[] /*out*/, hsize_t max_dims[] /*out*/);
H5_DLL herr_t H5S_write(H5F_t *f, struct H5O_t *oh, unsigned update_flags, H5S_t *ds);
H5_DLL herr_t H5S_append(H5F_t *f, struct H5O_t *oh, H5S_t *ds);
H5_DLL H5S_t *H5S_read(const struct H5O_loc_t *loc);
H5_DLL htri_t H5S_set_extent(H5S_t *space, const hsize_t *size);
H5_DLL herr_t H5S_set_extent_real(H5S_t *space, const hsize_t *size);
H5_DLL herr_t H5S_set_extent_simple(H5S_t *space, unsigned rank, const hsize_t *dims, const hsize_t *max);
H5_DLL H5S_t *H5S_create(H5S_class_t type);
H5_DLL H5S_t *H5S_create_simple(unsigned rank, const hsize_t dims[/*rank*/], const hsize_t maxdims[/*rank*/]);
H5_DLL herr_t H5S_set_version(H5F_t *f, H5S_t *ds);
H5_DLL herr_t H5S_encode(H5S_t *obj, unsigned char **p, size_t *nalloc);
H5_DLL H5S_t *H5S_decode(const unsigned char **p);
H5_DLL herr_t H5S_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* Operations on dataspace extents */
H5_DLL hsize_t H5S_extent_nelem(const H5S_extent_t *ext);
H5_DLL int     H5S_extent_get_dims(const H5S_extent_t *ext, hsize_t dims[], hsize_t max_dims[]);
H5_DLL htri_t  H5S_extent_equal(const H5S_t *ds1, const H5S_t *ds2);
H5_DLL herr_t  H5S_extent_copy(H5S_t *dst, const H5S_t *src);

/* Operations on selections */
H5_DLL herr_t       H5S_select_deserialize(H5S_t **space, const uint8_t **p, const size_t p_size);
H5_DLL H5S_sel_type H5S_get_select_type(const H5S_t *space);
H5_DLL herr_t   H5S_select_iterate(void *buf, const H5T_t *type, H5S_t *space, const H5S_sel_iter_op_t *op,
                                   void *op_data);
H5_DLL herr_t   H5S_select_fill(const void *fill, size_t fill_size, H5S_t *space, void *buf);
H5_DLL htri_t   H5S_select_valid(const H5S_t *space);
H5_DLL hsize_t  H5S_get_select_npoints(const H5S_t *space);
H5_DLL herr_t   H5S_get_select_bounds(const H5S_t *space, hsize_t *start, hsize_t *end);
H5_DLL herr_t   H5S_get_select_offset(const H5S_t *space, hsize_t *offset);
H5_DLL int      H5S_get_select_unlim_dim(const H5S_t *space);
H5_DLL herr_t   H5S_get_select_num_elem_non_unlim(const H5S_t *space, hsize_t *num_elem_non_unlim);
H5_DLL herr_t   H5S_select_offset(H5S_t *space, const hssize_t *offset);
H5_DLL herr_t   H5S_select_copy(H5S_t *dst, const H5S_t *src, bool share_selection);
H5_DLL htri_t   H5S_select_shape_same(H5S_t *space1, H5S_t *space2);
H5_DLL htri_t   H5S_select_intersect_block(H5S_t *space, const hsize_t *start, const hsize_t *end);
H5_DLL herr_t   H5S_select_construct_projection(H5S_t *base_space, H5S_t **new_space_ptr,
                                                unsigned new_space_rank, hsize_t element_size,
                                                ptrdiff_t *buf_adj);
H5_DLL herr_t   H5S_select_release(H5S_t *ds);
H5_DLL hssize_t H5S_select_serial_size(H5S_t *space);
H5_DLL herr_t   H5S_select_serialize(H5S_t *space, uint8_t **p);
H5_DLL htri_t   H5S_select_is_contiguous(const H5S_t *space);
H5_DLL htri_t   H5S_select_is_single(const H5S_t *space);
H5_DLL htri_t   H5S_select_is_regular(H5S_t *space);
H5_DLL herr_t   H5S_select_adjust_u(H5S_t *space, const hsize_t *offset);
H5_DLL herr_t   H5S_select_adjust_s(H5S_t *space, const hssize_t *offset);
H5_DLL herr_t   H5S_select_project_scalar(const H5S_t *space, hsize_t *offset);
H5_DLL herr_t   H5S_select_project_simple(const H5S_t *space, H5S_t *new_space, hsize_t *offset);
H5_DLL herr_t H5S_select_project_intersection(H5S_t *src_space, H5S_t *dst_space, H5S_t *src_intersect_space,
                                              H5S_t **new_space_ptr, bool share_space);
H5_DLL herr_t H5S_select_subtract(H5S_t *space, H5S_t *subtract_space);
H5_DLL herr_t H5S_select_contig_block(H5S_t *space, bool *is_contig, hsize_t *off, size_t *len);

/* Operations on all selections */
H5_DLL herr_t H5S_select_all(H5S_t *space, bool rel_prev);

/* Operations on none selections */
H5_DLL herr_t H5S_select_none(H5S_t *space);

/* Operations on point selections */
H5_DLL herr_t H5S_select_elements(H5S_t *space, H5S_seloper_t op, size_t num_elem, const hsize_t *coord);

/* Operations on hyperslab selections */
H5_DLL herr_t  H5S_select_hyperslab(H5S_t *space, H5S_seloper_t op, const hsize_t start[],
                                    const hsize_t *stride, const hsize_t count[], const hsize_t *block);
H5_DLL herr_t  H5S_combine_hyperslab(const H5S_t *old_space, H5S_seloper_t op, const hsize_t start[],
                                     const hsize_t *stride, const hsize_t count[], const hsize_t *block,
                                     H5S_t **new_space);
H5_DLL herr_t  H5S_hyper_add_span_element(H5S_t *space, unsigned rank, const hsize_t *coords);
H5_DLL htri_t  H5S_hyper_normalize_offset(H5S_t *space, hssize_t *old_offset);
H5_DLL herr_t  H5S_hyper_denormalize_offset(H5S_t *space, const hssize_t *old_offset);
H5_DLL herr_t  H5S_hyper_clip_unlim(H5S_t *space, hsize_t clip_size);
H5_DLL hsize_t H5S_hyper_get_clip_extent(const H5S_t *clip_space, const H5S_t *match_space, bool incl_trail);
H5_DLL hsize_t H5S_hyper_get_clip_extent_match(const H5S_t *clip_space, const H5S_t *match_space,
                                               hsize_t match_clip_size, bool incl_trail);
H5_DLL H5S_t  *H5S_hyper_get_unlim_block(const H5S_t *space, hsize_t block_index);
H5_DLL hsize_t H5S_hyper_get_first_inc_block(const H5S_t *space, hsize_t clip_size, bool *partial);

/* Operations on selection iterators */
H5_DLL herr_t  H5S_select_iter_init(H5S_sel_iter_t *iter, H5S_t *space, size_t elmt_size, unsigned flags);
H5_DLL herr_t  H5S_select_iter_coords(const H5S_sel_iter_t *sel_iter, hsize_t *coords);
H5_DLL hsize_t H5S_select_iter_nelmts(const H5S_sel_iter_t *sel_iter);
H5_DLL herr_t  H5S_select_iter_next(H5S_sel_iter_t *sel_iter, size_t nelem);
H5_DLL herr_t H5S_select_iter_get_seq_list(H5S_sel_iter_t *iter, size_t maxseq, size_t maxbytes, size_t *nseq,
                                           size_t *nbytes, hsize_t *off, size_t *len);
H5_DLL herr_t H5S_select_iter_release(H5S_sel_iter_t *sel_iter);
H5_DLL herr_t H5S_sel_iter_close(H5S_sel_iter_t *sel_iter);

#ifdef H5_HAVE_PARALLEL
H5_DLL herr_t H5S_mpio_space_type(H5S_t *space, size_t elmt_size,
                                  /* out: */ MPI_Datatype *new_type, int *count, bool *is_derived_type,
                                  bool do_permute, hsize_t **permute_map, bool *is_permuted);
#endif /* H5_HAVE_PARALLEL */

#endif /* H5Sprivate_H */
