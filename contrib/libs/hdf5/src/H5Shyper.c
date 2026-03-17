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
 * Purpose:     Hyperslab selection dataspace I/O functions.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Smodule.h" /* This source code file is part of the H5S module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5Iprivate.h"  /* ID Functions                             */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Spkg.h"      /* Dataspace functions                      */
#include "H5VMprivate.h" /* Vector functions                         */

/****************/
/* Local Macros */
/****************/

/* Flags for which hyperslab fragments to compute */
#define H5S_HYPER_COMPUTE_B_NOT_A 0x01
#define H5S_HYPER_COMPUTE_A_AND_B 0x02
#define H5S_HYPER_COMPUTE_A_NOT_B 0x04

/* Macro to advance a span, possibly recycling it first */
#define H5S_HYPER_ADVANCE_SPAN(recover, curr_span, next_span, ERR)                                           \
    do {                                                                                                     \
        H5S_hyper_span_t *saved_next_span = (next_span);                                                     \
                                                                                                             \
        /* Check if the span should be recovered */                                                          \
        if (recover) {                                                                                       \
            if (H5S__hyper_free_span(curr_span) < 0)                                                         \
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, ERR, "unable to free span");                        \
            (recover) = false;                                                                               \
        }                                                                                                    \
                                                                                                             \
        /* Set the current span to saved next span */                                                        \
        (curr_span) = saved_next_span;                                                                       \
    } while (0)

/* Macro to add "skipped" elements to projection during the execution of
 * H5S__hyper_project_intersect() */
#define H5S_HYPER_PROJ_INT_ADD_SKIP(UDATA, ADD, ERR)                                                         \
    do {                                                                                                     \
        /* If there are any elements to add, we must add them                                                \
         * to the projection first before adding skip */                                                     \
        if ((UDATA)->nelem > 0)                                                                              \
            if (H5S__hyper_proj_int_build_proj(UDATA) < 0)                                                   \
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, ERR,                                              \
                            "can't add elements to projected selection");                                    \
        (UDATA)->skip += (ADD);                                                                              \
    } while (0) /* end H5S_HYPER_PROJ_INT_ADD_SKIP() */

/******************/
/* Local Typedefs */
/******************/

/* Define alias for hsize_t, for allocating H5S_hyper_span_info_t + bounds objects */
/* (Makes it easier to understand the alloc / free calls) */
typedef hsize_t hbounds_t;

/* Struct for holding persistent information during iteration for
 * H5S__hyper_project_intersect() */
typedef struct {
    const H5S_hyper_span_t
           *ds_span[H5S_MAX_RANK]; /* Array of the current spans in the destination space in each dimension */
    hsize_t ds_low[H5S_MAX_RANK]; /* Array of current low bounds (of iteration) for each element in ds_span */
    H5S_hyper_span_info_t
            *ps_span_info[H5S_MAX_RANK]; /* Array of span info structs for projected space during iteration */
    uint32_t ps_clean_bitmap; /* Bitmap of whether the nth rank has a clean projected space since the last
                                 time it was set to 1 */
    unsigned ss_rank;         /* Rank of source space */
    unsigned ds_rank;         /* Rank of destination space */
    unsigned depth;           /* Current depth of iterator in destination space */
    hsize_t  skip;            /* Number of elements to skip in projected space */
    hsize_t  nelem;           /* Number of elements to add to projected space (after skip) */
    uint64_t op_gen;          /* Operation generation for counting elements */
    bool     share_selection; /* Whether span trees in dst_space can be shared with proj_space */
} H5S_hyper_project_intersect_ud_t;

/* Assert that H5S_MAX_RANK is <= 32 so our trick with using a 32 bit bitmap
 * (ps_clean_bitmap) works.  If H5S_MAX_RANK increases either increase the size
 * of ps_clean_bitmap or change the algorithm to use an array. */
#if H5S_MAX_RANK > 32
#error H5S_MAX_RANK too large for ps_clean_bitmap field in H5S_hyper_project_intersect_ud_t struct
#endif

/********************/
/* Local Prototypes */
/********************/
static H5S_hyper_span_t      *H5S__hyper_new_span(hsize_t low, hsize_t high, H5S_hyper_span_info_t *down,
                                                  H5S_hyper_span_t *next);
static H5S_hyper_span_info_t *H5S__hyper_new_span_info(unsigned rank);
static H5S_hyper_span_info_t *H5S__hyper_copy_span_helper(H5S_hyper_span_info_t *spans, unsigned rank,
                                                          unsigned op_info_i, uint64_t op_gen);
static H5S_hyper_span_info_t *H5S__hyper_copy_span(H5S_hyper_span_info_t *spans, unsigned rank);
static bool                   H5S__hyper_cmp_spans(const H5S_hyper_span_info_t *span_info1,
                                                   const H5S_hyper_span_info_t *span_info2);
static herr_t                 H5S__hyper_free_span_info(H5S_hyper_span_info_t *span_info);
static herr_t                 H5S__hyper_free_span(H5S_hyper_span_t *span);
static herr_t H5S__hyper_span_blocklist(const H5S_hyper_span_info_t *spans, hsize_t start[], hsize_t end[],
                                        hsize_t rank, hsize_t *startblock, hsize_t *numblocks, hsize_t **buf);
static herr_t H5S__get_select_hyper_blocklist(H5S_t *space, hsize_t startblock, hsize_t numblocks,
                                              hsize_t *buf);
static H5S_hyper_span_t *H5S__hyper_coord_to_span(unsigned rank, const hsize_t *coords);
static herr_t  H5S__hyper_append_span(H5S_hyper_span_info_t **span_tree, unsigned ndims, hsize_t low,
                                      hsize_t high, H5S_hyper_span_info_t *down);
static herr_t  H5S__hyper_clip_spans(H5S_hyper_span_info_t *a_spans, H5S_hyper_span_info_t *b_spans,
                                     unsigned selector, unsigned ndims, H5S_hyper_span_info_t **a_not_b,
                                     H5S_hyper_span_info_t **a_and_b, H5S_hyper_span_info_t **b_not_a);
static herr_t  H5S__hyper_merge_spans(H5S_t *space, H5S_hyper_span_info_t *new_spans);
static hsize_t H5S__hyper_spans_nelem_helper(H5S_hyper_span_info_t *spans, unsigned op_info_i,
                                             uint64_t op_gen);
static hsize_t H5S__hyper_spans_nelem(H5S_hyper_span_info_t *spans);
static herr_t  H5S__hyper_add_disjoint_spans(H5S_t *space, H5S_hyper_span_info_t *new_spans);
static H5S_hyper_span_info_t *H5S__hyper_make_spans(unsigned rank, const hsize_t *start,
                                                    const hsize_t *stride, const hsize_t *count,
                                                    const hsize_t *block);
static herr_t                 H5S__hyper_update_diminfo(H5S_t *space, H5S_seloper_t op,
                                                        const H5S_hyper_dim_t *new_hyper_diminfo);
static herr_t                 H5S__hyper_generate_spans(H5S_t *space);
static bool                   H5S__check_spans_overlap(const H5S_hyper_span_info_t *spans1,
                                                       const H5S_hyper_span_info_t *spans2);
static herr_t  H5S__fill_in_new_space(H5S_t *space1, H5S_seloper_t op, H5S_hyper_span_info_t *space2_span_lst,
                                      bool can_own_span2, bool *span2_owned, bool *updated_spans,
                                      H5S_t **result);
static herr_t  H5S__generate_hyperslab(H5S_t *space, H5S_seloper_t op, const hsize_t start[],
                                       const hsize_t stride[], const hsize_t count[], const hsize_t block[]);
static herr_t  H5S__set_regular_hyperslab(H5S_t *space, const hsize_t start[], const hsize_t *app_stride,
                                          const hsize_t app_count[], const hsize_t *app_block,
                                          const hsize_t *opt_stride, const hsize_t opt_count[],
                                          const hsize_t *opt_block);
static herr_t  H5S__fill_in_select(H5S_t *space1, H5S_seloper_t op, H5S_t *space2, H5S_t **result);
static H5S_t  *H5S__combine_select(H5S_t *space1, H5S_seloper_t op, H5S_t *space2);
static herr_t  H5S__hyper_iter_get_seq_list_gen(H5S_sel_iter_t *iter, size_t maxseq, size_t maxelem,
                                                size_t *nseq, size_t *nelem, hsize_t *off, size_t *len);
static herr_t  H5S__hyper_iter_get_seq_list_opt(H5S_sel_iter_t *iter, size_t maxseq, size_t maxelem,
                                                size_t *nseq, size_t *nelem, hsize_t *off, size_t *len);
static herr_t  H5S__hyper_iter_get_seq_list_single(H5S_sel_iter_t *iter, size_t maxseq, size_t maxelem,
                                                   size_t *nseq, size_t *nelem, hsize_t *off, size_t *len);
static herr_t  H5S__hyper_proj_int_build_proj(H5S_hyper_project_intersect_ud_t *udata);
static herr_t  H5S__hyper_proj_int_iterate(H5S_hyper_span_info_t       *ss_span_info,
                                           const H5S_hyper_span_info_t *sis_span_info, hsize_t count,
                                           unsigned depth, H5S_hyper_project_intersect_ud_t *udata);
static void    H5S__hyper_get_clip_diminfo(hsize_t start, hsize_t stride, hsize_t *count, hsize_t *block,
                                           hsize_t clip_size);
static hsize_t H5S__hyper_get_clip_extent_real(const H5S_t *clip_space, hsize_t num_slices, bool incl_trail);

/* Selection callbacks */
static herr_t   H5S__hyper_copy(H5S_t *dst, const H5S_t *src, bool share_selection);
static herr_t   H5S__hyper_release(H5S_t *space);
static htri_t   H5S__hyper_is_valid(const H5S_t *space);
static hsize_t  H5S__hyper_span_nblocks(H5S_hyper_span_info_t *spans);
static hssize_t H5S__hyper_serial_size(H5S_t *space);
static herr_t   H5S__hyper_serialize(H5S_t *space, uint8_t **p);
static herr_t   H5S__hyper_deserialize(H5S_t **space, const uint8_t **p, const size_t p_size, bool skip);
static herr_t   H5S__hyper_bounds(const H5S_t *space, hsize_t *start, hsize_t *end);
static herr_t   H5S__hyper_offset(const H5S_t *space, hsize_t *offset);
static int      H5S__hyper_unlim_dim(const H5S_t *space);
static herr_t   H5S__hyper_num_elem_non_unlim(const H5S_t *space, hsize_t *num_elem_non_unlim);
static htri_t   H5S__hyper_is_contiguous(const H5S_t *space);
static htri_t   H5S__hyper_is_single(const H5S_t *space);
static htri_t   H5S__hyper_is_regular(H5S_t *space);
static htri_t   H5S__hyper_shape_same(H5S_t *space1, H5S_t *space2);
static htri_t   H5S__hyper_intersect_block(H5S_t *space, const hsize_t *start, const hsize_t *end);
static herr_t   H5S__hyper_adjust_u(H5S_t *space, const hsize_t *offset);
static herr_t   H5S__hyper_adjust_s(H5S_t *space, const hssize_t *offset);
static herr_t   H5S__hyper_project_scalar(const H5S_t *space, hsize_t *offset);
static herr_t   H5S__hyper_project_simple(const H5S_t *space, H5S_t *new_space, hsize_t *offset);
static herr_t   H5S__hyper_iter_init(H5S_t *space, H5S_sel_iter_t *iter);

/* Selection iteration callbacks */
static herr_t  H5S__hyper_iter_coords(const H5S_sel_iter_t *iter, hsize_t *coords);
static herr_t  H5S__hyper_iter_block(const H5S_sel_iter_t *iter, hsize_t *start, hsize_t *end);
static hsize_t H5S__hyper_iter_nelmts(const H5S_sel_iter_t *iter);
static htri_t  H5S__hyper_iter_has_next_block(const H5S_sel_iter_t *sel_iter);
static herr_t  H5S__hyper_iter_next(H5S_sel_iter_t *sel_iter, size_t nelem);
static herr_t  H5S__hyper_iter_next_block(H5S_sel_iter_t *sel_iter);
static herr_t H5S__hyper_iter_get_seq_list(H5S_sel_iter_t *iter, size_t maxseq, size_t maxbytes, size_t *nseq,
                                           size_t *nbytes, hsize_t *off, size_t *len);
static herr_t H5S__hyper_iter_release(H5S_sel_iter_t *sel_iter);

/*****************************/
/* Library Private Variables */
/*****************************/

/*********************/
/* Package Variables */
/*********************/

/* Selection properties for hyperslab selections */
const H5S_select_class_t H5S_sel_hyper[1] = {{
    H5S_SEL_HYPERSLABS,

    /* Methods on selection */
    H5S__hyper_copy,
    H5S__hyper_release,
    H5S__hyper_is_valid,
    H5S__hyper_serial_size,
    H5S__hyper_serialize,
    H5S__hyper_deserialize,
    H5S__hyper_bounds,
    H5S__hyper_offset,
    H5S__hyper_unlim_dim,
    H5S__hyper_num_elem_non_unlim,
    H5S__hyper_is_contiguous,
    H5S__hyper_is_single,
    H5S__hyper_is_regular,
    H5S__hyper_shape_same,
    H5S__hyper_intersect_block,
    H5S__hyper_adjust_u,
    H5S__hyper_adjust_s,
    H5S__hyper_project_scalar,
    H5S__hyper_project_simple,
    H5S__hyper_iter_init,
}};

/* Format version bounds for dataspace hyperslab selection */
const unsigned H5O_sds_hyper_ver_bounds[] = {
    H5S_HYPER_VERSION_1, /* H5F_LIBVER_EARLIEST */
    H5S_HYPER_VERSION_1, /* H5F_LIBVER_V18 */
    H5S_HYPER_VERSION_2, /* H5F_LIBVER_V110 */
    H5S_HYPER_VERSION_3, /* H5F_LIBVER_V112 */
    H5S_HYPER_VERSION_3  /* H5F_LIBVER_LATEST */
};

/*******************/
/* Local Variables */
/*******************/

/* Iteration properties for hyperslab selections */
static const H5S_sel_iter_class_t H5S_sel_iter_hyper[1] = {{
    H5S_SEL_HYPERSLABS,

    /* Methods on selection iterator */
    H5S__hyper_iter_coords,
    H5S__hyper_iter_block,
    H5S__hyper_iter_nelmts,
    H5S__hyper_iter_has_next_block,
    H5S__hyper_iter_next,
    H5S__hyper_iter_next_block,
    H5S__hyper_iter_get_seq_list,
    H5S__hyper_iter_release,
}};

/* Arrays for default stride, block, etc. */
static const hsize_t H5S_hyper_zeros_g[H5S_MAX_RANK] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
static const hsize_t H5S_hyper_ones_g[H5S_MAX_RANK]  = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                                       1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

/* Declare a free list to manage the H5S_hyper_sel_t struct */
H5FL_DEFINE_STATIC(H5S_hyper_sel_t);

/* Declare a free list to manage the H5S_hyper_span_t struct */
H5FL_DEFINE_STATIC(H5S_hyper_span_t);

/* Declare a free list to manage the H5S_hyper_span_info_t + hsize_t array struct */
H5FL_BARR_DEFINE_STATIC(H5S_hyper_span_info_t, hbounds_t, H5S_MAX_RANK * 2);

/* Declare extern free list to manage the H5S_sel_iter_t struct */
H5FL_EXTERN(H5S_sel_iter_t);

/* Current operation generation */
/* (Start with '1' to avoid clashing with '0' value in newly allocated structs) */
static uint64_t H5S_hyper_op_gen_g = 1;

/* Uncomment this to provide the debugging routines for printing selection info */
/* #define H5S_HYPER_DEBUG */
#ifdef H5S_HYPER_DEBUG
static herr_t
H5S__hyper_print_spans_helper(FILE *f, const H5S_hyper_span_t *span, unsigned depth)
{
    FUNC_ENTER_PACKAGE_NOERR

    while (span) {
        fprintf(f, "%s: %*sdepth=%u, span=%p, (%" PRIuHSIZE ", %" PRIuHSIZE "), next=%p\n", __func__,
                depth * 2, "", depth, (void *)span, span->low, span->high, (void *)span->next);
        if (span->down) {
            fprintf(f, "%s: %*sspans=%p, count=%u, bounds[0]={%" PRIuHSIZE ", %" PRIuHSIZE "}, head=%p\n",
                    __func__, (depth + 1) * 2, "", (void *)span->down, span->down->count,
                    span->down->low_bounds[0], span->down->high_bounds[0], (void *)span->down->head);
            H5S__hyper_print_spans_helper(f, span->down->head, depth + 1);
        } /* end if */
        span = span->next;
    } /* end while */

    FUNC_LEAVE_NOAPI(SUCCEED)
}

static herr_t
H5S__hyper_print_spans(FILE *f, const H5S_hyper_span_info_t *span_lst)
{
    FUNC_ENTER_PACKAGE_NOERR

    if (span_lst != NULL) {
        fprintf(f, "%s: spans=%p, count=%u, bounds[0]={%" PRIuHSIZE ", %" PRIuHSIZE "}, head=%p\n", __func__,
                (void *)span_lst, span_lst->count, span_lst->low_bounds[0], span_lst->high_bounds[0],
                (void *)span_lst->head);
        H5S__hyper_print_spans_helper(f, span_lst->head, 0);
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
}

static herr_t
H5S__space_print_spans(FILE *f, const H5S_t *space)
{
    FUNC_ENTER_PACKAGE_NOERR

    H5S__hyper_print_spans(f, space->select.sel_info.hslab->span_lst);

    FUNC_LEAVE_NOAPI(SUCCEED)
}

static herr_t
H5S__hyper_print_diminfo_helper(FILE *f, const char *field, unsigned ndims, const H5S_hyper_dim_t *dinfo)
{
    unsigned u; /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    if (dinfo != NULL) {
        fprintf(f, "%s: %s: start=[", __func__, field);
        for (u = 0; u < ndims; u++)
            fprintf(f, "%" PRIuHSIZE "%s", dinfo[u].start, (u < (ndims - 1) ? ", " : "]\n"));
        fprintf(f, "%s: %s: stride=[", __func__, field);
        for (u = 0; u < ndims; u++)
            fprintf(f, "%" PRIuHSIZE "%s", dinfo[u].stride, (u < (ndims - 1) ? ", " : "]\n"));
        fprintf(f, "%s: %s: count=[", __func__, field);
        for (u = 0; u < ndims; u++)
            fprintf(f, "%" PRIuHSIZE "%s", dinfo[u].count, (u < (ndims - 1) ? ", " : "]\n"));
        fprintf(f, "%s: %s: block=[", __func__, field);
        for (u = 0; u < ndims; u++)
            fprintf(f, "%" PRIuHSIZE "%s", dinfo[u].block, (u < (ndims - 1) ? ", " : "]\n"));
    } /* end if */
    else
        fprintf(f, "%s: %s==NULL\n", __func__, field);

    FUNC_LEAVE_NOAPI(SUCCEED)
}

static herr_t
H5S__hyper_print_diminfo(FILE *f, const H5S_t *space)
{
    FUNC_ENTER_PACKAGE_NOERR

    H5S__hyper_print_diminfo_helper(f, "diminfo.opt", space->extent.rank,
                                    space->select.sel_info.hslab->diminfo.opt);
    H5S__hyper_print_diminfo_helper(f, "diminfo.app", space->extent.rank,
                                    space->select.sel_info.hslab->diminfo.app);

    FUNC_LEAVE_NOAPI(SUCCEED)
}

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_print_spans_dfs
 PURPOSE
    Output the span elements for one span list in depth-first order
 USAGE
    herr_t H5S__hyper_print_spans_dfs(f, span_lst, depth)
        FILE *f;                                  IN: the file to output
        const H5S_hyper_span_info_t *span_lst;    IN: the span list to output
        unsigned depth;                           IN: the level of this span list
 RETURNS
    non-negative on success, negative on failure
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_print_spans_dfs(FILE *f, const H5S_hyper_span_info_t *span_lst, unsigned depth, unsigned dims)
{
    H5S_hyper_span_t *actual_tail = NULL;
    H5S_hyper_span_t *cur_elem;
    unsigned          num_elems = 0;
    unsigned          u, elem_idx;

    FUNC_ENTER_PACKAGE_NOERR

    /* get the actual tail from head */
    cur_elem = span_lst->head;
    assert(cur_elem); /* at least 1 element */
    while (cur_elem) {
        actual_tail = cur_elem;
        cur_elem    = cur_elem->next;
        num_elems++;
    } /* end while */

    for (u = 0; u < depth; u++)
        fprintf(f, "\t");
    fprintf(f, "DIM[%u]: ref_count=%u, #elems=%u, head=%p, tail=%p, actual_tail=%p, matched=%d\n", depth,
            span_lst->count, num_elems, (void *)span_lst->head, (void *)span_lst->tail, (void *)actual_tail,
            (span_lst->tail == actual_tail));

    for (u = 0; u < depth; u++)
        fprintf(f, "\t");
    fprintf(f, "low_bounds=[");
    for (u = 0; u < dims - 1; u++)
        fprintf(f, "%" PRIuHSIZE ",", span_lst->low_bounds[u]);
    fprintf(f, "%" PRIuHSIZE "]\n", span_lst->low_bounds[dims - 1]);

    for (u = 0; u < depth; u++)
        fprintf(f, "\t");
    fprintf(f, "high_bounds=[");
    for (u = 0; u < dims - 1; u++)
        fprintf(f, "%" PRIuHSIZE ",", span_lst->high_bounds[u]);
    fprintf(f, "%" PRIuHSIZE "]\n", span_lst->high_bounds[dims - 1]);

    cur_elem = span_lst->head;
    elem_idx = 0;
    while (cur_elem) {
        for (u = 0; u < depth; u++)
            fprintf(f, "\t");
        fprintf(f, "ELEM[%u]: ptr=%p, low=%" PRIuHSIZE ", high=%" PRIuHSIZE ", down=%p\n", elem_idx++,
                (void *)cur_elem, cur_elem->low, cur_elem->high, (void *)cur_elem->down);
        if (cur_elem->down)
            H5S__hyper_print_spans_dfs(f, cur_elem->down, depth + 1, dims);
        cur_elem = cur_elem->next;
    } /* end while */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_print_spans_dfs() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_print_space_dfs
 PURPOSE
    Output the span elements for one hyperslab selection space in depth-first order
 USAGE
    herr_t H5S__hyper_print_space_dfs(f, space)
        FILE *f;               IN: the file to output
        const H5S_t *space;    IN: the selection space to output
 RETURNS
    non-negative on success, negative on failure
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_print_space_dfs(FILE *f, const H5S_t *space)
{
    const H5S_hyper_sel_t *hslab = space->select.sel_info.hslab;
    const unsigned         dims  = space->extent.rank;
    unsigned               u;

    FUNC_ENTER_PACKAGE_NOERR

    assert(hslab);

    fprintf(f, "=======================\n");
    fprintf(f, "SPACE: span_lst=%p, #dims=%u, offset_changed=%d\n", (void *)hslab->span_lst, dims,
            space->select.offset_changed);

    fprintf(f, "       offset=[");
    for (u = 0; u < dims - 1; u++)
        fprintf(f, "%lld,", space->select.offset[u]);
    fprintf(f, "%lld]\n", space->select.offset[dims - 1]);

    fprintf(f, "       low_bounds=[");
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        for (u = 0; u < dims - 1; u++)
            fprintf(f, "%" PRIuHSIZE ",", space->select.sel_info.hslab->diminfo.low_bounds[u]);
        fprintf(f, "%" PRIuHSIZE "]\n", space->select.sel_info.hslab->diminfo.low_bounds[dims - 1]);
    } /* end if */
    else {
        for (u = 0; u < dims - 1; u++)
            fprintf(f, "%" PRIuHSIZE ",", space->select.sel_info.hslab->span_lst->low_bounds[u]);
        fprintf(f, "%" PRIuHSIZE "]\n", space->select.sel_info.hslab->span_lst->low_bounds[dims - 1]);
    } /* end else */

    fprintf(f, "       high_bounds=[");
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        for (u = 0; u < dims - 1; u++)
            fprintf(f, "%" PRIuHSIZE ",", space->select.sel_info.hslab->diminfo.high_bounds[u]);
        fprintf(f, "%" PRIuHSIZE "]\n", space->select.sel_info.hslab->diminfo.high_bounds[dims - 1]);
    } /* end if */
    else {
        for (u = 0; u < dims - 1; u++)
            fprintf(f, "%" PRIuHSIZE ",", space->select.sel_info.hslab->span_lst->high_bounds[u]);
        fprintf(f, "%" PRIuHSIZE "]\n", space->select.sel_info.hslab->span_lst->high_bounds[dims - 1]);
    } /* end else */

    /* Print out diminfo, if it's valid */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES)
        H5S__hyper_print_diminfo(f, space);

    /* Start print out the highest-order of dimension */
    if (hslab->span_lst)
        H5S__hyper_print_spans_dfs(f, hslab->span_lst, 0, dims);
    fprintf(f, "=======================\n\n");

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_print_space_dfs() */
#endif /* H5S_HYPER_DEBUG */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_get_op_gen
 *
 * Purpose:    Acquire a unique operation generation value
 *
 * Return:    Operation generation value (can't fail)
 *
 * Notes:       Assumes that a 64-bit value will not wrap around during
 *              the lifespan of the process.
 *
 *-------------------------------------------------------------------------
 */
uint64_t
H5S__hyper_get_op_gen(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(H5S_hyper_op_gen_g++)
} /* end H5S__hyper_op_gen() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_iter_init
 *
 * Purpose:     Initializes iteration information for hyperslab selection.
 *
 * Return:      Non-negative on success, negative on failure.
 *
 * Notes:       If the 'iter->elmt_size' field is set to zero, the regular
 *              hyperslab selection iterator will not be 'flattened'.  This
 *              is used by the H5S_select_shape_same() code to avoid changing
 *              the rank and appearance of the selection.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_iter_init(H5S_t *space, H5S_sel_iter_t *iter)
{
    hsize_t *slab_size;           /* Pointer to the dataspace dimensions to use for calc. slab */
    hsize_t  acc;                 /* Accumulator for computing cumulative sizes */
    unsigned slab_dim;            /* Rank of the fastest changing dimension for calc. slab */
    unsigned rank;                /* Dataspace's dimension rank */
    unsigned u;                   /* Index variable */
    int      i;                   /* Index variable */
    herr_t   ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space && H5S_SEL_HYPERSLABS == H5S_GET_SELECT_TYPE(space));
    assert(iter);
    assert(space->select.sel_info.hslab->unlim_dim < 0);

    /* Initialize the hyperslab iterator's rank */
    iter->u.hyp.iter_rank = 0;

    /* Get the rank of the dataspace */
    rank = iter->rank;

    /* Attempt to rebuild diminfo if it is invalid and has not been confirmed
     * to be impossible.
     */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_NO)
        H5S__hyper_rebuild(space);

    /* Check for the special case of just one H5Sselect_hyperslab call made */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        /* Initialize the information needed for regular hyperslab I/O */
        const H5S_hyper_dim_t *tdiminfo;     /* Temporary pointer to diminfo information */
        const hsize_t         *mem_size;     /* Temporary pointer to dataspace extent's dimension sizes */
        unsigned               cont_dim = 0; /* # of contiguous dimensions */

        /* Set the temporary pointer to the dimension information */
        tdiminfo = space->select.sel_info.hslab->diminfo.opt;

        /* Set the temporary pointer to the dataspace extent's dimension sizes */
        mem_size = iter->dims;

        /*
         * For a regular hyperslab to be contiguous up to some dimension, it
         * must have only one block (i.e. count==1 in all dimensions up to that
         * dimension) and the block size must be the same as the dataspace's
         * extent in that dimension and all dimensions up to that dimension.
         */

        /* Don't flatten adjacent elements into contiguous block if the
         * element size is 0.  This is for the H5S_select_shape_same() code.
         */
        if (iter->elmt_size > 0) {
            /* Check for any "contiguous" blocks that can be flattened */
            for (u = (rank - 1); u > 0; u--) {
                if (tdiminfo[u].count == 1 && tdiminfo[u].block == mem_size[u]) {
                    cont_dim++;
                    iter->u.hyp.flattened[u] = true;
                } /* end if */
                else
                    iter->u.hyp.flattened[u] = false;
            } /* end for */
            iter->u.hyp.flattened[0] = false;
        } /* end if */

        /* Check if the regular selection can be "flattened" */
        if (cont_dim > 0) {
            bool     last_dim_flattened = true; /* Flag to indicate that the last dimension was flattened */
            unsigned flat_rank          = rank - cont_dim; /* Number of dimensions after flattening */
            unsigned curr_dim;                             /* Current dimension */

            /* Set the iterator's rank to the contiguous dimensions */
            iter->u.hyp.iter_rank = flat_rank;

            /* "Flatten" dataspace extent and selection information */
            curr_dim = flat_rank - 1;
            for (i = (int)rank - 1, acc = 1; i >= 0; i--) {
                if (tdiminfo[i].block == mem_size[i] && i > 0) {
                    /* "Flatten" this dimension */
                    assert(tdiminfo[i].start == 0);
                    acc *= mem_size[i];

                    /* Indicate that the dimension was flattened */
                    last_dim_flattened = true;
                } /* end if */
                else {
                    if (last_dim_flattened) {
                        /* First dimension after flattened dimensions */
                        iter->u.hyp.diminfo[curr_dim].start = tdiminfo[i].start * acc;

                        /* Special case for single block regular selections */
                        if (tdiminfo[i].count == 1)
                            iter->u.hyp.diminfo[curr_dim].stride = 1;
                        else
                            iter->u.hyp.diminfo[curr_dim].stride = tdiminfo[i].stride * acc;
                        iter->u.hyp.diminfo[curr_dim].count = tdiminfo[i].count;
                        iter->u.hyp.diminfo[curr_dim].block = tdiminfo[i].block * acc;
                        iter->u.hyp.size[curr_dim]          = mem_size[i] * acc;
                        iter->u.hyp.sel_off[curr_dim]       = iter->sel_off[i] * (hssize_t)acc;

                        /* Reset the "last dim flattened" flag to avoid flattened any further dimensions */
                        last_dim_flattened = false;

                        /* Reset the "accumulator" for possible further dimension flattening */
                        acc = 1;
                    } /* end if */
                    else {
                        /* All other dimensions */
                        iter->u.hyp.diminfo[curr_dim].start  = tdiminfo[i].start;
                        iter->u.hyp.diminfo[curr_dim].stride = tdiminfo[i].stride;
                        iter->u.hyp.diminfo[curr_dim].count  = tdiminfo[i].count;
                        iter->u.hyp.diminfo[curr_dim].block  = tdiminfo[i].block;
                        iter->u.hyp.size[curr_dim]           = mem_size[i];
                        iter->u.hyp.sel_off[curr_dim]        = iter->sel_off[i];
                    } /* end else */

                    /* Decrement "current" flattened dimension */
                    curr_dim--;
                } /* end if */
            }     /* end for */

            /* Initialize "flattened" iterator offset to initial location and dataspace extent and selection
             * information to correct values */
            for (u = 0; u < flat_rank; u++)
                iter->u.hyp.off[u] = iter->u.hyp.diminfo[u].start;

            /* Set up information for computing slab sizes */
            slab_dim  = iter->u.hyp.iter_rank - 1;
            slab_size = iter->u.hyp.size;
        } /* end if */
        else {
            /* Make local copy of the regular selection information */
            HDcompile_assert(sizeof(iter->u.hyp.diminfo) ==
                             sizeof(space->select.sel_info.hslab->diminfo.opt));
            H5MM_memcpy(iter->u.hyp.diminfo, tdiminfo, sizeof(iter->u.hyp.diminfo));

            /* Initialize position to initial location */
            for (u = 0; u < rank; u++)
                iter->u.hyp.off[u] = tdiminfo[u].start;

            /* Set up information for computing slab sizes */
            slab_dim  = iter->rank - 1;
            slab_size = iter->dims;
        } /* end else */

        /* Flag the diminfo information as valid in the iterator */
        iter->u.hyp.diminfo_valid = true;

        /* Initialize irregular region information also (for release) */
        iter->u.hyp.spans = NULL;
    }                                 /* end if */
    else {                            /* Initialize the information needed for non-regular hyperslab I/O */
        H5S_hyper_span_info_t *spans; /* Pointer to hyperslab span info node */

        /* If this iterator is created from an API call, by default we clone the
         *  selection now, as the dataspace could be modified or go out of scope.
         *
         *  However, if the H5S_SEL_ITER_SHARE_WITH_DATASPACE flag is given,
         *  the selection is shared between the selection iterator and the
         *  dataspace.  In this case, the application _must_not_ modify or
         *  close the dataspace that the iterator is operating on, or undefined
         *  behavior will occur.
         */
        if ((iter->flags & H5S_SEL_ITER_API_CALL) && !(iter->flags & H5S_SEL_ITER_SHARE_WITH_DATASPACE)) {
            /* Copy the span tree */
            if (NULL == (iter->u.hyp.spans = H5S__hyper_copy_span(space->select.sel_info.hslab->span_lst,
                                                                  space->extent.rank)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "can't copy span tree");
        } /* end if */
        else {
            /* Share the source dataspace's span tree by incrementing the reference count on it */
            assert(space->select.sel_info.hslab->span_lst);
            iter->u.hyp.spans = space->select.sel_info.hslab->span_lst;
            iter->u.hyp.spans->count++;
        } /* end else */

        /* Initialize the starting span_info's and spans */
        spans = iter->u.hyp.spans;
        for (u = 0; u < rank; u++) {
            /* Set the pointers to the initial span in each dimension */
            assert(spans);
            assert(spans->head);

            /* Set the pointer to the first span in the list for this node */
            iter->u.hyp.span[u] = spans->head;

            /* Set the initial offset to low bound of span */
            iter->u.hyp.off[u] = iter->u.hyp.span[u]->low;

            /* Get the pointer to the next level down */
            spans = spans->head->down;
        } /* end for */

        /* Set up information for computing slab sizes */
        slab_dim  = iter->rank - 1;
        slab_size = iter->dims;

        /* Flag the diminfo information as not valid in the iterator */
        iter->u.hyp.diminfo_valid = false;
    } /* end else */

    /* Compute the cumulative size of dataspace dimensions */
    for (i = (int)slab_dim, acc = iter->elmt_size; i >= 0; i--) {
        iter->u.hyp.slab[i] = acc;
        acc *= slab_size[i];
    } /* end for */

    /* Initialize more information for irregular hyperslab selections */
    if (!iter->u.hyp.diminfo_valid) {
        /* Set the offset of the first element iterated on, in each dimension */
        for (u = 0; u < rank; u++)
            /* Compute the sequential element offset */
            iter->u.hyp.loc_off[u] =
                ((hsize_t)((hssize_t)iter->u.hyp.off[u] + iter->sel_off[u])) * iter->u.hyp.slab[u];
    } /* end if */

    /* Initialize type of selection iterator */
    iter->type = H5S_sel_iter_hyper;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_iter_init() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_iter_coords
 *
 * Purpose:     Retrieve the current coordinates of iterator for current
 *              selection
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_iter_coords(const H5S_sel_iter_t *iter, hsize_t *coords)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);
    assert(coords);

    /* Copy the offset of the current point */

    /* Check for a single "regular" hyperslab */
    if (iter->u.hyp.diminfo_valid) {
        /* Check if this is a "flattened" regular hyperslab selection */
        if (iter->u.hyp.iter_rank != 0 && iter->u.hyp.iter_rank < iter->rank) {
            int u, v; /* Dimension indices */

            /* Set the starting rank of both the "natural" & "flattened" dimensions */
            u = (int)iter->rank - 1;
            v = (int)iter->u.hyp.iter_rank - 1;

            /* Construct the "natural" dimensions from a set of flattened coordinates */
            while (u >= 0) {
                if (iter->u.hyp.flattened[u]) {
                    int begin = u; /* The rank of the first flattened dimension */

                    /* Walk up through as many flattened dimensions as possible */
                    do {
                        u--;
                    } while (u >= 0 && iter->u.hyp.flattened[u]);

                    /* Compensate for possibly overshooting dim 0 */
                    if (u < 0)
                        u = 0;

                    /* Sanity check */
                    assert(v >= 0);

                    /* Compute the coords for the flattened dimensions */
                    H5VM_array_calc(iter->u.hyp.off[v], (unsigned)((begin - u) + 1), &(iter->dims[u]),
                                    &(coords[u]));

                    /* Continue to faster dimension in both indices */
                    u--;
                    v--;
                } /* end if */
                else {
                    /* Walk up through as many non-flattened dimensions as possible */
                    while (u >= 0 && !iter->u.hyp.flattened[u]) {
                        /* Sanity check */
                        assert(v >= 0);

                        /* Copy the coordinate */
                        coords[u] = iter->u.hyp.off[v];

                        /* Continue to faster dimension in both indices */
                        u--;
                        v--;
                    } /* end while */
                }     /* end else */
            }         /* end while */
            assert(v < 0);
        } /* end if */
        else
            H5MM_memcpy(coords, iter->u.hyp.off, sizeof(hsize_t) * iter->rank);
    } /* end if */
    else
        H5MM_memcpy(coords, iter->u.hyp.off, sizeof(hsize_t) * iter->rank);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_iter_coords() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_iter_block
 *
 * Purpose:     Retrieve the current block of iterator for current
 *              selection
 *
 * Return:      Non-negative on success, negative on failure
 *
 * Notes:       This routine assumes that the iterator is always located at
 *              the beginning of a block.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_iter_block(const H5S_sel_iter_t *iter, hsize_t *start, hsize_t *end)
{
    unsigned u; /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);
    assert(start);
    assert(end);

    /* Copy the offset of the current point */

    /* Check for a single "regular" hyperslab */
    if (iter->u.hyp.diminfo_valid) {
        /* Copy the start and compute the end of the block */
        for (u = 0; u < iter->rank; u++) {
            start[u] = iter->u.hyp.off[u];
            end[u]   = (start[u] + iter->u.hyp.diminfo[u].block) - 1;
        }
    } /* end if */
    else {
        /* Copy the start & end of the block */
        for (u = 0; u < iter->rank; u++) {
            start[u] = iter->u.hyp.span[u]->low;
            end[u]   = iter->u.hyp.span[u]->high;
        }
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_iter_block() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_iter_nelmts
 *
 * Purpose:     Return number of elements left to process in iterator
 *
 * Return:      Non-negative number of elements on success, zero on failure
 *
 *-------------------------------------------------------------------------
 */
static hsize_t
H5S__hyper_iter_nelmts(const H5S_sel_iter_t *iter)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);

    FUNC_LEAVE_NOAPI(iter->elmt_left)
} /* end H5S__hyper_iter_nelmts() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_iter_has_next_block
 PURPOSE
    Check if there is another block left in the current iterator
 USAGE
    htri_t H5S__hyper_iter_has_next_block(iter)
        const H5S_sel_iter_t *iter;       IN: Pointer to selection iterator
 RETURNS
    Non-negative (true/false) on success/Negative on failure
 DESCRIPTION
    Check if there is another block available in the selection iterator.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5_ATTR_PURE htri_t
H5S__hyper_iter_has_next_block(const H5S_sel_iter_t *iter)
{
    unsigned u;                 /* Local index variable */
    htri_t   ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);

    /* Check for a single "regular" hyperslab */
    if (iter->u.hyp.diminfo_valid) {
        const H5S_hyper_dim_t *tdiminfo; /* Temporary pointer to diminfo information */
        const hsize_t         *toff;     /* Temporary offset in selection */

        /* Check if the offset of the iterator is at the last location in all dimensions */
        tdiminfo = iter->u.hyp.diminfo;
        toff     = iter->u.hyp.off;
        for (u = 0; u < iter->rank; u++) {
            /* If there is only one block, continue */
            if (tdiminfo[u].count == 1)
                continue;
            if (toff[u] != (tdiminfo[u].start + ((tdiminfo[u].count - 1) * tdiminfo[u].stride)))
                HGOTO_DONE(true);
        } /* end for */
    }     /* end if */
    else {
        /* Check for any levels of the tree with more sequences in them */
        for (u = 0; u < iter->rank; u++)
            if (iter->u.hyp.span[u]->next != NULL)
                HGOTO_DONE(true);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_iter_has_next_block() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_iter_next
 *
 * Purpose:     Moves a hyperslab iterator to the beginning of the next sequence
 *              of elements to read.  Handles walking off the end in all dimensions.
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_iter_next(H5S_sel_iter_t *iter, size_t nelem)
{
    unsigned ndims;    /* Number of dimensions of dataset */
    int      fast_dim; /* Rank of the fastest changing dimension for the dataspace */
    unsigned u;        /* Counters */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check for the special case of just one H5Sselect_hyperslab call made */
    /* (i.e. a regular hyperslab selection */
    if (iter->u.hyp.diminfo_valid) {
        const H5S_hyper_dim_t *tdiminfo; /* Temporary pointer to diminfo information */
        hsize_t                iter_offset[H5S_MAX_RANK];
        hsize_t                iter_count[H5S_MAX_RANK];
        int                    temp_dim; /* Temporary rank holder */

        /* Check if this is a "flattened" regular hyperslab selection */
        if (iter->u.hyp.iter_rank != 0 && iter->u.hyp.iter_rank < iter->rank)
            /* Set the aliases for the dimension rank */
            ndims = iter->u.hyp.iter_rank;
        else
            /* Set the aliases for the dimension rank */
            ndims = iter->rank;

        /* Set the fastest dimension rank */
        fast_dim = (int)ndims - 1;

        /* Set the local copy of the diminfo pointer */
        tdiminfo = iter->u.hyp.diminfo;

        /* Calculate the offset and block count for each dimension */
        for (u = 0; u < ndims; u++) {
            if (tdiminfo[u].count == 1) {
                iter_offset[u] = iter->u.hyp.off[u] - tdiminfo[u].start;
                iter_count[u]  = 0;
            } /* end if */
            else {
                iter_offset[u] = (iter->u.hyp.off[u] - tdiminfo[u].start) % tdiminfo[u].stride;
                iter_count[u]  = (iter->u.hyp.off[u] - tdiminfo[u].start) / tdiminfo[u].stride;
            } /* end else */
        }     /* end for */

        /* Loop through, advancing the offset & counts, until all the nelements are accounted for */
        while (nelem > 0) {
            /* Start with the fastest changing dimension */
            temp_dim = fast_dim;
            while (temp_dim >= 0) {
                if (temp_dim == fast_dim) {
                    size_t  actual_elem; /* Actual # of elements advanced on each iteration through loop */
                    hsize_t block_elem;  /* Number of elements left in a block */

                    /* Compute the number of elements left in block */
                    block_elem = tdiminfo[temp_dim].block - iter_offset[temp_dim];

                    /* Compute the number of actual elements to advance */
                    actual_elem = (size_t)MIN(nelem, block_elem);

                    /* Move the iterator over as many elements as possible */
                    iter_offset[temp_dim] += actual_elem;

                    /* Decrement the number of elements advanced */
                    nelem -= actual_elem;
                } /* end if */
                else
                    /* Move to the next row in the current dimension */
                    iter_offset[temp_dim]++;

                /* If this block is still in the range of blocks to output for the dimension, break out of
                 * loop */
                if (iter_offset[temp_dim] < tdiminfo[temp_dim].block)
                    break;
                else {
                    /* Move to the next block in the current dimension */
                    iter_offset[temp_dim] = 0;
                    iter_count[temp_dim]++;

                    /* If this block is still in the range of blocks to output for the dimension, break out of
                     * loop */
                    if (iter_count[temp_dim] < tdiminfo[temp_dim].count)
                        break;
                    else
                        iter_count[temp_dim] = 0; /* reset back to the beginning of the line */
                }                                 /* end else */

                /* Decrement dimension count */
                temp_dim--;
            } /* end while */
        }     /* end while */

        /* Translate current iter_offset and iter_count into iterator position */
        for (u = 0; u < ndims; u++)
            iter->u.hyp.off[u] = tdiminfo[u].start + (tdiminfo[u].stride * iter_count[u]) + iter_offset[u];
    } /* end if */
    /* Must be an irregular hyperslab selection */
    else {
        H5S_hyper_span_t  *curr_span = NULL; /* Current hyperslab span node */
        H5S_hyper_span_t **ispan;            /* Iterator's hyperslab span nodes */
        hsize_t           *abs_arr;          /* Absolute hyperslab span position */
        int                curr_dim;         /* Temporary rank holder */

        /* Set the rank of the fastest changing dimension */
        ndims    = iter->rank;
        fast_dim = (int)ndims - 1;

        /* Get the pointers to the current span info and span nodes */
        abs_arr = iter->u.hyp.off;
        ispan   = iter->u.hyp.span;

        /* Loop through, advancing the span information, until all the nelements are accounted for */
        while (nelem > 0) {
            /* Start at the fastest dim */
            curr_dim = fast_dim;

            /* Work back up through the dimensions */
            while (curr_dim >= 0) {
                /* Reset the current span */
                curr_span = ispan[curr_dim];

                /* Increment absolute position */
                if (curr_dim == fast_dim) {
                    size_t  actual_elem; /* Actual # of elements advanced on each iteration through loop */
                    hsize_t span_elem;   /* Number of elements left in a span */

                    /* Compute the number of elements left in block */
                    span_elem = (curr_span->high - abs_arr[curr_dim]) + 1;

                    /* Compute the number of actual elements to advance */
                    actual_elem = (size_t)MIN(nelem, span_elem);

                    /* Move the iterator over as many elements as possible */
                    abs_arr[curr_dim] += actual_elem;

                    /* Decrement the number of elements advanced */
                    nelem -= actual_elem;
                } /* end if */
                else
                    /* Move to the next row in the current dimension */
                    abs_arr[curr_dim]++;

                /* Check if we are still within the span */
                if (abs_arr[curr_dim] <= curr_span->high)
                    break;
                /* If we walked off that span, advance to the next span */
                else {
                    /* Advance span in this dimension */
                    curr_span = curr_span->next;

                    /* Check if we have a valid span in this dimension still */
                    if (curr_span != NULL) {
                        /* Reset the span in the current dimension */
                        ispan[curr_dim] = curr_span;

                        /* Reset absolute position */
                        abs_arr[curr_dim] = curr_span->low;

                        break;
                    } /* end if */
                    else
                        /* If we finished the span list in this dimension, decrement the dimension worked on
                         * and loop again */
                        curr_dim--;
                } /* end else */
            }     /* end while */

            /* Check if we are finished with the spans in the tree */
            if (curr_dim >= 0) {
                /* Walk back down the iterator positions, resetting them */
                while (curr_dim < fast_dim) {
                    assert(curr_span);
                    assert(curr_span->down);
                    assert(curr_span->down->head);

                    /* Increment current dimension */
                    curr_dim++;

                    /* Set the new span_info & span for this dimension */
                    ispan[curr_dim] = curr_span->down->head;

                    /* Advance span down the tree */
                    curr_span = curr_span->down->head;

                    /* Reset the absolute offset for the dim */
                    abs_arr[curr_dim] = curr_span->low;
                } /* end while */

                /* Verify that the curr_span points to the fastest dim */
                assert(curr_span == ispan[fast_dim]);
            } /* end if */
        }     /* end while */
    }         /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_iter_next() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_iter_next_block
 *
 * Purpose:     Moves a hyperslab iterator to the beginning of the next sequence
 *              of elements to read.  Handles walking off the end in all dimensions.
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_iter_next_block(H5S_sel_iter_t *iter)
{
    unsigned ndims;    /* Number of dimensions of dataset */
    int      fast_dim; /* Rank of the fastest changing dimension for the dataspace */
    unsigned u;        /* Counters */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check for the special case of just one H5Sselect_hyperslab call made */
    /* (i.e. a regular hyperslab selection) */
    if (iter->u.hyp.diminfo_valid) {
        const H5S_hyper_dim_t *tdiminfo; /* Temporary pointer to diminfo information */
        hsize_t                iter_offset[H5S_MAX_RANK];
        hsize_t                iter_count[H5S_MAX_RANK];
        int                    temp_dim; /* Temporary rank holder */

        /* Check if this is a "flattened" regular hyperslab selection */
        if (iter->u.hyp.iter_rank != 0 && iter->u.hyp.iter_rank < iter->rank)
            /* Set the aliases for the dimension rank */
            ndims = iter->u.hyp.iter_rank;
        else
            /* Set the aliases for the dimension rank */
            ndims = iter->rank;

        /* Set the fastest dimension rank */
        fast_dim = (int)ndims - 1;

        /* Set the local copy of the diminfo pointer */
        tdiminfo = iter->u.hyp.diminfo;

        /* Calculate the offset and block count for each dimension */
        for (u = 0; u < ndims; u++) {
            if (tdiminfo[u].count == 1) {
                iter_offset[u] = iter->u.hyp.off[u] - tdiminfo[u].start;
                iter_count[u]  = 0;
            } /* end if */
            else {
                iter_offset[u] = (iter->u.hyp.off[u] - tdiminfo[u].start) % tdiminfo[u].stride;
                iter_count[u]  = (iter->u.hyp.off[u] - tdiminfo[u].start) / tdiminfo[u].stride;
            } /* end else */
        }     /* end for */

        /* Advance one block */
        temp_dim = fast_dim; /* Start with the fastest changing dimension */
        while (temp_dim >= 0) {
            if (temp_dim == fast_dim)
                /* Move iterator over current block */
                iter_offset[temp_dim] += tdiminfo[temp_dim].block;
            else
                /* Move to the next row in the current dimension */
                iter_offset[temp_dim]++;

            /* If this block is still in the range of blocks to output for the dimension, break out of loop */
            if (iter_offset[temp_dim] < tdiminfo[temp_dim].block)
                break;
            else {
                /* Move to the next block in the current dimension */
                iter_offset[temp_dim] = 0;
                iter_count[temp_dim]++;

                /* If this block is still in the range of blocks to output for the dimension, break out of
                 * loop */
                if (iter_count[temp_dim] < tdiminfo[temp_dim].count)
                    break;
                else
                    iter_count[temp_dim] = 0; /* reset back to the beginning of the line */
            }                                 /* end else */

            /* Decrement dimension count */
            temp_dim--;
        } /* end while */

        /* Translate current iter_offset and iter_count into iterator position */
        for (u = 0; u < ndims; u++)
            iter->u.hyp.off[u] = tdiminfo[u].start + (tdiminfo[u].stride * iter_count[u]) + iter_offset[u];
    } /* end if */
    /* Must be an irregular hyperslab selection */
    else {
        H5S_hyper_span_t  *curr_span = NULL; /* Current hyperslab span node */
        H5S_hyper_span_t **ispan;            /* Iterator's hyperslab span nodes */
        hsize_t           *abs_arr;          /* Absolute hyperslab span position */
        int                curr_dim;         /* Temporary rank holder */

        /* Set the rank of the fastest changing dimension */
        ndims    = iter->rank;
        fast_dim = (int)ndims - 1;

        /* Get the pointers to the current span info and span nodes */
        abs_arr = iter->u.hyp.off;
        ispan   = iter->u.hyp.span;

        /* Loop through, advancing the span information, until all the nelements are accounted for */
        curr_dim = fast_dim; /* Start at the fastest dim */

        /* Work back up through the dimensions */
        while (curr_dim >= 0) {
            /* Reset the current span */
            curr_span = ispan[curr_dim];

            /* Increment absolute position */
            if (curr_dim == fast_dim)
                /* Move the iterator over rest of element in span */
                abs_arr[curr_dim] = curr_span->high + 1;
            else
                /* Move to the next row in the current dimension */
                abs_arr[curr_dim]++;

            /* Check if we are still within the span */
            if (abs_arr[curr_dim] <= curr_span->high)
                break;
            /* If we walked off that span, advance to the next span */
            else {
                /* Advance span in this dimension */
                curr_span = curr_span->next;

                /* Check if we have a valid span in this dimension still */
                if (curr_span != NULL) {
                    /* Reset the span in the current dimension */
                    ispan[curr_dim] = curr_span;

                    /* Reset absolute position */
                    abs_arr[curr_dim] = curr_span->low;

                    break;
                } /* end if */
                else
                    /* If we finished the span list in this dimension, decrement the dimension worked on and
                     * loop again */
                    curr_dim--;
            } /* end else */
        }     /* end while */

        /* Check if we are finished with the spans in the tree */
        if (curr_dim >= 0) {
            /* Walk back down the iterator positions, resetting them */
            while (curr_dim < fast_dim) {
                assert(curr_span);
                assert(curr_span->down);
                assert(curr_span->down->head);

                /* Increment current dimension */
                curr_dim++;

                /* Set the new span_info & span for this dimension */
                ispan[curr_dim] = curr_span->down->head;

                /* Advance span down the tree */
                curr_span = curr_span->down->head;

                /* Reset the absolute offset for the dim */
                abs_arr[curr_dim] = curr_span->low;
            } /* end while */

            /* Verify that the curr_span points to the fastest dim */
            assert(curr_span == ispan[fast_dim]);
        } /* end if */
    }     /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_iter_next_block() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_iter_get_seq_list_gen
 PURPOSE
    Create a list of offsets & lengths for a selection
 USAGE
    herr_t H5S__hyper_iter_get_seq_list_gen(iter,maxseq,maxelem,nseq,nelem,off,len)
        H5S_sel_iter_t *iter;   IN/OUT: Selection iterator describing last
                                    position of interest in selection.
        size_t maxseq;          IN: Maximum number of sequences to generate
        size_t maxelem;         IN: Maximum number of elements to include in the
                                    generated sequences
        size_t *nseq;           OUT: Actual number of sequences generated
        size_t *nelem;          OUT: Actual number of elements in sequences generated
        hsize_t *off;           OUT: Array of offsets
        size_t *len;            OUT: Array of lengths
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Use the selection in the dataspace to generate a list of byte offsets and
    lengths for the region(s) selected.  Start/Restart from the position in the
    ITER parameter.  The number of sequences generated is limited by the MAXSEQ
    parameter and the number of sequences actually generated is stored in the
    NSEQ parameter.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_iter_get_seq_list_gen(H5S_sel_iter_t *iter, size_t maxseq, size_t maxelem, size_t *nseq,
                                 size_t *nelem, hsize_t *off, size_t *len)
{
    H5S_hyper_span_t  *curr_span;         /* Current hyperslab span node */
    H5S_hyper_span_t **ispan;             /* Iterator's hyperslab span nodes */
    hsize_t           *slab;              /* Cumulative size of each dimension in bytes */
    hsize_t            loc_off;           /* Byte offset in the dataspace */
    hsize_t            last_span_end = 0; /* The offset of the end of the last span */
    hsize_t           *abs_arr;           /* Absolute hyperslab span position, in elements */
    hsize_t           *loc_arr;           /* Byte offset of hyperslab span position within buffer */
    const hssize_t    *sel_off;           /* Offset within the dataspace extent */
    size_t             span_elmts = 0;    /* Number of elements to actually use for this span */
    size_t             span_size  = 0;    /* Number of bytes in current span to actually process */
    size_t             io_left;           /* Initial number of elements to process */
    size_t             io_elmts_left;     /* Number of elements left to process */
    size_t             io_used;           /* Number of elements processed */
    size_t             curr_seq = 0;      /* Number of sequence/offsets stored in the arrays */
    size_t             elem_size;         /* Size of each element iterating over */
    unsigned           ndims;             /* Number of dimensions of dataset */
    unsigned           fast_dim;          /* Rank of the fastest changing dimension for the dataspace */
    int                curr_dim;          /* Current dimension being operated on */
    unsigned           u;                 /* Index variable */
    herr_t             ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(iter);
    assert(maxseq > 0);
    assert(maxelem > 0);
    assert(nseq);
    assert(nelem);
    assert(off);
    assert(len);

    /* Set the rank of the fastest changing dimension */
    ndims    = iter->rank;
    fast_dim = (ndims - 1);

    /* Get the pointers to the current span info and span nodes */
    curr_span = iter->u.hyp.span[fast_dim];
    abs_arr   = iter->u.hyp.off;
    loc_arr   = iter->u.hyp.loc_off;
    slab      = iter->u.hyp.slab;
    sel_off   = iter->sel_off;
    ispan     = iter->u.hyp.span;
    elem_size = iter->elmt_size;

    /* Set the amount of elements to perform I/O on, etc. */
    H5_CHECK_OVERFLOW(iter->elmt_left, hsize_t, size_t);
    io_elmts_left = io_left = MIN(maxelem, (size_t)iter->elmt_left);

    /* Set the offset of the first element iterated on */
    for (u = 0, loc_off = 0; u < ndims; u++)
        loc_off += loc_arr[u];

    /* Take care of any partial spans leftover from previous I/Os */
    if (abs_arr[fast_dim] != curr_span->low) {
        /* Finish the span in the fastest changing dimension */

        /* Compute the number of elements to attempt in this span */
        H5_CHECKED_ASSIGN(span_elmts, size_t, ((curr_span->high - abs_arr[fast_dim]) + 1), hsize_t);

        /* Check number of elements against upper bounds allowed */
        if (span_elmts > io_elmts_left)
            span_elmts = io_elmts_left;

        /* Set the span_size, in bytes */
        span_size = span_elmts * elem_size;

        /* Add the partial span to the list of sequences */
        off[curr_seq] = loc_off;
        len[curr_seq] = span_size;

        /* Increment sequence count */
        curr_seq++;

        /* Set the location of the last span's end */
        last_span_end = loc_off + span_size;

        /* Decrement I/O left to perform */
        io_elmts_left -= span_elmts;

        /* Check if we are done */
        if (io_elmts_left > 0) {
            /* Move to next span in fastest changing dimension */
            curr_span = curr_span->next;

            if (NULL != curr_span) {
                /* Move location offset of destination */
                loc_off += (curr_span->low - abs_arr[fast_dim]) * elem_size;

                /* Move iterator for fastest changing dimension */
                abs_arr[fast_dim] = curr_span->low;
                loc_arr[fast_dim] =
                    ((hsize_t)((hssize_t)curr_span->low + sel_off[fast_dim])) * slab[fast_dim];
                ispan[fast_dim] = curr_span;
            } /* end if */
        }     /* end if */
        else {
            /* Advance the hyperslab iterator */
            abs_arr[fast_dim] += span_elmts;

            /* Check if we are still within the span */
            if (abs_arr[fast_dim] <= curr_span->high) {
                /* Sanity check */
                assert(ispan[fast_dim] == curr_span);

                /* Update byte offset */
                loc_arr[fast_dim] += span_size;
            } /* end if */
            /* If we walked off that span, advance to the next span */
            else {
                /* Advance span in this dimension */
                curr_span = curr_span->next;

                /* Check if we have a valid span in this dimension still */
                if (NULL != curr_span) {
                    /* Reset absolute position */
                    abs_arr[fast_dim] = curr_span->low;

                    /* Update location offset */
                    loc_arr[fast_dim] =
                        ((hsize_t)((hssize_t)curr_span->low + sel_off[fast_dim])) * slab[fast_dim];

                    /* Reset the span in the current dimension */
                    ispan[fast_dim] = curr_span;
                } /* end if */
            }     /* end else */
        }         /* end else */

        /* Adjust iterator pointers */

        if (NULL == curr_span) {
            /* Same as code in main loop */
            /* Start at the next fastest dim */
            curr_dim = (int)(fast_dim - 1);

            /* Work back up through the dimensions */
            while (curr_dim >= 0) {
                /* Reset the current span */
                curr_span = ispan[curr_dim];

                /* Increment absolute position */
                abs_arr[curr_dim]++;

                /* Check if we are still within the span */
                if (abs_arr[curr_dim] <= curr_span->high) {
                    /* Update location offset */
                    loc_arr[curr_dim] += slab[curr_dim];

                    break;
                } /* end if */
                /* If we walked off that span, advance to the next span */
                else {
                    /* Advance span in this dimension */
                    curr_span = curr_span->next;

                    /* Check if we have a valid span in this dimension still */
                    if (NULL != curr_span) {
                        /* Reset the span in the current dimension */
                        ispan[curr_dim] = curr_span;

                        /* Reset absolute position */
                        abs_arr[curr_dim] = curr_span->low;

                        /* Update byte location */
                        loc_arr[curr_dim] =
                            ((hsize_t)((hssize_t)curr_span->low + sel_off[curr_dim])) * slab[curr_dim];

                        break;
                    } /* end if */
                    else
                        /* If we finished the span list in this dimension, decrement the dimension worked on
                         * and loop again */
                        curr_dim--;
                } /* end else */
            }     /* end while */

            /* Check if we have more spans in the tree */
            if (curr_dim >= 0) {
                /* Walk back down the iterator positions, resetting them */
                while ((unsigned)curr_dim < fast_dim) {
                    assert(curr_span);
                    assert(curr_span->down);
                    assert(curr_span->down->head);

                    /* Increment current dimension */
                    curr_dim++;

                    /* Set the new span_info & span for this dimension */
                    ispan[curr_dim] = curr_span->down->head;

                    /* Advance span down the tree */
                    curr_span = curr_span->down->head;

                    /* Reset the absolute offset for the dim */
                    abs_arr[curr_dim] = curr_span->low;

                    /* Update the location offset */
                    loc_arr[curr_dim] =
                        ((hsize_t)((hssize_t)curr_span->low + sel_off[curr_dim])) * slab[curr_dim];
                } /* end while */

                /* Verify that the curr_span points to the fastest dim */
                assert(curr_span == ispan[fast_dim]);

                /* Reset the buffer offset */
                for (u = 0, loc_off = 0; u < ndims; u++)
                    loc_off += loc_arr[u];
            } /* end else */
            else
                /* We had better be done with I/O or bad things are going to happen... */
                assert(io_elmts_left == 0);
        } /* end if */
    }     /* end if */

    /* Perform the I/O on the elements, based on the position of the iterator */
    while (io_elmts_left > 0 && curr_seq < maxseq) {
        H5S_hyper_span_t *prev_span; /* Previous hyperslab span node */

        /* Sanity check */
        assert(curr_span);

        /* Set to current span, so the first adjustment to loc_off is 0 */
        prev_span = curr_span;

        /* Loop over all the spans in the fastest changing dimension */
        while (curr_span != NULL) {
            hsize_t nelmts; /* # of elements covered by current span */

            /* Move location offset of current span */
            loc_off += (curr_span->low - prev_span->low) * elem_size;

            /* Compute the number of elements to attempt in this span */
            nelmts = (curr_span->high - curr_span->low) + 1;
            H5_CHECKED_ASSIGN(span_elmts, size_t, nelmts, hsize_t);

            /* Check number of elements against upper bounds allowed */
            if (span_elmts >= io_elmts_left) {
                /* Trim the number of elements to output */
                span_elmts    = io_elmts_left;
                span_size     = span_elmts * elem_size;
                io_elmts_left = 0;

                /* COMMON */
                /* Store the I/O information for the span */

                /* Check if this is appending onto previous sequence */
                if (curr_seq > 0 && last_span_end == loc_off)
                    len[curr_seq - 1] += span_size;
                else {
                    off[curr_seq] = loc_off;
                    len[curr_seq] = span_size;

                    /* Increment the number of sequences in arrays */
                    curr_seq++;
                } /* end else */
                  /* end COMMON */

                /* Break out now, we are finished with I/O */
                break;
            } /* end if */
            else {
                /* Decrement I/O left to perform */
                span_size = span_elmts * elem_size;
                io_elmts_left -= span_elmts;

                /* COMMON */
                /* Store the I/O information for the span */

                /* Check if this is appending onto previous sequence */
                if (curr_seq > 0 && last_span_end == loc_off)
                    len[curr_seq - 1] += span_size;
                else {
                    off[curr_seq] = loc_off;
                    len[curr_seq] = span_size;

                    /* Increment the number of sequences in arrays */
                    curr_seq++;
                } /* end else */
                  /* end COMMON */

                /* If the sequence & offset arrays are full, do what? */
                if (curr_seq >= maxseq)
                    /* Break out now, we are finished with sequences */
                    break;
            } /* end else */

            /* Set the location of the last span's end */
            last_span_end = loc_off + span_size;

            /* Move to next span in fastest changing dimension */
            prev_span = curr_span;
            curr_span = curr_span->next;
        } /* end while */

        /* Check if we are done */
        if (io_elmts_left == 0 || curr_seq >= maxseq) {
            /* Sanity checks */
            if (!curr_span)
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "curr_span pointer was NULL");

            /* Update absolute position */
            abs_arr[fast_dim] = curr_span->low + span_elmts;

            /* Check if we are still within the span */
            if (abs_arr[fast_dim] <= curr_span->high) {
                /* Reset the span for the fast dimension */
                ispan[fast_dim] = curr_span;

                /* Update location offset */
                loc_arr[fast_dim] =
                    ((hsize_t)((hssize_t)curr_span->low + (hssize_t)span_elmts + sel_off[fast_dim])) *
                    slab[fast_dim];

                break;
            } /* end if */
            /* If we walked off that span, advance to the next span */
            else {
                /* Advance span in this dimension */
                curr_span = curr_span->next;

                /* Check if we have a valid span in this dimension still */
                if (curr_span != NULL) {
                    /* Reset absolute position */
                    abs_arr[fast_dim] = curr_span->low;
                    loc_arr[fast_dim] =
                        ((hsize_t)((hssize_t)curr_span->low + sel_off[fast_dim])) * slab[fast_dim];
                    ispan[fast_dim] = curr_span;

                    break;
                } /* end if */
            }     /* end else */
        }         /* end if */

        /* Adjust iterator pointers */

        /* Start at the next fastest dim */
        curr_dim = (int)(fast_dim - 1);

        /* Work back up through the dimensions */
        while (curr_dim >= 0) {
            /* Reset the current span */
            curr_span = ispan[curr_dim];

            /* Increment absolute position */
            abs_arr[curr_dim]++;

            /* Check if we are still within the span */
            if (abs_arr[curr_dim] <= curr_span->high) {
                /* Update location offset */
                loc_arr[curr_dim] += slab[curr_dim];

                break;
            } /* end if */
            /* If we walked off that span, advance to the next span */
            else {
                /* Advance span in this dimension */
                curr_span = curr_span->next;

                /* Check if we have a valid span in this dimension still */
                if (curr_span != NULL) {
                    /* Reset the span in the current dimension */
                    ispan[curr_dim] = curr_span;

                    /* Reset absolute position */
                    abs_arr[curr_dim] = curr_span->low;

                    /* Update location offset */
                    loc_arr[curr_dim] =
                        ((hsize_t)((hssize_t)curr_span->low + sel_off[curr_dim])) * slab[curr_dim];

                    break;
                } /* end if */
                else
                    /* If we finished the span list in this dimension, decrement the dimension worked on and
                     * loop again */
                    curr_dim--;
            } /* end else */
        }     /* end while */

        /* Check if we are finished with the spans in the tree */
        if (curr_dim < 0) {
            /* We had better be done with I/O or bad things are going to happen... */
            assert(io_elmts_left == 0);
            break;
        } /* end if */
        else {
            /* Walk back down the iterator positions, resetting them */
            while ((unsigned)curr_dim < fast_dim) {
                assert(curr_span);
                assert(curr_span->down);
                assert(curr_span->down->head);

                /* Increment current dimension to the next dimension down */
                curr_dim++;

                /* Set the new span for the next dimension down */
                ispan[curr_dim] = curr_span->down->head;

                /* Advance span down the tree */
                curr_span = curr_span->down->head;

                /* Reset the absolute offset for the dim */
                abs_arr[curr_dim] = curr_span->low;

                /* Update location offset */
                loc_arr[curr_dim] =
                    ((hsize_t)((hssize_t)curr_span->low + sel_off[curr_dim])) * slab[curr_dim];
            } /* end while */

            /* Verify that the curr_span points to the fastest dim */
            assert(curr_span == ispan[fast_dim]);
        } /* end else */

        /* Reset the buffer offset */
        for (u = 0, loc_off = 0; u < ndims; u++)
            loc_off += loc_arr[u];
    } /* end while */

    /* Decrement number of elements left in iterator */
    io_used = io_left - io_elmts_left;
    iter->elmt_left -= io_used;

    /* Set the number of sequences generated */
    *nseq = curr_seq;

    /* Set the number of elements used */
    *nelem = io_used;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_iter_get_seq_list_gen() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_iter_get_seq_list_opt
 PURPOSE
    Create a list of offsets & lengths for a selection
 USAGE
    herr_t H5S__hyper_iter_get_seq_list_opt(iter,maxseq,maxelem,nseq,nelem,off,len)
        H5S_sel_iter_t *iter;   IN/OUT: Selection iterator describing last
                                    position of interest in selection.
        size_t maxseq;          IN: Maximum number of sequences to generate
        size_t maxelem;         IN: Maximum number of elements to include in the
                                    generated sequences
        size_t *nseq;           OUT: Actual number of sequences generated
        size_t *nelem;          OUT: Actual number of elements in sequences generated
        hsize_t *off;           OUT: Array of offsets
        size_t *len;            OUT: Array of lengths
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    Use the selection in the dataspace to generate a list of byte offsets and
    lengths for the region(s) selected.  Start/Restart from the position in the
    ITER parameter.  The number of sequences generated is limited by the MAXSEQ
    parameter and the number of sequences actually generated is stored in the
    NSEQ parameter.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_iter_get_seq_list_opt(H5S_sel_iter_t *iter, size_t maxseq, size_t maxelem, size_t *nseq,
                                 size_t *nelem, hsize_t *off, size_t *len)
{
    hsize_t               *mem_size;                /* Size of the source buffer */
    hsize_t               *slab;                    /* Hyperslab size */
    const hssize_t        *sel_off;                 /* Selection offset in dataspace */
    hsize_t                offset[H5S_MAX_RANK];    /* Coordinate offset in dataspace */
    hsize_t                tmp_count[H5S_MAX_RANK]; /* Temporary block count */
    hsize_t                tmp_block[H5S_MAX_RANK]; /* Temporary block offset */
    hsize_t                wrap[H5S_MAX_RANK];      /* Bytes to wrap around at the end of a row */
    hsize_t                skip[H5S_MAX_RANK];      /* Bytes to skip between blocks */
    const H5S_hyper_dim_t *tdiminfo;                /* Temporary pointer to diminfo information */
    hsize_t                fast_dim_start,          /* Local copies of fastest changing dimension info */
        fast_dim_stride, fast_dim_block, fast_dim_offset;
    size_t   fast_dim_buf_off; /* Local copy of amount to move fastest dimension buffer offset */
    size_t   fast_dim_count;   /* Number of blocks left in fastest changing dimension */
    size_t   tot_blk_count;    /* Total number of blocks left to output */
    size_t   act_blk_count;    /* Actual number of blocks to output */
    size_t   total_rows;       /* Total number of entire rows to output */
    size_t   curr_rows;        /* Current number of entire rows to output */
    unsigned fast_dim;         /* Rank of the fastest changing dimension for the dataspace */
    unsigned ndims;            /* Number of dimensions of dataset */
    int      temp_dim;         /* Temporary rank holder */
    hsize_t  loc;              /* Coordinate offset */
    size_t   curr_seq = 0;     /* Current sequence being operated on */
    size_t   actual_elem;      /* The actual number of elements to count */
    size_t   actual_bytes;     /* The actual number of bytes to copy */
    size_t   io_left;          /* The number of elements left in I/O operation */
    size_t   start_io_left;    /* The initial number of elements left in I/O operation */
    size_t   elem_size;        /* Size of each element iterating over */
    unsigned u;                /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);
    assert(maxseq > 0);
    assert(maxelem > 0);
    assert(nseq);
    assert(nelem);
    assert(off);
    assert(len);

    /* Set the local copy of the diminfo pointer */
    tdiminfo = iter->u.hyp.diminfo;

    /* Check if this is a "flattened" regular hyperslab selection */
    if (iter->u.hyp.iter_rank != 0 && iter->u.hyp.iter_rank < iter->rank) {
        /* Set the aliases for a few important dimension ranks */
        ndims = iter->u.hyp.iter_rank;

        /* Set the local copy of the selection offset */
        sel_off = iter->u.hyp.sel_off;

        /* Set up the pointer to the size of the memory dataspace */
        mem_size = iter->u.hyp.size;
    } /* end if */
    else {
        /* Set the aliases for a few important dimension ranks */
        ndims = iter->rank;

        /* Set the local copy of the selection offset */
        sel_off = iter->sel_off;

        /* Set up the pointer to the size of the memory dataspace */
        mem_size = iter->dims;
    } /* end else */

    /* Set up some local variables */
    fast_dim  = ndims - 1;
    elem_size = iter->elmt_size;
    slab      = iter->u.hyp.slab;

    /* Calculate the number of elements to sequence through */
    H5_CHECK_OVERFLOW(iter->elmt_left, hsize_t, size_t);
    io_left = MIN((size_t)iter->elmt_left, maxelem);

    /* Sanity check that there aren't any "remainder" sequences in process */
    assert(!((iter->u.hyp.off[fast_dim] - tdiminfo[fast_dim].start) % tdiminfo[fast_dim].stride != 0 ||
             ((iter->u.hyp.off[fast_dim] != tdiminfo[fast_dim].start) && tdiminfo[fast_dim].count == 1)));

    /* We've cleared the "remainder" of the previous fastest dimension
     * sequence before calling this routine, so we must be at the beginning of
     * a sequence.  Use the fancy algorithm to compute the offsets and run
     * through as many as possible, until the buffer fills up.
     */

    /* Keep the number of elements we started with */
    start_io_left = io_left;

    /* Compute the arrays to perform I/O on */

    /* Copy the location of the point to get */
    /* (Add in the selection offset) */
    for (u = 0; u < ndims; u++)
        offset[u] = (hsize_t)((hssize_t)iter->u.hyp.off[u] + sel_off[u]);

    /* Compute the current "counts" for this location */
    for (u = 0; u < ndims; u++) {
        if (tdiminfo[u].count == 1) {
            tmp_count[u] = 0;
            tmp_block[u] = iter->u.hyp.off[u] - tdiminfo[u].start;
        } /* end if */
        else {
            tmp_count[u] = (iter->u.hyp.off[u] - tdiminfo[u].start) / tdiminfo[u].stride;
            tmp_block[u] = (iter->u.hyp.off[u] - tdiminfo[u].start) % tdiminfo[u].stride;
        } /* end else */
    }     /* end for */

    /* Compute the initial buffer offset */
    for (u = 0, loc = 0; u < ndims; u++)
        loc += offset[u] * slab[u];

    /* Set the number of elements to write each time */
    H5_CHECKED_ASSIGN(actual_elem, size_t, tdiminfo[fast_dim].block, hsize_t);

    /* Set the number of actual bytes */
    actual_bytes = actual_elem * elem_size;

    /* Set local copies of information for the fastest changing dimension */
    fast_dim_start  = tdiminfo[fast_dim].start;
    fast_dim_stride = tdiminfo[fast_dim].stride;
    fast_dim_block  = tdiminfo[fast_dim].block;
    H5_CHECKED_ASSIGN(fast_dim_buf_off, size_t, slab[fast_dim] * fast_dim_stride, hsize_t);
    fast_dim_offset = (hsize_t)((hssize_t)fast_dim_start + sel_off[fast_dim]);

    /* Compute the number of blocks which would fit into the buffer */
    H5_CHECK_OVERFLOW(io_left / fast_dim_block, hsize_t, size_t);
    tot_blk_count = (size_t)(io_left / fast_dim_block);

    /* Don't go over the maximum number of sequences allowed */
    tot_blk_count = MIN(tot_blk_count, (maxseq - curr_seq));

    /* Compute the amount to wrap at the end of each row */
    for (u = 0; u < ndims; u++)
        wrap[u] = (mem_size[u] - (tdiminfo[u].stride * tdiminfo[u].count)) * slab[u];

    /* Compute the amount to skip between blocks */
    for (u = 0; u < ndims; u++)
        skip[u] = (tdiminfo[u].stride - tdiminfo[u].block) * slab[u];

    /* Check if there is a partial row left (with full blocks) */
    if (tmp_count[fast_dim] > 0) {
        /* Get number of blocks in fastest dimension */
        H5_CHECKED_ASSIGN(fast_dim_count, size_t, tdiminfo[fast_dim].count - tmp_count[fast_dim], hsize_t);

        /* Make certain this entire row will fit into buffer */
        fast_dim_count = MIN(fast_dim_count, tot_blk_count);

        /* Number of blocks to sequence over */
        act_blk_count = fast_dim_count;

        /* Loop over all the blocks in the fastest changing dimension */
        while (fast_dim_count > 0) {
            /* Store the sequence information */
            off[curr_seq] = loc;
            len[curr_seq] = actual_bytes;

            /* Increment sequence count */
            curr_seq++;

            /* Increment information to reflect block just processed */
            loc += fast_dim_buf_off;

            /* Decrement number of blocks */
            fast_dim_count--;
        } /* end while */

        /* Decrement number of elements left */
        io_left -= actual_elem * act_blk_count;

        /* Decrement number of blocks left */
        tot_blk_count -= act_blk_count;

        /* Increment information to reflect block just processed */
        tmp_count[fast_dim] += act_blk_count;

        /* Check if we finished the entire row of blocks */
        if (tmp_count[fast_dim] >= tdiminfo[fast_dim].count) {
            /* Increment offset in destination buffer */
            loc += wrap[fast_dim];

            /* Increment information to reflect block just processed */
            offset[fast_dim]    = fast_dim_offset; /* reset the offset in the fastest dimension */
            tmp_count[fast_dim] = 0;

            /* Increment the offset and count for the other dimensions */
            temp_dim = (int)fast_dim - 1;
            while (temp_dim >= 0) {
                /* Move to the next row in the current dimension */
                offset[temp_dim]++;
                tmp_block[temp_dim]++;

                /* If this block is still in the range of blocks to output for the dimension, break out of
                 * loop */
                if (tmp_block[temp_dim] < tdiminfo[temp_dim].block)
                    break;
                else {
                    /* Move to the next block in the current dimension */
                    offset[temp_dim] += (tdiminfo[temp_dim].stride - tdiminfo[temp_dim].block);
                    loc += skip[temp_dim];
                    tmp_block[temp_dim] = 0;
                    tmp_count[temp_dim]++;

                    /* If this block is still in the range of blocks to output for the dimension, break out of
                     * loop */
                    if (tmp_count[temp_dim] < tdiminfo[temp_dim].count)
                        break;
                    else {
                        offset[temp_dim] = (hsize_t)((hssize_t)tdiminfo[temp_dim].start + sel_off[temp_dim]);
                        loc += wrap[temp_dim];
                        tmp_count[temp_dim] = 0; /* reset back to the beginning of the line */
                        tmp_block[temp_dim] = 0;
                    } /* end else */
                }     /* end else */

                /* Decrement dimension count */
                temp_dim--;
            } /* end while */
        }     /* end if */
        else {
            /* Update the offset in the fastest dimension */
            offset[fast_dim] += (fast_dim_stride * act_blk_count);
        } /* end else */
    }     /* end if */

    /* Compute the number of entire rows to read in */
    H5_CHECK_OVERFLOW(tot_blk_count / tdiminfo[fast_dim].count, hsize_t, size_t);
    curr_rows = total_rows = (size_t)(tot_blk_count / tdiminfo[fast_dim].count);

    /* Reset copy of number of blocks in fastest dimension */
    H5_CHECKED_ASSIGN(fast_dim_count, size_t, tdiminfo[fast_dim].count, hsize_t);

    /* Read in data until an entire sequence can't be written out any longer */
    while (curr_rows > 0) {

#define DUFF_GUTS                                                                                            \
    /* Store the sequence information */                                                                     \
    off[curr_seq] = loc;                                                                                     \
    len[curr_seq] = actual_bytes;                                                                            \
                                                                                                             \
    /* Increment sequence count */                                                                           \
    curr_seq++;                                                                                              \
                                                                                                             \
    /* Increment information to reflect block just processed */                                              \
    loc += fast_dim_buf_off;

#ifdef NO_DUFFS_DEVICE
        /* Loop over all the blocks in the fastest changing dimension */
        while (fast_dim_count > 0) {
            DUFF_GUTS

            /* Decrement number of blocks */
            fast_dim_count--;
        } /* end while */
#else     /* NO_DUFFS_DEVICE */
        {
            size_t duffs_index; /* Counting index for Duff's device */

            duffs_index = (fast_dim_count + 7) / 8;
            switch (fast_dim_count % 8) {
                default:
                    assert(0 && "This Should never be executed!");
                    break;
                case 0:
                    do {
                        DUFF_GUTS
                        /* FALLTHROUGH */
                        H5_ATTR_FALLTHROUGH
                        case 7:
                            DUFF_GUTS
                            /* FALLTHROUGH */
                            H5_ATTR_FALLTHROUGH
                        case 6:
                            DUFF_GUTS
                            /* FALLTHROUGH */
                            H5_ATTR_FALLTHROUGH
                        case 5:
                            DUFF_GUTS
                            /* FALLTHROUGH */
                            H5_ATTR_FALLTHROUGH
                        case 4:
                            DUFF_GUTS
                            /* FALLTHROUGH */
                            H5_ATTR_FALLTHROUGH
                        case 3:
                            DUFF_GUTS
                            /* FALLTHROUGH */
                            H5_ATTR_FALLTHROUGH
                        case 2:
                            DUFF_GUTS
                            /* FALLTHROUGH */
                            H5_ATTR_FALLTHROUGH
                        case 1:
                            DUFF_GUTS
                    } while (--duffs_index > 0);
            } /* end switch */
        }
#endif    /* NO_DUFFS_DEVICE */
#undef DUFF_GUTS

        /* Increment offset in destination buffer */
        loc += wrap[fast_dim];

        /* Increment the offset and count for the other dimensions */
        temp_dim = (int)fast_dim - 1;
        while (temp_dim >= 0) {
            /* Move to the next row in the current dimension */
            offset[temp_dim]++;
            tmp_block[temp_dim]++;

            /* If this block is still in the range of blocks to output for the dimension, break out of loop */
            if (tmp_block[temp_dim] < tdiminfo[temp_dim].block)
                break;
            else {
                /* Move to the next block in the current dimension */
                offset[temp_dim] += (tdiminfo[temp_dim].stride - tdiminfo[temp_dim].block);
                loc += skip[temp_dim];
                tmp_block[temp_dim] = 0;
                tmp_count[temp_dim]++;

                /* If this block is still in the range of blocks to output for the dimension, break out of
                 * loop */
                if (tmp_count[temp_dim] < tdiminfo[temp_dim].count)
                    break;
                else {
                    offset[temp_dim] = (hsize_t)((hssize_t)tdiminfo[temp_dim].start + sel_off[temp_dim]);
                    loc += wrap[temp_dim];
                    tmp_count[temp_dim] = 0; /* reset back to the beginning of the line */
                    tmp_block[temp_dim] = 0;
                } /* end else */
            }     /* end else */

            /* Decrement dimension count */
            temp_dim--;
        } /* end while */

        /* Decrement the number of rows left */
        curr_rows--;
    } /* end while */

    /* Adjust the number of blocks & elements left to transfer */

    /* Decrement number of elements left */
    H5_CHECK_OVERFLOW(actual_elem * (total_rows * tdiminfo[fast_dim].count), hsize_t, size_t);
    io_left -= (size_t)(actual_elem * (total_rows * tdiminfo[fast_dim].count));

    /* Decrement number of blocks left */
    H5_CHECK_OVERFLOW((total_rows * tdiminfo[fast_dim].count), hsize_t, size_t);
    tot_blk_count -= (size_t)(total_rows * tdiminfo[fast_dim].count);

    /* Read in partial row of blocks */
    if (io_left > 0 && curr_seq < maxseq) {
        /* Get remaining number of blocks left to output */
        fast_dim_count = tot_blk_count;

        /* Loop over all the blocks in the fastest changing dimension */
        while (fast_dim_count > 0) {
            /* Store the sequence information */
            off[curr_seq] = loc;
            len[curr_seq] = actual_bytes;

            /* Increment sequence count */
            curr_seq++;

            /* Increment information to reflect block just processed */
            loc += fast_dim_buf_off;

            /* Decrement number of blocks */
            fast_dim_count--;
        } /* end while */

        /* Decrement number of elements left */
        io_left -= actual_elem * tot_blk_count;

        /* Increment information to reflect block just processed */
        offset[fast_dim] += (fast_dim_stride * tot_blk_count); /* move the offset in the fastest dimension */

        /* Handle any leftover, partial blocks in this row */
        if (io_left > 0 && curr_seq < maxseq) {
            actual_elem  = io_left;
            actual_bytes = actual_elem * elem_size;

            /* Store the sequence information */
            off[curr_seq] = loc;
            len[curr_seq] = actual_bytes;

            /* Increment sequence count */
            curr_seq++;

            /* Decrement the number of elements left */
            io_left -= actual_elem;

            /* Increment buffer correctly */
            offset[fast_dim] += actual_elem;
        } /* end if */

        /* don't bother checking slower dimensions */
        assert(io_left == 0 || curr_seq == maxseq);
    } /* end if */

    /* Update the iterator */

    /* Update the iterator with the location we stopped */
    /* (Subtract out the selection offset) */
    for (u = 0; u < ndims; u++)
        iter->u.hyp.off[u] = (hsize_t)((hssize_t)offset[u] - sel_off[u]);

    /* Decrement the number of elements left in selection */
    iter->elmt_left -= (start_io_left - io_left);

    /* Increment the number of sequences generated */
    *nseq += curr_seq;

    /* Increment the number of elements used */
    *nelem += start_io_left - io_left;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_iter_get_seq_list_opt() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_iter_get_seq_list_single
 PURPOSE
    Create a list of offsets & lengths for a selection
 USAGE
    herr_t H5S__hyper_iter_get_seq_list_single(flags, iter, maxseq, maxelem, nseq, nelem, off, len)
        unsigned flags;         IN: Flags for extra information about operation
        H5S_sel_iter_t *iter;   IN/OUT: Selection iterator describing last
                                    position of interest in selection.
        size_t maxseq;          IN: Maximum number of sequences to generate
        size_t maxelem;         IN: Maximum number of elements to include in the
                                    generated sequences
        size_t *nseq;           OUT: Actual number of sequences generated
        size_t *nelem;          OUT: Actual number of elements in sequences generated
        hsize_t *off;           OUT: Array of offsets
        size_t *len;            OUT: Array of lengths
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    Use the selection in the dataspace to generate a list of byte offsets and
    lengths for the region(s) selected.  Start/Restart from the position in the
    ITER parameter.  The number of sequences generated is limited by the MAXSEQ
    parameter and the number of sequences actually generated is stored in the
    NSEQ parameter.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_iter_get_seq_list_single(H5S_sel_iter_t *iter, size_t maxseq, size_t maxelem, size_t *nseq,
                                    size_t *nelem, hsize_t *off, size_t *len)
{
    const H5S_hyper_dim_t *tdiminfo;                  /* Temporary pointer to diminfo information */
    const hssize_t        *sel_off;                   /* Selection offset in dataspace */
    hsize_t               *mem_size;                  /* Size of the source buffer */
    hsize_t                base_offset[H5S_MAX_RANK]; /* Base coordinate offset in dataspace */
    hsize_t                offset[H5S_MAX_RANK];      /* Coordinate offset in dataspace */
    hsize_t               *slab;                      /* Hyperslab size */
    hsize_t                fast_dim_block;            /* Local copies of fastest changing dimension info */
    hsize_t                loc;                       /* Coordinate offset */
    size_t                 tot_blk_count;             /* Total number of blocks left to output */
    size_t                 elem_size;                 /* Size of each element iterating over */
    size_t                 io_left;                   /* The number of elements left in I/O operation */
    size_t                 actual_elem;               /* The actual number of elements to count */
    unsigned               ndims;                     /* Number of dimensions of dataset */
    unsigned               fast_dim; /* Rank of the fastest changing dimension for the dataspace */
    unsigned               skip_dim; /* Rank of the dimension to skip along */
    unsigned               u;        /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);
    assert(maxseq > 0);
    assert(maxelem > 0);
    assert(nseq);
    assert(nelem);
    assert(off);
    assert(len);

    /* Set a local copy of the diminfo pointer */
    tdiminfo = iter->u.hyp.diminfo;

    /* Check if this is a "flattened" regular hyperslab selection */
    if (iter->u.hyp.iter_rank != 0 && iter->u.hyp.iter_rank < iter->rank) {
        /* Set the aliases for a few important dimension ranks */
        ndims = iter->u.hyp.iter_rank;

        /* Set the local copy of the selection offset */
        sel_off = iter->u.hyp.sel_off;

        /* Set up the pointer to the size of the memory dataspace */
        mem_size = iter->u.hyp.size;
    } /* end if */
    else {
        /* Set the aliases for a few important dimension ranks */
        ndims = iter->rank;

        /* Set the local copy of the selection offset */
        sel_off = iter->sel_off;

        /* Set up the pointer to the size of the memory dataspace */
        mem_size = iter->dims;
    } /* end else */

    /* Set up some local variables */
    fast_dim  = ndims - 1;
    elem_size = iter->elmt_size;
    slab      = iter->u.hyp.slab;

    /* Copy the base location of the block */
    /* (Add in the selection offset) */
    for (u = 0; u < ndims; u++)
        base_offset[u] = (hsize_t)((hssize_t)tdiminfo[u].start + sel_off[u]);

    /* Copy the location of the point to get */
    /* (Add in the selection offset) */
    for (u = 0; u < ndims; u++)
        offset[u] = (hsize_t)((hssize_t)iter->u.hyp.off[u] + sel_off[u]);

    /* Compute the initial buffer offset */
    for (u = 0, loc = 0; u < ndims; u++)
        loc += offset[u] * slab[u];

    /* Set local copies of information for the fastest changing dimension */
    fast_dim_block = tdiminfo[fast_dim].block;

    /* Calculate the number of elements to sequence through */
    H5_CHECK_OVERFLOW(iter->elmt_left, hsize_t, size_t);
    io_left = MIN((size_t)iter->elmt_left, maxelem);

    /* Compute the number of blocks which would fit into the buffer */
    H5_CHECK_OVERFLOW(io_left / fast_dim_block, hsize_t, size_t);
    tot_blk_count = (size_t)(io_left / fast_dim_block);

    /* Don't go over the maximum number of sequences allowed */
    tot_blk_count = MIN(tot_blk_count, maxseq);

    /* Set the number of elements to write each time */
    H5_CHECKED_ASSIGN(actual_elem, size_t, fast_dim_block, hsize_t);

    /* Check for blocks to operate on */
    if (tot_blk_count > 0) {
        size_t actual_bytes; /* The actual number of bytes to copy */

        /* Set the number of actual bytes */
        actual_bytes = actual_elem * elem_size;

        /* Check for 1-dim selection */
        if (0 == fast_dim) {
            /* Sanity checks */
            assert(1 == tot_blk_count);
            assert(io_left == actual_elem);

            /* Store the sequence information */
            *off++ = loc;
            *len++ = actual_bytes;
        } /* end if */
        else {
            hsize_t skip_slab; /* Temporary copy of slab[fast_dim - 1] */
            size_t  blk_count; /* Total number of blocks left to output */
            int     i;         /* Local index variable */

            /* Find first dimension w/block >1 */
            skip_dim = fast_dim;
            for (i = (int)(fast_dim - 1); i >= 0; i--)
                if (tdiminfo[i].block > 1) {
                    skip_dim = (unsigned)i;
                    break;
                } /* end if */
            skip_slab = slab[skip_dim];

            /* Check for being able to use fast algorithm for 1-D */
            if (0 == skip_dim) {
                /* Create sequences until an entire row can't be used */
                blk_count = tot_blk_count;
                while (blk_count > 0) {
                    /* Store the sequence information */
                    *off++ = loc;
                    *len++ = actual_bytes;

                    /* Increment offset in destination buffer */
                    loc += skip_slab;

                    /* Decrement block count */
                    blk_count--;
                } /* end while */

                /* Move to the next location */
                offset[skip_dim] += tot_blk_count;
            } /* end if */
            else {
                hsize_t tmp_block[H5S_MAX_RANK]; /* Temporary block offset */
                hsize_t skip[H5S_MAX_RANK];      /* Bytes to skip between blocks */
                int     temp_dim;                /* Temporary rank holder */

                /* Set the starting block location */
                for (u = 0; u < ndims; u++)
                    tmp_block[u] = iter->u.hyp.off[u] - tdiminfo[u].start;

                /* Compute the amount to skip between sequences */
                for (u = 0; u < ndims; u++)
                    skip[u] = (mem_size[u] - tdiminfo[u].block) * slab[u];

                /* Create sequences until an entire row can't be used */
                blk_count = tot_blk_count;
                while (blk_count > 0) {
                    /* Store the sequence information */
                    *off++ = loc;
                    *len++ = actual_bytes;

                    /* Set temporary dimension for advancing offsets */
                    temp_dim = (int)skip_dim;

                    /* Increment offset in destination buffer */
                    loc += skip_slab;

                    /* Increment the offset and count for the other dimensions */
                    while (temp_dim >= 0) {
                        /* Move to the next row in the current dimension */
                        offset[temp_dim]++;
                        tmp_block[temp_dim]++;

                        /* If this block is still in the range of blocks to output for the dimension, break
                         * out of loop */
                        if (tmp_block[temp_dim] < tdiminfo[temp_dim].block)
                            break;
                        else {
                            offset[temp_dim] = base_offset[temp_dim];
                            loc += skip[temp_dim];
                            tmp_block[temp_dim] = 0;
                        } /* end else */

                        /* Decrement dimension count */
                        temp_dim--;
                    } /* end while */

                    /* Decrement block count */
                    blk_count--;
                } /* end while */
            }     /* end else */
        }         /* end else */

        /* Update the iterator, if there were any blocks used */

        /* Decrement the number of elements left in selection */
        iter->elmt_left -= tot_blk_count * actual_elem;

        /* Check if there are elements left in iterator */
        if (iter->elmt_left > 0) {
            /* Update the iterator with the location we stopped */
            /* (Subtract out the selection offset) */
            for (u = 0; u < ndims; u++)
                iter->u.hyp.off[u] = (hsize_t)((hssize_t)offset[u] - sel_off[u]);
        } /* end if */

        /* Increment the number of sequences generated */
        *nseq += tot_blk_count;

        /* Increment the number of elements used */
        *nelem += tot_blk_count * actual_elem;
    } /* end if */

    /* Check for partial block, with room for another sequence */
    if (io_left > (tot_blk_count * actual_elem) && tot_blk_count < maxseq) {
        size_t elmt_remainder; /* Elements remaining */

        /* Compute elements left */
        elmt_remainder = io_left - (tot_blk_count * actual_elem);
        assert(elmt_remainder < fast_dim_block);
        assert(elmt_remainder > 0);

        /* Store the sequence information */
        *off++ = loc;
        *len++ = elmt_remainder * elem_size;

        /* Update the iterator with the location we stopped */
        iter->u.hyp.off[fast_dim] += (hsize_t)elmt_remainder;

        /* Decrement the number of elements left in selection */
        iter->elmt_left -= elmt_remainder;

        /* Increment the number of sequences generated */
        (*nseq)++;

        /* Increment the number of elements used */
        *nelem += elmt_remainder;
    } /* end if */

    /* Sanity check */
    assert(*nseq > 0);
    assert(*nelem > 0);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_iter_get_seq_list_single() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_iter_get_seq_list
 PURPOSE
    Create a list of offsets & lengths for a selection
 USAGE
    herr_t H5S__hyper_iter_get_seq_list(iter,maxseq,maxelem,nseq,nelem,off,len)
        H5S_t *space;           IN: Dataspace containing selection to use.
        H5S_sel_iter_t *iter;   IN/OUT: Selection iterator describing last
                                    position of interest in selection.
        size_t maxseq;          IN: Maximum number of sequences to generate
        size_t maxelem;         IN: Maximum number of elements to include in the
                                    generated sequences
        size_t *nseq;           OUT: Actual number of sequences generated
        size_t *nelem;          OUT: Actual number of elements in sequences generated
        hsize_t *off;           OUT: Array of offsets (in bytes)
        size_t *len;            OUT: Array of lengths (in bytes)
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    Use the selection in the dataspace to generate a list of byte offsets and
    lengths for the region(s) selected.  Start/Restart from the position in the
    ITER parameter.  The number of sequences generated is limited by the MAXSEQ
    parameter and the number of sequences actually generated is stored in the
    NSEQ parameter.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_iter_get_seq_list(H5S_sel_iter_t *iter, size_t maxseq, size_t maxelem, size_t *nseq, size_t *nelem,
                             hsize_t *off, size_t *len)
{
    herr_t ret_value = FAIL; /* return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);
    assert(iter->elmt_left > 0);
    assert(maxseq > 0);
    assert(maxelem > 0);
    assert(nseq);
    assert(nelem);
    assert(off);
    assert(len);

    /* Check for the special case of just one H5Sselect_hyperslab call made */
    if (iter->u.hyp.diminfo_valid) {
        const H5S_hyper_dim_t *tdiminfo;     /* Temporary pointer to diminfo information */
        const hssize_t        *sel_off;      /* Selection offset in dataspace */
        unsigned               ndims;        /* Number of dimensions of dataset */
        unsigned               fast_dim;     /* Rank of the fastest changing dimension for the dataspace */
        bool                   single_block; /* Whether the selection is a single block */
        unsigned               u;            /* Local index variable */

        /* Set a local copy of the diminfo pointer */
        tdiminfo = iter->u.hyp.diminfo;

        /* Check if this is a "flattened" regular hyperslab selection */
        if (iter->u.hyp.iter_rank != 0 && iter->u.hyp.iter_rank < iter->rank) {
            /* Set the aliases for a few important dimension ranks */
            ndims = iter->u.hyp.iter_rank;

            /* Set the local copy of the selection offset */
            sel_off = iter->u.hyp.sel_off;
        } /* end if */
        else {
            /* Set the aliases for a few important dimension ranks */
            ndims = iter->rank;

            /* Set the local copy of the selection offset */
            sel_off = iter->sel_off;
        } /* end else */
        fast_dim = ndims - 1;

        /* Check if we stopped in the middle of a sequence of elements */
        if ((iter->u.hyp.off[fast_dim] - tdiminfo[fast_dim].start) % tdiminfo[fast_dim].stride != 0 ||
            ((iter->u.hyp.off[fast_dim] != tdiminfo[fast_dim].start) && tdiminfo[fast_dim].count == 1)) {
            hsize_t *slab;        /* Hyperslab size */
            hsize_t  loc;         /* Coordinate offset */
            size_t   leftover;    /* The number of elements left over from the last sequence */
            size_t   actual_elem; /* The actual number of elements to count */
            size_t   elem_size;   /* Size of each element iterating over */

            /* Calculate the number of elements left in the sequence */
            if (tdiminfo[fast_dim].count == 1) {
                H5_CHECKED_ASSIGN(leftover, size_t,
                                  tdiminfo[fast_dim].block -
                                      (iter->u.hyp.off[fast_dim] - tdiminfo[fast_dim].start),
                                  hsize_t);
            } /* end if */
            else {
                H5_CHECKED_ASSIGN(
                    leftover, size_t,
                    tdiminfo[fast_dim].block -
                        ((iter->u.hyp.off[fast_dim] - tdiminfo[fast_dim].start) % tdiminfo[fast_dim].stride),
                    hsize_t);
            } /* end else */

            /* Make certain that we don't write too many */
            actual_elem = MIN3(leftover, (size_t)iter->elmt_left, maxelem);

            /* Set up some local variables */
            elem_size = iter->elmt_size;
            slab      = iter->u.hyp.slab;

            /* Compute the initial buffer offset */
            for (u = 0, loc = 0; u < ndims; u++)
                loc += ((hsize_t)((hssize_t)iter->u.hyp.off[u] + sel_off[u])) * slab[u];

            /* Add a new sequence */
            off[0] = loc;
            H5_CHECKED_ASSIGN(len[0], size_t, actual_elem * elem_size, hsize_t);

            /* Increment sequence array locations */
            off++;
            len++;

            /* Advance the hyperslab iterator */
            H5S__hyper_iter_next(iter, actual_elem);

            /* Decrement the number of elements left in selection */
            iter->elmt_left -= actual_elem;

            /* Decrement element/sequence limits */
            maxelem -= actual_elem;
            maxseq--;

            /* Set the number of sequences generated and elements used */
            *nseq  = 1;
            *nelem = actual_elem;

            /* Check for using up all the sequences/elements */
            if (0 == iter->elmt_left || 0 == maxelem || 0 == maxseq)
                return (SUCCEED);
        } /* end if */
        else {
            /* Reset the number of sequences generated and elements used */
            *nseq  = 0;
            *nelem = 0;
        } /* end else */

        /* Check for a single block selected */
        single_block = true;
        for (u = 0; u < ndims; u++)
            if (1 != tdiminfo[u].count) {
                single_block = false;
                break;
            } /* end if */

        /* Check for single block selection */
        if (single_block)
            /* Use single-block optimized call to generate sequence list */
            ret_value = H5S__hyper_iter_get_seq_list_single(iter, maxseq, maxelem, nseq, nelem, off, len);
        else
            /* Use optimized call to generate sequence list */
            ret_value = H5S__hyper_iter_get_seq_list_opt(iter, maxseq, maxelem, nseq, nelem, off, len);
    } /* end if */
    else
        /* Call the general sequence generator routine */
        ret_value = H5S__hyper_iter_get_seq_list_gen(iter, maxseq, maxelem, nseq, nelem, off, len);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_iter_get_seq_list() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_iter_release
 PURPOSE
    Release hyperslab selection iterator information for a dataspace
 USAGE
    herr_t H5S__hyper_iter_release(iter)
        H5S_sel_iter_t *iter;       IN: Pointer to selection iterator
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Releases all information for a dataspace hyperslab selection iterator
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_iter_release(H5S_sel_iter_t *iter)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(iter);

    /* Free the copy of the hyperslab selection span tree */
    if (iter->u.hyp.spans != NULL)
        if (H5S__hyper_free_span_info(iter->u.hyp.spans) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_iter_release() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_new_span
 PURPOSE
    Make a new hyperslab span node
 USAGE
    H5S_hyper_span_t *H5S__hyper_new_span(low, high, down, next)
        hsize_t low, high;         IN: Low and high bounds for new span node
        H5S_hyper_span_info_t *down;     IN: Down span tree for new node
        H5S_hyper_span_t *next;     IN: Next span for new node
 RETURNS
    Pointer to new span node on success, NULL on failure
 DESCRIPTION
    Allocate and initialize a new hyperslab span node, filling in the low &
    high bounds, the down span and next span pointers also.  Increment the
    reference count of the 'down span' if applicable.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5S_hyper_span_t *
H5S__hyper_new_span(hsize_t low, hsize_t high, H5S_hyper_span_info_t *down, H5S_hyper_span_t *next)
{
    H5S_hyper_span_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate a new span node */
    if (NULL == (ret_value = H5FL_MALLOC(H5S_hyper_span_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

    /* Copy the span's basic information */
    ret_value->low  = low;
    ret_value->high = high;
    ret_value->down = down;
    ret_value->next = next;

    /* Increment the reference count of the 'down span' if there is one */
    if (ret_value->down)
        ret_value->down->count++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_new_span() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_new_span_info
 PURPOSE
    Make a new hyperslab span info node
 USAGE
    H5S_hyper_span_info_t *H5S__hyper_new_span_info(rank)
        unsigned rank;          IN: Rank of span info, in selection
 RETURNS
    Pointer to new span node info on success, NULL on failure
 DESCRIPTION
    Allocate and initialize a new hyperslab span info node of a given rank,
    setting up the low & high bound array pointers.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Note that this uses the C99 "flexible array member" feature.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5S_hyper_span_info_t *
H5S__hyper_new_span_info(unsigned rank)
{
    H5S_hyper_span_info_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(rank > 0);
    assert(rank <= H5S_MAX_RANK);

    /* Allocate a new span info node */
    if (NULL == (ret_value = (H5S_hyper_span_info_t *)H5FL_ARR_CALLOC(hbounds_t, rank * 2)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span info");

    /* Set low & high bound pointers into the 'bounds' array */
    ret_value->low_bounds  = ret_value->bounds;
    ret_value->high_bounds = &ret_value->bounds[rank];

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_new_span_info() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_copy_span_helper
 PURPOSE
    Helper routine to copy a hyperslab span tree
 USAGE
    H5S_hyper_span_info_t * H5S__hyper_copy_span_helper(spans, rank, op_info_i, op_gen)
        H5S_hyper_span_info_t *spans;   IN: Span tree to copy
        unsigned rank;                  IN: Rank of span tree
        unsigned op_info_i;             IN: Index of op info to use
        uint64_t op_gen;                IN: Operation generation
 RETURNS
    Pointer to the copied span tree on success, NULL on failure
 DESCRIPTION
    Copy a hyperslab span tree, using reference counting as appropriate.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5S_hyper_span_info_t *
H5S__hyper_copy_span_helper(H5S_hyper_span_info_t *spans, unsigned rank, unsigned op_info_i, uint64_t op_gen)
{
    H5S_hyper_span_t      *span;             /* Hyperslab span */
    H5S_hyper_span_t      *new_span;         /* Temporary hyperslab span */
    H5S_hyper_span_t      *prev_span;        /* Previous hyperslab span */
    H5S_hyper_span_info_t *new_down;         /* New down span tree */
    H5S_hyper_span_info_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(spans);

    /* Check if the span tree was already copied */
    if (spans->op_info[op_info_i].op_gen == op_gen) {
        /* Just return the value of the already copied span tree */
        ret_value = spans->op_info[op_info_i].u.copied;

        /* Increment the reference count of the span tree */
        ret_value->count++;
    } /* end if */
    else {
        /* Allocate a new span_info node */
        if (NULL == (ret_value = H5S__hyper_new_span_info(rank)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span info");

        /* Set the non-zero span_info information */
        H5MM_memcpy(ret_value->low_bounds, spans->low_bounds, rank * sizeof(hsize_t));
        H5MM_memcpy(ret_value->high_bounds, spans->high_bounds, rank * sizeof(hsize_t));
        ret_value->count = 1;

        /* Set the operation generation for the span info, to avoid future copies */
        spans->op_info[op_info_i].op_gen = op_gen;

        /* Set the 'copied' pointer in the node being copied to the newly allocated node */
        spans->op_info[op_info_i].u.copied = ret_value;

        /* Copy over the nodes in the span list */
        span      = spans->head;
        prev_span = NULL;
        while (span != NULL) {
            /* Allocate a new node */
            if (NULL == (new_span = H5S__hyper_new_span(span->low, span->high, NULL, NULL)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

            /* Append to list of spans */
            if (NULL == prev_span)
                ret_value->head = new_span;
            else
                prev_span->next = new_span;

            /* Recurse to copy the 'down' spans, if there are any */
            if (span->down != NULL) {
                if (NULL == (new_down = H5S__hyper_copy_span_helper(span->down, rank - 1, op_info_i, op_gen)))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, NULL, "can't copy hyperslab spans");
                new_span->down = new_down;
            } /* end if */

            /* Update the previous (new) span */
            prev_span = new_span;

            /* Advance to next span */
            span = span->next;
        } /* end while */

        /* Retain a pointer to the last span */
        ret_value->tail = prev_span;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_copy_span_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_copy_span
 PURPOSE
    Copy a hyperslab span tree
 USAGE
    H5S_hyper_span_info_t * H5S__hyper_copy_span(span_info, rank)
        H5S_hyper_span_info_t *span_info;       IN: Span tree to copy
        unsigned rank;                          IN: Rank of span tree
 RETURNS
    Pointer to the copied span tree on success, NULL on failure
 DESCRIPTION
    Copy a hyperslab span tree, using reference counting as appropriate.
    (Which means that just the nodes in the top span tree are duplicated and
    the reference counts of their 'down spans' are just incremented)
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5S_hyper_span_info_t *
H5S__hyper_copy_span(H5S_hyper_span_info_t *spans, unsigned rank)
{
    uint64_t               op_gen;           /* Operation generation value */
    H5S_hyper_span_info_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(spans);

    /* Acquire an operation generation value for this operation */
    op_gen = H5S__hyper_get_op_gen();

    /* Copy the hyperslab span tree */
    /* Always use op_info[0] since we own this op_info, so there can be no
     * simultaneous operations */
    if (NULL == (ret_value = H5S__hyper_copy_span_helper(spans, rank, 0, op_gen)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, NULL, "can't copy hyperslab span tree");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_copy_span() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_cmp_spans
 PURPOSE
    Check if two hyperslab span trees are the same
 USAGE
    bool H5S__hyper_cmp_spans(span1, span2)
        H5S_hyper_span_info_t *span_info1;      IN: First span tree to compare
        H5S_hyper_span_info_t *span_info2;      IN: Second span tree to compare
 RETURNS
    true (1) or false (0) on success, can't fail
 DESCRIPTION
    Compare two hyperslab span trees to determine if they refer to the same
    selection.  If span1 & span2 are both NULL, that counts as equal.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5_ATTR_PURE bool
H5S__hyper_cmp_spans(const H5S_hyper_span_info_t *span_info1, const H5S_hyper_span_info_t *span_info2)
{
    bool ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check for redundant comparison (or both spans being NULL) */
    if (span_info1 != span_info2) {
        /* Check for one span being NULL */
        if (span_info1 == NULL || span_info2 == NULL)
            HGOTO_DONE(false);
        else {
            /* Compare low & high bounds for this span list */
            /* (Could compare lower dimensions also, but not certain if
             *      that's worth it. - QAK, 2019/01/23)
             */
            if (span_info1->low_bounds[0] != span_info2->low_bounds[0])
                HGOTO_DONE(false);
            else if (span_info1->high_bounds[0] != span_info2->high_bounds[0])
                HGOTO_DONE(false);
            else {
                const H5S_hyper_span_t *span1;
                const H5S_hyper_span_t *span2;

                /* Get the pointers to the actual lists of spans */
                span1 = span_info1->head;
                span2 = span_info2->head;

                /* Sanity checking */
                assert(span1);
                assert(span2);

                /* infinite loop which must be broken out of */
                while (1) {
                    /* Check for both spans being NULL */
                    if (span1 == NULL && span2 == NULL)
                        HGOTO_DONE(true);
                    else {
                        /* Check for one span being NULL */
                        if (span1 == NULL || span2 == NULL)
                            HGOTO_DONE(false);
                        else {
                            /* Check if the actual low & high span information is the same */
                            if (span1->low != span2->low || span1->high != span2->high)
                                HGOTO_DONE(false);
                            else {
                                if (span1->down != NULL || span2->down != NULL) {
                                    if (!H5S__hyper_cmp_spans(span1->down, span2->down))
                                        HGOTO_DONE(false);
                                    else {
                                        /* Keep going... */
                                    } /* end else */
                                }     /* end if */
                                else {
                                    /* Keep going... */
                                } /* end else */
                            }     /* end else */
                        }         /* end else */
                    }             /* end else */

                    /* Advance to the next nodes in the span list */
                    span1 = span1->next;
                    span2 = span2->next;
                } /* end while */
            }     /* end else */
        }         /* end else */
    }             /* end if */

    /* Fall through, with default return value of 'true' if spans were already visited */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_cmp_spans() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_free_span_info
 PURPOSE
    Free a hyperslab span info node
 USAGE
    herr_t H5S__hyper_free_span_info(span_info)
        H5S_hyper_span_info_t *span_info;      IN: Span info node to free
 RETURNS
    SUCCEED/FAIL
 DESCRIPTION
    Free a hyperslab span info node, along with all the span nodes and the
    'down spans' from the nodes, if reducing their reference count to zero
    indicates it is appropriate to do so.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_free_span_info(H5S_hyper_span_info_t *span_info)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    if (!span_info)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "span_info pointer was NULL");

    /* Decrement the span tree's reference count */
    span_info->count--;

    /* Free the span tree if the reference count drops to zero */
    if (span_info->count == 0) {
        H5S_hyper_span_t *span; /* Pointer to spans to iterate over */

        /* Work through the list of spans pointed to by this 'info' node */
        span = span_info->head;
        while (span != NULL) {
            H5S_hyper_span_t *next_span; /* Pointer to next span to iterate over */

            /* Keep a pointer to the next span */
            next_span = span->next;

            /* Free the current span */
            if (H5S__hyper_free_span(span) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span");

            /* Advance to next span */
            span = next_span;
        }

        /* Free this span info */
        span_info = (H5S_hyper_span_info_t *)H5FL_ARR_FREE(hbounds_t, span_info);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_free_span_info() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_free_span
 PURPOSE
    Free a hyperslab span node
 USAGE
    herr_t H5S__hyper_free_span(span)
        H5S_hyper_span_t *span;      IN: Span node to free
 RETURNS
    SUCCEED/FAIL
 DESCRIPTION
    Free a hyperslab span node, along with the 'down spans' from the node,
    if reducing their reference count to zero indicates it is appropriate to
    do so.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_free_span(H5S_hyper_span_t *span)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(span);

    /* Decrement the reference count of the 'down spans', freeing them if appropriate */
    if (span->down != NULL)
        if (H5S__hyper_free_span_info(span->down) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");

    /* Free this span */
    span = H5FL_FREE(H5S_hyper_span_t, span);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_free_span() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_copy
 PURPOSE
    Copy a selection from one dataspace to another
 USAGE
    herr_t H5S__hyper_copy(dst, src, share_selection)
        H5S_t *dst;  OUT: Pointer to the destination dataspace
        H5S_t *src;  IN: Pointer to the source dataspace
        bool;     IN: Whether to share the selection between the dataspaces
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Copies all the hyperslab selection information from the source
    dataspace to the destination dataspace.

    If the SHARE_SELECTION flag is set, then the selection can be shared
    between the source and destination dataspaces.  (This should only occur in
    situations where the destination dataspace will immediately change to a new
    selection)
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_copy(H5S_t *dst, const H5S_t *src, bool share_selection)
{
    H5S_hyper_sel_t       *dst_hslab;           /* Pointer to destination hyperslab info */
    const H5S_hyper_sel_t *src_hslab;           /* Pointer to source hyperslab info */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(src);
    assert(dst);

    /* Allocate space for the hyperslab selection information */
    if (NULL == (dst->select.sel_info.hslab = H5FL_MALLOC(H5S_hyper_sel_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab info");

    /* Set temporary pointers */
    dst_hslab = dst->select.sel_info.hslab;
    src_hslab = src->select.sel_info.hslab;

    /* Copy the hyperslab information */
    dst_hslab->diminfo_valid = src_hslab->diminfo_valid;
    if (src_hslab->diminfo_valid == H5S_DIMINFO_VALID_YES)
        H5MM_memcpy(&dst_hslab->diminfo, &src_hslab->diminfo, sizeof(H5S_hyper_diminfo_t));

    /* Check if there is hyperslab span information to copy */
    /* (Regular hyperslab information is copied with the selection structure) */
    if (src->select.sel_info.hslab->span_lst != NULL) {
        if (share_selection) {
            /* Share the source's span tree by incrementing the reference count on it */
            dst->select.sel_info.hslab->span_lst = src->select.sel_info.hslab->span_lst;
            dst->select.sel_info.hslab->span_lst->count++;
        } /* end if */
        else
            /* Copy the hyperslab span information */
            dst->select.sel_info.hslab->span_lst =
                H5S__hyper_copy_span(src->select.sel_info.hslab->span_lst, src->extent.rank);
    } /* end if */
    else
        dst->select.sel_info.hslab->span_lst = NULL;

    /* Copy the unlimited dimension info */
    dst_hslab->unlim_dim          = src_hslab->unlim_dim;
    dst_hslab->num_elem_non_unlim = src_hslab->num_elem_non_unlim;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_copy() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_is_valid
 PURPOSE
    Check whether the selection fits within the extent, with the current
    offset defined.
 USAGE
    htri_t H5S__hyper_is_valid(space);
        H5S_t *space;             IN: Dataspace pointer to query
 RETURNS
    true if the selection fits within the extent, false if it does not and
        Negative on an error.
 DESCRIPTION
    Determines if the current selection at the current offset fits within the
    extent for the dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static htri_t
H5S__hyper_is_valid(const H5S_t *space)
{
    const hsize_t *low_bounds, *high_bounds; /* Pointers to the correct pair of low & high bounds */
    unsigned       u;                        /* Counter */
    htri_t         ret_value = true;         /* return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);

    /* Check for unlimited selection */
    if (space->select.sel_info.hslab->unlim_dim >= 0)
        HGOTO_DONE(false);

    /* Check which set of low & high bounds we should be using */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        low_bounds  = space->select.sel_info.hslab->diminfo.low_bounds;
        high_bounds = space->select.sel_info.hslab->diminfo.high_bounds;
    } /* end if */
    else {
        low_bounds  = space->select.sel_info.hslab->span_lst->low_bounds;
        high_bounds = space->select.sel_info.hslab->span_lst->high_bounds;
    } /* end else */

    /* Check each dimension */
    for (u = 0; u < space->extent.rank; u++) {
        /* Bounds check the selected point + offset against the extent */
        if (((hssize_t)low_bounds[u] + space->select.offset[u]) < 0)
            HGOTO_DONE(false);
        if ((high_bounds[u] + (hsize_t)space->select.offset[u]) >= space->extent.size[u])
            HGOTO_DONE(false);
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_is_valid() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_span_nblocks_helper
 PURPOSE
    Helper routine to count the number of blocks in a span tree
 USAGE
    hsize_t H5S__hyper_span_nblocks_helper(spans, op_info_i, op_gen)
        H5S_hyper_span_info_t *spans; IN: Hyperslab span tree to count blocks of
        unsigned op_info_i; IN: Index of op info to use
        uint64_t op_gen;   IN: Operation generation
 RETURNS
    Number of blocks in span tree on success; negative on failure
 DESCRIPTION
    Counts the number of blocks described by the spans in a span tree.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static hsize_t
H5S__hyper_span_nblocks_helper(H5S_hyper_span_info_t *spans, unsigned op_info_i, uint64_t op_gen)
{
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(spans);

    /* Check if the span tree was already counted */
    if (spans->op_info[op_info_i].op_gen == op_gen)
        /* Just return the # of blocks in the already counted span tree */
        ret_value = spans->op_info[op_info_i].u.nblocks;
    else {                      /* Count the number of elements in the span tree */
        H5S_hyper_span_t *span; /* Hyperslab span */

        span = spans->head;
        if (span->down) {
            while (span) {
                /* If there are down spans, add the total down span blocks */
                ret_value += H5S__hyper_span_nblocks_helper(span->down, op_info_i, op_gen);

                /* Advance to next span */
                span = span->next;
            } /* end while */
        }     /* end if */
        else {
            while (span) {
                /* If there are no down spans, just count the block in this span */
                ret_value++;

                /* Advance to next span */
                span = span->next;
            } /* end while */
        }     /* end else */

        /* Set the operation generation for this span tree, to avoid re-computing */
        spans->op_info[op_info_i].op_gen = op_gen;

        /* Hold a copy of the # of blocks */
        spans->op_info[op_info_i].u.nblocks = ret_value;
    } /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_span_nblocks_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_span_nblocks
 PURPOSE
    Count the number of blocks in a span tree
 USAGE
    hsize_t H5S__hyper_span_nblocks(spans)
        H5S_hyper_span_info_t *spans; IN: Hyperslab span tree to count blocks of
 RETURNS
    Number of blocks in span tree on success; negative on failure
 DESCRIPTION
    Counts the number of blocks described by the spans in a span tree.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static hsize_t
H5S__hyper_span_nblocks(H5S_hyper_span_info_t *spans)
{
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Count the number of elements in the span tree */
    if (spans != NULL) {
        uint64_t op_gen; /* Operation generation value */

        /* Acquire an operation generation value for this operation */
        op_gen = H5S__hyper_get_op_gen();

        /* Count the blocks */
        /* Always use op_info[0] since we own this op_info, so there can be no
         * simultaneous operations */
        ret_value = H5S__hyper_span_nblocks_helper(spans, 0, op_gen);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_span_nblocks() */

/*--------------------------------------------------------------------------
 NAME
    H5S__get_select_hyper_nblocks
 PURPOSE
    Get the number of hyperslab blocks in current hyperslab selection
 USAGE
    hsize_t H5S__get_select_hyper_nblocks(space, app_ref)
        H5S_t *space;             IN: Dataspace ptr of selection to query
        bool app_ref;          IN: Whether this is an appl. ref. call
 RETURNS
    The number of hyperslab blocks in selection on success, negative on failure
 DESCRIPTION
    Returns the number of hyperslab blocks in current selection for dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static hsize_t
H5S__get_select_hyper_nblocks(const H5S_t *space, bool app_ref)
{
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);
    assert(space->select.sel_info.hslab->unlim_dim < 0);

    /* Check for a "regular" hyperslab selection */
    /* (No need to rebuild the dimension info yet -QAK) */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        unsigned u; /* Local index variable */

        /* Check each dimension */
        for (ret_value = 1, u = 0; u < space->extent.rank; u++)
            ret_value *= (app_ref ? space->select.sel_info.hslab->diminfo.app[u].count
                                  : space->select.sel_info.hslab->diminfo.opt[u].count);
    } /* end if */
    else
        ret_value = H5S__hyper_span_nblocks(space->select.sel_info.hslab->span_lst);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__get_select_hyper_nblocks() */

/*--------------------------------------------------------------------------
 NAME
    H5Sget_select_hyper_nblocks
 PURPOSE
    Get the number of hyperslab blocks in current hyperslab selection
 USAGE
    hssize_t H5Sget_select_hyper_nblocks(dsid)
        hid_t dsid;             IN: Dataspace ID of selection to query
 RETURNS
    The number of hyperslab blocks in selection on success, negative on failure
 DESCRIPTION
    Returns the number of hyperslab blocks in current selection for dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hssize_t
H5Sget_select_hyper_nblocks(hid_t spaceid)
{
    H5S_t   *space;     /* Dataspace to modify selection of */
    hssize_t ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("Hs", "i", spaceid);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (H5S_GET_SELECT_TYPE(space) != H5S_SEL_HYPERSLABS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a hyperslab selection");
    if (space->select.sel_info.hslab->unlim_dim >= 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL,
                    "cannot get number of blocks for unlimited selection");

    ret_value = (hssize_t)H5S__get_select_hyper_nblocks(space, true);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sget_select_hyper_nblocks() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_get_enc_size_real
 PURPOSE
    Determine the size to encode the hyperslab selection info
 USAGE
    hssize_t H5S__hyper_get_enc_size_real(max_size, enc_size)
        hsize_t max_size:       IN: The maximum size of the hyperslab selection info
        uint8_t *enc_size:      OUT:The encoding size
 RETURNS
    The size to encode hyperslab selection info
 DESCRIPTION
    Determine the size by comparing "max_size" with (2^32 - 1) and (2^16 - 1).
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static uint8_t
H5S__hyper_get_enc_size_real(hsize_t max_size)
{
    uint8_t ret_value = H5S_SELECT_INFO_ENC_SIZE_2;

    FUNC_ENTER_PACKAGE_NOERR

    if (max_size > H5S_UINT32_MAX)
        ret_value = H5S_SELECT_INFO_ENC_SIZE_8;
    else if (max_size > H5S_UINT16_MAX)
        ret_value = H5S_SELECT_INFO_ENC_SIZE_4;
    else
        ret_value = H5S_SELECT_INFO_ENC_SIZE_2;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__hyper_get_enc_size_real() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_get_version_enc_size
 PURPOSE
    Determine the version and encoded size to use for encoding hyperslab selection info
 USAGE
    hssize_t H5S__hyper_get_version_enc_size(space, block_count, version, enc_size)
        const H5S_t *space:             IN: The dataspace
        hsize_t block_count:            IN: The number of blocks in the selection
        uint32_t *version:              OUT: The version to use for encoding
        uint8_t *enc_size:              OUT: The encoded size to use

 RETURNS
    The version and the size to encode hyperslab selection info
 DESCRIPTION
    Determine the version to use for encoding hyperslab selection info based
    on the following:
    (1) the file format setting in fapl
    (2) whether the number of blocks or selection high bounds exceeds H5S_UINT32_MAX or not

    Determine the encoded size based on version:
    For version 3, the encoded size is determined according to:
    (a) regular hyperslab
        (1) The maximum needed to store start/stride/count/block
        (2) Special handling for count/block: need to provide room for H5S_UNLIMITED
    (b) irregular hyperslab
        The maximum size needed to store:
            (1) the number of blocks
            (2) the selection high bounds
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_get_version_enc_size(H5S_t *space, hsize_t block_count, uint32_t *version, uint8_t *enc_size)
{
    hsize_t      bounds_start[H5S_MAX_RANK]; /* Starting coordinate of bounding box */
    hsize_t      bounds_end[H5S_MAX_RANK];   /* Opposite coordinate of bounding box */
    bool         count_up_version = false;   /* Whether number of blocks exceed H5S_UINT32_MAX */
    bool         bound_up_version = false;   /* Whether high bounds exceed H5S_UINT32_MAX */
    H5F_libver_t low_bound;                  /* The 'low' bound of library format versions */
    H5F_libver_t high_bound;                 /* The 'high' bound of library format versions */
    htri_t       is_regular;                 /* A regular hyperslab or not */
    uint32_t     tmp_version;                /* Local temporary version */
    unsigned     u;                          /* Local index variable */
    herr_t       ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get bounding box for the selection */
    memset(bounds_end, 0, sizeof(bounds_end));

    if (space->select.sel_info.hslab->unlim_dim < 0) /* ! H5S_UNLIMITED */
        /* Get bounding box for the selection */
        if (H5S__hyper_bounds(space, bounds_start, bounds_end) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get selection bounds");

    /* Determine whether the number of blocks or the high bounds in the selection exceed (2^32 - 1) */
    if (block_count > H5S_UINT32_MAX)
        count_up_version = true;
    else {
        for (u = 0; u < space->extent.rank; u++)
            if (bounds_end[u] > H5S_UINT32_MAX) {
                bound_up_version = true;
                break;
            } /* end if */
    }         /* end else */

    /* Get the file's low_bound and high_bound */
    if (H5CX_get_libver_bounds(&low_bound, &high_bound) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get low/high bounds from API context");

    /* Determine regular hyperslab */
    is_regular = H5S__hyper_is_regular(space);

    if (low_bound >= H5F_LIBVER_V112 || space->select.sel_info.hslab->unlim_dim >= 0)
        tmp_version = MAX(H5S_HYPER_VERSION_2, H5O_sds_hyper_ver_bounds[low_bound]);
    else {
        if (count_up_version || bound_up_version)
            tmp_version = is_regular ? H5S_HYPER_VERSION_2 : H5S_HYPER_VERSION_3;
        else
            tmp_version =
                (is_regular && block_count >= 4) ? H5O_sds_hyper_ver_bounds[low_bound] : H5S_HYPER_VERSION_1;
    } /* end else */

    /* Version bounds check */
    if (tmp_version > H5O_sds_hyper_ver_bounds[high_bound]) {
        /* Fail for irregular hyperslab if exceeds 32 bits */
        if (count_up_version)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                        "The number of blocks in hyperslab selection exceeds 2^32");
        else if (bound_up_version)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                        "The end of bounding box in hyperslab selection exceeds 2^32");
        else
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL,
                        "Dataspace hyperslab selection version out of bounds");
    } /* end if */

    /* Set the message version */
    *version = tmp_version;

    /* Determine the encoded size based on version */
    switch (tmp_version) {
        case H5S_HYPER_VERSION_1:
            *enc_size = H5S_SELECT_INFO_ENC_SIZE_4;
            break;

        case H5S_HYPER_VERSION_2:
            *enc_size = H5S_SELECT_INFO_ENC_SIZE_8;
            break;

        case H5S_HYPER_VERSION_3:
            if (is_regular) {
                uint8_t enc1, enc2;
                hsize_t max1 = 0;
                hsize_t max2 = 0;

                /* Find max for count[] and block[] */
                for (u = 0; u < space->extent.rank; u++) {
                    if (space->select.sel_info.hslab->diminfo.opt[u].count != H5S_UNLIMITED &&
                        space->select.sel_info.hslab->diminfo.opt[u].count > max1)
                        max1 = space->select.sel_info.hslab->diminfo.opt[u].count;
                    if (space->select.sel_info.hslab->diminfo.opt[u].block != H5S_UNLIMITED &&
                        space->select.sel_info.hslab->diminfo.opt[u].block > max1)
                        max1 = space->select.sel_info.hslab->diminfo.opt[u].block;
                } /* end for */

                /* +1 to provide room for H5S_UNLIMITED */
                enc1 = H5S__hyper_get_enc_size_real(++max1);

                /* Find max for start[] and stride[] */
                for (u = 0; u < space->extent.rank; u++) {
                    if (space->select.sel_info.hslab->diminfo.opt[u].start > max2)
                        max2 = space->select.sel_info.hslab->diminfo.opt[u].start;
                    if (space->select.sel_info.hslab->diminfo.opt[u].stride > max2)
                        max2 = space->select.sel_info.hslab->diminfo.opt[u].stride;
                } /* end for */

                /* Determine the encoding size */
                enc2 = H5S__hyper_get_enc_size_real(max2);

                *enc_size = (uint8_t)MAX(enc1, enc2);
            } /* end if */
            else {
                hsize_t max_size = block_count;
                assert(space->select.sel_info.hslab->unlim_dim < 0);

                /* Find max for block_count and bounds_end[] */
                for (u = 0; u < space->extent.rank; u++)
                    if (bounds_end[u] > max_size)
                        max_size = bounds_end[u];

                /* Determine the encoding size */
                *enc_size = H5S__hyper_get_enc_size_real(max_size);
            } /* end else */
            break;

        default:
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unknown hyperslab selection version");
            break;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__hyper_get_version_enc_size() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_serial_size
 PURPOSE
    Determine the number of bytes needed to store the serialized hyperslab
        selection information.
 USAGE
    hssize_t H5S__hyper_serial_size(space)
        H5S_t *space;             IN: Dataspace pointer to query
 RETURNS
    The number of bytes required on success, negative on an error.
 DESCRIPTION
    Determines the number of bytes required to serialize the current hyperslab
    selection information for storage on disk.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static hssize_t
H5S__hyper_serial_size(H5S_t *space)
{
    hsize_t  block_count = 0; /* block counter for regular hyperslabs */
    uint32_t version;         /* Version number */
    uint8_t  enc_size;        /* Encoded size of hyperslab selection info */
    hssize_t ret_value = -1;  /* return value */

    FUNC_ENTER_PACKAGE

    assert(space);

    /* Determine the number of blocks */
    if (space->select.sel_info.hslab->unlim_dim < 0) /* ! H5S_UNLIMITED */
        block_count = H5S__get_select_hyper_nblocks(space, false);

    /* Determine the version and the encoded size */
    if (H5S__hyper_get_version_enc_size(space, block_count, &version, &enc_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't determine hyper version & enc_size");

    if (version == H5S_HYPER_VERSION_3) {
        /* Version 3: regular */
        /* Size required is always:
         * <type (4 bytes)> + <version (4 bytes)> + <flags (1 byte)> +
         * <size of offset info (1 byte)> + <rank (4 bytes)> +
         * (4 (start/stride/count/block) * <enc_size> * <rank>) =
         * 14 + (4 * enc_size * rank) bytes
         */
        if (H5S__hyper_is_regular(space))
            ret_value = (hssize_t)14 + ((hssize_t)4 * (hssize_t)enc_size * (hssize_t)space->extent.rank);
        else {
            /* Version 3: irregular */
            /* Size required is always:
             * <type (4 bytes)> + <version (4 bytes)> + <flags (1 byte)> +
             * <size of offset info (1 byte)> + <rank (4 bytes)> +
             * < # of blocks (depend on enc_size) > +
             * (2 (starting/ending offset) * <rank> * <enc_size> * <# of blocks) =
             * = 14 bytes + enc_size (block_count) + (2 * enc_size * rank * block_count) bytes
             */
            ret_value = 14 + enc_size;
            H5_CHECK_OVERFLOW(((unsigned)2 * enc_size * space->extent.rank * block_count), hsize_t, hssize_t);
            ret_value += (hssize_t)((unsigned)2 * enc_size * space->extent.rank * block_count);
        } /* end else */
    }     /* end if */
    else if (version == H5S_HYPER_VERSION_2) {
        /* Version 2 */
        /* Size required is always:
         * <type (4 bytes)> + <version (4 bytes)> + <flags (1 byte)> +
         * <length (4 bytes)> + <rank (4 bytes)> +
         * (4 (start/stride/count/block) * <enc_size (8 bytes)> * <rank>) =
         * 17 + (4 * 8 * rank) bytes
         */
        assert(enc_size == 8);
        ret_value = (hssize_t)17 + ((hssize_t)4 * (hssize_t)8 * (hssize_t)space->extent.rank);
    }
    else {
        assert(version == H5S_HYPER_VERSION_1);
        assert(enc_size == 4);
        /* Version 1 */
        /* Basic number of bytes required to serialize hyperslab selection:
         * <type (4 bytes)> + <version (4 bytes)> + <padding (4 bytes)> +
         * <length (4 bytes)> + <rank (4 bytes)> + <# of blocks (4 bytes)> +
         * (2 (starting/ending offset) * <enc_size (4 bytes)> * <rank> * <# of blocks) =
         * = 24 bytes + (2 * 4 * rank * block_count)
         */
        ret_value = 24;
        H5_CHECK_OVERFLOW((8 * space->extent.rank * block_count), hsize_t, hssize_t);
        ret_value += (hssize_t)(8 * space->extent.rank * block_count);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_serial_size() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_serialize_helper
 PURPOSE
    Serialize the current selection into a user-provided buffer.
 USAGE
    void H5S__hyper_serialize_helper(spans, start, end, rank, enc_size, buf)
        H5S_hyper_span_info_t *spans;   IN: Hyperslab span tree to serialize
        hssize_t start[];       IN/OUT: Accumulated start points
        hssize_t end[];         IN/OUT: Accumulated end points
        hsize_t rank;           IN: Current rank looking at
        uint8_t enc_size        IN: Encoded size of hyperslab selection info
        uint8_t *buf;           OUT: Buffer to put serialized selection into
 RETURNS
    None
 DESCRIPTION
    Serializes the current element selection into a buffer.  (Primarily for
    storing on disk).
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static void
H5S__hyper_serialize_helper(const H5S_hyper_span_info_t *spans, hsize_t *start, hsize_t *end, hsize_t rank,
                            uint8_t enc_size, uint8_t **p)
{
    H5S_hyper_span_t *curr;      /* Pointer to current hyperslab span */
    uint8_t          *pp = (*p); /* Local pointer for decoding */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(spans);
    assert(start);
    assert(end);
    assert(rank < H5S_MAX_RANK);
    assert(p && pp);

    /* Walk through the list of spans, recursing or outputting them */
    curr = spans->head;
    while (curr != NULL) {
        /* Recurse if this node has down spans */
        if (curr->down != NULL) {
            /* Add the starting and ending points for this span to the list */
            start[rank] = curr->low;
            end[rank]   = curr->high;

            /* Recurse down to the next dimension */
            H5S__hyper_serialize_helper(curr->down, start, end, rank + 1, enc_size, &pp);
        } /* end if */
        else {
            hsize_t u; /* Index variable */

            /* Encode all the previous dimensions starting & ending points */
            switch (enc_size) {
                case H5S_SELECT_INFO_ENC_SIZE_2:
                    /* Encode previous starting points */
                    for (u = 0; u < rank; u++)
                        UINT16ENCODE(pp, (uint16_t)start[u]);

                    /* Encode starting point for this span */
                    UINT16ENCODE(pp, (uint16_t)curr->low);

                    /* Encode previous ending points */
                    for (u = 0; u < rank; u++)
                        UINT16ENCODE(pp, (uint16_t)end[u]);

                    /* Encode starting point for this span */
                    UINT16ENCODE(pp, (uint16_t)curr->high);
                    break;

                case H5S_SELECT_INFO_ENC_SIZE_4:
                    /* Encode previous starting points */
                    for (u = 0; u < rank; u++)
                        UINT32ENCODE(pp, (uint32_t)start[u]);

                    /* Encode starting point for this span */
                    UINT32ENCODE(pp, (uint32_t)curr->low);

                    /* Encode previous ending points */
                    for (u = 0; u < rank; u++)
                        UINT32ENCODE(pp, (uint32_t)end[u]);

                    /* Encode starting point for this span */
                    UINT32ENCODE(pp, (uint32_t)curr->high);
                    break;

                case H5S_SELECT_INFO_ENC_SIZE_8:
                    /* Encode previous starting points */
                    for (u = 0; u < rank; u++)
                        UINT64ENCODE(pp, (uint64_t)start[u]);

                    /* Encode starting point for this span */
                    UINT64ENCODE(pp, (uint64_t)curr->low);

                    /* Encode previous ending points */
                    for (u = 0; u < rank; u++)
                        UINT64ENCODE(pp, (uint64_t)end[u]);

                    /* Encode starting point for this span */
                    UINT64ENCODE(pp, (uint64_t)curr->high);
                    break;

                default:
                    assert(0 && "Unknown enc size?!?");

            } /* end switch */
        }     /* end else */

        /* Advance to next node */
        curr = curr->next;
    } /* end while */

    /* Update encoding pointer */
    *p = pp;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5S__hyper_serialize_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_serialize
 PURPOSE
    Serialize the current selection into a user-provided buffer.
 USAGE
    herr_t H5S__hyper_serialize(space, p)
        H5S_t *space;           IN: Dataspace with selection to serialize
        uint8_t **p;            OUT: Pointer to buffer to put serialized
                                selection.  Will be advanced to end of
                                serialized selection.
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Serializes the current element selection into a buffer.  (Primarily for
    storing on disk).
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_serialize(H5S_t *space, uint8_t **p)
{
    const H5S_hyper_dim_t *diminfo;                 /* Alias for dataspace's diminfo information */
    hsize_t                tmp_count[H5S_MAX_RANK]; /* Temporary hyperslab counts */
    hsize_t                offset[H5S_MAX_RANK];    /* Offset of element in dataspace */
    hsize_t                start[H5S_MAX_RANK];     /* Location of start of hyperslab */
    hsize_t                end[H5S_MAX_RANK];       /* Location of end of hyperslab */
    uint8_t               *pp;                      /* Local pointer for encoding */
    uint8_t               *lenp = NULL;             /* pointer to length location for later storage */
    uint32_t               len  = 0;                /* number of bytes used */
    uint32_t               version;                 /* Version number */
    uint8_t                flags       = 0;         /* Flags for message */
    hsize_t                block_count = 0;         /* block counter for regular hyperslabs */
    unsigned               fast_dim;            /* Rank of the fastest changing dimension for the dataspace */
    unsigned               ndims;               /* Rank of the dataspace */
    unsigned               u;                   /* Local counting variable */
    bool                   complete = false;    /* Whether we are done with the iteration */
    bool                   is_regular;          /* Whether selection is regular */
    uint8_t                enc_size;            /* Encoded size */
    herr_t                 ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(space);
    assert(p);
    pp = (*p);
    assert(pp);

    /* Set some convenience values */
    ndims   = space->extent.rank;
    diminfo = space->select.sel_info.hslab->diminfo.opt;

    /* Calculate the # of blocks */
    if (space->select.sel_info.hslab->unlim_dim < 0) /* ! H5S_UNLIMITED */
        block_count = H5S__get_select_hyper_nblocks(space, false);

    /* Determine the version and the encoded size */
    if (H5S__hyper_get_version_enc_size(space, block_count, &version, &enc_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't determine hyper version & enc_size");

    is_regular = H5S__hyper_is_regular(space);
    if (is_regular && (version == H5S_HYPER_VERSION_2 || version == H5S_HYPER_VERSION_3))
        flags |= H5S_HYPER_REGULAR;

    /* Store the preamble information */
    UINT32ENCODE(pp, (uint32_t)H5S_GET_SELECT_TYPE(space)); /* Store the type of selection */
    UINT32ENCODE(pp, version);                              /* Store the version number */

    if (version >= 3) {
        *(pp)++ = flags;    /* Store the flags */
        *(pp)++ = enc_size; /* Store size of offset info */
    }                       /* end if */
    else {
        if (version == 2)
            *(pp)++ = flags; /* Store the flags */
        else
            UINT32ENCODE(pp, (uint32_t)0); /* Store the un-used padding */
        lenp = pp;                         /* keep the pointer to the length location for later */
        pp += 4;                           /* skip over space for length */

        len += 4; /* ndims */
    }             /* end else */

    /* Encode number of dimensions */
    UINT32ENCODE(pp, (uint32_t)ndims);

    if (is_regular) {
        if (version >= H5S_HYPER_VERSION_2) {
            assert(H5S_UNLIMITED == HSIZE_UNDEF);

            /* Iterate over dimensions */
            /* Encode start/stride/block/count */
            switch (enc_size) {
                case H5S_SELECT_INFO_ENC_SIZE_2:
                    assert(version == H5S_HYPER_VERSION_3);
                    for (u = 0; u < space->extent.rank; u++) {
                        UINT16ENCODE(pp, diminfo[u].start);
                        UINT16ENCODE(pp, diminfo[u].stride);
                        if (diminfo[u].count == H5S_UNLIMITED)
                            UINT16ENCODE(pp, H5S_UINT16_MAX);
                        else
                            UINT16ENCODE(pp, diminfo[u].count);
                        if (diminfo[u].block == H5S_UNLIMITED)
                            UINT16ENCODE(pp, H5S_UINT16_MAX);
                        else
                            UINT16ENCODE(pp, diminfo[u].block);
                    } /* end for */
                    break;

                case H5S_SELECT_INFO_ENC_SIZE_4:
                    assert(version == H5S_HYPER_VERSION_3);
                    for (u = 0; u < space->extent.rank; u++) {
                        UINT32ENCODE(pp, diminfo[u].start);
                        UINT32ENCODE(pp, diminfo[u].stride);
                        if (diminfo[u].count == H5S_UNLIMITED)
                            UINT32ENCODE(pp, H5S_UINT32_MAX);
                        else
                            UINT32ENCODE(pp, diminfo[u].count);
                        if (diminfo[u].block == H5S_UNLIMITED)
                            UINT32ENCODE(pp, H5S_UINT32_MAX);
                        else
                            UINT32ENCODE(pp, diminfo[u].block);
                    } /* end for */
                    break;

                case H5S_SELECT_INFO_ENC_SIZE_8:
                    assert(version == H5S_HYPER_VERSION_2 || version == H5S_HYPER_VERSION_3);
                    for (u = 0; u < space->extent.rank; u++) {
                        UINT64ENCODE(pp, diminfo[u].start);
                        UINT64ENCODE(pp, diminfo[u].stride);
                        if (diminfo[u].count == H5S_UNLIMITED)
                            UINT64ENCODE(pp, H5S_UINT64_MAX);
                        else
                            UINT64ENCODE(pp, diminfo[u].count);
                        if (diminfo[u].block == H5S_UNLIMITED)
                            UINT64ENCODE(pp, H5S_UINT64_MAX);
                        else
                            UINT64ENCODE(pp, diminfo[u].block);
                    } /* end for */
                    if (version == H5S_HYPER_VERSION_2)
                        len += (4 * space->extent.rank * 8);
                    break;
                default:
                    HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL,
                                "unknown offset info size for hyperslab");
                    break;
            } /* end switch */
        }     /* end if */
        else {
            assert(version == H5S_HYPER_VERSION_1);

            /* Set some convenience values */
            fast_dim = ndims - 1;

            /* Encode number of hyperslabs */
            H5_CHECK_OVERFLOW(block_count, hsize_t, uint32_t);
            UINT32ENCODE(pp, (uint32_t)block_count);
            len += 4;

            /* Now serialize the information for the regular hyperslab */

            /* Build the tables of count sizes as well as the initial offset */
            for (u = 0; u < ndims; u++) {
                tmp_count[u] = diminfo[u].count;
                offset[u]    = diminfo[u].start;
            } /* end for */

            /* Go iterate over the hyperslabs */
            while (complete == false) {
                /* Iterate over the blocks in the fastest dimension */
                while (tmp_count[fast_dim] > 0) {
                    /* Add 8 bytes times the rank for each hyperslab selected */
                    len += 8 * ndims;

                    /* Encode hyperslab starting location */
                    for (u = 0; u < ndims; u++)
                        UINT32ENCODE(pp, (uint32_t)offset[u]);

                    /* Encode hyperslab ending location */
                    for (u = 0; u < ndims; u++)
                        UINT32ENCODE(pp, (uint32_t)(offset[u] + (diminfo[u].block - 1)));

                    /* Move the offset to the next sequence to start */
                    offset[fast_dim] += diminfo[fast_dim].stride;

                    /* Decrement the block count */
                    tmp_count[fast_dim]--;
                } /* end while */

                /* Work on other dimensions if necessary */
                if (fast_dim > 0) {
                    int temp_dim; /* Temporary rank holder */

                    /* Reset the block counts */
                    tmp_count[fast_dim] = diminfo[fast_dim].count;

                    /* Bubble up the decrement to the slower changing dimensions */
                    temp_dim = (int)fast_dim - 1;
                    while (temp_dim >= 0 && complete == false) {
                        /* Decrement the block count */
                        tmp_count[temp_dim]--;

                        /* Check if we have more blocks left */
                        if (tmp_count[temp_dim] > 0)
                            break;

                        /* Check for getting out of iterator */
                        if (temp_dim == 0)
                            complete = true;

                        /* Reset the block count in this dimension */
                        tmp_count[temp_dim] = diminfo[temp_dim].count;

                        /* Wrapped a dimension, go up to next dimension */
                        temp_dim--;
                    } /* end while */
                }     /* end if */
                else
                    break; /* Break out now, for 1-D selections */

                /* Re-compute offset array */
                for (u = 0; u < ndims; u++)
                    offset[u] = diminfo[u].start + diminfo[u].stride * (diminfo[u].count - tmp_count[u]);
            } /* end while */
        }     /* end else */
    }         /* end if */
    else {    /* irregular */
        /* Encode number of hyperslabs */
        switch (enc_size) {
            case H5S_SELECT_INFO_ENC_SIZE_2:
                assert(version == H5S_HYPER_VERSION_3);
                H5_CHECK_OVERFLOW(block_count, hsize_t, uint16_t);
                UINT16ENCODE(pp, (uint16_t)block_count);
                break;

            case H5S_SELECT_INFO_ENC_SIZE_4:
                assert(version == H5S_HYPER_VERSION_1 || version == H5S_HYPER_VERSION_3);
                H5_CHECK_OVERFLOW(block_count, hsize_t, uint32_t);
                UINT32ENCODE(pp, (uint32_t)block_count);
                break;

            case H5S_SELECT_INFO_ENC_SIZE_8:
                assert(version == H5S_HYPER_VERSION_3);
                UINT64ENCODE(pp, block_count);
                break;

            default:
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unknown offset info size for hyperslab");
                break;
        } /* end switch */

        if (version == H5S_HYPER_VERSION_1) {
            len += 4; /* block_count */

            /* Add 8 bytes times the rank for each hyperslab selected */
            H5_CHECK_OVERFLOW((8 * ndims * block_count), hsize_t, size_t);
            len += (uint32_t)(8 * ndims * block_count);
        } /* end if */

        H5S__hyper_serialize_helper(space->select.sel_info.hslab->span_lst, start, end, (hsize_t)0, enc_size,
                                    &pp);
    } /* end else */

    /* Encode length */
    if (version <= H5S_HYPER_VERSION_2)
        UINT32ENCODE(lenp, (uint32_t)len); /* Store the length of the extra information */

    /* Update encoding pointer */
    *p = pp;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_serialize() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_deserialize
 PURPOSE
    Deserialize the current selection from a user-provided buffer.
 USAGE
    herr_t H5S__hyper_deserialize(space, p)
        H5S_t **space;          IN/OUT: Dataspace pointer to place
                                selection into
        uint8 **p;              OUT: Pointer to buffer holding serialized
                                selection.  Will be advanced to end of
                                serialized selection.
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Deserializes the current selection into a buffer.  (Primarily for retrieving
    from disk).
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_deserialize(H5S_t **space, const uint8_t **p, const size_t p_size, bool skip)
{
    H5S_t *tmp_space = NULL;                    /* Pointer to actual dataspace to use,
                                                   either *space or a newly allocated one */
    hsize_t        dims[H5S_MAX_RANK];          /* Dimension sizes */
    hsize_t        start[H5S_MAX_RANK];         /* hyperslab start information */
    hsize_t        block[H5S_MAX_RANK];         /* hyperslab block information */
    uint32_t       version;                     /* Version number */
    uint8_t        flags    = 0;                /* Flags */
    uint8_t        enc_size = 0;                /* Encoded size of selection info */
    unsigned       rank;                        /* rank of points */
    const uint8_t *pp;                          /* Local pointer for decoding */
    unsigned       u;                           /* Local counting variable */
    herr_t         ret_value = FAIL;            /* return value */
    const uint8_t *p_end     = *p + p_size - 1; /* Pointer to last valid byte in buffer */
    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(p);
    pp = (*p);
    assert(pp);

    /* As part of the efforts to push all selection-type specific coding
       to the callbacks, the coding for the allocation of a null dataspace
       is moved from H5S_select_deserialize() in H5Sselect.c to here.
       This is needed for decoding virtual layout in H5O__layout_decode() */
    /* Allocate space if not provided */
    if (!*space) {
        if (NULL == (tmp_space = H5S_create(H5S_SIMPLE)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "can't create dataspace");
    } /* end if */
    else
        tmp_space = *space;

    /* Decode version */
    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint32_t), p_end))
        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL, "buffer overflow while decoding selection version");
    UINT32DECODE(pp, version);

    if (version < H5S_HYPER_VERSION_1 || version > H5S_HYPER_VERSION_LATEST)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "bad version number for hyperslab selection");

    if (version >= (uint32_t)H5S_HYPER_VERSION_2) {
        /* Decode flags */
        if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, 1, p_end))
            HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL, "buffer overflow while decoding selection flags");
        flags = *(pp)++;

        if (version >= (uint32_t)H5S_HYPER_VERSION_3) {
            /* decode size of offset info */
            if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, 1, p_end))
                HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                            "buffer overflow while decoding selection encoding size");
            enc_size = *(pp)++;
        }
        else {
            /* Skip over the remainder of the header */
            if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, 4, p_end))
                HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                            "buffer overflow while decoding selection header");
            pp += 4;
            enc_size = H5S_SELECT_INFO_ENC_SIZE_8;
        } /* end else */

        /* Check for unknown flags */
        if (flags & ~H5S_SELECT_FLAG_BITS)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTLOAD, FAIL, "unknown flag for selection");
    }
    else {
        /* Skip over the remainder of the header */
        if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, 8, p_end))
            HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL, "buffer overflow while decoding selection header");
        pp += 8;
        enc_size = H5S_SELECT_INFO_ENC_SIZE_4;
    } /* end else */

    /* Check encoded */
    if (enc_size & ~H5S_SELECT_INFO_ENC_SIZE_BITS)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTLOAD, FAIL, "unknown size of point/offset info for selection");

    /* Decode the rank of the point selection */
    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint32_t), p_end))
        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL, "buffer overflow while decoding selection rank");
    UINT32DECODE(pp, rank);

    if (!*space) {
        /* Patch the rank of the allocated dataspace */
        memset(dims, 0, (size_t)rank * sizeof(dims[0]));
        if (H5S_set_extent_simple(tmp_space, rank, dims, NULL) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't set dimensions");
    } /* end if */
    else
        /* Verify the rank of the provided dataspace */
        if (rank != tmp_space->extent.rank)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL,
                        "rank of serialized selection does not match dataspace");

    if (flags & H5S_HYPER_REGULAR) {
        hsize_t stride[H5S_MAX_RANK]; /* Hyperslab stride information */
        hsize_t count[H5S_MAX_RANK];  /* Hyperslab count information */

        /* Sanity checks */
        assert(H5S_UNLIMITED == HSIZE_UNDEF);
        assert(version >= H5S_HYPER_VERSION_2);

        /* Decode start/stride/block/count */
        switch (enc_size) {
            case H5S_SELECT_INFO_ENC_SIZE_2:
                for (u = 0; u < tmp_space->extent.rank; u++) {
                    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, 4 * sizeof(uint16_t), p_end))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                                    "buffer overflow while decoding selection ranks");

                    UINT16DECODE(pp, start[u]);
                    UINT16DECODE(pp, stride[u]);

                    UINT16DECODE(pp, count[u]);
                    if ((uint16_t)count[u] == H5S_UINT16_MAX)
                        count[u] = H5S_UNLIMITED;

                    UINT16DECODE(pp, block[u]);
                    if ((uint16_t)block[u] == H5S_UINT16_MAX)
                        block[u] = H5S_UNLIMITED;
                } /* end for */
                break;

            case H5S_SELECT_INFO_ENC_SIZE_4:
                for (u = 0; u < tmp_space->extent.rank; u++) {
                    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, 4 * sizeof(uint32_t), p_end))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                                    "buffer overflow while decoding selection ranks");

                    UINT32DECODE(pp, start[u]);
                    UINT32DECODE(pp, stride[u]);

                    UINT32DECODE(pp, count[u]);
                    if ((uint32_t)count[u] == H5S_UINT32_MAX)
                        count[u] = H5S_UNLIMITED;

                    UINT32DECODE(pp, block[u]);
                    if ((uint32_t)block[u] == H5S_UINT32_MAX)
                        block[u] = H5S_UNLIMITED;
                } /* end for */
                break;

            case H5S_SELECT_INFO_ENC_SIZE_8:
                for (u = 0; u < tmp_space->extent.rank; u++) {
                    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, 4 * sizeof(uint64_t), p_end))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                                    "buffer overflow while decoding selection ranks");

                    UINT64DECODE(pp, start[u]);
                    UINT64DECODE(pp, stride[u]);

                    UINT64DECODE(pp, count[u]);
                    if ((uint64_t)count[u] == H5S_UINT64_MAX)
                        count[u] = H5S_UNLIMITED;

                    UINT64DECODE(pp, block[u]);
                    if ((uint64_t)block[u] == H5S_UINT64_MAX)
                        block[u] = H5S_UNLIMITED;
                } /* end for */
                break;

            default:
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unknown offset info size for hyperslab");
                break;
        } /* end switch */

        /* Select the hyperslab to the current selection */
        if ((ret_value = H5S_select_hyperslab(tmp_space, H5S_SELECT_SET, start, stride, count, block)) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't change selection");
    } /* end if */
    else {
        const hsize_t *stride;            /* Hyperslab stride information */
        const hsize_t *count;             /* Hyperslab count information */
        hsize_t        end[H5S_MAX_RANK]; /* Hyperslab end information */
        hsize_t       *tstart;            /* Temporary hyperslab pointers */
        hsize_t       *tend;              /* Temporary hyperslab pointers */
        hsize_t       *tblock;            /* Temporary hyperslab pointers */
        size_t         num_elem;          /* Number of elements in selection */
        unsigned       v;                 /* Local counting variable */

        /* Decode the number of blocks */
        switch (enc_size) {
            case H5S_SELECT_INFO_ENC_SIZE_2:
                if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint16_t), p_end))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                                "buffer overflow while decoding number of selection blocks");
                UINT16DECODE(pp, num_elem);
                break;

            case H5S_SELECT_INFO_ENC_SIZE_4:
                if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint32_t), p_end))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                                "buffer overflow while decoding number of selection blocks");
                UINT32DECODE(pp, num_elem);
                break;

            case H5S_SELECT_INFO_ENC_SIZE_8:
                if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint64_t), p_end))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                                "buffer overflow while decoding number of selection blocks");
                UINT64DECODE(pp, num_elem);
                break;

            default:
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unknown offset info size for hyperslab");
                break;
        } /* end switch */

        /* Set the count & stride for all blocks */
        stride = count = H5S_hyper_ones_g;

        /* Retrieve the coordinates from the buffer */
        for (u = 0; u < num_elem; u++) {
            /* Decode the starting and ending points */
            switch (enc_size) {
                case H5S_SELECT_INFO_ENC_SIZE_2:
                    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, rank * 2 * sizeof(uint16_t), p_end))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                                    "buffer overflow while decoding selection coordinates");

                    for (tstart = start, v = 0; v < rank; v++, tstart++)
                        UINT16DECODE(pp, *tstart);
                    for (tend = end, v = 0; v < rank; v++, tend++)
                        UINT16DECODE(pp, *tend);
                    break;

                case H5S_SELECT_INFO_ENC_SIZE_4:
                    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, rank * 2 * sizeof(uint32_t), p_end))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                                    "buffer overflow while decoding selection coordinates");

                    for (tstart = start, v = 0; v < rank; v++, tstart++)
                        UINT32DECODE(pp, *tstart);
                    for (tend = end, v = 0; v < rank; v++, tend++)
                        UINT32DECODE(pp, *tend);
                    break;

                case H5S_SELECT_INFO_ENC_SIZE_8:
                    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, rank * 2 * sizeof(uint64_t), p_end))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                                    "buffer overflow while decoding selection coordinates");

                    for (tstart = start, v = 0; v < rank; v++, tstart++)
                        UINT64DECODE(pp, *tstart);
                    for (tend = end, v = 0; v < rank; v++, tend++)
                        UINT64DECODE(pp, *tend);
                    break;

                default:
                    HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL,
                                "unknown offset info size for hyperslab");
                    break;
            } /* end switch */

            /* Change the ending points into blocks */
            for (tblock = block, tstart = start, tend = end, v = 0; v < rank; v++, tstart++, tend++, tblock++)
                *tblock = (*tend - *tstart) + 1;

            /* Select or add the hyperslab to the current selection */
            if ((ret_value = H5S_select_hyperslab(tmp_space, (u == 0 ? H5S_SELECT_SET : H5S_SELECT_OR), start,
                                                  stride, count, block)) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't change selection");
        } /* end for */
    }     /* end else */

    /* Update decoding pointer */
    *p = pp;

    /* Return space to the caller if allocated */
    if (!*space)
        *space = tmp_space;

done:
    /* Free temporary space if not passed to caller (only happens on error) */
    if (!*space && tmp_space)
        if (H5S_close(tmp_space) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "can't close dataspace");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_deserialize() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_span_blocklist
 PURPOSE
    Get a list of hyperslab blocks currently selected
 USAGE
    herr_t H5S__hyper_span_blocklist(spans, start, end, rank, startblock, numblocks, buf)
        H5S_hyper_span_info_t *spans;   IN: Dataspace pointer of selection to query
        hsize_t start[];       IN/OUT: Accumulated start points
        hsize_t end[];         IN/OUT: Accumulated end points
        hsize_t rank;           IN: Rank of dataspace
        hsize_t *startblock;    IN/OUT: Hyperslab block to start with
        hsize_t *numblocks;     IN/OUT: Number of hyperslab blocks to get
        hsize_t **buf;          OUT: List of hyperslab blocks selected
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
        Puts a list of the hyperslab blocks into the user's buffer.  The blocks
    start with the '*startblock'th block in the list of blocks and put
    '*numblocks' number of blocks into the user's buffer (or until the end of
    the list of blocks, whichever happens first)
        The block coordinates have the same dimensionality (rank) as the
    dataspace they are located within.  The list of blocks is formatted as
    follows: <"start" coordinate> immediately followed by <"opposite" corner
    coordinate>, followed by the next "start" and "opposite" coordinate, etc.
    until all the block information requested has been put into the user's
    buffer.
        No guarantee of any order of the blocks is implied.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_span_blocklist(const H5S_hyper_span_info_t *spans, hsize_t start[], hsize_t end[], hsize_t rank,
                          hsize_t *startblock, hsize_t *numblocks, hsize_t **buf)
{
    const H5S_hyper_span_t *curr;                /* Pointer to current hyperslab span */
    herr_t                  ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(spans);
    assert(rank < H5S_MAX_RANK);
    assert(start);
    assert(end);
    assert(startblock);
    assert(numblocks && *numblocks > 0);
    assert(buf && *buf);

    /* Walk through the list of spans, recursing or outputting them */
    curr = spans->head;
    while (curr != NULL && *numblocks > 0) {
        /* Recurse if this node has down spans */
        if (curr->down != NULL) {
            /* Add the starting and ending points for this span to the list */
            start[rank] = curr->low;
            end[rank]   = curr->high;

            /* Recurse down to the next dimension */
            if (H5S__hyper_span_blocklist(curr->down, start, end, (rank + 1), startblock, numblocks, buf) < 0)
                HGOTO_ERROR(H5E_INTERNAL, H5E_CANTFREE, FAIL, "failed to release hyperslab spans");
        } /* end if */
        else {
            /* Skip this block if we haven't skipped all the startblocks yet */
            if (*startblock > 0) {
                /* Decrement the starting block */
                (*startblock)--;
            } /* end if */
            /* Process this block */
            else {
                /* Encode all the previous dimensions starting & ending points */

                /* Copy previous starting points */
                H5MM_memcpy(*buf, start, rank * sizeof(hsize_t));
                (*buf) += rank;

                /* Copy starting point for this span */
                **buf = curr->low;
                (*buf)++;

                /* Copy previous ending points */
                H5MM_memcpy(*buf, end, rank * sizeof(hsize_t));
                (*buf) += rank;

                /* Copy ending point for this span */
                **buf = curr->high;
                (*buf)++;

                /* Decrement the number of blocks processed */
                (*numblocks)--;
            } /* end else */
        }     /* end else */

        /* Advance to next node */
        curr = curr->next;
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_span_blocklist() */

/*--------------------------------------------------------------------------
 NAME
    H5S__get_select_hyper_blocklist
 PURPOSE
    Get the list of hyperslab blocks currently selected
 USAGE
    herr_t H5S__get_select_hyper_blocklist(space, startblock, numblocks, buf)
        H5S_t *space;           IN: Dataspace pointer of selection to query
        hsize_t startblock;     IN: Hyperslab block to start with
        hsize_t numblocks;      IN: Number of hyperslab blocks to get
        hsize_t *buf;           OUT: List of hyperslab blocks selected
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
        Puts a list of the hyperslab blocks into the user's buffer.  The blocks
    start with the 'startblock'th block in the list of blocks and put
    'numblocks' number of blocks into the user's buffer (or until the end of
    the list of blocks, whichever happens first)
        The block coordinates have the same dimensionality (rank) as the
    dataspace they are located within.  The list of blocks is formatted as
    follows: <"start" coordinate> immediately followed by <"opposite" corner
    coordinate>, followed by the next "start" and "opposite" coordinate, etc.
    until all the block information requested has been put into the user's
    buffer.
        No guarantee of any order of the blocks is implied.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__get_select_hyper_blocklist(H5S_t *space, hsize_t startblock, hsize_t numblocks, hsize_t *buf)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);
    assert(buf);
    assert(space->select.sel_info.hslab->unlim_dim < 0);

    /* Attempt to rebuild diminfo if it is invalid and has not been confirmed
     * to be impossible.
     */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_NO)
        H5S__hyper_rebuild(space);

    /* Check for a "regular" hyperslab selection */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        const H5S_hyper_dim_t *diminfo;                 /* Alias for dataspace's diminfo information */
        hsize_t                tmp_count[H5S_MAX_RANK]; /* Temporary hyperslab counts */
        hsize_t                offset[H5S_MAX_RANK];    /* Offset of element in dataspace */
        hsize_t                end[H5S_MAX_RANK];       /* End of elements in dataspace */
        unsigned               fast_dim; /* Rank of the fastest changing dimension for the dataspace */
        unsigned               ndims;    /* Rank of the dataspace */
        bool                   done;     /* Whether we are done with the iteration */
        unsigned               u;        /* Counter */

        /* Set some convenience values */
        ndims    = space->extent.rank;
        fast_dim = ndims - 1;

        /* Check which set of dimension information to use */
        if (space->select.sel_info.hslab->unlim_dim >= 0)
            /*
             * There is an unlimited dimension so we must use diminfo.opt as
             * it has been "clipped" to the current extent.
             */
            diminfo = space->select.sel_info.hslab->diminfo.opt;
        else
            /*
             * Use the "application dimension information" to pass back to
             * the user the blocks they set, not the optimized, internal
             * information.
             */
            diminfo = space->select.sel_info.hslab->diminfo.app;

        /* Build the tables of count sizes as well as the initial offset */
        for (u = 0; u < ndims; u++) {
            tmp_count[u] = diminfo[u].count;
            offset[u]    = diminfo[u].start;
            end[u]       = diminfo[u].start + (diminfo[u].block - 1);
        } /* end for */

        /* We're not done with the iteration */
        done = false;

        /* Go iterate over the hyperslabs */
        while (!done && numblocks > 0) {
            /* Skip over initial blocks */
            if (startblock > 0) {
                /* Skip all blocks in row */
                if (startblock >= tmp_count[fast_dim]) {
                    startblock -= tmp_count[fast_dim];
                    tmp_count[fast_dim] = 0;
                } /* end if */
                else {
                    /* Move the offset to the next sequence to start */
                    offset[fast_dim] += diminfo[fast_dim].stride * startblock;
                    end[fast_dim] += diminfo[fast_dim].stride * startblock;

                    /* Decrement the block count */
                    tmp_count[fast_dim] -= startblock;

                    /* Done with starting blocks */
                    startblock = 0;
                } /* end else */
            }     /* end if */

            /* Iterate over the blocks in the fastest dimension */
            while (tmp_count[fast_dim] > 0 && numblocks > 0) {
                /* Sanity check */
                assert(startblock == 0);

                /* Copy the starting location */
                H5MM_memcpy(buf, offset, sizeof(hsize_t) * ndims);
                buf += ndims;

                /* Compute the ending location */
                H5MM_memcpy(buf, end, sizeof(hsize_t) * ndims);
                buf += ndims;

                /* Decrement the number of blocks to retrieve */
                numblocks--;

                /* Move the offset to the next sequence to start */
                offset[fast_dim] += diminfo[fast_dim].stride;
                end[fast_dim] += diminfo[fast_dim].stride;

                /* Decrement the block count */
                tmp_count[fast_dim]--;
            } /* end while */

            /* Work on other dimensions if necessary */
            if (fast_dim > 0 && numblocks > 0) {
                int temp_dim; /* Temporary rank holder */

                /* Reset the block counts */
                tmp_count[fast_dim] = diminfo[fast_dim].count;

                /* Bubble up the decrement to the slower changing dimensions */
                temp_dim = (int)(fast_dim - 1);
                while (temp_dim >= 0 && !done) {
                    /* Decrement the block count */
                    tmp_count[temp_dim]--;

                    /* Check if we have more blocks left */
                    if (tmp_count[temp_dim] > 0)
                        break;

                    /* Reset the block count in this dimension */
                    tmp_count[temp_dim] = diminfo[temp_dim].count;

                    /* Check for getting out of iterator */
                    if (temp_dim == 0)
                        done = true;

                    /* Wrapped a dimension, go up to next dimension */
                    temp_dim--;
                } /* end while */
            }     /* end if */

            /* Re-compute offset & end arrays */
            if (!done)
                for (u = 0; u < ndims; u++) {
                    offset[u] = diminfo[u].start + diminfo[u].stride * (diminfo[u].count - tmp_count[u]);
                    end[u]    = offset[u] + (diminfo[u].block - 1);
                } /* end for */
        }         /* end while */
    }             /* end if */
    else {
        hsize_t start[H5S_MAX_RANK]; /* Location of start of hyperslab */
        hsize_t end[H5S_MAX_RANK];   /* Location of end of hyperslab */

        ret_value = H5S__hyper_span_blocklist(space->select.sel_info.hslab->span_lst, start, end, (hsize_t)0,
                                              &startblock, &numblocks, &buf);
    } /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__get_select_hyper_blocklist() */

/*--------------------------------------------------------------------------
 NAME
    H5Sget_select_hyper_blocklist
 PURPOSE
    Get the list of hyperslab blocks currently selected
 USAGE
    herr_t H5Sget_select_hyper_blocklist(dsid, startblock, numblocks, buf)
        hid_t dsid;             IN: Dataspace ID of selection to query
        hsize_t startblock;     IN: Hyperslab block to start with
        hsize_t numblocks;      IN: Number of hyperslab blocks to get
        hsize_t buf[];          OUT: List of hyperslab blocks selected
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
        Puts a list of the hyperslab blocks into the user's buffer.  The blocks
    start with the 'startblock'th block in the list of blocks and put
    'numblocks' number of blocks into the user's buffer (or until the end of
    the list of blocks, whichever happen first)
        The block coordinates have the same dimensionality (rank) as the
    dataspace they are located within.  The list of blocks is formatted as
    follows: <"start" coordinate> immediately followed by <"opposite" corner
    coordinate>, followed by the next "start" and "opposite" coordinate, etc.
    until all the block information requested has been put into the user's
    buffer.
        No guarantee of any order of the blocks is implied.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Sget_select_hyper_blocklist(hid_t spaceid, hsize_t startblock, hsize_t numblocks,
                              hsize_t buf[/*numblocks*/] /*out*/)
{
    H5S_t *space;     /* Dataspace to modify selection of */
    herr_t ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "ihhx", spaceid, startblock, numblocks, buf);

    /* Check args */
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid pointer");
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (H5S_GET_SELECT_TYPE(space) != H5S_SEL_HYPERSLABS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a hyperslab selection");
    if (space->select.sel_info.hslab->unlim_dim >= 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "cannot get blocklist for unlimited selection");

    /* Go get the correct number of blocks */
    if (numblocks > 0)
        ret_value = H5S__get_select_hyper_blocklist(space, startblock, numblocks, buf);
    else
        ret_value = SUCCEED; /* Successfully got 0 blocks... */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sget_select_hyper_blocklist() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_bounds
 PURPOSE
    Gets the bounding box containing the selection.
 USAGE
    herr_t H5S__hyper_bounds(space, hsize_t *start, hsize_t *end)
        H5S_t *space;           IN: Dataspace pointer of selection to query
        hsize_t *start;         OUT: Starting coordinate of bounding box
        hsize_t *end;           OUT: Opposite coordinate of bounding box
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Retrieves the bounding box containing the current selection and places
    it into the user's buffers.  The start and end buffers must be large
    enough to hold the dataspace rank number of coordinates.  The bounding box
    exactly contains the selection, ie. if a 2-D element selection is currently
    defined with the following points: (4,5), (6,8) (10,7), the bounding box
    with be (4, 5), (10, 8).
        The bounding box calculations _does_ include the current offset of the
    selection within the dataspace extent.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_bounds(const H5S_t *space, hsize_t *start, hsize_t *end)
{
    const hsize_t *low_bounds, *high_bounds; /* Pointers to the correct pair of low & high bounds */
    herr_t         ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(space);
    assert(start);
    assert(end);

    /* Check which set of low & high bounds we should be using */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        low_bounds  = space->select.sel_info.hslab->diminfo.low_bounds;
        high_bounds = space->select.sel_info.hslab->diminfo.high_bounds;
    } /* end if */
    else {
        low_bounds  = space->select.sel_info.hslab->span_lst->low_bounds;
        high_bounds = space->select.sel_info.hslab->span_lst->high_bounds;
    } /* end else */

    /* Check for offset set */
    if (space->select.offset_changed) {
        unsigned u; /* Local index variable */

        /* Loop over dimensions */
        for (u = 0; u < space->extent.rank; u++) {
            /* Sanity check */
            assert(low_bounds[u] <= high_bounds[u]);

            /* Check for offset moving selection negative */
            if (((hssize_t)low_bounds[u] + space->select.offset[u]) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL, "offset moves selection out of bounds");

            /* Set the low & high bounds in this dimension */
            start[u] = (hsize_t)((hssize_t)low_bounds[u] + space->select.offset[u]);
            if ((int)u == space->select.sel_info.hslab->unlim_dim)
                end[u] = H5S_UNLIMITED;
            else
                end[u] = (hsize_t)((hssize_t)high_bounds[u] + space->select.offset[u]);
        } /* end for */
    }     /* end if */
    else {
        /* Offset vector is still zeros, just copy low & high bounds */
        H5MM_memcpy(start, low_bounds, sizeof(hsize_t) * space->extent.rank);
        H5MM_memcpy(end, high_bounds, sizeof(hsize_t) * space->extent.rank);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_bounds() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_offset
 PURPOSE
    Gets the linear offset of the first element for the selection.
 USAGE
    herr_t H5S__hyper_offset(space, offset)
        const H5S_t *space;     IN: Dataspace pointer of selection to query
        hsize_t *offset;        OUT: Linear offset of first element in selection
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Retrieves the linear offset (in "units" of elements) of the first element
    selected within the dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Calling this function on a "none" selection returns fail.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_offset(const H5S_t *space, hsize_t *offset)
{
    const hssize_t *sel_offset;          /* Pointer to the selection's offset */
    const hsize_t  *dim_size;            /* Pointer to a dataspace's extent */
    hsize_t         accum;               /* Accumulator for dimension sizes */
    unsigned        rank;                /* Dataspace rank */
    int             i;                   /* index variable */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(space && space->extent.rank > 0);
    assert(offset);

    /* Start at linear offset 0 */
    *offset = 0;

    /* Set up pointers to arrays of values */
    rank       = space->extent.rank;
    sel_offset = space->select.offset;
    dim_size   = space->extent.size;

    /* Check for a "regular" hyperslab selection */
    /* (No need to rebuild the dimension info yet -QAK) */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        const H5S_hyper_dim_t *diminfo =
            space->select.sel_info.hslab->diminfo.opt; /* Local alias for diminfo */

        /* Loop through starting coordinates, calculating the linear offset */
        accum = 1;
        for (i = (int)(rank - 1); i >= 0; i--) {
            hssize_t hyp_offset =
                (hssize_t)diminfo[i].start + sel_offset[i]; /* Hyperslab's offset in this dimension */

            /* Check for offset moving selection out of the dataspace */
            if (hyp_offset < 0 || (hsize_t)hyp_offset >= dim_size[i])
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL, "offset moves selection out of bounds");

            /* Add the hyperslab's offset in this dimension to the total linear offset */
            *offset += (hsize_t)(hyp_offset * (hssize_t)accum);

            /* Increase the accumulator */
            accum *= dim_size[i];
        } /* end for */
    }     /* end if */
    else {
        const H5S_hyper_span_t *span;                    /* Hyperslab span node */
        hsize_t                 dim_accum[H5S_MAX_RANK]; /* Accumulators, for each dimension */

        /* Calculate the accumulator for each dimension */
        accum = 1;
        for (i = (int)(rank - 1); i >= 0; i--) {
            /* Set the accumulator for this dimension */
            dim_accum[i] = accum;

            /* Increase the accumulator */
            accum *= dim_size[i];
        } /* end for */

        /* Get information for the first span, in the slowest changing dimension */
        span = space->select.sel_info.hslab->span_lst->head;

        /* Work down the spans, computing the linear offset */
        i = 0;
        while (span) {
            hssize_t hyp_offset =
                (hssize_t)span->low + sel_offset[i]; /* Hyperslab's offset in this dimension */

            /* Check for offset moving selection out of the dataspace */
            if (hyp_offset < 0 || (hsize_t)hyp_offset >= dim_size[i])
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL, "offset moves selection out of bounds");

            /* Add the hyperslab's offset in this dimension to the total linear offset */
            *offset += (hsize_t)(hyp_offset * (hssize_t)dim_accum[i]);

            /* Advance to first span in "down" dimension */
            if (span->down) {
                assert(span->down->head);
                span = span->down->head;
            } /* end if */
            else
                span = NULL;
            i++;
        } /* end while */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_offset() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_unlim_dim
 PURPOSE
    Return unlimited dimension of selection, or -1 if none
 USAGE
    int H5S__hyper_unlim_dim(space)
        H5S_t *space;           IN: Dataspace pointer to check
 RETURNS
    Unlimited dimension of selection, or -1 if none (never fails).
 DESCRIPTION
    Returns the index of the unlimited dimension of the selection, or -1
    if the selection has no unlimited dimension.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5S__hyper_unlim_dim(const H5S_t *space)
{
    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(space->select.sel_info.hslab->unlim_dim)
} /* end H5S__hyper_unlim_dim() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_num_elem_non_unlim
 PURPOSE
    Return number of elements in the non-unlimited dimensions
 USAGE
    herr_t H5S__hyper_num_elem_non_unlim(space,num_elem_non_unlim)
        H5S_t *space;           IN: Dataspace pointer to check
        hsize_t *num_elem_non_unlim; OUT: Number of elements in the non-unlimited dimensions
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Returns the number of elements in a slice through the non-unlimited
    dimensions of the selection.  Fails if the selection has no unlimited
    dimension.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_num_elem_non_unlim(const H5S_t *space, hsize_t *num_elem_non_unlim)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(space);
    assert(num_elem_non_unlim);

    /* Get number of elements in the non-unlimited dimensions */
    if (space->select.sel_info.hslab->unlim_dim >= 0)
        *num_elem_non_unlim = space->select.sel_info.hslab->num_elem_non_unlim;
    else
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "selection has no unlimited dimension");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_num_elem_non_unlim() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_is_contiguous
 PURPOSE
    Check if a hyperslab selection is contiguous within the dataspace extent.
 USAGE
    htri_t H5S__hyper_is_contiguous(space)
        H5S_t *space;           IN: Dataspace pointer to check
 RETURNS
    true/false/FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspace is contiguous.
    This is primarily used for reading the entire selection in one swoop.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5_ATTR_PURE htri_t
H5S__hyper_is_contiguous(const H5S_t *space)
{
    bool small_contiguous,      /* Flag for small contiguous block */
        large_contiguous;       /* Flag for large contiguous block */
    unsigned u;                 /* index variable */
    htri_t   ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);

    /* Check for a "regular" hyperslab selection */
    /* (No need to rebuild the dimension info yet -QAK) */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        const H5S_hyper_dim_t *diminfo =
            space->select.sel_info.hslab->diminfo.opt; /* local alias for diminfo */

        /*
         * For a regular hyperslab to be contiguous, it must have only one
         * block (i.e. count==1 in all dimensions) and the block size must be
         * the same as the dataspace extent's in all but the slowest changing
         * dimension. (dubbed "large contiguous" block)
         *
         * OR
         *
         * The selection must have only one block (i.e. count==1) in all
         * dimensions and the block size must be 1 in all but the fastest
         * changing dimension. (dubbed "small contiguous" block)
         */

        /* Initialize flags */
        large_contiguous = true;  /* assume true and reset if the dimensions don't match */
        small_contiguous = false; /* assume false initially */

        /* Check for a "large contiguous" block */
        for (u = 0; u < space->extent.rank; u++) {
            if (diminfo[u].count > 1) {
                large_contiguous = false;
                break;
            } /* end if */
            if (u > 0 && diminfo[u].block != space->extent.size[u]) {
                large_contiguous = false;
                break;
            } /* end if */
        }     /* end for */

        /* If we didn't find a large contiguous block, check for a small one */
        if (!large_contiguous) {
            small_contiguous = true;
            for (u = 0; u < space->extent.rank; u++) {
                if (diminfo[u].count > 1) {
                    small_contiguous = false;
                    break;
                } /* end if */
                if (u < (space->extent.rank - 1) && diminfo[u].block != 1) {
                    small_contiguous = false;
                    break;
                } /* end if */
            }     /* end for */
        }         /* end if */

        /* Indicate true if it's either a large or small contiguous block */
        if (large_contiguous || small_contiguous)
            ret_value = true;
    } /* end if */
    else {
        H5S_hyper_span_info_t *spans; /* Hyperslab span info node */
        H5S_hyper_span_t      *span;  /* Hyperslab span node */

        /*
         * For a hyperslab to be contiguous, it must have only one block and
         * either it's size must be the same as the dataspace extent's in all
         *      but the slowest changing dimension
         *   OR
         *      block size must be 1 in all but the fastest changing dimension.
         */
        /* Initialize flags */
        large_contiguous = true;  /* assume true and reset if the dimensions don't match */
        small_contiguous = false; /* assume false initially */

        /* Get information for slowest changing information */
        spans = space->select.sel_info.hslab->span_lst;
        span  = spans->head;

        /* If there are multiple spans in the slowest changing dimension, the selection isn't contiguous */
        if (span->next != NULL)
            large_contiguous = false;
        else {
            /* Now check the rest of the dimensions */
            if (span->down != NULL) {
                u = 1; /* Current dimension working on */

                /* Get the span information for the next fastest dimension */
                spans = span->down;

                /* Cycle down the spans until we run out of down spans or find a non-contiguous span */
                while (spans != NULL) {
                    span = spans->head;

                    /* Check that this is the only span and it spans the entire dimension */
                    if (span->next != NULL) {
                        large_contiguous = false;
                        break;
                    } /* end if */
                    else {
                        /* If this span doesn't cover the entire dimension, then this selection isn't
                         * contiguous */
                        if (((span->high - span->low) + 1) != space->extent.size[u]) {
                            large_contiguous = false;
                            break;
                        } /* end if */
                        else {
                            /* Walk down to the next span */
                            spans = span->down;

                            /* Increment dimension */
                            u++;
                        } /* end else */
                    }     /* end else */
                }         /* end while */
            }             /* end if */
        }                 /* end else */

        /* If we didn't find a large contiguous block, check for a small one */
        if (!large_contiguous) {
            small_contiguous = true;

            /* Get information for slowest changing information */
            spans = space->select.sel_info.hslab->span_lst;
            span  = spans->head;

            /* Current dimension working on */
            u = 0;

            /* Cycle down the spans until we run out of down spans or find a non-contiguous span */
            while (spans != NULL) {
                span = spans->head;

                /* Check that this is the only span and it spans the entire dimension */
                if (span->next != NULL) {
                    small_contiguous = false;
                    break;
                } /* end if */
                else {
                    /* If this span doesn't cover the entire dimension, then this selection isn't contiguous
                     */
                    if (u < (space->extent.rank - 1) && ((span->high - span->low) + 1) != 1) {
                        small_contiguous = false;
                        break;
                    } /* end if */
                    else {
                        /* Walk down to the next span */
                        spans = span->down;

                        /* Increment dimension */
                        u++;
                    } /* end else */
                }     /* end else */
            }         /* end while */
        }             /* end if */

        /* Indicate true if it's either a large or small contiguous block */
        if (large_contiguous || small_contiguous)
            ret_value = true;
    } /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_is_contiguous() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_is_single
 PURPOSE
    Check if a hyperslab selection is a single block within the dataspace extent.
 USAGE
    htri_t H5S__hyper_is_single(space)
        H5S_t *space;           IN: Dataspace pointer to check
 RETURNS
    true/false/FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspace is a single block.
    This is primarily used for reading the entire selection in one swoop.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5_ATTR_PURE htri_t
H5S__hyper_is_single(const H5S_t *space)
{
    htri_t ret_value = true; /* return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);

    /* Check for a "single" hyperslab selection */
    /* (No need to rebuild the dimension info yet, since the span-tree
     *  algorithm is fast -QAK)
     */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        unsigned u; /* index variable */

        /*
         * For a regular hyperslab to be single, it must have only one
         * block (i.e. count==1 in all dimensions)
         */

        /* Check for a single block */
        for (u = 0; u < space->extent.rank; u++)
            if (space->select.sel_info.hslab->diminfo.opt[u].count > 1)
                HGOTO_DONE(false);
    } /* end if */
    else {
        H5S_hyper_span_info_t *spans; /* Hyperslab span info node */

        /*
         * For a region to be single, it must have only one block
         */
        /* Get information for slowest changing information */
        spans = space->select.sel_info.hslab->span_lst;

        /* Cycle down the spans until we run out of down spans or find a non-contiguous span */
        while (spans != NULL) {
            H5S_hyper_span_t *span; /* Hyperslab span node */

            span = spans->head;

            /* Check that this is the only span and it spans the entire dimension */
            if (span->next != NULL)
                HGOTO_DONE(false);
            else
                /* Walk down to the next span */
                spans = span->down;
        } /* end while */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_is_single() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_is_regular
 PURPOSE
    Check if a hyperslab selection is "regular"
 USAGE
    htri_t H5S__hyper_is_regular(space)
        H5S_t *space;     IN: Dataspace pointer to check
 RETURNS
    true/false/FAIL
 DESCRIPTION
    Checks to see if the current selection in a dataspace is the a regular
    pattern.
    This is primarily used for reading the entire selection in one swoop.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static htri_t
H5S__hyper_is_regular(H5S_t *space)
{
    htri_t ret_value = FAIL; /* return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(space);

    /* Attempt to rebuild diminfo if it is invalid and has not been confirmed
     * to be impossible.
     */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_NO)
        H5S__hyper_rebuild(space);

    /* Only simple check for regular hyperslabs for now... */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES)
        ret_value = true;
    else
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_is_regular() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_spans_shape_same_helper
 PURPOSE
    Helper routine to check if two hyperslab span trees are the same shape
 USAGE
    bool H5S__hyper_spans_shape_same_helper(span1, span2, offset, rest_zeros)
        H5S_hyper_span_info_t *span_info1;      IN: First span tree to compare
        H5S_hyper_span_info_t *span_info2;      IN: Second span tree to compare
        hssize_t offset[];                      IN: Offset between the span trees
        bool rest_zeros[];                   IN: Array of flags which indicate
                                                    the rest of the offset[] array
                                                    is zero values.
 RETURNS
    true (1) or false (0) on success, can't fail
 DESCRIPTION
    Compare two hyperslab span trees to determine if they refer to a selection
    with the same shape, with a possible (constant) offset between their
    elements.  Very similar to H5S__hyper_cmp_spans, except the selected
    elements can be offset by a vector.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5_ATTR_PURE bool
H5S__hyper_spans_shape_same_helper(const H5S_hyper_span_info_t *span_info1,
                                   const H5S_hyper_span_info_t *span_info2, hssize_t offset[],
                                   bool rest_zeros[])
{
    bool ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(span_info1);
    assert(span_info2);
    assert(offset);
    assert(rest_zeros);

    /* Compare low & high bounds for this span list */
    /* (Could compare lower dimensions also, but not certain if
     *      that's worth it. - QAK, 2019/01/23)
     */
    if ((hsize_t)((hssize_t)span_info1->low_bounds[0] + offset[0]) != span_info2->low_bounds[0])
        HGOTO_DONE(false);
    else if ((hsize_t)((hssize_t)span_info1->high_bounds[0] + offset[0]) != span_info2->high_bounds[0])
        HGOTO_DONE(false);
    else {
        const H5S_hyper_span_t *span1;
        const H5S_hyper_span_t *span2;

        /* Get the pointers to the actual lists of spans */
        span1 = span_info1->head;
        span2 = span_info2->head;

        /* Sanity checking */
        assert(span1);
        assert(span2);

        /* infinite loop which must be broken out of */
        while (1) {
            /* Check for both spans being NULL */
            if (span1 == NULL && span2 == NULL)
                HGOTO_DONE(true);

            /* Check for one span being NULL */
            if (span1 == NULL || span2 == NULL)
                HGOTO_DONE(false);

            /* Check if the actual low & high span information is the same */
            if ((hsize_t)((hssize_t)span1->low + offset[0]) != span2->low ||
                (hsize_t)((hssize_t)span1->high + offset[0]) != span2->high)
                HGOTO_DONE(false);

            /* Check for down tree for this span */
            if (span1->down != NULL || span2->down != NULL) {
                /* If the rest of the span trees have a zero offset, use the faster comparison routine */
                if (rest_zeros[0]) {
                    if (!H5S__hyper_cmp_spans(span1->down, span2->down))
                        HGOTO_DONE(false);
                    else {
                        /* Keep going... */
                    } /* end else */
                }     /* end if */
                else {
                    if (!H5S__hyper_spans_shape_same_helper(span1->down, span2->down, &offset[1],
                                                            &rest_zeros[1]))
                        HGOTO_DONE(false);
                    else {
                        /* Keep going... */
                    } /* end else */
                }     /* end else */
            }         /* end if */
            else {
                /* Keep going... */
            } /* end else */

            /* Advance to the next nodes in the span list */
            span1 = span1->next;
            span2 = span2->next;
        } /* end while */
    }     /* end else */

    /* Fall through, with default return value of 'true' if spans were already visited */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_spans_shape_same_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_spans_shape_same
 PURPOSE
    Check if two hyperslab span trees are the same shape
 USAGE
    bool H5S__hyper_spans_shape_same(span1, span2)
        H5S_hyper_span_info_t *span_info1;      IN: First span tree to compare
        H5S_hyper_span_info_t *span_info2;      IN: Second span tree to compare
 RETURNS
    true (1) or false (0) on success, can't fail
 DESCRIPTION
    Compare two hyperslab span trees to determine if they refer to a selection
    with the same shape.  Very similar to H5S__hyper_cmp_spans, except the
    selected elements can be offset by a vector.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5_ATTR_PURE bool
H5S__hyper_spans_shape_same(const H5S_hyper_span_info_t *span_info1, const H5S_hyper_span_info_t *span_info2,
                            unsigned ndims)
{
    const H5S_hyper_span_t *span1;                /* Pointer to spans in first span tree */
    const H5S_hyper_span_t *span2;                /* Pointer to spans in second span tree */
    hssize_t                offset[H5S_MAX_RANK]; /* Offset vector for selections */
    bool     rest_zeros[H5S_MAX_RANK]; /* Vector of flags to indicate when remaining offset is all zero */
    bool     zero_offset;              /* Whether the two selections have a non-zero offset */
    unsigned u;                        /* Local index variable */
    bool     ret_value = true;         /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(span_info1);
    assert(span_info2);
    assert(ndims > 0);

    /* Initialize arrays */
    memset(offset, 0, sizeof(offset));
    memset(rest_zeros, 0, sizeof(rest_zeros));

    /* Check for an offset between the two selections */
    span1       = span_info1->head;
    span2       = span_info2->head;
    zero_offset = true;
    for (u = 0; u < ndims; u++) {
        /* Check for offset in this dimension */
        if (span1->low != span2->low) {
            offset[u] = (hssize_t)span2->low - (hssize_t)span1->low;

            /* Indicate that the offset vector is not all zeros */
            if (zero_offset)
                zero_offset = false;
        } /* end if */

        /* Sanity check */
        /* (Both span trees must have the same depth) */
        assert((span1->down && span2->down) || (NULL == span1->down && NULL == span2->down));

        /* Advance to next dimension */
        if (span1->down) {
            span1 = span1->down->head;
            span2 = span2->down->head;
        } /* end if */
    }     /* end for */

    /* Check if there's a "tail" of all zeros in a non-zero offset vector */
    if (!zero_offset) {
        int i; /* Local index variable */

        /* Find first non-zero offset, from the fastest dimension up */
        for (i = (int)(ndims - 1); i >= 0; i--)
            if (offset[i]) {
                rest_zeros[i] = true;
                break;
            } /* end if */

        /* Sanity check */
        /* (Must eventually have found a non-zero offset) */
        assert(i >= 0);
    } /* end if */

    /* If the offset vector is all zero, we can use the faster span tree
     *  comparison routine.  Otherwise, use a generalized version of that
     *  routine.
     */
    if (zero_offset)
        ret_value = H5S__hyper_cmp_spans(span_info1, span_info2);
    else
        ret_value = H5S__hyper_spans_shape_same_helper(span_info1, span_info2, offset, rest_zeros);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_spans_shape_same() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_shape_same
 PURPOSE
    Check if a two hyperslab selections are the same shape
 USAGE
    htri_t H5S__hyper_shape_same(space1, space2)
        H5S_t *space1;           IN: First dataspace to check
        H5S_t *space2;           IN: Second dataspace to check
 RETURNS
    true / false / FAIL
 DESCRIPTION
    Checks to see if the current selection in each dataspace are the same
    shape.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Handles when both are regular in an efficient way, otherwise converts
    both to span tree form (if necessary) and compares efficiently them in
    that form.

    Rank of space1 must always be >= to rank of space2.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static htri_t
H5S__hyper_shape_same(H5S_t *space1, H5S_t *space2)
{
    unsigned space1_rank;      /* Number of dimensions of first dataspace */
    unsigned space2_rank;      /* Number of dimensions of second dataspace */
    htri_t   ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space1);
    assert(space2);

    /* Get dataspace ranks */
    space1_rank = space1->extent.rank;
    space2_rank = space2->extent.rank;

    /* Sanity check */
    assert(space1_rank >= space2_rank);
    assert(space2_rank > 0);

    /* Rebuild diminfo if it is invalid and has not been confirmed to be
     * impossible */
    if (space1->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_NO)
        H5S__hyper_rebuild(space1);
    if (space2->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_NO)
        H5S__hyper_rebuild(space2);

    /* If both are regular hyperslabs, compare their diminfo values */
    if (space1->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES &&
        space2->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        int space1_dim; /* Current dimension in first dataspace */
        int space2_dim; /* Current dimension in second dataspace */

        /* Initialize dimensions */
        space1_dim = (int)space1_rank - 1;
        space2_dim = (int)space2_rank - 1;

        /* Check that the shapes are the same in the common dimensions, and that
         * block == 1 in all dimensions that appear only in space1.
         */
        while (space2_dim >= 0) {
            if (space1->select.sel_info.hslab->diminfo.opt[space1_dim].stride !=
                space2->select.sel_info.hslab->diminfo.opt[space2_dim].stride)
                HGOTO_DONE(false);

            if (space1->select.sel_info.hslab->diminfo.opt[space1_dim].count !=
                space2->select.sel_info.hslab->diminfo.opt[space2_dim].count)
                HGOTO_DONE(false);

            if (space1->select.sel_info.hslab->diminfo.opt[space1_dim].block !=
                space2->select.sel_info.hslab->diminfo.opt[space2_dim].block)
                HGOTO_DONE(false);

            space1_dim--;
            space2_dim--;
        } /* end while */

        while (space1_dim >= 0) {
            if (space1->select.sel_info.hslab->diminfo.opt[space1_dim].block != 1)
                HGOTO_DONE(false);

            space1_dim--;
        } /* end while */
    }     /* end if */
    /* If both aren't regular, use fast irregular comparison */
    else {
        H5S_hyper_span_info_t *spans1; /* Hyperslab spans for first dataspace */

        /* Make certain that both selections have span trees */
        if (NULL == space1->select.sel_info.hslab->span_lst)
            if (H5S__hyper_generate_spans(space1) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNINITIALIZED, FAIL,
                            "can't construct span tree for hyperslab selection");
        if (NULL == space2->select.sel_info.hslab->span_lst)
            if (H5S__hyper_generate_spans(space2) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNINITIALIZED, FAIL,
                            "can't construct span tree for hyperslab selection");

        /* If rank of space A is different (guaranteed greater) than
         *      rank of space B, walk down the span tree, verifying
         *      that the block size is 1 on the way down.
         */
        if (space1_rank > space2_rank) {
            unsigned diff_rank = space1_rank - space2_rank; /* Difference in ranks */

            /* Walk down the dimensions */
            spans1 = space1->select.sel_info.hslab->span_lst;
            while (diff_rank > 0) {
                H5S_hyper_span_t *span; /* Span for this dimension */

                /* Get pointer to first span in tree */
                span = spans1->head;

                /* Check for more spans in this dimension */
                if (span->next)
                    HGOTO_DONE(false);

                /* Check for span size > 1 element */
                if (span->low != span->high)
                    HGOTO_DONE(false);

                /* Walk down to the next dimension */
                spans1 = span->down;
                diff_rank--;
            } /* end while */

            /* Sanity check */
            assert(spans1);
        } /* end if */
        else
            spans1 = space1->select.sel_info.hslab->span_lst;

        /* Compare the span trees */
        ret_value = H5S__hyper_spans_shape_same(spans1, space2->select.sel_info.hslab->span_lst, space2_rank);
    } /* end else */

    /* Fall through with 'true' value, if not set earlier */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_shape_same() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_release
 PURPOSE
    Release hyperslab selection information for a dataspace
 USAGE
    herr_t H5S__hyper_release(space)
        H5S_t *space;       IN: Pointer to dataspace
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Releases all hyperslab selection information for a dataspace
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_release(H5S_t *space)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space && H5S_SEL_HYPERSLABS == H5S_GET_SELECT_TYPE(space));

    /* Reset the number of points selected */
    space->select.num_elem = 0;

    /* Release irregular hyperslab information */
    if (space->select.sel_info.hslab) {
        if (space->select.sel_info.hslab->span_lst != NULL)
            if (H5S__hyper_free_span_info(space->select.sel_info.hslab->span_lst) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "unable to free span info");

        /* Release space for the hyperslab selection information */
        space->select.sel_info.hslab = H5FL_FREE(H5S_hyper_sel_t, space->select.sel_info.hslab);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_release() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_coord_to_span
 PURPOSE
    Create a span tree for a single element
 USAGE
    H5S_hyper_span_t *H5S__hyper_coord_to_span(rank, coords)
        unsigned rank;                  IN: Number of dimensions of coordinate
        hsize_t *coords;               IN: Location of element
 RETURNS
    Non-NULL pointer to new span tree on success, NULL on failure
 DESCRIPTION
    Create a span tree for a single element
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5S_hyper_span_t *
H5S__hyper_coord_to_span(unsigned rank, const hsize_t *coords)
{
    H5S_hyper_span_t      *new_span;         /* Pointer to new span tree for coordinate */
    H5S_hyper_span_info_t *down      = NULL; /* Pointer to new span tree for next level down */
    H5S_hyper_span_t      *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(rank > 0);
    assert(coords);

    /* Search for location to insert new element in tree */
    if (rank > 1) {
        /* Allocate a span info node for coordinates below this one */
        if (NULL == (down = H5S__hyper_new_span_info(rank - 1)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

        /* Set the low & high bounds for this span info node */
        H5MM_memcpy(down->low_bounds, &coords[1], (rank - 1) * sizeof(hsize_t));
        H5MM_memcpy(down->high_bounds, &coords[1], (rank - 1) * sizeof(hsize_t));

        /* Build span tree for coordinates below this one */
        if (NULL == (down->head = H5S__hyper_coord_to_span(rank - 1, &coords[1])))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

        /* Update the tail pointer of the down dimension, and it's a single span element */
        down->tail = down->head;
    } /* end if */

    /* Build span for this coordinate */
    if (NULL == (new_span = H5S__hyper_new_span(coords[0], coords[0], down, NULL)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

    /* Set return value */
    ret_value = new_span;

done:
    if (ret_value == NULL && down != NULL)
        if (H5S__hyper_free_span_info(down) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, NULL, "unable to free span info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_coord_to_span() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_add_span_element_helper
 PURPOSE
    Helper routine to add a single element to a span tree
 USAGE
    herr_t H5S__hyper_add_span_element_helper(span_tree, rank, coords, first_dim_modified)
        H5S_hyper_span_info_t *span_tree;  IN/OUT: Pointer to span tree to append to
        unsigned rank;                  IN: Number of dimensions of coordinates
        hsize_t *coords;                IN: Location of element to add to span tree
        int *first_dim_modified;        IN: Index of the first dimension modified
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Add a single element to an existing span tree.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Assumes that the element is not already covered by the span tree
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_add_span_element_helper(H5S_hyper_span_info_t *span_tree, unsigned rank, const hsize_t *coords,
                                   int *first_dim_modified)
{
    H5S_hyper_span_t *tail_span;           /* Pointer to the tail span of one dimension */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(span_tree);
    assert(rank > 0);
    assert(coords);
    assert(first_dim_modified);

    /* Get pointer to last span in span tree */
    tail_span = span_tree->tail;

    /* Determine if tail span includes a portion of the coordinate */
    /* (Should never happen with the lowest level in the span tree) */
    if (coords[0] >= tail_span->low && coords[0] <= tail_span->high) {
        H5S_hyper_span_t *prev_down_tail_span;      /* Pointer to previous down spans' tail pointer */
        hsize_t           prev_down_tail_span_high; /* Value of previous down spans' tail's high value */

        /* Retain into about down spans' tail */
        prev_down_tail_span      = tail_span->down->tail;
        prev_down_tail_span_high = tail_span->down->tail->high;

        /* Drop down a dimension */
        assert(rank > 1);
        if (H5S__hyper_add_span_element_helper(tail_span->down, rank - 1, &coords[1], first_dim_modified) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "can't insert coordinate into span tree");

        /* Check & update high bounds for lower dimensions */
        if (*first_dim_modified >= 0) {
            unsigned first_dim;             /* First dimension modified, relative to this span tree */
            bool     first_dim_set = false; /* Whether first dimension modified is set */
            unsigned u;                     /* Local index variable */

            /* Adjust first dimension modified to be relative to this span tree */
            first_dim = (unsigned)(*first_dim_modified + 1);

            /* Reset modified dimension, in case no bounds in this span tree change */
            *first_dim_modified = -1;

            /* Iterate through coordinates */
            for (u = first_dim; u < rank; u++) {
                /* Check if coordinate is outside the bounds for this span tree */
                if (coords[u] > span_tree->high_bounds[u]) {
                    /* Update high bounds for this tree */
                    span_tree->high_bounds[u] = coords[u];

                    /* Need to signal to higher dimensions if high bounds changed */
                    if (!first_dim_set) {
                        *first_dim_modified = (int)u;
                        first_dim_set       = true;
                    } /* end if */
                }     /* end if */
            }         /* end for */
        }             /* end if */

        /* Check if previous tail span in down spans is different than current
         * tail span, or if its high value changed, in which case we should
         * check if the updated node can share down spans with other nodes.
         */
        if (tail_span->down->tail != prev_down_tail_span ||
            prev_down_tail_span_high != tail_span->down->tail->high) {
            H5S_hyper_span_t *stop_span; /* Pointer to span to stop at */
            H5S_hyper_span_t *tmp_span;  /* Temporary pointer to a span */
            uint64_t          op_gen;    /* Operation generation value */

            /* Determine which span to stop at */
            if (tail_span->down->tail != prev_down_tail_span) {
                /* Sanity check */
                assert(prev_down_tail_span->next == tail_span->down->tail);

                /* Set the span to stop at */
                stop_span = prev_down_tail_span;
            } /* end if */
            else {
                /* Sanity check */
                assert(prev_down_tail_span_high != tail_span->down->tail->high);

                /* Set the span to stop at */
                stop_span = tail_span->down->tail;
            } /* end else */

            /* Acquire an operation generation value for this operation */
            op_gen = H5S__hyper_get_op_gen();

            /* Check if the 'stop' span in the "down tree" is equal to any other
             * spans in the list of spans in the span tree.
             *
             * If so, release last span information and make last span merge into
             * previous span (if possible), or at least share their "down tree"
             * information.
             */
            tmp_span = tail_span->down->head;
            while (tmp_span != stop_span) {
                bool attempt_merge_spans = false; /* Whether to merge spans */

                /* Different tests for when to run the 'merge' algorithm,
                 * depending whether there's "down trees" or not.
                 */
                if (NULL == tmp_span->down) {
                    /* Spin through spans until we find the one before the 'stop' span */
                    if (tmp_span->next == stop_span)
                        attempt_merge_spans = true;
                } /* end if */
                else {
                    /* Check if we've compared the 'stop' span's "down tree" to
                     *      this span's "down tree" already.
                     */
                    if (tmp_span->down->op_info[0].op_gen != op_gen) {
                        if (H5S__hyper_cmp_spans(tmp_span->down, stop_span->down))
                            attempt_merge_spans = true;

                        /* Remember that we visited this span's "down tree" already */
                        /* (Because it wasn't the same as the 'stop' span's down tree
                         *      and we don't need to compare it again)
                         */
                        tmp_span->down->op_info[0].op_gen = op_gen;
                    } /* end if */
                }     /* end else */

                /* Check for merging into previous span */
                if (attempt_merge_spans) {
                    if (tmp_span->high + 1 == stop_span->low) {
                        /* Increase size of previous span */
                        tmp_span->high++;

                        /* Update pointers appropriately */
                        if (stop_span == prev_down_tail_span) {
                            /* Sanity check */
                            assert(stop_span->next == tail_span->down->tail);

                            tmp_span->next = stop_span->next;
                        } /* end if */
                        else {
                            /* Sanity check */
                            assert(tmp_span->next == tail_span->down->tail);

                            tmp_span->next        = NULL;
                            tail_span->down->tail = tmp_span;
                        } /* end else */

                        /* Release last span created */
                        if (H5S__hyper_free_span(stop_span) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span");
                    }
                    /* Span is disjoint, but has the same "down tree" selection */
                    /* (If it has a "down tree") */
                    else if (stop_span->down) {
                        /* Release "down tree" information */
                        if (H5S__hyper_free_span_info(stop_span->down) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");

                        /* Point at earlier span's "down tree" */
                        stop_span->down = tmp_span->down;

                        /* Increment reference count on shared "down tree" */
                        stop_span->down->count++;
                    } /* end else */

                    /* Found span to merge into, break out now */
                    break;
                } /* end if */

                /* Advance to next span to check */
                tmp_span = tmp_span->next;
            } /* end while */
        }     /* end if */
    }         /* end if */
    else {
        unsigned u; /* Local index variable */

        /* Check if we made it all the way to the bottom span list in the tree
         *      and the new coordinate adjoins the current tail span.
         */
        if (rank == 1 && (tail_span->high + 1) == coords[0])
            /* Append element to current tail span */
            tail_span->high++;
        else {
            H5S_hyper_span_t *new_span; /* New span created for element */

            /* Make span tree for current coordinate(s) */
            if (NULL == (new_span = H5S__hyper_coord_to_span(rank, coords)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL,
                            "can't allocate hyperslab spans for coordinate");

            /* Add new span to span tree list */
            tail_span->next = new_span;
            span_tree->tail = new_span;
        } /* end else */

        /* Update high bound for current span tree */
        assert(coords[0] > span_tree->high_bounds[0]);
        span_tree->high_bounds[0] = coords[0];

        /* Update high bounds for dimensions below this one */
        for (u = 1; u < rank; u++)
            if (coords[u] > span_tree->high_bounds[u])
                span_tree->high_bounds[u] = coords[u];

        /* Need to signal to higher dimensions that high bounds changed */
        *first_dim_modified = 0;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_add_span_element_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S_hyper_add_span_element
 PURPOSE
    Add a single element to a span tree
 USAGE
    herr_t H5S_hyper_add_span_element(space, span_tree, rank, coords)
        H5S_t *space;           IN/OUT: Pointer to dataspace to add coordinate to
        unsigned rank;          IN: Number of dimensions of coordinates
        hsize_t *coords;       IN: Location of element to add to span tree
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Add a single element to an existing span tree.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Assumes that the element is not already in the dataspace's selection

    NOTE: There's also an assumption about the context of this function call -
        This function is only called is only being called from H5D_chunk_mem_cb
        in src/H5Dchunk.c, when the library is iterating over a memory
        selection, so the coordinates passed to H5S_hyper_add_span_element will
        always be in increasing order (according to a row-major (i.e. C, not
        FORTRAN) scan) over the dataset. Therefore, for every input of
        coordinates, only the last span element (i.e., the tail pointer) in
        one dimension is checked against the input.

    NOTE: This algorithm is definitely "correct" and tries to conserve memory
        as much as possible, but it's doing a _lot_ of work that might be
        better spent running a similar algorithm to "condense" the span tree
        (possibly even back into a regular selection) just before the selection
        is used for I/O on the chunk.  I'm not going to spend the time on this
        currently, but it does sound like a good direction to explore.
        QAK, 2019/01/24

 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_hyper_add_span_element(H5S_t *space, unsigned rank, const hsize_t *coords)
{
    H5S_hyper_span_info_t *head      = NULL;    /* Pointer to new head of span tree */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(space);
    assert(rank > 0);
    assert(coords);
    assert(space->extent.rank == rank);

    /* Check if this is the first element in the selection */
    if (NULL == space->select.sel_info.hslab) {
        /* Allocate a span info node */
        if (NULL == (head = H5S__hyper_new_span_info(rank)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab span info");

        /* Set the low & high bounds for this span info node */
        H5MM_memcpy(head->low_bounds, coords, rank * sizeof(hsize_t));
        H5MM_memcpy(head->high_bounds, coords, rank * sizeof(hsize_t));

        /* Set the reference count */
        head->count = 1;

        /* Build span tree for this coordinate */
        if (NULL == (head->head = H5S__hyper_coord_to_span(rank, coords)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab spans for coordinate");

        /* Update the tail pointer of this newly created span in dimension "rank" */
        head->tail = head->head;

        /* Allocate selection info */
        if (NULL == (space->select.sel_info.hslab = H5FL_MALLOC(H5S_hyper_sel_t)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab selection");

        /* Set the selection to the new span tree */
        space->select.sel_info.hslab->span_lst = head;

        /* Set selection type */
        space->select.type = H5S_sel_hyper;

        /* Reset "regular" hyperslab flag */
        space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;

        /* Set unlim_dim */
        space->select.sel_info.hslab->unlim_dim = -1;

        /* Set # of elements in selection */
        space->select.num_elem = 1;
    } /* end if */
    else {
        int first_dim_modified = -1; /* Index of first dimension modified */

        /* Add the element to the current set of spans */
        if (H5S__hyper_add_span_element_helper(space->select.sel_info.hslab->span_lst, rank, coords,
                                               &first_dim_modified) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't insert coordinate into span tree");

        /* Increment # of elements in selection */
        space->select.num_elem++;
    } /* end else */

done:
    if (ret_value < 0)
        if (head)
            if (H5S__hyper_free_span_info(head) < 0)
                HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_hyper_add_span_element() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_intersect_block_helper
 PURPOSE
    Helper routine to detect intersections in span trees
 USAGE
    bool H5S__hyper_intersect_block_helper(spans, rank, start, end, op_info_i, op_gen)
        H5S_hyper_span_info_t *spans;     IN: First span tree to operate with
        unsigned rank;     IN: Number of dimensions for span tree
        hsize_t *start;    IN: Starting coordinate for block
        hsize_t *end;      IN: Ending coordinate for block
        unsigned op_info_i;             IN: Index of op info to use
        uint64_t op_gen;   IN: Operation generation
 RETURN
    Non-negative (true/false) on success, can't fail
 DESCRIPTION
    Quickly detect intersections between span tree and block
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static bool
H5S__hyper_intersect_block_helper(H5S_hyper_span_info_t *spans, unsigned rank, const hsize_t *start,
                                  const hsize_t *end, unsigned op_info_i, uint64_t op_gen)
{
    bool ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(spans);
    assert(start);
    assert(end);

    /* Check if we've already visited this span tree */
    if (spans->op_info[op_info_i].op_gen != op_gen) {
        H5S_hyper_span_t *curr; /* Pointer to current span in 1st span tree */
        unsigned          u;    /* Local index variable */

        /* Verify that there is a possibility of an overlap by checking the block
         *  against the low & high bounds for the span tree.
         */
        for (u = 0; u < rank; u++)
            if (start[u] > spans->high_bounds[u] || end[u] < spans->low_bounds[u])
                HGOTO_DONE(false);

        /* Get the span list for spans in this tree */
        curr = spans->head;

        /* Iterate over the spans in the tree */
        while (curr != NULL) {
            /* Check for span entirely before block */
            if (curr->high < *start)
                /* Advance to next span in this dimension */
                curr = curr->next;
            /* If this span is past the end of the block, then we're done in this dimension */
            else if (curr->low > *end)
                HGOTO_DONE(false);
            /* block & span overlap */
            else {
                /* If this is the bottom dimension, then the span tree overlaps the block */
                if (curr->down == NULL)
                    HGOTO_DONE(true);
                /* Recursively check spans in next dimension down */
                else {
                    /* If there is an intersection in the "down" dimensions,
                     * the span trees overlap.
                     */
                    if (H5S__hyper_intersect_block_helper(curr->down, rank - 1, start + 1, end + 1, op_info_i,
                                                          op_gen))
                        HGOTO_DONE(true);

                    /* No intersection in down dimensions, advance to next span */
                    curr = curr->next;
                } /* end else */
            }     /* end else */
        }         /* end while */

        /* Set the tree's operation generation */
        spans->op_info[op_info_i].op_gen = op_gen;
    } /* end if */

    /* Fall through with 'false' return value */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_intersect_block_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_intersect_block
 PURPOSE
    Detect intersections of selection with block
 USAGE
    htri_t H5S__hyper_intersect_block(space, start, end)
        H5S_t *space;           IN: Dataspace with selection to use
        const hsize_t *start;   IN: Starting coordinate for block
        const hsize_t *end;     IN: Ending coordinate for block
 RETURNS
    Non-negative true / false on success, negative on failure
 DESCRIPTION
    Quickly detect intersections between both regular hyperslabs and span trees
    with a block
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Does not use selection offset.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static htri_t
H5S__hyper_intersect_block(H5S_t *space, const hsize_t *start, const hsize_t *end)
{
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(space);
    assert(H5S_SEL_HYPERSLABS == H5S_GET_SELECT_TYPE(space));
    assert(start);
    assert(end);

    /* Attempt to rebuild diminfo if it is invalid and has not been confirmed
     * to be impossible.
     */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_NO)
        H5S__hyper_rebuild(space);

    /* Check for regular hyperslab intersection */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        bool     single_block; /* Whether the regular selection is a single block */
        unsigned u;            /* Local index variable */

        /* Check for a single block */
        /* For a regular hyperslab to be single, it must have only one block
         * (i.e. count == 1 in all dimensions).
         */
        single_block = true;
        for (u = 0; u < space->extent.rank; u++)
            if (space->select.sel_info.hslab->diminfo.opt[u].count > 1)
                single_block = false;

        /* Single blocks have already been "compared" above, in the low / high
         * bound checking, so just return true if we've reached here - they
         * would have been rejected earlier, if they didn't intersect.
         */
        if (single_block)
            HGOTO_DONE(true);
        else {
            /* Loop over the dimensions, checking for an intersection */
            for (u = 0; u < space->extent.rank; u++) {
                /* If the block's start is <= the hyperslab start, they intersect */
                /* (So, if the start is > the hyperslab start, check more conditions) */
                if (start[u] > space->select.sel_info.hslab->diminfo.opt[u].start) {
                    hsize_t adj_start; /* Start coord, adjusted for hyperslab selection parameters */
                    hsize_t nstride;   /* Number of strides into the selection */

                    /* Adjust start coord for selection's 'start' offset */
                    adj_start = start[u] - space->select.sel_info.hslab->diminfo.opt[u].start;

                    /* Compute # of strides into the selection */
                    if (space->select.sel_info.hslab->diminfo.opt[u].count > 1)
                        nstride = adj_start / space->select.sel_info.hslab->diminfo.opt[u].stride;
                    else
                        nstride = 0;

                    /* Sanity check */
                    assert(nstride <= space->select.sel_info.hslab->diminfo.opt[u].count);

                    /* "Rebase" the adjusted start coord into the same range
                     *      range of values as the selections's first block.
                     */
                    adj_start -= nstride * space->select.sel_info.hslab->diminfo.opt[u].stride;

                    /* If the adjusted start doesn't fall within the first hyperslab
                     *  span, check for the block overlapping with the next one.
                     */
                    if (adj_start >= space->select.sel_info.hslab->diminfo.opt[u].block) {
                        hsize_t adj_end; /* End coord, adjusted for hyperslab selection parameters */

                        /* Adjust end coord for selection's 'start' offset */
                        adj_end = end[u] - space->select.sel_info.hslab->diminfo.opt[u].start;

                        /* "Rebase" the adjusted end coord into the same range
                         *      range of values as the selections's first block.
                         */
                        adj_end -= nstride * space->select.sel_info.hslab->diminfo.opt[u].stride;

                        /* If block doesn't extend over beginning of next span,
                         *  it doesn't intersect.
                         */
                        if (adj_end < space->select.sel_info.hslab->diminfo.opt[u].stride)
                            HGOTO_DONE(false);
                    } /* end if */
                }     /* end if */
            }         /* end for */

            /* If we've looped through all dimensions and none of them didn't
             *  overlap, then all of them do, so we report true.
             */
            HGOTO_DONE(true);
        } /* end else */
    }     /* end if */
    else {
        uint64_t op_gen; /* Operation generation value */

        /* Acquire an operation generation value for this operation */
        op_gen = H5S__hyper_get_op_gen();

        /* Perform the span-by-span intersection check */
        /* Always use op_info[0] since we own this op_info, so there can be no
         * simultaneous operations */
        ret_value = H5S__hyper_intersect_block_helper(space->select.sel_info.hslab->span_lst,
                                                      space->extent.rank, start, end, 0, op_gen);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_intersect_block() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_adjust_u_helper
 PURPOSE
    Helper routine to adjust offsets in span trees
 USAGE
    void H5S__hyper_adjust_u_helper(spans, rank, offset, op_info_i, op_gen)
        H5S_hyper_span_info_t *spans;   IN: Span tree to operate with
        unsigned rank;                  IN: Number of dimensions for span tree
        const hsize_t *offset;          IN: Offset to subtract
        unsigned op_info_i;             IN: Index of op info to use
        uint64_t op_gen;                IN: Operation generation
 RETURNS
    None
 DESCRIPTION
    Adjust the location of the spans in a span tree by subtracting an offset
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static void
H5S__hyper_adjust_u_helper(H5S_hyper_span_info_t *spans, unsigned rank, const hsize_t *offset,
                           unsigned op_info_i, uint64_t op_gen)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(spans);
    assert(offset);

    /* Check if we've already set this span tree */
    if (spans->op_info[op_info_i].op_gen != op_gen) {
        H5S_hyper_span_t *span; /* Pointer to current span in span tree */
        unsigned          u;    /* Local index variable */

        /* Adjust the span tree's low & high bounds */
        for (u = 0; u < rank; u++) {
            assert(spans->low_bounds[u] >= offset[u]);
            spans->low_bounds[u] -= offset[u];
            spans->high_bounds[u] -= offset[u];
        } /* end for */

        /* Iterate over the spans in tree */
        span = spans->head;
        while (span != NULL) {
            /* Adjust span offset */
            assert(span->low >= *offset);
            span->low -= *offset;
            span->high -= *offset;

            /* Recursively adjust spans in next dimension down */
            if (span->down != NULL)
                H5S__hyper_adjust_u_helper(span->down, rank - 1, offset + 1, op_info_i, op_gen);

            /* Advance to next span in this dimension */
            span = span->next;
        } /* end while */

        /* Set the tree's operation generation */
        spans->op_info[op_info_i].op_gen = op_gen;
    } /* end if */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5S__hyper_adjust_u_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_adjust_u
 PURPOSE
    Adjust a hyperslab selection by subtracting an offset
 USAGE
    void H5S__hyper_adjust_u(space,offset)
        H5S_t *space;           IN/OUT: Pointer to dataspace to adjust
        const hsize_t *offset; IN: Offset to subtract
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Moves a hyperslab selection by subtracting an offset from it.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_adjust_u(H5S_t *space, const hsize_t *offset)
{
    bool     non_zero_offset = false; /* Whether any offset is non-zero */
    unsigned u;                       /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(space);
    assert(offset);

    /* Check for an all-zero offset vector */
    for (u = 0; u < space->extent.rank; u++)
        if (0 != offset[u]) {
            non_zero_offset = true;
            break;
        }

    /* Only perform operation if the offset is non-zero */
    if (non_zero_offset) {
        /* Subtract the offset from the "regular" coordinates, if they exist */
        /* (No need to rebuild the dimension info yet -QAK) */
        if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
            for (u = 0; u < space->extent.rank; u++) {
                assert(space->select.sel_info.hslab->diminfo.opt[u].start >= offset[u]);
                space->select.sel_info.hslab->diminfo.opt[u].start -= offset[u];

                /* Adjust the low & high bounds */
                assert(space->select.sel_info.hslab->diminfo.low_bounds[u] >= offset[u]);
                space->select.sel_info.hslab->diminfo.low_bounds[u] -= offset[u];
                space->select.sel_info.hslab->diminfo.high_bounds[u] -= offset[u];
            } /* end for */
        }     /* end if */

        /* Subtract the offset from the span tree coordinates, if they exist */
        if (space->select.sel_info.hslab->span_lst) {
            uint64_t op_gen; /* Operation generation value */

            /* Acquire an operation generation value for this operation */
            op_gen = H5S__hyper_get_op_gen();

            /* Perform adjustment */
            /* Always use op_info[0] since we own this op_info, so there can be no
             * simultaneous operations */
            H5S__hyper_adjust_u_helper(space->select.sel_info.hslab->span_lst, space->extent.rank, offset, 0,
                                       op_gen);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_adjust_u() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_project_scalar
 *
 * Purpose:    Projects a single element hyperslab selection into a scalar
 *              dataspace
 *
 * Return:    Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_project_scalar(const H5S_t *space, hsize_t *offset)
{
    hsize_t block[H5S_MAX_RANK]; /* Block selected in base dataspace */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(space && H5S_SEL_HYPERSLABS == H5S_GET_SELECT_TYPE(space));
    assert(offset);

    /* Check for a "regular" hyperslab selection */
    /* (No need to rebuild the dimension info yet -QAK) */
    if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        const H5S_hyper_dim_t *diminfo =
            space->select.sel_info.hslab->diminfo.opt; /* Alias for dataspace's diminfo information */
        unsigned u;                                    /* Counter */

        /* Build the table of the initial offset */
        for (u = 0; u < space->extent.rank; u++) {
            /* Sanity check diminfo */
            assert(1 == diminfo[u].count);
            assert(1 == diminfo[u].block);

            /* Sanity check bounds, while we're here */
            assert(diminfo[u].start == space->select.sel_info.hslab->diminfo.low_bounds[u]);

            /* Keep the offset for later */
            block[u] = diminfo[u].start;
        } /* end for */
    }     /* end if */
    else {
        const H5S_hyper_span_t *curr;     /* Pointer to current hyperslab span */
        unsigned                curr_dim; /* Current dimension being operated on */

        /* Advance down selected spans */
        curr     = space->select.sel_info.hslab->span_lst->head;
        curr_dim = 0;
        while (1) {
            /* Sanity checks */
            assert(NULL == curr->next);
            assert(curr->low == curr->high);
            assert(curr_dim < space->extent.rank);

            /* Save the location of the selection in current dimension */
            block[curr_dim] = curr->low;

            /* Advance down to next dimension */
            if (curr->down) {
                curr = curr->down->head;
                curr_dim++;
            } /* end if */
            else
                break;
        } /* end while */
    }     /* end else */

    /* Calculate offset of selection in projected buffer */
    *offset = H5VM_array_offset(space->extent.rank, space->extent.size, block);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_project_scalar() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_project_simple_lower
 *
 * Purpose:    Projects a hyperslab selection onto/into a simple dataspace
 *              of a lower rank
 *
 * Return:    Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_project_simple_lower(const H5S_t *base_space, H5S_t *new_space)
{
    H5S_hyper_span_info_t *down;                /* Pointer to list of spans */
    unsigned               curr_dim;            /* Current dimension being operated on */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(base_space && H5S_SEL_HYPERSLABS == H5S_GET_SELECT_TYPE(base_space));
    assert(new_space);
    assert(new_space->extent.rank < base_space->extent.rank);

    /* Walk down the span tree until we reach the selection to project */
    down     = base_space->select.sel_info.hslab->span_lst;
    curr_dim = 0;
    while (down && curr_dim < (base_space->extent.rank - new_space->extent.rank)) {
        /* Sanity check */
        assert(NULL == down->head->next);

        /* Advance down to next dimension */
        down = down->head->down;
        curr_dim++;
    } /* end while */
    if (NULL == down)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "NULL span list pointer");

    /* Share the underlying hyperslab span information */
    new_space->select.sel_info.hslab->span_lst = down;
    new_space->select.sel_info.hslab->span_lst->count++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_project_simple_lower() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_project_simple_higher
 *
 * Purpose:    Projects a hyperslab selection onto/into a simple dataspace
 *              of a higher rank
 *
 * Return:    Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_project_simple_higher(const H5S_t *base_space, H5S_t *new_space)
{
    H5S_hyper_span_t *prev_span = NULL;    /* Pointer to previous list of spans */
    unsigned          delta_rank;          /* Difference in dataspace ranks */
    unsigned          curr_dim;            /* Current dimension being operated on */
    unsigned          u;                   /* Local index variable */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(base_space && H5S_SEL_HYPERSLABS == H5S_GET_SELECT_TYPE(base_space));
    assert(new_space);
    assert(new_space->extent.rank > base_space->extent.rank);

    /* Create nodes until reaching the correct # of dimensions */
    new_space->select.sel_info.hslab->span_lst = NULL;
    curr_dim                                   = 0;
    delta_rank                                 = (new_space->extent.rank - base_space->extent.rank);
    while (curr_dim < delta_rank) {
        H5S_hyper_span_info_t *new_span_info; /* Pointer to list of spans */
        H5S_hyper_span_t      *new_span;      /* Temporary hyperslab span */

        /* Allocate a new span_info node */
        if (NULL == (new_span_info = H5S__hyper_new_span_info(new_space->extent.rank))) {
            if (prev_span)
                (void)H5S__hyper_free_span(prev_span);
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab span info");
        }

        /* Check for linking into higher span */
        if (prev_span)
            prev_span->down = new_span_info;

        /* Allocate a new node */
        if (NULL == (new_span = H5S__hyper_new_span((hsize_t)0, (hsize_t)0, NULL, NULL))) {
            assert(new_span_info);
            if (!prev_span)
                (void)H5FL_ARR_FREE(hbounds_t, new_span_info);
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab span");
        } /* end if */

        /* Set the span_info information */
        new_span_info->count = 1;
        new_span_info->head  = new_span;
        new_span_info->tail  = new_span;

        /* Set the bounding box */
        for (u = 0; u < delta_rank; u++) {
            new_span_info->low_bounds[u]  = 0;
            new_span_info->high_bounds[u] = 0;
        } /* end for */
        for (; u < new_space->extent.rank; u++) {
            new_span_info->low_bounds[u] =
                base_space->select.sel_info.hslab->span_lst->low_bounds[u - delta_rank];
            new_span_info->high_bounds[u] =
                base_space->select.sel_info.hslab->span_lst->high_bounds[u - delta_rank];
        } /* end for */

        /* Attach to new space, if top span info */
        if (NULL == new_space->select.sel_info.hslab->span_lst)
            new_space->select.sel_info.hslab->span_lst = new_span_info;

        /* Remember previous span info */
        prev_span = new_span;

        /* Advance to next dimension */
        curr_dim++;
    } /* end while */
    if (NULL == new_space->select.sel_info.hslab->span_lst)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "NULL span list pointer");
    assert(prev_span);

    /* Share the underlying hyperslab span information */
    prev_span->down = base_space->select.sel_info.hslab->span_lst;
    prev_span->down->count++;

done:
    if (ret_value < 0 && new_space->select.sel_info.hslab->span_lst) {
        if (new_space->select.sel_info.hslab->span_lst->head)
            if (H5S__hyper_free_span(new_space->select.sel_info.hslab->span_lst->head) < 0)
                HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span");

        new_space->select.sel_info.hslab->span_lst =
            (H5S_hyper_span_info_t *)H5FL_ARR_FREE(hbounds_t, new_space->select.sel_info.hslab->span_lst);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_project_simple_higher() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_project_simple
 *
 * Purpose:    Projects a hyperslab selection onto/into a simple dataspace
 *              of a different rank
 *
 * Return:    Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_project_simple(const H5S_t *base_space, H5S_t *new_space, hsize_t *offset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(base_space && H5S_SEL_HYPERSLABS == H5S_GET_SELECT_TYPE(base_space));
    assert(new_space);
    assert(offset);

    /* We are setting a new selection, remove any current selection in new dataspace */
    if (H5S_SELECT_RELEASE(new_space) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't release selection");

    /* Allocate space for the hyperslab selection information */
    if (NULL == (new_space->select.sel_info.hslab = H5FL_MALLOC(H5S_hyper_sel_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab info");

    /* Set unlim_dim */
    new_space->select.sel_info.hslab->unlim_dim = -1;

    /* Check for a "regular" hyperslab selection */
    /* (No need to rebuild the dimension info yet -QAK) */
    if (base_space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
        unsigned base_space_dim; /* Current dimension in the base dataspace */
        unsigned new_space_dim;  /* Current dimension in the new dataspace */
        unsigned u;              /* Local index variable */

        /* Check if the new space's rank is < or > base space's rank */
        if (new_space->extent.rank < base_space->extent.rank) {
            const H5S_hyper_dim_t *opt_diminfo = base_space->select.sel_info.hslab->diminfo
                                                     .opt; /* Alias for dataspace's diminfo information */
            hsize_t block[H5S_MAX_RANK];                   /* Block selected in base dataspace */

            /* Compute the offset for the down-projection */
            memset(block, 0, sizeof(block));
            for (u = 0; u < (base_space->extent.rank - new_space->extent.rank); u++)
                block[u] = opt_diminfo[u].start;
            *offset = H5VM_array_offset(base_space->extent.rank, base_space->extent.size, block);

            /* Set the correct dimensions for the base & new spaces */
            base_space_dim = base_space->extent.rank - new_space->extent.rank;
            new_space_dim  = 0;
        } /* end if */
        else {
            assert(new_space->extent.rank > base_space->extent.rank);

            /* The offset is zero when projected into higher dimensions */
            *offset = 0;

            /* Set the diminfo information for the higher dimensions */
            for (new_space_dim = 0; new_space_dim < (new_space->extent.rank - base_space->extent.rank);
                 new_space_dim++) {
                new_space->select.sel_info.hslab->diminfo.app[new_space_dim].start  = 0;
                new_space->select.sel_info.hslab->diminfo.app[new_space_dim].stride = 1;
                new_space->select.sel_info.hslab->diminfo.app[new_space_dim].count  = 1;
                new_space->select.sel_info.hslab->diminfo.app[new_space_dim].block  = 1;

                new_space->select.sel_info.hslab->diminfo.opt[new_space_dim].start  = 0;
                new_space->select.sel_info.hslab->diminfo.opt[new_space_dim].stride = 1;
                new_space->select.sel_info.hslab->diminfo.opt[new_space_dim].count  = 1;
                new_space->select.sel_info.hslab->diminfo.opt[new_space_dim].block  = 1;
            } /* end for */

            /* Start at beginning of base space's dimension info */
            base_space_dim = 0;
        } /* end else */

        /* Copy the diminfo */
        while (base_space_dim < base_space->extent.rank) {
            new_space->select.sel_info.hslab->diminfo.app[new_space_dim].start =
                base_space->select.sel_info.hslab->diminfo.app[base_space_dim].start;
            new_space->select.sel_info.hslab->diminfo.app[new_space_dim].stride =
                base_space->select.sel_info.hslab->diminfo.app[base_space_dim].stride;
            new_space->select.sel_info.hslab->diminfo.app[new_space_dim].count =
                base_space->select.sel_info.hslab->diminfo.app[base_space_dim].count;
            new_space->select.sel_info.hslab->diminfo.app[new_space_dim].block =
                base_space->select.sel_info.hslab->diminfo.app[base_space_dim].block;

            new_space->select.sel_info.hslab->diminfo.opt[new_space_dim].start =
                base_space->select.sel_info.hslab->diminfo.opt[base_space_dim].start;
            new_space->select.sel_info.hslab->diminfo.opt[new_space_dim].stride =
                base_space->select.sel_info.hslab->diminfo.opt[base_space_dim].stride;
            new_space->select.sel_info.hslab->diminfo.opt[new_space_dim].count =
                base_space->select.sel_info.hslab->diminfo.opt[base_space_dim].count;
            new_space->select.sel_info.hslab->diminfo.opt[new_space_dim].block =
                base_space->select.sel_info.hslab->diminfo.opt[base_space_dim].block;

            /* Advance to next dimensions */
            base_space_dim++;
            new_space_dim++;
        } /* end for */

        /* Update the bounding box */
        for (u = 0; u < new_space->extent.rank; u++) {
            new_space->select.sel_info.hslab->diminfo.low_bounds[u] =
                new_space->select.sel_info.hslab->diminfo.opt[u].start;
            new_space->select.sel_info.hslab->diminfo.high_bounds[u] =
                new_space->select.sel_info.hslab->diminfo.low_bounds[u] +
                new_space->select.sel_info.hslab->diminfo.opt[u].stride *
                    (new_space->select.sel_info.hslab->diminfo.opt[u].count - 1) +
                (new_space->select.sel_info.hslab->diminfo.opt[u].block - 1);
        } /* end for */

        /* Indicate that the dimension information is valid */
        new_space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_YES;

        /* Indicate that there's no slab information */
        new_space->select.sel_info.hslab->span_lst = NULL;
    } /* end if */
    else {
        /* Check if the new space's rank is < or > base space's rank */
        if (new_space->extent.rank < base_space->extent.rank) {
            const H5S_hyper_span_t *curr;                /* Pointer to current hyperslab span */
            hsize_t                 block[H5S_MAX_RANK]; /* Block selected in base dataspace */
            unsigned                curr_dim;            /* Current dimension being operated on */

            /* Clear the block buffer */
            memset(block, 0, sizeof(block));

            /* Advance down selected spans */
            curr     = base_space->select.sel_info.hslab->span_lst->head;
            curr_dim = 0;
            while (curr && curr_dim < (base_space->extent.rank - new_space->extent.rank)) {
                /* Save the location of the selection in current dimension */
                block[curr_dim] = curr->low;

                /* Advance down to next dimension */
                curr = curr->down->head;
                curr_dim++;
            } /* end while */

            /* Compute the offset for the down-projection */
            *offset = H5VM_array_offset(base_space->extent.rank, base_space->extent.size, block);

            /* Project the base space's selection down in less dimensions */
            if (H5S__hyper_project_simple_lower(base_space, new_space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                            "can't project hyperslab selection into less dimensions");
        } /* end if */
        else {
            assert(new_space->extent.rank > base_space->extent.rank);

            /* The offset is zero when projected into higher dimensions */
            *offset = 0;

            /* Project the base space's selection down in more dimensions */
            if (H5S__hyper_project_simple_higher(base_space, new_space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                            "can't project hyperslab selection into less dimensions");
        } /* end else */

        /* Copy the status of the dimension information */
        new_space->select.sel_info.hslab->diminfo_valid = base_space->select.sel_info.hslab->diminfo_valid;
    } /* end else */

    /* Number of elements selected will be the same */
    new_space->select.num_elem = base_space->select.num_elem;

    /* Set selection type */
    new_space->select.type = H5S_sel_hyper;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_project_simple() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_adjust_s_helper
 PURPOSE
    Helper routine to adjust offsets in span trees
 USAGE
    void H5S__hyper_adjust_s_helper(spans, rank, offset, op_info_i, op_gen)
        H5S_hyper_span_info_t *spans;   IN: Span tree to operate with
        unsigned rank;                  IN: Number of dimensions for span tree
        const hssize_t *offset;         IN: Offset to subtract
        unsigned op_info_i;             IN: Index of op info to use
        uint64_t op_gen;                IN: Operation generation
 RETURNS
    None
 DESCRIPTION
    Adjust the location of the spans in a span tree by subtracting an offset
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static void
H5S__hyper_adjust_s_helper(H5S_hyper_span_info_t *spans, unsigned rank, const hssize_t *offset,
                           unsigned op_info_i, uint64_t op_gen)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(spans);
    assert(offset);

    /* Check if we've already set this span tree */
    if (spans->op_info[op_info_i].op_gen != op_gen) {
        H5S_hyper_span_t *span; /* Pointer to current span in span tree */
        unsigned          u;    /* Local index variable */

        /* Adjust the span tree's low & high bounds */
        for (u = 0; u < rank; u++) {
            assert((hssize_t)spans->low_bounds[u] >= offset[u]);
            spans->low_bounds[u]  = (hsize_t)((hssize_t)spans->low_bounds[u] - offset[u]);
            spans->high_bounds[u] = (hsize_t)((hssize_t)spans->high_bounds[u] - offset[u]);
        } /* end for */

        /* Iterate over the spans in tree */
        span = spans->head;
        while (span != NULL) {
            /* Adjust span offset */
            assert((hssize_t)span->low >= *offset);
            span->low  = (hsize_t)((hssize_t)span->low - *offset);
            span->high = (hsize_t)((hssize_t)span->high - *offset);

            /* Recursively adjust spans in next dimension down */
            if (span->down != NULL)
                H5S__hyper_adjust_s_helper(span->down, rank - 1, offset + 1, op_info_i, op_gen);

            /* Advance to next span in this dimension */
            span = span->next;
        } /* end while */

        /* Set the tree's operation generation */
        spans->op_info[op_info_i].op_gen = op_gen;
    } /* end if */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5S__hyper_adjust_s_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_adjust_s
 PURPOSE
    Adjust a hyperslab selection by subtracting an offset
 USAGE
    herr_t H5S__hyper_adjust_s(space,offset)
        H5S_t *space;           IN/OUT: Pointer to dataspace to adjust
        const hssize_t *offset; IN: Offset to subtract
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Moves a hyperslab selection by subtracting an offset from it.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_adjust_s(H5S_t *space, const hssize_t *offset)
{
    bool     non_zero_offset = false; /* Whether any offset is non-zero */
    unsigned u;                       /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(space);
    assert(offset);

    /* Check for an all-zero offset vector */
    for (u = 0; u < space->extent.rank; u++)
        if (0 != offset[u]) {
            non_zero_offset = true;
            break;
        } /* end if */

    /* Only perform operation if the offset is non-zero */
    if (non_zero_offset) {
        /* Subtract the offset from the "regular" coordinates, if they exist */
        /* (No need to rebuild the dimension info yet -QAK) */
        if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
            for (u = 0; u < space->extent.rank; u++) {
                assert((hssize_t)space->select.sel_info.hslab->diminfo.opt[u].start >= offset[u]);
                space->select.sel_info.hslab->diminfo.opt[u].start =
                    (hsize_t)((hssize_t)space->select.sel_info.hslab->diminfo.opt[u].start - offset[u]);

                /* Adjust the low & high bounds */
                assert((hssize_t)space->select.sel_info.hslab->diminfo.low_bounds[u] >= offset[u]);
                space->select.sel_info.hslab->diminfo.low_bounds[u] =
                    (hsize_t)((hssize_t)space->select.sel_info.hslab->diminfo.low_bounds[u] - offset[u]);
                space->select.sel_info.hslab->diminfo.high_bounds[u] =
                    (hsize_t)((hssize_t)space->select.sel_info.hslab->diminfo.high_bounds[u] - offset[u]);
            } /* end for */
        }     /* end if */

        /* Subtract the offset from the span tree coordinates, if they exist */
        if (space->select.sel_info.hslab->span_lst) {
            uint64_t op_gen; /* Operation generation value */

            /* Acquire an operation generation value for this operation */
            op_gen = H5S__hyper_get_op_gen();

            /* Perform the adjustment */
            /* Always use op_info[0] since we own this op_info, so there can be no
             * simultaneous operations */
            H5S__hyper_adjust_s_helper(space->select.sel_info.hslab->span_lst, space->extent.rank, offset, 0,
                                       op_gen);
        } /* end if */
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__hyper_adjust_s() */

/*--------------------------------------------------------------------------
 NAME
    H5S_hyper_normalize_offset
 PURPOSE
    "Normalize" a hyperslab selection by adjusting it's coordinates by the
    amount of the selection offset.
 USAGE
    htri_t H5S_hyper_normalize_offset(space, old_offset)
        H5S_t *space;           IN/OUT: Pointer to dataspace to move
        hssize_t *old_offset;   OUT: Pointer to space to store old offset
 RETURNS
    true/false for hyperslab selection, FAIL on error
 DESCRIPTION
    Copies the current selection offset into the array provided, then
    inverts the selection offset, subtracts the offset from the hyperslab
    selection and resets the offset to zero.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5S_hyper_normalize_offset(H5S_t *space, hssize_t *old_offset)
{
    htri_t ret_value = false; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(space);
    assert(old_offset);

    /* Check for hyperslab selection & offset changed */
    if (H5S_GET_SELECT_TYPE(space) == H5S_SEL_HYPERSLABS && space->select.offset_changed) {
        unsigned u; /* Local index variable */

        /* Copy & invert the selection offset */
        for (u = 0; u < space->extent.rank; u++) {
            old_offset[u]           = space->select.offset[u];
            space->select.offset[u] = -space->select.offset[u];
        } /* end for */

        /* Call the 'adjust' routine */
        if (H5S__hyper_adjust_s(space, space->select.offset) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't adjust selection");

        /* Zero out the selection offset */
        memset(space->select.offset, 0, sizeof(hssize_t) * space->extent.rank);

        /* Indicate that the offset was normalized */
        ret_value = true;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_hyper_normalize_offset() */

/*--------------------------------------------------------------------------
 NAME
    H5S_hyper_denormalize_offset
 PURPOSE
    "Denormalize" a hyperslab selection by reverse adjusting it's coordinates
    by the amount of the former selection offset.
 USAGE
    herr_t H5S_hyper_denormalize_offset(space, old_offset)
        H5S_t *space;           IN/OUT: Pointer to dataspace to move
        hssize_t *old_offset;   IN: Pointer to old offset array
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Subtracts the old offset from the current selection (canceling out the
    effect of the "normalize" routine), then restores the old offset into
    the dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_hyper_denormalize_offset(H5S_t *space, const hssize_t *old_offset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(space);
    assert(H5S_GET_SELECT_TYPE(space) == H5S_SEL_HYPERSLABS);

    /* Call the 'adjust' routine */
    if (H5S__hyper_adjust_s(space, old_offset) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't adjust selection");

    /* Copy the selection offset over */
    H5MM_memcpy(space->select.offset, old_offset, sizeof(hssize_t) * space->extent.rank);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_hyper_denormalize_offset() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_append_span
 PURPOSE
    Create a new span and append to span list
 USAGE
    herr_t H5S__hyper_append_span(span_tree, ndims, low, high, down)
        H5S_hyper_span_info_t **span_tree;  IN/OUT: Pointer to span tree to append to
        unsigned ndims;                  IN: Number of dimension for span
        hsize_t low, high;               IN: Low and high bounds for new span node
        H5S_hyper_span_info_t *down;     IN: Down span tree for new node
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Create a new span node and append to a span list.  Update the previous
    span in the list also.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_append_span(H5S_hyper_span_info_t **span_tree, unsigned ndims, hsize_t low, hsize_t high,
                       H5S_hyper_span_info_t *down)
{
    H5S_hyper_span_t *new_span  = NULL;
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(span_tree);

    /* Check for adding first node to merged spans */
    if (*span_tree == NULL) {
        /* Allocate new span node to append to list */
        if (NULL == (new_span = H5S__hyper_new_span(low, high, down, NULL)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab span");

        /* Make new span the first node in span list */

        /* Allocate a new span_info node */
        if (NULL == (*span_tree = H5S__hyper_new_span_info(ndims)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab span");

        /* Set the span tree's basic information */
        (*span_tree)->count = 1;
        (*span_tree)->head  = new_span;
        (*span_tree)->tail  = new_span;

        /* Set low & high bounds for new span tree */
        (*span_tree)->low_bounds[0]  = low;
        (*span_tree)->high_bounds[0] = high;
        if (down) {
            /* Sanity check */
            assert(ndims > 1);

            H5MM_memcpy(&((*span_tree)->low_bounds[1]), down->low_bounds, sizeof(hsize_t) * (ndims - 1));
            H5MM_memcpy(&((*span_tree)->high_bounds[1]), down->high_bounds, sizeof(hsize_t) * (ndims - 1));
        } /* end if */
    }     /* end if */
    /* Merge or append to existing merged spans list */
    else {
        htri_t down_cmp = (-1); /* Comparison value for down spans */

        /* Check if span can just extend the previous merged span */
        if ((((*span_tree)->tail->high + 1) == low) &&
            (down_cmp = H5S__hyper_cmp_spans(down, (*span_tree)->tail->down))) {
            /* Extend previous merged span to include new high bound */
            (*span_tree)->tail->high = high;

            /* Extend span tree's high bound in this dimension */
            /* (No need to update lower dimensions, since this span shares them with previous span) */
            (*span_tree)->high_bounds[0] = high;
        } /* end if */
        else {
            H5S_hyper_span_info_t *new_down; /* Down pointer for new span node */

            /* Sanity check */
            /* (If down_cmp was set to true above, we won't be in this branch) */
            assert(down_cmp != true);

            /* Check if there is actually a down span */
            if (down) {
                /* Check if the down spans for the new span node are the same as the previous span node */
                /* (Uses the 'down span comparison' from earlier, if already computed) */
                if (down_cmp < 0 && (down_cmp = H5S__hyper_cmp_spans(down, (*span_tree)->tail->down)))
                    /* Share the previous span's down span tree */
                    new_down = (*span_tree)->tail->down;
                else
                    new_down = down;
            } /* end if */
            else
                new_down = NULL;

            /* Allocate new span node to append to list */
            if (NULL == (new_span = H5S__hyper_new_span(low, high, new_down, NULL)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab span");

            /* Update the high bounds for current dimension */
            (*span_tree)->high_bounds[0] = high;

            /* Update low & high bounds in lower dimensions, if there are any */
            if (down) {
                /* Sanity checks */
                assert(ndims > 1);
                assert(down_cmp >= 0);

                /* Check if we are sharing down spans with a previous node */
                /* (Only need to check for bounds changing if down spans aren't shared) */
                if (down_cmp == false) {
                    unsigned u; /* Local index variable */

                    /* Loop over lower dimensions, checking & updating low & high bounds */
                    for (u = 0; u < (ndims - 1); u++) {
                        if (down->low_bounds[u] < (*span_tree)->low_bounds[u + 1])
                            (*span_tree)->low_bounds[u + 1] = down->low_bounds[u];
                        if (down->high_bounds[u] > (*span_tree)->high_bounds[u + 1])
                            (*span_tree)->high_bounds[u + 1] = down->high_bounds[u];
                    } /* end for */
                }     /* end if */
            }         /* end if */

            /* Append to end of merged spans list */
            (*span_tree)->tail->next = new_span;
            (*span_tree)->tail       = new_span;
        } /* end else */
    }     /* end else */

done:
    if (ret_value < 0)
        if (new_span)
            if (H5S__hyper_free_span(new_span) < 0)
                HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_append_span() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_clip_spans
 PURPOSE
    Clip a new span tree against the current spans in the hyperslab selection
 USAGE
    herr_t H5S__hyper_clip_spans(span_a, span_b, selector, curr_dim, dim_size,
                                span_a_b_bounds[4], all_clips_bound,
                                a_not_b, a_and_b, b_not_a)
        H5S_hyper_span_t *a_spans;    IN: Span tree 'a' to clip with.
        H5S_hyper_span_t *b_spans;    IN: Span tree 'b' to clip with.
        unsigned selector;            IN: The parameter deciding which output is needed
                                          (only considering the last three bits ABC:
                                           If A is set, then a_not_b is needed;
                                           If B is set, then a_and_b is needed;
                                           If C is set, then b_not_a is needed;
                                           )
        unsigned ndims;               IN: Number of dimensions of this span tree
        H5S_hyper_span_t **a_not_b;  OUT: Span tree of 'a' hyperslab spans which
                                            doesn't overlap with 'b' hyperslab
                                            spans.
        H5S_hyper_span_t **a_and_b;  OUT: Span tree of 'a' hyperslab spans which
                                            overlaps with 'b' hyperslab spans.
        H5S_hyper_span_t **b_not_a;  OUT: Span tree of 'b' hyperslab spans which
                                            doesn't overlap with 'a' hyperslab
                                            spans.
 RETURNS
    non-negative on success, negative on failure
 DESCRIPTION
    Clip one span tree ('a') against another span tree ('b').  Creates span
    trees for the area defined by the 'a' span tree which does not overlap the
    'b' span tree ("a not b"), the area defined by the overlap of the 'a'
    hyperslab span tree and the 'b' span tree ("a and b"), and the area defined
    by the 'b' hyperslab span tree which does not overlap the 'a' span
    tree ("b not a").
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_clip_spans(H5S_hyper_span_info_t *a_spans, H5S_hyper_span_info_t *b_spans, unsigned selector,
                      unsigned ndims, H5S_hyper_span_info_t **a_not_b, H5S_hyper_span_info_t **a_and_b,
                      H5S_hyper_span_info_t **b_not_a)
{
    bool   need_a_not_b;        /* Whether to generate a_not_b list */
    bool   need_a_and_b;        /* Whether to generate a_and_b list */
    bool   need_b_not_a;        /* Whether to generate b_not_a list */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(a_spans);
    assert(b_spans);
    assert(a_not_b);
    assert(a_and_b);
    assert(b_not_a);

    /* Set which list(s) to be generated, based on selector */
    need_a_not_b = ((selector & H5S_HYPER_COMPUTE_A_NOT_B) != 0);
    need_a_and_b = ((selector & H5S_HYPER_COMPUTE_A_AND_B) != 0);
    need_b_not_a = ((selector & H5S_HYPER_COMPUTE_B_NOT_A) != 0);

    /* Check if both span trees are not defined */
    if (a_spans == NULL && b_spans == NULL) {
        *a_not_b = NULL;
        *a_and_b = NULL;
        *b_not_a = NULL;
    } /* end if */
    /* If span 'a' is not defined, but 'b' is, copy 'b' and set the other return span trees to empty */
    else if (a_spans == NULL) {
        *a_not_b = NULL;
        *a_and_b = NULL;
        if (need_b_not_a) {
            if (NULL == (*b_not_a = H5S__hyper_copy_span(b_spans, ndims)))
                HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, FAIL, "can't copy hyperslab span tree");
        } /* end if */
        else
            *b_not_a = NULL;
    } /* end if */
    /* If span 'b' is not defined, but 'a' is, copy 'a' and set the other return span trees to empty */
    else if (b_spans == NULL) {
        *a_and_b = NULL;
        *b_not_a = NULL;
        if (need_a_not_b) {
            if (NULL == (*a_not_b = H5S__hyper_copy_span(a_spans, ndims)))
                HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, FAIL, "can't copy hyperslab span tree");
        } /* end if */
        else
            *a_not_b = NULL;
    } /* end if */
    /* If span 'a' and 'b' are both defined, calculate the proper span trees */
    else {
        /* Check if both span trees completely overlap */
        if (H5S__hyper_cmp_spans(a_spans, b_spans)) {
            *a_not_b = NULL;
            *b_not_a = NULL;
            if (need_a_and_b) {
                if (NULL == (*a_and_b = H5S__hyper_copy_span(a_spans, ndims)))
                    HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, FAIL, "can't copy hyperslab span tree");
            } /* end if */
            else
                *a_and_b = NULL;
        } /* end if */
        else {
            H5S_hyper_span_t *span_a;               /* Pointer to a node in span tree 'a' */
            H5S_hyper_span_t *span_b;               /* Pointer to a node in span tree 'b' */
            bool              recover_a, recover_b; /* Flags to indicate when to recover temporary spans */

            /* Get the pointers to the new and old span lists */
            span_a = a_spans->head;
            span_b = b_spans->head;

            /* No spans to recover yet */
            recover_a = recover_b = false;

            /* Work through the list of spans in the new list */
            while (span_a != NULL && span_b != NULL) {
                H5S_hyper_span_info_t *down_a_not_b; /* Temporary pointer to a_not_b span tree of down spans
                                                        for overlapping nodes */
                H5S_hyper_span_info_t *down_a_and_b; /* Temporary pointer to a_and_b span tree of down spans
                                                        for overlapping nodes */
                H5S_hyper_span_info_t *down_b_not_a; /* Temporary pointer to b_and_a span tree of down spans
                                                        for overlapping nodes */
                H5S_hyper_span_t *tmp_span;          /* Temporary pointer to new span */

                /* Check if span 'a' is completely before span 'b' */
                /*    AAAAAAA                            */
                /* <-----------------------------------> */
                /*             BBBBBBBBBB                */
                if (span_a->high < span_b->low) {
                    /* Copy span 'a' and add to a_not_b list */

                    /* Merge/add span 'a' with/to a_not_b list */
                    if (need_a_not_b)
                        if (H5S__hyper_append_span(a_not_b, ndims, span_a->low, span_a->high, span_a->down) <
                            0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");

                    /* Advance span 'a', leave span 'b' */
                    H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, FAIL);
                } /* end if */
                /* Check if span 'a' overlaps only the lower bound */
                /*  of span 'b' , up to the upper bound of span 'b' */
                /*    AAAAAAAAAAAA                       */
                /* <-----------------------------------> */
                /*             BBBBBBBBBB                */
                else if (span_a->low < span_b->low &&
                         (span_a->high >= span_b->low && span_a->high <= span_b->high)) {
                    /* Split span 'a' into two parts at the low bound of span 'b' */

                    /* Merge/add lower part of span 'a' with/to a_not_b list */
                    if (need_a_not_b)
                        if (H5S__hyper_append_span(a_not_b, ndims, span_a->low, span_b->low - 1,
                                                   span_a->down) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");

                    /* Check for overlaps between upper part of span 'a' and lower part of span 'b' */

                    /* Make certain both spans either have a down span or both don't have one */
                    assert((span_a->down != NULL && span_b->down != NULL) ||
                           (span_a->down == NULL && span_b->down == NULL));

                    /* If there are no down spans, just add the overlapping area to the a_and_b list */
                    if (span_a->down == NULL) {
                        /* Merge/add overlapped part with/to a_and_b list */
                        if (need_a_and_b)
                            if (H5S__hyper_append_span(a_and_b, ndims, span_b->low, span_a->high, NULL) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                    } /* end if */
                    /* If there are down spans, check for the overlap in them and add to each appropriate list
                     */
                    else {
                        /* NULL out the temporary pointers to clipped areas in down spans */
                        down_a_not_b = NULL;
                        down_a_and_b = NULL;
                        down_b_not_a = NULL;

                        /* Check for overlaps in the 'down spans' of span 'a' & 'b' */
                        /** Note: since the bound box of remaining dimensions
                         *  has been updated in the following clip function (via
                         *  all_clips_bounds), there's no need updating the bound box
                         *  after each append call in the following codes */
                        if (H5S__hyper_clip_spans(span_a->down, span_b->down, selector, ndims - 1,
                                                  &down_a_not_b, &down_a_and_b, &down_b_not_a) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, FAIL,
                                        "can't clip hyperslab information");

                        /* Check for additions to the a_not_b list */
                        if (down_a_not_b) {
                            assert(need_a_not_b == true);

                            /* Merge/add overlapped part with/to a_not_b list */
                            if (H5S__hyper_append_span(a_not_b, ndims, span_b->low, span_a->high,
                                                       down_a_not_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_a_not_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }

                        /* Check for additions to the a_and_b list */
                        if (down_a_and_b) {
                            assert(need_a_and_b == true);

                            /* Merge/add overlapped part with/to a_and_b list */
                            if (H5S__hyper_append_span(a_and_b, ndims, span_b->low, span_a->high,
                                                       down_a_and_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_a_and_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }

                        /* Check for additions to the b_not_a list */
                        if (down_b_not_a) {
                            assert(need_b_not_a == true);

                            /* Merge/add overlapped part with/to b_not_a list */
                            if (H5S__hyper_append_span(b_not_a, ndims, span_b->low, span_a->high,
                                                       down_b_not_a) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_b_not_a) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }
                    } /* end else */

                    /* Split off upper part of span 'b' at upper span of span 'a' */

                    /* Check if there is actually an upper part of span 'b' to split off */
                    if (span_a->high < span_b->high) {
                        /* Allocate new span node for upper part of span 'b' */
                        if (NULL == (tmp_span = H5S__hyper_new_span(span_a->high + 1, span_b->high,
                                                                    span_b->down, span_b->next)))
                            HGOTO_ERROR(H5E_DATASPACE, H5E_NOSPACE, FAIL, "can't allocate hyperslab span");

                        /* Advance span 'a' */
                        H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, FAIL);

                        /* Make upper part of span 'b' into new span 'b' */
                        H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, tmp_span, FAIL);
                        recover_b = true;
                    } /* end if */
                    /* No upper part of span 'b' to split */
                    else {
                        /* Advance both 'a' and 'b' */
                        H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, FAIL);
                        H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, FAIL);
                    } /* end else */
                }     /* end if */
                /* Check if span 'a' overlaps the lower & upper bound */
                /*  of span 'b' */
                /*    AAAAAAAAAAAAAAAAAAAAA              */
                /* <-----------------------------------> */
                /*             BBBBBBBBBB                */
                else if (span_a->low < span_b->low && span_a->high > span_b->high) {
                    /* Split off lower part of span 'a' at lower span of span 'b' */

                    /* Merge/add lower part of span 'a' with/to a_not_b list */
                    if (need_a_not_b)
                        if (H5S__hyper_append_span(a_not_b, ndims, span_a->low, span_b->low - 1,
                                                   span_a->down) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");

                    /* Check for overlaps between middle part of span 'a' and span 'b' */

                    /* Make certain both spans either have a down span or both don't have one */
                    assert((span_a->down != NULL && span_b->down != NULL) ||
                           (span_a->down == NULL && span_b->down == NULL));

                    /* If there are no down spans, just add the overlapping area to the a_and_b list */
                    if (span_a->down == NULL) {
                        /* Merge/add overlapped part with/to a_and_b list */
                        if (need_a_and_b)
                            if (H5S__hyper_append_span(a_and_b, ndims, span_b->low, span_b->high, NULL) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                    } /* end if */
                    /* If there are down spans, check for the overlap in them and add to each appropriate list
                     */
                    else {
                        /* NULL out the temporary pointers to clipped areas in down spans */
                        down_a_not_b = NULL;
                        down_a_and_b = NULL;
                        down_b_not_a = NULL;

                        /* Check for overlaps in the 'down spans' of span 'a' & 'b' */
                        if (H5S__hyper_clip_spans(span_a->down, span_b->down, selector, ndims - 1,
                                                  &down_a_not_b, &down_a_and_b, &down_b_not_a) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, FAIL,
                                        "can't clip hyperslab information");

                        /* Check for additions to the a_not_b list */
                        if (down_a_not_b) {
                            assert(need_a_not_b == true);

                            /* Merge/add overlapped part with/to a_not_b list */
                            if (H5S__hyper_append_span(a_not_b, ndims, span_b->low, span_b->high,
                                                       down_a_not_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_a_not_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }

                        /* Check for additions to the a_and_b list */
                        if (down_a_and_b) {
                            assert(need_a_and_b == true);

                            /* Merge/add overlapped part with/to a_and_b list */
                            if (H5S__hyper_append_span(a_and_b, ndims, span_b->low, span_b->high,
                                                       down_a_and_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_a_and_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }

                        /* Check for additions to the b_not_a list */
                        if (down_b_not_a) {
                            assert(need_b_not_a == true);

                            /* Merge/add overlapped part with/to b_not_a list */
                            if (H5S__hyper_append_span(b_not_a, ndims, span_b->low, span_b->high,
                                                       down_b_not_a) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_b_not_a) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }
                    } /* end else */

                    /* Split off upper part of span 'a' at upper span of span 'b' */

                    /* Allocate new span node for upper part of span 'a' */
                    if (NULL == (tmp_span = H5S__hyper_new_span(span_b->high + 1, span_a->high, span_a->down,
                                                                span_a->next)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_NOSPACE, FAIL, "can't allocate hyperslab span");

                    /* Make upper part of span 'a' the new span 'a' */
                    H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, tmp_span, FAIL);
                    recover_a = true;

                    /* Advance span 'b' */
                    H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, FAIL);
                } /* end if */
                /* Check if span 'a' is entirely within span 'b' */
                /*                AAAAA                  */
                /* <-----------------------------------> */
                /*             BBBBBBBBBB                */
                else if (span_a->low >= span_b->low && span_a->high <= span_b->high) {
                    /* Split off lower part of span 'b' at lower span of span 'a' */

                    /* Check if there is actually a lower part of span 'b' to split off */
                    if (span_a->low > span_b->low) {
                        /* Merge/add lower part of span 'b' with/to b_not_a list */
                        if (need_b_not_a)
                            if (H5S__hyper_append_span(b_not_a, ndims, span_b->low, span_a->low - 1,
                                                       span_b->down) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                    } /* end if */
                    else {
                        /* Keep going, nothing to split off */
                    } /* end else */

                    /* Check for overlaps between span 'a' and midle of span 'b' */

                    /* Make certain both spans either have a down span or both don't have one */
                    assert((span_a->down != NULL && span_b->down != NULL) ||
                           (span_a->down == NULL && span_b->down == NULL));

                    /* If there are no down spans, just add the overlapping area to the a_and_b list */
                    if (span_a->down == NULL) {
                        /* Merge/add overlapped part with/to a_and_b list */
                        if (need_a_and_b)
                            if (H5S__hyper_append_span(a_and_b, ndims, span_a->low, span_a->high, NULL) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                    } /* end if */
                    /* If there are down spans, check for the overlap in them and add to each appropriate list
                     */
                    else {
                        /* NULL out the temporary pointers to clipped areas in down spans */
                        down_a_not_b = NULL;
                        down_a_and_b = NULL;
                        down_b_not_a = NULL;

                        /* Check for overlaps in the 'down spans' of span 'a' & 'b' */
                        if (H5S__hyper_clip_spans(span_a->down, span_b->down, selector, ndims - 1,
                                                  &down_a_not_b, &down_a_and_b, &down_b_not_a) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, FAIL,
                                        "can't clip hyperslab information");

                        /* Check for additions to the a_not_b list */
                        if (down_a_not_b) {
                            assert(need_a_not_b == true);

                            /* Merge/add overlapped part with/to a_not_b list */
                            if (H5S__hyper_append_span(a_not_b, ndims, span_a->low, span_a->high,
                                                       down_a_not_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_a_not_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }

                        /* Check for additions to the a_and_b list */
                        if (down_a_and_b) {
                            assert(need_a_and_b == true);

                            /* Merge/add overlapped part with/to a_and_b list */
                            if (H5S__hyper_append_span(a_and_b, ndims, span_a->low, span_a->high,
                                                       down_a_and_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_a_and_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }

                        /* Check for additions to the b_not_a list */
                        if (down_b_not_a) {
                            assert(need_b_not_a == true);

                            /* Merge/add overlapped part with/to b_not_a list */
                            if (H5S__hyper_append_span(b_not_a, ndims, span_a->low, span_a->high,
                                                       down_b_not_a) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_b_not_a) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }
                    } /* end else */

                    /* Check if there is actually an upper part of span 'b' to split off */
                    if (span_a->high < span_b->high) {
                        /* Split off upper part of span 'b' at upper span of span 'a' */

                        /* Allocate new span node for upper part of spans 'a' */
                        if (NULL == (tmp_span = H5S__hyper_new_span(span_a->high + 1, span_b->high,
                                                                    span_b->down, span_b->next)))
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab span");

                        /* And advance span 'a' */
                        H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, FAIL);

                        /* Make upper part of span 'b' the new span 'b' */
                        H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, tmp_span, FAIL);
                        recover_b = true;
                    } /* end if */
                    else {
                        /* Advance both span 'a' & span 'b' */
                        H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, FAIL);
                        H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, FAIL);
                    } /* end else */
                }     /* end if */
                /* Check if span 'a' overlaps only the upper bound */
                /*  of span 'b' */
                /*                AAAAAAAAAA             */
                /* <-----------------------------------> */
                /*             BBBBBBBBBB                */
                else if ((span_a->low >= span_b->low && span_a->low <= span_b->high) &&
                         span_a->high > span_b->high) {
                    /* Check if there is actually a lower part of span 'b' to split off */
                    if (span_a->low > span_b->low) {
                        /* Split off lower part of span 'b' at lower span of span 'a' */

                        /* Merge/add lower part of span 'b' with/to b_not_a list */
                        if (need_b_not_a)
                            if (H5S__hyper_append_span(b_not_a, ndims, span_b->low, span_a->low - 1,
                                                       span_b->down) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                    } /* end if */
                    else {
                        /* Keep going, nothing to split off */
                    } /* end else */

                    /* Check for overlaps between lower part of span 'a' and upper part of span 'b' */

                    /* Make certain both spans either have a down span or both don't have one */
                    assert((span_a->down != NULL && span_b->down != NULL) ||
                           (span_a->down == NULL && span_b->down == NULL));

                    /* If there are no down spans, just add the overlapping area to the a_and_b list */
                    if (span_a->down == NULL) {
                        /* Merge/add overlapped part with/to a_and_b list */
                        if (need_a_and_b)
                            if (H5S__hyper_append_span(a_and_b, ndims, span_a->low, span_b->high, NULL) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                    } /* end if */
                    /* If there are down spans, check for the overlap in them and add to each appropriate list
                     */
                    else {
                        /* NULL out the temporary pointers to clipped areas in down spans */
                        down_a_not_b = NULL;
                        down_a_and_b = NULL;
                        down_b_not_a = NULL;

                        /* Check for overlaps in the 'down spans' of span 'a' & 'b' */
                        if (H5S__hyper_clip_spans(span_a->down, span_b->down, selector, ndims - 1,
                                                  &down_a_not_b, &down_a_and_b, &down_b_not_a) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, FAIL,
                                        "can't clip hyperslab information");

                        /* Check for additions to the a_not_b list */
                        if (down_a_not_b) {
                            assert(need_a_not_b == true);

                            /* Merge/add overlapped part with/to a_not_b list */
                            if (H5S__hyper_append_span(a_not_b, ndims, span_a->low, span_b->high,
                                                       down_a_not_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_a_not_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }

                        /* Check for additions to the a_and_b list */
                        if (down_a_and_b) {
                            assert(need_a_and_b == true);

                            /* Merge/add overlapped part with/to a_and_b list */
                            if (H5S__hyper_append_span(a_and_b, ndims, span_a->low, span_b->high,
                                                       down_a_and_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_a_and_b) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }

                        /* Check for additions to the b_not_a list */
                        if (down_b_not_a) {
                            assert(need_b_not_a == true);

                            /* Merge/add overlapped part with/to b_not_a list */
                            if (H5S__hyper_append_span(b_not_a, ndims, span_a->low, span_b->high,
                                                       down_b_not_a) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");

                            /* Release the down span tree generated */
                            if (H5S__hyper_free_span_info(down_b_not_a) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                        }
                    } /* end else */

                    /* Split off upper part of span 'a' at upper span of span 'b' */

                    /* Allocate new span node for upper part of span 'a' */
                    if (NULL == (tmp_span = H5S__hyper_new_span(span_b->high + 1, span_a->high, span_a->down,
                                                                span_a->next)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab span");

                    /* Make upper part of span 'a' into new span 'a' */
                    H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, tmp_span, FAIL);
                    recover_a = true;

                    /* Advance span 'b' */
                    H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, FAIL);
                } /* end if */
                /* span 'a' must be entirely above span 'b' */
                /*                         AAAAA         */
                /* <-----------------------------------> */
                /*             BBBBBBBBBB                */
                else {
                    /* Copy span 'b' and add to b_not_a list */

                    /* Merge/add span 'b' with/to b_not_a list */
                    if (need_b_not_a)
                        if (H5S__hyper_append_span(b_not_a, ndims, span_b->low, span_b->high, span_b->down) <
                            0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");

                    /* Advance span 'b', leave span 'a' */
                    H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, FAIL);
                } /* end else */
            }     /* end while */

            /* Clean up 'a' spans which haven't been covered yet */
            if (span_a != NULL && span_b == NULL) {
                /* Check if need to merge/add 'a' spans with/to a_not_b list */
                if (need_a_not_b) {
                    /* (This loop, and the similar one below for 'b' spans,
                     *  could be replaced with an optimized routine that quickly
                     *  appended the remaining spans to the 'not' list, but
                     *  until it looks like it's taking a lot of time for an
                     *  important use case, it's been left generic, and similar
                     *  to other code above. -QAK, 2019/02/01)
                     */
                    while (span_a != NULL) {
                        /* Copy span 'a' and add to a_not_b list */
                        if (H5S__hyper_append_span(a_not_b, ndims, span_a->low, span_a->high, span_a->down) <
                            0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");

                        /* Advance to the next 'a' span */
                        H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, FAIL);
                    } /* end while */
                }     /* end if */
                else {
                    /* Free the span, if it's generated */
                    if (recover_a)
                        if (H5S__hyper_free_span(span_a) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span");
                } /* end else */
            }     /* end if */
            /* Clean up 'b' spans which haven't been covered yet */
            else if (span_a == NULL && span_b != NULL) {
                /* Check if need to merge/add 'b' spans with/to b_not_a list */
                if (need_b_not_a) {
                    /* (This loop, and the similar one above for 'a' spans,
                     *  could be replaced with an optimized routine that quickly
                     *  appended the remaining spans to the 'not' list, but
                     *  until it looks like it's taking a lot of time for an
                     *  important use case, it's been left generic, and similar
                     *  to other code above. -QAK, 2019/02/01)
                     */
                    while (span_b != NULL) {
                        /* Copy span 'b' and add to b_not_a list */
                        if (H5S__hyper_append_span(b_not_a, ndims, span_b->low, span_b->high, span_b->down) <
                            0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");

                        /* Advance to the next 'b' span */
                        H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, FAIL);
                    } /* end while */
                }     /* end if */
                else {
                    /* Free the span, if it's generated */
                    if (recover_b)
                        if (H5S__hyper_free_span(span_b) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span");
                } /* end else */
            }     /* end if */
            else
                /* Sanity check */
                assert(span_a == NULL && span_b == NULL);
        } /* end else */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_clip_spans() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_merge_spans_helper
 PURPOSE
    Merge two hyperslab span tree together
 USAGE
    H5S_hyper_span_info_t *H5S__hyper_merge_spans_helper(a_spans, b_spans)
        H5S_hyper_span_info_t *a_spans; IN: First hyperslab spans to merge
                                                together
        H5S_hyper_span_info_t *b_spans; IN: Second hyperslab spans to merge
                                                together
        unsigned ndims;                 IN: Number of dimensions of this span tree
 RETURNS
    Pointer to span tree containing the merged spans on success, NULL on failure
 DESCRIPTION
    Merge two sets of hyperslab spans together and return the span tree from
    the merged set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Handles merging span trees that overlap.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5S_hyper_span_info_t *
H5S__hyper_merge_spans_helper(H5S_hyper_span_info_t *a_spans, H5S_hyper_span_info_t *b_spans, unsigned ndims)
{
    H5S_hyper_span_info_t *merged_spans = NULL; /* Pointer to the merged span tree */
    H5S_hyper_span_info_t *ret_value    = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Make certain both 'a' & 'b' spans have down span trees or neither does */
    assert((a_spans != NULL && b_spans != NULL) || (a_spans == NULL && b_spans == NULL));

    /* Check if the span trees for the 'a' span and the 'b' span are the same */
    if (H5S__hyper_cmp_spans(a_spans, b_spans)) {
        if (a_spans == NULL)
            merged_spans = NULL;
        else {
            /* Copy one of the span trees to return */
            if (NULL == (merged_spans = H5S__hyper_copy_span(a_spans, ndims)))
                HGOTO_ERROR(H5E_INTERNAL, H5E_CANTCOPY, NULL, "can't copy hyperslab span tree");
        } /* end else */
    }     /* end if */
    else {
        H5S_hyper_span_t *span_a;               /* Pointer to current span 'a' working on */
        H5S_hyper_span_t *span_b;               /* Pointer to current span 'b' working on */
        bool              recover_a, recover_b; /* Flags to indicate when to recover temporary spans */

        /* Get the pointers to the 'a' and 'b' span lists */
        span_a = a_spans->head;
        span_b = b_spans->head;

        /* No spans to recover yet */
        recover_a = recover_b = false;

        /* Work through the list of spans in the new list */
        while (span_a != NULL && span_b != NULL) {
            H5S_hyper_span_info_t *tmp_spans; /* Pointer to temporary new span tree */
            H5S_hyper_span_t      *tmp_span;  /* Pointer to temporary new span */

            /* Check if the 'a' span is completely before 'b' span */
            /*    AAAAAAA                            */
            /* <-----------------------------------> */
            /*             BBBBBBBBBB                */
            if (span_a->high < span_b->low) {
                /* Merge/add span 'a' with/to the merged spans */
                if (H5S__hyper_append_span(&merged_spans, ndims, span_a->low, span_a->high, span_a->down) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                /* Advance span 'a' */
                H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, NULL);
            } /* end if */
            /* Check if span 'a' overlaps only the lower bound */
            /*  of span 'b', up to the upper bound of span 'b' */
            /*    AAAAAAAAAAAA                       */
            /* <-----------------------------------> */
            /*             BBBBBBBBBB                */
            else if (span_a->low < span_b->low &&
                     (span_a->high >= span_b->low && span_a->high <= span_b->high)) {
                /* Check if span 'a' and span 'b' down spans are equal */
                if (H5S__hyper_cmp_spans(span_a->down, span_b->down)) {
                    /* Merge/add copy of span 'a' with/to merged spans */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_a->low, span_a->high,
                                               span_a->down) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");
                }
                else {
                    /* Merge/add lower part of span 'a' with/to merged spans */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_a->low, span_b->low - 1,
                                               span_a->down) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                    /* Get merged span tree for overlapped section */
                    tmp_spans = H5S__hyper_merge_spans_helper(span_a->down, span_b->down, ndims - 1);

                    /* Merge/add overlapped section to merged spans */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_b->low, span_a->high, tmp_spans) <
                        0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                    /* Release merged span tree for overlapped section */
                    if (H5S__hyper_free_span_info(tmp_spans) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, NULL, "unable to free span info");
                }

                /* Check if there is an upper part of span 'b' */
                if (span_a->high < span_b->high) {
                    /* Copy upper part of span 'b' as new span 'b' */

                    /* Allocate new span node to append to list */
                    if (NULL == (tmp_span = H5S__hyper_new_span(span_a->high + 1, span_b->high, span_b->down,
                                                                span_b->next)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

                    /* Advance span 'a' */
                    H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, NULL);

                    /* Set new span 'b' to tmp_span */
                    H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, tmp_span, NULL);
                    recover_b = true;
                } /* end if */
                else {
                    /* Advance both span 'a' & 'b' */
                    H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, NULL);
                    H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, NULL);
                } /* end else */
            }     /* end if */
            /* Check if span 'a' overlaps the lower & upper bound */
            /*  of span 'b' */
            /*    AAAAAAAAAAAAAAAAAAAAA              */
            /* <-----------------------------------> */
            /*             BBBBBBBBBB                */
            else if (span_a->low < span_b->low && span_a->high > span_b->high) {
                /* Check if span 'a' and span 'b' down spans are equal */
                if (H5S__hyper_cmp_spans(span_a->down, span_b->down)) {
                    /* Merge/add copy of lower & middle parts of span 'a' to merged spans */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_a->low, span_b->high,
                                               span_a->down) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");
                }
                else {
                    /* Merge/add lower part of span 'a' to merged spans */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_a->low, span_b->low - 1,
                                               span_a->down) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                    /* Get merged span tree for overlapped section */
                    tmp_spans = H5S__hyper_merge_spans_helper(span_a->down, span_b->down, ndims - 1);

                    /* Merge/add overlapped section to merged spans */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_b->low, span_b->high, tmp_spans) <
                        0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                    /* Release merged span tree for overlapped section */
                    if (H5S__hyper_free_span_info(tmp_spans) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, NULL, "unable to free span info");
                }

                /* Copy upper part of span 'a' as new span 'a' (remember to free) */

                /* Allocate new span node to append to list */
                if (NULL == (tmp_span = H5S__hyper_new_span(span_b->high + 1, span_a->high, span_a->down,
                                                            span_a->next)))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

                /* Set new span 'a' to tmp_span */
                H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, tmp_span, NULL);
                recover_a = true;

                /* Advance span 'b' */
                H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, NULL);
            } /* end if */
            /* Check if span 'a' is entirely within span 'b' */
            /*                AAAAA                  */
            /* <-----------------------------------> */
            /*             BBBBBBBBBB                */
            else if (span_a->low >= span_b->low && span_a->high <= span_b->high) {
                /* Check if span 'a' and span 'b' down spans are equal */
                if (H5S__hyper_cmp_spans(span_a->down, span_b->down)) {
                    /* Merge/add copy of lower & middle parts of span 'b' to merged spans */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_b->low, span_a->high,
                                               span_a->down) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");
                }
                else {
                    /* Check if there is a lower part of span 'b' */
                    if (span_a->low > span_b->low) {
                        /* Merge/add lower part of span 'b' to merged spans */
                        if (H5S__hyper_append_span(&merged_spans, ndims, span_b->low, span_a->low - 1,
                                                   span_b->down) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");
                    } /* end if */
                    else {
                        /* No lower part of span 'b' , keep going... */
                    } /* end else */

                    /* Get merged span tree for overlapped section */
                    tmp_spans = H5S__hyper_merge_spans_helper(span_a->down, span_b->down, ndims - 1);

                    /* Merge/add overlapped section to merged spans */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_a->low, span_a->high, tmp_spans) <
                        0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                    /* Release merged span tree for overlapped section */
                    if (H5S__hyper_free_span_info(tmp_spans) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, NULL, "unable to free span info");
                }

                /* Check if there is an upper part of span 'b' */
                if (span_a->high < span_b->high) {
                    /* Copy upper part of span 'b' as new span 'b' (remember to free) */

                    /* Allocate new span node to append to list */
                    if (NULL == (tmp_span = H5S__hyper_new_span(span_a->high + 1, span_b->high, span_b->down,
                                                                span_b->next)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

                    /* Advance span 'a' */
                    H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, NULL);

                    /* Set new span 'b' to tmp_span */
                    H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, tmp_span, NULL);
                    recover_b = true;
                } /* end if */
                else {
                    /* Advance both spans */
                    H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, NULL);
                    H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, NULL);
                } /* end else */
            }     /* end if */
            /* Check if span 'a' overlaps only the upper bound */
            /*  of span 'b' */
            /*                AAAAAAAAAA             */
            /* <-----------------------------------> */
            /*             BBBBBBBBBB                */
            else if ((span_a->low >= span_b->low && span_a->low <= span_b->high) &&
                     span_a->high > span_b->high) {
                /* Check if span 'a' and span 'b' down spans are equal */
                if (H5S__hyper_cmp_spans(span_a->down, span_b->down)) {
                    /* Merge/add copy of span 'b' to merged spans if so */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_b->low, span_b->high,
                                               span_b->down) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");
                }
                else {
                    /* Check if there is a lower part of span 'b' */
                    if (span_a->low > span_b->low) {
                        /* Merge/add lower part of span 'b' to merged spans */
                        if (H5S__hyper_append_span(&merged_spans, ndims, span_b->low, span_a->low - 1,
                                                   span_b->down) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");
                    } /* end if */
                    else {
                        /* No lower part of span 'b' , keep going... */
                    } /* end else */

                    /* Get merged span tree for overlapped section */
                    tmp_spans = H5S__hyper_merge_spans_helper(span_a->down, span_b->down, ndims - 1);

                    /* Merge/add overlapped section to merged spans */
                    if (H5S__hyper_append_span(&merged_spans, ndims, span_a->low, span_b->high, tmp_spans) <
                        0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                    /* Release merged span tree for overlapped section */
                    if (H5S__hyper_free_span_info(tmp_spans) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, NULL, "unable to free span info");
                }

                /* Copy upper part of span 'a' as new span 'a' */

                /* Allocate new span node to append to list */
                if (NULL == (tmp_span = H5S__hyper_new_span(span_b->high + 1, span_a->high, span_a->down,
                                                            span_a->next)))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

                /* Set new span 'a' to tmp_span */
                H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, tmp_span, NULL);
                recover_a = true;

                /* Advance span 'b' */
                H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, NULL);
            } /* end if */
            /* Span 'a' must be entirely above span 'b' */
            /*                         AAAAA         */
            /* <-----------------------------------> */
            /*             BBBBBBBBBB                */
            else {
                /* Merge/add span 'b' with the merged spans */
                if (H5S__hyper_append_span(&merged_spans, ndims, span_b->low, span_b->high, span_b->down) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                /* Advance span 'b' */
                H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, NULL);
            } /* end else */
        }     /* end while */

        /* Clean up 'a' spans which haven't been added to the list of merged spans */
        if (span_a != NULL && span_b == NULL) {
            while (span_a != NULL) {
                /* Merge/add all 'a' spans into the merged spans */
                if (H5S__hyper_append_span(&merged_spans, ndims, span_a->low, span_a->high, span_a->down) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                /* Advance to next 'a' span, until all processed */
                H5S_HYPER_ADVANCE_SPAN(recover_a, span_a, span_a->next, NULL);
            } /* end while */
        }     /* end if */

        /* Clean up 'b' spans which haven't been added to the list of merged spans */
        if (span_a == NULL && span_b != NULL) {
            while (span_b != NULL) {
                /* Merge/add all 'b' spans into the merged spans */
                if (H5S__hyper_append_span(&merged_spans, ndims, span_b->low, span_b->high, span_b->down) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, NULL, "can't allocate hyperslab span");

                /* Advance to next 'b' span, until all processed */
                H5S_HYPER_ADVANCE_SPAN(recover_b, span_b, span_b->next, NULL);
            } /* end while */
        }     /* end if */
    }         /* end else */

    /* Set return value */
    ret_value = merged_spans;

done:
    if (ret_value == NULL)
        if (merged_spans)
            if (H5S__hyper_free_span_info(merged_spans) < 0)
                HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, NULL, "unable to free span info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_merge_spans_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_merge_spans
 PURPOSE
    Merge new hyperslab spans to existing hyperslab selection
 USAGE
    herr_t H5S__hyper_merge_spans(space, new_spans, can_own)
        H5S_t *space;             IN: Dataspace to add new spans to hyperslab
                                        selection.
        H5S_hyper_span_t *new_spans;    IN: Span tree of new spans to add to
                                            hyperslab selection
 RETURNS
    non-negative on success, negative on failure
 DESCRIPTION
    Add a set of hyperslab spans to an existing hyperslab selection.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_merge_spans(H5S_t *space, H5S_hyper_span_info_t *new_spans)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(space);
    assert(new_spans);

    /* If this is the first span tree in the hyperslab selection, just use it */
    if (space->select.sel_info.hslab->span_lst == NULL) {
        space->select.sel_info.hslab->span_lst = new_spans;
        space->select.sel_info.hslab->span_lst->count++;
    } /* end if */
    else {
        H5S_hyper_span_info_t *merged_spans;

        /* Get the merged spans */
        if (NULL == (merged_spans = H5S__hyper_merge_spans_helper(space->select.sel_info.hslab->span_lst,
                                                                  new_spans, space->extent.rank)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTMERGE, FAIL, "can't merge hyperslab spans");

        /* Free the previous spans */
        if (H5S__hyper_free_span_info(space->select.sel_info.hslab->span_lst) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");

        /* Point to the new merged spans */
        space->select.sel_info.hslab->span_lst = merged_spans;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_merge_spans() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_spans_nelem_helper
 PURPOSE
    Count the number of elements in a span tree
 USAGE
    hsize_t H5S__hyper_spans_nelem_helper(spans, op_info_i, op_gen)
        const H5S_hyper_span_info_t *spans; IN: Hyperslan span tree to count elements of
        unsigned op_info_i;             IN: Index of op info to use
        uint64_t op_gen;                IN: Operation generation
 RETURNS
    Number of elements in span tree on success; negative on failure
 DESCRIPTION
    Counts the number of elements described by the spans in a span tree.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static hsize_t
H5S__hyper_spans_nelem_helper(H5S_hyper_span_info_t *spans, unsigned op_info_i, uint64_t op_gen)
{
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(spans);

    /* Check if the span tree was already counted */
    if (spans->op_info[op_info_i].op_gen == op_gen)
        /* Just return the # of elements in the already counted span tree */
        ret_value = spans->op_info[op_info_i].u.nelmts;
    else {                            /* Count the number of elements in the span tree */
        const H5S_hyper_span_t *span; /* Hyperslab span */

        span = spans->head;
        if (NULL == span->down) {
            while (span != NULL) {
                /* Compute # of elements covered */
                ret_value += (span->high - span->low) + 1;

                /* Advance to next span */
                span = span->next;
            } /* end while */
        }     /* end if */
        else {
            while (span != NULL) {
                hsize_t nelmts; /* # of elements covered by current span */

                /* Compute # of elements covered */
                nelmts = (span->high - span->low) + 1;

                /* Multiply the size of this span by the total down span elements */
                ret_value += nelmts * H5S__hyper_spans_nelem_helper(span->down, op_info_i, op_gen);

                /* Advance to next span */
                span = span->next;
            } /* end while */
        }     /* end else */

        /* Set the operation generation for this span tree, to avoid re-computing */
        spans->op_info[op_info_i].op_gen = op_gen;

        /* Hold a copy of the # of elements */
        spans->op_info[op_info_i].u.nelmts = ret_value;
    } /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_spans_nelem_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_spans_nelem
 PURPOSE
    Count the number of elements in a span tree
 USAGE
    hsize_t H5S__hyper_spans_nelem(spans)
        const H5S_hyper_span_info_t *spans; IN: Hyperslan span tree to count elements of
 RETURNS
    Number of elements in span tree on success; negative on failure
 DESCRIPTION
    Counts the number of elements described by the spans in a span tree.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static hsize_t
H5S__hyper_spans_nelem(H5S_hyper_span_info_t *spans)
{
    uint64_t op_gen;        /* Operation generation value */
    hsize_t  ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(spans);

    /* Acquire an operation generation value for this operation */
    op_gen = H5S__hyper_get_op_gen();

    /* Count the number of elements in the span tree */
    /* Always use op_info[0] since we own this op_info, so there can be no
     * simultaneous operations */
    ret_value = H5S__hyper_spans_nelem_helper(spans, 0, op_gen);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_spans_nelem() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_add_disjoint_spans
 PURPOSE
    Add new hyperslab spans to existing hyperslab selection in the case the
    new hyperslab spans don't overlap with the existing hyperslab selection
 USAGE
    herr_t H5S__hyper_add_disjoint_spans(space, new_spans)
        H5S_t *space;             IN: Dataspace to add new spans to hyperslab
                                        selection.
        H5S_hyper_span_t *new_spans;    IN: Span tree of new spans to add to
                                            hyperslab selection
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Add a set of hyperslab spans to an existing hyperslab selection.  The
    new spans are required not to overlap with the existing spans in the
    dataspace's current hyperslab selection in terms of bound box.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_add_disjoint_spans(H5S_t *space, H5S_hyper_span_info_t *new_spans)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);
    assert(new_spans);

    /* Update the number of elements in the selection */
    space->select.num_elem += H5S__hyper_spans_nelem(new_spans);

    /* Add the new spans to the existing selection in the dataspace */
    if (H5S__hyper_merge_spans(space, new_spans) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't merge hyperslabs");

    /* Free the memory space for new spans */
    if (H5S__hyper_free_span_info(new_spans) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_add_disjoint_spans */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_make_spans
 PURPOSE
    Create a span tree
 USAGE
    H5S_hyper_span_t *H5S__hyper_make_spans(rank, start, stride, count, block)
        unsigned rank;          IN: # of dimensions of the space
        const hsize_t *start;   IN: Starting location of the hyperslabs
        const hsize_t *stride;  IN: Stride from the beginning of one block to
                                        the next
        const hsize_t *count;   IN: Number of blocks
        const hsize_t *block;   IN: Size of hyperslab block
 RETURNS
    Pointer to new span tree on success, NULL on failure
 DESCRIPTION
    Generates a new span tree for the hyperslab parameters specified.
    Each span tree has a list of the elements spanned in each dimension, with
    each span node containing a pointer to the list of spans in the next
    dimension down.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5S_hyper_span_info_t *
H5S__hyper_make_spans(unsigned rank, const hsize_t *start, const hsize_t *stride, const hsize_t *count,
                      const hsize_t *block)
{
    H5S_hyper_span_info_t *down = NULL;      /* Pointer to spans in next dimension down */
    H5S_hyper_span_t      *last_span;        /* Current position in hyperslab span list */
    H5S_hyper_span_t      *head = NULL;      /* Head of new hyperslab span list */
    int                    i;                /* Counters */
    H5S_hyper_span_info_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(rank > 0);
    assert(start);
    assert(stride);
    assert(count);
    assert(block);

    /* Start creating spans in fastest changing dimension */
    for (i = (int)(rank - 1); i >= 0; i--) {
        hsize_t  curr_low, curr_high; /* Current low & high values */
        hsize_t  dim_stride;          /* Current dim's stride */
        unsigned u;                   /* Local index variable */

        /* Sanity check */
        if (0 == count[i])
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, NULL, "count == 0 is invalid");

        /* Start a new list in this dimension */
        head      = NULL;
        last_span = NULL;

        /* Generate all the span segments for this dimension */
        curr_low   = start[i];
        curr_high  = start[i] + (block[i] - 1);
        dim_stride = stride[i];
        for (u = 0; u < count[i]; u++, curr_low += dim_stride, curr_high += dim_stride) {
            H5S_hyper_span_t *span; /* New hyperslab span */

            /* Allocate a span node */
            if (NULL == (span = H5FL_MALLOC(H5S_hyper_span_t)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

            /* Set the span's basic information */
            span->low  = curr_low;
            span->high = curr_high;
            span->next = NULL;

            /* Set the information for the next dimension down's spans */
            /* (Will be NULL for fastest changing dimension) */
            span->down = down;

            /* Append to the list of spans in this dimension */
            if (head == NULL)
                head = span;
            else
                last_span->next = span;

            /* Move current pointer */
            last_span = span;
        } /* end for */

        /* Increment ref. count of shared span */
        if (down != NULL)
            down->count = (unsigned)count[i];

        /* Allocate a span info node */
        if (NULL == (down = H5S__hyper_new_span_info(rank)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate hyperslab span");

        /* Keep the pointer to the next dimension down's completed list */
        down->head = head;

        /* Keep the tail pointer to the next dimension down's completed list */
        down->tail = last_span;

        /* Set the low & high bounds for this dimension */
        down->low_bounds[0]  = down->head->low;
        down->high_bounds[0] = down->tail->high;

        /* Copy bounds from lower dimensions */
        /* (head & tail pointers share lower dimensions, so using either is OK) */
        if (head->down) {
            H5MM_memcpy(&down->low_bounds[1], &head->down->low_bounds[0],
                        sizeof(hsize_t) * ((rank - 1) - (unsigned)i));
            H5MM_memcpy(&down->high_bounds[1], &head->down->high_bounds[0],
                        sizeof(hsize_t) * ((rank - 1) - (unsigned)i));
        } /* end if */
    }     /* end for */

    /* Indicate that there is a pointer to this tree */
    if (down)
        down->count = 1;

    /* Success!  Return the head of the list in the slowest changing dimension */
    ret_value = down;

done:
    /* cleanup if error (ret_value will be NULL) */
    if (!ret_value) {
        if (head || down) {
            if (head && down)
                if (down->head != head)
                    down = NULL;

            do {
                if (down) {
                    head = down->head;
                    down = (H5S_hyper_span_info_t *)H5FL_ARR_FREE(hbounds_t, down);
                } /* end if */
                down = head->down;

                while (head) {
                    last_span = head->next;
                    head      = H5FL_FREE(H5S_hyper_span_t, head);
                    head      = last_span;
                } /* end while */
            } while (down);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_make_spans() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_update_diminfo
 PURPOSE
    Attempt to update optimized hyperslab information quickly.  (It can be
    recovered with regular selection).  If this algorithm cannot determine
    the optimized dimension info quickly, this function will simply mark it
    as invalid and unknown if it can be built (H5S_DIMINFO_VALID_NO), so
    H5S__hyper_rebuild can be run later to determine for sure.
 USAGE
    herr_t H5S__hyper_update_diminfo(space, op, new_hyper_diminfo)
        H5S_t *space;       IN: Dataspace to check
        H5S_seloper_t op;   IN: The operation being performed on the
                                selection
        const H5S_hyper_dim_t new_hyper_diminfo; IN: The new selection that
                                                     is being combined with
                                                     the current
 RETURNS
    >=0 on success, <0 on failure
 DESCRIPTION
    Examine the span tree for a hyperslab selection and rebuild
    the start/stride/count/block information for the selection, if possible.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_update_diminfo(H5S_t *space, H5S_seloper_t op, const H5S_hyper_dim_t *new_hyper_diminfo)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(space);
    assert(new_hyper_diminfo);

    /* Check for conditions that prevent us from using the fast algorithm here */
    /* (and instead require H5S__hyper_rebuild) */
    if (!((op == H5S_SELECT_OR) || (op == H5S_SELECT_XOR)) ||
        space->select.sel_info.hslab->diminfo_valid != H5S_DIMINFO_VALID_YES ||
        !space->select.sel_info.hslab->span_lst->head)
        space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
    else {
        H5S_hyper_dim_t tmp_diminfo[H5S_MAX_RANK]; /* Temporary dimension info */
        bool            found_nonidentical_dim = false;
        unsigned        curr_dim;

        /* Copy current diminfo.opt values */
        H5MM_memcpy(tmp_diminfo, space->select.sel_info.hslab->diminfo.opt, sizeof(tmp_diminfo));

        /* Loop over dimensions */
        for (curr_dim = 0; curr_dim < space->extent.rank; curr_dim++) {
            /* Check for this being identical */
            if ((tmp_diminfo[curr_dim].start != new_hyper_diminfo[curr_dim].start) ||
                (tmp_diminfo[curr_dim].stride != new_hyper_diminfo[curr_dim].stride) ||
                (tmp_diminfo[curr_dim].count != new_hyper_diminfo[curr_dim].count) ||
                (tmp_diminfo[curr_dim].block != new_hyper_diminfo[curr_dim].block)) {
                hsize_t high_start, high_count,
                    high_block; /* The start, count & block values for the higher block */

                /* Dimension is not identical */
                /* Check if we already found a nonidentical dim - only one is
                 * allowed */
                if (found_nonidentical_dim) {
                    space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
                    break;
                } /* end if */

                /* Check that strides are the same, or count is 1 for one of the
                 * slabs */
                if ((tmp_diminfo[curr_dim].stride != new_hyper_diminfo[curr_dim].stride) &&
                    (tmp_diminfo[curr_dim].count > 1) && (new_hyper_diminfo[curr_dim].count > 1)) {
                    space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
                    break;
                } /* end if */

                /* Patch tmp_diminfo.stride if its count is 1 */
                if ((tmp_diminfo[curr_dim].count == 1) && (new_hyper_diminfo[curr_dim].count > 1))
                    tmp_diminfo[curr_dim].stride = new_hyper_diminfo[curr_dim].stride;

                /* Determine lowest start, and set tmp_diminfo.start, count and
                 *  block to use the lowest, and high_start, high_count and
                 *  high_block to use the highest
                 */
                if (tmp_diminfo[curr_dim].start < new_hyper_diminfo[curr_dim].start) {
                    high_start = new_hyper_diminfo[curr_dim].start;
                    high_count = new_hyper_diminfo[curr_dim].count;
                    high_block = new_hyper_diminfo[curr_dim].block;
                } /* end if */
                else {
                    high_start                  = tmp_diminfo[curr_dim].start;
                    tmp_diminfo[curr_dim].start = new_hyper_diminfo[curr_dim].start;
                    high_count                  = tmp_diminfo[curr_dim].count;
                    tmp_diminfo[curr_dim].count = new_hyper_diminfo[curr_dim].count;
                    high_block                  = tmp_diminfo[curr_dim].block;
                    tmp_diminfo[curr_dim].block = new_hyper_diminfo[curr_dim].block;
                } /* end else */

                /* If count is 1 for both slabs, take different actions */
                if ((tmp_diminfo[curr_dim].count == 1) && (high_count == 1)) {
                    /* Check for overlap */
                    if ((tmp_diminfo[curr_dim].start + tmp_diminfo[curr_dim].block) > high_start) {
                        /* Check operation type */
                        if (op == H5S_SELECT_OR)
                            /* Merge blocks */
                            tmp_diminfo[curr_dim].block =
                                ((high_start + high_block) >=
                                 (tmp_diminfo[curr_dim].start + tmp_diminfo[curr_dim].block))
                                    ? (high_start + high_block - tmp_diminfo[curr_dim].start)
                                    : tmp_diminfo[curr_dim].block;
                        else {
                            /* Block values must be the same */
                            if (tmp_diminfo[curr_dim].block != high_block) {
                                space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
                                break;
                            } /* end if */

                            /* XOR - overlap creates 2 blocks */
                            tmp_diminfo[curr_dim].stride = high_block;
                            tmp_diminfo[curr_dim].count  = 2;
                            tmp_diminfo[curr_dim].block  = high_start - tmp_diminfo[curr_dim].start;
                        } /* end else */
                    }     /* end if */
                    else if ((tmp_diminfo[curr_dim].start + tmp_diminfo[curr_dim].block) == high_start)
                        /* Blocks border, merge them */
                        tmp_diminfo[curr_dim].block += high_block;
                    else {
                        /* Distinct blocks */
                        /* Block values must be the same */
                        if (tmp_diminfo[curr_dim].block != high_block) {
                            space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
                            break;
                        } /* end if */

                        /* Create strided selection */
                        tmp_diminfo[curr_dim].stride = high_start - tmp_diminfo[curr_dim].start;
                        tmp_diminfo[curr_dim].count  = 2;
                    } /* end else */
                }     /* end if */
                else {
                    /* Check if block values are the same */
                    if (tmp_diminfo[curr_dim].block != high_block) {
                        space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
                        break;
                    } /* end if */

                    /* Check phase of strides */
                    if ((tmp_diminfo[curr_dim].start % tmp_diminfo[curr_dim].stride) !=
                        (high_start % tmp_diminfo[curr_dim].stride)) {
                        space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
                        break;
                    } /* end if */

                    /* Check operation type */
                    if (op == H5S_SELECT_OR) {
                        /* Make sure the slabs border or overlap */
                        if (high_start > (tmp_diminfo[curr_dim].start +
                                          (tmp_diminfo[curr_dim].count * tmp_diminfo[curr_dim].stride))) {
                            space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
                            break;
                        } /* end if */
                    }     /* end if */
                    else
                        /* XOR: Make sure the slabs border */
                        if (high_start != (tmp_diminfo[curr_dim].start +
                                           (tmp_diminfo[curr_dim].count * tmp_diminfo[curr_dim].stride))) {
                            space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
                            break;
                        } /* end if */

                    /* Set count for combined selection */
                    tmp_diminfo[curr_dim].count =
                        ((high_start - tmp_diminfo[curr_dim].start) / tmp_diminfo[curr_dim].stride) +
                        high_count;
                } /* end else */

                /* Indicate that we found a nonidentical dim */
                found_nonidentical_dim = true;
            } /* end if */
        }     /* end for */

        /* Check if we succeeded, if so, set the new diminfo values */
        if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES)
            for (curr_dim = 0; curr_dim < space->extent.rank; curr_dim++) {
                hsize_t tmp_high_bound;

                /* Set the new diminfo values */
                space->select.sel_info.hslab->diminfo.app[curr_dim].start =
                    space->select.sel_info.hslab->diminfo.opt[curr_dim].start = tmp_diminfo[curr_dim].start;
                assert(tmp_diminfo[curr_dim].stride > 0);
                space->select.sel_info.hslab->diminfo.app[curr_dim].stride =
                    space->select.sel_info.hslab->diminfo.opt[curr_dim].stride = tmp_diminfo[curr_dim].stride;
                assert(tmp_diminfo[curr_dim].count > 0);
                space->select.sel_info.hslab->diminfo.app[curr_dim].count =
                    space->select.sel_info.hslab->diminfo.opt[curr_dim].count = tmp_diminfo[curr_dim].count;
                assert(tmp_diminfo[curr_dim].block > 0);
                space->select.sel_info.hslab->diminfo.app[curr_dim].block =
                    space->select.sel_info.hslab->diminfo.opt[curr_dim].block = tmp_diminfo[curr_dim].block;

                /* Check for updating the low & high bounds */
                if (tmp_diminfo[curr_dim].start < space->select.sel_info.hslab->diminfo.low_bounds[curr_dim])
                    space->select.sel_info.hslab->diminfo.low_bounds[curr_dim] = tmp_diminfo[curr_dim].start;
                tmp_high_bound = tmp_diminfo[curr_dim].start + (tmp_diminfo[curr_dim].block - 1) +
                                 (tmp_diminfo[curr_dim].stride * (tmp_diminfo[curr_dim].count - 1));
                if (tmp_high_bound > space->select.sel_info.hslab->diminfo.low_bounds[curr_dim])
                    space->select.sel_info.hslab->diminfo.high_bounds[curr_dim] = tmp_high_bound;
            } /* end for */
    }         /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_update_diminfo() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_rebuild_helper
 PURPOSE
    Helper routine to rebuild optimized hyperslab information if possible.
    (It can be recovered with regular selection)
 USAGE
    herr_t H5S__hyper_rebuild_helper(space)
        const H5S_hyper_span_t *spans;  IN: Portion of span tree to check
        H5S_hyper_dim_t span_slab_info[]; OUT: Rebuilt section of hyperslab description
 RETURNS
    true/false for hyperslab selection rebuilt
 DESCRIPTION
    Examine the span tree for a hyperslab selection and rebuild
    the start/stride/count/block information for the selection, if possible.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    To be able to recover the optimized information, the span tree must conform
    to span tree able to be generated from a single H5S_SELECT_SET operation.
 EXAMPLES
 REVISION LOG
    KY, 2005/9/22
--------------------------------------------------------------------------*/
static bool
H5S__hyper_rebuild_helper(const H5S_hyper_span_info_t *spans, H5S_hyper_dim_t span_slab_info[])
{
    const H5S_hyper_span_t *span;             /* Hyperslab span */
    const H5S_hyper_span_t *prev_span;        /* Previous span in list */
    hsize_t                 start;            /* Starting element for this dimension */
    hsize_t                 stride;           /* Stride for this dimension */
    hsize_t                 block;            /* Block size for this dimension */
    hsize_t                 prev_low;         /* Low bound for previous span */
    size_t                  spancount;        /* Number of spans encountered in this dimension */
    bool                    ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(spans);

    /* Initialization */
    span      = spans->head;
    stride    = 1;
    prev_low  = 0;
    spancount = 0;

    /* Get "canonical" down span information */
    if (span->down)
        /* Go to the next down span and check whether the selection can be rebuilt */
        if (!H5S__hyper_rebuild_helper(span->down, &span_slab_info[1]))
            HGOTO_DONE(false);

    /* Assign the initial starting point & block size for this dimension */
    start = span->low;
    block = (span->high - span->low) + 1;

    /* Loop the spans */
    prev_span = NULL;
    while (span) {
        if (spancount > 0) {
            hsize_t curr_stride; /* Current stride from previous span */
            hsize_t curr_block;  /* Block size of current span */

            /* Sanity check */
            assert(prev_span);

            /* Check that down spans match current slab info */
            /* (Can skip check if previous span's down pointer is same as current one) */
            if (span->down && prev_span->down != span->down)
                if (!H5S__hyper_cmp_spans(span->down, prev_span->down))
                    HGOTO_DONE(false);

            /* Obtain values for stride and block */
            curr_stride = span->low - prev_low;
            curr_block  = (span->high - span->low) + 1;

            /* Compare stride and block for this span.  To compare stride,
             * three spans are needed.  Account for the first two spans.
             */
            if (curr_block != block)
                HGOTO_DONE(false);
            if (spancount > 1) {
                if (stride != curr_stride)
                    HGOTO_DONE(false);
            } /* end if */
            else
                stride = curr_stride;
        } /* end if */

        /* Keep current starting point */
        prev_low = span->low;

        /* Advance to next span */
        prev_span = span;
        span      = span->next;
        spancount++;
    } /* end while */

    /* Save the span information. */
    span_slab_info[0].start  = start;
    span_slab_info[0].count  = spancount;
    span_slab_info[0].block  = block;
    span_slab_info[0].stride = stride;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_rebuild_helper() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_rebuild
 PURPOSE
    Rebuild optimized hyperslab information if possible.
    (It can be recovered with regular selection)
 USAGE
    void H5S__hyper_rebuild(space)
        H5S_t *space;     IN: Dataspace to check
 RETURNS
    None
 DESCRIPTION
    Examine the span tree for a hyperslab selection and rebuild a regular
    start/stride/count/block hyperslab selection, if possible.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    To be able to recover the optimized information, the span tree must conform
    to span tree able to be generated from a single H5S_SELECT_SET operation.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
void
H5S__hyper_rebuild(H5S_t *space)
{
    H5S_hyper_dim_t rebuilt_slab_info[H5S_MAX_RANK];

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(space);
    assert(space->select.sel_info.hslab->span_lst);

    /* Check whether the slab can be rebuilt */
    /* (Only regular selection can be rebuilt. If yes, fill in correct values) */
    if (false == H5S__hyper_rebuild_helper(space->select.sel_info.hslab->span_lst, rebuilt_slab_info))
        space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_IMPOSSIBLE;
    else {
        /* Set the dimension info & bounds for the dataspace, from the rebuilt info */
        H5MM_memcpy(space->select.sel_info.hslab->diminfo.app, rebuilt_slab_info, sizeof(rebuilt_slab_info));
        H5MM_memcpy(space->select.sel_info.hslab->diminfo.opt, rebuilt_slab_info, sizeof(rebuilt_slab_info));
        H5MM_memcpy(space->select.sel_info.hslab->diminfo.low_bounds,
                    space->select.sel_info.hslab->span_lst->low_bounds, sizeof(hsize_t) * space->extent.rank);
        H5MM_memcpy(space->select.sel_info.hslab->diminfo.high_bounds,
                    space->select.sel_info.hslab->span_lst->high_bounds,
                    sizeof(hsize_t) * space->extent.rank);

        space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_YES;
    } /* end else */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5S__hyper_rebuild() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_generate_spans
 PURPOSE
    Create span tree for a regular hyperslab selection
 USAGE
    herr_t H5S__hyper_generate_spans(space)
        H5S_t *space;           IN/OUT: Pointer to dataspace
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Create a span tree representation of a regular hyperslab selection and
    add it to the information for the hyperslab selection.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_generate_spans(H5S_t *space)
{
    hsize_t  tmp_start[H5S_MAX_RANK];  /* Temporary start information */
    hsize_t  tmp_stride[H5S_MAX_RANK]; /* Temporary stride information */
    hsize_t  tmp_count[H5S_MAX_RANK];  /* Temporary count information */
    hsize_t  tmp_block[H5S_MAX_RANK];  /* Temporary block information */
    unsigned u;                        /* Local index variable */
    herr_t   ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    assert(space);
    assert(H5S_GET_SELECT_TYPE(space) == H5S_SEL_HYPERSLABS);

    /* Get the diminfo */
    for (u = 0; u < space->extent.rank; u++) {
        /* Check for unlimited dimension and return error */
        /* These should be able to be converted to assertions once everything
         * that calls this function checks for unlimited selections first
         * (especially the new hyperslab API)  -NAF */
        if (space->select.sel_info.hslab->diminfo.opt[u].count == H5S_UNLIMITED)
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "can't generate spans with unlimited count");
        if (space->select.sel_info.hslab->diminfo.opt[u].block == H5S_UNLIMITED)
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "can't generate spans with unlimited block");

        tmp_start[u]  = space->select.sel_info.hslab->diminfo.opt[u].start;
        tmp_stride[u] = space->select.sel_info.hslab->diminfo.opt[u].stride;
        tmp_count[u]  = space->select.sel_info.hslab->diminfo.opt[u].count;
        tmp_block[u]  = space->select.sel_info.hslab->diminfo.opt[u].block;
    } /* end for */

    /* Build the hyperslab information also */
    if (H5S__generate_hyperslab(space, H5S_SELECT_SET, tmp_start, tmp_stride, tmp_count, tmp_block) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't generate hyperslabs");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_generate_spans() */

/*--------------------------------------------------------------------------
 NAME
    H5S__check_spans_overlap
 PURPOSE
    Check if two selections' bounds overlap.
 USAGE
    bool H5S__check_spans_overlap(spans1, spans2)
        const H5S_hyper_span_info_t *spans1;  IN: Second span list
        const H5S_hyper_span_info_t *spans2;  IN: Second span list
 RETURNS
    true for overlap, false for no overlap
 PROGRAMMER
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5_ATTR_PURE bool
H5S__check_spans_overlap(const H5S_hyper_span_info_t *spans1, const H5S_hyper_span_info_t *spans2)
{
    bool ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(spans1);
    assert(spans2);

    /* Use low & high bounds to try to avoid spinning through the span lists */
    if (H5S_RANGE_OVERLAP(spans1->low_bounds[0], spans1->high_bounds[0], spans2->low_bounds[0],
                          spans2->high_bounds[0])) {
        H5S_hyper_span_t *span1, *span2; /* Hyperslab spans */

        /* Walk over spans, comparing them for overlap */
        span1 = spans1->head;
        span2 = spans2->head;
        while (span1 && span2) {
            /* Check current two spans for overlap */
            if (H5S_RANGE_OVERLAP(span1->low, span1->high, span2->low, span2->high)) {
                /* Check for spans in lowest dimension already */
                if (span1->down) {
                    /* Sanity check */
                    assert(span2->down);

                    /* Check lower dimensions for overlap */
                    if (H5S__check_spans_overlap(span1->down, span2->down))
                        HGOTO_DONE(true);
                } /* end if */
                else
                    HGOTO_DONE(true);
            } /* end if */

            /* Advance one of the spans */
            if (span1->high <= span2->high) {
                /* Advance span1, unless it would be off the list and span2 has more nodes */
                if (NULL == span1->next && NULL != span2->next)
                    span2 = span2->next;
                else
                    span1 = span1->next;
            } /* end if */
            else {
                /* Advance span2, unless it would be off the list and span1 has more nodes */
                if (NULL == span2->next && NULL != span1->next)
                    span1 = span1->next;
                else
                    span2 = span2->next;
            } /* end else */
        }     /* end while */

        /* Make certain we've exhausted our comparisons */
        assert((NULL == span1 && (NULL != span2 && NULL == span2->next)) ||
               ((NULL != span1 && NULL == span1->next) && NULL == span2));
    } /* end of */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__check_spans_overlap() */

/*--------------------------------------------------------------------------
 NAME
    H5S__fill_in_new_space
 PURPOSE
    Combine two span lists, one from an existing dataspace and the
    other from input arguments, into a new selection depending on the
    selection operator. The new selection is put into a resulting dataspace
    which could be allocated inside the function.
 USAGE
    herr_t H5S__fill_in_new_space(space1, op, space2_span_lst, can_own_span2,
                                span2_owned, result)
        H5S_t *space1;           IN: Dataspace containing the first span list
        H5S_seloper_t op;        IN: Selection operation
        H5S_hyper_span_info_t *space2_span_lst; IN: Second span list
        bool can_own_span2;   IN: Indicates whether the 2nd span list could be
                                     owned by the result. If not, the 2nd span list
                                     has to be copied.
        bool *span2_owned;  OUT: Indicates if the 2nd span list is actually owned
        H5S_t **result;  OUT: The dataspace containing the new selection. It
                              could be same with the 1st dataspace.
 RETURNS
    Non-negative on success, negative on failure
 PROGRAMMER
    Chao Mei July 8, 2011
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__fill_in_new_space(H5S_t *space1, H5S_seloper_t op, H5S_hyper_span_info_t *space2_span_lst,
                       bool can_own_span2, bool *span2_owned, bool *updated_spans, H5S_t **result)
{
    H5S_hyper_span_info_t *a_not_b =
        NULL; /* Span tree for hyperslab spans in old span tree and not in new span tree */
    H5S_hyper_span_info_t *a_and_b = NULL; /* Span tree for hyperslab spans in both old and new span trees */
    H5S_hyper_span_info_t *b_not_a =
        NULL; /* Span tree for hyperslab spans in new span tree and not in old span tree */
    bool   overlapped    = false; /* Whether selections overlap */
    bool   is_result_new = false;
    herr_t ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(space1);
    assert(space2_span_lst);
    assert(op >= H5S_SELECT_OR && op <= H5S_SELECT_NOTA);
    /* The result is either a to-be-created space or an empty one */
    assert(*result == NULL || *result == space1);
    assert(space1->select.sel_info.hslab->span_lst);
    assert(span2_owned);

    /* Reset flags to return */
    *span2_owned   = false;
    *updated_spans = false;

    /* The result shares the same info from space1 */
    if (*result == NULL) {
        if (NULL == ((*result) = H5S_copy(space1, true, true)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to copy dataspace");
        space1->select.sel_info.hslab->span_lst->count--;
        (*result)->select.sel_info.hslab->span_lst = NULL;
        is_result_new                              = true;
    } /* end if */

    /* Check both spaces to see if they overlap */
    overlapped = H5S__check_spans_overlap(space1->select.sel_info.hslab->span_lst, space2_span_lst);

    if (!overlapped) {
        switch (op) {
            case H5S_SELECT_OR:
            case H5S_SELECT_XOR:
                /* Add the new disjoint spans to the space */
                /* Copy of space1's spans to *result, and another copy of space2's spans */
                if (is_result_new)
                    (*result)->select.sel_info.hslab->span_lst =
                        H5S__hyper_copy_span(space1->select.sel_info.hslab->span_lst, space1->extent.rank);
                if (!can_own_span2) {
                    b_not_a = H5S__hyper_copy_span(space2_span_lst, space1->extent.rank);
                    if (H5S__hyper_add_disjoint_spans(*result, b_not_a) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't append hyperslabs");

                    /* The new_spans are now owned by 'space', so they should not be released */
                    b_not_a = NULL;
                } /* end if */
                else {
                    if (H5S__hyper_add_disjoint_spans(*result, space2_span_lst) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't append hyperslabs");
                    *span2_owned = true;
                } /* end else */

                /* Indicate that the spans changed */
                *updated_spans = true;
                break;

            case H5S_SELECT_AND:
                /* Convert *result to "none" selection */
                if (H5S_select_none(*result) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
                HGOTO_DONE(SUCCEED);

            case H5S_SELECT_NOTB:
                /* Copy space1's spans to *result */
                if (is_result_new)
                    (*result)->select.sel_info.hslab->span_lst =
                        H5S__hyper_copy_span(space1->select.sel_info.hslab->span_lst, space1->extent.rank);

                /* Indicate that the spans changed */
                *updated_spans = true;
                break;

            case H5S_SELECT_NOTA:
                if (!is_result_new) {
                    assert(space1 == *result);

                    /* Free the current selection */
                    if (H5S__hyper_free_span_info(space1->select.sel_info.hslab->span_lst) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                    space1->select.sel_info.hslab->span_lst = NULL;
                }

                /* Copy space2's spans to *result */
                if (!can_own_span2)
                    (*result)->select.sel_info.hslab->span_lst =
                        H5S__hyper_copy_span(space2_span_lst, space1->extent.rank);
                else {
                    (*result)->select.sel_info.hslab->span_lst = space2_span_lst;
                    *span2_owned                               = true;
                }

                /* Reset the number of items in selection */
                (*result)->select.num_elem = H5S__hyper_spans_nelem(space2_span_lst);

                /* Indicate that the spans changed */
                *updated_spans = true;
                break;

            case H5S_SELECT_NOOP:
            case H5S_SELECT_SET:
            case H5S_SELECT_APPEND:
            case H5S_SELECT_PREPEND:
            case H5S_SELECT_INVALID:
            default:
                HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
        } /* end switch */
    }     /* end if */
    else {
        unsigned selector = 0; /* Select which clipping spans to generate */

        /* Generate mask for clip operation depending on the op */
        switch (op) {
            case H5S_SELECT_OR: /* a + b_not_a */
                selector = H5S_HYPER_COMPUTE_B_NOT_A;
                break;

            case H5S_SELECT_XOR: /* a_not_b + b_not_a */
                selector = H5S_HYPER_COMPUTE_A_NOT_B | H5S_HYPER_COMPUTE_B_NOT_A;
                break;

            case H5S_SELECT_AND: /* a_and_b */
                selector = H5S_HYPER_COMPUTE_A_AND_B;
                break;

            case H5S_SELECT_NOTB: /* a_not_b */
                selector = H5S_HYPER_COMPUTE_A_NOT_B;
                break;

            case H5S_SELECT_NOTA: /* b_not_a */
                selector = H5S_HYPER_COMPUTE_B_NOT_A;
                break;

            case H5S_SELECT_NOOP:
            case H5S_SELECT_SET:
            case H5S_SELECT_APPEND:
            case H5S_SELECT_PREPEND:
            case H5S_SELECT_INVALID:
            default:
                HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
        } /* end switch */

        /* Generate lists of spans which overlap and don't overlap */
        if (H5S__hyper_clip_spans(space1->select.sel_info.hslab->span_lst, space2_span_lst, selector,
                                  space1->extent.rank, &a_not_b, &a_and_b, &b_not_a) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, FAIL, "can't clip hyperslab information");
        switch (op) {
            case H5S_SELECT_OR:
                if (is_result_new)
                    (*result)->select.sel_info.hslab->span_lst =
                        H5S__hyper_copy_span(space1->select.sel_info.hslab->span_lst, space1->extent.rank);
                break;

            case H5S_SELECT_AND:
            case H5S_SELECT_XOR:
            case H5S_SELECT_NOTB:
            case H5S_SELECT_NOTA:
                if (!is_result_new) {
                    assert(space1 == *result);

                    /* Free the current selection */
                    if (H5S__hyper_free_span_info(space1->select.sel_info.hslab->span_lst) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                    space1->select.sel_info.hslab->span_lst = NULL;
                }

                /* Reset the number of items in selection */
                /* (Will be set below) */
                (*result)->select.num_elem = 0;
                break;

            case H5S_SELECT_NOOP:
            case H5S_SELECT_SET:
            case H5S_SELECT_APPEND:
            case H5S_SELECT_PREPEND:
            case H5S_SELECT_INVALID:
            default:
                HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
        } /* end switch */

        /* Check if there are any non-overlapped selections */
        if (a_not_b) {
            /* Other than OR, the span_lst is set to NULL. And in OR,
             *      a_not_b is not needed
             */
            assert(NULL == (*result)->select.sel_info.hslab->span_lst);

            /* The results dataspace takes ownership of the spans */
            /* (Since it must be NULL) */
            (*result)->select.sel_info.hslab->span_lst = a_not_b;

            /* Update the number of elements in current selection */
            (*result)->select.num_elem = H5S__hyper_spans_nelem(a_not_b);

            /* Indicate that the spans were updated */
            *updated_spans = true;

            /* Indicate that the a_not_b spans are owned */
            a_not_b = NULL;
        } /* end if */

        if (a_and_b) {
            /**
             * 1. Other than OR, the span_lst is set to NULL. And in OR,
             *      a_and_b is not needed
             * 2. a_not_b will never be computed together with a_and_b
             *      because merging these two equals to a.
             */
            assert(NULL == (*result)->select.sel_info.hslab->span_lst);

            /* The results dataspace takes ownership of the spans */
            /* (Since it must be NULL) */
            (*result)->select.sel_info.hslab->span_lst = a_and_b;

            /* Update the number of elements in current selection */
            (*result)->select.num_elem = H5S__hyper_spans_nelem(a_and_b);

            /* Indicate that the spans were updated */
            *updated_spans = true;

            /* Indicate that the a_and_b spans are owned */
            a_and_b = NULL;
        } /* end if */

        if (b_not_a) {
            /* Merge the b_not_a spans into the result dataspace */
            if (H5S__hyper_merge_spans(*result, b_not_a) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't insert hyperslabs");

            /* Update the number of elements in current selection */
            (*result)->select.num_elem += H5S__hyper_spans_nelem(b_not_a);

            /* Indicate that the spans were updated */
            *updated_spans = true;
        } /* end if */
    }     /* end else for the case the new span overlaps with the old (i.e. space) */

    /* Check if the spans weren't updated, and reset selection if so */
    if (!*updated_spans) {
        /* If updated_spans remains false as in this branch, it means the
         *  result has been cleared in XOR / AND / NOTB / NOTA cases, and the
         *  result is a copy of the dataspace in the OR case.
         *
         *  If two dataspaces have generated any of the three clipped
         *  span trees (i.e. a_not_b, a_and_b, and b_not_a), the
         *  updated_spans must be true.
         */
        if (H5S_SELECT_OR != op) {
            /* Convert *result to "none" selection */
            if (H5S_select_none(*result) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
        }
    }

done:
    /* Free resources */
    if (a_not_b)
        if (H5S__hyper_free_span_info(a_not_b) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
    if (a_and_b)
        if (H5S__hyper_free_span_info(a_and_b) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
    if (b_not_a)
        if (H5S__hyper_free_span_info(b_not_a) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__fill_in_new_space() */

/*-------------------------------------------------------------------------
 * Function:    H5S__generate_hyperlab
 *
 * Purpose:     Generate hyperslab information from H5S_select_hyperslab()
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__generate_hyperslab(H5S_t *space, H5S_seloper_t op, const hsize_t start[], const hsize_t stride[],
                        const hsize_t count[], const hsize_t block[])
{
    H5S_hyper_span_info_t *new_spans = NULL;    /* Span tree for new hyperslab */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);
    assert(op > H5S_SELECT_NOOP && op < H5S_SELECT_INVALID);
    assert(start);
    assert(stride);
    assert(count);
    assert(block);

    /* Generate span tree for new hyperslab information */
    if (NULL == (new_spans = H5S__hyper_make_spans(space->extent.rank, start, stride, count, block)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't create hyperslab information");

    /* Generate list of blocks to add/remove based on selection operation */
    if (op == H5S_SELECT_SET) {
        /* Free current selection */
        if (NULL != space->select.sel_info.hslab->span_lst)
            if (H5S__hyper_free_span_info(space->select.sel_info.hslab->span_lst) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");

        /* Set the hyperslab selection to the new span tree */
        space->select.sel_info.hslab->span_lst = new_spans;

        /* Set the number of elements in current selection */
        space->select.num_elem = H5S__hyper_spans_nelem(new_spans);

        /* Indicate that the new_spans are owned */
        new_spans = NULL;
    }
    else {
        bool new_spans_owned = false;
        bool updated_spans   = false;

        /* Generate new spans for space */
        if (H5S__fill_in_new_space(space, op, new_spans, true, &new_spans_owned, &updated_spans, &space) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't generate the specified hyperslab");

        /* Check if the spans were updated by H5S__fill_in_new_space */
        if (updated_spans) {
            H5S_hyper_dim_t new_hyper_diminfo[H5S_MAX_RANK];
            unsigned        u; /* Local index variable */

            /* Sanity check */
            assert(space->select.sel_info.hslab->span_lst->head);

            /* Build diminfo struct */
            for (u = 0; u < space->extent.rank; u++) {
                new_hyper_diminfo[u].start  = start[u];
                new_hyper_diminfo[u].stride = stride[u];
                new_hyper_diminfo[u].count  = count[u];
                new_hyper_diminfo[u].block  = block[u];
            } /* end for */

            /* Update space's dim info */
            if (H5S__hyper_update_diminfo(space, op, new_hyper_diminfo) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOUNT, FAIL, "can't update hyperslab info");
        } /* end if */

        /* Indicate that the new_spans are owned, there's no need to free */
        if (new_spans_owned)
            new_spans = NULL;
    }

done:
    if (new_spans)
        if (H5S__hyper_free_span_info(new_spans) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__generate_hyperslab() */

/*-------------------------------------------------------------------------
 * Function:    H5S__set_regular_hyperslab
 *
 * Purpose:    Set a regular hyperslab
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5S__set_regular_hyperslab(H5S_t *space, const hsize_t start[], const hsize_t *app_stride,
                           const hsize_t app_count[], const hsize_t *app_block, const hsize_t *opt_stride,
                           const hsize_t opt_count[], const hsize_t *opt_block)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);
    assert(start);
    assert(app_stride);
    assert(app_count);
    assert(app_block);
    assert(opt_stride);
    assert(opt_count);
    assert(opt_block);

    /* If we are setting a new selection, remove current selection first */
    if (H5S_SELECT_RELEASE(space) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't release selection");

    /* Allocate space for the hyperslab selection information */
    if (NULL == (space->select.sel_info.hslab = H5FL_MALLOC(H5S_hyper_sel_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab info");

    /* Set the diminfo */
    space->select.num_elem                  = 1;
    space->select.sel_info.hslab->unlim_dim = -1;
    for (u = 0; u < space->extent.rank; u++) {
        /* Set application and optimized hyperslab info */
        space->select.sel_info.hslab->diminfo.app[u].start  = start[u];
        space->select.sel_info.hslab->diminfo.app[u].stride = app_stride[u];
        space->select.sel_info.hslab->diminfo.app[u].count  = app_count[u];
        space->select.sel_info.hslab->diminfo.app[u].block  = app_block[u];

        space->select.sel_info.hslab->diminfo.opt[u].start  = start[u];
        space->select.sel_info.hslab->diminfo.opt[u].stride = opt_stride[u];
        space->select.sel_info.hslab->diminfo.opt[u].count  = opt_count[u];
        space->select.sel_info.hslab->diminfo.opt[u].block  = opt_block[u];

        /* Update # of elements selected */
        space->select.num_elem *= (opt_count[u] * opt_block[u]);

        /* Set low bound of bounding box for the hyperslab selection */
        space->select.sel_info.hslab->diminfo.low_bounds[u] = start[u];

        /* Check for unlimited dimension & set high bound */
        if ((app_count[u] == H5S_UNLIMITED) || (app_block[u] == H5S_UNLIMITED)) {
            space->select.sel_info.hslab->unlim_dim              = (int)u;
            space->select.sel_info.hslab->diminfo.high_bounds[u] = H5S_UNLIMITED;
        } /* end if */
        else
            space->select.sel_info.hslab->diminfo.high_bounds[u] =
                start[u] + opt_stride[u] * (opt_count[u] - 1) + (opt_block[u] - 1);
    } /* end for */

    /* Handle unlimited selections */
    if (space->select.sel_info.hslab->unlim_dim >= 0) {
        /* Calculate num_elem_non_unlim */
        space->select.sel_info.hslab->num_elem_non_unlim = (hsize_t)1;
        for (u = 0; u < space->extent.rank; u++)
            if ((int)u != space->select.sel_info.hslab->unlim_dim)
                space->select.sel_info.hslab->num_elem_non_unlim *= (opt_count[u] * opt_block[u]);

        /* Update num_elem */
        space->select.num_elem = H5S_UNLIMITED;
    } /* end if */

    /* Indicate that the dimension information is valid */
    space->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_YES;

    /* Indicate that there's no slab information */
    space->select.sel_info.hslab->span_lst = NULL;

    /* Set selection type */
    space->select.type = H5S_sel_hyper;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__set_regular_hyperslab() */

/*-------------------------------------------------------------------------
 * Function:    H5S__hyper_regular_and_single_block
 *
 * Purpose:    Optimized routine to perform "AND" operation of a single
 *        block against a regular hyperslab selection.
 *
 * Note:    This algorithm is invoked when constructing the chunk map
 *              and a regular hyperslab is selected in the file's dataspace.
 *
 * Return:    Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__hyper_regular_and_single_block(H5S_t *space, const hsize_t start[], const hsize_t block[])
{
    hsize_t  select_end, block_end; /* End of block & selection */
    bool     single_block;          /* Whether the selection is a single block */
    bool     overlap;               /* Whether block & selection overlap */
    unsigned u;                     /* Local index variable */
    herr_t   ret_value = SUCCEED;   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);
    assert(start);
    assert(block);

    /* Check for single block selection in dataspace */
    single_block = true;
    for (u = 0; u < space->extent.rank; u++)
        if (1 != space->select.sel_info.hslab->diminfo.opt[u].count) {
            single_block = false;
            break;
        } /* end if */

    /* Perform different optimizations, based on type of regular selection */
    if (single_block) {
        hsize_t new_start[H5S_MAX_RANK]; /* New starting coordinate */
        hsize_t new_block[H5S_MAX_RANK]; /* New block size */

        /* Check for overlap and compute new start offset & block sizes */
        overlap = true;
        for (u = 0; u < space->extent.rank; u++) {
            /* Compute the end of the selection & block in this dimension */
            select_end = space->select.sel_info.hslab->diminfo.high_bounds[u];
            block_end  = (start[u] + block[u]) - 1;

            /* Check for overlap */
            if (!H5S_RANGE_OVERLAP(space->select.sel_info.hslab->diminfo.opt[u].start, select_end, start[u],
                                   block_end)) {
                overlap = false;
                break;
            } /* end if */

            /* Set new start & block size in this dimension */
            new_start[u] = MAX(space->select.sel_info.hslab->diminfo.opt[u].start, start[u]);
            new_block[u] = (MIN(select_end, block_end) - new_start[u]) + 1;
        } /* end for */

        /* Check for overlap of selection & block */
        if (overlap) {
            /* Set selection to regular hyperslab */
            if (H5S__set_regular_hyperslab(space, new_start, H5S_hyper_ones_g, H5S_hyper_ones_g, new_block,
                                           H5S_hyper_ones_g, H5S_hyper_ones_g, new_block) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't set regular hyperslab selection");
        } /* end if */
        else
            /* Selection & block don't overlap, set to "none" selection */
            if (H5S_select_none(space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
    } /* end if */
    else {
        hsize_t new_start[H5S_MAX_RANK]; /* New start for hyperslab selection */
        hsize_t new_count[H5S_MAX_RANK]; /* New count for hyperslab selection */
        hsize_t stride[H5S_MAX_RANK];    /* Stride for hyperslab selection */
        hsize_t new_block[H5S_MAX_RANK]; /* New block for hyperslab selection */
        bool    partial_first_span;      /* Whether first span in intersection is partial */
        bool    partial_last_span;       /* Whether last span in intersection is partial */

        /* Iterate over selection, checking for overlap and computing first / last
         *      span that intersects with the block.
         */
        overlap            = true;
        partial_first_span = false;
        partial_last_span  = false;
        for (u = 0; u < space->extent.rank; u++) {
            hsize_t first_span_start, first_span_end; /* Start / end of first span */
            hsize_t last_span_start, last_span_end;   /* Start / end of last span */
            hsize_t nstride;                          /* Number of strides into the selection */

            /* Compute the end of the selection & block in this dimension */
            select_end = space->select.sel_info.hslab->diminfo.high_bounds[u];
            block_end  = (start[u] + block[u]) - 1;

            /* Check for overlap */
            if (!H5S_RANGE_OVERLAP(space->select.sel_info.hslab->diminfo.opt[u].start, select_end, start[u],
                                   block_end)) {
                overlap = false;
                break;
            } /* end if */

            /* Find first span that is before or overlaps with start of block */
            if (space->select.sel_info.hslab->diminfo.opt[u].start >= start[u]) {
                /* Calculate start & end of first span */
                first_span_start = space->select.sel_info.hslab->diminfo.opt[u].start;
                first_span_end = (first_span_start + space->select.sel_info.hslab->diminfo.opt[u].block) - 1;

                /* Check if first span overlaps _end_ of block */
                if (block_end >= first_span_start && block_end <= first_span_end)
                    partial_first_span = true;
            } /* end if */
            else {
                hsize_t adj_start; /* Start coord, adjusted for hyperslab selection parameters */

                /* Adjust start coord for selection's 'start' offset */
                adj_start = start[u] - space->select.sel_info.hslab->diminfo.opt[u].start;

                /* Compute # of strides into the selection */
                if (space->select.sel_info.hslab->diminfo.opt[u].count > 1)
                    nstride = adj_start / space->select.sel_info.hslab->diminfo.opt[u].stride;
                else
                    nstride = 0;

                /* Calculate start & end of first span */
                first_span_start = space->select.sel_info.hslab->diminfo.opt[u].start +
                                   (nstride * space->select.sel_info.hslab->diminfo.opt[u].stride);
                first_span_end = (first_span_start + space->select.sel_info.hslab->diminfo.opt[u].block) - 1;

                /* Check if first span overlaps start of block */
                if (first_span_start < start[u] && first_span_end >= start[u])
                    partial_first_span = true;

                /* Advance first span to start higher than block's start,
                 *      if it's not partial.
                 */
                if (first_span_end < start[u]) {
                    first_span_start += space->select.sel_info.hslab->diminfo.opt[u].stride;
                    first_span_end += space->select.sel_info.hslab->diminfo.opt[u].stride;
                } /* end if */
            }     /* end else */

            /* Find last span that is before or overlaps with end of block */
            if (select_end < block_end) {
                /* Calculate start & end of last span */
                last_span_start = (select_end - space->select.sel_info.hslab->diminfo.opt[u].block) + 1;
                last_span_end   = select_end;

                /* Check if last span overlaps _start_ of block */
                if (start[u] >= last_span_start && start[u] <= last_span_end)
                    partial_last_span = true;
            } /* end if */
            else {
                hsize_t adj_end; /* End coord, adjusted for hyperslab selection parameters */

                /* Adjust end coord for selection's 'start' offset */
                adj_end = block_end - space->select.sel_info.hslab->diminfo.opt[u].start;

                /* Compute # of strides into the selection */
                if (space->select.sel_info.hslab->diminfo.opt[u].count > 1)
                    nstride = adj_end / space->select.sel_info.hslab->diminfo.opt[u].stride;
                else
                    nstride = 0;

                /* Calculate start & end of last span */
                last_span_start = space->select.sel_info.hslab->diminfo.opt[u].start +
                                  (nstride * space->select.sel_info.hslab->diminfo.opt[u].stride);
                last_span_end = (last_span_start + space->select.sel_info.hslab->diminfo.opt[u].block) - 1;

                /* Check if last span overlaps end of block */
                if (block_end >= last_span_start && block_end <= last_span_end)
                    partial_last_span = true;
            } /* end else */

            /* Check if no spans are inside block */
            /* (Can happen when block falls in "gap" between spans) */
            if (last_span_end < start[u]) {
                overlap = false;
                break;
            } /* end if */

            /* Sanity check */
            assert(first_span_start <= last_span_start);

            /* Compute new start / count / block values */
            new_start[u] = first_span_start;
            if (last_span_start != first_span_start)
                new_count[u] = ((last_span_start - first_span_start) /
                                space->select.sel_info.hslab->diminfo.opt[u].stride) +
                               1;
            else
                new_count[u] = 1;
            new_block[u] = space->select.sel_info.hslab->diminfo.opt[u].block;

            /* Keep same stride */
            stride[u] = space->select.sel_info.hslab->diminfo.opt[u].stride;
        } /* end for */

        /* Check for overlap of selection & block */
        if (overlap) {
            /* Set selection to regular hyperslab */
            if (H5S__set_regular_hyperslab(space, new_start, stride, new_count, new_block, stride, new_count,
                                           new_block) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't set regular hyperslab selection");

            /* If there's a partial first or last span, have to 'AND' against selection */
            if (partial_first_span || partial_last_span) {
                /* Generate span tree for regular selection */
                if (H5S__hyper_generate_spans(space) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_UNINITIALIZED, FAIL, "dataspace does not have span tree");

                /* 'AND' against block */
                if (H5S__generate_hyperslab(space, H5S_SELECT_AND, start, H5S_hyper_ones_g, H5S_hyper_ones_g,
                                            block) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't generate hyperslabs");
            } /* end if */
        }     /* end if */
        else {
            /* Selection & block don't overlap, set to "none" selection */
            if (H5S_select_none(space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
        } /* end else */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_regular_and_single_block() */

/*-------------------------------------------------------------------------
 * Function:    H5S_select_hyperslab
 *
 * Purpose:     Internal version of H5Sselect_hyperslab().
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5S_select_hyperslab(H5S_t *space, H5S_seloper_t op, const hsize_t start[], const hsize_t *stride,
                     const hsize_t count[], const hsize_t *block)
{
    hsize_t        int_stride[H5S_MAX_RANK]; /* Internal storage for stride information */
    hsize_t        int_count[H5S_MAX_RANK];  /* Internal storage for count information */
    hsize_t        int_block[H5S_MAX_RANK];  /* Internal storage for block information */
    const hsize_t *opt_stride;               /* Optimized stride information */
    const hsize_t *opt_count;                /* Optimized count information */
    const hsize_t *opt_block;                /* Optimized block information */
    int            unlim_dim = -1;           /* Unlimited dimension in selection, of -1 if none */
    unsigned       u;                        /* Local index variable */
    herr_t         ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(space);
    assert(start);
    assert(count);
    assert(op > H5S_SELECT_NOOP && op < H5S_SELECT_INVALID);

    /* Point to the correct stride values */
    if (stride == NULL)
        stride = H5S_hyper_ones_g;

    /* Point to the correct block values */
    if (block == NULL)
        block = H5S_hyper_ones_g;

    /* Check new selection */
    for (u = 0; u < space->extent.rank; u++) {
        /* Check for overlapping hyperslab blocks in new selection. */
        if (count[u] > 1 && stride[u] < block[u])
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "hyperslab blocks overlap");

        /* Detect zero-sized hyperslabs in new selection */
        if (count[u] == 0 || block[u] == 0) {
            switch (op) {
                case H5S_SELECT_SET:  /* Select "set" operation */
                case H5S_SELECT_AND:  /* Binary "and" operation for hyperslabs */
                case H5S_SELECT_NOTA: /* Binary "B not A" operation for hyperslabs */
                    /* Convert to "none" selection */
                    if (H5S_select_none(space) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
                    HGOTO_DONE(SUCCEED);

                case H5S_SELECT_OR:      /* Binary "or" operation for hyperslabs */
                case H5S_SELECT_XOR:     /* Binary "xor" operation for hyperslabs */
                case H5S_SELECT_NOTB:    /* Binary "A not B" operation for hyperslabs */
                    HGOTO_DONE(SUCCEED); /* Selection stays same */

                case H5S_SELECT_NOOP:
                case H5S_SELECT_APPEND:
                case H5S_SELECT_PREPEND:
                case H5S_SELECT_INVALID:
                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
            } /* end switch */
        }     /* end if */

        /* Check for unlimited dimension */
        if ((count[u] == H5S_UNLIMITED) || (block[u] == H5S_UNLIMITED)) {
            if (unlim_dim >= 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL,
                            "cannot have more than one unlimited dimension in selection");
            else {
                if (count[u] == block[u]) /* Both are H5S_UNLIMITED */
                    HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL,
                                "count and block cannot both be unlimited");
                unlim_dim = (int)u;
            } /* end else */
        }     /* end if */
    }         /* end for */

    /* Optimize hyperslab parameters to merge contiguous blocks, etc. */
    if (stride == H5S_hyper_ones_g && block == H5S_hyper_ones_g) {
        /* Point to existing arrays */
        opt_stride = H5S_hyper_ones_g;
        opt_count  = H5S_hyper_ones_g;
        opt_block  = count;
    } /* end if */
    else {
        /* Point to local arrays */
        opt_stride = int_stride;
        opt_count  = int_count;
        opt_block  = int_block;
        for (u = 0; u < space->extent.rank; u++) {
            /* contiguous hyperslabs have the block size equal to the stride */
            if ((stride[u] == block[u]) && (count[u] != H5S_UNLIMITED)) {
                int_count[u]  = 1;
                int_stride[u] = 1;
                if (block[u] == 1)
                    int_block[u] = count[u];
                else
                    int_block[u] = block[u] * count[u];
            } /* end if */
            else {
                if (count[u] == 1)
                    int_stride[u] = 1;
                else {
                    assert((stride[u] > block[u]) ||
                           ((stride[u] == block[u]) && (count[u] == H5S_UNLIMITED)));
                    int_stride[u] = stride[u];
                } /* end else */
                int_count[u] = count[u];
                int_block[u] = block[u];
            } /* end else */
        }     /* end for */
    }         /* end else */

    /* Check for operating on unlimited selection */
    if ((H5S_GET_SELECT_TYPE(space) == H5S_SEL_HYPERSLABS) &&
        (space->select.sel_info.hslab->unlim_dim >= 0) && (op != H5S_SELECT_SET)) {
        /* Check for invalid operation */
        if (unlim_dim >= 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL,
                        "cannot modify unlimited selection with another unlimited selection");
        if (!((op == H5S_SELECT_AND) || (op == H5S_SELECT_NOTA)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unsupported operation on unlimited selection");
        assert(space->select.sel_info.hslab->diminfo_valid);

        /* Clip unlimited selection to include new selection */
        if (H5S_hyper_clip_unlim(space,
                                 start[space->select.sel_info.hslab->unlim_dim] +
                                     ((opt_count[space->select.sel_info.hslab->unlim_dim] - (hsize_t)1) *
                                      opt_stride[space->select.sel_info.hslab->unlim_dim]) +
                                     opt_block[space->select.sel_info.hslab->unlim_dim]) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, FAIL, "failed to clip unlimited selection");

        /* If an empty space was returned it must be "none" */
        assert((space->select.num_elem > (hsize_t)0) || (space->select.type->type == H5S_SEL_NONE));
    } /* end if */

    /* Fixup operation for non-hyperslab selections */
    switch (H5S_GET_SELECT_TYPE(space)) {
        case H5S_SEL_NONE: /* No elements selected in dataspace */
            switch (op) {
                case H5S_SELECT_SET: /* Select "set" operation */
                    /* Change "none" selection to hyperslab selection */
                    break;

                case H5S_SELECT_OR:      /* Binary "or" operation for hyperslabs */
                case H5S_SELECT_XOR:     /* Binary "xor" operation for hyperslabs */
                case H5S_SELECT_NOTA:    /* Binary "B not A" operation for hyperslabs */
                    op = H5S_SELECT_SET; /* Maps to "set" operation when applied to "none" selection */
                    break;

                case H5S_SELECT_AND:     /* Binary "and" operation for hyperslabs */
                case H5S_SELECT_NOTB:    /* Binary "A not B" operation for hyperslabs */
                    HGOTO_DONE(SUCCEED); /* Selection stays "none" */

                case H5S_SELECT_NOOP:
                case H5S_SELECT_APPEND:
                case H5S_SELECT_PREPEND:
                case H5S_SELECT_INVALID:
                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
            } /* end switch */
            break;

        case H5S_SEL_ALL: /* All elements selected in dataspace */
            switch (op) {
                case H5S_SELECT_SET: /* Select "set" operation */
                    /* Change "all" selection to hyperslab selection */
                    break;

                case H5S_SELECT_OR:      /* Binary "or" operation for hyperslabs */
                    HGOTO_DONE(SUCCEED); /* Selection stays "all" */

                case H5S_SELECT_AND:     /* Binary "and" operation for hyperslabs */
                    op = H5S_SELECT_SET; /* Maps to "set" operation when applied to "none" selection */
                    break;

                case H5S_SELECT_XOR:  /* Binary "xor" operation for hyperslabs */
                case H5S_SELECT_NOTB: /* Binary "A not B" operation for hyperslabs */
                    /* Convert current "all" selection to "real" hyperslab selection */
                    /* Then allow operation to proceed */
                    {
                        const hsize_t *tmp_start;  /* Temporary start information */
                        const hsize_t *tmp_stride; /* Temporary stride information */
                        const hsize_t *tmp_count;  /* Temporary count information */
                        const hsize_t *tmp_block;  /* Temporary block information */

                        /* Set up temporary information for the dimensions */
                        tmp_start  = H5S_hyper_zeros_g;
                        tmp_stride = tmp_count = H5S_hyper_ones_g;
                        tmp_block              = space->extent.size;

                        /* Convert to hyperslab selection */
                        if (H5S_select_hyperslab(space, H5S_SELECT_SET, tmp_start, tmp_stride, tmp_count,
                                                 tmp_block) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't convert selection");
                    } /* end case */
                    break;

                case H5S_SELECT_NOTA: /* Binary "B not A" operation for hyperslabs */
                    /* Convert to "none" selection */
                    if (H5S_select_none(space) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
                    HGOTO_DONE(SUCCEED);

                case H5S_SELECT_NOOP:
                case H5S_SELECT_APPEND:
                case H5S_SELECT_PREPEND:
                case H5S_SELECT_INVALID:
                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
            } /* end switch */
            break;

        case H5S_SEL_HYPERSLABS:
            /* Hyperslab operation on hyperslab selection, OK */
            break;

        case H5S_SEL_POINTS:          /* Can't combine hyperslab operations and point selections currently */
            if (op == H5S_SELECT_SET) /* Allow only "set" operation to proceed */
                break;
            /* FALLTHROUGH (to error) */
            H5_ATTR_FALLTHROUGH

        case H5S_SEL_ERROR:
        case H5S_SEL_N:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
    } /* end switch */

    if (op == H5S_SELECT_SET) {
        /* Set selection to regular hyperslab */
        if (H5S__set_regular_hyperslab(space, start, stride, count, block, opt_stride, opt_count, opt_block) <
            0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't set regular hyperslab selection");
    } /* end if */
    else if (op >= H5S_SELECT_OR && op <= H5S_SELECT_NOTA) {
        bool single_block; /* Whether the selection is a single block */

        /* Sanity check */
        assert(H5S_GET_SELECT_TYPE(space) == H5S_SEL_HYPERSLABS);

        /* Handle unlimited selections */
        if (unlim_dim >= 0) {
            hsize_t bounds_start[H5S_MAX_RANK];
            hsize_t bounds_end[H5S_MAX_RANK];
            hsize_t tmp_count = opt_count[unlim_dim];
            hsize_t tmp_block = opt_block[unlim_dim];

            /* Check for invalid operation */
            if (space->select.sel_info.hslab->unlim_dim >= 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL,
                            "cannot modify unlimited selection with another unlimited selection");
            if (!((op == H5S_SELECT_AND) || (op == H5S_SELECT_NOTB)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL,
                            "unsupported operation with unlimited selection");

            /* Get bounds of existing selection */
            if (H5S__hyper_bounds(space, bounds_start, bounds_end) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get selection bounds");

            /* Patch count and block to remove unlimited and include the
             * existing selection.
             */
            H5S__hyper_get_clip_diminfo(start[unlim_dim], opt_stride[unlim_dim], &tmp_count, &tmp_block,
                                        bounds_end[unlim_dim] + (hsize_t)1);
            assert((tmp_count == 1) || (opt_count != H5S_hyper_ones_g));
            assert((tmp_block == 1) || (opt_block != H5S_hyper_ones_g));
            if (opt_count != H5S_hyper_ones_g) {
                assert(opt_count == int_count);
                int_count[unlim_dim] = tmp_count;
            } /* end if */
            if (opt_block != H5S_hyper_ones_g) {
                assert(opt_block == int_block);
                int_block[unlim_dim] = tmp_block;
            } /* end if */
        }     /* end if */

        /* Check for a single block selected */
        single_block = true;
        for (u = 0; u < space->extent.rank; u++)
            if (1 != opt_count[u]) {
                single_block = false;
                break;
            } /* end if */

        /* Check for single block "AND" operation on a regular hyperslab, which
         *      is used for constructing chunk maps and can be optimized for.
         */
        if (H5S_SELECT_AND == op && single_block &&
            space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
            if (H5S__hyper_regular_and_single_block(space, start, opt_block) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTOPERATE, FAIL,
                            "can't 'AND' single block against regular hyperslab");
        } /* end if */
        else {
            /* Check if there's no hyperslab span information currently */
            if (NULL == space->select.sel_info.hslab->span_lst)
                if (H5S__hyper_generate_spans(space) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_UNINITIALIZED, FAIL, "dataspace does not have span tree");

            /* Set selection type */
            space->select.type = H5S_sel_hyper;

            /* Add in the new hyperslab information */
            if (H5S__generate_hyperslab(space, op, start, opt_stride, opt_count, opt_block) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't generate hyperslabs");
        } /* end else */
    }     /* end if */
    else
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_hyperslab() */

/*--------------------------------------------------------------------------
 NAME
    H5Sselect_hyperslab
 PURPOSE
    Specify a hyperslab to combine with the current hyperslab selection
 USAGE
    herr_t H5Sselect_hyperslab(dsid, op, start, stride, count, block)
        hid_t dsid;             IN: Dataspace ID of selection to modify
        H5S_seloper_t op;       IN: Operation to perform on current selection
        const hsize_t *start;        IN: Offset of start of hyperslab
        const hsize_t *stride;       IN: Hyperslab stride
        const hsize_t *count;        IN: Number of blocks included in hyperslab
        const hsize_t *block;        IN: Size of block in hyperslab
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Combines a hyperslab selection with the current selection for a dataspace.
    If the current selection is not a hyperslab, it is freed and the hyperslab
    parameters passed in are combined with the H5S_SEL_ALL hyperslab (ie. a
    selection composing the entire current extent).  If STRIDE or BLOCK is
    NULL, they are assumed to be set to all '1'.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Sselect_hyperslab(hid_t space_id, H5S_seloper_t op, const hsize_t start[], const hsize_t stride[],
                    const hsize_t count[], const hsize_t block[])
{
    H5S_t *space;               /* Dataspace to modify selection of */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iSs*h*h*h*h", space_id, op, start, stride, count, block);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (H5S_SCALAR == H5S_GET_EXTENT_TYPE(space))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "hyperslab doesn't support H5S_SCALAR space");
    if (H5S_NULL == H5S_GET_EXTENT_TYPE(space))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "hyperslab doesn't support H5S_NULL space");
    if (start == NULL || count == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "hyperslab not specified");
    if (!(op > H5S_SELECT_NOOP && op < H5S_SELECT_INVALID))
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
    if (stride != NULL) {
        unsigned u; /* Local index variable */

        /* Check for 0-sized strides */
        for (u = 0; u < space->extent.rank; u++)
            if (stride[u] == 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid stride==0 value");
    } /* end if */

    if (H5S_select_hyperslab(space, op, start, stride, count, block) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to set hyperslab selection");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sselect_hyperslab() */

/*--------------------------------------------------------------------------
 NAME
    H5S_combine_hyperslab
 PURPOSE
    Specify a hyperslab to combine with the current hyperslab selection, and
    store the result in the new hyperslab selection.
 USAGE
    herr_t H5S_combine_hyperslab(old_space, op, start, stride, count, block, new_space)
        H5S_t *old_space;            IN: The old space the selection is performed on
        H5S_seloper_t op;            IN: Operation to perform on current selection
        const hsize_t start[];       IN: Offset of start of hyperslab
        const hsize_t *stride;       IN: Hyperslab stride
        const hsize_t count[];       IN: Number of blocks included in hyperslab
        const hsize_t *block;        IN: Size of block in hyperslab
        H5S_t **new_space;           OUT: The new dataspace to store the selection result
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Combines a hyperslab selection with the current selection for a dataspace.
    If STRIDE or BLOCK is NULL, they are assumed to be set to all '1'.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    In some cases, copying the whole span tree from old_space to new_space
    can be avoided.  Deal with such cases directly, otherwise this function
    is equivalent to:
        1. Copy the whole span tree from old_space into new_space
        2. Call H5S_select_hyperslab with the new_space.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_combine_hyperslab(const H5S_t *old_space, H5S_seloper_t op, const hsize_t start[], const hsize_t *stride,
                      const hsize_t count[], const hsize_t *block, H5S_t **new_space)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(old_space);
    assert(start);
    assert(count);
    assert(op >= H5S_SELECT_SET && op <= H5S_SELECT_NOTA);
    assert(new_space);
    assert(*new_space == NULL);

    /* Point to the correct stride values */
    if (stride == NULL)
        stride = H5S_hyper_ones_g;

    /* Point to the correct block values */
    if (block == NULL)
        block = H5S_hyper_ones_g;

    /* Check new selection. */
    for (u = 0; u < old_space->extent.rank; u++) {
        /* Check for overlapping hyperslab blocks in new selection. */
        if (count[u] > 1 && stride[u] < block[u])
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "hyperslab blocks overlap");

        /* Detect zero-sized hyperslabs in new selection */
        if (count[u] == 0 || block[u] == 0) {
            switch (op) {
                case H5S_SELECT_AND:  /* Binary "and" operation for hyperslabs */
                case H5S_SELECT_NOTA: /* Binary "B not A" operation for hyperslabs */
                    /* Convert to "none" selection */
                    /* Copy the first dataspace without sharing the list of spans */
                    if (NULL == ((*new_space) = H5S_copy(old_space, true, true)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to copy dataspace");
                    if (H5S_select_none((*new_space)) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
                    HGOTO_DONE(SUCCEED);

                case H5S_SELECT_OR:   /* Binary "or" operation for hyperslabs */
                case H5S_SELECT_XOR:  /* Binary "xor" operation for hyperslabs */
                case H5S_SELECT_NOTB: /* Binary "A not B" operation for hyperslabs */
                    /* Copy the first dataspace with sharing the list of spans */
                    if (NULL == ((*new_space) = H5S_copy(old_space, false, true)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to copy dataspace");
                    HGOTO_DONE(SUCCEED); /* Selection stays same */

                case H5S_SELECT_NOOP:
                case H5S_SELECT_SET:
                case H5S_SELECT_APPEND:
                case H5S_SELECT_PREPEND:
                case H5S_SELECT_INVALID:
                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
            } /* end switch */
        }     /* end if */
    }         /* end for */

    if (H5S_GET_SELECT_TYPE(old_space) == H5S_SEL_HYPERSLABS) {
        hsize_t *old_low_bounds; /* Pointer to old space's low & high bounds */
        hsize_t *old_high_bounds;
        hsize_t  new_low_bounds[H5S_MAX_RANK]; /* New space's low & high bounds */
        hsize_t  new_high_bounds[H5S_MAX_RANK];
        bool     overlapped = false;

        /* Set up old space's low & high bounds */
        if (old_space->select.sel_info.hslab->span_lst) {
            old_low_bounds  = old_space->select.sel_info.hslab->span_lst->low_bounds;
            old_high_bounds = old_space->select.sel_info.hslab->span_lst->high_bounds;
        } /* end if */
        else {
            old_low_bounds  = old_space->select.sel_info.hslab->diminfo.low_bounds;
            old_high_bounds = old_space->select.sel_info.hslab->diminfo.high_bounds;
        } /* end else */

        /* Generate bounding box for hyperslab parameters */
        for (u = 0; u < old_space->extent.rank; u++) {
            new_low_bounds[u]  = start[u];
            new_high_bounds[u] = start[u] + stride[u] * (count[u] - 1) + (block[u] - 1);
        } /* end for */

        /* Check bound box of both spaces to see if they overlap */
        if (H5S_RANGE_OVERLAP(old_low_bounds[0], old_high_bounds[0], new_low_bounds[0], new_high_bounds[0]))
            overlapped = true;

        /* Non-overlapping situations can be handled in special ways */
        if (!overlapped) {
            H5S_hyper_span_info_t *new_spans = NULL;
            H5S_hyper_dim_t        new_hyper_diminfo[H5S_MAX_RANK];

            if (NULL == ((*new_space) = H5S_copy(old_space, true, true)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy dataspace");
            if (NULL != (*new_space)->select.sel_info.hslab->span_lst) {
                old_space->select.sel_info.hslab->span_lst->count--;
                (*new_space)->select.sel_info.hslab->span_lst = NULL;
            } /* end if */

            /* Generate hyperslab info for new space */
            switch (op) {
                case H5S_SELECT_OR:
                case H5S_SELECT_XOR:
                    /* Add the new space to the space */
                    if (NULL == (new_spans = H5S__hyper_make_spans(old_space->extent.rank, start, stride,
                                                                   count, block)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL,
                                    "can't create hyperslab information");
                    if (NULL != old_space->select.sel_info.hslab->span_lst)
                        (*new_space)->select.sel_info.hslab->span_lst = H5S__hyper_copy_span(
                            old_space->select.sel_info.hslab->span_lst, old_space->extent.rank);
                    if (H5S__hyper_add_disjoint_spans(*new_space, new_spans) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't append hyperslabs");

                    /* Build diminfo struct */
                    for (u = 0; u < (*new_space)->extent.rank; u++) {
                        new_hyper_diminfo[u].start  = start[u];
                        new_hyper_diminfo[u].stride = stride[u];
                        new_hyper_diminfo[u].count  = count[u];
                        new_hyper_diminfo[u].block  = block[u];
                    } /* end for */

                    /* Update space's dim info */
                    if (H5S__hyper_update_diminfo(*new_space, op, new_hyper_diminfo) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOUNT, FAIL, "can't update hyperslab info");
                    break;

                case H5S_SELECT_AND:
                    if (H5S_select_none((*new_space)) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
                    break;

                case H5S_SELECT_NOTB:
                    if (NULL != old_space->select.sel_info.hslab->span_lst) {
                        if (NULL == ((*new_space)->select.sel_info.hslab->span_lst = H5S__hyper_copy_span(
                                         old_space->select.sel_info.hslab->span_lst, old_space->extent.rank)))
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy dataspace");
                    } /* end if */
                    else {
                        if (H5S_select_none((*new_space)) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
                    } /* end else */
                    break;

                case H5S_SELECT_NOTA:
                    if (H5S__set_regular_hyperslab(*new_space, start, stride, count, block, stride, count,
                                                   block) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't set regular selection");
                    break;

                case H5S_SELECT_NOOP:
                case H5S_SELECT_SET:
                case H5S_SELECT_APPEND:
                case H5S_SELECT_PREPEND:
                case H5S_SELECT_INVALID:
                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");
            } /* end switch */

            HGOTO_DONE(SUCCEED);
        } /* end if(!overlapped) */
    }     /* end if the selection of old space is H5S_SEL_HYPERSLABS */

    /* Copy the first dataspace with sharing the list of spans */
    if (NULL == ((*new_space) = H5S_copy(old_space, true, true)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to copy dataspace");

    /* Note: a little overhead in calling the function as some conditions are checked again */
    if (H5S_select_hyperslab(*new_space, op, start, stride, count, block) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to set hyperslab selection");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_combine_hyperslab() */

/*-------------------------------------------------------------------------
 * Function:    H5S__fill_in_select
 *
 * Purpose:    Combines two hyperslabs with an operation, putting the
 *              result into a third hyperslab selection
 *
 * Return:    Non-negative on success/negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__fill_in_select(H5S_t *space1, H5S_seloper_t op, H5S_t *space2, H5S_t **result)
{
    bool   span2_owned;
    bool   updated_spans;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(space1);
    assert(space2);
    assert(op >= H5S_SELECT_OR && op <= H5S_SELECT_NOTA);
    assert(space1->extent.rank == space2->extent.rank);
    /* The result is either a to-be-created space or an empty one */
    assert(NULL == *result || *result == space1);
    assert(space1->select.sel_info.hslab->span_lst);
    assert(space2->select.sel_info.hslab->span_lst);

    /* Note: the offset of space2 is not considered here for bounding box */
    if (H5S__fill_in_new_space(space1, op, space2->select.sel_info.hslab->span_lst, false, &span2_owned,
                               &updated_spans, result) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't create the specified selection");

    /* Update diminfo if space2's diminfo was valid, otherwise just mark it as
     * invalid if the spans were updated */
    assert(result);
    if (updated_spans) {
        if (space2->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
            if (H5S__hyper_update_diminfo(*result, op, space2->select.sel_info.hslab->diminfo.opt) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOUNT, FAIL, "can't update hyperslab info");
        } /* end if */
        else
            (*result)->select.sel_info.hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__fill_in_select() */

/*--------------------------------------------------------------------------
 NAME
    H5Scombine_hyperslab
 PURPOSE
    Specify a hyperslab to combine with the current hyperslab selection and
    return a new dataspace with the combined selection as the selection in the
    new dataspace.
 USAGE
    hid_t H5Scombine_hyperslab(dsid, op, start, stride, count, block)
        hid_t dsid;             IN: Dataspace ID of selection to use
        H5S_seloper_t op;       IN: Operation to perform on current selection
        const hsize_t *start;        IN: Offset of start of hyperslab
        const hsize_t *stride;       IN: Hyperslab stride
        const hsize_t *count;        IN: Number of blocks included in hyperslab
        const hsize_t *block;        IN: Size of block in hyperslab
 RETURNS
    Dataspace ID on success / H5I_INVALID_HID on failure
 DESCRIPTION
    Combines a hyperslab selection with the current selection for a dataspace,
    creating a new dataspace to return the generated selection.
    If the current selection is not a hyperslab, it is freed and the hyperslab
    parameters passed in are combined with the H5S_SEL_ALL hyperslab (ie. a
    selection composing the entire current extent).  If STRIDE or BLOCK is
    NULL, they are assumed to be set to all '1'.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hid_t
H5Scombine_hyperslab(hid_t space_id, H5S_seloper_t op, const hsize_t start[], const hsize_t stride[],
                     const hsize_t count[], const hsize_t block[])
{
    H5S_t *space;            /* Dataspace to modify selection of */
    H5S_t *new_space = NULL; /* New dataspace created */
    hid_t  ret_value;        /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE6("i", "iSs*h*h*h*h", space_id, op, start, stride, count, block);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a dataspace");
    if (start == NULL || count == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "hyperslab not specified");
    if (!(op >= H5S_SELECT_SET && op <= H5S_SELECT_NOTA))
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, H5I_INVALID_HID, "invalid selection operation");

    /* Generate new space, with combination of selections */
    if (H5S_combine_hyperslab(space, op, start, stride, count, block, &new_space) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, H5I_INVALID_HID, "unable to set hyperslab selection");

    /* Register */
    if ((ret_value = H5I_register(H5I_DATASPACE, new_space, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register dataspace ID");

done:
    if (ret_value < 0 && new_space)
        H5S_close(new_space);

    FUNC_LEAVE_API(ret_value)
} /* end H5Scombine_hyperslab() */

/*-------------------------------------------------------------------------
 * Function:    H5S__combine_select
 *
 * Purpose:     Internal version of H5Scombine_select().
 *
 * Return:      New dataspace on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
static H5S_t *
H5S__combine_select(H5S_t *space1, H5S_seloper_t op, H5S_t *space2)
{
    H5S_t *new_space = NULL; /* New dataspace generated */
    H5S_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space1);
    assert(space2);
    assert(op >= H5S_SELECT_OR && op <= H5S_SELECT_NOTA);

    /* Check if space1 selections has span trees */
    if (NULL == space1->select.sel_info.hslab->span_lst)
        if (H5S__hyper_generate_spans(space1) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNINITIALIZED, NULL, "dataspace does not have span tree");

    if (NULL == space2->select.sel_info.hslab->span_lst) {
        hsize_t  tmp_start[H5S_MAX_RANK];
        hsize_t  tmp_stride[H5S_MAX_RANK];
        hsize_t  tmp_count[H5S_MAX_RANK];
        hsize_t  tmp_block[H5S_MAX_RANK];
        unsigned u;

        for (u = 0; u < space2->extent.rank; u++) {
            tmp_start[u]  = space2->select.sel_info.hslab->diminfo.opt[u].start;
            tmp_stride[u] = space2->select.sel_info.hslab->diminfo.opt[u].stride;
            tmp_count[u]  = space2->select.sel_info.hslab->diminfo.opt[u].count;
            tmp_block[u]  = space2->select.sel_info.hslab->diminfo.opt[u].block;
        } /* end for */

        /* Combine hyperslab selection with regular selection directly */
        if (H5S_combine_hyperslab(space1, op, tmp_start, tmp_stride, tmp_count, tmp_block, &new_space) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, NULL, "unable to set hyperslab selection");
    } /* end if */
    else {
        /* Combine new_space (a copy of space 1) & space2, with the result in new_space */
        if (H5S__fill_in_select(space1, op, space2, &new_space) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, NULL, "can't clip hyperslab information");
    } /* end else */

    /* Set unlim_dim */
    new_space->select.sel_info.hslab->unlim_dim = -1;

    /* Set return value */
    ret_value = new_space;

done:
    if (ret_value == NULL && new_space)
        H5S_close(new_space);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__combine_select() */

/*--------------------------------------------------------------------------
 NAME
    H5Scombine_select
 PURPOSE
    Combine two hyperslab selections with an operation, returning a dataspace
    with the resulting selection.
 USAGE
    hid_t H5Scombine_select(space1, op, space2)
        hid_t space1;           IN: First Dataspace ID
        H5S_seloper_t op;       IN: Selection operation
        hid_t space2;           IN: Second Dataspace ID
 RETURNS
    Dataspace ID on success / H5I_INVALID_HID on failure
 DESCRIPTION
    Combine two existing hyperslab selections with an operation, returning
    a new dataspace with the resulting selection.  The dataspace extent from
    space1 is copied for the dataspace extent of the newly created dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hid_t
H5Scombine_select(hid_t space1_id, H5S_seloper_t op, hid_t space2_id)
{
    H5S_t *space1;           /* First Dataspace */
    H5S_t *space2;           /* Second Dataspace */
    H5S_t *new_space = NULL; /* New Dataspace */
    hid_t  ret_value;        /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "iSsi", space1_id, op, space2_id);

    /* Check args */
    if (NULL == (space1 = (H5S_t *)H5I_object_verify(space1_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a dataspace");
    if (NULL == (space2 = (H5S_t *)H5I_object_verify(space2_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a dataspace");
    if (!(op >= H5S_SELECT_OR && op <= H5S_SELECT_NOTA))
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, H5I_INVALID_HID, "invalid selection operation");

    /* Check that both dataspaces have the same rank */
    if (space1->extent.rank != space2->extent.rank)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "dataspaces not same rank");

        /* Note: currently, the offset of each dataspace is ignored */
#if 0
    /* Check that both dataspaces have the same offset */
    /* Same note as in H5Smodify_select */
    for(u=0; u<space1->extent.rank; u++) {
        if(space1->select.offset[u] != space2->select.offset[u])
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "dataspaces not same offset");
    } /* end for */
#endif

    /* Check that both dataspaces have hyperslab selections */
    if (H5S_GET_SELECT_TYPE(space1) != H5S_SEL_HYPERSLABS ||
        H5S_GET_SELECT_TYPE(space2) != H5S_SEL_HYPERSLABS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "dataspaces don't have hyperslab selections");

    /* Go combine the dataspaces */
    if (NULL == (new_space = H5S__combine_select(space1, op, space2)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, H5I_INVALID_HID, "unable to create hyperslab selection");

    /* Register */
    if ((ret_value = H5I_register(H5I_DATASPACE, new_space, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register dataspace ID");

done:
    if (ret_value < 0 && new_space)
        H5S_close(new_space);

    FUNC_LEAVE_API(ret_value)
} /* end H5Scombine_select() */

/*-------------------------------------------------------------------------
 * Function:    H5S__modify_select
 *
 * Purpose:     Internal version of H5Smodify_select().
 *
 * Return:      New dataspace on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5S__modify_select(H5S_t *space1, H5S_seloper_t op, H5S_t *space2)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space1);
    assert(space2);
    assert(op >= H5S_SELECT_OR && op <= H5S_SELECT_NOTA);

    /* Check that the space selections both have span trees */
    if (NULL == space1->select.sel_info.hslab->span_lst)
        if (H5S__hyper_generate_spans(space1) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNINITIALIZED, FAIL, "dataspace does not have span tree");

    /* Set unlim_dim */
    space1->select.sel_info.hslab->unlim_dim = -1;

    if (NULL == space2->select.sel_info.hslab->span_lst) {
        hsize_t  tmp_start[H5S_MAX_RANK];
        hsize_t  tmp_stride[H5S_MAX_RANK];
        hsize_t  tmp_count[H5S_MAX_RANK];
        hsize_t  tmp_block[H5S_MAX_RANK];
        unsigned u;

        for (u = 0; u < space2->extent.rank; u++) {
            tmp_start[u]  = space2->select.sel_info.hslab->diminfo.opt[u].start;
            tmp_stride[u] = space2->select.sel_info.hslab->diminfo.opt[u].stride;
            tmp_count[u]  = space2->select.sel_info.hslab->diminfo.opt[u].count;
            tmp_block[u]  = space2->select.sel_info.hslab->diminfo.opt[u].block;
        } /* end for */

        /* Call H5S_select_hyperslab directly */
        if (H5S_select_hyperslab(space1, op, tmp_start, tmp_stride, tmp_count, tmp_block) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to set hyperslab selection");
    } /* end if */
    else
        /* Combine spans from space1 & spans from space2, with the result in space1 */
        if (H5S__fill_in_select(space1, op, space2, &space1) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, FAIL, "can't perform operation on two selections");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__modify_select() */

/*--------------------------------------------------------------------------
 NAME
    H5Smodify_select
 PURPOSE
    Refine a hyperslab selection with an operation using a second hyperslab
    to modify it
 USAGE
    herr_t H5Smodify_select(space1, op, space2)
        hid_t space1;           IN/OUT: First Dataspace ID
        H5S_seloper_t op;       IN: Selection operation
        hid_t space2;           IN: Second Dataspace ID
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Refine an existing hyperslab selection with an operation, using a second
    hyperslab.  The first selection is modified to contain the result of
    space1 operated on by space2.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Smodify_select(hid_t space1_id, H5S_seloper_t op, hid_t space2_id)
{
    H5S_t *space1;              /* First Dataspace */
    H5S_t *space2;              /* Second Dataspace */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iSsi", space1_id, op, space2_id);

    /* Check args */
    if (NULL == (space1 = (H5S_t *)H5I_object_verify(space1_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (NULL == (space2 = (H5S_t *)H5I_object_verify(space2_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (!(op >= H5S_SELECT_OR && op <= H5S_SELECT_NOTA))
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "invalid selection operation");

    /* Check that both dataspaces have the same rank */
    if (space1->extent.rank != space2->extent.rank)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataspaces not same rank");

        /* Check that both dataspaces have the same offset */
        /** Note that this is a tricky part of this function. It's
         *  possible that two dataspaces have different "offset". If the
         *  space2 has smaller offset value than that of space1 in a
         *  dimension, then the span elements of this dimension in
         *  space2 could have negative "low" and "high" values relative
         *  to the offset in space1. In other words, if the bounds of
         *  span elements in space2 are adjusted relative to the offset
         *  in space1, then every span element's bound is computed as
         *  "origin_bound+offset2-offset1". Therefore, if offset2 (the
         *  offset of space2) is smaller, then
         *  "origin_bound+offset2-offset1" could be negative which is
         *  not allowed by the bound type declaration as hsize_t!
         *  As a result, if the op is an OR selection, then the final
         *  result may contain span elements that have negative bound!
         *  So right now, the difference in the offset is totally
         *  ignored!!
         */
#if 0
    for(u=0; u<space1->extent.rank; u++) {
        if(space1->select.offset[u] != space2->select.offset[u])
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataspaces not same offset");
    } /* end for */
#endif

    /* Check that both dataspaces have hyperslab selections */
    if (H5S_GET_SELECT_TYPE(space1) != H5S_SEL_HYPERSLABS ||
        H5S_GET_SELECT_TYPE(space2) != H5S_SEL_HYPERSLABS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataspaces don't have hyperslab selections");

    /* Go refine the first selection */
    if (H5S__modify_select(space1, op, space2) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to modify hyperslab selection");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Smodify_select() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_proj_int_build_proj
 PURPOSE
    Secondary iteration routine for H5S__hyper_project_intersection
 USAGE
    herr_t H5S__hyper_proj_int_build_proj(udata)
        H5S_hyper_project_intersect_ud_t *udata; IN/OUT: Persistent shared data for iteration
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    Takes the skip and nelem amounts listed in udata and converts them to
    span trees in the projected space, using the destination space.  This
    is a non-recursive algorithm by necessity, it saves the current state
    of iteration in udata and resumes in the same location on subsequent
    calls.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_proj_int_build_proj(H5S_hyper_project_intersect_ud_t *udata)
{
    H5S_hyper_span_info_t *copied_span_info = NULL;    /* Temporary span info pointer */
    herr_t                 ret_value        = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(udata->nelem > 0);

    /*
     * Skip over skipped elements
     */
    if (udata->skip > 0) {
        /* Work upwards, finishing each span tree before moving up */
        assert(udata->ds_span[udata->depth]);
        do {
            /* Check for lowest dimension */
            if (udata->ds_span[udata->depth]->down) {
                if (udata->ds_low[udata->depth] <= udata->ds_span[udata->depth]->high) {
                    /* If we will run out of elements to skip in this span,
                     * advance to the first not fully skipped span and break
                     * out of this loop (start moving downwards) */
                    if (udata->skip <
                        H5S__hyper_spans_nelem_helper(udata->ds_span[udata->depth]->down, 0, udata->op_gen) *
                            (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1)) {
                        udata->ds_low[udata->depth] +=
                            udata->skip / udata->ds_span[udata->depth]->down->op_info[0].u.nelmts;
                        udata->skip %= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts;
                        break;
                    } /* end if */

                    /* Skip over this entire span */
                    udata->skip -= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts *
                                   (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1);
                } /* end if */
            }     /* end if */
            else {
                assert(udata->ds_rank - udata->depth == 1);

                /* If we will run out of elements to skip in this span,
                 * skip the remainder of the skipped elements and break out */
                assert(udata->ds_low[udata->depth] <= udata->ds_span[udata->depth]->high);
                if (udata->skip < (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1)) {
                    udata->ds_low[udata->depth] += udata->skip;
                    udata->skip = 0;
                    break;
                } /* end if */

                /* Skip over this entire span */
                udata->skip -= udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1;
            } /* end else */

            /* Advance to next span */
            udata->ds_span[udata->depth] = udata->ds_span[udata->depth]->next;
            if (udata->ds_span[udata->depth])
                udata->ds_low[udata->depth] = udata->ds_span[udata->depth]->low;
            else if (udata->depth > 0) {
                /* If present, append this span tree to the higher dimension's,
                 * and release ownership of it */
                if (udata->ps_span_info[udata->depth]) {
                    if (H5S__hyper_append_span(
                            &udata->ps_span_info[udata->depth - 1], udata->ds_rank - udata->depth + 1,
                            udata->ds_low[udata->depth - 1], udata->ds_low[udata->depth - 1],
                            udata->ps_span_info[udata->depth]) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
                    if (H5S__hyper_free_span_info(udata->ps_span_info[udata->depth]) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                    udata->ps_span_info[udata->depth] = NULL;
                }

                /* Ran out of spans, move up one dimension */
                udata->depth--;
                assert(udata->ds_span[udata->depth]);
                udata->ds_low[udata->depth]++;
            }
            else
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                            "insufficient elements in destination selection");
        } while ((udata->skip > 0) || (udata->ds_low[udata->depth] > udata->ds_span[udata->depth]->high));

        /* Work downwards until skip is 0 */
        assert(udata->ds_span[udata->depth]);
        while (udata->skip > 0) {
            assert(udata->ds_span[udata->depth]->down);
            udata->depth++;
            udata->ds_span[udata->depth] = udata->ds_span[udata->depth - 1]->down->head;
            udata->ds_low[udata->depth]  = udata->ds_span[udata->depth]->low;
            if (udata->ds_span[udata->depth]->down) {
                do {
                    /* If we will run out of elements to skip in this span,
                     * advance to the first not fully skipped span and
                     * continue down */
                    if (udata->skip <
                        H5S__hyper_spans_nelem_helper(udata->ds_span[udata->depth]->down, 0, udata->op_gen) *
                            (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1)) {
                        udata->ds_low[udata->depth] +=
                            udata->skip / udata->ds_span[udata->depth]->down->op_info[0].u.nelmts;
                        udata->skip %= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts;
                        break;
                    } /* end if */

                    /* Skip over this entire span */
                    udata->skip -= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts *
                                   (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1);

                    /* Advance to next span */
                    udata->ds_span[udata->depth] = udata->ds_span[udata->depth]->next;
                    assert(udata->ds_span[udata->depth]);
                    udata->ds_low[udata->depth] = udata->ds_span[udata->depth]->low;
                } while (udata->skip > 0);
            } /* end if */
            else {
                do {
                    /* If we will run out of elements to skip in this span,
                     * skip the remainder of the skipped elements */
                    if (udata->skip <
                        (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1)) {
                        udata->ds_low[udata->depth] += udata->skip;
                        udata->skip = 0;
                        break;
                    } /* end if */

                    /* Skip over this entire span */
                    udata->skip -= udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1;

                    /* Advance to next span */
                    udata->ds_span[udata->depth] = udata->ds_span[udata->depth]->next;
                    assert(udata->ds_span[udata->depth]);
                    udata->ds_low[udata->depth] = udata->ds_span[udata->depth]->low;
                } while (udata->skip > 0);
            } /* end else */
        }     /* end while */
    }         /* end if */

    /*
     * Add requested number of elements to projected space
     */
    /* Work upwards, adding all elements of each span tree until it can't fit
     * all elements */
    assert(udata->ds_span[udata->depth]);
    do {
        /* Check for lowest dimension */
        if (udata->ds_span[udata->depth]->down) {
            if (udata->ds_low[udata->depth] <= udata->ds_span[udata->depth]->high) {
                /* If we will run out of elements to add in this span, add
                 * any complete spans, advance to the first not fully added
                 * span, and break out of this loop (start moving downwards)
                 */
                if (udata->nelem <
                    H5S__hyper_spans_nelem_helper(udata->ds_span[udata->depth]->down, 0, udata->op_gen) *
                        (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1)) {
                    if (udata->nelem >= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts) {
                        if (udata->share_selection) {
                            if (H5S__hyper_append_span(
                                    &udata->ps_span_info[udata->depth], udata->ds_rank - udata->depth,
                                    udata->ds_low[udata->depth],
                                    udata->ds_low[udata->depth] +
                                        (udata->nelem /
                                         udata->ds_span[udata->depth]->down->op_info[0].u.nelmts) -
                                        1,
                                    udata->ds_span[udata->depth]->down) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                        }
                        else {
                            /* If we're not sharing the destination space's
                             * spans, we must copy it first (then release it
                             * afterwards) */
                            if (NULL == (copied_span_info = H5S__hyper_copy_span_helper(
                                             udata->ds_span[udata->depth]->down,
                                             udata->ds_rank - udata->depth, 1, udata->op_gen)))
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL,
                                            "can't copy destination spans");
                            if (H5S__hyper_append_span(
                                    &udata->ps_span_info[udata->depth], udata->ds_rank - udata->depth,
                                    udata->ds_low[udata->depth],
                                    udata->ds_low[udata->depth] +
                                        (udata->nelem /
                                         udata->ds_span[udata->depth]->down->op_info[0].u.nelmts) -
                                        1,
                                    copied_span_info) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                            if (H5S__hyper_free_span_info(copied_span_info) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                            copied_span_info = NULL;
                        }
                        udata->ds_low[udata->depth] +=
                            udata->nelem / udata->ds_span[udata->depth]->down->op_info[0].u.nelmts;
                        udata->nelem %= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts;
                    } /* end if */
                    break;
                } /* end if */

                /* Append span tree for entire span */
                if (udata->share_selection) {
                    if (H5S__hyper_append_span(&udata->ps_span_info[udata->depth],
                                               udata->ds_rank - udata->depth, udata->ds_low[udata->depth],
                                               udata->ds_span[udata->depth]->high,
                                               udata->ds_span[udata->depth]->down) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
                }
                else {
                    /* If we're not sharing the destination space's
                     * spans, we must copy it first (then release it
                     * afterwards) */
                    if (NULL == (copied_span_info = H5S__hyper_copy_span_helper(
                                     udata->ds_span[udata->depth]->down, udata->ds_rank - udata->depth, 1,
                                     udata->op_gen)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "can't copy destination spans");
                    if (H5S__hyper_append_span(&udata->ps_span_info[udata->depth],
                                               udata->ds_rank - udata->depth, udata->ds_low[udata->depth],
                                               udata->ds_span[udata->depth]->high, copied_span_info) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
                    if (H5S__hyper_free_span_info(copied_span_info) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                    copied_span_info = NULL;
                }
                udata->nelem -= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts *
                                (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1);
            } /* end if */
        }     /* end if */
        else {
            assert(udata->ds_rank - udata->depth == 1);

            /* If we will run out of elements to add in this span, add the
             * remainder of the elements and break out */
            assert(udata->ds_low[udata->depth] <= udata->ds_span[udata->depth]->high);
            if (udata->nelem < (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1)) {
                if (H5S__hyper_append_span(&udata->ps_span_info[udata->depth], 1, udata->ds_low[udata->depth],
                                           udata->ds_low[udata->depth] + udata->nelem - 1, NULL) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
                udata->ds_low[udata->depth] += udata->nelem;
                udata->nelem = 0;
                break;
            } /* end if */

            /* Append span tree for entire span */
            if (H5S__hyper_append_span(&udata->ps_span_info[udata->depth], 1, udata->ds_low[udata->depth],
                                       udata->ds_span[udata->depth]->high, NULL) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
            udata->nelem -= udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1;
        } /* end else */

        /* Advance to next span */
        udata->ds_span[udata->depth] = udata->ds_span[udata->depth]->next;
        if (udata->ds_span[udata->depth])
            udata->ds_low[udata->depth] = udata->ds_span[udata->depth]->low;
        else if (udata->depth > 0) {
            /* Append this span tree to the higher dimension's, and release
             * ownership of it */
            assert(udata->ps_span_info[udata->depth]);
            if (H5S__hyper_append_span(&udata->ps_span_info[udata->depth - 1],
                                       udata->ds_rank - udata->depth + 1, udata->ds_low[udata->depth - 1],
                                       udata->ds_low[udata->depth - 1],
                                       udata->ps_span_info[udata->depth]) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
            if (H5S__hyper_free_span_info(udata->ps_span_info[udata->depth]) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
            udata->ps_span_info[udata->depth] = NULL;

            /* Ran out of spans, move up one dimension */
            udata->depth--;
            assert(udata->ds_span[udata->depth]);
            udata->ds_low[udata->depth]++;
        } /* end if */
        else {
            /* We have finished the entire destination span tree.  If there are
             * still elements to add, issue an error. */
            if (udata->nelem > 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                            "insufficient elements in destination selection");
            break;
        } /* end else */
    } while ((udata->nelem > 0) || (udata->ds_low[udata->depth] > udata->ds_span[udata->depth]->high));

    /* Work downwards until nelem is 0 */
    assert(udata->ds_span[udata->depth] || (udata->nelem == 0));
    while (udata->nelem > 0) {
        assert(udata->ds_span[udata->depth]->down);
        udata->depth++;
        udata->ds_span[udata->depth] = udata->ds_span[udata->depth - 1]->down->head;
        udata->ds_low[udata->depth]  = udata->ds_span[udata->depth]->low;
        if (udata->ds_span[udata->depth]->down) {
            do {
                /* If we will run out of elements to add in this span, add
                 * any complete spans, advance to the first not fully added
                 * span and continue down
                 */
                assert(udata->ds_low[udata->depth] <= udata->ds_span[udata->depth]->high);
                if (udata->nelem <
                    H5S__hyper_spans_nelem_helper(udata->ds_span[udata->depth]->down, 0, udata->op_gen) *
                        (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1)) {
                    if (udata->nelem >= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts) {
                        if (udata->share_selection) {
                            if (H5S__hyper_append_span(
                                    &udata->ps_span_info[udata->depth], udata->ds_rank - udata->depth,
                                    udata->ds_low[udata->depth],
                                    udata->ds_low[udata->depth] +
                                        (udata->nelem /
                                         udata->ds_span[udata->depth]->down->op_info[0].u.nelmts) -
                                        1,
                                    udata->ds_span[udata->depth]->down) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                        }
                        else {
                            /* If we're not sharing the destination space's
                             * spans, we must copy it first (then release it
                             * afterwards) */
                            if (NULL == (copied_span_info = H5S__hyper_copy_span_helper(
                                             udata->ds_span[udata->depth]->down,
                                             udata->ds_rank - udata->depth, 1, udata->op_gen)))
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL,
                                            "can't copy destination spans");
                            if (H5S__hyper_append_span(
                                    &udata->ps_span_info[udata->depth], udata->ds_rank - udata->depth,
                                    udata->ds_low[udata->depth],
                                    udata->ds_low[udata->depth] +
                                        (udata->nelem /
                                         udata->ds_span[udata->depth]->down->op_info[0].u.nelmts) -
                                        1,
                                    copied_span_info) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL,
                                            "can't allocate hyperslab span");
                            if (H5S__hyper_free_span_info(copied_span_info) < 0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                            copied_span_info = NULL;
                        }
                        udata->ds_low[udata->depth] +=
                            udata->nelem / udata->ds_span[udata->depth]->down->op_info[0].u.nelmts;
                        udata->nelem %= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts;
                    } /* end if */
                    break;
                } /* end if */

                /* Append span tree for entire span */
                if (udata->share_selection) {
                    if (H5S__hyper_append_span(&udata->ps_span_info[udata->depth],
                                               udata->ds_rank - udata->depth, udata->ds_low[udata->depth],
                                               udata->ds_span[udata->depth]->high,
                                               udata->ds_span[udata->depth]->down) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
                }
                else {
                    /* If we're not sharing the destination space's
                     * spans, we must copy it first (then release it
                     * afterwards) */
                    if (NULL == (copied_span_info = H5S__hyper_copy_span_helper(
                                     udata->ds_span[udata->depth]->down, udata->ds_rank - udata->depth, 1,
                                     udata->op_gen)))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "can't copy destination spans");
                    if (H5S__hyper_append_span(&udata->ps_span_info[udata->depth],
                                               udata->ds_rank - udata->depth, udata->ds_low[udata->depth],
                                               udata->ds_span[udata->depth]->high, copied_span_info) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
                    if (H5S__hyper_free_span_info(copied_span_info) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                    copied_span_info = NULL;
                }
                udata->nelem -= udata->ds_span[udata->depth]->down->op_info[0].u.nelmts *
                                (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1);

                /* Advance to next span */
                udata->ds_span[udata->depth] = udata->ds_span[udata->depth]->next;
                assert(udata->ds_span[udata->depth]);
                udata->ds_low[udata->depth] = udata->ds_span[udata->depth]->low;
            } while (udata->nelem > 0);
        } /* end if */
        else {
            assert(udata->ds_rank - udata->depth == 1);
            do {
                /* If we will run out of elements to add in this span, add
                 * the remainder of the elements and break out */
                assert(udata->ds_low[udata->depth] <= udata->ds_span[udata->depth]->high);
                if (udata->nelem < (udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1)) {
                    if (H5S__hyper_append_span(&udata->ps_span_info[udata->depth], 1,
                                               udata->ds_low[udata->depth],
                                               udata->ds_low[udata->depth] + udata->nelem - 1, NULL) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
                    udata->ds_low[udata->depth] += udata->nelem;
                    udata->nelem = 0;
                    break;
                } /* end if */

                /* Append span tree for entire span */
                if (H5S__hyper_append_span(&udata->ps_span_info[udata->depth], 1, udata->ds_low[udata->depth],
                                           udata->ds_span[udata->depth]->high, NULL) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
                udata->nelem -= udata->ds_span[udata->depth]->high - udata->ds_low[udata->depth] + 1;

                /* Advance to next span */
                udata->ds_span[udata->depth] = udata->ds_span[udata->depth]->next;
                assert(udata->ds_span[udata->depth]);
                udata->ds_low[udata->depth] = udata->ds_span[udata->depth]->low;
            } while (udata->nelem > 0);
        } /* end else */
    }     /* end while */

    assert(udata->skip == 0);
    assert(udata->nelem == 0);

    /* Mark projected space as changed (for all ranks) */
    udata->ps_clean_bitmap = 0;

done:
    /* Cleanup on failure */
    if (copied_span_info) {
        assert(ret_value < 0);
        if (H5S__hyper_free_span_info(copied_span_info) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
        copied_span_info = NULL;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_proj_int_build_proj() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_proj_int_iterate
 PURPOSE
    Main iteration routine for H5S__hyper_project_intersection
 USAGE
    herr_t H5S__hyper_proj_int_iterate(ss_span_info,sis_span_info,count,depth,udata)
        const H5S_hyper_span_info_t *ss_span_info; IN: Span tree for source selection
        const H5S_hyper_span_info_t *sis_span_info; IN: Span tree for source intersect selection
        hsize_t count;          IN: Number of times to compute the intersection of ss_span_info and
sis_span_info unsigned depth;         IN: Depth of iteration (in terms of rank)
        H5S_hyper_project_intersect_ud_t *udata; IN/OUT: Persistent shared data for iteration
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    Computes the intersection of ss_span_info and sis_span_info and projects it
    to the projected space (held in udata).  It accomplishes this by iterating
    over both spaces and computing the number of elements to skip (in
    ss_span_info) and the number of elements to add (the intersection) in a
    sequential fashion (similar to run length encoding).  As necessary, this
    function both recurses into lower dimensions and calls
    H5S__hyper_proj_int_build_proj to convert the skip/nelem pairs to the
    projected span tree.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__hyper_proj_int_iterate(H5S_hyper_span_info_t *ss_span_info, const H5S_hyper_span_info_t *sis_span_info,
                            hsize_t count, unsigned depth, H5S_hyper_project_intersect_ud_t *udata)
{
    const H5S_hyper_span_t *ss_span;             /* Current span in source space */
    const H5S_hyper_span_t *sis_span;            /* Current span in source intersect space */
    hsize_t                 ss_low;              /* Current low bounds of source span */
    hsize_t                 sis_low;             /* Current low bounds of source intersect span */
    hsize_t                 high;                /* High bounds of current intersection */
    hsize_t                 low;                 /* Low bounds of current intersection */
    hsize_t                 old_skip;            /* Value of udata->skip before main loop */
    hsize_t                 old_nelem;           /* Value of udata->nelem before main loop */
    bool                    check_intersect;     /* Whether to check for intersecting elements */
    unsigned                u;                   /* Local index variable */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for non-overlapping bounds */
    check_intersect = true;
    for (u = 0; u < (udata->ss_rank - depth); u++)
        if (!H5S_RANGE_OVERLAP(ss_span_info->low_bounds[u], ss_span_info->high_bounds[u],
                               sis_span_info->low_bounds[u], sis_span_info->high_bounds[u])) {
            check_intersect = false;
            break;
        } /* end if */

    /* Only enter main loop if there's something to do */
    if (check_intersect) {
        /* Set ps_clean_bitmap */
        udata->ps_clean_bitmap |= (((uint32_t)1) << depth);

        /* Save old skip and nelem */
        old_skip  = udata->skip;
        old_nelem = udata->nelem;

        /* Intersect spaces once per count */
        for (u = 0; u < count; u++) {
            ss_span  = ss_span_info->head;
            sis_span = sis_span_info->head;
            assert(ss_span && sis_span);
            ss_low  = ss_span->low;
            sis_low = sis_span->low;

            /* Main loop */
            do {
                /* Check if spans overlap */
                if (H5S_RANGE_OVERLAP(ss_low, ss_span->high, sis_low, sis_span->high)) {
                    high = MIN(ss_span->high, sis_span->high);
                    if (ss_span->down) {
                        /* Add skipped elements if there's a pre-gap */
                        if (ss_low < sis_low) {
                            low = sis_low;
                            H5S_HYPER_PROJ_INT_ADD_SKIP(
                                udata,
                                H5S__hyper_spans_nelem_helper(ss_span->down, 0, udata->op_gen) *
                                    (sis_low - ss_low),
                                FAIL);
                        } /* end if */
                        else
                            low = ss_low;

                        /* Recurse into next dimension down */
                        if (H5S__hyper_proj_int_iterate(ss_span->down, sis_span->down, high - low + 1,
                                                        depth + 1, udata) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL,
                                        "can't iterate over source selections");
                    } /* end if */
                    else {
                        assert(depth == udata->ss_rank - 1);

                        /* Add skipped elements if there's a pre-gap */
                        if (ss_low < sis_low) {
                            low = sis_low;
                            H5S_HYPER_PROJ_INT_ADD_SKIP(udata, sis_low - ss_low, FAIL);
                        } /* end if */
                        else
                            low = ss_low;

                        /* Add overlapping elements */
                        udata->nelem += high - low + 1;
                    } /* end else */

                    /* Advance spans */
                    if (ss_span->high == sis_span->high) {
                        /* Advance both spans */
                        ss_span = ss_span->next;
                        if (ss_span)
                            ss_low = ss_span->low;
                        sis_span = sis_span->next;
                        if (sis_span)
                            sis_low = sis_span->low;
                    } /* end if */
                    else if (ss_span->high == high) {
                        /* Advance source span */
                        assert(ss_span->high < sis_span->high);
                        sis_low = high + 1;
                        ss_span = ss_span->next;
                        if (ss_span)
                            ss_low = ss_span->low;
                    } /* end if */
                    else {
                        /* Advance source intersect span */
                        assert(ss_span->high > sis_span->high);
                        ss_low   = high + 1;
                        sis_span = sis_span->next;
                        if (sis_span)
                            sis_low = sis_span->low;
                    } /* end else */
                }     /* end if */
                else {
                    /* Advance spans */
                    if (ss_span->high < sis_low) {
                        /* Add skipped elements */
                        if (ss_span->down)
                            H5S_HYPER_PROJ_INT_ADD_SKIP(
                                udata,
                                H5S__hyper_spans_nelem_helper(ss_span->down, 0, udata->op_gen) *
                                    (ss_span->high - ss_low + 1),
                                FAIL);
                        else
                            H5S_HYPER_PROJ_INT_ADD_SKIP(udata, ss_span->high - ss_low + 1, FAIL);

                        /* Advance source span */
                        ss_span = ss_span->next;
                        if (ss_span)
                            ss_low = ss_span->low;
                    } /* end if */
                    else {
                        /* Advance source intersect span */
                        assert(ss_low > sis_span->high);
                        sis_span = sis_span->next;
                        if (sis_span)
                            sis_low = sis_span->low;
                    } /* end else */
                }     /* end else */
            } while (ss_span && sis_span);

            if (ss_span && !((depth == 0) && (u == count - 1))) {
                /* Count remaining elements in ss_span_info */
                if (ss_span->down) {
                    H5S_HYPER_PROJ_INT_ADD_SKIP(
                        udata,
                        H5S__hyper_spans_nelem_helper(ss_span->down, 0, udata->op_gen) *
                            (ss_span->high - ss_low + 1),
                        FAIL);
                    ss_span = ss_span->next;
                    while (ss_span) {
                        H5S_HYPER_PROJ_INT_ADD_SKIP(
                            udata,
                            H5S__hyper_spans_nelem_helper(ss_span->down, 0, udata->op_gen) *
                                (ss_span->high - ss_span->low + 1),
                            FAIL);
                        ss_span = ss_span->next;
                    } /* end while */
                }     /* end if */
                else {
                    H5S_HYPER_PROJ_INT_ADD_SKIP(udata, ss_span->high - ss_low + 1, FAIL);
                    ss_span = ss_span->next;
                    while (ss_span) {
                        H5S_HYPER_PROJ_INT_ADD_SKIP(udata, ss_span->high - ss_span->low + 1, FAIL);
                        ss_span = ss_span->next;
                    } /* end while */
                }     /* end else */
            }         /* end if */

            /* Check if the projected space was not changed since we started the
             * first iteration of the loop, if so we do not need to continue
             * looping and can just copy the result */
            if (udata->ps_clean_bitmap & (((uint32_t)1) << depth)) {
                assert(u == 0);
                if (udata->skip == old_skip) {
                    /* First case: algorithm added only elements */
                    assert(udata->nelem >= old_nelem);
                    udata->nelem += (count - 1) * (udata->nelem - old_nelem);
                } /* end if */
                else if (udata->nelem == 0) {
                    /* Second case: algorithm added only skip.  In this case,
                     * nelem must be 0 since otherwise adding skip would have
                     * triggered a change in the projected space */
                    assert(old_nelem == 0);
                    assert(udata->skip > old_skip);
                    udata->skip += (count - 1) * (udata->skip - old_skip);
                } /* end if */
                else {
                    /* Third case: algorithm added skip and nelem (in that
                     * order).  Add the same skip and nelem once for each item
                     * remaining in count. */
                    hsize_t skip_add;
                    hsize_t nelem_add;

                    assert(udata->nelem > 0);
                    assert(udata->skip > old_skip);
                    assert(old_nelem == 0);

                    skip_add  = udata->skip - old_skip;
                    nelem_add = udata->nelem - old_nelem;
                    for (u = 1; u < count; u++) {
                        H5S_HYPER_PROJ_INT_ADD_SKIP(udata, skip_add, FAIL);
                        udata->nelem += nelem_add;
                    } /* end for */
                }     /* end else */

                /* End loop since we already took care of it */
                break;
            } /* end if */
        }     /* end for */
    }         /* end if */
    else if (depth > 0)
        /* Just count skipped elements */
        H5S_HYPER_PROJ_INT_ADD_SKIP(
            udata,
            H5S__hyper_spans_nelem_helper((H5S_hyper_span_info_t *)ss_span_info, 0, udata->op_gen) * count,
            FAIL);

    /* Clean up if we are done */
    if (depth == 0) {
        /* Add remaining elements */
        if (udata->nelem > 0)
            if (H5S__hyper_proj_int_build_proj(udata) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't add elements to projected selection");

        /* Append remaining span trees */
        for (u = udata->ds_rank - 1; u > 0; u--)
            if (udata->ps_span_info[u]) {
                if (H5S__hyper_append_span(&udata->ps_span_info[u - 1], udata->ds_rank - u + 1,
                                           udata->ds_low[u - 1], udata->ds_low[u - 1],
                                           udata->ps_span_info[u]) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTAPPEND, FAIL, "can't allocate hyperslab span");
                if (H5S__hyper_free_span_info(udata->ps_span_info[u]) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                udata->ps_span_info[u] = NULL;
            }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_proj_int_iterate() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_project_intersection
 PURPOSE
    Projects the intersection of of the selections of src_space and
    src_intersect_space within the selection of src_space as a selection
    within the selection of dst_space
 USAGE
    herr_t H5S__hyper_project_intersection(src_space,dst_space,src_intersect_space,proj_space,share_selection)
        H5S_t *src_space;       IN: Selection that is mapped to dst_space, and intersected with
src_intersect_space H5S_t *dst_space;       IN: Selection that is mapped to src_space, and which contains the
result H5S_t *src_intersect_space; IN: Selection whose intersection with src_space is projected to dst_space
to obtain the result H5S_t *proj_space;      OUT: Will contain the result (intersection of src_intersect_space
and src_space projected from src_space to dst_space) after the operation bool share_selection; IN: Whether
we are allowed to share structures inside dst_space with proj_space RETURNS Non-negative on success/Negative
on failure. DESCRIPTION Projects the intersection of of the selections of src_space and src_intersect_space
within the selection of src_space as a selection within the selection of dst_space.  The result is placed in
the selection of proj_space.  Note src_space, dst_space, and src_intersect_space do not need to use hyperslab
selections, but they cannot use point selections. The result is always a hyperslab or none selection.  Note
also that proj_space can share some span trees with dst_space, so proj_space must not be subsequently modified
if dst_space must be preserved. GLOBAL VARIABLES COMMENTS, BUGS, ASSUMPTIONS EXAMPLES REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S__hyper_project_intersection(H5S_t *src_space, H5S_t *dst_space, H5S_t *src_intersect_space,
                                H5S_t *proj_space, bool share_selection)
{
    H5S_hyper_project_intersect_ud_t udata; /* User data for subroutines */
    H5S_hyper_span_info_t           *ss_span_info;
    const H5S_hyper_span_info_t     *ds_span_info;
    H5S_hyper_span_info_t           *ss_span_info_buf = NULL;
    H5S_hyper_span_info_t           *ds_span_info_buf = NULL;
    herr_t                           ret_value        = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check parameters */
    assert(src_space);
    assert(dst_space);
    assert(src_intersect_space);
    assert(proj_space);

    /* Assert that src_space and src_intersect_space have same rank and there
     * are no point selections */
    assert(H5S_GET_EXTENT_NDIMS(src_space) == H5S_GET_EXTENT_NDIMS(src_intersect_space));
    assert(H5S_GET_SELECT_NPOINTS(src_space) == H5S_GET_SELECT_NPOINTS(dst_space));
    assert(H5S_GET_SELECT_TYPE(src_space) != H5S_SEL_POINTS);
    assert(H5S_GET_SELECT_TYPE(dst_space) != H5S_SEL_POINTS);
    assert(H5S_GET_SELECT_TYPE(src_intersect_space) == H5S_SEL_HYPERSLABS);

    /* Set up ss_span_info */
    if (H5S_GET_SELECT_TYPE(src_space) == H5S_SEL_HYPERSLABS) {
        /* Make certain the selection has a span tree */
        if (NULL == src_space->select.sel_info.hslab->span_lst)
            if (H5S__hyper_generate_spans(src_space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNINITIALIZED, FAIL,
                            "can't construct span tree for source hyperslab selection");

        /* Simply point to existing span tree */
        ss_span_info = src_space->select.sel_info.hslab->span_lst;
    } /* end if */
    else {
        /* Create temporary span tree from all selection */
        assert(H5S_GET_SELECT_TYPE(src_space) == H5S_SEL_ALL);

        if (NULL == (ss_span_info_buf =
                         H5S__hyper_make_spans(H5S_GET_EXTENT_NDIMS(src_space), H5S_hyper_zeros_g,
                                               H5S_hyper_zeros_g, H5S_hyper_ones_g, src_space->extent.size)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't create span tree for ALL source space");
        ss_span_info = ss_span_info_buf;
    } /* end else */

    /* Set up ds_span_info */
    if (H5S_GET_SELECT_TYPE(dst_space) == H5S_SEL_HYPERSLABS) {
        /* Make certain the selection has a span tree */
        if (NULL == dst_space->select.sel_info.hslab->span_lst)
            if (H5S__hyper_generate_spans(dst_space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_UNINITIALIZED, FAIL,
                            "can't construct span tree for dsetination hyperslab selection");

        /* Simply point to existing span tree */
        ds_span_info = dst_space->select.sel_info.hslab->span_lst;
    } /* end if */
    else {
        /* Create temporary span tree from all selection */
        assert(H5S_GET_SELECT_TYPE(dst_space) == H5S_SEL_ALL);

        if (NULL == (ds_span_info_buf =
                         H5S__hyper_make_spans(H5S_GET_EXTENT_NDIMS(dst_space), H5S_hyper_zeros_g,
                                               H5S_hyper_zeros_g, H5S_hyper_ones_g, dst_space->extent.size)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL,
                        "can't create span tree for ALL destination space");
        ds_span_info = ds_span_info_buf;
    } /* end else */

    /* Make certain the source intersect selection has a span tree */
    if (NULL == src_intersect_space->select.sel_info.hslab->span_lst)
        if (H5S__hyper_generate_spans(src_intersect_space) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNINITIALIZED, FAIL,
                        "can't construct span tree for source intersect hyperslab selection");

    /* Initialize udata */
    /* We will use op_info[0] for nelem and op_info[1] for copied spans */
    memset(&udata, 0, sizeof(udata));
    udata.ds_span[0]      = ds_span_info->head;
    udata.ds_low[0]       = udata.ds_span[0]->low;
    udata.ss_rank         = H5S_GET_EXTENT_NDIMS(src_space);
    udata.ds_rank         = H5S_GET_EXTENT_NDIMS(dst_space);
    udata.op_gen          = H5S__hyper_get_op_gen();
    udata.share_selection = share_selection;

    /* Iterate over selections and build projected span tree */
    if (H5S__hyper_proj_int_iterate(ss_span_info, src_intersect_space->select.sel_info.hslab->span_lst, 1, 0,
                                    &udata) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL, "selection iteration failed");

    /* Remove current selection from proj_space */
    if (H5S_SELECT_RELEASE(proj_space) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't release selection");

    /* Check for elements in projected space */
    if (udata.ps_span_info[0]) {
        /* Allocate space for the hyperslab selection information (note this sets
         * diminfo_valid to false, diminfo arrays to 0, and span list to NULL) */
        if (NULL == (proj_space->select.sel_info.hslab = H5FL_CALLOC(H5S_hyper_sel_t)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate hyperslab info");

        /* Set selection type */
        proj_space->select.type = H5S_sel_hyper;

        /* Set unlim_dim */
        proj_space->select.sel_info.hslab->unlim_dim = -1;

        /* Set span tree */
        proj_space->select.sel_info.hslab->span_lst = udata.ps_span_info[0];
        udata.ps_span_info[0]                       = NULL;

        /* Set the number of elements in current selection */
        proj_space->select.num_elem = H5S__hyper_spans_nelem(proj_space->select.sel_info.hslab->span_lst);

        /* Attempt to build "optimized" start/stride/count/block information
         * from resulting hyperslab span tree.
         */
        H5S__hyper_rebuild(proj_space);
    } /* end if */
    else
        /* If we did not add anything to proj_space, select none instead */
        if (H5S_select_none(proj_space) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't convert selection");

done:
    /* Free ss_span_info_buf */
    if (ss_span_info_buf) {
        if (H5S__hyper_free_span_info(ss_span_info_buf) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
        ss_span_info_buf = NULL;
    }

    /* Free ds_span_info_buf */
    if (ds_span_info_buf) {
        if (H5S__hyper_free_span_info(ds_span_info_buf) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
        ds_span_info_buf = NULL;
    }

    /* Cleanup on error */
    if (ret_value < 0) {
        unsigned u;

        /* Free span trees */
        for (u = 0; u < udata.ds_rank; u++)
            if (udata.ps_span_info[u]) {
                if (H5S__hyper_free_span_info(udata.ps_span_info[u]) < 0)
                    HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "unable to free span info");
                udata.ps_span_info[u] = NULL;
            }
    }

#ifndef NDEBUG
    /* Verify there are no more span trees */
    {
        unsigned u;

        for (u = 0; u < H5S_MAX_RANK; u++)
            assert(!udata.ps_span_info[u]);
    }
#endif /* NDEBUG */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_project_intersection() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_get_clip_diminfo
 PURPOSE
    Calculates the count and block required to clip the specified
    unlimited dimension to include clip_size.  The returned selection may
    extent beyond clip_size.
 USAGE
    void H5S__hyper_get_clip_diminfo(start,stride,count,block,clip_size)
        hsize_t start;          IN: Start of hyperslab in unlimited dimension
        hsize_t stride;         IN: Stride of hyperslab in unlimited dimension
        hsize_t *count;         IN/OUT: Count of hyperslab in unlimited dimension
        hsize_t *block;         IN/OUT: Block of hyperslab in unlimited dimension
        hsize_t clip_size;      IN: Extent that hyperslab will be clipped to
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    This function recalculates the internal description of the hyperslab
    to make the unlimited dimension extend to the specified extent.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static void
H5S__hyper_get_clip_diminfo(hsize_t start, hsize_t stride, hsize_t *count, hsize_t *block, hsize_t clip_size)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check for selection outside clip size */
    if (start >= clip_size) {
        if (*block == H5S_UNLIMITED)
            *block = 0;
        else
            *count = 0;
    } /* end if */
    /* Check for single block in unlimited dimension */
    else if ((*block == H5S_UNLIMITED) || (*block == stride)) {
        /* Calculate actual block size for this clip size */
        *block = clip_size - start;
        *count = (hsize_t)1;
    } /* end if */
    else {
        assert(*count == H5S_UNLIMITED);

        /* Calculate initial count (last block may be partial) */
        *count = (clip_size - start + stride - (hsize_t)1) / stride;
        assert(*count > (hsize_t)0);
    } /* end else */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5S__hyper_get_clip_diminfo() */

/*--------------------------------------------------------------------------
 NAME
    H5S_hyper_clip_unlim
 PURPOSE
    Clips the unlimited dimension of the hyperslab selection to the
    specified size
 USAGE
    void H5S_hyper_clip_unlim(space,clip_size)
        H5S_t *space,           IN/OUT: Unlimited space to clip
        hsize_t clip_size;      IN: Extent that hyperslab will be clipped to
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    This function changes the unlimited selection into a fixed-dimension selection
    with the extent of the formerly unlimited dimension specified by clip_size.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Note this function does not take the offset into account.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_hyper_clip_unlim(H5S_t *space, hsize_t clip_size)
{
    H5S_hyper_sel_t *hslab = NULL;        /* Convenience pointer to hyperslab info */
    hsize_t          orig_count;          /* Original count in unlimited dimension */
    int              orig_unlim_dim;      /* Original unliminted dimension */
    H5S_hyper_dim_t *diminfo   = NULL;    /* Convenience pointer to diminfo.opt in unlimited dimension */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check parameters */
    assert(space);
    hslab = space->select.sel_info.hslab;
    assert(hslab);
    assert(hslab->unlim_dim >= 0);
    assert(!hslab->span_lst);

    /* Save original unlimited dimension */
    orig_unlim_dim = hslab->unlim_dim;

    /* Set up convenience pointer */
    diminfo = &hslab->diminfo.opt[orig_unlim_dim];

    /* Save original count in unlimited dimension */
    orig_count = diminfo->count;

    /* Get initial diminfo */
    H5S__hyper_get_clip_diminfo(diminfo->start, diminfo->stride, &diminfo->count, &diminfo->block, clip_size);

    /* Selection is no longer unlimited */
    space->select.sel_info.hslab->unlim_dim = -1;

    /* Check for nothing returned */
    if ((diminfo->block == 0) || (diminfo->count == 0)) {
        /* Convert to "none" selection */
        if (H5S_select_none(space) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't convert selection");

        /* Reset the convenience pointers */
        hslab   = NULL;
        diminfo = NULL;
    } /* end if */
    /* Check for single block in unlimited dimension */
    else if (orig_count == (hsize_t)1) {
        /* Calculate number of elements */
        space->select.num_elem = diminfo->block * hslab->num_elem_non_unlim;

        /* Mark that diminfo.opt is valid */
        hslab->diminfo_valid = H5S_DIMINFO_VALID_YES;
    } /* end if */
    else {
        /* Calculate number of elements */
        space->select.num_elem = diminfo->count * diminfo->block * hslab->num_elem_non_unlim;

        /* Check if last block is partial.  If superset is set, just keep the
         * last block complete to speed computation. */
        assert(clip_size > diminfo->start);
        if (((diminfo->stride * (diminfo->count - (hsize_t)1)) + diminfo->block) >
            (clip_size - diminfo->start)) {
            hsize_t  start[H5S_MAX_RANK];
            hsize_t  block[H5S_MAX_RANK];
            unsigned u;

            /* Last block is partial, need to construct compound selection */
            /* Fill start with zeros */
            memset(start, 0, sizeof(start));

            /* Set block to clip_size in unlimited dimension, H5S_MAX_SIZE in
             * others so only unlimited dimension is clipped */
            for (u = 0; u < space->extent.rank; u++)
                if ((int)u == orig_unlim_dim)
                    block[u] = clip_size;
                else
                    block[u] = H5S_MAX_SIZE;

            /* Generate span tree in selection */
            if (!hslab->span_lst)
                if (H5S__hyper_generate_spans(space) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to generate span tree");

            /* Indicate that the regular dimensions are no longer valid */
            hslab->diminfo_valid = H5S_DIMINFO_VALID_NO;

            /* "And" selection with calculated block to perform clip operation */
            if (H5S__generate_hyperslab(space, H5S_SELECT_AND, start, H5S_hyper_ones_g, H5S_hyper_ones_g,
                                        block) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't generate hyperslabs");
        } /* end if */
        else
            /* Last block is complete, simply mark that diminfo.opt is valid */
            hslab->diminfo_valid = H5S_DIMINFO_VALID_YES;
    } /* end else */

    /* Update the upper bound, if the diminfo is valid */
    if (hslab && (H5S_DIMINFO_VALID_YES == hslab->diminfo_valid))
        hslab->diminfo.high_bounds[orig_unlim_dim] =
            hslab->diminfo.opt[orig_unlim_dim].start +
            hslab->diminfo.opt[orig_unlim_dim].stride * (hslab->diminfo.opt[orig_unlim_dim].count - 1) +
            (hslab->diminfo.opt[orig_unlim_dim].block - 1);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_hyper_clip_unlim() */

/*--------------------------------------------------------------------------
 NAME
    H5S__hyper_get_clip_extent_real
 PURPOSE
    Gets the extent a space should be clipped to in order to contain the
    specified number of slices in the unlimited dimension
 USAGE
    hsize_t H5S__hyper_get_clip_extent_real(clip_space,num_slices,incl_trail)
        const H5S_t *clip_space, IN: Space that clip size will be calculated based on
        hsize_t num_slizes,     IN: Number of slices clip_space should contain when clipped
        bool incl_trail;     IN: Whether to include trailing unselected space
 RETURNS
    Clip extent to match num_slices (never fails)
 DESCRIPTION
    Calculates and returns the extent that clip_space should be clipped to
    (via H5S_hyper_clip_unlim) in order for it to contain num_slices
    slices in the unlimited dimension.  If the clipped selection would end
    immediately before a section of unselected space (i.e. at the end of a
    block), then if incl_trail is true, the returned clip extent is
    selected to include that trailing "blank" space, otherwise it is
    selected to end at the end before the blank space.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Note this assumes the offset has been normalized.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static hsize_t
H5S__hyper_get_clip_extent_real(const H5S_t *clip_space, hsize_t num_slices, bool incl_trail)
{
    const H5S_hyper_dim_t *diminfo; /* Convenience pointer to opt_unlim_diminfo in unlimited dimension */
    hsize_t                count;
    hsize_t                rem_slices;
    hsize_t                ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check parameters */
    assert(clip_space);
    assert(clip_space->select.sel_info.hslab);
    assert(clip_space->select.sel_info.hslab->unlim_dim >= 0);

    diminfo = &clip_space->select.sel_info.hslab->diminfo.opt[clip_space->select.sel_info.hslab->unlim_dim];

    if (num_slices == 0)
        ret_value = incl_trail ? diminfo->start : 0;
    else if ((diminfo->block == H5S_UNLIMITED) || (diminfo->block == diminfo->stride))
        /* Unlimited block, just set the extent large enough for the block size
         * to match num_slices */
        ret_value = diminfo->start + num_slices;
    else {
        /* Unlimited count, need to match extent so a block (possibly) gets cut
         * off so the number of slices matches num_slices */
        assert(diminfo->count == H5S_UNLIMITED);

        /* Calculate number of complete blocks in clip_space */
        count = num_slices / diminfo->block;

        /* Calculate slices remaining */
        rem_slices = num_slices - (count * diminfo->block);

        if (rem_slices > 0)
            /* Must end extent in middle of partial block (or beginning of empty
             * block if include_trailing_space and rem_slices == 0) */
            ret_value = diminfo->start + (count * diminfo->stride) + rem_slices;
        else {
            if (incl_trail)
                /* End extent just before first missing block */
                ret_value = diminfo->start + (count * diminfo->stride);
            else
                /* End extent at end of last block */
                ret_value = diminfo->start + ((count - (hsize_t)1) * diminfo->stride) + diminfo->block;
        } /* end else */
    }     /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__hyper_get_clip_extent_real() */

/*--------------------------------------------------------------------------
 NAME
    H5S_hyper_get_clip_extent
 PURPOSE
    Gets the extent a space should be clipped to in order to contain the
    same number of elements as another space
 USAGE
    hsize_t H5S__hyper_get_clip_extent(clip_space,match_space,incl_trail)
        const H5S_t *clip_space, IN: Space that clip size will be calculated based on
        const H5S_t *match_space, IN: Space containing the same number of elements as clip_space should after
clipping bool incl_trail;     IN: Whether to include trailing unselected space RETURNS Calculated clip
extent (never fails) DESCRIPTION Calculates and returns the extent that clip_space should be clipped to (via
H5S_hyper_clip_unlim) in order for it to contain the same number of elements as match_space.  If the clipped
selection would end immediately before a section of unselected space (i.e. at the end of a block), then if
incl_trail is true, the returned clip extent is selected to include that trailing "blank" space, otherwise it
is selected to end at the end before the blank space. GLOBAL VARIABLES COMMENTS, BUGS, ASSUMPTIONS Note this
assumes the offset has been normalized. EXAMPLES REVISION LOG
--------------------------------------------------------------------------*/
hsize_t
H5S_hyper_get_clip_extent(const H5S_t *clip_space, const H5S_t *match_space, bool incl_trail)
{
    hsize_t num_slices;    /* Number of slices in unlimited dimension */
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Check parameters */
    assert(clip_space);
    assert(match_space);
    assert(clip_space->select.sel_info.hslab->unlim_dim >= 0);

    /* Check for "none" match space */
    if (match_space->select.type->type == H5S_SEL_NONE)
        num_slices = (hsize_t)0;
    else {
        assert(match_space->select.type->type == H5S_SEL_HYPERSLABS);
        assert(match_space->select.sel_info.hslab);

        /* Calculate number of slices */
        num_slices = match_space->select.num_elem / clip_space->select.sel_info.hslab->num_elem_non_unlim;
        assert((match_space->select.num_elem % clip_space->select.sel_info.hslab->num_elem_non_unlim) == 0);
    } /* end else */

    /* Call "real" get_clip_extent function */
    ret_value = H5S__hyper_get_clip_extent_real(clip_space, num_slices, incl_trail);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_hyper_get_clip_extent() */

/*--------------------------------------------------------------------------
 NAME
    H5S_hyper_get_clip_extent_match
 PURPOSE
    Gets the extent a space should be clipped to in order to contain the
    same number of elements as another unlimited space that has been
    clipped to a different extent
 USAGE
    hsize_t H5S__hyper_get_clip_extent_match(clip_space,match_space,match_clip_size,incl_trail)
        const H5S_t *clip_space, IN: Space that clip size will be calculated based on
        const H5S_t *match_space, IN: Space that, after being clipped to match_clip_size, contains the same
number of elements as clip_space should after clipping hsize_t match_clip_size, IN: Extent match_space would
be clipped to to match the number of elements in clip_space bool incl_trail;     IN: Whether to include
trailing unselected space RETURNS Calculated clip extent (never fails) DESCRIPTION Calculates and returns the
extent that clip_space should be clipped to (via H5S_hyper_clip_unlim) in order for it to contain the same
number of elements as match_space would have after being clipped to match_clip_size.  If the clipped selection
would end immediately before a section of unselected space (i.e. at the end of a block), then if incl_trail is
true, the returned clip extent is selected to include that trailing "blank" space, otherwise it is selected to
end at the end before the blank space. GLOBAL VARIABLES COMMENTS, BUGS, ASSUMPTIONS Note this assumes the
offset has been normalized. EXAMPLES REVISION LOG
--------------------------------------------------------------------------*/
hsize_t
H5S_hyper_get_clip_extent_match(const H5S_t *clip_space, const H5S_t *match_space, hsize_t match_clip_size,
                                bool incl_trail)
{
    const H5S_hyper_dim_t
        *match_diminfo; /* Convenience pointer to opt_unlim_diminfo in unlimited dimension in match_space */
    hsize_t count;      /* Temporary count */
    hsize_t block;      /* Temporary block */
    hsize_t num_slices; /* Number of slices in unlimited dimension */
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Check parameters */
    assert(clip_space);
    assert(match_space);
    assert(clip_space->select.sel_info.hslab);
    assert(match_space->select.sel_info.hslab);
    assert(clip_space->select.sel_info.hslab->unlim_dim >= 0);
    assert(match_space->select.sel_info.hslab->unlim_dim >= 0);
    assert(clip_space->select.sel_info.hslab->num_elem_non_unlim ==
           match_space->select.sel_info.hslab->num_elem_non_unlim);

    match_diminfo =
        &match_space->select.sel_info.hslab->diminfo.opt[match_space->select.sel_info.hslab->unlim_dim];

    /* Get initial count and block */
    count = match_diminfo->count;
    block = match_diminfo->block;
    H5S__hyper_get_clip_diminfo(match_diminfo->start, match_diminfo->stride, &count, &block, match_clip_size);

    /* Calculate number of slices */
    /* Check for nothing returned */
    if ((block == 0) || (count == 0))
        num_slices = (hsize_t)0;
    /* Check for single block in unlimited dimension */
    else if (count == (hsize_t)1)
        num_slices = block;
    else {
        /* Calculate initial num_slices */
        num_slices = block * count;

        /* Check for partial last block */
        assert(match_clip_size >= match_diminfo->start);
        if (((match_diminfo->stride * (count - (hsize_t)1)) + block) >
            (match_clip_size - match_diminfo->start)) {
            /* Subtract slices missing from last block */
            assert((((match_diminfo->stride * (count - (hsize_t)1)) + block) -
                    (match_clip_size - match_diminfo->start)) < num_slices);
            num_slices -= ((match_diminfo->stride * (count - (hsize_t)1)) + block) -
                          (match_clip_size - match_diminfo->start);
        } /* end if */
    }     /* end else */

    /* Call "real" get_clip_extent function */
    ret_value = H5S__hyper_get_clip_extent_real(clip_space, num_slices, incl_trail);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_hyper_get_clip_extent_match() */

/*--------------------------------------------------------------------------
 NAME
    H5S_hyper_get_unlim_block
 PURPOSE
    Get the nth block in the unlimited dimension
 USAGE
    H5S_t *H5S_hyper_get_unlim_block(space,block_index)
        const H5S_t *space,     IN: Space with unlimited selection
        hsize_t block_index,    IN: Index of block to return in unlimited dimension
        bool incl_trail;     IN: Whether to include trailing unselected space
 RETURNS
    New space on success/NULL on failure.
 DESCRIPTION
    Returns a space containing only the block_indexth block in the
    unlimited dimension on space.  All blocks in all other dimensions are
    preserved.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Note this assumes the offset has been normalized.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5S_t *
H5S_hyper_get_unlim_block(const H5S_t *space, hsize_t block_index)
{
    H5S_hyper_sel_t *hslab;               /* Convenience pointer to hyperslab info */
    H5S_t           *space_out = NULL;    /* Dataspace to return */
    hsize_t          start[H5S_MAX_RANK]; /* Hyperslab selection info for unlim. selection */
    hsize_t          stride[H5S_MAX_RANK];
    hsize_t          count[H5S_MAX_RANK];
    hsize_t          block[H5S_MAX_RANK];
    unsigned         u;                /* Local index variable */
    H5S_t           *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Check parameters */
    assert(space);
    hslab = space->select.sel_info.hslab;
    assert(hslab);
    assert(hslab->unlim_dim >= 0);
    assert(hslab->diminfo.opt[hslab->unlim_dim].count == H5S_UNLIMITED);

    /* Set start to select block_indexth block in unlimited dimension and set
     * count to 1 in that dimension to only select that block.  Copy all other
     * diminfo parameters. */
    for (u = 0; u < space->extent.rank; u++) {
        if ((int)u == hslab->unlim_dim) {
            start[u] = hslab->diminfo.opt[u].start + (block_index * hslab->diminfo.opt[u].stride);
            count[u] = (hsize_t)1;
        } /* end if */
        else {
            start[u] = hslab->diminfo.opt[u].start;
            count[u] = hslab->diminfo.opt[u].count;
        } /* end else */
        stride[u] = hslab->diminfo.opt[u].stride;
        block[u]  = hslab->diminfo.opt[u].block;
    } /* end for */

    /* Create output space, copy extent */
    if (NULL == (space_out = H5S_create(H5S_SIMPLE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, NULL, "unable to create output dataspace");
    if (H5S__extent_copy_real(&space_out->extent, &space->extent, true) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, NULL, "unable to copy destination space extent");

    /* Select block as defined by start/stride/count/block computed above */
    if (H5S_select_hyperslab(space_out, H5S_SELECT_SET, start, stride, count, block) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, NULL, "can't select hyperslab");

    /* Set return value */
    ret_value = space_out;

done:
    /* Free space on error */
    if (!ret_value)
        if (space_out && H5S_close(space_out) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, NULL, "unable to release dataspace");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_hyper_get_unlim_block */

/*--------------------------------------------------------------------------
 NAME
    H5S_hyper_get_first_inc_block
 PURPOSE
    Get the index of the first incomplete block in the specified extent
 USAGE
    hsize_t H5S_hyper_get_first_inc_block(space,clip_size,partial)
        const H5S_t *space,     IN: Space with unlimited selection
        hsize_t clip_size,      IN: Extent space would be clipped to
        bool *partial;       OUT: Whether the ret_valueth block (first incomplete block) is partial
 RETURNS
    Index of first incomplete block in clip_size (never fails).
 DESCRIPTION
    Calculates and returns the index (as would be passed to
    H5S_hyper_get_unlim_block()) of the first block in the unlimited
    dimension of space which would be incomplete or missing when space is
    clipped to clip_size.  partial is set to true if the first incomplete
    block is partial, and false if the first incomplete block is missing.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Note this assumes the offset has been normalized.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hsize_t
H5S_hyper_get_first_inc_block(const H5S_t *space, hsize_t clip_size, bool *partial)
{
    H5S_hyper_sel_t *hslab;   /* Convenience pointer to hyperslab info */
    H5S_hyper_dim_t *diminfo; /* Convenience pointer to diminfo in unlimited dimension */
    hsize_t          ret_value = 0;

    FUNC_ENTER_NOAPI_NOERR

    /* Check parameters */
    assert(space);
    hslab = space->select.sel_info.hslab;
    assert(hslab);
    assert(hslab->unlim_dim >= 0);
    assert(hslab->diminfo.opt[hslab->unlim_dim].count == H5S_UNLIMITED);

    diminfo = &hslab->diminfo.opt[hslab->unlim_dim];

    /* Check for selection outside of clip_size */
    if (diminfo->start >= clip_size) {
        ret_value = 0;
        if (partial)
            partial = false;
    } /* end if */
    else {
        /* Calculate index of first incomplete block */
        ret_value = (clip_size - diminfo->start + diminfo->stride - diminfo->block) / diminfo->stride;

        if (partial) {
            /* Check for partial block */
            if ((diminfo->stride * ret_value) < (clip_size - diminfo->start))
                *partial = true;
            else
                *partial = false;
        } /* end if */
    }     /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_hyper_get_first_inc_block */

/*--------------------------------------------------------------------------
 NAME
    H5Sis_regular_hyperslab
 PURPOSE
    Determine if a hyperslab selection is regular
 USAGE
    htri_t H5Sis_regular_hyperslab(dsid)
        hid_t dsid;             IN: Dataspace ID of hyperslab selection to query
 RETURNS
    true/false for hyperslab selection, FAIL on error or when querying other
    selection types.
 DESCRIPTION
    If a hyperslab can be represented as a single call to H5Sselect_hyperslab,
    with the H5S_SELECT_SET option, it is regular.  If the hyperslab selection
    would require multiple calls to H5Sselect_hyperslab, it is irregular.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5Sis_regular_hyperslab(hid_t spaceid)
{
    H5S_t *space;     /* Dataspace to query */
    htri_t ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "i", spaceid);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (H5S_GET_SELECT_TYPE(space) != H5S_SEL_HYPERSLABS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a hyperslab selection");

    ret_value = H5S__hyper_is_regular(space);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sis_regular_hyperslab() */

/*--------------------------------------------------------------------------
 NAME
    H5Sget_regular_hyperslab
 PURPOSE
    Retrieve a regular hyperslab selection
 USAGE
    herr_t H5Sget_regular_hyperslab(dsid, start, stride, block, count)
        hid_t dsid;             IN: Dataspace ID of hyperslab selection to query
        hsize_t start[];        OUT: Offset of start of hyperslab
        hsize_t stride[];       OUT: Hyperslab stride
        hsize_t count[];        OUT: Number of blocks included in hyperslab
        hsize_t block[];        OUT: Size of block in hyperslab
 RETURNS
    Non-negative on success/Negative on failure.  (It is an error to query
    the regular hyperslab selections for non-regular hyperslab selections)
 DESCRIPTION
    Retrieve the start/stride/count/block for a regular hyperslab selection.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Note that if a hyperslab is originally regular, then becomes irregular
    through selection operations, and then becomes regular again, the new
    final regular selection may be equivalent but not identical to the
    original regular selection.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Sget_regular_hyperslab(hid_t spaceid, hsize_t start[] /*out*/, hsize_t stride[] /*out*/,
                         hsize_t count[] /*out*/, hsize_t block[] /*out*/)
{
    H5S_t   *space;               /* Dataspace to query */
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "ixxxx", spaceid, start, stride, count, block);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (H5S_GET_SELECT_TYPE(space) != H5S_SEL_HYPERSLABS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a hyperslab selection");
    if (true != H5S__hyper_is_regular(space))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a regular hyperslab selection");

    /* Retrieve hyperslab parameters */
    if (start)
        for (u = 0; u < space->extent.rank; u++)
            start[u] = space->select.sel_info.hslab->diminfo.app[u].start;
    if (stride)
        for (u = 0; u < space->extent.rank; u++)
            stride[u] = space->select.sel_info.hslab->diminfo.app[u].stride;
    if (count)
        for (u = 0; u < space->extent.rank; u++)
            count[u] = space->select.sel_info.hslab->diminfo.app[u].count;
    if (block)
        for (u = 0; u < space->extent.rank; u++)
            block[u] = space->select.sel_info.hslab->diminfo.app[u].block;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sget_regular_hyperslab() */
