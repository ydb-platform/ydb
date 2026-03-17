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
 * Purpose:     v2 B-tree testing functions
 *
 */

/****************/
/* Module Setup */
/****************/

#include "H5B2module.h" /* This source code file is part of the H5B2 module */
#define H5B2_TESTING    /*suppress warning about H5B2 testing funcs*/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata Cache                           */
#include "H5B2pkg.h"     /* B-Trees (Version 2)                      */
#include "H5Eprivate.h"  /* Error Handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5FLprivate.h" /* Free Lists                               */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* v2 B-tree client callback context */
typedef struct H5B2_test_ctx_t {
    uint8_t sizeof_size; /* Size of file sizes */
} H5B2_test_ctx_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* v2 B-tree driver callbacks for 'test' B-trees */
static void  *H5B2__test_crt_context(void *udata);
static herr_t H5B2__test_dst_context(void *ctx);
static herr_t H5B2__test_store(void *nrecord, const void *udata);
static herr_t H5B2__test_compare(const void *rec1, const void *rec2, int *result);
static herr_t H5B2__test_encode(uint8_t *raw, const void *nrecord, void *ctx);
static herr_t H5B2__test_decode(const uint8_t *raw, void *nrecord, void *ctx);
static herr_t H5B2__test_debug(FILE *stream, int indent, int fwidth, const void *record, const void *_udata);

/* v2 B-tree driver callbacks for 'test2' B-trees */
static herr_t H5B2__test2_store(void *nrecord, const void *udata);
static herr_t H5B2__test2_compare(const void *rec1, const void *rec2, int *result);
static herr_t H5B2__test2_encode(uint8_t *raw, const void *nrecord, void *ctx);
static herr_t H5B2__test2_decode(const uint8_t *raw, void *nrecord, void *ctx);
static herr_t H5B2__test2_debug(FILE *stream, int indent, int fwidth, const void *record, const void *_udata);

/*********************/
/* Package Variables */
/*********************/

/* Class structure for testing simple B-tree records */
const H5B2_class_t H5B2_TEST[1] = {{
    /* B-tree class information */
    H5B2_TEST_ID,           /* Type of B-tree */
    "H5B2_TEST_ID",         /* Name of B-tree class */
    sizeof(hsize_t),        /* Size of native record */
    H5B2__test_crt_context, /* Create client callback context */
    H5B2__test_dst_context, /* Destroy client callback context */
    H5B2__test_store,       /* Record storage callback */
    H5B2__test_compare,     /* Record comparison callback */
    H5B2__test_encode,      /* Record encoding callback */
    H5B2__test_decode,      /* Record decoding callback */
    H5B2__test_debug        /* Record debugging callback */
}};

/* Class structure for testing key/value B-tree records */
const H5B2_class_t H5B2_TEST2[1] = {{
    /* B-tree class information */
    H5B2_TEST2_ID,           /* Type of B-tree */
    "H5B2_TEST2_ID",         /* Name of B-tree class */
    sizeof(H5B2_test_rec_t), /* Size of native record */
    H5B2__test_crt_context,  /* Create client callback context */
    H5B2__test_dst_context,  /* Destroy client callback context */
    H5B2__test2_store,       /* Record storage callback */
    H5B2__test2_compare,     /* Record comparison callback */
    H5B2__test2_encode,      /* Record encoding callback */
    H5B2__test2_decode,      /* Record decoding callback */
    H5B2__test2_debug        /* Record debugging callback */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5B2_test_ctx_t struct */
H5FL_DEFINE_STATIC(H5B2_test_ctx_t);

/*-------------------------------------------------------------------------
 * Function:	H5B2__test_crt_context
 *
 * Purpose:	Create client callback context
 *
 * Return:	Success:	non-NULL
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5B2__test_crt_context(void *_f)
{
    H5F_t           *f = (H5F_t *)_f;  /* User data for building callback context */
    H5B2_test_ctx_t *ctx;              /* Callback context structure */
    void            *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);

    /* Allocate callback context */
    if (NULL == (ctx = H5FL_MALLOC(H5B2_test_ctx_t)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, NULL, "can't allocate callback context");

    /* Determine the size of lengths in the file */
    ctx->sizeof_size = H5F_SIZEOF_SIZE(f);

    /* Set return value */
    ret_value = ctx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__test_crt_context() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test_dst_context
 *
 * Purpose:	Destroy client callback context
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test_dst_context(void *_ctx)
{
    H5B2_test_ctx_t *ctx = (H5B2_test_ctx_t *)_ctx; /* Callback context structure */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Release callback context */
    ctx = H5FL_FREE(H5B2_test_ctx_t, ctx);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test_dst_context() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test_store
 *
 * Purpose:	Store native information into record for B-tree
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test_store(void *nrecord, const void *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(hsize_t *)nrecord = *(const hsize_t *)udata;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test_store() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test_compare
 *
 * Purpose:	Compare two native information records, according to some key
 *
 * Return:	<0 if rec1 < rec2
 *              =0 if rec1 == rec2
 *              >0 if rec1 > rec2
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test_compare(const void *rec1, const void *rec2, int *result)
{
    FUNC_ENTER_PACKAGE_NOERR

    *result = (int)(*(const hssize_t *)rec1 - *(const hssize_t *)rec2);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test_compare() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test_encode
 *
 * Purpose:	Encode native information into raw form for storing on disk
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test_encode(uint8_t *raw, const void *nrecord, void *_ctx)
{
    H5B2_test_ctx_t *ctx = (H5B2_test_ctx_t *)_ctx; /* Callback context structure */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    H5_ENCODE_LENGTH_LEN(raw, *(const hsize_t *)nrecord, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test_decode
 *
 * Purpose:	Decode raw disk form of record into native form
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test_decode(const uint8_t *raw, void *nrecord, void *_ctx)
{
    H5B2_test_ctx_t *ctx = (H5B2_test_ctx_t *)_ctx; /* Callback context structure */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    H5_DECODE_LENGTH_LEN(raw, *(hsize_t *)nrecord, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test_debug
 *
 * Purpose:	Debug native form of record
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test_debug(FILE *stream, int indent, int fwidth, const void *record, const void H5_ATTR_UNUSED *_udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(record);

    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth, "Record:", *(const hsize_t *)record);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test2_store
 *
 * Purpose:	Store native information into record for B-tree
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test2_store(void *nrecord, const void *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(H5B2_test_rec_t *)nrecord = *(const H5B2_test_rec_t *)udata;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test2_store() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test2_compare
 *
 * Purpose:	Compare two native information records, according to some key
 *
 * Return:	<0 if rec1 < rec2
 *              =0 if rec1 == rec2
 *              >0 if rec1 > rec2
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test2_compare(const void *rec1, const void *rec2, int *result)
{
    FUNC_ENTER_PACKAGE_NOERR

    *result = (int)(((const H5B2_test_rec_t *)rec1)->key - ((const H5B2_test_rec_t *)rec2)->key);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test2_compare() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test2_encode
 *
 * Purpose:	Encode native information into raw form for storing on disk
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test2_encode(uint8_t *raw, const void *nrecord, void *_ctx)
{
    H5B2_test_ctx_t *ctx = (H5B2_test_ctx_t *)_ctx; /* Callback context structure */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    H5_ENCODE_LENGTH_LEN(raw, ((const H5B2_test_rec_t *)nrecord)->key, ctx->sizeof_size);
    H5_ENCODE_LENGTH_LEN(raw, ((const H5B2_test_rec_t *)nrecord)->val, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test2_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test2_decode
 *
 * Purpose:	Decode raw disk form of record into native form
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test2_decode(const uint8_t *raw, void *nrecord, void *_ctx)
{
    H5B2_test_ctx_t *ctx = (H5B2_test_ctx_t *)_ctx; /* Callback context structure */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    H5_DECODE_LENGTH_LEN(raw, ((H5B2_test_rec_t *)nrecord)->key, ctx->sizeof_size);
    H5_DECODE_LENGTH_LEN(raw, ((H5B2_test_rec_t *)nrecord)->val, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test2_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__test2_debug
 *
 * Purpose:	Debug native form of record
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B2__test2_debug(FILE *stream, int indent, int fwidth, const void *record, const void H5_ATTR_UNUSED *_udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(record);

    fprintf(stream, "%*s%-*s (%" PRIuHSIZE ", %" PRIuHSIZE ")\n", indent, "", fwidth,
            "Record:", ((const H5B2_test_rec_t *)record)->key, ((const H5B2_test_rec_t *)record)->val);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__test2_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__get_root_addr_test
 *
 * Purpose:     Retrieve the root node's address
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__get_root_addr_test(H5B2_t *bt2, haddr_t *root_addr)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(bt2);
    assert(root_addr);

    /* Get B-tree root addr */
    *root_addr = bt2->hdr->root.addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2__get_root_addr_test() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__get_node_info_test
 *
 * Purpose:     Determine information about a node holding a record in the B-tree
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__get_node_info_test(H5B2_t *bt2, void *udata, H5B2_node_info_test_t *ninfo)
{
    H5B2_hdr_t     *hdr;                 /* Pointer to the B-tree header */
    H5B2_node_ptr_t curr_node_ptr;       /* Node pointer info for current node */
    void           *parent = NULL;       /* Parent of current node */
    uint16_t        depth;               /* Current depth of the tree */
    int             cmp;                 /* Comparison value of records */
    unsigned        idx;                 /* Location of record which matches key */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(bt2);

    /* Set the shared v2 B-tree header's file context for this operation */
    bt2->hdr->f = bt2->f;

    /* Get the v2 B-tree header */
    hdr = bt2->hdr;

    /* Make copy of the root node pointer to start search with */
    curr_node_ptr = hdr->root;

    /* Set initial parent, if doing swmr writes */
    if (hdr->swmr_write)
        parent = hdr;

    /* Current depth of the tree */
    depth = hdr->depth;

    /* Check for empty tree */
    if (0 == curr_node_ptr.node_nrec)
        HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "B-tree has no records");

    /* Walk down B-tree to find record or leaf node where record is located */
    cmp = -1;
    while (depth > 0 && cmp != 0) {
        H5B2_internal_t *internal;      /* Pointer to internal node in B-tree */
        H5B2_node_ptr_t  next_node_ptr; /* Node pointer info for next node */

        /* Lock B-tree current node */
        if (NULL == (internal = H5B2__protect_internal(hdr, parent, &curr_node_ptr, depth, false,
                                                       H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to load B-tree internal node");

        /* Unpin parent if necessary */
        if (parent) {
            if (parent != hdr && H5AC_unpin_entry(parent) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPIN, FAIL, "unable to unpin parent entry");
            parent = NULL;
        } /* end if */

        /* Locate node pointer for child */
        if (H5B2__locate_record(hdr->cls, internal->nrec, hdr->nat_off, internal->int_native, udata, &idx,
                                &cmp) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");

        if (cmp > 0)
            idx++;

        if (cmp != 0) {
            /* Get node pointer for next node to search */
            next_node_ptr = internal->node_ptrs[idx];

            /* Unlock current node */
            if (H5AC_unprotect(hdr->f, H5AC_BT2_INT, curr_node_ptr.addr, internal,
                               (unsigned)(hdr->swmr_write ? H5AC__PIN_ENTRY_FLAG : H5AC__NO_FLAGS_SET)) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");

            /* Keep track of parent if necessary */
            if (hdr->swmr_write)
                parent = internal;

            /* Set pointer to next node to load */
            curr_node_ptr = next_node_ptr;
        } /* end if */
        else {
            /* Unlock current node */
            if (H5AC_unprotect(hdr->f, H5AC_BT2_INT, curr_node_ptr.addr, internal, H5AC__NO_FLAGS_SET) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");

            /* Fill in information about the node */
            ninfo->depth = depth;
            ninfo->nrec  = curr_node_ptr.node_nrec;

            /* Indicate success */
            HGOTO_DONE(SUCCEED);
        } /* end else */

        /* Decrement depth we're at in B-tree */
        depth--;
    } /* end while */

    {
        H5B2_leaf_t *leaf; /* Pointer to leaf node in B-tree */

        /* Lock B-tree leaf node */
        if (NULL == (leaf = H5B2__protect_leaf(hdr, parent, &curr_node_ptr, false, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");

        /* Unpin parent if necessary */
        if (parent) {
            if (parent != hdr && H5AC_unpin_entry(parent) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPIN, FAIL, "unable to unpin parent entry");
            parent = NULL;
        } /* end if */

        /* Locate record */
        if (H5B2__locate_record(hdr->cls, leaf->nrec, hdr->nat_off, leaf->leaf_native, udata, &idx, &cmp) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTCOMPARE, FAIL, "can't compare btree2 records");

        /* Unlock current node */
        if (H5AC_unprotect(hdr->f, H5AC_BT2_LEAF, curr_node_ptr.addr, leaf, H5AC__NO_FLAGS_SET) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");

        /* Indicate the depth that the record was found */
        if (cmp != 0)
            HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "record not in B-tree");
    } /* end block */

    /* Fill in information about the leaf node */
    ninfo->depth = depth;
    ninfo->nrec  = curr_node_ptr.node_nrec;

done:
    if (parent) {
        assert(ret_value < 0);
        if (parent != hdr && H5AC_unpin_entry(parent) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPIN, FAIL, "unable to unpin parent entry");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__get_node_info_test() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__get_node_depth_test
 *
 * Purpose:     Determine the depth of a node holding a record in the B-tree
 *
 * Note:        Just a simple wrapper around the H5B2__get_node_info_test() routine
 *
 * Return:      Success:    Non-negative depth of the node where the record
 *                          was found
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5B2__get_node_depth_test(H5B2_t *bt2, void *udata)
{
    H5B2_node_info_test_t ninfo;          /* Node information */
    int                   ret_value = -1; /* Return information */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(bt2);

    /* Get information abou the node */
    if (H5B2__get_node_info_test(bt2, udata, &ninfo) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, (-1), "error looking up node info");

    /* Set return value */
    ret_value = (int)ninfo.depth;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2__get_node_depth_test() */
