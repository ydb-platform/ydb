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
 * Created:		H5Gdense.c
 *
 * Purpose:		Routines for operating on "dense" link storage for a
 *                      group in a file.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Gmodule.h" /* This source code file is part of the H5G module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5WBprivate.h" /* Wrapped Buffers                      */

/****************/
/* Local Macros */
/****************/

/* Fractal heap creation parameters for "dense" link storage */
#define H5G_FHEAP_MAN_WIDTH            4
#define H5G_FHEAP_MAN_START_BLOCK_SIZE 512
#define H5G_FHEAP_MAN_MAX_DIRECT_SIZE  (64 * 1024)
#define H5G_FHEAP_MAN_MAX_INDEX        32
#define H5G_FHEAP_MAN_START_ROOT_ROWS  1
#define H5G_FHEAP_CHECKSUM_DBLOCKS     true
#define H5G_FHEAP_MAX_MAN_SIZE         (4 * 1024)

/* v2 B-tree creation macros for 'name' field index */
#define H5G_NAME_BT2_NODE_SIZE  512
#define H5G_NAME_BT2_MERGE_PERC 40
#define H5G_NAME_BT2_SPLIT_PERC 100

/* v2 B-tree creation macros for 'corder' field index */
#define H5G_CORDER_BT2_NODE_SIZE  512
#define H5G_CORDER_BT2_MERGE_PERC 40
#define H5G_CORDER_BT2_SPLIT_PERC 100

/* Size of stack buffer for serialized link */
#define H5G_LINK_BUF_SIZE 128

/******************/
/* Local Typedefs */
/******************/

/* Data exchange structure to use when building table of links in group */
typedef struct {
    H5G_link_table_t *ltable;   /* Pointer to link table to build */
    size_t            curr_lnk; /* Current link to operate on */
} H5G_dense_bt_ud_t;

/*
 * Data exchange structure to pass through the v2 B-tree layer for the
 * H5B2_iterate function when iterating over densely stored links.
 */
typedef struct {
    /* downward (internal) */
    H5F_t  *f;     /* Pointer to file that fractal heap is in */
    H5HF_t *fheap; /* Fractal heap handle               */
    hsize_t count; /* # of links examined               */

    /* downward (from application) */
    hsize_t           skip;    /* Number of links to skip           */
    H5G_lib_iterate_t op;      /* Callback for each link            */
    void             *op_data; /* Callback data for each link       */

    /* upward */
    int op_ret; /* Return value from callback        */
} H5G_bt2_ud_it_t;

/*
 * Data exchange structure to pass through the fractal heap layer for the
 * H5HF_op function when iterating over densely stored links.
 */
typedef struct {
    /* downward (internal) */
    H5F_t *f; /* Pointer to file that fractal heap is in */

    /* upward */
    H5O_link_t *lnk; /* Copy of link                      */
} H5G_fh_ud_it_t;

/*
 * Data exchange structure for dense link storage.  This structure is
 * passed through the v2 B-tree layer when removing links.
 */
typedef struct {
    /* downward */
    H5G_bt2_ud_common_t common;          /* Common info for B-tree user data (must be first) */
    bool                rem_from_fheap;  /* Whether to remove the link from the fractal heap */
    haddr_t             corder_bt2_addr; /* Address of v2 B-tree indexing creation order */
    H5RS_str_t         *grp_full_path_r; /* Full path of group where link is removed */
    bool                replace_names;   /* Whether to replace the names of open objects */
} H5G_bt2_ud_rm_t;

/*
 * Data exchange structure to pass through the fractal heap layer for the
 * H5HF_op function when removing a link from densely stored links.
 */
typedef struct {
    /* downward */
    H5F_t      *f;               /* Pointer to file that fractal heap is in */
    haddr_t     corder_bt2_addr; /* Address of v2 B-tree indexing creation order */
    H5RS_str_t *grp_full_path_r; /* Full path of group where link is removed */
    bool        replace_names;   /* Whether to replace the names of open objects */
} H5G_fh_ud_rm_t;

/*
 * Data exchange structure for dense link storage.  This structure is
 * passed through the v2 B-tree layer when removing links by index.
 */
typedef struct {
    /* downward */
    H5F_t      *f;               /* Pointer to file that fractal heap is in */
    H5HF_t     *fheap;           /* Fractal heap handle               */
    H5_index_t  idx_type;        /* Primary index for removing link */
    haddr_t     other_bt2_addr;  /* Address of "other" v2 B-tree indexing link */
    H5RS_str_t *grp_full_path_r; /* Full path of group where link is removed */
} H5G_bt2_ud_rmbi_t;

/*
 * Data exchange structure to pass through the fractal heap layer for the
 * H5HF_op function when removing a link from densely stored links by index.
 */
typedef struct {
    /* downward */
    H5F_t *f; /* Pointer to file that fractal heap is in */

    /* upward */
    H5O_link_t *lnk; /* Pointer to link to remove */
} H5G_fh_ud_rmbi_t;

/*
 * Data exchange structure to pass through the v2 B-tree layer for the
 * H5B2_index function when retrieving the name of a link by index.
 */
typedef struct {
    /* downward (internal) */
    H5F_t  *f;     /* Pointer to file that fractal heap is in */
    H5HF_t *fheap; /* Fractal heap handle               */

    /* downward (from application) */
    char  *name;      /* Name buffer to fill               */
    size_t name_size; /* Size of name buffer to fill       */

    /* upward */
    size_t name_len; /* Full length of name               */
} H5G_bt2_ud_gnbi_t;

/*
 * Data exchange structure to pass through the fractal heap layer for the
 * H5HF_op function when retrieving the name of a link by index.
 */
typedef struct {
    /* downward (internal) */
    H5F_t *f; /* Pointer to file that fractal heap is in */

    /* downward (from application) */
    char  *name;      /* Name buffer to fill               */
    size_t name_size; /* Size of name buffer to fill       */

    /* upward */
    size_t name_len; /* Full length of name               */
} H5G_fh_ud_gnbi_t;

/*
 * Data exchange structure to pass through the v2 B-tree layer for the
 * H5B2_index function when retrieving a link by index.
 */
typedef struct {
    /* downward (internal) */
    H5F_t  *f;     /* Pointer to file that fractal heap is in */
    H5HF_t *fheap; /* Fractal heap handle               */

    /* upward */
    H5O_link_t *lnk; /* Pointer to link                   */
} H5G_bt2_ud_lbi_t;

/*
 * Data exchange structure to pass through the fractal heap layer for the
 * H5HF_op function when retrieving a link by index.
 */
typedef struct {
    /* downward (internal) */
    H5F_t *f; /* Pointer to file that fractal heap is in */

    /* upward */
    H5O_link_t *lnk; /* Pointer to link                   */
} H5G_fh_ud_lbi_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_create
 *
 * Purpose:	Creates dense link storage structures for a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_create(H5F_t *f, H5O_linfo_t *linfo, const H5O_pline_t *pline)
{
    H5HF_create_t fheap_cparam;        /* Fractal heap creation parameters */
    H5B2_create_t bt2_cparam;          /* v2 B-tree creation parameters */
    H5HF_t       *fheap      = NULL;   /* Fractal heap handle */
    H5B2_t       *bt2_name   = NULL;   /* v2 B-tree handle for names */
    H5B2_t       *bt2_corder = NULL;   /* v2 B-tree handle for creation order */
    size_t        fheap_id_len;        /* Fractal heap ID length */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(linfo);

    /* Set fractal heap creation parameters */
    /* XXX: Give some control of these to applications? */
    memset(&fheap_cparam, 0, sizeof(fheap_cparam));
    fheap_cparam.managed.width            = H5G_FHEAP_MAN_WIDTH;
    fheap_cparam.managed.start_block_size = H5G_FHEAP_MAN_START_BLOCK_SIZE;
    fheap_cparam.managed.max_direct_size  = H5G_FHEAP_MAN_MAX_DIRECT_SIZE;
    fheap_cparam.managed.max_index        = H5G_FHEAP_MAN_MAX_INDEX;
    fheap_cparam.managed.start_root_rows  = H5G_FHEAP_MAN_START_ROOT_ROWS;
    fheap_cparam.checksum_dblocks         = H5G_FHEAP_CHECKSUM_DBLOCKS;
    fheap_cparam.max_man_size             = H5G_FHEAP_MAX_MAN_SIZE;
    if (pline)
        fheap_cparam.pline = *pline;

    /* Create fractal heap for storing links */
    if (NULL == (fheap = H5HF_create(f, &fheap_cparam)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create fractal heap");

    /* Retrieve the heap's address in the file */
    if (H5HF_get_heap_addr(fheap, &(linfo->fheap_addr)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get fractal heap address");

    /* Retrieve the heap's ID length in the file */
    if (H5HF_get_id_len(fheap, &fheap_id_len) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGETSIZE, FAIL, "can't get fractal heap ID length");
    assert(fheap_id_len == H5G_DENSE_FHEAP_ID_LEN);

    /* Create the name index v2 B-tree */
    memset(&bt2_cparam, 0, sizeof(bt2_cparam));
    bt2_cparam.cls       = H5G_BT2_NAME;
    bt2_cparam.node_size = (size_t)H5G_NAME_BT2_NODE_SIZE;
    H5_CHECK_OVERFLOW(fheap_id_len, /* From: */ hsize_t, /* To: */ uint32_t);
    bt2_cparam.rrec_size = 4 +                     /* Name's hash value */
                           (uint32_t)fheap_id_len; /* Fractal heap ID */
    bt2_cparam.split_percent = H5G_NAME_BT2_SPLIT_PERC;
    bt2_cparam.merge_percent = H5G_NAME_BT2_MERGE_PERC;
    if (NULL == (bt2_name = H5B2_create(f, &bt2_cparam, NULL)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create v2 B-tree for name index");

    /* Retrieve the v2 B-tree's address in the file */
    if (H5B2_get_addr(bt2_name, &(linfo->name_bt2_addr)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get v2 B-tree address for name index");

    /* Check if we should create a creation order index v2 B-tree */
    if (linfo->index_corder) {
        /* Create the creation order index v2 B-tree */
        memset(&bt2_cparam, 0, sizeof(bt2_cparam));
        bt2_cparam.cls       = H5G_BT2_CORDER;
        bt2_cparam.node_size = (size_t)H5G_CORDER_BT2_NODE_SIZE;
        H5_CHECK_OVERFLOW(fheap_id_len, /* From: */ hsize_t, /* To: */ uint32_t);
        bt2_cparam.rrec_size = 8 +                     /* Creation order value */
                               (uint32_t)fheap_id_len; /* Fractal heap ID */
        bt2_cparam.split_percent = H5G_CORDER_BT2_SPLIT_PERC;
        bt2_cparam.merge_percent = H5G_CORDER_BT2_MERGE_PERC;
        if (NULL == (bt2_corder = H5B2_create(f, &bt2_cparam, NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create v2 B-tree for creation order index");

        /* Retrieve the v2 B-tree's address in the file */
        if (H5B2_get_addr(bt2_corder, &(linfo->corder_bt2_addr)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get v2 B-tree address for creation order index");
    } /* end if */

done:
    /* Close the open objects */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");
    if (bt2_corder && H5B2_close(bt2_corder) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for creation order index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_create() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_insert
 *
 * Purpose:	Insert a link into the  dense link storage structures for a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_insert(H5F_t *f, const H5O_linfo_t *linfo, const H5O_link_t *lnk)
{
    H5G_bt2_ud_ins_t udata;                       /* User data for v2 B-tree insertion */
    H5HF_t          *fheap      = NULL;           /* Fractal heap handle */
    H5B2_t          *bt2_name   = NULL;           /* v2 B-tree handle for name index */
    H5B2_t          *bt2_corder = NULL;           /* v2 B-tree handle for creation order index */
    size_t           link_size;                   /* Size of serialized link in the heap */
    H5WB_t          *wb = NULL;                   /* Wrapped buffer for link data */
    uint8_t          link_buf[H5G_LINK_BUF_SIZE]; /* Buffer for serializing link */
    void            *link_ptr  = NULL;            /* Pointer to serialized link */
    herr_t           ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(linfo);
    assert(lnk);

    /* Find out the size of buffer needed for serialized link */
    if ((link_size = H5O_msg_raw_size(f, H5O_LINK_ID, false, lnk)) == 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGETSIZE, FAIL, "can't get link size");

    /* Wrap the local buffer for serialized link */
    if (NULL == (wb = H5WB_wrap(link_buf, sizeof(link_buf))))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't wrap buffer");

    /* Get a pointer to a buffer that's large enough for link */
    if (NULL == (link_ptr = H5WB_actual(wb, link_size)))
        HGOTO_ERROR(H5E_SYM, H5E_NOSPACE, FAIL, "can't get actual buffer");

    /* Create serialized form of link */
    if (H5O_msg_encode(f, H5O_LINK_ID, false, (unsigned char *)link_ptr, lnk) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTENCODE, FAIL, "can't encode link");

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, linfo->fheap_addr)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Insert the serialized link into the fractal heap */
    if (H5HF_insert(fheap, link_size, link_ptr, udata.id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert link into fractal heap");

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(f, linfo->name_bt2_addr, NULL)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Create the callback information for v2 B-tree record insertion */
    udata.common.f             = f;
    udata.common.fheap         = fheap;
    udata.common.name          = lnk->name;
    udata.common.name_hash     = H5_checksum_lookup3(lnk->name, strlen(lnk->name), 0);
    udata.common.corder        = lnk->corder;
    udata.common.found_op      = NULL;
    udata.common.found_op_data = NULL;
    /* udata.id already set in H5HF_insert() call */

    /* Insert link into 'name' tracking v2 B-tree */
    if (H5B2_insert(bt2_name, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert record into v2 B-tree");

    /* Check if we should create a creation order index v2 B-tree record */
    if (linfo->index_corder) {
        /* Open the creation order index v2 B-tree */
        assert(H5_addr_defined(linfo->corder_bt2_addr));
        if (NULL == (bt2_corder = H5B2_open(f, linfo->corder_bt2_addr, NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for creation order index");

        /* Insert the record into the creation order index v2 B-tree */
        if (H5B2_insert(bt2_corder, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert record into v2 B-tree");
    } /* end if */

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");
    if (bt2_corder && H5B2_close(bt2_corder) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for creation order index");
    if (wb && H5WB_unwrap(wb) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close wrapped buffer");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_insert() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_lookup_cb
 *
 * Purpose:	Callback when a link is located in an index
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_lookup_cb(const void *_lnk, void *_user_lnk)
{
    const H5O_link_t *lnk       = (const H5O_link_t *)_lnk; /* Record from B-tree */
    H5O_link_t       *user_lnk  = (H5O_link_t *)_user_lnk;  /* User data from v2 B-tree link lookup */
    herr_t            ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(lnk);
    assert(user_lnk);

    /* Copy link information */
    if (H5O_msg_copy(H5O_LINK_ID, lnk, user_lnk) == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "can't copy link message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_lookup_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_lookup
 *
 * Purpose:	Look up a link within a group that uses dense link storage
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_lookup(H5F_t *f, const H5O_linfo_t *linfo, const char *name, bool *found, H5O_link_t *lnk)
{
    H5G_bt2_ud_common_t udata;               /* User data for v2 B-tree link lookup */
    H5HF_t             *fheap     = NULL;    /* Fractal heap handle */
    H5B2_t             *bt2_name  = NULL;    /* v2 B-tree handle for name index */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(linfo);
    assert(name && *name);
    assert(found);
    assert(lnk);

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, linfo->fheap_addr)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(f, linfo->name_bt2_addr, NULL)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Construct the user data for v2 B-tree callback */
    udata.f             = f;
    udata.fheap         = fheap;
    udata.name          = name;
    udata.name_hash     = H5_checksum_lookup3(name, strlen(name), 0);
    udata.found_op      = H5G__dense_lookup_cb; /* v2 B-tree comparison callback */
    udata.found_op_data = lnk;

    /* Find & copy the named link in the 'name' index */
    if (H5B2_find(bt2_name, &udata, found, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to locate link in name index");

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_lookup() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_lookup_by_idx_fh_cb
 *
 * Purpose:	Callback for fractal heap operator, to make copy of link when
 *              when lookup up a link by index
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_lookup_by_idx_fh_cb(const void *obj, size_t obj_len, void *_udata)
{
    H5G_fh_ud_lbi_t *udata     = (H5G_fh_ud_lbi_t *)_udata; /* User data for fractal heap 'op' callback */
    H5O_link_t      *tmp_lnk   = NULL;                      /* Temporary pointer to link */
    herr_t           ret_value = SUCCEED;                   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Decode link information & keep a copy */
    if (NULL == (tmp_lnk = (H5O_link_t *)H5O_msg_decode(udata->f, NULL, H5O_LINK_ID, obj_len,
                                                        (const unsigned char *)obj)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, FAIL, "can't decode link");

    /* Copy link information */
    if (NULL == H5O_msg_copy(H5O_LINK_ID, tmp_lnk, udata->lnk))
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "can't copy link message");

done:
    /* Release the space allocated for the link */
    if (tmp_lnk)
        H5O_msg_free(H5O_LINK_ID, tmp_lnk);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_lookup_by_idx_fh_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_lookup_by_idx_bt2_cb
 *
 * Purpose:	v2 B-tree callback for dense link storage lookup by index
 *
 * Return:	H5_ITER_ERROR/H5_ITER_CONT/H5_ITER_STOP
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_lookup_by_idx_bt2_cb(const void *_record, void *_bt2_udata)
{
    const H5G_dense_bt2_name_rec_t *record    = (const H5G_dense_bt2_name_rec_t *)_record;
    H5G_bt2_ud_lbi_t               *bt2_udata = (H5G_bt2_ud_lbi_t *)_bt2_udata; /* User data for callback */
    H5G_fh_ud_lbi_t                 fh_udata;                 /* User data for fractal heap 'op' callback */
    int                             ret_value = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Prepare user data for callback */
    /* down */
    fh_udata.f   = bt2_udata->f;
    fh_udata.lnk = bt2_udata->lnk;

    /* Call fractal heap 'op' routine, to copy the link information */
    if (H5HF_op(bt2_udata->fheap, record->id, H5G__dense_lookup_by_idx_fh_cb, &fh_udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPERATE, H5_ITER_ERROR, "link found callback failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_lookup_by_idx_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_lookup_by_idx
 *
 * Purpose:	Look up a link within a group that uses dense link storage,
 *              according to the order of an index
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_lookup_by_idx(H5F_t *f, const H5O_linfo_t *linfo, H5_index_t idx_type, H5_iter_order_t order,
                         hsize_t n, H5O_link_t *lnk)
{
    H5HF_t          *fheap  = NULL;       /* Fractal heap handle */
    H5G_link_table_t ltable = {0, NULL};  /* Table of links */
    H5B2_t          *bt2    = NULL;       /* v2 B-tree handle for index */
    haddr_t          bt2_addr;            /* Address of v2 B-tree to use for lookup */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(linfo);
    assert(lnk);

    /* Determine the address of the index to use */
    if (idx_type == H5_INDEX_NAME) {
        /* Since names are hashed, getting them in strictly increasing or
         *      decreasing order requires building a table and sorting it.
         *      If the order is native, use the B-tree for names.
         */
        bt2_addr = HADDR_UNDEF;
    } /* end if */
    else {
        assert(idx_type == H5_INDEX_CRT_ORDER);

        /* This address may not be defined if creation order is tracked, but
         *      there's no index on it.  If there's no v2 B-tree that indexes
         *      the links and the order is native, use the B-tree for names.
         *      Otherwise, build a table.
         */
        bt2_addr = linfo->corder_bt2_addr;
    } /* end else */

    /* If the order is native and there's no B-tree for indexing the links,
     * use the B-tree for names instead of building a table to speed up the
     * process.
     */
    if (order == H5_ITER_NATIVE && !H5_addr_defined(bt2_addr)) {
        bt2_addr = linfo->name_bt2_addr;
        assert(H5_addr_defined(bt2_addr));
    } /* end if */

    /* If there is an index defined for the field, use it */
    if (H5_addr_defined(bt2_addr)) {
        H5G_bt2_ud_lbi_t udata; /* User data for v2 B-tree link lookup */

        /* Open the fractal heap */
        if (NULL == (fheap = H5HF_open(f, linfo->fheap_addr)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(f, bt2_addr, NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for index");

        /* Construct the user data for v2 B-tree callback */
        udata.f     = f;
        udata.fheap = fheap;
        udata.lnk   = lnk;

        /* Find & copy the link in the appropriate index */
        if (H5B2_index(bt2, order, n, H5G__dense_lookup_by_idx_bt2_cb, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to locate link in index");
    }      /* end if */
    else { /* Otherwise, we need to build a table of the links and sort it */
        /* Build the table of links for this group */
        if (H5G__dense_build_table(f, linfo, idx_type, order, &ltable) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "error building table of links");

        /* Check for going out of bounds */
        if (n >= ltable.nlinks)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "index out of bound");

        /* Copy link information */
        if (NULL == H5O_msg_copy(H5O_LINK_ID, &ltable.lnks[n], lnk))
            HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "can't copy link message");
    } /* end else */

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for index");
    if (ltable.lnks && H5G__link_release_table(&ltable) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_lookup_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_build_table_cb
 *
 * Purpose:	Callback routine for building table of links from dense
 *              link storage.
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_build_table_cb(const H5O_link_t *lnk, void *_udata)
{
    H5G_dense_bt_ud_t *udata     = (H5G_dense_bt_ud_t *)_udata; /* 'User data' passed in */
    herr_t             ret_value = H5_ITER_CONT;                /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(lnk);
    assert(udata);
    assert(udata->curr_lnk < udata->ltable->nlinks);

    /* Copy link information */
    if (H5O_msg_copy(H5O_LINK_ID, lnk, &(udata->ltable->lnks[udata->curr_lnk])) == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "can't copy link message");

    /* Increment number of links stored */
    udata->curr_lnk++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_build_table_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_build_table
 *
 * Purpose:     Builds a table containing a sorted list of links for a group
 *
 * Note:	Used for building table of links in non-native iteration order
 *		for an index
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_build_table(H5F_t *f, const H5O_linfo_t *linfo, H5_index_t idx_type, H5_iter_order_t order,
                       H5G_link_table_t *ltable)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);
    assert(linfo);
    assert(ltable);

    /* Set size of table */
    H5_CHECK_OVERFLOW(linfo->nlinks, /* From: */ hsize_t, /* To: */ size_t);
    ltable->nlinks = (size_t)linfo->nlinks;

    /* Allocate space for the table entries */
    if (ltable->nlinks > 0) {
        H5G_dense_bt_ud_t udata; /* User data for iteration callback */

        /* Allocate the table to store the links */
        if ((ltable->lnks = (H5O_link_t *)H5MM_malloc(sizeof(H5O_link_t) * ltable->nlinks)) == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

        /* Set up user data for iteration */
        udata.ltable   = ltable;
        udata.curr_lnk = 0;

        /* Iterate over the links in the group, building a table of the link messages */
        if (H5G__dense_iterate(f, linfo, H5_INDEX_NAME, H5_ITER_NATIVE, (hsize_t)0, NULL,
                               H5G__dense_build_table_cb, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTNEXT, FAIL, "error iterating over links");

        /* Sort link table in correct iteration order */
        if (H5G__link_sort_table(ltable, idx_type, order) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTSORT, FAIL, "error sorting link messages");
    } /* end if */
    else
        ltable->lnks = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_build_table() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_iterate_fh_cb
 *
 * Purpose:	Callback for fractal heap operator, to make user's callback
 *              when iterating over links
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_iterate_fh_cb(const void *obj, size_t obj_len, void *_udata)
{
    H5G_fh_ud_it_t *udata     = (H5G_fh_ud_it_t *)_udata; /* User data for fractal heap 'op' callback */
    herr_t          ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Decode link information & keep a copy */
    /* (we make a copy instead of calling the user/library callback directly in
     *  this routine because this fractal heap 'op' callback routine is called
     *  with the direct block protected and if the callback routine invokes an
     *  HDF5 routine, it could attempt to re-protect that direct block for the
     *  heap, causing the HDF5 routine called to fail - QAK)
     */
    if (NULL == (udata->lnk = (H5O_link_t *)H5O_msg_decode(udata->f, NULL, H5O_LINK_ID, obj_len,
                                                           (const unsigned char *)obj)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, FAIL, "can't decode link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_iterate_fh_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_iterate_bt2_cb
 *
 * Purpose:	v2 B-tree callback for dense link storage iterator
 *
 * Return:	H5_ITER_ERROR/H5_ITER_CONT/H5_ITER_STOP
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_iterate_bt2_cb(const void *_record, void *_bt2_udata)
{
    const H5G_dense_bt2_name_rec_t *record    = (const H5G_dense_bt2_name_rec_t *)_record;
    H5G_bt2_ud_it_t                *bt2_udata = (H5G_bt2_ud_it_t *)_bt2_udata; /* User data for callback */
    herr_t                          ret_value = H5_ITER_CONT;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for skipping links */
    if (bt2_udata->skip > 0)
        --bt2_udata->skip;
    else {
        H5G_fh_ud_it_t fh_udata; /* User data for fractal heap 'op' callback */

        /* Prepare user data for callback */
        /* down */
        fh_udata.f = bt2_udata->f;

        /* Call fractal heap 'op' routine, to copy the link information */
        if (H5HF_op(bt2_udata->fheap, record->id, H5G__dense_iterate_fh_cb, &fh_udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPERATE, H5_ITER_ERROR, "heap op callback failed");

        /* Make the callback */
        ret_value = (bt2_udata->op)(fh_udata.lnk, bt2_udata->op_data);

        /* Release the space allocated for the link */
        H5O_msg_free(H5O_LINK_ID, fh_udata.lnk);
    } /* end else */

    /* Increment the number of entries passed through */
    /* (whether we skipped them or not) */
    bt2_udata->count++;

    /* Check for callback failure and pass along return value */
    if (ret_value < 0)
        HERROR(H5E_SYM, H5E_CANTNEXT, "iteration operator failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_iterate_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_iterate
 *
 * Purpose:	Iterate over the objects in a group using dense link storage
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_iterate(H5F_t *f, const H5O_linfo_t *linfo, H5_index_t idx_type, H5_iter_order_t order,
                   hsize_t skip, hsize_t *last_lnk, H5G_lib_iterate_t op, void *op_data)
{
    H5HF_t          *fheap  = NULL;      /* Fractal heap handle */
    H5G_link_table_t ltable = {0, NULL}; /* Table of links */
    H5B2_t          *bt2    = NULL;      /* v2 B-tree handle for index */
    haddr_t          bt2_addr;           /* Address of v2 B-tree to use for lookup */
    herr_t           ret_value = FAIL;   /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(linfo);
    assert(op);

    /* Determine the address of the index to use */
    if (idx_type == H5_INDEX_NAME) {
        /* Since names are hashed, getting them in strictly increasing or
         * decreasing order requires building a table and sorting it. If
         * the order is native, use the B-tree for names.
         */
        bt2_addr = HADDR_UNDEF;
    } /* end if */
    else {
        assert(idx_type == H5_INDEX_CRT_ORDER);

        /* This address may not be defined if creation order is tracked, but
         *      there's no index on it.  If there's no v2 B-tree that indexes
         *      the links and the order is native, use the B-tree for names.
         *      Otherwise, build a table.
         */
        bt2_addr = linfo->corder_bt2_addr;
    } /* end else */

    /* If the order is native and there's no B-tree for indexing the links,
     * use the B-tree for names instead of building a table to speed up the
     * process.
     */
    if (order == H5_ITER_NATIVE && !H5_addr_defined(bt2_addr)) {
        assert(H5_addr_defined(linfo->name_bt2_addr));
        bt2_addr = linfo->name_bt2_addr;
    } /* end if */

    /* Check on iteration order */
    if (order == H5_ITER_NATIVE) {
        H5G_bt2_ud_it_t udata; /* User data for iterator callback */

        /* Sanity check */
        assert(H5_addr_defined(bt2_addr));

        /* Open the fractal heap */
        if (NULL == (fheap = H5HF_open(f, linfo->fheap_addr)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(f, bt2_addr, NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for index");

        /* Construct the user data for v2 B-tree iterator callback */
        udata.f       = f;
        udata.fheap   = fheap;
        udata.skip    = skip;
        udata.count   = 0;
        udata.op      = op;
        udata.op_data = op_data;

        /* Iterate over the records in the v2 B-tree's "native" order */
        /* (by hash of name) */
        if ((ret_value = H5B2_iterate(bt2, H5G__dense_iterate_bt2_cb, &udata)) < 0)
            HERROR(H5E_SYM, H5E_BADITER, "link iteration failed");

        /* Update the last link examined, if requested */
        if (last_lnk)
            *last_lnk = udata.count;
    } /* end if */
    else {
        /* Build the table of links for this group */
        if (H5G__dense_build_table(f, linfo, idx_type, order, &ltable) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "error building table of links");

        /* Iterate over links in table */
        if ((ret_value = H5G__link_iterate_table(&ltable, skip, last_lnk, op, op_data)) < 0)
            HERROR(H5E_SYM, H5E_CANTNEXT, "iteration operator failed");
    } /* end else */

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for index");
    if (ltable.lnks && H5G__link_release_table(&ltable) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_iterate() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_get_name_by_idx_fh_cb
 *
 * Purpose:	Callback for fractal heap operator, to retrieve name according
 *              to an index
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_get_name_by_idx_fh_cb(const void *obj, size_t obj_len, void *_udata)
{
    H5G_fh_ud_gnbi_t *udata = (H5G_fh_ud_gnbi_t *)_udata; /* User data for fractal heap 'op' callback */
    H5O_link_t       *lnk;                                /* Pointer to link created from heap object */
    herr_t            ret_value = SUCCEED;                /* Return value */

    FUNC_ENTER_PACKAGE

    /* Decode link information */
    if (NULL == (lnk = (H5O_link_t *)H5O_msg_decode(udata->f, NULL, H5O_LINK_ID, obj_len,
                                                    (const unsigned char *)obj)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, FAIL, "can't decode link");

    /* Get the length of the name */
    udata->name_len = strlen(lnk->name);

    /* Copy the name into the user's buffer, if given */
    if (udata->name) {
        strncpy(udata->name, lnk->name, MIN((udata->name_len + 1), udata->name_size));
        if (udata->name_len >= udata->name_size)
            udata->name[udata->name_size - 1] = '\0';
    } /* end if */

    /* Release the space allocated for the link */
    H5O_msg_free(H5O_LINK_ID, lnk);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_get_name_by_idx_fh_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_get_name_by_idx_bt2_cb
 *
 * Purpose:	v2 B-tree callback for dense link storage 'get name by idx' call
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_get_name_by_idx_bt2_cb(const void *_record, void *_bt2_udata)
{
    const H5G_dense_bt2_name_rec_t *record    = (const H5G_dense_bt2_name_rec_t *)_record;
    H5G_bt2_ud_gnbi_t              *bt2_udata = (H5G_bt2_ud_gnbi_t *)_bt2_udata; /* User data for callback */
    H5G_fh_ud_gnbi_t                fh_udata;            /* User data for fractal heap 'op' callback */
    herr_t                          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Prepare user data for callback */
    /* down */
    fh_udata.f         = bt2_udata->f;
    fh_udata.name      = bt2_udata->name;
    fh_udata.name_size = bt2_udata->name_size;

    /* Call fractal heap 'op' routine, to perform user callback */
    if (H5HF_op(bt2_udata->fheap, record->id, H5G__dense_get_name_by_idx_fh_cb, &fh_udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPERATE, FAIL, "link found callback failed");

    /* Set the name's full length to return */
    bt2_udata->name_len = fh_udata.name_len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_get_name_by_idx_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_get_name_by_idx
 *
 * Purpose:     Returns the name of objects in the group by giving index.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_get_name_by_idx(H5F_t *f, H5O_linfo_t *linfo, H5_index_t idx_type, H5_iter_order_t order,
                           hsize_t n, char *name, size_t name_size, size_t *name_len)
{
    H5HF_t          *fheap  = NULL;       /* Fractal heap handle */
    H5G_link_table_t ltable = {0, NULL};  /* Table of links */
    H5B2_t          *bt2    = NULL;       /* v2 B-tree handle for index */
    haddr_t          bt2_addr;            /* Address of v2 B-tree to use for lookup */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(linfo);

    /* Determine the address of the index to use */
    if (idx_type == H5_INDEX_NAME) {
        /* Since names are hashed, getting them in strictly increasing or
         * decreasing order requires building a table and sorting it.  If
         * the order is native, use the B-tree for names.
         */
        bt2_addr = HADDR_UNDEF;
    } /* end if */
    else {
        assert(idx_type == H5_INDEX_CRT_ORDER);

        /* This address may not be defined if creation order is tracked, but
         *      there's no index on it.  If there's no v2 B-tree that indexes
         *      the links and the order is native, use the B-tree for names.
         *      Otherwise, build a table.
         */
        bt2_addr = linfo->corder_bt2_addr;
    } /* end else */

    /* If the order is native and there's no B-tree for indexing the links,
     * use the B-tree for names instead of building a table to speed up the
     * process.
     */
    if (order == H5_ITER_NATIVE && !H5_addr_defined(bt2_addr)) {
        bt2_addr = linfo->name_bt2_addr;
        assert(H5_addr_defined(bt2_addr));
    } /* end if */

    /* If there is an index defined for the field, use it */
    if (H5_addr_defined(bt2_addr)) {
        H5G_bt2_ud_gnbi_t udata; /* User data for v2 B-tree callback */

        /* Open the fractal heap */
        if (NULL == (fheap = H5HF_open(f, linfo->fheap_addr)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(f, bt2_addr, NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for index");

        /* Set up the user data for the v2 B-tree 'record remove' callback */
        udata.f         = f;
        udata.fheap     = fheap;
        udata.name      = name;
        udata.name_size = name_size;

        /* Retrieve the name according to the v2 B-tree's index order */
        if (H5B2_index(bt2, order, n, H5G__dense_get_name_by_idx_bt2_cb, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTLIST, FAIL, "can't locate object in v2 B-tree");

        /* Set return value */
        *name_len = udata.name_len;
    }      /* end if */
    else { /* Otherwise, we need to build a table of the links and sort it */
        /* Build the table of links for this group */
        if (H5G__dense_build_table(f, linfo, idx_type, order, &ltable) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "error building table of links");

        /* Check for going out of bounds */
        if (n >= ltable.nlinks)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "index out of bound");

        /* Get the length of the name */
        *name_len = strlen(ltable.lnks[n].name);

        /* Copy the name into the user's buffer, if given */
        if (name) {
            strncpy(name, ltable.lnks[n].name, MIN((*name_len + 1), name_size));
            if (*name_len >= name_size)
                name[name_size - 1] = '\0';
        } /* end if */
    }     /* end else */

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for index");
    if (ltable.lnks && H5G__link_release_table(&ltable) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_get_name_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_remove_fh_cb
 *
 * Purpose:	Callback for fractal heap operator when removing links
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_remove_fh_cb(const void *obj, size_t obj_len, void *_udata)
{
    H5G_fh_ud_rm_t *udata     = (H5G_fh_ud_rm_t *)_udata; /* User data for fractal heap 'op' callback */
    H5O_link_t     *lnk       = NULL;                     /* Pointer to link created from heap object */
    H5B2_t         *bt2       = NULL;                     /* v2 B-tree handle for index */
    herr_t          ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Decode link information */
    if (NULL == (lnk = (H5O_link_t *)H5O_msg_decode(udata->f, NULL, H5O_LINK_ID, obj_len,
                                                    (const unsigned char *)obj)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, FAIL, "can't decode link");

    /* Check for removing the link from the creation order index */
    if (H5_addr_defined(udata->corder_bt2_addr)) {
        H5G_bt2_ud_common_t bt2_udata; /* Info for B-tree callbacks */

        /* Open the creation order index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(udata->f, udata->corder_bt2_addr, NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for creation order index");

        /* Set up the user data for the v2 B-tree 'record remove' callback */
        assert(lnk->corder_valid);
        bt2_udata.corder = lnk->corder;

        /* Remove the record from the name index v2 B-tree */
        if (H5B2_remove(bt2, &bt2_udata, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTREMOVE, FAIL,
                        "unable to remove link from creation order index v2 B-tree");
    } /* end if */

    /* Replace open objects' names, if requested */
    if (udata->replace_names)
        if (H5G__link_name_replace(udata->f, udata->grp_full_path_r, lnk) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTRENAME, FAIL, "unable to rename open objects");

    /* Perform the deletion action on the link, if requested */
    /* (call message "delete" callback directly: *ick* - QAK) */
    if (H5O_link_delete(udata->f, NULL, lnk) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete link");

done:
    /* Release resources */
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for creation order index");
    if (lnk)
        H5O_msg_free(H5O_LINK_ID, lnk);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_remove_fh_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_remove_bt2_cb
 *
 * Purpose:	v2 B-tree callback for dense link storage record removal
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_remove_bt2_cb(const void *_record, void *_bt2_udata)
{
    const H5G_dense_bt2_name_rec_t *record    = (const H5G_dense_bt2_name_rec_t *)_record;
    H5G_bt2_ud_rm_t                *bt2_udata = (H5G_bt2_ud_rm_t *)_bt2_udata; /* User data for callback */
    H5G_fh_ud_rm_t                  fh_udata;            /* User data for fractal heap 'op' callback */
    herr_t                          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set up the user data for fractal heap 'op' callback */
    fh_udata.f               = bt2_udata->common.f;
    fh_udata.corder_bt2_addr = bt2_udata->corder_bt2_addr;
    fh_udata.grp_full_path_r = bt2_udata->grp_full_path_r;
    fh_udata.replace_names   = bt2_udata->replace_names;

    /* Call fractal heap 'op' routine, to perform user callback */
    if (H5HF_op(bt2_udata->common.fheap, record->id, H5G__dense_remove_fh_cb, &fh_udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPERATE, FAIL, "link removal callback failed");

    /* Remove record from fractal heap, if requested */
    if (bt2_udata->rem_from_fheap)
        if (H5HF_remove(bt2_udata->common.fheap, record->id) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTREMOVE, FAIL, "unable to remove link from fractal heap");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_remove_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_remove
 *
 * Purpose:	Remove a link from the dense storage of a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_remove(H5F_t *f, const H5O_linfo_t *linfo, H5RS_str_t *grp_full_path_r, const char *name)
{
    H5HF_t         *fheap = NULL;        /* Fractal heap handle */
    H5G_bt2_ud_rm_t udata;               /* User data for v2 B-tree record removal */
    H5B2_t         *bt2       = NULL;    /* v2 B-tree handle for index */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(linfo);
    assert(name && *name);

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, linfo->fheap_addr)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Open the name index v2 B-tree */
    if (NULL == (bt2 = H5B2_open(f, linfo->name_bt2_addr, NULL)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Set up the user data for the v2 B-tree 'record remove' callback */
    udata.common.f             = f;
    udata.common.fheap         = fheap;
    udata.common.name          = name;
    udata.common.name_hash     = H5_checksum_lookup3(name, strlen(name), 0);
    udata.common.found_op      = NULL;
    udata.common.found_op_data = NULL;
    udata.rem_from_fheap       = true;
    udata.corder_bt2_addr      = linfo->corder_bt2_addr;
    udata.grp_full_path_r      = grp_full_path_r;
    udata.replace_names        = true;

    /* Remove the record from the name index v2 B-tree */
    if (H5B2_remove(bt2, &udata, H5G__dense_remove_bt2_cb, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTREMOVE, FAIL, "unable to remove link from name index v2 B-tree");

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_remove_by_idx_fh_cb
 *
 * Purpose:	Callback for fractal heap operator when removing links by index
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_remove_by_idx_fh_cb(const void *obj, size_t obj_len, void *_udata)
{
    H5G_fh_ud_rmbi_t *udata     = (H5G_fh_ud_rmbi_t *)_udata; /* User data for fractal heap 'op' callback */
    herr_t            ret_value = SUCCEED;                    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Decode link information */
    if (NULL == (udata->lnk = (H5O_link_t *)H5O_msg_decode(udata->f, NULL, H5O_LINK_ID, obj_len,
                                                           (const unsigned char *)obj)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, H5_ITER_ERROR, "can't decode link");

    /* Can't operate on link here because the fractal heap block is locked */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_remove_by_idx_fh_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_remove_by_idx_bt2_cb
 *
 * Purpose:	v2 B-tree callback for dense link storage record removal by index
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__dense_remove_by_idx_bt2_cb(const void *_record, void *_bt2_udata)
{
    H5G_bt2_ud_rmbi_t *bt2_udata = (H5G_bt2_ud_rmbi_t *)_bt2_udata; /* User data for callback */
    H5G_fh_ud_rmbi_t   fh_udata;            /* User data for fractal heap 'op' callback */
    H5B2_t            *bt2 = NULL;          /* v2 B-tree handle for index */
    const uint8_t     *heap_id;             /* Heap ID for link */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Determine the index being used */
    if (bt2_udata->idx_type == H5_INDEX_NAME) {
        const H5G_dense_bt2_name_rec_t *record = (const H5G_dense_bt2_name_rec_t *)_record;

        /* Set the heap ID to operate on */
        heap_id = record->id;
    } /* end if */
    else {
        const H5G_dense_bt2_corder_rec_t *record = (const H5G_dense_bt2_corder_rec_t *)_record;

        assert(bt2_udata->idx_type == H5_INDEX_CRT_ORDER);

        /* Set the heap ID to operate on */
        heap_id = record->id;
    } /* end else */

    /* Set up the user data for fractal heap 'op' callback */
    fh_udata.f   = bt2_udata->f;
    fh_udata.lnk = NULL;

    /* Call fractal heap 'op' routine, to perform user callback */
    if (H5HF_op(bt2_udata->fheap, heap_id, H5G__dense_remove_by_idx_fh_cb, &fh_udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPERATE, FAIL, "link removal callback failed");
    assert(fh_udata.lnk);

    /* Check for removing the link from the "other" index (creation order, when name used and vice versa) */
    if (H5_addr_defined(bt2_udata->other_bt2_addr)) {
        H5G_bt2_ud_common_t other_bt2_udata; /* Info for B-tree callbacks */

        /* Determine the index being used */
        if (bt2_udata->idx_type == H5_INDEX_NAME) {
            /* Set up the user data for the v2 B-tree 'record remove' callback */
            other_bt2_udata.corder = fh_udata.lnk->corder;
        } /* end if */
        else {
            assert(bt2_udata->idx_type == H5_INDEX_CRT_ORDER);

            /* Set up the user data for the v2 B-tree 'record remove' callback */
            other_bt2_udata.f     = bt2_udata->f;
            other_bt2_udata.fheap = bt2_udata->fheap;
            other_bt2_udata.name  = fh_udata.lnk->name;
            other_bt2_udata.name_hash =
                H5_checksum_lookup3(fh_udata.lnk->name, strlen(fh_udata.lnk->name), 0);
            other_bt2_udata.found_op      = NULL;
            other_bt2_udata.found_op_data = NULL;
        } /* end else */

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(bt2_udata->f, bt2_udata->other_bt2_addr, NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for 'other' index");

        /* Set the common information for the v2 B-tree remove operation */

        /* Remove the record from the name index v2 B-tree */
        if (H5B2_remove(bt2, &other_bt2_udata, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTREMOVE, H5_ITER_ERROR,
                        "unable to remove link from 'other' index v2 B-tree");
    } /* end if */

    /* Replace open objects' names */
    if (H5G__link_name_replace(bt2_udata->f, bt2_udata->grp_full_path_r, fh_udata.lnk) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTRENAME, FAIL, "unable to rename open objects");

    /* Perform the deletion action on the link */
    /* (call link message "delete" callback directly: *ick* - QAK) */
    if (H5O_link_delete(bt2_udata->f, NULL, fh_udata.lnk) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete link");

    /* Release the space allocated for the link */
    H5O_msg_free(H5O_LINK_ID, fh_udata.lnk);

    /* Remove record from fractal heap */
    if (H5HF_remove(bt2_udata->fheap, heap_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTREMOVE, FAIL, "unable to remove link from fractal heap");

done:
    /* Release resources */
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for 'other' index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_remove_by_idx_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_remove_by_idx
 *
 * Purpose:	Remove a link from the dense storage of a group, according to
 *              to the offset in an indexed order
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_remove_by_idx(H5F_t *f, const H5O_linfo_t *linfo, H5RS_str_t *grp_full_path_r, H5_index_t idx_type,
                         H5_iter_order_t order, hsize_t n)
{
    H5HF_t          *fheap  = NULL;       /* Fractal heap handle */
    H5G_link_table_t ltable = {0, NULL};  /* Table of links */
    H5B2_t          *bt2    = NULL;       /* v2 B-tree handle for index */
    haddr_t          bt2_addr;            /* Address of v2 B-tree to use for lookup */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(linfo);

    /* Determine the address of the index to use */
    if (idx_type == H5_INDEX_NAME) {
        /* Since names are hashed, getting them in strictly increasing or
         * decreasing order requires building a table and sorting it.  If
         * the order is native, use the B-tree for names.
         */
        bt2_addr = HADDR_UNDEF;
    } /* end if */
    else {
        assert(idx_type == H5_INDEX_CRT_ORDER);

        /* This address may not be defined if creation order is tracked, but
         *      there's no index on it.  If there's no v2 B-tree that indexes
         *      the links and the order is native, use the B-tree for names.
         *      Otherwise, build a table.
         */
        bt2_addr = linfo->corder_bt2_addr;
    } /* end else */

    /* If the order is native and there's no B-tree for indexing the links,
     * use the B-tree for names instead of building a table to speed up the
     * process.
     */
    if (order == H5_ITER_NATIVE && !H5_addr_defined(bt2_addr)) {
        bt2_addr = linfo->name_bt2_addr;
        assert(H5_addr_defined(bt2_addr));
    } /* end if */

    /* If there is an index defined for the field, use it */
    if (H5_addr_defined(bt2_addr)) {
        H5G_bt2_ud_rmbi_t udata; /* User data for v2 B-tree record removal */

        /* Open the fractal heap */
        if (NULL == (fheap = H5HF_open(f, linfo->fheap_addr)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(f, bt2_addr, NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for index");

        /* Set up the user data for the v2 B-tree 'remove by index' callback */
        udata.f               = f;
        udata.fheap           = fheap;
        udata.idx_type        = idx_type;
        udata.other_bt2_addr  = idx_type == H5_INDEX_NAME ? linfo->corder_bt2_addr : linfo->name_bt2_addr;
        udata.grp_full_path_r = grp_full_path_r;

        /* Remove the record from the name index v2 B-tree */
        if (H5B2_remove_by_idx(bt2, order, n, H5G__dense_remove_by_idx_bt2_cb, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTREMOVE, FAIL, "unable to remove link from indexed v2 B-tree");
    }      /* end if */
    else { /* Otherwise, we need to build a table of the links and sort it */
        /* Build the table of links for this group */
        if (H5G__dense_build_table(f, linfo, idx_type, order, &ltable) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "error building table of links");

        /* Check for going out of bounds */
        if (n >= ltable.nlinks)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "index out of bound");

        /* Remove the appropriate link from the dense storage */
        if (H5G__dense_remove(f, linfo, grp_full_path_r, ltable.lnks[n].name) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTREMOVE, FAIL, "unable to remove link from dense storage");
    } /* end else */

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for index");
    if (ltable.lnks && H5G__link_release_table(&ltable) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_remove_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5G__dense_delete
 *
 * Purpose:	Delete the dense storage for a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__dense_delete(H5F_t *f, H5O_linfo_t *linfo, bool adj_link)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(linfo);

    /* Check if we are to adjust the ref. count for all the links */
    /* (we adjust the ref. count when deleting a group and we _don't_ adjust
     *  the ref. count when transitioning back to compact storage)
     */
    if (adj_link) {
        H5HF_t         *fheap = NULL; /* Fractal heap handle */
        H5G_bt2_ud_rm_t udata;        /* User data for v2 B-tree record removal */

        /* Open the fractal heap */
        if (NULL == (fheap = H5HF_open(f, linfo->fheap_addr)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

        /* Set up the user data for the v2 B-tree 'record remove' callback */
        udata.common.f             = f;
        udata.common.fheap         = fheap;
        udata.common.name          = NULL;
        udata.common.name_hash     = 0;
        udata.common.found_op      = NULL;
        udata.common.found_op_data = NULL;
        udata.rem_from_fheap       = false; /* handled in "bulk" below by deleting entire heap */
        udata.corder_bt2_addr      = linfo->corder_bt2_addr;
        udata.grp_full_path_r      = NULL;
        udata.replace_names        = false;

        /* Delete the name index, adjusting the ref. count on links removed */
        if (H5B2_delete(f, linfo->name_bt2_addr, NULL, H5G__dense_remove_bt2_cb, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete v2 B-tree for name index");

        /* Close the fractal heap */
        if (H5HF_close(fheap) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    } /* end if */
    else {
        /* Delete the name index, without adjusting the ref. count on the links  */
        if (H5B2_delete(f, linfo->name_bt2_addr, NULL, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete v2 B-tree for name index");
    } /* end else */
    linfo->name_bt2_addr = HADDR_UNDEF;

    /* Check if we should delete the creation order index v2 B-tree */
    if (linfo->index_corder) {
        /* Delete the creation order index, without adjusting the ref. count on the links  */
        assert(H5_addr_defined(linfo->corder_bt2_addr));
        if (H5B2_delete(f, linfo->corder_bt2_addr, NULL, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete v2 B-tree for creation order index");
        linfo->corder_bt2_addr = HADDR_UNDEF;
    } /* end if */
    else
        assert(!H5_addr_defined(linfo->corder_bt2_addr));

    /* Delete the fractal heap */
    if (H5HF_delete(f, linfo->fheap_addr) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete fractal heap");
    linfo->fheap_addr = HADDR_UNDEF;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__dense_delete() */
