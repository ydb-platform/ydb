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
 * Created:		H5Adense.c
 *
 * Purpose:		Routines for operating on "dense" attribute storage
 *                      for an object.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Amodule.h" /* This source code file is part of the H5A module */
#define H5O_FRIEND     /*suppress error about including H5Opkg  */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Apkg.h"      /* Attributes	  			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object headers			*/
#include "H5SMprivate.h" /* Shared object header messages        */
#include "H5WBprivate.h" /* Wrapped Buffers                      */

/****************/
/* Local Macros */
/****************/

/* v2 B-tree creation macros for 'name' field index */
#define H5A_NAME_BT2_NODE_SIZE  512
#define H5A_NAME_BT2_MERGE_PERC 40
#define H5A_NAME_BT2_SPLIT_PERC 100

/* v2 B-tree creation macros for 'corder' field index */
#define H5A_CORDER_BT2_NODE_SIZE  512
#define H5A_CORDER_BT2_MERGE_PERC 40
#define H5A_CORDER_BT2_SPLIT_PERC 100

/* Size of stack buffer for serialized attributes */
#define H5A_ATTR_BUF_SIZE 128

/******************/
/* Local Typedefs */
/******************/

/*
 * Data exchange structure for dense attribute storage.  This structure is
 * passed through the v2 B-tree layer when modifying the attribute data value.
 */
typedef struct H5A_bt2_od_wrt_t {
    /* downward */
    H5F_t  *f;               /* Pointer to file that fractal heap is in */
    H5HF_t *fheap;           /* Fractal heap handle to operate on */
    H5HF_t *shared_fheap;    /* Fractal heap handle for shared messages */
    H5A_t  *attr;            /* Attribute to write */
    haddr_t corder_bt2_addr; /* v2 B-tree address of creation order index */
} H5A_bt2_od_wrt_t;

/*
 * Data exchange structure to pass through the v2 B-tree layer for the
 * H5B2_iterate function when iterating over densely stored attributes.
 */
typedef struct {
    /* downward (internal) */
    H5F_t  *f;            /* Pointer to file that fractal heap is in */
    H5HF_t *fheap;        /* Fractal heap handle               */
    H5HF_t *shared_fheap; /* Fractal heap handle for shared messages */
    hsize_t count;        /* # of attributes examined          */

    /* downward (from application) */
    hid_t                     loc_id;  /* Object ID for application callback */
    hsize_t                   skip;    /* Number of attributes to skip      */
    const H5A_attr_iter_op_t *attr_op; /* Callback for each attribute       */
    void                     *op_data; /* Callback data for each attribute  */

    /* upward */
    int op_ret; /* Return value from callback        */
} H5A_bt2_ud_it_t;

/*
 * Data exchange structure to pass through the fractal heap layer for the
 * H5HF_op function when copying an attribute stored in densely stored attributes.
 * (or the shared message heap)
 */
typedef struct {
    /* downward (internal) */
    H5F_t                          *f;      /* Pointer to file that fractal heap is in */
    const H5A_dense_bt2_name_rec_t *record; /* v2 B-tree record for attribute */

    /* upward */
    H5A_t *attr; /* Copy of attribute                 */
} H5A_fh_ud_cp_t;

/*
 * Data exchange structure for dense attribute storage.  This structure is
 * passed through the v2 B-tree layer when removing attributes.
 */
typedef struct H5A_bt2_ud_rm_t {
    /* downward */
    H5A_bt2_ud_common_t common;          /* Common info for B-tree user data (must be first) */
    haddr_t             corder_bt2_addr; /* v2 B-tree address of creation order index */
} H5A_bt2_ud_rm_t;

/*
 * Data exchange structure for dense attribute storage.  This structure is
 * passed through the v2 B-tree layer when removing attributes by index.
 */
typedef struct H5A_bt2_ud_rmbi_t {
    /* downward */
    H5F_t     *f;              /* Pointer to file that fractal heap is in */
    H5HF_t    *fheap;          /* Fractal heap handle               */
    H5HF_t    *shared_fheap;   /* Fractal heap handle for shared messages */
    H5_index_t idx_type;       /* Index type for operation */
    haddr_t    other_bt2_addr; /* v2 B-tree address of "other" index */
} H5A_bt2_ud_rmbi_t;

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
 * Function:    H5A__dense_create
 *
 * Purpose:     Creates dense attribute storage structures for an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5A__dense_create(H5F_t *f, H5O_ainfo_t *ainfo)
{
    H5HF_create_t fheap_cparam;         /* Fractal heap creation parameters */
    H5B2_create_t bt2_cparam;           /* v2 B-tree creation parameters */
    H5HF_t       *fheap      = NULL;    /* Fractal heap handle */
    H5B2_t       *bt2_name   = NULL;    /* v2 B-tree handle for names */
    H5B2_t       *bt2_corder = NULL;    /* v2 B-tree handle for creation order */
    herr_t        ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);

    /* Set fractal heap creation parameters */
    /* XXX: Give some control of these to applications? */
    memset(&fheap_cparam, 0, sizeof(fheap_cparam));
    fheap_cparam.managed.width            = H5O_FHEAP_MAN_WIDTH;
    fheap_cparam.managed.start_block_size = H5O_FHEAP_MAN_START_BLOCK_SIZE;
    fheap_cparam.managed.max_direct_size  = H5O_FHEAP_MAN_MAX_DIRECT_SIZE;
    fheap_cparam.managed.max_index        = H5O_FHEAP_MAN_MAX_INDEX;
    fheap_cparam.managed.start_root_rows  = H5O_FHEAP_MAN_START_ROOT_ROWS;
    fheap_cparam.checksum_dblocks         = H5O_FHEAP_CHECKSUM_DBLOCKS;
    fheap_cparam.max_man_size             = H5O_FHEAP_MAX_MAN_SIZE;

    /* Create fractal heap for storing attributes */
    if (NULL == (fheap = H5HF_create(f, &fheap_cparam)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "unable to create fractal heap");

    /* Retrieve the heap's address in the file */
    if (H5HF_get_heap_addr(fheap, &ainfo->fheap_addr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGETSIZE, FAIL, "can't get fractal heap address");

#ifndef NDEBUG
    {
        size_t fheap_id_len; /* Fractal heap ID length */

        /* Retrieve the heap's ID length in the file */
        if (H5HF_get_id_len(fheap, &fheap_id_len) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGETSIZE, FAIL, "can't get fractal heap ID length");
        assert(fheap_id_len == H5O_FHEAP_ID_LEN);
    }
#endif /* NDEBUG */

    /* Create the name index v2 B-tree */
    memset(&bt2_cparam, 0, sizeof(bt2_cparam));
    bt2_cparam.cls       = H5A_BT2_NAME;
    bt2_cparam.node_size = (size_t)H5A_NAME_BT2_NODE_SIZE;
    bt2_cparam.rrec_size = 4 +               /* Name's hash value */
                           4 +               /* Creation order index */
                           1 +               /* Message flags */
                           H5O_FHEAP_ID_LEN; /* Fractal heap ID */
    bt2_cparam.split_percent = H5A_NAME_BT2_SPLIT_PERC;
    bt2_cparam.merge_percent = H5A_NAME_BT2_MERGE_PERC;
    if (NULL == (bt2_name = H5B2_create(f, &bt2_cparam, NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "unable to create v2 B-tree for name index");

    /* Retrieve the v2 B-tree's address in the file */
    if (H5B2_get_addr(bt2_name, &ainfo->name_bt2_addr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get v2 B-tree address for name index");

    /* Check if we should create a creation order index v2 B-tree */
    if (ainfo->index_corder) {
        /* Create the creation order index v2 B-tree */
        memset(&bt2_cparam, 0, sizeof(bt2_cparam));
        bt2_cparam.cls       = H5A_BT2_CORDER;
        bt2_cparam.node_size = (size_t)H5A_CORDER_BT2_NODE_SIZE;
        bt2_cparam.rrec_size = 4 +               /* Creation order index */
                               1 +               /* Message flags */
                               H5O_FHEAP_ID_LEN; /* Fractal heap ID */
        bt2_cparam.split_percent = H5A_CORDER_BT2_SPLIT_PERC;
        bt2_cparam.merge_percent = H5A_CORDER_BT2_MERGE_PERC;
        if (NULL == (bt2_corder = H5B2_create(f, &bt2_cparam, NULL)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "unable to create v2 B-tree for creation order index");

        /* Retrieve the v2 B-tree's address in the file */
        if (H5B2_get_addr(bt2_corder, &ainfo->corder_bt2_addr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get v2 B-tree address for creation order index");
    } /* end if */

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");
    if (bt2_corder && H5B2_close(bt2_corder) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for creation order index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_create() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_fnd_cb
 *
 * Purpose:     Callback when an attribute is located in an index
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5A__dense_fnd_cb(const H5A_t *attr, bool *took_ownership, void *_user_attr)
{
    const H5A_t **user_attr = (const H5A_t **)_user_attr; /* User data from v2 B-tree attribute lookup */
    herr_t        ret_value = SUCCEED;                    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(attr);
    assert(user_attr);
    assert(took_ownership);

    /*
     *  If there is an attribute already stored in "user_attr",
     *  we need to free the dynamially allocated spaces for the
     *  attribute, otherwise we got infinite loop closing library due to
     *  outstanding allocation. (HDFFV-10659)
     *
     *  This callback is used by H5A__dense_remove() to close/free the
     *  attribute stored in "user_attr" (via H5O__msg_free_real()) after
     *  the attribute node is deleted from the name index v2 B-tree.
     *  The issue is:
     *      When deleting the attribute node from the B-tree,
     *      if the attribute is found in the intermediate B-tree nodes,
     *      which may be merged/redistributed, we need to free the dynamically
     *      allocated spaces for the intermediate decoded attribute.
     */
    if (*user_attr != NULL) {
        H5A_t *old_attr = *(H5A_t **)_user_attr;

        /* Free any dynamically allocated items */
        if (old_attr->shared)
            if (H5A__shared_free(old_attr) < 0)
                HGOTO_ERROR(H5E_ATTR, H5E_CANTRELEASE, FAIL, "can't release attribute info");

        old_attr = H5FL_FREE(H5A_t, old_attr);
    } /* end if */

    /* Take over attribute ownership */
    *user_attr      = attr;
    *took_ownership = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_fnd_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_open
 *
 * Purpose:     Open an attribute in dense storage structures for an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
H5A_t *
H5A__dense_open(H5F_t *f, const H5O_ainfo_t *ainfo, const char *name)
{
    H5A_bt2_ud_common_t udata;               /* User data for v2 B-tree modify */
    H5HF_t             *fheap        = NULL; /* Fractal heap handle */
    H5HF_t             *shared_fheap = NULL; /* Fractal heap handle for shared header messages */
    H5B2_t             *bt2_name     = NULL; /* v2 B-tree handle for name index */
    htri_t              attr_sharable;       /* Flag indicating attributes are shareable */
    bool                attr_exists;         /* Attribute exists in v2 B-tree */
    H5A_t              *ret_value = NULL;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);
    assert(name);

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, ainfo->fheap_addr)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "unable to open fractal heap");

    /* Check if attributes are shared in this file */
    if ((attr_sharable = H5SM_type_shared(f, H5O_ATTR_ID)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, NULL, "can't determine if attributes are shared");

    /* Get handle for shared message heap, if attributes are shareable */
    if (attr_sharable) {
        haddr_t shared_fheap_addr; /* Address of fractal heap to use */

        /* Retrieve the address of the shared message's fractal heap */
        if (H5SM_get_fheap_addr(f, H5O_ATTR_ID, &shared_fheap_addr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, NULL, "can't get shared message heap address");

        /* Check if there are any shared messages currently */
        if (H5_addr_defined(shared_fheap_addr)) {
            /* Open the fractal heap for shared header messages */
            if (NULL == (shared_fheap = H5HF_open(f, shared_fheap_addr)))
                HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "unable to open fractal heap");
        } /* end if */
    }     /* end if */

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(f, ainfo->name_bt2_addr, NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "unable to open v2 B-tree for name index");

    /* Create the "udata" information for v2 B-tree record find */
    udata.f             = f;
    udata.fheap         = fheap;
    udata.shared_fheap  = shared_fheap;
    udata.name          = name;
    udata.name_hash     = H5_checksum_lookup3(name, strlen(name), 0);
    udata.flags         = 0;
    udata.corder        = 0;
    udata.found_op      = H5A__dense_fnd_cb; /* v2 B-tree comparison callback */
    udata.found_op_data = &ret_value;

    /* Find & copy the attribute in the 'name' index */
    attr_exists = false;
    if (H5B2_find(bt2_name, &udata, &attr_exists, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, NULL, "can't search for attribute in name index");
    if (attr_exists == false)
        HGOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, NULL, "can't locate attribute in name index");

done:
    /* Release resources */
    if (shared_fheap && H5HF_close(shared_fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't close fractal heap");
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, NULL, "can't close v2 B-tree for name index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_open() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_insert
 *
 * Purpose:     Insert an attribute into dense storage structures for an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5A__dense_insert(H5F_t *f, const H5O_ainfo_t *ainfo, H5A_t *attr)
{
    H5A_bt2_ud_ins_t udata;                       /* User data for v2 B-tree insertion */
    H5HF_t          *fheap        = NULL;         /* Fractal heap handle for attributes */
    H5HF_t          *shared_fheap = NULL;         /* Fractal heap handle for shared header messages */
    H5B2_t          *bt2_name     = NULL;         /* v2 B-tree handle for name index */
    H5B2_t          *bt2_corder   = NULL;         /* v2 B-tree handle for creation order index */
    H5WB_t          *wb           = NULL;         /* Wrapped buffer for attribute data */
    uint8_t          attr_buf[H5A_ATTR_BUF_SIZE]; /* Buffer for serializing message */
    unsigned         mesg_flags = 0;              /* Flags for storing message */
    htri_t           attr_sharable;               /* Flag indicating attributes are shareable */
    herr_t           ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);
    assert(attr);

    /* Check if attributes are shared in this file */
    if ((attr_sharable = H5SM_type_shared(f, H5O_ATTR_ID)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't determine if attributes are shared");

    /* Get handle for shared message heap, if attributes are shareable */
    if (attr_sharable) {
        haddr_t shared_fheap_addr; /* Address of fractal heap to use */
        htri_t  shared_mesg;       /* Should this message be stored in the Shared Message table? */

        /* Check if message is already shared */
        if ((shared_mesg = H5O_msg_is_shared(H5O_ATTR_ID, attr)) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "error determining if message is shared");
        else if (shared_mesg > 0)
            /* Mark the message as shared */
            mesg_flags |= H5O_MSG_FLAG_SHARED;
        else {
            /* Should this attribute be written as a SOHM? */
            if (H5SM_try_share(f, NULL, 0, H5O_ATTR_ID, attr, &mesg_flags) < 0)
                HGOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "error determining if message should be shared");

            /* Attributes can't be "unique be shareable" yet */
            assert(!(mesg_flags & H5O_MSG_FLAG_SHAREABLE));
        } /* end else */

        /* Retrieve the address of the shared message's fractal heap */
        if (H5SM_get_fheap_addr(f, H5O_ATTR_ID, &shared_fheap_addr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get shared message heap address");

        /* Check if there are any shared messages currently */
        if (H5_addr_defined(shared_fheap_addr)) {
            /* Open the fractal heap for shared header messages */
            if (NULL == (shared_fheap = H5HF_open(f, shared_fheap_addr)))
                HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");
        } /* end if */
    }     /* end if */

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, ainfo->fheap_addr)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Check for inserting shared attribute */
    if (mesg_flags & H5O_MSG_FLAG_SHARED) {
        /* Sanity check */
        assert(attr_sharable);

        /* Use heap ID for shared message heap */
        udata.id = attr->sh_loc.u.heap_id;
    } /* end if */
    else {
        void  *attr_ptr;  /* Pointer to serialized message */
        size_t attr_size; /* Size of serialized attribute in the heap */

        /* Find out the size of buffer needed for serialized message */
        if ((attr_size = H5O_msg_raw_size(f, H5O_ATTR_ID, false, attr)) == 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGETSIZE, FAIL, "can't get message size");

        /* Wrap the local buffer for serialized attributes */
        if (NULL == (wb = H5WB_wrap(attr_buf, sizeof(attr_buf))))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't wrap buffer");

        /* Get a pointer to a buffer that's large enough for attribute */
        if (NULL == (attr_ptr = H5WB_actual(wb, attr_size)))
            HGOTO_ERROR(H5E_ATTR, H5E_NOSPACE, FAIL, "can't get actual buffer");

        /* Create serialized form of attribute or shared message */
        if (H5O_msg_encode(f, H5O_ATTR_ID, false, (unsigned char *)attr_ptr, attr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTENCODE, FAIL, "can't encode attribute");

        /* Insert the serialized attribute into the fractal heap */
        /* (sets the heap ID in the user data) */
        if (H5HF_insert(fheap, attr_size, attr_ptr, &udata.id) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "unable to insert attribute into fractal heap");
    } /* end else */

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(f, ainfo->name_bt2_addr, NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Create the callback information for v2 B-tree record insertion */
    udata.common.f            = f;
    udata.common.fheap        = fheap;
    udata.common.shared_fheap = shared_fheap;
    udata.common.name         = attr->shared->name;
    udata.common.name_hash    = H5_checksum_lookup3(attr->shared->name, strlen(attr->shared->name), 0);
    H5_CHECKED_ASSIGN(udata.common.flags, uint8_t, mesg_flags, unsigned);
    udata.common.corder        = attr->shared->crt_idx;
    udata.common.found_op      = NULL;
    udata.common.found_op_data = NULL;
    /* udata.id already set */

    /* Insert attribute into 'name' tracking v2 B-tree */
    if (H5B2_insert(bt2_name, &udata) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "unable to insert record into v2 B-tree");

    /* Check if we should create a creation order index v2 B-tree record */
    if (ainfo->index_corder) {
        /* Open the creation order index v2 B-tree */
        assert(H5_addr_defined(ainfo->corder_bt2_addr));
        if (NULL == (bt2_corder = H5B2_open(f, ainfo->corder_bt2_addr, NULL)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for creation order index");

        /* Insert the record into the creation order index v2 B-tree */
        if (H5B2_insert(bt2_corder, &udata) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "unable to insert record into v2 B-tree");
    } /* end if */

done:
    /* Release resources */
    if (shared_fheap && H5HF_close(shared_fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");
    if (bt2_corder && H5B2_close(bt2_corder) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for creation order index");
    if (wb && H5WB_unwrap(wb) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close wrapped buffer");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_write_bt2_cb2
 *
 * Purpose:     v2 B-tree 'modify' callback to update the record for a creation
 *              order index
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5A__dense_write_bt2_cb2(void *_record, void *_op_data, bool *changed)
{
    H5A_dense_bt2_corder_rec_t *record = (H5A_dense_bt2_corder_rec_t *)_record; /* Record from B-tree */
    H5O_fheap_id_t *new_heap_id        = (H5O_fheap_id_t *)_op_data; /* "op data" from v2 B-tree modify */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(record);
    assert(new_heap_id);

    /* Update record's heap ID */
    record->id = *new_heap_id;

    /* Note that the record changed */
    *changed = true;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5A__dense_write_bt2_cb2() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_write_bt2_cb
 *
 * Purpose:     v2 B-tree 'modify' callback to update the data for an attribute
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5A__dense_write_bt2_cb(void *_record, void *_op_data, bool *changed)
{
    H5A_dense_bt2_name_rec_t *record     = (H5A_dense_bt2_name_rec_t *)_record; /* Record from B-tree */
    H5A_bt2_od_wrt_t         *op_data    = (H5A_bt2_od_wrt_t *)_op_data; /* "op data" from v2 B-tree modify */
    H5B2_t                   *bt2_corder = NULL;           /* v2 B-tree handle for creation order index */
    H5WB_t                   *wb         = NULL;           /* Wrapped buffer for attribute data */
    uint8_t                   attr_buf[H5A_ATTR_BUF_SIZE]; /* Buffer for serializing attribute */
    herr_t                    ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(record);
    assert(op_data);

    /* Check for modifying shared attribute */
    if (record->flags & H5O_MSG_FLAG_SHARED) {
        /* Update the shared attribute in the SOHM info */
        if (H5O__attr_update_shared(op_data->f, NULL, op_data->attr, NULL) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTUPDATE, FAIL, "unable to update attribute in shared storage");

        /* Update record's heap ID */
        record->id = op_data->attr->sh_loc.u.heap_id;

        /* Check if we need to modify the creation order index with new heap ID */
        if (H5_addr_defined(op_data->corder_bt2_addr)) {
            H5A_bt2_ud_common_t udata; /* User data for v2 B-tree modify */

            /* Open the creation order index v2 B-tree */
            if (NULL == (bt2_corder = H5B2_open(op_data->f, op_data->corder_bt2_addr, NULL)))
                HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL,
                            "unable to open v2 B-tree for creation order index");

            /* Create the "udata" information for v2 B-tree record modify */
            udata.f             = op_data->f;
            udata.fheap         = NULL;
            udata.shared_fheap  = NULL;
            udata.name          = NULL;
            udata.name_hash     = 0;
            udata.flags         = 0;
            udata.corder        = op_data->attr->shared->crt_idx;
            udata.found_op      = NULL;
            udata.found_op_data = NULL;

            /* Modify record for creation order index */
            if (H5B2_modify(bt2_corder, &udata, H5A__dense_write_bt2_cb2, &op_data->attr->sh_loc.u.heap_id) <
                0)
                HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "unable to modify record in v2 B-tree");
        } /* end if */

        /* Note that the record changed */
        *changed = true;
    } /* end if */
    else {
        void  *attr_ptr;  /* Pointer to serialized message */
        size_t attr_size; /* Size of serialized attribute in the heap */

        /* Find out the size of buffer needed for serialized attribute */
        if ((attr_size = H5O_msg_raw_size(op_data->f, H5O_ATTR_ID, false, op_data->attr)) == 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGETSIZE, FAIL, "can't get attribute size");

        /* Wrap the local buffer for serialized attributes */
        if (NULL == (wb = H5WB_wrap(attr_buf, sizeof(attr_buf))))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINIT, FAIL, "can't wrap buffer");

        /* Get a pointer to a buffer that's large enough for attribute */
        if (NULL == (attr_ptr = H5WB_actual(wb, attr_size)))
            HGOTO_ERROR(H5E_ATTR, H5E_NOSPACE, FAIL, "can't get actual buffer");

        /* Create serialized form of attribute */
        if (H5O_msg_encode(op_data->f, H5O_ATTR_ID, false, (unsigned char *)attr_ptr, op_data->attr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTENCODE, FAIL, "can't encode attribute");

/* Sanity check */
#ifndef NDEBUG
        {
            size_t obj_len; /* Length of existing encoded attribute */

            if (H5HF_get_obj_len(op_data->fheap, &record->id, &obj_len) < 0)
                HGOTO_ERROR(H5E_ATTR, H5E_CANTGETSIZE, FAIL, "can't get object size");
            assert(obj_len == attr_size);
        }
#endif /* NDEBUG */
        /* Update existing attribute in heap */
        /* (might be more efficient as fractal heap 'op' callback, but leave that for later -QAK) */
        if (H5HF_write(op_data->fheap, &record->id, changed, attr_ptr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTUPDATE, FAIL, "unable to update attribute in heap");
    } /* end else */

done:
    /* Release resources */
    if (bt2_corder && H5B2_close(bt2_corder) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for creation order index");
    if (wb && H5WB_unwrap(wb) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close wrapped buffer");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_write_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_write
 *
 * Purpose:     Modify an attribute in dense storage structures for an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5A__dense_write(H5F_t *f, const H5O_ainfo_t *ainfo, H5A_t *attr)
{
    H5A_bt2_ud_common_t udata;               /* User data for v2 B-tree modify */
    H5A_bt2_od_wrt_t    op_data;             /* "Op data" for v2 B-tree modify */
    H5HF_t             *fheap        = NULL; /* Fractal heap handle */
    H5HF_t             *shared_fheap = NULL; /* Fractal heap handle for shared header messages */
    H5B2_t             *bt2_name     = NULL; /* v2 B-tree handle for name index */
    htri_t              attr_sharable;       /* Flag indicating attributes are shareable */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);
    assert(H5_addr_defined(ainfo->fheap_addr));
    assert(H5_addr_defined(ainfo->name_bt2_addr));
    assert(attr);

    /* Check if attributes are shared in this file */
    if ((attr_sharable = H5SM_type_shared(f, H5O_ATTR_ID)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't determine if attributes are shared");

    /* Get handle for shared message heap, if attributes are shareable */
    if (attr_sharable) {
        haddr_t shared_fheap_addr; /* Address of fractal heap to use */

        /* Retrieve the address of the shared message's fractal heap */
        if (H5SM_get_fheap_addr(f, H5O_ATTR_ID, &shared_fheap_addr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get shared message heap address");

        /* Check if there are any shared messages currently */
        if (H5_addr_defined(shared_fheap_addr)) {
            /* Open the fractal heap for shared header messages */
            if (NULL == (shared_fheap = H5HF_open(f, shared_fheap_addr)))
                HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");
        } /* end if */
    }     /* end if */

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, ainfo->fheap_addr)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(f, ainfo->name_bt2_addr, NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Create the "udata" information for v2 B-tree record modify */
    udata.f             = f;
    udata.fheap         = fheap;
    udata.shared_fheap  = shared_fheap;
    udata.name          = attr->shared->name;
    udata.name_hash     = H5_checksum_lookup3(attr->shared->name, strlen(attr->shared->name), 0);
    udata.flags         = 0;
    udata.corder        = 0;
    udata.found_op      = NULL;
    udata.found_op_data = NULL;

    /* Create the "op_data" for the v2 B-tree record 'modify' callback */
    op_data.f               = f;
    op_data.fheap           = fheap;
    op_data.shared_fheap    = shared_fheap;
    op_data.attr            = attr;
    op_data.corder_bt2_addr = ainfo->corder_bt2_addr;

    /* Modify attribute through 'name' tracking v2 B-tree */
    if (H5B2_modify(bt2_name, &udata, H5A__dense_write_bt2_cb, &op_data) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "unable to modify record in v2 B-tree");

done:
    /* Release resources */
    if (shared_fheap && H5HF_close(shared_fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_write() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_copy_fh_cb
 *
 * Purpose:     Callback for fractal heap operator, to make copy of attribute
 *              for calling routine
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5A__dense_copy_fh_cb(const void *obj, size_t obj_len, void *_udata)
{
    H5A_fh_ud_cp_t *udata     = (H5A_fh_ud_cp_t *)_udata; /* User data for fractal heap 'op' callback */
    herr_t          ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Decode attribute information & keep a copy */
    /* (we make a copy instead of calling the user/library callback directly in
     *  this routine because this fractal heap 'op' callback routine is called
     *  with the direct block protected and if the callback routine invokes an
     *  HDF5 routine, it could attempt to re-protect that direct block for the
     *  heap, causing the HDF5 routine called to fail)
     */
    if (NULL == (udata->attr = (H5A_t *)H5O_msg_decode(udata->f, NULL, H5O_ATTR_ID, obj_len,
                                                       (const unsigned char *)obj)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTDECODE, FAIL, "can't decode attribute");

    /* Set the creation order index for the attribute */
    udata->attr->shared->crt_idx = udata->record->corder;

    /* Check whether we should "reconstitute" the shared message info */
    if (udata->record->flags & H5O_MSG_FLAG_SHARED)
        H5SM_reconstitute(&(udata->attr->sh_loc), udata->f, H5O_ATTR_ID, udata->record->id);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_copy_fh_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_rename
 *
 * Purpose:     Rename an attribute in dense storage structures for an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5A__dense_rename(H5F_t *f, const H5O_ainfo_t *ainfo, const char *old_name, const char *new_name)
{
    H5A_bt2_ud_common_t udata;               /* User data for v2 B-tree modify */
    H5HF_t             *fheap        = NULL; /* Fractal heap handle */
    H5HF_t             *shared_fheap = NULL; /* Fractal heap handle for shared header messages */
    H5B2_t             *bt2_name     = NULL; /* v2 B-tree handle for name index */
    H5B2_t             *bt2_corder   = NULL; /* v2 B-tree handle for creation order ndex */
    H5A_t              *attr_copy    = NULL; /* Copy of attribute to rename */
    htri_t              attr_sharable;       /* Flag indicating attributes are shareable */
    htri_t              shared_mesg;         /* Should this message be stored in the Shared Message table? */
    bool                attr_exists;         /* Attribute exists in v2 B-tree */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);
    assert(old_name);
    assert(new_name);

    /* Check if attributes are shared in this file */
    if ((attr_sharable = H5SM_type_shared(f, H5O_ATTR_ID)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't determine if attributes are shared");

    /* Get handle for shared message heap, if attributes are shareable */
    if (attr_sharable) {
        haddr_t shared_fheap_addr; /* Address of fractal heap to use */

        /* Retrieve the address of the shared message's fractal heap */
        if (H5SM_get_fheap_addr(f, H5O_ATTR_ID, &shared_fheap_addr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get shared message heap address");

        /* Check if there are any shared messages currently */
        if (H5_addr_defined(shared_fheap_addr)) {
            /* Open the fractal heap for shared header messages */
            if (NULL == (shared_fheap = H5HF_open(f, shared_fheap_addr)))
                HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");
        } /* end if */
    }     /* end if */

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, ainfo->fheap_addr)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(f, ainfo->name_bt2_addr, NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Create the "udata" information for v2 B-tree record modify */
    udata.f             = f;
    udata.fheap         = fheap;
    udata.shared_fheap  = shared_fheap;
    udata.name          = old_name;
    udata.name_hash     = H5_checksum_lookup3(old_name, strlen(old_name), 0);
    udata.flags         = 0;
    udata.corder        = 0;
    udata.found_op      = H5A__dense_fnd_cb; /* v2 B-tree comparison callback */
    udata.found_op_data = &attr_copy;

    /* Get copy of attribute through 'name' tracking v2 B-tree */
    attr_exists = false;
    if (H5B2_find(bt2_name, &udata, &attr_exists, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, FAIL, "can't search for attribute in name index");
    if (attr_exists == false)
        HGOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, FAIL, "can't locate attribute in name index");
    assert(attr_copy);

    /* Check if message is already shared */
    if ((shared_mesg = H5O_msg_is_shared(H5O_ATTR_ID, attr_copy)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "error determining if message is shared");
    else if (shared_mesg > 0) {
        /* Reset shared status of copy */
        /* (so it will get shared again if necessary) */
        attr_copy->sh_loc.type = H5O_SHARE_TYPE_UNSHARED;
    } /* end if */

    /* Change name of attribute */
    H5MM_xfree(attr_copy->shared->name);
    attr_copy->shared->name = H5MM_xstrdup(new_name);

    /* Recompute the version to encode the attribute with */
    if (H5A__set_version(f, attr_copy) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "unable to update attribute version");

    /* Need to remove the attribute from the creation order index v2 B-tree */
    if (ainfo->index_corder) {
        bool corder_attr_exists; /* Attribute exists in v2 B-tree */

        /* Open the creation order index v2 B-tree */
        assert(H5_addr_defined(ainfo->corder_bt2_addr));
        if (NULL == (bt2_corder = H5B2_open(f, ainfo->corder_bt2_addr, NULL)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for creation index");

        /* Set up the creation order to search for */
        udata.corder = attr_copy->shared->crt_idx;

        corder_attr_exists = false;
        if (H5B2_find(bt2_corder, &udata, &corder_attr_exists, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, FAIL, "can't search for attribute in name index");

        if (corder_attr_exists) {
            H5A_bt2_ud_rm_t rm_udata;

            /* Set up the creation order in user data for the v2 B-tree 'record remove' callback */
            rm_udata.common.corder = attr_copy->shared->crt_idx;

            /* Remove the record from the creation order index v2 B-tree */
            if (H5B2_remove(bt2_corder, &rm_udata, NULL, NULL) < 0)
                HGOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL,
                            "unable to remove attribute from creation order index v2 B-tree");
        }
    }

    /* Insert renamed attribute back into dense storage */
    /* (Possibly making it shared) */
    if (H5A__dense_insert(f, ainfo, attr_copy) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "unable to add to dense storage");

    /* Was this attribute shared? */
    if ((shared_mesg = H5O_msg_is_shared(H5O_ATTR_ID, attr_copy)) > 0) {
        hsize_t attr_rc; /* Attribute's ref count in shared message storage */

        /* Retrieve ref count for shared attribute */
        if (H5SM_get_refcount(f, H5O_ATTR_ID, &attr_copy->sh_loc, &attr_rc) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't retrieve shared message ref count");

        /* If the newly shared attribute needs to share "ownership" of the shared
         *      components (ie. its reference count is 1), increment the reference
         *      count on any shared components of the attribute, so that they won't
         *      be removed from the file.  (Essentially a "copy on write" operation).
         *
         *      *ick* -QAK, 2007/01/08
         */
        if (attr_rc == 1) {
            /* Increment reference count on attribute components */
            if (H5O__attr_link(f, NULL, attr_copy) < 0)
                HGOTO_ERROR(H5E_ATTR, H5E_LINKCOUNT, FAIL, "unable to adjust attribute link count");
        } /* end if */
    }     /* end if */
    else if (shared_mesg == 0) {
        /* Increment reference count on attribute components */
        /* (so that they aren't deleted when the attribute is removed shortly) */
        if (H5O__attr_link(f, NULL, attr_copy) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_LINKCOUNT, FAIL, "unable to adjust attribute link count");
    } /* end if */
    else if (shared_mesg < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "error determining if message should be shared");

    /* Delete old attribute from dense storage */
    if (H5A__dense_remove(f, ainfo, old_name) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute in dense storage");

done:
    /* Release resources */
    if (shared_fheap && H5HF_close(shared_fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");
    if (bt2_corder && H5B2_close(bt2_corder) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for creation order index");
    if (attr_copy)
        H5O_msg_free(H5O_ATTR_ID, attr_copy);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_rename() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_iterate_bt2_cb
 *
 * Purpose:     v2 B-tree callback for dense attribute storage iterator
 *
 * Return:      H5_ITER_ERROR/H5_ITER_CONT/H5_ITER_STOP
 *
 *-------------------------------------------------------------------------
 */
static int
H5A__dense_iterate_bt2_cb(const void *_record, void *_bt2_udata)
{
    const H5A_dense_bt2_name_rec_t *record =
        (const H5A_dense_bt2_name_rec_t *)_record;              /* Record from B-tree */
    H5A_bt2_ud_it_t *bt2_udata = (H5A_bt2_ud_it_t *)_bt2_udata; /* User data for callback */
    herr_t           ret_value = H5_ITER_CONT;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for skipping attributes */
    if (bt2_udata->skip > 0)
        --bt2_udata->skip;
    else {
        H5A_fh_ud_cp_t fh_udata; /* User data for fractal heap 'op' callback */
        H5HF_t        *fheap;    /* Fractal heap handle for attribute storage */

        /* Check for iterating over shared attribute */
        if (record->flags & H5O_MSG_FLAG_SHARED)
            fheap = bt2_udata->shared_fheap;
        else
            fheap = bt2_udata->fheap;

        /* Prepare user data for callback */
        /* down */
        fh_udata.f      = bt2_udata->f;
        fh_udata.record = record;
        fh_udata.attr   = NULL;

        /* Call fractal heap 'op' routine, to copy the attribute information */
        if (H5HF_op(fheap, &record->id, H5A__dense_copy_fh_cb, &fh_udata) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPERATE, H5_ITER_ERROR, "heap op callback failed");

        /* Check which type of callback to make */
        switch (bt2_udata->attr_op->op_type) {
            case H5A_ATTR_OP_APP2: {
                H5A_info_t ainfo; /* Info for attribute */

                /* Get the attribute information */
                if (H5A__get_info(fh_udata.attr, &ainfo) < 0)
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, H5_ITER_ERROR, "unable to get attribute info");

                /* Make the application callback */
                ret_value = (bt2_udata->attr_op->u.app_op2)(bt2_udata->loc_id, fh_udata.attr->shared->name,
                                                            &ainfo, bt2_udata->op_data);
                break;
            }

#ifndef H5_NO_DEPRECATED_SYMBOLS
            case H5A_ATTR_OP_APP:
                /* Make the application callback */
                ret_value = (bt2_udata->attr_op->u.app_op)(bt2_udata->loc_id, fh_udata.attr->shared->name,
                                                           bt2_udata->op_data);
                break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

            case H5A_ATTR_OP_LIB:
                /* Call the library's callback */
                ret_value = (bt2_udata->attr_op->u.lib_op)(fh_udata.attr, bt2_udata->op_data);
                break;

            default:
                assert("unknown attribute op type" && 0);
#ifdef NDEBUG
                HGOTO_ERROR(H5E_ATTR, H5E_UNSUPPORTED, FAIL, "unsupported attribute op type");
#endif    /* NDEBUG */
        } /* end switch */

        /* Release the space allocated for the attribute */
        H5O_msg_free(H5O_ATTR_ID, fh_udata.attr);
    } /* end else */

    /* Increment the number of attributes passed through */
    /* (whether we skipped them or not) */
    bt2_udata->count++;

    /* Check for callback failure and pass along return value */
    if (ret_value < 0)
        HERROR(H5E_ATTR, H5E_CANTNEXT, "iteration operator failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_iterate_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_iterate
 *
 * Purpose:     Iterate over attributes in dense storage structures for an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5A__dense_iterate(H5F_t *f, hid_t loc_id, const H5O_ainfo_t *ainfo, H5_index_t idx_type,
                   H5_iter_order_t order, hsize_t skip, hsize_t *last_attr, const H5A_attr_iter_op_t *attr_op,
                   void *op_data)
{
    H5HF_t          *fheap        = NULL;      /* Fractal heap handle */
    H5HF_t          *shared_fheap = NULL;      /* Fractal heap handle for shared header messages */
    H5A_attr_table_t atable       = {0, NULL}; /* Table of attributes */
    H5B2_t          *bt2          = NULL;      /* v2 B-tree handle for index */
    haddr_t          bt2_addr;                 /* Address of v2 B-tree to use for lookup */
    herr_t           ret_value = FAIL;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);
    assert(H5_addr_defined(ainfo->fheap_addr));
    assert(H5_addr_defined(ainfo->name_bt2_addr));
    assert(attr_op);

    /* Determine the address of the index to use */
    if (idx_type == H5_INDEX_NAME) {
        /* Check if "native" order is OK - since names are hashed, getting them
         *      in strictly increasing or decreasing order requires building a
         *      table and sorting it.
         */
        if (order == H5_ITER_NATIVE) {
            assert(H5_addr_defined(ainfo->name_bt2_addr));
            bt2_addr = ainfo->name_bt2_addr;
        } /* end if */
        else
            bt2_addr = HADDR_UNDEF;
    } /* end if */
    else {
        assert(idx_type == H5_INDEX_CRT_ORDER);

        /* This address may not be defined if creation order is tracked, but
         *      there's no index on it.  If there's no v2 B-tree that indexes
         *      the links, a table will be built.
         */
        bt2_addr = ainfo->corder_bt2_addr;
    } /* end else */

    /* Check on iteration order */
    if (order == H5_ITER_NATIVE && H5_addr_defined(bt2_addr)) {
        H5A_bt2_ud_it_t udata;         /* User data for iterator callback */
        htri_t          attr_sharable; /* Flag indicating attributes are shareable */

        /* Open the fractal heap */
        if (NULL == (fheap = H5HF_open(f, ainfo->fheap_addr)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

        /* Check if attributes are shared in this file */
        if ((attr_sharable = H5SM_type_shared(f, H5O_ATTR_ID)) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't determine if attributes are shared");

        /* Get handle for shared message heap, if attributes are shareable */
        if (attr_sharable) {
            haddr_t shared_fheap_addr; /* Address of fractal heap to use */

            /* Retrieve the address of the shared message's fractal heap */
            if (H5SM_get_fheap_addr(f, H5O_ATTR_ID, &shared_fheap_addr) < 0)
                HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get shared message heap address");

            /* Check if there are any shared messages currently */
            if (H5_addr_defined(shared_fheap_addr)) {
                /* Open the fractal heap for shared header messages */
                if (NULL == (shared_fheap = H5HF_open(f, shared_fheap_addr)))
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");
            } /* end if */
        }     /* end if */

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(f, bt2_addr, NULL)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for index");

        /* Construct the user data for v2 B-tree iterator callback */
        udata.f            = f;
        udata.fheap        = fheap;
        udata.shared_fheap = shared_fheap;
        udata.loc_id       = loc_id;
        udata.skip         = skip;
        udata.count        = 0;
        udata.attr_op      = attr_op;
        udata.op_data      = op_data;

        /* Iterate over the records in the v2 B-tree's "native" order */
        /* (by hash of name) */
        if ((ret_value = H5B2_iterate(bt2, H5A__dense_iterate_bt2_cb, &udata)) < 0)
            HERROR(H5E_ATTR, H5E_BADITER, "attribute iteration failed");

        /* Update the last attribute examined, if requested */
        if (last_attr)
            *last_attr = udata.count;
    } /* end if */
    else {
        /* Build the table of attributes for this object */
        /* (build table using the name index, but sort according to idx_type) */
        if (H5A__dense_build_table(f, ainfo, idx_type, order, &atable) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "error building table of attributes");

        /* Iterate over attributes in table */
        if ((ret_value = H5A__attr_iterate_table(&atable, skip, last_attr, loc_id, attr_op, op_data)) < 0)
            HERROR(H5E_ATTR, H5E_CANTNEXT, "iteration operator failed");
    } /* end else */

done:
    /* Release resources */
    if (shared_fheap && H5HF_close(shared_fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for index");
    if (atable.attrs && H5A__attr_release_table(&atable) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CANTFREE, FAIL, "unable to release attribute table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_remove_bt2_cb
 *
 * Purpose:     v2 B-tree callback for dense attribute storage record removal
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5A__dense_remove_bt2_cb(const void *_record, void *_udata)
{
    const H5A_dense_bt2_name_rec_t *record = (const H5A_dense_bt2_name_rec_t *)_record;
    H5A_bt2_ud_rm_t                *udata  = (H5A_bt2_ud_rm_t *)_udata; /* User data for callback */
    H5A_t  *attr       = *(H5A_t **)udata->common.found_op_data;        /* Pointer to attribute to remove */
    H5B2_t *bt2_corder = NULL;    /* v2 B-tree handle for creation order index */
    herr_t  ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for removing the link from the creation order index */
    if (H5_addr_defined(udata->corder_bt2_addr)) {
        /* Open the creation order index v2 B-tree */
        if (NULL == (bt2_corder = H5B2_open(udata->common.f, udata->corder_bt2_addr, NULL)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for creation order index");

        /* Set up the user data for the v2 B-tree 'record remove' callback */
        udata->common.corder = attr->shared->crt_idx;

        /* Remove the record from the creation order index v2 B-tree */
        if (H5B2_remove(bt2_corder, udata, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL,
                        "unable to remove attribute from creation order index v2 B-tree");
    } /* end if */

    /* Check for removing shared attribute */
    if (record->flags & H5O_MSG_FLAG_SHARED) {
        /* Decrement the reference count on the shared attribute message */
        if (H5SM_delete(udata->common.f, NULL, &(attr->sh_loc)) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTFREE, FAIL, "unable to delete shared attribute");
    } /* end if */
    else {
        /* Perform the deletion action on the attribute */
        /* (takes care of shared & committed datatype/dataspace components) */
        if (H5O__attr_delete(udata->common.f, NULL, attr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");

        /* Remove record from fractal heap */
        if (H5HF_remove(udata->common.fheap, &record->id) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL, "unable to remove attribute from fractal heap");
    } /* end else */

done:
    /* Release resources */
    if (bt2_corder && H5B2_close(bt2_corder) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for creation order index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_remove_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_remove
 *
 * Purpose:     Remove an attribute from the dense storage of an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5A__dense_remove(H5F_t *f, const H5O_ainfo_t *ainfo, const char *name)
{
    H5A_bt2_ud_rm_t udata;               /* User data for v2 B-tree record removal */
    H5HF_t         *fheap        = NULL; /* Fractal heap handle */
    H5HF_t         *shared_fheap = NULL; /* Fractal heap handle for shared header messages */
    H5B2_t         *bt2_name     = NULL; /* v2 B-tree handle for name index */
    H5A_t          *attr_copy    = NULL; /* Copy of attribute to remove */
    htri_t          attr_sharable;       /* Flag indicating attributes are shareable */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);
    assert(name && *name);

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, ainfo->fheap_addr)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Check if attributes are shared in this file */
    if ((attr_sharable = H5SM_type_shared(f, H5O_ATTR_ID)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't determine if attributes are shared");

    /* Get handle for shared message heap, if attributes are shareable */
    if (attr_sharable) {
        haddr_t shared_fheap_addr; /* Address of fractal heap to use */

        /* Retrieve the address of the shared message's fractal heap */
        if (H5SM_get_fheap_addr(f, H5O_ATTR_ID, &shared_fheap_addr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get shared message heap address");

        /* Check if there are any shared messages currently */
        if (H5_addr_defined(shared_fheap_addr)) {
            /* Open the fractal heap for shared header messages */
            if (NULL == (shared_fheap = H5HF_open(f, shared_fheap_addr)))
                HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");
        } /* end if */
    }     /* end if */

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(f, ainfo->name_bt2_addr, NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Set up the user data for the v2 B-tree 'record remove' callback */
    udata.common.f             = f;
    udata.common.fheap         = fheap;
    udata.common.shared_fheap  = shared_fheap;
    udata.common.name          = name;
    udata.common.name_hash     = H5_checksum_lookup3(name, strlen(name), 0);
    udata.common.found_op      = H5A__dense_fnd_cb; /* v2 B-tree comparison callback */
    udata.common.found_op_data = &attr_copy;
    udata.corder_bt2_addr      = ainfo->corder_bt2_addr;

    /* Remove the record from the name index v2 B-tree */
    if (H5B2_remove(bt2_name, &udata, H5A__dense_remove_bt2_cb, &udata) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL, "unable to remove attribute from name index v2 B-tree");

done:
    /* Release resources */
    if (shared_fheap && H5HF_close(shared_fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");
    if (attr_copy)
        H5O_msg_free_real(H5O_MSG_ATTR, attr_copy);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_remove_by_idx_bt2_cb
 *
 * Purpose:     v2 B-tree callback for dense attribute storage record removal by index
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5A__dense_remove_by_idx_bt2_cb(const void *_record, void *_bt2_udata)
{
    H5HF_t                         *fheap;         /* Fractal heap handle */
    H5B2_t                         *bt2    = NULL; /* v2 B-tree handle for index */
    const H5A_dense_bt2_name_rec_t *record = (const H5A_dense_bt2_name_rec_t *)_record; /* v2 B-tree record */
    H5A_bt2_ud_rmbi_t              *bt2_udata = (H5A_bt2_ud_rmbi_t *)_bt2_udata; /* User data for callback */
    H5A_fh_ud_cp_t                  fh_udata; /* User data for fractal heap 'op' callback */
    H5O_shared_t                    sh_loc;   /* Shared message info for attribute */
    bool   use_sh_loc;          /* Whether to use the attribute's shared location or the separate one */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set up the user data for fractal heap 'op' callback */
    fh_udata.f      = bt2_udata->f;
    fh_udata.record = record;
    fh_udata.attr   = NULL;

    /* Get correct fractal heap handle to use for operations */
    if (record->flags & H5O_MSG_FLAG_SHARED)
        fheap = bt2_udata->shared_fheap;
    else
        fheap = bt2_udata->fheap;

    /* Check whether to make a copy of the attribute or just need the shared location info */
    if (H5_addr_defined(bt2_udata->other_bt2_addr) || !(record->flags & H5O_MSG_FLAG_SHARED)) {
        /* Call fractal heap 'op' routine, to make copy of attribute to remove */
        if (H5HF_op(fheap, &record->id, H5A__dense_copy_fh_cb, &fh_udata) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPERATE, FAIL, "attribute removal callback failed");
        assert(fh_udata.attr);

        /* Use the attribute's shared location */
        use_sh_loc = false;
    } /* end if */
    else {
        /* Create a shared message location from the heap ID for this record */
        H5SM_reconstitute(&sh_loc, bt2_udata->f, H5O_ATTR_ID, record->id);

        /* Use the separate shared location */
        use_sh_loc = true;
    } /* end else */

    /* Check for removing the link from the "other" index (creation order, when name used and vice versa) */
    if (H5_addr_defined(bt2_udata->other_bt2_addr)) {
        H5A_bt2_ud_common_t other_bt2_udata; /* Info for B-tree callbacks */

        /* Determine the index being used */
        if (bt2_udata->idx_type == H5_INDEX_NAME) {
            /* Set up the user data for the v2 B-tree 'record remove' callback */
            other_bt2_udata.corder = fh_udata.attr->shared->crt_idx;
        } /* end if */
        else {
            assert(bt2_udata->idx_type == H5_INDEX_CRT_ORDER);

            /* Set up the user data for the v2 B-tree 'record remove' callback */
            other_bt2_udata.f            = bt2_udata->f;
            other_bt2_udata.fheap        = bt2_udata->fheap;
            other_bt2_udata.shared_fheap = bt2_udata->shared_fheap;
            other_bt2_udata.name         = fh_udata.attr->shared->name;
            other_bt2_udata.name_hash =
                H5_checksum_lookup3(fh_udata.attr->shared->name, strlen(fh_udata.attr->shared->name), 0);
            other_bt2_udata.found_op      = NULL;
            other_bt2_udata.found_op_data = NULL;
        } /* end else */

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(bt2_udata->f, bt2_udata->other_bt2_addr, NULL)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for index");

        /* Set the common information for the v2 B-tree remove operation */

        /* Remove the record from the "other" index v2 B-tree */
        if (H5B2_remove(bt2, &other_bt2_udata, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL,
                        "unable to remove record from 'other' index v2 B-tree");
    } /* end if */

    /* Check for removing shared attribute */
    if (record->flags & H5O_MSG_FLAG_SHARED) {
        H5O_shared_t *sh_loc_ptr; /* Pointer to shared message info for attribute */

        /* Set up pointer to correct shared location */
        if (use_sh_loc)
            sh_loc_ptr = &sh_loc;
        else
            sh_loc_ptr = &(fh_udata.attr->sh_loc);

        /* Decrement the reference count on the shared attribute message */
        if (H5SM_delete(bt2_udata->f, NULL, sh_loc_ptr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTFREE, FAIL, "unable to delete shared attribute");
    } /* end if */
    else {
        /* Perform the deletion action on the attribute */
        /* (takes care of shared & committed datatype/dataspace components) */
        if (H5O__attr_delete(bt2_udata->f, NULL, fh_udata.attr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");

        /* Remove record from fractal heap */
        if (H5HF_remove(fheap, &record->id) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL, "unable to remove attribute from fractal heap");
    } /* end else */

done:
    /* Release resources */
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for index");
    if (fh_udata.attr)
        H5O_msg_free(H5O_ATTR_ID, fh_udata.attr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_remove_by_idx_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_remove_by_idx
 *
 * Purpose:     Remove an attribute from the dense storage of an object,
 *              according to the order within an index
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5A__dense_remove_by_idx(H5F_t *f, const H5O_ainfo_t *ainfo, H5_index_t idx_type, H5_iter_order_t order,
                         hsize_t n)
{
    H5HF_t          *fheap        = NULL;      /* Fractal heap handle */
    H5HF_t          *shared_fheap = NULL;      /* Fractal heap handle for shared header messages */
    H5A_attr_table_t atable       = {0, NULL}; /* Table of attributes */
    H5B2_t          *bt2          = NULL;      /* v2 B-tree handle for index */
    haddr_t          bt2_addr;                 /* Address of v2 B-tree to use for operation */
    herr_t           ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);

    /* Determine the address of the index to use */
    if (idx_type == H5_INDEX_NAME) {
        /* Check if "native" order is OK - since names are hashed, getting them
         *      in strictly increasing or decreasing order requires building a
         *      table and sorting it.
         */
        if (order == H5_ITER_NATIVE) {
            bt2_addr = ainfo->name_bt2_addr;
            assert(H5_addr_defined(bt2_addr));
        } /* end if */
        else
            bt2_addr = HADDR_UNDEF;
    } /* end if */
    else {
        assert(idx_type == H5_INDEX_CRT_ORDER);

        /* This address may not be defined if creation order is tracked, but
         *      there's no index on it.  If there's no v2 B-tree that indexes
         *      the links, a table will be built.
         */
        bt2_addr = ainfo->corder_bt2_addr;
    } /* end else */

    /* If there is an index defined for the field, use it */
    if (H5_addr_defined(bt2_addr)) {
        H5A_bt2_ud_rmbi_t udata;         /* User data for v2 B-tree record removal */
        htri_t            attr_sharable; /* Flag indicating attributes are shareable */

        /* Open the fractal heap */
        if (NULL == (fheap = H5HF_open(f, ainfo->fheap_addr)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

        /* Check if attributes are shared in this file */
        if ((attr_sharable = H5SM_type_shared(f, H5O_ATTR_ID)) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't determine if attributes are shared");

        /* Get handle for shared message heap, if attributes are shareable */
        if (attr_sharable) {
            haddr_t shared_fheap_addr; /* Address of fractal heap to use */

            /* Retrieve the address of the shared message's fractal heap */
            if (H5SM_get_fheap_addr(f, H5O_ATTR_ID, &shared_fheap_addr) < 0)
                HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get shared message heap address");

            /* Check if there are any shared messages currently */
            if (H5_addr_defined(shared_fheap_addr)) {
                /* Open the fractal heap for shared header messages */
                if (NULL == (shared_fheap = H5HF_open(f, shared_fheap_addr)))
                    HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");
            } /* end if */
        }     /* end if */

        /* Open the index v2 B-tree */
        if (NULL == (bt2 = H5B2_open(f, bt2_addr, NULL)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for index");

        /* Set up the user data for the v2 B-tree 'record remove' callback */
        udata.f              = f;
        udata.fheap          = fheap;
        udata.shared_fheap   = shared_fheap;
        udata.idx_type       = idx_type;
        udata.other_bt2_addr = idx_type == H5_INDEX_NAME ? ainfo->corder_bt2_addr : ainfo->name_bt2_addr;

        /* Remove the record from the name index v2 B-tree */
        if (H5B2_remove_by_idx(bt2, order, n, H5A__dense_remove_by_idx_bt2_cb, &udata) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTREMOVE, FAIL, "unable to remove attribute from v2 B-tree index");
    } /* end if */
    else {
        /* Build the table of attributes for this object */
        /* (build table using the name index, but sort according to idx_type) */
        if (H5A__dense_build_table(f, ainfo, idx_type, order, &atable) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "error building table of attributes");

        /* Check for skipping too many attributes */
        if (n >= atable.nattrs)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index specified");

        /* Delete appropriate attribute from dense storage */
        if (H5A__dense_remove(f, ainfo, ((atable.attrs[n])->shared)->name) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute in dense storage");
    } /* end else */

done:
    /* Release resources */
    if (shared_fheap && H5HF_close(shared_fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2 && H5B2_close(bt2) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for index");
    if (atable.attrs && H5A__attr_release_table(&atable) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CANTFREE, FAIL, "unable to release attribute table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_remove_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_exists
 *
 * Purpose:     Check if an attribute exists in dense storage structures for
 *              an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5A__dense_exists(H5F_t *f, const H5O_ainfo_t *ainfo, const char *name, bool *attr_exists)
{
    H5A_bt2_ud_common_t udata;               /* User data for v2 B-tree modify */
    H5HF_t             *fheap        = NULL; /* Fractal heap handle */
    H5HF_t             *shared_fheap = NULL; /* Fractal heap handle for shared header messages */
    H5B2_t             *bt2_name     = NULL; /* v2 B-tree handle for name index */
    htri_t              attr_sharable;       /* Flag indicating attributes are shareable */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);
    assert(name);
    assert(attr_exists);

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, ainfo->fheap_addr)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Check if attributes are shared in this file */
    if ((attr_sharable = H5SM_type_shared(f, H5O_ATTR_ID)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't determine if attributes are shared");

    /* Get handle for shared message heap, if attributes are shareable */
    if (attr_sharable) {
        haddr_t shared_fheap_addr; /* Address of fractal heap to use */

        /* Retrieve the address of the shared message's fractal heap */
        if (H5SM_get_fheap_addr(f, H5O_ATTR_ID, &shared_fheap_addr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get shared message heap address");

        /* Check if there are any shared messages currently */
        if (H5_addr_defined(shared_fheap_addr)) {
            /* Open the fractal heap for shared header messages */
            if (NULL == (shared_fheap = H5HF_open(f, shared_fheap_addr)))
                HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");
        } /* end if */
    }     /* end if */

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(f, ainfo->name_bt2_addr, NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Create the "udata" information for v2 B-tree record 'find' */
    udata.f             = f;
    udata.fheap         = fheap;
    udata.shared_fheap  = shared_fheap;
    udata.name          = name;
    udata.name_hash     = H5_checksum_lookup3(name, strlen(name), 0);
    udata.flags         = 0;
    udata.corder        = 0;
    udata.found_op      = NULL; /* v2 B-tree comparison callback */
    udata.found_op_data = NULL;

    /* Find the attribute in the 'name' index */
    if (H5B2_find(bt2_name, &udata, attr_exists, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_NOTFOUND, FAIL, "can't search for attribute in name index");

done:
    /* Release resources */
    if (shared_fheap && H5HF_close(shared_fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_exists() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_delete_bt2_cb
 *
 * Purpose:     v2 B-tree callback for dense attribute storage deletion
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5A__dense_delete_bt2_cb(const void *_record, void *_bt2_udata)
{
    const H5A_dense_bt2_name_rec_t *record =
        (const H5A_dense_bt2_name_rec_t *)_record;                      /* Record from B-tree */
    H5A_bt2_ud_common_t *bt2_udata = (H5A_bt2_ud_common_t *)_bt2_udata; /* User data for callback */
    H5A_t               *attr      = NULL;                              /* Attribute being removed */
    herr_t               ret_value = SUCCEED;                           /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for shared attribute */
    if (record->flags & H5O_MSG_FLAG_SHARED) {
        H5O_shared_t sh_mesg; /* Temporary shared message info */

        /* "reconstitute" the shared message info for the attribute */
        H5SM_reconstitute(&sh_mesg, bt2_udata->f, H5O_ATTR_ID, record->id);

        /* Decrement the reference count on the shared attribute message */
        if (H5SM_delete(bt2_udata->f, NULL, &sh_mesg) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTFREE, FAIL, "unable to delete shared attribute");
    } /* end if */
    else {
        H5A_fh_ud_cp_t fh_udata; /* User data for fractal heap 'op' callback */

        /* Prepare user data for callback */
        /* down */
        fh_udata.f      = bt2_udata->f;
        fh_udata.record = record;
        /* up */
        fh_udata.attr = NULL;

        /* Call fractal heap 'op' routine, to copy the attribute information */
        if (H5HF_op(bt2_udata->fheap, &record->id, H5A__dense_copy_fh_cb, &fh_udata) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTOPERATE, FAIL, "heap op callback failed");
        attr = fh_udata.attr;

        /* Perform the deletion action on the attribute */
        /* (takes care of shared/committed datatype & dataspace components) */
        if (H5O__attr_delete(bt2_udata->f, NULL, fh_udata.attr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");
    } /* end else */

done:
    /* Release resources */
    if (attr)
        H5O_msg_free_real(H5O_MSG_ATTR, attr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_delete_bt2_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5A__dense_delete
 *
 * Purpose:     Delete all dense storage structures for attributes on an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5A__dense_delete(H5F_t *f, H5O_ainfo_t *ainfo)
{
    H5A_bt2_ud_common_t udata;               /* v2 B-tree user data for deleting attributes */
    H5HF_t             *fheap     = NULL;    /* Fractal heap handle */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(ainfo);

    /* Open the fractal heap */
    if (NULL == (fheap = H5HF_open(f, ainfo->fheap_addr)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, FAIL, "unable to open fractal heap");

    /* Create the "udata" information for v2 B-tree 'delete' */
    udata.f             = f;
    udata.fheap         = fheap;
    udata.shared_fheap  = NULL;
    udata.name          = NULL;
    udata.name_hash     = 0;
    udata.flags         = 0;
    udata.found_op      = NULL; /* v2 B-tree comparison callback */
    udata.found_op_data = NULL;

    /* Delete name index v2 B-tree */
    if (H5B2_delete(f, ainfo->name_bt2_addr, NULL, H5A__dense_delete_bt2_cb, &udata) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete v2 B-tree for name index");
    ainfo->name_bt2_addr = HADDR_UNDEF;

    /* Release resources */
    if (H5HF_close(fheap) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");
    fheap = NULL;

    /* Check if we should delete the creation order index v2 B-tree */
    if (H5_addr_defined(ainfo->corder_bt2_addr)) {
        /* Delete the creation order index, without adjusting the ref. count on the attributes  */
        if (H5B2_delete(f, ainfo->corder_bt2_addr, NULL, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL,
                        "unable to delete v2 B-tree for creation order index");
        ainfo->corder_bt2_addr = HADDR_UNDEF;
    } /* end if */

    /* Delete fractal heap */
    if (H5HF_delete(f, ainfo->fheap_addr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete fractal heap");
    ainfo->fheap_addr = HADDR_UNDEF;

done:
    /* Release resources */
    if (fheap && H5HF_close(fheap) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, FAIL, "can't close fractal heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5A__dense_delete() */
