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
 * Created:		H5Gobj.c
 *
 * Purpose:		Functions for abstract handling of objects in groups.
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
#include "H5Fprivate.h"  /* File access				*/
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5Lprivate.h"  /* Links			  	*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Pprivate.h"  /* Property Lists			*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* User data for object header iterator when converting link messages to dense
 * link storage
 */
typedef struct {
    H5F_t       *f;       /* Pointer to file for insertion */
    haddr_t      oh_addr; /* Address of the object header */
    H5O_linfo_t *linfo;   /* Pointer to link info */
} H5G_obj_oh_it_ud1_t;

/* User data for link iterator when converting dense link storage to link
 * messages
 */
typedef struct {
    H5O_link_t *lnk_table;   /* Array of links to convert */
    size_t      nlinks;      /* Number of links converted */
    size_t      alloc_links; /* Size of link table        */
} H5G_obj_lnk_it_ud1_t;

/* User data for symbol table iterator when converting old-format group to
 * a new-format group
 */
typedef struct {
    const H5O_loc_t *grp_oloc; /* Pointer to group for insertion */
} H5G_obj_stab_it_ud1_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5G__obj_compact_to_dense_cb(const void *_mesg, unsigned idx, void *_udata);
static herr_t H5G__obj_remove_update_linfo(const H5O_loc_t *oloc, H5O_linfo_t *linfo);

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
 * Function:    H5G__obj_create
 *
 * Purpose:     Create an object header for a group and update object location info
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__obj_create(H5F_t *f, H5G_obj_create_t *gcrt_info, H5O_loc_t *oloc /*out*/)
{
    H5P_genplist_t *gc_plist;            /* Group creation property list */
    H5O_ginfo_t     ginfo;               /* Group info */
    H5O_linfo_t     linfo;               /* Link info */
    H5O_pline_t     pline;               /* Pipeline */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(oloc);

    /* Get the property list */
    if (NULL == (gc_plist = (H5P_genplist_t *)H5I_object(gcrt_info->gcpl_id)))
        HGOTO_ERROR(H5E_SYM, H5E_BADTYPE, FAIL, "not a property list");

    /* Get the group info property */
    if (H5P_get(gc_plist, H5G_CRT_GROUP_INFO_NAME, &ginfo) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get group info");

    /* Get the link info property */
    if (H5P_get(gc_plist, H5G_CRT_LINK_INFO_NAME, &linfo) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get group info");

    /* Get the pipeline property */
    if (H5P_peek(gc_plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get group info");

    /* Call the "real" group creation routine now */
    if (H5G__obj_create_real(f, &ginfo, &linfo, &pline, gcrt_info, oloc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, FAIL, "unable to create group");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__obj_create() */

/*-------------------------------------------------------------------------
 * Function:    H5G__obj_create_real
 *
 * Purpose:     Create an object header for a group and update object location info
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__obj_create_real(H5F_t *f, const H5O_ginfo_t *ginfo, const H5O_linfo_t *linfo, const H5O_pline_t *pline,
                     H5G_obj_create_t *gcrt_info, H5O_loc_t *oloc /*out*/)
{
    size_t hdr_size;                       /* Size of object header to request */
    bool   use_at_least_v18;               /* Flag indicating the new group format should be used */
    hid_t  gcpl_id   = gcrt_info->gcpl_id; /* Group creation property list ID */
    herr_t ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(ginfo);
    assert(linfo);
    assert(pline);
    assert(oloc);

    /* Check for invalid access request */
    if (0 == (H5F_INTENT(f) & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "no write intent on file");

    /* Check for using the latest version of the group format which is introduced in v18 */
    /* (add more checks for creating "new format" groups when needed) */
    if ((H5F_LOW_BOUND(f) >= H5F_LIBVER_V18) || linfo->track_corder || (pline && pline->nused))
        use_at_least_v18 = true;
    else
        use_at_least_v18 = false;

    /* Make certain that the creation order is being tracked if an index is
     *  going to be built on it.
     */
    if (linfo->index_corder && !linfo->track_corder)
        HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "must track creation order to create index for it");

    /* Check if we should be using the latest version of the group format */
    if (use_at_least_v18) {
        H5O_link_t lnk;              /* Temporary link message info for computing message size */
        char       null_char = '\0'; /* Character for creating null string */
        size_t     ginfo_size;       /* Size of the group info message */
        size_t     linfo_size;       /* Size of the link info message */
        size_t     pline_size = 0;   /* Size of the pipeline message */
        size_t     link_size;        /* Size of a link message */

        /* Calculate message size information, for creating group's object header */
        linfo_size = H5O_msg_size_f(f, gcpl_id, H5O_LINFO_ID, linfo, (size_t)0);
        assert(linfo_size);

        ginfo_size = H5O_msg_size_f(f, gcpl_id, H5O_GINFO_ID, ginfo, (size_t)0);
        assert(ginfo_size);

        if (pline && pline->nused) {
            pline_size = H5O_msg_size_f(f, gcpl_id, H5O_PLINE_ID, pline, (size_t)0);
            assert(pline_size);
        } /* end if */

        lnk.type         = H5L_TYPE_HARD;
        lnk.corder       = 0;
        lnk.corder_valid = linfo->track_corder;
        lnk.cset         = H5T_CSET_ASCII;
        lnk.name         = &null_char;
        link_size        = H5O_msg_size_f(f, gcpl_id, H5O_LINK_ID, &lnk, (size_t)ginfo->est_name_len);
        assert(link_size);

        /* Compute size of header to use for creation */
        hdr_size = linfo_size + ginfo_size + pline_size + (ginfo->est_num_entries * link_size);
    } /* end if */
    else
        hdr_size = (size_t)(4 + 2 * H5F_SIZEOF_ADDR(f));

    /*
     * Create group's object header.  It has a zero link count
     * since nothing refers to it yet.	The link count will be
     * incremented if the object is added to the group directed graph.
     */
    if (H5O_create(f, hdr_size, (size_t)1, gcpl_id, oloc /*out*/) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create header");

    /* Check for format of group to create */
    if (use_at_least_v18) {
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        /* Insert link info message */
        if (H5O_msg_create(oloc, H5O_LINFO_ID, 0, H5O_UPDATE_TIME, (void *)linfo) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create message");

        /* Insert group info message */
        if (H5O_msg_create(oloc, H5O_GINFO_ID, H5O_MSG_FLAG_CONSTANT, 0, (void *)ginfo) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create message");

        /* Insert pipeline message */
        if (pline && pline->nused)
            if (H5O_msg_create(oloc, H5O_PLINE_ID, H5O_MSG_FLAG_CONSTANT, 0, (void *)pline) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create message");
        H5_GCC_CLANG_DIAG_ON("cast-qual")
    } /* end if */
    else {
        H5O_stab_t stab; /* Symbol table message	*/

        /* The group doesn't currently have a 'stab' message, go create one */
        if (H5G__stab_create(oloc, ginfo, &stab) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create symbol table");

        /* Cache the symbol table information */
        gcrt_info->cache_type            = H5G_CACHED_STAB;
        gcrt_info->cache.stab.btree_addr = stab.btree_addr;
        gcrt_info->cache.stab.heap_addr  = stab.heap_addr;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__obj_create_real() */

/*-------------------------------------------------------------------------
 * Function:    H5G__obj_get_linfo
 *
 * Purpose:     Retrieves the "link info" message for an object.  Also
 *              sets the number of links correctly, if it isn't set up yet.
 *
 * Return:	Success:	true/false whether message was found & retrieved
 *              Failure:        FAIL if error occurred
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5G__obj_get_linfo(const H5O_loc_t *grp_oloc, H5O_linfo_t *linfo)
{
    H5B2_t *bt2_name  = NULL; /* v2 B-tree handle for name index */
    htri_t  ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(grp_oloc->addr)

    /* check arguments */
    assert(grp_oloc);
    assert(linfo);

    /* Check for the group having a link info message */
    if ((ret_value = H5O_msg_exists(grp_oloc, H5O_LINFO_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to read object header");
    if (ret_value) {
        /* Retrieve the "link info" structure */
        if (NULL == H5O_msg_read(grp_oloc, H5O_LINFO_ID, linfo))
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "link info message not present");

        /* Check if we don't know how many links there are */
        if (linfo->nlinks == HSIZET_MAX) {
            /* Check if we are using "dense" link storage */
            if (H5_addr_defined(linfo->fheap_addr)) {
                /* Open the name index v2 B-tree */
                if (NULL == (bt2_name = H5B2_open(grp_oloc->file, linfo->name_bt2_addr, NULL)))
                    HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

                /* Retrieve # of records in "name" B-tree */
                /* (should be same # of records in all indices) */
                if (H5B2_get_nrec(bt2_name, &linfo->nlinks) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't retrieve # of records in index");
            } /* end if */
            else {
                /* Retrieve # of links from object header */
                if (H5O_get_nlinks(grp_oloc, &linfo->nlinks) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't retrieve # of links for object");
            } /* end if */
        }     /* end if */
    }         /* end if */

done:
    /* Release resources */
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for name index");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G__obj_get_linfo() */

/*-------------------------------------------------------------------------
 * Function:	H5G__obj_compact_to_dense_cb
 *
 * Purpose:	Callback routine for converting "compact" to "dense"
 *              link storage form.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__obj_compact_to_dense_cb(const void *_mesg, unsigned H5_ATTR_UNUSED idx, void *_udata)
{
    const H5O_link_t    *lnk       = (const H5O_link_t *)_mesg;     /* Pointer to link */
    H5G_obj_oh_it_ud1_t *udata     = (H5G_obj_oh_it_ud1_t *)_udata; /* 'User data' passed in */
    herr_t               ret_value = H5_ITER_CONT;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(lnk);
    assert(udata);

    /* Insert link into dense link storage */
    if (H5G__dense_insert(udata->f, udata->linfo, lnk) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert link into dense storage");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__obj_compact_to_dense_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__obj_stab_to_new_cb
 *
 * Purpose:	Callback routine for converting "symbol table" link storage to
 *              "new format" storage (either "compact" or "dense" storage).
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__obj_stab_to_new_cb(const H5O_link_t *lnk, void *_udata)
{
    H5G_obj_stab_it_ud1_t *udata     = (H5G_obj_stab_it_ud1_t *)_udata; /* 'User data' passed in */
    herr_t                 ret_value = H5_ITER_CONT;                    /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(lnk);
    assert(udata);

    /* Insert link into group */
    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    if (H5G_obj_insert(udata->grp_oloc, lnk->name, (H5O_link_t *)lnk, false, H5O_TYPE_UNKNOWN, NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, H5_ITER_ERROR, "can't insert link into group");
    H5_GCC_CLANG_DIAG_ON("cast-qual")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__obj_stab_to_new_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G_obj_insert
 *
 * Purpose:	Insert a new symbol into the group described by GRP_OLOC.
 *		file F.	 The name of the new symbol is NAME and its symbol
 *		table entry is OBJ_LNK.  Increment the reference
 *              count for the object the link points if OBJ_LNK is a hard link
 *              and ADJ_LINK is true.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_obj_insert(const H5O_loc_t *grp_oloc, const char *name, H5O_link_t *obj_lnk, bool adj_link,
               H5O_type_t obj_type, const void *crt_info)
{
    H5O_pline_t  tmp_pline;             /* Pipeline message */
    H5O_pline_t *pline = NULL;          /* Pointer to pipeline message */
    H5O_linfo_t  linfo;                 /* Link info message */
    htri_t       linfo_exists;          /* Whether the link info message exists */
    bool         use_old_format;        /* Whether to use 'old format' (symbol table) for insertions or not */
    bool         use_new_dense = false; /* Whether to use "dense" form of 'new format' group */
    herr_t       ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(grp_oloc->addr, FAIL)

    /* check arguments */
    assert(grp_oloc && grp_oloc->file);
    assert(name && *name);
    assert(obj_lnk);

    /* Check if we have information about the number of objects in this group */
    /* (by attempting to get the link info message for this group) */
    if ((linfo_exists = H5G__obj_get_linfo(grp_oloc, &linfo)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check for link info message");
    if (linfo_exists) {
        H5O_ginfo_t ginfo;         /* Group info message */
        size_t      link_msg_size; /* Size of new link message in the file */

        /* Using the new format for groups */
        use_old_format = false;

        /* Check for tracking creation order on this group's links */
        if (linfo.track_corder) {
            /* Set the creation order for the new link & indicate that it's valid */
            obj_lnk->corder       = linfo.max_corder;
            obj_lnk->corder_valid = true;

            /* Increment the max. creation order used in the group */
            linfo.max_corder++;
        } /* end if */

        /* Get the link's message size */
        if ((link_msg_size = H5O_msg_raw_size(grp_oloc->file, H5O_LINK_ID, false, obj_lnk)) == 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGETSIZE, FAIL, "can't get link size");

        /* Get the group info */
        if (NULL == H5O_msg_read(grp_oloc, H5O_GINFO_ID, &ginfo))
            HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "can't get group info");

        /* If there's still a small enough number of links, use the 'link' message */
        /* (If the encoded form of the link is too large to fit into an object
         *  header message, convert to using dense link storage instead of link messages)
         */
        if (H5_addr_defined(linfo.fheap_addr))
            use_new_dense = true;
        else if (linfo.nlinks < ginfo.max_compact && link_msg_size < H5O_MESG_MAX_SIZE)
            use_new_dense = false;
        else {
            htri_t              pline_exists; /* Whether the pipeline message exists */
            H5G_obj_oh_it_ud1_t udata;        /* User data for iteration */
            H5O_mesg_operator_t op;           /* Message operator */

            /* Get the pipeline message, if it exists */
            if ((pline_exists = H5O_msg_exists(grp_oloc, H5O_PLINE_ID)) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to read object header");
            if (pline_exists) {
                if (NULL == H5O_msg_read(grp_oloc, H5O_PLINE_ID, &tmp_pline))
                    HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "can't get link pipeline");
                pline = &tmp_pline;
            } /* end if */

            /* The group doesn't currently have "dense" storage for links */
            if (H5G__dense_create(grp_oloc->file, &linfo, pline) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create 'dense' form of new format group");

            /* Set up user data for object header message iteration */
            udata.f       = grp_oloc->file;
            udata.oh_addr = grp_oloc->addr;
            udata.linfo   = &linfo;

            /* Iterate over the 'link' messages, inserting them into the dense link storage  */
            op.op_type  = H5O_MESG_OP_APP;
            op.u.app_op = H5G__obj_compact_to_dense_cb;
            if (H5O_msg_iterate(grp_oloc, H5O_LINK_ID, &op, &udata) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "error iterating over links");

            /* Remove all the 'link' messages */
            if (H5O_msg_remove(grp_oloc, H5O_LINK_ID, H5O_ALL, false) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete link messages");

            use_new_dense = true;
        } /* end else */
    }     /* end if */
    else {
        /* Check for new-style link information */
        if (obj_lnk->cset != H5T_CSET_ASCII || obj_lnk->type > H5L_TYPE_BUILTIN_MAX) {
            H5O_linfo_t           new_linfo = H5G_CRT_LINK_INFO_DEF;  /* Link information */
            H5O_ginfo_t           new_ginfo = H5G_CRT_GROUP_INFO_DEF; /* Group information */
            H5G_obj_stab_it_ud1_t udata;                              /* User data for iteration */

            /* Convert group to "new format" group, in order to hold the information */

            /* Insert link info message */
            if (H5O_msg_create(grp_oloc, H5O_LINFO_ID, 0, 0, &new_linfo) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create message");

            /* Insert group info message */
            if (H5O_msg_create(grp_oloc, H5O_GINFO_ID, H5O_MSG_FLAG_CONSTANT, H5O_UPDATE_TIME, &new_ginfo) <
                0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create message");

            /* Set up user data for iteration */
            udata.grp_oloc = grp_oloc;

            /* Iterate through all links in "old format" group and insert them into new format */
            if (H5G__stab_iterate(grp_oloc, H5_ITER_NATIVE, (hsize_t)0, NULL, H5G__obj_stab_to_new_cb,
                                  &udata) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTNEXT, FAIL, "error iterating over old format links");

            /* Remove the symbol table message from the group */
            if (H5O_msg_remove(grp_oloc, H5O_STAB_ID, 0, false) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete old format link storage");

            /* Recursively call this routine to insert the new link, since the
             *  group is in the "new format" now and the link info should be
             *  set up, etc.
             */
            if (H5G_obj_insert(grp_oloc, name, obj_lnk, adj_link, obj_type, crt_info) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert link into group");

            /* Done with insertion now */
            HGOTO_DONE(SUCCEED);
        } /* end if */
        else
            use_old_format = true;
    } /* end if */

    /* Insert into symbol table or "dense" storage */
    if (use_old_format) {
        /* Insert into symbol table */
        if (H5G__stab_insert(grp_oloc, name, obj_lnk, obj_type, crt_info) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert entry into symbol table");
    } /* end if */
    else {
        if (use_new_dense) {
            /* Insert into dense link storage */
            if (H5G__dense_insert(grp_oloc->file, &linfo, obj_lnk) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert link into dense storage");
        } /* end if */
        else {
            /* Insert with link message */
            if (H5G__compact_insert(grp_oloc, obj_lnk) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert link as link message");
        } /* end else */
    }     /* end else */

    /* Increment the number of objects in this group */
    if (!use_old_format) {
        linfo.nlinks++;
        if (H5O_msg_write(grp_oloc, H5O_LINFO_ID, 0, H5O_UPDATE_TIME, &linfo) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't update link info message");
    } /* end if */

    /* Increment link count on object, if requested and it's a hard link */
    if (adj_link && obj_lnk->type == H5L_TYPE_HARD) {
        H5O_loc_t obj_oloc; /* Object location */
        H5O_loc_reset(&obj_oloc);

        /* Create temporary object location */
        obj_oloc.file = grp_oloc->file;
        obj_oloc.addr = obj_lnk->u.hard.addr;

        /* Increment reference count for object */
        if (H5O_link(&obj_oloc, 1) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_LINKCOUNT, FAIL, "unable to increment hard link count");
    } /* end if */

done:
    /* Free any space used by the pipeline message */
    if (pline && H5O_msg_reset(H5O_PLINE_ID, pline) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "can't release pipeline");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G_obj_insert() */

/*-------------------------------------------------------------------------
 * Function:	H5G__obj_iterate
 *
 * Purpose:     Private function for H5Giterate.
 *              Iterates over objects in a group
 *
 * Return:	Success:        Non-negative
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__obj_iterate(const H5O_loc_t *grp_oloc, H5_index_t idx_type, H5_iter_order_t order, hsize_t skip,
                 hsize_t *last_lnk, H5G_lib_iterate_t op, void *op_data)
{
    H5O_linfo_t linfo;            /* Link info message */
    htri_t      linfo_exists;     /* Whether the link info message exists */
    herr_t      ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(grp_oloc->addr)

    /* Sanity check */
    assert(grp_oloc);
    assert(op);

    /* Attempt to get the link info for this group */
    if ((linfo_exists = H5G__obj_get_linfo(grp_oloc, &linfo)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check for link info message");
    if (linfo_exists) {
        /* Check for going out of bounds */
        if (skip > 0 && (size_t)skip >= linfo.nlinks)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "index out of bound");

        /* Check for creation order tracking, if creation order index lookup requested */
        if (idx_type == H5_INDEX_CRT_ORDER) {
            /* Check if creation order is tracked */
            if (!linfo.track_corder)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "creation order not tracked for links in group");
        } /* end if */

        if (H5_addr_defined(linfo.fheap_addr)) {
            /* Iterate over the links in the group, building a table of the link messages */
            if ((ret_value = H5G__dense_iterate(grp_oloc->file, &linfo, idx_type, order, skip, last_lnk, op,
                                                op_data)) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't iterate over dense links");
        } /* end if */
        else {
            /* Get the object's name from the link messages */
            if ((ret_value = H5G__compact_iterate(grp_oloc, &linfo, idx_type, order, skip, last_lnk, op,
                                                  op_data)) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't iterate over compact links");
        } /* end else */
    }     /* end if */
    else {
        /* Can only perform name lookups on groups with symbol tables */
        if (idx_type != H5_INDEX_NAME)
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "no creation order index to query");

        /* Iterate over symbol table */
        if ((ret_value = H5G__stab_iterate(grp_oloc, order, skip, last_lnk, op, op_data)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't iterate over symbol table");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G__obj_iterate() */

/*-------------------------------------------------------------------------
 * Function:	H5G__obj_info
 *
 * Purpose:	Retrieve information about a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__obj_info(const H5O_loc_t *oloc, H5G_info_t *grp_info)
{
    H5G_t      *grp = NULL;          /* Group to query */
    H5G_loc_t   grp_loc;             /* Entry of group to be queried */
    H5G_name_t  grp_path;            /* Group hier. path */
    H5O_loc_t   grp_oloc;            /* Group object location */
    H5O_linfo_t linfo;               /* Link info message */
    htri_t      linfo_exists;        /* Whether the link info message exists */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(oloc);
    assert(grp_info);

    /* Set up group location to fill in */
    grp_loc.oloc = &grp_oloc;
    grp_loc.path = &grp_path;
    H5G_loc_reset(&grp_loc);

    /* Deep copy (duplicate) of the group location object */
    if (H5O_loc_copy_deep(&grp_oloc, oloc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "can't copy object location");

    /* Open the group */
    if (NULL == (grp = H5G_open(&grp_loc)))
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "mount point not found");

    /* Get information from the group */
    grp_info->mounted = H5G_MOUNTED(grp);

    /* Attempt to get the link info for this group */
    if ((linfo_exists = H5G__obj_get_linfo(oloc, &linfo)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check for link info message");
    if (linfo_exists) {
        /* Retrieve the information about the links */
        grp_info->nlinks     = linfo.nlinks;
        grp_info->max_corder = linfo.max_corder;

        /* Check if the group is using compact or dense storage for its links */
        if (H5_addr_defined(linfo.fheap_addr))
            grp_info->storage_type = H5G_STORAGE_TYPE_DENSE;
        else
            grp_info->storage_type = H5G_STORAGE_TYPE_COMPACT;
    } /* end if */
    else {
        /* Get the number of objects in this group by iterating over symbol table */
        if (H5G__stab_count(oloc, &grp_info->nlinks) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTCOUNT, FAIL, "can't count objects");

        /* Set the other information about the group */
        grp_info->storage_type = H5G_STORAGE_TYPE_SYMBOL_TABLE;
        grp_info->max_corder   = 0;
    } /* end else */

done:
    /* Clean up resources */
    if (grp && H5G_close(grp) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTCLOSEOBJ, FAIL, "unable to close queried group");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__obj_info() */

/*-------------------------------------------------------------------------
 * Function:	H5G_obj_get_name_by_idx
 *
 * Purpose:     Returns the name of link in a group by giving index.
 *
 * Return:	Success:        Non-negative, length of name
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_obj_get_name_by_idx(const H5O_loc_t *oloc, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                        char *name, size_t name_size, size_t *name_len)
{
    H5O_linfo_t linfo;               /* Link info message */
    htri_t      linfo_exists;        /* Whether the link info message exists */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(oloc->addr, FAIL)

    /* Sanity check */
    assert(oloc && oloc->file);

    /* Attempt to get the link info for this group */
    if ((linfo_exists = H5G__obj_get_linfo(oloc, &linfo)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check for link info message");
    if (linfo_exists) {
        /* Check for creation order tracking, if creation order index lookup requested */
        if (idx_type == H5_INDEX_CRT_ORDER)
            /* Check if creation order is tracked */
            if (!linfo.track_corder)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "creation order not tracked for links in group");

        /* Check for dense link storage */
        if (H5_addr_defined(linfo.fheap_addr)) {
            /* Get the object's name from the dense link storage */
            if (H5G__dense_get_name_by_idx(oloc->file, &linfo, idx_type, order, n, name, name_size,
                                           name_len) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't locate name");
        } /* end if */
        else {
            /* Get the object's name from the link messages */
            if (H5G__compact_get_name_by_idx(oloc, &linfo, idx_type, order, n, name, name_size, name_len) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't locate name");
        } /* end else */
    }     /* end if */
    else {
        /* Can only perform name lookups on groups with symbol tables */
        if (idx_type != H5_INDEX_NAME)
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "no creation order index to query");

        /* Get the object's name from the symbol table */
        if (H5G__stab_get_name_by_idx(oloc, order, n, name, name_size, name_len) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't locate name");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G_obj_get_name_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5G__obj_remove_update_linfo
 *
 * Purpose:     Update the link info after removing a link from a group
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__obj_remove_update_linfo(const H5O_loc_t *oloc, H5O_linfo_t *linfo)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(oloc);
    assert(linfo);

    /* Decrement # of links in group */
    linfo->nlinks--;

    /* Reset the creation order min/max if there's no more links in group */
    if (linfo->nlinks == 0)
        linfo->max_corder = 0;

    /* Check for transitioning out of dense storage, if we are using it */
    if (H5_addr_defined(linfo->fheap_addr)) {
        /* Check if there's no more links */
        if (linfo->nlinks == 0) {
            /* Delete the dense storage */
            if (H5G__dense_delete(oloc->file, linfo, false) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete dense link storage");
        } /* end if */
        /* Check for switching back to compact storage */
        else {
            H5O_ginfo_t ginfo; /* Group info message            */

            /* Get the group info */
            if (NULL == H5O_msg_read(oloc, H5O_GINFO_ID, &ginfo))
                HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "can't get group info");

            /* Check if we should switch from dense storage back to link messages */
            if (linfo->nlinks < ginfo.min_dense) {
                struct H5O_t    *oh = NULL;          /* Pointer to group's object header */
                H5G_link_table_t ltable;             /* Table of links */
                bool             can_convert = true; /* Whether converting to link messages is possible */
                size_t           u;                  /* Local index */

                /* Build the table of links for this group */
                if (H5G__dense_build_table(oloc->file, linfo, H5_INDEX_NAME, H5_ITER_NATIVE, &ltable) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTNEXT, FAIL, "error iterating over links");

                /* Pin the object header */
                if (NULL == (oh = H5O_pin(oloc)))
                    HGOTO_ERROR(H5E_SYM, H5E_CANTPIN, FAIL, "unable to pin group object header");

                /* Inspect links in table for ones that can't be converted back
                 * into link message form (currently only links which can't fit
                 * into an object header message)
                 */
                for (u = 0; u < linfo->nlinks; u++)
                    if (H5O_msg_size_oh(oloc->file, oh, H5O_LINK_ID, &(ltable.lnks[u]), (size_t)0) >=
                        H5O_MESG_MAX_SIZE) {
                        can_convert = false;
                        break;
                    } /* end if */

                /* If ok, insert links as link messages */
                if (can_convert) {
                    /* Insert link messages into group */
                    for (u = 0; u < linfo->nlinks; u++)
                        if (H5O_msg_append_oh(oloc->file, oh, H5O_LINK_ID, 0, H5O_UPDATE_TIME,
                                              &(ltable.lnks[u])) < 0) {
                            /* Release object header */
                            if (H5O_unpin(oh) < 0)
                                HDONE_ERROR(H5E_SYM, H5E_CANTUNPIN, FAIL,
                                            "unable to unpin group object header");

                            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create message");
                        } /* end if */

                    /* Remove the dense storage */
                    if (H5G__dense_delete(oloc->file, linfo, false) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete dense link storage");
                } /* end if */

                /* Release object header */
                if (H5O_unpin(oh) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTUNPIN, FAIL, "unable to unpin group object header");

                /* Free link table information */
                if (H5G__link_release_table(&ltable) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");
            } /* end if */
        }     /* end else */
    }         /* end if */

    /* Update link info in the object header */
    if (H5O_msg_write(oloc, H5O_LINFO_ID, 0, H5O_UPDATE_TIME, linfo) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't update link info message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__obj_remove_update_linfo() */

/*-------------------------------------------------------------------------
 * Function:	H5G_obj_remove
 *
 * Purpose:     Remove a link from a group.
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_obj_remove(const H5O_loc_t *oloc, H5RS_str_t *grp_full_path_r, const char *name)
{
    H5O_linfo_t linfo;               /* Link info message            */
    htri_t      linfo_exists;        /* Whether the link info message exists */
    bool        use_old_format;      /* Whether to use 'old format' (symbol table) for deletion or not */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(oloc->addr, FAIL)

    /* Sanity check */
    assert(oloc);
    assert(name && *name);

    /* Attempt to get the link info for this group */
    if ((linfo_exists = H5G__obj_get_linfo(oloc, &linfo)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check for link info message");
    if (linfo_exists) {
        /* Using the new format for groups */
        use_old_format = false;

        /* Check for dense or compact storage */
        if (H5_addr_defined(linfo.fheap_addr)) {
            /* Remove object from the dense link storage */
            if (H5G__dense_remove(oloc->file, &linfo, grp_full_path_r, name) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't remove object");
        } /* end if */
        else
            /* Remove object from the link messages */
            if (H5G__compact_remove(oloc, grp_full_path_r, name) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't remove object");
    } /* end if */
    else {
        /* Using the old format for groups */
        use_old_format = true;

        /* Remove object from the symbol table */
        if (H5G__stab_remove(oloc, grp_full_path_r, name) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't remove object");
    } /* end else */

    /* Update link info for a new-style group */
    if (!use_old_format)
        if (H5G__obj_remove_update_linfo(oloc, &linfo) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTUPDATE, FAIL, "unable to update link info");

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G_obj_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5G_obj_remove_by_idx
 *
 * Purpose:     Remove a link from a group, according to the order within an index.
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_obj_remove_by_idx(const H5O_loc_t *grp_oloc, H5RS_str_t *grp_full_path_r, H5_index_t idx_type,
                      H5_iter_order_t order, hsize_t n)
{
    H5O_linfo_t linfo;               /* Link info message            */
    htri_t      linfo_exists;        /* Whether the link info message exists */
    bool        use_old_format;      /* Whether to use 'old format' (symbol table) for deletion or not */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(grp_oloc && grp_oloc->file);

    /* Attempt to get the link info for this group */
    if ((linfo_exists = H5G__obj_get_linfo(grp_oloc, &linfo)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check for link info message");
    if (linfo_exists) {
        /* Check for creation order tracking, if creation order index lookup requested */
        if (idx_type == H5_INDEX_CRT_ORDER) {
            /* Check if creation order is tracked */
            if (!linfo.track_corder)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "creation order not tracked for links in group");
        } /* end if */

        /* Using the new format for groups */
        use_old_format = false;

        /* Check for dense or compact storage */
        if (H5_addr_defined(linfo.fheap_addr)) {
            /* Remove object from the dense link storage */
            if (H5G__dense_remove_by_idx(grp_oloc->file, &linfo, grp_full_path_r, idx_type, order, n) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't remove object");
        } /* end if */
        else {
            /* Remove object from compact link storage */
            if (H5G__compact_remove_by_idx(grp_oloc, &linfo, grp_full_path_r, idx_type, order, n) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't remove object");
        } /* end else */
    }     /* end if */
    else {
        /* Can only perform name lookups on groups with symbol tables */
        if (idx_type != H5_INDEX_NAME)
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "no creation order index to query");

        /* Using the old format for groups */
        use_old_format = true;

        /* Remove object from the symbol table */
        if (H5G__stab_remove_by_idx(grp_oloc, grp_full_path_r, order, n) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't remove object");
    } /* end else */

    /* Update link info for a new-style group */
    if (!use_old_format)
        if (H5G__obj_remove_update_linfo(grp_oloc, &linfo) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTUPDATE, FAIL, "unable to update link info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_obj_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5G__obj_lookup
 *
 * Purpose:	Look up a link in a group, using the name as the key.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__obj_lookup(const H5O_loc_t *grp_oloc, const char *name, bool *found, H5O_link_t *lnk)
{
    H5O_linfo_t linfo;               /* Link info message */
    htri_t      linfo_exists;        /* Whether the link info message exists */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(grp_oloc->addr)

    /* check arguments */
    assert(grp_oloc && grp_oloc->file);
    assert(name && *name);

    /* Attempt to get the link info message for this group */
    if ((linfo_exists = H5G__obj_get_linfo(grp_oloc, &linfo)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check for link info message");
    if (linfo_exists) {
        /* Check for dense link storage */
        if (H5_addr_defined(linfo.fheap_addr)) {
            /* Get the object's info from the dense link storage */
            if (H5G__dense_lookup(grp_oloc->file, &linfo, name, found, lnk) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't locate object");
        } /* end if */
        else {
            /* Get the object's info from the link messages */
            if (H5G__compact_lookup(grp_oloc, name, found, lnk) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't locate object");
        } /* end else */
    }     /* end if */
    else
        /* Get the object's info from the symbol table */
        if (H5G__stab_lookup(grp_oloc, name, found, lnk) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't locate object");

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G__obj_lookup() */

/*-------------------------------------------------------------------------
 * Function:	H5G_obj_lookup_by_idx
 *
 * Purpose:	Look up link info in a group, according to an order within an
 *              index.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_obj_lookup_by_idx(const H5O_loc_t *grp_oloc, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                      H5O_link_t *lnk)
{
    H5O_linfo_t linfo;               /* Link info message */
    htri_t      linfo_exists;        /* Whether the link info message exists */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(grp_oloc->addr, FAIL)

    /* check arguments */
    assert(grp_oloc && grp_oloc->file);

    /* Attempt to get the link info message for this group */
    if ((linfo_exists = H5G__obj_get_linfo(grp_oloc, &linfo)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check for link info message");
    if (linfo_exists) {
        /* Check for creation order tracking, if creation order index lookup requested */
        if (idx_type == H5_INDEX_CRT_ORDER) {
            /* Check if creation order is tracked */
            if (!linfo.track_corder)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "creation order not tracked for links in group");
        } /* end if */

        /* Check for dense link storage */
        if (H5_addr_defined(linfo.fheap_addr)) {
            /* Get the link from the dense storage */
            if (H5G__dense_lookup_by_idx(grp_oloc->file, &linfo, idx_type, order, n, lnk) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't locate object");
        } /* end if */
        else {
            /* Get the link from the link messages */
            if (H5G__compact_lookup_by_idx(grp_oloc, &linfo, idx_type, order, n, lnk) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't locate object");
        } /* end else */
    }     /* end if */
    else {
        /* Can only perform name lookups on groups with symbol tables */
        if (idx_type != H5_INDEX_NAME)
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "no creation order index to query");

        /* Get the object's info from the symbol table */
        if (H5G__stab_lookup_by_idx(grp_oloc, order, n, lnk) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't locate object");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G_obj_lookup_by_idx() */
