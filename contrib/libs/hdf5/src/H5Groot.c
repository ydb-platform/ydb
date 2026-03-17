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
 * Created:		H5Groot.c
 *
 * Purpose:		Functions for operating on the root group.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#define H5F_FRIEND     /*suppress error about including H5Fpkg	  */
#include "H5Gmodule.h" /* This source code file is part of the H5G module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fpkg.h"      /* File access				*/
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Pprivate.h"  /* Property Lists			*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

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
 * Function:	H5G_rootof
 *
 * Purpose:	Return a pointer to the root group of the file.  If the file
 *		is part of a virtual file then the root group of the virtual
 *		file is returned.
 *
 * Return:	Success:	Ptr to the root group of the file.  Do not
 *				free the pointer -- it points directly into
 *				the file struct.
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
H5G_t *
H5G_rootof(H5F_t *f)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(f);
    assert(f->shared);

    /* Walk to top of mounted files */
    while (f->parent)
        f = f->parent;

    /* Sanity check */
    assert(f);
    assert(f->shared);
    assert(f->shared->root_grp);

    /* Check to see if the root group was opened through a different
     * "top" file, and switch it to point at the current "top" file.
     */
    if (f->shared->root_grp->oloc.file != f)
        f->shared->root_grp->oloc.file = f;

    FUNC_LEAVE_NOAPI(f->shared->root_grp)
} /* end H5G_rootof() */

/*-------------------------------------------------------------------------
 * Function:	H5G_mkroot
 *
 * Purpose:	Creates a root group in an empty file and opens it.  If a
 *		root group is already open then this function immediately
 *		returns.   If ENT is non-null then it's the symbol table
 *		entry for an existing group which will be opened as the root
 *		group.  Otherwise a new root group is created and then
 *		opened.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_mkroot(H5F_t *f, bool create_root)
{
    H5G_loc_t        root_loc;               /* Root location information */
    H5G_obj_create_t gcrt_info;              /* Root group object creation info */
    htri_t           stab_exists  = -1;      /* Whether the symbol table exists */
    bool             sblock_dirty = false;   /* Whether superblock was dirtied */
    bool             path_init    = false;   /* Whether path was initialized */
    herr_t           ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(f);
    assert(f->shared);
    assert(f->shared->sblock);

    /* Check if the root group is already initialized */
    if (f->shared->root_grp)
        HGOTO_DONE(SUCCEED);

    /* Create information needed for group nodes */
    if (H5G__node_init(f) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create group node info");

    /*
     * Create the group pointer
     */
    if (NULL == (f->shared->root_grp = H5FL_CALLOC(H5G_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
    if (NULL == (f->shared->root_grp->shared = H5FL_CALLOC(H5G_shared_t))) {
        f->shared->root_grp = H5FL_FREE(H5G_t, f->shared->root_grp);
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
    } /* end if */

    /* Initialize the root_loc structure to point to fields in the newly created
     * f->shared->root_grp structure */
    root_loc.oloc = &(f->shared->root_grp->oloc);
    root_loc.path = &(f->shared->root_grp->path);
    H5G_loc_reset(&root_loc);

    /*
     * If there is no root object then create one. The root group always starts
     * with a hard link count of one since it's pointed to by the superblock.
     */
    if (create_root) {
        /* Create root group */
        /* (Pass the FCPL which is a sub-class of the group creation property class) */
        gcrt_info.gcpl_id    = f->shared->fcpl_id;
        gcrt_info.cache_type = H5G_NOTHING_CACHED;
        if (H5G__obj_create(f, &gcrt_info, root_loc.oloc /*out*/) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create group entry");
        if (1 != H5O_link(root_loc.oloc, 1))
            HGOTO_ERROR(H5E_SYM, H5E_LINKCOUNT, FAIL, "internal error (wrong link count)");

        /* Decrement refcount on root group's object header in memory */
        if (H5O_dec_rc_by_loc(root_loc.oloc) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTDEC, FAIL,
                        "unable to decrement refcount on root group's object header");

        /* Mark superblock dirty, so root group info is flushed */
        sblock_dirty = true;

        /* Create the root group symbol table entry */
        assert(!f->shared->sblock->root_ent);
        if (f->shared->sblock->super_vers < HDF5_SUPERBLOCK_VERSION_2) {
            /* Allocate space for the root group symbol table entry */
            if (NULL == (f->shared->sblock->root_ent = (H5G_entry_t *)H5MM_calloc(sizeof(H5G_entry_t))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate space for symbol table entry");

            /* Initialize the root group symbol table entry */
            f->shared->sblock->root_ent->type = gcrt_info.cache_type;
            if (gcrt_info.cache_type != H5G_NOTHING_CACHED)
                f->shared->sblock->root_ent->cache = gcrt_info.cache;
            f->shared->sblock->root_ent->name_off = 0; /* No name (yet) */
            f->shared->sblock->root_ent->header   = root_loc.oloc->addr;
        } /* end if */
    }     /* end if */
    else {
        /* Create root group object location from f */
        root_loc.oloc->addr = f->shared->sblock->root_addr;
        root_loc.oloc->file = f;

        /*
         * Open the root object as a group.
         */
        if (H5O_open(root_loc.oloc) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open root group");

        /* Actions to take if the symbol table information is cached */
        if (f->shared->sblock->root_ent && f->shared->sblock->root_ent->type == H5G_CACHED_STAB) {
            /* Check for the situation where the symbol table is cached but does
             * not exist.  This can happen if, for example, an external link is
             * added to the root group. */
            if ((stab_exists = H5O_msg_exists(root_loc.oloc, H5O_STAB_ID)) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check if symbol table message exists");

            /* Remove the cache if the stab does not exist */
            if (!stab_exists)
                f->shared->sblock->root_ent->type = H5G_NOTHING_CACHED;
#ifndef H5_STRICT_FORMAT_CHECKS
            /* If symbol table information is cached, check if we should replace the
             * symbol table message with the cached symbol table information */
            else if (H5F_INTENT(f) & H5F_ACC_RDWR) {
                H5O_stab_t cached_stab;

                /* Retrieve the cached symbol table information */
                cached_stab.btree_addr = f->shared->sblock->root_ent->cache.stab.btree_addr;
                cached_stab.heap_addr  = f->shared->sblock->root_ent->cache.stab.heap_addr;

                /* Check if the symbol table message is valid, and replace with the
                 * cached symbol table if necessary */
                if (H5G__stab_valid(root_loc.oloc, &cached_stab) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to verify symbol table");
            } /* end if */
#endif        /* H5_STRICT_FORMAT_CHECKS */
        }     /* end if */
    }         /* end else */

    /* Cache the root group's symbol table information in the root group symbol
     * table entry.  It will have been allocated by now if it needs to be
     * present, so we don't need to check the superblock version.  We do this if
     * we have write access, the root entry has been allocated (i.e.
     * super_vers < 2) and the stab info is not already cached. */
    if ((H5F_INTENT(f) & H5F_ACC_RDWR) && stab_exists != false && f->shared->sblock->root_ent &&
        f->shared->sblock->root_ent->type != H5G_CACHED_STAB) {
        H5O_stab_t stab; /* Symbol table */

        /* Check if the stab message exists.  It's possible for the root group
         * to use the latest version while the superblock is an old version.
         * If stab_exists is not -1 then we have already checked. */
        if (stab_exists == -1 && (stab_exists = H5O_msg_exists(root_loc.oloc, H5O_STAB_ID)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't check if symbol table message exists");

        if (stab_exists) {
            /* Read the root group's symbol table message */
            if (NULL == H5O_msg_read(root_loc.oloc, H5O_STAB_ID, &stab))
                HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "unable to read symbol table message");

            /* Update the root group symbol table entry */
            f->shared->sblock->root_ent->type                  = H5G_CACHED_STAB;
            f->shared->sblock->root_ent->cache.stab.btree_addr = stab.btree_addr;
            f->shared->sblock->root_ent->cache.stab.heap_addr  = stab.heap_addr;

            /* Mark superblock dirty, so root group info is flushed */
            sblock_dirty = true;
        } /* end if */
    }     /* end if */

    /* Create the path names for the root group's entry */
    H5G__name_init(root_loc.path, "/");
    path_init = true;

    f->shared->root_grp->shared->fo_count = 1;
    /* The only other open object should be the superblock extension, if it
     * exists.  Don't count either the superblock extension or the root group
     * in the number of open objects in the file.
     */
    assert((1 == f->nopen_objs) || (2 == f->nopen_objs && HADDR_UNDEF != f->shared->sblock->ext_addr));
    f->nopen_objs--;

done:
    /* In case of error, free various memory locations that may have been
     * allocated */
    if (ret_value < 0) {
        if (f->shared->root_grp) {
            if (path_init)
                H5G_name_free(root_loc.path);
            if (f->shared->root_grp->shared)
                f->shared->root_grp->shared = H5FL_FREE(H5G_shared_t, f->shared->root_grp->shared);
            f->shared->root_grp = H5FL_FREE(H5G_t, f->shared->root_grp);
        } /* end if */
        if (f->shared->sblock)
            f->shared->sblock->root_ent = (H5G_entry_t *)H5MM_xfree(f->shared->sblock->root_ent);
    } /* end if */

    /* Mark superblock dirty in cache, if necessary */
    if (sblock_dirty)
        if (H5AC_mark_entry_dirty(f->shared->sblock) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTMARKDIRTY, FAIL, "unable to mark superblock as dirty");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_mkroot() */

/*-------------------------------------------------------------------------
 * Function:    H5G_root_free
 *
 * Purpose:	Free memory used by an H5G_t struct (and its H5G_shared_t).
 *		Does not close the group or decrement the reference count.
 *		Used to free memory used by the root group.
 *
 * Return:	Success:    Non-negative
 *		Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_root_free(H5G_t *grp)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(grp && grp->shared);
    assert(grp->shared->fo_count > 0);

    /* Free the path */
    H5G_name_free(&(grp->path));

    grp->shared = H5FL_FREE(H5G_shared_t, grp->shared);
    grp         = H5FL_FREE(H5G_t, grp);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G_root_free() */

/*-------------------------------------------------------------------------
 * Function:	H5G_root_loc
 *
 * Purpose:	Construct a "group location" for the root group of a file
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_root_loc(H5F_t *f, H5G_loc_t *loc)
{
    H5G_t *root_grp;            /* Pointer to root group's info */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(f);
    assert(loc);

    /* Retrieve the root group for the file */
    root_grp = H5G_rootof(f);
    assert(root_grp);

    /* Build the group location for the root group */
    if (NULL == (loc->oloc = H5G_oloc(root_grp)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get object location for root group");
    if (NULL == (loc->path = H5G_nameof(root_grp)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get path for root group");

    /* Patch up root group's object location to reflect this file */
    /* (Since the root group info is only stored once for files which
     *  share an underlying low-level file)
     */
    /* (but only for non-mounted files) */
    if (!H5F_is_mount(f)) {
        loc->oloc->file         = f;
        loc->oloc->holding_file = false;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_root_loc() */
