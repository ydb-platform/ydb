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

#include "H5Fmodule.h" /* This source code file is part of the H5F module */

/* Packages needed by this file... */
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fpkg.h"      /* File access                              */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Pprivate.h"  /* Property lists                           */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

/* PRIVATE PROTOTYPES */
static void H5F__mount_count_ids_recurse(H5F_t *f, unsigned *nopen_files, unsigned *nopen_objs);

/*-------------------------------------------------------------------------
 * Function:	H5F__close_mounts
 *
 * Purpose:	Close all mounts for a given file
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__close_mounts(H5F_t *f)
{
    unsigned u;                   /* Local index */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);

    /* Unmount all child files.  Loop backwards to avoid having to adjust u when
     * a file is unmounted.  Note that we rely on unsigned u "wrapping around"
     * to terminate the loop.
     */
    for (u = f->shared->mtab.nmounts - 1; u < f->shared->mtab.nmounts; u--) {
        /* Only unmount children mounted to this top level file structure */
        if (f->shared->mtab.child[u].file->parent == f) {
            /* Detach the child file from the parent file */
            f->shared->mtab.child[u].file->parent = NULL;

            /* Close the internal group maintaining the mount point */
            if (H5G_close(f->shared->mtab.child[u].group) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEOBJ, FAIL, "can't close child group");

            /* Close the child file */
            if (H5F_try_close(f->shared->mtab.child[u].file, NULL) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close child file");

            /* Eliminate the mount point from the table */
            memmove(f->shared->mtab.child + u, f->shared->mtab.child + u + 1,
                    (f->shared->mtab.nmounts - u - 1) * sizeof(f->shared->mtab.child[0]));
            f->shared->mtab.nmounts--;
            f->nmounts--;
        }
    }

    assert(f->nmounts == 0);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__close_mounts() */

/*-------------------------------------------------------------------------
 * Function:	H5F_mount
 *
 * Purpose:	Mount file CHILD onto the group specified by LOC and NAME,
 *		using mount properties in PLIST.  CHILD must not already be
 *		mouted and must not be a mount ancestor of the mount-point.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_mount(const H5G_loc_t *loc, const char *name, H5F_t *child, hid_t H5_ATTR_UNUSED plist_id)
{
    H5G_t     *mount_point = NULL;  /*mount point group		*/
    H5F_t     *ancestor    = NULL;  /*ancestor files		*/
    H5F_t     *parent      = NULL;  /*file containing mount point	*/
    unsigned   lt, rt, md;          /*binary search indices		*/
    int        cmp;                 /*binary search comparison value*/
    H5G_loc_t  mp_loc;              /* entry of mount point to be opened */
    H5G_name_t mp_path;             /* Mount point group hier. path */
    H5O_loc_t  mp_oloc;             /* Mount point object location */
    H5G_loc_t  root_loc;            /* Group location of root of file to mount */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(loc);
    assert(name && *name);
    assert(child);
    assert(true == H5P_isa_class(plist_id, H5P_FILE_MOUNT));

    /* Set up group location to fill in */
    mp_loc.oloc = &mp_oloc;
    mp_loc.path = &mp_path;
    H5G_loc_reset(&mp_loc);

    /*
     * Check that the child isn't mounted, that the mount point exists, that
     * the mount point wasn't reached via external link, that
     * the parent & child files have the same file close degree, and
     * that the mount wouldn't introduce a cycle in the mount tree.
     */
    if (child->parent)
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "file is already mounted");
    if (H5G_loc_find(loc, name, &mp_loc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "group not found");
    /* If the mount location is holding its file open, that file will close
     * and remove the mount as soon as we exit this function.  Prevent the
     * user from doing this.
     */
    if (mp_loc.oloc->holding_file != false)
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "mount path cannot contain links to external files");

    /* Open the mount point group */
    if (NULL == (mount_point = H5G_open(&mp_loc)))
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "mount point not found");

    /* Check if the proposed mount point group is already a mount point */
    if (H5G_MOUNTED(mount_point))
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "mount point is already in use");

    /* Retrieve information from the mount point group */
    /* (Some of which we had before but was reset in mp_loc when the group
     *  "took over" the group location - QAK)
     */
    parent = H5G_fileof(mount_point);
    assert(parent);
    mp_loc.oloc = H5G_oloc(mount_point);
    assert(mp_loc.oloc);
    mp_loc.path = H5G_nameof(mount_point);
    assert(mp_loc.path);
    for (ancestor = parent; ancestor; ancestor = ancestor->parent)
        if (ancestor->shared == child->shared)
            HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "mount would introduce a cycle");

    /* Make certain that the parent & child files have the same "file close degree" */
    if (parent->shared->fc_degree != child->shared->fc_degree)
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "mounted file has different file close degree than parent");

    /*
     * Use a binary search to locate the position that the child should be
     * inserted into the parent mount table.  At the end of this paragraph
     * `md' will be the index where the child should be inserted.
     */
    lt = md = 0;
    rt      = parent->shared->mtab.nmounts;
    cmp     = -1;
    while (lt < rt && cmp) {
        H5O_loc_t *oloc; /*temporary symbol table entry	*/

        md   = (lt + rt) / 2;
        oloc = H5G_oloc(parent->shared->mtab.child[md].group);
        cmp  = H5_addr_cmp(mp_loc.oloc->addr, oloc->addr);
        if (cmp < 0)
            rt = md;
        else if (cmp > 0)
            lt = md + 1;
    }
    if (cmp > 0)
        md++;
    if (!cmp)
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "mount point is already in use");

    /* Make room in the table */
    if (parent->shared->mtab.nmounts >= parent->shared->mtab.nalloc) {
        unsigned     n = MAX(16, 2 * parent->shared->mtab.nalloc);
        H5F_mount_t *x = (H5F_mount_t *)H5MM_realloc(parent->shared->mtab.child,
                                                     n * sizeof(parent->shared->mtab.child[0]));

        if (!x)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for mount table");
        parent->shared->mtab.child  = x;
        parent->shared->mtab.nalloc = n;
    }

    /* Insert into table */
    memmove(parent->shared->mtab.child + md + 1, parent->shared->mtab.child + md,
            (parent->shared->mtab.nmounts - md) * sizeof(parent->shared->mtab.child[0]));
    parent->shared->mtab.nmounts++;
    parent->nmounts++;
    parent->shared->mtab.child[md].group = mount_point;
    parent->shared->mtab.child[md].file  = child;
    child->parent                        = parent;

    /* Set the group's mountpoint flag */
    if (H5G_mount(parent->shared->mtab.child[md].group) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEOBJ, FAIL, "unable to set group mounted flag");

    /* Get the group location for the root group in the file to unmount */
    if (NULL == (root_loc.oloc = H5G_oloc(child->shared->root_grp)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get object location for root group");
    if (NULL == (root_loc.path = H5G_nameof(child->shared->root_grp)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get path for root group");

    /* Search the open IDs and replace names for mount operation */
    /* We pass H5G_UNKNOWN as object type; search all IDs */
    if (H5G_name_replace(NULL, H5G_NAME_MOUNT, mp_loc.oloc->file, mp_loc.path->full_path_r,
                         root_loc.oloc->file, root_loc.path->full_path_r) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "unable to replace name");

done:
    if (ret_value < 0) {
        if (mount_point) {
            if (H5G_close(mount_point) < 0)
                HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEOBJ, FAIL, "unable to close mounted group");
        }
        else {
            if (H5G_loc_free(&mp_loc) < 0)
                HDONE_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "unable to free mount location");
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_mount() */

/*-------------------------------------------------------------------------
 * Function:	H5F_unmount
 *
 * Purpose:	Unmount the child which is mounted at the group specified by
 *		LOC and NAME or fail if nothing is mounted there.  Neither
 *		file is closed.
 *
 *		Because the mount point is specified by name and opened as a
 *		group, the H5G_namei() will resolve it to the root of the
 *		mounted file, not the group where the file is mounted.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_unmount(const H5G_loc_t *loc, const char *name)
{
    H5G_t     *child_group = NULL;   /* Child's group in parent mtab	*/
    H5F_t     *child       = NULL;   /*mounted file			*/
    H5F_t     *parent      = NULL;   /*file where mounted		*/
    H5O_loc_t *mnt_oloc;             /* symbol table entry for root of mounted file */
    H5G_name_t mp_path;              /* Mount point group hier. path */
    H5O_loc_t  mp_oloc;              /* Mount point object location  */
    H5G_loc_t  mp_loc;               /* entry used to open mount point*/
    bool       mp_loc_setup = false; /* Whether mount point location is set up */
    H5G_loc_t  root_loc;             /* Group location of root of file to unmount */
    int        child_idx;            /* Index of child in parent's mtab */
    herr_t     ret_value = SUCCEED;  /*return value			*/

    FUNC_ENTER_NOAPI(FAIL)

    assert(loc);
    assert(name && *name);

    /* Set up mount point location to fill in */
    mp_loc.oloc = &mp_oloc;
    mp_loc.path = &mp_path;
    H5G_loc_reset(&mp_loc);

    /*
     * Get the mount point, or more precisely the root of the mounted file.
     * If we get the root group and the file has a parent in the mount tree,
     * then we must have found the mount point.
     */
    if (H5G_loc_find(loc, name, &mp_loc /*out*/) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_NOTFOUND, FAIL, "group not found");
    mp_loc_setup = true;
    child        = mp_loc.oloc->file;
    mnt_oloc     = H5G_oloc(child->shared->root_grp);
    child_idx    = -1;

    if (child->parent && H5_addr_eq(mp_oloc.addr, mnt_oloc->addr)) {
        unsigned u; /*counters			*/

        /*
         * We've been given the root group of the child.  We do a reverse
         * lookup in the parent's mount table to find the correct entry.
         */
        parent = child->parent;
        for (u = 0; u < parent->shared->mtab.nmounts; u++) {
            if (parent->shared->mtab.child[u].file->shared == child->shared) {
                /* Found the correct index */
                child_idx = (int)u;
                break;
            }
        }
    }
    else {
        unsigned lt, rt, md = 0; /*binary search indices		*/
        int      cmp;            /*binary search comparison value*/

        /* We've been given the mount point in the parent.  We use a binary
         * search in the parent to locate the mounted file, if any.
         */
        parent = child; /*we guessed wrong*/
        lt     = 0;
        rt     = parent->shared->mtab.nmounts;
        cmp    = -1;

        while (lt < rt && cmp) {
            md       = (lt + rt) / 2;
            mnt_oloc = H5G_oloc(parent->shared->mtab.child[md].group);
            cmp      = H5_addr_cmp(mp_oloc.addr, mnt_oloc->addr);
            if (cmp < 0)
                rt = md;
            else
                lt = md + 1;
        }

        if (cmp)
            HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "not a mount point");

        /* Found the correct index, set the info about the child */
        child_idx = (int)md;
        H5G_loc_free(&mp_loc);
        mp_loc_setup = false;
        mp_loc.oloc  = mnt_oloc;
        mp_loc.path  = H5G_nameof(parent->shared->mtab.child[md].group);
        child        = parent->shared->mtab.child[child_idx].file;

        /* Set the parent to be the actual parent of the discovered child.
         * Could be different due to the shared mount table. */
        parent = child->parent;
    } /* end else */
    assert(child_idx >= 0);

    /* Save the information about the child from the mount table */
    child_group = parent->shared->mtab.child[child_idx].group;

    /* Get the group location for the root group in the file to unmount */
    if (NULL == (root_loc.oloc = H5G_oloc(child->shared->root_grp)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get object location for root group");
    if (NULL == (root_loc.path = H5G_nameof(child->shared->root_grp)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get path for root group");

    /* Search the open IDs replace names to reflect unmount operation */
    if (H5G_name_replace(NULL, H5G_NAME_UNMOUNT, mp_loc.oloc->file, mp_loc.path->full_path_r,
                         root_loc.oloc->file, root_loc.path->full_path_r) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to replace name");

    /* Eliminate the mount point from the table */
    memmove(parent->shared->mtab.child + (unsigned)child_idx,
            (parent->shared->mtab.child + (unsigned)child_idx) + 1,
            ((parent->shared->mtab.nmounts - (unsigned)child_idx) - 1) *
                sizeof(parent->shared->mtab.child[0]));
    parent->shared->mtab.nmounts -= 1;
    parent->nmounts -= 1;

    /* Unmount the child file from the parent file */
    if (H5G_unmount(child_group) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTRELEASE, FAIL, "unable to reset group mounted flag");
    if (H5G_close(child_group) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEOBJ, FAIL, "unable to close unmounted group");

    /* Detach child file from parent & see if it should close */
    child->parent = NULL;
    if (H5F_try_close(child, NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "unable to close unmounted file");

done:
    /* Free the mount point location's information, if it's been set up */
    if (mp_loc_setup)
        H5G_loc_free(&mp_loc);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_unmount() */

/*-------------------------------------------------------------------------
 * Function:	H5F_is_mount
 *
 * Purpose:	Check if a file is mounted within another file.
 *
 * Return:	Success:	true/false
 *		Failure:	(can't happen)
 *
 *-------------------------------------------------------------------------
 */
bool
H5F_is_mount(const H5F_t *file)
{
    bool ret_value = false; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(file);

    if (file->parent != NULL)
        ret_value = true;
    else
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_is_mount() */

/*-------------------------------------------------------------------------
 * Function:    H5F__mount_count_ids_recurse
 *
 * Purpose:     Helper routine for counting number of open IDs in mount
 *              hierarchy.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
static void
H5F__mount_count_ids_recurse(H5F_t *f, unsigned *nopen_files, unsigned *nopen_objs)
{
    unsigned u; /* Local index value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(f);
    assert(nopen_files);
    assert(nopen_objs);

    /* If this file is still open, increment number of file IDs open */
    if (H5F_ID_EXISTS(f))
        *nopen_files += 1;

    /* Increment number of open objects in file
     * (Reduced by number of mounted files, we'll add back in the mount point's
     *  groups later, if they are open)
     */
    *nopen_objs += (f->nopen_objs - f->nmounts);

    /* Iterate over files mounted in this file and add in their open ID counts also */
    for (u = 0; u < f->shared->mtab.nmounts; u++) {
        /* Only recurse on children mounted to this top level file structure */
        if (f->shared->mtab.child[u].file->parent == f) {
            /* Increment the open object count if the mount point group has an open ID */
            if (H5G_get_shared_count(f->shared->mtab.child[u].group) > 1)
                *nopen_objs += 1;

            H5F__mount_count_ids_recurse(f->shared->mtab.child[u].file, nopen_files, nopen_objs);
        }
    }

    FUNC_LEAVE_NOAPI_VOID
} /* end H5F__mount_count_ids_recurse() */

/*-------------------------------------------------------------------------
 * Function:    H5F__mount_count_ids
 *
 * Purpose:     Count the number of open file & object IDs in a mount hierarchy
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__mount_count_ids(H5F_t *f, unsigned *nopen_files, unsigned *nopen_objs)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(f);
    assert(nopen_files);
    assert(nopen_objs);

    /* Find the top file in the mounting hierarchy */
    while (f->parent)
        f = f->parent;

    /* Count open IDs in the hierarchy */
    H5F__mount_count_ids_recurse(f, nopen_files, nopen_objs);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5F__mount_count_ids() */

/*-------------------------------------------------------------------------
 * Function:	H5F__flush_mounts_recurse
 *
 * Purpose:	Flush a mount hierarchy, recursively
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__flush_mounts_recurse(H5F_t *f)
{
    unsigned nerrors = 0;         /* Errors from recursive flushes */
    unsigned u;                   /* Index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);

    /* Flush all child files, not stopping for errors */
    for (u = 0; u < f->shared->mtab.nmounts; u++)
        if (H5F__flush_mounts_recurse(f->shared->mtab.child[u].file) < 0)
            nerrors++;

    /* Call the "real" flush routine, for this file */
    if (H5F__flush(f) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "unable to flush file's cached information");

    /* Check flush errors for children - errors are already on the stack */
    if (nerrors)
        HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "unable to flush file's child mounts");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__flush_mounts_recurse() */

/*-------------------------------------------------------------------------
 * Function:    H5F_flush_mounts
 *
 * Purpose:     Flush a mount hierarchy
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_flush_mounts(H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);

    /* Find the top file in the mount hierarchy */
    while (f->parent)
        f = f->parent;

    /* Flush the mounted file hierarchy */
    if (H5F__flush_mounts_recurse(f) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "unable to flush mounted file hierarchy");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_flush_mounts() */

/*-------------------------------------------------------------------------
 * Function:	H5F_traverse_mount
 *
 * Purpose:	If LNK is a mount point then copy the entry for the root
 *		group of the mounted file into LNK.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_traverse_mount(H5O_loc_t *oloc /*in,out*/)
{
    H5F_t *parent = oloc->file, /* File of object */
        *child    = NULL;       /* Child file */
    unsigned   lt, rt, md = 0;  /* Binary search indices */
    int        cmp;
    H5O_loc_t *mnt_oloc  = NULL;    /* Object location for mount points */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(oloc);

    /*
     * The loop is necessary because we might have file1 mounted at the root
     * of file2, which is mounted somewhere in file3.
     */
    do {
        /*
         * Use a binary search to find the potential mount point in the mount
         * table for the parent
         */
        lt  = 0;
        rt  = parent->shared->mtab.nmounts;
        cmp = -1;
        while (lt < rt && cmp) {
            md       = (lt + rt) / 2;
            mnt_oloc = H5G_oloc(parent->shared->mtab.child[md].group);
            cmp      = H5_addr_cmp(oloc->addr, mnt_oloc->addr);
            if (cmp < 0)
                rt = md;
            else
                lt = md + 1;
        }

        /* Copy root info over to ENT */
        if (0 == cmp) {
            /* Get the child file */
            child = parent->shared->mtab.child[md].file;

            /* Get the location for the root group in the child's file */
            mnt_oloc = H5G_oloc(child->shared->root_grp);

            /* Release the mount point */
            if (H5O_loc_free(oloc) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "unable to free object location");

            /* Copy the entry for the root group */
            if (H5O_loc_copy_deep(oloc, mnt_oloc) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTCOPY, FAIL, "unable to copy object location");

            /* In case the shared root group info points to a different file handle
             * than the child, modify oloc */
            oloc->file = child;

            /* Switch to child's file */
            parent = child;
        } /* end if */
    } while (!cmp);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_traverse_mount() */
