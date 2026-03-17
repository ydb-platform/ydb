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
 * Created:             H5Defc.c
 *
 * Purpose:             External file caching routines - implements a
 *                      cache of external files to minimize the number of
 *                      file opens and closes.
 *
 *-------------------------------------------------------------------------
 */

#include "H5Fmodule.h" /* This source code file is part of the H5F module */

/* Packages needed by this file... */
#include "H5private.h"   /* Generic Functions                    */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5Fpkg.h"      /* File access                          */
#include "H5Iprivate.h"  /* IDs                                  */
#include "H5MMprivate.h" /* Memory management                    */
#include "H5Pprivate.h"  /* Property lists                       */

/* Special values for the "tag" field below */
#define H5F_EFC_TAG_DEFAULT   (-1)
#define H5F_EFC_TAG_LOCK      (-2)
#define H5F_EFC_TAG_CLOSE     (-3)
#define H5F_EFC_TAG_DONTCLOSE (-4)

/* Structure for each entry in a file's external file cache */
typedef struct H5F_efc_ent_t {
    char                 *name;     /* Name of the file */
    H5F_t                *file;     /* File object */
    struct H5F_efc_ent_t *LRU_next; /* Next item in LRU list */
    struct H5F_efc_ent_t *LRU_prev; /* Previous item in LRU list */
    unsigned              nopen;    /* Number of times this file is currently opened by an EFC client */
} H5F_efc_ent_t;

/* Structure for a shared file struct's external file cache */
struct H5F_efc_t {
    H5SL_t        *slist;      /* Skip list of cached external files */
    H5F_efc_ent_t *LRU_head;   /* Head of LRU list.  This is the least recently used file */
    H5F_efc_ent_t *LRU_tail;   /* Tail of LRU list.  This is the most recently used file */
    unsigned       nfiles;     /* Size of the external file cache */
    unsigned       max_nfiles; /* Maximum size of the external file cache */
    unsigned       nrefs;      /* Number of times this file appears in another file's EFC */
    int            tag;        /* Temporary variable used by H5F__efc_try_close() */
    H5F_shared_t  *tmp_next;   /* Next file in temporary list used by H5F__efc_try_close() */
};

/* Private prototypes */
static herr_t H5F__efc_release_real(H5F_efc_t *efc);
static herr_t H5F__efc_remove_ent(H5F_efc_t *efc, H5F_efc_ent_t *ent);
static void   H5F__efc_try_close_tag1(H5F_shared_t *sf, H5F_shared_t **tail);
static void   H5F__efc_try_close_tag2(H5F_shared_t *sf, H5F_shared_t **tail);

/* Free lists */
H5FL_DEFINE_STATIC(H5F_efc_ent_t);
H5FL_DEFINE_STATIC(H5F_efc_t);

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_create
 *
 * Purpose:     Allocate and initialize a new external file cache object,
 *              which can the be used to cache open external files.
 *              the object must be freed with H5F__efc_destroy.
 *
 * Return:      Pointer to new external file cache object on success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5F_efc_t *
H5F__efc_create(unsigned max_nfiles)
{
    H5F_efc_t *efc       = NULL; /* EFC object */
    H5F_efc_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(max_nfiles > 0);

    /* Allocate EFC struct */
    if (NULL == (efc = H5FL_CALLOC(H5F_efc_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Initialize maximum number of files */
    efc->max_nfiles = max_nfiles;

    /* Initialize temporary ref count */
    efc->tag = H5F_EFC_TAG_DEFAULT;

    /* Set the return value */
    ret_value = efc;

done:
    if (ret_value == NULL && efc)
        efc = H5FL_FREE(H5F_efc_t, efc);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__efc_create() */

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_open
 *
 * Purpose:     Opens a file using the external file cache.  The target
 *              file is added to the external file cache of the parent
 *              if it is not already present.  If the target file is in
 *              the parent's EFC, simply returns the target file.  When
 *              the file object is no longer in use, it should be closed
 *              with H5F_efc_close (will not actually close the file
 *              until it is evicted from the EFC).
 *
 * Return:      Pointer to open file on success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5F_t *
H5F__efc_open(H5F_efc_t *efc, const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id)
{
    H5F_efc_ent_t        *ent       = NULL;  /* Entry for target file in efc */
    bool                  open_file = false; /* Whether ent->file needs to be closed in case of error */
    H5P_genplist_t       *plist;             /* Property list pointer for FAPL */
    H5VL_connector_prop_t connector_prop;    /* Property for VOL connector ID & info        */
    H5F_t                *ret_value = NULL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(name);

    /* Get the VOL info from the fapl */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_FILE, H5E_BADTYPE, NULL, "not a file access property list");
    if (H5P_peek(plist, H5F_ACS_VOL_CONN_NAME, &connector_prop) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "can't get VOL connector info");

    /* Stash a copy of the "top-level" connector property, before any pass-through
     *  connectors modify or unwrap it.
     */
    if (H5CX_set_vol_connector_prop(&connector_prop) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, NULL, "can't set VOL connector info in API context");

    /* Check if the EFC exists.  If it does not, just call H5F_open().  We
     * support this so clients do not have to make 2 different calls depending
     * on the state of the efc. */
    if (!efc) {
        if (NULL == (ret_value = H5F_open(name, flags, fcpl_id, fapl_id)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open file");

        /* Make file post open call */
        if (H5F__post_open(ret_value) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't finish opening file");

        /* Increment the number of open objects to prevent the file from being
         * closed out from under us - "simulate" having an open file id.  Note
         * that this behaviour replaces the calls to H5F_incr_nopen_objs() and
         * H5F_decr_nopen_objs() in H5L_extern_traverse(). */
        ret_value->nopen_objs++;

        HGOTO_DONE(ret_value);
    } /* end if */

    /* Search the skip list for name if the skip list exists, create the skip
     * list otherwise */
    if (efc->slist) {
        if (efc->nfiles > 0)
            ent = (H5F_efc_ent_t *)H5SL_search(efc->slist, name);
    } /* end if */
    else {
        assert(efc->nfiles == 0);
        if (NULL == (efc->slist = H5SL_create(H5SL_TYPE_STR, NULL)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTCREATE, NULL, "can't create skip list");
    } /* end else */

    /* If we found the file update the LRU list and return the cached file,
     * otherwise open the file and cache it */
    if (ent) {
        assert(efc->LRU_head);
        assert(efc->LRU_tail);

        /* Move ent to the head of the LRU list, if it is not already there */
        if (ent->LRU_prev) {
            assert(efc->LRU_head != ent);

            /* Remove from current position.  Note that once we touch the LRU
             * list we cannot revert to the previous state.  Make sure there can
             * be no errors between when we first touch the LRU list and when
             * the cache is in a consistent state! */
            if (ent->LRU_next)
                ent->LRU_next->LRU_prev = ent->LRU_prev;
            else {
                assert(efc->LRU_tail == ent);
                efc->LRU_tail = ent->LRU_prev;
            } /* end else */
            ent->LRU_prev->LRU_next = ent->LRU_next;

            /* Add to head of LRU list */
            ent->LRU_next           = efc->LRU_head;
            ent->LRU_next->LRU_prev = ent;
            ent->LRU_prev           = NULL;
            efc->LRU_head           = ent;
        } /* end if */

        /* Mark the file as open */
        ent->nopen++;
    } /* end if */
    else {
        /* Check if we need to evict something */
        if (efc->nfiles == efc->max_nfiles) {
            /* Search for an unopened file from the tail */
            for (ent = efc->LRU_tail; ent && ent->nopen; ent = ent->LRU_prev)
                ;

            /* Evict the file if found, otherwise just open the target file and
             * do not add it to cache */
            if (ent) {
                if (H5F__efc_remove_ent(efc, ent) < 0)
                    HGOTO_ERROR(H5E_FILE, H5E_CANTREMOVE, NULL,
                                "can't remove entry from external file cache");

                /* Do not free ent, we will recycle it below */
            } /* end if */
            else {
                /* Cannot cache file, just open file and return */
                if (NULL == (ret_value = H5F_open(name, flags, fcpl_id, fapl_id)))
                    HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open file");

                /* Make file post open call */
                if (H5F__post_open(ret_value) < 0)
                    HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't finish opening file");

                /* Increment the number of open objects to prevent the file from
                 * being closed out from under us - "simulate" having an open
                 * file id */
                ret_value->nopen_objs++;

                HGOTO_DONE(ret_value);
            } /* end else */
        }     /* end if */
        else
            /* Allocate new entry */
            if (NULL == (ent = H5FL_MALLOC(H5F_efc_ent_t)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

        /* Build new entry */
        if (NULL == (ent->name = H5MM_strdup(name)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

        /* Open the file */
        if (NULL == (ent->file = H5F_open(name, flags, fcpl_id, fapl_id)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "can't open file");
        open_file = true;

        /* Make file post open call */
        if (H5F__post_open(ent->file) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, NULL, "can't finish opening file");

        /* Increment the number of open objects to prevent the file from being
         * closed out from under us - "simulate" having an open file id */
        ent->file->nopen_objs++;

        /* Add the file to the cache */
        /* Skip list */
        if (H5SL_insert(efc->slist, ent, ent->name) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTINSERT, NULL, "can't insert entry into skip list");

        /* Add to head of LRU list and update tail if necessary */
        ent->LRU_next = efc->LRU_head;
        if (ent->LRU_next)
            ent->LRU_next->LRU_prev = ent;
        ent->LRU_prev = NULL;
        efc->LRU_head = ent;
        if (!efc->LRU_tail) {
            assert(!ent->LRU_next);
            efc->LRU_tail = ent;
        } /* end if */

        /* Mark the file as open */
        ent->nopen = 1;

        /* Update nfiles and nrefs */
        efc->nfiles++;
        if (ent->file->shared->efc)
            ent->file->shared->efc->nrefs++;
    } /* end else */

    assert(ent);
    assert(ent->file);
    assert(ent->name);
    assert(ent->nopen);

    /* Set the return value */
    ret_value = ent->file;

done:
    if (!ret_value)
        if (ent) {
            if (open_file) {
                ent->file->nopen_objs--;
                if (H5F_try_close(ent->file, NULL) < 0)
                    HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "can't close external file");
            } /* end if */
            ent->name = (char *)H5MM_xfree(ent->name);
            ent       = H5FL_FREE(H5F_efc_ent_t, ent);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__efc_open() */

/*-------------------------------------------------------------------------
 * Function:    H5F_efc_close
 *
 * Purpose:     Closes (unlocks) a file opened using the external file
 *              cache.  The target file is not immediately closed unless
 *              there is no external file cache for the parent file.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_efc_close(H5F_t *parent, H5F_t *file)
{
    H5F_efc_t     *efc       = NULL;    /* External file cache for parent file */
    H5F_efc_ent_t *ent       = NULL;    /* Entry for target file in efc */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity checks */
    assert(parent);
    assert(parent->shared);
    assert(file);
    assert(file->shared);

    /* Get external file cache */
    efc = parent->shared->efc;

    /* Check if the EFC exists.  If it does not, just call H5F_try_close().  We
     * support this so clients do not have to make 2 different calls depending
     * on the state of the efc. */
    if (!efc) {
        file->nopen_objs--;
        if (H5F_try_close(file, NULL) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close external file");

        HGOTO_DONE(SUCCEED);
    } /* end if */

    /* Scan the parent's LRU list from the head to file file.  We do this
     * instead of a skip list lookup because the file will almost always be at
     * the head.  In the unlikely case that the file is not found, just call
     * H5F_try_close().  This could happen if the EFC was full of open files
     * when the file was opened. */
    for (ent = efc->LRU_head; ent && ent->file != file; ent = ent->LRU_next)
        ;
    if (!ent) {
        file->nopen_objs--;
        if (H5F_try_close(file, NULL) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close external file");
    } /* end if */
    else
        /* Reduce the open count on this entry */
        ent->nopen--;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_efc_close() */

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_max_nfiles
 *
 * Purpose:     Returns the maximum number of files in the provided
 *              external file cache.
 *
 * Return:      Maximum number of files (never fails)
 *
 *-------------------------------------------------------------------------
 */
unsigned
H5F__efc_max_nfiles(H5F_efc_t *efc)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(efc);
    assert(efc->max_nfiles > 0);

    FUNC_LEAVE_NOAPI(efc->max_nfiles)
} /* end H5F__efc_max_nfiles */

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_release_real
 *
 * Purpose:     Releases the external file cache, potentially closing any
 *              cached files unless they are held open from somewhere
 *              else (or are currently opened by a client).
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__efc_release_real(H5F_efc_t *efc)
{
    H5F_efc_ent_t *ent       = NULL;    /* EFC entry */
    H5F_efc_ent_t *prev_ent  = NULL;    /* Previous EFC entry */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(efc);

    /* Lock the EFC to prevent manipulation of the EFC while we are releasing it.
     * The EFC should never be locked when we enter this function because that
     * would require a cycle, a cycle would necessarily invoke
     * H5F__efc_try_close(), and that function checks the status of the lock
     * before calling this one. */
    assert((efc->tag == H5F_EFC_TAG_DEFAULT) || (efc->tag == H5F_EFC_TAG_CLOSE));
    efc->tag = H5F_EFC_TAG_LOCK;

    /* Walk down the LRU list, releasing any files that are not opened by an EFC
     * client */
    ent = efc->LRU_head;
    while (ent)
        if (!ent->nopen) {
            if (H5F__efc_remove_ent(efc, ent) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTREMOVE, FAIL, "can't remove entry from external file cache");

            /* Free the entry and move to next entry in LRU list */
            prev_ent = ent;
            ent      = ent->LRU_next;
            prev_ent = H5FL_FREE(H5F_efc_ent_t, prev_ent);
        } /* end if */
        else
            /* Can't release file because it's open; just advance the pointer */
            ent = ent->LRU_next;

    /* Reset tag.  No need to reset to CLOSE if that was the original tag, as in
     * that case the file must be getting closed anyways. */
    efc->tag = H5F_EFC_TAG_DEFAULT;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__efc_release_real() */

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_release
 *
 * Purpose:     Releases the external file cache, potentially closing any
 *              cached files unless they are held open from somewhere
 *              else (or are currently opened by a client).
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__efc_release(H5F_efc_t *efc)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(efc);

    /* Call 'real' routine */
    if (H5F__efc_release_real(efc) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTRELEASE, FAIL, "can't remove entry from external file cache");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_efc_release() */

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_destroy
 *
 * Purpose:     Frees an external file cache object, releasing it first
 *              if necessary.  If it cannot be fully released, for example
 *              if there are open files, returns an error.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__efc_destroy(H5F_efc_t *efc)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(efc);

    if (efc->nfiles > 0) {
        /* Release (clear) the efc */
        if (H5F__efc_release_real(efc) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTRELEASE, FAIL, "can't release external file cache");

        /* If there are still cached files, return an error */
        if (efc->nfiles > 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "can't destroy EFC after incomplete release");
    } /* end if */

    assert(efc->nfiles == 0);
    assert(efc->LRU_head == NULL);
    assert(efc->LRU_tail == NULL);

    /* Close skip list */
    if (efc->slist)
        if (H5SL_close(efc->slist) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "can't close skip list");

    /* Free EFC object */
    (void)H5FL_FREE(H5F_efc_t, efc);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__efc_destroy() */

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_remove_ent
 *
 * Purpose:     Removes the specified entry from the specified EFC,
 *              closing the file if requested.  Does not free the entry.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__efc_remove_ent(H5F_efc_t *efc, H5F_efc_ent_t *ent)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(efc);
    assert(efc->slist);
    assert(ent);

    /* Remove from skip list */
    if (ent != H5SL_remove(efc->slist, ent->name))
        HGOTO_ERROR(H5E_FILE, H5E_CANTDELETE, FAIL, "can't delete entry from skip list");

    /* Remove from LRU list */
    if (ent->LRU_next)
        ent->LRU_next->LRU_prev = ent->LRU_prev;
    else {
        assert(efc->LRU_tail == ent);
        efc->LRU_tail = ent->LRU_prev;
    } /* end else */
    if (ent->LRU_prev)
        ent->LRU_prev->LRU_next = ent->LRU_next;
    else {
        assert(efc->LRU_head == ent);
        efc->LRU_head = ent->LRU_next;
    } /* end else */

    /* Update nfiles and nrefs */
    efc->nfiles--;
    if (ent->file->shared->efc)
        ent->file->shared->efc->nrefs--;

    /* Free the name */
    ent->name = (char *)H5MM_xfree(ent->name);

    /* Close the file.  Note that since H5F_t structs returned from H5F_open()
     * are *always* unique, there is no need to reference count this struct.
     * However we must still manipulate the nopen_objs field to prevent the file
     * from being closed out from under us. */
    ent->file->nopen_objs--;
    if (H5F_try_close(ent->file, NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close external file");
    ent->file = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__efc_remove_ent() */

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_try_close_tag1
 *
 * Purpose:     Recursively traverse the EFC tree, keeping a temporary
 *              reference count on each file that assumes all reachable
 *              files will eventually be closed.
 *
 * Return:      void (never fails)
 *
 *-------------------------------------------------------------------------
 */
static void
H5F__efc_try_close_tag1(H5F_shared_t *sf, H5F_shared_t **tail)
{
    H5F_efc_ent_t *ent = NULL; /* EFC entry */
    H5F_shared_t  *esf;        /* Convenience pointer to ent->file->shared */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(sf);
    assert(sf->efc);
    assert((sf->efc->tag > 0) || (sf->nrefs == sf->efc->nrefs));
    assert(sf->efc->tag != H5F_EFC_TAG_LOCK);
    assert(tail);
    assert(*tail);

    /* Recurse into this file's cached files */
    for (ent = sf->efc->LRU_head; ent; ent = ent->LRU_next) {
        esf = ent->file->shared;

        if (esf->efc) {
            /* If tag were 0, that would mean there are more actual references
             * than are counted by nrefs */
            assert(esf->efc->tag != 0);

            /* If tag has been set, we have already visited this file so just
             * decrement tag and continue */
            if (esf->efc->tag > 0)
                esf->efc->tag--;
            /* If there are references that are not from an EFC, it will never
             * be possible to close the file.  Just continue.  Also continue if
             * the EFC is locked or the file is open (through the EFC).  Note
             * that the reference counts will never match for the root file, but
             * that's ok because the root file will always have a tag and enter
             * the branch above. */
            else if ((esf->nrefs == esf->efc->nrefs) && (esf->efc->tag != H5F_EFC_TAG_LOCK) &&
                     !(ent->nopen)) {
                /* If we get here, this file's "tmp_next" pointer must be NULL
                 */
                assert(esf->efc->tmp_next == NULL);

                /* If nrefs > 1, Add this file to the list of files with nrefs >
                 * 1 and initialize tag to the number of references (except this
                 * one) */
                if (esf->nrefs > 1) {
                    (*tail)->efc->tmp_next = esf;
                    *tail                  = esf;
                    esf->efc->tag          = (int)esf->nrefs - 1;
                } /* end if */

                /* Recurse into the entry */
                H5F__efc_try_close_tag1(ent->file->shared, tail);
            } /* end if */
        }     /* end if */
    }         /* end for */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5F__efc_try_close_tag1() */

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_try_close_tag2
 *
 * Purpose:     Recuresively mark all files reachable through this one as
 *              uncloseable, and add newly uncloseable files to the tail
 *              of the provided linked list.
 *
 * Return:      void (never fails)
 *
 *-------------------------------------------------------------------------
 */
static void
H5F__efc_try_close_tag2(H5F_shared_t *sf, H5F_shared_t **tail)
{
    H5F_efc_ent_t *ent = NULL; /* EFC entry */
    H5F_shared_t  *esf;        /* Convenience pointer to ent->file->shared */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(sf);
    assert(sf->efc);

    /* Recurse into this file's cached files */
    for (ent = sf->efc->LRU_head; ent; ent = ent->LRU_next) {
        esf = ent->file->shared;

        /* Only recurse if the file is tagged CLOSE or DEFAULT.  If it is tagged
         * DONTCLOSE, we have already visited this file *or* it will be the
         * start point of another iteration.  No files should be tagged with a
         * nonegative value at this point.  If it is tagged as DEFAULT, we must
         * apply the same conditions as in cb1 above for recursion in order to
         * make sure  we do not go off into somewhere cb1 didn't touch.  The
         * root file should never be tagged DEFAULT here, so the reference check
         * is still appropriate. */
        if ((esf->efc) &&
            ((esf->efc->tag == H5F_EFC_TAG_CLOSE) ||
             ((esf->efc->tag == H5F_EFC_TAG_DEFAULT) && (esf->nrefs == esf->efc->nrefs) && !(ent->nopen)))) {
            /* tag should always be CLOSE is nrefs > 1 or DEFAULT if nrefs == 1
             * here */
            assert(((esf->nrefs > 1) && ((esf->efc->tag == H5F_EFC_TAG_CLOSE))) ||
                   ((esf->nrefs == 1) && (esf->efc->tag == H5F_EFC_TAG_DEFAULT)));

            /* If tag is set to DONTCLOSE, we have already visited this file
             * *or* it will be the start point of another iteration so just
             * continue */
            if (esf->efc->tag != H5F_EFC_TAG_DONTCLOSE) {
                /* If tag is CLOSE, set to DONTCLOSE and add to the list of
                 * uncloseable files. */
                if (esf->efc->tag == H5F_EFC_TAG_CLOSE) {
                    esf->efc->tag          = H5F_EFC_TAG_DONTCLOSE;
                    esf->efc->tmp_next     = NULL;
                    (*tail)->efc->tmp_next = esf;
                    *tail                  = esf;
                } /* end if */

                /* Recurse into the entry */
                H5F__efc_try_close_tag2(esf, tail);
            } /* end if */
        }     /* end if */
    }         /* end for */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5F__efc_try_close_tag2() */

/*-------------------------------------------------------------------------
 * Function:    H5F__efc_try_close
 *
 * Purpose:     Attempts to close the provided (shared) file by checking
 *              to see if the releasing the EFC would cause its reference
 *              count to drop to 0.  Necessary to handle the case where
 *              chained EFCs form a cycle.  Note that this function does
 *              not actually close the file (though it closes all children
 *              as appropriate), as that is left up to the calling
 *              function H5F_try_close().
 *
 *              Because H5F_try_close() has no way of telling if it is
 *              called recursively from within this function, this
 *              function serves as both the root of iteration and the
 *              "callback" for the final pass (the one where the files are
 *              actually closed).  The code for the callback case is at
 *              the top of this function; luckily it only consists of a
 *              (possible) call to H5F__efc_release_real().
 *
 *              The algorithm basically consists of 3 passes over the EFC
 *              tree.  The first pass assumes that every reachable file is
 *              closed, and keeps track of what the final reference count
 *              would be for every reachable file.  The files are then
 *              tagged as either closeable or uncloseable based on whether
 *              this reference count drops to 0.
 *
 *              The second pass initiates a traversal from each file
 *              marked as uncloseable in the first pass, and marks every
 *              file reachable from the initial uncloseable file as
 *              uncloseable.  This eliminates files that were marked as
 *              closeable only because the first pass assumed that an
 *              uncloseable file would be closed.
 *
 *              The final pass exploits the H5F__efc_release_real()->
 *              H5F__efc_remove_ent()->H5F_try_close()->H5F__efc_try_close()
 *              calling chain to recursively close the tree, but only the
 *              files that are still marked as closeable.  All files
 *              marked as closeable have their EFCs released, and will
 *              eventually be closed when their last parent EFC is
 *              released (the last part is guaranteed to be true by the
 *              first 2 passes).
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__efc_try_close(H5F_t *f)
{
    H5F_shared_t *tail; /* Tail of linked list of found files.  Head will be f->shared. */
    H5F_shared_t *uncloseable_head =
        NULL; /* Head of linked list of files found to be uncloseable by the first pass */
    H5F_shared_t *uncloseable_tail =
        NULL;           /* Tail of linked list of files found to be uncloseable by the first pass */
    H5F_shared_t *sf;   /* Temporary file pointer */
    H5F_shared_t *next; /* Temporary file pointer */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->efc);
    assert(f->shared->nrefs > f->shared->efc->nrefs);
    assert(f->shared->nrefs > 1);
    assert(f->shared->efc->tag < 0);

    if (f->shared->efc->tag == H5F_EFC_TAG_CLOSE) {
        /* We must have reentered this function, and we should close this file.
         * In actuality, we just release the EFC, the recursion should
         * eventually reduce this file's reference count to 1 (though possibly
         * not from this call to H5F__efc_release_real()). */
        if (H5F__efc_release_real(f->shared->efc) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTRELEASE, FAIL, "can't release external file cache");

        /* If we marked the file as closeable, there must be no open files in
         * its EFC.  This is because, in order to close an open child file, the
         * client must keep a copy of the parent file open.  The algorithm
         * detect that the parent file is open (directly or through an EFC) and
         * refuse to close it.  Verify that all files were released from this
         * EFC (i.e. none were open). */
        assert(f->shared->efc->nfiles == 0);

        HGOTO_DONE(SUCCEED);
    } /* end if */

    /* Conditions where we should not do anything and just return immediately */
    /* If there are references that are not from an EFC or f, it will never
     * be possible to close the file.  Just return.  Note that this holds true
     * for the case that this file is being closed through H5F__efc_release_real()
     * because that function (through H5F__efc_remove_ent()) decrements the EFC
     * reference count before it calls H5F_try_close(). This may occur if this
     * function is reentered. */
    /* If the tag is H5F_EFC_TAG_DONTCLOSE, then we have definitely reentered
     * this function, and this file has been marked as uncloseable, so we should
     * not close/release it */
    /* If nfiles is 0, then there is nothing to do.  Just return.  This may also
     * occur on reentry (for example if this file was previously released). */
    if ((f->shared->nrefs != f->shared->efc->nrefs + 1) || (f->shared->efc->tag == H5F_EFC_TAG_DONTCLOSE) ||
        (f->shared->efc->nfiles == 0))
        /* We must have reentered this function, and we should not close this
         * file.  Just return. */
        HGOTO_DONE(SUCCEED);

    /* If the file EFC were locked, that should always mean that there exists
     * a reference to this file that is not in an EFC (it may have just been
     * removed from an EFC), and should have been caught by the above check */
    /* If we get here then we must be beginning a new run.  Make sure that the
     * temporary variables in f->shared->efc are at the default value */
    assert(f->shared->efc->tag == H5F_EFC_TAG_DEFAULT);
    assert(f->shared->efc->tmp_next == NULL);

    /* Set up linked list for traversal into EFC tree.  f->shared is guaranteed
     * to always be at the head. */
    tail = f->shared;

    /* Set up temporary reference count on root file */
    f->shared->efc->tag = (int)f->shared->efc->nrefs;

    /* First Pass: simulate closing all files reachable from this one, use "tag"
     * field to keep track of final reference count for each file (including
     * this one).  Keep list of files with starting reference count > 1 (head is
     * f->shared). */
    H5F__efc_try_close_tag1(f->shared, &tail);

    /* Check if f->shared->efc->tag dropped to 0.  If it did not,
     * we cannot close anything.  Just reset temporary values and return. */
    if (f->shared->efc->tag > 0) {
        sf = f->shared;
        while (sf) {
            next              = sf->efc->tmp_next;
            sf->efc->tag      = H5F_EFC_TAG_DEFAULT;
            sf->efc->tmp_next = NULL;
            sf                = next;
        } /* end while */
        HGOTO_DONE(SUCCEED);
    } /* end if */

    /* Run through the linked list , separating into two lists, one with tag ==
     * 0 and one with tag > 0.  Mark them as either H5F_EFC_TAG_CLOSE or
     * H5F_EFC_TAG_DONTCLOSE as appropriate. */
    sf   = f->shared;
    tail = NULL;
    while (sf) {
        assert(sf->efc->tag >= 0);
        next = sf->efc->tmp_next;
        if (sf->efc->tag > 0) {
            /* Remove from main list */
            assert(tail);
            tail->efc->tmp_next = sf->efc->tmp_next;
            sf->efc->tmp_next   = NULL;

            /* Add to uncloseable list */
            if (!uncloseable_head)
                uncloseable_head = sf;
            else
                uncloseable_tail->efc->tmp_next = sf;
            uncloseable_tail = sf;

            /* Mark as uncloseable */
            sf->efc->tag = H5F_EFC_TAG_DONTCLOSE;
        } /* end if */
        else {
            sf->efc->tag = H5F_EFC_TAG_CLOSE;
            tail         = sf;
        } /* end else */
        sf = next;
    } /* end while */

    /* Second pass: Determine which of the reachable files found in pass 1
     * cannot be closed by releasing the root file's EFC.  Run through the
     * uncloseable list, for each item traverse the files reachable through the
     * EFC, mark the file as uncloseable, and add it to the list of uncloseable
     * files (for cleanup).  Use "tail" to store the original uncloseable tail
     * so we know when to stop.  We do not need to keep track of the closeable
     * list any more. */
    sf = uncloseable_head;
    if (sf) {
        tail = uncloseable_tail;
        assert(tail);
        while (sf != tail->efc->tmp_next) {
            H5F__efc_try_close_tag2(sf, &uncloseable_tail);
            sf = sf->efc->tmp_next;
        } /* end while */
    }     /* end if */

    /* If the root file's tag is still H5F_EFC_TAG_CLOSE, release its EFC.  This
     * should start the recursive release that should close all closeable files.
     * Also, see the top of this function. */
    if (f->shared->efc->tag == H5F_EFC_TAG_CLOSE) {
        if (H5F__efc_release_real(f->shared->efc) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTRELEASE, FAIL, "can't release external file cache");

        /* Make sure the file's reference count is now 1 and will be closed by
         * H5F_dest(). */
        assert(f->shared->nrefs == 1);
    } /* end if */

    /* Clean up uncloseable files (reset tag and tmp_next).  All closeable files
     * should have been closed, and therefore do not need to be cleaned up. */
    if (uncloseable_head) {
        sf = uncloseable_head;
        while (sf) {
            next = sf->efc->tmp_next;
            assert(sf->efc->tag == H5F_EFC_TAG_DONTCLOSE);
            sf->efc->tag      = H5F_EFC_TAG_DEFAULT;
            sf->efc->tmp_next = NULL;
            sf                = next;
        } /* end while */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__efc_try_close() */
