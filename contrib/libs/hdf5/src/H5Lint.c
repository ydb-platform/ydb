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

/****************/
/* Module Setup */
/****************/

#include "H5Lmodule.h" /* This source code file is part of the H5L module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* File access                              */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lpkg.h"      /* Links                                    */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Oprivate.h"  /* File objects                             */
#include "H5Pprivate.h"  /* Property lists                           */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

/****************/
/* Local Macros */
/****************/

#define H5L_MIN_TABLE_SIZE 32 /* Minimum size of the user-defined link type table if it is allocated */

/******************/
/* Local Typedefs */
/******************/

/* User data for path traversal routine for getting link info by name */
typedef struct {
    H5L_info2_t *linfo; /* Buffer to return to user */
} H5L_trav_gi_t;

/* User data for path traversal routine for getting link value by index */
typedef struct {
    /* In */
    H5_index_t      idx_type; /* Index to use */
    H5_iter_order_t order;    /* Order to iterate in index */
    hsize_t         n;        /* Offset of link within index */
    size_t          size;     /* Size of user buffer */

    /* Out */
    void *buf; /* User buffer */
} H5L_trav_gvbi_t;

/* User data for path traversal routine for getting link info by index */
typedef struct {
    /* In */
    H5_index_t      idx_type; /* Index to use */
    H5_iter_order_t order;    /* Order to iterate in index */
    hsize_t         n;        /* Offset of link within index */

    /* Out */
    H5L_info2_t *linfo; /* Buffer to return to user */
} H5L_trav_gibi_t;

/* User data for path traversal routine for removing link by index */
typedef struct {
    /* In */
    H5_index_t      idx_type; /* Index to use */
    H5_iter_order_t order;    /* Order to iterate in index */
    hsize_t         n;        /* Offset of link within index */
} H5L_trav_rmbi_t;

/* User data for path traversal routine for getting name by index */
typedef struct {
    /* In */
    H5_index_t      idx_type; /* Index to use */
    H5_iter_order_t order;    /* Order to iterate in index */
    hsize_t         n;        /* Offset of link within index */
    size_t          size;     /* Size of name buffer */

    /* Out */
    char  *name;     /* Buffer to return name to user */
    size_t name_len; /* Length of full name */
} H5L_trav_gnbi_t;

/* User data for path traversal callback to creating a link */
typedef struct {
    H5F_t            *file;      /* Pointer to the file */
    H5P_genplist_t   *lc_plist;  /* Link creation property list */
    H5G_name_t       *path;      /* Path to object being linked */
    H5O_obj_create_t *ocrt_info; /* Pointer to object creation info */
    H5O_link_t       *lnk;       /* Pointer to link information to insert */
} H5L_trav_cr_t;

/* User data for path traversal routine for moving and renaming a link */
typedef struct {
    const char      *dst_name;         /* Destination name for moving object */
    H5T_cset_t       cset;             /* Char set for new name */
    const H5G_loc_t *dst_loc;          /* Destination location for moving object */
    unsigned         dst_target_flags; /* Target flags for destination object */
    bool             copy;             /* true if this is a copy operation */
    size_t           orig_nlinks; /* The original value for the # of soft / UD links that can be traversed */
} H5L_trav_mv_t;

/* User data for path traversal routine for moving and renaming an object */
typedef struct {
    H5F_t      *file; /* Pointer to the file */
    H5O_link_t *lnk;  /* Pointer to link information to insert */
    bool        copy; /* true if this is a copy operation */
} H5L_trav_mv2_t;

/* User data for path traversal routine for checking if a link exists */
typedef struct {
    /* Down */
    char *sep; /* Pointer to next separator in the string */

    /* Up */
    bool *exists; /* Whether the link exists or not */
} H5L_trav_le_t;

/* User data for path traversal routine for getting link value */
typedef struct {
    size_t size; /* Size of user buffer */
    void  *buf;  /* User buffer */
} H5L_trav_gv_t;

/********************/
/* Local Prototypes */
/********************/

static int    H5L__find_class_idx(H5L_type_t id);
static herr_t H5L__link_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                           H5G_loc_t *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__create_real(const H5G_loc_t *link_loc, const char *link_name, H5G_name_t *obj_path,
                               H5F_t *obj_file, H5O_link_t *lnk, H5O_obj_create_t *ocrt_info, hid_t lcpl_id);
static herr_t H5L__get_val_real(const H5O_link_t *lnk, void *buf, size_t size);
static herr_t H5L__get_val_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                              H5G_loc_t *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__get_val_by_idx_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                                     H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                                     H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__delete_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                             H5G_loc_t *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__delete_by_idx_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                                    H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                                    H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__move_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                           H5G_loc_t *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__move_dest_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                                H5G_loc_t *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__exists_final_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                                   H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                                   H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__exists_inter_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                                   H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                                   H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__get_info_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                               H5G_loc_t *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__get_info_by_idx_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                                      H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                                      H5G_own_loc_t *own_loc /*out*/);
static herr_t H5L__get_name_by_idx_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                                      H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                                      H5G_own_loc_t *own_loc /*out*/);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Information about user-defined links */
static size_t       H5L_table_alloc_g = 0;
static size_t       H5L_table_used_g  = 0;
static H5L_class_t *H5L_table_g       = NULL;

/*-------------------------------------------------------------------------
 * Function:    H5L_init
 *
 * Purpose:     Initialize the interface from some other package.
 *
 * Return:      Success:	non-negative
 *
 *              Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_init(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Initialize user-defined link classes */
    if (H5L_register_external() < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTREGISTERED, FAIL, "unable to register external link class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L_init() */

/*-------------------------------------------------------------------------
 * Function:    H5L_term_package
 *
 * Purpose:     Terminate any resources allocated in H5L_init.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5L_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Free the table of link types */
    if (H5L_table_g) {
        H5L_table_g      = (H5L_class_t *)H5MM_xfree(H5L_table_g);
        H5L_table_used_g = H5L_table_alloc_g = 0;
        n++;
    } /* end if */

    FUNC_LEAVE_NOAPI(n)
} /* H5L_term_package() */

/*-------------------------------------------------------------------------
 * Function:	H5L__find_class_idx
 *
 * Purpose:	Given a link class ID, return the offset in the global array
 *              that holds all the registered link classes.
 *
 * Return:	Success:	Non-negative index of entry in global
 *                              link class table.
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5L__find_class_idx(H5L_type_t id)
{
    size_t i;                /* Local index variable */
    int    ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    for (i = 0; i < H5L_table_used_g; i++)
        if (H5L_table_g[i].id == id)
            HGOTO_DONE((int)i);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__find_class_idx */

/*-------------------------------------------------------------------------
 * Function:	H5L_find_class
 *
 * Purpose:	Given a link class ID return a pointer to a global struct that
 *		defines the link class.
 *
 * Return:	Success:	Ptr to entry in global link class table.
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
const H5L_class_t *
H5L_find_class(H5L_type_t id)
{
    int          idx;              /* Filter index in global table */
    H5L_class_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Get the index in the global table */
    if ((idx = H5L__find_class_idx(id)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTREGISTERED, NULL, "unable to find link class");

    /* Set return value */
    ret_value = H5L_table_g + idx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L_find_class */

/*-------------------------------------------------------------------------
 * Function:	H5L_register
 *
 * Purpose:	Registers a class of user-defined links, or changes the
 *              behavior of an existing class.
 *
 *              See H5Lregister for full documentation.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_register(const H5L_class_t *cls)
{
    size_t i;                   /* Local index variable */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(cls);
    assert(cls->id >= 0 && cls->id <= H5L_TYPE_MAX);

    /* Is the link type already registered? */
    for (i = 0; i < H5L_table_used_g; i++)
        if (H5L_table_g[i].id == cls->id)
            break;

    /* Filter not already registered */
    if (i >= H5L_table_used_g) {
        if (H5L_table_used_g >= H5L_table_alloc_g) {
            size_t       n     = MAX(H5L_MIN_TABLE_SIZE, (2 * H5L_table_alloc_g));
            H5L_class_t *table = (H5L_class_t *)H5MM_realloc(H5L_table_g, (n * sizeof(H5L_class_t)));
            if (!table)
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to extend link type table");
            H5L_table_g       = table;
            H5L_table_alloc_g = n;
        } /* end if */

        /* Initialize */
        i = H5L_table_used_g++;
    } /* end if */

    /* Copy link class info into table */
    H5MM_memcpy(H5L_table_g + i, cls, sizeof(H5L_class_t));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L_register */

/*-------------------------------------------------------------------------
 * Function:	H5L_unregister
 *
 * Purpose:	Unregisters a class of user-defined links.
 *
 *              See H5Lunregister for full documentation.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_unregister(H5L_type_t id)
{
    size_t i;                   /* Local index variable */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(id >= 0 && id <= H5L_TYPE_MAX);

    /* Is the filter already registered? */
    for (i = 0; i < H5L_table_used_g; i++)
        if (H5L_table_g[i].id == id)
            break;

    /* Fail if filter not found */
    if (i >= H5L_table_used_g)
        HGOTO_ERROR(H5E_LINK, H5E_NOTREGISTERED, FAIL, "link class is not registered");

    /* Remove filter from table */
    /* Don't worry about shrinking table size (for now) */
    memmove(&H5L_table_g[i], &H5L_table_g[i + 1], sizeof(H5L_class_t) * ((H5L_table_used_g - 1) - i));
    H5L_table_used_g--;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L_unregister() */

/*-------------------------------------------------------------------------
 * Function:	H5L_is_registered
 *
 * Purpose:     Tests whether a user-defined link class has been registered
 *              or not.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_is_registered(H5L_type_t id, bool *is_registered)
{
    size_t i; /* Local index variable */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(is_registered);

    /* Is the link class already registered? */
    *is_registered = false;
    for (i = 0; i < H5L_table_used_g; i++)
        if (H5L_table_g[i].id == id) {
            *is_registered = true;
            break;
        }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5L_is_registered() */

/*-------------------------------------------------------------------------
 * Function:	H5L_link
 *
 * Purpose:	Creates a link from OBJ_ID to CUR_NAME.  See H5Olink() for
 *		full documentation.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_link(const H5G_loc_t *new_loc, const char *new_name, H5G_loc_t *obj_loc, hid_t lcpl_id)
{
    H5O_link_t lnk;                 /* Link to insert */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(new_loc);
    assert(obj_loc);
    assert(new_name && *new_name);

    /* The link callback will check that the object isn't being hard linked
     * into a different file, so we don't need to do it here (there could be
     * external links along the path).
     */

    /* Construct link information for eventual insertion */
    lnk.type        = H5L_TYPE_HARD;
    lnk.u.hard.addr = obj_loc->oloc->addr;

    /* Create the link */
    if (H5L__create_real(new_loc, new_name, obj_loc->path, obj_loc->oloc->file, &lnk, NULL, lcpl_id) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create new link to object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L_link() */

/*-------------------------------------------------------------------------
 * Function:	H5L_link_object
 *
 * Purpose:	Creates a new object and a link to it.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_link_object(const H5G_loc_t *new_loc, const char *new_name, H5O_obj_create_t *ocrt_info, hid_t lcpl_id)
{
    H5O_link_t lnk;                 /* Link to insert */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(new_loc);
    assert(new_name && *new_name);
    assert(ocrt_info);

    /* The link callback will check that the object isn't being hard linked
     * into a different file, so we don't need to do it here (there could be
     * external links along the path).
     */

    /* Construct link information for eventual insertion */
    lnk.type = H5L_TYPE_HARD;

    /* Create the link */
    if (H5L__create_real(new_loc, new_name, NULL, NULL, &lnk, ocrt_info, lcpl_id) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create new link to object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L_link_object() */

/*-------------------------------------------------------------------------
 * Function:	H5L__link_cb
 *
 * Purpose:	Callback for creating a link to an object.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__link_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t H5_ATTR_UNUSED *lnk,
             H5G_loc_t *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_cr_t *udata  = (H5L_trav_cr_t *)_udata; /* User data passed in */
    H5G_t         *grp    = NULL;   /* H5G_t for this group, opened to pass to user callback */
    hid_t          grp_id = FAIL;   /* Id for this group (passed to user callback */
    H5G_loc_t      temp_loc;        /* For UD callback */
    bool   temp_loc_init = false;   /* Temporary location for UD callback (temp_loc) has been initialized */
    bool   obj_created   = false;   /* Whether an object was created (through a hard link) */
    herr_t ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid location */
    /* (which is not what we want) */
    if (obj_loc != NULL)
        HGOTO_ERROR(H5E_LINK, H5E_EXISTS, FAIL, "name already exists");

    /* Check for crossing file boundaries with a new hard link */
    if (udata->lnk->type == H5L_TYPE_HARD) {
        /* Check for creating an object */
        /* (only for hard links) */
        if (udata->ocrt_info) {
            H5G_loc_t new_loc; /* Group location for new object */

            /* Create new object at this location */
            if (NULL ==
                (udata->ocrt_info->new_obj = H5O_obj_create(grp_loc->oloc->file, udata->ocrt_info->obj_type,
                                                            udata->ocrt_info->crt_info, &new_loc)))
                HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create object");

            /* Set address for hard link */
            udata->lnk->u.hard.addr = new_loc.oloc->addr;

            /* Set object path to use for setting object name (below) */
            udata->path = new_loc.path;

            /* Indicate that an object was created */
            obj_created = true;
        } /* end if */
        else {
            /* Check that both objects are in same file */
            if (!H5F_SAME_SHARED(grp_loc->oloc->file, udata->file))
                HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "interfile hard links are not allowed");
        } /* end else */
    }     /* end if */

    /* Set 'standard' aspects of link */
    udata->lnk->corder =
        0; /* Will be re-written during group insertion, if the group is tracking creation order */
    udata->lnk->corder_valid = false; /* Creation order not valid (yet) */

    /* Check for non-default link creation properties */
    if (udata->lc_plist) {
        /* Get character encoding property */
        if (H5CX_get_encoding(&udata->lnk->cset) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get 'character set' property");
    } /* end if */
    else
        udata->lnk->cset = H5F_DEFAULT_CSET; /* Default character encoding for link */

    /* Set the link's name correctly */
    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    udata->lnk->name = (char *)name;
    H5_GCC_CLANG_DIAG_ON("cast-qual")

    /* Insert link into group */
    if (H5G_obj_insert(grp_loc->oloc, name, udata->lnk, true,
                       udata->ocrt_info ? udata->ocrt_info->obj_type : H5O_TYPE_UNKNOWN,
                       udata->ocrt_info ? udata->ocrt_info->crt_info : NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create new link for object");

    /* Set object's path if it has been passed in and is not set */
    if (udata->path != NULL && udata->path->user_path_r == NULL)
        if (H5G_name_set(grp_loc->path, udata->path, name) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "cannot set name");

    /* If link is a user-defined link, trigger its creation callback if it has one */
    if (udata->lnk->type >= H5L_TYPE_UD_MIN) {
        const H5L_class_t *link_class; /* User-defined link class */

        /* Get the link class for this type of link. */
        if (NULL == (link_class = H5L_find_class(udata->lnk->type)))
            HGOTO_ERROR(H5E_LINK, H5E_NOTREGISTERED, FAIL, "unable to get class of UD link");

        if (link_class->create_func != NULL) {
            H5O_loc_t  temp_oloc;
            H5G_name_t temp_path;

            /* Create a temporary location (or else H5G_open will do a shallow
             * copy and wipe out grp_loc)
             */
            H5G_name_reset(&temp_path);
            if (H5O_loc_copy_deep(&temp_oloc, grp_loc->oloc) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to copy object location");

            temp_loc.oloc = &temp_oloc;
            temp_loc.path = &temp_path;
            temp_loc_init = true;

            /* Set up location for user-defined callback */
            if (NULL == (grp = H5G_open(&temp_loc)))
                HGOTO_ERROR(H5E_LINK, H5E_CANTOPENOBJ, FAIL, "unable to open group");
            if ((grp_id = H5VL_wrap_register(H5I_GROUP, grp, true)) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTREGISTER, FAIL, "unable to register ID for group");

            /* Make callback */
            if ((link_class->create_func)(name, grp_id, udata->lnk->u.ud.udata, udata->lnk->u.ud.size,
                                          H5P_DEFAULT) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CALLBACK, FAIL, "link creation callback failed");
        } /* end if */
    }     /* end if */

done:
    /* Check if an object was created */
    if (obj_created) {
        H5O_loc_t oloc; /* Object location for created object */

        /* Set up object location */
        memset(&oloc, 0, sizeof(oloc));
        oloc.file = grp_loc->oloc->file;
        oloc.addr = udata->lnk->u.hard.addr;

        /* Decrement refcount on new object's object header in memory */
        if (H5O_dec_rc_by_loc(&oloc) < 0)
            HDONE_ERROR(H5E_LINK, H5E_CANTDEC, FAIL, "unable to decrement refcount on newly created object");
    } /* end if */

    /* Close the location given to the user callback if it was created */
    if (grp_id >= 0) {
        if (H5I_dec_app_ref(grp_id) < 0)
            HDONE_ERROR(H5E_LINK, H5E_CANTRELEASE, FAIL, "unable to close ID from UD callback");
    } /* end if */
    else if (grp != NULL) {
        if (H5G_close(grp) < 0)
            HDONE_ERROR(H5E_LINK, H5E_CANTRELEASE, FAIL, "unable to close group given to UD callback");
    } /* end if */
    else if (temp_loc_init)
        H5G_loc_free(&temp_loc);

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__link_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5L__create_real
 *
 * Purpose:     Creates a link at a path location
 *
 *              lnk should have linkclass-specific information already
 *              set, but this function will take care of setting name.
 *
 *              obj_path can be NULL if the object's path doesn't need to
 *              be set, and obj_file can be NULL if the object is not a
 *              hard link.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__create_real(const H5G_loc_t *link_loc, const char *link_name, H5G_name_t *obj_path, H5F_t *obj_file,
                 H5O_link_t *lnk, H5O_obj_create_t *ocrt_info, hid_t lcpl_id)
{
    char           *norm_link_name = NULL;              /* Pointer to normalized link name */
    unsigned        target_flags   = H5G_TARGET_NORMAL; /* Flags to pass to group traversal function */
    H5P_genplist_t *lc_plist       = NULL;              /* Link creation property list */
    H5L_trav_cr_t   udata;                              /* User data for callback */
    herr_t          ret_value = SUCCEED;                /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(link_loc);
    assert(link_name && *link_name);
    assert(lnk);
    assert(lnk->type >= H5L_TYPE_HARD && lnk->type <= H5L_TYPE_MAX);

    /* Get normalized link name */
    if ((norm_link_name = H5G_normalize(link_name)) == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "can't normalize name");

    /* Check for flags present in creation property list */
    if (lcpl_id != H5P_DEFAULT) {
        unsigned crt_intmd_group;

        /* Get link creation property list */
        if (NULL == (lc_plist = (H5P_genplist_t *)H5I_object(lcpl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

        /* Get intermediate group creation property */
        if (H5CX_get_intermediate_group(&crt_intmd_group) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get 'create intermediate group' property");

        if (crt_intmd_group > 0)
            target_flags |= H5G_CRT_INTMD_GROUP;
    } /* end if */

    /* Set up user data
     * FILE is used to make sure that hard links don't cross files, and
     * should be NULL for other link types.
     * LC_PLIST is a pointer to the link creation property list.
     * PATH is a pointer to the path of the object being inserted if this is
     * a hard link; this is used to set the paths to objects when they are
     * created.  For other link types, this is NULL.
     * OCRT_INFO is a pointer to the structure for object creation.
     * LNK is the link struct passed into this function.  At this point all
     * of its fields should be populated except for name, which is set when
     * inserting it in the callback.
     */
    udata.file      = obj_file;
    udata.lc_plist  = lc_plist;
    udata.path      = obj_path;
    udata.ocrt_info = ocrt_info;
    udata.lnk       = lnk;

    /* Traverse the destination path & create new link */
    if (H5G_traverse(link_loc, link_name, target_flags, H5L__link_cb, &udata) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINSERT, FAIL, "can't insert link");

done:
    /* Free the normalized path name */
    if (norm_link_name)
        H5MM_xfree(norm_link_name);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__create_real() */

/*-------------------------------------------------------------------------
 * Function:	H5L__create_hard
 *
 * Purpose:	Creates a hard link from NEW_NAME to CUR_NAME.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__create_hard(H5G_loc_t *cur_loc, const char *cur_name, const H5G_loc_t *link_loc, const char *link_name,
                 hid_t lcpl_id)
{
    char      *norm_cur_name = NULL; /* Pointer to normalized current name */
    H5F_t     *link_file     = NULL; /* Pointer to file to link to */
    H5O_link_t lnk;                  /* Link to insert */
    H5G_loc_t  obj_loc;              /* Location of object to link to */
    H5G_name_t path;                 /* obj_loc's path*/
    H5O_loc_t  oloc;                 /* obj_loc's oloc */
    bool       loc_valid = false;
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(cur_loc);
    assert(cur_name && *cur_name);
    assert(link_loc);
    assert(link_name && *link_name);

    /* Get normalized copy of the current name */
    if ((norm_cur_name = H5G_normalize(cur_name)) == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "can't normalize name");

    /* Set up link data specific to hard links */
    lnk.type = H5L_TYPE_HARD;

    /* Get object location for object pointed to */
    obj_loc.path = &path;
    obj_loc.oloc = &oloc;
    H5G_loc_reset(&obj_loc);
    if (H5G_loc_find(cur_loc, norm_cur_name, &obj_loc) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "source object not found");
    loc_valid = true;

    /* Construct link information for eventual insertion */
    lnk.u.hard.addr = obj_loc.oloc->addr;

    /* Set destination's file information */
    link_file = obj_loc.oloc->file;

    /* Create actual link to the object.  Pass in NULL for the path, since this
     * function shouldn't change an object's user path. */
    if (H5L__create_real(link_loc, link_name, NULL, link_file, &lnk, NULL, lcpl_id) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create new link to object");

done:
    /* Free the object header location */
    if (loc_valid)
        if (H5G_loc_free(&obj_loc) < 0)
            HDONE_ERROR(H5E_LINK, H5E_CANTRELEASE, FAIL, "unable to free location");

    /* Free the normalized path name */
    if (norm_cur_name)
        H5MM_xfree(norm_cur_name);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__create_hard() */

/*-------------------------------------------------------------------------
 * Function:    H5L__create_soft
 *
 * Purpose:     Creates a soft link from LINK_NAME to TARGET_PATH.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__create_soft(const char *target_path, const H5G_loc_t *link_loc, const char *link_name, hid_t lcpl_id)
{
    char      *norm_target = NULL;  /* Pointer to normalized current name */
    H5O_link_t lnk;                 /* Link to insert */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(link_loc);
    assert(target_path && *target_path);
    assert(link_name && *link_name);

    /* Get normalized copy of the link target */
    if ((norm_target = H5G_normalize(target_path)) == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "can't normalize name");

    /* Set up link data specific to soft links */
    lnk.type        = H5L_TYPE_SOFT;
    lnk.u.soft.name = norm_target;

    /* Create actual link to the object */
    if (H5L__create_real(link_loc, link_name, NULL, NULL, &lnk, NULL, lcpl_id) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create new link to object");

done:
    /* Free the normalized target name */
    if (norm_target)
        H5MM_xfree(norm_target);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__create_soft() */

/*-------------------------------------------------------------------------
 * Function:	H5L__create_ud
 *
 * Purpose:	Creates a user-defined link. See H5Lcreate_ud for
 *              full documentation.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__create_ud(const H5G_loc_t *link_loc, const char *link_name, const void *ud_data, size_t ud_data_size,
               H5L_type_t type, hid_t lcpl_id)
{
    H5O_link_t lnk;                 /* Link to insert */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(type >= H5L_TYPE_UD_MIN && type <= H5L_TYPE_MAX);
    assert(link_loc);
    assert(link_name && *link_name);
    assert(ud_data_size == 0 || ud_data);

    /* Initialize the link struct's pointer to its udata buffer */
    lnk.u.ud.udata = NULL;

    /* Make sure that this link class is registered */
    if (H5L__find_class_idx(type) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "link class has not been registered with library");

    /* Fill in UD link-specific information in the link struct*/
    if (ud_data_size > 0) {
        lnk.u.ud.udata = H5MM_malloc((size_t)ud_data_size);
        H5MM_memcpy(lnk.u.ud.udata, ud_data, (size_t)ud_data_size);
    } /* end if */
    else
        lnk.u.ud.udata = NULL;

    lnk.u.ud.size = ud_data_size;
    lnk.type      = type;

    /* Create actual link to the object */
    if (H5L__create_real(link_loc, link_name, NULL, NULL, &lnk, NULL, lcpl_id) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to register new name for object");

done:
    /* Free the link's udata buffer if it's been allocated */
    H5MM_xfree(lnk.u.ud.udata);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__create_ud() */

/*-------------------------------------------------------------------------
 * Function:	H5L__get_val_real
 *
 * Purpose:	Retrieve link value from a link object
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__get_val_real(const H5O_link_t *lnk, void *buf, size_t size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(lnk);

    /* Check for soft link */
    if (H5L_TYPE_SOFT == lnk->type) {
        /* Copy to output buffer */
        if (size > 0 && buf) {
            strncpy((char *)buf, lnk->u.soft.name, size);
            if (strlen(lnk->u.soft.name) >= size)
                ((char *)buf)[size - 1] = '\0';
        } /* end if */
    }     /* end if */
    /* Check for user-defined link */
    else if (lnk->type >= H5L_TYPE_UD_MIN) {
        const H5L_class_t *link_class; /* User-defined link class */

        /* Get the link class for this type of link.  It's okay if the class
         * isn't registered, though--we just can't give any more information
         * about it
         */
        link_class = H5L_find_class(lnk->type);

        if (link_class != NULL && link_class->query_func != NULL) {
            if ((link_class->query_func)(lnk->name, lnk->u.ud.udata, lnk->u.ud.size, buf, size) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CALLBACK, FAIL, "query callback returned failure");
        } /* end if */
        else if (buf && size > 0)
            ((char *)buf)[0] = '\0';
    } /* end if */
    else
        HGOTO_ERROR(H5E_LINK, H5E_BADTYPE, FAIL, "object is not a symbolic or user-defined link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__get_val_real() */

/*-------------------------------------------------------------------------
 * Function:	H5L__get_val_cb
 *
 * Purpose:	Callback for retrieving link value or udata.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__get_val_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                H5G_loc_t H5_ATTR_UNUSED *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_gv_t *udata     = (H5L_trav_gv_t *)_udata; /* User data passed in */
    herr_t         ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (lnk == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "'%s' doesn't exist", name);

    /* Retrieve the value for the link */
    if (H5L__get_val_real(lnk, udata->buf, udata->size) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't retrieve link value");

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__get_val_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5L__get_val
 *
 * Purpose:	Returns the value of a symbolic link or the udata for a
 *              user-defined link.
 *
 * Return:	Success:	Non-negative, with at most SIZE bytes of the
 *				link value copied into the BUF buffer.  If the
 *				link value is larger than SIZE characters
 *				counting the null terminator then the BUF
 *				result will not be null terminated.
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__get_val(const H5G_loc_t *loc, const char *name, void *buf /*out*/, size_t size)
{
    H5L_trav_gv_t udata;               /* User data for callback */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(loc);
    assert(name && *name);

    /* Set up user data for retrieving information */
    udata.size = size;
    udata.buf  = buf;

    /* Traverse the group hierarchy to locate the object to get info about */
    if (H5G_traverse(loc, name, H5G_TARGET_SLINK | H5G_TARGET_UDLINK, H5L__get_val_cb, &udata) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "name doesn't exist");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L__get_val() */

/*-------------------------------------------------------------------------
 * Function:	H5L__get_val_by_idx_cb
 *
 * Purpose:	Callback for retrieving a link's value according to an
 *              index's order.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__get_val_by_idx_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                       const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                       H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_gvbi_t *udata = (H5L_trav_gvbi_t *)_udata; /* User data passed in */
    H5O_link_t       fnd_lnk;                           /* Link within group */
    bool             lnk_copied = false;                /* Whether the link was copied */
    herr_t           ret_value  = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name of the group resolved to a valid object */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "group doesn't exist");

    /* Query link */
    if (H5G_obj_lookup_by_idx(obj_loc->oloc, udata->idx_type, udata->order, udata->n, &fnd_lnk) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "link not found");
    lnk_copied = true;

    /* Retrieve the value for the link */
    if (H5L__get_val_real(&fnd_lnk, udata->buf, udata->size) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't retrieve link value");

done:
    /* Reset the link information, if we have a copy */
    if (lnk_copied)
        H5O_msg_reset(H5O_LINK_ID, &fnd_lnk);

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__get_val_by_idx_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5L__get_val_by_idx
 *
 * Purpose:     Internal routine to query a link value according to the
 *              index within a group
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__get_val_by_idx(const H5G_loc_t *loc, const char *name, H5_index_t idx_type, H5_iter_order_t order,
                    hsize_t n, void *buf /*out*/, size_t size)
{
    H5L_trav_gvbi_t udata;               /* User data for callback */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(loc);
    assert(name && *name);

    /* Set up user data for retrieving information */
    udata.idx_type = idx_type;
    udata.order    = order;
    udata.n        = n;
    udata.buf      = buf;
    udata.size     = size;

    /* Traverse the group hierarchy to locate the object to get info about */
    if (H5G_traverse(loc, name, H5G_TARGET_SLINK | H5G_TARGET_UDLINK, H5L__get_val_by_idx_cb, &udata) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link info for index: %llu",
                    (unsigned long long)n);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__get_val_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5L__delete_cb
 *
 * Purpose:	Callback for deleting a link.  This routine
 *              actually deletes the link
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__delete_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
               H5G_loc_t H5_ATTR_UNUSED *obj_loc, void H5_ATTR_UNUSED *_udata /*in,out*/,
               H5G_own_loc_t *own_loc /*out*/)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check if the group resolved to a valid link */
    if (grp_loc == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "group doesn't exist");

    /* Check if the name in this group resolved to a valid link */
    if (name == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "name doesn't exist");

    /* Check for non-existent (NULL) link.
     * Note that this can also occur when attempting to remove '.'
     */
    if (lnk == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL,
                    "callback link pointer is NULL (specified link may be '.' or not exist)");

    /* Remove the link from the group */
    if (H5G_obj_remove(grp_loc->oloc, grp_loc->path->full_path_r, name) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to remove link from group");

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__delete_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5L__delete
 *
 * Purpose:	Delete a link from a group.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__delete(const H5G_loc_t *loc, const char *name)
{
    char  *norm_name = NULL;    /* Pointer to normalized name */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(loc);
    assert(name && *name);

    /* Get normalized copy of the name */
    if ((norm_name = H5G_normalize(name)) == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "can't normalize name");

    /* Set up user data for unlink operation */
    if (H5G_traverse(loc, norm_name, H5G_TARGET_SLINK | H5G_TARGET_UDLINK | H5G_TARGET_MOUNT, H5L__delete_cb,
                     NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTREMOVE, FAIL, "can't unlink object");

done:
    /* Free the normalized path name */
    if (norm_name)
        H5MM_xfree(norm_name);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__delete() */

/*-------------------------------------------------------------------------
 * Function:	H5L__delete_by_idx_cb
 *
 * Purpose:	Callback for removing a link according to an index's order.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__delete_by_idx_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                      const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                      H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_gvbi_t *udata     = (H5L_trav_gvbi_t *)_udata; /* User data passed in */
    herr_t           ret_value = SUCCEED;                   /* Return value */

    FUNC_ENTER_PACKAGE_TAG((obj_loc) ? (obj_loc->oloc->addr) : HADDR_UNDEF)

    /* Check if the name of the group resolved to a valid object */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "group doesn't exist");

    /* Delete link */
    if (H5G_obj_remove_by_idx(obj_loc->oloc, obj_loc->path->full_path_r, udata->idx_type, udata->order,
                              udata->n) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "link not found");

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5L__delete_by_idx_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5L__delete_by_idx
 *
 * Purpose:     Internal routine to delete a link according to its index
 *              within a group.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__delete_by_idx(const H5G_loc_t *loc, const char *name, H5_index_t idx_type, H5_iter_order_t order,
                   hsize_t n)
{
    H5L_trav_rmbi_t udata;               /* User data for callback */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(loc);
    assert(name && *name);

    /* Set up user data for unlink operation */
    udata.idx_type = idx_type;
    udata.order    = order;
    udata.n        = n;

    /* Traverse the group hierarchy to remove the link */
    if (H5G_traverse(loc, name, H5G_TARGET_SLINK | H5G_TARGET_UDLINK | H5G_TARGET_MOUNT,
                     H5L__delete_by_idx_cb, &udata) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "link doesn't exist");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__delete_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5L__move_dest_cb
 *
 * Purpose:	Second callback for moving and renaming an object.  This routine
 *              inserts a new link into the group returned by the traversal.
 *              It is called by H5L__move_cb.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__move_dest_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t H5_ATTR_UNUSED *lnk,
                  H5G_loc_t *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_mv2_t *udata  = (H5L_trav_mv2_t *)_udata; /* User data passed in */
    H5G_t          *grp    = NULL; /* H5G_t for this group, opened to pass to user callback */
    hid_t           grp_id = FAIL; /* ID for this group (passed to user callback */
    H5G_loc_t       temp_loc;      /* For UD callback */
    bool            temp_loc_init = false;
    herr_t          ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Make sure an object with this name doesn't already exist */
    if (obj_loc != NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "an object with that name already exists");

    /* Check for crossing file boundaries with a new hard link */
    if (udata->lnk->type == H5L_TYPE_HARD)
        /* Check that both objects are in same file */
        if (!H5F_SAME_SHARED(grp_loc->oloc->file, udata->file))
            HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "moving a link across files is not allowed");

    /* Give the object its new name */
    assert(udata->lnk->name == NULL);
    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    udata->lnk->name = (char *)name;
    H5_GCC_CLANG_DIAG_ON("cast-qual")

    /* Insert the link into the group */
    if (H5G_obj_insert(grp_loc->oloc, name, udata->lnk, true, H5O_TYPE_UNKNOWN, NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create new link to object");

    /* If the link was a user-defined link, call its move callback if it has one */
    if (udata->lnk->type >= H5L_TYPE_UD_MIN) {
        const H5L_class_t *link_class; /* User-defined link class */

        /* Get the link class for this type of link. */
        if (NULL == (link_class = H5L_find_class(udata->lnk->type)))
            HGOTO_ERROR(H5E_LINK, H5E_NOTREGISTERED, FAIL, "link class is not registered");

        if ((!udata->copy && link_class->move_func) || (udata->copy && link_class->copy_func)) {
            H5O_loc_t  temp_oloc;
            H5G_name_t temp_path;

            /* Create a temporary location (or else H5G_open will do a shallow
             * copy and wipe out grp_loc)
             */
            H5G_name_reset(&temp_path);
            if (H5O_loc_copy_deep(&temp_oloc, grp_loc->oloc) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to copy object location");

            temp_loc.oloc = &temp_oloc;
            temp_loc.path = &temp_path;
            temp_loc_init = true;

            /* Set up location for user-defined callback */
            if (NULL == (grp = H5G_open(&temp_loc)))
                HGOTO_ERROR(H5E_LINK, H5E_CANTOPENOBJ, FAIL, "unable to open group");
            if ((grp_id = H5VL_wrap_register(H5I_GROUP, grp, true)) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTREGISTER, FAIL, "unable to register group ID");

            if (udata->copy) {
                if ((link_class->copy_func)(udata->lnk->name, grp_id, udata->lnk->u.ud.udata,
                                            udata->lnk->u.ud.size) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_CALLBACK, FAIL, "UD copy callback returned error");
            } /* end if */
            else {
                if ((link_class->move_func)(udata->lnk->name, grp_id, udata->lnk->u.ud.udata,
                                            udata->lnk->u.ud.size) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_CALLBACK, FAIL, "UD move callback returned error");
            } /* end else */
        }     /* end if */
    }         /* end if */

done:
    /* Close the location given to the user callback if it was created */
    if (grp_id >= 0) {
        if (H5I_dec_app_ref(grp_id) < 0)
            HDONE_ERROR(H5E_LINK, H5E_CANTRELEASE, FAIL, "unable to close ID from UD callback");
    } /* end if */
    else if (grp != NULL) {
        if (H5G_close(grp) < 0)
            HDONE_ERROR(H5E_LINK, H5E_CANTRELEASE, FAIL, "unable to close group given to UD callback");
    } /* end if */
    else if (temp_loc_init)
        H5G_loc_free(&temp_loc);

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    /* Reset the "name" field in udata->lnk because it is owned by traverse()
     * and must not be manipulated after traverse closes */
    udata->lnk->name = NULL;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__move_dest_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5L__move_cb
 *
 * Purpose:	Callback for moving and renaming an object.  This routine
 *              replaces the names of open objects with the moved object
 *              in the path
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__move_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk, H5G_loc_t *obj_loc,
             void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_mv_t *udata = (H5L_trav_mv_t *)_udata; /* User data passed in */
    H5L_trav_mv2_t udata_out;                       /* User data for H5L__move_dest_cb traversal */
    char          *orig_name   = NULL;              /* The name of the link in this group */
    bool           link_copied = false;             /* Has udata_out.lnk been allocated? */
    herr_t         ret_value   = SUCCEED;           /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "name doesn't exist");

    /* Check for operations on '.' */
    if (lnk == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "the name of a link must be supplied to move or copy");

    /* Set up user data for move_dest_cb */
    if (NULL == (udata_out.lnk = (H5O_link_t *)H5O_msg_copy(H5O_LINK_ID, lnk, NULL)))
        HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to copy link to be moved");

    /* In this special case, the link's name is going to be replaced at its
     * destination, so we should free it here.
     */
    udata_out.lnk->name = (char *)H5MM_xfree(udata_out.lnk->name);
    link_copied         = true;

    udata_out.lnk->cset = udata->cset;
    udata_out.file      = grp_loc->oloc->file;
    udata_out.copy      = udata->copy;

    /* Keep a copy of link's name (it's "owned" by the H5G_traverse() routine) */
    orig_name = H5MM_xstrdup(name);

    /* Reset the # of soft / UD links that can be traversed, so that the second
     * (destination) traversal has the correct value
     */
    if (H5CX_set_nlinks(udata->orig_nlinks) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't reset # of soft / UD links to traverse");

    /* Insert the link into its new location */
    if (H5G_traverse(udata->dst_loc, udata->dst_name, udata->dst_target_flags, H5L__move_dest_cb,
                     &udata_out) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to follow symbolic link");

    /* If this is a move and not a copy operation, change the object's name and remove the old link */
    if (!udata->copy) {
        H5RS_str_t *dst_name_r; /* Ref-counted version of dest name */

        /* Make certain that the destination name is a full (not relative) path */
        if (*(udata->dst_name) != '/') {
            assert(udata->dst_loc->path->full_path_r);

            /* Create reference counted string for full dst path */
            if ((dst_name_r = H5G_build_fullpath_refstr_str(udata->dst_loc->path->full_path_r,
                                                            udata->dst_name)) == NULL)
                HGOTO_ERROR(H5E_LINK, H5E_PATH, FAIL, "can't build destination path name");
        } /* end if */
        else
            dst_name_r = H5RS_wrap(udata->dst_name);
        assert(dst_name_r);

        /* Fix names up */
        if (H5G_name_replace(lnk, H5G_NAME_MOVE, obj_loc->oloc->file, obj_loc->path->full_path_r,
                             udata->dst_loc->oloc->file, dst_name_r) < 0) {
            H5RS_decr(dst_name_r);
            HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to replace name");
        } /* end if */

        /* Remove the old link */
        if (H5G_obj_remove(grp_loc->oloc, grp_loc->path->full_path_r, orig_name) < 0) {
            H5RS_decr(dst_name_r);
            HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to remove old name");
        } /* end if */

        H5RS_decr(dst_name_r);
    } /* end if */

done:
    /* Cleanup */
    if (orig_name)
        H5MM_xfree(orig_name);

    /* If udata_out.lnk was copied, free any memory allocated
     * In this special case, the H5L__move_dest_cb callback resets the name
     * so H5O_msg_free shouldn't try to free it
     */
    if (link_copied)
        H5O_msg_free(H5O_LINK_ID, udata_out.lnk);

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__move_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5L__move
 *
 * Purpose:	Atomically move or copy a link.
 *
 *              Creates a copy of a link in a new destination with a new name.
 *              SRC_LOC and SRC_NAME together define the link's original
 *              location, while DST_LOC and DST_NAME together define its
 *              final location.
 *
 *              If copy_flag is false, the original link is removed
 *              (effectively moving the link).
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__move(const H5G_loc_t *src_loc, const char *src_name, const H5G_loc_t *dst_loc, const char *dst_name,
          bool copy_flag, hid_t lcpl_id)
{
    unsigned        dst_target_flags = H5G_TARGET_NORMAL;
    H5T_cset_t      char_encoding    = H5F_DEFAULT_CSET; /* Character encoding for link */
    H5P_genplist_t *lc_plist;                            /* Link creation property list */
    H5L_trav_mv_t   udata;                               /* User data for traversal */
    herr_t          ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(src_loc);
    assert(dst_loc);
    assert(src_name && *src_name);
    assert(dst_name && *dst_name);

    /* Check for flags present in creation property list */
    if (lcpl_id != H5P_DEFAULT) {
        unsigned crt_intmd_group;

        if (NULL == (lc_plist = (H5P_genplist_t *)H5I_object(lcpl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

        /* Get intermediate group creation property */
        if (H5CX_get_intermediate_group(&crt_intmd_group) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get property value for creating missing groups");

        /* Set target flags for source and destination */
        if (crt_intmd_group > 0)
            dst_target_flags |= H5G_CRT_INTMD_GROUP;

        /* Get character encoding property */
        if (H5CX_get_encoding(&char_encoding) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get property value for character encoding");
    } /* end if */

    /* Set up user data */
    udata.dst_loc          = dst_loc;
    udata.dst_name         = dst_name;
    udata.dst_target_flags = dst_target_flags;
    udata.cset             = char_encoding;
    udata.copy             = copy_flag;

    /* Retrieve the original # of soft / UD links that can be traversed, so
     *  that the countdown can be reset after the first path is traversed.
     */
    if (H5CX_get_nlinks(&udata.orig_nlinks) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to retrieve # of soft / UD links to traverse");

    /* Do the move */
    if (H5G_traverse(src_loc, src_name, H5G_TARGET_MOUNT | H5G_TARGET_SLINK | H5G_TARGET_UDLINK, H5L__move_cb,
                     &udata) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to find link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__move() */

/*-------------------------------------------------------------------------
 * Function:	H5L__exists_final_cb
 *
 * Purpose:	Callback for checking whether a link exists, as the final
 *              component of a path
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__exists_final_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                     const H5O_link_t *lnk, H5G_loc_t H5_ATTR_UNUSED *obj_loc, void *_udata /*in,out*/,
                     H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_le_t *udata = (H5L_trav_le_t *)_udata; /* User data passed in */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check if the name in this group resolved to a valid link */
    *udata->exists = (bool)(lnk != NULL);

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5L__exists_final_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5L__exists_inter_cb
 *
 * Purpose:	Callback for checking whether a link exists, as an intermediate
 *              component of a path
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__exists_inter_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                     const H5O_link_t *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                     H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_le_t *udata     = (H5L_trav_le_t *)_udata; /* User data passed in */
    herr_t         ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (lnk != NULL) {
        /* Check for more components to the path */
        if (udata->sep) {
            H5G_traverse_t cb_func; /* Callback function for traversal */
            char          *next;    /* Pointer to next component name */

            /* Look for another separator */
            next = udata->sep;
            if (NULL == (udata->sep = strchr(udata->sep, '/')))
                cb_func = H5L__exists_final_cb;
            else {
                /* Chew through adjacent separators, if present */
                do {
                    *udata->sep = '\0';
                    udata->sep++;
                } while ('/' == *udata->sep);
                cb_func = H5L__exists_inter_cb;
            } /* end else */
            if (H5G_traverse(obj_loc, next, H5G_TARGET_SLINK | H5G_TARGET_UDLINK, cb_func, udata) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if link exists");
        } /* end if */
        else
            *udata->exists = true;
    } /* end if */
    else
        *udata->exists = false;

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__exists_inter_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5L_exists_tolerant
 *
 * Purpose:	Returns whether a link exists in a group
 *
 * Note:	Same as H5L__exists, except that missing links are reported
 *		as 'false' instead of causing failures
 *
 * Return:	Non-negative (true/false) on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_exists_tolerant(const H5G_loc_t *loc, const char *name, bool *exists)
{
    H5L_trav_le_t  udata;               /* User data for traversal */
    H5G_traverse_t cb_func;             /* Callback function for traversal */
    char          *name_copy = NULL;    /* Duplicate of name */
    char          *name_trav;           /* Name to traverse */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(loc);
    assert(name);
    assert(exists);

    /* Copy the name and skip leading '/'s */
    name_trav = name_copy = H5MM_strdup(name);
    while ('/' == *name_trav)
        name_trav++;

    /* A path of "/" will always exist in a file */
    if ('\0' == *name_trav)
        *exists = true;
    else {
        /* Set up user data & correct callback */
        udata.exists = exists;
        if (NULL == (udata.sep = strchr(name_trav, '/')))
            cb_func = H5L__exists_final_cb;
        else {
            /* Chew through adjacent separators, if present */
            do {
                *udata.sep = '\0';
                udata.sep++;
            } while ('/' == *udata.sep);
            cb_func = H5L__exists_inter_cb;
        } /* end else */

        /* Traverse the group hierarchy to locate the link to check */
        if (H5G_traverse(loc, name_trav, H5G_TARGET_SLINK | H5G_TARGET_UDLINK, cb_func, &udata) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if link exists");
    }

done:
    /* Release duplicated string */
    H5MM_xfree(name_copy);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L_exists_tolerant() */

/*-------------------------------------------------------------------------
 * Function:	H5L__exists
 *
 * Purpose:	Returns whether a link exists in a group
 *
 * Note:	Same as H5L_exists_tolerant, except that missing links are reported
 *		as failures
 *
 * Return:	Non-negative on success, with *exists set/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__exists(const H5G_loc_t *loc, const char *name, bool *exists)
{
    H5L_trav_le_t udata;               /* User data for traversal */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(loc);
    assert(name);
    assert(exists);

    /* A path of "/" will always exist in a file */
    if (0 == strcmp(name, "/"))
        *exists = true;
    else {
        /* Traverse the group hierarchy to locate the object to get info about */
        udata.exists = exists;
        if (H5G_traverse(loc, name, H5G_TARGET_SLINK | H5G_TARGET_UDLINK, H5L__exists_final_cb, &udata) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_EXISTS, FAIL, "link doesn't exist");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L__exists() */

/*-------------------------------------------------------------------------
 * Function:	H5L__get_info_cb
 *
 * Purpose:	Callback for retrieving a link's metadata
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__get_info_cb(H5G_loc_t *grp_loc /*in*/, const char H5_ATTR_UNUSED *name, const H5O_link_t *lnk,
                 H5G_loc_t H5_ATTR_UNUSED *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_gi_t *udata     = (H5L_trav_gi_t *)_udata; /* User data passed in */
    herr_t         ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (lnk == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "name doesn't exist");

    /* Get information from the link */
    if (H5G_link_to_info(grp_loc->oloc, lnk, udata->linfo) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link info");

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__get_info_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5L_get_info
 *
 * Purpose:     Returns metadata about a link.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_get_info(const H5G_loc_t *loc, const char *name, H5L_info2_t *linfo /*out*/)
{
    H5L_trav_gi_t udata;               /* User data for callback */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    udata.linfo = linfo;

    /* Traverse the group hierarchy to locate the object to get info about */
    if (H5G_traverse(loc, name, H5G_TARGET_SLINK | H5G_TARGET_UDLINK, H5L__get_info_cb, &udata) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_EXISTS, FAIL, "name doesn't exist");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L_get_info() */

/*-------------------------------------------------------------------------
 * Function:	H5L__get_info_by_idx_cb
 *
 * Purpose:	Callback for retrieving a link's metadata according to an
 *              index's order.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__get_info_by_idx_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                        const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                        H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_gibi_t *udata = (H5L_trav_gibi_t *)_udata; /* User data passed in */
    H5O_link_t       fnd_lnk;                           /* Link within group */
    bool             lnk_copied = false;                /* Whether the link was copied */
    herr_t           ret_value  = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name of the group resolved to a valid object */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "group doesn't exist");

    /* Query link */
    if (H5G_obj_lookup_by_idx(obj_loc->oloc, udata->idx_type, udata->order, udata->n, &fnd_lnk) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "link not found");
    lnk_copied = true;

    /* Get information from the link */
    if (H5G_link_to_info(obj_loc->oloc, &fnd_lnk, udata->linfo) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get link info");

done:
    /* Reset the link information, if we have a copy */
    if (lnk_copied)
        H5O_msg_reset(H5O_LINK_ID, &fnd_lnk);

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__get_info_by_idx_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5L__get_info_by_idx
 *
 * Purpose:     Internal routine to retrieve link info according to an
 *              index's order.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__get_info_by_idx(const H5G_loc_t *loc, const char *name, H5_index_t idx_type, H5_iter_order_t order,
                     hsize_t n, H5L_info2_t *linfo /*out*/)
{
    H5L_trav_gibi_t udata;               /* User data for callback */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(loc);
    assert(name && *name);
    assert(linfo);

    /* Set up user data for callback */
    udata.idx_type = idx_type;
    udata.order    = order;
    udata.n        = n;
    udata.linfo    = linfo;

    /* Traverse the group hierarchy to locate the object to get info about */
    if (H5G_traverse(loc, name, H5G_TARGET_SLINK | H5G_TARGET_UDLINK, H5L__get_info_by_idx_cb, &udata) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to get link info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__get_info_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5L__get_name_by_idx_cb
 *
 * Purpose:	Callback for retrieving a link's name according to an
 *              index's order.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__get_name_by_idx_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                        const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                        H5G_own_loc_t *own_loc /*out*/)
{
    H5L_trav_gnbi_t *udata     = (H5L_trav_gnbi_t *)_udata; /* User data passed in */
    herr_t           ret_value = SUCCEED;                   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name of the group resolved to a valid object */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "group doesn't exist");

    /* Query link */
    if (H5G_obj_get_name_by_idx(obj_loc->oloc, udata->idx_type, udata->order, udata->n, udata->name,
                                udata->size, &udata->name_len) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "link not found");

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__get_name_by_idx_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5L__get_name_by_idx
 *
 * Purpose:     Internal routine to retrieve link name according to an
 *              index's order.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__get_name_by_idx(const H5G_loc_t *loc, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                     hsize_t n, char *name /*out*/, size_t size, size_t *link_name_len)
{
    H5L_trav_gnbi_t udata;               /* User data for callback */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(loc);
    assert(group_name && *group_name);
    assert(link_name_len);

    /* Set up user data for callback */
    udata.idx_type = idx_type;
    udata.order    = order;
    udata.n        = n;
    udata.name     = name;
    udata.size     = size;
    udata.name_len = 0;

    /* Traverse the group hierarchy to locate the link to get name of */
    if (H5G_traverse(loc, group_name, H5G_TARGET_SLINK | H5G_TARGET_UDLINK, H5L__get_name_by_idx_cb, &udata) <
        0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get name");

    /* Set the return value */
    *link_name_len = udata.name_len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__get_name_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5L__link_copy_file
 *
 * Purpose:     Copy a link and the object it points to from one file to
 *              another.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L__link_copy_file(H5F_t *dst_file, const H5O_link_t *_src_lnk, const H5O_loc_t *src_oloc,
                    H5O_link_t *dst_lnk, H5O_copy_t *cpy_info)
{
    H5O_link_t        tmp_src_lnk;                   /* Temporary copy of src link, when needed */
    const H5O_link_t *src_lnk            = _src_lnk; /* Source link */
    bool              dst_lnk_init       = false;    /* Whether the destination link is initialized */
    bool              expanded_link_open = false;    /* Whether the target location has been opened */
    H5G_loc_t         tmp_src_loc;                   /* Group location holding target object */
    H5G_name_t        tmp_src_path;                  /* Path for target object */
    H5O_loc_t         tmp_src_oloc;                  /* Object location for target object */
    herr_t            ret_value = SUCCEED;           /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(dst_file);
    assert(src_lnk);
    assert(dst_lnk);
    assert(cpy_info);

    /* Expand soft or external link, if requested */
    if ((H5L_TYPE_SOFT == src_lnk->type && cpy_info->expand_soft_link) ||
        (H5L_TYPE_EXTERNAL == src_lnk->type && cpy_info->expand_ext_link)) {
        H5G_loc_t  lnk_grp_loc;        /* Group location holding link */
        H5G_name_t lnk_grp_path;       /* Path for link */
        bool       tar_exists = false; /* Whether the target object exists */

        /* Set up group location for link */
        H5G_name_reset(&lnk_grp_path);
        lnk_grp_loc.path = &lnk_grp_path;
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        lnk_grp_loc.oloc = (H5O_loc_t *)src_oloc;
        H5_GCC_CLANG_DIAG_ON("cast-qual")

        /* Check if the target object exists */
        if (H5G_loc_exists(&lnk_grp_loc, src_lnk->name, &tar_exists) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to check if target object exists");

        if (tar_exists) {
            /* Make a temporary copy of the link, so that it will not change the
             * info in the cache when we change it to a hard link */
            if (NULL == H5O_msg_copy(H5O_LINK_ID, src_lnk, &tmp_src_lnk))
                HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to copy message");

            /* Set up group location for target object.  Let H5G_traverse expand
             * the link. */
            tmp_src_loc.path = &tmp_src_path;
            tmp_src_loc.oloc = &tmp_src_oloc;
            if (H5G_loc_reset(&tmp_src_loc) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to reset location");

            /* Find the target object */
            if (H5G_loc_find(&lnk_grp_loc, src_lnk->name, &tmp_src_loc) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to find target object");
            expanded_link_open = true;

            /* Convert symbolic link to hard link */
            if (tmp_src_lnk.type == H5L_TYPE_SOFT)
                tmp_src_lnk.u.soft.name = (char *)H5MM_xfree(tmp_src_lnk.u.soft.name);
            else if (tmp_src_lnk.u.ud.size > 0)
                tmp_src_lnk.u.ud.udata = H5MM_xfree(tmp_src_lnk.u.ud.udata);
            tmp_src_lnk.type        = H5L_TYPE_HARD;
            tmp_src_lnk.u.hard.addr = tmp_src_oloc.addr;
            src_lnk                 = &tmp_src_lnk;
        } /* end if */
    }     /* end if */

    /* Copy src link information to dst link information */
    if (NULL == H5O_msg_copy(H5O_LINK_ID, src_lnk, dst_lnk))
        HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to copy message");
    dst_lnk_init = true;

    /* Check if object in source group is a hard link & copy it */
    if (H5L_TYPE_HARD == src_lnk->type) {
        H5O_loc_t new_dst_oloc; /* Copied object location in destination */

        /* Set up copied object location to fill in */
        H5O_loc_reset(&new_dst_oloc);
        new_dst_oloc.file = dst_file;

        if (!expanded_link_open) {
            /* Build temporary object location for source */
            H5O_loc_reset(&tmp_src_oloc);
            tmp_src_oloc.file = src_oloc->file;
            tmp_src_oloc.addr = src_lnk->u.hard.addr;
        } /* end if */
        assert(H5_addr_defined(tmp_src_oloc.addr));

        /* Copy the shared object from source to destination */
        /* Don't care about obj_type or udata because those are only important
         * for old style groups */
        if (H5O_copy_header_map(&tmp_src_oloc, &new_dst_oloc, cpy_info, true, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to copy object");

        /* Copy new destination object's information for eventual insertion */
        dst_lnk->u.hard.addr = new_dst_oloc.addr;
    } /* end if */

done:
    /* Check if we used a temporary src link */
    if (src_lnk != _src_lnk) {
        assert(src_lnk == &tmp_src_lnk);
        H5O_msg_reset(H5O_LINK_ID, &tmp_src_lnk);
    } /* end if */
    if (ret_value < 0)
        if (dst_lnk_init)
            H5O_msg_reset(H5O_LINK_ID, dst_lnk);
    /* Check if we need to free the temp source oloc */
    if (expanded_link_open)
        if (H5G_loc_free(&tmp_src_loc) < 0)
            HDONE_ERROR(H5E_LINK, H5E_CANTFREE, FAIL, "unable to free object");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__link_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5L_iterate
 *
 * Purpose:     Iterates through links in a group
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_iterate(H5G_loc_t *loc, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
            hsize_t *idx_p, H5L_iterate2_t op, void *op_data)
{
    H5G_link_iterate_t lnk_op;           /* Link operator                    */
    hsize_t            last_lnk;         /* Index of last object looked at   */
    hsize_t            idx;              /* Internal location to hold index  */
    herr_t             ret_value = FAIL; /* Return value                     */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity checks */
    assert(loc);
    assert(group_name);
    assert(op);

    /* Set up iteration beginning/end info */
    idx      = (idx_p == NULL ? 0 : *idx_p);
    last_lnk = 0;

    /* Build link operator info */
    lnk_op.op_type        = H5G_LINK_OP_NEW;
    lnk_op.op_func.op_new = op;

    /* Iterate over the links */
    if ((ret_value = H5G_iterate(loc, group_name, idx_type, order, idx, &last_lnk, &lnk_op, op_data)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link iteration failed");

    /* Set the index we stopped at */
    if (idx_p)
        *idx_p = last_lnk;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L_iterate() */
