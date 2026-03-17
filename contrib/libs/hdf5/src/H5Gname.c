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
 * Created:		H5Gname.c
 *
 * Purpose:		Functions for handling group hierarchy paths.
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
#include "H5Dprivate.h"  /* Datasets				*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5Lprivate.h"  /* Links                                */
#include "H5MMprivate.h" /* Memory wrappers			*/

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Struct used by change name callback function */
typedef struct H5G_names_t {
    H5G_names_op_t op;              /* Operation performed on file */
    H5F_t         *src_file;        /* Top file in src location's mounted file hier. */
    H5RS_str_t    *src_full_path_r; /* Source location's full path */
    H5F_t         *dst_file;        /* Destination location's file */
    H5RS_str_t    *dst_full_path_r; /* Destination location's full path */
} H5G_names_t;

/* Info to pass to the iteration function when building name */
typedef struct H5G_gnba_iter_t {
    /* In */
    const H5O_loc_t *loc; /* The location of the object we're looking for */

    /* Out */
    char *path; /* Name of the object */
} H5G_gnba_iter_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

static htri_t      H5G__common_path(const H5RS_str_t *fullpath_r, const H5RS_str_t *prefix_r);
static H5RS_str_t *H5G__build_fullpath(const char *prefix, const char *name);
static herr_t      H5G__name_move_path(H5RS_str_t **path_r_ptr, const char *full_suffix, const char *src_path,
                                       const char *dst_path);
static int         H5G__name_replace_cb(void *obj_ptr, hid_t obj_id, void *key);

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
 * Function:	H5G__component
 *
 * Purpose:	Returns the pointer to the first component of the
 *		specified name by skipping leading slashes.  Returns
 *		the size in characters of the component through SIZE_P not
 *		counting leading slashes or the null terminator.
 *
 * Return:	Success:	Ptr into NAME.
 *
 *		Failure:	Ptr to the null terminator of NAME.
 *
 *-------------------------------------------------------------------------
 */
const char *
H5G__component(const char *name, size_t *size_p)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(name);

    while ('/' == *name)
        name++;
    if (size_p)
        *size_p = strcspn(name, "/");

    FUNC_LEAVE_NOAPI(name)
} /* end H5G__component() */

/*-------------------------------------------------------------------------
 * Function:	H5G_normalize
 *
 * Purpose:	Returns a pointer to a new string which has duplicate and
 *              trailing slashes removed from it.
 *
 * Return:	Success:	Ptr to normalized name.
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
char *
H5G_normalize(const char *name)
{
    char    *norm;             /* Pointer to the normalized string */
    size_t   s, d;             /* Positions within the strings */
    unsigned last_slash;       /* Flag to indicate last character was a slash */
    char    *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check */
    assert(name);

    /* Duplicate the name, to return */
    if (NULL == (norm = H5MM_strdup(name)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for normalized string");

    /* Walk through the characters, omitting duplicated '/'s */
    s = d      = 0;
    last_slash = 0;
    while (name[s] != '\0') {
        if (name[s] == '/')
            if (last_slash)
                ;
            else {
                norm[d++]  = name[s];
                last_slash = 1;
            } /* end else */
        else {
            norm[d++]  = name[s];
            last_slash = 0;
        } /* end else */
        s++;
    } /* end while */

    /* Terminate normalized string */
    norm[d] = '\0';

    /* Check for final '/' on normalized name & eliminate it */
    if (d > 1 && last_slash)
        norm[d - 1] = '\0';

    /* Set return value */
    ret_value = norm;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_normalize() */

/*-------------------------------------------------------------------------
 * Function: H5G__common_path
 *
 * Purpose: Determine if one path is a valid prefix of another path
 *
 * Return: true for valid prefix, false for not a valid prefix, FAIL
 *              on error
 *
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5G__common_path(const H5RS_str_t *fullpath_r, const H5RS_str_t *prefix_r)
{
    const char *fullpath;          /* Pointer to actual fullpath string */
    const char *prefix;            /* Pointer to actual prefix string */
    size_t      nchars1, nchars2;  /* Number of characters in components */
    htri_t      ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Get component of each name */
    fullpath = H5RS_get_str(fullpath_r);
    assert(fullpath);
    fullpath = H5G__component(fullpath, &nchars1);
    assert(fullpath);
    prefix = H5RS_get_str(prefix_r);
    assert(prefix);
    prefix = H5G__component(prefix, &nchars2);
    assert(prefix);

    /* Check if we have a real string for each component */
    while (*fullpath && *prefix) {
        /* Check that the components we found are the same length */
        if (nchars1 == nchars2) {
            /* Check that the two components are equal */
            if (strncmp(fullpath, prefix, nchars1) == 0) {
                /* Advance the pointers in the names */
                fullpath += nchars1;
                prefix += nchars2;

                /* Get next component of each name */
                fullpath = H5G__component(fullpath, &nchars1);
                assert(fullpath);
                prefix = H5G__component(prefix, &nchars2);
                assert(prefix);
            } /* end if */
            else
                HGOTO_DONE(false);
        } /* end if */
        else
            HGOTO_DONE(false);
    } /* end while */

    /* If we reached the end of the prefix path to check, it must be a valid prefix */
    if (*prefix == '\0')
        ret_value = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__common_path() */

/*-------------------------------------------------------------------------
 * Function: H5G__build_fullpath
 *
 * Purpose: Build a full path from a prefix & base pair of strings
 *
 * Return: Pointer to reference counted string on success, NULL on error
 *
 *-------------------------------------------------------------------------
 */
static H5RS_str_t *
H5G__build_fullpath(const char *prefix, const char *name)
{
    H5RS_str_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(prefix);
    assert(name);

    /* Create full path */
    if (NULL == (ret_value = H5RS_create(prefix)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, NULL, "can't create ref-counted string");
    if (prefix[strlen(prefix) - 1] != '/')
        H5RS_aputc(ret_value, '/'); /* Add separator, if the prefix doesn't end in one */
    H5RS_acat(ret_value, name);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__build_fullpath() */

/*-------------------------------------------------------------------------
 * Function:	H5G_build_fullpath_refstr_str
 *
 * Purpose:     Append an object path to an existing ref-counted path
 *
 * Return:	Success:	Non-NULL, combined path
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
H5RS_str_t *
H5G_build_fullpath_refstr_str(H5RS_str_t *prefix_r, const char *name)
{
    const char *prefix;           /* Pointer to raw string for path */
    H5RS_str_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(prefix_r);
    assert(name);

    /* Get the raw string for the user path */
    prefix = H5RS_get_str(prefix_r);
    assert(prefix);

    /* Create reference counted string for path */
    ret_value = H5G__build_fullpath(prefix, name);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_build_fullpath_refstr_str() */

/*-------------------------------------------------------------------------
 * Function:    H5G__name_init
 *
 * Purpose:     Set the initial path for a group hierarchy name
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__name_init(H5G_name_t *name, const char *path)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(name);

    /* Set the initial paths for a name object */
    name->full_path_r = H5RS_create(path);
    assert(name->full_path_r);
    name->user_path_r = H5RS_create(path);
    assert(name->user_path_r);
    name->obj_hidden = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G__name_init() */

/*-------------------------------------------------------------------------
 * Function:	H5G_name_set
 *
 * Purpose:     Set the name of a symbol entry OBJ, located at LOC
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_name_set(const H5G_name_t *loc, H5G_name_t *obj, const char *name)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(loc);
    assert(obj);
    assert(name);

    /* Free & reset the object's previous paths info (if they exist) */
    H5G_name_free(obj);

    /* Create the object's full path, if a full path exists in the location */
    if (loc->full_path_r) {
        /* Go build the new full path */
        if ((obj->full_path_r = H5G_build_fullpath_refstr_str(loc->full_path_r, name)) == NULL)
            HGOTO_ERROR(H5E_SYM, H5E_PATH, FAIL, "can't build user path name");
    } /* end if */

    /* Create the object's user path, if a user path exists in the location */
    if (loc->user_path_r) {
        /* Go build the new user path */
        if ((obj->user_path_r = H5G_build_fullpath_refstr_str(loc->user_path_r, name)) == NULL)
            HGOTO_ERROR(H5E_SYM, H5E_PATH, FAIL, "can't build user path name");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_name_set() */

/*-------------------------------------------------------------------------
 * Function:    H5G_name_copy
 *
 * Purpose:     Do a copy of group hier. names
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 * Notes:       'depth' parameter determines how much of the group entry
 *              structure we want to copy.  The depths are:
 *                  H5_COPY_SHALLOW - Copy all the fields from the source
 *                      to the destination, including the user path and
 *                      canonical path. (Destination "takes ownership" of
 *                      user and canonical paths)
 *                  H5_COPY_DEEP - Copy all the fields from the source to
 *                      the destination, deep copying the user and canonical
 *                      paths.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_name_copy(H5G_name_t *dst, const H5G_name_t *src, H5_copy_depth_t depth)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments */
    assert(src);
    assert(dst);
#if defined(H5_USING_MEMCHECKER) || !defined(NDEBUG)
    assert(dst->full_path_r == NULL);
    assert(dst->user_path_r == NULL);
#endif /* H5_USING_MEMCHECKER */
    assert(depth == H5_COPY_SHALLOW || depth == H5_COPY_DEEP);

    /* Copy the top level information */
    H5MM_memcpy(dst, src, sizeof(H5G_name_t));

    /* Deep copy the names */
    if (depth == H5_COPY_DEEP) {
        dst->full_path_r = H5RS_dup(src->full_path_r);
        dst->user_path_r = H5RS_dup(src->user_path_r);
    }
    else {
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        H5G_name_reset((H5G_name_t *)src);
        H5_GCC_CLANG_DIAG_ON("cast-qual")
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G_name_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5G_get_name
 *
 * Purpose:     Gets a name of an object from its ID.
 *
 * Notes:	Internal routine for H5Iget_name().

 * Return:	Success:	Non-negative, length of name
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_get_name(const H5G_loc_t *loc, char *name /*out*/, size_t size, size_t *name_len, bool *cached)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(loc);

    /* If the user path is available and it's not "hidden", use it */
    if (loc->path->user_path_r != NULL && loc->path->obj_hidden == 0) {
        size_t len; /* Length of object's name */

        len = H5RS_len(loc->path->user_path_r);

        if (name) {
            strncpy(name, H5RS_get_str(loc->path->user_path_r), MIN((len + 1), size));
            if (len >= size)
                name[size - 1] = '\0';
        } /* end if */

        /* Set name length, if requested */
        if (name_len)
            *name_len = len;

        /* Indicate that the name is cached, if requested */
        /* (Currently only used for testing - QAK, 2010/07/26) */
        if (cached)
            *cached = true;
    } /* end if */
    else if (!loc->path->obj_hidden) {
        /* Search for name of object */
        if (H5G_get_name_by_addr(loc->oloc->file, loc->oloc, name, size, name_len) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't determine name");

        /* Indicate that the name is _not_ cached, if requested */
        /* (Currently only used for testing - QAK, 2010/07/26) */
        if (cached)
            *cached = false;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_get_name() */

/*-------------------------------------------------------------------------
 * Function:	H5G_name_reset
 *
 * Purpose:	Reset a group hierarchy name to an empty state
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_name_reset(H5G_name_t *name)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments */
    assert(name);

    /* Clear the group hier. name to an empty state */
    memset(name, 0, sizeof(H5G_name_t));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G_name_reset() */

/*-------------------------------------------------------------------------
 * Function:	H5G_name_free
 *
 * Purpose:	Free the 'ID to name' buffers.
 *
 * Return:	Success
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_name_free(H5G_name_t *name)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(name);

    if (name->full_path_r) {
        H5RS_decr(name->full_path_r);
        name->full_path_r = NULL;
    } /* end if */
    if (name->user_path_r) {
        H5RS_decr(name->user_path_r);
        name->user_path_r = NULL;
    } /* end if */
    name->obj_hidden = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G_name_free() */

/*-------------------------------------------------------------------------
 * Function:    H5G__name_move_path
 *
 * Purpose:     Update a user or canonical path after an object moves
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__name_move_path(H5RS_str_t **path_r_ptr, const char *full_suffix, const char *src_path,
                    const char *dst_path)
{
    const char *path;                /* Path to update */
    size_t      path_len;            /* Length of path */
    size_t      full_suffix_len;     /* Length of full suffix */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(path_r_ptr && *path_r_ptr);
    assert(full_suffix);
    assert(src_path);
    assert(dst_path);

    /* Get pointer to path to update */
    path = H5RS_get_str(*path_r_ptr);
    assert(path);

    /* Check if path needs to be updated */
    full_suffix_len = strlen(full_suffix);
    path_len        = strlen(path);
    if (full_suffix_len < path_len) {
        const char *dst_suffix;        /* Destination suffix that changes */
        const char *src_suffix;        /* Source suffix that changes */
        size_t      path_prefix_len;   /* Length of path prefix */
        const char *path_prefix2;      /* 2nd prefix for path */
        size_t      path_prefix2_len;  /* Length of 2nd path prefix */
        size_t      common_prefix_len; /* Length of common prefix */
        H5RS_str_t *rs;                /* Ref-counted string for new path */

        /* Compute path prefix before full suffix */
        path_prefix_len = path_len - full_suffix_len;

        /* Determine the common prefix for src & dst paths */
        common_prefix_len = 0;
        /* Find first character that is different */
        while (*(src_path + common_prefix_len) == *(dst_path + common_prefix_len))
            common_prefix_len++;
        /* Back up to previous '/' */
        while (*(src_path + common_prefix_len) != '/')
            common_prefix_len--;
        /* Include '/' */
        common_prefix_len++;

        /* Determine source suffix */
        src_suffix = src_path + (common_prefix_len - 1);

        /* Determine destination suffix */
        dst_suffix = dst_path + (common_prefix_len - 1);

        /* Compute path prefix before src suffix */
        path_prefix2     = path;
        path_prefix2_len = path_prefix_len - strlen(src_suffix);

        /* Allocate new ref-counted string */
        if (NULL == (rs = H5RS_create(NULL)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, FAIL, "can't create ref-counted string");

        /* Create the new path */
        if (path_prefix2_len > 0)
            H5RS_ancat(rs, path_prefix2, path_prefix2_len);
        H5RS_acat(rs, dst_suffix);
        if (full_suffix_len > 0)
            H5RS_acat(rs, full_suffix);

        /* Release previous path */
        H5RS_decr(*path_r_ptr);

        /* Take ownership of the new full path */
        *path_r_ptr = rs;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__name_move_path() */

/*-------------------------------------------------------------------------
 * Function: H5G__name_replace_cb
 *
 * Purpose: H5I_iterate callback function to replace group entry names
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5G__name_replace_cb(void *obj_ptr, hid_t obj_id, void *key)
{
    const H5G_names_t *names = (const H5G_names_t *)key; /* Get operation's information */
    H5O_loc_t         *oloc;         /* Object location for object that the ID refers to */
    H5G_name_t        *obj_path;     /* Pointer to group hier. path for obj */
    H5F_t             *top_obj_file; /* Top file in object's mounted file hier. */
    bool   obj_in_child = false;     /* Flag to indicate that the object is in the child mount hier. */
    herr_t ret_value    = SUCCEED;   /* Return value */

    FUNC_ENTER_PACKAGE

    assert(obj_ptr);

    /* Get the symbol table entry */
    switch (H5I_get_type(obj_id)) {
        case H5I_GROUP:
            oloc     = H5G_oloc((H5G_t *)obj_ptr);
            obj_path = H5G_nameof((H5G_t *)obj_ptr);
            break;

        case H5I_DATASET:
            oloc     = H5D_oloc((H5D_t *)obj_ptr);
            obj_path = H5D_nameof((H5D_t *)obj_ptr);
            break;

        case H5I_DATATYPE:
            /* Avoid non-named datatypes */
            if (!H5T_is_named((H5T_t *)obj_ptr))
                HGOTO_DONE(SUCCEED); /* Do not exit search over IDs */

            oloc     = H5T_oloc((H5T_t *)obj_ptr);
            obj_path = H5T_nameof((H5T_t *)obj_ptr);
            break;

        case H5I_MAP:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "maps not supported in native VOL connector");

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_FILE:
        case H5I_DATASPACE:
        case H5I_ATTR:
        case H5I_VFL:
        case H5I_VOL:
        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
        case H5I_SPACE_SEL_ITER:
        case H5I_EVENTSET:
        case H5I_NTYPES:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "unknown data object");
    } /* end switch */
    assert(oloc);
    assert(obj_path);

    /* Check if the object has a full path still */
    if (!obj_path->full_path_r)
        HGOTO_DONE(SUCCEED); /* No need to look at object, it's path is already invalid */

    /* Find the top file in object's mount hier. */
    if (H5F_PARENT(oloc->file)) {
        /* Check if object is in child file (for mount & unmount operations) */
        if (names->dst_file && H5F_SAME_SHARED(oloc->file, names->dst_file))
            obj_in_child = true;

        /* Find the "top" file in the chain of mounted files */
        top_obj_file = H5F_PARENT(oloc->file);
        while (H5F_PARENT(top_obj_file) != NULL) {
            /* Check if object is in child mount hier. (for mount & unmount operations) */
            if (names->dst_file && H5F_SAME_SHARED(top_obj_file, names->dst_file))
                obj_in_child = true;

            top_obj_file = H5F_PARENT(top_obj_file);
        } /* end while */
    }     /* end if */
    else
        top_obj_file = oloc->file;

    /* Check if object is in top of child mount hier. (for mount & unmount operations) */
    if (names->dst_file && H5F_SAME_SHARED(top_obj_file, names->dst_file))
        obj_in_child = true;

    /* Check if the object is in same file mount hier. */
    if (!H5F_SAME_SHARED(top_obj_file, names->src_file))
        HGOTO_DONE(SUCCEED); /* No need to look at object, it's path is already invalid */

    switch (names->op) {
        /*-------------------------------------------------------------------------
         * H5G_NAME_MOUNT
         *-------------------------------------------------------------------------
         */
        case H5G_NAME_MOUNT:
            /* Check if object is in child mount hier. */
            if (obj_in_child) {
                const char *full_path; /* Full path of current object */
                const char *src_path;  /* Full path of source object */
                H5RS_str_t *rs;        /* Ref-counted string for new path */

                /* Get pointers to paths of interest */
                full_path = H5RS_get_str(obj_path->full_path_r);
                src_path  = H5RS_get_str(names->src_full_path_r);

                /* Create new full path */
                if (NULL == (rs = H5RS_create(src_path)))
                    HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, FAIL, "can't create ref-counted string");
                H5RS_acat(rs, full_path);

                /* Release previous full path */
                H5RS_decr(obj_path->full_path_r);

                /* Take ownership of the new full path */
                obj_path->full_path_r = rs;
            } /* end if */
            /* Object must be in parent mount file hier. */
            else {
                /* Check if the source is along the entry's path */
                /* (But not actually the entry itself) */
                if (H5G__common_path(obj_path->full_path_r, names->src_full_path_r) &&
                    H5RS_cmp(obj_path->full_path_r, names->src_full_path_r)) {
                    /* Hide the user path */
                    (obj_path->obj_hidden)++;
                } /* end if */
            }     /* end else */
            break;

        /*-------------------------------------------------------------------------
         * H5G_NAME_UNMOUNT
         *-------------------------------------------------------------------------
         */
        case H5G_NAME_UNMOUNT:
            if (obj_in_child) {
                const char *full_path;   /* Full path of current object */
                const char *full_suffix; /* Full path after source path */
                const char *src_path;    /* Full path of source object */
                H5RS_str_t *rs;          /* Ref-counted string for new path */

                /* Get pointers to paths of interest */
                full_path = H5RS_get_str(obj_path->full_path_r);
                src_path  = H5RS_get_str(names->src_full_path_r);

                /* Construct full path suffix */
                full_suffix = full_path + strlen(src_path);

                /* Create new full path suffix */
                if (NULL == (rs = H5RS_create(full_suffix)))
                    HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, FAIL, "can't create ref-counted string");

                /* Release previous full path */
                H5RS_decr(obj_path->full_path_r);

                /* Take ownership of the new full path */
                obj_path->full_path_r = rs;

                /* Check if the object's user path should be invalidated */
                if (obj_path->user_path_r && H5RS_len(rs) < H5RS_len(obj_path->user_path_r)) {
                    /* Free user path */
                    H5RS_decr(obj_path->user_path_r);
                    obj_path->user_path_r = NULL;
                } /* end if */
            }     /* end if */
            else {
                /* Check if file being unmounted was hiding the object */
                if (H5G__common_path(obj_path->full_path_r, names->src_full_path_r) &&
                    H5RS_cmp(obj_path->full_path_r, names->src_full_path_r)) {
                    /* Un-hide the user path */
                    (obj_path->obj_hidden)--;
                } /* end if */
            }     /* end else */
            break;

        /*-------------------------------------------------------------------------
         * H5G_NAME_DELETE
         *-------------------------------------------------------------------------
         */
        case H5G_NAME_DELETE:
            /* Check if the location being unlinked is in the path for the current object */
            if (H5G__common_path(obj_path->full_path_r, names->src_full_path_r)) {
                /* Free paths for object */
                H5G_name_free(obj_path);
            } /* end if */
            break;

        /*-------------------------------------------------------------------------
         * H5G_NAME_MOVE
         *-------------------------------------------------------------------------
         */
        case H5G_NAME_MOVE: /* Link move case, check for relative names case */
            /* Check if the src object moved is in the current object's path */
            if (H5G__common_path(obj_path->full_path_r, names->src_full_path_r)) {
                const char *full_path;   /* Full path of current object */
                const char *full_suffix; /* Suffix of full path, after src_path */
                const char *src_path;    /* Full path of source object */
                const char *dst_path;    /* Full path of destination object */
                H5RS_str_t *rs;          /* Ref-counted string for new path */

                /* Sanity check */
                assert(names->dst_full_path_r);

                /* Get pointers to paths of interest */
                full_path = H5RS_get_str(obj_path->full_path_r);
                src_path  = H5RS_get_str(names->src_full_path_r);
                dst_path  = H5RS_get_str(names->dst_full_path_r);

                /* Make certain that the source and destination names are full (not relative) paths */
                assert(*src_path == '/');
                assert(*dst_path == '/');

                /* Get pointer to "full suffix" */
                full_suffix = full_path + strlen(src_path);

                /* Update the user path, if one exists */
                if (obj_path->user_path_r)
                    if (H5G__name_move_path(&(obj_path->user_path_r), full_suffix, src_path, dst_path) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_PATH, FAIL, "can't build user path name");

                /* Create new full path */
                if (NULL == (rs = H5RS_create(dst_path)))
                    HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, FAIL, "can't create ref-counted string");
                H5RS_acat(rs, full_suffix);

                /* Release previous full path */
                H5RS_decr(obj_path->full_path_r);

                /* Take ownership of the new full path */
                obj_path->full_path_r = rs;
            } /* end if */
            break;

        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__name_replace_cb() */

/*-------------------------------------------------------------------------
 * Function: H5G_name_replace
 *
 * Purpose: Search the list of open IDs and replace names according to a
 *              particular operation.  The operation occurred on the
 *              SRC_FILE/SRC_FULL_PATH_R object.  The new name (if there is
 *              one) is NEW_NAME_R.  Additional entry location information
 *              (currently only needed for the 'move' operation) is passed in
 *              DST_FILE/DST_FULL_PATH_R.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_name_replace(const H5O_link_t *lnk, H5G_names_op_t op, H5F_t *src_file, H5RS_str_t *src_full_path_r,
                 H5F_t *dst_file, H5RS_str_t *dst_full_path_r)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(src_file);

    /* Check if the object we are manipulating has a path */
    if (src_full_path_r) {
        bool search_group    = false; /* Flag to indicate that groups are to be searched */
        bool search_dataset  = false; /* Flag to indicate that datasets are to be searched */
        bool search_datatype = false; /* Flag to indicate that datatypes are to be searched */

        /* Check for particular link to operate on */
        if (lnk) {
            /* Look up the object type for each type of link */
            switch (lnk->type) {
                case H5L_TYPE_HARD: {
                    H5O_loc_t  tmp_oloc; /* Temporary object location */
                    H5O_type_t obj_type; /* Type of object at location */

                    /* Build temporary object location */
                    tmp_oloc.file = src_file;
                    tmp_oloc.addr = lnk->u.hard.addr;

                    /* Get the type of the object */
                    if (H5O_obj_type(&tmp_oloc, &obj_type) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get object type");

                    /* Determine which type of objects to operate on */
                    switch (obj_type) {
                        case H5O_TYPE_GROUP:
                            /* Search and replace names through group IDs */
                            search_group = true;
                            break;

                        case H5O_TYPE_DATASET:
                            /* Search and replace names through dataset IDs */
                            search_dataset = true;
                            break;

                        case H5O_TYPE_NAMED_DATATYPE:
                            /* Search and replace names through datatype IDs */
                            search_datatype = true;
                            break;

                        case H5O_TYPE_MAP:
                            HGOTO_ERROR(H5E_SYM, H5E_BADTYPE, FAIL,
                                        "maps not supported in native VOL connector");

                        case H5O_TYPE_UNKNOWN:
                        case H5O_TYPE_NTYPES:
                            /* Search and replace names through datatype IDs */
                        default:
                            HGOTO_ERROR(H5E_SYM, H5E_BADTYPE, FAIL, "not valid object type");
                    } /* end switch */
                }     /* end case */
                break;

                case H5L_TYPE_SOFT:
                    /* Symbolic links might resolve to any object, so we need to search all IDs */
                    search_group = search_dataset = search_datatype = true;
                    break;

                case H5L_TYPE_ERROR:
                case H5L_TYPE_EXTERNAL:
                case H5L_TYPE_MAX:
                default: /* User-defined link */
                    /* Check for unknown library-defined link type */
                    if (lnk->type < H5L_TYPE_UD_MIN)
                        HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "unknown link type");

                    /* User-defined & external links automatically wipe out
                     * names (because it would be too much work to track them),
                     * so there's no point in searching them.
                     */
                    break;
            } /* end switch */
        }     /* end if */
        else {
            /* We pass NULL as link pointer when we need to search all IDs */
            search_group = search_dataset = search_datatype = true;
        }

        /* Check if we need to operate on the objects affected */
        if (search_group || search_dataset || search_datatype) {
            H5G_names_t names; /* Structure to hold operation information for callback */

            /* Find top file in src location's mount hierarchy */
            while (H5F_PARENT(src_file))
                src_file = H5F_PARENT(src_file);

            /* Set up common information for callback */
            names.src_file        = src_file;
            names.src_full_path_r = src_full_path_r;
            names.dst_file        = dst_file;
            names.dst_full_path_r = dst_full_path_r;
            names.op              = op;

            /* Search through group IDs */
            if (search_group)
                if (H5I_iterate(H5I_GROUP, H5G__name_replace_cb, &names, false) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't iterate over groups");

            /* Search through dataset IDs */
            if (search_dataset)
                if (H5I_iterate(H5I_DATASET, H5G__name_replace_cb, &names, false) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't iterate over datasets");

            /* Search through datatype IDs */
            if (search_datatype)
                if (H5I_iterate(H5I_DATATYPE, H5G__name_replace_cb, &names, false) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "can't iterate over datatypes");
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_name_replace() */

/*-------------------------------------------------------------------------
 * Function:    H5G__get_name_by_addr_cb
 *
 * Purpose:     Callback for retrieving object's name by address
 *
 * Return:      Positive if path is for object desired
 * 	            0 if not correct object
 * 	            negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__get_name_by_addr_cb(hid_t gid, const char *path, const H5L_info2_t *linfo, void *_udata)
{
    H5G_gnba_iter_t *udata = (H5G_gnba_iter_t *)_udata; /* User data for iteration */
    H5G_loc_t        obj_loc;                           /* Location of object */
    H5G_name_t       obj_path;                          /* Object's group hier. path */
    H5O_loc_t        obj_oloc;                          /* Object's object location */
    bool             obj_found = false;                 /* Object at 'path' found */
    herr_t           ret_value = H5_ITER_CONT;          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(path);
    assert(linfo);
    assert(udata->loc);
    assert(udata->path == NULL);

    /* Check for hard link with correct address */
    if (linfo->type == H5L_TYPE_HARD) {
        haddr_t link_addr;

        /* Retrieve hard link address from VOL token */
        if (H5VL_native_token_to_addr(udata->loc->file, H5I_FILE, linfo->u.token, &link_addr) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTUNSERIALIZE, FAIL, "can't deserialize object token into address");

        if (udata->loc->addr == link_addr) {
            H5G_loc_t grp_loc; /* Location of group */

            /* Get group's location */
            if (H5G_loc(gid, &grp_loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5_ITER_ERROR, "bad group location");

            /* Set up opened object location to fill in */
            obj_loc.oloc = &obj_oloc;
            obj_loc.path = &obj_path;
            H5G_loc_reset(&obj_loc);

            /* Find the object */
            if (H5G_loc_find(&grp_loc, path, &obj_loc /*out*/) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, H5_ITER_ERROR, "object not found");
            obj_found = true;

            /* Check for object in same file (handles mounted files) */
            /* (re-verify address, in case we traversed a file mount) */
            if (udata->loc->addr == obj_loc.oloc->addr && udata->loc->file == obj_loc.oloc->file) {
                if (NULL == (udata->path = H5MM_strdup(path)))
                    HGOTO_ERROR(H5E_SYM, H5E_CANTALLOC, H5_ITER_ERROR, "can't duplicate path string");

                /* We found a match so we return immediately */
                HGOTO_DONE(H5_ITER_STOP);
            } /* end if */
        }     /* end if */
    }         /* end if */

done:
    if (obj_found && H5G_loc_free(&obj_loc) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRELEASE, H5_ITER_ERROR, "can't free location");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__get_name_by_addr_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5G_get_name_by_addr
 *
 * Purpose:     Tries to figure out the path to an object from it's address
 *
 * Return:      Success:    Returns size of path name, and copies it into buffer
 *                          pointed to by name if that buffer is big enough.
 *                          0 if it cannot find the path
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_get_name_by_addr(H5F_t *f, const H5O_loc_t *loc, char *name, size_t size, size_t *name_len)
{
    H5G_gnba_iter_t udata;               /* User data for iteration  */
    size_t          len;                 /* Length of path name */
    H5G_loc_t       root_loc;            /* Root group's location    */
    bool            found_obj = false;   /* If we found the object   */
    herr_t          status;              /* Status from iteration    */
    herr_t          ret_value = SUCCEED; /* Return value             */

    /* Portably clear udata struct (before FUNC_ENTER) */
    memset(&udata, 0, sizeof(udata));

    FUNC_ENTER_NOAPI(FAIL)

    /* Construct a group location for root group of the file */
    if (H5G_root_loc(f, &root_loc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get root group's location");

    /* Check for root group being the object looked for */
    if (root_loc.oloc->addr == loc->addr && root_loc.oloc->file == loc->file) {
        if (NULL == (udata.path = H5MM_strdup("")))
            HGOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "can't duplicate path string");
        found_obj = true;
    } /* end if */
    else {
        /* Set up user data for iterator */
        udata.loc  = loc;
        udata.path = NULL;

        /* Visit all the links in the file */
        if ((status = H5G_visit(&root_loc, "/", H5_INDEX_NAME, H5_ITER_NATIVE, H5G__get_name_by_addr_cb,
                                &udata)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "group traversal failed while looking for object name");
        else if (status > 0)
            found_obj = true;
    } /* end else */

    /* Check for finding the object */
    if (found_obj) {
        /* Set the length of the full path */
        len = strlen(udata.path) + 1; /* Length of path + 1 (for "/") */

        /* If there's a buffer provided, copy into it, up to the limit of its size */
        if (name) {
            /* Copy the initial path separator */
            strncpy(name, "/", (size_t)2);

            /* Append the rest of the path */
            /* (less one character, for the initial path separator) */
            strncat(name, udata.path, (size - 2));
            if (len >= size)
                name[size - 1] = '\0';
        } /* end if */
    }     /* end if */
    else
        len = 0;

    /* Set path name length, if given */
    if (name_len)
        *name_len = len;

done:
    /* Release resources */
    H5MM_xfree(udata.path);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_get_name_by_addr() */
