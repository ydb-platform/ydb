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
 * Open object info algorithms.
 *
 * These are used to track the objects currently open in a file, for various
 * internal mechanisms which need to be aware of such things.
 *
 */

#define H5F_FRIEND /*suppress error about including H5Fpkg	  */

#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fpkg.h"      /* File access                          */
#include "H5FLprivate.h" /* Free lists                           */
#include "H5FOprivate.h" /* File objects                         */
#include "H5Oprivate.h"  /* Object headers		  	*/

/* Private typedefs */

/* Information about open objects in a file */
typedef struct H5FO_open_obj_t {
    haddr_t addr;    /* Address of object header for object */
    void   *obj;     /* Pointer to the object            */
    bool    deleted; /* Flag to indicate that the object was deleted from the file */
} H5FO_open_obj_t;

/* Information about counted objects in a file */
typedef struct H5FO_obj_count_t {
    haddr_t addr;  /* Address of object header for object */
    hsize_t count; /* Number of times object is opened */
} H5FO_obj_count_t;

/* Declare a free list to manage the H5FO_open_obj_t struct */
H5FL_DEFINE_STATIC(H5FO_open_obj_t);

/* Declare a free list to manage the H5FO_obj_count_t struct */
H5FL_DEFINE_STATIC(H5FO_obj_count_t);

/*--------------------------------------------------------------------------
 NAME
    H5FO_create
 PURPOSE
    Create an open object info set
 USAGE
    herr_t H5FO_create(f)
        H5F_t *f;       IN/OUT: File to create opened object info set for

 RETURNS
    Returns non-negative on success, negative on failure
 DESCRIPTION
    Create a new open object info set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5FO_create(const H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);
    assert(f->shared);

    /* Create container used to store open object info */
    if ((f->shared->open_objs = H5SL_create(H5SL_TYPE_HADDR, NULL)) == NULL)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to create open object container");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_create() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_opened
 PURPOSE
    Checks if an object at an address is already open in the file.
 USAGE
    void * H5FO_opened(f,addr)
        const H5F_t *f;         IN: File to check opened object info set
        haddr_t addr;           IN: Address of object to check

 RETURNS
    Returns a pointer to the object on success, NULL on failure
 DESCRIPTION
    Check is an object at an address (the address of the object's object header)
    is already open in the file and return the ID for that object if it is open.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
void *
H5FO_opened(const H5F_t *f, haddr_t addr)
{
    H5FO_open_obj_t *open_obj;  /* Information about open object */
    void            *ret_value; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    assert(f);
    assert(f->shared);
    assert(f->shared->open_objs);
    assert(H5_addr_defined(addr));

    /* Get the object node from the container */
    if (NULL != (open_obj = (H5FO_open_obj_t *)H5SL_search(f->shared->open_objs, &addr))) {
        ret_value = open_obj->obj;
        assert(ret_value != NULL);
    } /* end if */
    else
        ret_value = NULL;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_opened() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_insert
 PURPOSE
    Insert a newly opened object/pointer pair into the opened object info set
 USAGE
    herr_t H5FO_insert(f,addr,obj)
        H5F_t *f;               IN/OUT: File's opened object info set
        haddr_t addr;           IN: Address of object to insert
        void *obj;              IN: Pointer to object to insert
        bool delete_flag;    IN: Whether to 'mark' this object for deletion

 RETURNS
    Returns a non-negative on success, negative on failure
 DESCRIPTION
    Insert an object/ID pair into the opened object info set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5FO_insert(const H5F_t *f, haddr_t addr, void *obj, bool delete_flag)
{
    H5FO_open_obj_t *open_obj;            /* Information about open object */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);
    assert(f->shared);
    assert(f->shared->open_objs);
    assert(H5_addr_defined(addr));
    assert(obj);

    /* Allocate new opened object information structure */
    if ((open_obj = H5FL_MALLOC(H5FO_open_obj_t)) == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Assign information */
    open_obj->addr    = addr;
    open_obj->obj     = obj;
    open_obj->deleted = delete_flag;

    /* Insert into container */
    if (H5SL_insert(f->shared->open_objs, &open_obj->addr, open_obj) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "can't insert object into container");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_insert() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_delete
 PURPOSE
    Remove an opened object/ID pair from the opened object info set
 USAGE
    herr_t H5FO_delete(f,addr)
        H5F_t *f;               IN/OUT: File's opened object info set
        haddr_t addr;           IN: Address of object to remove

 RETURNS
    Returns a non-negative on success, negative on failure
 DESCRIPTION
    Remove an object/ID pair from the opened object info.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5FO_delete(H5F_t *f, haddr_t addr)
{
    H5FO_open_obj_t *open_obj;            /* Information about open object */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);
    assert(f->shared);
    assert(f->shared->open_objs);
    assert(H5_addr_defined(addr));

    /* Remove from container */
    if (NULL == (open_obj = (H5FO_open_obj_t *)H5SL_remove(f->shared->open_objs, &addr)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTRELEASE, FAIL, "can't remove object from container");

    /* Check if the object was deleted from the file */
    if (open_obj->deleted) {
        if (H5O_delete(f, addr) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTDELETE, FAIL, "can't delete object from file");
    } /* end if */

    /* Release the object information */
    open_obj = H5FL_FREE(H5FO_open_obj_t, open_obj);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_delete() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_mark
 PURPOSE
    Mark an object to be deleted when it is closed
 USAGE
    herr_t H5FO_mark(f,addr)
        const H5F_t *f;         IN: File opened object is in
        haddr_t addr;           IN: Address of object to delete

 RETURNS
    Returns a non-negative ID for the object on success, negative on failure
 DESCRIPTION
    Mark an opened object for deletion from the file when it is closed.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5FO_mark(const H5F_t *f, haddr_t addr, bool deleted)
{
    H5FO_open_obj_t *open_obj;            /* Information about open object */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    assert(f);
    assert(f->shared);
    assert(f->shared->open_objs);
    assert(H5_addr_defined(addr));

    /* Get the object node from the container */
    if (NULL != (open_obj = (H5FO_open_obj_t *)H5SL_search(f->shared->open_objs, &addr)))
        open_obj->deleted = deleted;
    else
        ret_value = FAIL;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_mark() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_marked
 PURPOSE
    Check if an object is marked to be deleted when it is closed
 USAGE
    bool H5FO_marked(f,addr)
        const H5F_t *f;         IN: File opened object is in
        haddr_t addr;           IN: Address of object to delete

 RETURNS
    Returns a true/false on success
 DESCRIPTION
    Checks if the object is currently in the "opened objects" tree and
    whether its marks for deletion from the file when it is closed.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
bool
H5FO_marked(const H5F_t *f, haddr_t addr)
{
    H5FO_open_obj_t *open_obj;          /* Information about open object */
    bool             ret_value = false; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    assert(f);
    assert(f->shared);
    assert(f->shared->open_objs);
    assert(H5_addr_defined(addr));

    /* Get the object node from the container */
    if (NULL != (open_obj = (H5FO_open_obj_t *)H5SL_search(f->shared->open_objs, &addr)))
        ret_value = open_obj->deleted;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_marked() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_dest
 PURPOSE
    Destroy an open object info set
 USAGE
    herr_t H5FO_dest(f)
        H5F_t *f;               IN/OUT: File's opened object info set

 RETURNS
    Returns a non-negative on success, negative on failure
 DESCRIPTION
    Destroy an existing open object info set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5FO_dest(const H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);
    assert(f->shared);
    assert(f->shared->open_objs);

    /* Check if the object info set is empty */
    if (H5SL_count(f->shared->open_objs) != 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTRELEASE, FAIL, "objects still in open object info set");

    /* Release the open object info set container */
    if (H5SL_close(f->shared->open_objs) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTCLOSEOBJ, FAIL, "can't close open object info set");

    f->shared->open_objs = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_dest() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_top_create
 PURPOSE
    Create the "top" open object count set
 USAGE
    herr_t H5FO_create(f)
        H5F_t *f;       IN/OUT: File to create opened object count set for

 RETURNS
    Returns non-negative on success, negative on failure
 DESCRIPTION
    Create a new open object count set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5FO_top_create(H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);

    /* Create container used to store open object info */
    if ((f->obj_count = H5SL_create(H5SL_TYPE_HADDR, NULL)) == NULL)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to create open object container");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_top_create() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_top_incr
 PURPOSE
    Increment the "top" reference count for an object in a file
 USAGE
    herr_t H5FO_top_incr(f, addr)
        H5F_t *f;               IN/OUT: File's opened object info set
        haddr_t addr;           IN: Address of object to increment

 RETURNS
    Returns a non-negative on success, negative on failure
 DESCRIPTION
    Increment the reference count for an object in the opened object count set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5FO_top_incr(const H5F_t *f, haddr_t addr)
{
    H5FO_obj_count_t *obj_count;           /* Ref. count for object */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);
    assert(f->obj_count);
    assert(H5_addr_defined(addr));

    /* Get the object node from the container */
    if (NULL != (obj_count = (H5FO_obj_count_t *)H5SL_search(f->obj_count, &addr))) {
        (obj_count->count)++;
    } /* end if */
    else {
        /* Allocate new opened object information structure */
        if (NULL == (obj_count = H5FL_MALLOC(H5FO_obj_count_t)))
            HGOTO_ERROR(H5E_CACHE, H5E_NOSPACE, FAIL, "memory allocation failed");

        /* Assign information */
        obj_count->addr  = addr;
        obj_count->count = 1;

        /* Insert into container */
        if (H5SL_insert(f->obj_count, &obj_count->addr, obj_count) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "can't insert object into container");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_top_incr() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_top_decr
 PURPOSE
    Decrement the "top" reference count for an object in a file
 USAGE
    herr_t H5FO_top_decr(f, addr)
        H5F_t *f;               IN/OUT: File's opened object info set
        haddr_t addr;           IN: Address of object to decrement

 RETURNS
    Returns a non-negative on success, negative on failure
 DESCRIPTION
    Decrement the reference count for an object in the opened object count set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5FO_top_decr(const H5F_t *f, haddr_t addr)
{
    H5FO_obj_count_t *obj_count;           /* Ref. count for object */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);
    assert(f->obj_count);
    assert(H5_addr_defined(addr));

    /* Get the object node from the container */
    if (NULL != (obj_count = (H5FO_obj_count_t *)H5SL_search(f->obj_count, &addr))) {
        /* Decrement the reference count for the object */
        (obj_count->count)--;

        if (obj_count->count == 0) {
            /* Remove from container */
            if (NULL == (obj_count = (H5FO_obj_count_t *)H5SL_remove(f->obj_count, &addr)))
                HGOTO_ERROR(H5E_CACHE, H5E_CANTRELEASE, FAIL, "can't remove object from container");

            /* Release the object information */
            obj_count = H5FL_FREE(H5FO_obj_count_t, obj_count);
        } /* end if */
    }     /* end if */
    else
        HGOTO_ERROR(H5E_CACHE, H5E_NOTFOUND, FAIL, "can't decrement ref. count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_top_decr() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_top_count
 PURPOSE
    Return the "top" reference count for an object in a file
 USAGE
    hsize_t H5FO_top_incr(f, addr)
        H5F_t *f;               IN/OUT: File's opened object info set
        haddr_t addr;           IN: Address of object to increment

 RETURNS
    Returns a non-negative on success, negative on failure
 DESCRIPTION
    Retrieves the reference count for an object in the opened object count set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hsize_t
H5FO_top_count(const H5F_t *f, haddr_t addr)
{
    H5FO_obj_count_t *obj_count; /* Ref. count for object */
    hsize_t           ret_value; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(f);
    assert(f->obj_count);
    assert(H5_addr_defined(addr));

    /* Get the object node from the container */
    if (NULL != (obj_count = (H5FO_obj_count_t *)H5SL_search(f->obj_count, &addr)))
        ret_value = obj_count->count;
    else
        ret_value = 0;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_top_count() */

/*--------------------------------------------------------------------------
 NAME
    H5FO_top_dest
 PURPOSE
    Destroy an open object info set
 USAGE
    herr_t H5FO_top_dest(f)
        H5F_t *f;               IN/OUT: File's opened object info set

 RETURNS
    Returns a non-negative on success, negative on failure
 DESCRIPTION
    Destroy an existing open object info set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5FO_top_dest(H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);
    assert(f->obj_count);

    /* Check if the object count set is empty */
    if (H5SL_count(f->obj_count) != 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTRELEASE, FAIL, "objects still in open object info set");

    /* Release the open object count set container */
    if (H5SL_close(f->obj_count) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTCLOSEOBJ, FAIL, "can't close open object info set");

    f->obj_count = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FO_top_dest() */
