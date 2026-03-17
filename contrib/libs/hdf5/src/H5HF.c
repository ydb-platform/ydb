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
 * Created:		H5HF.c
 *
 * Purpose:		Implements a "fractal heap" for storing variable-
 *                      length objects in a file.
 *
 *                      Please see the documentation in:
 *                      doc/html/TechNotes/FractalHeap.html for a full description
 *                      of how they work, etc.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5HFmodule.h" /* This source code file is part of the H5HF module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FOprivate.h" /* File objects                         */
#include "H5HFpkg.h"     /* Fractal heaps			*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/

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

/* Declare a free list to manage the H5HF_t struct */
H5FL_DEFINE_STATIC(H5HF_t);

/*-------------------------------------------------------------------------
 * Function:	H5HF__op_read
 *
 * Purpose:	Performs a 'read' operation for a heap 'op' callback
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__op_read(const void *obj, size_t obj_len, void *op_data)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Perform "read", using memcpy() */
    H5MM_memcpy(op_data, obj, obj_len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__op_read() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__op_write
 *
 * Purpose:	Performs a 'write' operation for a heap 'op' callback
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__op_write(const void *obj, size_t obj_len, void *op_data)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Perform "write", using memcpy()
     *
     * We cast away const here because no obj pointer that was originally
     * const should ever arrive here.
     */
    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    H5MM_memcpy((void *)obj, op_data, obj_len);
    H5_GCC_CLANG_DIAG_ON("cast-qual")

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__op_write() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_create
 *
 * Purpose:	Creates a new empty fractal heap in the file.
 *
 * Return:	Pointer to heap wrapper on success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5HF_t *
H5HF_create(H5F_t *f, const H5HF_create_t *cparam)
{
    H5HF_t     *fh  = NULL;       /* Pointer to new fractal heap */
    H5HF_hdr_t *hdr = NULL;       /* The fractal heap header information */
    haddr_t     fh_addr;          /* Heap header address */
    H5HF_t     *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(cparam);

    /* Create shared fractal heap header */
    if (HADDR_UNDEF == (fh_addr = H5HF__hdr_create(f, cparam)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, NULL, "can't create fractal heap header");

    /* Allocate fractal heap wrapper */
    if (NULL == (fh = H5FL_MALLOC(H5HF_t)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, NULL, "memory allocation failed for fractal heap info");

    /* Lock the heap header into memory */
    if (NULL == (hdr = H5HF__hdr_protect(f, fh_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, NULL, "unable to protect fractal heap header");

    /* Point fractal heap wrapper at header and bump it's ref count */
    fh->hdr = hdr;
    if (H5HF__hdr_incr(fh->hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL, "can't increment reference count on shared heap header");

    /* Increment # of files using this heap header */
    if (H5HF__hdr_fuse_incr(fh->hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL,
                    "can't increment file reference count on shared heap header");

    /* Set file pointer for this heap open context */
    fh->f = f;

    /* Set the return value */
    ret_value = fh;

done:
    if (hdr && H5AC_unprotect(f, H5AC_FHEAP_HDR, fh_addr, hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, NULL, "unable to release fractal heap header");
    if (!ret_value && fh)
        if (H5HF_close(fh) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTCLOSEOBJ, NULL, "unable to close fractal heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_create() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_open
 *
 * Purpose:	Opens an existing fractal heap in the file.
 *
 * Return:	Pointer to heap wrapper on success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5HF_t *
H5HF_open(H5F_t *f, haddr_t fh_addr)
{
    H5HF_t     *fh        = NULL; /* Pointer to new fractal heap */
    H5HF_hdr_t *hdr       = NULL; /* The fractal heap header information */
    H5HF_t     *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(fh_addr));

    /* Load the heap header into memory */
    if (NULL == (hdr = H5HF__hdr_protect(f, fh_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, NULL, "unable to protect fractal heap header");

    /* Check for pending heap deletion */
    if (hdr->pending_delete)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTOPENOBJ, NULL, "can't open fractal heap pending deletion");

    /* Create fractal heap info */
    if (NULL == (fh = H5FL_MALLOC(H5HF_t)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, NULL, "memory allocation failed for fractal heap info");

    /* Point fractal heap wrapper at header */
    fh->hdr = hdr;
    if (H5HF__hdr_incr(fh->hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL, "can't increment reference count on shared heap header");

    /* Increment # of files using this heap header */
    if (H5HF__hdr_fuse_incr(fh->hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL,
                    "can't increment file reference count on shared heap header");

    /* Set file pointer for this heap open context */
    fh->f = f;

    /* Set the return value */
    ret_value = fh;

done:
    if (hdr && H5AC_unprotect(f, H5AC_FHEAP_HDR, fh_addr, hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, NULL, "unable to release fractal heap header");
    if (!ret_value && fh)
        if (H5HF_close(fh) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTCLOSEOBJ, NULL, "unable to close fractal heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_open() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_id_len
 *
 * Purpose:	Get the size of IDs for entries in a fractal heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_get_id_len(H5HF_t *fh, size_t *id_len_p)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /*
     * Check arguments.
     */
    assert(fh);
    assert(id_len_p);

    /* Retrieve the ID length for entries in this heap */
    *id_len_p = fh->hdr->id_len;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF_get_id_len() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_heap_addr
 *
 * Purpose:	Get the address of a fractal heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_get_heap_addr(const H5HF_t *fh, haddr_t *heap_addr_p)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /*
     * Check arguments.
     */
    assert(fh);
    assert(heap_addr_p);

    /* Retrieve the heap header address for this heap */
    *heap_addr_p = fh->hdr->heap_addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF_get_heap_addr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_insert
 *
 * Purpose:	Insert a new object into a fractal heap.
 *
 * Return:	Non-negative on success (with heap ID of new object
 *              filled in), negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_insert(H5HF_t *fh, size_t size, const void *obj, void *id /*out*/)
{
    H5HF_hdr_t *hdr       = NULL; /* The fractal heap header information */
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(fh);
    assert(obj);
    assert(id);

    /* Check arguments */
    if (size == 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "can't insert 0-sized objects");

    /* Set the shared heap header's file context for this operation */
    fh->hdr->f = fh->f;

    /* Get the fractal heap header */
    hdr = fh->hdr;

    /* Check for 'huge' object */
    if (size > hdr->max_man_size) {
        /* Store 'huge' object in heap
         *
         * Although not ideal, we can quiet the const warning here because no
         * obj pointer that was originally const should ever arrive here.
         */
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        if (H5HF__huge_insert(hdr, size, (void *)obj, id) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINSERT, FAIL, "can't store 'huge' object in fractal heap");
        H5_GCC_CLANG_DIAG_ON("cast-qual")
    } /* end if */
    /* Check for 'tiny' object */
    else if (size <= hdr->tiny_max_len) {
        /* Store 'tiny' object in heap */
        if (H5HF__tiny_insert(hdr, size, obj, id) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINSERT, FAIL, "can't store 'tiny' object in fractal heap");
    } /* end if */
    else {
        /* Check if we are in "append only" mode, or if there's enough room for the object */
        if (hdr->write_once) {
            HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "'write once' managed blocks not supported yet");
        } /* end if */
        else {
            /* Allocate space for object in 'managed' heap */
            if (H5HF__man_insert(hdr, size, obj, id) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTINSERT, FAIL, "can't store 'managed' object in fractal heap");
        } /* end else */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_insert() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_obj_len
 *
 * Purpose:	Get the size of an entry in a fractal heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_get_obj_len(H5HF_t *fh, const void *_id, size_t *obj_len_p)
{
    const uint8_t *id = (const uint8_t *)_id; /* Object ID */
    uint8_t        id_flags;                  /* Heap ID flag bits */
    herr_t         ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(fh);
    assert(id);
    assert(obj_len_p);

    /* Get the ID flags */
    id_flags = *id;

    /* Check for correct heap ID version */
    if ((id_flags & H5HF_ID_VERS_MASK) != H5HF_ID_VERS_CURR)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, FAIL, "incorrect heap ID version");

    /* Set the shared heap header's file context for this operation */
    fh->hdr->f = fh->f;

    /* Check type of object in heap */
    if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_MAN) {
        if (H5HF__man_get_obj_len(fh->hdr, id, obj_len_p) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get 'managed' object's length");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_HUGE) {
        if (H5HF__huge_get_obj_len(fh->hdr, id, obj_len_p) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get 'huge' object's length");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_TINY) {
        if (H5HF__tiny_get_obj_len(fh->hdr, id, obj_len_p) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get 'tiny' object's length");
    } /* end if */
    else {
        fprintf(stderr, "%s: Heap ID type not supported yet!\n", __func__);
        HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "heap ID type not supported yet");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_get_obj_len() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_obj_off
 *
 * Purpose:	Get the offset of an entry in a fractal heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_get_obj_off(H5HF_t *fh, const void *_id, hsize_t *obj_off_p)
{
    const uint8_t *id = (const uint8_t *)_id; /* Object ID */
    uint8_t        id_flags;                  /* Heap ID flag bits */
    herr_t         ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(fh);
    assert(id);
    assert(obj_off_p);

    /* Get the ID flags */
    id_flags = *id;

    /* Check for correct heap ID version */
    if ((id_flags & H5HF_ID_VERS_MASK) != H5HF_ID_VERS_CURR)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, FAIL, "incorrect heap ID version");

    /* Set the shared heap header's file context for this operation */
    fh->hdr->f = fh->f;

    /* Check type of object in heap */
    if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_MAN) {
        H5HF__man_get_obj_off(fh->hdr, id, obj_off_p);
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_HUGE) {
        /* Huge objects are located directly in the file */
        if (H5HF__huge_get_obj_off(fh->hdr, id, obj_off_p) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get 'huge' object's offset");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_TINY) {
        /* Tiny objects are not stored in the heap */
        *obj_off_p = (hsize_t)0;
    } /* end if */
    else {
        fprintf(stderr, "%s: Heap ID type not supported yet!\n", __func__);
        HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "heap ID type not supported yet");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_get_obj_off() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_read
 *
 * Purpose:	Read an object from a fractal heap into a buffer
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_read(H5HF_t *fh, const void *_id, void *obj /*out*/)
{
    const uint8_t *id = (const uint8_t *)_id; /* Object ID */
    uint8_t        id_flags;                  /* Heap ID flag bits */
    herr_t         ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(fh);
    assert(id);
    assert(obj);

    /* Get the ID flags */
    id_flags = *id;

    /* Check for correct heap ID version */
    if ((id_flags & H5HF_ID_VERS_MASK) != H5HF_ID_VERS_CURR)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, FAIL, "incorrect heap ID version");

    /* Set the shared heap header's file context for this operation */
    fh->hdr->f = fh->f;

    /* Check type of object in heap */
    if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_MAN) {
        /* Read object from managed heap blocks */
        if (H5HF__man_read(fh->hdr, id, obj) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't read object from fractal heap");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_HUGE) {
        /* Read 'huge' object from file */
        if (H5HF__huge_read(fh->hdr, id, obj) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't read 'huge' object from fractal heap");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_TINY) {
        /* Read 'tiny' object from file */
        if (H5HF__tiny_read(fh->hdr, id, obj) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't read 'tiny' object from fractal heap");
    } /* end if */
    else {
        fprintf(stderr, "%s: Heap ID type not supported yet!\n", __func__);
        HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "heap ID type not supported yet");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_read() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_write
 *
 * Purpose:	Write an object from a buffer into a fractal heap
 *
 * Notes:	Writing objects in "managed" heap blocks is only storage
 *		method currently supported.  (Which could be expanded to
 *		'huge' and 'tiny' objects, with some work)
 *
 *		Also, assumes that the 'op' routine modifies the data, and
 *		marks data to be written back to disk, even if 'op' routine
 *		didn't actually change anything.  (Which could be modified
 *		to pass "did_modify" flag to callback, if necessary)
 *
 *		Also, assumes that object to write is same size as object in
 *		heap.
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_write(H5HF_t *fh, void *_id, bool H5_ATTR_UNUSED *id_changed, const void *obj)
{
    uint8_t *id = (uint8_t *)_id; /* Object ID */
    uint8_t  id_flags;            /* Heap ID flag bits */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(fh);
    assert(id);
    assert(obj);

    /* Get the ID flags */
    id_flags = *id;

    /* Check for correct heap ID version */
    if ((id_flags & H5HF_ID_VERS_MASK) != H5HF_ID_VERS_CURR)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, FAIL, "incorrect heap ID version");

    /* Set the shared heap header's file context for this operation */
    fh->hdr->f = fh->f;

    /* Check type of object in heap */
    if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_MAN) {
        /* Operate on object from managed heap blocks */
        /* (ID can't change and modifying object is "easy" to manage) */
        if (H5HF__man_write(fh->hdr, id, obj) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_WRITEERROR, FAIL, "can't write to 'managed' heap object");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_HUGE) {
        /* Operate on "huge" object */
        if (H5HF__huge_write(fh->hdr, id, obj) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_WRITEERROR, FAIL, "can't write to 'huge' heap object");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_TINY) {
        /* Check for writing a 'tiny' object */
        /* (which isn't supported yet - ID will change) */
        HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "modifying 'tiny' object not supported yet");
    } /* end if */
    else {
        fprintf(stderr, "%s: Heap ID type not supported yet!\n", __func__);
        HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "heap ID type not supported yet");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_write() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_op
 *
 * Purpose:	Perform an operation directly on a heap object
 *
 * Note:	The library routines currently assume that the 'op' callback
 *		won't modify the object.  This can easily be changed later for
 *		"managed" heap objects, and, with some difficulty, for 'huge'
 *		and 'tiny' heap objects.
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_op(H5HF_t *fh, const void *_id, H5HF_operator_t op, void *op_data)
{
    const uint8_t *id = (const uint8_t *)_id; /* Object ID */
    uint8_t        id_flags;                  /* Heap ID flag bits */
    herr_t         ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(fh);
    assert(id);
    assert(op);

    /* Get the ID flags */
    id_flags = *id;

    /* Check for correct heap ID version */
    if ((id_flags & H5HF_ID_VERS_MASK) != H5HF_ID_VERS_CURR)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, FAIL, "incorrect heap ID version");

    /* Set the shared heap header's file context for this operation */
    fh->hdr->f = fh->f;

    /* Check type of object in heap */
    if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_MAN) {
        /* Operate on object from managed heap blocks */
        if (H5HF__man_op(fh->hdr, id, op, op_data) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "can't operate on object from fractal heap");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_HUGE) {
        /* Operate on 'huge' object from file */
        if (H5HF__huge_op(fh->hdr, id, op, op_data) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "can't operate on 'huge' object from fractal heap");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_TINY) {
        /* Operate on 'tiny' object from file */
        if (H5HF__tiny_op(fh->hdr, id, op, op_data) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "can't operate on 'tiny' object from fractal heap");
    } /* end if */
    else {
        fprintf(stderr, "%s: Heap ID type not supported yet!\n", __func__);
        HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "heap ID type not supported yet");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_op() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_remove
 *
 * Purpose:	Remove an object from a fractal heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_remove(H5HF_t *fh, const void *_id)
{
    const uint8_t *id = (const uint8_t *)_id; /* Object ID */
    uint8_t        id_flags;                  /* Heap ID flag bits */
    herr_t         ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(fh);
    assert(fh->hdr);
    assert(id);

    /* Get the ID flags */
    id_flags = *id;

    /* Check for correct heap ID version */
    if ((id_flags & H5HF_ID_VERS_MASK) != H5HF_ID_VERS_CURR)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, FAIL, "incorrect heap ID version");

    /* Set the shared heap header's file context for this operation */
    fh->hdr->f = fh->f;

    /* Check type of object in heap */
    if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_MAN) {
        /* Remove object from managed heap blocks */
        if (H5HF__man_remove(fh->hdr, id) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTREMOVE, FAIL, "can't remove object from fractal heap");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_HUGE) {
        /* Remove 'huge' object from file & v2 B-tree tracker */
        if (H5HF__huge_remove(fh->hdr, id) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTREMOVE, FAIL, "can't remove 'huge' object from fractal heap");
    } /* end if */
    else if ((id_flags & H5HF_ID_TYPE_MASK) == H5HF_ID_TYPE_TINY) {
        /* Remove 'tiny' object from heap statistics */
        if (H5HF__tiny_remove(fh->hdr, id) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTREMOVE, FAIL, "can't remove 'tiny' object from fractal heap");
    } /* end if */
    else {
        fprintf(stderr, "%s: Heap ID type not supported yet!\n", __func__);
        HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "heap ID type not supported yet");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_close
 *
 * Purpose:	Close a fractal heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_close(H5HF_t *fh)
{
    bool    pending_delete = false;       /* Whether the heap is pending deletion */
    haddr_t heap_addr      = HADDR_UNDEF; /* Address of heap (for deletion) */
    herr_t  ret_value      = SUCCEED;     /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(fh);

    /* Decrement file reference & check if this is the last open fractal heap using the shared heap header */
    if (0 == H5HF__hdr_fuse_decr(fh->hdr)) {
        /* Set the shared heap header's file context for this operation */
        fh->hdr->f = fh->f;

        /* Close the free space information */
        /* (Can't put this in header "destroy" routine, because it has
         *      pointers to indirect blocks in the heap, which would create
         *      a reference loop and the objects couldn't be removed from
         *      the metadata cache - QAK)
         */
        if (H5HF__space_close(fh->hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't release free space info");

        /* Reset the block iterator, if necessary */
        /* (Can't put this in header "destroy" routine, because it has
         *      pointers to indirect blocks in the heap, which would create
         *      a reference loop and the objects couldn't be removed from
         *      the metadata cache - QAK)
         */
        if (H5HF__man_iter_ready(&fh->hdr->next_block))
            if (H5HF__man_iter_reset(&fh->hdr->next_block) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't reset block iterator");

        /* Shut down the huge object information */
        /* (Can't put this in header "destroy" routine, because it has
         *      has the address of an object in the file, which might be
         *      modified by the shutdown routine - QAK)
         */
        if (H5HF__huge_term(fh->hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't release 'huge' object info");

        /* Check for pending heap deletion */
        if (fh->hdr->pending_delete) {
            /* Set local info, so heap deletion can occur after decrementing the
             *  header's ref count
             */
            pending_delete = true;
            heap_addr      = fh->hdr->heap_addr;
        } /* end if */
    }     /* end if */

    /* Decrement the reference count on the heap header */
    /* (don't put in H5HF__hdr_fuse_decr() as the heap header may be evicted
     *  immediately -QAK)
     */
    if (H5HF__hdr_decr(fh->hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement reference count on shared heap header");

    /* Check for pending heap deletion */
    if (pending_delete) {
        H5HF_hdr_t *hdr; /* Another pointer to fractal heap header */

        /* Lock the heap header into memory */
        if (NULL == (hdr = H5HF__hdr_protect(fh->f, heap_addr, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap header");

        /* Delete heap, starting with header (unprotects header) */
        if (H5HF__hdr_delete(hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDELETE, FAIL, "unable to delete fractal heap");
    } /* end if */

done:
    /* Release the fractal heap wrapper */
    fh = H5FL_FREE(H5HF_t, fh);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_close() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_delete
 *
 * Purpose:	Delete a fractal heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_delete(H5F_t *f, haddr_t fh_addr)
{
    H5HF_hdr_t *hdr       = NULL;    /* The fractal heap header information */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(fh_addr));

    /* Lock the heap header into memory */
    if (NULL == (hdr = H5HF__hdr_protect(f, fh_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap header");

    /* Check for files using shared heap header */
    if (hdr->file_rc)
        hdr->pending_delete = true;
    else {
        /* Delete heap now, starting with header (unprotects header) */
        if (H5HF__hdr_delete(hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDELETE, FAIL, "unable to delete fractal heap");
        hdr = NULL;
    } /* end if */

done:
    /* Unprotect the header, if an error occurred */
    if (hdr && H5AC_unprotect(f, H5AC_FHEAP_HDR, fh_addr, hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF_delete() */
