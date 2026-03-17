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
 * Created:     H5MM.c
 *
 * Purpose:     Memory management functions
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5MMprivate.h" /* Memory management			*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

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
 * Function:    H5MM_realloc
 *
 * Purpose:     Similar semantics as C89's realloc(). Specifically, the
 *              following calls are equivalent:
 *
 *              H5MM_realloc(NULL, size)    <==> H5MM_malloc(size)
 *              H5MM_realloc(ptr, 0)        <==> H5MM_xfree(ptr)
 *              H5MM_realloc(NULL, 0)       <==> NULL
 *
 *              Note that the (NULL, 0) combination is undefined behavior
 *              in the C standard.
 *
 * Return:      Success:    Ptr to new memory if size > 0
 *                          NULL if size is zero
 *              Failure:    NULL (input buffer is unchanged on failure)
 *-------------------------------------------------------------------------
 */
void *
H5MM_realloc(void *mem, size_t size)
{
    void *ret_value = NULL;

    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (NULL == mem && 0 == size)
        /* Not defined in the standard, return NULL */
        ret_value = NULL;
    else {
        ret_value = realloc(mem, size);

        /* Some platforms do not return NULL if size is zero. */
        if (0 == size)
            ret_value = NULL;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5MM_realloc() */

/*-------------------------------------------------------------------------
 * Function:    H5MM_xstrdup
 *
 * Purpose:     Duplicates a string, including memory allocation.
 *              NULL is an acceptable value for the input string.
 *
 * Return:      Success:    Pointer to a new string (NULL if s is NULL).
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
char *
H5MM_xstrdup(const char *s)
{
    char *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    if (s)
        if (NULL == (ret_value = strdup(s)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "string duplication failed");
done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5MM_xstrdup() */

/*-------------------------------------------------------------------------
 * Function:    H5MM_strdup
 *
 * Purpose:     Duplicates a string, including memory allocation.
 *              NULL is NOT an acceptable value for the input string.
 *
 *              If the string to be duplicated is the NULL pointer, then
 *              an error will be raised.
 *
 * Return:      Success:    Pointer to a new string
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
char *
H5MM_strdup(const char *s)
{
    char *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    if (!s)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "NULL string not allowed");
    if (NULL == (ret_value = strdup(s)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "string duplication failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5MM_strdup() */

/*-------------------------------------------------------------------------
 * Function:    H5MM_strndup
 *
 * Purpose:     Duplicates a string, including memory allocation, but only
 *              copies at most `n` bytes from the string to be duplicated.
 *              If the string to be duplicated is longer than `n`, only `n`
 *              bytes are copied and a terminating null byte is added.
 *              NULL is NOT an acceptable value for the input string.
 *
 *              If the string to be duplicated is the NULL pointer, then
 *              an error will be raised.
 *
 * Return:      Success:    Pointer to a new string
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
char *
H5MM_strndup(const char *s, size_t n)
{
    char *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    if (!s)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "NULL string not allowed");

    if (NULL == (ret_value = HDstrndup(s, n)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "string duplication failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5MM_strndup() */

/*-------------------------------------------------------------------------
 * Function:    H5MM_xfree
 *
 * Purpose:     Just like free(3) except the return value (always NULL) can
 *              be assigned to the pointer whose memory was just freed:
 *
 *                  thing = H5MM_xfree(thing);
 *
 * Return:      Success:    NULL
 *              Failure:    never fails
 *-------------------------------------------------------------------------
 */
void *
H5MM_xfree(void *mem)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    free(mem);

    FUNC_LEAVE_NOAPI(NULL)
} /* end H5MM_xfree() */

/*-------------------------------------------------------------------------
 * Function:    H5MM_xfree_const
 *
 * Purpose:     H5MM_xfree() wrapper that handles const pointers without
 *              warnings. Used for freeing buffers that should be regarded
 *              as const in use but need to be freed when no longer needed.
 *
 * Return:      Success:    NULL
 *              Failure:    never fails
 *-------------------------------------------------------------------------
 */
void *
H5MM_xfree_const(const void *mem)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Cast through uintptr_t to de-const memory */
    H5MM_xfree((void *)(uintptr_t)mem);

    FUNC_LEAVE_NOAPI(NULL)
} /* end H5MM_xfree_const() */

#ifdef H5MM_DEBUG

/*-------------------------------------------------------------------------
 * Function:    H5MM_memcpy
 *
 * Purpose:     Like memcpy(3) but with sanity checks on the parameters,
 *              particularly buffer overlap.
 *
 * Return:      Success:    pointer to dest
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
void *
H5MM_memcpy(void *dest, const void *src, size_t n)
{
    void *ret = NULL;

    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(dest);
    assert(src);

    /* Check for buffer overlap */
    assert((char *)dest >= (const char *)src + n || (const char *)src >= (char *)dest + n);

    /* Copy */
    ret = memcpy(dest, src, n);

    FUNC_LEAVE_NOAPI(ret)

} /* end H5MM_memcpy() */

#endif /* H5MM_DEBUG */
