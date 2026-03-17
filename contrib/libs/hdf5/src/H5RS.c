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
 * Reference counted string algorithms.
 *
 * These are used for various internal strings which get copied multiple times.
 * They also efficiently handle dynamically allocations and appends.
 *
 */

/****************/
/* Module Setup */
/****************/

#include "H5RSmodule.h" /* This source code file is part of the H5RS module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FLprivate.h" /* Free lists                               */
#include "H5RSprivate.h" /* Reference-counted strings                */

/****************/
/* Local Macros */
/****************/

/* Initial buffer size to allocate */
#define H5RS_ALLOC_SIZE 256

/******************/
/* Local Typedefs */
/******************/

/* Private typedefs & structs */
struct H5RS_str_t {
    char    *s;       /* String to be reference counted */
    char    *end;     /* Pointer to terminating NUL character at the end of the string */
    size_t   len;     /* Current length of the string */
    size_t   max;     /* Size of allocated buffer */
    bool     wrapped; /* Indicates that the string to be ref-counted is not copied */
    unsigned n;       /* Reference count of number of pointers sharing string */
};

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5RS__xstrdup(H5RS_str_t *rs, const char *s);
static herr_t H5RS__prepare_for_append(H5RS_str_t *rs);
static herr_t H5RS__resize_for_append(H5RS_str_t *rs, size_t len);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5RS_str_t struct */
H5FL_DEFINE_STATIC(H5RS_str_t);

/* Declare the PQ free list for the wrapped strings */
H5FL_BLK_DEFINE_STATIC(str_buf);

/*--------------------------------------------------------------------------
 NAME
    H5RS__xstrdup
 PURPOSE
    Duplicate the string being reference counted
 USAGE
    herr_t H5RS__xstrdup(rs, s)
        H5RS_str_t *rs;         IN/OUT: Ref-counted string to hold duplicated string
        const char *s;          IN: String to duplicate

 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Duplicate a string buffer being reference counted.  Use this instead of
    [H5MM_][x]strdup, in order to use the free-list memory routines.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5RS__xstrdup(H5RS_str_t *rs, const char *s)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(rs);

    if (s) {
        size_t len = strlen(s);

        /* Determine size of buffer to allocate */
        rs->max = H5RS_ALLOC_SIZE;
        while ((len + 1) > rs->max)
            rs->max *= 2;

        /* Allocate the underlying string */
        if (NULL == (rs->s = (char *)H5FL_BLK_MALLOC(str_buf, rs->max)))
            HGOTO_ERROR(H5E_RS, H5E_CANTALLOC, FAIL, "memory allocation failed");
        if (len)
            H5MM_memcpy(rs->s, s, len);
        rs->end  = rs->s + len;
        *rs->end = '\0';
        rs->len  = len;
    } /* end if */
    else {
        /* Free previous string, if one */
        if (rs->s) {
            H5FL_BLK_FREE(str_buf, rs->s);
            rs->s = rs->end = NULL;
            rs->max = rs->len = 0;
        } /* end if */
        else {
            /* Sanity checks */
            assert(NULL == rs->end);
            assert(0 == rs->max);
            assert(0 == rs->len);
        } /* end else */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS__xstrdup() */

/*--------------------------------------------------------------------------
 NAME
    H5RS__prepare_for_append
 PURPOSE
    Prepare a ref-counted string for an append
 USAGE
    herr_t H5RS__prepare_for_append(rs)
        H5RS_str_t *rs;         IN/OUT: Ref-counted string to hold duplicated string
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Allocate space for a string, or duplicate a wrapped string, in preparation
    for appending another string.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5RS__prepare_for_append(H5RS_str_t *rs)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(rs);

    if (NULL == rs->s) {
        rs->max = H5RS_ALLOC_SIZE;
        if (NULL == (rs->s = (char *)H5FL_BLK_MALLOC(str_buf, rs->max)))
            HGOTO_ERROR(H5E_RS, H5E_CANTALLOC, FAIL, "memory allocation failed");
        rs->end = rs->s;
        *rs->s  = '\0';
        rs->len = 0;
    } /* end if */
    else {
        /* If the ref-counted string started life as a wrapper around an
         * existing string, duplicate the string now, so we can modify it.
         */
        if (rs->wrapped) {
            if (H5RS__xstrdup(rs, rs->s) < 0)
                HGOTO_ERROR(H5E_RS, H5E_CANTCOPY, FAIL, "can't copy string");
            rs->wrapped = false;
        } /* end if */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS__prepare_for_append() */

/*--------------------------------------------------------------------------
 NAME
    H5RS__resize_for_append
 PURPOSE
    Resize ref-counted string buffer to accommodate appending another string
 USAGE
    herr_t H5RS__resize_for_append(rs, len)
        H5RS_str_t *rs;         IN/OUT: Ref-counted string to hold duplicated string
        size_t len;             IN: Additional length to accommodate
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Resize a ref-counted string buffer to be large enough to accommodate
    another string of a specified length.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5RS__resize_for_append(H5RS_str_t *rs, size_t len)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(rs);

    /* Check if buffer should be re-allocated */
    if (len >= (rs->max - rs->len)) {
        /* Allocate a large enough buffer */
        while (len >= (rs->max - rs->len))
            rs->max *= 2;
        if (NULL == (rs->s = (char *)H5FL_BLK_REALLOC(str_buf, rs->s, rs->max)))
            HGOTO_ERROR(H5E_RS, H5E_CANTALLOC, FAIL, "memory allocation failed");
        rs->end = rs->s + rs->len;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS__resize() */

/*--------------------------------------------------------------------------
 NAME
    H5RS_create
 PURPOSE
    Create a reference counted string
 USAGE
    H5RS_str_t *H5RS_create(s)
        const char *s;          IN: String to initialize ref-counted string with

 RETURNS
    Returns a pointer to a new ref-counted string on success, NULL on failure.
 DESCRIPTION
    Create a reference counted string.  The string passed in is copied into an
    internal buffer.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5RS_str_t *
H5RS_create(const char *s)
{
    H5RS_str_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Allocate ref-counted string structure */
    if (NULL == (ret_value = H5FL_CALLOC(H5RS_str_t)))
        HGOTO_ERROR(H5E_RS, H5E_CANTALLOC, NULL, "memory allocation failed");

    /* Set the internal fields */
    if (s)
        if (H5RS__xstrdup(ret_value, s) < 0)
            HGOTO_ERROR(H5E_RS, H5E_CANTCOPY, NULL, "can't copy string");
    ret_value->n = 1;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS_create() */

/*--------------------------------------------------------------------------
 NAME
    H5RS_wrap
 PURPOSE
    "Wrap" a reference counted string around an existing string
 USAGE
    H5RS_str_t *H5RS_wrap(s)
        const char *s;          IN: String to wrap ref-counted string around

 RETURNS
    Returns a pointer to a new ref-counted string on success, NULL on failure.
 DESCRIPTION
    Wrap a reference counted string around an existing string, which is not
    duplicated, unless its reference count gets incremented.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5RS_str_t *
H5RS_wrap(const char *s)
{
    H5RS_str_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Allocate ref-counted string structure */
    if (NULL == (ret_value = H5FL_MALLOC(H5RS_str_t)))
        HGOTO_ERROR(H5E_RS, H5E_CANTALLOC, NULL, "memory allocation failed");

    /* Set the internal fields
     *
     * We ignore warnings about storing a const char pointer in the struct
     * since we never modify or free the string when the wrapped struct
     * field is set to true.
     */
    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    ret_value->s = (char *)s;
    H5_GCC_CLANG_DIAG_ON("cast-qual")

    ret_value->len = strlen(s);
    ret_value->end = ret_value->s + ret_value->len;

    ret_value->wrapped = true;
    ret_value->max     = 0; /* Wrapped, not allocated */
    ret_value->n       = 1;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS_wrap() */

/*-------------------------------------------------------------------------
 * Function:    H5RS_asprintf_cat
 *
 * Purpose:     This function appends formatted output to a ref-counted string,
 *              allocating the managed string if necessary.  The formatting
 *              string is printf() compatible.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
/* Disable warning for "format not a string literal" here -QAK */
/*
 *      This pragma only needs to surround the snprintf() calls with
 *      format_templ in the code below, but early (4.4.7, at least) gcc only
 *      allows diagnostic pragmas to be toggled outside of functions.
 */
H5_GCC_CLANG_DIAG_OFF("format-nonliteral")
H5_ATTR_FORMAT(printf, 2, 3)
herr_t
H5RS_asprintf_cat(H5RS_str_t *rs, const char *fmt, ...)
{
    va_list args1, args2;
    size_t  out_len;
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(rs);
    assert(fmt);

    /* Prepare the [possibly wrapped or empty] ref-counted string for an append */
    if (H5RS__prepare_for_append(rs) < 0)
        HGOTO_ERROR(H5E_RS, H5E_CANTINIT, FAIL, "can't initialize ref-counted string");

    /* Attempt to write formatted output into the managed string */
    va_start(args1, fmt);
    va_copy(args2, args1);
    while ((out_len = (size_t)vsnprintf(rs->end, (rs->max - rs->len), fmt, args1)) >= (rs->max - rs->len)) {
        /* Allocate a large enough buffer */
        if (H5RS__resize_for_append(rs, out_len) < 0)
            HGOTO_ERROR(H5E_RS, H5E_CANTRESIZE, FAIL, "can't resize ref-counted string buffer");

        /* Restart the va_list */
        va_end(args1);
        va_copy(args1, args2);
    } /* end while */

    /* Increment the size & end of the string */
    rs->len += out_len;
    rs->end += out_len;

    /* Finish access to varargs */
    va_end(args1);
    va_end(args2);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS_asprintf_cat() */
H5_GCC_CLANG_DIAG_ON("format-nonliteral")

/*-------------------------------------------------------------------------
 * Function:    H5RS_acat
 *
 * Purpose:     This function appends a character string to a ref-counted string,
 *              allocating the managed string if necessary.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5RS_acat(H5RS_str_t *rs, const char *s)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(rs);
    assert(s);

    /* Concatenate the provided string on to the managed string */
    if (*s) {
        size_t len = strlen(s);

        /* Allocate the underlying string, if necessary */
        if (H5RS__prepare_for_append(rs) < 0)
            HGOTO_ERROR(H5E_RS, H5E_CANTINIT, FAIL, "can't initialize ref-counted string");

        /* Increase the managed string's buffer size if necessary */
        if ((rs->len + len) >= rs->max)
            if (H5RS__resize_for_append(rs, len) < 0)
                HGOTO_ERROR(H5E_RS, H5E_CANTRESIZE, FAIL, "can't resize ref-counted string buffer");

        /* Append the string */
        H5MM_memcpy(rs->end, s, len);
        rs->end += len;
        *rs->end = '\0';
        rs->len += len;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS_acat() */

/*-------------------------------------------------------------------------
 * Function:    H5RS_ancat
 *
 * Purpose:     This function appends at most 'n' characters from a string
 *              to a ref-counted string, allocating the managed string if
 *              necessary.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5RS_ancat(H5RS_str_t *rs, const char *s, size_t n)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(rs);
    assert(s);

    /* Concatenate the provided string on to the managed string */
    if (n && *s) {
        size_t len = strlen(s);

        /* Limit characters to copy to the minimum of 'n' and 'len' */
        n = MIN(len, n);

        /* Allocate the underlying string, if necessary */
        if (H5RS__prepare_for_append(rs) < 0)
            HGOTO_ERROR(H5E_RS, H5E_CANTINIT, FAIL, "can't initialize ref-counted string");

        /* Increase the managed string's buffer size if necessary */
        if ((rs->len + n) >= rs->max)
            if (H5RS__resize_for_append(rs, n) < 0)
                HGOTO_ERROR(H5E_RS, H5E_CANTRESIZE, FAIL, "can't resize ref-counted string buffer");

        /* Append the string */
        H5MM_memcpy(rs->end, s, n);
        rs->end += n;
        *rs->end = '\0';
        rs->len += n;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS_ancat() */

/*-------------------------------------------------------------------------
 * Function:    H5RS_aputc
 *
 * Purpose:     This function appends a character to a ref-counted string,
 *              allocating the managed string if necessary.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5RS_aputc(H5RS_str_t *rs, int c)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(rs);
    assert(c);

    /* Allocate the underlying string, if necessary */
    if (H5RS__prepare_for_append(rs) < 0)
        HGOTO_ERROR(H5E_RS, H5E_CANTINIT, FAIL, "can't initialize ref-counted string");

    /* Increase the managed string's buffer size if necessary */
    if ((rs->len + 1) >= rs->max)
        if (H5RS__resize_for_append(rs, 1) < 0)
            HGOTO_ERROR(H5E_RS, H5E_CANTRESIZE, FAIL, "can't resize ref-counted string buffer");

    /* Append the current character */
    *rs->end++ = (char)c;
    rs->len++;
    *rs->end = '\0';

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS_aputc() */

/*--------------------------------------------------------------------------
 NAME
    H5RS_decr
 PURPOSE
    Decrement the reference count for a ref-counted string
 USAGE
    herr_t H5RS_decr(rs)
        H5RS_str_t *rs;     IN/OUT: Ref-counted string to decrement count of

 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Decrement the reference count for a reference counted string.  If the
    reference count drops to zero, the reference counted string is deleted.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5RS_decr(H5RS_str_t *rs)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(rs);
    assert(rs->n > 0);

    /* Decrement reference count for string */
    if ((--rs->n) == 0) {
        if (!rs->wrapped)
            rs->s = (char *)H5FL_BLK_FREE(str_buf, rs->s);
        rs = H5FL_FREE(H5RS_str_t, rs);
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5RS_decr() */

/*--------------------------------------------------------------------------
 NAME
    H5RS_incr
 PURPOSE
    Increment the reference count for a ref-counted string
 USAGE
    herr_t H5RS_incr(rs)
        H5RS_str_t *rs;     IN/OUT: Ref-counted string to increment count of

 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Increment the reference count for a reference counted string.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5RS_incr(H5RS_str_t *rs)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(rs);
    assert(rs->n > 0);

    /* If the ref-counted string started life as a wrapper around an existing
     * string, duplicate the string now, so that the wrapped string can go out
     * scope appropriately.
     */
    if (rs->wrapped) {
        if (H5RS__xstrdup(rs, rs->s) < 0)
            HGOTO_ERROR(H5E_RS, H5E_CANTCOPY, FAIL, "can't copy string");
        rs->wrapped = false;
    } /* end if */

    /* Increment reference count for string */
    rs->n++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS_incr() */

/*--------------------------------------------------------------------------
 NAME
    H5RS_dup
 PURPOSE
    "Duplicate" a ref-counted string
 USAGE
    H5RS_str_t H5RS_dup(rs)
        H5RS_str_t *rs;     IN/OUT: Ref-counted string to "duplicate"

 RETURNS
    Returns a pointer to ref-counted string on success, NULL on failure.
 DESCRIPTION
    Increment the reference count for the reference counted string and return
    a pointer to it.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5RS_str_t *
H5RS_dup(H5RS_str_t *ret_value)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check for valid reference counted string */
    if (ret_value != NULL)
        /* Increment reference count for string */
        ret_value->n++;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5RS_dup() */

/*--------------------------------------------------------------------------
 NAME
    H5RS_cmp
 PURPOSE
    Compare two ref-counted strings
 USAGE
    int H5RS_cmp(rs1,rs2)
        const H5RS_str_t *rs1;  IN: First Ref-counted string to compare
        const H5RS_str_t *rs2;  IN: Second Ref-counted string to compare

 RETURNS
    Returns positive, negative or 0 for comparison of two r-strings [same as
    strcmp()]
 DESCRIPTION
    Compare two ref-counted strings and return a value indicating their sort
    order [same as strcmp()]
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
int
H5RS_cmp(const H5RS_str_t *rs1, const H5RS_str_t *rs2)
{
    /* Can't return invalid value from this function */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(rs1);
    assert(rs1->s);
    assert(rs2);
    assert(rs2->s);

    FUNC_LEAVE_NOAPI(strcmp(rs1->s, rs2->s))
} /* end H5RS_cmp() */

/*--------------------------------------------------------------------------
 NAME
    H5RS_len
 PURPOSE
    Compute the length of a ref-counted string
 USAGE
    ssize_t H5RS_cmp(rs)
        const H5RS_str_t *rs;  IN: Ref-counted string to compute length of

 RETURNS
    Returns non-negative value on success, can't fail
 DESCRIPTION
    Compute the length of a ref-counted string.  [same as strlen()]
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
size_t
H5RS_len(const H5RS_str_t *rs)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(rs);
    assert(rs->s);

    FUNC_LEAVE_NOAPI(strlen(rs->s))
} /* end H5RS_len() */

/*--------------------------------------------------------------------------
 NAME
    H5RS_get_str
 PURPOSE
    Get a pointer to the internal string contained in a ref-counted string
 USAGE
    char *H5RS_get_str(rs)
        const H5RS_str_t *rs;   IN: Ref-counted string to get internal string from

 RETURNS
    Returns a pointer to the internal string being ref-counted on success,
        NULL on failure.
 DESCRIPTION
    Gets a pointer to the internal string being reference counted.  This
    pointer is volatile and might be invalid is further calls to the H5RS
    API are made.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
char *
H5RS_get_str(const H5RS_str_t *rs)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(rs);
    assert(rs->s);

    FUNC_LEAVE_NOAPI(rs->s)
} /* end H5RS_get_str() */

/*--------------------------------------------------------------------------
 NAME
    H5RS_get_count
 PURPOSE
    Get the reference count for a ref-counted string
 USAGE
    unsigned H5RS_get_count(rs)
        const H5RS_str_t *rs;   IN: Ref-counted string to get internal count from

 RETURNS
    Returns the number of references to the internal string being ref-counted on success,
        0 on failure.
 DESCRIPTION
    Gets the count of references to the reference counted string.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
unsigned
H5RS_get_count(const H5RS_str_t *rs)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(rs);
    assert(rs->n > 0);

    FUNC_LEAVE_NOAPI(rs->n)
} /* end H5RS_get_count() */
