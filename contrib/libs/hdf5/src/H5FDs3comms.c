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

/*****************************************************************************
 * Read-Only S3 Virtual File Driver (VFD)
 *
 * Source for S3 Communications module
 *
 * ***NOT A FILE DRIVER***
 *
 * Provide functions and structures required for interfacing with Amazon
 * Simple Storage Service (S3).
 *
 * Provide S3 object access as if it were a local file.
 *
 * Connect to remote host, send and receive HTTP requests and responses
 * as part of the AWS REST API, authenticating requests as appropriate.
 *
 *****************************************************************************/

/****************/
/* Module Setup */
/****************/

/***********/
/* Headers */
/***********/

#include "H5private.h"   /* generic functions */
#include "H5Eprivate.h"  /* error handling    */
#include "H5MMprivate.h" /* memory management */
#include "H5FDs3comms.h" /* S3 Communications */
#include "H5FDros3.h"    /* ros3 file driver  */

/****************/
/* Local Macros */
/****************/

#ifdef H5_HAVE_ROS3_VFD

/* toggle debugging (enable with 1)
 */
#define S3COMMS_DEBUG 0

/* manipulate verbosity of CURL output
 * operates separately from S3COMMS_DEBUG
 *
 * 0 -> no explicit curl output
 * 1 -> on error, print failure info to stderr
 * 2 -> in addition to above, print information for all performs; sets all
 *      curl handles with CURLOPT_VERBOSE
 */
#define S3COMMS_CURL_VERBOSITY 0

/* size to allocate for "bytes=<first_byte>[-<last_byte>]" HTTP Range value
 */
#define S3COMMS_MAX_RANGE_STRING_SIZE 128

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Structures */
/********************/

/* struct s3r_datastruct
 * Structure passed to curl write callback
 * pointer to data region and record of bytes written (offset)
 */
struct s3r_datastruct {
    unsigned long magic;
    char         *data;
    size_t        size;
};
#define S3COMMS_CALLBACK_DATASTRUCT_MAGIC 0x28c2b2ul

/********************/
/* Local Prototypes */
/********************/

size_t curlwritecallback(char *ptr, size_t size, size_t nmemb, void *userdata);

herr_t H5FD_s3comms_s3r_getsize(s3r_t *handle);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*************/
/* Functions */
/*************/

/*----------------------------------------------------------------------------
 *
 * Function: curlwritecallback()
 *
 * Purpose:
 *
 *     Function called by CURL to write received data.
 *
 *     Writes bytes to `userdata`.
 *
 *     Internally manages number of bytes processed.
 *
 * Return:
 *
 *     - Number of bytes processed.
 *         - Should equal number of bytes passed to callback.
 *         - Failure will result in curl error: CURLE_WRITE_ERROR.
 *
 *----------------------------------------------------------------------------
 */
size_t
curlwritecallback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    struct s3r_datastruct *sds     = (struct s3r_datastruct *)userdata;
    size_t                 product = (size * nmemb);
    size_t                 written = 0;

    if (sds->magic != S3COMMS_CALLBACK_DATASTRUCT_MAGIC)
        return written;

    if (size > 0) {
        H5MM_memcpy(&(sds->data[sds->size]), ptr, product);
        sds->size += product;
        written = product;
    }

    return written;
} /* end curlwritecallback() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_hrb_node_set()
 *
 * Purpose:
 *
 *     Create, insert, modify, and remove elements in a field node list.
 *
 *     `name` cannot be null; will return FAIL and list will be unaltered.
 *
 *     Entries are accessed via the lowercase representation of their name:
 *     "Host", "host", and "hOSt" would all access the same node,
 *     but name's case is relevant in HTTP request output.
 *
 *     List pointer `L` must always point to either of :
 *     - header node with lowest alphabetical order (by lowername)
 *     - NULL, if list is empty
 *
 *    Types of operations:
 *
 *    - CREATE
 *        - If `L` is NULL and `name` and `value` are not NULL,
 *          a new node is created at `L`, starting a list.
 *    - MODIFY
 *        - If a node is found with a matching lowercase name and `value`
 *          is not NULL, the existing name, value, and cat values are released
 *          and replaced with the new data.
 *        - No modifications are made to the list pointers.
 *    - REMOVE
 *        - If `value` is NULL, will attempt to remove node with matching
 *          lowercase name.
 *        - If no match found, returns FAIL and list is not modified.
 *        - When removing a node, all its resources is released.
 *        - If removing the last node in the list, list pointer is set to NULL.
 *    - INSERT
 *        - If no nodes exists with matching lowercase name and `value`
 *          is not NULL, a new node is created, inserted into list
 *          alphabetically by lowercase name.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *         - List was successfully modified
 *     - FAILURE: `FAIL`
 *         - Unable to perform operation
 *             - Forbidden (attempting to remove absent node, e.g.)
 *             - Internal error
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_hrb_node_set(hrb_node_t **L, const char *name, const char *value)
{
    size_t      i          = 0;
    char       *valuecpy   = NULL;
    char       *namecpy    = NULL;
    size_t      namelen    = 0;
    char       *lowername  = NULL;
    char       *nvcat      = NULL;
    hrb_node_t *node_ptr   = NULL;
    hrb_node_t *new_node   = NULL;
    bool        is_looking = true;
    herr_t      ret_value  = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_hrb_node_set.");
    printf("NAME: %s\n", name);
    printf("VALUE: %s\n", value);
    printf("LIST:\n->");
    for (node_ptr = (*L); node_ptr != NULL; node_ptr = node_ptr->next)
        fprintf(stdout, "{%s}\n->", node_ptr->cat);
    printf("(null)\n");
    fflush(stdout);
    node_ptr = NULL;
#endif

    if (name == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to operate on null name");
    namelen = strlen(name);

    /***********************
     * PREPARE ALL STRINGS *
     **********************/

    /* copy and lowercase name
     */
    lowername = (char *)H5MM_malloc(sizeof(char) * (namelen + 1));
    if (lowername == NULL)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "cannot make space for lowercase name copy.");
    for (i = 0; i < namelen; i++)
        lowername[i] = (char)HDtolower((int)name[i]);
    lowername[namelen] = 0;

    /* If value supplied, copy name, value, and concatenated "name: value".
     * If NULL, we will be removing a node or doing nothing, so no need for
     * copies
     */
    if (value != NULL) {
        int    ret      = 0;
        size_t valuelen = strlen(value);
        size_t catlen   = namelen + valuelen + 2; /* +2 from ": " */
        size_t catwrite = catlen + 3;             /* 3 not 1 to quiet compiler warning */

        namecpy = (char *)H5MM_malloc(sizeof(char) * (namelen + 1));
        if (namecpy == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "cannot make space for name copy.");
        H5MM_memcpy(namecpy, name, (namelen + 1));

        valuecpy = (char *)H5MM_malloc(sizeof(char) * (valuelen + 1));
        if (valuecpy == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "cannot make space for value copy.");
        H5MM_memcpy(valuecpy, value, (valuelen + 1));

        nvcat = (char *)H5MM_malloc(sizeof(char) * catwrite);
        if (nvcat == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "cannot make space for concatenated string.");
        ret = snprintf(nvcat, catwrite, "%s: %s", name, value);
        if (ret < 0 || (size_t)ret > catlen)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cannot concatenate `%s: %s", name, value);
        assert(catlen == strlen(nvcat));

        /* create new_node, should we need it
         */
        new_node = (hrb_node_t *)H5MM_malloc(sizeof(hrb_node_t));
        if (new_node == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "cannot make space for new set.");

        new_node->magic     = S3COMMS_HRB_NODE_MAGIC;
        new_node->name      = NULL;
        new_node->value     = NULL;
        new_node->cat       = NULL;
        new_node->lowername = NULL;
        new_node->next      = NULL;
    }

    /***************
     * ACT ON LIST *
     ***************/

    if (*L == NULL) {
        if (value == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "trying to remove node from empty list");
        else {
#if S3COMMS_DEBUG
            printf("CREATE NEW\n");
            fflush(stdout);
#endif
            /*******************
             * CREATE NEW LIST *
             *******************/

            new_node->cat       = nvcat;
            new_node->name      = namecpy;
            new_node->lowername = lowername;
            new_node->value     = valuecpy;

            *L = new_node;
            goto done; /* bypass further seeking */
        }
    }

    /* sanity-check pointer passed in
     */
    assert((*L) != NULL);
    assert((*L)->magic == S3COMMS_HRB_NODE_MAGIC);
    node_ptr = (*L);

    /* Check whether to modify/remove first node in list
     */
    if (strcmp(lowername, node_ptr->lowername) == 0) {

        is_looking = false;

        if (value == NULL) {
#if S3COMMS_DEBUG
            printf("REMOVE HEAD\n");
            fflush(stdout);
#endif
            /***************
             * REMOVE HEAD *
             ***************/

            *L = node_ptr->next;

#if S3COMMS_DEBUG
            printf("FREEING CAT (node)\n");
            fflush(stdout);
#endif
            H5MM_xfree(node_ptr->cat);
#if S3COMMS_DEBUG
            printf("FREEING LOWERNAME (node)\n");
            fflush(stdout);
#endif
            H5MM_xfree(node_ptr->lowername);
#if S3COMMS_DEBUG
            printf("FREEING NAME (node)\n");
            fflush(stdout);
#endif
            H5MM_xfree(node_ptr->name);
#if S3COMMS_DEBUG
            printf("FREEING VALUE (node)\n");
            fflush(stdout);
#endif
            H5MM_xfree(node_ptr->value);
#if S3COMMS_DEBUG
            printf("MAGIC OK? %s\n", (node_ptr->magic == S3COMMS_HRB_NODE_MAGIC) ? "YES" : "NO");
            fflush(stdout);
#endif
            assert(node_ptr->magic == S3COMMS_HRB_NODE_MAGIC);
            node_ptr->magic += 1ul;
#if S3COMMS_DEBUG
            printf("FREEING POINTER\n");
            fflush(stdout);
#endif
            H5MM_xfree(node_ptr);

#if S3COMMS_DEBUG
            printf("FREEING WORKING LOWERNAME\n");
            fflush(stdout);
#endif
            H5MM_xfree(lowername);
            lowername = NULL;
        }
        else {
#if S3COMMS_DEBUG
            printf("MODIFY HEAD\n");
            fflush(stdout);
#endif
            /***************
             * MODIFY HEAD *
             ***************/

            H5MM_xfree(node_ptr->cat);
            H5MM_xfree(node_ptr->name);
            H5MM_xfree(node_ptr->value);

            node_ptr->name  = namecpy;
            node_ptr->value = valuecpy;
            node_ptr->cat   = nvcat;

            H5MM_xfree(lowername);
            lowername = NULL;
            new_node->magic += 1ul;
            H5MM_xfree(new_node);
            new_node = NULL;
        }
    }
    else if (strcmp(lowername, node_ptr->lowername) < 0) {

        is_looking = false;

        if (value == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "trying to remove a node 'before' head");
        else {
#if S3COMMS_DEBUG
            printf("PREPEND NEW HEAD\n");
            fflush(stdout);
#endif
            /*******************
             * INSERT NEW HEAD *
             *******************/

            new_node->name      = namecpy;
            new_node->value     = valuecpy;
            new_node->lowername = lowername;
            new_node->cat       = nvcat;
            new_node->next      = node_ptr;
            *L                  = new_node;
        }
    }

    /***************
     * SEARCH LIST *
     ***************/

    while (is_looking) {
        if (node_ptr->next == NULL) {

            is_looking = false;

            if (value == NULL)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "trying to remove absent node");
            else {
#if S3COMMS_DEBUG
                printf("APPEND A NODE\n");
                fflush(stdout);
#endif
                /*******************
                 * APPEND NEW NODE *
                 *******************/

                assert(strcmp(lowername, node_ptr->lowername) > 0);
                new_node->name      = namecpy;
                new_node->value     = valuecpy;
                new_node->lowername = lowername;
                new_node->cat       = nvcat;
                node_ptr->next      = new_node;
            }
        }
        else if (strcmp(lowername, node_ptr->next->lowername) < 0) {

            is_looking = false;

            if (value == NULL)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "trying to remove absent node");
            else {
#if S3COMMS_DEBUG
                printf("INSERT A NODE\n");
                fflush(stdout);
#endif
                /*******************
                 * INSERT NEW NODE *
                 *******************/

                assert(strcmp(lowername, node_ptr->lowername) > 0);
                new_node->name      = namecpy;
                new_node->value     = valuecpy;
                new_node->lowername = lowername;
                new_node->cat       = nvcat;
                new_node->next      = node_ptr->next;
                node_ptr->next      = new_node;
            }
        }
        else if (strcmp(lowername, node_ptr->next->lowername) == 0) {

            is_looking = false;

            if (value == NULL) {
                /*****************
                 * REMOVE A NODE *
                 *****************/

                hrb_node_t *tmp = node_ptr->next;
                node_ptr->next  = tmp->next;

#if S3COMMS_DEBUG
                printf("REMOVE A NODE\n");
                fflush(stdout);
#endif
                H5MM_xfree(tmp->cat);
                H5MM_xfree(tmp->lowername);
                H5MM_xfree(tmp->name);
                H5MM_xfree(tmp->value);

                assert(tmp->magic == S3COMMS_HRB_NODE_MAGIC);
                tmp->magic += 1ul;
                H5MM_xfree(tmp);

                H5MM_xfree(lowername);
                lowername = NULL;
            }
            else {
#if S3COMMS_DEBUG
                printf("MODIFY A NODE\n");
                fflush(stdout);
#endif
                /*****************
                 * MODIFY A NODE *
                 *****************/

                node_ptr = node_ptr->next;
                H5MM_xfree(node_ptr->name);
                H5MM_xfree(node_ptr->value);
                H5MM_xfree(node_ptr->cat);

                assert(new_node->magic == S3COMMS_HRB_NODE_MAGIC);
                new_node->magic += 1ul;
                H5MM_xfree(new_node);
                H5MM_xfree(lowername);
                new_node  = NULL;
                lowername = NULL;

                node_ptr->name  = namecpy;
                node_ptr->value = valuecpy;
                node_ptr->cat   = nvcat;
            }
        }
        else {
            /****************
             * KEEP LOOKING *
             ****************/

            node_ptr = node_ptr->next;
        }
    } /* end while is_looking */

done:
    if (ret_value == FAIL) {
        /* clean up */
        if (nvcat != NULL)
            H5MM_xfree(nvcat);
        if (namecpy != NULL)
            H5MM_xfree(namecpy);
        if (lowername != NULL)
            H5MM_xfree(lowername);
        if (valuecpy != NULL)
            H5MM_xfree(valuecpy);
        if (new_node != NULL) {
            assert(new_node->magic == S3COMMS_HRB_NODE_MAGIC);
            new_node->magic += 1ul;
            H5MM_xfree(new_node);
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_hrb_node_set() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_hrb_destroy()
 *
 * Purpose:
 *
 *    Destroy and free resources _directly_ associated with an HTTP Buffer.
 *
 *    Takes a pointer to pointer to the buffer structure.
 *    This allows for the pointer itself to be NULLed from within the call.
 *
 *    If buffer or buffer pointer is NULL, there is no effect.
 *
 *    Headers list at `first_header` is not touched.
 *
 *    - Programmer should reuse or destroy `first_header` pointer
 *      (hrb_node_t *) as suits their purposes.
 *    - Recommend fetching prior to destroy()
 *      e.g., `reuse_node = hrb_to_die->first_header; destroy(hrb_to_die);`
 *      or maintaining an external reference.
 *    - Destroy node/list separately as appropriate
 *    - Failure to account for this will result in a memory leak.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *         - successfully released buffer resources
 *         - if `buf` is NULL or `*buf` is NULL, no effect
 *     - FAILURE: `FAIL`
 *         - `buf->magic != S3COMMS_HRB_MAGIC`
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_hrb_destroy(hrb_t **_buf)
{
    hrb_t *buf       = NULL;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_hrb_destroy.\n");
#endif

    if (_buf != NULL && *_buf != NULL) {
        buf = *_buf;
        if (buf->magic != S3COMMS_HRB_MAGIC)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "pointer's magic does not match.");

        H5MM_xfree(buf->verb);
        H5MM_xfree(buf->version);
        H5MM_xfree(buf->resource);
        buf->magic += 1ul;
        H5MM_xfree(buf);
        *_buf = NULL;
    } /* end if `_buf` has some value */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_hrb_destroy() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_hrb_init_request()
 *
 * Purpose:
 *
 *     Create a new HTTP Request Buffer
 *
 *     All non-null arguments should be null-terminated strings.
 *
 *     If `verb` is NULL, defaults to "GET".
 *     If `http_version` is NULL, defaults to "HTTP/1.1".
 *
 *     `resource` cannot be NULL; should be string beginning with slash
 *     character ('/').
 *
 *     All strings are copied into the structure, making them safe from
 *     modification in source strings.
 *
 * Return:
 *
 *     - SUCCESS: pointer to new `hrb_t`
 *     - FAILURE: `NULL`
 *
 *----------------------------------------------------------------------------
 */
hrb_t *
H5FD_s3comms_hrb_init_request(const char *_verb, const char *_resource, const char *_http_version)
{
    hrb_t *request   = NULL;
    char  *res       = NULL;
    size_t reslen    = 0;
    hrb_t *ret_value = NULL;
    char  *verb      = NULL;
    size_t verblen   = 0;
    char  *vrsn      = NULL;
    size_t vrsnlen   = 0;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_hrb_init_request.\n");
#endif

    if (_resource == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "resource string cannot be null.");

    /* populate valid NULLs with defaults */
    if (_verb == NULL)
        _verb = "GET";
    if (_http_version == NULL)
        _http_version = "HTTP/1.1";

    /* malloc space for and prepare structure */
    request = (hrb_t *)H5MM_malloc(sizeof(hrb_t));
    if (request == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, NULL, "no space for request structure");
    request->magic        = S3COMMS_HRB_MAGIC;
    request->body         = NULL;
    request->body_len     = 0;
    request->first_header = NULL;

    /* malloc and copy strings for the structure */
    reslen = strlen(_resource);

    if (_resource[0] == '/') {
        res = (char *)H5MM_malloc(sizeof(char) * (reslen + 1));
        if (res == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, NULL, "no space for resource string");
        H5MM_memcpy(res, _resource, (reslen + 1));
    }
    else {
        res = (char *)H5MM_malloc(sizeof(char) * (reslen + 2));
        if (res == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, NULL, "no space for resource string");
        *res = '/';
        H5MM_memcpy((&res[1]), _resource, (reslen + 1));
        assert((reslen + 1) == strlen(res));
    } /* end if (else resource string not starting with '/') */

    verblen = strlen(_verb) + 1;
    verb    = (char *)H5MM_malloc(sizeof(char) * verblen);
    if (verb == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "no space for verb string");
    strncpy(verb, _verb, verblen);

    vrsnlen = strlen(_http_version) + 1;
    vrsn    = (char *)H5MM_malloc(sizeof(char) * vrsnlen);
    if (vrsn == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "no space for http-version string");
    strncpy(vrsn, _http_version, vrsnlen);

    /* place new copies into structure */
    request->resource = res;
    request->verb     = verb;
    request->version  = vrsn;

    ret_value = request;

done:
    /* if there is an error, clean up after ourselves */
    if (ret_value == NULL) {
        if (request != NULL)
            H5MM_xfree(request);
        if (vrsn != NULL)
            H5MM_xfree(vrsn);
        if (verb != NULL)
            H5MM_xfree(verb);
        if (res != NULL)
            H5MM_xfree(res);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_hrb_init_request() */

/****************************************************************************
 * S3R FUNCTIONS
 ****************************************************************************/

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_s3r_close()
 *
 * Purpose:
 *
 *     Close communications through given S3 Request Handle (`s3r_t`)
 *     and clean up associated resources.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *     - FAILURE: `FAIL`
 *         - fails if handle is null or has invalid magic number
 *
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_s3r_close(s3r_t *handle)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_s3r_close.\n");
#endif

    if (handle == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle cannot be null.");
    if (handle->magic != S3COMMS_S3R_MAGIC)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle has invalid magic.");

    curl_easy_cleanup(handle->curlhandle);

    H5MM_xfree(handle->secret_id);
    H5MM_xfree(handle->region);
    H5MM_xfree(handle->signing_key);
    H5MM_xfree(handle->token);

    assert(handle->httpverb != NULL);
    H5MM_xfree(handle->httpverb);

    if (FAIL == H5FD_s3comms_free_purl(handle->purl))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to release parsed url structure");

    H5MM_xfree(handle);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD_s3comms_s3r_close */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_s3r_get_filesize()
 *
 * Purpose:
 *
 *     Retrieve the filesize of an open request handle.
 *
 *     Wrapper "getter" to hide implementation details.
 *
 *
 * Return:
 *
 *     - SUCCESS: size of file, in bytes, if handle is valid.
 *     - FAILURE: 0, if handle is NULL or undefined.
 *
 *----------------------------------------------------------------------------
 */
size_t
H5FD_s3comms_s3r_get_filesize(s3r_t *handle)
{
    size_t ret_value = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (handle != NULL)
        ret_value = handle->filesize;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD_s3comms_s3r_get_filesize */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_s3r_getsize()
 *
 * Purpose:
 *
 *    Get the number of bytes of handle's target resource.
 *
 *    Sets handle and curlhandle with to enact an HTTP HEAD request on file,
 *    and parses received headers to extract "Content-Length" from response
 *    headers, storing file size at `handle->filesize`.
 *
 *    Critical step in opening (initiating) an `s3r_t` handle.
 *
 *    Wraps `s3r_read()`.
 *    Sets curlhandle to write headers to a temporary buffer (using extant
 *    write callback) and provides no buffer for body.
 *
 *    Upon exit, unsets HTTP HEAD settings from curl handle, returning to
 *    initial state. In event of error, curl handle state is undefined and is
 *    not to be trusted.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *     - FAILURE: `FAIL`
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_s3r_getsize(s3r_t *handle)
{
    uintmax_t             content_length = 0;
    CURL                 *curlh          = NULL;
    char                 *end            = NULL;
    char                 *headerresponse = NULL;
    struct s3r_datastruct sds            = {S3COMMS_CALLBACK_DATASTRUCT_MAGIC, NULL, 0};
    char                 *start          = NULL;
    herr_t                ret_value      = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_s3r_getsize.\n");
#endif

    if (handle == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle cannot be null.");
    if (handle->magic != S3COMMS_S3R_MAGIC)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle has invalid magic.");
    if (handle->curlhandle == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle has bad (null) curlhandle.");

    /********************
     * PREPARE FOR HEAD *
     ********************/

    curlh = handle->curlhandle;
    if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_NOBODY, 1L))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "error while setting CURL option (CURLOPT_NOBODY).");

    if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERDATA, &sds))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "error while setting CURL option (CURLOPT_HEADERDATA).");

    assert(handle->httpverb == NULL);
    handle->httpverb = (char *)H5MM_malloc(sizeof(char) * 16);
    if (handle->httpverb == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "unable to allocate space for S3 request HTTP verb");
    H5MM_memcpy(handle->httpverb, "HEAD", 5);

    headerresponse = (char *)H5MM_malloc(sizeof(char) * CURL_MAX_HTTP_HEADER);
    if (headerresponse == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "unable to allocate space for curl header response");
    sds.data = headerresponse;

    /*******************
     * PERFORM REQUEST *
     *******************/

    /* these parameters fetch the entire file,
     * but, with a NULL destination and NOBODY and HEADERDATA supplied above,
     * only http metadata will be sent by server and recorded by s3comms
     */
    if (FAIL == H5FD_s3comms_s3r_read(handle, 0, 0, NULL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem in reading during getsize.");

    if (sds.size > CURL_MAX_HTTP_HEADER)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "HTTP metadata buffer overrun");
    else if (sds.size == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "No HTTP metadata");
#if S3COMMS_DEBUG
    else
        fprintf(stdout, "GETSIZE: OK\n");
#endif

    /******************
     * PARSE RESPONSE *
     ******************/

    start = HDstrcasestr(headerresponse, "\r\nContent-Length: ");
    if (start == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "could not find \"Content-Length\" in response.");

    /* move "start" to beginning of value in line; find end of line */
    start = start + strlen("\r\nContent-Length: ");
    end   = strstr(start, "\r\n");
    if (end == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "could not find end of content length line");

    /* place null terminator at end of numbers
     */
    *end = '\0';

    content_length = strtoumax((const char *)start, NULL, 0);
    if (UINTMAX_MAX > SIZE_MAX && content_length > SIZE_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "content_length overflows size_t");

    if (content_length == 0 || errno == ERANGE) /* errno set by strtoumax*/
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "could not convert found \"Content-Length\" response (\"%s\")",
                    start); /* range is null-terminated, remember */

    handle->filesize = (size_t)content_length;

#if S3COMMS_DEBUG
    fprintf(stdout, "FILESIZE: %zu\n", handle->filesize);
#endif

    /**********************
     * UNDO HEAD SETTINGS *
     **********************/

    if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_NOBODY, NULL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "error while setting CURL option (CURLOPT_NOBODY).");

    if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERDATA, NULL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "error while setting CURL option (CURLOPT_HEADERDATA).");

done:
    H5MM_xfree(headerresponse);
    sds.magic += 1; /* set to bad magic */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD_s3comms_s3r_getsize */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_s3r_open()
 *
 * Purpose:
 *
 *     Logically 'open' a file hosted on S3.
 *
 *     - create new Request Handle
 *     - copy supplied url
 *     - copy authentication info if supplied
 *     - create CURL handle
 *     - fetch size of file
 *         - connect with server and execute HEAD request
 *     - return request handle ready for reads
 *
 *     To use 'default' port to connect, `port` should be 0.
 *
 *     To prevent AWS4 authentication, pass null pointer to `region`, `id`,
 *     and `signing_key`.
 *
 *     Uses `H5FD_s3comms_parse_url()` to validate and parse url input.
 *
 * Return:
 *
 *     - SUCCESS: Pointer to new request handle.
 *     - FAILURE: NULL
 *         - occurs if:
 *             - authentication strings are inconsistent
 *             - must _all_ be null, or have at least `region` and `id`
 *             - url is NULL (no filename)
 *             - unable to parse url (malformed?)
 *             - error while performing `getsize()`
 *
 *----------------------------------------------------------------------------
 */
s3r_t *
H5FD_s3comms_s3r_open(const char *url, const char *region, const char *id, const unsigned char *signing_key,
                      const char *token)
{
    size_t        tmplen    = 0;
    CURL         *curlh     = NULL;
    s3r_t        *handle    = NULL;
    parsed_url_t *purl      = NULL;
    s3r_t        *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_s3r_open.\n");
#endif

    if (url == NULL || url[0] == '\0')
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "url cannot be null.");

    if (FAIL == H5FD_s3comms_parse_url(url, &purl))
        /* probably a malformed url, but could be internal error */
        HGOTO_ERROR(H5E_ARGS, H5E_CANTCREATE, NULL, "unable to create parsed url structure");

    assert(purl != NULL); /* if above passes, this must be true */
    assert(purl->magic == S3COMMS_PARSED_URL_MAGIC);

    handle = (s3r_t *)H5MM_malloc(sizeof(s3r_t));
    if (handle == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, NULL, "could not malloc space for handle.");

    handle->magic       = S3COMMS_S3R_MAGIC;
    handle->purl        = purl;
    handle->filesize    = 0;
    handle->region      = NULL;
    handle->secret_id   = NULL;
    handle->signing_key = NULL;
    handle->token       = NULL;
    handle->httpverb    = NULL;

    /*************************************
     * RECORD AUTHENTICATION INFORMATION *
     *************************************/

    if ((region != NULL && *region != '\0') || (id != NULL && *id != '\0') || (signing_key != NULL) ||
        (token != NULL)) {

        /* if one exists, all three must exist */
        if (region == NULL || region[0] == '\0')
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "region cannot be null.");
        if (id == NULL || id[0] == '\0')
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "secret id cannot be null.");
        if (signing_key == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "signing key cannot be null.");
        if (token == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "token cannot be null.");

        /* copy strings */
        tmplen         = strlen(region) + 1;
        handle->region = (char *)H5MM_malloc(sizeof(char) * tmplen);
        if (handle->region == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "could not malloc space for handle region copy.");
        H5MM_memcpy(handle->region, region, tmplen);

        tmplen            = strlen(id) + 1;
        handle->secret_id = (char *)H5MM_malloc(sizeof(char) * tmplen);
        if (handle->secret_id == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "could not malloc space for handle ID copy.");
        H5MM_memcpy(handle->secret_id, id, tmplen);

        tmplen              = SHA256_DIGEST_LENGTH;
        handle->signing_key = (unsigned char *)H5MM_malloc(sizeof(unsigned char) * tmplen);
        if (handle->signing_key == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "could not malloc space for handle key copy.");
        H5MM_memcpy(handle->signing_key, signing_key, tmplen);

        tmplen        = strlen(token) + 1;
        handle->token = (char *)H5MM_malloc(sizeof(char) * tmplen);
        if (handle->token == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "could not malloc space for handle token copy.");
        H5MM_memcpy(handle->token, token, tmplen);
    } /* if authentication information provided */

    /************************
     * INITIATE CURL HANDLE *
     ************************/

    curlh = curl_easy_init();
    if (curlh == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "problem creating curl easy handle!");

    if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HTTPGET, 1L))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "error while setting CURL option (CURLOPT_HTTPGET).");

    if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "error while setting CURL option (CURLOPT_HTTP_VERSION).");

    if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_FAILONERROR, 1L))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "error while setting CURL option (CURLOPT_FAILONERROR).");

    if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_WRITEFUNCTION, curlwritecallback))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "error while setting CURL option (CURLOPT_WRITEFUNCTION).");

    if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_URL, url))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "error while setting CURL option (CURLOPT_URL).");

#if S3COMMS_CURL_VERBOSITY > 1
    /* CURL will print (to stdout) information for each operation
     */
    curl_easy_setopt(curlh, CURLOPT_VERBOSE, 1L);
#endif

    handle->curlhandle = curlh;

    /*******************
     * OPEN CONNECTION *
     * * * * * * * * * *
     *  GET FILE SIZE  *
     *******************/

    if (FAIL == H5FD_s3comms_s3r_getsize(handle))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "problem in H5FD_s3comms_s3r_getsize.");

    /*********************
     * FINAL PREPARATION *
     *********************/

    assert(handle->httpverb != NULL);
    H5MM_memcpy(handle->httpverb, "GET", 4);

    ret_value = handle;

done:
    if (ret_value == NULL) {
        if (curlh != NULL)
            curl_easy_cleanup(curlh);
        if (FAIL == H5FD_s3comms_free_purl(purl))
            HDONE_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "unable to free parsed url structure");
        if (handle != NULL) {
            H5MM_xfree(handle->region);
            H5MM_xfree(handle->secret_id);
            H5MM_xfree(handle->signing_key);
            H5MM_xfree(handle->token);
            if (handle->httpverb != NULL)
                H5MM_xfree(handle->httpverb);
            H5MM_xfree(handle);
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD_s3comms_s3r_open */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_s3r_read()
 *
 * Purpose:
 *
 *     Read file pointed to by request handle, writing specified
 *     `offset` .. `offset + len` bytes to buffer `dest`.
 *
 *     If `len` is 0, reads entirety of file starting at `offset`.
 *     If `offset` and `len` are both 0, reads entire file.
 *
 *     If `offset` or `offset+len` is greater than the file size, read is
 *     aborted and returns `FAIL`.
 *
 *     Uses configured "curl easy handle" to perform request.
 *
 *     In event of error, buffer should remain unaltered.
 *
 *     If handle is set to authorize a request, creates a new (temporary)
 *     HTTP Request object (hrb_t) for generating requisite headers,
 *     which is then translated to a `curl slist` and set in the curl handle
 *     for the request.
 *
 *     `dest` _may_ be NULL, but no body data will be recorded.
 *
 *     - In general practice, NULL should never be passed in as `dest`.
 *     - NULL `dest` passed in by internal function `s3r_getsize()`, in
 *       conjunction with CURLOPT_NOBODY to preempt transmission of file data
 *       from server.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *     - FAILURE: `FAIL`
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_s3r_read(s3r_t *handle, haddr_t offset, size_t len, void *dest)
{
    CURL              *curlh         = NULL;
    CURLcode           p_status      = CURLE_OK;
    struct curl_slist *curlheaders   = NULL;
    hrb_node_t        *headers       = NULL;
    hrb_node_t        *node          = NULL;
    struct tm         *now           = NULL;
    char              *rangebytesstr = NULL;
    hrb_t             *request       = NULL;
    int                ret           = 0; /* working variable to check  */
                                          /* return value of snprintf  */
    char                  *authorization  = NULL;
    char                  *buffer1        = NULL;
    char                  *signed_headers = NULL;
    struct s3r_datastruct *sds            = NULL;
    herr_t                 ret_value      = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_s3r_read.\n");
#endif

    /**************************************
     * ABSOLUTELY NECESSARY SANITY-CHECKS *
     **************************************/

    if (handle == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle cannot be null.");
    if (handle->magic != S3COMMS_S3R_MAGIC)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle has invalid magic.");
    if (handle->curlhandle == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle has bad (null) curlhandle.");
    if (handle->purl == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle has bad (null) url.");
    assert(handle->purl->magic == S3COMMS_PARSED_URL_MAGIC);
    if (offset > handle->filesize || (len + offset) > handle->filesize)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to read past EoF");

    curlh = handle->curlhandle;

    /*********************
     * PREPARE WRITEDATA *
     *********************/

    if (dest != NULL) {
        sds = (struct s3r_datastruct *)H5MM_malloc(sizeof(struct s3r_datastruct));
        if (sds == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "could not malloc destination datastructure.");

        sds->magic = S3COMMS_CALLBACK_DATASTRUCT_MAGIC;
        sds->data  = (char *)dest;
        sds->size  = 0;
        if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_WRITEDATA, sds))
            HGOTO_ERROR(H5E_ARGS, H5E_UNINITIALIZED, FAIL,
                        "error while setting CURL option (CURLOPT_WRITEDATA).");
    }

    /*********************
     * FORMAT HTTP RANGE *
     *********************/

    if (len > 0) {
        rangebytesstr = (char *)H5MM_malloc(sizeof(char) * (S3COMMS_MAX_RANGE_STRING_SIZE + 1));
        if (rangebytesstr == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "could not malloc range format string.");
        ret = snprintf(rangebytesstr, (S3COMMS_MAX_RANGE_STRING_SIZE), "bytes=%" PRIuHADDR "-%" PRIuHADDR,
                       offset, offset + len - 1);
        if (ret <= 0 || ret >= S3COMMS_MAX_RANGE_STRING_SIZE)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to format HTTP Range value");
    }
    else if (offset > 0) {
        rangebytesstr = (char *)H5MM_malloc(sizeof(char) * (S3COMMS_MAX_RANGE_STRING_SIZE + 1));
        if (rangebytesstr == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "could not malloc range format string.");
        ret = snprintf(rangebytesstr, (S3COMMS_MAX_RANGE_STRING_SIZE), "bytes=%" PRIuHADDR "-", offset);
        if (ret <= 0 || ret >= S3COMMS_MAX_RANGE_STRING_SIZE)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to format HTTP Range value");
    }

#if S3COMMS_DEBUG
    fprintf(stdout, "%s: Bytes %" PRIuHADDR " - %" PRIuHADDR ", Request Size: %zu\n", handle->httpverb,
            offset, offset + len - 1, len);
#endif

    /*******************
     * COMPILE REQUEST *
     *******************/

    if (handle->signing_key == NULL) {
        /* Do not authenticate.  */
        if (rangebytesstr != NULL) {
            /* Pass in range directly */
            char *bytesrange_ptr = NULL; /* pointer past "bytes=" portion */

            bytesrange_ptr = strchr(rangebytesstr, '=');
            assert(bytesrange_ptr != NULL);
            bytesrange_ptr++; /* move to first char past '=' */
            assert(*bytesrange_ptr != '\0');

            if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_RANGE, bytesrange_ptr))
                HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL,
                            "error while setting CURL option (CURLOPT_RANGE). ");
        }
    }
    else {
        /* authenticate request
         */
        authorization = (char *)H5MM_malloc(512 + H5FD_ROS3_MAX_SECRET_TOK_LEN + 1);
        if (authorization == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "cannot make space for authorization variable.");
        /*   2048 := approximate max length...
         *     67 <len("AWS4-HMAC-SHA256 Credential=///s3/aws4_request,"
         *             "SignedHeaders=,Signature=")>
         * +    8 <yyyyMMDD>
         * +   64 <hex(sha256())>
         * +  128 <max? len(secret_id)>
         * +   20 <max? len(region)>
         * +  128 <max? len(signed_headers)>
         * + 1024 <max? len(session_token)>
         */
        char buffer2[256 + 1]; /* -> String To Sign -> Credential */
        char iso8601now[ISO8601_SIZE];
        buffer1 = (char *)H5MM_malloc(512 + H5FD_ROS3_MAX_SECRET_TOK_LEN +
                                      1); /* -> Canonical Request -> Signature */
        if (buffer1 == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "cannot make space for buffer1 variable.");
        signed_headers = (char *)H5MM_malloc(48 + H5FD_ROS3_MAX_SECRET_KEY_LEN + 1);
        if (signed_headers == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "cannot make space for signed_headers variable.");
        /* should be large enough for nominal listing:
         * "host;range;x-amz-content-sha256;x-amz-date;x-amz-security-token"
         * + '\0', with "range;" and/or "x-amz-security-token" possibly absent
         */

        /* zero start of strings */
        authorization[0]  = 0;
        buffer1[0]        = 0;
        buffer2[0]        = 0;
        iso8601now[0]     = 0;
        signed_headers[0] = 0;

        /**** VERIFY INFORMATION EXISTS ****/

        if (handle->region == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle must have non-null region.");
        if (handle->secret_id == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle must have non-null secret_id.");
        if (handle->signing_key == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle must have non-null signing_key.");
        if (handle->token == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle must have non-null token.");
        if (handle->httpverb == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle must have non-null httpverb.");
        if (handle->purl->host == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle must have non-null host.");
        if (handle->purl->path == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "handle must have non-null resource.");

        /**** CREATE HTTP REQUEST STRUCTURE (hrb_t) ****/

        request = H5FD_s3comms_hrb_init_request((const char *)handle->httpverb,
                                                (const char *)handle->purl->path, "HTTP/1.1");
        if (request == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "could not allocate hrb_t request.");
        assert(request->magic == S3COMMS_HRB_MAGIC);

        now = gmnow();
        if (ISO8601NOW(iso8601now, now) != (ISO8601_SIZE - 1))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "could not format ISO8601 time.");

        if (FAIL == H5FD_s3comms_hrb_node_set(&headers, "x-amz-date", (const char *)iso8601now))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to set x-amz-date header");
        if (headers == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem building headers list.");
        assert(headers->magic == S3COMMS_HRB_NODE_MAGIC);

        if (FAIL == H5FD_s3comms_hrb_node_set(&headers, "x-amz-content-sha256", (const char *)EMPTY_SHA256))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to set x-amz-content-sha256 header");
        if (headers == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem building headers list.");
        assert(headers->magic == S3COMMS_HRB_NODE_MAGIC);

        if (strlen((const char *)handle->token) > 0) {
            if (FAIL ==
                H5FD_s3comms_hrb_node_set(&headers, "x-amz-security-token", (const char *)handle->token))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to set x-amz-security-token header");
            if (headers == NULL)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem building headers list.");
            assert(headers->magic == S3COMMS_HRB_NODE_MAGIC);
        }

        if (rangebytesstr != NULL) {
            if (FAIL == H5FD_s3comms_hrb_node_set(&headers, "Range", rangebytesstr))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to set range header");
            if (headers == NULL)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem building headers list.");
            assert(headers->magic == S3COMMS_HRB_NODE_MAGIC);
        }

        if (FAIL == H5FD_s3comms_hrb_node_set(&headers, "Host", handle->purl->host))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to set host header");
        if (headers == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem building headers list.");
        assert(headers->magic == S3COMMS_HRB_NODE_MAGIC);

        request->first_header = headers;

        /**** COMPUTE AUTHORIZATION ****/

        /* buffer1 -> canonical request */
        if (FAIL == H5FD_s3comms_aws_canonical_request(buffer1, 512 + H5FD_ROS3_MAX_SECRET_TOK_LEN,
                                                       signed_headers, 48 + H5FD_ROS3_MAX_SECRET_TOK_LEN,
                                                       request)) {
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "bad canonical request");
        }
        /* buffer2->string-to-sign */
        if (FAIL == H5FD_s3comms_tostringtosign(buffer2, buffer1, iso8601now, handle->region))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "bad string-to-sign");
        /* buffer1 -> signature */
        if (FAIL == H5FD_s3comms_HMAC_SHA256(handle->signing_key, SHA256_DIGEST_LENGTH, buffer2,
                                             strlen(buffer2), buffer1))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "bad signature");

        iso8601now[8] = 0; /* trim to yyyyMMDD */
        ret = S3COMMS_FORMAT_CREDENTIAL(buffer2, handle->secret_id, iso8601now, handle->region, "s3");
        if (ret == 0 || ret >= S3COMMS_MAX_CREDENTIAL_SIZE)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to format aws4 credential string");

        ret = snprintf(authorization, 512 + H5FD_ROS3_MAX_SECRET_TOK_LEN,
                       "AWS4-HMAC-SHA256 Credential=%s,SignedHeaders=%s,Signature=%s", buffer2,
                       signed_headers, buffer1);
        if (ret <= 0 || ret >= 512 + H5FD_ROS3_MAX_SECRET_TOK_LEN)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to format aws4 authorization string");

        /* append authorization header to http request buffer */
        if (H5FD_s3comms_hrb_node_set(&headers, "Authorization", (const char *)authorization) == FAIL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to set Authorization header");
        if (headers == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem building headers list.");

        /* update hrb's "first header" pointer */
        request->first_header = headers;

        /**** SET CURLHANDLE HTTP HEADERS FROM GENERATED DATA ****/

        node = request->first_header;
        while (node != NULL) {
            assert(node->magic == S3COMMS_HRB_NODE_MAGIC);
            curlheaders = curl_slist_append(curlheaders, (const char *)node->cat);
            if (curlheaders == NULL)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "could not append header to curl slist.");
            node = node->next;
        }

        /* sanity-check */
        if (curlheaders == NULL)
            /* above loop was probably never run */
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "curlheaders was never populated.");

        /* finally, set http headers in curl handle */
        if (curl_easy_setopt(curlh, CURLOPT_HTTPHEADER, curlheaders) != CURLE_OK)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "error while setting CURL option (CURLOPT_HTTPHEADER).");
    } /* end if should authenticate (info provided) */

    /*******************
     * PERFORM REQUEST *
     *******************/

#if S3COMMS_CURL_VERBOSITY > 0
    /* In event of error, print detailed information to stderr
     * This is not the default behavior.
     */
    {
        long int httpcode = 0;
        char     curlerrbuf[CURL_ERROR_SIZE];
        curlerrbuf[0] = '\0';

        if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_ERRORBUFFER, curlerrbuf))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem setting error buffer");

        p_status = curl_easy_perform(curlh);

        if (p_status != CURLE_OK) {
            if (CURLE_OK != curl_easy_getinfo(curlh, CURLINFO_RESPONSE_CODE, &httpcode))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem getting response code");
            fprintf(stdout, "CURL ERROR CODE: %d\nHTTP CODE: %ld\n", p_status, httpcode);
            fprintf(stdout, "%s\n", curl_easy_strerror(p_status));

            HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, FAIL, "problem while performing request.");
        }
        if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_ERRORBUFFER, NULL))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem unsetting error buffer");
    } /* verbose error reporting */
#else
    p_status = curl_easy_perform(curlh);

    if (p_status != CURLE_OK)
        HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, FAIL, "curl cannot perform request");
#endif

#if S3COMMS_DEBUG
    if (dest != NULL) {
        fprintf(stdout, "len: %d\n", (int)len);
        fprintf(stdout, "CHECKING FOR BUFFER OVERFLOW\n");
        if (sds == NULL)
            fprintf(stdout, "sds is NULL!\n");
        else {
            fprintf(stdout, "sds: 0x%llx\n", (long long)sds);
            fprintf(stdout, "sds->size: %d\n", (int)sds->size);
            if (len > sds->size)
                fprintf(stdout, "buffer overwrite\n");
        }
    }
    else
        fprintf(stdout, "performed on entire file\n");
#endif

done:
    /* clean any malloc'd resources
     */
    if (authorization != NULL) {
        H5MM_xfree(authorization);
        authorization = NULL;
    }
    if (buffer1 != NULL) {
        H5MM_xfree(buffer1);
        buffer1 = NULL;
    }
    if (signed_headers != NULL) {
        H5MM_xfree(signed_headers);
        signed_headers = NULL;
    }
    if (curlheaders != NULL) {
        curl_slist_free_all(curlheaders);
        curlheaders = NULL;
    }
    if (rangebytesstr != NULL) {
        H5MM_xfree(rangebytesstr);
        rangebytesstr = NULL;
    }
    if (sds != NULL) {
        H5MM_xfree(sds);
        sds = NULL;
    }
    if (request != NULL) {
        while (headers != NULL)
            if (FAIL == H5FD_s3comms_hrb_node_set(&headers, headers->name, NULL))
                HDONE_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cannot release header node");
        assert(NULL == headers);
        if (FAIL == H5FD_s3comms_hrb_destroy(&request))
            HDONE_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cannot release header request structure");
        assert(NULL == request);
    }

    if (curlh != NULL) {
        /* clear any Range */
        if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_RANGE, NULL))
            HDONE_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cannot unset CURLOPT_RANGE");

        /* clear headers */
        if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HTTPHEADER, NULL))
            HDONE_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cannot unset CURLOPT_HTTPHEADER");
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD_s3comms_s3r_read */

/****************************************************************************
 * MISCELLANEOUS FUNCTIONS
 ****************************************************************************/

/*----------------------------------------------------------------------------
 *
 * Function: gmnow()
 *
 * Purpose:
 *
 *    Get the output of `time.h`'s `gmtime()` call while minimizing setup
 *    clutter where important.
 *
 * Return:
 *
 *    Pointer to resulting `struct tm`,as created by gmtime(time_t * T).
 *
 *----------------------------------------------------------------------------
 */
struct tm *
gmnow(void)
{
    time_t     now;
    time_t    *now_ptr   = &now;
    struct tm *ret_value = NULL;

    /* Doctor assert, checks against error in time() */
    if ((time_t)(-1) != HDtime(now_ptr))
        ret_value = HDgmtime(now_ptr);

    assert(ret_value != NULL);

    return ret_value;
} /* end gmnow() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_aws_canonical_request()
 *
 * Purpose:
 *
 *     Compose AWS "Canonical Request" (and signed headers string)
 *     as defined in the REST API documentation.
 *
 *     Both destination strings are null-terminated.
 *
 *     Destination string arguments must be provided with adequate space.
 *
 *     Canonical Request format:
 *
 *      <HTTP VERB>"\n"
 *      <resource path>"\n"
 *      <query string>"\n"
 *      <header1>"\n" (`lowercase(name)`":"`trim(value)`)
 *      <header2>"\n"
 *      ... (headers sorted by name)
 *      <header_n>"\n"
 *      "\n"
 *      <signed headers>"\n" (`lowercase(header 1 name)`";"`header 2 name`;...)
 *      <hex-string of sha256sum of body> ("e3b0c4429...", e.g.)
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *         - writes canonical request to respective `...dest` strings
 *     - FAILURE: `FAIL`
 *         - one or more input argument was NULL
 *         - internal error
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_aws_canonical_request(char *canonical_request_dest, int _cr_size, char *signed_headers_dest,
                                   int _sh_size, hrb_t *http_request)
{
    hrb_node_t *node         = NULL;
    const char *query_params = ""; /* unused at present */
    herr_t      ret_value    = SUCCEED;
    int         ret          = 0;
    size_t      cr_size      = (size_t)_cr_size;
    size_t      sh_size      = (size_t)_sh_size;
    size_t      cr_len       = 0; /* working length of canonical request str */
    size_t      sh_len       = 0; /* working length of signed headers str */
    char        tmpstr[1024];

    /* "query params" refers to the optional element in the URL, e.g.
     *     http://bucket.aws.com/myfile.txt?max-keys=2&prefix=J
     *                                      ^-----------------^
     *
     * Not handled/implemented as of 2017-10-xx.
     * Element introduced as empty placeholder and reminder.
     * Further research to be done if this is ever relevant for the
     * VFD use-cases.
     */

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_aws_canonical_request.\n");
#endif

    if (http_request == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "hrb object cannot be null.");
    assert(http_request->magic == S3COMMS_HRB_MAGIC);

    if (canonical_request_dest == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "canonical request destination cannot be null.");

    if (signed_headers_dest == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "signed headers destination cannot be null.");

    /* HTTP verb, resource path, and query string lines */
    cr_len = (strlen(http_request->verb) + strlen(http_request->resource) + strlen(query_params) +
              (size_t)3); /* three newline chars */
    if (cr_len >= cr_size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not enough space in canonical request");

    /* TODO: compiler warning */
    ret = snprintf(canonical_request_dest, (cr_size - 1), "%s\n%s\n%s\n", http_request->verb,
                   http_request->resource, query_params);
    if (ret < 0 || (size_t)ret >= cr_size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to compose canonical request first line");

    /* write in canonical headers, building signed headers concurrently */
    node = http_request->first_header; /* assumed sorted */
    while (node != NULL) {

        assert(node->magic == S3COMMS_HRB_NODE_MAGIC);

        ret = snprintf(tmpstr, 1024, "%s:%s\n", node->lowername, node->value);
        if (ret < 0 || ret >= 1024)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to concatenate HTTP header %s:%s",
                        node->lowername, node->value);
        cr_len += strlen(tmpstr);
        if (cr_len + 1 > cr_size)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not enough space in canonical request");
        strcat(canonical_request_dest, tmpstr);

        ret = snprintf(tmpstr, 1024, "%s;", node->lowername);
        if (ret < 0 || ret >= 1024)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to append semicolon to lowername %s",
                        node->lowername);
        sh_len += strlen(tmpstr);
        if (sh_len + 1 > sh_size)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not enough space in signed headers");
        strcat(signed_headers_dest, tmpstr);

        node = node->next;
    } /* end while node is not NULL */

    /* remove trailing ';' from signed headers sequence */
    if (*signed_headers_dest != '\0')
        signed_headers_dest[strlen(signed_headers_dest) - 1] = '\0';

    /* append signed headers and payload hash
     * NOTE: at present, no HTTP body is handled, per the nature of
     *       requests/range-gets
     */
    strcat(canonical_request_dest, "\n");
    strcat(canonical_request_dest, signed_headers_dest);
    strcat(canonical_request_dest, "\n");
    strcat(canonical_request_dest, EMPTY_SHA256);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_aws_canonical_request() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_bytes_to_hex()
 *
 * Purpose:
 *
 *     Produce human-readable hex string [0-9A-F] from sequence of bytes.
 *
 *     For each byte (char), writes two-character hexadecimal representation.
 *
 *     No null-terminator appended.
 *
 *     Assumes `dest` is allocated to enough size (msg_len * 2).
 *
 *     Fails if either `dest` or `msg` are null.
 *
 *     `msg_len` message length of 0 has no effect.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *         - hex string written to `dest` (not null-terminated)
 *     - FAILURE: `FAIL`
 *         - `dest == NULL`
 *         - `msg == NULL`
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_bytes_to_hex(char *dest, const unsigned char *msg, size_t msg_len, bool lowercase)
{
    size_t i         = 0;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_bytes_to_hex.\n");
#endif

    if (dest == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "hex destination cannot be null.");
    if (msg == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "bytes sequence cannot be null.");

    for (i = 0; i < msg_len; i++) {
        int chars_written = snprintf(&(dest[i * 2]), 3, /* 'X', 'X', '\n' */
                                     (lowercase == true) ? "%02x" : "%02X", msg[i]);
        if (chars_written != 2)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem while writing hex chars for %c", msg[i]);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_bytes_to_hex() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_free_purl()
 *
 * Purpose:
 *
 *     Release resources from a parsed_url_t pointer.
 *
 *     If pointer is null, nothing happens.
 *
 * Return:
 *
 *     `SUCCEED` (never fails)
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_free_purl(parsed_url_t *purl)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

#if S3COMMS_DEBUG
    printf("called H5FD_s3comms_free_purl.\n");
#endif

    if (purl != NULL) {
        assert(purl->magic == S3COMMS_PARSED_URL_MAGIC);
        if (purl->scheme != NULL)
            H5MM_xfree(purl->scheme);
        if (purl->host != NULL)
            H5MM_xfree(purl->host);
        if (purl->port != NULL)
            H5MM_xfree(purl->port);
        if (purl->path != NULL)
            H5MM_xfree(purl->path);
        if (purl->query != NULL)
            H5MM_xfree(purl->query);
        purl->magic += 1ul;
        H5MM_xfree(purl);
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD_s3comms_free_purl() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_HMAC_SHA256()
 *
 * Purpose:
 *
 *     Generate Hash-based Message Authentication Checksum using the SHA-256
 *     hashing algorithm.
 *
 *     Given a key, message, and respective lengths (to accommodate null
 *     characters in either), generate _hex string_ of authentication checksum
 *     and write to `dest`.
 *
 *     `dest` must be at least `SHA256_DIGEST_LENGTH * 2` characters in size.
 *     Not enforceable by this function.
 *     `dest` will _not_ be null-terminated by this function.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *         - hex string written to `dest` (not null-terminated)
 *     - FAILURE: `FAIL`
 *         - `dest == NULL`
 *         - error while generating hex string output
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_HMAC_SHA256(const unsigned char *key, size_t key_len, const char *msg, size_t msg_len,
                         char *dest)
{
    unsigned char md[SHA256_DIGEST_LENGTH];
    unsigned int  md_len    = SHA256_DIGEST_LENGTH;
    herr_t        ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_HMAC_SHA256.\n");
#endif

    if (dest == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination cannot be null.");

    HMAC(EVP_sha256(), key, (int)key_len, (const unsigned char *)msg, msg_len, md, &md_len);

    if (H5FD_s3comms_bytes_to_hex(dest, (const unsigned char *)md, (size_t)md_len, true) == FAIL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "could not convert to hex string.");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD_s3comms_HMAC_SHA256 */

/*-----------------------------------------------------------------------------
 *
 * Function: H5FD__s3comms_load_aws_creds_from_file()
 *
 * Purpose:
 *
 *     Extract AWS configuration information from a target file.
 *
 *     Given a file and a profile name, e.g. "ros3_vfd_test", attempt to locate
 *     that region in the file. If not found, returns in error and output
 *     pointers are not modified.
 *
 *     If the profile label is found, attempts to locate and parse configuration
 *     data, stopping at the first line where:
 *     + reached end of file
 *     + line does not start with a recognized setting name
 *
 *     Following AWS documentation, looks for any of:
 *     + aws_access_key_id
 *     + aws_secret_access_key
 *     + region
 *
 *     To be valid, the setting must begin the line with one of the keywords,
 *     followed immediately by an equals sign '=', and have some data before
 *     newline at end of line.
 *     + `spam=eggs` would be INVALID because name is unrecognized
 *     + `region = us-east-2` would be INVALID because of spaces
 *     + `region=` would be INVALID because no data.
 *
 *     Upon successful parsing of a setting line, will store the result in the
 *     corresponding output pointer. If the output pointer is NULL, will skip
 *     any matching setting line while parsing -- useful to prevent overwrite
 *     when reading from multiple files.
 *
 * Return:
 *
 *     + SUCCESS: `SUCCEED`
 *         + no error. settings may or may not have been loaded.
 *     + FAILURE: `FAIL`
 *         + internal error occurred.
 *         + -1 :: unable to format profile label
 *         + -2 :: profile name/label not found in file
 *         + -3 :: some other error
 *
 *-----------------------------------------------------------------------------
 */
static herr_t
H5FD__s3comms_load_aws_creds_from_file(FILE *file, const char *profile_name, char *key_id, char *access_key,
                                       char *aws_region)
{
    char        profile_line[32];
    char        buffer[128];
    const char *setting_names[] = {
        "region",
        "aws_access_key_id",
        "aws_secret_access_key",
    };
    char *const setting_pointers[] = {
        aws_region,
        key_id,
        access_key,
    };
    unsigned setting_count = 3;
    herr_t   ret_value     = SUCCEED;
    unsigned buffer_i      = 0;
    unsigned setting_i     = 0;
    int      found_setting = 0;
    char    *line_buffer   = &(buffer[0]);

    FUNC_ENTER_PACKAGE

#if S3COMMS_DEBUG
    fprintf(stdout, "called load_aws_creds_from_file.\n");
#endif

    /* format target line for start of profile */
    if (32 < snprintf(profile_line, 32, "[%s]", profile_name))
        HGOTO_ERROR(H5E_ARGS, H5E_CANTCOPY, FAIL, "unable to format profile label");

    /* look for start of profile */
    do {
        /* clear buffer */
        for (buffer_i = 0; buffer_i < 128; buffer_i++)
            buffer[buffer_i] = 0;

        line_buffer = fgets(line_buffer, 128, file);
        if (line_buffer == NULL) /* reached end of file */
            goto done;
    } while (strncmp(line_buffer, profile_line, strlen(profile_line)));

    /* extract credentials from lines */
    do {
        /* clear buffer */
        for (buffer_i = 0; buffer_i < 128; buffer_i++)
            buffer[buffer_i] = 0;

        /* collect a line from file */
        line_buffer = fgets(line_buffer, 128, file);
        if (line_buffer == NULL)
            goto done; /* end of file */

        /* loop over names to see if line looks like assignment */
        for (setting_i = 0; setting_i < setting_count; setting_i++) {
            size_t      setting_name_len = 0;
            const char *setting_name     = NULL;
            char        line_prefix[128];

            setting_name     = setting_names[setting_i];
            setting_name_len = strlen(setting_name);
            if (snprintf(line_prefix, 128, "%s=", setting_name) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_CANTCOPY, FAIL, "unable to format line prefix");

            /* found a matching name? */
            if (!strncmp(line_buffer, line_prefix, setting_name_len + 1)) {
                found_setting = 1;

                /* skip NULL destination buffer */
                if (setting_pointers[setting_i] == NULL)
                    break;

                /* advance to end of name in string */
                do {
                    line_buffer++;
                } while (*line_buffer != 0 && *line_buffer != '=');

                if (*line_buffer == 0 || *(line_buffer + 1) == 0)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "incomplete assignment in file");
                line_buffer++; /* was pointing at '='; advance */

                /* copy line buffer into out pointer */
                strncpy(setting_pointers[setting_i], (const char *)line_buffer, strlen(line_buffer));

                /* "trim" tailing whitespace by replacing with null terminator*/
                buffer_i = 0;
                while (!isspace(setting_pointers[setting_i][buffer_i]))
                    buffer_i++;
                setting_pointers[setting_i][buffer_i] = '\0';

                break; /* have read setting; don't compare with others */
            }          /* end if possible name match */
        }              /* end for each setting name */
    } while (found_setting);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__s3comms_load_aws_creds_from_file() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_load_aws_profile()
 *
 * Purpose :
 *
 *     Read aws profile elements from standard location on system and store
 *     settings in memory.
 *
 *     Looks for both `~/.aws/config` and `~/.aws/credentials`, the standard
 *     files for AWS tools. If a file exists (can be opened), looks for the
 *     given profile name and reads the settings into the relevant buffer.
 *
 *     Any setting duplicated in both files will be set to that from
 *     `credentials`.
 *
 *     Settings are stored in the supplied buffers as null-terminated strings.
 *
 * Return:
 *
 *     + SUCCESS: `SUCCEED` (0)
 *         + no error occurred and all settings were populated
 *     + FAILURE: `FAIL` (-1)
 *         + internal error occurred
 *         + unable to locate profile
 *         + region, key id, and secret key were not all found and set
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_load_aws_profile(const char *profile_name, char *key_id_out, char *secret_access_key_out,
                              char *aws_region_out)
{
    herr_t ret_value = SUCCEED;
    FILE  *credfile  = NULL;
    char   awspath[117];
    char   filepath[128];
    int    ret = 0;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_load_aws_profile.\n");
#endif

#ifdef H5_HAVE_WIN32_API
    ret = snprintf(awspath, 117, "%s/.aws/", getenv("USERPROFILE"));
#else
    ret = snprintf(awspath, 117, "%s/.aws/", getenv("HOME"));
#endif
    if (ret < 0 || (size_t)ret >= 117)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTCOPY, FAIL, "unable to format home-aws path");
    ret = snprintf(filepath, 128, "%s%s", awspath, "credentials");
    if (ret < 0 || (size_t)ret >= 128)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTCOPY, FAIL, "unable to format credentials path");

    credfile = fopen(filepath, "r");
    if (credfile != NULL) {
        if (H5FD__s3comms_load_aws_creds_from_file(credfile, profile_name, key_id_out, secret_access_key_out,
                                                   aws_region_out) == FAIL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to load from aws credentials");
        if (fclose(credfile) == EOF)
            HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "unable to close credentials file");
        credfile = NULL;
    } /* end if credential file opened */

    ret = snprintf(filepath, 128, "%s%s", awspath, "config");
    if (ret < 0 || (size_t)ret >= 128)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTCOPY, FAIL, "unable to format config path");
    credfile = fopen(filepath, "r");
    if (credfile != NULL) {
        if (H5FD__s3comms_load_aws_creds_from_file(
                credfile, profile_name, (*key_id_out == 0) ? key_id_out : NULL,
                (*secret_access_key_out == 0) ? secret_access_key_out : NULL,
                (*aws_region_out == 0) ? aws_region_out : NULL) == FAIL)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to load from aws config");
        if (fclose(credfile) == EOF)
            HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "unable to close config file");
        credfile = NULL;
    } /* end if credential file opened */

    /* fail if not all three settings were loaded */
    if (*key_id_out == 0 || *secret_access_key_out == 0 || *aws_region_out == 0)
        ret_value = FAIL;

done:
    if (credfile != NULL)
        if (fclose(credfile) == EOF)
            HDONE_ERROR(H5E_ARGS, H5E_ARGS, FAIL, "problem error-closing aws configuration file");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_load_aws_profile() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_nlowercase()
 *
 * Purpose:
 *
 *     From string starting at `s`, write `len` characters to `dest`,
 *     converting all to lowercase.
 *
 *     Behavior is undefined if `s` is NULL or `len` overruns the allocated
 *     space of either `s` or `dest`.
 *
 *     Provided as convenience.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *         - upon completion, `dest` is populated
 *     - FAILURE: `FAIL`
 *         - `dest == NULL`
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_nlowercase(char *dest, const char *s, size_t len)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_nlowercase.\n");
#endif

    if (dest == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination cannot be null.");

    if (len > 0) {
        H5MM_memcpy(dest, s, len);
        do {
            len--;
            dest[len] = (char)HDtolower((int)dest[len]);
        } while (len > 0);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_nlowercase() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_parse_url()
 *
 * Purpose:
 *
 *     Parse URL-like string and stuff URL components into
 *     `parsed_url` structure, if possible.
 *
 *     Expects null-terminated string of format:
 *     SCHEME "://" HOST [":" PORT ] ["/" [ PATH ] ] ["?" QUERY]
 *     where SCHEME :: "[a-zA-Z/.-]+"
 *           PORT   :: "[0-9]"
 *
 *     Stores resulting structure in argument pointer `purl`, if successful,
 *     creating and populating new `parsed_url_t` structure pointer.
 *     Empty or absent elements are NULL in new purl structure.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *         - `purl` pointer is populated
 *     - FAILURE: `FAIL`
 *         - unable to parse
 *             - `purl` is unaltered (probably NULL)
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_parse_url(const char *str, parsed_url_t **_purl)
{
    parsed_url_t *purl      = NULL; /* pointer to new structure */
    const char   *tmpstr    = NULL; /* working pointer in string */
    const char   *curstr    = str;  /* "start" pointer in string */
    long int      len       = 0;    /* substring length */
    long int      urllen    = 0;    /* length of passed-in url string */
    unsigned int  i         = 0;
    herr_t        ret_value = FAIL;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    printf("called H5FD_s3comms_parse_url.\n");
#endif

    if (str == NULL || *str == '\0')
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid url string");

    urllen = (long int)strlen(str);

    purl = (parsed_url_t *)H5MM_malloc(sizeof(parsed_url_t));
    if (purl == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "can't allocate space for parsed_url_t");
    purl->magic  = S3COMMS_PARSED_URL_MAGIC;
    purl->scheme = NULL;
    purl->host   = NULL;
    purl->port   = NULL;
    purl->path   = NULL;
    purl->query  = NULL;

    /***************
     * READ SCHEME *
     ***************/

    tmpstr = strchr(curstr, ':');
    if (tmpstr == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid SCHEME construction: probably not URL");
    len = tmpstr - curstr;
    assert((0 <= len) && (len < urllen));

    /* check for restrictions */
    for (i = 0; i < len; i++) {
        /* scheme = [a-zA-Z+-.]+ (terminated by ":") */
        if (!isalpha(curstr[i]) && '+' != curstr[i] && '-' != curstr[i] && '.' != curstr[i])
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid SCHEME construction");
    }

    /* copy lowercased scheme to structure */
    purl->scheme = (char *)H5MM_malloc(sizeof(char) * (size_t)(len + 1));
    if (purl->scheme == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "can't allocate space for SCHEME");
    strncpy(purl->scheme, curstr, (size_t)len);
    purl->scheme[len] = '\0';
    for (i = 0; i < len; i++)
        purl->scheme[i] = (char)HDtolower(purl->scheme[i]);

    /* Skip "://" */
    tmpstr += 3;
    curstr = tmpstr;

    /*************
     * READ HOST *
     *************/

    if (*curstr == '[') {
        /* IPv6 */
        while (']' != *tmpstr) {
            /* end of string reached! */
            if (tmpstr == 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "reached end of URL: incomplete IPv6 HOST");
            tmpstr++;
        }
        tmpstr++;
    } /* end if (IPv6) */
    else {
        while (0 != *tmpstr) {
            if (':' == *tmpstr || '/' == *tmpstr || '?' == *tmpstr)
                break;
            tmpstr++;
        }
    } /* end else (IPv4) */
    len = tmpstr - curstr;
    if (len == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "HOST substring cannot be empty");
    else if (len > urllen)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem with length of HOST substring");

    /* copy host */
    purl->host = (char *)H5MM_malloc(sizeof(char) * (size_t)(len + 1));
    if (purl->host == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "can't allocate space for HOST");
    strncpy(purl->host, curstr, (size_t)len);
    purl->host[len] = 0;

    /*************
     * READ PORT *
     *************/

    if (':' == *tmpstr) {
        tmpstr += 1; /* advance past ':' */
        curstr = tmpstr;
        while ((0 != *tmpstr) && ('/' != *tmpstr) && ('?' != *tmpstr))
            tmpstr++;
        len = tmpstr - curstr;
        if (len == 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "PORT element cannot be empty");
        else if (len > urllen)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem with length of PORT substring");
        for (i = 0; i < len; i++)
            if (!isdigit(curstr[i]))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "PORT is not a decimal string");

        /* copy port */
        purl->port = (char *)H5MM_malloc(sizeof(char) * (size_t)(len + 1));
        if (purl->port == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "can't allocate space for PORT");
        strncpy(purl->port, curstr, (size_t)len);
        purl->port[len] = 0;
    } /* end if PORT element */

    /*************
     * READ PATH *
     *************/

    if ('/' == *tmpstr) {
        /* advance past '/' */
        tmpstr += 1;
        curstr = tmpstr;

        /* seek end of PATH */
        while ((0 != *tmpstr) && ('?' != *tmpstr))
            tmpstr++;
        len = tmpstr - curstr;
        if (len > urllen)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem with length of PATH substring");
        if (len > 0) {
            purl->path = (char *)H5MM_malloc(sizeof(char) * (size_t)(len + 1));
            if (purl->path == NULL)
                HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "can't allocate space for PATH");
            strncpy(purl->path, curstr, (size_t)len);
            purl->path[len] = 0;
        }
    } /* end if PATH element */

    /**************
     * READ QUERY *
     **************/

    if ('?' == *tmpstr) {
        tmpstr += 1;
        curstr = tmpstr;
        while (0 != *tmpstr)
            tmpstr++;
        len = tmpstr - curstr;
        if (len == 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "QUERY cannot be empty");
        else if (len > urllen)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem with length of QUERY substring");
        purl->query = (char *)H5MM_malloc(sizeof(char) * (size_t)(len + 1));
        if (purl->query == NULL)
            HGOTO_ERROR(H5E_ARGS, H5E_CANTALLOC, FAIL, "can't allocate space for QUERY");
        strncpy(purl->query, curstr, (size_t)len);
        purl->query[len] = 0;
    } /* end if QUERY exists */

    *_purl    = purl;
    ret_value = SUCCEED;

done:
    if (ret_value == FAIL)
        H5FD_s3comms_free_purl(purl);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_parse_url() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_percent_encode_char()
 *
 * Purpose:
 *
 *     "Percent-encode" utf-8 character `c`, e.g.,
 *         '$' -> "%24"
 *         '¢' -> "%C2%A2"
 *
 *     `c` cannot be null.
 *
 *     Does not (currently) accept multi-byte characters...
 *     limit to (?) u+00ff, well below upper bound for two-byte utf-8 encoding
 *        (u+0080..u+07ff).
 *
 *     Writes output to `repr`.
 *     `repr` cannot be null.
 *     Assumes adequate space i `repr`...
 *         >>> char[4] or [7] for most characters,
 *         >>> [13] as theoretical maximum.
 *
 *     Representation `repr` is null-terminated.
 *
 *     Stores length of representation (without null terminator) at pointer
 *     `repr_len`.
 *
 * Return : SUCCEED/FAIL
 *
 *     - SUCCESS: `SUCCEED`
 *         - percent-encoded representation  written to `repr`
 *         - 'repr' is null-terminated
 *     - FAILURE: `FAIL`
 *         - `c` or `repr` was NULL
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_percent_encode_char(char *repr, const unsigned char c, size_t *repr_len)
{
    unsigned int i             = 0;
    int          chars_written = 0;
    herr_t       ret_value     = SUCCEED;
#if S3COMMS_DEBUG
    unsigned char s[2]   = {c, 0};
    unsigned char hex[3] = {0, 0, 0};
#endif

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_percent_encode_char.\n");
#endif

    if (repr == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no destination `repr`.");

#if S3COMMS_DEBUG
    H5FD_s3comms_bytes_to_hex((char *)hex, s, 1, false);
    fprintf(stdout, "    CHAR: \'%s\'\n", s);
    fprintf(stdout, "    CHAR-HEX: \"%s\"\n", hex);
#endif

    if (c <= (unsigned char)0x7f) {
        /* character represented in a single "byte"
         * and single percent-code
         */
#if S3COMMS_DEBUG
        fprintf(stdout, "    SINGLE-BYTE\n");
#endif
        *repr_len     = 3;
        chars_written = snprintf(repr, 4, "%%%02X", c);
        if (chars_written < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cannot write char %c", c);
    } /* end if single-byte unicode char */
    else {
        /* multi-byte, multi-percent representation
         */
        unsigned int  acc        = 0; /* byte accumulator */
        unsigned int  k          = 0; /* uint character representation */
        unsigned int  stack_size = 0;
        unsigned char stack[4]   = {0, 0, 0, 0};
#if S3COMMS_DEBUG
        fprintf(stdout, "    MULTI-BYTE\n");
#endif
        stack_size = 0;
        k          = (unsigned int)c;
        *repr_len  = 0;
        do {
            /* push number onto stack in six-bit slices
             */
            acc = k;
            acc >>= 6; /* cull least */
            acc <<= 6; /* six bits   */
            stack[stack_size++] = (unsigned char)(k - acc);
            k                   = acc >> 6;
        } while (k > 0);

        /* `stack` now has two to four six-bit 'numbers' to be put into
         * UTF-8 byte fields.
         */

#if S3COMMS_DEBUG
        fprintf(stdout, "    STACK:\n    {\n");
        for (i = 0; i < stack_size; i++) {
            H5FD_s3comms_bytes_to_hex((char *)hex, (&stack[i]), 1, false);
            hex[2] = 0;
            fprintf(stdout, "      %s,\n", hex);
        }
        fprintf(stdout, "    }\n");
#endif

        /****************
         * leading byte *
         ****************/

        /* prepend 11[1[1]]0 to first byte */
        /* 110xxxxx, 1110xxxx, or 11110xxx */
        acc = 0xC0;                         /* 0x11000000 */
        acc += (stack_size > 2) ? 0x20 : 0; /* 0x00100000 */
        acc += (stack_size > 3) ? 0x10 : 0; /* 0x00010000 */
        stack_size--;
        chars_written = snprintf(repr, 4, "%%%02X", (unsigned char)(acc + stack[stack_size]));
        if (chars_written < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cannot write char %c", c);
        *repr_len += 3;

        /************************
         * continuation byte(s) *
         ************************/

        /* 10xxxxxx */
        for (i = 0; i < stack_size; i++) {
            chars_written =
                snprintf(&repr[i * 3 + 3], 4, "%%%02X", (unsigned char)(0x80 + stack[stack_size - 1 - i]));
            if (chars_written < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cannot write char %c", c);
            *repr_len += 3;
        } /* end for each continuation byte */
    }     /* end else (multi-byte) */

    *(repr + *repr_len) = '\0';

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD_s3comms_percent_encode_char */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_signing_key()
 *
 * Purpose:
 *
 *     Create AWS4 "Signing Key" from secret key, AWS region, and timestamp.
 *
 *     Sequentially runs HMAC_SHA256 on strings in specified order,
 *     generating reusable checksum (according to documentation, valid for
 *     7 days from time given).
 *
 *     `secret` is `access key id` for targeted service/bucket/resource.
 *
 *     `iso8601now` must conform to format, yyyyMMDD'T'hhmmss'Z'
 *     e.g. "19690720T201740Z".
 *
 *     `region` should be one of AWS service region names, e.g. "us-east-1".
 *
 *     Hard-coded "service" algorithm requirement to "s3".
 *
 *     Inputs must be null-terminated strings.
 *
 *     Writes to `md` the raw byte data, length of `SHA256_DIGEST_LENGTH`.
 *     Programmer must ensure that `md` is appropriately allocated.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *         - raw byte data of signing key written to `md`
 *     - FAILURE: `FAIL`
 *         - if any input arguments was NULL
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_signing_key(unsigned char *md, const char *secret, const char *region, const char *iso8601now)
{
    char         *AWS4_secret     = NULL;
    size_t        AWS4_secret_len = 0;
    unsigned char datekey[SHA256_DIGEST_LENGTH];
    unsigned char dateregionkey[SHA256_DIGEST_LENGTH];
    unsigned char dateregionservicekey[SHA256_DIGEST_LENGTH];
    int           ret       = 0; /* return value of snprintf */
    herr_t        ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_signing_key.\n");
#endif

    if (md == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Destination `md` cannot be NULL.");
    if (secret == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "`secret` cannot be NULL.");
    if (region == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "`region` cannot be NULL.");
    if (iso8601now == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "`iso8601now` cannot be NULL.");

    AWS4_secret_len = 4 + strlen(secret) + 1;
    AWS4_secret     = (char *)H5MM_malloc(sizeof(char *) * AWS4_secret_len);
    if (AWS4_secret == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Could not allocate space.");

    /* prepend "AWS4" to start of the secret key */
    ret = snprintf(AWS4_secret, AWS4_secret_len, "%s%s", "AWS4", secret);
    if ((size_t)ret != (AWS4_secret_len - 1))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem writing AWS4+secret `%s`", secret);

    /* hash_func, key, len(key), msg, len(msg), digest_dest, digest_len_dest
     * we know digest length, so ignore via NULL
     */
    HMAC(EVP_sha256(), (const unsigned char *)AWS4_secret, (int)strlen(AWS4_secret),
         (const unsigned char *)iso8601now, 8, /* 8 --> length of 8 --> "yyyyMMDD"  */
         datekey, NULL);
    HMAC(EVP_sha256(), (const unsigned char *)datekey, SHA256_DIGEST_LENGTH, (const unsigned char *)region,
         strlen(region), dateregionkey, NULL);
    HMAC(EVP_sha256(), (const unsigned char *)dateregionkey, SHA256_DIGEST_LENGTH,
         (const unsigned char *)"s3", 2, dateregionservicekey, NULL);
    HMAC(EVP_sha256(), (const unsigned char *)dateregionservicekey, SHA256_DIGEST_LENGTH,
         (const unsigned char *)"aws4_request", 12, md, NULL);

done:
    H5MM_xfree(AWS4_secret);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_signing_key() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_tostringtosign()
 *
 * Purpose:
 *
 *     Get AWS "String to Sign" from Canonical Request, timestamp,
 *     and AWS "region".
 *
 *     Common between single request and "chunked upload",
 *     conforms to:
 *         "AWS4-HMAC-SHA256\n" +
 *         <ISO8601 date format> + "\n" +  // yyyyMMDD'T'hhmmss'Z'
 *         <yyyyMMDD> + "/" + <AWS Region> + "/s3/aws4-request\n" +
 *         hex(SHA256(<CANONICAL-REQUEST>))
 *
 *     Inputs `creq` (canonical request string), `now` (ISO8601 format),
 *     and `region` (s3 region designator string) must all be
 *     null-terminated strings.
 *
 *     Result is written to `dest` with null-terminator.
 *     It is left to programmer to ensure `dest` has adequate space.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *         - "string to sign" written to `dest` and null-terminated
 *     - FAILURE: `FAIL`
 *         - if any of the inputs are NULL
 *         - if an error is encountered while computing checksum
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_tostringtosign(char *dest, const char *req, const char *now, const char *region)
{
    unsigned char checksum[SHA256_DIGEST_LENGTH * 2 + 1];
    size_t        d = 0;
    char          day[9];
    char          hexsum[SHA256_DIGEST_LENGTH * 2 + 1];
    size_t        i         = 0;
    int           ret       = 0; /* snprintf return value */
    herr_t        ret_value = SUCCEED;
    char          tmp[128];

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_tostringtosign.\n");
#endif

    if (dest == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination buffer cannot be null.");
    if (req == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "canonical request cannot be null.");
    if (now == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Timestring cannot be NULL.");
    if (region == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Region cannot be NULL.");

    for (i = 0; i < 128; i++)
        tmp[i] = '\0';
    for (i = 0; i < SHA256_DIGEST_LENGTH * 2 + 1; i++) {
        checksum[i] = '\0';
        hexsum[i]   = '\0';
    }
    strncpy(day, now, 8);
    day[8] = '\0';
    ret    = snprintf(tmp, 127, "%s/%s/s3/aws4_request", day, region);
    if (ret <= 0 || ret >= 127)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "problem adding day and region to string");

    H5MM_memcpy((dest + d), "AWS4-HMAC-SHA256\n", 17);
    d = 17;

    H5MM_memcpy((dest + d), now, strlen(now));
    d += strlen(now);
    dest[d++] = '\n';

    H5MM_memcpy((dest + d), tmp, strlen(tmp));
    d += strlen(tmp);
    dest[d++] = '\n';

    SHA256((const unsigned char *)req, strlen(req), checksum);

    if (H5FD_s3comms_bytes_to_hex(hexsum, (const unsigned char *)checksum, SHA256_DIGEST_LENGTH, true) ==
        FAIL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "could not create hex string");

    for (i = 0; i < SHA256_DIGEST_LENGTH * 2; i++)
        dest[d++] = hexsum[i];

    dest[d] = '\0';

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ros3_tostringtosign() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_trim()
 *
 * Purpose:
 *
 *     Remove all whitespace characters from start and end of a string `s`
 *     of length `s_len`, writing trimmed string copy to `dest`.
 *     Stores number of characters remaining at `n_written`.
 *
 *     Destination for trimmed copy `dest` cannot be null.
 *     `dest` must have adequate space allocated for trimmed copy.
 *         If inadequate space, behavior is undefined, possibly resulting
 *         in segfault or overwrite of other data.
 *
 *     If `s` is NULL or all whitespace, `dest` is untouched and `n_written`
 *     is set to 0.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *     - FAILURE: `FAIL`
 *         - `dest == NULL`
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_trim(char *dest, char *s, size_t s_len, size_t *n_written)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "called H5FD_s3comms_trim.\n");
#endif

    if (dest == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination cannot be null.");
    if (s == NULL)
        s_len = 0;

    if (s_len > 0) {
        /* Find first non-whitespace character from start;
         * reduce total length per character.
         */
        while (s_len > 0 && isspace((unsigned char)s[0])) {
            s++;
            s_len--;
        }

        /* Find first non-whitespace character from tail;
         * reduce length per-character.
         * If length is 0 already, there is no non-whitespace character.
         */
        if (s_len > 0) {
            do {
                s_len--;
            } while (isspace((unsigned char)s[s_len]));
            s_len++;

            /* write output into dest */
            H5MM_memcpy(dest, s, s_len);
        }
    }

    *n_written = s_len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3comms_trim() */

/*----------------------------------------------------------------------------
 *
 * Function: H5FD_s3comms_uriencode()
 *
 * Purpose:
 *
 *     URIencode (percent-encode) every byte except "[a-zA-Z0-9]-._~".
 *
 *     For each character in source string `_s` from `s[0]` to `s[s_len-1]`,
 *     writes to `dest` either the raw character or its percent-encoded
 *     equivalent.
 *
 *     See `H5FD_s3comms_bytes_to_hex` for information on percent-encoding.
 *
 *     Space (' ') character encoded as "%20" (not "+")
 *
 *     Forward-slash ('/') encoded as "%2F" only when `encode_slash == true`.
 *
 *     Records number of characters written at `n_written`.
 *
 *     Assumes that `dest` has been allocated with enough space.
 *
 *     Neither `dest` nor `s` can be NULL.
 *
 *     `s_len == 0` will have no effect.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *     - FAILURE: `FAIL`
 *         - source strings `s` or destination `dest` are NULL
 *         - error while attempting to percent-encode a character
 *
 *----------------------------------------------------------------------------
 */
herr_t
H5FD_s3comms_uriencode(char *dest, const char *s, size_t s_len, bool encode_slash, size_t *n_written)
{
    char   c        = 0;
    size_t dest_off = 0;
    char   hex_buffer[13];
    size_t hex_off   = 0;
    size_t hex_len   = 0;
    herr_t ret_value = SUCCEED;
    size_t s_off     = 0;

    FUNC_ENTER_NOAPI_NOINIT

#if S3COMMS_DEBUG
    fprintf(stdout, "H5FD_s3comms_uriencode called.\n");
#endif

    if (s == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source string cannot be NULL");
    if (dest == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination cannot be NULL");

    /* Write characters to destination, converting to percent-encoded
     * "hex-utf-8" strings if necessary.
     * e.g., '$' -> "%24"
     */
    for (s_off = 0; s_off < s_len; s_off++) {
        c = s[s_off];
        if (isalnum(c) || c == '.' || c == '-' || c == '_' || c == '~' || (c == '/' && encode_slash == false))
            dest[dest_off++] = c;
        else {
            hex_off = 0;
            if (H5FD_s3comms_percent_encode_char(hex_buffer, (const unsigned char)c, &hex_len) == FAIL) {
                hex_buffer[0] = c;
                hex_buffer[1] = 0;
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                            "unable to percent-encode character \'%s\' "
                            "at %d in \"%s\"",
                            hex_buffer, (int)s_off, s);
            }

            for (hex_off = 0; hex_off < hex_len; hex_off++)
                dest[dest_off++] = hex_buffer[hex_off];
        } /* end else (not a regular character) */
    }     /* end for each character */

    if (dest_off < s_len)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buffer overflow");

    *n_written = dest_off;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD_s3comms_uriencode */

#endif /* H5_HAVE_ROS3_VFD */
