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

#include "H5LTprivate.h"

/* For Lex and Yacc */
#define COL       3
#define LIMIT     512
#define INCREMENT 1024
#define TMP_LEN   256
#define MAX(a, b) (((a) > (b)) ? (a) : (b))
size_t input_len;
char  *myinput;
size_t indent = 0;

/* File Image operations

   A file image is a representation of an HDF5 file in a memory
   buffer. In order to perform operations on an image in a similar way
   to a  file, the application buffer is copied to a FAPL buffer, which
   in turn is copied to a VFD buffer. Buffer copying can decrease
   performance, especially when using large file images. A solution to
   this issue is to simulate the copying of the application buffer,
   when actually the same buffer is used for the FAPL and the VFD.
   This is implemented by using callbacks that simulate the standard
   functions for memory management (additional callbacks are used for
   the management of associated data structures). From the application
   standpoint, a file handle can be obtained from a file image by using
   the API routine H5LTopen_file_image(). This function takes a flag
   argument that indicates the HDF5 library how to handle the given image;
   several flag values can be combined by using the bitwise OR operator.
   Valid flag values include:

   H5LT_FILE_IMAGE_OPEN_RW indicates the HDF5 library to open the file
   image in read/write mode. Default is read-only mode.

   H5LT_FILE_IMAGE_DONT_COPY indicates the HDF5 library to not copy the
   supplied user buffer; the same buffer will be handled by the FAPL and
   the VFD driver. Default operation copies the user buffer to the FAPL and
   VFD driver.

   H5LT_FILE_IMAGE_DONT_RELEASE indicates the HDF5 library to not release
   the buffer handled by the FAPL and the VFD upon closing. This flag value
   is only applicable when the flag value H5LT_FILE_IMAGE_DONT_COPY is set as
   well. The application is responsible to release the image buffer.
*/

/* Data structure to pass application data to callbacks. */
typedef struct {
    void    *app_image_ptr;   /* Pointer to application buffer */
    size_t   app_image_size;  /* Size of application buffer */
    void    *fapl_image_ptr;  /* Pointer to FAPL buffer */
    size_t   fapl_image_size; /* Size of FAPL buffer */
    int      fapl_ref_count;  /* Reference counter for FAPL buffer */
    void    *vfd_image_ptr;   /* Pointer to VFD buffer */
    size_t   vfd_image_size;  /* Size of VFD buffer */
    int      vfd_ref_count;   /* Reference counter for VFD buffer */
    unsigned flags;           /* Flags indicate how the file image will */
                              /* be open */
    int ref_count;            /* Reference counter on udata struct */
} H5LT_file_image_ud_t;

/* callbacks prototypes for file image ops */
static void  *image_malloc(size_t size, H5FD_file_image_op_t file_image_op, void *udata);
static void  *image_memcpy(void *dest, const void *src, size_t size, H5FD_file_image_op_t file_image_op,
                           void *udata);
static void  *image_realloc(void *ptr, size_t size, H5FD_file_image_op_t file_image_op, void *udata);
static herr_t image_free(void *ptr, H5FD_file_image_op_t file_image_op, void *udata);
static void  *udata_copy(void *udata);
static herr_t udata_free(void *udata);

/* Definition of callbacks for file image operations. */

/*-------------------------------------------------------------------------
 * Function: image_malloc
 *
 * Purpose: Simulates malloc() function to avoid copying file images.
 *          The application buffer is set to the buffer on only one FAPL.
 *          Then the FAPL buffer can be copied to other FAPL buffers or
 *          to only one VFD buffer.
 *
 * Return: Address of "allocated" buffer, if successful. Otherwise, it returns
 *         NULL.
 *
 *-------------------------------------------------------------------------
 */
static void *
image_malloc(size_t size, H5FD_file_image_op_t file_image_op, void *_udata)
{
    H5LT_file_image_ud_t *udata        = (H5LT_file_image_ud_t *)_udata;
    void                 *return_value = NULL;

    /* callback is only used if the application buffer is not actually copied */
    if (!(udata->flags & H5LT_FILE_IMAGE_DONT_COPY))
        goto out;

    switch (file_image_op) {
        /* the app buffer is "copied" to only one FAPL. Afterwards, FAPLs can be "copied" */
        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_SET:
            if (udata->app_image_ptr == NULL)
                goto out;
            if (udata->app_image_size != size)
                goto out;
            if (udata->fapl_image_ptr != NULL)
                goto out;
            if (udata->fapl_image_size != 0)
                goto out;
            if (udata->fapl_ref_count != 0)
                goto out;

            udata->fapl_image_ptr  = udata->app_image_ptr;
            udata->fapl_image_size = udata->app_image_size;
            return_value           = udata->fapl_image_ptr;
            udata->fapl_ref_count++;
            break;

        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_COPY:
            if (udata->fapl_image_ptr == NULL)
                goto out;
            if (udata->fapl_image_size != size)
                goto out;
            if (udata->fapl_ref_count == 0)
                goto out;

            return_value = udata->fapl_image_ptr;
            udata->fapl_ref_count++;
            break;

        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_GET:
            goto out;

        case H5FD_FILE_IMAGE_OP_FILE_OPEN:
            /* FAPL buffer is "copied" to only one VFD buffer */
            if (udata->vfd_image_ptr != NULL)
                goto out;
            if (udata->vfd_image_size != 0)
                goto out;
            if (udata->vfd_ref_count != 0)
                goto out;
            if (udata->fapl_image_ptr == NULL)
                goto out;
            if (udata->fapl_image_size != size)
                goto out;
            if (udata->fapl_ref_count == 0)
                goto out;

            udata->vfd_image_ptr  = udata->fapl_image_ptr;
            udata->vfd_image_size = size;
            udata->vfd_ref_count++;
            return_value = udata->vfd_image_ptr;
            break;

        /* added unused labels to shut the compiler up */
        case H5FD_FILE_IMAGE_OP_NO_OP:
        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_CLOSE:
        case H5FD_FILE_IMAGE_OP_FILE_RESIZE:
        case H5FD_FILE_IMAGE_OP_FILE_CLOSE:
        default:
            goto out;
    } /* end switch */

    return (return_value);

out:
    return NULL;
} /* end image_malloc() */

/*-------------------------------------------------------------------------
 * Function: image_memcpy
 *
 * Purpose:  Simulates memcpy() function to avoid copying file images.
 *           The image buffer can be set to only one FAPL buffer, and
 *           "copied" to only one VFD buffer. The FAPL buffer can be
 *           "copied" to other FAPLs buffers.
 *
 * Return: The address of the destination buffer, if successful. Otherwise, it
 *         returns NULL.
 *
 *-------------------------------------------------------------------------
 */
static void *
image_memcpy(void *dest, const void *src, size_t size, H5FD_file_image_op_t file_image_op, void *_udata)
{
    H5LT_file_image_ud_t *udata = (H5LT_file_image_ud_t *)_udata;

    /* callback is only used if the application buffer is not actually copied */
    if (!(udata->flags & H5LT_FILE_IMAGE_DONT_COPY))
        goto out;

    switch (file_image_op) {
        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_SET:
            if (dest != udata->fapl_image_ptr)
                goto out;
            if (src != udata->app_image_ptr)
                goto out;
            if (size != udata->fapl_image_size)
                goto out;
            if (size != udata->app_image_size)
                goto out;
            if (udata->fapl_ref_count == 0)
                goto out;
            break;

        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_COPY:
            if (dest != udata->fapl_image_ptr)
                goto out;
            if (src != udata->fapl_image_ptr)
                goto out;
            if (size != udata->fapl_image_size)
                goto out;
            if (udata->fapl_ref_count < 2)
                goto out;
            break;

        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_GET:
            goto out;

        case H5FD_FILE_IMAGE_OP_FILE_OPEN:
            if (dest != udata->vfd_image_ptr)
                goto out;
            if (src != udata->fapl_image_ptr)
                goto out;
            if (size != udata->vfd_image_size)
                goto out;
            if (size != udata->fapl_image_size)
                goto out;
            if (udata->fapl_ref_count == 0)
                goto out;
            if (udata->vfd_ref_count != 1)
                goto out;
            break;

        /* added unused labels to shut the compiler up */
        case H5FD_FILE_IMAGE_OP_NO_OP:
        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_CLOSE:
        case H5FD_FILE_IMAGE_OP_FILE_RESIZE:
        case H5FD_FILE_IMAGE_OP_FILE_CLOSE:
        default:
            goto out;
    } /* end switch */

    return (dest);

out:
    return NULL;
} /* end image_memcpy() */

/*-------------------------------------------------------------------------
 * Function: image_realloc
 *
 * Purpose: Reallocates the shared application image buffer and updates data
 *          structures that manage buffer "copying".
 *
 * Return: Address of reallocated buffer, if successful. Otherwise, it returns
 *         NULL.
 *
 *-------------------------------------------------------------------------
 */
static void *
image_realloc(void *ptr, size_t size, H5FD_file_image_op_t file_image_op, void *_udata)
{
    H5LT_file_image_ud_t *udata        = (H5LT_file_image_ud_t *)_udata;
    void                 *return_value = NULL;

    /* callback is only used if the application buffer is not actually copied */
    if (!(udata->flags & H5LT_FILE_IMAGE_DONT_COPY))
        goto out;

    /* realloc() is not allowed when the HDF5 library won't release the image
       buffer because reallocation may change the address of the buffer. The
       new address cannot be communicated to the application to release it. */
    if (udata->flags & H5LT_FILE_IMAGE_DONT_RELEASE)
        goto out;

    /* realloc() is not allowed if the image is open in read-only mode */
    if (!(udata->flags & H5LT_FILE_IMAGE_OPEN_RW))
        goto out;

    if (file_image_op == H5FD_FILE_IMAGE_OP_FILE_RESIZE) {
        if (udata->vfd_image_ptr != ptr)
            goto out;

        if (udata->vfd_ref_count != 1)
            goto out;

        if (NULL == (udata->vfd_image_ptr = realloc(ptr, size)))
            goto out;

        udata->vfd_image_size = size;
        return_value          = udata->vfd_image_ptr;
    } /* end if */
    else
        goto out;

    return (return_value);

out:
    return NULL;
} /* end image_realloc() */

/*-------------------------------------------------------------------------
 * Function: image_free
 *
 * Purpose: Simulates deallocation of FAPL and VFD buffers by decreasing
 *          reference counters. Shared application buffer is actually
 *          deallocated if there are no outstanding references.
 *
 * Return: SUCCEED or FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
image_free(void *ptr, H5FD_file_image_op_t file_image_op, void *_udata)
{
    H5LT_file_image_ud_t *udata = (H5LT_file_image_ud_t *)_udata;

    /* callback is only used if the application buffer is not actually copied */
    if (!(udata->flags & H5LT_FILE_IMAGE_DONT_COPY))
        goto out;

    switch (file_image_op) {
        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_CLOSE:
            if (udata->fapl_image_ptr != ptr)
                goto out;
            if (udata->fapl_ref_count == 0)
                goto out;

            udata->fapl_ref_count--;

            /* release the shared buffer only if indicated by the respective flag and there are no outstanding
             * references */
            if (udata->fapl_ref_count == 0 && udata->vfd_ref_count == 0 &&
                !(udata->flags & H5LT_FILE_IMAGE_DONT_RELEASE)) {
                free(udata->fapl_image_ptr);
                udata->app_image_ptr  = NULL;
                udata->fapl_image_ptr = NULL;
                udata->vfd_image_ptr  = NULL;
            } /* end if */
            break;

        case H5FD_FILE_IMAGE_OP_FILE_CLOSE:
            if (udata->vfd_image_ptr != ptr)
                goto out;
            if (udata->vfd_ref_count != 1)
                goto out;

            udata->vfd_ref_count--;

            /* release the shared buffer only if indicated by the respective flag and there are no outstanding
             * references */
            if (udata->fapl_ref_count == 0 && udata->vfd_ref_count == 0 &&
                !(udata->flags & H5LT_FILE_IMAGE_DONT_RELEASE)) {
                free(udata->vfd_image_ptr);
                udata->app_image_ptr  = NULL;
                udata->fapl_image_ptr = NULL;
                udata->vfd_image_ptr  = NULL;
            } /* end if */
            break;

        /* added unused labels to keep the compiler quite */
        case H5FD_FILE_IMAGE_OP_NO_OP:
        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_SET:
        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_COPY:
        case H5FD_FILE_IMAGE_OP_PROPERTY_LIST_GET:
        case H5FD_FILE_IMAGE_OP_FILE_OPEN:
        case H5FD_FILE_IMAGE_OP_FILE_RESIZE:
        default:
            goto out;
    } /* end switch */

    return (SUCCEED);

out:
    return (FAIL);
} /* end image_free() */

/*-------------------------------------------------------------------------
 * Function: udata_copy
 *
 * Purpose: Simulates the copying of the user data structure utilized in the
 *          management of the "copying" of file images.
 *
 * Return: Address of "newly allocated" structure, if successful. Otherwise, it
 *         returns NULL.
 *
 *-------------------------------------------------------------------------
 */
static void *
udata_copy(void *_udata)
{
    H5LT_file_image_ud_t *udata = (H5LT_file_image_ud_t *)_udata;

    /* callback is only used if the application buffer is not actually copied */
    if (!(udata->flags & H5LT_FILE_IMAGE_DONT_COPY))
        goto out;
    if (udata->ref_count == 0)
        goto out;

    udata->ref_count++;

    return (udata);

out:
    return NULL;
} /* end udata_copy */

/*-------------------------------------------------------------------------
 * Function: udata_free
 *
 * Purpose: Simulates deallocation of the user data structure utilized in the
 *          management of the "copying" of file images. The data structure is
 *          actually deallocated when there are no outstanding references.
 *
 * Return: SUCCEED or FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
udata_free(void *_udata)
{
    H5LT_file_image_ud_t *udata = (H5LT_file_image_ud_t *)_udata;

    /* callback is only used if the application buffer is not actually copied */
    if (!(udata->flags & H5LT_FILE_IMAGE_DONT_COPY))
        goto out;
    if (udata->ref_count == 0)
        goto out;

    udata->ref_count--;

    /* checks that there are no references outstanding before deallocating udata */
    if (udata->ref_count == 0 && udata->fapl_ref_count == 0 && udata->vfd_ref_count == 0)
        free(udata);

    return (SUCCEED);

out:
    return (FAIL);
} /* end udata_free */

/* End of callbacks definitions for file image operations */

/*-------------------------------------------------------------------------
 *
 * internal functions
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5LT_get_attribute_mem(hid_t loc_id, const char *obj_name, const char *attr_name,
                                     hid_t mem_type_id, void *data);

/*-------------------------------------------------------------------------
 * Function: H5LT_make_dataset
 *
 * Purpose: Creates and writes a dataset of a type tid
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

static herr_t
H5LT_make_dataset_numerical(hid_t loc_id, const char *dset_name, int rank, const hsize_t *dims, hid_t tid,
                            const void *data)
{
    hid_t did = -1, sid = -1;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* Create the data space for the dataset. */
    if ((sid = H5Screate_simple(rank, dims, NULL)) < 0)
        return -1;

    /* Create the dataset. */
    if ((did = H5Dcreate2(loc_id, dset_name, tid, sid, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        goto out;

    /* Write the dataset only if there is data to write */
    if (data)
        if (H5Dwrite(did, tid, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0)
            goto out;

    /* End access to the dataset and release resources used by it. */
    if (H5Dclose(did) < 0)
        return -1;

    /* Terminate access to the data space. */
    if (H5Sclose(sid) < 0)
        return -1;

    return 0;

out:
    H5E_BEGIN_TRY
    {
        H5Dclose(did);
        H5Sclose(sid);
    }
    H5E_END_TRY
    return -1;
}

/*-------------------------------------------------------------------------
 *
 * Public functions
 *
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 * Function: H5LTmake_dataset
 *
 * Purpose: Creates and writes a dataset of a type tid
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTmake_dataset(hid_t loc_id, const char *dset_name, int rank, const hsize_t *dims, hid_t tid,
                 const void *data)
{
    return (H5LT_make_dataset_numerical(loc_id, dset_name, rank, dims, tid, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTmake_dataset_char
 *
 * Purpose: Creates and writes a dataset of H5T_NATIVE_CHAR type
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTmake_dataset_char(hid_t loc_id, const char *dset_name, int rank, const hsize_t *dims, const char *data)
{
    return (H5LT_make_dataset_numerical(loc_id, dset_name, rank, dims, H5T_NATIVE_CHAR, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTmake_dataset_short
 *
 * Purpose: Creates and writes a dataset of H5T_NATIVE_SHORT type
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTmake_dataset_short(hid_t loc_id, const char *dset_name, int rank, const hsize_t *dims, const short *data)
{
    return (H5LT_make_dataset_numerical(loc_id, dset_name, rank, dims, H5T_NATIVE_SHORT, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTmake_dataset_int
 *
 * Purpose: Creates and writes a dataset of H5T_NATIVE_INT type
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTmake_dataset_int(hid_t loc_id, const char *dset_name, int rank, const hsize_t *dims, const int *data)
{
    return (H5LT_make_dataset_numerical(loc_id, dset_name, rank, dims, H5T_NATIVE_INT, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTmake_dataset_long
 *
 * Purpose: Creates and writes a dataset of H5T_NATIVE_LONG type
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTmake_dataset_long(hid_t loc_id, const char *dset_name, int rank, const hsize_t *dims, const long *data)
{
    return (H5LT_make_dataset_numerical(loc_id, dset_name, rank, dims, H5T_NATIVE_LONG, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTmake_dataset_float
 *
 * Purpose: Creates and writes a dataset of H5T_NATIVE_FLOAT type
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTmake_dataset_float(hid_t loc_id, const char *dset_name, int rank, const hsize_t *dims, const float *data)
{
    return (H5LT_make_dataset_numerical(loc_id, dset_name, rank, dims, H5T_NATIVE_FLOAT, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTmake_dataset_double
 *
 * Purpose: Creates and writes a dataset of H5T_NATIVE_DOUBLE type
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTmake_dataset_double(hid_t loc_id, const char *dset_name, int rank, const hsize_t *dims,
                        const double *data)
{
    return (H5LT_make_dataset_numerical(loc_id, dset_name, rank, dims, H5T_NATIVE_DOUBLE, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTmake_dataset_string
 *
 * Purpose: Creates and writes a dataset of H5T_C_S1 type
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTmake_dataset_string(hid_t loc_id, const char *dset_name, const char *buf)
{
    hid_t  did = -1;
    hid_t  sid = -1;
    hid_t  tid = -1;
    size_t size;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* create a string data type */
    if ((tid = H5Tcopy(H5T_C_S1)) < 0)
        goto out;

    size = strlen(buf) + 1; /* extra null term */

    if (H5Tset_size(tid, size) < 0)
        goto out;

    if (H5Tset_strpad(tid, H5T_STR_NULLTERM) < 0)
        goto out;

    /* Create the data space for the dataset. */
    if ((sid = H5Screate(H5S_SCALAR)) < 0)
        goto out;

    /* Create the dataset. */
    if ((did = H5Dcreate2(loc_id, dset_name, tid, sid, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        goto out;

    /* Write the dataset only if there is data to write */
    if (buf)
        if (H5Dwrite(did, tid, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0)
            goto out;

    /* close*/
    if (H5Dclose(did) < 0)
        return -1;
    if (H5Sclose(sid) < 0)
        return -1;
    if (H5Tclose(tid) < 0)
        goto out;

    return 0;

out:
    H5E_BEGIN_TRY
    {
        H5Dclose(did);
        H5Tclose(tid);
        H5Sclose(sid);
    }
    H5E_END_TRY
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5LTopen_file_image
 *
 * Purpose: Open a user supplied file image using the core file driver.
 *
 * Return: File identifier, Failure: -1
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5LTopen_file_image(void *buf_ptr, size_t buf_size, unsigned flags)
{
    hid_t    fapl = -1, file_id = -1; /* HDF5 identifiers */
    unsigned file_open_flags;         /* Flags for image open */
    char     file_name[64];           /* Filename buffer */
    size_t   alloc_incr;              /* Buffer allocation increment */
    size_t   min_incr  = 65536;       /* Minimum buffer increment */
    double   buf_prcnt = 0.1;         /* Percentage of buffer size to set
                                          as increment */
    static long                 file_name_counter;
    H5FD_file_image_callbacks_t callbacks = {&image_malloc, &image_memcpy, &image_realloc, &image_free,
                                             &udata_copy,   &udata_free,   (void *)NULL};

    /* check arguments */
    if (buf_ptr == NULL)
        goto out;
    if (buf_size == 0)
        goto out;
    if (flags & (unsigned)~(H5LT_FILE_IMAGE_ALL))
        goto out;

    /* Create FAPL to transmit file image */
    if ((fapl = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        goto out;

    /* set allocation increment to a percentage of the supplied buffer size, or
     * a pre-defined minimum increment value, whichever is larger
     */
    if ((size_t)(buf_prcnt * (double)buf_size) > min_incr)
        alloc_incr = (size_t)(buf_prcnt * (double)buf_size);
    else
        alloc_incr = min_incr;

    /* Configure FAPL to use the core file driver */
    if (H5Pset_fapl_core(fapl, alloc_incr, false) < 0)
        goto out;

    /* Set callbacks for file image ops ONLY if the file image is NOT copied */
    if (flags & H5LT_FILE_IMAGE_DONT_COPY) {
        H5LT_file_image_ud_t *udata; /* Pointer to udata structure */

        /* Allocate buffer to communicate user data to callbacks */
        if (NULL == (udata = (H5LT_file_image_ud_t *)malloc(sizeof(H5LT_file_image_ud_t))))
            goto out;

        /* Initialize udata with info about app buffer containing file image  and flags */
        udata->app_image_ptr   = buf_ptr;
        udata->app_image_size  = buf_size;
        udata->fapl_image_ptr  = NULL;
        udata->fapl_image_size = 0;
        udata->fapl_ref_count  = 0;
        udata->vfd_image_ptr   = NULL;
        udata->vfd_image_size  = 0;
        udata->vfd_ref_count   = 0;
        udata->flags           = flags;
        udata->ref_count       = 1; /* corresponding to the first FAPL */

        /* copy address of udata into callbacks */
        callbacks.udata = (void *)udata;

        /* Set file image callbacks */
        if (H5Pset_file_image_callbacks(fapl, &callbacks) < 0) {
            free(udata);
            goto out;
        } /* end if */
    }     /* end if */

    /* Assign file image in user buffer to FAPL */
    if (H5Pset_file_image(fapl, buf_ptr, buf_size) < 0)
        goto out;

    /* set file open flags */
    if (flags & H5LT_FILE_IMAGE_OPEN_RW)
        file_open_flags = H5F_ACC_RDWR;
    else
        file_open_flags = H5F_ACC_RDONLY;

    /* define a unique file name */
    snprintf(file_name, (sizeof(file_name) - 1), "file_image_%ld", file_name_counter++);

    /* Assign file image in FAPL to the core file driver */
    if ((file_id = H5Fopen(file_name, file_open_flags, fapl)) < 0)
        goto out;

    /* Close FAPL */
    if (H5Pclose(fapl) < 0)
        goto out;

    /* Return file identifier */
    return file_id;

out:
    H5E_BEGIN_TRY
    {
        H5Pclose(fapl);
    }
    H5E_END_TRY
    return -1;
} /* end H5LTopen_file_image() */

/*-------------------------------------------------------------------------
 * Function: H5LT_read_dataset
 *
 * Purpose: Reads a dataset from disk.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

static herr_t
H5LT_read_dataset_numerical(hid_t loc_id, const char *dset_name, hid_t tid, void *data)
{
    hid_t did;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, dset_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Read */
    if (H5Dread(did, tid, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0)
        goto out;

    /* End access to the dataset and release resources used by it. */
    if (H5Dclose(did))
        return -1;

    return 0;

out:
    H5Dclose(did);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5LTread_dataset
 *
 * Purpose: Reads a dataset from disk.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTread_dataset(hid_t loc_id, const char *dset_name, hid_t tid, void *data)
{
    return (H5LT_read_dataset_numerical(loc_id, dset_name, tid, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTread_dataset_char
 *
 * Purpose: Reads a dataset from disk.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTread_dataset_char(hid_t loc_id, const char *dset_name, char *data)
{
    return (H5LT_read_dataset_numerical(loc_id, dset_name, H5T_NATIVE_CHAR, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTread_dataset_short
 *
 * Purpose: Reads a dataset from disk.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTread_dataset_short(hid_t loc_id, const char *dset_name, short *data)
{
    return (H5LT_read_dataset_numerical(loc_id, dset_name, H5T_NATIVE_SHORT, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTread_dataset_int
 *
 * Purpose: Reads a dataset from disk.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTread_dataset_int(hid_t loc_id, const char *dset_name, int *data)
{
    return (H5LT_read_dataset_numerical(loc_id, dset_name, H5T_NATIVE_INT, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTread_dataset_long
 *
 * Purpose: Reads a dataset from disk.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTread_dataset_long(hid_t loc_id, const char *dset_name, long *data)
{
    return (H5LT_read_dataset_numerical(loc_id, dset_name, H5T_NATIVE_LONG, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTread_dataset_float
 *
 * Purpose: Reads a dataset from disk.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTread_dataset_float(hid_t loc_id, const char *dset_name, float *data)
{
    return (H5LT_read_dataset_numerical(loc_id, dset_name, H5T_NATIVE_FLOAT, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTread_dataset_double
 *
 * Purpose: Reads a dataset from disk.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTread_dataset_double(hid_t loc_id, const char *dset_name, double *data)
{
    return (H5LT_read_dataset_numerical(loc_id, dset_name, H5T_NATIVE_DOUBLE, data));
}

/*-------------------------------------------------------------------------
 * Function: H5LTread_dataset_string
 *
 * Purpose: Reads a dataset
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTread_dataset_string(hid_t loc_id, const char *dset_name, char *buf)
{
    hid_t did = -1;
    hid_t tid = -1;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, dset_name, H5P_DEFAULT)) < 0)
        return -1;

    if ((tid = H5Dget_type(did)) < 0)
        goto out;

    /* Read */
    if (H5Dread(did, tid, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0)
        goto out;

    /* close */
    if (H5Dclose(did))
        goto out;
    if (H5Tclose(tid))
        return -1;

    return 0;

out:
    H5E_BEGIN_TRY
    {
        H5Dclose(did);
        H5Tclose(tid);
    }
    H5E_END_TRY
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_dataset_ndims
 *
 * Purpose: Gets the dimensionality of a dataset.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTget_dataset_ndims(hid_t loc_id, const char *dset_name, int *rank)
{
    hid_t did = -1;
    hid_t sid = -1;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, dset_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Get the dataspace handle */
    if ((sid = H5Dget_space(did)) < 0)
        goto out;

    /* Get rank */
    if ((*rank = H5Sget_simple_extent_ndims(sid)) < 0)
        goto out;

    /* Terminate access to the dataspace */
    if (H5Sclose(sid) < 0)
        goto out;

    /* End access to the dataset */
    if (H5Dclose(did))
        return -1;

    return 0;

out:
    H5E_BEGIN_TRY
    {
        H5Dclose(did);
        H5Sclose(sid);
    }
    H5E_END_TRY
    return -1;
}

/*-------------------------------------------------------------------------
 * Function:    H5LTget_dataset_info
 *
 * Purpose:     Gets information about a dataset.
 *
 * Return:      Success: 0, Failure: -1
 *-------------------------------------------------------------------------
 */

herr_t
H5LTget_dataset_info(hid_t loc_id, const char *dset_name, hsize_t *dims, H5T_class_t *type_class,
                     size_t *type_size)
{
    hid_t did = -1;
    hid_t tid = -1;
    hid_t sid = -1;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* open the dataset. */
    if ((did = H5Dopen2(loc_id, dset_name, H5P_DEFAULT)) < 0)
        return -1;

    /* get an identifier for the datatype. */
    tid = H5Dget_type(did);

    /* get the class. */
    if (type_class != NULL)
        *type_class = H5Tget_class(tid);

    /* get the size. */
    if (type_size != NULL)
        *type_size = H5Tget_size(tid);

    if (dims != NULL) {
        /* get the dataspace handle */
        if ((sid = H5Dget_space(did)) < 0)
            goto out;

        /* get dimensions */
        if (H5Sget_simple_extent_dims(sid, dims, NULL) < 0)
            goto out;

        /* terminate access to the dataspace */
        if (H5Sclose(sid) < 0)
            goto out;
    } /* end if */

    /* release the datatype. */
    if (H5Tclose(tid))
        return -1;

    /* end access to the dataset */
    if (H5Dclose(did))
        return -1;

    return 0;

out:
    H5E_BEGIN_TRY
    {
        H5Tclose(tid);
        H5Sclose(sid);
        H5Dclose(did);
    }
    H5E_END_TRY
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: find_dataset
 *
 * Purpose: operator function used by H5LTfind_dataset
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

static herr_t
find_dataset(H5_ATTR_UNUSED hid_t loc_id, const char *name, H5_ATTR_UNUSED const H5L_info2_t *linfo,
             void *op_data)
{
    /* Define a default zero value for return. This will cause the iterator to continue if
     * the dataset is not found yet.
     */
    int ret = 0;

    /* check the arguments */
    if (name == NULL)
        return ret;

    /* Shut the compiler up */
    (void)loc_id;
    (void)linfo;

    /* Define a positive value for return value if the dataset was found. This will
     * cause the iterator to immediately return that positive value,
     * indicating short-circuit success
     */
    if (strncmp(name, (char *)op_data, strlen((char *)op_data)) == 0)
        ret = 1;

    return ret;
}

/*-------------------------------------------------------------------------
 * Function: H5LTfind_dataset
 *
 * Purpose:  Inquires if a dataset named dset_name exists attached
 *           to the object loc_id.
 *
 * Return:
 *     Success: The return value of the first operator that
 *              returns non-zero, or zero if all members were
 *              processed with no operator returning non-zero.
 *
 *      Failure:    Negative if something goes wrong within the
 *              library, or the negative value returned by one
 *              of the operators.
 *
 *-------------------------------------------------------------------------
 */
/* H5Literate wants a non-const pointer but we have a const pointer in the API
 * call. It's safe to ignore this because we control the callback, don't
 * modify the op_data buffer (i.e.: dset_name) during the traversal, and the
 * library never modifies that buffer.
 */
H5_GCC_CLANG_DIAG_OFF("cast-qual")
herr_t
H5LTfind_dataset(hid_t loc_id, const char *dset_name)
{
    return H5Literate2(loc_id, H5_INDEX_NAME, H5_ITER_INC, 0, find_dataset, (void *)dset_name);
}
H5_GCC_CLANG_DIAG_ON("cast-qual")

/*-------------------------------------------------------------------------
 *
 * Set attribute functions
 *
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_string
 *
 * Purpose: Creates and writes a string attribute named attr_name and attaches
 *          it to the object specified by the name obj_name.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments: If the attribute already exists, it is overwritten
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTset_attribute_string(hid_t loc_id, const char *obj_name, const char *attr_name, const char *attr_data)
{
    hid_t  attr_type;
    hid_t  attr_space_id;
    hid_t  attr_id;
    hid_t  obj_id;
    htri_t has_attr;
    size_t attr_size;

    /* check the arguments */
    if (obj_name == NULL)
        return -1;
    if (attr_name == NULL)
        return -1;
    if (attr_data == NULL)
        return -1;

    /* Open the object */
    if ((obj_id = H5Oopen(loc_id, obj_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Create the attribute */
    if ((attr_type = H5Tcopy(H5T_C_S1)) < 0)
        goto out;

    attr_size = strlen(attr_data) + 1; /* extra null term */

    if (H5Tset_size(attr_type, (size_t)attr_size) < 0)
        goto out;

    if (H5Tset_strpad(attr_type, H5T_STR_NULLTERM) < 0)
        goto out;

    if ((attr_space_id = H5Screate(H5S_SCALAR)) < 0)
        goto out;

    /* Delete the attribute if it already exists */
    if ((has_attr = H5Aexists(obj_id, attr_name)) < 0)
        goto out;
    if (has_attr > 0)
        if (H5Adelete(obj_id, attr_name) < 0)
            goto out;

    /* Create and write the attribute */

    if ((attr_id = H5Acreate2(obj_id, attr_name, attr_type, attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        goto out;

    if (H5Awrite(attr_id, attr_type, attr_data) < 0)
        goto out;

    if (H5Aclose(attr_id) < 0)
        goto out;

    if (H5Sclose(attr_space_id) < 0)
        goto out;

    if (H5Tclose(attr_type) < 0)
        goto out;

    /* Close the object */
    if (H5Oclose(obj_id) < 0)
        return -1;

    return 0;

out:

    H5Oclose(obj_id);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5LT_set_attribute_numerical
 *
 * Purpose: Private function used by H5LTset_attribute_int and H5LTset_attribute_float
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LT_set_attribute_numerical(hid_t loc_id, const char *obj_name, const char *attr_name, size_t size,
                             hid_t tid, const void *data)
{

    hid_t   obj_id, sid, attr_id;
    hsize_t dim_size = size;
    htri_t  has_attr;

    /* check the arguments */
    if (obj_name == NULL)
        return -1;
    if (attr_name == NULL)
        return -1;

    /* Open the object */
    if ((obj_id = H5Oopen(loc_id, obj_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Create the data space for the attribute. */
    if ((sid = H5Screate_simple(1, &dim_size, NULL)) < 0)
        goto out;

    /* Delete the attribute if it already exists */
    if ((has_attr = H5Aexists(obj_id, attr_name)) < 0)
        goto out;
    if (has_attr > 0)
        if (H5Adelete(obj_id, attr_name) < 0)
            goto out;

    /* Create the attribute. */
    if ((attr_id = H5Acreate2(obj_id, attr_name, tid, sid, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        goto out;

    /* Write the attribute data. */
    if (H5Awrite(attr_id, tid, data) < 0)
        goto out;

    /* Close the attribute. */
    if (H5Aclose(attr_id) < 0)
        goto out;

    /* Close the dataspace. */
    if (H5Sclose(sid) < 0)
        goto out;

    /* Close the object */
    if (H5Oclose(obj_id) < 0)
        return -1;

    return 0;

out:
    H5Oclose(obj_id);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_char
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_char(hid_t loc_id, const char *obj_name, const char *attr_name, const char *data,
                       size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_CHAR, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_uchar
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_uchar(hid_t loc_id, const char *obj_name, const char *attr_name, const unsigned char *data,
                        size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_UCHAR, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_short
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_short(hid_t loc_id, const char *obj_name, const char *attr_name, const short *data,
                        size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_SHORT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_ushort
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_ushort(hid_t loc_id, const char *obj_name, const char *attr_name,
                         const unsigned short *data, size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_USHORT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_int
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_int(hid_t loc_id, const char *obj_name, const char *attr_name, const int *data, size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_INT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_uint
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_uint(hid_t loc_id, const char *obj_name, const char *attr_name, const unsigned int *data,
                       size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_UINT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_long
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_long(hid_t loc_id, const char *obj_name, const char *attr_name, const long *data,
                       size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_LONG, data) < 0)
        return -1;

    return 0;
}
/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_long_long
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments: This function was added to support attributes of type long long
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_long_long(hid_t loc_id, const char *obj_name, const char *attr_name, const long long *data,
                            size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_LLONG, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_ulong
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_ulong(hid_t loc_id, const char *obj_name, const char *attr_name, const unsigned long *data,
                        size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_ULONG, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_ullong
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTset_attribute_ullong(hid_t loc_id, const char *obj_name, const char *attr_name,
                         const unsigned long long *data, size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_ULLONG, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_float
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_float(hid_t loc_id, const char *obj_name, const char *attr_name, const float *data,
                        size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_FLOAT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTset_attribute_double
 *
 * Purpose: Create and write an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTset_attribute_double(hid_t loc_id, const char *obj_name, const char *attr_name, const double *data,
                         size_t size)
{

    if (H5LT_set_attribute_numerical(loc_id, obj_name, attr_name, size, H5T_NATIVE_DOUBLE, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTfind_attribute
 *
 * Purpose:  Checks if an attribute named attr_name exists attached to
 *           the object loc_id
 *
 * TODO:     Overloading herr_t is not a great idea. This function either
 *           needs to be rewritten to take a Boolean out parameter in
 *           HDF5 2.0 or possibly even eliminated entirely as it simply
 *           wraps H5Aexists.
 *
 * Return:   An htri_t value cast to herr_t
 *              Exists:         Positive
 *              Does not exist: 0
 *              Error:          Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5LTfind_attribute(hid_t loc_id, const char *attr_name)
{
    return (herr_t)H5Aexists(loc_id, attr_name);
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_ndims
 *
 * Purpose: Gets the dimensionality of an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTget_attribute_ndims(hid_t loc_id, const char *obj_name, const char *attr_name, int *rank)
{
    hid_t attr_id;
    hid_t sid;
    hid_t obj_id;

    /* check the arguments */
    if (obj_name == NULL)
        return -1;
    if (attr_name == NULL)
        return -1;

    /* Open the object */
    if ((obj_id = H5Oopen(loc_id, obj_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Open the attribute. */
    if ((attr_id = H5Aopen(obj_id, attr_name, H5P_DEFAULT)) < 0) {
        H5Oclose(obj_id);
        return -1;
    }

    /* Get the dataspace handle */
    if ((sid = H5Aget_space(attr_id)) < 0)
        goto out;

    /* Get rank */
    if ((*rank = H5Sget_simple_extent_ndims(sid)) < 0)
        goto out;

    /* Terminate access to the attribute */
    if (H5Sclose(sid) < 0)
        goto out;

    /* End access to the attribute */
    if (H5Aclose(attr_id))
        goto out;

    /* Close the object */
    if (H5Oclose(obj_id) < 0)
        return -1;

    return 0;

out:
    H5Aclose(attr_id);
    H5Oclose(obj_id);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_info
 *
 * Purpose: Gets information about an attribute.
 *
 * Return: Success: 0, Failure: -1
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTget_attribute_info(hid_t loc_id, const char *obj_name, const char *attr_name, hsize_t *dims,
                       H5T_class_t *type_class, size_t *type_size)
{
    hid_t attr_id;
    hid_t tid;
    hid_t sid;
    hid_t obj_id;

    /* check the arguments */
    if (obj_name == NULL)
        return -1;
    if (attr_name == NULL)
        return -1;

    /* Open the object */
    if ((obj_id = H5Oopen(loc_id, obj_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Open the attribute. */
    if ((attr_id = H5Aopen(obj_id, attr_name, H5P_DEFAULT)) < 0) {
        H5Oclose(obj_id);
        return -1;
    }

    /* Get an identifier for the datatype. */
    tid = H5Aget_type(attr_id);

    /* Get the class. */
    *type_class = H5Tget_class(tid);

    /* Get the size. */
    *type_size = H5Tget_size(tid);

    /* Get the dataspace handle */
    if ((sid = H5Aget_space(attr_id)) < 0)
        goto out;

    /* Get dimensions */
    if (H5Sget_simple_extent_dims(sid, dims, NULL) < 0)
        goto out;

    /* Terminate access to the dataspace */
    if (H5Sclose(sid) < 0)
        goto out;

    /* Release the datatype. */
    if (H5Tclose(tid))
        goto out;

    /* End access to the attribute */
    if (H5Aclose(attr_id))
        goto out;

    /* Close the object */
    if (H5Oclose(obj_id) < 0)
        return -1;

    return 0;

out:
    H5Tclose(tid);
    H5Aclose(attr_id);
    H5Oclose(obj_id);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5LTtext_to_dtype
 *
 * Purpose:  Convert DDL description to HDF5 data type.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5LTtext_to_dtype(const char *text, H5LT_lang_t lang_type)
{
    hid_t type_id;

    /* check the arguments */
    if (text == NULL)
        return -1;

    if (lang_type <= H5LT_LANG_ERR || lang_type >= H5LT_NO_LANG)
        goto out;

    if (lang_type != H5LT_DDL) {
        fprintf(stderr, "only DDL is supported for now.\n");
        goto out;
    }

    input_len = strlen(text);
    myinput   = strdup(text);

    if ((type_id = H5LTyyparse()) < 0) {
        free(myinput);
        goto out;
    }

    free(myinput);
    input_len = 0;

    return type_id;

out:
    return -1;
}

/*-------------------------------------------------------------------------
 * Function:    realloc_and_append
 *
 * Purpose:     Expand the buffer and append a string to it.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
static char *
realloc_and_append(bool _no_user_buf, size_t *len, char *buf, const char *str_to_add)
{
    size_t size_str_to_add, size_str;

    if (_no_user_buf) {
        char *tmp_realloc;

        if (!buf)
            goto out;

        /* If the buffer isn't big enough, reallocate it.  Otherwise, go to do strcat. */
        if (str_to_add && ((ssize_t)(*len - (strlen(buf) + strlen(str_to_add) + 1)) < LIMIT)) {
            *len += ((strlen(buf) + strlen(str_to_add) + 1) / INCREMENT + 1) * INCREMENT;
        }
        else if (!str_to_add && ((ssize_t)(*len - strlen(buf) - 1) < LIMIT)) {
            *len += INCREMENT;
        }

        tmp_realloc = (char *)realloc(buf, *len);
        if (tmp_realloc == NULL) {
            free(buf);
            buf = NULL;
            goto out;
        }
        else
            buf = tmp_realloc;
    }

    if (str_to_add) {
        /* find the size of the buffer to add */
        size_str_to_add = strlen(str_to_add);
        /* find the size of the current buffer */
        size_str = strlen(buf);

        /* Check to make sure the appended string does not
         * extend past the allocated buffer; if it does then truncate the string
         */
        if (size_str < *len - 1) {
            if (size_str + size_str_to_add < *len - 1) {
                strcat(buf, str_to_add);
            }
            else {
                strncat(buf, str_to_add, (*len - 1) - size_str);
            }
        }
        else {
            buf[*len - 1] = '\0'; /* buffer is full, null terminate */
        }
    }

    return buf;

out:
    return NULL;
}

/*-------------------------------------------------------------------------
 * Function:    indentation
 *
 * Purpose:     Print spaces for indentation
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
static char *
indentation(size_t x, char *str, bool no_u_buf, size_t *s_len)
{
    char tmp_str[TMP_LEN];

    if (x < 80) {
        memset(tmp_str, ' ', x);
        tmp_str[x] = '\0';
    }
    else
        snprintf(tmp_str, TMP_LEN, "error: the indentation exceeds the number of cols.");

    if (!(str = realloc_and_append(no_u_buf, s_len, str, tmp_str)))
        goto out;

    return str;

out:
    return NULL;
}

/*-------------------------------------------------------------------------
 * Function:    print_enum
 *
 * Purpose:     prints the enum data
 *
 * Return:      Success: 0, Failure: -1
 *
 *-----------------------------------------------------------------------*/
static char *
print_enum(hid_t type, char *str, size_t *str_len, bool no_ubuf, size_t indt)
{
    char         **name  = NULL; /*member names                   */
    unsigned char *value = NULL; /*value array                    */
    int            nmembs;       /*number of members              */
    char           tmp_str[TMP_LEN];
    int            nchars;      /*number of output characters    */
    hid_t          super  = -1; /*enum base integer type         */
    hid_t          native = -1; /*native integer data type       */
    size_t         super_size;  /*enum base type size            */
    size_t         dst_size;    /*destination value type size    */
    int            i;

    if ((nmembs = H5Tget_nmembers(type)) <= 0)
        goto out;

    if ((super = H5Tget_super(type)) < 0)
        goto out;

    /* Use buffer of INT or UNSIGNED INT to print enum values because
     * we don't expect these values to be so big that INT or UNSIGNED
     * INT can't hold.
     */
    if (H5T_SGN_NONE == H5Tget_sign(super)) {
        native = H5T_NATIVE_UINT;
    }
    else {
        native = H5T_NATIVE_INT;
    }

    super_size = H5Tget_size(super);
    dst_size   = H5Tget_size(native);

    /* Get the names and raw values of all members */
    name  = (char **)calloc((size_t)nmembs, sizeof(char *));
    value = (unsigned char *)calloc((size_t)nmembs, MAX(dst_size, super_size));

    for (i = 0; i < nmembs; i++) {
        if ((name[i] = H5Tget_member_name(type, (unsigned)i)) == NULL)
            goto out;
        if (H5Tget_member_value(type, (unsigned)i, value + (size_t)i * super_size) < 0)
            goto out;
    }

    /* Convert values to native data type */
    if (native > 0) {
        if (H5Tconvert(super, native, (size_t)nmembs, value, NULL, H5P_DEFAULT) < 0)
            goto out;
    }

    /*
     * Sort members by increasing value
     *    ***not implemented yet***
     */

    /* Print members */
    for (i = 0; i < nmembs; i++) {
        if (!(str = indentation(indt + COL, str, no_ubuf, str_len)))
            goto out;
        nchars = snprintf(tmp_str, TMP_LEN, "\"%s\"", name[i]);
        if (!(str = realloc_and_append(no_ubuf, str_len, str, tmp_str)))
            goto out;
        memset(tmp_str, ' ', (size_t)MAX(3, 19 - nchars) + 1);
        tmp_str[MAX(3, 19 - nchars)] = '\0';
        if (!(str = realloc_and_append(no_ubuf, str_len, str, tmp_str)))
            goto out;

        if (H5T_SGN_NONE == H5Tget_sign(native))
            snprintf(tmp_str, TMP_LEN, "%u", *((unsigned int *)((void *)(value + (size_t)i * dst_size))));
        else
            snprintf(tmp_str, TMP_LEN, "%d", *((int *)((void *)(value + (size_t)i * dst_size))));
        if (!(str = realloc_and_append(no_ubuf, str_len, str, tmp_str)))
            goto out;

        snprintf(tmp_str, TMP_LEN, ";\n");
        if (!(str = realloc_and_append(no_ubuf, str_len, str, tmp_str)))
            goto out;
    }

    /* Release resources */
    for (i = 0; i < nmembs; i++)
        H5free_memory(name[i]);

    free(name);
    free(value);
    H5Tclose(super);

    return str;

out:

    if (0 == nmembs) {
        str = realloc_and_append(no_ubuf, str_len, str, "\n");
        assert((indt + 4) < TMP_LEN);
        memset(tmp_str, ' ', (indt + 4) + 1);
        tmp_str[(indt + 4)] = '\0';
        str                 = realloc_and_append(no_ubuf, str_len, str, tmp_str);
        str                 = realloc_and_append(no_ubuf, str_len, str, " <empty>");
    } /* end if */

    /* Release resources */
    if (name) {
        for (i = 0; i < nmembs; i++)
            if (name[i])
                free(name[i]);
        free(name);
    } /* end if */

    if (value)
        free(value);

    if (super >= 0)
        H5Tclose(super);

    return NULL;
}

/*-------------------------------------------------------------------------
 * Function:    H5LTdtype_to_text
 *
 * Purpose:     Convert HDF5 data type to DDL description.
 *
 * Return:      Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTdtype_to_text(hid_t dtype, char *str, H5LT_lang_t lang_type, size_t *len)
{
    size_t str_len  = INCREMENT;
    char  *text_str = NULL;
    herr_t ret      = SUCCEED;

    if (lang_type <= H5LT_LANG_ERR || lang_type >= H5LT_NO_LANG)
        goto out;

    if (len && !str) {
        text_str    = (char *)calloc(str_len, sizeof(char));
        text_str[0] = '\0';
        if (!(text_str = H5LT_dtype_to_text(dtype, text_str, lang_type, &str_len, 1)))
            goto out;
        *len = strlen(text_str) + 1;
        if (text_str)
            free(text_str);
        text_str = NULL;
    }
    else if (len && str) {
        if (!(H5LT_dtype_to_text(dtype, str, lang_type, len, 0)))
            goto out;
        str[*len - 1] = '\0';
    }

    return ret;

out:
    free(text_str);

    return FAIL;
}

/*-------------------------------------------------------------------------
 * Function:    H5LT_dtype_to_text
 *
 * Purpose:     Private function to convert HDF5 data type to DDL description.
 *
 * Return:      Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
char *
H5LT_dtype_to_text(hid_t dtype, char *dt_str, H5LT_lang_t lang, size_t *slen, bool no_user_buf)
{
    H5T_class_t tcls;
    char        tmp_str[TMP_LEN];
    int         i;

    if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, NULL)))
        goto out;

    if (lang != H5LT_DDL) {
        snprintf(dt_str, *slen, "only DDL is supported for now");
        goto out;
    }

    if ((tcls = H5Tget_class(dtype)) < 0)
        goto out;

    switch (tcls) {
        case H5T_INTEGER:
        case H5T_BITFIELD:
            if (H5Tequal(dtype, H5T_STD_I8BE)) {
                snprintf(dt_str, *slen, "H5T_STD_I8BE");
            }
            else if (H5Tequal(dtype, H5T_STD_I8LE)) {
                snprintf(dt_str, *slen, "H5T_STD_I8LE");
            }
            else if (H5Tequal(dtype, H5T_STD_I16BE)) {
                snprintf(dt_str, *slen, "H5T_STD_I16BE");
            }
            else if (H5Tequal(dtype, H5T_STD_I16LE)) {
                snprintf(dt_str, *slen, "H5T_STD_I16LE");
            }
            else if (H5Tequal(dtype, H5T_STD_I32BE)) {
                snprintf(dt_str, *slen, "H5T_STD_I32BE");
            }
            else if (H5Tequal(dtype, H5T_STD_I32LE)) {
                snprintf(dt_str, *slen, "H5T_STD_I32LE");
            }
            else if (H5Tequal(dtype, H5T_STD_I64BE)) {
                snprintf(dt_str, *slen, "H5T_STD_I64BE");
            }
            else if (H5Tequal(dtype, H5T_STD_I64LE)) {
                snprintf(dt_str, *slen, "H5T_STD_I64LE");
            }
            else if (H5Tequal(dtype, H5T_STD_U8BE)) {
                snprintf(dt_str, *slen, "H5T_STD_U8BE");
            }
            else if (H5Tequal(dtype, H5T_STD_U8LE)) {
                snprintf(dt_str, *slen, "H5T_STD_U8LE");
            }
            else if (H5Tequal(dtype, H5T_STD_U16BE)) {
                snprintf(dt_str, *slen, "H5T_STD_U16BE");
            }
            else if (H5Tequal(dtype, H5T_STD_U16LE)) {
                snprintf(dt_str, *slen, "H5T_STD_U16LE");
            }
            else if (H5Tequal(dtype, H5T_STD_U32BE)) {
                snprintf(dt_str, *slen, "H5T_STD_U32BE");
            }
            else if (H5Tequal(dtype, H5T_STD_U32LE)) {
                snprintf(dt_str, *slen, "H5T_STD_U32LE");
            }
            else if (H5Tequal(dtype, H5T_STD_U64BE)) {
                snprintf(dt_str, *slen, "H5T_STD_U64BE");
            }
            else if (H5Tequal(dtype, H5T_STD_U64LE)) {
                snprintf(dt_str, *slen, "H5T_STD_U64LE");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_SCHAR)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_SCHAR");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_UCHAR)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_UCHAR");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_SHORT)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_SHORT");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_USHORT)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_USHORT");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_INT)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_INT");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_UINT)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_UINT");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_LONG)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_LONG");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_ULONG)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_ULONG");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_LLONG)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_LLONG");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_ULLONG)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_ULLONG");
            }
            else {
                snprintf(dt_str, *slen, "undefined integer");
            }

            break;
        case H5T_FLOAT:
            if (H5Tequal(dtype, H5T_IEEE_F32BE)) {
                snprintf(dt_str, *slen, "H5T_IEEE_F32BE");
            }
            else if (H5Tequal(dtype, H5T_IEEE_F32LE)) {
                snprintf(dt_str, *slen, "H5T_IEEE_F32LE");
            }
            else if (H5Tequal(dtype, H5T_IEEE_F64BE)) {
                snprintf(dt_str, *slen, "H5T_IEEE_F64BE");
            }
            else if (H5Tequal(dtype, H5T_IEEE_F64LE)) {
                snprintf(dt_str, *slen, "H5T_IEEE_F64LE");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_FLOAT)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_FLOAT");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_DOUBLE)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_DOUBLE");
            }
            else if (H5Tequal(dtype, H5T_NATIVE_LDOUBLE)) {
                snprintf(dt_str, *slen, "H5T_NATIVE_LDOUBLE");
            }
            else {
                snprintf(dt_str, *slen, "undefined float");
            }

            break;
        case H5T_STRING: {
            /* Make a copy of type in memory in case when DTYPE is on disk, the size
             * will be bigger than in memory.  This makes it easier to compare
             * types in memory. */
            hid_t       str_type;
            H5T_order_t order;
            hid_t       tmp_type;
            size_t      size;
            H5T_str_t   str_pad;
            H5T_cset_t  cset;
            htri_t      is_vlstr;

            if ((tmp_type = H5Tcopy(dtype)) < 0)
                goto out;
            if ((size = H5Tget_size(tmp_type)) == 0)
                goto out;
            if ((str_pad = H5Tget_strpad(tmp_type)) < 0)
                goto out;
            if ((cset = H5Tget_cset(tmp_type)) < 0)
                goto out;
            if ((is_vlstr = H5Tis_variable_str(tmp_type)) < 0)
                goto out;

            /* Print lead-in */
            snprintf(dt_str, *slen, "H5T_STRING {\n");
            indent += COL;

            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;

            if (is_vlstr)
                snprintf(tmp_str, TMP_LEN, "STRSIZE H5T_VARIABLE;\n");
            else
                snprintf(tmp_str, TMP_LEN, "STRSIZE %d;\n", (int)size);

            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;

            if (str_pad == H5T_STR_NULLTERM)
                snprintf(tmp_str, TMP_LEN, "STRPAD H5T_STR_NULLTERM;\n");
            else if (str_pad == H5T_STR_NULLPAD)
                snprintf(tmp_str, TMP_LEN, "STRPAD H5T_STR_NULLPAD;\n");
            else if (str_pad == H5T_STR_SPACEPAD)
                snprintf(tmp_str, TMP_LEN, "STRPAD H5T_STR_SPACEPAD;\n");
            else
                snprintf(tmp_str, TMP_LEN, "STRPAD H5T_STR_ERROR;\n");

            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;

            if (cset == H5T_CSET_ASCII)
                snprintf(tmp_str, TMP_LEN, "CSET H5T_CSET_ASCII;\n");
            else if (cset == H5T_CSET_UTF8)
                snprintf(tmp_str, TMP_LEN, "CSET H5T_CSET_UTF8;\n");
            else
                snprintf(tmp_str, TMP_LEN, "CSET unknown;\n");

            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            /* Reproduce a C type string */
            if ((str_type = H5Tcopy(H5T_C_S1)) < 0)
                goto out;
            if (is_vlstr) {
                if (H5Tset_size(str_type, H5T_VARIABLE) < 0)
                    goto out;
            }
            else {
                if (H5Tset_size(str_type, size) < 0)
                    goto out;
            }
            if (H5Tset_cset(str_type, cset) < 0)
                goto out;
            if (H5Tset_strpad(str_type, str_pad) < 0)
                goto out;

            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;

            /* Check C variable-length string first. Are the two types equal? */
            if (H5Tequal(tmp_type, str_type)) {
                snprintf(tmp_str, TMP_LEN, "CTYPE H5T_C_S1;\n");
                if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                    goto out;
                goto next;
            }

            /* Change the endianness and see if they're equal. */
            if ((order = H5Tget_order(tmp_type)) < 0)
                goto out;
            if (order == H5T_ORDER_LE) {
                if (H5Tset_order(str_type, H5T_ORDER_LE) < 0)
                    goto out;
            }
            else if (order == H5T_ORDER_BE) {
                if (H5Tset_order(str_type, H5T_ORDER_BE) < 0)
                    goto out;
            }

            if (H5Tequal(tmp_type, str_type)) {
                snprintf(tmp_str, TMP_LEN, "CTYPE H5T_C_S1;\n");
                if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                    goto out;
                goto next;
            }

            /* If not equal to C variable-length string, check Fortran type.
             * Actually H5Tequal can't tell difference between H5T_C_S1 and
             * H5T_FORTRAN_S1!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! */
            if (H5Tclose(str_type) < 0)
                goto out;
            if ((str_type = H5Tcopy(H5T_FORTRAN_S1)) < 0)
                goto out;
            if (H5Tset_cset(str_type, cset) < 0)
                goto out;
            if (H5Tset_size(str_type, size) < 0)
                goto out;
            if (H5Tset_strpad(str_type, str_pad) < 0)
                goto out;

            /* Are the two types equal? */
            if (H5Tequal(tmp_type, str_type)) {
                snprintf(tmp_str, TMP_LEN, "CTYPE H5T_FORTRAN_S1;\n");
                if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                    goto out;
                goto next;
            }

            /* Change the endianness and see if they're equal. */
            if ((order = H5Tget_order(tmp_type)) < 0)
                goto out;
            if (order == H5T_ORDER_LE) {
                if (H5Tset_order(str_type, H5T_ORDER_LE) < 0)
                    goto out;
            }
            else if (order == H5T_ORDER_BE) {
                if (H5Tset_order(str_type, H5T_ORDER_BE) < 0)
                    goto out;
            }

            /* Are the two types equal? */
            if (H5Tequal(tmp_type, str_type)) {
                snprintf(tmp_str, TMP_LEN, "CTYPE H5T_FORTRAN_S1;\n");
                if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                    goto out;
                goto next;
            }

            /* Type doesn't match any of above. */
            snprintf(tmp_str, TMP_LEN, "CTYPE unknown_one_character_type;\n");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

next:
            H5Tclose(str_type);
            H5Tclose(tmp_type);

            /* Print closing */
            indent -= COL;
            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;
            snprintf(tmp_str, TMP_LEN, "}");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            break;
        }
        case H5T_OPAQUE: {
            char *tag = NULL;

            /* Print lead-in */
            snprintf(dt_str, *slen, "H5T_OPAQUE {\n");
            indent += COL;

            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;
            snprintf(tmp_str, TMP_LEN, "OPQ_SIZE %lu;\n", (unsigned long)H5Tget_size(dtype));
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;
            tag = H5Tget_tag(dtype);
            if (tag) {
                snprintf(tmp_str, TMP_LEN, "OPQ_TAG \"%s\";\n", tag);
                if (tag)
                    H5free_memory(tag);
                tag = NULL;
            }
            else
                snprintf(tmp_str, TMP_LEN, "OPQ_TAG \"\";\n");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            /* Print closing */
            indent -= COL;
            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;
            snprintf(tmp_str, TMP_LEN, "}");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            break;
        }
        case H5T_ENUM: {
            hid_t  super;
            size_t super_len;
            char  *stmp = NULL;

            /* Print lead-in */
            snprintf(dt_str, *slen, "H5T_ENUM {\n");
            indent += COL;
            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;

            if ((super = H5Tget_super(dtype)) < 0)
                goto out;
            if (H5LTdtype_to_text(super, NULL, lang, &super_len) < 0)
                goto out;
            stmp = (char *)calloc(super_len, sizeof(char));
            if (H5LTdtype_to_text(super, stmp, lang, &super_len) < 0) {
                free(stmp);
                goto out;
            }
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, stmp))) {
                free(stmp);
                goto out;
            }

            if (stmp)
                free(stmp);
            stmp = NULL;

            snprintf(tmp_str, TMP_LEN, ";\n");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;
            H5Tclose(super);

            if (!(dt_str = print_enum(dtype, dt_str, slen, no_user_buf, indent)))
                goto out;

            /* Print closing */
            indent -= COL;
            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;
            snprintf(tmp_str, TMP_LEN, "}");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            break;
        }
        case H5T_VLEN: {
            hid_t  super;
            size_t super_len;
            char  *stmp = NULL;

            /* Print lead-in */
            snprintf(dt_str, *slen, "H5T_VLEN {\n");
            indent += COL;
            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;

            if ((super = H5Tget_super(dtype)) < 0)
                goto out;
            if (H5LTdtype_to_text(super, NULL, lang, &super_len) < 0)
                goto out;
            stmp = (char *)calloc(super_len, sizeof(char));
            if (H5LTdtype_to_text(super, stmp, lang, &super_len) < 0) {
                free(stmp);
                goto out;
            }
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, stmp))) {
                free(stmp);
                goto out;
            }

            if (stmp)
                free(stmp);
            stmp = NULL;
            snprintf(tmp_str, TMP_LEN, "\n");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;
            H5Tclose(super);

            /* Print closing */
            indent -= COL;
            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;
            snprintf(tmp_str, TMP_LEN, "}");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            break;
        }
        case H5T_ARRAY: {
            hid_t   super;
            size_t  super_len;
            char   *stmp = NULL;
            hsize_t dims[H5S_MAX_RANK];
            int     ndims;

            /* Print lead-in */
            snprintf(dt_str, *slen, "H5T_ARRAY {\n");
            indent += COL;
            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;

            /* Get array information */
            if ((ndims = H5Tget_array_ndims(dtype)) < 0)
                goto out;
            if (H5Tget_array_dims2(dtype, dims) < 0)
                goto out;

            /* Print array dimensions */
            for (i = 0; i < ndims; i++) {
                snprintf(tmp_str, TMP_LEN, "[%d]", (int)dims[i]);
                if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                    goto out;
            }
            snprintf(tmp_str, TMP_LEN, " ");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            if ((super = H5Tget_super(dtype)) < 0)
                goto out;
            if (H5LTdtype_to_text(super, NULL, lang, &super_len) < 0)
                goto out;
            stmp = (char *)calloc(super_len, sizeof(char));
            if (H5LTdtype_to_text(super, stmp, lang, &super_len) < 0) {
                free(stmp);
                goto out;
            }
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, stmp))) {
                free(stmp);
                goto out;
            }
            if (stmp)
                free(stmp);
            stmp = NULL;
            snprintf(tmp_str, TMP_LEN, "\n");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;
            H5Tclose(super);

            /* Print closing */
            indent -= COL;
            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;
            snprintf(tmp_str, TMP_LEN, "}");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            break;
        }
        case H5T_COMPOUND: {
            char       *mname = NULL;
            hid_t       mtype;
            size_t      moffset;
            H5T_class_t mclass;
            size_t      mlen;
            char       *mtmp = NULL;
            int         nmembs;

            if ((nmembs = H5Tget_nmembers(dtype)) < 0)
                goto out;

            snprintf(dt_str, *slen, "H5T_COMPOUND {\n");
            indent += COL;

            for (i = 0; i < nmembs; i++) {
                if ((mname = H5Tget_member_name(dtype, (unsigned)i)) == NULL)
                    goto out;
                if ((mtype = H5Tget_member_type(dtype, (unsigned)i)) < 0)
                    goto out;
                moffset = H5Tget_member_offset(dtype, (unsigned)i);
                if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                    goto out;

                if ((mclass = H5Tget_class(mtype)) < 0)
                    goto out;
                if (H5T_COMPOUND == mclass)
                    indent += COL;

                if (H5LTdtype_to_text(mtype, NULL, lang, &mlen) < 0)
                    goto out;
                mtmp = (char *)calloc(mlen, sizeof(char));
                if (H5LTdtype_to_text(mtype, mtmp, lang, &mlen) < 0) {
                    free(mtmp);
                    goto out;
                }
                if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, mtmp))) {
                    free(mtmp);
                    goto out;
                }
                if (mtmp)
                    free(mtmp);
                mtmp = NULL;

                if (H5T_COMPOUND == mclass)
                    indent -= COL;

                snprintf(tmp_str, TMP_LEN, " \"%s\"", mname);
                if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                    goto out;
                if (mname)
                    H5free_memory(mname);
                mname = NULL;

                snprintf(tmp_str, TMP_LEN, " : %lu;\n", (unsigned long)moffset);
                if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                    goto out;
            }

            /* Print closing */
            indent -= COL;
            if (!(dt_str = indentation(indent + COL, dt_str, no_user_buf, slen)))
                goto out;
            snprintf(tmp_str, TMP_LEN, "}");
            if (!(dt_str = realloc_and_append(no_user_buf, slen, dt_str, tmp_str)))
                goto out;

            break;
        }
        case H5T_TIME:
            snprintf(dt_str, *slen, "H5T_TIME: not yet implemented");
            break;
        case H5T_NO_CLASS:
            snprintf(dt_str, *slen, "H5T_NO_CLASS");
            break;
        case H5T_REFERENCE:
            if (H5Tequal(dtype, H5T_STD_REF_DSETREG) == true) {
                snprintf(dt_str, *slen, " H5T_REFERENCE { H5T_STD_REF_DSETREG }");
            }
            else {
                snprintf(dt_str, *slen, " H5T_REFERENCE { H5T_STD_REF_OBJECT }");
            }
            break;
        case H5T_NCLASSES:
            break;
        default:
            snprintf(dt_str, *slen, "unknown data type");
    }

    return dt_str;

out:
    return NULL;
}

/*-------------------------------------------------------------------------
 *
 * Get attribute functions
 *
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_string
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTget_attribute_string(hid_t loc_id, const char *obj_name, const char *attr_name, char *data)
{
    /* identifiers */
    hid_t obj_id;

    /* check the arguments */
    if (obj_name == NULL)
        return -1;
    if (attr_name == NULL)
        return -1;

    /* Open the object */
    if ((obj_id = H5Oopen(loc_id, obj_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Get the attribute */
    if (H5LT_get_attribute_disk(obj_id, attr_name, data) < 0) {
        H5Oclose(obj_id);
        return -1;
    }

    /* Close the object */
    if (H5Oclose(obj_id) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_char
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_char(hid_t loc_id, const char *obj_name, const char *attr_name, char *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_CHAR, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_uchar
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_uchar(hid_t loc_id, const char *obj_name, const char *attr_name, unsigned char *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_UCHAR, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_short
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_short(hid_t loc_id, const char *obj_name, const char *attr_name, short *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_SHORT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_ushort
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_ushort(hid_t loc_id, const char *obj_name, const char *attr_name, unsigned short *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_USHORT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_int
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_int(hid_t loc_id, const char *obj_name, const char *attr_name, int *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_INT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_uint
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_uint(hid_t loc_id, const char *obj_name, const char *attr_name, unsigned int *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_UINT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_long
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_long(hid_t loc_id, const char *obj_name, const char *attr_name, long *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_LONG, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_long_long
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments: This function was added to support INTEGER*8 Fortran types
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_long_long(hid_t loc_id, const char *obj_name, const char *attr_name, long long *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_LLONG, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_ulong
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_ulong(hid_t loc_id, const char *obj_name, const char *attr_name, unsigned long *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_ULONG, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_ullong
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LTget_attribute_ullong(hid_t loc_id, const char *obj_name, const char *attr_name, unsigned long long *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_ULLONG, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_float
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTget_attribute_float(hid_t loc_id, const char *obj_name, const char *attr_name, float *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_FLOAT, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute_double
 *
 * Purpose: Reads an attribute named attr_name
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTget_attribute_double(hid_t loc_id, const char *obj_name, const char *attr_name, double *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, H5T_NATIVE_DOUBLE, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5LTget_attribute
 *
 * Purpose: Reads an attribute named attr_name with the memory type mem_type_id
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments: Private function
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LTget_attribute(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t mem_type_id, void *data)
{
    /* Get the attribute */
    if (H5LT_get_attribute_mem(loc_id, obj_name, attr_name, mem_type_id, data) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * private functions
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 * Function: H5LT_get_attribute_mem
 *
 * Purpose: Reads an attribute named attr_name with the memory type mem_type_id
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments: Private function
 *
 *-------------------------------------------------------------------------
 */

static herr_t
H5LT_get_attribute_mem(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t mem_type_id,
                       void *data)
{
    /* identifiers */
    hid_t obj_id  = -1;
    hid_t attr_id = -1;

    /* check the arguments */
    if (obj_name == NULL)
        return -1;
    if (attr_name == NULL)
        return -1;

    /* Open the object */
    if ((obj_id = H5Oopen(loc_id, obj_name, H5P_DEFAULT)) < 0)
        goto out;

    if ((attr_id = H5Aopen(obj_id, attr_name, H5P_DEFAULT)) < 0)
        goto out;

    if (H5Aread(attr_id, mem_type_id, data) < 0)
        goto out;

    if (H5Aclose(attr_id) < 0)
        goto out;
    attr_id = -1;

    /* Close the object */
    if (H5Oclose(obj_id) < 0)
        goto out;
    obj_id = -1;

    return 0;

out:
    if (obj_id > 0)
        H5Oclose(obj_id);
    if (attr_id > 0)
        H5Aclose(attr_id);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5LT_get_attribute_disk
 *
 * Purpose: Reads an attribute named attr_name with the datatype stored on disk
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5LT_get_attribute_disk(hid_t loc_id, const char *attr_name, void *attr_out)
{
    /* identifiers */
    hid_t attr_id;
    hid_t attr_type;

    if ((attr_id = H5Aopen(loc_id, attr_name, H5P_DEFAULT)) < 0)
        return -1;

    if ((attr_type = H5Aget_type(attr_id)) < 0)
        goto out;

    if (H5Aread(attr_id, attr_type, attr_out) < 0)
        goto out;

    if (H5Tclose(attr_type) < 0)
        goto out;

    if (H5Aclose(attr_id) < 0)
        return -1;

    return 0;

out:
    H5Tclose(attr_type);
    H5Aclose(attr_id);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5LT_set_attribute_string
 *
 * Purpose: creates and writes an attribute named NAME to the dataset DSET_ID
 *
 * Return: FAIL on error, SUCCESS on success
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LT_set_attribute_string(hid_t dset_id, const char *name, const char *buf)
{
    hid_t  tid;
    hid_t  sid = -1;
    hid_t  aid = -1;
    htri_t has_attr;
    size_t size;

    /* Delete the attribute if it already exists */
    if ((has_attr = H5Aexists(dset_id, name)) < 0)
        return FAIL;
    if (has_attr > 0)
        if (H5Adelete(dset_id, name) < 0)
            return FAIL;

    /*-------------------------------------------------------------------------
     * create the attribute type
     *-------------------------------------------------------------------------
     */
    if ((tid = H5Tcopy(H5T_C_S1)) < 0)
        return FAIL;

    size = strlen(buf) + 1; /* extra null term */

    if (H5Tset_size(tid, (size_t)size) < 0)
        goto out;

    if (H5Tset_strpad(tid, H5T_STR_NULLTERM) < 0)
        goto out;

    if ((sid = H5Screate(H5S_SCALAR)) < 0)
        goto out;

    /*-------------------------------------------------------------------------
     * create and write the attribute
     *-------------------------------------------------------------------------
     */
    if ((aid = H5Acreate2(dset_id, name, tid, sid, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        goto out;

    if (H5Awrite(aid, tid, buf) < 0)
        goto out;

    if (H5Aclose(aid) < 0)
        goto out;

    if (H5Sclose(sid) < 0)
        goto out;

    if (H5Tclose(tid) < 0)
        goto out;

    return SUCCEED;

    /* error zone */
out:
    H5E_BEGIN_TRY
    {
        H5Aclose(aid);
        H5Tclose(tid);
        H5Sclose(sid);
    }
    H5E_END_TRY
    return FAIL;
}

htri_t
H5LTpath_valid(hid_t loc_id, const char *path, hbool_t check_object_valid)
{
    char      *tmp_path = NULL; /* Temporary copy of the path */
    char      *curr_name;       /* Pointer to current component of path name */
    char      *delimit;         /* Pointer to path delimiter during traversal */
    H5I_type_t obj_type;
    htri_t     link_exists, obj_exists;
    size_t     path_length;
    htri_t     ret_value;

    /* Initialize */
    ret_value = false;

    /* check the arguments */
    if (path == NULL) {
        ret_value = FAIL;
        goto done;
    }

    /* Find the type of loc_id */
    if ((obj_type = H5Iget_type(loc_id)) == H5I_BADID) {
        ret_value = FAIL;
        goto done;
    }

    /* Find the length of the path */
    path_length = strlen(path);

    /* Check if the identifier is the object itself, i.e. path is '.' */
    if (strncmp(path, ".", path_length) == 0) {
        if (check_object_valid) {
            obj_exists = H5Oexists_by_name(loc_id, path, H5P_DEFAULT);
            ret_value  = obj_exists;
            goto done;
        }
        else {
            ret_value = true; /* Since the object is the identifier itself,
                               * we can only check if loc_id is a valid type */
            goto done;
        }
    }

    /* Duplicate the path to use */
    if (NULL == (tmp_path = strdup(path))) {
        ret_value = FAIL;
        goto done;
    }

    curr_name = tmp_path;

    /* check if absolute pathname */
    if (strncmp(path, "/", 1) == 0)
        curr_name++;

    /* check if relative path name starts with "./" */
    if (strncmp(path, "./", 2) == 0)
        curr_name += 2;

    while ((delimit = strchr(curr_name, '/')) != NULL) {
        /* Change the delimiter to terminate the string */
        *delimit = '\0';

        obj_exists = false;
        if ((link_exists = H5Lexists(loc_id, tmp_path, H5P_DEFAULT)) < 0) {
            ret_value = FAIL;
            goto done;
        }

        /* If target link does not exist then no reason to
         *  continue checking the path */
        if (link_exists != true) {
            ret_value = false;
            goto done;
        }

        /* Determine if link resolves to an actual object */
        if ((obj_exists = H5Oexists_by_name(loc_id, tmp_path, H5P_DEFAULT)) < 0) {
            ret_value = FAIL;
            goto done;
        }

        if (obj_exists != true)
            break;

        /* Change the delimiter back to '/' */
        *delimit = '/';

        /* Advance the pointer in the path to the start of the next component */
        curr_name = delimit + 1;

    } /* end while */

    /* Should be pointing to the last component in the path name now... */

    /* Check if link does not exist */
    if ((link_exists = H5Lexists(loc_id, tmp_path, H5P_DEFAULT)) < 0) {
        ret_value = FAIL;
    }
    else {
        ret_value = link_exists;
        /* Determine if link resolves to an actual object for check_object_valid true */
        if (check_object_valid == true && link_exists == true) {
            if ((obj_exists = H5Oexists_by_name(loc_id, tmp_path, H5P_DEFAULT)) < 0) {
                ret_value = FAIL;
            }
            else {
                ret_value = obj_exists;
            }
        }
    }

done:
    if (tmp_path != NULL)
        free(tmp_path);

    return ret_value;
}
