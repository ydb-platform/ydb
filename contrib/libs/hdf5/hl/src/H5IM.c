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

#include "H5IMprivate.h"
#include "H5LTprivate.h"

/*-------------------------------------------------------------------------
 * Function: H5IMmake_image_8bit
 *
 * Purpose: Creates and writes an image an 8 bit image
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMmake_image_8bit(hid_t loc_id, const char *dset_name, hsize_t width, hsize_t height,
                    const unsigned char *buf)
{
    hsize_t dims[IMAGE8_RANK];

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* Initialize the image dimensions */
    dims[0] = height;
    dims[1] = width;

    /* Make the dataset */
    if (H5LTmake_dataset(loc_id, dset_name, IMAGE8_RANK, dims, H5T_NATIVE_UCHAR, buf) < 0)
        return -1;

    /* Attach the CLASS attribute */
    if (H5LTset_attribute_string(loc_id, dset_name, "CLASS", IMAGE_CLASS) < 0)
        return -1;

    /* Attach the VERSION attribute */
    if (H5LTset_attribute_string(loc_id, dset_name, "IMAGE_VERSION", IMAGE_VERSION) < 0)
        return -1;

    /* Attach the IMAGE_SUBCLASS attribute */
    if (H5LTset_attribute_string(loc_id, dset_name, "IMAGE_SUBCLASS", "IMAGE_INDEXED") < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5IMmake_image_24bit
 *
 * Purpose:
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 * Interlace Mode Dimensions in the Dataspace
 * INTERLACE_PIXEL [height][width][pixel components]
 * INTERLACE_PLANE [pixel components][height][width]
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMmake_image_24bit(hid_t loc_id, const char *dset_name, hsize_t width, hsize_t height,
                     const char *interlace, const unsigned char *buf)
{
    hsize_t dims[IMAGE24_RANK];

    /* check the arguments */
    if (interlace == NULL)
        return -1;
    if (dset_name == NULL)
        return -1;

    /* Initialize the image dimensions */

    if (strncmp(interlace, "INTERLACE_PIXEL", 15) == 0) {
        /* Number of color planes is defined as the third dimension */
        dims[0] = height;
        dims[1] = width;
        dims[2] = IMAGE24_RANK;
    }
    else if (strncmp(interlace, "INTERLACE_PLANE", 15) == 0) {
        /* Number of color planes is defined as the first dimension */
        dims[0] = IMAGE24_RANK;
        dims[1] = height;
        dims[2] = width;
    }
    else
        return -1;

    /* Make the dataset */
    if (H5LTmake_dataset(loc_id, dset_name, IMAGE24_RANK, dims, H5T_NATIVE_UCHAR, buf) < 0)
        return -1;

    /* Attach the CLASS attribute */
    if (H5LTset_attribute_string(loc_id, dset_name, "CLASS", IMAGE_CLASS) < 0)
        return -1;

    /* Attach the VERSION attribute */
    if (H5LTset_attribute_string(loc_id, dset_name, "IMAGE_VERSION", IMAGE_VERSION) < 0)
        return -1;

    /* Attach the IMAGE_SUBCLASS attribute */
    if (H5LTset_attribute_string(loc_id, dset_name, "IMAGE_SUBCLASS", "IMAGE_TRUECOLOR") < 0)
        return -1;

    /* Attach the INTERLACE_MODE attribute. This attributes is only for true color images */
    if (H5LTset_attribute_string(loc_id, dset_name, "INTERLACE_MODE", interlace) < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: find_palette
 *
 * Purpose: operator function used by H5LT_find_palette
 *
 * Return:
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */
static herr_t
find_palette(H5_ATTR_UNUSED hid_t loc_id, const char *name, H5_ATTR_UNUSED const H5A_info_t *ainfo,
             H5_ATTR_UNUSED void *op_data)
{
    int ret = H5_ITER_CONT;

    /* check the arguments */
    if (name == NULL)
        return -1;

    /* Shut compiler up */
    (void)loc_id;
    (void)ainfo;
    (void)op_data;

    /* Define a positive value for return value if the attribute was found. This will
     * cause the iterator to immediately return that positive value,
     * indicating short-circuit success
     */
    if (strncmp(name, "PALETTE", 7) == 0)
        ret = H5_ITER_STOP;

    return ret;
}

/*-------------------------------------------------------------------------
 * Function: H5IM_find_palette
 *
 * Purpose: Private function. Find the attribute "PALETTE" in the image dataset
 *
 * Return: Success: 1, Failure: 0
 *
 * Comments:
 *  The function uses H5Aiterate2 with the operator function find_palette
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IM_find_palette(hid_t loc_id)
{
    return H5Aiterate2(loc_id, H5_INDEX_NAME, H5_ITER_INC, NULL, find_palette, NULL);
}

/*-------------------------------------------------------------------------
 * Function: H5IMget_image_info
 *
 * Purpose: Gets information about an image dataset (dimensions, interlace mode
 *          and number of associated palettes).
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5IMget_image_info(hid_t loc_id, const char *dset_name, hsize_t *width, hsize_t *height, hsize_t *planes,
                   char *interlace, hssize_t *npals)
{
    hid_t       did = -1;
    hid_t       sid = -1;
    hsize_t     dims[IMAGE24_RANK];
    hid_t       aid  = -1;
    hid_t       asid = -1;
    hid_t       atid = -1;
    H5T_class_t aclass;
    int         has_pal;
    hid_t       has_attr;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;
    if (interlace == NULL)
        return -1;

    /*assume initially we have no palettes attached*/
    *npals = 0;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, dset_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Try to find the attribute "INTERLACE_MODE" on the >>image<< dataset */
    if ((has_attr = H5Aexists(did, "INTERLACE_MODE")) < 0)
        goto out;

    /* It exists, get it */
    if (has_attr > 0) {

        if ((aid = H5Aopen(did, "INTERLACE_MODE", H5P_DEFAULT)) < 0)
            goto out;

        if ((atid = H5Aget_type(aid)) < 0)
            goto out;

        if (H5Aread(aid, atid, interlace) < 0)
            goto out;

        if (H5Tclose(atid) < 0)
            goto out;

        if (H5Aclose(aid) < 0)
            goto out;
    }

    /* Get the dataspace handle */
    if ((sid = H5Dget_space(did)) < 0)
        goto out;

    if (H5Sget_simple_extent_dims(sid, NULL, NULL) > IMAGE24_RANK)
        goto out;
    /* Get dimensions */
    if (H5Sget_simple_extent_dims(sid, dims, NULL) < 0)
        goto out;

    /* Initialize the image dimensions */

    if (has_attr > 0) {
        /* This is a 24 bit image */

        if (strncmp(interlace, "INTERLACE_PIXEL", 15) == 0) {
            /* Number of color planes is defined as the third dimension */
            *height = dims[0];
            *width  = dims[1];
            *planes = dims[2];
        }
        else if (strncmp(interlace, "INTERLACE_PLANE", 15) == 0) {
            /* Number of color planes is defined as the first dimension */
            *planes = dims[0];
            *height = dims[1];
            *width  = dims[2];
        }
        else
            return -1;
    }
    else {
        /* This is a 8 bit image */
        *height = dims[0];
        *width  = dims[1];
        *planes = 1;
    }

    /* Close */
    if (H5Sclose(sid) < 0)
        goto out;

    /* Get number of palettes */

    /* Try to find the attribute "PALETTE" on the >>image<< dataset */
    has_pal = H5IM_find_palette(did);

    if (has_pal == 1) {

        if ((aid = H5Aopen(did, "PALETTE", H5P_DEFAULT)) < 0)
            goto out;

        if ((atid = H5Aget_type(aid)) < 0)
            goto out;

        if ((aclass = H5Tget_class(atid)) < 0)
            goto out;

        /* Check if it is really a reference */

        if (aclass == H5T_REFERENCE) {

            /* Get the reference(s) */

            if ((asid = H5Aget_space(aid)) < 0)
                goto out;

            *npals = H5Sget_simple_extent_npoints(asid);

            if (H5Sclose(asid) < 0)
                goto out;

        } /* H5T_REFERENCE */

        if (H5Tclose(atid) < 0)
            goto out;

        /* Close the attribute. */
        if (H5Aclose(aid) < 0)
            goto out;
    }

    /* End access to the dataset and release resources used by it. */
    if (H5Dclose(did) < 0)
        goto out;

    return 0;

out:
    if (did > 0)
        H5Dclose(did);
    if (aid > 0)
        H5Aclose(aid);
    if (asid > 0)
        H5Sclose(asid);
    if (atid > 0)
        H5Tclose(atid);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5IMread_image
 *
 * Purpose: Reads image data from disk.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMread_image(hid_t loc_id, const char *dset_name, unsigned char *buf)
{
    hid_t did;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, dset_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Read */
    if (H5Dread(did, H5T_NATIVE_UCHAR, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0)
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
 * Function: H5IMmake_palette
 *
 * Purpose: Creates and writes a palette.
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMmake_palette(hid_t loc_id, const char *pal_name, const hsize_t *pal_dims, const unsigned char *pal_data)

{

    int has_pal;

    /* check the arguments */
    if (pal_name == NULL)
        return -1;

    /* Check if the dataset already exists */
    has_pal = H5LTfind_dataset(loc_id, pal_name);

    /* It exists. Return */
    if (has_pal == 1)
        return 0;

    /* Make the palette dataset. */
    if (H5LTmake_dataset(loc_id, pal_name, 2, pal_dims, H5T_NATIVE_UCHAR, pal_data) < 0)
        return -1;

    /* Attach the attribute "CLASS" to the >>palette<< dataset*/
    if (H5LTset_attribute_string(loc_id, pal_name, "CLASS", PALETTE_CLASS) < 0)
        return -1;

    /* Attach the attribute "PAL_VERSION" to the >>palette<< dataset*/
    if (H5LTset_attribute_string(loc_id, pal_name, "PAL_VERSION", "1.2") < 0)
        return -1;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function: H5IMlink_palette
 *
 * Purpose: This function attaches a palette to an existing image dataset
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *  An image (dataset) within an HDF5 file may optionally specify an array of
 *  palettes to be viewed with. The dataset will have an attribute
 *  which contains an array of object reference pointers which refer to palettes in the file.
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMlink_palette(hid_t loc_id, const char *image_name, const char *pal_name)

{
    hid_t       did;
    hid_t       atid = -1;
    hid_t       aid  = -1;
    hid_t       asid = -1;
    hobj_ref_t  ref;    /* write a new reference */
    hobj_ref_t *refbuf; /* buffer to read references */
    hssize_t    n_refs;
    hsize_t     dim_ref;
    htri_t      ok_pal;

    /* check the arguments */
    if (image_name == NULL)
        return -1;
    if (pal_name == NULL)
        return -1;

    /* The image dataset may or may not have the attribute "PALETTE"
     * First we try to open to see if it is already there; if not, it is created.
     * If it exists, the array of references is extended to hold the reference
     * to the new palette
     */

    /* First we get the image id */
    if ((did = H5Dopen2(loc_id, image_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Try to find the attribute "PALETTE" on the >>image<< dataset */
    if ((ok_pal = H5Aexists(did, "PALETTE")) < 0)
        goto out;

    /*-------------------------------------------------------------------------
     * It does not exist. We create the attribute and one reference
     *-------------------------------------------------------------------------
     */
    if (ok_pal == 0) {
        if ((asid = H5Screate(H5S_SCALAR)) < 0)
            goto out;

        /* Create the attribute type for the reference */
        if ((atid = H5Tcopy(H5T_STD_REF_OBJ)) < 0)
            goto out;

        /* Create the attribute "PALETTE" to be attached to the image*/
        if ((aid = H5Acreate2(did, "PALETTE", atid, asid, H5P_DEFAULT, H5P_DEFAULT)) < 0)
            goto out;

        /* Create a reference. The reference is created on the local id.  */
        if (H5Rcreate(&ref, loc_id, pal_name, H5R_OBJECT, (hid_t)-1) < 0)
            goto out;

        /* Write the attribute with the reference */
        if (H5Awrite(aid, atid, &ref) < 0)
            goto out;

        /* close */
        if (H5Sclose(asid) < 0)
            goto out;
        if (H5Tclose(atid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
    }

    /*-------------------------------------------------------------------------
     * The attribute already exists, open it
     *-------------------------------------------------------------------------
     */
    else if (ok_pal > 0) {
        if ((aid = H5Aopen(did, "PALETTE", H5P_DEFAULT)) < 0)
            goto out;

        if ((atid = H5Aget_type(aid)) < 0)
            goto out;

        if (H5Tget_class(atid) < 0)
            goto out;

        /* Get and save the old reference(s) */
        if ((asid = H5Aget_space(aid)) < 0)
            goto out;

        n_refs = H5Sget_simple_extent_npoints(asid);

        dim_ref = (hsize_t)n_refs + 1;

        refbuf = (hobj_ref_t *)malloc(sizeof(hobj_ref_t) * (size_t)dim_ref);

        if (H5Aread(aid, atid, refbuf) < 0)
            goto out;

        /* The attribute must be deleted, in order to the new one can reflect the changes*/
        if (H5Adelete(did, "PALETTE") < 0)
            goto out;

        /* Create a new reference for this palette. */
        if (H5Rcreate(&ref, loc_id, pal_name, H5R_OBJECT, (hid_t)-1) < 0)
            goto out;

        refbuf[n_refs] = ref;

        /* Create the data space for the new references */
        if (H5Sclose(asid) < 0)
            goto out;

        if ((asid = H5Screate_simple(1, &dim_ref, NULL)) < 0)
            goto out;

        /* Create the attribute again with the changes of space */
        if (H5Aclose(aid) < 0)
            goto out;

        if ((aid = H5Acreate2(did, "PALETTE", atid, asid, H5P_DEFAULT, H5P_DEFAULT)) < 0)
            goto out;

        /* Write the attribute with the new references */
        if (H5Awrite(aid, atid, refbuf) < 0)
            goto out;

        /* close */
        if (H5Sclose(asid) < 0)
            goto out;
        if (H5Tclose(atid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;

        free(refbuf);

    } /* ok_pal > 0 */

    /* Close the image dataset. */
    if (H5Dclose(did) < 0)
        return -1;

    return 0;

out:
    H5Dclose(did);
    H5Sclose(asid);
    H5Tclose(atid);
    H5Aclose(aid);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5IMunlink_palette
 *
 * Purpose: This function detaches a palette from an existing image dataset
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMunlink_palette(hid_t loc_id, const char *image_name, const char *pal_name)
{
    hid_t       did;
    hid_t       atid;
    hid_t       aid;
    H5T_class_t aclass;
    htri_t      ok_pal;
    int         has_pal;

    /* check the arguments */
    if (image_name == NULL)
        return -1;
    if (pal_name == NULL)
        return -1;

    /* Try to find the palette dataset */
    has_pal = H5LTfind_dataset(loc_id, pal_name);

    /* It does not exist. Return */
    if (has_pal == 0)
        return -1;

    /* The image dataset may or not have the attribute "PALETTE"
     * First we try to open to see if it is already there; if not, it is created.
     * If it exists, the array of references is extended to hold the reference
     * to the new palette
     */

    /* First we get the image id */
    if ((did = H5Dopen2(loc_id, image_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Try to find the attribute "PALETTE" on the >>image<< dataset */
    if ((ok_pal = H5Aexists(did, "PALETTE")) < 0)
        goto out;

    /* It does not exist. Nothing to do */
    if (ok_pal == 0)
        goto out;
    else if (ok_pal > 0) {
        /* The attribute exists, open it */
        if ((aid = H5Aopen(did, "PALETTE", H5P_DEFAULT)) < 0)
            goto out;

        if ((atid = H5Aget_type(aid)) < 0)
            goto out;

        if ((aclass = H5Tget_class(atid)) < 0)
            goto out;

        /* Check if it is really a reference */
        if (aclass == H5T_REFERENCE) {
            /* Delete the attribute */
            if (H5Adelete(did, "PALETTE") < 0)
                goto out;

        } /* H5T_REFERENCE */

        if (H5Tclose(atid) < 0)
            goto out;

        /* Close the attribute. */
        if (H5Aclose(aid) < 0)
            goto out;
    }

    /* Close the image dataset. */
    if (H5Dclose(did) < 0)
        return -1;

    return 0;

out:
    H5Dclose(did);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5IMget_npalettes
 *
 * Purpose: Gets the number of palettes associated to an image
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMget_npalettes(hid_t loc_id, const char *image_name, hssize_t *npals)
{
    hid_t       did;
    hid_t       atid;
    hid_t       aid;
    hid_t       asid;
    H5T_class_t aclass;
    int         has_pal;

    /* check the arguments */
    if (image_name == NULL)
        return -1;

    /*assume initially we have no palettes attached*/
    *npals = 0;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, image_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Try to find the attribute "PALETTE" on the >>image<< dataset */
    has_pal = H5IM_find_palette(did);

    if (has_pal == 1) {

        if ((aid = H5Aopen(did, "PALETTE", H5P_DEFAULT)) < 0)
            goto out;

        if ((atid = H5Aget_type(aid)) < 0)
            goto out;

        if ((aclass = H5Tget_class(atid)) < 0)
            goto out;

        /* Check if it is really a reference */

        if (aclass == H5T_REFERENCE) {
            if ((asid = H5Aget_space(aid)) < 0)
                goto out;

            *npals = H5Sget_simple_extent_npoints(asid);

            if (H5Sclose(asid) < 0)
                goto out;

        } /* H5T_REFERENCE */

        if (H5Tclose(atid) < 0)
            goto out;

        /* Close the attribute. */
        if (H5Aclose(aid) < 0)
            goto out;
    }

    /* Close the image dataset. */
    if (H5Dclose(did) < 0)
        return -1;

    return 0;

out:
    H5Dclose(did);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5IMget_palette_info
 *
 * Purpose: Get palette information
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMget_palette_info(hid_t loc_id, const char *image_name, int pal_number, hsize_t *pal_dims)
{
    hid_t       did;
    int         has_pal;
    hid_t       atid = -1;
    hid_t       aid;
    hid_t       asid = -1;
    hssize_t    n_refs;
    hsize_t     dim_ref;
    hobj_ref_t *refbuf; /* buffer to read references */
    hid_t       pal_id;
    hid_t       pal_space_id;
    hsize_t     pal_maxdims[2];

    /* check the arguments */
    if (image_name == NULL)
        return -1;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, image_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Try to find the attribute "PALETTE" on the >>image<< dataset */
    has_pal = H5IM_find_palette(did);

    if (has_pal == 1) {
        if ((aid = H5Aopen(did, "PALETTE", H5P_DEFAULT)) < 0)
            goto out;

        if ((atid = H5Aget_type(aid)) < 0)
            goto out;

        if (H5Tget_class(atid) < 0)
            goto out;

        /* Get the reference(s) */
        if ((asid = H5Aget_space(aid)) < 0)
            goto out;

        n_refs = H5Sget_simple_extent_npoints(asid);

        dim_ref = (hsize_t)n_refs;

        refbuf = (hobj_ref_t *)malloc(sizeof(hobj_ref_t) * (size_t)dim_ref);

        if (H5Aread(aid, atid, refbuf) < 0)
            goto out;

        /* Get the actual palette */
        if ((pal_id = H5Rdereference2(did, H5P_DEFAULT, H5R_OBJECT, &refbuf[pal_number])) < 0)
            goto out;

        if ((pal_space_id = H5Dget_space(pal_id)) < 0)
            goto out;

        if (H5Sget_simple_extent_ndims(pal_space_id) < 0)
            goto out;

        if (H5Sget_simple_extent_dims(pal_space_id, pal_dims, pal_maxdims) < 0)
            goto out;

        /* close */
        if (H5Dclose(pal_id) < 0)
            goto out;
        if (H5Sclose(pal_space_id) < 0)
            goto out;
        if (H5Sclose(asid) < 0)
            goto out;
        if (H5Tclose(atid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
        free(refbuf);
    }

    /* Close the image dataset. */
    if (H5Dclose(did) < 0)
        return -1;

    return 0;

out:
    H5Dclose(did);
    H5Sclose(asid);
    H5Tclose(atid);
    H5Aclose(aid);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5IMget_palette
 *
 * Purpose: Read palette
 *
 * Return: Success: 0, Failure: -1
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMget_palette(hid_t loc_id, const char *image_name, int pal_number, unsigned char *pal_data)
{
    hid_t       did;
    int         has_pal;
    hid_t       atid = -1;
    hid_t       aid;
    hid_t       asid = -1;
    hssize_t    n_refs;
    hsize_t     dim_ref;
    hobj_ref_t *refbuf; /* buffer to read references */
    hid_t       pal_id;

    /* check the arguments */
    if (image_name == NULL)
        return -1;
    if (pal_data == NULL)
        return -1;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, image_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Try to find the attribute "PALETTE" on the >>image<< dataset */
    has_pal = H5IM_find_palette(did);

    if (has_pal == 1) {
        if ((aid = H5Aopen(did, "PALETTE", H5P_DEFAULT)) < 0)
            goto out;

        if ((atid = H5Aget_type(aid)) < 0)
            goto out;

        if (H5Tget_class(atid) < 0)
            goto out;

        /* Get the reference(s) */
        if ((asid = H5Aget_space(aid)) < 0)
            goto out;

        n_refs = H5Sget_simple_extent_npoints(asid);

        dim_ref = (hsize_t)n_refs;

        refbuf = (hobj_ref_t *)malloc(sizeof(hobj_ref_t) * (size_t)dim_ref);

        if (H5Aread(aid, atid, refbuf) < 0)
            goto out;

        /* Get the palette id */
        if ((pal_id = H5Rdereference2(did, H5P_DEFAULT, H5R_OBJECT, &refbuf[pal_number])) < 0)
            goto out;

        /* Read the palette dataset */
        if (H5Dread(pal_id, H5Dget_type(pal_id), H5S_ALL, H5S_ALL, H5P_DEFAULT, pal_data) < 0)
            goto out;

        /* close */
        if (H5Dclose(pal_id) < 0)
            goto out;
        if (H5Sclose(asid) < 0)
            goto out;
        if (H5Tclose(atid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
        free(refbuf);
    }

    /* Close the image dataset. */
    if (H5Dclose(did) < 0)
        return -1;

    return 0;

out:
    H5Dclose(did);
    H5Sclose(asid);
    H5Tclose(atid);
    H5Aclose(aid);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5IMis_image
 *
 * Purpose:
 *
 * Return: true, false, fail
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMis_image(hid_t loc_id, const char *dset_name)
{
    hid_t   did;
    htri_t  has_class;
    hid_t   atid;
    hid_t   aid = -1;
    char   *attr_data;    /* Name of attribute */
    hsize_t storage_size; /* Size of storage for attribute */
    herr_t  ret;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* Assume initially fail condition */
    ret = -1;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, dset_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Try to find the attribute "CLASS" on the dataset */
    if ((has_class = H5Aexists(did, "CLASS")) < 0)
        goto out;

    if (has_class == 0) {
        H5Dclose(did);
        return 0;
    }
    else {

        if ((aid = H5Aopen(did, "CLASS", H5P_DEFAULT)) < 0)
            goto out;

        if ((atid = H5Aget_type(aid)) < 0)
            goto out;

        /* check to make sure attribute is a string */
        if (H5T_STRING != H5Tget_class(atid))
            goto out;

        /* check to make sure string is null-terminated */
        if (H5T_STR_NULLTERM != H5Tget_strpad(atid))
            goto out;

        /* allocate buffer large enough to hold string */
        if ((storage_size = H5Aget_storage_size(aid)) == 0)
            goto out;

        attr_data = (char *)malloc((size_t)storage_size * sizeof(char) + 1);
        if (attr_data == NULL)
            goto out;

        if (H5Aread(aid, atid, attr_data) < 0)
            goto out;

        if (strncmp(attr_data, IMAGE_CLASS, MIN(strlen(IMAGE_CLASS), strlen(attr_data))) == 0)
            ret = 1;
        else
            ret = 0;

        free(attr_data);

        if (H5Tclose(atid) < 0)
            goto out;

        if (H5Aclose(aid) < 0)
            goto out;
    }

    /* Close the dataset. */
    if (H5Dclose(did) < 0)
        return -1;

    return ret;

out:
    H5Dclose(did);
    return -1;
}

/*-------------------------------------------------------------------------
 * Function: H5IMis_palette
 *
 * Purpose:
 *
 * Return: true, false, fail
 *
 * Comments:
 *  based on HDF5 Image and Palette Specification
 *
 *-------------------------------------------------------------------------
 */

herr_t
H5IMis_palette(hid_t loc_id, const char *dset_name)
{
    hid_t   did;
    htri_t  has_class;
    hid_t   atid;
    hid_t   aid = -1;
    char   *attr_data;    /* Name of attribute */
    hsize_t storage_size; /* Size of storage for attribute */
    herr_t  ret;

    /* check the arguments */
    if (dset_name == NULL)
        return -1;

    /* Assume initially fail condition */
    ret = -1;

    /* Open the dataset. */
    if ((did = H5Dopen2(loc_id, dset_name, H5P_DEFAULT)) < 0)
        return -1;

    /* Try to find the attribute "CLASS" on the dataset */
    if ((has_class = H5Aexists(did, "CLASS")) < 0)
        goto out;

    if (has_class == 0) {
        H5Dclose(did);
        return 0;
    }
    else {

        if ((aid = H5Aopen(did, "CLASS", H5P_DEFAULT)) < 0)
            goto out;

        if ((atid = H5Aget_type(aid)) < 0)
            goto out;

        /* check to make sure attribute is a string */
        if (H5T_STRING != H5Tget_class(atid))
            goto out;

        /* check to make sure string is null-terminated */
        if (H5T_STR_NULLTERM != H5Tget_strpad(atid))
            goto out;

        /* allocate buffer large enough to hold string */
        if ((storage_size = H5Aget_storage_size(aid)) == 0)
            goto out;

        attr_data = (char *)malloc((size_t)storage_size * sizeof(char) + 1);
        if (attr_data == NULL)
            goto out;

        if (H5Aread(aid, atid, attr_data) < 0)
            goto out;

        if (strncmp(attr_data, PALETTE_CLASS, MIN(strlen(PALETTE_CLASS), strlen(attr_data))) == 0)
            ret = 1;
        else
            ret = 0;

        free(attr_data);

        if (H5Tclose(atid) < 0)
            goto out;

        if (H5Aclose(aid) < 0)
            goto out;
    }

    /* Close the dataset. */
    if (H5Dclose(did) < 0)
        return -1;

    return ret;

out:
    H5Dclose(did);
    return -1;
}
