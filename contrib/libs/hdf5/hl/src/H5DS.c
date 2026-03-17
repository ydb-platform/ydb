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

#include "H5DSprivate.h"
#include "H5LTprivate.h"
#include "H5IMprivate.h"
#include "H5TBprivate.h"

/* Local routines */
static herr_t H5DS_is_reserved(hid_t did, bool *is_reserved);

/*-------------------------------------------------------------------------
 * Function: H5DSwith_new_ref
 *
 * Purpose: Determines if new references are used with dimension scales.
 *   The function H5DSwith_new_ref takes any object identifier and checks
 *   if new references are used for dimension scales. Currently,
 *   new references are used when non-native VOL connector is used or when
 *   H5_DIMENSION_SCALES_WITH_NEW_REF is set up via configure option.
 *
 * Return: Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5DSwith_new_ref(hid_t obj_id, hbool_t *with_new_ref)
{
    bool config_flag = false;
    bool native      = false;

    if (!with_new_ref)
        return FAIL;

    if (H5VLobject_is_native(obj_id, &native) < 0)
        return FAIL;

#ifdef H5_DIMENSION_SCALES_WITH_NEW_REF
    config_flag = true;
#endif

    *with_new_ref = (config_flag || !native);

    return SUCCEED;
}

/*-------------------------------------------------------------------------
 * Function: H5DSset_scale
 *
 * Purpose: The dataset DSID is converted to a Dimension Scale dataset.
 *   Creates the CLASS attribute, set to the value "DIMENSION_SCALE"
 *   and an empty REFERENCE_LIST attribute.
 *   If DIMNAME is specified, then an attribute called NAME is created,
 *   with the value DIMNAME.
 *
 * Return: Success: SUCCEED, Failure: FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5DSset_scale(hid_t dsid, const char *dimname)
{
    htri_t     has_dimlist;
    H5I_type_t it;

    /*-------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */
    /* get ID type */
    if ((it = H5Iget_type(dsid)) < 0)
        return FAIL;

    if (H5I_DATASET != it)
        return FAIL;

    /*-------------------------------------------------------------------------
     * check if the dataset is a dataset which has references to dimension scales
     *-------------------------------------------------------------------------
     */

    /* Try to find the attribute "DIMENSION_LIST"  */
    if ((has_dimlist = H5Aexists(dsid, DIMENSION_LIST)) < 0)
        return FAIL;
    if (has_dimlist > 0)
        return FAIL;

    /*-------------------------------------------------------------------------
     * write the standard attributes for a Dimension Scale dataset
     *-------------------------------------------------------------------------
     */

    if (H5LT_set_attribute_string(dsid, "CLASS", DIMENSION_SCALE_CLASS) < 0)
        return FAIL;

    if (dimname != NULL) {
        if (H5LT_set_attribute_string(dsid, "NAME", dimname) < 0)
            return FAIL;
    }

    return SUCCEED;
}

/*-------------------------------------------------------------------------
 * Function: H5DSattach_scale
 *
 * Purpose: Define Dimension Scale DSID to be associated with dimension IDX
 *  of Dataset DID. Entries are created in the DIMENSION_LIST and
 *  REFERENCE_LIST attributes.
 *
 * Return:
 *   Success: SUCCEED
 *   Failure: FAIL
 *
 * Fails if: Bad arguments
 *           If DSID is not a Dimension Scale
 *           If DID is a Dimension Scale (A Dimension Scale cannot have scales)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5DSattach_scale(hid_t did, hid_t dsid, unsigned int idx)
{
    htri_t   has_dimlist;
    htri_t   has_reflist;
    int      is_ds;
    hssize_t nelmts;
    hid_t    sid, sid_w;             /* space ID */
    hid_t    tid  = H5I_INVALID_HID; /* attribute type ID */
    hid_t    ntid = H5I_INVALID_HID; /* attribute native type ID */
    hid_t    aid  = H5I_INVALID_HID; /* attribute ID */
    int      rank;                   /* rank of dataset */
    hsize_t  dims[1];                /* dimension of the "REFERENCE_LIST" array */

    ds_list_t  dsl;          /* attribute data in the DS pointing to the dataset */
    ds_list_t *dsbuf = NULL; /* array of attribute data in the DS pointing to the dataset */
    ds_list_t *dsbuf_w =
        NULL; /* array of "REFERENCE_LIST" attribute data to write when adding new reference to a dataset */
    hobj_ref_t ref_to_ds = HADDR_UNDEF; /* reference to the DS */
    hobj_ref_t ref_j;                   /* iterator reference */

    /* Variables to be used when new references are used */
    nds_list_t  ndsl;
    nds_list_t *ndsbuf     = NULL;
    nds_list_t *ndsbuf_w   = NULL;
    H5R_ref_t   nref_to_ds = {0};
    H5R_ref_t   nref_j;
    bool        is_new_ref;

    hvl_t      *buf = NULL; /* VL buffer to store in the attribute */
    hid_t       dsid_j;     /* DS dataset ID in DIMENSION_LIST */
    H5O_info2_t oi1, oi2;
    H5I_type_t  it1, it2;
    int         i;
    size_t      len;
    int         found_ds = 0;
    htri_t      is_scale;
    bool        is_reserved;

    /*-------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */

    if ((is_scale = H5DSis_scale(did)) < 0)
        return FAIL;

    /* the dataset cannot be a DS dataset */
    if (is_scale == 1)
        return FAIL;

    /* get info for the dataset in the parameter list */
    if (H5Oget_info3(did, &oi1, H5O_INFO_BASIC) < 0)
        return FAIL;

    /* get info for the scale in the parameter list */
    if (H5Oget_info3(dsid, &oi2, H5O_INFO_BASIC) < 0)
        return FAIL;

    /* same object, not valid */
    if (oi1.fileno == oi2.fileno) {
        int token_cmp;

        if (H5Otoken_cmp(did, &oi1.token, &oi2.token, &token_cmp) < 0)
            return FAIL;
        if (!token_cmp)
            return FAIL;
    } /* end if */

    /*-------------------------------------------------------------------------
     * determine if old or new references should be used
     *-------------------------------------------------------------------------
     */

    if (H5DSwith_new_ref(did, &is_new_ref) < 0)
        return FAIL;

    /* get ID type */
    if ((it1 = H5Iget_type(did)) < 0)
        return FAIL;
    if ((it2 = H5Iget_type(dsid)) < 0)
        return FAIL;

    if (H5I_DATASET != it1 || H5I_DATASET != it2)
        return FAIL;

    /* The DS dataset cannot have dimension scales */
    if (H5Aexists(dsid, DIMENSION_LIST) > 0)
        return FAIL;

    /* Check if the dataset is a "reserved" dataset (image, table) */
    if (H5DS_is_reserved(did, &is_reserved) < 0)
        return FAIL;
    if (is_reserved == true)
        return FAIL;

    /*-------------------------------------------------------------------------
     * The dataset may or may not have the associated DS attribute
     * First we try to open to see if it is already there; if not, it is created.
     * If it exists, the array of references is extended to hold the reference
     * to the new DS
     *-------------------------------------------------------------------------
     */

    /* get dataset space */
    if ((sid = H5Dget_space(did)) < 0)
        return FAIL;

    /* get rank */
    if ((rank = H5Sget_simple_extent_ndims(sid)) < 0)
        goto out;

    /* scalar rank */
    if (rank == 0)
        rank = 1;

    /* close dataset space */
    if (H5Sclose(sid) < 0)
        return FAIL;

    /* parameter range checking */
    if (idx > (unsigned)rank - 1)
        return FAIL;

    /*-------------------------------------------------------------------------
     * two references are created: one to the DS, saved in "DIMENSION_LIST"
     *  and one to the dataset, saved in "REFERENCE_LIST"
     *-------------------------------------------------------------------------
     */
    if (is_new_ref) {
        /* create a reference for the >>DS<< dataset */
        if (H5Rcreate_object(dsid, ".", H5P_DEFAULT, &nref_to_ds) < 0)
            return FAIL;
        /* create a reference for the >>data<< dataset */
        if (H5Rcreate_object(did, ".", H5P_DEFAULT, &ndsl.ref) < 0)
            return FAIL;
    }
    else {
        /* create a reference for the >>DS<< dataset */
        if (H5Rcreate(&ref_to_ds, dsid, ".", H5R_OBJECT, (hid_t)-1) < 0)
            return FAIL;

        /* create a reference for the >>data<< dataset */
        if (H5Rcreate(&dsl.ref, did, ".", H5R_OBJECT, (hid_t)-1) < 0)
            return FAIL;
    }
    /* Try to find the attribute "DIMENSION_LIST" on the >>data<< dataset */
    if ((has_dimlist = H5Aexists(did, DIMENSION_LIST)) < 0)
        return FAIL;

    /*-------------------------------------------------------------------------
     * it does not exist. we create the attribute and its reference data
     *-------------------------------------------------------------------------
     */
    if (has_dimlist == 0) {

        dims[0] = (hsize_t)rank;

        /* space for the attribute */
        if ((sid = H5Screate_simple(1, dims, NULL)) < 0)
            return FAIL;

        /* create the type for the attribute "DIMENSION_LIST" */
        if (is_new_ref) {
            if ((tid = H5Tvlen_create(H5T_STD_REF)) < 0)
                goto out;
        }
        else {
            if ((tid = H5Tvlen_create(H5T_STD_REF_OBJ)) < 0)
                goto out;
        }
        /* create the attribute */
        if ((aid = H5Acreate2(did, DIMENSION_LIST, tid, sid, H5P_DEFAULT, H5P_DEFAULT)) < 0)
            goto out;

        /* allocate and initialize the VL */
        buf = (hvl_t *)malloc((size_t)rank * sizeof(hvl_t));
        if (buf == NULL)
            goto out;

        for (i = 0; i < rank; i++) {
            buf[i].len = 0;
            buf[i].p   = NULL;
        }

        /* store the REF information in the index of the dataset that has the DS */
        buf[idx].len = 1;
        if (is_new_ref) {
            buf[idx].p                   = malloc(1 * sizeof(H5R_ref_t));
            ((H5R_ref_t *)buf[idx].p)[0] = nref_to_ds;
        }
        else {
            buf[idx].p                    = malloc(1 * sizeof(hobj_ref_t));
            ((hobj_ref_t *)buf[idx].p)[0] = ref_to_ds;
        }
        /* write the attribute with the reference */
        if (H5Awrite(aid, tid, buf) < 0)
            goto out;

        /* close */
        if (is_new_ref) {
            if (H5Rdestroy(&nref_to_ds) < 0)
                goto out;
        }
        if (H5Sclose(sid) < 0)
            goto out;
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
        free(buf[idx].p);
        buf[idx].p = NULL;
        free(buf);
        buf = NULL;
    }

    /*-------------------------------------------------------------------------
     * the attribute already exists, open it, extend the buffer,
     *  and insert the new reference
     *-------------------------------------------------------------------------
     */
    else if (has_dimlist > 0) {
        if ((aid = H5Aopen(did, DIMENSION_LIST, H5P_DEFAULT)) < 0)
            goto out;

        if ((tid = H5Aget_type(aid)) < 0)
            goto out;

        if ((sid = H5Aget_space(aid)) < 0)
            goto out;

        /* allocate and initialize the VL */
        buf = (hvl_t *)malloc((size_t)rank * sizeof(hvl_t));
        if (buf == NULL)
            goto out;

        /* read */
        if (H5Aread(aid, tid, buf) < 0)
            goto out;

        /* check to avoid inserting duplicates. it is not FAIL, just do nothing */
        /* iterate all the REFs in this dimension IDX */
        for (i = 0; i < (int)buf[idx].len; i++) {
            /* get the reference */
            if (is_new_ref) {
                nref_j = ((H5R_ref_t *)buf[idx].p)[i];

                /* get the scale id for this REF */
                if ((dsid_j = H5Ropen_object(&nref_j, H5P_DEFAULT, H5P_DEFAULT)) < 0)
                    goto out;
            }
            else {
                ref_j = ((hobj_ref_t *)buf[idx].p)[i];

                /* get the scale id for this REF */
                if ((dsid_j = H5Rdereference2(did, H5P_DEFAULT, H5R_OBJECT, &ref_j)) < 0)
                    goto out;
            }
            /* get info for DS in the parameter list */
            if (H5Oget_info3(dsid, &oi1, H5O_INFO_BASIC) < 0)
                goto out;

            /* get info for this DS */
            if (H5Oget_info3(dsid_j, &oi2, H5O_INFO_BASIC) < 0)
                goto out;

            /* same object, so this DS scale is already in this DIM IDX */
            if (oi1.fileno == oi2.fileno) {
                int token_cmp;

                if (H5Otoken_cmp(did, &oi1.token, &oi2.token, &token_cmp) < 0)
                    goto out;
                if (!token_cmp)
                    found_ds = 1;
            } /* end if */

            /* close the dereferenced dataset */
            if (H5Dclose(dsid_j) < 0)
                goto out;
        } /* end for */

        if (found_ds == 0) {
            /* we are adding one more DS to this dimension */
            if (buf[idx].len > 0) {
                buf[idx].len++;
                len = buf[idx].len;
                if (is_new_ref) {
                    buf[idx].p                         = realloc(buf[idx].p, len * sizeof(H5R_ref_t));
                    ((H5R_ref_t *)buf[idx].p)[len - 1] = nref_to_ds;
                }
                else {
                    buf[idx].p                          = realloc(buf[idx].p, len * sizeof(hobj_ref_t));
                    ((hobj_ref_t *)buf[idx].p)[len - 1] = ref_to_ds;
                }
            } /* end if */
            else {
                /* store the REF information in the index of the dataset that has the DS */
                buf[idx].len = 1;
                if (is_new_ref) {
                    buf[idx].p                   = malloc(sizeof(H5R_ref_t));
                    ((H5R_ref_t *)buf[idx].p)[0] = nref_to_ds;
                }
                else {
                    buf[idx].p                    = malloc(sizeof(hobj_ref_t));
                    ((hobj_ref_t *)buf[idx].p)[0] = ref_to_ds;
                }
            } /* end else */
        }     /* end if */
        else {
            if (is_new_ref && H5Rdestroy(&nref_to_ds) < 0)
                goto out;
        }

        /* write the attribute with the new references */
        if (H5Awrite(aid, tid, buf) < 0)
            goto out;

        /* close */
        if (H5Treclaim(tid, sid, H5P_DEFAULT, buf) < 0)
            goto out;
        if (H5Sclose(sid) < 0)
            goto out;
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
        free(buf);
        buf = NULL;
    } /* has_dimlist */

    /*-------------------------------------------------------------------------
     * save DS info on the >>DS<< dataset
     *-------------------------------------------------------------------------
     */

    /* try to find the attribute "REFERENCE_LIST" on the >>DS<< dataset */
    if ((has_reflist = H5Aexists(dsid, REFERENCE_LIST)) < 0)
        goto out;

    /*-------------------------------------------------------------------------
     * it does not exist. we create the attribute and its reference data
     *-------------------------------------------------------------------------
     */
    if (has_reflist == 0) {
        dims[0] = 1;

        /* space for the attribute */
        if ((sid = H5Screate_simple(1, dims, NULL)) < 0)
            goto out;

        /* create the compound datatype for the attribute "REFERENCE_LIST" */
        if (is_new_ref) {
            if ((tid = H5Tcreate(H5T_COMPOUND, sizeof(nds_list_t))) < 0)
                goto out;
            if (H5Tinsert(tid, "dataset", HOFFSET(nds_list_t, ref), H5T_STD_REF) < 0)
                goto out;
            if (H5Tinsert(tid, "dimension", HOFFSET(nds_list_t, dim_idx), H5T_NATIVE_UINT) < 0)
                goto out;
        }
        else {
            if ((tid = H5Tcreate(H5T_COMPOUND, sizeof(ds_list_t))) < 0)
                goto out;
            if (H5Tinsert(tid, "dataset", HOFFSET(ds_list_t, ref), H5T_STD_REF_OBJ) < 0)
                goto out;
            if (H5Tinsert(tid, "dimension", HOFFSET(ds_list_t, dim_idx), H5T_NATIVE_UINT) < 0)
                goto out;
        }

        /* create the attribute */
        if ((aid = H5Acreate2(dsid, REFERENCE_LIST, tid, sid, H5P_DEFAULT, H5P_DEFAULT)) < 0)
            goto out;

        /* store the IDX information */
        if (is_new_ref) {
            ndsl.dim_idx = idx;
            if (H5Awrite(aid, tid, &ndsl) < 0)
                goto out;
            if (H5Rdestroy(&ndsl.ref) < 0)
                goto out;
        }
        else {
            dsl.dim_idx = idx;
            if (H5Awrite(aid, tid, &dsl) < 0)
                goto out;
        }
        if (H5Sclose(sid) < 0)
            goto out;
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
    } /* end if */

    /*-------------------------------------------------------------------------
     * the "REFERENCE_LIST" array already exists, open it and extend it
     *-------------------------------------------------------------------------
     */
    else if (has_reflist > 0) {
        hid_t tmp_id; /* Temporary DS dataset ID to recreate reference */
        int   j;

        if ((aid = H5Aopen(dsid, REFERENCE_LIST, H5P_DEFAULT)) < 0)
            goto out;

        if ((tid = H5Aget_type(aid)) < 0)
            goto out;

        /* get native type to read attribute REFERENCE_LIST */
        if ((ntid = H5Tget_native_type(tid, H5T_DIR_ASCEND)) < 0)
            goto out;

        /* get and save the old reference(s) */
        if ((sid = H5Aget_space(aid)) < 0)
            goto out;

        if ((nelmts = H5Sget_simple_extent_npoints(sid)) < 0)
            goto out;

        nelmts++;
        if (is_new_ref) {
            ndsbuf = (nds_list_t *)malloc((size_t)nelmts * sizeof(nds_list_t));
            if (ndsbuf == NULL)
                goto out;
            if (H5Aread(aid, ntid, ndsbuf) < 0)
                goto out;
        }
        else {
            dsbuf = (ds_list_t *)malloc((size_t)nelmts * sizeof(ds_list_t));
            if (dsbuf == NULL)
                goto out;
            if (H5Aread(aid, ntid, dsbuf) < 0)
                goto out;
        }

        /* close */
        if (H5Aclose(aid) < 0)
            goto out;

        /*-------------------------------------------------------------------------
         * create a new attribute
         *-------------------------------------------------------------------------
         */

        /* Allocate new buffer to copy old references and add new one */

        if (is_new_ref) {
            ndsbuf_w = (nds_list_t *)malloc((size_t)nelmts * sizeof(nds_list_t));
            if (ndsbuf_w == NULL)
                goto out;
        }
        else {
            dsbuf_w = (ds_list_t *)malloc((size_t)nelmts * sizeof(ds_list_t));
            if (dsbuf_w == NULL)
                goto out;
        }
        /* Recreate the references we read from the existing "REFERENCE_LIST" attribute */
        for (j = 0; j < nelmts - 1; j++) {
            if (is_new_ref) {
                ndsbuf_w[j].dim_idx = ndsbuf[j].dim_idx;
                tmp_id              = H5Ropen_object(&ndsbuf[j].ref, H5P_DEFAULT, H5P_DEFAULT);
                if (tmp_id < 0)
                    goto out;
                if (H5Rcreate_object(tmp_id, ".", H5P_DEFAULT, &ndsbuf_w[j].ref) < 0) {
                    H5Dclose(tmp_id);
                    goto out;
                }
            }
            else {
                dsbuf_w[j] = dsbuf[j];
            }
        }
        /* store the IDX information (index of the dataset that has the DS) */
        if (is_new_ref) {
            ndsl.dim_idx         = idx;
            ndsbuf_w[nelmts - 1] = ndsl;
        }
        else {
            dsl.dim_idx         = idx;
            dsbuf_w[nelmts - 1] = dsl;
        }

        /* the attribute must be deleted, in order to the new one can reflect the changes*/
        if (H5Adelete(dsid, REFERENCE_LIST) < 0)
            goto out;

        /* create a new data space for the new references array */
        dims[0] = (hsize_t)nelmts;

        if ((sid_w = H5Screate_simple(1, dims, NULL)) < 0)
            goto out;

        /* create the attribute again with the changes of space */
        if ((aid = H5Acreate2(dsid, REFERENCE_LIST, tid, sid_w, H5P_DEFAULT, H5P_DEFAULT)) < 0)
            goto out;

        /* write the attribute with the new references */
        if (is_new_ref) {
            if (H5Awrite(aid, ntid, ndsbuf_w) < 0)
                goto out;
            if (H5Treclaim(tid, sid, H5P_DEFAULT, ndsbuf_w) < 0)
                goto out;
        }
        else {
            if (H5Awrite(aid, ntid, dsbuf_w) < 0)
                goto out;
            if (H5Treclaim(tid, sid, H5P_DEFAULT, dsbuf_w) < 0)
                goto out;
        }
        if (H5Sclose(sid) < 0)
            goto out;
        if (H5Sclose(sid_w) < 0)
            goto out;
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
        if (H5Tclose(ntid) < 0)
            goto out;
        if (is_new_ref) {
            free(ndsbuf);
            dsbuf = NULL;
            free(ndsbuf_w);
            dsbuf = NULL;
        }
        else {
            free(dsbuf);
            dsbuf = NULL;
            free(dsbuf_w);
            dsbuf = NULL;
        }
    } /* has_reflist */

    /*-------------------------------------------------------------------------
     * write the standard attributes for a Dimension Scale dataset
     *-------------------------------------------------------------------------
     */

    if ((is_ds = H5DSis_scale(dsid)) < 0)
        return FAIL;

    if (is_ds == 0) {
        if (H5LT_set_attribute_string(dsid, "CLASS", DIMENSION_SCALE_CLASS) < 0)
            return FAIL;
    }

    return SUCCEED;

    /* error zone */
out:
    if (buf)
        free(buf);
    if (dsbuf)
        free(dsbuf);
    if (dsbuf_w)
        free(dsbuf_w);

    H5E_BEGIN_TRY
    {
        H5Sclose(sid);
        H5Aclose(aid);
        H5Tclose(ntid);
        H5Tclose(tid);
    }
    H5E_END_TRY
    return FAIL;
}

/*-------------------------------------------------------------------------
 * Function: H5DSdetach_scale
 *
 * Purpose: If possible, deletes association of Dimension Scale DSID with
 *     dimension IDX of Dataset DID. This deletes the entries in the
 *     DIMENSION_LIST and REFERENCE_LIST attributes.
 *
 * Return:
 *   Success: SUCCEED
 *   Failure: FAIL
 *
 * Fails if: Bad arguments
 *           The dataset DID or DSID do not exist.
 *           The DSID is not a Dimension Scale
 *           DSID is not attached to DID.
 * Note that a scale may be associated with more than dimension of the same dataset.
 * If so, the detach operation only deletes one of the associations, for DID.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5DSdetach_scale(hid_t did, hid_t dsid, unsigned int idx)
{
    htri_t       has_dimlist;
    htri_t       has_reflist;
    hssize_t     nelmts;
    hid_t        dsid_j;                  /* DS dataset ID in DIMENSION_LIST */
    hid_t        did_i;                   /* dataset ID in REFERENCE_LIST */
    hid_t        sid   = H5I_INVALID_HID; /* space ID */
    hid_t        sid_w = H5I_INVALID_HID; /* space ID */
    hid_t        tid   = H5I_INVALID_HID; /* attribute type ID */
    hid_t        ntid  = H5I_INVALID_HID; /* attribute native type ID */
    hid_t        aid   = H5I_INVALID_HID; /* attribute ID */
    int          rank;                    /* rank of dataset */
    nds_list_t  *ndsbuf   = NULL;         /* array of attribute data in the DS pointing to the dataset */
    nds_list_t  *ndsbuf_w = NULL; /* array of attribute data in the DS pointing to the dataset to write*/
    ds_list_t   *dsbuf    = NULL; /* array of attribute data in the DS pointing to the dataset */
    ds_list_t   *dsbuf_w  = NULL; /* array of attribute data in the DS pointing to the dataset to write*/
    hsize_t      dims[1];         /* dimension of the "REFERENCE_LIST" array */
    H5R_ref_t    nref;
    hobj_ref_t   ref;        /* reference to the DS */
    hvl_t       *buf = NULL; /* VL buffer to store in the attribute */
    int          i;
    size_t       j;
    hssize_t     ii;
    H5O_info2_t  did_oi, dsid_oi, tmp_oi;
    int          found_dset = 0, found_ds = 0;
    int          have_ds = 0;
    htri_t       is_scale;
    bool         is_new_ref;
    unsigned int tmp_idx;
    hid_t        tmp_id;

    /*-------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */

    /* check for valid types of identifiers */

    if (H5I_DATASET != H5Iget_type(did) || H5I_DATASET != H5Iget_type(dsid))
        return FAIL;

    if ((is_scale = H5DSis_scale(did)) < 0)
        return FAIL;

    /* the dataset cannot be a DS dataset */
    if (is_scale == 1)
        return FAIL;

    /* get info for the dataset in the parameter list */
    if (H5Oget_info3(did, &did_oi, H5O_INFO_BASIC) < 0)
        return FAIL;

    /* get info for the scale in the parameter list */
    if (H5Oget_info3(dsid, &dsid_oi, H5O_INFO_BASIC) < 0)
        return FAIL;

    /* same object, not valid */
    if (did_oi.fileno == dsid_oi.fileno) {
        int token_cmp;

        if (H5Otoken_cmp(did, &did_oi.token, &dsid_oi.token, &token_cmp) < 0)
            return FAIL;
        if (!token_cmp)
            return FAIL;
    } /* end if */

    /*-------------------------------------------------------------------------
     * determine if old or new references should be used
     *-------------------------------------------------------------------------
     */
    if (H5DSwith_new_ref(did, &is_new_ref) < 0)
        return FAIL;

    /*-------------------------------------------------------------------------
     * find "DIMENSION_LIST"
     *-------------------------------------------------------------------------
     */

    /* Try to find the attribute "DIMENSION_LIST" on the >>data<< dataset */
    if ((has_dimlist = H5Aexists(did, DIMENSION_LIST)) < 0)
        return FAIL;
    if (has_dimlist == 0)
        return FAIL;

    /* get dataset space */
    if ((sid = H5Dget_space(did)) < 0)
        return FAIL;

    /* get rank */
    if ((rank = H5Sget_simple_extent_ndims(sid)) < 0)
        goto out;

    /* close dataset space */
    if (H5Sclose(sid) < 0)
        return FAIL;

    /* parameter range checking */
    if (idx > (unsigned)rank - 1)
        return FAIL;

    /*-------------------------------------------------------------------------
     * find "REFERENCE_LIST"
     *-------------------------------------------------------------------------
     */

    /* try to find the attribute "REFERENCE_LIST" on the >>DS<< dataset */
    if ((has_reflist = H5Aexists(dsid, REFERENCE_LIST)) < 0)
        return FAIL;
    if (has_reflist == 0)
        return FAIL;

    /*-------------------------------------------------------------------------
     * open "DIMENSION_LIST", and delete the reference
     *-------------------------------------------------------------------------
     */

    if ((aid = H5Aopen(did, DIMENSION_LIST, H5P_DEFAULT)) < 0)
        return FAIL;

    if ((tid = H5Aget_type(aid)) < 0)
        goto out;

    if ((sid = H5Aget_space(aid)) < 0)
        goto out;

    /* allocate and initialize the VL */
    buf = (hvl_t *)malloc((size_t)rank * sizeof(hvl_t));
    if (buf == NULL)
        goto out;

    /* read */
    if (H5Aread(aid, tid, buf) < 0)
        goto out;

    /* reset */
    if (buf[idx].len > 0) {
        for (j = 0; j < buf[idx].len; j++) {
            if (is_new_ref) {
                /* get the reference */
                nref = ((H5R_ref_t *)buf[idx].p)[j];

                /* get the scale id for this REF */
                if ((dsid_j = H5Ropen_object(&nref, H5P_DEFAULT, H5P_DEFAULT)) < 0)
                    goto out;
            }
            else {
                /* get the reference */
                ref = ((hobj_ref_t *)buf[idx].p)[j];

                /* get the DS id */
                if ((dsid_j = H5Rdereference2(did, H5P_DEFAULT, H5R_OBJECT, &ref)) < 0)
                    goto out;
            }
            /* get info for this DS */
            if (H5Oget_info3(dsid_j, &tmp_oi, H5O_INFO_BASIC) < 0)
                goto out;

            /* Close the dereferenced dataset */
            if (H5Dclose(dsid_j) < 0)
                goto out;

            /* same object, reset */
            if (dsid_oi.fileno == tmp_oi.fileno) {
                int token_cmp;

                if (H5Otoken_cmp(did, &dsid_oi.token, &tmp_oi.token, &token_cmp) < 0)
                    goto out;
                if (!token_cmp) {
                    /* If there are more than one reference in the VL element
                       and the reference we found is not the last one,
                       copy the last one to replace the found one since the order
                       of the references doesn't matter according to the spec;
                       reduce the size of the VL element by 1;
                       if the length of the element becomes 0, free the pointer
                       and reset to NULL */

                    size_t len = buf[idx].len;

                    if (j < len - 1) {
                        if (is_new_ref) {
                            ((H5R_ref_t *)buf[idx].p)[j] = ((H5R_ref_t *)buf[idx].p)[len - 1];
                        }
                        else {
                            ((hobj_ref_t *)buf[idx].p)[j] = ((hobj_ref_t *)buf[idx].p)[len - 1];
                        }
                    }
                    len = --buf[idx].len;
                    if (len == 0) {
                        free(buf[idx].p);
                        buf[idx].p = NULL;
                    }
                    /* Since a reference to a dim. scale can be inserted only once,
                       we do not need to continue the search if it is found */
                    found_ds = 1;
                    break;
                } /* end if */
            }     /* end if */
        }         /* j */
    }             /* if */

    /* the scale must be present to continue */
    if (found_ds == 0)
        goto out;

    /* Write the attribute, but check first, if we have any scales left,
       because if not, we should delete the attribute according to the spec */
    for (i = 0; i < rank; i++) {
        if (buf[i].len > 0) {
            have_ds = 1;
            break;
        }
    }
    if (have_ds) {
        if (H5Awrite(aid, tid, buf) < 0)
            goto out;
    }
    else {
        if (H5Adelete(did, DIMENSION_LIST) < 0)
            goto out;
    }

    /* close */
    if (H5Treclaim(tid, sid, H5P_DEFAULT, buf) < 0)
        goto out;
    if (H5Sclose(sid) < 0)
        goto out;
    if (H5Tclose(tid) < 0)
        goto out;
    if (H5Aclose(aid) < 0)
        goto out;

    free(buf);
    buf = NULL;

    /*-------------------------------------------------------------------------
     * the "REFERENCE_LIST" array exists, update
     *-------------------------------------------------------------------------
     */

    if ((aid = H5Aopen(dsid, REFERENCE_LIST, H5P_DEFAULT)) < 0)
        goto out;

    if ((tid = H5Aget_type(aid)) < 0)
        goto out;

    /* get native type to read attribute REFERENCE_LIST */
    if ((ntid = H5Tget_native_type(tid, H5T_DIR_ASCEND)) < 0)
        goto out;

    /* get and save the old reference(s) */
    if ((sid = H5Aget_space(aid)) < 0)
        goto out;

    if ((nelmts = H5Sget_simple_extent_npoints(sid)) < 0)
        goto out;

    if (is_new_ref) {
        ndsbuf = (nds_list_t *)malloc((size_t)nelmts * sizeof(nds_list_t));
        if (ndsbuf == NULL)
            goto out;
        if (H5Aread(aid, ntid, ndsbuf) < 0)
            goto out;
        ndsbuf_w = (nds_list_t *)malloc((size_t)nelmts * sizeof(nds_list_t));
        if (ndsbuf_w == NULL)
            goto out;
    }
    else {
        dsbuf = (ds_list_t *)malloc((size_t)nelmts * sizeof(ds_list_t));
        if (dsbuf == NULL)
            goto out;
        if (H5Aread(aid, ntid, dsbuf) < 0)
            goto out;
        dsbuf_w = (ds_list_t *)malloc((size_t)nelmts * sizeof(ds_list_t));
        if (dsbuf_w == NULL)
            goto out;
    }
    /* Recreate the references we read from the existing "REFERENCE_LIST" attribute */
    for (i = 0; i < nelmts; i++) {
        if (is_new_ref) {
            ndsbuf_w[i].dim_idx = ndsbuf[i].dim_idx;
            tmp_id              = H5Ropen_object(&ndsbuf[i].ref, H5P_DEFAULT, H5P_DEFAULT);
            if (tmp_id < 0)
                goto out;
            if (H5Rcreate_object(tmp_id, ".", H5P_DEFAULT, &ndsbuf_w[i].ref) < 0) {
                H5Dclose(tmp_id);
                goto out;
            }
            H5Dclose(tmp_id);
        }
        else {
            dsbuf_w[i] = dsbuf[i];
        }
    }
    for (ii = 0; ii < nelmts; ii++) {
        /* First check if we have the same dimension index */
        if (is_new_ref) {
            tmp_idx = ndsbuf_w[ii].dim_idx;
        }
        else {
            tmp_idx = dsbuf_w[ii].dim_idx;
        }
        if (idx == tmp_idx) {
            /* get the reference to the dataset */
            if (is_new_ref) {
                /* get the dataset id */
                nref = ndsbuf_w[ii].ref;
                if ((did_i = H5Ropen_object(&nref, H5P_DEFAULT, H5P_DEFAULT)) < 0)
                    goto out;
            }
            else {
                /* get the dataset id */
                ref = dsbuf_w[ii].ref;
                if ((did_i = H5Rdereference2(did, H5P_DEFAULT, H5R_OBJECT, &ref)) < 0)
                    goto out;
            }

            /* get info for this dataset */
            if (H5Oget_info3(did_i, &tmp_oi, H5O_INFO_BASIC) < 0)
                goto out;

            /* close the dereferenced dataset */
            if (H5Dclose(did_i) < 0)
                goto out;

            /* same object, reset. we want to detach only for this DIM */
            if (did_oi.fileno == tmp_oi.fileno) {
                int token_cmp;

                if (H5Otoken_cmp(did, &did_oi.token, &tmp_oi.token, &token_cmp) < 0)
                    goto out;
                if (!token_cmp) {
                    /* copy the last one to replace the one which is found */
                    if (is_new_ref) {
                        ndsbuf_w[ii] = ndsbuf_w[nelmts - 1];
                    }
                    else {
                        dsbuf_w[ii] = dsbuf_w[nelmts - 1];
                    }
                    nelmts--;
                    found_dset = 1;
                    break;
                } /* end if */
            }     /* end if */
        }         /* if we have the same dimension index */
    }             /* ii */

    /* close attribute */
    if (H5Aclose(aid) < 0)
        goto out;

    /*-------------------------------------------------------------------------
     * check if we found the pointed dataset
     *-------------------------------------------------------------------------
     */

    /* the pointed dataset must exist */
    if (found_dset == 0)
        goto out;

    /*-------------------------------------------------------------------------
     * create a new attribute
     *-------------------------------------------------------------------------
     */

    /* the attribute must be deleted, in order to the new one can reflect the changes*/
    if (H5Adelete(dsid, REFERENCE_LIST) < 0)
        goto out;

    /* don't do anything for an empty array */
    if (nelmts) {
        /* create a new data space for the new references array */
        dims[0] = (hsize_t)nelmts;

        if ((sid_w = H5Screate_simple(1, dims, NULL)) < 0)
            goto out;

        /* create the attribute again with the changes of space */
        if ((aid = H5Acreate2(dsid, REFERENCE_LIST, tid, sid_w, H5P_DEFAULT, H5P_DEFAULT)) < 0)
            goto out;

        /* write the new attribute with the new references */
        if (is_new_ref) {
            if (H5Awrite(aid, ntid, ndsbuf_w) < 0)
                goto out;
        }
        else {
            if (H5Awrite(aid, ntid, dsbuf_w) < 0)
                goto out;
        }

        if (H5Aclose(aid) < 0)
            goto out;
    } /* nelmts */

    /* Free references */
    if (is_new_ref) {
        if (H5Treclaim(tid, sid, H5P_DEFAULT, ndsbuf) < 0)
            goto out;
        if (H5Sclose(sid) < 0)
            goto out;
        if (sid_w > 0) {
            if (H5Treclaim(tid, sid_w, H5P_DEFAULT, ndsbuf_w) < 0)
                goto out;
            if (H5Sclose(sid_w) < 0)
                goto out;
        }
    }
    else {
        if (H5Treclaim(tid, sid, H5P_DEFAULT, dsbuf) < 0)
            goto out;
        if (H5Sclose(sid) < 0)
            goto out;
        if (sid_w > 0) {
            if (H5Treclaim(tid, sid_w, H5P_DEFAULT, dsbuf_w) < 0)
                goto out;
            if (H5Sclose(sid_w) < 0)
                goto out;
        }
    }
    /* close type */
    if (H5Tclose(tid) < 0)
        goto out;
    if (H5Tclose(ntid) < 0)
        goto out;
    if (is_new_ref) {
        free(ndsbuf);
        free(ndsbuf_w);
        ndsbuf   = NULL;
        ndsbuf_w = NULL;
    }
    else {
        free(dsbuf);
        free(dsbuf_w);
        dsbuf   = NULL;
        dsbuf_w = NULL;
    }

    return SUCCEED;

    /* error zone */
out:
    H5E_BEGIN_TRY
    {
        H5Sclose(sid);
        H5Aclose(aid);
        H5Tclose(ntid);
        H5Tclose(tid);

        if (ndsbuf) {
            free(ndsbuf);
            ndsbuf = NULL;
        }
        if (ndsbuf_w) {
            free(ndsbuf_w);
            ndsbuf_w = NULL;
        }
        if (dsbuf) {
            free(dsbuf);
            dsbuf = NULL;
        }
        if (buf) {
            /* Failure occurred before H5Treclaim was called;
               free the pointers allocated when we read data in */
            for (i = 0; i < rank; i++) {
                if (buf[i].p)
                    free(buf[i].p);
            }
            free(buf);
            buf = NULL;
        }
    }
    H5E_END_TRY
    return FAIL;
}

/*-------------------------------------------------------------------------
 * Function: H5DSis_attached
 *
 * Purpose: Report if dimension scale DSID is currently attached to
 *  dimension IDX of dataset DID by checking if DID has a pointer in the REFERENCE_LIST
 *  attribute and DSID (scale ) has a pointer in the DIMENSION_LIST attribute
 *
 * Return:
 *   1: both the DS and the dataset pointers match
 *   0: one of them or both do not match
 *   FAIL (-1): error
 *
 * Fails if: Bad arguments
 *           If DSID is not a Dimension Scale
 *           If DID is a Dimension Scale (A Dimension Scale cannot have scales)
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5DSis_attached(hid_t did, hid_t dsid, unsigned int idx)
{
    htri_t      has_dimlist;
    htri_t      has_reflist;
    hssize_t    nelmts;
    hid_t       sid;                    /* space ID */
    hid_t       tid  = H5I_INVALID_HID; /* attribute type ID */
    hid_t       ntid = H5I_INVALID_HID; /* attribute native type ID */
    hid_t       aid  = H5I_INVALID_HID; /* attribute ID */
    int         rank;                   /* rank of dataset */
    nds_list_t *ndsbuf = NULL;          /* array of attribute data in the DS pointing to the dataset */
    ds_list_t  *dsbuf  = NULL;          /* array of attribute data in the DS pointing to the dataset */
    H5R_ref_t   nref;                   /* reference to the DS */
    hobj_ref_t  ref;                    /* reference to the DS */
    hvl_t      *buf = NULL;             /* VL buffer to store in the attribute */
    hid_t       dsid_j;                 /* DS dataset ID in DIMENSION_LIST */
    hid_t       did_i;                  /* dataset ID in REFERENCE_LIST */
    H5O_info2_t oi1, oi2, oi3, oi4;
    H5I_type_t  it1, it2;
    int         i;
    int         found_dset = 0, found_ds = 0;
    htri_t      is_scale;
    bool        is_new_ref;

    /*-------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */

    if ((is_scale = H5DSis_scale(did)) < 0)
        return FAIL;

    /* the dataset cannot be a DS dataset */
    if (is_scale == 1)
        return FAIL;

    /* get info for the dataset in the parameter list */
    if (H5Oget_info3(did, &oi1, H5O_INFO_BASIC) < 0)
        return FAIL;

    /* get info for the scale in the parameter list */
    if (H5Oget_info3(dsid, &oi2, H5O_INFO_BASIC) < 0)
        return FAIL;

    /* same object, not valid */
    if (oi1.fileno == oi2.fileno) {
        int token_cmp;

        if (H5Otoken_cmp(did, &oi1.token, &oi2.token, &token_cmp) < 0)
            return FAIL;
        if (!token_cmp)
            return FAIL;
    } /* end if */

    /*-------------------------------------------------------------------------
     * determine if old or new references should be used
     *-------------------------------------------------------------------------
     */

    if (H5DSwith_new_ref(did, &is_new_ref) < 0)
        return FAIL;

    /* get ID type */
    if ((it1 = H5Iget_type(did)) < 0)
        return FAIL;
    if ((it2 = H5Iget_type(dsid)) < 0)
        return FAIL;

    if (H5I_DATASET != it1 || H5I_DATASET != it2)
        return FAIL;

    /*-------------------------------------------------------------------------
     * get space
     *-------------------------------------------------------------------------
     */

    /* get dataset space */
    if ((sid = H5Dget_space(did)) < 0)
        return FAIL;

    /* get rank */
    if ((rank = H5Sget_simple_extent_ndims(sid)) < 0)
        goto out;

    /* close dataset space */
    if (H5Sclose(sid) < 0)
        goto out;

    /* parameter range checking */
    if (idx > ((unsigned)rank - 1))
        return FAIL;

    /* try to find the attribute "DIMENSION_LIST" on the >>data<< dataset */
    if ((has_dimlist = H5Aexists(did, DIMENSION_LIST)) < 0)
        return FAIL;

    /*-------------------------------------------------------------------------
     * open "DIMENSION_LIST"
     *-------------------------------------------------------------------------
     */

    if (has_dimlist > 0) {
        if ((aid = H5Aopen(did, DIMENSION_LIST, H5P_DEFAULT)) < 0)
            goto out;

        if ((tid = H5Aget_type(aid)) < 0)
            goto out;

        if ((sid = H5Aget_space(aid)) < 0)
            goto out;

        /* allocate and initialize the VL */
        buf = (hvl_t *)malloc((size_t)rank * sizeof(hvl_t));
        if (buf == NULL)
            goto out;

        /* read */
        if (H5Aread(aid, tid, buf) < 0)
            goto out;

        /* iterate all the REFs in this dimension IDX */
        for (i = 0; i < (int)buf[idx].len; i++) {
            if (is_new_ref) {
                /* get the reference */
                nref = ((H5R_ref_t *)buf[idx].p)[i];

                /* get the scale id for this REF */
                if ((dsid_j = H5Ropen_object(&nref, H5P_DEFAULT, H5P_DEFAULT)) < 0)
                    goto out;
            }
            else {
                /* get the reference */
                ref = ((hobj_ref_t *)buf[idx].p)[i];

                /* get the scale id for this REF */
                if ((dsid_j = H5Rdereference2(did, H5P_DEFAULT, H5R_OBJECT, &ref)) < 0)
                    goto out;
            }

            /* get info for DS in the parameter list */
            if (H5Oget_info3(dsid, &oi1, H5O_INFO_BASIC) < 0)
                goto out;

            /* get info for this DS */
            if (H5Oget_info3(dsid_j, &oi2, H5O_INFO_BASIC) < 0)
                goto out;

            /* same object */
            if (oi1.fileno == oi2.fileno) {
                int token_cmp;

                if (H5Otoken_cmp(did, &oi1.token, &oi2.token, &token_cmp) < 0)
                    goto out;
                if (!token_cmp)
                    found_ds = 1;
            } /* end if */

            /* close the dereferenced dataset */
            if (H5Dclose(dsid_j) < 0)
                goto out;
        }

        /* close */
        if (H5Treclaim(tid, sid, H5P_DEFAULT, buf) < 0)
            goto out;
        if (H5Sclose(sid) < 0)
            goto out;
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
        free(buf);
        buf = NULL;
    } /* has_dimlist */

    /*-------------------------------------------------------------------------
     * info on the >>DS<< dataset
     *-------------------------------------------------------------------------
     */

    /* try to find the attribute "REFERENCE_LIST" on the >>DS<< dataset */
    if ((has_reflist = H5Aexists(dsid, REFERENCE_LIST)) < 0)
        goto out;

    /*-------------------------------------------------------------------------
     * open "REFERENCE_LIST"
     *-------------------------------------------------------------------------
     */

    if (has_reflist > 0) {
        if ((aid = H5Aopen(dsid, REFERENCE_LIST, H5P_DEFAULT)) < 0)
            goto out;

        if ((tid = H5Aget_type(aid)) < 0)
            goto out;

        /* get native type to read REFERENCE_LIST attribute */
        if ((ntid = H5Tget_native_type(tid, H5T_DIR_ASCEND)) < 0)
            goto out;

        /* get and save the old reference(s) */
        if ((sid = H5Aget_space(aid)) < 0)
            goto out;

        if ((nelmts = H5Sget_simple_extent_npoints(sid)) < 0)
            goto out;

        if (is_new_ref) {
            ndsbuf = (nds_list_t *)malloc((size_t)nelmts * sizeof(nds_list_t));
            if (ndsbuf == NULL)
                goto out;
            if (H5Aread(aid, ntid, ndsbuf) < 0)
                goto out;
        }
        else {
            dsbuf = (ds_list_t *)malloc((size_t)nelmts * sizeof(ds_list_t));
            if (dsbuf == NULL)
                goto out;
            if (H5Aread(aid, ntid, dsbuf) < 0)
                goto out;
        }

        /*-------------------------------------------------------------------------
         * iterate
         *-------------------------------------------------------------------------
         */

        for (i = 0; i < nelmts; i++) {

            if (is_new_ref) {
                nref = ndsbuf[i].ref;
                /* get the dataset id */
                if ((did_i = H5Ropen_object(&nref, H5P_DEFAULT, H5P_DEFAULT)) < 0)
                    goto out;
            }
            else {
                ref = dsbuf[i].ref;
                /* get the dataset id */
                if ((did_i = H5Rdereference2(did, H5P_DEFAULT, H5R_OBJECT, &ref)) < 0)
                    goto out;
            }

            /* get info for dataset in the parameter list */
            if (H5Oget_info3(did, &oi3, H5O_INFO_BASIC) < 0)
                goto out;

            /* get info for this dataset */
            if (H5Oget_info3(did_i, &oi4, H5O_INFO_BASIC) < 0)
                goto out;

            /* same object */
            if (oi3.fileno == oi4.fileno) {
                int token_cmp;

                if (H5Otoken_cmp(did, &oi3.token, &oi4.token, &token_cmp) < 0)
                    goto out;
                if (is_new_ref) {
                    if (!token_cmp && (idx == ndsbuf[i].dim_idx))
                        found_dset = 1;
                }
                else {
                    if (!token_cmp && (idx == dsbuf[i].dim_idx))
                        found_dset = 1;
                }
            } /* end if */

            /* close the dereferenced dataset */
            if (H5Dclose(did_i) < 0)
                goto out;
        } /* for */

        /* close */
        if (is_new_ref) {
            if (H5Treclaim(ntid, sid, H5P_DEFAULT, ndsbuf) < 0)
                goto out;
        }
        else {
            if (H5Treclaim(ntid, sid, H5P_DEFAULT, dsbuf) < 0)
                goto out;
        }
        if (H5Sclose(sid) < 0)
            goto out;
        if (H5Tclose(ntid) < 0)
            goto out;
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;

        if (ndsbuf) {
            free(ndsbuf);
            ndsbuf = NULL;
        }
        if (dsbuf) {
            free(dsbuf);
            dsbuf = NULL;
        }
    } /* has_reflist */

    if (found_ds && found_dset)
        return 1;
    else
        return 0;

    /* error zone */
out:
    H5E_BEGIN_TRY
    {
        H5Sclose(sid);
        H5Aclose(aid);
        H5Tclose(tid);
        H5Tclose(ntid);
    }
    H5E_END_TRY

    if (buf) {
        free(buf);
        buf = NULL;
    }
    if (ndsbuf) {
        free(ndsbuf);
        ndsbuf = NULL;
    }
    if (dsbuf) {
        free(dsbuf);
        dsbuf = NULL;
    }
    return FAIL;
}

/*-------------------------------------------------------------------------
 * Function: H5DSiterate_scales
 *
 * Purpose: H5DSiterate_scales iterates over the scales attached to dimension DIM
 *  of dataset DID. For each scale in the list, the visitor_data and some
 *  additional information, specified below, are passed to the visitor function.
 *  The iteration begins with the IDX object in the group and the next element
 *  to be processed by the operator is returned in IDX. If IDX is NULL, then the
 *  iterator starts at zero.
 *
 * Parameters:
 *
 *  hid_t DID;               IN: the dataset
 *  unsigned int DIM;        IN: the dimension of the dataset
 *  int *DS_IDX;             IN/OUT: on input the dimension scale index to start iterating,
 *                               on output the next index to visit. If NULL, start at
 *                               the first position.
 *  H5DS_iterate_t VISITOR;  IN: the visitor function
 *  void *VISITOR_DATA;      IN: arbitrary data to pass to the visitor function.
 *
 *  Iterate over all scales of DIM, calling an application callback
 *   with the item, key and any operator data.
 *
 *   The operator callback receives a pointer to the item ,
 *   and the pointer to the operator data passed
 *   in to H5SL_iterate ('op_data').  The return values from an operator are:
 *       A. Zero causes the iterator to continue, returning zero when all
 *           nodes of that type have been processed.
 *       B. Positive causes the iterator to immediately return that positive
 *           value, indicating short-circuit success.
 *       C. Negative causes the iterator to immediately return that value,
 *           indicating failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5DSiterate_scales(hid_t did, unsigned int dim, int *ds_idx, H5DS_iterate_t visitor, void *visitor_data)
{
    hid_t      scale_id;
    int        rank;
    H5R_ref_t  nref;                  /* reference to the DS */
    hobj_ref_t ref;                   /* reference to the DS */
    hid_t      sid;                   /* space ID */
    hid_t      tid = H5I_INVALID_HID; /* attribute type ID */
    hid_t      aid = H5I_INVALID_HID; /* attribute ID */
    hvl_t     *buf = NULL;            /* VL buffer to store in the attribute */
    H5I_type_t it;                    /* ID type */
    herr_t     ret_value = 0;
    int        j_idx;
    int        nscales;
    htri_t     has_dimlist;
    int        i;
    bool       is_new_ref;

    /*-------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */
    /* get ID type */
    if ((it = H5Iget_type(did)) < 0)
        return FAIL;

    if (H5I_DATASET != it)
        return FAIL;

    /*-------------------------------------------------------------------------
     * determine if old or new references should be used
     *-------------------------------------------------------------------------
     */

    if (H5DSwith_new_ref(did, &is_new_ref) < 0)
        return FAIL;

    /* get the number of scales associated with this DIM */
    if ((nscales = H5DSget_num_scales(did, dim)) < 0)
        return FAIL;

    /* parameter range checking */
    if (ds_idx != NULL) {
        if (*ds_idx >= nscales)
            return FAIL;
    }

    /* get dataset space */
    if ((sid = H5Dget_space(did)) < 0)
        return FAIL;

    /* get rank */
    if ((rank = H5Sget_simple_extent_ndims(sid)) < 0)
        goto out;

    /* close dataset space */
    if (H5Sclose(sid) < 0)
        goto out;

    if (dim >= (unsigned)rank)
        return FAIL;

    /* Try to find the attribute "DIMENSION_LIST" on the >>data<< dataset */
    if ((has_dimlist = H5Aexists(did, DIMENSION_LIST)) < 0)
        return FAIL;
    if (has_dimlist == 0)
        return SUCCEED;

    else if (has_dimlist > 0) {
        if ((aid = H5Aopen(did, DIMENSION_LIST, H5P_DEFAULT)) < 0)
            goto out;
        if ((tid = H5Aget_type(aid)) < 0)
            goto out;
        if ((sid = H5Aget_space(aid)) < 0)
            goto out;

        /* allocate and initialize the VL */
        buf = (hvl_t *)malloc((size_t)rank * sizeof(hvl_t));

        if (buf == NULL)
            goto out;

        /* read */
        if (H5Aread(aid, tid, buf) < 0)
            goto out;

        if (buf[dim].len > 0) {
            if (ds_idx != NULL)
                j_idx = *ds_idx;
            else
                j_idx = 0;

            /* iterate */
            for (i = j_idx; i < nscales; i++) {
                if (is_new_ref) {
                    /* get the reference */
                    nref = ((H5R_ref_t *)buf[dim].p)[i];

                    /* disable error reporting, the ID might refer to a deleted dataset */
                    H5E_BEGIN_TRY
                    {
                        /* get the DS id */
                        if ((scale_id = H5Ropen_object(&nref, H5P_DEFAULT, H5P_DEFAULT)) < 0)
                            goto out;
                    }
                    H5E_END_TRY
                }
                else {
                    /* get the reference */
                    ref = ((hobj_ref_t *)buf[dim].p)[i];

                    /* disable error reporting, the ID might refer to a deleted dataset */
                    H5E_BEGIN_TRY
                    {
                        /* get the DS id */
                        if ((scale_id = H5Rdereference2(did, H5P_DEFAULT, H5R_OBJECT, &ref)) < 0)
                            goto out;
                    }
                    H5E_END_TRY
                }

                /* set the return IDX OUT value at current scale index */
                if (ds_idx != NULL) {
                    *ds_idx = i;
                }

                if ((ret_value = (visitor)(did, dim, scale_id, visitor_data)) != 0) {
                    /* break */

                    /* close the DS id */
                    if (H5Dclose(scale_id) < 0)
                        goto out;

                    break;
                }

                /* close the DS id */
                if (H5Dclose(scale_id) < 0)
                    goto out;

            } /* i */
        }     /* if */

        /* close */
        if (H5Treclaim(tid, sid, H5P_DEFAULT, buf) < 0)
            goto out;
        if (H5Sclose(sid) < 0)
            goto out;
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;

        free(buf);
        buf = NULL;
    } /* if has_dimlist */

    return ret_value;

out:
    H5E_BEGIN_TRY
    {
        if (buf) {
            H5Treclaim(tid, sid, H5P_DEFAULT, buf);
            free(buf);
        }
        H5Sclose(sid);
        H5Aclose(aid);
        H5Tclose(tid);
    }
    H5E_END_TRY

    return FAIL;
}

/*-------------------------------------------------------------------------
 * Function: H5DSset_label
 *
 * Purpose: Set label for the dimension IDX of dataset DID to the value LABEL
 *
 * Return: Success: SUCCEED, Failure: FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5DSset_label(hid_t did, unsigned int idx, const char *label)
{
    htri_t       has_labels;
    hid_t        sid = H5I_INVALID_HID; /* space ID */
    hid_t        tid = H5I_INVALID_HID; /* attribute type ID */
    hid_t        aid = H5I_INVALID_HID; /* attribute ID */
    int          rank;                  /* rank of dataset */
    hsize_t      dims[1];               /* dimensions of dataset */
    H5I_type_t   it;                    /* ID type */
    unsigned int i;
    union {                     /* union is needed to eliminate compiler warnings about */
        char       **buf;       /* discarding the 'const' qualifier in the free */
        char const **const_buf; /* buf calls */
    } u;

    memset(&u, 0, sizeof(u));

    /*-------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */
    /* get ID type */
    if ((it = H5Iget_type(did)) < 0)
        return FAIL;

    if (H5I_DATASET != it)
        return FAIL;

    if (label == NULL)
        return FAIL;

    /* get dataset space */
    if ((sid = H5Dget_space(did)) < 0)
        return FAIL;

    /* get rank */
    if ((rank = H5Sget_simple_extent_ndims(sid)) < 0)
        goto out;

    /* close dataset space */
    if (H5Sclose(sid) < 0)
        goto out;

    if (idx >= (unsigned)rank)
        return FAIL;

    /*-------------------------------------------------------------------------
     * attribute "DIMENSION_LABELS"
     *-------------------------------------------------------------------------
     */

    /* try to find the attribute "DIMENSION_LABELS" on the >>data<< dataset */
    if ((has_labels = H5Aexists(did, DIMENSION_LABELS)) < 0)
        return FAIL;

    /*-------------------------------------------------------------------------
     * make the attribute and insert label
     *-------------------------------------------------------------------------
     */

    if (has_labels == 0) {
        dims[0] = (hsize_t)rank;

        /* space for the attribute */
        if ((sid = H5Screate_simple(1, dims, NULL)) < 0)
            goto out;

        /* create the datatype  */
        if ((tid = H5Tcopy(H5T_C_S1)) < 0)
            goto out;
        if (H5Tset_size(tid, H5T_VARIABLE) < 0)
            goto out;

        /* create the attribute */
        if ((aid = H5Acreate2(did, DIMENSION_LABELS, tid, sid, H5P_DEFAULT, H5P_DEFAULT)) < 0)
            goto out;

        /* allocate and initialize */
        u.const_buf = (char const **)malloc((size_t)rank * sizeof(char *));

        if (u.const_buf == NULL)
            goto out;

        for (i = 0; i < (unsigned int)rank; i++)
            u.const_buf[i] = NULL;

        /* store the label information in the required index */
        u.const_buf[idx] = label;

        /* write the attribute with the label */
        if (H5Awrite(aid, tid, u.const_buf) < 0)
            goto out;

        /* close */
        if (H5Sclose(sid) < 0)
            goto out;
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
        if (u.const_buf) {
            free(u.const_buf);
            u.const_buf = NULL;
        }
    }

    /*-------------------------------------------------------------------------
     * just insert label
     *-------------------------------------------------------------------------
     */

    else {

        if ((aid = H5Aopen(did, DIMENSION_LABELS, H5P_DEFAULT)) < 0)
            goto out;

        if ((tid = H5Aget_type(aid)) < 0)
            goto out;

        /* allocate and initialize */
        u.buf = (char **)malloc((size_t)rank * sizeof(char *));

        if (u.buf == NULL)
            goto out;

        /* read */
        if (H5Aread(aid, tid, (void *)u.buf) < 0)
            goto out;

        /* free the ptr that will be replaced by label */
        if (u.buf[idx])
            free(u.buf[idx]);

        /* store the label information in the required index */
        u.const_buf[idx] = label;

        /* write the attribute with the new references */
        if (H5Awrite(aid, tid, u.buf) < 0)
            goto out;

        /* label was brought in, so don't free */
        u.buf[idx] = NULL;

        /* free all the ptr's from the H5Aread() */
        for (i = 0; i < (unsigned int)rank; i++) {
            if (u.buf[i])
                free(u.buf[i]);
        }

        /* close */
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
        if (u.buf) {
            free(u.buf);
            u.buf = NULL;
        }
    }

    return SUCCEED;

    /* error zone */

out:
    if (u.buf) {
        if (u.buf[idx])        /* check if we errored during H5Awrite */
            u.buf[idx] = NULL; /* don't free label */
        /* free all the ptr's from the H5Aread() */
        for (i = 0; i < (unsigned int)rank; i++) {
            if (u.buf[i])
                free(u.buf[i]);
        }
        free(u.buf);
    }
    H5E_BEGIN_TRY
    {
        H5Sclose(sid);
        H5Aclose(aid);
        H5Tclose(tid);
    }
    H5E_END_TRY
    return FAIL;
}

/*-------------------------------------------------------------------------
 * Function: H5DSget_label
 *
 * Purpose: Read the label LABEL for dimension IDX of dataset DID
 *   Up to 'size' characters are stored in 'label' followed by a '\0' string
 *   terminator.  If the label is longer than 'size'-1,
 *   the string terminator is stored in the last position of the buffer to
 *   properly terminate the string.
 *
 * Return: 0 if no label found, size of label if found, Failure: FAIL
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5DSget_label(hid_t did, unsigned int idx, char *label, size_t size)
{
    htri_t     has_labels;
    hid_t      sid = H5I_INVALID_HID; /* space ID */
    hid_t      tid = H5I_INVALID_HID; /* attribute type ID */
    hid_t      aid = H5I_INVALID_HID; /* attribute ID */
    int        rank;                  /* rank of dataset */
    char     **buf = NULL;            /* buffer to store in the attribute */
    H5I_type_t it;                    /* ID type */
    size_t     nbytes = 0;
    size_t     copy_len;
    int        i;

    /*-------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */
    /* get ID type */
    if ((it = H5Iget_type(did)) < 0)
        return FAIL;

    if (H5I_DATASET != it)
        return FAIL;

    /* get dataset space */
    if ((sid = H5Dget_space(did)) < 0)
        return FAIL;

    /* get rank */
    if ((rank = H5Sget_simple_extent_ndims(sid)) < 0)
        goto out;

    /* close dataset space */
    if (H5Sclose(sid) < 0)
        goto out;

    if (idx >= (unsigned)rank)
        return FAIL;

    /*-------------------------------------------------------------------------
     * attribute "DIMENSION_LABELS"
     *-------------------------------------------------------------------------
     */

    /* Try to find the attribute "DIMENSION_LABELS" on the >>data<< dataset */
    if ((has_labels = H5Aexists(did, DIMENSION_LABELS)) < 0)
        return FAIL;

    /* Return 0 and NULL for label if no label found */
    if (has_labels == 0) {
        if (label)
            label[0] = 0;
        return 0;
    }

    /*-------------------------------------------------------------------------
     * open the attribute and read label
     *-------------------------------------------------------------------------
     */

    if ((aid = H5Aopen(did, DIMENSION_LABELS, H5P_DEFAULT)) < 0)
        goto out;

    if ((tid = H5Aget_type(aid)) < 0)
        goto out;

    /* allocate and initialize */
    buf = (char **)malloc((size_t)rank * sizeof(char *));

    if (buf == NULL)
        goto out;

    /* read */
    if (H5Aread(aid, tid, buf) < 0)
        goto out;

    /* do only if the label name exists for the dimension */
    if (buf[idx] != NULL) {
        /* get the real string length */
        nbytes = strlen(buf[idx]);

        /* compute the string length which will fit into the user's buffer */
        copy_len = MIN(size - 1, nbytes);

        /* copy all/some of the name */
        if (label) {
            memcpy(label, buf[idx], copy_len);

            /* terminate the string */
            label[copy_len] = '\0';
        }
    }
    /* free all the ptr's from the H5Aread() */
    for (i = 0; i < rank; i++) {
        if (buf[i])
            free(buf[i]);
    }

    /* close */
    if (H5Tclose(tid) < 0)
        goto out;
    if (H5Aclose(aid) < 0)
        goto out;
    if (buf) {
        free(buf);
        buf = NULL;
    }

    return (ssize_t)nbytes;

    /* error zone */
out:
    if (buf) {
        /* free all the ptr's from the H5Aread() */
        for (i = 0; i < rank; i++) {
            if (buf[i])
                free(buf[i]);
        }
        free(buf);
    }
    H5E_BEGIN_TRY
    {
        H5Sclose(sid);
        H5Aclose(aid);
        H5Tclose(tid);
    }
    H5E_END_TRY
    return FAIL;
}

/*-------------------------------------------------------------------------
 * Function: H5DSget_scale_name
 *
 * Purpose: Read the name of dataset scale DID into buffer NAME
 *   Up to 'size' characters are stored in 'name' followed by a '\0' string
 *   terminator.  If the name is longer than 'size'-1,
 *   the string terminator is stored in the last position of the buffer to
 *   properly terminate the string.
 *
 * Return: size of name if found, zero if not found,  Failure: FAIL
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5DSget_scale_name(hid_t did, char *name, size_t size)
{
    hid_t      aid = H5I_INVALID_HID; /* attribute ID  */
    hid_t      tid = H5I_INVALID_HID; /* attribute type ID */
    hid_t      sid = H5I_INVALID_HID; /* space ID  */
    H5I_type_t it;                    /* ID type */
    size_t     nbytes;
    size_t     copy_len;
    htri_t     has_name;
    char      *buf = NULL;

    /*-------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */
    /* get ID type */
    if ((it = H5Iget_type(did)) < 0)
        return FAIL;

    if (H5I_DATASET != it)
        return FAIL;

    if ((H5DSis_scale(did)) <= 0)
        return FAIL;

    /*-------------------------------------------------------------------------
     * check if the DS has a name
     *-------------------------------------------------------------------------
     */

    /* try to find the attribute "NAME" on the >>DS<< dataset */
    if ((has_name = H5Aexists(did, "NAME")) < 0)
        return FAIL;
    if (has_name == 0)
        return 0;

    /*-------------------------------------------------------------------------
     * open the attribute
     *-------------------------------------------------------------------------
     */

    if ((aid = H5Aopen(did, "NAME", H5P_DEFAULT)) < 0)
        return FAIL;

    /* get space */
    if ((sid = H5Aget_space(aid)) < 0)
        goto out;

    /* get type */
    if ((tid = H5Aget_type(aid)) < 0)
        goto out;

    /* get the size */
    if ((nbytes = H5Tget_size(tid)) == 0)
        goto out;

    /* allocate a temporary buffer */
    buf = (char *)malloc(nbytes * sizeof(char));
    if (buf == NULL)
        goto out;

    /* read */
    if (H5Aread(aid, tid, buf) < 0)
        goto out;

    /* compute the string length which will fit into the user's buffer */
    copy_len = MIN(size - 1, nbytes);

    /* copy all/some of the name */
    if (name) {
        memcpy(name, buf, copy_len);

        /* terminate the string */
        name[copy_len] = '\0';
    }

    /* close */
    if (H5Tclose(tid) < 0)
        goto out;
    if (H5Aclose(aid) < 0)
        goto out;
    if (H5Sclose(sid) < 0)
        goto out;
    if (buf)
        free(buf);

    return (ssize_t)(nbytes - 1);

    /* error zone */
out:
    H5E_BEGIN_TRY
    {
        H5Aclose(aid);
        H5Tclose(tid);
        H5Sclose(sid);
    }
    H5E_END_TRY
    if (buf)
        free(buf);
    return FAIL;
}

/*-------------------------------------------------------------------------
 * Function: H5DSis_scale
 *
 * Purpose: check if the dataset DID is a dimension scale
 *
 * Return: 1, is, 0, not, FAIL, error
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5DSis_scale(hid_t did)
{
    hid_t       tid = H5I_INVALID_HID; /* attribute type ID */
    hid_t       aid = H5I_INVALID_HID; /* attribute ID */
    htri_t      attr_class;            /* has the "CLASS" attribute */
    htri_t      is_ds = -1;            /* set to "not a dimension scale" */
    H5I_type_t  it;                    /* type of identifier */
    char       *buf = NULL;            /* buffer to read name of attribute */
    size_t      string_size;           /* size of storage for the attribute */
    H5T_class_t type_class;
    H5T_str_t   strpad;

    /*------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */
    /* get ID type */
    if ((it = H5Iget_type(did)) < 0)
        goto out;

    if (H5I_DATASET != it)
        goto out;

    /* try to find the attribute "CLASS" on the dataset */
    if ((attr_class = H5Aexists(did, "CLASS")) < 0)
        goto out;

    if (attr_class == 0) {
        is_ds = 0;
        goto out;
    }
    else {
        if ((aid = H5Aopen(did, "CLASS", H5P_DEFAULT)) < 0)
            goto out;

        if ((tid = H5Aget_type(aid)) < 0)
            goto out;

        /* check to make sure attribute is a string;
           if not, then it is not dimension scale  */
        if ((type_class = H5Tget_class(tid)) < 0)
            goto out;
        if (H5T_STRING != type_class) {
            is_ds = 0;
            goto out;
        }
        /* check to make sure string is null-terminated;
           if not, then it is not dimension scale */
        if ((strpad = H5Tget_strpad(tid)) < 0)
            goto out;
        if (H5T_STR_NULLTERM != strpad) {
            is_ds = 0;
            goto out;
        }

        /* According to Spec string is ASCII and its size should be 16 to hold
           "DIMENSION_SCALE" string */
        if ((string_size = H5Tget_size(tid)) == 0)
            goto out;
        if (string_size != 16) {
            is_ds = 0;
            goto out;
        }

        buf = (char *)malloc((size_t)string_size * sizeof(char));
        if (buf == NULL)
            goto out;

        /* Read the attribute */
        if (H5Aread(aid, tid, buf) < 0)
            goto out;

        /* compare strings */
        if (strncmp(buf, DIMENSION_SCALE_CLASS, MIN(strlen(DIMENSION_SCALE_CLASS), strlen(buf))) == 0)
            is_ds = 1;

        free(buf);

        if (H5Tclose(tid) < 0)
            goto out;

        if (H5Aclose(aid) < 0)
            goto out;
    }
out:
    if (is_ds < 0) {
        free(buf);
        H5E_BEGIN_TRY
        {
            H5Aclose(aid);
            H5Tclose(tid);
        }
        H5E_END_TRY
    }
    return is_ds;
}

/*-------------------------------------------------------------------------
 * Function: H5DSget_num_scales
 *
 * Purpose: get the number of scales linked to the IDX dimension of dataset DID
 *
 * Return:
 *   Success: number of scales
 *   Failure: FAIL
 *
 *-------------------------------------------------------------------------
 */
int
H5DSget_num_scales(hid_t did, unsigned int idx)
{
    htri_t     has_dimlist;
    hid_t      sid;                   /* space ID */
    hid_t      tid = H5I_INVALID_HID; /* attribute type ID */
    hid_t      aid = H5I_INVALID_HID; /* attribute ID */
    int        rank;                  /* rank of dataset */
    hvl_t     *buf = NULL;            /* VL buffer to store in the attribute */
    H5I_type_t it;                    /* ID type */
    int        nscales;

    /*-------------------------------------------------------------------------
     * parameter checking
     *-------------------------------------------------------------------------
     */
    /* get ID type */
    if ((it = H5Iget_type(did)) < 0)
        return FAIL;

    if (H5I_DATASET != it)
        return FAIL;

    /*-------------------------------------------------------------------------
     * the attribute "DIMENSION_LIST" on the >>data<< dataset must exist
     *-------------------------------------------------------------------------
     */
    /* get dataset space */
    if ((sid = H5Dget_space(did)) < 0)
        return FAIL;

    /* get rank */
    if ((rank = H5Sget_simple_extent_ndims(sid)) < 0)
        goto out;

    /* close dataset space */
    if (H5Sclose(sid) < 0)
        goto out;

    /* dimemsion index IDX range checking */
    if (idx >= (unsigned int)rank)
        return FAIL;

    /* Try to find the attribute "DIMENSION_LIST" on the >>data<< dataset */
    if ((has_dimlist = H5Aexists(did, DIMENSION_LIST)) < 0)
        return FAIL;

    /* No scales */
    if (has_dimlist == 0)
        return 0;

    /*-------------------------------------------------------------------------
     * the attribute exists, open it
     *-------------------------------------------------------------------------
     */
    else {
        if ((aid = H5Aopen(did, DIMENSION_LIST, H5P_DEFAULT)) < 0)
            goto out;
        if ((tid = H5Aget_type(aid)) < 0)
            goto out;
        if ((sid = H5Aget_space(aid)) < 0)
            goto out;

        /* allocate and initialize the VL */
        buf = (hvl_t *)malloc((size_t)rank * sizeof(hvl_t));
        if (buf == NULL)
            goto out;

        /* read */
        if (H5Aread(aid, tid, buf) < 0)
            goto out;

        nscales = (int)buf[idx].len;

        /* close */
        if (H5Treclaim(tid, sid, H5P_DEFAULT, buf) < 0)
            goto out;
        if (H5Sclose(sid) < 0)
            goto out;
        if (H5Tclose(tid) < 0)
            goto out;
        if (H5Aclose(aid) < 0)
            goto out;
        free(buf);
        buf = NULL;
    } /* has_dimlist */

    return nscales;

    /* error zone */
out:
    H5E_BEGIN_TRY
    {
        H5Sclose(sid);
        H5Aclose(aid);
        H5Tclose(tid);
    }
    H5E_END_TRY

    if (buf)
        free(buf);

    return FAIL;
}

/*-------------------------------------------------------------------------
 * Function: H5DS_is_reserved
 *
 * Purpose:  Verify that a dataset's CLASS is either an image, palette or
 *           table
 *
 * Return:   SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5DS_is_reserved(hid_t did, bool *is_reserved)
{
    htri_t has_class;
    hid_t  tid = H5I_INVALID_HID;
    hid_t  aid = H5I_INVALID_HID;
    char  *buf = NULL;  /* Name of attribute */
    size_t string_size; /* Size of storage for attribute */

    /* Try to find the attribute "CLASS" on the dataset */
    if ((has_class = H5Aexists(did, "CLASS")) < 0)
        return FAIL;
    if (has_class == 0) {
        *is_reserved = false;
        return SUCCEED;
    }

    if ((aid = H5Aopen(did, "CLASS", H5P_DEFAULT)) < 0)
        goto error;
    if ((tid = H5Aget_type(aid)) < 0)
        goto error;

    /* Check to make sure attribute is a string */
    if (H5T_STRING != H5Tget_class(tid))
        goto error;

    /* Check to make sure string is null-terminated */
    if (H5T_STR_NULLTERM != H5Tget_strpad(tid))
        goto error;

    /* Allocate buffer large enough to hold string */
    if ((string_size = H5Tget_size(tid)) == 0)
        goto error;
    if (NULL == (buf = malloc(string_size * sizeof(char))))
        goto error;

    /* Read the attribute */
    if (H5Aread(aid, tid, buf) < 0)
        goto error;

    if (strncmp(buf, IMAGE_CLASS, MIN(strlen(IMAGE_CLASS), strlen(buf))) == 0 ||
        strncmp(buf, PALETTE_CLASS, MIN(strlen(PALETTE_CLASS), strlen(buf))) == 0 ||
        strncmp(buf, TABLE_CLASS, MIN(strlen(TABLE_CLASS), strlen(buf))) == 0)
        *is_reserved = true;
    else
        *is_reserved = false;

    free(buf);

    if (H5Tclose(tid) < 0)
        goto error;
    if (H5Aclose(aid) < 0)
        goto error;

    return SUCCEED;

error:
    H5E_BEGIN_TRY
    {
        H5Tclose(tid);
        H5Aclose(aid);
    }
    H5E_END_TRY

    free(buf);

    return FAIL;
}
