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

#include "H5LDprivate.h"

/*-------------------------------------------------------------------------
 *
 * internal functions
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5LD_construct_info(H5LD_memb_t *memb, hid_t par_tid);
static herr_t H5LD_get_dset_dims(hid_t did, hsize_t *cur_dims);
static size_t H5LD_get_dset_type_size(hid_t did, const char *fields);
static herr_t H5LD_get_dset_elmts(hid_t did, const hsize_t *prev_dims, const hsize_t *cur_dims,
                                  const char *fields, void *buf);

/*-------------------------------------------------------------------------
 * Function: H5LD_clean_vector
 *
 * Purpose: Process the vector of info:
 *        1) free the array of pointers to member names in listv[n]
 *        2) close the type id of the last member in listv[n]
 *        3) free the H5LD_memb_t structure itself as pointed to by listv[n]
 *
 * Return: void
 *
 *-------------------------------------------------------------------------
 */
void
H5LD_clean_vector(H5LD_memb_t *listv[])
{
    unsigned n; /* Local index variable */

    assert(listv);

    /* Go through info for each field stored in listv[] */
    for (n = 0; listv[n] != NULL; n++) {
        if (listv[n]->names) {
            free(listv[n]->names);
            listv[n]->names = NULL;
        } /* end if */

        /* Close the type id of the last member in the field */
        if (!(listv[n]->last_tid < 0)) {
            H5Tclose(listv[n]->last_tid);
            listv[n]->last_tid = -1;
        } /* end if */

        /* Free the H5LD_memb_t structure for the field */
        free(listv[n]);
        listv[n] = NULL;
    } /* end for */
} /* H5LD_clean_vector() */

/*-------------------------------------------------------------------------
 * Function: H5LD_construct_info()
 *
 * Purpose: Get the remaining info for a field:
 *        1) Get the type id of the last member in the field
 *        2) Get the total offset of all the members in the field
 *        3) Get the type size of the last member in the field
 *
 * Return: Success: 0
 *       Failure: negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5LD_construct_info(H5LD_memb_t *memb, hid_t par_tid)
{
    hid_t    tmp_tid = -1;     /* Dataset type id */
    unsigned i;                /* Local index variable */
    herr_t   ret_value = FAIL; /* Return value */

    /* Make a copy of the incoming datatype */
    tmp_tid = H5Tcopy(par_tid);

    /* Validate all the members in a field */
    for (i = 0; memb->names[i] != NULL; i++) {
        hid_t memb_tid; /* Type id for a member in a field */
        int   idx;      /* Index # of a member in a compound datatype */

        /* Get the member index and member type id */
        if ((idx = H5Tget_member_index(tmp_tid, memb->names[i])) < 0)
            goto done;
        if ((memb_tid = H5Tget_member_type(tmp_tid, (unsigned)idx)) < 0)
            goto done;

        /* Sum up the offset of all the members in the field */
        memb->tot_offset += H5Tget_member_offset(tmp_tid, (unsigned)idx);
        if (H5Tclose(tmp_tid) < 0)
            goto done;
        tmp_tid = memb_tid;
    } /* end for */

    /* Get the type size of the last member in the field */
    memb->last_tsize = H5Tget_size(tmp_tid);

    /* Save the type id of the last member in the field */
    memb->last_tid = H5Tcopy(tmp_tid);

    /* Indicate success */
    ret_value = SUCCEED;

done:
    H5E_BEGIN_TRY
    H5Tclose(tmp_tid);
    H5E_END_TRY

    return (ret_value);
} /* H5LD_construct_info() */

/*-------------------------------------------------------------------------
 * Function: H5LD_construct_vector
 *
 * Purpose: Process the comma-separated list of fields in "fields" as follows:
 *     Example:
 *        "fields": "a.b.c,d"
 *        listv[0]->tot_offset = total offset of "a" & "b" & "c"
 *        listv[0]->last_tid = type id of "c"
 *        listv[0]->last_tsize = type size of "c"
 *        listv[0]->names[0] = "a"
 *        listv[0]->names[1] = "b"
 *        listv[0]->names[2] = "c"
 *        listv[0]->names[3] = NULL
 *
 *        listv[1]->tot_offset = offset of "d"
 *        listv[1]->last_tid = type id of "d"
 *        listv[1]->last_tsize = type size of "d"
 *        listv[1]->names[0] = "d"
 *        listv[1]->names[1] = NULL
 *
 * Return: Success: # of comma-separated fields in "fields"
 *       Failure: negative value
 *
 *-------------------------------------------------------------------------
 */
int
H5LD_construct_vector(char *fields, H5LD_memb_t *listv[] /*OUT*/, hid_t par_tid)
{
    int   nfields;               /* The # of comma-separated fields in "fields" */
    bool  end_of_fields = false; /* end of "fields" */
    char *fields_ptr;            /* Pointer to "fields" */
    int   ret_value = FAIL;      /* Return value */

    assert(listv);
    assert(fields);

    fields_ptr = fields;
    nfields    = 0;

    /* Process till end of "fields" */
    while (!end_of_fields) {
        H5LD_memb_t *memb = NULL;       /* Pointer to structure for storing a field's info */
        char        *cur;               /* Pointer to a member in a field */
        size_t       len;               /* Estimated # of members in a field */
        bool         gotcomma  = false; /* A comma encountered */
        bool         gotmember = false; /* Getting member in a field */
        bool         valid     = true;  /* Whether a field being processed is valid or not */
        int          j         = 0;     /* The # of members in a field */

        len = (strlen(fields_ptr) / 2) + 2;

        /* Allocate memory for an H5LD_memb_t for storing a field's info */
        if (NULL == (memb = (H5LD_memb_t *)calloc((size_t)1, sizeof(H5LD_memb_t))))
            goto done;

        /* Allocate memory for an array of pointers to member names */
        if (NULL == (memb->names = (char **)calloc(len, sizeof(char *))))
            goto done;

        memb->names[j] = fields_ptr;
        memb->last_tid = -1;
        cur            = fields_ptr;

        /* Continue processing till: not valid or comma encountered or "fields" ended */
        while (valid && !gotcomma && !end_of_fields) {
            switch (*fields_ptr) {
                case '\0':           /* end of list */
                    if (gotmember) { /* getting something and end of "fields" */
                        *cur++           = '\0';
                        memb->names[++j] = NULL;
                    }    /* end if */
                    else /* getting nothing but end of list */
                        valid = false;
                    end_of_fields = true;
                    break;

                case '\\':        /* escape character */
                    ++fields_ptr; /* skip it */
                    if (*fields_ptr == '\0')
                        valid = false;
                    else {
                        *cur++    = *fields_ptr++;
                        gotmember = true;
                    } /* end else */
                    break;

                case '.': /* nested field separator */
                    *fields_ptr++ = *cur++ = '\0';
                    if (gotmember) {
                        memb->names[++j] = cur;
                        gotmember        = false;
                    } /* end if */
                    else
                        valid = false;
                    break;

                case ',': /* field separator */
                    *fields_ptr++ = *cur++ = '\0';
                    if (gotmember) {
                        memb->names[++j] = NULL;
                        gotmember        = false;
                    } /* end if */
                    else
                        valid = false;
                    gotcomma = true;
                    break;

                default:
                    *cur++    = *fields_ptr++;
                    gotmember = true;
                    break;
            } /* end switch */
        }     /* while (valid && !gotcomma && !end_of_fields) */

        /* If valid, put into listv and continue processing further info */
        if (valid) {
            listv[nfields++] = memb;
            if (H5LD_construct_info(memb, par_tid) < 0)
                goto done;
        } /* end if */
        else {
            if (memb) {
                free(memb->names);
                free(memb);
            }
            goto done;
        } /* end else */
    }     /* while !end_of_fields */

    /* Indicate success */
    ret_value = nfields;

done:
    listv[nfields] = NULL;
    if (ret_value == FAIL)
        H5LD_clean_vector(listv);

    return (ret_value);
} /* H5LD_construct_vector() */

/*-------------------------------------------------------------------------
 * Function: H5LD_get_dset_dims
 *
 * Purpose: To return the current size for each dimension of the
 *        dataset's dataspace
 *
 * Return: Success: 0
 *       Failure: negative value
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5LD_get_dset_dims(hid_t did, hsize_t *cur_dims)
{
    hid_t  sid       = -1;   /* Dataspace ID */
    herr_t ret_value = FAIL; /* Return Value */

    /* Verify parameter */
    if (cur_dims == NULL)
        goto done;

    /* Get the dataset's dataspace */
    if ((sid = H5Dget_space(did)) < 0)
        goto done;

    /* Get the current dimension size */
    if (H5Sget_simple_extent_dims(sid, cur_dims, NULL) < 0)
        goto done;

    /* Indicate success */
    ret_value = SUCCEED;

done:
    H5E_BEGIN_TRY
    {
        H5Sclose(sid);
    }
    H5E_END_TRY

    return (ret_value);
} /* H5LD_get_dset_dims() */

/*-------------------------------------------------------------------------
 * Function: H5LD_get_dset_type_size
 *
 * Purpose: To return the size of the dataset's datatype in bytes
 *    null "fields": return the size of the dataset's datatype
 *    non-null "fields": return the size of the dataset's datatype
 *               with respect to the selection in "fields"
 *
 * Return: Success: size of the dataset's datatype
 *       Failure: 0 (valid datatypes are never zero size)
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5LD_get_dset_type_size(hid_t did, const char *fields)
{
    hid_t         dset_tid   = -1;   /* Dataset's type identifier */
    hid_t         tid        = -1;   /* Native Type identifier */
    H5LD_memb_t **listv      = NULL; /* Vector for storing information in "fields" */
    char         *dup_fields = NULL; /* A copy of "fields" */
    size_t        ret_value  = 0;    /* Return value */

    /* Get the datatype of the dataset */
    if ((dset_tid = H5Dget_type(did)) < 0)
        goto done;
    if ((tid = H5Tget_native_type(dset_tid, H5T_DIR_DEFAULT)) < 0)
        goto done;

    if (fields == NULL) /* If no "fields" is specified */
        ret_value = H5Tget_size(tid);
    else {                     /* "fields" are specified */
        size_t len;            /* Estimate the number of comma-separated fields in "fields" */
        size_t tot = 0;        /* Data type size of all the fields in "fields" */
        int    n = 0, num = 0; /* Local index variables */

        assert(fields && *fields);

        /* Should be a compound datatype if "fields" exists */
        if (H5Tget_class(dset_tid) != H5T_COMPOUND)
            goto done;

        /* Get a copy of "fields" */
        if (NULL == (dup_fields = strdup(fields)))
            goto done;

        /* Allocate memory for a list of H5LD_memb_t pointers to store "fields" info */
        len = (strlen(fields) / 2) + 2;
        if (NULL == (listv = (H5LD_memb_t **)calloc(len, sizeof(H5LD_memb_t *))))
            goto done;

        /* Process and store info for "fields" */
        if ((num = H5LD_construct_vector(dup_fields, listv /*OUT*/, tid)) < 0)
            goto done;

        /* Sum up the size of all the datatypes in "fields" */
        for (n = 0; n < num; n++)
            tot += listv[n]->last_tsize;

        /* Clean up the vector of H5LD_memb_t structures */
        H5LD_clean_vector(listv);

        /* Return the total size */
        ret_value = tot;
    } /* end else */

done:
    H5E_BEGIN_TRY
    H5Tclose(tid);
    H5Tclose(dset_tid);
    H5E_END_TRY

    /* Free the array of H5LD_memb_t pointers */
    if (listv)
        free(listv);

    /* Free memory */
    if (dup_fields)
        free(dup_fields);

    return (ret_value);
} /* H5LD_get_dset_type_size() */

/*-------------------------------------------------------------------------
 * Function: H5LD_get_dset_elmts
 *
 * Purpose: To retrieve selected data from the dataset
 *
 * Return: Success: 0
 *       Failure: negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5LD_get_dset_elmts(hid_t did, const hsize_t *prev_dims, const hsize_t *cur_dims, const char *fields,
                    void *buf)
{
    hid_t         dtid = -1, tid = -1; /* Dataset type id */
    hid_t         sid = -1, mid = -1;  /* Dataspace and memory space id */
    hssize_t      snum_elmts;          /* Number of dataset elements in the selection (signed) */
    hsize_t       num_elmts;           /* Number of dataset elements in the selection */
    hsize_t       start[H5S_MAX_RANK]; /* Starting offset */
    hsize_t       count[H5S_MAX_RANK]; /* ??offset */
    H5LD_memb_t **listv      = NULL;   /* Vector for storing information in "fields" */
    char         *dup_fields = NULL;   /* A copy of "fields" */
    char         *sav_buf    = NULL;   /* Saved pointer temporary buffer */
    unsigned      ctr;                 /* Counter for # of curr_dims > prev_dims */
    int           ndims;               /* Number of dimensions for the dataset */
    int           i;                   /* Local index variable */
    herr_t        ret_value = FAIL;    /* Return value */

    /* Verify parameters */
    if (prev_dims == NULL || cur_dims == NULL || buf == NULL)
        goto done;

    /* Get dataset's dataspace */
    if ((sid = H5Dget_space(did)) < 0)
        goto done;

    /* Get the number of dimensions */
    if ((ndims = H5Sget_simple_extent_ndims(sid)) < 0)
        goto done;

    /* Verify that cur_dims must have one dimension whose size is greater than prev_dims */
    memset(start, 0, sizeof start);
    memset(count, 0, sizeof count);
    ctr = 0;
    for (i = 0; i < ndims; i++)
        if (cur_dims[i] > prev_dims[i]) {
            ++ctr;
            count[i] = cur_dims[i] - prev_dims[i];
            start[i] = prev_dims[i];
        }      /* end if */
        else { /* < or = */
            start[i] = 0;
            count[i] = MIN(prev_dims[i], cur_dims[i]);
        } /* end else */
    if (!ctr)
        goto done;

    if (ctr == 1) { /* changes for only one dimension */
        /* Make the selection in the dataset based on "cur_dims" and "prev_dims" */
        if (H5Sselect_hyperslab(sid, H5S_SELECT_SET, start, NULL, count, NULL) < 0)
            goto done;
    }      /* end if */
    else { /* changes for more than one dimensions */
        memset(start, 0, sizeof start);

        /* Make the selection in the dataset based on "cur_dims" and "prev_dims" */
        if (H5Sselect_hyperslab(sid, H5S_SELECT_SET, start, NULL, cur_dims, NULL) < 0)
            goto done;
        if (H5Sselect_hyperslab(sid, H5S_SELECT_NOTB, start, NULL, prev_dims, NULL) < 0)
            goto done;
    } /* end else */

    /* Get the number of elements in the selection */
    if (0 == (snum_elmts = H5Sget_select_npoints(sid)))
        goto done;
    num_elmts = (hsize_t)snum_elmts;

    /* Create the memory space for the selection */
    if ((mid = H5Screate_simple(1, &num_elmts, NULL)) < 0)
        goto done;

    /* Get the native datatype size */
    if ((dtid = H5Dget_type(did)) < 0)
        goto done;
    if ((tid = H5Tget_native_type(dtid, H5T_DIR_DEFAULT)) < 0)
        goto done;

    if (fields == NULL) { /* nothing in "fields" */
        /* Read and store all the elements in "buf" */
        if (H5Dread(did, tid, mid, sid, H5P_DEFAULT, buf) < 0)
            goto done;
    }                                                /* end if */
    else {                                           /* "fields" is specified */
        unsigned char *buf_p = (unsigned char *)buf; /* Pointer to the destination buffer */
        char          *tmp_buf;                      /* Temporary buffer for data read */
        size_t         tot_tsize;                    /* Total datatype size */
        size_t         len; /* Estimate the number of comma-separated fields in "fields" */

        /* should be a compound datatype if "fields" exists */
        if (H5Tget_class(tid) != H5T_COMPOUND)
            goto done;

        /* Get the total size of the dataset's datatypes */
        if (0 == (tot_tsize = H5LD_get_dset_type_size(did, NULL)))
            goto done;

        /* Allocate memory for reading in the elements in the dataset selection */
        if (NULL == (sav_buf = tmp_buf = (char *)calloc((size_t)num_elmts, tot_tsize)))
            goto done;

        /* Read the dataset elements in the selection */
        if (H5Dread(did, tid, mid, sid, H5P_DEFAULT, tmp_buf) < 0)
            goto done;

        /* Make a copy of "fields" */
        if (NULL == (dup_fields = strdup(fields)))
            goto done;

        /* Allocate memory for the vector of H5LD_memb_t pointers */
        len = (strlen(fields) / 2) + 2;
        if (NULL == (listv = (H5LD_memb_t **)calloc(len, sizeof(H5LD_memb_t *))))
            goto done;

        /* Process and store information for "fields" */
        if (H5LD_construct_vector(dup_fields, listv, tid) < 0)
            goto done;

        /* Copy data for each dataset element in the selection */
        for (i = 0; i < (int)num_elmts; i++) {
            int j; /* Local index variable */

            /* Copy data for "fields" to the input buffer */
            for (j = 0; listv[j] != NULL; j++) {
                memcpy(buf_p, tmp_buf + listv[j]->tot_offset, listv[j]->last_tsize);
                buf_p += listv[j]->last_tsize;
            } /* end for */
            tmp_buf += tot_tsize;
        } /* end for */

        /* Clean up the vector of H5LD_memb_t structures */
        H5LD_clean_vector(listv);
    } /* end else */

    /* Indicate success */
    ret_value = SUCCEED;

done:
    H5E_BEGIN_TRY
    H5Tclose(dtid);
    H5Tclose(tid);
    H5Sclose(sid);
    H5Sclose(mid);
    H5E_END_TRY

    /* Free the array of H5LD_memb_t pointers */
    if (listv)
        free(listv);

    /* Free memory */
    if (dup_fields)
        free(dup_fields);
    if (sav_buf)
        free(sav_buf);

    return (ret_value);
} /* H5LD_get_dset_elmts() */

/*-------------------------------------------------------------------------
 *
 * Public functions
 *
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 * Function: H5LDget_dset_dims
 *
 * Purpose: To retrieve the current dimension sizes for a dataset
 *
 * Return: Success: 0
 *       Failure: negative value
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LDget_dset_dims(hid_t did, hsize_t *cur_dims)
{
    return (H5LD_get_dset_dims(did, cur_dims));
} /* H5LDget_dset_dims() */

/*-------------------------------------------------------------------------
 * Function: H5LDget_dset_type_size
 *
 * Purpose:  To return the size in bytes of the datatype for the dataset
 *
 * Return: Success: size in bytes of the dataset's datatype
 *       Failure: 0 (valid datatypes are never zero size)
 *
 *-------------------------------------------------------------------------
 */
size_t
H5LDget_dset_type_size(hid_t did, const char *fields)
{
    return (H5LD_get_dset_type_size(did, fields));
} /* H5LDget_dset_type_size() */

/*-------------------------------------------------------------------------
 * Function: H5LDget_dset_elmts
 *
 * Purpose: To retrieve selected data from the dataset
 *
 * Return: Success: 0
 *       Failure: negative value
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5LDget_dset_elmts(hid_t did, const hsize_t *prev_dims, const hsize_t *cur_dims, const char *fields,
                   void *buf)
{
    return (H5LD_get_dset_elmts(did, prev_dims, cur_dims, fields, buf));
} /* H5LDget_dset_elmts() */
