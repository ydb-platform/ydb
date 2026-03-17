/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * Copyright (c) 2018      DataDirect Networks. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/info/info.h"
#include "ompi/file/file.h"
#include "ompi/mca/io/base/base.h"
#include "ompi/mca/fs/fs.h"
#include "ompi/mca/fs/base/base.h"
#include "ompi/mca/fcoll/fcoll.h"
#include "ompi/mca/fcoll/base/base.h"
#include "ompi/mca/fbtl/fbtl.h"
#include "ompi/mca/fbtl/base/base.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"

#include <unistd.h>
#include <math.h>
#include "common_ompio.h"
#include "ompi/mca/topo/topo.h"

static mca_common_ompio_generate_current_file_view_fn_t generate_current_file_view_fn;
static mca_common_ompio_get_mca_parameter_value_fn_t get_mca_parameter_value_fn;

int mca_common_ompio_file_open (ompi_communicator_t *comm,
                                const char *filename,
                                int amode,
                                opal_info_t *info,
                                ompio_file_t *ompio_fh, bool use_sharedfp)
{
    int ret = OMPI_SUCCESS;
    int remote_arch;


    ompio_fh->f_iov_type = MPI_DATATYPE_NULL;
    ompio_fh->f_comm     = MPI_COMM_NULL;

    if ( ((amode&MPI_MODE_RDONLY)?1:0) + ((amode&MPI_MODE_RDWR)?1:0) +
	 ((amode&MPI_MODE_WRONLY)?1:0) != 1 ) {
	return MPI_ERR_AMODE;
    }

    if ((amode & MPI_MODE_RDONLY) &&
        ((amode & MPI_MODE_CREATE) || (amode & MPI_MODE_EXCL))) {
	return  MPI_ERR_AMODE;
    }

    if ((amode & MPI_MODE_RDWR) && (amode & MPI_MODE_SEQUENTIAL)) {
	return MPI_ERR_AMODE;
    }

    ompio_fh->f_rank     = ompi_comm_rank (comm);
    ompio_fh->f_size     = ompi_comm_size (comm);
    remote_arch = opal_local_arch;
    ompio_fh->f_convertor = opal_convertor_create (remote_arch, 0);

    if ( true == use_sharedfp ) {
	ret = ompi_comm_dup (comm, &ompio_fh->f_comm);
	if ( OMPI_SUCCESS != ret )  {
	    goto fn_fail;
	}
    }
    else {
	/* No need to duplicate the communicator if the file_open is called
	   from the sharedfp component, since the comm used as an input
	   is already a dup of the user level comm. */
	ompio_fh->f_comm = comm;
    }

    ompio_fh->f_fstype = NONE;
    ompio_fh->f_amode  = amode;
    ompio_fh->f_info   = info;

    /* set some function pointers required for fcoll, fbtls and sharedfp modules*/
    ompio_fh->f_generate_current_file_view=generate_current_file_view_fn;
    ompio_fh->f_get_mca_parameter_value=get_mca_parameter_value_fn;

    ompio_fh->f_filename = filename;
    mca_common_ompio_set_file_defaults (ompio_fh);

    ompio_fh->f_split_coll_req    = NULL;
    ompio_fh->f_split_coll_in_use = false;

    /*Initialize the print_queues queues here!*/
    mca_common_ompio_initialize_print_queue(&ompio_fh->f_coll_write_time);
    mca_common_ompio_initialize_print_queue(&ompio_fh->f_coll_read_time);

    /* This fix is needed for data seiving to work with
       two-phase collective I/O */
    if ( OMPIO_MCA_GET(ompio_fh, overwrite_amode) && !(amode & MPI_MODE_SEQUENTIAL) ) {

        if ((amode & MPI_MODE_WRONLY)){
            amode -= MPI_MODE_WRONLY;
            amode += MPI_MODE_RDWR;
        }
    }
     /*--------------------------------------------------*/


    if (OMPI_SUCCESS != (ret = mca_fs_base_file_select (ompio_fh,
                                                        NULL))) {
        opal_output(1, "mca_fs_base_file_select() failed\n");
        goto fn_fail;
    }
    if (OMPI_SUCCESS != (ret = mca_fbtl_base_file_select (ompio_fh,
                                                          NULL))) {
        opal_output(1, "mca_fbtl_base_file_select() failed\n");
        goto fn_fail;
    }


    ompio_fh->f_sharedfp_component = NULL; /*component*/
    ompio_fh->f_sharedfp           = NULL; /*module*/
    ompio_fh->f_sharedfp_data      = NULL; /*data*/

    if ( true == use_sharedfp ) {
	if (OMPI_SUCCESS != (ret = mca_sharedfp_base_file_select (ompio_fh, NULL))) {
	    opal_output ( ompi_io_base_framework.framework_output,
			  "mca_sharedfp_base_file_select() failed\n");
	    ompio_fh->f_sharedfp           = NULL; /*module*/
	    /* Its ok to not have a shared file pointer module as long as the shared file
	    ** pointer operations are not used. However, the first call to any file_read/write_shared
	    ** function will return an error code.
	    */
	}
    }
    else {
	ompio_fh->f_flags |= OMPIO_SHAREDFP_IS_SET;
    }

    ret = ompio_fh->f_fs->fs_file_open (comm,
					filename,
					amode,
					info,
					ompio_fh);

    if ( OMPI_SUCCESS != ret ) {
#ifdef OMPIO_DEBUG
        opal_output(1, "fs_file failed, error code %d\n", ret);
#endif
        goto fn_fail;
    }

    if ( true == use_sharedfp ) {
	/* open the file once more for the shared file pointer if required.           
        ** Can be disabled by the user if no shared file pointer operations
        ** are used by his application.	
        */
	if ( NULL != ompio_fh->f_sharedfp ) {
	    ret = ompio_fh->f_sharedfp->sharedfp_file_open(comm,
							   filename,
							   amode,
							   info,
							   ompio_fh);

	    if ( OMPI_SUCCESS != ret ) {
		goto fn_fail;
	    }
	}
    }

    /* Set default file view */
    mca_common_ompio_set_view(ompio_fh,
                              0,
                              &ompi_mpi_byte.dt,
                              &ompi_mpi_byte.dt,
                              "native",
                              info);

    

    /* If file has been opened in the append mode, move the internal
       file pointer of OMPIO to the very end of the file. */
    if ( ompio_fh->f_amode & MPI_MODE_APPEND ) {
        OMPI_MPI_OFFSET_TYPE current_size;
        mca_sharedfp_base_module_t * shared_fp_base_module;

        ompio_fh->f_fs->fs_file_get_size( ompio_fh,
                                          &current_size);
        mca_common_ompio_set_explicit_offset (ompio_fh, current_size);
        if ( true == use_sharedfp ) {
            if ( NULL != ompio_fh->f_sharedfp ) {
                shared_fp_base_module = ompio_fh->f_sharedfp;
                ret = shared_fp_base_module->sharedfp_seek(ompio_fh,current_size, MPI_SEEK_SET);
                if ( MPI_SUCCESS != ret  ) {
                    opal_output(1, "mca_common_ompio_file_open: Could not adjust position of "
                                "shared file pointer with MPI_MODE_APPEND\n");
                    ret = MPI_ERR_OTHER;
                    goto fn_fail;
                }
            }
        }

    }



    return OMPI_SUCCESS;

    fn_fail:
        /* no need to free resources here, since the destructor
	 * is calling mca_io_ompio_file_close, which actually gets
	 *rid of all allocated memory items */

    return ret;
}

int mca_common_ompio_file_close (ompio_file_t *ompio_fh)
{
    int ret = OMPI_SUCCESS;
    int delete_flag = 0;
    char name[256];

    ret = ompio_fh->f_comm->c_coll->coll_barrier ( ompio_fh->f_comm, ompio_fh->f_comm->c_coll->coll_barrier_module);
    if ( OMPI_SUCCESS != ret ) {
        /* Not sure what to do */
        opal_output (1,"mca_common_ompio_file_close: error in Barrier \n");
        return ret;
    }


    if(OMPIO_MCA_GET(ompio_fh, coll_timing_info)){
        strcpy (name, "WRITE");
        if (!mca_common_ompio_empty_print_queue(ompio_fh->f_coll_write_time)){
            ret = mca_common_ompio_print_time_info(ompio_fh->f_coll_write_time,
                                                   name,
                                                   ompio_fh);
            if (OMPI_SUCCESS != ret){
                printf("Error in print_time_info ");
            }

        }
        strcpy (name, "READ");
        if (!mca_common_ompio_empty_print_queue(ompio_fh->f_coll_read_time)){
            ret = mca_common_ompio_print_time_info(ompio_fh->f_coll_read_time,
                                                   name,
                                                   ompio_fh);
            if (OMPI_SUCCESS != ret){
                printf("Error in print_time_info ");
            }
        }
    }
    if ( ompio_fh->f_amode & MPI_MODE_DELETE_ON_CLOSE ) {
        delete_flag = 1;
    }

    /*close the sharedfp file*/
    if( NULL != ompio_fh->f_sharedfp ){
        ret = ompio_fh->f_sharedfp->sharedfp_file_close(ompio_fh);
    }
    if ( NULL != ompio_fh->f_fs ) {
	/* The pointer might not be set if file_close() is
	** called from the file destructor in case of an error
	** during file_open()
	*/
	ret = ompio_fh->f_fs->fs_file_close (ompio_fh);
    }
    if ( delete_flag ) {
        ret = mca_common_ompio_file_delete ( ompio_fh->f_filename, &(MPI_INFO_NULL->super) );
    }

    if ( NULL != ompio_fh->f_fs ) {
	mca_fs_base_file_unselect (ompio_fh);
    }
    if ( NULL != ompio_fh->f_fbtl ) {
	mca_fbtl_base_file_unselect (ompio_fh);
    }

    if ( NULL != ompio_fh->f_fcoll ) {
	mca_fcoll_base_file_unselect (ompio_fh);
    }
    if ( NULL != ompio_fh->f_sharedfp)  {
	mca_sharedfp_base_file_unselect (ompio_fh);
    }

    if (NULL != ompio_fh->f_io_array) {
        free (ompio_fh->f_io_array);
        ompio_fh->f_io_array = NULL;
    }

    if (NULL != ompio_fh->f_init_aggr_list) {
        free (ompio_fh->f_init_aggr_list);
        ompio_fh->f_init_aggr_list = NULL;
    }
    if (NULL != ompio_fh->f_aggr_list) {
        free (ompio_fh->f_aggr_list);
        ompio_fh->f_aggr_list = NULL;
    }
    if (NULL != ompio_fh->f_init_procs_in_group) {
        free (ompio_fh->f_init_procs_in_group);
        ompio_fh->f_init_procs_in_group = NULL;
    }
    if (NULL != ompio_fh->f_procs_in_group) {
        free (ompio_fh->f_procs_in_group);
        ompio_fh->f_procs_in_group = NULL;
    }

    if (NULL != ompio_fh->f_decoded_iov) {
        free (ompio_fh->f_decoded_iov);
        ompio_fh->f_decoded_iov = NULL;
    }

    if (NULL != ompio_fh->f_convertor) {
        free (ompio_fh->f_convertor);
        ompio_fh->f_convertor = NULL;
    }

    if (NULL != ompio_fh->f_datarep) {
        free (ompio_fh->f_datarep);
        ompio_fh->f_datarep = NULL;
    }


    if ( NULL != ompio_fh->f_coll_write_time ) {
        free ( ompio_fh->f_coll_write_time );
        ompio_fh->f_coll_write_time = NULL;
    }

    if ( NULL != ompio_fh->f_coll_read_time ) {
        free ( ompio_fh->f_coll_read_time );
        ompio_fh->f_coll_read_time = NULL;
    }

    if (MPI_DATATYPE_NULL != ompio_fh->f_iov_type) {
        ompi_datatype_destroy (&ompio_fh->f_iov_type);
        ompio_fh->f_iov_type=MPI_DATATYPE_NULL;
    }

    if ( MPI_DATATYPE_NULL != ompio_fh->f_etype ) {
	ompi_datatype_destroy (&ompio_fh->f_etype);
    }
    if ( MPI_DATATYPE_NULL != ompio_fh->f_filetype ){
	ompi_datatype_destroy (&ompio_fh->f_filetype);
    }

    if ( MPI_DATATYPE_NULL != ompio_fh->f_orig_filetype ){
	ompi_datatype_destroy (&ompio_fh->f_orig_filetype);
    }


    if (MPI_COMM_NULL != ompio_fh->f_comm && !(ompio_fh->f_flags & OMPIO_SHAREDFP_IS_SET) )  {
        ompi_comm_free (&ompio_fh->f_comm);
    }

    return ret;
}

int mca_common_ompio_file_get_size (ompio_file_t *ompio_fh,
                                  OMPI_MPI_OFFSET_TYPE *size)
{
    int ret = OMPI_SUCCESS;

    ret = ompio_fh->f_fs->fs_file_get_size (ompio_fh, size);

    return ret;
}


int mca_common_ompio_file_get_position (ompio_file_t *fh,
                                      OMPI_MPI_OFFSET_TYPE *offset)
{
    OMPI_MPI_OFFSET_TYPE off;

    /* No. of copies of the entire file view */
    off = (fh->f_offset - fh->f_disp)/fh->f_view_extent;

    /* No. of elements per view */
    off *= (fh->f_view_size / fh->f_etype_size);

    /* No of elements used in the current copy of the view */
    off += fh->f_total_bytes / fh->f_etype_size;

    *offset = off;
    return OMPI_SUCCESS;
}

int mca_common_ompio_set_file_defaults (ompio_file_t *fh)
{

   if (NULL != fh) {
       char char_stripe[MPI_MAX_INFO_VAL];
       ompi_datatype_t *types[2];
       int blocklen[2] = {1, 1};
       ptrdiff_t d[2], base;
       int i, flag;
       
       fh->f_io_array = NULL;
       fh->f_perm = OMPIO_PERM_NULL;
       fh->f_flags = 0;
       
       fh->f_bytes_per_agg = OMPIO_MCA_GET(fh, bytes_per_agg);
       opal_info_get (fh->f_info, "cb_buffer_size", MPI_MAX_INFO_VAL, char_stripe, &flag);
       if ( flag ) {
           /* Info object trumps mca parameter value */
           sscanf ( char_stripe, "%d", &fh->f_bytes_per_agg  );
           OMPIO_MCA_PRINT_INFO(fh, "cb_buffer_size", char_stripe, "");
       }

       fh->f_atomicity = 0;
       fh->f_fs_block_size = 4096;
       
       fh->f_offset = 0;
       fh->f_disp = 0;
       fh->f_position_in_file_view = 0;
       fh->f_index_in_file_view = 0;
       fh->f_total_bytes = 0;
       
       fh->f_init_procs_per_group = -1;
       fh->f_init_procs_in_group = NULL;
       
       fh->f_procs_per_group = -1;
       fh->f_procs_in_group = NULL;
       
       fh->f_init_num_aggrs = -1;
       fh->f_init_aggr_list = NULL;
       
       fh->f_num_aggrs = -1;
       fh->f_aggr_list = NULL;
       
       /* Default file View */
       fh->f_iov_type = MPI_DATATYPE_NULL;
       fh->f_stripe_size = 0;
       /*Decoded iovec of the file-view*/
       fh->f_decoded_iov = NULL;
       fh->f_etype = MPI_DATATYPE_NULL;
       fh->f_filetype = MPI_DATATYPE_NULL;
       fh->f_orig_filetype = MPI_DATATYPE_NULL;
       fh->f_datarep = NULL;
       
       /*Create a derived datatype for the created iovec */
       types[0] = &ompi_mpi_long.dt;
       types[1] = &ompi_mpi_long.dt;
       
       d[0] = (ptrdiff_t) fh->f_decoded_iov;
       d[1] = (ptrdiff_t) &fh->f_decoded_iov[0].iov_len;
       
       base = d[0];
       for (i=0 ; i<2 ; i++) {
           d[i] -= base;
       }
       
       ompi_datatype_create_struct (2,
                                    blocklen,
                                    d,
                                    types,
                                    &fh->f_iov_type);
       ompi_datatype_commit (&fh->f_iov_type);
       
       return OMPI_SUCCESS;
   }
   else {
       return OMPI_ERROR;
   }
}


int mca_common_ompio_file_delete (const char *filename,
                                  struct opal_info_t *info)
{
    int ret = OMPI_SUCCESS;
    ompio_file_t *fh = NULL;

    /* No locking required for file_delete according to my understanding.
       One thread will succeed, the other ones silently ignore the 
       error that the file is already deleted.
    */

    /* Create an incomplete file handle, it will basically only
       contain the filename. It is needed to select the correct
       component in the fs framework and call the file_remove
       function corresponding to the file type. 
    */
    ret = mca_common_ompio_create_incomplete_file_handle(filename, &fh);
    if (OMPI_SUCCESS != ret) {
        return ret;
    }

    ret = mca_fs_base_file_select (fh, NULL);
    if (OMPI_SUCCESS != ret) {
        opal_output(1, "error in mca_common_ompio_file_delete: "
                       "mca_fs_base_file_select() failed\n");
        free(fh);
        return ret;
    }

    ret = fh->f_fs->fs_file_delete ( (char *)filename, NULL);
    free(fh);

    if (OMPI_SUCCESS != ret) {
        return ret;
    }
    return OMPI_SUCCESS;
}

int mca_common_ompio_create_incomplete_file_handle (const char *filename,
                                                    ompio_file_t **fh)
{
    ompio_file_t *file;

    if (NULL == filename) {
        opal_output(1, "error in mca_common_ompio_create_incomplete_file_handle"
                       ", filename is NULL.\n");
        return OMPI_ERROR;
    }

    file = calloc(1, sizeof(ompio_file_t));
    if (NULL == file) {
        opal_output(1, "Out of memory.\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }


    /* Do not use communicator */
    file->f_comm = MPI_COMM_NULL;
    file->f_rank = OMPIO_ROOT;

    /* TODO:
        - Maybe copy the info for the info layer
        - Maybe do the same as a file open: first create an ompi_file_t,
            then allocate f_io_selected_data,
            then use the ompio_file_t stored in this data structure
    */

    /* We don't need to create a copy of the filename,
       this file handle is only temporary. */
    file->f_filename = filename;

    *fh = file;
    return OMPI_SUCCESS;
}

int mca_common_ompio_decode_datatype (struct ompio_file_t *fh,
                                      ompi_datatype_t *datatype,
                                      int count,
                                      const void *buf,
                                      size_t *max_data,
                                      struct iovec **iov,
                                      uint32_t *iovec_count)
{



    opal_convertor_t convertor;
    size_t remaining_length = 0;
    uint32_t i;
    uint32_t temp_count;
    struct iovec *temp_iov=NULL;
    size_t temp_data;


    opal_convertor_clone (fh->f_convertor, &convertor, 0);

    if (OMPI_SUCCESS != opal_convertor_prepare_for_send (&convertor,
                                                         &(datatype->super),
                                                         count,
                                                         buf)) {
        opal_output (1, "Cannot attach the datatype to a convertor\n");
        return OMPI_ERROR;
    }

    if ( 0 == datatype->super.size ) {
	*max_data = 0;
	*iovec_count = 0;
	*iov = NULL;
	return OMPI_SUCCESS;
    }

    remaining_length = count * datatype->super.size;

    temp_count = OMPIO_IOVEC_INITIAL_SIZE;
    temp_iov = (struct iovec*)malloc(temp_count * sizeof(struct iovec));
    if (NULL == temp_iov) {
        opal_output (1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    while (0 == opal_convertor_raw(&convertor,
				   temp_iov,
                                   &temp_count,
                                   &temp_data)) {
#if 0
        printf ("%d: New raw extraction (iovec_count = %d, max_data = %lu)\n",
                fh->f_rank,temp_count, (unsigned long)temp_data);
        for (i = 0; i < temp_count; i++) {
            printf ("%d: \t{%p, %lu}\n",fh->f_rank,
		    temp_iov[i].iov_base,
		    (unsigned long)temp_iov[i].iov_len);
        }
#endif

        *iovec_count = *iovec_count + temp_count;
        *max_data = *max_data + temp_data;
        *iov = (struct iovec *) realloc (*iov, *iovec_count * sizeof(struct iovec));
        if (NULL == *iov) {
            opal_output(1, "OUT OF MEMORY\n");
            free(temp_iov);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        for (i=0 ; i<temp_count ; i++) {
            (*iov)[i+(*iovec_count-temp_count)].iov_base = temp_iov[i].iov_base;
            (*iov)[i+(*iovec_count-temp_count)].iov_len = temp_iov[i].iov_len;
        }

        remaining_length -= temp_data;
        temp_count = OMPIO_IOVEC_INITIAL_SIZE;
    }
#if 0
    printf ("%d: LAST raw extraction (iovec_count = %d, max_data = %d)\n",
            fh->f_rank,temp_count, temp_data);
    for (i = 0; i < temp_count; i++) {
        printf ("%d: \t offset[%d]: %ld; length[%d]: %ld\n", fh->f_rank,i,temp_iov[i].iov_base, i,temp_iov[i].iov_len);
    }
#endif
    *iovec_count = *iovec_count + temp_count;
    *max_data = *max_data + temp_data;
    if ( temp_count > 0 ) {
	*iov = (struct iovec *) realloc (*iov, *iovec_count * sizeof(struct iovec));
	if (NULL == *iov) {
	    opal_output(1, "OUT OF MEMORY\n");
            free(temp_iov);
	    return OMPI_ERR_OUT_OF_RESOURCE;
	}
    }
    for (i=0 ; i<temp_count ; i++) {
        (*iov)[i+(*iovec_count-temp_count)].iov_base = temp_iov[i].iov_base;
        (*iov)[i+(*iovec_count-temp_count)].iov_len = temp_iov[i].iov_len;
    }

    remaining_length -= temp_data;

#if 0
    if (0 == fh->f_rank) {
        printf ("%d Entries: \n",*iovec_count);
        for (i=0 ; i<*iovec_count ; i++) {
            printf ("\t{%p, %d}\n",
                    (*iov)[i].iov_base,
                    (*iov)[i].iov_len);
        }
    }
#endif
    if (remaining_length != 0) {
        printf( "Not all raw description was been extracted (%lu bytes missing)\n",
                (unsigned long) remaining_length );
    }

    free (temp_iov);

    return OMPI_SUCCESS;
}

int mca_common_ompio_set_callbacks(mca_common_ompio_generate_current_file_view_fn_t generate_current_file_view,
                                   mca_common_ompio_get_mca_parameter_value_fn_t get_mca_parameter_value)
{
    generate_current_file_view_fn = generate_current_file_view;
    get_mca_parameter_value_fn = get_mca_parameter_value;
    return OMPI_SUCCESS;
}
