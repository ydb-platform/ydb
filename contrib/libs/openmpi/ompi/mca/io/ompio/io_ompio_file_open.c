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
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
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
#include "io_ompio.h"
#include "ompi/mca/common/ompio/common_ompio_request.h"
#include "ompi/mca/topo/topo.h"

int mca_io_ompio_file_open (ompi_communicator_t *comm,
                            const char *filename,
                            int amode,
                            opal_info_t *info,
                            ompi_file_t *fh)
{
    int ret = OMPI_SUCCESS;
    mca_common_ompio_data_t *data=NULL;
    bool use_sharedfp = true;


    /* No locking required for file_open according to my understanding
       There is virtually no way on how to reach this point from multiple
       threads simultaniously 
    */
    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;
    if ( NULL == data ) {
        return  OMPI_ERR_OUT_OF_RESOURCE;
    }


    /*save pointer back to the file_t structure */
    data->ompio_fh.f_fh = fh;
    /* No lock required for file_open even in multi-threaded scenarios,
       since only one collective operation per communicator
       is allowed anyway */
    ret = mca_common_ompio_file_open(comm,filename,amode,info,&data->ompio_fh,use_sharedfp);

    if ( OMPI_SUCCESS == ret ) {
        fh->f_flags |= OMPIO_FILE_IS_OPEN;
    }




    return ret;
}

int mca_io_ompio_file_close (ompi_file_t *fh)
{
    int ret = OMPI_SUCCESS;
    mca_common_ompio_data_t *data;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;
    if ( NULL == data ) {
	/* structure has already been freed, this is an erroneous call to file_close */
	return ret;
    }
    /* No locking required for file_close according to my understanding.
       Multiple threads closing the same file handle at the same time
       is a clear user error.
    */
    ret = mca_common_ompio_file_close(&data->ompio_fh);

    if ( NULL != data ) {
      free ( data );
    }

    return ret;
}

int mca_io_ompio_file_preallocate (ompi_file_t *fh,
                                   OMPI_MPI_OFFSET_TYPE diskspace)
{
    int ret = OMPI_SUCCESS, cycles, i;
    OMPI_MPI_OFFSET_TYPE tmp, current_size, size, written, len;
    mca_common_ompio_data_t *data;
    char *buf = NULL;
    ompi_status_public_t *status = NULL;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;

    OPAL_THREAD_LOCK(&fh->f_lock);
    tmp = diskspace;

    ret = data->ompio_fh.f_comm->c_coll->coll_bcast (&tmp,
                                                    1,
                                                    OMPI_OFFSET_DATATYPE,
                                                    OMPIO_ROOT,
                                                    data->ompio_fh.f_comm,
                                                    data->ompio_fh.f_comm->c_coll->coll_bcast_module);
    if ( OMPI_SUCCESS != ret ) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return OMPI_ERROR;
    }

    if (tmp != diskspace) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return OMPI_ERROR;
    }
    ret = data->ompio_fh.f_fs->fs_file_get_size (&data->ompio_fh,
                                                 &current_size);
    if ( OMPI_SUCCESS != ret ) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return OMPI_ERROR;
    }
    
    if ( current_size > diskspace ) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return OMPI_SUCCESS;
    }


    /* ROMIO explanation
       On file systems with no preallocation function, we have to
       explicitly write to allocate space. Since there could be holes in the file,
       we need to read up to the current file size, write it back,
       and then write beyond that depending on how much
       preallocation is needed.
    */
    if (OMPIO_ROOT == data->ompio_fh.f_rank) {
        OMPI_MPI_OFFSET_TYPE prev_offset;
        mca_common_ompio_file_get_position (&data->ompio_fh, &prev_offset );

        size = diskspace;
        if (size > current_size) {
            size = current_size;
        }

        cycles = (size + OMPIO_PREALLOC_MAX_BUF_SIZE - 1)/
            OMPIO_PREALLOC_MAX_BUF_SIZE;
        buf = (char *) malloc (OMPIO_PREALLOC_MAX_BUF_SIZE);
        if (NULL == buf) {
            opal_output(1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
        written = 0;

        for (i=0; i<cycles; i++) {
            len = OMPIO_PREALLOC_MAX_BUF_SIZE;
            if (len > size-written) {
                len = size - written;
            }
            ret = mca_common_ompio_file_read (&data->ompio_fh, buf, len, MPI_BYTE, status);
            if (ret != OMPI_SUCCESS) {
                goto exit;
            }
            ret = mca_common_ompio_file_write (&data->ompio_fh, buf, len, MPI_BYTE, status);
            if (ret != OMPI_SUCCESS) {
                goto exit;
            }
            written += len;
        }

        if (diskspace > current_size) {
            memset(buf, 0, OMPIO_PREALLOC_MAX_BUF_SIZE);
            size = diskspace - current_size;
            cycles = (size + OMPIO_PREALLOC_MAX_BUF_SIZE - 1) /
                OMPIO_PREALLOC_MAX_BUF_SIZE;
            for (i=0; i<cycles; i++) {
                len = OMPIO_PREALLOC_MAX_BUF_SIZE;
                if (len > diskspace-written) {
                    len = diskspace - written;
                }
                ret = mca_common_ompio_file_write (&data->ompio_fh, buf, len, MPI_BYTE, status);
                if (ret != OMPI_SUCCESS) {
                    goto exit;
                }
                written += len;
            }
        }

        // This operation should not affect file pointer position.
        mca_common_ompio_set_explicit_offset ( &data->ompio_fh, prev_offset);
    }

exit:     
    free ( buf );
    fh->f_comm->c_coll->coll_bcast ( &ret, 1, MPI_INT, OMPIO_ROOT, fh->f_comm,
                                   fh->f_comm->c_coll->coll_bcast_module);
    
    if ( diskspace > current_size ) {
        data->ompio_fh.f_fs->fs_file_set_size (&data->ompio_fh, diskspace);
    }
    OPAL_THREAD_UNLOCK(&fh->f_lock);

    return ret;
}

int mca_io_ompio_file_set_size (ompi_file_t *fh,
                                OMPI_MPI_OFFSET_TYPE size)
{
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE tmp;
    mca_common_ompio_data_t *data;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;

    tmp = size;
    OPAL_THREAD_LOCK(&fh->f_lock);
    ret = data->ompio_fh.f_comm->c_coll->coll_bcast (&tmp,
                                                    1,
                                                    OMPI_OFFSET_DATATYPE,
                                                    OMPIO_ROOT,
                                                    data->ompio_fh.f_comm,
                                                    data->ompio_fh.f_comm->c_coll->coll_bcast_module);
    if ( OMPI_SUCCESS != ret ) {
        opal_output(1, ",mca_io_ompio_file_set_size: error in bcast\n");
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return ret;
    }
    

    if (tmp != size) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return OMPI_ERROR;
    }

    ret = data->ompio_fh.f_fs->fs_file_set_size (&data->ompio_fh, size);
    if ( OMPI_SUCCESS != ret ) {
        opal_output(1, ",mca_io_ompio_file_set_size: error in fs->set_size\n");
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return ret;
    }
    
    ret = data->ompio_fh.f_comm->c_coll->coll_barrier (data->ompio_fh.f_comm,
                                                      data->ompio_fh.f_comm->c_coll->coll_barrier_module);
    if ( OMPI_SUCCESS != ret ) {
        opal_output(1, ",mca_io_ompio_file_set_size: error in barrier\n");
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return ret;
    }
    OPAL_THREAD_UNLOCK(&fh->f_lock);

    return ret;
}

int mca_io_ompio_file_get_size (ompi_file_t *fh,
                            OMPI_MPI_OFFSET_TYPE *size)
{
    int ret = OMPI_SUCCESS;
    mca_common_ompio_data_t *data;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;
    OPAL_THREAD_LOCK(&fh->f_lock);
    ret = mca_common_ompio_file_get_size(&data->ompio_fh,size);
    OPAL_THREAD_UNLOCK(&fh->f_lock);

    return ret;
}


int mca_io_ompio_file_get_amode (ompi_file_t *fh,
                                 int *amode)
{
    mca_common_ompio_data_t *data;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;
    /* No lock necessary in this case, amode is set in file_open, and 
       not modified later on*/
    *amode = data->ompio_fh.f_amode;

    return OMPI_SUCCESS;
}


int mca_io_ompio_file_get_type_extent (ompi_file_t *fh,
                                       struct ompi_datatype_t *datatype,
                                       MPI_Aint *extent)
{
    opal_datatype_type_extent (&datatype->super, extent);
    return OMPI_SUCCESS;
}


int mca_io_ompio_file_set_atomicity (ompi_file_t *fh,
                                     int flag)
{
    int tmp;
    mca_common_ompio_data_t *data;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;

    OPAL_THREAD_LOCK(&fh->f_lock);
    if (flag) {
        flag = 1;
    }

    /* check if the atomicity flag is the same on all processes */
    tmp = flag;
    data->ompio_fh.f_comm->c_coll->coll_bcast (&tmp,
                                              1,
                                              MPI_INT,
                                              OMPIO_ROOT,
                                              data->ompio_fh.f_comm,
                                              data->ompio_fh.f_comm->c_coll->coll_bcast_module);

    if (tmp != flag) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return OMPI_ERROR;
    }

    data->ompio_fh.f_atomicity = flag;
    OPAL_THREAD_UNLOCK(&fh->f_lock);

    return OMPI_SUCCESS;
}

int mca_io_ompio_file_get_atomicity (ompi_file_t *fh,
                                     int *flag)
{
    mca_common_ompio_data_t *data;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;

    OPAL_THREAD_LOCK(&fh->f_lock);
    *flag = data->ompio_fh.f_atomicity;
    OPAL_THREAD_UNLOCK(&fh->f_lock);

    return OMPI_SUCCESS;
}

int mca_io_ompio_file_sync (ompi_file_t *fh)
{
    int ret = OMPI_SUCCESS;
    mca_common_ompio_data_t *data;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;

    OPAL_THREAD_LOCK(&fh->f_lock);
    if ( !opal_list_is_empty (&mca_common_ompio_pending_requests) ) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return MPI_ERR_OTHER;
    }

    if ( data->ompio_fh.f_amode & MPI_MODE_RDONLY ) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return MPI_ERR_ACCESS;
    }        
    // Make sure all processes reach this point before syncing the file.
    ret = data->ompio_fh.f_comm->c_coll->coll_barrier (data->ompio_fh.f_comm,
                                                       data->ompio_fh.f_comm->c_coll->coll_barrier_module);
    if ( MPI_SUCCESS != ret ) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return ret;
    }
    ret = data->ompio_fh.f_fs->fs_file_sync (&data->ompio_fh);
    OPAL_THREAD_UNLOCK(&fh->f_lock);

    return ret;
}


int mca_io_ompio_file_seek (ompi_file_t *fh,
                            OMPI_MPI_OFFSET_TYPE off,
                            int whence)
{
    int ret = OMPI_SUCCESS;
    mca_common_ompio_data_t *data;
    OMPI_MPI_OFFSET_TYPE offset, temp_offset;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;

    OPAL_THREAD_LOCK(&fh->f_lock);
    offset = off * data->ompio_fh.f_etype_size;

    switch(whence) {
    case MPI_SEEK_SET:
        if (offset < 0) {
            OPAL_THREAD_UNLOCK(&fh->f_lock);
            return OMPI_ERROR;
        }
        break;
    case MPI_SEEK_CUR:
        ret = mca_common_ompio_file_get_position (&data->ompio_fh,
                                                  &temp_offset);
        offset += temp_offset;
        if (offset < 0) {
            OPAL_THREAD_UNLOCK(&fh->f_lock);
            return OMPI_ERROR;
        }
        break;
    case MPI_SEEK_END:
        ret = data->ompio_fh.f_fs->fs_file_get_size (&data->ompio_fh,
                                                     &temp_offset);
        offset += temp_offset;
        if (offset < 0 || OMPI_SUCCESS != ret) {
            OPAL_THREAD_UNLOCK(&fh->f_lock);
            return OMPI_ERROR;
        }
        break;
    default:
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return OMPI_ERROR;
    }

    ret = mca_common_ompio_set_explicit_offset (&data->ompio_fh,
                                             offset/data->ompio_fh.f_etype_size);
    OPAL_THREAD_UNLOCK(&fh->f_lock);

    return ret;
}

int mca_io_ompio_file_get_position (ompi_file_t *fd,
                                    OMPI_MPI_OFFSET_TYPE *offset)
{
    int ret=OMPI_SUCCESS;
    mca_common_ompio_data_t *data=NULL;
    ompio_file_t *fh=NULL;

    data = (mca_common_ompio_data_t *) fd->f_io_selected_data;
    fh = &data->ompio_fh;

    OPAL_THREAD_LOCK(&fd->f_lock);
    ret = mca_common_ompio_file_get_position (fh, offset);
    OPAL_THREAD_UNLOCK(&fd->f_lock);

    return ret;
}


int mca_io_ompio_file_get_byte_offset (ompi_file_t *fh,
                                       OMPI_MPI_OFFSET_TYPE offset,
                                       OMPI_MPI_OFFSET_TYPE *disp)
{
    mca_common_ompio_data_t *data;
    int i, k, index;
    long temp_offset;

    data = (mca_common_ompio_data_t *) fh->f_io_selected_data;

    OPAL_THREAD_LOCK(&fh->f_lock);
    temp_offset = (long) data->ompio_fh.f_view_extent *
        (offset*data->ompio_fh.f_etype_size / data->ompio_fh.f_view_size);
    if ( 0 > temp_offset ) {
        OPAL_THREAD_UNLOCK(&fh->f_lock);
        return MPI_ERR_ARG;
    }
    
    i = (offset*data->ompio_fh.f_etype_size) % data->ompio_fh.f_view_size;
    index = 0;
    k = 0;

    while (1) {
        k = data->ompio_fh.f_decoded_iov[index].iov_len;
        if (i >= k) {
            i -= k;
            index++;
            if ( 0 == i ) {
                k=0;
                break;
            }
        }
        else {
            k=i;
            break;
        }
    }

    *disp = data->ompio_fh.f_disp + temp_offset +
        (OMPI_MPI_OFFSET_TYPE)(intptr_t)data->ompio_fh.f_decoded_iov[index].iov_base + k;
    OPAL_THREAD_UNLOCK(&fh->f_lock);

    return OMPI_SUCCESS;
}

int mca_io_ompio_file_seek_shared (ompi_file_t *fp,
                                   OMPI_MPI_OFFSET_TYPE offset,
                                   int whence)
{
    int ret = OMPI_SUCCESS;
    mca_common_ompio_data_t *data;
    ompio_file_t *fh;
    mca_sharedfp_base_module_t * shared_fp_base_module;

    data = (mca_common_ompio_data_t *) fp->f_io_selected_data;
    fh = &data->ompio_fh;

    /*get the shared fp module associated with this file*/
    shared_fp_base_module = fh->f_sharedfp;
    if ( NULL == shared_fp_base_module ){
        opal_output(0, "No shared file pointer component found for this communicator. Can not execute\n");
        return OMPI_ERROR;
    }

    OPAL_THREAD_LOCK(&fp->f_lock);
    ret = shared_fp_base_module->sharedfp_seek(fh,offset,whence);
    OPAL_THREAD_UNLOCK(&fp->f_lock);

    return ret;
}


int mca_io_ompio_file_get_position_shared (ompi_file_t *fp,
                                           OMPI_MPI_OFFSET_TYPE * offset)
{
    int ret = OMPI_SUCCESS;
    mca_common_ompio_data_t *data;
    ompio_file_t *fh;
    mca_sharedfp_base_module_t * shared_fp_base_module;

    data = (mca_common_ompio_data_t *) fp->f_io_selected_data;
    fh = &data->ompio_fh;

    /*get the shared fp module associated with this file*/
    shared_fp_base_module = fh->f_sharedfp;
    if ( NULL == shared_fp_base_module ){
        opal_output(0, "No shared file pointer component found for this communicator. Can not execute\n");
        return OMPI_ERROR;
    }
    OPAL_THREAD_LOCK(&fp->f_lock);
    ret = shared_fp_base_module->sharedfp_get_position(fh,offset);
    *offset = *offset / fh->f_etype_size;
    OPAL_THREAD_UNLOCK(&fp->f_lock);

    return ret;
}

