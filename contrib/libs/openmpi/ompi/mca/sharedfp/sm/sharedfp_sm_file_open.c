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
 * Copyright (c) 2013-2018 University of Houston. All rights reserved.
 * Copyright (c) 2013      Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"

#if HAVE_LIBGEN_H
#include <libgen.h>
#endif
#if HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#include "sharedfp_sm.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/group/group.h"
#include "ompi/proc/proc.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"

#include <semaphore.h>
#include <sys/mman.h>
#include <libgen.h>
#include <unistd.h>

int mca_sharedfp_sm_file_open (struct ompi_communicator_t *comm,
                               const char* filename,
                               int amode,
                               struct opal_info_t *info,
                               ompio_file_t *fh)
{
    int err = OMPI_SUCCESS;
    struct mca_sharedfp_base_data_t* sh;
    struct mca_sharedfp_sm_data * sm_data = NULL;
    char * filename_basename;
    char * sm_filename;
    int sm_filename_length;
    struct mca_sharedfp_sm_offset * sm_offset_ptr;
    struct mca_sharedfp_sm_offset sm_offset;
    int sm_fd;
    uint32_t comm_cid;
    int int_pid;
    pid_t my_pid;

    /*Memory is allocated here for the sh structure*/
    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "mca_sharedfp_sm_file_open: malloc f_sharedfp_ptr struct\n");
    }

    sh = (struct mca_sharedfp_base_data_t*)malloc(sizeof(struct mca_sharedfp_base_data_t));
    if ( NULL == sh ) {
        opal_output(0, "mca_sharedfp_sm_file_open: Error, unable to malloc f_sharedfp  struct\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /*Populate the sh file structure based on the implementation*/
    sh->global_offset = 0;                        /* Global Offset*/
    sh->selected_module_data = NULL;

    /*Open a shared memory segment which will hold the shared file pointer*/
    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "mca_sharedfp_sm_file_open: allocatge shared memory segment.\n");
    }


    sm_data = (struct mca_sharedfp_sm_data*) malloc ( sizeof(struct mca_sharedfp_sm_data));
    if ( NULL == sm_data ){
        opal_output(0, "mca_sharedfp_sm_file_open: Error, unable to malloc sm_data struct\n");
        free(sh);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    sm_data->sm_filename=NULL;


    /* the shared memory segment is identified opening a file
    ** and then mapping it to memory
    ** For sharedfp we also want to put the file backed shared memory into the tmp directory
    */
    filename_basename = basename((char*)filename);
    /* format is "%s/%s_cid-%d-%d.sm", see below */
    sm_filename_length = strlen(ompi_process_info.job_session_dir) + 1 + strlen(filename_basename) + 5 + (3*sizeof(uint32_t)+1) + 4;
    sm_filename = (char*) malloc( sizeof(char) * sm_filename_length);
    if (NULL == sm_filename) {
        opal_output(0, "mca_sharedfp_sm_file_open: Error, unable to malloc sm_filename\n");
        free(sm_data);
        free(sh);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    comm_cid = ompi_comm_get_cid(comm);
    if ( 0 == fh->f_rank ) {
        my_pid = getpid();
        int_pid = (int) my_pid;
    }
    err = comm->c_coll->coll_bcast (&int_pid, 1, MPI_INT, 0, comm, comm->c_coll->coll_bcast_module );
    if ( OMPI_SUCCESS != err ) {
        opal_output(0,"mca_sharedfp_sm_file_open: Error in bcast operation \n");
        free(sm_filename);
        free(sm_data);
        free(sh);
        return err;
    }

    snprintf(sm_filename, sm_filename_length, "%s/%s_cid-%d-%d.sm", ompi_process_info.job_session_dir,
             filename_basename, comm_cid, int_pid);
    /* open shared memory file, initialize to 0, map into memory */
    sm_fd = open(sm_filename, O_RDWR | O_CREAT,
                 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if ( sm_fd == -1){
        /*error opening file*/
        opal_output(0,"mca_sharedfp_sm_file_open: Error, unable to open file for mmap: %s\n",sm_filename);
        free(sm_filename);
        free(sm_data);
        free(sh);
        return OMPI_ERROR;
    }

    sm_data->sm_filename = sm_filename;

    /* TODO: is it necessary to write to the file first? */
    if( 0 == fh->f_rank ){
        memset ( &sm_offset, 0, sizeof (struct mca_sharedfp_sm_offset ));
        write ( sm_fd, &sm_offset, sizeof(struct mca_sharedfp_sm_offset));
    }
    err = comm->c_coll->coll_barrier (comm, comm->c_coll->coll_barrier_module );
    if ( OMPI_SUCCESS != err ) {
        opal_output(0,"mca_sharedfp_sm_file_open: Error in barrier operation \n");
        free(sm_filename);
        free(sm_data);
        free(sh);
        close (sm_fd);
        return err;
    }

    /*the file has been written to, now we can map*/
    sm_offset_ptr = mmap(NULL, sizeof(struct mca_sharedfp_sm_offset), PROT_READ | PROT_WRITE,
                         MAP_SHARED, sm_fd, 0);

    close(sm_fd);

    if ( sm_offset_ptr==MAP_FAILED){
        err = OMPI_ERROR;
        opal_output(0, "mca_sharedfp_sm_file_open: Error, unable to mmap file: %s\n",sm_filename);
        opal_output(0, "%s\n", strerror(errno));
        free(sm_filename);
        free(sm_data);
        free(sh);
        return OMPI_ERROR;
    }

    /* Initialize semaphore so that is shared between processes.           */
    /* the semaphore is shared by keeping it in the shared memory segment  */

#if defined(HAVE_SEM_OPEN)

#if defined (__APPLE__)
    sm_data->sem_name = (char*) malloc( sizeof(char) * 32);
    snprintf(sm_data->sem_name,31,"OMPIO_%s",filename_basename);
#else
    sm_data->sem_name = (char*) malloc( sizeof(char) * 253);
    snprintf(sm_data->sem_name,252,"OMPIO_%s",filename_basename);
#endif

    if( (sm_data->mutex = sem_open(sm_data->sem_name, O_CREAT, 0644, 1)) != SEM_FAILED ) {
#elif defined(HAVE_SEM_INIT)
    sm_data->mutex = &sm_offset_ptr->mutex;
    if(sem_init(&sm_offset_ptr->mutex, 1, 1) != -1){
#endif
        /*If opening was successful*/
        /*Store the new file handle*/
        sm_data->sm_offset_ptr = sm_offset_ptr;
        /* Assign the sm_data to sh->selected_module_data*/
        sh->selected_module_data   = sm_data;
        /*remember the shared file handle*/
        fh->f_sharedfp_data = sh;

        /*write initial zero*/
        if(fh->f_rank==0){
            MPI_Offset position=0;

            sem_wait(sm_data->mutex);
            sm_offset_ptr->offset=position;
            sem_post(sm_data->mutex);
        }
    }else{
        free(sm_filename);
        free(sm_data);
        free(sh);
        munmap(sm_offset_ptr, sizeof(struct mca_sharedfp_sm_offset));
        return OMPI_ERROR;
    }

    err = comm->c_coll->coll_barrier (comm, comm->c_coll->coll_barrier_module );
    if ( OMPI_SUCCESS != err ) {
        opal_output(0,"mca_sharedfp_sm_file_open: Error in barrier operation \n");
        free(sm_filename);
        free(sm_data);
        free(sh);
        munmap(sm_offset_ptr, sizeof(struct mca_sharedfp_sm_offset));
        return err;
    }

#if defined(HAVE_SEM_OPEN)
    if ( 0 == fh->f_rank ) {
        sem_unlink ( sm_data->sem_name);
    }
#endif

    return OMPI_SUCCESS;
}

int mca_sharedfp_sm_file_close (ompio_file_t *fh)
{
    int err = OMPI_SUCCESS;
    /*sharedfp data structure*/
    struct mca_sharedfp_base_data_t *sh=NULL;
    /*sharedfp sm module data structure*/
    struct mca_sharedfp_sm_data * file_data=NULL;

    if( NULL == fh->f_sharedfp_data ){
        return OMPI_SUCCESS;
    }
    sh = fh->f_sharedfp_data;

    /* Use an MPI Barrier in order to make sure that
     * all processes are ready to release the
     * shared file pointer resources
     */
    fh->f_comm->c_coll->coll_barrier (fh->f_comm, fh->f_comm->c_coll->coll_barrier_module );

    file_data = (sm_data*)(sh->selected_module_data);
    if (file_data)  {
        /*Close sm handle*/
        if (file_data->sm_offset_ptr) {
            /* destroy semaphore */
#if defined(HAVE_SEM_OPEN)
             sem_close ( file_data->mutex);
             free (file_data->sem_name);
#elif defined(HAVE_SEM_INIT)
            sem_destroy(&file_data->sm_offset_ptr->mutex);
#endif
            /*Release the shared memory segment.*/
            munmap(file_data->sm_offset_ptr,sizeof(struct mca_sharedfp_sm_offset));
            /*Q: Do we need to delete the file? */
            remove(file_data->sm_filename);
        }
        /*free our sm data structure*/
        if(file_data->sm_filename){
            free(file_data->sm_filename);
        }
        free(file_data);
    }

    /*free shared file pointer data struct*/
    free(sh);

    return err;
}
