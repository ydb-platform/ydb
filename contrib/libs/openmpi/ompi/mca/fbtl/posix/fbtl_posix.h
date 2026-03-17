/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_FBTL_POSIX_H
#define MCA_FBTL_POSIX_H

#include "ompi_config.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/fbtl/fbtl.h"
#include "ompi/mca/common/ompio/common_ompio.h"
#include "ompi/mca/common/ompio/common_ompio_request.h"

extern int mca_fbtl_posix_priority;

BEGIN_C_DECLS

int mca_fbtl_posix_component_init_query(bool enable_progress_threads,
                                        bool enable_mpi_threads);
struct mca_fbtl_base_module_1_0_0_t *
mca_fbtl_posix_component_file_query (ompio_file_t *file, int *priority);
int mca_fbtl_posix_component_file_unquery (ompio_file_t *file);

int mca_fbtl_posix_module_init (ompio_file_t *file);
int mca_fbtl_posix_module_finalize (ompio_file_t *file);

extern int fbtl_posix_max_aio_active_reqs;

OMPI_MODULE_DECLSPEC extern mca_fbtl_base_component_2_0_0_t mca_fbtl_posix_component;
/*
 * ******************************************************************
 * ********* functions which are implemented in this module *********
 * ******************************************************************
 */

ssize_t mca_fbtl_posix_preadv (ompio_file_t *file );
ssize_t mca_fbtl_posix_pwritev (ompio_file_t *file );
ssize_t mca_fbtl_posix_ipreadv (ompio_file_t *file,
                               ompi_request_t *request);
ssize_t mca_fbtl_posix_ipwritev (ompio_file_t *file,
                                ompi_request_t *request);

bool mca_fbtl_posix_progress     ( mca_ompio_request_t *req);
void mca_fbtl_posix_request_free ( mca_ompio_request_t *req);

int mca_fbtl_posix_lock ( struct flock *lock, ompio_file_t *fh, int op, 
                          OMPI_MPI_OFFSET_TYPE iov_offset, off_t len, int flags);
void  mca_fbtl_posix_unlock ( struct flock *lock, ompio_file_t *fh );


struct mca_fbtl_posix_request_data_t {
    int            aio_req_count;       /* total number of aio reqs */
    int            aio_open_reqs;       /* number of unfinished reqs */
    int            aio_req_type;        /* read or write */
    int            aio_req_chunks;      /* max. no. of aio reqs that can be posted at once*/
    int            aio_first_active_req; /* first active posted req */
    int            aio_last_active_req;  /* last currently active poted req */
    struct aiocb       *aio_reqs;       /* pointer array of req structures */
    int          *aio_req_status;       /* array of statuses */
    ssize_t        aio_total_len;       /* total amount of data written */
    struct flock   aio_lock;            /* lock used for certain file systems */
    ompio_file_t  *aio_fh;       /* pointer back to the mca_io_ompio_fh structure */
};
typedef struct mca_fbtl_posix_request_data_t mca_fbtl_posix_request_data_t;

/* Right now statically defined, will become a configure check */
#define FBTL_POSIX_HAVE_AIO 1

/* define constants for AIO requests */
#define FBTL_POSIX_READ 1
#define FBTL_POSIX_WRITE 2


/*
 * ******************************************************************
 * ************ functions implemented in this module end ************
 * ******************************************************************
 */

END_C_DECLS

#endif /* MCA_FBTL_POSIX_H */
