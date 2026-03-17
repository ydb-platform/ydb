/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2017      University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "fbtl_posix.h"

#include "mpi.h"
#include <unistd.h>
#include <sys/uio.h>
#include <errno.h>
#include <limits.h>
#include "ompi/constants.h"
#include "ompi/mca/fbtl/fbtl.h"

#define MAX_ERRCOUNT 100

/*
  op:    can be F_WRLCK or F_RDLCK
  flags: can be OMPIO_LOCK_ENTIRE_REGION or OMPIO_LOCK_SELECTIVE. This is typically set by the operation, not the fs component.
         e.g. a collective and an individual component might require different level of protection through locking, 
         also one might need to do different things for blocking (pwritev,preadv) operations and non-blocking (aio) operations.

  fh->f_flags can contain similar sounding flags, those were set by the fs component and/or user requests.
  
  Support for MPI atomicity operations are envisioned, but not yet tested.
*/

int mca_fbtl_posix_lock ( struct flock *lock, ompio_file_t *fh, int op, 
                          OMPI_MPI_OFFSET_TYPE offset, off_t len, int flags)
{
    off_t lmod, bmod;
    int ret, err_count;

    lock->l_type   = op;
    lock->l_whence = SEEK_SET;
    lock->l_start  =-1;
    lock->l_len    =-1;
    if ( 0 == len ) {
        return 0;
    } 

    if ( fh->f_flags & OMPIO_LOCK_ENTIRE_FILE ) {
        lock->l_start = (off_t) 0;
        lock->l_len   = 0;
    }  
    else {
        if ( (fh->f_flags & OMPIO_LOCK_NEVER) ||
             (fh->f_flags & OMPIO_LOCK_NOT_THIS_OP )){
            /* OMPIO_LOCK_NEVER:
                 ompio tells us not to worry about locking. This can be due to three
                 reasons:
                 1. user enforced
                 2. single node job where the locking is handled already in the kernel
                 3. file view is set to distinct regions such that multiple processes
                    do not collide on the block level. ( not entirely sure yet how
                    to check for this except in trivial cases).
               OMPI_LOCK_NOT_THIS_OP:
                 will typically be set by fcoll components indicating that the file partitioning
                 ensures no overlap in blocks.
            */
            return 0;
        }
        if ( flags == OMPIO_LOCK_ENTIRE_REGION ) {
            lock->l_start = (off_t) offset;
            lock->l_len   = len;            
        }
        else {
            /* We only try to lock the first block in the data range if
               the starting offset is not the starting offset of a file system 
               block. And the last block in the data range if the offset+len
               is not equal to the end of a file system block.
               If we need to lock both beginning + end, we combine 
               the two into a single lock.
            */
            bmod = offset % fh->f_fs_block_size; 
            if ( bmod  ) {
                lock->l_start = (off_t) offset;
                lock->l_len   = bmod;
            }
            lmod = (offset+len)%fh->f_fs_block_size;
            if ( lmod ) {
                if ( !bmod ) {
                    lock->l_start = (offset+len-lmod );
                    lock->l_len   = lmod;
                }
                else {
                    lock->l_len = len;
                }
            }
            if ( -1 == lock->l_start && -1 == lock->l_len ) {
                /* no need to lock in this instance */
                return 0;
            }
        }
    }


#ifdef OMPIO_DEBUG
    printf("%d: acquiring lock for offset %ld length %ld requested offset %ld request len %ld \n", 
           fh->f_rank, lock->l_start, lock->l_len, offset, len);
#endif
    errno=0;
    err_count=0;
    do {
        ret = fcntl ( fh->fd, F_SETLKW, lock);
        if ( ret ) {
#ifdef OMPIO_DEBUG
            printf("[%d] ret = %d errno=%d %s\n", fh->f_rank, ret, errno, strerror(errno) );
#endif
            err_count++;
        }
    } while (  ret && ((errno == EINTR) || ((errno == EINPROGRESS) && err_count < MAX_ERRCOUNT )));


    return ret;
}

void  mca_fbtl_posix_unlock ( struct flock *lock, ompio_file_t *fh )
{
    if ( -1 == lock->l_start && -1 == lock->l_len ) {
        return;
    }
    
    lock->l_type = F_UNLCK;
#ifdef OMPIO_DEBUG
    printf("%d: releasing lock for offset %ld length %ld\n", fh->f_rank, lock->l_start, lock->l_len);
#endif
    fcntl ( fh->fd, F_SETLK, lock);     
    lock->l_start = -1;
    lock->l_len   = -1;

    return;
}
