/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <errno.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif  /* HAVE_FCNTL_H */
#if OPAL_HAVE_SOLARIS && !defined(_POSIX_C_SOURCE)
  #define _POSIX_C_SOURCE 200112L /* Required for shm_{open,unlink} decls */
  #include <sys/mman.h>
  #undef _POSIX_C_SOURCE
#else
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif /* HAVE_SYS_MMAN_H */
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif /* HAVE_UNISTD_H */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif /* HAVE_SYS_TYPES_H */
#include <string.h>
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif /* HAVE_NETDB_H */

#include "opal/constants.h"
#include "opal_stdint.h"
#include "opal/util/output.h"
#include "opal/util/path.h"
#include "opal/util/show_help.h"
#include "opal/mca/shmem/shmem.h"
#include "opal/mca/shmem/base/base.h"

#include "shmem_posix.h"
#include "shmem_posix_common_utils.h"

/* for tons of debug output: -mca shmem_base_verbose 70 */

/* ////////////////////////////////////////////////////////////////////////// */
/* local functions */
static int
module_init(void);

static int
segment_create(opal_shmem_ds_t *ds_buf,
               const char *file_name,
               size_t size);

static int
ds_copy(const opal_shmem_ds_t *from,
        opal_shmem_ds_t *to);

static void *
segment_attach(opal_shmem_ds_t *ds_buf);

static int
segment_detach(opal_shmem_ds_t *ds_buf);

static int
segment_unlink(opal_shmem_ds_t *ds_buf);

static int
module_finalize(void);

/* posix shmem module */
opal_shmem_posix_module_t opal_shmem_posix_module = {
    .super = {
        .module_init = module_init,
        .segment_create = segment_create,
        .ds_copy = ds_copy,
        .segment_attach = segment_attach,
        .segment_detach = segment_detach,
        .unlink = segment_unlink,
        .module_finalize = module_finalize
    }
};

/* ////////////////////////////////////////////////////////////////////////// */
/* private utility functions */
/* ////////////////////////////////////////////////////////////////////////// */

/* ////////////////////////////////////////////////////////////////////////// */
/**
 * completely resets the contents of *ds_buf
 */
static inline void
shmem_ds_reset(opal_shmem_ds_t *ds_buf)
{
    /* don't print ds_buf info here, as we may be printing garbage. */
    OPAL_OUTPUT_VERBOSE(
        (70, opal_shmem_base_framework.framework_output,
         "%s: %s: shmem_ds_resetting\n",
         mca_shmem_posix_component.super.base_version.mca_type_name,
         mca_shmem_posix_component.super.base_version.mca_component_name)
    );

    ds_buf->seg_cpid = 0;
    OPAL_SHMEM_DS_RESET_FLAGS(ds_buf);
    ds_buf->seg_id = OPAL_SHMEM_DS_ID_INVALID;
    ds_buf->seg_size = 0;
    memset(ds_buf->seg_name, '\0', OPAL_PATH_MAX);
    ds_buf->seg_base_addr = (unsigned char *)MAP_FAILED;
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
module_init(void)
{
    /* nothing to do */
    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
module_finalize(void)
{
    /* nothing to do */
    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
ds_copy(const opal_shmem_ds_t *from,
        opal_shmem_ds_t *to)
{
    memcpy(to, from, sizeof(opal_shmem_ds_t));

    OPAL_OUTPUT_VERBOSE(
        (70, opal_shmem_base_framework.framework_output,
         "%s: %s: ds_copy complete "
         "from: (id: %d, size: %lu, "
         "name: %s flags: 0x%02x) "
         "to: (id: %d, size: %lu, "
         "name: %s flags: 0x%02x)\n",
         mca_shmem_posix_component.super.base_version.mca_type_name,
         mca_shmem_posix_component.super.base_version.mca_component_name,
         from->seg_id, (unsigned long)from->seg_size, from->seg_name,
         from->flags, to->seg_id, (unsigned long)to->seg_size, to->seg_name,
         to->flags)
    );

    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
segment_create(opal_shmem_ds_t *ds_buf,
               const char *file_name,
               size_t size)
{
    int rc = OPAL_SUCCESS;
    pid_t my_pid = getpid();
    /* the real size of the shared memory segment.  this includes enough space
     * to store our segment header.
     */
    size_t real_size = size + sizeof(opal_shmem_seg_hdr_t);
    opal_shmem_seg_hdr_t *seg_hdrp = MAP_FAILED;

    /* init the contents of opal_shmem_ds_t */
    shmem_ds_reset(ds_buf);

    /* for posix shared memory we don't have to worry about the backing store
     * being located on a network file system... so no check is needed here.
     */

    /* calling shmem_posix_shm_open searches for an available posix shared
     * memory object name and upon successful completion populates the name
     * buffer
     */
    if (-1 == (ds_buf->seg_id = shmem_posix_shm_open(
                                    ds_buf->seg_name,
                                    OPAL_SHMEM_POSIX_FILE_LEN_MAX - 1))) {
        /* snaps!  something happened in posix_shm_open.  don't report anything
         * here because posix_shm_open will display all the necessary info.
         */
        rc = OPAL_ERROR;
        goto out;
    }
    /* size backing file - note the use of real_size here */
    else if (0 != ftruncate(ds_buf->seg_id, real_size)) {
        int err = errno;
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-posix.txt", "sys call fail", 1, hn,
                       "ftruncate(2)", "", strerror(err), err);
        rc = OPAL_ERROR;
        goto out;
    }
    else if (MAP_FAILED == (seg_hdrp = (opal_shmem_seg_hdr_t*)mmap(NULL, real_size,
                                                                   PROT_READ | PROT_WRITE, MAP_SHARED,
                                                                   ds_buf->seg_id, 0))) {
        int err = errno;
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-posix.txt", "sys call fail", 1, hn,
                       "mmap(2)", "", strerror(err), err);
        rc = OPAL_ERROR;
        goto out;
    }
    /* all is well */
    else {
        /* -- initialize the shared memory segment -- */
        opal_atomic_rmb();

        /* init segment lock */
        opal_atomic_lock_init(&seg_hdrp->lock, OPAL_ATOMIC_LOCK_UNLOCKED);
        /* i was the creator of this segment, so note that fact */
        seg_hdrp->cpid = my_pid;

        opal_atomic_wmb();

        /* -- initialize the contents of opal_shmem_ds_t -- */
        ds_buf->seg_cpid = my_pid;
        ds_buf->seg_size = real_size;
        ds_buf->seg_base_addr = (unsigned char *)seg_hdrp;

        /* notice that we are not setting ds_buf->name here.  at this point,
         * posix_shm_open was successful, so the contents of ds_buf->name are
         * already set for us :-)
         */

        /* set "valid" bit because setment creation was successful */
        OPAL_SHMEM_DS_SET_VALID(ds_buf);

        OPAL_OUTPUT_VERBOSE(
            (70, opal_shmem_base_framework.framework_output,
             "%s: %s: create successful "
             "(id: %d, size: %lu, name: %s)\n",
             mca_shmem_posix_component.super.base_version.mca_type_name,
             mca_shmem_posix_component.super.base_version.mca_component_name,
             ds_buf->seg_id, (unsigned long)ds_buf->seg_size, ds_buf->seg_name)
        );
    }

out:
    /* in this component, the id is the file descriptor returned by open.  this
     * check is here to see if it is safe to call close on the file descriptor.
     * that is, we are making sure that our call to open was successful and
     * we are not not in an error path.
     */
    if (-1 != ds_buf->seg_id) {
        if (0 != close(ds_buf->seg_id)) {
            int err = errno;
            char hn[OPAL_MAXHOSTNAMELEN];
            gethostname(hn, sizeof(hn));
            opal_show_help("help-opal-shmem-mmap.txt", "sys call fail", 1, hn,
                           "close(2)", "", strerror(err), err);
            rc = OPAL_ERROR;
         }
     }
    /* an error occured, so invalidate the shmem object and release any
     * allocated resources.
     */
    if (OPAL_SUCCESS != rc) {
        /* posix_shm_open was successful, but something else wasn't.
         * note: if the id is not equal to -1 and we are here, name will be
         * valid.  that is, we can safely call shm_unlink with ds_buf->name.
         */
        if (-1 != ds_buf->seg_id) {
            shm_unlink(ds_buf->seg_name);
        }
        if (MAP_FAILED != seg_hdrp) {
            munmap((void*)seg_hdrp, real_size);
        }
        /* always invalidate in this error path */
        shmem_ds_reset(ds_buf);
    }
    return rc;
}

/* ////////////////////////////////////////////////////////////////////////// */
/**
 * segment_attach can only be called after a successful call to segment_create
 */
static void *
segment_attach(opal_shmem_ds_t *ds_buf)
{
    pid_t my_pid = getpid();

    if (my_pid != ds_buf->seg_cpid) {
        if (-1 == (ds_buf->seg_id = shm_open(ds_buf->seg_name, O_RDWR, 0600))) {
            int err = errno;
            char hn[OPAL_MAXHOSTNAMELEN];
            gethostname(hn, sizeof(hn));
            opal_show_help("help-opal-shmem-posix.txt", "sys call fail", 1, hn,
                           "open(2)", "", strerror(err), err);
            return NULL;
        }
        else if (MAP_FAILED == (ds_buf->seg_base_addr =
                                (unsigned char*)mmap(NULL, ds_buf->seg_size,
                                                     PROT_READ | PROT_WRITE, MAP_SHARED,
                                                     ds_buf->seg_id, 0))) {
            int err = errno;
            char hn[OPAL_MAXHOSTNAMELEN];
            gethostname(hn, sizeof(hn));
            opal_show_help("help-opal-shmem-posix.txt", "sys call fail", 1, hn,
                           "mmap(2)", "", strerror(err), err);
            /* mmap failed, so shm_unlink and return NULL - no error check here
             * because we are already in an error path...
             */
            shm_unlink(ds_buf->seg_name);
            return NULL;
        }
        /* all is well */
        else {
            /* if close fails here, that's okay.  just let the user know and
             * continue.  if we got this far, open and mmap were successful...
             */
            if (0 != close(ds_buf->seg_id)) {
                int err = errno;
                char hn[OPAL_MAXHOSTNAMELEN];
                gethostname(hn, sizeof(hn));
                opal_show_help("help-opal-shmem-mmap.txt", "sys call fail", 1,
                               hn, "close(2)", "", strerror(err), err);
             }
        }
    }
    /* else i was the segment creator.  nothing to do here because all the hard
     * work was done in segment_create :-).
     */

    OPAL_OUTPUT_VERBOSE(
        (70, opal_shmem_base_framework.framework_output,
         "%s: %s: attach successful "
         "(id: %d, size: %lu, name: %s)\n",
         mca_shmem_posix_component.super.base_version.mca_type_name,
         mca_shmem_posix_component.super.base_version.mca_component_name,
         ds_buf->seg_id, (unsigned long)ds_buf->seg_size, ds_buf->seg_name)
    );

    /* update returned base pointer with an offset that hides our stuff */
    return (ds_buf->seg_base_addr + sizeof(opal_shmem_seg_hdr_t));
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
segment_detach(opal_shmem_ds_t *ds_buf)
{
    int rc = OPAL_SUCCESS;

    OPAL_OUTPUT_VERBOSE(
        (70, opal_shmem_base_framework.framework_output,
         "%s: %s: detaching "
         "(id: %d, size: %lu, name: %s)\n",
         mca_shmem_posix_component.super.base_version.mca_type_name,
         mca_shmem_posix_component.super.base_version.mca_component_name,
         ds_buf->seg_id, (unsigned long)ds_buf->seg_size, ds_buf->seg_name)
    );

    if (0 != munmap((void*)ds_buf->seg_base_addr, ds_buf->seg_size)) {
        int err = errno;
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-posix.txt", "sys call fail", 1, hn,
                       "munmap(2)", "", strerror(err), err);
        rc = OPAL_ERROR;
    }
    /* reset the contents of the opal_shmem_ds_t associated with this
     * shared memory segment.
     */
    shmem_ds_reset(ds_buf);
    return rc;
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
segment_unlink(opal_shmem_ds_t *ds_buf)
{
    OPAL_OUTPUT_VERBOSE(
        (70, opal_shmem_base_framework.framework_output,
         "%s: %s: unlinking "
         "(id: %d, size: %lu, name: %s)\n",
         mca_shmem_posix_component.super.base_version.mca_type_name,
         mca_shmem_posix_component.super.base_version.mca_component_name,
         ds_buf->seg_id, (unsigned long)ds_buf->seg_size, ds_buf->seg_name)
    );

    if (-1 == shm_unlink(ds_buf->seg_name)) {
        int err = errno;
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-posix.txt", "sys call fail", 1, hn,
                       "shm_unlink(2)", ds_buf->seg_name, strerror(err), err);
        return OPAL_ERROR;
    }

    /* don't completely reset the opal_shmem_ds_t.  in particular, only reset
     * the id and flip the invalid bit.  size and name values will remain valid
     * across unlinks. other information stored in flags will remain untouched.
     */
    ds_buf->seg_id = OPAL_SHMEM_DS_ID_INVALID;
    /* note: this is only chaning the valid bit to 0. */
    OPAL_SHMEM_DS_INVALIDATE(ds_buf);
    return OPAL_SUCCESS;
}

