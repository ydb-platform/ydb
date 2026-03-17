/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2010-2014 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2016      University of Houston. All rights reserved.
 *
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
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif /* HAVE_SYS_MMAN_H */
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
#include <time.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif /* HAVE_SYS_STAT_H */

#include "opal_stdint.h"
#include "opal/constants.h"
#include "opal/util/alfg.h"
#include "opal/util/output.h"
#include "opal/util/path.h"
#include "opal/util/show_help.h"
#include "opal/mca/shmem/shmem.h"
#include "opal/mca/shmem/base/base.h"

#include "shmem_mmap.h"

/* for tons of debug output: -mca shmem_base_verbose 70 */

/* ////////////////////////////////////////////////////////////////////////// */
/*local functions */
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

static int
enough_space(const char *filename,
             size_t space_req,
             uint64_t *space_avail,
             bool *result);

/*
 * mmap shmem module
 */
opal_shmem_mmap_module_t opal_shmem_mmap_module = {
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
         mca_shmem_mmap_component.super.base_version.mca_type_name,
         mca_shmem_mmap_component.super.base_version.mca_component_name)
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
enough_space(const char *filename,
             size_t space_req,
             uint64_t *space_avail,
             bool *result)
{
    uint64_t avail = 0;
    size_t fluff = (size_t)(.05 * space_req);
    bool enough = false;
    char *last_sep = NULL;
    /* the target file name is passed here, but we need to check the parent
     * directory. store it so we can extract that info later. */
    char *target_dir = strdup(filename);
    int rc;

    if (NULL == target_dir) {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto out;
    }
    /* get the parent directory */
    last_sep = strrchr(target_dir, OPAL_PATH_SEP[0]);
    *last_sep = '\0';
    /* now check space availability */
    if (OPAL_SUCCESS != (rc = opal_path_df(target_dir, &avail))) {
        OPAL_OUTPUT_VERBOSE(
            (70, opal_shmem_base_framework.framework_output,
             "WARNING: opal_path_df failure!")
        );
        goto out;
    }
    /* do we have enough space? */
    if (avail >= space_req + fluff) {
        enough = true;
    }
    else {
        OPAL_OUTPUT_VERBOSE(
            (70, opal_shmem_base_framework.framework_output,
             "WARNING: not enough space on %s to meet request!"
             "available: %"PRIu64 "requested: %lu", target_dir,
             avail, (unsigned long)space_req + fluff)
        );
    }

out:
    if (NULL != target_dir) {
        free(target_dir);
    }
    *result = enough;
    *space_avail = avail;
    return rc;
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
         mca_shmem_mmap_component.super.base_version.mca_type_name,
         mca_shmem_mmap_component.super.base_version.mca_component_name,
         from->seg_id, (unsigned long)from->seg_size, from->seg_name,
         from->flags, to->seg_id, (unsigned long)to->seg_size, to->seg_name,
         to->flags)
    );

    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
static unsigned long
sdbm_hash(const unsigned char *hash_key)
{
    unsigned long str_hash = 0;
    int c;

    /* hash using sdbm algorithm */
    while ((c = *hash_key++)) {
        str_hash = c + (str_hash << 6) + (str_hash << 16) - str_hash;
    }
    return str_hash;
}

/* ////////////////////////////////////////////////////////////////////////// */
static bool
path_usable(const char *path, int *stat_errno)
{
    struct stat buf;
    int rc;

    rc = stat(path, &buf);
    *stat_errno = errno;
    return (0 == rc);
}

/* ////////////////////////////////////////////////////////////////////////// */
/* the file name is only guaranteed to be unique on the local host.  if there
 * was a failure that left backing files behind, then no such guarantees can be
 * made.  we use the pid + file_name hash + random number to help avoid issues.
 *
 * caller is responsible for freeing returned resources. the returned string
 * will be OPAL_PATH_MAX long.
 */
static char *
get_uniq_file_name(const char *base_path, const char *hash_key)
{
    char *uniq_name_buf = NULL;
    unsigned long str_hash = 0;
    pid_t my_pid;
    opal_rng_buff_t rand_buff;
    uint32_t rand_num;

    /* invalid argument */
    if (NULL == hash_key) {
        return NULL;
    }
    if (NULL == (uniq_name_buf = calloc(OPAL_PATH_MAX, sizeof(char)))) {
        /* out of resources */
        return NULL;
    }

    my_pid = getpid();
    opal_srand(&rand_buff,((uint32_t)(time(NULL) + my_pid)));
    rand_num = opal_rand(&rand_buff) % 1024;
    str_hash = sdbm_hash((unsigned char *)hash_key);
    /* build the name */
    snprintf(uniq_name_buf, OPAL_PATH_MAX, "%s/open_mpi_shmem_mmap.%d_%lu_%d",
             base_path, (int)my_pid, str_hash, rand_num);

    return uniq_name_buf;
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
segment_create(opal_shmem_ds_t *ds_buf,
               const char *file_name,
               size_t size)
{
    int rc = OPAL_SUCCESS;
    char *real_file_name = NULL;
    pid_t my_pid = getpid();
    bool space_available = false;
    uint64_t amount_space_avail = 0;

    /* the real size of the shared memory segment.  this includes enough space
     * to store our segment header.
     */
    size_t real_size = size + sizeof(opal_shmem_seg_hdr_t);
    opal_shmem_seg_hdr_t *seg_hdrp = MAP_FAILED;

    /* init the contents of opal_shmem_ds_t */
    shmem_ds_reset(ds_buf);

    /* change the path of shmem mmap's backing store? */
    if (0 != opal_shmem_mmap_relocate_backing_file) {
        int err;
        if (path_usable(opal_shmem_mmap_backing_file_base_dir, &err)) {
            if (NULL ==
                (real_file_name =
                     get_uniq_file_name(opal_shmem_mmap_backing_file_base_dir,
                                        file_name))) {
                /* out of resources */
                return OPAL_ERROR;
            }
        }
        /* a relocated backing store was requested, but the path specified
         * cannot be used :-(. if the flag is negative, then warn and continue
         * with the default path.  otherwise, fail.
         */
        else if (opal_shmem_mmap_relocate_backing_file < 0) {
            opal_output(0, "shmem: mmap: WARNING: could not relocate "
                        "backing store to \"%s\" (%s).  Continuing with "
                        "default path.\n",
                        opal_shmem_mmap_backing_file_base_dir, strerror(err));
        }
        /* must be positive, so fail */
        else {
            opal_output(0, "shmem: mmap: WARNING: could not relocate "
                        "backing store to \"%s\" (%s).  Cannot continue with "
                        "shmem mmap.\n", opal_shmem_mmap_backing_file_base_dir,
                        strerror(err));
            return OPAL_ERROR;
        }
    }
    /* are we using the default path? */
    if (NULL == real_file_name) {
        /* use the path specified by the caller of this function */
        if (NULL == (real_file_name = strdup(file_name))) {
            /* out of resources */
            return OPAL_ERROR;
        }
    }

    OPAL_OUTPUT_VERBOSE(
        (70, opal_shmem_base_framework.framework_output,
         "%s: %s: backing store base directory: %s\n",
         mca_shmem_mmap_component.super.base_version.mca_type_name,
         mca_shmem_mmap_component.super.base_version.mca_component_name,
         real_file_name)
    );

    /* determine whether the specified filename is on a network file system.
     * this is an important check because if the backing store is located on
     * a network filesystem, the user may see a shared memory performance hit.
     */
    if (opal_shmem_mmap_nfs_warning && opal_path_nfs(real_file_name, NULL)) {
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-mmap.txt", "mmap on nfs", 1, hn,
                       real_file_name);
    }
    /* let's make sure we have enough space for the backing file */
    if (OPAL_SUCCESS != (rc = enough_space(real_file_name,
                                           real_size,
                                           &amount_space_avail,
                                           &space_available))) {
        opal_output(0, "shmem: mmap: an error occurred while determining "
                    "whether or not %s could be created.", real_file_name);
        /* rc is set */
        goto out;
    }
    if (!space_available) {
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        opal_show_help("help-opal-shmem-mmap.txt", "target full", 1,
                       real_file_name, hn, (unsigned long)real_size,
                       (unsigned long long)amount_space_avail);
        goto out;
    }
    /* enough space is available, so create the segment */
    if (-1 == (ds_buf->seg_id = open(real_file_name, O_CREAT | O_RDWR, 0600))) {
        int err = errno;
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-mmap.txt", "sys call fail", 1, hn,
                       "open(2)", "", strerror(err), err);
        rc = OPAL_ERROR;
        goto out;
    }
    /* size backing file - note the use of real_size here */
    if (0 != ftruncate(ds_buf->seg_id, real_size)) {
        int err = errno;
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-mmap.txt", "sys call fail", 1, hn,
                       "ftruncate(2)", "", strerror(err), err);
        rc = OPAL_ERROR;
        goto out;
    }
    if (MAP_FAILED == (seg_hdrp = (opal_shmem_seg_hdr_t *)
                                  mmap(NULL, real_size,
                                       PROT_READ | PROT_WRITE, MAP_SHARED,
                                       ds_buf->seg_id, 0))) {
        int err = errno;
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-mmap.txt", "sys call fail", 1, hn,
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
        (void)strncpy(ds_buf->seg_name, real_file_name, OPAL_PATH_MAX - 1);

        /* set "valid" bit because setment creation was successful */
        OPAL_SHMEM_DS_SET_VALID(ds_buf);

        OPAL_OUTPUT_VERBOSE(
            (70, opal_shmem_base_framework.framework_output,
             "%s: %s: create successful "
             "(id: %d, size: %lu, name: %s)\n",
             mca_shmem_mmap_component.super.base_version.mca_type_name,
             mca_shmem_mmap_component.super.base_version.mca_component_name,
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
    /* an error occured, so invalidate the shmem object and munmap if needed */
    if (OPAL_SUCCESS != rc) {
        if (MAP_FAILED != seg_hdrp) {
            munmap((void *)seg_hdrp, real_size);
        }
        shmem_ds_reset(ds_buf);
    }
    /* safe to free now because its contents have already been copied */
    if (NULL != real_file_name) {
        free(real_file_name);
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
        if (-1 == (ds_buf->seg_id = open(ds_buf->seg_name, O_RDWR))) {
            int err = errno;
            char hn[OPAL_MAXHOSTNAMELEN];
            gethostname(hn, sizeof(hn));
            opal_show_help("help-opal-shmem-mmap.txt", "sys call fail", 1, hn,
                           "open(2)", "", strerror(err), err);
            return NULL;
        }
        if (MAP_FAILED == (ds_buf->seg_base_addr = (unsigned char *)
                              mmap(NULL, ds_buf->seg_size,
                                   PROT_READ | PROT_WRITE, MAP_SHARED,
                                   ds_buf->seg_id, 0))) {
            int err = errno;
            char hn[OPAL_MAXHOSTNAMELEN];
            gethostname(hn, sizeof(hn));
            opal_show_help("help-opal-shmem-mmap.txt", "sys call fail", 1, hn,
                           "mmap(2)", "", strerror(err), err);
            /* mmap failed, so close the file and return NULL - no error check
             * here because we are already in an error path...
             */
            close(ds_buf->seg_id);
            return NULL;
        }
        /* all is well */
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
    /* else i was the segment creator.  nothing to do here because all the hard
     * work was done in segment_create :-).
     */

    OPAL_OUTPUT_VERBOSE(
        (70, opal_shmem_base_framework.framework_output,
         "%s: %s: attach successful "
         "(id: %d, size: %lu, name: %s)\n",
         mca_shmem_mmap_component.super.base_version.mca_type_name,
         mca_shmem_mmap_component.super.base_version.mca_component_name,
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
         mca_shmem_mmap_component.super.base_version.mca_type_name,
         mca_shmem_mmap_component.super.base_version.mca_component_name,
         ds_buf->seg_id, (unsigned long)ds_buf->seg_size, ds_buf->seg_name)
    );

    if (0 != munmap((void *)ds_buf->seg_base_addr, ds_buf->seg_size)) {
        int err = errno;
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-mmap.txt", "sys call fail", 1, hn,
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
         "%s: %s: unlinking"
         "(id: %d, size: %lu, name: %s)\n",
         mca_shmem_mmap_component.super.base_version.mca_type_name,
         mca_shmem_mmap_component.super.base_version.mca_component_name,
         ds_buf->seg_id, (unsigned long)ds_buf->seg_size, ds_buf->seg_name)
    );

    if (-1 == unlink(ds_buf->seg_name)) {
        int err = errno;
        char hn[OPAL_MAXHOSTNAMELEN];
        gethostname(hn, sizeof(hn));
        opal_show_help("help-opal-shmem-mmap.txt", "sys call fail", 1, hn,
                       "unlink(2)", ds_buf->seg_name, strerror(err), err);
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

