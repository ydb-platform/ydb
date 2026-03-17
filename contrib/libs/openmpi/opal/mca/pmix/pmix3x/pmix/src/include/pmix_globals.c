/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 * Copyright (c) 2014-2019 Intel, Inc.  All rights reserved.
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2014-2015 Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/* THIS FILE IS INCLUDED SOLELY TO INSTANTIATE AND INIT/FINALIZE THE GLOBAL CLASSES */

#include <src/include/pmix_config.h>

#include <src/include/types.h>
#include <src/include/pmix_stdint.h>
#include <src/include/pmix_socket_errno.h>

#include "src/include/pmix_globals.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include <fcntl.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <ctype.h>
#include PMIX_EVENT_HEADER
#if HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif /* HAVE_SYS_STAT_H */
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif  /* HAVE_DIRENT_H */

#include <pmix_common.h>

#include "src/mca/bfrops/bfrops_types.h"
#include "src/class/pmix_hash_table.h"
#include "src/class/pmix_list.h"
#include "src/threads/threads.h"
#include "src/util/argv.h"
#include "src/util/os_path.h"

static void dirpath_destroy(char *path, pmix_cleanup_dir_t *cd,
                            pmix_epilog_t *epi);
static bool dirpath_is_empty(const char *path);

PMIX_EXPORT pmix_lock_t pmix_global_lock = {
    .mutex = PMIX_MUTEX_STATIC_INIT,
    .cond = PMIX_CONDITION_STATIC_INIT,
    .active = false
};

PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_namelist_t,
                                pmix_list_item_t,
                                NULL, NULL);

static void cfcon(pmix_cleanup_file_t *p)
{
    p->path = NULL;
}
static void cfdes(pmix_cleanup_file_t *p)
{
    if (NULL != p->path) {
        free(p->path);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_cleanup_file_t,
                                pmix_list_item_t,
                                cfcon, cfdes);

static void cdcon(pmix_cleanup_dir_t *p)
{
    p->path = NULL;
    p->recurse = false;
    p->leave_topdir = false;
}
static void cddes(pmix_cleanup_dir_t *p)
{
    if (NULL != p->path) {
        free(p->path);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_cleanup_dir_t,
                                pmix_list_item_t,
                                cdcon, cddes);

static void nscon(pmix_namespace_t *p)
{
    p->nspace = NULL;
    p->nprocs = 0;
    p->nlocalprocs = 0;
    p->all_registered = false;
    p->version_stored = false;
    p->jobbkt = NULL;
    p->ndelivered = 0;
    p->nfinalized = 0;
    PMIX_CONSTRUCT(&p->ranks, pmix_list_t);
    memset(&p->compat, 0, sizeof(p->compat));
    PMIX_CONSTRUCT(&p->epilog.cleanup_dirs, pmix_list_t);
    PMIX_CONSTRUCT(&p->epilog.cleanup_files, pmix_list_t);
    PMIX_CONSTRUCT(&p->epilog.ignores, pmix_list_t);
    PMIX_CONSTRUCT(&p->setup_data, pmix_list_t);
}
static void nsdes(pmix_namespace_t *p)
{
    if (NULL != p->nspace) {
        free(p->nspace);
    }
    if (NULL != p->jobbkt) {
        PMIX_RELEASE(p->jobbkt);
    }
    PMIX_LIST_DESTRUCT(&p->ranks);
    /* perform any epilog */
    pmix_execute_epilog(&p->epilog);
    /* cleanup the epilog */
    PMIX_LIST_DESTRUCT(&p->epilog.cleanup_dirs);
    PMIX_LIST_DESTRUCT(&p->epilog.cleanup_files);
    PMIX_LIST_DESTRUCT(&p->epilog.ignores);
    PMIX_LIST_DESTRUCT(&p->setup_data);
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_namespace_t,
                                pmix_list_item_t,
                                nscon, nsdes);

static void ncdcon(pmix_nspace_caddy_t *p)
{
    p->ns = NULL;
}
static void ncddes(pmix_nspace_caddy_t *p)
{
    if (NULL != p->ns) {
        PMIX_RELEASE(p->ns);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_nspace_caddy_t,
                                pmix_list_item_t,
                                ncdcon, ncddes);

static void info_con(pmix_rank_info_t *info)
{
    info->peerid = -1;
    info->gid = info->uid = 0;
    info->pname.nspace = NULL;
    info->pname.rank = PMIX_RANK_UNDEF;
    info->modex_recvd = false;
    info->proc_cnt = 0;
    info->server_object = NULL;
}
static void info_des(pmix_rank_info_t *info)
{
    if (NULL != info->pname.nspace) {
        free(info->pname.nspace);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_rank_info_t,
                                pmix_list_item_t,
                                info_con, info_des);

static void pcon(pmix_peer_t *p)
{
    p->proc_type = PMIX_PROC_UNDEF;
    p->protocol = PMIX_PROTOCOL_UNDEF;
    p->finalized = false;
    p->info = NULL;
    p->proc_cnt = 0;
    p->index = 0;
    p->sd = -1;
    p->send_ev_active = false;
    p->recv_ev_active = false;
    PMIX_CONSTRUCT(&p->send_queue, pmix_list_t);
    p->send_msg = NULL;
    p->recv_msg = NULL;
    p->commit_cnt = 0;
    PMIX_CONSTRUCT(&p->epilog.cleanup_dirs, pmix_list_t);
    PMIX_CONSTRUCT(&p->epilog.cleanup_files, pmix_list_t);
    PMIX_CONSTRUCT(&p->epilog.ignores, pmix_list_t);

}

static void pdes(pmix_peer_t *p)
{
    if (0 <= p->sd) {
        CLOSE_THE_SOCKET(p->sd);
    }
    if (p->send_ev_active) {
        pmix_event_del(&p->send_event);
    }
    if (p->recv_ev_active) {
        pmix_event_del(&p->recv_event);
    }

    if (NULL != p->info) {
        PMIX_RELEASE(p->info);
    }

    PMIX_LIST_DESTRUCT(&p->send_queue);
    if (NULL != p->send_msg) {
        PMIX_RELEASE(p->send_msg);
    }
    if (NULL != p->recv_msg) {
        PMIX_RELEASE(p->recv_msg);
    }
    /* perform any epilog */
    pmix_execute_epilog(&p->epilog);
    /* cleanup the epilog */
    PMIX_LIST_DESTRUCT(&p->epilog.cleanup_dirs);
    PMIX_LIST_DESTRUCT(&p->epilog.cleanup_files);
    PMIX_LIST_DESTRUCT(&p->epilog.ignores);
    if (NULL != p->nptr) {
        PMIX_RELEASE(p->nptr);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_peer_t,
                                pmix_object_t,
                                pcon, pdes);

static void iofreqcon(pmix_iof_req_t *p)
{
    p->peer = NULL;
    memset(&p->pname, 0, sizeof(pmix_name_t));
    p->channels = PMIX_FWD_NO_CHANNELS;
    p->cbfunc = NULL;
}
static void iofreqdes(pmix_iof_req_t *p)
{
    if (NULL != p->peer) {
        PMIX_RELEASE(p->peer);
    }
    if (NULL != p->pname.nspace) {
        free(p->pname.nspace);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_iof_req_t,
                                pmix_list_item_t,
                                iofreqcon, iofreqdes);


static void scon(pmix_shift_caddy_t *p)
{
    PMIX_CONSTRUCT_LOCK(&p->lock);
    p->codes = NULL;
    p->ncodes = 0;
    p->pname.nspace = NULL;
    p->pname.rank = PMIX_RANK_UNDEF;
    p->data = NULL;
    p->ndata = 0;
    p->key = NULL;
    p->info = NULL;
    p->ninfo = 0;
    p->directives = NULL;
    p->ndirs = 0;
    p->evhdlr = NULL;
    p->iofreq = NULL;
    p->kv = NULL;
    p->vptr = NULL;
    p->cd = NULL;
    p->tracker = NULL;
    p->enviro = false;
    p->cbfunc.relfn = NULL;
    p->cbdata = NULL;
    p->ref = 0;
}
static void scdes(pmix_shift_caddy_t *p)
{
    PMIX_DESTRUCT_LOCK(&p->lock);
    if (NULL != p->pname.nspace) {
        free(p->pname.nspace);
    }
    if (NULL != p->kv) {
        PMIX_RELEASE(p->kv);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_shift_caddy_t,
                                pmix_object_t,
                                scon, scdes);

static void cbcon(pmix_cb_t *p)
{
    PMIX_CONSTRUCT_LOCK(&p->lock);
    p->checked = false;
    PMIX_CONSTRUCT(&p->data, pmix_buffer_t);
    p->cbfunc.ptlfn = NULL;
    p->cbdata = NULL;
    p->pname.nspace = NULL;
    p->pname.rank = PMIX_RANK_UNDEF;
    p->scope = PMIX_SCOPE_UNDEF;
    p->key = NULL;
    p->value = NULL;
    p->procs = NULL;
    p->nprocs = 0;
    p->info = NULL;
    p->ninfo = 0;
    p->nvals = 0;
    PMIX_CONSTRUCT(&p->kvs, pmix_list_t);
    p->copy = false;
    p->timer_running = false;
}
static void cbdes(pmix_cb_t *p)
{
    if (p->timer_running) {
        pmix_event_del(&p->ev);
    }
    if (NULL != p->pname.nspace) {
        free(p->pname.nspace);
    }
    PMIX_DESTRUCT(&p->data);
    PMIX_LIST_DESTRUCT(&p->kvs);
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_cb_t,
                                pmix_list_item_t,
                                cbcon, cbdes);

PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_info_caddy_t,
                                pmix_list_item_t,
                                NULL, NULL);

static void qcon(pmix_query_caddy_t *p)
{
    PMIX_CONSTRUCT_LOCK(&p->lock);
    p->queries = NULL;
    p->nqueries = 0;
    p->targets = NULL;
    p->ntargets = 0;
    p->info = NULL;
    p->ninfo = 0;
    PMIX_BYTE_OBJECT_CONSTRUCT(&p->bo);
    p->cbfunc = NULL;
    p->valcbfunc = NULL;
    p->cbdata = NULL;
    p->relcbfunc = NULL;
    p->credcbfunc = NULL;
    p->validcbfunc = NULL;
}
static void qdes(pmix_query_caddy_t *p)
{
    PMIX_DESTRUCT_LOCK(&p->lock);
    PMIX_BYTE_OBJECT_DESTRUCT(&p->bo);
    PMIX_PROC_FREE(p->targets, p->ntargets);
    PMIX_INFO_FREE(p->info, p->ninfo);
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_query_caddy_t,
                                pmix_object_t,
                                qcon, qdes);

void pmix_execute_epilog(pmix_epilog_t *epi)
{
    pmix_cleanup_file_t *cf, *cfnext;
    pmix_cleanup_dir_t *cd, *cdnext;
    struct stat statbuf;
    int rc;
    char **tmp;
    size_t n;

    /* start with any specified files */
    PMIX_LIST_FOREACH_SAFE(cf, cfnext, &epi->cleanup_files, pmix_cleanup_file_t) {
        /* check the effective uid/gid of the file and ensure it
         * matches that of the peer - we do this to provide at least
         * some minimum level of protection */
        tmp = pmix_argv_split(cf->path, ',');
        for (n=0; NULL != tmp[n]; n++) {
            rc = stat(tmp[n], &statbuf);
            if (0 != rc) {
                pmix_output_verbose(10, pmix_globals.debug_output,
                                    "File %s failed to stat: %d", tmp[n], rc);
                continue;
            }
            if (statbuf.st_uid != epi->uid ||
                statbuf.st_gid != epi->gid) {
                pmix_output_verbose(10, pmix_globals.debug_output,
                                    "File %s uid/gid doesn't match: uid %lu(%lu) gid %lu(%lu)",
                                    cf->path,
                                    (unsigned long)statbuf.st_uid, (unsigned long)epi->uid,
                                    (unsigned long)statbuf.st_gid, (unsigned long)epi->gid);
                continue;
            }
            rc = unlink(tmp[n]);
            if (0 != rc) {
                pmix_output_verbose(10, pmix_globals.debug_output,
                                    "File %s failed to unlink: %d", tmp[n], rc);
            }
        }
        pmix_argv_free(tmp);
        pmix_list_remove_item(&epi->cleanup_files, &cf->super);
        PMIX_RELEASE(cf);
    }

    /* now cleanup the directories */
    PMIX_LIST_FOREACH_SAFE(cd, cdnext, &epi->cleanup_dirs, pmix_cleanup_dir_t) {
        /* check the effective uid/gid of the file and ensure it
         * matches that of the peer - we do this to provide at least
         * some minimum level of protection */
        tmp = pmix_argv_split(cd->path, ',');
        for (n=0; NULL != tmp[n]; n++) {
            rc = stat(tmp[n], &statbuf);
            if (0 != rc) {
                pmix_output_verbose(10, pmix_globals.debug_output,
                                    "Directory %s failed to stat: %d", tmp[n], rc);
                continue;
            }
            if (statbuf.st_uid != epi->uid ||
                statbuf.st_gid != epi->gid) {
                pmix_output_verbose(10, pmix_globals.debug_output,
                                    "Directory %s uid/gid doesn't match: uid %lu(%lu) gid %lu(%lu)",
                                    cd->path,
                                    (unsigned long)statbuf.st_uid, (unsigned long)epi->uid,
                                    (unsigned long)statbuf.st_gid, (unsigned long)epi->gid);
                continue;
            }
            if ((statbuf.st_mode & S_IRWXU) == S_IRWXU) {
                dirpath_destroy(tmp[n], cd, epi);
            } else {
                pmix_output_verbose(10, pmix_globals.debug_output,
                                    "Directory %s lacks permissions", tmp[n]);
            }
        }
        pmix_argv_free(tmp);
        pmix_list_remove_item(&epi->cleanup_dirs, &cd->super);
        PMIX_RELEASE(cd);
    }
}

static void dirpath_destroy(char *path, pmix_cleanup_dir_t *cd, pmix_epilog_t *epi)
{
    int rc;
    bool is_dir = false;
    DIR *dp;
    struct dirent *ep;
    char *filenm;
    struct stat buf;
    pmix_cleanup_file_t *cf;

    if (NULL == path) {  /* protect against error */
        return;
    }

    /* if this path is it to be ignored, then do so */
    PMIX_LIST_FOREACH(cf, &epi->ignores, pmix_cleanup_file_t) {
        if (0 == strcmp(cf->path, path)) {
            return;
        }
    }

    /* Open up the directory */
    dp = opendir(path);
    if (NULL == dp) {
        return;
    }

    while (NULL != (ep = readdir(dp))) {
        /* skip:
         *  - . and ..
         */
        if ((0 == strcmp(ep->d_name, ".")) ||
            (0 == strcmp(ep->d_name, ".."))) {
            continue;
        }

        /* Create a pathname.  This is not always needed, but it makes
         * for cleaner code just to create it here.  Note that we are
         * allocating memory here, so we need to free it later on.
         */
        filenm = pmix_os_path(false, path, ep->d_name, NULL);

        /* if this path is to be ignored, then do so */
        PMIX_LIST_FOREACH(cf, &epi->ignores, pmix_cleanup_file_t) {
            if (0 == strcmp(cf->path, filenm)) {
                free(filenm);
                filenm = NULL;
                break;
            }
        }
        if (NULL == filenm) {
            continue;
        }

        /* Check to see if it is a directory */
        is_dir = false;

        rc = stat(filenm, &buf);
        if (0 > rc) {
            /* Handle a race condition. filenm might have been deleted by an
             * other process running on the same node. That typically occurs
             * when one task is removing the job_session_dir and an other task
             * is still removing its proc_session_dir.
             */
            free(filenm);
            continue;
        }
        /* if the uid/gid don't match, then leave it alone */
        if (buf.st_uid != epi->uid ||
            buf.st_gid != epi->gid) {
            free(filenm);
            continue;
        }

        if (S_ISDIR(buf.st_mode)) {
            is_dir = true;
        }

        /*
         * If not recursively decending, then if we find a directory then fail
         * since we were not told to remove it.
         */
        if (is_dir && !cd->recurse) {
            /* continue removing files */
            free(filenm);
            continue;
        }

        /* Directories are recursively destroyed */
        if (is_dir && cd->recurse && ((buf.st_mode & S_IRWXU) == S_IRWXU)) {
            dirpath_destroy(filenm, cd, epi);
            free(filenm);
        } else {
            /* Files are removed right here */
            unlink(filenm);
            free(filenm);
        }
    }

    /* Done with this directory */
    closedir(dp);

    /* If the directory is empty, then remove it unless we
     * were told to leave it */
    if (0 == strcmp(path, cd->path) && cd->leave_topdir) {
        return;
    }
    if (dirpath_is_empty(path)) {
        rmdir(path);
    }
}

static bool dirpath_is_empty(const char *path )
{
    DIR *dp;
    struct dirent *ep;

    if (NULL != path) {  /* protect against error */
        dp = opendir(path);
        if (NULL != dp) {
            while ((ep = readdir(dp))) {
                        if ((0 != strcmp(ep->d_name, ".")) &&
                            (0 != strcmp(ep->d_name, ".."))) {
                            closedir(dp);
                            return false;
                        }
            }
            closedir(dp);
            return true;
        }
        return false;
    }

    return true;
}
