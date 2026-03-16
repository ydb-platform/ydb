/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 *
 * Copyright (c) 2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#if HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */

#include "opal/util/opal_environ.h"
#include "opal/util/output.h"
#include "opal/util/argv.h"
#include "opal/util/opal_environ.h"

#include "opal/constants.h"
#include "opal/util/basename.h"

#include "opal/mca/compress/compress.h"
#include "opal/mca/compress/base/base.h"
#include "opal/runtime/opal_cr.h"

#include "compress_gzip.h"

static bool is_directory(char *fname );

int opal_compress_gzip_module_init(void)
{
    return OPAL_SUCCESS;
}

int opal_compress_gzip_module_finalize(void)
{
    return OPAL_SUCCESS;
}

int opal_compress_gzip_compress(char * fname, char **cname, char **postfix)
{
    pid_t child_pid = 0;
    int status = 0;

    opal_output_verbose(10, mca_compress_gzip_component.super.output_handle,
                        "compress:gzip: compress(%s)",
                        fname);

    opal_compress_gzip_compress_nb(fname, cname, postfix, &child_pid);
    waitpid(child_pid, &status, 0);

    if( WIFEXITED(status) ) {
        return OPAL_SUCCESS;
    } else {
        return OPAL_ERROR;
    }
}

int opal_compress_gzip_compress_nb(char * fname, char **cname, char **postfix, pid_t *child_pid)
{
    char **argv = NULL;
    char * base_fname = NULL;
    char * dir_fname = NULL;
    int status;
    bool is_dir;

    is_dir = is_directory(fname);

    *child_pid = fork();
    if( *child_pid == 0 ) { /* Child */
        char * cmd = NULL;

        dir_fname  = opal_dirname(fname);
        base_fname = opal_basename(fname);

        chdir(dir_fname);

        if( is_dir ) {
#if 0
            opal_compress_base_tar_create(&base_fname);
            asprintf(cname, "%s.gz", base_fname);
            asprintf(&cmd, "gzip %s", base_fname);
#else
            asprintf(cname, "%s.tar.gz", base_fname);
            asprintf(&cmd, "tar -zcf %s %s", *cname, base_fname);
#endif
        } else {
            asprintf(cname, "%s.gz", base_fname);
            asprintf(&cmd, "gzip %s", base_fname);
        }

        opal_output_verbose(10, mca_compress_gzip_component.super.output_handle,
                            "compress:gzip: compress_nb(%s -> [%s])",
                            fname, *cname);
        opal_output_verbose(10, mca_compress_gzip_component.super.output_handle,
                            "compress:gzip: compress_nb() command [%s]",
                            cmd);

        argv = opal_argv_split(cmd, ' ');
        status = execvp(argv[0], argv);

        opal_output(0, "compress:gzip: compress_nb: Failed to exec child [%s] status = %d\n", cmd, status);
        exit(OPAL_ERROR);
    }
    else if( *child_pid > 0 ) {
        if( is_dir ) {
            *postfix = strdup(".tar.gz");
        } else {
            *postfix = strdup(".gz");
        }
        asprintf(cname, "%s%s", fname, *postfix);

    }
    else {
        return OPAL_ERROR;
    }

    return OPAL_SUCCESS;
}

int opal_compress_gzip_decompress(char * cname, char **fname)
{
    pid_t child_pid = 0;
    int status = 0;

    opal_output_verbose(10, mca_compress_gzip_component.super.output_handle,
                        "compress:gzip: decompress(%s)",
                        cname);

    opal_compress_gzip_decompress_nb(cname, fname, &child_pid);
    waitpid(child_pid, &status, 0);

    if( WIFEXITED(status) ) {
        return OPAL_SUCCESS;
    } else {
        return OPAL_ERROR;
    }
}

int opal_compress_gzip_decompress_nb(char * cname, char **fname, pid_t *child_pid)
{
    char **argv = NULL;
    char * dir_cname = NULL;
    pid_t loc_pid = 0;
    int status;
    bool is_tar = false;

    if( 0 == strncmp(&(cname[strlen(cname)-7]), ".tar.gz", strlen(".tar.gz")) ) {
        is_tar = true;
    }

    *fname = strdup(cname);
    if( is_tar ) {
        /* Strip off '.tar.gz' */
        (*fname)[strlen(cname)-7] = '\0';
    } else {
        /* Strip off '.gz' */
        (*fname)[strlen(cname)-3] = '\0';
    }

    opal_output_verbose(10, mca_compress_gzip_component.super.output_handle,
                        "compress:gzip: decompress_nb(%s -> [%s])",
                        cname, *fname);

    *child_pid = fork();
    if( *child_pid == 0 ) { /* Child */
        char * cmd;
        dir_cname  = opal_dirname(cname);

        chdir(dir_cname);

        /* Fork(gunzip) */
        loc_pid = fork();
        if( loc_pid == 0 ) { /* Child */
            asprintf(&cmd, "gunzip %s", cname);

            opal_output_verbose(10, mca_compress_gzip_component.super.output_handle,
                                "compress:gzip: decompress_nb() command [%s]",
                                cmd);

            argv = opal_argv_split(cmd, ' ');
            status = execvp(argv[0], argv);

            opal_output(0, "compress:gzip: decompress_nb: Failed to exec child [%s] status = %d\n", cmd, status);
            exit(OPAL_ERROR);
        }
        else if( loc_pid > 0 ) { /* Parent */
            waitpid(loc_pid, &status, 0);
            if( !WIFEXITED(status) ) {
                opal_output(0, "compress:gzip: decompress_nb: Failed to bunzip the file [%s] status = %d\n", cname, status);
                exit(OPAL_ERROR);
            }
        }
        else {
            exit(OPAL_ERROR);
        }

        /* tar_decompress */
        if( is_tar ) {
            /* Strip off '.gz' leaving just '.tar' */
            cname[strlen(cname)-3] = '\0';
            opal_compress_base_tar_extract(&cname);
        }

        /* Once this child is done, then directly exit */
        exit(OPAL_SUCCESS);
    }
    else if( *child_pid > 0 ) {
        ;
    }
    else {
        return OPAL_ERROR;
    }

    return OPAL_SUCCESS;
}

static bool is_directory(char *fname ) {
    struct stat file_status;
    int rc;

    if(0 != (rc = stat(fname, &file_status) ) ) {
        return false;
    }
    if(S_ISDIR(file_status.st_mode)) {
        return true;
    }

    return false;
}
