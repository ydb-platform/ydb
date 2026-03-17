/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * GZIP COMPRESS component
 *
 * Uses the gzip library
 */

#ifndef MCA_COMPRESS_GZIP_EXPORT_H
#define MCA_COMPRESS_GZIP_EXPORT_H

#include "opal_config.h"

#include "opal/util/output.h"

#include "opal/mca/mca.h"
#include "opal/mca/compress/compress.h"

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

    /*
     * Local Component structures
     */
    struct opal_compress_gzip_component_t {
        opal_compress_base_component_t super;  /** Base COMPRESS component */

    };
    typedef struct opal_compress_gzip_component_t opal_compress_gzip_component_t;
    OPAL_MODULE_DECLSPEC extern opal_compress_gzip_component_t mca_compress_gzip_component;

    int opal_compress_gzip_component_query(mca_base_module_t **module, int *priority);

    /*
     * Module functions
     */
    int opal_compress_gzip_module_init(void);
    int opal_compress_gzip_module_finalize(void);

    /*
     * Actual funcationality
     */
    int opal_compress_gzip_compress(char *fname, char **cname, char **postfix);
    int opal_compress_gzip_compress_nb(char *fname, char **cname, char **postfix, pid_t *child_pid);
    int opal_compress_gzip_decompress(char *cname, char **fname);
    int opal_compress_gzip_decompress_nb(char *cname, char **fname, pid_t *child_pid);

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif /* MCA_COMPRESS_GZIP_EXPORT_H */
