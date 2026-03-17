/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * This interface is for regex support. This is a multi-select framework.
 *
 * Available plugins may be defined at runtime via the typical MCA parameter
 * syntax.
 */

#ifndef PMIX_PREG_H
#define PMIX_PREG_H

#include <src/include/pmix_config.h>

#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_var.h"
#include "src/mca/base/pmix_mca_base_framework.h"

#include "src/mca/preg/preg_types.h"

BEGIN_C_DECLS

/******    MODULE DEFINITION    ******/

#define PMIX_MAX_NODE_PREFIX        50

/* given a semicolon-separated list of input values, generate
 * a regex that can be passed down to a client for parsing.
 * The caller is responsible for free'ing the resulting
 * string
 *
 * If values have leading zero's, then that is preserved.
 * Example:
 *
 * Input: odin009;odin010;odin011;odin012;odin017;odin018;thor176
 *
 * Output:
 *     "foo:odin[009-012,017-018],thor176"
 *
 * Note that the "foo" at the beginning of the regex indicates
 * that the "foo" regex component is to be used to parse the
 * provided regex.
 */
typedef pmix_status_t (*pmix_preg_base_module_generate_node_regex_fn_t)(const char *input,
                                                                        char **regex);

/* The input is expected to consist of a comma-separated list
 * of ranges. Thus, an input of:
 *     "1-4;2-5;8,10,11,12;6,7,9"
 * would generate a regex of
 *     "[pmix:2x(3);8,10-12;6-7,9]"
 *
 * Note that the "pmix" at the beginning of each regex indicates
 * that the PMIx native parser is to be used by the client for
 * parsing the provided regex. Other parsers may be supported - see
 * the pmix_client.h header for a list.
 */
typedef pmix_status_t (*pmix_preg_base_module_generate_ppn_fn_t)(const char *input,
                                                                 char **ppn);


typedef pmix_status_t (*pmix_preg_base_module_parse_nodes_fn_t)(const char *regexp,
                                                                char ***names);

typedef pmix_status_t (*pmix_preg_base_module_parse_procs_fn_t)(const char *regexp,
                                                                char ***procs);

typedef pmix_status_t (*pmix_preg_base_module_resolve_peers_fn_t)(const char *nodename,
                                                                  const char *nspace,
                                                                  pmix_proc_t **procs, size_t *nprocs);

typedef pmix_status_t (*pmix_preg_base_module_resolve_nodes_fn_t)(const char *nspace,
                                                                  char **nodelist);

/**
 * Base structure for a PREG module
 */
typedef struct {
    char *name;
    pmix_preg_base_module_generate_node_regex_fn_t      generate_node_regex;
    pmix_preg_base_module_generate_ppn_fn_t             generate_ppn;
    pmix_preg_base_module_parse_nodes_fn_t              parse_nodes;
    pmix_preg_base_module_parse_procs_fn_t              parse_procs;
    pmix_preg_base_module_resolve_peers_fn_t            resolve_peers;
    pmix_preg_base_module_resolve_nodes_fn_t            resolve_nodes;
} pmix_preg_module_t;

/* we just use the standard component definition */

PMIX_EXPORT extern pmix_preg_module_t pmix_preg;

/*
 * Macro for use in components that are of type preg
 */
#define PMIX_PREG_BASE_VERSION_1_0_0 \
    PMIX_MCA_BASE_VERSION_1_0_0("preg", 1, 0, 0)

END_C_DECLS

#endif
