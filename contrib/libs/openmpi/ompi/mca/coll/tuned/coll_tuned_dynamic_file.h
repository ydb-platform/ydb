/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COLL_TUNED_DYNAMIC_FILE_H_HAS_BEEN_INCLUDED
#define MCA_COLL_TUNED_DYNAMIC_FILE_H_HAS_BEEN_INCLUDED

#include "ompi_config.h"

/* also need the dynamic rule structures */
#include "coll_tuned_dynamic_rules.h"


BEGIN_C_DECLS

int ompi_coll_tuned_read_rules_config_file (char *fname, ompi_coll_alg_rule_t** rules, int n_collectives);


END_C_DECLS
#endif /* MCA_COLL_TUNED_DYNAMIC_FILE_H_HAS_BEEN_INCLUDED */


