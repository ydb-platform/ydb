/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2010-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _PMIX_SERVER_H_
#define _PMIX_SERVER_H_

#include "orte_config.h"

#include "opal/mca/pmix/pmix.h"

BEGIN_C_DECLS

ORTE_DECLSPEC int pmix_server_init(void);
ORTE_DECLSPEC void pmix_server_start(void);
ORTE_DECLSPEC void pmix_server_finalize(void);
ORTE_DECLSPEC void pmix_server_register_params(void);


ORTE_DECLSPEC int orte_pmix_server_register_nspace(orte_job_t *jdata, bool force);

END_C_DECLS

#endif /* PMIX_SERVER_H_ */
