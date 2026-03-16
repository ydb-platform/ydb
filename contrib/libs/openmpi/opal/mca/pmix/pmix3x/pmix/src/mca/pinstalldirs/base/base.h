/*
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2007-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#ifndef PMIX_PINSTALLDIRS_BASE_H
#define PMIX_PINSTALLDIRS_BASE_H

#include <src/include/pmix_config.h>
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/mca/pinstalldirs/pinstalldirs.h"

/*
 * Global functions for MCA overall pinstalldirs open and close
 */
BEGIN_C_DECLS

/**
 * Framework structure declaration
 */
PMIX_EXPORT extern pmix_mca_base_framework_t pmix_pinstalldirs_base_framework;

/* Just like pmix_pinstall_dirs_expand() (see pinstalldirs.h), but will
   also insert the value of the environment variable $PMIX_DESTDIR, if
   it exists/is set.  This function should *only* be used during the
   setup routines of pinstalldirs. */
char * pmix_pinstall_dirs_expand_setup(const char* input);

END_C_DECLS

#endif /* PMIX_BASE_PINSTALLDIRS_H */
