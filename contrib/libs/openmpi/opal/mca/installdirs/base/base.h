/*
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2007-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Sandia National Laboratories. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#ifndef OPAL_INSTALLDIRS_BASE_H
#define OPAL_INSTALLDIRS_BASE_H

#include "opal_config.h"
#include "opal/mca/base/mca_base_framework.h"
#include "opal/mca/installdirs/installdirs.h"

/*
 * Global functions for MCA overall installdirs open and close
 */
BEGIN_C_DECLS

/**
 * Framework structure declaration
 */
OPAL_DECLSPEC extern mca_base_framework_t opal_installdirs_base_framework;

/* Just like opal_install_dirs_expand() (see installdirs.h), but will
   also insert the value of the environment variable $OPAL_DESTDIR, if
   it exists/is set.  This function should *only* be used during the
   setup routines of installdirs. */
char * opal_install_dirs_expand_setup(const char* input);

END_C_DECLS

#endif /* OPAL_BASE_INSTALLDIRS_H */
