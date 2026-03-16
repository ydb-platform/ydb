/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2012 University of Houston. All rights reserved.
 * Copyright (c) 2010-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_INFO_SUPPORT_H)
#define OMPI_INFO_SUPPORT_H

#include "opal/class/opal_pointer_array.h"

OMPI_DECLSPEC void  ompi_info_register_types(opal_pointer_array_t *mca_types);

OMPI_DECLSPEC int ompi_info_register_framework_params(opal_pointer_array_t *component_map);

OMPI_DECLSPEC void  ompi_info_close_components(void);

OMPI_DECLSPEC void ompi_info_show_ompi_version(const char *scope);

#endif /* !defined(OMPI_INFO_SUPPORT_H) */
