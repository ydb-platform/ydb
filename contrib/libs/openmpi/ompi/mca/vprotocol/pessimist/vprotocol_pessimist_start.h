/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef __VPROTOCOL_PESSIMIST_START_H__
#define __VPROTOCOL_PESSIMIST_START_H__

#include "ompi_config.h"
#include "vprotocol_pessimist.h"

BEGIN_C_DECLS

OMPI_DECLSPEC int mca_vprotocol_pessimist_start(size_t count, ompi_request_t **requests);

END_C_DECLS

#endif /* __VPROTOCOL_PESSIMIST_START_H__ */

