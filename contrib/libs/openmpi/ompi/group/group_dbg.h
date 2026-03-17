/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_GROUP_DBG_H
#define OMPI_GROUP_DBG_H

/*
 * This file contains definitions used by both OMPI and debugger plugins.
 * For more information on why we do this see the Notice to developers
 * comment at the top of the ompi_msgq_dll.c file.
 */

/* Some definitions for the flags */
#define OMPI_GROUP_ISFREED       0x00000001
#define OMPI_GROUP_INTRINSIC     0x00000002

#define OMPI_GROUP_DENSE     0x00000004
#define OMPI_GROUP_SPORADIC  0x00000008
#define OMPI_GROUP_STRIDED   0x00000010
#define OMPI_GROUP_BITMAP    0x00000020

#endif /* OMPI_GROUP_DBG_H */
