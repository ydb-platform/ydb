/*
 * Copyright (c) 2004-2009 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2010-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#define OMPI_AFFINITY_STRING_MAX 1024

typedef enum ompi_affinity_fmt {
    OMPI_AFFINITY_RSRC_STRING_FMT,
    OMPI_AFFINITY_LAYOUT_FMT
} ompi_affinity_fmt_t;

OMPI_DECLSPEC int OMPI_Affinity_str(ompi_affinity_fmt_t fmt_type,
				    char ompi_bound[OMPI_AFFINITY_STRING_MAX],
                                    char current_binding[OMPI_AFFINITY_STRING_MAX],
                                    char exists[OMPI_AFFINITY_STRING_MAX]);
