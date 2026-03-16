/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2016 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COMMON_OMPIO_PRINT_QUEUE_H
#define MCA_COMMON_OMPIO_PRINT_QUEUE_H


#include "mpi.h"

#define MCA_COMMON_OMPIO_QUEUESIZE 2048

/*To extract time-information */
struct mca_common_ompio_print_entry{
    double time[3];
    int nprocs_for_coll;
    int aggregator;
};

typedef struct mca_common_ompio_print_entry mca_common_ompio_print_entry;

struct mca_common_ompio_print_queue {
    mca_common_ompio_print_entry entry[MCA_COMMON_OMPIO_QUEUESIZE + 1];
    int first;
    int last;
    int count;
};
typedef struct mca_common_ompio_print_queue mca_common_ompio_print_queue;


OMPI_DECLSPEC int mca_common_ompio_register_print_entry (struct mca_common_ompio_print_queue *q,
                                                         mca_common_ompio_print_entry x);

OMPI_DECLSPEC int mca_common_ompio_unregister_print_entry (struct mca_common_ompio_print_queue *q,
                                                           mca_common_ompio_print_entry *x);

OMPI_DECLSPEC int mca_common_ompio_empty_print_queue( struct mca_common_ompio_print_queue *q);

OMPI_DECLSPEC int mca_common_ompio_full_print_queue( struct mca_common_ompio_print_queue *q);

OMPI_DECLSPEC int mca_common_ompio_initialize_print_queue(struct mca_common_ompio_print_queue **q);

OMPI_DECLSPEC int mca_common_ompio_print_time_info( struct mca_common_ompio_print_queue *q,
                                                    char *name_operation, struct ompio_file_t *fh);


END_C_DECLS

#endif /* MCA_COMMON_OMPIO_PRINT_QUEUE_H */
