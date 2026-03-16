
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
 * Copyright (c) 2011-2012 FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COLL_TUNED_DYNAMIC_RULES_H_HAS_BEEN_INCLUDED
#define MCA_COLL_TUNED_DYNAMIC_RULES_H_HAS_BEEN_INCLUDED

#include "ompi_config.h"

BEGIN_C_DECLS


typedef struct msg_rule_s {
    /* paranoid / debug */
    int mpi_comsize;  /* which MPI comm size this is is for */

    /* paranoid / debug */
    int alg_rule_id; /* unique alg rule id */
    int com_rule_id; /* unique com rule id */
    int msg_rule_id; /* unique msg rule id */

    /* RULE */
    size_t msg_size; /* message size */

    /* RESULT */
    int result_alg;              /* result algorithm to use */
    int result_topo_faninout;    /* result topology fan in/out to use (if applicable) */
    long result_segsize;         /* result segment size to use */
    int result_max_requests;     /* maximum number of outstanding requests (if applicable) */
} ompi_coll_msg_rule_t;


typedef struct com_rule_s {
    /* paranoid / debug */
    int mpi_comsize;  /* which MPI comm size this is is for */

    /* paranoid / debug */
    int alg_rule_id; /* unique alg rule id */
    int com_rule_id; /* unique com rule id */

    /* RULE */
    int n_msg_sizes;
    ompi_coll_msg_rule_t *msg_rules;

}  ompi_coll_com_rule_t;


typedef struct alg_rule_s {
    /* paranoid / debug */
    int alg_rule_id; /* unique alg rule id */

    /* RULE */
    int n_com_sizes;
    ompi_coll_com_rule_t *com_rules;

} ompi_coll_alg_rule_t;

/* function prototypes */

/* these are used to build the rule tables (by the read file routines) */
ompi_coll_alg_rule_t* ompi_coll_tuned_mk_alg_rules (int n_alg);
ompi_coll_com_rule_t* ompi_coll_tuned_mk_com_rules (int n_com_rules, int alg_rule_id);
ompi_coll_msg_rule_t* ompi_coll_tuned_mk_msg_rules (int n_msg_rules, int alg_rule_id, int com_rule_id, int mpi_comsize);

/* debugging support */
int ompi_coll_tuned_dump_msg_rule (ompi_coll_msg_rule_t* msg_p);
int ompi_coll_tuned_dump_com_rule (ompi_coll_com_rule_t* com_p);
int ompi_coll_tuned_dump_alg_rule (ompi_coll_alg_rule_t* alg_p);
int ompi_coll_tuned_dump_all_rules (ompi_coll_alg_rule_t* alg_p, int n_rules);

/* free alloced memory routines, used by file and tuned component/module */
int ompi_coll_tuned_free_msg_rules_in_com_rule (ompi_coll_com_rule_t* com_p);
int ompi_coll_tuned_free_coms_in_alg_rule (ompi_coll_alg_rule_t* alg_p);
int ompi_coll_tuned_free_all_rules (ompi_coll_alg_rule_t* alg_p, int n_algs);


/* the IMPORTANT routines, i.e. the ones that do stuff for everyday communicators and collective calls */

ompi_coll_com_rule_t* ompi_coll_tuned_get_com_rule_ptr (ompi_coll_alg_rule_t* rules, int alg_id, int mpi_comsize);

int ompi_coll_tuned_get_target_method_params (ompi_coll_com_rule_t* base_com_rule, size_t mpi_msgsize,
                                              int* result_topo_faninout, int* result_segsize,
                                              int* max_requests);


END_C_DECLS
#endif /* MCA_COLL_TUNED_DYNAMIC_RULES_H_HAS_BEEN_INCLUDED */

