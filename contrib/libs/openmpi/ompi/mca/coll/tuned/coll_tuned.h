/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COLL_TUNED_EXPORT_H
#define MCA_COLL_TUNED_EXPORT_H

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "ompi/request/request.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "opal/util/output.h"

/* also need the dynamic rule structures */
#include "coll_tuned_dynamic_rules.h"

BEGIN_C_DECLS

/* these are the same across all modules and are loaded at component query time */
extern int   ompi_coll_tuned_stream;
extern int   ompi_coll_tuned_priority;
extern bool  ompi_coll_tuned_use_dynamic_rules;
extern char* ompi_coll_tuned_dynamic_rules_filename;
extern int   ompi_coll_tuned_init_tree_fanout;
extern int   ompi_coll_tuned_init_chain_fanout;
extern int   ompi_coll_tuned_init_max_requests;
extern int   ompi_coll_tuned_alltoall_small_msg;
extern int   ompi_coll_tuned_alltoall_intermediate_msg;

/* forced algorithm choices */
/* this structure is for storing the indexes to the forced algorithm mca params... */
/* we get these at component query (so that registered values appear in ompi_infoi) */
struct coll_tuned_force_algorithm_mca_param_indices_t {
    int  algorithm_param_index;      /* which algorithm you want to force */
    int  segsize_param_index;        /* segsize to use (if supported), 0 = no segmentation */
    int  tree_fanout_param_index;    /* tree fanout/in to use */
    int  chain_fanout_param_index;   /* K-chain fanout/in to use */
    int  max_requests_param_index;   /* Maximum number of outstanding send or recv requests */
};
typedef struct coll_tuned_force_algorithm_mca_param_indices_t coll_tuned_force_algorithm_mca_param_indices_t;


/* the following type is for storing actual value obtained from the MCA on each tuned module */
/* via their mca param indices lookup in the component */
/* this structure is stored once per collective type per communicator... */
struct coll_tuned_force_algorithm_params_t {
    int  algorithm;      /* which algorithm you want to force */
    int  segsize;        /* segsize to use (if supported), 0 = no segmentation */
    int  tree_fanout;    /* tree fanout/in to use */
    int  chain_fanout;   /* K-chain fanout/in to use */
    int  max_requests;   /* Maximum number of outstanding send or recv requests */
};
typedef struct coll_tuned_force_algorithm_params_t coll_tuned_force_algorithm_params_t;

/* the indices to the MCA params so that modules can look them up at open / comm create time  */
extern coll_tuned_force_algorithm_mca_param_indices_t ompi_coll_tuned_forced_params[COLLCOUNT];
/* the actual max algorithm values (readonly), loaded at component open */
extern int ompi_coll_tuned_forced_max_algorithms[COLLCOUNT];

/*
 * coll API functions
 */

/* API functions */

int ompi_coll_tuned_init_query(bool enable_progress_threads,
                               bool enable_mpi_threads);

mca_coll_base_module_t *
ompi_coll_tuned_comm_query(struct ompi_communicator_t *comm, int *priority);

/* API functions of decision functions and any implementations */

/*
 * Note this gets long as we have to have a prototype for each
 * MPI collective 4 times.. 2 for the comm type and 2 for each decision
 * type.
 * we might cut down the decision prototypes by conditional compiling
 */

/* All Gather */
int ompi_coll_tuned_allgather_intra_dec_fixed(ALLGATHER_ARGS);
int ompi_coll_tuned_allgather_intra_dec_dynamic(ALLGATHER_ARGS);
int ompi_coll_tuned_allgather_intra_do_this(ALLGATHER_ARGS, int algorithm, int faninout, int segsize);
int ompi_coll_tuned_allgather_intra_check_forced_init(coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* All GatherV */
int ompi_coll_tuned_allgatherv_intra_dec_fixed(ALLGATHERV_ARGS);
int ompi_coll_tuned_allgatherv_intra_dec_dynamic(ALLGATHERV_ARGS);
int ompi_coll_tuned_allgatherv_intra_do_this(ALLGATHERV_ARGS, int algorithm, int faninout, int segsize);
int ompi_coll_tuned_allgatherv_intra_check_forced_init(coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* All Reduce */
int ompi_coll_tuned_allreduce_intra_dec_fixed(ALLREDUCE_ARGS);
int ompi_coll_tuned_allreduce_intra_dec_dynamic(ALLREDUCE_ARGS);
int ompi_coll_tuned_allreduce_intra_do_this(ALLREDUCE_ARGS, int algorithm, int faninout, int segsize);
int ompi_coll_tuned_allreduce_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* AlltoAll */
int ompi_coll_tuned_alltoall_intra_dec_fixed(ALLTOALL_ARGS);
int ompi_coll_tuned_alltoall_intra_dec_dynamic(ALLTOALL_ARGS);
int ompi_coll_tuned_alltoall_intra_do_this(ALLTOALL_ARGS, int algorithm, int faninout, int segsize, int max_requests);
int ompi_coll_tuned_alltoall_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* AlltoAllV */
int ompi_coll_tuned_alltoallv_intra_dec_fixed(ALLTOALLV_ARGS);
int ompi_coll_tuned_alltoallv_intra_dec_dynamic(ALLTOALLV_ARGS);
int ompi_coll_tuned_alltoallv_intra_do_this(ALLTOALLV_ARGS, int algorithm);
int ompi_coll_tuned_alltoallv_intra_check_forced_init(coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* Barrier */
int ompi_coll_tuned_barrier_intra_dec_fixed(BARRIER_ARGS);
int ompi_coll_tuned_barrier_intra_dec_dynamic(BARRIER_ARGS);
int ompi_coll_tuned_barrier_intra_do_this(BARRIER_ARGS, int algorithm, int faninout, int segsize);
int ompi_coll_tuned_barrier_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* Bcast */
int ompi_coll_tuned_bcast_intra_dec_fixed(BCAST_ARGS);
int ompi_coll_tuned_bcast_intra_dec_dynamic(BCAST_ARGS);
int ompi_coll_tuned_bcast_intra_do_this(BCAST_ARGS, int algorithm, int faninout, int segsize);
int ompi_coll_tuned_bcast_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* Gather */
int ompi_coll_tuned_gather_intra_dec_fixed(GATHER_ARGS);
int ompi_coll_tuned_gather_intra_dec_dynamic(GATHER_ARGS);
int ompi_coll_tuned_gather_intra_do_this(GATHER_ARGS, int algorithm, int faninout, int segsize);
int ompi_coll_tuned_gather_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* Reduce */
int ompi_coll_tuned_reduce_intra_dec_fixed(REDUCE_ARGS);
int ompi_coll_tuned_reduce_intra_dec_dynamic(REDUCE_ARGS);
int ompi_coll_tuned_reduce_intra_do_this(REDUCE_ARGS, int algorithm, int faninout, int segsize, int max_oustanding_reqs);
int ompi_coll_tuned_reduce_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* Reduce_scatter */
int ompi_coll_tuned_reduce_scatter_intra_dec_fixed(REDUCESCATTER_ARGS);
int ompi_coll_tuned_reduce_scatter_intra_dec_dynamic(REDUCESCATTER_ARGS);
int ompi_coll_tuned_reduce_scatter_intra_do_this(REDUCESCATTER_ARGS, int algorithm, int faninout, int segsize);
int ompi_coll_tuned_reduce_scatter_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* Reduce_scatter_block */
int ompi_coll_tuned_reduce_scatter_block_intra_dec_fixed(REDUCESCATTERBLOCK_ARGS);
int ompi_coll_tuned_reduce_scatter_block_intra_dec_dynamic(REDUCESCATTERBLOCK_ARGS);
int ompi_coll_tuned_reduce_scatter_block_intra_do_this(REDUCESCATTERBLOCK_ARGS, int algorithm, int faninout, int segsize);
int ompi_coll_tuned_reduce_scatter_block_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* Scatter */
int ompi_coll_tuned_scatter_intra_dec_fixed(SCATTER_ARGS);
int ompi_coll_tuned_scatter_intra_dec_dynamic(SCATTER_ARGS);
int ompi_coll_tuned_scatter_intra_do_this(SCATTER_ARGS, int algorithm, int faninout, int segsize);
int ompi_coll_tuned_scatter_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* Exscan */
int ompi_coll_tuned_exscan_intra_dec_fixed(EXSCAN_ARGS);
int ompi_coll_tuned_exscan_intra_dec_dynamic(EXSCAN_ARGS);
int ompi_coll_tuned_exscan_intra_do_this(EXSCAN_ARGS, int algorithm);
int ompi_coll_tuned_exscan_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

/* Scan */
int ompi_coll_tuned_scan_intra_dec_fixed(SCAN_ARGS);
int ompi_coll_tuned_scan_intra_dec_dynamic(SCAN_ARGS);
int ompi_coll_tuned_scan_intra_do_this(SCAN_ARGS, int algorithm);
int ompi_coll_tuned_scan_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices);

int mca_coll_tuned_ft_event(int state);

struct mca_coll_tuned_component_t {
	/** Base coll component */
	mca_coll_base_component_2_0_0_t super;

	/** MCA parameter: Priority of this component */
	int tuned_priority;

	/** global stuff that I need the component to store */

	/* MCA parameters first */

	/* cached decision table stuff (moved from MCW module) */
	ompi_coll_alg_rule_t *all_base_rules;
};
/**
 * Convenience typedef
 */
typedef struct mca_coll_tuned_component_t mca_coll_tuned_component_t;

/**
 * Global component instance
 */
OMPI_MODULE_DECLSPEC extern mca_coll_tuned_component_t mca_coll_tuned_component;

struct mca_coll_tuned_module_t {
    mca_coll_base_module_t super;

    /* for forced algorithms we store the information on the module */
    /* previously we only had one shared copy, ops, it really is per comm/module */
    coll_tuned_force_algorithm_params_t user_forced[COLLCOUNT];

    /* the communicator rules for each MPI collective for ONLY my comsize */
    ompi_coll_com_rule_t *com_rules[COLLCOUNT];
};
typedef struct mca_coll_tuned_module_t mca_coll_tuned_module_t;
OBJ_CLASS_DECLARATION(mca_coll_tuned_module_t);

#endif  /* MCA_COLL_TUNED_EXPORT_H */
