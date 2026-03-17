/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include <stdlib.h>
#include <stdio.h>

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "coll_tuned.h"

/* need to include our own topo prototypes so we can malloc data on the comm correctly */
#include "ompi/mca/coll/base/coll_base_topo.h"

/* also need the dynamic rule structures */
#include "coll_tuned_dynamic_rules.h"

/* and our own prototypes */
#include "coll_tuned_dynamic_file.h"


#define MYEOF   -999

static long getnext (FILE *fptr); /* local function */

static int fileline=0; /* used for verbose error messages */

/*
 * Reads a rule file called fname
 * Builds the algorithm rule table for a max of n_collectives
 *
 * If an error occurs it removes rule table and then exits with a very verbose
 * error message (this stops the user using a half baked rule table
 *
 * Returns the number of actual collectives that a rule exists for
 * (note 0 is NOT an error)
 *
 */

int ompi_coll_tuned_read_rules_config_file (char *fname, ompi_coll_alg_rule_t** rules, int n_collectives)
{
    FILE *fptr = (FILE*) NULL;
    int X, CI, NCS, CS, ALG, NMS, FANINOUT;
    long MS, SS;
    int x, ncs, nms;

    ompi_coll_alg_rule_t *alg_rules = (ompi_coll_alg_rule_t*) NULL;   /* complete table of rules */

    /* individual pointers to sections of rules */
    ompi_coll_alg_rule_t *alg_p = (ompi_coll_alg_rule_t*) NULL;
    ompi_coll_com_rule_t *com_p = (ompi_coll_com_rule_t*) NULL;
    ompi_coll_msg_rule_t *msg_p = (ompi_coll_msg_rule_t*) NULL;

    /* stats info */
    int total_alg_count = 0;
    int total_com_count = 0;
    int total_msg_count = 0;

    if (!fname) {
        OPAL_OUTPUT((ompi_coll_tuned_stream,"Gave NULL as rule table configuration file for tuned collectives... ignoring!\n"));
        return (-1);
    }

    if (!rules) {
        OPAL_OUTPUT((ompi_coll_tuned_stream,"Gave NULL as rule table result ptr!... ignoring!\n"));
        return (-2);
    }

    if (n_collectives<1) {
        OPAL_OUTPUT((ompi_coll_tuned_stream,"Gave %d as max number of collectives in the rule table configuration file for tuned collectives!... ignoring!\n", n_collectives));
        return (-3);
    }

    fptr = fopen (fname, "r");
    if (!fptr) {
        OPAL_OUTPUT((ompi_coll_tuned_stream,"cannot read rules file [%s]\n", fname));
        goto on_file_error;
    }

    /* make space and init the algorithm rules for each of the n_collectives MPI collectives */
    alg_rules = ompi_coll_tuned_mk_alg_rules (n_collectives);
    if (NULL == alg_rules) {
        OPAL_OUTPUT((ompi_coll_tuned_stream,"cannot cannot allocate rules for file [%s]\n", fname));
        goto on_file_error;
    }

    X = (int)getnext(fptr);
    if (X<0) {
        OPAL_OUTPUT((ompi_coll_tuned_stream,"Could not read number of collectives in configuration file around line %d\n", fileline));
        goto on_file_error;
    }
    if (X>n_collectives) {
        OPAL_OUTPUT((ompi_coll_tuned_stream,"Number of collectives in configuration file %d is greater than number of MPI collectives possible %d ??? error around line %d\n", X, n_collectives, fileline));
        goto on_file_error;
    }

    for (x=0;x<X;x++) { /* for each collective */

        CI = (int)getnext (fptr);
        if (CI<0) {
            OPAL_OUTPUT((ompi_coll_tuned_stream,"Could not read next Collective id in configuration file around line %d\n", fileline));
            goto on_file_error;
        }
        if (CI>=n_collectives) {
            OPAL_OUTPUT((ompi_coll_tuned_stream,"Collective id in configuration file %d is greater than MPI collectives possible %d. Error around line %d\n", CI, n_collectives, fileline));
            goto on_file_error;
        }

        if (alg_rules[CI].alg_rule_id != CI) {
            OPAL_OUTPUT((ompi_coll_tuned_stream, "Internal error in handling collective ID %d\n", CI));
            goto on_file_error;
        }
        OPAL_OUTPUT((ompi_coll_tuned_stream, "Reading dynamic rule for collective ID %d\n", CI));
        alg_p = &alg_rules[CI];

        alg_p->alg_rule_id = CI;
        alg_p->n_com_sizes = 0;
        alg_p->com_rules = (ompi_coll_com_rule_t *) NULL;

        NCS = (int)getnext (fptr);
        if (NCS<0) {
            OPAL_OUTPUT((ompi_coll_tuned_stream,"Could not read count of communicators for collective ID %d at around line %d\n", CI, fileline));
            goto on_file_error;
        }
        OPAL_OUTPUT((ompi_coll_tuned_stream, "Read communicator count %d for dynamic rule for collective ID %d\n", NCS, CI));
        alg_p->n_com_sizes = NCS;
        alg_p->com_rules = ompi_coll_tuned_mk_com_rules (NCS, CI);

        for (ncs=0;ncs<NCS;ncs++) {	/* for each comm size */

            com_p = &(alg_p->com_rules[ncs]);

            CS = (int)getnext (fptr);
            if (CS<0) {
                OPAL_OUTPUT((ompi_coll_tuned_stream,"Could not read communicator size for collective ID %d com rule %d at around line %d\n", CI, ncs, fileline));
                goto on_file_error;
            }

            com_p->mpi_comsize = CS;

            NMS = (int)getnext (fptr);
            if (NMS<0) {
                OPAL_OUTPUT((ompi_coll_tuned_stream,"Could not read number of message sizes for collective ID %d com rule %d at around line %d\n", CI, ncs, fileline));
                goto on_file_error;
            }
            OPAL_OUTPUT((ompi_coll_tuned_stream, "Read message count %d for dynamic rule for collective ID %d and comm size %d\n",
                         NMS, CI, CS));
            com_p->n_msg_sizes = NMS;
            com_p->msg_rules = ompi_coll_tuned_mk_msg_rules (NMS, CI, ncs, CS);

            msg_p = com_p->msg_rules;

            for (nms=0;nms<NMS;nms++) {	/* for each msg size */

                msg_p = &(com_p->msg_rules[nms]);

                MS = getnext (fptr);
                if (MS<0) {
                    OPAL_OUTPUT((ompi_coll_tuned_stream,"Could not read message size for collective ID %d com rule %d msg rule %d at around line %d\n", CI, ncs, nms, fileline));
                    goto on_file_error;
                }
                msg_p->msg_size = (size_t)MS;

                ALG = (int)getnext (fptr);
                if (ALG<0) {
                    OPAL_OUTPUT((ompi_coll_tuned_stream,"Could not read target algorithm method for collective ID %d com rule %d msg rule %d at around line %d\n", CI, ncs, nms, fileline));
                    goto on_file_error;
                }
                msg_p->result_alg = ALG;

                FANINOUT = (int)getnext (fptr);
                if (FANINOUT<0) {
                    OPAL_OUTPUT((ompi_coll_tuned_stream,"Could not read fan in/out topo for collective ID %d com rule %d msg rule %d at around line %d\n", CI, ncs, nms, fileline));
                    goto on_file_error;
                }
                msg_p->result_topo_faninout = FANINOUT;

                SS = getnext (fptr);
                if (SS<0) {
                    OPAL_OUTPUT((ompi_coll_tuned_stream,"Could not read target segment size for collective ID %d com rule %d msg rule %d at around line %d\n", CI, ncs, nms, fileline));
                    goto on_file_error;
                }
                msg_p->result_segsize = SS;

                if (!nms && MS) {
                    OPAL_OUTPUT((ompi_coll_tuned_stream,"All algorithms must specify a rule for message size of zero upwards always first!\n"));
                    OPAL_OUTPUT((ompi_coll_tuned_stream,"Message size was %lu for collective ID %d com rule %d msg rule %d at around line %d\n", MS, CI, ncs, nms, fileline));
                    goto on_file_error;
                }

                total_msg_count++;

            } /* msg size */

            total_com_count++;

        } /* comm size */

        total_alg_count++;
        OPAL_OUTPUT((ompi_coll_tuned_stream, "Done reading dynamic rule for collective ID %d\n", CI));

    } /* per collective */

    fclose (fptr);

    OPAL_OUTPUT((ompi_coll_tuned_stream,"\nConfigure file Stats\n"));
    OPAL_OUTPUT((ompi_coll_tuned_stream,"Collectives with rules\t\t\t: %5d\n", total_alg_count));
    OPAL_OUTPUT((ompi_coll_tuned_stream,"Communicator sizes with rules\t\t: %5d\n", total_com_count));
    OPAL_OUTPUT((ompi_coll_tuned_stream,"Message sizes with rules\t\t: %5d\n", total_msg_count));
    OPAL_OUTPUT((ompi_coll_tuned_stream,"Lines in configuration file read\t\t: %5d\n", fileline));

    /* return the rules to the caller */
    *rules = alg_rules;

    return (total_alg_count);


 on_file_error:

    /* here we close out the file and delete any memory allocated nicely */
    /* we return back a verbose message and a count of -1 algorithms read */
    /* draconian but its better than having a bad collective decision table */

    OPAL_OUTPUT((ompi_coll_tuned_stream,"read_rules_config_file: bad configure file [%s]. Read afar as line %d\n", fname, fileline));
    OPAL_OUTPUT((ompi_coll_tuned_stream,"Ignoring user supplied tuned collectives configuration decision file.\n"));
    OPAL_OUTPUT((ompi_coll_tuned_stream,"Switching back to [compiled in] fixed decision table.\n"));
    OPAL_OUTPUT((ompi_coll_tuned_stream,"Fix errors as listed above and try again.\n"));

    /* deallocate memory if allocated */
    if (alg_rules) ompi_coll_tuned_free_all_rules (alg_rules, n_collectives);

    /* close file */
    if (fptr) fclose (fptr);

    *rules = (ompi_coll_alg_rule_t*) NULL;
    return (-1);
}


static void skiptonewline (FILE *fptr)
{
    char val;
    int rc;

    do {
        rc = fread(&val, 1, 1, fptr);
        if (0 == rc) return;
        if ((1 == rc)&&('\n' == val)) {
            fileline++;
            return;
        }
    } while (1);
}

static long getnext (FILE *fptr)
{
    long val;
    int rc;
    char trash;

    do {
        rc = fscanf(fptr, "%li", &val);
        if (rc == EOF) return MYEOF;
        if (1 == rc) return val;
        /* in all other cases, skip to the end */
        rc = fread(&trash, 1, 1, fptr);
        if (rc == EOF) return MYEOF;
        if ('\n' == trash) fileline++;
        if ('#' == trash) skiptonewline (fptr);
    } while (1);
}
