/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "opal/util/output.h"

#include <stdlib.h>
#include <string.h>

#include "opal/runtime/opal_progress.h"
#include "opal/mca/btl/base/base.h"
#include "opal/mca/pmix/pmix.h"

#include "ompi/runtime/ompi_cr.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/mca/bml/base/bml_base_btl.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/proc/proc.h"

#include "bml_r2.h"
#include "bml_r2_ft.h"

int mca_bml_r2_ft_event(int state)
{
#if OPAL_ENABLE_FT_CR == 1
    static bool first_continue_pass = false;
    ompi_proc_t** procs = NULL;
    size_t num_procs;
    size_t btl_idx;
    int ret, p;
    int loc_state;
    int param_type = -1;
    const char **btl_list;

    if(OPAL_CRS_CHECKPOINT == state) {
        /* Do nothing for now */
    }
    else if(OPAL_CRS_CONTINUE == state) {
        first_continue_pass = !first_continue_pass;

        /* Since nothing in Checkpoint, we are fine here (unless required by BTL) */
        if (opal_cr_continue_like_restart && !first_continue_pass) {
            procs = ompi_proc_all(&num_procs);
            if(NULL == procs) {
                return OMPI_ERR_OUT_OF_RESOURCE;
            }
        }
    }
    else if(OPAL_CRS_RESTART_PRE == state ) {
        /* Nothing here */
    }
    else if(OPAL_CRS_RESTART == state ) {
        procs = ompi_proc_all(&num_procs);
        if(NULL == procs) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
    }
    else if(OPAL_CRS_TERM == state ) {
        ;
    }
    else {
        ;
    }

    /* Never call the ft_event functions attached to the BTLs on the second
     * pass of RESTART since on the first pass they were unloaded and therefore
     * no longer exist.
     */
    if( OPAL_CRS_RESTART != state ) {
        if( OPAL_CRS_CONTINUE == state && !first_continue_pass ) {
            ;
        } else {
            /* Since we only ever call into the BTLs once during the first restart
             * pass, just lie to them on this pass for a bit of local clarity.
             */
            if( OPAL_CRS_RESTART_PRE == state ) {
                loc_state = OPAL_CRS_RESTART;
            } else {
                loc_state = state;
            }

            /*
             * Call ft_event in:
             * - BTL modules
             * - MPool modules
             *
             * These should be cleaning out stale state, and memory references in
             * preparation for being shut down.
             */
            for(btl_idx = 0; btl_idx < mca_bml_r2.num_btl_modules; btl_idx++) {
                /*
                 * Notify Mpool
                 */
                if( NULL != (mca_bml_r2.btl_modules[btl_idx])->btl_mpool &&
                    NULL != (mca_bml_r2.btl_modules[btl_idx])->btl_mpool->mpool_ft_event ) {
                    opal_output_verbose(10, ompi_cr_output,
                                        "bml:r2: ft_event: Notify the %s MPool.\n",
                                        (mca_bml_r2.btl_modules[btl_idx])->btl_mpool->mpool_component->mpool_version.mca_component_name);
                    if(OMPI_SUCCESS != (ret = (mca_bml_r2.btl_modules[btl_idx])->btl_mpool->mpool_ft_event(loc_state) ) ) {
                        continue;
                    }
                }

                /*
                 * Notify BTL
                 */
                if( NULL != (mca_bml_r2.btl_modules[btl_idx])->btl_ft_event) {
                    opal_output_verbose(10, ompi_cr_output,
                                        "bml:r2: ft_event: Notify the %s BTL.\n",
                                        (mca_bml_r2.btl_modules[btl_idx])->btl_component->btl_version.mca_component_name);
                    if(OMPI_SUCCESS != (ret = (mca_bml_r2.btl_modules[btl_idx])->btl_ft_event(loc_state) ) ) {
                        continue;
                    }
                }
            }
        } /* OPAL_CRS_CONTINUE == state && !first_continue_pass */
    }

    if(OPAL_CRS_CHECKPOINT == state) {
        ;
    }
    else if(OPAL_CRS_CONTINUE == state) {
        /* Matches OPAL_CRS_RESTART_PRE */
        if (opal_cr_continue_like_restart && first_continue_pass) {
            if( OMPI_SUCCESS != (ret = mca_bml_r2_finalize()) ) {
                opal_output(0, "bml:r2: ft_event(Restart): Failed to finalize BML framework\n");
                return ret;
            }
            if( OMPI_SUCCESS != (ret = mca_base_framework_close(&opal_btl_base_framework)) ) {
                opal_output(0, "bml:r2: ft_event(Restart): Failed to close BTL framework\n");
                return ret;
            }
        }
        /* Matches OPAL_CRS_RESTART */
        else if (opal_cr_continue_like_restart && !first_continue_pass) {
            /*
             * Barrier to make all processes have been successfully restarted before
             * we try to remove some restart only files.
             */
            if( OMPI_SUCCESS != (ret = opal_pmix.fence(NULL, 0))) {
                opal_output(0, "bml:r2: ft_event(Restart): Failed to fence complete\n");
                return ret;
            }

            /*
             * Re-open the BTL framework to get the full list of components.
             */
            if( OMPI_SUCCESS != (ret = mca_base_framework_open(&opal_btl_base_framework, 0)) ) {
                opal_output(0, "bml:r2: ft_event(Restart): Failed to open BTL framework\n");
                return ret;
            }

            /*
             * Re-select the BTL components/modules
             * This will cause the BTL components to discover the available
             * network options on this machine, and post proper modex informaiton.
             */
            if( OMPI_SUCCESS != (ret = mca_btl_base_select(OPAL_ENABLE_PROGRESS_THREADS, 1) ) ) {
                opal_output(0, "bml:r2: ft_event(Restart): Failed to select in BTL framework\n");
                return ret;
            }

            /*
             * Clear some structures so we can properly repopulate them
             */
            mca_bml_r2.btls_added = false;

            for(p = 0; p < (int)num_procs; ++p) {
                if( NULL != procs[p]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML]) {
                    OBJ_RELEASE(procs[p]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML]);
                    procs[p]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML] = NULL;
                }

                OBJ_RELEASE(procs[p]);
            }

            if( NULL != procs ) {
                free(procs);
                procs = NULL;
            }
        }
    }
    else if(OPAL_CRS_RESTART_PRE == state ) {
        opal_output_verbose(10, ompi_cr_output,
                            "bml:r2: ft_event(Restart): Finalize BML\n");

        /*
         * Finalize the BML
         * - Flush progress functions
         * - Flush module references
         * - mca_btl_base_close()
         *   Need to do this because we may have BTL components that were
         *   unloaded in the first selection that may be available now.
         *   Conversely we may have BTL components loaded now that
         *   are not available now.
         */
        if( OMPI_SUCCESS != (ret = mca_bml_r2_finalize()) ) {
            opal_output(0, "bml:r2: ft_event(Restart): Failed to finalize BML framework\n");
            return ret;
        }
        if( OMPI_SUCCESS != (ret = mca_base_framework_close(&opal_btl_base_framework)) ) {
            opal_output(0, "bml:r2: ft_event(Restart): Failed to close BTL framework\n");
            return ret;
        }
    }
    else if(OPAL_CRS_RESTART == state  ) {

        /*
         * Barrier to make all processes have been successfully restarted before
         * we try to remove some restart only files.
         */
        if( OMPI_SUCCESS != (ret = opal_pmix.fence(NULL, 0))) {
            opal_output(0, "bml:r2: ft_event(Restart): Failed to fence complete\n");
            return ret;
        }

        /*
         * Re-open the BTL framework to get the full list of components.
         * - but first clear the MCA value that was there
         */
        param_type = mca_base_var_find("ompi", "btl", NULL, NULL);
        btl_list = NULL;
        mca_base_var_get_value(param_type, &btl_list, NULL, NULL);
        opal_output_verbose(11, ompi_cr_output,
                            "Restart (Previous BTL MCA): <%s>\n", btl_list ? btl_list[0] : "");

        if( OMPI_SUCCESS != (ret = mca_base_framework_open(&opal_btl_base_framework, 0)) ) {
            opal_output(0, "bml:r2: ft_event(Restart): Failed to open BTL framework\n");
            return ret;
        }

        /* The reregistered paramter is guaranteed to have the same index */
        btl_list = NULL;
        mca_base_var_get_value(param_type, &btl_list, NULL, NULL);
        opal_output_verbose(11, ompi_cr_output,
                            "Restart (New BTL MCA): <%s>\n", btl_list ? btl_list[0] : "");
        if( NULL != btl_list ) {
            free(btl_list);
            btl_list = NULL;
        }

        /*
         * Re-select the BTL components/modules
         * This will cause the BTL components to discover the available
         * network options on this machine, and post proper modex informaiton.
         */
        if( OMPI_SUCCESS != (ret = mca_btl_base_select(OPAL_ENABLE_PROGRESS_THREADS, 1) ) ) {
            opal_output(0, "bml:r2: ft_event(Restart): Failed to select in BTL framework\n");
            return ret;
        }

        /*
         * Clear some structures so we can properly repopulate them
         */
        mca_bml_r2.btls_added = false;

        for(p = 0; p < (int)num_procs; ++p) {
            if( NULL != procs[p]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML]) {
                OBJ_RELEASE(procs[p]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML]);
                procs[p]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML] = NULL;
            }

            OBJ_RELEASE(procs[p]);
        }

        if( NULL != procs ) {
            free(procs);
            procs = NULL;
        }
    }
    else if(OPAL_CRS_TERM == state ) {
        ;
    }
    else {
        ;
    }
#endif

    return OMPI_SUCCESS;
}
