/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013-2015 University of Houston. All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics. Since linkers generally pull in symbols by object fules,
 * keeping these symbols as the only symbols in this file prevents
 * utility programs such as "ompi_info" from having to import entire
 * modules just to query their version and parameters
 */

#include "ompi_config.h"
#include "mpi.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"
#include "ompi/mca/sharedfp/individual/sharedfp_individual.h"

/*
 * *******************************************************************
 * ************************ actions structure ************************
 * *******************************************************************
 */
 /* IMPORTANT: Update here when adding sharedfp component interface functions*/
static mca_sharedfp_base_module_1_0_0_t individual =  {
    mca_sharedfp_individual_module_init, /* initalise after being selected */
    mca_sharedfp_individual_module_finalize, /* close a module on a communicator */
    mca_sharedfp_individual_seek,
    mca_sharedfp_individual_get_position,
    mca_sharedfp_individual_read,
    mca_sharedfp_individual_read_ordered,
    mca_sharedfp_individual_read_ordered_begin,
    mca_sharedfp_individual_read_ordered_end,
    mca_sharedfp_individual_iread,
    mca_sharedfp_individual_write,
    mca_sharedfp_individual_write_ordered,
    mca_sharedfp_individual_write_ordered_begin,
    mca_sharedfp_individual_write_ordered_end,
    mca_sharedfp_individual_iwrite,
    mca_sharedfp_individual_file_open,
    mca_sharedfp_individual_file_close
};
/*
 * *******************************************************************
 * ************************* structure ends **************************
 * *******************************************************************
 */

int mca_sharedfp_individual_component_init_query(bool enable_progress_threads,
                                            bool enable_mpi_threads)
{
    /* Nothing to do */

   return OMPI_SUCCESS;
}

struct mca_sharedfp_base_module_1_0_0_t * mca_sharedfp_individual_component_file_query (ompio_file_t *fh, int *priority) {

    int amode;
    bool wronly_flag=false;
    bool relaxed_order_flag=false;
    opal_info_t *info;
    int flag;
    int valuelen;
    char value[MPI_MAX_INFO_VAL+1];
    *priority = 0;

    /*test, and update priority*/
    /*---------------------------------------------------------*/
    /* 1. Is the file write only? check amode for MPI_MODE_WRONLY */
    amode = fh->f_amode;
    if ( amode & MPI_MODE_WRONLY || amode & MPI_MODE_RDWR ) {
        wronly_flag=true;
	if ( mca_sharedfp_individual_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "mca_sharedfp_individual_component_file_query: "
                        "MPI_MODE_WRONLY[true=%d,false=%d]=%d\n",true,false,wronly_flag);
	}
    } else {
        wronly_flag=false;
	if ( mca_sharedfp_individual_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
			"mca_sharedfp_individual_component_file_query: Can not run!, "
			"MPI_MODE_WRONLY[true=%d,false=%d]=%d\n",true,false,wronly_flag);
	}
    }

    /*---------------------------------------------------------*/
    /* 2. Did the user specify MPI_INFO relaxed ordering flag? */
    info = fh->f_info;
    if ( info != &(MPI_INFO_NULL->super) ){
        valuelen = MPI_MAX_INFO_VAL;
        opal_info_get ( info,"OMPIO_SHAREDFP_RELAXED_ORDERING", valuelen, value, &flag);
        if ( flag ) {
           if ( mca_sharedfp_individual_verbose ) {
                opal_output(ompi_sharedfp_base_framework.framework_output,
                        "mca_sharedfp_individual_component_file_query: "
                        "OMPIO_SHAREDFP_RELAXED_ORDERING=%s\n",value);
	    }
            /* flag - Returns true if key defined, false if not (boolean). */
            relaxed_order_flag=true;
        }
        else {
            if ( mca_sharedfp_individual_verbose ) {
               opal_output(ompi_sharedfp_base_framework.framework_output,
                        "mca_sharedfp_individual_component_file_query: "
                        "OMPIO_SHAREDFP_RELAXED_ORDERING MPI_Info key not set. "
                        "Set this key in order to increase this component's priority value.\n");
	    }
	}
    }
    else {
	if ( mca_sharedfp_individual_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                 "mca_sharedfp_individual_component_file_query: "
                 "OMPIO_SHAREDFP_RELAXED_ORDERING MPI_Info key not set, "
                 "got MPI_INFO_NULL. Set this key in order to increase "
                 "this component's priority value.\n");
	}
    }

    /*For now, this algorithm will not run if the file is not opened write only.
     *Setting the OMPIO_SHAREDFP_RELAXED_ORDERING gives this module a higher priority
     *otherwise it gets a priority of zero. This means that this module will
     *run only if no other module can run
     */
    if ( wronly_flag && relaxed_order_flag){
        *priority=mca_sharedfp_individual_priority;
    }
    else {
        *priority=1;
    }

    if ( wronly_flag ){
        return &individual;
    }

    return NULL;
}

int mca_sharedfp_individual_component_file_unquery (ompio_file_t *file)
{
   /* This function might be needed for some purposes later. for now it
    * does not have anything to do since there are no steps which need
    * to be undone if this module is not selected */

   return OMPI_SUCCESS;
}

int mca_sharedfp_individual_module_init (ompio_file_t *file)
{
    return OMPI_SUCCESS;
}


int mca_sharedfp_individual_module_finalize (ompio_file_t *file)
{
    return OMPI_SUCCESS;
}
