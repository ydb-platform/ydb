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
 * Copyright (c) 2013-2016 University of Houston. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include "sharedfp_individual.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"

#include <stdlib.h>
#include <stdio.h>
#include "mpi.h"

int mca_sharedfp_individual_insert_metadata(int functype,long recordlength,struct mca_sharedfp_base_data_t *sh )
{
    int ret = OMPI_SUCCESS;
    mca_sharedfp_individual_metadata_node *newnode = NULL;
    mca_sharedfp_individual_metadata_node *tempnode = NULL;
    mca_sharedfp_individual_header_record *headnode = NULL;

    headnode = (mca_sharedfp_individual_header_record*)sh->selected_module_data;
    if ( NULL == headnode)  {
	opal_output (0, "sharedfp_individual_insert_metadat: headnode is NULL but file is open\n");
	return OMPI_ERROR;
    }


    if ( mca_sharedfp_individual_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_individual_insert_metadata: Headnode->numofrecords = %d\n",
                    headnode->numofrecords);
    }
    /* Check if the maximum limit is reached for the records in the linked list*/
    if (headnode->numofrecords == MAX_METADATA_RECORDS) {
	/* Entire linked list is now deleted and a new file*/
	ret = mca_sharedfp_individual_write_metadata_file(sh);
	headnode->next = NULL;
    }

    /* Allocate a new Node */
    newnode = (mca_sharedfp_individual_metadata_node*)malloc(sizeof(mca_sharedfp_individual_metadata_node));
    if (NULL == newnode) {
	opal_output(0,"mca_sharedfp_individual_insert_metadata:Error while allocating new node\n");
	return OMPI_ERR_OUT_OF_RESOURCE;
    }


    /*numofrecords will always carry the number of records present in the metadata linked list*/
    headnode->numofrecords      = headnode->numofrecords + 1;

    newnode->recordid           = functype;
    newnode->timestamp          = mca_sharedfp_individual_gettime();
    newnode->localposition      = headnode->datafile_offset;	        /* Datafile offset*/
    newnode->recordlength       = recordlength;
    newnode->next               = NULL;


    if ( headnode->next == NULL) {
	/*headnode allocated but no further metadata node is allocated*/
        headnode->next = newnode;
    }
    else {
	/*We need to append the new node*/
        tempnode = headnode->next;

        while(tempnode->next) {
            tempnode = tempnode->next;
        }

        tempnode->next = newnode;
    }

    return ret;
}

int mca_sharedfp_individual_write_metadata_file(struct mca_sharedfp_base_data_t *sh)
{
    mca_sharedfp_individual_metadata_node *current = NULL;
    struct mca_sharedfp_individual_record2 buff;
    mca_sharedfp_individual_header_record *headnode = NULL;
    int ret=OMPI_SUCCESS;
    MPI_Status status;

    headnode = (mca_sharedfp_individual_header_record*)sh->selected_module_data;

    if (headnode->numofrecordsonfile == 0)
        headnode->metadatafile_offset = headnode->metafile_start_offset;

    current = headnode->next;
    while (current != NULL)  {
        /*Read from the linked list*/

        buff.recordid = current->recordid;
        buff.timestamp = current->timestamp;
        buff.localposition = current->localposition;
        buff.recordlength = current->recordlength;

	if ( mca_sharedfp_individual_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_individual_write_metadata_file: Buff recordid %ld\n",buff.recordid);
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_individual_write_metadata_file: Buff timestamp %f\n", buff.timestamp);
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_individual_write_metadata_file: Buff localposition %lld\n",buff.localposition);
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_individual_write_metadata_file: Buff recordlength %ld\n",buff.recordlength);
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_individual_write_metadata_file: Size of buff %ld\n",sizeof(buff));
        }

        headnode->next = current->next;
        free(current);
        current = (headnode)->next;

        /*Write to the metadata file*/

        ret = mca_common_ompio_file_write_at ( (headnode)->metadatafilehandle,
                                               (headnode)->metadatafile_offset,
                                               &buff,32, MPI_BYTE, &status);
	if ( OMPI_SUCCESS != ret ) {
	    goto exit;
	}
        (headnode)->numofrecordsonfile = headnode->numofrecordsonfile + 1;
        (headnode)->metadatafile_offset = (headnode)->metadatafile_offset + sizeof(buff);
    }

    /*All records are being read from the linked list and written to the file*/
    (headnode)->numofrecords = 0;

exit:
    return ret;
}
