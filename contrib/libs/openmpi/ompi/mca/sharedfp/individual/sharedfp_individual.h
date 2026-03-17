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
 * Copyright (c) 2013-2016 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_SHAREDFP_INDIVIDUAL_H
#define MCA_SHAREDFP_INDIVIDUAL_H

#include "ompi_config.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/common/ompio/common_ompio.h"


BEGIN_C_DECLS

int mca_sharedfp_individual_component_init_query(bool enable_progress_threads,
                                                 bool enable_mpi_threads);
struct mca_sharedfp_base_module_1_0_0_t *
        mca_sharedfp_individual_component_file_query (ompio_file_t *file, int *priority);
int mca_sharedfp_individual_component_file_unquery (ompio_file_t *file);

int mca_sharedfp_individual_module_init (ompio_file_t *file);
int mca_sharedfp_individual_module_finalize (ompio_file_t *file);

extern int mca_sharedfp_individual_priority;
extern int mca_sharedfp_individual_verbose;
extern int mca_sharedfp_individual_usage_counter;

OMPI_MODULE_DECLSPEC extern mca_sharedfp_base_component_2_0_0_t mca_sharedfp_individual_component;
/*
 * ******************************************************************
 * ********* functions which are implemented in this module *********
 * ******************************************************************
 */
/*IMPORANT: Update here when implementing functions from sharedfp API*/

int mca_sharedfp_individual_get_position(ompio_file_t *fh,
					 OMPI_MPI_OFFSET_TYPE * offset);
int mca_sharedfp_individual_seek (ompio_file_t *fh,
                                  OMPI_MPI_OFFSET_TYPE offset, int whence);
int mca_sharedfp_individual_file_open (struct ompi_communicator_t *comm,
                                       const char* filename,
                                       int amode,
                                       struct opal_info_t *info,
                                       ompio_file_t *fh);
int mca_sharedfp_individual_file_close (ompio_file_t *fh);
int mca_sharedfp_individual_read (ompio_file_t *fh,
                                  void *buf, int count, MPI_Datatype datatype, MPI_Status *status);
int mca_sharedfp_individual_read_ordered (ompio_file_t *fh,
                                          void *buf, int count, struct ompi_datatype_t *datatype,
                                          ompi_status_public_t *status);
int mca_sharedfp_individual_read_ordered_begin (ompio_file_t *fh,
                                                 void *buf,
                                                 int count,
                                                 struct ompi_datatype_t *datatype);
int mca_sharedfp_individual_read_ordered_end (ompio_file_t *fh,
                                               void *buf,
                                               ompi_status_public_t *status);
int mca_sharedfp_individual_iread (ompio_file_t *fh,
                                    void *buf,
                                    int count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_request_t **request);
int mca_sharedfp_individual_write (ompio_file_t *fh,
                                   const void *buf,
                                   int count,
                                   struct ompi_datatype_t *datatype,
                                   ompi_status_public_t *status);
int mca_sharedfp_individual_write_ordered (ompio_file_t *fh,
                                           const void *buf,
                                           int count,
                                           struct ompi_datatype_t *datatype,
                                           ompi_status_public_t *status);
int mca_sharedfp_individual_write_ordered_begin (ompio_file_t *fh,
                                                 const void *buf,
                                                 int count,
                                                 struct ompi_datatype_t *datatype);
int mca_sharedfp_individual_write_ordered_end (ompio_file_t *fh,
                                               const void *buf,
                                               ompi_status_public_t *status);
int mca_sharedfp_individual_iwrite (ompio_file_t *fh,
                                    const void *buf,
                                    int count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_request_t **request);

#define OMPI_FILE_WRITE_SHARED  3
#define MAX_METADATA_RECORDS 1024

typedef struct mca_sharedfp_individual_metadata_node_s {
    long recordid;					/* Denotes the type of function*/
    double timestamp;					/* Timestamp*/
    MPI_Offset localposition;				/* Offset of the data in the central data file*/
    long recordlength;					/* Number of bytes*/
    struct mca_sharedfp_individual_metadata_node_s* next;
} mca_sharedfp_individual_metadata_node;

struct mca_sharedfp_individual_record2 {
    long recordid;                                          /* Denotes the type of function */
    double timestamp;                                       /* Timestamp */
    MPI_Offset localposition;                               /* Offset of the data in the central data file */
    long recordlength;                                      /* Number of bytes*/
};

/*This structure will hang off of the mca_sharedfp_base_data_t's
 *selected_module_data attribute
 */
typedef struct mca_sharedfp_individual_header_record_s{
    int numofrecords;					/* Number of records in the linked list*/
    int numofrecordsonfile;				/* Number of records in the metadatafile*/
    MPI_Offset datafile_offset;
    MPI_Offset metadatafile_offset;
    ompio_file_t * datafilehandle;
    ompio_file_t * metadatafilehandle;
    char * datafilename;                /*for now need to delete this on file close*/
    char * metadatafilename;            /*for now need to delete this on file close*/
    MPI_Offset metafile_start_offset;
    MPI_Offset datafile_start_offset;
    struct mca_sharedfp_individual_metadata_node_s *next;
} mca_sharedfp_individual_header_record;


mca_sharedfp_individual_header_record* mca_sharedfp_individual_insert_headnode(void);

int mca_sharedfp_individual_collaborate_data(struct mca_sharedfp_base_data_t *sh, ompio_file_t *ompio_fh );
int mca_sharedfp_individual_get_timestamps_and_reclengths(double **buff, long **rec_length, MPI_Offset **offbuff,struct mca_sharedfp_base_data_t *sh);
int mca_sharedfp_individual_create_buff(double **ts,MPI_Offset **off,int totalnodes,int size);
int mca_sharedfp_individual_sort_timestamps(double **ts,MPI_Offset **off, int **ranks, int totalnodes);
MPI_Offset  mca_sharedfp_individual_assign_globaloffset(MPI_Offset **offsetbuff,int totalnodes,struct mca_sharedfp_base_data_t *sh);
int mca_sharedfp_individual_getoffset(double timestamp, double *ts, int *ranks, int myrank, int totalnodes);
/*int mca_sharedfp_individual_cleanup(double *ts, int* rnk, MPI_Offset *off);*/

int mca_sharedfp_individual_insert_metadata(int functype,long recordlength,struct mca_sharedfp_base_data_t *sh );
int mca_sharedfp_individual_write_metadata_file(struct mca_sharedfp_base_data_t *sh);
/*MPI_Datatype mca_sharedfp_individual_create_datatype();*/
/*int mca_sharedfp_individual_compute_highest_globalposition(MPI_Offset* global_off, int size);*/
/*MPI_Offset mca_sharedfp_individual_get_last_offset(struct mca_sharedfp_file_handle *sh);*/

double mca_sharedfp_individual_gettime(void);

/*
 * ******************************************************************
 * ************ functions implemented in this module end ************
 * ******************************************************************
 */

END_C_DECLS

#endif /* MCA_SHAREDFP_INDIVIDUAL_H */
