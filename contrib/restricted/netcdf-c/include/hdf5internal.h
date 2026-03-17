/* Copyright 2018-2022 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file This header file contains macros, types, and prototypes for
 * the HDF5 code in libhdf5. This header should not be included in
 * code outside libhdf5.
 *
 * @author Ed Hartnett
 */

#ifndef _HDF5INTERNAL_
#define _HDF5INTERNAL_

#include "config.h"
#include <hdf5.h>
#include <hdf5_hl.h>
#include "nc4internal.h"
#include "ncdimscale.h"
#include "nc4dispatch.h"
#include "hdf5dispatch.h"
#include "netcdf_filter.h"

#define NC_MAX_HDF5_NAME (NC_MAX_NAME + 10)

/* These have to do with creating chunked datasets in HDF5. */
#define NC_HDF5_UNLIMITED_DIMSIZE (0)
#define NC_HDF5_CHUNKSIZE_FACTOR (10)
#define NC_HDF5_MIN_CHUNK_SIZE (2)

#define NC_EMPTY_SCALE "NC_EMPTY_SCALE"

/* This is an attribute I had to add to handle multidimensional
 * coordinate variables. See nc4internal.h:NC_ATT_COORDINATES.
 * in nc4internal.h.
 */
#define COORDINATES NC_ATT_COORDINATES
#define COORDINATES_LEN (NC_MAX_NAME * 5)

/* This is used when the user defines a non-coordinate variable with
 * same name as a dimension. */
#define NON_COORD_PREPEND "_nc4_non_coord_"

/* An attribute in the HDF5 root group of this name means that the
 * file must follow strict netCDF classic format rules. */
#define NC3_STRICT_ATT_NAME NC_ATT_NC3_STRICT_NAME

/* An attribute in the HDF5 root group of this name provides
 * provenance information for Netcdf-4 files. */
#define NC_STRICT_ATT_NAME NC_ATT_NC3_STRICT_NAME

/* If this attribute is present on a dimscale variable, use the value
 * as the netCDF dimid. */
#define NC_DIMID_ATT_NAME NC_ATT_DIMID_NAME /*See nc4internal.h*/

/** This is the name of the class HDF5 dimension scale attribute. */
#define HDF5_DIMSCALE_CLASS_ATT_NAME NC_ATT_CLASS /*See nc4internal.h*/

/** This is the name of the name HDF5 dimension scale attribute. */
#define HDF5_DIMSCALE_NAME_ATT_NAME NC_ATT_NAME

/* forward */
struct NCauth;

/** Struct to hold HDF5-specific info for the file. */
typedef struct NC_HDF5_FILE_INFO {
   hid_t hdfid;
   unsigned transientid; /* counter for transient ids */
   NCURI* uri; /* Parse of the incoming path, if url */
#if defined(NETCDF_ENABLE_BYTERANGE)
   int byterange;
#endif
#ifdef NETCDF_ENABLE_S3
   struct NCauth* auth;
#endif
} NC_HDF5_FILE_INFO_T;

/* This is a struct to handle the dim metadata. */
typedef struct NC_HDF5_DIM_INFO
{
    hid_t hdf_dimscaleid;        /* Non-zero if a DIM_WITHOUT_VARIABLE dataset is in use (no coord var). */
    HDF5_OBJID_T hdf5_objid;
} NC_HDF5_DIM_INFO_T;

/** Strut to hold HDF5-specific info for attributes. */
typedef struct  NC_HDF5_ATT_INFO
{
    hid_t native_hdf_typeid;     /* Native HDF5 datatype for attribute's data */
} NC_HDF5_ATT_INFO_T;

/* Struct to hold HDF5-specific info for a group. */
typedef struct NC_HDF5_GRP_INFO
{
    hid_t hdf_grpid;
} NC_HDF5_GRP_INFO_T;

/* Struct to hold HDF5-specific info for a variable. */
typedef struct NC_HDF5_VAR_INFO
{
    hid_t hdf_datasetid;
    HDF5_OBJID_T *dimscale_hdf5_objids;
    nc_bool_t dimscale;          /**< True if var is a dimscale. */
    nc_bool_t *dimscale_attached;  /**< Array of flags that are true if dimscale is attached for that dim index. */
    int flags;
#       define NC_HDF5_VAR_FILTER_MISSING 1 /* if any filter is missing */
} NC_HDF5_VAR_INFO_T;

/* Struct to hold HDF5-specific info for a field. */
typedef struct NC_HDF5_FIELD_INFO
{
    hid_t hdf_typeid;
    hid_t native_hdf_typeid;
} NC_HDF5_FIELD_INFO_T;

/* Struct to hold HDF5-specific info for a type. */
typedef struct NC_HDF5_TYPE_INFO
{
    hid_t hdf_typeid;
    hid_t native_hdf_typeid;
} NC_HDF5_TYPE_INFO_T;

/* Logging and debugging. */
void reportopenobjects(int log, hid_t);
int hdf5_set_log_level();
void nc_log_hdf5(void);

/* These functions deal with HDF5 dimension scales. */
int rec_detach_scales(NC_GRP_INFO_T *grp, int dimid, hid_t dimscaleid);
int rec_reattach_scales(NC_GRP_INFO_T *grp, int dimid, hid_t dimscaleid);
int delete_dimscale_dataset(NC_GRP_INFO_T *grp, int dimid, NC_DIM_INFO_T *dim);

/* Write metadata. */
int nc4_rec_write_metadata(NC_GRP_INFO_T *grp, nc_bool_t bad_coord_order);
int nc4_rec_write_groups_types(NC_GRP_INFO_T *grp);

/* Adjust the cache. */
int nc4_adjust_var_cache(NC_GRP_INFO_T *grp, NC_VAR_INFO_T * var);

/* Open a HDF5 dataset. */
int nc4_open_var_grp2(NC_GRP_INFO_T *grp, int varid, hid_t *dataset);

/* Find types. */
NC_TYPE_INFO_T *nc4_rec_find_hdf_type(NC_FILE_INFO_T* h5,
                                      hid_t target_hdf_typeid);
int nc4_get_hdf_typeid(NC_FILE_INFO_T *h5, nc_type xtype,
                       hid_t *hdf_typeid, int endianness);

/* Enddef and closing files. */
int nc4_close_hdf5_file(NC_FILE_INFO_T *h5, int abort, NC_memio *memio);
int nc4_rec_grp_HDF5_del(NC_GRP_INFO_T *grp);
int nc4_enddef_netcdf4_file(NC_FILE_INFO_T *h5);
int nc4_HDF5_close_type(NC_TYPE_INFO_T* type);

/* Break & reform coordinate variables */
int nc4_break_coord_var(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *coord_var, NC_DIM_INFO_T *dim);
int nc4_reform_coord_var(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *coord_var, NC_DIM_INFO_T *dim);

/* In-memory functions */
extern hid_t NC4_image_init(NC_FILE_INFO_T* h5);
extern void NC4_image_finalize(void*);

/* Create HDF5 dataset for dim without a coord var. */
extern int nc4_create_dim_wo_var(NC_DIM_INFO_T *dim);

/* Give a var a secret HDF5 name, for use when there is a dim of this
 * name, but the var is not a coord var of that dim. */
extern int nc4_give_var_secret_name(NC_VAR_INFO_T *var);

/* Find file, group, var, and att info, doing lazy reads if needed. */
int nc4_hdf5_find_grp_var_att(int ncid, int varid, const char *name, int attnum,
                              int use_name, char *norm_name, NC_FILE_INFO_T **h5,
                              NC_GRP_INFO_T **grp, NC_VAR_INFO_T **var,
                              NC_ATT_INFO_T **att);

/* Find var, doing lazy var metadata read if needed. */
int nc4_hdf5_find_grp_h5_var(int ncid, int varid, NC_FILE_INFO_T **h5,
                             NC_GRP_INFO_T **grp, NC_VAR_INFO_T **var);

int nc4_HDF5_close_att(NC_ATT_INFO_T *att);

/* Perform lazy read of the rest of the metadata for a var. */
int nc4_get_var_meta(NC_VAR_INFO_T *var);

/* Get the file chunk cache settings from HDF5. */
int nc4_hdf5_get_chunk_cache(int ncid, size_t *sizep, size_t *nelemsp,
			     float *preemptionp);
/* Filter Dispatch Entries */
int NC4_hdf5_def_var_filter(int ncid, int varid, unsigned int filterid, size_t nparams, const unsigned int *params);
int NC4_hdf5_inq_var_filter_ids(int ncid, int varid, size_t* nfiltersp, unsigned int *filterids);
int NC4_hdf5_inq_var_filter_info(int ncid, int varid, unsigned int filterid, size_t* nparamsp, unsigned int *params);
int NC4_hdf5_inq_filter_avail(int ncid, unsigned id);

/* Filterlist management */

/* The NC_VAR_INFO_T->filters field is an NClist of this struct */
struct NC_HDF5_Filter {
    int flags;             /**< Flags describing state of this filter. */
#       define NC_HDF5_FILTER_MISSING 1 /* Filter implementation is not accessible */
    unsigned int filterid; /**< ID for arbitrary filter. */
    size_t nparams;        /**< nparams for arbitrary filter. */
    unsigned int* params;  /**< Params for arbitrary filter. */
};

int NC4_hdf5_filter_initialize(void);
int NC4_hdf5_filter_finalize(void);
int NC4_hdf5_filter_remove(NC_VAR_INFO_T* var, unsigned int id);
int NC4_hdf5_filter_lookup(NC_VAR_INFO_T* var, unsigned int id, struct NC_HDF5_Filter** fi);
int NC4_hdf5_addfilter(NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params, int flags);
int NC4_hdf5_filter_freelist(NC_VAR_INFO_T* var);
int NC4_hdf5_find_missing_filter(NC_VAR_INFO_T* var, unsigned int* idp);

/* Add an attribute to the attribute list. */
int nc4_put_att(NC_GRP_INFO_T* grp, int varid, const char *name, nc_type file_type,
		size_t len, const void *data, nc_type mem_type, int force);

/* Support functions for provenance info (defined in nc4hdf.c) */
extern int NC4_hdf5get_libversion(unsigned*,unsigned*,unsigned*);/*libsrc4/nc4hdf.c*/
extern int NC4_hdf5get_superblock(struct NC_FILE_INFO*, int*);/*libsrc4/nc4hdf.c*/
extern int NC4_isnetcdf4(struct NC_FILE_INFO*); /*libsrc4/nc4hdf.c*/

extern int nc4_find_default_chunksizes2(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var);

EXTERNL hid_t nc4_H5Fopen(const char *filename, unsigned flags, hid_t fapl_id);
EXTERNL hid_t nc4_H5Fcreate(const char *filename, unsigned flags, hid_t fcpl_id, hid_t fapl_id);

int hdf5set_format_compatibility(hid_t fapl_id);

/* HDF5 initialization/finalization */
extern int nc4_hdf5_initialized;
extern void nc4_hdf5_initialize(void);
extern void nc4_hdf5_finalize(void);

#endif /* _HDF5INTERNAL_ */
