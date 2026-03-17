/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal Includes prototypes for core dispatch functionality.
 *
 * @author Dennis Heimbigner
 */

#ifndef NC_DISPATCH_H
#define NC_DISPATCH_H

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#if defined(HDF5_PARALLEL) || defined(USE_PNETCDF)
#error #include <mpi.h>
#endif
#include "netcdf.h"
#include "ncmodel.h"
#include "nc.h"
#include "ncuri.h"
#ifdef USE_PARALLEL
#include "netcdf_par.h"
#endif
#include "netcdf_dispatch.h"

#define longtype ((sizeof(long) == sizeof(int) ? NC_INT : NC_INT64))

#define X_INT_MAX	2147483647

/* Given a filename, check its magic number */
/* Change magic number size from 4 to 8 to be more precise for HDF5 */
#define MAGIC_NUMBER_LEN ((unsigned long long)8)
#define MAGIC_HDF5_FILE 1
#define MAGIC_HDF4_FILE 2
#define MAGIC_CDF1_FILE 1 /* std classic format */
#define MAGIC_CDF2_FILE 2 /* classic 64 bit */

/* Define the mappings from fcn name types
   to corresponding NC types. */
#define T_text   NC_CHAR
#define T_schar  NC_BYTE
#define T_char   NC_CHAR
#define T_short  NC_SHORT
#define T_int    NC_INT
#define T_float  NC_FLOAT
#define T_double NC_DOUBLE
#define T_ubyte  NC_UBYTE
#define T_ushort NC_USHORT
#define T_uint   NC_UINT
#define T_longlong  NC_INT64
#define T_ulonglong  NC_UINT64
#define T_string NC_STRING

/* Synthetic type to handle special memtypes */
#define T_uchar  NC_UBYTE
#define T_long   longtype
#define T_ulong   ulongtype

/**************************************************/

/* Define a type for use when doing e.g. nc_get_vara_long, etc. */
/* Should matche values in libsrc4/netcdf.h */
#ifndef NC_UINT64
#define	NC_UBYTE 	7	/* unsigned 1 byte int */
#define	NC_USHORT 	8	/* unsigned 2-byte int */
#define	NC_UINT 	9	/* unsigned 4-byte int */
#define	NC_INT64 	10	/* signed 8-byte int */
#define	NC_UINT64 	11	/* unsigned 8-byte int */
#define	NC_STRING 	12	/* char* */
#endif

/* Define the range of Atomic types */
#define ATOMICTYPEMAX4 NC_STRING
#define ATOMICTYPEMAX3 NC_DOUBLE
#define ATOMICTYPEMAX5 NC_UINT64

#if !defined HDF5_PARALLEL && !defined USE_PNETCDF
typedef int MPI_Comm;
typedef int MPI_Info;
#define MPI_COMM_WORLD 0
#define MPI_INFO_NULL 0
#endif

/* Define a struct to hold the MPI info so it can be passed down the
 * call stack. This is used internally by the netCDF library. It
 * should not be used by netcdf users. */
typedef struct NC_MPI_INFO {
    MPI_Comm comm;
    MPI_Info info;
} NC_MPI_INFO;

/* Define known dispatch tables and initializers */

extern int NCDISPATCH_initialize(void);
extern int NCDISPATCH_finalize(void);

extern const NC_Dispatch* NC3_dispatch_table;
extern int NC3_initialize(void);
extern int NC3_finalize(void);

#ifdef NETCDF_ENABLE_DAP
extern const NC_Dispatch* NCD2_dispatch_table;
extern int NCD2_initialize(void);
extern int NCD2_finalize(void);
#endif
#ifdef NETCDF_ENABLE_DAP4
extern const NC_Dispatch* NCD4_dispatch_table;
extern int NCD4_initialize(void);
extern int NCD4_finalize(void);
#endif

#ifdef USE_PNETCDF
extern const NC_Dispatch* NCP_dispatch_table;
extern int NCP_initialize(void);
extern int NCP_finalize(void);
#endif

#ifdef USE_NETCDF4
extern int NC4_initialize(void);
extern int NC4_finalize(void);
#endif

#ifdef USE_HDF5
extern const NC_Dispatch* HDF5_dispatch_table;
extern int NC_HDF5_initialize(void);
extern int NC_HDF5_finalize(void);
#endif

#ifdef USE_HDF4
extern const NC_Dispatch* HDF4_dispatch_table;
extern int HDF4_initialize(void);
extern int HDF4_finalize(void);
#endif

#ifdef NETCDF_ENABLE_NCZARR
extern const NC_Dispatch* NCZ_dispatch_table;
extern int NCZ_initialize(void);
extern int NCZ_finalize(void);
#endif

/* User-defined formats.*/
extern NC_Dispatch* UDF_dispatch_tables[NC_MAX_UDF_FORMATS];
extern char UDF_magic_numbers[NC_MAX_UDF_FORMATS][NC_MAX_MAGIC_NUMBER_LEN + 1];

/* Prototypes. */
int NC_check_nulls(int ncid, int varid, const size_t *start, size_t **count,
                   ptrdiff_t **stride);

/**************************************************/
/* Forward */
#ifndef USE_NETCDF4
/* Taken from libsrc4/netcdf.h */
struct nc_vlen_t;
#define NC_NETCDF4 0x1000
#define NC_CLASSIC_MODEL 0x0100
#define NC_ENOPAR (-114)
#endif /*!USE_NETCDF4*/

struct NC;

int NC_create(const char *path, int cmode,
	      size_t initialsz, int basepe, size_t *chunksizehintp,
	      int useparallel, void *parameters, int *ncidp);
int NC_open(const char *path, int cmode,
	    int basepe, size_t *chunksizehintp,
	    int useparallel, void *parameters, int *ncidp);

/* Expose the default vars and varm dispatch entries */
EXTERNL int NCDEFAULT_get_vars(int, int, const size_t*,
	       const size_t*, const ptrdiff_t*, void*, nc_type);
EXTERNL int NCDEFAULT_put_vars(int, int, const size_t*,
	       const size_t*, const ptrdiff_t*, const void*, nc_type);
EXTERNL int NCDEFAULT_get_varm(int, int, const size_t*,
               const size_t*, const ptrdiff_t*, const ptrdiff_t*,
               void*, nc_type);
EXTERNL int NCDEFAULT_put_varm(int, int, const size_t*,
               const size_t*, const ptrdiff_t*, const ptrdiff_t*,
               const void*, nc_type);

/**************************************************/
/* Forward */
struct NCHDR;


/* Following functions must be handled as non-dispatch */
#ifdef NONDISPATCH
void (*nc_advise)(const char*cdf_routine_name,interr,const char*fmt,...);
void (*nc_set_log_level)(int);
const char* (*nc_inq_libvers)(void);
const char* (*nc_strerror)(int);
int (*nc_delete)(const char*path);
int (*nc_delete_mp)(const char*path,intbasepe);
int (*nc_initialize)();
int (*nc_finalize)();
#endif /*NONDISPATCH*/

/* Define the common fields for NC and NC_FILE_INFO_T etc */
typedef struct NCcommon {
	int ext_ncid; /* uid << 16 */
	int int_ncid; /* unspecified other id */
	const struct NC_Dispatch* dispatch;
	void* dispatchdata; /* per-protocol instance data */
	char* path; /* as specified at open or create */
} NCcommon;

EXTERNL size_t NC_atomictypelen(nc_type xtype);
EXTERNL char* NC_atomictypename(nc_type xtype);

/* Misc */

extern int NC_getshape(int ncid, int varid, int ndims, size_t* shape);
extern int NC_is_recvar(int ncid, int varid, size_t* nrecs);
extern int NC_inq_recvar(int ncid, int varid, int* nrecdims, int* is_recdim);

#define nullstring(s) (s==NULL?"(null)":s)

#undef TRACECALLS
#ifdef TRACECALLS
#include <stdio.h>
#define TRACE(fname) fprintf(stderr,"call: %s\n",#fname)
#else
#define TRACE(fname)
#endif

/* Vectors of ones and zeros */
extern size_t NC_coord_zero[NC_MAX_VAR_DIMS];
extern size_t NC_coord_one[NC_MAX_VAR_DIMS];
extern ptrdiff_t NC_stride_one[NC_MAX_VAR_DIMS];

extern int NC_initialized;

/**
Certain functions are in the dispatch table,
but not in the netcdf.h API. These need to
be exposed for use in delegation such as
in libdap2.
*/
EXTERNL int
NCDISPATCH_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
               int *ndimsp, int *dimidsp, int *nattsp,
               int *shufflep, int *deflatep, int *deflate_levelp,
               int *fletcher32p, int *contiguousp, size_t *chunksizesp,
               int *no_fill, void *fill_valuep, int *endiannessp,
	       unsigned int* idp, size_t* nparamsp, unsigned int* paramsp
               );
EXTERNL int
NCDISPATCH_get_att(int ncid, int varid, const char* name, void* value, nc_type t);

#endif /* NC_DISPATCH_H */
