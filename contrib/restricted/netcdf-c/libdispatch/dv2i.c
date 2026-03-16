/** \file 
The V2 API Functions.

Copyright 2018, University Corporation for Atmospheric Research
See \ref copyright file for copying and redistribution conditions.
 */

#ifndef NO_NETCDF_2

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include "netcdf.h"
#include <math.h>

/** \defgroup v2_api The Version 2 API

NetCDF's modern history began with the introduction of the V2 netCDF
API by Glenn Davis and Russ Rew in 1991. (The V1 API is lost to mists
of time.)

The V2 API is still fully supported, but should not be used for new
development.

All of the V2 functions have been reimplemented in terms of the V3 API
code; see the documentation for the related V3 functions to get more
documentation.

The V2 API is tested in test directory nctest.
*/

/** The subroutines in error.c emit no messages unless NC_VERBOSE bit
 * is on.  They call exit() when NC_FATAL bit is on. */
MSC_EXTRA int ncopts = (NC_FATAL | NC_VERBOSE) ;

MSC_EXTRA int ncerr = NC_NOERR ; /**< V2 API error code. */

#if SIZEOF_LONG == SIZEOF_SIZE_T
/*
 * We don't have to copy the arguments to switch from 'long'
 * to 'size_t' or 'ptrdiff_t'. Use dummy macros.
 */

# define NDIMS_DECL  /**< NDIMS declaration */

/** @internal Declaration. */
# define A_DECL(name, type, ndims, rhs) \
	const type *const name = ((const type *)(rhs))

# define A_FREE(name)  /**< Free a variable. */

# define A_INIT(lhs, type, ndims, rhs)  /**< Init a variable */
	
#else 
/*
 * We do have to copy the arguments to switch from 'long'
 * to 'size_t' or 'ptrdiff_t'. In my tests on an SGI,
 * any additional cost was lost in measurement variation.
 *
 * This stanza is true on Windows with MinGW-64
 */

# include "onstack.h"

static int
nvdims(int ncid, int varid)
{
   int ndims=-1, status;

   if ((status = nc_inq_varndims(ncid, varid, &ndims)))
   {
      nc_advise("ncvdims", status, "ncid %d", ncid);
      return -1;
   }
   return ndims;
}

/* Used to avoid errors on 64-bit windows related to 
   c89 macros and flow control/conditionals. */
static void* nvmalloc(off_t size) {
  if(size < 0)
    return NULL;
  
  return malloc(size);

}

#define NDIMS_DECL const int ndims = nvdims(ncid, varid); \
  
  
# define A_DECL(name, type, ndims, rhs)		\
  type *const name = (type*) nvmalloc((ndims) * sizeof(type))


#if 0
  ALLOC_ONSTACK(name, type, ndims)		
#endif

# define A_FREE(name) \
	FREE_ONSTACK(name)

# define A_INIT(lhs, type, ndims, rhs) \
	{ \
	  if((off_t)ndims >= 0) {     \
		const long *lp = rhs; \
		type *tp = lhs; \
		type *const end = lhs + ndims; \
		while(tp < end) \
		{ \
			*tp++ = (type) *lp++; \
		} \
		} \
	} \
	\
    if ((off_t)ndims < 0) {nc_advise("nvdims",NC_EMAXDIMS,"ndims %d",ndims); return -1;}


#endif

typedef signed char schar;  /**< Signed character type. */

/**
 * Computes number of record variables in an open netCDF file, and an array of
 * the record variable ids, if the array parameter is non-null.
 *
 * @param ncid File ID.
 * @param nrecvarsp Pointer that gets number of record variables.
 * @param recvarids Pointer that gets array of record variable IDs.
 *
 * @return ::NC_NOERR No error.
 * @return -1 on error.
 * @author Russ Rew
 */
static int
numrecvars(int ncid, int* nrecvarsp, int *recvarids)
{
    int status = NC_NOERR;
    int nvars = 0;
    int ndims = 0;
    int nrecvars = 0;
    int varid;
    int recdimid;
    int dimids[MAX_NC_DIMS];

    status = nc_inq_nvars(ncid, &nvars); 
    if(status != NC_NOERR)
	return status;

    status = nc_inq_unlimdim(ncid, &recdimid); 
    if(status != NC_NOERR)
	return status;

    if (recdimid == -1) {
	*nrecvarsp = 0;
	return NC_NOERR;
    }
    nrecvars = 0;
    for (varid = 0; varid < nvars; varid++) {
	status = nc_inq_varndims(ncid, varid, &ndims); 
	if(status != NC_NOERR)
	    return status;
	status = nc_inq_vardimid(ncid, varid, dimids); 
	if(status != NC_NOERR)
	    return status;
	if (ndims > 0 && dimids[0] == recdimid) {
	    if (recvarids != NULL)
	      recvarids[nrecvars] = varid;
	    nrecvars++;
	}
    }
    *nrecvarsp = nrecvars;
    return NC_NOERR;
}


/**
 * Computes record size (in bytes) of the record variable with a specified
 * variable id.  Returns size as 0 if not a record variable.
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param recsizep Pointer that gets record size.
 *
 * @return size, or 0 if not a record variable
 */
static int
ncrecsize(int ncid, int varid, size_t *recsizep)
{
    int status = NC_NOERR;
    int recdimid;
    nc_type type;
    int ndims;
    int dimids[MAX_NC_DIMS];
    int id;
    int size;

    *recsizep = 0;
    status = nc_inq_unlimdim(ncid, &recdimid); 
    if(status != NC_NOERR)
	return status;
    status = nc_inq_vartype(ncid, varid, &type); 
    if(status != NC_NOERR)
	return status;
    status = nc_inq_varndims(ncid, varid, &ndims); 
    if(status != NC_NOERR)
	return status;
    status = nc_inq_vardimid(ncid, varid, dimids); 
    if(status != NC_NOERR)
	return status;
    if (ndims == 0 || dimids[0] != recdimid) {
	return NC_NOERR;
    }
    size = nctypelen(type);
    for (id = 1; id < ndims; id++) {
	size_t len;
	status = nc_inq_dimlen(ncid, dimids[id], &len);
	if(status != NC_NOERR)
		return status;
	size *= (int)len;
    }
    *recsizep = (size_t)size;
    return NC_NOERR;
}


/**
 * Retrieves the dimension sizes of a variable with a specified variable id in
 * an open netCDF file.  
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param sizes Pointer that gets sizes.
 *
 * @return ::NC_NOERR No error.
 * @return -1 on error.
 * @author Russ Rew
 */
static int
dimsizes(int ncid, int varid, size_t *sizes)
{
    int status = NC_NOERR;
    int ndims;
    int id;
    int dimids[MAX_NC_DIMS];

    status = nc_inq_varndims(ncid, varid, &ndims); 
    if(status != NC_NOERR)
	return status;
    status = nc_inq_vardimid(ncid, varid, dimids); 
    if(status != NC_NOERR)
	return status;
    if (ndims == 0 || sizes == NULL)
      return NC_NOERR;
    for (id = 0; id < ndims; id++) {
	size_t len;
	status = nc_inq_dimlen(ncid, dimids[id], &len);
	if(status != NC_NOERR)
		return status;
	sizes[id] = len;
    }
    return NC_NOERR;
}

/** \ingroup v2_api

Retrieves the number of record variables, the record variable ids, and the
record size of each record variable.  If any pointer to info to be returned
is null, the associated information is not returned.  Returns -1 on error.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 functions nc_inq_nvars(),
nc_inq_unlimdim(), nc_inq_dim().

\param ncid file ID
\param nrecvarsp pointer that will get the number of record variables
in the file.
\param recvarids pointer to array that will get the variable IDs of
all variables that use the record dimension.
\param recsizes pointer to array that will dimension size of the
record dimension for each variable.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad ncid.
\returns ::NC_ENOTVAR Invalid variable ID.
\returns ::NC_EINVAL Invalid input
*/
int
nc_inq_rec(
	int ncid,
	size_t *nrecvarsp,
	int *recvarids,
	size_t *recsizes)
{
    int status = NC_NOERR;
    int nvars = 0;
    int recdimid;
    int varid;
    int rvarids[MAX_NC_VARS];
    int nrvars = 0;

    status = nc_inq_nvars(ncid, &nvars); 
    if(status != NC_NOERR)
	return status;

    status = nc_inq_unlimdim(ncid, &recdimid); 
    if(status != NC_NOERR)
	return status;

    if (recdimid == -1)
	return NC_NOERR;
    
    status = numrecvars(ncid, &nrvars, rvarids);
    if(status != NC_NOERR)
	return status;

    if (nrecvarsp != NULL)
	*nrecvarsp = (size_t)nrvars;

    if (recvarids != NULL)
	for (varid = 0; varid < nrvars; varid++)
	    recvarids[varid] = rvarids[varid];

    if (recsizes != NULL)
	for (varid = 0; varid < nrvars; varid++) {
	    size_t rsize;
	    status = ncrecsize(ncid, rvarids[varid], &rsize);
	    if (status != NC_NOERR)
		return status;
	    recsizes[varid] = rsize;
	}
    return NC_NOERR;
}

/** \ingroup v2_api

Write one record's worth of data, except don't write to variables for which
the address of the data to be written is NULL.  Return -1 on error.  This is
the same as the ncrecput() in the library, except that can handle errors
better.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_put_vara().

\param ncid file ID
\param recnum the record number to write.
\param datap pointer to one record's worth of data for all variables.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad ncid.
\returns ::NC_ENOTVAR Invalid variable ID.
\returns ::NC_EINVAL Invalid input
*/
int
nc_put_rec(
	int ncid,
	size_t recnum,
	void* const* datap)
{
    int status = NC_NOERR;
    int varid;
    int rvarids[MAX_NC_VARS];
    int nrvars;
    size_t start[MAX_NC_DIMS];
    size_t edges[MAX_NC_DIMS];

    status = numrecvars(ncid, &nrvars, rvarids);
    if(status != NC_NOERR)
	return status;

    if (nrvars == 0)
      return NC_NOERR;

    start[0] = recnum;
    for (varid = 1; varid < nrvars; varid++)
	start[varid] = 0;

    for (varid = 0; varid < nrvars; varid++) {
	if (datap[varid] != NULL) {
	    status = dimsizes(ncid, rvarids[varid], edges);
	    if(status != NC_NOERR)
		return status;

	    edges[0] = 1;		/* only 1 record's worth */
	    status = nc_put_vara(ncid, rvarids[varid], start, edges, datap[varid]);
	    if(status != NC_NOERR)
		return status;
	}
    }    
    return 0;
}


/** \ingroup v2_api

Read one record's worth of data, except don't read from variables for which
the address of the data to be read is null.  Return -1 on error.  This is
the same as the ncrecget() in the library, except that can handle errors
better.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_get_vara().

\param ncid file ID
\param recnum the record number to read.
\param datap pointer memory to hold one record's worth of data for all
variables.

\returns ::NC_NOERR No error.
\returns ::NC_EBADID Bad ncid.
\returns ::NC_ENOTVAR Invalid variable ID.
\returns ::NC_EINVAL Invalid input

*/
int
nc_get_rec(
	int ncid,
	size_t recnum,
	void **datap)
{
    int status = NC_NOERR;
    int varid;
    int rvarids[MAX_NC_VARS];
    int nrvars;
    size_t start[MAX_NC_DIMS];
    size_t edges[MAX_NC_DIMS];

    status = numrecvars(ncid, &nrvars, rvarids);
    if(status != NC_NOERR)
	return status;

    if (nrvars == 0)
      return NC_NOERR;

    start[0] = recnum;
    for (varid = 1; varid < nrvars; varid++)
	start[varid] = 0;

    for (varid = 0; varid < nrvars; varid++) {
	if (datap[varid] != NULL) {
	    status = dimsizes(ncid, rvarids[varid], edges);
	    if(status != NC_NOERR)
		return status;
	    edges[0] = 1;		/* only 1 record's worth */
	    status = nc_get_vara(ncid, rvarids[varid], start, edges, datap[varid]);
	    if(status != NC_NOERR)
		return status;
	}
    }    
    return 0;
}

/** \ingroup v2_api

Show an error message and exit (based on ncopts).

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_strerror()

\param routine_name
\param err error code
\param fmt pointer to a char array containing string format

*/
void
nc_advise(const char *routine_name, int err, const char *fmt,...)
{
	va_list args;

	if(NC_ISSYSERR(err))
		ncerr = NC_SYSERR;
	else
		ncerr = err;

	if( ncopts & NC_VERBOSE )
	{
		(void) fprintf(stderr,"%s: ", routine_name);
		va_start(args ,fmt);
		(void) vfprintf(stderr,fmt,args);
		va_end(args);
		if(err != NC_NOERR)
		{
			(void) fprintf(stderr,": %s",
				nc_strerror(err));
		}
		(void) fputc('\n',stderr);
		(void) fflush(stderr);	/* to ensure log files are current */
	}

	if( (ncopts & NC_FATAL) && err != NC_NOERR )
	{
		exit(ncopts);
	}
}

/* End error handling */

/** \ingroup v2_api

Create a netCDF file.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_create().

\param path path and filename of the file to be created.
\param cmode see nc_create() for full discussion of the create mode.

\returns the ncid of the created file.
*/
int
nccreate(const char* path, int cmode)
{
	int ncid;
	const int status = nc_create(path, cmode, &ncid);
	if(status != NC_NOERR)
	{
		nc_advise("nccreate", status, "filename \"%s\"", path);
		return -1;
	}
	return ncid;
}

/** \ingroup v2_api

Open a netCDF file.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_open().

\param path path and filename of the file to be created.
\param mode see nc_open() for full discussion of the open mode.

\returns the ncid of the created file.
*/
int
ncopen(const char *path, int mode)
{
	int ncid;
	const int status = nc_open(path, mode, &ncid);
	if(status != NC_NOERR)
	{
		nc_advise("ncopen", status, "filename \"%s\"", path);
		return -1;
	}
	return ncid;
}

/** \ingroup v2_api

Put file in define mode.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_redef().

\param ncid file ID

\returns 0 for success, -1 for failure.
*/
int
ncredef(int ncid)
{
	const int status =  nc_redef(ncid);
	if(status != NC_NOERR)
	{
		nc_advise("ncredef", status, "ncid %d", ncid);
		return -1;
	}
	return 0;
}

/** \ingroup v2_api

End define mode for file.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_enddef().

\param ncid file ID

\returns 0 for success, -1 for failure.
*/
int
ncendef(int ncid)
{
	const int status = nc_enddef(ncid);
	if(status != NC_NOERR)
	{
		nc_advise("ncendef", status, "ncid %d", ncid);
		return -1;
	}
	return 0;
}

/** \ingroup v2_api

Close a file.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_close().

\param ncid file ID

\returns 0 for success, -1 for failure.
*/
int
ncclose(int ncid)
{
	const int status = nc_close(ncid);
	if(status != NC_NOERR)
	{
		nc_advise("ncclose", status, "ncid %d", ncid);
		return -1;
		
	}
	return 0;
}

/** \ingroup v2_api

Learn about a file.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_inq().

\param ncid file ID
\param ndims pointer that will get number of dimensions.
\param nvars pointer that will get number of variables.
\param natts pointer that will get number of global attributes.
\param recdim pointer that will get dimension ID of record dimension,
or -1 if there is no record dimension.

\returns 0 for success, -1 for failure.
*/
int
ncinquire(
    int		ncid,
    int*	ndims,
    int*	nvars,
    int*	natts, 
    int*	recdim
)
{
	int nd, nv, na;
	const int status = nc_inq(ncid, &nd, &nv, &na, recdim);

	if(status != NC_NOERR)
	{
		nc_advise("ncinquire", status, "ncid %d", ncid);
		return -1;
	}
	/* else */

	if(ndims != NULL)
		*ndims = (int) nd;

	if(nvars != NULL)
		*nvars = (int) nv;

	if(natts != NULL)
		*natts = (int) na;

	return ncid;
}

/** \ingroup v2_api

Sync a file.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_sync().

\param ncid file ID

\returns 0 for success, -1 for failure.
*/
int
ncsync(int ncid)
{
	const int status = nc_sync(ncid);
	if(status != NC_NOERR)
	{
		nc_advise("ncsync", status, "ncid %d", ncid);
		return -1;
		
	}
	return 0;
}

/** \ingroup v2_api

Abort defining a file.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_abort().

\param ncid file ID
\returns 0 for success, -1 for failure.
*/
int
ncabort(int ncid)
{
	const int status = nc_abort(ncid);
	if(status != NC_NOERR)
	{
		nc_advise("ncabort", status, "ncid %d", ncid);
		return -1;
	}
	return 0;
}

/** \ingroup v2_api

Define a dimension.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_def_dim().

\param ncid file ID
\param name name of dimension.
\param length length of the dimension, NC_UNLIMITED for a record
dimension.

\returns dimid or -1 for failure.
*/
int
ncdimdef(
    int		ncid,
    const char*	name,
    long	length
)
{
	int dimid;
	int status = NC_NOERR;
	if(length < 0) {
	    status = NC_EDIMSIZE;
	    nc_advise("ncdimdef", status, "ncid %d", ncid);
	    return -1;
	}
	status =  nc_def_dim(ncid, name, (size_t)length, &dimid);
	if(status != NC_NOERR)
	{
		nc_advise("ncdimdef", status, "ncid %d", ncid);
		return -1;
	}
	return dimid;
}

/** \ingroup v2_api

Find dimension ID from name.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_inq_dimid().

\param ncid file ID
\param name name of dimension.

\returns dimid or -1 for failure.
*/
int
ncdimid(int ncid, const char*	name)
{
	int dimid;
	const int status =  nc_inq_dimid(ncid, name, &dimid);
	if(status != NC_NOERR)
	{
		nc_advise("ncdimid", status, "ncid %d", ncid);
		return -1;
	}
	return dimid;
}

/** \ingroup v2_api

Learn about a dimension.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_inq_dim().

\param ncid file ID
\param dimid the dimension ID to learn about
\param name pointer that will get name of dimension.
\param length pointer that will get length of dimension.

\returns dimid or -1 for failure.
*/
int
ncdiminq(
    int		ncid,
    int		dimid,
    char*	name,
    long*	length
)
{
	size_t ll;
	const int status = nc_inq_dim(ncid, dimid, name, &ll);

	if(status != NC_NOERR)
	{
		nc_advise("ncdiminq", status, "ncid %d", ncid);
		return -1;
	}
	/* else */
	
	if(length != NULL)
		*length = (int) ll;

	return dimid;
}

/** \ingroup v2_api

Rename a dimension.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_rename_dim().

\param ncid file ID
\param dimid the dimension ID.
\param name the new name.

\returns dimid or -1 for failure.
*/
int
ncdimrename(
    int		ncid,
    int		dimid,
    const char*	name
)
{
	const int status = nc_rename_dim(ncid, dimid, name);
	if(status != NC_NOERR)
	{
		nc_advise("ncdimrename", status, "ncid %d", ncid);
		return -1;
	}
	return dimid;
}

/** \ingroup v2_api

Define a variable.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_def_var().

\param ncid file ID
\param name the name of the variable.
\param datatype the data type of the variable.
\param ndims the number of dimensions.
\param dim array of dimension IDs.

\returns varid or -1 for failure.
*/
int
ncvardef(
    int		ncid,
    const char*	name,
    nc_type	datatype, 
    int		ndims,
    const int*	dim
)
{
	int varid = -1;
	const int status = nc_def_var(ncid, name, datatype, ndims, dim, &varid);
	if(status != NC_NOERR)
	{
		nc_advise("ncvardef", status, "ncid %d", ncid);
		return -1;
	}
	return varid;
}

/** \ingroup v2_api

Learn a variable ID from the name.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_inq_varid().

\param ncid file ID
\param name the name of the variable.

\returns varid or -1 for failure.
*/
int
ncvarid(
    int		ncid,
    const char*	name
)
{
	int varid = -1;
	const int status = nc_inq_varid(ncid, name, &varid);
	if(status != NC_NOERR)
	{
		nc_advise("ncvarid", status, "ncid %d", ncid);
		return -1;
	}
	return varid;
}

/** \ingroup v2_api

Learn about a variable.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_inq_var().

\param ncid file ID
\param varid the variable ID.
\param name pointer to array of char that will get name of variable.
\param datatype pointer that will get variable data type.
\param ndims pointer that will get number of dimensions.
\param dim pointer to array that will get dimension IDs.
\param natts pointer that will get number of variable attributes.

\returns varid or -1 for failure.
*/
int
ncvarinq(
    int		ncid,
    int		varid,
    char*	name,
    nc_type*	datatype,
    int*	ndims,
    int*	dim,
    int*	natts
)
{
	int nd, na;
	const int status = nc_inq_var(ncid, varid, name, datatype,
		 &nd, dim, &na);

	if(status != NC_NOERR)
	{
		nc_advise("ncvarinq", status, "ncid %d", ncid);
		return -1;
	}
	/* else */
	
	if(ndims != NULL)
		*ndims = (int) nd;

	if(natts != NULL)
		*natts = (int) na;

	return varid;
}

/** \ingroup v2_api

Write 1 data value.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_put_var1().

\param ncid file ID
\param varid the variable ID.
\param index pointer to array of index values.
\param value pointer to data.

\returns 0 for success or -1 for failure.
*/
int
ncvarput1(
    int		ncid,
    int		varid,
    const long*	index,
    const void*	value
)
{
	NDIMS_DECL
	A_DECL(coordp, size_t, (size_t)ndims, index);
	A_INIT(coordp, size_t, (size_t)ndims, index);
	{
	const int status = nc_put_var1(ncid, varid, coordp, value);
	A_FREE(coordp);
	if(status != NC_NOERR)
	{
		nc_advise("ncvarput1", status, "ncid %d", ncid);
		return -1;
	}
	}
	return 0;
}

/** \ingroup v2_api

Read 1 data value.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_get_var1().

\param ncid file ID
\param varid the variable ID.
\param index pointer to array of index values.
\param value pointer that will get data.

\returns 0 for success or -1 for failure.
*/
int
ncvarget1(
    int		ncid,
    int		varid,
    const long*	index,
    void*	value
)
{
	NDIMS_DECL
	A_DECL(coordp, size_t, ndims, index);
	A_INIT(coordp, size_t, ndims, index);
	{
	const int status = nc_get_var1(ncid, varid, coordp, value);
	A_FREE(coordp);
	if(status != NC_NOERR)
	{
		nc_advise("ncdimid", status, "ncid %d", ncid);
		return -1;
	}
	}
	return 0;
}

/** \ingroup v2_api

Write some data.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_put_vara().

\param ncid file ID
\param varid the variable ID.
\param start pointer to array of start values.
\param count pointer to array of count values.
\param value pointer to data.

\returns 0 for success or -1 for failure.
*/
int
ncvarput(
    int		ncid,
    int		varid,
    const long*	start,
    const long*	count, 
    const void*	value
)
{
	NDIMS_DECL
	A_DECL(stp, size_t, ndims, start);
	A_DECL(cntp, size_t, ndims, count);
	A_INIT(stp, size_t, ndims, start);
	A_INIT(cntp, size_t, ndims, count);
	{
	const int status = nc_put_vara(ncid, varid, stp, cntp, value);
	A_FREE(cntp);
	A_FREE(stp);
	if(status != NC_NOERR)
	{
		nc_advise("ncvarput", status, "ncid %d", ncid);
		return -1;
	}
	}
	return 0;
}

/** \ingroup v2_api

Read some data.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_get_vara().

\param ncid file ID
\param varid the variable ID.
\param start pointer to array of start values.
\param count pointer to array of count values.
\param value pointer to data.

\returns 0 for success or -1 for failure.
*/
int
ncvarget(
    int		ncid,
    int		varid,
    const long*	start,
    const long*	count, 
    void*	value
)
{
	NDIMS_DECL
	A_DECL(stp, size_t, ndims, start);
	A_DECL(cntp, size_t, ndims, count);
	A_INIT(stp, size_t, ndims, start);
	A_INIT(cntp, size_t, ndims, count);
	{
	const int status = nc_get_vara(ncid, varid, stp, cntp, value);
	A_FREE(cntp);
	A_FREE(stp);
	if(status != NC_NOERR)
	{
		nc_advise("ncvarget", status, "ncid %d; varid %d", ncid, varid);
		return -1;
	}
	}
	return 0;
}

/** \ingroup v2_api

Write strided data.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_put_vars().

\param ncid file ID
\param varid the variable ID.
\param start pointer to array of start values.
\param count pointer to array of count values.
\param stride pointer to array of stride values.
\param value pointer to data.

\returns 0 for success or -1 for failure.
*/
int
ncvarputs(
    int		ncid,
    int		varid,
    const long*	start,
    const long*	count,
    const long*	stride,
    const void*	value
)
{
	if(stride == NULL)
		return ncvarput(ncid, varid, start, count, value);
	/* else */
	{

	NDIMS_DECL 
	A_DECL(stp, size_t, ndims, start);
	A_DECL(cntp, size_t, ndims, count);
	A_DECL(strdp, ptrdiff_t, ndims, stride);
	A_INIT(stp, size_t, ndims, start);
	A_INIT(cntp, size_t, ndims, count);
	A_INIT(strdp, ptrdiff_t, ndims, stride);
	{
	const int status = nc_put_vars(ncid, varid, stp, cntp, strdp, value);
	A_FREE(strdp);
	A_FREE(cntp);
	A_FREE(stp);
	if(status != NC_NOERR)
	{
		nc_advise("ncvarputs", status, "ncid %d", ncid);
		return -1;
	}
	}
	return 0;
	}
}

/** \ingroup v2_api

Read strided data.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_get_vars().

\param ncid file ID
\param varid the variable ID.
\param start pointer to array of start values.
\param count pointer to array of count values.
\param stride pointer to array of stride values.
\param value pointer to data.

\returns 0 for success or -1 for failure.
*/
int
ncvargets(
    int		ncid,
    int		varid,
    const long*	start,
    const long*	count,
    const long*	stride,
    void*	value
)
{
	if(stride == NULL)
		return ncvarget(ncid, varid, start, count, value);
	/* else */
	{
	NDIMS_DECL
	A_DECL(stp, size_t, ndims, start);
	A_DECL(cntp, size_t, ndims, count);
	A_DECL(strdp, ptrdiff_t, ndims, stride);
	A_INIT(stp, size_t, ndims, start);
	A_INIT(cntp, size_t, ndims, count);
	A_INIT(strdp, ptrdiff_t, ndims, stride);
	{
	const int status = nc_get_vars(ncid, varid, stp, cntp, strdp, value);
	A_FREE(strdp);
	A_FREE(cntp);
	A_FREE(stp);
	if(status != NC_NOERR)
	{
		nc_advise("ncvargets", status, "ncid %d", ncid);
		return -1;
	}
	}
	return 0;
	}
}

/** \ingroup v2_api

Write mapped data.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_get_varm().

\param ncid file ID
\param varid the variable ID.
\param start pointer to array of start values.
\param count pointer to array of count values.
\param stride pointer to array of stride values.
\param map pointer to array of map values.
\param value pointer to data.

\returns 0 for success or -1 for failure.
*/
int
ncvarputg(
    int		ncid,
    int		varid,
    const long*	start,
    const long*	count,
    const long*	stride,
    const long*	map,
    const void* value
)
{
	int ndims = 0;
	if(map == NULL)
		return ncvarputs(ncid, varid, start, count, stride, value);
	/* else */
	{
	ptrdiff_t *imp=NULL;
	if (map != NULL) {
		int ret = NC_NOERR;
		/* make map[ndims-1] number of elements instead of bytes */
		int i, el_size;
		nc_type type;
		ret = nc_inq_varndims(ncid, varid, &ndims);
		if(ret) return ret;
		ret = nc_inq_vartype(ncid, varid, &type);
		if(ret) return ret;
				el_size = nctypelen(type);
		imp = (ptrdiff_t*) malloc((size_t)ndims * sizeof(ptrdiff_t));
		for (i=0; i<ndims; i++) imp[i] = map[i] / el_size;
	}

	{
	A_DECL(stp, size_t, ndims, start);
	A_DECL(cntp, size_t, ndims, count);
	A_DECL(strdp, ptrdiff_t, ndims, stride);
	A_INIT(stp, size_t, ndims, start);
	A_INIT(cntp, size_t, ndims, count);
	A_INIT(strdp, ptrdiff_t, ndims, stride);
	{
	const int status = nc_put_varm(ncid, varid,
			 stp, cntp, strdp, imp, value);
	if (imp!=NULL) free(imp);
	A_FREE(strdp);
	A_FREE(cntp);
	A_FREE(stp);
	if(status != NC_NOERR)
	{
		nc_advise("ncvarputg", status, "ncid %d", ncid);
		return -1;
	}
	}
	return 0;
	}
	}
}

/** \ingroup v2_api

Read mapped data.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_get_varm().

\param ncid file ID
\param varid the variable ID.
\param start pointer to array of start values.
\param count pointer to array of count values.
\param stride pointer to array of stride values.
\param map pointer to array of map values.
\param value pointer to data.

\returns 0 for success or -1 for failure.
*/
int
ncvargetg(
    int		ncid,
    int		varid,
    const long*	start,
    const long*	count,
    const long*	stride,
    const long*	map,
    void*	value
)
{
	int ndims = 0;
	if(map == NULL)
		return ncvargets(ncid, varid, start, count, stride, value);
	/* else */
	{
	ptrdiff_t *imp=NULL;
	if (map != NULL) {
		int ret = NC_NOERR;
		/* make map[ndims-1] number of elements instead of bytes */
		int i, el_size;
		nc_type type;
		ret = nc_inq_varndims(ncid, varid, &ndims);
		if(ret) return ret;
		ret = nc_inq_vartype(ncid, varid, &type);
		if(ret) return ret;
		el_size = nctypelen(type);
		imp = (ptrdiff_t*) malloc((size_t)ndims * sizeof(ptrdiff_t));
		for (i=0; i<ndims; i++) imp[i] = map[i] / el_size;
	}

	{
	A_DECL(stp, size_t, ndims, start);
	A_DECL(cntp, size_t, ndims, count);
	A_DECL(strdp, ptrdiff_t, ndims, stride);
	A_INIT(stp, size_t, ndims, start);
	A_INIT(cntp, size_t, ndims, count);
	A_INIT(strdp, ptrdiff_t, ndims, stride);
	{
	const int status = nc_get_varm(ncid, varid,
			stp, cntp, strdp, imp, value);
	if (imp!=NULL) free(imp);
	A_FREE(strdp);
	A_FREE(cntp);
	A_FREE(stp);
	if(status != NC_NOERR)
	{
		nc_advise("ncvargetg", status, "ncid %d", ncid);
		return -1;
	}
	}
	return 0;
	}
	}
}

/** \ingroup v2_api

Rename a variable.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_rename_var().

\param ncid file ID
\param varid the variable ID.
\param name the new name.

\returns varid or -1 for failure.
*/
int
ncvarrename(
    int		ncid,
    int		varid,
    const char*	name
)
{
	const int status = nc_rename_var(ncid, varid, name);
	if(status != NC_NOERR)
	{
		nc_advise("ncvarrename", status, "ncid %d", ncid);
		return -1;
	}
	return varid;
}

/** \ingroup v2_api

Write an attribute.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_put_att_int(), etc.

\param ncid file ID
\param varid the variable ID or NC_GLOBAL.
\param name the name of the attribute.
\param datatype the type of the attribute.
\param len the length of the attribute.
\param value the attribute value.

\returns dimid or -1 for failure.
*/
int
ncattput(
    int		ncid,
    int		varid,
    const char*	name, 
    nc_type	datatype,
    int		len,
    const void*	value
)
{
	const int status = nc_put_att(ncid, varid, name, datatype, (size_t)len, value);
	if(status != NC_NOERR)
	{
		nc_advise("ncattput", status, "ncid %d", ncid);
		return -1;
	}
	return 0;
}

/** \ingroup v2_api

Learn about an attribute.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_inq_att().

\param ncid file ID
\param varid the variable ID.
\param name the name of the attribute.
\param datatype pointer that will get data type.
\param len pointer that will get length.

\returns 1 for success or -1 for failure. (That's a delightful
artifact of a by-gone era of C programming, isn't it?)
*/
int
ncattinq(
    int		ncid,
    int		varid,
    const char*	name, 
    nc_type*	datatype,
    int*	len
)
{
	size_t ll;
	const int status = nc_inq_att(ncid, varid, name, datatype, &ll);
	if(status != NC_NOERR)
	{
		nc_advise("ncattinq", status,
		    "ncid %d; varid %d; attname \"%s\"",
		    ncid, varid, name);
		return -1;
	}
	
	if(len != NULL)
		*len = (int) ll;

	return 1;
}

/** \ingroup v2_api

Read an attribute.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_get_att_int(), etc.

\param ncid file ID.
\param varid the variable ID or NC_GLOBAL.
\param name the name of the attribute.
\param value pointer that will get the attribute data.

\returns 1 for success or -1 for failure.
*/
int
ncattget(
    int		ncid,
    int		varid,
    const char*	name, 
    void*	value
)
{
	const int status = nc_get_att(ncid, varid, name, value);
	if(status != NC_NOERR)
	{
		nc_advise("ncattget", status, "ncid %d", ncid);
		return -1;
	}
	return 1;
}

/** \ingroup v2_api

Copy an attribute.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_get_att_int(), etc.

\param ncid_in file ID to copy from.
\param varid_in the variable ID or NC_GLOBAL to copy from.
\param name the name of the attribute.
\param ncid_out file ID to copy to.
\param varid_out the variable ID or NC_GLOBAL to copy to.

\returns 0 for success or -1 for failure.
*/
int
ncattcopy(
    int		ncid_in,
    int		varid_in,
    const char*	name, 
    int		ncid_out,
    int		varid_out
)
{
	const int status = nc_copy_att(ncid_in, varid_in, name, ncid_out, varid_out);
	if(status != NC_NOERR)
	{
		nc_advise("ncattcopy", status, "%s", name);
		return -1;
	}
	return 0;
}

/** \ingroup v2_api

Learn attribute name from its number.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_inq_attname().

\param ncid file ID
\param varid the variable ID.
\param attnum the number of the attribute.
\param name the name of the attribute.

\returns attnum for success or -1 for failure.
*/
int
ncattname(
    int		ncid,
    int		varid,
    int		attnum,
    char*	name
)
{
	const int status = nc_inq_attname(ncid, varid, attnum, name);
	if(status != NC_NOERR)
	{
		nc_advise("ncattname", status, "ncid %d", ncid);
		return -1;
	}
	return attnum;
}

/** \ingroup v2_api

Rename an attribute.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_rename_att().

\param ncid file ID
\param varid the variable ID.
\param name the attribute name.
\param newname the new name.

\returns 1 for success or -1 for failure.
*/
int
ncattrename(
    int		ncid,
    int		varid,
    const char*	name, 
    const char*	newname
)
{
	const int status = nc_rename_att(ncid, varid, name, newname);
	if(status != NC_NOERR)
	{
		nc_advise("ncattrename", status, "ncid %d", ncid);
		return -1;
	}
	return 1;
}

/** \ingroup v2_api

Delete an attribute.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_delete_att().

\param ncid file ID
\param varid the variable ID.
\param name the attribute name.

\returns 1 for success or -1 for failure.
*/
int
ncattdel(
    int		ncid,
    int		varid,
    const char*	name
)
{
	 const int status = nc_del_att(ncid, varid, name);
	if(status != NC_NOERR)
	{
		nc_advise("ncattdel", status, "ncid %d", ncid);
		return -1;
	}
	return 1;
}

#endif /* NO_NETCDF_2 */

#ifndef NO_NETCDF_2

/** \ingroup v2_api

Set the fill mode.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_set_fill().

\param ncid file ID
\param fillmode NC_FILL or NC_NOFILL.

\returns oldmode for success or -1 for failure.
*/
int
ncsetfill(
    int		ncid,
    int		fillmode
)
{
	int oldmode = -1;
	const int status = nc_set_fill(ncid, fillmode, &oldmode);
	if(status != NC_NOERR)
	{
		nc_advise("ncsetfill", status, "ncid %d", ncid);
		return -1;
	}
	return oldmode;
}

/** \ingroup v2_api

Learn record variables and the lengths of the record dimension.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 functions nc_inq_var()/nc_inq_dim().

\param ncid file ID
\param nrecvars pointer that will get number of record variables.
\param recvarids pointer that will get array of record variable IDs.
\param recsizes pointer that will get array of record dimension length.

\returns oldmode for success or -1 for failure.
*/
int
ncrecinq(
    int		ncid,
    int*	nrecvars,
    int*	recvarids,
    long*	recsizes
)
{
	size_t nrv = 0;
	size_t *rs = NULL;
	int status = NC_NOERR;

	rs = (size_t*)malloc(sizeof(size_t)*NC_MAX_VARS);
	if(rs == NULL)
	    return NC_ENOMEM;

	status = nc_inq_rec(ncid, &nrv, recvarids, rs);
	if(status != NC_NOERR)
	{
		nc_advise("ncrecinq", status, "ncid %d", ncid);
		if(rs != NULL) free(rs);
		return -1;
	}

	if(nrecvars != NULL)
		*nrecvars = (int) nrv;

	if(recsizes != NULL)
	{
		size_t ii;
		for(ii = 0; ii < nrv; ii++)
		{
			recsizes[ii] = (long) rs[ii];
		}
	}

	if(rs != NULL) free(rs);

	return (int) nrv;
}

/** \ingroup v2_api

Read one record's worth of data, except don't read from variables for which
the address of the data to be read is null.  Return -1 on error. This is
the same as the nc_get_rec(), with poorer error handling.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_get_vara().

\param ncid file ID
\param recnum the record number to read.
\param datap pointer memory to hold one record's worth of data for all
variables.

\returns 0 for success, -1 for error.
*/
int
ncrecget(
    int		ncid,
    long	recnum,
    void**	datap
)
{
	const int status = nc_get_rec(ncid, (size_t)recnum, datap);
	if(status != NC_NOERR)
	{
		nc_advise("ncrecget", status, "ncid %d", ncid);
		return -1;
	}
	return 0;
}

/** \ingroup v2_api

Write one record's worth of data, except don't write to variables for which
the address of the data to be written is NULL.  Return -1 on error.  This is
the same as the nc_put_rec(), but with poorer error handling.

This is part of the legacy V2 API of netCDF. New code should be
written with the V3 API. See V3 function nc_put_vara().

\param ncid file ID
\param recnum the record number to write.
\param datap pointer to one record's worth of data for all variables.

\returns 0 for success, -1 for error.
*/
int
ncrecput(
    int		ncid,
    long	recnum,
    void* const* datap
)
{
	const int status = nc_put_rec(ncid, (size_t)recnum, datap);
	if(status != NC_NOERR)
	{
		nc_advise("ncrecput", status, "ncid %d", ncid);
		return -1;
	}
	return 0;
}

#endif /* NO_NETCDF_2 */
