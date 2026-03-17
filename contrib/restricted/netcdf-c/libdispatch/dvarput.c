/*! \file dvarput.c
Functions for writing data to variables.

Copyright 2018 University Corporation for Atmospheric
Research/Unidata. See COPYRIGHT file for more info.
*/

#include "ncdispatch.h"

struct PUTodometer {
    int            rank;
    size_t         index[NC_MAX_VAR_DIMS];
    size_t         start[NC_MAX_VAR_DIMS];
    size_t         edges[NC_MAX_VAR_DIMS];
    ptrdiff_t      stride[NC_MAX_VAR_DIMS];
    size_t         stop[NC_MAX_VAR_DIMS];
};

/**
 * @internal Initialize odometer.
 *
 * @param odom Pointer to odometer.
 * @param rank
 * @param start Start indices.
 * @param edges Counts.
 * @param stride Strides.
 *
 */
static void
odom_init(struct PUTodometer* odom, int rank, const size_t* start,
          const size_t* edges, const ptrdiff_t* stride)
{
    int i;
    memset(odom,0,sizeof(struct PUTodometer));
    odom->rank = rank;
    assert(odom->rank <= NC_MAX_VAR_DIMS);
    for(i=0;i<odom->rank;i++) {
	odom->start[i] = (start != NULL ? start[i] : 0);
	odom->edges[i] = (edges != NULL ? edges[i] : 1);
	odom->stride[i] = (stride != NULL ? stride[i] : 1);
	odom->stop[i] = odom->start[i] + (odom->edges[i]*(size_t)odom->stride[i]);
	odom->index[i] = odom->start[i];
    }
}

/**
 * @internal Return true if there is more.
 *
 * @param odom Pointer to odometer.
 *
 * @return True if there is more, 0 otherwise.
 */
static int
odom_more(struct PUTodometer* odom)
{
    return (odom->index[0] < odom->stop[0]);
}

/**
 * @internal Return true if there is more.
 *
 * @param odom Pointer to odometer.
 *
 * @return True if there is more, 0 otherwise.
 */
static int
odom_next(struct PUTodometer* odom)
{
    int i;
    if(odom->rank == 0) return 0;
    for(i=odom->rank-1;i>=0;i--) {
        odom->index[i] += (size_t)odom->stride[i];
        if(odom->index[i] < odom->stop[i]) break;
	if(i == 0) return 0; /* leave the 0th entry if it overflows*/
	odom->index[i] = odom->start[i]; /* reset this position*/
    }
    return 1;
}

/** \internal
\ingroup variables
*/
static int
NC_put_vara(int ncid, int varid, const size_t *start,
	    const size_t *edges, const void *value, nc_type memtype)
{
   NC* ncp;
   size_t *my_count = (size_t *)edges;

   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;

   if(start == NULL || edges == NULL) {
      stat = NC_check_nulls(ncid, varid, start, &my_count, NULL);
      if(stat != NC_NOERR) return stat;
   }
   stat = ncp->dispatch->put_vara(ncid, varid, start, my_count, value, memtype);
   if(edges == NULL) free(my_count);
   return stat;
}

/** \internal
\ingroup variables
*/
static int
NC_put_var(int ncid, int varid, const void *value, nc_type memtype)
{
   int ndims;
   size_t shape[NC_MAX_VAR_DIMS];
   int stat = nc_inq_varndims(ncid,varid, &ndims);
   if(stat) return stat;
   stat = NC_getshape(ncid,varid, ndims, shape);
   if(stat) return stat;
   return NC_put_vara(ncid, varid, NC_coord_zero, shape, value, memtype);
}

/** \internal
\ingroup variables
*/
static int
NC_put_var1(int ncid, int varid, const size_t *coord, const void* value,
	    nc_type memtype)
{
   return NC_put_vara(ncid, varid, coord, NC_coord_one, value, memtype);
}

/** \internal
\ingroup variables
*/
int
NCDEFAULT_put_vars(int ncid, int varid, const size_t * start,
	    const size_t * edges, const ptrdiff_t * stride,
	    const void *value0, nc_type memtype)
{
  /* Rebuilt put_vars code to simplify and avoid use of put_varm */

   int status = NC_NOERR;
   int i,isstride1,isrecvar;
   int rank;
   struct PUTodometer odom;
   nc_type vartype = NC_NAT;
   NC* ncp;
   size_t vartypelen;
   size_t nels;
   int memtypelen;
   const char* value = (const char*)value0;
   int nrecdims;                /* number of record dims for a variable */
   int is_recdim[NC_MAX_VAR_DIMS]; /* for variable's dimensions */
   size_t varshape[NC_MAX_VAR_DIMS];
   size_t mystart[NC_MAX_VAR_DIMS];
   size_t myedges[NC_MAX_VAR_DIMS];
   ptrdiff_t mystride[NC_MAX_VAR_DIMS];
   const char* memptr = value;

   status = NC_check_id (ncid, &ncp);
   if(status != NC_NOERR) return status;

   status = nc_inq_vartype(ncid, varid, &vartype);
   if(status != NC_NOERR) return status;

   if(memtype == NC_NAT) memtype = vartype;

   /* compute the variable type size */
   status = nc_inq_type(ncid,vartype,NULL,&vartypelen);
   if(status != NC_NOERR) return status;

   if(memtype > NC_MAX_ATOMIC_TYPE)
	memtypelen = (int)vartypelen;
    else
	memtypelen = nctypelen(memtype);

   /* Check gross internal/external type compatibility */
   if(vartype != memtype) {
      /* If !atomic, the two types must be the same */
      if(vartype > NC_MAX_ATOMIC_TYPE
         || memtype > NC_MAX_ATOMIC_TYPE)
	 return NC_EBADTYPE;
      /* ok, the types differ but both are atomic */
      if(memtype == NC_CHAR || vartype == NC_CHAR)
	 return NC_ECHAR;
   }

   /* Get the variable rank */
   status = nc_inq_varndims(ncid, varid, &rank);
   if(status != NC_NOERR) return status;

   /* Start array is always required for non-scalar vars. */
   if(rank > 0 && start == NULL)
      return NC_EINVALCOORDS;

   /* Get variable dimension sizes */
   status = NC_inq_recvar(ncid,varid,&nrecdims,is_recdim);
   if(status != NC_NOERR) return status;
   isrecvar = (nrecdims > 0);
   NC_getshape(ncid,varid,rank,varshape);

   /* Optimize out using various checks */
   if (rank == 0) {
      /*
       * The variable is a scalar; consequently,
       * there is only one thing to get and only one place to put it.
       * (Why was I called?)
       */
      size_t edge1[1] = {1};
      return NC_put_vara(ncid, varid, start, edge1, value0, memtype);
   }

   /* Do various checks and fixups on start/edges/stride */
   isstride1 = 1; /* assume so */
   nels = 1;
   for(i=0;i<rank;i++) {
	size_t dimlen;
	mystart[i] = (start == NULL ? 0 : start[i]);
#if 0
	dimlen = (i == 0 && isrecvar ? numrecs : varshape[i]);
	if(i == 0 && isrecvar) {/*do nothing*/}
#else
        /* illegal value checks */
	dimlen = varshape[i];
	if(is_recdim[i]) {/*do nothing*/}
#endif
        else {
	  /* mystart is unsigned, will never be < 0 */
	  if (mystart[i] > dimlen) return NC_EINVALCOORDS;
       }
	if(edges == NULL) {
#if 0
	   if(i == 0 && isrecvar)
  	      myedges[i] = numrecs - start[i];
#else
	   if(is_recdim[i] && isrecvar)
  	      myedges[i] = varshape[i] - start[i];
#endif
	   else
	      myedges[i] = varshape[i] - mystart[i];
	} else
	    myedges[i] = edges[i];

	if(!is_recdim[i]) {
	  if (mystart[i] == dimlen && myedges[i] > 0)
              return NC_EINVALCOORDS;
        }

	if(!is_recdim[i]) {
          /* myediges is unsigned, will never be < 0 */
	  if(mystart[i] + myedges[i] > dimlen)
	    return NC_EEDGE;
        }
	mystride[i] = (stride == NULL ? 1 : stride[i]);
	if(mystride[i] <= 0
	   /* cast needed for braindead systems with signed size_t */
           || ((unsigned long) mystride[i] >= X_INT_MAX))
           return NC_ESTRIDE;
  	if(mystride[i] != 1) isstride1 = 0;
        nels *= myedges[i];
   }
   
   if(isstride1) {
      return NC_put_vara(ncid, varid, mystart, myedges, value, memtype);
   }

   if(nels == 0) {
      /* This should be here instead of before NC_put_vara call to 
       * avoid hang in parallel write for single stride.
       * Still issue with parallel hang if stride > 1
       */
      return NC_NOERR; /* cannot write anything */
   }
	   
   /* Initial version uses and odometer to walk the variable
      and read each value one at a time. This can later be optimized
      to read larger chunks at a time.
    */


   odom_init(&odom,rank,mystart,myedges,mystride);

   /* walk the odometer to extract values */
   while(odom_more(&odom)) {
      int localstatus = NC_NOERR;
      /* Write a single value */
      localstatus = NC_put_vara(ncid,varid,odom.index,NC_coord_one,memptr,memtype);
      /* So it turns out that when get_varm is used, all errors are
         delayed and ERANGE will be overwritten by more serious errors.
      */
      if(localstatus != NC_NOERR) {
	    if(status == NC_NOERR || localstatus != NC_ERANGE)
	       status = localstatus;
      }
      memptr += memtypelen;
      odom_next(&odom);
   }
   return status;
}

/** \internal
\ingroup variables
*/
int
NCDEFAULT_put_varm(
   int ncid,
   int varid,
   const size_t * start,
   const size_t * edges,
   const ptrdiff_t * stride,
   const ptrdiff_t * imapp,
   const void *value0,
   nc_type memtype)
{
   int status = NC_NOERR;
   nc_type vartype = NC_NAT;
   int varndims = 0;
   int maxidim = 0;
   NC* ncp;
   int memtypelen;
   const char* value = (char*)value0;

   status = NC_check_id (ncid, &ncp);
   if(status != NC_NOERR) return status;

/*
  if(NC_indef(ncp)) return NC_EINDEFINE;
  if(NC_readonly (ncp)) return NC_EPERM;
*/

   /* mid body */
   status = nc_inq_vartype(ncid, varid, &vartype);
   if(status != NC_NOERR) return status;
   /* Check that this is an atomic type */
   if(vartype > NC_MAX_ATOMIC_TYPE)
	return NC_EMAPTYPE;

   status = nc_inq_varndims(ncid, varid, &varndims);
   if(status != NC_NOERR) return status;

   if(memtype == NC_NAT) {
      memtype = vartype;
   }

   if(memtype == NC_CHAR && vartype != NC_CHAR)
      return NC_ECHAR;
   else if(memtype != NC_CHAR && vartype == NC_CHAR)
      return NC_ECHAR;

   memtypelen = nctypelen(memtype);

   maxidim = (int) varndims - 1;

   if (maxidim < 0)
   {
      /*
       * The variable is a scalar; consequently,
       * there s only one thing to get and only one place to put it.
       * (Why was I called?)
       */
      size_t edge1[1] = {1};
      return NC_put_vara(ncid, varid, start, edge1, value, memtype);
   }

   /*
    * else
    * The variable is an array.
    */
   {
      int idim;
      size_t *mystart = NULL;
      size_t *myedges = 0;
      size_t *iocount= 0;    /* count vector */
      size_t *stop = 0;   /* stop indexes */
      size_t *length = 0; /* edge lengths in bytes */
      ptrdiff_t *mystride = 0;
      ptrdiff_t *mymap= 0;
      size_t varshape[NC_MAX_VAR_DIMS];
      int isrecvar;
      size_t numrecs;
      int stride1; /* is stride all ones? */

      /*
       * Verify stride argument.
       */
      stride1 = 1;		/*  assume ok; */
      if(stride != NULL) {
	 for (idim = 0; idim <= maxidim; ++idim) {
            if ((stride[idim] == 0)
		/* cast needed for braindead systems with signed size_t */
                || ((unsigned long) stride[idim] >= X_INT_MAX))
	    {
	       return NC_ESTRIDE;
            }
	    if(stride[idim] != 1) stride1 = 0;
	 }
      }

      /* If stride1 is true, and there is no imap, then call get_vara
         directly
      */
      if(stride1 && imapp == NULL) {
	 return NC_put_vara(ncid, varid, start, edges, value, memtype);
      }

      /* Compute some dimension related values */
      isrecvar = NC_is_recvar(ncid,varid,&numrecs);
      NC_getshape(ncid,varid,varndims,varshape);

      /* assert(sizeof(ptrdiff_t) >= sizeof(size_t)); */
      mystart = (size_t *)calloc((size_t)(varndims * 7), sizeof(ptrdiff_t));
      if(mystart == NULL) return NC_ENOMEM;
      myedges = mystart + varndims;
      iocount = myedges + varndims;
      stop = iocount + varndims;
      length = stop + varndims;
      mystride = (ptrdiff_t *)(length + varndims);
      mymap = mystride + varndims;

      /*
       * Check start, edges
       */
      for (idim = maxidim; idim >= 0; --idim)
      {
	 mystart[idim] = start != NULL
	    ? start[idim]
	    : 0;

	 myedges[idim] = edges != NULL
	    ? edges[idim]
	    : idim == 0 && isrecvar
    	        ? numrecs - mystart[idim]
	        : varshape[idim] - mystart[idim];
      }

      for (idim = isrecvar; idim <= maxidim; ++idim)
      {
	 if (mystart[idim] > varshape[idim] ||
	    (mystart[idim] == varshape[idim] && myedges[idim] > 0))
	 {
	    status = NC_EINVALCOORDS;
	    goto done;
	 }

	 if (mystart[idim] + myedges[idim] > varshape[idim])
	 {
	    status = NC_EEDGE;
	    goto done;
	 }
      }

      /*
       * Initialize I/O parameters.
       */
      for (idim = maxidim; idim >= 0; --idim)
      {
	 if (edges != NULL && edges[idim] == 0)
	 {
	    status = NC_NOERR;    /* read/write no data */
	    goto done;
	 }

	 mystride[idim] = stride != NULL
	    ? stride[idim]
	    : 1;
	 mymap[idim] = imapp != NULL
	    ? imapp[idim]
	    : idim == maxidim
	        ? 1
	        : mymap[idim + 1] * (ptrdiff_t) myedges[idim + 1];

	 iocount[idim] = 1;
	 length[idim] = ((size_t)mymap[idim]) * myedges[idim];
	 stop[idim] = mystart[idim] + myedges[idim] * (size_t)mystride[idim];
      }

      /* Lower body */
      /*
       * As an optimization, adjust I/O parameters when the fastest
       * dimension has unity stride both externally and internally.
       * In this case, the user could have called a simpler routine
       * (i.e. ncvar$1()
       */
      if (mystride[maxidim] == 1
	  && mymap[maxidim] == 1)
      {
	 iocount[maxidim] = myedges[maxidim];
	 mystride[maxidim] = (ptrdiff_t) myedges[maxidim];
	 mymap[maxidim] = (ptrdiff_t) length[maxidim];
      }

      /*
       * Perform I/O.  Exit when done.
       */
      for (;;)
      {
	 /* TODO: */
	 int lstatus = NC_put_vara(ncid, varid, mystart, iocount,
				   value, memtype);
	 if (lstatus != NC_NOERR) {
	    if(status == NC_NOERR || lstatus != NC_ERANGE)
	       status = lstatus;
	 }

	 /*
	  * The following code permutes through the variable s
	  * external start-index space and it s internal address
	  * space.  At the UPC, this algorithm is commonly
	  * called "odometer code".
	  */
	 idim = maxidim;
        carry:
	 value += (mymap[idim] * memtypelen);
	 mystart[idim] += (size_t)mystride[idim];
	 if (mystart[idim] == stop[idim])
	 {
	    size_t l = (length[idim] * (size_t)memtypelen);
	    value -= l;
	    mystart[idim] = start[idim];
	    if (--idim < 0)
	       break; /* normal return */
	    goto carry;
	 }
      } /* I/O loop */
     done:
      free(mystart);
   } /* variable is array */
   return status;
}

/** \internal
\ingroup variables
*/
static int
NC_put_vars(int ncid, int varid, const size_t *start,
	    const size_t *edges, const ptrdiff_t *stride,
	    const void *value, nc_type memtype)
{
   NC* ncp;
   size_t *my_count = (size_t *)edges;
   ptrdiff_t *my_stride = (ptrdiff_t *)stride;
   int stat;

   stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;

   /* Handle any NULL parameters. */
   if(start == NULL || edges == NULL || stride == NULL) {
      stat = NC_check_nulls(ncid, varid, start, &my_count, &my_stride);
      if(stat != NC_NOERR) return stat;
   }

   stat = ncp->dispatch->put_vars(ncid, varid, start, my_count, my_stride,
                                  value, memtype);
   if(edges == NULL) free(my_count);
   if(stride == NULL) free(my_stride);
   return stat;
}

/** \internal
\ingroup variables
*/
static int
NC_put_varm(int ncid, int varid, const size_t *start,
	    const size_t *edges, const ptrdiff_t *stride, const ptrdiff_t* map,
	    const void *value, nc_type memtype)
{
   NC* ncp;
   size_t *my_count = (size_t *)edges;
   ptrdiff_t *my_stride = (ptrdiff_t *)stride;
   int stat;

   stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;

   /* Handle any NULL parameters. */
   if(start == NULL || edges == NULL || stride == NULL) {
      stat = NC_check_nulls(ncid, varid, start, &my_count, &my_stride);
      if(stat != NC_NOERR) return stat;
   }

   stat = ncp->dispatch->put_varm(ncid, varid, start, my_count, my_stride,
                                  map, value, memtype);
   if(edges == NULL) free(my_count);
   if(stride == NULL) free(my_stride);
   return stat;
}

/** \name Writing Data to Variables

Functions to write data from variables. */
/*! \{ */ /* All these functions are part of this named group... */

/** \ingroup variables
Write an array of values to a variable.

The values to be written are associated with the netCDF variable by
assuming that the last dimension of the netCDF variable varies fastest
in the C interface. The netCDF dataset must be in data mode. The array
to be written is specified by giving a corner and a vector of edge
lengths to \ref specify_hyperslab.

The functions for types ubyte, ushort, uint, longlong, ulonglong, and
string are only available for netCDF-4/HDF5 files.

The nc_put_var() function will write a variable of any type, including
user defined type. For this function, the type of the data in memory
must match the type of the variable - no data conversion is done.

\param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().

\param varid Variable ID

\param startp Start vector with one element for each dimension to \ref
specify_hyperslab. This array must be same size as variable's number
of dimensions.

\param countp Count vector with one element for each dimension to \ref
specify_hyperslab. This array must be same size as variable's number
of dimensions.

\param op Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_EEDGE Start+count exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
 */
/**@{*/
int
nc_put_vara(int ncid, int varid, const size_t *startp,
	    const size_t *countp, const void *op)
{
   NC* ncp;
   int stat = NC_check_id(ncid, &ncp);
   nc_type xtype;
   if(stat != NC_NOERR) return stat;
   stat = nc_inq_vartype(ncid, varid, &xtype);
   if(stat != NC_NOERR) return stat;
   return NC_put_vara(ncid, varid, startp, countp, op, xtype);
}

int
nc_put_vara_text(int ncid, int varid, const size_t *startp,
		 const size_t *countp, const char *op)
{
   return NC_put_vara(ncid, varid, startp, countp,
		      (void*)op, NC_CHAR);
}

int
nc_put_vara_schar(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const signed char *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      NC_BYTE);
}

int
nc_put_vara_uchar(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const unsigned char *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      T_uchar);
}

int
nc_put_vara_short(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const short *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      NC_SHORT);
}

int
nc_put_vara_int(int ncid, int varid, const size_t *startp,
		const size_t *countp, const int *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      NC_INT);
}

int
nc_put_vara_long(int ncid, int varid, const size_t *startp,
		 const size_t *countp, const long *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      T_long);
}

int
nc_put_vara_float(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const float *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      T_float);
}

int
nc_put_vara_double(int ncid, int varid, const size_t *startp,
		   const size_t *countp, const double *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      T_double);
}

int
nc_put_vara_ubyte(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const unsigned char *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      T_ubyte);
}

int
nc_put_vara_ushort(int ncid, int varid, const size_t *startp,
		   const size_t *countp, const unsigned short *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      T_ushort);
}

int
nc_put_vara_uint(int ncid, int varid, const size_t *startp,
		 const size_t *countp, const unsigned int *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      T_uint);
}

int
nc_put_vara_longlong(int ncid, int varid, const size_t *startp,
		     const size_t *countp, const long long *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      T_longlong);
}

int
nc_put_vara_ulonglong(int ncid, int varid, const size_t *startp,
		      const size_t *countp, const unsigned long long *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      NC_UINT64);
}

int
nc_put_vara_string(int ncid, int varid, const size_t *startp,
		   const size_t *countp, const char* *op)
{
   return NC_put_vara(ncid, varid, startp, countp, (void *)op,
		      NC_STRING);
}

/**@}*/

/** \ingroup variables
Write one datum.

\param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().

\param varid Variable ID

\param indexp Index vector with one element for each dimension.

\param op Pointer from where the data will be copied.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_EEDGE Start+count exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
 */
/**@{*/
int
nc_put_var1(int ncid, int varid, const size_t *indexp, const void *op)
{
   return NC_put_var1(ncid, varid, indexp, op, NC_NAT);
}

int
nc_put_var1_text(int ncid, int varid, const size_t *indexp, const char *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_CHAR);
}

int
nc_put_var1_schar(int ncid, int varid, const size_t *indexp, const signed char *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_BYTE);
}

int
nc_put_var1_uchar(int ncid, int varid, const size_t *indexp, const unsigned char *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_UBYTE);
}

int
nc_put_var1_short(int ncid, int varid, const size_t *indexp, const short *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_SHORT);
}

int
nc_put_var1_int(int ncid, int varid, const size_t *indexp, const int *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_INT);
}

int
nc_put_var1_long(int ncid, int varid, const size_t *indexp, const long *op)
{
   return NC_put_var1(ncid, varid, indexp, (void*)op, longtype);
}

int
nc_put_var1_float(int ncid, int varid, const size_t *indexp, const float *op)
{
   return NC_put_var1(ncid, varid, indexp, (void*)op, NC_FLOAT);
}

int
nc_put_var1_double(int ncid, int varid, const size_t *indexp, const double *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_DOUBLE);
}

int
nc_put_var1_ubyte(int ncid, int varid, const size_t *indexp, const unsigned char *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_UBYTE);
}

int
nc_put_var1_ushort(int ncid, int varid, const size_t *indexp, const unsigned short *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_USHORT);
}

int
nc_put_var1_uint(int ncid, int varid, const size_t *indexp, const unsigned int *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_UINT);
}

int
nc_put_var1_longlong(int ncid, int varid, const size_t *indexp, const long long *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_INT64);
}

int
nc_put_var1_ulonglong(int ncid, int varid, const size_t *indexp, const unsigned long long *op)
{
   return NC_put_var1(ncid, varid, indexp, (void *)op, NC_UINT64);
}

int
nc_put_var1_string(int ncid, int varid, const size_t *indexp, const char* *op)
{
   return NC_put_var1(ncid, varid, indexp, (void*)op, NC_STRING);
}

/**@}*/

/** \ingroup variables
Write an entire variable with one call.

The nc_put_var_ type family of functions write all the values of a
variable into a netCDF variable of an open netCDF dataset. This is the
simplest interface to use for writing a value in a scalar variable or
whenever all the values of a multidimensional variable can all be
written at once. The values to be written are associated with the
netCDF variable by assuming that the last dimension of the netCDF
variable varies fastest in the C interface. The values are converted
to the external data type of the variable, if necessary.

Take care when using this function with record variables (variables
that use the ::NC_UNLIMITED dimension). If you try to write all the
values of a record variable into a netCDF file that has no record data
yet (hence has 0 records), nothing will be written. Similarly, if you
try to write all the values of a record variable but there are more
records in the file than you assume, more in-memory data will be
accessed than you supply, which may result in a segmentation
violation. To avoid such problems, it is better to use the nc_put_vara
interfaces for variables that use the ::NC_UNLIMITED dimension.

The functions for types ubyte, ushort, uint, longlong, ulonglong, and
string are only available for netCDF-4/HDF5 files.

The nc_put_var() function will write a variable of any type, including
user defined type. For this function, the type of the data in memory
must match the type of the variable - no data conversion is done.

\param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().

\param varid Variable ID

\param op Pointer from where the data will be copied.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_EEDGE Start+count exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
 */
/**@{*/
int
nc_put_var(int ncid, int varid, const void *op)
{
   return NC_put_var(ncid, varid, op, NC_NAT);
}

int
nc_put_var_text(int ncid, int varid, const char *op)
{
   return NC_put_var(ncid,varid,(void*)op,NC_CHAR);
}

int
nc_put_var_schar(int ncid, int varid, const signed char *op)
{
   return NC_put_var(ncid,varid,(void*)op,NC_BYTE);
}

int
nc_put_var_uchar(int ncid, int varid, const unsigned char *op)
{
   return NC_put_var(ncid,varid,(void*)op,T_uchar);
}

int
nc_put_var_short(int ncid, int varid, const short *op)
{
   return NC_put_var(ncid,varid,(void*)op,NC_SHORT);
}

int
nc_put_var_int(int ncid, int varid, const int *op)
{
   return NC_put_var(ncid,varid,(void*)op,NC_INT);
}

int
nc_put_var_long(int ncid, int varid, const long *op)
{
   return NC_put_var(ncid,varid,(void*)op,T_long);
}

int
nc_put_var_float(int ncid, int varid, const float *op)
{
   return NC_put_var(ncid,varid,(void*)op,T_float);
}

int
nc_put_var_double(int ncid, int varid, const double *op)
{
   return NC_put_var(ncid,varid,(void*)op,T_double);
}

int
nc_put_var_ubyte(int ncid, int varid, const unsigned char *op)
{
   return NC_put_var(ncid,varid,(void*)op,T_ubyte);
}

int
nc_put_var_ushort(int ncid, int varid, const unsigned short *op)
{
   return NC_put_var(ncid,varid,(void*)op,T_ushort);
}

int
nc_put_var_uint(int ncid, int varid, const unsigned int *op)
{
   return NC_put_var(ncid,varid,(void*)op,T_uint);
}

int
nc_put_var_longlong(int ncid, int varid, const long long *op)
{
   return NC_put_var(ncid,varid,(void*)op,T_longlong);
}

int
nc_put_var_ulonglong(int ncid, int varid, const unsigned long long *op)
{
   return NC_put_var(ncid,varid,(void*)op,NC_UINT64);
}

int
nc_put_var_string(int ncid, int varid, const char* *op)
{
   return NC_put_var(ncid,varid,(void*)op,NC_STRING);
}

/**\} */

/** \ingroup variables
Write a strided array of values to a variable.

\param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().

\param varid Variable ID

\param startp Start vector with one element for each dimension to \ref
specify_hyperslab. This array must be same size as variable's number
of dimensions.

\param countp Count vector with one element for each dimension to \ref
specify_hyperslab. This array must be same size as variable's number
of dimensions.

\param stridep Stride vector with one element for each dimension to
\ref specify_hyperslab. This array must be same size as variable's
number of dimensions.

\param op Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_EEDGE Start+count exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
 */
/**@{*/
int
nc_put_vars(int ncid, int varid, const size_t *startp,
	     const size_t *countp, const ptrdiff_t *stridep,
	     const void *op)
{
   return NC_put_vars(ncid, varid, startp, countp, stridep, op, NC_NAT);
}

int
nc_put_vars_text(int ncid, int varid, const size_t *startp,
		 const size_t *countp, const ptrdiff_t *stridep,
		 const char *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep,(void*)op,NC_CHAR);
}

int
nc_put_vars_schar(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const ptrdiff_t *stridep,
		  const signed char *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep,(void*)op,NC_BYTE);
}

int
nc_put_vars_uchar(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep,
		  const unsigned char *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, T_uchar);
}

int
nc_put_vars_short(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep,
		  const short *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, NC_SHORT);
}

int
nc_put_vars_int(int ncid, int varid,
		const size_t *startp, const size_t *countp,
		const ptrdiff_t *stridep,
		const int *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, NC_INT);
}

int
nc_put_vars_long(int ncid, int varid,
		 const size_t *startp, const size_t *countp,
		 const ptrdiff_t *stridep,
		 const long *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, T_long);
}

int
nc_put_vars_float(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep,
		  const float *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, T_float);
}

int
nc_put_vars_double(int ncid, int varid,
		   const size_t *startp, const size_t *countp,
		   const ptrdiff_t *stridep,
		   const double *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, T_double);
}

int
nc_put_vars_ubyte(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep,
		  const unsigned char *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, T_ubyte);
}

int
nc_put_vars_ushort(int ncid, int varid,
		   const size_t *startp, const size_t *countp,
		   const ptrdiff_t *stridep,
		   const unsigned short *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, T_ushort);
}

int
nc_put_vars_uint(int ncid, int varid,
		 const size_t *startp, const size_t *countp,
		 const ptrdiff_t *stridep,
		 const unsigned int *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, T_uint);
}

int
nc_put_vars_longlong(int ncid, int varid,
		     const size_t *startp, const size_t *countp,
		     const ptrdiff_t *stridep,
		     const long long *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, T_longlong);
}

int
nc_put_vars_ulonglong(int ncid, int varid,
		      const size_t *startp, const size_t *countp,
		      const ptrdiff_t *stridep,
		      const unsigned long long *op)
{
   return NC_put_vars(ncid, varid, startp, countp,
		      stridep, (void *)op, NC_UINT64);
}

int
nc_put_vars_string(int ncid, int varid,
		   const size_t *startp, const size_t *countp,
		   const ptrdiff_t *stridep,
		   const char**op)
{
   return NC_put_vars(ncid, varid, startp, countp, stridep,
		      (void *)op, NC_STRING);
}

/**\} */

/** \ingroup variables
Write a mapped array of values to a variable.

The nc_put_varm() function will only write a variable of an
atomic type; it will not write user defined types. For this
function, the type of the data in memory must match the type
of the variable - no data conversion is done.

@deprecated Use of this family of functions is discouraged,
although it will continue to be supported.
The reason is the complexity of the
algorithm makes its use difficult for users to properly use.

\param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().

\param varid Variable ID

\param startp Start vector with one element for each dimension to \ref
specify_hyperslab. This array must be same size as variable's number
of dimensions.

\param countp Count vector with one element for each dimension to \ref
specify_hyperslab. This array must be same size as variable's number
of dimensions.

\param stridep Stride vector with one element for each dimension to
\ref specify_hyperslab. This array must be same size as variable's
number of dimensions.

\param imapp Mapping vector with one element for each dimension to
\ref specify_hyperslab.

\param op Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_EEDGE Start+count exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
 */
/**@{*/
int
nc_put_varm(int ncid, int varid, const size_t *startp,
	     const size_t *countp, const ptrdiff_t *stridep,
	     const ptrdiff_t *imapp, const void *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp, op, NC_NAT);
}

int
nc_put_varm_text(int ncid, int varid, const size_t *startp,
		 const size_t *countp, const ptrdiff_t *stridep,
		 const ptrdiff_t *imapp, const char *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, NC_CHAR);
}

int
nc_put_varm_schar(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		  const signed char *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, NC_BYTE);
}

int
nc_put_varm_uchar(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		  const unsigned char *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, T_uchar);
}

int
nc_put_varm_short(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		  const short *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, NC_SHORT);
}

int
nc_put_varm_int(int ncid, int varid,
		const size_t *startp, const size_t *countp,
		const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		const int *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, NC_INT);
}

int
nc_put_varm_long(int ncid, int varid,
		 const size_t *startp, const size_t *countp,
		 const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		 const long *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, T_long);
}

int
nc_put_varm_float(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		  const float *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, T_float);
}

int
nc_put_varm_double(int ncid, int varid,
		   const size_t *startp, const size_t *countp,
		   const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		   const double *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, T_double);
}

int
nc_put_varm_ubyte(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		  const unsigned char *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, T_ubyte);
}

int
nc_put_varm_ushort(int ncid, int varid,
		   const size_t *startp, const size_t *countp,
		   const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		   const unsigned short *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, T_ushort);
}

int
nc_put_varm_uint(int ncid, int varid,
		 const size_t *startp, const size_t *countp,
		 const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		 const unsigned int *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, T_uint);
}

int
nc_put_varm_longlong(int ncid, int varid,
		     const size_t *startp, const size_t *countp,
		     const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		     const long long *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, T_longlong);
}

int
nc_put_varm_ulonglong(int ncid, int varid,
		      const size_t *startp, const size_t *countp,
		      const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		      const unsigned long long *op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, NC_UINT64);
}

int
nc_put_varm_string(int ncid, int varid,
		   const size_t *startp, const size_t *countp,
		   const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		   const char**op)
{
   return NC_put_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)op, NC_STRING);
}

/**\} */

/*! \} */ /*End of named group... */
