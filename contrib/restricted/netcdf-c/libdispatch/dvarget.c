/*! \file
Functions for getting data from variables.

Copyright 2018 University Corporation for Atmospheric
Research/Unidata. See \ref copyright file for more info.

*/

#include "ncdispatch.h"

/*!
  \internal

*/
struct GETodometer {
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
odom_init(struct GETodometer* odom, int rank, const size_t* start,
          const size_t* edges, const ptrdiff_t* stride)
{
    int i;
    memset(odom,0,sizeof(struct GETodometer));
    odom->rank = rank;
    assert(odom->rank <= NC_MAX_VAR_DIMS);
    for(i=0;i<odom->rank;i++) {
	odom->start[i] = (start != NULL ? start[i] : 0);
	odom->edges[i] = (edges != NULL ? edges[i] : 1);
	odom->stride[i] = (stride != NULL ? stride[i] : 1);
	odom->stop[i] = odom->start[i] + (odom->edges[i]*((size_t)odom->stride[i]));
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
odom_more(struct GETodometer* odom)
{
    return (odom->index[0] < odom->stop[0]);
}

/**
 * @internal Move odometer.
 *
 * @param odom Pointer to odometer.
 *
 * @return 0 or 1
 */
static int
odom_next(struct GETodometer* odom)
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
int
NC_get_vara(int ncid, int varid,
	    const size_t *start, const size_t *edges,
            void *value, nc_type memtype)
{
   NC* ncp;
   size_t *my_count = (size_t *)edges;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;

   if(start == NULL || edges == NULL) {
      stat = NC_check_nulls(ncid, varid, start, &my_count, NULL);
      if(stat != NC_NOERR) return stat;
   }
   stat =  ncp->dispatch->get_vara(ncid,varid,start,my_count,value,memtype);
   if(edges == NULL) free(my_count);
   return stat;
}

/** 
\internal
Get data for a variable.

\param ncid NetCDF or group ID.

\param varid Variable ID

\param value Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\param memtype the NC type of the data after it is read into
memory. Data are converted from the variable's type to the memtype as
they are read.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_EEDGE Start+count exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.

\ingroup variables
\author Dennis Heimbigner
 */
static int
NC_get_var(int ncid, int varid, void *value, nc_type memtype)
{
   return NC_get_vara(ncid, varid, NC_coord_zero, NULL, value, memtype);
}

/** \internal
\ingroup variables
 Most dispatch tables will use the default procedures
*/
int
NCDEFAULT_get_vars(int ncid, int varid, const size_t * start,
	    const size_t * edges, const ptrdiff_t * stride,
	    void *value0, nc_type memtype)
{
  /* Rebuilt get_vars code to simplify and avoid use of get_varm */

   int status = NC_NOERR;
   int i,simplestride,isrecvar;
   int rank;
   struct GETodometer odom;
   nc_type vartype = NC_NAT;
   NC* ncp;
   int memtypelen;
   size_t vartypelen;
   size_t nels;
   char* value = (char*)value0;
   size_t numrecs;
   size_t varshape[NC_MAX_VAR_DIMS];
   size_t mystart[NC_MAX_VAR_DIMS];
   size_t myedges[NC_MAX_VAR_DIMS];
   ptrdiff_t mystride[NC_MAX_VAR_DIMS];
   char *memptr = NULL;

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
   isrecvar = NC_is_recvar(ncid,varid,&numrecs);
   NC_getshape(ncid,varid,rank,varshape);

   /* Optimize out using various checks */
   if (rank == 0) {
      /*
       * The variable is a scalar; consequently,
       * there s only one thing to get and only one place to put it.
       * (Why was I called?)
       */
      size_t edge1[1] = {1};
      return NC_get_vara(ncid, varid, start, edge1, value, memtype);
   }

   /* Do various checks and fixups on start/edges/stride */
   simplestride = 1; /* assume so */
   nels = 1;
   for(i=0;i<rank;i++) {
	size_t dimlen;
	mystart[i] = (start == NULL ? 0 : start[i]);
        /* illegal value checks */
	dimlen = (i == 0 && isrecvar ? numrecs : varshape[i]);
        /* mystart is unsigned, never < 0 */
	if (mystart[i] > dimlen) return NC_EINVALCOORDS;

	if(edges == NULL) {
	   if(i == 0 && isrecvar)
  	      myedges[i] = numrecs - start[i];
	   else
	      myedges[i] = varshape[i] - mystart[i];
	} else
	    myedges[i] = edges[i];

	if (mystart[i] == dimlen && myedges[i] > 0) return NC_EINVALCOORDS;

        /* myedges is unsigned, never < 0 */
	if(mystart[i] + myedges[i] > dimlen)
	  return NC_EEDGE;
	mystride[i] = (stride == NULL ? 1 : stride[i]);
	if(mystride[i] <= 0
	   /* cast needed for braindead systems with signed size_t */
           || ((unsigned long) mystride[i] >= X_INT_MAX))
           return NC_ESTRIDE;
  	if(mystride[i] != 1) simplestride = 0;
        if(myedges[i] == 0)
          nels = 0;
   }
   if(nels == 0)
      return NC_NOERR; /* cannot read anything */
   if(simplestride) {
      return NC_get_vara(ncid, varid, mystart, myedges, value, memtype);
   }

   /* memptr indicates where to store the next value */
   memptr = value;

   odom_init(&odom,rank,mystart,myedges,mystride);

   /* walk the odometer to extract values */
   while(odom_more(&odom)) {
      int localstatus = NC_NOERR;
      /* Read a single value */
      localstatus = NC_get_vara(ncid,varid,odom.index,NC_coord_one,memptr,memtype);
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
static int
NC_get_var1(int ncid, int varid, const size_t *coord, void* value,
	    nc_type memtype)
{
   return NC_get_vara(ncid, varid, coord, NC_coord_one, value, memtype);
}

/** \internal
\ingroup variables
 */
int
NCDEFAULT_get_varm(int ncid, int varid, const size_t *start,
	    const size_t *edges, const ptrdiff_t *stride,
	    const ptrdiff_t *imapp, void *value0, nc_type memtype)
{
   int status = NC_NOERR;
   nc_type vartype = NC_NAT;
   int varndims,maxidim;
   NC* ncp;
   int memtypelen;
   char* value = (char*)value0;

   status = NC_check_id (ncid, &ncp);
   if(status != NC_NOERR) return status;

/*
  if(NC_indef(ncp)) return NC_EINDEFINE;
*/

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
      return NC_get_vara(ncid, varid, start, edge1, value, memtype);
   }

   /*
    * else
    * The variable is an array.
    */
   {
      int idim;
      size_t *mystart = NULL;
      size_t *myedges;
      size_t *iocount;    /* count vector */
      size_t *stop;   /* stop indexes */
      size_t *length; /* edge lengths in bytes */
      ptrdiff_t *mystride;
      ptrdiff_t *mymap;
      size_t varshape[NC_MAX_VAR_DIMS];
      int isrecvar;
      size_t numrecs;

      /* Compute some dimension related values */
      isrecvar = NC_is_recvar(ncid,varid,&numrecs);
      NC_getshape(ncid,varid,varndims,varshape);

      /*
       * Verify stride argument; also see if stride is all ones
       */
      if(stride != NULL) {
	 int stride1 = 1;
	 for (idim = 0; idim <= maxidim; ++idim)
	 {
            if (stride[idim] == 0
		/* cast needed for braindead systems with signed size_t */
                || ((unsigned long) stride[idim] >= X_INT_MAX))
            {
	       return NC_ESTRIDE;
            }
	    if(stride[idim] != 1) stride1 = 0;
	 }
         /* If stride1 is true, and there is no imap
            then call get_vara directly.
         */
         if(stride1 && imapp == NULL) {
	     return NC_get_vara(ncid, varid, start, edges, value, memtype);
	 }
      }

      /* assert(sizeof(ptrdiff_t) >= sizeof(size_t)); */
      /* Allocate space for mystart,mystride,mymap etc.all at once */
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
	 size_t dimlen =
	    idim == 0 && isrecvar
	    ? numrecs
	    : varshape[idim];

	 mystart[idim] = start != NULL
	    ? start[idim]
	    : 0;

	 if (mystart[idim] > dimlen)
	 {
	    status = NC_EINVALCOORDS;
	    goto done;
	 }

#ifdef COMPLEX
	 myedges[idim] = edges != NULL
	    ? edges[idim]
	    : idim == 0 && isrecvar
	    ? numrecs - mystart[idim]
	    : varshape[idim] - mystart[idim];
#else
	 if(edges != NULL)
	    myedges[idim] = edges[idim];
	 else if (idim == 0 && isrecvar)
	    myedges[idim] = numrecs - mystart[idim];
	 else
	    myedges[idim] = varshape[idim] - mystart[idim];
#endif

	 if (mystart[idim] == dimlen && myedges[idim] > 0)
	 {
	    status = NC_EINVALCOORDS;
	    goto done;
	 }

	 if (mystart[idim] + myedges[idim] > dimlen)
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

	 /* Remember: in netCDF-2 imapp is byte oriented, not index oriented
	  *           Starting from netCDF-3, imapp is index oriented */
#ifdef COMPLEX
	 mymap[idim] = (imapp != NULL
			? imapp[idim]
			: (idim == maxidim ? 1
			   : mymap[idim + 1] * (ptrdiff_t) myedges[idim + 1]));
#else
	 if(imapp != NULL)
	    mymap[idim] = imapp[idim];
	 else if (idim == maxidim)
	    mymap[idim] = 1;
	 else
	    mymap[idim] =
	       mymap[idim + 1] * (ptrdiff_t) myedges[idim + 1];
#endif
	 iocount[idim] = 1;
	 length[idim] = ((size_t)mymap[idim]) * myedges[idim];
	 stop[idim] = (mystart[idim] + myedges[idim] * (size_t)mystride[idim]);
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
	 int lstatus = NC_get_vara(ncid, varid, mystart, iocount,
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
	 value += (((int)mymap[idim]) * memtypelen);
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

/**
\internal
Called by externally visible nc_get_vars_xxx routines.

\param ncid NetCDF or group ID.

\param varid Variable ID

\param start start indices. Required for non-scalar vars. This array
   must be same size as variable's number of dimensions.

\param edges count indices. This array must be same size as variable's
   number of dimensions.

\param stride data strides. This array must be same size as variable's
   number of dimensions.

\param value Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\param memtype the NC type of the data after it is read into
memory. Data are converted from the variable's type to the memtype as
they are read.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_EEDGE Start+count exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.

\ingroup variables
\author Dennis Heimbigner, Ed Hartnett
*/
static int
NC_get_vars(int ncid, int varid, const size_t *start,
	    const size_t *edges, const ptrdiff_t *stride, void *value,
	    nc_type memtype)
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

   stat = ncp->dispatch->get_vars(ncid,varid,start,my_count,my_stride,
                                  value,memtype);
   if(edges == NULL) free(my_count);
   if(stride == NULL) free(my_stride);
   return stat;
}

/** 
\internal
Called by externally visible nc_get_varm_xxx routines. Note that the
varm routines are deprecated. Use the vars routines instead for new
code.

\param ncid NetCDF or group ID.

\param varid Variable ID

\param start start indices. 

\param edges count indices.

\param stride data strides.

\param map mapping of dimensions.

\param value Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\param memtype the NC type of the data after it is read into
memory. Data are converted from the variable's type to the memtype as
they are read.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_EEDGE Start+count exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.

\ingroup variables
\author Dennis Heimbigner, Ed Hartnett
 */
static int
NC_get_varm(int ncid, int varid, const size_t *start,
	    const size_t *edges, const ptrdiff_t *stride, const ptrdiff_t* map,
	    void *value, nc_type memtype)
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

   stat = ncp->dispatch->get_varm(ncid, varid, start, my_count, my_stride,
                                  map, value, memtype);
   if(edges == NULL) free(my_count);
   if(stride == NULL) free(my_stride);
   return stat;
}

/** \name Reading Data from Variables

Functions to read data from variables. */
/*! \{ */ /* All these functions are part of this named group... */

/** \ingroup variables
Read an array of values from a variable.

The array to be read is specified by giving a corner and a vector of
edge lengths to \ref specify_hyperslab.

The data values are read into consecutive locations with the last
dimension varying fastest. The netCDF dataset must be in data mode
(for netCDF-4/HDF5 files, the switch to data mode will happen
automatically, unless the classic model is used).

The nc_get_vara() function will read a variable of any type,
including user defined type. For this function, the type of the data
in memory must match the type of the variable - no data conversion is
done.

Other nc_get_vara_ functions will convert data to the desired output
type as needed.

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

\param ip Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_EEDGE Start+count exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.

\section nc_get_vara_double_example Example

Here is an example using nc_get_vara_double() to read all the values of
the variable named rh from an existing netCDF dataset named
foo.nc. For simplicity in this example, we assume that we know that rh
is dimensioned with time, lat, and lon, and that there are three time
values, five lat values, and ten lon values.

\code
     #include <netcdf.h>
        ...
     #define TIMES 3
     #define LATS 5
     #define LONS 10
     int  status;
     int ncid;
     int rh_id;
     static size_t start[] = {0, 0, 0};
     static size_t count[] = {TIMES, LATS, LONS};
     double rh_vals[TIMES*LATS*LONS];
        ...
     status = nc_open("foo.nc", NC_NOWRITE, &ncid);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_inq_varid (ncid, "rh", &rh_id);
     if (status != NC_NOERR) handle_error(status);
        ...
     status = nc_get_vara_double(ncid, rh_id, start, count, rh_vals);
     if (status != NC_NOERR) handle_error(status);
\endcode
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
 */
/**@{*/
int
nc_get_vara(int ncid, int varid, const size_t *startp,
	    const size_t *countp, void *ip)
{
   NC* ncp;
   nc_type xtype = NC_NAT;
   int stat = NC_check_id(ncid, &ncp);
   if(stat != NC_NOERR) return stat;
   stat = nc_inq_vartype(ncid, varid, &xtype);
   if(stat != NC_NOERR) return stat;
   return NC_get_vara(ncid, varid, startp, countp, ip, xtype);
}

int
nc_get_vara_text(int ncid, int varid, const size_t *startp,
		 const size_t *countp, char *ip)
{
   return NC_get_vara(ncid, varid, startp, countp, (void *)ip, NC_CHAR);
}

int
nc_get_vara_schar(int ncid, int varid, const size_t *startp,
		  const size_t *countp, signed char *ip)
{
   return NC_get_vara(ncid, varid, startp, countp, (void *)ip, NC_BYTE);
}

int
nc_get_vara_uchar(int ncid, int varid, const size_t *startp,
		  const size_t *countp, unsigned char *ip)
{
   return NC_get_vara(ncid, varid, startp, countp, (void *)ip, T_uchar);
}

int
nc_get_vara_short(int ncid, int varid, const size_t *startp,
		  const size_t *countp, short *ip)
{
   return NC_get_vara(ncid, varid, startp, countp, (void *)ip, NC_SHORT);
}

int
nc_get_vara_int(int ncid, int varid,
		const size_t *startp, const size_t *countp, int *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,NC_INT);
}

int
nc_get_vara_long(int ncid, int varid,
		 const size_t *startp, const size_t *countp, long *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,T_long);
}

int
nc_get_vara_float(int ncid, int varid,
		  const size_t *startp, const size_t *countp, float *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,T_float);
}

int
nc_get_vara_double(int ncid, int varid, const size_t *startp,
		   const size_t *countp, double *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,T_double);
}

int
nc_get_vara_ubyte(int ncid, int varid,
		  const size_t *startp, const size_t *countp, unsigned char *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,T_ubyte);
}

int
nc_get_vara_ushort(int ncid, int varid,
		   const size_t *startp, const size_t *countp, unsigned short *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,T_ushort);
}

int
nc_get_vara_uint(int ncid, int varid,
		 const size_t *startp, const size_t *countp, unsigned int *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,T_uint);
}

int
nc_get_vara_longlong(int ncid, int varid,
		     const size_t *startp, const size_t *countp, long long *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,T_longlong);
}

int
nc_get_vara_ulonglong(int ncid, int varid, const size_t *startp,
                      const size_t *countp, unsigned long long *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,NC_UINT64);
}

int
nc_get_vara_string(int ncid, int varid, const size_t *startp,
                   const size_t *countp, char* *ip)
{
   return NC_get_vara(ncid,varid,startp,countp, (void *)ip,NC_STRING);
}

/**@}*/

/** \ingroup variables
Read a single datum from a variable.

Inputs are the netCDF ID, the variable ID, a multidimensional index
that specifies which value to get, and the address of a location into
which the data value will be read. The value is converted from the
external data type of the variable, if necessary.

The nc_get_var1() function will read a variable of any type, including
user defined type. For this function, the type of the data in memory
must match the type of the variable - no data conversion is done.

Other nc_get_var1_ functions will convert data to the desired output
type as needed.

\param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().

\param varid Variable ID

\param indexp Index vector with one element for each dimension.

\param ip Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
*/
/** \{ */
int
nc_get_var1(int ncid, int varid, const size_t *indexp, void *ip)
{
   return NC_get_var1(ncid, varid, indexp, ip, NC_NAT);
}

int
nc_get_var1_text(int ncid, int varid, const size_t *indexp, char *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_CHAR);
}

int
nc_get_var1_schar(int ncid, int varid, const size_t *indexp, signed char *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_BYTE);
}

int
nc_get_var1_uchar(int ncid, int varid, const size_t *indexp, unsigned char *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_UBYTE);
}

int
nc_get_var1_short(int ncid, int varid, const size_t *indexp, short *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_SHORT);
}

int
nc_get_var1_int(int ncid, int varid, const size_t *indexp, int *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_INT);
}

int
nc_get_var1_long(int ncid, int varid, const size_t *indexp,
		 long *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, longtype);
}

int
nc_get_var1_float(int ncid, int varid, const size_t *indexp,
		  float *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_FLOAT);
}

int
nc_get_var1_double(int ncid, int varid, const size_t *indexp,
		   double *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_DOUBLE);
}

int
nc_get_var1_ubyte(int ncid, int varid, const size_t *indexp,
		  unsigned char *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_UBYTE);
}

int
nc_get_var1_ushort(int ncid, int varid, const size_t *indexp,
		   unsigned short *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_USHORT);
}

int
nc_get_var1_uint(int ncid, int varid, const size_t *indexp,
		 unsigned int *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_UINT);
}

int
nc_get_var1_longlong(int ncid, int varid, const size_t *indexp,
		     long long *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_INT64);
}

int
nc_get_var1_ulonglong(int ncid, int varid, const size_t *indexp,
		      unsigned long long *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_UINT64);
}

int
nc_get_var1_string(int ncid, int varid, const size_t *indexp, char* *ip)
{
   return NC_get_var1(ncid, varid, indexp, (void *)ip, NC_STRING);
}

/** \} */

/** \ingroup variables
Read an entire variable in one call.

This function will read all the values from a netCDF variable of an
open netCDF dataset.

This is the simplest interface to use for reading the value of a
scalar variable or when all the values of a multidimensional variable
can be read at once. The values are read into consecutive locations
with the last dimension varying fastest. The netCDF dataset must be in
data mode.

Take care when using this function with record variables (variables
that use the ::NC_UNLIMITED dimension). If you try to read all the
values of a record variable into an array but there are more records
in the file than you assume, more data will be read than you expect,
which may cause a segmentation violation. To avoid such problems, it
is better to use the nc_get_vara interfaces for variables that use the
::NC_UNLIMITED dimension.

The functions for types ubyte, ushort, uint, longlong, ulonglong, and
string are only available for netCDF-4/HDF5 files.

The nc_get_var() function will read a variable of any type, including
user defined type. For this function, the type of the data in memory
must match the type of the variable - no data conversion is done.

\param ncid NetCDF or group ID, from a previous call to nc_open(),
nc_create(), nc_def_grp(), or associated inquiry functions such as
nc_inq_ncid().

\param varid Variable ID

\param ip Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
*/
/** \{ */
int
nc_get_var(int ncid, int varid, void *ip)
{
   return NC_get_var(ncid, varid, ip, NC_NAT);
}

int
nc_get_var_text(int ncid, int varid, char *ip)
{
   return NC_get_var(ncid, varid, (void *)ip, NC_CHAR);
}

int
nc_get_var_schar(int ncid, int varid, signed char *ip)
{
   return NC_get_var(ncid, varid, (void *)ip, NC_BYTE);
}

int
nc_get_var_uchar(int ncid, int varid, unsigned char *ip)
{
   return NC_get_var(ncid,varid, (void *)ip, NC_UBYTE);
}

int
nc_get_var_short(int ncid, int varid, short *ip)
{
   return NC_get_var(ncid, varid, (void *)ip, NC_SHORT);
}

int
nc_get_var_int(int ncid, int varid, int *ip)
{
   return NC_get_var(ncid,varid, (void *)ip, NC_INT);
}

int
nc_get_var_long(int ncid, int varid, long *ip)
{
   return NC_get_var(ncid,varid, (void *)ip, longtype);
}

int
nc_get_var_float(int ncid, int varid, float *ip)
{
   return NC_get_var(ncid,varid, (void *)ip, NC_FLOAT);
}

int
nc_get_var_double(int ncid, int varid, double *ip)
{
   return NC_get_var(ncid,varid, (void *)ip, NC_DOUBLE);
}

int
nc_get_var_ubyte(int ncid, int varid, unsigned char *ip)
{
   return NC_get_var(ncid,varid, (void *)ip, NC_UBYTE);
}

int
nc_get_var_ushort(int ncid, int varid, unsigned short *ip)
{
   return NC_get_var(ncid,varid, (void *)ip, NC_USHORT);
}

int
nc_get_var_uint(int ncid, int varid, unsigned int *ip)
{
   return NC_get_var(ncid,varid, (void *)ip, NC_UINT);
}

int
nc_get_var_longlong(int ncid, int varid, long long *ip)
{
   return NC_get_var(ncid,varid, (void *)ip, NC_INT64);
}

int
nc_get_var_ulonglong(int ncid, int varid, unsigned long long *ip)
{
   return NC_get_var(ncid,varid, (void *)ip,NC_UINT64);
}

int
nc_get_var_string(int ncid, int varid, char* *ip)
{
   return NC_get_var(ncid,varid, (void *)ip,NC_STRING);
}
/** \} */

/** \ingroup variables
Read a strided array from a variable.

This function reads a subsampled (strided) array section of values
from a netCDF variable of an open netCDF dataset. The subsampled array
section is specified by giving a corner, a vector of edge lengths, and
a stride vector. The values are read with the last dimension of the
netCDF variable varying fastest. The netCDF dataset must be in data
mode.

The nc_get_vars() function will read a variable of any type, including
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

\param stridep Stride vector with one element for each dimension to
\ref specify_hyperslab. This array must be same size as variable's
number of dimensions.

\param ip Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
*/
/** \{ */
int
nc_get_vars(int ncid, int varid, const size_t * startp,
            const size_t * countp, const ptrdiff_t * stridep,
            void *ip)
{
   return NC_get_vars(ncid, varid, startp, countp, stridep,
		      ip, NC_NAT);
}

int
nc_get_vars_text(int ncid, int varid, const size_t *startp,
		 const size_t *countp, const ptrdiff_t * stridep,
		 char *ip)
{
   return NC_get_vars(ncid,varid,startp, countp, stridep,
		      (void *)ip, NC_CHAR);
}

int
nc_get_vars_schar(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const ptrdiff_t * stridep,
		  signed char *ip)
{
   return NC_get_vars(ncid,varid,startp, countp, stridep,
		      (void *)ip, NC_BYTE);
}

int
nc_get_vars_uchar(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const ptrdiff_t * stridep,
		  unsigned char *ip)
{
   return NC_get_vars(ncid,varid,startp, countp, stridep,
		      (void *)ip, T_uchar);
}

int
nc_get_vars_short(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const ptrdiff_t *stridep,
		  short *ip)
{
   return NC_get_vars(ncid,varid,startp, countp, stridep,
		      (void *)ip, NC_SHORT);
}

int
nc_get_vars_int(int ncid, int varid, const size_t *startp,
		const size_t *countp, const ptrdiff_t * stridep,
		int *ip)
{
   return NC_get_vars(ncid,varid,startp, countp, stridep,
		      (void *)ip, NC_INT);
}

int
nc_get_vars_long(int ncid, int varid, const size_t *startp,
		 const size_t *countp, const ptrdiff_t * stridep,
		 long *ip)
{
   return NC_get_vars(ncid,varid,startp, countp, stridep,
		      (void *)ip, T_long);
}

int
nc_get_vars_float(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const ptrdiff_t * stridep,
		  float *ip)
{
   return NC_get_vars(ncid,varid,startp, countp, stridep,
		      (void *)ip, T_float);
}

int
nc_get_vars_double(int ncid, int varid, const size_t *startp,
		   const size_t *countp, const ptrdiff_t * stridep,
		   double *ip)
{
   return NC_get_vars(ncid,varid,startp, countp, stridep,
		      (void *)ip, T_double);
}

int
nc_get_vars_ubyte(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const ptrdiff_t * stridep,
		  unsigned char *ip)
{
   return NC_get_vars(ncid,varid, startp, countp, stridep,
		      (void *)ip, T_ubyte);
}

int
nc_get_vars_ushort(int ncid, int varid, const size_t *startp,
		   const size_t *countp, const ptrdiff_t * stridep,
		   unsigned short *ip)
{
   return NC_get_vars(ncid,varid,startp,countp, stridep,
		      (void *)ip, T_ushort);
}

int
nc_get_vars_uint(int ncid, int varid, const size_t *startp,
		 const size_t *countp, const ptrdiff_t * stridep,
		 unsigned int *ip)
{
   return NC_get_vars(ncid,varid,startp, countp, stridep,
		      (void *)ip, T_uint);
}

int
nc_get_vars_longlong(int ncid, int varid, const size_t *startp,
		     const size_t *countp, const ptrdiff_t * stridep,
		     long long *ip)
{
   return NC_get_vars(ncid, varid, startp, countp, stridep,
		      (void *)ip, T_longlong);
}

int
nc_get_vars_ulonglong(int ncid, int varid, const size_t *startp,
		      const size_t *countp, const ptrdiff_t * stridep,
		      unsigned long long *ip)
{
   return NC_get_vars(ncid, varid, startp, countp, stridep,
		      (void *)ip, NC_UINT64);
}

int
nc_get_vars_string(int ncid, int varid,
		   const size_t *startp, const size_t *countp,
		   const ptrdiff_t * stridep,
		   char* *ip)
{
   return NC_get_vars(ncid, varid, startp, countp, stridep,
		      (void *)ip, NC_STRING);
}

/** \} */

/** \ingroup variables
Read a mapped array from a variable.

The nc_get_varm_ type family of functions reads a mapped array section
of values from a netCDF variable of an open netCDF dataset. The mapped
array section is specified by giving a corner, a vector of edge
lengths, a stride vector, and an index mapping vector. The index
mapping vector is a vector of integers that specifies the mapping
between the dimensions of a netCDF variable and the in-memory
structure of the internal data array. No assumptions are made about
the ordering or length of the dimensions of the data array. The netCDF
dataset must be in data mode.

The functions for types ubyte, ushort, uint, longlong, ulonglong, and
string are only available for netCDF-4/HDF5 files.

The nc_get_varm() function will only read a variable of an
atomic type; it will not read user defined types. For this
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

\param ip Pointer where the data will be copied. Memory must be
allocated by the user before this function is called.

\returns ::NC_NOERR No error.
\returns ::NC_ENOTVAR Variable not found.
\returns ::NC_EINVALCOORDS Index exceeds dimension bound.
\returns ::NC_ERANGE One or more of the values are out of range.
\returns ::NC_EINDEFINE Operation not allowed in define mode.
\returns ::NC_EBADID Bad ncid.
\author Glenn Davis, Russ Rew, Ed Hartnett, Dennis Heimbigner, Ward Fisher
*/
/** \{ */
int
nc_get_varm(int ncid, int varid, const size_t * startp,
	    const size_t * countp, const ptrdiff_t * stridep,
	    const ptrdiff_t * imapp, void *ip)
{
   return NC_get_varm(ncid, varid, startp, countp, stridep, imapp, ip, NC_NAT);
}

int
nc_get_varm_schar(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep,
		  const ptrdiff_t *imapp, signed char *ip)
{
   return NC_get_varm(ncid, varid, startp, countp,
		      stridep, imapp, (void *)ip, NC_BYTE);
}

int
nc_get_varm_uchar(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		  unsigned char *ip)
{
   return NC_get_varm(ncid,varid,startp,countp,stridep,imapp, (void *)ip,T_uchar);
}

int
nc_get_varm_short(int ncid, int varid, const size_t *startp,
		  const size_t *countp, const ptrdiff_t *stridep,
		  const ptrdiff_t *imapp, short *ip)
{
   return NC_get_varm(ncid,varid,startp,countp,stridep,imapp, (void *)ip,NC_SHORT);
}

int
nc_get_varm_int(int ncid, int varid,
		const size_t *startp, const size_t *countp,
		const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		int *ip)
{
   return NC_get_varm(ncid,varid,startp,countp,stridep,imapp, (void *)ip,NC_INT);
}

int
nc_get_varm_long(int ncid, int varid,
		 const size_t *startp, const size_t *countp,
		 const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		 long *ip)
{
   return NC_get_varm(ncid,varid,startp,countp,stridep,imapp, (void *)ip,T_long);
}

int
nc_get_varm_float(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		  float *ip)
{
   return NC_get_varm(ncid,varid,startp,countp,stridep,imapp, (void *)ip,T_float);
}

int
nc_get_varm_double(int ncid, int varid,
		   const size_t *startp, const size_t *countp,
		   const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		   double *ip)
{
   return NC_get_varm(ncid,varid,startp,countp,stridep,imapp, (void *)ip,T_double);
}

int
nc_get_varm_ubyte(int ncid, int varid,
		  const size_t *startp, const size_t *countp,
		  const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		  unsigned char *ip)
{
   return NC_get_varm(ncid,varid,startp,countp,stridep,
		      imapp, (void *)ip, T_ubyte);
}

int
nc_get_varm_ushort(int ncid, int varid,
		   const size_t *startp, const size_t *countp,
		   const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		   unsigned short *ip)
{
   return NC_get_varm(ncid, varid, startp, countp, stridep,
		      imapp, (void *)ip, T_ushort);
}

int
nc_get_varm_uint(int ncid, int varid,
		 const size_t *startp, const size_t *countp,
		 const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		 unsigned int *ip)
{
   return NC_get_varm(ncid, varid, startp, countp,
		      stridep, imapp, (void *)ip, T_uint);
}

int
nc_get_varm_longlong(int ncid, int varid, const size_t *startp,
		     const size_t *countp, const ptrdiff_t *stridep,
		     const ptrdiff_t *imapp, long long *ip)
{
   return NC_get_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)ip, T_longlong);
}

int
nc_get_varm_ulonglong(int ncid, int varid,
		      const size_t *startp, const size_t *countp,
		      const ptrdiff_t *stridep, const ptrdiff_t *imapp,
		      unsigned long long *ip)
{
   return NC_get_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)ip, NC_UINT64);
}

int
nc_get_varm_text(int ncid, int varid, const size_t *startp,
		 const size_t *countp, const ptrdiff_t *stridep,
		 const ptrdiff_t *imapp, char *ip)
{
   return NC_get_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)ip, NC_CHAR);
}

int
nc_get_varm_string(int ncid, int varid, const size_t *startp,
		   const size_t *countp, const ptrdiff_t *stridep,
		   const ptrdiff_t *imapp, char **ip)
{
   return NC_get_varm(ncid, varid, startp, countp, stridep, imapp,
		      (void *)ip, NC_STRING);
}
/** \} */


/*! \} */ /* End of named group... */
