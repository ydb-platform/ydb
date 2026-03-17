/**
 * @file
 * Copyright 2018 University Corporation for Atmospheric
 * Research/Unidata. See COPYRIGHT file for more info.
 *
 * This file has the var and att copy functions.
 *
 * @author Dennis Heimbigner
*/
#include "config.h"
#include "ncdispatch.h"
#include "nc_logging.h"
#include "nclist.h"

static int NC_find_equal_type(int ncid1, nc_type xtype1, int ncid2, nc_type *xtype2);

#ifdef USE_NETCDF4

static int searchgroup(int ncid1, int tid1, int grp, int* tid2);
static int searchgrouptree(int ncid1, int tid1, int grp, int* tid2);

#endif /*USE_NETCDF4*/


#ifdef USE_NETCDF4
/**
 * @internal Compare two netcdf types for equality. Must have the
 * ncids as well, to find user-defined types.
 *
 * @param ncid1 File ID.
 * @param typeid1 Type ID.
 * @param ncid2 File ID.
 * @param typeid2 Type ID.
 * @param equalp Pointer that gets 1 of the types are equal, 0
 * otherwise.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
*/
static int
NC_compare_nc_types(int ncid1, int typeid1, int ncid2, int typeid2, int *equalp)
{
   int ret = NC_NOERR;

   /* If you don't care about the answer, neither do I! */
   if(equalp == NULL)
      return NC_NOERR;

   /* Assume the types are not equal. If we find any inequality, then
      exit with NC_NOERR and we're done. */
   *equalp = 0;

   /* Atomic types are so easy! */
   if (typeid1 <= NC_MAX_ATOMIC_TYPE)
   {
      if (typeid2 != typeid1)
	 return NC_NOERR;
      *equalp = 1;
   }
   else
   {
      size_t i;
      int ret, equal1;
      char name1[NC_MAX_NAME];
      char name2[NC_MAX_NAME];
      size_t size1, size2;
      nc_type base1, base2;
      size_t nelems1, nelems2;
      int class1, class2;
      void* value1 = NULL;
      void* value2 = NULL;
      size_t offset1, offset2;
      nc_type ftype1, ftype2;
      int ndims1, ndims2;
      int dimsizes1[NC_MAX_VAR_DIMS];
      int dimsizes2[NC_MAX_VAR_DIMS];

      /* Find out about the two types. */
      if ((ret = nc_inq_user_type(ncid1, typeid1, name1, &size1,
				  &base1, &nelems1, &class1)))
	 return ret;
      if ((ret = nc_inq_user_type(ncid2, typeid2, name2, &size2,
				  &base2, &nelems2, &class2)))
	 return ret;

      /* Check the obvious. */
      if(size1 != size2 || class1 != class2 || strcmp(name1,name2) != 0)
	 return NC_NOERR;

      /* Check user-defined types in detail. */
      switch(class1)
      {
	 case NC_VLEN:
	    if((ret = NC_compare_nc_types(ncid1, base1, ncid2,
					  base1, &equal1)))
	       return ret;
	    if(!equal1)
	       return NC_NOERR;
	    break;
	 case NC_OPAQUE:
	    /* Already checked size above. */
	    break;
	 case NC_ENUM:
	    if(base1 != base2 || nelems1 != nelems2) return NC_NOERR;

	    if (!(value1 = malloc(size1)))
	       return NC_ENOMEM;
	    if (!(value2 = malloc(size2))) {
          free(value1);
          return NC_ENOMEM;
        }

	    for(i = 0; i < nelems1; i++)
	    {
	       if ((ret = nc_inq_enum_member(ncid1, typeid1, i, name1,
					     value1)) ||
		   (ret = nc_inq_enum_member(ncid2, typeid2, i, name2,
					     value2)) ||
		   strcmp(name1, name2) != 0 || memcmp(value1, value2, size1) != 0)
	       {
		  free(value1);
		  free(value2);
		  return ret;
	       }
	    }
	    free(value1);
	    free(value2);
	    break;
	 case NC_COMPOUND:
	    if(nelems1 != nelems2)
	       return NC_NOERR;

	    /* Compare each field. Each must be equal! */
	    for(i = 0; i < nelems1; i++)
	    {
	       int j;
	       if ((ret = nc_inq_compound_field(ncid1, typeid1, i, name1, &offset1,
						&ftype1, &ndims1, dimsizes1)))
		  return ret;
	       if ((ret = nc_inq_compound_field(ncid2, typeid2, i, name2, &offset2,
						&ftype2, &ndims2, dimsizes2)))
		  return ret;
	       if(ndims1 != ndims2)
		  return NC_NOERR;
	       for(j = 0; j < ndims1;j++)
		  if(dimsizes1[j] != dimsizes2[j])
		     return NC_NOERR;

	       /* Compare user-defined field types. */
	       if((ret = NC_compare_nc_types(ncid1, ftype1, ncid2, ftype2,
					     &equal1)))
		  return ret;
	       if(!equal1)
		  return NC_NOERR;
	    }
	    break;
	 default:
	    return NC_EINVAL;
      }
      *equalp = 1;
   }
   return ret;
}

/**
 * @internal Recursively hunt for a netCDF type id, tid2, that is "equal" to tid1.
 * Question is: what search order do we use? Ncgen uses root group tree in pre-order.
 * But NC4_inq_typeid uses these rules:
 * 1. ncid2
 * 2. parents of ncid2 (up the tree to root)
 * 3. root group tree in pre-order.
 * We will leave ncgen for another day and use the nc_inq_typeid rule.
 *
 * Return matching typeid or 0 if not found.
 *
 * @param ncid1 File ID.
 * @param tid1 Type ID.
 * @param ncid2 File ID.
 * @param tid2 Pointer that gets type ID of equal type.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Dennis Heimbigner
*/
static int
NC_rec_find_nc_type(int ncid1, nc_type tid1, int ncid2, nc_type* tid2)
{
    int ret = NC_NOERR;
    int parent;

    if((ret = searchgroup(ncid1,tid1,ncid2,tid2)))
        goto done;
    if(*tid2 != 0)
        goto done; /* found */

   /* Look in the parents of ncid2 upto the root */
   switch (ret = nc_inq_grp_parent(ncid2,&parent)) {
   case NC_NOERR:
	/* Recurse up using parent grp */
        ret = NC_rec_find_nc_type(ncid1, tid1, parent, tid2);
	break;
   case NC_ENOGRP:
	/* do the breadth-first pre-order search of the whole tree */
	/* ncid2 should be root group */
	ret = searchgrouptree(ncid1,tid1,ncid2,tid2);
	break;
   default: break;
   }

done:
    return ret;
}

#endif /* USE_NETCDF4 */

/**
 * @internal Given a type in one file, find its equal (if any) in
 * another file. It sounds so simple, but it's a real pain!
 *
 * @param ncid1 File ID.
 * @param xtype1 Type ID.
 * @param ncid2 File ID.
 * @param xtype2 Pointer that gets type ID of equal type.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
*/
static int
NC_find_equal_type(int ncid1, nc_type xtype1, int ncid2, nc_type *xtype2)
{
   int ret = NC_NOERR;

   /* Check input */
   if(xtype1 <= NC_NAT)
      return NC_EINVAL;

   /* Handle atomic types. */
   if (xtype1 <= NC_MAX_ATOMIC_TYPE)
   {
      if(xtype2)
	 *xtype2 = xtype1;
      return NC_NOERR;
   }

#ifdef USE_NETCDF4
   /* Recursively search group ncid2 and its children
      to find a type that is equal (using compare_type)
      to xtype1. */
   ret = NC_rec_find_nc_type(ncid1, xtype1 , ncid2, xtype2);
#endif /* USE_NETCDF4 */
   return ret;
}

/**
 * This will copy a variable that is an array of primitive type and
 * its attributes from one file to another, assuming dimensions in the
 * output file are already defined and have same dimension IDs and
 * length.  However it doesn't work for copying netCDF-4 variables of
 * type string or a user-defined type.
 *
 * This function works even if the files are different formats,
 * (for example, one netcdf classic, the other netcdf-4).
 *
 * If you're copying into a classic-model file, from a netcdf-4 file,
 * you must be copying a variable of one of the six classic-model
 * types, and similarly for the attributes.
 *
 * For large netCDF-3 files, this can be a very inefficient way to
 * copy data from one file to another, because adding a new variable
 * to the target file may require more space in the header and thus
 * result in moving data for other variables in the target file. This
 * is not a problem for netCDF-4 files, which support efficient
 * addition of variables without moving data for other variables.
 *
 * @param ncid_in File ID to copy from.
 * @param varid_in Variable ID to copy.
 * @param ncid_out File ID to copy to.
 *
 * @return ::NC_NOERR No error.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_copy_var(int ncid_in, int varid_in, int ncid_out)
{
   char name[NC_MAX_NAME + 1];
   char att_name[NC_MAX_NAME + 1];
   nc_type xtype;
   int ndims; 
   int dimids_in[NC_MAX_VAR_DIMS], dimids_out[NC_MAX_VAR_DIMS];
   int natts, real_ndims;
   int varid_out;
   int a, d;
   void *data = NULL;
   size_t *count = NULL, *start = NULL;
   size_t reclen = 1;
   size_t *dimlen = NULL;
   int retval = NC_NOERR;
   size_t type_size;
   int src_format, dest_format;
   char type_name[NC_MAX_NAME+1];
   char dimname_in[NC_MAX_NAME + 1];
   size_t i;

   /* Learn about this var. */
   if ((retval = nc_inq_var(ncid_in, varid_in, name, &xtype,
                            &ndims, dimids_in, &natts)))
      return retval;
   /* find corresponding dimids in the output file */
   for(i = 0; i < (size_t)ndims; i++) {
      dimids_out[i] = dimids_in[i];
      if ((retval = nc_inq_dimname(ncid_in, dimids_in[i], dimname_in)))
         return retval;
      if ((retval = nc_inq_dimid(ncid_out, dimname_in, &dimids_out[i])))
         return retval;
   }

   LOG((2, "nc_copy_var: ncid_in 0x%x varid_in %d ncid_out 0x%x",
        ncid_in, varid_in, ncid_out));

   /* Make sure we are not trying to write into a netcdf-3 file
    * anything that won't fit in netcdf-3. */
   if ((retval = nc_inq_format(ncid_in, &src_format)))
      return retval;
   if ((retval = nc_inq_format(ncid_out, &dest_format)))
      return retval;
   if ((dest_format == NC_FORMAT_CLASSIC
        || dest_format == NC_FORMAT_64BIT_DATA
        || dest_format == NC_FORMAT_64BIT_OFFSET) &&
       src_format == NC_FORMAT_NETCDF4 && xtype > NC_DOUBLE)
      return NC_ENOTNC4;

   /* Later on, we will need to know the size of this type. */
   if ((retval = nc_inq_type(ncid_in, xtype, type_name, &type_size)))
      return retval;
   LOG((3, "type %s has size %d", type_name, type_size));

   /* Switch back to define mode, and create the output var. */
   retval = nc_redef(ncid_out);
   if (retval && retval != NC_EINDEFINE)
      BAIL(retval);
   if ((retval = nc_def_var(ncid_out, name, xtype,
                            ndims, dimids_out, &varid_out)))
      BAIL(retval);

   /* Copy the attributes. */
   for (a=0; a<natts; a++)
   {
      if ((retval = nc_inq_attname(ncid_in, varid_in, a, att_name)))
         BAIL(retval);
      if ((retval = nc_copy_att(ncid_in, varid_in, att_name,
				ncid_out, varid_out)))
         BAIL(retval);
   }

   /* End define mode, to write metadata and create file. */
   nc_enddef(ncid_out);
   nc_sync(ncid_out);

   /* Allocate memory for our start and count arrays. If ndims = 0
      this is a scalar, which I will treat as a 1-D array with one
      element. */
   real_ndims = ndims ? ndims : 1;
   if (!(start = malloc((size_t)real_ndims * sizeof(size_t))))
      BAIL(NC_ENOMEM);
   if (!(count = malloc((size_t)real_ndims * sizeof(size_t))))
      BAIL(NC_ENOMEM);

   /* The start array will be all zeros, except the first element,
      which will be the record number. Count will be the dimension
      size, except for the first element, which will be one, because
      we will copy one record at a time. For this we need the var
      shape. */
   if (!(dimlen = malloc((size_t)real_ndims * sizeof(size_t))))
      BAIL(NC_ENOMEM);

   /* Set to 0, to correct for an unlikely dereference
      error reported by clang/llvm. */
   dimlen[0] = 0;

   /* Find out how much data. */
   for (d=0; d<ndims; d++)
   {
      if ((retval = nc_inq_dimlen(ncid_in, dimids_in[d], &dimlen[d])))
         BAIL(retval);
      LOG((4, "nc_copy_var: there are %d data", dimlen[d]));
   }

   /* If this is really a scalar, then set the dimlen to 1. */
   if (ndims == 0)
      dimlen[0] = 1;

   for (d=0; d<real_ndims; d++)
   {
      start[d] = 0;
      count[d] = d ? dimlen[d] : 1;
      if (d) reclen *= dimlen[d];
   }

   /* If there are no records, we're done. */
   if (!dimlen[0])
      goto exit;

   /* Allocate memory for one record. */
   if (!(data = malloc(reclen * type_size))) {
     if(count) free(count);
     if(dimlen) free(dimlen);
     if(start) free(start);
     return NC_ENOMEM;
   }

   /* Copy the var data one record at a time. */
   for (start[0]=0; !retval && start[0]<(size_t)dimlen[0]; start[0]++)
   {
      switch (xtype)
      {
	 case NC_BYTE:
	    retval = nc_get_vara_schar(ncid_in, varid_in, start, count,
				       (signed char *)data);
	    if (!retval)
	       retval = nc_put_vara_schar(ncid_out, varid_out, start, count,
					  (const signed char *)data);
	    break;
	 case NC_CHAR:
	    retval = nc_get_vara_text(ncid_in, varid_in, start, count,
				      (char *)data);
	    if (!retval)
	       retval = nc_put_vara_text(ncid_out, varid_out, start, count,
					 (char *)data);
	    break;
	 case NC_SHORT:
	    retval = nc_get_vara_short(ncid_in, varid_in, start, count,
				       (short *)data);
	    if (!retval)
	       retval = nc_put_vara_short(ncid_out, varid_out, start, count,
					  (short *)data);
	    break;
	 case NC_INT:
	    retval = nc_get_vara_int(ncid_in, varid_in, start, count,
				     (int *)data);
	    if (!retval)
	       retval = nc_put_vara_int(ncid_out, varid_out, start, count,
					(int *)data);
	    break;
	 case NC_FLOAT:
	    retval = nc_get_vara_float(ncid_in, varid_in, start, count,
				       (float *)data);
	    if (!retval)
	       retval = nc_put_vara_float(ncid_out, varid_out, start, count,
					  (float *)data);
	    break;
	 case NC_DOUBLE:
	    retval = nc_get_vara_double(ncid_in, varid_in, start, count,
					(double *)data);
	    if (!retval)
	       retval = nc_put_vara_double(ncid_out, varid_out, start, count,
					   (double *)data);
	    break;
	 case NC_UBYTE:
	    retval = nc_get_vara_uchar(ncid_in, varid_in, start, count,
				       (unsigned char *)data);
	    if (!retval)
	       retval = nc_put_vara_uchar(ncid_out, varid_out, start, count,
					  (unsigned char *)data);
	    break;
	 case NC_USHORT:
	    retval = nc_get_vara_ushort(ncid_in, varid_in, start, count,
					(unsigned short *)data);
	    if (!retval)
	       retval = nc_put_vara_ushort(ncid_out, varid_out, start, count,
					   (unsigned short *)data);
	    break;
	 case NC_UINT:
	    retval = nc_get_vara_uint(ncid_in, varid_in, start, count,
				      (unsigned int *)data);
	    if (!retval)
	       retval = nc_put_vara_uint(ncid_out, varid_out, start, count,
					 (unsigned int *)data);
	    break;
	 case NC_INT64:
	    retval = nc_get_vara_longlong(ncid_in, varid_in, start, count,
					  (long long *)data);
	    if (!retval)
	       retval = nc_put_vara_longlong(ncid_out, varid_out, start, count,
					     (long long *)data);
	    break;
	 case NC_UINT64:
	    retval = nc_get_vara_ulonglong(ncid_in, varid_in, start, count,
					   (unsigned long long *)data);
	    if (!retval)
	       retval = nc_put_vara_ulonglong(ncid_out, varid_out, start, count,
					      (unsigned long long *)data);
	    break;
	 default:
	    retval = NC_EBADTYPE;
      }
   }

  exit:
   if (data) free(data);
   if (dimlen) free(dimlen);
   if (start) free(start);
   if (count) free(count);
   return retval;
}

/**
 * Copy an attribute from one open file to another. This is called by
 * nc_copy_att().
 *
 * @param ncid_in File ID to copy from.
 * @param varid_in Variable ID to copy from.
 * @param name Name of attribute to copy.
 * @param ncid_out File ID to copy to.
 * @param varid_out Variable ID to copy to.
 *
 * @return ::NC_NOERR No error.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
static int
NC_copy_att(int ncid_in, int varid_in, const char *name,
	    int ncid_out, int varid_out)
{
   nc_type xtype;
   size_t len;
   void *data=NULL;
   int res;

   LOG((2, "nc_copy_att: ncid_in 0x%x varid_in %d name %s",
	ncid_in, varid_in, name));

   /* Find out about the attribute to be copied. */
   if ((res = nc_inq_att(ncid_in, varid_in, name, &xtype, &len)))
      return res;

   {
	/* Copy arbitrary attributes. */
        int class;
        size_t size = 0;
        nc_type xtype_out = NC_NAT;

        if(xtype <= NC_MAX_ATOMIC_TYPE) {
	    xtype_out = xtype;
	    if((res = nc_inq_type(ncid_out,xtype_out,NULL,&size))) return res;
	} else { /* User defined type */
            /* Find out if there is an equal type in the output file. */
            /* Note: original code used a libsrc4 specific internal function
   	       which we had to "duplicate" here */
            if ((res = NC_find_equal_type(ncid_in, xtype, ncid_out, &xtype_out)))
  	        return res;
            if (xtype_out) {
		/* We found an equal type! */
		if ((res = nc_inq_user_type(ncid_in, xtype, NULL, &size, NULL, NULL, &class)))
		    return res;
	    }
	}
        if((data = malloc(size * len))==NULL) {return NC_ENOMEM;}
        res = nc_get_att(ncid_in, varid_in, name, data);
	if(!res)
	    res = nc_put_att(ncid_out, varid_out, name, xtype_out, len, data);
	(void)nc_reclaim_data_all(ncid_out,xtype_out,data,len);
      }

   return res;
}

/**
 * Copy an attribute from one open file to another.
 *
 * Special programming challenge: this function must work even if one
 * of the other of the files is a netcdf version 1.0 file (i.e. not
 * HDF5). So only use top level netcdf api functions.
 *
 * From the netcdf-3 docs: The output netCDF dataset should be in
 * define mode if the attribute to be copied does not already exist
 * for the target variable, or if it would cause an existing target
 * attribute to grow.
 *
 * @param ncid_in File ID to copy from.
 * @param varid_in Variable ID to copy from.
 * @param name Name of attribute to copy.
 * @param ncid_out File ID to copy to.
 * @param varid_out Variable ID to copy to.
 *
 * @return ::NC_NOERR No error.
 * @author Glenn Davis, Ed Hartnett, Dennis Heimbigner
*/
int
nc_copy_att(int ncid_in, int varid_in, const char *name,
	    int ncid_out, int varid_out)
{
   int format, target_natts, target_attid;
   char att_name[NC_MAX_NAME + 1];
   int a, retval;

   /* What is the destination format? */
   if ((retval = nc_inq_format(ncid_out, &format)))
      return retval;

   /* Can't copy to same var in same file. */
   if (ncid_in == ncid_out && varid_in == varid_out)
      return NC_NOERR;

   /* For classic model netCDF-4 files, order of attributes must be
    * maintained during copies. We MUST MAINTAIN ORDER! */
   if (format == NC_FORMAT_NETCDF4_CLASSIC)
   {
      /* Does this attribute already exist in the target file? */
      retval = nc_inq_attid(ncid_out, varid_out, name, &target_attid);
      if (retval == NC_ENOTATT)
      {
	 /* Attribute does not exist. No order to be preserved. */
	 return NC_copy_att(ncid_in, varid_in, name, ncid_out, varid_out);
      }
      else if (retval == NC_NOERR)
      {
	 /* How many atts for this var? */
	 if ((retval = nc_inq_varnatts(ncid_out, varid_out, &target_natts)))
	    return retval;

	 /* If this is the last attribute in the target file, we are
	  * off the hook. */
	 if (target_attid == target_natts - 1)
	    return NC_copy_att(ncid_in, varid_in, name, ncid_out, varid_out);

	 /* Order MUST BE MAINTAINED! Copy all existing atts in the target
	  * file, stopping at our target att. */
	 for (a = 0; a < target_natts; a++)
	 {
	    if (a == target_attid)
	    {
	       if ((retval = NC_copy_att(ncid_in, varid_in, name, ncid_out, varid_out)))
		  return retval;
	    }
	    else
	    {
	       if ((retval = nc_inq_attname(ncid_out, varid_out, a, att_name)))
		  return retval;
	       if ((retval = NC_copy_att(ncid_out, varid_out, att_name,
					 ncid_out, varid_out)))
		  return retval;
	    }
	 }
      }
      else
	 return retval; /* Some other error occurred. */
   }
   else
      return NC_copy_att(ncid_in, varid_in, name, ncid_out, varid_out);

   return NC_NOERR;
}

#ifdef USE_NETCDF4

/* Helper function for NC_rec_find_nc_type();
   search a specified group for matching type.
*/
static int
searchgroup(int ncid1, int tid1, int grp, int* tid2)
{
    int i,ret = NC_NOERR;
    int nids;
    int* ids = NULL;

    /* Get all types in grp */
    if(tid2)
	*tid2 = 0;
    if ((ret = nc_inq_typeids(grp, &nids, NULL)))
	goto done;
    if (nids)
    {
	if (!(ids = (int *)malloc((size_t)nids * sizeof(int))))
	    {ret = NC_ENOMEM; goto done;}
	if ((ret = nc_inq_typeids(grp, &nids, ids)))
	    goto done;
	for(i = 0; i < nids; i++)
        {
	    int equal = 0;
	    if ((ret = NC_compare_nc_types(ncid1, tid1, grp, ids[i], &equal)))
	        goto done;
	    if(equal)
	    {
		if(tid2)
		    *tid2 = ids[i];
		goto done;
	    }
	}
    }

done:
    nullfree(ids);
    return ret;
}

/* Helper function for NC_rec_find_nc_type();
   search a tree of groups for a matching type
   using a breadth first queue
*/
static int
searchgrouptree(int ncid1, int tid1, int grp, int* tid2)
{
    int i,ret = NC_NOERR;
    int nids;
    int* ids = NULL;
    NClist* queue = nclistnew();
    int gid;
    uintptr_t id;

    id = (uintptr_t)grp;
    nclistpush(queue,(void*)id); /* prime the queue */
    while(nclistlength(queue) > 0) {
        id = (uintptr_t)nclistremove(queue,0);
	gid  = (int)id;
        if((ret = searchgroup(ncid1,tid1,gid,tid2)))
            goto done;
        if(*tid2 != 0)
            goto done; /*we found it*/
	/* Get subgroups of gid and push onto front of the queue (for breadth first) */
        if((ret = nc_inq_grps(gid,&nids,NULL)))
            goto done;
        if (!(ids = (int *)malloc((size_t)nids * sizeof(int))))
	    {ret = NC_ENOMEM; goto done;}
        if ((ret = nc_inq_grps(gid, &nids, ids)))
            goto done;
	/* push onto the end of the queue */
        for(i=0;i<nids;i++) {
	    id = (uintptr_t)ids[i];
	    nclistpush(queue,(void*)id);
	}
	free(ids); ids = NULL;
    }
    /* Not found */
    ret = NC_EBADTYPE;

done:
    nclistfree(queue);
    nullfree(ids);
    return ret;
}

#endif /* USE_NETCDF4 */

