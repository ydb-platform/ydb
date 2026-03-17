/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
/* $Id: var.c,v 1.144 2010/05/30 00:50:35 russ Exp $ */

#if HAVE_CONFIG_H
#include <config.h>
#endif

#include "nc3internal.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <sys/types.h>
#include "ncx.h"
#include "rnd.h"
#include "ncutf8.h"
#include "nc3dispatch.h"

#ifndef OFF_T_MAX
#if 0
#define OFF_T_MAX (~ (off_t) 0 - (~ (off_t) 0 << (CHAR_BIT * sizeof (off_t) - 1)))
#endif

/* The behavior above is undefined, re: bitshifting a negative value, according
   to warnings thrown by clang/gcc.  An alternative OFF_T_MAX was written
   based on info found at:
   * http://stackoverflow.com/questions/4514572/c-question-off-t-and-other-signed-integer-types-minimum-and-maximum-values
   */
#define MAX_INT_VAL_STEP(t) \
    ((t) 1 << (CHAR_BIT * sizeof(t) - 1 - ((t) -1 < 1)))

#define MAX_INT_VAL(t) \
    ((MAX_INT_VAL_STEP(t) - 1) + MAX_INT_VAL_STEP(t))

#define MIN_INT_VAL(t) \
    ((t) -MAX_INT_VAL(t) - 1)

#define OFF_T_MAX MAX_INT_VAL(off_t)

#endif

/*
 * Free var
 * Formerly NC_free_var(var)
 */
void
free_NC_var(NC_var *varp)
{
	if(varp == NULL)
		return;
	free_NC_attrarrayV(&varp->attrs);
	free_NC_string(varp->name);
#ifndef MALLOCHACK
	if(varp->dimids != NULL) free(varp->dimids);
	if(varp->shape != NULL) free(varp->shape);
	if(varp->dsizes != NULL) free(varp->dsizes);
#endif /*!MALLOCHACK*/
	free(varp);
}


/*
 * Common code for new_NC_var()
 * and ncx_get_NC_var()
 */
NC_var *
new_x_NC_var(
	NC_string *strp,
	size_t ndims)
{
	NC_var *varp;

	if (ndims > SIZE_MAX / sizeof(int))
		return NULL;
	const size_t o1 = M_RNDUP(ndims * sizeof(int));

	if (ndims > SIZE_MAX / sizeof(size_t))
		return NULL;
	const size_t o2 = M_RNDUP(ndims * sizeof(size_t));

#ifdef MALLOCHACK
	const size_t sz =  M_RNDUP(sizeof(NC_var)) +
		 o1 + o2 + ndims * sizeof(off_t);
#else /*!MALLOCHACK*/
	if (ndims > SIZE_MAX / sizeof(off_t))
		return NULL;
	const size_t o3 = ndims * sizeof(off_t);
	const size_t sz = sizeof(NC_var);
#endif /*!MALLOCHACK*/

	varp = (NC_var *) malloc(sz);
	if(varp == NULL )
		return NULL;
	(void) memset(varp, 0, sz);
	varp->name = strp;
	varp->ndims = ndims;

	if(ndims != 0)
	{
#ifdef MALLOCHACK
	  /*
	   * NOTE: lint may complain about the next 3 lines:
	   * "pointer cast may result in improper alignment".
	   * We use the M_RNDUP() macro to get the proper alignment.
	   */
	  varp->dimids = (int *)((char *)varp + M_RNDUP(sizeof(NC_var)));
	  varp->shape = (size_t *)((char *)varp->dimids + o1);
	  varp->dsizes = (off_t *)((char *)varp->shape + o2);
#else /*!MALLOCHACK*/
	  varp->dimids = (int*)malloc(o1);
	  varp->shape = (size_t*)malloc(o2);
	  varp->dsizes = (off_t*)malloc(o3);
#endif /*!MALLOCHACK*/
	} else {
	  varp->dimids = NULL;
	  varp->shape = NULL;
	  varp->dsizes=NULL;
	}


	varp->xsz = 0;
	varp->len = 0;
	varp->begin = 0;

	return varp;
}


/*
 * Formerly
NC_new_var()
 */
static NC_var *
new_NC_var(const char *uname, nc_type type,
	size_t ndims, const int *dimids)
{
	NC_string *strp = NULL;
	NC_var *varp = NULL;
	int stat;
	char* name;

    stat = nc_utf8_normalize((const unsigned char *)uname,(unsigned char **)&name);
    if(stat != NC_NOERR)
      return NULL;
	strp = new_NC_string(strlen(name), name);
	free(name);
	if(strp == NULL)
	  return NULL;

	varp = new_x_NC_var(strp, ndims);
	if(varp == NULL )
	{
		free_NC_string(strp);
		return NULL;
	}

	varp->type = type;

    if( ndims != 0 && dimids != NULL)
      (void) memcpy(varp->dimids, dimids, ndims * sizeof(int));
    else
      varp->dimids=NULL;


	return(varp);
}


static NC_var *
dup_NC_var(const NC_var *rvarp)
{
	NC_var *varp = new_NC_var(rvarp->name->cp, rvarp->type,
		 rvarp->ndims, rvarp->dimids);
	if(varp == NULL)
		return NULL;


	if(dup_NC_attrarrayV(&varp->attrs, &rvarp->attrs) != NC_NOERR)
	{
		free_NC_var(varp);
		return NULL;
	}

	if(rvarp->shape != NULL)
		(void) memcpy(varp->shape, rvarp->shape,
				 rvarp->ndims * sizeof(size_t));
	if(rvarp->dsizes != NULL)
		(void) memcpy(varp->dsizes, rvarp->dsizes,
				 rvarp->ndims * sizeof(off_t));
	varp->xsz = rvarp->xsz;
	varp->len = rvarp->len;
	varp->begin = rvarp->begin;

	return varp;
}


/* vararray */


/*
 * Free the stuff "in" (referred to by) an NC_vararray.
 * Leaves the array itself allocated.
 */
void
free_NC_vararrayV0(NC_vararray *ncap)
{
	assert(ncap != NULL);

	if(ncap->nelems == 0)
		return;

	assert(ncap->value != NULL);

	{
		NC_var **vpp = ncap->value;
		NC_var *const *const end = &vpp[ncap->nelems];
		for( /*NADA*/; vpp < end; vpp++)
		{
			free_NC_var(*vpp);
			*vpp = NULL;
		}
	}
	ncap->nelems = 0;
}


/*
 * Free NC_vararray values.
 * formerly
NC_free_array()
 */
void
free_NC_vararrayV(NC_vararray *ncap)
{
	assert(ncap != NULL);

	if(ncap->nalloc == 0)
		return;

	NC_hashmapfree(ncap->hashmap);
	ncap->hashmap = NULL;

	assert(ncap->value != NULL);

	free_NC_vararrayV0(ncap);

	free(ncap->value);
	ncap->value = NULL;
	ncap->nalloc = 0;
}


int
dup_NC_vararrayV(NC_vararray *ncap, const NC_vararray *ref)
{
	int status = NC_NOERR;

	assert(ref != NULL);
	assert(ncap != NULL);

	if(ref->nelems != 0)
	{
		const size_t sz = ref->nelems * sizeof(NC_var *);
		ncap->value = (NC_var **) malloc(sz);
		if(ncap->value == NULL)
			return NC_ENOMEM;
		(void) memset(ncap->value, 0, sz);
		ncap->nalloc = ref->nelems;
	}

	ncap->nelems = 0;
	{
		NC_var **vpp = ncap->value;
		const NC_var **drpp = (const NC_var **)ref->value;
		if (vpp)
		{
			NC_var *const *const end = &vpp[ref->nelems];
			for( /*NADA*/; vpp < end; drpp++, vpp++, ncap->nelems++)
			{
				*vpp = dup_NC_var(*drpp);
				if(*vpp == NULL)
				{
					status = NC_ENOMEM;
					break;
				}
			}
		}
	}

	if(status != NC_NOERR)
	{
		free_NC_vararrayV(ncap);
		return status;
	}

	assert(ncap->nelems == ref->nelems);

	return NC_NOERR;
}


/*
 * Add a new handle on the end of an array of handles
 * Formerly
NC_incr_array(array, tail)
 */
static int
incr_NC_vararray(NC_vararray *ncap, NC_var *newelemp)
{
	NC_var **vp;

	assert(ncap != NULL);

	if(ncap->nalloc == 0)
	{
		assert(ncap->nelems == 0);
		vp = (NC_var **) malloc(NC_ARRAY_GROWBY * sizeof(NC_var *));
		if(vp == NULL)
			return NC_ENOMEM;
		ncap->value = vp;
		ncap->nalloc = NC_ARRAY_GROWBY;

		ncap->hashmap = NC_hashmapnew(0);
	}
	else if(ncap->nelems +1 > ncap->nalloc)
	{
		vp = (NC_var **) realloc(ncap->value,
			(ncap->nalloc + NC_ARRAY_GROWBY) * sizeof(NC_var *));
		if(vp == NULL)
			return NC_ENOMEM;
		ncap->value = vp;
		ncap->nalloc += NC_ARRAY_GROWBY;
	}

	if(newelemp != NULL)
	{
		NC_hashmapadd(ncap->hashmap, (uintptr_t)ncap->nelems, newelemp->name->cp, strlen(newelemp->name->cp));
		ncap->value[ncap->nelems] = newelemp;
		ncap->nelems++;
	}
	return NC_NOERR;
}


static NC_var *
elem_NC_vararray(const NC_vararray *ncap, size_t elem)
{
	assert(ncap != NULL);
		/* cast needed for braindead systems with signed size_t */
	if(ncap->nelems == 0 || (unsigned long)elem >= ncap->nelems)
		return NULL;

	assert(ncap->value != NULL);

	return ncap->value[elem];
}


/* End vararray per se */


/*
 * Step thru NC_VARIABLE array, seeking match on name.
 * Return varid or -1 on not found.
 * *varpp is set to the appropriate NC_var.
 * Formerly (sort of) NC_hvarid
 */
int
NC_findvar(const NC_vararray *ncap, const char *uname, NC_var **varpp)
{
	int hash_var_id = -1;
	uintptr_t data;
	char *name = NULL;

	assert(ncap != NULL);

	if(ncap->nelems == 0)
	    goto done;

	/* normalized version of uname */
        if(nc_utf8_normalize((const unsigned char *)uname,(unsigned char **)&name))
	    goto done;

	if(NC_hashmapget(ncap->hashmap, name, strlen(name), &data) == 0)
	    goto done;

	hash_var_id = (int)data;
        if (varpp != NULL)
	  *varpp = ncap->value[hash_var_id];
done:
	if(name != NULL) free(name);
	return(hash_var_id); /* Normal return */
}

/*
 * For a netcdf type
 *  return the size of one element in the external representation.
 * Note that arrays get rounded up to X_ALIGN boundaries.
 * Formerly
NC_xtypelen
 * See also ncx_len()
 */
size_t
ncx_szof(nc_type type)
{
	switch(type){
	case NC_BYTE:
	case NC_CHAR:
	case NC_UBYTE:
		return(1);
	case NC_SHORT :
		return(2);
	case NC_INT:
		return X_SIZEOF_INT;
	case NC_FLOAT:
		return X_SIZEOF_FLOAT;
	case NC_DOUBLE :
		return X_SIZEOF_DOUBLE;
	case NC_USHORT :
		return X_SIZEOF_USHORT;
	case NC_UINT :
		return X_SIZEOF_UINT;
	case NC_INT64 :
		return X_SIZEOF_INT64;
	case NC_UINT64 :
		return X_SIZEOF_UINT64;
	default:
		/* 37824 Ignore */
	        assert("ncx_szof invalid type" == 0);
	        return 0;
	}
}


/*
 * 'compile' the shape and len of a variable
 *  Formerly
NC_var_shape(var, dims)
 */
int
NC_var_shape(NC_var *varp, const NC_dimarray *dims)
{
	size_t *shp, *op;
	off_t *dsp;
	int *ip = NULL;
	const NC_dim *dimp;
	off_t product = 1;

	varp->xsz = ncx_szof(varp->type);

	if(varp->ndims == 0 || varp->dimids == NULL)
	{
		goto out;
	}

	/*
	 * use the user supplied dimension indices
	 * to determine the shape
	 */
	for(ip = varp->dimids, op = varp->shape
          ; ip < &varp->dimids[varp->ndims]; ip++, op++)
	{
		if(*ip < 0 || (size_t) (*ip) >= ((dims != NULL) ? dims->nelems : 1) )
			return NC_EBADDIM;

		dimp = elem_NC_dimarray(dims, (size_t)*ip);
		*op = dimp->size;
		if(*op == NC_UNLIMITED && ip != varp->dimids)
			return NC_EUNLIMPOS;
	}

	/*
	 * Compute the dsizes
	 */
				/* ndims is > 0 here */
	for(shp = varp->shape + varp->ndims -1,
				dsp = varp->dsizes + varp->ndims -1;
 			shp >= varp->shape;
			shp--, dsp--)
	{
      /*if(!(shp == varp->shape && IS_RECVAR(varp)))*/
      if( shp != NULL && (shp != varp->shape || !IS_RECVAR(varp)))
		{
          if(product <= 0)
            return NC_ERANGE;
          if( ((off_t)(*shp)) <= OFF_T_MAX / product )
			{
              product *= (*shp > 0 ? (off_t)*shp : 1);
			} else
			{
              product = OFF_T_MAX ;
			}
		}
      *dsp = product;
	}


out :

    /*
     * For CDF-1 and CDF-2 formats, the total number of array elements
     * cannot exceed 2^32, unless this variable is the last fixed-size
     * variable, there is no record variable, and the file starting
     * offset of this variable is less than 2GiB.
     * This will be checked in NC_check_vlens() during NC_endef()
     */
    varp->len = product * (off_t)varp->xsz;
    if (varp->len % 4 > 0)
        varp->len += 4 - varp->len % 4; /* round up */

#if 0
	arrayp("\tshape", varp->ndims, varp->shape);
	arrayp("\tdsizes", varp->ndims, varp->dsizes);
#endif
	return NC_NOERR;
}

/*
 * Check whether variable size is less than or equal to vlen_max,
 * without overflowing in arithmetic calculations.  If OK, return 1,
 * else, return 0.  For CDF1 format or for CDF2 format on non-LFS
 * platforms, vlen_max should be 2^31 - 4, but for CDF2 format on
 * systems with LFS it should be 2^32 - 4.
 */
int
NC_check_vlen(NC_var *varp, long long vlen_max) {
    size_t ii;
    long long prod = (long long)varp->xsz;	/* product of xsz and dimensions so far */

    assert(varp != NULL);
    for(ii = IS_RECVAR(varp) ? 1 : 0; ii < varp->ndims; ii++) {
      if(!varp->shape)
        return 0; /* Shape is undefined/NULL. */
      if(prod <= 0)
	    return 0; /* Multiplication operations may result in overflow */
      if ((long long)varp->shape[ii] > vlen_max / prod) {
        return 0;		/* size in bytes won't fit in a 32-bit int */
      }
      prod *= (long long)varp->shape[ii];
    }
    return 1;			/* OK */
}


/*! Look up a variable by varid.
 *
 * Given a valid ncp structure and varid, return the var.
 *
 * Formerly NC_hlookupvar()
 *
 * @param[in] ncp NC3_INFO data structure.
 * @param[in] varid The varid key for the var we are looking up.
 * @param[out] varp Data structure to contain the varp pointer.
 * @return Error code, if one exists, 0 otherwise.
 */

int NC_lookupvar(NC3_INFO* ncp, int varid, NC_var **varp)
{
  if(varid == NC_GLOBAL)
	{
      /* Global is error in this context */
      return NC_EGLOBAL;
	}

  if(varp)
    *varp = elem_NC_vararray(&ncp->vars, (size_t)varid);
  else
    return NC_ENOTVAR;

  if(*varp == NULL)
    return NC_ENOTVAR;

  return NC_NOERR;

}


/* Public */

int
NC3_def_var( int ncid, const char *name, nc_type type,
	 int ndims, const int *dimids, int *varidp)
{
	int status;
	NC *nc;
	NC3_INFO* ncp;
	int varid;
	NC_var *varp = NULL;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	ncp = NC3_DATA(nc);

	if(!NC_indef(ncp))
	{
		return NC_ENOTINDEFINE;
	}

	status = NC_check_name(name);
	if(status != NC_NOERR)
		return status;

	status = nc3_cktype(nc->mode, type);
	if(status != NC_NOERR)
		return status;

        if (ndims > NC_MAX_VAR_DIMS) return NC_EMAXDIMS;

		/* cast needed for braindead systems with signed size_t */
	if((unsigned long) ndims > X_INT_MAX) /* Backward compat */
	{
		return NC_EINVAL;
	}

	varid = NC_findvar(&ncp->vars, name, &varp);
	if(varid != -1)
	{
		return NC_ENAMEINUSE;
	}

	varp = new_NC_var(name, type, (size_t)ndims, dimids);
	if(varp == NULL)
		return NC_ENOMEM;

	status = NC_var_shape(varp, &ncp->dims);
	if(status != NC_NOERR)
	{
		free_NC_var(varp);
		return status;
	}

	status = incr_NC_vararray(&ncp->vars, varp);
	if(status != NC_NOERR)
	{
		free_NC_var(varp);
		return status;
	}

	if(varidp != NULL)
		*varidp = (int)ncp->vars.nelems -1; /* varid */

	/* set the variable's fill mode */
	if (NC_dofill(ncp))
		varp->no_fill = 0;
	else
		varp->no_fill = 1;

	return NC_NOERR;
}


int
NC3_inq_varid(int ncid, const char *name, int *varid_ptr)
{
	int status;
	NC *nc;
	NC3_INFO* ncp;
	NC_var *varp;
	int varid;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	ncp = NC3_DATA(nc);

	varid = NC_findvar(&ncp->vars, name, &varp);
	if(varid == -1)
	{
		return NC_ENOTVAR;
	}

	*varid_ptr = varid;
	return NC_NOERR;
}


int
NC3_inq_var(int ncid,
	int varid,
	char *name,
	nc_type *typep,
	int *ndimsp,
	int *dimids,
	int *nattsp,
	int *no_fillp,
	void *fill_valuep)
{
	int status;
	NC *nc;
	NC3_INFO* ncp;
	NC_var *varp;
	size_t ii;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	ncp = NC3_DATA(nc);

	varp = elem_NC_vararray(&ncp->vars, (size_t)varid);
	if(varp == NULL)
		return NC_ENOTVAR;

	if(name != NULL)
	{
		(void) strncpy(name, varp->name->cp, varp->name->nchars);
		name[varp->name->nchars] = 0;
	}

	if(typep != 0)
		*typep = varp->type;
	if(ndimsp != 0)
	{
		*ndimsp = (int) varp->ndims;
	}
	if(dimids != 0)
	{
		for(ii = 0; ii < varp->ndims; ii++)
		{
			dimids[ii] = varp->dimids[ii];
		}
	}
	if(nattsp != 0)
	{
		*nattsp = (int) varp->attrs.nelems;
	}

	if (no_fillp != NULL) *no_fillp = varp->no_fill;

	if (fill_valuep != NULL) {
		status = nc_get_att(ncid, varid, NC_FillValue, fill_valuep);
		if (status != NC_NOERR && status != NC_ENOTATT)
			return status;
		if (status == NC_ENOTATT) {
			status = NC3_inq_default_fill_value(varp->type, fill_valuep);
			if (status != NC_NOERR) return status;
		}
	}

	return NC_NOERR;
}

int
NC3_rename_var(int ncid, int varid, const char *unewname)
{
	int status = NC_NOERR;
	NC *nc;
	NC3_INFO* ncp;
	uintptr_t intdata;
	NC_var *varp;
	NC_string *old, *newStr;
	int other;
	char *newname = NULL;	/* normalized */

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
	    goto done;
	ncp = NC3_DATA(nc);

	if(NC_readonly(ncp))
	    {status = NC_EPERM; goto done;}

	status = NC_check_name(unewname);
	if(status != NC_NOERR)
	    goto done;

	/* check for name in use */
	other = NC_findvar(&ncp->vars, unewname, &varp);
	if(other != -1)
	    {status = NC_ENAMEINUSE; goto done;}

	status = NC_lookupvar(ncp, varid, &varp);
	if(status != NC_NOERR)
	    goto done; /* invalid varid */

	old = varp->name;
        status = nc_utf8_normalize((const unsigned char *)unewname,(unsigned char **)&newname);
        if(status != NC_NOERR)
	    goto done;
	if(NC_indef(ncp))
	{
		/* Remove old name from hashmap; add new... */
	        /* WARNING: strlen(NC_string.cp) may be less than NC_string.nchars */
		NC_hashmapremove(ncp->vars.hashmap,old->cp,strlen(old->cp),NULL);
		newStr = new_NC_string(strlen(newname),newname);
		if(newStr == NULL)
		    {status = NC_ENOMEM; goto done;}
		varp->name = newStr;
		intdata = (uintptr_t)varid;
		NC_hashmapadd(ncp->vars.hashmap, intdata, varp->name->cp, strlen(varp->name->cp));
		free_NC_string(old);
		goto done;
	}

	/* else, not in define mode */
	/* If new name is longer than old, then complain,
           but otherwise, no change (test is same as set_NC_string)*/
	if(varp->name->nchars < strlen(newname))
	    {status = NC_ENOTINDEFINE; goto done;}

	/* WARNING: strlen(NC_string.cp) may be less than NC_string.nchars */
	/* Remove old name from hashmap; add new... */
        NC_hashmapremove(ncp->vars.hashmap,old->cp,strlen(old->cp),NULL);

	/* WARNING: strlen(NC_string.cp) may be less than NC_string.nchars */
	status = set_NC_string(varp->name, newname);
	if(status != NC_NOERR)
		goto done;

	intdata = (uintptr_t)varid;
	NC_hashmapadd(ncp->vars.hashmap, intdata, varp->name->cp, strlen(varp->name->cp));

	set_NC_hdirty(ncp);

	if(NC_doHsync(ncp))
	{
		status = NC_sync(ncp);
		if(status != NC_NOERR)
			goto done;
	}
done:
	if(newname) free(newname);
	return status;
}

int
NC3_def_var_fill(int ncid,
	int varid,
	int no_fill,
	const void *fill_value)
{
	int status;
	NC *nc;
	NC3_INFO* ncp;
	NC_var *varp;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	ncp = NC3_DATA(nc);

	if(NC_readonly(ncp))
	{
		return NC_EPERM;
	}

	if(!NC_indef(ncp))
	{
		return NC_ENOTINDEFINE;
	}

	varp = elem_NC_vararray(&ncp->vars, (size_t)varid);
	if(varp == NULL)
		return NC_ENOTVAR;

	if (no_fill)
		varp->no_fill = 1;
	else
		varp->no_fill = 0;

	/* Are we setting a fill value? */
	if (fill_value != NULL && !varp->no_fill) {

		/* If there's a _FillValue attribute, delete it. */
		status = NC3_del_att(ncid, varid, NC_FillValue);
		if (status != NC_NOERR && status != NC_ENOTATT)
			return status;

		/* Create/overwrite attribute _FillValue */
		status = NC3_put_att(ncid, varid, NC_FillValue, varp->type, 1, fill_value, varp->type);
		if (status != NC_NOERR) return status;
	}

	return NC_NOERR;
}
