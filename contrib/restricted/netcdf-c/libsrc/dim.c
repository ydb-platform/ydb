/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
/* $Id: dim.c,v 1.83 2010/05/25 17:54:15 dmh Exp $ */

#if HAVE_CONFIG_H
#include <config.h>
#endif

#include "nc3internal.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "ncx.h"
#include "fbits.h"
#include "ncutf8.h"

/*
 * Free dim
 * Formerly
NC_free_dim(dim)
 */
void
free_NC_dim(NC_dim *dimp)
{
	if(dimp == NULL)
		return;
	free_NC_string(dimp->name);
	free(dimp);
}


NC_dim *
new_x_NC_dim(NC_string *name)
{
	NC_dim *dimp;

	dimp = (NC_dim *) malloc(sizeof(NC_dim));
	if(dimp == NULL)
		return NULL;

	dimp->name = name;
	dimp->size = 0;

	return(dimp);
}


/*
 * Formerly
NC_new_dim(const char *uname, long size)
 */
static NC_dim *
new_NC_dim(const char *uname, size_t size)
{
	NC_string *strp;
	NC_dim *dimp = NULL;
	int stat = NC_NOERR;
	char* name = NULL;

	stat = nc_utf8_normalize((const unsigned char *)uname,(unsigned char **)&name);
	if(stat != NC_NOERR)
	    goto done;
	strp = new_NC_string(strlen(name), name);
	if(strp == NULL)
		{stat = NC_ENOMEM; goto done;}

	dimp = new_x_NC_dim(strp);
	if(dimp == NULL)
	{
		free_NC_string(strp);
		goto done;
	}

	dimp->size = size;

done:
	if(name) free(name);
	return (dimp);
}


static NC_dim *
dup_NC_dim(const NC_dim *dimp)
{
	return new_NC_dim(dimp->name->cp, dimp->size);
}

/*
 * Step thru NC_DIMENSION array, seeking the UNLIMITED dimension.
 * Return dimid or -1 on not found.
 * *dimpp is set to the appropriate NC_dim.
 * The loop structure is odd. In order to parallelize,
 * we moved a clearer 'break' inside the loop body to the loop test.
 */
int
find_NC_Udim(const NC_dimarray *ncap, NC_dim **dimpp)
{
	assert(ncap != NULL);

	if(ncap->nelems == 0)
		return -1;

	{
	int dimid = 0;
	NC_dim **loc = ncap->value;

	for(; (size_t) dimid < ncap->nelems
			 && (*loc)->size != NC_UNLIMITED; dimid++, loc++)
	{
		/*EMPTY*/
	}
	if(dimid >= ncap->nelems)
		return(-1); /* not found */
	/* else, normal return */
	if(dimpp != NULL)
		*dimpp = *loc;
	return dimid;
	}
}

/*
 * Step thru NC_DIMENSION array, seeking match on uname.
 * Return dimid or -1 on not found.
 * *dimpp is set to the appropriate NC_dim.
 * The loop structure is odd. In order to parallelize,
 * we moved a clearer 'break' inside the loop body to the loop test.
 */
static int
NC_finddim(const NC_dimarray *ncap, const char *uname, NC_dim **dimpp)
{
   int dimid = -1;
   char *name = NULL;
   uintptr_t data;

   assert(ncap != NULL);
   if(ncap->nelems == 0)
	goto done;
   /* normalized version of uname */
  if(nc_utf8_normalize((const unsigned char *)uname,(unsigned char **)&name))
	goto done;	 
  if(NC_hashmapget(ncap->hashmap, name, strlen(name), &data) == 0)
	goto done;
  dimid = (int)data;
  if(dimpp) *dimpp = ncap->value[dimid];

done:
   if(name) free(name);
   return dimid;
}


/* dimarray */


/*
 * Free the stuff "in" (referred to by) an NC_dimarray.
 * Leaves the array itself allocated.
 */
void
free_NC_dimarrayV0(NC_dimarray *ncap)
{
	assert(ncap != NULL);

	if(ncap->nelems == 0)
		return;

	assert(ncap->value != NULL);

	{
		NC_dim **dpp = ncap->value;
		NC_dim *const *const end = &dpp[ncap->nelems];
		for( /*NADA*/; dpp < end; dpp++)
		{
			free_NC_dim(*dpp);
			*dpp = NULL;
		}
	}
	ncap->nelems = 0;
}


/*
 * Free NC_dimarray values.
 * formerly
NC_free_array()
 */
void
free_NC_dimarrayV(NC_dimarray *ncap)
{
	assert(ncap != NULL);

	if(ncap->nalloc == 0)
		return;

	NC_hashmapfree(ncap->hashmap);
	ncap->hashmap = NULL;

	assert(ncap->value != NULL);

	free_NC_dimarrayV0(ncap);

	free(ncap->value);
	ncap->value = NULL;
	ncap->nalloc = 0;
}


int
dup_NC_dimarrayV(NC_dimarray *ncap, const NC_dimarray *ref)
{
	int status = NC_NOERR;

	assert(ref != NULL);
	assert(ncap != NULL);

	if(ref->nelems != 0)
	{
		const size_t sz = ref->nelems * sizeof(NC_dim *);
		ncap->value = (NC_dim **) malloc(sz);
		if(ncap->value == NULL)
			return NC_ENOMEM;
		(void) memset(ncap->value, 0, sz);
		ncap->nalloc = ref->nelems;
	}

	ncap->nelems = 0;
	{
		NC_dim **dpp = ncap->value;
		if(dpp != NULL)
		{
			const NC_dim **drpp = (const NC_dim **)ref->value;
			NC_dim *const *const end = &dpp[ref->nelems];
			for( /*NADA*/; dpp < end; drpp++, dpp++, ncap->nelems++)
			{
				*dpp = dup_NC_dim(*drpp);
				if(*dpp == NULL)
				{
					status = NC_ENOMEM;
					break;
				}
			}
		}
	}

	if(status != NC_NOERR)
	{
		free_NC_dimarrayV(ncap);
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
incr_NC_dimarray(NC_dimarray *ncap, NC_dim *newelemp)
{
	NC_dim **vp;

	assert(ncap != NULL);

	if(ncap->nalloc == 0)
	{
		assert(ncap->nelems == 0);
		vp = (NC_dim **) malloc(NC_ARRAY_GROWBY * sizeof(NC_dim *));
		if(vp == NULL)
			return NC_ENOMEM;
		ncap->value = vp;
		ncap->nalloc = NC_ARRAY_GROWBY;
		ncap->hashmap = NC_hashmapnew(0);
	}
	else if(ncap->nelems +1 > ncap->nalloc)
	{
		vp = (NC_dim **) realloc(ncap->value,
			(ncap->nalloc + NC_ARRAY_GROWBY) * sizeof(NC_dim *));
		if(vp == NULL)
			return NC_ENOMEM;
		ncap->value = vp;
		ncap->nalloc += NC_ARRAY_GROWBY;
	}

	if(newelemp != NULL)
	{
           uintptr_t intdata = ncap->nelems;
	   NC_hashmapadd(ncap->hashmap, intdata, newelemp->name->cp, strlen(newelemp->name->cp));
	   ncap->value[ncap->nelems] = newelemp;
	   ncap->nelems++;
	}
	return NC_NOERR;
}


NC_dim *
elem_NC_dimarray(const NC_dimarray *ncap, size_t elem)
{
	assert(ncap != NULL);
		/* cast needed for braindead systems with signed size_t */
	if(ncap->nelems == 0 || (unsigned long) elem >= ncap->nelems)
		return NULL;

	assert(ncap->value != NULL);

	return ncap->value[elem];
}


/* Public */

int
NC3_def_dim(int ncid, const char *name, size_t size, int *dimidp)
{
	int status;
	NC *nc;
	NC3_INFO* ncp;
	int dimid;
	NC_dim *dimp;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	ncp = NC3_DATA(nc);

	if(!NC_indef(ncp))
		return NC_ENOTINDEFINE;

	status = NC_check_name(name);
	if(status != NC_NOERR)
		return status;

	if(ncp->flags & NC_64BIT_DATA) {/*CDF-5*/
	    if((sizeof(size_t) > 4) && (size > X_UINT64_MAX - 3)) /* "- 3" handles rounded-up size */
		return NC_EDIMSIZE;
	} else if(ncp->flags & NC_64BIT_OFFSET) {/* CDF2 format and LFS */
	    if((sizeof(size_t) > 4) && (size > X_UINT_MAX - 3)) /* "- 3" handles rounded-up size */
		return NC_EDIMSIZE;
	} else {/*CDF-1*/
	    if(size > X_INT_MAX - 3)
		return NC_EDIMSIZE;
	}

	if(size == NC_UNLIMITED)
	{
		dimid = find_NC_Udim(&ncp->dims, &dimp);
		if(dimid != -1)
		{
			assert(dimid != -1);
			return NC_EUNLIMIT;
		}
	}

	dimid = NC_finddim(&ncp->dims, name, &dimp);
	if(dimid != -1)
		return NC_ENAMEINUSE;

	dimp = new_NC_dim(name, size);
	if(dimp == NULL)
		return NC_ENOMEM;
	status = incr_NC_dimarray(&ncp->dims, dimp);
	if(status != NC_NOERR)
	{
		free_NC_dim(dimp);
		return status;
	}

	if(dimidp != NULL)
		*dimidp = (int)ncp->dims.nelems -1;
	return NC_NOERR;
}


int
NC3_inq_dimid(int ncid, const char *name, int *dimid_ptr)
{
	int status;
	NC *nc;
	NC3_INFO* ncp;
	int dimid;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	ncp = NC3_DATA(nc);

	dimid = NC_finddim(&ncp->dims, name, NULL);

	if(dimid == -1)
		return NC_EBADDIM;

	if (dimid_ptr)
	   *dimid_ptr = dimid;
	return NC_NOERR;
}

int
NC3_inq_dim(int ncid, int dimid, char *name, size_t *sizep)
{
	int status;
	NC *nc;
	NC3_INFO* ncp;
	NC_dim *dimp;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	ncp = NC3_DATA(nc);

	dimp = elem_NC_dimarray(&ncp->dims, (size_t)dimid);
	if(dimp == NULL)
		return NC_EBADDIM;

	if(name != NULL)
	{
		(void)strncpy(name, dimp->name->cp,
			dimp->name->nchars);
		name[dimp->name->nchars] = 0;
	}
	if(sizep != NULL)
	{
		if(dimp->size == NC_UNLIMITED)
			*sizep = NC_get_numrecs(ncp);
		else
			*sizep = dimp->size;
	}
	return NC_NOERR;
}

int
NC3_rename_dim( int ncid, int dimid, const char *unewname)
{
	int status = NC_NOERR;
	NC *nc;
	NC3_INFO* ncp;
	int existid;
	NC_dim *dimp;
	char *newname = NULL; /* normalized */
	NC_string *old = NULL;
 	uintptr_t intdata;


	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		goto done;
	ncp = NC3_DATA(nc);

	if(NC_readonly(ncp))
		{status = NC_EPERM; goto done;}

	status = NC_check_name(unewname);
	if(status != NC_NOERR)
		goto done;

	existid = NC_finddim(&ncp->dims, unewname, &dimp);
	if(existid != -1)
		{status = NC_ENAMEINUSE; goto done;}

	dimp = elem_NC_dimarray(&ncp->dims, (size_t)dimid);
	if(dimp == NULL)
		{status = NC_EBADDIM; goto done;}

    old = dimp->name;
    status = nc_utf8_normalize((const unsigned char *)unewname,(unsigned char **)&newname);
    if(status != NC_NOERR)
	goto done;
    if(NC_indef(ncp))
	{
		NC_string *newStr = new_NC_string(strlen(newname), newname);
		if(newStr == NULL)
			{status = NC_ENOMEM; goto done;}

		/* Remove old name from hashmap; add new... */
	        NC_hashmapremove(ncp->dims.hashmap, old->cp, strlen(old->cp), NULL);
		dimp->name = newStr;

		intdata = (uintptr_t)dimid;
		NC_hashmapadd(ncp->dims.hashmap, intdata, newStr->cp, strlen(newStr->cp));
		free_NC_string(old);
		goto done;
	}

	/* else, not in define mode */

	/* If new name is longer than old, then complain,
           but otherwise, no change (test is same as set_NC_string)*/
	if(dimp->name->nchars < strlen(newname)) {
	    {status = NC_ENOTINDEFINE; goto done;}
	}

	/* Remove old name from hashmap; add new... */
	/* WARNING: strlen(NC_string.cp) may be less than NC_string.nchars */
	NC_hashmapremove(ncp->dims.hashmap, old->cp, strlen(old->cp), NULL);

	/* WARNING: strlen(NC_string.cp) may be less than NC_string.nchars */
	status = set_NC_string(dimp->name, newname);
	if(status != NC_NOERR)
		goto done;

        intdata = (uintptr_t)dimid;
	NC_hashmapadd(ncp->dims.hashmap, intdata, dimp->name->cp, strlen(dimp->name->cp));

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
