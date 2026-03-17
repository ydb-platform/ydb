/*
 *	Copyright 2018, Unuiversity Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#if HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "nc3internal.h"
#include "netcdf_mem.h"
#include "rnd.h"
#include "ncx.h"
#include "ncrc.h"

/* These have to do with version numbers. */
#define MAGIC_NUM_LEN 4
#define VER_CLASSIC 1
#define VER_64BIT_OFFSET 2
#define VER_HDF5 3

#define NC_NUMRECS_OFFSET 4

/* For netcdf classic */
#define NC_NUMRECS_EXTENT3 4
/* For cdf5 */
#define NC_NUMRECS_EXTENT5 8

/* Internal function; breaks ncio abstraction */
extern int memio_extract(ncio* const nciop, size_t* sizep, void** memoryp);

static void
free_NC3INFO(NC3_INFO *nc3)
{
	if(nc3 == NULL)
		return;
	free_NC_dimarrayV(&nc3->dims);
	free_NC_attrarrayV(&nc3->attrs);
	free_NC_vararrayV(&nc3->vars);
	free(nc3);
}

static NC3_INFO *
new_NC3INFO(const size_t *chunkp)
{
	NC3_INFO *ncp;
	ncp = (NC3_INFO*)calloc(1,sizeof(NC3_INFO));
	if(ncp == NULL) return ncp;
        ncp->chunk = chunkp != NULL ? *chunkp : NC_SIZEHINT_DEFAULT;
	/* Note that ncp->xsz is not set yet because we do not know the file format */
	return ncp;
}

static NC3_INFO *
dup_NC3INFO(const NC3_INFO *ref)
{
	NC3_INFO *ncp;
	ncp = (NC3_INFO*)calloc(1,sizeof(NC3_INFO));
	if(ncp == NULL) return ncp;

	if(dup_NC_dimarrayV(&ncp->dims, &ref->dims) != NC_NOERR)
		goto err;
	if(dup_NC_attrarrayV(&ncp->attrs, &ref->attrs) != NC_NOERR)
		goto err;
	if(dup_NC_vararrayV(&ncp->vars, &ref->vars) != NC_NOERR)
		goto err;

	ncp->xsz = ref->xsz;
	ncp->begin_var = ref->begin_var;
	ncp->begin_rec = ref->begin_rec;
	ncp->recsize = ref->recsize;
	NC_set_numrecs(ncp, NC_get_numrecs(ref));
	return ncp;
err:
	free_NC3INFO(ncp);
	return NULL;
}


/*
 *  Verify that this is a user nc_type
 * Formerly NCcktype()
 * Sense of the return is changed.
 */
int
nc3_cktype(int mode, nc_type type)
{
#ifdef NETCDF_ENABLE_CDF5
    if (mode & NC_CDF5) { /* CDF-5 format */
        if (type >= NC_BYTE && type < NC_STRING) return NC_NOERR;
    } else
#endif
      if (mode & NC_64BIT_OFFSET) { /* CDF-2 format */
        if (type >= NC_BYTE && type <= NC_DOUBLE) return NC_NOERR;
    } else if ((mode & NC_64BIT_OFFSET) == 0) { /* CDF-1 format */
        if (type >= NC_BYTE && type <= NC_DOUBLE) return NC_NOERR;
    }
    return(NC_EBADTYPE);
}


/*
 * How many objects of 'type'
 * will fit into xbufsize?
 */
size_t
ncx_howmany(nc_type type, size_t xbufsize)
{
	switch(type){
	case NC_BYTE:
	case NC_CHAR:
		return xbufsize;
	case NC_SHORT:
		return xbufsize/X_SIZEOF_SHORT;
	case NC_INT:
		return xbufsize/X_SIZEOF_INT;
	case NC_FLOAT:
		return xbufsize/X_SIZEOF_FLOAT;
	case NC_DOUBLE:
		return xbufsize/X_SIZEOF_DOUBLE;
	case NC_UBYTE:
 		return xbufsize;
	case NC_USHORT:
		return xbufsize/X_SIZEOF_USHORT;
	case NC_UINT:
		return xbufsize/X_SIZEOF_UINT;
	case NC_INT64:
		return xbufsize/X_SIZEOF_LONGLONG;
	case NC_UINT64:
		return xbufsize/X_SIZEOF_ULONGLONG;
	default:
	        assert("ncx_howmany: Bad type" == 0);
		return(0);
	}
}

#define	D_RNDUP(x, align) _RNDUP(x, (off_t)(align))

/*
 * Compute each variable's 'begin' offset,
 * update 'begin_rec' as well.
 */
static int
NC_begins(NC3_INFO* ncp,
	size_t h_minfree, size_t v_align,
	size_t v_minfree, size_t r_align)
{
	size_t ii, j;
	size_t sizeof_off_t;
	off_t index = 0;
	off_t old_ncp_begin_var;
	NC_var **vpp;
	NC_var *last = NULL;
	NC_var *first_var = NULL;       /* first "non-record" var */


	if(v_align == NC_ALIGN_CHUNK)
		v_align = ncp->chunk;
	if(r_align == NC_ALIGN_CHUNK)
		r_align = ncp->chunk;

	if (fIsSet(ncp->flags, NC_64BIT_OFFSET) || fIsSet(ncp->flags, NC_64BIT_DATA)) {
	  sizeof_off_t = 8;
	} else {
	  sizeof_off_t = 4;
	}

	ncp->xsz = ncx_len_NC(ncp,sizeof_off_t);

	if(ncp->vars.nelems == 0)
		return NC_NOERR;

        old_ncp_begin_var = ncp->begin_var;

	/* only (re)calculate begin_var if there is not sufficient space in header
	   or start of non-record variables is not aligned as requested by valign */
	if (ncp->begin_var < ncp->xsz + h_minfree ||
	    ncp->begin_var != D_RNDUP(ncp->begin_var, v_align) )
	{
	  index = (off_t) ncp->xsz;
	  ncp->begin_var = D_RNDUP(index, v_align);
	  if(ncp->begin_var < index + (off_t)h_minfree)
	  {
	    ncp->begin_var = D_RNDUP(index + (off_t)h_minfree, v_align);
	  }
	}

	if (ncp->old != NULL) {
            /* check whether the new begin_var is smaller */
            if (ncp->begin_var < ncp->old->begin_var)
                ncp->begin_var = ncp->old->begin_var;
	}

	index = ncp->begin_var;

	/* loop thru vars, first pass is for the 'non-record' vars */
	j = 0;
	vpp = ncp->vars.value;
	for(ii = 0; ii < ncp->vars.nelems ; ii++, vpp++)
	{
		if( IS_RECVAR(*vpp) )
		{
			/* skip record variables on this pass */
			continue;
		}
		if (first_var == NULL) first_var = *vpp;

#if 0
fprintf(stderr, "    VAR %d %s: %ld\n", ii, (*vpp)->name->cp, (long)index);
#endif
                if( sizeof_off_t == 4 && (index > X_OFF_MAX || index < 0) )
		{
                    ncp->begin_var = old_ncp_begin_var;
		    return NC_EVARSIZE;
                }
		(*vpp)->begin = index;

		if (ncp->old != NULL) {
          /* move to the next fixed variable */
          for (; j<ncp->old->vars.nelems; j++) {
            if (!IS_RECVAR(ncp->old->vars.value[j]))
              break;
          }

          if (j < ncp->old->vars.nelems) {
            if ((*vpp)->begin < ncp->old->vars.value[j]->begin) {
              /* the first ncp->vars.nelems fixed variables
                 should be the same. If the new begin is smaller,
                 reuse the old begin */
              (*vpp)->begin = ncp->old->vars.value[j]->begin;
              index = (*vpp)->begin;
            }
            j++;
          }
		}

		index += (*vpp)->len;
	}

	if (ncp->old != NULL) {
	    /* check whether the new begin_rec is smaller */
	    if (ncp->begin_rec < ncp->old->begin_rec)
	        ncp->begin_rec = ncp->old->begin_rec;
	}

	/* only (re)calculate begin_rec if there is not sufficient
	   space at end of non-record variables or if start of record
	   variables is not aligned as requested by r_align */
	if (ncp->begin_rec < index + (off_t)v_minfree ||
	    ncp->begin_rec != D_RNDUP(ncp->begin_rec, r_align) )
	{
	  ncp->begin_rec = D_RNDUP(index, r_align);
	  if(ncp->begin_rec < index + (off_t)v_minfree)
	  {
	    ncp->begin_rec = D_RNDUP(index + (off_t)v_minfree, r_align);
	  }
	}

	if (first_var != NULL)
	    ncp->begin_var = first_var->begin;
	else
	    ncp->begin_var = ncp->begin_rec;

	index = ncp->begin_rec;

	ncp->recsize = 0;

	/* loop thru vars, second pass is for the 'record' vars */
	j = 0;
	vpp = (NC_var **)ncp->vars.value;
	for(ii = 0; ii < ncp->vars.nelems; ii++, vpp++)
	{
		if( !IS_RECVAR(*vpp) )
		{
			/* skip non-record variables on this pass */
			continue;
		}

#if 0
fprintf(stderr, "    REC %d %s: %ld\n", ii, (*vpp)->name->cp, (long)index);
#endif
                if( sizeof_off_t == 4 && (index > X_OFF_MAX || index < 0) )
		{
                    ncp->begin_var = old_ncp_begin_var;
		    return NC_EVARSIZE;
                }
		(*vpp)->begin = index;

                if (ncp->old != NULL) {
                    /* move to the next record variable */
                    for (; j<ncp->old->vars.nelems; j++)
                        if (IS_RECVAR(ncp->old->vars.value[j]))
                            break;
                    if (j < ncp->old->vars.nelems) {
                        if ((*vpp)->begin < ncp->old->vars.value[j]->begin)
                            /* if the new begin is smaller, use the old begin */
                            (*vpp)->begin = ncp->old->vars.value[j]->begin;
                        j++;
                    }
                }

		index += (*vpp)->len;
		/* check if record size must fit in 32-bits */
#if SIZEOF_OFF_T == SIZEOF_SIZE_T && SIZEOF_SIZE_T == 4
		if( ncp->recsize > X_UINT_MAX - (*vpp)->len )
		{
                    ncp->begin_var = old_ncp_begin_var;
		    return NC_EVARSIZE;
		}
#endif
		ncp->recsize += (*vpp)->len;
		last = (*vpp);
	}

    /*
     * for special case (Check CDF-1 and CDF-2 file format specifications.)
     * "A special case: Where there is exactly one record variable, we drop the
     * requirement that each record be four-byte aligned, so in this case there
     * is no record padding."
     */
    if (last != NULL) {
        if (ncp->recsize == last->len) {
            /* exactly one record variable, pack value */
            ncp->recsize = *last->dsizes * last->xsz;
        }
    }

	if(NC_IsNew(ncp))
		NC_set_numrecs(ncp, 0);
	return NC_NOERR;
}


/*
 * Read just the numrecs member.
 * (A relatively expensive way to do things.)
 */
int
read_numrecs(NC3_INFO *ncp)
{
	int status = NC_NOERR;
	const void *xp = NULL;
	size_t new_nrecs = 0;
	size_t  old_nrecs = NC_get_numrecs(ncp);
	size_t nc_numrecs_extent = NC_NUMRECS_EXTENT3; /* CDF-1 and CDF-2 */

	assert(!NC_indef(ncp));

	if (fIsSet(ncp->flags, NC_64BIT_DATA))
		nc_numrecs_extent = NC_NUMRECS_EXTENT5; /* CDF-5 */

	status = ncio_get(ncp->nciop,
		 NC_NUMRECS_OFFSET, nc_numrecs_extent, 0, (void **)&xp);/* cast away const */
	if(status != NC_NOERR)
		return status;

	if (fIsSet(ncp->flags, NC_64BIT_DATA)) {
	    unsigned long long tmp=0;
	    status = ncx_get_uint64(&xp, &tmp);
	    new_nrecs = (size_t)tmp;
        } else
	    status = ncx_get_size_t(&xp, &new_nrecs);

	(void) ncio_rel(ncp->nciop, NC_NUMRECS_OFFSET, 0);

	if(status == NC_NOERR && old_nrecs != new_nrecs)
	{
		NC_set_numrecs(ncp, new_nrecs);
		fClr(ncp->flags, NC_NDIRTY);
	}

	return status;
}


/*
 * Write out just the numrecs member.
 * (A relatively expensive way to do things.)
 */
int
write_numrecs(NC3_INFO *ncp)
{
	int status = NC_NOERR;
	void *xp = NULL;
	size_t nc_numrecs_extent = NC_NUMRECS_EXTENT3; /* CDF-1 and CDF-2 */

	assert(!NC_readonly(ncp));
	assert(!NC_indef(ncp));

	if (fIsSet(ncp->flags, NC_64BIT_DATA))
	    nc_numrecs_extent = NC_NUMRECS_EXTENT5; /* CDF-5 */

	status = ncio_get(ncp->nciop,
		 NC_NUMRECS_OFFSET, nc_numrecs_extent, RGN_WRITE, &xp);
	if(status != NC_NOERR)
		return status;

	{
		const size_t nrecs = NC_get_numrecs(ncp);
		if (fIsSet(ncp->flags, NC_64BIT_DATA))
		    status = ncx_put_uint64(&xp, (unsigned long long)nrecs);
		else
 		    status = ncx_put_size_t(&xp, &nrecs);
	}

	(void) ncio_rel(ncp->nciop, NC_NUMRECS_OFFSET, RGN_MODIFIED);

	if(status == NC_NOERR)
		fClr(ncp->state, NC_NDIRTY);

	return status;
}


/*
 * Read in the header
 * It is expensive.
 */
static int
read_NC(NC3_INFO *ncp)
{
	int status = NC_NOERR;

	free_NC_dimarrayV(&ncp->dims);
	free_NC_attrarrayV(&ncp->attrs);
	free_NC_vararrayV(&ncp->vars);

	status = nc_get_NC(ncp);

	if(status == NC_NOERR)
		fClr(ncp->state, NC_NDIRTY | NC_HDIRTY);

	return status;
}


/*
 * Write out the header
 */
static int
write_NC(NC3_INFO *ncp)
{
	int status = NC_NOERR;

	assert(!NC_readonly(ncp));

	status = ncx_put_NC(ncp, NULL, 0, 0);

	if(status == NC_NOERR)
		fClr(ncp->state, NC_NDIRTY | NC_HDIRTY);

	return status;
}


/*
 * Write the header or the numrecs if necessary.
 */
int
NC_sync(NC3_INFO *ncp)
{
	assert(!NC_readonly(ncp));

	if(NC_hdirty(ncp))
	{
		return write_NC(ncp);
	}
	/* else */

	if(NC_ndirty(ncp))
	{
		return write_numrecs(ncp);
	}
	/* else */

	return NC_NOERR;
}


/*
 * Initialize the 'non-record' variables.
 */
static int
fillerup(NC3_INFO *ncp)
{
	int status = NC_NOERR;
	size_t ii;
	NC_var **varpp;

	assert(!NC_readonly(ncp));

	/* loop thru vars */
	varpp = ncp->vars.value;
	for(ii = 0; ii < ncp->vars.nelems; ii++, varpp++)
	{
		if ((*varpp)->no_fill) continue;

		if(IS_RECVAR(*varpp))
		{
			/* skip record variables */
			continue;
		}

		status = fill_NC_var(ncp, *varpp, (*varpp)->len, 0);
		if(status != NC_NOERR)
			break;
	}
	return status;
}

/* Begin endef */

/*
 */
static int
fill_added_recs(NC3_INFO *gnu, NC3_INFO *old)
{
	NC_var ** const gnu_varpp = (NC_var **)gnu->vars.value;

	const size_t old_nrecs = NC_get_numrecs(old);
	size_t recno = 0;
	NC_var **vpp = gnu_varpp;
	NC_var *const *const end = &vpp[gnu->vars.nelems];
	int numrecvars = 0;

	/* Determine if there is only one record variable.  If so, we
	   must treat as a special case because there's no record padding */
	for(; vpp < end; vpp++) {
	    if(IS_RECVAR(*vpp)) {
		numrecvars++;
	    }
	}

	for(; recno < old_nrecs; recno++)
	    {
		int varid = (int)old->vars.nelems;
		for(; varid < (int)gnu->vars.nelems; varid++)
		    {
			const NC_var *const gnu_varp = *(gnu_varpp + varid);

			if (gnu_varp->no_fill) continue;

			if(!IS_RECVAR(gnu_varp))
			    {
				/* skip non-record variables */
				continue;
			    }
			/* else */
			{
			    long long varsize = numrecvars == 1 ? gnu->recsize :  gnu_varp->len;
			    const int status = fill_NC_var(gnu, gnu_varp, varsize, recno);
			    if(status != NC_NOERR)
				return status;
			}
		    }
	    }
	return NC_NOERR;
}

/*
 */
static int
fill_added(NC3_INFO *gnu, NC3_INFO *old)
{
	NC_var ** const gnu_varpp = (NC_var **)gnu->vars.value;
	int varid = (int)old->vars.nelems;

	for(; varid < (int)gnu->vars.nelems; varid++)
	{
		const NC_var *const gnu_varp = *(gnu_varpp + varid);

		if (gnu_varp->no_fill) continue;

		if(IS_RECVAR(gnu_varp))
		{
			/* skip record variables */
			continue;
		}
		/* else */
		{
		const int status = fill_NC_var(gnu, gnu_varp, gnu_varp->len, 0);
		if(status != NC_NOERR)
			return status;
		}
	}

	return NC_NOERR;
}


/*
 * Move the records "out".
 * Fill as needed.
 */
static int
move_recs_r(NC3_INFO *gnu, NC3_INFO *old)
{
	int status;
	int recno;
	int varid;
	NC_var **gnu_varpp = (NC_var **)gnu->vars.value;
	NC_var **old_varpp = (NC_var **)old->vars.value;
	NC_var *gnu_varp;
	NC_var *old_varp;
	off_t gnu_off;
	off_t old_off;
	const size_t old_nrecs = NC_get_numrecs(old);

	/* Don't parallelize this loop */
	for(recno = (int)old_nrecs -1; recno >= 0; recno--)
	{
	/* Don't parallelize this loop */
	for(varid = (int)old->vars.nelems -1; varid >= 0; varid--)
	{
		gnu_varp = *(gnu_varpp + varid);
		if(!IS_RECVAR(gnu_varp))
		{
			/* skip non-record variables on this pass */
			continue;
		}
		/* else */

		/* else, a pre-existing variable */
		old_varp = *(old_varpp + varid);
		gnu_off = gnu_varp->begin + (off_t)(gnu->recsize * (size_t)recno);
		old_off = old_varp->begin + (off_t)(old->recsize * (size_t)recno);

		if(gnu_off == old_off)
			continue; 	/* nothing to do */

		assert(gnu_off > old_off);

		status = ncio_move(gnu->nciop, gnu_off, old_off,
			 old_varp->len, 0);

		if(status != NC_NOERR)
			return status;

	}
	}

	NC_set_numrecs(gnu, old_nrecs);

	return NC_NOERR;
}


/*
 * Move the "non record" variables "out".
 * Fill as needed.
 */
static int
move_vars_r(NC3_INFO *gnu, NC3_INFO *old)
{
	int err, status=NC_NOERR;
	int varid;
	NC_var **gnu_varpp = (NC_var **)gnu->vars.value;
	NC_var **old_varpp = (NC_var **)old->vars.value;
	NC_var *gnu_varp;
	NC_var *old_varp;
	off_t gnu_off;
	off_t old_off;

	/* Don't parallelize this loop */
	for(varid = (int)old->vars.nelems -1;
		 varid >= 0; varid--)
	{
		gnu_varp = *(gnu_varpp + varid);
		if(IS_RECVAR(gnu_varp))
		{
			/* skip record variables on this pass */
			continue;
		}
		/* else */

		old_varp = *(old_varpp + varid);
		gnu_off = gnu_varp->begin;
		old_off = old_varp->begin;

		if (gnu_off > old_off) {
		    err = ncio_move(gnu->nciop, gnu_off, old_off,
			               old_varp->len, 0);
		    if (status == NC_NOERR) status = err;
		}
	}
	return status;
}


/*
 * Given a valid ncp, return NC_EVARSIZE if any variable has a bad len
 * (product of non-rec dim sizes too large), else return NC_NOERR.
 */
int
NC_check_vlens(NC3_INFO *ncp)
{
    NC_var **vpp;
    /* maximum permitted variable size (or size of one record's worth
       of a record variable) in bytes.  This is different for format 1
       and format 2. */
    long long vlen_max;
    size_t ii;
    size_t large_vars_count;
    size_t rec_vars_count;
    int last = 0;

    if(ncp->vars.nelems == 0)
	return NC_NOERR;

    if (fIsSet(ncp->flags,NC_64BIT_DATA)) /* CDF-5 */
	vlen_max = X_INT64_MAX - 3; /* "- 3" handles rounded-up size */
    else if (fIsSet(ncp->flags,NC_64BIT_OFFSET) && sizeof(off_t) > 4)
	/* CDF2 format and LFS */
	vlen_max = X_UINT_MAX - 3; /* "- 3" handles rounded-up size */
    else /* CDF1 format */
	vlen_max = X_INT_MAX - 3;

    /* Loop through vars, first pass is for non-record variables.   */
    large_vars_count = 0;
    rec_vars_count = 0;
    vpp = ncp->vars.value;
    for (ii = 0; ii < ncp->vars.nelems; ii++, vpp++) {
	assert(vpp != NULL && *vpp != NULL);
	if( !IS_RECVAR(*vpp) ) {
	    last = 0;
	    if( NC_check_vlen(*vpp, vlen_max) == 0 ) {
                if (fIsSet(ncp->flags,NC_64BIT_DATA)) /* too big for CDF-5 */
                    return NC_EVARSIZE;
		large_vars_count++;
		last = 1;
	    }
	} else {
	  rec_vars_count++;
	}
    }
    /* OK if last non-record variable size too large, since not used to
       compute an offset */
    if( large_vars_count > 1) { /* only one "too-large" variable allowed */
      return NC_EVARSIZE;
    }
    /* and it has to be the last one */
    if( large_vars_count == 1 && last == 0) {
      return NC_EVARSIZE;
    }
    if( rec_vars_count > 0 ) {
	/* and if it's the last one, there can't be any record variables */
	if( large_vars_count == 1 && last == 1) {
	    return NC_EVARSIZE;
	}
	/* Loop through vars, second pass is for record variables.   */
	large_vars_count = 0;
	vpp = ncp->vars.value;
	for (ii = 0; ii < ncp->vars.nelems; ii++, vpp++) {
	    if( IS_RECVAR(*vpp) ) {
		last = 0;
		if( NC_check_vlen(*vpp, vlen_max) == 0 ) {
                    if (fIsSet(ncp->flags,NC_64BIT_DATA)) /* too big for CDF-5 */
                        return NC_EVARSIZE;
		    large_vars_count++;
		    last = 1;
		}
	    }
	}
	/* OK if last record variable size too large, since not used to
	   compute an offset */
	if( large_vars_count > 1) { /* only one "too-large" variable allowed */
	    return NC_EVARSIZE;
	}
	/* and it has to be the last one */
	if( large_vars_count == 1 && last == 0) {
	    return NC_EVARSIZE;
	}
    }
    return NC_NOERR;
}

/*----< NC_check_voffs() >---------------------------------------------------*/
/*
 * Given a valid ncp, check whether the file starting offsets (begin) of all
 * variables follows the same increasing order as they were defined.
 */
int
NC_check_voffs(NC3_INFO *ncp)
{
    size_t i;
    off_t prev_off;
    NC_var *varp;

    if (ncp->vars.nelems == 0) return NC_NOERR;

    /* Loop through vars, first pass is for non-record variables */
    prev_off = ncp->begin_var;
    for (i=0; i<ncp->vars.nelems; i++) {
        varp = ncp->vars.value[i];
        if (IS_RECVAR(varp)) continue;

        if (varp->begin < prev_off) {
#if 0
            fprintf(stderr,"Variable \"%s\" begin offset (%lld) is less than previous variable end offset (%lld)\n", varp->name->cp, varp->begin, prev_off);
#endif
            return NC_ENOTNC;
        }
        prev_off = varp->begin + varp->len;
    }

    if (ncp->begin_rec < prev_off) {
#if 0
        fprintf(stderr,"Record variable section begin offset (%lld) is less than fix-sized variable section end offset (%lld)\n", varp->begin, prev_off);
#endif
        return NC_ENOTNC;
    }

    /* Loop through vars, second pass is for record variables */
    prev_off = ncp->begin_rec;
    for (i=0; i<ncp->vars.nelems; i++) {
        varp = ncp->vars.value[i];
        if (!IS_RECVAR(varp)) continue;

        if (varp->begin < prev_off) {
#if 0
            fprintf(stderr,"Variable \"%s\" begin offset (%lld) is less than previous variable end offset (%lld)\n", varp->name->cp, varp->begin, prev_off);
#endif
            return NC_ENOTNC;
        }
        prev_off = varp->begin + varp->len;
    }

    return NC_NOERR;
}

/*
 *  End define mode.
 *  Common code for ncendef, ncclose(endef)
 *  Flushes I/O buffers.
 */
static int
NC_endef(NC3_INFO *ncp,
	size_t h_minfree, size_t v_align,
	size_t v_minfree, size_t r_align)
{
	int status = NC_NOERR;

	assert(!NC_readonly(ncp));
	assert(NC_indef(ncp));

	status = NC_check_vlens(ncp);
	if(status != NC_NOERR)
	    return status;
	status = NC_begins(ncp, h_minfree, v_align, v_minfree, r_align);
	if(status != NC_NOERR)
	    return status;
	status = NC_check_voffs(ncp);
	if(status != NC_NOERR)
	    return status;

	if(ncp->old != NULL)
	{
		/* a plain redef, not a create */
		assert(!NC_IsNew(ncp));
		assert(fIsSet(ncp->state, NC_INDEF));
		assert(ncp->begin_rec >= ncp->old->begin_rec);
		assert(ncp->begin_var >= ncp->old->begin_var);

		if(ncp->vars.nelems != 0)
		{
		if(ncp->begin_rec > ncp->old->begin_rec)
		{
			status = move_recs_r(ncp, ncp->old);
			if(status != NC_NOERR)
				return status;
			if(ncp->begin_var > ncp->old->begin_var)
			{
				status = move_vars_r(ncp, ncp->old);
				if(status != NC_NOERR)
					return status;
			}
			/* else if (ncp->begin_var == ncp->old->begin_var) { NOOP } */
		}
		else
                {
			/* due to fixed variable alignment, it is possible that header
                           grows but begin_rec did not change */
			if(ncp->begin_var > ncp->old->begin_var)
			{
				status = move_vars_r(ncp, ncp->old);
				if(status != NC_NOERR)
					return status;
			}
		 	/* Even if (ncp->begin_rec == ncp->old->begin_rec)
			   and     (ncp->begin_var == ncp->old->begin_var)
			   might still have added a new record variable */
		        if(ncp->recsize > ncp->old->recsize)
			{
			        status = move_recs_r(ncp, ncp->old);
				if(status != NC_NOERR)
				      return status;
			}
		}
		}
	}

	status = write_NC(ncp);
	if(status != NC_NOERR)
		return status;

	/* fill mode is now per variable */
	{
		if(NC_IsNew(ncp))
		{
			status = fillerup(ncp);
			if(status != NC_NOERR)
				return status;

		}
		else if(ncp->old == NULL ? 0
                                         : (ncp->vars.nelems > ncp->old->vars.nelems))
          {
            status = fill_added(ncp, ncp->old);
            if(status != NC_NOERR)
              return status;
            status = fill_added_recs(ncp, ncp->old);
            if(status != NC_NOERR)
              return status;
          }
	}

	if(ncp->old != NULL)
	{
		free_NC3INFO(ncp->old);
		ncp->old = NULL;
	}

	fClr(ncp->state, NC_CREAT | NC_INDEF);

	return ncio_sync(ncp->nciop);
}


/*
 * Compute the expected size of the file.
 */
int
NC_calcsize(const NC3_INFO *ncp, off_t *calcsizep)
{
	NC_var **vpp = (NC_var **)ncp->vars.value;
	NC_var *last_fix = NULL;	/* last "non-record" var */
	int numrecvars = 0;	/* number of record variables */

	if(ncp->vars.nelems == 0) { /* no non-record variables and
				       no record variables */
	    *calcsizep = (off_t)ncp->xsz; /* size of header */
	    return NC_NOERR;
	}

	if (vpp)
	{
		NC_var *const *const end = &vpp[ncp->vars.nelems];
		for( /*NADA*/; vpp < end; vpp++) {
		    if(IS_RECVAR(*vpp)) {
			numrecvars++;
		    } else {
			last_fix = *vpp;
		    }
		}
	}

	if(numrecvars == 0) {
	    off_t varsize;
	    assert(last_fix != NULL);
	    varsize = last_fix->len;
	    if(last_fix->len == X_UINT_MAX) { /* huge last fixed var */
		varsize = 1;
		for(size_t i = 0; i < last_fix->ndims; i++ ) {
		    varsize *= (last_fix->shape ? last_fix->shape[i] : 1);
		}
	    }
	    *calcsizep = last_fix->begin + varsize;
	    /*last_var = last_fix;*/
	} else {       /* we have at least one record variable */
	    *calcsizep = ncp->begin_rec + (off_t)(ncp->numrecs * ncp->recsize);
	}

	return NC_NOERR;
}

/* Public */

#if 0 /* no longer needed */
int NC3_new_nc(NC3_INFO** ncpp)
{
	NC *nc;
	NC3_INFO* nc3;

	ncp = (NC *) malloc(sizeof(NC));
	if(ncp == NULL)
		return NC_ENOMEM;
	(void) memset(ncp, 0, sizeof(NC));

	ncp->xsz = MIN_NC_XSZ;
	assert(ncp->xsz == ncx_len_NC(ncp,0));

        if(ncpp) *ncpp = ncp;
        return NC_NOERR;

}
#endif

/* WARNING: SIGNATURE CHANGE */
int
NC3_create(const char *path, int ioflags, size_t initialsz, int basepe,
           size_t *chunksizehintp, void *parameters,
           const NC_Dispatch *dispatch, int ncid)
{
	int status = NC_NOERR;
	void *xp = NULL;
	size_t sizeof_off_t = 0;
        NC *nc;
	NC3_INFO* nc3 = NULL;

        /* Find NC struct for this file. */
        if ((status = NC_check_id(ncid, &nc)))
            return status;

	/* Create our specific NC3_INFO instance */
	nc3 = new_NC3INFO(chunksizehintp);

#if ALWAYS_NC_SHARE /* DEBUG */
	fSet(ioflags, NC_SHARE);
#endif

	/*
	 * Only pe 0 is valid
	 */
	if(basepe != 0) {
            if(nc3) free(nc3);
            return NC_EINVAL;
        }
	assert(nc3->flags == 0);

	/* Now we can set min size */
	if (fIsSet(ioflags, NC_64BIT_DATA))
	    nc3->xsz = MIN_NC5_XSZ; /* CDF-5 has minimum 16 extra bytes */
	else
	    nc3->xsz = MIN_NC3_XSZ;

	if (fIsSet(ioflags, NC_64BIT_OFFSET)) {
	    fSet(nc3->flags, NC_64BIT_OFFSET);
	    sizeof_off_t = 8;
	} else if (fIsSet(ioflags, NC_64BIT_DATA)) {
	    fSet(nc3->flags, NC_64BIT_DATA);
	    sizeof_off_t = 8;
	} else {
	  sizeof_off_t = 4;
	}

	assert(nc3->xsz == ncx_len_NC(nc3,sizeof_off_t));

        status =  ncio_create(path, ioflags, initialsz,
			      0, nc3->xsz, &nc3->chunk, NULL,
			      &nc3->nciop, &xp);
	if(status != NC_NOERR)
	{
		/* translate error status */
		if(status == EEXIST)
			status = NC_EEXIST;
		goto unwind_alloc;
	}

	fSet(nc3->state, NC_CREAT);

	if(fIsSet(nc3->nciop->ioflags, NC_SHARE))
	{
		/*
		 * NC_SHARE implies sync up the number of records as well.
		 * (File format version one.)
		 * Note that other header changes are not shared
		 * automatically.  Some sort of IPC (external to this package)
		 * would be used to trigger a call to nc_sync().
		 */
		fSet(nc3->state, NC_NSYNC);
	}

	status = ncx_put_NC(nc3, &xp, sizeof_off_t, nc3->xsz);
	if(status != NC_NOERR)
		goto unwind_ioc;

	if(chunksizehintp != NULL)
		*chunksizehintp = nc3->chunk;

	/* Link nc3 and nc */
        NC3_DATA_SET(nc,nc3);
	nc->int_ncid = nc3->nciop->fd;

	return NC_NOERR;

unwind_ioc:
	if(nc3 != NULL) {
	    (void) ncio_close(nc3->nciop, 1); /* N.B.: unlink */
	    nc3->nciop = NULL;
	}
	/*FALLTHRU*/
unwind_alloc:
	free_NC3INFO(nc3);
	if(nc)
            NC3_DATA_SET(nc,NULL);
	return status;
}

#if 0
/* This function sets a default create flag that will be logically
   or'd to whatever flags are passed into nc_create for all future
   calls to nc_create.
   Valid default create flags are NC_64BIT_OFFSET, NC_CDF5, NC_CLOBBER,
   NC_LOCK, NC_SHARE. */
int
nc_set_default_format(int format, int *old_formatp)
{
    /* Return existing format if desired. */
    if (old_formatp)
      *old_formatp = default_create_format;

    /* Make sure only valid format is set. */
#ifdef USE_NETCDF4
    if (format != NC_FORMAT_CLASSIC && format != NC_FORMAT_64BIT_OFFSET &&
	format != NC_FORMAT_NETCDF4 && format != NC_FORMAT_NETCDF4_CLASSIC)
      return NC_EINVAL;
#else
    if (format != NC_FORMAT_CLASSIC && format != NC_FORMAT_64BIT_OFFSET
#ifdef NETCDF_ENABLE_CDF5
        && format != NC_FORMAT_CDF5
#endif
        )
      return NC_EINVAL;
#endif
    default_create_format = format;
    return NC_NOERR;
}
#endif

int
NC3_open(const char *path, int ioflags, int basepe, size_t *chunksizehintp,
         void *parameters, const NC_Dispatch *dispatch, int ncid)
{
	int status;
	NC3_INFO* nc3 = NULL;
        NC *nc;

        /* Find NC struct for this file. */
        if ((status = NC_check_id(ncid, &nc)))
            return status;

	/* Create our specific NC3_INFO instance */
	nc3 = new_NC3INFO(chunksizehintp);

#if ALWAYS_NC_SHARE /* DEBUG */
	fSet(ioflags, NC_SHARE);
#endif

	/*
	 * Only pe 0 is valid.
	 */
	if(basepe != 0) {
            if(nc3) {
                free(nc3);
                nc3 = NULL;
            }
            status = NC_EINVAL;
            goto unwind_alloc;
        }

        status = ncio_open(path, ioflags, 0, 0, &nc3->chunk, parameters,
			       &nc3->nciop, NULL);
	if(status)
		goto unwind_alloc;

	assert(nc3->state == 0);

	if(fIsSet(nc3->nciop->ioflags, NC_SHARE))
	{
		/*
		 * NC_SHARE implies sync up the number of records as well.
		 * (File format version one.)
		 * Note that other header changes are not shared
		 * automatically.  Some sort of IPC (external to this package)
		 * would be used to trigger a call to nc_sync().
		 */
		fSet(nc3->state, NC_NSYNC);
	}

	status = nc_get_NC(nc3);
	if(status != NC_NOERR)
		goto unwind_ioc;

	if(chunksizehintp != NULL)
		*chunksizehintp = nc3->chunk;

	/* Link nc3 and nc */
        NC3_DATA_SET(nc,nc3);
	nc->int_ncid = nc3->nciop->fd;

	return NC_NOERR;

unwind_ioc:
	if(nc3) {
    	    (void) ncio_close(nc3->nciop, 0);
	    nc3->nciop = NULL;
	}
	/*FALLTHRU*/
unwind_alloc:
	free_NC3INFO(nc3);
	if(nc)
            NC3_DATA_SET(nc,NULL);
	return status;
}

int
NC3__enddef(int ncid,
	size_t h_minfree, size_t v_align,
	size_t v_minfree, size_t r_align)
{
	int status;
	NC *nc;
	NC3_INFO* nc3;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
	  return status;
	nc3 = NC3_DATA(nc);

	if(!NC_indef(nc3))
		return(NC_ENOTINDEFINE);

	return (NC_endef(nc3, h_minfree, v_align, v_minfree, r_align));
}

/*
 * In data mode, same as ncclose.
 * In define mode, restore previous definition.
 * In create, remove the file.
 */
int
NC3_abort(int ncid)
{
	int status;
	NC *nc;
	NC3_INFO* nc3;
	int doUnlink = 0;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
	    return status;
	nc3 = NC3_DATA(nc);

	doUnlink = NC_IsNew(nc3);

	if(nc3->old != NULL)
	{
		/* a plain redef, not a create */
		assert(!NC_IsNew(nc3));
		assert(fIsSet(nc3->state, NC_INDEF));
		free_NC3INFO(nc3->old);
		nc3->old = NULL;
		fClr(nc3->state, NC_INDEF);
	}
	else if(!NC_readonly(nc3))
	{
		status = NC_sync(nc3);
		if(status != NC_NOERR)
			return status;
	}


	(void) ncio_close(nc3->nciop, doUnlink);
	nc3->nciop = NULL;

	free_NC3INFO(nc3);
	if(nc)
            NC3_DATA_SET(nc,NULL);

	return NC_NOERR;
}

int
NC3_close(int ncid, void* params)
{
	int status = NC_NOERR;
	NC *nc;
	NC3_INFO* nc3;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
	    return status;
	nc3 = NC3_DATA(nc);

	if(NC_indef(nc3))
	{
		status = NC_endef(nc3, 0, 1, 0, 1); /* TODO: defaults */
		if(status != NC_NOERR )
		{
			(void) NC3_abort(ncid);
			return status;
		}
	}
	else if(!NC_readonly(nc3))
	{
		status = NC_sync(nc3);
		/* flush buffers before any filesize comparisons */
		(void) ncio_sync(nc3->nciop);
	}

	/*
	 * If file opened for writing and filesize is less than
	 * what it should be (due to previous use of NOFILL mode),
	 * pad it to correct size, as reported by NC_calcsize().
	 */
	if (status == NC_NOERR) {
	    off_t filesize; 	/* current size of open file */
	    off_t calcsize;	/* calculated file size, from header */
	    status = ncio_filesize(nc3->nciop, &filesize);
	    if(status != NC_NOERR)
		return status;
	    status = NC_calcsize(nc3, &calcsize);
	    if(status != NC_NOERR)
		return status;
	    if(filesize < calcsize && !NC_readonly(nc3)) {
		status = ncio_pad_length(nc3->nciop, calcsize);
		if(status != NC_NOERR)
		    return status;
	    }
	}

	if(params != NULL && (nc->mode & NC_INMEMORY) != 0) {
	    NC_memio* memio = (NC_memio*)params;
            /* Extract the final memory size &/or contents */
            status = memio_extract(nc3->nciop,&memio->size,&memio->memory);
        }

	(void) ncio_close(nc3->nciop, 0);
	nc3->nciop = NULL;

	free_NC3INFO(nc3);
        NC3_DATA_SET(nc,NULL);

	return status;
}

int
NC3_redef(int ncid)
{
	int status;
	NC *nc;
	NC3_INFO* nc3;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	nc3 = NC3_DATA(nc);

	if(NC_readonly(nc3))
		return NC_EPERM;

	if(NC_indef(nc3))
		return NC_EINDEFINE;


	if(fIsSet(nc3->nciop->ioflags, NC_SHARE))
	{
		/* read in from disk */
		status = read_NC(nc3);
		if(status != NC_NOERR)
			return status;
	}

	nc3->old = dup_NC3INFO(nc3);
	if(nc3->old == NULL)
		return NC_ENOMEM;

	fSet(nc3->state, NC_INDEF);

	return NC_NOERR;
}


int
NC3_inq(int ncid,
	int *ndimsp,
	int *nvarsp,
	int *nattsp,
	int *xtendimp)
{
	int status;
	NC *nc;
	NC3_INFO* nc3;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	nc3 = NC3_DATA(nc);

	if(ndimsp != NULL)
		*ndimsp = (int) nc3->dims.nelems;
	if(nvarsp != NULL)
		*nvarsp = (int) nc3->vars.nelems;
	if(nattsp != NULL)
		*nattsp = (int) nc3->attrs.nelems;
	if(xtendimp != NULL)
		*xtendimp = find_NC_Udim(&nc3->dims, NULL);

	return NC_NOERR;
}

int
NC3_inq_unlimdim(int ncid, int *xtendimp)
{
	int status;
	NC *nc;
	NC3_INFO* nc3;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	nc3 = NC3_DATA(nc);

	if(xtendimp != NULL)
		*xtendimp = find_NC_Udim(&nc3->dims, NULL);

	return NC_NOERR;
}

int
NC3_sync(int ncid)
{
	int status;
	NC *nc;
	NC3_INFO* nc3;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	nc3 = NC3_DATA(nc);

	if(NC_indef(nc3))
		return NC_EINDEFINE;

	if(NC_readonly(nc3))
	{
		return read_NC(nc3);
	}
	/* else, read/write */

	status = NC_sync(nc3);
	if(status != NC_NOERR)
		return status;

	status = ncio_sync(nc3->nciop);
	if(status != NC_NOERR)
		return status;

#ifdef USE_FSYNC
	/* may improve concurrent access, but slows performance if
	 * called frequently */
#ifndef _WIN32
	status = fsync(nc3->nciop->fd);
#else
	status = _commit(nc3->nciop->fd);
#endif	/* _WIN32 */
#endif	/* USE_FSYNC */

	return status;
}


int
NC3_set_fill(int ncid,
	int fillmode, int *old_mode_ptr)
{
	int i, status;
	NC *nc;
	NC3_INFO* nc3;
	int oldmode;

	status = NC_check_id(ncid, &nc);
	if(status != NC_NOERR)
		return status;
	nc3 = NC3_DATA(nc);

	if(NC_readonly(nc3))
		return NC_EPERM;

	oldmode = fIsSet(nc3->state, NC_NOFILL) ? NC_NOFILL : NC_FILL;

	if(fillmode == NC_NOFILL)
	{
		fSet(nc3->state, NC_NOFILL);
	}
	else if(fillmode == NC_FILL)
	{
		if(fIsSet(nc3->state, NC_NOFILL))
		{
			/*
			 * We are changing back to fill mode
			 * so do a sync
			 */
			status = NC_sync(nc3);
			if(status != NC_NOERR)
				return status;
		}
		fClr(nc3->state, NC_NOFILL);
	}
	else
	{
		return NC_EINVAL; /* Invalid fillmode */
	}

	if(old_mode_ptr != NULL)
		*old_mode_ptr = oldmode;

	/* loop thru all variables to set/overwrite its fill mode */
	for (i=0; i<nc3->vars.nelems; i++)
		nc3->vars.value[i]->no_fill = (fillmode == NC_NOFILL);

	/* once the file's fill mode is set, any new variables defined after
	 * this call will check NC_dofill(nc3) and set their no_fill accordingly.
	 * See NC3_def_var() */

	return NC_NOERR;
}

/**
 * Return the file format.
 *
 * \param ncid the ID of the open file.

 * \param formatp a pointer that gets the format. Ignored if NULL.
 *
 * \returns NC_NOERR No error.
 * \returns NC_EBADID Bad ncid.
 * \internal
 * \author Ed Hartnett, Dennis Heimbigner
 */
int
NC3_inq_format(int ncid, int *formatp)
{
   int status;
   NC *nc;
   NC3_INFO* nc3;

   status = NC_check_id(ncid, &nc);
   if(status != NC_NOERR)
      return status;
   nc3 = NC3_DATA(nc);

   /* Why even call this function with no format pointer? */
   if (!formatp)
      return NC_NOERR;

   /* only need to check for netCDF-3 variants, since this is never called for netCDF-4 files */
#ifdef NETCDF_ENABLE_CDF5
   if (fIsSet(nc3->flags, NC_64BIT_DATA))
      *formatp = NC_FORMAT_CDF5;
   else
#endif
      if (fIsSet(nc3->flags, NC_64BIT_OFFSET))
         *formatp = NC_FORMAT_64BIT_OFFSET;
      else
         *formatp = NC_FORMAT_CLASSIC;
   return NC_NOERR;
}

/**
 * Return the extended format (i.e. the dispatch model), plus the mode
 * associated with an open file.
 *
 * \param ncid the ID of the open file.
 * \param formatp a pointer that gets the extended format. Note that
 * this is not the same as the format provided by nc_inq_format(). The
 * extended format indicates the dispatch layer model. Classic, 64-bit
 * offset, and CDF5 files all have an extended format of
 * ::NC_FORMATX_NC3. Ignored if NULL.
 * \param modep a pointer that gets the open/create mode associated with
 * this file. Ignored if NULL.
 *
 * \returns NC_NOERR No error.
 * \returns NC_EBADID Bad ncid.
 * \internal
 * \author Dennis Heimbigner
 */
int
NC3_inq_format_extended(int ncid, int *formatp, int *modep)
{
   int status;
   NC *nc;

   status = NC_check_id(ncid, &nc);
   if(status != NC_NOERR)
      return status;
   if(formatp) *formatp = NC_FORMATX_NC3;
   if(modep) *modep = nc->mode;
   return NC_NOERR;
}

/**
 * Determine name and size of netCDF type. This netCDF-4 function
 * proved so popular that a netCDF-classic version is provided. You're
 * welcome.
 *
 * \param ncid The ID of an open file.
 * \param typeid The ID of a netCDF type.
 * \param name Pointer that will get the name of the type. Maximum
 * size will be NC_MAX_NAME. Ignored if NULL.
 * \param size Pointer that will get size of type in bytes. Ignored if
 * null.
 *
 * \returns NC_NOERR No error.
 * \returns NC_EBADID Bad ncid.
 * \returns NC_EBADTYPE Bad typeid.
 * \internal
 * \author Ed Hartnett
 */
int
NC3_inq_type(int ncid, nc_type typeid, char *name, size_t *size)
{
   NC *ncp;
   int stat = NC_check_id(ncid, &ncp);
   if (stat != NC_NOERR)
      return stat;

   if(typeid < NC_BYTE || typeid > NC_STRING)
      return NC_EBADTYPE;

   /* Give the user the values they want. */
   if (name)
      strcpy(name, NC_atomictypename(typeid));
   if (size)
      *size = NC_atomictypelen(typeid);

   return NC_NOERR;
}

/**
 * This is an obsolete form of nc_delete(), supported for backwards
 * compatibility.
 *
 * @param path Filename to delete.
 * @param basepe Must be 0.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EIO Couldn't delete file.
 * @return ::NC_EINVAL Invaliod basepe. Must be 0.
 * @author Glenn Davis, Ed Hartnett
 */
int
nc_delete_mp(const char * path, int basepe)
{
	NC *nc;
	int status;
	int ncid;

	status = nc_open(path,NC_NOWRITE,&ncid);
        if(status) return status;

	status = NC_check_id(ncid,&nc);
        if(status) return status;

	/*
	 * Only pe 0 is valid.
	 */
	if(basepe != 0)
		return NC_EINVAL;

	(void) nc_close(ncid);
	if(unlink(path) == -1) {
	    return NC_EIO;	/* No more specific error code is appropriate */
	}
	return NC_NOERR;
}

int
nc_delete(const char * path)
{
        return nc_delete_mp(path, 0);
}

/*----< NC3_inq_default_fill_value() >---------------------------------------*/
/* copy the default fill value to the memory space pointed by fillp */
int
NC3_inq_default_fill_value(int xtype, void *fillp)
{
    if (fillp == NULL) return NC_NOERR;

    switch(xtype) {
        case NC_CHAR   :               *(char*)fillp = NC_FILL_CHAR;   break;
        case NC_BYTE   :        *(signed char*)fillp = NC_FILL_BYTE;   break;
        case NC_SHORT  :              *(short*)fillp = NC_FILL_SHORT;  break;
        case NC_INT    :                *(int*)fillp = NC_FILL_INT;    break;
        case NC_FLOAT  :              *(float*)fillp = NC_FILL_FLOAT;  break;
        case NC_DOUBLE :             *(double*)fillp = NC_FILL_DOUBLE; break;
        case NC_UBYTE  :      *(unsigned char*)fillp = NC_FILL_UBYTE;  break;
        case NC_USHORT :     *(unsigned short*)fillp = NC_FILL_USHORT; break;
        case NC_UINT   :       *(unsigned int*)fillp = NC_FILL_UINT;   break;
        case NC_INT64  :          *(long long*)fillp = NC_FILL_INT64;  break;
        case NC_UINT64 : *(unsigned long long*)fillp = NC_FILL_UINT64; break;
        default : return NC_EBADTYPE;
    }
    return NC_NOERR;
}


/*----< NC3_inq_var_fill() >-------------------------------------------------*/
/* inquire the fill value of a variable */
int
NC3_inq_var_fill(const NC_var *varp, void *fill_value)
{
    NC_attr **attrpp = NULL;

    if (fill_value == NULL) return NC_EINVAL;

    /*
     * find fill value
     */
    attrpp = NC_findattr(&varp->attrs, NC_FillValue);
    if ( attrpp != NULL ) {
        const void *xp;
        /* User defined fill value */
        if ( (*attrpp)->type != varp->type || (*attrpp)->nelems != 1 )
            return NC_EBADTYPE;

        xp = (*attrpp)->xvalue;
        /* value stored in xvalue is in external representation, may need byte-swap */
        switch(varp->type) {
            case NC_CHAR:   return ncx_getn_text               (&xp, 1,               (char*)fill_value);
            case NC_BYTE:   return ncx_getn_schar_schar        (&xp, 1,        (signed char*)fill_value);
            case NC_UBYTE:  return ncx_getn_uchar_uchar        (&xp, 1,      (unsigned char*)fill_value);
            case NC_SHORT:  return ncx_getn_short_short        (&xp, 1,              (short*)fill_value);
            case NC_USHORT: return ncx_getn_ushort_ushort      (&xp, 1,     (unsigned short*)fill_value);
            case NC_INT:    return ncx_getn_int_int            (&xp, 1,                (int*)fill_value);
            case NC_UINT:   return ncx_getn_uint_uint          (&xp, 1,       (unsigned int*)fill_value);
            case NC_FLOAT:  return ncx_getn_float_float        (&xp, 1,              (float*)fill_value);
            case NC_DOUBLE: return ncx_getn_double_double      (&xp, 1,             (double*)fill_value);
            case NC_INT64:  return ncx_getn_longlong_longlong  (&xp, 1,          (long long*)fill_value);
            case NC_UINT64: return ncx_getn_ulonglong_ulonglong(&xp, 1, (unsigned long long*)fill_value);
            default: return NC_EBADTYPE;
        }
    }
    else {
        /* use the default */
        switch(varp->type){
            case NC_CHAR:                *(char *)fill_value = NC_FILL_CHAR;
                 break;
            case NC_BYTE:          *(signed char *)fill_value = NC_FILL_BYTE;
                 break;
            case NC_SHORT:               *(short *)fill_value = NC_FILL_SHORT;
                 break;
            case NC_INT:                   *(int *)fill_value = NC_FILL_INT;
                 break;
            case NC_UBYTE:       *(unsigned char *)fill_value = NC_FILL_UBYTE;
                 break;
            case NC_USHORT:     *(unsigned short *)fill_value = NC_FILL_USHORT;
                 break;
            case NC_UINT:         *(unsigned int *)fill_value = NC_FILL_UINT;
                 break;
            case NC_INT64:           *(long long *)fill_value = NC_FILL_INT64;
                 break;
            case NC_UINT64: *(unsigned long long *)fill_value = NC_FILL_UINT64;
                 break;
            case NC_FLOAT:               *(float *)fill_value = NC_FILL_FLOAT;
                 break;
            case NC_DOUBLE:             *(double *)fill_value = NC_FILL_DOUBLE;
                 break;
            default:
                 return NC_EINVAL;
        }
    }
    return NC_NOERR;
}
