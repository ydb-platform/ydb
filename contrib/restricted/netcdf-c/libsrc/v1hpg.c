/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#if HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "nc3internal.h"
#include "rnd.h"
#include "ncx.h"

/*
 * This module defines the external representation
 * of the "header" of a netcdf version one file and
 * the version two variant that uses 64-bit file
 * offsets instead of the 32-bit file offsets in version
 * one files.
 * For each of the components of the NC structure,
 * There are (static) ncx_len_XXX(), v1h_put_XXX()
 * and v1h_get_XXX() functions. These define the
 * external representation of the components.
 * The exported entry points for the whole NC structure
 * are built up from these.
 */


/*
 * "magic number" at beginning of file: 0x43444601 (big endian)
 * assert(sizeof(ncmagic) % X_ALIGN == 0);
 */
static const schar ncmagic[] = {'C', 'D', 'F', 0x02};
static const schar ncmagic1[] = {'C', 'D', 'F', 0x01};
static const schar ncmagic5[] = {'C', 'D', 'F', 0x05};

/*
 * v1hs == "Version 1 Header Stream"
 *
 * The netcdf file version 1 header is
 * of unknown and potentially unlimited size.
 * So, we don't know how much to get() on
 * the initial read. We build a stream, 'v1hs'
 * on top of ncio to do the header get.
 */
typedef struct v1hs {
	ncio *nciop;
	off_t offset;	/* argument to nciop->get() */
	size_t extent;	/* argument to nciop->get() */
	int flags;	/* set to RGN_WRITE for write */
        int version;    /* format variant: NC_FORMAT_CLASSIC, NC_FORMAT_64BIT_OFFSET or NC_FORMAT_CDF5 */
	void *base;	/* beginning of current buffer */
	void *pos;	/* current position in buffer */
	void *end;	/* end of current buffer = base + extent */
} v1hs;


/*
 * Release the stream, invalidate buffer
 */
static int
rel_v1hs(v1hs *gsp)
{
	int status;
	if(gsp->offset == OFF_NONE || gsp->base == NULL)
        return NC_NOERR;
	status = ncio_rel(gsp->nciop, gsp->offset,
			 gsp->flags == RGN_WRITE ? RGN_MODIFIED : 0);
	gsp->end = NULL;
	gsp->pos = NULL;
	gsp->base = NULL;
	return status;
}


/*
 * Release the current chunk and get the next one.
 * Also used for initialization when gsp->base == NULL.
 */
static int
fault_v1hs(v1hs *gsp, size_t extent)
{
	int status;

	if(gsp->base != NULL)
	{
		const ptrdiff_t incr = (char *)gsp->pos - (char *)gsp->base;
		status = rel_v1hs(gsp);
		if(status)
			return status;
		gsp->offset += incr;
	}

	if(extent > gsp->extent)
		gsp->extent = extent;

	status = ncio_get(gsp->nciop,
		 	gsp->offset, gsp->extent,
			gsp->flags, &gsp->base);
	if(status)
		return status;

	gsp->pos = gsp->base;

	gsp->end = (char *)gsp->base + gsp->extent;
    return NC_NOERR;
}


/*
 * Ensure that 'nextread' bytes are available.
 */
static int
check_v1hs(v1hs *gsp, size_t nextread)
{

#if 0 /* DEBUG */
fprintf(stderr, "nextread %lu, remaining %lu\n",
	(unsigned long)nextread,
	(unsigned long)((char *)gsp->end - (char *)gsp->pos));
#endif
    if((char *)gsp->pos + nextread <= (char *)gsp->end)
	return NC_NOERR;

    return fault_v1hs(gsp, nextread);
}

/* End v1hs */

/* Write a size_t to the header */
static int
v1h_put_size_t(v1hs *psp, const size_t *sp)
{
	int status;
	if (psp->version == 5) /* all integers in CDF-5 are 64 bits */
		status = check_v1hs(psp, X_SIZEOF_INT64);
	else
		status = check_v1hs(psp, X_SIZEOF_SIZE_T);
	if(status != NC_NOERR)
 		return status;
        if (psp->version == 5) {
                unsigned long long tmp = (unsigned long long) (*sp);
		return ncx_put_uint64(&psp->pos, tmp);
        }
        else
	    return ncx_put_size_t(&psp->pos, sp);
}

/* Read a size_t from the header */
static int
v1h_get_size_t(v1hs *gsp, size_t *sp)
{
	int status;
	if (gsp->version == 5) /* all integers in CDF-5 are 64 bits */
		status = check_v1hs(gsp, X_SIZEOF_INT64);
	else
		status = check_v1hs(gsp, X_SIZEOF_SIZE_T);
	if(status != NC_NOERR)
		return status;
        if (gsp->version == 5) {
		unsigned long long tmp=0;
		status = ncx_get_uint64((const void **)(&gsp->pos), &tmp);
		*sp = (size_t)tmp;
		return status;
        }
        else
	    return ncx_get_size_t((const void **)(&gsp->pos), sp);
}

/* Begin nc_type */

#define X_SIZEOF_NC_TYPE X_SIZEOF_INT

/* Write a nc_type to the header */
static int
v1h_put_nc_type(v1hs *psp, const nc_type *typep)
{
    const unsigned int itype = (unsigned int) *typep;
    int status = check_v1hs(psp, X_SIZEOF_INT);
    if(status != NC_NOERR) return status;
    status =  ncx_put_uint32(&psp->pos, itype);
    return status;
}


/* Read a nc_type from the header */
static int
v1h_get_nc_type(v1hs *gsp, nc_type *typep)
{
    unsigned int type = 0;
    int status = check_v1hs(gsp, X_SIZEOF_INT);
    if(status != NC_NOERR) return status;
    status =  ncx_get_uint32((const void**)(&gsp->pos), &type);
    if(status != NC_NOERR)
		return status;
/* Fix 35382
	assert(type == NC_BYTE
		|| type == NC_CHAR
		|| type == NC_SHORT
		|| type == NC_INT
		|| type == NC_FLOAT
		|| type == NC_DOUBLE
		|| type == NC_UBYTE
		|| type == NC_USHORT
		|| type == NC_UINT
		|| type == NC_INT64
		|| type == NC_UINT64
		|| type == NC_STRING);
*/
    if(type == NC_NAT || type > NC_MAX_ATOMIC_TYPE)
        return NC_EINVAL;
    else
	*typep = (nc_type) type;

    return NC_NOERR;
}

/* End nc_type */
/* Begin NCtype (internal tags) */

#define X_SIZEOF_NCTYPE X_SIZEOF_INT

/* Write a NCtype to the header */
static int
v1h_put_NCtype(v1hs *psp, NCtype type)
{
    const unsigned int itype = (unsigned int) type;
    int status = check_v1hs(psp, X_SIZEOF_INT);
    if(status != NC_NOERR) return status;
    status = ncx_put_uint32(&psp->pos, itype);
    return status;
}

/* Read a NCtype from the header */
static int
v1h_get_NCtype(v1hs *gsp, NCtype *typep)
{
    unsigned int type = 0;
    int status = check_v1hs(gsp, X_SIZEOF_INT);
    if(status != NC_NOERR) return status;
    status =  ncx_get_uint32((const void**)(&gsp->pos), &type);
    if(status != NC_NOERR) return status;
    /* else */
    *typep = (NCtype) type;
    return NC_NOERR;
}

/* End NCtype */
/* Begin NC_string */

/*
 * How much space will the xdr'd string take.
 * Formerly
NC_xlen_string(cdfstr)
 */
static size_t
ncx_len_NC_string(const NC_string *ncstrp, int version)
{
	size_t sz = (version == 5) ? X_SIZEOF_INT64 : X_SIZEOF_INT; /* nchars */

	assert(ncstrp != NULL);

	if(ncstrp->nchars != 0)
	{
#if 0
		assert(ncstrp->nchars % X_ALIGN == 0);
		sz += ncstrp->nchars;
#else
		sz += _RNDUP(ncstrp->nchars, X_ALIGN);
#endif
	}
	return sz;
}


/* Write a NC_string to the header */
static int
v1h_put_NC_string(v1hs *psp, const NC_string *ncstrp)
{
	int status;

#if 0
	assert(ncstrp->nchars % X_ALIGN == 0);
#endif

	status = v1h_put_size_t(psp, &ncstrp->nchars);
    if(status != NC_NOERR)
		return status;
	status = check_v1hs(psp, _RNDUP(ncstrp->nchars, X_ALIGN));
    if(status != NC_NOERR)
		return status;
	status = ncx_pad_putn_text(&psp->pos, ncstrp->nchars, ncstrp->cp);
    if(status != NC_NOERR)
		return status;

    return NC_NOERR;
}


/* Read a NC_string from the header */
static int
v1h_get_NC_string(v1hs *gsp, NC_string **ncstrpp)
{
	int status = 0;
	size_t nchars = 0;
	NC_string *ncstrp = NULL;
#if USE_STRICT_NULL_BYTE_HEADER_PADDING
        size_t padding = 0;        
#endif /* USE_STRICT_NULL_BYTE_HEADER_PADDING */

	status = v1h_get_size_t(gsp, &nchars);
	if(status != NC_NOERR)
		return status;

	ncstrp = new_NC_string(nchars, NULL);
	if(ncstrp == NULL)
	{
		return NC_ENOMEM;
	}

#if 0
/* assert(ncstrp->nchars == nchars || ncstrp->nchars - nchars < X_ALIGN); */
	assert(ncstrp->nchars % X_ALIGN == 0);
	status = check_v1hs(gsp, ncstrp->nchars);
#else

	status = check_v1hs(gsp, _RNDUP(ncstrp->nchars, X_ALIGN));
#endif
	if(status != NC_NOERR)
		goto unwind_alloc;

	status = ncx_pad_getn_text((const void **)(&gsp->pos),
		 nchars, ncstrp->cp);
	if(status != NC_NOERR)
		goto unwind_alloc;

#if USE_STRICT_NULL_BYTE_HEADER_PADDING
	padding = _RNDUP(X_SIZEOF_CHAR * ncstrp->nchars, X_ALIGN)
		- X_SIZEOF_CHAR * ncstrp->nchars;

	if (padding > 0) {
		/* CDF specification: Header padding uses null (\x00) bytes. */
		char pad[X_ALIGN-1];
		memset(pad, 0, X_ALIGN-1);
		if (memcmp((char*)gsp->pos-padding, pad, padding) != 0) {
			free_NC_string(ncstrp);
			return NC_ENULLPAD;
		}
	}
#endif

	*ncstrpp = ncstrp;

	return NC_NOERR;

unwind_alloc:
	free_NC_string(ncstrp);
	return status;
}

/* End NC_string */
/* Begin NC_dim */

/*
 * How much space will the xdr'd dim take.
 * Formerly
NC_xlen_dim(dpp)
 */
static size_t
ncx_len_NC_dim(const NC_dim *dimp, int version)
{
	size_t sz;

	assert(dimp != NULL);

	sz = ncx_len_NC_string(dimp->name, version);
	sz += (version == 5) ? X_SIZEOF_INT64 : X_SIZEOF_SIZE_T;

	return(sz);
}


/* Write a NC_dim to the header */
static int
v1h_put_NC_dim(v1hs *psp, const NC_dim *dimp)
{
	int status;

	status = v1h_put_NC_string(psp, dimp->name);
    if(status != NC_NOERR)
		return status;

	status = v1h_put_size_t(psp, &dimp->size);
    if(status != NC_NOERR)
		return status;

    return NC_NOERR;
}

/* Read a NC_dim from the header */
static int
v1h_get_NC_dim(v1hs *gsp, NC_dim **dimpp)
{
	int status;
	NC_string *ncstrp;
	NC_dim *dimp;

	status = v1h_get_NC_string(gsp, &ncstrp);
    if(status != NC_NOERR)
		return status;

	dimp = new_x_NC_dim(ncstrp);
	if(dimp == NULL)
	{
		status = NC_ENOMEM;
		goto unwind_name;
	}

	status = v1h_get_size_t(gsp, &dimp->size);
    if(status != NC_NOERR)
	{
		free_NC_dim(dimp); /* frees name */
		return status;
	}

	*dimpp = dimp;

    return NC_NOERR;

unwind_name:
	free_NC_string(ncstrp);
	return status;
}


/* How much space in the header is required for this NC_dimarray? */
static size_t
ncx_len_NC_dimarray(const NC_dimarray *ncap, int version)
{
	size_t xlen = X_SIZEOF_NCTYPE;	/* type */
	xlen += (version == 5) ? X_SIZEOF_INT64 : X_SIZEOF_SIZE_T; /* count */
	if(ncap == NULL)
		return xlen;
	/* else */
	{
		const NC_dim **dpp = (const NC_dim **)ncap->value;
		if (dpp)
		{
			const NC_dim *const *const end = &dpp[ncap->nelems];
			for(  /*NADA*/; dpp < end; dpp++)
			{
				xlen += ncx_len_NC_dim(*dpp,version);
			}
		}
	}
	return xlen;
}


/* Write a NC_dimarray to the header */
static int
v1h_put_NC_dimarray(v1hs *psp, const NC_dimarray *ncap)
{
	int status;

	assert(psp != NULL);

	if(ncap == NULL
#if 1
		/* Backward:
		 * This clause is for 'byte for byte'
		 * backward compatibility.
		 * Strickly speaking, it is 'bug for bug'.
		 */
		|| ncap->nelems == 0
#endif
		)
	{
		/*
		 * Handle empty netcdf
		 */
		const size_t nosz = 0;

		status = v1h_put_NCtype(psp, NC_UNSPECIFIED);
        if(status != NC_NOERR)
			return status;
		status = v1h_put_size_t(psp, &nosz);
        if(status != NC_NOERR)
			return status;
        return NC_NOERR;
	}
	/* else */

	status = v1h_put_NCtype(psp, NC_DIMENSION);
    if(status != NC_NOERR)
		return status;
	status = v1h_put_size_t(psp, &ncap->nelems);
    if(status != NC_NOERR)
		return status;

	{
		const NC_dim **dpp = (const NC_dim **)ncap->value;
		const NC_dim *const *const end = &dpp[ncap->nelems];
		for( /*NADA*/; dpp < end; dpp++)
		{
			status = v1h_put_NC_dim(psp, *dpp);
			if(status)
				return status;
		}
	}
    return NC_NOERR;
}


/* Read a NC_dimarray from the header */
static int
v1h_get_NC_dimarray(v1hs *gsp, NC_dimarray *ncap)
{
	int status;
	NCtype type = NC_UNSPECIFIED;

	assert(gsp != NULL && gsp->pos != NULL);
	assert(ncap != NULL);
	assert(ncap->value == NULL);

	status = v1h_get_NCtype(gsp, &type);
    if(status != NC_NOERR)
		return status;

	status = v1h_get_size_t(gsp, &ncap->nelems);
    if(status != NC_NOERR)
		return status;

	if(ncap->nelems == 0)
        return NC_NOERR;
	/* else */
	if(type != NC_DIMENSION)
		return EINVAL;

	if (ncap->nelems > SIZE_MAX / sizeof(NC_dim *))
		return NC_ERANGE;
	ncap->value = (NC_dim **) calloc(1,ncap->nelems * sizeof(NC_dim *));
	if(ncap->value == NULL)
		return NC_ENOMEM;
	ncap->nalloc = ncap->nelems;

	ncap->hashmap = NC_hashmapnew(ncap->nelems);

	{
		NC_dim **dpp = ncap->value;
		NC_dim *const *const end = &dpp[ncap->nelems];
		for( /*NADA*/; dpp < end; dpp++)
		{
			status = v1h_get_NC_dim(gsp, dpp);
			if(status)
			{
				ncap->nelems = (size_t)(dpp - ncap->value);
				free_NC_dimarrayV(ncap);
				return status;
			}
			{
			  uintptr_t dimid = (uintptr_t)(dpp - ncap->value);
			  NC_hashmapadd(ncap->hashmap, dimid, (*dpp)->name->cp, strlen((*dpp)->name->cp));
			}
		}
	}

    return NC_NOERR;
}


/* End NC_dim */
/* Begin NC_attr */


/*
 * How much space will 'attrp' take in external representation?
 * Formerly
NC_xlen_attr(app)
 */
static size_t
ncx_len_NC_attr(const NC_attr *attrp, int version)
{
	size_t sz;

	assert(attrp != NULL);

	sz = ncx_len_NC_string(attrp->name, version);
	sz += X_SIZEOF_NC_TYPE; /* type */
	sz += (version == 5) ? X_SIZEOF_INT64 : X_SIZEOF_SIZE_T; /* nelems */
	sz += attrp->xsz;

	return(sz);
}


#undef MIN
#define MIN(mm,nn) (((mm) < (nn)) ? (mm) : (nn))

/*----< ncmpix_len_nctype() >------------------------------------------------*/
/* return the length of external data type */
static size_t
ncmpix_len_nctype(nc_type type) {
    switch(type) {
        case NC_BYTE:
        case NC_CHAR:
        case NC_UBYTE:  return X_SIZEOF_CHAR;
        case NC_SHORT:  return X_SIZEOF_SHORT;
        case NC_USHORT: return X_SIZEOF_USHORT;
        case NC_INT:    return X_SIZEOF_INT;
        case NC_UINT:   return X_SIZEOF_UINT;
        case NC_FLOAT:  return X_SIZEOF_FLOAT;
        case NC_DOUBLE: return X_SIZEOF_DOUBLE;
        case NC_INT64:  return X_SIZEOF_INT64;
        case NC_UINT64: return X_SIZEOF_UINT64;
        default: fprintf(stderr,"ncmpix_len_nctype bad type %d\n",type);
                 assert(0);
    }
    return 0;
}

/*
 * Put the values of an attribute
 * The loop is necessary since attrp->nelems
 * could potentially be quite large.
 */
static int
v1h_put_NC_attrV(v1hs *psp, const NC_attr *attrp)
{
	int status = 0;
	const size_t perchunk =  psp->extent;
	size_t remaining = attrp->xsz;
	void *value = attrp->xvalue;
	size_t nbytes = 0, padding = 0;

	assert(psp->extent % X_ALIGN == 0);

	do {
		nbytes = MIN(perchunk, remaining);

		status = check_v1hs(psp, nbytes);
		if(status != NC_NOERR)
			return status;

		if (value) {
			(void) memcpy(psp->pos, value, nbytes);
			value = (void *)((char *)value + nbytes);
		}
		
		psp->pos = (void *)((char *)psp->pos + nbytes);
		remaining -= nbytes;

	} while(remaining != 0);


	padding = attrp->xsz - ncmpix_len_nctype(attrp->type) * attrp->nelems;
	if (padding > 0) {
		/* CDF specification: Header padding uses null (\x00) bytes. */
		memset((char*)psp->pos-padding, 0, padding);
	}

	return NC_NOERR;
}

/* Write a NC_attr to the header */
static int
v1h_put_NC_attr(v1hs *psp, const NC_attr *attrp)
{
	int status;

	status = v1h_put_NC_string(psp, attrp->name);
    if(status != NC_NOERR)
		return status;

	status = v1h_put_nc_type(psp, &attrp->type);
    if(status != NC_NOERR)
		return status;

	status = v1h_put_size_t(psp, &attrp->nelems);
    if(status != NC_NOERR)
		return status;

	status = v1h_put_NC_attrV(psp, attrp);
    if(status != NC_NOERR)
		return status;

    return NC_NOERR;
}


/*
 * Get the values of an attribute
 * The loop is necessary since attrp->nelems
 * could potentially be quite large.
 */
static int
v1h_get_NC_attrV(v1hs *gsp, NC_attr *attrp)
{
	int status;
	const size_t perchunk =  gsp->extent;
	size_t remaining = attrp->xsz;
	void *value = attrp->xvalue;
	size_t nget;
#if USE_STRICT_NULL_BYTE_HEADER_PADDING
	size_t padding;
#endif /* USE_STRICT_NULL_BYTE_HEADER_PADDING */

	do {
		nget = MIN(perchunk, remaining);

		status = check_v1hs(gsp, nget);
		if(status != NC_NOERR)
			return status;

		if (value) {
			(void) memcpy(value, gsp->pos, nget);
			value = (void *)((signed char *)value + nget);
		}
		
		gsp->pos = (void*)((unsigned char *)gsp->pos + nget);

		remaining -= nget;

	} while(remaining != 0);

#if USE_STRICT_NULL_BYTE_HEADER_PADDING
	padding = attrp->xsz - ncmpix_len_nctype(attrp->type) * attrp->nelems;
	if (padding > 0) {
		/* CDF specification: Header padding uses null (\x00) bytes. */
		char pad[X_ALIGN-1];
		memset(pad, 0, X_ALIGN-1);
		if (memcmp((char*)gsp->pos-padding, pad, (size_t)padding) != 0)
			return NC_ENULLPAD;
	}
#endif

	return NC_NOERR;
}


/* Read a NC_attr from the header */
static int
v1h_get_NC_attr(v1hs *gsp, NC_attr **attrpp)
{
	NC_string *strp;
	int status;
	nc_type type;
	size_t nelems;
	NC_attr *attrp;

	status = v1h_get_NC_string(gsp, &strp);
    if(status != NC_NOERR)
		return status;

	status = v1h_get_nc_type(gsp, &type);
    if(status != NC_NOERR)
		goto unwind_name;

	status = v1h_get_size_t(gsp, &nelems);
    if(status != NC_NOERR)
		goto unwind_name;

	attrp = new_x_NC_attr(strp, type, nelems);
	if(attrp == NULL)
	{
		status = NC_ENOMEM;
		goto unwind_name;
	}

	status = v1h_get_NC_attrV(gsp, attrp);
        if(status != NC_NOERR)
	{
		free_NC_attr(attrp); /* frees strp */
		return status;
	}

	*attrpp = attrp;

    return NC_NOERR;

unwind_name:
	free_NC_string(strp);
	return status;
}


/* How much space in the header is required for this NC_attrarray? */
static size_t
ncx_len_NC_attrarray(const NC_attrarray *ncap, int version)
{
	size_t xlen = X_SIZEOF_NCTYPE;	/* type */
	xlen += (version == 5) ? X_SIZEOF_INT64 : X_SIZEOF_SIZE_T; /* count */
	if(ncap == NULL)
		return xlen;
	/* else */
	{
		const NC_attr **app = (const NC_attr **)ncap->value;
		if (app)
		{
			const NC_attr *const *const end = &app[ncap->nelems];
			for( /*NADA*/; app < end; app++)
			{
				xlen += ncx_len_NC_attr(*app,version);
			}
		}
	}
	return xlen;
}


/* Write a NC_attrarray to the header */
static int
v1h_put_NC_attrarray(v1hs *psp, const NC_attrarray *ncap)
{
	int status;

	assert(psp != NULL);

	if(ncap == NULL
#if 1
		/* Backward:
		 * This clause is for 'byte for byte'
		 * backward compatibility.
		 * Strickly speaking, it is 'bug for bug'.
		 */
		|| ncap->nelems == 0
#endif
		)
	{
		/*
		 * Handle empty netcdf
		 */
		const size_t nosz = 0;

		status = v1h_put_NCtype(psp, NC_UNSPECIFIED);
        if(status != NC_NOERR)
			return status;
		status = v1h_put_size_t(psp, &nosz);
        if(status != NC_NOERR)
			return status;
        return NC_NOERR;
	}
	/* else */

	status = v1h_put_NCtype(psp, NC_ATTRIBUTE);
    if(status != NC_NOERR)
		return status;
	status = v1h_put_size_t(psp, &ncap->nelems);
    if(status != NC_NOERR)
		return status;

	{
		const NC_attr **app = (const NC_attr **)ncap->value;
		const NC_attr *const *const end = &app[ncap->nelems];
		for( /*NADA*/; app < end; app++)
		{
			status = v1h_put_NC_attr(psp, *app);
			if(status)
				return status;
		}
	}
    return NC_NOERR;
}


/* Read a NC_attrarray from the header */
static int
v1h_get_NC_attrarray(v1hs *gsp, NC_attrarray *ncap)
{
	int status;
	NCtype type = NC_UNSPECIFIED;

	assert(gsp != NULL && gsp->pos != NULL);
	assert(ncap != NULL);
	assert(ncap->value == NULL);

	status = v1h_get_NCtype(gsp, &type);
    if(status != NC_NOERR)
		return status;
	status = v1h_get_size_t(gsp, &ncap->nelems);
    if(status != NC_NOERR)
		return status;

	if(ncap->nelems == 0)
        return NC_NOERR;
	/* else */
	if(type != NC_ATTRIBUTE)
		return EINVAL;

	ncap->value = (NC_attr **) malloc(ncap->nelems * sizeof(NC_attr *));
	if(ncap->value == NULL)
		return NC_ENOMEM;
	ncap->nalloc = ncap->nelems;

	{
		NC_attr **app = ncap->value;
		NC_attr *const *const end = &app[ncap->nelems];
		for( /*NADA*/; app < end; app++)
		{
			status = v1h_get_NC_attr(gsp, app);
			if(status)
			{
				ncap->nelems = (size_t)(app - ncap->value);
				free_NC_attrarrayV(ncap);
				return status;
			}
		}
	}

    return NC_NOERR;
}

/* End NC_attr */
/* Begin NC_var */

/*
 * How much space will the xdr'd var take.
 * Formerly
NC_xlen_var(vpp)
 */
static size_t
ncx_len_NC_var(const NC_var *varp, size_t sizeof_off_t, int version)
{
	size_t sz;

	assert(varp != NULL);
	assert(sizeof_off_t != 0);

	sz = ncx_len_NC_string(varp->name, version);
        if (version == 5) {
	    sz += X_SIZEOF_INT64; /* ndims */
	    sz += ncx_len_int64(varp->ndims); /* dimids */
        }
        else {
	    sz += X_SIZEOF_SIZE_T; /* ndims */
	    sz += ncx_len_int(varp->ndims); /* dimids */
	}
	sz += ncx_len_NC_attrarray(&varp->attrs, version);
	sz += X_SIZEOF_NC_TYPE; /* nc_type */
	sz += (version == 5) ? X_SIZEOF_INT64 : X_SIZEOF_SIZE_T; /* vsize */
	sz += sizeof_off_t; /* begin */

	return(sz);
}


/* Write a NC_var to the header */
static int
v1h_put_NC_var(v1hs *psp, const NC_var *varp)
{
	int status;
    size_t vsize;

	status = v1h_put_NC_string(psp, varp->name);
    if(status != NC_NOERR)
		return status;

	status = v1h_put_size_t(psp, &varp->ndims);
    if(status != NC_NOERR)
		return status;

	if (psp->version == 5) {
		status = check_v1hs(psp, ncx_len_int64(varp->ndims));
        if(status != NC_NOERR)
			return status;
		status = ncx_putn_longlong_int(&psp->pos,
				varp->ndims, varp->dimids, NULL);
        if(status != NC_NOERR)
			return status;
	}
	else {
  	    status = check_v1hs(psp, ncx_len_int(varp->ndims));
        if(status != NC_NOERR)
		return status;
	    status = ncx_putn_int_int(&psp->pos,
			varp->ndims, varp->dimids, NULL);
        if(status != NC_NOERR)
		return status;
	}

	status = v1h_put_NC_attrarray(psp, &varp->attrs);
    if(status != NC_NOERR)
		return status;

	status = v1h_put_nc_type(psp, &varp->type);
    if(status != NC_NOERR)
		return status;

    /* write vsize to header.
     * CDF format specification: The vsize field is actually redundant, because
     * its value may be computed from other information in the header. The
     * 32-bit vsize field is not large enough to contain the size of variables
     * that require more than 2^32 - 4 bytes, so 2^32 - 1 is used in the vsize
     * field for such variables.
     */
    vsize = varp->len;
    if (varp->len > 4294967292UL && (psp->version == NC_FORMAT_CLASSIC ||
                                     psp->version == NC_FORMAT_64BIT_OFFSET))
        vsize = 4294967295UL; /* 2^32-1 */
    status = v1h_put_size_t(psp, &vsize);
    if(status != NC_NOERR) return status;

	status = check_v1hs(psp, psp->version == 1 ? 4 : 8); /*begin*/
    if(status != NC_NOERR)
		 return status;
	status = ncx_put_off_t(&psp->pos, &varp->begin, psp->version == 1 ? 4 : 8);
    if(status != NC_NOERR)
		return status;

    return NC_NOERR;
}


/* Read a NC_var from the header */
static int
v1h_get_NC_var(v1hs *gsp, NC_var **varpp)
{
	NC_string *strp;
	int status;
	size_t ndims;
	NC_var *varp;

	status = v1h_get_NC_string(gsp, &strp);
    if(status != NC_NOERR)
		return status;

	status = v1h_get_size_t(gsp, &ndims);
    if(status != NC_NOERR)
		goto unwind_name;

	varp = new_x_NC_var(strp, ndims);
	if(varp == NULL)
	{
		status = NC_ENOMEM;
		goto unwind_name;
	}

	if (gsp->version == 5) {
		status = check_v1hs(gsp, ncx_len_int64(ndims));
        if(status != NC_NOERR)
			goto unwind_alloc;
		status = ncx_getn_longlong_int((const void **)(&gsp->pos),
				ndims, varp->dimids);
        if(status != NC_NOERR)
			goto unwind_alloc;
	}
	else {
	    status = check_v1hs(gsp, ncx_len_int(ndims));
        if(status != NC_NOERR)
		goto unwind_alloc;
	    status = ncx_getn_int_int((const void **)(&gsp->pos),
			ndims, varp->dimids);
        if(status != NC_NOERR)
		goto unwind_alloc;
	}
	status = v1h_get_NC_attrarray(gsp, &varp->attrs);
    if(status != NC_NOERR)
		goto unwind_alloc;
	status = v1h_get_nc_type(gsp, &varp->type);
    if(status != NC_NOERR)
		 goto unwind_alloc;

    size_t tmp;
    status = v1h_get_size_t(gsp, &tmp);
    varp->len = tmp;
    if(status != NC_NOERR)
		 goto unwind_alloc;

	status = check_v1hs(gsp, gsp->version == 1 ? 4 : 8);
    if(status != NC_NOERR)
		 goto unwind_alloc;
	status = ncx_get_off_t((const void **)&gsp->pos,
			       &varp->begin, gsp->version == 1 ? 4 : 8);
    if(status != NC_NOERR)
		 goto unwind_alloc;

	*varpp = varp;
    return NC_NOERR;

unwind_alloc:
	free_NC_var(varp); /* frees name */
	return status;

unwind_name:
	free_NC_string(strp);
	return status;
}


/* How much space in the header is required for this NC_vararray? */
static size_t
ncx_len_NC_vararray(const NC_vararray *ncap, size_t sizeof_off_t, int version)
{
	size_t xlen = X_SIZEOF_NCTYPE;	/* type */
	xlen += (version == 5) ? X_SIZEOF_INT64 : X_SIZEOF_SIZE_T; /* count */
	if(ncap == NULL)
		return xlen;
	/* else */
	{
		const NC_var **vpp = (const NC_var **)ncap->value;
		if (vpp)
		{
			const NC_var *const *const end = &vpp[ncap->nelems];
			for( /*NADA*/; vpp < end; vpp++)
			{
				xlen += ncx_len_NC_var(*vpp, sizeof_off_t, version);
			}
		}
	}
	return xlen;
}


/* Write a NC_vararray to the header */
static int
v1h_put_NC_vararray(v1hs *psp, const NC_vararray *ncap)
{
	int status;

	assert(psp != NULL);

	if(ncap == NULL
#if 1
		/* Backward:
		 * This clause is for 'byte for byte'
		 * backward compatibility.
		 * Strickly speaking, it is 'bug for bug'.
		 */
		|| ncap->nelems == 0
#endif
		)
	{
		/*
		 * Handle empty netcdf
		 */
		const size_t nosz = 0;

		status = v1h_put_NCtype(psp, NC_UNSPECIFIED);
        if(status != NC_NOERR)
			return status;
		status = v1h_put_size_t(psp, &nosz);
        if(status != NC_NOERR)
			return status;
        return NC_NOERR;
	}
	/* else */

	status = v1h_put_NCtype(psp, NC_VARIABLE);
    if(status != NC_NOERR)
		return status;
	status = v1h_put_size_t(psp, &ncap->nelems);
    if(status != NC_NOERR)
		return status;

	{
		const NC_var **vpp = (const NC_var **)ncap->value;
		const NC_var *const *const end = &vpp[ncap->nelems];
		for( /*NADA*/; vpp < end; vpp++)
		{
			status = v1h_put_NC_var(psp, *vpp);
			if(status)
				return status;
		}
	}
    return NC_NOERR;
}


/* Read a NC_vararray from the header */
static int
v1h_get_NC_vararray(v1hs *gsp, NC_vararray *ncap)
{
	int status;
	NCtype type = NC_UNSPECIFIED;

	assert(gsp != NULL && gsp->pos != NULL);
	assert(ncap != NULL);
	assert(ncap->value == NULL);

	status = v1h_get_NCtype(gsp, &type);
    if(status != NC_NOERR)
		return status;

	status = v1h_get_size_t(gsp, &ncap->nelems);
    if(status != NC_NOERR)
		return status;

	if(ncap->nelems == 0)
        return NC_NOERR;
	/* else */
	if(type != NC_VARIABLE)
		return EINVAL;
	
	if (ncap->nelems > SIZE_MAX / sizeof(NC_var *))
		return NC_ERANGE;
	ncap->value = (NC_var **) calloc(1,ncap->nelems * sizeof(NC_var *));
	if(ncap->value == NULL)
		return NC_ENOMEM;
	ncap->nalloc = ncap->nelems;

	ncap->hashmap = NC_hashmapnew(ncap->nelems);
	if (ncap->hashmap == NULL)
		return NC_ENOMEM;
	{
		NC_var **vpp = ncap->value;
		NC_var *const *const end = &vpp[ncap->nelems];
		for( /*NADA*/; vpp < end; vpp++)
		{
			status = v1h_get_NC_var(gsp, vpp);
			if(status)
			{
				ncap->nelems = (size_t)(vpp - ncap->value);
				free_NC_vararrayV(ncap);
				return status;
			}
			{
			  uintptr_t varid = (uintptr_t)(vpp - ncap->value);
			  NC_hashmapadd(ncap->hashmap, varid, (*vpp)->name->cp, strlen((*vpp)->name->cp));
			}
		}
	}

    return NC_NOERR;
}


/* End NC_var */
/* Begin NC */

/*
 * Recompute the shapes of all variables
 * Sets ncp->begin_var to start of first variable.
 * Sets ncp->begin_rec to start of first record variable.
 * Returns -1 on error. The only possible error is a reference
 * to a non existent dimension, which could occur for a corrupted
 * netcdf file.
 */
static int
NC_computeshapes(NC3_INFO* ncp)
{
	NC_var **vpp = (NC_var **)ncp->vars.value;
	NC_var *first_var = NULL;	/* first "non-record" var */
	NC_var *first_rec = NULL;	/* first "record" var */
	int status;

	ncp->begin_var = (off_t) ncp->xsz;
	ncp->begin_rec = (off_t) ncp->xsz;
	ncp->recsize = 0;

	if(ncp->vars.nelems == 0)
		return(0);

	if (vpp)
	{
		NC_var *const *const end = &vpp[ncp->vars.nelems];
		for( /*NADA*/; vpp < end; vpp++)
		{
			status = NC_var_shape(*vpp, &ncp->dims);
		        if(status != NC_NOERR)
				return(status);

		  	if(IS_RECVAR(*vpp))
			{
		  		if(first_rec == NULL)
					first_rec = *vpp;
				ncp->recsize += (*vpp)->len;
			}
			else
			{
		            if(first_var == NULL)
			        first_var = *vpp;
			    /*
			     * Overwritten each time thru.
			     * Usually overwritten in first_rec != NULL clause below.
			     */
			    ncp->begin_rec = (*vpp)->begin + (off_t)(*vpp)->len;
			}
		}
	}

	if(first_rec != NULL)
	{
		if(ncp->begin_rec > first_rec->begin)
		    return(NC_ENOTNC); /* not a netCDF file or corrupted */
		ncp->begin_rec = first_rec->begin;
		/*
	 	 * for special case of exactly one record variable, pack value
	 	 */
		if(ncp->recsize == first_rec->len)
			ncp->recsize = *first_rec->dsizes * first_rec->xsz;
	}

	if(first_var != NULL)
	{
		ncp->begin_var = first_var->begin;
	}
	else
	{
		ncp->begin_var = ncp->begin_rec;
	}

	if(ncp->begin_var <= 0 ||
	   ncp->xsz > (size_t)ncp->begin_var ||
	   ncp->begin_rec <= 0 ||
	   ncp->begin_var > ncp->begin_rec)
	    return(NC_ENOTNC); /* not a netCDF file or corrupted */

    return(NC_NOERR);
}

/* How much space in the header is required for the NC data structure? */
size_t
ncx_len_NC(const NC3_INFO* ncp, size_t sizeof_off_t)
{
	int version=1;
	size_t xlen = sizeof(ncmagic);

	assert(ncp != NULL);
	if (fIsSet(ncp->flags, NC_64BIT_DATA)) /* CDF-5 */
		version = 5;
    	else if (fIsSet(ncp->flags, NC_64BIT_OFFSET)) /* CDF-2 */
		version = 2;

	xlen += (version == 5) ? X_SIZEOF_INT64 : X_SIZEOF_SIZE_T; /* numrecs */
	xlen += ncx_len_NC_dimarray(&ncp->dims, version);
	xlen += ncx_len_NC_attrarray(&ncp->attrs, version);
	xlen += ncx_len_NC_vararray(&ncp->vars, sizeof_off_t, version);

	return xlen;
}


/* Write the file header */
int
ncx_put_NC(const NC3_INFO* ncp, void **xpp, off_t offset, size_t extent)
{
    int status = NC_NOERR;
	v1hs ps; /* the get stream */

	assert(ncp != NULL);

	/* Initialize stream ps */

	ps.nciop = ncp->nciop;
	ps.flags = RGN_WRITE;

	if (ncp->flags & NC_64BIT_DATA)
	  ps.version = 5;
	else if (ncp->flags & NC_64BIT_OFFSET)
	  ps.version = 2;
	else
	  ps.version = 1;

	if(xpp == NULL)
	{
		/*
		 * Come up with a reasonable stream read size.
		 */
		extent = ncp->xsz;
		if(extent <= ((ps.version==5)?MIN_NC5_XSZ:MIN_NC3_XSZ))
		{
			/* first time read */
			extent = ncp->chunk;
			/* Protection for when ncp->chunk is huge;
			 * no need to read hugely. */
	      		if(extent > 4096)
				extent = 4096;
		}
		else if(extent > ncp->chunk)
		    extent = ncp->chunk;

		ps.offset = 0;
		ps.extent = extent;
		ps.base = NULL;
		ps.pos = ps.base;

		status = fault_v1hs(&ps, extent);
		if(status)
			return status;
	}
	else
	{
		ps.offset = offset;
		ps.extent = extent;
		ps.base = *xpp;
		ps.pos = ps.base;
		ps.end = (char *)ps.base + ps.extent;
	}

	if (ps.version == 5)
	  status = ncx_putn_schar_schar(&ps.pos, sizeof(ncmagic5), ncmagic5, NULL);
	else if (ps.version == 2)
	  status = ncx_putn_schar_schar(&ps.pos, sizeof(ncmagic), ncmagic, NULL);
	else
	  status = ncx_putn_schar_schar(&ps.pos, sizeof(ncmagic1), ncmagic1, NULL);
	if(status != NC_NOERR)
		goto release;

	{
	const size_t nrecs = NC_get_numrecs(ncp);
	if (ps.version == 5) {
            unsigned long long tmp = (unsigned long long) nrecs;
	    status = ncx_put_uint64(&ps.pos, tmp);
        }
       	else
	    status = ncx_put_size_t(&ps.pos, &nrecs);
	if(status != NC_NOERR)
		goto release;
	}

	assert((char *)ps.pos < (char *)ps.end);

	status = v1h_put_NC_dimarray(&ps, &ncp->dims);
    if(status != NC_NOERR)
		goto release;

	status = v1h_put_NC_attrarray(&ps, &ncp->attrs);
    if(status != NC_NOERR)
		goto release;

	status = v1h_put_NC_vararray(&ps, &ncp->vars);
    if(status != NC_NOERR)
		goto release;

release:
	(void) rel_v1hs(&ps);

	return status;
}


/* Make the in-memory NC structure from reading the file header */
int
nc_get_NC(NC3_INFO* ncp)
{
	int status;
	v1hs gs; /* the get stream */

	assert(ncp != NULL);

	/* Initialize stream gs */

	gs.nciop = ncp->nciop;
	gs.offset = 0; /* beginning of file */
	gs.extent = 0;
	gs.flags = 0;
	gs.version = 0;
	gs.base = NULL;
	gs.pos = gs.base;

	{
		/*
		 * Come up with a reasonable stream read size.
		 */
	        off_t filesize;
		size_t extent = ncp->xsz;

		if(extent <= ((fIsSet(ncp->flags, NC_64BIT_DATA))?MIN_NC5_XSZ:MIN_NC3_XSZ))
		{
		        status = ncio_filesize(ncp->nciop, &filesize);
			if(status)
			    return status;
			if(filesize < sizeof(ncmagic)) { /* too small, not netcdf */

			    status = NC_ENOTNC;
			    return status;
			}
			/* first time read */
			extent = ncp->chunk;
			/* Protection for when ncp->chunk is huge;
			 * no need to read hugely. */
	      		if(extent > 4096)
				extent = 4096;
			if(extent > filesize)
			        extent = (size_t)filesize;
		}
		else if(extent > ncp->chunk)
		    extent = ncp->chunk;

		/*
		 * Invalidate the I/O buffers to force a read of the header
		 * region.
		 */
		status = ncio_sync(gs.nciop);
		if(status)
			return status;

		status = fault_v1hs(&gs, extent);
		if(status)
			return status;
	}

	/* get the header from the stream gs */

	{
		/* Get & check magic number */
		schar magic[sizeof(ncmagic)];
		(void) memset(magic, 0, sizeof(magic));

		status = ncx_getn_schar_schar(
			(const void **)(&gs.pos), sizeof(magic), magic);
        if(status != NC_NOERR)
			goto unwind_get;

		if(memcmp(magic, ncmagic, sizeof(ncmagic)-1) != 0)
		{
			status = NC_ENOTNC;
			goto unwind_get;
		}
		/* Check version number in last byte of magic */
		if (magic[sizeof(ncmagic)-1] == 0x1) {
		  gs.version = 1;
		} else if (magic[sizeof(ncmagic)-1] == 0x2) {
		  gs.version = 2;
		  fSet(ncp->flags, NC_64BIT_OFFSET);
		  /* Now we support version 2 file access on non-LFS systems -- rkr */
#if 0
		  if (sizeof(off_t) != 8) {
		    fprintf(stderr, "NETCDF WARNING: Version 2 file on 32-bit system.\n");
		  }
#endif
		} else if (magic[sizeof(ncmagic)-1] == 0x5) {
		  gs.version = 5;
		  fSet(ncp->flags, NC_64BIT_DATA);
		} else {
			status = NC_ENOTNC;
			goto unwind_get;
		}
	}

	{
	size_t nrecs = 0;
       	if (gs.version == 5) {
		unsigned long long tmp = 0;
		status = ncx_get_uint64((const void **)(&gs.pos), &tmp);
		nrecs = (size_t)tmp;
       	}
       	else
	    status = ncx_get_size_t((const void **)(&gs.pos), &nrecs);
    if(status != NC_NOERR)
		goto unwind_get;
	NC_set_numrecs(ncp, nrecs);
	}

	assert((char *)gs.pos < (char *)gs.end);

	status = v1h_get_NC_dimarray(&gs, &ncp->dims);
    if(status != NC_NOERR)
		goto unwind_get;

	status = v1h_get_NC_attrarray(&gs, &ncp->attrs);
    if(status != NC_NOERR)
		goto unwind_get;

	status = v1h_get_NC_vararray(&gs, &ncp->vars);
    if(status != NC_NOERR)
		goto unwind_get;

	ncp->xsz = ncx_len_NC(ncp, (gs.version == 1) ? 4 : 8);

	status = NC_computeshapes(ncp);
    if(status != NC_NOERR)
		goto unwind_get;

	status = NC_check_vlens(ncp);
    if(status != NC_NOERR)
		goto unwind_get;

	status = NC_check_voffs(ncp);
    if(status != NC_NOERR)
		goto unwind_get;

unwind_get:
	(void) rel_v1hs(&gs);
	return status;
}
