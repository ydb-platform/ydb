/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "config.h"
#include "netcdf.h"
#include "netcdf_aux.h"
#include "nc4internal.h"
#include "ncoffsets.h"
#include "nclog.h"
#include "ncrc.h"
#include "netcdf_filter.h"
#include "ncpathmgr.h"
#include "nclist.h"
#include "ncutil.h"

struct NCAUX_FIELD {
    char* name;
    nc_type fieldtype;
    size_t ndims;
    int dimsizes[NC_MAX_VAR_DIMS];
    size_t size;
    size_t offset;
    size_t alignment;
};

struct NCAUX_CMPD {
    int ncid;
    int mode;
    char* name;
    size_t nfields;
    struct NCAUX_FIELD* fields;
    size_t size;
    size_t offset; /* cumulative as fields are added */
    size_t alignment;
};

static int computefieldinfo(struct NCAUX_CMPD* cmpd);

static int filterspec_cvt(const char* txt, size_t* nparamsp, unsigned int* params);

EXTERNL int nc_dump_data(int ncid, nc_type xtype, void* memory, size_t count, char** bufp);
EXTERNL int nc_parse_plugin_pathlist(const char* path0, NClist* dirlist);

/**************************************************/
/*
This code is a variant of the H5detect.c code from HDF5.
Author: D. Heimbigner 10/7/2008
*/

EXTERNL int
ncaux_begin_compound(int ncid, const char *name, int alignmode, void** tagp)
{
#ifdef USE_NETCDF4
    int status = NC_NOERR;
    struct NCAUX_CMPD* cmpd = NULL;

    if(tagp) *tagp = NULL;

    cmpd = (struct NCAUX_CMPD*)calloc(1,sizeof(struct NCAUX_CMPD));
    if(cmpd == NULL) {status = NC_ENOMEM; goto fail;}
    cmpd->ncid = ncid;
    cmpd->mode = alignmode;
    cmpd->nfields = 0;
    cmpd->name = strdup(name);
    if(cmpd->name == NULL) {status = NC_ENOMEM; goto fail;}

    if(tagp) {
      *tagp = (void*)cmpd;
    } else { /* Error, free cmpd to avoid memory leak. */
      free(cmpd);
    }
    return status;

fail:
    ncaux_abort_compound((void*)cmpd);
    return status;
#else
    return NC_ENOTBUILT;
#endif
}

EXTERNL int
ncaux_abort_compound(void* tag)
{
#ifdef USE_NETCDF4
    size_t i;
    struct NCAUX_CMPD* cmpd = (struct NCAUX_CMPD*)tag;
    if(cmpd == NULL) goto done;
    if(cmpd->name) free(cmpd->name);
    for(i=0;i<cmpd->nfields;i++) {
	struct NCAUX_FIELD* field = &cmpd->fields[i];
	if(field->name) free(field->name);
    }
    if(cmpd->fields) free(cmpd->fields);
    free(cmpd);

done:
    return NC_NOERR;
#else
    return NC_ENOTBUILT;
#endif
}

EXTERNL int
ncaux_add_field(void* tag,  const char *name, nc_type field_type,
			   int ndims, const int* dimsizes)
{
#ifdef USE_NETCDF4
    int i;
    int status = NC_NOERR;
    struct NCAUX_CMPD* cmpd = (struct NCAUX_CMPD*)tag;
    struct NCAUX_FIELD* newfields = NULL;
    struct NCAUX_FIELD* field = NULL;

    if(cmpd == NULL) goto done;
    if(ndims < 0) {status = NC_EINVAL; goto done;}
    for(i=0;i<ndims;i++) {
	if(dimsizes[i] <= 0) {status = NC_EINVAL; goto done;}
    }
    if(cmpd->fields == NULL) {
        newfields = (struct NCAUX_FIELD*)calloc(1,sizeof(struct NCAUX_FIELD));
    } else {
        newfields = (struct NCAUX_FIELD*)realloc(cmpd->fields,cmpd->nfields+1*sizeof(struct NCAUX_FIELD));
    }
    if(cmpd->fields == NULL) {status = NC_ENOMEM; goto done;}
    cmpd->fields = newfields;
    field = &cmpd->fields[cmpd->nfields+1];
    field->name = strdup(name);
    field->fieldtype = field_type;
    if(field->name == NULL) {status = NC_ENOMEM; goto done;}
    field->ndims = (size_t)ndims;
    memcpy(field->dimsizes,dimsizes,sizeof(int)*field->ndims);
    cmpd->nfields++;

done:
    if(newfields)
      free(newfields);
    return status;
#else
    return NC_ENOTBUILT;
#endif
}

EXTERNL int
ncaux_end_compound(void* tag, nc_type* idp)
{
#ifdef USE_NETCDF4
    size_t i;
    int status = NC_NOERR;
    struct NCAUX_CMPD* cmpd = (struct NCAUX_CMPD*)tag;

    if(cmpd == NULL) {status = NC_EINVAL; goto done;}

    /* Compute field and compound info */
    status = computefieldinfo(cmpd);
    if(status != NC_NOERR) goto done;

    status = nc_def_compound(cmpd->ncid, cmpd->size, cmpd->name, idp);
    if(status != NC_NOERR) goto done;

    for(i=0;i<cmpd->nfields;i++) {
	struct NCAUX_FIELD* field = &cmpd->fields[i];
	if(field->ndims > 0) {
            status = nc_insert_compound(cmpd->ncid, *idp, field->name,
					field->offset, field->fieldtype);
	} else {
            status = nc_insert_array_compound(cmpd->ncid, *idp, field->name,
					field->offset, field->fieldtype,
					(int)field->ndims,field->dimsizes);
	}
        if(status != NC_NOERR) goto done;
    }

done:
    return status;
#else
    return NC_ENOTBUILT;
#endif
}

/**************************************************/

/**
 @param ncclass - type class for which alignment is requested; excludes ENUM|COMPOUND
*/

int
ncaux_class_alignment(int ncclass, size_t* alignp)
{
    int stat = NC_NOERR;
    size_t align = 0;
    if(ncclass <= NC_MAX_ATOMIC_TYPE || ncclass == NC_VLEN || ncclass == NC_OPAQUE) {
        stat = NC_class_alignment(ncclass,&align);
    } else {
        nclog(NCLOGERR,"ncaux_class_alignment: class %d; alignment cannot be determermined",ncclass);
    }
    if(alignp) *alignp = align;
    if(align == 0) stat = NC_EINVAL;
    return stat;
}


#ifdef USE_NETCDF4
/* Find first primitive field of a possibly nested sequence of compounds */
static nc_type
findfirstfield(int ncid, nc_type xtype)
{
    int status = NC_NOERR;
    nc_type fieldtype = xtype;
    if(xtype <= NC_MAX_ATOMIC_TYPE) goto done;

    status = nc_inq_compound_fieldtype(ncid, xtype, 0, &fieldtype);
    if(status != NC_NOERR) goto done;
    fieldtype = findfirstfield(ncid,fieldtype);

done:
    return (status == NC_NOERR?fieldtype:NC_NAT);
}

static size_t
getpadding(size_t offset, size_t alignment)
{
    size_t rem = (alignment==0?0:(offset % alignment));
    size_t pad = (rem==0?0:(alignment - rem));
    return pad;
}

static size_t
dimproduct(size_t ndims, int* dimsizes)
{
    size_t i;
    size_t product = 1;
    for(i=0;i<ndims;i++) product *= (size_t)dimsizes[i];
    return product;
}

static int
computefieldinfo(struct NCAUX_CMPD* cmpd)
{
    size_t i;
    int status = NC_NOERR;
    size_t offset = 0;
    size_t totaldimsize;

    /* Assign the sizes for the fields */
    for(i=0;i<cmpd->nfields;i++) {
	struct NCAUX_FIELD* field = &cmpd->fields[i];
	status = nc_inq_type(cmpd->ncid,field->fieldtype,NULL,&field->size);
        if(status != NC_NOERR) goto done;
	totaldimsize = dimproduct(field->ndims,field->dimsizes);
	field->size *= totaldimsize;
    }

    for(offset=0,i=0;i<cmpd->nfields;i++) {
        struct NCAUX_FIELD* field = &cmpd->fields[i];
	size_t alignment = 0;
	nc_type firsttype = findfirstfield(cmpd->ncid,field->fieldtype);

        /* only support 'c' alignment for now*/
	switch (field->fieldtype) {
	case NC_OPAQUE:
	    field->alignment = 1;
	    break;
	case NC_ENUM:
	    status = ncaux_type_alignment(firsttype,cmpd->ncid,&field->alignment);
	    break;
	case NC_VLEN: /*fall thru*/
	case NC_COMPOUND:
            status = ncaux_type_alignment(firsttype,cmpd->ncid,&field->alignment);
	    break;
	default:
            status = ncaux_type_alignment(field->fieldtype,cmpd->ncid,&field->alignment);
	    break;

	}
        offset += getpadding(offset,alignment);
        field->offset = offset;
        offset += field->size;
    }
    cmpd->size = offset;
    cmpd->alignment = cmpd->fields[0].alignment;

done:
    return status;
}

#endif /*USE_NETCDF4*/


/**************************************************/
/* Forward */

#define NUMCHAR "0123456789"
#define LPAREN '('
#define RPAREN ')'
#define LBRACK '['
#define RBRACK ']'

/* Look at q0 and q1) to determine type */
static int
gettype(const char q0, const char q1, int* isunsignedp)
{
    int type = 0;
    int isunsigned = 0;
    char typechar;
    
    isunsigned = (q0 == 'u' || q0 == 'U');
    if(q1 == '\0')
	typechar = q0; /* we were given only a single char */
    else if(isunsigned)
	typechar = q1; /* we have something like Ux as the tag */
    else
	typechar = q1; /* look at last char for tag */
    switch (typechar) {
    case 'f': case 'F': case '.': type = 'f'; break; /* float */
    case 'd': case 'D': type = 'd'; break; /* double */
    case 'b': case 'B': type = 'b'; break; /* byte */
    case 's': case 'S': type = 's'; break; /* short */
    case 'l': case 'L': type = 'l'; break; /* long long */
    case '0': case '1': case '2': case '3': case '4':
    case '5': case '6': case '7': case '8': case '9': type = 'i'; break;
    case 'u': case 'U': type = 'i'; isunsigned = 1; break; /* unsigned int */
    case '\0': type = 'i'; break;
    default: break;
    }
    if(isunsignedp) *isunsignedp = isunsigned;
    return type;
}

#ifdef WORDS_BIGENDIAN
/* Byte swap an 8-byte integer in place */
static void
byteswap8(unsigned char* mem)
{
    unsigned char c;
    c = mem[0];
    mem[0] = mem[7];
    mem[7] = c;
    c = mem[1];
    mem[1] = mem[6];
    mem[6] = c;
    c = mem[2];
    mem[2] = mem[5];
    mem[5] = c;
    c = mem[3];
    mem[3] = mem[4];
    mem[4] = c;
}

/* Byte swap an 8-byte integer in place */
static void
byteswap4(unsigned char* mem)
{
    unsigned char c;
    c = mem[0];
    mem[0] = mem[3];
    mem[3] = c;
    c = mem[1];
    mem[1] = mem[2];
    mem[2] = c;
}
#endif

/**************************************************/
/* Moved here from netcdf_filter.h */

/*
This function implements the 8-byte conversion algorithms for HDF5
Before calling *nc_def_var_filter* (unless *NC_parsefilterspec* was used),
the client must call this function with the decode argument set to 0.
Inside the filter code, this function should be called with the decode
argument set to 1.

* @params mem8 is a pointer to the 8-byte value either to fix.
* @params decode is 1 if the function should apply the 8-byte decoding algorithm
          else apply the encoding algorithm.
*/

void
ncaux_h5filterspec_fix8(unsigned char* mem8, int decode)
{
#ifdef WORDS_BIGENDIAN
    if(decode) { /* Apply inverse of the encode case */
	byteswap4(mem8); /* step 1: byte-swap each piece */
	byteswap4(mem8+4);
	byteswap8(mem8); /* step 2: convert to little endian format */
    } else { /* encode */
	byteswap8(mem8); /* step 1: convert to little endian format */
	byteswap4(mem8); /* step 2: byte-swap each piece */
	byteswap4(mem8+4);
    }
#else /* Little endian */
    /* No action is necessary */
#endif	    
}

/*
Parse a filter spec string into a NC_FILTER_SPEC*
Note that this differs from the usual case in that the
function is called once to get both the number of parameters
and the parameters themselves (hence the unsigned int** paramsp).

@param txt - a string containing the spec as a sequence of
              constants separated by commas, where first constant
	      is the filter id and the rest are parameters.
@param idp - store the parsed filter id here
@param nparamsp - store the number of parameters here
@param paramsp - store the vector of parameters here; caller frees.
@return NC_NOERR if parse succeeded
@return NC_EINVAL otherwise
*/

EXTERNL int
ncaux_h5filterspec_parse(const char* txt, unsigned int* idp, size_t* nparamsp, unsigned int** paramsp)
{
    int stat = NC_NOERR;
    size_t i;
    char* p;
    char* sdata0 = NULL; /* what to free */
    char* sdata = NULL; /* sdata0 with leading prefix skipped */
    size_t nparams; /* no. of comma delimited params */
    size_t nactual; /* actual number of unsigned int's */
    const char* sid = NULL;
    unsigned int filterid = 0;
    unsigned int* params = NULL;
    size_t len;
    
    if(txt == NULL)
        {stat = NC_EINVAL; goto done;}
    len = strlen(txt);
    if(len == 0)
        {stat = NC_EINVAL; goto done;}

    if((sdata0 = (char*)calloc(1,len+1+1))==NULL)
	{stat = NC_ENOMEM; goto done;}	
    memcpy(sdata0,txt,len);
    sdata = sdata0;

    /* Count number of parameters + id and delimit */
    p=sdata;
    for(nparams=0;;nparams++) {
        char* q = strchr(p,',');
	if(q == NULL) break;
	*q++ = '\0';
	p = q;
    }
    nparams++; /* for final piece */

    if(nparams == 0)
	{stat = NC_EINVAL; goto done;} /* no id and no parameters */

    p = sdata;

    /* Extract the filter id */
    sid = p;
    if((sscanf(sid,"%u",&filterid)) != 1) {stat = NC_EINVAL; goto done;}
    nparams--;

    /* skip past the filter id */
    p = p + strlen(p) + 1;

    /* Allocate the max needed space (assume all params are 64 bit) */
    if((params = (unsigned int*)calloc(sizeof(unsigned int),(nparams)*2))==NULL)
	{stat = NC_ENOMEM; goto done;}

    /* walk and capture */
    for(nactual=0,i=0;i<nparams;i++) { /* step thru param strings */
	size_t count = 0;
	len = strlen(p);
	/* skip leading white space */
	while(strchr(" 	",*p) != NULL) {p++; len--;}
	if((stat = filterspec_cvt(p,&count,params+nactual))) goto done;
	nactual += count;
        p = p + strlen(p) + 1; /* move to next param string */
    }
    /* Now return results */
    if(idp) *idp = filterid;
    if(nparamsp) *nparamsp = nactual;
    if(paramsp) {*paramsp = params; params = NULL;}
done:
    nullfree(params);
    nullfree(sdata0);
    return stat;
}

/*
Parse a filter parameter string into a sequence of unsigned ints.

@param txt - a string containing the parameter string.
@param nuiparamsp - store the number of unsigned ints here
@param uiparamsp - store the vector of unsigned ints here; caller frees.
@return NC_NOERR if parse succeeded
@return NC_EINVAL otherwise
*/

EXTERNL int
ncaux_h5filterspec_parse_parameter(const char* txt, size_t* nuiparamsp, unsigned int* uiparams)
{
    int stat = NC_NOERR;
    char* p;
    char* sdata0 = NULL; /* what to free */
    char* sdata = NULL; /* sdata0 with leading prefix skipped */
    size_t nuiparams = 0;
    size_t len;
    
    if(txt == NULL)
        {stat = NC_EINVAL; goto done;}
    len = strlen(txt);
    if(len == 0)
        {stat = NC_EINVAL; goto done;}

    if((sdata0 = (char*)calloc(1,len+1+1))==NULL)
	{stat = NC_ENOMEM; goto done;}	
    memcpy(sdata0,txt,len);
    sdata = sdata0;

    p = sdata;

    nuiparams = 0;
    len = strlen(p);
    /* skip leading white space */
    while(strchr(" 	",*p) != NULL) {p++; len--;}
    if((stat = filterspec_cvt(p,&nuiparams,uiparams))) goto done;
    /* Now return results */
    if(nuiparamsp) *nuiparamsp = nuiparams;
done:
    nullfree(sdata0);
    return stat;
}

/*
Parse a string containing multiple '|' separated filter specs.
Use a vector of NC_Filterspec structs to return results.
@param txt0 - a string containing the list of filter specs.
@param formatp - store any leading format integer here
@param nspecsp - # of parsed specs
@param specsp - pointer to hold vector of parsed specs. Caller frees
@return NC_NOERR if parse succeeded
@return NC_EINVAL if bad parameters or parse failed
*/

EXTERNL int
ncaux_h5filterspec_parselist(const char* txt0, int* formatp, size_t* nspecsp, NC_H5_Filterspec*** vectorp)
{
    int stat = NC_NOERR;
    int format = 0;
    size_t len = 0;
    size_t nspecs = 0;
    NC_H5_Filterspec** vector = NULL;
    char* spec = NULL; /* without prefix */
    char* p = NULL;
    char* q = NULL;

    if(txt0  == NULL) return NC_EINVAL;
    /* Duplicate txt0 so we can modify it */
    len = strlen(txt0);
    if((spec = calloc(1,len+1+1)) == NULL) {stat = NC_ENOMEM; goto done;}
    memcpy(spec,txt0,len); /* Note double ending nul */

    /* See if there is a prefix '[format]' tag */
    if(spec[0] == LBRACK) {
	p = spec + 1;
	q = strchr(p,RBRACK);
	if(q == NULL) {stat = NC_EINVAL; goto done;}
	*q++ = '\0'; /* delimit tag */
	if(sscanf(p,"%d",&format) != 1) {stat = NC_EINVAL; goto done;}
	spec = q; /* skip tag wrt later processing */
    }

    /* pass 1: count number of specs */
    p = spec;
    nspecs = 0;
    while(*p) {
	q = strchr(p,'|');
	if(q == NULL) q = p + strlen(p); /* fake it */
	nspecs++;
	p = q + 1;
    }
    if(nspecs >  0) {
	size_t count = 0;
	if((vector = (NC_H5_Filterspec**)calloc(sizeof(NC_H5_Filterspec*),nspecs)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	/* pass 2: parse */
	p = spec;
	for(count=0;count<nspecs;count++) {
	    NC_H5_Filterspec* spec = (NC_H5_Filterspec*)calloc(1,sizeof(NC_H5_Filterspec));
	    if(spec == NULL) {stat = NC_ENOMEM; goto done;}
	    vector[count] = spec;
	    q = strchr(p,'|');
	    if(q == NULL) q = p + strlen(p); /* fake it */
	    *q = '\0';
	    if((stat=ncaux_h5filterspec_parse(p,&spec->filterid,&spec->nparams,&spec->params))) goto done;
	    p = q+1; /* ok because of double nul */
	}
    }
    if(formatp) *formatp = format;
    if(nspecsp) *nspecsp = nspecs;
    if(vectorp) {*vectorp = vector; vector = NULL;}
done:
    nullfree(spec);
    if(vector) {
	size_t i;
        for(i=0;i<nspecs;i++)
	    ncaux_h5filterspec_free(vector[i]);
	nullfree(vector);
    }
    return stat;
}

/*
Parse a string containing multiple '|' separated filter specs.
Use a vector of NC_Filterspec structs to return results.
@param txt0 - a string containing the list of filter specs.
@param formatp - store any leading format integer here
@param nspecsp - # of parsed specs
@param specsp - pointer to hold vector of parsed specs. Caller frees
@return NC_NOERR if parse succeeded
@return NC_EINVAL if bad parameters or parse failed
*/

EXTERNL void
ncaux_h5filterspec_free(NC_H5_Filterspec* f)
{
    if(f) nullfree(f->params);
    nullfree(f);
}


/*
Convert a parameter string to one or two unsigned ints/
@param txt - (in) string constant
@param nparamsp - (out) # of unsigned ints produced
@param params - (out) produced unsigned ints
@return NC_NOERR if parse succeeded
@return NC_EINVAL if bad parameters or parse failed
*/

static int
filterspec_cvt(const char* txt, size_t* nparamsp, unsigned int* params)
{
    int stat = NC_NOERR;
    size_t nparams = 0; /*actual count*/
    unsigned long long val64u;
    unsigned int val32u;
    double vald;
    float valf;
    unsigned int *vector;
    unsigned char mem[8];
    int isunsigned = 0;
    int isnegative = 0;
    int type = 0;
    const char* q;
    const char* p = txt;
    size_t len = strlen(p);
    int sstat;

    /* skip leading white space */
    while(strchr(" 	",*p) != NULL) {p++; len--;}
    /* Get leading sign character, if any */
    if(*p == '-') isnegative = 1;
    /* Get trailing type tag characters */
    switch (len) {
    case 0: stat = NC_EINVAL; goto done; /* empty parameter */
    case 1: case 2:
        q = (p + len) - 1; /* point to last char */
        type = gettype(*q,'\0',&isunsigned);
        break;
    default: /* > 2 => we might have a two letter tag */
        q = (p + len) - 2;
        type = gettype(*q,*(q+1),&isunsigned);
        break;
    }
    /* Now parse */
    switch (type) {
    case 'b': case 's': case 'i':
         /* special case for a positive integer;for back compatibility.*/
        if(!isnegative)
	     sstat = sscanf(p,"%u",&val32u);
        else
	    sstat = sscanf(p,"%d",(int*)&val32u);
        if(sstat != 1) {stat = NC_EINVAL; goto done;}
        switch(type) {
        case 'b': val32u = (val32u & 0xFF); break;
        case 's': val32u = (val32u & 0xFFFF); break;
        }
        params[nparams++] = val32u;
        break;
    case 'f':
        sstat = sscanf(p,"%lf",&vald);
        if(sstat != 1) {stat = NC_EINVAL; goto done;}
        valf = (float)vald;
	/* avoid type punning */
	memcpy(&params[nparams++], &valf, sizeof(unsigned int));
        break;
    /* The following are 8-byte values, so we must swap pieces if this
    is a little endian machine */        
    case 'd':
        sstat = sscanf(p,"%lf",&vald);
        if(sstat != 1) {stat = NC_EINVAL; goto done;};
        memcpy(mem,&vald,sizeof(mem));
        ncaux_h5filterspec_fix8(mem,0);
        vector = (unsigned int*)mem;
        params[nparams++] = vector[0];
        params[nparams++] = vector[1];
        break;
    case 'l': /* long long */
        if(isunsigned)
            sstat = sscanf(p,"%llu",&val64u);
        else
            sstat = sscanf(p,"%lld",(long long*)&val64u);
        if(sstat != 1) {stat = NC_EINVAL; goto done;};
        memcpy(mem,&val64u,sizeof(mem));
        ncaux_h5filterspec_fix8(mem,0);
        vector = (unsigned int*)&mem;
        params[nparams++] = vector[0];
        params[nparams++] = vector[1];
        break;
    default:
        {stat = NC_EINVAL; goto done;};
    }
    *nparamsp = nparams;

done:
    return stat;
}
    
#if 0
/*
Parse a filter spec string into a NC_H5_Filterspec*
@param txt - a string containing the spec as a sequence of
              constants separated by commas.
@param specp - store the parsed filter here -- caller frees
@return NC_NOERR if parse succeeded
@return NC_EINVAL otherwise
*/

EXTERNL int
ncaux_filter_parsespec(const char* txt, NC_H5_Filterspec** h5specp)
{
    int stat = NC_NOERR;
    NC_Filterspec* spec = NULL;
    NC_H5_Filterspec* h5spec = NULL;
    size_t len;
    
    if(txt == NULL) 
	{stat = NC_EINVAL; goto done;}
    len = strlen(txt);
    if(len == 0) {stat = NC_EINVAL; goto done;}

    /* Parse as strings */
    if((stat = ncaux_filterspec_parse(txt,&spec))) goto done;
    /* walk and convert */
    if((stat = ncaux_filterspec_cvt(spec,&h5spec))) goto done;
    /* Now return results */
    if(h5specp != NULL) {*h5specp = h5spec; h5spec = NULL;}

done:
    ncaux_filterspec_free(spec);
    if(h5spec) nullfree(h5spec->params);
    nullfree(h5spec);
    return stat;
}

/*
Parse a string containing multiple '|' separated filter specs.

@param spec0 - a string containing the list of filter specs.
@param nspecsp - # of parsed specs
@param specsp - pointer to hold vector of parsed specs. Caller frees
@return NC_NOERR if parse succeeded
@return NC_EINVAL if bad parameters or parse failed
*/

EXTERNL int
ncaux_filter_parselist(const char* txt0, size_t* nspecsp, NC_H5_Filterspec*** vectorp)
{
    int stat = NC_NOERR;
    size_t len = 0;
    size_t nspecs = 0;
    NC_H5_Filterspec** vector = NULL;
    char* spec0 = NULL; /* with prefix */
    char* spec = NULL; /* without prefix */
    char* p = NULL;
    char* q = NULL;

    if(txt0  == NULL) return NC_EINVAL;
    /* Duplicate txt0 so we can modify it */
    len = strlen(txt0);
    if((spec = calloc(1,len+1+1)) == NULL) return NC_ENOMEM;
    memcpy(spec,txt0,len); /* Note double ending nul */
    spec0 = spec; /* Save for later free */

    /* See if there is a prefix '[format]' tag; ignore it */
    if(spec[0] == LBRACK) {
	spec = q; /* skip tag wrt later processing */
    }
    /* pass 1: count number of specs */
    p = spec;
    nspecs = 0;
    while(*p) {
	q = strchr(p,'|');
	if(q == NULL) q = p + strlen(p); /* fake it */
	nspecs++;
	p = q + 1;
    }
    if(nspecs >  0) {
	int count = 0;
	if((vector = (NC_H5_Filterspec**)malloc(sizeof(NC_H5_Filterspec*)*nspecs)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	/* pass 2: parse */
	p = spec;
	for(count=0;count<nspecs;count++) {
	    NC_H5_Filterspec* aspec = NULL;
	    q = strchr(p,'|');
	    if(q == NULL) q = p + strlen(p); /* fake it */
	    *q = '\0';
	    if(ncaux_filter_parsespec(p,&aspec))
	        {stat = NC_EINVAL; goto done;}
	    vector[count] = aspec; aspec = NULL;
	    p = q+1; /* ok because of double nul */
	}
    }
    if(nspecsp) *nspecsp = nspecs;
    if(vectorp) *vectorp = (nspecs == 0 ? NULL : vector);
    vector = NULL;
done:
    nullfree(spec0);
    if(vector != NULL) {
	int k;
	for(k=0;k<nspecs;k++) {
	    NC_H5_Filterspec* nfs = vector[k];
	    if(nfs->params) free(nfs->params);
	    nullfree(nfs);
	}
	free(vector);
    }
    return stat;
}
#endif

/**************************************************/
/* Wrappers to export selected functions from libnetcdf */

EXTERNL int
ncaux_readfile(const char* filename, size_t* sizep, void** datap)
{
    int stat = NC_NOERR;
    NCbytes* content = ncbytesnew();
    stat = NC_readfile(filename,content);
    if(stat == NC_NOERR && sizep)
        *sizep = ncbyteslength(content);
    if(stat == NC_NOERR && datap)
        *datap = ncbytesextract(content);
    ncbytesfree(content);
    return stat;        
}

EXTERNL int
ncaux_writefile(const char* filename, size_t size, void* content)
{
    return NC_writefile(filename,size,content);
}

/**************************************************/
/**
Reclaim the output tree of data from a call
to e.g. nc_get_vara or the input to e.g. nc_put_vara.
This recursively walks the top-level instances to
reclaim any nested data such as vlen or strings or such.

This function is just a wrapper around nc_reclaim_data.

@param ncid file ncid
@param xtype type id
@param memory to reclaim
@param count number of instances of the type in memory
@return error code
*/

EXTERNL int
ncaux_reclaim_data(int ncid, int xtype, void* memory, size_t count)
{
    /* Defer to the internal version */
    return nc_reclaim_data(ncid, xtype, memory, count);
}

/*
This function is just a wrapper around nc_reclaim_data_all.
@param ncid file ncid
@param xtype type id
@param memory to reclaim
@param count number of instances of the type in memory
@return error code
*/

EXTERNL int
ncaux_reclaim_data_all(int ncid, int xtype, void* memory, size_t count)
{
    /* Defer to the internal version */
    return nc_reclaim_data_all(ncid, xtype, memory, count);
}

EXTERNL int NC_inq_any_type(int ncid, nc_type typeid, char *name, size_t *size, nc_type *basetypep, size_t *nfieldsp, int *classp);

EXTERNL int
ncaux_inq_any_type(int ncid, nc_type typeid, char *name, size_t *sizep, nc_type *basetypep, size_t *nfieldsp, int *classp)
{
    return NC_inq_any_type(ncid, typeid, name, sizep, basetypep, nfieldsp, classp);
}

#ifdef USE_NETCDF4
/**
 @param ncid - only needed for a compound type
 @param xtype - type for which alignment is requested
*/
int
ncaux_type_alignment(int xtype, int ncid, size_t* alignp)
{
    /* Defer to the internal version */
    return NC_type_alignment(ncid, xtype, alignp);
}
#endif

/**
Dump the output tree of data from a call
to e.g. nc_get_vara or the input to e.g. nc_put_vara.
This function is just a wrapper around nc_dump__data.

@param ncid file ncid
@param xtype type id
@param memory to print
@param count number of instances of the type in memory
@return error code
*/

EXTERNL int nc_dump_data(int ncid, nc_type xtype, void* memory, size_t count, char** bufp);

EXTERNL int
ncaux_dump_data(int ncid, int xtype, void* memory, size_t count, char** bufp)
{
    return nc_dump_data(ncid, xtype, memory, count, bufp);
}

/**************************************************/
/* Path List Utilities */

/* Path-list Parser:
@param pathlen length of the pathlist0 arg
@param pathlist0 the string to parse
@param dirs return the parsed directories -- see note below.
@param sep the separator parsing: one of ';' | ':' | '\0', where zero means use platform default.
@return NC_NOERR || NC_EXXX

Note: If dirs->dirs is not NULL, then this function
will allocate the space for the vector of directory path.
The user is then responsible for free'ing that vector
(or call ncaux_plugin_path_reclaim).
*/
EXTERNL int
ncaux_plugin_path_parsen(size_t pathlen, const char* pathlist0, char sep, NCPluginList* dirs)
{
    int stat = NC_NOERR;
    size_t i;
    char* path = NULL;
    char* p;
    size_t count;
    char seps[2] = "\0\0"; /* will contain all allowable separators */

    if(dirs == NULL) {stat = NC_EINVAL; goto done;}

    if(pathlen == 0 || pathlist0 == NULL) {dirs->ndirs = 0; goto done;}

    /* If a separator is specified, use it, otherwise search for ';' or ':' */
    seps[0] = sep;
    if(sep == 0) {
	if(NCgetlocalpathkind() == NCPD_WIN
	    || NCgetlocalpathkind() == NCPD_MSYS)
	   seps[0] = ';';
	else
	    seps[0] = ':';
    }
    if((path = malloc(pathlen+1+1))==NULL) {stat = NC_ENOMEM; goto done;}
    memcpy(path,pathlist0,pathlen);
    path[pathlen] = '\0'; path[pathlen+1] = '\0';  /* double null term */

    for(count=0,p=path;*p;p++) {
	if(strchr(seps,*p) == NULL)
	    continue; /* non-separator */
	else {
	    *p = '\0';
 	    count++;
	}
    }
    count++; /* count last piece */

    /* Save and allocate */
    dirs->ndirs = count;
    if(dirs->dirs == NULL) {
	if((dirs->dirs = (char**)calloc(count,sizeof(char*)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
    }

    /* capture the parsed pieces */
    for(p=path,i=0;i<count;i++) {
	size_t len = strlen(p);
	dirs->dirs[i] = strdup(p);
        p = p+len+1; /* point to next piece */
    }

done:
    nullfree(path);
    return stat;
}

/* Wrapper around ncaux_plugin_path_parsen
to allow passing a nul-terminated string to parse.
@param pathlist0 the nul-termiated string to parse
@param dirs return the parsed directories -- see note below.
@param sep the separator parsing: one of ';' | ':' | '\0', where zero means use platform default.
@return NC_NOERR || NC_EXXX
See also the comments for ncaux_plugin_path_parsen.
*/
EXTERNL int
ncaux_plugin_path_parse(const char* pathlist0, char sep, NCPluginList* dirs)
{
    return ncaux_plugin_path_parsen(nulllen(pathlist0),pathlist0,sep,dirs);
}

/*
Path-list concatenator where given a vector of directories,
concatenate all dirs with specified separator.
If the separator is 0, then use the default platform separator.

@param dirs the counted directory vector to concatenate
@param sep one of ';', ':', or '\0'
@param catp return the concatenation; WARNING: caller frees.
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments
*/
EXTERNL int
ncaux_plugin_path_tostring(const NCPluginList* dirs, char sep, char** catp)
{
    int stat = NC_NOERR;
    NCbytes* buf = ncbytesnew();
    size_t i;

    if(dirs == NULL) {stat = NC_EINVAL; goto done;}
    if(dirs->ndirs > 0 && dirs->dirs == NULL) {stat = NC_EINVAL; goto done;}

    if(sep == '\0')
#ifdef _WIN32
        sep = ';';
#else
	sep = ':';
#endif    
    if(dirs->ndirs > 0) {
	for(i=0;i<dirs->ndirs;i++) {
	    if(i>0) ncbytesappend(buf,sep);
	    if(dirs->dirs[i] != NULL) ncbytescat(buf,dirs->dirs[i]);
	}
    }
    ncbytesnull(buf);
    if(catp) *catp = ncbytesextract(buf);
done:
    ncbytesfree(buf);
    return stat;
}

/*
Clear an NCPluginList object possibly produced by ncaux_plugin_parse function.
@param dirs the object to clear
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments
*/
EXTERNL int
ncaux_plugin_path_clear(NCPluginList* dirs)
{
    int stat = NC_NOERR;
    size_t i;
    if(dirs == NULL || dirs->ndirs == 0 || dirs->dirs == NULL) goto done;
    for(i=0;i<dirs->ndirs;i++) {
	if(dirs->dirs[i] != NULL) free(dirs->dirs[i]);
	dirs->dirs[i] = NULL;
    }
    free(dirs->dirs);
    dirs->dirs = NULL;
    dirs->ndirs = 0;
done:
    return stat;
}

/*
Reclaim an NCPluginList object.
@param dir the object to reclaim
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments
*/
EXTERNL int
ncaux_plugin_path_reclaim(NCPluginList* dirs)
{
    int stat = NC_NOERR;
    if((stat = ncaux_plugin_path_clear(dirs))) goto done;
    nullfree(dirs);
done:
    return stat;
}

/*
Modify a plugin path set to append a new directory to the end.
@param dirs a pointer to an  NCPluginPath object giving the number and vector of directories to which 'dir' argument is appended.
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments

WARNING: dirs->dirs may be reallocated.

Author: Dennis Heimbigner
*/

EXTERNL int
ncaux_plugin_path_append(NCPluginList* dirs, const char* dir)
{
    int stat = NC_NOERR;
    char** newdirs = NULL;
    char** olddirs = NULL;
    if(dirs == NULL || dir == NULL) {stat = NC_EINVAL; goto done;}
    olddirs = dirs->dirs; dirs->dirs = NULL;
    if((newdirs = (char**)calloc(dirs->ndirs+1,sizeof(char*)))==NULL)
	{stat = NC_ENOMEM; goto done;}
    if(dirs->ndirs > 0)
	memcpy(newdirs,olddirs,sizeof(char*)*dirs->ndirs);
    nullfree(olddirs);
    dirs->dirs = newdirs; newdirs = NULL;
    dirs->dirs[dirs->ndirs] = nulldup(dir);
    dirs->ndirs++;
done:
    return stat;
}

/*
Modify a plugin path set to prepend a new directory to the front.
@param dirs a pointer to an  NCPluginList object giving the number and vector of directories to which 'dir' argument is appended.
@return ::NC_NOERR
@return ::NC_EINVAL for illegal arguments

WARNING: dirs->dirs may be reallocated.

Author: Dennis Heimbigner
*/
EXTERNL int
ncaux_plugin_path_prepend(struct NCPluginList* dirs, const char* dir)
{
    int stat = NC_NOERR;
    char** newdirs = NULL;
    char** olddirs = NULL;
    if(dirs == NULL || dir == NULL) {stat = NC_EINVAL; goto done;}
    olddirs = dirs->dirs; dirs->dirs = NULL;
    if((newdirs = (char**)calloc(dirs->ndirs+1,sizeof(char*)))==NULL)
	{stat = NC_ENOMEM; goto done;}
    if(dirs->ndirs > 0)
	memcpy(&newdirs[1],olddirs,sizeof(char*)*dirs->ndirs);
    nullfree(olddirs);
    dirs->dirs = newdirs; newdirs = NULL;
    dirs->dirs[0] = nulldup(dir);
    dirs->ndirs++;
done:
    return stat;
}

/* FORTRAN is not good at manipulating C char** vectors,
   so provide some wrappers for use by netcdf-fortran
   that read/write plugin path as a single string.
   For simplicity, the path separator is always semi-colon.
*/

/**
 * Return the length (as in strlen) of the current plugin path directories encoded as a string.
 * @return length of the string encoded plugin path or -1 if failed.
 * @author Dennis Heimbigner
 *
 * @author: Dennis Heimbigner
*/
int
ncaux_plugin_path_stringlen(void)
{
    int len = 0;
    int stat = NC_NOERR;
    struct NCPluginList npl = {0,NULL};
    char* buf = NULL;

    /* Get the list of dirs */
    if((stat = nc_plugin_path_get(&npl))) goto done;
    /* Convert to a string path separated by ';' */
    if((stat = ncaux_plugin_path_tostring(&npl,';',&buf))) goto done;
    len = (int)nulllen(buf);

done:
    if(npl.dirs != NULL) {(void)ncaux_plugin_path_clear(&npl);}
    nullfree(buf);
    if(stat) return -1; else return len;
}

/**
 * Return the current sequence of directories in the internal global
 * plugin path list encoded as a string path using ';' as a path separator.
 * @param pathlen the length of the path argument.
 * @param path a string into which the current plugin paths are encodeded.
 * @return NC_NOERR | NC_EXXX
 * @author Dennis Heimbigner
 *
 * @author: Dennis Heimbigner
*/
int
ncaux_plugin_path_stringget(int pathlen, char* path)
{
    int stat = NC_NOERR;
    struct NCPluginList npl = {0,NULL};
    char* buf = NULL;

    if(pathlen == 0 || path == NULL) {stat = NC_EINVAL; goto done;}

    /* Get the list of dirs */
    if((stat = nc_plugin_path_get(&npl))) goto done;
    /* Convert to a string path separated by ';' */
    if((stat = ncaux_plugin_path_tostring(&npl,';',&buf))) goto done;
    strncpy(path,buf,(size_t)pathlen);

done:
    nullfree(buf);
    if(npl.dirs != NULL) {(void)ncaux_plugin_path_clear(&npl);}
    return stat;
}

/**
 * Set the current sequence of directories in the internal global
 * plugin path list to the sequence of directories encoded as a
 * string path using ';' as a path separator.
 * @param pathlen the length of the path argument.
 * @param path a string encoding the sequence of directories and using ';' to separate them.
 * @return NC_NOERR | NC_EXXX
 * @author Dennis Heimbigner
 *
 * @author: Dennis Heimbigner
*/
int
ncaux_plugin_path_stringset(int pathlen, const char* path)
{
    int stat = NC_NOERR;
    struct NCPluginList npl = {0,NULL};

    if(pathlen == 0 || path == NULL) {stat = NC_EINVAL; goto done;}
    /* Parse the incoming path */
    if((stat = ncaux_plugin_path_parsen((size_t)pathlen,path,';',&npl))) goto done;
    /* set the list of dirs */
    if((stat = nc_plugin_path_set(&npl))) goto done;

done:
    if(npl.dirs != NULL) {(void)ncaux_plugin_path_clear(&npl);}
    return stat;
}

/**************************************************/

/* De-escape a string */
static char*
deescape(const char* s)
{
    char* des = strdup(s);
    char* p = NULL;
    char* q = NULL;
    if(s == NULL) return NULL;
    for(p=des,q=des;*p;) {
	switch (*p) {
	case '\\':
	    p++;
	    if(*p == '\0') {*q++ = '\\';} break; /* edge case */
	    /* fall thru */
	default:
	    *q++ = *p++;
	    break;
	}
    }
    *q = '\0';
    return des;
}

/**
 * @internal
 *
 * Construct the parsed provenance information
 * Provide a parser for _NCProperties attribute.
 * @param ncprop the contents of the _NCProperties attribute.
 * @param pairsp allocate and return a pointer to a NULL terminated vector of (key,value) pairs.
 * @return NC_NOERR | NC_EXXX
 */
int
ncaux_parse_provenance(const char* ncprop0, char*** pairsp)
{
    int stat = NC_NOERR;
    NClist* pairs = NULL;
    char* ncprop = NULL;
    size_t ncproplen = 0;
    char* thispair = NULL;
    char* p = NULL;
    int i,count = 0;
    int endinner;
    
    if(pairsp == NULL) goto done;
    *pairsp = NULL;
    ncproplen = nulllen(ncprop0);

    if(ncproplen == 0) goto done;
    
    ncprop = (char*)malloc(ncproplen+1+1); /* double nul term */
    strcpy(ncprop,ncprop0); /* Make modifiable copy */
    ncprop[ncproplen] = '\0'; /* double nul term */
    ncprop[ncproplen+1] = '\0'; /* double nul term */
    pairs = nclistnew();

    /* delimit the key,value pairs */
    thispair = ncprop;
    count = 0;
    p = thispair;
    endinner = 0;
    do {
	switch (*p) {
	case '\0':
	    if(strlen(thispair)==0) {stat = NC_EINVAL; goto done;} /* Has to be a non-null key */
	    endinner = 1; /* terminate loop */
	    break;
	case ',': case '|': /* '|' is version one pair separator */
	    *p++ = '\0'; /* terminate this pair */
	    if(strlen(thispair)==0) {stat = NC_EINVAL; goto done;} /* Has to be a non-null key */
	    thispair = p;
	    count++;
	    break;
	case '\\': 
	    p++; /* skip the escape and escaped char */
	    /* fall thru */
	default:
	    p++;
	    break;
	}
    } while(!endinner);
    count++;
    /* Split and store the pairs */
    thispair = ncprop;
    for(i=0;i<count;i++) {
	char* key = thispair;
	char* value = NULL;
	char* nextpair = (thispair + strlen(thispair) + 1);
	/* Find the '=' separator for each pair */
	p = thispair;
	endinner = 0;
	do {
	    switch (*p) {
	    case '\0': /* Key has no value */
	        value = p;
		endinner = 1; /* => leave loop */
		break;
	    case '=':
		*p++ = '\0'; /* split this pair */
		value = p;
		endinner = 1;
		break;
	    case '\\': 
	        p++; /* skip the escape + escaped char */
		/* fall thru */
	    default:
	        p++;
		break;
	    }
	} while(!endinner);
	/* setup next iteration */
        nclistpush(pairs,deescape(key));
        nclistpush(pairs,deescape(value));
	thispair = nextpair;
    }
    /* terminate the list with (NULL,NULL) key value pair*/
    nclistpush(pairs,NULL); nclistpush(pairs,NULL);
    *pairsp = (char**)nclistextract(pairs);
done:
    nullfree(ncprop);
    nclistfreeall(pairs);
    return stat;
}
