/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#include "config.h"
#include "ocinternal.h"
#include "ocdebug.h"
#include "ocdump.h"
#include "ncutil.h"

/* Forward*/
static OCerror ocread(OCdata*, XXDR*, char*, size_t, size_t, size_t);

#ifdef OCDEBUG

static void
octrace(char* proc, OCstate* state, OCdata* data)
{
    NCbytes* buffer = ncbytesnew();
    ocdumpdatapath(state,data,buffer);
    fprintf(stderr,"%s: %s\n",proc,ncbytescontents(buffer));
}

#else

#define octrace(proc,state,data)
#endif /*OCDEBUG*/

/* Use this to attach to a data tree for a DATADDS */
OCerror
ocdata_getroot(OCstate* state, OCnode* root, OCdata** datap)
{
    OCdata* data;
    assert(root->tree->dxdclass == OCDATADDS);
    assert(root->octype == OC_Dataset);

    if(root->tree->data.data == NULL)
	return OCTHROW(OC_ENODATA);

    data = root->tree->data.data;
    if(datap) *datap = data;

    octrace("attach",state,data);

    return OCTHROW(OC_NOERR);
}

OCerror
ocdata_container(OCstate* state, OCdata* data, OCdata** containerp)
{
    OCdata* container;
    OCnode* pattern;

    OCASSERT(state != NULL);

    pattern = data->pattern;

    if(pattern->container == NULL)
	return OCTHROW(OC_EBADTYPE);

    container = data->container;
    if(container == NULL)
	return OCTHROW(OC_EBADTYPE);

    if(containerp) *containerp = container;

    octrace("container", state, container);

    return OC_NOERR;
}

OCerror
ocdata_root(OCstate* state, OCdata* data, OCdata** rootp)
{
    OCdata* root;
    OCnode* pattern;

    OCASSERT(state != NULL);

    pattern = data->pattern;
    root = pattern->tree->data.data;
    if(rootp) *rootp = root;

    octrace("root", state, root);

    return OC_NOERR;
}

OCerror
ocdata_ithfield(OCstate* state, OCdata* container, size_t index, OCdata** fieldp)
{
    OCdata* field;
    OCnode* pattern;

    OCASSERT(state != NULL);
    OCASSERT(container != NULL);

    pattern = container->pattern;

    if(!ociscontainer(pattern->octype))
	return OCTHROW(OC_EBADTYPE);

    /* Validate index */
    if(index >= container->ninstances)
	return OCTHROW(OC_EINDEX);

    field = container->instances[index];
    if(fieldp) *fieldp = field;

    octrace("ithfield", state, field);

    return OC_NOERR;
}

OCerror
ocdata_ithelement(OCstate* state, OCdata* data, size_t* indices, OCdata** elementp)
{
    int stat = OC_NOERR;
    OCdata* element;
    OCnode* pattern;
    size_t index,rank;

    OCASSERT(state != NULL);
    OCASSERT(data != NULL);

    pattern = data->pattern;
    rank = pattern->array.rank;

    /* Must be a dimensioned Structure */
    if(pattern->octype != OC_Structure || rank == 0)
	return OCTHROW(OC_EBADTYPE);

    /* Validate indices */
    if(!ocvalidateindices(rank,pattern->array.sizes,indices))
	return OCTHROW(OC_EINVALCOORDS);

    /* compute linearized index */
    index = ocarrayoffset(rank,pattern->array.sizes,indices);

    if(index >= data->ninstances)
	return OCTHROW(OC_EINDEX);

    element = data->instances[index];

    if(elementp) *elementp = element;

    octrace("ithelement", state, element);

    return OCTHROW(stat);
}

/* Move to the ith sequence record. */
OCerror
ocdata_ithrecord(OCstate* state, OCdata* data,
                 size_t index, /* record number */
		 OCdata** recordp
                 )
{
    int stat = OC_NOERR;
    OCdata* record;
    OCnode* pattern;

    OCASSERT(state != NULL);
    OCASSERT(data != NULL);

    pattern = data->pattern;

    /* Must be a Sequence */
    if(pattern->octype != OC_Sequence
       || !fisset(data->datamode,OCDT_SEQUENCE))
	return OCTHROW(OC_EBADTYPE);

    /* Validate index */
    if(index >= data->ninstances)
	return OCTHROW(OC_EINDEX);

    record = data->instances[index];

    if(recordp) *recordp = record;

    octrace("ithrecord", state, record);

    return OCTHROW(stat);
}

OCerror
ocdata_position(OCstate* state, OCdata* data, size_t* indices)
{
    OCnode* pattern;

    OCASSERT(state != NULL);
    OCASSERT(data != NULL);
    OCASSERT(indices != NULL);

    pattern = data->pattern;
    if(fisset(data->datamode,OCDT_RECORD))
	indices[0] = data->index;
    else if(fisset(data->datamode,OCDT_ELEMENT)) {
	/* Transform the linearized array index into a set of indices */
	ocarrayindices(data->index,
                       pattern->array.rank,
                       pattern->array.sizes,
                       indices);
    } else
	return OCTHROW(OC_EBADTYPE);
    return OCTHROW(OC_NOERR);
}

OCerror
ocdata_recordcount(OCstate* state, OCdata* data, size_t* countp)
{
    OCASSERT(state != NULL);
    OCASSERT(data != NULL);
    OCASSERT(countp != NULL);

    if(data->pattern->octype != OC_Sequence
       || !fisset(data->datamode,OCDT_SEQUENCE))
	return OCTHROW(OC_EBADTYPE);

    *countp = data->ninstances;		

    return OC_NOERR;
}

/**************************************************/
/*
In order to actually extract data, one must move to the
specific primitive typed field containing the data of
interest by using ocdata_fieldith().  Then this procedure
is invoked to extract some subsequence of items from the
field.  For scalars, the start and count are ignored.
Note that start and count are linearized from the oc_data_read
arguments.
*/

OCerror
ocdata_read(OCstate* state, OCdata* data, size_t start, size_t count,
		void* memory, size_t memsize)
             
{
    int stat = OC_NOERR;
    XXDR* xdrs;
    OCtype etype;
    int isscalar;
    size_t elemsize, totalsize, countsize;
    OCnode* pattern;

    octrace("read", state, data);

    assert(state != NULL);
    assert(data != NULL);
    assert(memory != NULL);
    assert(memsize > 0);

    pattern = data->pattern;
    assert(pattern->octype == OC_Atomic);
    etype = pattern->etype;

    isscalar = (pattern->array.rank == 0 ? 1 : 0);

    /* validate memory space*/
    elemsize = octypesize(etype);
    totalsize = elemsize*data->ninstances;
    countsize = elemsize*count;
    if(totalsize < countsize || memsize < countsize)
	return OCTHROW(OC_EINVAL);

    /* Get XXDR* */
    xdrs = pattern->root->tree->data.xdrs;

    if(isscalar) {
        /* Extract the data */
        stat = ocread(data,xdrs,(char*)memory,memsize,0,1);
    } else {
        /* Validate the start and count */
        if(start >= data->ninstances
           || (start+count) > data->ninstances)
	    return OCTHROW(OC_EINVALCOORDS);
        /* Extract the data */
        stat = ocread(data,xdrs,(char*)memory,memsize,start,count);
    }

    return OCTHROW(stat);
}

/**************************************************/
/*
Extract data from a leaf into memory.
*/

static OCerror
ocread(OCdata* data, XXDR* xdrs, char* memory, size_t memsize, size_t start, size_t count)
{
    int i;
    OCnode* pattern;
    OCtype etype;
    off_t xdrtotal, xdrstart;
    int scalar;

    OCASSERT(data != NULL);
    OCASSERT(memory != NULL);
    OCASSERT(memsize > 0);
    OCASSERT(count > 0);
    OCASSERT((start+count) <= data->ninstances);

    pattern = data->pattern;
    etype = pattern->etype;
    scalar = (pattern->array.rank == 0);

    /* Note that for strings, xdrsize == 0 */
    xdrtotal = (off_t)count*data->xdrsize; /* amount (in xdr sizes) to read */
    xdrstart = (off_t)start*data->xdrsize; /* offset from the start of the data */

    size_t elemsize = octypesize(etype); /* wrt memory, not xdrsize */
    size_t totalsize = elemsize*count;

    /* validate memory space*/
    if(memsize < totalsize) return OCTHROW(OC_EINVAL);

    /* copy out with appropriate byte-order conversions */
    switch (etype) {

    case OC_Int32: case OC_UInt32: case OC_Float32:
	xxdr_setpos(xdrs,data->xdroffset+xdrstart);
	if(!xxdr_getbytes(xdrs,memory,xdrtotal)) {goto xdrfail;}
	if(!xxdr_network_order) {
	    unsigned int* p;
	    for(p=(unsigned int*)memory,i=0;i<count;i++,p++) {
		swapinline32(p);
	    }
	}
	break;
	
    case OC_Int64: case OC_UInt64:
	xxdr_setpos(xdrs,data->xdroffset+xdrstart);
	if(!xxdr_getbytes(xdrs,memory,xdrtotal)) {goto xdrfail;}
	if(!xxdr_network_order) {
	    unsigned long long* llp;
	    for(llp=(unsigned long long*)memory,i=0;i<count;i++,llp++) {
		swapinline64(llp);
	    }
	}
        break;

    case OC_Float64:
	xxdr_setpos(xdrs,data->xdroffset+xdrstart);
	if(!xxdr_getbytes(xdrs,memory,xdrtotal)) {goto xdrfail;}
	{
	    double* dp;
	    for(dp=(double*)memory,i=0;i<count;i++,dp++) {
		double swap;
		xxdrntohdouble((char*)dp,&swap);
		*dp = swap;
	    }
	}
	break;

    /* non-packed fixed length, but memory size < xdrsize */
    case OC_Int16: case OC_UInt16: {
	/* In order to avoid allocating a lot of space, we do this one int at a time */
	/* Remember also that the short is not packed, so its xdr size is twice
           its memory size */
        xxdr_setpos(xdrs,data->xdroffset+xdrstart);
        if(scalar) {
	    if(!xxdr_ushort(xdrs,(unsigned short*)memory)) {goto xdrfail;}
	} else {
	    unsigned short* sp = (unsigned short*)memory;
	    for(i=0;i<count;i++,sp++) {
	        unsigned int tmp;
		if(!xxdr_getbytes(xdrs,(char*)&tmp,(off_t)XDRUNIT))
		    {goto xdrfail;}
		/* convert from network order if necessary */
		if(!xxdr_network_order)
		    swapinline32(&tmp);
		/* store as unsigned short */
		*sp = (unsigned short)tmp;
	    }
	}
	} break;

    /* Do the byte types, packed/unpacked */
    case OC_Byte:
    case OC_UByte:
    case OC_Char:
	if(scalar) {
	    /* scalar bytes are stored in xdr as int */
	    xxdr_setpos(xdrs,data->xdroffset+xdrstart);
	    if(!xxdr_uchar(xdrs,(unsigned char*)memory)) {goto xdrfail;}
	} else {
	    /* the xdroffset will always be at the start of the
               packed data, so we need to add the start count to it */
	    xxdr_setpos(xdrs,data->xdroffset+xdrstart);
	    if(!xxdr_getbytes(xdrs,memory,xdrtotal)) {goto xdrfail;}
	}
	break;

    /* Hard case, because strings are variable length */
    case OC_String: case OC_URL: {
	/* use the data->nstrings data->string fields */
	char** sp = (char**)memory;
	if(count > data->nstrings)
	    return OCTHROW(OC_EDATADDS);
	for(size_t i=0;i<count;i++,sp++) {
	    off_t len;
	    off_t offset = data->strings[start+i];
            xxdr_setpos(xdrs,offset);
            /* get the string */
	    if(!xxdr_string(xdrs,sp,&len))
		{goto xdrfail;}
	}
        } break;

    default: OCPANIC("unexpected etype"); break;
    }

    return OC_NOERR;

xdrfail:
    nclog(NCLOGERR,"DAP DATADDS packet is apparently too short");
    return OCTHROW(OC_EDATADDS);
}

