/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "d4includes.h"
#include <stdarg.h>
#include <assert.h>
#include <stddef.h>
#include "d4includes.h"
#include "d4odom.h"
#include "nccrc.h"

/**
This code serves two purposes
1. Preprocess the dap4 serialization wrt endianness, etc.
   (NCD4_processdata)
2. Walk a specified variable instance to convert to netcdf4
   memory representation.
   (NCD4_movetoinstance)

*/

#undef DUMPCHECKSUM

/***************************************************/
/* Forwards */
static int fillstring(NCD4meta*, NCD4offset* offset, void** dstp, NClist* blobs);
static int fillopfixed(NCD4meta*, d4size_t opaquesize, NCD4offset* offset, void** dstp);
static int fillopvar(NCD4meta*, NCD4node* type, NCD4offset* offset, void** dstp, NClist* blobs);
static int fillstruct(NCD4meta*, NCD4node* type, NCD4offset* offset, void** dstp, NClist* blobs);
static int fillseq(NCD4meta*, NCD4node* type, NCD4offset* offset, void** dstp, NClist* blobs);
static unsigned NCD4_computeChecksum(NCD4meta* meta, NCD4node* topvar);

/***************************************************/
/* Macro define procedures */

#ifdef D4DUMPCSUM
static unsigned int debugcrc32(unsigned int crc, const void *buf, unsigned int size)
{
    int i;
    fprintf(stderr,"crc32: ");
    for(i=0;i<size;i++) {fprintf(stderr,"%02x",((unsigned char*)buf)[i]);}
    fprintf(stderr,"\n");
    return NC_crc32(crc,buf,size);
}
#define CRC32 debugcrc32
#else
#define CRC32 NC_crc32
#endif

#define ISTOPLEVEL(var) ((var)->container == NULL || (var)->container->sort == NCD4_GROUP)

/***************************************************/
/* API */

/* Parcel out the dechunked data to the corresponding vars */
int
NCD4_parcelvars(NCD4meta* meta, NCD4response* resp)
{
    int ret = NC_NOERR;
    size_t i;
    NClist* toplevel = NULL;
    NCD4node* root = meta->root;
    NCD4offset* offset = NULL;

    /* Recursively walk the tree in prefix order 
       to get the top-level variables; also mark as unvisited */
    toplevel = nclistnew();
    NCD4_getToplevelVars(meta,root,toplevel);

    /* Compute the  offset and size of the toplevel vars in the raw dap data. */
    offset = BUILDOFFSET(resp->serial.dap,resp->serial.dapsize);
    for(i=0;i<nclistlength(toplevel);i++) {
	NCD4node* var = (NCD4node*)nclistget(toplevel,i);
        if((ret=NCD4_delimit(meta,var,offset,resp->inferredchecksumming))) {
	    FAIL(ret,"delimit failure");
	}
	var->data.response = resp; /* cross link */	
    }
done:
    nclistfree(toplevel);
    nullfree(offset);    
    return THROW(ret);
}

/* Process top level vars wrt checksums and swapping */
int
NCD4_processdata(NCD4meta* meta, NCD4response* resp)
{
    int ret = NC_NOERR;
    size_t i;
    NClist* toplevel = NULL;
    NCD4node* root = meta->root;
    NCD4offset* offset = NULL;

    /* Do we need to swap the dap4 data? */
    meta->swap = (meta->controller->platform.hostlittleendian != resp->remotelittleendian);

    /* Recursively walk the tree in prefix order 
       to get the top-level variables; also mark as unvisited */
    toplevel = nclistnew();
    NCD4_getToplevelVars(meta,root,toplevel);

    /* Extract remote checksums */ 
    for(i=0;i<nclistlength(toplevel);i++) {
	NCD4node* var = (NCD4node*)nclistget(toplevel,i);
        if(resp->inferredchecksumming) {
	    /* Compute checksum of response data: must occur before any byte swapping and after delimiting */
            var->data.localchecksum = NCD4_computeChecksum(meta,var);
#ifdef DUMPCHECKSUM
            fprintf(stderr,"var %s: remote-checksum = 0x%x\n",var->name,var->data.remotechecksum);
#endif
            /* verify checksums */
	    if(!resp->checksumignore) {
                if(var->data.localchecksum != var->data.remotechecksum) {
                    nclog(NCLOGERR,"Checksum mismatch: %s\n",var->name);
                    ret = NC_EDAP;
                    goto done;
                 }
                 /* Also verify checksum attribute */
                 if(resp->attrchecksumming) {
                    if(var->data.attrchecksum != var->data.remotechecksum) {
                        nclog(NCLOGERR,"Attribute Checksum mismatch: %s\n",var->name);
                        ret = NC_EDAP;
                        goto done;
                    }
                }
	    }
	}
        if(meta->swap) {
            if((ret=NCD4_swapdata(resp,var,meta->swap)))
	        FAIL(ret,"byte swapping failed");
	}
	var->data.valid = 1; /* Everything should be in place */
    }

done:
    if(offset) free(offset);
    if(toplevel) nclistfree(toplevel);
    return THROW(ret);
}

/*
Build a single instance of a type. The blobs
argument accumulates any malloc'd data so we can
reclaim it in case of an error.

Activity is to walk the variable's data to
produce a copy that is compatible with the
netcdf4 memory format.

Assumes that NCD4_processdata has been called.
*/

int
NCD4_movetoinstance(NCD4meta* meta, NCD4node* type, NCD4offset* offset, void** dstp, NClist* blobs)
{
    int ret = NC_NOERR;
    void* dst = *dstp;
    d4size_t memsize = type->meta.memsize;
    d4size_t dapsize = type->meta.dapsize;

    /* If the type is fixed size, then just copy it  */
    if(type->subsort <= NC_UINT64 || type->subsort == NC_ENUM) {
	/* memsize and dapsize are the same */
	assert(memsize == dapsize);
	TRANSFER(dst,offset,dapsize);
	INCR(offset,dapsize);
    } else switch(type->subsort) {
        case NC_STRING: /* oob strings */
	    if((ret=fillstring(meta,offset,&dst,blobs)))
	        FAIL(ret,"movetoinstance");
	    break;
	case NC_OPAQUE:
	    if(type->opaque.size > 0) {
	        /* We know the size and its the same for all instances */
	        if((ret=fillopfixed(meta,type->opaque.size,offset,&dst)))
	            FAIL(ret,"movetoinstance");
   	    } else {
	        /* Size differs per instance, so we need to convert each opaque to a vlen */
	        if((ret=fillopvar(meta,type,offset,&dst,blobs)))
	            FAIL(ret,"movetoinstance");
	    }
	    break;
	case NC_STRUCT:
	    if((ret=fillstruct(meta,type,offset,&dst,blobs)))
                FAIL(ret,"movetoinstance");
	    break;
	case NC_SEQ:
	    if((ret=fillseq(meta,type,offset,&dst,blobs)))
                FAIL(ret,"movetoinstance");
	    break;
	default:
	    ret = NC_EINVAL;
            FAIL(ret,"movetoinstance");
    }
    *dstp = dst;

done:
    return THROW(ret);
}

static int
fillstruct(NCD4meta* meta, NCD4node* type, NCD4offset* offset, void** dstp, NClist* blobs)
{
    size_t i;
    int ret = NC_NOERR;
    void* dst = *dstp;
 
#ifdef CLEARSTRUCT
    /* Avoid random data within aligned structs */
    memset(dst,0,type->meta.memsize);
#endif

    /* Walk and read each field taking alignments into account */
    for(i=0;i<nclistlength(type->vars);i++) {
	NCD4node* field = nclistget(type->vars,i);
	NCD4node* ftype = field->basetype;
	void* fdst = (((char*)dst) + field->meta.offset);
	if((ret=NCD4_movetoinstance(meta,ftype,offset,&fdst,blobs)))
            FAIL(ret,"fillstruct");
    }
    dst = ((char*)dst) + type->meta.memsize;
    *dstp = dst;
done:
    return THROW(ret);
}

static int
fillseq(NCD4meta* meta, NCD4node* type, NCD4offset* offset, void** dstp, NClist* blobs)
{
    int ret = NC_NOERR;
    d4size_t i,recordcount;    
    nc_vlen_t* dst;
    NCD4node* vlentype;
    d4size_t recordsize;

    dst = (nc_vlen_t*)*dstp;
    vlentype = type->basetype;    
    recordsize = vlentype->meta.memsize;

    /* Get record count (remember, it is already properly swapped) */
    recordcount = GETCOUNTER(offset);
    SKIPCOUNTER(offset);
    dst->len = (size_t)recordcount;

    /* compute the required memory */
    dst->p = d4alloc(recordsize*recordcount);
    if(dst->p == NULL) 
	FAIL(NC_ENOMEM,"fillseq");

    for(i=0;i<recordcount;i++) {
	/* Read each record instance */
	void* recdst = ((char*)(dst->p))+(recordsize * i);
	if((ret=NCD4_movetoinstance(meta,vlentype,offset,&recdst,blobs)))
	    FAIL(ret,"fillseq");
    }
    dst++;
    *dstp = dst;
done:
    return THROW(ret);
}

/*
Extract and oob a single string instance
*/
static int
fillstring(NCD4meta* meta, NCD4offset* offset, void** dstp, NClist* blobs)
{
    int ret = NC_NOERR;
    d4size_t count;    
    char** dst = *dstp;
    char* q;

    /* Get string count (remember, it is already properly swapped) */
    count = GETCOUNTER(offset);
    SKIPCOUNTER(offset);
    /* Transfer out of band */
    q = (char*)d4alloc(count+1);
    if(q == NULL)
	{FAIL(NC_ENOMEM,"out of space");}
    TRANSFER(q,offset,count);
    q[count] = '\0';
    /* Write the pointer to the string */
    *dst = q;
    dst++;
    *dstp = dst;    
    INCR(offset,count);
#if 0
    nclistpush(blobs,q);
#else
    q = NULL;
#endif
done:
    return THROW(ret);
}

static int
fillopfixed(NCD4meta* meta, d4size_t opaquesize, NCD4offset* offset, void** dstp)
{
    int ret = NC_NOERR;
    d4size_t count, actual;
    int delta;
    void* dst = *dstp;

    /* Get opaque count */
    count = GETCOUNTER(offset);
    SKIPCOUNTER(offset);
    /* verify that it is the correct size */
    actual = count;
    delta = (int)actual - (int)opaquesize;
    if(delta != 0) {
#ifdef FIXEDOPAQUE
	nclog(NCLOGWARN,"opaque changed from %lu to %lu",actual,opaquesize);
	memset(dst,0,opaquesize); /* clear in case we have short case */
	count = (delta < 0 ? actual : opaquesize);
#else
        FAIL(NC_EVARSIZE,"Expected opaque size to be %lld; found %lld",opaquesize,count);
#endif
    }
    /* move */
    TRANSFER(dst,offset,count);
    dst = ((char*)dst) + count;
    *dstp = dst;
    INCR(offset,count);
#ifndef FIXEDOPAQUE
done:
#endif
    return THROW(ret);
}

/*
Move a dap4 variable length opaque out of band.
We treat as if it was (in cdl) ubyte(*).
*/

static int
fillopvar(NCD4meta* meta, NCD4node* type, NCD4offset* offset, void** dstp, NClist* blobs)
{
    int ret = NC_NOERR;
    d4size_t count;
    nc_vlen_t* vlen;
    void* dst = *dstp;
    char* q;

    /* alias dst format */
    vlen = (nc_vlen_t*)dst;

    /* Get opaque count */
    count = GETCOUNTER(offset);
    SKIPCOUNTER(offset);
    /* Transfer out of band */
    q = (char*)d4alloc(count);
    if(q == NULL) FAIL(NC_ENOMEM,"out of space");
    TRANSFER(q,offset,count);
    vlen->p = q;
    vlen->len = (size_t)count;
    q = NULL; /*nclistpush(blobs,q);*/
    dst = ((char*)dst) + sizeof(nc_vlen_t);
    *dstp = dst;
    INCR(offset,count);
done:
    return THROW(ret);
}


/**************************************************/
/* Utilities */

int
NCD4_getToplevelVars(NCD4meta* meta, NCD4node* group, NClist* toplevel)
{
    int ret = NC_NOERR;
    size_t i;

    if(group == NULL)
	group = meta->root;

    /* Collect vars in this group */
    for(i=0;i<nclistlength(group->vars);i++) {
        NCD4node* node = (NCD4node*)nclistget(group->vars,i);
        nclistpush(toplevel,node);
        node->visited = 0; /* We will set later to indicate written vars */
#ifdef D4DEBUGDATA
fprintf(stderr,"toplevel: var=%s\n",node->name);
#endif
    }
    /* Now, recurse into subgroups; will produce prefix order */
    for(i=0;i<nclistlength(group->groups);i++) {
        NCD4node* g = (NCD4node*)nclistget(group->groups,i);
	if((ret=NCD4_getToplevelVars(meta,g,toplevel))) goto done;
    }
done:
    return THROW(ret);
}

int
NCD4_inferChecksums(NCD4meta* meta, NCD4response* resp)
{
    int ret = NC_NOERR;
    size_t i;
    int attrfound;
    NClist* toplevel = NULL;
 
    /* Get the toplevel vars */
    toplevel = nclistnew();
    NCD4_getToplevelVars(meta,meta->root,toplevel);

    /* First, look thru the DMR to see if there is a checksum attribute */
    attrfound = 0;
    for(i=0;i<nclistlength(toplevel);i++) {
	size_t a;
        NCD4node* node = (NCD4node*)nclistget(toplevel,i);
	for(a=0;a<nclistlength(node->attributes);a++) {	
            NCD4node* attr = (NCD4node*)nclistget(node->attributes,a);
	    if(strcmp(D4CHECKSUMATTR,attr->name)==0) {
		const char* val =  NULL;
		if(nclistlength(attr->attr.values) != 1)
		    return NC_EDMR;
		val = (const char*)nclistget(attr->attr.values,0);
		sscanf(val,"%u",&node->data.attrchecksum);
		node->data.checksumattr = 1;
		attrfound = 1;
		break;
	    }
	}
    }
    nclistfree(toplevel);
    resp->attrchecksumming = (attrfound ? 1 : 0);
    /* Infer checksums */
    resp->inferredchecksumming = ((resp->attrchecksumming || resp->querychecksumming) ? 1 : 0);
    return THROW(ret);
}

/* Compute the checksum of each variable's data.
*/
static unsigned
NCD4_computeChecksum(NCD4meta* meta, NCD4node* topvar)
{
    unsigned int csum = 0;

    ASSERT((ISTOPLEVEL(topvar)));

#ifndef HYRAXCHECKSUM
    csum = CRC32(csum,topvar->data.dap4data.memory, (unsigned int)topvar->data.dap4data.size);
#else
    if(topvar->basetype->subsort != NC_STRING) {
            csum = CRC32(csum,topvar->data.dap4data.memory,topvar->data.dap4data.size);
    } else { /* Checksum over the actual strings */
	int i;
        unsigned long long count;
	NCD4offset offset;
        d4size_t dimproduct = NCD4_dimproduct(topvar);
	offset.base = topvar->data.dap4data.memory;
	offset.limit = offset.base + topvar->data.dap4data.size;
	offset.offset = offset.base;
        for(i=0;i<dimproduct;i++) {
            /* Get string count */
            count = GETCOUNTER(&offset);
            SKIPCOUNTER(&offset);
  	    if(meta->swap) swapinline64(&count);
            /* CRC32 count bytes */
            csum = CRC32(csum,offset.offset,count);
	    /* Skip count bytes */
  	    INCR(&offset,count);
        }
    }
#endif
    return csum;
}

#if 0
/* As a hack, if the server sent remotechecksums and
       _DAP4_Checksum_CRC32 is not defined, then define it.
*/
static int
NCD4_addchecksumattr(NCD4meta* meta, NClist* toplevel)
{
    int ret = NC_NOERR;
    int i;

    for(i=0;i<nclistlength(toplevel);i++) {
        NCD4node* node = (NCD4node*)nclistget(toplevel,i);
	
        /* Is remote checksum available? */
	if(node->data.remotechecksummed) {
	    NCD4node* attr = NCD4_findAttr(node,D4CHECKSUMATTR);
	    if(attr == NULL) { /* add it */
		char value[64];
		snprintf(value,sizeof(value),"%u",node->data.remotechecksum);
	        if((ret=NCD4_defineattr(meta,node,D4CHECKSUMATTR,"UInt32",&attr)))
			return NC_EINTERNAL;
		attr->attr.values = nclistnew();
		nclistpush(attr->attr.values,strdup(value));
	    }
	}
    }
    return THROW(ret);
}
#endif

