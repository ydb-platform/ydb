/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/*
This file contains various instance operations that operate
on a deep level rather than the shallow level of e.g. nc_free_vlen_t.
Currently two operations are defined:
1. reclaim a vector of instances
2. copy a vector of instances
*/

#include "config.h"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "netcdf.h"
#include "netcdf_aux.h"
#include "nc4internal.h"
#include "nc4dispatch.h"
#include "ncoffsets.h"
#include "ncbytes.h"

#undef REPORT
#undef DEBUG

/* DAP2 and DAP4 currently defer most of their API to a substrate NC
   that hold the true metadata. So there is a level of indirection
   necessary in order to get to the right NC* instance.
*/

#if defined(NETCDF_ENABLE_DAP4) || defined(NETCDF_ENABLE_DAP2)
EXTERNL NC* NCD4_get_substrate(NC* nc);
EXTERNL NC* NCD2_get_substrate(NC* nc);
static NC*
DAPSUBSTRATE(NC* nc)
{
    if(USED2INFO(nc) != 0)
        return NCD2_get_substrate(nc);
    else if(USED4INFO(nc) != 0)
        return NCD4_get_substrate(nc);
    return nc;
}
#else
#define DAPSUBSTRATE(nc) (nc)
#endif

/* It is helpful to have a structure that contains memory and an offset */
typedef struct Position{char* memory; ptrdiff_t offset;} Position;

/* Forward */
static int dump_datar(int ncid, nc_type xtype, Position*, NCbytes* buf);
#ifdef USE_NETCDF4
static int dump_compound(int ncid, nc_type xtype, size_t size, size_t nfields, Position* offset, NCbytes* buf);
static int dump_vlen(int ncid, nc_type xtype, nc_type basetype, Position* offset, NCbytes* buf);
static int dump_enum(int ncid, nc_type xtype, nc_type basetype, Position* offset, NCbytes* buf);
static int dump_opaque(int ncid, nc_type xtype, size_t size, Position* offset, NCbytes* buf);
#endif

/**
\ingroup user_types 
Reclaim an array of instances of an arbitrary type.
This function should be used when the other simpler functions
such as *nc_free_vlens* or *nc_free_string* cannot be used.
This recursively walks the top-level instances to reclaim
any nested data such as vlen or strings or such.

Assumes it is passed a pointer to count instances of xtype.
Reclaims any nested data.

WARNING: This needs access to the type metadata of the file, so
a valid ncid and typeid must be available, which means the file
must not have been closed or aborted.

WARNING: DOES NOT RECLAIM THE TOP-LEVEL MEMORY (see the
nc_reclaim_data_all function).  The reason is that we do not
know how it was allocated (e.g. static vs dynamic); only the
caller can know that.  But note that it assumes all memory
blocks other than the top were dynamically allocated, so they
will be free'd.

Should work for any netcdf format.

@param ncid root id
@param xtype type id
@param memory ptr to top-level memory to reclaim
@param count number of instances of the type in memory block
@return error code
*/

int
nc_reclaim_data(int ncid, nc_type xtype, void* memory, size_t count)
{
    int stat = NC_NOERR;
    NC* nc = NULL;
    
    if(ncid < 0 || xtype <= 0)
        {stat = NC_EINVAL; goto done;}
    if(memory == NULL && count > 0)
        {stat = NC_EINVAL; goto done;}
    if(memory == NULL || count == 0)
        goto done; /* ok, do nothing */
#ifdef REPORT
    fprintf(stderr,">>> reclaim: memory=%p count=%lu ncid=%d xtype=%d\n",memory,(unsigned long)count,ncid,xtype);
#endif

    if((stat = NC_check_id(ncid,&nc))) goto done;    
    nc = DAPSUBSTRATE(nc);

    /* Call internal version */
    stat = NC_reclaim_data(nc,xtype,memory,count);

#if 0
#ifdef USE_NETCDF4
	NC_FILE_INFO_T* file = NULL;
	/* Find info for this file and group and var, and set pointer to each. */
	if ((stat = nc4_find_grp_h5(ncid, NULL, &file))) return stat;
        /* Call internal version */
        stat = NC_reclaim_data(file,xtype,memory,count);
#endif
#endif
done:
    return stat;
}

/**
\ingroup user_types
Reclaim the memory allocated for an array of instances of an arbitrary type.
This recursively walks the top-level instances to reclaim any nested data such as vlen or strings or such.
This function differs from *nc_reclaim_data* in that it also reclaims the top-level memory.

Assumes it is passed a count and a pointer to the top-level memory.
Should work for any netcdf format.

@param ncid root id
@param xtype type id
@param memory ptr to top-level memory to reclaim
@param count number of instances of the type in memory block
@return error code
*/

int
nc_reclaim_data_all(int ncid, nc_type xtypeid, void* memory, size_t count)
{
    int stat = NC_NOERR;
    stat = nc_reclaim_data(ncid,xtypeid,memory,count);
    if(stat == NC_NOERR && memory != NULL)
        free(memory);
    return stat;
}

/**************************************************/

/**
\ingroup user_types
Copy an array of instances of an arbitrary type.  This recursively walks
the top-level instances to copy any nested data such as vlen or strings or such.

Assumes it is passed a pointer to count instances of xtype and a
space into which to copy the instance.  Copies any nested data.

WARNING: This needs access to the type metadata of the file, so
a valid ncid and typeid must be available, which means the file
must not have been closed or aborted.

WARNING: DOES NOT ALLOCATE THE TOP-LEVEL MEMORY (see the
nc_copy_data_all function).  Note that all memory blocks other
than the top are dynamically allocated.

Should work for any netcdf format.

@param ncid root id
@param xtype type id
@param memory ptr to top-level memory to copy
@param count number of instances of the type in memory block
@param copy top-level space into which to copy the instance
@return error code
*/

int
nc_copy_data(int ncid, nc_type xtype, const void* memory, size_t count, void* copy)
{
    int stat = NC_NOERR;
    NC* nc = NULL;
    
    if(ncid < 0 || xtype <= 0)
        {stat = NC_EINVAL; goto done;}
    if(memory == NULL && count > 0)
        {stat = NC_EINVAL; goto done;}
    if(copy == NULL && count > 0)
        {stat = NC_EINVAL; goto done;}
    if(memory == NULL || count == 0)
        goto done; /* ok, do nothing */

#ifdef REPORT
    fprintf(stderr,">>> copy   : copy  =%p memory=%p count=%lu ncid=%d xtype=%d\n",copy,memory,(unsigned long)count,ncid,xtype);
#endif

    if((stat = NC_check_id(ncid,&nc))) goto done;    
    nc = DAPSUBSTRATE(nc);

    /* Call internal version */
    stat = NC_copy_data(nc,xtype,memory,count,copy);
done:
    return stat;
}

/**
\ingroup user_types
Copy an array of instances of an arbitrary type.  This recursively walks
the top-level instances to copy any nested data such as vlen or strings or such.
This function differs from *nc_copy_data* in that it also allocates
the top-level memory.

Assumes it is passed a pointer to count instances of xtype and a pointer
into which the top-level allocated space is stored.

@param ncid root id
@param xtype type id
@param memory ptr to top-level memory to copy
@param count number of instances of the type in memory block
@param copyp pointer into which the allocated top-level space is stored.
@return error code
*/
int
nc_copy_data_all(int ncid, nc_type xtype, const void* memory, size_t count, void** copyp)
{
    int stat = NC_NOERR;
    size_t xsize = 0;
    void* copy = NULL;

    /* Get type size */
    if((stat = ncaux_inq_any_type(ncid,xtype,NULL,&xsize,NULL,NULL,NULL))) goto done;

    /* allocate the top-level */
    if(count > 0) {
        if((copy = calloc(count,xsize))==NULL)
	    {stat = NC_ENOMEM; goto done;}
    }
    stat = nc_copy_data(ncid,xtype,memory,count,copy);
    if(copyp) {*copyp = copy; copy = NULL;}

done:
    if(copy)
        stat = nc_reclaim_data_all(ncid,xtype,copy,count);

    return stat;
}

/**************************************************/
/* Alignment functions */

#ifdef USE_NETCDF4

/**
 @param ncid - only needed for a compound type
 @param xtype - type for which alignment is requested
 @return 0 if not found
*/
int
NC_type_alignment(int ncid, nc_type xtype, size_t* alignp)
{
    int stat = NC_NOERR;
    NC* nc = NULL;

    if((stat = NC_check_id(ncid,&nc))) goto done;
    nc = DAPSUBSTRATE(nc);
    if(USENC3INFO(nc)) goto done;

    /* Call internal version */
    stat = NC_type_alignment_internal((NC_FILE_INFO_T*)nc->dispatchdata,xtype,NULL,alignp);
done:
    return stat;
}
#endif

/**************************************************/
/* Dump an instance into a bytebuffer

@param ncid root id
@param xtype type id
@param memory ptr to top-level memory to dump
@param count number of instances of the type in memory block
@param bufp return ptr to a buffer of dump'd data
@return error code
*/

int
nc_dump_data(int ncid, nc_type xtype, const void* memory, size_t count, char** bufp)
{
    int stat = NC_NOERR;
    size_t i;
    Position offset;
    NCbytes* buf = ncbytesnew();

    if(ncid < 0 || xtype <= 0)
        {stat = NC_EINVAL; goto done;}
    if(memory == NULL && count > 0)
        {stat = NC_EINVAL; goto done;}
    if(memory == NULL || count == 0)
        goto done; /* ok, do nothing */
#ifdef REPORT
    fprintf(stderr,">>> dump: memory=%p count=%lu ncid=%d xtype=%d\n",memory,(unsigned long)count,ncid,xtype);
#endif
    offset.memory = (char*)memory; /* use char* so we can do pointer arithmetic */
    offset.offset = 0;
    for(i=0;i<count;i++) {
	if(i > 0) ncbytescat(buf," ");
        if((stat=dump_datar(ncid,xtype,&offset,buf))) /* dump one instance */
	    break;
    }

    if(bufp) *bufp = ncbytesextract(buf);

done:
    ncbytesfree(buf);
    return stat;
}

int
nc_print_data(int ncid, nc_type xtype, const void* memory, size_t count)
{
    char* s = NULL;
    int stat = NC_NOERR;
    if((stat=nc_dump_data(ncid,xtype,memory,count,&s))) return stat;
    fprintf(stderr,"%s\n",s);
    nullfree(s)
    return stat;
}

/* Recursive type walker: dump a single instance */
static int
dump_datar(int ncid, nc_type xtype, Position* offset, NCbytes* buf)
{
    int stat = NC_NOERR;
    size_t xsize;
    nc_type basetype;
    size_t nfields;
    int klass;
    char s[128];

    /* Get relevant type info */
    if((stat = ncaux_inq_any_type(ncid,xtype,NULL,&xsize,&basetype,&nfields,&klass))) goto done;

    switch  (xtype) {
    case NC_CHAR:
	snprintf(s,sizeof(s),"'%c'",*(char*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_BYTE:
	snprintf(s,sizeof(s),"%d",*(char*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_UBYTE:
	snprintf(s,sizeof(s),"%u",*(unsigned char*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_SHORT:
	snprintf(s,sizeof(s),"%d",*(short*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_USHORT:
	snprintf(s,sizeof(s),"%d",*(unsigned short*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_INT:
	snprintf(s,sizeof(s),"%d",*(int*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_UINT:
	snprintf(s,sizeof(s),"%d",*(unsigned int*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_FLOAT:
	snprintf(s,sizeof(s),"%f",*(float*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_INT64:
	snprintf(s,sizeof(s),"%lld",*(long long*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_UINT64:
	snprintf(s,sizeof(s),"%llu",*(unsigned long long*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
    case NC_DOUBLE:
	snprintf(s,sizeof(s),"%lf",*(double*)(offset->memory+offset->offset));
	ncbytescat(buf,s);
	break;
#ifdef USE_NETCDF4
    case NC_STRING: {
        char* s = *(char**)(offset->memory + offset->offset);
	ncbytescat(buf,"\"");
	ncbytescat(buf,s);
	ncbytescat(buf,"\"");
	} break;
#endif
    default:
#ifdef USE_NETCDF4
    	/* dump a user type */
        switch (klass) {
        case NC_OPAQUE: stat = dump_opaque(ncid,xtype,xsize,offset,buf); break;
        case NC_ENUM: stat = dump_enum(ncid,xtype,basetype,offset,buf); break;
        case NC_COMPOUND: stat = dump_compound(ncid,xtype,xsize,nfields,offset,buf); break;
        case NC_VLEN: stat = dump_vlen(ncid,xtype,basetype,offset,buf); break;
        default: stat = NC_EBADTYPE; break;
        }
#else
	stat = NC_EBADTYPE;
#endif
	break;
    }
    if(xtype <= NC_MAX_ATOMIC_TYPE)
        offset->offset += (ptrdiff_t)xsize;

done:
    return stat;
}

#ifdef USE_NETCDF4
static int
dump_vlen(int ncid, nc_type xtype, nc_type basetype, Position* offset, NCbytes* buf)
{
    int stat = NC_NOERR;
    size_t i;
    nc_vlen_t* vl = (nc_vlen_t*)(offset->memory+offset->offset);
    char s[128];

    if(vl->len > 0 && vl->p == NULL)
        {stat = NC_EINVAL; goto done;}

    snprintf(s,sizeof(s),"{len=%u,p=(",(unsigned)vl->len);
    ncbytescat(buf,s);
    /* dump each entry in the vlen list */
    if(vl->len > 0) {
	Position voffset;
	size_t alignment = 0;
	if((stat = NC_type_alignment(ncid,basetype,&alignment))) goto done;;
	voffset.memory = vl->p;
	voffset.offset = 0;
        for(i=0;i<vl->len;i++) {
	    if(i > 0) ncbytescat(buf," ");
	    voffset.offset = (ptrdiff_t)NC_read_align((uintptr_t)voffset.offset, alignment);
	    if((stat = dump_datar(ncid,basetype,&voffset,buf))) goto done;
	}
    } 
    ncbytescat(buf,")}");
    offset->offset += (ptrdiff_t)sizeof(nc_vlen_t);
    
done:
    return stat;
}

static int
dump_enum(int ncid, nc_type xtype, nc_type basetype, Position* offset, NCbytes* buf)
{
    int stat = NC_NOERR;

    /* basically same as an instance of the enum's integer basetype */
    stat = dump_datar(ncid,basetype,offset,buf);
    return stat;
}

static int
dump_opaque(int ncid, nc_type xtype, size_t size, Position* offset, NCbytes* buf)
{
    size_t i;
    char sx[16];
    /* basically a fixed size sequence of bytes */
    ncbytescat(buf,"|");
    for(i=0;i<size;i++) {
	unsigned char x = (unsigned char)*(offset->memory+offset->offset+i);
	snprintf(sx,sizeof(sx),"%2x",x);
	ncbytescat(buf,sx);
    }
    ncbytescat(buf,"|");
    offset->offset += (ptrdiff_t)size;
    return NC_NOERR;
}

static int
dump_compound(int ncid, nc_type xtype, size_t size, size_t nfields, Position* offset, NCbytes* buf)
{
    int stat = NC_NOERR;
    int i;
    ptrdiff_t saveoffset;
    int ndims;
    int dimsizes[NC_MAX_VAR_DIMS];
    int fid;

    saveoffset = offset->offset;

    ncbytescat(buf,"<");

    /* Get info about each field in turn and dump it */
    for(fid=0;fid<(int)nfields;fid++) {
	size_t fieldalignment;
	nc_type fieldtype;
	char name[NC_MAX_NAME];
	char sd[128];

	/* Get all relevant info about the field */
	if((stat = nc_inq_compound_field(ncid,xtype,fid,name,&fieldalignment,&fieldtype,&ndims,dimsizes))) goto done;
	if(fid > 0) ncbytescat(buf,";");
	ncbytescat(buf,name);
	ncbytescat(buf,"(");
        if(ndims > 0) {
	    int j;
	    for(j=0;j<ndims;j++) {
		snprintf(sd,sizeof(sd),"%s%d",(j==0?"":","),(int)dimsizes[j]);
		ncbytescat(buf,sd);
	    }
	}
	ncbytescat(buf,")");
	if(ndims == 0) {ndims=1; dimsizes[0]=1;} /* fake the scalar case */
	/* Align to this field */
	offset->offset = saveoffset + (ptrdiff_t)fieldalignment;
	/* compute the total number of elements in the field array */
	int arraycount = 1;
	for(i=0;i<ndims;i++) arraycount *= dimsizes[i];
	for(i=0;i<arraycount;i++) {
	    if(i > 0) ncbytescat(buf," ");
	    if((stat = dump_datar(ncid, fieldtype, offset,buf))) goto done;
	}
    }
    ncbytescat(buf,">");
    /* Return to beginning of the compound and move |compound| */
    offset->offset = saveoffset;
    offset->offset += (ptrdiff_t)size;

done:
    return stat;
}
#endif

/* Extended version that can handle atomic typeids */
int
NC_inq_any_type(int ncid, nc_type typeid, char *name, size_t *size,
                  nc_type *basetypep, size_t *nfieldsp, int *classp)
{
    int stat = NC_NOERR;
#ifdef USE_NETCDF4
    if(typeid >= NC_FIRSTUSERTYPEID) {
        stat = nc_inq_user_type(ncid,typeid,name,size,basetypep,nfieldsp,classp);
    } else
#endif
    if(typeid > NC_NAT && typeid <= NC_MAX_ATOMIC_TYPE) {
	if(basetypep) *basetypep = NC_NAT;
	if(nfieldsp) *nfieldsp = 0;
	if(classp) *classp = typeid;
	stat = NC4_inq_atomic_type(typeid,name,size);
    } else
        stat = NC_EBADTYPE;
    return stat;
}
