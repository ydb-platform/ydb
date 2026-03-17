/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/** \internal
This file contains various instance operations that operate
on a deep level rather than the shallow level of e.g. nc_free_vlen_t.
Currently two operations are defined:
1. reclaim a vector of instances
2. copy a vector of instances
*/

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "netcdf.h"
#include "nc4internal.h"
#include "nc4dispatch.h"
#include "ncoffsets.h"
#include "ncbytes.h"

#undef REPORT
#undef DEBUG

/* It is helpful to have a structure that identifies a pointer into the overall memory */
typedef struct Position{char* memory;} Position;

static int type_alignment_initialized = 0;

/* Forward */
#ifdef USE_NETCDF4
static int reclaim_datar(NC_FILE_INFO_T* file, NC_TYPE_INFO_T* utype, Position instance);
static int copy_datar(NC_FILE_INFO_T* file, NC_TYPE_INFO_T* utype, Position src, Position dst);
#endif

int NC_print_data(NC_FILE_INFO_T* file, nc_type xtype, const void* memory, size_t count);

/**
Reclaim a vector of instances of a type.  This recursively walks the
top-level instances to reclaim any nested data such as vlen or strings
or such.

Assumes it is passed a pointer to count instances of xtype.
Reclaims any nested data.

These are the internal equivalents of nc_reclaim_data[_all] and
nc_copy_data[_all] and as such operate using internal data structures.

WARNING: DOES NOT RECLAIM THE TOP-LEVEL MEMORY.
The reason is that we do not know how it was allocated (e.g. static vs
dynamic); only the caller can know that.  But note that it assumes all
memory blocks other than the top were dynamically allocated, so they
will be free'd.

Should work for any netcdf type.

Note that this has been optimized significantly, largely
by unwinding various reclaim cliche procedures.

@param nc NC* structure
@param xtype type id
@param memory ptr to top-level memory to reclaim
@param count number of instances of the type in memory block
@return error code
*/

int
NC_reclaim_data(NC* nc, nc_type xtype, void* memory, size_t count)
{
    int stat = NC_NOERR;
    size_t i;
    Position instance;
    NC_FILE_INFO_T* file = NULL;
    NC_TYPE_INFO_T* utype = NULL;

    assert(nc != NULL);
    assert((memory == NULL && count == 0) || (memory != NULL || count > 0));

    /* Process atomic types */

    /* Optimize: Vector of fixed size atomic types (always the case for netcdf-3)*/
    if(xtype < NC_STRING) goto done;

#ifdef USE_NETCDF4
    /* Optimize: Vector of strings */
    if(xtype == NC_STRING) {
	char** ss = (char**)memory;
        for(i=0;i<count;i++) {
	    nullfree(ss[i]);
	}
        goto done;
    }

    /* Process User types */
    assert(USEFILEINFO(nc) != 0);
    file = (NC_FILE_INFO_T*)(nc)->dispatchdata;
    if((stat = nc4_find_type(file,xtype,&utype))) goto done;

    /* Optimize: vector of fixed sized compound type instances */
    if(!utype->varsized) goto done; /* no need to reclaim anything */

    /* Remaining cases: vector of VLEN and vector of (transitive) variable sized compound types.
       These all require potential recursion.
    */

    /* Build a memory walker object */
    instance.memory = (char*)memory; /* use char* so we can do pointer arithmetic */
    /* Walk each vector instance */
    /* Note that we avoid reclaiming the top level memory */
    for(i=0;i<count;i++) {
        if((stat=reclaim_datar(file,utype,instance))) goto done;
	instance.memory += utype->size; /* move to next entry */
    }
#else
    stat = NC_EBADTYPE;
#endif

done:
    return stat;
}

#ifdef USE_NETCDF4
/* Recursive type walker: reclaim a single instance of a variable-sized user-defined type;
   specifically a vlen or a variable-sized compound type instance
*/
static int
reclaim_datar(NC_FILE_INFO_T* file, NC_TYPE_INFO_T* utype, Position instance)
{
    int i,stat = NC_NOERR;
    nc_type basetypeid;
    NC_TYPE_INFO_T* basetype = NULL;
    size_t nfields;
    nc_vlen_t* vlen;
    size_t fid;
    int ndims;
    int dimsizes[NC_MAX_VAR_DIMS];
    size_t alignment = 0;
    Position vinstance; /* walk the vlen instance memory */

    assert(utype->varsized); /* All fixed size cases are optimized out */

    /* Leaving VLEN or Compound */

    if(utype->nc_type_class == NC_VLEN) {
	basetypeid = utype->u.v.base_nc_typeid; /* Get basetype */
	vlen = (nc_vlen_t*)instance.memory;/* memory as vector of nc_vlen_t instances */
	/* Optimize: basetype is atomic fixed size */
	if(basetypeid < NC_STRING) {
	    goto out;
  	}
	/* Optimize: basetype is string */
	if(basetypeid == NC_STRING) {
            if(vlen->len > 0 && vlen->p != NULL) {
	        char** slist = (char**)vlen->p; /* vlen instance is a vector of string pointers */
		for(i=0;i<vlen->len;i++) {if(slist[i] != NULL) free(slist[i]);}
	    }
	    goto out;
	}
	/* Optimize: vlen basetype is a fixed-size user-type */
        if((stat = nc4_find_type(file,basetypeid,&basetype))) goto done;
        if(!basetype->varsized) {
	    goto out;
	}
        /* Remaining case: basetype is itself variable size => recurse */
        if((stat = NC_type_alignment_internal(file,basetypeid,basetype,&alignment))) goto done;;
        vinstance.memory = (char*)vlen->p; /* use char* so we can do pointer arithmetic */
        vinstance.memory = (void*)NC_read_align((uintptr_t)vinstance.memory,alignment);
        for(i=0;i<vlen->len;i++) {
            if((stat=reclaim_datar(file,basetype,vinstance))) goto done; /* reclaim one basetype instance */
	    vinstance.memory += basetype->size; /* move to next base instance */
        }
out:
        if(vlen->len > 0 && vlen->p != NULL) {free(vlen->p);}
        goto done;	
    } else if(utype->nc_type_class == NC_COMPOUND) {    
	Position finstance;  /* mark the fields's instance */
	nfields = nclistlength(utype->u.c.field);
        /* Get info about each field in turn and reclaim it */
        for(fid=0;fid<nfields;fid++) {
	    NC_FIELD_INFO_T* field = NULL;
	    
	    /* Get field's dimension sizes */
	    field = (NC_FIELD_INFO_T*)nclistget(utype->u.c.field,fid);    
	    ndims = field->ndims;
	    int arraycount = 1;
	    for(i=0;i<ndims;i++) {dimsizes[i] = field->dim_size[i]; arraycount *= dimsizes[i];}
            if(field->ndims == 0) {ndims=1; dimsizes[0]=1;} /* fake the scalar case */

            /* "Move" to start of this field's instance */
            finstance.memory = instance.memory + field->offset; /* includes proper alignment */

	    /* optimize: fixed length atomic type */
	    if(field->nc_typeid < NC_STRING) continue;

	    /* optimize: string field type */
	    if(field->nc_typeid == NC_STRING) {
	        char** strvec = (char**)finstance.memory;
		for(i=0;i<arraycount;i++) {
		    if(strvec[i] != NULL) free(strvec[i]);
		}
		continue; /* do next field */
	    }
	    
	    /* optimize: fixed length compound base type */
            if((stat = nc4_find_type(file,field->nc_typeid,&basetype))) goto done;
	    if(!basetype->varsized) continue;

	    /* Field is itself variable length (possibly transitively) */
	    for(i=0;i<arraycount;i++) {
                if((stat = reclaim_datar(file, basetype, finstance))) goto done;
		finstance.memory += basetype->size;
            }
        }
	goto done;
    } else {stat = NC_EBADTYPE; goto done;}

done:
    return stat;
}
#endif

/**************************************************/

/**
Copy a vector of instances of a type.  This recursively walks
the top-level instances to copy any nested data such as vlen or
strings or such.

Assumes it is passed a pointer to count instances of xtype and a
space into which to copy the instance.  Copies any nested data
by calling malloc().

WARNING: DOES NOT ALLOCATE THE TOP-LEVEL MEMORY (see the
nc_copy_data_all function).  Note that all memory blocks other
than the top are dynamically allocated.

Should work for any netcdf type.

@param file NC_FILE_INFO_T* structure
@param xtype type id
@param memory ptr to top-level memory to reclaim
@param count number of instances of the type in memory block
@param copy top-level space into which to copy the instance
@return error code
*/

int
NC_copy_data(NC* nc, nc_type xtype, const void* memory, size_t count, void* copy)
{
    int stat = NC_NOERR;
    size_t i;
    Position src;
    Position dst;
    NC_FILE_INFO_T* file = NULL;
    NC_TYPE_INFO_T* utype = NULL;
    size_t typesize = 0;

    if(memory == NULL || count == 0)
        goto done; /* ok, do nothing */

    assert(nc != NULL);
    assert(memory != NULL || count > 0);
    assert(copy != NULL || count == 0);

    /* Process atomic types */

    /* Optimize: Vector of fixed size atomic types */
    if(xtype < NC_STRING) {
	typesize = NC_atomictypelen(xtype);
        memcpy(copy,memory,count*typesize);
	goto done;
    }
    
#ifdef USE_NETCDF4
    /* Optimize: Vector of strings */
    if(xtype == NC_STRING) {
	char** svec = (char**)memory;
	char** dvec = (char**)copy;
	size_t len;
        for(i=0;i<count;i++) {
	    const char* ssrc = svec[i];
	    char* sdst;
	    if(ssrc == NULL)
		sdst = NULL;
	    else {
	        len = nulllen(ssrc);
                if((sdst = (char*)malloc(len+1))==NULL) {stat = NC_ENOMEM; goto done;}
	        memcpy(sdst,ssrc,len+1); /* +1 for trailing nul */
	    }
	    dvec[i] = sdst;
	}
        goto done;
    }

    assert(USEFILEINFO(nc) != 0);
    file = (NC_FILE_INFO_T*)(nc)->dispatchdata;

    /* Process User types */
    if((stat = nc4_find_type(file,xtype,&utype))) goto done;

    /* Optimize: vector of fixed sized compound type instances */
    if(!utype->varsized) {
        memcpy(copy,memory,count*utype->size);
	goto done;
    }

    /* Remaining cases: vector of VLEN and vector of variable sized compound types.
       These all require potential recursion.
    */

    src.memory = (char*)memory; /* use char* so we can do pointer arithmetic */
    dst.memory = (char*)copy; /* use char* so we can do pointer arithmetic */
    /* Walk each vector instance */
    for(i=0;i<count;i++) {
        if((stat=copy_datar(file,utype,src,dst))) goto done;
	src.memory += utype->size;
	dst.memory += utype->size;
    }
#else
    stat = NC_EBADTYPE;
#endif

done:
    return stat;
}

#ifdef USE_NETCDF4
/* Recursive type walker: reclaim an instance of variable-sized user-defined types;
   specifically a vlen or a variable-sized compound type instance
*/
static int
copy_datar(NC_FILE_INFO_T* file, NC_TYPE_INFO_T* utype, Position src, Position dst)
{
    int i, stat = NC_NOERR;
    nc_type basetypeid;
    NC_TYPE_INFO_T* basetype = NULL;
    size_t nfields;
    nc_vlen_t* srcvlens = NULL;
    nc_vlen_t* dstvlens = NULL;
    size_t fid, arraycount;
    int ndims;
    int dimsizes[NC_MAX_VAR_DIMS];
    Position vsrc, vdst;
    size_t alignment = 0;

    assert(utype->varsized); /* All fixed size cases are optimized out */

    /* Leaving VLEN or Compound */

    if(utype->nc_type_class == NC_VLEN) {
	size_t basetypesize = 0;
	size_t copycount = 0;

	basetypeid = utype->u.v.base_nc_typeid; /* Get basetype */
	srcvlens = (nc_vlen_t*)src.memory;
	dstvlens = (nc_vlen_t*)dst.memory;
	dstvlens->len = srcvlens->len; /* always */

	if(srcvlens->len == 0) {
	    dstvlens->p = NULL;
	    goto done;
	}

	if(basetypeid <= NC_MAX_ATOMIC_TYPE)
	    basetypesize = NC_atomictypelen(basetypeid);
        copycount = srcvlens->len*basetypesize;

	/* Optimize: basetype is atomic fixed size */
	if(basetypeid < NC_STRING) {
	    if((dstvlens->p = (void*)malloc(copycount))==NULL) {stat = NC_ENOMEM; goto done;}
	    memcpy(dstvlens->p,srcvlens->p,copycount);
	    goto done;
  	}

	/* Optimize: basetype is string */
	if(basetypeid == NC_STRING) {
	    char** srcstrvec = (char**)srcvlens->p;
    	    char** dststrvec = NULL;
	    if((dststrvec = (void*)malloc(copycount))==NULL) {stat = NC_ENOMEM; goto done;}
    	    dstvlens->p = (void*)dststrvec;
	    for(i=0;i<srcvlens->len;i++) {
		if((dststrvec[i] = strdup(srcstrvec[i]))==NULL) {stat = NC_ENOMEM; goto done;}
	    }
	    goto done;
	}

	/* User-defined type */
        /* Recompute base type size */
        if((stat = nc4_find_type(file,basetypeid,&basetype))) goto done;
	basetypesize = basetype->size;
        copycount = srcvlens->len*basetypesize;

	/* Optimize: basetype is user-type fixed size */
        if(!basetype->varsized) {
	    if((dstvlens->p = (void*)malloc(copycount))==NULL) {stat = NC_ENOMEM; goto done;}
	    memcpy(dstvlens->p,srcvlens->p,copycount);
	    goto done;
	}

        /* Remaining case: basetype is itself variable size => recurse */
        if((stat = NC_type_alignment_internal(file,basetypeid,basetype,&alignment))) goto done;;
        vsrc.memory = (char*)srcvlens->p; /* use char* so we can do pointer arithmetic */
        if((vdst.memory = (char*)malloc(copycount))==NULL) {stat = NC_ENOMEM; goto done;}
        dstvlens->p = vdst.memory; /* don't lose it */
        vsrc.memory = (void*)NC_read_align((uintptr_t)vsrc.memory,alignment);
	vdst.memory = (void*)NC_read_align((uintptr_t)vdst.memory,alignment);
        for(i=0;i<srcvlens->len;i++) {
            if((stat=copy_datar(file,basetype,vsrc,vdst))) goto done;
	    vsrc.memory += basetype->size;
	    vdst.memory += basetype->size;
        }
	goto done;	
    } else if(utype->nc_type_class == NC_COMPOUND) {    
	Position fsrc;  /* mark the src fields's instance */
	Position fdst;  /* mark the dst fields's instance */
	nfields = nclistlength(utype->u.c.field);
        /* Get info about each field in turn and copy it */
        for(fid=0;fid<nfields;fid++) {
	    NC_FIELD_INFO_T* field = NULL;
	    
	    field = (NC_FIELD_INFO_T*)nclistget(utype->u.c.field,fid);    
	    ndims = field->ndims;
	    arraycount = 1;
	    for(i=0;i<ndims;i++) {dimsizes[i] = field->dim_size[i]; arraycount *= (size_t)dimsizes[i];}
            if(field->ndims == 0) {ndims=1; dimsizes[0]=1;} /* fake the scalar case */

            /* "move" to this field */
            fsrc.memory = src.memory + field->offset;
            fdst.memory = dst.memory + field->offset;

	    /* optimize: fixed length atomic type */
	    if(field->nc_typeid < NC_STRING) {
		size_t typesize = NC_atomictypelen(field->nc_typeid);
		memcpy(fdst.memory,fsrc.memory,arraycount*typesize);
		continue; /* move to next field */
	    }

	    /* optimize: string field type */
	    if(field->nc_typeid == NC_STRING) {
	        char** srcstrvec = (char**)fsrc.memory;
	        char** dststrvec = (char**)fdst.memory;
		for(i=0;i<arraycount;i++) 
		    {if(srcstrvec[i] != NULL) {dststrvec[i] = strdup(srcstrvec[i]);} else {dststrvec[i] = NULL;}}
		continue; /* move to next field */
	    }

	    /* optimize: fixed length compound base type */
            if((stat = nc4_find_type(file,field->nc_typeid,&basetype))) goto done;
	    if(!basetype->varsized) {
		memcpy(fdst.memory,fsrc.memory,arraycount*basetype->size);
		continue; /* move to next field */
	    }

	    /* Remaining case; field type is variable type */
            for(i=0;i<arraycount;i++) {
                if((stat = copy_datar(file, basetype, fsrc, fdst))) goto done;
		fsrc.memory += basetype->size;
		fdst.memory += basetype->size;
            }
        }
	goto done;

    } else {stat = NC_EBADTYPE; goto done;}

done:
    return stat;
}
#endif

/**************************************************/
/* Alignment functions */

#ifdef USE_NETCDF4
uintptr_t
NC_read_align(uintptr_t addr, size_t alignment)
{
  size_t loc_align = (alignment == 0 ? 1 : alignment);
  size_t delta = (addr % loc_align);
  if(delta == 0) return addr;
  return (addr + (alignment - delta));
}


/**
 * Compute proper data alignment for a type
 * @param file - only needed for a compound type
 * @param xtype - type for which alignment is requested
 * @param utype - if known
 * @return 0 if not found
 */
int
NC_type_alignment_internal(NC_FILE_INFO_T* file, nc_type xtype, NC_TYPE_INFO_T* utype, size_t* alignp)
{
    int stat = NC_NOERR;
    size_t align = 0;
    int klass;

    if(!type_alignment_initialized) {
	NC_compute_alignments();
	type_alignment_initialized = 1;
    }
    if(xtype <= NC_MAX_ATOMIC_TYPE)
        {stat = NC_class_alignment(xtype,&align); goto done;}
    else {/* Presumably a user type */
	if(utype == NULL) {
            if((stat = nc4_find_type(file,xtype,&utype))) goto done;
	}
	klass = utype->nc_type_class;
	switch(klass) {
        case NC_VLEN: stat = NC_class_alignment(klass,&align); break;
        case NC_OPAQUE: stat = NC_class_alignment(klass,&align); break;
        case NC_COMPOUND: {/* get alignment of the first field of the compound */
	    NC_FIELD_INFO_T* field = NULL;
	    NC_TYPE_INFO_T* basetype = NULL;
	    if(nclistlength(utype->u.c.field) == 0) {stat = NC_EINVAL; goto done;}
    	    field = (NC_FIELD_INFO_T*)nclistget(utype->u.c.field,0);
	    if(field->nc_typeid <= NC_MAX_ATOMIC_TYPE) {
	        if((stat = nc4_find_type(file,field->nc_typeid,&basetype))) goto done;
	    }
	    stat =  NC_type_alignment_internal(file,field->nc_typeid,basetype,&align); /* may recurse repeatedly */
	} break;
        default: break;
	}
    }
    if(alignp) *alignp = align;

done:
#if 0
Why was this here?
    if(stat == NC_NOERR && align == 0) stat = NC_EINVAL;
#endif
    return stat;
}
#endif

/**************************************************/

/* Internal versions of the XX_all functions */

/* Alternate entry point: includes recovering the top-level memory */
int
NC_reclaim_data_all(NC* nc, nc_type xtypeid, void* memory, size_t count)
{
    int stat = NC_NOERR;

    assert(nc != NULL);

    stat = NC_reclaim_data(nc,xtypeid,memory,count);
    if(stat == NC_NOERR && memory != NULL)
        free(memory);
    return stat;
}

/* Alternate entry point: includes recovering the top-level memory */
int
NC_copy_data_all(NC* nc, nc_type xtype, const void* memory, size_t count, void** copyp)
{
    int stat = NC_NOERR;
    size_t xsize = 0;
    void* copy = NULL;
    NC_FILE_INFO_T* file = NULL;
    NC_TYPE_INFO_T* utype = NULL;

    assert(nc != NULL);

    if(xtype <= NC_STRING && count > 0) {
        xsize = NC_atomictypelen(xtype);
        if((copy = calloc(count,xsize))==NULL) {stat = NC_ENOMEM; goto done;}
	if(xtype < NC_STRING) /* fixed-size atomic type */
   	    memcpy(copy,memory,xsize*count);
	else { /* string type */
	    size_t i;
	    char** strvec = (char**)memory;
	    char** scopyvec = (char**)copy;
	    for(i=0;i<count;i++) {
		char* s = strvec[i];
		char* sdup = NULL;
		if(s != NULL) sdup = strdup(s);
		scopyvec[i] = sdup;
	    }
	}
    }
#ifdef USE_NETCDF4
    else {
        file = (NC_FILE_INFO_T*)(nc)->dispatchdata;
        if((stat = nc4_find_type(file,xtype,&utype))) goto done;
        xsize = utype->size;
        /* allocate the top-level */
        if(count > 0) {
            if((copy = calloc(count,xsize))==NULL) {stat = NC_ENOMEM; goto done;}
        }
        if((stat = NC_copy_data(nc,xtype,memory,count,copy)))
            (void)NC_reclaim_data_all(nc,xtype,copy,count);
    }
#endif
    if(copyp) {*copyp = copy; copy = NULL;}
done:
    return stat;
}

/* Alternate entry point: includes recovering the top-level memory */
int
NC_print_data(NC_FILE_INFO_T* file, nc_type xtype, const void* memory, size_t count)
{
    EXTERNL int nc_print_data(int ncid, nc_type xtype, const void* memory, size_t count);
    return nc_print_data(file->controller->ext_ncid,xtype,memory,count);
}

