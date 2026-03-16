/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

/* Mnemonic */
#define RAW 1

/**************************************************/
/* Data Structure  */

struct ZUTEST* zutest = NULL;

/**************************************************/

#ifdef ZCATCH
/* Place breakpoint here to catch errors close to where they occur*/
int
zbreakpoint(int err)
{
    return ncbreakpoint(err);
}

int
zthrow(int err, const char* file, const char* fcn, int line)
{
    if(err == 0) return err;
    ncbacktrace();
    return zbreakpoint(err);
}

int
zreport(int err, const char* msg, const char* file, const char* fcn, int line)
{
    if(err == 0) return err;
    ZLOG(NCLOGWARN,"!!! zreport: err=%d msg=%s @ %s#%s:%d",err,msg,file,fcn,line);
    ncbacktrace();
    return zbreakpoint(err);
}

#endif /*ZCATCH*/

/**************************************************/
/* Data Structure printers */

#if 0
static NClist  llocal = {NULL};
static void* ldata[1024];
#endif
#define BDATASIZE (1<<14)
static NCbytes blocal = {NULL};
static char  bdata[BDATASIZE];

static void
nczprint_setup(void)
{
#if 0
    if(llocal.content == NULL) {
	NClist* l = nclistnew();
	nclistsetcontents(l,ldata,1024,0);
	llocal = *l;
	nullfree(l);    }
#endif
    if(blocal.content == NULL) {
	NCbytes* b = ncbytesnew();
	ncbytessetcontents(b,bdata,BDATASIZE,0);
	blocal = *b;
	nullfree(b);
    }
}

char*
nczprint_slice(const NCZSlice slice)
{
    return nczprint_slicex(slice,!RAW);
}

char*
nczprint_slicex(const NCZSlice slice, int raw)
{
    char* result = NULL;
    char value[64];

    nczprint_setup();
    if(raw)
        ncbytescat(&blocal,"[");
    else
        ncbytescat(&blocal,"Slice{");
    snprintf(value,sizeof(value),"%lu",(unsigned long)slice.start);
    ncbytescat(&blocal,value);
    ncbytescat(&blocal,":");
    snprintf(value,sizeof(value),"%lu",(unsigned long)slice.stop);
    ncbytescat(&blocal,value);
    if(slice.stride != 1) {
        ncbytescat(&blocal,":");
        snprintf(value,sizeof(value),"%lu",(unsigned long)slice.stride);
        ncbytescat(&blocal,value);
    }
    ncbytescat(&blocal,"|");
    snprintf(value,sizeof(value),"%lu",(unsigned long)slice.len);
    ncbytescat(&blocal,value);
    if(raw)
        ncbytescat(&blocal,"]");
    else
        ncbytescat(&blocal,"}");
    result = ncbytescontents(&blocal);
    return (result);
}

char*
nczprint_slices(int rank, const NCZSlice* slices)
{
    return nczprint_slicesx(rank, slices, !RAW);
}

char*
nczprint_slicesx(int rank, const NCZSlice* slices, int raw)
{
    int i;
    char* result = NULL;

    nczprint_setup();
    for(i=0;i<rank;i++) {
	char* ssl;
	if(!raw)
            ncbytescat(&blocal,"[");
	ssl = nczprint_slicex(slices[i],raw);
	ncbytescat(&blocal,ssl);
	if(!raw)
	    ncbytescat(&blocal,"]");
    }
    result = ncbytescontents(&blocal);
    return (result);
}

char*
nczprint_slab(int rank, const NCZSlice* slices)
{
    return nczprint_slicesx(rank,slices,RAW);
}

char*
nczprint_odom(const NCZOdometer* odom)
{
    char* result = NULL;
    char value[128];
    char* txt = NULL;

    nczprint_setup();
    snprintf(value,sizeof(value),"Odometer{rank=%d ",odom->rank);
    ncbytescat(&blocal,value);

    ncbytescat(&blocal," start=");
    txt = nczprint_vector(odom->rank,odom->start);
    ncbytescat(&blocal,txt);
    ncbytescat(&blocal," stop=");
    txt = nczprint_vector(odom->rank,odom->stop);
    ncbytescat(&blocal,txt);
    ncbytescat(&blocal," len=");
    txt = nczprint_vector(odom->rank,odom->len);
    ncbytescat(&blocal,txt);
    ncbytescat(&blocal," stride=");
    txt = nczprint_vector(odom->rank,odom->stride);
    ncbytescat(&blocal,txt);
    ncbytescat(&blocal," index=");
    txt = nczprint_vector(odom->rank,odom->index);
    ncbytescat(&blocal,txt);
    ncbytescat(&blocal," offset=");
    snprintf(value,sizeof(value),"%llu",nczodom_offset(odom));
    ncbytescat(&blocal,value);
    ncbytescat(&blocal," avail=");
    snprintf(value,sizeof(value),"%llu",nczodom_avail(odom));
    ncbytescat(&blocal,value);
    ncbytescat(&blocal," more=");
    snprintf(value,sizeof(value),"%d",nczodom_more(odom));
    ncbytescat(&blocal,value);
    
    ncbytescat(&blocal,"}");
    result = ncbytescontents(&blocal);
    return (result);
}

char*
nczprint_projection(const NCZProjection proj)
{
   return nczprint_projectionx(proj,!RAW);
}

char*
nczprint_projectionx(const NCZProjection proj, int raw)
{
    char* result = NULL;
    char value[128];

    nczprint_setup();
    ncbytescat(&blocal,"Projection{");
    snprintf(value,sizeof(value),"id=%d,",proj.id);
    ncbytescat(&blocal,value);
    if(proj.skip) ncbytescat(&blocal,"*");
    snprintf(value,sizeof(value),"chunkindex=%lu",(unsigned long)proj.chunkindex);
    ncbytescat(&blocal,value);
    snprintf(value,sizeof(value),",first=%lu",(unsigned long)proj.first);
    ncbytescat(&blocal,value);
    snprintf(value,sizeof(value),",last=%lu",(unsigned long)proj.last);
    ncbytescat(&blocal,value);
    snprintf(value,sizeof(value),",limit=%lu",(unsigned long)proj.limit);
    ncbytescat(&blocal,value);
    snprintf(value,sizeof(value),",iopos=%lu",(unsigned long)proj.iopos);
    ncbytescat(&blocal,value);
    snprintf(value,sizeof(value),",iocount=%lu",(unsigned long)proj.iocount);
    ncbytescat(&blocal,value);
    ncbytescat(&blocal,",chunkslice=");
    result = nczprint_slicex(proj.chunkslice,raw);
    ncbytescat(&blocal,result);
    ncbytescat(&blocal,",memslice=");
    result = nczprint_slicex(proj.memslice,raw);
    ncbytescat(&blocal,result);
    result = ncbytescontents(&blocal);
    return (result);
}

char*
nczprint_allsliceprojections(int r, const NCZSliceProjections* slp)
{
    int i;
    char* s;    

    nczprint_setup();
    for(i=0;i<r;i++) {
	s = nczprint_sliceprojections(slp[i]);
	ncbytescat(&blocal,s);
    } 	
    s = ncbytescontents(&blocal);
    return (s);
}

char*
nczprint_sliceprojections(const NCZSliceProjections slp)
{
    return nczprint_sliceprojectionsx(slp,!RAW);
}

char*
nczprint_sliceprojectionsx(const NCZSliceProjections slp, int raw)
{
    char* result = NULL;
    char tmp[4096];
    int i;

    nczprint_setup();
    snprintf(tmp,sizeof(tmp),"SliceProjection{r=%d range=%s count=%ld",
    		slp.r,nczprint_chunkrange(slp.range),(long)slp.count);
    ncbytescat(&blocal,tmp);
    ncbytescat(&blocal,",projections=[\n");
    for(i=0;i<slp.count;i++) {
	NCZProjection* p = (NCZProjection*)&slp.projections[i];
	ncbytescat(&blocal,"\t");
        result = nczprint_projectionx(*p,raw);
        ncbytescat(&blocal,result);
	ncbytescat(&blocal,"\n");
    }
    result = NULL;
    ncbytescat(&blocal,"]");
    ncbytescat(&blocal,"}\n");
    result = ncbytescontents(&blocal);
    return (result);
}

char*
nczprint_chunkrange(const NCZChunkRange range)
{
    char* result = NULL;
    char digits[64];

    nczprint_setup();
    ncbytescat(&blocal,"ChunkRange{start=");
    snprintf(digits,sizeof(digits),"%llu",range.start);
    ncbytescat(&blocal,digits);
    ncbytescat(&blocal," stop=");
    snprintf(digits,sizeof(digits),"%llu",range.stop);
    ncbytescat(&blocal,digits);
    ncbytescat(&blocal,"}");
    result = ncbytescontents(&blocal);
    return (result);
}

char*
nczprint_idvector(size_t len, const int* ids)
{
    size64_t v[4096];
    size_t i;
    for(i=0;i<len;i++) v[i] = ids[i];    
    return nczprint_vector(len,v);
}

char*
nczprint_paramvector(size_t len, const unsigned* params)
{
    size64_t v[4096];
    size_t i;
    for(i=0;i<len;i++) v[i] = params[i];    
    return nczprint_vector(len,v);
}

char*
nczprint_sizevector(size_t len, const size_t* sizes)
{
    size64_t v[4096];
    size_t i;
    for(i=0;i<len;i++) v[i] = sizes[i];    
    return nczprint_vector(len,v);
}

char*
nczprint_vector(size_t len, const size64_t* vec)
{
    char* result = NULL;
    int i;
    char value[128];

    nczprint_setup();
    ncbytescat(&blocal,"(");
    for(i=0;i<len;i++) {
        if(i > 0) ncbytescat(&blocal,",");
        snprintf(value,sizeof(value),"%lu",(unsigned long)vec[i]);	
	ncbytescat(&blocal,value);
    }
    ncbytescat(&blocal,")");
    result = ncbytescontents(&blocal);
    return (result);
}

char*
nczprint_envv(NClist* envv)
{
    char* result = NULL;
    int i;

    nczprint_setup();
    ncbytescat(&blocal,"(");
    if(envv) {
        for(i=0;i<nclistlength(envv);i++) {
	    const char* p = (const char*)nclistget(envv,i);
            if(i > 0) ncbytescat(&blocal,",");
	    ncbytescat(&blocal,"'");
	    ncbytescat(&blocal,p);
	    ncbytescat(&blocal,"'");
	}
    }
    ncbytescat(&blocal,")");
    result = ncbytescontents(&blocal);
    return (result);
}

void
zdumpcommon(const struct Common* c)
{
    int r;
    fprintf(stderr,"Common:\n");
#if 0
    fprintf(stderr,"\tfile: %s\n",c->file->controller->path);
    fprintf(stderr,"\tvar: %s\n",c->var->hdr.name);
    fprintf(stderr,"\treading=%d\n",c->reading);
#endif
    fprintf(stderr,"\trank=%d",c->rank);
    fprintf(stderr," dimlens=%s",nczprint_vector(c->rank,c->dimlens));
    fprintf(stderr," chunklens=%s",nczprint_vector(c->rank,c->chunklens));
#if 0
    fprintf(stderr,"\tmemory=%p\n",c->memory);
    fprintf(stderr,"\ttypesize=%d\n",c->typesize);
    fprintf(stderr,"\tswap=%d\n",c->swap);
#endif
    fprintf(stderr," shape=%s\n",nczprint_vector(c->rank,c->shape));
    fprintf(stderr,"\tallprojections:\n");
    for(r=0;r<c->rank;r++)
        fprintf(stderr,"\t\t[%d] %s\n",r,nczprint_sliceprojectionsx(c->allprojections[r],RAW));
    fflush(stderr);
}
