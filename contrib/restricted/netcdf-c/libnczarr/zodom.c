/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

/*Forward*/
static int buildodom(int rank, NCZOdometer** odomp);

void
nczodom_reset(NCZOdometer* odom)
{
    int r;
    for(r=0;r<odom->rank;r++)
        odom->index[r] = odom->start[r];
}

NCZOdometer*
nczodom_new(int rank, const size64_t* start, const size64_t* stop, const size64_t* stride, const size64_t* len)
{
    int i;
    NCZOdometer* odom = NULL;
    if(buildodom(rank,&odom)) return NULL;
    odom->properties.stride1 = 1; /* assume */
    odom->properties.start0 = 1; /* assume */
    for(i=0;i<rank;i++) { 
	odom->start[i] = (size64_t)start[i];
	odom->stride[i] = (size64_t)stride[i];
	odom->stop[i] = (size64_t)stop[i];
	odom->len[i] = (size64_t)len[i];
	if(odom->start[i] != 0) odom->properties.start0 = 0;
	if(odom->stride[i] != 1) odom->properties.stride1 = 0;
    }
    nczodom_reset(odom);
    for(i=0;i<rank;i++)
        assert(stop[i] >= start[i] && stride[i] > 0 && (len[i]+1) >= stop[i]);
    return odom;
}

NCZOdometer*
nczodom_fromslices(int rank, const NCZSlice* slices)
{
    size_t i;
    NCZOdometer* odom = NULL;

    if(buildodom(rank,&odom)) return NULL;
    odom->properties.stride1 = 1; /* assume */
    odom->properties.start0 = 1; /* assume */
    for(i=0;i<rank;i++) {    
	odom->start[i] = slices[i].start;
	odom->stop[i] = slices[i].stop;
	odom->stride[i] = slices[i].stride;
	odom->len[i] = slices[i].len;
	if(odom->start[i] != 0) odom->properties.start0 = 0;
	if(odom->stride[i] != 1) odom->properties.stride1 = 0;
    }
    nczodom_reset(odom);
    for(i=0;i<rank;i++) {
        assert(slices[i].stop >= slices[i].start && slices[i].stride > 0);
        assert((slices[i].stop - slices[i].start) <= slices[i].len);
    }
    return odom;
}
  
void
nczodom_free(NCZOdometer* odom)
{
    if(odom == NULL) return;
    nullfree(odom->start);
    nullfree(odom->stop);
    nullfree(odom->stride);
    nullfree(odom->len);
    nullfree(odom->index);
    nullfree(odom);
}

int
nczodom_more(const NCZOdometer* odom)
{
    return (odom->index[0] < odom->stop[0]);
}

void
nczodom_next(NCZOdometer* odom)
{
    int i;
    int rank;
    rank = odom->rank;
    for(i=rank-1;i>=0;i--) {
	odom->index[i] += odom->stride[i];
        if(odom->index[i] < odom->stop[i]) break;
        if(i == 0) goto done; /* leave the 0th entry if it overflows */
        odom->index[i] = odom->start[i]; /* reset this position */
    }
done:
    return;
}
  
/* Get the value of the odometer */
size64_t*
nczodom_indices(const NCZOdometer* odom)
{
    return odom->index;
}

size64_t
nczodom_offset(const NCZOdometer* odom)
{
    int i;
    size64_t offset;
    int rank = odom->rank;

    offset = 0;
    for(i=0;i<rank;i++) {
#if 1
        offset *= odom->len[i];
#else
        offset *= odom->stop[i];
#endif
        offset += odom->index[i];
    } 
    return offset;
}

static int
buildodom(int rank, NCZOdometer** odomp)
{
    int stat = NC_NOERR;
    NCZOdometer* odom = NULL;
    if(odomp) {
        if((odom = calloc(1,sizeof(NCZOdometer))) == NULL)
	    goto done;   
        odom->rank = rank;
        if((odom->start=calloc(1,(sizeof(size64_t)*(size_t)rank)))==NULL) goto nomem;
        if((odom->stop=calloc(1,(sizeof(size64_t)*(size_t)rank)))==NULL) goto nomem;
        if((odom->stride=calloc(1,(sizeof(size64_t)*(size_t)rank)))==NULL) goto nomem;
        if((odom->len=calloc(1,(sizeof(size64_t)*(size_t)rank)))==NULL) goto nomem;
        if((odom->index=calloc(1,(sizeof(size64_t)*(size_t)rank)))==NULL) goto nomem;
        *odomp = odom; odom = NULL;
    }
done:
    nczodom_free(odom);
    return stat;
nomem:
    stat = NC_ENOMEM;
    goto done;
}

/* Compute the total avail in last position */
size64_t
nczodom_avail(const NCZOdometer* odom)
{
    size64_t avail;
    /* The best we can do is compute the count for the rightmost index */
    avail = (odom->stop[odom->rank-1] - odom->start[odom->rank-1]);
    return avail;
}

/*
Incr the odometer by nczodom_avail() values.
Calling nczodom_next at that point should properly increment
rest of the odometer.
*/
void
nczodom_skipavail(NCZOdometer* odom)
{
    if(odom->rank > 0)
        odom->index[odom->rank-1] = odom->stop[odom->rank-1];
}

size64_t
nczodom_laststride(const NCZOdometer* odom)
{
    assert(odom != NULL && odom->rank > 0);
    return odom->stride[odom->rank-1];
}

size64_t
nczodom_lastlen(const NCZOdometer* odom)
{
    assert(odom != NULL && odom->rank > 0);
    return odom->len[odom->rank-1];
}

void
nczodom_print(const NCZOdometer* odom)
{
    size_t i;
    fprintf(stderr,"odom{rank=%d offset=%llu avail=%llu",odom->rank,nczodom_offset(odom),nczodom_avail(odom));
    fprintf(stderr," start=(");
        for(i=0;i<odom->rank;i++) {fprintf(stderr,"%s%llu",(i==0?"":" "),(unsigned long long)odom->start[i]);}
	fprintf(stderr,")");
    fprintf(stderr," stride=(");
        for(i=0;i<odom->rank;i++) {fprintf(stderr,"%s%llu",(i==0?"":" "),(unsigned long long)odom->stride[i]);}
	fprintf(stderr,")");
    fprintf(stderr," stop=(");
        for(i=0;i<odom->rank;i++) {fprintf(stderr,"%s%llu",(i==0?"":" "),(unsigned long long)odom->stop[i]);}
	fprintf(stderr,")");
    fprintf(stderr," len=(");
        for(i=0;i<odom->rank;i++) {fprintf(stderr,"%s%llu",(i==0?"":" "),(unsigned long long)odom->len[i]);}
	fprintf(stderr,")");
    fprintf(stderr," index=(");
        for(i=0;i<odom->rank;i++) {fprintf(stderr,"%s%llu",(i==0?"":" "),(unsigned long long)odom->index[i]);}
	fprintf(stderr,")");
    fprintf(stderr,"}\n");
}
