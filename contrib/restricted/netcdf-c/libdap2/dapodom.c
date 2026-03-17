/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "dapincludes.h"
#include "dapodom.h"

/**********************************************/
/* Define methods for a dimension dapodometer*/

/* Build an odometer covering slices startslice up to, but not including, stopslice */
Dapodometer*
dapodom_fromsegment(DCEsegment* segment, size_t startindex, size_t stopindex)
{
    Dapodometer* odom;

    assert(stopindex > startindex);
    assert((stopindex - startindex) <= NC_MAX_VAR_DIMS);
    odom = (Dapodometer*)calloc(1,sizeof(Dapodometer));
    MEMCHECK(odom,NULL);
    odom->rank = (stopindex - startindex);
    for(size_t i=0;i<odom->rank;i++) {
	odom->start[i] = segment->slices[i+startindex].first;
	odom->stride[i] = segment->slices[i+startindex].stride;
	odom->stop[i] = (segment->slices[i+startindex].last + 1);
	/* should the above line be instead?
 	odom->stop[i] = odom->start[i] + (odom->count[i]*odom->stride[i]);
	*/
#if 0
	odom->count[i] = segment->slices[i+startindex].count;
#endif
	odom->declsize[i] = segment->slices[i+startindex].declsize;
	odom->index[i] = odom->start[i];
    }    
    return odom;
}

Dapodometer*
dapodom_new(size_t rank,
	    const size_t* start, const size_t* count,
	    const ptrdiff_t* stride, const size_t* size)
{
    int i;
    Dapodometer* odom = (Dapodometer*)calloc(1,sizeof(Dapodometer));
    MEMCHECK(odom,NULL);
    odom->rank = rank;
    assert(odom->rank <= NC_MAX_VAR_DIMS);
    for(i=0;i<odom->rank;i++) {
	size_t istart,icount,istop,ideclsize;
	istart = (start != NULL ? start[i] : 0);
	icount = (count != NULL ? count[i] : (size != NULL ? size[i] : 1));
	size_t istride = (size_t)(stride != NULL ? stride[i] : 1);
	istop = istart + icount*istride;
	ideclsize = (size != NULL ? size[i]: (istop - istart));
	odom->start[i] = istart;
	odom->stop[i] = istop;
	odom->stride[i] = istride;
	odom->declsize[i] = ideclsize;
	odom->index[i] = odom->start[i];
    }    
    return odom;
}

void
dapodom_free(Dapodometer* odom)
{
    if(odom) free(odom);
}

#if 0
char*
dapodom_print(Dapodometer* odom)
{
    int i;
    static char line[1024];
    char tmp[64];
    line[0] = '\0';
    if(odom->rank == 0) {
	strlcat(line,"[]",sizeof(line));
    } else for(i=0;i<odom->rank;i++) {
	snprintf(tmp,sizeof(tmp),"[%lu/%lu:%lu:%lu]",
		(size_t)odom->index[i],
		(size_t)odom->start[i],
		(size_t)odom->stride[i],
		(size_t)odom->length[i]);
	strlcat(line,tmp,sizeof(line));	
    }
    return line;
}
#endif

int
dapodom_more(Dapodometer* odom)
{
    return (odom->index[0] < odom->stop[0]);
}

/* Convert current dapodometer settings to a single integer count*/
size_t
dapodom_count(Dapodometer* odom)
{
    size_t offset = 0;
    for(size_t i=0;i<odom->rank;i++) {
        offset *= odom->declsize[i];
        offset += odom->index[i];
    } 
    return offset;
}

int
dapodom_next(Dapodometer* odom)
{
    if(odom->rank == 0) return 0; 
    for(size_t i = odom->rank; i-->0;) {
        odom->index[i] += odom->stride[i];
        if(odom->index[i] < odom->stop[i]) break;
	if(i == 0) return 0; /* leave the 0th entry if it overflows*/
	odom->index[i] = odom->start[i]; /* reset this position*/
    }
    return 1;
}

/**************************************************/
size_t
dapodom_varmcount(Dapodometer* odom, const ptrdiff_t* steps, const size_t* declsizes)
{
    int i;
    size_t offset = 0;
    for(i=0;i<odom->rank;i++) {
	size_t tmp;
	tmp = odom->index[i];
	tmp = tmp - odom->start[i];
	tmp = tmp / odom->stride[i];
	tmp = tmp * (size_t)steps[i];
	offset += tmp;
    } 
    return offset;
}

/*
Given a dapodometer, compute the total
number of elements in its space.
*/

#if 0
off_t
dapodom_space(Dapodometer* odom)
{
    size_t i;
    off_t count = 1;
    for(i=0;i<odom->rank;i++) {
	count *= odom->size[i];
    }
    return count;
}
#endif
