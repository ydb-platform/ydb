/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "config.h"
#include <stdlib.h>
#include <assert.h>
#include "netcdf.h"
#include "d4util.h"
#include "d4odom.h"

/**********************************************/
/* Define methods for a dimension dapodometer*/

/* Build an odometer covering slices startslice up to,
   but not including, stopslice
*/

/*
Future optimization. It is sometimes the case that
some suffix of the index set cover a contiguous chunk
of the index space. This happens when the index positions
starting at p up to rank have a start of zero, a count
the same as the max index size, and a stride of one.
Then we can effectively treat indices 0 thru p-1 as
a short odometer that steps through the chunks.
In practice, it is usually only ncdump that does this.
A simpler optimization that is to see if the (start,count)
covers the whole index space and has stride one.
We can detect and use this optimization elsewhere to
circumvent use of the odometer altogether.
See d4odometer#NCD4_odomWhole().
*/

D4odometer*
d4odom_new(size_t rank,
	    const size_t* start, const size_t* count,
	    const ptrdiff_t* stride, const size_t* size)
{
    int i;
    D4odometer* odom = (D4odometer*)calloc(1,sizeof(D4odometer));
    if(odom == NULL)
	return NULL;
    odom->rank = rank;
    assert(odom->rank <= NC_MAX_VAR_DIMS);
    for(i=0;i<odom->rank;i++) {
	size_t istart,icount,istop,ideclsize;
	size_t istride;
	istart = (start != NULL ? start[i] : 0);
	icount = (count != NULL ? count[i] : (size != NULL ? size[i] : 1));
	istride = (size_t)(stride != NULL ? stride[i] : 1);
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
d4odom_free(D4odometer* odom)
{
    if(odom) free(odom);
}

#if 0
char*
d4odom_print(D4odometer* odom)
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
d4odom_more(D4odometer* odom)
{
    return (odom->index[0] < odom->stop[0]);
}

d4size_t
d4odom_next(D4odometer* odom)
{
    d4size_t count;
    if(odom->rank == 0) { /*scalar*/
	odom->index[0]++;
	return 0;
    }
    count = d4odom_offset(odom); /* convenience */
    for(size_t i=odom->rank; i-- >0;) {
        odom->index[i] += odom->stride[i];
        if(odom->index[i] < odom->stop[i]) break;
	if(i == 0) break; /* leave the 0th entry if it overflows*/
	odom->index[i] = odom->start[i]; /* reset this position*/
    }
    return count;
}

/* Convert current d4odometer settings to a single integer offset*/
d4size_t
d4odom_offset(D4odometer* odom)
{
    int i;
    d4size_t offset = 0;
    for(i=0;i<odom->rank;i++) {
	offset *= odom->declsize[i];
	offset += odom->index[i];
    } 
    return offset;
}

/**************************************************/
/*
Given a d4odometer, compute the total
number of elements in its space.
*/

d4size_t
d4odom_nelements(D4odometer* odom)
{
    size_t i;
    d4size_t count = 1;
    for(i=0;i<odom->rank;i++) {
	count *= odom->declsize[i];
    }
    return count;
}

int
d4odom_isWhole(D4odometer* odom)
{
    int i;
    for(i=0;i<odom->rank;i++) {
	if(odom->start[i] != 0
	   || odom->stride[i] != 1
	   || odom->stop[i] != odom->declsize[i])
	    return 0;
    }
    return 1;
}

/* Scalar Odometer support */
D4odometer*
d4scalarodom_new(void)
{
    D4odometer* odom = (D4odometer*)calloc(1,sizeof(D4odometer));
    odom->rank = 0;
    /* Fake things to execute exactly once */
    odom->index[0] = 0;
    odom->stop[0] = 1;
    return odom;
}
