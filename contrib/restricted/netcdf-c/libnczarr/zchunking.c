/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

#define MAX(a,b) ((a)>(b)?(a):(b))
#define MIN(a,b) ((a)<(b)?(a):(b))

static int pcounter = 0;

/* Forward */
static int compute_intersection(const NCZSlice* slice, size64_t chunklen, unsigned char isunlimited, NCZChunkRange* range);
static void skipchunk(const NCZSlice* slice, NCZProjection* projection);
static int verifyslice(const NCZSlice* slice);

/**************************************************/
/* Goal:create a vector of chunk ranges: one for each slice in
   the top-level input. For each slice, compute the index (not
   absolute position) of the first chunk that intersects the slice
   and the index of the last chunk that intersects the slice.
   In practice, the count = last - first + 1 is stored instead of the last index.
   Note that this n-dim array of indices may have holes in it if the slice stride
   is greater than the chunk length.
   @param rank variable rank
   @param slices the complete set of slices |slices| == R
   @param ncr (out) the vector of computed chunk ranges.
   @return NC_EXXX error code
*/
int
NCZ_compute_chunk_ranges(
	struct Common* common,
        const NCZSlice* slices, /* the complete set of slices |slices| == R*/
        NCZChunkRange* ncr)
{
    int stat = NC_NOERR;
    int i;
    int rank = common->rank;

    for(i=0;i<rank;i++) {
	if((stat = compute_intersection(&slices[i],common->chunklens[i],common->isunlimited[i],&ncr[i])))
	    goto done;
    }

done:
    return stat;
}

/**
@param Compute chunk range for a single slice.
@param chunklen size of the chunk
@param isunlimited if corresponding dim is unlimited
@param range (out) the range of chunks covered by this slice
@return NC_EXX error code
*/
static int
compute_intersection(
        const NCZSlice* slice,
	size64_t chunklen,
	unsigned char isunlimited,
        NCZChunkRange* range)
{
    range->start = floordiv(slice->start, chunklen);
    range->stop = ceildiv(slice->stop, chunklen);
    return NC_NOERR;
}

/**
Compute the projection of a slice as applied to n'th chunk.
A projection defines the set of grid points touched within a
chunk by a slice. This set of points is the "projection"
of the slice onto the chunk.
This is somewhat complex because:
1. for the first projection, the start is the slice start,
   but after that, we have to take into account that for
   a non-one stride, the start point in a projection may
   be offset by some value in the range of 0..(slice.stride-1).
2. The stride might be so large as to completely skip some chunks.

@return NC_NOERR if ok
@return NC_ERANGE if chunk skipped
@return NC_EXXXX if failed

*/

int
NCZ_compute_projections(struct Common* common,  int r, size64_t chunkindex, const NCZSlice* slice, size_t n, NCZProjection* projections)
{
    int stat = NC_NOERR;
    NCZProjection* projection = NULL;
    NCZProjection* prev = NULL;
    size64_t dimlen = common->dimlens[r]; /* the dimension length for r'th dimension */
    size64_t chunklen = common->chunklens[r]; /* the chunk length corresponding to the dimension */
    size64_t abslimit;

    projection = &projections[n];
    if(n > 0) {
	/* Find last non-skipped projection */
	for(size_t i=n;i-->0;) { /* walk backward */
            if(!projections[i].skip) {
	        prev = &projections[i];
		break;
	    }
	}
	if(prev == NULL) {stat = NC_ENCZARR; goto done;}
    }

    projection->id = ++pcounter;
    projection->chunkindex = chunkindex;

    projection->offset = chunklen * chunkindex; /* with respect to dimension (WRD) */

    /* limit in the n'th touched chunk, taking dimlen and stride->stop into account. */
    abslimit = (chunkindex + 1) * chunklen;
    if(abslimit > slice->stop) abslimit = slice->stop;
    if(abslimit > dimlen) abslimit = dimlen;
    projection->limit = abslimit - projection->offset;

    /*  See if the next point after the last one in prev lands in the current projection.
	If not, then we have skipped the current chunk. Also take limit into account.
	Note by definition, n must be greater than zero because we always start in a relevant chunk.
	*/

    if(n == 0) {
	/*initial case: original slice start is in 1st projection */
	projection->first = slice->start - projection->offset;
	projection->iopos = 0;
    } else { /* n > 0 */
       /* Use absolute offsets for these computations to avoid negative values */
       size64_t abslastpoint, absnextpoint, absthislast;

        /* abs last point touched in prev projection */
        abslastpoint = prev->offset + prev->last;

	/* Compute the abs last touchable point in this chunk */
        absthislast = projection->offset + projection->limit;
	
        /* Compute next point touched after the last point touched in previous projection;
	   note that the previous projection might be wrt a chunk other than the immediately preceding
	   one (because the intermediate ones were skipped).
        */
	absnextpoint = abslastpoint + slice->stride; /* abs next point to be touched */

	if(absnextpoint >= absthislast) { /* this chunk is being skipped */
	    skipchunk(slice,projection);
	    goto done;
	}

        /* Compute start point in this chunk */
	/* basically absnextpoint - abs start of this projection */
	projection->first = absnextpoint - projection->offset;

	/* Compute the memory location of this first point in this chunk */
	projection->iopos = ceildiv((projection->offset - slice->start),slice->stride);

    }
    if(slice->stop > abslimit)
	projection->stop = chunklen;
    else
	projection->stop = slice->stop - projection->offset;

    projection->iocount = ceildiv((projection->stop - projection->first),slice->stride);

    /* Compute the slice relative to this chunk.
       Recall the possibility that start+stride >= projection->limit */
    projection->chunkslice.start = projection->first;
    projection->chunkslice.stop = projection->stop;
    projection->chunkslice.stride = slice->stride;
    projection->chunkslice.len = chunklen;

    /* Last place to be touched */
    projection->last = projection->first + (slice->stride * (projection->iocount - 1));

    projection->memslice.start = projection->iopos;
    projection->memslice.stop = projection->iopos + projection->iocount;
    projection->memslice.stride = 1;
//    projection->memslice.stride = slice->stride;
//    projection->memslice.len = projection->memslice.stop;
    projection->memslice.len = common->memshape[r];
#ifdef NEVERUSE
   projection->memslice.len = dimlen;
   projection->memslice.len = chunklen;
#endif

    if(!verifyslice(&projection->memslice) || !verifyslice(&projection->chunkslice))
        {stat = NC_ECONSTRAINT; goto done;}

done:
    return stat;
}

static void
skipchunk(const NCZSlice* slice, NCZProjection* projection)
{
    projection->skip = 1;
    projection->first = 0;
    projection->last = 0;
    projection->iopos = ceildiv(projection->offset - slice->start, slice->stride);
    projection->iocount = 0;
    projection->chunkslice.start = 0;
    projection->chunkslice.stop = 0;
    projection->chunkslice.stride = 1;
    projection->chunkslice.len = 0;
    projection->memslice.start = 0;
    projection->memslice.stop = 0;
    projection->memslice.stride = 1;
    projection->memslice.len = 0;
}

/* Goal:
Create a vector of projections wrt a slice and a sequence of chunks.
*/

int
NCZ_compute_per_slice_projections(
	struct Common* common,
	int r, /* which dimension are we projecting? */
        const NCZSlice* slice, /* the slice for which projections are computed */
	const NCZChunkRange* range, /* range */
	NCZSliceProjections* slp)
{
    int stat = NC_NOERR;
    size64_t index,slicecount;
    size_t n;

    /* Part fill the Slice Projections */
    slp->r = r;
    slp->range = *range;
    slp->count = range->stop - range->start;
    if((slp->projections = calloc(slp->count,sizeof(NCZProjection))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* Compute the total number of output items defined by this slice
           (equivalent to count as used by nc_get_vars) */
    slicecount = ceildiv((slice->stop - slice->start), slice->stride);
    if(slicecount < 0) slicecount = 0;

    /* Iterate over each chunk that intersects slice to produce projection */
    for(n=0,index=range->start;index<range->stop;index++,n++) {
	if((stat = NCZ_compute_projections(common, r, index, slice, n, slp->projections))) 
	    goto done; /* something went wrong */
    }

done:
    return stat;
}

/* Goal:create a vector of SliceProjection instances: one for each
    slice in the top-level input. For each slice, compute a set
    of projections from it wrt a dimension and a chunk size
    associated with that dimension.
*/
int
NCZ_compute_all_slice_projections(
	struct Common* common,
        const NCZSlice* slices, /* the complete set of slices |slices| == R*/
        const NCZChunkRange* ranges,
        NCZSliceProjections* results)
{
    int stat = NC_NOERR;
    int r; 

    for(r=0;r<common->rank;r++) {
	/* Compute each of the rank SliceProjections instances */
	NCZSliceProjections* slp = &results[r];
        if((stat=NCZ_compute_per_slice_projections(
					common,
					r,
					&slices[r],
					&ranges[r],
                                        slp))) goto done;
    }

done:
    return stat;
}

/**************************************************/
/* Utilities */
    
/* return 0 if slice is malformed; 1 otherwise */
static int
verifyslice(const NCZSlice* slice)
{
    if(slice->stop < slice->start) return 0;
    if(slice->stride <= 0) return 0;
    if((slice->stop - slice->start) > slice->len) return 0;
    return 1;    
}

void
NCZ_clearsliceprojections(int count, NCZSliceProjections* slpv)
{
    if(slpv != NULL) {
	int i;
        for(i=0;i<count;i++) {
	    NCZSliceProjections* slp = &slpv[i];
	    nullfree(slp->projections);	
	}
    }
}

#if 0
static void
clearallprojections(NCZAllProjections* nap)
{
    if(nap != NULL) {
	int i;
	for(i=0;i<nap->rank;i++) 
	    nclistfreeall(&nap->allprojections[i].projections);
    }
}
#endif
