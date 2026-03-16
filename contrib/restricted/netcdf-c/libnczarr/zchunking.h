/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef ZCHUNKING_H
#define ZCHUNKING_H

#include "ncexternl.h"

typedef int (*NCZ_reader)(void* source, size64_t* chunkindices, void** chunkdata);
struct Reader {void* source; NCZ_reader read;};

/* Define the intersecting set of chunks for a slice
   in terms of chunk indices (not absolute positions)
*/
typedef struct NCZChunkRange {
    size64_t start; /* index, not absolute */
    size64_t stop;
} NCZChunkRange;

/* A per-dimension slice for the incoming hyperslab */
typedef struct NCZSlice {
    size64_t start;
    size64_t stop; /* start + (count*stride) */
    size64_t stride;
    size64_t len; /* full dimension length */
} NCZSlice;

/* A projection defines the set of grid points
   for a given set of slices as projected onto
   a single chunk.
*/
typedef struct NCProjection {
    int id;
    int skip; /* Should this projection be skipped? */
    size64_t chunkindex; /* which chunk are we projecting */
    size64_t offset; /* Absolute offset of this chunk (== chunklen*chunkindex) */
    size64_t first;  /* absolute first position to be touched in this chunk */
    size64_t last;   /* absolute position of last value touched */
    size64_t stop;   /* absolute position of last value touched */
    size64_t limit;  /* Actual limit of chunk WRT start of chunk */
    size64_t iopos;    /* start point in the data memory to access the data */
    size64_t iocount;  /* no. of I/O items */
    NCZSlice chunkslice;  /* slice relative to this chunk */
    NCZSlice memslice;  /* slice relative to memory */
} NCZProjection;

/* Set of Projections for a slice */
typedef struct NCZSliceProjections {
    int r; /* 0<=r<rank */
    NCZChunkRange range; /* Chunk ranges covered by this set of projections */
    size_t count; /* |projections| == (range.stop - range.start) */
    NCZProjection* projections; /* Vector of projections derived from the
                                   original slice when intersected across
				   the chunk */
} NCZSliceProjections;

/* Combine some values to avoid having to pass long argument lists*/
struct Common {
    NC_FILE_INFO_T* file;
    NC_VAR_INFO_T* var;
    struct NCZChunkCache* cache;
    int reading; /* 1=> read, 0 => write */
    int rank;
    int scalar; /* 1 => scalar variable */
    size64_t dimlens[NC_MAX_VAR_DIMS];
    unsigned char isunlimited[NC_MAX_VAR_DIMS];
    size64_t chunklens[NC_MAX_VAR_DIMS];
    size64_t memshape[NC_MAX_VAR_DIMS];
    void* memory;
    size_t typesize;
    size64_t chunkcount; /* computed product of chunklens; warning indices, not bytes */
    int swap; /* var->format_info_file->native_endianness == var->endianness */
    size64_t shape[NC_MAX_VAR_DIMS]; /* shape of the output hyperslab */
    NCZSliceProjections* allprojections;
    /* Parametric chunk reader */
    struct Reader reader;
};

/**************************************************/
/* From zchunking.c */
EXTERNL int NCZ_compute_chunk_ranges(struct Common*, const NCZSlice*, NCZChunkRange* ncr);
EXTERNL int NCZ_compute_projections(struct Common*, int r, size64_t chunkindex, const NCZSlice* slice, size_t n, NCZProjection* projections);
EXTERNL int NCZ_compute_per_slice_projections(struct Common*, int rank, const NCZSlice*, const NCZChunkRange*, NCZSliceProjections* slp);
EXTERNL int NCZ_compute_all_slice_projections(struct Common*, const NCZSlice* slices, const NCZChunkRange*, NCZSliceProjections*);

/* From zwalk.c */
EXTERNL int ncz_chunking_init(void);
EXTERNL int NCZ_transferslice(NC_VAR_INFO_T* var, int reading,
		  size64_t* start, size64_t* count, size64_t* stride,
		  void* memory, nc_type typecode);
EXTERNL int NCZ_transfer(struct Common* common, NCZSlice* slices);
EXTERNL int NCZ_transferscalar(struct Common* common);
EXTERNL size64_t NCZ_computelinearoffset(size_t, const size64_t*, const size64_t*);

/* Special entry points for unit testing */
struct Common;
struct NCZOdometer;
EXTERNL int NCZ_projectslices(struct Common*, NCZSlice* slices, struct NCZOdometer**);
EXTERNL int NCZ_chunkindexodom(int rank, const NCZChunkRange* ranges, size64_t*, struct NCZOdometer** odom);
EXTERNL void NCZ_clearsliceprojections(int count, NCZSliceProjections* slpv);
EXTERNL void NCZ_clearcommon(struct Common* common);

#define floordiv(x,y) ((x) / (y))

#define ceildiv(x,y) (((x) % (y)) == 0 ? ((x) / (y)) : (((x) / (y)) + 1))

#define minimum(x,y) ((x) > (y) ? (y) : (x))

#endif /*ZCHUNKING_H*/
