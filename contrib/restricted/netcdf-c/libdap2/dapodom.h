/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef DAPODOM_H
#define DAPODOM_H 1

#include <stddef.h>

typedef struct Dapodometer {
    size_t         rank;
    size_t         index[NC_MAX_VAR_DIMS];
    size_t         start[NC_MAX_VAR_DIMS];
#if 0
    size_t         count[NC_MAX_VAR_DIMS];
#endif
    size_t         stride[NC_MAX_VAR_DIMS];
    size_t         stop[NC_MAX_VAR_DIMS];
    size_t         declsize[NC_MAX_VAR_DIMS];
} Dapodometer;

#ifndef TESTING
extern Dapodometer* dapodom_fromsegment(DCEsegment* segment, size_t start, size_t stop);
#endif

extern Dapodometer* dapodom_new(size_t rank,
                                const size_t* start, const size_t* count,
				const ptrdiff_t* stride, const size_t* size);

extern void dapodom_free(Dapodometer*);

extern int dapodom_more(Dapodometer* odom);
extern int dapodom_next(Dapodometer* odo);

extern size_t dapodom_count(Dapodometer* odo);

extern size_t dapodom_varmcount(Dapodometer*, const ptrdiff_t*, const size_t*);

#endif /*DAPODOM_H*/
