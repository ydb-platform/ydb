/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef D4ODOM_H
#define D4ODOM_H 1

typedef struct D4odometer {
    size_t         rank;
    size_t         index[NC_MAX_VAR_DIMS];
    size_t         start[NC_MAX_VAR_DIMS];
#if 0
    size_t         count[NC_MAX_VAR_DIMS];
#endif
    size_t         stride[NC_MAX_VAR_DIMS];
    size_t         stop[NC_MAX_VAR_DIMS];
    size_t         declsize[NC_MAX_VAR_DIMS];
} D4odometer;

extern D4odometer* d4scalarodom_new(void);

extern D4odometer* d4odom_new(size_t rank,
                                const size_t* start, const size_t* count,
				const ptrdiff_t* stride, const size_t* size);

extern void d4odom_free(D4odometer*);

extern int d4odom_more(D4odometer* odom);
extern d4size_t d4odom_next(D4odometer* odom);

extern d4size_t d4odom_offset(D4odometer* odom);

extern d4size_t d4odom_nelements(D4odometer* odom);

extern int d4odom_isWhole(D4odometer* odom);

#endif /*D4ODOM_H*/
