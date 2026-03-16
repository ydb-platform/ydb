/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef ZODOM_H
#define ZODOM_H

struct NCZSlice;

typedef struct NCZOdometer {
    int rank; /*rank */
    size64_t* start;
    size64_t* stride;
    size64_t* stop; /* start + (count*stride) */
    size64_t* len; /* for computing offset */
    size64_t* index; /* current value of the odometer*/
    struct NCZOprop {
	int stride1; /* all strides == 1 */
	int start0;  /* all starts == 0 */
//        int optimized; /* stride[rank-1]==1 && start[rank-1]==0 */
    } properties;
} NCZOdometer;

/**************************************************/
/* From zodom.c */
extern NCZOdometer* nczodom_new(int rank, const size64_t*, const size64_t*, const size64_t*, const size64_t*);
extern NCZOdometer* nczodom_fromslices(int rank, const struct NCZSlice* slices);
extern int nczodom_more(const NCZOdometer*);
extern void nczodom_next(NCZOdometer*);
extern size64_t* nczodom_indices(const NCZOdometer*);
extern size64_t nczodom_offset(const NCZOdometer*);
extern void nczodom_reset(NCZOdometer* odom);
extern void nczodom_free(NCZOdometer*);
extern size64_t nczodom_avail(const NCZOdometer*);
extern void nczodom_skipavail(NCZOdometer* odom);
extern size64_t nczodom_laststride(const NCZOdometer* odom);
extern size64_t nczodom_lastlen(const NCZOdometer* odom);
extern void nczodom_print(const NCZOdometer* odom);

#endif /*ZODOM_H*/
