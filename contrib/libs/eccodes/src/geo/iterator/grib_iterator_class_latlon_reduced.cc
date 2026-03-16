/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_class_latlon_reduced.h"

eccodes::geo_iterator::LatlonReduced _grib_iterator_latlon_reduced{};
eccodes::geo_iterator::Iterator* grib_iterator_latlon_reduced = &_grib_iterator_latlon_reduced;

namespace eccodes::geo_iterator {

int LatlonReduced::next(double* lat, double* lon, double* val) const
{
    if ((long)e_ >= (long)(nv_ - 1))
        return 0;
    e_++;

    *lat = lats_[e_];
    *lon = lons_[e_];
    if (val && data_) {
        *val = data_[e_];
    }
    return 1;
}

int LatlonReduced::init(grib_handle* h, grib_arguments* args)
{
    int ret = GRIB_SUCCESS;
    if ((ret = Gen::init(h, args)) != GRIB_SUCCESS)
        return ret;

    double laf;
    double lal;
    long nlats;
    double lof, tlof;
    double lol, dimin;
    long* pl;
    size_t plsize = 0;
    long k, j, ii;
    long nlons, plmax;
    double jdirinc = 0;
    double idirinc = 0;
    double dlon    = 0;
    int islocal    = 0;
    long nlons2    = 0; /* adjusted num of longitudes */

    const char* latofirst   = args->get_name(h, carg_++);
    const char* longoffirst = args->get_name(h, carg_++);
    const char* latoflast   = args->get_name(h, carg_++);
    const char* longoflast  = args->get_name(h, carg_++);
    const char* nlats_name  = args->get_name(h, carg_++);
    const char* jdirec      = args->get_name(h, carg_++);
    const char* plac        = args->get_name(h, carg_++);

    if ((ret = grib_get_double_internal(h, latofirst, &laf)))
        return ret;
    if ((ret = grib_get_double_internal(h, longoffirst, &lof)))
        return ret;

    if ((ret = grib_get_double_internal(h, latoflast, &lal)))
        return ret;
    if ((ret = grib_get_double_internal(h, longoflast, &lol)))
        return ret;

    if ((ret = grib_get_long_internal(h, nlats_name, &nlats)))
        return ret;

    if ((ret = grib_get_double_internal(h, jdirec, &jdirinc)))
        return ret;

    plsize = nlats;
    pl     = (long*)grib_context_malloc(h->context, plsize * sizeof(long));
    grib_get_long_array_internal(h, plac, pl, &plsize);

    lats_ = (double*)grib_context_malloc(h->context, nv_ * sizeof(double));
    lons_ = (double*)grib_context_malloc(h->context, nv_ * sizeof(double));

    plmax = pl[0];
    for (j = 0; j < nlats; j++)
        if (plmax < pl[j])
            plmax = pl[j];
    dimin = 360.0 / plmax;

    if (360 - fabs(lol - lof) < 2 * dimin) {
        dlon    = 360;
        islocal = 0;
    }
    else if (lol < lof) {
        /* handle something like 150 to -120 to treat as 150 to 240 */
        /* so that dlon is 90 (not -270) */
        dlon    = lol + 360.0 - lof;
        islocal = 1;
    }
    else {
        dlon    = lol - lof;
        islocal = 1;
    }

    if (laf > lal)
        jdirinc = -jdirinc;
    k = 0;
    for (j = 0; j < nlats; j++) {
        nlons  = pl[j];
        tlof   = lof;
        nlons2 = nlons - islocal;
        /*Sometimes there are no points on a latitude! Protect against div by zero*/
        if (nlons2 < 1)
            nlons2 = 1;
        idirinc = dlon / nlons2;
        for (ii = 0; ii < nlons; ii++) {
            lats_[k] = laf;
            lons_[k] = tlof;
            tlof += idirinc;
            k++;
        }
        laf += jdirinc;
    }

    e_ = -1;
    grib_context_free(h->context, pl);

    return ret;
}

int LatlonReduced::destroy()
{
    DEBUG_ASSERT(h_);
    const grib_context* c = h_->context;
    grib_context_free(c, lats_);
    grib_context_free(c, lons_);

    return Gen::destroy();
}

}  // namespace eccodes::geo_iterator
