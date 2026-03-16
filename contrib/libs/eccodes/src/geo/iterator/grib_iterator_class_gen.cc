/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_class_gen.h"

namespace eccodes::geo_iterator {

int Gen::init(grib_handle* h, grib_arguments* args)
{
    int err = GRIB_SUCCESS;
    lats_ = lons_ = data_ = NULL;

    if ((err = Iterator::init(h, args)) != GRIB_SUCCESS)
        return err;

    // Skip the 1st argument which is the name of the iterator itself
    // e.g., latlon, gaussian_reduced etc
    carg_ = 1; // start from 1 and not 0

    const char* s_numPoints    = args->get_name(h, carg_++);
    // The missingValue argument is not currently used. Skip it
    carg_++;  //const char* s_missingValue = args->get_name(h, carg_++);
    const char* s_rawData      = args->get_name(h, carg_++);

    size_t dli = 0;
    if ((err = grib_get_size(h, s_rawData, &dli)) != GRIB_SUCCESS)
        return err;

    long numberOfPoints = 0;
    if ((err = grib_get_long_internal(h, s_numPoints, &numberOfPoints)) != GRIB_SUCCESS)
        return err;

    // See ECC-1792. If we don't want to decode the Data Section, we should not do a check
    // to see if it is consistent with the Grid Section
    if (flags_ & GRIB_GEOITERATOR_NO_VALUES) {
        // Iterator's number of values taken from the Grid Section
        nv_ = numberOfPoints;
    }
    else {
        // Check for consistency between the Grid and Data Sections
        if (numberOfPoints != dli) {
            grib_context_log(h->context, GRIB_LOG_ERROR, "Geoiterator: %s != size(%s) (%ld!=%ld)",
                             s_numPoints, s_rawData, numberOfPoints, dli);
            return GRIB_WRONG_GRID;
        }
        nv_ = dli;
    }

    if (nv_ == 0) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "Geoiterator: size(%s) is %ld", s_rawData, dli);
        return GRIB_WRONG_GRID;
    }

    if ((flags_ & GRIB_GEOITERATOR_NO_VALUES) == 0) {
        // ECC-1525
        // When the NO_VALUES flag is unset, decode the values and store them in the iterator.
        // By default (and legacy) flags==0, so we decode
        data_ = (double*)grib_context_malloc(h->context, (nv_) * sizeof(double));

        if ((err = grib_get_double_array_internal(h, s_rawData, data_, &(nv_)))) {
            return err;
        }
    }
    e_ = -1;

    return err;
}

int Gen::reset()
{
    e_ = -1;
    return 0;
}

int Gen::destroy()
{
    const grib_context* c = h_->context;
    grib_context_free(c, data_);

    return Iterator::destroy();
}

bool Gen::has_next() const
{
    if (flags_ == 0 && data_ == NULL)
        return false;
    if (e_ >= (long)(nv_ - 1))
        return false;
    return true;
}

int Gen::previous(double*, double*, double*) const
{
    return GRIB_NOT_IMPLEMENTED;
}

int Gen::next(double*, double*, double*) const
{
    return GRIB_NOT_IMPLEMENTED;
}

// int Gen::get(double* lat, double* lon, double* val)
//{
//     if (e_ >= (long)(nv_ - 1))
//         return GRIB_END_OF_ITERATION;

//    e_++;
//    if (lat) *lat = 0;
//    if (lon) *lon = 0;
//    if (val) *val = 0;

//    return 1;
//}

}  // namespace eccodes::geo_iterator
