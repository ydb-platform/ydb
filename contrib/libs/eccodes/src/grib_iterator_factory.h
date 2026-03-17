/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#pragma once

#include "grib_api_internal.h"
#include "geo/iterator/grib_iterator.h"

extern eccodes::geo_iterator::Iterator* grib_iterator_gaussian;
extern eccodes::geo_iterator::Iterator* grib_iterator_gaussian_reduced;
extern eccodes::geo_iterator::Iterator* grib_iterator_healpix;
extern eccodes::geo_iterator::Iterator* grib_iterator_lambert_azimuthal_equal_area;
extern eccodes::geo_iterator::Iterator* grib_iterator_lambert_conformal;
extern eccodes::geo_iterator::Iterator* grib_iterator_latlon;
extern eccodes::geo_iterator::Iterator* grib_iterator_latlon_reduced;
extern eccodes::geo_iterator::Iterator* grib_iterator_mercator;
extern eccodes::geo_iterator::Iterator* grib_iterator_polar_stereographic;
extern eccodes::geo_iterator::Iterator* grib_iterator_regular;
extern eccodes::geo_iterator::Iterator* grib_iterator_space_view;
extern eccodes::geo_iterator::Iterator* grib_iterator_unstructured;


eccodes::geo_iterator::Iterator* grib_iterator_factory(grib_handle* h, grib_arguments* args, unsigned long flags, int* error);
