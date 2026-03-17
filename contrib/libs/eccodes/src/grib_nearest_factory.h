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
#include "geo/nearest/grib_nearest.h"

extern eccodes::geo_nearest::Nearest* grib_nearest_healpix;
extern eccodes::geo_nearest::Nearest* grib_nearest_lambert_azimuthal_equal_area;
extern eccodes::geo_nearest::Nearest* grib_nearest_lambert_conformal;
extern eccodes::geo_nearest::Nearest* grib_nearest_latlon_reduced;
extern eccodes::geo_nearest::Nearest* grib_nearest_mercator;
extern eccodes::geo_nearest::Nearest* grib_nearest_polar_stereographic;
extern eccodes::geo_nearest::Nearest* grib_nearest_reduced;
extern eccodes::geo_nearest::Nearest* grib_nearest_regular;
extern eccodes::geo_nearest::Nearest* grib_nearest_space_view;

eccodes::geo_nearest::Nearest* grib_nearest_factory(grib_handle* h, grib_arguments* args, int* error);
