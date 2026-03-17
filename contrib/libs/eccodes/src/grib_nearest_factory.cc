/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_nearest_factory.h"
#include "accessor/grib_accessor_class_nearest.h"

struct table_entry
{
    const char* type;
    eccodes::geo_nearest::Nearest** nearest;
};

static const struct table_entry table[] = {
    { "healpix", &grib_nearest_healpix, },
    { "lambert_azimuthal_equal_area", &grib_nearest_lambert_azimuthal_equal_area, },
    { "lambert_conformal", &grib_nearest_lambert_conformal, },
    { "latlon_reduced", &grib_nearest_latlon_reduced, },
    { "mercator", &grib_nearest_mercator, },
    { "polar_stereographic", &grib_nearest_polar_stereographic, },
    { "reduced", &grib_nearest_reduced, },
    { "regular", &grib_nearest_regular, },
    { "space_view", &grib_nearest_space_view, },
};

eccodes::geo_nearest::Nearest* grib_nearest_factory(grib_handle* h, grib_arguments* args, int* error)
{
    size_t i = 0, num_table_entries = 0;
    *error = GRIB_NOT_IMPLEMENTED;
    char* type = (char*)args->get_name(h, 0);

    num_table_entries = sizeof(table) / sizeof(table[0]);
    for (i = 0; i < num_table_entries; i++) {
        if (strcmp(type, table[i].type) == 0) {
            eccodes::geo_nearest::Nearest* creator = *(table[i].nearest);
            eccodes::geo_nearest::Nearest* it = creator->create();

            *error = it->init(h, args);

            if (*error == GRIB_SUCCESS)
                return it;
            grib_context_log(h->context, GRIB_LOG_ERROR, "grib_nearest_factory: Error instantiating nearest %s (%s)",
                             table[i].type, grib_get_error_message(*error));
            gribNearestDelete(it);
            return NULL;
        }
    }

    grib_context_log(h->context, GRIB_LOG_ERROR, "grib_nearest_factory: Unknown type: %s", type);

    return NULL;
}
