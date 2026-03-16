/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */


#include "grib_accessor_class_grid_spec.h"

#include "eccodes_config.h"

#if defined(HAVE_ECKIT_GEO)
    #include <cstring>
    #include <memory>

    #error #include "eckit/geo/Grid.h"
    #error #include "eckit/geo/Exceptions.h"

    #error #include "geo/GribSpec.h"
    #error #include "geo/EckitMainInit.h"
#endif


grib_accessor_grid_spec_t _grib_accessor_grid_spec;
grib_accessor* grib_accessor_grid_spec = &_grib_accessor_grid_spec;


void grib_accessor_grid_spec_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    length_ = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
}

long grib_accessor_grid_spec_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

int grib_accessor_grid_spec_t::pack_string(const char* sval, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;

#if defined(HAVE_GEOGRAPHY) && defined(HAVE_ECKIT_GEO)
    // TODO(mapm)
#else
    return GRIB_NOT_IMPLEMENTED;
#endif
}

int grib_accessor_grid_spec_t::unpack_string(char* v, size_t* len)
{
#if defined(HAVE_GEOGRAPHY) && defined(HAVE_ECKIT_GEO)
    ECCODES_ASSERT(0 < *len);
    ECCODES_ASSERT(v != nullptr);

    auto* h = grib_handle_of_accessor(this);
    ECCODES_ASSERT(h != nullptr);

    std::string spec_str;

    try {
        eccodes::geo::eckit_main_init();

        std::unique_ptr<const eckit::geo::Spec> spec(new eccodes::geo::GribSpec(h));
        std::unique_ptr<const eckit::geo::Grid> grid(eckit::geo::GridFactory::build(*spec));

        spec_str = grid->spec_str();
    }
    catch (eckit::geo::Exception& e) {
        grib_context_log(context_, GRIB_LOG_ERROR, "grib_accessor_grid_spec_t: geo::Exception thrown (%s)", e.what());
        return GRIB_GEOCALCULUS_PROBLEM;
    }
    catch (std::exception& e) {
        grib_context_log(context_, GRIB_LOG_ERROR, "grib_accessor_grid_spec_t: Exception thrown (%s)", e.what());
        return GRIB_GEOCALCULUS_PROBLEM;
    }

    // guarantee null-termination
    auto length = spec_str.length();
    if (*len < length + 1) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, length, *len);
        return GRIB_BUFFER_TOO_SMALL;
    }

    std::strncpy(v, spec_str.c_str(), *len);
    ECCODES_ASSERT(v[length] == '\0');

    *len = length;

    return GRIB_SUCCESS;
#else
    return GRIB_NOT_IMPLEMENTED;
#endif
}
