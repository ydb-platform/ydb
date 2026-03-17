/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_spectral_truncation.h"

grib_accessor_spectral_truncation_t _grib_accessor_spectral_truncation{};
grib_accessor* grib_accessor_spectral_truncation = &_grib_accessor_spectral_truncation;

void grib_accessor_spectral_truncation_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    int n = 0;

    J_ = c->get_name(grib_handle_of_accessor(this), n++);
    K_ = c->get_name(grib_handle_of_accessor(this), n++);
    M_ = c->get_name(grib_handle_of_accessor(this), n++);
    T_ = c->get_name(grib_handle_of_accessor(this), n++);

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_spectral_truncation_t::unpack_long(long* val, size_t* len)
{
    int ret = 0;

    long J, K, M, T, Tc;

    if (*len < 1)
        return GRIB_ARRAY_TOO_SMALL;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), J_, &J)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), K_, &K)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), M_, &M)) != GRIB_SUCCESS)
        return ret;

    Tc = -1;
    if (J == K && K == M) {
        /* Triangular truncation */
        Tc = (M + 1) * (M + 2);
    }
    if (K == J + M) {
        /* Rhomboidal truncation */
        Tc = 2 * J * M;
    }
    if (J == K && K > M) {
        /* Trapezoidal truncation */
        Tc = M * (2 * J - M);
    }
    *val = Tc;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), T_, &T)) != GRIB_SUCCESS) {
        if (Tc == -1)
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "%s. Spectral Truncation Type Unknown: %s=%ld %s=%ld %s=%ld",
                             name_, J, J, K, K, M_, M);
        Tc = 0;
        grib_set_long(grib_handle_of_accessor(this), T_, Tc);
    }
    else {
        if (Tc != -1 && Tc != T)
            grib_set_long(grib_handle_of_accessor(this), T_, Tc);
    }

    if (ret == GRIB_SUCCESS)
        *len = 1;

    return ret;
}
