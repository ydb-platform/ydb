/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_octahedral_gaussian.h"

grib_accessor_octahedral_gaussian_t _grib_accessor_octahedral_gaussian{};
grib_accessor* grib_accessor_octahedral_gaussian = &_grib_accessor_octahedral_gaussian;

void grib_accessor_octahedral_gaussian_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    int n             = 0;
    grib_handle* hand = grib_handle_of_accessor(this);

    N_         = c->get_name(hand, n++);
    Ni_        = c->get_name(hand, n++);
    plpresent_ = c->get_name(hand, n++);
    pl_        = c->get_name(hand, n++);
}

/* Returns 1 (=true) if input pl array is Octahedral, 0 otherwise.
 * Possible cases for the deltas in an octahedral pl array:
 *  +4 .. +4        Top part, above equator
 *  +4 .. 0         Top part, above and including equator
 *  +4.. 0  -4..    Middle part, above, equator and below
 *  0 -4..          Equator and below
 *  -4 ..-4         All below equator
 * Anything else is considered not octahedral
 */
static int is_pl_octahedral(const long pl[], size_t size)
{
    long i;
    long prev_diff = -1;
    for (i = 1; i < size; ++i) {
        const long diff = pl[i] - pl[i - 1];
        if (diff == 0) {
            /* prev must be +4 or undef */
            if (!(prev_diff == +4 || i == 1)) {
                return 0;
            }
        }
        else {
            if (labs(diff) != 4) {
                return 0;
            }
            if (diff == +4) {
                /* prev must be +4 or undef */
                if (!(prev_diff == +4 || i == 1)) {
                    return 0;
                }
            }
            if (diff == -4) {
                /* prev must be 0, -4 or undef */
                if (!(prev_diff == -4 || prev_diff == 0 || i == 1)) {
                    return 0;
                }
            }
        }
        prev_diff = diff;
    }
    return 1; /* it's octahedral */
}

int grib_accessor_octahedral_gaussian_t::unpack_long(long* val, size_t* len)
{
    int ret = GRIB_SUCCESS;
    long Ni;
    long plpresent    = 0;
    long* pl          = NULL; /* pl array */
    size_t plsize     = 0;
    grib_handle* hand = grib_handle_of_accessor(this);

    grib_context* c = context_;

    if ((ret = grib_get_long_internal(hand, Ni_, &Ni)) != GRIB_SUCCESS)
        return ret;

    /* If Ni is not missing, then this is a plain gaussian grid and not reduced. */
    /* So it cannot be an octahedral grid */
    if (Ni != GRIB_MISSING_LONG) {
        *val = 0;
        return GRIB_SUCCESS;
    }

    if ((ret = grib_get_long_internal(hand, plpresent_, &plpresent)) != GRIB_SUCCESS)
        return ret;
    if (!plpresent) {
        *val = 0; /* Not octahedral */
        return GRIB_SUCCESS;
    }

    if ((ret = grib_get_size(hand, pl_, &plsize)) != GRIB_SUCCESS)
        return ret;
    ECCODES_ASSERT(plsize); /* pl array must have at least one element */

    pl = (long*)grib_context_malloc_clear(c, sizeof(long) * plsize);
    if (!pl) {
        return GRIB_OUT_OF_MEMORY;
    }
    if ((ret = grib_get_long_array_internal(hand, pl_, pl, &plsize)) != GRIB_SUCCESS)
        return ret;

    /* pl[0] is guaranteed to exist. Have already asserted previously */
    *val = is_pl_octahedral(pl, plsize);
    grib_context_free(c, pl);

    return ret;
}

int grib_accessor_octahedral_gaussian_t::pack_long(const long* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}
