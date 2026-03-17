/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_ifs_param.h"

grib_accessor_ifs_param_t _grib_accessor_ifs_param{};
grib_accessor* grib_accessor_ifs_param = &_grib_accessor_ifs_param;

void grib_accessor_ifs_param_t::init(const long l, grib_arguments* c)
{
    grib_accessor_gen_t::init(l, c);
    int n = 0;

    paramId_ = c->get_name(grib_handle_of_accessor(this), n++);
    type_    = c->get_name(grib_handle_of_accessor(this), n++);
}

int grib_accessor_ifs_param_t::unpack_long(long* val, size_t* len)
{
    int ret      = 0;
    long paramId = 0;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), paramId_, &paramId)) != GRIB_SUCCESS)
        return ret;

    if (paramId > 129000 && paramId < 129999)
        *val = paramId - 129000;
    else if (paramId > 200000 && paramId < 200999)
        *val = paramId - 200000;
    else if (paramId > 211000 && paramId < 211999)
        *val = paramId - 1000;
    else
        *val = paramId;

    return ret;
}

int grib_accessor_ifs_param_t::pack_long(const long* val, size_t* len)
{
    long type    = 0;
    long table   = 128;
    long paramId = *val;
    long param;

    grib_get_long(grib_handle_of_accessor(this), type_, &type);

    if (type == 33 || type == 35) {
        if (paramId > 1000) {
            table   = paramId / 1000;
            param   = paramId - table * 1000;
            paramId = param;
        }
        switch (table) {
            case 210:
                paramId += 211000;
                break;
            case 128:
                paramId += 200000;
                break;
            default:
                break;
        }
    }
    if (type == 50 || type == 52) {
        if (paramId > 1000) {
            table   = paramId / 1000;
            param   = paramId - table * 1000;
            paramId = param;
        }
        switch (table) {
            case 128:
                paramId += 129000;
                break;
            default:
                break;
        }
    }

    return grib_set_long_internal(grib_handle_of_accessor(this), paramId_, paramId);
}

long grib_accessor_ifs_param_t::get_native_type()
{
    return GRIB_TYPE_LONG;
}
