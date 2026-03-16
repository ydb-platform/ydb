/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_variable.h"

grib_action* grib_action_create_variable(grib_context* context, const char* name, const char* op, const long len, grib_arguments* params, grib_arguments* default_value, int flags, const char* name_space)
{
    return new eccodes::action::Variable(context, name, op, len, params, default_value, flags, name_space);
}

namespace eccodes::action
{

Variable::Variable(grib_context* context, const char* name, const char* op, const long len, grib_arguments* params, grib_arguments* default_value, int flags, const char* name_space) :
Gen(context, name, op, len, params, default_value, flags, name_space, NULL)
{
    class_name_ = "action_class_variable";

    /* printf("CREATE %s\n",name); */
}

int Variable::execute(grib_handle* h)
{
    return create_accessor(h->root, NULL);
}

}  // namespace eccodes::action
