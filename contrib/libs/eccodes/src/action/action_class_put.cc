/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_put.h"

grib_action* grib_action_create_put(grib_context* context, const char* name, grib_arguments* args)
{
    grib_context_log(context, GRIB_LOG_ERROR, "The 'export' statement is deprecated");
    return NULL;
}
