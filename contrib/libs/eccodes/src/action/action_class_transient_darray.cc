/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_transient_darray.h"

grib_action* grib_action_create_transient_darray(grib_context* context, const char* name, grib_darray* darray, int flags)
{
    return new eccodes::action::TransientDArray(context, name, darray, flags);
}

namespace eccodes::action
{

TransientDArray::TransientDArray(grib_context* context, const char* name, grib_darray* darray, int flags) :
    Gen(context, name, "transient_darray", 0, NULL, NULL, flags, NULL, NULL)
{
    class_name_ = "action_class_transient_darray";
    darray_     = darray;
    name2_      = grib_context_strdup_persistent(context, name);
}

TransientDArray::~TransientDArray()
{
    grib_context_free_persistent(context_, name2_);
    grib_darray_delete(darray_);
}

int TransientDArray::execute(grib_handle* h)
{
    size_t len       = grib_darray_used_size(darray_);
    grib_accessor* a = NULL;
    grib_section* p  = h->root;

    a = grib_accessor_factory(p, this, len_, params_);
    if (!a)
        return GRIB_INTERNAL_ERROR;

    grib_push_accessor(a, p->block);

    if (a->flags_ & GRIB_ACCESSOR_FLAG_CONSTRAINT)
        grib_dependency_observe_arguments(a, default_value_);

    return a->pack_double(darray_->v, &len);
}

void TransientDArray::dump(FILE* f, int lvl)
{
    int i = 0;
    for (i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");
    grib_context_print(context_, f, name2_);
    printf("\n");
}

}  // namespace eccodes::action
