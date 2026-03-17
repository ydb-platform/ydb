/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"

grib_concept_value* grib_concept_value_new(grib_context* c, const char* name, grib_concept_condition* conditions)
{
    grib_concept_value* v = (grib_concept_value*)grib_context_malloc_clear_persistent(c, sizeof(grib_concept_value));
    v->name               = grib_context_strdup_persistent(c, name);
    v->conditions         = conditions;
    return v;
}

void grib_concept_value_delete(grib_context* c, grib_concept_value* v)
{
    grib_concept_condition* e = v->conditions;
    while (e) {
        grib_concept_condition* n = e->next;
        grib_concept_condition_delete(c, e);
        e = n;
    }
    grib_context_free_persistent(c, v->name);
    grib_context_free_persistent(c, v);
}

grib_concept_condition* grib_concept_condition_new(grib_context* c, const char* name, grib_expression* expression, grib_iarray* iarray)
{
    grib_concept_condition* v = (grib_concept_condition*)grib_context_malloc_clear_persistent(c, sizeof(grib_concept_condition));
    v->name                   = grib_context_strdup_persistent(c, name);
    v->expression             = expression;
    v->iarray                 = iarray;
    return v;
}

void grib_concept_condition_delete(grib_context* c, grib_concept_condition* v)
{
    v->expression->destroy(c);
    delete v->expression;
    grib_context_free_persistent(c, v->name);
    grib_context_free_persistent(c, v);
}
