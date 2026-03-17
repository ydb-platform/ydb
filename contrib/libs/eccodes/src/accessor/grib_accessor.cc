/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

/***************************************************************************
 *   Jean Baptiste Filippi - 01.11.2005
 ***************************************************************************/

#include "grib_accessor.h"

// Note: A fast cut-down version of strcmp which does NOT return -1
// 0 means input strings are equal and 1 means not equal
GRIB_INLINE static int grib_inline_strcmp(const char* a, const char* b)
{
    if (*a != *b)
        return 1;
    while ((*a != 0 && *b != 0) && *(a) == *(b)) {
        a++;
        b++;
    }
    return (*a == 0 && *b == 0) ? 0 : 1;
}

int grib_accessor::compare_accessors(grib_accessor* a2, int compare_flags)
{
    int ret           = 0;
    long type1        = 0;
    long type2        = 0;
    int type_mismatch = 0;
    grib_accessor* a1 = this;

    if ((compare_flags & GRIB_COMPARE_NAMES) && grib_inline_strcmp(a1->name_, a2->name_))
        return GRIB_NAME_MISMATCH;

    if (compare_flags & GRIB_COMPARE_TYPES) {
        type1 = a1->get_native_type();
        type2 = a2->get_native_type();

        type_mismatch = type1 != type2 ? 1 : 0;
    }

    // ret = GRIB_UNABLE_TO_COMPARE_ACCESSORS;
    ret = compare(a2);

    if (ret == GRIB_VALUE_MISMATCH && type_mismatch)
        ret = GRIB_TYPE_AND_VALUE_MISMATCH;

    return ret;
}

int grib_accessor::add_attribute(grib_accessor* attr, int nest_if_clash)
{
    int id               = 0;
    int idx              = 0;
    grib_accessor* pSame = NULL;
    grib_accessor* pAloc = this;

    if (this->has_attributes()) {
        pSame = this->get_attribute_index(attr->name_, &id);
    }

    if (pSame) {
        if (nest_if_clash == 0)
            return GRIB_ATTRIBUTE_CLASH;
        pAloc = pSame;
    }

    for (id = 0; id < MAX_ACCESSOR_ATTRIBUTES; id++) {
        if (pAloc->attributes_[id] == NULL) {
            // attr->parent=a->parent;
            pAloc->attributes_[id]     = attr;
            attr->parent_as_attribute_ = pAloc;
            if (pAloc->same_)
                attr->same_ = pAloc->same_->get_attribute_index(attr->name_, &idx);

            grib_context_log(this->context_, GRIB_LOG_DEBUG, "added attribute %s->%s", this->name_, attr->name_);
            return GRIB_SUCCESS;
        }
    }
    return GRIB_TOO_MANY_ATTRIBUTES;
}

grib_accessor* grib_accessor::get_attribute_index(const char* name_, int* index)
{
    int i = 0;
    while (i < MAX_ACCESSOR_ATTRIBUTES && this->attributes_[i]) {
        if (!grib_inline_strcmp(this->attributes_[i]->name_, name_)) {
            *index = i;
            return this->attributes_[i];
        }
        i++;
    }
    return NULL;
}

int grib_accessor::has_attributes()
{
    return this->attributes_[0] ? 1 : 0;
}

grib_accessor* grib_accessor::get_attribute(const char* name_)
{
    int index                  = 0;
    const char* p              = 0;
    char* basename             = NULL;
    const char* attribute_name = NULL;
    grib_accessor* acc         = NULL;
    p                          = name_;
    while (*(p + 1) != '\0' && (*p != '-' || *(p + 1) != '>'))
        p++;
    if (*(p + 1) == '\0') {
        return this->get_attribute_index(name_, &index);
    }
    else {
        size_t size    = p - name_;
        attribute_name = p + 2;
        basename       = (char*)grib_context_malloc_clear(this->context_, size + 1);
        basename       = (char*)memcpy(basename, name_, size);
        acc            = this->get_attribute_index(basename, &index);
        grib_context_free(this->context_, basename);
        if (acc)
            return acc->get_attribute(attribute_name);
        else
            return NULL;
    }
}
