/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_bufr_data_element.h"

grib_accessor_bufr_data_element_t _grib_accessor_bufr_data_element{};
grib_accessor* grib_accessor_bufr_data_element = &_grib_accessor_bufr_data_element;

grib_accessor* grib_accessor_bufr_data_element_t::make_clone(grib_section* s, int* err)
{
    grib_accessor* the_clone = NULL;
    grib_accessor* attribute = NULL;
    grib_accessor_bufr_data_element_t* elementAccessor;
    char* copied_name = NULL;
    int i;
    grib_action creator;

    creator.op_         = (char*)"bufr_data_element";
    creator.name_space_ = (char*)"";
    creator.set_        = 0;
    creator.name_       = (char*)"unknown";
    if (strcmp(class_name_, "bufr_data_element")) {
        grib_context_log(context_, GRIB_LOG_FATAL, "wrong accessor type: '%s' should be '%s'", class_name_, "bufr_data_element");
    }
    *err = 0;

    the_clone                                  = grib_accessor_factory(s, &creator, 0, NULL);
    copied_name                                = grib_context_strdup(context_, name_);
    the_clone->name_                           = copied_name;
    elementAccessor                            = dynamic_cast<grib_accessor_bufr_data_element_t*>(the_clone);
    the_clone->flags_                          = flags_;
    the_clone->parent_                         = NULL;
    the_clone->h_                              = s->h;
    elementAccessor->index_                    = index_;
    elementAccessor->type_                     = type_;
    elementAccessor->numberOfSubsets_          = numberOfSubsets_;
    elementAccessor->subsetNumber_             = subsetNumber_;
    elementAccessor->compressedData_           = compressedData_;
    elementAccessor->descriptors_              = descriptors_;
    elementAccessor->numericValues_            = numericValues_;
    elementAccessor->stringValues_             = stringValues_;
    elementAccessor->elementsDescriptorsIndex_ = elementsDescriptorsIndex_;
    elementAccessor->cname_                    = copied_name; /* ECC-765 */

    i = 0;
    while (attributes_[i]) {
        attribute = attributes_[i]->clone(s, err);
        /* attribute->parent=parent_ ; */
        the_clone->add_attribute(attribute, 0);
        i++;
    }

    return the_clone;
}

void grib_accessor_bufr_data_element_t::init(const long len, grib_arguments* params)
{
    grib_accessor_gen_t::init(len, params);
    length_ = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_BUFR_DATA;
    /* flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY; */
    index_                    = 0;
    type_                     = 0;
    compressedData_           = 0;
    subsetNumber_             = 0;
    numberOfSubsets_          = 0;
    descriptors_              = NULL;
    numericValues_            = NULL;
    stringValues_             = NULL;
    elementsDescriptorsIndex_ = NULL;
    cname_                    = NULL;
}

void grib_accessor_bufr_data_element_t::dump(eccodes::Dumper* dumper)
{
    const int ntype = get_native_type();

    switch (ntype) {
        case GRIB_TYPE_LONG:
            dumper->dump_long(this, NULL);
            break;
        case GRIB_TYPE_DOUBLE:
            dumper->dump_values(this);
            break;
        case GRIB_TYPE_STRING:
            dumper->dump_string_array(this, NULL);
            break;
    }
}

int grib_accessor_bufr_data_element_t::unpack_string_array(char** val, size_t* len)
{
    int ret = 0, idx = 0;
    size_t count = 0, i = 0;
    grib_context* c = context_;

    if (compressedData_) {
        DEBUG_ASSERT(index_ < numericValues_->n);
        idx = ((int)numericValues_->v[index_]->v[0] / 1000 - 1) / numberOfSubsets_;
        DEBUG_ASSERT(idx < stringValues_->n);
        count = grib_sarray_used_size(stringValues_->v[idx]);
        for (i = 0; i < count; i++) {
            val[i] = grib_context_strdup(c, stringValues_->v[idx]->v[i]);
        }
        *len = count;
    }
    else {
        DEBUG_ASSERT(subsetNumber_ < numericValues_->n);
        DEBUG_ASSERT(index_ < numericValues_->v[subsetNumber_]->n);
        idx    = (int)numericValues_->v[subsetNumber_]->v[index_] / 1000 - 1;
        val[0] = grib_context_strdup(c, stringValues_->v[idx]->v[0]);
        *len   = 1;
    }

    return ret;
}

int grib_accessor_bufr_data_element_t::pack_string_array(const char** v, size_t* len)
{
    int ret = GRIB_SUCCESS, idx = 0;
    size_t i        = 0;
    char* s         = NULL;
    grib_context* c = context_;

    if (compressedData_) {
        idx = ((int)numericValues_->v[index_]->v[0] / 1000 - 1) / numberOfSubsets_;
        if (*len != 1 && *len != (size_t)numberOfSubsets_) {
            grib_context_log(c, GRIB_LOG_ERROR, "Number of values mismatch for '%s': %ld strings provided but expected %ld (=number of subsets)",
                             descriptors_->v[elementsDescriptorsIndex_->v[0]->v[idx]]->shortName, *len, numberOfSubsets_);
            return GRIB_ARRAY_TOO_SMALL;
        }
        grib_sarray_delete_content(stringValues_->v[idx]); /* ECC-1172 */
        grib_sarray_delete(stringValues_->v[idx]);
        stringValues_->v[idx] = grib_sarray_new(*len, 1);
        for (i = 0; i < *len; i++) {
            s = grib_context_strdup(c, v[i]);
            grib_sarray_push(stringValues_->v[idx], s);
        }
    }
    else {
        // ECC-1623
        if (*len != (size_t)numberOfSubsets_) {
            grib_context_log(c, GRIB_LOG_ERROR,
                             "Number of values mismatch for '%s': %zu strings provided but expected %ld (=number of subsets)",
                             name_, *len, numberOfSubsets_);
            return GRIB_WRONG_ARRAY_SIZE;
        }
        for (i = 0; i < *len; i++) {
            // idx = (int)numericValues->v[subsetNumber]->v[index_ ] / 1000 - 1;
            idx                         = (int)numericValues_->v[i]->v[index_] / 1000 - 1;
            stringValues_->v[idx]->v[0] = strdup(v[i]);
        }
        *len = 1;
    }

    return ret;
}

int grib_accessor_bufr_data_element_t::unpack_string(char* val, size_t* len)
{
    char* str   = NULL;
    char* p     = 0;
    size_t slen = 0;
    double dval = 0;
    size_t dlen = 1;
    int idx = 0, err = 0;
    grib_context* c = context_;

    if (type_ != BUFR_DESCRIPTOR_TYPE_STRING) {
        char sval[32] = {0,};
        err = unpack_double(&dval, &dlen);
        if (err) return err;
        snprintf(sval, sizeof(sval), "%g", dval);
        slen = strlen(sval);
        if (*len < slen)
            return GRIB_BUFFER_TOO_SMALL;
        strcpy(val, sval);
        return GRIB_SUCCESS;
    }

    if (compressedData_) {
        DEBUG_ASSERT(index_ < numericValues_->n);
        idx = ((int)numericValues_->v[index_]->v[0] / 1000 - 1) / numberOfSubsets_;
        if (idx < 0)
            return GRIB_INTERNAL_ERROR;
        str = grib_context_strdup(c, stringValues_->v[idx]->v[0]);
    }
    else {
        DEBUG_ASSERT(subsetNumber_ < numericValues_->n);
        idx = (int)numericValues_->v[subsetNumber_]->v[index_] / 1000 - 1;
        if (idx < 0)
            return GRIB_INTERNAL_ERROR;
        DEBUG_ASSERT(idx < stringValues_->n);
        str = grib_context_strdup(c, stringValues_->v[idx]->v[0]);
    }

    if (str == NULL || strlen(str) == 0) {
        grib_context_free(c, str);
        *len = 0;
        *val = 0;
        return GRIB_SUCCESS;
    }

    /* Start from the end of the string and remove spaces */
    p = str;
    while (*p != 0)
        p++;
    p--;
    while (p != str) {
        if (*p != ' ')
            break;
        else
            *p = 0;
        p--;
    }
    slen = strlen(str);
    if (slen > *len)
        return GRIB_ARRAY_TOO_SMALL;

    strcpy(val, str);
    grib_context_free(c, str);
    *len = slen;

    return GRIB_SUCCESS;
}

int grib_accessor_bufr_data_element_t::pack_string(const char* val, size_t* len)
{
    int ret = GRIB_SUCCESS, idx = 0;
    char* s         = NULL;
    grib_context* c = context_;

    if (compressedData_) {
        idx = ((int)numericValues_->v[index_]->v[0] / 1000 - 1) / numberOfSubsets_;
    }
    else {
        idx = (int)numericValues_->v[subsetNumber_]->v[index_] / 1000 - 1;
    }
    grib_sarray_delete_content(stringValues_->v[idx]); /* ECC-1172 */
    grib_sarray_delete(stringValues_->v[idx]);
    stringValues_->v[idx] = grib_sarray_new(1, 1);
    s                     = grib_context_strdup(c, val);
    grib_sarray_push(stringValues_->v[idx], s);

    return ret;
}

int grib_accessor_bufr_data_element_t::unpack_long(long* val, size_t* len)
{
    int ret    = GRIB_SUCCESS;
    long count = 0, i = 0;

    value_count(&count);

    if (*len < (size_t)count)
        return GRIB_ARRAY_TOO_SMALL;

    if (compressedData_) {
        for (i = 0; i < count; i++) {
            DEBUG_ASSERT(index_ < numericValues_->n);
            DEBUG_ASSERT(i < numericValues_->v[index_]->n);
            val[i] = numericValues_->v[index_]->v[i] == GRIB_MISSING_DOUBLE ? GRIB_MISSING_LONG : (long)numericValues_->v[index_]->v[i];
        }
        *len = count;
    }
    else {
        DEBUG_ASSERT(subsetNumber_ < numericValues_->n);
        DEBUG_ASSERT(index_ < numericValues_->v[subsetNumber_]->n);
        val[0] = numericValues_->v[subsetNumber_]->v[index_] == GRIB_MISSING_DOUBLE ? GRIB_MISSING_LONG : (long)numericValues_->v[subsetNumber_]->v[index_];
        *len   = 1;
    }

    return ret;
}

int grib_accessor_bufr_data_element_t::unpack_double(double* val, size_t* len)
{
    int ret    = GRIB_SUCCESS;
    long count = 0, i = 0;

    value_count(&count);

    if (*len < (size_t)count)
        return GRIB_ARRAY_TOO_SMALL;

    if (compressedData_) {
        for (i = 0; i < count; i++) {
            DEBUG_ASSERT(index_ < numericValues_->n);
            DEBUG_ASSERT(i < numericValues_->v[index_]->n);
            val[i] = numericValues_->v[index_]->v[i];
        }
        *len = count;
    }
    else {
        DEBUG_ASSERT(subsetNumber_ < numericValues_->n);
        DEBUG_ASSERT(index_ < numericValues_->v[subsetNumber_]->n);
        val[0] = numericValues_->v[subsetNumber_]->v[index_];
        *len   = 1;
    }

    return ret;
}

int grib_accessor_bufr_data_element_t::pack_double(const double* val, size_t* len)
{
    int ret      = GRIB_SUCCESS;
    size_t count = 1, i = 0;
    grib_context* c = context_;

    if (compressedData_) {
        count = *len;
        if (count != 1 && count != (size_t)numberOfSubsets_) {
            grib_context_log(c, GRIB_LOG_ERROR, "Number of values mismatch for '%s': %ld doubles provided but expected %ld (=number of subsets)",
                             descriptors_->v[elementsDescriptorsIndex_->v[0]->v[index_]]->shortName, count, numberOfSubsets_);
            return GRIB_ARRAY_TOO_SMALL;
        }
        grib_darray_delete(numericValues_->v[index_]);
        numericValues_->v[index_] = grib_darray_new(count, 1);

        for (i = 0; i < count; i++)
            grib_darray_push(numericValues_->v[index_], val[i]);

        *len = count;
    }
    else {
        numericValues_->v[subsetNumber_]->v[index_] = val[0];
        *len                                        = 1;
    }

    return ret;
}

int grib_accessor_bufr_data_element_t::pack_long(const long* val, size_t* len)
{
    int ret      = 0;
    size_t count = 1, i = 0;
    grib_context* c = context_;

    if (compressedData_) {
        count = *len;
        if (count != 1 && count != (size_t)numberOfSubsets_) {
            grib_context_log(c, GRIB_LOG_ERROR, "Number of values mismatch for '%s': %zu integers provided but expected %ld (=number of subsets)",
                             descriptors_->v[elementsDescriptorsIndex_->v[0]->v[index_]]->shortName, count, numberOfSubsets_);
            return GRIB_ARRAY_TOO_SMALL;
        }
        grib_darray_delete(numericValues_->v[index_]);
        numericValues_->v[index_] = grib_darray_new(count, 1);

        for (i = 0; i < count; i++) {
            grib_darray_push(numericValues_->v[index_], val[i] == GRIB_MISSING_LONG ? GRIB_MISSING_DOUBLE : val[i]);
        }
        *len = count;
    }
    else {
        numericValues_->v[subsetNumber_]->v[index_] = val[0] == GRIB_MISSING_LONG ? GRIB_MISSING_DOUBLE : val[0];
        *len                                        = 1;
    }

    return ret;
}

int grib_accessor_bufr_data_element_t::value_count(long* count)
{
    int ret = 0, idx = 0;
    size_t size = 0;

    if (!compressedData_) {
        *count = 1;
        return 0;
    }
    const int ntype = get_native_type();

    if (ntype == GRIB_TYPE_STRING) {
        DEBUG_ASSERT(index_ < numericValues_->n);
        idx  = ((int)numericValues_->v[index_]->v[0] / 1000 - 1) / numberOfSubsets_;
        size = grib_sarray_used_size(stringValues_->v[idx]);
    }
    else {
        DEBUG_ASSERT(index_ < numericValues_->n);
        size = grib_darray_used_size(numericValues_->v[index_]);
    }

    *count = size == 1 ? 1 : numberOfSubsets_;

    return ret;
}

int grib_accessor_bufr_data_element_t::unpack_double_element(size_t idx, double* val)
{
    /* ECC-415 */
    int ret    = GRIB_SUCCESS;
    long count = 0;

    value_count(&count);
    if (idx >= (size_t)count) {
        return GRIB_INTERNAL_ERROR;
    }

    if (compressedData_) {
        *val = numericValues_->v[index_]->v[idx];
    }
    else {
        ret = GRIB_NOT_IMPLEMENTED;
    }
    return ret;
}

long grib_accessor_bufr_data_element_t::get_native_type()
{
    int ret = GRIB_TYPE_DOUBLE;
    switch (type_) {
        case BUFR_DESCRIPTOR_TYPE_STRING:
            ret = GRIB_TYPE_STRING;
            break;
        case BUFR_DESCRIPTOR_TYPE_DOUBLE:
            ret = GRIB_TYPE_DOUBLE;
            break;
        case BUFR_DESCRIPTOR_TYPE_LONG:
            ret = GRIB_TYPE_LONG;
            break;
        case BUFR_DESCRIPTOR_TYPE_TABLE:
            ret = GRIB_TYPE_LONG;
            break;
        case BUFR_DESCRIPTOR_TYPE_FLAG:
            ret = GRIB_TYPE_LONG;
            break;
    }

    return ret;
}

void grib_accessor_bufr_data_element_t::destroy(grib_context* ct)
{
    int i = 0;
    if (cname_)
        grib_context_free(ct, cname_); /* ECC-765 */
    while (i < MAX_ACCESSOR_ATTRIBUTES && attributes_[i]) {
        /*grib_context_log(ct,GRIB_LOG_DEBUG,"deleting attribute %s->%s",a->name,attributes_ [i]->name);*/
        /*printf("bufr_data_element destroy %s %p\n", a->attributes_[i]->name, (void*)attributes_ [i]);*/
        attributes_[i]->destroy(ct);
        delete attributes_[i];
        attributes_[i] = NULL;
        i++;
    }
    grib_accessor_gen_t::destroy(ct);
}

#define MAX_STRING_SIZE 4096
/* Return 1 if BUFR element(s) is/are missing, 0 otherwise. In case of decoding errors, also return 0 */
int grib_accessor_bufr_data_element_t::is_missing()
{
    const int ktype = get_native_type();
    int err = 0, result = 1; /* default: assume all are missing */
    long count = 0;
    size_t i = 0, size = 1, size2 = 0;
    grib_context* c = context_;

    if (ktype == GRIB_TYPE_LONG) {
        long* values = NULL;
        long value   = 0;

        value_count(&count);
        size = size2 = count;
        if (size > 1) {
            values = (long*)grib_context_malloc_clear(c, sizeof(long) * size);
            err    = unpack_long(values, &size2);
        }
        else {
            err = unpack_long(&value, &size2);
        }
        if (err) return 0; /* TODO: no way of propagating the error up */
        ECCODES_ASSERT(size2 == size);
        if (size > 1) {
            for (i = 0; i < size; i++) {
                if (!grib_is_missing_long(this, values[i])) {
                    result = 0; /* at least one not missing */
                    break;
                }
            }
            grib_context_free(c, values);
        }
        else {
            result = grib_is_missing_long(this, value);
        }
    }
    else if (ktype == GRIB_TYPE_DOUBLE) {
        double value   = 0;
        double* values = NULL;

        value_count(&count);
        size = size2 = count;
        if (size > 1) {
            values = (double*)grib_context_malloc_clear(c, sizeof(double) * size);
            err    = unpack_double(values, &size2);
        }
        else {
            err = unpack_double(&value, &size2);
        }
        if (err) return 0; /* TODO: no way of propagating the error up */
        ECCODES_ASSERT(size2 == size);
        if (size > 1) {
            for (i = 0; i < size; ++i) {
                if (!grib_is_missing_double(this, values[i])) {
                    result = 0;
                    break;
                }
            }
            grib_context_free(c, values);
        }
        else {
            result = grib_is_missing_double(this, value);
        }
    }
    else if (ktype == GRIB_TYPE_STRING) {
        char** values = NULL;
        value_count(&count);
        size = count;
        if (size > 1) {
            values = (char**)grib_context_malloc_clear(context_, size * sizeof(char*));
            err    = unpack_string_array(values, &size);
            if (err) return 0; /* TODO: no way of propagating the error up */
            for (i = 0; i < size; i++) {
                if (!grib_is_missing_string(this, (unsigned char*)values[i], size)) {
                    result = 0;
                    break;
                }
            }
            for (i = 0; i < size; i++)
                grib_context_free(c, values[i]);
            grib_context_free(c, values);
        }
        else {
            char value[MAX_STRING_SIZE] = {0,}; /* See ECC-710 */
            size = MAX_STRING_SIZE;
            err  = unpack_string(value, &size);
            if (err) return 0; /* TODO: no way of propagating the error up */
            result = grib_is_missing_string(this, (unsigned char*)value, size);
        }
    }
    else {
        return GRIB_INVALID_TYPE;
    }
    return result;
}

int grib_accessor_bufr_data_element_t::pack_missing()
{
    int ktype                = GRIB_TYPE_UNDEFINED;
    int err                  = 0;
    size_t size              = 1;
    const int can_be_missing = (flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING);
    if (!can_be_missing)
        return GRIB_VALUE_CANNOT_BE_MISSING;

    ktype = get_native_type();
    if (ktype == GRIB_TYPE_LONG) {
        long missing = GRIB_MISSING_LONG;
        err          = pack_long(&missing, &size);
    }
    else if (ktype == GRIB_TYPE_DOUBLE) {
        double missing = GRIB_MISSING_DOUBLE;
        err            = pack_double(&missing, &size);
    }
    else if (ktype == GRIB_TYPE_STRING) {
        err = pack_string("", &size);
    }
    else {
        err = GRIB_INVALID_TYPE;
    }

    return err;
}
