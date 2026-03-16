/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

/**************************************************************************
 *  Jean Baptiste Filippi - 01.11.2005                                    *
 **************************************************************************/
#include "grib_api_internal.h"
#include "accessor/grib_accessor_class_bufr_data_array.h"

/* Note: A fast cut-down version of strcmp which does NOT return -1 */
/* 0 means input strings are equal and 1 means not equal */
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

static int matching(grib_accessor* a, const char* name, const char* name_space)
{
    int i = 0;
    while (i < MAX_ACCESSOR_NAMES) {
        if (a->all_names_[i] == 0)
            return 0;

        if ((grib_inline_strcmp(name, a->all_names_[i]) == 0) &&
            ((name_space == NULL) || (a->all_name_spaces_[i] != NULL &&
                                      grib_inline_strcmp(a->all_name_spaces_[i], name_space) == 0)))
            return 1;
        i++;
    }
    return 0;
}

static grib_accessor* search(grib_section* s, const char* name, const char* name_space)
{
    grib_accessor* match = NULL;

    grib_accessor* a = s ? s->block->first : NULL;
    grib_accessor* b = NULL;

    if (!a || !s)
        return NULL;

    while (a) {
        grib_section* sub = a->sub_section_;

        if (matching(a, name, name_space))
            match = a;

        if ((b = search(sub, name, name_space)) != NULL)
            match = b;

        a = a->next_;
    }

    return match;
}

static void rebuild_hash_keys(grib_handle* h, grib_section* s)
{
    grib_accessor* a = s ? s->block->first : NULL;

    while (a) {
        grib_section* sub = a->sub_section_;
        int i             = 0;
        int id            = -1;
        const char* p;
        DEBUG_ASSERT(h == grib_handle_of_accessor(a));

        while (i < MAX_ACCESSOR_NAMES && ((p = a->all_names_[i]) != NULL)) {
            if (*p != '_') {
                id = grib_hash_keys_get_id(a->context_->keys, p);

                if (a->same_ != a && i == 0) {
                    grib_handle* hand   = grib_handle_of_accessor(a);
                    a->same_             = hand->accessors[id];
                    hand->accessors[id] = a;
                    DEBUG_ASSERT(a->same_ != a);
                }
            }
            i++;
        }
        rebuild_hash_keys(h, sub);
        a = a->next_;
    }
}

static grib_accessor* search_and_cache(grib_handle* h, const char* name, const char* the_namespace);

static grib_accessor* _search_and_cache(grib_handle* h, const char* name, const char* the_namespace)
{
    if (h->use_trie) {
        grib_accessor* a = NULL;
        int id           = -1;

        if (h->trie_invalid && h->kid == NULL) {
            int i = 0;
            for (i = 0; i < ACCESSORS_ARRAY_SIZE; i++)
                h->accessors[i] = NULL;

            if (h->root)
                rebuild_hash_keys(h, h->root);

            h->trie_invalid = 0;
            id              = grib_hash_keys_get_id(h->context->keys, name);
        }
        else {
            id = grib_hash_keys_get_id(h->context->keys, name);

            if ((a = h->accessors[id]) != NULL &&
                (the_namespace == NULL || matching(a, name, the_namespace)))
                return a;
        }

        a                = search(h->root, name, the_namespace);
        h->accessors[id] = a;

        return a;
    }
    else {
        return search(h->root, name, the_namespace);
    }
}

static char* get_rank(grib_context* c, const char* name, int* rank)
{
    char* p   = (char*)name;
    char* end = p;
    char* ret = NULL;

    *rank = -1;

    if (*p == '#') {
        *rank = strtol(++p, &end, 10);
        if (*end != '#') {
            *rank = -1;
        }
        else {
            DEBUG_ASSERT(c);
            end++;
            ret = grib_context_strdup(c, end);
        }
    }
    return ret;
}

static char* get_condition(const char* name, codes_condition* condition)
{
    char* equal        = (char*)name;
    char* endCondition = NULL;
    char* str          = NULL;
    char* end          = NULL;
    long lval;
    grib_context* c = grib_context_get_default();

    condition->rightType = GRIB_TYPE_UNDEFINED;

    DEBUG_ASSERT(name[0] == '/');

    while (*equal != 0 && *equal != '=')
        equal++;
    if (*equal == 0)
        return NULL;

    endCondition = equal;
    while (*endCondition != 0 && *endCondition != '/')
        endCondition++;
    if (*endCondition == 0)
        return NULL;

    str = (char*)grib_context_malloc_clear(c, strlen(name));
    memcpy(str, equal + 1, endCondition - equal - 1);

    end  = NULL;
    lval = strtol(str, &end, 10);
    if (*end != 0) { /* strtol failed. Not an integer */
        double dval;
        dval = strtod(str, &end);
        if (*end == 0) { /* strtod passed. So a double */
            condition->rightType   = GRIB_TYPE_DOUBLE;
            condition->rightDouble = dval;
        }
    }
    else {
        condition->rightType = GRIB_TYPE_LONG;
        condition->rightLong = lval;
    }

    if (condition->rightType != GRIB_TYPE_UNDEFINED) {
        strcpy(str, endCondition + 1);
        condition->left = (char*)grib_context_malloc_clear(c, equal - name);
        memcpy(condition->left, name + 1, equal - name - 1);
    }
    else {
        grib_context_free(c, str);
        str = NULL;
    }
    return str;
}

static grib_accessor* _search_by_rank(grib_accessor* a, const char* name, int rank)
{
    grib_accessor_bufr_data_array_t* data_accessor = dynamic_cast<grib_accessor_bufr_data_array_t*>(a);
    grib_trie_with_rank* t = data_accessor->accessor_bufr_data_array_get_dataAccessorsTrie();
    grib_accessor* ret = (grib_accessor*)grib_trie_with_rank_get(t, name, rank);
    return ret;
}

/*
static grib_accessor* _search_by_rank(grib_accessor* a,const char* name,long rank) {
    long r=1;
    grib_accessors_list* al=accessor_bufr_data_array_get_dataAccessors(a);

    while (al) {
        if (!grib_inline_strcmp(al->accessor->name_,name)) {
            if (r==rank) return al->accessor;
            r++;
        }
        al=al->next_;
    }

    return NULL;
}
*/

static grib_accessor* search_by_rank(grib_handle* h, const char* name, int rank, const char* the_namespace)
{
    grib_accessor* data = search_and_cache(h, "dataAccessors", the_namespace);
    if (data) {
        return _search_by_rank(data, name, rank);
    }
    else {
        int rank2;
        char* str          = get_rank(h->context, name, &rank2);
        grib_accessor* ret = _search_and_cache(h, str, the_namespace);
        grib_context_free(h->context, str);
        return ret;
    }
}

static int get_single_long_val(grib_accessor* a, long* result)
{
    grib_context* c = a->context_;
    int err         = 0;
    size_t size     = 1;
    if (c->bufr_multi_element_constant_arrays) {
        long count = 0;
        a->value_count(&count);
        if (count > 1) {
            size_t i        = 0;
            long val0       = 0;
            int is_constant = 1;
            long* values    = (long*)grib_context_malloc_clear(c, sizeof(long) * count);
            size            = count;
            err             = a->unpack_long(values, &size);
            val0            = values[0];
            for (i = 0; i < size; i++) {
                if (val0 != values[i]) {
                    is_constant = 0;
                    break;
                }
            }
            if (is_constant) {
                *result = val0;
                grib_context_free(c, values);
            }
            else {
                return GRIB_ARRAY_TOO_SMALL;
            }
        }
        else {
            err = a->unpack_long(result, &size);
        }
    }
    else {
        err = a->unpack_long(result, &size);
    }
    return err;
}
static int get_single_double_val(grib_accessor* a, double* result)
{
    grib_context* c = a->context_;
    int err         = 0;
    size_t size     = 1;
    if (c->bufr_multi_element_constant_arrays) {
        long count = 0;
        a->value_count(&count);
        if (count > 1) {
            size_t i        = 0;
            double val0     = 0;
            int is_constant = 1;
            double* values  = (double*)grib_context_malloc_clear(c, sizeof(double) * count);
            size            = count;
            err             = a->unpack_double(values, &size);
            val0            = values[0];
            for (i = 0; i < size; i++) {
                if (val0 != values[i]) {
                    is_constant = 0;
                    break;
                }
            }
            if (is_constant) {
                *result = val0;
                grib_context_free(c, values);
            }
            else {
                return GRIB_ARRAY_TOO_SMALL;
            }
        }
        else {
            err = a->unpack_double(result, &size);
        }
    }
    else {
        err = a->unpack_double(result, &size);
    }
    return err;
}

static int condition_true(grib_accessor* a, codes_condition* condition)
{
    int ret = 0, err = 0;
    long lval   = 0;
    double dval = 0;

    /* The condition has to be of the form:
     *   key=value
     * and the value has to be a single scalar (integer or double).
     * If the key is an array of different values, then the condition is false.
     * But if the key is a constant array and the value matches then it's true.
     */

    switch (condition->rightType) {
        case GRIB_TYPE_LONG:
            err = get_single_long_val(a, &lval);
            if (err)
                ret = 0;
            else
                ret = lval == condition->rightLong ? 1 : 0;
            break;
        case GRIB_TYPE_DOUBLE:
            err = get_single_double_val(a, &dval);
            if (err)
                ret = 0;
            else
                ret = dval == condition->rightDouble ? 1 : 0;
            break;
        default:
            ret = 0;
            break;
    }
    return ret;
}

static void search_from_accessors_list(grib_accessors_list* al, const grib_accessors_list* end, const char* name, grib_accessors_list* result)
{
    char attribute_name[200] = {0,};
    grib_accessor* accessor_result = 0;
    grib_context* c = al->accessor->context_;
    int doFree = 1;

    char* accessor_name = grib_split_name_attribute(c, name, attribute_name);
    if (*attribute_name == 0) doFree = 0;

    while (al && al != end && al->accessor) {
        if (grib_inline_strcmp(al->accessor->name_, accessor_name) == 0) {
            if (attribute_name[0]) {
                accessor_result = al->accessor->get_attribute(attribute_name);
            }
            else {
                accessor_result = al->accessor;
            }
            if (accessor_result) {
                result->push(accessor_result, al->rank());
            }
        }
        al = al->next_;
    }
    if (al == end && al->accessor) {
        if (grib_inline_strcmp(al->accessor->name_, accessor_name) == 0) {
            if (attribute_name[0]) {
                accessor_result = al->accessor->get_attribute(attribute_name);
            }
            else {
                accessor_result = al->accessor;
            }
            if (accessor_result) {
                result->push(accessor_result, al->rank());
            }
        }
    }
    if (doFree) grib_context_free(c, accessor_name);
}

static void search_accessors_list_by_condition(grib_accessors_list* al, const char* name, codes_condition* condition, grib_accessors_list* result)
{
    grib_accessors_list* start = NULL;
    grib_accessors_list* end   = NULL;

    while (al) {
        if (!grib_inline_strcmp(al->accessor->name_, condition->left)) {
            if (start == NULL && condition_true(al->accessor, condition))
                start = al;
            if (start && !condition_true(al->accessor, condition))
                end = al;
        }
        if (start != NULL && (end != NULL || al->next_ == NULL)) {
            if (end == NULL)
                end = al;
            search_from_accessors_list(start, end, name, result);
            al    = end;
            start = NULL;
            end   = NULL;
        }
        al = al->next_;
    }
}

static grib_accessors_list* search_by_condition(grib_handle* h, const char* name, codes_condition* condition)
{
    grib_accessors_list* al;
    grib_accessors_list* result = NULL;
    grib_accessor* a = search_and_cache(h, "dataAccessors", 0);
    grib_accessor_bufr_data_array_t* data_accessor = dynamic_cast<grib_accessor_bufr_data_array_t*>(a);
    if (data_accessor && condition->left) {
        al = data_accessor->accessor_bufr_data_array_get_dataAccessors();
        if (!al)
            return NULL;
        result = (grib_accessors_list*)grib_context_malloc_clear(al->accessor->context_, sizeof(grib_accessors_list));
        search_accessors_list_by_condition(al, name, condition, result);
        if (!result->accessor) {
            grib_accessors_list_delete(h->context, result);
            result = NULL;
        }
    }

    return result;
}

static void grib_find_same_and_push(grib_accessors_list* al, grib_accessor* a)
{
    if (a) {
        grib_find_same_and_push(al, a->same_);
        al->push(a, al->rank());
    }
}

grib_accessors_list* grib_find_accessors_list(const grib_handle* ch, const char* name)
{
    char* str                  = NULL;
    grib_accessors_list* al    = NULL;
    codes_condition* condition = NULL;
    grib_accessor* a           = NULL;
    grib_handle* h             = (grib_handle*)ch;

    if (name[0] == '/') {
        condition = (codes_condition*)grib_context_malloc_clear(h->context, sizeof(codes_condition));
        str       = get_condition(name, condition);
        if (str) {
            al = search_by_condition(h, str, condition);
            grib_context_free(h->context, str);
            if (condition->left)
                grib_context_free(h->context, condition->left);
            if (condition->rightString)
                grib_context_free(h->context, condition->rightString);
        }
        grib_context_free(h->context, condition);
    }
    else if (name[0] == '#') {
        a = grib_find_accessor(h, name);
        if (a) {
            char* str2;
            int r;
            al   = (grib_accessors_list*)grib_context_malloc_clear(h->context, sizeof(grib_accessors_list));
            str2 = get_rank(h->context, name, &r);
            al->push(a, r);
            grib_context_free(h->context, str2);
        }
    }
    else {
        a = grib_find_accessor(h, name);
        if (a) {
            al = (grib_accessors_list*)grib_context_malloc_clear(h->context, sizeof(grib_accessors_list));
            grib_find_same_and_push(al, a);
        }
    }

    return al;
}

static grib_accessor* search_and_cache(grib_handle* h, const char* name, const char* the_namespace)
{
    grib_accessor* a = NULL;

    if (name[0] == '#') {
        int rank       = -1;
        char* basename = get_rank(h->context, name, &rank);
        a              = search_by_rank(h, basename, rank, the_namespace);
        grib_context_free(h->context, basename);
    }
    else {
        a = _search_and_cache(h, name, the_namespace);
    }

    return a;
}

static grib_accessor* _grib_find_accessor(const grib_handle* ch, const char* name)
{
    grib_handle* h   = (grib_handle*)ch;
    grib_accessor* a = NULL;
    char* p          = NULL;
    DEBUG_ASSERT(name);

    p = strchr((char*)name, '.');
    if (p) {
        int i = 0, len = 0;
        char name_space[MAX_NAMESPACE_LEN];
        char* basename = p + 1;
        p--;
        len = p - name + 1;

        for (i = 0; i < len; i++)
            name_space[i] = *(name + i);

        name_space[len] = '\0';

        a = search_and_cache(h, basename, name_space);
    }
    else {
        a = search_and_cache(h, name, NULL);
    }

    if (a == NULL && h->main)
        a = grib_find_accessor(h->main, name);

    return a;
}

char* grib_split_name_attribute(grib_context* c, const char* name, char* attribute_name)
{
    /*returns accessor name and attribute*/
    size_t size         = 0;
    char* accessor_name = NULL;
    char* p             = strstr((char*)name, "->");
    if (!p) {
        *attribute_name = 0;
        return (char*)name;
    }
    size          = p - name;
    accessor_name = (char*)grib_context_malloc_clear(c, size + 1);
    accessor_name = (char*)memcpy(accessor_name, name, size);
    p += 2;
    strcpy(attribute_name, p);
    return accessor_name;
}

grib_accessor* grib_find_accessor(const grib_handle* h, const char* name)
{
    grib_accessor* aret = NULL;
    DEBUG_ASSERT(h);
    if (h->product_kind == PRODUCT_GRIB) {
        aret = _grib_find_accessor(h, name); /* ECC-144: Performance */
    }
    else {
        char attribute_name[512] = {0,};
        grib_accessor* a = NULL;

        char* accessor_name = grib_split_name_attribute(h->context, name, attribute_name);

        a = _grib_find_accessor(h, accessor_name);

        if (*attribute_name == 0) {
            aret = a;
        }
        else if (a) {
            aret = a->get_attribute(attribute_name);
            grib_context_free(h->context, accessor_name);
        }
    }
    return aret;
}

// grib_accessor* grib_find_attribute(grib_handle* h, const char* name, const char* attr_name, int* err)
// {
//     grib_accessor* a   = NULL;
//     grib_accessor* act = NULL;
//     if ((a = grib_find_accessor(h, name)) == NULL) {
//         *err = GRIB_NOT_FOUND;
//         return NULL;
//     }
//     if ((act = a->get_attribute(attr_name)) == NULL) {
//         *err = GRIB_ATTRIBUTE_NOT_FOUND;
//         return NULL;
//     }
//     return act;
// }

/* Only look in trie. Used only in alias. Should not be used in other cases.*/
grib_accessor* grib_find_accessor_fast(grib_handle* h, const char* name)
{
    grib_accessor* a = NULL;
    char* p          = NULL;
    DEBUG_ASSERT(name);

    p = strchr((char*)name, '.');
    if (p) {
        int i = 0, len = 0;
        char name_space[MAX_NAMESPACE_LEN];
        p--;
        len = p - name + 1;

        for (i = 0; i < len; i++)
            name_space[i] = *(name + i);

        name_space[len] = '\0';

        a = h->accessors[grib_hash_keys_get_id(h->context->keys, name)];
        if (a && !matching(a, name, name_space))
            a = NULL;
    }
    else {
        a = h->accessors[grib_hash_keys_get_id(h->context->keys, name)];
    }

    if (a == NULL && h->main)
        a = grib_find_accessor_fast(h->main, name);

    return a;
}
