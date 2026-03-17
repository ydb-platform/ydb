/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_concept.h"
#include "action_class_concept.h"
#include <unordered_map>
#include <utility>
#include <map>

grib_accessor_concept_t _grib_accessor_concept{};
grib_accessor* grib_accessor_concept = &_grib_accessor_concept;

#define MAX_CONCEPT_STRING_LENGTH 255

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

void grib_accessor_concept_t::init(const long len, grib_arguments* args)
{
    grib_accessor_gen_t::init(len, args);
    length_ = 0;
}

void grib_accessor_concept_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

// See ECC-1905
static int grib_get_long_memoize(
    grib_handle* h, const char* key, long* value,
    std::unordered_map<std::string_view, long>& memo)
{
    int err = 0;
    auto pos = memo.find(key);
    if (pos == memo.end()) { // not in map so decode & insert
        err = grib_get_long(h, key, value);
        if (!err) {
            memo.insert(std::make_pair(key, *value));
        }
    } else {
        *value = pos->second; // found in map
    }
    return err;
}

// Return 1 (=True) or 0 (=False)
static int concept_condition_expression_true(
    grib_handle* h, grib_concept_condition* c,
    std::unordered_map<std::string_view, long>& memo)
{
    long lval;
    long lres      = 0;
    int ok         = 0; // Boolean
    int err        = 0;
    const int type = c->expression->native_type(h);

    switch (type) {
        case GRIB_TYPE_LONG:
            c->expression->evaluate_long(h, &lres);
            // Use memoization for the most common type (integer keys)
            ok = (grib_get_long_memoize(h, c->name, &lval, memo) == GRIB_SUCCESS) &&
                 (lval == lres);
            //ok = (grib_get_long(h, c->name, &lval) == GRIB_SUCCESS) && (lval == lres);
            break;

        case GRIB_TYPE_DOUBLE: {
            double dval;
            double dres = 0.0;
            c->expression->evaluate_double(h, &dres);
            ok = (grib_get_double(h, c->name, &dval) == GRIB_SUCCESS) &&
                 (dval == dres);
            break;
        }

        case GRIB_TYPE_STRING: {
            const char* cval;
            char buf[80];
            char tmp[80];
            size_t len  = sizeof(buf);
            size_t size = sizeof(tmp);

            ok = (grib_get_string(h, c->name, buf, &len) == GRIB_SUCCESS) &&
                 ((cval = c->expression->evaluate_string(h, tmp, &size, &err)) != NULL) &&
                 (err == 0) && (grib_inline_strcmp(buf, cval) == 0);
            break;
        }

        default:
            // TODO
            break;
    }
    return ok;
}

// Return 0 (=no match) or >0 which is the count of matches
// See ECC-1992
static int concept_condition_iarray_true_count(grib_handle* h, grib_concept_condition* c)
{
    size_t size = 0;
    int ret = 0; // count of matches

    int err = grib_get_size(h, c->name, &size);
    if (err || size != grib_iarray_used_size(c->iarray))
        return 0; // no match

    long* val = (long*)grib_context_malloc_clear(h->context, sizeof(long) * size);
    if (!val) return 0;

    err = grib_get_long_array(h, c->name, val, &size);
    if (err) {
        grib_context_free(h->context, val);
        return 0; // no match
    }

    ret = (int)size; // Assume all array entries match
    for (size_t i = 0; i < size; i++) {
        if (val[i] != c->iarray->v[i]) {
            ret = 0; // no match
            break;
        }
    }

    grib_context_free(h->context, val);
    return ret;
}

// Return 0 (=no match) or >0 (=match count)
static int concept_condition_true_count(
    grib_handle* h, grib_concept_condition* c,
    std::unordered_map<std::string_view, long>& memo)
{
    if (c->expression == NULL)
        return concept_condition_iarray_true_count(h, c);
    else
        return concept_condition_expression_true(h, c, memo);
}

static const char* concept_evaluate(grib_accessor* a)
{
    int match        = 0;
    const char* best = 0;
    //const char* prev = 0;
    grib_concept_value* c = action_concept_get_concept(a);
    grib_handle* h        = grib_handle_of_accessor(a);

    std::unordered_map<std::string_view, long> memo; // See ECC-1905

    while (c) {
        // printf("DEBUG: %s concept=%s while loop c->name=%s\n", __func__, a->name_, c->name);
        grib_concept_condition* e = c->conditions;
        int cnt = 0;
        while (e) {
            const int cc_count = concept_condition_true_count(h, e, memo);
            if (cc_count == 0) // match failed
                break;
            e = e->next;
            cnt += cc_count; // ECC-1992
        }

        if (e == NULL) {
            if (cnt >= match) {
                // prev  = (cnt > match) ? NULL : best;
                // A better candidate was found. Update 'match' and 'best'
                match = cnt;
                best  = c->name;
                // printf("DEBUG: %s concept=%s current best=%s\n", __func__, a->name_, best);
            }
        }

        c = c->next;
    }

    // printf("DEBUG: %s concept=%s final best=%s\n", __func__, a->name_, best);
    return best;
}

// Note: This function does not change the handle, but stores the results in the "values" array to be applied later
static int concept_conditions_expression_apply(grib_handle* h, grib_concept_condition* e, grib_values* values, grib_sarray* sa, int* n)
{
    long lres   = 0;
    double dres = 0.0;
    int count   = *n;
    size_t size;
    int err = 0;

    ECCODES_ASSERT(count < 1024);
    values[count].name = e->name;

    values[count].type = e->expression->native_type(h);
    switch (values[count].type) {
        case GRIB_TYPE_LONG:
            e->expression->evaluate_long(h, &lres);
            values[count].long_value = lres;
            break;
        case GRIB_TYPE_DOUBLE:
            e->expression->evaluate_double(h, &dres);
            values[count].double_value = dres;
            break;
        case GRIB_TYPE_STRING:
            size                       = sizeof(sa->v[count]);
            values[count].string_value = e->expression->evaluate_string(h, sa->v[count], &size, &err);
            break;
        default:
            return GRIB_NOT_IMPLEMENTED;
    }
    (*n)++;
    return err;
}

static int rectify_concept_apply(grib_handle* h, const char* key)
{
    // The key was not found. In specific cases, rectify the problem by setting
    // a secondary key
    // e.g.,
    // GRIB is instantaneous but paramId being set is for accum/avg
    //
    int ret = GRIB_NOT_FOUND;
    static const std::map<std::string_view, std::pair<std::string_view, long>> keyMap = {
        { "typeOfStatisticalProcessing", { "selectStepTemplateInterval", 1 } },
        { "typeOfWavePeriodInterval", { "productDefinitionTemplateNumber", 103 } },
        { "sourceSinkChemicalPhysicalProcess", { "is_chemical_srcsink", 1 } },
        // TODO(masn): Add a new key e.g. is_probability_forecast
        { "probabilityType", { "productDefinitionTemplateNumber", 5 } }
    };
    const auto mapIter = keyMap.find(key);
    if (mapIter != keyMap.end()) {
        const char* key2 = mapIter->second.first.data();
        const long val2  = mapIter->second.second;
        grib_context_log(h->context, GRIB_LOG_DEBUG, "Concept: Key %s not found, setting %s to %ld", key, key2, val2);
        ret = grib_set_long(h, key2, val2);
    }
    return ret;
}

// Note: This applies the changes to the handle passed in
static int concept_conditions_iarray_apply(grib_handle* h, grib_concept_condition* c)
{
    const size_t size = grib_iarray_used_size(c->iarray);
    int err = grib_set_long_array(h, c->name, c->iarray->v, size);
    // ECC-1992: Special case for GRIB2
    // When typeOfStatisticalProcessing is an array, must also set numberOfTimeRanges
    if ((err == GRIB_NOT_FOUND || err == GRIB_ARRAY_TOO_SMALL) &&
        STR_EQUAL(c->name, "typeOfStatisticalProcessing"))
    {
        grib_context_log(h->context, GRIB_LOG_DEBUG, "Concept: Key %s not found, setting PDTN", c->name);
        if (grib_set_long(h, "selectStepTemplateInterval", 1) == GRIB_SUCCESS) {
            // Now should have the correct PDTN
            err = grib_set_long(h, "numberOfTimeRanges", (long)size);
            if (!err) {
                err = grib_set_long_array(h, c->name, c->iarray->v, size);  //re-apply
            }
        }
    }
    return err;
}

static int concept_conditions_apply(grib_handle* h, grib_concept_condition* c, grib_values* values, grib_sarray* sa, int* n)
{
    if (c->expression == NULL)
        return concept_conditions_iarray_apply(h, c);
    else
        return concept_conditions_expression_apply(h, c, values, sa, n);
}

static int cmpstringp(const void* p1, const void* p2)
{
    // The actual arguments to this function are "pointers to
    // pointers to char", but strcmp(3) arguments are "pointers
    // to char", hence the following cast plus dereference */
    return strcmp(*(char* const*)p1, *(char* const*)p2);
}

static bool blacklisted(grib_handle* h, long edition, const char* concept_name, const char* concept_value)
{
    if (strcmp(concept_name, "packingType") == 0) {
        char input_packing_type[100];
        size_t len = sizeof(input_packing_type);
        if (strstr(concept_value, "SPD")) {
            return true;
        }
        if (edition == 2 && strstr(concept_value, "grid_run_length")) {
            return true;
        }
        if (strstr(concept_value, "grid_simple_matrix")) {
            return true;
        }
        if (edition == 1 && (strstr(concept_value, "ccsds") || strstr(concept_value, "jpeg"))) {
            return true;
        }
        grib_get_string(h, "packingType", input_packing_type, &len);
        if (strstr(input_packing_type, "grid_") && !strstr(concept_value, "grid_")) {
            return true;
        }
        if (strstr(input_packing_type, "spectral_") && !strstr(concept_value, "spectral_")) {
            return true;
        }
    }
    return false;
}

#define MAX_NUM_CONCEPT_VALUES 40

static void print_user_friendly_message(grib_handle* h, const char* name, grib_concept_value* concepts, grib_action* act)
{
    size_t i = 0, concept_count = 0;
    long dummy = 0, editionNumber = 0;
    char centre_s[32] = {0,};
    size_t centre_len = sizeof(centre_s);
    char* all_concept_vals[MAX_NUM_CONCEPT_VALUES] = {NULL,}; // sorted array containing concept values
    grib_concept_value* pCon = concepts;

    grib_context_log(h->context, GRIB_LOG_ERROR, "concept: no match for %s=%s", act->name_, name);
    if (grib_get_long(h, "edition", &editionNumber) == GRIB_SUCCESS &&
        grib_get_string(h, "centre", centre_s, &centre_len) == GRIB_SUCCESS) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "concept: input handle edition=%ld, centre=%s", editionNumber, centre_s);
    }
    char dataset_s[80];
    size_t dataset_len = sizeof(dataset_s);
    if (grib_get_string(h, "datasetForLocal", dataset_s, &dataset_len) == GRIB_SUCCESS && !STR_EQUAL(dataset_s, "unknown")) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "concept: input handle dataset=%s", dataset_s);
    }
    if (strcmp(act->name_, "paramId") == 0) {
        if (string_to_long(name, &dummy, 1) == GRIB_SUCCESS) {
            // The paramId value is an integer. Show them the param DB
            grib_context_log(h->context, GRIB_LOG_ERROR,
                             "Please check the Parameter Database 'https://codes.ecmwf.int/grib/param-db/?id=%s'", name);
        }
        else {
            // paramId being set to a non-integer
            grib_context_log(h->context, GRIB_LOG_ERROR,
                             "The paramId value should be an integer. Are you trying to set the shortName?");
        }
    }
    if (strcmp(act->name_, "shortName") == 0) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Please check the Parameter Database 'https://codes.ecmwf.int/grib/param-db/'");
    }

    // Create a list of all possible values for this concept and sort it
    while (pCon) {
        if (i >= MAX_NUM_CONCEPT_VALUES)
            break;
        all_concept_vals[i++] = pCon->name;
        pCon                  = pCon->next;
    }
    concept_count = i;
    // Only print out all concepts if fewer than MAX_NUM_CONCEPT_VALUES.
    // Printing out all values for concepts like paramId would be silly!
    if (concept_count < MAX_NUM_CONCEPT_VALUES) {
        fprintf(stderr, "Here are some possible values for concept %s:\n", act->name_);
        qsort(&all_concept_vals, concept_count, sizeof(char*), cmpstringp);
        for (i = 0; i < concept_count; ++i) {
            if (all_concept_vals[i]) {
                bool print_it = true;
                if (i > 0 && all_concept_vals[i - 1] && strcmp(all_concept_vals[i], all_concept_vals[i - 1]) == 0) {
                    print_it = false; // skip duplicate entries
                }
                if (blacklisted(h, editionNumber, act->name_, all_concept_vals[i])) {
                    print_it = false;
                }
                if (print_it) {
                    fprintf(stderr, "\t%s\n", all_concept_vals[i]);
                }
            }
        }
    }
}

static int grib_concept_apply(grib_accessor* a, const char* name)
{
    int err = 0;
    int count = 0;
    grib_concept_condition* e = NULL;
    grib_values values[1024] = {{0},}; // key/value pair array
    grib_sarray* sa       = NULL;
    grib_concept_value* c = NULL;
    grib_concept_value* concepts = action_concept_get_concept(a);
    grib_handle* h   = grib_handle_of_accessor(a);
    grib_action* act = a->creator_;
    const int nofail = action_concept_get_nofail(a);

    DEBUG_ASSERT(concepts);

    c = (grib_concept_value*)grib_trie_get(concepts->index, name);

    if (!c)
        c = (grib_concept_value*)grib_trie_get(concepts->index, "default");

    if (!c) {
        err = nofail ? GRIB_SUCCESS : GRIB_CONCEPT_NO_MATCH;
        if (err) {
            print_user_friendly_message(h, name, concepts, act);
        }
        return err;
    }
    e  = c->conditions;
    sa = grib_sarray_new(10, 10);
    while (e) {
        err = concept_conditions_apply(h, e, values, sa, &count);
        if (err) return err;
        e = e->next;
    }
    grib_sarray_delete(sa);

    if (count) {
        err = grib_set_values_silent(h, values, count, /*silent=*/1);
        if (err) {
            // Encoding of the concept failed. Can we recover?
            bool resubmit = false;
            for (int i = 0; i < count; i++) {
                if (values[i].error == GRIB_NOT_FOUND) {
                    // Try to rectify the most common causes of failure
                    if (rectify_concept_apply(h, values[i].name) == GRIB_SUCCESS) {
                        resubmit = true;
                        grib_set_values(h, &values[i], 1);
                    }
                }
            }

            if (resubmit) {
                grib_context_log(h->context, GRIB_LOG_DEBUG, "%s: Resubmitting key/values", __func__);
                err = grib_set_values(h, values, count);
            }
        }
    }
    //grib_print_values("DEBUG grib_concept_apply", values, stdout, count);
    if (err) {
        for (int i = 0; i < count; i++) {
            if (values[i].error != GRIB_SUCCESS) {
                grib_context_log(h->context, GRIB_LOG_ERROR,
                                 "grib_set_values[%d] %s (type=%s) failed: %s",
                                 i, values[i].name, grib_get_type_name(values[i].type),
                                 grib_get_error_message(values[i].error));
            }
        }
    }

    return err;
}

int grib_accessor_concept_t::pack_double(const double* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_concept_t::pack_long(const long* val, size_t* len)
{
    char buf[80];
    size_t s;
    snprintf(buf, sizeof(buf), "%ld", *val);

    // if(*len > 1)
    //     return GRIB_NOT_IMPLEMENTED;

    // ECC-1806: GRIB: Change of paramId in conversion from GRIB1 to GRIB2
    if (STR_EQUAL(name_, "paramId")) {
        grib_handle* h = grib_handle_of_accessor(this);
        long edition   = 0;
        if (grib_get_long(h, "edition", &edition) == GRIB_SUCCESS && edition == 2) {
            long newParamId = 0;
            if (grib_get_long(h, "paramIdForConversion", &newParamId) == GRIB_SUCCESS && newParamId > 0) {
                if (context_->debug) {
                    fprintf(stderr, "ECCODES DEBUG %s::%s: Changing %s from %ld to %ld\n",
                            class_name_, __func__, name_, *val, newParamId);
                }
                snprintf(buf, sizeof(buf), "%ld", newParamId);
            }
        }
    }

    s = strlen(buf) + 1;
    return pack_string(buf, &s);
}

int grib_accessor_concept_t::unpack_double(double* val, size_t* len)
{
    //  If we want to have a condition which contains tests for paramId as well
    //  as a floating point key, then need to be able to evaluate paramId as a
    //  double. E.g.
    //     if (referenceValue > 0 && paramId == 129)
    // return GRIB_NOT_IMPLEMENTED
    int ret = 0;
    if (flags_ & GRIB_ACCESSOR_FLAG_LONG_TYPE) {
        long lval = 0;
        ret       = unpack_long(&lval, len);
        if (ret == GRIB_SUCCESS) {
            *val = lval;
        }
    }
    else if (flags_ & GRIB_ACCESSOR_FLAG_DOUBLE_TYPE) {
        const char* p = concept_evaluate(this);

        if (!p) {
            grib_handle* h = grib_handle_of_accessor(this);
            if (creator_->defaultkey_)
                return grib_get_double_internal(h, creator_->defaultkey_, val);

            return GRIB_NOT_FOUND;
        }
        *val = atof(p);
        *len = 1;
    }
    return ret;
}

int grib_accessor_concept_t::unpack_long(long* val, size_t* len)
{
    const char* p = concept_evaluate(this);

    if (!p) {
        grib_handle* h = grib_handle_of_accessor(this);
        if (creator_->defaultkey_)
            return grib_get_long_internal(h, creator_->defaultkey_, val);

        return GRIB_NOT_FOUND;
    }

    *val = atol(p);
    *len = 1;
#ifdef DEBUG
    /* ECC-980: Changes reverted because of side-effects!
     * e.g. marsType being a codetable and concept! see ifsParam.
     * Keep this check in DEBUG mode only
     */
    {
        char* endptr;
        *val = strtol(p, &endptr, 10);
        if (endptr == p || *endptr != '\0') {
            /* Failed to convert string into integer */
            int type = GRIB_TYPE_UNDEFINED;
            grib_context_log(context_, GRIB_LOG_ERROR, "Cannot unpack %s as long", name_);
            if (grib_get_native_type(grib_handle_of_accessor(this), name_, &type) == GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR, "Hint: Try unpacking as %s", grib_get_type_name(type));
            }
            return GRIB_DECODING_ERROR;
        }
    }
#endif
    return GRIB_SUCCESS;
}

long grib_accessor_concept_t::get_native_type()
{
    int type = GRIB_TYPE_STRING;
    if (flags_ & GRIB_ACCESSOR_FLAG_LONG_TYPE)
        type = GRIB_TYPE_LONG;

    return type;
}

void grib_accessor_concept_t::destroy(grib_context* c)
{
    // grib_accessor_concept_t *self = (grib_accessor_concept_t*)a;
    // grib_context_free(c,cval_ );
    grib_accessor_gen_t::destroy(c);
}

int grib_accessor_concept_t::unpack_string(char* val, size_t* len)
{
    size_t slen;
    const char* p = concept_evaluate(this);

    if (!p) {
        grib_handle* h = grib_handle_of_accessor(this);
        if (creator_->defaultkey_)
            return grib_get_string_internal(h, creator_->defaultkey_, val, len);

        return GRIB_NOT_FOUND;
    }

    slen = strlen(p) + 1;
    if (*len < slen) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Concept unpack_string. Buffer too small for %s, value='%s' which requires %lu bytes (len=%lu)",
                         name_, p, slen, *len);
        *len = slen;
        return GRIB_BUFFER_TOO_SMALL;
    }
    strcpy(val, p); /* NOLINT: CWE-119 clang-analyzer-security.insecureAPI.strcpy */
    *len = slen;

    //     if (context_ ->debug==1) {
    //         int err = 0;
    //         char result[1024] = {0,};
    //         err = get_concept_condition_string(grib_handle_of_accessor(this), name_ , val, result);
    //         if (!err) fprintf(stderr, "ECCODES DEBUG concept name=%s, value=%s, conditions=%s\n", name_ , val, result);
    //     }

    return GRIB_SUCCESS;
}

int grib_accessor_concept_t::pack_string(const char* val, size_t* len)
{
    return grib_concept_apply(this, val);
}

size_t grib_accessor_concept_t::string_length()
{
    return MAX_CONCEPT_STRING_LENGTH;
}

int grib_accessor_concept_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

int grib_accessor_concept_t::compare(grib_accessor* b)
{
    int retval = 0;
    char* aval = 0;
    char* bval = 0;

    size_t alen = 0;
    size_t blen = 0;
    int err     = 0;
    long count  = 0;

    err = value_count(&count);
    if (err) return err;
    alen = count;

    err = b->value_count(&count);
    if (err) return err;
    blen = count;

    if (alen != blen)
        return GRIB_COUNT_MISMATCH;
    alen = MAX_CONCEPT_STRING_LENGTH;
    blen = MAX_CONCEPT_STRING_LENGTH;

    aval = (char*)grib_context_malloc(context_, alen * sizeof(char));
    bval = (char*)grib_context_malloc(b->context_, blen * sizeof(char));

    err = unpack_string(aval, &alen);
    if (err) return err;
    err = b->unpack_string(bval, &blen);
    if (err) return err;

    retval = GRIB_SUCCESS;
    if (!aval || !bval || grib_inline_strcmp(aval, bval))
        retval = GRIB_STRING_VALUE_MISMATCH;

    grib_context_free(context_, aval);
    grib_context_free(b->context_, bval);

    return retval;
}
