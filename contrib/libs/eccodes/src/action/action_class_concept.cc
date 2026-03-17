/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_concept.h"


#if GRIB_PTHREADS
static pthread_once_t once   = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_action_class_concept_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex);
            once = 1;
        }
    }
}
#endif

grib_concept_value* action_concept_get_concept(grib_accessor* a)
{
    return static_cast<eccodes::action::Concept*>(a->creator_)->get_concept(grib_handle_of_accessor(a));
}

int action_concept_get_nofail(grib_accessor* a)
{
    const eccodes::action::Concept* self = static_cast<eccodes::action::Concept*>(a->creator_);
    return self->nofail_;
}

grib_action* grib_action_create_concept(grib_context* context,
                                        const char* name,
                                        grib_concept_value* concept_value,
                                        const char* basename, const char* name_space, const char* defaultkey,
                                        const char* masterDir, const char* localDir, const char* ecmfDir, int flags, int nofail)
{
    return new eccodes::action::Concept(context, name, concept_value, basename, name_space, defaultkey, masterDir, localDir, ecmfDir, flags, nofail);
}


namespace eccodes::action
{

Concept::Concept(grib_context* context,
                 const char* name,
                 grib_concept_value* concept_value,
                 const char* basename, const char* name_space, const char* defaultkey,
                 const char* masterDir, const char* localDir, const char* ecmfDir, int flags, int nofail) :
Gen(context, name, "concept", 0, nullptr, nullptr, flags, name_space, nullptr)
{
    class_name_    = "action_class_concept";
    basename_      = basename ? grib_context_strdup_persistent(context, basename) : nullptr;
    masterDir_     = masterDir ? grib_context_strdup_persistent(context, masterDir) : nullptr;
    localDir_      = localDir ? grib_context_strdup_persistent(context, localDir) : nullptr;
    defaultkey_    = defaultkey ? grib_context_strdup_persistent(context, defaultkey) : nullptr;
    concept_value_ = concept_value;

    if (concept_value) {
        grib_concept_value* conc_val = concept_value;
        grib_trie* index             = grib_trie_new(context);
        while (conc_val) {
            conc_val->index = index;
            grib_trie_insert_no_replace(index, conc_val->name, conc_val);
            conc_val = conc_val->next;
        }
    }

    nofail_ = nofail;
}

Concept::~Concept()
{
    grib_concept_value* v = concept_value_;
    if (v) {
        grib_trie_delete_container(v->index);
    }
    while (v) {
        grib_concept_value* n = v->next;
        grib_concept_value_delete(context_, v);
        v = n;
    }
    grib_context_free_persistent(context_, masterDir_);
    grib_context_free_persistent(context_, localDir_);
    grib_context_free_persistent(context_, basename_);
}

void Concept::dump(FILE* f, int lvl)
{
    for (int i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");

    printf("concept(%s) { \n", name_);

    for (int i = 0; i < lvl; i++)
        grib_context_print(context_, f, "     ");
    printf("}\n");
}

grib_concept_value* Concept::get_concept_impl(grib_handle* h)
{
    char buf[4096]        = {0, };
    char master[1024]     = {0, };
    char local[1024]      = {0, };
    char masterDir[1024]  = {0, };
    size_t lenMasterDir   = sizeof(masterDir);
    char key[4096]        = {0, };
    char* full            = 0;
    const size_t bufLen   = sizeof(buf);
    const size_t keyLen   = sizeof(key);
    grib_context* context = context_;
    grib_concept_value* c = NULL;

    if (concept_value_ != NULL)
        return concept_value_;

    ECCODES_ASSERT(masterDir_);
    grib_get_string(h, masterDir_, masterDir, &lenMasterDir);

    // See ECC-1920: The basename could be a key or a string
    char* basename = basename_; // default is a string
    ECCODES_ASSERT(basename);
    char baseNameValue[1024] = {0, }; // its value if a key
    size_t lenBaseName = sizeof(baseNameValue);
    if (grib_get_string(h, basename_, baseNameValue, &lenBaseName) == GRIB_SUCCESS) {
        basename = baseNameValue;  // basename_ was a key whose value is baseNameValue
    }
    snprintf(buf, bufLen, "%s/%s", masterDir, basename);

    grib_recompose_name(h, NULL, buf, master, 1);

    if (localDir_) {
        char localDir[1024] = {0, };
        size_t lenLocalDir = 1024;
        grib_get_string(h, localDir_, localDir, &lenLocalDir);
        snprintf(buf, bufLen, "%s/%s", localDir, basename);
        grib_recompose_name(h, NULL, buf, local, 1);
    }

    snprintf(key, keyLen, "%s%s", master, local);

    int id = grib_itrie_get_id(h->context->concepts_index, key);
    if ((c = h->context->concepts[id]) != NULL)
        return c;

    if (*local && (full = grib_context_full_defs_path(context, local)) != NULL) {
        c = grib_parse_concept_file(context, full);
        grib_context_log(h->context, GRIB_LOG_DEBUG,
                         "Loading concept %s from %s", name_, full);
    }

    full = grib_context_full_defs_path(context, master);

    if (c) {
        grib_concept_value* last = c;
        while (last->next)
            last = last->next;
        if (full) {
            last->next = grib_parse_concept_file(context, full);
        }
    }
    else if (full) {
        c = grib_parse_concept_file(context, full);
    }
    else {
        grib_context_log(context, GRIB_LOG_FATAL,
                         "unable to find definition file %s in %s:%s\nDefinition files path=\"%s\"",
                         basename, master, local, context->grib_definition_files_path);
        return NULL;
    }

    if (full) {
        grib_context_log(h->context, GRIB_LOG_DEBUG,
                         "Loading concept %s from %s", name_, full);
    }

    h->context->concepts[id] = c;
    if (c) {
        grib_trie* index = grib_trie_new(context);
        while (c) {
            c->index = index;
            grib_trie_insert_no_replace(index, c->name, c);
            c = c->next;
        }
    }

    return h->context->concepts[id];
}


grib_concept_value* Concept::get_concept(grib_handle* h)
{
    grib_concept_value* result = NULL;
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);

    result = get_concept_impl(h);

    GRIB_MUTEX_UNLOCK(&mutex);
    return result;
}

}  // namespace eccodes::action

static int concept_condition_expression_true(grib_handle* h, grib_concept_condition* c, char* exprVal)
{
    long lval      = 0;
    long lres      = 0;
    int ok         = 0;
    int err        = 0;
    const int type = c->expression->native_type(h);

    switch (type) {
        case GRIB_TYPE_LONG:
            c->expression->evaluate_long(h, &lres);
            ok = (grib_get_long(h, c->name, &lval) == GRIB_SUCCESS) &&
                 (lval == lres);
            if (ok)
                snprintf(exprVal, 64, "%ld", lres);
            break;

        case GRIB_TYPE_DOUBLE: {
            double dval;
            double dres = 0.0;
            c->expression->evaluate_double(h, &dres);
            ok = (grib_get_double(h, c->name, &dval) == GRIB_SUCCESS) &&
                 (dval == dres);
            if (ok)
                snprintf(exprVal, 64, "%g", dres);
            break;
        }

        case GRIB_TYPE_STRING: {
            const char* cval;
            char buf[256];
            char tmp[256];
            size_t len  = sizeof(buf);
            size_t size = sizeof(tmp);

            ok = (grib_get_string(h, c->name, buf, &len) == GRIB_SUCCESS) &&
                 ((cval = c->expression->evaluate_string(h, tmp, &size, &err)) != NULL) &&
                 (err == 0) && (strcmp(buf, cval) == 0);
            if (ok) {
                snprintf(exprVal, size, "%s", cval);
            }
            break;
        }

        default:
            /* TODO: */
            break;
    }
    return ok;
}

/* Caller has to allocate space for the result.
 * INPUTS: h, key and value (can be NULL)
 * OUTPUT: result
 * Example: key='typeOfLevel' whose value is 'mixedLayerDepth',
 * result='typeOfFirstFixedSurface=169,typeOfSecondFixedSurface=255'
 */
int get_concept_condition_string(grib_handle* h, const char* key, const char* value, char* result)
{
    int err         = 0;
    int length      = 0;
    char strVal[64] = {0, };
    char exprVal[256] = {0, };
    const char* pValue                = value;
    size_t len                        = sizeof(strVal);
    grib_concept_value* concept_value = NULL;
    grib_accessor* acc                = grib_find_accessor(h, key);
    if (!acc)
        return GRIB_NOT_FOUND;

    if (!value) {
        err = grib_get_string(h, key, strVal, &len);
        if (err)
            return GRIB_INTERNAL_ERROR;
        pValue = strVal;
    }

    concept_value = action_concept_get_concept(acc);
    while (concept_value) {
        grib_concept_condition* concept_condition = concept_value->conditions;
        if (strcmp(pValue, concept_value->name) == 0) {
            while (concept_condition) {
                // grib_expression* expression = concept_condition->expression;
                const char* condition_name  = concept_condition->name;
                //ECCODES_ASSERT(expression);
                if (concept_condition_expression_true(h, concept_condition, exprVal) && strcmp(condition_name, "one") != 0) {
                    length += snprintf(result + length, 2048, "%s%s=%s",
                                       (length == 0 ? "" : ","), condition_name, exprVal);
                }
                concept_condition = concept_condition->next;
            }
        }

        concept_value = concept_value->next;
    }
    if (length == 0)
        return GRIB_CONCEPT_NO_MATCH;
    return GRIB_SUCCESS;
}
