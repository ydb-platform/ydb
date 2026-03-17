/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_codetable.h"
#include <cctype>

grib_accessor_codetable_t _grib_accessor_codetable{};
grib_accessor* grib_accessor_codetable = &_grib_accessor_codetable;

#if GRIB_PTHREADS
static pthread_once_t once    = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex1, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex1;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_grib_accessor_codetable_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex1);
            once = 1;
        }
    }
}
#endif

static int grib_load_codetable(grib_context* c, const char* filename, const char* recomposed_name, size_t size, grib_codetable* t);

void grib_accessor_codetable_t::init(const long len, grib_arguments* params)
{
    grib_accessor_unsigned_t::init(len, params);

    int n             = 0;
    long new_len      = len;
    grib_handle* hand = grib_handle_of_accessor(this);
    grib_action* act  = (grib_action*)(creator_);
    DEBUG_ASSERT(len == nbytes_);
    table_        = NULL;
    table_loaded_ = 0;

    if (new_len == 0) {
        /* ECC-485: When the codetable length is 0, it means we are passing
         * its length as an identifier not an integer. This identifier is
         * added to the argument list (at the beginning)
         */
        new_len = params->get_long(hand, n++);
        if (new_len <= 0) {
            grib_context_log(context_, GRIB_LOG_FATAL, "%s: codetable length must be a positive integer", name_);
        }
        nbytes_ = new_len;
    }

    tablename_ = params->get_string(hand, n++);
    if (tablename_ == NULL) {
        grib_context_log(context_, GRIB_LOG_FATAL, "%s: codetable table is invalid", name_);
    }
    masterDir_ = params->get_name(hand, n++); /* can be NULL */
    localDir_  = params->get_name(hand, n++); /* can be NULL */

    /*if (flags_ & GRIB_ACCESSOR_FLAG_STRING_TYPE)
    printf("-------- %s type string (%ld)\n",a->name,flags_ );*/
#ifdef DEBUG
    if (flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) {
        grib_context_log(context_, GRIB_LOG_FATAL, "codetable '%s' has flag can_be_missing!", name_);
        ECCODES_ASSERT(!"codetable with can_be_missing?");
    }
#endif

    if (flags_ & GRIB_ACCESSOR_FLAG_TRANSIENT) {
        length_ = 0;
        if (!vvalue_)
            vvalue_ = (grib_virtual_value*)grib_context_malloc_clear(context_, sizeof(grib_virtual_value));
        vvalue_->type   = get_native_type();
        vvalue_->length = new_len;
        if (act->default_value_ != NULL) {
            const char* p = 0;
            size_t s_len  = 1;
            long l;
            int ret = 0;
            double d;
            char tmp[1024];
            grib_expression* expression = act->default_value_->get_expression(hand, 0);
            int type                    = expression->native_type(hand);
            switch (type) {
                case GRIB_TYPE_DOUBLE:
                    expression->evaluate_double(hand, &d);
                    pack_double(&d, &s_len);
                    break;

                case GRIB_TYPE_LONG:
                    expression->evaluate_long(grib_handle_of_accessor(this), &l);
                    pack_long(&l, &s_len);
                    break;

                default:
                    s_len = sizeof(tmp);
                    p     = expression->evaluate_string(grib_handle_of_accessor(this), tmp, &s_len, &ret);
                    if (ret != GRIB_SUCCESS) {
                        grib_context_log(context_, GRIB_LOG_FATAL,
                                         "Unable to evaluate %s as string", name_);
                    }
                    s_len = strlen(p) + 1;
                    pack_string(p, &s_len);
                    break;
            }
        }
    }
    else {
        length_ = new_len;
    }
}

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

// Cater for parameters being NULL
static int str_eq(const char* a, const char* b)
{
    if (a && b && (grib_inline_strcmp(a, b) == 0))
        return 1;
    return 0;
}

#ifdef DEBUGGING
static void dump_codetable(grib_codetable* atable)
{
    grib_codetable* next = NULL;
    int count            = 0;

    next = atable;
    while (next) {
        printf("[%.2d] CodeTable Dump: f0=%s f1=%s\n", count, next->filename[0], next->filename[1]);
        count++;
        next = next->next;
    }
}
#endif

grib_codetable* grib_accessor_codetable_t::load_table()
{
    size_t size                     = 0;
    grib_handle* h                  = parent_->h;
    grib_context* c                 = h->context;
    grib_codetable* t               = NULL;
    grib_codetable* next            = NULL;
    char* filename                  = 0;
    char recomposed[1024]           = {0,};
    char localRecomposed[1024] = {0,};
    char* localFilename  = 0;
    char masterDir[1024] = {0,};
    char localDir[1024] = {0,};
    size_t len = 1024;

    if (masterDir_ != NULL)
        grib_get_string(h, masterDir_, masterDir, &len);

    len = 1024;
    if (localDir_ != NULL)
        grib_get_string(h, localDir_, localDir, &len);

    if (*masterDir != 0) {
        char name[2048] = {0,};
        snprintf(name, sizeof(name), "%s/%s", masterDir, tablename_);
        grib_recompose_name(h, NULL, name, recomposed, 0);
        filename = grib_context_full_defs_path(c, recomposed);
    }
    else {
        grib_recompose_name(h, NULL, tablename_, recomposed, 0);
        filename = grib_context_full_defs_path(c, recomposed);
    }

    if (*localDir != 0) {
        char localName[2048] = {0,};
        snprintf(localName, sizeof(localName), "%s/%s", localDir, tablename_);
        grib_recompose_name(h, NULL, localName, localRecomposed, 0);
        localFilename = grib_context_full_defs_path(c, localRecomposed);
    }

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex1); /* GRIB-930 */

    /*printf("DBG %s: Look in cache: f=%s lf=%s (recomposed=%s)\n", att_ .name, filename, localFilename,recomposed);*/
    if (filename == NULL && localFilename == NULL) {
        t = NULL;
        goto the_end;
    }
    next = c->codetable;
    while (next) {
        if ((filename && next->filename[0] && grib_inline_strcmp(filename, next->filename[0]) == 0) &&
            ((localFilename == 0 && next->filename[1] == NULL) ||
             ((localFilename != 0 && next->filename[1] != NULL) && grib_inline_strcmp(localFilename, next->filename[1]) == 0))) {
            t = next;
            goto the_end;
        }
        /* Special case: see GRIB-735 */
        if (filename == NULL && localFilename != NULL) {
            if (str_eq(localFilename, next->filename[0]) ||
                str_eq(localFilename, next->filename[1])) {
                t = next;
                goto the_end;
            }
        }
        next = next->next;
    }

    if (flags_ & GRIB_ACCESSOR_FLAG_TRANSIENT) {
        ECCODES_ASSERT(vvalue_ != NULL);
        size = vvalue_->length * 8;
    }
    else {
        size = byte_count() * 8;
    }

    size = (1ULL << size); /* 2^size - 64bits */

    t = (grib_codetable*)grib_context_malloc_clear_persistent(c, sizeof(grib_codetable) +
                                                                     (size - 1) * sizeof(code_table_entry));

    if (filename != 0)
        grib_load_codetable(c, filename, recomposed, size, t);

    if (localFilename != 0)
        grib_load_codetable(c, localFilename, localRecomposed, size, t);

    /*dump_codetable(c->codetable);*/

    if (t->filename[0] == NULL && t->filename[1] == NULL) {
        grib_context_free_persistent(c, t);
        t = NULL;
        goto the_end;
    }

the_end:
    GRIB_MUTEX_UNLOCK(&mutex1);

    return t;
}

static int grib_load_codetable(grib_context* c, const char* filename,
                               const char* recomposed_name, size_t size, grib_codetable* t)
{
    char line[1024];
    FILE* f        = NULL;
    int lineNumber = 0;
    grib_context_log(c, GRIB_LOG_DEBUG, "Loading code table from %s", filename);

    f = codes_fopen(filename, "r");
    if (!f)
        return GRIB_IO_PROBLEM;

    ECCODES_ASSERT(t != NULL);

    if (t->filename[0] == NULL) {
        t->filename[0]        = grib_context_strdup_persistent(c, filename);
        t->recomposed_name[0] = grib_context_strdup_persistent(c, recomposed_name);
        t->next               = c->codetable;
        t->size               = size;
        c->codetable          = t;
    }
    else {
        t->filename[1]        = grib_context_strdup_persistent(c, filename);
        t->recomposed_name[1] = grib_context_strdup_persistent(c, recomposed_name);
    }

    while (fgets(line, sizeof(line) - 1, f)) {
        char* p                 = line;
        int code                = 0;
        char abbreviation[1024] = {0,};
        char title[1024] = {0,};
        char* pAbbrev         = abbreviation;
        char* pTitle          = title;
        char* units           = 0;
        char unknown[]        = "unknown";
        char* last_open_paren = NULL;
        char* last_clos_paren = NULL;

        ++lineNumber;

        line[strlen(line) - 1] = 0;

        while (*p != '\0' && isspace(*p))
            p++;

        if (*p == '#')
            continue;

        last_open_paren = strrchr(line, '(');

        while (*p != '\0' && isspace(*p))
            p++;

        if (*p == '\0')
            continue;

        if (!isdigit(*p)) {
            grib_context_log(c, GRIB_LOG_ERROR, "Invalid entry in file %s: line %d", filename, lineNumber);
            continue; /* skip this line */
        }
        ECCODES_ASSERT(isdigit(*p));

        while (*p != '\0') {
            if (isspace(*p))
                break;
            code *= 10;
            code += *p - '0';
            p++;
        }

        if (code < 0 || code >= size) {
            grib_context_log(c, GRIB_LOG_WARNING, "code_table_entry: invalid code in %s: %d (table size=%ld)", filename, code, size);
            continue;
        }

        while (*p != '\0' && isspace(*p))
            p++;

        while (*p != '\0') {
            if (isspace(*p))
                break;
            *pAbbrev++ = *p++;
        }
        *pAbbrev = 0;
        while (*p != '\0' && isspace(*p))
            p++;

        /* The title goes as far as the last open paren */
        while (*p != '\0') {
            if (last_open_paren && p >= last_open_paren && *p == '(')
                break;
            *pTitle++ = *p++;
        }
        *pTitle = 0;

        /* units at the end */
        if (last_open_paren) {
            last_clos_paren = strrchr(line, ')');
            if (last_clos_paren && last_open_paren != last_clos_paren) {
                units = last_open_paren + 1;
                p     = units;
                p += (last_clos_paren - last_open_paren - 1);
                *p = '\0';
            }
        }
        if (!units)
            units = unknown;

        ECCODES_ASSERT(*abbreviation);
        ECCODES_ASSERT(*title);
        string_rtrim(title); /* ECC-1315 */

        if (t->entries[code].abbreviation != NULL) {
            grib_context_log(c, GRIB_LOG_WARNING, "code_table_entry: duplicate code in %s: %d (table size=%ld)", filename, code, size);
            continue;
        }

        ECCODES_ASSERT(t->entries[code].abbreviation == NULL);
        ECCODES_ASSERT(t->entries[code].title == NULL);

        t->entries[code].abbreviation = grib_context_strdup_persistent(c, abbreviation);
        t->entries[code].title        = grib_context_strdup_persistent(c, title);
        t->entries[code].units        = grib_context_strdup_persistent(c, units);
    }

    fclose(f);

    return 0;
}

void grib_codetable_delete(grib_context* c)
{
    grib_codetable* t = c->codetable;

    while (t) {
        grib_codetable* s = t->next;
        int i;

        for (i = 0; i < t->size; i++) {
            grib_context_free_persistent(c, t->entries[i].abbreviation);
            grib_context_free_persistent(c, t->entries[i].title);
            grib_context_free_persistent(c, t->entries[i].units);
        }
        grib_context_free_persistent(c, t->filename[0]);
        if (t->filename[1])
            grib_context_free_persistent(c, t->filename[1]);
        grib_context_free_persistent(c, t->recomposed_name[0]);
        if (t->recomposed_name[1])
            grib_context_free_persistent(c, t->recomposed_name[1]);
        grib_context_free_persistent(c, t);
        t = s;
    }
}

int codes_codetable_get_contents_malloc(const grib_handle* h, const char* key, code_table_entry** entries, size_t* num_entries)
{
    long lvalue     = 0;
    size_t size     = 1;
    int err         = 0;
    grib_context* c = h->context;

    grib_accessor* aa = grib_find_accessor(h, key);
    if (!aa) return GRIB_NOT_FOUND;

    if (!STR_EQUAL(aa->class_name_, "codetable")) {
        return GRIB_INVALID_ARGUMENT;  // key is not a codetable
    }

    const grib_accessor_codetable_t* ca = (const grib_accessor_codetable_t*)aa;  // could be dynamic_cast

    // Decode the key itself. This will either fetch it from the cache or place it there
    if ((err = aa->unpack_long(&lvalue, &size)) != GRIB_SUCCESS) {
        return err;
    }

    const grib_codetable* table = ca->codetable();
    if (!table) return GRIB_INTERNAL_ERROR;

    grib_codetable* cached_table = c->codetable;  // Access the codetable cache
    while (cached_table) {
        if (STR_EQUAL(table->recomposed_name[0], cached_table->recomposed_name[0])) {
            // Found a cache entry that matches the recomposed name of ours
            *num_entries = cached_table->size;
            *entries     = (code_table_entry*)calloc(cached_table->size, sizeof(code_table_entry));
            if (!*entries) {
                return GRIB_OUT_OF_MEMORY;
            }
            for (size_t i = 0; i < cached_table->size; i++) {
                (*entries)[i] = cached_table->entries[i];
            }
            return GRIB_SUCCESS;
        }
        cached_table = cached_table->next;
    }

    return GRIB_CODE_NOT_FOUND_IN_TABLE;
}

int codes_codetable_check_code_figure(const grib_handle* h, const char* key, long code_figure)
{
    code_table_entry* entries = NULL;
    size_t num_entries        = 0;
    int err                   = 0;
    err                       = codes_codetable_get_contents_malloc(h, key, &entries, &num_entries);
    if (err) return err;

    if (code_figure < 0 || (size_t)code_figure >= num_entries) {
        err = GRIB_OUT_OF_RANGE;
        goto cleanup;
    }

    if (entries[code_figure].abbreviation == NULL) {
        err = GRIB_INVALID_KEY_VALUE;
        goto cleanup;
    }
cleanup:
    free(entries);
    return err;
}

int codes_codetable_check_abbreviation(const grib_handle* h, const char* key, const char* abbreviation)
{
    code_table_entry* entries = NULL;
    size_t num_entries        = 0;
    int err                   = 0;
    err                       = codes_codetable_get_contents_malloc(h, key, &entries, &num_entries);
    if (err) return err;

    bool found = false;
    for (size_t i = 0; i < num_entries; ++i) {
        const char* abbrev = entries[i].abbreviation;
        if (abbrev && STR_EQUAL(abbrev, abbreviation)) {
            found = true;
            break;
        }
    }
    if (!found) err = GRIB_INVALID_KEY_VALUE;

    free(entries);
    return err;
}

void grib_accessor_codetable_t::dump(eccodes::Dumper* dumper)
{
    char comment[2048];
    grib_codetable* table;

    size_t llen = 1;
    long value;

    if (!table_loaded_) {
        table_        = load_table(); /* may return NULL */
        table_loaded_ = 1;
    }
    table = table_;

    unpack_long(&value, &llen);

    if (value == GRIB_MISSING_LONG) {
        if (length_ < 4) {
            value = (1L << length_) - 1;
        }
    }

    if (table && value >= 0 && value < table->size) {
        if (table->entries[value].abbreviation) {
            long b = atol(table->entries[value].abbreviation);
            if (b == value)
                strcpy(comment, table->entries[value].title);
            else
                snprintf(comment, sizeof(comment), "%s", table->entries[value].title);

            if (table->entries[value].units != NULL && grib_inline_strcmp(table->entries[value].units, "unknown")) {
                strcat(comment, " (");
                strcat(comment, table->entries[value].units);
                strcat(comment, ") ");
            }
        }
        else {
            strcpy(comment, "Unknown code table entry");
        }
    }
    else {
        strcpy(comment, "Unknown code table entry");
    }

    strcat(comment, " (");
    if (table) {
        strcat(comment, table->recomposed_name[0]);
        if (table->recomposed_name[1] != NULL) {
            strcat(comment, " , ");
            strcat(comment, table->recomposed_name[1]);
        }
    }
    strcat(comment, ") ");

    dumper->dump_long(this, comment);
}

int grib_accessor_codetable_t::unpack_string(char* buffer, size_t* len)
{
    grib_codetable* table = NULL;

    size_t size = 1;
    long value;
    int err = GRIB_SUCCESS;
    char tmp[1024];
    size_t l = 0;

    if ((err = unpack_long(&value, &size)) != GRIB_SUCCESS)
        return err;

    if (!table_loaded_) {
        table_        = load_table(); /* may return NULL */
        table_loaded_ = 1;
    }
    table = table_;

    if (table && (value >= 0) && (value < table->size) && table->entries[value].abbreviation) {
        strcpy(tmp, table->entries[value].abbreviation);
    }
    else {
        snprintf(tmp, sizeof(tmp), "%d", (int)value);
    }

    l = strlen(tmp) + 1;

    if (*len < l) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, l, *len);
        *len = l;
        return GRIB_BUFFER_TOO_SMALL;
    }

    strcpy(buffer, tmp);
    *len = l;

    return GRIB_SUCCESS;
}

int grib_accessor_codetable_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

// Return true if the input is an integer (non-negative)
static bool is_number(const char* s)
{
    while (*s) {
        if (!isdigit(*s))
            return false;
        s++;
    }
    return true;
}

bool strings_equal(const char* s1, const char* s2, bool case_sensitive)
{
    if (case_sensitive) return (strcmp(s1, s2) == 0);
    return (strcmp_nocase(s1, s2) == 0);
}

int grib_accessor_codetable_t::pack_string(const char* buffer, size_t* len)
{
    long lValue = 0;
    ECCODES_ASSERT(buffer);
    if (is_number(buffer) && string_to_long(buffer, &lValue, 1) == GRIB_SUCCESS) {
        // ECC-1654: If value is a pure number, just pack as long
        size_t l = 1;
        return pack_long(&lValue, &l);
    }

    if (STR_EQUAL_NOCASE(buffer, "missing")) {
        return pack_missing();
    }

    grib_codetable* table = NULL;
    long i                = 0;
    size_t size           = 1;

    if (!table_loaded_) {
        table_        = load_table(); /* may return NULL */
        table_loaded_ = 1;
    }
    table = table_;

    if (!table)
        return GRIB_ENCODING_ERROR;

    if (set_) {
        int err = grib_set_string(grib_handle_of_accessor(this), set_, buffer, len);
        if (err != 0)
            return err;
    }

    // If the key has the "lowercase" flag set, then the string comparison
    // should ignore the case
    bool case_sensitive = true;
    if (flags_ & GRIB_ACCESSOR_FLAG_LOWERCASE) case_sensitive = false;

    for (i = 0; i < table->size; i++) {
        if (table->entries[i].abbreviation) {
            if (strings_equal(table->entries[i].abbreviation, buffer, case_sensitive)) {
                return pack_long(&i, &size);
            }
        }
    }

    if (flags_ & GRIB_ACCESSOR_FLAG_NO_FAIL) {
        grib_action* act = (grib_action*)(creator_);
        if (act->default_value_ != NULL) {
            const char* p  = 0;
            size_t s_len   = 1;
            long l         = 0;
            int ret        = 0;
            double d       = 0;
            char tmp[1024] = {0,};
            grib_expression* expression = act->default_value_->get_expression(grib_handle_of_accessor(this), 0);
            int type                    = expression->native_type(grib_handle_of_accessor(this));
            switch (type) {
                case GRIB_TYPE_DOUBLE:
                    expression->evaluate_double(grib_handle_of_accessor(this), &d);
                    pack_double(&d, &s_len);
                    break;

                case GRIB_TYPE_LONG:
                    expression->evaluate_long(grib_handle_of_accessor(this), &l);
                    pack_long(&l, &s_len);
                    break;

                default:
                    s_len = sizeof(tmp);
                    p     = expression->evaluate_string(grib_handle_of_accessor(this), tmp, &s_len, &ret);
                    if (ret != GRIB_SUCCESS) {
                        grib_context_log(context_, GRIB_LOG_ERROR,
                                         "%s: Unable to evaluate default value of %s as string expression", __func__, name_);
                        return ret;
                    }
                    s_len = strlen(p) + 1;
                    pack_string(p, &s_len);
                    break;
            }
            return GRIB_SUCCESS;
        }
    }

    // ECC-1652: Failed. Now do a case-insensitive compare to give the user a hint
    for (i = 0; i < table->size; i++) {
        if (table->entries[i].abbreviation) {
            if (strcmp_nocase(table->entries[i].abbreviation, buffer) == 0) {
                grib_context_log(context_, GRIB_LOG_ERROR,
                                 "%s: No such code table entry: '%s' "
                                 "(Did you mean '%s'?)",
                                 name_, buffer, table->entries[i].abbreviation);
            }
        }
    }

    return GRIB_ENCODING_ERROR;
}

int grib_accessor_codetable_t::pack_expression(grib_expression* e)
{
    const char* cval  = NULL;
    int ret           = 0;
    long lval         = 0;
    size_t len        = 1;
    grib_handle* hand = grib_handle_of_accessor(this);

    if (strcmp(e->class_name(), "long") == 0) {
        e->evaluate_long(hand, &lval); /* TODO: check return value */
        // if (hand->context->debug) printf("ECCODES DEBUG grib_accessor_codetable::pack_expression %s %ld\n", name_ ,lval);
        ret = pack_long(&lval, &len);
    }
    else {
        char tmp[1024];
        len  = sizeof(tmp);
        cval = e->evaluate_string(hand, tmp, &len, &ret);
        if (ret != GRIB_SUCCESS) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "grib_accessor_codetable.%s: Unable to evaluate string %s to be set in %s",
                             __func__, e->get_name(), name_);
            return ret;
        }
        len = strlen(cval) + 1;
        // if (hand->context->debug)
        //     printf("ECCODES DEBUG grib_accessor_codetable::pack_expression %s %s\n", name_ , cval);
        ret = pack_string(cval, &len);
    }
    return ret;
}

void grib_accessor_codetable_t::destroy(grib_context* context)
{
    if (vvalue_ != NULL) {
        grib_context_free(context, vvalue_);
        vvalue_ = NULL;
    }
    grib_accessor_unsigned_t::destroy(context);
}

long grib_accessor_codetable_t::get_native_type()
{
    int type = GRIB_TYPE_LONG;
    /*printf("---------- %s flags=%ld GRIB_ACCESSOR_FLAG_STRING_TYPE=%d\n",
         a->name,flags_ ,GRIB_ACCESSOR_FLAG_STRING_TYPE);*/
    if (flags_ & GRIB_ACCESSOR_FLAG_STRING_TYPE)
        type = GRIB_TYPE_STRING;
    return type;
}

int grib_accessor_codetable_t::unpack_long(long* val, size_t* len)
{
    long rlen = 0, i = 0;
    long pos          = offset_ * 8;
    grib_handle* hand = NULL;

#ifdef DEBUG
    {
        int err = value_count(&rlen);
        ECCODES_ASSERT(!err);
        ECCODES_ASSERT(rlen == 1);
    }
#endif
    rlen = 1; /* ECC-480 Performance: avoid func call overhead of grib_value_count */

    if (!table_loaded_) {
        table_        = load_table(); /* may return NULL */
        table_loaded_ = 1;
    }

    if (*len < rlen) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size (%lu) for %s, it contains %ld values",
                         *len, name_, rlen);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }

    if (flags_ & GRIB_ACCESSOR_FLAG_TRANSIENT) {
        *val = vvalue_->lval;
        *len = 1;
        return GRIB_SUCCESS;
    }

    /* ECC-480 Performance: inline the grib_handle_of_accessor here to reduce func call overhead */
    if (parent_ == NULL)
        hand = h_;
    else
        hand = parent_->h;

    for (i = 0; i < rlen; i++) {
        val[i] = (long)grib_decode_unsigned_long(hand->buffer->data, &pos, nbytes_ * 8);
    }

    *len = rlen;
    return GRIB_SUCCESS;
}

int grib_accessor_codetable_t::pack_missing()
{
    // Many of the code tables do have a 'Missing' entry (all bits = 1)
    // So it is more user-friendly to allow setting codetable keys to
    // missing. For tables that do not have such an entry, an error is issued
    grib_handle* h = grib_handle_of_accessor(this);

    const long nbytes = length_;
    const long nbits  = nbytes * 8;
    const long maxVal = (1 << nbits) - 1;

    int err = codes_codetable_check_code_figure(h, name_, maxVal);
    if (!err) {
        size_t l = 1;
        return pack_long(&maxVal, &l);
    }

    grib_context_log(context_, GRIB_LOG_ERROR, "There is no 'missing' entry in Code Table %s (%s)",
                     tablename_, grib_get_error_message(err));

    return err;
}
