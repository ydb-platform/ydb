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
 *   Jean Baptiste Filippi - 01.11.2005                                    *
 ***************************************************************************/
#include "grib_api_internal.h"
#include "action_class_noop.h"

grib_action* grib_parser_all_actions          = 0;
grib_context* grib_parser_context             = 0;
grib_concept_value* grib_parser_concept       = 0;
grib_hash_array_value* grib_parser_hash_array = 0;
grib_rule* grib_parser_rules                  = 0;

extern FILE* grib_yyin;
extern int grib_yydebug;

static const char* parse_file = 0;

#if GRIB_PTHREADS
static pthread_once_t once              = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex_file       = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_rules      = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_concept    = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_hash_array = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_stream     = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_parse      = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex_file, &attr);
    pthread_mutex_init(&mutex_rules, &attr);
    pthread_mutex_init(&mutex_concept, &attr);
    pthread_mutex_init(&mutex_hash_array, &attr);
    pthread_mutex_init(&mutex_stream, &attr);
    pthread_mutex_init(&mutex_parse, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex_file;
static omp_nest_lock_t mutex_rules;
static omp_nest_lock_t mutex_concept;
static omp_nest_lock_t mutex_hash_array;
static omp_nest_lock_t mutex_stream;
static omp_nest_lock_t mutex_parse;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_grib_parse_utils_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex_file);
            omp_init_nest_lock(&mutex_rules);
            omp_init_nest_lock(&mutex_concept);
            omp_init_nest_lock(&mutex_hash_array);
            omp_init_nest_lock(&mutex_stream);
            omp_init_nest_lock(&mutex_parse);
            once = 1;
        }
    }
}
#endif

int grib_recompose_name(grib_handle* h, grib_accessor* observer, const char* uname, char* fname, int fail)
{
    grib_accessor* a;
    char loc[1024] = {0,};
    int i          = 0;
    int ret        = 0;
    int mode       = -1;
    char val[1024] = {0,};
    double dval        = 0;
    long lval          = 0;
    int type           = GRIB_TYPE_STRING;
    size_t replen      = 0;
    char* ptrEnd_fname = NULL; /* Maintain ptr to end of fname string */

    loc[0]       = 0;
    fname[0]     = 0;
    ptrEnd_fname = fname;

    /* uname is a string like "grib[GRIBEditionNumber:i]/boot.def". The result fname will be grib2/boot.def */
    while (uname[i] != '\0') {
        if (mode > -1) {
            if (uname[i] == ':') {
                type = grib_type_to_int(uname[i + 1]);
                i++;
            }
            else if (uname[i] == ']') {
                loc[mode] = 0;
                mode      = -1;
                a         = grib_find_accessor(h, loc);
                if (!a) {
                    if (!fail) {
                        snprintf(val, sizeof(val), "undef");
                    }
                    else {
                        grib_context_log(h->context, GRIB_LOG_WARNING,
                          "%s: Problem recomposing filename with: %s (%s no accessor found)", __func__, uname, loc);
                        return GRIB_NOT_FOUND;
                    }
                }
                else {
                    switch (type) {
                        case GRIB_TYPE_STRING:
                            replen = 1024;
                            ret    = a->unpack_string(val, &replen);
                            break;
                        case GRIB_TYPE_DOUBLE:
                            replen = 1;
                            ret    = a->unpack_double(&dval, &replen);
                            snprintf(val, sizeof(val), "%.12g", dval);
                            break;
                        case GRIB_TYPE_LONG:
                            replen = 1;
                            ret    = a->unpack_long(&lval, &replen);
                            snprintf(val, sizeof(val), "%d", (int)lval);
                            break;
                        default:
                            grib_context_log(h->context, GRIB_LOG_WARNING,
                            "Recompose name: Problem recomposing filename with %s, invalid type %d", loc, type);
                            break;
                    }

                    grib_dependency_add(observer, a);

                    if ((ret != GRIB_SUCCESS)) {
                        grib_context_log(h->context, GRIB_LOG_ERROR, "Recompose name: Could not recompose filename: %s", uname);
                        return ret;
                    }
                }
                {
                    char* pc = fname;
                    while (*pc != '\0')
                        pc++;
                    strcpy(pc, val);
                    ptrEnd_fname = pc + strlen(val); /* Update ptr to end of fname */
                }

                loc[0] = 0;
            }
            else
                loc[mode++] = uname[i];
        }
        else if (uname[i] == '[')
            mode = 0;
        else {
            // Old way: Slow; The strlen cost is too high
            //int llen=strlen(fname);
            //fname[llen]=uname[i];
            //fname[llen+1]='\0';

            // Faster to avoid call to strlen. Append to end
            *ptrEnd_fname++ = uname[i];
            *ptrEnd_fname   = '\0';

            type = GRIB_TYPE_STRING;
        }
        i++;
    }
    /*fprintf(stdout,"parsed > %s\n",fname);*/
    return GRIB_SUCCESS;
}

// int grib_accessor_print(grib_accessor* a, const char* name, int type, const char* format,
//                         const char* separator, int maxcols, int* newline, FILE* out)
// {
//     size_t size     = 0;
//     char* sval      = NULL;
//     char* p         = NULL;
//     double* dval    = 0;
//     long* lval      = 0;
//     char sbuf[1024] = {0,};
//     size_t replen            = 0;
//     int ret                  = 0;
//     char* myformat           = NULL;
//     char* myseparator        = NULL;
//     char double_format[]     = "%.12g"; /* default format for printing double keys */
//     char long_format[]       = "%ld";   /* default format for printing integer keys */
//     char default_separator[] = " ";
//     grib_handle* h           = grib_handle_of_accessor(a);

//     if (type == -1)
//         type = a->get_native_type();
//     switch (type) {
//         case GRIB_TYPE_STRING:
//             replen = sizeof(sbuf) / sizeof(*sbuf);
//             ret    = a->unpack_string(sbuf, &replen);
//             fprintf(out, "%s", sbuf);
//             break;
//         case GRIB_TYPE_DOUBLE:
//             myformat    = format ? (char*)format : double_format;
//             myseparator = separator ? (char*)separator : default_separator;
//             if (name[0] == '/' || name[0] == '#') {
//                 long count;
//                 ret  = a->value_count(&count);
//                 size = count;
//             }
//             else {
//                 ret = grib_get_size_(h, a, &size);
//             }
//             if (ret) return ret;
//             dval = (double*)grib_context_malloc_clear(h->context, sizeof(double) * size);
//             if (name[0] == '/' || name[0] == '#') {
//                 replen = size;
//                 ret    = a->unpack_double(dval, &replen);
//             }
//             else {
//                 replen = 0;
//                 ret    = ecc__grib_get_double_array_internal(h, a, dval, size, &replen);
//             }
//             if (replen == 1)
//                 fprintf(out, myformat, dval[0]);
//             else {
//                 int i    = 0;
//                 int cols = 0;
//                 for (i = 0; i < replen; i++) {
//                     *newline = 1;
//                     fprintf(out, myformat, dval[i]);
//                     if (i < replen - 1)
//                         fprintf(out, "%s", myseparator);
//                     cols++;
//                     if (cols >= maxcols) {
//                         fprintf(out, "\n");
//                         *newline = 1;
//                         cols     = 0;
//                     }
//                 }
//             }
//             grib_context_free(h->context, dval);
//             break;
//         case GRIB_TYPE_LONG:
//             myformat    = format ? (char*)format : long_format;
//             myseparator = separator ? (char*)separator : default_separator;
//             if (name[0] == '/' || name[0] == '#') {
//                 long count;
//                 ret  = a->value_count(&count);
//                 size = count;
//             }
//             else {
//                 ret = grib_get_size_(h, a, &size);
//             }
//             if (ret) return ret;
//             lval = (long*)grib_context_malloc_clear(h->context, sizeof(long) * size);
//             if (name[0] == '/' || name[0] == '#') {
//                 replen = size;
//                 ret    = a->unpack_long(lval, &replen);
//             }
//             else {
//                 replen = 0;
//                 ret    = ecc__grib_get_long_array_internal(h, a, lval, size, &replen);
//             }
//             if (replen == 1)
//                 fprintf(out, myformat, lval[0]);
//             else {
//                 int i    = 0;
//                 int cols = 0;
//                 for (i = 0; i < replen; i++) {
//                     *newline = 1;
//                     fprintf(out, myformat, lval[i]);
//                     if (i < replen - 1)
//                         fprintf(out, "%s", myseparator);
//                     cols++;
//                     if (cols >= maxcols) {
//                         fprintf(out, "\n");
//                         *newline = 1;
//                         cols     = 0;
//                     }
//                 }
//             }
//             grib_context_free(h->context, lval);
//             break;
//         case GRIB_TYPE_BYTES:
//             replen = a->length;
//             sval   = (char*)grib_context_malloc(h->context, replen * sizeof(char));
//             ret    = a->unpack_string(sval, &replen);
//             p      = sval;
//             while ((replen--) > 0)
//                 fprintf(out, "%c", *(p++));
//             grib_context_free(h->context, sval);
//             *newline = 0;
//             break;
//         default:
//             grib_context_log(h->context, GRIB_LOG_WARNING, "grib_accessor_print: Problem to print \"%s\", invalid type %d", a->name, type);
//     }
//     return ret;
// }

int grib_accessors_list_print(grib_handle* h, grib_accessors_list* al, const char* name,
                              int type, const char* format, const char* separator, int equal,
                              int maxcols, int* newline, FILE* out)
{
    size_t size = 0, len = 0, replen = 0, j = 0;
    unsigned char* bval      = NULL;
    double* dval             = 0;
    long* lval               = 0;
    char** cvals             = NULL;
    int ret                  = 0;
    char* myformat           = NULL;
    char* myseparator        = NULL;
    char double_format[]     = "%.12g"; /* default format for printing double keys */
    char long_format[]       = "%ld";   /* default format for printing integer keys */
    char default_separator[] = " ";
    grib_accessor* a         = al->accessor;
    DEBUG_ASSERT(a);

    /* Number of columns specified as 0 means print on ONE line i.e. num cols = infinity */
    if (maxcols == 0)
        maxcols = INT_MAX;

    if (equal) fprintf(out, "%s=", name); // ECC-1878

    if (type == -1)
        type = al->accessor->get_native_type();
    al->value_count(&size);
    switch (type) {
        case GRIB_TYPE_STRING:
            myseparator = separator ? (char*)separator : default_separator;
            if (size == 1) {
                char sbuf[1024] = {0,};
                len = sizeof(sbuf);
                ret = al->accessor->unpack_string(sbuf, &len);
                if (grib_is_missing_string(al->accessor, (unsigned char*)sbuf, len)) {
                    fprintf(out, "%s", "MISSING");
                }
                else {
                    fprintf(out, "%s", sbuf);
                }
            }
            else {
                int cols = 0;
                j = 0;
                cvals    = (char**)grib_context_malloc_clear(h->context, sizeof(char*) * size);
                al->unpack_string(cvals, &size);
                for (j = 0; j < size; j++) {
                    *newline = 1;
                    fprintf(out, "%s", cvals[j]);
                    if (j < size - 1)
                        fprintf(out, "%s", myseparator);
                    cols++;
                    if (cols >= maxcols) {
                        fprintf(out, "\n");
                        *newline = 1;
                        cols     = 0;
                    }
                    grib_context_free(h->context, cvals[j]);
                }
            }
            grib_context_free(h->context, cvals);
            break;
        case GRIB_TYPE_DOUBLE:
            myformat    = format ? (char*)format : double_format;
            myseparator = separator ? (char*)separator : default_separator;
            dval        = (double*)grib_context_malloc_clear(h->context, sizeof(double) * size);
            ret         = al->unpack_double(dval, &size);
            if (size == 1)
                fprintf(out, myformat, dval[0]);
            else {
                int cols = 0;
                j = 0;
                for (j = 0; j < size; j++) {
                    *newline = 1;
                    fprintf(out, myformat, dval[j]);
                    if (j < size - 1)
                        fprintf(out, "%s", myseparator);
                    cols++;
                    if (cols >= maxcols) {
                        fprintf(out, "\n");
                        *newline = 1;
                        cols     = 0;
                    }
                }
            }
            grib_context_free(h->context, dval);
            break;
        case GRIB_TYPE_LONG:
            myformat    = format ? (char*)format : long_format;
            myseparator = separator ? (char*)separator : default_separator;
            lval        = (long*)grib_context_malloc_clear(h->context, sizeof(long) * size);
            ret         = al->unpack_long(lval, &size);
            if (size == 1)
                fprintf(out, myformat, lval[0]);
            else {
                int cols = 0;
                j = 0;
                for (j = 0; j < size; j++) {
                    *newline = 1;
                    fprintf(out, myformat, lval[j]);
                    if (j < size - 1)
                        fprintf(out, "%s", myseparator);
                    cols++;
                    if (cols >= maxcols) {
                        fprintf(out, "\n");
                        *newline = 1;
                        cols     = 0;
                    }
                }
            }
            grib_context_free(h->context, lval);
            break;
        case GRIB_TYPE_BYTES:
            replen = a->length_;
            bval   = (unsigned char*)grib_context_malloc(h->context, replen * sizeof(unsigned char));
            ret    = al->accessor->unpack_bytes(bval, &replen);
            for (j = 0; j < replen; j++) {
                fprintf(out, "%02x", bval[j]);
            }
            grib_context_free(h->context, bval);
            *newline = 1;
            break;
        default:
            grib_context_log(h->context, GRIB_LOG_WARNING,
                             "Accessor print: Problem printing \"%s\", invalid type %d", a->name_, grib_get_type_name(type));
    }
    return ret;
}

int grib_recompose_print(grib_handle* h, grib_accessor* observer, const char* uname, int fail, FILE* out)
{
    grib_accessors_list* al = NULL;
    char loc[1024];
    int i           = 0;
    int ret         = 0;
    int mode        = -1;
    char* pp        = NULL;
    char* format    = NULL;
    int type        = -1;
    char* separator = NULL;
    int equal = 0; // See ECC-1878
    int l;
    char buff[10] = {0,};
    char buff1[1024] = {0,};
    int maxcolsd = 8;
    int maxcols;
    long numcols           = 0;
    int newline            = 1;
    const size_t uname_len = strlen(uname);

    maxcols = maxcolsd;
    loc[0]  = 0;
    for (i = 0; i < uname_len; i++) {
        if (mode > -1) {
            switch (uname[i]) {
                case ':':
                    type = grib_type_to_int(uname[i + 1]);
                    i++;
                    break;
                case '\'':
                    pp = (char*)(uname + i + 1);
                    while (*pp != '%' && *pp != '!' && *pp != ']' && *pp != ':' && *pp != '\'')
                        pp++;
                    l = pp - uname - i;
                    if (*pp == '\'')
                        separator = strncpy(buff1, uname + i + 1, l - 1);
                    i += l;
                    break;
                case '%':
                    pp = (char*)(uname + i + 1);
                    while (*pp != '%' && *pp != '!' && *pp != ']' && *pp != ':' && *pp != '\'')
                        pp++;
                    l      = pp - uname - i;
                    format = strncpy(buff, uname + i, l);
                    i += l - 1;
                    break;
                case '!':
                    pp = (char*)uname;
                    // Turn off strict as the input string will have a final ']' suffix
                    if (string_to_long(uname + i + 1, &numcols, /*strict=*/0) == GRIB_SUCCESS) {
                        maxcols = (int)numcols;
                    }
                    else {
                        /* Columns specification is invalid integer */
                        maxcols = maxcolsd;
                    }
                    strtol(uname + i + 1, &pp, 10);
                    while (pp && *pp != '%' && *pp != '!' && *pp != ']' && *pp != ':' && *pp != '\'')
                        pp++;
                    i += pp - uname - i - 1;
                    break;
                case ']':
                    // ECC-1878: The '=' format specifier
                    if (loc[mode - 1] == '=') { loc[mode-1] = 0; equal = 1; }
                    else                      { loc[mode] = 0; }
                    mode = -1;
                    if (al) grib_accessors_list_delete(h->context, al);
                    al        = grib_find_accessors_list(h, loc); /* This allocates memory */
                    if (!al) {
                        if (!fail) {
                            fprintf(out, "undef");
                            ret = GRIB_NOT_FOUND;
                        }
                        else {
                            grib_context_log(h->context, GRIB_LOG_WARNING,
                            "Recompose print: Problem recomposing print with : %s, no accessor found", loc);
                            return GRIB_NOT_FOUND;
                        }
                    }
                    else {
                        ret = grib_accessors_list_print(h, al, loc, type, format, separator, equal, maxcols, &newline, out);

                        if (ret != GRIB_SUCCESS) {
                            /* grib_context_log(h->context, GRIB_LOG_ERROR,"grib_recompose_print: Could not recompose print : %s", uname); */
                            grib_accessors_list_delete(h->context, al);
                            return ret;
                        }
                    }
                    loc[0] = 0;
                    break;
                default:
                    loc[mode++] = uname[i];
                    break;
            }
        }
        else if (uname[i] == '[') {
            mode = 0;
        }
        else {
            fprintf(out, "%c", uname[i]);
            type = -1;
        }
    }
    if (newline)
        fprintf(out, "\n");

    grib_accessors_list_delete(h->context, al);
    return ret;
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

grib_action_file* grib_find_action_file(const char* fname, grib_action_file_list* afl)
{
    grib_action_file* act = afl->first;
    while (act) {
        if (grib_inline_strcmp(act->filename, fname) == 0)
            return act;
        act = act->next;
    }
    return 0;
}

static void grib_push_action_file(grib_action_file* af, grib_action_file_list* afl)
{
    if (!afl->first)
        afl->first = afl->last = af;
    else
        afl->last->next = af;
    afl->last = af;
}

#define MAXINCLUDE 10

typedef struct
{
    char* name;
    FILE* file;
    char* io_buffer;
    int line;
} context;

static context stack[MAXINCLUDE];
static int top = 0;
extern FILE* grib_yyin;
extern int grib_yylineno;
extern void grib_yyrestart(FILE*);
static int error = 0;

int grib_yywrap()
{
    /* int i; */
    top--;

    /* for(i = 0; i < top ; i++) printf("   "); */
    /* printf("CLOSE %s\n",parse_file); */

    fclose(stack[top].file);
    /* if (stack[top].io_buffer) free(stack[top].io_buffer); */

    grib_yylineno = stack[top].line;

    if (top) {
        parse_file = stack[top - 1].name;
        grib_yyin  = stack[top - 1].file;
        ECCODES_ASSERT(parse_file);
        ECCODES_ASSERT(grib_yyin);
        /* grib_yyrestart(grib_yyin); */

        /* for(i = 0; i < top ; i++) printf("   "); */
        /* printf("BACK TO %s\n",parse_file); */

        grib_context_free(grib_parser_context, stack[top].name);
        return 0;
    }
    else {
        grib_context_free(grib_parser_context, stack[top].name);
        parse_file = 0;
        grib_yyin  = NULL;
        return 1;
    }
}

char* file_being_parsed()
{
    return (char*)parse_file;
}

int grib_yyerror(const char* msg)
{
    grib_context_log(grib_parser_context, GRIB_LOG_ERROR,
                     "Parser: %s at line %d of %s", msg, grib_yylineno + 1, parse_file);
    grib_context_log(grib_parser_context, GRIB_LOG_ERROR,
                     "ecCodes Version: %s", ECCODES_VERSION_STR);
    error = 1;
    return 1;
}

void grib_parser_include(const char* included_fname)
{
    FILE* f         = NULL;
    char* io_buffer = 0;
    /* int i; */
    ECCODES_ASSERT(top < MAXINCLUDE);
    ECCODES_ASSERT(included_fname);
    if (!included_fname)
        return;

    if (parse_file == 0) {
        parse_file = included_fname;
        ECCODES_ASSERT(top == 0);
    }
    else {
        /* When parse_file is not NULL, it's the path of the parent file (includer) */
        /* and 'included_fname' is the name of the file being included (includee) */

        /* GRIB-796: Search for the included file in ECCODES_DEFINITION_PATH */
        char* new_path = NULL;
        ECCODES_ASSERT(*included_fname != '/');
        new_path = grib_context_full_defs_path(grib_parser_context, included_fname);
        if (!new_path) {
            fprintf(stderr, "ecCodes Version:       %s\nDefinition files path: %s\n",
                    ECCODES_VERSION_STR,
                    grib_parser_context->grib_definition_files_path);

            grib_context_log(grib_parser_context, GRIB_LOG_FATAL,
                             "Parser include: Could not resolve '%s' (included in %s)", included_fname, parse_file);

            return;
        }
        parse_file = new_path;
    }

    if (strcmp(parse_file, "-") == 0) {
        grib_context_log(grib_parser_context, GRIB_LOG_DEBUG, "parsing standard input");
        f = stdin; /* read from std input */
    }
    else {
        grib_context_log(grib_parser_context, GRIB_LOG_DEBUG, "parsing include file %s", parse_file);
        f = codes_fopen(parse_file, "r");
    }
    /* for(i = 0; i < top ; i++) printf("   "); */
    /* printf("PARSING %s\n",parse_file); */

    if (f == NULL) {
        char buffer[1024];
        grib_context_log(grib_parser_context, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR), "Parser include: cannot open: '%s'", parse_file);
        snprintf(buffer, sizeof(buffer), "Cannot include file: '%s'", parse_file);
        grib_yyerror(buffer);
    }
    else {
        /*
        c=grib_context_get_default();
        if (c->io_buffer_size) {
            if (posix_memalign(&(io_buffer),sysconf(_SC_PAGESIZE),c->io_buffer_size) ) {
                        grib_context_log(c,GRIB_LOG_FATAL,"grib_parser_include: posix_memalign unable to allocate io_buffer\n");
            }
            setvbuf(f,io_buffer,_IOFBF,c->io_buffer_size);
        }
        */
        grib_yyin            = f;
        stack[top].file      = f;
        stack[top].io_buffer = io_buffer;
        stack[top].name      = grib_context_strdup(grib_parser_context, parse_file);
        parse_file           = stack[top].name;
        stack[top].line      = grib_yylineno;
        grib_yylineno        = 0;
        top++;
        /* grib_yyrestart(f); */
    }
}

extern int grib_yyparse(void);

static int parse(grib_context* gc, const char* filename)
{
    int err = 0;
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_parse);

#ifdef YYDEBUG
    {
        extern int grib_yydebug;
        grib_yydebug = getenv("YYDEBUG") != NULL;
    }
#endif

    gc = gc ? gc : grib_context_get_default();

    grib_yyin  = NULL;
    top        = 0;
    parse_file = 0;
    grib_parser_include(filename);
    if (!grib_yyin) {
        /* Could not read from file */
        parse_file = 0;
        GRIB_MUTEX_UNLOCK(&mutex_parse);
        return GRIB_FILE_NOT_FOUND;
    }
    err        = grib_yyparse();
    parse_file = 0;

    if (err)
        grib_context_log(gc, GRIB_LOG_ERROR, "Parsing error: %s, file: %s\n",
                grib_get_error_message(err), filename);

    GRIB_MUTEX_UNLOCK(&mutex_parse);
    return err;
}

static grib_action* grib_parse_stream(grib_context* gc, const char* filename)
{
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_stream);

    grib_parser_all_actions = 0;

    if (parse(gc, filename) == 0) {
        if (grib_parser_all_actions) {
            GRIB_MUTEX_UNLOCK(&mutex_stream);
            return grib_parser_all_actions;
        }
        else {
            grib_action* ret = grib_action_create_noop(gc, filename);
            GRIB_MUTEX_UNLOCK(&mutex_stream);
            return ret;
        }
    }
    else {
        GRIB_MUTEX_UNLOCK(&mutex_stream);
        return NULL;
    }
}

grib_concept_value* grib_parse_concept_file(grib_context* gc, const char* filename)
{
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_file);

    gc                  = gc ? gc : grib_context_get_default();
    grib_parser_context = gc;

    if (parse(gc, filename) == 0) {
        GRIB_MUTEX_UNLOCK(&mutex_file);
        return grib_parser_concept;
    }
    else {
        GRIB_MUTEX_UNLOCK(&mutex_file);
        return NULL;
    }
}

grib_hash_array_value* grib_parse_hash_array_file(grib_context* gc, const char* filename)
{
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_file);

    gc                  = gc ? gc : grib_context_get_default();
    grib_parser_context = gc;

    if (parse(gc, filename) == 0) {
        GRIB_MUTEX_UNLOCK(&mutex_file);
        return grib_parser_hash_array;
    }
    else {
        GRIB_MUTEX_UNLOCK(&mutex_file);
        return NULL;
    }
}

// grib_rule* grib_parse_rules_file(grib_context* gc, const char* filename)
// {
//     if (!gc) gc = grib_context_get_default();
//     GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
//     GRIB_MUTEX_LOCK(&mutex_rules);
//     gc                  = gc ? gc : grib_context_get_default();
//     grib_parser_context = gc;
//     if (parse(gc, filename) == 0) {
//         GRIB_MUTEX_UNLOCK(&mutex_rules);
//         return grib_parser_rules;
//     }
//     else {
//         GRIB_MUTEX_UNLOCK(&mutex_rules);
//         return NULL;
//     }
// }

grib_action* grib_parse_file(grib_context* gc, const char* filename)
{
    grib_action_file* af;

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_file);

    af = 0;

    gc = gc ? gc : grib_context_get_default();

    grib_parser_context = gc;

    if (!gc->grib_reader)
        gc->grib_reader = (grib_action_file_list*)grib_context_malloc_clear_persistent(gc, sizeof(grib_action_file_list));
    else {
        af = grib_find_action_file(filename, gc->grib_reader);
    }

    if (!af) {
        grib_action* a;
        grib_context_log(gc, GRIB_LOG_DEBUG, "Loading %s", filename);

        a = grib_parse_stream(gc, filename);

        if (error) {
            if (a) {
                delete a;
            }
            GRIB_MUTEX_UNLOCK(&mutex_file);
            return NULL;
        }

        af = (grib_action_file*)grib_context_malloc_clear_persistent(gc, sizeof(grib_action_file));

        af->root = a;

        af->filename = grib_context_strdup_persistent(gc, filename);
        grib_push_action_file(af, gc->grib_reader); /* Add af to grib_reader action file list */
    }
    else
        grib_context_log(gc, GRIB_LOG_DEBUG, "Using cached version of %s", filename);

    GRIB_MUTEX_UNLOCK(&mutex_file);
    return af->root;
}

int grib_type_to_int(char id)
{
    switch (id) {
        case 'd':
            return GRIB_TYPE_DOUBLE;
        case 'f':
            return GRIB_TYPE_DOUBLE;
        case 'l':
            return GRIB_TYPE_LONG;
        case 'i':
            return GRIB_TYPE_LONG;
        case 's':
            return GRIB_TYPE_STRING;
    }
    return GRIB_TYPE_UNDEFINED;
}
