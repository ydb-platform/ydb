/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

/**
    include private headers used for all internal functions of
    ecCodes, not seen by the user of the API
  */

#ifndef grib_api_internal_H
#define grib_api_internal_H

#ifdef __cplusplus
extern "C" {
#endif

/* cmake config header */
#ifdef HAVE_ECCODES_CONFIG_H
    #include "eccodes_config.h"
#endif

/* autoconf config header */
#ifdef HAVE_CONFIG_H
    #error #include "config.h"
    #ifdef _LARGE_FILES
        #undef _LARGE_FILE_API
    #endif
#endif

#ifndef GRIB_INLINE
    #define GRIB_INLINE
#endif

/* See ECC-670 */
#if IS_BIG_ENDIAN
    #if GRIB_MEM_ALIGN
        #define FAST_BIG_ENDIAN 1
    #else
        #define FAST_BIG_ENDIAN 0
    #endif
#endif

#if IEEE_BE
    #define IEEE
#else
    #if IEEE_LE
        #define IEEE
    #endif
#endif

#if defined(_WIN32) && defined(_MSC_VER)
    #define _CRT_SECURE_NO_WARNINGS
    #define _CRT_NONSTDC_NO_DEPRECATE
#endif

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "eccodes_windef.h"

#ifndef ECCODES_ON_WINDOWS
    #include <dirent.h>
    #include <unistd.h>
    #include <inttypes.h>
#else
    #define strtok_r strtok_s
    #include <direct.h>
    #include <io.h>

    /* Replace C99/Unix rint() for Windows Visual C++ (only before VC++ 2013 versions) */
    #if defined _MSC_VER && _MSC_VER < 1800
double rint(double x);
    #endif

    #ifndef S_ISREG
        #define S_ISREG(mode) (mode & S_IFREG)
    #endif

    #ifndef S_ISDIR
        #define S_ISDIR(mode) (mode & S_IFDIR)
    #endif

    #ifndef M_PI
        #define M_PI 3.14159265358979323846
    #endif

    #define R_OK 04 /* Needed for Windows */

    #ifndef F_OK
        #define F_OK 0
    #endif

    #define mkdir(dirname, mode) _mkdir(dirname)

    #ifdef _MSC_VER
        #define access(path, mode) _access(path, mode)
        #define chmod(path, mode)  _chmod(path, mode)
        #define strdup(str)        _strdup(str)
    #endif

#endif /* ifndef ECCODES_ON_WINDOWS */


#include <limits.h>
#include <errno.h>
#include <stdlib.h>
#include <ctype.h>


#ifdef HAVE_STRING_H
    #include <string.h>
#else
    #include <strings.h>
#endif

/*
#if GRIB_LINUX_PTHREADS
extern int pthread_mutexattr_settype(pthread_mutexattr_t* attr, int type);
#endif
*/

#if GRIB_PTHREADS
    #include <pthread.h>
    #define GRIB_MUTEX_INIT_ONCE(a, b) pthread_once(a, b);
    #define GRIB_MUTEX_LOCK(a)         pthread_mutex_lock(a);
    #define GRIB_MUTEX_UNLOCK(a)       pthread_mutex_unlock(a);
/*
 #define GRIB_MUTEX_LOCK(a) {pthread_mutex_lock(a); printf("MUTEX LOCK %p %s line %d\n",(void*)a,__FILE__,__LINE__);}
 #define GRIB_MUTEX_UNLOCK(a) {pthread_mutex_unlock(a);printf("MUTEX UNLOCK %p %s line %d\n",(void*)a,__FILE__,__LINE__);}
 */
#elif GRIB_OMP_THREADS
    #include <omp.h>
    #ifdef _MSC_VER
        #define GRIB_OMP_CRITICAL(a) __pragma(omp critical(a))
    #else
        #define GRIB_OMP_STR(a)      #a
        #define GRIB_OMP_XSTR(a)     GRIB_OMP_STR(a)
        #define GRIB_OMP_CRITICAL(a) _Pragma(GRIB_OMP_XSTR(omp critical(a)))
    #endif
    #define GRIB_MUTEX_INIT_ONCE(a, b) (*(b))();
    #define GRIB_MUTEX_LOCK(a)         omp_set_nest_lock(a);
    #define GRIB_MUTEX_UNLOCK(a)       omp_unset_nest_lock(a);
#else
    #define GRIB_MUTEX_INIT_ONCE(a, b)
    #define GRIB_MUTEX_LOCK(a)
    #define GRIB_MUTEX_UNLOCK(a)
#endif

#if GRIB_LINUX_PTHREADS
    /* Note: in newer pthreads PTHREAD_MUTEX_RECURSIVE and PTHREAD_MUTEX_RECURSIVE_NP are enums */
    #if !defined(PTHREAD_MUTEX_RECURSIVE)
        #define PTHREAD_MUTEX_RECURSIVE PTHREAD_MUTEX_RECURSIVE_NP
    #endif
#endif


#ifndef HAVE_FSEEKO
    #define fseeko fseek
    #define ftello ftell
#endif

#define ECCODES_ASSERT(a)                                                 \
    do {                                                          \
        if (!(a)) codes_assertion_failed(#a, __FILE__, __LINE__); \
    } while (0)

#ifdef DEBUG
    #define DEBUG_ASSERT(a) ECCODES_ASSERT(a)
    #define DEBUG_ASSERT_ACCESS(array, index, size)                                                                           \
        do {                                                                                                                  \
            if (!((index) >= 0 && (index) < (size))) {                                                                        \
                printf("ARRAY ACCESS ERROR: array=%s idx=%ld size=%ld @ %s +%d \n", #array, index, size, __FILE__, __LINE__); \
                abort();                                                                                                      \
            }                                                                                                                 \
        } while (0)
#else
    #define DEBUG_ASSERT(a)
    #define DEBUG_ASSERT_ACCESS(array, index, size)
#endif

/* Return true if two strings are equal */
#define STR_EQUAL(a, b) (strcmp((a), (b)) == 0)
/* Return true if two strings are equal, ignoring case */
#define STR_EQUAL_NOCASE(a, b) (strcmp_nocase((a), (b)) == 0)

#include "grib_api.h"

#define MAX_ACCESSOR_ATTRIBUTES     20
#define MAX_FILE_HANDLES_WITH_MULTI 10
#define ACCESSORS_ARRAY_SIZE        5000
#define MAX_NUM_CONCEPTS            2000
#define MAX_NUM_HASH_ARRAY          2000

#define CODES_NAMESPACE   10
#define MAX_NAMESPACE_LEN 64

#define CODES_MY_BUFFER   0
#define CODES_USER_BUFFER 1

#define CODES_REAL_MODE8 8

#define MAX_NUM_SECTIONS 12

#define GRIB_LOG_PERROR (1 << 10)

#define GRIB_HASH_ARRAY_TYPE_UNKNOWN 0
#define GRIB_HASH_ARRAY_TYPE_INTEGER 1
#define GRIB_HASH_ARRAY_TYPE_DOUBLE  2
#define GRIB_HASH_ARRAY_TYPE_STRING  3

#define CODES_GRIB  1
#define CODES_BUFR  2
#define CODES_METAR 3
#define CODES_GTS   4

#define CODES_BUFR_UNPACK_STRUCTURE 0
#define CODES_BUFR_UNPACK_FLAT      1
#define CODES_BUFR_NEW_DATA         2

#define MAX_SMART_TABLE_COLUMNS 20
#define MAX_CODETABLE_ENTRIES   65536

/* ACCESSOR COMPARE FLAGS */
#define GRIB_COMPARE_NAMES (1 << 0)
#define GRIB_COMPARE_TYPES (1 << 1)

extern const int max_nbits;

// typedef struct grib_expression grib_expression;
//
namespace eccodes {
class Expression;
class Arguments;
class Action;
}  // namespace eccodes
using grib_expression = eccodes::Expression;
using grib_arguments  = eccodes::Arguments;
using grib_action = eccodes::Action;

typedef struct grib_action_file grib_action_file;
typedef struct grib_action_file_list grib_action_file_list;
typedef struct grib_block_of_accessors grib_block_of_accessors;
typedef struct grib_buffer grib_buffer;
class grib_accessor_class;
typedef struct grib_section grib_section;
typedef struct grib_codetable grib_codetable;
typedef struct grib_smart_table grib_smart_table;

class grib_accessor;
namespace eccodes::geo_iterator {
class Iterator;
}

typedef struct grib_iterator
{
    eccodes::geo_iterator::Iterator* iterator;
} grib_iterator;

typedef struct grib_dependency grib_dependency;

typedef struct codes_condition codes_condition;

/* typedef void (*dynamic_key_proc) (const char*, void*) */

typedef int (*grib_pack_proc)(grib_handle* h, const double* in, size_t inlen, void* out, size_t* outlen);
typedef int (*grib_unpack_proc)(grib_handle* h, const void* in, size_t inlen, double* out, size_t* outlen);


typedef long (*grib_binop_long_proc)(long, long);
typedef long (*grib_unop_long_proc)(long);

typedef double (*grib_binop_double_proc)(double, double);
typedef double (*grib_unop_double_proc)(double);

typedef int (*grib_binop_string_proc)(char*, char*);

struct grib_key_value_list
{
    const char* name;
    int type;
    int size;
    long* long_value;
    double* double_value;
    grib_key_value_list* namespace_value;
    char* string_value;
    int has_value;
    int error;
    grib_key_value_list* next;
};

/* struct second_order_packed
{
    unsigned long nbits_per_widths;
    unsigned long nbits_per_group_size;
    size_t size_of_group_array;
    size_t packed_byte_count;
    unsigned long* array_of_group_size;
    unsigned long* array_of_group_width;
    long* array_of_group_refs;
}; */

/* --------------- */
typedef struct grib_loader grib_loader;
typedef int (*grib_loader_init_accessor_proc)(grib_loader*, grib_accessor*, grib_arguments*);
typedef int (*grib_loader_lookup_long_proc)(grib_context*, grib_loader*, const char* name, long* value);

struct grib_loader
{
    void* data;
    grib_loader_init_accessor_proc init_accessor;
    grib_loader_lookup_long_proc lookup_long;
    int list_is_resized; /** will be true if we resize a list */
    int changing_edition;
};

class grib_accessors_list;

/**
 *  A buffer
 *  Structure containing the data of a message
 */
struct grib_buffer
{
    int property;        /** < property parameter of buffer */
    int validity;        /** < validity parameter of buffer */
    int growable;        /** < buffer can be grown */
    size_t length;       /** < Buffer length */
    size_t ulength;      /** < length used of the buffer */
    size_t ulength_bits; /** < length used of the buffer in bits */
    unsigned char* data; /** < the data byte array */
};

/**
 *  An accessor
 *  Structure supporting each single data unit and allowing its access
 *  @see  grib_accessor_class
 */

#define MAX_ACCESSOR_NAMES 20

typedef struct grib_virtual_value grib_virtual_value;

struct grib_virtual_value
{
    long lval;
    double dval;
    char* cval;
    int missing;
    int length;
    int type;
};

#define GRIB_ACCESSOR_FLAG_READ_ONLY                (1 << 1)
#define GRIB_ACCESSOR_FLAG_DUMP                     (1 << 2)
#define GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC         (1 << 3)
#define GRIB_ACCESSOR_FLAG_CAN_BE_MISSING           (1 << 4)
#define GRIB_ACCESSOR_FLAG_HIDDEN                   (1 << 5)
#define GRIB_ACCESSOR_FLAG_CONSTRAINT               (1 << 6)
#define GRIB_ACCESSOR_FLAG_BUFR_DATA                (1 << 7)
#define GRIB_ACCESSOR_FLAG_NO_COPY                  (1 << 8)
#define GRIB_ACCESSOR_FLAG_COPY_OK                  (1 << 9)
#define GRIB_ACCESSOR_FLAG_FUNCTION                 (1 << 10)
#define GRIB_ACCESSOR_FLAG_DATA                     (1 << 11)
#define GRIB_ACCESSOR_FLAG_NO_FAIL                  (1 << 12)
#define GRIB_ACCESSOR_FLAG_TRANSIENT                (1 << 13)
#define GRIB_ACCESSOR_FLAG_STRING_TYPE              (1 << 14)
#define GRIB_ACCESSOR_FLAG_LONG_TYPE                (1 << 15)
#define GRIB_ACCESSOR_FLAG_DOUBLE_TYPE              (1 << 16)
#define GRIB_ACCESSOR_FLAG_LOWERCASE                (1 << 17)
#define GRIB_ACCESSOR_FLAG_BUFR_COORD               (1 << 18)
#define GRIB_ACCESSOR_FLAG_COPY_IF_CHANGING_EDITION (1 << 19)
#define GRIB_ACCESSOR_FLAG_COPY_AS_INT              (1 << 20)

/**
 *  A section accessor
 *  Structure supporting hierarchical naming of the accessors
 *  @see  grib_accessor
 */
struct grib_section
{
    grib_accessor* owner;
    grib_handle* h;                 /** < Handles of all accessors and buffer  */
    grib_accessor* aclength;        /** < block of the length of the block */
    grib_block_of_accessors* block; /** < block */
    grib_action* branch;            /** < branch that created the block */
    size_t length;
    size_t padding;
};

namespace eccodes::geo_nearest {
class Nearest;
}

typedef struct grib_nearest
{
    eccodes::geo_nearest::Nearest* nearest;
} grib_nearest;

/* --------------- */

struct grib_dependency
{
    grib_dependency* next;
    grib_accessor* observed;
    grib_accessor* observer;
    int run;
};

struct grib_block_of_accessors
{
    grib_accessor* first;
    grib_accessor* last;
};


typedef struct grib_trie grib_trie;
typedef struct grib_trie_with_rank_list grib_trie_with_rank_list;
typedef struct grib_trie_with_rank grib_trie_with_rank;
typedef struct grib_itrie grib_itrie;

/* Dynamic array of strings */
struct grib_sarray
{
    char** v;
    size_t size; /* capacity */
    size_t n;    /* used size */
    size_t incsize;
};

/* Dynamic array of objects (void*) */
struct grib_oarray
{
    void** v;
    size_t size; /* capacity */
    size_t n;    /* used size */
    size_t incsize;
};

/* Dynamic array of doubles */
struct grib_darray
{
    double* v;
    size_t size; /* capacity */
    size_t n;    /* used size */
    size_t incsize;
};

/* Dynamic array of integers (long) */
struct grib_iarray
{
    long* v;
    size_t size; /* capacity */
    size_t n;    /* used size */
    size_t incsize;
    size_t number_of_pop_front;
};

/* Dynamic array of double arrays */
struct grib_vdarray
{
    grib_darray** v;
    size_t size; /* capacity */
    size_t n;    /* used size */
    size_t incsize;
};

/* Dynamic array of string arrays */
struct grib_vsarray
{
    grib_sarray** v;
    size_t size; /* capacity */
    size_t n;    /* used size */
    size_t incsize;
};

/* Dynamic array of integer arrays */
struct grib_viarray
{
    grib_iarray** v;
    size_t size; /* capacity */
    size_t n;    /* used size */
    size_t incsize;
};

/* types of BUFR descriptors used in bufr_descriptor->type*/
#define BUFR_DESCRIPTOR_TYPE_UNKNOWN     0
#define BUFR_DESCRIPTOR_TYPE_STRING      1
#define BUFR_DESCRIPTOR_TYPE_DOUBLE      2
#define BUFR_DESCRIPTOR_TYPE_LONG        3
#define BUFR_DESCRIPTOR_TYPE_TABLE       4
#define BUFR_DESCRIPTOR_TYPE_FLAG        5
#define BUFR_DESCRIPTOR_TYPE_REPLICATION 6
#define BUFR_DESCRIPTOR_TYPE_OPERATOR    7
#define BUFR_DESCRIPTOR_TYPE_SEQUENCE    8

struct bufr_descriptor
{
    grib_context* context;
    long code;
    int F;
    int X;
    int Y;
    int type;
    /*char* name;   Not needed: All usage commented out. See ECC-489 */
    char shortName[128];
    char units[64];
    long scale;
    double factor;
    long reference;
    long width;
    int nokey; /* set if descriptor does not have an associated key */
    grib_accessor* a;
};

struct bufr_descriptors_array
{
    bufr_descriptor** v;
    size_t size;
    size_t n;
    size_t incsize;
    size_t number_of_pop_front;
    grib_context* context;
};

struct bufr_descriptors_map_list
{
    bufr_descriptors_array* unexpanded;
    bufr_descriptors_array* expanded;
    bufr_descriptors_map_list* next;
};

/* BUFR: Operator 203YYY: Linked list storing Table B changed reference values */
typedef struct bufr_tableb_override bufr_tableb_override;
struct bufr_tableb_override
{
    bufr_tableb_override* next;
    int code;
    long new_ref_val;
};

struct codes_condition
{
    char* left;
    int rightType;
    char* rightString;
    long rightLong;
    double rightDouble;
};

void codes_assertion_failed(const char* message, const char* file, int line);

#define MAX_SET_VALUES     10
#define MAX_ACCESSOR_CACHE 100

struct grib_handle
{
    grib_context* context;         /** < context attached to this handle */
    grib_buffer* buffer;           /** < buffer attached to the handle */
    grib_section* root;            /**  the root section*/
    grib_dependency* dependencies; /** List of dependencies */
    grib_handle* main;             /** Used during reparsing */
    grib_handle* kid;              /** Used during reparsing */
    grib_loader* loader;           /** Used during reparsing */
    int values_stack;
    const grib_values* values[MAX_SET_VALUES]; /** Used when setting multiple values at once */
    size_t values_count[MAX_SET_VALUES];       /** Used when setting multiple values at once */
    int partial;                               /** Not a complete message (just headers) */
    int header_mode;                           /** Header not jet complete */
    char* gts_header;
    size_t gts_header_len;
    int use_trie;
    int trie_invalid;
    grib_accessor* accessors[ACCESSORS_ARRAY_SIZE];
    char* section_offset[MAX_NUM_SECTIONS];
    char* section_length[MAX_NUM_SECTIONS];
    int sections_count;
    off_t offset;
    /* grib_accessor* groups[MAX_NUM_GROUPS]; */
    ProductKind product_kind;
    /* grib_trie* bufr_elements_table; */
};

/* For GRIB2 multi-field messages */
struct grib_multi_handle
{
    grib_context* context; /** < context attached to this handle */
    grib_buffer* buffer;   /** < buffer attached to the handle */
    size_t offset;
    size_t length;
};


typedef struct grib_multi_support grib_multi_support;
struct grib_multi_support
{
    FILE* file;
    size_t offset;
    unsigned char* message;
    size_t message_length;
    unsigned char* sections[8];
    unsigned char* bitmap_section;
    size_t bitmap_section_length;
    size_t sections_length[9]; /* GRIB2 has 9 sections */
    int section_number;
    grib_multi_support* next;
};

/* Hash_array */
typedef struct grib_hash_array_value grib_hash_array_value;
struct grib_hash_array_value
{
    grib_hash_array_value* next;
    char* name;
    int type;
    grib_iarray* iarray;
    grib_darray* darray;
    grib_trie* index;
};

/* Concepts */
typedef struct grib_concept_condition grib_concept_condition;
struct grib_concept_condition
{
    grib_concept_condition* next;
    char* name;
    grib_expression* expression;
    grib_iarray* iarray;
};

typedef struct grib_concept_value_name grib_concept_value_name;
struct grib_concept_value_name
{
    grib_concept_value_name* next;
    char* name;
};

typedef struct grib_concept_value grib_concept_value;
struct grib_concept_value
{
    grib_concept_value* next;
    char* name;
    grib_concept_condition* conditions;
    grib_trie* index;
};

/* ----------*/
struct grib_context
{
    int inited;
    int debug;
    int write_on_fail;
    int no_abort;
    int io_buffer_size;
    int no_big_group_split;
    int no_spd;
    int keep_matrix;
    int grib_hourly_steps_with_units;
    char* grib_definition_files_path;
    char* grib_samples_path;
    char* grib_concept_path;

    grib_action_file_list* grib_reader;
    void* user_data;
    int real_mode;

    grib_free_proc free_mem;
    grib_malloc_proc alloc_mem;
    grib_realloc_proc realloc_mem;

    grib_free_proc free_persistent_mem;
    grib_malloc_proc alloc_persistent_mem;

    grib_free_proc free_buffer_mem;
    grib_malloc_proc alloc_buffer_mem;
    grib_realloc_proc realloc_buffer_mem;

    grib_data_read_proc read;
    grib_data_write_proc write;
    grib_data_tell_proc tell;
    grib_data_seek_proc seek;
    grib_data_eof_proc eof;

    grib_log_proc output_log;
    grib_print_proc print;

    grib_codetable* codetable;
    grib_smart_table* smart_table;
    char* outfilename;
    int multi_support_on;
    grib_multi_support* multi_support;
    grib_string_list* grib_definition_files_dir;
    int handle_file_count;
    int handle_total_count;
    off_t message_file_offset;
    int no_fail_on_wrong_length;
    int gts_header_on;
    int gribex_mode_on;
    int large_constant_fields;
    grib_itrie* keys;
    int keys_count;
    grib_itrie* concepts_index;
    int concepts_count;
    grib_concept_value* concepts[MAX_NUM_CONCEPTS];
    grib_itrie* hash_array_index;
    int hash_array_count;
    grib_hash_array_value* hash_array[MAX_NUM_HASH_ARRAY];
    grib_trie* def_files;
    grib_string_list* blocklist;
    int ieee_packing; /* 32 or 64 */
    int bufrdc_mode;
    int bufr_set_to_missing_if_out_of_range;
    int bufr_multi_element_constant_arrays;
    int grib_data_quality_checks;
    int single_precision;
    int eckit_geo;
    FILE* log_stream;
    grib_trie* classes;
    grib_trie* lists;
    grib_trie* expanded_descriptors;
    int file_pool_max_opened_files;
#if GRIB_PTHREADS
    pthread_mutex_t mutex;
#elif GRIB_OMP_THREADS
    omp_nest_lock_t mutex;
#endif
};

/* expression*/

// typedef int (*expression_evaluate_long_proc)(grib_expression*, grib_handle*, long*);
// typedef int (*expression_evaluate_double_proc)(grib_expression*, grib_handle*, double*);
// typedef const char* (*expression_evaluate_string_proc)(grib_expression*, grib_handle*, char*, size_t*, int*);
// typedef const char* (*expression_get_name_proc)(grib_expression*);

// typedef void (*expression_print_proc)(grib_context*, grib_expression*, grib_handle*, FILE*);
// typedef void (*expression_add_dependency_proc)(grib_expression* e, grib_accessor* observer);

// typedef struct grib_expression_class grib_expression_class;

// typedef void (*expression_class_init_proc)(grib_expression_class* e);
// typedef void (*expression_init_proc)(grib_expression* e);
// typedef void (*expression_destroy_proc)(grib_context*, grib_expression* e);

// typedef int (*expression_native_type_proc)(grib_expression*, grib_handle*);


// struct grib_expression
//{
//     grib_expression_class* cclass;
// };

// struct grib_expression_class
//{
//     grib_expression_class** super;
//     const char* name;
//     size_t size;
//     int inited;
//     expression_init_proc            init;
//     expression_destroy_proc         destroy;
//     expression_print_proc           print;
//     expression_add_dependency_proc  add_dependency;
//     expression_native_type_proc     native_type;
//     expression_get_name_proc        get_name;
//     expression_evaluate_long_proc   evaluate_long;
//     expression_evaluate_double_proc evaluate_double;
//     expression_evaluate_string_proc evaluate_string;
// };

// struct grib_arguments
//{
//     struct grib_arguments* next;
//     grib_expression* expression;
// };


// long grib_expression_evaluate(grib_handle*, grib_expression*);
// void grib_expression_free(grib_context*, grib_expression*);

grib_arguments* grib_arguments_new(grib_context*, grib_expression*, grib_arguments*);
void grib_arguments_free(grib_context*, grib_arguments*);

const char* grib_argument_next(grib_arguments**);

/* file_pool */
extern grib_string_list grib_file_not_found;

typedef struct grib_file grib_file;
typedef struct grib_file_pool grib_file_pool;

struct grib_file
{
    grib_context* context;
    char* name;
    FILE* handle;
    char* mode;
    char* buffer;
    long refcount;
    grib_file* next;
    grib_file* pool_file;
    long pool_file_refcount;
    short id;
};

struct grib_file_pool
{
    grib_context* context;
    grib_file* first;
    grib_file* current;
    size_t size;
    int number_of_opened_files;
    int max_opened_files;
};

/* fieldset */
typedef struct grib_field grib_field;
typedef struct grib_column grib_column;
typedef struct grib_fields grib_fields;
typedef struct grib_int_array grib_int_array;

struct grib_where
{
    char* string;
};

struct grib_column
{
    int refcount;
    char* name;
    int type;
    size_t size;
    size_t values_array_size;
    long* long_values;
    double* double_values;
    char** string_values;
    int* errors;
};

struct grib_order_by
{
    char* key;
    int idkey;
    int mode;
    grib_order_by* next;
};

#ifdef NEWDB
struct grib_query
{
    grib_context* context;
    char* where_string;
    grib_order_by* order_by;
};
#endif

struct grib_field
{
    grib_file* file;
    off_t offset;
    long length;
    grib_field* next;
};

struct grib_int_array
{
    grib_context* context;
    size_t size;
    int* el;
};

struct grib_fieldset
{
    grib_context* context;
    grib_int_array* filter;
    grib_int_array* order;
    size_t fields_array_size;
    size_t size;
    grib_column* columns;
    size_t columns_size;
    grib_where* where;
    grib_order_by* order_by;
    long current;
    grib_field** fields;
};

/* support for in-memory definition and tables */

extern int codes_memfs_exists(const char* path);
extern FILE* codes_memfs_open(const char* path);

/* index structures */

#define STRING_VALUE_LEN 100

typedef struct grib_field_tree grib_field_tree;

struct grib_field_tree
{
    grib_field* field;
    char* value;
    grib_field_tree* next;
    grib_field_tree* next_level;
};

typedef struct grib_index_key grib_index_key;

struct grib_index_key
{
    char* name;
    int type;
    char value[STRING_VALUE_LEN];
    grib_string_list* values;
    grib_string_list* current;
    int values_count;
    int count;
    grib_index_key* next;
};

typedef struct grib_field_list grib_field_list;
struct grib_field_list
{
    grib_field* field;
    grib_field_list* next;
};

struct grib_index
{
    grib_context* context;
    grib_index_key* keys;
    int rewind;
    int orderby;
    grib_index_key* orederby_keys;
    grib_field_tree* fields;
    grib_field_list* fieldset;
    grib_field_list* current;
    grib_file* files;
    int count;
    ProductKind product_kind;
    int unpack_bufr; /* Only meaningful for product_kind of BUFR */
};

/* header compute */
typedef struct grib_math grib_math;

struct grib_math
{
    struct grib_math* left;
    struct grib_math* right;
    char* name;
    int arity;
};

typedef double (*mathproc)(void);
typedef int (*funcproc)(grib_math*, mathproc);

typedef struct func
{
    char* name;
    funcproc addr;
    mathproc proc;
    int arity;
    char* info;
} func;

/* action file */
struct grib_action_file
{
    char* filename;
    grib_action* root;
    grib_action_file* next;
};

struct grib_action_file_list
{
    grib_action_file* first;
    grib_action_file* last;
};

/* Common keys iterator */
struct grib_keys_iterator
{
    grib_handle* handle;
    unsigned long filter_flags; /** flags to filter out accessors */
    unsigned long accessor_flags_skip;
    grib_accessor* current;
    char* name_space;
    int at_start;
    int match;
    int i_curr_attribute;
    grib_accessor** attributes;
    char* prefix;
    grib_trie* seen;
};

/* BUFR-specific keys iterator */
struct bufr_keys_iterator
{
    grib_handle* handle;
    unsigned long filter_flags; /** flags to filter out accessors */
    unsigned long accessor_flags_skip;
    unsigned long accessor_flags_only;
    grib_accessor* current;
    char* key_name;
    int at_start;
    int match;
    int i_curr_attribute;
    grib_accessor** attributes;
    char* prefix;
    grib_trie* seen;
};

/* --------- md5 --------*/
typedef unsigned long cvs_uint32;
struct cvs_MD5Context
{
    cvs_uint32 buf[4];
    cvs_uint32 bits[2];
    unsigned char in[64];
};
/* --- */

typedef struct grib_rule_entry grib_rule_entry;
struct grib_rule_entry
{
    grib_rule_entry* next;
    char* name;
    grib_expression* value;
};

typedef struct grib_rule grib_rule;
struct grib_rule
{
    grib_rule* next;
    grib_expression* condition;
    grib_rule_entry* entries;
};

typedef struct grib_case grib_case;
struct grib_case
{
    grib_arguments* values;
    grib_action* action;
    grib_case* next;
};

/* ----------*/
typedef struct code_table_entry
{
    char* abbreviation;
    char* title;
    char* units;
} code_table_entry;

struct grib_codetable
{
    char* filename[2];
    char* recomposed_name[2];
    grib_codetable* next;
    size_t size;
    code_table_entry entries[1];
};

typedef struct grib_smart_table_entry
{
    /*int code;*/
    char* abbreviation;
    char* column[MAX_SMART_TABLE_COLUMNS];
} grib_smart_table_entry;

struct grib_smart_table
{
    char* filename[3];
    char* recomposed_name[3];
    grib_smart_table* next;
    size_t numberOfEntries;
    grib_smart_table_entry* entries;
};


#if ECCODES_TIMER
typedef struct grib_timer
{
    struct timeval start_;
    double timer_;
    int active_;
    char* name_;
    int count_;
    long total_;

    int elapsed_;
    double cpu_;
    double total_cpu_;

    char* statname_;
    grib_context* context;

    struct grib_timer* next_;
} grib_timer;
#else
typedef struct grib_timer
{
    char nothing;
} grib_timer;
#endif

typedef struct j2k_encode_helper
{
    size_t buffer_size;

    long width;
    long height;
    long bits_per_value;

    float compression;

    long no_values;
    const double* values;
    double reference_value;
    double divisor;
    double decimal;

    long jpeg_length;
    unsigned char* jpeg_buffer;

} j2k_encode_helper;


#include "eccodes_prototypes.h"
#ifdef __cplusplus
}
    #include "action/action.h"
    #include "accessor/grib_accessor.h"
    #include "accessor/grib_accessors_list.h"
    #include "geo/iterator/grib_iterator.h"
    #include "geo/nearest/grib_nearest.h"
    #include "expression/grib_expression.h"
    #include "expression/grib_arguments.h"
#endif

#endif
