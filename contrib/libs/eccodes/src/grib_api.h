/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

/*! \file grib_api.h
  \brief grib_api C header file
*/

#ifndef grib_api_H
#define grib_api_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "eccodes_windef.h"

#ifndef ECCODES_ON_WINDOWS
    #include <sys/time.h>
#endif
#include <math.h>

#if defined(__GNUC__) || defined(__clang__)
    #define ECCODES_DEPRECATED __attribute__((deprecated))
#else
    #define ECCODES_DEPRECATED
#endif

typedef enum ProductKind
{
    PRODUCT_ANY,
    PRODUCT_GRIB,
    PRODUCT_BUFR,
    PRODUCT_METAR,
    PRODUCT_GTS,
    PRODUCT_TAF
} ProductKind;

#include "eccodes_version.h"

#ifdef __cplusplus
extern "C" {
#endif

/* sections */
#define GRIB_SECTION_PRODUCT (1 << 0)
#define GRIB_SECTION_GRID    (1 << 1)
#define GRIB_SECTION_LOCAL   (1 << 2)
#define GRIB_SECTION_DATA    (1 << 3)
#define GRIB_SECTION_BITMAP  (1 << 4)


/* Log modes for processing information */
/*  Log mode for info */
#define GRIB_LOG_INFO 0
/*  Log mode for warnings */
#define GRIB_LOG_WARNING 1
/*  Log mode for errors */
#define GRIB_LOG_ERROR 2
/*  Log mode for fatal errors */
#define GRIB_LOG_FATAL 3
/*  Log mode for debug */
#define GRIB_LOG_DEBUG 4

/* Types */
/*  undefined */
#define GRIB_TYPE_UNDEFINED 0
/*  long integer */
#define GRIB_TYPE_LONG 1
/*  double */
#define GRIB_TYPE_DOUBLE 2
/*  char*    */
#define GRIB_TYPE_STRING 3
/*  bytes */
#define GRIB_TYPE_BYTES 4
/*  section */
#define GRIB_TYPE_SECTION 5
/*  label */
#define GRIB_TYPE_LABEL 6
/*  missing */
#define GRIB_TYPE_MISSING 7

/* Missing values */
/* #define GRIB_MISSING_LONG 0x80000001 */
/* #define GRIB_MISSING_LONG 0xffffffff */
#define GRIB_MISSING_LONG   2147483647
#define GRIB_MISSING_DOUBLE -1e+100

/* Dump option flags */
#define GRIB_DUMP_FLAG_READ_ONLY      (1 << 0)
#define GRIB_DUMP_FLAG_DUMP_OK        (1 << 1)
#define GRIB_DUMP_FLAG_VALUES         (1 << 2)
#define GRIB_DUMP_FLAG_CODED          (1 << 3)
#define GRIB_DUMP_FLAG_OCTET          (1 << 4)
#define GRIB_DUMP_FLAG_ALIASES        (1 << 5)
#define GRIB_DUMP_FLAG_TYPE           (1 << 6)
#define GRIB_DUMP_FLAG_HEXADECIMAL    (1 << 7)
#define GRIB_DUMP_FLAG_NO_DATA        (1 << 8)
#define GRIB_DUMP_FLAG_ALL_DATA       (1 << 9)
#define GRIB_DUMP_FLAG_ALL_ATTRIBUTES (1 << 10)

/* grib_nearest flags */
#define GRIB_NEAREST_SAME_GRID  (1 << 0)
#define GRIB_NEAREST_SAME_DATA  (1 << 1)
#define GRIB_NEAREST_SAME_POINT (1 << 2)

/* Geoiterator flags */
#define GRIB_GEOITERATOR_NO_VALUES (1 << 0)

/*! Iteration is carried out on all the keys available in the message
\ingroup keys_iterator
\see grib_keys_iterator_new
*/
#define GRIB_KEYS_ITERATOR_ALL_KEYS 0

/*! read only keys are skipped by keys iterator.
\ingroup keys_iterator
\see grib_keys_iterator_new
*/
#define GRIB_KEYS_ITERATOR_SKIP_READ_ONLY (1 << 0)

/*! optional keys are skipped by keys iterator.
\ingroup keys_iterator
\see grib_keys_iterator_new */
#define GRIB_KEYS_ITERATOR_SKIP_OPTIONAL (1 << 1)

/*! edition specific keys are skipped by keys iterator.
\ingroup keys_iterator
\see grib_keys_iterator_new */
#define GRIB_KEYS_ITERATOR_SKIP_EDITION_SPECIFIC (1 << 2)

/*! coded keys are skipped by keys iterator.
\ingroup keys_iterator
\see grib_keys_iterator_new */
#define GRIB_KEYS_ITERATOR_SKIP_CODED (1 << 3)

/*! computed keys are skipped by keys iterator.
\ingroup keys_iterator
\see grib_keys_iterator_new */
#define GRIB_KEYS_ITERATOR_SKIP_COMPUTED (1 << 4)

/*! duplicates of a key are skipped by keys iterator.
\ingroup keys_iterator
\see grib_keys_iterator_new */
#define GRIB_KEYS_ITERATOR_SKIP_DUPLICATES (1 << 5)

/*! function keys are skipped by keys iterator.
\ingroup keys_iterator
\see grib_keys_iterator_new */
#define GRIB_KEYS_ITERATOR_SKIP_FUNCTION (1 << 6)

/*! only keys present in the dump
\ingroup keys_iterator
\see grib_keys_iterator_new */
#define GRIB_KEYS_ITERATOR_DUMP_ONLY (1 << 7)

typedef struct grib_key_value_list grib_key_value_list;

typedef struct grib_values grib_values;

struct grib_values
{
    const char* name;
    int type;
    long long_value;
    double double_value;
    const char* string_value;
    int error;
    int has_value;
    int equal;
    grib_values* next;
};


/*! Grib handle, structure giving access to parsed message values by keys
    \ingroup grib_handle
*/
typedef struct grib_handle grib_handle;

/*! Grib multi field handle, structure used to build multi-field GRIB messages.
    \ingroup grib_handle
 */
typedef struct grib_multi_handle grib_multi_handle;

/*! Grib context, structure containing the memory methods, the parsers and the formats.
    \ingroup grib_context
*/
typedef struct grib_context grib_context;

/*! Grib geoiterator, structure supporting a geographic iteration of values on a GRIB message.
    \ingroup grib_iterator
*/
typedef struct grib_iterator grib_iterator;

/*! Grib nearest, structure used to find the nearest points of a latitude longitude point.
    \ingroup grib_iterator
*/
typedef struct grib_nearest grib_nearest;

/*! Grib keys iterator. Iterator over keys.
    \ingroup keys_iterator
*/
typedef struct grib_keys_iterator grib_keys_iterator;
typedef struct bufr_keys_iterator bufr_keys_iterator;

typedef struct grib_fieldset grib_fieldset;

typedef struct grib_order_by grib_order_by;
typedef struct grib_where grib_where;

typedef struct grib_sarray grib_sarray;
typedef struct grib_oarray grib_oarray;
typedef struct grib_darray grib_darray;
typedef struct grib_iarray grib_iarray;
typedef struct grib_vdarray grib_vdarray;
typedef struct grib_vsarray grib_vsarray;
typedef struct grib_viarray grib_viarray;
typedef struct bufr_descriptor bufr_descriptor;
typedef struct bufr_descriptors_array bufr_descriptors_array;
typedef struct bufr_descriptors_map_list bufr_descriptors_map_list;

grib_fieldset* grib_fieldset_new_from_files(grib_context* c, const char* filenames[], int nfiles, const char** keys, int nkeys, const char* where_string, const char* order_by_string, int* err);
void grib_fieldset_delete(grib_fieldset* set);
void grib_fieldset_rewind(grib_fieldset* set);
int grib_fieldset_apply_order_by(grib_fieldset* set, const char* order_by_string);
grib_handle* grib_fieldset_next_handle(grib_fieldset* set, int* err);
int grib_fieldset_count(const grib_fieldset* set);
int grib_values_check(grib_handle* h, grib_values* values, int count);

/*! \defgroup grib_index The grib_index
The grib_index is the structure giving indexed access to messages in a file.
 */
/*! @{*/

/*! index structure to access messages in a file.
 */
typedef struct grib_index grib_index;

/**
 *  Create a new index from a file. The file is indexed with the keys in argument.
 *
 * @param c           : context  (NULL for default context)
 * @param filename    : name of the file of messages to be indexed
 * @param keys        : comma separated list of keys for the index.
 *    The type of the key can be explicitly declared appending :l for long,
 *    (or alternatively :i)
 *    :d for double, :s for string to the key name. If the type is not
 *    declared explicitly, the native type is assumed.
 * @param err         :  0 if OK, integer value on error
 * @return            the newly created index
 */
grib_index* grib_index_new_from_file(grib_context* c,
                                     const char* filename, const char* keys, int* err);
/**
 *  Create a new index based on a set of keys.
 *
 * @param c           : context  (NULL for default context)
 * @param keys        : comma separated list of keys for the index.
 *    The type of the key can be explicitly declared appending ":l" for long,
 *    (or alternatively ":i"), ":d" for double, ":s" for string to the key name. If the type is not
 *    declared explicitly, the native type is assumed.
 * @param err         :  0 if OK, integer value on error
 * @return            the newly created index
 */
grib_index* grib_index_new(grib_context* c, const char* keys, int* err);

/* EXPERIMENTAL */
int codes_index_set_product_kind(grib_index* index, ProductKind product_kind);
int codes_index_set_unpack_bufr(grib_index* index, int unpack);


/**
 *  Indexes the file given in argument in the index given in argument.
 *
 * @param index       : index
 * @param filename    : name of the file of messages to be indexed
 * @return            0 if OK, integer value on error
 */
int grib_index_add_file(grib_index* index, const char* filename);
int grib_index_write(grib_index* index, const char* filename);
grib_index* grib_index_read(grib_context* c, const char* filename, int* err);

/**
 *  Get the number of distinct values of the key in argument contained in the index. The key must belong to the index.
 *
 * @param index       : an index created from a file.
 *     The index must have been created with the key in argument.
 * @param key         : key for which the number of values is computed
 * @param size        : number of distinct values of the key in the index
 * @return            0 if OK, integer value on error
 */
int grib_index_get_size(const grib_index* index, const char* key, size_t* size);

/**
 *  Get the distinct values of the key in argument contained in the index. The key must belong to the index. This function is used when the type of the key was explicitly defined as long or when the native type of the key is long.
 *
 * @param index       : an index created from a file.
 *     The index must have been created with the key in argument.
 * @param key         : key for which the values are returned
 * @param values      : array of values. The array must be allocated before entering this function and its size must be enough to contain all the values.
 * @param size        : size of the values array
 * @return            0 if OK, integer value on error
 */
int grib_index_get_long(const grib_index* index, const char* key,
                        long* values, size_t* size);

/**
 *  Get the distinct values of the key in argument contained in the index. The key must belong to the index. This function is used when the type of the key was explicitly defined as double or when the native type of the key is double.
 *
 * @param index       : an index created from a file.
 *     The index must have been created with the key in argument.
 * @param key         : key for which the values are returned
 * @param values      : array of values. The array must be allocated before entering this function and its size must be enough to contain all the values.
 * @param size        : size of the values array
 * @return            0 if OK, integer value on error
 */
int grib_index_get_double(const grib_index* index, const char* key,
                          double* values, size_t* size);

/**
 *  Get the distinct values of the key in argument contained in the index. The key must belong to the index. This function is used when the type of the key was explicitly defined as string or when the native type of the key is string.
 *
 * @param index       : an index created from a file.
 *     The index must have been created with the key in argument.
 * @param key         : key for which the values are returned
 * @param values      : array of values. The array must be allocated before entering this function and its size must be enough to contain all the values.
 * @param size        : size of the values array
 * @return            0 if OK, integer value on error
 */
int grib_index_get_string(const grib_index* index, const char* key,
                          char** values, size_t* size);


/**
 *  Select the message subset with key==value. The value is a long. The key must have been created with long type or have long as native type if the type was not explicitly defined in the index creation.
 *
 * @param index       : an index created from a file.
 *     The index must have been created with the key in argument.
 * @param key         : key to be selected
 * @param value       : value of the key to select
 * @return            0 if OK, integer value on error
 */
int grib_index_select_long(grib_index* index, const char* key, long value);

/**
 *  Select the message subset with key==value. The value is a double. The key must have been created with double type or have double as native type if the type was not explicitly defined in the index creation.
 *
 * @param index       : an index created from a file.
 *     The index must have been created with the key in argument.
 * @param key         : key to be selected
 * @param value       : value of the key to select
 * @return            0 if OK, integer value on error
 */
int grib_index_select_double(grib_index* index, const char* key, double value);

/**
 *  Select the message subset with key==value. The value is a string. The key must have been created with string type or have string as native type if the type was not explicitly defined in the index creation.
 *
 * @param index       : an index created from a file.
 *     The index must have been created with the key in argument.
 * @param key         : key to be selected
 * @param value       : value of the key to select
 * @return            0 if OK, integer value on error
 */
int grib_index_select_string(grib_index* index, const char* key, const char* value);

/**
 *  Create a new handle from an index after having selected the key values.
 *  All the keys belonging to the index must be selected before calling this function. Successive calls to this function will return all the handles compatible with the constraints defined selecting the values of the index keys.
 * When no more handles are available from the index a NULL pointer is returned and the err variable is set to GRIB_END_OF_INDEX.
 *
 * @param index       : an index created from a file.
 * @param err         : 0 if OK, integer value on error. GRIB_END_OF_INDEX when no more handles are contained in the index.
 * @return            GRIB handle.
 */
grib_handle* grib_handle_new_from_index(grib_index* index, int* err);

/**
 *  Delete the index.
 *
 * @param index       : index to be deleted.
 */
void grib_index_delete(grib_index* index);

/*! @} */

/*! \defgroup grib_handle The grib_handle
The grib_handle is the structure giving access to parsed grib values by keys.
*/
/*! @{*/
/**
 *  Counts the messages contained in a file resource.
 *
 * @param c           : the context from which the handle will be created (NULL for default context)
 * @param f           : the file resource
 * @param n           : the number of messages in the file
 * @return            0 if OK, integer value on error
 */
int grib_count_in_file(grib_context* c, FILE* f, int* n);

/**
 *  Counts the messages contained in a file.
 *
 * @param c           : the context from which the handle will be created (NULL for default context)
 * @param filename    : the path to the file
 * @param n           : the number of messages in the file
 * @return            0 if OK, integer value on error
 */
int grib_count_in_filename(grib_context* c, const char* filename, int* n);


/**
 *  Create a handle from a file resource.
 *  The file is read until a message is found. The message is then copied.
 *  Remember always to delete the handle when it is not needed anymore to avoid
 *  memory leaks.
 *
 * @param c           : the context from which the handle will be created (NULL for default context)
 * @param f           : the file resource
 * @param error       : error code set if the returned handle is NULL and the end of file is not reached
 * @return            the new handle, NULL if the resource is invalid or a problem is encountered
 */
grib_handle* grib_handle_new_from_file(grib_context* c, FILE* f, int* error);

/**
 *  Write a coded message in a file.
 *
 * @param h           : grib_handle to be written
 * @param file        : name of the file
 * @param mode        : mode
 * @return            0 if OK, integer value on error
 */
int grib_write_message(const grib_handle* h, const char* file, const char* mode);

typedef struct grib_string_list grib_string_list;
struct grib_string_list
{
    char* value;
    int count;
    grib_string_list* next;
};

grib_handle* grib_util_sections_copy(grib_handle* hfrom, grib_handle* hto, int what, int* err);
grib_string_list* grib_util_get_param_id(const char* mars_param);
grib_string_list* grib_util_get_mars_param(const char* param_id);

/**
 *  Create a handle from a user message in memory. The message will not be freed at the end.
 *  The message will be copied as soon as a modification is needed.
 *
 * @param c           : the context from which the handle will be created (NULL for default context)
 * @param data        : the actual message
 * @param data_len    : the length of the message in number of bytes
 * @return            the new handle, NULL if the message is invalid or a problem is encountered
 */
grib_handle* grib_handle_new_from_message(grib_context* c, const void* data, size_t data_len);

/**
 *  Create a handle from a user message in memory. The message will not be freed at the end.
 *  The message will be copied as soon as a modification is needed.
 *  This function works also with multi-field messages.
 *  Note: The data pointer argument may be modified
 *
 * @param c           : the context from which the handle will be created (NULL for default context)
 * @param data        : the actual message
 * @param data_len    : the length of the message in number of bytes
 * @param error       : error code
 * @return            the new handle, NULL if the message is invalid or a problem is encountered
 */
grib_handle* grib_handle_new_from_multi_message(grib_context* c, void** data,
                                                size_t* data_len, int* error);

/**
 *  Create a handle from a user message. The message is copied and will be freed with the handle
 *
 * @param c           : the context from which the handle will be created (NULL for default context)
 * @param data        : the actual message
 * @param data_len    : the length of the message in number of bytes
 * @return            the new handle, NULL if the message is invalid or a problem is encountered
 */
grib_handle* grib_handle_new_from_message_copy(grib_context* c, const void* data, size_t data_len);


/**
 *  Create a handle from a GRIB message contained in the samples directory.
 *  The message is copied at the creation of the handle
 *
 * @param c           : the context from which the handle will be created (NULL for default context)
 * @param sample_name : the name of the GRIB sample file
 * @return            the new handle, NULL if the resource is invalid or a problem is encountered
 */
grib_handle* grib_handle_new_from_samples(grib_context* c, const char* sample_name);


/**
 *  Clone an existing handle using the context of the original handle,
 *  The message is copied and reparsed
 *
 * @param h           : The handle to be cloned
 * @return            the new handle, NULL if the message is invalid or a problem is encountered
 */
grib_handle* grib_handle_clone(const grib_handle* h);
grib_handle* grib_handle_clone_headers_only(const grib_handle* h);

/**
 *  Frees a handle, also frees the message if it is not a user message
 *  @see  grib_handle_new_from_message
 * @param h           : The handle to be deleted
 * @return            0 if OK, integer value on error
 */
int grib_handle_delete(grib_handle* h);

/**
 *  Create an empty multi-field handle.
 *  Remember always to delete the multi handle when it is not needed anymore to avoid
 *  memory leaks.
 *
 * @param c           : the context from which the handle will be created (NULL for default context)
 */
grib_multi_handle* grib_multi_handle_new(grib_context* c);

/**
 *  Append the sections starting with start_section of the message pointed by h at
 *  the end of the multi-field handle mh.
 *  Remember always to delete the multi handle when it is not needed anymore to avoid
 *  memory leaks.
 *
 * @param h           : The handle from which the sections are copied.
 * @param start_section : section number. Starting from this section all the sections to the end of the message will be copied.
 * @param mh           : The multi field handle on which the sections are appended.
 * @return            0 if OK, integer value on error
 */
int grib_multi_handle_append(grib_handle* h, int start_section, grib_multi_handle* mh);

/**
 * Delete multi-field handle.
 *
 * @param mh          : The multi-field handle to be deleted.
 * @return            0 if OK, integer value on error
 */
int grib_multi_handle_delete(grib_multi_handle* mh);

/**
 *  Write a multi-field handle in a file.
 *  Remember to delete the multi handle when it is not needed anymore to avoid
 *  memory leaks.
 *
 * @param mh          : The multi field handle to be written.
 * @param f           : File on which the file handle is written.
 * @return            0 if OK, integer value on error
 */
int grib_multi_handle_write(grib_multi_handle* mh, FILE* f);

/*! @} */

/*! \defgroup handling_coded_messages Handling coded messages */
/*! @{ */
/**
 * getting the message attached to a handle
 *
 * @param h              : the handle to which the buffer should be gathered
 * @param message        : the pointer to be set to the handle's data
 * @param message_length : On exit, the message size in number of bytes
 * @return            0 if OK, integer value on error
 */
int grib_get_message(const grib_handle* h, const void** message, size_t* message_length);


/**
 * getting a copy of the message attached to a handle
 *
 * @param h              : the handle to which the buffer should be returned
 * @param message        : the pointer to the data buffer to be filled
 * @param message_length : On entry, the size in number of bytes of the allocated empty message.
 *                         On exit, the actual message length in number of bytes
 * @return            0 if OK, integer value on error
 */
int grib_get_message_copy(const grib_handle* h, void* message, size_t* message_length);
/*! @} */

/*! \defgroup iterators Iterating on latitude/longitude/values */
/*! @{ */

/*!
 * \brief Create a new geoiterator from a handle, using current geometry and values.
 *
 * \param h           : the handle from which the geoiterator will be created
 * \param flags       : flags for future use.
 * \param error       : error code
 * \return            the new geoiterator, NULL if no geoiterator can be created
 */
grib_iterator* grib_iterator_new(const grib_handle* h, unsigned long flags, int* error);

/**
 * Get latitude/longitude and data values.
 * The latitudes, longitudes and values arrays must be properly allocated by the caller.
 * Their required dimension can be obtained by getting the value of the integer key "numberOfPoints".
 *
 * @param h           : handle from which geography and data values are taken
 * @param lats        : returned array of latitudes
 * @param lons        : returned array of longitudes
 * @param values      : returned array of data values
 * @return            0 if OK, integer value on error
 */
int grib_get_data(const grib_handle* h, double* lats, double* lons, double* values);

/**
 * Get the next value from a geoiterator.
 *
 * @param i           : the geoiterator
 * @param lat         : output latitude in degrees
 * @param lon         : output longitude in degrees
 * @param value       : output value of the point
 * @return            positive value if successful, 0 if no more data are available
 */
int grib_iterator_next(grib_iterator* i, double* lat, double* lon, double* value);

/**
 * Get the previous value from a geoiterator.
 *
 * @param i           : the geoiterator
 * @param lat         : output latitude in degrees
 * @param lon         : output longitude in degrees
 * @param value       : output value of the point*
 * @return            positive value if successful, 0 if no more data are available
 */
int grib_iterator_previous(grib_iterator* i, double* lat, double* lon, double* value);

/**
 * Test procedure for values in a geoiterator.
 *
 * @param i           : the geoiterator
 * @return            boolean, 1 if the geoiterator still has next values, 0 otherwise
 */
int grib_iterator_has_next(grib_iterator* i);

/**
 * Test procedure for values in a geoiterator.
 *
 * @param i           : the geoiterator
 * @return            0 if OK, integer value on error
 */
int grib_iterator_reset(grib_iterator* i);

/**
 *  Frees a geoiterator from memory
 *
 * @param i           : the geoiterator
 * @return            0 if OK, integer value on error
 */
int grib_iterator_delete(grib_iterator* i);

/*!
 * \brief Create a new nearest neighbour object from a handle, using current geometry.
 *
 * \param h           : the handle from which the nearest object will be created
 * \param error       : error code
 * \return            the new nearest, NULL if no nearest can be created
 */
grib_nearest* grib_nearest_new(const grib_handle* h, int* error);

/**
 * Find the 4 nearest points of a latitude longitude point.
 * The flags are provided to speed up the process of searching. If you are
 * sure that the point you are asking for is not changing from a call
 * to another you can use GRIB_NEAREST_SAME_POINT. The same is valid for
 * the grid. Flags can be used together doing a bitwise OR.
 * The distances are given in kilometres.
 *
 * @param nearest     : nearest structure
 * @param h           : handle from which geography and data values are taken
 * @param inlat       : latitude of the point to search for
 * @param inlon       : longitude of the point to search for
 * @param flags       : GRIB_NEAREST_SAME_POINT, GRIB_NEAREST_SAME_GRID
 * @param outlats     : returned array of latitudes of the nearest points
 * @param outlons     : returned array of longitudes of the nearest points
 * @param values      : returned array of data values of the nearest points
 * @param distances   : returned array of distances from the nearest points
 * @param indexes     : returned array of indexes of the nearest points
 * @param len         : size of the arrays
 * @return            0 if OK, integer value on error
 */
int grib_nearest_find(grib_nearest* nearest, const grib_handle* h, double inlat, double inlon,
                      unsigned long flags, double* outlats, double* outlons,
                      double* values, double* distances, int* indexes, size_t* len);

/**
 *  Frees a nearest neighbour object from memory
 *
 * @param nearest     : the nearest
 * @return            0 if OK, integer value on error
 */
int grib_nearest_delete(grib_nearest* nearest);

/**
 * Find the nearest point of a set of points whose latitudes and longitudes
 * are given in the inlats, inlons arrays respectively.
 * If the flag is_lsm is 1 the nearest land point is returned and the
 * GRIB passed as handle (h) is considered a land sea mask.
 * The land nearest point is the nearest point with land sea mask value>=0.5.
 * If no nearest land points are found the nearest value is returned.
 * If the flag is_lsm is 0 the nearest point is returned.
 * values, distances, indexes (in the "values" array) for the nearest points (ilons,ilats)
 * are returned.
 * The distances are given in kilometres.
 *
 * @param h           : handle from which geography and data values are taken
 * @param is_lsm      : lsm flag (1-> nearest land, 0-> nearest)
 * @param inlats      : latitudes of the points to search for
 * @param inlons      : longitudes of the points to search for
 * @param npoints     : number of points (size of the inlats,inlons,outlats,outlons,values,distances,indexes arrays)
 * @param outlats     : returned array of latitudes of the nearest points
 * @param outlons     : returned array of longitudes of the nearest points
 * @param values      : returned array of data values of the nearest points
 * @param distances   : returned array of distances from the nearest points
 * @param indexes     : returned array of indexes of the nearest points
 * @return            0 if OK, integer value on error
 */
int grib_nearest_find_multiple(const grib_handle* h, int is_lsm,
                               const double* inlats, const double* inlons, long npoints,
                               double* outlats, double* outlons,
                               double* values, double* distances, int* indexes);

/* @} */

/*! \defgroup get_set Accessing header and data values   */
/*! @{ */
/**
 *  Get the byte offset of a key. If several keys of the same name
 *  are present, the offset of the last one is returned
 *
 * @param h           : the handle to get the offset from
 * @param key         : the key to be searched
 * @param offset      : the address of a size_t where the offset will be set
 * @return            0 if OK, integer value on error
 */
int grib_get_offset(const grib_handle* h, const char* key, size_t* offset);

/**
 *  Get the number of coded value from a key, if several keys of the same name are present, the total sum is returned
 *
 * @param h           : the handle to get the data from
 * @param key         : the key to be searched
 * @param size        : the address of a size_t where the size will be set
 * @return            0 if OK, integer value on error
 */
int grib_get_size(const grib_handle* h, const char* key, size_t* size);

/**
 *  Get the length of the string representation of the key, if several keys of the same name are present, the maximum length is returned
 *
 * @param h           : the handle to get the data from
 * @param key         : the key to be searched
 * @param length      : the address of a size_t where the length will be set
 * @return            0 if OK, integer value on error
 */
int grib_get_length(const grib_handle* h, const char* key, size_t* length);

/**
 *  Get a long value from a key, if several keys of the same name are present, the last one is returned
 *  @see  grib_set_long
 *
 * @param h           : the handle to get the data from
 * @param key         : the key to be searched
 * @param value       : the address of a long where the data will be retrieved
 * @return            0 if OK, integer value on error
 */
int grib_get_long(const grib_handle* h, const char* key, long* value);

/**
 *  Get a double value from a key, if several keys of the same name are present, the last one is returned
 *  @see  grib_set_double
 *
 * @param h           : the handle to get the data from
 * @param key         : the key to be searched
 * @param value       : the address of a double where the data will be retrieved
 * @return            0 if OK, integer value on error
 */
int grib_get_double(const grib_handle* h, const char* key, double* value);
int grib_get_float(const grib_handle* h, const char* key, float* value);

/**
 *  Get as double the i-th element of the "key" array
 *
 * @param h           : the handle to get the data from
 * @param key         : the key to be searched
 * @param i           : zero-based index
 * @param value       : the address of a double where the data will be retrieved
 * @return            0 if OK, integer value on error
 */
int grib_get_double_element(const grib_handle* h, const char* key, int i, double* value);
int grib_get_float_element(const grib_handle* h, const char* key, int i, float* value);

/**
 *  Get as double array the elements of the "key" array whose indexes are listed in the input array "index_array"
 *
 * @param h           : the handle to get the data from
 * @param key         : the key to be searched
 * @param index_array : zero-based array of indexes
 * @param size        : size of the index_array and value arrays
 * @param value       : the double array for the data values
 * @return            0 if OK, integer value on error
 */
int grib_get_double_elements(const grib_handle* h, const char* key, const int* index_array, long size, double* value);
int grib_get_float_elements(const grib_handle* h, const char* key, const int* index_array, long size, float* value);

/**
 *  Get a string value from a key, if several keys of the same name are present, the last one is returned
 * @see  grib_set_string
 *
 * @param h        : the handle to get the data from
 * @param key      : the key to be searched
 * @param value    : the address of a string where the data will be retrieved
 * @param length   : the address of a size_t that contains allocated length of the string on input,
 *                   and that contains the actual length of the string on output
 * @return         0 if OK, integer value on error
 */
int grib_get_string(const grib_handle* h, const char* key, char* value, size_t* length);

/**
 *  Get string array values from a key. If several keys of the same name are present, the last one is returned
 * @see  grib_set_string_array
 *
 * @param h       : the handle to get the data from
 * @param key     : the key to be searched
 * @param vals    : the address of a string array where the data will be retrieved
 * @param length  : the address of a size_t that contains allocated length of the array on input, and that contains the actual length of the array on output
 * @return        0 if OK, integer value on error
 */
int grib_get_string_array(const grib_handle* h, const char* key, char** vals, size_t* length);

/**
 *  Get raw bytes values from a key. If several keys of the same name are present, the last one is returned
 * @see  grib_set_bytes
 *
 * @param h           : the handle to get the data from
 * @param key         : the key to be searched
 * @param bytes       : the address of a byte array where the data will be retrieved
 * @param length      : the address of a size_t that contains allocated length of the byte array on input, and that contains the actual length of the byte array on output
 * @return            0 if OK, integer value on error
 */
int grib_get_bytes(const grib_handle* h, const char* key, unsigned char* bytes, size_t* length);
/**
 *  Get double array values from a key. If several keys of the same name are present, the last one is returned
 * @see  grib_set_double_array
 *
 * @param h           : the handle to get the data from
 * @param key         : the key to be searched
 * @param vals       : the address of a double array where the data will be retrieved
 * @param length      : the address of a size_t that contains allocated length of the double array on input, and that contains the actual length of the double array on output
 * @return            0 if OK, integer value on error
 */
int grib_get_double_array(const grib_handle* h, const char* key, double* vals, size_t* length);
int grib_get_float_array(const grib_handle* h, const char* key, float* vals, size_t* length);

/**
 *  Get long array values from a key. If several keys of the same name are present, the last one is returned
 * @see  grib_set_long_array
 *
 * @param h           : the handle to get the data from
 * @param key         : the key to be searched
 * @param vals       : the address of a long array where the data will be retrieved
 * @param length      : the address of a size_t that contains allocated length of the long array on input, and that contains the actual length of the long array on output
 * @return            0 if OK, integer value on error
 */
int grib_get_long_array(const grib_handle* h, const char* key, long* vals, size_t* length);


/*   setting      data         */
/**
 *  Copy the keys belonging to a given namespace from a source handle to a destination handle
 *
 *
 * @param dest      : destination handle
 * @param name      : namespace
 * @param src       : source handle
 * @return          0 if OK, integer value on error
 */
int grib_copy_namespace(grib_handle* dest, const char* name, grib_handle* src);

/**
 *  Set a long value from a key. If several keys of the same name are present, the last one is set
 *  @see  grib_get_long
 *
 * @param h           : the handle to set the data to
 * @param key         : the key to be searched
 * @param val         : a long where the data will be read
 * @return            0 if OK, integer value on error
 */
int grib_set_long(grib_handle* h, const char* key, long val);

/**
 *  Set a double value from a key. If several keys of the same name are present, the last one is set
 *  @see  grib_get_double
 *
 * @param h           : the handle to set the data to
 * @param key         : the key to be searched
 * @param val       : a double where the data will be read
 * @return            0 if OK, integer value on error
 */
int grib_set_double(grib_handle* h, const char* key, double val);

/**
 *  Set a string value from a key. If several keys of the same name are present, the last one is set
 *  @see  grib_get_string
 *
 * @param h           : the handle to set the data to
 * @param key         : the key to be searched
 * @param value       : the address of a string where the data will be read
 * @param length      : the address of a size_t that contains the length of the string on input,
 *                      and that contains the actual packed length of the string on output
 * @return            0 if OK, integer value on error
 */
int grib_set_string(grib_handle* h, const char* key, const char* value, size_t* length);

/**
 *  Set a bytes array from a key. If several keys of the same name are present, the last one is set
 *  @see  grib_get_bytes
 *
 * @param h           : the handle to set the data to
 * @param key         : the key to be searched
 * @param bytes       : the address of a byte array where the data will be read
 * @param length      : the address of a size_t that contains the length of the byte array on input, and that contains the actual packed length of the byte array  on output
 * @return            0 if OK, integer value on error
 */
int grib_set_bytes(grib_handle* h, const char* key, const unsigned char* bytes, size_t* length);

/**
 *  Set a double array from a key. If several keys of the same name are present, the last one is set
 *   @see  grib_get_double_array
 *
 * @param h           : the handle to set the data to
 * @param key         : the key to be searched
 * @param vals        : the address of a double array where the data will be read
 * @param length      : a size_t that contains the length of the byte array on input
 * @return            0 if OK, integer value on error
 */
int grib_set_double_array(grib_handle* h, const char* key, const double* vals, size_t length);
int grib_set_float_array(grib_handle* h, const char* key, const float* vals, size_t length);

/**
 * Same as grib_set_double_array but allows setting of READ-ONLY keys like codedValues.
 * Use with great caution!!
 */
int grib_set_force_double_array(grib_handle* h, const char* key, const double* vals, size_t length);
int grib_set_force_float_array(grib_handle* h, const char* key, const float* vals, size_t length);


/**
 *  Set a long array from a key. If several keys of the same name are present, the last one is set
 *  @see  grib_get_long_array
 *
 * @param h           : the handle to set the data to
 * @param key         : the key to be searched
 * @param vals        : the address of a long array where the data will be read
 * @param length      : a size_t that contains the length of the long array on input
 * @return            0 if OK, integer value on error
 */
int grib_set_long_array(grib_handle* h, const char* key, const long* vals, size_t length);

/**
 *  Set a string array from a key. If several keys of the same name are present, the last one is set
 *  @see  grib_get_string_array
 *
 * @param h           : the handle to set the data to
 * @param key         : the key to be searched
 * @param vals        : the address of a string array where the data will be read
 * @param length      : a size_t that contains the length of the array on input
 * @return            0 if OK, integer value on error
 */
int grib_set_string_array(grib_handle* h, const char* key, const char** vals, size_t length);
/*! @} */


/**
 *  Print all keys, with the context print procedure and dump mode to a resource
 *
 * @param h            : the handle to be printed
 * @param out          : output file handle
 * @param mode         : Examples of available dump modes: debug wmo
 * @param option_flags : all the GRIB_DUMP_FLAG_x flags can be used
 * @param arg          : used to provide a format to output data (experimental)
 */
void grib_dump_content(const grib_handle* h, FILE* out, const char* mode, unsigned long option_flags, void* arg);

/**
 *  Print all keys from the parsed definition files available in a context
 *
 * @param f           : the File used to print the keys on
 * @param c           : the context that contains the cached definition files to be printed
 */
void grib_dump_action_tree(grib_context* c, FILE* f);

/*! \defgroup context The context object
 The context is a long life configuration object of the grib_api.
 It is used to define special allocation and free routines or
 to set special grib_api behaviours and variables.
 */
/*! @{ */
/**
 * free procedure, format of a procedure referenced in the context that is used to free memory
 *
 * @param c           : the context where the memory freeing will apply
 * @param data        : pointer to the data to be freed
 * must match @see grib_malloc_proc
 */
typedef void (*grib_free_proc)(const grib_context* c, void* data);

/**
 * malloc procedure, format of a procedure referenced in the context that is used to allocate memory
 * @param c             : the context where the memory allocation will apply
 * @param length        : length to be allocated in number of bytes
 * @return              a pointer to the allocated memory, NULL if no memory can be allocated
 * must match @see grib_free_proc
 */
typedef void* (*grib_malloc_proc)(const grib_context* c, size_t length);

/**
 * realloc procedure, format of a procedure referenced in the context that is used to reallocate memory
 * @param c             : the context where the memory allocation will apply
 * @param data          : pointer to the data to be reallocated
 * @param length        : length to be allocated in number of bytes
 * @return              a pointer to the allocated memory
 */
typedef void* (*grib_realloc_proc)(const grib_context* c, void* data, size_t length);

/**
 * log procedure, format of a procedure referenced in the context that is used to log internal messages
 *
 * @param c             : the context where the logging will apply
 * @param level         : the log level, as defined in log modes
 * @param mesg          : the message to be logged
 */
typedef void (*grib_log_proc)(const grib_context* c, int level, const char* mesg);

/**
 * print procedure, format of a procedure referenced in the context that is used to print external messages
 *
 * @param c             : the context where the logging will apply
 * @param descriptor    : the structure to be printed on, must match the implementation
 * @param mesg          : the message to be printed
 */
typedef void (*grib_print_proc)(const grib_context* c, void* descriptor, const char* mesg);


/**
 * data read procedure, format of a procedure referenced in the context that is used to read from a stream in a resource
 *
 * @param c            : the context where the read will apply
 * @param ptr          : the resource
 * @param size         : size to read
 * @param stream       : the stream
 * @return              size read
 */
typedef size_t (*grib_data_read_proc)(const grib_context* c, void* ptr, size_t size, void* stream);

/**
 * data write procedure, format of a procedure referenced in the context that is used to write to a stream from a resource
 *
 * @param c            : the context where the write will apply
 * @param ptr          : the resource
 * @param size         : size to read
 * @param stream       : the stream
 * @return              size written
 */
typedef size_t (*grib_data_write_proc)(const grib_context* c, const void* ptr, size_t size, void* stream);

/**
 * data tell procedure, format of a procedure referenced in the context that is used to tell the current position in a stream
 *
 * @param c             : the context where the tell will apply
 * @param stream       : the stream
 * @return              the position in the stream
 */
typedef off_t (*grib_data_tell_proc)(const grib_context* c, void* stream);

/**
* data seek procedure, format of a procedure referenced in the context that is used to seek the current position in a stream
*
* @param c        : the context where the tell will apply
* @param offset   : the offset to seek to
* @param whence   : If whence is set to SEEK_SET, SEEK_CUR, or SEEK_END,
                    the offset is relative to the start of the file, the current position indicator, or end-of-file, respectively.
* @param stream   : the stream
* @return         0 if OK, integer value on error
*/
typedef off_t (*grib_data_seek_proc)(const grib_context* c, off_t offset, int whence, void* stream);

/**
 * data eof procedure, format of a procedure referenced in the context that is used to test end of file
 *
 * @param c             : the context where the tell will apply
 * @param stream       : the stream
 * @return              the position in the stream
 */
typedef int (*grib_data_eof_proc)(const grib_context* c, void* stream);

/**
 *  Get the static default context
 *
 * @return            the default context, NULL if the context is not available
 */
grib_context* grib_context_get_default(void);

/**
 *  Frees the cached definition files of the context
 *
 * @param c           : the context to be deleted
 */
void grib_context_delete(grib_context* c);

/**
 *  Set the GTS header mode on.
 *  The GTS headers will be preserved.
 *
 * @param c           : the context
 */
void grib_gts_header_on(grib_context* c);

/**
 *  Set the GTS header mode off.
 *  The GTS headers will be deleted.
 *
 * @param c           : the context
 */
void grib_gts_header_off(grib_context* c);

/**
 *  Set the GRIBEX mode on.
 *  Grib files will be compatible with GRIBEX.
 *
 * @param c           : the context
 */
void grib_gribex_mode_on(grib_context* c);

/**
 *  Get the GRIBEX mode.
 *
 * @param c           : the context
 */
int grib_get_gribex_mode(const grib_context* c);

/**
 *  Set the GRIBEX mode off.
 *  GRIB files won't be always compatible with GRIBEX.
 *
 * @param c           : the context
 */
void grib_gribex_mode_off(grib_context* c);

/**
 * Sets the search path for definition files.
 *
 * @param c      : the context to be modified
 * @param path   : the search path for definition files
 */
void grib_context_set_definitions_path(grib_context* c, const char* path);

/**
 * Sets the search path for sample files.
 *
 * @param c      : the context to be modified
 * @param path   : the search path for sample files
 */
void grib_context_set_samples_path(grib_context* c, const char* path);

void grib_context_set_debug(grib_context* c, int mode);
void grib_context_set_data_quality_checks(grib_context* c, int val);

/**
 *  Sets the context printing procedure used for user interaction
 *
 * @param c            : the context to be modified
 * @param printp       : the printing procedure to be set @see grib_print_proc
 */
void grib_context_set_print_proc(grib_context* c, grib_print_proc printp);

/**
 *  Sets the context logging procedure used for system (warning, errors, infos ...) messages
 *
 * @param c            : the context to be modified
 * @param logp         : the logging procedure to be set @see grib_log_proc
 */
void grib_context_set_logging_proc(grib_context* c, grib_log_proc logp);

/**
 *  Turn on support for multi-fields in single GRIB messages
 *
 * @param c            : the context to be modified
 */
void grib_multi_support_on(grib_context* c);

/**
 *  Turn off support for multi-fields in single GRIB messages
 *
 * @param c            : the context to be modified
 */
void grib_multi_support_off(grib_context* c);

/**
 *  Reset file handle in GRIB multi-field support mode
 *
 * @param c            : the context to be modified
 * @param f            : the file pointer
 */
void grib_multi_support_reset_file(grib_context* c, FILE* f);

char* grib_samples_path(const grib_context* c);
char* grib_definition_path(const grib_context* c);
/*! @} */

/**
 *  Get the API version
 *
 *  @return        API version
 */
long grib_get_api_version(void);

/**
 *  Get the Git version control SHA1 identifier
 *
 *  @return character string with SHA1 identifier
 */
const char* grib_get_git_sha1(void);

const char* grib_get_git_branch(void);

/**
 *  Get the package name
 *
 *  @return character string with package name
 */
const char* grib_get_package_name(void);

/**
 *  Prints the API version
 */
void grib_print_api_version(FILE* out);

/*! \defgroup keys_iterator Iterating on keys names
The keys iterator is designed to get the key names defined in a message.
Key names on which the iteration is carried out can be filtered through their
attributes or by the namespace they belong to.
*/
/*! @{ */
/*! Create a new iterator from a valid and initialised handle.
 *  @param h             : the handle whose keys you want to iterate
 *  @param filter_flags  : flags to filter out some of the keys through their attributes
 *  @param name_space    : if not null the iteration is carried out only on
 *                         keys belonging to the namespace passed. (NULL for all the keys)
 *  @return              keys iterator ready to iterate through keys according to filter_flags
 *                       and namespace
 */
grib_keys_iterator* grib_keys_iterator_new(grib_handle* h, unsigned long filter_flags, const char* name_space);
bufr_keys_iterator* codes_bufr_keys_iterator_new(grib_handle* h, unsigned long filter_flags);
bufr_keys_iterator* codes_bufr_data_section_keys_iterator_new(grib_handle* h);

/*! Step to the next iterator.
 *  @param kiter         : valid grib_keys_iterator
 *  @return              1 if next iterator exists, 0 if no more elements to iterate on
 */
int grib_keys_iterator_next(grib_keys_iterator* kiter);
int codes_bufr_keys_iterator_next(bufr_keys_iterator* kiter);

/*! get the key name from the iterator
 *  @param kiter         : valid grib_keys_iterator
 *  @return              key name
 */
const char* grib_keys_iterator_get_name(const grib_keys_iterator* kiter);
char* codes_bufr_keys_iterator_get_name(const bufr_keys_iterator* kiter);

/*! Delete the iterator.
 *  @param kiter         : valid grib_keys_iterator
 *  @return              0 if OK, integer value on error
 */
int grib_keys_iterator_delete(grib_keys_iterator* kiter);
int codes_bufr_keys_iterator_delete(bufr_keys_iterator* kiter);

/*! Rewind the iterator.
 *  @param kiter         : valid grib_keys_iterator
 *  @return              0 if OK, integer value on error
 */
int grib_keys_iterator_rewind(grib_keys_iterator* kiter);
int codes_bufr_keys_iterator_rewind(bufr_keys_iterator* kiter);

int grib_keys_iterator_set_flags(grib_keys_iterator* kiter, unsigned long flags);

int grib_keys_iterator_get_long(const grib_keys_iterator* kiter, long* v, size_t* len);
int grib_keys_iterator_get_double(const grib_keys_iterator* kiter, double* v, size_t* len);
int grib_keys_iterator_get_float(const grib_keys_iterator* kiter, float* v, size_t* len);
int grib_keys_iterator_get_string(const grib_keys_iterator* kiter, char* v, size_t* len);
int grib_keys_iterator_get_bytes(const grib_keys_iterator* kiter, unsigned char* v, size_t* len);
int codes_copy_key(grib_handle* h1, grib_handle* h2, const char* key, int type);

/* @} */

void grib_update_sections_lengths(grib_handle* h);


/**
 * Convert an error code into a string
 * @param code       : the error code
 * @return           the error message
 */
const char* grib_get_error_message(int code);

const char* grib_get_type_name(int type);
int grib_get_native_type(const grib_handle* h, const char* name, int* type);
void grib_check(const char* call, const char* file, int line, int e, const char* msg);

#define GRIB_CHECK(a, msg)        grib_check(#a, __FILE__, __LINE__, a, msg)
#define GRIB_CHECK_NOLINE(a, msg) grib_check(#a, 0, 0, a, msg)

int grib_set_values(grib_handle* h, grib_values* grib_values, size_t arg_count);
grib_handle* grib_handle_new_from_partial_message_copy(grib_context* c, const void* data, size_t size);
grib_handle* grib_handle_new_from_partial_message(grib_context* c, const void* data, size_t buflen);

/* Check whether the given key has the value 'missing'.
   Returns a bool i.e. 0 or 1. The error code is an argument */
int grib_is_missing(const grib_handle* h, const char* key, int* err);

/* Check whether the given key is defined (exists).
   Returns a bool i.e. 0 or 1 */
int grib_is_defined(const grib_handle* h, const char* key);

/* Set the given key to have the value 'missing' */
int grib_set_missing(grib_handle* h, const char* key);

/* The truncation is the Gaussian number (also called order) */
int grib_get_gaussian_latitudes(long truncation, double* latitudes);

int grib_julian_to_datetime(double jd, long* year, long* month, long* day, long* hour, long* minute, long* second);
int grib_datetime_to_julian(long year, long month, long day, long hour, long minute, long second, double* jd);
long grib_julian_to_date(long jdate);
long grib_date_to_julian(long ddate);

void grib_get_reduced_row(long pl, double lon_first, double lon_last, long* npoints, long* ilon_first, long* ilon_last);
void grib_get_reduced_row_p(long pl, double lon_first, double lon_last, long* npoints, double* olon_first, double* olon_last);

/* read products */
int wmo_read_any_from_file(FILE* f, void* buffer, size_t* len);
int wmo_read_grib_from_file(FILE* f, void* buffer, size_t* len);
int wmo_read_bufr_from_file(FILE* f, void* buffer, size_t* len);
int wmo_read_gts_from_file(FILE* f, void* buffer, size_t* len);
int wmo_read_any_from_stream(void* stream_data, long (*stream_proc)(void*, void* buffer, long len), void* buffer, size_t* len);

/* These functions allocate memory for the result so the user is responsible for freeing it */
void* wmo_read_any_from_stream_malloc(void* stream_data, long (*stream_proc)(void*, void* buffer, long len), size_t* size, int* err);
void* wmo_read_any_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err);
void* wmo_read_gts_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err);
void* wmo_read_bufr_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err);
void* wmo_read_grib_from_file_malloc(FILE* f, int headers_only, size_t* size, off_t* offset, int* err);

int grib_read_any_from_file(grib_context* ctx, FILE* f, void* buffer, size_t* len);
int grib_get_message_offset(const grib_handle* h, off_t* offset);
int grib_get_message_size(const grib_handle* h, size_t* size);


/* --------------------------------------- */
#define GRIB_UTIL_GRID_SPEC_REGULAR_LL 1
#define GRIB_UTIL_GRID_SPEC_ROTATED_LL 2

#define GRIB_UTIL_GRID_SPEC_REGULAR_GG 3
#define GRIB_UTIL_GRID_SPEC_ROTATED_GG 4
#define GRIB_UTIL_GRID_SPEC_REDUCED_GG 5

#define GRIB_UTIL_GRID_SPEC_SH                           6
#define GRIB_UTIL_GRID_SPEC_REDUCED_LL                   7
#define GRIB_UTIL_GRID_SPEC_POLAR_STEREOGRAPHIC          8
#define GRIB_UTIL_GRID_SPEC_REDUCED_ROTATED_GG           9
#define GRIB_UTIL_GRID_SPEC_LAMBERT_AZIMUTHAL_EQUAL_AREA 10
#define GRIB_UTIL_GRID_SPEC_LAMBERT_CONFORMAL            11
#define GRIB_UTIL_GRID_SPEC_UNSTRUCTURED                 12
#define GRIB_UTIL_GRID_SPEC_HEALPIX                      13

typedef struct grib_util_grid_spec
{
    int grid_type;         /* e.g. GRIB_UTIL_GRID_SPEC_REGULAR_LL etc */
    const char* grid_name; /* e.g. N320 */

    /* Grid */
    long Ni;
    long Nj;

    double iDirectionIncrementInDegrees;
    double jDirectionIncrementInDegrees;

    double longitudeOfFirstGridPointInDegrees;
    double longitudeOfLastGridPointInDegrees;

    double latitudeOfFirstGridPointInDegrees;
    double latitudeOfLastGridPointInDegrees;

    /* Rotation */
    long uvRelativeToGrid;
    double latitudeOfSouthernPoleInDegrees;
    double longitudeOfSouthernPoleInDegrees;
    double angleOfRotationInDegrees;

    /* Scanning mode */
    long iScansNegatively;
    long jScansPositively;

    /* Gaussian number or HEALPIX Nside */
    long N;

    /* Bitmap */
    long bitmapPresent;
    double missingValue; /* 0 means use the default */

    /* 'pl' array for reduced Gaussian grids */
    const long* pl;
    long pl_size;

    /* Spherical harmonics */
    long truncation;

    /* Polar stereographic */
    double orientationOfTheGridInDegrees;
    long DyInMetres;
    long DxInMetres;

} grib_util_grid_spec;

#define GRIB_UTIL_PACKING_TYPE_SAME_AS_INPUT      0
#define GRIB_UTIL_PACKING_TYPE_SPECTRAL_COMPLEX   1
#define GRIB_UTIL_PACKING_TYPE_SPECTRAL_SIMPLE    2
#define GRIB_UTIL_PACKING_TYPE_JPEG               3
#define GRIB_UTIL_PACKING_TYPE_GRID_COMPLEX       4
#define GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE        5
#define GRIB_UTIL_PACKING_TYPE_GRID_SIMPLE_MATRIX 6
#define GRIB_UTIL_PACKING_TYPE_GRID_SECOND_ORDER  7
#define GRIB_UTIL_PACKING_TYPE_CCSDS              8
#define GRIB_UTIL_PACKING_TYPE_IEEE               9

#define GRIB_UTIL_PACKING_SAME_AS_INPUT 0
#define GRIB_UTIL_PACKING_USE_PROVIDED  1

#define GRIB_UTIL_ACCURACY_SAME_BITS_PER_VALUES_AS_INPUT      0
#define GRIB_UTIL_ACCURACY_USE_PROVIDED_BITS_PER_VALUES       1
#define GRIB_UTIL_ACCURACY_SAME_DECIMAL_SCALE_FACTOR_AS_INPUT 2
#define GRIB_UTIL_ACCURACY_USE_PROVIDED_DECIMAL_SCALE_FACTOR  3

typedef struct grib_util_packing_spec
{
    /* Packing options */
    long packing_type;
    long packing;
    long boustrophedonic;

    long editionNumber; /* =0 for default value */

    /* Accuracy */
    long accuracy;
    long bitsPerValue;
    long decimalScaleFactor;

    long computeLaplacianOperator;
    int truncateLaplacian;
    double laplacianOperator;

    /* local definition */
    long deleteLocalDefinition; /* default(=0) local definition is taken from the input field */

    /* Extra values when packing */
    grib_values extra_settings[80];
    long extra_settings_count;
} grib_util_packing_spec;


grib_handle* grib_util_set_spec(grib_handle* h,
                                const grib_util_grid_spec* grid_spec,
                                const grib_util_packing_spec* packing_spec, /* NULL for defaults (same as input) */
                                int flags,
                                const double* data_values,
                                size_t data_values_count,
                                int* err);

int parse_keyval_string(const char* grib_tool, char* arg, int values_required, int default_type, grib_values values[], int* count);
grib_handle* grib_new_from_file(grib_context* c, FILE* f, int headers_only, int* error);

/* EXPERIMENTAL */
typedef struct codes_bufr_header
{
    unsigned long message_offset;
    unsigned long message_size;

    /* Section 0 keys */
    long edition;

    /* Section 1 keys */
    long masterTableNumber;
    long bufrHeaderSubCentre;
    long bufrHeaderCentre;
    long updateSequenceNumber;
    long dataCategory;
    long dataSubCategory;
    long masterTablesVersionNumber;
    long localTablesVersionNumber;

    long typicalYear;
    long typicalMonth;
    long typicalDay;
    long typicalHour;
    long typicalMinute;
    long typicalSecond;
    long typicalDate; /* computed key */
    long typicalTime; /* computed key */

    long internationalDataSubCategory; /*BUFR4-specific*/

    long localSectionPresent;
    long ecmwfLocalSectionPresent;

    /* ECMWF local section keys */
    long rdbType;
    long oldSubtype;
    long rdbSubtype;
    char ident[9];
    long localYear;
    long localMonth;
    long localDay;
    long localHour;
    long localMinute;
    long localSecond;

    long rdbtimeDay;
    long rdbtimeHour;
    long rdbtimeMinute;
    long rdbtimeSecond;

    long rectimeDay;
    long rectimeHour;
    long rectimeMinute;
    long rectimeSecond;
    long restricted;

    long isSatellite;
    double localLongitude1;
    double localLatitude1;
    double localLongitude2;
    double localLatitude2;
    double localLatitude;
    double localLongitude;
    long localNumberOfObservations;
    long satelliteID;
    long qualityControl;
    long newSubtype;
    long daLoop;

    /* Section 3 keys */
    unsigned long numberOfSubsets;
    long observedData;
    long compressedData;

} codes_bufr_header;

/* --------------------------------------- */

typedef void (*codes_assertion_failed_proc)(const char* message);
void codes_set_codes_assertion_failed_proc(codes_assertion_failed_proc proc);

#ifdef __cplusplus
}
#endif
#endif
/* This part is automatically generated by ./errors.pl, do not edit */
#ifndef grib_errors_H
#define grib_errors_H
/*! \defgroup errors Error codes
Error codes returned by the grib_api functions.
*/
/*! @{*/
/** No error */
#define GRIB_SUCCESS		0
/** End of resource reached */
#define GRIB_END_OF_FILE		-1
/** Internal error */
#define GRIB_INTERNAL_ERROR		-2
/** Passed buffer is too small */
#define GRIB_BUFFER_TOO_SMALL		-3
/** Function not yet implemented */
#define GRIB_NOT_IMPLEMENTED		-4
/** Missing 7777 at end of message */
#define GRIB_7777_NOT_FOUND		-5
/** Passed array is too small */
#define GRIB_ARRAY_TOO_SMALL		-6
/** File not found */
#define GRIB_FILE_NOT_FOUND		-7
/** Code not found in code table */
#define GRIB_CODE_NOT_FOUND_IN_TABLE		-8
/** Array size mismatch */
#define GRIB_WRONG_ARRAY_SIZE		-9
/** Key/value not found */
#define GRIB_NOT_FOUND		-10
/** Input output problem */
#define GRIB_IO_PROBLEM		-11
/** Message invalid */
#define GRIB_INVALID_MESSAGE		-12
/** Decoding invalid */
#define GRIB_DECODING_ERROR		-13
/** Encoding invalid */
#define GRIB_ENCODING_ERROR		-14
/** Code cannot unpack because of string too small */
#define GRIB_NO_MORE_IN_SET		-15
/** Problem with calculation of geographic attributes */
#define GRIB_GEOCALCULUS_PROBLEM		-16
/** Memory allocation error */
#define GRIB_OUT_OF_MEMORY		-17
/** Value is read only */
#define GRIB_READ_ONLY		-18
/** Invalid argument */
#define GRIB_INVALID_ARGUMENT		-19
/** Null handle */
#define GRIB_NULL_HANDLE		-20
/** Invalid section number */
#define GRIB_INVALID_SECTION_NUMBER		-21
/** Value cannot be missing */
#define GRIB_VALUE_CANNOT_BE_MISSING		-22
/** Wrong message length */
#define GRIB_WRONG_LENGTH		-23
/** Invalid key type */
#define GRIB_INVALID_TYPE		-24
/** Unable to set step */
#define GRIB_WRONG_STEP		-25
/** Wrong units for step (step must be integer) */
#define GRIB_WRONG_STEP_UNIT		-26
/** Invalid file id */
#define GRIB_INVALID_FILE		-27
/** Invalid GRIB id */
#define GRIB_INVALID_GRIB		-28
/** Invalid index id */
#define GRIB_INVALID_INDEX		-29
/** Invalid iterator id */
#define GRIB_INVALID_ITERATOR		-30
/** Invalid keys iterator id */
#define GRIB_INVALID_KEYS_ITERATOR		-31
/** Invalid nearest id */
#define GRIB_INVALID_NEAREST		-32
/** Invalid order by */
#define GRIB_INVALID_ORDERBY		-33
/** Missing a key from the fieldset */
#define GRIB_MISSING_KEY		-34
/** The point is out of the grid area */
#define GRIB_OUT_OF_AREA		-35
/** Concept no match */
#define GRIB_CONCEPT_NO_MATCH		-36
/** Hash array no match */
#define GRIB_HASH_ARRAY_NO_MATCH		-37
/** Definitions files not found */
#define GRIB_NO_DEFINITIONS		-38
/** Wrong type while packing */
#define GRIB_WRONG_TYPE		-39
/** End of resource */
#define GRIB_END		-40
/** Unable to code a field without values */
#define GRIB_NO_VALUES		-41
/** Grid description is wrong or inconsistent */
#define GRIB_WRONG_GRID		-42
/** End of index reached */
#define GRIB_END_OF_INDEX		-43
/** Null index */
#define GRIB_NULL_INDEX		-44
/** End of resource reached when reading message */
#define GRIB_PREMATURE_END_OF_FILE		-45
/** An internal array is too small */
#define GRIB_INTERNAL_ARRAY_TOO_SMALL		-46
/** Message is too large for the current architecture */
#define GRIB_MESSAGE_TOO_LARGE		-47
/** Constant field */
#define GRIB_CONSTANT_FIELD		-48
/** Switch unable to find a matching case */
#define GRIB_SWITCH_NO_MATCH		-49
/** Underflow */
#define GRIB_UNDERFLOW		-50
/** Message malformed */
#define GRIB_MESSAGE_MALFORMED		-51
/** Index is corrupted */
#define GRIB_CORRUPTED_INDEX		-52
/** Invalid number of bits per value */
#define GRIB_INVALID_BPV		-53
/** Edition of two messages is different */
#define GRIB_DIFFERENT_EDITION		-54
/** Value is different */
#define GRIB_VALUE_DIFFERENT		-55
/** Invalid key value */
#define GRIB_INVALID_KEY_VALUE		-56
/** String is smaller than requested */
#define GRIB_STRING_TOO_SMALL		-57
/** Wrong type conversion */
#define GRIB_WRONG_CONVERSION		-58
/** Missing BUFR table entry for descriptor */
#define GRIB_MISSING_BUFR_ENTRY		-59
/** Null pointer */
#define GRIB_NULL_POINTER		-60
/** Attribute is already present, cannot add */
#define GRIB_ATTRIBUTE_CLASH		-61
/** Too many attributes. Increase MAX_ACCESSOR_ATTRIBUTES */
#define GRIB_TOO_MANY_ATTRIBUTES		-62
/** Attribute not found. */
#define GRIB_ATTRIBUTE_NOT_FOUND		-63
/** Edition not supported. */
#define GRIB_UNSUPPORTED_EDITION		-64
/** Value out of coding range */
#define GRIB_OUT_OF_RANGE		-65
/** Size of bitmap is incorrect */
#define GRIB_WRONG_BITMAP_SIZE		-66
/** Functionality not enabled */
#define GRIB_FUNCTIONALITY_NOT_ENABLED		-67
/** Value mismatch */
#define GRIB_VALUE_MISMATCH		-68
/** Double values are different */
#define GRIB_DOUBLE_VALUE_MISMATCH		-69
/** Long values are different */
#define GRIB_LONG_VALUE_MISMATCH		-70
/** Byte values are different */
#define GRIB_BYTE_VALUE_MISMATCH		-71
/** String values are different */
#define GRIB_STRING_VALUE_MISMATCH		-72
/** Offset mismatch */
#define GRIB_OFFSET_MISMATCH		-73
/** Count mismatch */
#define GRIB_COUNT_MISMATCH		-74
/** Name mismatch */
#define GRIB_NAME_MISMATCH		-75
/** Type mismatch */
#define GRIB_TYPE_MISMATCH		-76
/** Type and value mismatch */
#define GRIB_TYPE_AND_VALUE_MISMATCH		-77
/** Unable to compare accessors */
#define GRIB_UNABLE_TO_COMPARE_ACCESSORS		-78
/** Assertion failure */
#define GRIB_ASSERTION_FAILURE		-79
/*! @}*/
#endif
