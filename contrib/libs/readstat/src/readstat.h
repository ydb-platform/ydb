//
//  readstat.h - API and internal data structures for ReadStat
//
//  Copyright Evan Miller and ReadStat authors (see LICENSE)
//

#ifndef INCLUDE_READSTAT_H
#define INCLUDE_READSTAT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sys/types.h>
#include <string.h>
#include <math.h>
#include <stdio.h>

enum {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE
};

typedef enum readstat_type_e {
    READSTAT_TYPE_STRING,
    READSTAT_TYPE_INT8,
    READSTAT_TYPE_INT16,
    READSTAT_TYPE_INT32,
    READSTAT_TYPE_FLOAT,
    READSTAT_TYPE_DOUBLE,
    READSTAT_TYPE_STRING_REF
} readstat_type_t;

typedef enum readstat_type_class_e {
    READSTAT_TYPE_CLASS_STRING,
    READSTAT_TYPE_CLASS_NUMERIC
} readstat_type_class_t;

typedef enum readstat_measure_e {
    READSTAT_MEASURE_UNKNOWN,
    READSTAT_MEASURE_NOMINAL = 1,
    READSTAT_MEASURE_ORDINAL,
    READSTAT_MEASURE_SCALE
} readstat_measure_t;

typedef enum readstat_alignment_e {
    READSTAT_ALIGNMENT_UNKNOWN,
    READSTAT_ALIGNMENT_LEFT = 1,
    READSTAT_ALIGNMENT_CENTER,
    READSTAT_ALIGNMENT_RIGHT
} readstat_alignment_t;

typedef enum readstat_compress_e {
    READSTAT_COMPRESS_NONE,
    READSTAT_COMPRESS_ROWS,
    READSTAT_COMPRESS_BINARY
} readstat_compress_t;

typedef enum readstat_endian_e {
    READSTAT_ENDIAN_NONE,
    READSTAT_ENDIAN_LITTLE,
    READSTAT_ENDIAN_BIG
} readstat_endian_t;

typedef enum readstat_error_e {
    READSTAT_OK,
    READSTAT_ERROR_OPEN = 1,
    READSTAT_ERROR_READ,
    READSTAT_ERROR_MALLOC,
    READSTAT_ERROR_USER_ABORT,
    READSTAT_ERROR_PARSE,
    READSTAT_ERROR_UNSUPPORTED_COMPRESSION,
    READSTAT_ERROR_UNSUPPORTED_CHARSET,
    READSTAT_ERROR_COLUMN_COUNT_MISMATCH,
    READSTAT_ERROR_ROW_COUNT_MISMATCH,
    READSTAT_ERROR_ROW_WIDTH_MISMATCH,
    READSTAT_ERROR_BAD_FORMAT_STRING,
    READSTAT_ERROR_VALUE_TYPE_MISMATCH,
    READSTAT_ERROR_WRITE,
    READSTAT_ERROR_WRITER_NOT_INITIALIZED,
    READSTAT_ERROR_SEEK,
    READSTAT_ERROR_CONVERT,
    READSTAT_ERROR_CONVERT_BAD_STRING,
    READSTAT_ERROR_CONVERT_SHORT_STRING,
    READSTAT_ERROR_CONVERT_LONG_STRING,
    READSTAT_ERROR_NUMERIC_VALUE_IS_OUT_OF_RANGE,
    READSTAT_ERROR_TAGGED_VALUE_IS_OUT_OF_RANGE,
    READSTAT_ERROR_STRING_VALUE_IS_TOO_LONG,
    READSTAT_ERROR_TAGGED_VALUES_NOT_SUPPORTED,
    READSTAT_ERROR_UNSUPPORTED_FILE_FORMAT_VERSION,
    READSTAT_ERROR_NAME_BEGINS_WITH_ILLEGAL_CHARACTER,
    READSTAT_ERROR_NAME_CONTAINS_ILLEGAL_CHARACTER,
    READSTAT_ERROR_NAME_IS_RESERVED_WORD,
    READSTAT_ERROR_NAME_IS_TOO_LONG,
    READSTAT_ERROR_BAD_TIMESTAMP_STRING,
    READSTAT_ERROR_BAD_FREQUENCY_WEIGHT,
    READSTAT_ERROR_TOO_MANY_MISSING_VALUE_DEFINITIONS,
    READSTAT_ERROR_NOTE_IS_TOO_LONG,
    READSTAT_ERROR_STRING_REFS_NOT_SUPPORTED,
    READSTAT_ERROR_STRING_REF_IS_REQUIRED,
    READSTAT_ERROR_ROW_IS_TOO_WIDE_FOR_PAGE,
    READSTAT_ERROR_TOO_FEW_COLUMNS,
    READSTAT_ERROR_TOO_MANY_COLUMNS,
    READSTAT_ERROR_NAME_IS_ZERO_LENGTH,
    READSTAT_ERROR_BAD_TIMESTAMP_VALUE
} readstat_error_t;

const char *readstat_error_message(readstat_error_t error_code);

typedef struct readstat_metadata_s {
    int64_t     row_count;
    int64_t     var_count;
    time_t      creation_time;
    time_t      modified_time;
    int64_t     file_format_version;
    readstat_compress_t compression;
    readstat_endian_t endianness;
    const char *table_name;
    const char *file_label;
    const char *file_encoding;
    unsigned int is64bit:1;
} readstat_metadata_t;

/* If the row count is unknown (e.g. it's an XPORT or POR file, or an SAV
 * file created with non-conforming software), then readstat_get_row_count
 * returns -1.
 */
int readstat_get_row_count(readstat_metadata_t *metadata);
int readstat_get_var_count(readstat_metadata_t *metadata);
time_t readstat_get_creation_time(readstat_metadata_t *metadata);
time_t readstat_get_modified_time(readstat_metadata_t *metadata);
int readstat_get_file_format_version(readstat_metadata_t *metadata);
int readstat_get_file_format_is_64bit(readstat_metadata_t *metadata);
readstat_compress_t readstat_get_compression(readstat_metadata_t *metadata);
readstat_endian_t readstat_get_endianness(readstat_metadata_t *metadata);
const char *readstat_get_table_name(readstat_metadata_t *metadata);
const char *readstat_get_file_label(readstat_metadata_t *metadata);
const char *readstat_get_file_encoding(readstat_metadata_t *metadata);

typedef struct readstat_value_s {
    union {
        float       float_value;
        double      double_value;
        int8_t      i8_value;
        int16_t     i16_value;
        int32_t     i32_value;
        const char *string_value;
    } v;
    readstat_type_t         type;
    char                    tag;
    unsigned int            is_system_missing:1;
    unsigned int            is_tagged_missing:1;
} readstat_value_t;

/* Internal data structures */
typedef struct readstat_value_label_s {
    double      double_key;
    int32_t     int32_key;
    char        tag;

    char       *string_key;
    size_t      string_key_len;

    char       *label;
    size_t      label_len;
} readstat_value_label_t;

typedef struct readstat_label_set_s {
    readstat_type_t            type;
    char                        name[256];

    readstat_value_label_t     *value_labels;
    long                        value_labels_count;
    long                        value_labels_capacity;

    void                       *variables;
    long                        variables_count;
    long                        variables_capacity;
} readstat_label_set_t;

typedef struct readstat_missingness_s {
    readstat_value_t missing_ranges[32];
    long             missing_ranges_count;
} readstat_missingness_t;

typedef struct readstat_variable_s {
    readstat_type_t         type;
    int                     index;
    char                    name[300];
    char                    format[256];
    char                    label[1024];
    readstat_label_set_t   *label_set;
    off_t                   offset;
    size_t                  storage_width;
    size_t                  user_width;
    readstat_missingness_t  missingness;
    readstat_measure_t      measure;
    readstat_alignment_t    alignment;
    int                     display_width;
    int                     decimals;
    int                     skip;
    int                     index_after_skipping;
} readstat_variable_t;

typedef struct readstat_schema_entry_s {
    uint32_t            row;
    uint32_t            col;
    uint32_t            len;
    int                 skip;
    readstat_variable_t variable;
    char                labelset[32];
    char                decimal_separator;
} readstat_schema_entry_t;

typedef struct readstat_schema_s {
    char                    filename[255];
    uint32_t                rows_per_observation;
    uint32_t                cols_per_observation;
    int                     first_line;
    int                     entry_count;
    char                    field_delimiter;
    readstat_schema_entry_t *entries;
} readstat_schema_t;

/* Value accessors */
readstat_type_t readstat_value_type(readstat_value_t value);
readstat_type_class_t readstat_value_type_class(readstat_value_t value);

/* Values can be missing in one of three ways:
 * 1. "System missing", delivered to value handlers as NaN. Occurs in all file
 *    types. The most common kind of missing value.
 * 2. Tagged missing, also delivered as NaN, but with a single character tag
 *    accessible via readstat_value_tag(). The tag might be 'a', 'b', etc,
 *    corresponding to Stata's .a, .b, values etc. Occurs only in Stata and
 *    SAS files.
 * 3. Defined missing. The value is a real number but is to be treated as
 *    missing according to the variable's missingness rules (such as "value < 0 ||
 *    value == 999"). Occurs only in SPSS files. access the rules via:
 *
 *       readstat_variable_get_missing_ranges_count()
 *       readstat_variable_get_missing_range_lo()
 *       readstat_variable_get_missing_range_hi()
 *
 * Note that "ranges" include individual values where lo == hi.
 *
 * readstat_value_is_missing() is equivalent to:
 *
 *    (readstat_value_is_system_missing() 
 *     || readstat_value_is_tagged_missing()
 *     || readstat_value_is_defined_missing())
 */
int readstat_value_is_missing(readstat_value_t value, readstat_variable_t *variable);
int readstat_value_is_system_missing(readstat_value_t value);
int readstat_value_is_tagged_missing(readstat_value_t value);
int readstat_value_is_defined_missing(readstat_value_t value, readstat_variable_t *variable);
char readstat_value_tag(readstat_value_t value);

char readstat_int8_value(readstat_value_t value);
int16_t readstat_int16_value(readstat_value_t value);
int32_t readstat_int32_value(readstat_value_t value);
float readstat_float_value(readstat_value_t value);
double readstat_double_value(readstat_value_t value);
const char *readstat_string_value(readstat_value_t value);

readstat_type_class_t readstat_type_class(readstat_type_t type);

/* Accessor methods for use inside variable handlers */
int readstat_variable_get_index(const readstat_variable_t *variable);
int readstat_variable_get_index_after_skipping(const readstat_variable_t *variable);
const char *readstat_variable_get_name(const readstat_variable_t *variable);
const char *readstat_variable_get_label(const readstat_variable_t *variable);
const char *readstat_variable_get_format(const readstat_variable_t *variable);
readstat_type_t readstat_variable_get_type(const readstat_variable_t *variable);
readstat_type_class_t readstat_variable_get_type_class(const readstat_variable_t *variable);
size_t readstat_variable_get_storage_width(const readstat_variable_t *variable);
int readstat_variable_get_display_width(const readstat_variable_t *variable);
readstat_measure_t readstat_variable_get_measure(const readstat_variable_t *variable);
readstat_alignment_t readstat_variable_get_alignment(const readstat_variable_t *variable);

int readstat_variable_get_missing_ranges_count(const readstat_variable_t *variable);
readstat_value_t readstat_variable_get_missing_range_lo(const readstat_variable_t *variable, int i);
readstat_value_t readstat_variable_get_missing_range_hi(const readstat_variable_t *variable, int i);

/* Callbacks should return 0 (aka READSTAT_HANDLER_OK) on success and 1 (aka READSTAT_HANDLER_ABORT) to abort. */
/* If the variable handler returns READSTAT_HANDLER_SKIP_VARIABLE, the value handler will not be called on
 * the associated variable. (Note that subsequent variables will retain their original index values.)
 */
typedef int (*readstat_metadata_handler)(readstat_metadata_t *metadata, void *ctx);
typedef int (*readstat_note_handler)(int note_index, const char *note, void *ctx);
typedef int (*readstat_variable_handler)(int index, readstat_variable_t *variable, 
        const char *val_labels, void *ctx);
typedef int (*readstat_fweight_handler)(readstat_variable_t *variable, void *ctx);
typedef int (*readstat_value_handler)(int obs_index, readstat_variable_t *variable,
        readstat_value_t value, void *ctx);
typedef int (*readstat_value_label_handler)(const char *val_labels, 
        readstat_value_t value, const char *label, void *ctx);
typedef void (*readstat_error_handler)(const char *error_message, void *ctx);
typedef int (*readstat_progress_handler)(double progress, void *ctx);

#if defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
typedef __int64 readstat_off_t;
#elif defined _WIN32 || defined __CYGWIN__
typedef _off64_t readstat_off_t;
#elif defined _AIX
typedef off64_t readstat_off_t;
#else
typedef off_t readstat_off_t;
#endif

typedef enum readstat_io_flags_e {
    READSTAT_SEEK_SET,
    READSTAT_SEEK_CUR,
    READSTAT_SEEK_END
} readstat_io_flags_t;

typedef int (*readstat_open_handler)(const char *path, void *io_ctx);
typedef int (*readstat_close_handler)(void *io_ctx);
typedef readstat_off_t (*readstat_seek_handler)(readstat_off_t offset, readstat_io_flags_t whence, void *io_ctx);
typedef ssize_t (*readstat_read_handler)(void *buf, size_t nbyte, void *io_ctx);
typedef readstat_error_t (*readstat_update_handler)(long file_size, readstat_progress_handler progress_handler, void *user_ctx, void *io_ctx);

typedef struct readstat_io_s {
    readstat_open_handler          open;
    readstat_close_handler         close;
    readstat_seek_handler          seek;
    readstat_read_handler          read;
    readstat_update_handler        update;
    void                          *io_ctx;
    int                            io_ctx_needs_free;
} readstat_io_t;

typedef struct readstat_callbacks_s {
    readstat_metadata_handler      metadata;
    readstat_note_handler          note;
    readstat_variable_handler      variable;
    readstat_fweight_handler       fweight;
    readstat_value_handler         value;
    readstat_value_label_handler   value_label;
    readstat_error_handler         error;
    readstat_progress_handler      progress;
} readstat_callbacks_t;

typedef struct readstat_parser_s {
    readstat_callbacks_t    handlers;
    readstat_io_t          *io;
    const char             *input_encoding;
    const char             *output_encoding;
    long                    row_limit;
    long                    row_offset;
} readstat_parser_t;

readstat_parser_t *readstat_parser_init(void);
void readstat_parser_free(readstat_parser_t *parser);
void readstat_io_free(readstat_io_t *io);

readstat_error_t readstat_set_metadata_handler(readstat_parser_t *parser, readstat_metadata_handler metadata_handler);
readstat_error_t readstat_set_note_handler(readstat_parser_t *parser, readstat_note_handler note_handler);
readstat_error_t readstat_set_variable_handler(readstat_parser_t *parser, readstat_variable_handler variable_handler);
readstat_error_t readstat_set_fweight_handler(readstat_parser_t *parser, readstat_fweight_handler fweight_handler);
readstat_error_t readstat_set_value_handler(readstat_parser_t *parser, readstat_value_handler value_handler);
readstat_error_t readstat_set_value_label_handler(readstat_parser_t *parser, readstat_value_label_handler value_label_handler);
readstat_error_t readstat_set_error_handler(readstat_parser_t *parser, readstat_error_handler error_handler);
readstat_error_t readstat_set_progress_handler(readstat_parser_t *parser, readstat_progress_handler progress_handler);

readstat_error_t readstat_set_open_handler(readstat_parser_t *parser, readstat_open_handler open_handler);
readstat_error_t readstat_set_close_handler(readstat_parser_t *parser, readstat_close_handler close_handler);
readstat_error_t readstat_set_seek_handler(readstat_parser_t *parser, readstat_seek_handler seek_handler);
readstat_error_t readstat_set_read_handler(readstat_parser_t *parser, readstat_read_handler read_handler);
readstat_error_t readstat_set_update_handler(readstat_parser_t *parser, readstat_update_handler update_handler);
readstat_error_t readstat_set_io_ctx(readstat_parser_t *parser, void *io_ctx);

// Usually inferred from the file, but sometimes a manual override is desirable.
// In particular, pre-14 Stata uses the system encoding, which is usually Win 1252
// but could be anything. `encoding' should be an iconv-compatible name.
readstat_error_t readstat_set_file_character_encoding(readstat_parser_t *parser, const char *encoding);

// Defaults to UTF-8. Pass in NULL to disable transliteration.
readstat_error_t readstat_set_handler_character_encoding(readstat_parser_t *parser, const char *encoding);

readstat_error_t readstat_set_row_limit(readstat_parser_t *parser, long row_limit);
readstat_error_t readstat_set_row_offset(readstat_parser_t *parser, long row_offset);

/* Parse binary / portable files */
readstat_error_t readstat_parse_dta(readstat_parser_t *parser, const char *path, void *user_ctx);
readstat_error_t readstat_parse_sav(readstat_parser_t *parser, const char *path, void *user_ctx);
readstat_error_t readstat_parse_por(readstat_parser_t *parser, const char *path, void *user_ctx);
readstat_error_t readstat_parse_sas7bdat(readstat_parser_t *parser, const char *path, void *user_ctx);
readstat_error_t readstat_parse_sas7bcat(readstat_parser_t *parser, const char *path, void *user_ctx);
readstat_error_t readstat_parse_xport(readstat_parser_t *parser, const char *path, void *user_ctx);

/* Parse a schema file... */
readstat_schema_t *readstat_parse_sas_commands(readstat_parser_t *parser,
    const char *filepath, void *user_ctx, readstat_error_t *outError);
readstat_schema_t *readstat_parse_spss_commands(readstat_parser_t *parser,
    const char *filepath, void *user_ctx, readstat_error_t *outError);
readstat_schema_t *readstat_parse_stata_dictionary(readstat_parser_t *parser,
    const char *filepath, void *user_ctx, readstat_error_t *outError);

/* ... then pass the schema to the plain-text parser ... */
readstat_error_t readstat_parse_txt(readstat_parser_t *parser, const char *filename, 
        readstat_schema_t *schema, void *user_ctx);

/* ... and free the schema structure */
void readstat_schema_free(readstat_schema_t *schema);

/* Internal module callbacks */
typedef struct readstat_string_ref_s {
    int64_t     first_v;
    int64_t     first_o;
    size_t      len;
    char        data[1]; // Flexible array; using [1] for C++98 compatibility
} readstat_string_ref_t;

typedef size_t (*readstat_variable_width_callback)(readstat_type_t type, size_t user_width);
typedef readstat_error_t (*readstat_variable_ok_callback)(const readstat_variable_t *variable);

typedef readstat_error_t (*readstat_write_int8_callback)(void *row_data, const readstat_variable_t *variable, int8_t value);
typedef readstat_error_t (*readstat_write_int16_callback)(void *row_data, const readstat_variable_t *variable, int16_t value);
typedef readstat_error_t (*readstat_write_int32_callback)(void *row_data, const readstat_variable_t *variable, int32_t value);
typedef readstat_error_t (*readstat_write_float_callback)(void *row_data, const readstat_variable_t *variable, float value);
typedef readstat_error_t (*readstat_write_double_callback)(void *row_data, const readstat_variable_t *variable, double value);
typedef readstat_error_t (*readstat_write_string_callback)(void *row_data, const readstat_variable_t *variable, const char *value);
typedef readstat_error_t (*readstat_write_string_ref_callback)(void *row_data, const readstat_variable_t *variable, readstat_string_ref_t *ref);
typedef readstat_error_t (*readstat_write_missing_callback)(void *row_data, const readstat_variable_t *variable);
typedef readstat_error_t (*readstat_write_tagged_callback)(void *row_data, const readstat_variable_t *variable, char tag);

typedef readstat_error_t (*readstat_begin_data_callback)(void *writer);
typedef readstat_error_t (*readstat_write_row_callback)(void *writer, void *row_data, size_t row_len);
typedef readstat_error_t (*readstat_end_data_callback)(void *writer);
typedef void (*readstat_module_ctx_free_callback)(void *module_ctx);
typedef readstat_error_t (*readstat_metadata_ok_callback)(void *writer);

typedef struct readstat_writer_callbacks_s {
    readstat_variable_width_callback    variable_width;
    readstat_variable_ok_callback       variable_ok;
    readstat_write_int8_callback        write_int8;
    readstat_write_int16_callback       write_int16;
    readstat_write_int32_callback       write_int32;
    readstat_write_float_callback       write_float;
    readstat_write_double_callback      write_double;
    readstat_write_string_callback      write_string;
    readstat_write_string_ref_callback  write_string_ref;
    readstat_write_missing_callback     write_missing_string;
    readstat_write_missing_callback     write_missing_number;
    readstat_write_tagged_callback      write_missing_tagged;
    readstat_begin_data_callback        begin_data;
    readstat_write_row_callback         write_row;
    readstat_end_data_callback          end_data;
    readstat_module_ctx_free_callback   module_ctx_free;
    readstat_metadata_ok_callback       metadata_ok;
} readstat_writer_callbacks_t;

/* You'll need to define one of these to get going. Should return # bytes written,
 * or -1 on error, a la write(2) */
typedef ssize_t (*readstat_data_writer)(const void *data, size_t len, void *ctx);

typedef struct readstat_writer_s {
    readstat_data_writer        data_writer;
    size_t                      bytes_written;
    long                        version;
    int                         is_64bit; // SAS only
    readstat_compress_t         compression;
    time_t                      timestamp;

    readstat_variable_t       **variables;
    long                        variables_count;
    long                        variables_capacity;

    readstat_label_set_t      **label_sets;
    long                        label_sets_count;
    long                        label_sets_capacity;

    char                      **notes;
    long                        notes_count;
    long                        notes_capacity;

    readstat_string_ref_t    **string_refs;
    long                       string_refs_count;
    long                       string_refs_capacity;

    unsigned char              *row;
    size_t                      row_len;

    int                         row_count;
    int                         current_row;
    char                        file_label[257];
    char                        table_name[33];
    const readstat_variable_t  *fweight_variable;

    readstat_writer_callbacks_t callbacks;
    readstat_error_handler      error_handler;

    void                       *module_ctx;
    void                       *user_ctx;

    int                         initialized;
} readstat_writer_t;

/* Writer API */


// First call this...
readstat_writer_t *readstat_writer_init(void);

// Then specify a function that will handle the output bytes...
readstat_error_t readstat_set_data_writer(readstat_writer_t *writer, readstat_data_writer data_writer);

// Next define your value labels, if any. Create as many named sets as you'd like.
readstat_label_set_t *readstat_add_label_set(readstat_writer_t *writer, readstat_type_t type, const char *name);
void readstat_label_double_value(readstat_label_set_t *label_set, double value, const char *label);
void readstat_label_int32_value(readstat_label_set_t *label_set, int32_t value, const char *label);
void readstat_label_string_value(readstat_label_set_t *label_set, const char *value, const char *label);
void readstat_label_tagged_value(readstat_label_set_t *label_set, char tag, const char *label);

// Now define your variables. Note that `storage_width' is used for:
// * READSTAT_TYPE_STRING variables in all formats
// * READSTAT_TYPE_DOUBLE variables, but only in the SAS XPORT format (valid values 3-8, defaults to 8)
readstat_variable_t *readstat_add_variable(readstat_writer_t *writer, const char *name, readstat_type_t type, 
        size_t storage_width);
void readstat_variable_set_label(readstat_variable_t *variable, const char *label);
void readstat_variable_set_format(readstat_variable_t *variable, const char *format);
void readstat_variable_set_label_set(readstat_variable_t *variable, readstat_label_set_t *label_set);
void readstat_variable_set_measure(readstat_variable_t *variable, readstat_measure_t measure);
void readstat_variable_set_alignment(readstat_variable_t *variable, readstat_alignment_t alignment);
void readstat_variable_set_display_width(readstat_variable_t *variable, int display_width);
readstat_error_t readstat_variable_add_missing_double_value(readstat_variable_t *variable, double value);
readstat_error_t readstat_variable_add_missing_double_range(readstat_variable_t *variable, double lo, double hi);
readstat_error_t readstat_variable_add_missing_string_value(readstat_variable_t *variable, const char *value);
readstat_error_t readstat_variable_add_missing_string_range(readstat_variable_t *variable, const char *lo, const char *hi);
readstat_variable_t *readstat_get_variable(readstat_writer_t *writer, int index);

// "Notes" appear in the file metadata. In SPSS these are stored as
// lines in the Document Record; in Stata these are stored using
// the "notes" feature.
// 
// Note that the line length in SPSS is 80 characters; ReadStat will
// produce a write error if a note is longer than this limit.
void readstat_add_note(readstat_writer_t *writer, const char *note);

// String refs are used for creating a READSTAT_TYPE_STRING_REF column,
// which is only supported in Stata. String references can be shared
// across columns, and inserted with readstat_insert_string_ref().
readstat_string_ref_t *readstat_add_string_ref(readstat_writer_t *writer, const char *string);
readstat_string_ref_t *readstat_get_string_ref(readstat_writer_t *writer, int index);

// Optional metadata
readstat_error_t readstat_writer_set_file_label(readstat_writer_t *writer, const char *file_label);
readstat_error_t readstat_writer_set_file_timestamp(readstat_writer_t *writer, time_t timestamp);
readstat_error_t readstat_writer_set_fweight_variable(readstat_writer_t *writer, const readstat_variable_t *variable);

readstat_error_t readstat_writer_set_file_format_version(readstat_writer_t *writer, 
        uint8_t file_format_version);
// e.g. 104-119 for DTA; 5 or 8 for SAS Transport.
// SAV files support 2 or 3, where 3 is equivalent to setting 
// readstat_writer_set_compression(READSTAT_COMPRESS_BINARY)

readstat_error_t readstat_writer_set_table_name(readstat_writer_t *writer, const char *table_name);
// Only used in XPORT files at the moment (defaults to DATASET)

readstat_error_t readstat_writer_set_file_format_is_64bit(readstat_writer_t *writer,
        int is_64bit); // applies only to SAS files; defaults to 1=true
readstat_error_t readstat_writer_set_compression(readstat_writer_t *writer,
        readstat_compress_t compression); 
        // READSTAT_COMPRESS_BINARY is supported only with SAV files (i.e. ZSAV files)
        // READSTAT_COMPRESS_ROWS is supported only with sas7bdat and SAV files

// Optional error handler
readstat_error_t readstat_writer_set_error_handler(readstat_writer_t *writer, 
        readstat_error_handler error_handler);

// Call one of these at any time before the first invocation of readstat_begin_row
readstat_error_t readstat_begin_writing_dta(readstat_writer_t *writer, void *user_ctx, long row_count);
readstat_error_t readstat_begin_writing_por(readstat_writer_t *writer, void *user_ctx, long row_count);
readstat_error_t readstat_begin_writing_sas7bcat(readstat_writer_t *writer, void *user_ctx);
readstat_error_t readstat_begin_writing_sas7bdat(readstat_writer_t *writer, void *user_ctx, long row_count);
readstat_error_t readstat_begin_writing_sav(readstat_writer_t *writer, void *user_ctx, long row_count);
readstat_error_t readstat_begin_writing_xport(readstat_writer_t *writer, void *user_ctx, long row_count);

// Optional, file-specific validation routines, to be called AFTER readstat_begin_writing_XXX
readstat_error_t readstat_validate_metadata(readstat_writer_t *writer);
readstat_error_t readstat_validate_variable(readstat_writer_t *writer, const readstat_variable_t *variable);

// Start a row of data (that is, a case or observation)
readstat_error_t readstat_begin_row(readstat_writer_t *writer);

// Then call one of these for each variable
readstat_error_t readstat_insert_int8_value(readstat_writer_t *writer, const readstat_variable_t *variable, int8_t value);
readstat_error_t readstat_insert_int16_value(readstat_writer_t *writer, const readstat_variable_t *variable, int16_t value);
readstat_error_t readstat_insert_int32_value(readstat_writer_t *writer, const readstat_variable_t *variable, int32_t value);
readstat_error_t readstat_insert_float_value(readstat_writer_t *writer, const readstat_variable_t *variable, float value);
readstat_error_t readstat_insert_double_value(readstat_writer_t *writer, const readstat_variable_t *variable, double value);
readstat_error_t readstat_insert_string_value(readstat_writer_t *writer, const readstat_variable_t *variable, const char *value);
readstat_error_t readstat_insert_string_ref(readstat_writer_t *writer, const readstat_variable_t *variable, readstat_string_ref_t *ref);
readstat_error_t readstat_insert_missing_value(readstat_writer_t *writer, const readstat_variable_t *variable);
readstat_error_t readstat_insert_tagged_missing_value(readstat_writer_t *writer, const readstat_variable_t *variable, char tag);

// Finally, close out the row
readstat_error_t readstat_end_row(readstat_writer_t *writer);

// Once you've written all the rows, clean up after yourself
readstat_error_t readstat_end_writing(readstat_writer_t *writer);
void readstat_writer_free(readstat_writer_t *writer);

#ifdef __cplusplus
}
#endif

#endif
