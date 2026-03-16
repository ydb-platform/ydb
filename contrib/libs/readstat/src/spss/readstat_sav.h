//
//  readstat_sav.h
//

#include "readstat_spss.h"

#pragma pack(push, 1)

// SAV files

typedef struct sav_file_header_record_s {
    char     rec_type[4];
    char     prod_name[60];
    int32_t  layout_code;
    int32_t  nominal_case_size;
    int32_t  compression;
    int32_t  weight_index;
    int32_t  ncases;
    double   bias; /* TODO is this portable? */
    char     creation_date[9];
    char     creation_time[8];
    char     file_label[64];
    char     padding[3];
} sav_file_header_record_t;

typedef struct sav_variable_record_s {
    int32_t  type;
    int32_t  has_var_label;
    int32_t  n_missing_values;
    int32_t  print;
    int32_t  write;
    char     name[8];
} sav_variable_record_t;

typedef struct sav_info_record_header_s {
    int32_t  rec_type;
    int32_t  subtype;
    int32_t  size;
    int32_t  count;
} sav_info_record_t;

typedef struct sav_machine_integer_info_record_s {
    int32_t  version_major;
    int32_t  version_minor;
    int32_t  version_revision;
    int32_t  machine_code;
    int32_t  floating_point_rep;
    int32_t  compression_code;
    int32_t  endianness;
    int32_t  character_code;
} sav_machine_integer_info_record_t;

typedef struct sav_machine_floating_point_info_record_s {
    uint64_t sysmis;
    uint64_t highest;
    uint64_t lowest;
} sav_machine_floating_point_info_record_t;

typedef struct sav_dictionary_termination_record_s {
    int32_t  rec_type;
    int32_t  filler;
} sav_dictionary_termination_record_t;

#pragma pack(pop)

typedef struct sav_ctx_s {
    readstat_callbacks_t  handle;
    size_t                file_size;
    readstat_io_t        *io;
    void                 *user_ctx;

    spss_varinfo_t      **varinfo;
    size_t                varinfo_capacity;
    readstat_variable_t **variables;

    const char    *input_encoding;
    const char    *output_encoding;
    char           file_label[4*64+1];
    time_t         timestamp;
    uint32_t      *variable_display_values;
    size_t         variable_display_values_count;
    iconv_t        converter;
    int            var_index;
    int            var_offset;
    int            var_count;
    int            record_count;
    int            row_limit;
    int            row_offset;
    int            current_row;
    int            value_labels_count;
    int            fweight_index;

    char          *raw_string;
    size_t         raw_string_len;

    char          *utf8_string;
    size_t         utf8_string_len;

    uint64_t       missing_double;
    uint64_t       lowest_double;
    uint64_t       highest_double;

    double         bias;
    int            format_version;

    readstat_compress_t  compression;
    readstat_endian_t   endianness;
    unsigned int   bswap:1;
} sav_ctx_t;

#define SAV_RECORD_TYPE_VARIABLE                2
#define SAV_RECORD_TYPE_VALUE_LABEL             3
#define SAV_RECORD_TYPE_VALUE_LABEL_VARIABLES   4
#define SAV_RECORD_TYPE_DOCUMENT                6
#define SAV_RECORD_TYPE_HAS_DATA                7
#define SAV_RECORD_TYPE_DICT_TERMINATION        999

#define SAV_RECORD_SUBTYPE_INTEGER_INFO       3
#define SAV_RECORD_SUBTYPE_FP_INFO            4
#define SAV_RECORD_SUBTYPE_PRODUCT_INFO      10
#define SAV_RECORD_SUBTYPE_VAR_DISPLAY       11
#define SAV_RECORD_SUBTYPE_LONG_VAR_NAME     13
#define SAV_RECORD_SUBTYPE_VERY_LONG_STR     14
#define SAV_RECORD_SUBTYPE_NUMBER_OF_CASES   16
#define SAV_RECORD_SUBTYPE_DATA_FILE_ATTRS   17
#define SAV_RECORD_SUBTYPE_VARIABLE_ATTRS    18
#define SAV_RECORD_SUBTYPE_CHAR_ENCODING     20
#define SAV_RECORD_SUBTYPE_LONG_STRING_VALUE_LABELS   21
#define SAV_RECORD_SUBTYPE_LONG_STRING_MISSING_VALUES 22

#define SAV_FLOATING_POINT_REP_IEEE      1
#define SAV_FLOATING_POINT_REP_IBM       2
#define SAV_FLOATING_POINT_REP_VAX       3

#define SAV_ENDIANNESS_BIG               1
#define SAV_ENDIANNESS_LITTLE            2

#define SAV_EIGHT_SPACES              "        "

sav_ctx_t *sav_ctx_init(sav_file_header_record_t *header, readstat_io_t *io);
void sav_ctx_free(sav_ctx_t *ctx);
