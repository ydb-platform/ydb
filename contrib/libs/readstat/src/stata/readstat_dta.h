#pragma pack(push, 1)

// DTA files

typedef struct dta_header_s {
    unsigned char    ds_format;
    unsigned char    byteorder;
    unsigned char    filetype;
    unsigned char    unused;
    uint16_t         nvar;
    uint32_t         nobs;
} dta_header_t;

typedef struct dta_header64_s {
    unsigned char    ds_format;
    unsigned char    byteorder;
    unsigned char    filetype;
    unsigned char    unused;
    uint32_t         nvar;
    uint64_t         nobs;
} dta_header64_t;

/* Some compilers will pack the elements, but still round up
 * the total size of the structure. So use macros in place of
 * sizeof(dta_117_strl_header_t)/sizeof(dta_118_strl_header_t).
 */
typedef struct dta_117_strl_header_s {
    uint32_t        v;
    uint32_t        o;
    unsigned char   type;
    int32_t         len;
} dta_117_strl_header_t;

#define SIZEOF_DTA_117_STRL_HEADER_T 13

typedef struct dta_118_strl_header_s {
    uint32_t        v;
    uint64_t        o;
    unsigned char   type;
    int32_t         len;
} dta_118_strl_header_t;

#define SIZEOF_DTA_118_STRL_HEADER_T 17

#pragma pack(pop)

typedef struct dta_strl_s {
    uint16_t        v;
    uint64_t        o;
    unsigned char   type;
    size_t          len;
    char            data[1]; // Flexible array; use [1] for C++98 compatibility
} dta_strl_t;

typedef struct dta_ctx_s {
    char          *data_label;
    size_t         data_label_len;
    size_t         data_label_len_len;
    time_t         timestamp;
    size_t         timestamp_len;
    char           typlist_version;
    size_t         typlist_entry_len;
    uint16_t      *typlist;
    size_t         typlist_len;
    char          *varlist;
    size_t         varlist_len;
    int16_t       *srtlist;
    size_t         srtlist_len;
    char          *fmtlist;
    size_t         fmtlist_len;
    char          *lbllist;
    size_t         lbllist_len;
    char          *variable_labels;
    size_t         variable_labels_len;

    size_t         variable_name_len;
    size_t         fmtlist_entry_len;
    size_t         lbllist_entry_len;
    size_t         variable_labels_entry_len;
    size_t         expansion_len_len;
    size_t         ch_metadata_len;
    size_t         value_label_table_len_len;
    size_t         value_label_table_labname_len;
    size_t         value_label_table_padding_len;

    size_t         strl_v_len;
    size_t         strl_o_len;

    int64_t        data_offset;
    int64_t        strls_offset;
    int64_t        value_labels_offset;

    int            ds_format;
    int            nvar;
    int64_t        nobs;
    size_t         record_len;
    int64_t        row_limit;
    int64_t        row_offset;
    int64_t        current_row;

    unsigned int   bswap:1;
    unsigned int   machine_is_twos_complement:1;
    unsigned int   file_is_xmlish:1;
    unsigned int   supports_tagged_missing:1;

    int8_t         max_int8;
    int16_t        max_int16;
    int32_t        max_int32;
    int32_t        max_float;
    int64_t        max_double;

    dta_strl_t   **strls;
    size_t         strls_count;
    size_t         strls_capacity;

    readstat_variable_t  **variables;
    readstat_endian_t    endianness;

    iconv_t              converter;
    readstat_callbacks_t handle;
    size_t               file_size;
    void                *user_ctx;
    readstat_io_t       *io;
    int                  initialized;

    char            error_buf[256];
} dta_ctx_t;

#define DTA_HILO  0x01
#define DTA_LOHI  0x02

#define DTA_OLD_MAX_INT8                     0x7e
#define DTA_OLD_MAX_INT16                  0x7ffe
#define DTA_OLD_MAX_INT32              0x7ffffffe
#define DTA_OLD_MAX_FLOAT              0x7effffff // +1.7e38f
#define DTA_OLD_MAX_DOUBLE     0x7fdfffffffffffffL // +8.9e307

#define DTA_OLD_MISSING_INT8                 0x7F
#define DTA_OLD_MISSING_INT16              0x7FFF
#define DTA_OLD_MISSING_INT32          0x7FFFFFFF
#define DTA_OLD_MISSING_FLOAT          0x7F000000
#define DTA_OLD_MISSING_DOUBLE 0x7FE0000000000000L

#define DTA_113_MAX_INT8                    0x64
#define DTA_113_MAX_INT16                 0x7fe4
#define DTA_113_MAX_INT32             0x7fffffe4
#define DTA_113_MAX_FLOAT             0x7effffff // +1.7e38f
#define DTA_113_MAX_DOUBLE    0x7fdfffffffffffffL // +8.9e307

#define DTA_113_MISSING_INT8                 0x65
#define DTA_113_MISSING_INT16              0x7FE5
#define DTA_113_MISSING_INT32          0x7FFFFFE5
#define DTA_113_MISSING_FLOAT          0x7F000000
#define DTA_113_MISSING_DOUBLE 0x7FE0000000000000L

#define DTA_113_MISSING_INT8_A    (DTA_113_MISSING_INT8+1)
#define DTA_113_MISSING_INT16_A   (DTA_113_MISSING_INT16+1)
#define DTA_113_MISSING_INT32_A   (DTA_113_MISSING_INT32+1)
#define DTA_113_MISSING_FLOAT_A   (DTA_113_MISSING_FLOAT+0x0800)
#define DTA_113_MISSING_DOUBLE_A  (DTA_113_MISSING_DOUBLE+0x010000000000)

#define DTA_GSO_TYPE_BINARY        0x81
#define DTA_GSO_TYPE_ASCII         0x82

#define DTA_117_TYPE_CODE_INT8     0xFFFA
#define DTA_117_TYPE_CODE_INT16    0xFFF9
#define DTA_117_TYPE_CODE_INT32    0xFFF8
#define DTA_117_TYPE_CODE_FLOAT    0xFFF7
#define DTA_117_TYPE_CODE_DOUBLE   0xFFF6
#define DTA_117_TYPE_CODE_STRL     0x8000

#define DTA_111_TYPE_CODE_INT8     0xFB
#define DTA_111_TYPE_CODE_INT16    0xFC
#define DTA_111_TYPE_CODE_INT32    0xFD
#define DTA_111_TYPE_CODE_FLOAT    0xFE
#define DTA_111_TYPE_CODE_DOUBLE   0xFF

#define DTA_OLD_TYPE_CODE_INT8     'b'
#define DTA_OLD_TYPE_CODE_INT16    'i'
#define DTA_OLD_TYPE_CODE_INT32    'l'
#define DTA_OLD_TYPE_CODE_FLOAT    'f'
#define DTA_OLD_TYPE_CODE_DOUBLE   'd'

dta_ctx_t *dta_ctx_alloc(readstat_io_t *io);
readstat_error_t dta_ctx_init(dta_ctx_t *ctx, uint32_t nvar, uint64_t nobs, 
        unsigned char byteorder, unsigned char ds_format,
        const char *input_encoding, const char *output_encoding);
void dta_ctx_free(dta_ctx_t *ctx);

readstat_error_t dta_type_info(uint16_t typecode, dta_ctx_t *ctx,
        size_t *max_len, readstat_type_t *out_type);
