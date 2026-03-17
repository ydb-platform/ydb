
#include "../readstat.h"
#include "../readstat_bits.h"

#pragma pack(push, 1)

typedef struct sas_header_start_s {
    unsigned char magic[32];
    unsigned char a2;
    unsigned char mystery1[2];
    unsigned char a1;
    unsigned char mystery2[1];
    unsigned char endian;
    unsigned char mystery3[1];
    char          file_format;
    unsigned char mystery4[30];
    unsigned char encoding;
    unsigned char mystery5[13];
    char          file_type[8];
    char          table_name[32];
    unsigned char mystery6[32];
    char          file_info[8];
} sas_header_start_t;

typedef struct sas_header_end_s {
    char          release[8];
    char          host[16];
    char          version[16];
    char          os_vendor[16];
    char          os_name[16];
    char          extra[48];
} sas_header_end_t;

#pragma pack(pop)

typedef struct sas_header_info_s {
    int      little_endian;
    int      u64;
    int      vendor;
    int      major_version;
    int      minor_version;
    int      revision;
    int      pad1;
    int64_t  page_size;
    int64_t  page_header_size;
    int64_t  subheader_pointer_size;
    int64_t  page_count;
    int64_t  header_size;
    time_t   creation_time;
    time_t   modification_time;
    char     table_name[32];
    char     file_label[256];
    char    *encoding;
} sas_header_info_t;

enum {
    READSTAT_VENDOR_STAT_TRANSFER,
    READSTAT_VENDOR_SAS
};

typedef struct sas_text_ref_s {
    uint16_t    index;
    uint16_t    offset;
    uint16_t    length;
} sas_text_ref_t;

#define SAS_ENDIAN_BIG       0x00
#define SAS_ENDIAN_LITTLE    0x01

#define SAS_FILE_FORMAT_UNIX    '1'
#define SAS_FILE_FORMAT_WINDOWS '2'

#define SAS_ALIGNMENT_OFFSET_0  0x22
#define SAS_ALIGNMENT_OFFSET_4  0x33

#define SAS_COLUMN_TYPE_NUM  0x01
#define SAS_COLUMN_TYPE_CHR  0x02

#define SAS_SUBHEADER_SIGNATURE_ROW_SIZE       0xF7F7F7F7
#define SAS_SUBHEADER_SIGNATURE_COLUMN_SIZE    0xF6F6F6F6
#define SAS_SUBHEADER_SIGNATURE_COUNTS         0xFFFFFC00
#define SAS_SUBHEADER_SIGNATURE_COLUMN_FORMAT  0xFFFFFBFE

#define SAS_SUBHEADER_SIGNATURE_COLUMN_MASK    0xFFFFFFF8
/* Seen in the wild: FA (unknown), F8 (locale?) */

#define SAS_SUBHEADER_SIGNATURE_COLUMN_ATTRS   0xFFFFFFFC
#define SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT    0xFFFFFFFD
#define SAS_SUBHEADER_SIGNATURE_COLUMN_LIST    0xFFFFFFFE
#define SAS_SUBHEADER_SIGNATURE_COLUMN_NAME    0xFFFFFFFF

#define SAS_PAGE_TYPE_META   0x0000
#define SAS_PAGE_TYPE_DATA   0x0100
#define SAS_PAGE_TYPE_MIX    0x0200
#define SAS_PAGE_TYPE_AMD    0x0400
#define SAS_PAGE_TYPE_MASK   0x0F00

#define SAS_PAGE_TYPE_META2  0x4000
#define SAS_PAGE_TYPE_COMP   0x9000

#define SAS_SUBHEADER_POINTER_SIZE_32BIT    12
#define SAS_SUBHEADER_POINTER_SIZE_64BIT    24

#define SAS_PAGE_HEADER_SIZE_32BIT  24
#define SAS_PAGE_HEADER_SIZE_64BIT  40

#define SAS_COMPRESSION_NONE   0x00
#define SAS_COMPRESSION_TRUNC  0x01
#define SAS_COMPRESSION_ROW    0x04

#define SAS_COMPRESSION_SIGNATURE_RLE  "SASYZCRL"
#define SAS_COMPRESSION_SIGNATURE_RDC  "SASYZCR2"

#define SAS_DEFAULT_FILE_VERSION  9

extern unsigned char sas7bdat_magic_number[32];
extern unsigned char sas7bcat_magic_number[32];

uint64_t sas_read8(const char *data, int bswap);
uint32_t sas_read4(const char *data, int bswap);
uint16_t sas_read2(const char *data, int bswap);
readstat_error_t sas_read_header(readstat_io_t *io, sas_header_info_t *ctx, readstat_error_handler error_handler, void *user_ctx);
size_t sas_subheader_remainder(size_t len, size_t signature_len);

sas_header_info_t *sas_header_info_init(readstat_writer_t *writer, int is_64bit);
readstat_error_t sas_write_header(readstat_writer_t *writer, sas_header_info_t *hinfo, sas_header_start_t header_start);
readstat_error_t sas_fill_page(readstat_writer_t *writer, sas_header_info_t *hinfo);
readstat_error_t sas_validate_variable(const readstat_variable_t *variable);
readstat_error_t sas_validate_name(const char *name, size_t max_len);
readstat_error_t sas_validate_tag(char tag);
void sas_assign_tag(readstat_value_t *value, uint8_t tag);
