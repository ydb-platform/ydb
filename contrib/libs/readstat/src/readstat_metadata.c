#include "readstat.h"

int readstat_get_row_count(readstat_metadata_t *metadata) {
    return metadata->row_count;
}

int readstat_get_var_count(readstat_metadata_t *metadata) {
    return metadata->var_count;
}

time_t readstat_get_creation_time(readstat_metadata_t *metadata) {
    return metadata->creation_time;
}

time_t readstat_get_modified_time(readstat_metadata_t *metadata) {
    return metadata->modified_time;
}

int readstat_get_file_format_version(readstat_metadata_t *metadata) {
    return metadata->file_format_version;
}

int readstat_get_file_format_is_64bit(readstat_metadata_t *metadata) {
    return metadata->is64bit;
}

readstat_compress_t readstat_get_compression(readstat_metadata_t *metadata) {
    return metadata->compression;
}

readstat_endian_t readstat_get_endianness(readstat_metadata_t *metadata) {
    return metadata->endianness;
}

const char *readstat_get_file_label(readstat_metadata_t *metadata) {
    return metadata->file_label;
}

const char *readstat_get_file_encoding(readstat_metadata_t *metadata) {
    return metadata->file_encoding;
}

const char *readstat_get_table_name(readstat_metadata_t *metadata) {
    return metadata->table_name;
}
