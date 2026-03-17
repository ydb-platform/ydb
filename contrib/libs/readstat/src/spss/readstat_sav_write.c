
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <stdint.h>
#include <math.h>
#include <float.h>
#include <time.h>
#include <ctype.h>

#include "../readstat.h"
#include "../readstat_iconv.h"
#include "../readstat_bits.h"
#include "../readstat_malloc.h"
#include "../readstat_writer.h"

#include "../CKHashTable.h"

#include "readstat_sav.h"
#include "readstat_sav_compress.h"
#include "readstat_spss_parse.h"

#if HAVE_ZLIB
#error #include <zlib.h>

#error #include "readstat_zsav_compress.h"
#error #include "readstat_zsav_write.h"
#endif

#define MAX_STRING_SIZE             255
#define MAX_LABEL_SIZE              256
#define MAX_VALUE_LABEL_SIZE        120

typedef struct sav_varnames_s {
    char    shortname[9];
    char    stem[6];
} sav_varnames_t;

static long readstat_label_set_number_short_variables(readstat_label_set_t *r_label_set) {
    long count = 0;
    int j;
    for (j=0; j<r_label_set->variables_count; j++) {
        readstat_variable_t *r_variable = readstat_get_label_set_variable(r_label_set, j);
        if (r_variable->storage_width <= 8) {
            count++;
        }
    }
    return count;
}

static int readstat_label_set_needs_short_value_labels_record(readstat_label_set_t *r_label_set) {
    return readstat_label_set_number_short_variables(r_label_set) > 0;
}

static int readstat_label_set_needs_long_value_labels_record(readstat_label_set_t *r_label_set) {
    return readstat_label_set_number_short_variables(r_label_set) < r_label_set->variables_count;
}

static int32_t sav_encode_format(spss_format_t *spss_format) {
    uint8_t width = spss_format->width > 0xff ? 0xff : spss_format->width;
    return ((spss_format->type << 16) | (width << 8) |
            spss_format->decimal_places);
}

static readstat_error_t sav_encode_base_variable_format(readstat_variable_t *r_variable,
        int32_t *out_code) {
    spss_format_t spss_format;
    readstat_error_t retval = spss_format_for_variable(r_variable, &spss_format);
    if (retval == READSTAT_OK && out_code)
        *out_code = sav_encode_format(&spss_format);

    return retval;
}

static readstat_error_t sav_encode_ghost_variable_format(readstat_variable_t *r_variable,
        size_t user_width, int32_t *out_code) {
    spss_format_t spss_format;
    readstat_error_t retval = spss_format_for_variable(r_variable, &spss_format);
    spss_format.width = user_width;
    if (retval == READSTAT_OK && out_code)
        *out_code = sav_encode_format(&spss_format);

    return retval;
}

static size_t sav_format_variable_name(char *output, size_t output_len,
        sav_varnames_t *varnames) {
    snprintf(output, output_len, "%s", varnames->shortname);
    return strlen(output);
}

static size_t sav_format_ghost_variable_name(char *output, size_t output_len,
        sav_varnames_t *varnames, unsigned int segment) {
    snprintf(output, output_len, "%s", varnames->stem);
    size_t len = strlen(output);
    int letter = segment % 36;
    if (letter < 10) {
        output[len++] = '0' + letter;
    } else {
        output[len++] = 'A' + (letter - 10);
    }
    return len;
}

static int sav_variable_segments(readstat_type_t type, size_t user_width) {
    if (type == READSTAT_TYPE_STRING && user_width > MAX_STRING_SIZE) {
        return (user_width + 251) / 252;
    }
    return 1;
}

static readstat_error_t sav_emit_header(readstat_writer_t *writer) {
    sav_file_header_record_t header = { { 0 } };
    readstat_error_t retval = READSTAT_OK;
    time_t now = writer->timestamp;
    struct tm *time_s = localtime(&now);

    /* There are portability issues with strftime so hack something up */
    char months[][4] = { 
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

    char creation_date[sizeof(header.creation_date)+1] = { 0 };
    char creation_time[sizeof(header.creation_time)+1] = { 0 };

    if (!time_s) {
        retval = READSTAT_ERROR_BAD_TIMESTAMP_VALUE;
        goto cleanup;
    }

    memcpy(header.rec_type, "$FL2", sizeof("$FL2")-1);
    if (writer->compression == READSTAT_COMPRESS_BINARY) {
        header.rec_type[3] = '3';
    }
    memset(header.prod_name, ' ', sizeof(header.prod_name));
    memcpy(header.prod_name,
           "@(#) SPSS DATA FILE - " READSTAT_PRODUCT_URL, 
           sizeof("@(#) SPSS DATA FILE - " READSTAT_PRODUCT_URL)-1);
    header.layout_code = 2;
    header.nominal_case_size = writer->row_len / 8;
    if (writer->compression == READSTAT_COMPRESS_ROWS) {
        header.compression = 1;
    } else if (writer->compression == READSTAT_COMPRESS_BINARY) {
        header.compression = 2;
    }
    if (writer->fweight_variable) {
        int32_t dictionary_index = 1 + writer->fweight_variable->offset / 8;
        header.weight_index = dictionary_index;
    } else {
        header.weight_index = 0;
    }
    header.ncases = writer->row_count;
    header.bias = 100.0;
    
    snprintf(creation_date, sizeof(creation_date),
            "%02d %3.3s %02d", 
            (unsigned int)time_s->tm_mday % 100,
            months[time_s->tm_mon],
            (unsigned int)time_s->tm_year % 100);
    memcpy(header.creation_date, creation_date, sizeof(header.creation_date));

    snprintf(creation_time, sizeof(creation_time),
            "%02d:%02d:%02d",
            (unsigned int)time_s->tm_hour % 100,
            (unsigned int)time_s->tm_min % 100,
            (unsigned int)time_s->tm_sec % 100);
    memcpy(header.creation_time, creation_time, sizeof(header.creation_time));
    
    memset(header.file_label, ' ', sizeof(header.file_label));

    size_t file_label_len = strlen(writer->file_label);
    if (file_label_len > sizeof(header.file_label))
        file_label_len = sizeof(header.file_label);

    if (writer->file_label[0])
        memcpy(header.file_label, writer->file_label, file_label_len);
    
    retval = readstat_write_bytes(writer, &header, sizeof(header));

cleanup:
    return retval;
}

static readstat_error_t sav_emit_variable_label(readstat_writer_t *writer, readstat_variable_t *r_variable) {
    readstat_error_t retval = READSTAT_OK;
    const char *title_data = r_variable->label;
    size_t title_data_len = strlen(title_data);
    if (title_data_len > 0) {
        char padded_label[MAX_LABEL_SIZE];
        uint32_t label_len = title_data_len;
        if (label_len > sizeof(padded_label))
            label_len = sizeof(padded_label);

        retval = readstat_write_bytes(writer, &label_len, sizeof(label_len));
        if (retval != READSTAT_OK)
            goto cleanup;

        strncpy(padded_label, title_data, (label_len + 3) / 4 * 4);

        retval = readstat_write_bytes(writer, padded_label, (label_len + 3) / 4 * 4);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    return retval;
}

static int sav_n_missing_double_values(readstat_variable_t *r_variable) {
    int n_missing_ranges = readstat_variable_get_missing_ranges_count(r_variable);
    int n_missing_values = n_missing_ranges;
    int has_missing_range = 0;
    int j;
    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi = readstat_variable_get_missing_range_hi(r_variable, j);
        if (spss_64bit_value(lo) != spss_64bit_value(hi)) {
            n_missing_values++;
            has_missing_range = 1;
        }
    }
    return has_missing_range ? -n_missing_values : n_missing_values;
}

static int sav_n_missing_string_values(readstat_variable_t *r_variable) {
    int n_missing_ranges = readstat_variable_get_missing_ranges_count(r_variable);
    int n_missing_values = n_missing_ranges;
    int has_missing_range = 0;
    int j;
    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi = readstat_variable_get_missing_range_hi(r_variable, j);
        const char *lo_string = readstat_string_value(lo);
        const char *hi_string = readstat_string_value(hi);
        if (lo_string && hi_string && strcmp(lo_string, hi_string) != 0) {
            n_missing_values++;
            has_missing_range = 1;
        }
    }
    return has_missing_range ? -n_missing_values : n_missing_values;
}

static readstat_error_t sav_n_missing_values(int *out_n_missing_values, readstat_variable_t *r_variable) {
    int n_missing_values = 0;
    if (readstat_variable_get_type_class(r_variable) == READSTAT_TYPE_CLASS_NUMERIC) {
        n_missing_values = sav_n_missing_double_values(r_variable);
    } else if (readstat_variable_get_storage_width(r_variable) <= 8) {
        n_missing_values = sav_n_missing_string_values(r_variable);
    }
    if (abs(n_missing_values) > 3) {
        return READSTAT_ERROR_TOO_MANY_MISSING_VALUE_DEFINITIONS;
    }

    if (out_n_missing_values)
        *out_n_missing_values = n_missing_values;

    return READSTAT_OK;
}

static readstat_error_t sav_emit_variable_missing_string_values(readstat_writer_t *writer, readstat_variable_t *r_variable) {
    readstat_error_t retval = READSTAT_OK;
    int n_missing_values = 0;
    int n_missing_ranges = readstat_variable_get_missing_ranges_count(r_variable);
    /* ranges */
    int j;

    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi = readstat_variable_get_missing_range_hi(r_variable, j);
        const char *lo_string = readstat_string_value(lo);
        const char *hi_string = readstat_string_value(hi);
        if (lo_string && hi_string && strcmp(lo_string, hi_string) != 0) {
            if ((retval = readstat_write_space_padded_string(writer, lo_string, 8)) != READSTAT_OK)
                goto cleanup;

            if ((retval = readstat_write_space_padded_string(writer, hi_string, 8)) != READSTAT_OK)
                goto cleanup;

            n_missing_values += 2;

            break;
        }
    }
    /* values */
    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi = readstat_variable_get_missing_range_hi(r_variable, j);
        const char *lo_string = readstat_string_value(lo);
        const char *hi_string = readstat_string_value(hi);
        if (lo_string && hi_string && strcmp(lo_string, hi_string) == 0) {
            if ((retval = readstat_write_space_padded_string(writer, lo_string, 8)) != READSTAT_OK)
                goto cleanup;

            if (++n_missing_values == 3)
                break;
        }
    }
cleanup:
    return retval;
}

static readstat_error_t sav_emit_variable_missing_double_values(readstat_writer_t *writer, readstat_variable_t *r_variable) {
    readstat_error_t retval = READSTAT_OK;
    int n_missing_values = 0;
    int n_missing_ranges = readstat_variable_get_missing_ranges_count(r_variable);
    /* ranges */
    int j;

    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi = readstat_variable_get_missing_range_hi(r_variable, j);
        if (spss_64bit_value(lo) != spss_64bit_value(hi)) {
            uint64_t lo_val = spss_64bit_value(lo);
            retval = readstat_write_bytes(writer, &lo_val, sizeof(uint64_t));
            if (retval != READSTAT_OK)
                goto cleanup;

            uint64_t hi_val = spss_64bit_value(hi);
            retval = readstat_write_bytes(writer, &hi_val, sizeof(uint64_t));
            if (retval != READSTAT_OK)
                goto cleanup;

            n_missing_values += 2;

            break;
        }
    }
    /* values */
    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi = readstat_variable_get_missing_range_hi(r_variable, j);
        if (spss_64bit_value(lo) == spss_64bit_value(hi)) {
            uint64_t d_val = spss_64bit_value(lo);
            if ((retval = readstat_write_bytes(writer, &d_val, sizeof(uint64_t))) != READSTAT_OK)
                goto cleanup;

            if (++n_missing_values == 3)
                break;
        }
    }
cleanup:
    return retval;
}

static readstat_error_t sav_emit_variable_missing_values(readstat_writer_t *writer, readstat_variable_t *r_variable) {
    if (readstat_variable_get_type_class(r_variable) == READSTAT_TYPE_CLASS_NUMERIC) {
        return sav_emit_variable_missing_double_values(writer, r_variable);
    } else if (readstat_variable_get_storage_width(r_variable) <= 8) {
        return sav_emit_variable_missing_string_values(writer, r_variable);
    }
    return READSTAT_OK;
}

static readstat_error_t sav_emit_blank_variable_records(readstat_writer_t *writer, int extra_fields) {
    readstat_error_t retval = READSTAT_OK;
    int32_t rec_type = SAV_RECORD_TYPE_VARIABLE;
    sav_variable_record_t variable;

    while (extra_fields--) {
        retval = readstat_write_bytes(writer, &rec_type, sizeof(rec_type));
        if (retval != READSTAT_OK)
            goto cleanup;

        memset(&variable, '\0', sizeof(variable));
        memset(variable.name, ' ', sizeof(variable.name));
        variable.type = -1;
        variable.print = variable.write = 0x011d01;
        retval = readstat_write_bytes(writer, &variable, sizeof(variable));
        if (retval != READSTAT_OK)
            goto cleanup;
    }
cleanup:
    return retval;
}

static readstat_error_t sav_emit_base_variable_record(readstat_writer_t *writer, readstat_variable_t *r_variable,
        sav_varnames_t *varnames) {
    readstat_error_t retval = READSTAT_OK;
    int32_t rec_type = SAV_RECORD_TYPE_VARIABLE;
    
    char name_data[9];
    size_t name_data_len = sav_format_variable_name(name_data, sizeof(name_data), varnames);

    retval = readstat_write_bytes(writer, &rec_type, sizeof(rec_type));
    if (retval != READSTAT_OK)
        goto cleanup;

    sav_variable_record_t variable = {0};

    if (r_variable->type == READSTAT_TYPE_STRING) {
        variable.type = r_variable->user_width > MAX_STRING_SIZE ? MAX_STRING_SIZE : r_variable->user_width;
    }
    variable.has_var_label = (r_variable->label[0] != '\0');

    retval = sav_n_missing_values(&variable.n_missing_values, r_variable); 
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_encode_base_variable_format(r_variable, &variable.print);
    if (retval != READSTAT_OK)
        goto cleanup;

    variable.write = variable.print;

    memset(variable.name, ' ', sizeof(variable.name));
    if (name_data_len > 0 && name_data_len <= sizeof(variable.name))
        memcpy(variable.name, name_data, name_data_len);

    retval = readstat_write_bytes(writer, &variable, sizeof(variable));
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_variable_label(writer, r_variable);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_variable_missing_values(writer, r_variable);
    if (retval != READSTAT_OK)
        goto cleanup;

    int extra_fields = r_variable->storage_width / 8 - 1;
    if (extra_fields > 31)
        extra_fields = 31;

    retval = sav_emit_blank_variable_records(writer, extra_fields);
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t sav_emit_ghost_variable_record(readstat_writer_t *writer, 
        readstat_variable_t *r_variable, sav_varnames_t *varnames, int segment, size_t user_width) {
    readstat_error_t retval = READSTAT_OK;
    int32_t rec_type = SAV_RECORD_TYPE_VARIABLE;
    sav_variable_record_t variable = { 0 };
    char name_data[9];
    size_t name_len = sav_format_ghost_variable_name(name_data, sizeof(name_data), varnames, segment);

    retval = readstat_write_bytes(writer, &rec_type, sizeof(rec_type));
    if (retval != READSTAT_OK)
        goto cleanup;

    variable.type = user_width;

    retval = sav_encode_ghost_variable_format(r_variable, user_width, &variable.print);
    if (retval != READSTAT_OK)
        goto cleanup;

    variable.write = variable.print;

    memset(variable.name, ' ', sizeof(variable.name));
    if (name_len > 0 && name_len <= sizeof(variable.name))
        memcpy(variable.name, name_data, name_len);

    retval = readstat_write_bytes(writer, &variable, sizeof(variable));
    if (retval != READSTAT_OK)
        goto cleanup;

    int extra_fields = (user_width + 7) / 8 - 1;
    if (extra_fields > 31)
        extra_fields = 31;

    retval = sav_emit_blank_variable_records(writer, extra_fields);
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t sav_emit_full_variable_record(readstat_writer_t *writer, readstat_variable_t *r_variable,
        sav_varnames_t *varnames) {
    readstat_error_t retval = READSTAT_OK;
    
    retval = sav_emit_base_variable_record(writer, r_variable, varnames);
    if (retval != READSTAT_OK)
        goto cleanup;

    int n_segments = sav_variable_segments(r_variable->type, r_variable->user_width);
    int i;
    for (i=1; i<n_segments; i++) {
        size_t storage_size = MAX_STRING_SIZE;
        if (i == n_segments - 1) {
            storage_size = (r_variable->user_width - (n_segments - 1) * 252);
        }
        retval = sav_emit_ghost_variable_record(writer, r_variable, varnames, i, storage_size);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    return retval;
}

static readstat_error_t sav_emit_variable_records(readstat_writer_t *writer, sav_varnames_t *varnames) {
    readstat_error_t retval = READSTAT_OK;
    int i;
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, i);
        retval = sav_emit_full_variable_record(writer, r_variable, &varnames[i]);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    return retval;
}

static readstat_error_t sav_emit_value_label_records(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;
    int i, j;
    for (i=0; i<writer->label_sets_count; i++) {
        readstat_label_set_t *r_label_set = readstat_get_label_set(writer, i);
        if (!readstat_label_set_needs_short_value_labels_record(r_label_set))
            continue;

        readstat_type_t user_type = r_label_set->type;
        int32_t label_count = r_label_set->value_labels_count;
        int32_t rec_type = 0;

        if (label_count) {
            rec_type = SAV_RECORD_TYPE_VALUE_LABEL;

            retval = readstat_write_bytes(writer, &rec_type, sizeof(rec_type));
            if (retval != READSTAT_OK)
                goto cleanup;

            retval = readstat_write_bytes(writer, &label_count, sizeof(label_count));
            if (retval != READSTAT_OK)
                goto cleanup;
            
            for (j=0; j<label_count; j++) {
                readstat_value_label_t *r_value_label = readstat_get_value_label(r_label_set, j);
                char value[8];
                if (user_type == READSTAT_TYPE_STRING) {
                    size_t key_len = r_value_label->string_key_len;
                    if (key_len > sizeof(value))
                        key_len = sizeof(value);
                    memset(value, ' ', sizeof(value));
                    memcpy(value, r_value_label->string_key, key_len);
                } else if (user_type == READSTAT_TYPE_DOUBLE) {
                    double num_val = r_value_label->double_key;
                    memcpy(value, &num_val, sizeof(double));
                } else if (user_type == READSTAT_TYPE_INT32) {
                    double num_val = r_value_label->int32_key;
                    memcpy(value, &num_val, sizeof(double));
                }
                retval = readstat_write_bytes(writer, value, sizeof(value));
                
                const char *label_data = r_value_label->label;
                
                uint8_t label_len = MAX_VALUE_LABEL_SIZE;
                if (label_len > r_value_label->label_len)
                    label_len = r_value_label->label_len;

                retval = readstat_write_bytes(writer, &label_len, sizeof(label_len));
                if (retval != READSTAT_OK)
                    goto cleanup;

                char label[MAX_VALUE_LABEL_SIZE+8];
                memset(label, ' ', sizeof(label));
                memcpy(label, label_data, label_len);
                retval = readstat_write_bytes(writer, label,
                        (label_len + sizeof(label_len) + 7) / 8 * 8 - sizeof(label_len));
                if (retval != READSTAT_OK)
                    goto cleanup;
            }
            
            rec_type = SAV_RECORD_TYPE_VALUE_LABEL_VARIABLES;
            int32_t var_count = readstat_label_set_number_short_variables(r_label_set);
            
            retval = readstat_write_bytes(writer, &rec_type, sizeof(rec_type));
            if (retval != READSTAT_OK)
                goto cleanup;

            retval = readstat_write_bytes(writer, &var_count, sizeof(var_count));
            if (retval != READSTAT_OK)
                goto cleanup;

            for (j=0; j<r_label_set->variables_count; j++) {
                readstat_variable_t *r_variable = readstat_get_label_set_variable(r_label_set, j);
                if (r_variable->storage_width > 8)
                    continue;

                int32_t dictionary_index = 1 + r_variable->offset / 8;
                retval = readstat_write_bytes(writer, &dictionary_index, sizeof(dictionary_index));
                if (retval != READSTAT_OK)
                    goto cleanup;
            }
        }
    }

cleanup:
    return retval;
}

static readstat_error_t sav_emit_document_record(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;
    int32_t rec_type = SAV_RECORD_TYPE_DOCUMENT;
    int32_t n_lines = writer->notes_count;

    if (n_lines == 0)
        goto cleanup;

    retval = readstat_write_bytes(writer, &rec_type, sizeof(rec_type));
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = readstat_write_bytes(writer, &n_lines, sizeof(n_lines));
    if (retval != READSTAT_OK)
        goto cleanup;

    int i;
    for (i=0; i<writer->notes_count; i++) {
        size_t len = strlen(writer->notes[i]);
        if (len > SPSS_DOC_LINE_SIZE) {
            retval = READSTAT_ERROR_NOTE_IS_TOO_LONG;
            goto cleanup;
        }
        retval = readstat_write_bytes(writer, writer->notes[i], len);
        if (retval != READSTAT_OK)
            goto cleanup;

        retval = readstat_write_spaces(writer, SPSS_DOC_LINE_SIZE - len);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    return retval;
}

static readstat_error_t sav_emit_integer_info_record(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;

    sav_info_record_t info_header = {
        .rec_type = SAV_RECORD_TYPE_HAS_DATA,
        .subtype = SAV_RECORD_SUBTYPE_INTEGER_INFO,
        .size = 4,
        .count = 8
    };

    sav_machine_integer_info_record_t machine_info = {
        .version_major = 20,
        .version_minor = 0,
        .version_revision = 0,
        .machine_code = -1,
        .floating_point_rep = SAV_FLOATING_POINT_REP_IEEE,
        .compression_code = 1,
        .endianness = machine_is_little_endian() ? SAV_ENDIANNESS_LITTLE : SAV_ENDIANNESS_BIG,
        .character_code = 65001 // UTF-8
    };

    retval = readstat_write_bytes(writer, &info_header, sizeof(info_header));
    if (retval != READSTAT_OK)
        goto cleanup;
    
    retval = readstat_write_bytes(writer, &machine_info, sizeof(machine_info));
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t sav_emit_floating_point_info_record(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;

    sav_info_record_t info_header = {
        .rec_type = SAV_RECORD_TYPE_HAS_DATA,
        .subtype = SAV_RECORD_SUBTYPE_FP_INFO,
        .size = 8,
        .count = 3
    };

    retval = readstat_write_bytes(writer, &info_header, sizeof(info_header));
    if (retval != READSTAT_OK)
        goto cleanup;
    
    sav_machine_floating_point_info_record_t fp_info = {0};

    fp_info.sysmis = SAV_MISSING_DOUBLE;
    fp_info.highest = SAV_HIGHEST_DOUBLE;
    fp_info.lowest = SAV_LOWEST_DOUBLE;

    retval = readstat_write_bytes(writer, &fp_info, sizeof(fp_info));
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t sav_emit_variable_display_record(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;
    int i;
    sav_info_record_t info_header = {
        .rec_type = SAV_RECORD_TYPE_HAS_DATA,
        .subtype = SAV_RECORD_SUBTYPE_VAR_DISPLAY,
        .size = sizeof(int32_t)
    };

    int total_segments = 0;
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, i);
        total_segments += sav_variable_segments(r_variable->type, r_variable->user_width);
    }

    info_header.count = 3 * total_segments;

    retval = readstat_write_bytes(writer, &info_header, sizeof(info_header));
    if (retval != READSTAT_OK)
        goto cleanup;
 
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, i);

        readstat_measure_t measure = readstat_variable_get_measure(r_variable);
        int32_t sav_measure = spss_measure_from_readstat_measure(measure);

        int32_t sav_display_width = readstat_variable_get_display_width(r_variable);
        if (sav_display_width <= 0)
            sav_display_width = 8;

        readstat_alignment_t alignment = readstat_variable_get_alignment(r_variable);
        int32_t sav_alignment = spss_alignment_from_readstat_alignment(alignment);

        int n_segments = sav_variable_segments(r_variable->type, r_variable->user_width);

        while (n_segments--) {
            retval = readstat_write_bytes(writer, &sav_measure, sizeof(int32_t));
            if (retval != READSTAT_OK)
                goto cleanup;

            retval = readstat_write_bytes(writer, &sav_display_width, sizeof(int32_t));
            if (retval != READSTAT_OK)
                goto cleanup;

            retval = readstat_write_bytes(writer, &sav_alignment, sizeof(int32_t));
            if (retval != READSTAT_OK)
                goto cleanup;
        }
    }

cleanup:
    return retval;
}

static readstat_error_t sav_emit_long_var_name_record(readstat_writer_t *writer, sav_varnames_t *varnames) {
    readstat_error_t retval = READSTAT_OK;
    int i;
    sav_info_record_t info_header = {
        .rec_type = SAV_RECORD_TYPE_HAS_DATA,
        .subtype = SAV_RECORD_SUBTYPE_LONG_VAR_NAME,
        .size = 1,
        .count = 0
    };
    
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, i);

        char name_data[9];
        size_t name_data_len = sav_format_variable_name(name_data, sizeof(name_data), &varnames[i]);

        const char *title_data = r_variable->name;
        size_t title_data_len = strlen(title_data);

        if (title_data_len > 0 && name_data_len > 0) {
            if (title_data_len > 64)
                title_data_len = 64;
            
            info_header.count += name_data_len;
            info_header.count += sizeof("=")-1;
            info_header.count += title_data_len;
            info_header.count += sizeof("\x09")-1;
        }
    }
    
    if (info_header.count > 0) {
        info_header.count--; /* no trailing 0x09 */
        
        retval = readstat_write_bytes(writer, &info_header, sizeof(info_header));
        if (retval != READSTAT_OK)
            goto cleanup;
        
        int is_first = 1;
        
        for (i=0; i<writer->variables_count; i++) {
            readstat_variable_t *r_variable = readstat_get_variable(writer, i);

            char name_data[9];
            sav_format_variable_name(name_data, sizeof(name_data), &varnames[i]);

            const char *title_data = r_variable->name;
            size_t title_data_len = strlen(title_data);
            
            char kv_separator = '=';
            char tuple_separator = 0x09;
            
            if (title_data_len > 0) {
                if (title_data_len > 64)
                    title_data_len = 64;

                if (!is_first) {
                    retval = readstat_write_bytes(writer, &tuple_separator, sizeof(tuple_separator));
                    if (retval != READSTAT_OK)
                        goto cleanup;
                }
                
                retval = readstat_write_string(writer, name_data);
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_bytes(writer, &kv_separator, sizeof(kv_separator));
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_bytes(writer, title_data, title_data_len);
                if (retval != READSTAT_OK)
                    goto cleanup;
                
                is_first = 0;
            }
        }
    }

cleanup:
    return retval;
}

static readstat_error_t sav_emit_very_long_string_record(readstat_writer_t *writer, sav_varnames_t *varnames) {
    readstat_error_t retval = READSTAT_OK;
    int i;
    char tuple_separator[2] = { 0x00, 0x09 };

    sav_info_record_t info_header = {
        .rec_type = SAV_RECORD_TYPE_HAS_DATA,
        .subtype = SAV_RECORD_SUBTYPE_VERY_LONG_STR,
        .size = 1,
        .count = 0
    };

    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, i);
        if (r_variable->user_width <= MAX_STRING_SIZE)
            continue;

        char name_data[9];
        sav_format_variable_name(name_data, sizeof(name_data), &varnames[i]);

        char kv_data[8+1+5+1];
        snprintf(kv_data, sizeof(kv_data), "%.8s=%d", 
                name_data, (unsigned int)r_variable->user_width % 100000);

        info_header.count += strlen(kv_data) + sizeof(tuple_separator);
    }

    if (info_header.count == 0)
        return READSTAT_OK;

    retval = readstat_write_bytes(writer, &info_header, sizeof(info_header));
    if (retval != READSTAT_OK)
        goto cleanup;

    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, i);
        if (r_variable->user_width <= MAX_STRING_SIZE)
            continue;

        char name_data[9];
        sav_format_variable_name(name_data, sizeof(name_data), &varnames[i]);

        char kv_data[8+1+5+1];
        snprintf(kv_data, sizeof(kv_data), "%.8s=%d", 
                name_data, (unsigned int)r_variable->user_width % 100000);

        retval = readstat_write_string(writer, kv_data);
        if (retval != READSTAT_OK)
            goto cleanup;

        retval = readstat_write_bytes(writer, tuple_separator, sizeof(tuple_separator));
        if (retval != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    return retval;
}

static readstat_error_t sav_emit_long_string_value_labels_record(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;
    int i, j, k;
    char *space_buffer = NULL;

    sav_info_record_t info_header = {
        .rec_type = SAV_RECORD_TYPE_HAS_DATA,
        .subtype = SAV_RECORD_SUBTYPE_LONG_STRING_VALUE_LABELS,
        .size = 1,
        .count = 0
    };

    for (i=0; i<writer->label_sets_count; i++) {
        readstat_label_set_t *r_label_set = readstat_get_label_set(writer, i);
        if (!readstat_label_set_needs_long_value_labels_record(r_label_set))
            continue;

        int32_t label_count = r_label_set->value_labels_count;
        int32_t var_count = r_label_set->variables_count;
        
        for (k=0; k<var_count; k++) {
            readstat_variable_t *r_variable = readstat_get_label_set_variable(r_label_set, k);
            int32_t name_len = strlen(r_variable->name);
            int32_t storage_width = readstat_variable_get_storage_width(r_variable);
            if (storage_width <= 8)
                continue;

            info_header.count += sizeof(int32_t); // name length
            info_header.count += name_len;
            info_header.count += sizeof(int32_t); // variable width
            info_header.count += sizeof(int32_t); // label count

            for (j=0; j<label_count; j++) {
                readstat_value_label_t *r_value_label = readstat_get_value_label(r_label_set, j);
                int32_t label_len = r_value_label->label_len;
                if (label_len > MAX_VALUE_LABEL_SIZE)
                    label_len = MAX_VALUE_LABEL_SIZE;

                info_header.count += sizeof(int32_t); // value length
                info_header.count += storage_width;
                info_header.count += sizeof(int32_t); // label length
                info_header.count += label_len;
            }
        }
    }

    if (info_header.count == 0)
        goto cleanup;

    retval = readstat_write_bytes(writer, &info_header, sizeof(info_header));
    if (retval != READSTAT_OK)
        goto cleanup;

    for (i=0; i<writer->label_sets_count; i++) {
        readstat_label_set_t *r_label_set = readstat_get_label_set(writer, i);
        if (!readstat_label_set_needs_long_value_labels_record(r_label_set))
            continue;

        int32_t label_count = r_label_set->value_labels_count;
        int32_t var_count = r_label_set->variables_count;
        
        for (k=0; k<var_count; k++) {
            readstat_variable_t *r_variable = readstat_get_label_set_variable(r_label_set, k);
            int32_t name_len = strlen(r_variable->name);
            int32_t storage_width = readstat_variable_get_storage_width(r_variable);
            if (storage_width <= 8)
                continue;

            space_buffer = realloc(space_buffer, storage_width);
            memset(space_buffer, ' ', storage_width);

            retval = readstat_write_bytes(writer, &name_len, sizeof(int32_t));
            if (retval != READSTAT_OK)
                goto cleanup;

            retval = readstat_write_bytes(writer, r_variable->name, name_len);
            if (retval != READSTAT_OK)
                goto cleanup;

            retval = readstat_write_bytes(writer, &storage_width, sizeof(int32_t));
            if (retval != READSTAT_OK)
                goto cleanup;

            retval = readstat_write_bytes(writer, &label_count, sizeof(int32_t));
            if (retval != READSTAT_OK)
                goto cleanup;

            for (j=0; j<label_count; j++) {
                readstat_value_label_t *r_value_label = readstat_get_value_label(r_label_set, j);
                int32_t value_len = r_value_label->string_key_len;
                int32_t label_len = r_value_label->label_len;
                if (label_len > MAX_VALUE_LABEL_SIZE)
                    label_len = MAX_VALUE_LABEL_SIZE;

                retval = readstat_write_bytes(writer, &storage_width, sizeof(int32_t));
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_bytes(writer, r_value_label->string_key, value_len);
                if (retval != READSTAT_OK)
                    goto cleanup;

                if (value_len < storage_width) {
                    retval = readstat_write_bytes(writer, space_buffer, storage_width - value_len);
                    if (retval != READSTAT_OK)
                        goto cleanup;
                }

                retval = readstat_write_bytes(writer, &label_len, sizeof(int32_t));
                if (retval != READSTAT_OK)
                    goto cleanup;

                retval = readstat_write_bytes(writer, r_value_label->label, label_len);
                if (retval != READSTAT_OK)
                    goto cleanup;
            }
        }
    }
cleanup:
    if (space_buffer)
        free(space_buffer);

    return retval;
}

static readstat_error_t sav_emit_long_string_missing_values_record(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;
    int j, k;

    sav_info_record_t info_header = {
        .rec_type = SAV_RECORD_TYPE_HAS_DATA,
        .subtype = SAV_RECORD_SUBTYPE_LONG_STRING_MISSING_VALUES,
        .size = 1,
        .count = 0
    };

    int32_t var_count = writer->variables_count;

    for (k=0; k<var_count; k++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, k);
        int32_t name_len = strlen(r_variable->name);
        int32_t storage_width = readstat_variable_get_storage_width(r_variable);
        if (storage_width <= 8)
            continue;
        
        int n_missing_values = 0;

        for (j=0; j<readstat_variable_get_missing_ranges_count(r_variable); j++) {
            readstat_value_t lo = readstat_variable_get_missing_range_lo(r_variable, j);
            readstat_value_t hi = readstat_variable_get_missing_range_hi(r_variable, j);
            const char *lo_string = readstat_string_value(lo);
            const char *hi_string = readstat_string_value(hi);

            if (lo_string && hi_string && strcmp(lo_string, hi_string) == 0) {
                n_missing_values++;
            }
        }

        if (n_missing_values) {
            info_header.count += sizeof(int32_t); // name length
            info_header.count += name_len;
            info_header.count += sizeof(int8_t); // # missing values

            info_header.count += sizeof(int32_t) + 8 * n_missing_values; // value length
        }
    }

    if (info_header.count == 0)
        goto cleanup;

    retval = readstat_write_bytes(writer, &info_header, sizeof(info_header));
    if (retval != READSTAT_OK)
        goto cleanup;

    for (k=0; k<var_count; k++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, k);
        int32_t name_len = strlen(r_variable->name);
        int8_t n_missing_values = 0;
        int32_t storage_width = readstat_variable_get_storage_width(r_variable);
        if (storage_width <= 8)
            continue;

        for (j=0; j<readstat_variable_get_missing_ranges_count(r_variable); j++) {
            readstat_value_t lo = readstat_variable_get_missing_range_lo(r_variable, j);
            readstat_value_t hi = readstat_variable_get_missing_range_hi(r_variable, j);
            const char *lo_string = readstat_string_value(lo);
            const char *hi_string = readstat_string_value(hi);
            if (lo_string && hi_string && strcmp(lo_string, hi_string) == 0) {
                n_missing_values++;
            }
        }

        if (n_missing_values == 0)
            continue;

        retval = readstat_write_bytes(writer, &name_len, sizeof(int32_t));
        if (retval != READSTAT_OK)
            goto cleanup;

        retval = readstat_write_bytes(writer, r_variable->name, name_len);
        if (retval != READSTAT_OK)
            goto cleanup;

        retval = readstat_write_bytes(writer, &n_missing_values, sizeof(int8_t));
        if (retval != READSTAT_OK)
            goto cleanup;

        uint32_t value_len = 8;
        retval = readstat_write_bytes(writer, &value_len, sizeof(int32_t));
        if (retval != READSTAT_OK)
            goto cleanup;

        for (j=0; j<readstat_variable_get_missing_ranges_count(r_variable); j++) {
            readstat_value_t lo = readstat_variable_get_missing_range_lo(r_variable, j);
            readstat_value_t hi = readstat_variable_get_missing_range_hi(r_variable, j);
            const char *lo_string = readstat_string_value(lo);
            const char *hi_string = readstat_string_value(hi);

            if (lo_string && hi_string && strcmp(lo_string, hi_string) == 0) {
                retval = readstat_write_space_padded_string(writer, lo_string, value_len);
                if (retval != READSTAT_OK)
                    goto cleanup;
            }
        }
    }

cleanup:
    return retval;
}

static readstat_error_t sav_emit_number_of_cases_record(readstat_writer_t *writer) {
    readstat_error_t retval = READSTAT_OK;
    sav_info_record_t info_header = {
        .rec_type = SAV_RECORD_TYPE_HAS_DATA,
        .subtype = SAV_RECORD_SUBTYPE_NUMBER_OF_CASES,
        .size = 8,
        .count = 2
    };
    uint64_t one = 1, ncases = writer->row_count;

    retval = readstat_write_bytes(writer, &info_header, sizeof(sav_info_record_t));
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = readstat_write_bytes(writer, &one, sizeof(uint64_t));
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = readstat_write_bytes(writer, &ncases, sizeof(uint64_t));
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t sav_emit_termination_record(readstat_writer_t *writer) {
    sav_dictionary_termination_record_t termination_record = { .rec_type = SAV_RECORD_TYPE_DICT_TERMINATION };

    return readstat_write_bytes(writer, &termination_record, sizeof(termination_record));
}

static readstat_error_t sav_write_int8(void *row, const readstat_variable_t *var, int8_t value) {
    double dval = value;
    memcpy(row, &dval, sizeof(double));
    return READSTAT_OK;
}

static readstat_error_t sav_write_int16(void *row, const readstat_variable_t *var, int16_t value) {
    double dval = value;
    memcpy(row, &dval, sizeof(double));
    return READSTAT_OK;
}

static readstat_error_t sav_write_int32(void *row, const readstat_variable_t *var, int32_t value) {
    double dval = value;
    memcpy(row, &dval, sizeof(double));
    return READSTAT_OK;
}

static readstat_error_t sav_write_float(void *row, const readstat_variable_t *var, float value) {
    double dval = value;
    memcpy(row, &dval, sizeof(double));
    return READSTAT_OK;
}

static readstat_error_t sav_write_double(void *row, const readstat_variable_t *var, double value) {
    double dval = value;
    memcpy(row, &dval, sizeof(double));
    return READSTAT_OK;
}

static readstat_error_t sav_write_string(void *row, const readstat_variable_t *var, const char *value) {
    memset(row, ' ', var->storage_width);
    if (value != NULL && value[0] != '\0') {
        size_t value_len = strlen(value);
        off_t row_offset = 0;
        off_t val_offset = 0;
        unsigned char *row_bytes = (unsigned char *)row;

        if (value_len > var->storage_width)
            return READSTAT_ERROR_STRING_VALUE_IS_TOO_LONG;

        while (value_len - val_offset > 255) {
            memcpy(&row_bytes[row_offset], &value[val_offset], 255);
            row_offset += 256;
            val_offset += 255;
        }
        memcpy(&row_bytes[row_offset], &value[val_offset], value_len - val_offset);
    }
    return READSTAT_OK;
}

static readstat_error_t sav_write_missing_string(void *row, const readstat_variable_t *var) {
    memset(row, ' ', var->storage_width);
    return READSTAT_OK;
}

static readstat_error_t sav_write_missing_number(void *row, const readstat_variable_t *var) {
    uint64_t missing_val = SAV_MISSING_DOUBLE;
    memcpy(row, &missing_val, sizeof(uint64_t));
    return READSTAT_OK;
}

static size_t sav_variable_width(readstat_type_t type, size_t user_width) {
    if (type == READSTAT_TYPE_STRING) {
        if (user_width > MAX_STRING_SIZE) {
            size_t n_segments = sav_variable_segments(type, user_width);
            size_t last_segment_width = ((user_width - (n_segments - 1) * 252) + 7)/8*8;
            return (n_segments-1)*256 + last_segment_width;
        }
        if (user_width == 0) {
            return 8;
        }
        return (user_width + 7) / 8 * 8;
    }
    return 8;
}

static readstat_error_t sav_validate_name_chars(const char *name, int unicode) {
    /* TODO check Unicode class */
    int j;
    for (j=0; name[j]; j++) {
        if (name[j] == ' ')
            return READSTAT_ERROR_NAME_CONTAINS_ILLEGAL_CHARACTER;

        if (((unsigned char)name[j] < 0x80 || !unicode) && name[j] != '@' &&
                name[j] != '.' && name[j] != '_' &&
                name[j] != '$' && name[j] != '#' &&
                !(name[j] >= 'a' && name[j] <= 'z') &&
                !(name[j] >= 'A' && name[j] <= 'Z') &&
                !(name[j] >= '0' && name[j] <= '9')) {
            return READSTAT_ERROR_NAME_CONTAINS_ILLEGAL_CHARACTER;
        }
    }
    char first_char = name[0];
    if (((unsigned char)first_char < 0x80 || !unicode) && first_char != '@' &&
            !(first_char >= 'a' && first_char <= 'z') &&
            !(first_char >= 'A' && first_char <= 'Z')) {
        return READSTAT_ERROR_NAME_BEGINS_WITH_ILLEGAL_CHARACTER;
    }
    return READSTAT_OK;
}

static readstat_error_t sav_validate_name_unreserved(const char *name) {
    if (strcmp(name, "ALL") == 0 || strcmp(name, "AND") == 0 ||
            strcmp(name, "BY") == 0 || strcmp(name, "EQ") == 0 ||
            strcmp(name, "GE") == 0 || strcmp(name, "GT") == 0 ||
            strcmp(name, "GT") == 0 || strcmp(name, "LE") == 0 ||
            strcmp(name, "LT") == 0 || strcmp(name, "NE") == 0 ||
            strcmp(name, "NOT") == 0 || strcmp(name, "OR") == 0 ||
            strcmp(name, "TO") == 0 || strcmp(name, "WITH") == 0)
        return READSTAT_ERROR_NAME_IS_RESERVED_WORD;

    return READSTAT_OK;
}

static readstat_error_t sav_validate_name_length(size_t name_len) {
    if (name_len > 64)
        return READSTAT_ERROR_NAME_IS_TOO_LONG;
    if (name_len == 0)
        return READSTAT_ERROR_NAME_IS_ZERO_LENGTH;
    return READSTAT_OK;
}

static readstat_error_t sav_variable_ok(const readstat_variable_t *variable) {
    readstat_error_t error = READSTAT_OK;

    error = sav_validate_name_length(strlen(variable->name));
    if (error != READSTAT_OK)
        return error;

    error = sav_validate_name_unreserved(variable->name);
    if (error != READSTAT_OK)
        return error;

    return sav_validate_name_chars(variable->name, 1);
}

static sav_varnames_t *sav_varnames_init(readstat_writer_t *writer) {
    sav_varnames_t *varnames = calloc(writer->variables_count, sizeof(sav_varnames_t));

    ck_hash_table_t *table = ck_hash_table_init(writer->variables_count, 8);
    int i, k;
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, i);
        const char *name = r_variable->name;
        char *shortname = varnames[i].shortname;
        char *stem = varnames[i].stem;
        snprintf(shortname, sizeof(varnames[0].shortname), "%.8s", name);
        for (k=0; shortname[k]; k++) { // upcase
            shortname[k] = toupper(shortname[k]);
        }
        if (ck_str_hash_lookup(shortname, table)) {
            snprintf(shortname, sizeof(varnames[0].shortname), "V%d_A", ((unsigned int)i+1)%100000);
        }
        ck_str_hash_insert(shortname, r_variable, table);

        if (r_variable->user_width <= MAX_STRING_SIZE)
            continue;

        snprintf(stem, sizeof(varnames[0].stem), "%.5s", shortname); // conflict resolution?
    }
    ck_hash_table_free(table);
    return varnames;
}

static readstat_error_t sav_begin_data(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    readstat_error_t retval = READSTAT_OK;
    if (!writer->initialized)
        return READSTAT_ERROR_WRITER_NOT_INITIALIZED;

    sav_varnames_t *varnames = sav_varnames_init(writer);

    retval = sav_emit_header(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_variable_records(writer, varnames);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_value_label_records(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_document_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_integer_info_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_floating_point_info_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_variable_display_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_long_var_name_record(writer, varnames);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_very_long_string_record(writer, varnames);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_long_string_value_labels_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_long_string_missing_values_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_number_of_cases_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sav_emit_termination_record(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    free(varnames);
    if (retval == READSTAT_OK) {
        size_t row_bound = sav_compressed_row_bound(writer->row_len);
        if (writer->compression == READSTAT_COMPRESS_ROWS) {
            writer->module_ctx = readstat_malloc(row_bound);
#if HAVE_ZLIB
        } else if (writer->compression == READSTAT_COMPRESS_BINARY) {
            writer->module_ctx = zsav_ctx_init(row_bound, writer->bytes_written);
#endif
        }
    }
    return retval;
}

static readstat_error_t sav_write_compressed_row(void *writer_ctx, void *row, size_t len) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    unsigned char *output = writer->module_ctx;
    size_t output_offset = sav_compress_row(output, row, len, writer);
    return readstat_write_bytes(writer, output, output_offset);
}

static readstat_error_t sav_metadata_ok(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;

    if (writer->version == 2 && writer->compression == READSTAT_COMPRESS_BINARY)
        return READSTAT_ERROR_UNSUPPORTED_COMPRESSION;

    if (writer->version != 2 && writer->version != 3)
        return READSTAT_ERROR_UNSUPPORTED_FILE_FORMAT_VERSION;

    return READSTAT_OK;
}

readstat_error_t readstat_begin_writing_sav(readstat_writer_t *writer, void *user_ctx, long row_count) {

    writer->callbacks.metadata_ok = &sav_metadata_ok;
    writer->callbacks.variable_width = &sav_variable_width;
    writer->callbacks.variable_ok = &sav_variable_ok;
    writer->callbacks.write_int8 = &sav_write_int8;
    writer->callbacks.write_int16 = &sav_write_int16;
    writer->callbacks.write_int32 = &sav_write_int32;
    writer->callbacks.write_float = &sav_write_float;
    writer->callbacks.write_double = &sav_write_double;
    writer->callbacks.write_string = &sav_write_string;
    writer->callbacks.write_missing_string = &sav_write_missing_string;
    writer->callbacks.write_missing_number = &sav_write_missing_number;
    writer->callbacks.begin_data = &sav_begin_data;

    if (writer->version == 3) {
        writer->compression = READSTAT_COMPRESS_BINARY;
    } else if (writer->version == 0) {
        writer->version = (writer->compression == READSTAT_COMPRESS_BINARY) ? 3 : 2;
    }

    if (writer->compression == READSTAT_COMPRESS_ROWS) {
        writer->callbacks.write_row = &sav_write_compressed_row;
        writer->callbacks.module_ctx_free = &free;
#if HAVE_ZLIB
    } else if (writer->compression == READSTAT_COMPRESS_BINARY) {
        writer->callbacks.write_row = &zsav_write_compressed_row;
        writer->callbacks.end_data = &zsav_end_data;
        writer->callbacks.module_ctx_free = (readstat_module_ctx_free_callback)&zsav_ctx_free;
#endif
    } else if (writer->compression == READSTAT_COMPRESS_NONE) {
        /* void */
    } else {
        return READSTAT_ERROR_UNSUPPORTED_COMPRESSION;
    }

    return readstat_begin_writing_file(writer, user_ctx, row_count);
}
