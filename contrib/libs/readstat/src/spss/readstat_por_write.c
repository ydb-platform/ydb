
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iconv.h>
#include <inttypes.h>

#include "../readstat.h"
#include "../CKHashTable.h"
#include "../readstat_writer.h"

#include "readstat_spss.h"
#include "readstat_por.h"

#define POR_BASE30_PRECISION  50

typedef struct por_write_ctx_s {
    unsigned char   *unicode2byte;
    size_t           unicode2byte_len;
} por_write_ctx_t;

static inline char por_encode_base30_digit(uint64_t digit) {
    if (digit < 10)
        return '0' + digit;
    return 'A' + (digit - 10);
}

static int por_write_base30_integer(char *string, size_t string_len, uint64_t integer) {
    int start = 0;
    int end = 0;
    int offset = 0;
    while (integer) {
        string[offset++] = por_encode_base30_digit(integer % 30);
        integer /= 30;
    }
    end = offset;
    offset--;
    while (offset > start) {
        char tmp = string[start];
        string[start] = string[offset];
        string[offset] = tmp;
        offset--; start++;
    }
    return end;
}

static readstat_error_t por_finish(readstat_writer_t *writer) {
    return readstat_write_line_padding(writer, 'Z', 80, "\r\n");
}

static readstat_error_t por_write_bytes(readstat_writer_t *writer, const void *bytes, size_t len) {
    return readstat_write_bytes_as_lines(writer, bytes, len, 80, "\r\n");
}

static readstat_error_t por_write_string_n(readstat_writer_t *writer, por_write_ctx_t *ctx, 
        const char *string, size_t input_len) {
    char error_buf[1024];
    readstat_error_t retval = READSTAT_OK;
    char *por_string = malloc(input_len);
    ssize_t output_len = por_utf8_decode(string, input_len, por_string, input_len,
            ctx->unicode2byte, ctx->unicode2byte_len);
    if (output_len == -1) {
        if (writer->error_handler) {
            snprintf(error_buf, sizeof(error_buf), "Error converting string (length=%" PRId64 "): %.*s", 
                    (int64_t)input_len, (int)input_len, string);
            writer->error_handler(error_buf, writer->user_ctx);
        }
        retval = READSTAT_ERROR_CONVERT;
        goto cleanup;
    }
    retval = por_write_bytes(writer, por_string, output_len);
cleanup:
    if (por_string)
        free(por_string);
    return retval;
}

static readstat_error_t por_write_tag(readstat_writer_t *writer, por_write_ctx_t *ctx, char tag) {
    char string[2];
    string[0] = tag;
    string[1] = '\0';
    return por_write_string_n(writer, ctx, string, 1);
}

static ssize_t por_write_double_to_buffer(char *string, size_t buffer_len, double value, long precision) {
    int offset = 0;
    if (isnan(value)) {
        string[offset++] = '*';
        string[offset++] = '.';
    } else if (isinf(value)) {
        if (value < 0.0) {
            string[offset++] = '-';
        }
        string[offset++] = '1';
        string[offset++] = '+';
        string[offset++] = 'T';
        string[offset++] = 'T';
        string[offset++] = '/';
    } else {
        long integers_printed = 0;
        double integer_part;
        double fraction = modf(fabs(value), &integer_part);
        int64_t integer = integer_part;
        int64_t exponent = 0;
        if (value < 0.0) {
            string[offset++] = '-';
        }
        if (integer == 0) {
            string[offset++] = '0';
        } else {
            while (fraction == 0 && integer != 0 && (integer % 30) == 0) {
                integer /= 30;
                exponent++;
            }
            integers_printed = por_write_base30_integer(&string[offset], buffer_len - offset, integer);
            offset += integers_printed;
        }
        /* should use exponents for efficiency, but this works */
        if (fraction) {
            string[offset++] = '.';
        }
        while (fraction && integers_printed < precision) {
            fraction = modf(fraction * 30, &integer_part);
            integer = integer_part;
            if (integer < 0) {
                return -1;
            } else {
                string[offset++] = por_encode_base30_digit(integer);
            }
            integers_printed++;
        }
        if (exponent) {
            string[offset++] = '+';
            offset += por_write_base30_integer(&string[offset], buffer_len - offset, exponent);
        }
        string[offset++] = '/';
    }
    string[offset] = '\0';
    return offset;
}

static readstat_error_t por_write_double(readstat_writer_t *writer, por_write_ctx_t *ctx, double value) {
    char error_buf[1024];
    char string[256];
    ssize_t bytes_written = por_write_double_to_buffer(string, sizeof(string), value, POR_BASE30_PRECISION);
    if (bytes_written == -1) {
        if (writer->error_handler) {
            snprintf(error_buf, sizeof(error_buf), "Unable to encode number: %lf", value);
            writer->error_handler(error_buf, writer->user_ctx);
        }
        return READSTAT_ERROR_WRITE;
    }

    return por_write_string_n(writer, ctx, string, bytes_written);
}

static readstat_error_t por_write_string_field_n(readstat_writer_t *writer, por_write_ctx_t *ctx, 
        const char *string, size_t len) {
    readstat_error_t error = por_write_double(writer, ctx, len);
    if (error != READSTAT_OK)
        return error;

    return por_write_string_n(writer, ctx, string, len);
}

static readstat_error_t por_write_string_field(readstat_writer_t *writer, por_write_ctx_t *ctx, const char *string) {
    return por_write_string_field_n(writer, ctx, string, strlen(string));
}

static por_write_ctx_t *por_write_ctx_init(void) {
    por_write_ctx_t *ctx = calloc(1, sizeof(por_write_ctx_t));
    uint16_t max_unicode = 0;
    int i;
    for (i=0; i<sizeof(por_unicode_lookup)/sizeof(por_unicode_lookup[0]); i++) {
        if (por_unicode_lookup[i] > max_unicode)
            max_unicode = por_unicode_lookup[i];
    }
    ctx->unicode2byte = malloc(max_unicode+1);
    ctx->unicode2byte_len = max_unicode+1;

    for (i=0; i<sizeof(por_unicode_lookup)/sizeof(por_unicode_lookup[0]); i++) {
        if (por_unicode_lookup[i]) {
            ctx->unicode2byte[por_unicode_lookup[i]] = por_ascii_lookup[i];
        }
        if (por_ascii_lookup[i]) {
            ctx->unicode2byte[por_ascii_lookup[i]] = por_ascii_lookup[i];
        }
    }
    return ctx;
}

static void por_write_ctx_free(por_write_ctx_t *ctx) {
    if (ctx->unicode2byte)
        free(ctx->unicode2byte);
    free(ctx);
}

static readstat_error_t por_emit_header(readstat_writer_t *writer, por_write_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;

    size_t file_label_len = strlen(writer->file_label);
    char vanity[5][40];
    memset(vanity, '0', sizeof(vanity));

    memcpy(vanity[1], "ASCII SPSS PORT FILE", 20);
    strncpy(vanity[1] + 20, writer->file_label, 20);
    if (file_label_len < 20)
        memset(vanity[1] + 20 + file_label_len, ' ', 20 - file_label_len);

    por_write_bytes(writer, vanity, sizeof(vanity));

    char lookup[256];
    int i;
    memset(lookup, '0', sizeof(lookup));
    for (i=0; i<sizeof(lookup); i++) {
        if (por_ascii_lookup[i]) {
            lookup[i] = por_ascii_lookup[i];
        }
    }
    if ((retval = por_write_bytes(writer, lookup, sizeof(lookup))) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_write_string_n(writer, ctx, "SPSSPORT", sizeof("SPSSPORT")-1)) != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t por_emit_version_and_timestamp(readstat_writer_t *writer,
        por_write_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    struct tm *timestamp = localtime(&writer->timestamp);

    if (!timestamp) {
        retval = READSTAT_ERROR_BAD_TIMESTAMP_VALUE;
        goto cleanup;
    }

    if ((retval = por_write_tag(writer, ctx, 'A')) != READSTAT_OK)
        goto cleanup;

    char date[9];
    snprintf(date, sizeof(date), "%04d%02d%02d",
            (unsigned int)(timestamp->tm_year + 1900) % 10000,
            (unsigned int)(timestamp->tm_mon + 1) % 100,
            (unsigned int)(timestamp->tm_mday) % 100);
    if ((retval = por_write_string_field(writer, ctx, date)) != READSTAT_OK)
        goto cleanup;

    char time[7];
    snprintf(time, sizeof(time), "%02d%02d%02d",
            (unsigned int)timestamp->tm_hour % 100,
            (unsigned int)timestamp->tm_min % 100,
            (unsigned int)timestamp->tm_sec % 100);
    if ((retval = por_write_string_field(writer, ctx, time)) != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t por_emit_identification_records(readstat_writer_t *writer,
        por_write_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;

    if ((retval = por_write_tag(writer, ctx, '1')) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_write_string_field(writer, ctx, READSTAT_PRODUCT_NAME)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_write_tag(writer, ctx, '3')) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_write_string_field(writer, ctx, READSTAT_PRODUCT_URL)) != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t por_emit_variable_count_record(readstat_writer_t *writer,
        por_write_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;

    if ((retval = por_write_tag(writer, ctx, '4')) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_write_double(writer, ctx, writer->variables_count)) != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t por_emit_precision_record(readstat_writer_t *writer,
        por_write_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;

    if ((retval = por_write_tag(writer, ctx, '5')) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_write_double(writer, ctx, POR_BASE30_PRECISION)) != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t por_emit_case_weight_variable_record(readstat_writer_t *writer,
        por_write_ctx_t *ctx) {
    if (!writer->fweight_variable)
        return READSTAT_OK;

    readstat_error_t retval = READSTAT_OK;

    if ((retval = por_write_tag(writer, ctx, '6')) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_write_string_field(writer, ctx, 
                    readstat_variable_get_name(writer->fweight_variable))) != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t por_emit_format(readstat_writer_t *writer, por_write_ctx_t *ctx,
        spss_format_t *format) {
    readstat_error_t error = READSTAT_OK;

    if ((error = por_write_double(writer, ctx, format->type)) != READSTAT_OK)
        goto cleanup;

    if ((error = por_write_double(writer, ctx, format->width)) != READSTAT_OK)
        goto cleanup;

    if ((error = por_write_double(writer, ctx, format->decimal_places)) != READSTAT_OK)
        goto cleanup;

cleanup:
    return error;
}

static readstat_error_t validate_variable_name(const char *name) {
    size_t len = strlen(name);
    if (len < 1 || len > 8)
        return READSTAT_ERROR_NAME_IS_TOO_LONG;
    int i;
    for (i=0; name[i]; i++) {
        if (name[i] >= 'A' && name[i] <= 'Z')
            continue;
        if (name[i] >= '0' && name[i] <= '9')
            continue;
        if (name[i] == '@' || name[i] == '#' || name[i] == '$')
            continue;
        if (name[i] == '_' || name[i] == '.')
            continue;

        return READSTAT_ERROR_NAME_CONTAINS_ILLEGAL_CHARACTER;
    }

    if (!(name[0] >= 'A' && name[0] <= 'Z') && name[0] != '@')
        return READSTAT_ERROR_NAME_BEGINS_WITH_ILLEGAL_CHARACTER;

    return READSTAT_OK;
}

static readstat_error_t por_emit_variable_label_record(readstat_writer_t *writer,
        por_write_ctx_t *ctx, readstat_variable_t *r_variable) {
    const char *label = readstat_variable_get_label(r_variable);
    readstat_error_t retval = READSTAT_OK;
    if (!label)
        return READSTAT_OK;

    if ((retval = por_write_tag(writer, ctx, 'C')) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_write_string_field(writer, ctx, label)) != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t por_emit_missing_string_values_records(readstat_writer_t *writer,
        por_write_ctx_t *ctx, readstat_variable_t *r_variable) {
    readstat_error_t retval = READSTAT_OK;
    int n_missing_values = 0;
    int n_missing_ranges = readstat_variable_get_missing_ranges_count(r_variable);
    /* ranges */
    int j;

    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo_value = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi_value = readstat_variable_get_missing_range_hi(r_variable, j);
        const char *lo = readstat_string_value(lo_value);
        const char *hi = readstat_string_value(hi_value);
        if (lo && hi && strcmp(lo, hi) != 0) {
            if ((retval = por_write_tag(writer, ctx, 'B')) != READSTAT_OK)
                goto cleanup;

            if ((retval = por_write_string_field(writer, ctx, lo)) != READSTAT_OK)
                goto cleanup;

            if ((retval = por_write_string_field(writer, ctx, hi)) != READSTAT_OK)
                goto cleanup;

            n_missing_values += 2;
        }
    }
    /* values */
    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo_value = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi_value = readstat_variable_get_missing_range_hi(r_variable, j);
        const char *lo = readstat_string_value(lo_value);
        const char *hi = readstat_string_value(hi_value);
        if (lo && hi && strcmp(lo, hi) == 0) {
            if ((retval = por_write_tag(writer, ctx, '8')) != READSTAT_OK)
                goto cleanup;

            if ((retval = por_write_string_field(writer, ctx, lo)) != READSTAT_OK)
                goto cleanup;

            n_missing_values++;
        }
    }
    if (n_missing_values > 3)
        retval = READSTAT_ERROR_TOO_MANY_MISSING_VALUE_DEFINITIONS;

cleanup:
    return retval;
}

static readstat_error_t por_emit_missing_double_values_records(readstat_writer_t *writer,
        por_write_ctx_t *ctx, readstat_variable_t *r_variable) {
    readstat_error_t retval = READSTAT_OK;
    int n_missing_values = 0;
    int n_missing_ranges = readstat_variable_get_missing_ranges_count(r_variable);
    /* ranges */
    int j;

    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo_value = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi_value = readstat_variable_get_missing_range_hi(r_variable, j);
        double lo = readstat_double_value(lo_value);
        double hi = readstat_double_value(hi_value);
        if (isinf(lo)) {
            if ((retval = por_write_tag(writer, ctx, '9')) != READSTAT_OK)
                goto cleanup;

            if ((retval = por_write_double(writer, ctx, hi)) != READSTAT_OK)
                goto cleanup;

            n_missing_values += 2;
        } else if (isinf(hi)) {
            if ((retval = por_write_tag(writer, ctx, 'A')) != READSTAT_OK)
                goto cleanup;

            if ((retval = por_write_double(writer, ctx, lo)) != READSTAT_OK)
                goto cleanup;

            n_missing_values += 2;
        } else if (lo != hi) {
            if ((retval = por_write_tag(writer, ctx, 'B')) != READSTAT_OK)
                goto cleanup;

            if ((retval = por_write_double(writer, ctx, lo)) != READSTAT_OK)
                goto cleanup;

            if ((retval = por_write_double(writer, ctx, hi)) != READSTAT_OK)
                goto cleanup;

            n_missing_values += 2;
        }
    }
    /* values */
    for (j=0; j<n_missing_ranges; j++) {
        readstat_value_t lo_value = readstat_variable_get_missing_range_lo(r_variable, j);
        readstat_value_t hi_value = readstat_variable_get_missing_range_hi(r_variable, j);
        double lo = readstat_double_value(lo_value);
        double hi = readstat_double_value(hi_value);
        if (lo == hi && !isinf(lo) && !isinf(hi)) {
            if ((retval = por_write_tag(writer, ctx, '8')) != READSTAT_OK)
                goto cleanup;

            if ((retval = por_write_double(writer, ctx, lo)) != READSTAT_OK)
                goto cleanup;

            n_missing_values++;
        }
    }
    if (n_missing_values > 3)
        retval = READSTAT_ERROR_TOO_MANY_MISSING_VALUE_DEFINITIONS;

cleanup:
    return retval;
}

static readstat_error_t por_emit_missing_values_records(readstat_writer_t *writer,
        por_write_ctx_t *ctx, readstat_variable_t *r_variable) {
    if (r_variable->type == READSTAT_TYPE_DOUBLE) {
        return por_emit_missing_double_values_records(writer, ctx, r_variable);
    }
    return por_emit_missing_string_values_records(writer, ctx, r_variable);
}

static readstat_error_t por_emit_variable_records(readstat_writer_t *writer,
        por_write_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    int i;
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *r_variable = readstat_get_variable(writer, i);
        const char *variable_name = readstat_variable_get_name(r_variable);
        spss_format_t print_format;

        if ((retval = por_write_tag(writer, ctx, '7')) != READSTAT_OK)
            break;

        retval = por_write_double(writer, ctx, 
                (r_variable->type == READSTAT_TYPE_STRING) ?
                r_variable->user_width : 0);
        if (retval != READSTAT_OK)
            break;

        if ((retval = por_write_string_field(writer, ctx, variable_name)) != READSTAT_OK)
            break;

        if ((retval = spss_format_for_variable(r_variable, &print_format)) != READSTAT_OK)
            break;

        if ((retval = por_emit_format(writer, ctx, &print_format)) != READSTAT_OK)
            break;

        if ((retval = por_emit_format(writer, ctx, &print_format)) != READSTAT_OK)
            break;

        if ((retval = por_emit_missing_values_records(writer, ctx, r_variable)) != READSTAT_OK)
            break;

        if ((retval = por_emit_variable_label_record(writer, ctx, r_variable)) != READSTAT_OK)
            break;
    }
    return retval;
}

static readstat_error_t por_emit_value_label_records(readstat_writer_t *writer,
        por_write_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    int i, j;

    for (i=0; i<writer->label_sets_count; i++) {
        readstat_label_set_t *r_label_set = readstat_get_label_set(writer, i);
        readstat_type_t user_type = r_label_set->type;
        if (r_label_set->value_labels_count == 0 || r_label_set->variables_count == 0)
            continue;

        if ((retval = por_write_tag(writer, ctx, 'D')) != READSTAT_OK)
            goto cleanup;

        if ((retval = por_write_double(writer, ctx, r_label_set->variables_count)) != READSTAT_OK)
            goto cleanup;

        for (j=0; j<r_label_set->variables_count; j++) {
            readstat_variable_t *r_variable = readstat_get_label_set_variable(r_label_set, j);

            if ((retval = por_write_string_field(writer, ctx, 
                            readstat_variable_get_name(r_variable))) != READSTAT_OK)
                goto cleanup;
        }

        if ((retval = por_write_double(writer, ctx, r_label_set->value_labels_count)) != READSTAT_OK)
            goto cleanup;

        for (j=0; j<r_label_set->value_labels_count; j++) {
            readstat_value_label_t *r_value_label = readstat_get_value_label(r_label_set, j);

            if (user_type == READSTAT_TYPE_STRING) {
                retval = por_write_string_field_n(writer, ctx, 
                        r_value_label->string_key, r_value_label->string_key_len);
            } else if (user_type == READSTAT_TYPE_DOUBLE) {
                retval = por_write_double(writer, ctx, r_value_label->double_key);
            } else if (user_type == READSTAT_TYPE_INT32) {
                retval = por_write_double(writer, ctx, r_value_label->int32_key);
            }

            if (retval != READSTAT_OK)
                goto cleanup;

            if ((retval = por_write_string_field_n(writer, ctx, 
                            r_value_label->label, r_value_label->label_len)) != READSTAT_OK)
                goto cleanup;
        }
    }

cleanup:
    return retval;
}

static readstat_error_t por_emit_document_record(readstat_writer_t *writer, por_write_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;

    if ((retval = por_write_tag(writer, ctx, 'E')) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_write_double(writer, ctx, writer->notes_count)) != READSTAT_OK)
        goto cleanup;

    int i;
    for (i=0; i<writer->notes_count; i++) {
        size_t len = strlen(writer->notes[i]);
        if (len > SPSS_DOC_LINE_SIZE) {
            retval = READSTAT_ERROR_NOTE_IS_TOO_LONG;
            goto cleanup;
        }

        if ((retval = por_write_string_field_n(writer, ctx, writer->notes[i], len)) != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    return retval;
}

static readstat_error_t por_emit_data_tag(readstat_writer_t *writer, por_write_ctx_t *ctx) {
    return por_write_tag(writer, ctx, 'F');
}

static readstat_error_t por_begin_data(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    por_write_ctx_t *ctx = por_write_ctx_init();
    readstat_error_t retval = READSTAT_OK;

    if ((retval = por_emit_header(writer, ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_emit_version_and_timestamp(writer, ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_emit_identification_records(writer, ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_emit_variable_count_record(writer, ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_emit_precision_record(writer, ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_emit_case_weight_variable_record(writer, ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_emit_variable_records(writer, ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_emit_value_label_records(writer, ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_emit_document_record(writer, ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = por_emit_data_tag(writer, ctx)) != READSTAT_OK)
        goto cleanup;

cleanup:
    if (retval != READSTAT_OK) {
        por_write_ctx_free(ctx);
    } else {
        writer->module_ctx = ctx;
    }

    return retval;
}

static readstat_error_t por_end_data(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    readstat_error_t error = READSTAT_OK;

    if ((error = por_write_tag(writer, writer->module_ctx, 'Z')) != READSTAT_OK)
        goto cleanup;

    if ((error = por_finish(writer)) != READSTAT_OK)
        goto cleanup;

cleanup:
    por_write_ctx_free(writer->module_ctx);
    return error;
}

static size_t por_variable_width(readstat_type_t type, size_t user_width) {
    if (type == READSTAT_TYPE_STRING) {
        return POR_BASE30_PRECISION + 4 + user_width;
    }
    return POR_BASE30_PRECISION + 4; // minus sign + period + plus/minus + slash
}

static readstat_error_t por_variable_ok(const readstat_variable_t *variable) {
    return validate_variable_name(readstat_variable_get_name(variable));
}

static readstat_error_t por_write_double_value(void *row, const readstat_variable_t *var, double value) {
    if (por_write_double_to_buffer(row, POR_BASE30_PRECISION + 4, value, POR_BASE30_PRECISION) == -1) {
        return READSTAT_ERROR_WRITE;
    }

    return READSTAT_OK;
}

static readstat_error_t por_write_int8_value(void *row, const readstat_variable_t *var, int8_t value) {
    return por_write_double_value(row, var, value);
}

static readstat_error_t por_write_int16_value(void *row, const readstat_variable_t *var, int16_t value) {
    return por_write_double_value(row, var, value);
}

static readstat_error_t por_write_int32_value(void *row, const readstat_variable_t *var, int32_t value) {
    return por_write_double_value(row, var, value);
}

static readstat_error_t por_write_float_value(void *row, const readstat_variable_t *var, float value) {
    return por_write_double_value(row, var, value);
}

static readstat_error_t por_write_missing_number(void *row, const readstat_variable_t *var) {
    return por_write_double_value(row, var, NAN);
}

static readstat_error_t por_write_missing_string(void *row, const readstat_variable_t *var) {
    return por_write_double_value(row, var, 0);
}

static readstat_error_t por_write_string_value(void *row, const readstat_variable_t *var, const char *string) {
    size_t len = strlen(string);
    if (len == 0) {
        string = " ";
        len = 1;
    }
    size_t storage_width = readstat_variable_get_storage_width(var);
    if (len > storage_width) {
        len = storage_width;
    }
    ssize_t bytes_written = por_write_double_to_buffer(row, POR_BASE30_PRECISION + 4, len, POR_BASE30_PRECISION);
    if (bytes_written == -1) {
        return READSTAT_ERROR_WRITE;
    }

    strncpy(((char *)row) + bytes_written, string, len);
    return READSTAT_OK;
}

static readstat_error_t por_write_row(void *writer_ctx, void *row, size_t row_len) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    char *row_chars = (char *)row;
    int offset = 0, output = 0;
    for (offset=0; offset<row_len; offset++) {
        if (row_chars[offset]) {
            if (offset != output) {
                row_chars[output] = row_chars[offset];
            }
            output++;
        }
    }
    return por_write_string_n(writer, writer->module_ctx, row_chars, output);
}

static readstat_error_t por_metadata_ok(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;

    if (writer->compression != READSTAT_COMPRESS_NONE)
        return READSTAT_ERROR_UNSUPPORTED_COMPRESSION;

    return READSTAT_OK;
}

readstat_error_t readstat_begin_writing_por(readstat_writer_t *writer, void *user_ctx, long row_count) {

    writer->callbacks.metadata_ok = &por_metadata_ok;
    writer->callbacks.variable_width = &por_variable_width;
    writer->callbacks.variable_ok = &por_variable_ok;
    writer->callbacks.write_int8 = &por_write_int8_value;
    writer->callbacks.write_int16 = &por_write_int16_value;
    writer->callbacks.write_int32 = &por_write_int32_value;
    writer->callbacks.write_float = &por_write_float_value;
    writer->callbacks.write_double = &por_write_double_value;
    writer->callbacks.write_string = &por_write_string_value;
    writer->callbacks.write_missing_string = &por_write_missing_string;
    writer->callbacks.write_missing_number = &por_write_missing_number;
    writer->callbacks.begin_data = &por_begin_data;
    writer->callbacks.write_row = &por_write_row;
    writer->callbacks.end_data = &por_end_data;

    return readstat_begin_writing_file(writer, user_ctx, row_count);

}
