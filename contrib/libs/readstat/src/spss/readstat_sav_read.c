
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <stdint.h>
#include <math.h>
#include <float.h>
#include <time.h>
#include <limits.h>

#include "../readstat.h"
#include "../readstat_bits.h"
#include "../readstat_iconv.h"
#include "../readstat_convert.h"
#include "../readstat_malloc.h"

#include "readstat_sav.h"
#include "readstat_sav_compress.h"
#include "readstat_sav_parse.h"
#include "readstat_sav_parse_timestamp.h"

#if HAVE_ZLIB
#error #include "readstat_zsav_read.h"
#endif

#define DATA_BUFFER_SIZE    65536
#define VERY_LONG_STRING_MAX_LENGTH INT_MAX

/* Others defined in table below */

/* See http://msdn.microsoft.com/en-us/library/dd317756(VS.85).aspx */
static readstat_charset_entry_t _charset_table[] = { 
    { .code = 1,     .name = "EBCDIC-US" },
    { .code = 2,     .name = "WINDOWS-1252" }, /* supposed to be ASCII, but some files are miscoded */
    { .code = 3,     .name = "WINDOWS-1252" },
    { .code = 4,     .name = "DEC-KANJI" },
    { .code = 437,   .name = "CP437" },
    { .code = 708,   .name = "ASMO-708" },
    { .code = 737,   .name = "CP737" },
    { .code = 775,   .name = "CP775" },
    { .code = 850,   .name = "CP850" },
    { .code = 852,   .name = "CP852" },
    { .code = 855,   .name = "CP855" },
    { .code = 857,   .name = "CP857" },
    { .code = 858,   .name = "CP858" },
    { .code = 860,   .name = "CP860" },
    { .code = 861,   .name = "CP861" },
    { .code = 862,   .name = "CP862" },
    { .code = 863,   .name = "CP863" },
    { .code = 864,   .name = "CP864" },
    { .code = 865,   .name = "CP865" },
    { .code = 866,   .name = "CP866" },
    { .code = 869,   .name = "CP869" },
    { .code = 874,   .name = "CP874" },
    { .code = 932,   .name = "CP932" },
    { .code = 936,   .name = "CP936" },
    { .code = 949,   .name = "CP949" },
    { .code = 950,   .name = "BIG-5" },
    { .code = 1200,  .name = "UTF-16LE" },
    { .code = 1201,  .name = "UTF-16BE" },
    { .code = 1250,  .name = "WINDOWS-1250" },
    { .code = 1251,  .name = "WINDOWS-1251" },
    { .code = 1252,  .name = "WINDOWS-1252" },
    { .code = 1253,  .name = "WINDOWS-1253" },
    { .code = 1254,  .name = "WINDOWS-1254" },
    { .code = 1255,  .name = "WINDOWS-1255" },
    { .code = 1256,  .name = "WINDOWS-1256" },
    { .code = 1257,  .name = "WINDOWS-1257" },
    { .code = 1258,  .name = "WINDOWS-1258" },
    { .code = 1361,  .name = "CP1361" },
    { .code = 10000, .name = "MACROMAN" },
    { .code = 10004, .name = "MACARABIC" },
    { .code = 10005, .name = "MACHEBREW" },
    { .code = 10006, .name = "MACGREEK" },
    { .code = 10007, .name = "MACCYRILLIC" },
    { .code = 10010, .name = "MACROMANIA" },
    { .code = 10017, .name = "MACUKRAINE" },
    { .code = 10021, .name = "MACTHAI" },
    { .code = 10029, .name = "MACCENTRALEUROPE" },
    { .code = 10079, .name = "MACICELAND" },
    { .code = 10081, .name = "MACTURKISH" },
    { .code = 10082, .name = "MACCROATIAN" },
    { .code = 12000, .name = "UTF-32LE" },
    { .code = 12001, .name = "UTF-32BE" },
    { .code = 20127, .name = "US-ASCII" },
    { .code = 20866, .name = "KOI8-R" },
    { .code = 20932, .name = "EUC-JP" },
    { .code = 21866, .name = "KOI8-U" },
    { .code = 28591, .name = "ISO-8859-1" },
    { .code = 28592, .name = "ISO-8859-2" },
    { .code = 28593, .name = "ISO-8859-3" },
    { .code = 28594, .name = "ISO-8859-4" },
    { .code = 28595, .name = "ISO-8859-5" },
    { .code = 28596, .name = "ISO-8859-6" },
    { .code = 28597, .name = "ISO-8859-7" },
    { .code = 28598, .name = "ISO-8859-8" },
    { .code = 28599, .name = "ISO-8859-9" },
    { .code = 28603, .name = "ISO-8859-13" },
    { .code = 28605, .name = "ISO-8859-15" },
    { .code = 50220, .name = "ISO-2022-JP" },
    { .code = 50221, .name = "ISO-2022-JP" }, // same as above?
    { .code = 50222, .name = "ISO-2022-JP" }, // same as above?
    { .code = 50225, .name = "ISO-2022-KR" },
    { .code = 50229, .name = "ISO-2022-CN" },
    { .code = 51932, .name = "EUC-JP" },
    { .code = 51936, .name = "GBK" },
    { .code = 51949, .name = "EUC-KR" },
    { .code = 52936, .name = "HZ-GB-2312" },
    { .code = 54936, .name = "GB18030" },
    { .code = 65000, .name = "UTF-7" },
    { .code = 65001, .name = "UTF-8" }
};

#define SAV_LABEL_NAME_PREFIX         "labels"

typedef struct value_label_s {
    char             raw_value[8];
    char             utf8_string_value[8*4+1];
    readstat_value_t final_value;
    char            *label;
} value_label_t;

static readstat_error_t sav_update_progress(sav_ctx_t *ctx);
static readstat_error_t sav_read_data(sav_ctx_t *ctx);
static readstat_error_t sav_read_compressed_data(sav_ctx_t *ctx,
        readstat_error_t (*row_handler)(unsigned char *, size_t, sav_ctx_t *));
static readstat_error_t sav_read_uncompressed_data(sav_ctx_t *ctx,
        readstat_error_t (*row_handler)(unsigned char *, size_t, sav_ctx_t *));

static readstat_error_t sav_skip_variable_record(sav_ctx_t *ctx);
static readstat_error_t sav_read_variable_record(sav_ctx_t *ctx);

static readstat_error_t sav_skip_document_record(sav_ctx_t *ctx);
static readstat_error_t sav_read_document_record(sav_ctx_t *ctx);

static readstat_error_t sav_skip_value_label_record(sav_ctx_t *ctx);
static readstat_error_t sav_read_value_label_record(sav_ctx_t *ctx);

static readstat_error_t sav_read_dictionary_termination_record(sav_ctx_t *ctx);

static readstat_error_t sav_parse_machine_floating_point_record(const void *data, size_t size, size_t count, sav_ctx_t *ctx);
static readstat_error_t sav_store_variable_display_parameter_record(const void *data, size_t size, size_t count, sav_ctx_t *ctx);
static readstat_error_t sav_parse_variable_display_parameter_record(sav_ctx_t *ctx);
static readstat_error_t sav_parse_machine_integer_info_record(const void *data, size_t data_len, sav_ctx_t *ctx);
static readstat_error_t sav_parse_long_string_value_labels_record(const void *data, size_t size, size_t count, sav_ctx_t *ctx);
static readstat_error_t sav_parse_long_string_missing_values_record(const void *data, size_t size, size_t count, sav_ctx_t *ctx);

static void sav_tag_missing_double(readstat_value_t *value, sav_ctx_t *ctx) {
    double fp_value = value->v.double_value;
    uint64_t long_value = 0;
    memcpy(&long_value, &fp_value, 8);
    if (long_value == ctx->missing_double)
        value->is_system_missing = 1;
    if (long_value == ctx->lowest_double)
        value->is_system_missing = 1;
    if (long_value == ctx->highest_double)
        value->is_system_missing = 1;
    if (isnan(fp_value))
        value->is_system_missing = 1;
}

static readstat_error_t sav_update_progress(sav_ctx_t *ctx) {
    readstat_io_t *io = ctx->io;
    return io->update(ctx->file_size, ctx->handle.progress, ctx->user_ctx, io->io_ctx);
}

static readstat_error_t sav_skip_variable_record(sav_ctx_t *ctx) {
    sav_variable_record_t variable;
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    if (io->read(&variable, sizeof(sav_variable_record_t), io->io_ctx) < sizeof(sav_variable_record_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    if (variable.has_var_label) {
        uint32_t label_len;
        if (io->read(&label_len, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }
        label_len = ctx->bswap ? byteswap4(label_len) : label_len;
        uint32_t label_capacity = (label_len + 3) / 4 * 4;
        if (io->seek(label_capacity, READSTAT_SEEK_CUR, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            goto cleanup;
        }
    }
    if (variable.n_missing_values) {
        int n_missing_values = ctx->bswap ? byteswap4(variable.n_missing_values) : variable.n_missing_values;
        if (io->seek(abs(n_missing_values) * sizeof(double), READSTAT_SEEK_CUR, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            goto cleanup;
        }
    }
cleanup:
    return retval;
}

static readstat_error_t sav_read_variable_label(spss_varinfo_t *info, sav_ctx_t *ctx) {
    readstat_io_t *io = ctx->io;
    readstat_error_t retval = READSTAT_OK;
    uint32_t label_len, label_capacity;
    size_t out_label_len;
    char *label_buf = NULL;
    if (io->read(&label_len, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    label_len = ctx->bswap ? byteswap4(label_len) : label_len;

    if (label_len == 0)
        goto cleanup;

    label_capacity = (label_len + 3) / 4 * 4;
    if ((label_buf = readstat_malloc(label_capacity)) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }

    out_label_len = (size_t)label_len*4+1;
    if ((info->label = readstat_malloc(out_label_len)) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }

    if (io->read(label_buf, label_capacity, io->io_ctx) < label_capacity) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }

    retval = readstat_convert(info->label, out_label_len, label_buf, label_len, ctx->converter);
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    if (label_buf)
        free(label_buf);

    if (retval != READSTAT_OK) {
        if (info->label) {
            free(info->label);
            info->label = NULL;
        }
    }

    return retval;
}

static readstat_error_t sav_read_variable_missing_double_values(spss_varinfo_t *info, sav_ctx_t *ctx) {
    readstat_io_t *io = ctx->io;
    int i;
    readstat_error_t retval = READSTAT_OK;
    if (io->read(info->missing_double_values, info->n_missing_values * sizeof(double), io->io_ctx)
            < info->n_missing_values * sizeof(double)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    for (i=0; i<info->n_missing_values; i++) {
        if (ctx->bswap) {
            info->missing_double_values[i] = byteswap_double(info->missing_double_values[i]);
        }

        uint64_t long_value = 0;
        memcpy(&long_value, &info->missing_double_values[i], 8);

        if (long_value == ctx->missing_double)
            info->missing_double_values[i] = NAN;
        if (long_value == ctx->lowest_double)
            info->missing_double_values[i] = -HUGE_VAL;
        if (long_value == ctx->highest_double)
            info->missing_double_values[i] = HUGE_VAL;
    }

cleanup:
    return retval;
}

static readstat_error_t sav_read_variable_missing_string_values(spss_varinfo_t *info, sav_ctx_t *ctx) {
    readstat_io_t *io = ctx->io;
    int i;
    readstat_error_t retval = READSTAT_OK;
    for (i=0; i<info->n_missing_values; i++) {
        char missing_value[8];
        if (io->read(missing_value, sizeof(missing_value), io->io_ctx) < sizeof(missing_value)) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }
        retval = readstat_convert(info->missing_string_values[i], sizeof(info->missing_string_values[0]),
                missing_value, sizeof(missing_value), ctx->converter);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    return retval;
}

static readstat_error_t sav_read_variable_missing_values(spss_varinfo_t *info, sav_ctx_t *ctx) {
    if (info->n_missing_values > 3 || info->n_missing_values < -3) {
        return READSTAT_ERROR_PARSE;
    }
    if (info->n_missing_values < 0) {
        info->missing_range = 1;
        info->n_missing_values = abs(info->n_missing_values);
    } else {
        info->missing_range = 0;
    }
    if (info->type == READSTAT_TYPE_DOUBLE) {
        return sav_read_variable_missing_double_values(info, ctx);
    }
    return sav_read_variable_missing_string_values(info, ctx);
}

static readstat_error_t sav_read_variable_record(sav_ctx_t *ctx) {
    readstat_io_t *io = ctx->io;
    sav_variable_record_t variable = { 0 };
    spss_varinfo_t *info = NULL;
    readstat_error_t retval = READSTAT_OK;
    if (ctx->var_index == ctx->varinfo_capacity) {
        if ((ctx->varinfo = readstat_realloc(ctx->varinfo, (ctx->varinfo_capacity *= 2) * sizeof(spss_varinfo_t *))) == NULL) {
            retval = READSTAT_ERROR_MALLOC;
            goto cleanup;
        }
    }
    if (io->read(&variable, sizeof(sav_variable_record_t), io->io_ctx) < sizeof(sav_variable_record_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    variable.print = ctx->bswap ? byteswap4(variable.print) : variable.print;
    variable.write = ctx->bswap ? byteswap4(variable.write) : variable.write;

    int32_t type = ctx->bswap ? byteswap4(variable.type) : variable.type;
    if (type < 0) {
        if (ctx->var_index == 0) {
            return READSTAT_ERROR_PARSE;
        }
        ctx->var_offset++;
        ctx->varinfo[ctx->var_index-1]->width++;
        return 0;
    }

    if ((info = readstat_calloc(1, sizeof(spss_varinfo_t))) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }
    info->width = 1;
    info->n_segments = 1;
    info->index = ctx->var_index;
    info->offset = ctx->var_offset;
    info->labels_index = -1;

    retval = readstat_convert(info->name, sizeof(info->name),
            variable.name, sizeof(variable.name), NULL);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = readstat_convert(info->longname, sizeof(info->longname), 
            variable.name, sizeof(variable.name), NULL);
    if (retval != READSTAT_OK)
        goto cleanup;

    info->print_format.decimal_places = (variable.print & 0x000000FF);
    info->print_format.width = (variable.print & 0x0000FF00) >> 8;
    info->print_format.type = (variable.print  & 0x00FF0000) >> 16;

    info->write_format.decimal_places = (variable.write & 0x000000FF);
    info->write_format.width = (variable.write & 0x0000FF00) >> 8;
    info->write_format.type = (variable.write  & 0x00FF0000) >> 16;

    if (type > 0 || info->print_format.type == SPSS_FORMAT_TYPE_A || info->write_format.type == SPSS_FORMAT_TYPE_A) {
        info->type = READSTAT_TYPE_STRING;
    } else {
        info->type = READSTAT_TYPE_DOUBLE;
    }
    
    if (variable.has_var_label) {
        if ((retval = sav_read_variable_label(info, ctx)) != READSTAT_OK) {
            goto cleanup;
        }
    }
    
    if (variable.n_missing_values) {
        info->n_missing_values = ctx->bswap ? byteswap4(variable.n_missing_values) : variable.n_missing_values;
        if ((retval = sav_read_variable_missing_values(info, ctx)) != READSTAT_OK) {
            goto cleanup;
        }
    }
    
    ctx->varinfo[ctx->var_index] = info;

    ctx->var_index++;
    ctx->var_offset++;
    
cleanup:
    if (retval != READSTAT_OK) {
        spss_varinfo_free(info);
    }

    return retval;
}

static readstat_error_t sav_skip_value_label_record(sav_ctx_t *ctx) {
    uint32_t label_count;
    uint32_t rec_type;
    uint32_t var_count;
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    if (io->read(&label_count, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    if (ctx->bswap)
        label_count = byteswap4(label_count);
    int i;
    for (i=0; i<label_count; i++) {
        unsigned char unpadded_len = 0;
        size_t padded_len = 0;
        if (io->seek(8, READSTAT_SEEK_CUR, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            goto cleanup;
        }
        if (io->read(&unpadded_len, 1, io->io_ctx) < 1) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }
        padded_len = (unpadded_len + 8) / 8 * 8 - 1;
        if (io->seek(padded_len, READSTAT_SEEK_CUR, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            goto cleanup;
        }
    }

    if (io->read(&rec_type, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    if (ctx->bswap)
        rec_type = byteswap4(rec_type);
    
    if (rec_type != 4) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }
    if (io->read(&var_count, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    if (ctx->bswap)
        var_count = byteswap4(var_count);
    
    if (io->seek(var_count * sizeof(uint32_t), READSTAT_SEEK_CUR, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_SEEK;
        goto cleanup;
    }

cleanup:
    return retval;
}

static readstat_error_t sav_submit_value_labels(value_label_t *value_labels, int32_t label_count, 
        readstat_type_t value_type, sav_ctx_t *ctx) {
    char label_name_buf[256];
    readstat_error_t retval = READSTAT_OK;
    int32_t i;

    snprintf(label_name_buf, sizeof(label_name_buf), SAV_LABEL_NAME_PREFIX "%d", ctx->value_labels_count);

    for (i=0; i<label_count; i++) {
        value_label_t *vlabel = &value_labels[i];
        if (ctx->handle.value_label(label_name_buf, vlabel->final_value, vlabel->label, ctx->user_ctx) != READSTAT_HANDLER_OK) {
            retval = READSTAT_ERROR_USER_ABORT;
            goto cleanup;
        }
    }
cleanup:
    return retval;
}

static readstat_error_t sav_read_value_label_record(sav_ctx_t *ctx) {
    uint32_t label_count;
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    uint32_t *vars = NULL;
    uint32_t var_count;
    int32_t rec_type;
    readstat_type_t value_type = READSTAT_TYPE_STRING;
    char label_buf[256];
    value_label_t *value_labels = NULL;

    if (io->read(&label_count, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    if (ctx->bswap)
        label_count = byteswap4(label_count);
    
    if (label_count && (value_labels = readstat_calloc(label_count, sizeof(value_label_t))) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }
    
    int i;
    for (i=0; i<label_count; i++) {
        value_label_t *vlabel = &value_labels[i];
        unsigned char unpadded_label_len = 0;
        size_t padded_label_len = 0, utf8_label_len = 0;

        if (io->read(vlabel->raw_value, 8, io->io_ctx) < 8) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }
        if (io->read(&unpadded_label_len, 1, io->io_ctx) < 1) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }

        padded_label_len = (unpadded_label_len + 8) / 8 * 8 - 1;
        if (io->read(label_buf, padded_label_len, io->io_ctx) < padded_label_len) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }

        utf8_label_len = padded_label_len*4+1;
        if ((vlabel->label = readstat_malloc(utf8_label_len)) == NULL) {
            retval = READSTAT_ERROR_MALLOC;
            goto cleanup;
        }

        retval = readstat_convert(vlabel->label, utf8_label_len, label_buf, padded_label_len, ctx->converter);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

    if (io->read(&rec_type, sizeof(int32_t), io->io_ctx) < sizeof(int32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    if (ctx->bswap)
        rec_type = byteswap4(rec_type);
    
    if (rec_type != 4) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }
    if (io->read(&var_count, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    if (ctx->bswap)
        var_count = byteswap4(var_count);
    
    if (var_count && (vars = readstat_malloc(var_count * sizeof(uint32_t))) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }
    if (io->read(vars, var_count * sizeof(uint32_t), io->io_ctx) < var_count * sizeof(uint32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    for (i=0; i<var_count; i++) {
        uint32_t var_offset = vars[i];
        if (ctx->bswap)
            var_offset = byteswap4(var_offset);

        var_offset--; // Why subtract 1????
        spss_varinfo_t **var = bsearch(&var_offset, ctx->varinfo, ctx->var_index, sizeof(spss_varinfo_t *),
                &spss_varinfo_compare);
        if (var) {
            (*var)->labels_index = ctx->value_labels_count;
            value_type = (*var)->type;
        }
    }

    for (i=0; i<label_count; i++) {
        value_label_t *vlabel = &value_labels[i];
        double val_d = 0.0;
        vlabel->final_value.type = value_type;
        if (value_type == READSTAT_TYPE_DOUBLE) {
            memcpy(&val_d, vlabel->raw_value, 8);
            if (ctx->bswap)
                val_d = byteswap_double(val_d);

            vlabel->final_value.v.double_value = val_d;
            sav_tag_missing_double(&vlabel->final_value, ctx);
        } else {
            retval = readstat_convert(vlabel->utf8_string_value, sizeof(vlabel->utf8_string_value),
                    vlabel->raw_value, 8, ctx->converter);
            if (retval != READSTAT_OK)
                break;

            vlabel->final_value.v.string_value = vlabel->utf8_string_value;
        }
    }

    if (ctx->handle.value_label) {
        sav_submit_value_labels(value_labels, label_count, value_type, ctx);
    }
    ctx->value_labels_count++;
cleanup:
    if (vars)
        free(vars);
    if (value_labels) {
        for (i=0; i<label_count; i++) {
            value_label_t *vlabel = &value_labels[i];
            if (vlabel->label)
                free(vlabel->label);
        }
        free(value_labels);
    }
    
    return retval;
}

static readstat_error_t sav_skip_document_record(sav_ctx_t *ctx) {
    uint32_t n_lines;
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    if (io->read(&n_lines, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    if (ctx->bswap)
        n_lines = byteswap4(n_lines);
    if (io->seek(n_lines * SPSS_DOC_LINE_SIZE, READSTAT_SEEK_CUR, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_SEEK;
        goto cleanup;
    }
    
cleanup:
    return retval;
}

static readstat_error_t sav_read_document_record(sav_ctx_t *ctx) {
    if (!ctx->handle.note)
        return sav_skip_document_record(ctx);

    uint32_t n_lines;
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    if (io->read(&n_lines, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }
    if (ctx->bswap)
        n_lines = byteswap4(n_lines);

    char raw_buffer[SPSS_DOC_LINE_SIZE];
    char utf8_buffer[4*SPSS_DOC_LINE_SIZE+1];
    int i;
    for (i=0; i<n_lines; i++) {
        if (io->read(raw_buffer, SPSS_DOC_LINE_SIZE, io->io_ctx) < SPSS_DOC_LINE_SIZE) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }

        retval = readstat_convert(utf8_buffer, sizeof(utf8_buffer),
                raw_buffer, sizeof(raw_buffer), ctx->converter);
        if (retval != READSTAT_OK)
            goto cleanup;

        if (ctx->handle.note(i, utf8_buffer, ctx->user_ctx) != READSTAT_HANDLER_OK) {
            retval = READSTAT_ERROR_USER_ABORT;
            goto cleanup;
        }
    }

cleanup:
    return retval;
}

static readstat_error_t sav_read_dictionary_termination_record(sav_ctx_t *ctx) {
    int32_t filler;
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    if (io->read(&filler, sizeof(int32_t), io->io_ctx) < sizeof(int32_t)) {
        retval = READSTAT_ERROR_READ;
    }
    return retval;
}

static readstat_error_t sav_process_row(unsigned char *buffer, size_t buffer_len, sav_ctx_t *ctx) {
    if (ctx->row_offset) {
        ctx->row_offset--;
        return READSTAT_OK;
    }

    readstat_error_t retval = READSTAT_OK;
    double fp_value;
    int offset = 0;
    readstat_off_t data_offset = 0;
    size_t raw_str_used = 0;
    int segment_offset = 0;
    int var_index = 0, col = 0;
    int raw_str_is_utf8 = ctx->input_encoding && !strcmp(ctx->input_encoding, "UTF-8");

    while (data_offset < buffer_len && col < ctx->var_index && var_index < ctx->var_index) {
        spss_varinfo_t *col_info = ctx->varinfo[col];
        spss_varinfo_t *var_info = ctx->varinfo[var_index];
        readstat_value_t value = { .type = var_info->type };
        if (offset > 31) {
            retval = READSTAT_ERROR_PARSE;
            goto done;
        }
        if (var_info->type == READSTAT_TYPE_STRING) {
            if (raw_str_used + 8 <= ctx->raw_string_len) {
                if (raw_str_is_utf8) {
                    /* Skip null bytes, see https://github.com/tidyverse/haven/issues/560  */
                    char c;
                    for (int i=0; i<8; i++)
                        if ((c = buffer[data_offset+i]))
                            ctx->raw_string[raw_str_used++] = c;
                } else {
                    memcpy(ctx->raw_string + raw_str_used, &buffer[data_offset], 8);
                    raw_str_used += 8;
                }
            }
            if (++offset == col_info->width) {
                if (++segment_offset < var_info->n_segments) {
                    raw_str_used--;
                }
                offset = 0;
                col++;
            }
            if (segment_offset == var_info->n_segments) {
                if (!ctx->variables[var_info->index]->skip) {
                    retval = readstat_convert(ctx->utf8_string, ctx->utf8_string_len, 
                            ctx->raw_string, raw_str_used, ctx->converter);
                    if (retval != READSTAT_OK)
                        goto done;
                    value.v.string_value = ctx->utf8_string;
                    if (ctx->handle.value(ctx->current_row, ctx->variables[var_info->index],
                                value, ctx->user_ctx) != READSTAT_HANDLER_OK) {
                        retval = READSTAT_ERROR_USER_ABORT;
                        goto done;
                    }
                }
                raw_str_used = 0;
                segment_offset = 0;
                var_index += var_info->n_segments;
            }
        } else if (var_info->type == READSTAT_TYPE_DOUBLE) {
            if (!ctx->variables[var_info->index]->skip) {
                memcpy(&fp_value, &buffer[data_offset], 8);
                if (ctx->bswap) {
                    fp_value = byteswap_double(fp_value);
                }
                value.v.double_value = fp_value;
                sav_tag_missing_double(&value, ctx);
                if (ctx->handle.value(ctx->current_row, ctx->variables[var_info->index],
                            value, ctx->user_ctx) != READSTAT_HANDLER_OK) {
                    retval = READSTAT_ERROR_USER_ABORT;
                    goto done;
                }
            }
            var_index += var_info->n_segments;
            col++;
        }
        data_offset += 8;
    }
    ctx->current_row++;
done:
    return retval;
}

static readstat_error_t sav_read_data(sav_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    size_t longest_string = 256;
    int i;

    for (i=0; i<ctx->var_index;) {
        spss_varinfo_t *info = ctx->varinfo[i];
        if (info->string_length > longest_string) {
            longest_string = info->string_length;
        }
        i += info->n_segments;
    }

    ctx->raw_string_len = longest_string + sizeof(SAV_EIGHT_SPACES)-2;
    ctx->raw_string = readstat_malloc(ctx->raw_string_len);

    ctx->utf8_string_len = 4*longest_string+1 + sizeof(SAV_EIGHT_SPACES)-2;
    ctx->utf8_string = readstat_malloc(ctx->utf8_string_len);

    if (ctx->raw_string == NULL || ctx->utf8_string == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto done;
    }

    if (ctx->compression == READSTAT_COMPRESS_ROWS) {
        retval = sav_read_compressed_data(ctx, &sav_process_row);
    } else if (ctx->compression == READSTAT_COMPRESS_BINARY) {
#if HAVE_ZLIB
        retval = zsav_read_compressed_data(ctx, &sav_process_row);
#else
        retval = READSTAT_ERROR_UNSUPPORTED_COMPRESSION;
#endif
    } else {
        retval = sav_read_uncompressed_data(ctx, &sav_process_row);
    }
    if (retval != READSTAT_OK)
        goto done;

    if (ctx->record_count >= 0 && ctx->current_row != ctx->row_limit) {
        retval = READSTAT_ERROR_ROW_COUNT_MISMATCH;
    }

done:
    return retval;
}

static readstat_error_t sav_read_uncompressed_data(sav_ctx_t *ctx,
        readstat_error_t (*row_handler)(unsigned char *, size_t, sav_ctx_t *)) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    unsigned char *buffer = NULL;
    size_t bytes_read = 0;
    size_t buffer_len = ctx->var_offset * 8;

    buffer = readstat_malloc(buffer_len);

    if (ctx->row_offset) {
        if (io->seek(buffer_len * ctx->row_offset, READSTAT_SEEK_CUR, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            goto done;
        }
        ctx->row_offset = 0;
    }

    while (ctx->row_limit == -1 || ctx->current_row < ctx->row_limit) {
        retval = sav_update_progress(ctx);
        if (retval != READSTAT_OK)
            goto done;

        if ((bytes_read = io->read(buffer, buffer_len, io->io_ctx)) != buffer_len)
            goto done;

        retval = row_handler(buffer, buffer_len, ctx);
        if (retval != READSTAT_OK)
            goto done;
    }
done:
    if (buffer)
        free(buffer);

    return retval;
}

static readstat_error_t sav_read_compressed_data(sav_ctx_t *ctx,
        readstat_error_t (*row_handler)(unsigned char *, size_t, sav_ctx_t *)) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    readstat_off_t data_offset = 0;
    unsigned char buffer[DATA_BUFFER_SIZE];
    int buffer_used = 0;

    size_t uncompressed_row_len = ctx->var_offset * 8;
    readstat_off_t uncompressed_offset = 0;
    unsigned char *uncompressed_row = NULL;

    struct sav_row_stream_s state = { 
        .missing_value = ctx->missing_double,
        .bias = ctx->bias,
        .bswap = ctx->bswap };

    if (uncompressed_row_len && (uncompressed_row = readstat_malloc(uncompressed_row_len)) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto done;
    }

    while (1) {
        retval = sav_update_progress(ctx);
        if (retval != READSTAT_OK)
            goto done;

        buffer_used = io->read(buffer, sizeof(buffer), io->io_ctx);
        if (buffer_used == -1 || buffer_used == 0 || (buffer_used % 8) != 0)
            goto done;

        state.status = SAV_ROW_STREAM_HAVE_DATA;
        data_offset = 0;

        while (state.status != SAV_ROW_STREAM_NEED_DATA) {
            state.next_in = &buffer[data_offset];
            state.avail_in = buffer_used - data_offset;

            state.next_out = &uncompressed_row[uncompressed_offset];
            state.avail_out = uncompressed_row_len - uncompressed_offset;

            sav_decompress_row(&state);

            uncompressed_offset = uncompressed_row_len - state.avail_out;
            data_offset = buffer_used - state.avail_in;

            if (state.status == SAV_ROW_STREAM_FINISHED_ROW) {
                retval = row_handler(uncompressed_row, uncompressed_row_len, ctx);
                if (retval != READSTAT_OK)
                    goto done;

                uncompressed_offset = 0;
            }

            if (state.status == SAV_ROW_STREAM_FINISHED_ALL)
                goto done;
            if (ctx->row_limit > 0 && ctx->current_row == ctx->row_limit)
                goto done;
        }
    }

done:
    if (uncompressed_row)
        free(uncompressed_row);

    return retval;
}

static readstat_error_t sav_parse_machine_integer_info_record(const void *data, size_t data_len, sav_ctx_t *ctx) {
    if (data_len != 32)
        return READSTAT_ERROR_PARSE;

    const char *src_charset = NULL;
    const char *dst_charset = ctx->output_encoding;
    sav_machine_integer_info_record_t record;
    memcpy(&record, data, data_len);
    if (ctx->bswap) {
        record.character_code = byteswap4(record.character_code);
    }
    if (ctx->input_encoding) {
        src_charset = ctx->input_encoding;
    } else {
        int i;
        for (i=0; i<sizeof(_charset_table)/sizeof(_charset_table[0]); i++) {
            if (record.character_code  == _charset_table[i].code) {
                src_charset = _charset_table[i].name;
                break;
            }
        }
        if (src_charset == NULL) {
            if (ctx->handle.error) {
                char error_buf[1024];
                snprintf(error_buf, sizeof(error_buf), "Unsupported character set: %d\n", record.character_code);
                ctx->handle.error(error_buf, ctx->user_ctx);
            }
            return READSTAT_ERROR_UNSUPPORTED_CHARSET;
        }
        ctx->input_encoding = src_charset;
    }
    if (src_charset && dst_charset) {
        // You might be tempted to skip the charset conversion when src_charset
        // and dst_charset are the same. However, some versions of SPSS insert
        // illegally truncated strings (e.g. the last character is three bytes
        // but the field only has room for two bytes). So to prevent the client
        // from receiving an invalid byte sequence, we ram everything through
        // our iconv machinery.
        iconv_t converter = iconv_open(dst_charset, src_charset);
        if (converter == (iconv_t)-1) {
            return READSTAT_ERROR_UNSUPPORTED_CHARSET;
        }
        if (ctx->converter) {
            iconv_close(ctx->converter);
        }
        ctx->converter = converter;
    }
    return READSTAT_OK;
}

static readstat_error_t sav_parse_machine_floating_point_record(const void *data, size_t size, size_t count, sav_ctx_t *ctx) {
    if (size != 8 || count != 3)
        return READSTAT_ERROR_PARSE;

    sav_machine_floating_point_info_record_t fp_info;
    memcpy(&fp_info, data, sizeof(sav_machine_floating_point_info_record_t));

    ctx->missing_double = ctx->bswap ? byteswap8(fp_info.sysmis) : fp_info.sysmis;
    ctx->highest_double = ctx->bswap ? byteswap8(fp_info.highest) : fp_info.highest;
    ctx->lowest_double = ctx->bswap ? byteswap8(fp_info.lowest) : fp_info.lowest;

    return READSTAT_OK;
}

/* We don't yet know how many real variables there are, so store the values in the record
 * and make sense of them later. */
static readstat_error_t sav_store_variable_display_parameter_record(const void *data, size_t size, size_t count, sav_ctx_t *ctx) {
    if (size != 4)
        return READSTAT_ERROR_PARSE;

    const uint32_t *data_ptr = data;
    int i;

    ctx->variable_display_values = readstat_realloc(ctx->variable_display_values, count * sizeof(uint32_t));
    if (count > 0 && ctx->variable_display_values == NULL)
        return READSTAT_ERROR_MALLOC;

    ctx->variable_display_values_count = count;
    for (i=0; i<count; i++) {
        ctx->variable_display_values[i] = ctx->bswap ? byteswap4(data_ptr[i]) : data_ptr[i];
    }
    return READSTAT_OK;
}

static readstat_error_t sav_parse_variable_display_parameter_record(sav_ctx_t *ctx) {
    if (!ctx->variable_display_values)
        return READSTAT_OK;

    int i;
    long count = ctx->variable_display_values_count;
    if (count != 2 * ctx->var_index && count != 3 * ctx->var_index) {
        return READSTAT_ERROR_PARSE;
    }
    int has_display_width = ctx->var_index > 0 && (count / ctx->var_index == 3);
    int offset = 0;
    for (i=0; i<ctx->var_index;) {
        spss_varinfo_t *info = ctx->varinfo[i];
        offset = (2 + has_display_width)*i;
        info->measure = spss_measure_to_readstat_measure(ctx->variable_display_values[offset++]);
        if (has_display_width) {
            info->display_width = ctx->variable_display_values[offset++];
        }
        info->alignment = spss_alignment_to_readstat_alignment(ctx->variable_display_values[offset++]);

        i += info->n_segments;
    }
    return READSTAT_OK;
}

static readstat_error_t sav_read_pascal_string(char *buf, size_t buf_len,
        const char **inout_data_ptr, size_t data_ptr_len, sav_ctx_t *ctx) {
    const char *data_ptr = *inout_data_ptr;
    const char *data_end = data_ptr + data_ptr_len;
    readstat_error_t retval = READSTAT_OK;
    uint32_t var_name_len = 0;

    if (data_ptr + sizeof(uint32_t) > data_end) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    memcpy(&var_name_len, data_ptr, sizeof(uint32_t));
    if (ctx->bswap)
        var_name_len = byteswap4(var_name_len);

    data_ptr += sizeof(uint32_t);

    if (data_ptr + var_name_len > data_end) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    retval = readstat_convert(buf, buf_len, data_ptr, var_name_len, NULL);
    if (retval != READSTAT_OK)
        goto cleanup;

    data_ptr += var_name_len;

cleanup:
    *inout_data_ptr = data_ptr;

    return retval;
}

static readstat_error_t sav_parse_long_string_value_labels_record(const void *data, size_t size, size_t count, sav_ctx_t *ctx) {
    if (!ctx->handle.value_label)
        return READSTAT_OK;
    if (size != 1)
        return READSTAT_ERROR_PARSE;

    readstat_error_t retval = READSTAT_OK;
    uint32_t label_count = 0;
    uint32_t i = 0;
    const char *data_ptr = data;
    const char *data_end = data_ptr + count;
    char var_name_buf[256+1]; // unconverted
    char label_name_buf[256];
    char *value_buffer = NULL;
    char *label_buffer = NULL;
    
    while (data_ptr < data_end) {
        memset(label_name_buf, '\0', sizeof(label_name_buf));

        retval = sav_read_pascal_string(var_name_buf, sizeof(var_name_buf),
                &data_ptr, data_end - data_ptr, ctx);
        if (retval != READSTAT_OK)
            goto cleanup;

        for (i=0; i<ctx->var_index;) {
            spss_varinfo_t *info = ctx->varinfo[i];
            if (strcmp(var_name_buf, info->longname) == 0) {
                info->labels_index = ctx->value_labels_count++;
                snprintf(label_name_buf, sizeof(label_name_buf),
                        SAV_LABEL_NAME_PREFIX "%d", info->labels_index);
                break;
            }
            i += info->n_segments;
        }

        if (label_name_buf[0] == '\0') {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }

        data_ptr += sizeof(uint32_t);

        if (data_ptr + sizeof(uint32_t) > data_end) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }

        memcpy(&label_count, data_ptr, sizeof(uint32_t));
        if (ctx->bswap)
            label_count = byteswap4(label_count);

        data_ptr += sizeof(uint32_t);

        for (i=0; i<label_count; i++) {
            uint32_t value_len = 0, label_len = 0;
            uint32_t value_buffer_len = 0, label_buffer_len = 0;

            if (data_ptr + sizeof(uint32_t) > data_end) {
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
            }

            memcpy(&value_len, data_ptr, sizeof(uint32_t));
            if (ctx->bswap)
                value_len = byteswap4(value_len);

            data_ptr += sizeof(uint32_t);

            value_buffer_len = value_len*4+1;
            value_buffer = readstat_realloc(value_buffer, value_buffer_len);
            if (value_buffer == NULL) {
                retval = READSTAT_ERROR_MALLOC;
                goto cleanup;
            }

            if (data_ptr + value_len > data_end) {
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
            }

            retval = readstat_convert(value_buffer, value_buffer_len, data_ptr, value_len, ctx->converter);
            if (retval != READSTAT_OK)
                goto cleanup;

            data_ptr += value_len;

            if (data_ptr + sizeof(uint32_t) > data_end) {
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
            }

            memcpy(&label_len, data_ptr, sizeof(uint32_t));
            if (ctx->bswap)
                label_len = byteswap4(label_len);

            data_ptr += sizeof(uint32_t);

            label_buffer_len = label_len*4+1;
            label_buffer = readstat_realloc(label_buffer, label_buffer_len);
            if (label_buffer == NULL) {
                retval = READSTAT_ERROR_MALLOC;
                goto cleanup;
            }

            if (data_ptr + label_len > data_end) {
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
            }

            retval = readstat_convert(label_buffer, label_buffer_len, data_ptr, label_len, ctx->converter);
            if (retval != READSTAT_OK)
                goto cleanup;

            data_ptr += label_len;

            readstat_value_t value = { .type = READSTAT_TYPE_STRING };
            value.v.string_value = value_buffer;

            if (ctx->handle.value_label(label_name_buf, value, label_buffer, ctx->user_ctx) != READSTAT_HANDLER_OK) {
                retval = READSTAT_ERROR_USER_ABORT;
                goto cleanup;
            }
        }
    }

    if (data_ptr != data_end) {
        retval = READSTAT_ERROR_PARSE;
    }

cleanup:
    if (value_buffer)
        free(value_buffer);
    if (label_buffer)
        free(label_buffer);
    return retval;
}

static readstat_error_t sav_parse_long_string_missing_values_record(const void *data, size_t size, size_t count, sav_ctx_t *ctx) {
    if (size != 1)
        return READSTAT_ERROR_PARSE;

    readstat_error_t retval = READSTAT_OK;
    uint32_t i = 0, j = 0;
    const char *data_ptr = data;
    const char *data_end = data_ptr + count;
    char var_name_buf[256+1];

    while (data_ptr < data_end) {
        retval = sav_read_pascal_string(var_name_buf, sizeof(var_name_buf),
                &data_ptr, data_end - data_ptr, ctx);
        if (retval != READSTAT_OK)
            goto cleanup;

        if (data_ptr == data_end) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }

        char n_missing_values = *data_ptr++;
        if (n_missing_values < 1 || n_missing_values > 3) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }

        for (i=0; i<ctx->var_index;) {
            spss_varinfo_t *info = ctx->varinfo[i];
            if (strcmp(var_name_buf, info->longname) == 0) {
                info->n_missing_values = n_missing_values;

                uint32_t var_name_len = 0;

                if (data_ptr + sizeof(uint32_t) > data_end) {
                    retval = READSTAT_ERROR_PARSE;
                    goto cleanup;
                }

                memcpy(&var_name_len, data_ptr, sizeof(uint32_t));
                if (ctx->bswap)
                    var_name_len = byteswap4(var_name_len);

                data_ptr += sizeof(uint32_t);

                for (j=0; j<n_missing_values; j++) {
                    if (data_ptr + var_name_len > data_end) {
                        retval = READSTAT_ERROR_PARSE;
                        goto cleanup;
                    }

                    retval = readstat_convert(info->missing_string_values[j],
                            sizeof(info->missing_string_values[0]),
                            data_ptr, var_name_len, ctx->converter);
                    if (retval != READSTAT_OK)
                        goto cleanup;

                    data_ptr += var_name_len;
                }
                break;
            }
            i += info->n_segments;
        }
        if (i == ctx->var_index) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }
    }

    if (data_ptr != data_end) {
        retval = READSTAT_ERROR_PARSE;
    }

cleanup:
    return retval;
}

static readstat_error_t sav_parse_records_pass1(sav_ctx_t *ctx) {
    char data_buf[4096];
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    while (1) {
        uint32_t rec_type;
        uint32_t extra_info[3];
        size_t data_len = 0;
        int i;
        int done = 0;
        if (io->read(&rec_type, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }
        
        if (ctx->bswap) {
            rec_type = byteswap4(rec_type);
        }
        
        switch (rec_type) {
            case SAV_RECORD_TYPE_VARIABLE:
                retval = sav_skip_variable_record(ctx);
                if (retval != READSTAT_OK)
                    goto cleanup;
                break;
            case SAV_RECORD_TYPE_VALUE_LABEL:
                retval = sav_skip_value_label_record(ctx);
                if (retval != READSTAT_OK)
                    goto cleanup;
                break;
            case SAV_RECORD_TYPE_DOCUMENT:
                retval = sav_skip_document_record(ctx);
                if (retval != READSTAT_OK)
                    goto cleanup;
                break;
            case SAV_RECORD_TYPE_DICT_TERMINATION:
                done = 1;
                break;
            case SAV_RECORD_TYPE_HAS_DATA:
                if (io->read(extra_info, sizeof(extra_info), io->io_ctx) < sizeof(extra_info)) {
                    retval = READSTAT_ERROR_READ;
                    goto cleanup;
                }
                if (ctx->bswap) {
                    for (i=0; i<3; i++)
                        extra_info[i] = byteswap4(extra_info[i]);
                }
                uint32_t subtype = extra_info[0];
                size_t size = extra_info[1];
                size_t count = extra_info[2];
                data_len = size * count;
                if (subtype == SAV_RECORD_SUBTYPE_INTEGER_INFO) {
                    if (data_len > sizeof(data_buf)) {
                        retval = READSTAT_ERROR_PARSE;
                        goto cleanup;
                    }
                    if (io->read(data_buf, data_len, io->io_ctx) < data_len) {
                        retval = READSTAT_ERROR_PARSE;
                        goto cleanup;
                    }
                    retval = sav_parse_machine_integer_info_record(data_buf, data_len, ctx);
                    if (retval != READSTAT_OK)
                        goto cleanup;
                } else {
                    if (io->seek(data_len, READSTAT_SEEK_CUR, io->io_ctx) == -1) {
                        retval = READSTAT_ERROR_SEEK;
                        goto cleanup;
                    }
                }
                break;
            default:
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
                break;
        }
        if (done)
            break;
    }
cleanup:
    return retval;
}

static readstat_error_t sav_parse_records_pass2(sav_ctx_t *ctx) {
    void *data_buf = NULL;
    size_t data_buf_capacity = 4096;
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;

    if ((data_buf = readstat_malloc(data_buf_capacity)) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }

    while (1) {
        uint32_t rec_type;
        uint32_t extra_info[3];
        size_t data_len = 0;
        int i;
        int done = 0;
        if (io->read(&rec_type, sizeof(uint32_t), io->io_ctx) < sizeof(uint32_t)) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }
        
        if (ctx->bswap) {
            rec_type = byteswap4(rec_type);
        }
        
        switch (rec_type) {
            case SAV_RECORD_TYPE_VARIABLE:
                if ((retval = sav_read_variable_record(ctx)) != READSTAT_OK)
                    goto cleanup;
                break;
            case SAV_RECORD_TYPE_VALUE_LABEL:
                if ((retval = sav_read_value_label_record(ctx)) != READSTAT_OK)
                    goto cleanup;
                break;
            case SAV_RECORD_TYPE_DOCUMENT:
                if ((retval = sav_read_document_record(ctx)) != READSTAT_OK)
                    goto cleanup;
                break;
            case SAV_RECORD_TYPE_DICT_TERMINATION:
                if ((retval = sav_read_dictionary_termination_record(ctx)) != READSTAT_OK)
                    goto cleanup;
                done = 1;
                break;
            case SAV_RECORD_TYPE_HAS_DATA:
                if (io->read(extra_info, sizeof(extra_info), io->io_ctx) < sizeof(extra_info)) {
                    retval = READSTAT_ERROR_READ;
                    goto cleanup;
                }
                if (ctx->bswap) {
                    for (i=0; i<3; i++)
                        extra_info[i] = byteswap4(extra_info[i]);
                }
                uint32_t subtype = extra_info[0];
                size_t size = extra_info[1];
                size_t count = extra_info[2];
                data_len = size * count;
                if (data_buf_capacity < data_len) {
                    if ((data_buf = readstat_realloc(data_buf, data_buf_capacity = data_len)) == NULL) {
                        retval = READSTAT_ERROR_MALLOC;
                        goto cleanup;
                    }
                }
                if (data_len == 0 || io->read(data_buf, data_len, io->io_ctx) < data_len) {
                    retval = READSTAT_ERROR_PARSE;
                    goto cleanup;
                }
                
                switch (subtype) {
                    case SAV_RECORD_SUBTYPE_INTEGER_INFO:
                        /* parsed in pass 1 */
                        break;
                    case SAV_RECORD_SUBTYPE_FP_INFO:
                        retval = sav_parse_machine_floating_point_record(data_buf, size, count, ctx);
                        if (retval != READSTAT_OK)
                            goto cleanup;
                        break;
                    case SAV_RECORD_SUBTYPE_VAR_DISPLAY:
                        retval = sav_store_variable_display_parameter_record(data_buf, size, count, ctx);
                        if (retval != READSTAT_OK)
                            goto cleanup;
                        break;
                    case SAV_RECORD_SUBTYPE_LONG_VAR_NAME:
                        retval = sav_parse_long_variable_names_record(data_buf, count, ctx);
                        if (retval != READSTAT_OK)
                            goto cleanup;
                        break;
                    case SAV_RECORD_SUBTYPE_VERY_LONG_STR:
                        retval = sav_parse_very_long_string_record(data_buf, count, ctx);
                        if (retval != READSTAT_OK)
                            goto cleanup;
                        break;
                    case SAV_RECORD_SUBTYPE_LONG_STRING_VALUE_LABELS:
                        retval = sav_parse_long_string_value_labels_record(data_buf, size, count, ctx);
                        if (retval != READSTAT_OK)
                            goto cleanup;
                        break;
                    case SAV_RECORD_SUBTYPE_LONG_STRING_MISSING_VALUES:
                        retval = sav_parse_long_string_missing_values_record(data_buf, size, count, ctx);
                        if (retval != READSTAT_OK)
                            goto cleanup;
                        break;
                    default: /* misc. info */
                        break;
                }
                break;
            default:
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
                break;
        }
        if (done)
            break;
    }
cleanup:
    if (data_buf)
        free(data_buf);
    return retval;
}

static readstat_error_t sav_set_n_segments_and_var_count(sav_ctx_t *ctx) {
    int i;
    ctx->var_count = 0;
    for (i=0; i<ctx->var_index;) {
        spss_varinfo_t *info = ctx->varinfo[i];
        if (info->string_length > VERY_LONG_STRING_MAX_LENGTH)
            return READSTAT_ERROR_PARSE;
        if (info->string_length) {
            info->n_segments = (info->string_length + 251) / 252;
        }
        info->index = ctx->var_count++;
        i += info->n_segments;
    }
    ctx->variables = readstat_calloc(ctx->var_count, sizeof(readstat_variable_t *));
    return READSTAT_OK;
}

static readstat_error_t sav_handle_variables(sav_ctx_t *ctx) {
    int i;
    int index_after_skipping = 0;
    readstat_error_t retval = READSTAT_OK;

    if (!ctx->handle.variable)
        return retval;

    for (i=0; i<ctx->var_index;) {
        char label_name_buf[256];
        spss_varinfo_t *info = ctx->varinfo[i];
        ctx->variables[info->index] = spss_init_variable_for_info(info, index_after_skipping, ctx->converter);

        snprintf(label_name_buf, sizeof(label_name_buf), SAV_LABEL_NAME_PREFIX "%d", info->labels_index);

        int cb_retval = ctx->handle.variable(info->index, ctx->variables[info->index],
                info->labels_index == -1 ? NULL : label_name_buf,
                ctx->user_ctx);

        if (cb_retval == READSTAT_HANDLER_ABORT) {
            retval = READSTAT_ERROR_USER_ABORT;
            goto cleanup;
        }

        if (cb_retval == READSTAT_HANDLER_SKIP_VARIABLE) {
            ctx->variables[info->index]->skip = 1;
        } else {
            index_after_skipping++;
        }

        i += info->n_segments;
    }
cleanup:
    return retval;
}

static readstat_error_t sav_handle_fweight(sav_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    int i;
    if (ctx->handle.fweight && ctx->fweight_index >= 0) {
        for (i=0; i<ctx->var_index;) {
            spss_varinfo_t *info = ctx->varinfo[i];
            if (info->offset == ctx->fweight_index - 1) {
                if (ctx->handle.fweight(ctx->variables[info->index], ctx->user_ctx) != READSTAT_HANDLER_OK) {
                    retval = READSTAT_ERROR_USER_ABORT;
                    goto cleanup;
                }
                break;
            }
            i += info->n_segments;
        }
    }
cleanup:
    return retval;
}

readstat_error_t sav_parse_timestamp(sav_ctx_t *ctx, sav_file_header_record_t *header) {
    readstat_error_t retval = READSTAT_OK;
    struct tm timestamp = { .tm_isdst = -1 };

    if ((retval = sav_parse_time(header->creation_time, sizeof(header->creation_time),
                    &timestamp, ctx->handle.error, ctx->user_ctx)) 
            != READSTAT_OK)
        goto cleanup;

    if ((retval = sav_parse_date(header->creation_date, sizeof(header->creation_date),
                    &timestamp, ctx->handle.error, ctx->user_ctx)) 
            != READSTAT_OK)
        goto cleanup;

    ctx->timestamp = mktime(&timestamp);

cleanup:
    return retval;
}

readstat_error_t readstat_parse_sav(readstat_parser_t *parser, const char *path, void *user_ctx) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = parser->io;
    sav_file_header_record_t header;
    sav_ctx_t *ctx = NULL;
    size_t file_size = 0;
    
    if (io->open(path, io->io_ctx) == -1) {
        return READSTAT_ERROR_OPEN;
    }

    file_size = io->seek(0, READSTAT_SEEK_END, io->io_ctx);
    if (file_size == -1) {
        retval = READSTAT_ERROR_SEEK;
        goto cleanup;
    }

    if (io->seek(0, READSTAT_SEEK_SET, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_SEEK;
        goto cleanup;
    }

    if (io->read(&header, sizeof(sav_file_header_record_t), io->io_ctx) < sizeof(sav_file_header_record_t)) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }

    ctx = sav_ctx_init(&header, io);
    if (ctx == NULL) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    ctx->handle = parser->handlers;
    ctx->input_encoding = parser->input_encoding;
    ctx->output_encoding = parser->output_encoding;
    ctx->user_ctx = user_ctx;
    ctx->file_size = file_size;
    if (parser->row_offset > 0)
        ctx->row_offset = parser->row_offset;
    if (ctx->record_count >= 0) {
        int record_count_after_skipping = ctx->record_count - ctx->row_offset;
        if (record_count_after_skipping < 0) {
            record_count_after_skipping = 0;
            ctx->row_offset = ctx->record_count;
        }
        ctx->row_limit = record_count_after_skipping;
        if (parser->row_limit > 0 && parser->row_limit < record_count_after_skipping) 
            ctx->row_limit = parser->row_limit;
    } else if (parser->row_limit > 0) {
        ctx->row_limit = parser->row_limit;
    }
    
    /* ignore errors */
    sav_parse_timestamp(ctx, &header);

    if ((retval = sav_parse_records_pass1(ctx)) != READSTAT_OK)
        goto cleanup;
    
    if (io->seek(sizeof(sav_file_header_record_t), READSTAT_SEEK_SET, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_SEEK;
        goto cleanup;
    }

    if ((retval = sav_update_progress(ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = sav_parse_records_pass2(ctx)) != READSTAT_OK)
        goto cleanup;
 
    if ((retval = sav_set_n_segments_and_var_count(ctx)) != READSTAT_OK)
        goto cleanup;

    if (ctx->var_count == 0) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    if (ctx->handle.metadata) {
        readstat_metadata_t metadata = {
            .row_count = ctx->record_count < 0 ? -1 : ctx->row_limit,
            .var_count = ctx->var_count,
            .file_encoding = ctx->input_encoding,
            .file_format_version = ctx->format_version,
            .creation_time = ctx->timestamp,
            .modified_time = ctx->timestamp,
            .compression = ctx->compression,
            .endianness = ctx->endianness
        };
        if ((retval = readstat_convert(ctx->file_label, sizeof(ctx->file_label),
                        header.file_label, sizeof(header.file_label), ctx->converter)) != READSTAT_OK)
            goto cleanup;

        metadata.file_label = ctx->file_label;

        if (ctx->handle.metadata(&metadata, ctx->user_ctx) != READSTAT_HANDLER_OK) {
            retval = READSTAT_ERROR_USER_ABORT;
            goto cleanup;
        }
    }

    if ((retval = sav_parse_variable_display_parameter_record(ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = sav_handle_variables(ctx)) != READSTAT_OK)
        goto cleanup;

    if ((retval = sav_handle_fweight(ctx)) != READSTAT_OK)
        goto cleanup;

    if (ctx->handle.value) {
        retval = sav_read_data(ctx);
    }
    
cleanup:
    io->close(io->io_ctx);
    if (ctx)
        sav_ctx_free(ctx);
    
    return retval;
}
