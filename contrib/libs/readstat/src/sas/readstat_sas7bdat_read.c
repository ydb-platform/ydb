
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <inttypes.h>
#include "readstat_sas.h"
#include "readstat_sas_rle.h"
#include "../readstat_iconv.h"
#include "../readstat_convert.h"
#include "../readstat_malloc.h"

typedef struct col_info_s {
    sas_text_ref_t  name_ref;
    sas_text_ref_t  format_ref;
    sas_text_ref_t  label_ref;

    int         index;
    uint64_t    offset;
    uint32_t    width;
    int         type;
    int         format_len;
} col_info_t;

typedef struct subheader_pointer_s {
    uint64_t offset;
    uint64_t len;
    unsigned char compression;
    unsigned char is_compressed_data;
} subheader_pointer_t;

typedef struct sas7bdat_ctx_s {
    readstat_callbacks_t handle;
    int64_t              file_size;

    int            little_endian;
    int            u64;
    int            vendor;
    void          *user_ctx;
    readstat_io_t *io;
    int            bswap;
    int            did_submit_columns;

    uint32_t        row_length;
    uint32_t        page_row_count;
    uint32_t        parsed_row_count;
    uint32_t        column_count;
    uint32_t        row_limit;
    uint32_t        row_offset;

    uint64_t        header_size;
    uint64_t        page_count;
    uint64_t        page_size;
    char           *page;
    char           *row;

    uint64_t        page_header_size;
    uint64_t        subheader_signature_size;
    uint64_t        subheader_pointer_size;

    int            text_blob_count;
    size_t        *text_blob_lengths;
    char         **text_blobs;

    int            col_names_count;
    int            col_attrs_count;
    int            col_formats_count;

    size_t         max_col_width;
    char          *scratch_buffer;
    size_t         scratch_buffer_len;

    int            col_info_count;
    col_info_t    *col_info;

    readstat_variable_t **variables;

    const char    *input_encoding;
    const char    *output_encoding;
    iconv_t        converter;

    time_t         ctime;
    time_t         mtime;
    int            version;
    char           table_name[4*32+1];
    char           file_label[4*256+1];
    char           error_buf[2048];

    unsigned int  rdc_compression:1;
} sas7bdat_ctx_t;

static void sas7bdat_ctx_free(sas7bdat_ctx_t *ctx) {
    int i;
    if (ctx->text_blobs) {
        for (i=0; i<ctx->text_blob_count; i++) {
            free(ctx->text_blobs[i]);
        }
        free(ctx->text_blobs);
        free(ctx->text_blob_lengths);
    }
    if (ctx->variables) {
        for (i=0; i<ctx->column_count; i++) {
            if (ctx->variables[i])
                free(ctx->variables[i]);
        }
        free(ctx->variables);
    }
    if (ctx->col_info)
        free(ctx->col_info);

    if (ctx->scratch_buffer)
        free(ctx->scratch_buffer);

    if (ctx->page)
        free(ctx->page);

    if (ctx->row)
        free(ctx->row);

    if (ctx->converter)
        iconv_close(ctx->converter);

    free(ctx);
}

static readstat_error_t sas7bdat_update_progress(sas7bdat_ctx_t *ctx) {
    readstat_io_t *io = ctx->io;
    return io->update(ctx->file_size, ctx->handle.progress, ctx->user_ctx, io->io_ctx);
}

static sas_text_ref_t sas7bdat_parse_text_ref(const char *data, sas7bdat_ctx_t *ctx) {
    sas_text_ref_t  ref;

    ref.index = sas_read2(&data[0], ctx->bswap);
    ref.offset = sas_read2(&data[2], ctx->bswap);
    ref.length = sas_read2(&data[4], ctx->bswap);

    return ref;
}

static readstat_error_t sas7bdat_copy_text_ref(char *out_buffer, size_t out_buffer_len, sas_text_ref_t text_ref, sas7bdat_ctx_t *ctx) {
    if (text_ref.index >= ctx->text_blob_count)
        return READSTAT_ERROR_PARSE;
    
    if (text_ref.length == 0) {
        out_buffer[0] = '\0';
        return READSTAT_OK;
    }

    char *blob = ctx->text_blobs[text_ref.index];

    if (text_ref.offset + text_ref.length > ctx->text_blob_lengths[text_ref.index])
        return READSTAT_ERROR_PARSE;

    return readstat_convert(out_buffer, out_buffer_len, &blob[text_ref.offset], text_ref.length,
            ctx->converter);
}

static readstat_error_t sas7bdat_parse_column_text_subheader(const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    size_t signature_len = ctx->subheader_signature_size;
    uint16_t remainder = sas_read2(&subheader[signature_len], ctx->bswap);
    char *blob = NULL;
    if (remainder != sas_subheader_remainder(len, signature_len)) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }
    ctx->text_blob_count++;
    ctx->text_blobs = readstat_realloc(ctx->text_blobs, ctx->text_blob_count * sizeof(char *));
    ctx->text_blob_lengths = readstat_realloc(ctx->text_blob_lengths,
            ctx->text_blob_count * sizeof(ctx->text_blob_lengths[0]));
    if (ctx->text_blobs == NULL || ctx->text_blob_lengths == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }

    if ((blob = readstat_malloc(len-signature_len)) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }
    memcpy(blob, subheader+signature_len, len-signature_len);
    ctx->text_blob_lengths[ctx->text_blob_count-1] = len-signature_len;
    ctx->text_blobs[ctx->text_blob_count-1] = blob;

cleanup:
    return retval;
}

static readstat_error_t sas7bdat_realloc_col_info(sas7bdat_ctx_t *ctx, size_t count) {
    if (ctx->col_info_count < count) {
        size_t old_count = ctx->col_info_count;
        ctx->col_info_count = count;
        ctx->col_info = readstat_realloc(ctx->col_info, ctx->col_info_count * sizeof(col_info_t));
        if (ctx->col_info == NULL) {
            return READSTAT_ERROR_MALLOC;
        }
        memset(ctx->col_info + old_count, 0, (count - old_count) * sizeof(col_info_t));
    }
    return READSTAT_OK;
}

static readstat_error_t sas7bdat_parse_column_size_subheader(const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    uint64_t col_count;
    readstat_error_t retval = READSTAT_OK;

    if (ctx->column_count || ctx->did_submit_columns) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    if (len < (ctx->u64 ? 16 : 8)) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    if (ctx->u64) {
        col_count = sas_read8(&subheader[8], ctx->bswap);
    } else {
        col_count = sas_read4(&subheader[4], ctx->bswap);
    }

    ctx->column_count = col_count;

    retval = sas7bdat_realloc_col_info(ctx, ctx->column_count);

cleanup:
    return retval;
}

static readstat_error_t sas7bdat_parse_row_size_subheader(const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    uint64_t total_row_count;
    uint64_t row_length, page_row_count;

    if (len < (ctx->u64 ? 250: 190)) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    if (ctx->u64) {
        row_length = sas_read8(&subheader[40], ctx->bswap);
        total_row_count = sas_read8(&subheader[48], ctx->bswap);
        page_row_count = sas_read8(&subheader[120], ctx->bswap);
    } else {
        row_length = sas_read4(&subheader[20], ctx->bswap);
        total_row_count = sas_read4(&subheader[24], ctx->bswap);
        page_row_count = sas_read4(&subheader[60], ctx->bswap);
    }

    sas_text_ref_t file_label_ref = sas7bdat_parse_text_ref(&subheader[len-130], ctx);
    if (file_label_ref.length) {
        if ((retval = sas7bdat_copy_text_ref(ctx->file_label, sizeof(ctx->file_label),
                        file_label_ref, ctx)) != READSTAT_OK) {
            goto cleanup;
        }
    }

    sas_text_ref_t compression_ref = sas7bdat_parse_text_ref(&subheader[len-118], ctx);
    if (compression_ref.length) {
        char compression[9];
        if ((retval = sas7bdat_copy_text_ref(compression, sizeof(compression),
                        compression_ref, ctx)) != READSTAT_OK) {
            goto cleanup;
        }
        ctx->rdc_compression = (memcmp(compression, SAS_COMPRESSION_SIGNATURE_RDC, 8) == 0);
    }

    ctx->row_length = row_length;
    ctx->row = readstat_realloc(ctx->row, ctx->row_length);
    if (ctx->row == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }

    ctx->page_row_count = page_row_count;
    uint64_t total_row_count_after_skipping = total_row_count;
    if (total_row_count > ctx->row_offset) {
        total_row_count_after_skipping -= ctx->row_offset;
    } else {
        total_row_count_after_skipping = 0;
        ctx->row_offset = total_row_count;
    }
    if (ctx->row_limit == 0 || total_row_count_after_skipping < ctx->row_limit)
        ctx->row_limit = total_row_count_after_skipping;

cleanup:
    return retval;
}

static readstat_error_t sas7bdat_parse_column_name_subheader(const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    size_t signature_len = ctx->subheader_signature_size;
    int cmax = ctx->u64 ? (len-28)/8 : (len-20)/8;
    int i;
    const char *cnp = &subheader[signature_len+8];
    uint16_t remainder = sas_read2(&subheader[signature_len], ctx->bswap);

    if (remainder != sas_subheader_remainder(len, signature_len)) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    ctx->col_names_count += cmax;

    if ((retval = sas7bdat_realloc_col_info(ctx, ctx->col_names_count)) != READSTAT_OK)
        goto cleanup;

    for (i=ctx->col_names_count-cmax; i<ctx->col_names_count; i++) {
        ctx->col_info[i].name_ref = sas7bdat_parse_text_ref(cnp, ctx);
        cnp += 8;
    }

cleanup:

    return retval;
}

static readstat_error_t sas7bdat_parse_column_attributes_subheader(const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    size_t signature_len = ctx->subheader_signature_size;
    int cmax = ctx->u64 ? (len-28)/16 : (len-20)/12;
    int i;
    const char *cap = &subheader[signature_len+8];
    uint16_t remainder = sas_read2(&subheader[signature_len], ctx->bswap);

    if (remainder != sas_subheader_remainder(len, signature_len)) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }
    ctx->col_attrs_count += cmax;
    if ((retval = sas7bdat_realloc_col_info(ctx, ctx->col_attrs_count)) != READSTAT_OK)
        goto cleanup;

    for (i=ctx->col_attrs_count-cmax; i<ctx->col_attrs_count; i++) {
        if (ctx->u64) {
            ctx->col_info[i].offset = sas_read8(&cap[0], ctx->bswap);
        } else {
            ctx->col_info[i].offset = sas_read4(&cap[0], ctx->bswap);
        }

        readstat_off_t off=4;
        if (ctx->u64)
            off=8;

        ctx->col_info[i].width = sas_read4(&cap[off], ctx->bswap);
        if (ctx->col_info[i].width > ctx->max_col_width)
            ctx->max_col_width = ctx->col_info[i].width;

        if (cap[off+6] == SAS_COLUMN_TYPE_NUM) {
            ctx->col_info[i].type = READSTAT_TYPE_DOUBLE;
        } else if (cap[off+6] == SAS_COLUMN_TYPE_CHR) {
            ctx->col_info[i].type = READSTAT_TYPE_STRING;
        } else {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }
        ctx->col_info[i].index = i;
        cap += off+8;
    }

cleanup:

    return retval;
}

static readstat_error_t sas7bdat_parse_column_format_subheader(const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;

    if (len < (ctx->u64 ? 58 : 46)) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    ctx->col_formats_count++;
    if ((retval = sas7bdat_realloc_col_info(ctx, ctx->col_formats_count)) != READSTAT_OK)
        goto cleanup;

    if (ctx->u64)
        ctx->col_info[ctx->col_formats_count-1].format_len = sas_read2(&subheader[24], ctx->bswap);
    ctx->col_info[ctx->col_formats_count-1].format_ref = sas7bdat_parse_text_ref(
            ctx->u64 ? &subheader[46] : &subheader[34], ctx);
    ctx->col_info[ctx->col_formats_count-1].label_ref = sas7bdat_parse_text_ref(
            ctx->u64 ? &subheader[52] : &subheader[40], ctx);

cleanup:
    return retval;
}

static readstat_error_t sas7bdat_handle_data_value(readstat_variable_t *variable, 
        col_info_t *col_info, const char *col_data, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    int cb_retval = 0;
    readstat_value_t value;
    memset(&value, 0, sizeof(readstat_value_t));

    value.type = col_info->type;

    if (col_info->type == READSTAT_TYPE_STRING) {
        retval = readstat_convert(ctx->scratch_buffer, ctx->scratch_buffer_len,
                col_data, col_info->width, ctx->converter);
        if (retval != READSTAT_OK) {
            if (ctx->handle.error) {
                snprintf(ctx->error_buf, sizeof(ctx->error_buf),
                        "ReadStat: Error converting string (row=%u, col=%u) to specified encoding: %.*s",
                        ctx->parsed_row_count+1, col_info->index+1, col_info->width, col_data);
                ctx->handle.error(ctx->error_buf, ctx->user_ctx);
            }
            goto cleanup;
        }

        value.v.string_value = ctx->scratch_buffer;
    } else if (col_info->type == READSTAT_TYPE_DOUBLE) {
        uint64_t  val = 0;
        double dval = NAN;
        if (ctx->little_endian) {
            int k;
            for (k=0; k<col_info->width; k++) {
                val = (val << 8) | (unsigned char)col_data[col_info->width-1-k];
            }
        } else {
            int k;
            for (k=0; k<col_info->width; k++) {
                val = (val << 8) | (unsigned char)col_data[k];
            }
        }
        val <<= (8-col_info->width)*8;

        memcpy(&dval, &val, 8);

        if (isnan(dval)) {
            value.v.double_value = NAN;
            sas_assign_tag(&value, ~((val >> 40) & 0xFF));
        } else {
            value.v.double_value = dval;
        }
    }
    cb_retval = ctx->handle.value(ctx->parsed_row_count, variable, value, ctx->user_ctx);

    if (cb_retval != READSTAT_HANDLER_OK)
        retval = READSTAT_ERROR_USER_ABORT;

cleanup:
    return retval;
}

static readstat_error_t sas7bdat_parse_single_row(const char *data, sas7bdat_ctx_t *ctx) {
    if (ctx->parsed_row_count == ctx->row_limit)
        return READSTAT_OK;
    if (ctx->row_offset) {
        ctx->row_offset--;
        return READSTAT_OK;
    }

    readstat_error_t retval = READSTAT_OK;
    int j;
    if (ctx->handle.value) {
        ctx->scratch_buffer_len = 4*ctx->max_col_width+1;
        ctx->scratch_buffer = readstat_realloc(ctx->scratch_buffer, ctx->scratch_buffer_len);
        if (ctx->scratch_buffer == NULL) {
            retval = READSTAT_ERROR_MALLOC;
            goto cleanup;
        }

        for (j=0; j<ctx->column_count; j++) {
            col_info_t *col_info = &ctx->col_info[j];
            readstat_variable_t *variable = ctx->variables[j];
            if (variable->skip)
                continue;

            if (col_info->offset > ctx->row_length || col_info->offset + col_info->width > ctx->row_length) {
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
            }
            retval = sas7bdat_handle_data_value(variable, col_info, &data[col_info->offset], ctx);
            if (retval != READSTAT_OK) {
                goto cleanup;
            }
        }
    }
    ctx->parsed_row_count++;

cleanup:
    return retval;
}

static readstat_error_t sas7bdat_parse_rows(const char *data, size_t len, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    int i;
    size_t row_offset=0;
    for (i=0; i<ctx->page_row_count && ctx->parsed_row_count < ctx->row_limit; i++) {
        if (row_offset + ctx->row_length > len) {
            retval = READSTAT_ERROR_ROW_WIDTH_MISMATCH;
            goto cleanup;
        }
        if ((retval = sas7bdat_parse_single_row(&data[row_offset], ctx)) != READSTAT_OK)
            goto cleanup;

        row_offset += ctx->row_length;
    }

cleanup:
    return retval;
}

static readstat_error_t sas7bdat_parse_subheader_rdc(const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    const unsigned char *input = (const unsigned char *)subheader;
    char *buffer = malloc(ctx->row_length);
    char *output = buffer;
    while (input + 2 <= (const unsigned char *)subheader + len) {
        int i;
        unsigned short prefix = (input[0] << 8) + input[1];
        input += 2;
        for (i=0; i<16; i++) {
            if ((prefix & (1 << (15 - i))) == 0) {
                if (input + 1 > (const unsigned char *)subheader + len) {
                    break;
                }
                if (output + 1 > buffer + ctx->row_length) {
                    retval = READSTAT_ERROR_ROW_WIDTH_MISMATCH;
                    goto cleanup;
                }
                *output++ = *input++;
                continue;
            }

            if (input + 2 > (const unsigned char *)subheader + len) {
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
            }

            unsigned char marker_byte = *input++;
            unsigned char next_byte = *input++;
            size_t insert_len = 0, copy_len = 0;
            unsigned char insert_byte = 0x00;
            size_t back_offset = 0;

            if (marker_byte <= 0x0F) {
                insert_len = 3 + marker_byte;
                insert_byte = next_byte;
            } else if ((marker_byte >> 4) == 1) {
                if (input + 1 > (const unsigned char *)subheader + len) {
                    retval = READSTAT_ERROR_PARSE;
                    goto cleanup;
                }
                insert_len = 19 + (marker_byte & 0x0F) + next_byte * 16;
                insert_byte = *input++;
            } else if ((marker_byte >> 4) == 2) {
                if (input + 1 > (const unsigned char *)subheader + len) {
                    retval = READSTAT_ERROR_PARSE;
                    goto cleanup;
                }
                copy_len = 16 + (*input++);
                back_offset = 3 + (marker_byte & 0x0F) + next_byte * 16;
            } else {
                copy_len = (marker_byte >> 4);
                back_offset = 3 + (marker_byte & 0x0F) + next_byte * 16;
            }

            if (insert_len) {
                if (output + insert_len > buffer + ctx->row_length) {
                    retval = READSTAT_ERROR_ROW_WIDTH_MISMATCH;
                    goto cleanup;
                }
                memset(output, insert_byte, insert_len);
                output += insert_len;
            } else if (copy_len) {
                if (output - buffer < back_offset || copy_len > back_offset) {
                    retval = READSTAT_ERROR_PARSE;
                    goto cleanup;
                }
                if (output + copy_len > buffer + ctx->row_length) {
                    retval = READSTAT_ERROR_ROW_WIDTH_MISMATCH;
                    goto cleanup;
                }
                memcpy(output, output - back_offset, copy_len);
                output += copy_len;
            }
        }
    }

    if (output - buffer != ctx->row_length) {
        retval = READSTAT_ERROR_ROW_WIDTH_MISMATCH;
        goto cleanup;
    }
    retval = sas7bdat_parse_single_row(buffer, ctx);
cleanup:
    free(buffer);

    return retval;
}

static readstat_error_t sas7bdat_parse_subheader_rle(const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    if (ctx->row_limit == ctx->parsed_row_count)
        return READSTAT_OK;

    readstat_error_t retval = READSTAT_OK;
    ssize_t bytes_decompressed = 0;

    bytes_decompressed = sas_rle_decompress(ctx->row, ctx->row_length, subheader, len);

    if (bytes_decompressed != ctx->row_length) {
        retval = READSTAT_ERROR_ROW_WIDTH_MISMATCH;
        if (ctx->handle.error) {
            snprintf(ctx->error_buf, sizeof(ctx->error_buf), 
                    "ReadStat: Row #%d decompressed to %ld bytes (expected %d bytes)",
                    ctx->parsed_row_count, (long)(bytes_decompressed), ctx->row_length);
            ctx->handle.error(ctx->error_buf, ctx->user_ctx);
        }
        goto cleanup;
    }
    retval = sas7bdat_parse_single_row(ctx->row, ctx);

cleanup:
    return retval;
}

static readstat_error_t sas7bdat_parse_subheader_compressed(const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    if (ctx->rdc_compression)
        return sas7bdat_parse_subheader_rdc(subheader, len, ctx);

    return sas7bdat_parse_subheader_rle(subheader, len, ctx);
}

static readstat_error_t sas7bdat_parse_subheader(uint32_t signature, const char *subheader, size_t len, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;

    if (len < 2 + ctx->subheader_signature_size) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }
    if (signature == SAS_SUBHEADER_SIGNATURE_ROW_SIZE) {
        retval = sas7bdat_parse_row_size_subheader(subheader, len, ctx);
    } else if (signature == SAS_SUBHEADER_SIGNATURE_COLUMN_SIZE) {
        retval = sas7bdat_parse_column_size_subheader(subheader, len, ctx);
    } else if (signature == SAS_SUBHEADER_SIGNATURE_COUNTS) {
        /* void */
    } else if (signature == SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT) {
        retval = sas7bdat_parse_column_text_subheader(subheader, len, ctx);
    } else if (signature == SAS_SUBHEADER_SIGNATURE_COLUMN_NAME) {
        retval = sas7bdat_parse_column_name_subheader(subheader, len, ctx);
    } else if (signature == SAS_SUBHEADER_SIGNATURE_COLUMN_ATTRS) {
        retval = sas7bdat_parse_column_attributes_subheader(subheader, len, ctx);
    } else if (signature == SAS_SUBHEADER_SIGNATURE_COLUMN_FORMAT) {
        retval = sas7bdat_parse_column_format_subheader(subheader, len, ctx);
    } else if (signature == SAS_SUBHEADER_SIGNATURE_COLUMN_LIST) {
        /* void */
    } else if ((signature & SAS_SUBHEADER_SIGNATURE_COLUMN_MASK) == SAS_SUBHEADER_SIGNATURE_COLUMN_MASK) {
        /* void */
    } else {
        retval = READSTAT_ERROR_PARSE;
    }

cleanup:

    return retval;
}

static readstat_error_t sas7bdat_validate_column(col_info_t *col_info) {
    if (col_info->type == READSTAT_TYPE_DOUBLE) {
        if (col_info->width > 8 || col_info->width < 3) {
            return READSTAT_ERROR_PARSE;
        }
    }
    if (col_info->type == READSTAT_TYPE_STRING) {
        if (col_info->width > INT16_MAX || col_info->width == 0) {
            return READSTAT_ERROR_PARSE;
        }
    }
    return READSTAT_OK;
}

static readstat_variable_t *sas7bdat_init_variable(sas7bdat_ctx_t *ctx, int i, 
        int index_after_skipping, readstat_error_t *out_retval) {
    readstat_error_t retval = READSTAT_OK;
    readstat_variable_t *variable = readstat_calloc(1, sizeof(readstat_variable_t));

    variable->index = i;
    variable->index_after_skipping = index_after_skipping;
    variable->type = ctx->col_info[i].type;
    variable->storage_width = ctx->col_info[i].width;

    if ((retval = sas7bdat_validate_column(&ctx->col_info[i])) != READSTAT_OK) {
        goto cleanup;
    }
    if ((retval = sas7bdat_copy_text_ref(variable->name, sizeof(variable->name), 
                    ctx->col_info[i].name_ref, ctx)) != READSTAT_OK) {
        goto cleanup;
    }
    if ((retval = sas7bdat_copy_text_ref(variable->format, sizeof(variable->format),
                    ctx->col_info[i].format_ref, ctx)) != READSTAT_OK) {
        goto cleanup;
    }
    size_t len = strlen(variable->format);
    if (len && ctx->col_info[i].format_len) {
        snprintf(variable->format + len, sizeof(variable->format) - len, "%d", ctx->col_info[i].format_len);
    }
    if ((retval = sas7bdat_copy_text_ref(variable->label, sizeof(variable->label), 
                    ctx->col_info[i].label_ref, ctx)) != READSTAT_OK) {
        goto cleanup;
    }

cleanup:
    if (retval != READSTAT_OK) {
        if (out_retval)
            *out_retval = retval;

        if (retval == READSTAT_ERROR_CONVERT_BAD_STRING) {
            if (ctx->handle.error) {
                snprintf(ctx->error_buf, sizeof(ctx->error_buf),
                        "ReadStat: Error converting variable #%d info to specified encoding: %s %s (%s)",
                        i, variable->name, variable->format, variable->label);
                ctx->handle.error(ctx->error_buf, ctx->user_ctx);
            }
        }

        free(variable);

        return NULL;
    }

    return variable;
}

static readstat_error_t sas7bdat_submit_columns(sas7bdat_ctx_t *ctx, int compressed) {
    readstat_error_t retval = READSTAT_OK;
    if (ctx->handle.metadata) {
        readstat_metadata_t metadata = {
            .row_count = ctx->row_limit,
            .var_count = ctx->column_count,
            .table_name = ctx->table_name,
            .file_label = ctx->file_label,
            .file_encoding = ctx->input_encoding, /* orig encoding? */
            .creation_time = ctx->ctime,
            .modified_time = ctx->mtime,
            .file_format_version = ctx->version,
            .compression = READSTAT_COMPRESS_NONE,
            .endianness = ctx->little_endian ? READSTAT_ENDIAN_LITTLE : READSTAT_ENDIAN_BIG,
            .is64bit = ctx->u64
        };
        if (compressed) {
            if (ctx->rdc_compression) {
                metadata.compression = READSTAT_COMPRESS_BINARY;
            } else {
                metadata.compression = READSTAT_COMPRESS_ROWS;
            }
        }
        if (ctx->handle.metadata(&metadata, ctx->user_ctx) != READSTAT_HANDLER_OK) {
            retval = READSTAT_ERROR_USER_ABORT;
            goto cleanup;
        }
    }
    if (ctx->column_count == 0)
        goto cleanup;

    if ((ctx->variables = readstat_calloc(ctx->column_count, sizeof(readstat_variable_t *))) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }
    int i;
    int index_after_skipping = 0;
    for (i=0; i<ctx->column_count; i++) {
        ctx->variables[i] = sas7bdat_init_variable(ctx, i, index_after_skipping, &retval);
        if (ctx->variables[i] == NULL)
            break;

        int cb_retval = READSTAT_HANDLER_OK;
        if (ctx->handle.variable) {
            cb_retval = ctx->handle.variable(i, ctx->variables[i], ctx->variables[i]->format, ctx->user_ctx);
        }
        if (cb_retval == READSTAT_HANDLER_ABORT) {
            retval = READSTAT_ERROR_USER_ABORT;
            goto cleanup;
        }

        if (cb_retval == READSTAT_HANDLER_SKIP_VARIABLE) {
            ctx->variables[i]->skip = 1;
        } else {
            index_after_skipping++;
        }
    }
cleanup:
    return retval;
}

static readstat_error_t sas7bdat_submit_columns_if_needed(sas7bdat_ctx_t *ctx, int compressed) {
    readstat_error_t retval = READSTAT_OK;
    if (!ctx->did_submit_columns) {
        if ((retval = sas7bdat_submit_columns(ctx, compressed)) != READSTAT_OK) {
            goto cleanup;
        }
        ctx->did_submit_columns = 1;
    }
cleanup:
    return retval;
}

static int sas7bdat_signature_is_recognized(uint32_t signature) {
    return (signature == SAS_SUBHEADER_SIGNATURE_ROW_SIZE ||
            signature == SAS_SUBHEADER_SIGNATURE_COLUMN_SIZE ||
            signature == SAS_SUBHEADER_SIGNATURE_COUNTS ||
            signature == SAS_SUBHEADER_SIGNATURE_COLUMN_FORMAT ||
            (signature & SAS_SUBHEADER_SIGNATURE_COLUMN_MASK) == SAS_SUBHEADER_SIGNATURE_COLUMN_MASK);
}

static readstat_error_t sas7bdat_parse_subheader_pointer(const char *shp, size_t shp_size,
        subheader_pointer_t *info, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    if (ctx->u64) {
        if (shp_size <= 17) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }
        info->offset = sas_read8(&shp[0], ctx->bswap);
        info->len = sas_read8(&shp[8], ctx->bswap);
        info->compression = shp[16];
        info->is_compressed_data = shp[17];
    } else {
        if (shp_size <= 9) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }
        info->offset = sas_read4(&shp[0], ctx->bswap);
        info->len = sas_read4(&shp[4], ctx->bswap);
        info->compression = shp[8];
        info->is_compressed_data = shp[9];
    }
cleanup:
    return retval;
}

static readstat_error_t sas7bdat_validate_subheader_pointer(subheader_pointer_t *shp_info, size_t page_size,
        uint16_t subheader_count, sas7bdat_ctx_t *ctx) {
    if (shp_info->offset > page_size)
        return READSTAT_ERROR_PARSE;
    if (shp_info->len > page_size)
        return READSTAT_ERROR_PARSE;
    if (shp_info->offset + shp_info->len > page_size)
        return READSTAT_ERROR_PARSE;
    if (shp_info->offset < ctx->page_header_size + subheader_count*ctx->subheader_pointer_size)
        return READSTAT_ERROR_PARSE;
    if (shp_info->compression == SAS_COMPRESSION_NONE) {
        if (shp_info->len < ctx->subheader_signature_size)
            return READSTAT_ERROR_PARSE;
        if (shp_info->offset + ctx->subheader_signature_size > page_size)
            return READSTAT_ERROR_PARSE;
    }
    
    return READSTAT_OK;
}

/* First, extract column text */
static readstat_error_t sas7bdat_parse_page_pass1(const char *page, size_t page_size, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;

    uint16_t subheader_count = sas_read2(&page[ctx->page_header_size-4], ctx->bswap);

    int i;
    const char *shp = &page[ctx->page_header_size];
    int lshp = ctx->subheader_pointer_size;

    if (ctx->page_header_size + subheader_count*lshp > page_size) {
        retval = READSTAT_ERROR_PARSE;
        goto cleanup;
    }

    for (i=0; i<subheader_count; i++) {
        subheader_pointer_t shp_info = { 0 };
        uint32_t signature = 0;
        size_t signature_len = ctx->subheader_signature_size;
        if ((retval = sas7bdat_parse_subheader_pointer(shp, page + page_size - shp, &shp_info, ctx)) != READSTAT_OK) {
            goto cleanup;
        }
        if (shp_info.len > 0 && shp_info.compression != SAS_COMPRESSION_TRUNC) {
            if ((retval = sas7bdat_validate_subheader_pointer(&shp_info, page_size, subheader_count, ctx)) != READSTAT_OK) {
                goto cleanup;
            }
            if (shp_info.compression == SAS_COMPRESSION_NONE) {
                signature = sas_read4(page + shp_info.offset, ctx->bswap);
                if (!ctx->little_endian && signature == -1 && signature_len == 8) {
                    signature = sas_read4(page + shp_info.offset + 4, ctx->bswap);
                }
                if (signature == SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT) {
                    if ((retval = sas7bdat_parse_subheader(signature, page + shp_info.offset, shp_info.len, ctx))
                            != READSTAT_OK) {
                        goto cleanup;
                    }
                }
            } else if (shp_info.compression == SAS_COMPRESSION_ROW) {
                /* void */
            } else {
                retval = READSTAT_ERROR_UNSUPPORTED_COMPRESSION;
                goto cleanup;
            }
        }

        shp += lshp;
    }

cleanup:

    return retval;
}

static readstat_error_t sas7bdat_parse_page_pass2(const char *page, size_t page_size, sas7bdat_ctx_t *ctx) {
    uint16_t page_type;

    readstat_error_t retval = READSTAT_OK;

    page_type = sas_read2(&page[ctx->page_header_size-8], ctx->bswap);

    const char *data = NULL;

    if ((page_type & SAS_PAGE_TYPE_MASK) == SAS_PAGE_TYPE_DATA) {
        ctx->page_row_count = sas_read2(&page[ctx->page_header_size-6], ctx->bswap);
        data = &page[ctx->page_header_size];
    } else if (!(page_type & SAS_PAGE_TYPE_COMP)) {
        uint16_t subheader_count = sas_read2(&page[ctx->page_header_size-4], ctx->bswap);

        int i;
        const char *shp = &page[ctx->page_header_size];
        int lshp = ctx->subheader_pointer_size;

        if (ctx->page_header_size + subheader_count*lshp > page_size) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }

        for (i=0; i<subheader_count; i++) {
            subheader_pointer_t shp_info = { 0 };
            uint32_t signature = 0;
            if ((retval = sas7bdat_parse_subheader_pointer(shp, page + page_size - shp, &shp_info, ctx)) != READSTAT_OK) {
                goto cleanup;
            }
            if (shp_info.len > 0 && shp_info.compression != SAS_COMPRESSION_TRUNC) {
                if ((retval = sas7bdat_validate_subheader_pointer(&shp_info, page_size, subheader_count, ctx)) != READSTAT_OK) {
                    goto cleanup;
                }
                if (shp_info.compression == SAS_COMPRESSION_NONE) {
                    signature = sas_read4(page + shp_info.offset, ctx->bswap);
                    if (!ctx->little_endian && signature == -1 && ctx->u64) {
                        signature = sas_read4(page + shp_info.offset + 4, ctx->bswap);
                    }
                    if (shp_info.is_compressed_data && !sas7bdat_signature_is_recognized(signature)) {
                        if (shp_info.len != ctx->row_length) {
                            retval = READSTAT_ERROR_ROW_WIDTH_MISMATCH;
                            goto cleanup;
                        }
                        if ((retval = sas7bdat_submit_columns_if_needed(ctx, 1)) != READSTAT_OK) {
                            goto cleanup;
                        }
                        if ((retval = sas7bdat_parse_single_row(page + shp_info.offset, ctx)) != READSTAT_OK) {
                            goto cleanup;
                        }
                    } else {
                        if (signature != SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT) {
                            if ((retval = sas7bdat_parse_subheader(signature, page + shp_info.offset, shp_info.len, ctx)) != READSTAT_OK) {
                                goto cleanup;
                            }
                        }
                    }
                } else if (shp_info.compression == SAS_COMPRESSION_ROW) {
                    if ((retval = sas7bdat_submit_columns_if_needed(ctx, 1)) != READSTAT_OK) {
                        goto cleanup;
                    }
                    if ((retval = sas7bdat_parse_subheader_compressed(page + shp_info.offset, shp_info.len, ctx)) != READSTAT_OK) {
                        goto cleanup;
                    }
                } else {
                    retval = READSTAT_ERROR_UNSUPPORTED_COMPRESSION;
                    goto cleanup;
                }
            }

            shp += lshp;
        }

        if ((page_type & SAS_PAGE_TYPE_MASK) == SAS_PAGE_TYPE_MIX) {
            /* HACK - this is supposed to obey 8-byte boundaries but
             * some files created by Stat/Transfer don't. So verify that the
             * padding is { 0, 0, 0, 0 } or { ' ', ' ', ' ', ' ' } (or that
             * the file is not from Stat/Transfer) before skipping it */
            if ((shp-page)%8 == 4 && shp + 4 <= page + page_size &&
                    (*(uint32_t *)shp == 0x00000000 ||
                     *(uint32_t *)shp == 0x20202020 ||
                     ctx->vendor != READSTAT_VENDOR_STAT_TRANSFER)) {
                data = shp + 4;
            } else {
                data = shp;
            }
        }
    }
    if (data) {
        if ((retval = sas7bdat_submit_columns_if_needed(ctx, 0)) != READSTAT_OK) {
            goto cleanup;
        }
        if (ctx->handle.value) {
            retval = sas7bdat_parse_rows(data, page + page_size - data, ctx);
        }
    } 
cleanup:

    return retval;
}

static readstat_error_t sas7bdat_parse_meta_pages_pass1(sas7bdat_ctx_t *ctx, int64_t *outLastExaminedPage) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    int64_t i;

    /* look for META and MIX pages at beginning... */
    for (i=0; i<ctx->page_count; i++) {
        if (io->seek(ctx->header_size + i*ctx->page_size, READSTAT_SEEK_SET, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            if (ctx->handle.error) {
                snprintf(ctx->error_buf, sizeof(ctx->error_buf), "ReadStat: Failed to seek to position %" PRId64 
                        " (= %" PRId64 " + %" PRId64 "*%" PRId64 ")",
                        ctx->header_size + i*ctx->page_size, ctx->header_size, i, ctx->page_size);
                ctx->handle.error(ctx->error_buf, ctx->user_ctx);
            }
            goto cleanup;
        }

        readstat_off_t off = 0;
        if (ctx->u64)
            off = 16;

        size_t head_len = off + 16 + 2;
        size_t tail_len = ctx->page_size - head_len;

        if (io->read(ctx->page, head_len, io->io_ctx) < head_len) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }

        uint16_t page_type = sas_read2(&ctx->page[off+16], ctx->bswap);

        if ((page_type & SAS_PAGE_TYPE_MASK) == SAS_PAGE_TYPE_DATA)
            break;
        if ((page_type & SAS_PAGE_TYPE_COMP))
            continue;

        if (io->read(ctx->page + head_len, tail_len, io->io_ctx) < tail_len) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }

        if ((retval = sas7bdat_parse_page_pass1(ctx->page, ctx->page_size, ctx)) != READSTAT_OK) {
            if (ctx->handle.error && retval != READSTAT_ERROR_USER_ABORT) {
                int64_t pos = io->seek(0, READSTAT_SEEK_CUR, io->io_ctx);
                snprintf(ctx->error_buf, sizeof(ctx->error_buf), 
                        "ReadStat: Error parsing page %" PRId64 ", bytes %" PRId64 "-%" PRId64, 
                        i, pos - ctx->page_size, pos-1);
                ctx->handle.error(ctx->error_buf, ctx->user_ctx);
            }
            goto cleanup;
        }
    }

cleanup:
    if (outLastExaminedPage)
        *outLastExaminedPage = i;

    return retval;
}

static readstat_error_t sas7bdat_parse_amd_pages_pass1(int64_t last_examined_page_pass1, sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    uint64_t i;
    uint64_t amd_page_count = 0;

    /* ...then AMD pages at the end */
    for (i=ctx->page_count-1; i>last_examined_page_pass1; i--) {
        if (io->seek(ctx->header_size + i*ctx->page_size, READSTAT_SEEK_SET, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            if (ctx->handle.error) {
                snprintf(ctx->error_buf, sizeof(ctx->error_buf), "ReadStat: Failed to seek to position %" PRId64 
                        " (= %" PRId64 " + %" PRId64 "*%" PRId64 ")",
                        ctx->header_size + i*ctx->page_size, ctx->header_size, i, ctx->page_size);
                ctx->handle.error(ctx->error_buf, ctx->user_ctx);
            }
            goto cleanup;
        }

        readstat_off_t off = 0;
        if (ctx->u64)
            off = 16;

        size_t head_len = off + 16 + 2;
        size_t tail_len = ctx->page_size - head_len;

        if (io->read(ctx->page, head_len, io->io_ctx) < head_len) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }

        uint16_t page_type = sas_read2(&ctx->page[off+16], ctx->bswap);

        if ((page_type & SAS_PAGE_TYPE_MASK) == SAS_PAGE_TYPE_DATA) {
            /* Usually AMD pages are at the end but sometimes data pages appear after them */
            if (amd_page_count > 0)
                break;
            continue;
        }
        if ((page_type & SAS_PAGE_TYPE_COMP))
            continue;

        if (io->read(ctx->page + head_len, tail_len, io->io_ctx) < tail_len) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }

        if ((retval = sas7bdat_parse_page_pass1(ctx->page, ctx->page_size, ctx)) != READSTAT_OK) {
            if (ctx->handle.error && retval != READSTAT_ERROR_USER_ABORT) {
                int64_t pos = io->seek(0, READSTAT_SEEK_CUR, io->io_ctx);
                snprintf(ctx->error_buf, sizeof(ctx->error_buf), 
                        "ReadStat: Error parsing page %" PRId64 ", bytes %" PRId64 "-%" PRId64, 
                        i, pos - ctx->page_size, pos-1);
                ctx->handle.error(ctx->error_buf, ctx->user_ctx);
            }
            goto cleanup;
        }

        amd_page_count++;
    }

cleanup:

    return retval;
}

static readstat_error_t sas7bdat_parse_all_pages_pass2(sas7bdat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    int64_t i;

    for (i=0; i<ctx->page_count; i++) {
        if ((retval = sas7bdat_update_progress(ctx)) != READSTAT_OK) {
            goto cleanup;
        }
        if (io->read(ctx->page, ctx->page_size, io->io_ctx) < ctx->page_size) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }

        if ((retval = sas7bdat_parse_page_pass2(ctx->page, ctx->page_size, ctx)) != READSTAT_OK) {
            if (ctx->handle.error && retval != READSTAT_ERROR_USER_ABORT) {
                int64_t pos = io->seek(0, READSTAT_SEEK_CUR, io->io_ctx);
                snprintf(ctx->error_buf, sizeof(ctx->error_buf), 
                        "ReadStat: Error parsing page %" PRId64 ", bytes %" PRId64 "-%" PRId64, 
                        i, pos - ctx->page_size, pos-1);
                ctx->handle.error(ctx->error_buf, ctx->user_ctx);
            }
            goto cleanup;
        }
        if (ctx->parsed_row_count == ctx->row_limit)
            break;
    }
cleanup:

    return retval;
}

readstat_error_t readstat_parse_sas7bdat(readstat_parser_t *parser, const char *path, void *user_ctx) {
    int64_t last_examined_page_pass1 = 0;
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = parser->io;

    sas7bdat_ctx_t  *ctx = calloc(1, sizeof(sas7bdat_ctx_t));
    sas_header_info_t  *hinfo = calloc(1, sizeof(sas_header_info_t));

    ctx->handle = parser->handlers;
    ctx->input_encoding = parser->input_encoding;
    ctx->output_encoding = parser->output_encoding;
    ctx->user_ctx = user_ctx;
    ctx->io = parser->io;
    ctx->row_limit = parser->row_limit;
    if (parser->row_offset > 0)
        ctx->row_offset = parser->row_offset;

    if (io->open(path, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_OPEN;
        goto cleanup;
    }

    if ((ctx->file_size = io->seek(0, READSTAT_SEEK_END, io->io_ctx)) == -1) {
        retval = READSTAT_ERROR_SEEK;
        if (ctx->handle.error) {
            snprintf(ctx->error_buf, sizeof(ctx->error_buf), "ReadStat: Failed to seek to end of file");
            ctx->handle.error(ctx->error_buf, ctx->user_ctx);
        }
        goto cleanup;
    }

    if (io->seek(0, READSTAT_SEEK_SET, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_SEEK;
        if (ctx->handle.error) {
            snprintf(ctx->error_buf, sizeof(ctx->error_buf), "ReadStat: Failed to seek to beginning of file");
            ctx->handle.error(ctx->error_buf, ctx->user_ctx);
        }
        goto cleanup;
    }

    if ((retval = sas_read_header(io, hinfo, ctx->handle.error, user_ctx)) != READSTAT_OK) {
        goto cleanup;
    }

    ctx->u64 = hinfo->u64;
    ctx->little_endian = hinfo->little_endian;
    ctx->vendor = hinfo->vendor;
    ctx->bswap = machine_is_little_endian() ^ hinfo->little_endian;
    ctx->header_size = hinfo->header_size;
    ctx->page_count = hinfo->page_count;
    ctx->page_size = hinfo->page_size;
    ctx->page_header_size = hinfo->page_header_size;
    ctx->subheader_pointer_size = hinfo->subheader_pointer_size;
    ctx->subheader_signature_size = ctx->u64 ? 8 : 4;
    ctx->ctime = hinfo->creation_time;
    ctx->mtime = hinfo->modification_time;
    ctx->version = hinfo->major_version;
    if (ctx->input_encoding == NULL) {
        ctx->input_encoding = hinfo->encoding;
    }
    if ((ctx->page = readstat_malloc(ctx->page_size)) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }

    if (ctx->input_encoding && ctx->output_encoding && strcmp(ctx->input_encoding, ctx->output_encoding) != 0) {
        iconv_t converter = iconv_open(ctx->output_encoding, ctx->input_encoding);
        if (converter == (iconv_t)-1) {
            retval = READSTAT_ERROR_UNSUPPORTED_CHARSET;
            goto cleanup;
        }
        ctx->converter = converter;
    }

    if ((retval = readstat_convert(ctx->table_name, sizeof(ctx->table_name),
                hinfo->table_name, sizeof(hinfo->table_name), ctx->converter)) != READSTAT_OK) {
        goto cleanup;
    }

    if ((retval = sas7bdat_parse_meta_pages_pass1(ctx, &last_examined_page_pass1)) != READSTAT_OK) {
        goto cleanup;
    }

    if ((retval = sas7bdat_parse_amd_pages_pass1(last_examined_page_pass1, ctx)) != READSTAT_OK) {
        goto cleanup;
    }

    if (io->seek(ctx->header_size, READSTAT_SEEK_SET, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_SEEK;
        if (ctx->handle.error) {
            snprintf(ctx->error_buf, sizeof(ctx->error_buf), "ReadStat: Failed to seek to position %" PRId64, 
                    ctx->header_size);
            ctx->handle.error(ctx->error_buf, ctx->user_ctx);
        }
        goto cleanup;
    }

    if ((retval = sas7bdat_parse_all_pages_pass2(ctx)) != READSTAT_OK) {
        goto cleanup;
    }
    
    if ((retval = sas7bdat_submit_columns_if_needed(ctx, 0)) != READSTAT_OK) {
        goto cleanup;
    }

    if (ctx->handle.value && ctx->parsed_row_count != ctx->row_limit) {
        retval = READSTAT_ERROR_ROW_COUNT_MISMATCH;
        if (ctx->handle.error) {
            snprintf(ctx->error_buf, sizeof(ctx->error_buf), "ReadStat: Expected %d rows in file, found %d",
                    ctx->row_limit, ctx->parsed_row_count);
            ctx->handle.error(ctx->error_buf, ctx->user_ctx);
        }
        goto cleanup;
    }

    if ((retval = sas7bdat_update_progress(ctx)) != READSTAT_OK) {
        goto cleanup;
    }

cleanup:
    io->close(io->io_ctx);

    if (retval == READSTAT_ERROR_OPEN ||
            retval == READSTAT_ERROR_READ ||
            retval == READSTAT_ERROR_SEEK) {
        if (ctx->handle.error) {
            snprintf(ctx->error_buf, sizeof(ctx->error_buf), "ReadStat: %s (retval = %d): %s (errno = %d)", 
                    readstat_error_message(retval), retval, strerror(errno), errno);
            ctx->handle.error(ctx->error_buf, user_ctx);
        }
    }

    if (ctx)
        sas7bdat_ctx_free(ctx);
    if (hinfo)
        free(hinfo);

    return retval;
}
