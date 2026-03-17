
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iconv.h>

#include "../readstat.h"
#include "../readstat_writer.h"
#include "readstat_sas.h"
#include "readstat_sas_rle.h"

typedef struct sas7bdat_subheader_s {
    uint32_t    signature;
    char       *data;
    size_t      len;
    int         is_row_data;
    int         is_row_data_compressed;
} sas7bdat_subheader_t;

typedef struct sas7bdat_subheader_array_s {
    int64_t             count;
    int64_t             capacity;
    sas7bdat_subheader_t   **subheaders;
} sas7bdat_subheader_array_t;

typedef struct sas7bdat_column_text_s {
    char       *data;
    size_t      capacity;
    size_t      used;
    int64_t     index;
} sas7bdat_column_text_t;

typedef struct sas7bdat_column_text_array_s {
    int64_t                    count;
    sas7bdat_column_text_t   **column_texts;
} sas7bdat_column_text_array_t;

typedef struct sas7bdat_write_ctx_s {
    sas_header_info_t       *hinfo;
    sas7bdat_subheader_array_t   *sarray;
} sas7bdat_write_ctx_t;

static size_t sas7bdat_variable_width(readstat_type_t type, size_t user_width);

static int32_t sas7bdat_count_meta_pages(readstat_writer_t *writer) {
    sas7bdat_write_ctx_t *ctx = (sas7bdat_write_ctx_t *)writer->module_ctx;
    sas_header_info_t *hinfo = ctx->hinfo;
    sas7bdat_subheader_array_t *sarray = ctx->sarray;
    int i;
    int pages = 1;
    size_t bytes_left = hinfo->page_size - hinfo->page_header_size;
    size_t shp_ptr_size = hinfo->subheader_pointer_size;
    for (i=sarray->count-1; i>=0; i--) {
        sas7bdat_subheader_t *subheader = sarray->subheaders[i];
        if (subheader->len + shp_ptr_size > bytes_left) {
            bytes_left = hinfo->page_size - hinfo->page_header_size;
            pages++;
        }
        bytes_left -= (subheader->len + shp_ptr_size);
    }
    return pages;
}

static size_t sas7bdat_row_length(readstat_writer_t *writer) {
    int i;
    size_t len = 0;
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *variable = readstat_get_variable(writer, i);
        len += sas7bdat_variable_width(readstat_variable_get_type(variable),
                readstat_variable_get_storage_width(variable));
    }
    return len;
}

static int32_t sas7bdat_rows_per_page(readstat_writer_t *writer, sas_header_info_t *hinfo) {
    size_t row_length = sas7bdat_row_length(writer);
    return (hinfo->page_size - hinfo->page_header_size) / row_length;
}

static int32_t sas7bdat_count_data_pages(readstat_writer_t *writer, sas_header_info_t *hinfo) {
    if (writer->compression == READSTAT_COMPRESS_ROWS)
        return 0;

    int32_t rows_per_page = sas7bdat_rows_per_page(writer, hinfo);
    return (writer->row_count + (rows_per_page - 1)) / rows_per_page;
}

static sas7bdat_column_text_t *sas7bdat_column_text_init(int64_t index, size_t len) {
    sas7bdat_column_text_t *column_text = calloc(1, sizeof(sas7bdat_column_text_t));
    column_text->data = malloc(len);
    column_text->capacity = len;
    column_text->index = index;
    return column_text;
}

static void sas7bdat_column_text_free(sas7bdat_column_text_t *column_text) {
    free(column_text->data);
    free(column_text);
}

static void sas7bdat_column_text_array_free(sas7bdat_column_text_array_t *column_text_array) {
    int i;
    for (i=0; i<column_text_array->count; i++) {
        sas7bdat_column_text_free(column_text_array->column_texts[i]);
    }
    free(column_text_array->column_texts);
    free(column_text_array);
}

static sas_text_ref_t sas7bdat_make_text_ref(sas7bdat_column_text_array_t *column_text_array,
        const char *string) {
    size_t len = strlen(string);
    size_t padded_len = (len + 3) / 4 * 4;
    sas7bdat_column_text_t *column_text = column_text_array->column_texts[
        column_text_array->count-1];
    if (column_text->used + padded_len > column_text->capacity) {
        column_text_array->count++;
        column_text_array->column_texts = realloc(column_text_array->column_texts,
                sizeof(sas7bdat_column_text_t *) * column_text_array->count);

        column_text = sas7bdat_column_text_init(column_text_array->count-1,
                column_text->capacity);
        column_text_array->column_texts[column_text_array->count-1] = column_text;
    }
    sas_text_ref_t text_ref = {
        .index = column_text->index,
        .offset = column_text->used + 28,
        .length = len
    };
    strncpy(&column_text->data[column_text->used], string, padded_len);
    column_text->used += padded_len;
    return text_ref;
}

static readstat_error_t sas7bdat_emit_header(readstat_writer_t *writer, sas_header_info_t *hinfo) {
    sas_header_start_t header_start = {
        .a2 = hinfo->u64 ? SAS_ALIGNMENT_OFFSET_4 : SAS_ALIGNMENT_OFFSET_0,
        .a1 = SAS_ALIGNMENT_OFFSET_0,
        .endian = machine_is_little_endian() ? SAS_ENDIAN_LITTLE : SAS_ENDIAN_BIG,
        .file_format = SAS_FILE_FORMAT_UNIX,
        .encoding = 20, /* UTF-8 */
        .file_type = "SAS FILE",
        .file_info = "DATA    "
    };

    memcpy(&header_start.magic, sas7bdat_magic_number, sizeof(header_start.magic));

    return sas_write_header(writer, hinfo, header_start);
}

static sas7bdat_subheader_t *sas7bdat_subheader_init(uint32_t signature, size_t len) {
    sas7bdat_subheader_t *subheader = calloc(1, sizeof(sas7bdat_subheader_t));
    subheader->signature = signature;
    subheader->len = len;
    subheader->data = calloc(1, len);

    return subheader;
}

static sas7bdat_subheader_t *sas7bdat_row_size_subheader_init(readstat_writer_t *writer, 
        sas_header_info_t *hinfo, sas7bdat_column_text_array_t *column_text_array) {
    sas7bdat_subheader_t *subheader = sas7bdat_subheader_init(
            SAS_SUBHEADER_SIGNATURE_ROW_SIZE,
            hinfo->u64 ? 808 : 480);

    if (hinfo->u64) {
        int64_t row_length = sas7bdat_row_length(writer);
        int64_t row_count = writer->row_count;
        int64_t ncfl1 = writer->variables_count;
        int64_t page_size = hinfo->page_size;

        memcpy(&subheader->data[40], &row_length, sizeof(int64_t));
        memcpy(&subheader->data[48], &row_count, sizeof(int64_t));
        memcpy(&subheader->data[72], &ncfl1, sizeof(int64_t));
        memcpy(&subheader->data[104], &page_size, sizeof(int64_t));
        memset(&subheader->data[128], 0xFF, 16);
    } else {
        int32_t row_length = sas7bdat_row_length(writer);
        int32_t row_count = writer->row_count;
        int32_t ncfl1 = writer->variables_count;
        int32_t page_size = hinfo->page_size;

        memcpy(&subheader->data[20], &row_length, sizeof(int32_t));
        memcpy(&subheader->data[24], &row_count, sizeof(int32_t));
        memcpy(&subheader->data[36], &ncfl1, sizeof(int32_t));
        memcpy(&subheader->data[52], &page_size, sizeof(int32_t));
        memset(&subheader->data[64], 0xFF, 8);
    }

    sas_text_ref_t text_ref = { 0 };

    if (writer->file_label[0]) {
        text_ref = sas7bdat_make_text_ref(column_text_array, writer->file_label);
        memcpy(&subheader->data[subheader->len-130], &text_ref, sizeof(sas_text_ref_t));
    }

    if (writer->compression == READSTAT_COMPRESS_ROWS) {
        text_ref = sas7bdat_make_text_ref(column_text_array, SAS_COMPRESSION_SIGNATURE_RLE);
        memcpy(&subheader->data[subheader->len-118], &text_ref, sizeof(sas_text_ref_t));
    }

    return subheader;
}

static sas7bdat_subheader_t *sas7bdat_col_size_subheader_init(readstat_writer_t *writer, 
        sas_header_info_t *hinfo) {
    sas7bdat_subheader_t *subheader = sas7bdat_subheader_init(
            SAS_SUBHEADER_SIGNATURE_COLUMN_SIZE,
            hinfo->u64 ? 24 : 12);
    if (hinfo->u64) {
        int64_t col_count = writer->variables_count;
        memcpy(&subheader->data[8], &col_count, sizeof(int64_t));
    } else {
        int32_t col_count = writer->variables_count;
        memcpy(&subheader->data[4], &col_count, sizeof(int32_t));
    }
    return subheader;
}

static size_t sas7bdat_col_name_subheader_length(readstat_writer_t *writer,
        sas_header_info_t *hinfo) {
    return (hinfo->u64 ? 28+8*writer->variables_count : 20+8*writer->variables_count);
}

static sas7bdat_subheader_t *sas7bdat_col_name_subheader_init(readstat_writer_t *writer,
        sas_header_info_t *hinfo, sas7bdat_column_text_array_t *column_text_array) {
    size_t len = sas7bdat_col_name_subheader_length(writer, hinfo);
    size_t signature_len = hinfo->u64 ? 8 : 4;
    uint16_t remainder = sas_subheader_remainder(len, signature_len);
    sas7bdat_subheader_t *subheader = sas7bdat_subheader_init(
            SAS_SUBHEADER_SIGNATURE_COLUMN_NAME, len);
    memcpy(&subheader->data[signature_len], &remainder, sizeof(uint16_t));
    
    int i;
    char *ptrs = &subheader->data[signature_len+8];
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *variable = readstat_get_variable(writer, i);
        const char *name = readstat_variable_get_name(variable);
        sas_text_ref_t text_ref = sas7bdat_make_text_ref(column_text_array, name);
        memcpy(ptrs, &text_ref, sizeof(sas_text_ref_t));
        ptrs += 8;
    }
    return subheader;
}

static size_t sas7bdat_col_attrs_subheader_length(readstat_writer_t *writer,
        sas_header_info_t *hinfo) {
    return (hinfo->u64 ? 28+16*writer->variables_count : 20+12*writer->variables_count);
}

static sas7bdat_subheader_t *sas7bdat_col_attrs_subheader_init(readstat_writer_t *writer,
        sas_header_info_t *hinfo) {
    size_t len = sas7bdat_col_attrs_subheader_length(writer, hinfo);
    size_t signature_len = hinfo->u64 ? 8 : 4;
    uint16_t remainder = sas_subheader_remainder(len, signature_len);
    sas7bdat_subheader_t *subheader = sas7bdat_subheader_init(
            SAS_SUBHEADER_SIGNATURE_COLUMN_ATTRS, len);
    memcpy(&subheader->data[signature_len], &remainder, sizeof(uint16_t));

    char *ptrs = &subheader->data[signature_len+8];
    uint64_t offset = 0;
    int i;
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *variable = readstat_get_variable(writer, i);
        const char *name = readstat_variable_get_name(variable);
        readstat_type_t type = readstat_variable_get_type(variable);
        uint16_t name_length_flag = strlen(name) <= 8 ? 4 : 2048;
        uint32_t width = 0;
        if (hinfo->u64) {
            memcpy(&ptrs[0], &offset, sizeof(uint64_t));
            ptrs += sizeof(uint64_t);
        } else {
            uint32_t offset32 = offset;
            memcpy(&ptrs[0], &offset32, sizeof(uint32_t));
            ptrs += sizeof(uint32_t);
        }
        if (type == READSTAT_TYPE_STRING) {
            ptrs[6] = SAS_COLUMN_TYPE_CHR;
            width = readstat_variable_get_storage_width(variable);
        } else {
            ptrs[6] = SAS_COLUMN_TYPE_NUM;
            width = 8;
        }
        memcpy(&ptrs[0], &width, sizeof(uint32_t));
        memcpy(&ptrs[4], &name_length_flag, sizeof(uint16_t));
        offset += width;
        ptrs += 8;
    }
    return subheader;
}

static sas7bdat_subheader_t *sas7bdat_col_format_subheader_init(readstat_variable_t *variable,
        sas_header_info_t *hinfo, sas7bdat_column_text_array_t *column_text_array) {
    sas7bdat_subheader_t *subheader = sas7bdat_subheader_init(
            SAS_SUBHEADER_SIGNATURE_COLUMN_FORMAT,
            hinfo->u64 ? 64 : 52);
    const char *format = readstat_variable_get_format(variable);
    const char *label = readstat_variable_get_label(variable);
    off_t format_offset = hinfo->u64 ? 46 : 34;
    off_t label_offset = hinfo->u64 ? 52 : 40;
    if (format) {
        sas_text_ref_t text_ref = sas7bdat_make_text_ref(column_text_array, format);
        memcpy(&subheader->data[format_offset+0], &text_ref.index, sizeof(uint16_t));
        memcpy(&subheader->data[format_offset+2], &text_ref.offset, sizeof(uint16_t));
        memcpy(&subheader->data[format_offset+4], &text_ref.length, sizeof(uint16_t));
    }
    if (label) {
        sas_text_ref_t text_ref = sas7bdat_make_text_ref(column_text_array, label);
        memcpy(&subheader->data[label_offset+0], &text_ref.index, sizeof(uint16_t));
        memcpy(&subheader->data[label_offset+2], &text_ref.offset, sizeof(uint16_t));
        memcpy(&subheader->data[label_offset+4], &text_ref.length, sizeof(uint16_t));
    }
    return subheader;
}

static size_t sas7bdat_col_text_subheader_length(sas_header_info_t *hinfo,
        sas7bdat_column_text_t *column_text) {
    size_t signature_len = hinfo->u64 ? 8 : 4;
    size_t text_len = column_text ? column_text->used : 0;
    return signature_len + 28 + text_len;
}

static sas7bdat_subheader_t *sas7bdat_col_text_subheader_init(readstat_writer_t *writer,
        sas_header_info_t *hinfo, sas7bdat_column_text_t *column_text) {
    size_t signature_len = hinfo->u64 ? 8 : 4;
    size_t len = sas7bdat_col_text_subheader_length(hinfo, column_text);
    sas7bdat_subheader_t *subheader = sas7bdat_subheader_init(
            SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT, len);

    uint16_t used = sas_subheader_remainder(len, signature_len);
    memcpy(&subheader->data[signature_len], &used, sizeof(uint16_t));
    memset(&subheader->data[signature_len+12], ' ', 8);
    memcpy(&subheader->data[signature_len+28], column_text->data, column_text->used);
    return subheader;
}

static sas7bdat_subheader_array_t *sas7bdat_subheader_array_init(readstat_writer_t *writer,
        sas_header_info_t *hinfo) {
    sas7bdat_column_text_array_t *column_text_array = calloc(1, sizeof(sas7bdat_column_text_array_t));
    column_text_array->count = 1;
    column_text_array->column_texts = malloc(sizeof(sas7bdat_column_text_t *));
    column_text_array->column_texts[0] = sas7bdat_column_text_init(0, 
            hinfo->page_size - hinfo->page_header_size - hinfo->subheader_pointer_size - 
            sas7bdat_col_text_subheader_length(hinfo, NULL));

    sas7bdat_subheader_array_t *sarray = calloc(1, sizeof(sas7bdat_subheader_array_t));
    sarray->count = 4+writer->variables_count;
    sarray->subheaders = calloc(sarray->count, sizeof(sas7bdat_subheader_t *));

    long idx = 0;
    int i;
    sas7bdat_subheader_t *col_name_subheader = NULL;
    sas7bdat_subheader_t *col_attrs_subheader = NULL;
    sas7bdat_subheader_t **col_format_subheaders = NULL;

    col_name_subheader = sas7bdat_col_name_subheader_init(writer, hinfo, column_text_array);
    col_attrs_subheader = sas7bdat_col_attrs_subheader_init(writer, hinfo);

    sarray->subheaders[idx++] = sas7bdat_row_size_subheader_init(writer, hinfo, column_text_array);
    sarray->subheaders[idx++] = sas7bdat_col_size_subheader_init(writer, hinfo);

    col_format_subheaders = calloc(writer->variables_count, sizeof(sas7bdat_subheader_t *));
    for (i=0; i<writer->variables_count; i++) {
        readstat_variable_t *variable = readstat_get_variable(writer, i);
        col_format_subheaders[i] = sas7bdat_col_format_subheader_init(variable, hinfo, column_text_array);
    }
    sarray->count += column_text_array->count;
    sarray->subheaders = realloc(sarray->subheaders, sarray->count * sizeof(sas7bdat_subheader_t *));
    for (i=0; i<column_text_array->count; i++) {
        sarray->subheaders[idx++] = sas7bdat_col_text_subheader_init(writer, hinfo, 
                column_text_array->column_texts[i]);
    }
    sas7bdat_column_text_array_free(column_text_array);

    sarray->subheaders[idx++] = col_name_subheader;
    sarray->subheaders[idx++] = col_attrs_subheader;

    for (i=0; i<writer->variables_count; i++) {
        sarray->subheaders[idx++] = col_format_subheaders[i];
    }
    free(col_format_subheaders);

    sarray->capacity = sarray->count;

    if (writer->compression == READSTAT_COMPRESS_ROWS) {
        sarray->capacity = (sarray->count + writer->row_count);
        sarray->subheaders = realloc(sarray->subheaders, 
                sarray->capacity * sizeof(sas7bdat_subheader_t *));
    }

    return sarray;
}

static void sas7bdat_subheader_free(sas7bdat_subheader_t *subheader) {
    if (!subheader)
        return;
    if (subheader->data)
        free(subheader->data);
    free(subheader);
}

static void sas7bdat_subheader_array_free(sas7bdat_subheader_array_t *sarray) {
    int i;
    for (i=0; i<sarray->count; i++) {
        sas7bdat_subheader_free(sarray->subheaders[i]);
    }
    free(sarray->subheaders);
    free(sarray);
}

static int sas7bdat_subheader_type(uint32_t signature) {
    return (signature == SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT ||
            signature == SAS_SUBHEADER_SIGNATURE_COLUMN_NAME ||
            signature == SAS_SUBHEADER_SIGNATURE_COLUMN_ATTRS ||
            signature == SAS_SUBHEADER_SIGNATURE_COLUMN_LIST);
}

static readstat_error_t sas7bdat_emit_meta_pages(readstat_writer_t *writer) {
    sas7bdat_write_ctx_t *ctx = (sas7bdat_write_ctx_t *)writer->module_ctx;
    sas_header_info_t *hinfo = ctx->hinfo;
    sas7bdat_subheader_array_t *sarray = ctx->sarray;
    readstat_error_t retval = READSTAT_OK;
    int16_t page_type = SAS_PAGE_TYPE_META;
    char *page = malloc(hinfo->page_size);
    int64_t shp_written = 0;

    while (sarray->count > shp_written) {
        memset(page, 0, hinfo->page_size);
        int16_t shp_count = 0;
        size_t shp_data_offset = hinfo->page_size;
        size_t shp_ptr_offset = hinfo->page_header_size;
        size_t shp_ptr_size = hinfo->subheader_pointer_size;

        memcpy(&page[hinfo->page_header_size-8], &page_type, sizeof(int16_t));

        if (sarray->subheaders[shp_written]->len + shp_ptr_size >
                shp_data_offset - shp_ptr_offset) {
            retval = READSTAT_ERROR_ROW_IS_TOO_WIDE_FOR_PAGE;
            goto cleanup;
        }

        while (sarray->count > shp_written && 
                sarray->subheaders[shp_written]->len + shp_ptr_size <=
                shp_data_offset - shp_ptr_offset) {
            sas7bdat_subheader_t *subheader = sarray->subheaders[shp_written];
            uint32_t signature32 = subheader->signature;

            /* copy ptr */
            if (hinfo->u64) {
                uint64_t offset = shp_data_offset - subheader->len;
                uint64_t len = subheader->len;
                memcpy(&page[shp_ptr_offset], &offset, sizeof(uint64_t));
                memcpy(&page[shp_ptr_offset+8], &len, sizeof(uint64_t));
                if (subheader->is_row_data) {
                    if (subheader->is_row_data_compressed) {
                        page[shp_ptr_offset+16] = SAS_COMPRESSION_ROW;
                    } else {
                        page[shp_ptr_offset+16] = SAS_COMPRESSION_NONE;
                    }
                    page[shp_ptr_offset+17] = 1;
                } else {
                    page[shp_ptr_offset+17] = sas7bdat_subheader_type(subheader->signature);
                    if (signature32 >= 0xFF000000) {
                        int64_t signature64 = (int32_t)signature32;
                        memcpy(&subheader->data[0], &signature64, sizeof(int64_t));
                    } else {
                        memcpy(&subheader->data[0], &signature32, sizeof(int32_t));
                    }
                }
            } else {
                uint32_t offset = shp_data_offset - subheader->len;
                uint32_t len = subheader->len;
                memcpy(&page[shp_ptr_offset], &offset, sizeof(uint32_t));
                memcpy(&page[shp_ptr_offset+4], &len, sizeof(uint32_t));
                if (subheader->is_row_data) {
                    if (subheader->is_row_data_compressed) {
                        page[shp_ptr_offset+8] = SAS_COMPRESSION_ROW;
                    } else {
                        page[shp_ptr_offset+8] = SAS_COMPRESSION_NONE;
                    }
                    page[shp_ptr_offset+9] = 1;
                } else {
                    page[shp_ptr_offset+9] = sas7bdat_subheader_type(subheader->signature);
                    memcpy(&subheader->data[0], &signature32, sizeof(int32_t));
                }
            }
            shp_ptr_offset += shp_ptr_size;

            /* copy data */
            shp_data_offset -= subheader->len;
            memcpy(&page[shp_data_offset], subheader->data, subheader->len);

            shp_written++;
            shp_count++;
        }

        if (hinfo->u64) {
            memcpy(&page[34], &shp_count, sizeof(int16_t));
            memcpy(&page[36], &shp_count, sizeof(int16_t));
        } else {
            memcpy(&page[18], &shp_count, sizeof(int16_t));
            memcpy(&page[20], &shp_count, sizeof(int16_t));
        }

        retval = readstat_write_bytes(writer, page, hinfo->page_size);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    free(page);

    return retval;
}

static int sas7bdat_page_is_too_small(readstat_writer_t *writer, sas_header_info_t *hinfo, size_t row_length) {
    size_t page_length = hinfo->page_size - hinfo->page_header_size;

    if (writer->compression == READSTAT_COMPRESS_NONE && page_length < row_length)
        return 1;

    if (writer->compression == READSTAT_COMPRESS_ROWS && page_length < row_length + hinfo->subheader_pointer_size)
        return 1;

    if (page_length < sas7bdat_col_name_subheader_length(writer, hinfo) + hinfo->subheader_pointer_size)
        return 1;

    if (page_length < sas7bdat_col_attrs_subheader_length(writer, hinfo) + hinfo->subheader_pointer_size)
        return 1;

    return 0;
}

static sas7bdat_write_ctx_t *sas7bdat_write_ctx_init(readstat_writer_t *writer) {
    sas7bdat_write_ctx_t *ctx = calloc(1, sizeof(sas7bdat_write_ctx_t));

    sas_header_info_t *hinfo = sas_header_info_init(writer, writer->is_64bit);

    size_t row_length = sas7bdat_row_length(writer);

    while (sas7bdat_page_is_too_small(writer, hinfo, row_length)) {
        hinfo->page_size <<= 1;
    }

    ctx->hinfo = hinfo;
    ctx->sarray = sas7bdat_subheader_array_init(writer, hinfo);

    return ctx;
}

static void sas7bdat_write_ctx_free(sas7bdat_write_ctx_t *ctx) {
    free(ctx->hinfo);
    sas7bdat_subheader_array_free(ctx->sarray);
    free(ctx);
}

static readstat_error_t sas7bdat_emit_header_and_meta_pages(readstat_writer_t *writer) {
    sas7bdat_write_ctx_t *ctx = (sas7bdat_write_ctx_t *)writer->module_ctx;
    readstat_error_t retval = READSTAT_OK;

    if (sas7bdat_row_length(writer) == 0) {
        retval = READSTAT_ERROR_TOO_FEW_COLUMNS;
        goto cleanup;
    }

    if (writer->compression == READSTAT_COMPRESS_NONE &&
            sas7bdat_rows_per_page(writer, ctx->hinfo) == 0) {
        retval = READSTAT_ERROR_ROW_IS_TOO_WIDE_FOR_PAGE;
        goto cleanup;
    }

    ctx->hinfo->page_count = sas7bdat_count_meta_pages(writer) + sas7bdat_count_data_pages(writer, ctx->hinfo);

    retval = sas7bdat_emit_header(writer, ctx->hinfo);
    if (retval != READSTAT_OK)
        goto cleanup;

    retval = sas7bdat_emit_meta_pages(writer);
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t sas7bdat_begin_data(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    readstat_error_t retval = READSTAT_OK;

    writer->module_ctx = sas7bdat_write_ctx_init(writer);

    if (writer->compression == READSTAT_COMPRESS_NONE) {
        retval = sas7bdat_emit_header_and_meta_pages(writer);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    if (retval != READSTAT_OK) {
        if (writer->module_ctx) {
            sas7bdat_write_ctx_free(writer->module_ctx);
            writer->module_ctx = NULL;
        }
    }

    return retval;
}

static readstat_error_t sas7bdat_end_data(void *writer_ctx) {
    readstat_error_t retval = READSTAT_OK;
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    sas7bdat_write_ctx_t *ctx = (sas7bdat_write_ctx_t *)writer->module_ctx;

    if (writer->compression == READSTAT_COMPRESS_ROWS) {
        retval = sas7bdat_emit_header_and_meta_pages(writer);
    } else {
        retval = sas_fill_page(writer, ctx->hinfo);
    }

    return retval;
}

static void sas7bdat_module_ctx_free(void *module_ctx) {
    sas7bdat_write_ctx_free(module_ctx);
}

static readstat_error_t sas7bdat_write_double(void *row, const readstat_variable_t *var, double value) {
    memcpy(row, &value, sizeof(double));
    return READSTAT_OK;
}

static readstat_error_t sas7bdat_write_float(void *row, const readstat_variable_t *var, float value) {
    return sas7bdat_write_double(row, var, value);
}

static readstat_error_t sas7bdat_write_int32(void *row, const readstat_variable_t *var, int32_t value) {
    return sas7bdat_write_double(row, var, value);
}

static readstat_error_t sas7bdat_write_int16(void *row, const readstat_variable_t *var, int16_t value) {
    return sas7bdat_write_double(row, var, value);
}

static readstat_error_t sas7bdat_write_int8(void *row, const readstat_variable_t *var, int8_t value) {
    return sas7bdat_write_double(row, var, value);
}

static readstat_error_t sas7bdat_write_missing_tagged_raw(void *row, const readstat_variable_t *var, char tag) {
    union {
        double dval;
        char   chars[8];
    } nan_value;

    nan_value.dval = NAN;
    nan_value.chars[machine_is_little_endian() ? 5 : 2] = ~tag;
    return sas7bdat_write_double(row, var, nan_value.dval);
}

static readstat_error_t sas7bdat_write_missing_tagged(void *row, const readstat_variable_t *var, char tag) {
    readstat_error_t error = sas_validate_tag(tag);
    if (error == READSTAT_OK)
        return sas7bdat_write_missing_tagged_raw(row, var, tag);

    return error;
}

static readstat_error_t sas7bdat_write_missing_numeric(void *row, const readstat_variable_t *var) {
    return sas7bdat_write_missing_tagged_raw(row, var, '.');
}

static readstat_error_t sas7bdat_write_string(void *row, const readstat_variable_t *var, const char *value) {
    size_t max_len = readstat_variable_get_storage_width(var);
    if (value == NULL || value[0] == '\0') {
        memset(row, '\0', max_len);
    } else {
        size_t value_len = strlen(value);
        if (value_len > max_len)
            return READSTAT_ERROR_STRING_VALUE_IS_TOO_LONG;

        strncpy((char *)row, value, max_len);
    }
    return READSTAT_OK;
}

static readstat_error_t sas7bdat_write_missing_string(void *row, const readstat_variable_t *var) {
    return sas7bdat_write_string(row, var, NULL);
}

static size_t sas7bdat_variable_width(readstat_type_t type, size_t user_width) {
    if (type == READSTAT_TYPE_STRING) {
        return user_width;
    }
    return 8;
}

static readstat_error_t sas7bdat_write_row_uncompressed(readstat_writer_t *writer, sas7bdat_write_ctx_t *ctx,
        void *bytes, size_t len) {
    readstat_error_t retval = READSTAT_OK;
    sas_header_info_t *hinfo = ctx->hinfo;

    int32_t rows_per_page = sas7bdat_rows_per_page(writer, hinfo);
    if (writer->current_row % rows_per_page == 0) {
        retval = sas_fill_page(writer, ctx->hinfo);
        if (retval != READSTAT_OK)
            goto cleanup;

        int16_t page_type = SAS_PAGE_TYPE_DATA;
        int16_t page_row_count = (writer->row_count - writer->current_row < rows_per_page 
                ? writer->row_count - writer->current_row
                : rows_per_page);
        char *header = calloc(hinfo->page_header_size, 1);
        memcpy(&header[hinfo->page_header_size-6], &page_row_count, sizeof(int16_t));
        memcpy(&header[hinfo->page_header_size-8], &page_type, sizeof(int16_t));
        retval = readstat_write_bytes(writer, header, hinfo->page_header_size);
        free(header);
        if (retval != READSTAT_OK)
            goto cleanup;
    }

    retval = readstat_write_bytes(writer, bytes, len);

cleanup:
    return retval;
}

/* We don't actually write compressed data out at this point; the file header
 * requires a page count, so instead we collect the compressed subheaders in
 * memory and write the entire file at the end, once the page count can be
 * determined.
 */
static readstat_error_t sas7bdat_write_row_compressed(readstat_writer_t *writer, sas7bdat_write_ctx_t *ctx,
        void *bytes, size_t len) {
    readstat_error_t retval = READSTAT_OK;
    size_t compressed_len = sas_rle_compressed_len(bytes, len);

    sas7bdat_subheader_t *subheader = NULL;
    if (compressed_len < len) {
        subheader = sas7bdat_subheader_init(0, compressed_len);
        subheader->is_row_data = 1;
        subheader->is_row_data_compressed = 1;
        size_t actual_len = sas_rle_compress(subheader->data, subheader->len, bytes, len);
        if (actual_len != compressed_len) {
            retval = READSTAT_ERROR_ROW_WIDTH_MISMATCH;
            goto cleanup;
        }
    } else {
        subheader = sas7bdat_subheader_init(0, len);
        subheader->is_row_data = 1;
        memcpy(subheader->data, bytes, len);
    }

    ctx->sarray->subheaders[ctx->sarray->count++] = subheader;

cleanup:
    if (retval != READSTAT_OK)
        sas7bdat_subheader_free(subheader);

    return retval;
}

static readstat_error_t sas7bdat_write_row(void *writer_ctx, void *bytes, size_t len) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    sas7bdat_write_ctx_t *ctx = (sas7bdat_write_ctx_t *)writer->module_ctx;
    readstat_error_t retval = READSTAT_OK;

    if (writer->compression == READSTAT_COMPRESS_NONE) {
        retval = sas7bdat_write_row_uncompressed(writer, ctx, bytes, len);
    } else if (writer->compression == READSTAT_COMPRESS_ROWS) {
        retval = sas7bdat_write_row_compressed(writer, ctx, bytes, len);
    }

    return retval;
}

static readstat_error_t sas7bdat_metadata_ok(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;

    if (writer->compression != READSTAT_COMPRESS_NONE &&
            writer->compression != READSTAT_COMPRESS_ROWS)
        return READSTAT_ERROR_UNSUPPORTED_COMPRESSION;

    return READSTAT_OK;
}

readstat_error_t readstat_begin_writing_sas7bdat(readstat_writer_t *writer, void *user_ctx, long row_count) {

    if (writer->version == 0)
        writer->version = SAS_DEFAULT_FILE_VERSION;

    writer->callbacks.metadata_ok = &sas7bdat_metadata_ok;
    writer->callbacks.write_int8 = &sas7bdat_write_int8;
    writer->callbacks.write_int16 = &sas7bdat_write_int16;
    writer->callbacks.write_int32 = &sas7bdat_write_int32;
    writer->callbacks.write_float = &sas7bdat_write_float;
    writer->callbacks.write_double = &sas7bdat_write_double;

    writer->callbacks.write_string = &sas7bdat_write_string;
    writer->callbacks.write_missing_string = &sas7bdat_write_missing_string;
    writer->callbacks.write_missing_number = &sas7bdat_write_missing_numeric;
    writer->callbacks.write_missing_tagged = &sas7bdat_write_missing_tagged;

    writer->callbacks.variable_width = &sas7bdat_variable_width;
    writer->callbacks.variable_ok = &sas_validate_variable;

    writer->callbacks.begin_data = &sas7bdat_begin_data;
    writer->callbacks.end_data = &sas7bdat_end_data;
    writer->callbacks.module_ctx_free = &sas7bdat_module_ctx_free;

    writer->callbacks.write_row = &sas7bdat_write_row;

    return readstat_begin_writing_file(writer, user_ctx, row_count);
}
