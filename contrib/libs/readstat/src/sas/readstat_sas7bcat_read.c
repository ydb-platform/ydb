#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include "readstat_sas.h"
#include "../readstat_iconv.h"
#include "../readstat_convert.h"
#include "../readstat_malloc.h"

#define SAS_CATALOG_FIRST_INDEX_PAGE 1
#define SAS_CATALOG_USELESS_PAGES    3

typedef struct sas7bcat_ctx_s {
    readstat_metadata_handler      metadata_handler;
    readstat_value_label_handler   value_label_handler;
    void          *user_ctx;
    readstat_io_t *io;
    int            u64;
    int            pad1;
    int            bswap;
    int64_t        xlsr_size;
    int64_t        xlsr_offset;
    int64_t        xlsr_O_offset;
    int64_t        page_count;
    int64_t        page_size;
    int64_t        header_size;
    uint64_t      *block_pointers;
    int            block_pointers_used;
    int            block_pointers_capacity;
    const char    *input_encoding;
    const char    *output_encoding;
    iconv_t        converter;
} sas7bcat_ctx_t;

static void sas7bcat_ctx_free(sas7bcat_ctx_t *ctx) {
    if (ctx->converter)
        iconv_close(ctx->converter);
    if (ctx->block_pointers)
        free(ctx->block_pointers);

    free(ctx);
}

static readstat_error_t sas7bcat_parse_value_labels(const char *value_start, size_t value_labels_len, 
        int label_count_used, int label_count_capacity, const char *name, sas7bcat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    int i;
    const char *lbp1 = value_start;
    uint32_t *value_offset = readstat_calloc(label_count_used, sizeof(uint32_t));
    /* Doubles appear to be stored as big-endian, always */
    int bswap_doubles = machine_is_little_endian();
    int is_string = (name[0] == '$');
    char *label = NULL;

    if (value_offset == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }

    /* Pass 1 -- find out the offset of the labels */
    for (i=0; i<label_count_capacity; i++) {
        if (&lbp1[3] - value_start > value_labels_len || sas_read2(&lbp1[2], ctx->bswap) < 0) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }
        if (i<label_count_used) {
            if (&lbp1[10+ctx->pad1+4] - value_start > value_labels_len) {
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
            }
            uint32_t label_pos = sas_read4(&lbp1[10+ctx->pad1], ctx->bswap);
            if (label_pos >= label_count_used) {
                retval = READSTAT_ERROR_PARSE;
                goto cleanup;
            }
            value_offset[label_pos] = lbp1 - value_start;
        }
        lbp1 += 6 + sas_read2(&lbp1[2], ctx->bswap);
    }

    const char *lbp2 = lbp1;

    /* Pass 2 -- parse pairs of values & labels */
    for (i=0; i<label_count_used && i<label_count_capacity; i++) {
        lbp1 = value_start + value_offset[i];

        if (&lbp1[30] - value_start > value_labels_len ||
                &lbp2[10] - value_start > value_labels_len) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }
        readstat_value_t value = { .type = is_string ? READSTAT_TYPE_STRING : READSTAT_TYPE_DOUBLE };
        char string_val[4*16+1];
        if (is_string) {
            size_t value_entry_len = 6 + sas_read2(&lbp1[2], ctx->bswap);
            retval = readstat_convert(string_val, sizeof(string_val),
                    &lbp1[value_entry_len-16], 16, ctx->converter);
            if (retval != READSTAT_OK)
                goto cleanup;

            value.v.string_value = string_val;
        } else {
            uint64_t val = sas_read8(&lbp1[22], bswap_doubles);
            double dval = NAN;
            if ((val | 0xFF0000000000) == 0xFFFFFFFFFFFF) {
                sas_assign_tag(&value, (val >> 40));
            } else {
                memcpy(&dval, &val, 8);
                dval *= -1.0;
            }

            value.v.double_value = dval;
        }
        size_t label_len = sas_read2(&lbp2[8], ctx->bswap);
        if (&lbp2[10] + label_len - value_start > value_labels_len) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }
        if (ctx->value_label_handler) {
            label = realloc(label, 4 * label_len + 1);
            retval = readstat_convert(label, 4 * label_len + 1,
                    &lbp2[10], label_len, ctx->converter);
            if (retval != READSTAT_OK)
                goto cleanup;

            if (ctx->value_label_handler(name, value, label, ctx->user_ctx) != READSTAT_HANDLER_OK) {
                retval = READSTAT_ERROR_USER_ABORT;
                goto cleanup;
            }
        }

        lbp2 += 8 + 2 + label_len + 1;
    }

cleanup:
    free(label);
    free(value_offset);
    return retval;
}

static readstat_error_t sas7bcat_parse_block(const char *data, size_t data_size, sas7bcat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;

    size_t pad = 0;
    uint64_t label_count_capacity = 0;
    uint64_t label_count_used = 0;
    int payload_offset = 106;
    uint16_t flags = 0;
    char name[4*32+1];

    if (data_size < payload_offset)
        goto cleanup;

    flags = sas_read2(&data[2], ctx->bswap);
    pad = (flags & 0x08) ? 4 : 0; // might be 0x10, not sure
    if (ctx->u64) {
        label_count_capacity = sas_read8(&data[42+pad], ctx->bswap);
        label_count_used = sas_read8(&data[50+pad], ctx->bswap);

        payload_offset += 32;
    } else {
        label_count_capacity = sas_read4(&data[38+pad], ctx->bswap);
        label_count_used = sas_read4(&data[42+pad], ctx->bswap);
    }

    if ((retval = readstat_convert(name, sizeof(name), &data[8], 8, ctx->converter)) != READSTAT_OK)
        goto cleanup;

    if (pad) {
        pad += 16;
    }

    if (((flags & 0x80) && !ctx->u64) || ((flags & 0x20) && ctx->u64)) { // has long name
        if (data_size < payload_offset + pad + 32)
            goto cleanup;

        retval = readstat_convert(name, sizeof(name), &data[payload_offset+pad], 32, ctx->converter);
        if (retval != READSTAT_OK)
            goto cleanup;
        pad += 32;
    }

    if (data_size < payload_offset + pad)
        goto cleanup;

    if (label_count_used == 0)
        goto cleanup;

    if ((retval = sas7bcat_parse_value_labels(&data[payload_offset+pad], data_size - payload_offset - pad,
                    label_count_used, label_count_capacity, name, ctx)) != READSTAT_OK)
        goto cleanup;

cleanup:
    return retval;
}

static readstat_error_t sas7bcat_augment_index(const char *index, size_t len, sas7bcat_ctx_t *ctx) {
    const char *xlsr = index;
    readstat_error_t retval = READSTAT_OK;
    while (xlsr + ctx->xlsr_size <= index + len) {
        if (memcmp(xlsr, "XLSR", 4) != 0) // some block pointers seem to have 8 bytes of extra padding
            xlsr += 8;
        if (memcmp(xlsr, "XLSR", 4) != 0)
            break;

        if (xlsr[ctx->xlsr_O_offset] == 'O') {
            uint64_t page = 0, pos = 0;
            if (ctx->u64) {
                page = sas_read8(&xlsr[8], ctx->bswap);
                pos = sas_read2(&xlsr[16], ctx->bswap);
            } else {
                page = sas_read4(&xlsr[4], ctx->bswap);
                pos = sas_read2(&xlsr[8], ctx->bswap);
            }
            ctx->block_pointers[ctx->block_pointers_used++] = (page << 32) + pos;
        }

        if (ctx->block_pointers_used == ctx->block_pointers_capacity) {
            ctx->block_pointers = readstat_realloc(ctx->block_pointers, (ctx->block_pointers_capacity *= 2) * sizeof(uint64_t));
            if (ctx->block_pointers == NULL) {
                retval = READSTAT_ERROR_MALLOC;
                goto cleanup;
            }
        }

        xlsr += ctx->xlsr_size;
    }
cleanup:
    return retval;
}

static int compare_block_pointers(const void *elem1, const void *elem2) {
    uint64_t v1 = *(const uint64_t *)elem1;
    uint64_t v2 = *(const uint64_t *)elem2;
    return v1 - v2;
}

static void sas7bcat_sort_index(sas7bcat_ctx_t *ctx) {
    if (ctx->block_pointers_used == 0)
        return;

    int i;
    for (i=1; i<ctx->block_pointers_used; i++) {
        if (ctx->block_pointers[i] < ctx->block_pointers[i-1]) {
            qsort(ctx->block_pointers, ctx->block_pointers_used, sizeof(uint64_t), &compare_block_pointers);
            break;
        }
    }
}

static void sas7bcat_uniq_index(sas7bcat_ctx_t *ctx) {
    if (ctx->block_pointers_used == 0)
        return;

    int i;
    int out_i = 1;
    for (i=1; i<ctx->block_pointers_used; i++) {
        if (ctx->block_pointers[i] != ctx->block_pointers[i-1]) {
            if (out_i != i) {
                ctx->block_pointers[out_i] = ctx->block_pointers[i];
            }
            out_i++;
        }
    }
    ctx->block_pointers_used = out_i;
}

static int sas7bcat_block_size(int start_page, int start_page_pos, sas7bcat_ctx_t *ctx, readstat_error_t *outError) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    int next_page = start_page;
    int next_page_pos = start_page_pos;
    int link_count = 0;

    int buffer_len = 0;
    int chain_link_len = 0;

    char chain_link[32];
    int chain_link_header_len = 16;
    if (ctx->u64) {
        chain_link_header_len = 32;
    }

    // calculate buffer size needed
    while (next_page > 0 && next_page_pos > 0 && next_page <= ctx->page_count && link_count++ < ctx->page_count) {
        if (io->seek(ctx->header_size+(next_page-1)*ctx->page_size+next_page_pos, READSTAT_SEEK_SET, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            goto cleanup;
        }
        if (io->read(chain_link, chain_link_header_len, io->io_ctx) < chain_link_header_len) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }

        if (ctx->u64) {
            next_page = sas_read4(&chain_link[0], ctx->bswap);
            next_page_pos = sas_read2(&chain_link[8], ctx->bswap);
            chain_link_len = sas_read2(&chain_link[10], ctx->bswap);
        } else {
            next_page = sas_read4(&chain_link[0], ctx->bswap);
            next_page_pos = sas_read2(&chain_link[4], ctx->bswap);
            chain_link_len = sas_read2(&chain_link[6], ctx->bswap);
        }

        buffer_len += chain_link_len;
    }

cleanup:
    if (outError)
        *outError = retval;

    return retval == READSTAT_OK ? buffer_len : -1;
}

static readstat_error_t sas7bcat_read_block(char *buffer, size_t buffer_len,
        int start_page, int start_page_pos, sas7bcat_ctx_t *ctx) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = ctx->io;
    int next_page = start_page;
    int next_page_pos = start_page_pos;
    int link_count = 0;

    int chain_link_len = 0;
    int buffer_offset = 0;

    char chain_link[32];
    int chain_link_header_len = 16;
    if (ctx->u64) {
        chain_link_header_len = 32;
    }

    while (next_page > 0 && next_page_pos > 0 && next_page <= ctx->page_count && link_count++ < ctx->page_count) {
        if (io->seek(ctx->header_size+(next_page-1)*ctx->page_size+next_page_pos, READSTAT_SEEK_SET, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            goto cleanup;
        }
        if (io->read(chain_link, chain_link_header_len, io->io_ctx) < chain_link_header_len) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }
        if (ctx->u64) {
            next_page = sas_read4(&chain_link[0], ctx->bswap);
            next_page_pos = sas_read2(&chain_link[8], ctx->bswap);
            chain_link_len = sas_read2(&chain_link[10], ctx->bswap);
        } else {
            next_page = sas_read4(&chain_link[0], ctx->bswap);
            next_page_pos = sas_read2(&chain_link[4], ctx->bswap);
            chain_link_len = sas_read2(&chain_link[6], ctx->bswap);
        }
        if (buffer_offset + chain_link_len > buffer_len) {
            retval = READSTAT_ERROR_PARSE;
            goto cleanup;
        }
        if (io->read(buffer + buffer_offset, chain_link_len, io->io_ctx) < chain_link_len) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }
        buffer_offset += chain_link_len;
    }
cleanup:

    return retval;
}

readstat_error_t readstat_parse_sas7bcat(readstat_parser_t *parser, const char *path, void *user_ctx) {
    readstat_error_t retval = READSTAT_OK;
    readstat_io_t *io = parser->io;
    int64_t i;
    char *page = NULL;
    char *buffer = NULL;

    sas7bcat_ctx_t *ctx = calloc(1, sizeof(sas7bcat_ctx_t));
    sas_header_info_t *hinfo = calloc(1, sizeof(sas_header_info_t));

    ctx->block_pointers = malloc((ctx->block_pointers_capacity = 200) * sizeof(uint64_t));

    ctx->value_label_handler = parser->handlers.value_label;
    ctx->metadata_handler = parser->handlers.metadata;
    ctx->input_encoding = parser->input_encoding;
    ctx->output_encoding = parser->output_encoding;
    ctx->user_ctx = user_ctx;
    ctx->io = io;

    if (io->open(path, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_OPEN;
        goto cleanup;
    }

    if ((retval = sas_read_header(io, hinfo, parser->handlers.error, user_ctx)) != READSTAT_OK) {
        goto cleanup;
    }

    ctx->u64 = hinfo->u64;
    ctx->pad1 = hinfo->pad1;
    ctx->bswap = machine_is_little_endian() ^ hinfo->little_endian;
    ctx->header_size = hinfo->header_size;
    ctx->page_count = hinfo->page_count;
    ctx->page_size = hinfo->page_size;
    if (ctx->input_encoding == NULL) {
        ctx->input_encoding = hinfo->encoding;
    }

    ctx->xlsr_size = 212 + ctx->pad1;
    ctx->xlsr_offset = 856 + 2 * ctx->pad1;
    ctx->xlsr_O_offset = 50 + ctx->pad1;
    if (ctx->u64) {
        ctx->xlsr_offset += 144;
        ctx->xlsr_size += 72;
        ctx->xlsr_O_offset += 24;
    }

    if (ctx->input_encoding && ctx->output_encoding && strcmp(ctx->input_encoding, ctx->output_encoding) != 0) {
        iconv_t converter = iconv_open(ctx->output_encoding, ctx->input_encoding);
        if (converter == (iconv_t)-1) {
            retval = READSTAT_ERROR_UNSUPPORTED_CHARSET;
            goto cleanup;
        }
        ctx->converter = converter;
    }

    if (ctx->metadata_handler) {
        char table_name[4*32+1];
        readstat_metadata_t metadata = { 
            .file_encoding = ctx->input_encoding, /* orig encoding? */
            .modified_time = hinfo->modification_time,
            .creation_time = hinfo->creation_time,
            .file_format_version = hinfo->major_version,
            .endianness = hinfo->little_endian ? READSTAT_ENDIAN_LITTLE : READSTAT_ENDIAN_BIG,
            .is64bit = ctx->u64
        };
        retval = readstat_convert(table_name, sizeof(table_name),
                hinfo->table_name, sizeof(hinfo->table_name), ctx->converter);
        if (retval != READSTAT_OK)
            goto cleanup;

        metadata.table_name = table_name;

        if (ctx->metadata_handler(&metadata, ctx->user_ctx) != READSTAT_HANDLER_OK) {
            retval = READSTAT_ERROR_USER_ABORT;
            goto cleanup;
        }
    }

    if ((page = readstat_malloc(ctx->page_size)) == NULL) {
        retval = READSTAT_ERROR_MALLOC;
        goto cleanup;
    }
    if (io->seek(ctx->header_size+SAS_CATALOG_FIRST_INDEX_PAGE*ctx->page_size, READSTAT_SEEK_SET, io->io_ctx) == -1) {
        retval = READSTAT_ERROR_SEEK;
        goto cleanup;
    }
    if (io->read(page, ctx->page_size, io->io_ctx) < ctx->page_size) {
        retval = READSTAT_ERROR_READ;
        goto cleanup;
    }

    retval = sas7bcat_augment_index(&page[ctx->xlsr_offset], ctx->page_size - ctx->xlsr_offset, ctx);
    if (retval != READSTAT_OK)
        goto cleanup;

    // Pass 1 -- find the XLSR entries
    for (i=SAS_CATALOG_USELESS_PAGES; i<ctx->page_count; i++) {
        if (io->seek(ctx->header_size+i*ctx->page_size, READSTAT_SEEK_SET, io->io_ctx) == -1) {
            retval = READSTAT_ERROR_SEEK;
            goto cleanup;
        }
        if (io->read(page, ctx->page_size, io->io_ctx) < ctx->page_size) {
            retval = READSTAT_ERROR_READ;
            goto cleanup;
        }
        if (memcmp(&page[16], "XLSR", sizeof("XLSR")-1) == 0) {
            retval = sas7bcat_augment_index(&page[16], ctx->page_size - 16, ctx);
            if (retval != READSTAT_OK)
                goto cleanup;
        }
    }

    sas7bcat_sort_index(ctx);
    sas7bcat_uniq_index(ctx);

    // Pass 2 -- look up the individual block pointers
    for (i=0; i<ctx->block_pointers_used; i++) {
        int start_page = ctx->block_pointers[i] >> 32;
        int start_page_pos = (ctx->block_pointers[i]) & 0xFFFF;

        int buffer_len = sas7bcat_block_size(start_page, start_page_pos, ctx, &retval);
        if (buffer_len == -1) {
            goto cleanup;
        } else if (buffer_len == 0) {
            continue;
        }
        if ((buffer = readstat_realloc(buffer, buffer_len)) == NULL) {
            retval = READSTAT_ERROR_MALLOC;
            goto cleanup;
        }
        if ((retval = sas7bcat_read_block(buffer, buffer_len, start_page, start_page_pos, ctx)) != READSTAT_OK)
            goto cleanup;
        if ((retval = sas7bcat_parse_block(buffer, buffer_len, ctx)) != READSTAT_OK)
            goto cleanup;
    }

cleanup:
    io->close(io->io_ctx);
    if (page)
        free(page);
    if (buffer)
        free(buffer);
    if (ctx)
        sas7bcat_ctx_free(ctx);
    if (hinfo)
        free(hinfo);

    return retval;
}
