
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iconv.h>

#include "../readstat.h"
#include "../readstat_writer.h"
#include "readstat_sas.h"
#include "readstat_sas_rle.h"

typedef struct sas7bcat_block_s {
    size_t  len;
    char    data[1]; // Flexible array; use [1] for C++-98 compatibility
} sas7bcat_block_t;

static sas7bcat_block_t *sas7bcat_block_for_label_set(readstat_label_set_t *r_label_set) {
    size_t len = 0;
    size_t name_len = strlen(r_label_set->name);
    int j;
    char name[32];

    len += 106;

    if (name_len > 8) {
        len += 32; // long name
        if (name_len > 32) {
            name_len = 32;
        }
    }

    memcpy(&name[0], r_label_set->name, name_len);

    for (j=0; j<r_label_set->value_labels_count; j++) {
        readstat_value_label_t *value_label = readstat_get_value_label(r_label_set, j);
        len += 30; // Value: 14-byte header + 16-byte padded value

        len += 8 + 2 + value_label->label_len + 1;
    }

    sas7bcat_block_t *block = calloc(1, sizeof(sas7bcat_block_t) + len);
    block->len = len;

    off_t begin = 106;
    int32_t count = r_label_set->value_labels_count;
    memcpy(&block->data[38], &count, sizeof(int32_t));
    memcpy(&block->data[42], &count, sizeof(int32_t));
    if (name_len > 8) {
        block->data[2] = (char)0x80;
        memcpy(&block->data[8], name, 8);

        memset(&block->data[106], ' ', 32);
        memcpy(&block->data[106], name, name_len);

        begin += 32;
    } else {
        memset(&block->data[8], ' ', 8);
        memcpy(&block->data[8], name, name_len);
    }

    char *lbp1 = &block->data[begin];
    char *lbp2 = &block->data[begin+r_label_set->value_labels_count*30];

    for (j=0; j<r_label_set->value_labels_count; j++) {
        readstat_value_label_t *value_label = readstat_get_value_label(r_label_set, j);
        lbp1[2] = 24; // size - 6
        int32_t index = j;
        memcpy(&lbp1[10], &index, sizeof(int32_t));
        if (r_label_set->type == READSTAT_TYPE_STRING) {
            size_t string_len = value_label->string_key_len;
            if (string_len > 16)
                string_len = 16;
            memset(&lbp1[14], ' ', 16);
            memcpy(&lbp1[14], value_label->string_key, string_len);
        } else {
            uint64_t big_endian_value;
            double double_value = -1.0 * value_label->double_key;
            memcpy(&big_endian_value, &double_value, sizeof(double));
            if (machine_is_little_endian()) {
                big_endian_value = byteswap8(big_endian_value);
            }
            memcpy(&lbp1[22], &big_endian_value, sizeof(uint64_t));
        }

        int16_t label_len = value_label->label_len;
        memcpy(&lbp2[8], &label_len, sizeof(int16_t));
        memcpy(&lbp2[10], value_label->label, label_len);

        lbp1 += 30;
        lbp2 += 8 + 2 + value_label->label_len + 1;
    }

    return block;
}

static readstat_error_t sas7bcat_emit_header(readstat_writer_t *writer, sas_header_info_t *hinfo) {
    sas_header_start_t header_start = {
        .a2 = hinfo->u64 ? SAS_ALIGNMENT_OFFSET_4 : SAS_ALIGNMENT_OFFSET_0,
        .a1 = SAS_ALIGNMENT_OFFSET_0,
        .endian = machine_is_little_endian() ? SAS_ENDIAN_LITTLE : SAS_ENDIAN_BIG,
        .file_format = SAS_FILE_FORMAT_UNIX,
        .encoding = 20, /* UTF-8 */
        .file_type = "SAS FILE",
        .file_info = "CATALOG "
    };

    memcpy(&header_start.magic, sas7bcat_magic_number, sizeof(header_start.magic));

    return sas_write_header(writer, hinfo, header_start);
}

static readstat_error_t sas7bcat_begin_data(void *writer_ctx) {
    readstat_writer_t *writer = (readstat_writer_t *)writer_ctx;
    readstat_error_t retval = READSTAT_OK;

    int i;
    sas_header_info_t *hinfo = sas_header_info_init(writer, 0);
    sas7bcat_block_t **blocks = malloc(writer->label_sets_count * sizeof(sas7bcat_block_t));
    char *page = malloc(hinfo->page_size);

    for (i=0; i<writer->label_sets_count; i++) {
        blocks[i] = sas7bcat_block_for_label_set(writer->label_sets[i]);
    }

    hinfo->page_count = 4;

    // Header
    retval = sas7bcat_emit_header(writer, hinfo);
    if (retval != READSTAT_OK)
        goto cleanup;

    // Page 0
    retval = readstat_write_zeros(writer, hinfo->page_size);
    if (retval != READSTAT_OK)
        goto cleanup;

    memset(page, '\0', hinfo->page_size);

    // Page 1
    char *xlsr = &page[856];
    int16_t block_idx, block_off;
    block_idx = 4;
    block_off = 16;
    for (i=0; i<writer->label_sets_count; i++) {
        if (xlsr + 212 > page + hinfo->page_size)
            break;

        memcpy(&xlsr[0], "XLSR", 4);

        memcpy(&xlsr[4], &block_idx, sizeof(int16_t));
        memcpy(&xlsr[8], &block_off, sizeof(int16_t));

        xlsr[50] = 'O';

        block_off += blocks[i]->len;

        xlsr += 212;
    }

    retval = readstat_write_bytes(writer, page, hinfo->page_size);
    if (retval != READSTAT_OK)
        goto cleanup;

    // Page 2
    retval = readstat_write_zeros(writer, hinfo->page_size);
    if (retval != READSTAT_OK)
        goto cleanup;

    // Page 3
    memset(page, '\0', hinfo->page_size);

    char block_header[16];
    block_off = 16;
    for (i=0; i<writer->label_sets_count; i++) {
        if (block_off + sizeof(block_header) + blocks[i]->len > hinfo->page_size)
            break;

        memset(block_header, '\0', sizeof(block_header));

        int32_t next_page = 0;
        int16_t next_off = 0;
        int16_t block_len = blocks[i]->len;
        memcpy(&block_header[0], &next_page, sizeof(int32_t));
        memcpy(&block_header[4], &next_off, sizeof(int16_t));
        memcpy(&block_header[6], &block_len, sizeof(int16_t));

        memcpy(&page[block_off], block_header, sizeof(block_header));
        block_off += sizeof(block_header);

        memcpy(&page[block_off], blocks[i]->data, blocks[i]->len);
        block_off += blocks[i]->len;
    }

    retval = readstat_write_bytes(writer, page, hinfo->page_size);
    if (retval != READSTAT_OK)
        goto cleanup;

cleanup:
    for (i=0; i<writer->label_sets_count; i++) {
        free(blocks[i]);
    }
    free(blocks);
    free(hinfo);
    free(page);

    return retval;
}

readstat_error_t readstat_begin_writing_sas7bcat(readstat_writer_t *writer, void *user_ctx) {

    if (writer->version == 0)
        writer->version = SAS_DEFAULT_FILE_VERSION;

    writer->callbacks.begin_data = &sas7bcat_begin_data;

    return readstat_begin_writing_file(writer, user_ctx, 0);
}
