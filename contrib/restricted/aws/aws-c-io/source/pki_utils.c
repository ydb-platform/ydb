/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/io/private/pki_utils.h>

#include <aws/common/encoding.h>

#include <aws/io/file_utils.h>
#include <aws/io/logging.h>

#include <ctype.h>
#include <errno.h>
#include <string.h>

enum PEM_PARSE_STATE {
    BEGIN,
    ON_DATA,
};

void aws_cert_chain_clean_up(struct aws_array_list *cert_chain) {
    for (size_t i = 0; i < aws_array_list_length(cert_chain); ++i) {
        struct aws_byte_buf *decoded_buffer_ptr = NULL;
        aws_array_list_get_at_ptr(cert_chain, (void **)&decoded_buffer_ptr, i);

        if (decoded_buffer_ptr) {
            aws_secure_zero(decoded_buffer_ptr->buffer, decoded_buffer_ptr->len);
            aws_byte_buf_clean_up(decoded_buffer_ptr);
        }
    }

    /* remember, we don't own it so we don't free it, just undo whatever mutations we've done at this point. */
    aws_array_list_clear(cert_chain);
}

static int s_convert_pem_to_raw_base64(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *pem,
    struct aws_array_list *cert_chain_or_key) {
    enum PEM_PARSE_STATE state = BEGIN;

    struct aws_byte_buf current_cert;
    const char *begin_header = "-----BEGIN";
    const char *end_header = "-----END";
    size_t begin_header_len = strlen(begin_header);
    size_t end_header_len = strlen(end_header);
    bool on_length_calc = true;

    struct aws_array_list split_buffers;
    if (aws_array_list_init_dynamic(&split_buffers, allocator, 16, sizeof(struct aws_byte_cursor))) {
        return AWS_OP_ERR;
    }

    if (aws_byte_cursor_split_on_char(pem, '\n', &split_buffers)) {
        aws_array_list_clean_up(&split_buffers);
        AWS_LOGF_ERROR(AWS_LS_IO_PKI, "static: Invalid PEM buffer: failed to split on newline");
        return AWS_OP_ERR;
    }

    size_t split_count = aws_array_list_length(&split_buffers);
    size_t i = 0;
    size_t index_of_current_cert_start = 0;
    size_t current_cert_len = 0;

    while (i < split_count) {
        struct aws_byte_cursor *current_cur_ptr = NULL;
        aws_array_list_get_at_ptr(&split_buffers, (void **)&current_cur_ptr, i);

        /* burn off the padding in the buffer first.
         * Worst case we'll only have to do this once per line in the buffer. */
        while (current_cur_ptr->len && aws_isspace(*current_cur_ptr->ptr)) {
            aws_byte_cursor_advance(current_cur_ptr, 1);
        }

        /* handle CRLF on Windows by burning '\r' off the end of the buffer */
        if (current_cur_ptr->len && (current_cur_ptr->ptr[current_cur_ptr->len - 1] == '\r')) {
            current_cur_ptr->len--;
        }

        switch (state) {
            case BEGIN:
                if (current_cur_ptr->len > begin_header_len &&
                    !strncmp((const char *)current_cur_ptr->ptr, begin_header, begin_header_len)) {
                    state = ON_DATA;
                    index_of_current_cert_start = i + 1;
                }
                ++i;
                break;
            /* this loops through the lines containing data twice. First to figure out the length, a second
             * time to actually copy the data. */
            case ON_DATA:
                /* Found end tag. */
                if (current_cur_ptr->len > end_header_len &&
                    !strncmp((const char *)current_cur_ptr->ptr, end_header, end_header_len)) {
                    if (on_length_calc) {
                        on_length_calc = false;
                        state = ON_DATA;
                        i = index_of_current_cert_start;

                        if (aws_byte_buf_init(&current_cert, allocator, current_cert_len)) {
                            goto end_of_loop;
                        }

                    } else {
                        if (aws_array_list_push_back(cert_chain_or_key, &current_cert)) {
                            aws_secure_zero(&current_cert.buffer, current_cert.len);
                            aws_byte_buf_clean_up(&current_cert);
                            goto end_of_loop;
                        }
                        state = BEGIN;
                        on_length_calc = true;
                        current_cert_len = 0;
                        ++i;
                    }
                    /* actually on a line with data in it. */
                } else {
                    if (!on_length_calc) {
                        aws_byte_buf_write(&current_cert, current_cur_ptr->ptr, current_cur_ptr->len);
                    } else {
                        current_cert_len += current_cur_ptr->len;
                    }
                    ++i;
                }
                break;
        }
    }

end_of_loop:
    aws_array_list_clean_up(&split_buffers);

    if (state == BEGIN && aws_array_list_length(cert_chain_or_key) > 0) {
        return AWS_OP_SUCCESS;
    }

    AWS_LOGF_ERROR(AWS_LS_IO_PKI, "static: Invalid PEM buffer.");
    aws_cert_chain_clean_up(cert_chain_or_key);
    return aws_raise_error(AWS_IO_FILE_VALIDATION_FAILURE);
}

int aws_decode_pem_to_buffer_list(
    struct aws_allocator *alloc,
    const struct aws_byte_cursor *pem_cursor,
    struct aws_array_list *cert_chain_or_key) {
    AWS_ASSERT(aws_array_list_length(cert_chain_or_key) == 0);
    struct aws_array_list base_64_buffer_list;

    if (aws_array_list_init_dynamic(&base_64_buffer_list, alloc, 2, sizeof(struct aws_byte_buf))) {
        return AWS_OP_ERR;
    }

    int err_code = AWS_OP_ERR;

    if (s_convert_pem_to_raw_base64(alloc, pem_cursor, &base_64_buffer_list)) {
        goto cleanup_base64_buffer_list;
    }

    for (size_t i = 0; i < aws_array_list_length(&base_64_buffer_list); ++i) {
        size_t decoded_len = 0;
        struct aws_byte_buf *byte_buf_ptr = NULL;
        aws_array_list_get_at_ptr(&base_64_buffer_list, (void **)&byte_buf_ptr, i);
        struct aws_byte_cursor byte_cur = aws_byte_cursor_from_buf(byte_buf_ptr);

        if (aws_base64_compute_decoded_len(&byte_cur, &decoded_len)) {
            aws_raise_error(AWS_IO_FILE_VALIDATION_FAILURE);
            goto cleanup_all;
        }

        struct aws_byte_buf decoded_buffer;
        if (aws_byte_buf_init(&decoded_buffer, alloc, decoded_len)) {
            goto cleanup_all;
        }

        if (aws_base64_decode(&byte_cur, &decoded_buffer)) {
            aws_raise_error(AWS_IO_FILE_VALIDATION_FAILURE);
            aws_byte_buf_clean_up_secure(&decoded_buffer);
            goto cleanup_all;
        }

        if (aws_array_list_push_back(cert_chain_or_key, &decoded_buffer)) {
            aws_byte_buf_clean_up_secure(&decoded_buffer);
            goto cleanup_all;
        }
    }

    err_code = AWS_OP_SUCCESS;

cleanup_all:
    if (err_code != AWS_OP_SUCCESS) {
        AWS_LOGF_ERROR(AWS_LS_IO_PKI, "static: Invalid PEM buffer.");
        aws_cert_chain_clean_up(cert_chain_or_key);
    }

cleanup_base64_buffer_list:
    aws_cert_chain_clean_up(&base_64_buffer_list);
    aws_array_list_clean_up(&base_64_buffer_list);

    return err_code;
}

int aws_read_and_decode_pem_file_to_buffer_list(
    struct aws_allocator *alloc,
    const char *filename,
    struct aws_array_list *cert_chain_or_key) {

    struct aws_byte_buf raw_file_buffer;
    if (aws_byte_buf_init_from_file(&raw_file_buffer, alloc, filename)) {
        AWS_LOGF_ERROR(AWS_LS_IO_PKI, "static: Failed to read file %s.", filename);
        return AWS_OP_ERR;
    }
    AWS_ASSERT(raw_file_buffer.buffer);

    struct aws_byte_cursor file_cursor = aws_byte_cursor_from_buf(&raw_file_buffer);
    if (aws_decode_pem_to_buffer_list(alloc, &file_cursor, cert_chain_or_key)) {
        aws_secure_zero(raw_file_buffer.buffer, raw_file_buffer.len);
        aws_byte_buf_clean_up(&raw_file_buffer);
        AWS_LOGF_ERROR(AWS_LS_IO_PKI, "static: Failed to decode PEM file %s.", filename);
        return AWS_OP_ERR;
    }

    aws_secure_zero(raw_file_buffer.buffer, raw_file_buffer.len);
    aws_byte_buf_clean_up(&raw_file_buffer);

    return AWS_OP_SUCCESS;
}
