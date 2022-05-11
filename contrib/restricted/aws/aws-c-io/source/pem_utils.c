/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/common/string.h>
#include <aws/io/private/pem_utils.h>

enum aws_pem_util_state {
    BEGIN,
    ON_DATA,
    END,
};

static const struct aws_byte_cursor begin_header = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("-----BEGIN");
static const struct aws_byte_cursor end_header = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("-----END");
static const struct aws_byte_cursor dashes = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("-----");

int aws_sanitize_pem(struct aws_byte_buf *pem, struct aws_allocator *allocator) {
    if (!pem->len) {
        /* reject files with no PEM data */
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }
    struct aws_byte_buf clean_pem_buf;
    if (aws_byte_buf_init(&clean_pem_buf, allocator, pem->len)) {
        return AWS_OP_ERR;
    }
    struct aws_byte_cursor pem_cursor = aws_byte_cursor_from_buf(pem);
    int state = BEGIN;

    for (size_t i = 0; i < pem_cursor.len; i++) {
        /* parse through the pem once */
        char current = *(pem_cursor.ptr + i);
        switch (state) {
            case BEGIN:
                if (current == '-') {
                    struct aws_byte_cursor compare_cursor = pem_cursor;
                    compare_cursor.len = begin_header.len;
                    compare_cursor.ptr += i;
                    if (aws_byte_cursor_eq(&compare_cursor, &begin_header)) {
                        state = ON_DATA;
                        i--;
                    }
                }
                break;
            case ON_DATA:
                /* start copying everything */
                if (current == '-') {
                    struct aws_byte_cursor compare_cursor = pem_cursor;
                    compare_cursor.len = end_header.len;
                    compare_cursor.ptr += i;
                    if (aws_byte_cursor_eq(&compare_cursor, &end_header)) {
                        /* Copy the end header string and start to search for the end part of a pem */
                        state = END;
                        aws_byte_buf_append(&clean_pem_buf, &end_header);
                        i += (end_header.len - 1);
                        break;
                    }
                }
                aws_byte_buf_append_byte_dynamic(&clean_pem_buf, (uint8_t)current);
                break;
            case END:
                if (current == '-') {
                    struct aws_byte_cursor compare_cursor = pem_cursor;
                    compare_cursor.len = dashes.len;
                    compare_cursor.ptr += i;
                    if (aws_byte_cursor_eq(&compare_cursor, &dashes)) {
                        /* End part of a pem, copy the last 5 dashes and a new line, then ignore everything before next
                         * begin header */
                        state = BEGIN;
                        aws_byte_buf_append(&clean_pem_buf, &dashes);
                        i += (dashes.len - 1);
                        aws_byte_buf_append_byte_dynamic(&clean_pem_buf, (uint8_t)'\n');
                        break;
                    }
                }
                aws_byte_buf_append_byte_dynamic(&clean_pem_buf, (uint8_t)current);
                break;
            default:
                break;
        }
    }

    if (clean_pem_buf.len == 0) {
        /* No valid data remains after sanitization. File might have been the wrong format */
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    struct aws_byte_cursor clean_pem_cursor = aws_byte_cursor_from_buf(&clean_pem_buf);
    aws_byte_buf_reset(pem, true);
    aws_byte_buf_append_dynamic(pem, &clean_pem_cursor);
    aws_byte_buf_clean_up(&clean_pem_buf);
    return AWS_OP_SUCCESS;

error:
    aws_byte_buf_clean_up(&clean_pem_buf);
    return AWS_OP_ERR;
}
