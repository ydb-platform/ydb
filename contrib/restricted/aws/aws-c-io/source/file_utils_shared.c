/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/io/file_utils.h>

#include <aws/common/environment.h>
#include <aws/common/string.h>
#include <aws/io/logging.h>

#include <errno.h>
#include <stdio.h>

#ifdef _MSC_VER
#    pragma warning(disable : 4996) /* Disable warnings about fopen() being insecure */
#endif                              /* _MSC_VER */

int aws_byte_buf_init_from_file(struct aws_byte_buf *out_buf, struct aws_allocator *alloc, const char *filename) {
    AWS_ZERO_STRUCT(*out_buf);
    FILE *fp = fopen(filename, "rb");

    if (fp) {
        if (fseek(fp, 0L, SEEK_END)) {
            AWS_LOGF_ERROR(AWS_LS_IO_FILE_UTILS, "static: Failed to seek file %s with errno %d", filename, errno);
            fclose(fp);
            return aws_translate_and_raise_io_error(errno);
        }

        size_t allocation_size = (size_t)ftell(fp) + 1;
        /* Tell the user that we allocate here and if success they're responsible for the free. */
        if (aws_byte_buf_init(out_buf, alloc, allocation_size)) {
            fclose(fp);
            return AWS_OP_ERR;
        }

        /* Ensure compatibility with null-terminated APIs, but don't consider
         * the null terminator part of the length of the payload */
        out_buf->len = out_buf->capacity - 1;
        out_buf->buffer[out_buf->len] = 0;

        if (fseek(fp, 0L, SEEK_SET)) {
            AWS_LOGF_ERROR(AWS_LS_IO_FILE_UTILS, "static: Failed to seek file %s with errno %d", filename, errno);
            aws_byte_buf_clean_up(out_buf);
            fclose(fp);
            return aws_translate_and_raise_io_error(errno);
        }

        size_t read = fread(out_buf->buffer, 1, out_buf->len, fp);
        fclose(fp);
        if (read < out_buf->len) {
            AWS_LOGF_ERROR(AWS_LS_IO_FILE_UTILS, "static: Failed to read file %s with errno %d", filename, errno);
            aws_secure_zero(out_buf->buffer, out_buf->len);
            aws_byte_buf_clean_up(out_buf);
            return aws_raise_error(AWS_IO_FILE_VALIDATION_FAILURE);
        }

        return AWS_OP_SUCCESS;
    }

    AWS_LOGF_ERROR(AWS_LS_IO_FILE_UTILS, "static: Failed to open file %s with errno %d", filename, errno);

    return aws_translate_and_raise_io_error(errno);
}

bool aws_is_any_directory_separator(char value) {
    return value == '\\' || value == '/';
}
