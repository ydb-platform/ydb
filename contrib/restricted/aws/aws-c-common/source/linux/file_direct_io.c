/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#define _GNU_SOURCE /* NOLINT(bugprone-reserved-identifier) */
/* O_DIRECT is defined with _GNU_SOURCE on Linux */

#include <aws/common/environment.h>
#include <aws/common/file.h>
#include <aws/common/logging.h>
#include <aws/common/string.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pwd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

/* O_DIRECT is not available on all platforms */
#ifndef O_DIRECT
#    define O_DIRECT 0
#endif

/*
 * Maximum chunk size for reading files with direct I/O.
 * On Linux, read() (and similar system calls) will transfer at most
 * 0x7ffff000 (2,147,479,552) bytes, returning the number of bytes
 * actually transferred. (This is true on both 32-bit and 64-bit systems.)
 */
static const size_t AWS_FILE_MAX_READ_CHUNK = 0x7ffff000;

int aws_file_path_read_from_offset_direct_io_with_chunk_size(
    const struct aws_string *file_path,
    uint64_t offset,
    size_t max_read_length,
    size_t max_chunk_size,
    struct aws_byte_buf *output_buf,
    size_t *out_actual_read) {
    if (max_chunk_size > AWS_FILE_MAX_READ_CHUNK) {
        /* Make sure it is less than the max. */
        max_chunk_size = AWS_FILE_MAX_READ_CHUNK;
    }

    if (O_DIRECT == 0) {
        AWS_LOGF_ERROR(AWS_LS_COMMON_GENERAL, "O_DIRECT is not supported on this platform");
        return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
    }
    size_t available_len = aws_sub_size_saturating(output_buf->capacity, output_buf->len);
    size_t length = aws_min_size(available_len, max_read_length);
    if (length == 0) {
        return AWS_OP_SUCCESS; /* Nothing to do. */
    }

    int rt_code = AWS_OP_ERR;
    int fd = open(aws_string_c_str(file_path), O_RDONLY | O_DIRECT);
    if (fd == -1) {
        int errno_value = errno; /* Always cache errno before potential side-effect */
        AWS_LOGF_ERROR(
            AWS_LS_COMMON_GENERAL,
            "Failed to open file %s for reading with O_DIRECT, errno: %d",
            aws_string_c_str(file_path),
            errno_value);
        aws_translate_and_raise_io_error(errno_value);
        goto cleanup;
    }

    /* seek to the right position and then read */
    if (lseek(fd, (off_t)offset, SEEK_SET) == -1) {
        int errno_value = errno; /* Always cache errno before potential side-effect */
        AWS_LOGF_ERROR(
            AWS_LS_COMMON_GENERAL,
            "Failed to seek to position %llu in file %s, errno: %d",
            (unsigned long long)offset,
            aws_string_c_str(file_path),
            errno_value);
        aws_translate_and_raise_io_error(errno_value);
        goto cleanup;
    }

    /* Read in chunks to handle the Linux read() limitation */
    size_t total_bytes_read = 0;
    size_t remaining_length = length;
    uint8_t *current_buffer = output_buf->buffer + output_buf->len;

    while (remaining_length > 0) {
        size_t chunk_size = aws_min_size(remaining_length, max_chunk_size);

        ssize_t bytes_read = read(fd, current_buffer, chunk_size);
        if (bytes_read == -1) {
            int errno_value = errno; /* Always cache errno before potential side-effect */
            AWS_LOGF_ERROR(
                AWS_LS_COMMON_GENERAL,
                "Failed to read %zu bytes from file %s, errno: %d",
                chunk_size,
                aws_string_c_str(file_path),
                errno_value);
            aws_translate_and_raise_io_error(errno_value);
            goto cleanup;
        }

        if (bytes_read == 0) {
            /* End of file reached */
            break;
        }

        total_bytes_read += (size_t)bytes_read;
        current_buffer += bytes_read;
        remaining_length -= (size_t)bytes_read;

        /* If we read less than requested, we've reached the end of file */
        if ((size_t)bytes_read < chunk_size) {
            break;
        }
    }

    *out_actual_read = total_bytes_read;
    output_buf->len += total_bytes_read;
    rt_code = AWS_OP_SUCCESS;
cleanup:
    if (fd != -1) {
        close(fd);
    }
    return rt_code;
}

int aws_file_path_read_from_offset_direct_io(
    const struct aws_string *file_path,
    uint64_t offset,
    size_t max_read_length,
    struct aws_byte_buf *output_buf,
    size_t *out_actual_read) {

    if (O_DIRECT == 0) {
        AWS_LOGF_ERROR(AWS_LS_COMMON_GENERAL, "O_DIRECT is not supported on this platform");
        return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
    }

    return aws_file_path_read_from_offset_direct_io_with_chunk_size(
        file_path, offset, max_read_length, AWS_FILE_MAX_READ_CHUNK, output_buf, out_actual_read);
}
