#ifndef AWS_IO_FILE_UTILS_H
#define AWS_IO_FILE_UTILS_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/io/io.h>

AWS_EXTERN_C_BEGIN

/**
 * Reads 'filename' into 'out_buf'. If successful, 'out_buf' is allocated and filled with the data;
 * It is your responsibility to call 'aws_byte_buf_clean_up()' on it. Otherwise, 'out_buf' remains
 * unused. In the very unfortunate case where some API needs to treat out_buf as a c_string, a null terminator
 * is appended, but is not included as part of the length field.
 */
AWS_IO_API int aws_byte_buf_init_from_file(
    struct aws_byte_buf *out_buf,
    struct aws_allocator *alloc,
    const char *filename);

/**
 * Returns true iff the character is a directory separator on ANY supported platform.
 */
AWS_IO_API
bool aws_is_any_directory_separator(char value);

/**
 * Returns the directory separator used by the local platform
 */
AWS_IO_API
char aws_get_platform_directory_separator(void);

/**
 * Returns the current user's home directory.
 */
AWS_IO_API
struct aws_string *aws_get_home_directory(struct aws_allocator *allocator);

/**
 * Returns true if a file or path exists, otherwise, false.
 */
AWS_IO_API
bool aws_path_exists(const char *path);

/*
 * Wrapper for highest-resolution platform-dependent seek implementation.
 * Maps to:
 *
 *   _fseeki64() on windows
 *   fseeko() on linux
 *
 * whence can either be SEEK_SET or SEEK_END
 */
AWS_IO_API
int aws_fseek(FILE *file, aws_off_t offset, int whence);

/*
 * Wrapper for os-specific file length query.  We can't use fseek(END, 0)
 * because support for it is not technically required.
 *
 * Unix flavors call fstat, while Windows variants use GetFileSize on a
 * HANDLE queried from the libc FILE pointer.
 */
AWS_IO_API
int aws_file_get_length(FILE *file, int64_t *length);

AWS_EXTERN_C_END

#endif /* AWS_IO_FILE_UTILS_H */
