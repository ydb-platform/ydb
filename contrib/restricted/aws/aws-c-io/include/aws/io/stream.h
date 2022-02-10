#ifndef AWS_IO_STREAM_H
#define AWS_IO_STREAM_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/io/io.h>

struct aws_input_stream;
struct aws_byte_buf;

/*
 * For seek calls, where in the stream to seek from.
 * CUR support can come later
 * Intentionally mirror libc constants
 */
enum aws_stream_seek_basis { AWS_SSB_BEGIN = 0, AWS_SSB_END = 2 };

struct aws_stream_status {
    bool is_end_of_stream;
    bool is_valid;
};

struct aws_input_stream_vtable {
    int (*seek)(struct aws_input_stream *stream, aws_off_t offset, enum aws_stream_seek_basis basis);
    /**
     * Stream as much data as will fit into the destination buffer and update its length.
     * The destination buffer's capacity MUST NOT be changed.
     *
     * Return AWS_OP_SUCCESS if the read is successful.
     * If AWS_OP_ERR is returned, the stream is assumed to be invalid and any data written to the buffer is ignored.
     *
     * If no more data is currently available, or the end of the stream has been reached, simply return AWS_OP_SUCCESS
     * without touching the destination buffer.
     */
    int (*read)(struct aws_input_stream *stream, struct aws_byte_buf *dest);
    int (*get_status)(struct aws_input_stream *stream, struct aws_stream_status *status);
    int (*get_length)(struct aws_input_stream *stream, int64_t *out_length);
    void (*destroy)(struct aws_input_stream *stream);
};

struct aws_input_stream {
    struct aws_allocator *allocator;
    void *impl;
    struct aws_input_stream_vtable *vtable;
};

AWS_EXTERN_C_BEGIN

/*
 * Seek to a position within a stream; analagous to fseek() and its relatives
 */
AWS_IO_API int aws_input_stream_seek(
    struct aws_input_stream *stream,
    aws_off_t offset,
    enum aws_stream_seek_basis basis);

/*
 * Read data from a stream.  If data is available, will read up to the (capacity - len) open bytes
 * in the destination buffer. If AWS_OP_ERR is returned, the destination buffer will be unchanged.
 */
AWS_IO_API int aws_input_stream_read(struct aws_input_stream *stream, struct aws_byte_buf *dest);

/*
 * Queries miscellaneous properties of the stream
 */
AWS_IO_API int aws_input_stream_get_status(struct aws_input_stream *stream, struct aws_stream_status *status);

/*
 * Returns the total stream length, if able, regardless of current stream position.  Under certain conditions,
 * a valid stream may return an error instead when there is not a good answer (socket stream, for example).
 *
 */
AWS_IO_API int aws_input_stream_get_length(struct aws_input_stream *stream, int64_t *out_length);

/*
 * Tears down the stream
 */
AWS_IO_API void aws_input_stream_destroy(struct aws_input_stream *stream);

/*
 * Creates a stream that operates on a range of bytes
 */
AWS_IO_API struct aws_input_stream *aws_input_stream_new_from_cursor(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *cursor);

/*
 * Creates a stream that operates on a (not-yet-opened) file.
 * Destruction closes the file.
 */
AWS_IO_API struct aws_input_stream *aws_input_stream_new_from_file(
    struct aws_allocator *allocator,
    const char *file_name);

/*
 * Creates an input stream that reads from an already opened file.
 * Destruction does not close the file.
 */
AWS_IO_API struct aws_input_stream *aws_input_stream_new_from_open_file(struct aws_allocator *allocator, FILE *file);

AWS_EXTERN_C_END

#endif /* AWS_IO_STREAM_H */
