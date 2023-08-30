#pragma once

#include <library/cpp/yson_pull/cyson_enums.h>

#include <util/system/types.h>

#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct yson_string {
    const char* ptr;
    size_t length;
} yson_string;

typedef yson_input_stream_result (*yson_input_stream_func)(
    void* ctx,
    const char** ptr,
    size_t* length);

typedef yson_output_stream_result (*yson_output_stream_func)(
    void* ctx,
    const char* ptr,
    size_t length);

/* Abstract types */

typedef struct yson_input_stream yson_input_stream;
typedef struct yson_output_stream yson_output_stream;

typedef struct yson_reader yson_reader;
typedef struct yson_writer yson_writer;

/* Input stream */

yson_input_stream*
yson_input_stream_from_string(const char* ptr, size_t length);

yson_input_stream*
yson_input_stream_from_file(FILE* file, size_t buffer_size);

yson_input_stream*
yson_input_stream_from_fd(int fd, size_t buffer_size);

yson_input_stream*
yson_input_stream_new(void* ctx, yson_input_stream_func callback);

void yson_input_stream_delete(yson_input_stream* stream);

/* Output stream */

yson_output_stream*
yson_output_stream_from_file(FILE* file, size_t buffer_size);

yson_output_stream*
yson_output_stream_from_fd(int fd, size_t buffer_size);

yson_output_stream*
yson_output_stream_new(void* ctx, yson_output_stream_func callback, size_t buffer_size);

void yson_output_stream_delete(yson_output_stream* stream);

/* Reader */

yson_reader*
yson_reader_new(yson_input_stream* stream, yson_stream_type mode);

void yson_reader_delete(yson_reader* reader);

yson_event_type
yson_reader_get_next_event(yson_reader* reader);

const char*
yson_reader_get_error_message(yson_reader* reader);

yson_scalar_type
yson_reader_get_scalar_type(yson_reader* reader);

int yson_reader_get_boolean(yson_reader* reader);

i64 yson_reader_get_int64(yson_reader* reader);

ui64 yson_reader_get_uint64(yson_reader* reader);

double
yson_reader_get_float64(yson_reader* reader);

const yson_string*
yson_reader_get_string(yson_reader* reader);

/* Writer */

yson_writer*
yson_writer_new_binary(
    yson_output_stream* stream,
    yson_stream_type mode);

yson_writer*
yson_writer_new_text(
    yson_output_stream* stream,
    yson_stream_type mode);

yson_writer*
yson_writer_new_pretty_text(
    yson_output_stream* stream,
    yson_stream_type mode,
    size_t indent);

void yson_writer_delete(yson_writer* writer);

const char*
yson_writer_get_error_message(yson_writer* writer);

yson_writer_result
yson_writer_write_begin_stream(yson_writer* writer);

yson_writer_result
yson_writer_write_end_stream(yson_writer* writer);

yson_writer_result
yson_writer_write_begin_list(yson_writer* writer);

yson_writer_result
yson_writer_write_end_list(yson_writer* writer);

yson_writer_result
yson_writer_write_begin_map(yson_writer* writer);

yson_writer_result
yson_writer_write_end_map(yson_writer* writer);

yson_writer_result
yson_writer_write_begin_attributes(yson_writer* writer);

yson_writer_result
yson_writer_write_end_attributes(yson_writer* writer);

yson_writer_result
yson_writer_write_entity(yson_writer* writer);

yson_writer_result
yson_writer_write_key(yson_writer* writer, const char* ptr, size_t length);

yson_writer_result
yson_writer_write_string(yson_writer* writer, const char* ptr, size_t length);

yson_writer_result
yson_writer_write_int64(yson_writer* writer, i64 value);

yson_writer_result
yson_writer_write_uint64(yson_writer* writer, ui64 value);

yson_writer_result
yson_writer_write_boolean(yson_writer* writer, int value);

yson_writer_result
yson_writer_write_float64(yson_writer* writer, double value);

#ifdef __cplusplus
} /* extern "C" */
#endif
