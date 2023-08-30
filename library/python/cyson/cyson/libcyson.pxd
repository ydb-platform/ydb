from libc.stddef cimport size_t
from libc.stdint cimport uint64_t, int64_t
from libc.stdio  cimport FILE

cdef extern from "<library/c/cyson/cyson.h>":
    struct yson_input_stream:
        pass
    struct yson_output_stream:
        pass

    struct yson_reader:
        pass
    struct yson_writer:
        pass

    struct yson_string:
        const char* ptr
        size_t length

    enum yson_event_type:
        YSON_EVENT_BEGIN_STREAM
        YSON_EVENT_END_STREAM
        YSON_EVENT_BEGIN_LIST
        YSON_EVENT_END_LIST
        YSON_EVENT_BEGIN_MAP
        YSON_EVENT_END_MAP
        YSON_EVENT_BEGIN_ATTRIBUTES
        YSON_EVENT_END_ATTRIBUTES
        YSON_EVENT_KEY
        YSON_EVENT_SCALAR
        YSON_EVENT_ERROR

    enum yson_scalar_type:
        YSON_SCALAR_ENTITY
        YSON_SCALAR_BOOLEAN
        YSON_SCALAR_INT64
        YSON_SCALAR_UINT64
        YSON_SCALAR_FLOAT64
        YSON_SCALAR_STRING

    enum yson_input_stream_result:
        YSON_INPUT_STREAM_RESULT_OK
        YSON_INPUT_STREAM_RESULT_EOF
        YSON_INPUT_STREAM_RESULT_ERROR

    enum yson_output_stream_result:
        YSON_OUTPUT_STREAM_RESULT_OK
        YSON_OUTPUT_STREAM_RESULT_ERROR

    enum yson_writer_result:
        YSON_WRITER_RESULT_OK
        YSON_WRITER_RESULT_BAD_STREAM
        YSON_WRITER_RESULT_ERROR

    enum yson_stream_type:
        YSON_STREAM_TYPE_NODE
        YSON_STREAM_TYPE_LIST_FRAGMENT
        YSON_STREAM_TYPE_MAP_FRAGMENT

    ctypedef yson_input_stream_result (*yson_input_stream_func)(
        void* ctx,
        const char** ptr,
        size_t* length) except YSON_INPUT_STREAM_RESULT_ERROR

    ctypedef yson_output_stream_result (*yson_output_stream_func)(
        void* ctx,
        const char* ptr,
        size_t length) except YSON_OUTPUT_STREAM_RESULT_ERROR

    yson_input_stream* yson_input_stream_from_string(const char* ptr, size_t length)
    yson_input_stream* yson_input_stream_from_file(FILE* file, size_t buffer_size)
    yson_input_stream* yson_input_stream_from_fd(int fd, size_t buffer_size)
    yson_input_stream* yson_input_stream_new(void* ctx, yson_input_stream_func callback);
    void yson_input_stream_delete(yson_input_stream* stream)

    yson_output_stream* yson_output_stream_from_file(FILE* file, size_t buffer_size)
    yson_output_stream* yson_output_stream_from_fd(int fd, size_t buffer_size)
    yson_output_stream* yson_output_stream_new(void* ctx, yson_output_stream_func callback, size_t buffer_size);
    void yson_output_stream_delete(yson_output_stream* stream)

    yson_reader* yson_reader_new(yson_input_stream* stream, yson_stream_type mode)
    void yson_reader_delete(yson_reader* reader)
    const char* yson_reader_get_error_message(yson_reader* reader)

    yson_event_type yson_reader_get_next_event(yson_reader* reader) except? YSON_EVENT_ERROR
    yson_scalar_type yson_reader_get_scalar_type(yson_reader* reader)

    bint yson_reader_get_boolean(yson_reader* reader)
    int64_t yson_reader_get_int64(yson_reader* reader)
    uint64_t yson_reader_get_uint64(yson_reader* reader)
    double yson_reader_get_float64(yson_reader* reader)
    const yson_string* yson_reader_get_string(yson_reader* reader)

    yson_writer* yson_writer_new_binary(yson_output_stream* stream, yson_stream_type mode)
    yson_writer* yson_writer_new_text(yson_output_stream* stream, yson_stream_type mode)
    yson_writer* yson_writer_new_pretty_text(yson_output_stream* stream, yson_stream_type mode, size_t indent)
    void yson_writer_delete(yson_writer* writer)
    const char* yson_writer_get_error_message(yson_writer* writer)

    yson_writer_result yson_writer_write_begin_stream(yson_writer* writer)
    yson_writer_result yson_writer_write_end_stream(yson_writer* writer)
    yson_writer_result yson_writer_write_begin_list(yson_writer* writer)
    yson_writer_result yson_writer_write_end_list(yson_writer* writer)
    yson_writer_result yson_writer_write_begin_map(yson_writer* writer)
    yson_writer_result yson_writer_write_end_map(yson_writer* writer)
    yson_writer_result yson_writer_write_begin_attributes(yson_writer* writer)
    yson_writer_result yson_writer_write_end_attributes(yson_writer* writer)

    yson_writer_result yson_writer_write_entity(yson_writer* writer)
    yson_writer_result yson_writer_write_key(yson_writer* writer, const char* ptr, size_t length)
    yson_writer_result yson_writer_write_string(yson_writer* writer, const char* ptr, size_t length)
    yson_writer_result yson_writer_write_int64(yson_writer* writer, int64_t value)
    yson_writer_result yson_writer_write_uint64(yson_writer* writer, uint64_t value)
    yson_writer_result yson_writer_write_boolean(yson_writer* writer, int value)
    yson_writer_result yson_writer_write_float64(yson_writer* writer, double value)

