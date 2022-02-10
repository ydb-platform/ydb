#pragma once

typedef enum yson_event_type {
    YSON_EVENT_BEGIN_STREAM = 0,
    YSON_EVENT_END_STREAM = 1,
    YSON_EVENT_BEGIN_LIST = 2,
    YSON_EVENT_END_LIST = 3,
    YSON_EVENT_BEGIN_MAP = 4,
    YSON_EVENT_END_MAP = 5,
    YSON_EVENT_BEGIN_ATTRIBUTES = 6,
    YSON_EVENT_END_ATTRIBUTES = 7,
    YSON_EVENT_KEY = 8,
    YSON_EVENT_SCALAR = 9,
    YSON_EVENT_ERROR = 10
} yson_event_type;

typedef enum yson_scalar_type {
    YSON_SCALAR_ENTITY = 0,
    YSON_SCALAR_BOOLEAN = 1,
    YSON_SCALAR_INT64 = 2,
    YSON_SCALAR_UINT64 = 3,
    YSON_SCALAR_FLOAT64 = 4,
    YSON_SCALAR_STRING = 5
} yson_scalar_type;

typedef enum yson_input_stream_result {
    YSON_INPUT_STREAM_RESULT_OK = 0,
    YSON_INPUT_STREAM_RESULT_EOF = 1,
    YSON_INPUT_STREAM_RESULT_ERROR = 2
} yson_input_stream_result;

typedef enum yson_output_stream_result {
    YSON_OUTPUT_STREAM_RESULT_OK = 0,
    YSON_OUTPUT_STREAM_RESULT_ERROR = 1
} yson_output_stream_result;

typedef enum yson_writer_result {
    YSON_WRITER_RESULT_OK = 0,
    YSON_WRITER_RESULT_BAD_STREAM = 1,
    YSON_WRITER_RESULT_ERROR = 2
} yson_writer_result;

typedef enum yson_stream_type {
    YSON_STREAM_TYPE_NODE = 0,
    YSON_STREAM_TYPE_LIST_FRAGMENT = 1,
    YSON_STREAM_TYPE_MAP_FRAGMENT = 2
} yson_stream_type;
