// Export visible API

#include "cyson.h"

#include <library/cpp/yson_pull/yson.h>
#include <library/cpp/yson_pull/detail/reader.h>
#include <library/cpp/yson_pull/detail/writer.h>
#include <library/cpp/yson_pull/detail/input/stream.h>
#include <library/cpp/yson_pull/detail/input/stdio_file.h>
#include <library/cpp/yson_pull/detail/output/buffered.h>
#include <library/cpp/yson_pull/detail/output/stream.h>
#include <library/cpp/yson_pull/detail/output/stdio_file.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace {
    template <typename T>
    void safe_assign_string(TString& dest, T&& value) noexcept {
        try {
            dest = std::forward<T>(value);
        } catch (...) {
            // Suppress exception
        }
    }

} // anonymous namespace

struct yson_reader {
    NYsonPull::NDetail::gen_reader_impl<false> impl;
    TString error_message;

    yson_reader(NYsonPull::NInput::IStream* stream, NYsonPull::EStreamType mode)
        : impl(*stream, mode)
    {
        error_message.reserve(64);
    }

    yson_event_type safe_get_next_event() noexcept {
        try {
            auto& event = impl.next_event();
            return static_cast<yson_event_type>(event.Type());
        } catch (...) {
            safe_assign_string(error_message, CurrentExceptionMessage());
            return YSON_EVENT_ERROR;
        }
    }
};

struct yson_writer {
    THolder<NYsonPull::IConsumer> consumer;
    TString error_message;

    yson_writer(THolder<NYsonPull::IConsumer> consumer_)
        : consumer{std::move(consumer_)} {
        error_message.reserve(64);
    }

    template <typename T>
    yson_writer_result safe_write(T&& func) noexcept {
        try {
            func(*consumer);
            return YSON_WRITER_RESULT_OK;
        } catch (const NYsonPull::NException::TBadOutput& err) {
            safe_assign_string(error_message, err.what());
            return YSON_WRITER_RESULT_BAD_STREAM;
        } catch (...) {
            safe_assign_string(error_message, CurrentExceptionMessage());
            return YSON_WRITER_RESULT_ERROR;
        }
    }
};

namespace {
    class callback_error: public std::exception {
    public:
        const char* what() const noexcept override {
            return "User callback returned error result code";
        }
    };

    class c_yson_input_stream: public NYsonPull::NInput::IStream {
        void* ctx_;
        yson_input_stream_func callback_;

    public:
        c_yson_input_stream(void* ctx, yson_input_stream_func callback)
            : ctx_{ctx}
            , callback_{callback} {
        }

    protected:
        result do_fill_buffer() override {
            const char* ptr;
            size_t length;
            switch (callback_(ctx_, &ptr, &length)) {
                case YSON_INPUT_STREAM_RESULT_OK:
                    buffer().reset(
                        reinterpret_cast<const ui8*>(ptr),
                        reinterpret_cast<const ui8*>(ptr) + length);
                    return result::have_more_data;

                case YSON_INPUT_STREAM_RESULT_EOF:
                    return result::at_end;

                default:
                case YSON_INPUT_STREAM_RESULT_ERROR:
                    throw callback_error();
            }
        }
    };

    class c_yson_output_stream: public NYsonPull::NDetail::NOutput::TBuffered<c_yson_output_stream> {
        using base_type = NYsonPull::NDetail::NOutput::TBuffered<c_yson_output_stream>;

        void* ctx_;
        yson_output_stream_func callback_;

    public:
        c_yson_output_stream(void* ctx, yson_output_stream_func callback, size_t buffer_size)
            : base_type(buffer_size)
            , ctx_{ctx}
            , callback_{callback} {
        }

        void write(TStringBuf data) {
            switch (callback_(ctx_, data.data(), data.size())) {
                case YSON_OUTPUT_STREAM_RESULT_OK:
                    return;

                default:
                case YSON_OUTPUT_STREAM_RESULT_ERROR:
                    throw callback_error();
            }
        }
    };

    // Type marshalling

    const yson_string* to_yson_string(const NYsonPull::TScalar& value) {
        assert(value.Type() == NYsonPull::EScalarType::String);
        auto* result = &value.AsUnsafeValue().AsString;
        return reinterpret_cast<const yson_string*>(result);
    }

    yson_input_stream* to_yson_input_stream(NYsonPull::NInput::IStream* ptr) {
        return reinterpret_cast<yson_input_stream*>(ptr);
    }

    NYsonPull::NInput::IStream* from_yson_input_stream(yson_input_stream* ptr) {
        return reinterpret_cast<NYsonPull::NInput::IStream*>(ptr);
    }

    yson_output_stream* to_yson_output_stream(NYsonPull::NOutput::IStream* ptr) {
        return reinterpret_cast<yson_output_stream*>(ptr);
    }

    NYsonPull::NOutput::IStream* from_yson_output_stream(yson_output_stream* ptr) {
        return reinterpret_cast<NYsonPull::NOutput::IStream*>(ptr);
    }

    // Exception-handling new/delete wrappers

    template <typename T, typename... Args>
    T* safe_new(Args&&... args) noexcept {
        try {
            return new T(std::forward<Args>(args)...);
        } catch (...) {
            return nullptr;
        }
    }

    template <typename T>
    void safe_delete(T* ptr) noexcept {
        assert(ptr != nullptr);
        try {
            delete ptr;
        } catch (...) {
            // Suppress destructor exceptions
        }
    }

    template <typename T, typename... Args>
    yson_writer* safe_new_writer(yson_output_stream* stream, Args&&... args) noexcept {
        try {
            auto impl = MakeHolder<T>(
                *from_yson_output_stream(stream),
                std::forward<Args>(args)...);
            return new yson_writer(std::move(impl));
        } catch (...) {
            return nullptr;
        }
    }

} // anonymous namespace

extern "C" {
// Input stream

yson_input_stream* yson_input_stream_from_string(const char* ptr, size_t length) {
    auto buf = TStringBuf{ptr, length};
    auto* result = safe_new<NYsonPull::NDetail::NInput::TOwned<TMemoryInput>>(buf);
    return to_yson_input_stream(result);
}

yson_input_stream* yson_input_stream_from_file(FILE* file, size_t buffer_size) {
    auto* result = safe_new<NYsonPull::NDetail::NInput::TStdioFile>(file, buffer_size);
    return to_yson_input_stream(result);
}

yson_input_stream* yson_input_stream_from_fd(int fd, size_t buffer_size) {
    auto* result = safe_new<NYsonPull::NDetail::NInput::TFHandle>(fd, buffer_size);
    return to_yson_input_stream(result);
}

yson_input_stream* yson_input_stream_new(void* ctx, yson_input_stream_func callback) {
    auto* result = safe_new<c_yson_input_stream>(ctx, callback);
    return to_yson_input_stream(result);
}

void yson_input_stream_delete(yson_input_stream* stream) {
    assert(stream != nullptr);
    safe_delete(from_yson_input_stream(stream));
}

// Reader

yson_reader* yson_reader_new(yson_input_stream* stream, yson_stream_type mode) {
    assert(stream != nullptr);
    return safe_new<yson_reader>(
        from_yson_input_stream(stream),
        static_cast<NYsonPull::EStreamType>(mode));
}

void yson_reader_delete(yson_reader* reader) {
    assert(reader != nullptr);
    safe_delete(reader);
}

yson_event_type yson_reader_get_next_event(yson_reader* reader) {
    assert(reader != nullptr);
    return reader->safe_get_next_event();
}

const char* yson_reader_get_error_message(yson_reader* reader) {
    assert(reader != nullptr);
    return reader->error_message.c_str();
}

yson_scalar_type yson_reader_get_scalar_type(yson_reader* reader) {
    assert(reader != nullptr);
    auto& event = reader->impl.last_event();
    return static_cast<yson_scalar_type>(event.AsScalar().Type());
}

int yson_reader_get_boolean(yson_reader* reader) {
    assert(reader != nullptr);
    auto& event = reader->impl.last_event();
    return static_cast<int>(event.AsScalar().AsBoolean());
}

i64 yson_reader_get_int64(yson_reader* reader) {
    assert(reader != nullptr);
    auto& event = reader->impl.last_event();
    return event.AsScalar().AsInt64();
}

ui64 yson_reader_get_uint64(yson_reader* reader) {
    assert(reader != nullptr);
    auto& event = reader->impl.last_event();
    return event.AsScalar().AsUInt64();
}

double yson_reader_get_float64(yson_reader* reader) {
    assert(reader != nullptr);
    auto& event = reader->impl.last_event();
    return event.AsScalar().AsFloat64();
}

const yson_string* yson_reader_get_string(yson_reader* reader) {
    assert(reader != nullptr);
    return to_yson_string(reader->impl.last_event().AsScalar());
}

// Output stream

yson_output_stream* yson_output_stream_from_file(FILE* file, size_t buffer_size) {
    auto* result = safe_new<NYsonPull::NDetail::NOutput::TStdioFile>(file, buffer_size);
    return to_yson_output_stream(result);
}

yson_output_stream* yson_output_stream_from_fd(int fd, size_t buffer_size) {
    auto* result = safe_new<NYsonPull::NDetail::NOutput::TFHandle>(fd, buffer_size);
    return to_yson_output_stream(result);
}

yson_output_stream* yson_output_stream_new(void* ctx, yson_output_stream_func callback, size_t buffer_size) {
    auto* result = safe_new<c_yson_output_stream>(ctx, callback, buffer_size);
    return to_yson_output_stream(result);
}

void yson_output_stream_delete(yson_output_stream* stream) {
    assert(stream != nullptr);
    safe_delete(from_yson_output_stream(stream));
}

// Writer

yson_writer* yson_writer_new_binary(yson_output_stream* stream, yson_stream_type mode) {
    assert(stream != nullptr);
    return safe_new_writer<NYsonPull::NDetail::TBinaryWriterImpl>(
        stream,
        static_cast<NYsonPull::EStreamType>(mode));
}

yson_writer* yson_writer_new_text(yson_output_stream* stream, yson_stream_type mode) {
    assert(stream != nullptr);
    return safe_new_writer<NYsonPull::NDetail::TTextWriterImpl>(
        stream,
        static_cast<NYsonPull::EStreamType>(mode));
}

yson_writer* yson_writer_new_pretty_text(yson_output_stream* stream, yson_stream_type mode, size_t indent) {
    assert(stream != nullptr);
    return safe_new_writer<NYsonPull::NDetail::TPrettyWriterImpl>(
        stream,
        static_cast<NYsonPull::EStreamType>(mode),
        indent);
}

void yson_writer_delete(yson_writer* writer) {
    assert(writer != nullptr);
    safe_delete(writer);
}

const char* yson_writer_get_error_message(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->error_message.c_str();
}

yson_writer_result yson_writer_write_begin_stream(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnBeginStream();
    });
}

yson_writer_result yson_writer_write_end_stream(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnEndStream();
    });
}

yson_writer_result yson_writer_write_begin_list(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnBeginList();
    });
}

yson_writer_result yson_writer_write_end_list(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnEndList();
    });
}

yson_writer_result yson_writer_write_begin_map(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnBeginMap();
    });
}

yson_writer_result yson_writer_write_end_map(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnEndMap();
    });
}

yson_writer_result yson_writer_write_begin_attributes(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnBeginAttributes();
    });
}

yson_writer_result yson_writer_write_end_attributes(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnEndAttributes();
    });
}

yson_writer_result yson_writer_write_entity(yson_writer* writer) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnEntity();
    });
}

yson_writer_result yson_writer_write_key(yson_writer* writer, const char* ptr, size_t length) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnKey({ptr, length});
    });
}

yson_writer_result yson_writer_write_string(yson_writer* writer, const char* ptr, size_t length) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnScalarString({ptr, length});
    });
}

yson_writer_result yson_writer_write_int64(yson_writer* writer, i64 value) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnScalarInt64(value);
    });
}

yson_writer_result yson_writer_write_uint64(yson_writer* writer, ui64 value) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnScalarUInt64(value);
    });
}

yson_writer_result yson_writer_write_boolean(yson_writer* writer, int value) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnScalarBoolean(static_cast<bool>(value));
    });
}

yson_writer_result yson_writer_write_float64(yson_writer* writer, double value) {
    assert(writer != nullptr);
    return writer->safe_write([=](NYsonPull::IConsumer& consumer) {
        consumer.OnScalarFloat64(value);
    });
}

} // extern "C"
