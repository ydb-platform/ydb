#include <library/cpp/yson_pull/input.h>
#include <library/cpp/yson_pull/output.h>
#include <library/cpp/yson_pull/reader.h>
#include <library/cpp/yson_pull/writer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cerrno>
#include <cmath>

#ifdef _unix_
#include <unistd.h>
#include <sys/wait.h>
#endif

namespace {
    constexpr const char* alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    void generate(NYsonPull::TWriter& writer, size_t count) {
        writer.BeginStream();
        for (size_t i = 0; i < count; ++i) {
            writer.BeginMap()
                .Key("ints")
                .BeginList()
                .Int64(0)
                .Int64(-1)
                .Int64(1000)
                .Int64(-1000)
                .EndList()
                .Key("uints")
                .BeginList()
                .UInt64(0)
                .UInt64(1000)
                .UInt64(10000000)
                .EndList()
                .Key("entities")
                .BeginList()
                .Entity()
                .BeginAttributes()
                .Key("color")
                .String("blue")
                .Key("size")
                .Int64(100)
                .EndAttributes()
                .Entity()
                .Entity()
                .EndList()
                .Key("booleans")
                .BeginList()
                .Boolean(true)
                .Boolean(false)
                .Boolean(true)
                .EndList()
                .Key("floats")
                .BeginList()
                .Float64(0.0)
                .Float64(13.0e30)
                .Float64(M_PI)
                .EndList()
                .Key("strings")
                .BeginList()
                .String("hello")
                .String("")
                .String("foo \"-bar-\" baz")
                .String("oh\nwow")
                .String(alphabet)
                .EndList()
                .EndMap();
        }
        writer.EndStream();
    }

#ifdef __clang__
    // XXX: With all the macros below (esp. UNIT_ASSERT_VALUES_EQUAL) unfolded,
    // the time it takes clang to optimize generated code becomes abysmal.
    // Locally disabling optimization brings it back to normal.
    __attribute__((optnone))
#endif // __clang__
    void
    verify(NYsonPull::TReader& reader, size_t count) {
#define NEXT(name__) \
    {                \
        auto& name__ = reader.NextEvent(); // SCOPED_TRACE(e);
#define END_NEXT }
#define NEXT_TYPE(type__)                                                  \
    NEXT(e) {                                                              \
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::type__, e.Type()); \
    }                                                                      \
    END_NEXT
#define NEXT_KEY(key__)                                                 \
    NEXT(e) {                                                           \
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Key, e.Type()); \
        UNIT_ASSERT_VALUES_EQUAL(key__, e.AsString());                  \
    }                                                                   \
    END_NEXT
#define NEXT_SCALAR(type__, value__)                                                   \
    NEXT(e) {                                                                          \
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());             \
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EScalarType::type__, e.AsScalar().Type()); \
        UNIT_ASSERT_VALUES_EQUAL(value__, e.AsScalar().As##type__());                  \
    }                                                                                  \
    END_NEXT
#define NEXT_ENTITY()                                                                  \
    NEXT(e) {                                                                          \
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());             \
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EScalarType::Entity, e.AsScalar().Type()); \
    }                                                                                  \
    END_NEXT
#define NEXT_FLOAT64(value__)                                                           \
    NEXT(e) {                                                                           \
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());              \
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EScalarType::Float64, e.AsScalar().Type()); \
        UNIT_ASSERT_DOUBLES_EQUAL(value__, e.AsScalar().AsFloat64(), 1e-5);             \
    }                                                                                   \
    END_NEXT

        constexpr auto true_ = true;
        constexpr auto false_ = false;

        NEXT_TYPE(BeginStream);
        for (size_t i = 0; i < count; ++i) {
            NEXT_TYPE(BeginMap);
            NEXT_KEY("ints") {
                NEXT_TYPE(BeginList);
                NEXT_SCALAR(Int64, 0);
                NEXT_SCALAR(Int64, -1);
                NEXT_SCALAR(Int64, 1000);
                NEXT_SCALAR(Int64, -1000);
                NEXT_TYPE(EndList);
            }
            NEXT_KEY("uints") {
                NEXT_TYPE(BeginList);
                NEXT_SCALAR(UInt64, 0U);
                NEXT_SCALAR(UInt64, 1000U);
                NEXT_SCALAR(UInt64, 10000000U);
                NEXT_TYPE(EndList);
            }
            NEXT_KEY("entities") {
                NEXT_TYPE(BeginList);
                NEXT_ENTITY();
                NEXT_TYPE(BeginAttributes) {
                    NEXT_KEY("color") {
                        NEXT_SCALAR(String, "blue");
                    }
                    NEXT_KEY("size") {
                        NEXT_SCALAR(Int64, 100);
                    }
                }
                NEXT_TYPE(EndAttributes);
                NEXT_ENTITY();
                NEXT_ENTITY();
                NEXT_TYPE(EndList);
            }
            NEXT_KEY("booleans") {
                NEXT_TYPE(BeginList);
                NEXT_SCALAR(Boolean, true_);
                NEXT_SCALAR(Boolean, false_);
                NEXT_SCALAR(Boolean, true_);
                NEXT_TYPE(EndList);
            }
            NEXT_KEY("floats") {
                NEXT_TYPE(BeginList);
                NEXT_FLOAT64(0.0);
                NEXT_FLOAT64(13.0e30);
                NEXT_FLOAT64(M_PI);
                NEXT_TYPE(EndList);
            }
            NEXT_KEY("strings") {
                NEXT_TYPE(BeginList);
                NEXT_SCALAR(String, "hello");
                NEXT_SCALAR(String, "");
                NEXT_SCALAR(String, "foo \"-bar-\" baz");
                NEXT_SCALAR(String, "oh\nwow");
                NEXT_SCALAR(String, alphabet);
                NEXT_TYPE(EndList);
            }
            NEXT_TYPE(EndMap);
        }
        NEXT_TYPE(EndStream);

#undef NEXT
#undef END_NEXT
#undef NEXT_TYPE
#undef NEXT_KEY
#undef NEXT_SCALAR
    }

    class sys_error {};

    IOutputStream& operator<<(IOutputStream& stream, const sys_error&) {
        stream << strerror(errno);
        return stream;
    }

    NYsonPull::TReader make_reader(THolder<NYsonPull::NInput::IStream> stream) {
        return NYsonPull::TReader(
            std::move(stream),
            NYsonPull::EStreamType::ListFragment);
    }

    template <typename Function>
    void test_memory(Function make_writer, size_t nrepeat) {
        TString text;
        {
            auto writer = make_writer(NYsonPull::NOutput::FromString(&text));
            generate(writer, nrepeat);
        }
        {
            auto reader = make_reader(NYsonPull::NInput::FromMemory(text));
            verify(reader, nrepeat);
        }
        {
            TStringInput input(text);
            auto reader = make_reader(NYsonPull::NInput::FromInputStream(&input, /* buffer_size = */ 1));
            verify(reader, nrepeat);
        }
    }

#ifdef _unix_
    template <typename Here, typename There>
    void pipe(Here&& reader, There&& writer) {
        int fildes[2];
        UNIT_ASSERT_VALUES_EQUAL_C(0, ::pipe(fildes), sys_error());
        auto read_fd = fildes[0];
        auto write_fd = fildes[1];

        auto pid = ::fork();
        UNIT_ASSERT_C(pid >= 0, sys_error());
        if (pid > 0) {
            // parent
            UNIT_ASSERT_VALUES_EQUAL_C(0, ::close(write_fd), sys_error());
            reader(read_fd);
            UNIT_ASSERT_VALUES_EQUAL_C(0, ::close(read_fd), sys_error());
        } else {
            // child
            UNIT_ASSERT_VALUES_EQUAL_C(0, ::close(read_fd), sys_error());
            UNIT_ASSERT_NO_EXCEPTION(writer(write_fd));
            UNIT_ASSERT_VALUES_EQUAL_C(0, ::close(write_fd), sys_error());
            ::exit(0);
        }
        int stat_loc;
        UNIT_ASSERT_VALUES_EQUAL_C(pid, ::waitpid(pid, &stat_loc, 0), sys_error());
    }

    template <typename Function>
    void test_posix_fd(
        Function make_writer,
        size_t nrepeat,
        size_t read_buffer_size,
        size_t write_buffer_size) {
        pipe(
            [&](int fd) {
                auto reader = make_reader(NYsonPull::NInput::FromPosixFd(fd, read_buffer_size));
                verify(reader, nrepeat);
            },
            [&](int fd) {
                auto writer = make_writer(NYsonPull::NOutput::FromPosixFd(fd, write_buffer_size));
                generate(writer, nrepeat);
            });
    }

    template <typename Function>
    void test_stdio_file(
        Function make_writer,
        size_t nrepeat,
        size_t read_buffer_size,
        size_t write_buffer_size) {
        pipe(
            [&](int fd) {
                auto file = ::fdopen(fd, "rb");
                UNIT_ASSERT_C(file != nullptr, sys_error());
                auto reader = make_reader(NYsonPull::NInput::FromStdioFile(file, read_buffer_size));
                verify(reader, nrepeat);
            },
            [&](int fd) {
                auto file = ::fdopen(fd, "wb");
                Y_UNUSED(write_buffer_size);
                auto writer = make_writer(NYsonPull::NOutput::FromStdioFile(file, write_buffer_size));
                generate(writer, nrepeat);
                fflush(file);
            });
    }
#endif

    NYsonPull::TWriter text(THolder<NYsonPull::NOutput::IStream> stream) {
        return NYsonPull::MakeTextWriter(
            std::move(stream),
            NYsonPull::EStreamType::ListFragment);
    }

    NYsonPull::TWriter pretty_text(THolder<NYsonPull::NOutput::IStream> stream) {
        return NYsonPull::MakePrettyTextWriter(
            std::move(stream),
            NYsonPull::EStreamType::ListFragment);
    }

    NYsonPull::TWriter binary(THolder<NYsonPull::NOutput::IStream> stream) {
        return NYsonPull::MakeBinaryWriter(
            std::move(stream),
            NYsonPull::EStreamType::ListFragment);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(Loop) {
    Y_UNIT_TEST(memory_pretty_text) {
        test_memory(pretty_text, 100);
    }

    Y_UNIT_TEST(memory_text) {
        test_memory(text, 100);
    }

    Y_UNIT_TEST(memory_binary) {
        test_memory(binary, 100);
    }

#ifdef _unix_
    Y_UNIT_TEST(posix_fd_pretty_text_buffered) {
        test_posix_fd(pretty_text, 100, 1024, 1024);
    }

    Y_UNIT_TEST(posix_fd_pretty_text_unbuffered) {
        test_posix_fd(pretty_text, 100, 1, 0);
    }

    Y_UNIT_TEST(posix_fd_text_buffered) {
        test_posix_fd(text, 100, 1024, 1024);
    }

    Y_UNIT_TEST(posix_fd_text_unbuffered) {
        test_posix_fd(text, 100, 1, 0);
    }

    Y_UNIT_TEST(posix_fd_binary_buffered) {
        test_posix_fd(binary, 100, 1024, 1024);
    }

    Y_UNIT_TEST(posix_fd_binary_unbuffered) {
        test_posix_fd(binary, 100, 1, 0);
    }

    Y_UNIT_TEST(stdio_file_pretty_text_buffered) {
        test_stdio_file(pretty_text, 100, 1024, 1024);
    }

    Y_UNIT_TEST(stdio_file_pretty_text_unbuffered) {
        test_stdio_file(pretty_text, 100, 1, 0);
    }

    Y_UNIT_TEST(stdio_file_text_buffered) {
        test_stdio_file(text, 100, 1024, 1024);
    }

    Y_UNIT_TEST(stdio_file_text_unbuffered) {
        test_stdio_file(text, 100, 1, 0);
    }

    Y_UNIT_TEST(stdio_file_binary_buffered) {
        test_stdio_file(binary, 100, 1024, 1024);
    }

    Y_UNIT_TEST(stdio_file_binary_unbuffered) {
        test_stdio_file(binary, 100, 1, 0);
    }
#endif
} // Y_UNIT_TEST_SUITE(Loop)
