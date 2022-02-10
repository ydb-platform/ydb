#include <library/cpp/yson_pull/exceptions.h>
#include <library/cpp/yson_pull/range.h>
#include <library/cpp/yson_pull/reader.h>
#include <library/cpp/yson_pull/detail/cescape.h>
#include <library/cpp/yson_pull/detail/macros.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {
    NYsonPull::TReader memory_reader(TStringBuf data, NYsonPull::EStreamType mode) {
        return NYsonPull::TReader(
            NYsonPull::NInput::FromMemory(data),
            mode);
    }

    template <typename T>
    void expect_scalar(const NYsonPull::TScalar& scalar, T value) {
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::TScalar{value}, scalar);
    }

    template <>
    void expect_scalar(const NYsonPull::TScalar& scalar, double value) {
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EScalarType::Float64, scalar.Type());

        auto scalarValue = scalar.AsFloat64();
        auto message = TStringBuilder() << "expected " << value << ", got " << scalarValue;

        if (std::isfinite(value)) {
            UNIT_ASSERT_C(std::isfinite(scalarValue), message);
            UNIT_ASSERT_DOUBLES_EQUAL(value, scalarValue, 1e-5);
        } else if (std::isnan(value)) {
            UNIT_ASSERT_C(std::isnan(scalarValue), message);
        } else if (value > 0) {
            UNIT_ASSERT_C(std::isinf(scalarValue) && (scalarValue > 0), message);
        } else {
            UNIT_ASSERT_C(std::isinf(scalarValue) && (scalarValue < 0), message);
        }
    }

    template <typename T>
    void test_scalar(TStringBuf data, T value) {
        // SCOPED_TRACE(NYsonPull::detail::cescape::quote(data));
        auto reader = memory_reader(data, NYsonPull::EStreamType::Node);

        try {
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::BeginStream, reader.NextEvent().Type());
            {
                auto& event = reader.NextEvent();
                UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, event.Type());
                expect_scalar(event.AsScalar(), value);
            }
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::EndStream, reader.NextEvent().Type());
        } catch (const std::exception& err) {
            UNIT_FAIL(err.what());
        }
    }

    void consume(TStringBuf data, NYsonPull::EStreamType mode = NYsonPull::EStreamType::Node) {
        // SCOPED_TRACE(NYsonPull::detail::cescape::quote(data));
        auto input_range = NYsonPull::TStreamEventsRange(
            NYsonPull::NInput::FromMemory(data),
            mode);
        for (auto& event : input_range) {
            Y_UNUSED(event);
        }
    }

#define ACCEPT(data) UNIT_ASSERT_NO_EXCEPTION(consume(data))
#define REJECT(data) UNIT_ASSERT_EXCEPTION(consume(data), NYsonPull::NException::TBadInput)

#define ACCEPT2(data, mode) UNIT_ASSERT_NO_EXCEPTION(consume(data, mode))
#define REJECT2(data, mode) UNIT_ASSERT_EXCEPTION(consume(data, mode), NYsonPull::NException::TBadInput)

} // anonymous namespace

Y_UNIT_TEST_SUITE(Reader) {
    Y_UNIT_TEST(ScalarEntity) {
        test_scalar(TStringBuf("#"), NYsonPull::TScalar{});
    }

    Y_UNIT_TEST(ScalarBoolean) {
        test_scalar(TStringBuf("%true"), true);
        test_scalar(TStringBuf("%false"), false);

        test_scalar(TStringBuf("\x05"sv), true);
        test_scalar(TStringBuf("\x04"sv), false);

        REJECT("%");
        REJECT("%trueth");
        REJECT("%tru");
        REJECT("%falseth");
        REJECT("%fals");
        REJECT("%hithere");
    }

    Y_UNIT_TEST(ScalarInt64) {
        test_scalar(TStringBuf("1"), i64{1});
        test_scalar(TStringBuf("+1"), i64{1});
        test_scalar(TStringBuf("100000"), i64{100000});
        test_scalar(TStringBuf("+100000"), i64{100000});
        test_scalar(TStringBuf("-100000"), i64{-100000});
        test_scalar(TStringBuf("9223372036854775807"), i64{9223372036854775807});
        test_scalar(TStringBuf("+9223372036854775807"), i64{9223372036854775807});

        test_scalar(TStringBuf("\x02\x02"sv), i64{1});
        test_scalar(TStringBuf("\x02\xc0\x9a\x0c"sv), i64{100000});
        test_scalar(TStringBuf("\x02\xbf\x9a\x0c"sv), i64{-100000});
        test_scalar(TStringBuf("\x02\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01"sv), i64{9223372036854775807});

        REJECT("1a2");
        REJECT("1-1-1-1");
        REJECT("1+0");
    }

    Y_UNIT_TEST(SclarUInt64) {
        test_scalar(TStringBuf("1u"), ui64{1});
        test_scalar(TStringBuf("+1u"), ui64{1});
        test_scalar(TStringBuf("100000u"), ui64{100000});
        test_scalar(TStringBuf("+100000u"), ui64{100000});
        test_scalar(TStringBuf("9223372036854775807u"), ui64{9223372036854775807u});
        test_scalar(TStringBuf("+9223372036854775807u"), ui64{9223372036854775807u});
        test_scalar(TStringBuf("18446744073709551615u"), ui64{18446744073709551615u});
        test_scalar(TStringBuf("+18446744073709551615u"), ui64{18446744073709551615u});

        REJECT("1a2u");
        REJECT("1-1-1-1u");
        REJECT("1+0u");

        // TODO: binary
    }

    Y_UNIT_TEST(ScalarFloat64) {
        test_scalar(TStringBuf("0.0"), double{0.0});
        test_scalar(TStringBuf("+0.0"), double{0.0});
        test_scalar(TStringBuf("+.0"), double{0.0});
        test_scalar(TStringBuf("+.5"), double{0.5});
        test_scalar(TStringBuf("-.5"), double{-0.5});
        test_scalar(TStringBuf("1.0"), double{1.0});
        test_scalar(TStringBuf("+1.0"), double{1.0});
        test_scalar(TStringBuf("-1.0"), double{-1.0});
        test_scalar(TStringBuf("1000.0"), double{1000.0});
        test_scalar(TStringBuf("+1000.0"), double{1000.0});
        test_scalar(TStringBuf("-1000.0"), double{-1000.0});
        test_scalar(TStringBuf("1e12"), double{1e12});
        test_scalar(TStringBuf("1e+12"), double{1e12});
        test_scalar(TStringBuf("+1e+12"), double{1e12});
        test_scalar(TStringBuf("-1e+12"), double{-1e12});
        test_scalar(TStringBuf("1e-12"), double{1e-12});
        test_scalar(TStringBuf("+1e-12"), double{1e-12});
        test_scalar(TStringBuf("-1e-12"), double{-1e-12});

        test_scalar(TStringBuf("\x03\x00\x00\x00\x00\x00\x00\x00\x00"sv), double{0.0});

        test_scalar(
            TStringBuf("\x03\x00\x00\x00\x00\x00\x00\xf8\x7f"sv),
            double{std::numeric_limits<double>::quiet_NaN()});
        test_scalar(
            TStringBuf("\x03\x00\x00\x00\x00\x00\x00\xf0\x7f"sv),
            double{std::numeric_limits<double>::infinity()});
        test_scalar(
            TStringBuf("\x03\x00\x00\x00\x00\x00\x00\xf0\xff"sv),
            double{-std::numeric_limits<double>::infinity()});

        test_scalar(
            TStringBuf("%nan"),
            double{std::numeric_limits<double>::quiet_NaN()});
        test_scalar(
            TStringBuf("%inf"),
            double{std::numeric_limits<double>::infinity()});
        test_scalar(
            TStringBuf("%-inf"),
            double{-std::numeric_limits<double>::infinity()});

        REJECT("++0.0");
        REJECT("++1.0");
        REJECT("++.1");
        REJECT("1.0.0");
        //REJECT("1e+10000");
        REJECT(TStringBuf("\x03\x00\x00\x00\x00\x00\x00\x00"sv));

        // XXX: Questionable behaviour?
        ACCEPT("+.0");
        ACCEPT("-.0");
        // XXX: Rejected on Mac OS, accepted on Linux (?!)
        //REJECT(".0");
        //REJECT(".5");

        REJECT("%NaN");
        REJECT("%+inf");
        REJECT("%infinity");
        REJECT("%na");
        REJECT("%in");
        REJECT("%-in");
    }

    Y_UNIT_TEST(ScalarString) {
        test_scalar(TStringBuf(R"(foobar)"), TStringBuf("foobar"));
        test_scalar(TStringBuf(R"(foobar11)"), TStringBuf("foobar11"));
        test_scalar(TStringBuf(R"("foobar")"), TStringBuf("foobar"));
        // wat? "\x0cf" parsed as a single char? no way!
        test_scalar("\x01\x0c" "foobar"sv,
                    TStringBuf("foobar"));

        REJECT(R"("foobar)");
        REJECT("\x01\x0c" "fooba"sv);
        REJECT("\x01\x0d" "foobar"sv); // negative length
    }

    Y_UNIT_TEST(EmptyList) {
        auto reader = memory_reader("[]", NYsonPull::EStreamType::Node);

        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::BeginStream, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::BeginList, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::EndList, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::EndStream, reader.NextEvent().Type());

        REJECT("[");
        REJECT("]");
    }

    Y_UNIT_TEST(EmptyMap) {
        auto reader = memory_reader("{}", NYsonPull::EStreamType::Node);

        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::BeginStream, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::BeginMap, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::EndMap, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::EndStream, reader.NextEvent().Type());

        REJECT("{");
        REJECT("}");
    }

    Y_UNIT_TEST(Sample) {
        auto reader = memory_reader(
            R"({"11"=11;"nothing"=#;"zero"=0.;"foo"="bar";"list"=[1;2;3]})",
            NYsonPull::EStreamType::Node);

        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::BeginStream, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::BeginMap, reader.NextEvent().Type());

        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Key, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf("11"), e.AsString());
        }
        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::TScalar{i64{11}}, e.AsScalar());
        }

        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Key, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf("nothing"), e.AsString());
        }
        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::TScalar{}, e.AsScalar());
        }

        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Key, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf("zero"), e.AsString());
        }
        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::TScalar{0.0}, e.AsScalar());
        }

        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Key, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf("foo"), e.AsString());
        }
        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::TScalar{TStringBuf("bar")}, e.AsScalar());
        }

        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Key, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf("list"), e.AsString());
        }
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::BeginList, reader.NextEvent().Type());
        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::TScalar{i64{1}}, e.AsScalar());
        }
        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::TScalar{i64{2}}, e.AsScalar());
        }
        {
            auto& e = reader.NextEvent();
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, e.Type());
            UNIT_ASSERT_VALUES_EQUAL(NYsonPull::TScalar{i64{3}}, e.AsScalar());
        }
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::EndList, reader.NextEvent().Type());

        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::EndMap, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::EndStream, reader.NextEvent().Type());
    }

    Y_UNIT_TEST(Accept) {
        ACCEPT("[]");
        ACCEPT("{}");
        ACCEPT("<>[]");
        ACCEPT("<>{}");
        ACCEPT("[{};{};{}]");
        ACCEPT("[{};{};{};]");
        ACCEPT("[<>{};<>{};<>{}]");
        ACCEPT("[<>{};<>{};<>{};]");

        ACCEPT("foo");
        ACCEPT("[foo]");
        ACCEPT("[foo;]");
        ACCEPT("{foo=foo}");
        ACCEPT("{foo=foo;}");
        ACCEPT("<>{foo=foo}");
        ACCEPT("{foo=<foo=foo>foo}");
        ACCEPT("{foo=<foo=foo;>foo}");
        ACCEPT("{foo=<foo=foo>[foo;foo]}");
    }

    Y_UNIT_TEST(Reject) {
        REJECT("[");
        REJECT("{");
        REJECT("<");

        REJECT("[[}]");
        REJECT("<>{]");
        REJECT("[>]");

        REJECT("<><>[]");
        REJECT("[<>;<>]");

        REJECT("{<>foo=foo}");
        REJECT("{foo=<>}");
        REJECT("{foo}");

        REJECT("<a=b>");
        REJECT("<>");

        REJECT("@");
    }

    Y_UNIT_TEST(ReadPastEnd) {
        auto reader = memory_reader("#", NYsonPull::EStreamType::Node);
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::BeginStream, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::Scalar, reader.NextEvent().Type());
        UNIT_ASSERT_VALUES_EQUAL(NYsonPull::EEventType::EndStream, reader.NextEvent().Type());
        UNIT_ASSERT_EXCEPTION(reader.NextEvent(), NYsonPull::NException::TBadInput);
    }

    Y_UNIT_TEST(BadInput) {
        // max_size<ui32> < varint size < max_size<ui64>
        auto t = TString("\x01\xff\xff\xff\xff\xff\xff\xff\xff");
        auto reader = memory_reader(t, NYsonPull::EStreamType::Node);

        UNIT_ASSERT_EQUAL(reader.NextEvent().Type(), NYsonPull::EEventType::BeginStream);
        UNIT_ASSERT_EXCEPTION(reader.NextEvent(), NYsonPull::NException::TBadInput);
    }

    Y_UNIT_TEST(StreamType) {
        REJECT2("", NYsonPull::EStreamType::Node);
        ACCEPT2("", NYsonPull::EStreamType::ListFragment);
        ACCEPT2("", NYsonPull::EStreamType::MapFragment);

        ACCEPT2("[1]", NYsonPull::EStreamType::Node);
        ACCEPT2("[1]", NYsonPull::EStreamType::ListFragment);
        REJECT2("[1]", NYsonPull::EStreamType::MapFragment);

        ACCEPT2("<foo=bar>[1]", NYsonPull::EStreamType::Node);
        ACCEPT2("<foo=bar>[1]", NYsonPull::EStreamType::ListFragment);
        REJECT2("<foo=bar>[1]", NYsonPull::EStreamType::MapFragment);

        ACCEPT2("           [1]   \t \t    ", NYsonPull::EStreamType::Node);
        ACCEPT2("           [1]   \t \t    ", NYsonPull::EStreamType::ListFragment);
        REJECT2("           [1]   \t \t    ", NYsonPull::EStreamType::MapFragment);

        REJECT2("[1];", NYsonPull::EStreamType::Node);
        ACCEPT2("[1];", NYsonPull::EStreamType::ListFragment);
        REJECT2("[1];", NYsonPull::EStreamType::MapFragment);

        REJECT2("[1]; foobar", NYsonPull::EStreamType::Node);
        ACCEPT2("[1]; foobar", NYsonPull::EStreamType::ListFragment);
        REJECT2("[1]; foobar", NYsonPull::EStreamType::MapFragment);

        REJECT2("a=[1]", NYsonPull::EStreamType::Node);
        REJECT2("a=[1]", NYsonPull::EStreamType::ListFragment);
        ACCEPT2("a=[1]", NYsonPull::EStreamType::MapFragment);

        REJECT2("a=[1]; ", NYsonPull::EStreamType::Node);
        REJECT2("a=[1]; ", NYsonPull::EStreamType::ListFragment);
        ACCEPT2("a=[1]; ", NYsonPull::EStreamType::MapFragment);

        REJECT2("a=[1]; b=foobar", NYsonPull::EStreamType::Node);
        REJECT2("a=[1]; b=foobar", NYsonPull::EStreamType::ListFragment);
        ACCEPT2("a=[1]; b=foobar", NYsonPull::EStreamType::MapFragment);
    }

} // Y_UNIT_TEST_SUITE(Reader)
