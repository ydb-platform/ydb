#include "ydb_json_value.h"

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <library/cpp/json/json_reader.h>

namespace NYdb {

Y_UNIT_TEST_SUITE(JsonValueTest) {
    Y_UNIT_TEST(PrimitiveValueBool) {
        TValue value = TValueBuilder()
            .Bool(true)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "true");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueInt8) {
        TValue value = TValueBuilder()
            .Int8(-128)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "-128");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueUint8) {
        TValue value = TValueBuilder()
            .Uint8(255)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "255");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueInt16) {
        TValue value = TValueBuilder()
            .Int16(-32768)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "-32768");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueUint16) {
        TValue value = TValueBuilder()
            .Uint16(65535)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "65535");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueInt32) {
        TValue value = TValueBuilder()
            .Int32(-2147483648)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "-2147483648");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueUint32) {
        TValue value = TValueBuilder()
            .Uint32(4294967295)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "4294967295");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueInt64) {
        TValue value = TValueBuilder()
            .Int64(-9223372036854775807 - 1)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "-9223372036854775808");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueUint64) {
        TValue value = TValueBuilder()
            .Uint64(18446744073709551615ull)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "18446744073709551615");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueFloat) {
        TValue value = TValueBuilder()
            .Float(0.1234567890123456789)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "0.12345679");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueDouble) {
        TValue value = TValueBuilder()
            .Double(0.1234567890123456789)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "0.12345678901234568");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueDate) {
        TInstant timestamp = TInstant::ParseIso8601("2000-01-02");
        TValue value = TValueBuilder()
            .Date(TInstant::Days(timestamp.Days()))
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"("2000-01-02")");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueDatetime) {
        TInstant timestamp = TInstant::ParseIso8601("2000-01-02T03:04:05Z");
        TValue value = TValueBuilder()
            .Datetime(TInstant::Seconds(timestamp.Seconds()))
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"("2000-01-02T03:04:05Z")");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueTimestamp) {
        TInstant timestamp = TInstant::ParseIso8601("2000-01-02T03:04:05.678901Z");
        TValue value = TValueBuilder()
            .Timestamp(TInstant::MicroSeconds(timestamp.MicroSeconds()))
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"("2000-01-02T03:04:05.678901Z")");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueDate32) {
        TValue value = TValueBuilder()
            .Date32(-10)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"("1969-12-22")");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueDatetime64) {
        TValue value = TValueBuilder()
            .Datetime64(-10)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"("1969-12-31T23:59:50Z")");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueTimestamp64) {
        TValue value = TValueBuilder()
            .Timestamp64(-10)
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"("1969-12-31T23:59:59.999990Z")");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueSimpleString) {
        TValue value = TValueBuilder()
            .String("Escape characters: \" \\ \f \b \t \r\nNon-escaped characters: / ' < > & []() ")
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            jsonString,
            "\"Escape characters: \\\" " R"(\\ \f \b \t \r\nNon-escaped characters: / ' < > & []() ")"
        );
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    namespace {
        TString GenerateBinaryString() {
            TStringStream str;
            for (ui8 i = 0; i < 255; ++i) {
                str << static_cast<unsigned char>(i);
            }
            str << static_cast<unsigned char>(0xff);
            return str.Str();
        }
    }

    Y_UNIT_TEST(BinaryStringAsciiFollowedByNonAscii) {
        TString binaryString = TStringBuilder() << "abc" << static_cast<unsigned char>(0xff)
            << static_cast<unsigned char>(0xfe);
        TValue value = TValueBuilder()
            .String(binaryString)
            .Build();
        TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            jsonString,
            R"("abc\u00FF\u00FE")"
        );
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(BinaryStringUnicode) {
        TString binaryString = GenerateBinaryString();
        TValue value = TValueBuilder()
            .String(binaryString)
            .Build();
        TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        TStringBuilder from0To255Utf8;
        from0To255Utf8 << "\"";
        from0To255Utf8 <<
            R"(\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000B\f\r\u000E\u000F)"
            R"(\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F)"
            " !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\\u007F"
            R"(\u0080\u0081\u0082\u0083\u0084\u0085\u0086\u0087\u0088\u0089\u008A\u008B\u008C\u008D\u008E\u008F)"
            R"(\u0090\u0091\u0092\u0093\u0094\u0095\u0096\u0097\u0098\u0099\u009A\u009B\u009C\u009D\u009E\u009F)"
            R"(\u00A0\u00A1\u00A2\u00A3\u00A4\u00A5\u00A6\u00A7\u00A8\u00A9\u00AA\u00AB\u00AC\u00AD\u00AE\u00AF)"
            R"(\u00B0\u00B1\u00B2\u00B3\u00B4\u00B5\u00B6\u00B7\u00B8\u00B9\u00BA\u00BB\u00BC\u00BD\u00BE\u00BF)"
            R"(\u00C0\u00C1\u00C2\u00C3\u00C4\u00C5\u00C6\u00C7\u00C8\u00C9\u00CA\u00CB\u00CC\u00CD\u00CE\u00CF)"
            R"(\u00D0\u00D1\u00D2\u00D3\u00D4\u00D5\u00D6\u00D7\u00D8\u00D9\u00DA\u00DB\u00DC\u00DD\u00DE\u00DF)"
            R"(\u00E0\u00E1\u00E2\u00E3\u00E4\u00E5\u00E6\u00E7\u00E8\u00E9\u00EA\u00EB\u00EC\u00ED\u00EE\u00EF)"
            R"(\u00F0\u00F1\u00F2\u00F3\u00F4\u00F5\u00F6\u00F7\u00F8\u00F9\u00FA\u00FB\u00FC\u00FD\u00FE\u00FF)";
        from0To255Utf8 << "\"";
        UNIT_ASSERT_NO_DIFF(
            jsonString,
            from0To255Utf8
        );

        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(BinaryStringBase64) {
        TString binaryString = GenerateBinaryString();
        TValue value = TValueBuilder()
            .String(binaryString)
            .Build();
        TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Base64);
        UNIT_ASSERT_NO_DIFF(
            jsonString,
            R"("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHS)"
            R"(ElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJG)"
            R"(Sk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna2)"
            R"(9zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==")"
        );
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Base64);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(EmptyBinaryStringUnicode) {
        TString binaryString;
        TValue value = TValueBuilder()
            .String(binaryString)
            .Build();
        TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "\"\"");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(EmptyBinaryStringBase64) {
        TString binaryString;
        TValue value = TValueBuilder()
            .String(binaryString)
            .Build();
        TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Base64);
        UNIT_ASSERT_NO_DIFF(jsonString, "\"\"");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Base64);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(EmptyList) {
        TValue value = TValueBuilder()
            .EmptyList(TTypeBuilder().Primitive(EPrimitiveType::Int64).Build())
            .Build();
        TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "[]");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    namespace {
        TString InvalidJsonToBinaryStringBase(const TString& jsonString) {
            TString errorMessage;
            try {
                TType stringType = TTypeBuilder().Primitive(EPrimitiveType::String).Build();
                TValue resultValue = JsonToYdbValue(jsonString, stringType, EBinaryStringEncoding::Unicode);
                UNIT_FAIL("Exception should have been thrown, but it hasn't");
            }
            catch (TContractViolation& e) {
                errorMessage = e.what();
            }
            catch (std::exception& e) {
                UNIT_FAIL(TStringBuilder() << "Uncaught exception: " << e.what());
            }
            return errorMessage;
        }
    }

    Y_UNIT_TEST(InvalidJsonToBinaryString1) {
        TString jsonString = R"(some string")";
        TString errorMessage = InvalidJsonToBinaryStringBase(jsonString);
        UNIT_ASSERT_STRING_CONTAINS(
            errorMessage,
            "Invalid value"
        );
    }

    Y_UNIT_TEST(InvalidJsonToBinaryString2) {
        TString jsonString = R"("some string)";
        TString errorMessage = InvalidJsonToBinaryStringBase(jsonString);
        UNIT_ASSERT_STRING_CONTAINS(
            errorMessage,
            "Missing a closing quotation mark in string"
        );
    }

    Y_UNIT_TEST(InvalidJsonToBinaryString3) {
        TString jsonString = "\"some string \\\"";
        TString errorMessage = InvalidJsonToBinaryStringBase(jsonString);
        UNIT_ASSERT_STRING_CONTAINS(
            errorMessage,
            "Missing a closing quotation mark in string"
        );
    }

    Y_UNIT_TEST(InvalidJsonToBinaryString4) {
        TString jsonString = R"("some \ string")";
        TString errorMessage = InvalidJsonToBinaryStringBase(jsonString);
        UNIT_ASSERT_STRING_CONTAINS(
            errorMessage,
            "Invalid escape character in string"
        );
    }

    Y_UNIT_TEST(InvalidJsonToBinaryString5) {
        TString jsonString = R"("some string \u001")";
        TString errorMessage = InvalidJsonToBinaryStringBase(jsonString);
        UNIT_ASSERT_STRING_CONTAINS(
            errorMessage,
            "Incorrect hex digit after \\u escape in string"
        );
    }

    Y_UNIT_TEST(InvalidJsonToBinaryString6) {
        TString jsonString = R"("some string \u0140")";
        TString errorMessage = InvalidJsonToBinaryStringBase(jsonString);
        UNIT_ASSERT_STRING_CONTAINS(
            errorMessage,
            "Unicode symbols with codes greater than 255 are not supported"
        );
    }

    Y_UNIT_TEST(InvalidJsonToBinaryString7) {
        TString jsonString = R"("some string \u00AG")";
        TString errorMessage = InvalidJsonToBinaryStringBase(jsonString);
        UNIT_ASSERT_STRING_CONTAINS(
            errorMessage,
            "Incorrect hex digit after \\u escape in string"
        );
    }

    Y_UNIT_TEST(PrimitiveValueUtf8String1) {
        TString utf8Str = "Escape characters: \" \\ \f \b \t \r\nNon-escaped characters: / ' < > & []() ";
        TValue value = TValueBuilder()
            .Utf8(utf8Str)
            .Build();
        TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);

        UNIT_ASSERT_NO_DIFF(
            jsonString,
            "\"Escape characters: \\\" " R"(\\ \f \b \t \r\nNon-escaped characters: / ' < > & []() ")"
        );
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PrimitiveValueUtf8String2) {
        TString utf8Str = "\xD0\xB0\xD0\xB1\xD0\xB2\xD0\xB3\xD0\xB4\xD0\xB5\xD1\x91\xD0\xB6\xD0\xB7\xD0\xB8\xD0\xBA\xD0\xBB"
            "\xD0\xBC\xD0\xBD\xD0\xBE\xD0\xBF\xD1\x80\xD1\x81\xD1\x82\xD1\x83\xD1\x84\xD1\x85\xD1\x86\xD1\x87\xD1\x88"
            "\xD1\x89\xD1\x8A\xD1\x8B\xD1\x8C\xD1\x8D\xD1\x8E\xD1\x8F\xD0\x90\xD0\x91\xD0\x92\xD0\x93\xD0\x94\xD0\x95"
            "\xD0\x81\xD0\x96\xD0\x97\xD0\x98\xD0\x9A\xD0\x9B\xD0\x9C\xD0\x9D\xD0\x9E\xD0\x9F\xD0\xA0\xD0\xA1\xD0\xA2"
            "\xD0\xA3\xD0\xA4\xD0\xA5\xD0\xA6\xD0\xA7\xD0\xA8\xD0\xA9\xD0\xAA\xD0\xAB\xD0\xAC\xD0\xAD\xD0\xAE\xD0\xAF";
        TValue value = TValueBuilder()
            .Utf8(utf8Str)
            .Build();
        TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);

        UNIT_ASSERT_NO_DIFF(
            jsonString,
            TStringBuilder() << '"' << utf8Str << '"'
        );
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(TaggedValue) {
        TValue value = TValueBuilder()
            .BeginTagged("my_tag")
                .BeginTagged("my_inner_tag")
                    .Uint32(1)
                .EndTagged()
            .EndTagged()
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"(1)");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(CompositeValueEmptyList) {
        TValue value = TValueBuilder()
            .EmptyList(TTypeBuilder().Primitive(EPrimitiveType::Uint32).Build())
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "[]");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(CompositeValueIntList) {
        TValue value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .Int32(1)
            .AddListItem()
                .Int32(10)
            .AddListItem()
                .Int32(100)
            .EndList()
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "[1,10,100]");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(CompositeValueStruct) {
        TValue value = TValueBuilder()
            .BeginStruct()
            .AddMember("Id")
                .Uint32(1)
            .AddMember("Name")
                .String("Anna")
            .AddMember("Value")
                .Int32(-100)
            .AddMember("Description")
                .EmptyOptional(EPrimitiveType::Utf8)
            .EndStruct()
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"({"Id":1,"Name":"Anna","Value":-100,"Description":null})");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(NewDatetimeValuesStruct) {
        TValue value = TValueBuilder()
            .BeginStruct()
            .AddMember("Interval64")
                .Interval64(1)
            .AddMember("Date32")
                .Date32(-1024)
            .AddMember("Datetime64")
                .Datetime64(-123456789)
            .AddMember("Timestamp64")
                .Timestamp64(-123456789000100LL)
            .EndStruct()
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"({"Interval64":1,"Date32":"1967-03-14","Datetime64":"1966-02-02T02:26:51Z","Timestamp64":"1966-02-02T02:26:50.999900Z"})");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(CompositeValueTuple) {
        TValue value = TValueBuilder()
            .BeginTuple()
            .AddElement()
                .BeginOptional()
                    .BeginOptional()
                        .Int32(10)
                    .EndOptional()
                .EndOptional()
            .AddElement()
                .BeginOptional()
                    .BeginOptional()
                        .BeginOptional()
                            .Int64(-1)
                        .EndOptional()
                    .EndOptional()
                .EndOptional()
            .AddElement()
                .BeginOptional()
                    .EmptyOptional(TTypeBuilder().Primitive(EPrimitiveType::String).Build())
                .EndOptional()
            .AddElement()
                .BeginOptional()
                    .BeginOptional()
                        .EmptyOptional(EPrimitiveType::Utf8)
                    .EndOptional()
                .EndOptional()
            .EndTuple()
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, "[10,-1,null,null]");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(CompositeValueDict) {
        TValue value = TValueBuilder()
            .BeginDict()
            .AddDictItem()
                .DictKey()
                    .Int64(1)
                .DictPayload()
                    .String("Value1")
            .AddDictItem()
                .DictKey()
                    .Int64(2)
                .DictPayload()
                    .String("Value2")
            .EndDict()
            .Build();
        const TString jsonString = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString, R"([[1,"Value1"],[2,"Value2"]])");
        TValue resultValue = JsonToYdbValue(jsonString, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue).DebugString()
        );
    }

    Y_UNIT_TEST(PgValue) {
        TPgType pgType("");
        TPgValue v1(TPgValue::VK_TEXT, "text_value", pgType);
        TPgValue v2(TPgValue::VK_BINARY, "binary_value", pgType);
        TPgValue v3(TPgValue::VK_TEXT, "", pgType);
        TPgValue v4(TPgValue::VK_BINARY, "", pgType);
        TPgValue v5(TPgValue::VK_NULL, "", pgType);

        TValue value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .Pg(v1)
            .AddListItem()
                .Pg(v2)
            .AddListItem()
                .Pg(v3)
            .AddListItem()
                .Pg(v4)
            .AddListItem()
                .Pg(v5)
            .EndList()
            .Build();

        // unicode
        const TString jsonString1 = FormatValueJson(value, EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(jsonString1, R"(["text_value",["binary_value"],"",[""],null])");

        TValue resultValue1 = JsonToYdbValue(jsonString1, value.GetType(), EBinaryStringEncoding::Unicode);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue1).DebugString()
        );

        // base64
        const TString jsonString2 = FormatValueJson(value, EBinaryStringEncoding::Base64);
        UNIT_ASSERT_NO_DIFF(jsonString2, R"(["text_value",["YmluYXJ5X3ZhbHVl"],"",[""],null])");

        TValue resultValue2 = JsonToYdbValue(jsonString2, value.GetType(), EBinaryStringEncoding::Base64);
        UNIT_ASSERT_NO_DIFF(
            TProtoAccessor::GetProto(value).DebugString(),
            TProtoAccessor::GetProto(resultValue2).DebugString()
        );
    }
}

} // namespace NYdb
