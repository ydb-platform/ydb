#include "pb_io.h"

#include "is_equal.h"

#include <library/cpp/protobuf/util/ut/common_ut.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

static NProtobufUtilUt::TTextTest GetCorrectMessage() {
    NProtobufUtilUt::TTextTest m;
    m.SetFoo(42);
    return m;
}

static NProtobufUtilUt::TTextEnumTest GetCorrectEnumMessage() {
    NProtobufUtilUt::TTextEnumTest m;
    m.SetSlot(NProtobufUtilUt::TTextEnumTest::EET_SLOT_1);
    return m;
}

static const TString CORRECT_MESSAGE =
    R"(Foo: 42
)";
static const TString CORRECT_ENUM_NAME_MESSAGE =
    R"(Slot: EET_SLOT_1
)";
static const TString CORRECT_ENUM_ID_MESSAGE =
    R"(Slot: 1
)";

static const TString INCORRECT_MESSAGE =
    R"(Bar: 1
)";
static const TString INCORRECT_ENUM_NAME_MESSAGE =
    R"(Slot: EET_SLOT_3
)";
static const TString INCORRECT_ENUM_ID_MESSAGE =
    R"(Slot: 3
)";

static const TString CORRECT_BASE64_MESSAGE = "CCo,";

static const TString CORRECT_UNEVEN_BASE64_MESSAGE = "CCo";

static const TString INCORRECT_BASE64_MESSAGE = "CC";

Y_UNIT_TEST_SUITE(TTestProtoBufIO) {
    Y_UNIT_TEST(TestBase64) {
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(NProtoBuf::TryParseFromBase64String(CORRECT_BASE64_MESSAGE, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(!NProtoBuf::TryParseFromBase64String(INCORRECT_BASE64_MESSAGE, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(NProtoBuf::TryParseFromBase64String(CORRECT_UNEVEN_BASE64_MESSAGE , message, true));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(!NProtoBuf::TryParseFromBase64String(CORRECT_UNEVEN_BASE64_MESSAGE , message, false));
        }
        {
            UNIT_ASSERT_VALUES_EQUAL(CORRECT_BASE64_MESSAGE, NProtoBuf::SerializeToBase64String(GetCorrectMessage()));
        }
        {
            const auto m = NProtoBuf::ParseFromBase64String<NProtobufUtilUt::TTextTest>(CORRECT_BASE64_MESSAGE);
            UNIT_ASSERT(NProtoBuf::IsEqual(GetCorrectMessage(), m));
        }
    }

    Y_UNIT_TEST(TestParseFromTextFormat) {
        TTempDir tempDir;
        const TFsPath correctFileName = TFsPath{tempDir()} / "correct.pb.txt";
        const TFsPath incorrectFileName = TFsPath{tempDir()} / "incorrect.pb.txt";

        TFileOutput{correctFileName}.Write(CORRECT_MESSAGE);
        TFileOutput{incorrectFileName}.Write(INCORRECT_MESSAGE);

        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(TryParseFromTextFormat(correctFileName, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(!TryParseFromTextFormat(incorrectFileName, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            TStringInput in{CORRECT_MESSAGE};
            UNIT_ASSERT(TryParseFromTextFormat(in, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            TStringInput in{INCORRECT_MESSAGE};
            UNIT_ASSERT(!TryParseFromTextFormat(in, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_NO_EXCEPTION(TryParseFromTextFormat(incorrectFileName, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(!TryParseFromTextFormat("this_file_doesnt_exists", message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_NO_EXCEPTION(TryParseFromTextFormat("this_file_doesnt_exists", message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_EXCEPTION(ParseFromTextFormat("this_file_doesnt_exists", message), TFileError);
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_NO_EXCEPTION(ParseFromTextFormat(correctFileName, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_EXCEPTION(ParseFromTextFormat(incorrectFileName, message), yexception);
        }
        {
            NProtobufUtilUt::TTextTest message;
            TStringInput in{CORRECT_MESSAGE};
            UNIT_ASSERT_NO_EXCEPTION(ParseFromTextFormat(in, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            TStringInput in{INCORRECT_MESSAGE};
            UNIT_ASSERT_EXCEPTION(ParseFromTextFormat(in, message), yexception);
        }
        {
            NProtobufUtilUt::TTextTest m;
            const auto f = [&correctFileName](NProtobufUtilUt::TTextTest& mm) {
                mm = ParseFromTextFormat<NProtobufUtilUt::TTextTest>(correctFileName);
            };
            UNIT_ASSERT_NO_EXCEPTION(f(m));
            UNIT_ASSERT(NProtoBuf::IsEqual(GetCorrectMessage(), m));
        }
        {
            UNIT_ASSERT_EXCEPTION(ParseFromTextFormat<NProtobufUtilUt::TTextTest>(incorrectFileName), yexception);
        }
        {
            NProtobufUtilUt::TTextTest m;
            TStringInput in{CORRECT_MESSAGE};
            const auto f = [&in](NProtobufUtilUt::TTextTest& mm) {
                mm = ParseFromTextFormat<NProtobufUtilUt::TTextTest>(in);
            };
            UNIT_ASSERT_NO_EXCEPTION(f(m));
            UNIT_ASSERT(NProtoBuf::IsEqual(GetCorrectMessage(), m));
        }
        {
            TStringInput in{INCORRECT_MESSAGE};
            UNIT_ASSERT_EXCEPTION(ParseFromTextFormat<NProtobufUtilUt::TTextTest>(in), yexception);
        }
        {
            const TFsPath correctFileName2 = TFsPath{tempDir()} / "serialized.pb.txt";
            const auto original = GetCorrectMessage();
            UNIT_ASSERT_NO_EXCEPTION(SerializeToTextFormat(original, correctFileName2));
            const auto serializedStr = TUnbufferedFileInput{correctFileName2}.ReadAll();
            UNIT_ASSERT_VALUES_EQUAL(serializedStr, CORRECT_MESSAGE);
        }
        {
            const auto original = GetCorrectMessage();
            TStringStream out;
            UNIT_ASSERT_NO_EXCEPTION(SerializeToTextFormat(original, out));
            UNIT_ASSERT_VALUES_EQUAL(out.Str(), CORRECT_MESSAGE);
        }
        {
            NProtobufUtilUt::TTextTest m;
            const auto f = [&correctFileName](NProtobufUtilUt::TTextTest& mm) {
                mm = ParseFromTextFormat<NProtobufUtilUt::TTextTest>(
                    correctFileName,
                    EParseFromTextFormatOption::AllowUnknownField);
            };
            UNIT_ASSERT_NO_EXCEPTION(f(m));
            UNIT_ASSERT(NProtoBuf::IsEqual(GetCorrectMessage(), m));
        }
        {
            const NProtobufUtilUt::TTextTest empty;
            NProtobufUtilUt::TTextTest m;
            const auto f = [&incorrectFileName](NProtobufUtilUt::TTextTest& mm) {
                mm = ParseFromTextFormat<NProtobufUtilUt::TTextTest>(
                    incorrectFileName,
                    EParseFromTextFormatOption::AllowUnknownField);
            };
            UNIT_ASSERT_NO_EXCEPTION(f(m));
            UNIT_ASSERT(NProtoBuf::IsEqual(empty, m));
        }
    }

    Y_UNIT_TEST(TestSerializeToTextFormatWithEnumId) {
        TTempDir tempDir;
        const TFsPath correctNameFileName = TFsPath{tempDir()} / "correct_name.pb.txt";
        const TFsPath incorrectNameFileName = TFsPath{tempDir()} / "incorrect_name.pb.txt";
        const TFsPath correctIdFileName = TFsPath{tempDir()} / "correct_id.pb.txt";
        const TFsPath incorrectIdFileName = TFsPath{tempDir()} / "incorrect_id.pb.txt";

        TFileOutput{correctNameFileName}.Write(CORRECT_ENUM_NAME_MESSAGE);
        TFileOutput{incorrectNameFileName}.Write(INCORRECT_ENUM_NAME_MESSAGE);
        TFileOutput{correctIdFileName}.Write(CORRECT_ENUM_ID_MESSAGE);
        TFileOutput{incorrectIdFileName}.Write(INCORRECT_ENUM_ID_MESSAGE);

        {
            NProtobufUtilUt::TTextEnumTest message;
            for (auto correct_message: {CORRECT_ENUM_ID_MESSAGE, CORRECT_ENUM_NAME_MESSAGE}) {
                TStringInput in{correct_message};
                UNIT_ASSERT_NO_EXCEPTION(ParseFromTextFormat(in, message));
            }
        }
        {
            NProtobufUtilUt::TTextEnumTest message;
            for (auto incorrect_message: {INCORRECT_ENUM_ID_MESSAGE, INCORRECT_ENUM_NAME_MESSAGE}) {
                TStringInput in{incorrect_message};
                UNIT_ASSERT_EXCEPTION(ParseFromTextFormat(in, message), yexception);
            }
        }
        {
            const auto f = [](NProtobufUtilUt::TTextEnumTest& mm, const TString fileName) {
                mm = ParseFromTextFormat<NProtobufUtilUt::TTextEnumTest>(fileName);
            };
            for (auto fileName: {correctIdFileName, correctNameFileName}) {
                NProtobufUtilUt::TTextEnumTest m;
                UNIT_ASSERT_NO_EXCEPTION(f(m, fileName));
                UNIT_ASSERT(NProtoBuf::IsEqual(GetCorrectEnumMessage(), m));
            }
        }
        {
            UNIT_ASSERT_EXCEPTION(ParseFromTextFormat<NProtobufUtilUt::TTextEnumTest>(incorrectIdFileName), yexception);
            UNIT_ASSERT_EXCEPTION(ParseFromTextFormat<NProtobufUtilUt::TTextEnumTest>(incorrectNameFileName), yexception);
        }
        {
            const auto original = GetCorrectEnumMessage();
            TStringStream out;
            UNIT_ASSERT_NO_EXCEPTION(SerializeToTextFormat(original, out));
            UNIT_ASSERT_VALUES_EQUAL(out.Str(), CORRECT_ENUM_NAME_MESSAGE);
        }
        {
            const auto original = GetCorrectEnumMessage();
            TStringStream out;
            UNIT_ASSERT_NO_EXCEPTION(SerializeToTextFormatWithEnumId(original, out));
            UNIT_ASSERT_VALUES_EQUAL(out.Str(), CORRECT_ENUM_ID_MESSAGE);
        }
    }

    Y_UNIT_TEST(TestMergeFromTextFormat) {
        //
        // Tests cases below are identical to `Parse` tests
        //
        TTempDir tempDir;
        const TFsPath correctFileName = TFsPath{tempDir()} / "correct.pb.txt";
        const TFsPath incorrectFileName = TFsPath{tempDir()} / "incorrect.pb.txt";

        TFileOutput{correctFileName}.Write(CORRECT_MESSAGE);
        TFileOutput{incorrectFileName}.Write(INCORRECT_MESSAGE);

        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(TryMergeFromTextFormat(correctFileName, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(!TryMergeFromTextFormat(incorrectFileName, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            TStringInput in{CORRECT_MESSAGE};
            UNIT_ASSERT(TryMergeFromTextFormat(in, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            TStringInput in{INCORRECT_MESSAGE};
            UNIT_ASSERT(!TryMergeFromTextFormat(in, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_NO_EXCEPTION(TryMergeFromTextFormat(incorrectFileName, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT(!TryMergeFromTextFormat("this_file_doesnt_exists", message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_NO_EXCEPTION(TryMergeFromTextFormat("this_file_doesnt_exists", message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_EXCEPTION(MergeFromTextFormat("this_file_doesnt_exists", message), TFileError);
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_NO_EXCEPTION(MergeFromTextFormat(correctFileName, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            UNIT_ASSERT_EXCEPTION(MergeFromTextFormat(incorrectFileName, message), yexception);
        }
        {
            NProtobufUtilUt::TTextTest message;
            TStringInput in{CORRECT_MESSAGE};
            UNIT_ASSERT_NO_EXCEPTION(MergeFromTextFormat(in, message));
        }
        {
            NProtobufUtilUt::TTextTest message;
            TStringInput in{INCORRECT_MESSAGE};
            UNIT_ASSERT_EXCEPTION(MergeFromTextFormat(in, message), yexception);
        }
        {
            NProtobufUtilUt::TTextTest m;
            const auto f = [&correctFileName](NProtobufUtilUt::TTextTest& mm) {
                mm = MergeFromTextFormat<NProtobufUtilUt::TTextTest>(correctFileName);
            };
            UNIT_ASSERT_NO_EXCEPTION(f(m));
            UNIT_ASSERT(NProtoBuf::IsEqual(GetCorrectMessage(), m));
        }
        {
            UNIT_ASSERT_EXCEPTION(MergeFromTextFormat<NProtobufUtilUt::TTextTest>(incorrectFileName), yexception);
        }
        {
            NProtobufUtilUt::TTextTest m;
            TStringInput in{CORRECT_MESSAGE};
            const auto f = [&in](NProtobufUtilUt::TTextTest& mm) {
                mm = MergeFromTextFormat<NProtobufUtilUt::TTextTest>(in);
            };
            UNIT_ASSERT_NO_EXCEPTION(f(m));
            UNIT_ASSERT(NProtoBuf::IsEqual(GetCorrectMessage(), m));
        }
        {
            TStringInput in{INCORRECT_MESSAGE};
            UNIT_ASSERT_EXCEPTION(MergeFromTextFormat<NProtobufUtilUt::TTextTest>(in), yexception);
        }
        {
            const TFsPath correctFileName2 = TFsPath{tempDir()} / "serialized.pb.txt";
            const auto original = GetCorrectMessage();
            UNIT_ASSERT_NO_EXCEPTION(SerializeToTextFormat(original, correctFileName2));
            const auto serializedStr = TUnbufferedFileInput{correctFileName2}.ReadAll();
            UNIT_ASSERT_VALUES_EQUAL(serializedStr, CORRECT_MESSAGE);
        }
        {
            const auto original = GetCorrectMessage();
            TStringStream out;
            UNIT_ASSERT_NO_EXCEPTION(SerializeToTextFormat(original, out));
            UNIT_ASSERT_VALUES_EQUAL(out.Str(), CORRECT_MESSAGE);
        }
        {
            NProtobufUtilUt::TTextTest m;
            const auto f = [&correctFileName](NProtobufUtilUt::TTextTest& mm) {
                mm = MergeFromTextFormat<NProtobufUtilUt::TTextTest>(
                    correctFileName,
                    EParseFromTextFormatOption::AllowUnknownField);
            };
            UNIT_ASSERT_NO_EXCEPTION(f(m));
            UNIT_ASSERT(NProtoBuf::IsEqual(GetCorrectMessage(), m));
        }
        {
            const NProtobufUtilUt::TTextTest empty;
            NProtobufUtilUt::TTextTest m;
            const auto f = [&incorrectFileName](NProtobufUtilUt::TTextTest& mm) {
                mm = MergeFromTextFormat<NProtobufUtilUt::TTextTest>(
                    incorrectFileName,
                    EParseFromTextFormatOption::AllowUnknownField);
            };
            UNIT_ASSERT_NO_EXCEPTION(f(m));
            UNIT_ASSERT(NProtoBuf::IsEqual(empty, m));
        }

        //
        // Test cases for `Merge`
        //
        {
            NProtobufUtilUt::TTextTest message;
            message.SetFoo(100500);
            TStringInput in{CORRECT_MESSAGE};
            UNIT_ASSERT(TryMergeFromTextFormat(in, message));
            UNIT_ASSERT(NProtoBuf::IsEqual(message, GetCorrectMessage()));
        }
    }

    Y_UNIT_TEST(TestMergeFromString) {
        NProtobufUtilUt::TMergeTest message;
        NProtobufUtilUt::TMergeTest messageFirstHalf;
        NProtobufUtilUt::TMergeTest messageSecondHalf;

        for (ui32 v = ~0; v != 0; v >>= 1) {
            message.AddMergeInt(v);
            (v > 0xffff ? messageFirstHalf : messageSecondHalf).AddMergeInt(v);
        }

        const TString full = message.SerializeAsString();

        {
            NProtobufUtilUt::TMergeTest m1;
            UNIT_ASSERT(NProtoBuf::MergeFromString(m1, full));
            UNIT_ASSERT(NProtoBuf::IsEqual(message, m1));
        }
        {
            NProtobufUtilUt::TMergeTest m2;
            TStringBuf s0 = TStringBuf(full).SubStr(0, 3);
            TStringBuf s1 = TStringBuf(full).SubStr(3);
            // объединение результатов двух MergePartialFromString не эквивалентно вызову MergePartialFromString от объединения строк
            UNIT_ASSERT(!(NProtoBuf::MergePartialFromString(m2, s0) && NProtoBuf::MergePartialFromString(m2, s1)));
        }
        {
            NProtobufUtilUt::TMergeTest m3;
            UNIT_ASSERT(NProtoBuf::MergePartialFromString(m3, messageFirstHalf.SerializeAsString()));
            UNIT_ASSERT(NProtoBuf::MergeFromString(m3, messageSecondHalf.SerializeAsString()));
            UNIT_ASSERT(NProtoBuf::IsEqual(message, m3));
        }
    }
}
