#include "test_base.h"

#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/binary_json/read.h>

using namespace NKikimr::NBinaryJson;

class TBinaryJsonExtractTest: public TBinaryJsonTestBase {
public:
    TBinaryJsonExtractTest()
        : TBinaryJsonTestBase()
    {
    }

    UNIT_TEST_SUITE(TBinaryJsonExtractTest);
    UNIT_TEST(TestInvalidCursor);
    UNIT_TEST(TestTopLevelScalar);
    UNIT_TEST(TestTopLevelArray);
    UNIT_TEST(TestTopLevelMap);
    UNIT_TEST(TestComplexCases);
    UNIT_TEST_SUITE_END();

    void TestInvalidCursor() {
        TEntryCursor invalidCursor({}, TEntry(EEntryType(31), 0));
        auto result = SerializeToBinaryJson(invalidCursor);
        UNIT_ASSERT(std::holds_alternative<TString>(result));

        UNIT_ASSERT_EQUAL("Unexpected entry type", std::get<TString>(result));
    }

    void TestTopLevelScalar() {
        const TVector<TString> testCases = {
            {"1"},
            {"1.25"},
            {"-2.36"},
            {"0"},
            {"\"string\""},
            {"\"\""},
            {"null"},
            {"true"},
            {"false"},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase));
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();
            const auto cursor = container.GetElement(0);
            const auto extractedBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(cursor));

            UNIT_ASSERT_EQUAL(SerializeToJson(binaryJson), SerializeToJson(extractedBinaryJson));
        }
    }

    void TestTopLevelArray() {
        const TVector<std::pair<TString, TString>> testCases = {
            {"[1]", "1"},
            {R"(["string"])", R"("string")"},
            {"[true]", "true"},
            {"[-1.2]", "-1.2"},
            {"[null]", "null"},
            {"[[1,2]]", "[1,2]"},
            {R"([{"a": "b"}])", R"({"a": "b"})"},
        };

        for (const auto& testCase : testCases) {
            const auto originalBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.first));
            const auto reader = TBinaryJsonReader::Make(originalBinaryJson);
            const auto container = reader->GetRootCursor();
            const auto cursor = container.GetArrayIterator().Next();
            const auto extractedBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(cursor));

            const auto expectedBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.second));

            UNIT_ASSERT_EQUAL(SerializeToJson(expectedBinaryJson), SerializeToJson(extractedBinaryJson));
        }
    }

    void TestTopLevelMap() {
        const TVector<std::pair<TString, TString>> testCases = {
            {R"({"root": "a"})", R"("a")"},
            {R"({"root": ""})", R"("")"},
            {R"({"root": 1})", "1"},
            {R"({"root": -1.2})", "-1.2"},
            {R"({"root": true})", "true"},
            {R"({"root": false})", "false"},
            {R"({"root": null})", "null"},
            {R"({"root": {}})", "{}"},
            {R"({"root": []})", "[]"},
        };

        for (const auto& testCase : testCases) {
            const auto originalBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.first));
            const auto reader = TBinaryJsonReader::Make(originalBinaryJson);
            const auto container = reader->GetRootCursor();
            const auto cursor = container.GetObjectIterator().Next();
            const auto extractedBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(cursor.second));

            const auto expectedBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.second));

            UNIT_ASSERT_EQUAL(SerializeToJson(expectedBinaryJson), SerializeToJson(extractedBinaryJson));
        }
    }

    void TestComplexCases() {
        // clang-format off
        const TVector<std::pair<TString, TString>> testCases = {
            {R"({"root": {"a": "b", "c": {"d": 1, "e": [1, 2.1, -3, "f", true, false, null, {}, {"g": []}]}}})",
                      R"({"a": "b", "c": {"d": 1, "e": [1, 2.1, -3, "f", true, false, null, {}, {"g": []}]}})"},

            {R"({"root": [[], [1, 1, 1, 2], {"a": true}, null, [], [[[]]], [[42]]]})",
                      R"([[], [1, 1, 1, 2], {"a": true}, null, [], [[[]]], [[42]]])"},
        };
        // clang-format on

        for (const auto& testCase : testCases) {
            const auto originalBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.first));
            const auto reader = TBinaryJsonReader::Make(originalBinaryJson);
            const auto container = reader->GetRootCursor();
            const auto cursor = container.GetObjectIterator().Next();
            const auto extractedBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(cursor.second));

            const auto expectedBinaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.second));

            UNIT_ASSERT_EQUAL(SerializeToJson(expectedBinaryJson), SerializeToJson(extractedBinaryJson));
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TBinaryJsonExtractTest);
