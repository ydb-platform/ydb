#include "test_base.h"

#include <ydb/library/binary_json/write.h>
#include <ydb/library/binary_json/read.h>

using namespace NKikimr::NBinaryJson;

class TBinaryJsonEntryTest : public TBinaryJsonTestBase {
public:
    TBinaryJsonEntryTest()
        : TBinaryJsonTestBase()
    {
    }

    UNIT_TEST_SUITE(TBinaryJsonEntryTest);
        UNIT_TEST(TestGetType);
        UNIT_TEST(TestGetContainer);
        UNIT_TEST(TestGetString);
        UNIT_TEST(TestGetNumber);
    UNIT_TEST_SUITE_END();

    void TestGetType() {
        const TVector<std::pair<TString, EEntryType>> testCases = {
            {"1", EEntryType::Number},
            {"\"string\"", EEntryType::String},
            {"null", EEntryType::Null},
            {"true", EEntryType::BoolTrue},
            {"false", EEntryType::BoolFalse},
            {"[[]]", EEntryType::Container},
            {"[[1, 2, 3, 4]]", EEntryType::Container},
            {"[{}]", EEntryType::Container},
            {R"([{"key": 1, "another": null}])", EEntryType::Container},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.first);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();

            UNIT_ASSERT_VALUES_EQUAL(container.GetElement(0).GetType(), testCase.second);
        }
    }

    void TestGetContainer() {
        const TVector<std::pair<TString, TString>> testCases = {
            {"[[]]", "[]"},
            {"[[1.2, 3.4, 5.6]]", "[1.2,3.4,5.6]"},
            {"[{}]", "{}"},
            {R"([{"abc": 123, "def": 456}])", R"({"abc":123,"def":456})"},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.first);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();
            const auto innerContainer = container.GetElement(0).GetContainer();

            UNIT_ASSERT_VALUES_EQUAL(ContainerToJsonText(innerContainer), testCase.second);
        }
    }

    void TestGetString() {
        const TVector<std::pair<TString, TString>> testCases = {
            {R"("")", ""},
            {R"("string")", "string"},
            {R"(["string", "another", "string"])", "string"},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.first);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();

            UNIT_ASSERT_VALUES_EQUAL(container.GetElement(0).GetString(), testCase.second);
        }
    }

    void TestGetNumber() {
        const TVector<std::pair<TString, double>> testCases = {
            {"0", 0},
            {"0.1234", 0.1234},
            {"1.2345", 1.2345},
            {"-0.12345", -0.12345},
            {"-1.2345", -1.2345},
            {"[1.5, 2, 3, 1.5]", 1.5},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.first);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();

            UNIT_ASSERT_VALUES_EQUAL(container.GetElement(0).GetNumber(), testCase.second);
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TBinaryJsonEntryTest);
