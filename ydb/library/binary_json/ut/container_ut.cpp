#include "test_base.h"

#include <ydb/library/binary_json/write.h>
#include <ydb/library/binary_json/read.h>

using namespace NKikimr::NBinaryJson;

class TBinaryJsonContainerTest : public TBinaryJsonTestBase {
public:
    TBinaryJsonContainerTest()
        : TBinaryJsonTestBase()
    {
    }

    UNIT_TEST_SUITE(TBinaryJsonContainerTest);
        UNIT_TEST(TestGetType);
        UNIT_TEST(TestGetSize);
        UNIT_TEST(TestGetElement);
        UNIT_TEST(TestArrayIterator);
        UNIT_TEST(TestLookup);
        UNIT_TEST(TestObjectIterator);
    UNIT_TEST_SUITE_END();

    void TestGetType() {
        const TVector<std::pair<TString, EContainerType>> testCases = {
            {"1", EContainerType::TopLevelScalar},
            {"\"string\"", EContainerType::TopLevelScalar},
            {"null", EContainerType::TopLevelScalar},
            {"true", EContainerType::TopLevelScalar},
            {"false", EContainerType::TopLevelScalar},
            {"[]", EContainerType::Array},
            {"[1, 2, 3, 4]", EContainerType::Array},
            {"{}", EContainerType::Object},
            {R"({"key": 1, "another": null})", EContainerType::Object},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.first);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();

            UNIT_ASSERT_VALUES_EQUAL(container.GetType(), testCase.second);
        }
    }

    void TestGetSize() {
        const TVector<std::pair<TString, ui32>> testCases = {
            {"[]", 0},
            {"{}", 0},
            {R"([1, {}, [], true, false, "string", null])", 7},
            {R"({"key": true, "another_key": 2.34})", 2},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.first);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();

            UNIT_ASSERT_VALUES_EQUAL(container.GetSize(), testCase.second);
        }
    }

    struct TGetElementTestCase {
        TString Json;
        ui32 Index;
        TString Element;
    };

    void TestGetElement() {
        const TVector<TGetElementTestCase> testCases = {
            {R"([1, {}, [], true, false, "string", null])", 0, "1"},
            {R"([1, {}, [], true, false, "string", null])", 1, "{}"},
            {R"([1, {}, [], true, false, "string", null])", 2, "[]"},
            {R"([1, {}, [], true, false, "string", null])", 3, "true"},
            {R"([1, {}, [], true, false, "string", null])", 4, "false"},
            {R"([1, {}, [], true, false, "string", null])", 5, "\"string\""},
            {R"([1, {}, [], true, false, "string", null])", 6, "null"},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.Json);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();
            const auto element = container.GetElement(testCase.Index);

            UNIT_ASSERT_VALUES_EQUAL(EntryToJsonText(element), testCase.Element);
        }
    }

    struct TArrayIteratorTestCase {
        TString Json;
        TVector<TString> Result;
    };

    void TestArrayIterator() {
        const TVector<TArrayIteratorTestCase> testCases = {
            {"[]", {}},
            {"[1, 2, 3, 4, 5]", {"1", "2", "3", "4", "5"}},
            {R"([1, {}, [], true, false, "string", null])", {"1", "{}", "[]", "true", "false", "\"string\"", "null"}},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.Json);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();

            TVector<TString> result;
            auto it = container.GetArrayIterator();
            while (it.HasNext()) {
                result.push_back(EntryToJsonText(it.Next()));
            }

            UNIT_ASSERT_VALUES_EQUAL(testCase.Result.size(), result.size());

            for (ui32 i = 0; i < result.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL(result[i], testCase.Result[i]);
            }
        }
    }

    struct TLookupTestCase {
        TString Json;
        TString Key;
        TMaybe<TString> Result;
    };

    void TestLookup() {
        const TVector<TLookupTestCase> testCases = {
            {"{}", "key", Nothing()},
            {R"({"_key_": 123})", "key", Nothing()},
            {R"({"key": "another"})", "another", Nothing()},

            {R"({"key": 123})", "key", {"123"}},
            {R"({"key": "string"})", "key", {"\"string\""}},
            {R"({"key": null})", "key", {"null"}},
            {R"({"key": true})", "key", {"true"}},
            {R"({"key": false})", "key", {"false"}},
            {R"({"key": {}})", "key", {"{}"}},
            {R"({"key": []})", "key", {"[]"}},

            {R"({"one": 1, "two": 2, "three": 3, "four": 4})", "one", {"1"}},
            {R"({"one": 1, "two": 2, "three": 3, "four": 4})", "two", {"2"}},
            {R"({"one": 1, "two": 2, "three": 3, "four": 4})", "three", {"3"}},
            {R"({"one": 1, "two": 2, "three": 3, "four": 4})", "four", {"4"}},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.Json);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();
            const auto result = container.Lookup(testCase.Key);

            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), testCase.Result.Defined());
            if (result.Defined()) {
                UNIT_ASSERT_VALUES_EQUAL(EntryToJsonText(*result), *testCase.Result);
            }
        }
    }

    struct TObjectIteratorTestCase {
        TString Json;
        THashMap<TString, TString> Result;
    };

    void TestObjectIterator() {
        const TVector<TObjectIteratorTestCase> testCases = {
            {"{}", {}},
            {R"({"key": 123})", {{"key", "123"}}},
            {R"({
                "one": 123,
                "two": null,
                "three": false,
                "four": true,
                "five": "string",
                "six": [],
                "seven": {}
            })", {
                {"one", "123"},
                {"two", "null"},
                {"three", "false"},
                {"four", "true"},
                {"five", "\"string\""},
                {"six", "[]"},
                {"seven", "{}"},
            }},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(testCase.Json);
            const auto reader = TBinaryJsonReader::Make(binaryJson);
            const auto container = reader->GetRootCursor();

            THashMap<TString, TString> result;
            auto it = container.GetObjectIterator();
            while (it.HasNext()) {
                const auto pair = it.Next();
                result[pair.first.GetString()] = EntryToJsonText(pair.second);
            }

            UNIT_ASSERT_VALUES_EQUAL(testCase.Result.size(), result.size());

            for (const auto& it : testCase.Result) {
                UNIT_ASSERT(result.contains(it.first));
                UNIT_ASSERT_VALUES_EQUAL(result.at(it.first), it.second);
            }
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TBinaryJsonContainerTest);
