#include "test_base.h"

#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/binary_json/read.h>

using namespace NKikimr::NBinaryJson;

class TBinaryJsonContainerTest: public TBinaryJsonTestBase {
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
            const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.first));
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
            const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.first));
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
            {.Json = R"([1, {}, [], true, false, "string", null])", .Index = 0, .Element = "1"},
            {.Json = R"([1, {}, [], true, false, "string", null])", .Index = 1, .Element = "{}"},
            {.Json = R"([1, {}, [], true, false, "string", null])", .Index = 2, .Element = "[]"},
            {.Json = R"([1, {}, [], true, false, "string", null])", .Index = 3, .Element = "true"},
            {.Json = R"([1, {}, [], true, false, "string", null])", .Index = 4, .Element = "false"},
            {.Json = R"([1, {}, [], true, false, "string", null])", .Index = 5, .Element = "\"string\""},
            {.Json = R"([1, {}, [], true, false, "string", null])", .Index = 6, .Element = "null"},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.Json));
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
            {.Json = "[]", .Result = {}},
            {.Json = "[1, 2, 3, 4, 5]", .Result = {"1", "2", "3", "4", "5"}},
            {.Json = R"([1, {}, [], true, false, "string", null])", .Result = {"1", "{}", "[]", "true", "false", "\"string\"", "null"}},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.Json));
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
            {.Json = "{}", .Key = "key", .Result = Nothing()},
            {.Json = R"({"_key_": 123})", .Key = "key", .Result = Nothing()},
            {.Json = R"({"key": "another"})", .Key = "another", .Result = Nothing()},

            {.Json = R"({"key": 123})", .Key = "key", .Result = {"123"}},
            {.Json = R"({"key": "string"})", .Key = "key", .Result = {"\"string\""}},
            {.Json = R"({"key": null})", .Key = "key", .Result = {"null"}},
            {.Json = R"({"key": true})", .Key = "key", .Result = {"true"}},
            {.Json = R"({"key": false})", .Key = "key", .Result = {"false"}},
            {.Json = R"({"key": {}})", .Key = "key", .Result = {"{}"}},
            {.Json = R"({"key": []})", .Key = "key", .Result = {"[]"}},

            {.Json = R"({"one": 1, "two": 2, "three": 3, "four": 4})", .Key = "one", .Result = {"1"}},
            {.Json = R"({"one": 1, "two": 2, "three": 3, "four": 4})", .Key = "two", .Result = {"2"}},
            {.Json = R"({"one": 1, "two": 2, "three": 3, "four": 4})", .Key = "three", .Result = {"3"}},
            {.Json = R"({"one": 1, "two": 2, "three": 3, "four": 4})", .Key = "four", .Result = {"4"}},
        };

        for (const auto& testCase : testCases) {
            const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.Json));
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
            {.Json = "{}", .Result = {}},
            {.Json = R"({"key": 123})", .Result = {{"key", "123"}}},
            {.Json = R"({
                "one": 123,
                "two": null,
                "three": false,
                "four": true,
                "five": "string",
                "six": [],
                "seven": {}
            })",
             .Result = {
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
            const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(testCase.Json));
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
