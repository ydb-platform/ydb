#include "test_base.h"

#include <yql/essentials/types/binary_json/write.h>

using namespace NKikimr::NBinaryJson;

TJsonPathTestBase::TJsonPathTestBase()
    : FunctionRegistry_(CreateFunctionRegistry(CreateBuiltinRegistry()))
    , Alloc_(__LOCATION__)
    , Env_(Alloc_)
    , MemInfo_("Memory")
    , HolderFactory_(Alloc_.Ref(), MemInfo_, FunctionRegistry_.Get())
    , ValueBuilder_(HolderFactory_)
{
}

TIssueCode TJsonPathTestBase::C(TIssuesIds::EIssueCode code) {
    return static_cast<TIssueCode>(code);
}

TUnboxedValue TJsonPathTestBase::ParseJson(TStringBuf raw) {
    return TryParseJsonDom(raw, &ValueBuilder_);
}

void TJsonPathTestBase::RunTestCase(const TString& rawJson, const TString& rawJsonPath, const TVector<TString>& expectedResult) {
    try {
        const auto unboxedValueJson = TValue(ParseJson(rawJson));

        const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(rawJson));
        auto reader = TBinaryJsonReader::Make(binaryJson);
        auto binaryJsonRoot = TValue(reader->GetRootCursor());

        TIssues issues;
        const TJsonPathPtr jsonPath = ParseJsonPath(rawJsonPath, issues, MaxParseErrors_);
        UNIT_ASSERT_C(issues.Empty(), "Parse errors found");

        for (const auto& json : {unboxedValueJson, binaryJsonRoot}) {
            const auto result = ExecuteJsonPath(jsonPath, json, TVariablesMap{}, &ValueBuilder_);
            UNIT_ASSERT_C(!result.IsError(), "Runtime errors found");

            const auto& nodes = result.GetNodes();
            UNIT_ASSERT_VALUES_EQUAL(nodes.size(), expectedResult.size());
            for (size_t i = 0; i < nodes.size(); i++) {
                const auto converted = nodes[i].ConvertToUnboxedValue(&ValueBuilder_);
                UNIT_ASSERT_VALUES_EQUAL(SerializeJsonDom(converted), expectedResult[i]);
            }
        }
    } catch (...) {
        TStringBuilder message;
        message << "Exception: " << CurrentExceptionMessage() << Endl
                << "Input JSON: " << rawJson << Endl
                << "Jsonpath: " << rawJsonPath << Endl
                << "Expected output:";
        for (const auto& item : expectedResult) {
            message << " " << item;
        }
        message << Endl;

        UNIT_FAIL(message);
    }
}

void TJsonPathTestBase::RunParseErrorTestCase(const TString& rawJsonPath) {
    try {
        TIssues issues;
        const TJsonPathPtr jsonPath = ParseJsonPath(rawJsonPath, issues, 2);
        UNIT_ASSERT_C(!issues.Empty(), "Expected parse errors");
    } catch (...) {
        UNIT_FAIL(
            "Exception: " << CurrentExceptionMessage() << Endl
            << "Jsonpath: " << rawJsonPath << Endl
        );
    }
}

void TJsonPathTestBase::RunRuntimeErrorTestCase(const TString& rawJson, const TString& rawJsonPath, TIssueCode error) {
    try {
        const auto unboxedValueJson = TValue(ParseJson(rawJson));

        const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(rawJson));
        auto reader = TBinaryJsonReader::Make(binaryJson);
        auto binaryJsonRoot = TValue(reader->GetRootCursor());

        TIssues issues;
        const TJsonPathPtr jsonPath = ParseJsonPath(rawJsonPath, issues, MaxParseErrors_);
        UNIT_ASSERT_C(issues.Empty(), "Parse errors found");

        for (const auto& json : {unboxedValueJson, binaryJsonRoot}) {
            const auto result = ExecuteJsonPath(jsonPath, json, TVariablesMap{}, &ValueBuilder_);
            UNIT_ASSERT_C(result.IsError(), "Expected runtime error");
            UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetCode(), error);
        }
    } catch (...) {
        UNIT_FAIL(
            TStringBuilder()
                 << "Exception: " << CurrentExceptionMessage() << Endl
                << "Input JSON: " << rawJson << Endl
                << "Jsonpath: " << rawJsonPath << Endl
                << "Expected error: " << error << Endl
        );
    }
}

void TJsonPathTestBase::RunVariablesTestCase(const TString& rawJson, const THashMap<TStringBuf, TStringBuf>& variables, const TString& rawJsonPath, const TVector<TString>& expectedResult) {
    try {
        const auto unboxedValueJson = TValue(ParseJson(rawJson));

        const auto binaryJson = std::get<TBinaryJson>(SerializeToBinaryJson(rawJson));
        auto reader = TBinaryJsonReader::Make(binaryJson);
        auto binaryJsonRoot = TValue(reader->GetRootCursor());

        TVariablesMap unboxedValueVariables;
        for (const auto& it : variables) {
            unboxedValueVariables[it.first] = TValue(ParseJson(it.second));
        }

        TVariablesMap binaryJsonVariables;
        TVector<TBinaryJson> storage;
        TVector<TBinaryJsonReaderPtr> readers;
        storage.reserve(variables.size());
        readers.reserve(variables.size());
        for (const auto& it : variables) {
            storage.push_back(std::get<TBinaryJson>(SerializeToBinaryJson(it.second)));
            readers.push_back(TBinaryJsonReader::Make(storage.back()));
            binaryJsonVariables[it.first] = TValue(readers.back()->GetRootCursor());
        }

        TIssues issues;
        const TJsonPathPtr jsonPath = ParseJsonPath(rawJsonPath, issues, MaxParseErrors_);
        UNIT_ASSERT_C(issues.Empty(), "Parse errors found");

        TVector<std::pair<TValue, TVariablesMap>> testCases = {
            {unboxedValueJson, unboxedValueVariables},
            {binaryJsonRoot, binaryJsonVariables},
        };
        for (const auto& testCase : testCases) {
            const auto result = ExecuteJsonPath(jsonPath, testCase.first, testCase.second, &ValueBuilder_);
            UNIT_ASSERT_C(!result.IsError(), "Runtime errors found");

            const auto& nodes = result.GetNodes();
            UNIT_ASSERT_VALUES_EQUAL(nodes.size(), expectedResult.size());
            for (size_t i = 0; i < nodes.size(); i++) {
                const auto converted = nodes[i].ConvertToUnboxedValue(&ValueBuilder_);
                UNIT_ASSERT_VALUES_EQUAL(SerializeJsonDom(converted), expectedResult[i]);
            }
        }
    } catch (...) {
        TStringBuilder message;
        message << "Exception: " << CurrentExceptionMessage() << Endl
                << "Input JSON: " << rawJson << Endl
                << "Variables:" << Endl;
        for (const auto& it : variables) {
            message << "\t" << it.first << " = " << it.second;
        }

        message << Endl
                << "Jsonpath: " << rawJsonPath << Endl
                << "Expected output:";
        for (const auto& item : expectedResult) {
            message << " " << item;
        }
        message << Endl;

        UNIT_FAIL(message);
    }
}

