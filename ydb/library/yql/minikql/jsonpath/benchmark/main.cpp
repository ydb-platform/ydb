#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/minikql/jsonpath/jsonpath.h>

#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_mem_info.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/testing/benchmark/bench.h>

#include <util/random/fast.h>

using namespace NJson;

using namespace NYql;
using namespace NYql::NDom;
using namespace NYql::NUdf;
using namespace NYql::NJsonPath;
using namespace NJson;
using namespace NKikimr::NMiniKQL;

TString RandomString(ui32 min, ui32 max) {
    static TReallyFastRng32 rand(0);
    TString result;
    const ui32 length = rand.Uniform(min, max + 1);
    result.reserve(length);
    for (ui32 i = 0; i < length; ++i) {
        result.push_back(char(rand.Uniform('a', 'z' + 1)));
    }
    return result;
}

TString RandomString(ui32 length) {
    return RandomString(length, length);
}

TString GenerateRandomJson() {
    TJsonMap result;
    TJsonMap id;
    id.InsertValue("id", TJsonValue(RandomString(24)));
    id.InsertValue("issueId", TJsonValue(RandomString(24)));
    result.InsertValue("_id", std::move(id));
    result.InsertValue("@class", TJsonValue(RandomString(60)));
    result.InsertValue("author", TJsonValue(RandomString(10)));
    result.InsertValue("transitionId", TJsonValue(RandomString(24)));
    TJsonArray comments;
    for (ui32 i = 0; i < 30; i++) {
        TJsonMap comment;
        comment.InsertValue("id", TJsonValue(RandomString(24)));
        comment.InsertValue("newText", TJsonValue(RandomString(150)));
        comments.AppendValue(std::move(comment));
    }
    TJsonMap changes;
    changes.InsertValue("comment", std::move(comments));
    result.InsertValue("changes", std::move(changes));
    return result.GetStringRobust();
}

const size_t MAX_PARSE_ERRORS = 100;

#define PREPARE() \
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry())); \
    TScopedAlloc Alloc(__LOCATION__); \
    TTypeEnvironment Env(Alloc); \
    TMemoryUsageInfo MemInfo("Memory"); \
    THolderFactory HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get()); \
    TDefaultValueBuilder ValueBuilder(HolderFactory); \


Y_CPU_BENCHMARK(JsonPath, iface) {
    PREPARE()

    const TString json = GenerateRandomJson();
    const TUnboxedValue dom = TryParseJsonDom(json, &ValueBuilder);

    for (size_t i = 0; i < iface.Iterations(); i++) {
        TIssues issues;
        const auto jsonPath = ParseJsonPath("$.'_id'.issueId", issues, MAX_PARSE_ERRORS);
        const auto result = ExecuteJsonPath(jsonPath, TValue(dom), TVariablesMap(), &ValueBuilder);
        Y_ABORT_UNLESS(!result.IsError());
    }
}

Y_CPU_BENCHMARK(JsonPathLikeRegexWithCompile, iface) {
    PREPARE()

    const TString json = GenerateRandomJson();
    const TUnboxedValue dom = TryParseJsonDom(json, &ValueBuilder);

    for (size_t i = 0; i < iface.Iterations(); i++) {
        TIssues issues;
        const auto jsonPath = ParseJsonPath("$[*] like_regex \"[0-9]+\"", issues, MAX_PARSE_ERRORS);
        const auto result = ExecuteJsonPath(jsonPath, TValue(dom), TVariablesMap(), &ValueBuilder);
        Y_ABORT_UNLESS(!result.IsError());
    }
}

Y_CPU_BENCHMARK(JsonPathLikeRegex, iface) {
    PREPARE()

    const TString json = GenerateRandomJson();
    const TUnboxedValue dom = TryParseJsonDom(json, &ValueBuilder);

    TIssues issues;
    const auto jsonPath = ParseJsonPath("$[*] like_regex \"[0-9]+\"", issues, MAX_PARSE_ERRORS);
    for (size_t i = 0; i < iface.Iterations(); i++) {
        const auto result = ExecuteJsonPath(jsonPath, TValue(dom), TVariablesMap(), &ValueBuilder);
        Y_ABORT_UNLESS(!result.IsError());
    }
}
