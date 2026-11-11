#include <yql/essentials/minikql/dom/json.h>
#include <yql/essentials/minikql/jsonpath/jsonpath.h>

#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_mem_info.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <library/cpp/json/json_value.h>
#include <benchmark/benchmark.h>

#include <util/generic/yexception.h>

#include <util/random/fast.h>

using namespace NJson;

using namespace NYql;
using namespace NYql::NDom;
using namespace NYql::NUdf;
using namespace NYql::NJsonPath;
using namespace NJson;
using namespace NKikimr::NMiniKQL;

namespace {

TString RandomString(ui32 min, ui32 max) {
    static TReallyFastRng32 Rand(0);
    TString result;
    const ui32 length = Rand.Uniform(min, max + 1);
    result.reserve(length);
    for (ui32 i = 0; i < length; ++i) {
        result.push_back(char(Rand.Uniform('a', 'z' + 1)));
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

#define PREPARE()                                                                                       \
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry())); \
    TScopedAlloc Alloc(__LOCATION__);                                                                   \
    TTypeEnvironment Env(Alloc);                                                                        \
    TMemoryUsageInfo MemInfo("Memory");                                                                 \
    THolderFactory HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get());                         \
    TDefaultValueBuilder ValueBuilder(HolderFactory);

void JsonPath(benchmark::State& state) {
    PREPARE()

    const TString json = GenerateRandomJson();
    const TUnboxedValue dom = TryParseJsonDom(json, &ValueBuilder);

    for (auto _ : state) {
        TIssues issues;
        const auto jsonPath = ParseJsonPath("$.'_id'.issueId", issues, MAX_PARSE_ERRORS);
        auto result = ExecuteJsonPath(jsonPath, TValue(dom), TVariablesMap(), &ValueBuilder);
        Y_ENSURE(!result.IsError(), "JSONPath execution failed");
        benchmark::DoNotOptimize(result);
    }
}
BENCHMARK(JsonPath);

void JsonPathLikeRegexWithCompile(benchmark::State& state) {
    PREPARE()

    const TString json = GenerateRandomJson();
    const TUnboxedValue dom = TryParseJsonDom(json, &ValueBuilder);

    for (auto _ : state) {
        TIssues issues;
        const auto jsonPath = ParseJsonPath("$[*] like_regex \"[0-9]+\"", issues, MAX_PARSE_ERRORS);
        auto result = ExecuteJsonPath(jsonPath, TValue(dom), TVariablesMap(), &ValueBuilder);
        Y_ENSURE(!result.IsError(), "JSONPath execution failed");
        benchmark::DoNotOptimize(result);
    }
}
BENCHMARK(JsonPathLikeRegexWithCompile);

void JsonPathLikeRegex(benchmark::State& state) {
    PREPARE()

    const TString json = GenerateRandomJson();
    const TUnboxedValue dom = TryParseJsonDom(json, &ValueBuilder);

    TIssues issues;
    const auto jsonPath = ParseJsonPath("$[*] like_regex \"[0-9]+\"", issues, MAX_PARSE_ERRORS);
    for (auto _ : state) {
        auto result = ExecuteJsonPath(jsonPath, TValue(dom), TVariablesMap(), &ValueBuilder);
        Y_ENSURE(!result.IsError(), "JSONPath execution failed");
        benchmark::DoNotOptimize(result);
    }
}
BENCHMARK(JsonPathLikeRegex);

} // namespace
