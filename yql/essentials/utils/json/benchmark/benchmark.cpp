#include <benchmark/benchmark.h>

#include <yql/essentials/utils/json/reflection.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#define DEFINE_BENCHMARK_MODEL(ns)    \
    namespace ns {                    \
                                      \
    enum class EPriority {            \
        Low,                          \
        Medium,                       \
        High,                         \
    };                                \
                                      \
    struct TTag {                     \
        i64 Id;                       \
        TString Label;                \
    };                                \
                                      \
    struct TDocument {                \
        ui64 DocumentId;              \
        TString Title;                \
        EPriority Priority;           \
        TVector<TTag> Tags;           \
        TMaybe<TString> Description;  \
        TMaybe<i64> Rating;           \
    };                                \
                                      \
    struct TLibrary {                 \
        TString LibraryName;          \
        TVector<TDocument> Documents; \
    };                                \
                                      \
    } // namespace ns

#define DECLARE_BENCHMARK_JSON(ns)          \
    JSON_DECLARE_FROM(ns::EPriority, json); \
    JSON_DECLARE_TO(ns::EPriority, value);  \
    JSON_DECLARE_FROM(ns::TTag, json);      \
    JSON_DECLARE_TO(ns::TTag, value);       \
    JSON_DECLARE_FROM(ns::TDocument, json); \
    JSON_DECLARE_TO(ns::TDocument, value);  \
    JSON_DECLARE_FROM(ns::TLibrary, json);  \
    JSON_DECLARE_TO(ns::TLibrary, value)

DEFINE_BENCHMARK_MODEL(NReflecting)
DEFINE_BENCHMARK_MODEL(NManual)

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NReflecting::TTag, (Id)(Label));
YQL_DEFINE_REFLECTING(NReflecting::TDocument, (DocumentId)(Title)(Priority)(Tags)(Description)(Rating));
YQL_DEFINE_REFLECTING(NReflecting::TLibrary, (LibraryName)(Documents));

} // namespace NYql::NReflection

namespace NYql::NJson {

DECLARE_BENCHMARK_JSON(NReflecting);
DECLARE_BENCHMARK_JSON(NManual);

YQL_DERIVE_JSON_FROM(NReflecting::TTag);
YQL_DERIVE_JSON_TO(NReflecting::TTag);
YQL_DERIVE_JSON_FROM(NReflecting::TDocument);
YQL_DERIVE_JSON_TO(NReflecting::TDocument);
YQL_DERIVE_JSON_FROM(NReflecting::TLibrary);
YQL_DERIVE_JSON_TO(NReflecting::TLibrary);

JSON_DEFINE_FROM(NReflecting::EPriority, json) {
    if (!json.IsString()) {
        return Unexpected("must be a string");
    }

    const TString& value = json.GetStringSafe();
    if (value == "Low") {
        return NReflecting::EPriority::Low;
    }
    if (value == "Medium") {
        return NReflecting::EPriority::Medium;
    }
    if (value == "High") {
        return NReflecting::EPriority::High;
    }

    return Unexpected(TStringBuilder() << "unknown priority: " << value);
}

JSON_DEFINE_TO(NReflecting::EPriority, value) {
    switch (value) {
        case NReflecting::EPriority::Low:
            return "Low";
        case NReflecting::EPriority::Medium:
            return "Medium";
        case NReflecting::EPriority::High:
            return "High";
    }
}

JSON_DEFINE_FROM(NManual::EPriority, json) {
    if (!json.IsString()) {
        return Unexpected("must be a string");
    }

    const TString& value = json.GetStringSafe();
    if (value == "Low") {
        return NManual::EPriority::Low;
    }
    if (value == "Medium") {
        return NManual::EPriority::Medium;
    }
    if (value == "High") {
        return NManual::EPriority::High;
    }

    return Unexpected(TStringBuilder() << "unknown priority: " << value);
}

JSON_DEFINE_TO(NManual::EPriority, value) {
    switch (value) {
        case NManual::EPriority::Low:
            return "Low";
        case NManual::EPriority::Medium:
            return "Medium";
        case NManual::EPriority::High:
            return "High";
    }
}

JSON_DEFINE_FROM(NManual::TTag, json) {
    NManual::TTag value;
    JSON_MOVE_FROM(json, "id", value.Id);
    JSON_MOVE_FROM(json, "label", value.Label);
    return value;
}

JSON_DEFINE_TO(NManual::TTag, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "id", value.Id);
    SaveTo(json, "label", std::move(value.Label));
    return json;
}

JSON_DEFINE_FROM(NManual::TDocument, json) {
    NManual::TDocument value;
    JSON_MOVE_FROM(json, "documentId", value.DocumentId);
    JSON_MOVE_FROM(json, "title", value.Title);
    JSON_MOVE_FROM(json, "priority", value.Priority);
    JSON_MOVE_FROM(json, "tags", value.Tags);
    JSON_MOVE_FROM(json, "description", value.Description);
    JSON_MOVE_FROM(json, "rating", value.Rating);
    return value;
}

JSON_DEFINE_TO(NManual::TDocument, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "documentId", value.DocumentId);
    SaveTo(json, "title", std::move(value.Title));
    SaveTo(json, "priority", value.Priority);
    SaveTo(json, "tags", std::move(value.Tags));
    SaveTo(json, "description", std::move(value.Description));
    SaveTo(json, "rating", value.Rating);
    return json;
}

JSON_DEFINE_FROM(NManual::TLibrary, json) {
    NManual::TLibrary value;
    JSON_MOVE_FROM(json, "libraryName", value.LibraryName);
    JSON_MOVE_FROM(json, "documents", value.Documents);
    return value;
}

JSON_DEFINE_TO(NManual::TLibrary, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "libraryName", std::move(value.LibraryName));
    SaveTo(json, "documents", std::move(value.Documents));
    return json;
}

} // namespace NYql::NJson

namespace {

constexpr TStringBuf JsonInput = R"json({
    "libraryName": "Corporate Document Archive",
    "documents": [
        {
            "documentId": 10203040506070809,
            "title": "System Architecture Overview",
            "priority": "High",
            "tags": [
                {"id": 1001, "label": "architecture"},
                {"id": 1002, "label": "infrastructure"}
            ],
            "description": "A high-level overview of the distributed system architecture.",
            "rating": 5
        },
        {
            "documentId": 10203040506070810,
            "title": "Weekly Team Notes",
            "priority": "Low",
            "tags": []
        },
        {
            "documentId": 9988776655443322,
            "title": "Q3 Financial Report",
            "priority": "Medium",
            "tags": [
                {"id": -500, "label": "confidential"}
            ],
            "description": "",
            "rating": -1
        }
    ]
})json";

template <typename TLibrary>
TLibrary ParseLibrary(const NJson::TJsonValue& json) {
    auto library = NYql::NJson::FromJson<TLibrary>(json);
    Y_ENSURE(library, library.error());
    return std::move(*library);
}

NJson::TJsonValue ParseJsonInput() {
    NJson::TJsonValue json;
    Y_ENSURE(ReadJsonTree(JsonInput, &json, /*throwOnError=*/true));
    return json;
}

template <typename TLibrary>
void BenchmarkFromJson(benchmark::State& state) {
    const auto json = ParseJsonInput();

    for (const auto _ : state) {
        auto library = NYql::NJson::FromJson<TLibrary>(json);
        benchmark::DoNotOptimize(*library);
        benchmark::ClobberMemory();
    }
}

template <typename TLibrary>
void BenchmarkToJson(benchmark::State& state) {
    const auto json = ParseJsonInput();
    const auto source = ParseLibrary<TLibrary>(json);

    for (const auto _ : state) {
        state.PauseTiming();
        auto library = source;
        state.ResumeTiming();

        auto json = NYql::NJson::ToJson(std::move(library));
        benchmark::DoNotOptimize(json);
        benchmark::ClobberMemory();
    }
}

void BenchmarkFromJsonReflecting(benchmark::State& state) {
    BenchmarkFromJson<NReflecting::TLibrary>(state);
}

void BenchmarkToJsonReflecting(benchmark::State& state) {
    BenchmarkToJson<NReflecting::TLibrary>(state);
}

void BenchmarkFromJsonManual(benchmark::State& state) {
    BenchmarkFromJson<NManual::TLibrary>(state);
}

void BenchmarkToJsonManual(benchmark::State& state) {
    BenchmarkToJson<NManual::TLibrary>(state);
}

} // namespace

BENCHMARK(BenchmarkFromJsonReflecting);
BENCHMARK(BenchmarkFromJsonManual);
BENCHMARK(BenchmarkToJsonReflecting);
BENCHMARK(BenchmarkToJsonManual);
