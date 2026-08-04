#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/dom/json.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_mem_info.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

#include <variant>

namespace {

constexpr size_t MaxInputSize = 64 * 1024;
constexpr ui16 MaxGeneratedDepth = 128;

TString MakeNestedJson(FuzzedDataProvider& provider) {
    const ui16 depth = provider.ConsumeIntegralInRange<ui16>(0, MaxGeneratedDepth);
    const bool objectShape = provider.ConsumeBool();

    TString result;
    if (!objectShape) {
        result.reserve(size_t(depth) * 2 + 1);
        result += TString(depth, '[');
        result += '0';
        result += TString(depth, ']');
    } else {
        result.reserve(size_t(depth) * 6 + 1);
        for (ui16 i = 0; i < depth; ++i) {
            result += "{\"a\":";
        }
        result += '0';
        result += TString(depth, '}');
    }
    return result;
}

void ExerciseBinaryJson(TStringBuf binaryJson, const NYql::NUdf::IValueBuilder* valueBuilder) {
    if (binaryJson.size() > MaxInputSize) {
        return;
    }

    bool isValid = false;
    try {
        isValid = !NKikimr::NBinaryJson::IsValidBinaryJsonWithError(binaryJson).Defined();
        isValid = NKikimr::NBinaryJson::IsValidBinaryJson(binaryJson) && isValid;
    } catch (...) {
    }

    if (!isValid) {
        return;
    }

    try {
        auto reader = NKikimr::NBinaryJson::TBinaryJsonReader::Make(binaryJson);
        const auto root = reader->GetRootCursor();
        (void)NKikimr::NBinaryJson::SerializeToJson(root);
    } catch (...) {
    }

    try {
        (void)NKikimr::NBinaryJson::SerializeToJson(binaryJson);
    } catch (...) {
    }

    try {
        const auto dom = NKikimr::NBinaryJson::ReadToJsonDom(binaryJson, valueBuilder);
        if (dom) {
            (void)NYql::NDom::SerializeJsonDom(dom);
        }
    } catch (...) {
    }
}

void ExerciseJsonText(TStringBuf json, const NYql::NUdf::IValueBuilder* valueBuilder) {
    if (json.size() > MaxInputSize) {
        return;
    }

    try {
        auto serialized = NKikimr::NBinaryJson::SerializeToBinaryJson(json);
        if (const auto* binaryJson = std::get_if<NKikimr::NBinaryJson::TBinaryJson>(&serialized)) {
            ExerciseBinaryJson(TStringBuf(binaryJson->Data(), binaryJson->Size()), valueBuilder);
        }
    } catch (...) {
    }
}

void Exercise(TStringBuf input) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("BinaryJsonFuzz");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, nullptr);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);

    ExerciseBinaryJson(input, &builder);
    ExerciseJsonText(input, &builder);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    const TStringBuf raw(reinterpret_cast<const char*>(data), size);
    Exercise(raw);

    FuzzedDataProvider provider(data, size);
    const TString generated = MakeNestedJson(provider);
    Exercise(generated);

    return 0;
}
