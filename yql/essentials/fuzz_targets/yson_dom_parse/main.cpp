#include <yql/essentials/minikql/dom/yson.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_mem_info.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

namespace {

constexpr size_t MaxInputSize = 64 * 1024;
constexpr ui16 MaxGeneratedDepth = 128;

TString MakeNestedYson(FuzzedDataProvider& provider) {
    const ui16 depth = provider.ConsumeIntegralInRange<ui16>(0, MaxGeneratedDepth);
    const ui8 shape = provider.ConsumeIntegralInRange<ui8>(0, 2);

    TString result;
    if (shape == 0) {
        result.reserve(size_t(depth) * 2 + 1);
        result += TString(depth, '[');
        result += '#';
        result += TString(depth, ']');
    } else if (shape == 1) {
        result.reserve(size_t(depth) * 5 + 1);
        for (ui16 i = 0; i < depth; ++i) {
            result += "{k=";
        }
        result += '#';
        result += TString(depth, '}');
    } else {
        result.reserve(size_t(depth) * 7 + 1);
        for (ui16 i = 0; i < depth; ++i) {
            result += "<k=#>";
        }
        result += '#';
    }
    return result;
}

void Exercise(TStringBuf yson) {
    if (yson.size() > MaxInputSize) {
        return;
    }

    try {
        (void)NYql::NDom::IsValidYson(yson);
    } catch (...) {
    }

    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("YsonDomFuzz");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, nullptr);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);

    try {
        const auto dom = NYql::NDom::TryParseYsonDom(yson, &builder);
        if (dom) {
            (void)NYql::NDom::SerializeYsonDomToBinary(dom);
            (void)NYql::NDom::SerializeYsonDomToText(dom);
            (void)NYql::NDom::SerializeYsonDomToPrettyText(dom);
        }
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    const TStringBuf raw(reinterpret_cast<const char*>(data), size);
    Exercise(raw);

    FuzzedDataProvider provider(data, size);
    const TString generated = MakeNestedYson(provider);
    Exercise(generated);

    return 0;
}
