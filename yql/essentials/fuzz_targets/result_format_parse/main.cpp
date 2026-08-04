#include <yql/essentials/public/result_format/yql_result_format_data.h>
#include <yql/essentials/public/result_format/yql_result_format_response.h>
#include <yql/essentials/public/result_format/yql_result_format_type.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <array>

namespace {

constexpr size_t MaxInputSize = 8 * 1024;

TString PrimitiveType(FuzzedDataProvider& provider) {
    static constexpr std::array<TStringBuf, 10> types = {
        "\"Bool\"", "\"Int32\"", "\"Uint64\"", "\"Double\"", "\"String\"",
        "\"Utf8\"", "\"Json\"", "\"Yson\"", "\"Date\"", "\"Timestamp\"",
    };
    return TString(provider.PickValueInArray(types));
}

TString BuildType(FuzzedDataProvider& provider, ui32 depth = 0) {
    if (depth >= 3) {
        return PrimitiveType(provider);
    }

    switch (provider.ConsumeIntegralInRange<ui8>(0, 6)) {
        case 0:
            return PrimitiveType(provider);
        case 1:
            return "[" + BuildType(provider, depth + 1) + ";\"Optional\"]";
        case 2:
            return "[" + BuildType(provider, depth + 1) + ";\"List\"]";
        case 3:
            return "[[" + BuildType(provider, depth + 1) + ";" + BuildType(provider, depth + 1) + "];\"Tuple\"]";
        case 4:
            return "[[[\"a\";" + BuildType(provider, depth + 1) + "];[\"b\";" + BuildType(provider, depth + 1) + "]];\"Struct\"]";
        case 5:
            return "[" + BuildType(provider, depth + 1) + ";" + BuildType(provider, depth + 1) + ";\"Dict\"]";
        case 6:
            return "[\"Decimal\";\"10\";\"2\"]";
    }
    return PrimitiveType(provider);
}

TString BuildData(FuzzedDataProvider& provider) {
    switch (provider.ConsumeIntegralInRange<ui8>(0, 7)) {
        case 0: return "%true";
        case 1: return ToString(provider.ConsumeIntegral<i32>());
        case 2: return "\"" + provider.ConsumeBytesAsString(provider.ConsumeIntegralInRange<size_t>(0, 32)) + "\"";
        case 3: return "#";
        case 4: return "[]";
        case 5: return "[1;2;3]";
        case 6: return "{a=1;b=\"x\"}";
        case 7: return "[\"0\";\"variant\"]";
    }
    return "#";
}

void ExerciseNode(const TString& yson) {
    if (yson.size() > MaxInputSize) {
        return;
    }

    try {
        auto node = NYT::NodeFromYsonString(yson);
        try {
            NYql::NResult::TEmptyTypeVisitor typeVisitor;
            NYql::NResult::ParseType(node, typeVisitor);
        } catch (...) {
        }
        try {
            (void)NYql::NResult::ParseResponse(node);
        } catch (...) {
        }
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    const TString raw(reinterpret_cast<const char*>(data), size);
    ExerciseNode(raw);

    FuzzedDataProvider provider(data, size);
    const TString type = BuildType(provider);
    const TString dataYson = BuildData(provider);
    ExerciseNode(type);

    try {
        const auto typeNode = NYT::NodeFromYsonString(type);
        const auto dataNode = NYT::NodeFromYsonString(dataYson);
        NYql::NResult::TEmptyDataVisitor dataVisitor;
        NYql::NResult::ParseData(typeNode, dataNode, dataVisitor);
    } catch (...) {
    }

    const TString response = "[{Write=[{Type=" + type + ";Data=" + dataYson + ";}];}]";
    ExerciseNode(response);
    return 0;
}
