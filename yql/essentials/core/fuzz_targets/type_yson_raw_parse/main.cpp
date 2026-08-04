#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>

#include <library/cpp/yson/node/node.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

namespace {

constexpr size_t MaxInputSize = 64 * 1024;
constexpr ui16 MaxGeneratedDepth = 128;

NYT::TNode MakeDataType() {
    return NYT::TNode().Add("DataType").Add("Int32");
}

NYT::TNode WrapList(NYT::TNode inner) {
    return NYT::TNode().Add("ListType").Add(std::move(inner));
}

NYT::TNode WrapOptional(NYT::TNode inner) {
    return NYT::TNode().Add("OptionalType").Add(std::move(inner));
}

NYT::TNode WrapTagged(NYT::TNode inner) {
    return NYT::TNode().Add("TaggedType").Add("tag").Add(std::move(inner));
}

NYT::TNode MakeNestedTypeNode(FuzzedDataProvider& provider) {
    const ui16 depth = provider.ConsumeIntegralInRange<ui16>(0, MaxGeneratedDepth);
    NYT::TNode type = MakeDataType();

    for (ui16 i = 0; i < depth; ++i) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 2)) {
            case 0:
                type = WrapList(std::move(type));
                break;
            case 1:
                type = WrapOptional(std::move(type));
                break;
            default:
                type = WrapTagged(std::move(type));
                break;
        }
    }

    return type;
}

void ExerciseNode(const NYT::TNode& node) {
    try {
        NYql::TExprContext ctx;
        const auto* type = NYql::NCommon::ParseTypeFromYson(node, ctx);
        if (type) {
            (void)NYql::FormatType(type);
        }
    } catch (...) {
    }
}

void ExerciseRaw(TStringBuf yson) {
    if (yson.size() > MaxInputSize) {
        return;
    }

    try {
        NYql::TExprContext ctx;
        const auto* type = NYql::NCommon::ParseTypeFromYson(yson, ctx);
        if (type) {
            (void)NYql::FormatType(type);
        }
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    const TStringBuf raw(reinterpret_cast<const char*>(data), size);
    ExerciseRaw(raw);

    FuzzedDataProvider provider(data, size);
    ExerciseNode(MakeNestedTypeNode(provider));

    return 0;
}
