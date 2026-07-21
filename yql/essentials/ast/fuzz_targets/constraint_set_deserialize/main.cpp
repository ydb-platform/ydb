#include <yql/essentials/ast/yql_expr.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

namespace {

constexpr size_t MaxInputSize = 64 * 1024;
constexpr ui16 MaxGeneratedDepth = 128;

NYT::TNode MakeLeafConstraintSet() {
    auto set = NYT::TNode::CreateMap();
    set["Empty"] = NYT::TNode::CreateEntity();
    return set;
}

NYT::TNode WrapMulti(NYT::TNode inner, ui32 index) {
    auto item = NYT::TNode::CreateList();
    item.Add(index).Add(std::move(inner));

    auto multi = NYT::TNode::CreateList();
    multi.Add(std::move(item));

    auto set = NYT::TNode::CreateMap();
    set["Multi"] = std::move(multi);
    return set;
}

NYT::TNode MakeNestedMulti(FuzzedDataProvider& provider) {
    const ui16 depth = provider.ConsumeIntegralInRange<ui16>(0, MaxGeneratedDepth);
    NYT::TNode set = MakeLeafConstraintSet();
    for (ui16 i = 0; i < depth; ++i) {
        set = WrapMulti(std::move(set), provider.ConsumeIntegral<ui32>());
    }
    return set;
}

void Exercise(const NYT::TNode& serialized) {
    try {
        NYql::TExprContext ctx;
        const auto constraints = ctx.MakeConstraintSet(serialized);
        (void)constraints.ToYson();
    } catch (...) {
    }
}

void ExerciseRaw(TStringBuf raw) {
    if (raw.size() > MaxInputSize) {
        return;
    }

    try {
        Exercise(NYT::NodeFromYsonString(TString(raw)));
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    ExerciseRaw(TStringBuf(reinterpret_cast<const char*>(data), size));

    FuzzedDataProvider provider(data, size);
    Exercise(MakeNestedMulti(provider));

    return 0;
}
