#include <yql/essentials/core/poly_args/yql_poly_args.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

namespace {

constexpr size_t MaxInputSize = 64 * 1024;
constexpr ui16 MaxGeneratedDepth = 128;

NYT::TNode MakeDataType(TStringBuf name) {
    return NYT::TNode::CreateList().Add("DataType").Add(TString(name));
}

NYT::TNode MakeTypePredicate() {
    auto predicate = NYT::TNode::CreateMap();
    predicate["cmd"] = "type";
    predicate["arg"] = "T0";
    predicate["value"] = MakeDataType("Int32");
    return predicate;
}

NYT::TNode WrapAnd(NYT::TNode child) {
    return NYT::TNode::CreateList().Add(std::move(child));
}

NYT::TNode WrapOr(NYT::TNode child) {
    auto children = NYT::TNode::CreateList();
    children.Add(std::move(child));

    auto predicate = NYT::TNode::CreateMap();
    predicate["cmd"] = "or";
    predicate["value"] = std::move(children);
    return predicate;
}

NYT::TNode MakeNestedPredicate(FuzzedDataProvider& provider) {
    const ui16 depth = provider.ConsumeIntegralInRange<ui16>(0, MaxGeneratedDepth);
    NYT::TNode predicate = MakeTypePredicate();
    for (ui16 i = 0; i < depth; ++i) {
        predicate = provider.ConsumeBool() ? WrapOr(std::move(predicate)) : WrapAnd(std::move(predicate));
    }
    return predicate;
}

NYT::TNode MakeConfig(NYT::TNode predicate) {
    auto action = NYT::TNode::CreateMap();

    auto rule = NYT::TNode::CreateList();
    rule.Add(std::move(predicate)).Add(action);

    auto fallback = NYT::TNode::CreateList();
    fallback.Add(NYT::TNode::CreateList()).Add(NYT::TNode::CreateMap());

    auto config = NYT::TNode::CreateList();
    config.Add(std::move(rule)).Add(std::move(fallback));
    return config;
}

void Exercise(const NYT::TNode& config) {
    try {
        auto polyArgs = NYql::ParsePolyArgs(config);
        NYql::IPolyArgs::TArgs args;
        args["T0"] = MakeDataType("Int32");
        (void)polyArgs->GetPredicatesCount();
        (void)polyArgs->GetUnresolvedInput(0);
        (void)polyArgs->Match(args, NYql::GetMaxLangVersion());
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
    Exercise(MakeConfig(MakeNestedPredicate(provider)));

    return 0;
}
