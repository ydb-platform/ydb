#include "mkql_pattern_test_utils.h"

#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/comp_nodes/test_support/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <util/datetime/cputimer.h>

namespace NKikimr::NMiniKQL::NTest {

TComputationNodeFactory GetListTestFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestList") {
            return new TExternalComputationNode(ctx.Mutables);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

TRuntimeNode CreateFlow(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    if (list) {
        return pb.ToFlow(TRuntimeNode(list, /*isImmediate=*/false), {});
    } else {
        std::vector<const TRuntimeNode> arr;
        arr.reserve(vecSize);
        for (ui64 i = 0; i < vecSize; ++i) {
            arr.push_back(NTest::ConvertValueToLiteralNode(pb, ui64((i + 124515) % 6740234)));
        }
        TArrayRef<const TRuntimeNode> arrRef(std::move(arr));
        return pb.ToFlow(pb.AsList(arrRef), {});
    }
}

template <>
TRuntimeNode CreateFilter<false>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode node) -> TRuntimeNode {
        return pb.AggrEquals(
            pb.Mod(node, NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(128))),
            NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(0)));
    };
    return pb.Filter(flow, handler);
}

template <>
TRuntimeNode CreateFilter<true>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode::TList node) -> TRuntimeNode {
        return pb.AggrEquals(
            pb.Mod(node.front(), NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(128))),
            NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(0)));
    };
    return pb.NarrowMap(
        pb.WideFilter(
            pb.ExpandMap(flow,
                         [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            handler),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); });
}

} // namespace NKikimr::NMiniKQL::NTest
