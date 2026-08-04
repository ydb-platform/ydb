#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t MaxString = 24;
constexpr size_t MaxExprNodes = 64;

TString ToTString(std::string value) {
    return TString(value.data(), value.size());
}

TString ConsumeString(FuzzedDataProvider& fdp) {
    const size_t limit = std::min(MaxString, fdp.remaining_bytes());
    const size_t size = fdp.ConsumeIntegralInRange<size_t>(0, limit);
    return ToTString(fdp.ConsumeBytesAsString(size));
}

void CheckExprNode(const NYql::TExprNode::TPtr& node) {
    Y_ABORT_UNLESS(node);
    if (node->IsAtom()) {
        Y_ABORT_UNLESS(node->ChildrenSize() == 0);
    }
    if (node->IsList() || node->IsCallable() || node->IsLambda() || node->IsArguments()) {
        for (size_t i = 0; i < node->ChildrenSize(); ++i) {
            Y_ABORT_UNLESS(node->ChildPtr(i));
        }
    }
}

void ExerciseExprContext(FuzzedDataProvider& fdp) {
    NYql::TExprContext ctx;
    const auto pos = ctx.AppendPosition(NYql::TPosition(1, 1, "p2_container_stateful_fuzz"));
    std::vector<NYql::TExprNode::TPtr> nodes;
    nodes.reserve(MaxExprNodes);

    const auto* boolType = ctx.MakeType<NYql::TDataExprType>(NYql::NUdf::EDataSlot::Bool);
    const auto* boolTypeAgain = ctx.MakeType<NYql::TDataExprType>(NYql::NUdf::EDataSlot::Bool);
    Y_ABORT_UNLESS(boolType == boolTypeAgain);
    const auto* optBoolType = ctx.MakeType<NYql::TOptionalExprType>(boolType);
    Y_ABORT_UNLESS(optBoolType == ctx.MakeType<NYql::TOptionalExprType>(boolType));

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 9)) {
            case 0:
                nodes.push_back(ctx.NewAtom(pos, ConsumeString(fdp), fdp.ConsumeBool() ? NYql::TNodeFlags::Default : NYql::TNodeFlags::ArbitraryContent));
                break;
            case 1:
                nodes.push_back(ctx.NewAtom(pos, fdp.ConsumeIntegralInRange<ui32>(0, 64)));
                break;
            case 2: {
                NYql::TExprNode::TListType children;
                const size_t count = nodes.empty() ? 0 : fdp.ConsumeIntegralInRange<size_t>(0, std::min<size_t>(4, nodes.size()));
                for (size_t j = 0; j < count; ++j) {
                    children.push_back(nodes[fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1)]);
                }
                nodes.push_back(ctx.NewList(pos, std::move(children)));
                break;
            }
            case 3: {
                NYql::TExprNode::TListType children;
                const size_t count = nodes.empty() ? 0 : fdp.ConsumeIntegralInRange<size_t>(0, std::min<size_t>(4, nodes.size()));
                for (size_t j = 0; j < count; ++j) {
                    children.push_back(nodes[fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1)]);
                }
                nodes.push_back(ctx.NewCallable(pos, ConsumeString(fdp), std::move(children)));
                break;
            }
            case 4:
                if (!nodes.empty()) {
                    auto base = nodes[fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1)];
                    if (base->IsList() || base->IsCallable() || base->IsArguments()) {
                        NYql::TExprNode::TListType children;
                        const size_t count = fdp.ConsumeIntegralInRange<size_t>(0, std::min<size_t>(4, nodes.size()));
                        for (size_t j = 0; j < count; ++j) {
                            children.push_back(nodes[fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1)]);
                        }
                        nodes.push_back(ctx.ChangeChildren(*base, std::move(children)));
                    }
                }
                break;
            case 5:
                if (!nodes.empty()) {
                    NYql::TNodeOnNodeOwnedMap clones;
                    nodes.push_back(ctx.DeepCopy(*nodes[fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1)], ctx, clones, true, true));
                }
                break;
            case 6:
                if (!nodes.empty()) {
                    const NYql::TTypeAnnotationNode* type = fdp.ConsumeBool() ? static_cast<const NYql::TTypeAnnotationNode*>(boolType) : optBoolType;
                    nodes[fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1)]->SetTypeAnn(type);
                }
                break;
            case 7: {
                auto arg = ctx.NewArgument(pos, "arg");
                auto args = ctx.NewArguments(pos, {arg});
                auto body = nodes.empty() ? arg : nodes[fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1)];
                nodes.push_back(ctx.NewLambda(pos, std::move(args), std::move(body)));
                break;
            }
            case 8:
                nodes.push_back(ctx.NewWorld(pos));
                break;
            default:
                if (!nodes.empty()) {
                    CheckExprNode(nodes[fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1)]);
                }
                break;
        }
        if (nodes.size() > MaxExprNodes) {
            nodes.erase(nodes.begin(), nodes.begin() + (nodes.size() - MaxExprNodes));
        }
    }

    for (const auto& node : nodes) {
        CheckExprNode(node);
    }
    {
        NYql::TExprContext::TFreezeGuard freezeGuard(ctx);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    ExerciseExprContext(fdp);

    return 0;
}
