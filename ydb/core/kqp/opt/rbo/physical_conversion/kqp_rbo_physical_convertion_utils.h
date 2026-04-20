#pragma once
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NKqp::NPhysicalConvertionUtils {

TString GetFullName(const TString& name);
TString GetFullName(const TInfoUnit& name);

TExprNode::TPtr BuildMultiConsumerHandler(TExprNode::TPtr input, const ui32 numConsumers, TExprContext& ctx, TPositionHandle pos);
bool IsMultiConsumerHandlerNeeded(const TIntrusivePtr<IOperator>& op);

TCoAtomList BuildAtomList(TStringBuf value, TPositionHandle pos, TExprContext& ctx);

TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext &ctx, bool removeAliases = false);
TExprNode::TPtr ExtractMembers(TExprNode::TPtr input, TExprContext &ctx, TVector<TInfoUnit> members);

template <typename T>
TExprNode::TPtr BuildExpandMapForNarrowInput(TExprNode::TPtr input, const TVector<T>& inputs, TExprContext& ctx) {
    // clang-format off
    return ctx.Builder(input->Pos())
        .Callable("ExpandMap")
            .Add(0, input)
            .Lambda(1)
                .Param("narrow_input_param")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < inputs.size(); ++i) {
                        parent
                            .Callable(i, "Member")
                                .Arg(0, "narrow_input_param")
                                .Atom(1, GetFullName(inputs[i]))
                            .Seal();
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
    // clang-format on
}

template <typename T>
TExprNode::TPtr BuildNarrowMapForWideInput(TExprNode::TPtr input, const TVector<T>& inputs, TExprContext& ctx) {
    // clang-format off
    return ctx.Builder(input->Pos())
        .Callable("NarrowMap")
            .Add(0, input)
            .Lambda(1)
                .Params("wide_input", inputs.size())
                .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < inputs.size(); ++i) {
                        parent.List(i)
                            .Atom(0, GetFullName(inputs[i]))
                            .Arg(1, "wide_input", i)
                        .Seal();
                    }
                    return parent;
                })
                .Seal()
            .Seal()
        .Seal()
    .Build();
    // clang-format on
}

template <typename T>
TExprNode::TPtr BuildNarrowMapForWideInput(TExprNode::TPtr input, const TVector<T>& inputs, const THashMap<ui32, TString>& renameMap, TExprContext& ctx) {
    // clang-format off
    return ctx.Builder(input->Pos())
        .Callable("NarrowMap")
            .Add(0, input)
            .Lambda(1)
                .Params("wide_input", inputs.size())
                .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < inputs.size(); ++i) {
                        auto it = renameMap.find(i);
                        const auto fullName = it != renameMap.end() ? it->second : GetFullName(inputs[i]);
                        parent.List(i)
                            .Atom(0, fullName)
                            .Arg(1, "wide_input", i)
                        .Seal();
                    }
                    return parent;
                })
                .Seal()
            .Seal()
        .Seal()
    .Build();
    // clang-format on
}
}
