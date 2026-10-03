#pragma once
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NKqp::NPhysicalConvertionUtils {

TString GetFullName(const TString& name, const TPhysicalNames& names);
TString GetFullName(TInfoUnitId id, const TPhysicalNames& names);

// Returns LiveOut in ascending ID order.
TVector<TInfoUnitId> GetLiveOutputIUs(IOperator& op);

// Returns child-edge LiveIn in ascending ID order.
TVector<TInfoUnitId> GetLiveInputIUs(IOperator& op, ui32 childIndex);

TExprNode::TPtr BuildSwitch(TExprNode::TPtr input, TReplicate& hub, const TPhysicalNames& names, TExprContext& ctx);
TCoAtomList BuildAtomList(TStringBuf value, TPositionHandle pos, TExprContext& ctx);
TExprNode::TPtr ExtractMembers(TExprNode::TPtr input, TExprContext &ctx, const TVector<TInfoUnitId>& members, const TPhysicalNames& names);
TExprNode::TPtr BuildRenameMap(TExprNode::TPtr input, const TVector<std::pair<TString, TString>>& renames, TExprContext& ctx, bool ordered = false);
TExprNode::TPtr ConvertToWideJoinFilter(TExprNode::TPtr input, const TMappedIUs<ui32>& inputs,
                                        const TUnorderedIUs& unwrapOptionalInputs, ui32 width, TExprContext& ctx);
// Substitute only members of this lambda's row argument, not fields of nested
// structs or arguments of nested lambdas. The input map is reusable across RHSs.
TExprNode::TPtr LowerRowLambdaBody(const TExprNode::TPtr& lambda,
    const TMappedIUs<TExprNode::TPtr>& fields, TExprContext& ctx);
TExprNode::TPtr BuildVoidLambda(TExprContext& ctx, TPositionHandle pos);

template <typename T>
THashSet<TString> BuildNameSet(const TVector<T>& columns, const TPhysicalNames& names) {
    THashSet<TString> result;
    for (const auto& column : columns) {
        result.insert(GetFullName(column, names));
    }
    return result;
}

template <typename T>
TExprNode::TPtr BuildExpandMapForNarrowInput(TExprNode::TPtr input, const TVector<T>& inputs, TExprContext& ctx, const TPhysicalNames& names) {
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
                                .Atom(1, GetFullName(inputs[i], names))
                            .Seal();
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
    // clang-format on
}

template <typename T>
TExprNode::TPtr BuildNarrowMapForWideInput(TExprNode::TPtr input, const TVector<T>& inputs, const THashSet<TString>& outputs, TExprContext& ctx, const TPhysicalNames& names) {
    // clang-format off
    return ctx.Builder(input->Pos())
        .Callable("NarrowMap")
            .Add(0, input)
            .Lambda(1)
                .Params("wide_input", inputs.size())
                .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 outIndex = 0;
                    for (ui32 i = 0; i < inputs.size(); ++i) {
                        const auto name = GetFullName(inputs[i], names);
                        if (outputs.contains(name)) {
                            parent.List(outIndex++)
                                .Atom(0, GetFullName(inputs[i], names))
                                .Arg(1, "wide_input", i)
                            .Seal();
                        }
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
TExprNode::TPtr BuildNarrowMapForWideInput(TExprNode::TPtr input, const TVector<T>& inputs, TExprContext& ctx, const TPhysicalNames& names) {
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
                            .Atom(0, GetFullName(inputs[i], names))
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
TExprNode::TPtr BuildNarrowMapForWideInput(TExprNode::TPtr input, const TVector<T>& inputs, const THashMap<ui32, TString>& renameMap, TExprContext& ctx, const TPhysicalNames& names) {
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
                        const auto fullName = it != renameMap.end() ? it->second : GetFullName(inputs[i], names);
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
} // namespace NKikimr::NKqp::NPhysicalConvertionUtils
