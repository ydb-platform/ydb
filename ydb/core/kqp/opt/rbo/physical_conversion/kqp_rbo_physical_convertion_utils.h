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

// Transform one stage output, preserving the other Replicate branches and the row-read stage wrapper.
template <class TTransform>
TExprNode::TPtr TransformStageOutput(TExprNode::TPtr input, TTransform&& transform, std::optional<ui32> outputIndex, TExprContext& ctx) {
    if (TDqPhyStage::Match(input.Get())) {
        const auto program = TDqPhyStage(input).Program().Ptr();
        return ctx.ChangeChild(*input, TDqPhyStage::idx_Program, ctx.ChangeChild(*program, 1,
            TransformStageOutput(program->TailPtr(), transform, outputIndex, ctx)));
    }
    if (outputIndex && input->IsCallable("Switch")) {
        // Switch(input, buffer, [input indexes], lambda, ...).
        const auto index = 3 + 2 * *outputIndex;
        const auto branch = input->ChildPtr(index);
        return ctx.ChangeChild(*input, index, ctx.ChangeChild(*branch, 1, transform(branch->TailPtr())));
    }
    return transform(input);
}

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

// Physical names of `columns`, in order.
TVector<TString> GetFullNames(const TVector<TInfoUnitId>& columns, const TPhysicalNames& names);

/**
 * Rearranges a wide flow so that its slots are exactly `to`, taken from `from` by name.
 * `to` must be a permutation of a subset of `from`.
 */
TExprNode::TPtr BuildWideProjection(TExprNode::TPtr input, const TVector<TString>& from, const TVector<TString>& to, TExprContext& ctx);

/**
 * Body of a physical stage while it is assembled operator by operator.
 *
 * Operators are emitted bottom-up and most of them are wide internally (WideMap,
 * WideFilter, WideSort, WideCombiner). Handing the body between two such operators in
 * wide form avoids emitting a NarrowMap/ExpandMap pair per operator that peephole would
 * only have to fuse away again, which costs one fixpoint round and a full re-annotation
 * of the stage lambda per layer.
 *
 * The narrow form - a stream of structs - is what stage arguments, connections and the
 * operators with no wide counterpart (Switch, Extend, row-read stages) work with.
 */
class TStageBody {
public:
    TStageBody() = default;

    static TStageBody Narrow(TExprNode::TPtr stream) {
        TStageBody body;
        body.NarrowStream = std::move(stream);
        return body;
    }

    // `columns` are the physical names of the slots of `flow`, in order.
    static TStageBody Wide(TExprNode::TPtr flow, TVector<TString> columns) {
        TStageBody body;
        body.WideFlow = std::move(flow);
        body.WideColumns = std::move(columns);
        return body;
    }

    explicit operator bool() const {
        return bool(NarrowStream) || bool(WideFlow);
    }

    bool IsWide() const {
        return bool(WideFlow);
    }

    // Wide flow whose slots are exactly `columns`. Expands the narrow form, or permutes
    // the wide one, only when the layout does not already match.
    TExprNode::TPtr AsWide(const TVector<TString>& columns, TExprContext& ctx, const TPhysicalNames& names) const;
    TExprNode::TPtr AsWide(const TVector<TInfoUnitId>& columns, TExprContext& ctx, const TPhysicalNames& names) const;

    // Stream of structs holding the columns this body currently carries.
    TExprNode::TPtr AsNarrow(TExprContext& ctx) const;

private:
    TExprNode::TPtr NarrowStream;
    TExprNode::TPtr WideFlow;
    TVector<TString> WideColumns;
};

// Wide body of the `live` slots of `flow`, laid out as `columns`. Wide operators pass every
// input slot through, so this drops the dead ones; the projection is emitted only when
// something is actually dropped.
TStageBody BuildLiveWideBody(TExprNode::TPtr flow, const TVector<TString>& columns, const THashSet<TString>& live, TExprContext& ctx);

// Transform one stage output of a body being assembled. Row-read stage wrappers and Switch
// branches are narrow, so the transform gets a narrow body there; any other body is passed as is.
template <class TTransform>
TStageBody TransformStageOutput(const TStageBody& input, TTransform&& transform, std::optional<ui32> outputIndex, TExprContext& ctx) {
    if (input.IsWide()) {
        return transform(input);
    }
    auto narrow = input.AsNarrow(ctx);
    if (!TDqPhyStage::Match(narrow.Get()) && !(outputIndex && narrow->IsCallable("Switch"))) {
        return transform(input);
    }
    return TStageBody::Narrow(TransformStageOutput(std::move(narrow), [&](TExprNode::TPtr body) {
        return transform(TStageBody::Narrow(std::move(body))).AsNarrow(ctx);
    }, outputIndex, ctx));
}

} // namespace NKikimr::NKqp::NPhysicalConvertionUtils
