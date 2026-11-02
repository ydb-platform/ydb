#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NKqp::NPhysicalConvertionUtils {
TString GetFullName(const TString& name, const TPhysicalNames&) {
    return name;
}

TString GetFullName(TInfoUnitId id, const TPhysicalNames& names) {
    return names.Get(id);
}

TVector<TInfoUnitId> GetLiveOutputIUs(IOperator& op) {
    const auto outputIUs = op.GetOutputIUs();
    const auto& liveOut = GetLiveOut(&op);
    TVector<TInfoUnitId> liveOutputIUs;
    liveOutputIUs.reserve(outputIUs.Size());
    for (const auto& output : outputIUs) {
        if (liveOut.Contains(output)) {
            liveOutputIUs.push_back(output);
        }
    }
    return liveOutputIUs;
}

TVector<TInfoUnitId> GetLiveInputIUs(IOperator& op, ui32 childIndex) {
    Y_ENSURE(childIndex < op.GetChildCount());
    const auto outputIUs = op.GetChild(childIndex)->GetOutputIUs();
    const auto& liveIn = GetLiveIn(&op, childIndex);

    TVector<TInfoUnitId> liveInputIUs;
    liveInputIUs.reserve(outputIUs.Size());
    for (const auto& output : outputIUs) {
        if (liveIn.Contains(output)) {
            liveInputIUs.push_back(output);
        }
    }
    return liveInputIUs;
}

TCoAtomList BuildAtomList(TStringBuf value, TPositionHandle pos, TExprContext& ctx) {
    // clang-format off
    return Build<TCoAtomList>(ctx, pos)
        .Add<TCoAtom>()
            .Value(value)
            .Build()
    .Done();
    // clang-format on
}

TExprNode::TPtr BuildSwitch(TExprNode::TPtr input, TReplicate& hub, const TPhysicalNames& names, TExprContext& ctx) {
    if (TDqPhyStage::Match(input.Get())) {
        const auto program = TDqPhyStage(input).Program().Ptr();
        return ctx.ChangeChild(*input, TDqPhyStage::idx_Program,
            ctx.ChangeChild(*program, 1, BuildSwitch(program->TailPtr(), hub, names, ctx)));
    }
    const auto pos = hub.Pos;
    const auto& outputs = hub.GetOutputs();
    TVector<TOpReplicate*> ports(outputs.size());
    for (auto* port : outputs) {
        ports.at(*GetReplicateOutputIndex(*port)) = port;
    }
    Y_ENSURE(!ports.empty());
    auto buildBranch = [&](TExprNode::TPtr stream, TOpReplicate& port) {
        TVector<std::pair<TString, TString>> columns;
        const auto& live = GetLiveOut(&port);
        const auto& rebindings = port.GetRebindings();
        for (const auto source : hub.GetInput()->GetOutputIUs()) {
            const auto output = port.IsPrimary() ? source : *rebindings.Find(source);
            if (live.Contains(output)) {
                columns.emplace_back(names.Get(source), names.Get(output));
            }
        }
        return BuildRenameMap(stream, columns, ctx, /*ordered=*/true);
    };
    if (ports.size() == 1) {
        return buildBranch(input, *ports.front());
    }
    TVector<TExprBase> branches;
    auto inputIndex = BuildAtomList("0", pos, ctx);
    for (auto* port : ports) {
        branches.emplace_back(inputIndex);
        auto arg = ctx.NewArgument(pos, "branch");
        branches.emplace_back(ctx.NewLambda(pos, ctx.NewArguments(pos, {arg}), buildBranch(arg, *port)));
    }

    // clang-format off
    return Build<TCoSwitch>(ctx, pos)
        .Input(input)
        .BufferBytes()
            .Value(ToString(128_MB))
        .Build()
        .FreeArgs()
            .Add(branches)
        .Build()
     .Done().Ptr();
     // clang-format on
}

TExprNode::TPtr ExtractMembers(TExprNode::TPtr input, TExprContext &ctx, const TVector<TInfoUnitId>& members, const TPhysicalNames& names) {
    TVector<TCoAtom> memberAtoms;
    memberAtoms.reserve(members.size());
    for (const auto& iu : members) {
        memberAtoms.push_back(Build<TCoAtom>(ctx, input->Pos())
            .Value(names.Get(iu))
        .Done());
    }

    // clang-format off
    return Build<TCoExtractMembers>(ctx, input->Pos())
        .Input(input)
        .Members<TCoAtomList>()
            .Add(memberAtoms)
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr BuildRenameMap(TExprNode::TPtr input, const TVector<std::pair<TString, TString>>& renames, TExprContext& ctx, bool ordered) {
    const auto arg = Build<TCoArgument>(ctx, input->Pos()).Name("map_arg").Done().Ptr();
    TVector<TExprBase> items;
    for (const auto& rename : renames) {
        // clang-format off
        auto tuple = Build<TCoNameValueTuple>(ctx, input->Pos())
            .Name().Build(rename.second)
            .Value<TCoMember>()
                .Struct(arg)
                .Name().Build(rename.first)
            .Build()
        .Done();
        // clang-format on
        items.push_back(tuple);
    }

    // clang-format off
    auto map = Build<TCoMap>(ctx, input->Pos())
        .Input(input)
        .Lambda<TCoLambda>()
            .Args({arg})
            .Body<TCoAsStruct>()
                .Add(items)
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on
    return ordered ? ctx.RenameNode(*map, "OrderedMap") : map;
}

TExprNode::TPtr ConvertToWideJoinFilter(TExprNode::TPtr input, const TMappedIUs<ui32>& inputs, const TUnorderedIUs& unwrapOptionalInputs, ui32 width, TExprContext& ctx) {
    Y_ENSURE(input->IsLambda());

    TVector<TExprNode::TPtr> lambdaArgs;
    lambdaArgs.reserve(width);
    for (ui32 i = 0; i < width; ++i) {
        lambdaArgs.push_back(ctx.NewArgument(input->Pos(), "param" + ToString(i)));
    }

    TMappedIUs<TExprNode::TPtr> fields;
    for (const auto& [id, position] : inputs.Items()) {
        TExprNode::TPtr value = lambdaArgs.at(position);
        if (unwrapOptionalInputs.Contains(id)) {
            value = Build<TCoUnwrap>(ctx, input->Pos())
                .Optional(value)
            .Done().Ptr();
        }
        fields.Add(id, std::move(value));
    }
    auto newBody = LowerRowLambdaBody(input, fields, ctx);

    if (!TMaybeNode<TCoVoid>(newBody)) {
        // Wrap with coalsesce in case of null input.
        // clang-format off
        newBody = Build<TCoCoalesce>(ctx, input->Pos())
            .Predicate(newBody)
            .Value<TCoBool>()
                .Literal().Value("false").Build()
            .Build()
        .Done().Ptr();
        // clang-format on
    }

    return ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), std::move(lambdaArgs)), std::move(newBody));
}

TExprNode::TPtr LowerRowLambdaBody(const TExprNode::TPtr& lambda,
    const TMappedIUs<TExprNode::TPtr>& fields, TExprContext& ctx)
{
    Y_ENSURE(lambda->IsLambda() && lambda->Head().ChildrenSize() == 1);
    const auto* row = &lambda->Head().Head();
    TNodeOnNodeOwnedMap replacements;
    VisitExpr(lambda->TailPtr(), [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("Member") && &node->Head() == row) {
            const auto id = GetMemberId(*node);
            const auto* field = fields.Find(id);
            Y_ENSURE(field, "Missing physical input ID " << id);
            replacements.emplace(node.Get(), *field);
            return false;
        }
        return true;
    });
    return ctx.ReplaceNodes(lambda->TailPtr(), replacements);
}

TExprNode::TPtr BuildVoidLambda(TExprContext& ctx, TPositionHandle pos) {
    // clang-format off
    return Build<TCoLambda>(ctx, pos)
        .Args({"arg"})
        .Body<TCoVoid>().Build()
    .Done().Ptr();
    // clang-format on
}

TVector<TString> GetFullNames(const TVector<TInfoUnitId>& columns, const TPhysicalNames& names) {
    TVector<TString> result;
    result.reserve(columns.size());
    for (const auto id : columns) {
        result.push_back(names.Get(id));
    }
    return result;
}

TExprNode::TPtr BuildWideProjection(TExprNode::TPtr input, const TVector<TString>& from, const TVector<TString>& to, TExprContext& ctx) {
    THashMap<TString, ui32> indices;
    indices.reserve(from.size());
    for (ui32 i = 0; i < from.size(); ++i) {
        indices.emplace(from[i], i);
    }

    TVector<TExprNode::TPtr> args;
    args.reserve(from.size());
    for (ui32 i = 0; i < from.size(); ++i) {
        args.push_back(ctx.NewArgument(input->Pos(), "wide_" + ToString(i)));
    }

    TExprNode::TListType results;
    results.reserve(to.size());
    for (const auto& column : to) {
        const auto it = indices.find(column);
        Y_ENSURE(it != indices.end(), "Column " << column << " is not available in the wide stage body");
        results.push_back(args[it->second]);
    }

    auto lambda = ctx.NewLambda(input->Pos(), ctx.NewArguments(input->Pos(), std::move(args)), std::move(results));
    return ctx.NewCallable(input->Pos(), "WideMap", {std::move(input), std::move(lambda)});
}

TExprNode::TPtr TStageBody::AsWide(const TVector<TString>& columns, TExprContext& ctx, const TPhysicalNames& names) const {
    Y_ENSURE(NarrowStream || WideFlow, "Stage body is empty");

    if (!WideFlow) {
        auto flow = ctx.NewCallable(NarrowStream->Pos(), "ToFlow", {NarrowStream});
        return BuildExpandMapForNarrowInput(flow, columns, ctx, names);
    }

    if (WideColumns == columns) {
        return WideFlow;
    }
    return BuildWideProjection(WideFlow, WideColumns, columns, ctx);
}

TExprNode::TPtr TStageBody::AsWide(const TVector<TInfoUnitId>& columns, TExprContext& ctx, const TPhysicalNames& names) const {
    return AsWide(GetFullNames(columns, names), ctx, names);
}

TExprNode::TPtr TStageBody::AsNarrow(TExprContext& ctx) const {
    Y_ENSURE(NarrowStream || WideFlow, "Stage body is empty");

    if (NarrowStream) {
        return NarrowStream;
    }

    const auto pos = WideFlow->Pos();
    TExprNode::TListType args;
    TExprNode::TListType members;
    args.reserve(WideColumns.size());
    members.reserve(WideColumns.size());
    for (ui32 i = 0; i < WideColumns.size(); ++i) {
        args.push_back(ctx.NewArgument(pos, "wide_input" + ToString(i)));
        members.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, WideColumns[i]), args.back()}));
    }
    auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, std::move(args)), ctx.NewCallable(pos, "AsStruct", std::move(members)));
    auto narrow = ctx.NewCallable(pos, "NarrowMap", {WideFlow, std::move(lambda)});
    return ctx.NewCallable(pos, "FromFlow", {std::move(narrow)});
}

TStageBody BuildLiveWideBody(TExprNode::TPtr flow, const TVector<TString>& columns, const THashSet<TString>& live, TExprContext& ctx) {
    TVector<TString> liveColumns;
    liveColumns.reserve(columns.size());
    for (const auto& column : columns) {
        if (live.contains(column)) {
            liveColumns.push_back(column);
        }
    }
    if (liveColumns.size() != columns.size()) {
        flow = BuildWideProjection(std::move(flow), columns, liveColumns, ctx);
    }
    return TStageBody::Wide(std::move(flow), std::move(liveColumns));
}

} // namespace NKikimr::NKqp::NPhysicalConvertionUtils
