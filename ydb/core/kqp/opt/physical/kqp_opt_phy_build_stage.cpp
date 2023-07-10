#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

bool IsSingleKey(const TKqlKeyRange& range, const TKikimrTableMetadata& tableMeta) {
    if (range.From().ArgCount() != tableMeta.KeyColumnNames.size()) {
        return false;
    }

    if (range.To().ArgCount() != tableMeta.KeyColumnNames.size()) {
        return false;
    }

    for (size_t i = 0; i < tableMeta.KeyColumnNames.size(); ++i) {
        if (range.From().Arg(i).Raw() != range.To().Arg(i).Raw()) {
            return false;
        }
    }

    return true;
}

} // namespace

TMaybeNode<TDqPhyPrecompute> BuildLookupKeysPrecompute(const TExprBase& input, TExprContext& ctx) {
    TMaybeNode<TDqConnection> precomputeInput;

    if (IsDqPureExpr(input)) {
        YQL_ENSURE(input.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List, "" << input.Ref().Dump());

        auto computeStage = Build<TDqStage>(ctx, input.Pos())
            .Inputs()
                .Build()
            .Program()
                .Args({})
                .Body<TCoToStream>()
                    .Input<TCoJust>()
                        .Input(input)
                        .Build()
                    .Build()
                .Build()
            .Settings(TDqStageSettings::New()
                .SetSinglePartition()
                .BuildNode(ctx, input.Pos()))
            .Done();

        precomputeInput = Build<TDqCnValue>(ctx, input.Pos())
            .Output()
                .Stage(computeStage)
                .Index().Build("0")
                .Build()
            .Done();

    } else if (input.Maybe<TDqCnUnionAll>()) {
        precomputeInput = input.Cast<TDqCnUnionAll>();
    } else {
        return {};
    }

    return Build<TDqPhyPrecompute>(ctx, input.Pos())
        .Connection(precomputeInput.Cast())
        .Done();
}

TExprBase KqpBuildReadTableStage(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlReadTable>()) {
        return node;
    }
    const TKqlReadTable& read = node.Cast<TKqlReadTable>();
    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());

    TVector<TExprBase> values;
    TNodeOnNodeOwnedMap replaceMap;

    auto checkRange = [&values](const TVarArgCallable<TExprBase>& tuple, bool& literalRange) {
        literalRange = true;

        for (const auto& value : tuple) {
            if (!IsDqPureExpr(value)) {
                literalRange = false;
                return false;
            }

            if (!value.Maybe<TCoDataCtor>() && !value.Maybe<TCoParameter>()) {
                literalRange = false;
            }

            if (!value.Maybe<TCoParameter>()) {
                values.push_back(value);
            }
        }

        return true;
    };

    bool fromIsLiteral = false;
    if (!checkRange(read.Range().From(), fromIsLiteral)) {
        return read;
    }

    bool toIsLiteral = false;
    if (!checkRange(read.Range().To(), toIsLiteral)) {
        return read;
    }

    bool literalRanges = fromIsLiteral && toIsLiteral;
    bool singleKey = IsSingleKey(read.Range(), *tableDesc.Metadata);

    TVector<TExprBase> inputs;
    TVector<TCoArgument> programArgs;
    TNodeOnNodeOwnedMap rangeReplaces;
    if (!values.empty() && !literalRanges) {
        auto computeStage = Build<TDqStage>(ctx, read.Pos())
            .Inputs()
                .Build()
            .Program()
                .Args({})
                .Body<TCoToStream>()
                    .Input<TCoJust>()
                        .Input<TExprList>()
                            .Add(values)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Settings(TDqStageSettings::New()
                .SetSinglePartition()
                .BuildNode(ctx, read.Pos()))
            .Done();

        auto precompute = Build<TDqPhyPrecompute>(ctx, read.Pos())
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(computeStage)
                    .Index().Build("0")
                    .Build()
                .Build()
            .Done();

        TCoArgument arg{ctx.NewArgument(read.Pos(), TStringBuilder() << "_kqp_pc_arg_0")};
        programArgs.push_back(arg);

        for (size_t i = 0; i < values.size(); ++i) {
            auto replace = Build<TCoNth>(ctx, read.Pos())
                .Tuple(arg)
                .Index().Build(ToString(i))
                .Done()
                .Ptr();

            rangeReplaces[values[i].Raw()] = replace;
        }
        inputs.push_back(precompute);
    }

    TMaybeNode<TExprBase> phyRead;
    switch (tableDesc.Metadata->Kind) {
        case EKikimrTableKind::Datashard:
        case EKikimrTableKind::SysView:
            phyRead = Build<TKqpReadTable>(ctx, read.Pos())
                .Table(read.Table())
                .Range(ctx.ReplaceNodes(read.Range().Ptr(), rangeReplaces))
                .Columns(read.Columns())
                .Settings(read.Settings())
                .Done();
            break;
        default:
            YQL_ENSURE(false, "Unexpected table kind: " << (ui32)tableDesc.Metadata->Kind);
            break;
    }

    auto stage = Build<TDqStage>(ctx, read.Pos())
        .Inputs()
            .Add(inputs)
            .Build()
        .Program()
            .Args(programArgs)
            .Body(phyRead.Cast())
            .Build()
        .Settings(TDqStageSettings::New()
            .SetSinglePartition(singleKey && UseSource(kqpCtx, tableDesc))
            .BuildNode(ctx, read.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, read.Pos())
        .Output()
            .Stage(stage)
            .Index().Build("0")
            .Build()
        .Done();
}

TExprBase KqpBuildReadTableRangesStage(TExprBase node, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, const TParentsMap& parents)
{
    if (!node.Maybe<TKqlReadTableRanges>()) {
        return node;
    }
    const TKqlReadTableRanges& read = node.Cast<TKqlReadTableRanges>();

    auto ranges = read.Ranges();
    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());

    bool fullScan = TCoVoid::Match(ranges.Raw());

    TVector<TExprBase> input;
    TMaybeNode<TExprBase> argument;
    TVector<TCoArgument> programArgs;
    TMaybeNode<TExprBase> rangesExpr;

    if (!fullScan) {
        TMaybe<TDqStage> rangesStage;
        if (IsDqPureExpr(read.Ranges())) {
            rangesStage = Build<TDqStage>(ctx, read.Pos())
                .Inputs()
                    .Build()
                .Program()
                    .Args({})
                    .Body<TCoToStream>()
                        .Input<TCoJust>()
                            .Input<TExprList>()
                                .Add(ranges)
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Settings(TDqStageSettings::New()
                    .SetSinglePartition()
                    .BuildNode(ctx, read.Pos()))
                .Done();
        } else {
            TVector<TDqConnection> connections;
            bool isPure;
            FindDqConnections(node, connections, isPure);
            if (!isPure) {
                return node;
            }
            YQL_ENSURE(!connections.empty());
            TVector<TDqConnection> inputs;
            TVector<TExprBase> stageInputs;
            TNodeOnNodeOwnedMap replaceMap;

            inputs.reserve(connections.size());
            for (auto& cn : connections) {
                auto input = TDqConnection(cn);
                if (!input.Maybe<TDqCnUnionAll>()) {
                    return node;
                }

                if (!IsSingleConsumerConnection(input, parents, true)) {
                    return node;
                }

                inputs.push_back(input);
                stageInputs.push_back(
                    Build<TDqPhyPrecompute>(ctx, cn.Pos())
                        .Connection(input)
                        .Done());
            }
            auto args = PrepareArgumentsReplacement(read.Ranges(), inputs, ctx, replaceMap);
            auto body = ctx.ReplaceNodes(read.Ranges().Ptr(), replaceMap);
            rangesStage = Build<TDqStage>(ctx, read.Pos())
                .Inputs()
                    .Add(std::move(stageInputs))
                    .Build()
                .Program()
                    .Args(args)
                    .Body<TCoToStream>()
                        .Input<TCoJust>()
                            .Input<TExprList>()
                                .Add(body)
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Settings().Build()
                .Done();
        }

        YQL_ENSURE(rangesStage);
        auto precompute = Build<TDqPhyPrecompute>(ctx, read.Pos())
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(*rangesStage)
                    .Index().Build("0")
                    .Build()
                .Build()
            .Done();

        rangesExpr = precompute;
        argument = Build<TCoArgument>(ctx, read.Pos())
            .Name("_kqp_pc_ranges_arg_0")
            .Done();

        input.push_back(precompute);
        programArgs.push_back(argument.Cast<TCoArgument>());
    } else {
        rangesExpr = argument = read.Ranges();
    }

    TMaybeNode<TExprBase> phyRead;

    switch (tableDesc.Metadata->Kind) {
        case EKikimrTableKind::Datashard:
        case EKikimrTableKind::SysView:
            phyRead = Build<TKqpReadTableRanges>(ctx, read.Pos())
                .Table(read.Table())
                .Ranges(argument.Cast())
                .Columns(read.Columns())
                .Settings(read.Settings())
                .ExplainPrompt(read.ExplainPrompt())
                .Done();
            break;

        case EKikimrTableKind::Olap:
            phyRead = Build<TKqpReadOlapTableRanges>(ctx, read.Pos())
                .Table(read.Table())
                .Ranges(argument.Cast())
                .Columns(read.Columns())
                .Settings(read.Settings())
                .ExplainPrompt(read.ExplainPrompt())
                .Process()
                    .Args({"row"})
                    .Body("row")
                    .Build()
                .Done();
            break;

        default:
            YQL_ENSURE(false, "Unexpected table kind: " << (ui32)tableDesc.Metadata->Kind);
            break;
    }

    auto stage = Build<TDqStage>(ctx, read.Pos())
        .Inputs()
            .Add(input)
            .Build()
        .Program()
            .Args(programArgs)
            .Body(phyRead.Cast())
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, read.Pos())
        .Output()
            .Stage(stage)
            .Index().Build("0")
            .Build()
        .Done();
}

bool RequireLookupPrecomputeStage(const TKqlLookupTable& lookup) {
    if (lookup.LookupKeys().Maybe<TCoParameter>()) {
        return false;
    }
    if (!lookup.LookupKeys().Maybe<TCoAsList>()) {
        return true;
    }
    auto asList = lookup.LookupKeys().Cast<TCoAsList>();

    for (auto row : asList) {
        if (auto maybeAsStruct = row.Maybe<TCoAsStruct>()) {
            auto asStruct = maybeAsStruct.Cast();
            for (auto item : asStruct) {
                auto tuple = item.Cast<TCoNameValueTuple>();
                if (tuple.Value().Maybe<TCoParameter>()) {
                    // pass
                } else if (tuple.Value().Maybe<TCoDataCtor>()) {
                    // TODO: support pg types
                    Y_ENSURE(tuple.Value().Ref().GetTypeAnn()->GetKind() != NYql::ETypeAnnotationKind::Pg);
                    auto slot = tuple.Value().Ref().GetTypeAnn()->Cast<TDataExprType>()->GetSlot();
                    auto typeId = NUdf::GetDataTypeInfo(slot).TypeId;
                    auto typeInfo = NScheme::TTypeInfo(typeId);
                    if (NScheme::NTypeIds::IsYqlType(typeId) && NSchemeShard::IsAllowedKeyType(typeInfo)) {
                        // pass
                    } else {
                        return true;
                    }
                } else {
                    return true;
                }
            }
        } else {
            return true;
        }
    }

    return false;
}

TExprBase KqpBuildLookupTableStage(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TKqlLookupTable>()) {
        return node;
    }
    const TKqlLookupTable& lookup = node.Cast<TKqlLookupTable>();

    YQL_ENSURE(lookup.CallableName() == TKqlLookupTable::CallableName());

    TMaybeNode<TDqStage> stage;

    if (!RequireLookupPrecomputeStage(lookup)) {
        stage = Build<TDqStage>(ctx, lookup.Pos())
            .Inputs()
                .Build()
            .Program()
                .Args({})
                .Body<TKqpLookupTable>()
                    .Table(lookup.Table())
                    .LookupKeys<TCoIterator>()
                        .List(lookup.LookupKeys())
                        .Build()
                    .Columns(lookup.Columns())
                    .Build()
                .Build()
            .Settings().Build()
            .Done();
    } else {
        auto precompute = BuildLookupKeysPrecompute(lookup.LookupKeys(), ctx);
        if (!precompute) {
            return node;
        }

        stage = Build<TDqStage>(ctx, lookup.Pos())
            .Inputs()
                .Add(precompute.Cast())
                .Build()
            .Program()
                .Args({"keys_arg"})
                .Body<TKqpLookupTable>()
                    .Table(lookup.Table())
                    .LookupKeys<TCoIterator>()
                        .List("keys_arg")
                        .Build()
                    .Columns(lookup.Columns())
                    .Build()
                .Build()
            .Settings().Build()
            .Done();
    }

    return Build<TDqCnUnionAll>(ctx, lookup.Pos())
        .Output()
            .Stage(stage.Cast())
            .Index().Build("0")
            .Build()
        .Done();
}

NYql::NNodes::TExprBase KqpBuildSequencerStages(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx) {
    if (!node.Maybe<TKqlSequencer>()) {
        return node;
    }

    const auto& sequencer = node.Cast<TKqlSequencer>();

    TMaybeNode<TKqpCnSequencer> cnSequencer;
    if (IsDqPureExpr(sequencer.Input())) {
        YQL_ENSURE(sequencer.Input().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List,
            "" << sequencer.Input().Ref().Dump());

        cnSequencer = Build<TKqpCnSequencer>(ctx, sequencer.Pos())
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                    .Build()
                    .Program()
                        .Args({})
                        .Body<TCoIterator>()
                            .List(sequencer.Input())
                            .Build()
                        .Build()
                    .Settings(TDqStageSettings::New()
                        .BuildNode(ctx, sequencer.Pos()))
                    .Build()
                .Index().Build("0")
            .Build()
            .Table(sequencer.Table())
            .Columns(sequencer.Columns())
            .AutoIncrementColumns(sequencer.AutoIncrementColumns())
            .InputItemType(sequencer.InputItemType())
            .Done();
    } else {
        return node;
    }

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
            .Inputs()
                .Add(cnSequencer.Cast())
                .Build()
            .Program()
                .Args({"sequencer_output"})
                .Body<TCoToStream>()
                    .Input("sequencer_output")
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Build()
            .Index().Build("0")
        .Build().Done();    
}

NYql::NNodes::TExprBase KqpBuildStreamLookupTableStages(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx) {
    if (!node.Maybe<TKqlStreamLookupTable>()) {
        return node;
    }

    const auto& lookup = node.Cast<TKqlStreamLookupTable>();

    TMaybeNode<TKqpCnStreamLookup> cnStreamLookup;
    if (IsDqPureExpr(lookup.LookupKeys())) {
        YQL_ENSURE(lookup.LookupKeys().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List,
            "" << lookup.LookupKeys().Ref().Dump());

        cnStreamLookup = Build<TKqpCnStreamLookup>(ctx, lookup.Pos())
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                    .Build()
                    .Program()
                        .Args({})
                        .Body<TCoIterator>()
                            .List(lookup.LookupKeys())
                            .Build()
                        .Build()
                    .Settings(TDqStageSettings::New()
                        .SetSinglePartition()
                        .BuildNode(ctx, lookup.Pos()))
                    .Build()
                .Index().Build("0")
            .Build()
            .Table(lookup.Table())
            .Columns(lookup.Columns())
            .LookupKeysType(ExpandType(lookup.Pos(), *lookup.LookupKeys().Ref().GetTypeAnn(), ctx))
            .Done();

    } else if (lookup.LookupKeys().Maybe<TDqCnUnionAll>()) {
        auto output = lookup.LookupKeys().Cast<TDqCnUnionAll>().Output();

        cnStreamLookup = Build<TKqpCnStreamLookup>(ctx, lookup.Pos())
            .Output(output)
            .Table(lookup.Table())
            .Columns(lookup.Columns())
            .LookupKeysType(ExpandType(lookup.Pos(), *output.Ref().GetTypeAnn(), ctx))
            .Done();
    } else {
        return node;
    }

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
            .Inputs()
                .Add(cnStreamLookup.Cast())
                .Build()
            .Program()
                .Args({"stream_lookup_output"})
                .Body<TCoToStream>()
                    .Input("stream_lookup_output")
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Build()
            .Index().Build("0")
        .Build().Done();
}

} // namespace NKikimr::NKqp::NOpt
