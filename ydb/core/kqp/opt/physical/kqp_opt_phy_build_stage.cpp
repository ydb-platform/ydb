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
            .Settings().Build()
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

// ReadRangesSource can't deal with skipnullkeys, so we should expand it to (ExtractMembers (SkipNullKeys))
//FIXME: simplify KIKIMR-16987
NYql::NNodes::TExprBase ExpandSkipNullMembersForReadTableSource(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx, const TKqpOptimizeContext&) {
    auto stage = node.Cast<TDqStage>();
    TMaybe<size_t> tableSourceIndex;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        auto input = stage.Inputs().Item(i);
        if (input.Maybe<TDqSource>() && input.Cast<TDqSource>().Settings().Maybe<TKqpReadRangesSourceSettings>()) {
            tableSourceIndex = i;
        }
    }
    if (!tableSourceIndex) {
        return node;
    }

    auto source = stage.Inputs().Item(*tableSourceIndex).Cast<TDqSource>();
    auto readRangesSource = source.Settings().Cast<TKqpReadRangesSourceSettings>();
    auto settings = TKqpReadTableSettings::Parse(readRangesSource.Settings());
    
    if (settings.SkipNullKeys.empty()) {
        return node;
    }

    auto sourceArg = stage.Program().Args().Arg(*tableSourceIndex);

    THashSet<TString> seenColumns;
    TVector<TCoAtom> columns;
    TVector<TCoAtom> skipNullColumns;
    for (size_t i = 0; i < readRangesSource.Columns().Size(); ++i) {
        auto atom = readRangesSource.Columns().Item(i);
        auto column = atom.StringValue();
        if (seenColumns.insert(column).second) {
            columns.push_back(atom);
        }
    }
    for (auto& column : settings.SkipNullKeys) {
        TCoAtom atom(ctx.NewAtom(readRangesSource.Settings().Pos(), column));
        skipNullColumns.push_back(atom);
        if (seenColumns.insert(column).second) {
            columns.push_back(atom);
        }
    }

    settings.SkipNullKeys.clear();
    auto newSettings = Build<TKqpReadRangesSourceSettings>(ctx, source.Pos())
        .Table(readRangesSource.Table())
        .Columns().Add(columns).Build()
        .Settings(settings.BuildNode(ctx, source.Settings().Pos()))
        .RangesExpr(readRangesSource.RangesExpr())
        .ExplainPrompt(readRangesSource.ExplainPrompt())
        .Done();
    TDqStage replacedSettings = ReplaceTableSourceSettings(stage, *tableSourceIndex, newSettings, ctx);

    TCoArgument replaceArg{ctx.NewArgument(sourceArg.Pos(), TStringBuilder() << "_kqp_source_arg_0")};
    auto replaceExpr =
        Build<TCoExtractMembers>(ctx, node.Pos())
            .Members(readRangesSource.Columns())
            .Input<TCoSkipNullMembers>()
                .Input(replaceArg)
                .Members().Add(skipNullColumns).Build()
            .Build()
        .Done();

    return ReplaceStageArg(replacedSettings, *tableSourceIndex, replaceArg, replaceExpr, ctx);
}

TExprBase KqpBuildReadTableStage(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlReadTable>()) {
        return node;
    }
    const TKqlReadTable& read = node.Cast<TKqlReadTable>();
    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());

    bool useSource = kqpCtx.Config->EnableKqpScanQuerySourceRead && kqpCtx.IsScanQuery();
    useSource = useSource || (kqpCtx.Config->EnableKqpDataQuerySourceRead && kqpCtx.IsDataQuery());
    useSource = useSource &&
        tableDesc.Metadata->Kind != EKikimrTableKind::SysView &&
        tableDesc.Metadata->Kind != EKikimrTableKind::Olap;

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
            .Settings().Build()
            .Done();

        auto precompute = Build<TDqPhyPrecompute>(ctx, read.Pos())
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(computeStage)
                    .Index().Build("0")
                    .Build()
                .Build()
            .Done();

        if (useSource) {
            for (size_t i = 0; i < values.size(); ++i) {
                auto replace = Build<TCoNth>(ctx, read.Pos())
                    .Tuple(precompute)
                    .Index().Build(ToString(i))
                    .Done()
                    .Ptr();

                rangeReplaces[values[i].Raw()] = replace;
            }
        } else {
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
    }

    if (useSource) {
        inputs.push_back(
            Build<TDqSource>(ctx, read.Pos())
                .Settings<TKqpReadRangesSourceSettings>()
                    .Table(read.Table())
                    .Columns(read.Columns())
                    .Settings(read.Settings())
                    .RangesExpr(ctx.ReplaceNodes(read.Range().Ptr(), rangeReplaces))
                .Build()
                .DataSource<TCoDataSource>()
                    .Category<TCoAtom>().Value(KqpReadRangesSourceName).Build()
                .Build()
            .Done());
    }

    TMaybeNode<TExprBase> phyRead;
    switch (tableDesc.Metadata->Kind) {
        case EKikimrTableKind::Datashard:
            if (useSource) {
                TCoArgument arg{ctx.NewArgument(read.Pos(), TStringBuilder() << "_kqp_source_arg")};
                programArgs.push_back(arg);

                phyRead = arg;
                break;
            }
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
        .Settings().Build()
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

    bool useSource = kqpCtx.Config->EnableKqpScanQuerySourceRead && kqpCtx.IsScanQuery();
    useSource = useSource || (kqpCtx.Config->EnableKqpDataQuerySourceRead && kqpCtx.IsDataQuery());
    useSource = useSource &&
        tableDesc.Metadata->Kind != EKikimrTableKind::SysView &&
        tableDesc.Metadata->Kind != EKikimrTableKind::Olap;

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
                .Settings()
                    .Build()
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
        if (!useSource) {
            argument = Build<TCoArgument>(ctx, read.Pos())
                .Name("_kqp_pc_ranges_arg_0")
                .Done();

            input.push_back(precompute);
            programArgs.push_back(argument.Cast<TCoArgument>());
        }
    } else {
        rangesExpr = argument = read.Ranges();
    }

    TMaybeNode<TExprBase> phyRead;

    TMaybeNode<TExprBase> sourceArg;
    if (useSource) {
        YQL_ENSURE(rangesExpr.IsValid());

        input.push_back(
            Build<TDqSource>(ctx, read.Pos())
                .Settings<TKqpReadRangesSourceSettings>()
                    .Table(read.Table())
                    .Columns(read.Columns())
                    .Settings(read.Settings())
                    .RangesExpr(rangesExpr.Cast())
                    .ExplainPrompt(read.ExplainPrompt())
                .Build()
                .DataSource<TCoDataSource>()
                    .Category<TCoAtom>().Value(KqpReadRangesSourceName).Build()
                .Build()
            .Done());
        sourceArg = Build<TCoArgument>(ctx, read.Pos())
            .Name("_kqp_pc_source_arg_0")
            .Done();
        programArgs.push_back(sourceArg.Cast<TCoArgument>());
    }

    switch (tableDesc.Metadata->Kind) {
        case EKikimrTableKind::Datashard:
        case EKikimrTableKind::SysView:
            if (useSource) {
                YQL_ENSURE(sourceArg.IsValid());
                phyRead = sourceArg.Cast();
            } else {
                phyRead = Build<TKqpReadTableRanges>(ctx, read.Pos())
                    .Table(read.Table())
                    .Ranges(argument.Cast())
                    .Columns(read.Columns())
                    .Settings(read.Settings())
                    .ExplainPrompt(read.ExplainPrompt())
                    .Done();
            }
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
                    .Settings(TDqStageSettings().BuildNode(ctx, lookup.Pos()))
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
