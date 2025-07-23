#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/iterator/zip.h>

#include <util/generic/hash_set.h>

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
                .SetPartitionMode(TDqStageSettings::EPartitionMode::Single)
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

bool IsLiteralNothing(TExprBase node) {
    if (node.Maybe<TCoNothing>()) {
        auto* type = node.Raw()->GetTypeAnn();
        switch (type->GetKind()) {
            case ETypeAnnotationKind::Optional: {
                type = type->Cast<TOptionalExprType>()->GetItemType();

                if (type->GetKind() != ETypeAnnotationKind::Data) {
                    return false;
                }

                auto slot = type->Cast<TDataExprType>()->GetSlot();
                auto typeId = NKikimr::NUdf::GetDataTypeInfo(slot).TypeId;

                return (
                    NKikimr::NScheme::NTypeIds::IsYqlType(typeId)
                    && NKikimr::IsAllowedKeyType(NKikimr::NScheme::TTypeInfo(typeId))
                );
            }
            case ETypeAnnotationKind::Pg: {
                auto pgTypeId = type->Cast<TPgExprType>()->GetId();
                return NKikimr::IsAllowedKeyType(
                    NKikimr::NScheme::TTypeInfo(NKikimr::NPg::TypeDescFromPgTypeId(pgTypeId))
                );
            }
            default:
                return false;
        }
    } else {
        return false;
    }
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

            if (!value.Maybe<TCoDataCtor>()
                && !value.Maybe<TCoParameter>()
                && !value.Maybe<TCoPgConst>()
                && !IsLiteralNothing(value))
            {
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
                .SetPartitionMode(TDqStageSettings::EPartitionMode::Single)
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
            .SetPartitionMode(singleKey && UseSource(kqpCtx, tableDesc) ? TDqStageSettings::EPartitionMode::Single : TDqStageSettings::EPartitionMode::Default)
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
                    .SetPartitionMode(TDqStageSettings::EPartitionMode::Single)
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
            .DefaultConstraintColumns(sequencer.DefaultConstraintColumns())
            .InputItemType(sequencer.InputItemType())
            .Done();
    } else if (sequencer.Input().Maybe<TDqCnUnionAll>()) {
        auto output = sequencer.Input().Cast<TDqCnUnionAll>().Output();

        cnSequencer = Build<TKqpCnSequencer>(ctx, sequencer.Pos())
            .Output(output)
            .Table(sequencer.Table())
            .Columns(sequencer.Columns())
            .DefaultConstraintColumns(sequencer.DefaultConstraintColumns())
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

NYql::NNodes::TExprBase KqpRewriteLookupTablePhy(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx) {

    Y_UNUSED(kqpCtx);

    if (!node.Maybe<TDqStage>()) {
        return node;
    }

    const auto& stage = node.Cast<TDqStage>();
    TMaybeNode<TKqpLookupTable> maybeLookupTable;
    VisitExpr(stage.Program().Body().Ptr(), [&](const TExprNode::TPtr& node) {
        TExprBase expr(node);
        if (expr.Maybe<TKqpLookupTable>()) {
            maybeLookupTable = expr.Maybe<TKqpLookupTable>();
            return false;
        }

        return true;
    });

    if (!maybeLookupTable) {
        return node;
    }

    auto lookupTable = maybeLookupTable.Cast();
    auto lookupKeys = lookupTable.LookupKeys();

    YQL_ENSURE(lookupKeys.Maybe<TCoIterator>(), "Expected list iterator as LookupKeys, but got: "
        << KqpExprToPrettyString(lookupKeys, ctx));

    TKqpStreamLookupSettings settings;
    settings.Strategy = EStreamLookupStrategyType::LookupRows;
    TNodeOnNodeOwnedMap replaceMap;
    TVector<TExprBase> newInputs;
    TVector<TCoArgument> newArgs;
    newInputs.reserve(stage.Inputs().Size() + 1);
    newArgs.reserve(stage.Inputs().Size() + 1);

    auto lookupKeysList = lookupKeys.Maybe<TCoIterator>().Cast().List();
    TMaybeNode<TCoArgument> lookupKeysArg = lookupKeysList.Maybe<TCoArgument>();
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        const auto& input = stage.Inputs().Item(i);
        const auto& inputArg = stage.Program().Args().Arg(i);

        TCoArgument newArg{ctx.NewArgument(inputArg.Pos(), TStringBuilder() << "_kqp_input_arg_" << i)};
        if (lookupKeysArg && lookupKeysArg.Cast().Raw() == inputArg.Raw()) {
            YQL_ENSURE(input.Maybe<TDqPhyPrecompute>());
            auto keysPrecompute = input.Maybe<TDqPhyPrecompute>().Cast();

            auto cnStreamLookup = Build<TKqpCnStreamLookup>(ctx, node.Pos())
                .Output()
                    .Stage<TDqStage>()
                        .Inputs()
                            .Add(input)
                            .Build()
                        .Program()
                            .Args({"stream_lookup_keys"})
                            .Body<TCoToStream>()
                                .Input("stream_lookup_keys")
                                .Build()
                            .Build()
                        .Settings().Build()
                        .Build()
                    .Index().Build("0")
                    .Build()
                .Table(lookupTable.Table())
                .Columns(lookupTable.Columns())
                .InputType(ExpandType(node.Pos(), *keysPrecompute.Ref().GetTypeAnn(), ctx))
                .Settings(settings.BuildNode(ctx, node.Pos()))
                .Done();

            newInputs.emplace_back(std::move(cnStreamLookup));
            newArgs.push_back(newArg);
            replaceMap[inputArg.Raw()] = newArg.Ptr();
            replaceMap[lookupTable.Raw()] = newArg.Ptr();
        } else {
            newInputs.push_back(input);
            newArgs.push_back(newArg);
            replaceMap[inputArg.Raw()] = newArg.Ptr();
        }
    }

    if (!lookupKeysArg) {  // lookupKeysList is pure expression
        TCoArgument newArg{ctx.NewArgument(node.Pos(), TStringBuilder() << "_kqp_source_arg_" << newArgs.size())};

        auto cnStreamLookup = Build<TKqpCnStreamLookup>(ctx, node.Pos())
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Build()
                    .Program()
                        .Args({})
                        .Body<TCoToStream>()
                            .Input(lookupKeysList)
                            .Build()
                        .Build()
                    .Settings().Build()
                    .Build()
                .Index().Build("0")
                .Build()
            .Table(lookupTable.Table())
            .Columns(lookupTable.Columns())
            .InputType(ExpandType(node.Pos(), *lookupKeysList.Ref().GetTypeAnn(), ctx))
            .Settings(settings.BuildNode(ctx, node.Pos()))
            .Done();

        newInputs.emplace_back(std::move(cnStreamLookup));
        newArgs.push_back(newArg);
        replaceMap[lookupTable.Raw()] = newArg.Ptr();
    }

    return Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(newInputs)
            .Build()
        .Program<TCoLambda>()
            .Args(newArgs)
            .Body(TExprBase(ctx.ReplaceNodes(stage.Program().Body().Ptr(), replaceMap)))
            .Build()
        .Settings().Build()
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
                    .Settings(TDqStageSettings::New()
                        .SetPartitionMode(TDqStageSettings::EPartitionMode::Single)
                        .BuildNode(ctx, lookup.Pos()))
                    .Build()
                .Index().Build("0")
            .Build()
            .Table(lookup.Table())
            .Columns(lookup.Columns())
            .InputType(ExpandType(lookup.Pos(), *lookup.LookupKeys().Ref().GetTypeAnn(), ctx))
            .Settings(lookup.Settings())
            .Done();

    } else if (lookup.LookupKeys().Maybe<TDqCnUnionAll>()) {
        auto output = lookup.LookupKeys().Cast<TDqCnUnionAll>().Output();

        cnStreamLookup = Build<TKqpCnStreamLookup>(ctx, lookup.Pos())
            .Output(output)
            .Table(lookup.Table())
            .Columns(lookup.Columns())
            .InputType(ExpandType(lookup.Pos(), *output.Ref().GetTypeAnn(), ctx))
            .Settings(lookup.Settings())
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

NYql::NNodes::TExprBase KqpBuildStreamIdxLookupJoinStagesKeepSorted(
    NYql::NNodes::TExprBase node,
    NYql::TExprContext& ctx,
    TTypeAnnotationContext& typeCtx,
    bool ruleEnabled
)
{
    if (!ruleEnabled) {
        return node;
    }

    if (!node.Maybe<TKqlIndexLookupJoin>()) {
        return node;
    }

    const auto& idxLookupJoin = node.Cast<TKqlIndexLookupJoin>();

    if (!idxLookupJoin.Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto unionAll = idxLookupJoin.Input().Cast<TDqCnUnionAll>();
    auto inputStats = typeCtx.GetStats(unionAll.Output().Raw());
    auto sortedByOrderingIdx = inputStats->SortingOrderings.GetInitOrderingIdx();

    if (!inputStats || !typeCtx.SortingsFSM || sortedByOrderingIdx == -1) {
        return node;
    }

    auto stage = unionAll
        .Output().Maybe<TDqOutput>()
        .Stage().Maybe<TDqStageBase>();

    auto streamLookup = unionAll
        .Output().Maybe<TDqOutput>()
        .Stage().Maybe<TDqStageBase>()
        .Inputs().Item(0).Maybe<TKqpCnStreamLookup>();

    if (!streamLookup.IsValid()) {
        return node;
    }

    TExprNodeList fields;

    auto tupleType = streamLookup.Cast().InputType().Cast<TCoListType>().ItemType().Cast<TCoTupleType>();

    auto arg = Build<TCoArgument>(ctx, node.Pos()).Name("row").Done();
    TExprNodeList args;
    args.push_back(arg.Ptr());

    auto rightStruct = tupleType.Arg(1).Cast<TCoStructType>();

    THashSet<TString> passthroughColumns;
    for (const auto& structContent : rightStruct ) {
        auto attrName = structContent.Ptr()->Child(0);
        passthroughColumns.insert(TString(attrName->Content()));
        auto field = Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name(attrName)
                .Value<TCoMember>()
                    .Struct<TCoNth>()
                        .Tuple(arg)
                        .Index().Value("0").Build()
                        .Build()
                    .Name(attrName)
                    .Build()
                .Done().Ptr();

        fields.push_back(field);
    }

    auto payload = Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name().Build("_payload")
                .Value(arg)
                .Done().Ptr();

    fields.push_back(payload);

    auto stageLambda = stage.Cast().Program();

    auto orderedMap = Build<TCoOrderedMap>(ctx, node.Pos())
        .Input(stageLambda.Body())
        .Lambda()
            .Args(args)
            .Body<TCoAsStruct>()
                .Add(fields).Build()
            .Build()
        .Done();

    auto builder = Build<TDqSortColumnList>(ctx, node.Pos());

    auto& fdStorage = typeCtx.SortingsFSM->FDStorage;
    Y_ENSURE(sortedByOrderingIdx >= 0);
    auto sortedBy = fdStorage.GetInterestingSortingByOrderingIdx(sortedByOrderingIdx);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    bool nextMustBeSkipped = false;
    // ^ we must save merge connection only for prefixies of the sorted columns - if it is not prefix, we will skip the rule
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    for (const auto& [column, dir]: Zip(sortedBy.Ordering, sortedBy.Directions)) {
        TString columnName = column.AttributeName;
        bool hasColumn = passthroughColumns.contains(columnName);
        if (!hasColumn && column.RelName) {
            columnName = column.RelName + "." + columnName;
        }
        bool hasColumnWithAlias = passthroughColumns.contains(columnName);

        if (!hasColumn && !hasColumnWithAlias) {
            nextMustBeSkipped = true;
            continue;
        } else if (nextMustBeSkipped) {
            return node;
        }

        TString columnDir;
        switch (dir) {
            using enum TOrdering::TItem::EDirection;
            case EAscending: { columnDir = TTopSortSettings::AscendingSort; break; }
            case EDescending: { columnDir = TTopSortSettings::DescendingSort; break; }
            case ENone: { return node; }
        }

        builder
            .Add<TDqSortColumn>()
                    .Column<TCoAtom>()
                .Build(std::move(columnName))
                    .SortDirection()
                .Build(std::move(columnDir))
            .Build();
    }

    auto newStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs(stage.Cast().Inputs())
        .Program()
            .Args(stageLambda.Args())
            .Body(orderedMap)
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
        .Done().Ptr();

    auto merge = Build<TDqCnMerge>(ctx, node.Pos())
        .Output()
            .Stage(newStage)
            .Index().Build(0)
            .Build()
        .SortColumns(builder.Build().Value())
        .Done().Ptr();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
            .Inputs()
                .Add(merge)
            .Build()
            .Program()
                .Args({"stream_lookup_join_output"})
                .Body<TKqpIndexLookupJoin>()
                    .Input<TCoOrderedMap>()
                        .Input<TCoToStream>()
                            .Input("stream_lookup_join_output")
                            .Build()
                        .Lambda()
                            .Args({"arg"})
                            .Body<TCoMember>()
                                .Struct("arg")
                                .Name().Build("_payload")
                                .Build()
                            .Build()
                        .Build()
                    .JoinType(idxLookupJoin.JoinType())
                    .LeftLabel(idxLookupJoin.LeftLabel())
                    .RightLabel(idxLookupJoin.RightLabel())
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Build()
        .Index().Build("0")
        .Build()
    .Done();
}

NYql::NNodes::TExprBase KqpBuildStreamIdxLookupJoinStages(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx) {

    if (!node.Maybe<TKqlIndexLookupJoin>()) {
        return node;
    }

    const auto& idxLookupJoin = node.Cast<TKqlIndexLookupJoin>();

    if (!idxLookupJoin.Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto unionAll = idxLookupJoin.Input().Cast<TDqCnUnionAll>();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
            .Inputs()
                .Add(unionAll)
                .Build()
            .Program()
                .Args({"stream_lookup_join_output"})
                .Body<TKqpIndexLookupJoin>()
                    .Input<TCoToStream>()
                        .Input("stream_lookup_join_output")
                        .Build()
                    .JoinType(idxLookupJoin.JoinType())
                    .LeftLabel(idxLookupJoin.LeftLabel())
                    .RightLabel(idxLookupJoin.RightLabel())
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Build()
        .Index().Build("0")
        .Build().Done();
}

} // namespace NKikimr::NKqp::NOpt
