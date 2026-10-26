#include "kqp_rbo_physical_source_builder.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/rbo/kqp_olap_expr_inspection.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TString TPhysicalSourceBuilder::GetOlapAggregateField(TInfoUnitId output, TStringBuf function) const {
    return Names.GetTemporaryName(TStringBuilder() << "__kqp_olap_agg_" << function << "_" << output << "_");
}

TExprNode::TPtr TPhysicalSourceBuilder::AddOlapAggregate(TExprNode::TPtr processLambda, TVector<TString>& outputFields) const {
    TVector<TExprBase> projections;
    TVector<TExprBase> aggregates;
    auto addAggregate = [&](TString field, TStringBuf function, const TString& column) {
        // clang-format off
        aggregates.push_back(Build<TKqpOlapAggOperation>(Ctx, Pos)
            .Name().Build(field)
            .Type().Build(function)
            .Column().Build(column)
        .Done());
        // clang-format on
        outputFields.push_back(std::move(field));
    };

    // Storage column -> its Double cast, which all AVGs of the column share.
    THashMap<TString, TString> averageInputs;
    for (const auto& aggregation : OlapAggregate->Aggregations) {
        // COUNT(*) has no column argument.
        const TString column = aggregation.Column ? Registry.Get(*aggregation.Column).GetColumnName() : TString("*");
        if (aggregation.Function != "avg") {
            addAggregate(GetOlapAggregateField(aggregation.Output, aggregation.Function), aggregation.Function, column);
            continue;
        }

        // Column shards return AVG as its sum and count. Cast the input to Double
        // before SUM, so integer sums cannot overflow and Float sums are Double.
        TString sumColumn = column;
        const auto& columnType = *Read.GetIUType(*aggregation.Column, Ctx);
        if (IsOlapAverageCast(columnType)) {
            const auto [it, inserted] = averageInputs.emplace(column, GetOlapAggregateField(aggregation.Output, "avg_input"));
            if (inserted) {
                projections.push_back(BuildOlapDoubleCast(column, columnType, it->second));
            }
            sumColumn = it->second;
        }
        addAggregate(GetOlapAggregateField(aggregation.Output, "sum"), "sum", sumColumn);
        addAggregate(GetOlapAggregateField(aggregation.Output, "count"), "count", column);
    }

    // Several key IDs may name one storage column; group by it once.
    TVector<TCoAtom> keys;
    THashSet<TString> keyColumns;
    for (const auto key : OlapAggregate->Keys) {
        const auto& column = Registry.Get(key).GetColumnName();
        if (keyColumns.insert(column).second) {
            keys.push_back(Build<TCoAtom>(Ctx, Pos).Value(column).Done());
            outputFields.push_back(column);
        }
    }
    // Block reads expose the program row in lexical field order.
    std::sort(outputFields.begin(), outputFields.end());

    TExprBase input = TCoLambda(processLambda).Body();
    if (!projections.empty()) {
        // clang-format off
        input = Build<TKqpOlapProjections>(Ctx, Pos)
            .Input(input)
            .Projections()
                .Add(projections)
            .Build()
        .Done();
        // clang-format on
    }

    // clang-format off
    auto olapAgg = Build<TKqpOlapAgg>(Ctx, Pos)
        .Input(input)
        .Aggregates<TKqpOlapAggOperationList>()
            .Add(aggregates)
        .Build()
        .KeyColumns<TCoAtomList>()
            .Add(keys)
        .Build()
    .Done().Ptr();
    // clang-format on
    return Ctx.ChangeChild(*processLambda, TCoLambda::idx_Body, std::move(olapAgg));
}

TExprNode::TPtr TPhysicalSourceBuilder::AddOlapDistinct(TExprNode::TPtr processLambda, TVector<TString>& outputFields) const {
    const auto column = Registry.Get(OlapDistinct->Key).GetColumnName();
    outputFields.push_back(column);
    // clang-format off
    auto olapDistinct = Build<TKqpOlapDistinct>(Ctx, Pos)
        .Input(TCoLambda(processLambda).Body())
        .Key().Build(column)
    .Done().Ptr();
    // clang-format on
    return Ctx.ChangeChild(*processLambda, TCoLambda::idx_Body, std::move(olapDistinct));
}

TExprBase TPhysicalSourceBuilder::BuildOlapDoubleCast(const TString& column, const TTypeAnnotationNode& columnType, const TString& field) const {
    const TVector<const TItemExprType*> rowItems{Ctx.MakeType<TItemExprType>(column, &columnType)};
    const auto* rowType = Ctx.MakeType<TStructExprType>(rowItems);
    const auto* doubleType = Ctx.MakeType<TDataExprType>(EDataSlot::Double);
    // clang-format off
    return Build<TKqpOlapProjection>(Ctx, Pos)
        .OlapOperation<TKqpOlapApply>()
            .Lambda()
                .Args({"value"})
                .Body<TCoSafeCast>()
                    .Value("value")
                    .Type(ExpandType(Pos, *doubleType, Ctx))
                .Build()
            .Build()
            .Args()
                .Add<TKqpOlapApplyColumnArg>()
                    .TableRowType(ExpandType(Pos, *rowType, Ctx))
                    .ColumnName().Build(column)
                .Build()
            .Build()
            .KernelName().Build("")
        .Build()
        .ColumnName().Build(field)
    .Done();
    // clang-format on
}

TExprNode::TPtr TPhysicalSourceBuilder::BuildOlapAggregateRow(const TProgramFields& fields) const {
    TExprNode::TListType members;
    auto addMember = [&](TInfoUnitId id, TExprNode::TPtr value) {
        members.push_back(Ctx.NewList(Pos, {Ctx.NewAtom(Pos, Names.Get(id)), std::move(value)}));
    };

    for (const auto key : OlapAggregate->Keys) {
        addMember(key, fields.at(Registry.Get(key).GetColumnName()));
    }
    for (const auto& aggregation : OlapAggregate->Aggregations) {
        if (aggregation.Function == "avg") {
            addMember(aggregation.Output, BuildOlapAverageState(aggregation, fields));
        } else {
            addMember(aggregation.Output, fields.at(GetOlapAggregateField(aggregation.Output, aggregation.Function)));
        }
    }
    return Ctx.NewCallable(Pos, "AsStruct", std::move(members));
}

TExprNode::TPtr TPhysicalSourceBuilder::BuildOlapAverageState(const TOlapAggregation& aggregation, const TProgramFields& fields) const {
    // The AVG state of TPhysicalAggregationBuilder is (sum as Double, count).
    // AddOlapAggregate already sums Double values.
    auto sum = fields.at(GetOlapAggregateField(aggregation.Output, "sum"));
    auto count = fields.at(GetOlapAggregateField(aggregation.Output, "count"));
    if (!Read.GetIUType(*aggregation.Column, Ctx)->IsOptionalOrNull()) {
        return Ctx.NewList(Pos, {std::move(sum), std::move(count)});
    }

    // An optional column with no values in the group has no sum, and no state.
    const TTypeAnnotationNode::TListType stateItems{
        Ctx.MakeType<TDataExprType>(EDataSlot::Double), Ctx.MakeType<TDataExprType>(EDataSlot::Uint64)};
    const auto* stateType = Ctx.MakeType<TOptionalExprType>(Ctx.MakeType<TTupleExprType>(stateItems));
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Add(0, std::move(sum))
            .Lambda(1)
                .Param("sum")
                .Callable(0, "Just")
                    .List(0)
                        .Arg(0, "sum")
                        .Add(1, std::move(count))
                    .Seal()
                .Seal()
            .Seal()
            .Callable(2, "Nothing")
                .Add(0, ExpandType(Pos, *stateType, Ctx))
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalSourceBuilder::BuildPhysicalOp() {
    TExprNode::TPtr source;
    TVector<TString> storageColumns;
    TVector<std::pair<TString, TString>> renames;
    THashMap<TString, TString> olapNames;
    for (const auto id : Read.GetColumns()) {
        const auto column = Registry.Get(id).GetColumnName();
        storageColumns.push_back(column);
        renames.emplace_back(column, Names.Get(id));
        olapNames.emplace(Ctx.GetIndexAsString(id), column);
    }
    // OLAP execution needs a nonempty storage projection to retain row counts.
    // The carrier has no logical ID and is dropped by the NarrowMap below.
    if (storageColumns.empty() && Read.GetTableStorageType() == NYql::EStorageType::ColumnStorage) {
        Y_ENSURE(!CarrierColumn.empty(), "An empty OLAP payload needs a storage carrier column");
        storageColumns.push_back(CarrierColumn);
    }
    // Block reads expose TStructExprType's lexical field order, not ID order.
    // Fetch a storage field once even when multiple logical IDs refer to it.
    std::sort(storageColumns.begin(), storageColumns.end());
    storageColumns.erase(std::unique(storageColumns.begin(), storageColumns.end()), storageColumns.end());
    TVector<TExprNode::TPtr> columns;
    for (const auto& column : storageColumns) {
        columns.push_back(Ctx.NewAtom(Pos, column));
    }
    // Extract ranges.
    TExprNode::TPtr ranges = Read.GetRanges() ? Read.GetRanges() : Build<TCoVoid>(Ctx, Pos).Done().Ptr();

    switch (Read.GetTableStorageType()) {
        case NYql::EStorageType::RowStorage: {
            // A literal range is passed to the source as is, so it needs no materialization.
            if (const auto literalRange = Read.GetLiteralRange()) {
                ranges = literalRange;
            }

            TKqpReadTableSettings settings;
            if (Read.SortDir != ESortDir::None) {
                settings.SetSorting(Read.SortDir == ESortDir::Asc ? ERequestSorting::ASC : ERequestSorting::DESC);
                if (Read.Limit) {
                    settings.SetItemsLimit(Read.Limit);
                }
            }

            TExprNode::TPtr sourceSettings;
            // System views have no datashard partitions and need their own reader.
            if (IsSysView) {
                // clang-format off
                sourceSettings = Build<TKqpReadSysViewSourceSettings>(Ctx, Pos)
                    .Table(Read.TableCallable)
                    .Columns().Add(columns).Build()
                    .Settings(settings.BuildNode(Ctx, Pos))
                    .RangesExpr(ranges)
                .Done().Ptr();
                // clang-format on
            } else {
                // clang-format off
                sourceSettings = Build<TKqpReadRangesSourceSettings>(Ctx, Pos)
                    .Table(Read.TableCallable)
                    .Columns()
                        .Add(columns)
                    .Build()
                    .Settings(settings.BuildNode(Ctx, Pos))
                    .RangesExpr(ranges)
                    .ExplainPrompt<TCoNameValueTupleList>().Build()
                .Done().Ptr();
                // clang-format on
            }

            // clang-format off
            source = Build<TDqSource>(Ctx, Pos)
                .DataSource<TCoDataSource>()
                    .Category<TCoAtom>().Build(IsSysView ? NYql::KqpSysViewSourceName : NYql::KqpReadRangesSourceName)
                .Build()
                .Settings(sourceSettings)
            .Done().Ptr();
            // clang-format on

            const auto programArg = Build<TCoArgument>(Ctx, Pos).Name("program_arg").Done().Ptr();
            const auto renameMap = NPhysicalConvertionUtils::BuildRenameMap(programArg, renames, Ctx);
            // clang-format off
            source = Build<TDqPhyStage>(Ctx, Pos)
                .Inputs()
                    .Add({source})
                .Build()
                .Program()
                    .Args({programArg})
                    .Body(renameMap)
                .Build()
                .Settings(NYql::NDq::TDqStageSettings::New(StageGUID).BuildNode(Ctx, Pos))
            .Done().Ptr();
            // clang-format on
            break;
        }
        case NYql::EStorageType::ColumnStorage: {
            // clang-format off
            auto processLambda = Build<TCoLambda>(Ctx, Pos)
                .Args({"arg"})
                .Body("arg")
            .Done().Ptr();
            // clang-format on

            if (Read.OlapFilterLambda) {
                // Do not carry the optimizer's ID-keyed argument type across the
                // storage-name boundary. The read annotator supplies the storage row.
                processLambda = Ctx.DeepCopyLambda(*NOpt::TOlapFilterInspector::RenameColumns(
                    Read.OlapFilterLambda, olapNames, Ctx));
            }

            // The program row fields, in the lexical order of block reads.
            TVector<TString> outputFields;
            if (OlapAggregate) {
                processLambda = AddOlapAggregate(processLambda, outputFields);
            } else if (OlapDistinct) {
                processLambda = AddOlapDistinct(processLambda, outputFields);
            } else {
                outputFields = storageColumns;
            }

            TKqpReadTableSettings settings;
            if (Read.Limit) {
                settings.SetItemsLimit(Read.Limit);
            } else if (OlapDistinctLimit) {
                // Column shards count distinct keys, not rows, against this limit.
                // clang-format off
                settings.SetItemsLimit(Build<TCoUint64>(Ctx, Pos)
                    .Literal<TCoAtom>()
                        .Value(ToString(OlapDistinctLimit))
                    .Build()
                .Done().Ptr());
                // clang-format on
            }

            if (Read.SortDir != ESortDir::None) {
                const auto sortDirection = Read.SortDir == ESortDir::Asc ? ERequestSorting::ASC : ERequestSorting::DESC;
                settings.SetSorting(sortDirection);
            } else if (Read.Limit) {
                // Limit without sort.
                settings.SequentialInFlight = 1;
            }

            // clang-format off
            auto olapRead = Build<TKqpBlockReadOlapTableRanges>(Ctx, Pos)
                .Table(Read.TableCallable)
                .Ranges(ranges)
                .Columns().Add(columns).Build()
                .Settings(settings.BuildNode(Ctx, Pos))
                .ExplainPrompt<TCoNameValueTupleList>().Build()
                .Process(processLambda)
            .Done().Ptr();

            // From blocks.
            auto flowNonBlockRead = Build<TCoToFlow>(Ctx, Pos)
                .Input<TCoWideFromBlocks>()
                    .Input<TCoFromFlow>()
                        .Input(olapRead)
                    .Build()
                .Build()
            .Done().Ptr();
            // clang-format on

            TExprNode::TListType args;
            TProgramFields fieldArgs;
            for (ui32 i = 0; i < outputFields.size(); ++i) {
                args.push_back(Ctx.NewArgument(Pos, "column_" + ToString(i)));
                fieldArgs.emplace(outputFields[i], args.back());
            }
            TExprNode::TPtr row;
            if (OlapAggregate) {
                row = BuildOlapAggregateRow(fieldArgs);
            } else {
                TExprNode::TListType fields;
                for (const auto& [column, name] : renames) {
                    // A DISTINCT program yields only its key column.
                    if (OlapDistinct && !fieldArgs.contains(column)) {
                        continue;
                    }
                    fields.push_back(Ctx.NewList(Pos, {Ctx.NewAtom(Pos, name), fieldArgs.at(column)}));
                }
                row = Ctx.NewCallable(Pos, "AsStruct", std::move(fields));
            }
            auto lambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(args)), std::move(row));
            auto narrowMap = Build<TCoNarrowMap>(Ctx, Pos).Input(flowNonBlockRead).Lambda(lambda).Done().Ptr();

            // clang-format off
            source = Build<TCoFromFlow>(Ctx, Pos)
                .Input(narrowMap)
            .Done().Ptr();
            // clang-format on
            break;
        }
        default:
            Y_ENSURE(false, "Unsupported table source type.");
    }

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical source] " << KqpExprToPrettyString(TExprBase(source), Ctx);

    return source;
}
