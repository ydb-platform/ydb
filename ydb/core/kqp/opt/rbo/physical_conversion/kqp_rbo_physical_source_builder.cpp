#include "kqp_rbo_physical_source_builder.h"

#include <ydb/core/kqp/opt/rbo/kqp_olap_expr_inspection.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <yql/essentials/core/yql_expr_optimize.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

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
    THashMap<TString, ui32> positions;
    for (const auto& column : storageColumns) {
        positions.emplace(column, columns.size());
        columns.push_back(Ctx.NewAtom(Pos, column));
    }
    // Extract ranges.
    TExprNode::TPtr ranges = Read.GetRanges() ? Read.GetRanges() : Build<TCoVoid>(Ctx, Pos).Done().Ptr();

    switch (Read.GetTableStorageType()) {
        case NYql::EStorageType::RowStorage: {
            TKqpReadTableSettings settings;
            if (Read.SortDir != ESortDir::None) {
                settings.SetSorting(Read.SortDir == ESortDir::Asc ? ERequestSorting::ASC : ERequestSorting::DESC);
                if (Read.Limit) {
                    settings.SetItemsLimit(Read.Limit);
                }
            }

            // clang-format off
            source = Build<TDqSource>(Ctx, Pos)
                .DataSource<TCoDataSource>()
                    .Category<TCoAtom>().Build("KqpReadRangesSource")
                .Build()
                .Settings<TKqpReadRangesSourceSettings>()
                    .Table(Read.TableCallable)
                    .Columns()
                        .Add(columns)
                    .Build()
                    .Settings(settings.BuildNode(Ctx, Pos))
                    .RangesExpr(ranges)
                    .ExplainPrompt<TCoNameValueTupleList>().Build()
                .Build()
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

            TKqpReadTableSettings settings;
            if (Read.Limit) {
                settings.SetItemsLimit(Read.Limit);
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
            for (ui32 i = 0; i < storageColumns.size(); ++i) {
                args.push_back(Ctx.NewArgument(Pos, "column_" + ToString(i)));
            }
            TExprNode::TListType fields;
            for (const auto& [column, name] : renames) {
                fields.push_back(Ctx.NewList(Pos, {Ctx.NewAtom(Pos, name), args.at(positions.at(column))}));
            }
            auto row = Ctx.NewCallable(Pos, "AsStruct", std::move(fields));
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
