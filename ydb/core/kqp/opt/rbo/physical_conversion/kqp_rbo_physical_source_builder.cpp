#include "kqp_rbo_physical_source_builder.h"
#include <yql/essentials/core/yql_expr_optimize.h>
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalSourceBuilder::BuildPhysicalOp() {
    TExprNode::TPtr source;
    TVector<TExprNode::TPtr> columns;
    for (const auto& column : Read->Columns) {
        columns.push_back(Ctx.NewAtom(Pos, column));
    }

    switch (Read->GetTableStorageType()) {
        case NYql::EStorageType::RowStorage: {
            // clang-format off
            source = Build<TDqSource>(Ctx, Pos)
                .DataSource<TCoDataSource>()
                    .Category<TCoAtom>().Build("KqpReadRangesSource")
                .Build()
                .Settings<TKqpReadRangesSourceSettings>()
                    .Table(Read->TableCallable)
                    .Columns()
                        .Add(columns)
                    .Build()
                    .Settings<TCoNameValueTupleList>().Build()
                    .RangesExpr<TCoVoid>().Build()
                    .ExplainPrompt<TCoNameValueTupleList>().Build()
                .Build()
            .Done().Ptr();
            // clang-format on

            const auto& columns = Read->Columns;
            const auto& outputs = Read->OutputIUs;
            Y_ENSURE(columns.size() == outputs.size());

            TVector<std::pair<TString, TString>> renames;
            for (ui32 i = 0; i < columns.size(); ++i) {
                renames.emplace_back(columns[i], outputs[i].GetFullName());
            }

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
                .Settings().Build()
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

            if (Read->OlapFilterLambda) {
                processLambda = Read->OlapFilterLambda;
            }

            TKqpReadTableSettings settings;
            if (Read->Limit) {
                settings.SetItemsLimit(Read->Limit);
            }

            if (Read->SortDir != ESortDir::None) {
                const auto sortDirection = Read->SortDir == ESortDir::Asc ? ERequestSorting::ASC : ERequestSorting::DESC;
                settings.SetSorting(sortDirection);
            } else if (Read->Limit) {
                // Limit without sort.
                settings.SequentialInFlight = 1;
            }

            TExprNode::TPtr ranges = Read->GetRanges() ? Read->GetRanges() : Build<TCoVoid>(Ctx, Pos).Done().Ptr();
            // clang-format off
            auto olapRead = Build<TKqpBlockReadOlapTableRanges>(Ctx, Pos)
                .Table(Read->TableCallable)
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

            auto narrowMap = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(flowNonBlockRead, Read->OutputIUs, Ctx);

            // clang-format off
            source = Build<TCoFromFlow>(Ctx, Pos)
                .Input(narrowMap)
            .Done().Ptr();
            // clang-format on
            break;
        }
        default:
            Y_ENSURE(false, "Unsupported table source type");
    }

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical source] " << KqpExprToPrettyString(TExprBase(source), Ctx);

    return source;
}
