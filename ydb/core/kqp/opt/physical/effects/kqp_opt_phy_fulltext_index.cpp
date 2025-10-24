#include "kqp_opt_phy_effects_impl.h"
#include <yql/essentials/providers/common/provider/yql_provider.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TExprBase BuildFulltextIndexRows(const TKikimrTableDescription& table, const TIndexDescription* indexDesc,
    const NNodes::TExprBase& inputRows, const THashSet<TStringBuf>& inputColumns, TVector<TStringBuf>& indexTableColumns, bool includeDataColumns,
    TPositionHandle pos, NYql::TExprContext& ctx)
{
    // Extract fulltext index settings
    const auto* fulltextDesc = std::get_if<NKikimrKqp::TFulltextIndexDescription>(&indexDesc->SpecializedIndexDescription);
    YQL_ENSURE(fulltextDesc, "Expected fulltext index description");
    
    const auto& settings = fulltextDesc->GetSettings();
    YQL_ENSURE(settings.columns().size() == 1, "Expected single text column in fulltext index");
    
    const TString textColumn = settings.columns().at(0).column();
    const auto& analyzers = settings.columns().at(0).analyzers();
    
    // Serialize analyzer settings for runtime usage
    TString settingsProto;
    YQL_ENSURE(analyzers.SerializeToString(&settingsProto));
    
    auto inputRowArg = TCoArgument(ctx.NewArgument(pos, "input_row"));
    auto tokenArg = TCoArgument(ctx.NewArgument(pos, "token"));
    
    // Build output row structure for each token
    TVector<TExprBase> tokenRowTuples;
    
    indexTableColumns.clear();
    auto addIndexColumn = [&](const TStringBuf& column) {
        indexTableColumns.emplace_back(column);
        auto columnAtom = ctx.NewAtom(pos, column);
        if (inputColumns.contains(column)) {
            auto tuple = Build<TCoNameValueTuple>(ctx, pos)
                .Name(columnAtom)
                .Value<TCoMember>()
                    .Struct(inputRowArg)
                    .Name(columnAtom)
                    .Build()
                .Done();
            tokenRowTuples.emplace_back(tuple);
        } else {
            auto columnType = table.GetColumnType(TString(column));
            auto tuple = Build<TCoNameValueTuple>(ctx, pos)
                .Name(columnAtom)
                .Value<TCoNothing>()
                    .OptionalType(NCommon::BuildTypeExpr(pos, *columnType, ctx))
                    .Build()
                .Done();
            tokenRowTuples.emplace_back(tuple);
        }
    };

    // Add token column first
    indexTableColumns.emplace_back(NTableIndex::NFulltext::TokenColumn);
    auto tokenTuple = Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build(NTableIndex::NFulltext::TokenColumn)
        .Value(tokenArg)
        .Done();
    tokenRowTuples.emplace_back(tokenTuple);

    // Add primary key columns
    for (const auto& column : table.Metadata->KeyColumnNames) {
        addIndexColumn(column);
    }

    // Add data columns (covered columns)
    if (includeDataColumns) {
        for (const auto& column : indexDesc->DataColumns) {
            addIndexColumn(column);
        }
    }
    
    // Create lambda that builds output row for each token
    // FlatMap expects lambda to return list/stream/optional, so wrap struct in Just
    auto tokenRowsLambda = Build<TCoLambda>(ctx, pos)
        .Args({tokenArg})
        .Body<TCoJust>()
            .Input<TCoAsStruct>()
                .Add(tokenRowTuples)
                .Build()
            .Build()
        .Done();
    
    // Get text member from input row
    auto textMember = Build<TCoMember>(ctx, pos)
        .Struct(inputRowArg)
        .Name().Build(textColumn)
        .Done();
    
    // Create callable for fulltext tokenization
    // Format: FulltextAnalyze(text: String, settings: String) -> List<String>
    auto settingsLiteral = Build<TCoString>(ctx, pos)
        .Literal().Build(settingsProto)
        .Done();
    
    auto analyzeCallable = ctx.Builder(pos)
        .Callable("FulltextAnalyze")
            .Add(0, textMember.Ptr())
            .Add(1, settingsLiteral.Ptr())
        .Seal()
        .Build();
    
    auto analyzeStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputRows)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoIterator>()
                .List<TCoFlatMap>()
                    .Input("rows")
                    .Lambda()
                        .Args({inputRowArg})
                        .Body<TCoFlatMap>()
                            .Input(analyzeCallable)
                            .Lambda(tokenRowsLambda)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();
    
    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(analyzeStage)
            .Index().Build("0")
            .Build()
        .Done();
}

}
