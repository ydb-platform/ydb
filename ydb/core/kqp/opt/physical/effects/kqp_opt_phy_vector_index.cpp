#include "kqp_opt_phy_effects_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TDqStageBase ReadTableToStage(const TExprBase& expr, TExprContext& ctx) {
    if (expr.Maybe<TDqStageBase>()) {
        return expr.Cast<TDqStageBase>();
    }
    if (expr.Maybe<TDqCnUnionAll>()) {
        return expr.Cast<TDqCnUnionAll>().Output().Stage();
    }
    auto pos = expr.Pos();
    TVector<TExprNode::TPtr> inputs;
    TVector<TExprNode::TPtr> args;
    TNodeOnNodeOwnedMap replaces;
    int i = 1;
    VisitExpr(expr.Ptr(), [&](const TExprNode::TPtr& node) {
        TExprBase expr(node);
        if (auto cast = expr.Maybe<TDqCnUnionAll>()) {
            auto newArg = ctx.NewArgument(pos, TStringBuilder() << "rows" << i);
            inputs.emplace_back(node);
            args.emplace_back(newArg);
            replaces.emplace(expr.Raw(), newArg);
            return false;
        }
        return true;
    });
    return Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputs)
            .Build()
        .Program()
            .Args(args)
            .Body(ctx.ReplaceNodes(expr.Ptr(), replaces))
            .Build()
        .Settings()
            .Build()
        .Done();
}

TExprBase BuildVectorIndexPostingRows(const TKikimrTableDescription& table,
    const TKqpTable& tableNode,
    const TString& indexName,
    const TVector<TStringBuf>& indexTableColumns,
    const TExprBase& inputRows,
    bool withData,
    TPositionHandle pos, TExprContext& ctx) {
    // Generate input type for vector resolve
    TVector<const TItemExprType*> rowItems;
    for (const auto& column : indexTableColumns) {
        auto type = table.GetColumnType(TString(column));
        YQL_ENSURE(type, "No key column: " << column);
        auto itemType = ctx.MakeType<TItemExprType>(column, type);
        YQL_ENSURE(itemType->Validate(pos, ctx));
        rowItems.push_back(itemType);
    }
    auto rowType = ctx.MakeType<TStructExprType>(rowItems);
    YQL_ENSURE(rowType->Validate(pos, ctx));
    const TTypeAnnotationNode* resolveInputType = ctx.MakeType<TListExprType>(rowType);

    auto resolveInput = (inputRows.Maybe<TDqCnUnionAll>()
        ? inputRows.Maybe<TDqCnUnionAll>().Cast().Output()
        : Build<TDqOutput>(ctx, pos)
            .Stage(ReadTableToStage(inputRows, ctx))
            .Index().Build(0)
            .Done());

    auto resolveOutput = Build<TKqpCnVectorResolve>(ctx, pos)
        .Output(resolveInput)
        .Table(tableNode)
        .InputType(ExpandType(pos, *resolveInputType, ctx))
        .Index(ctx.NewAtom(pos, indexName))
        .WithData(ctx.NewAtom(pos, withData ? "true" : "false"))
        .Done();

    auto resolveStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(resolveOutput)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoToStream>()
                .Input("rows")
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(resolveStage)
            .Index().Build(0)
            .Build()
        .Done();
}

TVector<TStringBuf> BuildVectorIndexPostingColumns(const TKikimrTableDescription& table,
    const TIndexDescription* indexDesc) {
    TVector<TStringBuf> indexTableColumns;
    THashSet<TStringBuf> indexTableColumnSet;

    indexTableColumns.emplace_back(NTableIndex::NKMeans::ParentColumn);
    indexTableColumnSet.insert(NTableIndex::NKMeans::ParentColumn);

    for (const auto& column : table.Metadata->KeyColumnNames) {
        if (indexTableColumnSet.insert(column).second) {
            indexTableColumns.emplace_back(column);
        }
    }

    for (const auto& column : indexDesc->DataColumns) {
        if (indexTableColumnSet.insert(column).second) {
            indexTableColumns.emplace_back(column);
        }
    }

    return indexTableColumns;
}

} // namespace NKikimr::NKqp::NOpt
