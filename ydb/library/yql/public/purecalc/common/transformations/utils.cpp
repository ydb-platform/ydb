#include "utils.h"

#include <ydb/library/yql/public/purecalc/common/names.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

using namespace NYql;
using namespace NYql::NPureCalc;

TExprNode::TPtr NYql::NPureCalc::NodeFromBlocks(
    const TPositionHandle& pos,
    const TStructExprType* structType,
    TExprContext& ctx
) {
    const auto items = structType->GetItems();
    Y_ENSURE(items.size() > 0);
    return ctx.Builder(pos)
        .Lambda()
            .Param("stream")
            .Callable(0, "FromFlow")
                .Callable(0, "NarrowMap")
                    .Callable(0, "WideFromBlocks")
                        .Callable(0, "ExpandMap")
                            .Callable(0, "ToFlow")
                                .Arg(0, "stream")
                            .Seal()
                            .Lambda(1)
                                .Param("item")
                                .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                                    ui32 i = 0;
                                    for (const auto& item : items) {
                                        lambda.Callable(i++, "Member")
                                            .Arg(0, "item")
                                            .Atom(1, item->GetName())
                                        .Seal();
                                    }
                                    lambda.Callable(i, "Member")
                                        .Arg(0, "item")
                                        .Atom(1, PurecalcBlockColumnLength)
                                    .Seal();
                                    return lambda;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                    .Lambda(1)
                        .Params("fields", items.size())
                        .Callable("AsStruct")
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    ui32 i = 0;
                                    for (const auto& item : items) {
                                        parent.List(i)
                                            .Atom(0, item->GetName())
                                            .Arg(1, "fields", i++)
                                        .Seal();
                                    }
                                    return parent;
                                })
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr NYql::NPureCalc::NodeToBlocks(
    const TPositionHandle& pos,
    const TStructExprType* structType,
    TExprContext& ctx
) {
    const auto items = structType->GetItems();
    Y_ENSURE(items.size() > 0);
    return ctx.Builder(pos)
        .Lambda()
            .Param("stream")
            .Callable("FromFlow")
                .Callable(0, "NarrowMap")
                    .Callable(0, "WideToBlocks")
                        .Callable(0, "ExpandMap")
                            .Callable(0, "ToFlow")
                                .Arg(0, "stream")
                            .Seal()
                            .Lambda(1)
                                .Param("item")
                                .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                                    ui32 i = 0;
                                    for (const auto& item : items) {
                                        lambda.Callable(i++, "Member")
                                            .Arg(0, "item")
                                            .Atom(1, item->GetName())
                                        .Seal();
                                    }
                                    return lambda;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                    .Lambda(1)
                        .Params("fields", items.size() + 1)
                        .Callable("AsStruct")
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                ui32 i = 0;
                                for (const auto& item : items) {
                                    parent.List(i)
                                        .Atom(0, item->GetName())
                                        .Arg(1, "fields", i++)
                                    .Seal();
                                }
                                parent.List(i)
                                    .Atom(0, PurecalcBlockColumnLength)
                                    .Arg(1, "fields", i)
                                .Seal();
                                return parent;
                            })
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr NYql::NPureCalc::ApplyToIterable(
    const TPositionHandle& pos,
    const TExprNode::TPtr iterable,
    const TExprNode::TPtr lambda,
    bool wrapLMap,
    TExprContext& ctx
) {
    if (wrapLMap) {
        return ctx.Builder(pos)
            .Callable("LMap")
                .Add(0, iterable)
                .Lambda(1)
                    .Param("stream")
                    .Apply(lambda)
                        .With(0, "stream")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    } else {
        return ctx.Builder(pos)
            .Apply(lambda)
                .With(0, iterable)
            .Seal()
            .Build();
    }
}

const TStructExprType* NYql::NPureCalc::WrapBlockStruct(
    const TStructExprType* structType,
    TExprContext& ctx
) {
    TVector<const TItemExprType*> members;
    for (const auto& item : structType->GetItems()) {
        const auto blockItemType = ctx.MakeType<TBlockExprType>(item->GetItemType());
        members.push_back(ctx.MakeType<TItemExprType>(item->GetName(), blockItemType));
    }
    const auto scalarItemType = ctx.MakeType<TScalarExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));
    members.push_back(ctx.MakeType<TItemExprType>(PurecalcBlockColumnLength, scalarItemType));
    return ctx.MakeType<TStructExprType>(members);
}

const TStructExprType* NYql::NPureCalc::UnwrapBlockStruct(
    const TStructExprType* structType,
    TExprContext& ctx
) {
    TVector<const TItemExprType*> members;
    for (const auto& item : structType->GetItems()) {
        if (item->GetName() == PurecalcBlockColumnLength) {
            continue;
        }
        bool isScalarUnused;
        const auto blockItemType = GetBlockItemType(*item->GetItemType(), isScalarUnused);
        members.push_back(ctx.MakeType<TItemExprType>(item->GetName(), blockItemType));
    }
    return ctx.MakeType<TStructExprType>(members);
}
