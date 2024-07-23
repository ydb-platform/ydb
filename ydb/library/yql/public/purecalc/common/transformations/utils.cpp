#include "utils.h"

using namespace NYql;
using namespace NYql::NPureCalc;

TExprNode::TPtr NYql::NPureCalc::NodeFromBlocks(
    const TPositionHandle& pos,
    const TStructExprType* structType,
    TExprContext& ctx
) {
    const auto items = structType->GetItems();
    Y_ENSURE(items.size() > 1);
    const auto blockLengthValue = structType->FindItem("_yql_block_length");
    Y_ENSURE(blockLengthValue);
    const ui32 blockLengthIndex = *blockLengthValue;
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
                                    for (ui32 j = 0; j < items.size(); j++) {
                                        if (j == blockLengthIndex) {
                                            continue;
                                        }
                                        lambda.Callable(i++, "Member")
                                            .Arg(0, "item")
                                            .Atom(1, items[j]->GetName())
                                        .Seal();
                                    }
                                    lambda.Callable(i, "Member")
                                        .Arg(0, "item")
                                        .Atom(1, items[blockLengthIndex]->GetName())
                                    .Seal();
                                    return lambda;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                    .Lambda(1)
                        .Params("fields", items.size() - 1)
                        .Callable("AsStruct")
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    ui32 i = 0;
                                    for (ui32 j = 0; j < items.size(); j++) {
                                        if (j == blockLengthIndex) {
                                            continue;
                                        }
                                        parent.List(i)
                                            .Atom(0, items[j]->GetName())
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
                                    .Atom(0, "_yql_block_length")
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
