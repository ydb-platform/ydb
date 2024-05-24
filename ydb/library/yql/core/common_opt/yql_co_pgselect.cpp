#include "yql_co_pgselect.h"

#include <ydb/library/yql/core/type_ann/type_ann_pg.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NYql {

TExprNode::TPtr WrapWithNonNegativeCheck(TPositionHandle pos, const TExprNode::TPtr& value, TExprContext& ctx, const TString& message) {
    return ctx.Builder(pos)
        .Callable("Ensure")
            .Add(0, value)
            .Callable(1, "FromPg")
                .Callable(0, "PgOp")
                    .Atom(0, ">=")
                    .Add(1, value)
                    .Callable(2, "PgConst")
                        .Atom(0, "0")
                        .Callable(1, "PgType")
                            .Atom(0, "int8")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Callable(2, "String")
                .Atom(0, message)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildArrayAggTraits(TExprContext& ctx, TPositionHandle pos, const TExprNode::TPtr& typeNode) {
    return ctx.Builder(pos)
        .Callable("AggApply")
            .Atom(0, "pg_array_agg")
            .Callable(1, "ListItemType")
                .Add(0, typeNode)
            .Seal()
            .Lambda(2)
                .Param("row")
                .Callable("SingleMember")
                    .Callable(0, "RemoveSystemMembers")
                        .Arg(0, "row")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TNodeMap<ui32> GatherSubLinks(const TExprNode::TPtr& root) {
    TNodeMap<ui32> subLinks;

    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgSubLink")) {
            subLinks[node.Get()] = subLinks.size();
            return false;
        }

        return true;
    });

    return subLinks;
}

TExprNode::TPtr AsFilterPredicate(TPositionHandle pos, const TExprNode::TPtr& expr, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Coalesce")
            .Callable(0, "FromPg")
                .Add(0, expr)
            .Seal()
            .Callable(1, "Bool")
                .Atom(0, "0")
            .Seal()
        .Seal()
        .Build();
}

std::pair<TExprNode::TPtr, TExprNode::TPtr> SplitByPredicate(TPositionHandle pos, const TExprNode::TPtr& input,
    const TExprNode::TPtr& predicate, const TExprNode::TPtr& args, TExprContext& ctx) {
    auto coalescedPredicate = AsFilterPredicate(pos, predicate, ctx);
    auto inversePredicate = ctx.NewCallable(pos, "Not", { coalescedPredicate });
    auto lambda = ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .ApplyPartial(args, coalescedPredicate)
                .With(0, "row")
            .Seal()
        .Seal()
        .Build();

    auto inverseLambda = ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .ApplyPartial(args, inversePredicate)
                .With(0, "row")
            .Seal()
        .Seal()
        .Build();

    return {
        ctx.NewCallable(pos, "OrderedFilter", { input, lambda }),
        ctx.NewCallable(pos, "OrderedFilter", { input, inverseLambda })
    };
}

TExprNode::TPtr JoinColumns(TPositionHandle pos, const TExprNode::TPtr& list1, const TExprNode::TPtr& list2,
    TExprNode::TPtr leftJoinColumns, TMaybe<ui32> subLinkId, TExprContext& ctx, const TString& leftPrefix = {}) {
    auto join = ctx.Builder(pos)
        .Callable("EquiJoin")
            .List(0)
                .Add(0, list1)
                .Atom(1, "a")
            .Seal()
            .List(1)
                .Add(0, list2)
                .Atom(1, "b")
            .Seal()
            .List(2)
                .Atom(0, leftJoinColumns ? "Left" : "Cross")
                .Atom(1, "a")
                .Atom(2, "b")
                .List(3)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                        if (leftJoinColumns) {
                            for (ui32 i = 0; i < leftJoinColumns->ChildrenSize(); ++i) {
                                parent.Atom(2 * i, "a");
                                parent.Atom(2* i + 1, leftPrefix + leftJoinColumns->ChildPtr(i)->Content());
                            }
                        }

                        return parent;
                    })
                .Seal()
                .List(4)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                        if (leftJoinColumns) {
                            for (ui32 i = 0; i < leftJoinColumns->ChildrenSize(); ++i) {
                                parent.Atom(2 * i, "b");
                                parent.Atom(2 * i + 1, subLinkId ? TString("_yql_join_sublink_") + ToString(*subLinkId) +
                                    "_" + leftJoinColumns->Child(i)->Content() : leftJoinColumns->ChildPtr(i)->Content());
                            }
                        }

                        return parent;
                    })
                .Seal()
                .List(5)
                .Seal()
            .Seal()
            .List(3)
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                    if (leftJoinColumns) {
                        for (ui32 i = 0; i < leftJoinColumns->ChildrenSize(); ++i) {
                            parent.List(i)
                                .Atom(0, "rename")
                                .Atom(1, TString("b.") + (subLinkId ? (TString("_yql_join_sublink_") + ToString(*subLinkId) +
                                    "_") : "") + leftJoinColumns->Child(i)->Content())
                                .Atom(2, "")
                                .Seal();
                        }
                    }

                    return parent;
                })
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, join)
            .Lambda(1)
                .Param("row")
                .Callable("DivePrefixMembers")
                    .Arg(0, "row")
                    .List(1)
                        .Atom(0, "a.")
                        .Atom(1, "b.")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

using TAggregationMap = TNodeMap<std::pair<ui32, bool>>; // uid + sublink test expression
using TAggs = TVector<std::pair<TExprNode::TPtr, TExprNode::TPtr>>;

void RewriteAggs(TExprNode::TPtr& lambda, const TAggregationMap& aggId, TExprContext& ctx, TOptimizeContext& optCtx, bool testExpr);

void RewriteAggsPartial(TExprNode::TPtr& root, const TExprNode::TPtr& arg, const TAggregationMap& aggId, TExprContext& ctx, TOptimizeContext& optCtx, bool testExpr) {
    auto subLinks = GatherSubLinks(root);
    auto status = OptimizeExpr(root, root, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        auto subIt = subLinks.find(node.Get());
        if (subIt != subLinks.end()) {
            if (!node->Child(3)->IsCallable("Void")) {
                auto lambda = node->ChildPtr(3);
                RewriteAggs(lambda, aggId, ctx, optCtx, true);
                return ctx.ChangeChild(*node, 3, std::move(lambda));
            }
        }

        auto it = aggId.find(node.Get());
        if (it != aggId.end() && it->second.second == testExpr) {
            auto ret = ctx.Builder(node->Pos())
                .Callable("Member")
                    .Add(0, arg)
                    .Atom(1, "_yql_agg_" + ToString(it->second.first))
                .Seal()
                .Build();

            return ret;
        }

        return node;
    }, ctx, TOptimizeExprSettings(optCtx.Types));

    YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error);
}

void RewriteAggs(TExprNode::TPtr& lambda, const TAggregationMap& aggId, TExprContext& ctx, TOptimizeContext& optCtx, bool testExpr) {
    RewriteAggsPartial(lambda, lambda->Head().HeadPtr(), aggId, ctx, optCtx, testExpr);
};

std::pair<TExprNode::TPtr, TExprNode::TPtr> RewriteSubLinksPartial(TPositionHandle pos,
    const TExprNode::TPtr& list, const TExprNode::TPtr& root, const TExprNode::TPtr& originalArg,
    const TNodeMap<ui32>& subLinks, const TVector<TString>& inputAliases,
    const TExprNode::TListType& cleanedInputs,
    TExprContext& ctx, TOptimizeContext& optCtx,
    const TString& leftPrefix = {}, TVector<TString>* sublinkColumns = nullptr) {
    auto newRoot = root;
    auto newList = list;
    TNodeOnNodeOwnedMap deepClones;
    auto status = OptimizeExpr(newRoot, newRoot, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        auto it = subLinks.find(node.Get());
        if (it != subLinks.end()) {
            auto linkType = node->Head().Content();
            auto testLambda = node->ChildPtr(3);
            auto extColumns = NTypeAnnImpl::ExtractExternalColumns(node->Tail());
            if (extColumns.empty()) {
                auto select = ExpandPgSelectSublink(node->TailPtr(), ctx, optCtx, it->second, cleanedInputs, inputAliases);
                if (linkType == "exists") {
                    return ctx.Builder(node->Pos())
                        .Callable("ToPg")
                            .Callable(0, ">")
                                .Callable(0, "Length")
                                    .Callable(0, "Take")
                                        .Add(0, select)
                                        .Callable(1, "Uint64")
                                            .Atom(0, "1")
                                        .Seal()
                                    .Seal()
                                .Seal()
                                .Callable(1, "Uint64")
                                    .Atom(0, "0")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                } else if (linkType == "expr") {
                    auto take2 = ctx.Builder(node->Pos())
                        .Callable("Take")
                            .Add(0, select)
                            .Callable(1, "Uint64")
                                .Atom(0, "2")
                            .Seal()
                        .Seal()
                        .Build();

                    return ctx.Builder(node->Pos())
                        .Callable("Ensure")
                            .Callable(0, "SingleMember")
                                .Callable(0, "ToOptional")
                                    .Add(0, take2)
                                .Seal()
                            .Seal()
                            .Callable(1, "<=")
                                .Callable(0, "Length")
                                    .Add(0, take2)
                                .Seal()
                                .Callable(1, "Uint64")
                                    .Atom(0, "1")
                                .Seal()
                            .Seal()
                            .Callable(2, "String")
                                .Atom(0, "More than one row returned by a subquery used as an expression")
                            .Seal()
                        .Seal()
                        .Build();
                } else if (linkType == "array") {
                    auto typeNode = ctx.NewCallable(node->Pos(), "TypeOf", { select });
                    auto aggTraits = BuildArrayAggTraits(ctx, node->Pos(), typeNode);
                    auto aggregate = ctx.Builder(node->Pos())
                        .Callable("Aggregate")
                            .Add(0, select)
                            .List(1)
                            .Seal()
                            .List(2)
                                .List(0)
                                    .Atom(0, "a")
                                    .Add(1, aggTraits)
                                .Seal()
                            .Seal()
                            .List(3)
                            .Seal()
                        .Seal()
                        .Build();

                    return ctx.Builder(node->Pos())
                        .Callable("SingleMember")
                            .Callable(0, "ToOptional")
                                .Add(0, aggregate)
                            .Seal()
                        .Seal()
                        .Build();
                } else if (linkType == "any" || linkType == "all") {
                    const bool useIn = testLambda->Tail().IsCallable("PgResolvedOp")
                        && testLambda->Tail().Head().Content() == "="
                        && testLambda->Tail().Child(3) == testLambda->Head().Child(1)
                        && !IsDepended(*testLambda->Tail().Child(2), *testLambda->Head().Child(1));

                    if (useIn) {
                        auto value = ctx.ReplaceNodes(testLambda->Tail().ChildPtr(2), {
                            {testLambda->Head().Child(0), originalArg}
                            });

                        return ctx.Builder(node->Pos())
                            .Callable("ToPg")
                                .Callable(0, "SqlIn")
                                    .Add(0, select)
                                    .Add(1, value)
                                    .List(2)
                                        .List(0)
                                            .Atom(0, "ansi", TNodeFlags::Default)
                                        .Seal()
                                        .List(1)
                                            .Atom(0, "tableSource", TNodeFlags::Default)
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Build();
                    }

                    auto foldArg = ctx.NewArgument(node->Pos(), "linkRow");
                    auto stateArg = ctx.NewArgument(node->Pos(), "state");
                    auto foldArgs = ctx.NewArguments(node->Pos(), { foldArg, stateArg });
                    auto value = ctx.Builder(node->Pos())
                        .Callable("SingleMember")
                            .Add(0, foldArg)
                        .Seal()
                        .Build();

                    auto foldExpr = ctx.ReplaceNodes(testLambda->TailPtr(), {
                        {testLambda->Head().Child(0), originalArg},
                        {testLambda->Head().Child(1), value},
                        });

                    foldExpr = ctx.Builder(node->Pos())
                        .Callable((linkType == "all") ? "PgAnd" : "PgOr")
                            .Add(0, foldExpr)
                            .Add(1, stateArg)
                        .Seal()
                        .Build();

                    auto foldLambda = ctx.NewLambda(node->Pos(), std::move(foldArgs), std::move(foldExpr));

                    auto result = ctx.Builder(node->Pos())
                        .Callable("Fold")
                            .Callable(0, "Collect")
                                .Add(0, select)
                            .Seal()
                            .Callable(1, "PgConst")
                                .Atom(0, (linkType == "all") ? "true" : "false")
                                .Callable(1, "PgType")
                                    .Atom(0, "bool")
                                .Seal()
                            .Seal()
                            .Add(2, foldLambda)
                        .Seal()
                        .Build();

                    return result;
                }
            } else {
                auto fullColList = ctx.Builder(node->Pos())
                    .List()
                        .Do([&](TExprNodeBuilder &parent) -> TExprNodeBuilder & {
                            ui32 i = 0;
                            for (const auto& c : extColumns) {
                                parent.Atom(i++, c.first);
                            }

                            return parent;
                        })
                    .Seal()
                    .Build();

                auto select = ExpandPgSelectSublink(node->TailPtr(), ctx, optCtx, it->second, cleanedInputs, inputAliases);

                auto exportsPtr = optCtx.Types->Modules->GetModule("/lib/yql/aggregate.yql");
                YQL_ENSURE(exportsPtr);
                const auto& exports = exportsPtr->Symbols();

                auto selectTypeNode = ctx.Builder(node->Pos())
                    .Callable("TypeOf")
                        .Add(0, select)
                    .Seal()
                    .Build();

                TExprNode::TPtr countAllTraits;
                TExprNode::TPtr someTraits;
                TExprNode::TPtr orTraits;
                TExprNode::TPtr andTraits;
                TExprNode::TPtr aggArrayTraits = BuildArrayAggTraits(ctx, node->Pos(), selectTypeNode);
                for (ui32 factoryIndex = 0; factoryIndex < 4; ++factoryIndex)
                {
                    TStringBuf name;
                    switch (factoryIndex) {
                    case 0:
                        name = "count_all_traits_factory";
                        break;
                    case 1:
                        if (linkType != "expr") {
                            continue;
                        }

                        name = "some_traits_factory";
                        break;
                    case 2:
                        if (linkType != "any") {
                            continue;
                        }

                        name = "or_traits_factory";
                        break;
                    case 3:
                        if (linkType != "all") {
                            continue;
                        }

                        name = "and_traits_factory";
                        break;
                    }

                    const auto ex = exports.find(name);
                    YQL_ENSURE(exports.cend() != ex);
                    auto lambda = ctx.DeepCopy(*ex->second, exportsPtr->ExprCtx(), deepClones, true, false);
                    auto arg = ctx.NewArgument(node->Pos(), "row");
                    auto arguments = ctx.NewArguments(node->Pos(), { arg });
                    TExprNode::TPtr root;
                    switch (factoryIndex) {
                    case 0:
                        root = arg;
                        break;
                    case 1:
                        root = ctx.NewCallable(node->Pos(), "SingleMember", {
                            ctx.NewCallable(node->Pos(), "RemoveSystemMembers", { arg }) });
                        break;
                    case 2:
                    case 3:
                    {
                        auto value = ctx.NewCallable(node->Pos(), "SingleMember", {
                            ctx.NewCallable(node->Pos(), "RemoveSystemMembers", { arg }) });

                        auto filterExpr = ctx.ReplaceNodes(testLambda->TailPtr(), {
                            {testLambda->Head().Child(0), arg},
                            {testLambda->Head().Child(1), value},
                            });

                        auto status = OptimizeExpr(filterExpr, filterExpr, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                            if (node->IsCallable("Member") && node->Child(0) == arg.Get()) {
                                return ctx.ChangeChild(*node, 1, ctx.NewAtom(node->Pos(), TString("_yql_join_sublink_") + ToString(it->second) + "_" + node->Child(1)->Content()));
                            }

                            return node;
                        }, ctx, TOptimizeExprSettings(optCtx.Types));

                        YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error);

                        root = ctx.NewCallable(node->Pos(), "FromPg", { filterExpr });
                        break;
                    }
                    }

                    auto extractor = ctx.NewLambda(node->Pos(), std::move(arguments), std::move(root));

                    auto traits = ctx.ReplaceNodes(lambda->TailPtr(), {
                        {lambda->Head().Child(0), selectTypeNode},
                        {lambda->Head().Child(1), extractor}
                    });

                    ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
                    auto status = ExpandApply(traits, traits, ctx);
                    YQL_ENSURE(status != IGraphTransformer::TStatus::Error);
                    switch (factoryIndex) {
                    case 0:
                        countAllTraits = traits;
                        break;
                    case 1:
                        someTraits = traits;
                        break;
                    case 2:
                        orTraits = traits;
                        break;
                    case 3:
                        andTraits = traits;
                        break;
                    }
                }

                auto columnName = "_yql_sublink_" + ToString(it->second);
                TExprNode::TListType aggregateItems;
                if (linkType == "exists") {
                    if (sublinkColumns) {
                        sublinkColumns->push_back(columnName);
                    }

                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName)
                            .Add(1, countAllTraits)
                        .Seal()
                        .Build());
                } else if (linkType == "expr") {
                    if (sublinkColumns) {
                        sublinkColumns->push_back(columnName + "_count");
                        sublinkColumns->push_back(columnName + "_value");
                    }

                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName + "_count")
                            .Add(1, countAllTraits)
                        .Seal()
                        .Build());
                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName + "_value")
                            .Add(1, someTraits)
                        .Seal()
                        .Build());
                } else if (linkType == "array") {
                    if (sublinkColumns) {
                        sublinkColumns->push_back(columnName + "_value");
                    }

                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName + "_value")
                            .Add(1, aggArrayTraits)
                        .Seal()
                        .Build());
                } else if (linkType == "any") {
                    if (sublinkColumns) {
                        sublinkColumns->push_back(columnName + "_count");
                        sublinkColumns->push_back(columnName + "_value");
                    }

                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName + "_count")
                            .Add(1, countAllTraits)
                        .Seal()
                        .Build());
                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName + "_value")
                            .Add(1, orTraits)
                        .Seal()
                        .Build());
                } else {
                    YQL_ENSURE(linkType == "all");
                    if (sublinkColumns) {
                        sublinkColumns->push_back(columnName + "_count");
                        sublinkColumns->push_back(columnName + "_value");
                    }

                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName + "_count")
                            .Add(1, countAllTraits)
                        .Seal()
                        .Build());
                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName + "_value")
                            .Add(1, andTraits)
                        .Seal()
                        .Build());
                }

                auto aggregates = ctx.NewList(node->Pos(), std::move(aggregateItems));
                auto groupedSublink = ctx.Builder(node->Pos())
                    .Callable("Aggregate")
                        .Add(0, select)
                        .List(1)
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                                for (ui32 i = 0; i < fullColList->ChildrenSize(); ++i) {
                                    parent.Atom(i, TString("_yql_join_sublink_") + ToString(it->second) +
                                        "_" + fullColList->Child(i)->Content());
                                }

                                return parent;
                            })
                        .Seal()
                        .Add(2, aggregates)
                    .Seal()
                    .Build();

                newList = JoinColumns(pos, newList, groupedSublink, fullColList, it->second, ctx, leftPrefix);

                if (linkType == "exists") {
                    return ctx.Builder(node->Pos())
                        .Callable("ToPg")
                            .Callable(0, ">")
                                .Callable(0, "Coalesce")
                                    .Callable(0, "Member")
                                        .Add(0, originalArg)
                                        .Atom(1, columnName)
                                    .Seal()
                                    .Callable(1, "Uint64")
                                        .Atom(0, "0")
                                    .Seal()
                                .Seal()
                                .Callable(1, "Uint64")
                                    .Atom(0, "0")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                } else if (linkType == "expr") {
                    return ctx.Builder(node->Pos())
                        .Callable("Ensure")
                            .Callable(0, "Member")
                                .Add(0, originalArg)
                                .Atom(1, columnName + "_value")
                            .Seal()
                            .Callable(1, "<=")
                                .Callable(0, "Coalesce")
                                    .Callable(0, "Member")
                                        .Add(0, originalArg)
                                        .Atom(1, columnName + "_count")
                                    .Seal()
                                    .Callable(1, "Uint64")
                                        .Atom(0, "0")
                                    .Seal()
                                .Seal()
                                .Callable(1, "Uint64")
                                    .Atom(0, "1")
                                .Seal()
                            .Seal()
                            .Callable(2, "String")
                                .Atom(0, "More than one row returned by a subquery used as an expression")
                            .Seal()
                        .Seal()
                        .Build();
                } else if (linkType == "array") {
                    return ctx.Builder(node->Pos())
                        .Callable("Member")
                            .Add(0, originalArg)
                            .Atom(1, columnName + "_value")
                        .Seal()
                        .Build();
                } else if (linkType == "any") {
                    return ctx.Builder(node->Pos())
                        .Callable("ToPg")
                            .Callable(0, "And")
                                .Callable(0, "!=")
                                    .Callable(0, "Coalesce")
                                        .Callable(0, "Member")
                                            .Add(0, originalArg)
                                            .Atom(1, columnName + "_count")
                                        .Seal()
                                        .Callable(1, "Uint64")
                                            .Atom(0, "0")
                                        .Seal()
                                    .Seal()
                                    .Callable(1, "Uint64")
                                        .Atom(0, "0")
                                    .Seal()
                                .Seal()
                                .Callable(1, "Member")
                                    .Add(0, originalArg)
                                    .Atom(1, columnName + "_value")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                } else {
                    YQL_ENSURE(linkType == "all");
                    return ctx.Builder(node->Pos())
                        .Callable("ToPg")
                            .Callable(0, "Or")
                                .Callable(0, "==")
                                    .Callable(0, "Coalesce")
                                        .Callable(0, "Member")
                                            .Add(0, originalArg)
                                            .Atom(1, columnName + "_count")
                                        .Seal()
                                        .Callable(1, "Uint64")
                                            .Atom(0, "0")
                                        .Seal()
                                    .Seal()
                                    .Callable(1, "Uint64")
                                        .Atom(0, "0")
                                    .Seal()
                                .Seal()
                                .Callable(1, "Member")
                                    .Add(0, originalArg)
                                    .Atom(1, columnName + "_value")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                }
            }

            return node;
        }

        return node;
    }, ctx, TOptimizeExprSettings(optCtx.Types));
    YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error);

    return {
        newRoot,
        newList
    };
}

std::pair<TExprNode::TPtr, TExprNode::TPtr> RewriteSubLinks(TPositionHandle pos,
    const TExprNode::TPtr& list, const TExprNode::TPtr& lambda,
    const TNodeMap<ui32>& subLinks, const TVector<TString>& inputAliases,
    const TExprNode::TListType& cleanedInputs,
    TExprContext& ctx, TOptimizeContext& optCtx,
    const TString& leftPrefix = {}, TVector<TString>* sublinkColumns = nullptr) {

    auto arg = ctx.NewArgument(pos, "row");
    auto arguments = ctx.NewArguments(pos, { arg });
    auto root = lambda->TailPtr();

    TExprNode::TPtr newList, newRoot;
    std::tie(newRoot, newList) = RewriteSubLinksPartial(pos, list, root, lambda->Head().HeadPtr(), subLinks, inputAliases,
        cleanedInputs, ctx, optCtx, leftPrefix, sublinkColumns);

    newRoot = ctx.ReplaceNode(std::move(newRoot), lambda->Head().Head(), arg);
    auto newLambda = ctx.NewLambda(pos, std::move(arguments), std::move(newRoot));

    return {
        newLambda,
        newList
    };
}

TExprNode::TPtr BuildFilter(TPositionHandle pos, const TExprNode::TPtr& list, const TExprNode::TPtr& filter,
    const TVector<TString>& inputAliases, const TExprNode::TListType& cleanedInputs, TExprContext& ctx, TOptimizeContext& optCtx) {
    TExprNode::TPtr actualList = list, actualFilter = filter;
    auto subLinks = GatherSubLinks(filter);
    if (!subLinks.empty()) {
        std::tie(actualFilter, actualList) = RewriteSubLinks(filter->Pos(), list, filter,
            subLinks, inputAliases, cleanedInputs, ctx, optCtx);
    }

    return ctx.Builder(pos)
        .Callable("OrderedFilter")
            .Add(0, actualList)
            .Lambda(1)
                .Param("row")
                .Callable("Coalesce")
                    .Callable(0, "FromPg")
                        .Apply(0, actualFilter)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                    .Callable(1, "Bool")
                        .Atom(0, "0")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr NormalizeColumnOrder(const TExprNode::TPtr& node, const TColumnOrder& sourceColumnOrder,
    const TColumnOrder& targetColumnOrder, TExprContext& ctx) {
    if (sourceColumnOrder == targetColumnOrder) {
        return node;
    }

    YQL_ENSURE(sourceColumnOrder.size() == targetColumnOrder.size());
    return ctx.Builder(node->Pos())
        .Callable("OrderedMap")
            .Add(0, node)
            .Lambda(1)
                .Param("row")
                .Callable("AsStruct")
                .Do([&](TExprNodeBuilder &parent) -> TExprNodeBuilder & {
                    for (size_t i = 0; i < sourceColumnOrder.size(); ++i) {
                        parent
                            .List(i)
                                .Atom(0, targetColumnOrder[i])
                                .Callable(1, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, sourceColumnOrder[i])
                                .Seal()
                            .Seal();
                    }
                    return parent;
                })
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPositionalUnionAll(const TExprNode& node, const TVector<TColumnOrder>& columnOrders,
    TExprNode::TListType children, TExprContext& ctx, TOptimizeContext& optCtx) {
    auto targetColumnOrder = optCtx.Types->LookupColumnOrder(node);
    YQL_ENSURE(targetColumnOrder);

    for (ui32 childIndex = 0; childIndex < children.size(); ++childIndex) {
        const auto& childColumnOrder = columnOrders[childIndex];
        auto& child = children[childIndex];
        child = NormalizeColumnOrder(child, childColumnOrder, *targetColumnOrder, ctx);
    }

    auto res = ctx.NewCallable(node.Pos(), "UnionAll", std::move(children));
    return KeepColumnOrder(res, node, ctx, *optCtx.Types);
}

TExprNode::TPtr BuildValues(
    TPositionHandle pos,
    const TExprNode::TPtr& values,
    TExprContext& ctx
) {
    return ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, values->ChildPtr(2))
            .Lambda(1)
                .Param("row")
                .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 index = 0; index < values->Child(1)->ChildrenSize(); ++index) {
                        TStringBuf alias = values->Child(1)->Child(index)->Content();
                        parent
                            .List(index)
                                .Atom(0, alias)
                                .Callable(1, "Nth")
                                    .Arg(0, "row")
                                    .Atom(1, ToString(index))
                                .Seal()
                            .Seal();
                    }

                    return parent;
                })
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildOneRow(TPositionHandle pos, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("AsList")
            .Callable(0, "AsStruct")
            .Seal()
        .Seal()
        .Build();
}

using TUsedColumns = TMap<TString, std::pair<ui32, TString>>;

void AddColumnsFromType(const TTypeAnnotationNode* type, TUsedColumns& columns) {
    auto structType = type->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
    for (auto item : structType->GetItems()) {
        columns.insert(std::make_pair(TString(item->GetName()), std::make_pair(Max<ui32>(), TString())));
    }
}

void AddColumnsFromSublinks(const TNodeMap<ui32>& subLinks, TUsedColumns& columns) {
    for (const auto& s : subLinks) {
        auto extColumns = NTypeAnnImpl::ExtractExternalColumns(s.first->Tail());
        for (const auto& c : extColumns) {
            columns.insert(std::make_pair(c.first, std::make_pair(Max<ui32>(), TString())));
        }

        if (!s.first->Child(2)->IsCallable("Void")) {
            AddColumnsFromType(s.first->Child(2)->GetTypeAnn(), columns);
        }
    }
}

struct TWindowsCtx {
    TVector<std::pair<TExprNode::TPtr, TExprNode::TPtr>> Funcs;
    TMap<ui32, TVector<ui32>> Window2funcs;
    TNodeMap<ui32> FuncsId;
};

void GatherUsedWindows(const TExprNode::TPtr& window, const TExprNode::TPtr& projectionLambda, TWindowsCtx& winCtx) {
    VisitExpr(projectionLambda, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgWindowCall") || node->IsCallable("PgAggWindowCall")) {
            YQL_ENSURE(window);
            ui32 windowIndex;
            if (node->Child(1)->IsCallable("PgAnonWindow")) {
                windowIndex = FromString<ui32>(node->Child(1)->Head().Content());
                YQL_ENSURE(windowIndex < window->Tail().ChildrenSize());
            } else {
                auto name = node->Child(1)->Content();
                bool found = false;
                for (ui32 index = 0; index < window->Tail().ChildrenSize(); ++index) {
                    if (window->Tail().Child(index)->Head().Content() == name) {
                        windowIndex = index;
                        found = true;
                        break;
                    }
                }

                YQL_ENSURE(found);
            }

            winCtx.Window2funcs[windowIndex].push_back(winCtx.Funcs.size());
            winCtx.FuncsId[node.Get()] = winCtx.Funcs.size();
            winCtx.Funcs.push_back({ node, projectionLambda->Head().HeadPtr() });
        }

        return true;
    });
}

TUsedColumns GatherUsedColumns(const TExprNode::TPtr& result, const TExprNode::TPtr& joinOps,
    const TExprNode::TPtr& filter, const TExprNode::TPtr& groupExprs, const TExprNode::TPtr& having, const TExprNode::TPtr& extraSortColumns,
    const TExprNode::TPtr& window, const TWindowsCtx& winCtx, TVector<std::pair<TString, TString>>& joinUsingColumns) {
    TUsedColumns usedColumns;
    for (const auto& x : result->Tail().Children()) {
        AddColumnsFromType(x->Child(1)->GetTypeAnn(), usedColumns);
        auto subLinks = GatherSubLinks(x->TailPtr());
        AddColumnsFromSublinks(subLinks, usedColumns);
    }

    for (ui32 groupNo = 0; groupNo < joinOps->Tail().ChildrenSize(); ++groupNo) {
        auto groupTuple = joinOps->Tail().Child(groupNo);
        for (ui32 i = 0; i < groupTuple->ChildrenSize(); ++i) {
            auto join = groupTuple->Child(i);
            auto joinType = join->Child(0)->Content();
            if (join->ChildrenSize() > 2) {
                Y_ENSURE(join->ChildrenSize() > 3, "Excepted at least 4 args there");
                Y_ENSURE(join->Child(1)->IsAtom(), "Supported only USING clause there");
                Y_ENSURE(join->Child(1)->Content() == "using", "Supported only USING clause there");
                for (ui32 col = 0; col < join->Child(3)->ChildrenSize(); ++col) {
                    auto lr = join->Child(3)->Child(col);
                    usedColumns.erase(TString(join->Child(2)->Child(col)->Content()));
                    if (lr->Child(0)->IsAtom()) {
                        usedColumns.insert(std::make_pair(TString(lr->Child(0)->Content()), std::make_pair(Max<ui32>(), TString())));
                    }
                    usedColumns.insert(std::make_pair(TString(lr->Child(1)->Content()), std::make_pair(Max<ui32>(), TString())));
                    joinUsingColumns.emplace_back(ToString(groupNo), join->Child(2)->Child(col)->Content());
                }
                continue;
            }
            if (joinType != "cross") {
                AddColumnsFromType(join->Tail().Child(0)->GetTypeAnn(), usedColumns);
            }
        }
    }

    if (filter) {
        AddColumnsFromType(filter->Tail().Head().GetTypeAnn(), usedColumns);
        auto subLinks = GatherSubLinks(filter->Tail().TailPtr());
        AddColumnsFromSublinks(subLinks, usedColumns);
    }

    if (groupExprs) {
        for (const auto& x : groupExprs->Tail().Children()) {
            AddColumnsFromType(x->Child(0)->GetTypeAnn(), usedColumns);
        }
    }

    if (having) {
        AddColumnsFromType(having->Tail().Head().GetTypeAnn(), usedColumns);
        auto subLinks = GatherSubLinks(having->Tail().TailPtr());
        AddColumnsFromSublinks(subLinks, usedColumns);
    }

    if (extraSortColumns) {
        for (ui32 inputIndex = 0; inputIndex < extraSortColumns->Tail().ChildrenSize(); ++inputIndex) {
            for (const auto& x : extraSortColumns->Tail().Child(inputIndex)->Children()) {
                auto from = x->Content();
                from.SkipPrefix("_yql_extra_");
                usedColumns.insert(std::make_pair(TString(from), std::make_pair(inputIndex, TString())));
            }
        }
    }

    if (window) {
        for (const auto& x : winCtx.Window2funcs) {
            auto winDef = window->Tail().Child(x.first);
            for (auto group : winDef->Child(2)->Children()) {
                AddColumnsFromType(group->Head().GetTypeAnn(), usedColumns);
            }

            for (auto sort : winDef->Child(3)->Children()) {
                AddColumnsFromType(sort->Head().GetTypeAnn(), usedColumns);
            }
        }
    }

    return usedColumns;
}

void FillInputIndices(const TExprNode::TPtr& from, const TExprNode::TPtr& finalExtTypes,
    TUsedColumns& usedColumns, TOptimizeContext& optCtx) {
    for (auto& x : usedColumns) {
        TStringBuf alias;
        TStringBuf column = NTypeAnnImpl::RemoveAlias(x.first, alias);

        bool foundColumn = false;
        ui32 inputIndex = 0;
        for (; inputIndex < from->Tail().ChildrenSize(); ++inputIndex) {
            auto inputAlias = from->Tail().Child(inputIndex)->Child(1)->Content();
            const auto& read = from->Tail().Child(inputIndex)->Head();
            const auto& columns = from->Tail().Child(inputIndex)->Tail();
            if (x.second.first != Max<ui32>() && x.second.first != inputIndex) {
                continue;
            }

            if (read.IsCallable("PgResolvedCall")) {
                if (inputAlias.empty()) {
                    inputAlias = read.Head().Content();
                }

                auto itemType = GetSeqItemType(read.GetTypeAnn());
                if (!itemType) {
                    itemType = read.GetTypeAnn();
                }

                if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
                    if (columns.ChildrenSize() == 0) {
                        auto structType = itemType->Cast<TStructExprType>();
                        for (const auto& item : structType->GetItems()) {
                            if (NTypeAnnImpl::MakeAliasedColumn(inputAlias, item->GetName()) == x.first) {
                                foundColumn = true;
                                break;
                            }
                        }
                    } else {
                        for (const auto& child : columns.Children()) {
                            if (NTypeAnnImpl::MakeAliasedColumn(inputAlias, child->Content()) == x.first) {
                                foundColumn = true;
                                break;
                            }
                        }
                    }
                } else {
                    Y_ENSURE(columns.ChildrenSize() == 0 || columns.ChildrenSize() == 1);
                    auto memberName = NTypeAnnImpl::MakeAliasedColumn(inputAlias,
                        (columns.ChildrenSize() == 1) ? columns.Head().Content() : inputAlias);
                    foundColumn = (memberName == x.first);
                }
            } else {
                if (alias && alias != inputAlias) {
                    continue;
                }

                if (columns.ChildrenSize() > 0) {
                    auto readOrder = optCtx.Types->LookupColumnOrder(read);
                    YQL_ENSURE(readOrder);
                    for (ui32 i = 0; i < columns.ChildrenSize(); ++i) {
                        if (columns.Child(i)->Content() == column) {
                            foundColumn = true;
                            x.second.second = (*readOrder)[i];
                            break;
                        }
                    }
                } else {
                    auto type = read.GetTypeAnn()->
                        Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                    auto pos = type->FindItem(column);
                    foundColumn = pos.Defined();
                }
            }

            if (x.second.first != Max<ui32>()) {
                foundColumn = true;
                break;
            }

            if (foundColumn) {
                x.second.first = inputIndex;
                break;
            }
        }

        if (!foundColumn && finalExtTypes) {
            for (const auto& input : finalExtTypes->Tail().Children()) {
                const auto& inputAlias = input->Head().Content();
                if (alias && alias != inputAlias) {
                    continue;
                }

                auto type = input->Tail().GetTypeAnn()->
                    Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                auto pos = type->FindItem(column);
                foundColumn = pos.Defined();
                if (foundColumn) {
                    x.second.first = inputIndex;
                    break;
                }

                ++inputIndex;
            }
        }

        YQL_ENSURE(foundColumn, "Missing column: " << x.first);
    }
}

TExprNode::TListType BuildCleanedColumns(TPositionHandle pos, const TExprNode::TPtr& from, const TUsedColumns& usedColumns,
    TVector<TString>& inputAliases, THashMap<TString, ui32>& memberToInput, TExprContext& ctx, const TTypeAnnotationContext& typeCtx) {
    TExprNode::TListType cleanedInputs;
    for (ui32 i = 0; i < from->Tail().ChildrenSize(); ++i) {
        auto list = from->Tail().Child(i)->HeadPtr();
        auto originalList = list;
        auto inputAlias = from->Tail().Child(i)->Child(1)->Content();
        inputAliases.push_back(TString(inputAlias));
        if (list->IsCallable("PgResolvedCall")) {
            const auto& columns = from->Tail().Child(i)->Tail();
            if (inputAlias.empty()) {
                inputAliases.back() = inputAlias = list->Head().Content();
            }

            auto itemType = GetSeqItemType(list->GetTypeAnn());
            if (!itemType) {
                itemType = list->GetTypeAnn();
            }

            if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
                if (list->GetTypeAnn()->GetKind() != ETypeAnnotationKind::List) {
                    list = ctx.Builder(pos)
                        .Callable("AsList")
                            .Add(0, list)
                        .Seal()
                        .Build();
                }

                if (columns.ChildrenSize() != 0) {
                    // rename columns
                    list = ctx.Builder(pos)
                        .Callable("OrderedMap")
                            .Add(0, list)
                            .Lambda(1)
                                .Param("item")
                                .Callable("AsStruct")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                                        auto originalColumnOrder = typeCtx.LookupColumnOrder(*originalList);
                                        YQL_ENSURE(originalColumnOrder);
                                        YQL_ENSURE(originalColumnOrder->size() == columns.ChildrenSize());
                                        for (ui32 i = 0; i < columns.ChildrenSize(); ++i) {
                                            parent.List(i)
                                                .Atom(0, columns.Child(i)->Content())
                                                .Callable(1, "Member")
                                                    .Arg(0, "item")
                                                    .Atom(1, (*originalColumnOrder)[i])
                                                .Seal()
                                                .Seal();
                                        }

                                        return parent;
                                    })
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                }
            } else {
                Y_ENSURE(columns.ChildrenSize() == 0 || columns.ChildrenSize() == 1);
                auto memberName = (columns.ChildrenSize() == 1) ? columns.Head().Content() : inputAlias;
                if (list->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
                    list = ctx.Builder(pos)
                        .Callable("OrderedMap")
                            .Add(0, list)
                            .Lambda(1)
                                .Param("item")
                                .Callable("AsStruct")
                                    .List(0)
                                        .Atom(0, memberName)
                                        .Arg(1, "item")
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                } else {
                    list = ctx.Builder(pos)
                        .Callable("AsList")
                            .Callable(0, "AsStruct")
                                .List(0)
                                    .Atom(0, memberName)
                                    .Add(1, list)
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                }
            }
        }

        auto cleaned = ctx.Builder(pos)
            .Callable("OrderedMap")
                .Add(0, list)
                .Lambda(1)
                    .Param("row")
                    .Callable("AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 index = 0;
                            for (const auto& x : usedColumns) {
                                if (x.second.first != i) {
                                    continue;
                                }

                                memberToInput[x.first] = i;
                                auto listBuilder = parent.List(index++);
                                listBuilder.Atom(0, x.first);
                                listBuilder.Callable(1, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, x.second.second ? x.second.second : NTypeAnnImpl::RemoveAlias(x.first))
                                .Seal();
                                listBuilder.Seal();
                            }

                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        cleanedInputs.push_back(cleaned);
    }

    return cleanedInputs;
}

TExprNode::TPtr BuildMinus(TPositionHandle pos, const TExprNode::TPtr& left, const TExprNode::TPtr& right, const TExprNode::TPtr& predicate, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("OrderedFilter")
            .Add(0, left)
            .Lambda(1)
                .Param("x")
                .Callable(0, "Not")
                    .Callable(0, "HasItems")
                        .Callable(0, "Filter")
                            .Add(0, right)
                            .Lambda(1)
                                .Param("y")
                                .Callable("Coalesce")
                                    .Callable(0, "FromPg")
                                        .Apply(0, predicate)
                                            .With(0)
                                                .Callable("FlattenMembers")
                                                    .List(0)
                                                        .Atom(0, "")
                                                        .Arg(1,"x")
                                                    .Seal()
                                                    .List(1)
                                                        .Atom(0, "")
                                                        .Arg(1, "y")
                                                    .Seal()
                                                .Seal()
                                            .Done()
                                        .Seal()
                                    .Seal()
                                    .Callable(1, "Bool")
                                        .Atom(0, "0")
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildScalarMinus(TPositionHandle pos, const TExprNode::TPtr& left, const TExprNode::TPtr& right,
    const TExprNode::TPtr& coalescedPredicate, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("If")
            .Callable(0, "Or")
                .Callable(0, "Not")
                    .Add(0, coalescedPredicate)
                .Seal()
                .Callable(1, "==")
                    .Callable(0, "Length")
                        .Add(0, right)
                    .Seal()
                    .Callable(1, "Uint64")
                        .Atom(0, "0")
                    .Seal()
                .Seal()
            .Seal()
            .Add(1, left)
            .Callable(2, "EmptyList")
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildConstPredicateJoin(TPositionHandle pos, TStringBuf joinType, const TExprNode::TPtr& predicate,
    const TExprNode::TPtr& cartesian, const TExprNode::TPtr& left, const TExprNode::TPtr& right, TExprContext& ctx) {
    auto coalescedPredicate = AsFilterPredicate(pos, predicate->TailPtr(), ctx);

    auto main = ctx.Builder(pos)
        .Callable("If")
            .Add(0, coalescedPredicate)
            .Add(1, cartesian)
            .Callable(2, "EmptyList")
            .Seal()
        .Seal()
        .Build();

    if (joinType == "inner") {
        return main;
    } else if (joinType == "left") {
        return ctx.Builder(pos)
            .Callable("UnionAll")
                .Add(0, main)
                .Add(1, BuildScalarMinus(pos, left, right, coalescedPredicate, ctx))
            .Seal()
            .Build();
    } else if (joinType == "right") {
        return ctx.Builder(pos)
            .Callable("UnionAll")
                .Add(0, main)
                .Add(1, BuildScalarMinus(pos, right, left, coalescedPredicate, ctx))
            .Seal()
            .Build();
    } else {
        YQL_ENSURE(joinType == "full");
        return ctx.Builder(pos)
            .Callable("UnionAll")
            .Add(0, main)
            .Add(1, BuildScalarMinus(pos, left, right, coalescedPredicate, ctx))
            .Add(2, BuildScalarMinus(pos, right, left , coalescedPredicate, ctx))
            .Seal()
            .Build();
    }
}

TExprNode::TPtr BuildSingleInputPredicateJoin(TPositionHandle pos, TStringBuf joinType, const TExprNode::TPtr& predicate,
    const TExprNode::TPtr& left, const TExprNode::TPtr& right, TExprContext& ctx) {
    auto filteredLeft = ctx.Builder(pos)
        .Callable("OrderedFilter")
            .Add(0, left)
            .Lambda(1)
                .Param("row")
                .Callable("Coalesce")
                    .Callable(0, "FromPg")
                        .Apply(0, predicate)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                    .Callable(1, "Bool")
                        .Atom(0, "0")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto main = JoinColumns(pos, filteredLeft, right, nullptr, {}, ctx);

    auto extraLeft = [&]() {
        return ctx.Builder(pos)
            .Callable("OrderedFilter")
                .Add(0, left)
                .Lambda(1)
                    .Param("row")
                    .Callable("Not")
                        .Callable(0, "Coalesce")
                            .Callable(0, "FromPg")
                                .Apply(0, predicate)
                                    .With(0, "row")
                                .Seal()
                            .Seal()
                            .Callable(1, "Bool")
                                .Atom(0, "0")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    };

    auto extraRight = [&]() {
        return ctx.Builder(pos)
            .Callable("If")
                .Callable(0, "Not")
                    .Callable(0, "HasItems")
                        .Add(0, main)
                    .Seal()
                .Seal()
                .Add(1, right)
                .Callable(2, "EmptyList")
                .Seal()
            .Seal()
            .Build();
    };

    if (joinType == "inner") {
        return main;
    } else if (joinType == "left") {
        return ctx.Builder(pos)
            .Callable("UnionAll")
                .Add(0, main)
                .Add(1, extraLeft())
            .Seal()
            .Build();
    } else if (joinType == "right") {
        return ctx.Builder(pos)
            .Callable("UnionAll")
                .Add(0, main)
                .Add(1, extraRight())
            .Seal()
            .Build();
    } else {
        YQL_ENSURE(joinType == "full");
        return ctx.Builder(pos)
            .Callable("UnionAll")
                .Add(0, main)
                .Add(1, extraLeft())
                .Add(2, extraRight())
            .Seal()
            .Build();
    }
}

bool GatherJoinInputs(const TExprNode& root, const TExprNode& row, ui32 rightInputIndex, const THashMap<TString, ui32>& memberToInput,
    bool& hasLeftInput, bool& hasRightInput) {
    hasLeftInput = false;
    hasRightInput = false;
    TParentsMap parents;
    GatherParents(root, parents);
    auto parentsOverRow = parents.find(&row);
    if (parentsOverRow == parents.end()) {
        return true;
    }

    for (const auto& p : parentsOverRow->second) {
        if (!p->IsCallable("Member")) {
            return false;
        }

        auto name = p->Child(1)->Content();
        auto inputPtr = memberToInput.FindPtr(name);
        YQL_ENSURE(inputPtr);
        if (*inputPtr == rightInputIndex) {
            hasRightInput = true;
        } else {
            hasLeftInput = true;
        }
    }

    return true;
}

TExprNode::TPtr BuildEquiJoin(TPositionHandle pos, TStringBuf joinType, const TExprNode::TPtr& left, const TExprNode::TPtr& right,
    const TExprNode::TListType& leftColumns, const TExprNode::TListType& rightColumns, TExprContext& ctx) {
    auto join = ctx.Builder(pos)
        .Callable("EquiJoin")
            .List(0)
                .Add(0, left)
                .Atom(1, "a")
            .Seal()
            .List(1)
                .Add(0, right)
                .Atom(1, "b")
            .Seal()
            .List(2)
                .Atom(0, to_title(TString(joinType)))
                .Atom(1, "a")
                .Atom(2, "b")
                .List(3)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                        for (ui32 i = 0; i < leftColumns.size(); ++i) {
                            parent.Atom(2 * i, "a");
                            parent.Add(2* i + 1, leftColumns[i]);
                        }

                        return parent;
                    })
                .Seal()
                .List(4)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                        for (ui32 i = 0; i < rightColumns.size(); ++i) {
                            parent.Atom(2 * i, "b");
                            parent.Add(2 * i + 1, rightColumns[i]);
                        }

                        return parent;
                    })
                .Seal()
                .List(5)
                .Seal()
            .Seal()
            .List(3)
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, join)
            .Lambda(1)
                .Param("row")
                .Callable("DivePrefixMembers")
                    .Arg(0, "row")
                    .List(1)
                        .Atom(0, "a.")
                        .Atom(1, "b.")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

std::tuple<TVector<ui32>, TExprNode::TListType> BuildJoinGroups(TPositionHandle pos, const TExprNode::TListType& cleanedInputs,
    const TExprNode::TPtr& joinOps, const THashMap<TString, ui32>& memberToInput, TExprContext& ctx, TOptimizeContext& optCtx) {
    TVector<ui32> groupForIndex;
    TExprNode::TListType joinGroups;

    ui32 inputIndex = 0;
    for (ui32 groupNo = 0; groupNo < joinOps->Tail().ChildrenSize(); ++groupNo) {
        groupForIndex.push_back(groupNo);
        auto groupTuple = joinOps->Tail().Child(groupNo);
        if (groupTuple->ChildrenSize() == 0) {
            joinGroups.push_back(cleanedInputs[inputIndex++]);
            continue;
        }

        auto current = cleanedInputs[inputIndex++];
        for (ui32 i = 0; i < groupTuple->ChildrenSize(); ++i) {
            groupForIndex.push_back(groupNo);
            auto with = cleanedInputs[inputIndex++];
            // current = join current & with
            auto join = groupTuple->Child(i);
            auto joinType = join->Child(0)->Content();
            auto cartesian = JoinColumns(pos, current, with, nullptr, {}, ctx);
            if (joinType == "cross") {
                current = cartesian;
                continue;
            }
            if (join->ChildrenSize() > 2) {
                Y_ENSURE(join->Child(1)->IsAtom(), "expected only USING clause when join_ops children size > 2");
                Y_ENSURE(join->Child(1)->Content() == "using", "expected only USING clause when join_ops children size > 2");
                Y_ENSURE(join->ChildrenSize() > 3 && join->Child(3)->IsList(), "Excepted list of aliased columns in USING join");
                auto left = current;
                auto right = with;
                TExprNode::TListType leftColumns;
                TExprNode::TListType rightColumns;
                TExprNode::TListType toRemove;
                for (auto& col: join->Child(3)->ChildrenList()) {
                    if (col->Child(0)->IsAtom()) {
                        leftColumns.push_back(col->Child(0));
                    } else {
                        toRemove.push_back(col->Child(0)->Child(0));
                        leftColumns.push_back(col->Child(0)->Child(0));
                    }
                    rightColumns.push_back(col->Child(1));
                }
                current = BuildEquiJoin(pos, joinType, left, right, leftColumns, rightColumns, ctx);
                auto secondStruct = ctx.Builder(pos)
                .Lambda()
                    .Param("row")
                    .Callable("AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (size_t i = 0; i < leftColumns.size(); ++i) {
                                parent.List(i)
                                    .Add(0, join->Child(2)->Child(i))
                                    .Callable(1, "Coalesce")
                                        .Callable(0, "Member")
                                            .Arg(0, "row")
                                            .Add(1, leftColumns[i])
                                        .Seal()
                                        .Callable(1, "Member")
                                            .Arg(0, "row")
                                            .Add(1, rightColumns[i])
                                        .Seal()
                                    .Seal()
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
                .Build();
                auto removeProjection = ctx.Builder(pos)
                    .Lambda()
                        .Param("row")
                        .Arg(0, "row")
                    .Seal().Build();
                current = ctx.Builder(pos)
                    .Callable("OrderedMap")
                        .Add(0, current)
                        .Lambda(1)
                            .Param("row")
                            .Callable("FlattenMembers")
                                .List(0)
                                    .Atom(0, "")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        if (toRemove.size()) {
                                            parent.Callable(1, "RemoveMembers")
                                                .Arg(0, "row")
                                                .Add(1, ctx.NewList(pos, std::move(toRemove)))
                                            .Seal();
                                        } else {
                                            parent.Arg(1, "row");
                                        }
                                        return parent;
                                    })
                                .Seal()
                                .List(1)
                                    .Atom(0, "")
                                    .Apply(1, secondStruct).With(0, "row").Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                    .Build();
                continue;
            }

            auto predicate = join->Tail().TailPtr();
            if (!IsDepended(predicate->Tail(), predicate->Head().Head())) {
                current = BuildConstPredicateJoin(pos, joinType, predicate, cartesian, current, with, ctx);
                continue;
            }

            // collect inputs from predicate
            bool hasLeftInput;
            bool hasRightInput;
            if (GatherJoinInputs(predicate->Tail(), predicate->Head().Head(), inputIndex - 1, memberToInput, hasLeftInput, hasRightInput)) {
                if (hasLeftInput && !hasRightInput) {
                    current = BuildSingleInputPredicateJoin(pos, joinType, predicate, current, with, ctx);
                    continue;
                } else if (!hasLeftInput && hasRightInput) {
                    auto reverseJoinType = joinType;
                    if (reverseJoinType == "left") {
                        reverseJoinType = "right";
                    } else if (reverseJoinType == "right") {
                        reverseJoinType = "left";
                    }

                    current = BuildSingleInputPredicateJoin(pos, reverseJoinType, predicate, with, current, ctx);
                    continue;
                } else if (hasLeftInput && hasRightInput) {
                    TExprNode::TListType andTerms;
                    bool isPg;
                    GatherAndTerms(predicate->TailPtr(), andTerms, isPg, ctx);
                    bool bad = false;
                    TExprNode::TListType leftColumns;
                    TExprNode::TListType rightColumns;
                    TExprNode::TListType constPredicates;
                    TExprNode::TListType leftInputPredicates;
                    TExprNode::TListType rightInputPredicates;
                    for (auto& andTerm : andTerms) {
                        if (!IsDepended(*andTerm, predicate->Head().Head())) {
                            constPredicates.push_back(andTerm);
                            continue;
                        }

                        YQL_ENSURE(GatherJoinInputs(*andTerm, predicate->Head().Head(), inputIndex - 1, memberToInput, hasLeftInput, hasRightInput));
                        if (!hasLeftInput && !hasRightInput) {
                            bad = true;
                            break;
                        }

                        if (hasLeftInput && !hasRightInput) {
                            leftInputPredicates.push_back(andTerm);
                            continue;
                        }

                        if (!hasLeftInput && hasRightInput) {
                            rightInputPredicates.push_back(andTerm);
                            continue;
                        }

                        TExprNode::TPtr left, right;
                        if (!IsEquality(andTerm, left, right)) {
                            bad = true;
                            break;
                        }

                        bool leftOnLeft;
                        if (left->IsCallable("Member") && &left->Head() == &predicate->Head().Head()) {
                            auto inputPtr = memberToInput.FindPtr(left->Child(1)->Content());
                            YQL_ENSURE(inputPtr);
                            leftOnLeft = (*inputPtr < inputIndex - 1);
                            (leftOnLeft ? leftColumns : rightColumns).push_back(left->ChildPtr(1));
                        } else {
                            bad = true;
                            break;
                        }

                        bool rightOnRight;
                        if (right->IsCallable("Member") && &right->Head() == &predicate->Head().Head()) {
                            auto inputPtr = memberToInput.FindPtr(right->Child(1)->Content());
                            YQL_ENSURE(inputPtr);
                            rightOnRight = (*inputPtr == inputIndex - 1);
                            (rightOnRight ? rightColumns : leftColumns).push_back(right->ChildPtr(1));
                        } else {
                            bad = true;
                            break;
                        }

                        if (leftOnLeft != rightOnRight) {
                            bad = true;
                            break;
                        }
                    }

                    if (!bad) {
                        TExprNode::TPtr constPredicate;
                        if (!constPredicates.empty()) {
                            constPredicate = ctx.NewCallable(pos, "And", std::move(constPredicates));
                        }

                        TExprNode::TPtr leftInputPredicate;
                        if (!leftInputPredicates.empty()) {
                            leftInputPredicate = ctx.NewCallable(pos, "And", std::move(leftInputPredicates));
                        }

                        TExprNode::TPtr rightInputPredicate;
                        if (!rightInputPredicates.empty()) {
                            rightInputPredicate = ctx.NewCallable(pos, "And", std::move(rightInputPredicates));
                        }

                        auto left = current;
                        auto right = with;
                        TExprNode::TPtr notLeft;
                        TExprNode::TPtr notRight;
                        if (leftInputPredicate) {
                            std::tie(left, notLeft) = SplitByPredicate(pos, left, leftInputPredicate, predicate->HeadPtr(), ctx);
                        }

                        if (rightInputPredicate) {
                            std::tie(right, notRight) = SplitByPredicate(pos, right, rightInputPredicate, predicate->HeadPtr(), ctx);
                        }

                        auto joined = BuildEquiJoin(pos, joinType, left, right, leftColumns, rightColumns, ctx);
                        if (notLeft && (joinType == "left" || joinType == "full")) {
                            joined = ctx.NewCallable(pos, "UnionAll", { joined, notLeft });
                        }

                        if (notRight && (joinType == "right" || joinType == "full")) {
                            joined = ctx.NewCallable(pos, "UnionAll", { joined, notRight });
                        }

                        if (constPredicate) {
                            TExprNode::TPtr elseValue;
                            if (joinType == "inner") {
                                elseValue = ctx.NewCallable(pos, "EmptyList", {});
                            } else if (joinType == "left") {
                                elseValue = current;
                            } else if (joinType == "right") {
                                elseValue = with;
                            } else {
                                elseValue = ctx.NewCallable(pos, "UnionAll", { current, with });
                            }

                            joined = ctx.Builder(pos)
                                .Callable("If")
                                    .Add(0, AsFilterPredicate(pos, constPredicate, ctx))
                                    .Add(1, joined)
                                    .Add(2, elseValue)
                                .Seal()
                                .Build();
                        }

                        current = joined;
                        continue;
                    }
                }
            }

            auto filteredCartesian = BuildFilter(pos, cartesian, predicate, {}, {}, ctx, optCtx);
            if (joinType == "inner") {
                current = filteredCartesian;
            } else if (joinType == "left") {
                current = ctx.Builder(pos)
                    .Callable("UnionAll")
                        .Add(0, filteredCartesian)
                        .Add(1, BuildMinus(pos, current, with, predicate, ctx))
                    .Seal()
                    .Build();
            } else if (joinType == "right") {
                current = ctx.Builder(pos)
                    .Callable("UnionAll")
                        .Add(0, filteredCartesian)
                        .Add(1, BuildMinus(pos, with, current, predicate, ctx))
                    .Seal()
                    .Build();
            } else {
                YQL_ENSURE(joinType == "full");
                current = ctx.Builder(pos)
                    .Callable("UnionAll")
                        .Add(0, filteredCartesian)
                        .Add(1, BuildMinus(pos, current, with, predicate, ctx))
                        .Add(2, BuildMinus(pos, with, current, predicate, ctx))
                    .Seal()
                    .Build();
            }
        }

        joinGroups.push_back(current);
    }

    return { groupForIndex, joinGroups };
}

TExprNode::TPtr BuildCrossJoinsBetweenGroups(TPositionHandle pos, const TExprNode::TListType& joinGroups,
    const TUsedColumns& usedColumns, const TVector<ui32>& groupForIndex, TExprContext& ctx, const TVector<std::pair<TString, TString>>& joinUsingColumns) {
    TExprNode::TListType args;
    for (ui32 i = 0; i < joinGroups.size(); ++i) {
        args.push_back(ctx.Builder(pos)
            .List()
                .Add(0, joinGroups[i])
                .Atom(1, ToString(i))
            .Seal()
            .Build());
    }

    auto tree = MakeCrossJoin(pos, ctx.NewAtom(pos, "0"), ctx.NewAtom(pos, "1"), ctx);
    for (ui32 i = 2; i < joinGroups.size(); ++i) {
        tree = MakeCrossJoin(pos, tree, ctx.NewAtom(pos, ToString(i)), ctx);
    }

    args.push_back(tree);
    TExprNode::TListType settings;
    for (const auto& x : usedColumns) {
        if (x.second.first >= groupForIndex.size()) {
            // skip external columns here
            continue;
        }

        settings.push_back(ctx.Builder(pos)
            .List()
                .Atom(0, "rename")
                .Atom(1, ToString(groupForIndex[x.second.first]) + "." + x.first)
                .Atom(2, x.first)
            .Seal()
            .Build());
    }

    for (const auto& x: joinUsingColumns) {
        settings.push_back(ctx.Builder(pos)
            .List()
                .Atom(0, "rename")
                .Atom(1, x.first + "." + x.second)
                .Atom(2, x.second)
            .Seal()
            .Build());
    }

    auto settingsNode = ctx.NewList(pos, std::move(settings));
    args.push_back(settingsNode);
    return ctx.NewCallable(pos, "EquiJoin", std::move(args));
}

TExprNode::TPtr BuildProjectionLambda(TPositionHandle pos, const TExprNode::TPtr& result, const TStructExprType* finalType,
    const TColumnOrder& nodeColumnOrder, const TColumnOrder& setItemColumnOrder,
    bool subLink, bool emitPgStar, TExprContext& ctx) {

    YQL_ENSURE(nodeColumnOrder.size() == setItemColumnOrder.size());

    TMap<TStringBuf, TStringBuf> columnNamesMap;
    for (size_t i = 0; i < nodeColumnOrder.size(); ++i) {
        columnNamesMap[setItemColumnOrder[i]] = nodeColumnOrder[i];
    }

    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Callable("AsStruct")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                auto addResultItem = [] (TExprNodeBuilder& builder, ui32 idx, TExprNode* resultItem) {
                    builder.Apply(idx, resultItem->TailPtr())
                        .With(0, "row")
                    .Seal();
                };

                auto addStructMember = [] (TExprNodeBuilder& builder, ui32 idx, const TStringBuf& memberName) {
                    builder.Callable(idx, "Member")
                        .Arg(0, "row")
                        .Atom(1, memberName)
                    .Seal();
                };

                auto addPgCast = [&finalType, &columnNamesMap, &pos, &ctx] (TExprNodeBuilder& builder, ui32 idx, const TStringBuf& columnName,
                        const TTypeAnnotationNode* actualTypeNode, const std::function<void(TExprNodeBuilder&, ui32)>& buildCore) {

                    const auto expectedTypeNode = finalType->FindItemType(columnNamesMap[columnName]);
                    Y_ENSURE(expectedTypeNode);
                    const auto expectedType = expectedTypeNode->Cast<TPgExprType>();

                    ui32 actualPgTypeId;
                    bool convertToPg;
                    Y_ENSURE(ExtractPgType(actualTypeNode, actualPgTypeId, convertToPg, pos, ctx));

                    auto needPgCast = (expectedType->GetId() != actualPgTypeId);

                    if (convertToPg) {
                        Y_ENSURE(!needPgCast, TStringBuilder()
                             << "Conversion to PG type is different at typization (" << expectedType->GetId()
                             << ") and optimization (" << actualPgTypeId << ") stages.");

                        auto wrapBuilder = builder.Callable(idx, "ToPg");
                        buildCore(wrapBuilder, 0);
                        wrapBuilder.Seal();
                    } else if (needPgCast) {
                        auto wrapBuilder = builder.Callable(idx, "PgCast");
                        buildCore(wrapBuilder, 0);
                        wrapBuilder.Callable(1, "PgType")
                            .Atom(0, NPg::LookupType(expectedType->GetId()).Name)
                            .Seal();
                        wrapBuilder.Seal();
                    } else {
                        buildCore(builder, idx);
                    }
                    builder.Seal();
                };

                ui32 index = 0;
                THashMap<TString, TExprNode*> overrideColumns;
                if (emitPgStar) {
                    for (const auto& x : result->Tail().Children()) {
                        if (x->HeadPtr()->IsAtom()) {
                            overrideColumns.emplace(TString(x->HeadPtr()->Content()), x.Get());
                        }
                    }
                }

                for (const auto& x : result->Tail().Children()) {
                    if (x->HeadPtr()->IsAtom()) {
                        if (emitPgStar) {
                            continue;
                        }
                        const auto& columnName = x->Head().Content();

                        auto listBuilder = parent.List(index++);
                        listBuilder.Add(0, x->HeadPtr());
                        addPgCast(listBuilder, 1, columnName, x->GetTypeAnn(),
                             [&addResultItem, &x] (TExprNodeBuilder& builder, ui32 idx) { addResultItem(builder, idx, x.Get()); });
                    } else {
                        auto type = x->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                        Y_ENSURE(type);

                        for (const auto& item : type->GetItems()) {
                            TStringBuf column = item->GetName();
                            auto columnName = subLink ? column : NTypeAnnImpl::RemoveAlias(column);

                            auto listBuilder = parent.List(index++);
                            if (auto* columnNode = overrideColumns.FindPtr(columnName)) {
                                // we never get here while processing SELECTs,
                                // so no need to add PgCasts due to query combining with UNION ALL et al
                                listBuilder.Add(0, (*columnNode)->HeadPtr());
                                addResultItem(listBuilder, 1, *columnNode);
                            } else {
                                listBuilder.Atom(0, columnName);
                                addPgCast(listBuilder, 1, columnName, item->GetItemType(),
                                    [&addStructMember, &column] (TExprNodeBuilder& builder, ui32 idx) { addStructMember(builder, idx, column); });
                            }
                        }
                    }
                }

                return parent;
            })
            .Seal()
        .Seal()
        .Build();
}

void GatherAggregationsFromLambda(const TExprNode::TPtr& lambda, TAggs& aggs, TAggregationMap& aggId, bool testExpr) {
    VisitExpr(lambda->TailPtr(), [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgSubLink")) {
            if (!node->Child(3)->IsCallable("Void")) {
                YQL_ENSURE(node->Child(3)->IsLambda());
                GatherAggregationsFromLambda(node->ChildPtr(3), aggs, aggId, true);
            }

            return false;
        }

        if (node->IsCallable("PgAgg")) {
            aggId[node.Get()] = { aggs.size(), testExpr };
            aggs.push_back({ node, lambda->Head().HeadPtr() });
        }

        return true;
    });
}

TExprNode::TPtr BuildAggregationTraits(TPositionHandle pos, bool onWindow, const TString& distinctColumnName,
    const std::pair<TExprNode::TPtr, TExprNode::TPtr>& agg,
    const TExprNode::TPtr& listTypeNode, const TAggregationMap* aggId, TExprContext& ctx, TOptimizeContext& optCtx) {
    auto func = agg.first->Head().Content();
    TExprNode::TPtr type = ctx.Builder(pos)
        .Callable("ListItemType")
            .Add(0, listTypeNode)
        .Seal()
        .Build();

    TExprNode::TPtr extractor;
    if (distinctColumnName) {
        type = ctx.Builder(pos)
            .Callable("StructMemberType")
                .Add(0, type)
                .Atom(1, distinctColumnName)
            .Seal()
            .Build();

        extractor = ctx.Builder(pos)
            .Lambda()
                .Param("value")
                .Arg("value")
            .Seal()
            .Build();
    } else {
        auto arg = ctx.NewArgument(pos, "row");
        auto arguments = ctx.NewArguments(pos, { arg });
        TExprNode::TListType aggFuncArgs;
        for (ui32 j = onWindow ? 3 : 2; j < agg.first->ChildrenSize(); ++j) {
            auto root = agg.first->ChildPtr(j);
            if (aggId && onWindow) {
                RewriteAggsPartial(root, arg, *aggId, ctx, optCtx, false);
            }

            aggFuncArgs.push_back(ctx.ReplaceNode(std::move(root), *agg.second, arg));
        }

        extractor = ctx.NewLambda(pos, std::move(arguments), std::move(aggFuncArgs));
    }

    const bool blockEngineEnabled = optCtx.Types->BlockEngineMode != EBlockEngineMode::Disable;
    if (optCtx.Types->PgEmitAggApply.GetOrElse(blockEngineEnabled) && !onWindow) {
        return ctx.Builder(pos)
            .Callable("AggApply")
                .Atom(0, TString("pg_") + func)
                .Add(1, type)
                .Add(2, extractor)
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Callable(TString(onWindow ? "PgWindowTraits" : "PgAggregationTraits") + (distinctColumnName ? "Tuple" : ""))
            .Atom(0, func)
            .Add(1, type)
            .Add(2, extractor)
        .Seal()
        .Build();
}

TExprNode::TPtr BuildGroup(TPositionHandle pos, TExprNode::TPtr list,
    const TAggs& aggs, const TExprNode::TPtr& groupExprs, const TExprNode::TPtr& groupSets,
    const TExprNode::TPtr& finalExtTypes, const TExprNode::TPtr& joinedUniqueExt,
    TExprContext& ctx, TOptimizeContext& optCtx) {

    bool needRemapForDistinct = false;
    for (ui32 i = 0; i < aggs.size(); ++i) {
        if (GetSetting(*aggs[i].first->Child(1), "distinct")) {
            needRemapForDistinct = true;
            break;
        }
    }

    if (needRemapForDistinct) {
        auto arg = ctx.NewArgument(pos, "row");
        auto arguments = ctx.NewArguments(pos, { arg });
        TExprNode::TListType newColumns;
        for (ui32 i = 0; i < aggs.size(); ++i) {
            if (!GetSetting(*aggs[i].first->Child(1), "distinct")) {
                continue;
            }

            TExprNode::TListType tupleArgs;
            for (ui32 j = 2; j < aggs[i].first->ChildrenSize(); ++j) {
                tupleArgs.push_back(ctx.ReplaceNode(aggs[i].first->ChildPtr(j), *aggs[i].second, arg));
            }

            auto tuple = ctx.NewList(pos, std::move(tupleArgs));
            newColumns.push_back(ctx.Builder(pos)
                .List()
                    .Atom(0, "_yql_distinct_" + ToString(i))
                    .Add(1, tuple)
                .Seal()
                .Build());
        }

        auto newColumnsNode = ctx.NewCallable(pos, "AsStruct", std::move(newColumns));
        auto root = ctx.Builder(pos)
            .Callable("FlattenMembers")
                .List(0)
                    .Atom(0, "")
                    .Add(1, arg)
                .Seal()
                .List(1)
                    .Atom(0, "")
                    .Add(1, newColumnsNode)
                .Seal()
            .Seal()
            .Build();

        auto distinctLambda = ctx.NewLambda(pos, std::move(arguments), std::move(root));

        list = ctx.Builder(pos)
            .Callable("OrderedMap")
                .Add(0, list)
                .Add(1, distinctLambda)
            .Seal()
            .Build();
    }

    auto listTypeNode = ctx.Builder(pos)
        .Callable("TypeOf")
            .Add(0, list)
        .Seal()
        .Build();

    TExprNode::TListType payloadItems;
    TVector<ui32> nonNullDefAggs;
    for (ui32 i = 0; i < aggs.size(); ++i) {
        if (aggs[i].first->Head().Content() == "count") {
            nonNullDefAggs.push_back(i);
        }

        const bool distinct = GetSetting(*aggs[i].first->Child(1), "distinct") != nullptr;
        auto traits = BuildAggregationTraits(pos, false, distinct ? "_yql_distinct_" + ToString(i) : "", aggs[i], listTypeNode, nullptr, ctx, optCtx);
        if (distinct) {
            payloadItems.push_back(ctx.Builder(pos)
                .List()
                .Atom(0, "_yql_agg_" + ToString(i))
                .Add(1, traits)
                .Atom(2, "_yql_distinct_" + ToString(i))
                .Seal()
                .Build());
        } else {
            payloadItems.push_back(ctx.Builder(pos)
                .List()
                .Atom(0, "_yql_agg_" + ToString(i))
                .Add(1, traits)
                .Seal()
                .Build());
        }
    }

    auto payloadsNode = ctx.NewList(pos, std::move(payloadItems));
    TExprNode::TListType extKeysItems;
    if (finalExtTypes) {
        for (const auto& x : finalExtTypes->Tail().Children()) {
            auto type = x->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
            for (const auto& i : type->GetItems()) {
                extKeysItems.push_back(ctx.NewAtom(pos, NTypeAnnImpl::MakeAliasedColumn(x->Head().Content(), i->GetName())));
            }
        }
    }

    TExprNode::TListType groupKeysItems;
    if (groupExprs->Tail().ChildrenSize()) {
        auto arg = ctx.NewArgument(pos, "row");
        auto arguments = ctx.NewArguments(pos, { arg });

        TExprNode::TListType newColumns;
        for (ui32 i = 0; i < groupExprs->Tail().ChildrenSize(); ++i) {
            const auto& group = groupExprs->Tail().Child(i);
            const auto& lambda = group->Tail();
            TStringBuf columnName;
            if (NTypeAnnImpl::IsPlainMemberOverArg(lambda.Tail(), columnName)) {
                groupKeysItems.push_back(ctx.NewAtom(pos, columnName));
                continue;
            }

            auto name = "_yql_agg_key_" + ToString(i);
            groupKeysItems.push_back(ctx.NewAtom(pos, name));
            newColumns.push_back(ctx.Builder(pos)
                .List()
                    .Atom(0, name)
                    .Apply(1, lambda)
                        .With(0, arg)
                    .Seal()
                .Seal()
                .Build());
        }

        auto newColumnsNode = ctx.NewCallable(pos, "AsStruct", std::move(newColumns));
        auto root = ctx.Builder(pos)
            .Callable("FlattenMembers")
                .List(0)
                    .Atom(0, "")
                    .Add(1, arg)
                .Seal()
                .List(1)
                    .Atom(0, "")
                    .Add(1, newColumnsNode)
                .Seal()
            .Seal()
            .Build();

        auto keyExprsLambda = ctx.NewLambda(pos, std::move(arguments), std::move(root));

        list = ctx.Builder(pos)
            .Callable("OrderedMap")
                .Add(0, list)
                .Add(1, keyExprsLambda)
            .Seal()
            .Build();
    }

    TVector<ui32> currentSetIndices, setCounts;
    currentSetIndices.resize(groupSets->Tail().ChildrenSize());
    for (ui32 i = 0; i < groupSets->Tail().ChildrenSize(); ++i) {
        auto set = groupSets->Tail().Child(i);
        YQL_ENSURE(set->ChildrenSize() >= 1);
        setCounts.push_back(set->ChildrenSize());
    }

    TExprNode::TListType unionAllItems;
    for (;;) {
        TExprNode::TListType keysItems = extKeysItems;
        // calculate grouping set keys for current position
        TSet<ui32> currentKeys;
        for (ui32 i = 0; i < currentSetIndices.size(); ++i) {
            const auto& set = groupSets->Tail().Child(i)->Child(currentSetIndices[i]);
            YQL_ENSURE(set->IsList());
            for (const auto& atom : set->Children()) {
                YQL_ENSURE(atom->IsAtom());
                currentKeys.insert(FromString<ui32>(atom->Content()));
            }
        }

        for (auto keyIndex : currentKeys) {
            YQL_ENSURE(keyIndex < groupKeysItems.size());
            keysItems.push_back(groupKeysItems[keyIndex]);
        }

        auto keysNode = ctx.NewList(pos, std::move(keysItems));
        auto aggregate = ctx.Builder(pos)
            .Callable("Aggregate")
                .Add(0, list)
                .Add(1, keysNode)
                .Add(2, payloadsNode)
                .List(3) // options
                .Seal()
            .Seal()
            .Build();

        if (!extKeysItems.empty() && currentKeys.empty() && !nonNullDefAggs.empty()) {
            // restore aggregation keys
            auto joinColumns = ctx.NewList(pos, TExprNode::TListType(extKeysItems));
            auto joined = JoinColumns(pos, joinedUniqueExt, aggregate, joinColumns, {}, ctx);

            auto pgZero = ctx.Builder(pos)
                .Callable("PgConst")
                    .Atom(0, "0")
                    .Callable(1, "PgType")
                        .Atom(0, "int8")
                    .Seal()
                .Seal()
                .Build();

            auto arg = ctx.NewArgument(pos, "row");
            auto root = arg;
            for (ui32 i = 0; i < nonNullDefAggs.size(); ++i) {
                auto column = ToString("_yql_agg_") + ToString(nonNullDefAggs[i]);
                root = ctx.Builder(pos)
                    .Callable("ReplaceMember")
                        .Add(0, root)
                        .Atom(1, column)
                        .Callable(2, "Coalesce")
                            .Callable(0, "Member")
                                .Add(0, root)
                                .Atom(1, column)
                            .Seal()
                            .Add(1, pgZero)
                        .Seal()
                    .Seal()
                    .Build();
            }

            auto coalesceLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { arg }), std::move(root));

            // replace nulls with def values
            aggregate = ctx.Builder(pos)
                .Callable("OrderedMap")
                    .Add(0, joined)
                    .Add(1, coalesceLambda)
                .Seal()
                .Build();
        }

        if (currentKeys.size() < groupExprs->Tail().ChildrenSize()) {
            // mark missing columns
            aggregate = ctx.Builder(pos)
                .Callable("OrderedMap")
                    .Add(0, aggregate)
                    .Lambda(1)
                        .Param("row")
                        .Callable("FlattenMembers")
                            .List(0)
                                .Atom(0, "")
                                .Arg(1, "row")
                            .Seal()
                            .List(1)
                                .Atom(0, "")
                                .Callable(1, "AsStruct")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                                        ui32 j = 0;
                                        for (ui32 i = 0; i < groupExprs->Tail().ChildrenSize(); ++i) {
                                            if (!currentKeys.contains(i)) {
                                                parent.List(j++)
                                                    .Atom(0, "_yql_grouping_" + ToString(i))
                                                    .Callable(1, "Int32")
                                                        .Atom(0, "1")
                                                    .Seal()
                                                    .Seal();
                                            }
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

        unionAllItems.push_back(aggregate);
        // shift iterator
        ui32 i = 0;
        while (i < currentSetIndices.size()) {
            ++currentSetIndices[i];
            if (currentSetIndices[i] < setCounts[i]) {
                break;
            }

            currentSetIndices[i] = 0;
            ++i;
        }

        if (i == currentSetIndices.size()) {
            break;
        }
    }

    return ctx.NewCallable(pos, "UnionAll", std::move(unionAllItems));
}

TExprNode::TPtr BuildHaving(TPositionHandle pos, TExprNode::TPtr list, const TExprNode::TPtr& having,
    const TAggregationMap& aggId, const TVector<TString>& inputAliases, const TExprNode::TListType& cleanedInputs,
    TExprContext& ctx, TOptimizeContext& optCtx) {
    auto havingLambda = having->TailPtr();
    auto havingLambdaRoot = havingLambda->TailPtr();
    RewriteAggsPartial(havingLambdaRoot, havingLambda->Head().HeadPtr(), aggId, ctx, optCtx, false);
    auto havingSubLinks = GatherSubLinks(havingLambdaRoot);
    if (!havingSubLinks.empty()) {
        std::tie(havingLambdaRoot, list) = RewriteSubLinksPartial(havingLambda->Pos(), list, havingLambdaRoot,
            havingLambda->Head().HeadPtr(), havingSubLinks, inputAliases, cleanedInputs, ctx, optCtx);
    }

    return ctx.Builder(pos)
        .Callable("OrderedFilter")
            .Add(0, list)
            .Lambda(1)
                .Param("row")
                .Callable("Coalesce")
                    .Callable(0, "FromPg")
                        .ApplyPartial(0, havingLambda->HeadPtr(), havingLambdaRoot)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                    .Callable(1, "Bool")
                        .Atom(0, "0")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

std::tuple<TExprNode::TPtr, TExprNode::TPtr> BuildFrame(TPositionHandle pos, const TExprNode& frameSettings, TExprContext& ctx) {
    TExprNode::TPtr begin;
    TExprNode::TPtr end;
    const auto& from = GetSetting(frameSettings, "from");
    const auto& fromValue = GetSetting(frameSettings, "from_value");

    auto fromName = from->Tail().Content();
    if (fromName == "up") {
        begin = ctx.NewCallable(pos, "Void", {});
    } else if (fromName == "p") {
        auto val = FromString<i32>(fromValue->Tail().Head().Content());
        begin = ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, ToString(-val)) });
    } else if (fromName == "c") {
        begin = ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, "0") });
    } else {
        YQL_ENSURE(fromName == "f");
        auto val = FromString<i32>(fromValue->Tail().Head().Content());
        begin = ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, ToString(val)) });
    }

    const auto& to = GetSetting(frameSettings, "to");
    const auto& toValue = GetSetting(frameSettings, "to_value");

    auto toName = to->Tail().Content();
    if (toName == "p") {
        auto val = FromString<i32>(toValue->Tail().Head().Content());
        end = ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, ToString(-val)) });
    } else if (toName == "c") {
        end = ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, "0") });
    } else if (toName == "f") {
        auto val = FromString<i32>(toValue->Tail().Head().Content());
        end = ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, ToString(val)) });
    } else {
        YQL_ENSURE(toName == "uf");
        end = ctx.NewCallable(pos, "Void", {});
    }

    return { begin, end };
}

TExprNode::TPtr BuildSortTraits(TPositionHandle pos, const TExprNode& sortColumns, const TExprNode::TPtr& list,
    const TAggregationMap* aggId, TExprContext& ctx, TOptimizeContext& optCtx) {
    return ctx.Builder(pos)
        .Callable("SortTraits")
            .Callable(0, "TypeOf")
                .Add(0, list)
            .Seal()
            .List(1)
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < sortColumns.ChildrenSize(); ++i) {
                        parent.Callable(i, "Bool")
                            .Atom(0, sortColumns.Child(i)->Child(2)->Content() == "asc" ? "true" : "false")
                            .Seal();
                    }
                    return parent;
                })
            .Seal()
            .Lambda(2)
                .Param("row")
                .List()
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0; i < sortColumns.ChildrenSize(); ++i) {
                            auto lambda = sortColumns.Child(i)->ChildPtr(1);
                            if (aggId) {
                                RewriteAggs(lambda, *aggId, ctx, optCtx, false);
                            }

                            if (sortColumns.Child(i)->ChildPtr(3)->Content() == "last") {
                                parent.List(i)
                                    .Callable(0, "Not")
                                        .Callable(0, "Exists")
                                            .Apply(0, lambda)
                                                .With(0, "row")
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                    .Apply(1, lambda)
                                        .With(0, "row")
                                    .Seal()
                                .Seal();
                            } else {
                                parent.Apply(i, lambda)
                                    .With(0, "row")
                                    .Seal();
                            }
                        }

                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildWindows(TPositionHandle pos, const TExprNode::TPtr& list, const TExprNode::TPtr& window, const TWindowsCtx& winCtx,
    TExprNode::TPtr& projectionRoot, const TExprNode::TPtr& projectionArg, const TAggregationMap& aggId, TExprContext& ctx, TOptimizeContext& optCtx) {
    auto ret = list;
    auto listTypeNode = ctx.Builder(pos)
        .Callable("TypeOf")
            .Add(0, list)
        .Seal()
        .Build();

    for (const auto& x : winCtx.Window2funcs) {
        auto winDef = window->Tail().Child(x.first);
        const auto& frameSettings = winDef->Tail();

        TExprNode::TListType keysItems;
        if (winDef->Child(2)->ChildrenSize()) {
            auto arg = ctx.NewArgument(pos, "row");
            auto arguments = ctx.NewArguments(pos, { arg });

            TExprNode::TListType newColumns;
            for (ui32 i = 0; i < winDef->Child(2)->ChildrenSize(); ++i) {
                const auto& group = winDef->Child(2)->Child(i);
                auto lambda = group->TailPtr();
                RewriteAggs(lambda, aggId, ctx, optCtx, false);
                auto name = "_yql_partition_key_" + ToString(x.first) + "_" + ToString(i);
                keysItems.push_back(ctx.NewAtom(pos, name));
                newColumns.push_back(ctx.Builder(pos)
                    .List()
                        .Atom(0, name)
                        .Apply(1, *lambda)
                            .With(0, arg)
                        .Seal()
                    .Seal()
                    .Build());
            }

            auto newColumnsNode = ctx.NewCallable(pos, "AsStruct", std::move(newColumns));
            auto root = ctx.Builder(pos)
                .Callable("FlattenMembers")
                    .List(0)
                        .Atom(0, "")
                        .Add(1, arg)
                    .Seal()
                    .List(1)
                        .Atom(0, "")
                        .Add(1, newColumnsNode)
                    .Seal()
                .Seal()
                .Build();

            auto keyExprsLambda = ctx.NewLambda(pos, std::move(arguments), std::move(root));

            ret = ctx.Builder(pos)
                .Callable("OrderedMap")
                    .Add(0, ret)
                    .Add(1, keyExprsLambda)
                .Seal()
                .Build();
        }

        auto keysNode = ctx.NewList(pos, std::move(keysItems));
        auto sortNode = ctx.NewCallable(pos, "Void", {});
        TExprNode::TPtr keyLambda;
        if (winDef->Child(3)->ChildrenSize() > 0) {
            sortNode = BuildSortTraits(pos, *winDef->Child(3), ret, &aggId, ctx, optCtx);
            keyLambda = sortNode->TailPtr();
        } else {
            keyLambda = ctx.Builder(pos)
                .Lambda()
                    .Param("row")
                    .Callable("Void")
                    .Seal()
                .Seal()
                .Build();
        }

        TExprNode::TListType args;
        TExprNode::TPtr begin, end;
        bool useRange = false;
        if (HasSetting(frameSettings, "type")) {
            std::tie(begin, end) = BuildFrame(pos, frameSettings, ctx);
        } else {
            // default frame
            if (winDef->Child(3)->ChildrenSize() > 0) {
                useRange = true;
                begin = ctx.Builder(pos)
                    .List()
                        .Atom(0, "preceding")
                        .Atom(1, "unbounded")
                    .Seal()
                    .Build();
                end = ctx.Builder(pos)
                    .List()
                        .Atom(0, "currentRow")
                    .Seal()
                    .Build();
            } else {
                begin = ctx.NewCallable(pos, "Void", {});
                end = begin;
            }
        }

        args.push_back(ctx.Builder(pos)
            .List()
                .List(0)
                    .Atom(0, "begin")
                    .Add(1, begin)
                .Seal()
                .List(1)
                    .Atom(0, "end")
                    .Add(1, end)
                .Seal()
            .Seal()
            .Build());

        for (const auto& index : x.second) {
            auto p = winCtx.Funcs[index];
            auto name = p.first->Head().Content();
            bool isAgg = p.first->IsCallable("PgAggWindowCall");
            TExprNode::TPtr value;
            if (isAgg) {
                value = BuildAggregationTraits(pos, true, "", p, listTypeNode, &aggId, ctx, optCtx);
            } else {
                if (name == "row_number") {
                    value = ctx.Builder(pos)
                        .Callable("RowNumber")
                            .Callable(0, "TypeOf")
                            .Add(0, list)
                            .Seal()
                        .Seal()
                        .Build();
                } else if (name == "rank" || name == "dense_rank") {
                    value = ctx.Builder(pos)
                        .Callable((name == "rank") ? "Rank" : "DenseRank")
                            .Callable(0, "TypeOf")
                                .Add(0, list)
                            .Seal()
                            .Add(1, keyLambda)
                            .List(2)
                                .List(0)
                                    .Atom(0, "ansi")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                } else if (name == "lead" || name == "lag") {
                    auto arg = ctx.NewArgument(pos, "row");
                    auto arguments = ctx.NewArguments(pos, { arg });
                    auto root = p.first->TailPtr();
                    RewriteAggsPartial(root, arg, aggId, ctx, optCtx, false);
                    auto extractor = ctx.NewLambda(pos, std::move(arguments),
                        ctx.ReplaceNode(std::move(root), *p.second, arg));

                    value = ctx.Builder(pos)
                        .Callable(name == "lead" ? "Lead" : "Lag")
                            .Callable(0, "TypeOf")
                                .Add(0, list)
                            .Seal()
                            .Add(1, extractor)
                        .Seal()
                        .Build();
                } else if (name == "first_value" || name == "last_value") {
                    auto arg = ctx.NewArgument(pos, "row");
                    auto arguments = ctx.NewArguments(pos, { arg });
                    auto root = p.first->TailPtr();
                    RewriteAggsPartial(root, arg, aggId, ctx, optCtx, false);
                    auto extractor = ctx.NewLambda(pos, std::move(arguments),
                        ctx.ReplaceNode(std::move(root), *p.second, arg));

                    TExprNode::TPtr updater;
                    if (name == "first_value") {
                        updater = ctx.Builder(pos)
                            .Lambda()
                                .Param("item")
                                .Param("state")
                                .Arg("state")
                            .Seal()
                            .Build();
                    } else {
                        updater = ctx.Builder(pos)
                            .Lambda()
                                .Param("item")
                                .Param("state")
                                .Apply(extractor)
                                    .With(0, "item")
                                .Seal()
                            .Seal()
                            .Build();
                    }

                    value = ctx.Builder(pos)
                        .Callable("WindowTraits")
                            .Callable(0, "ListItemType")
                                .Add(0, listTypeNode)
                            .Seal()
                            .Add(1, extractor)
                            .Add(2, updater)
                            .Lambda(3)
                                .Param("item")
                                .Param("state")
                                .Callable("Void")
                                .Seal()
                            .Seal()
                            .Lambda(4)
                                .Param("state")
                                .Arg("state")
                            .Seal()
                            .Callable(5, "Null")
                            .Seal()
                        .Seal()
                        .Build();
                } else {
                    ythrow yexception() << "Not supported function: " << name;
                }
            }

            args.push_back(ctx.Builder(pos)
                .List()
                    .Atom(0, "_yql_win_" + ToString(index))
                    .Add(1, value)
                .Seal()
                .Build());
        }

        auto winOnRows = ctx.NewCallable(pos, useRange ? "WinOnRange" : "WinOnRows", std::move(args));

        auto frames = ctx.Builder(pos)
            .List()
                .Add(0, winOnRows)
            .Seal()
            .Build();

        ret = ctx.Builder(pos)
            .Callable("CalcOverWindow")
                .Add(0, ret)
                .Add(1, keysNode)
                .Add(2, sortNode)
                .Add(3, frames)
            .Seal()
            .Build();
    }

    auto status = OptimizeExpr(projectionRoot, projectionRoot, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        auto it = winCtx.FuncsId.find(node.Get());
        if (it != winCtx.FuncsId.end()) {
            auto ret = ctx.Builder(pos)
                .Callable("Member")
                    .Add(0, projectionArg)
                    .Atom(1, "_yql_win_" + ToString(it->second))
                .Seal()
                .Build();

            if (node->Head().Content() == "row_number" || node->Head().Content() == "rank" || node->Head().Content() == "dense_rank") {
                ret = ctx.Builder(node->Pos())
                    .Callable("ToPg")
                        .Callable(0, "SafeCast")
                            .Add(0, ret)
                            .Atom(1, "Int64")
                        .Seal()
                    .Seal()
                    .Build();
            }

            return ret;
        }

        return node;
    }, ctx, TOptimizeExprSettings(optCtx.Types));

    YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error);
    return ret;
}

TExprNode::TPtr BuildSortLambda(TPositionHandle pos, const TExprNode::TPtr& sort, TExprContext& ctx) {
    auto argNode = ctx.NewArgument(pos, "row");
    auto argsNode = ctx.NewArguments(pos, { argNode });
    TExprNode::TListType rootItems;
    const auto& keys = sort->Tail();
    for (const auto& key : keys.Children()) {
        auto keyLambda = key->ChildPtr(1);
        auto value = ctx.ReplaceNode(keyLambda->TailPtr(), keyLambda->Head().Head(), argNode);
        if (key->Child(3)->Content() == "last") {
            rootItems.push_back(ctx.Builder(pos)
                .List()
                    .Callable(0, "Not")
                        .Callable(0, "Exists")
                            .Add(0, value)
                        .Seal()
                    .Seal()
                    .Add(1, value)
                .Seal()
                .Build());
        } else {
            rootItems.push_back(value);
        }
    }

    auto root = ctx.NewList(pos, std::move(rootItems));
    auto lambda = ctx.NewLambda(pos, std::move(argsNode), std::move(root));
    return lambda;
}

TExprNode::TPtr BuildSortDirections(TPositionHandle pos, const TExprNode::TPtr& sort, TExprContext& ctx) {
    const auto& keys = sort->Tail();
    TExprNode::TListType dirItems;
    for (const auto& key : keys.Children()) {
        dirItems.push_back(ctx.Builder(pos)
            .Callable("Bool")
                .Atom(0, key->Child(2)->Content() == "asc" ? "true" : "false")
            .Seal()
            .Build());
    }

    return ctx.NewList(pos, std::move(dirItems));
}

TExprNode::TPtr BuildSort(TPositionHandle pos, const TExprNode::TPtr& list, const TExprNode::TPtr& directions,
    const TExprNode::TPtr& sortLambdaRoot, const TExprNode::TPtr& sortLambdaArgs, TExprContext& ctx) {

    return ctx.Builder(pos)
        .Callable("Sort")
            .Add(0, list)
            .Add(1, directions)
            .Lambda(2)
                .Param("row")
                .ApplyPartial(sortLambdaArgs, sortLambdaRoot)
                    .With(0, "row")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildDistinctOn(TPositionHandle pos, TExprNode::TPtr list, const TExprNode::TPtr& distinctOn,
    const TExprNode::TPtr& sort, TExprContext& ctx, TOptimizeContext& optCtx) {
    // filter by RowNumber() == 1

    TExprNode::TListType args;
    auto begin = ctx.NewCallable(pos, "Void", {});
    auto end = ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, "0") });
    args.push_back(ctx.Builder(pos)
        .List()
            .List(0)
                .Atom(0, "begin")
                .Add(1, begin)
            .Seal()
            .List(1)
                .Atom(0, "end")
                .Add(1, end)
            .Seal()
        .Seal()
        .Build());

    auto value = ctx.Builder(pos)
        .Callable("RowNumber")
            .Callable(0, "TypeOf")
                .Add(0, list)
            .Seal()
        .Seal()
        .Build();

    args.push_back(ctx.Builder(pos)
        .List()
            .Atom(0, "_yql_row_number")
            .Add(1, value)
        .Seal()
        .Build());

    auto winOnRows = ctx.NewCallable(pos, "WinOnRows", std::move(args));
    auto frames = ctx.Builder(pos)
        .List()
            .Add(0, winOnRows)
        .Seal()
        .Build();

    bool rebuildKey = false;
    for (auto p : distinctOn->Children()) {
        YQL_ENSURE(p->IsCallable("PgGroup"));
        NNodes::TCoLambda lambda(&p->Tail());
        if (!lambda.Body().Ref().IsCallable("Member") ||
            &lambda.Body().Ref().Head() != &lambda.Args().Arg(0).Ref()) {
            rebuildKey = true;
            break;
        }
    }

    TExprNode::TPtr keysNode;
    auto originalList = list;
    if (!rebuildKey) {
        TExprNode::TListType keys;
        for (auto p : distinctOn->Children()) {
            NNodes::TCoLambda lambda(&p->Tail());
            const auto& member = lambda.Body().Ref();
            YQL_ENSURE(member.IsCallable("Member"));
            keys.push_back(member.TailPtr());
        }

        keysNode = ctx.NewList(pos, std::move(keys));
    } else {
        TExprNode::TListType keys;
        for (ui32 i = 0; i < distinctOn->ChildrenSize(); ++i) {
            keys.push_back(ctx.NewAtom(pos, "_yql_distinct_on_" + ToString(i)));
        }

        keysNode = ctx.NewList(pos, std::move(keys));

        list = ctx.Builder(pos)
            .Callable("OrderedMap")
                .Add(0, list)
                .Lambda(1)
                    .Param("row")
                    .Callable("FlattenMembers")
                        .List(0)
                            .Atom(0, "")
                            .Arg(1, "row")
                        .Seal()
                        .List(1)
                            .Atom(0, "")
                            .Callable(1, "AsStruct")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                                    for (ui32 i = 0; i < distinctOn->ChildrenSize(); ++i) {
                                        parent.List(i)
                                            .Atom(0, "_yql_distinct_on_" + ToString(i))
                                            .Apply(1, distinctOn->Child(i)->Tail())
                                                .With(0, "row")
                                             .Seal()
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

    auto sortNode = ctx.NewCallable(pos, "Void", {});
    if (sort && sort->Tail().ChildrenSize() > 0) {
        sortNode = BuildSortTraits(pos, sort->Tail(), list, nullptr, ctx, optCtx);
    }

    auto ret = ctx.Builder(pos)
        .Callable("CalcOverWindow")
            .Add(0, list)
            .Add(1, keysNode)
            .Add(2, sortNode)
            .Add(3, frames)
        .Seal()
        .Build();

    ret = ctx.Builder(pos)
        .Callable("OrderedFilter")
            .Add(0, ret)
            .Lambda(1)
                .Param("row")
                .Callable("==")
                    .Callable(0, "Member")
                        .Arg(0, "row")
                        .Atom(1, "_yql_row_number")
                    .Seal()
                    .Callable(1, "Uint64")
                        .Atom(0, "1")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    ret = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, ret)
            .Lambda(1)
                .Param("row")
                .Callable("CastStruct")
                    .Arg(0, "row")
                    .Callable(1, "ListItemType")
                        .Callable(0, "TypeOf")
                            .Add(0, originalList)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return ret;
}

TExprNode::TPtr BuildOffset(TPositionHandle pos, const TExprNode::TPtr& offset, const TExprNode::TPtr& list, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Skip")
            .Add(0, list)
            .Callable(1, "Unwrap")
                .Callable(0, "SafeCast")
                    .Callable(0, "Coalesce")
                        .Callable(0,"FromPg")
                            .Add(0, offset->ChildPtr(1))
                        .Seal()
                        .Callable(1, "Int64")
                            .Atom(0, "0")
                        .Seal()
                    .Seal()
                    .Atom(1, "Uint64")
                .Seal()
                .Callable(1, "String")
                    .Atom(0, "Negative offset")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildLimit(TPositionHandle pos, const TExprNode::TPtr& limit, const TExprNode::TPtr& list, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Take")
            .Add(0, list)
            .Callable(1, "Unwrap")
                .Callable(0, "SafeCast")
                    .Callable(0, "Coalesce")
                        .Callable(0,"FromPg")
                            .Add(0, limit->ChildPtr(1))
                        .Seal()
                        .Callable(1, "Int64")
                            .Atom(0, "9223372036854775807") // 2**63-1
                        .Seal()
                    .Seal()
                    .Atom(1, "Uint64")
                .Seal()
                .Callable(1, "String")
                    .Atom(0, "Negative limit")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr EnumerateForExtColumns(TPositionHandle pos, const TExprNode::TPtr& list, ui32 sublinkId,
    const TMap<TString, ui32>& extColumns, const TExprNode::TPtr& directions, const TExprNode::TPtr& sortLambda,
    TExprContext& ctx) {
    TExprNode::TListType keysItems;
    for (const auto& x : extColumns) {
        keysItems.push_back(ctx.NewAtom(pos, TString("_yql_join_sublink_") + ToString(sublinkId) +
                                    "_" + x.first));
    }

    auto value = ctx.Builder(pos)
        .Callable("RowNumber")
            .Callable(0, "TypeOf")
                .Add(0, list)
            .Seal()
        .Seal()
        .Build();

    TExprNode::TListType args;
    auto begin = ctx.NewCallable(pos, "Void", {});
    auto end = ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, "0") });
    args.push_back(ctx.Builder(pos)
        .List()
            .List(0)
                .Atom(0, "begin")
                .Add(1, begin)
            .Seal()
            .List(1)
                .Atom(0, "end")
                .Add(1, end)
            .Seal()
        .Seal()
        .Build());

    args.push_back(ctx.Builder(pos)
        .List()
            .Atom(0, "_yql_row_number")
            .Add(1, value)
        .Seal()
        .Build());

    auto winOnRows = ctx.NewCallable(pos, "WinOnRows", std::move(args));

    auto frames = ctx.Builder(pos)
        .List()
            .Add(0, winOnRows)
        .Seal()
        .Build();

    auto keysNode = ctx.NewList(pos, std::move(keysItems));
    auto sortNode = ctx.NewCallable(pos, "Void", {});
    if (directions) {
        YQL_ENSURE(sortLambda);
        sortNode = ctx.Builder(pos)
            .Callable("SortTraits")
                .Callable(0, "TypeOf")
                    .Add(0, list)
                .Seal()
                .Add(1, directions)
                .Add(2, sortLambda)
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Callable("CalcOverWindow")
            .Add(0, list)
            .Add(1, keysNode)
            .Add(2, sortNode)
            .Add(3, frames)
        .Seal()
        .Build();
}

TExprNode::TPtr ApplyOffsetLimitOverEnumerated(TPositionHandle pos, const TExprNode::TPtr& list,
    const TExprNode::TPtr& offsetValue, const TExprNode::TPtr& limitValue, TExprContext& ctx) {
    auto arg = ctx.NewArgument(pos, "row");
    auto member = ctx.NewCallable(pos, "SafeCast", { 
        ctx.NewCallable(pos, "Member", { arg, ctx.NewAtom(pos, "_yql_row_number") }),
        ctx.NewAtom(pos, "Int64")});
    auto int8type = ctx.NewCallable(pos, "PgType", { ctx.NewAtom(pos, "int8")});
    auto null = ctx.NewCallable(pos, "PgCast", { ctx.NewCallable(pos, "Null", {}), int8type});
    auto zero = ctx.NewCallable(pos, "PgConst", { ctx.NewAtom(pos, "0"), int8type });
    auto truth = ctx.NewCallable(pos, "PgConst", { ctx.NewAtom(pos, "true"), ctx.NewCallable(pos, "PgType", { ctx.NewAtom(pos, "bool")}) });
    auto replacedOffset = offsetValue ? ctx.NewCallable(pos, "Coalesce", { offsetValue, zero }) : zero;
    auto replacedLimit = limitValue ? limitValue : null;
    // enumeration values starts with 1
    // predicate: enumeration > offset and if(exists(limit), enumeration <= offset + limit, pgbool(true))
    auto root = ctx.Builder(pos)
        .Callable("Coalesce")
            .Callable(0, "FromPg")
                .Callable(0, "PgAnd")
                    .Callable(0, "PgOp")
                        .Atom(0, ">")
                        .Add(1, member)
                        .Add(2, replacedOffset)
                    .Seal()
                    .Callable(1, "If")
                        .Callable(0, "Exists")
                            .Add(0, replacedLimit)
                        .Seal()
                        .Callable(1, "PgOp")
                            .Atom(0, "<=")
                            .Add(1, member)
                            .Callable(2, "PgOp")
                                .Atom(0, "+")
                                .Add(1, replacedOffset)
                                .Add(2, replacedLimit)
                            .Seal()
                        .Seal()
                        .Add(2, truth)
                    .Seal()
                .Seal()
            .Seal()
            .Callable(1, "Bool")
                .Atom(0, "false")
            .Seal()
        .Seal()
        .Build();

    auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { arg }), std::move(root));

    return ctx.Builder(pos)
        .Callable("Map")
            .Callable(0, "Filter")
                .Add(0, list)
                .Add(1, lambda)
            .Seal()
            .Lambda(1)
                .Param("row")
                .Callable("RemoveMember")
                    .Arg(0, "row")
                    .Atom(1, "_yql_row_number")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr AddExtColumns(const TExprNode::TPtr& projectionRoot, const TExprNode::TPtr& projectionArg,
    const TExprNode::TPtr& finalExtTypes, ui32 subLinkId, TExprContext& ctx) {
    return ctx.Builder(projectionRoot->Pos())
        .Callable("FlattenMembers")
            .List(0)
                .Atom(0, "")
                .Add(1, projectionRoot)
            .Seal()
            .List(1)
                .Atom(0, "_yql_join_sublink_" + ToString(subLinkId) + "_")
                .Callable(1, "FilterMembers")
                    .Add(0, projectionArg)
                    .List(1)
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                            ui32 i = 0;
                            for (const auto& x : finalExtTypes->Children()) {
                                auto alias = x->Head().Content();
                                auto type = x->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                                for (const auto& item : type->GetItems()) {
                                    auto withAlias = NTypeAnnImpl::MakeAliasedColumn(alias, item->GetName());
                                    parent.Atom(i++, withAlias);
                                }
                            }

                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

void BuildExtraSortColumns(const TExprNode::TPtr& groupExprs,
    const TExprNode::TPtr& extraSortColumns, const TExprNode::TPtr& extraSortKeys,
    size_t aggIndexBegin, size_t aggIndexEnd, TVector<TString>& list) {
    if (extraSortColumns && !groupExprs) {
        for (const auto& x : extraSortColumns->Tail().Children()) {
            for (const auto& y : x->Children()) {
                list.push_back(TString(y->Content()));
            }
        }
    }

    if (extraSortKeys) {
        for (const auto& k : extraSortKeys->Tail().Children()) {
            list.push_back(TString(k->Content()));
        }
    }

    for (auto aggIndex = aggIndexBegin; aggIndex < aggIndexEnd; ++aggIndex) {
        list.push_back("_yql_agg_" + ToString(aggIndex));
    }
}

TExprNode::TPtr AddExtraSortColumns(const TExprNode::TPtr& root, const TExprNode::TPtr& originalArg, const TExprNode::TPtr& groupExprs,
    const TExprNode::TPtr& extraSortColumns, const TExprNode::TPtr& extraSortKeys,
    size_t aggIndexBegin, size_t aggIndexEnd, TExprContext& ctx) {
    return ctx.Builder(root->Pos())
        .Callable("FlattenMembers")
            .List(0)
                .Atom(0, "")
                .Add(1, root)
            .Seal()
            .List(1)
                .Atom(0, "")
                .Callable(1, "AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                        TVector<TString> list;
                        BuildExtraSortColumns(groupExprs, extraSortColumns, extraSortKeys, aggIndexBegin, aggIndexEnd, list);
                        for (ui32 i = 0; i < list.size(); ++i) {
                            TStringBuf from = list[i];
                            from.SkipPrefix("_yql_extra_");
                            parent.List(i)
                                .Atom(0, list[i])
                                .Callable(1, "Member")
                                    .Add(0, originalArg)
                                    .Atom(1, from)
                                .Seal()
                                .Seal();
                        }

                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr RemoveExtraSortColumns(const TExprNode::TPtr& list, const TExprNode::TPtr& groupExprs,
    const TExprNode::TPtr& extraSortColumns, const TExprNode::TPtr& extraSortKeys,
    size_t aggIndexBegin, size_t aggIndexEnd, const TVector<TString>& sublinkColumns, TExprContext& ctx) {
    return ctx.Builder(list->Pos())
        .Callable("OrderedMap")
            .Add(0, list)
            .Lambda(1)
                .Param("row")
                .Callable("ForceRemoveMembers")
                    .Arg(0, "row")
                    .List(1)
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                            TVector<TString> list;
                            BuildExtraSortColumns(groupExprs, extraSortColumns, extraSortKeys, aggIndexBegin, aggIndexEnd, list);
                            list.insert(list.end(), sublinkColumns.begin(), sublinkColumns.end());
                            for (ui32 i = 0; i < list.size(); ++i) {
                                parent.Atom(i, list[i]);
                            }

                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

std::pair<TExprNode::TPtr, TExprNode::TPtr> JoinOuter(TPositionHandle pos, TExprNode::TPtr list,
    const TExprNode::TPtr& finalExtTypes, const TExprNode::TListType& outerInputs,
    const TVector<TString>& outerInputAliases,
    TExprNode::TListType& cleanedInputs, TVector<TString>& inputAliases, TExprContext& ctx) {
    YQL_ENSURE(finalExtTypes);
    YQL_ENSURE(outerInputs.size() == finalExtTypes->Tail().ChildrenSize());
    TExprNode::TPtr joinedUniqueExt;
    for (ui32 index = 0; index < finalExtTypes->Tail().ChildrenSize(); ++index) {
        const auto& input = finalExtTypes->Tail().Child(index);
        const auto& inputAlias = input->Head().Content();
        YQL_ENSURE(inputAlias == outerInputAliases[index]);
        inputAliases.push_back(TString(inputAlias));
        cleanedInputs.push_back(outerInputs[index]);

        auto type = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        if (type->GetSize() == 0) {
            continue;
        }

        auto colList = ctx.Builder(pos)
            .List()
            .Do([&](TExprNodeBuilder &parent) -> TExprNodeBuilder & {
                ui32 i = 0;
                for (const auto& item : type->GetItems()) {
                    parent.Atom(i++, NTypeAnnImpl::MakeAliasedColumn(inputAlias, item->GetName()));
                }

                return parent;
            })
            .Seal()
            .Build();

        auto outerInput = ctx.Builder(pos)
            .Callable("ExtractMembers")
            .Add(0, outerInputs[index])
            .Add(1, colList)
            .Seal()
            .Build();

        auto uniqueOuterInput = ctx.Builder(pos)
            .Callable("Aggregate")
            .Add(0, outerInput)
            .Add(1, colList)
            .List(2)
            .Seal()
            .Seal()
            .Build();

        if (!joinedUniqueExt) {
            joinedUniqueExt = uniqueOuterInput;
        } else {
            joinedUniqueExt = JoinColumns(pos, joinedUniqueExt, uniqueOuterInput, nullptr, {}, ctx);
        }
    }

    if (joinedUniqueExt) {
        list = JoinColumns(pos, list, joinedUniqueExt, nullptr, {}, ctx);
    }

    return { list, joinedUniqueExt };
}

TExprNode::TPtr CombineSetItems(TPositionHandle pos, const TExprNode::TPtr& left, const TExprNode::TPtr& right, const TStringBuf& op, TExprContext& ctx) {
    if (op == "union_all") {
        return ctx.NewCallable(pos, "UnionAll", { left, right });
    }

    auto leftSide = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, left)
            .Lambda(1)
                .Param("row")
                .Callable("AddMember")
                    .Callable(0, "AddMember")
                        .Arg(0, "row")
                        .Atom(1, "_yql_count_right")
                        .Callable(2, "Null")
                        .Seal()
                    .Seal()
                    .Atom(1, "_yql_count_left")
                    .Callable(2, "Uint32")
                        .Atom(0, "1")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto rightSide = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, right)
            .Lambda(1)
                .Param("row")
                .Callable("AddMember")
                    .Callable(0, "AddMember")
                        .Arg(0, "row")
                        .Atom(1, "_yql_count_right")
                        .Callable(2, "Uint32")
                            .Atom(0, "1")
                        .Seal()
                    .Seal()
                    .Atom(1, "_yql_count_left")
                    .Callable(2, "Null")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto both = ctx.NewCallable(pos, "UnionAll", { leftSide, rightSide });
    auto aggregated = ctx.Builder(pos)
        .Callable("CountedAggregateAll")
            .Add(0, both)
            .List(1)
                .Atom(0, "_yql_count_left")
                .Atom(1, "_yql_count_right")
            .Seal()
        .Seal()
        .Build();

    TExprNode::TPtr ret;
    auto zero = ctx.Builder(pos)
        .Callable("Uint64")
            .Atom(0, "0")
        .Seal()
        .Build();

    if (!op.EndsWith("_all")) {
        if (op.StartsWith("union")) {
            ret = ctx.Builder(pos)
                .Callable("OrderedFilter")
                    .Add(0, aggregated)
                    .Lambda(1)
                        .Param("row")
                        .Callable("Or")
                            .Callable(0, ">")
                                .Callable(0, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, "_yql_count_left")
                                .Seal()
                                .Add(1, zero)
                            .Seal()
                            .Callable(1, ">")
                                .Callable(0, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, "_yql_count_right")
                                .Seal()
                                .Add(1, zero)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        } else if (op.StartsWith("intersect")) {
            ret = ctx.Builder(pos)
                .Callable("OrderedFilter")
                    .Add(0, aggregated)
                    .Lambda(1)
                        .Param("row")
                        .Callable("And")
                            .Callable(0, ">")
                                .Callable(0, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, "_yql_count_left")
                                .Seal()
                                .Add(1, zero)
                            .Seal()
                            .Callable(1, ">")
                                .Callable(0, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, "_yql_count_right")
                                .Seal()
                                .Add(1, zero)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        } else {
            YQL_ENSURE(op.StartsWith("except"));
            ret = ctx.Builder(pos)
                .Callable("OrderedFilter")
                    .Add(0, aggregated)
                    .Lambda(1)
                        .Param("row")
                        .Callable(0, ">")
                            .Callable(0, "Member")
                                .Arg(0, "row")
                                .Atom(1, "_yql_count_left")
                            .Seal()
                            .Callable(1, "Member")
                                .Arg(0, "row")
                                .Atom(1, "_yql_count_right")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }
    } else {
        YQL_ENSURE(!op.StartsWith("union"));
        if (op.StartsWith("intersect")) {
            ret = ctx.Builder(pos)
                .Callable("OrderedFlatMap")
                    .Add(0, aggregated)
                    .Lambda(1)
                        .Param("row")
                        .Callable("Replicate")
                            .Arg(0, "row")
                            .Callable(1, "Min")
                                .Callable(0, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, "_yql_count_left")
                                .Seal()
                                .Callable(1, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, "_yql_count_right")
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        } else {
            YQL_ENSURE(op.StartsWith("except"));
            ret = ctx.Builder(pos)
                .Callable("OrderedFlatMap")
                    .Add(0, aggregated)
                    .Lambda(1)
                        .Param("row")
                        .Callable("Replicate")
                            .Arg(0, "row")
                            .Callable(1, "-")
                                .Callable(0, "Max")
                                    .Callable(0, "Member")
                                        .Arg(0, "row")
                                        .Atom(1, "_yql_count_left")
                                    .Seal()
                                    .Callable(1, "Member")
                                        .Arg(0, "row")
                                        .Atom(1, "_yql_count_right")
                                    .Seal()
                                .Seal()
                                .Callable(1, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, "_yql_count_right")
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
            }
    }

    ret = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, ret)
            .Lambda(1)
                .Param("row")
                .Callable("RemoveMembers")
                    .Arg(0, "row")
                    .List(1)
                        .Atom(0, "_yql_count_left")
                        .Atom(1, "_yql_count_right")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
    return ret;
}

TExprNode::TPtr ExpandPgSelectImpl(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx,
    TMaybe<ui32> subLinkId, const TExprNode::TListType& outerInputs, const TVector<TString>& outerInputAliases) {
    auto order = optCtx.Types->LookupColumnOrder(*node);
    YQL_ENSURE(order);
    TExprNode::TListType columnsItems;
    for (const auto& x : *order) {
        columnsItems.push_back(ctx.NewAtom(node->Pos(), x));
    }

    TMap<TString, ui32> extColumns;
    TColumnOrder targetOrder = *order;
    if (subLinkId) {
        extColumns = NTypeAnnImpl::ExtractExternalColumns(*node);
        for (const auto& x : extColumns) {
            auto name = TString("_yql_join_sublink_") + ToString(*subLinkId) + "_" + x.first;
            columnsItems.push_back(ctx.NewAtom(node->Pos(), name));
            targetOrder.push_back(name);
        }
    }

    auto columns = ctx.NewList(node->Pos(), std::move(columnsItems));
    auto setItems = GetSetting(node->Head(), "set_items");
    auto setOps = GetSetting(node->Head(), "set_ops");
    YQL_ENSURE(setItems);
    YQL_ENSURE(setOps);
    const bool onlyOneSetItem = (setItems->Tail().ChildrenSize() == 1);

    TExprNode::TPtr finalSortLambda;
    TExprNode::TPtr finalSortDirections;

    TExprNode::TListType setItemNodes;
    TVector<TColumnOrder> columnOrders;
    for (auto setItem : setItems->Tail().Children()) {
        auto childOrder = optCtx.Types->LookupColumnOrder(*setItem);
        YQL_ENSURE(childOrder);
        columnOrders.push_back(*childOrder);
        if (subLinkId) {
            auto& setOrder = columnOrders.back();
            setOrder.insert(setOrder.end(), targetOrder.end() - extColumns.size(), targetOrder.end());
        }

        auto finalExtTypes = GetSetting(setItem->Tail(), "final_ext_types");
        if (finalExtTypes && !subLinkId) {
            return node;
        }

        auto result = GetSetting(setItem->Tail(), "result");
        auto values = GetSetting(setItem->Tail(), "values");
        auto from = GetSetting(setItem->Tail(), "from");
        auto filter = GetSetting(setItem->Tail(), "where");
        auto joinOps = GetSetting(setItem->Tail(), "join_ops");
        auto groupExprs = GetSetting(setItem->Tail(), "group_exprs");
        auto groupSets = GetSetting(setItem->Tail(), "group_sets");
        auto having = GetSetting(setItem->Tail(), "having");
        auto window = GetSetting(setItem->Tail(), "window");
        auto distinctAll = GetSetting(setItem->Tail(), "distinct_all");
        auto distinctOn = GetSetting(setItem->Tail(), "distinct_on");
        auto sort = GetSetting(setItem->Tail(), "sort");
        auto extraSortColumns = GetSetting(setItem->Tail(), "final_extra_sort_columns");
        auto extraSortKeys = GetSetting(setItem->Tail(), "final_extra_sort_keys");
        bool emitPgStar = (GetSetting(setItem->Tail(), "emit_pg_star") != nullptr);
        bool unknownsAllowed = (GetSetting(setItem->Tail(), "unknowns_allowed") != nullptr);
        bool oneRow = !from;
        TExprNode::TPtr list;
        if (values) {
            YQL_ENSURE(!result);
            list = BuildValues(node->Pos(), values, ctx);

            if (!unknownsAllowed) {
                auto pos = node->Pos();
                list = ctx.NewCallable(pos, "PgReplaceUnknown", { std::move(list), });
            }
        } else {
            YQL_ENSURE(result);
            auto finalType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            Y_ENSURE(finalType);
            TExprNode::TPtr projectionLambda = BuildProjectionLambda(node->Pos(), result, finalType, *order, *childOrder, subLinkId.Defined(), emitPgStar, ctx);
            TExprNode::TPtr projectionArg = projectionLambda->Head().HeadPtr();
            TExprNode::TPtr projectionRoot = projectionLambda->TailPtr();
            TVector<TString> inputAliases;
            TExprNode::TListType cleanedInputs;
            TWindowsCtx winCtx;
            if (window) {
                GatherUsedWindows(window, projectionLambda, winCtx);
            }

            if (oneRow) {
                list = BuildOneRow(node->Pos(), ctx);
                inputAliases.push_back("");
                cleanedInputs.push_back(list);
            } else {
                // extract all used columns
                TVector<std::pair<TString, TString>> joinUsingColumns;
                auto usedColumns = GatherUsedColumns(result, joinOps, filter, groupExprs, having, extraSortColumns, window, winCtx, joinUsingColumns);

                // fill index of input for each column
                FillInputIndices(from, finalExtTypes, usedColumns, optCtx);

                THashMap<TString, ui32> memberToInput;
                cleanedInputs = BuildCleanedColumns(node->Pos(), from, usedColumns, inputAliases, memberToInput, ctx, *optCtx.Types);
                if (cleanedInputs.size() == 1) {
                    list = cleanedInputs.front();
                } else {
                    TVector<ui32> groupForIndex;
                    TExprNode::TListType joinGroups;
                    std::tie(groupForIndex, joinGroups) = BuildJoinGroups(node->Pos(), cleanedInputs, joinOps, memberToInput, ctx, optCtx);
                    if (joinGroups.size() == 1) {
                        list = joinGroups.front();
                    } else {
                        list = BuildCrossJoinsBetweenGroups(node->Pos(), joinGroups, usedColumns, groupForIndex, ctx, joinUsingColumns);
                    }
                }
            }

            TExprNode::TPtr joinedUniqueExt;
            if (!outerInputs.empty() && finalExtTypes && 0 < finalExtTypes->Tail().ChildrenSize()) {
                std::tie(list, joinedUniqueExt) = JoinOuter(node->Pos(), list, finalExtTypes, outerInputs, outerInputAliases, cleanedInputs, inputAliases, ctx);
            }

            if (filter) {
                list = BuildFilter(node->Pos(), list, filter->Tail().TailPtr(), inputAliases, cleanedInputs, ctx, optCtx);
            }

            TAggs aggs;
            TAggregationMap aggId;

            GatherAggregationsFromLambda(projectionLambda, aggs, aggId, false);
            if (having) {
                GatherAggregationsFromLambda(having->Tail().TailPtr(), aggs, aggId, false);
            }

            if (!winCtx.Window2funcs.empty()) {
                YQL_ENSURE(window);
                for (const auto& x : winCtx.Window2funcs) {
                    auto winDef = window->Tail().Child(x.first);
                    for (ui32 i = 0; i < winDef->Child(2)->ChildrenSize(); ++i) {
                        const auto& group = winDef->Child(2)->Child(i);
                        GatherAggregationsFromLambda(group->TailPtr(), aggs, aggId, false);
                    }

                    for (ui32 i = 0; i < winDef->Child(3)->ChildrenSize(); ++i) {
                        const auto& sort = winDef->Child(3)->Child(i);
                        GatherAggregationsFromLambda(sort->ChildPtr(1), aggs, aggId, false);
                    }
                }
            }

            TExprNode::TPtr sortLambda;
            auto aggsSizeBeforeSort = aggs.size();
            if (sort) {
                sortLambda = BuildSortLambda(node->Pos(), sort, ctx);
                GatherAggregationsFromLambda(sortLambda, aggs, aggId, false);
            }

            if (groupExprs) {
                list = BuildGroup(node->Pos(), list, aggs, groupExprs, groupSets, finalExtTypes, joinedUniqueExt, ctx, optCtx);
            }

            if (having) {
                list = BuildHaving(node->Pos(), list, having->TailPtr(), aggId, inputAliases, cleanedInputs, ctx, optCtx);
            }

            if (!winCtx.Funcs.empty()) {
                list = BuildWindows(node->Pos(), list, window, winCtx, projectionRoot, projectionArg, aggId, ctx, optCtx);
            }

            RewriteAggsPartial(projectionRoot, projectionArg, aggId, ctx, optCtx, false);
            auto projectionSubLinks = GatherSubLinks(projectionRoot);
            if (!projectionSubLinks.empty()) {
                std::tie(projectionRoot, list) = RewriteSubLinksPartial(projectionLambda->Pos(), list, projectionRoot, projectionArg,
                    projectionSubLinks, inputAliases, cleanedInputs, ctx, optCtx);
            }

            if (finalExtTypes) {
                projectionRoot = AddExtColumns(projectionRoot, projectionArg, finalExtTypes->TailPtr(), *subLinkId, ctx);
            }

            bool hasExtraSortColumns = (extraSortColumns || extraSortKeys || (aggsSizeBeforeSort < aggs.size()));
            if (hasExtraSortColumns) {
                YQL_ENSURE(!distinctAll && !distinctOn);
                projectionRoot = AddExtraSortColumns(projectionRoot, projectionArg, groupExprs, extraSortColumns, extraSortKeys, aggsSizeBeforeSort, aggs.size(), ctx);
            }

            list = ctx.Builder(node->Pos())
                .Callable("OrderedMap")
                    .Add(0, list)
                    .Lambda(1)
                        .Param("row")
                        .ApplyPartial(projectionLambda->HeadPtr(), projectionRoot)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();

            if (!unknownsAllowed) {
                auto pos = node->Pos();
                list = ctx.NewCallable(pos, "PgReplaceUnknown", { std::move(list), });
            }

            if (distinctAll) {
                YQL_ENSURE(!extraSortColumns);
                list = ctx.NewCallable(node->Pos(), "SqlAggregateAll", { list });
            } else if (distinctOn) {
                YQL_ENSURE(!extraSortColumns);
                list = BuildDistinctOn(node->Pos(), list, distinctOn->TailPtr(), sort, ctx, optCtx);
            }

            TVector<TString> sublinkColumns;
            if (sort) {
                auto sortLambdaRoot = sortLambda->TailPtr();
                RewriteAggsPartial(sortLambdaRoot, sortLambda->Head().HeadPtr(), aggId, ctx, optCtx, false);

                auto sortSubLinks = GatherSubLinks(sortLambdaRoot);
                if (!sortSubLinks.empty()) {
                    std::tie(sortLambdaRoot, list) = RewriteSubLinksPartial(sortLambda->Pos(), list, sortLambdaRoot,
                        sortLambda->Head().HeadPtr(), sortSubLinks, inputAliases, cleanedInputs, ctx, optCtx, "_yql_extra_", &sublinkColumns);
                }

                auto directions = BuildSortDirections(node->Pos(), sort, ctx);
                if (!finalExtTypes) {
                    list = BuildSort(node->Pos(), list, directions, sortLambdaRoot, sortLambda->HeadPtr(), ctx);    
                } else if (onlyOneSetItem && finalExtTypes) {
                    finalSortDirections = directions;
                    finalSortLambda = sortLambda;
                }
            }

            if (hasExtraSortColumns) {
                list = RemoveExtraSortColumns(list, groupExprs, extraSortColumns, extraSortKeys, aggsSizeBeforeSort, aggs.size(), sublinkColumns, ctx);
            }
        }

        setItemNodes.push_back(list);
    }

    TExprNode::TPtr list;
    if (onlyOneSetItem == 1) {
        list = setItemNodes.front();
    } else {
        bool hasNonUnionAll = false;
        for (const auto& x : setOps->Tail().Children()) {
            if (x->Content() != "push" && x->Content() != "union_all") {
                hasNonUnionAll = true;
                break;
            }
        }

        if (hasNonUnionAll) {
            TExprNode::TListType stack;
            ui32 inputIndex = 0;
            for (const auto& x : setOps->Tail().Children()) {
                if (x->Content() == "push") {
                    YQL_ENSURE(inputIndex < setItemNodes.size());
                    stack.push_back(NormalizeColumnOrder(setItemNodes[inputIndex], columnOrders[inputIndex], targetOrder, ctx));
                    ++inputIndex;
                    continue;
                }

                YQL_ENSURE(stack.size() >= 2);
                auto left = stack[stack.size() - 2];
                auto right = stack[stack.size() - 1];
                stack.pop_back();
                stack.pop_back();
                auto combined = CombineSetItems(node->Pos(), left, right, x->Content(), ctx);
                stack.push_back(combined);
            }

            YQL_ENSURE(stack.size() == 1);
            list = KeepColumnOrder(targetOrder, stack.front(), ctx);
        } else {
            TExprNode::TListType children = setItemNodes;
            for (ui32 childIndex = 0; childIndex < children.size(); ++childIndex) {
                const auto& childColumnOrder = columnOrders[childIndex];
                auto& child = children[childIndex];
                child = NormalizeColumnOrder(child, childColumnOrder, targetOrder, ctx);
            }

            auto res = ctx.NewCallable(node->Pos(), "UnionAll", std::move(children));
            list = KeepColumnOrder(targetOrder, res, ctx);
        }
    }

    auto finalSort = GetSetting(node->Head(), "sort");
    auto limit = GetSetting(node->Head(), "limit");
    auto offset = GetSetting(node->Head(), "offset");

    if (finalSort && finalSort->Tail().ChildrenSize() > 0) {
        finalSortLambda = BuildSortLambda(node->Pos(), finalSort, ctx);
        finalSortDirections = BuildSortDirections(node->Pos(), finalSort, ctx);
    }

    if (extColumns.empty()) {
        if (finalSortLambda) {
            list = BuildSort(node->Pos(), list, finalSortDirections, finalSortLambda->TailPtr(), finalSortLambda->HeadPtr(), ctx);
        }

        if (offset) {
            list = BuildOffset(node->Pos(), offset, list, ctx);
        }

        if (limit) {
            list = BuildLimit(node->Pos(), limit, list, ctx);
        }
    } else {
        if (offset || limit) {
            // sort doesn't matter in sublinks without limit/offset
            TExprNode::TPtr offsetValue = offset ? WrapWithNonNegativeCheck(node->Pos(), offset->ChildPtr(1), ctx, "OFFSET must not be negative") : nullptr;
            TExprNode::TPtr limitValue = limit ? WrapWithNonNegativeCheck(node->Pos(), limit->ChildPtr(1), ctx, "LIMIT must not be negative") : nullptr;

            // enumerate all rows for each external key
            auto enumerated = EnumerateForExtColumns(node->Pos(), list, *subLinkId, extColumns, finalSortDirections, finalSortLambda, ctx);

            // apply limit & offset and drop a column used for enumeration
            list = ApplyOffsetLimitOverEnumerated(node->Pos(), enumerated, offsetValue, limitValue, ctx);
        }
    }

    return ctx.Builder(node->Pos())
        .Callable("AssumeColumnOrder")
            .Add(0, list)
            .Add(1, columns)
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPgSelect(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    return ExpandPgSelectImpl(node, ctx, optCtx, Nothing(), {}, {});
}

TExprNode::TPtr ExpandPgSelectSublink(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx, ui32 subLinkId,
    const TExprNode::TListType& outerInputs, const TVector<TString>& outerInputAliases) {
    return ExpandPgSelectImpl(node, ctx, optCtx, subLinkId, outerInputs, outerInputAliases);
}

TExprNode::TPtr ExpandPgLike(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    Y_UNUSED(optCtx);
    const bool insensitive = node->IsCallable("PgILike");
    if (!insensitive) {
        auto pattern = node->Child(1);
        bool isCasted = false;

        if (pattern->IsCallable("PgCast") &&
            pattern->Head().IsCallable("PgConst") &&
            pattern->Tail().IsCallable("PgType") &&
            pattern->Tail().Head().Content() == "text")
        {
            pattern = pattern->Child(0);
            isCasted = true;
        }

        if (pattern->IsCallable("PgConst") &&
            pattern->Tail().IsCallable("PgType") &&
            (pattern->Tail().Head().Content() == "text" || isCasted)) {
            auto str = pattern->Head().Content();
            auto hasUnderscore = AnyOf(str, [](char c) { return c == '_'; });
            size_t countOfPercents = 0;
            ForEach(str.begin(), str.end(), [&](char c) { countOfPercents += (c == '%');});
            if (!hasUnderscore && countOfPercents == 0) {
                return ctx.Builder(node->Pos())
                    .Callable("PgOp")
                        .Atom(0, "=")
                        .Add(1, node->ChildPtr(0))
                        .Add(2, pattern)
                    .Seal()
                    .Build();
            }

            TStringBuf op;
            TStringBuf arg;
            if (!hasUnderscore && countOfPercents == 1 && str.StartsWith('%')) {
                op = "EndsWith";
                arg = str.SubString(1, str.Size() - 1);
            }

            if (!hasUnderscore && countOfPercents == 1 && str.EndsWith('%')) {
                op = "StartsWith";
                arg = str.SubString(0, str.Size() - 1);
            }

            if (!hasUnderscore && countOfPercents == 2 && str.StartsWith('%') && str.EndsWith('%')) {
                op = "StringContains";
                arg = str.SubString(1, str.Size() - 2);
            }

            if (!op.empty()) {
                return ctx.Builder(node->Pos())
                    .Callable("ToPg")
                        .Callable(0, op)
                            .Callable(0, "FromPg")
                                .Add(0, node->ChildPtr(0))
                            .Seal()
                            .Callable(1, "String")
                                .Atom(0, arg)
                            .Seal()
                        .Seal()
                    .Seal()
                    .Build();
            }
        }
    }

    auto matcher = ctx.Builder(node->Pos())
        .Callable("Udf")
            .Atom(0, "Re2.Match")
            .List(1)
                .Callable(0, "Apply")
                    .Callable(0, "Udf")
                        .Atom(0, "Re2.PatternFromLike")
                    .Seal()
                    .Callable(1, "Coalesce")
                        .Callable(0, "FromPg")
                            .Add(0, node->ChildPtr(1))
                        .Seal()
                        .Callable(1, "Utf8")
                            .Atom(0, "")
                        .Seal()
                    .Seal()
                .Seal()
                .Callable(1, "NamedApply")
                    .Callable(0, "Udf")
                        .Atom(0, "Re2.Options")
                    .Seal()
                    .List(1)
                    .Seal()
                    .Callable(2, "AsStruct")
                        .List(0)
                            .Atom(0, "CaseSensitive")
                            .Callable(1, "Bool")
                                .Atom(0, insensitive ? "false" : "true")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(node->Pos())
        .Callable("ToPg")
            .Callable(0, "If")
                .Callable(0, "And")
                    .Callable(0, "Exists")
                        .Add(0, node->ChildPtr(0))
                    .Seal()
                    .Callable(1, "Exists")
                        .Add(0, node->ChildPtr(1))
                    .Seal()
                .Seal()
                .Callable(1, "Apply")
                    .Add(0, matcher)
                    .Callable(1, "FromPg")
                        .Add(0, node->ChildPtr(0))
                    .Seal()
                .Seal()
                .Callable(2, "Null")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPgIn(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    Y_UNUSED(optCtx);
    return ctx.Builder(node->Pos())
        .Callable("ToPg")
            .Callable(0, "SqlIn")
                .Add(0, node->ChildPtr(1))
                .Add(1, node->ChildPtr(0))
                .List(2)
                    .List(0)
                        .Atom(0, "ansi")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPgBetween(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    Y_UNUSED(optCtx);
    const bool isSym = node->IsCallable("PgBetweenSym");
    auto input = node->ChildPtr(0);
    auto begin = node->ChildPtr(1);
    auto end = node->ChildPtr(2);
    if (isSym) {
        auto swap = ctx.Builder(node->Pos())
            .Callable("PgOp")
                .Atom(0, "<")
                .Add(1, end)
                .Add(2, begin)
            .Seal()
            .Build();

        auto swapper = [&](auto x, auto y) {
            return ctx.Builder(node->Pos())
                .Callable("IfPresent")
                    .Callable(0, "FromPg")
                        .Add(0, swap)
                    .Seal()
                    .Lambda(1)
                        .Param("unwrapped")
                        .Callable("If")
                            .Arg(0, "unwrapped")
                            .Add(1, y)
                            .Add(2, x)
                        .Seal()
                    .Seal()
                    .Callable(2, "Null")
                    .Seal()
                .Seal()
                .Build();
        };

        // swap: null->null, false->begin, true->end
        auto newBegin = swapper(begin, end);
        // swap: null->null, false->end, true->begin
        auto newEnd = swapper(end, begin);
        begin = newBegin;
        end = newEnd;
    }

    return ctx.Builder(node->Pos())
        .Callable("PgAnd")
            .Callable(0, "PgOp")
                .Atom(0, ">=")
                .Add(1, input)
                .Add(2, begin)
            .Seal()
            .Callable(1, "PgOp")
                .Atom(0, "<=")
                .Add(1, input)
                .Add(2, end)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPgGroupRef(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    Y_UNUSED(optCtx);
    return ctx.Builder(node->Pos())
        .Callable("Member")
            .Add(0, node->HeadPtr())
            .Atom(1, node->ChildrenSize() == 3 ? ("_yql_agg_key_" + ToString(node->Tail().Content())) : node->Tail().Content())
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPgGrouping(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    Y_UNUSED(optCtx);
    TExprNode::TPtr sum;
    YQL_ENSURE(node->ChildrenSize() >= 1);
    for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
        auto child = node->Child(i);
        YQL_ENSURE(child->IsCallable("PgGroupRef"));
        auto row = child->HeadPtr();
        auto index = child->Child(2)->Content();
        auto value = ctx.Builder(node->Pos())
            .Callable("Coalesce")
                .Callable(0, "TryMember")
                    .Add(0, row)
                    .Atom(1, TString("_yql_grouping_") + index)
                    .Callable(2, "Null")
                    .Seal()
                .Seal()
                .Callable(1, "Int32")
                    .Atom(0, "0")
                .Seal()
            .Seal()
            .Build();

        if (!sum) {
            sum = value;
        } else {
            sum = ctx.Builder(node->Pos())
                .Callable("+")
                    .Callable(0, "*")
                        .Add(0, sum)
                        .Callable(1, "Int32")
                            .Atom(0, "2")
                        .Seal()
                    .Seal()
                    .Add(1, value)
                .Seal()
                .Build();
        }
    }

    return ctx.Builder(node->Pos())
        .Callable("ToPg")
            .Add(0, sum)
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPgIterate(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    const bool all = node->Content().EndsWith("All");
    auto init = node->HeadPtr();
    init = ctx.WrapByCallableIf(!all, "ListUniq", std::move(init));
    auto lambda = node->TailPtr();
    const auto limit = optCtx.Types->PgIterateLimit;
    auto itemArg = ctx.NewArgument(node->Pos(), "item");
    auto stateArg = ctx.NewArgument(node->Pos(), "state");
    auto state = ctx.Builder(node->Pos())
        .Callable("Ensure")
            .Add(0, stateArg)
            .Callable(1, "<")
                .Add(0, itemArg)
                .Callable(1, "Uint32")
                    .Atom(0, limit)
                .Seal()
            .Seal()
            .Callable(2, "String")
                .Atom(0, "Too many CTE iterations: " + ToString(limit))
            .Seal()
        .Seal()
        .Build();

    auto currentIter = ctx.NewCallable(node->Pos(), "Nth", { state, ctx.NewAtom(node->Pos(), 1)});
    auto add = ctx.Builder(node->Pos())
        .Apply(lambda)
            .With(0, currentIter)
        .Seal()
        .Build();

    auto currentRes = ctx.NewCallable(node->Pos(), "Nth", { state, ctx.NewAtom(node->Pos(), 0)});
    add = ctx.WrapByCallableIf(!all, "ListUniq", std::move(add));
    if (!all) {
        auto id = ctx.Builder(node->Pos())
            .Lambda()
                .Param("x")
                .Arg("x")
            .Seal()
            .Build();

        add = ctx.Builder(node->Pos())
            .Callable("Filter")
                .Add(0, add)
                .Lambda(1)
                    .Param("item")
                    .Callable("Not")
                        .Callable(0, "Contains")
                            .Callable(0, "ToDict")
                                .Add(0, currentRes)
                                .Add(1, id)
                                .Add(2, id)
                                .List(3)
                                    .Atom(0, "Auto", TNodeFlags::Default)
                                    .Atom(1, "One", TNodeFlags::Default)
                                .Seal()
                            .Seal()
                            .Arg(1, "item")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    auto res = ctx.NewCallable(node->Pos(), "Extend", { currentRes, add });
    auto p = ctx.NewList(node->Pos(), { res, add });
    auto foldLambdaBody = ctx.NewList(node->Pos(), { p, p});
    auto foldLambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), { itemArg, stateArg}), std::move(foldLambdaBody));
    auto foldMap = ctx.Builder(node->Pos())
        .Callable("FoldMap")
            .Callable(0, "ListFromRange")
                .Callable(0, "Uint32")
                    .Atom(0, 0)
                .Seal()
                .Callable(1, "Uint32")
                    .Atom(0, limit + 1)
                .Seal()
            .Seal()
            .List(1)
                .Add(0, init)
                .Add(1, init)
            .Seal()
            .Add(2, foldLambda)
        .Seal()
        .Build();

    return ctx.Builder(node->Pos())
        .Callable("Coalesce")
            .Callable(0, "Nth")
                .Callable(0, "ListLast")
                    .Callable(0, "ListTakeWhile")
                        .Callable(0, "ListExtend")
                            .Callable(0, "AsList")
                                .List(0)
                                    .Add(0, init)
                                    .Add(1, init)
                                .Seal()
                            .Seal()
                            .Add(1, foldMap)
                        .Seal()
                        .Lambda(1)
                            .Param("x")
                            .Callable("HasItems")
                                .Callable(0, "Nth")
                                    .Arg(0, "x")
                                    .Atom(1, 1)
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Atom(1, 0)
            .Seal()
            .Callable(1, "AsList")
            .Seal()
        .Seal()
        .Build();
}

} // namespace NYql
