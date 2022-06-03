#include "yql_co_pgselect.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NYql {

TNodeMap<ui32> GatherSubLinks(const TExprNode::TPtr& lambda) {
    TNodeMap<ui32> subLinks;

    VisitExpr(lambda->TailPtr(), [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgSubLink")) {
            subLinks[node.Get()] = subLinks.size();
            return false;
        }

        return true;
    });

    return subLinks;
}

TSet<TString> ExtractExternalColumns(const TExprNode& select) {
    TSet<TString> res;
    const auto& option = select.Head();
    auto setItems = GetSetting(option, "set_items");
    YQL_ENSURE(setItems);
    for (const auto& s : setItems->Tail().Children()) {
        YQL_ENSURE(s->IsCallable("PgSetItem"));
        auto extTypes = GetSetting(s->Head(), "final_ext_types");
        YQL_ENSURE(extTypes);
        for (const auto& input : extTypes->Tail().Children()) {
            auto type = input->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
            for (const auto& i : type->GetItems()) {
                res.insert(TString(i->GetName()));
            }
        }
    }

    return res;
}

TExprNode::TPtr JoinColumns(TPositionHandle pos, const TExprNode::TPtr& list1, const TExprNode::TPtr& list2,
    TExprNode::TPtr leftJoinColumns, TExprContext& ctx) {
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
                                parent.Add(2* i + 1, leftJoinColumns->ChildPtr(i));
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
                                parent.Atom(2 * i + 1, TString("_yql_join_sublink_") + leftJoinColumns->Child(i)->Content() );
                            }
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
        .Callable("Map")
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

std::pair<TExprNode::TPtr, TExprNode::TPtr> RewriteSubLinks(TPositionHandle pos,
    const TExprNode::TPtr& list, const TExprNode::TPtr& lambda,
    const TNodeMap<ui32>& subLinks, const TExprNode::TPtr& joins, TExprContext& ctx, TOptimizeContext& optCtx) {
    ui32 sublinkColumnIndex = 0;
    auto newList = list;
    auto originalRow = lambda->Head().HeadPtr();
    auto arg = ctx.NewArgument(pos, "row");
    auto arguments = ctx.NewArguments(pos, { arg });
    auto root = lambda->TailPtr();
    TNodeOnNodeOwnedMap deepClones;
    auto status = OptimizeExpr(root, root, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        auto it = subLinks.find(node.Get());
        if (it != subLinks.end()) {
            auto linkType = node->Head().Content();
            TSet<TString> extColumns = ExtractExternalColumns(node->Tail());
            if (extColumns.empty()) {
                auto select = ExpandPgSelectSublink(node->TailPtr(), ctx, optCtx, nullptr);
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
                } else if (linkType == "any" || linkType == "all") {
                    auto testArg = ctx.NewArgument(node->Pos(), "linkRow");
                    auto value = ctx.Builder(node->Pos())
                        .Callable("SingleMember")
                            .Add(0, testArg)
                        .Seal()
                        .Build();

                    auto testArgs = ctx.NewArguments(node->Pos(), { testArg });
                    auto testRoot = ctx.ReplaceNodes(node->Child(3)->TailPtr(), {
                        {node->Child(3)->Head().Child(0), originalRow},
                        {node->Child(3)->Head().Child(1), value},
                        });

                    if (linkType == "all") {
                        testRoot = ctx.Builder(node->Pos())
                            .Callable("PgNot")
                                .Add(0, testRoot)
                            .Seal()
                            .Build();
                    }

                    testRoot = ctx.Builder(node->Pos())
                        .Callable("Coalesce")
                            .Callable(0, "FromPg")
                                .Add(0, testRoot)
                            .Seal()
                            .Callable(1, "Bool")
                                .Atom(0, "0")
                            .Seal()
                        .Seal()
                        .Build();

                    auto testLambda = ctx.NewLambda(node->Pos(), std::move(testArgs), std::move(testRoot));

                    auto filtered = ctx.Builder(node->Pos())
                        .Callable("Filter")
                            .Add(0, select)
                            .Add(1, testLambda)
                        .Seal()
                        .Build();

                    auto take1 = ctx.Builder(node->Pos())
                        .Callable("Take")
                            .Add(0, filtered)
                            .Callable(1, "Uint64")
                                .Atom(0, "1")
                            .Seal()
                        .Seal()
                        .Build();

                    return ctx.Builder(node->Pos())
                        .Callable("ToPg")
                            .Callable(0, "==")
                                .Callable(0, "Length")
                                    .Add(0, take1)
                                .Seal()
                                .Callable(1, "Uint64")
                                    .Atom(0, (linkType == "any") ? "1" : "0")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                }
            } else {
                auto colList = ctx.Builder(node->Pos())
                    .List()
                        .Do([&](TExprNodeBuilder &parent) -> TExprNodeBuilder & {
                            ui32 i = 0;
                            for (const auto& c : extColumns) {
                                parent.Atom(i++, c);
                            }

                            return parent;
                        })
                    .Seal()
                    .Build();

                auto outerList = ctx.Builder(node->Pos())
                    .Callable("ExtractMembers")
                        .Add(0, joins)
                        .Add(1, colList)
                    .Seal()
                    .Build();

                auto uniqueOuterList = ctx.Builder(node->Pos())
                    .Callable("Aggregate")
                        .Add(0, outerList)
                        .Add(1, colList)
                        .List(2)
                        .Seal()
                    .Seal()
                    .Build();

                auto select = ExpandPgSelectSublink(node->TailPtr(), ctx, optCtx, uniqueOuterList);

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
                TExprNode::TPtr countIfTraits;
                for (ui32 factoryIndex = 0; factoryIndex < 3; ++factoryIndex)
                {
                    TStringBuf name;
                    switch (factoryIndex) {
                    case 0:
                        if (linkType != "exists" && linkType != "expr") {
                            continue;
                        }

                        name = "count_all_traits_factory";
                        break;
                    case 1:
                        if (linkType != "expr") {
                            continue;
                        }

                        name = "some_traits_factory";
                        break;
                    case 2:
                        if (linkType != "any" && linkType != "all") {
                            continue;
                        }

                        name = "count_if_traits_factory";
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
                    case 2: {
                        auto value = ctx.NewCallable(node->Pos(), "SingleMember", {
                            ctx.NewCallable(node->Pos(), "RemoveSystemMembers", { arg }) });

                        auto testRoot = ctx.ReplaceNodes(node->Child(3)->TailPtr(), {
                            {node->Child(3)->Head().Child(0), originalRow},
                            {node->Child(3)->Head().Child(1), value},
                            });

                        if (linkType == "all") {
                            testRoot = ctx.Builder(node->Pos())
                                .Callable("PgNot")
                                    .Add(0, testRoot)
                                .Seal()
                                .Build();
                        }

                        root = ctx.NewCallable(node->Pos(), "FromPg", { testRoot });
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
                        countIfTraits = traits;
                        break;
                    }
                }

                auto columnName = "_yql_sublink_" + ToString(sublinkColumnIndex++);
                TExprNode::TListType aggregateItems;
                if (linkType == "exists") {
                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName)
                            .Add(1, countAllTraits)
                        .Seal()
                        .Build());
                } else if (linkType == "expr") {
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
                } else if (linkType == "any" || linkType == "all") {
                    aggregateItems.push_back(ctx.Builder(node->Pos())
                        .List()
                            .Atom(0, columnName)
                            .Add(1, countIfTraits)
                        .Seal()
                        .Build());
                }

                auto aggregates = ctx.NewList(node->Pos(), std::move(aggregateItems));
                auto groupedSublink = ctx.Builder(node->Pos())
                    .Callable("Aggregate")
                        .Add(0, select)
                        .List(1)
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                                for (ui32 i = 0; i < colList->ChildrenSize(); ++i) {
                                    parent.Atom(i, TString("_yql_join_sublink_") + colList->Child(i)->Content());
                                }

                                return parent;
                            })
                        .Seal()
                        .Add(2, aggregates)
                    .Seal()
                    .Build();

                newList = JoinColumns(pos, newList, groupedSublink, colList, ctx);

                if (linkType == "exists") {
                    return ctx.Builder(node->Pos())
                        .Callable("ToPg")
                            .Callable(0, ">")
                                .Callable(0, "Coalesce")
                                    .Callable(0, "Member")
                                        .Add(0, originalRow)
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
                                .Add(0, originalRow)
                                .Atom(1, columnName + "_value")
                            .Seal()
                            .Callable(1, "<=")
                                .Callable(0, "Coalesce")
                                    .Callable(0, "Member")
                                        .Add(0, originalRow)
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
                } else if (linkType == "any") {
                    return ctx.Builder(node->Pos())
                        .Callable("ToPg")
                            .Callable(0, ">")
                                .Callable(0, "Member")
                                    .Add(0, originalRow)
                                    .Atom(1, columnName)
                                .Seal()
                                .Callable(1, "Uint64")
                                    .Atom(0, "0")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                } else if (linkType == "all") {
                    return ctx.Builder(node->Pos())
                        .Callable("ToPg")
                            .Callable(0, "==")
                                .Callable(0, "Coalesce")
                                    .Callable(0, "Member")
                                        .Add(0, originalRow)
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
                }
            }

            return node;
        }

        return node;
    }, ctx, TOptimizeExprSettings(optCtx.Types));

    root = ctx.ReplaceNode(std::move(root), *originalRow, arg);
    auto newLambda = ctx.NewLambda(pos, std::move(arguments), std::move(root));
    YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error);
    return {
        newList,
        newLambda
    };
}

TExprNode::TPtr BuildFilter(TPositionHandle pos, const TExprNode::TPtr& list, const TExprNode::TPtr& filter, TExprContext& ctx, TOptimizeContext& optCtx) {
    TExprNode::TPtr actualList = list, actualFilter = filter;
    auto subLinks = GatherSubLinks(filter);
    if (!subLinks.empty()) {
        std::tie(actualList, actualFilter) = RewriteSubLinks(filter->Pos(), list, filter, subLinks, list, ctx, optCtx);
    }

    return ctx.Builder(pos)
        .Callable("Filter")
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

TExprNode::TPtr ExpandPositionalUnionAll(const TExprNode& node, const TVector<TColumnOrder>& columnOrders,
    TExprNode::TListType children, TExprContext& ctx, TOptimizeContext& optCtx) {
    auto targetColumnOrder = optCtx.Types->LookupColumnOrder(node);
    YQL_ENSURE(targetColumnOrder);

    for (ui32 childIndex = 0; childIndex < children.size(); ++childIndex) {
        const auto& childColumnOrder = columnOrders[childIndex];
        auto& child = children[childIndex];
        if (childColumnOrder == *targetColumnOrder) {
            continue;
        }

        YQL_ENSURE(childColumnOrder.size() == targetColumnOrder->size());
        child = ctx.Builder(child->Pos())
            .Callable("Map")
                .Add(0, child)
                .Lambda(1)
                    .Param("row")
                    .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder &parent) -> TExprNodeBuilder & {
                        for (size_t i = 0; i < childColumnOrder.size(); ++i) {
                            parent
                                .List(i)
                                    .Atom(0, child->Pos(), (*targetColumnOrder)[i])
                                    .Callable(1, "Member")
                                        .Arg(0, "row")
                                        .Atom(1, childColumnOrder[i])
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

    auto res = ctx.NewCallable(node.Pos(), "UnionAll", std::move(children));
    return KeepColumnOrder(res, node, ctx, *optCtx.Types);
}

TExprNode::TPtr BuildValues(TPositionHandle pos, const TExprNode::TPtr& values, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Map")
            .Add(0, values->ChildPtr(2))
            .Lambda(1)
                .Param("row")
                .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 index = 0; index < values->Child(1)->ChildrenSize(); ++index) {
                        parent
                            .List(index)
                                .Atom(0, values->Child(1)->Child(index)->Content())
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

std::tuple<TExprNode::TPtr, TExprNode::TPtr> BuildOneRow(TPositionHandle pos, const TExprNode::TPtr& result, TExprContext& ctx) {
    auto arg = ctx.NewArgument(pos, "row");
    auto arguments = ctx.NewArguments(pos, { arg });

    TExprNode::TListType rowItems;
    for (const auto& x : result->Tail().Children()) {
        auto value = ctx.ReplaceNode(x->Tail().TailPtr(), x->Tail().Head().Head(), arg);
        rowItems.push_back(ctx.NewList(x->Pos(), { x->HeadPtr(), value }));
    }

    auto row = ctx.NewCallable(pos, "AsStruct", std::move(rowItems));
    auto projectionLambda = ctx.NewLambda(pos, std::move(arguments), std::move(row));

    auto list = ctx.Builder(pos)
        .Callable("AsList")
            .Callable(0, "AsStruct")
            .Seal()
        .Seal()
        .Build();

    return { list, projectionLambda };
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
        auto extColumns = ExtractExternalColumns(s.first->Tail());
        for (const auto& c : extColumns) {
            columns.insert(std::make_pair(c, std::make_pair(Max<ui32>(), TString())));
        }

        if (!s.first->Child(2)->IsCallable("Void")) {
            AddColumnsFromType(s.first->Child(2)->GetTypeAnn(), columns);
        }
    }
}

TUsedColumns GatherUsedColumns(const TExprNode::TPtr& result, const TExprNode::TPtr& joinOps,
    const TExprNode::TPtr& filter, const TExprNode::TPtr& having) {
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

    if (having) {
        AddColumnsFromType(having->Tail().Head().GetTypeAnn(), usedColumns);
        auto subLinks = GatherSubLinks(having->Tail().TailPtr());
        AddColumnsFromSublinks(subLinks, usedColumns);
    }

    return usedColumns;
}

void FillInputIndices(const TExprNode::TPtr& from, const TExprNode::TPtr& finalExtTypes,
    TUsedColumns& usedColumns, TOptimizeContext& optCtx) {
    for (auto& x : usedColumns) {
        bool foundColumn = false;
        ui32 inputIndex = 0;
        for (; inputIndex < from->Tail().ChildrenSize(); ++inputIndex) {
            const auto& read = from->Tail().Child(inputIndex)->Head();
            const auto& columns = from->Tail().Child(inputIndex)->Tail();
            if (read.IsCallable("PgResolvedCall")) {
                const auto& alias = from->Tail().Child(inputIndex)->Child(1)->Content();
                Y_ENSURE(!alias.empty());
                Y_ENSURE(columns.ChildrenSize() == 0 || columns.ChildrenSize() == 1);
                auto memberName = (columns.ChildrenSize() == 1) ? columns.Head().Content() : alias;
                foundColumn = (memberName == x.first);
            } else {
                if (columns.ChildrenSize() > 0) {
                    auto readOrder = optCtx.Types->LookupColumnOrder(read);
                    YQL_ENSURE(readOrder);
                    for (ui32 i = 0; i < columns.ChildrenSize(); ++i) {
                        if (columns.Child(i)->Content() == x.first) {
                            foundColumn = true;
                            x.second.second = (*readOrder)[i];
                            break;
                        }
                    }
                } else {
                    auto type = read.GetTypeAnn()->
                        Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                    auto pos = type->FindItem(x.first);
                    foundColumn = pos.Defined();
                }
            }

            if (foundColumn) {
                x.second.first = inputIndex;
                break;
            }
        }

        if (!foundColumn && finalExtTypes) {
            for (const auto& input : finalExtTypes->Tail().Children()) {
                auto type = input->Tail().GetTypeAnn()->
                    Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                auto pos = type->FindItem(x.first);
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

TExprNode::TListType BuildCleanedColumns(TPositionHandle pos, const TExprNode::TPtr& from, const TUsedColumns& usedColumns, TExprContext& ctx) {
    TExprNode::TListType cleanedInputs;
    for (ui32 i = 0; i < from->Tail().ChildrenSize(); ++i) {
        auto list = from->Tail().Child(i)->HeadPtr();
        if (list->IsCallable("PgResolvedCall")) {
            const auto& alias = from->Tail().Child(i)->Child(1)->Content();
            const auto& columns = from->Tail().Child(i)->Tail();
            Y_ENSURE(!alias.empty());
            Y_ENSURE(columns.ChildrenSize() == 0 || columns.ChildrenSize() == 1);
            auto memberName = (columns.ChildrenSize() == 1) ? columns.Head().Content() : alias;
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

        auto cleaned = ctx.Builder(pos)
            .Callable("Map")
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

                                auto listBuilder = parent.List(index++);
                                listBuilder.Atom(0, x.first);
                                listBuilder.Callable(1, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, x.second.second ? x.second.second : x.first)
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

std::tuple<TVector<ui32>, TExprNode::TListType> BuildJoinGroups(TPositionHandle pos, const TExprNode::TListType& cleanedInputs,
    const TExprNode::TPtr& joinOps, TExprContext& ctx, TOptimizeContext& optCtx) {
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
            auto cartesian = ctx.Builder(pos)
                    .Callable("FlatMap")
                        .Add(0, current)
                        .Lambda(1)
                            .Param("x")
                            .Callable("Map")
                                .Add(0, with)
                                .Lambda(1)
                                    .Param("y")
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
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                    .Build();

            auto buildMinus = [&](auto left, auto right) {
                return ctx.Builder(pos)
                    .Callable("Filter")
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
                                                    .Apply(0, join->Tail().TailPtr())
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
            };

            TExprNode::TPtr filteredCartesian;
            if (joinType != "cross") {
                filteredCartesian = BuildFilter(pos, cartesian, join->Tail().TailPtr(), ctx, optCtx);
            }

            if (joinType == "cross") {
                current = cartesian;
            } else if (joinType == "inner") {
                current = filteredCartesian;
            } else if (joinType == "left") {
                current = ctx.Builder(pos)
                    .Callable("UnionAll")
                        .Add(0, filteredCartesian)
                        .Add(1, buildMinus(current, with))
                    .Seal()
                    .Build();
            } else if (joinType == "right") {
                current = ctx.Builder(pos)
                    .Callable("UnionAll")
                        .Add(0, filteredCartesian)
                        .Add(1, buildMinus(with, current))
                    .Seal()
                    .Build();
            } else {
                YQL_ENSURE(joinType == "full");
                current = ctx.Builder(pos)
                    .Callable("UnionAll")
                        .Add(0, filteredCartesian)
                        .Add(1, buildMinus(current, with))
                        .Add(2, buildMinus(with, current))
                    .Seal()
                    .Build();
            }
        }

        joinGroups.push_back(current);
    }

    return { groupForIndex, joinGroups };
}

TExprNode::TPtr BuildCrossJoinsBetweenGroups(TPositionHandle pos, const TExprNode::TListType& joinGroups,
    const TUsedColumns& usedColumns, const TVector<ui32>& groupForIndex, TExprContext& ctx) {
    TExprNode::TListType args;
    for (ui32 i = 0; i < joinGroups.size(); ++i) {
        args.push_back(ctx.Builder(pos)
            .List()
                .Add(0, joinGroups[i])
                .Atom(1, ToString(i))
            .Seal()
            .Build());
    }

    auto tree = ctx.Builder(pos)
        .List()
            .Atom(0, "Cross")
            .Atom(1, "0")
            .Atom(2, "1")
            .List(3)
            .Seal()
            .List(4)
            .Seal()
            .List(5)
            .Seal()
        .Seal()
        .Build();

    for (ui32 i = 2; i < joinGroups.size(); ++i) {
        tree = ctx.Builder(pos)
            .List()
                .Atom(0, "Cross")
                .Add(1, tree)
                .Atom(2, ToString(i))
                .List(3)
                .Seal()
                .List(4)
                .Seal()
                .List(5)
                .Seal()
            .Seal()
            .Build();
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

    auto settingsNode = ctx.NewList(pos, std::move(settings));
    args.push_back(settingsNode);
    return ctx.NewCallable(pos, "EquiJoin", std::move(args));
}

TExprNode::TPtr BuildProjectionLambda(TPositionHandle pos, const TExprNode::TPtr& result, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Callable("AsStruct")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                ui32 index = 0;
                for (const auto& x : result->Tail().Children()) {
                    if (x->HeadPtr()->IsAtom()) {
                        auto listBuilder = parent.List(index++);
                        listBuilder.Add(0, x->HeadPtr());
                        listBuilder.Apply(1, x->TailPtr())
                            .With(0, "row")
                        .Seal();
                        listBuilder.Seal();
                    } else {
                        for (ui32 i = 0; i < x->Head().ChildrenSize(); ++i) {
                            auto listBuilder = parent.List(index++);
                            listBuilder.Add(0, x->Head().ChildPtr(i));
                            listBuilder.Callable(1, "Member")
                                .Arg(0, "row")
                                .Add(1, x->Head().ChildPtr(i));
                            listBuilder.Seal();
                        }
                    }
                }

                return parent;
            })
            .Seal()
        .Seal()
        .Build();
}

using TAggs = TVector<std::pair<TExprNode::TPtr, TExprNode::TPtr>>;

std::tuple<TAggs, TNodeMap<ui32>> GatherAggregations(const TExprNode::TPtr& projectionLambda, const TExprNode::TPtr& having) {
    TAggs aggs;
    TNodeMap<ui32> aggId;

    VisitExpr(projectionLambda->TailPtr(), [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgSubLink")) {
            return false;
        }

        if (node->IsCallable("PgAgg")) {
            aggId[node.Get()] = aggs.size();
            aggs.push_back({ node, projectionLambda->Head().HeadPtr() });
        }

        return true;
    });

    if (having) {
        auto havingLambda = having->Tail().TailPtr();
        VisitExpr(having->Tail().TailPtr(), [&](const TExprNode::TPtr& node) {
            if (node->IsCallable("PgSubLink")) {
                return false;
            }

            if (node->IsCallable("PgAgg")) {
                aggId[node.Get()] = aggs.size();
                aggs.push_back({ node, havingLambda->Head().HeadPtr() });
            }

            return true;
        });
    }

    return { aggs, aggId };
}

TExprNode::TPtr BuildAggregationTraits(TPositionHandle pos, bool onWindow,
    const std::pair<TExprNode::TPtr, TExprNode::TPtr>& agg,
    const TExprNode::TPtr& listTypeNode, TExprContext& ctx) {
    auto arg = ctx.NewArgument(pos, "row");
    auto arguments = ctx.NewArguments(pos, { arg });
    auto func = agg.first->Head().Content();
    TExprNode::TListType aggFuncArgs;
    for (ui32 j = onWindow ? 3 : 2; j < agg.first->ChildrenSize(); ++j) {
        aggFuncArgs.push_back(ctx.ReplaceNode(agg.first->ChildPtr(j), *agg.second, arg));
    }

    auto extractor = ctx.NewLambda(pos, std::move(arguments), std::move(aggFuncArgs));

    return ctx.Builder(pos)
        .Callable(onWindow ? "PgWindowTraits" : "PgAggregationTraits")
            .Atom(0, func)
            .Callable(1, "ListItemType")
                .Add(0, listTypeNode)
            .Seal()
            .Add(2, extractor)
        .Seal()
        .Build();
}

TExprNode::TPtr BuildGroupByAndHaving(TPositionHandle pos, const TExprNode::TPtr& list, const TAggs& aggs, const TNodeMap<ui32>& aggId,
    const TExprNode::TPtr& groupBy, const TExprNode::TPtr& having, TExprNode::TPtr& projectionLambda,
    const TExprNode::TPtr& finalExtTypes, TExprContext& ctx, TOptimizeContext& optCtx) {
    auto listTypeNode = ctx.Builder(pos)
        .Callable("TypeOf")
            .Add(0, list)
        .Seal()
        .Build();

    TExprNode::TListType payloadItems;
    for (ui32 i = 0; i < aggs.size(); ++i) {
        auto traits = BuildAggregationTraits(pos, false, aggs[i], listTypeNode, ctx);
        payloadItems.push_back(ctx.Builder(pos)
            .List()
                .Atom(0, "_yql_agg_" + ToString(i))
                .Add(1, traits)
            .Seal()
            .Build());
    }

    auto payloadsNode = ctx.NewList(pos, std::move(payloadItems));
    TExprNode::TListType keysItems;
    if (finalExtTypes) {
        for (const auto& x : finalExtTypes->Tail().Children()) {
            auto type = x->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
            for (const auto& item : type->GetItems()) {
                keysItems.push_back(ctx.NewAtom(pos, item->GetName()));
            }
        }
    }

    if (groupBy) {
        for (const auto& group : groupBy->Tail().Children()) {
            const auto& lambda = group->Tail();
            YQL_ENSURE(lambda.IsLambda());
            YQL_ENSURE(lambda.Tail().IsCallable("Member"));
            keysItems.push_back(lambda.Tail().TailPtr());
        }
    }

    auto keys = ctx.NewList(pos, std::move(keysItems));

    auto ret = ctx.Builder(pos)
        .Callable("Aggregate")
            .Add(0, list)
            .Add(1, keys)
            .Add(2, payloadsNode)
            .List(3) // options
            .Seal()
        .Seal()
        .Build();

    auto rewriteAggs = [&](auto& lambda) {
        auto status = OptimizeExpr(lambda, lambda, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            auto it = aggId.find(node.Get());
            if (it != aggId.end()) {
                auto ret = ctx.Builder(pos)
                    .Callable("Member")
                        .Add(0, lambda->Head().HeadPtr())
                        .Atom(1, "_yql_agg_" + ToString(it->second))
                    .Seal()
                    .Build();

                return ret;
            }

            return node;
        }, ctx, TOptimizeExprSettings(optCtx.Types));

        return status.Level != IGraphTransformer::TStatus::Error;
    };

    if (!rewriteAggs(projectionLambda)) {
        return {};
    }

    if (having) {
        auto havingLambda = having->Tail().TailPtr();
        if (!rewriteAggs(havingLambda)) {
            return {};
        }

        ret = ctx.Builder(pos)
            .Callable("Filter")
                .Add(0, ret)
                .Lambda(1)
                    .Param("row")
                    .Callable("Coalesce")
                        .Callable(0, "FromPg")
                            .Apply(0, havingLambda)
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

    return ret;
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

TExprNode::TPtr BuildSortTraits(TPositionHandle pos, const TExprNode& sortColumns, const TExprNode::TPtr& list, TExprContext& ctx) {
    if (sortColumns.ChildrenSize() == 1) {
        return ctx.Builder(pos)
            .Callable("SortTraits")
                .Callable(0, "TypeOf")
                    .Add(0, list)
                .Seal()
                .Callable(1, "Bool")
                    .Atom(0, sortColumns.Head().Tail().Content() == "asc" ? "true" : "false")
                .Seal()
                .Lambda(2)
                    .Param("row")
                    .Apply(sortColumns.Head().ChildPtr(1))
                        .With(0, "row")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    } else {
        return ctx.Builder(pos)
            .Callable("SortTraits")
                .Callable(0, "TypeOf")
                    .Add(0, list)
                .Seal()
                .List(1)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0; i < sortColumns.ChildrenSize(); ++i) {
                            parent.Callable(i, "Bool")
                                .Atom(0, sortColumns.Child(i)->Tail().Content() == "asc" ? "true" : "false")
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
                                parent.Apply(i, sortColumns.Child(i)->ChildPtr(1))
                                    .With(0, "row")
                                    .Seal();
                            }

                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }
}

TExprNode::TPtr BuildWindows(TPositionHandle pos, const TExprNode::TPtr& list, const TExprNode::TPtr& window,
    TExprNode::TPtr& projectionLambda, TExprContext& ctx, TOptimizeContext& optCtx) {
    TVector<std::pair<TExprNode::TPtr, TExprNode::TPtr>> winFuncs;
    TMap<ui32, TVector<ui32>> window2funcs;
    TNodeMap<ui32> winFuncsId;
    auto ret = list;
    VisitExpr(projectionLambda->TailPtr(), [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("PgWindowCall") || node->IsCallable("PgAggWindowCall")) {
            YQL_ENSURE(window);
            ui32 windowIndex;
            if (node->Child(1)->IsCallable("PgAnonWindow")) {
                windowIndex = FromString<ui32>(node->Child(1)->Head().Content());
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

            window2funcs[windowIndex].push_back(winFuncs.size());
            winFuncsId[node.Get()] = winFuncs.size();
            winFuncs.push_back({ node, projectionLambda->Head().HeadPtr() });
        }

        return true;
    });

    if (!winFuncs.empty()) {
        auto listTypeNode = ctx.Builder(pos)
            .Callable("TypeOf")
                .Add(0, list)
            .Seal()
            .Build();

        for (const auto& x : window2funcs) {
            auto win = window->Tail().Child(x.first);
            const auto& frameSettings = win->Tail();

            TExprNode::TListType args;
            // default frame
            auto begin = ctx.NewCallable(pos, "Void", {});
            auto end = win->Child(3)->ChildrenSize() > 0 ?
                ctx.NewCallable(pos, "Int32", { ctx.NewAtom(pos, "0") }) :
                ctx.NewCallable(pos, "Void", {});
            if (HasSetting(frameSettings, "type")) {
                std::tie(begin, end) = BuildFrame(pos, frameSettings, ctx);
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
                auto p = winFuncs[index];
                auto name = p.first->Head().Content();
                bool isAgg = p.first->IsCallable("PgAggWindowCall");
                TExprNode::TPtr value;
                if (isAgg) {
                    value = BuildAggregationTraits(pos, true, p, listTypeNode, ctx);
                } else {
                    if (name == "row_number") {
                        value = ctx.Builder(pos)
                            .Callable("RowNumber")
                                .Callable(0, "TypeOf")
                                    .Add(0, list)
                                .Seal()
                            .Seal()
                            .Build();
                    } else if (name == "lead" || name == "lag") {
                        auto arg = ctx.NewArgument(pos, "row");
                        auto arguments = ctx.NewArguments(pos, { arg });
                        auto extractor = ctx.NewLambda(pos, std::move(arguments),
                            ctx.ReplaceNode(p.first->TailPtr(), *p.second, arg));

                        value = ctx.Builder(pos)
                            .Callable(name == "lead" ? "Lead" : "Lag")
                                .Callable(0, "TypeOf")
                                    .Add(0, list)
                                .Seal()
                                .Add(1, extractor)
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

            auto winOnRows = ctx.NewCallable(pos, "WinOnRows", std::move(args));

            auto frames = ctx.Builder(pos)
                .List()
                    .Add(0, winOnRows)
                .Seal()
                .Build();

            TExprNode::TListType keys;
            for (auto p : win->Child(2)->Children()) {
                YQL_ENSURE(p->IsCallable("PgGroup"));
                const auto& member = p->Tail().Tail();
                YQL_ENSURE(member.IsCallable("Member"));
                keys.push_back(member.TailPtr());
            }

            auto keysNode = ctx.NewList(pos, std::move(keys));
            auto sortNode = ctx.NewCallable(pos, "Void", {});
            if (win->Child(3)->ChildrenSize() > 0) {
                sortNode = BuildSortTraits(pos, *win->Child(3), ret, ctx);
            }

            ret = ctx.Builder(pos)
                .Callable("CalcOverWindow")
                    .Add(0, ret)
                    .Add(1, keysNode)
                    .Add(2, sortNode)
                    .Add(3, frames)
                .Seal()
                .Build();
        }

        auto status = OptimizeExpr(projectionLambda, projectionLambda, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            auto it = winFuncsId.find(node.Get());
            if (it != winFuncsId.end()) {
                auto ret = ctx.Builder(pos)
                    .Callable("Member")
                        .Add(0, projectionLambda->Head().HeadPtr())
                        .Atom(1, "_yql_win_" + ToString(it->second))
                    .Seal()
                    .Build();

                if (node->Head().Content() == "row_number") {
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

        if (status.Level == IGraphTransformer::TStatus::Error) {
            return nullptr;
        }
    }

    return ret;
}

TExprNode::TPtr BuildSort(TPositionHandle pos, const TExprNode::TPtr& sort, const TExprNode::TPtr& list, TExprContext& ctx) {
    const auto& keys = sort->Tail();
    auto argNode = ctx.NewArgument(pos, "row");
    auto argsNode = ctx.NewArguments(pos, { argNode });

    TExprNode::TListType dirItems;
    TExprNode::TListType rootItems;
    for (const auto& key : keys.Children()) {
        dirItems.push_back(ctx.Builder(pos)
            .Callable("Bool")
                .Atom(0, key->Tail().Content() == "asc" ? "true" : "false")
            .Seal()
            .Build());

        auto keyLambda = key->ChildPtr(1);
        rootItems.push_back(ctx.ReplaceNode(keyLambda->TailPtr(), keyLambda->Head().Head(), argNode));
    }

    auto root = ctx.NewList(pos, std::move(rootItems));
    auto dir = ctx.NewList(pos, std::move(dirItems));
    auto lambda = ctx.NewLambda(pos, std::move(argsNode), std::move(root));

    return ctx.Builder(pos)
        .Callable("Sort")
            .Add(0, list)
            .Add(1, dir)
            .Add(2, lambda)
        .Seal()
        .Build();
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

TExprNode::TPtr AddExtColumns(const TExprNode::TPtr& lambda, const TExprNode::TPtr& finalExtTypes,
    TExprNode::TListType& columns, TExprContext& ctx) {
    return ctx.Builder(lambda->Pos())
        .Lambda()
            .Param("row")
            .Callable("FlattenMembers")
                .List(0)
                    .Atom(0, "")
                    .Apply(1, lambda)
                        .With(0, "row")
                    .Seal()
                .Seal()
                .List(1)
                    .Atom(0, "_yql_join_sublink_")
                    .Callable(1, "SelectMembers")
                        .Arg(0, "row")
                        .List(1)
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder & {
                                ui32 i = 0;
                                for (const auto& x : finalExtTypes->Children()) {
                                    auto type = x->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                                    for (const auto& item : type->GetItems()) {
                                        parent.Atom(i++, item->GetName());
                                        columns.push_back(ctx.NewAtom(lambda->Pos(), TString("_yql_join_sublink_") + item->GetName()));
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

TExprNode::TPtr ExpandPgSelectImpl(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx, bool subLink, const TExprNode::TPtr& outer) {
    auto order = optCtx.Types->LookupColumnOrder(*node);
    YQL_ENSURE(order);
    TExprNode::TListType columnsItems;
    for (const auto& x : *order) {
        columnsItems.push_back(ctx.NewAtom(node->Pos(), x));
    }

    auto setItems = GetSetting(node->Head(), "set_items");
    TExprNode::TListType setItemNodes;
    TVector<TColumnOrder> columnOrders;
    for (auto setItem : setItems->Tail().Children()) {
        auto childOrder = optCtx.Types->LookupColumnOrder(*setItem);
        YQL_ENSURE(*childOrder);
        columnOrders.push_back(*childOrder);
        auto finalExtTypes = GetSetting(setItem->Tail(), "final_ext_types");
        if (finalExtTypes && !subLink) {
            return node;
        }

        auto result = GetSetting(setItem->Tail(), "result");
        auto values = GetSetting(setItem->Tail(), "values");
        auto from = GetSetting(setItem->Tail(), "from");
        auto filter = GetSetting(setItem->Tail(), "where");
        auto joinOps = GetSetting(setItem->Tail(), "join_ops");
        auto groupBy = GetSetting(setItem->Tail(), "group_by");
        auto having = GetSetting(setItem->Tail(), "having");
        auto window = GetSetting(setItem->Tail(), "window");
        bool oneRow = !from;
        TExprNode::TPtr list;
        if (values) {
            YQL_ENSURE(!result);
            list = BuildValues(node->Pos(), values, ctx);
        } else {
            YQL_ENSURE(result);
            TExprNode::TPtr projectionLambda;
            if (oneRow) {
                std::tie(list, projectionLambda) = BuildOneRow(node->Pos(), result, ctx);
            } else {
                // extract all used columns
                auto usedColumns = GatherUsedColumns(result, joinOps, filter, having);

                // fill index of input for each column
                FillInputIndices(from, finalExtTypes, usedColumns, optCtx);

                auto cleanedInputs = BuildCleanedColumns(node->Pos(), from, usedColumns, ctx);
                if (cleanedInputs.size() == 1) {
                    list = cleanedInputs.front();
                } else {
                    TVector<ui32> groupForIndex;
                    TExprNode::TListType joinGroups;
                    std::tie(groupForIndex, joinGroups) = BuildJoinGroups(node->Pos(), cleanedInputs, joinOps, ctx, optCtx);
                    if (joinGroups.size() == 1) {
                        list = joinGroups.front();
                    } else {
                        list = BuildCrossJoinsBetweenGroups(node->Pos(), joinGroups, usedColumns, groupForIndex, ctx);
                    }
                }

                projectionLambda = BuildProjectionLambda(node->Pos(), result, ctx);
            }

            if (outer) {
                list = JoinColumns(node->Pos(), list, outer, nullptr, ctx);
            }

            auto joins = list;
            if (filter) {
                list = BuildFilter(node->Pos(), list, filter->Tail().TailPtr(), ctx, optCtx);
            }

            TAggs aggs;
            TNodeMap<ui32> aggId;
            std::tie(aggs, aggId) = GatherAggregations(projectionLambda, having);
            if (!aggs.empty() || groupBy) {
                list = BuildGroupByAndHaving(node->Pos(), list, aggs, aggId, groupBy, having, projectionLambda,
                    finalExtTypes, ctx, optCtx);
            }

            list = BuildWindows(node->Pos(), list, window, projectionLambda, ctx, optCtx);
            auto projectionSubLinks = GatherSubLinks(projectionLambda);
            if (!projectionSubLinks.empty()) {
                std::tie(list, projectionLambda) = RewriteSubLinks(projectionLambda->Pos(), list, projectionLambda,
                    projectionSubLinks, joins, ctx, optCtx);
            }

            if (finalExtTypes) {
                projectionLambda = AddExtColumns(projectionLambda, finalExtTypes->TailPtr(), columnsItems, ctx);
            }

            list = ctx.Builder(node->Pos())
                .Callable("Map")
                    .Add(0, list)
                    .Add(1, projectionLambda)
                .Seal()
                .Build();
        }

        setItemNodes.push_back(list);
    }

    TExprNode::TPtr list;
    if (setItemNodes.size() == 1) {
        list = setItemNodes.front();
    } else {
        list = ExpandPositionalUnionAll(*node, columnOrders, setItemNodes, ctx, optCtx);
    }

    auto sort = GetSetting(node->Head(), "sort");
    if (sort && sort->Tail().ChildrenSize() > 0) {
        list = BuildSort(node->Pos(), sort, list, ctx);
    }

    auto limit = GetSetting(node->Head(), "limit");
    auto offset = GetSetting(node->Head(), "offset");

    if (offset) {
        list = BuildOffset(node->Pos(), offset, list, ctx);
    }

    if (limit) {
        list = BuildLimit(node->Pos(), limit, list, ctx);
    }

    auto columns = ctx.NewList(node->Pos(), std::move(columnsItems));
    return ctx.Builder(node->Pos())
        .Callable("AssumeColumnOrder")
            .Add(0, list)
            .Add(1, columns)
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPgSelect(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    return ExpandPgSelectImpl(node, ctx, optCtx, false, nullptr);
}

TExprNode::TPtr ExpandPgSelectSublink(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx, const TExprNode::TPtr& outer) {
    return ExpandPgSelectImpl(node, ctx, optCtx, true, outer);
}

TExprNode::TPtr ExpandPgLike(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    Y_UNUSED(optCtx);
    const bool insensitive = node->IsCallable("PgILike");
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
        .Callable("Fold")
            .Add(0, node->ChildPtr(1))
            .Callable(1, "PgConst")
                .Atom(0, "false")
                .Callable(1, "PgType")
                    .Atom(0, "bool")
                .Seal()
            .Seal()
            .Lambda(2)
                .Param("item")
                .Param("state")
                .Callable("PgOr")
                    .Arg(0, "state")
                    .Callable(1, "PgOp")
                        .Atom(0, "=")
                        .Arg(1, "item")
                        .Add(2, node->ChildPtr(0))
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

} // namespace NYql
