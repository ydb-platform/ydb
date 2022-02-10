#include "yql_opt_aggregate.h"
#include "yql_opt_utils.h"
#include "yql_opt_window.h"
#include "yql_expr_type_annotation.h"

namespace NYql {

TExprNode::TPtr ExpandAggregate(const TExprNode::TPtr& node, TExprContext& ctx, bool forceCompact) { 
    auto list = node->HeadPtr();
    auto keyColumns = node->ChildPtr(1);
    auto aggregatedColumns = node->Child(2);
    auto settings = node->Child(3);

    YQL_ENSURE(!HasSetting(*settings, "hopping"), "Aggregate with hopping unsupported here.");

    static const TStringBuf sessionStartMemberName = "_yql_group_session_start";
    const TExprNode::TPtr voidNode = ctx.NewCallable(node->Pos(), "Void", {});

    TExprNode::TPtr sessionKey;
    const TTypeAnnotationNode* sessionKeyType = nullptr;
    const TTypeAnnotationNode* sessionParamsType = nullptr;
    TExprNode::TPtr sessionInit;
    TExprNode::TPtr sessionUpdate;

    TExprNode::TPtr sortKey = voidNode;
    TExprNode::TPtr sortOrder = voidNode;

    bool effectiveCompact = forceCompact || HasSetting(*settings, "compact"); 

    const TStructExprType* originalRowType = GetSeqItemType(node->Head().GetTypeAnn())->Cast<TStructExprType>();
    TVector<const TItemExprType*> rowItems = originalRowType->GetItems();

    const auto sessionSetting = GetSetting(*settings, "session");
    TMaybe<TStringBuf> sessionOutputColumn;
    if (sessionSetting) {
        YQL_ENSURE(sessionSetting->Child(1)->Child(0)->IsAtom());
        sessionOutputColumn = sessionSetting->Child(1)->Child(0)->Content();

        // remove session column from other keys
        TExprNodeList keyColumnsList = keyColumns->ChildrenList();
        EraseIf(keyColumnsList, [&](const auto& key) { return sessionOutputColumn == key->Content(); });
        keyColumns = ctx.NewList(keyColumns->Pos(), std::move(keyColumnsList));

        const bool haveDistinct = AnyOf(aggregatedColumns->ChildrenList(),
            [](const auto& child) { return child->ChildrenSize() == 3; });

        TExprNode::TPtr sessionSortTraits;
        ExtractSessionWindowParams(node->Pos(), sessionSetting->Child(1)->ChildPtr(1), sessionKey, sessionKeyType, sessionParamsType, sessionSortTraits,
            sessionInit, sessionUpdate, ctx);
        ExtractSortKeyAndOrder(node->Pos(), sessionSortTraits, sortKey, sortOrder, ctx);

        if (haveDistinct) {
            auto keySelector = BuildKeySelector(node->Pos(), *originalRowType, keyColumns, ctx);
            const auto sessionStartMemberLambda = AddSessionParamsMemberLambda(node->Pos(), sessionStartMemberName, "", keySelector,
                sessionKey, sessionInit, sessionUpdate, ctx);

            list = ctx.Builder(node->Pos())
                .Callable("PartitionsByKeys")
                    .Add(0, list)
                    .Add(1, keySelector)
                    .Add(2, sortOrder)
                    .Add(3, sortKey)
                    .Lambda(4)
                        .Param("partitionedStream")
                        .Apply(sessionStartMemberLambda)
                            .With(0, "partitionedStream")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();

            auto keyColumnsList = keyColumns->ChildrenList();
            keyColumnsList.push_back(ctx.NewAtom(node->Pos(), sessionStartMemberName));
            keyColumns = ctx.NewList(node->Pos(), std::move(keyColumnsList));

            rowItems.push_back(ctx.MakeType<TItemExprType>(sessionStartMemberName, sessionKeyType));

            sortOrder = sortKey = voidNode;
            sessionKey = sessionInit = sessionUpdate = {};
            sessionKeyType = nullptr;
        } else {
            effectiveCompact = true;
        }
    }

    const bool compact = effectiveCompact;
    const auto rowType = ctx.MakeType<TStructExprType>(rowItems);

    auto preMap = ctx.Builder(node->Pos())
        .Lambda()
            .Param("premap")
            .Callable("Just").Arg(0, "premap").Seal()
        .Seal().Build();

    bool needPickle = false;
    TVector<const TTypeAnnotationNode*> keyItemTypes;
    for (auto keyColumn : keyColumns->Children()) {
        auto index = rowType->FindItem(keyColumn->Content());
        YQL_ENSURE(index, "Unknown column: " << keyColumn->Content());
        auto type = rowType->GetItems()[*index]->GetItemType();
        keyItemTypes.push_back(type);
        needPickle = needPickle || !IsDataOrOptionalOfData(type);
    }

    const TTypeAnnotationNode* pickleType = nullptr;
    TExprNode::TPtr pickleTypeNode;
    if (needPickle) {
        pickleType = keyColumns->ChildrenSize() > 1 ? ctx.MakeType<TTupleExprType>(keyItemTypes) : keyItemTypes[0];
        pickleTypeNode = ExpandType(node->Pos(), *pickleType, ctx);
    }

    auto keyExtractor = ctx.Builder(node->Pos())
        .Lambda()
            .Param("item")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                if (keyColumns->ChildrenSize() == 0) {
                    return parent.Callable("Uint32").Atom(0, "0", TNodeFlags::Default).Seal();
                }
                else if (keyColumns->ChildrenSize() == 1) {
                    return parent.Callable("Member").Arg(0, "item").Add(1, keyColumns->HeadPtr()).Seal();
                }
                else {
                    auto listBuilder = parent.List();
                    ui32 pos = 0;
                    for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                        listBuilder
                            .Callable(pos++, "Member")
                                .Arg(0, "item")
                                .Add(1, keyColumns->ChildPtr(i))
                            .Seal();
                    }

                    return listBuilder.Seal();
                }
            })
        .Seal()
        .Build();

    if (needPickle) {
        keyExtractor = ctx.Builder(node->Pos())
            .Lambda()
                .Param("item")
                .Callable("StablePickle")
                    .Apply(0, *keyExtractor)
                        .With(0, "item")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    TExprNode::TListType initialColumnNames;
    TExprNode::TListType finalColumnNames;
    TExprNode::TListType distinctFields;
    using TIdxSet = std::set<ui32>;
    std::unordered_map<std::string_view, TIdxSet> distinct2Columns;
    std::unordered_map<std::string_view, bool> distinctFieldNeedsPickle;
    std::unordered_map<std::string_view, TExprNode::TPtr> udfSetCreate;
    std::unordered_map<std::string_view, TExprNode::TPtr> udfAddValue;
    std::unordered_map<std::string_view, TExprNode::TPtr> udfWasChanged;
    TIdxSet nondistinctColumns;
    for (auto child : aggregatedColumns->Children()) {
        if (const auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr) {
            const auto ins = distinct2Columns.emplace(distinctField->Content(), TIdxSet());
            if (ins.second) {
                distinctFields.push_back(distinctField);
            }
            ins.first->second.insert(initialColumnNames.size());
        } else {
            nondistinctColumns.insert(initialColumnNames.size());
        }

        if (child->Head().IsAtom()) {
            finalColumnNames.push_back(child->HeadPtr());
        } else {
            finalColumnNames.push_back(child->Head().HeadPtr());
        }

        initialColumnNames.push_back(ctx.NewAtom(finalColumnNames.back()->Pos(), "_yql_agg_" + ToString(initialColumnNames.size()), TNodeFlags::Default));
    }

    TExprNode::TListType nothingStates;
    for (ui32 index = 0; index < aggregatedColumns->ChildrenSize(); ++index) {
        auto trait = aggregatedColumns->Child(index)->Child(1);

        auto saveLambda = trait->Child(3);
        auto saveLambdaType = saveLambda->GetTypeAnn();
        auto typeNode = ExpandType(node->Pos(), *saveLambdaType, ctx);
        nothingStates.push_back(ctx.Builder(node->Pos())
            .Callable("Nothing")
                .Callable(0, "OptionalType")
                    .Add(0, std::move(typeNode))
                .Seal()
            .Seal()
            .Build()
        );
    }

    TExprNode::TPtr groupInput;

    if (!nondistinctColumns.empty()) {
        auto combineInit = ctx.Builder(node->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 ndx = 0;
                        for (ui32 i: nondistinctColumns) {
                            auto trait = aggregatedColumns->Child(i)->Child(1);
                            auto initLambda = trait->Child(1);
                            if (initLambda->Head().ChildrenSize() == 1) {
                                parent.List(ndx++)
                                    .Add(0, initialColumnNames[i])
                                    .Apply(1, *initLambda)
                                        .With(0)
                                            .Callable("CastStruct")
                                                .Arg(0, "item")
                                                .Add(1, ExpandType(node->Pos(), *initLambda->Head().Head().GetTypeAnn(), ctx))
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal();
                            } else {
                                parent.List(ndx++)
                                    .Add(0, initialColumnNames[i])
                                    .Apply(1, *initLambda)
                                        .With(0)
                                            .Callable("CastStruct")
                                                .Arg(0, "item")
                                                .Add(1, ExpandType(node->Pos(), *initLambda->Head().Head().GetTypeAnn(), ctx))
                                            .Seal()
                                        .Done()
                                        .With(1)
                                            .Callable("Uint32")
                                                .Atom(0, ToString(i), TNodeFlags::Default)
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal();
                            }
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build();

        auto combineUpdate = ctx.Builder(node->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Param("state")
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 ndx = 0;
                        for (ui32 i: nondistinctColumns) {
                            auto trait = aggregatedColumns->Child(i)->Child(1);
                            auto updateLambda = trait->Child(2);
                            if (updateLambda->Head().ChildrenSize() == 2) {
                                parent.List(ndx++)
                                    .Add(0, initialColumnNames[i])
                                    .Apply(1, *updateLambda)
                                        .With(0)
                                            .Callable("CastStruct")
                                                .Arg(0, "item")
                                                .Add(1, ExpandType(node->Pos(), *updateLambda->Head().Head().GetTypeAnn(), ctx))
                                            .Seal()
                                        .Done()
                                        .With(1)
                                            .Callable("Member")
                                                .Arg(0, "state")
                                                .Add(1, initialColumnNames[i])
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal();
                            } else {
                                parent.List(ndx++)
                                    .Add(0, initialColumnNames[i])
                                    .Apply(1, *updateLambda)
                                        .With(0)
                                            .Callable("CastStruct")
                                                .Arg(0, "item")
                                                .Add(1, ExpandType(node->Pos(), *updateLambda->Head().Head().GetTypeAnn(), ctx))
                                            .Seal()
                                        .Done()
                                        .With(1)
                                            .Callable("Member")
                                                .Arg(0, "state")
                                                .Add(1, initialColumnNames[i])
                                            .Seal()
                                        .Done()
                                        .With(2)
                                            .Callable("Uint32")
                                                .Atom(0, ToString(i), TNodeFlags::Default)
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal();
                            }
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build();

        auto combineSave = ctx.Builder(node->Pos())
            .Lambda()
                .Param("key")
                .Param("state")
                .Callable("Just")
                    .Callable(0, "AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i = 0; i < initialColumnNames.size(); ++i) {
                                if (nondistinctColumns.find(i) == nondistinctColumns.end()) {
                                    parent.List(i)
                                        .Add(0, initialColumnNames[i])
                                        .Add(1, nothingStates[i])
                                    .Seal();
                                } else {
                                    auto trait = aggregatedColumns->Child(i)->Child(1);
                                    auto saveLambda = trait->Child(3);
                                    if (!distinctFields.empty()) {
                                        parent.List(i)
                                            .Add(0, initialColumnNames[i])
                                            .Callable(1, "Just")
                                                .Apply(0, *saveLambda)
                                                    .With(0)
                                                        .Callable("Member")
                                                            .Arg(0, "state")
                                                            .Add(1, initialColumnNames[i])
                                                        .Seal()
                                                    .Done()
                                                .Seal()
                                            .Seal()
                                        .Seal();
                                    } else {
                                        parent.List(i)
                                            .Add(0, initialColumnNames[i])
                                            .Apply(1, *saveLambda)
                                                .With(0)
                                                    .Callable("Member")
                                                        .Arg(0, "state")
                                                        .Add(1, initialColumnNames[i])
                                                    .Seal()
                                                .Done()
                                            .Seal()
                                        .Seal();
                                    }
                                }
                            }
                            return parent;
                        })
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 pos = 0;
                            for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                                auto listBuilder = parent.List(initialColumnNames.size() + i);
                                listBuilder.Add(0, keyColumns->ChildPtr(i));
                                if (keyColumns->ChildrenSize() > 1) {
                                    if (needPickle) {
                                        listBuilder
                                            .Callable(1, "Nth")
                                                .Callable(0, "Unpickle")
                                                    .Add(0, pickleTypeNode)
                                                    .Arg(1, "key")
                                                .Seal()
                                                .Atom(1, ToString(pos), TNodeFlags::Default)
                                            .Seal();
                                    } else {
                                        listBuilder
                                            .Callable(1, "Nth")
                                                .Arg(0, "key")
                                                .Atom(1, ToString(pos), TNodeFlags::Default)
                                            .Seal();
                                    }
                                    ++pos;
                                } else {
                                    if (needPickle) {
                                        listBuilder.Callable(1, "Unpickle")
                                            .Add(0, pickleTypeNode)
                                            .Arg(1, "key")
                                            .Seal();
                                    } else {
                                        listBuilder.Arg(1, "key");
                                    }
                                }
                                listBuilder.Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        groupInput = ctx.Builder(node->Pos())
            .Callable("CombineByKey")
                .Add(0, list)
                .Add(1, preMap)
                .Add(2, keyExtractor)
                .Add(3, std::move(combineInit))
                .Add(4, std::move(combineUpdate))
                .Add(5, std::move(combineSave))
            .Seal()
            .Build();
    }

    for (ui32 index = 0; index < distinctFields.size(); ++index) {
        auto distinctField = distinctFields[index];
        auto& indicies = distinct2Columns[distinctField->Content()];
        auto distinctIndex = rowType->FindItem(distinctField->Content());
        YQL_ENSURE(distinctIndex, "Unknown field: " << distinctField->Content());
        auto distinctType = rowType->GetItems()[*distinctIndex]->GetItemType();
        TVector<const TTypeAnnotationNode*> distinctKeyItemTypes = keyItemTypes;
        distinctKeyItemTypes.push_back(distinctType);
        bool needDistinctPickle = compact ? false : needPickle;
        auto valueType = distinctType;
        if (distinctType->GetKind() == ETypeAnnotationKind::Optional) {
            distinctType = distinctType->Cast<TOptionalExprType>()->GetItemType();
        }

        if (distinctType->GetKind() != ETypeAnnotationKind::Data) {
            needDistinctPickle = true;
            valueType = ctx.MakeType<TDataExprType>(EDataSlot::String);
        }

        const auto expandedValueType = needDistinctPickle ?
            ctx.Builder(node->Pos())
                .Callable("DataType")
                    .Atom(0, "String", TNodeFlags::Default)
                .Seal()
            .Build():
            ExpandType(node->Pos(), *valueType, ctx);

        distinctFieldNeedsPickle[distinctField->Content()] = needDistinctPickle;
        auto udfSetCreateValue = ctx.Builder(node->Pos())
            .Callable("Udf")
                .Atom(0, "Set.Create")
                .Callable(1, "Void").Seal()
                .Callable(2, "TupleType")
                    .Callable(0, "TupleType")
                        .Add(0, expandedValueType)
                        .Callable(1, "DataType")
                            .Atom(0, "Uint32", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Callable(1, "StructType").Seal()
                    .Add(2, expandedValueType)
                .Seal()
            .Seal()
            .Build();

        udfSetCreate[distinctField->Content()] = udfSetCreateValue;
        auto resourceType = ctx.Builder(node->Pos())
            .Callable("TypeOf")
                .Callable(0, "Apply")
                    .Add(0, udfSetCreateValue)
                    .Callable(1, "InstanceOf")
                        .Add(0, expandedValueType)
                    .Seal()
                    .Callable(2, "Uint32")
                        .Atom(0, "0", TNodeFlags::Default)
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        udfAddValue[distinctField->Content()] = ctx.Builder(node->Pos())
            .Callable("Udf")
                .Atom(0, "Set.AddValue")
                .Callable(1, "Void").Seal()
                .Callable(2, "TupleType")
                    .Callable(0, "TupleType")
                        .Add(0, resourceType)
                        .Add(1, expandedValueType)
                    .Seal()
                    .Callable(1, "StructType").Seal()
                    .Add(2, expandedValueType)
                .Seal()
            .Seal()
            .Build();

        udfWasChanged[distinctField->Content()] = ctx.Builder(node->Pos())
            .Callable("Udf")
                .Atom(0, "Set.WasChanged")
                .Callable(1, "Void").Seal()
                .Callable(2, "TupleType")
                    .Callable(0, "TupleType")
                        .Add(0, resourceType)
                    .Seal()
                    .Callable(1, "StructType").Seal()
                    .Add(2, expandedValueType)
                .Seal()
            .Seal()
            .Build();

        auto distinctKeyExtractor = ctx.Builder(node->Pos())
            .Lambda()
                .Param("item")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    if (keyColumns->ChildrenSize() != 0) {
                        auto listBuilder = parent.List();
                        ui32 pos = 0;
                        for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                            listBuilder
                                .Callable(pos++, "Member")
                                    .Arg(0, "item")
                                    .Add(1, keyColumns->ChildPtr(i))
                                .Seal();
                        }
                        listBuilder
                            .Callable(pos, "Member")
                                .Arg(0, "item")
                                .Add(1, distinctField)
                            .Seal();

                        return listBuilder.Seal();
                    } else {
                        return parent
                            .Callable("Member")
                                .Arg(0, "item")
                                .Add(1, distinctField)
                            .Seal();
                    }
                })
            .Seal()
            .Build();

        const TTypeAnnotationNode* distinctPickleType = nullptr;
        TExprNode::TPtr distinctPickleTypeNode;
        if (needDistinctPickle) {
            distinctPickleType = keyColumns->ChildrenSize() > 0  ? ctx.MakeType<TTupleExprType>(distinctKeyItemTypes) : distinctKeyItemTypes.front();
            distinctPickleTypeNode = ExpandType(node->Pos(), *distinctPickleType, ctx);
        }

        if (needDistinctPickle) {
            distinctKeyExtractor = ctx.Builder(node->Pos())
                .Lambda()
                    .Param("item")
                    .Callable("StablePickle")
                        .Apply(0, *distinctKeyExtractor).With(0, "item").Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        auto distinctCombineInit = ctx.Builder(node->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 ndx = 0;
                        for (ui32 i: indicies) {
                            auto trait = aggregatedColumns->Child(i)->Child(1);
                            auto initLambda = trait->Child(1);
                            if (initLambda->Head().ChildrenSize() == 1) {
                                parent.List(ndx++)
                                    .Add(0, initialColumnNames[i])
                                    .Apply(1, *initLambda)
                                        .With(0)
                                            .Callable("Member")
                                                .Arg(0, "item")
                                                .Add(1, distinctField)
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal();
                            } else {
                                parent.List(ndx++)
                                    .Add(0, initialColumnNames[i])
                                    .Apply(1, *initLambda)
                                        .With(0)
                                            .Callable("Member")
                                                .Arg(0, "item")
                                                .Add(1, distinctField)
                                            .Seal()
                                        .Done()
                                        .With(1)
                                            .Callable("Uint32")
                                                .Atom(0, ToString(i), TNodeFlags::Default)
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal();
                            }
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build();

        auto distinctCombineUpdate = ctx.Builder(node->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Param("state")
                .Arg("state")
            .Seal()
            .Build();

        ui32 ndx = 0;
        auto distinctCombineSave = ctx.Builder(node->Pos())
            .Lambda()
                .Param("key")
                .Param("state")
                .Callable("Just")
                    .Callable(0, "AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i: indicies) {
                                auto trait = aggregatedColumns->Child(i)->Child(1);
                                auto saveLambda = trait->Child(3);
                                parent.List(ndx++)
                                    .Add(0, initialColumnNames[i])
                                    .Apply(1, *saveLambda)
                                        .With(0)
                                            .Callable("Member")
                                                .Arg(0, "state")
                                                .Add(1, initialColumnNames[i])
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal();
                            }
                            return parent;
                        })
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            if (keyColumns->ChildrenSize() > 0) {
                                if (needDistinctPickle) {
                                    ui32 pos = 0;
                                    for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                                        parent.List(ndx++)
                                            .Add(0, keyColumns->ChildPtr(i))
                                            .Callable(1, "Nth")
                                                .Callable(0, "Unpickle")
                                                    .Add(0, distinctPickleTypeNode)
                                                    .Arg(1, "key")
                                                .Seal()
                                                .Atom(1, ToString(pos++), TNodeFlags::Default)
                                            .Seal()
                                            .Seal();
                                    }
                                    parent.List(ndx++)
                                        .Add(0, distinctField)
                                        .Callable(1, "Nth")
                                                .Callable(0, "Unpickle")
                                                    .Add(0, distinctPickleTypeNode)
                                                    .Arg(1, "key")
                                                .Seal()
                                            .Atom(1, ToString(pos++), TNodeFlags::Default)
                                        .Seal()
                                    .Seal();

                                } else {
                                    ui32 pos = 0;
                                    for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                                        parent.List(ndx++)
                                            .Add(0, keyColumns->ChildPtr(i))
                                            .Callable(1, "Nth")
                                                .Arg(0, "key")
                                                .Atom(1, ToString(pos++), TNodeFlags::Default)
                                            .Seal()
                                            .Seal();
                                    }
                                    parent.List(ndx++)
                                        .Add(0, distinctField)
                                        .Callable(1, "Nth")
                                            .Arg(0, "key")
                                            .Atom(1, ToString(pos++), TNodeFlags::Default)
                                        .Seal()
                                    .Seal();
                                }
                            } else {
                                if (needDistinctPickle) {
                                    parent.List(ndx++)
                                        .Add(0, distinctField)
                                        .Callable(1, "Unpickle")
                                            .Add(0, distinctPickleTypeNode)
                                            .Arg(1, "key")
                                        .Seal()
                                    .Seal();
                                } else {
                                    parent.List(ndx++)
                                        .Add(0, distinctField)
                                        .Arg(1, "key")
                                    .Seal();
                                }
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        auto distinctCombiner = ctx.Builder(node->Pos())
            .Callable("CombineByKey")
                .Add(0, list)
                .Add(1, preMap)
                .Add(2, distinctKeyExtractor)
                .Add(3, std::move(distinctCombineInit))
                .Add(4, std::move(distinctCombineUpdate))
                .Add(5, std::move(distinctCombineSave))
            .Seal()
            .Build();

        auto distinctGrouper = ctx.Builder(node->Pos())
            .Callable("PartitionsByKeys")
                .Add(0, std::move(distinctCombiner))
                .Add(1, distinctKeyExtractor)
                .Callable(2, "Void").Seal()
                .Callable(3, "Void").Seal()
                .Lambda(4)
                    .Param("groups")
                    .Callable("Map")
                        .Callable(0, "Condense1")
                            .Arg(0, "groups")
                            .Lambda(1)
                                .Param("item")
                                .Arg("item")
                            .Seal()
                            .Lambda(2)
                                .Param("item")
                                .Param("state")
                                .Callable("IsKeySwitch")
                                    .Arg(0, "item")
                                    .Arg(1, "state")
                                    .Add(2, distinctKeyExtractor)
                                    .Add(3, distinctKeyExtractor)
                                .Seal()
                            .Seal()
                            .Lambda(3)
                                .Param("item")
                                .Param("state")
                                .Arg("item")
                            .Seal()
                        .Seal()
                        .Lambda(1)
                            .Param("state")
                            .Callable("AsStruct")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    for (ui32 i = 0; i < initialColumnNames.size(); ++i) {
                                        if (indicies.find(i) != indicies.end()) {
                                            parent.List(i)
                                                .Add(0, initialColumnNames[i])
                                                .Callable(1, "Just")
                                                    .Callable(0, "Member")
                                                        .Arg(0, "state")
                                                        .Add(1, initialColumnNames[i])
                                                    .Seal()
                                                .Seal()
                                            .Seal();
                                        } else {
                                            parent.List(i)
                                                .Add(0, initialColumnNames[i])
                                                .Add(1, nothingStates[i])
                                            .Seal();
                                        }
                                    }
                                    return parent;
                                })
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    if (keyColumns->ChildrenSize() > 0) {
                                        for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                                            parent.List(initialColumnNames.size() + i)
                                                .Add(0, keyColumns->ChildPtr(i))
                                                .Callable(1, "Member")
                                                    .Arg(0, "state")
                                                    .Add(1, keyColumns->ChildPtr(i))
                                                .Seal().Seal();
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

        if (!groupInput) {
            groupInput = std::move(distinctGrouper);
        } else {
            groupInput = ctx.Builder(node->Pos())
                .Callable("Extend")
                    .Add(0, std::move(groupInput))
                    .Add(1, std::move(distinctGrouper))
                .Seal()
                .Build();
        }
    }

    // If no aggregation functions than add addional combiner
    if (aggregatedColumns->ChildrenSize() == 0 && keyColumns->ChildrenSize() > 0 && !sessionUpdate) {
        // Return key as-is
        auto uniqCombineInit = ctx.Builder(node->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 pos = 0;
                        for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                            auto listBuilder = parent.List(i);
                            listBuilder.Add(0, keyColumns->Child(i));
                            if (keyColumns->ChildrenSize() > 1) {
                                if (needPickle) {
                                    listBuilder
                                        .Callable(1, "Nth")
                                            .Callable(0, "Unpickle")
                                                .Add(0, pickleTypeNode)
                                                .Arg(1, "key")
                                            .Seal()
                                        .Atom(1, ToString(pos++), TNodeFlags::Default)
                                        .Seal();
                                } else {
                                    listBuilder
                                        .Callable(1, "Nth")
                                            .Arg(0, "key")
                                            .Atom(1, ToString(pos++), TNodeFlags::Default)
                                        .Seal();
                                }
                            } else {
                                if (needPickle) {
                                    listBuilder.Callable(1, "Unpickle")
                                        .Add(0, pickleTypeNode)
                                        .Arg(1, "key")
                                    .Seal();
                                } else {
                                    listBuilder.Arg(1, "key");
                                }
                            }
                            listBuilder.Seal();
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build();

        auto uniqCombineUpdate = ctx.Builder(node->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Param("state")
                .Arg("state")
            .Seal()
            .Build();

        // Return state as-is
        auto uniqCombineSave = ctx.Builder(node->Pos())
            .Lambda()
                .Param("key")
                .Param("state")
                .Callable("Just")
                    .Arg(0, "state")
                .Seal()
            .Seal()
            .Build();

        if (!groupInput) {
            groupInput = list;
        }

        groupInput = ctx.Builder(node->Pos())
            .Callable("CombineByKey")
                .Add(0, std::move(groupInput))
                .Add(1, preMap)
                .Add(2, keyExtractor)
                .Add(3, std::move(uniqCombineInit))
                .Add(4, std::move(uniqCombineUpdate))
                .Add(5, std::move(uniqCombineSave))
            .Seal()
            .Build();
    }

    ui32 index = 0U;
    auto groupInit = ctx.Builder(node->Pos())
        .Lambda()
            .Param("item")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                        parent
                            .List(index++)
                                .Add(0, keyColumns->ChildPtr(i))
                                .Callable(1, "Member")
                                    .Arg(0, "item")
                                    .Add(1, keyColumns->ChildPtr(i))
                                .Seal()
                            .Seal();
                    }
                    if (sessionUpdate) {
                        parent
                            .List(index++)
                                .Atom(0, sessionStartMemberName)
                                .Callable(1, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, sessionStartMemberName)
                                .Seal()
                            .Seal();
                    }
                    return parent;
                })
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < initialColumnNames.size(); ++i) {
                        auto child = aggregatedColumns->Child(i);
                        auto trait = child->Child(1);
                        if (!compact) {
                            auto loadLambda = trait->Child(4);

                            if (!distinctFields.empty()) {
                                parent.List(index++)
                                    .Add(0, initialColumnNames[i])
                                    .Callable(1, "Map")
                                        .Callable(0, "Member")
                                            .Arg(0, "item")
                                            .Add(1, initialColumnNames[i])
                                        .Seal()
                                        .Add(1, loadLambda)
                                    .Seal()
                                .Seal();
                            } else {
                                parent.List(index++)
                                    .Add(0, initialColumnNames[i])
                                    .Apply(1, *loadLambda)
                                        .With(0)
                                            .Callable("Member")
                                                .Arg(0, "item")
                                                .Add(1, initialColumnNames[i])
                                            .Seal()
                                        .Done()
                                    .Seal();
                            }
                        } else {
                            auto initLambda = trait->Child(1);
                            auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr;
                            auto initApply = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                parent.Apply(1, *initLambda)
                                    .With(0)
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            if (distinctField) {
                                                parent
                                                    .Callable("Member")
                                                        .Arg(0, "item")
                                                        .Add(1, distinctField)
                                                    .Seal();
                                            } else {
                                                parent
                                                    .Callable("CastStruct")
                                                        .Arg(0, "item")
                                                        .Add(1, ExpandType(node->Pos(), *initLambda->Head().Head().GetTypeAnn(), ctx))
                                                    .Seal();
                                            }

                                            return parent;
                                        })
                                    .Done()
                                    .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                                        if (initLambda->Head().ChildrenSize() == 2) {
                                            parent.With(1)
                                                .Callable("Uint32")
                                                    .Atom(0, ToString(i), TNodeFlags::Default)
                                                    .Seal()
                                                .Done();
                                        }

                                        return parent;
                                    })
                                .Seal();

                                return parent;
                            };

                            if (distinctField) {
                                const bool isFirst = *distinct2Columns[distinctField->Content()].begin() == i;
                                if (isFirst) {
                                    parent.List(index++)
                                        .Add(0, initialColumnNames[i])
                                        .List(1)
                                            .Callable(0, "NamedApply")
                                                .Add(0, udfSetCreate[distinctField->Content()])
                                                .List(1)
                                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                        if (!distinctFieldNeedsPickle[distinctField->Content()]) {
                                                            parent.Callable(0, "Member")
                                                                .Arg(0, "item")
                                                                .Add(1, distinctField)
                                                            .Seal();
                                                        } else {
                                                            parent.Callable(0, "StablePickle")
                                                                .Callable(0, "Member")
                                                                .Arg(0, "item")
                                                                .Add(1, distinctField)
                                                                .Seal()
                                                                .Seal();
                                                        }

                                                        return parent;
                                                    })
                                                    .Callable(1, "Uint32")
                                                        .Atom(0, "0", TNodeFlags::Default)
                                                    .Seal()
                                                .Seal()
                                                .Callable(2, "AsStruct").Seal()
                                                .Callable(3, "DependsOn")
                                                    .Callable(0, "String")
                                                        .Add(0, distinctField)
                                                    .Seal()
                                                .Seal()
                                            .Seal()
                                            .Do(initApply)
                                        .Seal()
                                        .Seal();
                                } else {
                                    parent.List(index++)
                                        .Add(0, initialColumnNames[i])
                                        .Do(initApply)
                                        .Seal();
                                }
                            } else {
                                parent.List(index++)
                                    .Add(0, initialColumnNames[i])
                                    .Do(initApply)
                                .Seal();
                            }
                        }
                    }
                    return parent;
                })
            .Seal()
        .Seal()
        .Build();

    index = 0;
    auto groupMerge = ctx.Builder(node->Pos())
        .Lambda()
            .Param("item")
            .Param("state")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                        parent
                            .List(index++)
                                .Add(0, keyColumns->ChildPtr(i))
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Add(1, keyColumns->ChildPtr(i))
                                .Seal()
                            .Seal();
                    }
                    if (sessionUpdate) {
                        parent
                            .List(index++)
                                .Atom(0, sessionStartMemberName)
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Atom(1, sessionStartMemberName)
                                .Seal()
                            .Seal();
                    }
                    return parent;
                })
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < initialColumnNames.size(); ++i) {
                        auto child = aggregatedColumns->Child(i);
                        auto trait = child->Child(1);
                        if (!compact) {
                            auto loadLambda = trait->Child(4);
                            auto mergeLambda = trait->Child(5);

                            if (!distinctFields.empty()) {
                                parent.List(index++)
                                    .Add(0, initialColumnNames[i])
                                    .Callable(1, "OptionalReduce")
                                        .Callable(0, "Map")
                                            .Callable(0, "Member")
                                                .Arg(0, "item")
                                                .Add(1, initialColumnNames[i])
                                            .Seal()
                                            .Add(1, loadLambda)
                                        .Seal()
                                        .Callable(1, "Member")
                                            .Arg(0, "state")
                                            .Add(1, initialColumnNames[i])
                                        .Seal()
                                        .Add(2, mergeLambda)
                                    .Seal()
                                .Seal();
                            } else {
                                parent.List(index++)
                                    .Add(0, initialColumnNames[i])
                                    .Apply(1, *mergeLambda)
                                        .With(0)
                                            .Apply(*loadLambda)
                                                .With(0)
                                                    .Callable("Member")
                                                        .Arg(0, "item")
                                                        .Add(1, initialColumnNames[i])
                                                    .Seal()
                                                .Done()
                                            .Seal()
                                        .Done()
                                        .With(1)
                                            .Callable("Member")
                                                .Arg(0, "state")
                                                .Add(1, initialColumnNames[i])
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal();
                            }
                        } else {
                            auto updateLambda = trait->Child(2);
                            auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr;
                            const bool isFirst = distinctField ? (*distinct2Columns[distinctField->Content()].begin() == i) : false;
                            auto updateApply = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                parent.Apply(1, *updateLambda)
                                    .With(0)
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            if (distinctField) {
                                                parent
                                                    .Callable("Member")
                                                        .Arg(0, "item")
                                                        .Add(1, distinctField)
                                                    .Seal();
                                            } else {
                                                parent
                                                    .Callable("CastStruct")
                                                        .Arg(0, "item")
                                                        .Add(1, ExpandType(node->Pos(), *updateLambda->Head().Head().GetTypeAnn(), ctx))
                                                    .Seal();
                                            }

                                            return parent;
                                        })
                                    .Done()
                                    .With(1)
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            if (distinctField && isFirst) {
                                                parent.Callable("Nth")
                                                    .Callable(0, "Member")
                                                        .Arg(0, "state")
                                                        .Add(1, initialColumnNames[i])
                                                    .Seal()
                                                    .Atom(1, "1", TNodeFlags::Default)
                                                    .Seal();
                                            } else {
                                                parent.Callable("Member")
                                                    .Arg(0, "state")
                                                    .Add(1, initialColumnNames[i])
                                                    .Seal();
                                            }

                                            return parent;
                                        })
                                    .Done()
                                    .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                                        if (updateLambda->Head().ChildrenSize() == 3) {
                                            parent
                                                .With(2)
                                                    .Callable("Uint32")
                                                        .Atom(0, ToString(i), TNodeFlags::Default)
                                                    .Seal()
                                                .Done();
                                        }

                                        return parent;
                                    })
                                .Seal();

                                return parent;
                            };

                            if (distinctField) {
                                auto distinctIndex = *distinct2Columns[distinctField->Content()].begin();
                                ui32 newValueIndex = 0;
                                auto newValue = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    parent.Callable(newValueIndex, "NamedApply")
                                        .Add(0, udfAddValue[distinctField->Content()])
                                        .List(1)
                                            .Callable(0, "Nth")
                                                .Callable(0, "Member")
                                                    .Arg(0, "state")
                                                    .Add(1, initialColumnNames[distinctIndex])
                                                .Seal()
                                                .Atom(1, "0", TNodeFlags::Default)
                                            .Seal()
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                if (!distinctFieldNeedsPickle[distinctField->Content()]) {
                                                    parent.Callable(1, "Member")
                                                        .Arg(0, "item")
                                                        .Add(1, distinctField)
                                                    .Seal();
                                                } else {
                                                    parent.Callable(1, "StablePickle")
                                                        .Callable(0, "Member")
                                                        .Arg(0, "item")
                                                        .Add(1, distinctField)
                                                        .Seal()
                                                        .Seal();
                                                }

                                                return parent;
                                            })
                                        .Seal()
                                        .Callable(2, "AsStruct").Seal()
                                    .Seal();

                                    return parent;
                                };

                                parent.List(index++)
                                    .Add(0, initialColumnNames[i])
                                    .Callable(1, "If")
                                        .Callable(0, "NamedApply")
                                            .Add(0, udfWasChanged[distinctField->Content()])
                                            .List(1)
                                                .Callable(0, "NamedApply")
                                                    .Add(0, udfAddValue[distinctField->Content()])
                                                    .List(1)
                                                        .Callable(0, "Nth")
                                                            .Callable(0, "Member")
                                                                .Arg(0, "state")
                                                                .Add(1, initialColumnNames[distinctIndex])
                                                            .Seal()
                                                            .Atom(1, "0", TNodeFlags::Default)
                                                        .Seal()
                                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                            if (!distinctFieldNeedsPickle[distinctField->Content()]) {
                                                                parent.Callable(1, "Member")
                                                                    .Arg(0, "item")
                                                                    .Add(1, distinctField)
                                                                .Seal();
                                                            } else {
                                                                parent.Callable(1, "StablePickle")
                                                                    .Callable(0, "Member")
                                                                    .Arg(0, "item")
                                                                    .Add(1, distinctField)
                                                                    .Seal()
                                                                    .Seal();
                                                            }

                                                            return parent;
                                                        })
                                                    .Seal()
                                                    .Callable(2, "AsStruct").Seal()
                                                .Seal()
                                            .Seal()
                                            .Callable(2, "AsStruct").Seal()
                                        .Seal()
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            if (distinctIndex == i) {
                                                parent.List(1)
                                                    .Do(newValue)
                                                    .Do(updateApply)
                                                .Seal();
                                            } else {
                                                parent.Do(updateApply);
                                            }

                                            return parent;
                                        })
                                        .Callable(2, "Member")
                                            .Arg(0, "state")
                                            .Add(1, initialColumnNames[i])
                                        .Seal()
                                    .Seal()
                                    .Seal();
                            } else {
                                parent.List(index++)
                                    .Add(0, initialColumnNames[i])
                                    .Do(updateApply)
                                .Seal();
                            }
                        }
                    }
                    return parent;
                })
            .Seal()
        .Seal()
        .Build();

    index = 0U;
    auto groupSave = ctx.Builder(node->Pos())
        .Lambda()
            .Param("state")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < keyColumns->ChildrenSize(); ++i) {
                        if (keyColumns->Child(i)->Content() == sessionStartMemberName) {
                            continue;
                        }
                        parent
                            .List(index++)
                                .Add(0, keyColumns->ChildPtr(i))
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Add(1, keyColumns->ChildPtr(i))
                                .Seal()
                            .Seal();
                    }

                    if (sessionOutputColumn) {
                        parent
                            .List(index++)
                                .Atom(0, *sessionOutputColumn)
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Atom(1, sessionStartMemberName)
                                .Seal()
                            .Seal();
                    }
                    return parent;
                })
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < initialColumnNames.size(); ++i) {
                        auto child = aggregatedColumns->Child(i);
                        auto trait = child->Child(1);
                        auto finishLambda = trait->Child(6);

                        if (!compact && !distinctFields.empty()) {
                            if (child->Head().IsAtom()) {
                                parent.List(index++)
                                    .Add(0, finalColumnNames[i])
                                    .Callable(1, "Unwrap")
                                        .Callable(0, "Map")
                                            .Callable(0, "Member")
                                                .Arg(0, "state")
                                                .Add(1, initialColumnNames[i])
                                            .Seal()
                                            .Add(1, finishLambda)
                                        .Seal()
                                    .Seal()
                                .Seal();
                            } else {
                                const auto& multiFields = child->Child(0);
                                for (ui32 field = 0; field < multiFields->ChildrenSize(); ++field) {
                                    parent.List(index++)
                                        .Atom(0, multiFields->Child(field)->Content())
                                        .Callable(1, "Nth")
                                            .Callable(0, "Unwrap")
                                                .Callable(0, "Map")
                                                    .Callable(0, "Member")
                                                        .Arg(0, "state")
                                                        .Add(1, initialColumnNames[i])
                                                    .Seal()
                                                    .Add(1, finishLambda)
                                                .Seal()
                                            .Seal()
                                            .Atom(1, ToString(field), TNodeFlags::Default)
                                        .Seal()
                                    .Seal();
                                }
                            }
                        } else {
                            auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr;
                            auto stateExtractor = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                const bool isFirst = distinctField  ? (*distinct2Columns[distinctField->Content()].begin() == i) : false;
                                if (distinctField && isFirst) {
                                    parent.Callable("Nth")
                                        .Callable(0, "Member")
                                        .Arg(0, "state")
                                        .Add(1, initialColumnNames[i])
                                        .Seal()
                                        .Atom(1, "1", TNodeFlags::Default)
                                        .Seal();
                                } else {
                                    parent.Callable("Member")
                                        .Arg(0, "state")
                                        .Add(1, initialColumnNames[i])
                                        .Seal();
                                }

                                return parent;
                            };

                            if (child->Head().IsAtom()) {
                                parent.List(index++)
                                    .Add(0, finalColumnNames[i])
                                    .Apply(1, *finishLambda)
                                        .With(0)
                                            .Do(stateExtractor)
                                        .Done()
                                    .Seal()
                                .Seal();
                            } else {
                                const auto& multiFields = child->Head();
                                for (ui32 field = 0; field < multiFields.ChildrenSize(); ++field) {
                                    parent.List(index++)
                                        .Atom(0, multiFields.Child(field)->Content())
                                        .Callable(1, "Nth")
                                            .Apply(0, *finishLambda)
                                                .With(0)
                                                    .Do(stateExtractor)
                                                .Done()
                                            .Seal()
                                            .Atom(1, ToString(field), TNodeFlags::Default)
                                        .Seal()
                                    .Seal();
                                }
                            }
                        }
                    }
                    return parent;
                })
            .Seal()
        .Seal()
        .Build();

    if (compact || !groupInput) {
        groupInput = std::move(list);
    }

    TExprNode::TPtr preprocessLambda;
    TExprNode::TPtr condenseSwitch;
    if (sessionUpdate) {
        YQL_ENSURE(compact);
        YQL_ENSURE(sessionKey);
        YQL_ENSURE(sessionKeyType);
        YQL_ENSURE(sessionInit);

        preprocessLambda =
            AddSessionParamsMemberLambda(node->Pos(), sessionStartMemberName, "", keyExtractor, sessionKey, sessionInit, sessionUpdate, ctx);

        if (!IsDataOrOptionalOfData(sessionKeyType)) {
            preprocessLambda = ctx.Builder(node->Pos())
                .Lambda()
                    .Param("stream")
                    .Callable("OrderedMap")
                        .Apply(0, preprocessLambda)
                            .With(0, "stream")
                        .Seal()
                        .Lambda(1)
                            .Param("item")
                            .Callable("ReplaceMember")
                                .Arg(0, "item")
                                .Atom(1, sessionStartMemberName)
                                .Callable(2, "StablePickle")
                                    .Callable(0, "Member")
                                        .Arg(0, "item")
                                        .Atom(1, sessionStartMemberName)
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        condenseSwitch = ctx.Builder(node->Pos())
            .Lambda()
                .Param("item")
                .Param("state")
                .Callable("Or")
                    .Callable(0, "AggrNotEquals")
                        .Apply(0, keyExtractor)
                            .With(0, "item")
                        .Seal()
                        .Apply(1, keyExtractor)
                            .With(0, "state")
                        .Seal()
                    .Seal()
                    .Callable(1, "AggrNotEquals")
                        .Callable(0, "Member")
                            .Arg(0, "item")
                            .Atom(1, sessionStartMemberName)
                        .Seal()
                        .Callable(1, "Member")
                            .Arg(0, "state")
                            .Atom(1, sessionStartMemberName)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    } else {
        YQL_ENSURE(!sessionKey);
        preprocessLambda = MakeIdentityLambda(node->Pos(), ctx);
        condenseSwitch = ctx.Builder(node->Pos())
            .Lambda()
                .Param("item")
                .Param("state")
                .Callable("IsKeySwitch")
                    .Arg(0, "item")
                    .Arg(1, "state")
                    .Add(2, keyExtractor)
                    .Add(3, keyExtractor)
                .Seal()
            .Seal()
            .Build();
    }

    auto grouper = ctx.Builder(node->Pos())
        .Callable("PartitionsByKeys")
            .Add(0, std::move(groupInput))
            .Add(1, keyExtractor)
            .Add(2, sortOrder)
            .Add(3, sortKey)
            .Lambda(4)
                .Param("stream")
                .Callable("Map")
                    .Callable(0, "Condense1")
                        .Apply(0, preprocessLambda)
                            .With(0, "stream")
                        .Seal()
                        .Add(1, std::move(groupInit))
                        .Add(2, condenseSwitch)
                        .Add(3, std::move(groupMerge))
                    .Seal()
                    .Add(1, std::move(groupSave))
                .Seal()
            .Seal()
        .Seal().Build();

    if (keyColumns->ChildrenSize() == 0 && !sessionSetting) {
        return MakeSingleGroupRow(*node, grouper, ctx);
    }

    return grouper;
}

}
