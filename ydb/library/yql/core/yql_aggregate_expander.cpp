#include "yql_aggregate_expander.h"
#include "yql_aggregate_expander.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_window.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

TExprNode::TPtr TAggregateExpander::ExpandAggregate()
{
    YQL_CLOG(DEBUG, Core) << "Expand " << Node->Content();
    Suffix = Node->Content();
    YQL_ENSURE(Suffix.SkipPrefix("Aggregate"));
    AggList = Node->HeadPtr();
    KeyColumns = Node->ChildPtr(1);
    AggregatedColumns = Node->Child(2);
    auto settings = Node->Child(3);

    bool allTraitsCollected = CollectTraits();
    YQL_ENSURE(!HasSetting(*settings, "hopping"), "Aggregate with hopping unsupported here.");

    HaveDistinct = AnyOf(AggregatedColumns->ChildrenList(),
        [](const auto& child) { return child->ChildrenSize() == 3; });
    EffectiveCompact = (HaveDistinct && CompactForDistinct) || ForceCompact || HasSetting(*settings, "compact");
    for (const auto& trait : Traits) {
        auto mergeLambda = trait->Child(5);
        if (mergeLambda->Tail().IsCallable("Void")) {
            EffectiveCompact = true;
            break;
        }
    }

    if (Suffix == "Finalize") {
        EffectiveCompact = true;
        Suffix = "";
    } else if (Suffix != "") {
        EffectiveCompact = false;
    }

    OriginalRowType = GetSeqItemType(Node->Head().GetTypeAnn())->Cast<TStructExprType>();
    RowItems = OriginalRowType->GetItems();

    ProcessSessionSetting(GetSetting(*settings, "session"));
    RowType = Ctx.MakeType<TStructExprType>(RowItems);

    TVector<const TTypeAnnotationNode*> keyItemTypes = GetKeyItemTypes();
    bool needPickle = IsNeedPickle(keyItemTypes);
    auto keyExtractor = GetKeyExtractor(needPickle);
    CollectColumnsSpecs();

    if (Suffix == "" && !HaveSessionSetting && !EffectiveCompact && UsePhases) {
        return GeneratePhases();
    }

    if (!allTraitsCollected) {
        return RebuildAggregate();
    }

    BuildNothingStates();
    if (Suffix == "MergeState" || Suffix == "MergeFinalize" || Suffix == "MergeManyFinalize") {
        return GeneratePostAggregate(AggList, keyExtractor);
    }

    TExprNode::TPtr preAgg = GeneratePartialAggregate(keyExtractor, keyItemTypes, needPickle);
    if (EffectiveCompact || !preAgg) {
        preAgg = std::move(AggList);
    }

    if (Suffix == "Combine" || Suffix == "CombineState") {
        return preAgg;
    }

    return GeneratePostAggregate(preAgg, keyExtractor);
}

TExprNode::TPtr TAggregateExpander::ExpandAggApply(const TExprNode::TPtr& node)
{
    auto name = node->Head().Content();
    auto exportsPtr = TypesCtx.Modules->GetModule("/lib/yql/aggregate.yql");
    YQL_ENSURE(exportsPtr);
    const auto& exports = exportsPtr->Symbols();
    const auto ex = exports.find(TString(name) + "_traits_factory");
    YQL_ENSURE(exports.cend() != ex);
    TNodeOnNodeOwnedMap deepClones;
    auto lambda = Ctx.DeepCopy(*ex->second, exportsPtr->ExprCtx(), deepClones, true, false);

    auto listTypeNode = Ctx.NewCallable(node->Pos(), "ListType", { node->ChildPtr(1) });
    auto extractor = node->ChildPtr(2);

    auto traits = Ctx.ReplaceNodes(lambda->TailPtr(), {
        {lambda->Head().Child(0), listTypeNode},
        {lambda->Head().Child(1), extractor}
        });

    Ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
    auto status = ExpandApply(traits, traits, Ctx);
    YQL_ENSURE(status != IGraphTransformer::TStatus::Error);
    return traits;
}

bool TAggregateExpander::CollectTraits() {
    bool allTraitsCollected = true;
    for (ui32 index = 0; index < AggregatedColumns->ChildrenSize(); ++index) {
        auto trait = AggregatedColumns->Child(index)->ChildPtr(1);
        if (trait->IsCallable({ "AggApply", "AggApplyState" })) {
            trait = ExpandAggApply(trait);
            allTraitsCollected = false;
        }
        Traits.push_back(trait);
    }
    return allTraitsCollected;
}

TExprNode::TPtr TAggregateExpander::RebuildAggregate()
{
    TExprNode::TListType newAggregatedColumnsItems = AggregatedColumns->ChildrenList();
    for (ui32 index = 0; index < AggregatedColumns->ChildrenSize(); ++index) {
        auto trait = AggregatedColumns->Child(index)->ChildPtr(1);
        if (trait->IsCallable("AggApply")) {
            newAggregatedColumnsItems[index] = Ctx.ChangeChild(*(newAggregatedColumnsItems[index]), 1, std::move(Traits[index]));
        } else if (trait->IsCallable("AggApplyState")) {
            auto newTrait = Ctx.Builder(Node->Pos())
                .Callable("AggregationTraits")
                    .Add(0, trait->ChildPtr(1))
                    .Add(1, trait->ChildPtr(2)) // extractor for state, not initial value itself
                    .Lambda(2)
                        .Param("item")
                        .Param("state")
                        .Callable("Void")
                        .Seal()
                    .Seal()
                    .Add(3, Traits[index]->ChildPtr(3))
                    .Add(4, Traits[index]->ChildPtr(4))
                    .Add(5, Traits[index]->ChildPtr(5))
                    .Add(6, Traits[index]->ChildPtr(6))
                    .Add(7, Traits[index]->ChildPtr(7))
                .Seal()
                .Build();

            newAggregatedColumnsItems[index] = Ctx.ChangeChild(*(newAggregatedColumnsItems[index]), 1, std::move(newTrait));
        }
    }

    return Ctx.ChangeChild(*Node, 2, Ctx.NewList(Node->Pos(), std::move(newAggregatedColumnsItems)));
}

TExprNode::TPtr TAggregateExpander::GetContextLambda()
{
    return HasContextFuncs(*AggregatedColumns) ?
        Ctx.Builder(Node->Pos())
            .Lambda()
                .Param("stream")
                .Callable("WithContext")
                    .Arg(0, "stream")
                    .Atom(1, "Agg")
                .Seal()
            .Seal()
            .Build() :
        Ctx.Builder(Node->Pos())
            .Lambda()
                .Param("stream")
                .Arg("stream")
            .Seal()
            .Build();
}

void TAggregateExpander::ProcessSessionSetting(TExprNode::TPtr sessionSetting)
{
    if (!sessionSetting) {
        return;
    }
    HaveSessionSetting = true;

    YQL_ENSURE(sessionSetting->Child(1)->Child(0)->IsAtom());
    SessionOutputColumn = sessionSetting->Child(1)->Child(0)->Content();

    // remove session column from other keys
    TExprNodeList keyColumnsList = KeyColumns->ChildrenList();
    EraseIf(keyColumnsList, [&](const auto& key) { return SessionOutputColumn == key->Content(); });
    KeyColumns = Ctx.NewList(KeyColumns->Pos(), std::move(keyColumnsList));

    SessionWindowParams.Traits = sessionSetting->Child(1)->ChildPtr(1);
    ExtractSessionWindowParams(Node->Pos(), SessionWindowParams, Ctx);
    ExtractSortKeyAndOrder(Node->Pos(), SessionWindowParams.SortTraits, SortParams, Ctx);

    if (HaveDistinct) {
        auto keySelector = BuildKeySelector(Node->Pos(), *OriginalRowType, KeyColumns, Ctx);
        const auto sessionStartMemberLambda = AddSessionParamsMemberLambda(Node->Pos(), SessionStartMemberName, keySelector,
            SessionWindowParams, Ctx);

        AggList = Ctx.Builder(Node->Pos())
            .Callable("PartitionsByKeys")
                .Add(0, AggList)
                .Add(1, keySelector)
                .Add(2, SortParams.Order)
                .Add(3, SortParams.Key)
                .Lambda(4)
                    .Param("partitionedStream")
                    .Apply(sessionStartMemberLambda)
                        .With(0, "partitionedStream")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        auto keyColumnsList = KeyColumns->ChildrenList();
        keyColumnsList.push_back(Ctx.NewAtom(Node->Pos(), SessionStartMemberName));
        KeyColumns = Ctx.NewList(Node->Pos(), std::move(keyColumnsList));

        RowItems.push_back(Ctx.MakeType<TItemExprType>(SessionStartMemberName, SessionWindowParams.KeyType));

        SessionWindowParams.Reset();
        SortParams.Key = SortParams.Order = VoidNode;
    } else {
        EffectiveCompact = true;
    }
}

TVector<const TTypeAnnotationNode*> TAggregateExpander::GetKeyItemTypes()
{
    TVector<const TTypeAnnotationNode*> keyItemTypes;
    for (auto keyColumn : KeyColumns->Children()) {
        auto index = RowType->FindItem(keyColumn->Content());
        YQL_ENSURE(index, "Unknown column: " << keyColumn->Content());
        auto type = RowType->GetItems()[*index]->GetItemType();
        keyItemTypes.push_back(type);
        
    }
    return keyItemTypes;
}

bool TAggregateExpander::IsNeedPickle(const TVector<const TTypeAnnotationNode*>& keyItemTypes)
{
    bool needPickle = false;
    for (auto type : keyItemTypes) {
        needPickle = needPickle || AllowPickle && !IsDataOrOptionalOfData(type);
    }
    return needPickle;
}

TExprNode::TPtr TAggregateExpander::GetKeyExtractor(bool needPickle)
{
    TExprNode::TPtr keyExtractor = Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("item")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                if (KeyColumns->ChildrenSize() == 0) {
                    return parent.Callable("Uint32").Atom(0, "0", TNodeFlags::Default).Seal();
                }
                else if (KeyColumns->ChildrenSize() == 1) {
                    return parent.Callable("Member").Arg(0, "item").Add(1, KeyColumns->HeadPtr()).Seal();
                }
                else {
                    auto listBuilder = parent.List();
                    ui32 pos = 0;
                    for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                        listBuilder
                            .Callable(pos++, "Member")
                                .Arg(0, "item")
                                .Add(1, KeyColumns->ChildPtr(i))
                            .Seal();
                    }
                    return listBuilder.Seal();
                }
            })
        .Seal()
        .Build();

    if (needPickle) {
        keyExtractor = Ctx.Builder(Node->Pos())
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
    return keyExtractor;
}

void TAggregateExpander::CollectColumnsSpecs()
{
    for (ui32 index = 0; index < AggregatedColumns->ChildrenSize(); ++index) {
        auto child = AggregatedColumns->Child(index);
        if (const auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr) {
            const auto ins = Distinct2Columns.emplace(distinctField->Content(), TIdxSet());
            if (ins.second) {
                DistinctFields.push_back(distinctField);
            }
            ins.first->second.insert(InitialColumnNames.size());
        } else {
            NonDistinctColumns.insert(InitialColumnNames.size());
        }

        if (child->Head().IsAtom()) {
            FinalColumnNames.push_back(child->HeadPtr());
        } else {
            FinalColumnNames.push_back(child->Head().HeadPtr());
        }

        InitialColumnNames.push_back(Ctx.NewAtom(FinalColumnNames.back()->Pos(), "_yql_agg_" + ToString(InitialColumnNames.size()), TNodeFlags::Default));
    }
}

void TAggregateExpander::BuildNothingStates()
{
    for (ui32 index = 0; index < AggregatedColumns->ChildrenSize(); ++index) {
        auto trait = Traits[index];
        auto saveLambda = trait->Child(3);
        auto saveLambdaType = saveLambda->GetTypeAnn();
        auto typeNode = ExpandType(Node->Pos(), *saveLambdaType, Ctx);
        NothingStates.push_back(Ctx.Builder(Node->Pos())
            .Callable("Nothing")
                .Callable(0, "OptionalType")
                    .Add(0, std::move(typeNode))
                .Seal()
            .Seal()
            .Build()
        );
    }
}

TExprNode::TPtr TAggregateExpander::GeneratePartialAggregate(const TExprNode::TPtr keyExtractor,
    const TVector<const TTypeAnnotationNode*>& keyItemTypes, bool needPickle)
{
    TExprNode::TPtr pickleTypeNode = nullptr;
    if (needPickle) {
        const TTypeAnnotationNode* pickleType = nullptr;
        pickleType = KeyColumns->ChildrenSize() > 1 ? Ctx.MakeType<TTupleExprType>(keyItemTypes) : keyItemTypes[0];
        pickleTypeNode = ExpandType(Node->Pos(), *pickleType, Ctx);
    }

    TExprNode::TPtr partialAgg = nullptr;
    if (!NonDistinctColumns.empty()) {
        partialAgg = GeneratePartialAggregateForNonDistinct(keyExtractor, pickleTypeNode);
    }
    for (ui32 index = 0; index < DistinctFields.size(); ++index) {
        auto distinctField = DistinctFields[index];

        bool needDistinctPickle = EffectiveCompact ? false : needPickle;
        auto distinctGrouper = GenerateDistinctGrouper(distinctField, keyItemTypes, needDistinctPickle);

        if (!partialAgg) {
            partialAgg = std::move(distinctGrouper);
        } else {
            partialAgg = Ctx.Builder(Node->Pos())
                .Callable("Extend")
                    .Add(0, std::move(partialAgg))
                    .Add(1, std::move(distinctGrouper))
                .Seal()
                .Build();
        }
    }
    // If no aggregation functions then add additional combiner
    if (AggregatedColumns->ChildrenSize() == 0 && KeyColumns->ChildrenSize() > 0 && !SessionWindowParams.Update) {
        if (!partialAgg) {
            partialAgg = AggList;
        }

        auto uniqCombineInit = ReturnKeyAsIsForCombineInit(pickleTypeNode);
        auto uniqCombineUpdate = Ctx.Builder(Node->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Param("state")
                .Arg("state")
            .Seal()
            .Build();
        
        // Return state as-is
        auto uniqCombineSave = Ctx.Builder(Node->Pos())
            .Lambda()
                .Param("key")
                .Param("state")
                .Callable("Just")
                    .Arg(0, "state")
                .Seal()
            .Seal()
            .Build();

        partialAgg = Ctx.Builder(Node->Pos())
            .Callable("CombineByKey")
                .Add(0, std::move(partialAgg))
                .Add(1, PreMap)
                .Add(2, keyExtractor)
                .Add(3, std::move(uniqCombineInit))
                .Add(4, std::move(uniqCombineUpdate))
                .Add(5, std::move(uniqCombineSave))
            .Seal()
            .Build();
    }
    return partialAgg;
}

std::function<TExprNodeBuilder& (TExprNodeBuilder&)> TAggregateExpander::GetPartialAggArgExtractor(ui32 i, bool deserialize) {
    return [&, i, deserialize](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
        auto trait = Traits[i];
        auto extractorLambda = trait->Child(1);
        auto loadLambda = trait->Child(4);
        if (Suffix == "CombineState") {
            if (deserialize) {
                parent.Apply(*loadLambda)
                    .With(0)
                        .Apply(*extractorLambda)
                        .With(0)
                            .Callable("CastStruct")
                                .Arg(0, "item")
                                .Add(1, ExpandType(Node->Pos(), *extractorLambda->Head().Head().GetTypeAnn(), Ctx))
                            .Seal()
                        .Done()
                        .Seal()
                    .Done()
                    .Seal();
            } else {
                parent.Apply(*extractorLambda)
                    .With(0)
                        .Callable("CastStruct")
                            .Arg(0, "item")
                            .Add(1, ExpandType(Node->Pos(), *extractorLambda->Head().Head().GetTypeAnn(), Ctx))
                        .Seal()
                    .Done()
                    .Seal();
            }
        } else {
            parent.Callable("CastStruct")
                .Arg(0, "item")
                .Add(1, ExpandType(Node->Pos(), *extractorLambda->Head().Head().GetTypeAnn(), Ctx))
                .Seal();
        }

        return parent;
    };
}

TExprNode::TPtr TAggregateExpander::GetFinalAggStateExtractor(ui32 i) {
    auto trait = Traits[i];
    if (Suffix.StartsWith("Merge")) {
        auto lambda = trait->ChildPtr(1);
        if (!Suffix.StartsWith("MergeMany")) {
            return lambda;
        }

        if (lambda->Tail().IsCallable("Unwrap")) {
            return Ctx.Builder(Node->Pos())
                .Lambda()
                .Param("item")
                .ApplyPartial(lambda->HeadPtr(), lambda->Tail().HeadPtr())
                    .With(0, "item")
                .Seal()
                .Seal()
                .Build();
        } else {
            return Ctx.Builder(Node->Pos())
                .Lambda()
                .Param("item")
                .Callable("Just")
                    .Apply(0, *lambda)
                        .With(0, "item")
                    .Seal()
                .Seal()
                .Seal()
                .Build();
        }
    }

    bool aggregateOnly = (Suffix != "");
    const auto& columnNames = aggregateOnly ? FinalColumnNames : InitialColumnNames;
    return Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("item")
            .Callable("Member")
                .Arg(0, "item")
                .Add(1, columnNames[i])
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr TAggregateExpander::GeneratePartialAggregateForNonDistinct(const TExprNode::TPtr& keyExtractor, const TExprNode::TPtr& pickleTypeNode)
{
    bool combineOnly = Suffix == "Combine" || Suffix == "CombineState";
    const auto& columnNames = combineOnly ? FinalColumnNames : InitialColumnNames;
    auto initLambdaIndex = (Suffix == "CombineState") ? 4 : 1;
    auto updateLambdaIndex = (Suffix == "CombineState") ? 5 : 2;

    auto combineInit = Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("key")
            .Param("item")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 ndx = 0;
                    for (ui32 i: NonDistinctColumns) {
                        auto trait = Traits[i];
                        auto initLambda = trait->Child(initLambdaIndex);
                        if (initLambda->Head().ChildrenSize() == 1) {
                            parent.List(ndx++)
                                .Add(0, columnNames[i])
                                .Apply(1, *initLambda)
                                    .With(0)
                                        .Do(GetPartialAggArgExtractor(i, false))
                                    .Done()
                                .Seal()
                            .Seal();
                        } else {
                            parent.List(ndx++)
                                .Add(0, columnNames[i])
                                .Apply(1, *initLambda)
                                    .With(0)
                                        .Do(GetPartialAggArgExtractor(i, false))
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

    auto combineUpdate = Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("key")
            .Param("item")
            .Param("state")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 ndx = 0;
                    for (ui32 i: NonDistinctColumns) {
                        auto trait = Traits[i];
                        auto updateLambda = trait->Child(updateLambdaIndex);
                        if (updateLambda->Head().ChildrenSize() == 2) {
                            parent.List(ndx++)
                                .Add(0, columnNames[i])
                                .Apply(1, *updateLambda)
                                    .With(0)
                                        .Do(GetPartialAggArgExtractor(i, true))
                                    .Done()
                                    .With(1)
                                        .Callable("Member")
                                            .Arg(0, "state")
                                            .Add(1, columnNames[i])
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal();
                        } else {
                            parent.List(ndx++)
                                .Add(0, columnNames[i])
                                .Apply(1, *updateLambda)
                                    .With(0)
                                        .Do(GetPartialAggArgExtractor(i, true))
                                    .Done()
                                    .With(1)
                                        .Callable("Member")
                                            .Arg(0, "state")
                                            .Add(1, columnNames[i])
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

    auto combineSave = Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("key")
            .Param("state")
            .Callable("Just")
                .Callable(0, "AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 0; i < columnNames.size(); ++i) {
                            if (NonDistinctColumns.find(i) == NonDistinctColumns.end()) {
                                parent.List(i)
                                    .Add(0, columnNames[i])
                                    .Add(1, NothingStates[i])
                                .Seal();
                            } else {
                                auto trait = Traits[i];
                                auto saveLambda = trait->Child(3);
                                if (!DistinctFields.empty()) {
                                    parent.List(i)
                                        .Add(0, columnNames[i])
                                        .Callable(1, "Just")
                                            .Apply(0, *saveLambda)
                                                .With(0)
                                                    .Callable("Member")
                                                        .Arg(0, "state")
                                                        .Add(1, columnNames[i])
                                                    .Seal()
                                                .Done()
                                            .Seal()
                                        .Seal()
                                    .Seal();
                                } else {
                                    parent.List(i)
                                        .Add(0, columnNames[i])
                                        .Apply(1, *saveLambda)
                                            .With(0)
                                                .Callable("Member")
                                                    .Arg(0, "state")
                                                    .Add(1, columnNames[i])
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
                        for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                            auto listBuilder = parent.List(columnNames.size() + i);
                            listBuilder.Add(0, KeyColumns->ChildPtr(i));
                            if (KeyColumns->ChildrenSize() > 1) {
                                if (pickleTypeNode) {
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
                                if (pickleTypeNode) {
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

    return Ctx.Builder(Node->Pos())
        .Callable("CombineByKey")
            .Add(0, AggList)
            .Add(1, PreMap)
            .Add(2, keyExtractor)
            .Add(3, std::move(combineInit))
            .Add(4, std::move(combineUpdate))
            .Add(5, std::move(combineSave))
        .Seal()
        .Build();
}

void TAggregateExpander::GenerateInitForDistinct(TExprNodeBuilder& parent, ui32& ndx, const TIdxSet& indicies, const TExprNode::TPtr& distinctField) {
    for (ui32 i: indicies) {
        auto trait = Traits[i];
        auto initLambda = trait->Child(1);
        if (initLambda->Head().ChildrenSize() == 1) {
            parent.List(ndx++)
                .Add(0, InitialColumnNames[i])
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
                .Add(0, InitialColumnNames[i])
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
}

TExprNode::TPtr TAggregateExpander::GenerateDistinctGrouper(const TExprNode::TPtr distinctField,
    const TVector<const TTypeAnnotationNode*>& keyItemTypes, bool needDistinctPickle)
{
    auto& indicies = Distinct2Columns[distinctField->Content()];
    auto distinctIndex = RowType->FindItem(distinctField->Content());
    YQL_ENSURE(distinctIndex, "Unknown field: " << distinctField->Content());
    auto distinctType = RowType->GetItems()[*distinctIndex]->GetItemType();
    TVector<const TTypeAnnotationNode*> distinctKeyItemTypes = keyItemTypes;
    distinctKeyItemTypes.push_back(distinctType);
    auto valueType = distinctType;
    if (distinctType->GetKind() == ETypeAnnotationKind::Optional) {
        distinctType = distinctType->Cast<TOptionalExprType>()->GetItemType();
    }

    if (distinctType->GetKind() != ETypeAnnotationKind::Data) {
        needDistinctPickle = true;
        valueType = Ctx.MakeType<TDataExprType>(EDataSlot::String);
    }

    const auto expandedValueType = needDistinctPickle ?
        Ctx.Builder(Node->Pos())
            .Callable("DataType")
                .Atom(0, "String", TNodeFlags::Default)
            .Seal()
        .Build()
        : ExpandType(Node->Pos(), *valueType, Ctx);

    DistinctFieldNeedsPickle[distinctField->Content()] = needDistinctPickle;
    auto udfSetCreateValue = Ctx.Builder(Node->Pos())
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

    UdfSetCreate[distinctField->Content()] = udfSetCreateValue;
    auto resourceType = Ctx.Builder(Node->Pos())
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

    UdfAddValue[distinctField->Content()] = Ctx.Builder(Node->Pos())
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

    UdfWasChanged[distinctField->Content()] = Ctx.Builder(Node->Pos())
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

    auto distinctKeyExtractor = Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("item")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                if (KeyColumns->ChildrenSize() != 0) {
                    auto listBuilder = parent.List();
                    ui32 pos = 0;
                    for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                        listBuilder
                            .Callable(pos++, "Member")
                                .Arg(0, "item")
                                .Add(1, KeyColumns->ChildPtr(i))
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
        distinctPickleType = KeyColumns->ChildrenSize() > 0  ? Ctx.MakeType<TTupleExprType>(distinctKeyItemTypes) : distinctKeyItemTypes.front();
        distinctPickleTypeNode = ExpandType(Node->Pos(), *distinctPickleType, Ctx);
    }

    if (needDistinctPickle) {
        distinctKeyExtractor = Ctx.Builder(Node->Pos())
            .Lambda()
                .Param("item")
                .Callable("StablePickle")
                    .Apply(0, *distinctKeyExtractor).With(0, "item").Seal()
                .Seal()
            .Seal()
            .Build();
    }

    auto distinctCombineInit = Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("key")
            .Param("item")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 ndx = 0;
                    GenerateInitForDistinct(parent, ndx, indicies, distinctField);
                    return parent;
                })
            .Seal()
        .Seal()
        .Build();

    auto distinctCombineUpdate = Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("key")
            .Param("item")
            .Param("state")
            .Arg("state")
        .Seal()
        .Build();

    ui32 ndx = 0;
    auto distinctCombineSave = Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("key")
            .Param("state")
            .Callable("Just")
                .Callable(0, "AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i: indicies) {
                            auto trait = Traits[i];
                            auto saveLambda = trait->Child(3);
                            parent.List(ndx++)
                                .Add(0, InitialColumnNames[i])
                                .Apply(1, *saveLambda)
                                    .With(0)
                                        .Callable("Member")
                                            .Arg(0, "state")
                                            .Add(1, InitialColumnNames[i])
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal();
                        }
                        return parent;
                    })
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        if (KeyColumns->ChildrenSize() > 0) {
                            if (needDistinctPickle) {
                                ui32 pos = 0;
                                for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                                    parent.List(ndx++)
                                        .Add(0, KeyColumns->ChildPtr(i))
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
                                for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                                    parent.List(ndx++)
                                        .Add(0, KeyColumns->ChildPtr(i))
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

    auto distinctCombiner = Ctx.Builder(Node->Pos())
        .Callable("CombineByKey")
            .Add(0, AggList)
            .Add(1, PreMap)
            .Add(2, distinctKeyExtractor)
            .Add(3, std::move(distinctCombineInit))
            .Add(4, std::move(distinctCombineUpdate))
            .Add(5, std::move(distinctCombineSave))
        .Seal()
        .Build();

    auto distinctGrouper = Ctx.Builder(Node->Pos())
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
                                for (ui32 i = 0; i < InitialColumnNames.size(); ++i) {
                                    if (indicies.find(i) != indicies.end()) {
                                        parent.List(i)
                                            .Add(0, InitialColumnNames[i])
                                            .Callable(1, "Just")
                                                .Callable(0, "Member")
                                                    .Arg(0, "state")
                                                    .Add(1, InitialColumnNames[i])
                                                .Seal()
                                            .Seal()
                                        .Seal();
                                    } else {
                                        parent.List(i)
                                            .Add(0, InitialColumnNames[i])
                                            .Add(1, NothingStates[i])
                                        .Seal();
                                    }
                                }
                                return parent;
                            })
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                if (KeyColumns->ChildrenSize() > 0) {
                                    for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                                        parent.List(InitialColumnNames.size() + i)
                                            .Add(0, KeyColumns->ChildPtr(i))
                                            .Callable(1, "Member")
                                                .Arg(0, "state")
                                                .Add(1, KeyColumns->ChildPtr(i))
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
    return distinctGrouper;
}

TExprNode::TPtr TAggregateExpander::ReturnKeyAsIsForCombineInit(const TExprNode::TPtr& pickleTypeNode)
{
    return Ctx.Builder(Node->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 pos = 0;
                        for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                            auto listBuilder = parent.List(i);
                            listBuilder.Add(0, KeyColumns->Child(i));
                            if (KeyColumns->ChildrenSize() > 1) {
                                if (pickleTypeNode) {
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
                                if (pickleTypeNode) {
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
}

TExprNode::TPtr TAggregateExpander::GeneratePostAggregate(const TExprNode::TPtr& preAgg, const TExprNode::TPtr& keyExtractor)
{
    auto preprocessLambda = GeneratePreprocessLambda(keyExtractor);
    auto condenseSwitch = GenerateCondenseSwitch(keyExtractor);
    auto postAgg = Ctx.Builder(Node->Pos())
        .Callable("PartitionsByKeys")
            .Add(0, std::move(preAgg))
            .Add(1, keyExtractor)
            .Add(2, SortParams.Order)
            .Add(3, SortParams.Key)
            .Lambda(4)
                .Param("stream")
                .Apply(GetContextLambda())
                    .With(0)
                        .Callable("Map")
                            .Callable(0, "Condense1")
                                .Apply(0, preprocessLambda)
                                    .With(0, "stream")
                                .Seal()
                                .Add(1, GeneratePostAggregateInitPhase())
                                .Add(2, condenseSwitch)
                                .Add(3, GeneratePostAggregateMergePhase())
                            .Seal()
                            .Add(1, GeneratePostAggregateSavePhase())
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
        .Seal().Build();

    if (KeyColumns->ChildrenSize() == 0 && !HaveSessionSetting && (Suffix == "" || Suffix.EndsWith("Finalize"))) {
        return MakeSingleGroupRow(*Node, postAgg, Ctx);
    }

    return postAgg;
}

TExprNode::TPtr TAggregateExpander::GeneratePreprocessLambda(const TExprNode::TPtr& keyExtractor)
{
    TExprNode::TPtr preprocessLambda;
    if (SessionWindowParams.Update) {
        YQL_ENSURE(EffectiveCompact);
        YQL_ENSURE(SessionWindowParams.Key);
        YQL_ENSURE(SessionWindowParams.KeyType);
        YQL_ENSURE(SessionWindowParams.Init);

        preprocessLambda = AddSessionParamsMemberLambda(Node->Pos(), SessionStartMemberName, "", keyExtractor, 
            SessionWindowParams.Key, SessionWindowParams.Init, SessionWindowParams.Update, Ctx);
    } else {
        YQL_ENSURE(!SessionWindowParams.Key);
        preprocessLambda = MakeIdentityLambda(Node->Pos(), Ctx);
    }
    return preprocessLambda;
}

TExprNode::TPtr TAggregateExpander::GenerateCondenseSwitch(const TExprNode::TPtr& keyExtractor)
{
    TExprNode::TPtr condenseSwitch;
    if (SessionWindowParams.Update) {
        YQL_ENSURE(EffectiveCompact);
        YQL_ENSURE(SessionWindowParams.Key);
        YQL_ENSURE(SessionWindowParams.KeyType);
        YQL_ENSURE(SessionWindowParams.Init);

        condenseSwitch = Ctx.Builder(Node->Pos())
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
                            .Atom(1, SessionStartMemberName)
                        .Seal()
                        .Callable(1, "Member")
                            .Arg(0, "state")
                            .Atom(1, SessionStartMemberName)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    } else {
        YQL_ENSURE(!SessionWindowParams.Key);
        condenseSwitch = Ctx.Builder(Node->Pos())
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
    return condenseSwitch;
}

TExprNode::TPtr TAggregateExpander::GeneratePostAggregateInitPhase()
{
    bool aggregateOnly = (Suffix != "");
    const auto& columnNames = aggregateOnly ? FinalColumnNames : InitialColumnNames;

    ui32 index = 0U;
    return Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("item")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                        parent
                            .List(index++)
                                .Add(0, KeyColumns->ChildPtr(i))
                                .Callable(1, "Member")
                                    .Arg(0, "item")
                                    .Add(1, KeyColumns->ChildPtr(i))
                                .Seal()
                            .Seal();
                    }
                    if (SessionWindowParams.Update) {
                        parent
                            .List(index++)
                                .Atom(0, SessionStartMemberName)
                                .Callable(1, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, SessionStartMemberName)
                                .Seal()
                            .Seal();
                    }
                    return parent;
                })
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < columnNames.size(); ++i) {
                        auto child = AggregatedColumns->Child(i);
                        auto trait = Traits[i];
                        if (!EffectiveCompact) {
                            auto loadLambda = trait->Child(4);
                            auto extractorLambda = GetFinalAggStateExtractor(i);

                            if (!DistinctFields.empty() || Suffix == "MergeManyFinalize") {
                                parent.List(index++)
                                    .Add(0, columnNames[i])
                                    .Callable(1, "Map")
                                        .Apply(0, *extractorLambda)
                                            .With(0, "item")
                                        .Seal()
                                        .Add(1, loadLambda)
                                    .Seal()
                                .Seal();
                            } else {
                                parent.List(index++)
                                    .Add(0, columnNames[i])
                                    .Apply(1, *loadLambda)
                                        .With(0)
                                            .Apply(*extractorLambda)
                                                .With(0, "item")
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
                                                        .Add(1, ExpandType(Node->Pos(), *initLambda->Head().Head().GetTypeAnn(), Ctx))
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
                                const bool isFirst = *Distinct2Columns[distinctField->Content()].begin() == i;
                                if (isFirst) {
                                    parent.List(index++)
                                        .Add(0, columnNames[i])
                                        .List(1)
                                            .Callable(0, "NamedApply")
                                                .Add(0, UdfSetCreate[distinctField->Content()])
                                                .List(1)
                                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                        if (!DistinctFieldNeedsPickle[distinctField->Content()]) {
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
                                        .Add(0, columnNames[i])
                                        .Do(initApply)
                                        .Seal();
                                }
                            } else {
                                parent.List(index++)
                                    .Add(0, columnNames[i])
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
}

TExprNode::TPtr TAggregateExpander::GeneratePostAggregateSavePhase()
{
    bool aggregateOnly = (Suffix != "");
    const auto& columnNames = aggregateOnly ? FinalColumnNames : InitialColumnNames;

    ui32 index = 0U;
    return Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("state")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                        if (KeyColumns->Child(i)->Content() == SessionStartMemberName) {
                            continue;
                        }
                        parent
                            .List(index++)
                                .Add(0, KeyColumns->ChildPtr(i))
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Add(1, KeyColumns->ChildPtr(i))
                                .Seal()
                            .Seal();
                    }

                    if (SessionOutputColumn) {
                        parent
                            .List(index++)
                                .Atom(0, *SessionOutputColumn)
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Atom(1, SessionStartMemberName)
                                .Seal()
                            .Seal();
                    }
                    return parent;
                })
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < columnNames.size(); ++i) {
                        auto child = AggregatedColumns->Child(i);
                        auto trait = Traits[i];
                        auto finishLambda = (Suffix == "MergeState") ? trait->Child(3) : trait->Child(6);

                        if (!EffectiveCompact && (!DistinctFields.empty() || Suffix == "MergeManyFinalize")) {
                            if (child->Head().IsAtom()) {
                                parent.List(index++)
                                    .Add(0, FinalColumnNames[i])
                                    .Callable(1, "Unwrap")
                                        .Callable(0, "Map")
                                            .Callable(0, "Member")
                                                .Arg(0, "state")
                                                .Add(1, columnNames[i])
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
                                                        .Add(1, columnNames[i])
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
                                const bool isFirst = distinctField  ? (*Distinct2Columns[distinctField->Content()].begin() == i) : false;
                                if (distinctField && isFirst) {
                                    parent.Callable("Nth")
                                        .Callable(0, "Member")
                                        .Arg(0, "state")
                                        .Add(1, columnNames[i])
                                        .Seal()
                                        .Atom(1, "1", TNodeFlags::Default)
                                        .Seal();
                                } else {
                                    parent.Callable("Member")
                                        .Arg(0, "state")
                                        .Add(1, columnNames[i])
                                        .Seal();
                                }

                                return parent;
                            };

                            if (child->Head().IsAtom()) {
                                parent.List(index++)
                                    .Add(0, FinalColumnNames[i])
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
}

TExprNode::TPtr TAggregateExpander::GeneratePostAggregateMergePhase()
{
    bool aggregateOnly = (Suffix != "");
    const auto& columnNames = aggregateOnly ? FinalColumnNames : InitialColumnNames;

    ui32 index = 0U;
    return Ctx.Builder(Node->Pos())
        .Lambda()
            .Param("item")
            .Param("state")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                        parent
                            .List(index++)
                                .Add(0, KeyColumns->ChildPtr(i))
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Add(1, KeyColumns->ChildPtr(i))
                                .Seal()
                            .Seal();
                    }
                    if (SessionWindowParams.Update) {
                        parent
                            .List(index++)
                                .Atom(0, SessionStartMemberName)
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Atom(1, SessionStartMemberName)
                                .Seal()
                            .Seal();
                    }
                    return parent;
                })
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < columnNames.size(); ++i) {
                        auto child = AggregatedColumns->Child(i);
                        auto trait = Traits[i];
                        if (!EffectiveCompact) {
                            auto loadLambda = trait->Child(4);
                            auto mergeLambda = trait->Child(5);
                            auto extractorLambda = GetFinalAggStateExtractor(i);

                            if (!DistinctFields.empty() || Suffix == "MergeManyFinalize") {
                                parent.List(index++)
                                    .Add(0, columnNames[i])
                                    .Callable(1, "OptionalReduce")
                                        .Callable(0, "Map")
                                            .Apply(0, extractorLambda)
                                                .With(0, "item")
                                            .Seal()
                                            .Add(1, loadLambda)
                                        .Seal()
                                        .Callable(1, "Member")
                                            .Arg(0, "state")
                                            .Add(1, columnNames[i])
                                        .Seal()
                                        .Add(2, mergeLambda)
                                    .Seal()
                                .Seal();
                            } else {
                                parent.List(index++)
                                    .Add(0, columnNames[i])
                                    .Apply(1, *mergeLambda)
                                        .With(0)
                                            .Apply(*loadLambda)
                                                .With(0)
                                                    .Apply(extractorLambda)
                                                        .With(0, "item")
                                                    .Seal()
                                                .Done()
                                            .Seal()
                                        .Done()
                                        .With(1)
                                            .Callable("Member")
                                                .Arg(0, "state")
                                                .Add(1, columnNames[i])
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal();
                            }
                        } else {
                            auto updateLambda = trait->Child(2);
                            auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr;
                            const bool isFirst = distinctField ? (*Distinct2Columns[distinctField->Content()].begin() == i) : false;
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
                                                        .Add(1, ExpandType(Node->Pos(), *updateLambda->Head().Head().GetTypeAnn(), Ctx))
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
                                                        .Add(1, columnNames[i])
                                                    .Seal()
                                                    .Atom(1, "1", TNodeFlags::Default)
                                                    .Seal();
                                            } else {
                                                parent.Callable("Member")
                                                    .Arg(0, "state")
                                                    .Add(1, columnNames[i])
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
                                auto distinctIndex = *Distinct2Columns[distinctField->Content()].begin();
                                ui32 newValueIndex = 0;
                                auto newValue = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    parent.Callable(newValueIndex, "NamedApply")
                                        .Add(0, UdfAddValue[distinctField->Content()])
                                        .List(1)
                                            .Callable(0, "Nth")
                                                .Callable(0, "Member")
                                                    .Arg(0, "state")
                                                    .Add(1, columnNames[distinctIndex])
                                                .Seal()
                                                .Atom(1, "0", TNodeFlags::Default)
                                            .Seal()
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                if (!DistinctFieldNeedsPickle[distinctField->Content()]) {
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
                                    .Add(0, columnNames[i])
                                    .Callable(1, "If")
                                        .Callable(0, "NamedApply")
                                            .Add(0, UdfWasChanged[distinctField->Content()])
                                            .List(1)
                                                .Callable(0, "NamedApply")
                                                    .Add(0, UdfAddValue[distinctField->Content()])
                                                    .List(1)
                                                        .Callable(0, "Nth")
                                                            .Callable(0, "Member")
                                                                .Arg(0, "state")
                                                                .Add(1, columnNames[distinctIndex])
                                                            .Seal()
                                                            .Atom(1, "0", TNodeFlags::Default)
                                                        .Seal()
                                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                            if (!DistinctFieldNeedsPickle[distinctField->Content()]) {
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
                                            .Add(1, columnNames[i])
                                        .Seal()
                                    .Seal()
                                    .Seal();
                            } else {
                                parent.List(index++)
                                    .Add(0, columnNames[i])
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
}

TExprNode::TPtr TAggregateExpander::GenerateJustOverStates(const TExprNode::TPtr& input, const TIdxSet& indicies) {
    return Ctx.Builder(Node->Pos())
        .Callable("Map")
            .Add(0, input)
            .Lambda(1)
                .Param("row")
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 pos = 0;
                        for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                            parent
                                .List(pos++)
                                    .Add(0, KeyColumns->ChildPtr(i))
                                    .Callable(1, "Member")
                                        .Arg(0, "row")
                                        .Add(1, KeyColumns->ChildPtr(i))
                                    .Seal()
                                .Seal();
                        }

                        for (ui32 i : indicies) {
                            parent
                                .List(pos++)
                                    .Add(0, InitialColumnNames[i])
                                    .Callable(1, "Just")
                                        .Callable(0, "Member")
                                            .Arg(0, "row")
                                            .Add(1, InitialColumnNames[i])
                                        .Seal()
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

TExprNode::TPtr TAggregateExpander::GeneratePhases() {
    const bool many = HaveDistinct;
    YQL_CLOG(DEBUG, Core) << "Aggregate: generate " << (many ? "phases with distinct" : "simple phases");
    TExprNode::TListType mergeTraits;
    for (ui32 index = 0; index < AggregatedColumns->ChildrenSize(); ++index) {
        auto originalTrait = AggregatedColumns->Child(index)->ChildPtr(1);
        auto extractor = Ctx.Builder(Node->Pos())
            .Lambda()
                .Param("row")
                .Callable("Member")
                    .Arg(0, "row")
                    .Add(1, InitialColumnNames[index])
                .Seal()
            .Seal()
            .Build();

        if (many) {
            extractor = Ctx.Builder(Node->Pos())
                .Lambda()
                    .Param("row")
                    .Callable("Unwrap")
                        .Apply(0, extractor)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        bool isAggApply = originalTrait->IsCallable("AggApply");
        auto serializedStateType = isAggApply ? AggApplySerializedStateType(originalTrait, Ctx) : originalTrait->Child(3)->GetTypeAnn();
        if (many) {
            serializedStateType = Ctx.MakeType<TOptionalExprType>(serializedStateType);
        }

        auto extractorTypeNode = Ctx.Builder(Node->Pos())
            .Callable("StructType")
                .List(0)
                    .Add(0, InitialColumnNames[index])
                    .Add(1, ExpandType(Node->Pos(), *serializedStateType, Ctx))
                .Seal()
            .Seal()
            .Build();

        if (isAggApply) {
            mergeTraits.push_back(Ctx.Builder(Node->Pos())
                .Callable("AggApplyState")
                    .Add(0, originalTrait->ChildPtr(0))
                    .Add(1, extractorTypeNode)
                    .Add(2, extractor)
                .Seal()
                .Build());
        } else {
            YQL_ENSURE(originalTrait->IsCallable("AggregationTraits"));
            mergeTraits.push_back(Ctx.Builder(Node->Pos())
                .Callable("AggregationTraits")
                    .Add(0, extractorTypeNode)
                    .Add(1, extractor)
                    .Lambda(2)
                        .Param("item")
                        .Param("state")
                        .Callable("Void")
                        .Seal()
                    .Seal()
                    .Add(3, originalTrait->ChildPtr(3))
                    .Add(4, originalTrait->ChildPtr(4))
                    .Add(5, originalTrait->ChildPtr(5))
                    .Add(6, originalTrait->ChildPtr(6))
                    .Add(7, originalTrait->ChildPtr(7))
                .Seal()
                .Build());
        }
    }

    TExprNode::TListType finalizeColumns;
    for (ui32 index = 0; index < AggregatedColumns->ChildrenSize(); ++index) {
        finalizeColumns.push_back(Ctx.Builder(Node->Pos())
            .List()
                .Add(0, AggregatedColumns->Child(index)->ChildPtr(0))
                .Add(1, mergeTraits[index])
            .Seal()
            .Build());
    }

    if (!many) {
        // simple Combine + MergeFinalize
        TExprNode::TListType combineColumns;
        for (ui32 index = 0; index < AggregatedColumns->ChildrenSize(); ++index) {
            combineColumns.push_back(Ctx.Builder(Node->Pos())
                .List()
                    .Add(0, InitialColumnNames[index])
                    .Add(1, AggregatedColumns->Child(index)->ChildPtr(1))
                .Seal()
                .Build());
        }

        auto combine = Ctx.Builder(Node->Pos())
            .Callable("AggregateCombine")
                .Add(0, AggList)
                .Add(1, KeyColumns)
                .Add(2, Ctx.NewList(Node->Pos(), std::move(combineColumns)))
                .Add(3, Node->ChildPtr(3))
            .Seal()
            .Build();

        auto mergeFinalize = Ctx.Builder(Node->Pos())
            .Callable("AggregateMergeFinalize")
                .Add(0, combine)
                .Add(1, KeyColumns)
                .Add(2, Ctx.NewList(Node->Pos(), std::move(finalizeColumns)))
                .Add(3, Node->ChildPtr(3))
            .Seal()
            .Build();

        return mergeFinalize;
    }

    // process with distincts
    // Combine + Map with Just over states
    //      for each distinct field:
    //          Combine by keys + field
    //          MergeFinalize by keys + field
    //          Map with Just over init func
    // UnionAll
    // MergeManyFinalize
    TExprNode::TListType unionAllInputs;
    if (!NonDistinctColumns.empty()) {
        TExprNode::TListType combineColumns;
        for (ui32 i : NonDistinctColumns) {
            combineColumns.push_back(Ctx.Builder(Node->Pos())
                .List()
                    .Add(0, InitialColumnNames[i])
                    .Add(1, AggregatedColumns->Child(i)->ChildPtr(1))
                .Seal()
                .Build());
        }

        auto combine = Ctx.Builder(Node->Pos())
            .Callable("AggregateCombine")
                .Add(0, AggList)
                .Add(1, KeyColumns)
                .Add(2, Ctx.NewList(Node->Pos(), std::move(combineColumns)))
                .Add(3, Node->ChildPtr(3))
            .Seal()
            .Build();

        unionAllInputs.push_back(GenerateJustOverStates(combine, NonDistinctColumns));
    }

    for (ui32 index = 0; index < DistinctFields.size(); ++index) {
        auto distinctField = DistinctFields[index];
        auto& indicies = Distinct2Columns[distinctField->Content()];
        TExprNode::TListType allKeyColumns = KeyColumns->ChildrenList();
        allKeyColumns.push_back(distinctField);
        auto distinct = Ctx.Builder(Node->Pos())
            .Callable("Aggregate")
                .Add(0, AggList)
                .Add(1, Ctx.NewList(Node->Pos(), std::move(allKeyColumns)))
                .List(2)
                .Seal()
                .Add(3, Node->ChildPtr(3))
            .Seal()
            .Build();

        auto mapInit = Ctx.Builder(Node->Pos())
            .Callable("Map")
                .Add(0, distinct)
                .Lambda(1)
                    .Param("item")
                    .Callable("AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 pos = 0;
                            for (ui32 i = 0; i < KeyColumns->ChildrenSize(); ++i) {
                                parent
                                    .List(pos++)
                                        .Add(0, KeyColumns->ChildPtr(i))
                                        .Callable(1, "Member")
                                            .Arg(0, "item")
                                            .Add(1, KeyColumns->ChildPtr(i))
                                        .Seal()
                                    .Seal();
                            }

                            for (ui32 i: indicies) {
                                auto trait = Traits[i];
                                auto initLambda = trait->Child(1);
                                auto saveLambda = trait->Child(3);
                                if (initLambda->Head().ChildrenSize() == 1) {
                                    parent.List(pos++)
                                        .Add(0, InitialColumnNames[i])
                                        .Apply(1, *saveLambda)
                                            .With(0)
                                                .Apply(*initLambda)
                                                    .With(0)
                                                        .Callable("Member")
                                                            .Arg(0, "item")
                                                            .Add(1, distinctField)
                                                        .Seal()
                                                    .Done()
                                                .Seal()
                                            .Done()
                                        .Seal()
                                    .Seal();
                                } else {
                                    parent.List(pos++)
                                        .Add(0, InitialColumnNames[i])
                                        .Apply(1, *saveLambda)
                                            .With(0)
                                                .Apply(*initLambda)
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
                                            .Done()
                                        .Seal()
                                    .Seal();
                                }
                            }

                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        unionAllInputs.push_back(GenerateJustOverStates(mapInit, indicies));
    }

    auto unionAll = Ctx.NewCallable(Node->Pos(), "UnionAll", std::move(unionAllInputs));
    auto mergeManyFinalize = Ctx.Builder(Node->Pos())
        .Callable("AggregateMergeManyFinalize")
            .Add(0, unionAll)
            .Add(1, KeyColumns)
            .Add(2, Ctx.NewList(Node->Pos(), std::move(finalizeColumns)))
            .Add(3, Node->ChildPtr(3))
        .Seal()
        .Build();

    return mergeManyFinalize;
}

} // namespace NYql
