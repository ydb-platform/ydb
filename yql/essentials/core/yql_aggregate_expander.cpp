#include "yql_aggregate_expander.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_window.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_helpers.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql {

TExprNode::TPtr TAggregateExpander::ExpandAggregate() {
    YQL_CLOG(DEBUG, Core) << "Expand " << Node_->Content();
    if (Node_->Head().GetTypeAnn()->HasErrors()) {
        TErrorTypeVisitor errorVisitor(Ctx_);
        Node_->Head().GetTypeAnn()->Accept(errorVisitor);
        return nullptr;
    }

    auto result = ExpandAggregateWithFullOutput();
    if (result) {
        auto outputColumns = GetSetting(*Node_->Child(NNodes::TCoAggregate::idx_Settings), "output_columns");
        if (outputColumns) {
            result = Ctx_.NewCallable(result->Pos(), "ExtractMembers", { result, outputColumns->ChildPtr(1) });
        }
    }
    return result;
}

TExprNode::TPtr TAggregateExpander::ExpandAggregateWithFullOutput()
{
    Suffix_ = Node_->Content();
    YQL_ENSURE(Suffix_.SkipPrefix("Aggregate"));
    AggList_ = Node_->HeadPtr();
    KeyColumns_ = Node_->ChildPtr(1);
    AggregatedColumns_ = Node_->Child(2);
    auto settings = Node_->Child(3);

    bool allTraitsCollected = CollectTraits();
    if (HasSetting(*settings, "hopping")) {
        Ctx_.AddError(TIssue(Ctx_.GetPosition(settings->Pos()), "Aggregate with hopping is not supported"));
        return nullptr;
    }

    HaveDistinct_ = AnyOf(AggregatedColumns_->ChildrenList(),
        [](const auto& child) { return child->ChildrenSize() == 3; });
    EffectiveCompact_ = (HaveDistinct_ && CompactForDistinct_ && !UseBlocks_) || ForceCompact_ || HasSetting(*settings, "compact");
    for (const auto& trait : Traits_) {
        auto mergeLambda = trait->Child(5);
        if (mergeLambda->Tail().IsCallable("Void")) {
            EffectiveCompact_ = true;
            break;
        }
    }

    if (Suffix_ == "Finalize") {
        EffectiveCompact_ = true;
        Suffix_ = "";
    } else if (!Suffix_.empty()) {
        EffectiveCompact_ = false;
    }

    OriginalRowType_ = GetSeqItemType(*Node_->Head().GetTypeAnn()).Cast<TStructExprType>();
    RowItems_ = OriginalRowType_->GetItems();

    ProcessSessionSetting(GetSetting(*settings, "session"));
    RowType_ = Ctx_.MakeType<TStructExprType>(RowItems_);

    TVector<const TTypeAnnotationNode*> keyItemTypes = GetKeyItemTypes();
    bool needPickle = IsNeedPickle(keyItemTypes);
    auto keyExtractor = GetKeyExtractor(needPickle);
    CollectColumnsSpecs();

    if (Suffix_.empty() && !HaveSessionSetting_ && !EffectiveCompact_ && UsePhases_) {
        return GeneratePhases();
    }

    if (UseBlocks_) {
        if (Suffix_ == "Combine") {
            auto ret = TryGenerateBlockCombine();
            if (ret) {
                return ret;
            }
        }

        if (Suffix_ == "MergeFinalize" || Suffix_ == "MergeManyFinalize") {
            auto ret = TryGenerateBlockMergeFinalize();
            if (ret) {
                return ret;
            }
        }
    }

    if (!allTraitsCollected) {
        return RebuildAggregate();
    }

    BuildNothingStates();
    if (Suffix_ == "MergeState" || Suffix_ == "MergeFinalize" || Suffix_ == "MergeManyFinalize") {
        return GeneratePostAggregate(AggList_, keyExtractor);
    }

    TExprNode::TPtr preAgg = GeneratePartialAggregate(keyExtractor, keyItemTypes, needPickle);
    if (EffectiveCompact_ || !preAgg) {
        preAgg = std::move(AggList_);
    }

    if (Suffix_ == "Combine" || Suffix_ == "CombineState") {
        return preAgg;
    }

    return GeneratePostAggregate(preAgg, keyExtractor);
}

TExprNode::TPtr TAggregateExpander::ExpandAggApply(const TExprNode::TPtr& node)
{
    auto name = node->Head().Content();
    if (name.StartsWith("pg_")) {
        auto func = name.SubStr(3);
        auto itemType = node->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        TVector<ui32> argTypes;
        bool needRetype = false;
        bool isUniversal;
        auto status = ExtractPgTypesFromMultiLambda(node->ChildRef(2), argTypes, needRetype, Ctx_, isUniversal);
        YQL_ENSURE(!isUniversal);
        YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);

        const NPg::TAggregateDesc* aggDescPtr;
        if (node->Content().EndsWith("State")) {
            auto stateType = node->Child(2)->GetTypeAnn()->Cast<TPgExprType>()->GetId();
            auto resultType = node->GetTypeAnn()->Cast<TPgExprType>()->GetId();
            aggDescPtr = &NPg::LookupAggregation(TString(func), stateType, resultType);
        } else {
            aggDescPtr = &NPg::LookupAggregation(TString(func), argTypes);
        }

        return ExpandPgAggregationTraits(node->Pos(), *aggDescPtr, /*onWindow=*/false, node->ChildPtr(2), argTypes, itemType, Ctx_);
    }

    const TString modulePath = "/lib/yql/aggregate.yqls";
    auto exportsPtr = TypesCtx_.Modules->GetModule(modulePath);
    YQL_ENSURE(exportsPtr, "Failed to get module " << modulePath);
    const auto& exports = exportsPtr->Symbols();
    const auto ex = exports.find(TString(name) + "_traits_factory");
    YQL_ENSURE(exports.cend() != ex);
    TNodeOnNodeOwnedMap deepClones;
    auto lambda = Ctx_.DeepCopy(*ex->second, exportsPtr->ExprCtx(), deepClones, /*internStrings=*/true, /*copyTypes=*/false);

    auto listTypeNode = Ctx_.NewCallable(node->Pos(), "ListType", { node->ChildPtr(node->ChildrenSize() == 4 && !node->Child(3)->IsCallable("Void") ? 3 : 1) });
    auto extractor = node->ChildPtr(2);

    auto traits = Ctx_.ReplaceNodes(lambda->TailPtr(), {
        {lambda->Head().Child(0), listTypeNode},
        {lambda->Head().Child(1), extractor}
        });

    Ctx_.Step.Repeat(TExprStep::ExpandApplyForLambdas);
    auto status = ExpandApplyNoRepeat(traits, traits, Ctx_);
    YQL_ENSURE(status != IGraphTransformer::TStatus::Error);
    return traits;
}

bool TAggregateExpander::CollectTraits() {
    bool allTraitsCollected = true;
    for (ui32 index = 0; index < AggregatedColumns_->ChildrenSize(); ++index) {
        auto trait = AggregatedColumns_->Child(index)->ChildPtr(1);
        if (trait->IsCallable({ "AggApply", "AggApplyState", "AggApplyManyState" })) {
            trait = ExpandAggApply(trait);
            allTraitsCollected = false;
        }
        Traits_.push_back(trait);
    }
    return allTraitsCollected;
}

TExprNode::TPtr TAggregateExpander::RebuildAggregate()
{
    TExprNode::TListType newAggregatedColumnsItems = AggregatedColumns_->ChildrenList();
    for (ui32 index = 0; index < AggregatedColumns_->ChildrenSize(); ++index) {
        auto trait = AggregatedColumns_->Child(index)->ChildPtr(1);
        if (trait->IsCallable("AggApply")) {
            newAggregatedColumnsItems[index] = Ctx_.ChangeChild(*(newAggregatedColumnsItems[index]), 1, std::move(Traits_[index]));
        } else if (trait->IsCallable("AggApplyState") || trait->IsCallable("AggApplyManyState")) {
            // clang-format off
            auto newTrait = Ctx_.Builder(Node_->Pos())
                .Callable("AggregationTraits")
                    .Add(0, trait->ChildPtr(1))
                    .Add(1, trait->ChildPtr(2)) // extractor for state, not initial value itself
                    .Lambda(2)
                        .Param("item")
                        .Param("state")
                        .Callable("Void")
                        .Seal()
                    .Seal()
                    .Add(3, Traits_[index]->ChildPtr(3))
                    .Add(4, Traits_[index]->ChildPtr(4))
                    .Add(5, Traits_[index]->ChildPtr(5))
                    .Add(6, Traits_[index]->ChildPtr(6))
                    .Add(7, Traits_[index]->ChildPtr(7))
                .Seal()
                .Build();
            // clang-format on

            newAggregatedColumnsItems[index] = Ctx_.ChangeChild(*(newAggregatedColumnsItems[index]), 1, std::move(newTrait));
        }
    }

    return Ctx_.ChangeChild(*Node_, 2, Ctx_.NewList(Node_->Pos(), std::move(newAggregatedColumnsItems)));
}

TExprNode::TPtr TAggregateExpander::GetContextLambda()
{
    return HasContextFuncs(*AggregatedColumns_) ?
        // clang-format off
        Ctx_.Builder(Node_->Pos())
            .Lambda()
                .Param("stream")
                .Callable("WithContext")
                    .Arg(0, "stream")
                    .Atom(1, "Agg")
                .Seal()
            .Seal()
            .Build() :
        Ctx_.Builder(Node_->Pos())
            .Lambda()
                .Param("stream")
                .Arg("stream")
            .Seal()
            .Build();
        // clang-format on
}

void TAggregateExpander::ProcessSessionSetting(TExprNode::TPtr sessionSetting)
{
    if (!sessionSetting) {
        return;
    }
    HaveSessionSetting_ = true;

    YQL_ENSURE(sessionSetting->Child(1)->Child(0)->IsAtom());
    SessionOutputColumn_ = sessionSetting->Child(1)->Child(0)->Content();

    // remove session column from other keys
    TExprNodeList keyColumnsList = KeyColumns_->ChildrenList();
    EraseIf(keyColumnsList, [&](const auto& key) { return SessionOutputColumn_ == key->Content(); });
    KeyColumns_ = Ctx_.NewList(KeyColumns_->Pos(), std::move(keyColumnsList));

    SessionWindowParams_.Traits = sessionSetting->Child(1)->ChildPtr(1);
    ExtractSessionWindowParams(Node_->Pos(), SessionWindowParams_, Ctx_);
    ExtractSortKeyAndOrder(Node_->Pos(), SessionWindowParams_.SortTraits, SortParams_, Ctx_);

    if (HaveDistinct_) {
        auto keySelector = BuildKeySelector(Node_->Pos(), *OriginalRowType_, KeyColumns_, Ctx_);
        const auto sessionStartMemberLambda = AddSessionParamsMemberLambda(Node_->Pos(), SessionStartMemberName, keySelector,
            SessionWindowParams_, Ctx_);

        // clang-format off
        AggList_ = Ctx_.Builder(Node_->Pos())
            .Callable("PartitionsByKeys")
                .Add(0, AggList_)
                .Add(1, keySelector)
                .Add(2, SortParams_.Order)
                .Add(3, SortParams_.Key)
                .Lambda(4)
                    .Param("partitionedStream")
                    .Apply(sessionStartMemberLambda)
                        .With(0, "partitionedStream")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        // clang-format on

        auto keyColumnsList = KeyColumns_->ChildrenList();
        keyColumnsList.push_back(Ctx_.NewAtom(Node_->Pos(), SessionStartMemberName));
        KeyColumns_ = Ctx_.NewList(Node_->Pos(), std::move(keyColumnsList));

        RowItems_.push_back(Ctx_.MakeType<TItemExprType>(SessionStartMemberName, SessionWindowParams_.KeyType));

        SessionWindowParams_.Reset();
        SortParams_.Key = SortParams_.Order = VoidNode_;
    } else {
        EffectiveCompact_ = true;
    }
}

TVector<const TTypeAnnotationNode*> TAggregateExpander::GetKeyItemTypes()
{
    TVector<const TTypeAnnotationNode*> keyItemTypes;
    for (auto keyColumn : KeyColumns_->Children()) {
        auto index = RowType_->FindItem(keyColumn->Content());
        YQL_ENSURE(index, "Unknown column: " << keyColumn->Content());
        auto type = RowType_->GetItems()[*index]->GetItemType();
        keyItemTypes.push_back(type);

    }
    return keyItemTypes;
}

bool TAggregateExpander::IsNeedPickle(const TVector<const TTypeAnnotationNode*>& keyItemTypes)
{
    bool needPickle = false;
    for (auto type : keyItemTypes) {
        needPickle |= !IsDataOrOptionalOfData(type);
    }
    return needPickle;
}

TExprNode::TPtr TAggregateExpander::GetKeyExtractor(bool needPickle)
{
    // clang-format off
    TExprNode::TPtr keyExtractor = Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("item")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                // clang-format on
                if (KeyColumns_->ChildrenSize() == 0) {
                    return parent.Callable("Uint32").Atom(0, "0", TNodeFlags::Default).Seal();
                }
                else if (KeyColumns_->ChildrenSize() == 1) {
                    return parent.Callable("Member").Arg(0, "item").Add(1, KeyColumns_->HeadPtr()).Seal();
                }
                else {
                    auto listBuilder = parent.List();
                    ui32 pos = 0;
                    for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                        // clang-format off
                        listBuilder
                            .Callable(pos++, "Member")
                                .Arg(0, "item")
                                .Add(1, KeyColumns_->ChildPtr(i))
                            .Seal();
                        // clang-format on
                    }
                    return listBuilder.Seal();
                }
            // clang-format off
            })
        .Seal()
        .Build();
    // clang-format on

    if (needPickle) {
        // clang-format off
        keyExtractor = Ctx_.Builder(Node_->Pos())
            .Lambda()
                .Param("item")
                .Callable("StablePickle")
                    .Apply(0, *keyExtractor)
                        .With(0, "item")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        // clang-format on
    }
    return keyExtractor;
}

void TAggregateExpander::CollectColumnsSpecs()
{
    for (ui32 index = 0; index < AggregatedColumns_->ChildrenSize(); ++index) {
        auto child = AggregatedColumns_->Child(index);
        if (const auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr) {
            const auto ins = Distinct2Columns_.emplace(distinctField->Content(), TIdxSet());
            if (ins.second) {
                DistinctFields_.emplace_back(distinctField);
            }
            ins.first->second.insert(InitialColumnNames_.size());
        } else {
            NonDistinctColumns_.insert(InitialColumnNames_.size());
        }

        if (child->Head().IsAtom()) {
            FinalColumnNames_.push_back(child->HeadPtr());
        } else {
            FinalColumnNames_.push_back(child->Head().HeadPtr());
        }

        InitialColumnNames_.push_back(Ctx_.NewAtom(FinalColumnNames_.back()->Pos(), "_yql_agg_" + ToString(InitialColumnNames_.size()), TNodeFlags::Default));
    }
}

void TAggregateExpander::BuildNothingStates()
{
    for (ui32 index = 0; index < AggregatedColumns_->ChildrenSize(); ++index) {
        auto trait = Traits_[index];
        auto saveLambda = trait->Child(3);
        auto saveLambdaType = saveLambda->GetTypeAnn();
        auto typeNode = ExpandType(Node_->Pos(), *saveLambdaType, Ctx_);
        // clang-format off
        NothingStates_.push_back(Ctx_.Builder(Node_->Pos())
            .Callable("Nothing")
                .Callable(0, "OptionalType")
                    .Add(0, std::move(typeNode))
                .Seal()
            .Seal()
            .Build()
        );
        // clang-format on
    }
}

TExprNode::TPtr TAggregateExpander::GeneratePartialAggregate(const TExprNode::TPtr& keyExtractor,
    const TVector<const TTypeAnnotationNode*>& keyItemTypes, bool needPickle)
{
    TExprNode::TPtr pickleTypeNode = nullptr;
    if (needPickle) {
        const TTypeAnnotationNode* pickleType = nullptr;
        pickleType = KeyColumns_->ChildrenSize() > 1 ? Ctx_.MakeType<TTupleExprType>(keyItemTypes) : keyItemTypes[0];
        pickleTypeNode = ExpandType(Node_->Pos(), *pickleType, Ctx_);
    }

    TExprNode::TPtr partialAgg = nullptr;
    if (!NonDistinctColumns_.empty()) {
        partialAgg = GeneratePartialAggregateForNonDistinct(keyExtractor, pickleTypeNode);
    }
    for (auto distinctField : DistinctFields_) {
        bool needDistinctPickle = EffectiveCompact_ ? false : needPickle;
        auto distinctGrouper = GenerateDistinctGrouper(distinctField, keyItemTypes, needDistinctPickle);

        if (!partialAgg) {
            partialAgg = std::move(distinctGrouper);
        } else {
            // clang-format off
            partialAgg = Ctx_.Builder(Node_->Pos())
                .Callable("Extend")
                    .Add(0, std::move(partialAgg))
                    .Add(1, std::move(distinctGrouper))
                .Seal()
                .Build();
            // clang-format on
        }
    }
    // If no aggregation functions then add additional combiner
    if (AggregatedColumns_->ChildrenSize() == 0 && KeyColumns_->ChildrenSize() > 0 && !SessionWindowParams_.Update) {
        if (!partialAgg) {
            partialAgg = AggList_;
        }

        auto uniqCombineInit = ReturnKeyAsIsForCombineInit(pickleTypeNode);
        // clang-format off
        auto uniqCombineUpdate = Ctx_.Builder(Node_->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Param("state")
                .Arg("state")
            .Seal()
            .Build();
        // clang-format on

        // Return state as-is
        // clang-format off
        auto uniqCombineSave = Ctx_.Builder(Node_->Pos())
            .Lambda()
                .Param("key")
                .Param("state")
                .Callable("Just")
                    .Arg(0, "state")
                .Seal()
            .Seal()
            .Build();
        // clang-format on

        // clang-format off
        partialAgg = Ctx_.Builder(Node_->Pos())
            .Callable("CombineByKey")
                .Add(0, std::move(partialAgg))
                .Add(1, PreMap_)
                .Add(2, keyExtractor)
                .Add(3, std::move(uniqCombineInit))
                .Add(4, std::move(uniqCombineUpdate))
                .Add(5, std::move(uniqCombineSave))
            .Seal()
            .Build();
        // clang-format on
    }
    return partialAgg;
}

std::function<TExprNodeBuilder& (TExprNodeBuilder&)> TAggregateExpander::GetPartialAggArgExtractor(ui32 i, bool deserialize) {
    return [&, i, deserialize](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
        auto trait = Traits_[i];
        auto extractorLambda = trait->Child(1);
        auto loadLambda = trait->Child(4);
        if (Suffix_ == "CombineState") {
            if (deserialize) {
                // clang-format off
                parent.Apply(*loadLambda)
                    .With(0)
                        .Apply(*extractorLambda)
                        .With(0)
                            .Callable("CastStruct")
                                .Arg(0, "item")
                                .Add(1, ExpandType(Node_->Pos(), *extractorLambda->Head().Head().GetTypeAnn(), Ctx_))
                            .Seal()
                        .Done()
                        .Seal()
                    .Done()
                    .Seal();
                // clang-format on
            } else {
                // clang-format off
                parent.Apply(*extractorLambda)
                    .With(0)
                        .Callable("CastStruct")
                            .Arg(0, "item")
                            .Add(1, ExpandType(Node_->Pos(), *extractorLambda->Head().Head().GetTypeAnn(), Ctx_))
                        .Seal()
                    .Done()
                    .Seal();
                // clang-format on
            }
        } else {
            // clang-format off
            parent.Callable("CastStruct")
                .Arg(0, "item")
                .Add(1, ExpandType(Node_->Pos(), *extractorLambda->Head().Head().GetTypeAnn(), Ctx_))
                .Seal();
            // clang-format on
        }

        return parent;
    };
}

TExprNode::TPtr TAggregateExpander::GetFinalAggStateExtractor(ui32 i) {
    auto trait = Traits_[i];
    if (Suffix_.StartsWith("Merge")) {
        auto lambda = trait->ChildPtr(1);
        if (!Suffix_.StartsWith("MergeMany")) {
            return lambda;
        }

        if (lambda->Tail().IsCallable("Unwrap")) {
            // clang-format off
            return Ctx_.Builder(Node_->Pos())
                .Lambda()
                .Param("item")
                .ApplyPartial(lambda->HeadPtr(), lambda->Tail().HeadPtr())
                    .With(0, "item")
                .Seal()
                .Seal()
                .Build();
            // clang-format on
        } else {
            // clang-format off
            return Ctx_.Builder(Node_->Pos())
                .Lambda()
                .Param("item")
                .Callable("Just")
                    .Apply(0, *lambda)
                        .With(0, "item")
                    .Seal()
                .Seal()
                .Seal()
                .Build();
            // clang-format on
        }
    }

    bool aggregateOnly = (!Suffix_.empty());
    const auto& columnNames = aggregateOnly ? FinalColumnNames_ : InitialColumnNames_;
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("item")
            .Callable("Member")
                .Arg(0, "item")
                .Add(1, columnNames[i])
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr TAggregateExpander::MakeInputBlocks(const TExprNode::TPtr& stream, TExprNode::TListType& keyIdxs,
    TVector<TString>& outputColumns, TExprNode::TListType& aggs, bool overState, bool many, ui32* streamIdxColumn) {
    TVector<TString> inputColumns;
    auto flow = Ctx_.NewCallable(Node_->Pos(), "ToFlow", { stream });
    for (ui32 i = 0; i < RowType_->GetSize(); ++i) {
        inputColumns.push_back(TString(RowType_->GetItems()[i]->GetName()));
    }

    auto wideFlow = MakeExpandMap(Node_->Pos(), inputColumns, flow, Ctx_);

    TExprNode::TListType extractorArgs;
    TExprNode::TListType newRowItems;
    for (ui32 i = 0; i < RowType_->GetSize(); ++i) {
        extractorArgs.push_back(Ctx_.NewArgument(Node_->Pos(), "field" + ToString(i)));
        newRowItems.push_back(Ctx_.NewList(Node_->Pos(), { Ctx_.NewAtom(Node_->Pos(), RowType_->GetItems()[i]->GetName()), extractorArgs.back() }));
    }

    const TExprNode::TPtr newRow = Ctx_.NewCallable(Node_->Pos(), "AsStruct", std::move(newRowItems));

    TExprNode::TListType extractorRoots;
    TVector<const TTypeAnnotationNode*> allKeyTypes;
    for (ui32 index = 0; index < KeyColumns_->ChildrenSize(); ++index) {
        auto keyName = KeyColumns_->Child(index)->Content();
        auto rowIndex = RowType_->FindItem(keyName);
        YQL_ENSURE(rowIndex, "Unknown column: " << keyName);
        auto type = RowType_->GetItems()[*rowIndex]->GetItemType();
        extractorRoots.push_back(extractorArgs[*rowIndex]);

        allKeyTypes.push_back(type);
        keyIdxs.push_back(Ctx_.NewAtom(Node_->Pos(), ToString(index)));
        outputColumns.push_back(TString(keyName));
    }

    if (many) {
        auto rowIndex = RowType_->FindItem("_yql_group_stream_index");
        if (!rowIndex) {
            return nullptr;
        }
        if (streamIdxColumn) {
            *streamIdxColumn = extractorRoots.size();
        }

        extractorRoots.push_back(extractorArgs[*rowIndex]);
    }

    auto outputStructType = GetSeqItemType(*Node_->GetTypeAnn()).Cast<TStructExprType>();

    auto resolveStatus = TypesCtx_.ArrowResolver->AreTypesSupported(Ctx_.GetPosition(Node_->Pos()), allKeyTypes, Ctx_);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    if (resolveStatus != IArrowResolver::OK) {
        return nullptr;
    }

    for (ui32 index = 0; index < AggregatedColumns_->ChildrenSize(); ++index) {
        auto trait = AggregatedColumns_->Child(index)->ChildPtr(1);
        TVector<const TTypeAnnotationNode*> allTypes;

        const TTypeAnnotationNode* originalType = nullptr;
        if (overState && !trait->Child(3)->IsCallable("Void")) {
            auto originalExtractorType = trait->Child(3)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            originalType = GetOriginalResultType(trait->Pos(), many, originalExtractorType, Ctx_);
            YQL_ENSURE(originalType);
        }

        ui32 argsCount = trait->Child(2)->ChildrenSize() - 1;
        if (!overState && trait->Child(0)->Content() == "count_all") {
            argsCount = 0;
        }

        auto rowArg = &trait->Child(2)->Head().Head();
        const TNodeOnNodeOwnedMap remaps{ { rowArg, newRow } };

        TVector<TExprNode::TPtr> roots;
        for (ui32 i = 1; i < argsCount + 1; ++i) {
            auto root = trait->Child(2)->ChildPtr(i);
            allTypes.push_back(root->GetTypeAnn());

            auto status = RemapExpr(root, root, remaps, Ctx_, TOptimizeExprSettings(&TypesCtx_));

            YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error);
            roots.push_back(root);
        }

        // clang-format off
        aggs.push_back(Ctx_.Builder(Node_->Pos())
            .List()
                .Callable(0, TString("AggBlockApply") + (overState ? "State" : ""))
                    .Atom(0, trait->Child(0)->Content())
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        // clang-format on
                        if (overState) {
                            if (originalType) {
                                parent.Add(1, ExpandType(Node_->Pos(), *originalType, Ctx_));
                            } else {
                                // clang-format off
                                parent
                                    .Callable(1, "NullType")
                                    .Seal();
                                // clang-format on
                            }
                        }

                        return parent;
                    // clang-format off
                    })
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (ui32 i = 1; i < argsCount + 1; ++i) {
                            parent.Add(i + (overState ? 1 : 0), ExpandType(Node_->Pos(), *trait->Child(2)->Child(i)->GetTypeAnn(), Ctx_));
                        }

                        return parent;
                    })
                .Seal()
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 1; i < argsCount + 1; ++i) {
                        parent.Atom(i, ToString(extractorRoots.size() + i - 1));
                    }

                    return parent;
                })
            .Seal()
            .Build());
        // clang-format on

        for (auto root : roots) {
            if (many) {
                if (root->IsCallable("Unwrap")) {
                    root = root->HeadPtr();
                } else {
                    // clang-format off
                    root = Ctx_.Builder(Node_->Pos())
                        .Callable("Just")
                            .Add(0, root)
                        .Seal()
                        .Build();
                    // clang-format on
                }
            }

            extractorRoots.push_back(root);
        }

        auto outPos = outputStructType->FindItem(FinalColumnNames_[index]->Content());
        YQL_ENSURE(outPos);
        allTypes.push_back(outputStructType->GetItems()[*outPos]->GetItemType());
        auto resolveStatus = TypesCtx_.ArrowResolver->AreTypesSupported(Ctx_.GetPosition(Node_->Pos()), allTypes, Ctx_);
        YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
        if (resolveStatus != IArrowResolver::OK) {
            return nullptr;
        }

        outputColumns.push_back(TString(FinalColumnNames_[index]->Content()));
    }

    auto extractorLambda = Ctx_.NewLambda(Node_->Pos(), Ctx_.NewArguments(Node_->Pos(), std::move(extractorArgs)), std::move(extractorRoots));
    auto mappedWideFlow = Ctx_.NewCallable(Node_->Pos(), "WideMap", { wideFlow, extractorLambda });
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
        .Callable("WideToBlocks")
            .Callable(0, "FromFlow")
                .Add(0, mappedWideFlow)
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr TAggregateExpander::TryGenerateBlockCombineAllOrHashed() {
    if (!TypesCtx_.ArrowResolver) {
        return nullptr;
    }

    const bool hashed = (KeyColumns_->ChildrenSize() > 0);
    const bool isInputList = (AggList_->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List);

    TExprNode::TListType keyIdxs;
    TVector<TString> outputColumns;
    TExprNode::TListType aggs;
    TExprNode::TPtr stream = nullptr;
    if (isInputList) {
        stream = Ctx_.NewArgument(Node_->Pos(), "stream");
    } else {
        stream = AggList_;
    }

    TExprNode::TPtr blocks = MakeInputBlocks(stream, keyIdxs, outputColumns, aggs, /*overState=*/false, /*many=*/false);
    if (!blocks) {
        return nullptr;
    }

    TExprNode::TPtr aggWideFlow;
    if (hashed) {
        // clang-format off
        aggWideFlow = Ctx_.Builder(Node_->Pos())
            .Callable("ToFlow")
                .Callable(0, "WideFromBlocks")
                    .Callable(0, "BlockCombineHashed")
                        .Add(0, blocks)
                        .Callable(1, "Void")
                        .Seal()
                        .Add(2, Ctx_.NewList(Node_->Pos(), std::move(keyIdxs)))
                        .Add(3, Ctx_.NewList(Node_->Pos(), std::move(aggs)))
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        // clang-format on
    } else {
        // clang-format off
        aggWideFlow = Ctx_.Builder(Node_->Pos())
            .Callable("ToFlow")
                .Callable(0, "BlockCombineAll")
                    .Add(0, blocks)
                    .Callable(1, "Void")
                    .Seal()
                    .Add(2, Ctx_.NewList(Node_->Pos(), std::move(aggs)))
                .Seal()
            .Seal()
            .Build();
        // clang-format on
    }

    auto finalFlow = MakeNarrowMap(Node_->Pos(), outputColumns, aggWideFlow, Ctx_);
    if (isInputList) {
        auto root = Ctx_.NewCallable(Node_->Pos(), "FromFlow", { finalFlow });
        auto lambdaStream = Ctx_.NewLambda(Node_->Pos(), Ctx_.NewArguments(Node_->Pos(), { stream }), std::move(root));

        // clang-format off
        return Ctx_.Builder(Node_->Pos())
            .Callable("LMap")
                .Add(0, AggList_)
                .Lambda(1)
                    .Param("stream")
                    .Apply(GetContextLambda())
                        .With(0)
                            .Apply(lambdaStream)
                                .With(0, "stream")
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        // clang-format on
    } else {
        return finalFlow;
    }
}

TExprNode::TPtr TAggregateExpander::GeneratePartialAggregateForNonDistinct(const TExprNode::TPtr& keyExtractor, const TExprNode::TPtr& pickleTypeNode)
{
    bool combineOnly = Suffix_ == "Combine" || Suffix_ == "CombineState";
    const auto& columnNames = combineOnly ? FinalColumnNames_ : InitialColumnNames_;
    auto initLambdaIndex = (Suffix_ == "CombineState") ? 4 : 1;
    auto updateLambdaIndex = (Suffix_ == "CombineState") ? 5 : 2;

    // clang-format off
    auto combineInit = Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("key")
            .Param("item")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    ui32 ndx = 0;
                    for (ui32 i: NonDistinctColumns_) {
                        auto trait = Traits_[i];
                        auto initLambda = trait->Child(initLambdaIndex);
                        if (initLambda->Head().ChildrenSize() == 1) {
                            // clang-format off
                            parent.List(ndx++)
                                .Add(0, columnNames[i])
                                .Apply(1, *initLambda)
                                    .With(0)
                                        .Do(GetPartialAggArgExtractor(i, /*deserialize=*/false))
                                    .Done()
                                .Seal()
                            .Seal();
                            // clang-format on
                        } else {
                            // clang-format off
                            parent.List(ndx++)
                                .Add(0, columnNames[i])
                                .Apply(1, *initLambda)
                                    .With(0)
                                        .Do(GetPartialAggArgExtractor(i, /*deserialize=*/false))
                                    .Done()
                                    .With(1)
                                        .Callable("Uint32")
                                            .Atom(0, ToString(i), TNodeFlags::Default)
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal();
                            // clang-format on
                        }
                    }
                    return parent;
                // clang-format off
                })
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    auto combineUpdate = Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("key")
            .Param("item")
            .Param("state")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    ui32 ndx = 0;
                    for (ui32 i: NonDistinctColumns_) {
                        auto trait = Traits_[i];
                        auto updateLambda = trait->Child(updateLambdaIndex);
                        if (updateLambda->Head().ChildrenSize() == 2) {
                            // clang-format off
                            parent.List(ndx++)
                                .Add(0, columnNames[i])
                                .Apply(1, *updateLambda)
                                    .With(0)
                                        .Do(GetPartialAggArgExtractor(i, /*deserialize=*/true))
                                    .Done()
                                    .With(1)
                                        .Callable("Member")
                                            .Arg(0, "state")
                                            .Add(1, columnNames[i])
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal();
                            // clang-format on
                        } else {
                            // clang-format off
                            parent.List(ndx++)
                                .Add(0, columnNames[i])
                                .Apply(1, *updateLambda)
                                    .With(0)
                                        .Do(GetPartialAggArgExtractor(i, /*deserialize=*/true))
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
                            // clang-format on
                        }
                    }
                    return parent;
                // clang-format off
                })
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    auto combineSave = Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("key")
            .Param("state")
            .Callable("Just")
                .Callable(0, "AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        // clang-format on
                        for (ui32 i = 0; i < columnNames.size(); ++i) {
                            if (NonDistinctColumns_.find(i) == NonDistinctColumns_.end()) {
                                // clang-format off
                                parent.List(i)
                                    .Add(0, columnNames[i])
                                    .Add(1, NothingStates_[i])
                                .Seal();
                                // clang-format on
                            } else {
                                auto trait = Traits_[i];
                                auto saveLambda = trait->Child(3);
                                if (!DistinctFields_.empty()) {
                                    // clang-format off
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
                                    // clang-format on
                                } else {
                                    // clang-format off
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
                                    // clang-format on
                                }
                            }
                        }
                        return parent;
                    // clang-format off
                    })
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        // clang-format on
                        ui32 pos = 0;
                        for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                            auto listBuilder = parent.List(columnNames.size() + i);
                            listBuilder.Add(0, KeyColumns_->ChildPtr(i));
                            if (KeyColumns_->ChildrenSize() > 1) {
                                if (pickleTypeNode) {
                                    // clang-format off
                                    listBuilder
                                        .Callable(1, "Nth")
                                            .Callable(0, "Unpickle")
                                                .Add(0, pickleTypeNode)
                                                .Arg(1, "key")
                                            .Seal()
                                            .Atom(1, ToString(pos), TNodeFlags::Default)
                                        .Seal();
                                    // clang-format on
                                } else {
                                    // clang-format off
                                    listBuilder
                                        .Callable(1, "Nth")
                                            .Arg(0, "key")
                                            .Atom(1, ToString(pos), TNodeFlags::Default)
                                        .Seal();
                                    // clang-format on
                                }
                                ++pos;
                            } else {
                                if (pickleTypeNode) {
                                    // clang-format off
                                    listBuilder.Callable(1, "Unpickle")
                                        .Add(0, pickleTypeNode)
                                        .Arg(1, "key")
                                        .Seal();
                                    // clang-format on
                                } else {
                                    listBuilder.Arg(1, "key");
                                }
                            }
                            listBuilder.Seal();
                        }
                        return parent;
                    // clang-format off
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    return Ctx_.Builder(Node_->Pos())
        .Callable("CombineByKey")
            .Add(0, AggList_)
            .Add(1, PreMap_)
            .Add(2, keyExtractor)
            .Add(3, std::move(combineInit))
            .Add(4, std::move(combineUpdate))
            .Add(5, std::move(combineSave))
        .Seal()
        .Build();
    // clang-format on
}

void TAggregateExpander::GenerateInitForDistinct(TExprNodeBuilder& parent, ui32& ndx, const TIdxSet& indicies, const TExprNode::TPtr& distinctField) {
    for (ui32 i: indicies) {
        auto trait = Traits_[i];
        auto initLambda = trait->Child(1);
        if (initLambda->Head().ChildrenSize() == 1) {
            // clang-format off
            parent.List(ndx++)
                .Add(0, InitialColumnNames_[i])
                .Apply(1, *initLambda)
                    .With(0)
                        .Callable("Member")
                            .Arg(0, "item")
                            .Add(1, distinctField)
                        .Seal()
                    .Done()
                .Seal()
            .Seal();
            // clang-format on
        } else {
            // clang-format off
            parent.List(ndx++)
                .Add(0, InitialColumnNames_[i])
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
            // clang-format on
        }
    }
}

TExprNode::TPtr TAggregateExpander::GenerateDistinctGrouper(const TExprNode::TPtr& distinctField,
    const TVector<const TTypeAnnotationNode*>& keyItemTypes, bool needDistinctPickle)
{
    auto& indicies = Distinct2Columns_[distinctField->Content()];
    auto distinctIndex = RowType_->FindItem(distinctField->Content());
    YQL_ENSURE(distinctIndex, "Unknown field: " << distinctField->Content());
    auto distinctType = RowType_->GetItems()[*distinctIndex]->GetItemType();
    TVector<const TTypeAnnotationNode*> distinctKeyItemTypes = keyItemTypes;
    distinctKeyItemTypes.push_back(distinctType);
    auto valueType = distinctType;
    if (distinctType->GetKind() == ETypeAnnotationKind::Optional) {
        distinctType = distinctType->Cast<TOptionalExprType>()->GetItemType();
    }

    if (distinctType->GetKind() != ETypeAnnotationKind::Data) {
        needDistinctPickle = true;
        valueType = Ctx_.MakeType<TDataExprType>(EDataSlot::String);
    }

    const auto expandedValueType = needDistinctPickle ?
        // clang-format off
        Ctx_.Builder(Node_->Pos())
            .Callable("DataType")
                .Atom(0, "String", TNodeFlags::Default)
            .Seal()
        .Build()
        : ExpandType(Node_->Pos(), *valueType, Ctx_);
        // clang-format on

    DistinctFieldNeedsPickle_[distinctField->Content()] = needDistinctPickle;
    // clang-format off
    auto udfSetCreateValue = Ctx_.Builder(Node_->Pos())
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
    // clang-format on

    UdfSetCreate_[distinctField->Content()] = udfSetCreateValue;
    // clang-format off
    auto resourceType = Ctx_.Builder(Node_->Pos())
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
    // clang-format on

    // clang-format off
    UdfAddValue_[distinctField->Content()] = Ctx_.Builder(Node_->Pos())
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
    // clang-format on

    // clang-format off
    UdfWasChanged_[distinctField->Content()] = Ctx_.Builder(Node_->Pos())
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
    // clang-format on

    // clang-format off
    auto distinctKeyExtractor = Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("item")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                // clang-format on
                if (KeyColumns_->ChildrenSize() != 0) {
                    auto listBuilder = parent.List();
                    ui32 pos = 0;
                    for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                        // clang-format off
                        listBuilder
                            .Callable(pos++, "Member")
                                .Arg(0, "item")
                                .Add(1, KeyColumns_->ChildPtr(i))
                            .Seal();
                        // clang-format on
                    }
                    // clang-format off
                    listBuilder
                        .Callable(pos, "Member")
                            .Arg(0, "item")
                            .Add(1, distinctField)
                        .Seal();
                    // clang-format on

                    return listBuilder.Seal();
                } else {
                    // clang-format off
                    return parent
                        .Callable("Member")
                            .Arg(0, "item")
                            .Add(1, distinctField)
                        .Seal();
                    // clang-format on
                }
            // clang-format off
            })
        .Seal()
        .Build();
    // clang-format on

    const TTypeAnnotationNode* distinctPickleType = nullptr;
    TExprNode::TPtr distinctPickleTypeNode;
    if (needDistinctPickle) {
        distinctPickleType = KeyColumns_->ChildrenSize() > 0  ? Ctx_.MakeType<TTupleExprType>(distinctKeyItemTypes) : distinctKeyItemTypes.front();
        distinctPickleTypeNode = ExpandType(Node_->Pos(), *distinctPickleType, Ctx_);
    }

    if (needDistinctPickle) {
        // clang-format off
        distinctKeyExtractor = Ctx_.Builder(Node_->Pos())
            .Lambda()
                .Param("item")
                .Callable("StablePickle")
                    .Apply(0, *distinctKeyExtractor).With(0, "item").Seal()
                .Seal()
            .Seal()
            .Build();
        // clang-format on
    }

    // clang-format off
    auto distinctCombineInit = Ctx_.Builder(Node_->Pos())
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
    // clang-format on

    // clang-format off
    auto distinctCombineUpdate = Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("key")
            .Param("item")
            .Param("state")
            .Arg("state")
        .Seal()
        .Build();
    // clang-format on

    ui32 ndx = 0;
    // clang-format off
    auto distinctCombineSave = Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("key")
            .Param("state")
            .Callable("Just")
                .Callable(0, "AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        // clang-format on
                        for (ui32 i: indicies) {
                            auto trait = Traits_[i];
                            auto saveLambda = trait->Child(3);
                            // clang-format off
                            parent.List(ndx++)
                                .Add(0, InitialColumnNames_[i])
                                .Apply(1, *saveLambda)
                                    .With(0)
                                        .Callable("Member")
                                            .Arg(0, "state")
                                            .Add(1, InitialColumnNames_[i])
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal();
                            // clang-format on
                        }
                        return parent;
                    // clang-format off
                    })
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        // clang-format on
                        if (KeyColumns_->ChildrenSize() > 0) {
                            if (needDistinctPickle) {
                                ui32 pos = 0;
                                for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                                    // clang-format off
                                    parent.List(ndx++)
                                        .Add(0, KeyColumns_->ChildPtr(i))
                                        .Callable(1, "Nth")
                                            .Callable(0, "Unpickle")
                                                .Add(0, distinctPickleTypeNode)
                                                .Arg(1, "key")
                                            .Seal()
                                            .Atom(1, ToString(pos++), TNodeFlags::Default)
                                        .Seal()
                                        .Seal();
                                    // clang-format on
                                }
                                // clang-format off
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
                                // clang-format on

                            } else {
                                ui32 pos = 0;
                                for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                                    // clang-format off
                                    parent.List(ndx++)
                                        .Add(0, KeyColumns_->ChildPtr(i))
                                        .Callable(1, "Nth")
                                            .Arg(0, "key")
                                            .Atom(1, ToString(pos++), TNodeFlags::Default)
                                        .Seal()
                                        .Seal();
                                    // clang-format on
                                }
                                // clang-format off
                                parent.List(ndx++)
                                    .Add(0, distinctField)
                                    .Callable(1, "Nth")
                                        .Arg(0, "key")
                                        .Atom(1, ToString(pos++), TNodeFlags::Default)
                                    .Seal()
                                .Seal();
                                // clang-format on
                            }
                        } else {
                            if (needDistinctPickle) {
                                // clang-format off
                                parent.List(ndx++)
                                    .Add(0, distinctField)
                                    .Callable(1, "Unpickle")
                                        .Add(0, distinctPickleTypeNode)
                                        .Arg(1, "key")
                                    .Seal()
                                .Seal();
                                // clang-format on
                            } else {
                                // clang-format off
                                parent.List(ndx++)
                                    .Add(0, distinctField)
                                    .Arg(1, "key")
                                .Seal();
                                // clang-format on
                            }
                        }
                        return parent;
                    // clang-format off
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    auto distinctCombiner = Ctx_.Builder(Node_->Pos())
        .Callable("CombineByKey")
            .Add(0, AggList_)
            .Add(1, PreMap_)
            .Add(2, distinctKeyExtractor)
            .Add(3, std::move(distinctCombineInit))
            .Add(4, std::move(distinctCombineUpdate))
            .Add(5, std::move(distinctCombineSave))
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    auto distinctGrouper = Ctx_.Builder(Node_->Pos())
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
                                // clang-format on
                                for (ui32 i = 0; i < InitialColumnNames_.size(); ++i) {
                                    if (indicies.find(i) != indicies.end()) {
                                        // clang-format off
                                        parent.List(i)
                                            .Add(0, InitialColumnNames_[i])
                                            .Callable(1, "Just")
                                                .Callable(0, "Member")
                                                    .Arg(0, "state")
                                                    .Add(1, InitialColumnNames_[i])
                                                .Seal()
                                            .Seal()
                                        .Seal();
                                        // clang-format on
                                    } else {
                                        // clang-format off
                                        parent.List(i)
                                            .Add(0, InitialColumnNames_[i])
                                            .Add(1, NothingStates_[i])
                                        .Seal();
                                        // clang-format on
                                    }
                                }
                                return parent;
                            // clang-format off
                            })
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                // clang-format on
                                if (KeyColumns_->ChildrenSize() > 0) {
                                    for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                                        // clang-format off
                                        parent.List(InitialColumnNames_.size() + i)
                                            .Add(0, KeyColumns_->ChildPtr(i))
                                            .Callable(1, "Member")
                                                .Arg(0, "state")
                                                .Add(1, KeyColumns_->ChildPtr(i))
                                            .Seal().Seal();
                                        // clang-format on
                                    }
                                }
                                return parent;
                            // clang-format off
                            })
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // clang-format on
    return distinctGrouper;
}

TExprNode::TPtr TAggregateExpander::ReturnKeyAsIsForCombineInit(const TExprNode::TPtr& pickleTypeNode)
{
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
            .Lambda()
                .Param("key")
                .Param("item")
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        // clang-format on
                        ui32 pos = 0;
                        for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                            auto listBuilder = parent.List(i);
                            listBuilder.Add(0, KeyColumns_->Child(i));
                            if (KeyColumns_->ChildrenSize() > 1) {
                                if (pickleTypeNode) {
                                    // clang-format off
                                    listBuilder
                                        .Callable(1, "Nth")
                                            .Callable(0, "Unpickle")
                                                .Add(0, pickleTypeNode)
                                                .Arg(1, "key")
                                            .Seal()
                                        .Atom(1, ToString(pos++), TNodeFlags::Default)
                                        .Seal();
                                    // clang-format on
                                } else {
                                    // clang-format off
                                    listBuilder
                                        .Callable(1, "Nth")
                                            .Arg(0, "key")
                                            .Atom(1, ToString(pos++), TNodeFlags::Default)
                                        .Seal();
                                    // clang-format on
                                }
                            } else {
                                if (pickleTypeNode) {
                                    // clang-format off
                                    listBuilder.Callable(1, "Unpickle")
                                        .Add(0, pickleTypeNode)
                                        .Arg(1, "key")
                                    .Seal();
                                    // clang-format on
                                } else {
                                    listBuilder.Arg(1, "key");
                                }
                            }
                            listBuilder.Seal();
                        }
                        return parent;
                    // clang-format off
                    })
                .Seal()
            .Seal()
            .Build();
    // clang-format on
}

TExprNode::TPtr TAggregateExpander::BuildFinalizeByKeyLambda(const TExprNode::TPtr& preprocessLambda, const TExprNode::TPtr& keyExtractor) {
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
    .Lambda()
        .Param("stream")
        .Callable("FinalizeByKey")
            .Arg(0, "stream")
            .Lambda(1)
                .Param("item")
                .Callable("Just")
                    .Apply(0, preprocessLambda)
                        .With(0, "item")
                    .Seal()
                .Seal()
            .Seal()
            .Add(2, keyExtractor)
            .Lambda(3)
                .Param("key")
                .Param("item")
                .Apply(GeneratePostAggregateInitPhase())
                    .With(0, "item")
                .Seal()
            .Seal()
            .Lambda(4)
                .Param("key")
                .Param("item")
                .Param("state")
                .Apply(GeneratePostAggregateMergePhase())
                    .With(0, "item")
                    .With(1, "state")
                .Seal()
            .Seal()
            .Lambda(5)
                .Param("key")
                .Param("state")
                .Apply(GeneratePostAggregateSavePhase())
                    .With(0, "state")
                .Seal()
            .Seal()
        .Seal()
    .Seal().Build();
    // clang-format on
}


TExprNode::TPtr TAggregateExpander::CountAggregateRewrite(const NNodes::TCoAggregate& node, TExprContext& ctx, bool useBlocks) {
    auto keyColumns = node.Keys();
    auto aggregatedColumns = node.Handlers();
    if (keyColumns.Size() > 0 || aggregatedColumns.Size() != 1) {
        return node.Ptr();
    }

    auto settings = node.Settings();
    auto hoppingSetting = GetSetting(settings.Ref(), "hopping");
    if (hoppingSetting) {
        return node.Ptr();
    }

    if (GetSetting(settings.Ref(), "session")) {
        // TODO: support
        return node.Ptr();
    }

    auto aggregatedColumn = aggregatedColumns.Item(0);
    const bool isDistinct = (aggregatedColumn.Ref().ChildrenSize() == 3);

    auto traits = aggregatedColumn.Ref().Child(1);
    auto outputColumn = aggregatedColumn.Ref().HeadPtr();

    // validation of traits
    const TTypeAnnotationNode* inputItemType;
    bool onlyColumn = true;
    bool onlyZero = true;
    TExprNode::TPtr initVal;
    if (traits->IsCallable("AggregationTraits")) {
        inputItemType = traits->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();

        auto init = NNodes::TCoLambda(traits->Child(1));
        TExprNode::TPtr updateVal;
        if (init.Body().Ref().IsCallable("Uint64") &&
            init.Body().Ref().Head().Content() == "1") {
            onlyZero = false;
        } else if (init.Body().Ref().IsCallable("Uint64") &&
            init.Body().Ref().Head().Content() == "0") {
            onlyColumn = false;
        } else if (init.Body().Ref().IsCallable("AggrCountInit")) {
            initVal = init.Body().Ref().HeadPtr();
            onlyColumn = onlyColumn && init.Body().Ref().Child(0) == init.Args().Arg(0).Raw();
            onlyZero = false;
        } else {
            return node.Ptr();
        }

        auto update = NNodes::TCoLambda(traits->Child(2));
        auto inc = update.Body().Ptr();
        if (inc->IsCallable("Inc") && inc->Child(0) == update.Args().Arg(1).Raw()) {
            onlyZero = false;
        } else if (inc->IsCallable("AggrCountUpdate") && inc->Child(1) == update.Args().Arg(1).Raw()) {
            updateVal = inc->HeadPtr();
            onlyColumn = onlyColumn && inc->Child(0) == update.Args().Arg(0).Raw();
            onlyZero = false;
        } else if (inc == update.Args().Arg(1).Raw()) {
            onlyColumn = false;
        } else {
            return node.Ptr();
        }

        auto save = NNodes::TCoLambda(traits->Child(3));
        if (save.Body().Raw() != save.Args().Arg(0).Raw()) {
            return node.Ptr();
        }

        auto load = NNodes::TCoLambda(traits->Child(4));
        if (load.Body().Raw() != load.Args().Arg(0).Raw()) {
            return node.Ptr();
        }

        auto merge = NNodes::TCoLambda(traits->Child(5));
        {
            auto& plus = merge.Body().Ref();
            if (!plus.IsCallable({ "+", "AggrAdd" }) ) {
                return node.Ptr();
            }

            if (!(plus.Child(0) == merge.Args().Arg(0).Raw() &&
                plus.Child(1) == merge.Args().Arg(1).Raw())) {
                return node.Ptr();
            }
        }

        auto finish = NNodes::TCoLambda(traits->Child(6));
        if (finish.Body().Raw() != finish.Args().Arg(0).Raw()) {
            return node.Ptr();
        }

        auto defVal = traits->Child(7);
        if (!defVal->IsCallable("Uint64") || defVal->Head().Content() != "0") {
            return node.Ptr();
        }

        if (!isDistinct) {
            if (!onlyZero && !onlyColumn) {
                if (!initVal || !updateVal || initVal != updateVal) {
                    return node.Ptr();
                }
            }
        }
    } else if (traits->IsCallable("AggApply")) {
        if (traits->Head().Content() != "count_all" && traits->Head().Content() != "count") {
            return node.Ptr();
        }

        inputItemType = traits->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        onlyZero = false;
        onlyColumn = false;
        if (&traits->Child(2)->Head().Head() == &traits->Child(2)->Tail()) {
            onlyColumn = true;
        }

        if (!isDistinct) {
            if (traits->Head().Content() == "count") {
                initVal = traits->Child(2)->TailPtr();
                if (initVal->GetTypeAnn()->IsOptionalOrNull()) {
                    if (IsDepended(traits->Child(2)->Tail(), traits->Child(2)->Head().Head())) {
                        return node.Ptr();
                    }
                } else {
                    initVal = nullptr;
                }
            }
        }
    } else {
        return node.Ptr();
    }

    const bool isOptionalColumn = inputItemType->GetKind() == ETypeAnnotationKind::Optional;

    if (!isDistinct) {
        // clang-format off
        auto length = ctx.Builder(node.Pos())
            .Callable("Length")
                .Add(0, node.Input().Ptr())
            .Seal()
            .Build();
        // clang-format on

        if (onlyZero) {
            // clang-format off
            length = ctx.Builder(node.Pos())
                .Callable("Uint64")
                    .Atom(0, "0", TNodeFlags::Default)
                .Seal()
                .Build();
            // clang-format on
        } else if (!onlyColumn && initVal) {
            // clang-format off
            length = ctx.Builder(node.Pos())
                .Callable("If")
                    .Callable(0, "Exists")
                        .Add(0, initVal)
                    .Seal()
                    .Add(1, std::move(length))
                    .Callable(2, "Uint64")
                        .Atom(0, "0", TNodeFlags::Default)
                    .Seal()
                .Seal()
                .Build();
            // clang-format on
        }

        // clang-format off
        auto ret = ctx.Builder(node.Pos())
            .Callable("AsList")
                .Callable(0, "AsStruct")
                    .List(0)
                        .Add(0, std::move(outputColumn))
                        .Add(1, std::move(length))
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        // clang-format on

        return ret;
    }

    if (useBlocks || !onlyColumn) {
        return node.Ptr();
    }
    auto removedOptionalType = inputItemType;
    if (isOptionalColumn) {
        removedOptionalType = removedOptionalType->Cast<TOptionalExprType>()->GetItemType();
    }

    const bool needPickle = removedOptionalType->GetKind() != ETypeAnnotationKind::Data;
    auto pickleTypeNode = ExpandType(node.Pos(), *inputItemType, ctx);

    auto distictColumn = aggregatedColumn.Ref().ChildPtr(2);
    // clang-format off
    auto combine = ctx.Builder(node.Pos())
        .Callable("CombineByKey")
            .Callable(0, "ExtractMembers")
                .Add(0, node.Input().Ptr())
                .List(1)
                    .Add(0, distictColumn)
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("row")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    if (isOptionalColumn) {
                        // clang-format off
                        parent.Callable("Map")
                            .Callable(0, "Member")
                                .Arg(0, "row")
                                .Add(1, distictColumn)
                            .Seal()
                            .Lambda(1)
                                .Param("unpacked")
                                .Arg("unpacked")
                            .Seal()
                        .Seal();
                        // clang-format on
                    } else {
                        // clang-format off
                        parent.Callable("Just")
                            .Callable(0, "Member")
                                .Arg(0, "row")
                                .Add(1, distictColumn)
                            .Seal()
                        .Seal();
                        // clang-format on
                    }

                    return parent;
                // clang-format off
                })
            .Seal()
            .Lambda(2)
                .Param("item")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    if (needPickle) {
                        // clang-format off
                        parent.Callable("StablePickle")
                            .Arg(0, "item")
                            .Seal();
                        // clang-format on
                    } else {
                        parent.Arg("item");
                    }
                    return parent;
                // clang-format off
                })
            .Seal()
            .Lambda(3)
                .Param("key")
                .Param("item")
                .Callable("Void")
                .Seal()
            .Seal()
            .Lambda(4)
                .Param("key")
                .Param("item")
                .Param("state")
                .Arg("state")
            .Seal()
            .Lambda(5)
                .Param("key")
                .Param("state")
                .Callable("Just")
                    .Callable(0, "AsStruct")
                        .List(0)
                            .Atom(0, "value")
                            .Arg(1, "key")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    auto groupByKey = ctx.Builder(node.Pos())
        .Callable("PartitionByKey")
            .Add(0, combine)
            .Lambda(1)
                .Param("combineRow")
                .Callable("Member")
                    .Arg(0, "combineRow")
                    .Atom(1, "value")
                .Seal()
            .Seal()
            .Callable(2, "Void")
            .Seal()
            .Callable(3, "Void")
            .Seal()
            .Lambda(4)
                .Param("groups")
                .Callable("Map")
                    .Arg(0, "groups")
                    .Lambda(1)
                        .Param("group")
                        .Callable("AsStruct")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    auto ret = ctx.Builder(node.Pos())
        .Callable("AsList")
            .Callable(0, "AsStruct")
                .List(0)
                    .Add(0, outputColumn)
                    .Callable(1, "Length")
                        .Add(0, std::move(groupByKey))
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    return ret;
}

TExprNode::TPtr TAggregateExpander::GeneratePostAggregate(const TExprNode::TPtr& preAgg, const TExprNode::TPtr& keyExtractor)
{
    auto preprocessLambda = GeneratePreprocessLambda(keyExtractor);
    TExprNode::TPtr postAgg;
    if (!UsePartitionsByKeys_ && UseFinalizeByKeys_ && !HaveSessionSetting_) {
        // clang-format off
        postAgg = Ctx_.Builder(Node_->Pos())
            .Callable("ShuffleByKeys")
                .Add(0, preAgg)
                .Add(1, keyExtractor)
                .Lambda(2)
                    .Param("stream")
                    .Apply(GetContextLambda())
                        .With(0)
                            .Apply(BuildFinalizeByKeyLambda(preprocessLambda, keyExtractor))
                                .With(0, "stream")
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
            .Seal().Build();
        // clang-format on
    } else {
        auto condenseSwitch = GenerateCondenseSwitch(keyExtractor);
        // clang-format off
        postAgg = Ctx_.Builder(Node_->Pos())
            .Callable("PartitionsByKeys")
                .Add(0, preAgg)
                .Add(1, keyExtractor)
                .Add(2, SortParams_.Order)
                .Add(3, SortParams_.Key)
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
        // clang-format on
    }
    if (KeyColumns_->ChildrenSize() == 0 && !HaveSessionSetting_ && (Suffix_.empty() || Suffix_.EndsWith("Finalize"))) {
        return MakeSingleGroupRow(*Node_, postAgg, Ctx_);
    }
    return postAgg;

}

TExprNode::TPtr TAggregateExpander::GeneratePreprocessLambda(const TExprNode::TPtr& keyExtractor)
{
    TExprNode::TPtr preprocessLambda;
    if (SessionWindowParams_.Update) {
        YQL_ENSURE(EffectiveCompact_);
        YQL_ENSURE(SessionWindowParams_.Key);
        YQL_ENSURE(SessionWindowParams_.KeyType);
        YQL_ENSURE(SessionWindowParams_.Init);

        preprocessLambda = AddSessionParamsMemberLambda(Node_->Pos(), SessionStartMemberName, "", keyExtractor,
            SessionWindowParams_.Key, SessionWindowParams_.Init, SessionWindowParams_.Update, Ctx_);
    } else {
        YQL_ENSURE(!SessionWindowParams_.Key);
        preprocessLambda = MakeIdentityLambda(Node_->Pos(), Ctx_);
    }
    return preprocessLambda;
}

TExprNode::TPtr TAggregateExpander::GenerateCondenseSwitch(const TExprNode::TPtr& keyExtractor)
{
    TExprNode::TPtr condenseSwitch;
    if (SessionWindowParams_.Update) {
        YQL_ENSURE(EffectiveCompact_);
        YQL_ENSURE(SessionWindowParams_.Key);
        YQL_ENSURE(SessionWindowParams_.KeyType);
        YQL_ENSURE(SessionWindowParams_.Init);

        // clang-format off
        condenseSwitch = Ctx_.Builder(Node_->Pos())
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
        // clang-format on
    } else {
        YQL_ENSURE(!SessionWindowParams_.Key);
        // clang-format off
        condenseSwitch = Ctx_.Builder(Node_->Pos())
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
        // clang-format on
    }
    return condenseSwitch;
}

TExprNode::TPtr TAggregateExpander::GeneratePostAggregateInitPhase()
{
    bool aggregateOnly = (!Suffix_.empty());
    const auto& columnNames = aggregateOnly ? FinalColumnNames_ : InitialColumnNames_;

    ui32 index = 0U;
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("item")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                        // clang-format off
                        parent
                            .List(index++)
                                .Add(0, KeyColumns_->ChildPtr(i))
                                .Callable(1, "Member")
                                    .Arg(0, "item")
                                    .Add(1, KeyColumns_->ChildPtr(i))
                                .Seal()
                            .Seal();
                        // clang-format on
                    }
                    if (SessionWindowParams_.Update) {
                        // clang-format off
                        parent
                            .List(index++)
                                .Atom(0, SessionStartMemberName)
                                .Callable(1, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, SessionStartMemberName)
                                .Seal()
                            .Seal();
                        // clang-format on
                    }
                    return parent;
                // clang-format off
                })
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    for (ui32 i = 0; i < columnNames.size(); ++i) {
                        auto child = AggregatedColumns_->Child(i);
                        auto trait = Traits_[i];
                        if (!EffectiveCompact_) {
                            auto loadLambda = trait->Child(4);
                            auto extractorLambda = GetFinalAggStateExtractor(i);

                            if (!DistinctFields_.empty() || Suffix_ == "MergeManyFinalize") {
                                // clang-format off
                                parent.List(index++)
                                    .Add(0, columnNames[i])
                                    .Callable(1, "Map")
                                        .Apply(0, *extractorLambda)
                                            .With(0, "item")
                                        .Seal()
                                        .Add(1, loadLambda)
                                    .Seal()
                                .Seal();
                                // clang-format on
                            } else {
                                // clang-format off
                                parent.List(index++)
                                    .Add(0, columnNames[i])
                                    .Apply(1, *loadLambda)
                                        .With(0)
                                            .Apply(*extractorLambda)
                                                .With(0, "item")
                                            .Seal()
                                        .Done()
                                    .Seal();
                                // clang-format on
                            }
                        } else {
                            auto initLambda = trait->Child(1);
                            auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr;
                            auto initApply = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                // clang-format off
                                parent.Apply(1, *initLambda)
                                    .With(0)
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            // clang-format on
                                            if (distinctField) {
                                                // clang-format off
                                                parent
                                                    .Callable("Member")
                                                        .Arg(0, "item")
                                                        .Add(1, distinctField)
                                                    .Seal();
                                                // clang-format on
                                            } else {
                                                // clang-format off
                                                parent
                                                    .Callable("CastStruct")
                                                        .Arg(0, "item")
                                                        .Add(1, ExpandType(Node_->Pos(), *initLambda->Head().Head().GetTypeAnn(), Ctx_))
                                                    .Seal();
                                                // clang-format on
                                            }

                                            return parent;
                                        // clang-format off
                                        })
                                    .Done()
                                    .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                                        // clang-format on
                                        if (initLambda->Head().ChildrenSize() == 2) {
                                            // clang-format off
                                            parent.With(1)
                                                .Callable("Uint32")
                                                    .Atom(0, ToString(i), TNodeFlags::Default)
                                                    .Seal()
                                                .Done();
                                            // clang-format on
                                        }

                                        return parent;
                                    // clang-format off
                                    })
                                .Seal();
                                // clang-format on

                                return parent;
                            };

                            if (distinctField) {
                                const bool isFirst = *Distinct2Columns_[distinctField->Content()].begin() == i;
                                if (isFirst) {
                                    // clang-format off
                                    parent.List(index++)
                                        .Add(0, columnNames[i])
                                        .List(1)
                                            .Callable(0, "NamedApply")
                                                .Add(0, UdfSetCreate_[distinctField->Content()])
                                                .List(1)
                                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                        // clang-format on
                                                        if (!DistinctFieldNeedsPickle_[distinctField->Content()]) {
                                                            // clang-format off
                                                            parent.Callable(0, "Member")
                                                                .Arg(0, "item")
                                                                .Add(1, distinctField)
                                                            .Seal();
                                                            // clang-format on
                                                        } else {
                                                            // clang-format off
                                                            parent.Callable(0, "StablePickle")
                                                                .Callable(0, "Member")
                                                                .Arg(0, "item")
                                                                .Add(1, distinctField)
                                                                .Seal()
                                                                .Seal();
                                                            // clang-format on
                                                        }

                                                        return parent;
                                                    // clang-format off
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
                                    // clang-format on
                                } else {
                                    // clang-format off
                                    parent.List(index++)
                                        .Add(0, columnNames[i])
                                        .Do(initApply)
                                        .Seal();
                                    // clang-format on
                                }
                            } else {
                                // clang-format off
                                parent.List(index++)
                                    .Add(0, columnNames[i])
                                    .Do(initApply)
                                .Seal();
                                // clang-format on
                            }
                        }
                    }
                    return parent;
                // clang-format off
                })
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr TAggregateExpander::GeneratePostAggregateSavePhase()
{
    bool aggregateOnly = (!Suffix_.empty());
    const auto& columnNames = aggregateOnly ? FinalColumnNames_ : InitialColumnNames_;

    ui32 index = 0U;
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("state")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                        if (KeyColumns_->Child(i)->Content() == SessionStartMemberName) {
                            continue;
                        }
                        // clang-format off
                        parent
                            .List(index++)
                                .Add(0, KeyColumns_->ChildPtr(i))
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Add(1, KeyColumns_->ChildPtr(i))
                                .Seal()
                            .Seal();
                        // clang-format on
                    }

                    if (SessionOutputColumn_) {
                        // clang-format off
                        parent
                            .List(index++)
                                .Atom(0, *SessionOutputColumn_)
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Atom(1, SessionStartMemberName)
                                .Seal()
                            .Seal();
                        // clang-format on
                    }
                    return parent;
                // clang-format off
                })
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    for (ui32 i = 0; i < columnNames.size(); ++i) {
                        auto child = AggregatedColumns_->Child(i);
                        auto trait = Traits_[i];
                        auto finishLambda = (Suffix_ == "MergeState") ? trait->Child(3) : trait->Child(6);

                        if (!EffectiveCompact_ && (!DistinctFields_.empty() || Suffix_ == "MergeManyFinalize")) {
                            if (child->Head().IsAtom()) {
                                // clang-format off
                                parent.List(index++)
                                    .Add(0, FinalColumnNames_[i])
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
                                // clang-format on
                            } else {
                                const auto& multiFields = child->Child(0);
                                for (ui32 field = 0; field < multiFields->ChildrenSize(); ++field) {
                                    // clang-format off
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
                                    // clang-format on
                                }
                            }
                        } else {
                            auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr;
                            auto stateExtractor = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                const bool isFirst = distinctField  ? (*Distinct2Columns_[distinctField->Content()].begin() == i) : false;
                                if (distinctField && isFirst) {
                                    // clang-format off
                                    parent.Callable("Nth")
                                        .Callable(0, "Member")
                                        .Arg(0, "state")
                                        .Add(1, columnNames[i])
                                        .Seal()
                                        .Atom(1, "1", TNodeFlags::Default)
                                        .Seal();
                                    // clang-format on
                                } else {
                                    // clang-format off
                                    parent.Callable("Member")
                                        .Arg(0, "state")
                                        .Add(1, columnNames[i])
                                        .Seal();
                                    // clang-format on
                                }

                                return parent;
                            };

                            if (child->Head().IsAtom()) {
                                // clang-format off
                                parent.List(index++)
                                    .Add(0, FinalColumnNames_[i])
                                    .Apply(1, *finishLambda)
                                        .With(0)
                                            .Do(stateExtractor)
                                        .Done()
                                    .Seal()
                                .Seal();
                                // clang-format on
                            } else {
                                const auto& multiFields = child->Head();
                                for (ui32 field = 0; field < multiFields.ChildrenSize(); ++field) {
                                    // clang-format off
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
                                    // clang-format on
                                }
                            }
                        }
                    }
                    return parent;
                // clang-format off
                })
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr TAggregateExpander::GeneratePostAggregateMergePhase()
{
    bool aggregateOnly = (!Suffix_.empty());
    const auto& columnNames = aggregateOnly ? FinalColumnNames_ : InitialColumnNames_;

    ui32 index = 0U;
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
        .Lambda()
            .Param("item")
            .Param("state")
            .Callable("AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                        // clang-format off
                        parent
                            .List(index++)
                                .Add(0, KeyColumns_->ChildPtr(i))
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Add(1, KeyColumns_->ChildPtr(i))
                                .Seal()
                            .Seal();
                        // clang-format on
                    }
                    if (SessionWindowParams_.Update) {
                        // clang-format off
                        parent
                            .List(index++)
                                .Atom(0, SessionStartMemberName)
                                .Callable(1, "Member")
                                    .Arg(0, "state")
                                    .Atom(1, SessionStartMemberName)
                                .Seal()
                            .Seal();
                        // clang-format on
                    }
                    return parent;
                // clang-format off
                })
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    // clang-format on
                    for (ui32 i = 0; i < columnNames.size(); ++i) {
                        auto child = AggregatedColumns_->Child(i);
                        auto trait = Traits_[i];
                        if (!EffectiveCompact_) {
                            auto loadLambda = trait->Child(4);
                            auto mergeLambda = trait->Child(5);
                            auto extractorLambda = GetFinalAggStateExtractor(i);

                            if (!DistinctFields_.empty() || Suffix_ == "MergeManyFinalize") {
                                // clang-format off
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
                                // clang-format on
                            } else {
                                // clang-format off
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
                                // clang-format on
                            }
                        } else {
                            auto updateLambda = trait->Child(2);
                            auto distinctField = (child->ChildrenSize() == 3) ? child->Child(2) : nullptr;
                            const bool isFirst = distinctField ? (*Distinct2Columns_[distinctField->Content()].begin() == i) : false;
                            auto updateApply = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                // clang-format off
                                parent.Apply(1, *updateLambda)
                                    .With(0)
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            // clang-format on
                                            if (distinctField) {
                                                // clang-format off
                                                parent
                                                    .Callable("Member")
                                                        .Arg(0, "item")
                                                        .Add(1, distinctField)
                                                    .Seal();
                                                // clang-format on
                                            } else {
                                                // clang-format off
                                                parent
                                                    .Callable("CastStruct")
                                                        .Arg(0, "item")
                                                        .Add(1, ExpandType(Node_->Pos(), *updateLambda->Head().Head().GetTypeAnn(), Ctx_))
                                                    .Seal();
                                                // clang-format on
                                            }

                                            return parent;
                                        // clang-format off
                                        })
                                    .Done()
                                    .With(1)
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            // clang-format on
                                            if (distinctField && isFirst) {
                                                // clang-format off
                                                parent.Callable("Nth")
                                                    .Callable(0, "Member")
                                                        .Arg(0, "state")
                                                        .Add(1, columnNames[i])
                                                    .Seal()
                                                    .Atom(1, "1", TNodeFlags::Default)
                                                    .Seal();
                                                // clang-format on
                                            } else {
                                                // clang-format off
                                                parent.Callable("Member")
                                                    .Arg(0, "state")
                                                    .Add(1, columnNames[i])
                                                    .Seal();
                                                // clang-format on
                                            }

                                            return parent;
                                        // clang-format off
                                        })
                                    .Done()
                                    .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                                        // clang-format on
                                        if (updateLambda->Head().ChildrenSize() == 3) {
                                            // clang-format off
                                            parent
                                                .With(2)
                                                    .Callable("Uint32")
                                                        .Atom(0, ToString(i), TNodeFlags::Default)
                                                    .Seal()
                                                .Done();
                                            // clang-format on
                                        }

                                        return parent;
                                    // clang-format off
                                    })
                                .Seal();
                                // clang-format on

                                return parent;
                            };

                            if (distinctField) {
                                auto distinctIndex = *Distinct2Columns_[distinctField->Content()].begin();
                                ui32 newValueIndex = 0;
                                auto newValue = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    // clang-format off
                                    parent.Callable(newValueIndex, "NamedApply")
                                        .Add(0, UdfAddValue_[distinctField->Content()])
                                        .List(1)
                                            .Callable(0, "Nth")
                                                .Callable(0, "Member")
                                                    .Arg(0, "state")
                                                    .Add(1, columnNames[distinctIndex])
                                                .Seal()
                                                .Atom(1, "0", TNodeFlags::Default)
                                            .Seal()
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                // clang-format on
                                                if (!DistinctFieldNeedsPickle_[distinctField->Content()]) {
                                                    // clang-format off
                                                    parent.Callable(1, "Member")
                                                        .Arg(0, "item")
                                                        .Add(1, distinctField)
                                                    .Seal();
                                                    // clang-format on
                                                } else {
                                                    // clang-format off
                                                    parent.Callable(1, "StablePickle")
                                                        .Callable(0, "Member")
                                                        .Arg(0, "item")
                                                        .Add(1, distinctField)
                                                        .Seal()
                                                        .Seal();
                                                    // clang-format on
                                                }

                                                return parent;
                                            // clang-format off
                                            })
                                        .Seal()
                                        .Callable(2, "AsStruct").Seal()
                                    .Seal();
                                    // clang-format on

                                    return parent;
                                };

                                // clang-format off
                                parent.List(index++)
                                    .Add(0, columnNames[i])
                                    .Callable(1, "If")
                                        .Callable(0, "NamedApply")
                                            .Add(0, UdfWasChanged_[distinctField->Content()])
                                            .List(1)
                                                .Callable(0, "NamedApply")
                                                    .Add(0, UdfAddValue_[distinctField->Content()])
                                                    .List(1)
                                                        .Callable(0, "Nth")
                                                            .Callable(0, "Member")
                                                                .Arg(0, "state")
                                                                .Add(1, columnNames[distinctIndex])
                                                            .Seal()
                                                            .Atom(1, "0", TNodeFlags::Default)
                                                        .Seal()
                                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                            // clang-format on
                                                            if (!DistinctFieldNeedsPickle_[distinctField->Content()]) {
                                                                // clang-format off
                                                                parent.Callable(1, "Member")
                                                                    .Arg(0, "item")
                                                                    .Add(1, distinctField)
                                                                .Seal();
                                                                // clang-format on
                                                            } else {
                                                                // clang-format off
                                                                parent.Callable(1, "StablePickle")
                                                                    .Callable(0, "Member")
                                                                    .Arg(0, "item")
                                                                    .Add(1, distinctField)
                                                                    .Seal()
                                                                    .Seal();
                                                                // clang-format on
                                                            }

                                                            return parent;
                                                        // clang-format off
                                                        })
                                                    .Seal()
                                                    .Callable(2, "AsStruct").Seal()
                                                .Seal()
                                            .Seal()
                                            .Callable(2, "AsStruct").Seal()
                                        .Seal()
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            // clang-format on
                                            if (distinctIndex == i) {
                                                // clang-format off
                                                parent.List(1)
                                                    .Do(newValue)
                                                    .Do(updateApply)
                                                .Seal();
                                                // clang-format on
                                            } else {
                                                parent.Do(updateApply);
                                            }

                                            return parent;
                                        // clang-format off
                                        })
                                        .Callable(2, "Member")
                                            .Arg(0, "state")
                                            .Add(1, columnNames[i])
                                        .Seal()
                                    .Seal()
                                    .Seal();
                                // clang-format on
                            } else {
                                // clang-format off
                                parent.List(index++)
                                    .Add(0, columnNames[i])
                                    .Do(updateApply)
                                .Seal();
                                // clang-format on
                            }
                        }
                    }
                    return parent;
                // clang-format off
                })
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr TAggregateExpander::GenerateJustOverStates(const TExprNode::TPtr& input, const TIdxSet& indicies) {
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
        .Callable("Map")
            .Add(0, input)
            .Lambda(1)
                .Param("row")
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        // clang-format on
                        ui32 pos = 0;
                        for (ui32 i = 0; i < KeyColumns_->ChildrenSize(); ++i) {
                            // clang-format off
                            parent
                                .List(pos++)
                                    .Add(0, KeyColumns_->ChildPtr(i))
                                    .Callable(1, "Member")
                                        .Arg(0, "row")
                                        .Add(1, KeyColumns_->ChildPtr(i))
                                    .Seal()
                                .Seal();
                            // clang-format on
                        }

                        for (ui32 i : indicies) {
                            // clang-format off
                            parent
                                .List(pos++)
                                    .Add(0, InitialColumnNames_[i])
                                    .Callable(1, "Just")
                                        .Callable(0, "Member")
                                            .Arg(0, "row")
                                            .Add(1, InitialColumnNames_[i])
                                        .Seal()
                                    .Seal()
                                .Seal();
                            // clang-format on
                        }

                        return parent;
                    // clang-format off
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr TAggregateExpander::SerializeIdxSet(const TIdxSet& indicies) {
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
        .List()
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                ui32 pos = 0;
                for (ui32 i : indicies) {
                    parent.Atom(pos++, ToString(i));
                }

                return parent;
            })
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr TAggregateExpander::GeneratePhases() {
    const TExprNode::TPtr cleanOutputSettings = RemoveSetting(*Node_->Child(3), "output_columns", Ctx_);
    const bool many = HaveDistinct_;
    YQL_CLOG(DEBUG, Core) << "Aggregate: generate " << (many ? "phases with distinct" : "simple phases");
    TExprNode::TListType mergeTraits;
    for (ui32 index = 0; index < AggregatedColumns_->ChildrenSize(); ++index) {
        auto originalTrait = AggregatedColumns_->Child(index)->ChildPtr(1);
        // clang-format off
        auto extractor = Ctx_.Builder(Node_->Pos())
            .Lambda()
                .Param("row")
                .Callable("Member")
                    .Arg(0, "row")
                    .Add(1, InitialColumnNames_[index])
                .Seal()
            .Seal()
            .Build();
        // clang-format on

        if (many) {
            // clang-format off
            extractor = Ctx_.Builder(Node_->Pos())
                .Lambda()
                    .Param("row")
                    .Callable("Unwrap")
                        .Apply(0, extractor)
                            .With(0, "row")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
            // clang-format on
        }

        bool isAggApply = originalTrait->IsCallable("AggApply");
        TCheckedDerefPtr<const TTypeAnnotationNode> serializedStateType;
        if (isAggApply) {
            serializedStateType = AggApplySerializedStateType(originalTrait, Ctx_);
        } else {
            serializedStateType = originalTrait->Child(3)->GetTypeAnn();
        }
        if (many) {
            serializedStateType = Ctx_.MakeType<TOptionalExprType>(serializedStateType);
        }

        // clang-format off
        auto extractorTypeNode = Ctx_.Builder(Node_->Pos())
            .Callable("StructType")
                .List(0)
                    .Add(0, InitialColumnNames_[index])
                    .Add(1, ExpandType(Node_->Pos(), *serializedStateType, Ctx_))
                .Seal()
            .Seal()
            .Build();
        // clang-format on

        if (isAggApply) {
            auto initialType = originalTrait->GetTypeAnn();
            if (many) {
                initialType = Ctx_.MakeType<TOptionalExprType>(initialType);
            }

            // clang-format off
            auto originalExtractorTypeNode = Ctx_.Builder(Node_->Pos())
                .Callable("StructType")
                    .List(0)
                        .Add(0, InitialColumnNames_[index])
                        .Add(1, ExpandType(Node_->Pos(), *initialType, Ctx_))
                    .Seal()
                .Seal()
                .Build();
            // clang-format on

            auto name = TString(originalTrait->ChildPtr(0)->Content());
            if (name.StartsWith("pg_")) {
                auto func = name.substr(3);
                TVector<ui32> argTypes;
                bool needRetype = false;
                bool isUniversal;
                auto status = ExtractPgTypesFromMultiLambda(originalTrait->ChildRef(2), argTypes, needRetype, Ctx_, isUniversal);
                YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
                YQL_ENSURE(!isUniversal);
                const NPg::TAggregateDesc& aggDesc = NPg::LookupAggregation(TString(func), argTypes);
                name = "pg_" + aggDesc.Name + "#" + ToString(aggDesc.AggId);
            }

            // clang-format off
            mergeTraits.push_back(Ctx_.Builder(Node_->Pos())
                .Callable(many ? "AggApplyManyState" : "AggApplyState")
                    .Atom(0, name)
                    .Add(1, extractorTypeNode)
                    .Add(2, extractor)
                    .Add(3, originalExtractorTypeNode)
                .Seal()
                .Build());
            // clang-format on
        } else {
            YQL_ENSURE(originalTrait->IsCallable("AggregationTraits"));
            // clang-format off
            mergeTraits.push_back(Ctx_.Builder(Node_->Pos())
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
            // clang-format on
        }
    }

    TExprNode::TListType finalizeColumns;
    for (ui32 index = 0; index < AggregatedColumns_->ChildrenSize(); ++index) {
        // clang-format off
        finalizeColumns.push_back(Ctx_.Builder(Node_->Pos())
            .List()
                .Add(0, AggregatedColumns_->Child(index)->ChildPtr(0))
                .Add(1, mergeTraits[index])
            .Seal()
            .Build());
        // clang-format on
    }

    if (!many) {
        // simple Combine + MergeFinalize
        TExprNode::TListType combineColumns;
        for (ui32 index = 0; index < AggregatedColumns_->ChildrenSize(); ++index) {
            // clang-format off
            combineColumns.push_back(Ctx_.Builder(Node_->Pos())
                .List()
                    .Add(0, InitialColumnNames_[index])
                    .Add(1, AggregatedColumns_->Child(index)->ChildPtr(1))
                .Seal()
                .Build());
            // clang-format on
        }

        // clang-format off
        auto combine = Ctx_.Builder(Node_->Pos())
            .Callable("AggregateCombine")
                .Add(0, AggList_)
                .Add(1, KeyColumns_)
                .Add(2, Ctx_.NewList(Node_->Pos(), std::move(combineColumns)))
                .Add(3, cleanOutputSettings)
            .Seal()
            .Build();
        // clang-format on

        // clang-format off
        auto mergeFinalize = Ctx_.Builder(Node_->Pos())
            .Callable("AggregateMergeFinalize")
                .Add(0, combine)
                .Add(1, KeyColumns_)
                .Add(2, Ctx_.NewList(Node_->Pos(), std::move(finalizeColumns)))
                .Add(3, cleanOutputSettings)
            .Seal()
            .Build();
        // clang-format on

        return mergeFinalize;
    }

    // process with distincts
    // Combine + Map with Just over states
    //      for each distinct field:
    //          Aggregate by keys + field w/o aggs
    //          Combine by keys + field with aggs
    //          Map with Just over states
    // UnionAll
    // MergeManyFinalize
    TExprNode::TListType unionAllInputs;
    TExprNode::TListType streams;

    if (!NonDistinctColumns_.empty()) {
        TExprNode::TListType combineColumns;
        for (ui32 i : NonDistinctColumns_) {
            // clang-format off
            combineColumns.push_back(Ctx_.Builder(Node_->Pos())
                .List()
                    .Add(0, InitialColumnNames_[i])
                    .Add(1, AggregatedColumns_->Child(i)->ChildPtr(1))
                .Seal()
                .Build());
            // clang-format on
        }

        // clang-format off
        auto combine = Ctx_.Builder(Node_->Pos())
            .Callable("AggregateCombine")
                .Add(0, AggList_)
                .Add(1, KeyColumns_)
                .Add(2, Ctx_.NewList(Node_->Pos(), std::move(combineColumns)))
                .Add(3, cleanOutputSettings)
            .Seal()
            .Build();
        // clang-format on

        unionAllInputs.push_back(GenerateJustOverStates(combine, NonDistinctColumns_));
        streams.push_back(SerializeIdxSet(NonDistinctColumns_));
    }

    for (auto distinctField : DistinctFields_) {
        auto& indicies = Distinct2Columns_[distinctField->Content()];
        TExprNode::TListType allKeyColumns = KeyColumns_->ChildrenList();
        allKeyColumns.push_back(distinctField);

        // clang-format off
        auto distinct = Ctx_.Builder(Node_->Pos())
            .Callable("Aggregate")
                .Add(0, AggList_)
                .Add(1, Ctx_.NewList(Node_->Pos(), std::move(allKeyColumns)))
                .List(2)
                .Seal()
                .Add(3, cleanOutputSettings)
            .Seal()
            .Build();
        // clang-format on

        TExprNode::TListType combineColumns;
        for (ui32 i : indicies) {
            auto trait = AggregatedColumns_->Child(i)->ChildPtr(1);
            bool isAggApply = trait->IsCallable("AggApply");
            if (isAggApply) {
                // clang-format off
                trait = Ctx_.Builder(Node_->Pos())
                    .Callable("AggApply")
                        .Add(0, trait->ChildPtr(0))
                        .Callable(1, "StructType")
                            .List(0)
                                .Add(0, distinctField)
                                .Add(1, trait->ChildPtr(1))
                            .Seal()
                        .Seal()
                        .Lambda(2)
                            .Param("row")
                            .Apply(trait->ChildPtr(2))
                                .With(0)
                                    .Callable("Member")
                                        .Arg(0, "row")
                                        .Add(1, distinctField)
                                    .Seal()
                                .Done()
                            .Seal()
                        .Seal()
                    .Seal()
                    .Build();
                // clang-format on
            } else {
                TExprNode::TPtr newInit;
                if (trait->ChildPtr(1)->Head().ChildrenSize() == 1) {
                    // clang-format off
                    newInit = Ctx_.Builder(Node_->Pos())
                        .Lambda()
                            .Param("row")
                            .Apply(trait->ChildPtr(1))
                                .With(0)
                                    .Callable("Member")
                                        .Arg(0, "row")
                                        .Add(1, distinctField)
                                    .Seal()
                                .Done()
                            .Seal()
                        .Seal()
                        .Build();
                    // clang-format on
                } else {
                    // clang-format off
                    newInit = Ctx_.Builder(Node_->Pos())
                        .Lambda()
                            .Param("row")
                            .Param("parent")
                            .Apply(trait->ChildPtr(1))
                                .With(0)
                                    .Callable("Member")
                                        .Arg(0, "row")
                                        .Add(1, distinctField)
                                    .Seal()
                                .Done()
                                .With(1, "parent")
                            .Seal()
                        .Seal()
                        .Build();
                    // clang-format on
                }

                TExprNode::TPtr newUpdate;
                if (trait->ChildPtr(2)->Head().ChildrenSize() == 2) {
                    // clang-format off
                    newUpdate = Ctx_.Builder(Node_->Pos())
                        .Lambda()
                            .Param("row")
                            .Param("state")
                            .Apply(trait->ChildPtr(2))
                                .With(0)
                                    .Callable("Member")
                                        .Arg(0, "row")
                                        .Add(1, distinctField)
                                    .Seal()
                                .Done()
                                .With(1, "state")
                            .Seal()
                        .Seal()
                        .Build();
                    // clang-format on
                } else {
                    // clang-format off
                    newUpdate = Ctx_.Builder(Node_->Pos())
                        .Lambda()
                            .Param("row")
                            .Param("state")
                            .Param("parent")
                            .Apply(trait->ChildPtr(2))
                                .With(0)
                                    .Callable("Member")
                                        .Arg(0, "row")
                                        .Add(1, distinctField)
                                    .Seal()
                                .Done()
                                .With(1, "state")
                                .With(2, "parent")
                            .Seal()
                        .Seal()
                        .Build();
                    // clang-format on
                }

                // clang-format off
                trait = Ctx_.Builder(Node_->Pos())
                    .Callable("AggregationTraits")
                        .Callable(0, "StructType")
                            .List(0)
                                .Add(0, distinctField)
                                .Add(1, trait->ChildPtr(0))
                            .Seal()
                        .Seal()
                        .Add(1, newInit)
                        .Add(2, newUpdate)
                        .Add(3, trait->ChildPtr(3))
                        .Add(4, trait->ChildPtr(4))
                        .Add(5, trait->ChildPtr(5))
                        .Add(6, trait->ChildPtr(6))
                        .Add(7, trait->ChildPtr(7))
                    .Seal()
                    .Build();
                // clang-format on
            }

            // clang-format off
            combineColumns.push_back(Ctx_.Builder(Node_->Pos())
                .List()
                .Add(0, InitialColumnNames_[i])
                .Add(1, trait)
                .Seal()
                .Build());
            // clang-format on
        }

        // clang-format off
        auto combine = Ctx_.Builder(Node_->Pos())
            .Callable("AggregateCombine")
                .Add(0, distinct)
                .Add(1, KeyColumns_)
                .Add(2, Ctx_.NewList(Node_->Pos(), std::move(combineColumns)))
                .Add(3, cleanOutputSettings)
            .Seal()
            .Build();
        // clang-format on

        unionAllInputs.push_back(GenerateJustOverStates(combine, indicies));
        streams.push_back(SerializeIdxSet(indicies));
    }

    if (UseBlocks_) {
        for (ui32 i = 0; i < unionAllInputs.size(); ++i) {
            // clang-format off
            unionAllInputs[i] = Ctx_.Builder(Node_->Pos())
                .Callable("Map")
                    .Add(0, unionAllInputs[i])
                    .Lambda(1)
                        .Param("row")
                        .Callable("AddMember")
                            .Arg(0, "row")
                            .Atom(1, "_yql_group_stream_index")
                            .Callable(2, "Uint32")
                                .Atom(0, ToString(i))
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
            // clang-format on
        }
    }

    auto settings = cleanOutputSettings;
    if (UseBlocks_) {
        settings = AddSetting(*settings, Node_->Pos(), "many_streams", Ctx_.NewList(Node_->Pos(), std::move(streams)), Ctx_);
    }

    auto unionAll = Ctx_.NewCallable(Node_->Pos(), "UnionAll", std::move(unionAllInputs));
    // clang-format off
    auto mergeManyFinalize = Ctx_.Builder(Node_->Pos())
        .Callable("AggregateMergeManyFinalize")
            .Add(0, unionAll)
            .Add(1, KeyColumns_)
            .Add(2, Ctx_.NewList(Node_->Pos(), std::move(finalizeColumns)))
            .Add(3, settings)
        .Seal()
        .Build();
    // clang-format on

    return mergeManyFinalize;
}

TExprNode::TPtr TAggregateExpander::TryGenerateBlockCombine() {
    if (HaveSessionSetting_ || HaveDistinct_) {
        return nullptr;
    }

    for (const auto& x : AggregatedColumns_->Children()) {
        auto trait = x->ChildPtr(1);
        if (!trait->IsCallable("AggApply")) {
            return nullptr;
        }
    }

    return TryGenerateBlockCombineAllOrHashed();
}

TExprNode::TPtr TAggregateExpander::TryGenerateBlockMergeFinalize() {
    if (UsePartitionsByKeys_ || !UseBlocks_) {
        return nullptr;
    }

    if (HaveSessionSetting_ || HaveDistinct_) {
        return nullptr;
    }

    for (const auto& x : AggregatedColumns_->Children()) {
        auto trait = x->ChildPtr(1);
        if (!trait->IsCallable({ "AggApplyState", "AggApplyManyState" })) {
            return nullptr;
        }
    }

    return TryGenerateBlockMergeFinalizeHashed();
}

TExprNode::TPtr TAggregateExpander::TryGenerateBlockMergeFinalizeHashed() {
    if (!TypesCtx_.ArrowResolver) {
        return nullptr;
    }

    if (KeyColumns_->ChildrenSize() == 0) {
        return nullptr;
    }

    bool isMany = Suffix_ == "MergeManyFinalize";
    auto streamArg = Ctx_.NewArgument(Node_->Pos(), "stream");
    TExprNode::TListType keyIdxs;
    TVector<TString> outputColumns;
    TExprNode::TListType aggs;
    ui32 streamIdxColumn;
    auto blocks = MakeInputBlocks(streamArg, keyIdxs, outputColumns, aggs, /*overState=*/true, isMany, &streamIdxColumn);
    if (!blocks) {
        return nullptr;
    }

    TExprNode::TPtr aggBlocks;
    if (!isMany) {
        // clang-format off
        aggBlocks = Ctx_.Builder(Node_->Pos())
            .Callable("BlockMergeFinalizeHashed")
                .Add(0, blocks)
                .Add(1, Ctx_.NewList(Node_->Pos(), std::move(keyIdxs)))
                .Add(2, Ctx_.NewList(Node_->Pos(), std::move(aggs)))
            .Seal()
            .Build();
        // clang-format on
    } else {
        auto manyStreamsSetting = GetSetting(*Node_->Child(3), "many_streams");
        YQL_ENSURE(manyStreamsSetting, "Missing many_streams setting");

        // clang-format off
        aggBlocks = Ctx_.Builder(Node_->Pos())
            .Callable("BlockMergeManyFinalizeHashed")
                .Add(0, blocks)
                .Add(1, Ctx_.NewList(Node_->Pos(), std::move(keyIdxs)))
                .Add(2, Ctx_.NewList(Node_->Pos(), std::move(aggs)))
                .Atom(3, ToString(streamIdxColumn))
                .Add(4, manyStreamsSetting->TailPtr())
            .Seal()
            .Build();
        // clang-format on
    }

    // clang-format off
    auto aggWideFlow = Ctx_.Builder(Node_->Pos())
        .Callable("ToFlow")
            .Callable(0, "WideFromBlocks")
                .Add(0, aggBlocks)
            .Seal()
        .Seal()
        .Build();
    // clang-format on
    auto finalFlow = MakeNarrowMap(Node_->Pos(), outputColumns, aggWideFlow, Ctx_);
    auto root = Ctx_.NewCallable(Node_->Pos(), "FromFlow", { finalFlow });
    auto lambdaStream = Ctx_.NewLambda(Node_->Pos(), Ctx_.NewArguments(Node_->Pos(), { streamArg }), std::move(root));

    auto keySelector = BuildKeySelector(Node_->Pos(), *OriginalRowType_, KeyColumns_, Ctx_);
    // clang-format off
    return Ctx_.Builder(Node_->Pos())
        .Callable("ShuffleByKeys")
            .Add(0, AggList_)
            .Add(1, keySelector)
            .Lambda(2)
                .Param("stream")
                .Apply(GetContextLambda())
                    .With(0)
                        .Apply(lambdaStream)
                            .With(0, "stream")
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr ExpandAggregatePeephole(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    if (NNodes::TCoAggregate::Match(node.Get())) {
        NNodes::TCoAggregate self(node);
        auto ret = TAggregateExpander::CountAggregateRewrite(self, ctx, typesCtx.IsBlockEngineEnabled());
        if (ret != node) {
            YQL_CLOG(DEBUG, Core) << "CountAggregateRewrite on peephole";
            return ret;
        }
    }
    return ExpandAggregatePeepholeImpl(node, ctx, typesCtx, /*useFinalizeByKey=*/false, typesCtx.IsBlockEngineEnabled(), /*allowSpilling=*/false);
}

} // namespace NYql
