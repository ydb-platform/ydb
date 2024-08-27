#include "yql_expr_constraint.h"
#include "yql_callable_transform.h"
#include "yql_opt_utils.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/utils/log/profile.h>

#include <util/generic/scope.h>
#include <util/generic/utility.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/string/type.h>

namespace NYql {

using namespace NNodes;

namespace {

template <size_t FromChild, class... Other>
struct TApplyConstraintFromInput;

template <size_t FromChild>
struct TApplyConstraintFromInput<FromChild> {
    static void Do(const TExprNode::TPtr&) {
    }
};

template <size_t FromChild, class TConstraint, class... Other>
struct TApplyConstraintFromInput<FromChild, TConstraint, Other...> {
    static void Do(const TExprNode::TPtr& input) {
        if (auto c = input->Child(FromChild)->GetConstraint<TConstraint>()) {
            input->AddConstraint(c);
        }
        TApplyConstraintFromInput<FromChild, Other...>::Do(input);
    }
};

template <class TConstraint>
const TConstraint* MakeCommonConstraint(const TExprNode::TPtr& input, size_t from, TExprContext& ctx) {
    TVector<const TConstraintSet*> constraints;
    for (size_t i = from; i < input->ChildrenSize(); ++i) {
        constraints.push_back(&input->Child(i)->GetConstraintSet());
    }
    return TConstraint::MakeCommon(constraints, ctx);
}

template <class... Other>
struct TApplyCommonConstraint;

template <class TConstraint>
struct TApplyCommonConstraint<TConstraint> {
    static void Do(const TExprNode::TPtr& input, const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
        if (auto c = TConstraint::MakeCommon(constraints, ctx)) {
            input->AddConstraint(c);
        }
    }
};

template <class TConstraint, class... Other>
struct TApplyCommonConstraint<TConstraint, Other...> {
    static void Do(const TExprNode::TPtr& input, const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
        if (auto c = TConstraint::MakeCommon(constraints, ctx)) {
            input->AddConstraint(c);
        }
        TApplyCommonConstraint<Other...>::Do(input, constraints, ctx);
    }
};

class TCallableConstraintTransformer : public TCallableTransformerBase<TCallableConstraintTransformer> {
    using THandler = TStatus(TCallableConstraintTransformer::*)(const TExprNode::TPtr&, TExprNode::TPtr&, TExprContext&) const;

public:
    TCallableConstraintTransformer(TTypeAnnotationContext& types, bool instantOnly, bool subGraph)
        : TCallableTransformerBase<TCallableConstraintTransformer>(types, instantOnly)
        , SubGraph(subGraph)
    {
        Functions["Unordered"] = &TCallableConstraintTransformer::FromFirst<TEmptyConstraintNode, TUniqueConstraintNode, TDistinctConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["UnorderedSubquery"] = &TCallableConstraintTransformer::FromFirst<TEmptyConstraintNode, TUniqueConstraintNode, TDistinctConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["Sort"] = &TCallableConstraintTransformer::SortWrap;
        Functions["AssumeSorted"] = &TCallableConstraintTransformer::SortWrap;
        Functions["AssumeUnique"] = &TCallableConstraintTransformer::AssumeUniqueWrap<false, true>;
        Functions["AssumeDistinct"] = &TCallableConstraintTransformer::AssumeUniqueWrap<true, true>;
        Functions["AssumeUniqueHint"] = &TCallableConstraintTransformer::AssumeUniqueWrap<false, false>;
        Functions["AssumeDistinctHint"] = &TCallableConstraintTransformer::AssumeUniqueWrap<true, false>;
        Functions["AssumeChopped"] = &TCallableConstraintTransformer::AssumeChoppedWrap;
        Functions["AssumeColumnOrder"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["AssumeAllMembersNullableAtOnce"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["Top"] = &TCallableConstraintTransformer::TopWrap<false>;
        Functions["TopSort"] = &TCallableConstraintTransformer::TopWrap<true>;
        Functions["TakeWhile"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["SkipWhile"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["TakeWhileInclusive"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["SkipWhileInclusive"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["WideTakeWhile"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["WideSkipWhile"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["WideTakeWhileInclusive"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["WideSkipWhileInclusive"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["Iterator"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["ForwardList"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["LazyList"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["ToFlow"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["FromFlow"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["ToStream"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["ToSequence"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["Collect"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["FilterNullMembers"] = &TCallableConstraintTransformer::FromFirst<TSortedConstraintNode, TPartOfSortedConstraintNode, TChoppedConstraintNode, TPartOfChoppedConstraintNode, TEmptyConstraintNode, TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TVarIndexConstraintNode>;
        Functions["SkipNullMembers"] = &TCallableConstraintTransformer::FromFirst<TSortedConstraintNode, TPartOfSortedConstraintNode, TChoppedConstraintNode, TPartOfChoppedConstraintNode, TEmptyConstraintNode, TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TVarIndexConstraintNode>;
        Functions["FilterNullElements"] = &TCallableConstraintTransformer::FromFirst<TSortedConstraintNode, TPartOfSortedConstraintNode, TChoppedConstraintNode, TPartOfChoppedConstraintNode, TEmptyConstraintNode, TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TVarIndexConstraintNode>;
        Functions["SkipNullElements"] = &TCallableConstraintTransformer::FromFirst<TSortedConstraintNode, TPartOfSortedConstraintNode, TChoppedConstraintNode, TPartOfChoppedConstraintNode, TEmptyConstraintNode, TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TVarIndexConstraintNode>;
        Functions["Right!"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["Cons!"] = &TCallableConstraintTransformer::CopyAllFrom<1>;
        Functions["ExtractMembers"] = &TCallableConstraintTransformer::ExtractMembersWrap;
        Functions["RemoveSystemMembers"] = &TCallableConstraintTransformer::RemovePrefixMembersWrap;
        Functions["RemovePrefixMembers"] = &TCallableConstraintTransformer::RemovePrefixMembersWrap;
        Functions["FlattenMembers"] = &TCallableConstraintTransformer::FlattenMembersWrap;
        Functions["SelectMembers"] = &TCallableConstraintTransformer::SelectMembersWrap;
        Functions["FilterMembers"] = &TCallableConstraintTransformer::SelectMembersWrap;
        Functions["CastStruct"] = &TCallableConstraintTransformer::SelectMembersWrap;
        Functions["SafeCast"] = &TCallableConstraintTransformer::CastWrap<false>;
        Functions["StrictCast"] = &TCallableConstraintTransformer::CastWrap<true>;
        Functions["ToString"] = &TCallableConstraintTransformer::CastWrap<true>;
        Functions["ToBytes"] = &TCallableConstraintTransformer::CastWrap<true>;
        Functions["DivePrefixMembers"] = &TCallableConstraintTransformer::DivePrefixMembersWrap;
        Functions["OrderedFilter"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["Filter"] = &TCallableConstraintTransformer::FilterWrap<false>;
        Functions["WideFilter"] = &TCallableConstraintTransformer::FilterWrap<true>;
        Functions["OrderedMap"] = &TCallableConstraintTransformer::MapWrap<true, false>;
        Functions["Map"] = &TCallableConstraintTransformer::MapWrap<false, false>;
        Functions["MapNext"] = &TCallableConstraintTransformer::MapWrap<true, false>;
        Functions["OrderedFlatMap"] = &TCallableConstraintTransformer::MapWrap<true, true>;
        Functions["FlatMap"] = &TCallableConstraintTransformer::MapWrap<false, true>;
        Functions["OrderedMultiMap"] = &TCallableConstraintTransformer::MapWrap<true, false>;
        Functions["MultiMap"] = &TCallableConstraintTransformer::MapWrap<false, false>;
        Functions["ExpandMap"] = &TCallableConstraintTransformer::MapWrap<true, false, false, true>;
        Functions["WideMap"] = &TCallableConstraintTransformer::MapWrap<true, false, true, true>;
        Functions["NarrowMap"] = &TCallableConstraintTransformer::MapWrap<true, false, true, false>;
        Functions["NarrowFlatMap"] = &TCallableConstraintTransformer::MapWrap<true, true, true, false>;
        Functions["NarrowMultiMap"] = &TCallableConstraintTransformer::MapWrap<true, false, true, false>;
        Functions["OrderedFlatMapToEquiJoin"] = &TCallableConstraintTransformer::MapWrap<true, true>;
        Functions["FlatMapToEquiJoin"] = &TCallableConstraintTransformer::MapWrap<false, true>;
        Functions["OrderedLMap"] = &TCallableConstraintTransformer::LMapWrap<true>;
        Functions["LMap"] = &TCallableConstraintTransformer::LMapWrap<false>;
        Functions["Extract"] = &TCallableConstraintTransformer::FromFirst<TEmptyConstraintNode>;
        Functions["OrderedExtract"] = &TCallableConstraintTransformer::FromFirst<TEmptyConstraintNode>;
        Functions["OrderedExtend"] = &TCallableConstraintTransformer::ExtendWrap<true>;
        Functions["Extend"] = &TCallableConstraintTransformer::ExtendWrap<false>;
        Functions["UnionAll"] = &TCallableConstraintTransformer::ExtendWrap<false>;
        Functions["Merge"] = &TCallableConstraintTransformer::MergeWrap<false>;
        Functions["UnionMerge"] = &TCallableConstraintTransformer::MergeWrap<true>;
        Functions["Skip"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["Take"] = &TCallableConstraintTransformer::TakeWrap;
        Functions["Limit"] = &TCallableConstraintTransformer::TakeWrap;
        Functions["Member"] = &TCallableConstraintTransformer::MemberWrap;
        Functions["AsStruct"] = &TCallableConstraintTransformer::AsStructWrap;
        Functions["BlockAsStruct"] = &TCallableConstraintTransformer::AsStructWrap;
        Functions["Just"] = &TCallableConstraintTransformer::FromFirst<TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TPartOfSortedConstraintNode, TPartOfChoppedConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["Unwrap"] = &TCallableConstraintTransformer::FromFirst<TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TPartOfSortedConstraintNode, TPartOfChoppedConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["Ensure"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["ToList"] = &TCallableConstraintTransformer::FromFirst<TEmptyConstraintNode, TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TPartOfSortedConstraintNode, TPartOfChoppedConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["ToOptional"] = &TCallableConstraintTransformer::FromFirst<TEmptyConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["Head"] = &TCallableConstraintTransformer::FromFirst<TEmptyConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["Last"] = &TCallableConstraintTransformer::FromFirst<TEmptyConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["Reverse"] = &TCallableConstraintTransformer::ReverseWrap;
        Functions["Replicate"] = &TCallableConstraintTransformer::FromFirst<TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["AddMember"] = &TCallableConstraintTransformer::AddMemberWrap;
        Functions["RemoveMember"] = &TCallableConstraintTransformer::RemoveMemberWrap;
        Functions["ForceRemoveMember"] = &TCallableConstraintTransformer::RemoveMemberWrap;
        Functions["ReplaceMember"] = &TCallableConstraintTransformer::ReplaceMemberWrap;
        Functions["AsList"] = &TCallableConstraintTransformer::AsListWrap;
        Functions["OptionalIf"] = &TCallableConstraintTransformer::PassOrEmptyWrap<false, false>;
        Functions["FlatOptionalIf"] = &TCallableConstraintTransformer::PassOrEmptyWrap<false, true>;
        Functions["ListIf"] = &TCallableConstraintTransformer::PassOrEmptyWrap<true, false>;
        Functions["FlatListIf"] = &TCallableConstraintTransformer::PassOrEmptyWrap<true, true>;
        Functions["EmptyIterator"] = &TCallableConstraintTransformer::FromEmpty;
        Functions["EmptyFrom"] = &TCallableConstraintTransformer::EmptyFromWrap;
        Functions["List"] = &TCallableConstraintTransformer::ListWrap;
        Functions["Dict"] = &TCallableConstraintTransformer::DictWrap;
        Functions["EmptyList"] = &TCallableConstraintTransformer::FromEmpty;
        Functions["EmptyDict"] = &TCallableConstraintTransformer::FromEmpty;
        Functions["DictFromKeys"] = &TCallableConstraintTransformer::DictFromKeysWrap;
        Functions["If"] = &TCallableConstraintTransformer::IfWrap;
        Functions["Nothing"] = &TCallableConstraintTransformer::FromEmpty;
        Functions["IfPresent"] = &TCallableConstraintTransformer::IfPresentWrap;
        Functions["Coalesce"] = &TCallableConstraintTransformer::CommonFromChildren<0, TSortedConstraintNode, TPartOfSortedConstraintNode, TChoppedConstraintNode, TPartOfChoppedConstraintNode, TEmptyConstraintNode, TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>;
        Functions["CombineByKey"] = &TCallableConstraintTransformer::FromFinalLambda<TCoCombineByKey::idx_FinishHandlerLambda>;
        Functions["CombineByKeyWithSpilling"] = &TCallableConstraintTransformer::FromFinalLambda<TCoCombineByKeyWithSpilling::idx_FinishHandlerLambda>;
        Functions["FinalizeByKey"] = &TCallableConstraintTransformer::FromFinalLambda<TCoFinalizeByKey::idx_FinishHandlerLambda>;
        Functions["FinalizeByKeyWithSpilling"] = &TCallableConstraintTransformer::FromFinalLambda<TCoFinalizeByKeyWithSpilling::idx_FinishHandlerLambda>;
        Functions["CombineCore"] = &TCallableConstraintTransformer::FromFinalLambda<TCoCombineCore::idx_FinishHandler>;
        Functions["CombineCoreWithSpilling"] = &TCallableConstraintTransformer::FromFinalLambda<TCoCombineCoreWithSpilling::idx_FinishHandler>;
        Functions["PartitionByKey"] = &TCallableConstraintTransformer::ShuffleByKeysWrap<true>;
        Functions["PartitionsByKeys"] = &TCallableConstraintTransformer::ShuffleByKeysWrap<true>;
        Functions["ShuffleByKeys"] = &TCallableConstraintTransformer::ShuffleByKeysWrap<false>;
        Functions["Switch"] = &TCallableConstraintTransformer::SwitchWrap;
        Functions["Visit"] = &TCallableConstraintTransformer::VisitWrap;
        Functions["VariantItem"] = &TCallableConstraintTransformer::VariantItemWrap;
        Functions["Variant"] = &TCallableConstraintTransformer::VariantWrap;
        Functions["Guess"] = &TCallableConstraintTransformer::GuessWrap;
        Functions["Mux"] = &TCallableConstraintTransformer::MuxWrap;
        Functions["Nth"] = &TCallableConstraintTransformer::NthWrap;
        Functions["EquiJoin"] = &TCallableConstraintTransformer::EquiJoinWrap;
        Functions["JoinDict"] = &TCallableConstraintTransformer::JoinDictWrap;
        Functions["MapJoinCore"] = &TCallableConstraintTransformer::MapJoinCoreWrap;
        Functions["GraceJoinCore"] = &TCallableConstraintTransformer::GraceJoinCoreWrap;
        Functions["GraceSelfJoinCore"] = &TCallableConstraintTransformer::GraceSelfJoinCoreWrap;
        Functions["CommonJoinCore"] = &TCallableConstraintTransformer::FromFirst<TEmptyConstraintNode>;
        Functions["ToDict"] = &TCallableConstraintTransformer::ToDictWrap;
        Functions["DictItems"] = &TCallableConstraintTransformer::DictItemsWrap;
        Functions["DictKeys"] = &TCallableConstraintTransformer::DictHalfWrap<true>;
        Functions["DictPayloads"] = &TCallableConstraintTransformer::DictHalfWrap<false>;
        Functions["Chain1Map"] = &TCallableConstraintTransformer::Chain1MapWrap<false>;
        Functions["WideChain1Map"] = &TCallableConstraintTransformer::Chain1MapWrap<true>;
        Functions["IsKeySwitch"] = &TCallableConstraintTransformer::IsKeySwitchWrap;
        Functions["Condense"] = &TCallableConstraintTransformer::CondenseWrap;
        Functions["Condense1"] = &TCallableConstraintTransformer::Condense1Wrap<false>;
        Functions["GroupingCore"] = &TCallableConstraintTransformer::InheriteEmptyFromInput;
        Functions["Chopper"] = &TCallableConstraintTransformer::InheriteEmptyFromInput;
        Functions["WideChopper"] = &TCallableConstraintTransformer::InheriteEmptyFromInput;
        Functions["WideCombiner"] = &TCallableConstraintTransformer::InheriteEmptyFromInput;
        Functions["WideCombinerWithSpilling"] = &TCallableConstraintTransformer::InheriteEmptyFromInput;
        Functions["WideCondense1"] = &TCallableConstraintTransformer::Condense1Wrap<true>;
        Functions["Aggregate"] = &TCallableConstraintTransformer::AggregateWrap<true>;
        Functions["AggregateMergeState"] = &TCallableConstraintTransformer::AggregateWrap<true>;
        Functions["AggregateMergeFinalize"] = &TCallableConstraintTransformer::AggregateWrap<true>;
        Functions["AggregateMergeManyFinalize"] = &TCallableConstraintTransformer::AggregateWrap<true>;
        Functions["AggregateFinalize"] = &TCallableConstraintTransformer::AggregateWrap<true>;
        Functions["AggregateCombine"] = &TCallableConstraintTransformer::AggregateWrap<false>;
        Functions["AggregateCombineState"] = &TCallableConstraintTransformer::AggregateWrap<false>;
        Functions["Fold"] = &TCallableConstraintTransformer::FoldWrap;
        Functions["Fold1"] = &TCallableConstraintTransformer::FoldWrap;
        Functions["WithContext"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["WithWorld"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["WideTop"] = &TCallableConstraintTransformer::WideTopWrap<false>;
        Functions["WideTopSort"] = &TCallableConstraintTransformer::WideTopWrap<true>;
        Functions["WideSort"] = &TCallableConstraintTransformer::WideTopWrap<true>;
        Functions["WideTopBlocks"] = &TCallableConstraintTransformer::WideTopWrap<false>;
        Functions["WideTopSortBlocks"] = &TCallableConstraintTransformer::WideTopWrap<true>;
        Functions["WideSortBlocks"] = &TCallableConstraintTransformer::WideTopWrap<true>;
        Functions["WideToBlocks"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["WideFromBlocks"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["BlockExpandChunked"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["ReplicateScalars"] = &TCallableConstraintTransformer::CopyAllFrom<0>;
        Functions["BlockMergeFinalizeHashed"] = &TCallableConstraintTransformer::AggregateWrap<true>;
        Functions["BlockMergeManyFinalizeHashed"] = &TCallableConstraintTransformer::AggregateWrap<true>;
    }

    std::optional<IGraphTransformer::TStatus> ProcessCore(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (const auto func = Functions.find(input->Content()); Functions.cend() != func) {
            return (this->*func->second)(input, output, ctx);
        }
        return std::nullopt;
    }

    std::optional<IGraphTransformer::TStatus> ProcessList(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!input->ChildrenSize() || ETypeAnnotationKind::Tuple != input->GetTypeAnn()->GetKind())
            return TStatus::Ok;
        return AsTupleWrap(input, output, ctx);
    }

    TStatus ProcessUnknown(const TExprNode::TPtr& input, TExprContext&) {
        return UpdateAllChildLambdasConstraints(*input);
    }

    TStatus ValidateProviderCommitResult(const TExprNode::TPtr&, TExprContext&) {
        return TStatus::Ok;
    }

    TStatus ValidateProviderReadResult(const TExprNode::TPtr&, TExprContext&) {
        return TStatus::Ok;
    }

    TStatus ValidateProviderWriteResult(const TExprNode::TPtr&, TExprContext&) {
        return TStatus::Ok;
    }

    TStatus ValidateProviderConfigureResult(const TExprNode::TPtr&, TExprContext&) {
        return TStatus::Ok;
    }

    IGraphTransformer& GetTransformer(IDataProvider& provider) const {
        return provider.GetConstraintTransformer(InstantOnly, SubGraph);
    }

private:
    template <size_t Ndx>
    TStatus CopyAllFrom(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        Y_UNUSED(output);
        Y_UNUSED(ctx);
        input->CopyConstraints(*input->Child(Ndx));
        return TStatus::Ok;
    }

    template <class... TConstraints>
    TStatus FromFirst(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        Y_UNUSED(output);
        Y_UNUSED(ctx);
        TApplyConstraintFromInput<0, TConstraints...>::Do(input);
        return TStatus::Ok;
    }

    template <class... TConstraints>
    TStatus FromSecond(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        Y_UNUSED(output);
        Y_UNUSED(ctx);
        TApplyConstraintFromInput<1, TConstraints...>::Do(input);
        return TStatus::Ok;
    }

    template <size_t StartFromChild, class... TConstraints>
    TStatus CommonFromChildren(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        Y_UNUSED(output);
        TVector<const TConstraintSet*> constraints;
        for (size_t i = StartFromChild; i < input->ChildrenSize(); ++i) {
            constraints.push_back(&input->Child(i)->GetConstraintSet());
        }
        TApplyCommonConstraint<TConstraints...>::Do(input, constraints, ctx);
        return TStatus::Ok;
    }

    TStatus FromEmpty(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        return TStatus::Ok;
    }

    TStatus EmptyFromWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        auto set = input->Head().GetConstraintSet();
        set.RemoveConstraint(TEmptyConstraintNode::Name());
        if (!set) {
            const auto type = input->GetTypeAnn();
            output = ctx.NewCallable(input->Pos(), GetEmptyCollectionName(type), {ExpandType(input->Pos(), *type, ctx)});
            return TStatus::Repeat;
        }

        set.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        input->SetConstraints(set);
        return TStatus::Ok;
    }

    template <size_t LambdaIdx>
    TStatus FromFinalLambda(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        TStatus status = UpdateAllChildLambdasConstraints(*input);
        if (status != TStatus::Ok) {
            return status;
        }

        TApplyConstraintFromInput<LambdaIdx, TMultiConstraintNode, TEmptyConstraintNode>::Do(input);
        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    TStatus InheriteEmptyFromInput(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        auto status = UpdateAllChildLambdasConstraints(*input);
        if (status != TStatus::Ok) {
            return status;
        }
        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    template<bool Sort>
    TStatus WideTopWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if constexpr (Sort) {
            TSortedConstraintNode::TContainerType sorted;
            sorted.reserve(input->Tail().ChildrenSize());

            for (const auto& item : input->Tail().Children()) {
                if (item->Tail().IsCallable("Bool"))
                    sorted.emplace_back(std::make_pair(TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType(1U, item->Head().Content())}, FromString<bool>(item->Tail().Tail().Content())));
                else
                    break;
            }

            if (!sorted.empty()) {
                input->AddConstraint(ctx.MakeConstraint<TSortedConstraintNode>(std::move(sorted)));
            }
        }

        return FromFirst<TEmptyConstraintNode, TUniqueConstraintNode, TDistinctConstraintNode, TVarIndexConstraintNode>(input, output, ctx);
    }

    TStatus SortWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (const auto status = UpdateLambdaConstraints(input->Tail()); status != TStatus::Ok) {
            return status;
        }

        if (const auto sorted = DeduceSortConstraint(*input->Child(1), *input->Child(2), ctx)) {
            input->AddConstraint(sorted->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
        }

        return FromFirst<TEmptyConstraintNode, TUniqueConstraintNode, TDistinctConstraintNode, TVarIndexConstraintNode>(input, output, ctx);
    }

    template<bool Distinct, bool Strict>
    TStatus AssumeUniqueWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        typename TUniqueConstraintNodeBase<Distinct>::TContentType content;
        for (auto i = 1U; i < input->ChildrenSize(); ++i) {
            TPartOfConstraintBase::TSetOfSetsType sets;
            sets.reserve(input->Child(i)->ChildrenSize());
            for (const auto& list : input->Child(i)->Children()) {
                if (list->IsAtom())
                    sets.insert_unique(TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType(1U, list->Content())});
                else if (list->IsList()) {
                    TPartOfConstraintBase::TSetType columns;
                    columns.reserve(list->ChildrenSize());
                    for (const auto& column: list->Children()) {
                        if (column->IsAtom())
                            columns.insert_unique(TPartOfConstraintBase::TPathType(1U, column->Content()));
                        else if (column->IsList()) {
                            TPartOfConstraintBase::TPathType path(column->ChildrenSize());
                            std::transform(column->Children().cbegin(), column->Children().cend(), path.begin(), [](const TExprNode::TPtr& atom) { return atom->Content(); } );
                            columns.insert_unique(std::move(path));
                        }
                    }
                    sets.insert_unique(std::move(columns));
                }
            }
            content.insert_unique(std::move(sets));
        }

        if (content.empty())
            content.insert_unique(TPartOfConstraintBase::TSetOfSetsType{TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType()}});

        auto constraint = ctx.MakeConstraint<TUniqueConstraintNodeBase<Distinct>>(std::move(content));
        if (!constraint->IsApplicableToType(*input->GetTypeAnn())) {
            if constexpr  (Strict) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << *constraint
                    << " is not applicable to " << *input->GetTypeAnn()));
            } else {
                auto issue = TIssue(ctx.GetPosition(input->Pos()),
                    TStringBuilder() << (Distinct ? "Distinct" : "Unique") << " sql hint contains invalid column: " << Endl
                                     << *constraint << " is not applicable to " << *input->GetTypeAnn());
                SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_HINT_INVALID_PARAMETERS, issue);
                if (ctx.AddWarning(issue)) {
                    output = input->HeadPtr();
                    return IGraphTransformer::TStatus::Repeat;
                }
            }
            return IGraphTransformer::TStatus::Error;
        }

        if constexpr (!Strict) {
            output = ctx.RenameNode(*input, Distinct ? "AssumeDistinct" : "AssumeUnique");
            return IGraphTransformer::TStatus::Repeat;
        }

        if (const auto old = input->Head().GetConstraint<TUniqueConstraintNodeBase<Distinct>>()) {
            if (old->Includes(*constraint)) {
                output = input->HeadPtr();
                return TStatus::Repeat;
            } else
                constraint = TUniqueConstraintNodeBase<Distinct>::Merge(old, constraint, ctx);
        }

        input->AddConstraint(constraint);
        return FromFirst<TSortedConstraintNode, TChoppedConstraintNode, TUniqueConstraintNodeBase<!Distinct>, TEmptyConstraintNode, TVarIndexConstraintNode>(input, output, ctx);
    }

    TStatus AssumeChoppedWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        TPartOfConstraintBase::TSetOfSetsType sets;
        for (auto i = 1U; i < input->ChildrenSize(); ++i) {
            TPartOfConstraintBase::TSetType columns;
            columns.reserve(input->Child(i)->ChildrenSize());
            for (const auto& column: input->Child(i)->Children()) {
                if (column->IsAtom())
                    columns.insert_unique(TPartOfConstraintBase::TPathType(1U, column->Content()));
                else if (column->IsList()) {
                    TPartOfConstraintBase::TPathType path(column->ChildrenSize());
                    std::transform(column->Children().cbegin(), column->Children().cend(), path.begin(), [](const TExprNode::TPtr& atom) { return atom->Content(); } );
                    columns.insert_unique(std::move(path));
                }
            }
            sets.insert_unique(std::move(columns));
        }

        const auto constraint = ctx.MakeConstraint<TChoppedConstraintNode>(std::move(sets));
        if (!constraint->IsApplicableToType(*input->GetTypeAnn())) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << *constraint
                << " is not applicable to " << *input->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        if (const auto old = input->Head().GetConstraint<TChoppedConstraintNode>()) {
            if (old->Equals(*constraint)) {
                output = input->HeadPtr();
                return TStatus::Repeat;
            }
        }

        input->AddConstraint(constraint);
        return FromFirst<TSortedConstraintNode, TDistinctConstraintNode, TUniqueConstraintNode, TEmptyConstraintNode, TVarIndexConstraintNode>(input, output, ctx);
    }

    template <bool UseSort>
    TStatus TopWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (const auto status = UpdateLambdaConstraints(input->Tail()); status != TStatus::Ok) {
            return status;
        }

        if constexpr (UseSort) {
            if (const auto sorted = DeduceSortConstraint(*input->Child(2), *input->Child(3), ctx)) {
                input->AddConstraint(sorted->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
            }
        }

        return FromFirst<TEmptyConstraintNode, TUniqueConstraintNode, TDistinctConstraintNode, TVarIndexConstraintNode>(input, output, ctx);
    }

    template<class TConstraint>
    static void FilterFromHead(const TExprNode& input, TConstraintSet& constraints, const TPartOfConstraintBase::TPathFilter& filter, TExprContext& ctx) {
        if (const auto source = input.Head().GetConstraint<TConstraint>()) {
            if (const auto filtered = source->FilterFields(ctx, filter)) {
                constraints.AddConstraint(filtered);
            }
        }
    }

    template<class TConstraint, bool Simplify = false>
    static void ReduceFromHead(const TExprNode::TPtr& input, const TPartOfConstraintBase::TPathReduce& reduce, TExprContext& ctx) {
        if (const auto source = input->Head().GetConstraint<TConstraint>()) {
            if (const auto filtered = source->RenameFields(ctx, reduce)) {
                if constexpr (Simplify)
                    input->AddConstraint(filtered->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
                else
                    input->AddConstraint(filtered);
            }
        }
    }

    template<class TConstraint, bool Simplify = false>
    static void FilterFromHead(const TExprNode::TPtr& input, const TPartOfConstraintBase::TPathFilter& filter, TExprContext& ctx) {
        if (const auto source = input->Head().GetConstraint<TConstraint>()) {
            if (const auto filtered = source->FilterFields(ctx, filter)) {
                if constexpr (Simplify)
                    input->AddConstraint(filtered->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
                else
                    input->AddConstraint(filtered);
            }
        }
    }

    template<class TConstraint>
    static void FilterFromHeadIfMissed(const TExprNode::TPtr& input, const TPartOfConstraintBase::TPathFilter& filter, TExprContext& ctx) {
        if (!input->GetConstraint<TConstraint>())
            FilterFromHead<TConstraint>(input, filter, ctx);
    }

    TStatus SelectMembersWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        auto outItemType = input->GetTypeAnn();
        while (outItemType->GetKind() == ETypeAnnotationKind::Optional) {
            outItemType = outItemType->Cast<TOptionalExprType>()->GetItemType();
        }

        if (outItemType->GetKind() == ETypeAnnotationKind::Variant) {
            if (outItemType->Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
                const auto outSize = outItemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>()->GetSize();

                auto multi = input->Head().GetConstraint<TMultiConstraintNode>();
                if (multi && multi->GetItems().back().first >= outSize) {
                    TMultiConstraintNode::TMapType filteredItems;
                    for (auto& item: multi->GetItems()) {
                        if (item.first < outSize) {
                            filteredItems.push_back(item);
                        }
                    }
                    multi = filteredItems.empty() ? nullptr : ctx.MakeConstraint<TMultiConstraintNode>(std::move(filteredItems));
                }
                if (multi) {
                    input->AddConstraint(multi);
                }

                auto varIndex = input->Head().GetConstraint<TVarIndexConstraintNode>();
                if (varIndex && varIndex->GetIndexMapping().back().first >= outSize) {
                    TVarIndexConstraintNode::TMapType filteredItems;
                    for (auto& item: varIndex->GetIndexMapping()) {
                        if (item.first < outSize) {
                            filteredItems.push_back(item);
                        }
                    }
                    varIndex = filteredItems.empty() ? nullptr : ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(filteredItems));
                }
                if (varIndex) {
                    input->AddConstraint(varIndex);
                }
            }
        }
        else if (outItemType->GetKind() == ETypeAnnotationKind::Struct) {
            const auto filter = [outItemType](const TPartOfConstraintBase::TPathType& path) {
                return !path.empty() && TPartOfConstraintBase::GetSubTypeByPath(path, *outItemType);
            };

            FilterFromHead<TPartOfSortedConstraintNode>(input, filter, ctx);
            FilterFromHead<TPartOfChoppedConstraintNode>(input, filter, ctx);
            FilterFromHead<TPartOfUniqueConstraintNode>(input, filter, ctx);
            FilterFromHead<TPartOfDistinctConstraintNode>(input, filter, ctx);
        }

        return TStatus::Ok;
    }

    template <bool Strict>
    TStatus CastWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        const auto outItemType = input->GetTypeAnn();
        const auto inItemType = input->Head().GetTypeAnn();
        const auto filter = [inItemType, outItemType, toString = input->IsCallable({"ToString", "ToBytes"})](const TPartOfConstraintBase::TPathType& path) {
            if (const auto outType = TPartOfConstraintBase::GetSubTypeByPath(path, *outItemType)) {
                const auto inType = TPartOfConstraintBase::GetSubTypeByPath(path, *inItemType);
                return (toString && inType->GetKind() == ETypeAnnotationKind::Data && inType->Cast<TDataExprType>()->GetSlot() == EDataSlot::Utf8) || IsSameAnnotation(*outType, *inType);
            }
            return false;
        };

        const auto filterForUnique = [inItemType, outItemType](const TPartOfConstraintBase::TPathType& path) {
            const auto castResult = CastResult<Strict>(TPartOfConstraintBase::GetSubTypeByPath(path, *inItemType), TPartOfConstraintBase::GetSubTypeByPath(path, *outItemType));
            return NUdf::ECastOptions::Complete == castResult || NUdf::ECastOptions::MayFail == castResult;
        };

        const auto filterForDistinct = [inItemType, outItemType](const TPartOfConstraintBase::TPathType& path) {
            return NUdf::ECastOptions::Complete == CastResult<Strict>(TPartOfConstraintBase::GetSubTypeByPath(path, *inItemType), TPartOfConstraintBase::GetSubTypeByPath(path, *outItemType));
        };

        FilterFromHead<TSortedConstraintNode>(input, filter, ctx);
        FilterFromHead<TChoppedConstraintNode>(input, filter, ctx);
        FilterFromHead<TUniqueConstraintNode>(input, filterForUnique, ctx);
        FilterFromHead<TDistinctConstraintNode>(input, filterForDistinct, ctx);
        FilterFromHead<TPartOfSortedConstraintNode>(input, filter, ctx);
        FilterFromHead<TPartOfChoppedConstraintNode>(input, filter, ctx);
        FilterFromHead<TPartOfUniqueConstraintNode>(input, filterForUnique, ctx);
        FilterFromHead<TPartOfDistinctConstraintNode>(input, filterForDistinct, ctx);
        return TStatus::Ok;
    }

    TStatus DivePrefixMembersWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        const auto prefixes = input->Tail().Children();
        const auto rename = [&prefixes](const TPartOfConstraintBase::TPathType& path) -> std::vector<TPartOfConstraintBase::TPathType> {
            if (path.empty())
                return {};

            for (const auto& p : prefixes) {
                if (const auto& prefix = p->Content(); path.front().starts_with(prefix)) {
                    auto out = path;
                    out.front() = out.front().substr(prefix.length());
                    return {std::move(out)};
                }
            }

            return {};
        };

        ReduceFromHead<TPartOfSortedConstraintNode>(input, rename, ctx);
        ReduceFromHead<TPartOfChoppedConstraintNode>(input, rename, ctx);
        ReduceFromHead<TPartOfUniqueConstraintNode>(input, rename, ctx);
        ReduceFromHead<TPartOfDistinctConstraintNode>(input, rename, ctx);
        return FromFirst<TVarIndexConstraintNode>(input, output, ctx);
    }

    TStatus ExtractMembersWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        const auto outItemType = GetSeqItemType(*input->GetTypeAnn()).Cast<TStructExprType>();
        const auto filter = [outItemType](const TPartOfConstraintBase::TPathType& path) { return !path.empty() && outItemType->FindItem(path.front()); };
        FilterFromHead<TSortedConstraintNode>(input, filter, ctx);
        FilterFromHead<TChoppedConstraintNode>(input, filter, ctx);
        FilterFromHead<TUniqueConstraintNode>(input, filter, ctx);
        FilterFromHead<TDistinctConstraintNode>(input, filter, ctx);
        FilterFromHead<TPartOfSortedConstraintNode>(input, filter, ctx);
        FilterFromHead<TPartOfChoppedConstraintNode>(input, filter, ctx);
        FilterFromHead<TPartOfUniqueConstraintNode>(input, filter, ctx);
        FilterFromHead<TPartOfDistinctConstraintNode>(input, filter, ctx);
        return FromFirst<TEmptyConstraintNode, TVarIndexConstraintNode>(input, output, ctx);
    }

    TStatus RemovePrefixMembersWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        const TTypeAnnotationNode* outItemType = GetSeqItemType(input->GetTypeAnn());
        if (!outItemType) {
            outItemType = input->GetTypeAnn();
        }

        if (outItemType->GetKind() == ETypeAnnotationKind::Struct) {
            const auto outStructType = outItemType->Cast<TStructExprType>();
            const auto filter = [outStructType](const TPartOfConstraintBase::TPathType& path) { return !path.empty() && outStructType->FindItem(path.front()); };
            FilterFromHead<TPartOfSortedConstraintNode>(input, filter, ctx);
            FilterFromHead<TPartOfChoppedConstraintNode>(input, filter, ctx);
            FilterFromHead<TPartOfUniqueConstraintNode>(input, filter, ctx);
            FilterFromHead<TPartOfDistinctConstraintNode>(input, filter, ctx);
        }
        else if (outItemType->GetKind() == ETypeAnnotationKind::Variant) {
            if (auto multi = input->Head().GetConstraint<TMultiConstraintNode>()) {
                TMultiConstraintNode::TMapType multiItems;
                auto tupleUnderType = outItemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
                for (auto& item: multi->GetItems()) {
                    YQL_ENSURE(item.first < tupleUnderType->GetSize());

                    auto& constr = multiItems[item.first];
                    const auto outStructType = tupleUnderType->GetItems()[item.first]->Cast<TStructExprType>();
                    const auto filter = [outStructType](const TPartOfConstraintBase::TPathType& path) { return !path.empty() && outStructType->FindItem(path.front()); };
                    FilterFromHead<TPartOfSortedConstraintNode>(*input, constr, filter, ctx);
                    FilterFromHead<TPartOfChoppedConstraintNode>(*input, constr, filter, ctx);
                    FilterFromHead<TPartOfUniqueConstraintNode>(*input, constr, filter, ctx);
                    FilterFromHead<TPartOfDistinctConstraintNode>(*input, constr, filter, ctx);
                }
                input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(std::move(multiItems)));
            }
        }

        return FromFirst<TEmptyConstraintNode, TVarIndexConstraintNode>(input, output, ctx);
    }

    // TODO: Empty for false condition
    template <bool Ordered>
    TStatus FilterWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (const auto status = UpdateLambdaConstraints(*input->Child(1)); status != TStatus::Ok) {
            return status;
        }

        if constexpr (Ordered) {
            FromFirst<TSortedConstraintNode, TPartOfSortedConstraintNode, TChoppedConstraintNode, TPartOfChoppedConstraintNode>(input, output, ctx);
        }

        return FromFirst<TEmptyConstraintNode, TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>(input, output, ctx);
    }

    template<class TConstraint>
    static const TConstraint* GetConstraintFromWideResultLambda(const TExprNode& lambda, TExprContext& ctx);

    template<class TConstraintType>
    static const TConstraintType* GetLambdaConstraint(const TExprNode& lambda, TExprContext& ctx) {
        if (2U == lambda.ChildrenSize())
            return lambda.Tail().GetConstraint<TConstraintType>();

        TVector<const TConstraintSet*> constraints;
        constraints.reserve(lambda.ChildrenSize() - 1U);
        for (size_t i = 1U; i < lambda.ChildrenSize(); ++i) {
            constraints.emplace_back(&lambda.Child(i)->GetConstraintSet());
        }
        return TConstraintType::MakeCommon(constraints, ctx);
    }

    template<class TConstraintType, bool WideLambda>
    static const TConstraintType* GetConstraintFromLambda(const TExprNode& lambda, TExprContext& ctx) {
        if constexpr (WideLambda)
            return GetConstraintFromWideResultLambda<TConstraintType>(lambda, ctx);
        else
            return GetLambdaConstraint<TConstraintType>(lambda, ctx);
    }

    static std::optional<bool> GetDirection(const TExprNode& dir) {
        if (dir.IsCallable("Bool"))
            return IsTrue(dir.Tail().Content());

        if (dir.IsCallable("Not"))
            if (const auto d = GetDirection(dir.Head()))
                return !*d;

        return std::nullopt;
    }

    static std::vector<std::pair<TPartOfConstraintBase::TPathType, bool>>
    ExtractSimpleSortTraits(const TExprNode& sortDirections, const TExprNode& keySelectorLambda) {
        const auto& keySelectorBody = keySelectorLambda.Tail();
        const auto& keySelectorArg = keySelectorLambda.Head().Head();
        std::vector<std::pair<TPartOfConstraintBase::TPathType, bool>> columns;
        if (const auto dir = GetDirection(sortDirections))
            columns.emplace_back(TPartOfConstraintBase::TPathType(), *dir);
        else if (sortDirections.IsList())
            if (const auto size = keySelectorBody.ChildrenSize()) {
                columns.reserve(size);
                for (auto i = 0U; i < size; ++i)
                    if (const auto dir = GetDirection(*sortDirections.Child(i)))
                        columns.emplace_back(TPartOfConstraintBase::TPathType(), *dir);
                    else
                        return {};
            } else
                return {};
        else
            return {};

        if (keySelectorBody.IsList())
            if (const auto size = keySelectorBody.ChildrenSize()) {
                TPartOfConstraintBase::TSetType set;
                set.reserve(size);
                columns.resize(size, std::make_pair(TPartOfConstraintBase::TPathType(), columns.back().second));
                auto it = columns.begin();
                for (auto i = 0U; i < size; ++i) {
                    if (auto path = GetPathToKey<true>(*keySelectorBody.Child(i), keySelectorArg)) {
                        if (set.insert(*path).second)
                            it++->first = std::move(*path);
                        else if (columns.cend() != it)
                            it = columns.erase(it);
                    } else {
                        return {};
                    }
                }
            } else
                return {};
        else if (auto path = GetPathToKey<true>(keySelectorBody, keySelectorArg))
            if (columns.size() == 1U)
                columns.front().first = std::move(*path);
            else
                return {};
        else
            return {};

        return columns;
    }

    template<class TConstraint, bool OrderedMap, class TInput>
    static void GetFromMapLambda(const TInput& input, const TConstraintSet& handler, TConstraintSet& output, bool isSingleItem, TExprContext& ctx) {
        constexpr bool isOrderConstraint = std::is_same<typename TConstraint::TMainConstraint, TSortedConstraintNode>() || std::is_same<typename TConstraint::TMainConstraint, TChoppedConstraintNode>();
        if (const auto lambda = handler.GetConstraint<TConstraint>()) {
            const auto original = input.template GetConstraint<typename TConstraint::TMainConstraint>();
            if constexpr (OrderedMap || !isOrderConstraint) {
                if (original) {
                    if (const auto complete = TConstraint::MakeComplete(ctx, lambda->GetColumnMapping(), original)) {
                        output.AddConstraint(complete);
                    }
                }
            }
            if (const auto part = input.template GetConstraint<TConstraint>()) {
                auto mapping = lambda->GetColumnMapping();
                for (auto it = mapping.cbegin(); mapping.cend() != it;) {
                    if (part->GetColumnMapping().contains(it->first))
                        ++it;
                    else
                        it = mapping.erase(it);
                }
                if (!mapping.empty()) {
                    output.AddConstraint(ctx.MakeConstraint<TConstraint>(std::move(mapping)));
                }
            } else if (isOrderConstraint || isSingleItem) {
                if (const auto filtered = lambda->RemoveOriginal(ctx, original))
                    output.AddConstraint(filtered);
            }
        }
    }

    template<class TConstraint, bool OrderedMap, bool WideOutput>
    static void GetFromMapLambda(const TExprNode::TPtr& input, bool isSingleItem, TExprContext& ctx) {
        constexpr bool isOrderConstraint = std::is_same<typename TConstraint::TMainConstraint, TSortedConstraintNode>() || std::is_same<typename TConstraint::TMainConstraint, TChoppedConstraintNode>();
        if (const auto lambda = GetConstraintFromLambda<TConstraint, WideOutput>(input->Tail(), ctx)) {
            const auto original = GetDetailed(input->Head().GetConstraint<typename TConstraint::TMainConstraint>(), *input->Head().GetTypeAnn(), ctx);
            if constexpr (OrderedMap || !isOrderConstraint) {
                if (original) {
                    if (const auto complete = TConstraint::MakeComplete(ctx, lambda->GetColumnMapping(), original)) {
                        input->AddConstraint(complete->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
                    }
                }
            }
            if (const auto part = input->Head().GetConstraint<TConstraint>()) {
                auto mapping = lambda->GetColumnMapping();
                for (auto it = mapping.cbegin(); mapping.cend() != it;) {
                    if (part->GetColumnMapping().contains(it->first))
                        ++it;
                    else
                        it = mapping.erase(it);
                }
                if (!mapping.empty()) {
                    input->AddConstraint(ctx.MakeConstraint<TConstraint>(std::move(mapping)));
                }
            } else if (isOrderConstraint || isSingleItem) {
                if (const auto filtered = lambda->RemoveOriginal(ctx, original))
                    input->AddConstraint(filtered);
            }
        }
    }

    template <bool Ordered, bool Flat, bool WideInput = false, bool WideOutput = false>
    TStatus MapWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        auto argConstraints = GetConstraintsForInputArgument<Ordered, WideInput>(*input, ctx);

        if constexpr (Ordered && !(Flat || WideInput || WideOutput)) {
            // TODO: is temporary crutch for MapNext.
            if (argConstraints.size() < input->Tail().Head().ChildrenSize())
                argConstraints.resize(input->Tail().Head().ChildrenSize(), argConstraints.front());
        }

        if (const auto status = UpdateLambdaConstraints(input->TailRef(), ctx, argConstraints); status != TStatus::Ok) {
            return status;
        }

        const bool singleItem = ETypeAnnotationKind::Optional == input->GetTypeAnn()->GetKind();
        GetFromMapLambda<TPartOfUniqueConstraintNode, Ordered, WideOutput>(input, singleItem, ctx);
        GetFromMapLambda<TPartOfDistinctConstraintNode, Ordered, WideOutput>(input, singleItem, ctx);
        GetFromMapLambda<TPartOfSortedConstraintNode, Ordered, WideOutput>(input, singleItem, ctx);
        GetFromMapLambda<TPartOfChoppedConstraintNode, Ordered, WideOutput>(input, singleItem, ctx);

        const auto lambdaVarIndex = GetConstraintFromLambda<TVarIndexConstraintNode, WideOutput>(input->Tail(), ctx);
        const auto lambdaMulti = GetConstraintFromLambda<TMultiConstraintNode, WideOutput>(input->Tail(), ctx);
        const auto inItemType = GetSeqItemType(input->Head().GetTypeAnn());
        const bool multiInput = ETypeAnnotationKind::Variant == inItemType->GetKind();
        if (const auto varIndex = input->Head().GetConstraint<TVarIndexConstraintNode>()) {
            if (multiInput) {
                if (lambdaVarIndex) {
                    if (const auto outVarIndex = GetVarIndexOverVarIndexConstraint(*varIndex, *lambdaVarIndex, ctx)) {
                        input->AddConstraint(outVarIndex);
                    }
                }
            } else {
                if (lambdaMulti) {
                    TVarIndexConstraintNode::TMapType remapItems;
                    for (auto& multiItem: lambdaMulti->GetItems()) {
                        for (auto& varItem: varIndex->GetIndexMapping()) {
                            remapItems.push_back(std::make_pair(multiItem.first, varItem.second));
                        }
                    }
                    if (!remapItems.empty()) {
                        ::SortUnique(remapItems);
                        input->AddConstraint(ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(remapItems)));
                    }
                } else {
                    input->AddConstraint(varIndex);
                }
            }
        }

        const auto inputMulti = input->Head().GetConstraint<TMultiConstraintNode>();
        if (lambdaMulti && !input->Head().GetConstraint<TEmptyConstraintNode>()) {
            TMultiConstraintNode::TMapType remappedItems;
            for (auto& item: lambdaMulti->GetItems()) {
                remappedItems.push_back(std::make_pair(item.first, TConstraintSet{}));
                if (!multiInput) { // remapping one to many
                    GetFromMapLambda<TPartOfUniqueConstraintNode, Ordered>(input->Head(), item.second, remappedItems.back().second, singleItem, ctx);
                    GetFromMapLambda<TPartOfDistinctConstraintNode, Ordered>(input->Head(), item.second, remappedItems.back().second, singleItem, ctx);
                    GetFromMapLambda<TPartOfSortedConstraintNode, Ordered>(input->Head(), item.second, remappedItems.back().second, singleItem, ctx);
                    GetFromMapLambda<TPartOfChoppedConstraintNode, Ordered>(input->Head(), item.second, remappedItems.back().second, singleItem, ctx);

                    if (const auto empty = item.second.template GetConstraint<TEmptyConstraintNode>()) {
                        remappedItems.pop_back();
                    }
                }
                else if (lambdaVarIndex && inputMulti) {
                    const auto range = lambdaVarIndex->GetIndexMapping().equal_range(item.first);
                    switch (std::distance(range.first, range.second)) {
                    case 0: // new index
                        break;
                    case 1: // remapping 1 to 1
                        if (const auto origConstr = inputMulti->GetItem(range.first->second)) {
                            GetFromMapLambda<TPartOfUniqueConstraintNode, Ordered>(*origConstr, item.second, remappedItems.back().second, singleItem, ctx);
                            GetFromMapLambda<TPartOfDistinctConstraintNode, Ordered>(*origConstr, item.second, remappedItems.back().second, singleItem, ctx);
                            GetFromMapLambda<TPartOfSortedConstraintNode, Ordered>(*origConstr, item.second, remappedItems.back().second, singleItem, ctx);
                            GetFromMapLambda<TPartOfChoppedConstraintNode, Ordered>(*origConstr, item.second, remappedItems.back().second, singleItem, ctx);

                            if (const auto empty = item.second.template GetConstraint<TEmptyConstraintNode>()) {
                                remappedItems.pop_back();
                            }
                        } else {
                            remappedItems.pop_back();
                        }
                        break;
                    default: // remapping many to one
                        {
                            std::vector<const TConstraintSet*> nonEmpty;
                            for (auto i = range.first; i != range.second; ++i) {
                                if (auto origConstr = inputMulti->GetItem(i->second)) {
                                    nonEmpty.push_back(origConstr);
                                }
                            }
                            EraseIf(nonEmpty, [] (const TConstraintSet* c) { return !!c->GetConstraint<TEmptyConstraintNode>(); });

                            if (nonEmpty.empty()) {
                                remappedItems.back().second.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
                            } else if (nonEmpty.size() == 1) {
                                remappedItems.back().second = std::move(*nonEmpty.front());
                            }
                        }
                    }
                } else {
                    remappedItems.back().second = item.second;
                }
            }
            if (remappedItems) {
                input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(std::move(remappedItems)));
            }
        }
        else if (inputMulti && lambdaVarIndex) { // Many to one
            const auto range = lambdaVarIndex->GetIndexMapping().equal_range(0);
            std::vector<const TConstraintSet*> nonEmpty;
            for (auto i = range.first; i != range.second; ++i) {
                if (auto origConstr = inputMulti->GetItem(i->second)) {
                    nonEmpty.push_back(origConstr);
                }
            }
            EraseIf(nonEmpty, [] (const TConstraintSet* c) { return !!c->GetConstraint<TEmptyConstraintNode>(); });

            if (nonEmpty.empty()) {
                input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
            } else if (nonEmpty.size() == 1) {
                input->SetConstraints(*nonEmpty.front());
            }
        }

        if constexpr (Flat) {
            if (const auto lambdaEmpty = GetConstraintFromLambda<TEmptyConstraintNode, WideOutput>(input->Tail(), ctx)) {
                input->AddConstraint(lambdaEmpty);

                const auto& filter = std::bind(&TPartOfConstraintBase::GetSubTypeByPath, std::placeholders::_1, std::cref(GetSeqItemType(*input->GetTypeAnn())));
                FilterFromHeadIfMissed<TUniqueConstraintNode>(input, filter, ctx);
                FilterFromHeadIfMissed<TDistinctConstraintNode>(input, filter, ctx);
                if constexpr (Ordered) {
                    FilterFromHeadIfMissed<TSortedConstraintNode>(input, filter, ctx);
                    FilterFromHeadIfMissed<TChoppedConstraintNode>(input, filter, ctx);
                }
            }
        }

        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    template <bool Ordered>
    TStatus LMapWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        TConstraintNode::TListType argConstraints;
        for (const auto c: input->Head().GetAllConstraints()) {
            if (Ordered || (c->GetName() != TSortedConstraintNode::Name() && c->GetName() != TChoppedConstraintNode::Name())) {
                argConstraints.push_back(c);
            }
        }

        if (const auto status = UpdateLambdaConstraints(input->TailRef(), ctx, {argConstraints}); status != TStatus::Ok) {
            return status;
        }

        TSet<TStringBuf> except;
        if constexpr (!Ordered) {
            except.insert(TSortedConstraintNode::Name());
            except.insert(TChoppedConstraintNode::Name());
        }
        if (input->Tail().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            except.insert(TEmptyConstraintNode::Name());
        }
        CopyExcept(*input, input->Tail(), except);

        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    TStatus AsListWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (input->ChildrenSize() == 1) {
            if (const auto unique = input->Head().GetConstraint<TUniqueConstraintNode>()) {
                input->AddConstraint(unique);
            }
            if (const auto unique = input->Head().GetConstraint<TDistinctConstraintNode>()) {
                input->AddConstraint(unique);
            }
            if (const auto part = input->Head().GetConstraint<TPartOfUniqueConstraintNode>()) {
                input->AddConstraint(part);
            }
            if (const auto part = input->Head().GetConstraint<TPartOfDistinctConstraintNode>()) {
                input->AddConstraint(part);
            }
        }

        return CommonFromChildren<0, TPartOfSortedConstraintNode, TPartOfChoppedConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>(input, output, ctx);
    }

    template<bool Ordered>
    TStatus ExtendWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (input->ChildrenSize() == 1) {
            if (const auto unique = input->Head().GetConstraint<TUniqueConstraintNode>()) {
                input->AddConstraint(unique);
            }
            if (const auto part = input->Head().GetConstraint<TPartOfUniqueConstraintNode>()) {
                input->AddConstraint(part);
            }
            if (const auto unique = input->Head().GetConstraint<TDistinctConstraintNode>()) {
                input->AddConstraint(unique);
            }
            if (const auto part = input->Head().GetConstraint<TPartOfDistinctConstraintNode>()) {
                input->AddConstraint(part);
            }

            if constexpr (Ordered) {
                if (const auto sorted = input->Head().GetConstraint<TSortedConstraintNode>()) {
                    input->AddConstraint(sorted);
                }

                if (const auto part = input->Head().GetConstraint<TPartOfSortedConstraintNode>()) {
                    input->AddConstraint(part);
                }

                if (const auto sorted = input->Head().GetConstraint<TChoppedConstraintNode>()) {
                    input->AddConstraint(sorted);
                }

                if (const auto part = input->Head().GetConstraint<TPartOfChoppedConstraintNode>()) {
                    input->AddConstraint(part);
                }
            }
        }

        return CommonFromChildren<0, TEmptyConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>(input, output, ctx);
    }

    template <bool Union>
    TStatus MergeWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (auto sort = MakeCommonConstraint<TSortedConstraintNode>(input, 0, ctx)) {
            if (Union && input->ChildrenSize() > 1) {
                // Check and exclude modified keys from final constraint
                const auto resultItemType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                std::vector<const TTypeAnnotationNode*> inputs;
                for (const auto& child: input->Children()) {
                    inputs.emplace_back(child->GetTypeAnn()->Cast<TListExprType>()->GetItemType());
                }

                auto content = sort->GetContent();
                for (auto i = 0U; i < content.size(); ++i) {
                    for (auto it = content[i].first.cbegin(); content[i].first.cend() != it;) {
                        const auto resultItemSubType = TPartOfConstraintBase::GetSubTypeByPath(*it, *resultItemType);
                        YQL_ENSURE(resultItemSubType, "Missing " << *it << " in result type");
                        auto childNdx = 0U;
                        while (childNdx < input->ChildrenSize()) {
                            if (const auto inputItemSubType = TPartOfConstraintBase::GetSubTypeByPath(*it, *inputs[childNdx])) {
                                if (IsSameAnnotation(*inputItemSubType, *resultItemSubType)) {
                                    ++childNdx;
                                    continue;
                                }
                            } else {
                                YQL_ENSURE(input->Child(childNdx)->GetConstraint<TEmptyConstraintNode>(), "Missing column " << *it << " in non empty input type");
                            }
                            break;
                        }

                        if (childNdx < input->ChildrenSize())
                            it = content[i].first.erase(it);
                        else
                            ++it;
                    }

                    if (content[i].first.empty()) {
                        content.resize(i);
                        break;
                    }
                }

                sort = content.empty() ? nullptr : ctx.MakeConstraint<TSortedConstraintNode>(std::move(content));
            }
            if (sort) {
                input->AddConstraint(sort);
            }
        }
        return ExtendWrap<false>(input, output, ctx);
    }

    TStatus TakeWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (input->Tail().IsCallable("Uint64") && !FromString<ui64>(input->Tail().Head().Content())) {
            input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        }
        return CopyAllFrom<0>(input, output, ctx);
    }

    TStatus MemberWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        const auto& memberName = input->Tail().Content();
        const auto& structNode = input->Head();
        if (const auto emptyConstraint = structNode.GetConstraint<TEmptyConstraintNode>()) {
            input->AddConstraint(emptyConstraint);
        } else {
            if (const auto part = structNode.GetConstraint<TPartOfSortedConstraintNode>()) {
                if (const auto extracted = part->ExtractField(ctx, memberName)) {
                    input->AddConstraint(extracted);
                }
            }
            if (const auto part = structNode.GetConstraint<TPartOfChoppedConstraintNode>()) {
                if (const auto extracted = part->ExtractField(ctx, memberName)) {
                    input->AddConstraint(extracted);
                }
            }
            if (const auto part = structNode.GetConstraint<TPartOfUniqueConstraintNode>()) {
                if (const auto extracted = part->ExtractField(ctx, memberName)) {
                    input->AddConstraint(extracted);
                }
            }
            if (const auto part = structNode.GetConstraint<TPartOfDistinctConstraintNode>()) {
                if (const auto extracted = part->ExtractField(ctx, memberName)) {
                    input->AddConstraint(extracted);
                }
            }
        }

        if (structNode.IsCallable("AsStruct")) {
            for (const auto& child: structNode.Children()) {
                if (child->Head().IsAtom(memberName)) {
                    TApplyConstraintFromInput<1, TVarIndexConstraintNode>::Do(child);
                    break;
                }
            }
        } else {
            TApplyConstraintFromInput<0, TVarIndexConstraintNode>::Do(input);
        }
        return TStatus::Ok;
    }

    TStatus AsTupleWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        TPartOfSortedConstraintNode::TMapType sorted;
        TPartOfChoppedConstraintNode::TMapType chopped;
        TPartOfUniqueConstraintNode::TMapType uniques;
        TPartOfDistinctConstraintNode::TMapType distincts;

        std::vector<const TConstraintSet*> structConstraints;
        for (auto i = 0U; i < input->ChildrenSize(); ++i) {
            const auto child = input->Child(i);
            const auto& name = ctx.GetIndexAsString(i);

            if (const auto part = child->GetConstraint<TPartOfSortedConstraintNode>()) {
                TPartOfSortedConstraintNode::UniqueMerge(sorted, part->GetColumnMapping(name));
            }

            if (const auto part = child->GetConstraint<TPartOfChoppedConstraintNode>()) {
                TPartOfChoppedConstraintNode::UniqueMerge(chopped, part->GetColumnMapping(name));
            }

            if (const auto part = child->GetConstraint<TPartOfUniqueConstraintNode>()) {
                TPartOfUniqueConstraintNode::UniqueMerge(uniques, part->GetColumnMapping(name));
            }

            if (const auto part = child->GetConstraint<TPartOfDistinctConstraintNode>()) {
                TPartOfDistinctConstraintNode::UniqueMerge(distincts, part->GetColumnMapping(name));
            }

            if (const auto& valueNode = SkipModifiers(child); TCoMember::Match(valueNode) || TCoNth::Match(valueNode)) {
                structConstraints.push_back(&valueNode->Head().GetConstraintSet());
            } else if (valueNode->IsArgument() && ETypeAnnotationKind::Struct != valueNode->GetTypeAnn()->GetKind()) {
                structConstraints.push_back(&valueNode->GetConstraintSet());
            }
        }
        if (!sorted.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfSortedConstraintNode>(std::move(sorted)));
        }
        if (!chopped.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfChoppedConstraintNode>(std::move(chopped)));
        }
        if (!uniques.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(uniques)));
        }
        if (!distincts.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfDistinctConstraintNode>(std::move(distincts)));
        }
        if (const auto varIndex = TVarIndexConstraintNode::MakeCommon(structConstraints, ctx)) {
            input->AddConstraint(varIndex);
        }

        return TStatus::Ok;
    }

    TStatus AsStructWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        TPartOfSortedConstraintNode::TMapType sorted;
        TPartOfChoppedConstraintNode::TMapType chopped;
        TPartOfUniqueConstraintNode::TMapType uniques;
        TPartOfDistinctConstraintNode::TMapType distincts;

        std::vector<const TConstraintSet*> structConstraints;
        for (const auto& child : input->Children()) {
            const auto& name = child->Head().Content();

            if (const auto part = child->Tail().GetConstraint<TPartOfSortedConstraintNode>()) {
                TPartOfSortedConstraintNode::UniqueMerge(sorted, part->GetColumnMapping(name));
            }
            if (const auto part = child->Tail().GetConstraint<TPartOfChoppedConstraintNode>()) {
                TPartOfChoppedConstraintNode::UniqueMerge(chopped, part->GetColumnMapping(name));
            }
            if (const auto part = child->Tail().GetConstraint<TPartOfUniqueConstraintNode>()) {
                TPartOfUniqueConstraintNode::UniqueMerge(uniques, part->GetColumnMapping(name));
            }
            if (const auto part = child->Tail().GetConstraint<TPartOfDistinctConstraintNode>()) {
                TPartOfDistinctConstraintNode::UniqueMerge(distincts, part->GetColumnMapping(name));
            }

            if (const auto valueNode = SkipModifiers(&child->Tail()); TCoMember::Match(valueNode) || TCoNth::Match(valueNode)) {
                structConstraints.push_back(&valueNode->Head().GetConstraintSet());
            } else if (valueNode->Type() == TExprNode::Argument) {
                structConstraints.push_back(&valueNode->GetConstraintSet());
            }
        }
        if (!sorted.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfSortedConstraintNode>(std::move(sorted)));
        }
        if (!chopped.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfChoppedConstraintNode>(std::move(chopped)));
        }
        if (!uniques.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(uniques)));
        }
        if (!distincts.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfDistinctConstraintNode>(std::move(distincts)));
        }
        if (const auto varIndex = TVarIndexConstraintNode::MakeCommon(structConstraints, ctx)) {
            input->AddConstraint(varIndex);
        }

        return TStatus::Ok;
    }

    TStatus FlattenMembersWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        TPartOfSortedConstraintNode::TMapType sorted;
        TPartOfChoppedConstraintNode::TMapType chopped;
        TPartOfUniqueConstraintNode::TMapType uniques;
        TPartOfDistinctConstraintNode::TMapType distincts;

        for (const auto& child : input->Children()) {
            const auto& prefix = child->Head().Content();
            if (const auto part = child->Tail().GetConstraint<TPartOfSortedConstraintNode>()) {
                TPartOfSortedConstraintNode::UniqueMerge(sorted, part->GetColumnMapping(ctx, prefix));
            }
            if (const auto part = child->Tail().GetConstraint<TPartOfChoppedConstraintNode>()) {
                TPartOfChoppedConstraintNode::UniqueMerge(chopped, part->GetColumnMapping(ctx, prefix));
            }
            if (const auto part = child->Tail().GetConstraint<TPartOfUniqueConstraintNode>()) {
                TPartOfUniqueConstraintNode::UniqueMerge(uniques, part->GetColumnMapping(ctx, prefix));
            }
            if (const auto part = child->Tail().GetConstraint<TPartOfDistinctConstraintNode>()) {
                TPartOfDistinctConstraintNode::UniqueMerge(distincts, part->GetColumnMapping(ctx, prefix));
            }
        }
        if (!sorted.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfSortedConstraintNode>(std::move(sorted)));
        }
        if (!chopped.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfChoppedConstraintNode>(std::move(chopped)));
        }
        if (!uniques.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(uniques)));
        }
        if (!distincts.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfDistinctConstraintNode>(std::move(distincts)));
        }
        return TStatus::Ok;
    }

    template<class TPartOfConstraint>
    static void AddPartOf(const TExprNode::TPtr& input, TExprContext& ctx) {
        typename TPartOfConstraint::TMapType map;
        if (const auto part = input->Head().GetConstraint<TPartOfConstraint>()) {
            map = part->GetColumnMapping();
        }
        if (const auto part = input->Tail().GetConstraint<TPartOfConstraint>()) {
            TPartOfConstraint::UniqueMerge(map, part->GetColumnMapping(input->Child(1)->Content()));
        }
        if (!map.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfConstraint>(std::move(map)));
        }
    }

    template<class TPartOfConstraint>
    static void ReplacePartOf(const TExprNode::TPtr& input, TExprContext& ctx) {
        typename TPartOfConstraint::TMapType sorted;
        const auto& name = input->Child(1)->Content();
        if (const auto part = input->Head().GetConstraint<TPartOfConstraint>()) {
            if (const auto filtered = part->FilterFields(ctx, [&name](const TPartOfConstraintBase::TPathType& path) { return !path.empty() && path.front() != name; })) {
                sorted = filtered->GetColumnMapping();
            }
        }
        if (const auto part = input->Tail().GetConstraint<TPartOfConstraint>()) {
            TPartOfConstraint::UniqueMerge(sorted, part->GetColumnMapping(name));
        }
        if (!sorted.empty()) {
            input->AddConstraint(ctx.MakeConstraint<TPartOfConstraint>(std::move(sorted)));
        }
    }

    TStatus AddMemberWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        const auto& addStructNode = input->Head();
        const auto& extraFieldNode = input->Tail();

        if (const auto emptyConstraint = addStructNode.GetConstraint<TEmptyConstraintNode>()) {
            input->AddConstraint(emptyConstraint);
        }

        AddPartOf<TPartOfSortedConstraintNode>(input, ctx);
        AddPartOf<TPartOfChoppedConstraintNode>(input, ctx);
        AddPartOf<TPartOfUniqueConstraintNode>(input, ctx);
        AddPartOf<TPartOfDistinctConstraintNode>(input, ctx);

        TVector<const TConstraintSet*> structConstraints;
        structConstraints.push_back(&addStructNode.GetConstraintSet());
        if (const auto& valueNode = SkipModifiers(&extraFieldNode); TCoMember::Match(valueNode) || TCoNth::Match(valueNode)) {
            structConstraints.push_back(&valueNode->Head().GetConstraintSet());
        } else if (valueNode->Type() == TExprNode::Argument) {
            structConstraints.push_back(&valueNode->GetConstraintSet());
        }

        if (const auto varIndex = TVarIndexConstraintNode::MakeCommon(structConstraints, ctx)) {
            input->AddConstraint(varIndex);
        }
        return TStatus::Ok;
    }

    TStatus RemoveMemberWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        const auto& name = input->Tail().Content();
        const auto filter = [&name](const TPartOfConstraintBase::TPathType& path) { return !path.empty() && path.front() != name; };
        FilterFromHead<TPartOfSortedConstraintNode>(input, filter, ctx);
        FilterFromHead<TPartOfChoppedConstraintNode>(input, filter, ctx);
        FilterFromHead<TPartOfUniqueConstraintNode>(input, filter, ctx);
        FilterFromHead<TPartOfDistinctConstraintNode>(input, filter, ctx);
        return FromFirst<TVarIndexConstraintNode>(input, output, ctx);
    }

    TStatus ReplaceMemberWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        TVector<const TConstraintSet*> structConstraints;
        structConstraints.push_back(&input->Head().GetConstraintSet());

        ReplacePartOf<TPartOfSortedConstraintNode>(input, ctx);
        ReplacePartOf<TPartOfChoppedConstraintNode>(input, ctx);
        ReplacePartOf<TPartOfUniqueConstraintNode>(input, ctx);
        ReplacePartOf<TPartOfDistinctConstraintNode>(input, ctx);

        if (const auto varIndex = TVarIndexConstraintNode::MakeCommon(structConstraints, ctx)) {
            input->AddConstraint(varIndex);
        }

        return TStatus::Ok;
    }

    TStatus ListWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        switch (input->ChildrenSize()) {
        case 1:
            return FromEmpty(input, output, ctx);
        case 2:
            return FromSecond<TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>(input, output, ctx);
        default:
            break;
        }
        return CommonFromChildren<1, TVarIndexConstraintNode, TMultiConstraintNode>(input, output, ctx);
    }

    template<bool Ordered, bool WideInput, bool WithUnique = true>
    static TSmallVec<TConstraintNode::TListType> GetConstraintsForInputArgument(const TExprNode& node, TExprContext& ctx) {
        TSmallVec<TConstraintNode::TListType> argsConstraints(WideInput ? node.Child(1U)->Head().ChildrenSize() : 1U);
        if constexpr (WideInput) {
            if constexpr (Ordered) {
                if (const auto& mapping = TPartOfSortedConstraintNode::GetCommonMapping(node.Head().GetConstraint<TSortedConstraintNode>(), node.Head().GetConstraint<TPartOfSortedConstraintNode>()); !mapping.empty()) {
                    for (ui32 i = 0U; i < argsConstraints.size(); ++i) {
                        if (auto extracted = TPartOfSortedConstraintNode::ExtractField(mapping, ctx.GetIndexAsString(i)); !extracted.empty()) {
                            argsConstraints[i].emplace_back(ctx.MakeConstraint<TPartOfSortedConstraintNode>(std::move(extracted)));
                        }
                    }
                }
                if (const auto& mapping = TPartOfChoppedConstraintNode::GetCommonMapping(node.Head().GetConstraint<TChoppedConstraintNode>(), node.Head().GetConstraint<TPartOfChoppedConstraintNode>()); !mapping.empty()) {
                    for (ui32 i = 0U; i < argsConstraints.size(); ++i) {
                        if (auto extracted = TPartOfChoppedConstraintNode::ExtractField(mapping, ctx.GetIndexAsString(i)); !extracted.empty()) {
                            argsConstraints[i].emplace_back(ctx.MakeConstraint<TPartOfChoppedConstraintNode>(std::move(extracted)));
                        }
                    }
                }
            }

            if constexpr (WithUnique) {
                if (const auto& mapping = TPartOfUniqueConstraintNode::GetCommonMapping(node.Head().GetConstraint<TUniqueConstraintNode>(), node.Head().GetConstraint<TPartOfUniqueConstraintNode>()); !mapping.empty()) {
                    for (ui32 i = 0U; i < argsConstraints.size(); ++i) {
                        if (auto extracted = TPartOfUniqueConstraintNode::ExtractField(mapping, ctx.GetIndexAsString(i)); !extracted.empty()) {
                            argsConstraints[i].emplace_back(ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(extracted)));
                        }
                    }
                }

                if (const auto& mapping = TPartOfDistinctConstraintNode::GetCommonMapping(node.Head().GetConstraint<TDistinctConstraintNode>(), node.Head().GetConstraint<TPartOfDistinctConstraintNode>()); !mapping.empty()) {
                    for (ui32 i = 0U; i < argsConstraints.size(); ++i) {
                        if (auto extracted = TPartOfDistinctConstraintNode::ExtractField(mapping, ctx.GetIndexAsString(i)); !extracted.empty()) {
                            argsConstraints[i].emplace_back(ctx.MakeConstraint<TPartOfDistinctConstraintNode>(std::move(extracted)));
                        }
                    }
                }
            }

        } else {
            if (const auto inItemType = GetSeqItemType(node.Head().GetTypeAnn())) {
                if (inItemType->GetKind() == ETypeAnnotationKind::Variant) {
                    if (inItemType->Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
                        const auto tupleType = inItemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
                        argsConstraints.front().push_back(ctx.MakeConstraint<TVarIndexConstraintNode>(*inItemType->Cast<TVariantExprType>()));
                        TMultiConstraintNode::TMapType multiItems;
                        multiItems.reserve(tupleType->GetSize());
                        for (size_t i = 0; i < tupleType->GetSize(); ++i) {
                            multiItems.emplace_back(i, TConstraintSet{});
                            if constexpr (WithUnique) {
                                const auto inputMulti = node.Head().GetConstraint<TMultiConstraintNode>();
                                if (const auto inputConstr = inputMulti ? inputMulti->GetItem(i) : nullptr) {
                                    if (auto mapping = TPartOfUniqueConstraintNode::GetCommonMapping(inputConstr->GetConstraint<TUniqueConstraintNode>(), inputConstr->GetConstraint<TPartOfUniqueConstraintNode>()); !mapping.empty()) {
                                        multiItems.back().second.AddConstraint(ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(mapping)));
                                    }
                                    if (auto mapping = TPartOfDistinctConstraintNode::GetCommonMapping(inputConstr->GetConstraint<TDistinctConstraintNode>(), inputConstr->GetConstraint<TPartOfDistinctConstraintNode>()); !mapping.empty()) {
                                        multiItems.back().second.AddConstraint(ctx.MakeConstraint<TPartOfDistinctConstraintNode>(std::move(mapping)));
                                    }
                                }
                            }
                        }
                        if (!multiItems.empty()) {
                            argsConstraints.front().emplace_back(ctx.MakeConstraint<TMultiConstraintNode>(std::move(multiItems)));
                        }
                    }
                } else {
                    if constexpr (Ordered) {
                        if (auto mapping = TPartOfSortedConstraintNode::GetCommonMapping(GetDetailed(node.Head().GetConstraint<TSortedConstraintNode>(), *node.Head().GetTypeAnn(), ctx), node.Head().GetConstraint<TPartOfSortedConstraintNode>()); !mapping.empty()) {
                            argsConstraints.front().emplace_back(ctx.MakeConstraint<TPartOfSortedConstraintNode>(std::move(mapping)));
                        }
                        if (auto mapping = TPartOfChoppedConstraintNode::GetCommonMapping(GetDetailed(node.Head().GetConstraint<TChoppedConstraintNode>(), *node.Head().GetTypeAnn(), ctx), node.Head().GetConstraint<TPartOfChoppedConstraintNode>()); !mapping.empty()) {
                            argsConstraints.front().emplace_back(ctx.MakeConstraint<TPartOfChoppedConstraintNode>(std::move(mapping)));
                        }
                    }

                    if constexpr (WithUnique) {
                        if (auto mapping = TPartOfUniqueConstraintNode::GetCommonMapping(GetDetailed(node.Head().GetConstraint<TUniqueConstraintNode>(), *node.Head().GetTypeAnn(), ctx), node.Head().GetConstraint<TPartOfUniqueConstraintNode>()); !mapping.empty()) {
                            argsConstraints.front().emplace_back(ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(mapping)));
                        }
                        if (auto mapping = TPartOfDistinctConstraintNode::GetCommonMapping(GetDetailed(node.Head().GetConstraint<TDistinctConstraintNode>(), *node.Head().GetTypeAnn(), ctx), node.Head().GetConstraint<TPartOfDistinctConstraintNode>()); !mapping.empty()) {
                            argsConstraints.front().emplace_back(ctx.MakeConstraint<TPartOfDistinctConstraintNode>(std::move(mapping)));
                        }
                    }
                }
            }
        }

        return argsConstraints;
    }

    TStatus DictWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        const std::vector<std::string_view> k(1U, ctx.GetIndexAsString(0U));
        input->AddConstraint(ctx.MakeConstraint<TUniqueConstraintNode>(k));
        input->AddConstraint(ctx.MakeConstraint<TDistinctConstraintNode>(k));
        if (input->ChildrenSize() == 1) {
            return FromEmpty(input, output, ctx);
        }
        return TStatus::Ok;
    }

    TStatus DictFromKeysWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        if (input->Child(1)->ChildrenSize() == 0) {
            input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        }
        const std::vector<std::string_view> k(1U, ctx.GetIndexAsString(0U));
        input->AddConstraint(ctx.MakeConstraint<TUniqueConstraintNode>(k));
        input->AddConstraint(ctx.MakeConstraint<TDistinctConstraintNode>(k));
        return TStatus::Ok;
    }

    template<bool IsList, bool IsFlat>
    TStatus PassOrEmptyWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (const auto part = input->Tail().GetConstraint<TPartOfSortedConstraintNode>())
            if (const auto filtered = part->CompleteOnly(ctx))
                input->AddConstraint(filtered);

        if (const auto part = input->Tail().GetConstraint<TPartOfChoppedConstraintNode>())
            if (const auto filtered = part->CompleteOnly(ctx))
                input->AddConstraint(filtered);

        if (const auto part = input->Tail().GetConstraint<TPartOfDistinctConstraintNode>())
            if (const auto filtered = part->CompleteOnly(ctx))
                input->AddConstraint(filtered);

        if (const auto part = input->Tail().GetConstraint<TPartOfUniqueConstraintNode>())
            if constexpr (IsList) {
                if (const auto filtered = part->CompleteOnly(ctx))
                    input->AddConstraint(filtered);
            } else
                input->AddConstraint(part);

        if constexpr (IsFlat) {
            if (const auto empty = input->Tail().GetConstraint<TEmptyConstraintNode>())
                input->AddConstraint(empty);
        }

        return FromSecond<TUniqueConstraintNode, TDistinctConstraintNode, TSortedConstraintNode, TChoppedConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>(input, output, ctx);
    }

    TStatus IfWrap(const TExprNode::TPtr& input, TExprNode::TPtr&, TExprContext& ctx) const {
        std::vector<const TConstraintSet*> constraints;
        constraints.reserve((input->ChildrenSize() << 1U) + 1U);
        constraints.emplace_back(&input->Tail().GetConstraintSet());

        for (auto i = 0U; i < input->ChildrenSize() - 1U; ++i) {
            constraints.emplace_back(&input->Child(++i)->GetConstraintSet());
        }

        if (constraints.empty())
            input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        else if (1U == constraints.size())
            input->SetConstraints(**constraints.cbegin());
        else
            TApplyCommonConstraint<TSortedConstraintNode
                , TPartOfSortedConstraintNode
                , TChoppedConstraintNode
                , TPartOfChoppedConstraintNode
                , TUniqueConstraintNode
                , TPartOfUniqueConstraintNode
                , TDistinctConstraintNode
                , TPartOfDistinctConstraintNode
                , TEmptyConstraintNode
                , TVarIndexConstraintNode
                , TMultiConstraintNode
                >::Do(input, constraints, ctx);
        return TStatus::Ok;
    }

    TStatus IfPresentWrap(const TExprNode::TPtr& input, TExprNode::TPtr&, TExprContext& ctx) const {
        auto optionals = input->ChildrenList();
        const auto lambdaIndex = optionals.size() - 2U;
        auto lambda = std::move(optionals[lambdaIndex]);
        optionals.resize(lambdaIndex);

        std::vector<const TConstraintNode::TListType> constraints;
        constraints.reserve(optionals.size());
        std::transform(optionals.cbegin(), optionals.cend(), std::back_inserter(constraints), [](const TExprNode::TPtr& node){ return node->GetAllConstraints(); });

        if (const auto status = UpdateLambdaConstraints(input->ChildRef(lambdaIndex), ctx, constraints); status != TStatus::Ok) {
            return status;
        }

        if (std::any_of(optionals.cbegin(), optionals.cend(), [] (const TExprNode::TPtr& node) { return bool(node->GetConstraint<TEmptyConstraintNode>()); })) {
            input->CopyConstraints(input->Tail());
            return TStatus::Ok;
        }

        const std::vector<const TConstraintSet*> both = { &lambda->GetConstraintSet(), &input->Tail().GetConstraintSet() };
        TApplyCommonConstraint<TPartOfSortedConstraintNode
            , TPartOfChoppedConstraintNode
            , TPartOfUniqueConstraintNode
            , TPartOfDistinctConstraintNode
            , TEmptyConstraintNode
            , TVarIndexConstraintNode
            , TMultiConstraintNode
            >::Do(input, both, ctx);
        return TStatus::Ok;
    }

    TStatus ReverseWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (const auto sorted = input->Head().GetConstraint<TSortedConstraintNode>()) {
            auto content = sorted->GetContent();
            std::for_each(content.begin(), content.end(), [](std::pair<TPartOfConstraintBase::TSetType, bool>& pair) { pair.second = !pair.second; });
            input->AddConstraint(ctx.MakeConstraint<TSortedConstraintNode>(std::move(content)));
        }
        return FromFirst<TEmptyConstraintNode, TChoppedConstraintNode, TPartOfChoppedConstraintNode, TUniqueConstraintNode, TPartOfUniqueConstraintNode, TDistinctConstraintNode, TPartOfDistinctConstraintNode, TVarIndexConstraintNode, TMultiConstraintNode>(input, output, ctx);
    }

    TStatus SwitchWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        TStatus status = TStatus::Ok;
        TDynBitMap outFromChildren; // children, from which take a multi constraint for output
        if (const auto multi = input->Head().GetConstraint<TMultiConstraintNode>()) {
            for (size_t i = 2; i < input->ChildrenSize(); ++i) {
                TMultiConstraintNode::TMapType items;
                ui32 lambdaInputIndex = 0;
                for (auto& child : input->Child(i)->Children()) {
                    const ui32 index = FromString<ui32>(child->Content());
                    if (auto c = multi->GetItem(index)) {
                        items[lambdaInputIndex] = *c;
                        outFromChildren.Set(i + 1);
                    }
                    ++lambdaInputIndex;
                }
                TConstraintNode::TListType argConstraints;
                if (!items.empty()) {
                    if (input->Child(i)->ChildrenSize() > 1) {
                        argConstraints.push_back(ctx.MakeConstraint<TMultiConstraintNode>(std::move(items)));
                        argConstraints.push_back(ctx.MakeConstraint<TVarIndexConstraintNode>(input->Child(i)->ChildrenSize()));
                    } else {
                        argConstraints = items.front().second.GetAllConstraints();
                    }
                }

                status = status.Combine(UpdateLambdaConstraints(input->ChildRef(++i), ctx, {argConstraints}));
            }
        } else {
            const bool inVar = GetSeqItemType(*input->Head().GetTypeAnn()).GetKind() == ETypeAnnotationKind::Variant;
            const TSmallVec<TConstraintNode::TListType> argConstraints(1U, inVar ? TConstraintNode::TListType() : input->Head().GetAllConstraints());
            for (size_t i = 3; i < input->ChildrenSize(); i += 2) {
                status = status.Combine(UpdateLambdaConstraints(input->ChildRef(i), ctx, argConstraints));
            }
            outFromChildren.Set(0, input->ChildrenSize());
        }

        if (status != TStatus::Ok) {
            return status;
        }

        const auto inputVarIndex = input->Head().GetConstraint<TVarIndexConstraintNode>();
        const bool emptyInput = input->Head().GetConstraint<TEmptyConstraintNode>();
        if (GetSeqItemType(*input->GetTypeAnn()).GetKind() == ETypeAnnotationKind::Variant) {
            ui32 outIndexOffset = 0;
            TMultiConstraintNode::TMapType multiItems;
            TVarIndexConstraintNode::TMapType remapItems;
            bool emptyOut = true;
            for (size_t i = 2; i < input->ChildrenSize(); i += 2) {
                const auto lambda = input->Child(i + 1);
                const auto& lambdaItemType = GetSeqItemType(*lambda->GetTypeAnn());

                if (inputVarIndex) {
                    if (auto varIndex = lambda->GetConstraint<TVarIndexConstraintNode>()) {
                        for (auto& item: varIndex->GetIndexMapping()) {
                            YQL_ENSURE(item.second < input->Child(i)->ChildrenSize());
                            const auto srcIndex = FromString<size_t>(input->Child(i)->Child(item.second)->Content());
                            remapItems.push_back(std::make_pair(outIndexOffset + item.first, srcIndex));
                        }
                    } else if (lambdaItemType.GetKind() == ETypeAnnotationKind::Variant && input->Child(i)->ChildrenSize() == 1) {
                        const auto srcIndex = FromString<size_t>(input->Child(i)->Head().Content());
                        for (size_t j = 0; j < lambdaItemType.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>()->GetSize(); ++j) {
                            remapItems.push_back(std::make_pair(outIndexOffset + j, srcIndex));
                        }
                    } else if (lambdaItemType.GetKind() != ETypeAnnotationKind::Variant && input->Child(i)->ChildrenSize() > 1) {
                        for (auto& child : input->Child(i)->Children()) {
                            const auto srcIndex = FromString<size_t>(child->Content());
                            remapItems.push_back(std::make_pair(outIndexOffset, srcIndex));
                        }
                    }
                }

                const bool lambdaEmpty = lambda->GetConstraint<TEmptyConstraintNode>();
                if (!lambdaEmpty) {
                    emptyOut = false;
                }
                if (lambdaItemType.GetKind() == ETypeAnnotationKind::Variant) {
                    if (!emptyInput && outFromChildren.Test(i + 1)) {
                        if (auto multi = lambda->GetConstraint<TMultiConstraintNode>()) {
                            for (auto& item: multi->GetItems()) {
                                multiItems.insert_unique(std::make_pair(outIndexOffset + item.first, item.second));
                            }
                        }
                    }
                    outIndexOffset += lambdaItemType.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>()->GetSize();
                } else {
                    if (!emptyInput && outFromChildren.Test(i + 1) && !lambdaEmpty) {
                        multiItems[outIndexOffset] = lambda->GetConstraintSet();
                    }
                    ++outIndexOffset;
                }
            }

            if (inputVarIndex && !remapItems.empty()) {
                TVarIndexConstraintNode::TMapType result;
                for (auto& item: remapItems) {
                    auto range = inputVarIndex->GetIndexMapping().equal_range(item.second);
                    for (auto it = range.first; it != range.second; ++it) {
                        result.push_back(std::make_pair(item.first, it->second));
                    }
                }
                if (!result.empty()) {
                    ::Sort(result);
                    input->AddConstraint(ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(result)));
                }
            }

            if (!multiItems.empty()) {
                input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(std::move(multiItems)));
            }
            if (emptyOut) {
                input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
            }
        } else {
            YQL_ENSURE(input->ChildrenSize() == 4);
            input->CopyConstraints(*input->Child(3));
        }
        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    TStatus VisitWrap(const TExprNode::TPtr& input, TExprNode::TPtr&, TExprContext& ctx) const {
        TStatus status = TStatus::Ok;
        TDynBitMap outFromChildren; // children, from which take a multi constraint for output
        TDynBitMap usedAlts;
        const auto inMulti = input->Head().GetConstraint<TMultiConstraintNode>();
        for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
            if (const auto child = input->Child(i); child->IsAtom()) {
                TSmallVec<TConstraintNode::TListType> argConstraints(1U);
                if (inMulti) {
                    const auto index = FromString<ui32>(child->Content());
                    usedAlts.Set(index);
                    if (const auto c = inMulti->GetItem(index)) {
                        argConstraints.front() = c->GetAllConstraints();
                        outFromChildren.Set(i + 1U);
                    }
                }
                status = status.Combine(UpdateLambdaConstraints(input->ChildRef(++i), ctx, argConstraints));
            } else if (inMulti) {                // Check that we can fall to default branch
                for (auto& item: inMulti->GetItems()) {
                    if (!usedAlts.Test(item.first)) {
                        outFromChildren.Set(i);
                        break;
                    }
                }
            }
        }

        if (status != TStatus::Ok) {
            return status;
        }

        if (!inMulti) {
            outFromChildren.Set(0, input->ChildrenSize());
        }

        auto outType = input->GetTypeAnn();
        if (auto t = GetSeqItemType(outType)) {
            outType = t;
        }
        if (outType->GetKind() == ETypeAnnotationKind::Variant && outType->Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            TVector<const TConstraintSet*> outConstraints;
            TVarIndexConstraintNode::TMapType remapItems;
            for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
                if (input->Child(i)->IsAtom()) {
                    ui32 index = FromString<ui32>(input->Child(i)->Content());
                    ++i;
                    if (outFromChildren.Test(i)) {
                        outConstraints.push_back(&input->Child(i)->GetConstraintSet());
                        if (const auto outMulti = input->Child(i)->GetConstraint<TMultiConstraintNode>()) {
                            for (auto& item: outMulti->GetItems()) {
                                remapItems.push_back(std::make_pair(item.first, index));
                            }
                        }
                    }
                } else {
                    if (outFromChildren.Test(i)) {
                        outConstraints.push_back(&input->Child(i)->GetConstraintSet());
                        const auto outMulti = input->Child(i)->GetConstraint<TMultiConstraintNode>();
                        if (outMulti && inMulti) {
                            for (auto& outItem: outMulti->GetItems()) {
                                for (auto& inItem: inMulti->GetItems()) {
                                    if (!usedAlts.Test(inItem.first)) {
                                        remapItems.push_back(std::make_pair(outItem.first, inItem.first));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (auto multi = TMultiConstraintNode::MakeCommon(outConstraints, ctx)) {
                input->AddConstraint(multi);
            }

            if (auto empty = TEmptyConstraintNode::MakeCommon(outConstraints, ctx)) {
                input->AddConstraint(empty);
            }

            if (auto varIndex = input->Head().GetConstraint<TVarIndexConstraintNode>()) {
                TVarIndexConstraintNode::TMapType varIndexItems;
                for (auto& item: remapItems) {
                    const auto range = varIndex->GetIndexMapping().equal_range(item.second);
                    for (auto i = range.first; i != range.second; ++i) {
                        varIndexItems.push_back(std::make_pair(item.first, i->second));
                    }
                }
                if (!varIndexItems.empty()) {
                    ::Sort(varIndexItems);
                    input->AddConstraint(ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(varIndexItems)));
                }
            }
        }
        else {
            std::vector<const TConstraintSet*> nonEmpty;
            for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
                if (input->Child(i)->IsAtom()) {
                    ++i;
                }
                if (outFromChildren.Test(i)) {
                    nonEmpty.push_back(&input->Child(i)->GetConstraintSet());
                }
            }
            EraseIf(nonEmpty, [] (const TConstraintSet* c) { return !!c->GetConstraint<TEmptyConstraintNode>(); });

            if (nonEmpty.empty()) {
                input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
            } else if (nonEmpty.size() == 1) {
                input->SetConstraints(*nonEmpty.front());
            }

            if (auto varIndex = input->Head().GetConstraint<TVarIndexConstraintNode>()) {
                TVarIndexConstraintNode::TMapType varIndexItems;
                for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
                    if (input->Child(i)->IsAtom()) {
                        const auto index = FromString<ui32>(input->Child(i++)->Content());
                        if (outFromChildren.Test(i) && IsDepended(input->Child(i)->Tail(), input->Child(i)->Head().Head())) { // Somehow depends on arg
                            const auto range = varIndex->GetIndexMapping().equal_range(index);
                            for (auto i = range.first; i != range.second; ++i) {
                                varIndexItems.push_back(std::make_pair(0, i->second));
                            }
                        }
                    }
                    else if (outFromChildren.Test(i)) {
                        if (inMulti) {
                            for (auto& inItem: inMulti->GetItems()) {
                                if (!usedAlts.Test(inItem.first)) {
                                    auto range = varIndex->GetIndexMapping().equal_range(inItem.first);
                                    for (auto i = range.first; i != range.second; ++i) {
                                        varIndexItems.push_back(std::make_pair(0, i->second));
                                    }
                                }
                            }
                        }
                    }
                }

                if (!varIndexItems.empty()) {
                    ::SortUnique(varIndexItems);
                    input->AddConstraint(ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(varIndexItems)));
                }
            }

        }
        return TStatus::Ok;
    }

    TStatus VariantItemWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        auto inputType = input->Head().GetTypeAnn();
        if (inputType->GetKind() == ETypeAnnotationKind::Optional) {
            inputType = inputType->Cast<TOptionalExprType>()->GetItemType();
        }

        const auto underlyingType = inputType->Cast<TVariantExprType>()->GetUnderlyingType();
        if (underlyingType->GetKind() == ETypeAnnotationKind::Tuple) {
            if (auto multi = input->Head().GetConstraint<TMultiConstraintNode>()) {
                std::vector<TMultiConstraintNode::TMapType::value_type> nonEmpty;
                std::copy_if(multi->GetItems().begin(), multi->GetItems().end(), std::back_inserter(nonEmpty),
                    [] (const TMultiConstraintNode::TMapType::value_type& v) {
                        return !v.second.GetConstraint<TEmptyConstraintNode>();
                    }
                );

                if (nonEmpty.empty()) {
                    input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
                } else if (nonEmpty.size() == 1) {
                    input->SetConstraints(nonEmpty.front().second);
                }
            }
            if (auto varIndex = input->Head().GetConstraint<TVarIndexConstraintNode>()) {
                TVarIndexConstraintNode::TMapType varIndexItems;
                for (auto& item: varIndex->GetIndexMapping()) {
                    varIndexItems.push_back(std::make_pair(0, item.second));
                }
                if (!varIndexItems.empty()) {
                    ::SortUnique(varIndexItems);
                    input->AddConstraint(ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(varIndexItems)));
                }
            }
        }
        return TStatus::Ok;
    }

    TStatus VariantWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        if (input->GetTypeAnn()->Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            const auto index = FromString<ui32>(input->Child(1)->Content());
            TConstraintSet target;
            CopyExcept(target, input->Head().GetConstraintSet(), TVarIndexConstraintNode::Name());
            input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(index, target));
            if (auto varIndex = input->Head().GetConstraint<TVarIndexConstraintNode>()) {
                TVarIndexConstraintNode::TMapType filteredItems;
                for (auto& item: varIndex->GetIndexMapping()) {
                    filteredItems.push_back(std::make_pair(index, item.second));
                }
                input->AddConstraint(ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(filteredItems)));
            }
        }
        return TStatus::Ok;
    }

    TStatus GuessWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        auto inputType = input->Head().GetTypeAnn();
        if (inputType->GetKind() == ETypeAnnotationKind::Optional) {
            inputType = inputType->Cast<TOptionalExprType>()->GetItemType();
        }

        const auto underlyingType = inputType->Cast<TVariantExprType>()->GetUnderlyingType();
        if (underlyingType->GetKind() == ETypeAnnotationKind::Tuple) {
            const auto guessIndex = FromString<ui32>(input->Child(1)->Content());
            if (auto multi = input->Head().GetConstraint<TMultiConstraintNode>()) {
                if (auto c = multi->GetItem(guessIndex)) {
                    input->SetConstraints(*c);
                } else {
                    input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
                }
            }
            if (auto varIndex = input->Head().GetConstraint<TVarIndexConstraintNode>()) {
                TVarIndexConstraintNode::TMapType filteredItems;
                for (auto& item: varIndex->GetIndexMapping()) {
                    if (item.first == guessIndex) {
                        filteredItems.push_back(std::make_pair(0, item.second));
                    }
                }
                if (!filteredItems.empty()) {
                    input->AddConstraint(ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(filteredItems)));
                }
            }
        }
        return TStatus::Ok;
    }

    TStatus MuxWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        const auto listItemType = GetSeqItemType(input->GetTypeAnn());
        if (!listItemType) {
            return TStatus::Ok;
        }
        if (listItemType->Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
            if (input->Head().IsList()) {
                TMultiConstraintNode::TMapType items;
                ui32 index = 0;
                for (auto& child: input->Head().Children()) {
                    items.push_back(std::make_pair(index, child->GetConstraintSet()));
                    ++index;
                }
                if (!items.empty()) {
                    input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(std::move(items)));
                }
            }
        }
        return TStatus::Ok;
    }

    TStatus NthWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        const auto& memberName = input->Tail().Content();
        const auto& structNode = input->Head();
        if (const auto emptyConstraint = structNode.GetConstraint<TEmptyConstraintNode>()) {
            input->AddConstraint(emptyConstraint);
        } else {
            if (const auto part = structNode.GetConstraint<TPartOfSortedConstraintNode>()) {
                if (const auto extracted = part->ExtractField(ctx, memberName)) {
                    input->AddConstraint(extracted);
                }
            }
            if (const auto part = structNode.GetConstraint<TPartOfChoppedConstraintNode>()) {
                if (const auto extracted = part->ExtractField(ctx, memberName)) {
                    input->AddConstraint(extracted);
                }
            }
            if (const auto part = structNode.GetConstraint<TPartOfUniqueConstraintNode>()) {
                if (const auto extracted = part->ExtractField(ctx, memberName)) {
                    input->AddConstraint(extracted);
                }
            }
            if (const auto part = structNode.GetConstraint<TPartOfDistinctConstraintNode>()) {
                if (const auto extracted = part->ExtractField(ctx, memberName)) {
                    input->AddConstraint(extracted);
                }
            }
        }

        if (input->Head().IsList()) {
            input->CopyConstraints(*input->Head().Child(FromString<ui32>(input->Child(1)->Content())));
        }
        else if (input->Head().IsCallable("Demux")) {
            if (auto multi = input->Head().Head().GetConstraint<TMultiConstraintNode>()) {
                if (auto c = multi->GetItem(FromString<ui32>(input->Child(1)->Content()))) {
                    input->SetConstraints(*c);
                }
            }
        }
        return TStatus::Ok;
    }

    TStatus EquiJoinWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        const auto numLists = input->ChildrenSize() - 2U;
        std::vector<size_t> emptyInputs;
        TJoinLabels labels;
        for (auto i = 0U; i < numLists; ++i) {
            const auto& list = input->Child(i)->Head();
            if (list.GetConstraint<TEmptyConstraintNode>()) {
                emptyInputs.push_back(i);
            }
            if (const auto err = labels.Add(ctx, input->Child(i)->Tail(),
                GetSeqItemType(*list.GetTypeAnn()).Cast<TStructExprType>(),
                GetDetailed(list.GetConstraint<TUniqueConstraintNode>(), *list.GetTypeAnn(), ctx),
                GetDetailed(list.GetConstraint<TDistinctConstraintNode>(), *list.GetTypeAnn(), ctx))) {
                ctx.AddError(*err);
                return TStatus::Error;
            }
        }

        const auto joinTree = input->Child(numLists);
        for (auto i: emptyInputs) {
            if (IsRequiredSide(joinTree, labels, i).first) {
                input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
                break;
            }
        }

        TJoinOptions options;
        if (const auto status = ValidateEquiJoinOptions(input->Pos(), input->Tail(), options, ctx); status != IGraphTransformer::TStatus::Ok || options.Flatten) {
            return status;
        }

        const TUniqueConstraintNode* unique = nullptr;
        const TDistinctConstraintNode* distinct = nullptr;
        if (const auto status = EquiJoinConstraints(input->Pos(), unique, distinct, labels, *joinTree, ctx); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (const auto renames = LoadJoinRenameMap(input->Tail()); !renames.empty() && (unique || distinct)) {
            const auto rename = [&renames](const TPartOfConstraintBase::TPathType& path) -> std::vector<TPartOfConstraintBase::TPathType> {
                if (path.empty())
                    return {};

                const auto it = renames.find(path.front());
                if (renames.cend() == it)
                    return {path};
                if (it->second.empty())
                    return {};

                std::vector<TPartOfConstraintBase::TPathType> res(it->second.size());
                std::transform(it->second.cbegin(), it->second.cend(), res.begin(), [&path](const std::string_view& newName) {
                    auto newPath = path;
                    newPath.front() = newName;
                    return newPath;
                });
                return res;
            };

            if (unique)
                unique = unique->RenameFields(ctx, rename);
            if (distinct)
                distinct = distinct->RenameFields(ctx, rename);
        }

        if (unique)
            input->AddConstraint(unique->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
        if (distinct)
            input->AddConstraint(distinct->GetSimplifiedForType(*input->GetTypeAnn(), ctx));

        return TStatus::Ok;
    }

    static std::vector<std::string_view> GetKeys(const TExprNode& keys) {
        std::vector<std::string_view> result;
        result.reserve(keys.ChildrenSize());
        keys.ForEachChild([&result](const TExprNode& key) { result.emplace_back(key.Content()); });
        return result;
    }

    template<bool ForDict = false>
    static TPartOfConstraintBase::TPathReduce GetRenames(const TExprNode& renames) {
        std::unordered_map<std::string_view, std::string_view> map(renames.ChildrenSize() >> 1U);
        for (auto i = 0U; i < renames.ChildrenSize(); ++++i)
            map.emplace(renames.Child(i)->Content(), renames.Child(i + 1U)->Content());
        return [map](const TPartOfConstraintBase::TPathType& path) -> std::vector<TPartOfConstraintBase::TPathType> {
            if constexpr (ForDict) {
                if (path.size() > 1U && path.front() == "1"sv) {
                    auto out = path;
                    out.pop_front();
                    if (const auto it = map.find(out.front()); map.cend() != it) {
                        out.front() = it->second;
                        return {std::move(out)};
                    }
                }
            } else {
                if (!path.empty()) {
                    if (const auto it = map.find(path.front()); map.cend() != it) {
                        auto out = path;
                        out.front() = it->second;
                        return {std::move(out)};
                    }
                }
            }

            return {};
        };
    }

    TStatus MapJoinCoreWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        const TCoMapJoinCore core(input);
        const auto& joinType = core.JoinKind().Ref();
        if (const auto empty = core.LeftInput().Ref().GetConstraint<TEmptyConstraintNode>()) {
            input->AddConstraint(empty);
        } else if (const auto empty = core.RightDict().Ref().GetConstraint<TEmptyConstraintNode>()) {
            if (joinType.IsAtom({"Inner", "LeftSemi"})) {
                input->AddConstraint(empty);
            }
        }

        if (joinType.IsAtom({"LeftSemi", "LeftOnly"})) {
            const auto rename = GetRenames(core.LeftRenames().Ref());
            if (const auto unique = core.LeftInput().Ref().GetConstraint<TUniqueConstraintNode>())
                if (const auto renamed = unique->RenameFields(ctx, rename))
                    input->AddConstraint(renamed);
            if (const auto distinct = core.LeftInput().Ref().GetConstraint<TDistinctConstraintNode>())
                if (const auto renamed = distinct->RenameFields(ctx, rename))
                    input->AddConstraint(renamed);
        } else {
            if (const auto unique = core.LeftInput().Ref().GetConstraint<TUniqueConstraintNode>()) {
                if (unique->ContainsCompleteSet(GetKeys(core.LeftKeysColumns().Ref())) && core.RightDict().Ref().GetTypeAnn()->Cast<TDictExprType>()->GetPayloadType()->GetKind() != ETypeAnnotationKind::List) {
                    const auto rename = GetRenames(core.LeftRenames().Ref());
                    const auto rightRename = GetRenames<true>(core.RightRenames().Ref());
                    auto commonUnique = unique->RenameFields(ctx, rename);
                    if (const auto rUnique = core.RightDict().Ref().GetConstraint<TUniqueConstraintNode>()) {
                        commonUnique = TUniqueConstraintNode::Merge(commonUnique, rUnique->RenameFields(ctx, rightRename), ctx);
                    }

                    const auto distinct = core.LeftInput().Ref().GetConstraint<TDistinctConstraintNode>();
                    auto commonDistinct = distinct ? distinct->RenameFields(ctx, rename) : nullptr;
                    if (joinType.IsAtom("Inner")) {
                        if (const auto rDistinct = core.RightDict().Ref().GetConstraint<TDistinctConstraintNode>()) {
                            commonDistinct = TDistinctConstraintNode::Merge(commonDistinct, rDistinct->RenameFields(ctx, rightRename), ctx);
                        }
                    }

                    if (commonUnique)
                        input->AddConstraint(commonUnique);
                    if (commonDistinct)
                        input->AddConstraint(commonDistinct);
                }
            }
        }

        if (const auto sorted = core.LeftInput().Ref().GetConstraint<TSortedConstraintNode>())
            if (const auto renamed = sorted->RenameFields(ctx, GetRenames(core.LeftRenames().Ref())))
                input->AddConstraint(renamed);

        if (const auto chopped = core.LeftInput().Ref().GetConstraint<TChoppedConstraintNode>())
            if (const auto renamed = chopped->RenameFields(ctx, GetRenames(core.LeftRenames().Ref())))
                input->AddConstraint(renamed);

        return TStatus::Ok;
    }

    TExprNode::TPtr GraceJoinRightInput(const TCoGraceJoinCore& core) const {
        return core.RightInput().Ptr();
    }

    TExprNode::TPtr GraceJoinRightInput(const TCoGraceSelfJoinCore& core) const {
        return core.Input().Ptr();
    }

    TExprNode::TPtr GraceJoinLeftInput(const TCoGraceJoinCore& core) const {
        return core.LeftInput().Ptr();
    }

    TExprNode::TPtr GraceJoinLeftInput(const TCoGraceSelfJoinCore& core) const {
        return core.Input().Ptr();
    }

    template<typename GraceJoinCoreType>
    TStatus GraceJoinCoreWrapImpl(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        Y_UNUSED(output);
        const GraceJoinCoreType core(input);
        const auto& joinType = core.JoinKind().Ref();
        TExprNode::TPtr leftInput = GraceJoinLeftInput(core);
        TExprNode::TPtr rightInput = GraceJoinRightInput(core);

        if (const auto lEmpty = leftInput->GetConstraint<TEmptyConstraintNode>(), rEmpty = rightInput->GetConstraint<TEmptyConstraintNode>(); lEmpty && rEmpty) {
            input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        } else if (lEmpty && joinType.Content().starts_with("Left")) {
            input->AddConstraint(lEmpty);
        } else if (rEmpty && joinType.Content().starts_with("Right")) {
            input->AddConstraint(rEmpty);
        } else if ((lEmpty || rEmpty) && (joinType.IsAtom("Inner") || joinType.Content().ends_with("Semi"))) {
            input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        }

        bool leftAny = false, rigthAny = false;
        core.Flags().Ref().ForEachChild([&](const TExprNode& flag) {
            if (flag.IsAtom("LeftAny"))
               leftAny = true;
            else if (flag.IsAtom("RightAny"))
                rigthAny = true;
        });

        const TUniqueConstraintNode* lUnique = leftInput->GetConstraint<TUniqueConstraintNode>();
        const TUniqueConstraintNode* rUnique = rightInput->GetConstraint<TUniqueConstraintNode>();

        const bool lOneRow = lUnique && (leftAny || lUnique->ContainsCompleteSet(GetKeys(core.LeftKeysColumns().Ref())));
        const bool rOneRow = rUnique && (rigthAny || rUnique->ContainsCompleteSet(GetKeys(core.RightKeysColumns().Ref())));

        const bool singleSide = joinType.Content().ends_with("Semi") || joinType.Content().ends_with("Only");

        if (singleSide || lOneRow || rOneRow) {
            const TUniqueConstraintNode* unique = nullptr;
            const TDistinctConstraintNode* distinct = nullptr;

            const bool leftSide = joinType.Content().starts_with("Left");
            const bool rightSide = joinType.Content().starts_with("Right");
            const auto leftRename = GetRenames(core.LeftRenames().Ref());
            const auto rightRename = GetRenames(core.RightRenames().Ref());

            if (singleSide) {
                if (leftSide && lUnique)
                    unique = lUnique->RenameFields(ctx, leftRename);
                else if (rightSide && rUnique)
                    unique = rUnique->RenameFields(ctx, rightRename);
            } else {
                const bool exclusion = joinType.IsAtom("Exclusion");
                const bool useLeft = lUnique && (rOneRow || exclusion);
                const bool useRight = rUnique && (lOneRow || exclusion);

                if (useLeft && !useRight)
                    unique = lUnique->RenameFields(ctx, leftRename);
                else if (useRight && !useLeft)
                    unique = rUnique->RenameFields(ctx, rightRename);
                else if (useLeft && useRight)
                    unique = TUniqueConstraintNode::Merge(lUnique->RenameFields(ctx, leftRename), rUnique->RenameFields(ctx, rightRename), ctx);
            }

            const auto lDistinct = leftInput->GetConstraint<TDistinctConstraintNode>();
            const auto rDistinct = rightInput->GetConstraint<TDistinctConstraintNode>();

            if (singleSide) {
                if (leftSide && lDistinct)
                    distinct = lDistinct->RenameFields(ctx, leftRename);
                else if (rightSide && rDistinct)
                    distinct = rDistinct->RenameFields(ctx, rightRename);
            } else {
                const bool inner = joinType.IsAtom("Inner");
                const bool useLeft = lDistinct && rOneRow && (inner || leftSide);
                const bool useRight = rDistinct && lOneRow && (inner || rightSide);

                if (useLeft && !useRight)
                    distinct = lDistinct->RenameFields(ctx, leftRename);
                else if (useRight && !useLeft)
                    distinct = rDistinct->RenameFields(ctx, rightRename);
                else if (useLeft && useRight)
                    distinct = TDistinctConstraintNode::Merge(lDistinct->RenameFields(ctx, leftRename), rDistinct->RenameFields(ctx, rightRename), ctx);
            }

            if (unique)
                input->AddConstraint(unique);
            if (distinct)
                input->AddConstraint(distinct);
        }

        return TStatus::Ok;
    }

    TStatus GraceJoinCoreWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        return GraceJoinCoreWrapImpl<TCoGraceJoinCore>(input, output, ctx);
    }

    TStatus GraceSelfJoinCoreWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        return GraceJoinCoreWrapImpl<TCoGraceSelfJoinCore>(input, output, ctx);
    }

    template<bool Distinct>
    static const TUniqueConstraintNodeBase<Distinct>* GetForPayload(const TExprNode& input, TExprContext& ctx) {
        if (const auto constraint = input.GetConstraint<TUniqueConstraintNodeBase<Distinct>>())  {
            return constraint->RenameFields(ctx, [&ctx](const TPartOfConstraintBase::TPathType& path) -> std::vector<TPartOfConstraintBase::TPathType> {
                if (path.empty() || path.front() != ctx.GetIndexAsString(1U))
                    return {};
                auto copy = path;
                copy.pop_front();
                return {copy};
            });
        }
        return nullptr;
    }

    TStatus JoinDictWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        const TCoJoinDict join(input);
        const auto& joinType = join.JoinKind().Ref();
        if (const auto lEmpty = join.LeftInput().Ref().GetConstraint<TEmptyConstraintNode>(), rEmpty = join.RightInput().Ref().GetConstraint<TEmptyConstraintNode>(); lEmpty && rEmpty) {
            input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        } else if (lEmpty && joinType.Content().starts_with("Left")) {
            input->AddConstraint(lEmpty);
        } else if (rEmpty && joinType.Content().starts_with("Right")) {
            input->AddConstraint(rEmpty);
        } else if ((lEmpty || rEmpty) && (joinType.IsAtom("Inner") || joinType.Content().ends_with("Semi"))) {
            input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        }

        bool lOneRow = false, rOneRow = false;
        if (const auto& flags = join.Flags()) {
            flags.Cast().Ref().ForEachChild([&](const TExprNode& flag) {
                lOneRow = lOneRow || flag.IsAtom("LeftUnique");
                rOneRow = rOneRow || flag.IsAtom("RightUnique");
            });
        }

        const auto lUnique = GetForPayload<false>(join.LeftInput().Ref(), ctx);
        const auto rUnique = GetForPayload<false>(join.RightInput().Ref(), ctx);

        const auto lDistinct = GetForPayload<true>(join.LeftInput().Ref(), ctx);
        const auto rDistinct = GetForPayload<true>(join.RightInput().Ref(), ctx);

        const bool leftSide = joinType.Content().starts_with("Left");
        const bool rightSide = joinType.Content().starts_with("Right");

        if (joinType.Content().ends_with("Semi") || joinType.Content().ends_with("Only")) {
            if (leftSide) {
                if (lUnique)
                    input->AddConstraint(lUnique);
                if (lDistinct)
                    input->AddConstraint(lDistinct);
            } else if (rightSide) {
                if (rUnique)
                    input->AddConstraint(rUnique);
                if (rDistinct)
                    input->AddConstraint(rDistinct);
            }
        } else if (lOneRow || rOneRow) {
            const auto rename = [](const std::string_view& prefix, TPartOfConstraintBase::TPathType path) {
                path.emplace_front(prefix);
                return std::vector<TPartOfConstraintBase::TPathType>(1U, std::move(path));
            };
            const auto leftRename = std::bind(rename, ctx.GetIndexAsString(0U), std::placeholders::_1);
            const auto rightRename = std::bind(rename, ctx.GetIndexAsString(1U), std::placeholders::_1);

            if (lUnique || rUnique) {
                const TUniqueConstraintNode* unique = nullptr;

                const bool exclusion = joinType.IsAtom("Exclusion");
                const bool useLeft = lUnique && (rOneRow || exclusion);
                const bool useRight = rUnique && (lOneRow || exclusion);

                if (useLeft && !useRight)
                    unique = lUnique->RenameFields(ctx, leftRename);
                else if (useRight && !useLeft)
                    unique = rUnique->RenameFields(ctx, rightRename);
                else if (useLeft && useRight)
                    unique = TUniqueConstraintNode::Merge(lUnique->RenameFields(ctx, leftRename), rUnique->RenameFields(ctx, rightRename), ctx);

                if (unique)
                    input->AddConstraint(unique);
            }

            if (lDistinct || rDistinct) {
                const TDistinctConstraintNode* distinct = nullptr;

                const bool inner = joinType.IsAtom("Inner");
                const bool useLeft = lDistinct && rOneRow && (inner || leftSide);
                const bool useRight = rDistinct && lOneRow && (inner || rightSide);

                if (useLeft && !useRight)
                    distinct = lDistinct->RenameFields(ctx, leftRename);
                else if (useRight && !useLeft)
                    distinct = rDistinct->RenameFields(ctx, rightRename);
                else if (useLeft && useRight)
                    distinct = TDistinctConstraintNode::Merge(lDistinct->RenameFields(ctx, leftRename), rDistinct->RenameFields(ctx, rightRename), ctx);

                if (distinct)
                    input->AddConstraint(distinct);
            }
        }

        return TStatus::Ok;
    }

    TStatus IsKeySwitchWrap(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) const {
        const TCoIsKeySwitch keySwitch(input);
        TSmallVec<TConstraintNode::TListType> itemConstraints, stateConstraints;
        itemConstraints.emplace_back(keySwitch.Item().Ref().GetAllConstraints());
        stateConstraints.emplace_back(keySwitch.State().Ref().GetAllConstraints());
        return UpdateLambdaConstraints(input->ChildRef(TCoIsKeySwitch::idx_ItemKeyExtractor), ctx, itemConstraints)
            .Combine(UpdateLambdaConstraints(input->ChildRef(TCoIsKeySwitch::idx_StateKeyExtractor), ctx, stateConstraints));
    }

    template<bool Wide>
    static TPartOfConstraintBase::TSetType GetSimpleKeys(const TExprNode& node, const TExprNode::TChildrenType& args, TExprContext& ctx) {
        TPartOfConstraintBase::TSetType keys;
        if (node.IsCallable("AggrNotEquals")) {
            const TExprNode& body = node.Head().IsCallable("StablePickle") ? node.Head() : node;
            if (body.Head().IsList() && body.Tail().IsList() && body.Head().ChildrenSize() == body.Tail().ChildrenSize()) {
                keys.reserve(body.Tail().ChildrenSize());
                for (auto i = 0U; i < body.Head().ChildrenSize(); ++i){
                    if (auto l = GetPathToKey(*body.Head().Child(i), args), r = GetPathToKey(*body.Tail().Child(i), args); l && r && *l == *r) {
                        if constexpr (Wide) {
                            auto path = r->first;
                            path.emplace_front(ctx.GetIndexAsString(r->second));
                        } else {
                            YQL_ENSURE(l->second == 0U, "Unexpected arg index: " << l->second);
                            keys.insert_unique(l->first);
                        }
                    }
                }
            } else if (auto l = GetPathToKey(body.Head(), args), r = GetPathToKey(body.Tail(), args); l && r && *l == *r) {
                if constexpr (Wide) {
                    auto path = l->first;
                    path.emplace_front(ctx.GetIndexAsString(l->second));
                } else {
                    YQL_ENSURE(r->second == 0U, "Unexpected arg index: " << r->second);
                    keys.insert_unique(r->first);
                }
            }
        } else if (node.IsCallable("Or")) {
            keys.reserve(node.ChildrenSize());
            for (auto i = 0U; i < node.ChildrenSize(); ++i) {
                const auto& part = GetSimpleKeys<Wide>(*node.Child(i), args, ctx);
                keys.insert_unique(part.cbegin(), part.cend());
            }
        }

        return keys;
    }

    template<bool Wide>
    static TPartOfConstraintBase::TSetType GetSimpleKeys(const TExprNode& selector, TExprContext& ctx) {
        YQL_ENSURE(selector.IsLambda() && 2U == selector.ChildrenSize());
        const auto& body = selector.Tail();
        if constexpr (!Wide) {
            if (TCoIsKeySwitch::Match(&body)) {
                const TCoIsKeySwitch keySwitch(&body);
                const auto& i = GetPathsToKeys(*ctx.ReplaceNode(keySwitch.ItemKeyExtractor().Body().Ptr(), keySwitch.ItemKeyExtractor().Args().Arg(0).Ref(), keySwitch.Item().Ptr()), keySwitch.Item().Ref());
                const auto& s = GetPathsToKeys(*ctx.ReplaceNode(keySwitch.StateKeyExtractor().Body().Ptr(), keySwitch.StateKeyExtractor().Args().Arg(0).Ref(), keySwitch.State().Ptr()), keySwitch.Item().Ref());
                return i == s  ? i : TPartOfConstraintBase::TSetType();
            }
        }

        return GetSimpleKeys<Wide>(selector.Tail(), selector.Head().Children(), ctx);
    }

    static TExprNode::TPtr FuseInitLambda(const TExprNode& inner, const TExprNode& outer, TExprContext& ctx) {
        YQL_ENSURE(outer.IsLambda() && inner.IsLambda());
        const auto& outerArgs = outer.Head();
        const auto& innerArgs = inner.Head();
        YQL_ENSURE(outerArgs.ChildrenSize() + 1U == innerArgs.ChildrenSize() + inner.ChildrenSize());

        TNodeOnNodeOwnedMap outerReplaces(outerArgs.ChildrenSize());
        auto i = 0U;
        for (auto& item : innerArgs.ChildrenList())
            YQL_ENSURE(outerReplaces.emplace(outerArgs.Child(i++), std::move(item)).second);
        for (auto& item : GetLambdaBody(inner))
            YQL_ENSURE(outerReplaces.emplace(outerArgs.Child(i++), std::move(item)).second);
        return ctx.NewLambda(outer.Pos(), inner.HeadPtr(), ctx.ReplaceNodes(GetLambdaBody(outer), outerReplaces));
    }

    TStatus CondenseWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        auto argsConstraints = GetConstraintsForInputArgument<true, false, false>(*input, ctx);

        const auto initState = input->Child(1);
        argsConstraints.emplace_back(initState->GetAllConstraints());

        if (const auto status = UpdateLambdaConstraints(input->ChildRef(2), ctx, argsConstraints)
            .Combine(UpdateLambdaConstraints(input->TailRef(), ctx, argsConstraints)); status != TStatus::Ok) {
            return status;
        }

        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    template<class TConstraint, bool Wide>
    static void GetCommonFromBothLambdas(const TExprNode::TPtr& input, const typename TConstraint::TMainConstraint* original, TExprContext& ctx) {
        if (original)
            if (const auto initPart = GetConstraintFromLambda<TConstraint, Wide>(*input->Child(1), ctx))
                if (const auto init = TConstraint::MakeComplete(ctx, initPart->GetColumnMapping(), original))
                    if (const auto updatePart = GetConstraintFromLambda<TConstraint, Wide>(input->Tail(), ctx))
                        if (const auto update = TConstraint::MakeComplete(ctx, updatePart->GetColumnMapping(), original))
                            if (const auto common = init->MakeCommon(update, ctx))
                                input->AddConstraint(common->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
    }

    template<bool Wide>
    TStatus Condense1Wrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        auto argsConstraints = GetConstraintsForInputArgument<true, Wide, false>(*input, ctx);
        const auto initLambda = input->Child(1);
        const auto switchLambda = input->Child(2);

        const TUniqueConstraintNode* unique = nullptr;
        const TDistinctConstraintNode* distinct = nullptr;
        const auto sorted = input->Head().GetConstraint<TSortedConstraintNode>();
        const auto chopped = input->Head().GetConstraint<TChoppedConstraintNode>();
        if (sorted || chopped) {
            if (const auto& keys = GetSimpleKeys<Wide>(*FuseInitLambda(*initLambda, *switchLambda, ctx), ctx); !keys.empty()) {
                if (sorted && (sorted->StartsWith(keys) || sorted->GetSimplifiedForType(*input->Head().GetTypeAnn(), ctx)->StartsWith(keys)) ||
                    chopped && (chopped->Equals(keys) || chopped->GetSimplifiedForType(*input->Head().GetTypeAnn(), ctx)->Equals(keys))) {
                    TPartOfConstraintBase::TSetOfSetsType sets;
                    sets.reserve(keys.size());
                    for (const auto& key : keys)
                        sets.insert_unique(TPartOfConstraintBase::TSetType{key});
                    unique = ctx.MakeConstraint<TUniqueConstraintNode>(TUniqueConstraintNode::TContentType{sets})->GetComplicatedForType(*input->Head().GetTypeAnn(), ctx);
                    distinct = ctx.MakeConstraint<TDistinctConstraintNode>(TDistinctConstraintNode::TContentType{sets})->GetComplicatedForType(*input->Head().GetTypeAnn(), ctx);
                    if constexpr (Wide) {
                        if (const auto& mapping = TPartOfUniqueConstraintNode::GetCommonMapping(unique); !mapping.empty()) {
                            for (ui32 i = 0U; i < argsConstraints.size(); ++i) {
                                if (auto extracted = TPartOfUniqueConstraintNode::ExtractField(mapping, ctx.GetIndexAsString(i)); !extracted.empty()) {
                                    argsConstraints[i].emplace_back(ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(extracted)));
                                }
                            }
                        }

                        if (const auto& mapping = TPartOfDistinctConstraintNode::GetCommonMapping(distinct); !mapping.empty()) {
                            for (ui32 i = 0U; i < argsConstraints.size(); ++i) {
                                if (auto extracted = TPartOfDistinctConstraintNode::ExtractField(mapping, ctx.GetIndexAsString(i)); !extracted.empty()) {
                                    argsConstraints[i].emplace_back(ctx.MakeConstraint<TPartOfDistinctConstraintNode>(std::move(extracted)));
                                }
                            }
                        }
                    } else {
                        argsConstraints.front().emplace_back(ctx.MakeConstraint<TPartOfUniqueConstraintNode>(TPartOfUniqueConstraintNode::GetCommonMapping(unique)));
                        argsConstraints.front().emplace_back(ctx.MakeConstraint<TPartOfDistinctConstraintNode>(TPartOfDistinctConstraintNode::GetCommonMapping(distinct)));
                    }
                }
            }
        }

        if (const auto status = UpdateLambdaConstraints(input->ChildRef(1), ctx, argsConstraints); status != TStatus::Ok) {
            return status;
        }

        argsConstraints.reserve(argsConstraints.size() + initLambda->ChildrenSize() - 1U);
        for (ui32 i = 1U; i < initLambda->ChildrenSize(); ++i) {
            argsConstraints.emplace_back(initLambda->Child(i)->GetAllConstraints());
        }

        if (const auto status = UpdateLambdaConstraints(input->ChildRef(2), ctx, argsConstraints)
            .Combine(UpdateLambdaConstraints(input->TailRef(), ctx, argsConstraints)); status != TStatus::Ok) {
            return status;
        }

        GetCommonFromBothLambdas<TPartOfUniqueConstraintNode, Wide>(input, unique, ctx);
        GetCommonFromBothLambdas<TPartOfDistinctConstraintNode, Wide>(input, distinct, ctx);
        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    template<class TConstraint, bool Wide>
    static void GetCommonFromBothLambdas(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (const auto original = input->Head().GetConstraint<typename TConstraint::TMainConstraint>())
            if (const auto initPart = GetConstraintFromLambda<TConstraint, Wide>(*input->Child(1), ctx))
                if (const auto init = TConstraint::MakeComplete(ctx, initPart->GetColumnMapping(), original))
                    if (const auto updatePart = GetConstraintFromLambda<TConstraint, Wide>(input->Tail(), ctx))
                        if (const auto update = TConstraint::MakeComplete(ctx, updatePart->GetColumnMapping(), original))
                            if (const auto common = init->MakeCommon(update, ctx))
                                input->AddConstraint(common->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
    }

    template<bool Wide>
    TStatus Chain1MapWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        auto argsConstraints = GetConstraintsForInputArgument<true, Wide>(*input, ctx);
        if (const auto status = UpdateLambdaConstraints(input->ChildRef(1), ctx, argsConstraints); status != TStatus::Ok) {
            return status;
        }

        const auto initLambda = input->Child(1);
        argsConstraints.reserve(argsConstraints.size() + initLambda->ChildrenSize() - 1U);
        for (ui32 i = 1U; i < initLambda->ChildrenSize(); ++i) {
            argsConstraints.emplace_back(initLambda->Child(i)->GetAllConstraints());
        }

        if (const auto status = UpdateLambdaConstraints(input->ChildRef(2), ctx, argsConstraints); status != TStatus::Ok) {
            return status;
        }

        GetCommonFromBothLambdas<TPartOfSortedConstraintNode, Wide>(input, ctx);
        GetCommonFromBothLambdas<TPartOfChoppedConstraintNode, Wide>(input, ctx);
        GetCommonFromBothLambdas<TPartOfUniqueConstraintNode, Wide>(input, ctx);
        GetCommonFromBothLambdas<TPartOfDistinctConstraintNode, Wide>(input, ctx);
        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    template<bool Distinct>
    static void GetUniquesForPayloads(const TExprNode::TPtr& input, TExprContext& ctx) {
        typename TUniqueConstraintNodeBase<Distinct>::TContentType content{TPartOfConstraintBase::TSetOfSetsType{TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{ctx.GetIndexAsString(0U)}}}};
        if (const auto lambda = GetConstraintFromLambda<TPartOfConstraintNode<TUniqueConstraintNodeBase<Distinct>>, false>(*input->Child(2), ctx)) {
            if (const auto original = GetDetailed(input->Head().GetConstraint<TUniqueConstraintNodeBase<Distinct>>(), *input->Head().GetTypeAnn(), ctx)) {
                if (const auto complete = TPartOfConstraintNode<TUniqueConstraintNodeBase<Distinct>>::MakeComplete(ctx, lambda->GetColumnMapping(), original, ctx.GetIndexAsString(1U))) {
                    content.insert_unique(complete->GetContent().cbegin(), complete->GetContent().cend());
                }
            }
        }
        input->AddConstraint(ctx.MakeConstraint<TUniqueConstraintNodeBase<Distinct>>(std::move(content)));
    }

    TStatus ToDictWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        const auto argsConstraints = GetConstraintsForInputArgument<false, false>(*input, ctx);
        if (const auto status = UpdateLambdaConstraints(input->ChildRef(1), ctx, argsConstraints); status != TStatus::Ok) {
            return status;
        }
        if (const auto status = UpdateLambdaConstraints(input->ChildRef(2), ctx, argsConstraints); status != TStatus::Ok) {
            return status;
        }

        GetUniquesForPayloads<true>(input, ctx);
        GetUniquesForPayloads<false>(input, ctx);
        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    TStatus DictItemsWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (const auto unique = input->Head().GetConstraint<TUniqueConstraintNode>())
            input->AddConstraint(unique->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
        if (const auto distinct = input->Head().GetConstraint<TDistinctConstraintNode>())
            input->AddConstraint(distinct->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
        return FromFirst<TPartOfUniqueConstraintNode, TPartOfDistinctConstraintNode, TEmptyConstraintNode>(input, output, ctx);
    }

    template<bool Keys>
    TStatus DictHalfWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        const auto& side = ctx.GetIndexAsString(Keys ? 0U : 1U);
        const auto reduce = [&side](const TPartOfConstraintBase::TPathType& path) -> std::vector<TPartOfConstraintBase::TPathType> {
            if (path.empty() || path.front() != side)
                return {};

            auto copy = path;
            copy.pop_front();
            return {copy};
        };
        ReduceFromHead<TUniqueConstraintNode, true>(input, reduce, ctx);
        ReduceFromHead<TDistinctConstraintNode, true>(input, reduce, ctx);
        ReduceFromHead<TPartOfUniqueConstraintNode>(input, reduce, ctx);
        ReduceFromHead<TPartOfDistinctConstraintNode>(input, reduce, ctx);
        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    template<bool Partitions>
    TStatus ShuffleByKeysWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        using TCoBase = std::conditional_t<Partitions, TCoPartitionByKeyBase, TCoShuffleByKeys>;
        if (const auto status = UpdateLambdaConstraints(*input->Child(TCoBase::idx_KeySelectorLambda)); status != TStatus::Ok) {
            return status;
        }

        TPartOfConstraintBase::TSetType keys;
        if constexpr (Partitions) {
            keys = GetPathsToKeys(input->Child(TCoBase::idx_KeySelectorLambda)->Tail(), input->Child(TCoBase::idx_KeySelectorLambda)->Head().Head());
            if (const auto sortKeySelector = input->Child(TCoBase::idx_SortKeySelectorLambda); sortKeySelector->IsLambda()) {
                if (const auto status = UpdateLambdaConstraints(*sortKeySelector); status != TStatus::Ok) {
                    return status;
                }

                auto sortKeys = GetPathsToKeys(sortKeySelector->Tail(), sortKeySelector->Head().Head());
                std::move(sortKeys.begin(), sortKeys.end(), std::back_inserter(keys));
                std::sort(keys.begin(), keys.end());
            }
        }

        const auto filter = [](const std::string_view& name) {
            return name == TEmptyConstraintNode::Name() || name == TUniqueConstraintNode::Name() || name == TDistinctConstraintNode::Name();
        };

        TConstraintNode::TListType argConstraints;
        const auto source = input->Child(TCoBase::idx_Input);
        std::copy_if(source->GetAllConstraints().cbegin(), source->GetAllConstraints().cend(), std::back_inserter(argConstraints), std::bind(filter, std::bind(&TConstraintNode::GetName, std::placeholders::_1)));

        if (const auto multi = source->template GetConstraint<TMultiConstraintNode>())
            if (const auto filtered = multi->FilterConstraints(ctx, filter))
                argConstraints.emplace_back(filtered);

        if constexpr (Partitions) {
            if (!keys.empty())
                argConstraints.emplace_back(ctx.MakeConstraint<TChoppedConstraintNode>(keys)->GetComplicatedForType(*input->Head().GetTypeAnn(), ctx));
        }

        if (const auto status = UpdateLambdaConstraints(input->ChildRef(TCoBase::idx_ListHandlerLambda), ctx, {argConstraints}); status != TStatus::Ok) {
            return status;
        }

        const auto handlerLambda = input->Child(TCoBase::idx_ListHandlerLambda);

        if (const auto unique = handlerLambda->template GetConstraint<TUniqueConstraintNode>())
            input->AddConstraint(unique);
        if (const auto distinct = handlerLambda->template GetConstraint<TDistinctConstraintNode>())
            input->AddConstraint(distinct);

        const bool multiInput = ETypeAnnotationKind::Variant == GetSeqItemType(*input->Head().GetTypeAnn()).GetKind();
        const auto lambdaVarIndex = handlerLambda->template GetConstraint<TVarIndexConstraintNode>();
        const auto multi = input->Head().template GetConstraint<TMultiConstraintNode>();
        const auto lambdaMulti = handlerLambda->template GetConstraint<TMultiConstraintNode>();

        if (const auto varIndex = input->Head().template GetConstraint<TVarIndexConstraintNode>()) {
            if (multiInput) {
                if (lambdaVarIndex) {
                    if (auto outVarIndex = GetVarIndexOverVarIndexConstraint(*varIndex, *lambdaVarIndex, ctx)) {
                        input->AddConstraint(outVarIndex);
                    }
                }
            } else {
                if (lambdaMulti) {
                    TVarIndexConstraintNode::TMapType remapItems;
                    for (auto& multiItem: lambdaMulti->GetItems()) {
                        for (auto& varItem: varIndex->GetIndexMapping()) {
                            remapItems.push_back(std::make_pair(multiItem.first, varItem.second));
                        }
                    }
                    if (!remapItems.empty()) {
                        ::SortUnique(remapItems);
                        input->AddConstraint(ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(remapItems)));
                    }
                } else {
                    input->AddConstraint(varIndex);
                }
            }
        }

        if (lambdaMulti && !input->Head().GetConstraint<TEmptyConstraintNode>()) {
            TMultiConstraintNode::TMapType remappedItems;
            for (const auto& item: lambdaMulti->GetItems()) {
                remappedItems.push_back(std::make_pair(item.first, TConstraintSet{}));
                if (!multiInput) { // remapping one to many
                    if (const auto empty = item.second.template GetConstraint<TEmptyConstraintNode>())
                        remappedItems.pop_back();
                    else {
                        if (const auto unique = item.second.template GetConstraint<TUniqueConstraintNode>())
                            remappedItems.back().second.AddConstraint(unique);
                        if (const auto distinct = item.second.template GetConstraint<TDistinctConstraintNode>())
                            remappedItems.back().second.AddConstraint(distinct);
                    }
                }
                else if (lambdaVarIndex && multi) {
                    const auto range = lambdaVarIndex->GetIndexMapping().equal_range(item.first);
                    switch (std::distance(range.first, range.second)) {
                    case 0: // new index
                        break;
                    case 1: // remapping 1 to 1
                        if (auto origConstr = multi->GetItem(range.first->second)) {
                            if (const auto empty = item.second.template GetConstraint<TEmptyConstraintNode>())
                                remappedItems.pop_back();
                            else {
                                if (const auto unique = item.second.template GetConstraint<TUniqueConstraintNode>())
                                    remappedItems.back().second.AddConstraint(unique);
                                if (const auto distinct = item.second.template GetConstraint<TDistinctConstraintNode>())
                                    remappedItems.back().second.AddConstraint(distinct);
                            }
                        } else {
                            remappedItems.pop_back();
                        }
                        break;
                    default: // remapping many to one
                        {
                            std::vector<const TConstraintSet*> nonEmpty;
                            for (auto i = range.first; i != range.second; ++i) {
                                if (auto origConstr = multi->GetItem(i->second)) {
                                    nonEmpty.push_back(origConstr);
                                }
                            }
                            EraseIf(nonEmpty, [] (const TConstraintSet* c) { return !!c->GetConstraint<TEmptyConstraintNode>(); });

                            if (nonEmpty.empty()) {
                                remappedItems.back().second.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
                            } else if (nonEmpty.size() == 1) {
                                remappedItems.back().second = std::move(*nonEmpty.front());
                            }
                        }
                    }
                } else {
                    remappedItems.back().second = item.second;
                }
            }
            if (remappedItems) {
                input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(std::move(remappedItems)));
            }
        }
        else if (multi && lambdaVarIndex) { // Many to one
            const auto range = lambdaVarIndex->GetIndexMapping().equal_range(0);
            std::vector<const TConstraintSet*> nonEmpty;
            for (auto i = range.first; i != range.second; ++i) {
                if (auto origConstr = multi->GetItem(i->second)) {
                    nonEmpty.push_back(origConstr);
                }
            }
            EraseIf(nonEmpty, [] (const TConstraintSet* c) { return !!c->GetConstraint<TEmptyConstraintNode>(); });

            if (nonEmpty.empty()) {
                input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
            } else if (nonEmpty.size() == 1) {
                input->SetConstraints(*nonEmpty.front());
            }
        }

        TApplyConstraintFromInput<TCoBase::idx_ListHandlerLambda, TEmptyConstraintNode>::Do(input);
        return FromFirst<TEmptyConstraintNode>(input, output, ctx);
    }

    template <bool Final>
    TStatus AggregateWrap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) const {
        if (HasSetting(input->Tail(), "session")) {
            // TODO: support sessions
            return TStatus::Ok;
        }

        if (const auto size = input->Child(1)->ChildrenSize()) {
            if constexpr (Final) {
                bool allKeysInOutput = true;
                if (auto outputColumnsSetting = GetSetting(input->Tail(), "output_columns")) {
                    THashSet<TStringBuf> outputColumns;
                    for (auto& col : outputColumnsSetting->Child(1)->Children()) {
                        YQL_ENSURE(col->IsAtom());
                        outputColumns.insert(col->Content());
                    }
                    allKeysInOutput = AllOf(input->Child(1)->Children(), [&](const auto& key) { return outputColumns.contains(key->Content()); });
                }
                if (allKeysInOutput) {
                    std::vector<std::string_view> columns;
                    columns.reserve(size);
                    for (const auto& child: input->Child(1)->Children()) {
                        columns.emplace_back(child->Content());
                    }
                    input->AddConstraint(ctx.MakeConstraint<TUniqueConstraintNode>(columns));
                    input->AddConstraint(ctx.MakeConstraint<TDistinctConstraintNode>(columns));
                }
            }
            return FromFirst<TEmptyConstraintNode>(input, output, ctx);
        }
        return TStatus::Ok;
    }

    TStatus FoldWrap(const TExprNode::TPtr& input, TExprNode::TPtr&, TExprContext& ctx) const {
        const TStructExprType* inItemType = GetNonEmptyStructItemType(*input->Head().GetTypeAnn());
        const TStructExprType* outItemType = GetNonEmptyStructItemType(*input->GetTypeAnn());
        if (!inItemType || !outItemType) {
            return UpdateAllChildLambdasConstraints(*input);
        }

        if (input->Child(1)->IsLambda()) {
            TConstraintNode::TListType argConstraints;
            if (const auto status = UpdateLambdaConstraints(input->ChildRef(1), ctx, {argConstraints}); status != TStatus::Ok) {
                return status;
            }
        }

        const auto initState = input->Child(1);
        auto stateConstraints = initState->GetAllConstraints();
        stateConstraints.erase(
            std::remove_if(
                stateConstraints.begin(),
                stateConstraints.end(),
                [](const TConstraintNode* c) { return c->GetName() == TEmptyConstraintNode::Name(); }
            ),
            stateConstraints.end()
        );

        TConstraintNode::TListType argConstraints;
        if (const auto status = UpdateLambdaConstraints(input->TailRef(), ctx, {argConstraints, stateConstraints}); status != TStatus::Ok) {
            return status;
        }

        return TStatus::Ok;
    }
private:
    template <class TConstraintContainer>
    static void CopyExcept(TConstraintContainer& dst, const TConstraintContainer& from, const TSet<TStringBuf>& except) {
        for (auto c: from.GetAllConstraints()) {
            if (!except.contains(c->GetName())) {
                dst.AddConstraint(c);
            }
        }
    }

    template <class TConstraintContainer>
    static void CopyExcept(TConstraintContainer& dst, const TConstraintContainer& from, TStringBuf except) {
        for (auto c: from.GetAllConstraints()) {
            if (c->GetName() != except) {
                dst.AddConstraint(c);
            }
        }
    }

    static void ExtractKeys(const TExprNode& keySelectorLambda, TVector<TStringBuf>& columns) {
        const auto arg = keySelectorLambda.Head().Child(0);
        auto body = keySelectorLambda.Child(1);
        if (body->IsCallable("StablePickle")) {
            body = body->Child(0);
        }
        ExtractSimpleKeys(body, arg, columns);
    }

    template<class TConstraintWithFields>
    static const TConstraintWithFields* GetDetailed(const TConstraintWithFields* constraint, const TTypeAnnotationNode& type, TExprContext& ctx) {
        return constraint ? constraint->GetComplicatedForType(type, ctx) : nullptr;
    }

    static const TStructExprType* GetNonEmptyStructItemType(const TTypeAnnotationNode& type) {
        const auto itemType = GetSeqItemType(&type);
        if (!itemType || itemType->GetKind() != ETypeAnnotationKind::Struct) {
            return nullptr;
        }
        const TStructExprType* structType = itemType->Cast<TStructExprType>();
        return structType->GetSize() ? structType : nullptr;
    }

    static const TSortedConstraintNode* DeduceSortConstraint(const TExprNode& directions, const TExprNode& keyExtractor, TExprContext& ctx) {
        if (const auto& columns = ExtractSimpleSortTraits(directions, keyExtractor); !columns.empty()) {
            TSortedConstraintNode::TContainerType content(columns.size());
            std::transform(columns.cbegin(), columns.cend(), content.begin(), [](const std::pair<TPartOfConstraintBase::TPathType, bool>& item) {
                return std::make_pair(TSortedConstraintNode::TSetType{item.first}, item.second);
            });
            return ctx.MakeConstraint<TSortedConstraintNode>(std::move(content));
        }
        return nullptr;
    }

    static const TVarIndexConstraintNode* GetVarIndexOverVarIndexConstraint(const TVarIndexConstraintNode& inputVarIndex,
        const TVarIndexConstraintNode& varIndex, TExprContext& ctx)
    {
        TVarIndexConstraintNode::TMapType result;
        for (auto& item: varIndex.GetIndexMapping()) {
            auto range = inputVarIndex.GetIndexMapping().equal_range(item.second);
            for (auto it = range.first; it != range.second; ++it) {
                result.push_back(std::make_pair(item.first, it->second));
            }
        }
        if (!result.empty()) {
            return ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(result));
        }
        return nullptr;
    }

    static const TExprNode* SkipModifiers(const TExprNode* valueNode) {
        if (TCoJust::Match(valueNode)) {
            return SkipModifiers(valueNode->Child(0));
        }
        if (TCoUnwrap::Match(valueNode)) {
            return SkipModifiers(valueNode->Child(0));
        }
        return valueNode;
    }
private:
    const bool SubGraph;
    std::unordered_map<std::string_view, THandler> Functions;
};

template<> const TPartOfSortedConstraintNode*
TCallableConstraintTransformer::GetConstraintFromWideResultLambda<TPartOfSortedConstraintNode>(const TExprNode& lambda, TExprContext& ctx) {
    TPartOfSortedConstraintNode::TMapType sorted;

    for (auto i = 1U; i < lambda.ChildrenSize(); ++i) {
        if (const auto part = lambda.Child(i)->GetConstraint<TPartOfSortedConstraintNode>())
            TPartOfSortedConstraintNode::UniqueMerge(sorted, part->GetColumnMapping(ctx.GetIndexAsString(i - 1U)));
    }

    return sorted.empty() ? nullptr : ctx.MakeConstraint<TPartOfSortedConstraintNode>(std::move(sorted));
}

template<> const TPartOfChoppedConstraintNode*
TCallableConstraintTransformer::GetConstraintFromWideResultLambda<TPartOfChoppedConstraintNode>(const TExprNode& lambda, TExprContext& ctx) {
    TPartOfChoppedConstraintNode::TMapType chopped;

    for (auto i = 1U; i < lambda.ChildrenSize(); ++i) {
        if (const auto part = lambda.Child(i)->GetConstraint<TPartOfChoppedConstraintNode>())
            TPartOfChoppedConstraintNode::UniqueMerge(chopped, part->GetColumnMapping(ctx.GetIndexAsString(i - 1U)));
    }

    return chopped.empty() ? nullptr : ctx.MakeConstraint<TPartOfChoppedConstraintNode>(std::move(chopped));
}

template<> const TPartOfUniqueConstraintNode*
TCallableConstraintTransformer::GetConstraintFromWideResultLambda<TPartOfUniqueConstraintNode>(const TExprNode& lambda, TExprContext& ctx) {
    TPartOfUniqueConstraintNode::TMapType uniques;

    for (auto i = 1U; i < lambda.ChildrenSize(); ++i) {
        if (const auto part = lambda.Child(i)->GetConstraint<TPartOfUniqueConstraintNode>())
            TPartOfUniqueConstraintNode::UniqueMerge(uniques, part->GetColumnMapping(ctx.GetIndexAsString(i - 1U)));
    }

    return uniques.empty() ? nullptr : ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(uniques));
}

template<> const TPartOfDistinctConstraintNode*
TCallableConstraintTransformer::GetConstraintFromWideResultLambda<TPartOfDistinctConstraintNode>(const TExprNode& lambda, TExprContext& ctx) {
    TPartOfDistinctConstraintNode::TMapType uniques;

    for (auto i = 1U; i < lambda.ChildrenSize(); ++i) {
        if (const auto part = lambda.Child(i)->GetConstraint<TPartOfDistinctConstraintNode>())
            TPartOfDistinctConstraintNode::UniqueMerge(uniques, part->GetColumnMapping(ctx.GetIndexAsString(i - 1U)));
    }

    return uniques.empty() ? nullptr : ctx.MakeConstraint<TPartOfDistinctConstraintNode>(std::move(uniques));
}

template<> const TVarIndexConstraintNode*
TCallableConstraintTransformer::TCallableConstraintTransformer::GetConstraintFromWideResultLambda<TVarIndexConstraintNode>(const TExprNode& lambda, TExprContext& ctx) {
    TVector<const TConstraintSet*> structConstraints;
    structConstraints.reserve(lambda.ChildrenSize() - 1U);

    for (auto i = 1U; i < lambda.ChildrenSize(); ++i) {
        auto valueNode = lambda.Child(i);
        if (TCoCoalesce::Match(valueNode)) {
            if (valueNode->Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional || valueNode->ChildrenSize() == 1) {
                valueNode = valueNode->Child(0);
            }
        }
        if (TCoJust::Match(valueNode)) {
            valueNode = valueNode->Child(0);
        }

        if (TCoMember::Match(valueNode) || TCoNth::Match(valueNode)) {
            structConstraints.push_back(&valueNode->Head().GetConstraintSet());
        } else if (valueNode->Type() == TExprNode::Argument) {
            structConstraints.push_back(&valueNode->GetConstraintSet());
        }
    }

    return TVarIndexConstraintNode::MakeCommon(structConstraints, ctx);
}

template<class TConstraint> const TConstraint*
TCallableConstraintTransformer::GetConstraintFromWideResultLambda(const TExprNode&, TExprContext&) { return nullptr; }

class TDefaultCallableConstraintTransformer : public TSyncTransformerBase {
public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        Y_UNUSED(output);
        Y_UNUSED(ctx);
        return UpdateAllChildLambdasConstraints(*input);
    }

    void Rewind() final {
    }
};

template<bool DisableCheck>
class TConstraintTransformer : public TGraphTransformerBase {
public:
    TConstraintTransformer(TAutoPtr<IGraphTransformer> callableTransformer, TTypeAnnotationContext& types)
        : CallableTransformer(callableTransformer)
        , Types(types)
    {
    }

    ~TConstraintTransformer() = default;

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_PROFILE_SCOPE(DEBUG, "ConstraintTransformer::DoTransform");
        output = input;
        auto status = TransformNode(input, output, ctx);
        UpdateStatusIfChanged(status, input, output);

        if (status.Level != TStatus::Error && HasRenames) {
            output = ctx.ReplaceNodes(std::move(output), Processed);
        }

        Processed.clear();
        if (status == TStatus::Ok) {
            Types.ExpectedConstraints.clear();
        }

        HasRenames = false;
        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        YQL_PROFILE_SCOPE(DEBUG, "ConstraintTransformer::DoGetAsyncFuture");
        Y_UNUSED(input);
        TVector<NThreading::TFuture<void>> futures;
        for (const auto& callable : CallableInputs) {
            futures.push_back(CallableTransformer->GetAsyncFuture(*callable));
        }

        return WaitExceptionOrAll(futures);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_PROFILE_SCOPE(DEBUG, "ConstraintTransformer::DoApplyAsyncChanges");
        output = input;
        TStatus combinedStatus = TStatus::Ok;
        for (const auto& callable : CallableInputs) {
            callable->SetState(TExprNode::EState::ConstrPending);
            TExprNode::TPtr callableOutput;
            auto status = CallableTransformer->ApplyAsyncChanges(callable, callableOutput, ctx);
            Y_ABORT_UNLESS(callableOutput);
            YQL_ENSURE(status != TStatus::Async);
            YQL_ENSURE(callableOutput == callable);
            combinedStatus = combinedStatus.Combine(status);
            if (status.Level == TStatus::Error) {
                callable->SetState(TExprNode::EState::Error);
            }
        }

        CallableInputs.clear();
        if (combinedStatus.Level == TStatus::Ok) {
            Processed.clear();
        }

        return combinedStatus;
    }

    void Rewind() final {
        CallableTransformer->Rewind();
        CallableInputs.clear();
        Processed.clear();
        HasRenames = false;
        CurrentFunctions = {};
    }

private:
    TStatus TransformNode(const TExprNode::TPtr& start, TExprNode::TPtr& output, TExprContext& ctx) {
        output = start;
        auto processedPair = Processed.emplace(start.Get(), nullptr); // by default node is not changed
        if (!processedPair.second) {
            if (processedPair.first->second) {
                output = processedPair.first->second;
                return TStatus::Repeat;
            }

            switch (start->GetState()) {
            case TExprNode::EState::Initial:
            case TExprNode::EState::TypeInProgress:
            case TExprNode::EState::TypePending:
                return TStatus(TStatus::Repeat, true);
            case TExprNode::EState::TypeComplete:
                break;
            case TExprNode::EState::ConstrInProgress:
                return IGraphTransformer::TStatus::Async;
            case TExprNode::EState::ConstrPending:
                if (start->Type() == TExprNode::Lambda) {
                    if (start->Head().GetState() != TExprNode::EState::ConstrComplete) {
                        return TStatus::Ok;
                    } else if (start->Head().ChildrenSize() == 0) {
                        break;
                    }
                }

                if (start->Type() == TExprNode::Arguments || start->Type() == TExprNode::Argument) {
                    break;
                }

                return TStatus(TStatus::Repeat, true);
            case TExprNode::EState::ConstrComplete:
            case TExprNode::EState::ExecutionInProgress:
            case TExprNode::EState::ExecutionRequired:
            case TExprNode::EState::ExecutionPending:
            case TExprNode::EState::ExecutionComplete:
                return TStatus::Ok;
            case TExprNode::EState::Error:
                return TStatus::Error;
            default:
                YQL_ENSURE(false, "Unknown state");
            }
        }

        auto input = start;
        for (;;) {
            TIssueScopeGuard issueScope(ctx.IssueManager, [this, input, &ctx]() -> TIssuePtr {
                TStringBuilder str;
                str << "At ";
                switch (input->Type()) {
                case TExprNode::Callable:
                    if (!CurrentFunctions.empty() && CurrentFunctions.top().second) {
                        return nullptr;
                    }

                    if (!CurrentFunctions.empty()) {
                        CurrentFunctions.top().second = true;
                    }

                    str << "function: " << input->Content();
                    break;
                case TExprNode::List:
                    if (CurrentFunctions.empty()) {
                        str << "tuple";
                    } else if (!CurrentFunctions.top().second) {
                        CurrentFunctions.top().second = true;
                        str << "function: " << CurrentFunctions.top().first;
                    } else {
                        return nullptr;
                    }
                    break;
                case TExprNode::Lambda:
                    if (CurrentFunctions.empty()) {
                        str << "lambda";
                    } else if (!CurrentFunctions.top().second) {
                        CurrentFunctions.top().second = true;
                        str << "function: " << CurrentFunctions.top().first;
                    } else {
                        return nullptr;
                    }
                    break;
                default:
                    str << "unknown";
                }

                return MakeIntrusive<TIssue>(ctx.GetPosition(input->Pos()), str);
            });

            if (input->IsCallable()) {
                CurrentFunctions.emplace(input->Content(), false);
            }
            Y_SCOPE_EXIT(this, input) {
                if (input->IsCallable()) {
                    CurrentFunctions.pop();
                    if (!CurrentFunctions.empty() && CurrentFunctions.top().first.ends_with('!')) {
                        CurrentFunctions.top().second = true;
                    }
                }
            };

            TStatus retStatus = TStatus::Error;
            switch (input->GetState()) {
            case TExprNode::EState::Initial:
            case TExprNode::EState::TypeInProgress:
            case TExprNode::EState::TypePending:
                return TStatus(TStatus::Repeat, true);
            case TExprNode::EState::TypeComplete:
            case TExprNode::EState::ConstrPending:
                break;
            case TExprNode::EState::ConstrInProgress:
                return IGraphTransformer::TStatus::Async;
            case TExprNode::EState::ConstrComplete:
            case TExprNode::EState::ExecutionInProgress:
            case TExprNode::EState::ExecutionRequired:
            case TExprNode::EState::ExecutionPending:
            case TExprNode::EState::ExecutionComplete:
                return TStatus::Ok;
            case TExprNode::EState::Error:
                return TStatus::Error;
            default:
                YQL_ENSURE(false, "Unknown state");
            }

            input->SetState(TExprNode::EState::ConstrPending);
            switch (input->Type()) {
            case TExprNode::Atom:
            case TExprNode::World:
                input->SetState(TExprNode::EState::ConstrComplete);
                CheckExpected(*input);
                return TStatus::Ok;

            case TExprNode::List:
            {
                retStatus = TransformChildren(input, output, ctx);
                if (retStatus == TStatus::Ok) {
                    retStatus = CallableTransformer->Transform(input, output, ctx);
                    if (retStatus == TStatus::Ok) {
                        input->SetState(TExprNode::EState::ConstrComplete);
                        CheckExpected(*input);
                        break;
                    }
                }

                if (retStatus != TStatus::Error && input != output) {
                    processedPair.first->second = output;
                }
                break;
            }

            case TExprNode::Lambda:
            {
                YQL_ENSURE(input->ChildrenSize() > 0U);
                TExprNode::TPtr out;
                auto argStatus = TransformNode(input->HeadPtr(), out, ctx);
                UpdateStatusIfChanged(argStatus, input->HeadPtr(), out);
                if (argStatus.Level == TStatus::Error) {
                    input->SetState(TExprNode::EState::Error);
                    return argStatus;
                }

                if (argStatus.Level == TStatus::Repeat)
                    return TStatus::Ok;

                TStatus bodyStatus = TStatus::Ok;
                TExprNode::TListType newBody;
                newBody.reserve(input->ChildrenSize() - 1U);
                bool updatedChildren = false;
                for (ui32 i = 1U; i < input->ChildrenSize(); ++i) {
                    const auto child = input->ChildPtr(i);
                    TExprNode::TPtr newChild;
                    auto childStatus = TransformNode(child, newChild, ctx);
                    UpdateStatusIfChanged(childStatus, child, newChild);
                    updatedChildren = updatedChildren || (newChild != child);
                    bodyStatus = bodyStatus.Combine(childStatus);
                    newBody.emplace_back(std::move(newChild));
                }

                retStatus = argStatus.Combine(bodyStatus);
                if (retStatus != TStatus::Ok) {
                    if (retStatus.Level == TStatus::Error) {
                        input->SetState(TExprNode::EState::Error);
                    }
                    else if (updatedChildren) {
                        output = ctx.DeepCopyLambda(*input, std::move(newBody));
                        processedPair.first->second = output;
                        HasRenames = true;
                    }
                } else {
                    if (input->ChildrenSize() != 2U)
                        input->SetState(TExprNode::EState::ConstrComplete);
                    else
                        input->CopyConstraints(input->Tail());
                    CheckExpected(*input);
                }
                break;
            }

            case TExprNode::Argument:
                if (input->GetState() != TExprNode::EState::ConstrComplete) {
                    return TStatus::Repeat;
                }

                return TStatus::Ok;

            case TExprNode::Arguments:
            {
                if (input->Children().empty()) {
                    if (TExprNode::EState::ConstrComplete == input->GetState()) {
                        return TStatus::Ok;
                    }
                    return TStatus::Repeat;
                }

                retStatus = TStatus::Ok;
                for (auto& child : input->Children()) {
                    TExprNode::TPtr tmp;
                    auto childStatus = TransformNode(child, tmp, ctx);
                    UpdateStatusIfChanged(childStatus, child, tmp);
                    YQL_ENSURE(tmp == child);
                    retStatus = retStatus.Combine(childStatus);
                }

                if (retStatus != TStatus::Ok) {
                    if (retStatus.Level == TStatus::Error) {
                        input->SetState(TExprNode::EState::Error);
                    }
                } else {
                    input->SetState(TExprNode::EState::ConstrComplete);
                }
                return retStatus;
            }

            case TExprNode::Callable:
            {
                retStatus = TransformChildren(input, output, ctx);
                if (retStatus != TStatus::Ok) {
                    if (retStatus != TStatus::Error && input != output) {
                        processedPair.first->second = output;
                    }
                    break;
                }

                CurrentFunctions.top().second = true;
                retStatus = CallableTransformer->Transform(input, output, ctx);
                if (retStatus == TStatus::Error) {
                    input->SetState(TExprNode::EState::Error);
                } else if (retStatus == TStatus::Ok) {
                    // Sanity check
                    for (size_t i = 0; i < input->ChildrenSize(); ++i) {
                        YQL_ENSURE(input->Child(i)->GetState() >= TExprNode::EState::ConstrComplete,
                            "Child with index " << i << " of callable " << TString{input->Content()}.Quote() << " has bad state after constraint transform");
                    }
                    input->SetState(TExprNode::EState::ConstrComplete);
                    CheckExpected(*input);
                } else if (retStatus == TStatus::Async) {
                    CallableInputs.push_back(input);
                    input->SetState(TExprNode::EState::ConstrInProgress);
                } else {
                    if (output != input.Get()) {
                        processedPair.first->second = output;
                        HasRenames = true;
                    }
                }
                break;
            }
            default:
                YQL_ENSURE(false, "Unknown type");
            }

            if (retStatus.Level != TStatus::Repeat || retStatus.HasRestart) {
                return retStatus;
            }

            input = output;
        }
    }

    TStatus TransformChildren(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TStatus combinedStatus = TStatus::Ok;
        TExprNode::TListType newChildren;
        bool updatedChildren = false;
        for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
            const auto child = input->ChildPtr(i);
            TExprNode::TPtr newChild;
            auto childStatus = TransformNode(child, newChild, ctx);
            UpdateStatusIfChanged(childStatus, child, newChild);
            updatedChildren = updatedChildren || (newChild != child);
            combinedStatus = combinedStatus.Combine(childStatus);
            newChildren.emplace_back(std::move(newChild));
        }

        if (combinedStatus != TStatus::Ok) {
            if (combinedStatus.Level == TStatus::Error) {
                input->SetState(TExprNode::EState::Error);
            }
            else if (updatedChildren) {
                output = ctx.ChangeChildren(*input, std::move(newChildren));
                HasRenames = true;
            }
        }
        return combinedStatus;
    }

    void UpdateStatusIfChanged(TStatus& status, const TExprNode::TPtr& input, const TExprNode::TPtr& output) {
        if (status.Level == TStatus::Ok && input != output) {
            status = TStatus(TStatus::Repeat, status.HasRestart);
        }
    }

    void CheckExpected(const TExprNode& input) {
        if constexpr (DisableCheck)
            return;

        if (const auto it = Types.ExpectedConstraints.find(input.UniqueId()); it != Types.ExpectedConstraints.cend()) {
            for (const auto expectedConstr: it->second) {
                if (!Types.DisableConstraintCheck.contains(expectedConstr->GetName())) {
                    if (auto newConstr = input.GetConstraint(expectedConstr->GetName())) {
                        if (expectedConstr->GetName() == TMultiConstraintNode::Name()) {
                            YQL_ENSURE(static_cast<const TMultiConstraintNode*>(newConstr)->FilteredIncludes(*expectedConstr, Types.DisableConstraintCheck), "Rewrite error, unequal " << *newConstr
                                << " constraint in node " << input.Content() << ", previous was " << *expectedConstr);
                        } else {
                            YQL_ENSURE(newConstr->Includes(*expectedConstr), "Rewrite error, unequal " << *newConstr
                                << " constraint in node " << input.Content() << ", previous was " << *expectedConstr);
                        }
                    } else {
                        if (expectedConstr->GetName() == TMultiConstraintNode::Name()) {
                            // Constraint Multi(0:{Empty},1:{Empty}, ..., N:{Empty}) can be reduced to Empty
                            newConstr = input.GetConstraint<TEmptyConstraintNode>();
                        }
                        YQL_ENSURE(newConstr, "Rewrite error, missing " << *expectedConstr << " constraint in node " << input.Content());
                    }
                }
            }
        }
    }
private:
    TAutoPtr<IGraphTransformer> CallableTransformer;
    std::deque<TExprNode::TPtr> CallableInputs;
    TNodeOnNodeOwnedMap Processed;
    bool HasRenames = false;
    std::stack<std::pair<std::string_view, bool>> CurrentFunctions;
    TTypeAnnotationContext& Types;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateConstraintTransformer(TTypeAnnotationContext& types, bool instantOnly, bool subGraph, bool disableCheck) {
    TAutoPtr<IGraphTransformer> callableTransformer(new TCallableConstraintTransformer(types, instantOnly, subGraph));
    return disableCheck ?
        static_cast<IGraphTransformer*>(new TConstraintTransformer<true>(callableTransformer, types)):
        static_cast<IGraphTransformer*>(new TConstraintTransformer<false>(callableTransformer, types));
}

TAutoPtr<IGraphTransformer> CreateDefCallableConstraintTransformer() {
    return new TDefaultCallableConstraintTransformer();
}

IGraphTransformer::TStatus UpdateLambdaConstraints(const TExprNode& lambda) {
    const auto args = lambda.Child(0);
    for (const auto& arg: args->Children()) {
        if (arg->GetState() == TExprNode::EState::TypeComplete || arg->GetState() == TExprNode::EState::ConstrPending) {
            arg->SetState(TExprNode::EState::ConstrComplete);
        }
        YQL_ENSURE(arg->GetAllConstraints().empty());
    }

    if (args->GetState() == TExprNode::EState::TypeComplete || args->GetState() == TExprNode::EState::ConstrPending) {
        args->SetState(TExprNode::EState::ConstrComplete);
    }

    if (lambda.GetState() != TExprNode::EState::ConstrComplete) {
        return IGraphTransformer::TStatus::Repeat;
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus UpdateLambdaConstraints(TExprNode::TPtr& lambda, TExprContext& ctx, const TArrayRef<const TConstraintNode::TListType>& constraints) {
    bool updateArgs = false;
    const auto args = lambda->Child(0);

    YQL_ENSURE(args->ChildrenSize() == constraints.size());
    size_t i = 0;
    for (const auto& constrList: constraints) {
        const auto arg = args->Child(i++);
        if (arg->GetState() == TExprNode::EState::TypeComplete || arg->GetState() == TExprNode::EState::ConstrPending) {
            for (const auto c: constrList) {
                arg->AddConstraint(c);
            }
            arg->SetState(TExprNode::EState::ConstrComplete);
        } else {
            if (constrList.size() != arg->GetAllConstraints().size() || !AllOf(constrList, [arg] (const TConstraintNode* c) { return arg->GetConstraint(c->GetName()) == c; })) {
                updateArgs = true;
            }
        }
    }

    if (updateArgs) {
        TNodeOnNodeOwnedMap replaces(constraints.size());
        TExprNode::TListType argsChildren;
        argsChildren.reserve(constraints.size());
        i = 0;
        for (const auto& constrList: constraints) {
            const auto arg = args->Child(i++);
            const auto newArg = ctx.ShallowCopy(*arg);
            newArg->SetTypeAnn(arg->GetTypeAnn());
            for (const auto c: constrList) {
                newArg->AddConstraint(c);
            }
            newArg->SetState(TExprNode::EState::ConstrComplete);
            YQL_ENSURE(replaces.emplace(arg, newArg).second);
            argsChildren.emplace_back(std::move(newArg));
        }

        auto newArgs = ctx.NewArguments(args->Pos(), std::move(argsChildren));
        newArgs->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        newArgs->SetState(TExprNode::EState::ConstrComplete);
        const auto type = lambda->GetTypeAnn();
        lambda = ctx.NewLambda(lambda->Pos(), std::move(newArgs), ctx.ReplaceNodes<true>(GetLambdaBody(*lambda), replaces));
        lambda->SetTypeAnn(type);
        lambda->Head().ForEachChild(std::bind(&TExprNode::SetDependencyScope, std::placeholders::_1, lambda.Get(), lambda.Get()));
        return IGraphTransformer::TStatus::Repeat;
    }

    if (args->GetState() == TExprNode::EState::TypeComplete || args->GetState() == TExprNode::EState::ConstrPending) {
        args->SetState(TExprNode::EState::ConstrComplete);
    }

    if (lambda->GetState() != TExprNode::EState::ConstrComplete) {
        return IGraphTransformer::TStatus::Repeat;
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus UpdateAllChildLambdasConstraints(const TExprNode& node) {
    IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;
    for (ui32 i = 0; i < node.ChildrenSize(); ++i) {
        const auto child = node.Child(i);
        if (child->Type() == TExprNode::EType::Lambda) {
            status = status.Combine(UpdateLambdaConstraints(*child));
        }
    }
    return status;
}

} // namespace NYql
