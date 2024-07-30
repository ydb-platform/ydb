#include "yql_yt_phy_opt.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>

#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NYql {

using namespace NNodes;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::LambdaFieldsSubset(TYtWithUserJobsOpBase op, size_t lambdaIdx, TExprContext& ctx, const TGetParents& getParents) const {
    auto lambda = TCoLambda(op.Ref().ChildPtr(lambdaIdx));

    bool hasUpdates = false;
    TYtSection section = op.Input().Item(0);

    const TParentsMap* parentsMap = getParents();
    auto parents = parentsMap->find(lambda.Args().Arg(0).Raw());
    if (parents == parentsMap->cend()) {
        // Argument is not used in lambda body
        return op;
    }
    if (parents->second.size() == 1 && TCoExtractMembers::Match(*parents->second.begin())) {
        auto members = TCoExtractMembers(*parents->second.begin()).Members();
        TSet<TStringBuf> memberSet;
        std::for_each(members.begin(), members.end(), [&memberSet](const auto& m) { memberSet.insert(m.Value()); });
        auto reduceBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::ReduceBy);
        memberSet.insert(reduceBy.cbegin(), reduceBy.cend());
        auto sortBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::SortBy);
        memberSet.insert(sortBy.cbegin(), sortBy.cend());

        auto itemType = GetSeqItemType(lambda.Args().Arg(0).Ref().GetTypeAnn())->Cast<TStructExprType>();
        if (memberSet.size() < itemType->GetSize()) {
            section = UpdateInputFields(section, std::move(memberSet), ctx, NYql::HasSetting(op.Settings().Ref(), EYtSettingType::WeakFields));
            hasUpdates = true;
        }
    }

    if (!hasUpdates) {
        return op;
    }

    auto res = ctx.ChangeChild(op.Ref(), TYtTransientOpBase::idx_Input,
        Build<TYtSectionList>(ctx, op.Input().Pos())
            .Add(section)
            .Done().Ptr());

    res = ctx.ChangeChild(*res, lambdaIdx, ctx.DeepCopyLambda(lambda.Ref()));

    return TExprBase(res);

}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::LambdaVisitFieldsSubset(TYtWithUserJobsOpBase op, size_t lambdaIdx, TExprContext& ctx, const TGetParents& getParents) const {
    auto opLambda = TCoLambda(op.Ref().ChildPtr(lambdaIdx));

    const TParentsMap* parentsMap = getParents();
    auto maybeLambda = GetFlatMapOverInputStream(opLambda, *parentsMap).Lambda();
    if (!maybeLambda) {
        return op;
    }

    TCoLambda lambda = maybeLambda.Cast();
    auto arg = lambda.Args().Arg(0);

    // Check arg is used only in Visit
    auto it = parentsMap->find(arg.Raw());
    if (it == parentsMap->cend() || it->second.size() != 1 || !TCoVisit::Match(*it->second.begin())) {
        return op;
    }

    const TExprNode* visit = *it->second.begin();
    TVector<std::pair<size_t, TSet<TStringBuf>>> sectionFields;
    for (ui32 index = 1; index < visit->ChildrenSize(); ++index) {
        if (visit->Child(index)->IsAtom()) {
            size_t inputNum = FromString<size_t>(visit->Child(index)->Content());
            YQL_ENSURE(inputNum < op.Input().Size());

            ++index;
            auto visitLambda = visit->ChildPtr(index);

            TSet<TStringBuf> memberSet;
            if (HaveFieldsSubset(visitLambda->TailPtr(), visitLambda->Head().Head(), memberSet, *parentsMap)) {
                auto itemType = visitLambda->Head().Head().GetTypeAnn()->Cast<TStructExprType>();
                auto reduceBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::ReduceBy);
                for (auto& col: reduceBy) {
                    if (auto type = itemType->FindItemType(col)) {
                        memberSet.insert(type->Cast<TItemExprType>()->GetName());
                    }
                }
                auto sortBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::SortBy);
                for (auto& col: sortBy) {
                    if (auto type = itemType->FindItemType(col)) {
                        memberSet.insert(type->Cast<TItemExprType>()->GetName());
                    }
                }

                if (memberSet.size() < itemType->GetSize()) {
                    sectionFields.emplace_back(inputNum, std::move(memberSet));
                }
            }
        }
    }

    if (sectionFields.empty()) {
        return op;
    }

    auto res = ctx.ChangeChild(op.Ref(), lambdaIdx, ctx.DeepCopyLambda(opLambda.Ref()));

    TVector<TYtSection> updatedSections(op.Input().begin(), op.Input().end());
    const bool hasWeak = NYql::HasSetting(op.Settings().Ref(), EYtSettingType::WeakFields);
    for (auto& pair: sectionFields) {
        auto& section = updatedSections[pair.first];
        auto& memberSet = pair.second;
        section = UpdateInputFields(section, std::move(memberSet), ctx, hasWeak);
    }

    res = ctx.ChangeChild(*res, TYtTransientOpBase::idx_Input,
        Build<TYtSectionList>(ctx, op.Input().Pos())
            .Add(updatedSections)
            .Done().Ptr());

    return TExprBase(res);
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::MapFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();
    if (op.Input().Size() != 1) {
        return node;
    }
    if (auto map = op.Maybe<TYtMap>()) {
        return LambdaFieldsSubset(op, TYtMap::idx_Mapper, ctx, getParents);
    } else if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
        return LambdaFieldsSubset(op, TYtMapReduce::idx_Mapper, ctx, getParents);
    }

    return node;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ReduceFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();
    if (op.Input().Size() != 1) {
        return node;
    }
    if (auto reduce = op.Maybe<TYtReduce>()) {
        return LambdaFieldsSubset(op, TYtReduce::idx_Reducer, ctx, getParents);
    } else if (!op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
        return LambdaFieldsSubset(op, TYtMapReduce::idx_Reducer, ctx, getParents);
    }

    return node;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::MultiMapFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();
    if (op.Input().Size() < 2) {
        return node;
    }
    if (auto map = op.Maybe<TYtMap>()) {
        return LambdaVisitFieldsSubset(op, TYtMap::idx_Mapper, ctx, getParents);
    } else if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
        return LambdaVisitFieldsSubset(op, TYtMapReduce::idx_Mapper, ctx, getParents);
    }

    return node;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::MultiReduceFieldsSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();
    if (op.Input().Size() < 2) {
        return node;
    }
    if (auto reduce = op.Maybe<TYtReduce>()) {
        return LambdaVisitFieldsSubset(op, TYtReduce::idx_Reducer, ctx, getParents);
    } else if (!op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>()) {
        return LambdaVisitFieldsSubset(op, TYtMapReduce::idx_Reducer, ctx, getParents);
    }

    return node;
}

}  // namespace NYql
