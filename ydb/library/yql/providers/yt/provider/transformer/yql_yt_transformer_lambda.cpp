
#include <ydb/library/yql/providers/yt/provider/yql_yt_transformer.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_transformer_helper.h>

#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/xrange.h>
#include <util/string/type.h>

namespace NYql {

using namespace NPrivate;

TCoLambda TYtPhysicalOptProposalTransformer::MakeJobLambdaNoArg(TExprBase content, TExprContext& ctx) const {
    if (State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
        content = Build<TCoToFlow>(ctx, content.Pos()).Input(content).Done();
    } else {
        content = Build<TCoToStream>(ctx, content.Pos()).Input(content).Done();
    }

    return Build<TCoLambda>(ctx, content.Pos()).Args({}).Body(content).Done();
}

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

template<>
TCoLambda TYtPhysicalOptProposalTransformer::MakeJobLambda<false>(TCoLambda lambda, bool useFlow, TExprContext& ctx) const
{
    if (useFlow) {
        return Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"flow"})
            .Body<TCoToFlow>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With<TCoFromFlow>(0)
                        .Input("flow")
                    .Build()
                .Build()
            .Build()
        .Done();
    } else {
        return Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"stream"})
            .Body<TCoToStream>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With(0, "stream")
                .Build()
            .Build()
        .Done();
    }
}

template<>
TCoLambda TYtPhysicalOptProposalTransformer::MakeJobLambda<true>(TCoLambda lambda, bool useFlow, TExprContext& ctx) const
{
    if (useFlow) {
        return Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"flow"})
            .Body<TCoToFlow>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With<TCoForwardList>(0)
                        .Stream("flow")
                    .Build()
                .Build()
            .Build()
        .Done();
    } else {
        return Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"stream"})
            .Body<TCoToStream>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With<TCoForwardList>(0)
                        .Stream("stream")
                    .Build()
                .Build()
            .Build()
        .Done();
    }
}

TMaybe<bool>
TYtPhysicalOptProposalTransformer::CanFuseLambdas(const TCoLambda& innerLambda, const TCoLambda& outerLambda, TExprContext& ctx) const {
    auto maxJobMemoryLimit = State_->Configuration->MaxExtraJobMemoryToFuseOperations.Get();
    auto maxOperationFiles = State_->Configuration->MaxOperationFiles.Get().GetOrElse(DEFAULT_MAX_OPERATION_FILES);
    TMap<TStringBuf, ui64> memUsage;

    TExprNode::TPtr updatedBody = innerLambda.Body().Ptr();
    if (maxJobMemoryLimit) {
        auto status = UpdateTableContentMemoryUsage(innerLambda.Body().Ptr(), updatedBody, State_, ctx);
        if (status.Level != TStatus::Ok) {
            return {};
        }
    }
    size_t innerFiles = 1; // jobstate. Take into account only once
    ScanResourceUsage(*updatedBody, *State_->Configuration, State_->Types, maxJobMemoryLimit ? &memUsage : nullptr, nullptr, &innerFiles);

    auto prevMemory = Accumulate(memUsage.begin(), memUsage.end(), 0ul,
        [](ui64 sum, const std::pair<const TStringBuf, ui64>& val) { return sum + val.second; });

    updatedBody = outerLambda.Body().Ptr();
    if (maxJobMemoryLimit) {
        auto status = UpdateTableContentMemoryUsage(outerLambda.Body().Ptr(), updatedBody, State_, ctx);
        if (status.Level != TStatus::Ok) {
            return {};
        }
    }
    size_t outerFiles = 0;
    ScanResourceUsage(*updatedBody, *State_->Configuration, State_->Types, maxJobMemoryLimit ? &memUsage : nullptr, nullptr, &outerFiles);

    auto currMemory = Accumulate(memUsage.begin(), memUsage.end(), 0ul,
        [](ui64 sum, const std::pair<const TStringBuf, ui64>& val) { return sum + val.second; });

    if (maxJobMemoryLimit && currMemory != prevMemory && currMemory > *maxJobMemoryLimit) {
        YQL_CLOG(DEBUG, ProviderYt) << "Memory usage: innerLambda=" << prevMemory
            << ", joinedLambda=" << currMemory << ", MaxJobMemoryLimit=" << *maxJobMemoryLimit;
        return false;
    }
    if (innerFiles + outerFiles > maxOperationFiles) {
        YQL_CLOG(DEBUG, ProviderYt) << "Files usage: innerLambda=" << innerFiles
            << ", outerLambda=" << outerFiles << ", MaxOperationFiles=" << maxOperationFiles;
        return false;
    }

    if (auto maxReplcationFactor = State_->Configuration->MaxReplicationFactorToFuseOperations.Get()) {
        double replicationFactor1 = NCommon::GetDataReplicationFactor(innerLambda.Ref(), ctx);
        double replicationFactor2 = NCommon::GetDataReplicationFactor(outerLambda.Ref(), ctx);
        YQL_CLOG(DEBUG, ProviderYt) << "Replication factors: innerLambda=" << replicationFactor1
            << ", outerLambda=" << replicationFactor2 << ", MaxReplicationFactorToFuseOperations=" << *maxReplcationFactor;

        if (replicationFactor1 > 1.0 && replicationFactor2 > 1.0 && replicationFactor1 * replicationFactor2 > *maxReplcationFactor) {
            return false;
        }
    }
    return true;
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

}  // namespace NYql 
