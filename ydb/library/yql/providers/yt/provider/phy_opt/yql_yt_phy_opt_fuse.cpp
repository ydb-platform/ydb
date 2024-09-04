#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_optimize.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;
using namespace NPrivate;

TMaybe<bool> TYtPhysicalOptProposalTransformer::CanFuseLambdas(const TCoLambda& innerLambda, const TCoLambda& outerLambda, TExprContext& ctx) const {
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

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::FuseReduce(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto outerReduce = node.Cast<TYtReduce>();

    if (outerReduce.Input().Size() != 1 || outerReduce.Input().Item(0).Paths().Size() != 1) {
        return node;
    }
    if (outerReduce.Input().Item(0).Settings().Size() != 0) {
        return node;
    }
    TYtPath path = outerReduce.Input().Item(0).Paths().Item(0);
    if (!path.Ranges().Maybe<TCoVoid>()) {
        return node;
    }
    auto maybeInnerReduce = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtReduce>();
    if (!maybeInnerReduce) {
        return node;
    }
    TYtReduce innerReduce = maybeInnerReduce.Cast();

    if (innerReduce.Ref().StartsExecution() || innerReduce.Ref().HasResult()) {
        return node;
    }
    if (innerReduce.Output().Size() > 1) {
        return node;
    }

    if (outerReduce.DataSink().Cluster().Value() != innerReduce.DataSink().Cluster().Value()) {
        return node;
    }

    const TParentsMap* parentsReduce = getParents();
    if (IsOutputUsedMultipleTimes(innerReduce.Ref(), *parentsReduce)) {
        // Inner reduce output is used more than once
        return node;
    }
    // Check world dependencies
    auto parentsIt = parentsReduce->find(innerReduce.Raw());
    YQL_ENSURE(parentsIt != parentsReduce->cend());
    for (auto dep: parentsIt->second) {
        if (!TYtOutput::Match(dep)) {
            return node;
        }
    }

    if (!NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::KeySwitch) ||
        !NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::Flow) ||
        !NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::ReduceBy)) {
        return node;
    }
    if (NYql::HasSetting(outerReduce.Settings().Ref(), EYtSettingType::SortBy)) {
        auto innerSortBy = NYql::GetSettingAsColumnList(innerReduce.Settings().Ref(), EYtSettingType::SortBy);
        auto outerSortBy = NYql::GetSettingAsColumnList(outerReduce.Settings().Ref(), EYtSettingType::SortBy);
        if (outerSortBy.size() > innerSortBy.size()) {
            return node;
        }
        if (!std::equal(outerSortBy.cbegin(), outerSortBy.cend(), innerSortBy.cbegin())) {
            return node;
        }
    }

    if (NYql::HasSettingsExcept(innerReduce.Settings().Ref(), EYtSettingType::ReduceBy |
                                                             EYtSettingType::KeySwitch |
                                                             EYtSettingType::Flow |
                                                             EYtSettingType::FirstAsPrimary |
                                                             EYtSettingType::SortBy |
                                                             EYtSettingType::KeepSorted |
                                                             EYtSettingType::NoDq)) {
        return node;
    }

    if (!EqualSettingsExcept(innerReduce.Settings().Ref(), outerReduce.Settings().Ref(),
                                                            EYtSettingType::ReduceBy |
                                                            EYtSettingType::FirstAsPrimary |
                                                            EYtSettingType::NoDq |
                                                            EYtSettingType::SortBy)) {
        return node;
    }

    auto innerLambda = innerReduce.Reducer();
    auto outerLambda = outerReduce.Reducer();
    auto fuseRes = CanFuseLambdas(innerLambda, outerLambda, ctx);
    if (!fuseRes) {
        // Some error
        return {};
    }
    if (!*fuseRes) {
        // Cannot fuse
        return node;
    }

    auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerLambda.Ptr(), ctx, State_->Types);
    if (!placeHolder) {
        return {};
    }


    if (lambdaWithPlaceholder != outerLambda.Ptr()) {
        outerLambda = TCoLambda(lambdaWithPlaceholder);
    }

    innerLambda = FallbackLambdaOutput(innerLambda, ctx);
    outerLambda = FallbackLambdaInput(outerLambda, ctx);


    const auto outerReduceBy = NYql::GetSettingAsColumnList(outerReduce.Settings().Ref(), EYtSettingType::ReduceBy);
    auto reduceByList = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
        size_t index = 0;
        for (const auto& reduceByName: outerReduceBy) {
            parent.Callable(index++, "Member")
                .Arg(0, "item")
                .Atom(1, reduceByName)
            .Seal();
        }
        return parent;
    };

    // adds _yql_sys_tablekeyswitch column which is required for outer lambda
    // _yql_sys_tableswitch equals "true" when reduce key is changed
    TExprNode::TPtr keySwitchLambda = ctx.Builder(node.Pos())
        .Lambda()
            .Param("stream")
            .Callable(0, "Fold1Map")
                .Arg(0, "stream")
                .Lambda(1)
                    .Param("item")
                    .List(0)
                        .Callable(0, "AddMember")
                            .Arg(0, "item")
                            .Atom(1, "_yql_sys_tablekeyswitch")
                            .Callable(2, "Bool").Atom(0, "true").Seal()
                        .Seal()
                        .List(1).Do(reduceByList).Seal()
                    .Seal()
                .Seal()
                .Lambda(2)
                    .Param("item")
                    .Param("state")
                    .List(0)
                        .Callable(0, "AddMember")
                            .Arg(0, "item")
                            .Atom(1, "_yql_sys_tablekeyswitch")
                            .Callable(2, "If")
                                .Callable(0, "AggrEquals")
                                    .List(0).Do(reduceByList).Seal()
                                    .Arg(1, "state")
                                .Seal()
                                .Callable(1, "Bool").Atom(0, "false").Seal()
                                .Callable(2, "Bool").Atom(0, "true").Seal()
                            .Seal()
                        .Seal()
                        .List(1).Do(reduceByList).Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
    .Build();

    auto newSettings = innerReduce.Settings().Ptr();
    if (NYql::HasSetting(outerReduce.Settings().Ref(), EYtSettingType::NoDq) &&
       !NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::NoDq)) {
        newSettings = NYql::AddSetting(*newSettings, EYtSettingType::NoDq, {}, ctx);
    }

    if (NYql::HasSetting(outerReduce.Settings().Ref(), EYtSettingType::KeepSorted) &&
       !NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::KeepSorted)) {
        newSettings = NYql::AddSetting(*newSettings, EYtSettingType::KeepSorted, {}, ctx);
    }

    return Build<TYtReduce>(ctx, node.Pos())
        .InitFrom(outerReduce)
        .World<TCoSync>()
            .Add(innerReduce.World())
            .Add(outerReduce.World())
        .Build()
        .Input(innerReduce.Input())
        .Reducer()
            .Args({"stream"})
            .Body<TExprApplier>()
                .Apply(outerLambda)
                .With<TExprApplier>(0)
                    .Apply(TCoLambda(keySwitchLambda))
                    .With<TExprApplier>(0)
                        .Apply(innerLambda)
                        .With(0, "stream")
                    .Build()
                .Build()
                .With(TExprBase(placeHolder), "stream")
            .Build()
        .Build()
        .Settings(newSettings)
        .Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::FuseInnerMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto outerMap = node.Cast<TYtMap>();
    if (outerMap.Input().Size() != 1 || outerMap.Input().Item(0).Paths().Size() != 1) {
        return node;
    }

    TYtPath path = outerMap.Input().Item(0).Paths().Item(0);
    auto maybeInnerMap = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtMap>();
    if (!maybeInnerMap) {
        return node;
    }
    TYtMap innerMap = maybeInnerMap.Cast();

    if (innerMap.Ref().StartsExecution() || innerMap.Ref().HasResult()) {
        return node;
    }
    if (innerMap.Output().Size() > 1) {
        return node;
    }
    if (outerMap.DataSink().Cluster().Value() != innerMap.DataSink().Cluster().Value()) {
        return node;
    }
    if (NYql::HasAnySetting(innerMap.Settings().Ref(), EYtSettingType::Limit | EYtSettingType::SortLimitBy | EYtSettingType::JobCount)) {
        return node;
    }
    if (NYql::HasAnySetting(outerMap.Input().Item(0).Settings().Ref(),
        EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::DirectRead | EYtSettingType::Sample | EYtSettingType::SysColumns))
    {
        return node;
    }
    if (NYql::HasSetting(innerMap.Settings().Ref(), EYtSettingType::Flow) != NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Flow)) {
        return node;
    }
    if (NYql::HasAnySetting(outerMap.Settings().Ref(), EYtSettingType::JobCount)) {
        return node;
    }
    if (!path.Ranges().Maybe<TCoVoid>()) {
        return node;
    }

    if (NYql::HasNonEmptyKeyFilter(outerMap.Input().Item(0))) {
        return node;
    }

    const TParentsMap* parentsMap = getParents();
    if (IsOutputUsedMultipleTimes(innerMap.Ref(), *parentsMap)) {
        // Inner map output is used more than once
        return node;
    }
    // Check world dependencies
    auto parentsIt = parentsMap->find(innerMap.Raw());
    YQL_ENSURE(parentsIt != parentsMap->cend());
    for (auto dep: parentsIt->second) {
        if (!TYtOutput::Match(dep)) {
            return node;
        }
    }

    auto innerLambda = innerMap.Mapper();
    auto outerLambda = outerMap.Mapper();
    if (HasYtRowNumber(outerLambda.Body().Ref())) {
        return node;
    }

    auto fuseRes = CanFuseLambdas(innerLambda, outerLambda, ctx);
    if (!fuseRes) {
        // Some error
        return {};
    }
    if (!*fuseRes) {
        // Cannot fuse
        return node;
    }

    const bool unorderedOut = IsUnorderedOutput(path.Table().Cast<TYtOutput>());

    auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerLambda.Ptr(), ctx, State_->Types);
    if (!placeHolder) {
        return {};
    }

    if (lambdaWithPlaceholder != outerLambda.Ptr()) {
        outerLambda = TCoLambda(lambdaWithPlaceholder);
    }

    innerLambda = FallbackLambdaOutput(innerLambda, ctx);
    if (unorderedOut) {
        innerLambda = Build<TCoLambda>(ctx, innerLambda.Pos())
            .Args({"stream"})
            .Body<TCoUnordered>()
                .Input<TExprApplier>()
                    .Apply(innerLambda)
                    .With(0, "stream")
                .Build()
            .Build()
            .Done();
    }
    outerLambda = FallbackLambdaInput(outerLambda, ctx);

    if (!path.Columns().Maybe<TCoVoid>()) {
        const bool ordered = !unorderedOut && NYql::HasSetting(innerMap.Settings().Ref(), EYtSettingType::Ordered)
            && NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Ordered);
        outerLambda = MapEmbedInputFieldsFilter(outerLambda, ordered, path.Columns().Cast<TCoAtomList>(), ctx);
    } else if (TYqlRowSpecInfo(innerMap.Output().Item(0).RowSpec()).HasAuxColumns()) {
        auto itemType = GetSequenceItemType(path, false, ctx);
        if (!itemType) {
            return {};
        }
        TSet<TStringBuf> fields;
        for (auto item: itemType->Cast<TStructExprType>()->GetItems()) {
            fields.insert(item->GetName());
        }
        const bool ordered = !unorderedOut && NYql::HasSetting(innerMap.Settings().Ref(), EYtSettingType::Ordered)
            && NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Ordered);
        outerLambda = MapEmbedInputFieldsFilter(outerLambda, ordered, TCoAtomList(ToAtomList(fields, node.Pos(), ctx)), ctx);
    }

    const auto mergedSettings = MergeSettings(
        *NYql::RemoveSettings(outerMap.Settings().Ref(), EYtSettingType::Flow, ctx),
        *NYql::RemoveSettings(innerMap.Settings().Ref(), EYtSettingType::Ordered | EYtSettingType::KeepSorted, ctx), ctx);

    return Build<TYtMap>(ctx, node.Pos())
        .InitFrom(outerMap)
        .World<TCoSync>()
            .Add(innerMap.World())
            .Add(outerMap.World())
        .Build()
        .Input(innerMap.Input())
        .Mapper()
            .Args({"stream"})
            .Body<TExprApplier>()
                .Apply(outerLambda)
                .With<TExprApplier>(0)
                    .Apply(innerLambda)
                    .With(0, "stream")
                .Build()
                .With(TExprBase(placeHolder), "stream")
            .Build()
        .Build()
        .Settings(mergedSettings)
        .Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::FuseOuterMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto outerMap = node.Cast<TYtMap>();
    if (outerMap.Input().Size() != 1 || outerMap.Input().Item(0).Paths().Size() != 1) {
        return node;
    }

    TYtPath path = outerMap.Input().Item(0).Paths().Item(0);
    auto maybeInner = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtWithUserJobsOpBase>();
    if (!maybeInner) {
        return node;
    }
    if (!maybeInner.Maybe<TYtReduce>() && !maybeInner.Maybe<TYtMapReduce>()) {
        return node;
    }
    auto inner = maybeInner.Cast();

    if (inner.Ref().StartsExecution() || inner.Ref().HasResult()) {
        return node;
    }
    if (inner.Output().Size() > 1) {
        return node;
    }
    if (outerMap.DataSink().Cluster().Value() != inner.DataSink().Cluster().Value()) {
        return node;
    }
    if (NYql::HasAnySetting(inner.Settings().Ref(), EYtSettingType::Limit | EYtSettingType::SortLimitBy | EYtSettingType::JobCount)) {
        return node;
    }
    if (NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::JobCount)) {
        return node;
    }
    if (outerMap.Input().Item(0).Settings().Size() != 0) {
        return node;
    }
    if (NYql::HasSetting(inner.Settings().Ref(), EYtSettingType::Flow) != NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Flow)) {
        return node;
    }
    if (!path.Ranges().Maybe<TCoVoid>()) {
        return node;
    }
    if (inner.Maybe<TYtMapReduce>()) {
        for (auto out: outerMap.Output()) {
            if (TYqlRowSpecInfo(out.RowSpec()).IsSorted()) {
                return node;
            }
        }
    }

    const TParentsMap* parentsMap = getParents();
    if (IsOutputUsedMultipleTimes(inner.Ref(), *parentsMap)) {
        // Inner output is used more than once
        return node;
    }
    // Check world dependencies
    auto parentsIt = parentsMap->find(inner.Raw());
    YQL_ENSURE(parentsIt != parentsMap->cend());
    for (auto dep: parentsIt->second) {
        if (!TYtOutput::Match(dep)) {
            return node;
        }
    }

    auto outerLambda = outerMap.Mapper();
    if (HasYtRowNumber(outerLambda.Body().Ref())) {
        return node;
    }

    auto lambda = inner.Maybe<TYtMapReduce>() ? inner.Cast<TYtMapReduce>().Reducer() : inner.Cast<TYtReduce>().Reducer();

    auto fuseRes = CanFuseLambdas(lambda, outerLambda, ctx);
    if (!fuseRes) {
        // Some error
        return {};
    }
    if (!*fuseRes) {
        // Cannot fuse
        return node;
    }

    auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerLambda.Ptr(), ctx, State_->Types);
    if (!placeHolder) {
        return {};
    }

    if (lambdaWithPlaceholder != outerLambda.Ptr()) {
        outerLambda = TCoLambda(lambdaWithPlaceholder);
    }

    lambda = FallbackLambdaOutput(lambda, ctx);
    outerLambda = FallbackLambdaInput(outerLambda, ctx);

    if (!path.Columns().Maybe<TCoVoid>()) {
        const bool ordered = inner.Maybe<TYtReduce>() && TYqlRowSpecInfo(inner.Output().Item(0).RowSpec()).IsSorted()
            && NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Ordered);

        outerLambda = MapEmbedInputFieldsFilter(outerLambda, ordered, path.Columns().Cast<TCoAtomList>(), ctx);
    } else if (inner.Maybe<TYtReduce>() && TYqlRowSpecInfo(inner.Output().Item(0).RowSpec()).HasAuxColumns()) {
        auto itemType = GetSequenceItemType(path, false, ctx);
        if (!itemType) {
            return {};
        }
        TSet<TStringBuf> fields;
        for (auto item: itemType->Cast<TStructExprType>()->GetItems()) {
            fields.insert(item->GetName());
        }
        const bool ordered = NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Ordered);
        outerLambda = MapEmbedInputFieldsFilter(outerLambda, ordered, TCoAtomList(ToAtomList(fields, node.Pos(), ctx)), ctx);
    }

    lambda = Build<TCoLambda>(ctx, lambda.Pos())
        .Args({"stream"})
        .Body<TExprApplier>()
            .Apply(outerLambda)
            .With<TExprApplier>(0)
                .Apply(lambda)
                .With(0, "stream")
            .Build()
            .With(TExprBase(placeHolder), "stream")
        .Build()
        .Done();

    auto res = ctx.ChangeChild(inner.Ref(),
        inner.Maybe<TYtMapReduce>() ? TYtMapReduce::idx_Reducer : TYtReduce::idx_Reducer,
        lambda.Ptr());
    res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_Output, outerMap.Output().Ptr());

    auto mergedSettings = NYql::RemoveSettings(outerMap.Settings().Ref(), EYtSettingType::Ordered | EYtSettingType::Sharded | EYtSettingType::Flow, ctx);
    mergedSettings = MergeSettings(inner.Settings().Ref(), *mergedSettings, ctx);
    res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_Settings, std::move(mergedSettings));
    res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_World,
        Build<TCoSync>(ctx, inner.Pos())
            .Add(inner.World())
            .Add(outerMap.World())
        .Done().Ptr());

    return TExprBase(res);
}

}  // namespace NYql
