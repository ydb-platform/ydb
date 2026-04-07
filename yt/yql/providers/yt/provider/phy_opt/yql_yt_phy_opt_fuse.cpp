#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <yt/yql/providers/yt/provider/yql_yt_optimize.h>
#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>
#include <yt/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_helpers.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql {

using namespace NNodes;
using namespace NPrivate;

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
    if (!path.QLFilter().Maybe<TCoVoid>()) {
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
                                                            EYtSettingType::SortBy |
                                                            EYtSettingType::KeepSorted)) {
        return node;
    }

    const auto outerReduceBy = NYql::GetSettingAsColumnList(outerReduce.Settings().Ref(), EYtSettingType::ReduceBy);
    const auto innerReduceBy = NYql::GetSettingAsColumnList(innerReduce.Settings().Ref(), EYtSettingType::ReduceBy);

    if (outerReduceBy.size() != innerReduceBy.size()) {
        return node;
    }

    auto innerLambda = innerReduce.Reducer();
    auto outerLambda = outerReduce.Reducer();
    auto fuseRes = CanFuseLambdas(innerLambda, outerLambda, ctx, State_);
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

    if (!NYql::HasSetting(outerReduce.Settings().Ref(), EYtSettingType::KeepSorted) &&
       NYql::HasSetting(innerReduce.Settings().Ref(), EYtSettingType::KeepSorted)) {
        newSettings = NYql::RemoveSettings(*newSettings, EYtSettingType::KeepSorted, ctx);
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

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::FuseReduceWithTrivialMap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    const EYtSettingTypes acceptedReduceSettings =
          EYtSettingType::ReduceBy
        | EYtSettingType::Limit
        | EYtSettingType::SortLimitBy
        | EYtSettingType::SortBy
        // | EYtSettingType::JoinReduce
        // | EYtSettingType::FirstAsPrimary
        | EYtSettingType::Flow
        | EYtSettingType::KeepSorted
        | EYtSettingType::KeySwitch
        // | EYtSettingType::ReduceInputType
        | EYtSettingType::NoDq;

    const EYtSettingTypes acceptedMapSettings =
          EYtSettingType::Ordered
        //| EYtSettingType::Limit
        //| EYtSettingType::SortLimitBy
        //| EYtSettingType::WeakFields
        //| EYtSettingType::Sharded
        //| EYtSettingType::JobCount
        | EYtSettingType::Flow
        | EYtSettingType::KeepSorted
        | EYtSettingType::NoDq
        //| EYtSettingType::BlockInputReady
        //| EYtSettingType::BlockInputApplied
    ;

    auto outerReduce = node.Cast<TYtReduce>();
    if (NYql::HasSettingsExcept(outerReduce.Settings().Ref(), acceptedReduceSettings)) {
        return node;
    }

    const bool hasKeySwitch = NYql::HasSetting(outerReduce.Settings().Ref(), EYtSettingType::KeySwitch);
    const bool isFlow = NYql::HasSetting(outerReduce.Settings().Ref(), EYtSettingType::Flow);

    const auto sortBy = NYql::GetSettingAsColumnList(outerReduce.Settings().Ref(), EYtSettingType::SortBy);
    const auto reduceBy = NYql::GetSettingAsColumnList(outerReduce.Settings().Ref(), EYtSettingType::ReduceBy);

    THashSet<TString> sortOrKeyColumns(sortBy.begin(), sortBy.end());
    sortOrKeyColumns.insert(reduceBy.begin(), reduceBy.end());

    struct TFused {
        TYtPath Path;
        TCoLambda MapLambda;
        TCoLambda ReduceLambda;
        TExprBase ReducePlaceholder;
        size_t InputIndex;
        size_t OrigInputIndex;
        TYtMap OrigMap;
    };

    TExprNode::TPtr origVariantType;
    if (outerReduce.Input().Size() > 1) {
        auto itemType = GetSequenceItemType(outerReduce.Reducer().Args().Arg(0), true);
        YQL_ENSURE(itemType);
        origVariantType = ExpandType(outerReduce.Pos(), *itemType->Cast<TVariantExprType>(), ctx);
    }

    TMaybe<TFused> fusedMap;
    TVector<TYtSection> newInput;
    const size_t origReduceInputs = outerReduce.Input().Size();
    for (size_t i = 0; i < origReduceInputs; ++i) {
        const auto& section = outerReduce.Input().Item(i);
        if (fusedMap.Defined() || section.Settings().Size() != 0) {
            newInput.push_back(section);
            continue;
        }

        TVector<TYtPath> newPaths;
        newPaths.reserve(section.Paths().Size());
        for (const auto& path : section.Paths()) {
            if (fusedMap.Defined() || !path.Ranges().Maybe<TCoVoid>()) {
                newPaths.push_back(path);
                continue;
            }

            if (fusedMap.Defined() || !path.QLFilter().Maybe<TCoVoid>()) {
                newPaths.push_back(path);
                continue;
            }

            auto maybeInnerMap = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtMap>();
            if (!maybeInnerMap) {
                newPaths.push_back(path);
                continue;
            }

            TYtMap innerMap = maybeInnerMap.Cast();
            if (innerMap.Ref().StartsExecution() ||
                innerMap.Ref().HasResult() ||
                outerReduce.DataSink().Cluster().Value() != innerMap.DataSink().Cluster().Value() ||
                innerMap.Output().Size() > 1 ||
                innerMap.Input().Size() > 1 ||
                innerMap.Input().Item(0).Paths().Size() > 1 ||
                !NYql::HasSetting(innerMap.Settings().Ref(), EYtSettingType::Ordered) ||
                isFlow != NYql::HasSetting(innerMap.Settings().Ref(), EYtSettingType::Flow) ||
                NYql::HasSettingsExcept(innerMap.Settings().Ref(), acceptedMapSettings))
            {
                newPaths.push_back(path);
                continue;
            }

            const TParentsMap* parents = getParents();
            if (IsOutputUsedMultipleTimes(path.Table().Cast<TYtOutput>().Ref(), *parents)) {
                // Inner map output is used more than once
                newPaths.push_back(path);
                continue;
            }

            // Check world dependencies
            auto parentsIt = parents->find(innerMap.Raw());
            YQL_ENSURE(parentsIt != parents->cend());
            if (!AllOf(parentsIt->second, [](const TExprNode* dep) { return TYtOutput::Match(dep); })) {
                newPaths.push_back(path);
                continue;
            }

            const TCoLambda mapLambda = innerMap.Mapper();
            auto maybeFlatMap = GetFlatMapOverInputStream(mapLambda, *parents);
            TMaybe<THashSet<TStringBuf>> passthrough;
            if (!maybeFlatMap.Maybe<TCoOrderedFlatMap>() ||
                !IsJustOrSingleAsList(maybeFlatMap.Cast().Lambda().Body().Ref()) ||
                !IsPassthroughFlatMap(maybeFlatMap.Cast(), &passthrough) ||
                !passthrough ||
                !AllOf(sortOrKeyColumns, [&](const TString& col) { return passthrough->contains(col); }))
            {
                newPaths.push_back(path);
                continue;
            }

            auto fuseRes = CanFuseLambdas(mapLambda, outerReduce.Reducer(), ctx, State_);
            if (!fuseRes) {
                // Some error
                return {};
            }
            if (!*fuseRes) {
                // Cannot fuse
                newPaths.push_back(path);
                continue;
            }

            auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerReduce.Reducer().Ptr(), ctx, State_->Types);
            if (!placeHolder) {
                return {};
            }

            TYtPath newPath = innerMap.Input().Item(0).Paths().Item(0);
            YQL_ENSURE(newInput.size() == i);
            if (!newPaths.empty()) {
                newInput.push_back(
                    Build<TYtSection>(ctx, section.Pos())
                        .InitFrom(section)
                        .Paths()
                            .Add(newPaths)
                        .Build()
                        .Done());
                newPaths.clear();
            }
            size_t inputIndex = newInput.size();
            newInput.push_back(
                Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Paths()
                        .Add(newPath)
                    .Build()
                    .Done());
            fusedMap = {
                .Path = newPath,
                .MapLambda = mapLambda,
                .ReduceLambda = TCoLambda(lambdaWithPlaceholder),
                .ReducePlaceholder = TExprBase(placeHolder),
                .InputIndex = inputIndex,
                .OrigInputIndex = i,
                .OrigMap = innerMap,
            };
        }
        if (!newPaths.empty()) {
            newInput.push_back(
                Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Paths()
                        .Add(newPaths)
                    .Build()
                    .Done());
        }
    }

    if (!fusedMap) {
        return node;
    }

    YQL_ENSURE(newInput.size() >= origReduceInputs);
    // one section can be rewritten into 3:
    // (ABA) -> (A), (C), (A)
    YQL_ENSURE(newInput.size() - origReduceInputs <= 2);

    TExprNode::TPtr remapLambda = ctx.Builder(fusedMap->MapLambda.Pos())
        .Lambda()
            .Param("item")
            .Apply(fusedMap->MapLambda.Ptr())
                .With(0)
                    .Callable("AsList")
                        .Arg(0, "item")
                    .Seal()
                .Done()
            .Seal()
        .Seal()
        .Build();
    if (hasKeySwitch) {
        remapLambda = ctx.Builder(fusedMap->MapLambda.Pos())
            .Lambda()
                .Param("item")
                .Callable(0, "OrderedMap")
                    .Apply(0, remapLambda)
                        .With(0)
                            .Callable("RemoveMember")
                                .Arg(0, "item")
                                .Atom(1, "_yql_sys_tablekeyswitch")
                            .Seal()
                        .Done()
                    .Seal()
                    .Lambda(1)
                        .Param("remappedItem")
                        .Callable(0, "AddMember")
                            .Arg(0, "remappedItem")
                            .Atom(1, "_yql_sys_tablekeyswitch")
                            .Callable(2, "Member")
                                .Arg(0, "item")
                                .Atom(1, "_yql_sys_tablekeyswitch")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    TExprNode::TPtr flatMapLambda;
    if (newInput.size() == 1) {
        flatMapLambda = remapLambda;
    } else {
        flatMapLambda = ctx.Builder(outerReduce.Pos())
            .Lambda()
                .Param("item")
                .Callable("Visit")
                    .Arg(0, "item")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        for (size_t i = 0; i < newInput.size(); ++i) {
                            TString paramName = TStringBuilder() << "alt" << i;
                            TString remappedName = TStringBuilder() << "remapped" << i;
                            if (i != fusedMap->InputIndex) {
                                size_t origInputIndex = i;
                                if (i > fusedMap->InputIndex) {
                                    size_t delta = newInput.size() - origReduceInputs;
                                    YQL_ENSURE(i >= delta);
                                    origInputIndex = i - delta;
                                }
                                parent
                                    .Atom(2 * i + 1, i)
                                    .Lambda(2 * i + 2)
                                        .Param(paramName)
                                        .Callable("AsList")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                if (origVariantType) {
                                                    parent
                                                        .Callable(0, "Variant")
                                                            .Arg(0, paramName)
                                                            .Atom(1, origInputIndex)
                                                            .Add(2, origVariantType)
                                                        .Seal();
                                                } else {
                                                    parent
                                                        .Arg(0, paramName);
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Seal();
                            } else {
                                parent
                                    .Atom(2 * i + 1, i)
                                    .Lambda(2 * i + 2)
                                        .Param(paramName)
                                        .Callable("OrderedMap")
                                            .Apply(0, remapLambda)
                                                .With(0, paramName)
                                            .Seal()
                                            .Lambda(1)
                                                .Param(remappedName)
                                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                    if (origVariantType) {
                                                        parent
                                                            .Callable("Variant")
                                                                .Arg(0, remappedName)
                                                                .Atom(1, fusedMap->OrigInputIndex)
                                                                .Add(2, origVariantType)
                                                            .Seal();
                                                    } else {
                                                        parent
                                                            .Arg(remappedName);
                                                    }
                                                    return parent;
                                                })
                                            .Seal()
                                        .Seal()
                                    .Seal();
                            }
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build();
    }

    TExprNode::TPtr newReduceLambda = ctx.Builder(outerReduce.Pos())
        .Lambda()
            .Param("inputStream")
            .Apply(0, fusedMap->ReduceLambda.Ptr())
                .With(0)
                    .Callable("OrderedFlatMap")
                        .Arg(0, "inputStream")
                        .Add(1, flatMapLambda)
                    .Seal()
                .Done()
                .WithNode(fusedMap->ReducePlaceholder.Ref(), "inputStream")
            .Seal()
        .Seal()
        .Build();

    auto newSettings = outerReduce.Settings().Ptr();
    if (!NYql::HasSetting(outerReduce.Settings().Ref(), EYtSettingType::NoDq) &&
        NYql::HasSetting(fusedMap->OrigMap.Settings().Ref(), EYtSettingType::NoDq))
    {
        newSettings = NYql::AddSetting(*newSettings, EYtSettingType::NoDq, {}, ctx);
    }

    return Build<TYtReduce>(ctx, node.Pos())
        .InitFrom(outerReduce)
        .World<TCoSync>()
            .Add(fusedMap->OrigMap.World())
            .Add(outerReduce.World())
        .Build()
        .Input()
            .Add(newInput)
        .Build()
        .Reducer(newReduceLambda)
        .Settings(newSettings)
        .Done();

    return node;
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
        EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::DirectRead | EYtSettingType::Sample | EYtSettingType::SysColumns | EYtSettingType::BlockInputApplied | EYtSettingType::BlockOutputApplied))
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
    if (!path.QLFilter().Maybe<TCoVoid>()) {
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

    auto fuseRes = CanFuseLambdas(innerLambda, outerLambda, ctx, State_);
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
        *NYql::RemoveSettings(outerMap.Settings().Ref(), EYtSettingType::Flow | EYtSettingType::BlockInputReady | EYtSettingType::BlockOutputReady, ctx),
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
    if (NYql::HasAnySetting(outerMap.Settings().Ref(), EYtSettingType::JobCount | EYtSettingType::BlockInputApplied | EYtSettingType::BlockOutputApplied)) {
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
    if (!path.QLFilter().Maybe<TCoVoid>()) {
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

    auto fuseRes = CanFuseLambdas(lambda, outerLambda, ctx, State_);
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

    EYtSettingTypes toRemove = EYtSettingType::Ordered | EYtSettingType::Sharded | EYtSettingType::Flow | EYtSettingType::BlockInputReady | EYtSettingType::BlockOutputReady;
    if (inner.Maybe<TYtMapReduce>()) {
        // Can be safely removed, because outer map has no sorted outputs (checked below)
        toRemove |= EYtSettingType::KeepSorted;
    }
    auto mergedSettings = NYql::RemoveSettings(outerMap.Settings().Ref(), toRemove, ctx);
    mergedSettings = MergeSettings(inner.Settings().Ref(), *mergedSettings, ctx);
    res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_Settings, std::move(mergedSettings));
    res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_World,
        Build<TCoSync>(ctx, inner.Pos())
            .Add(inner.World())
            .Add(outerMap.World())
        .Done().Ptr());

    return TExprBase(res);
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::FuseMapToMapReduce(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    return NYql::FuseMapToMapReduce(node, ctx, getParents, State_);
}

}  // namespace NYql
