#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;
using namespace NPrivate;

// All keyFilter settings are combined by OR.
// keyFilter value := '(<memberItem>+) <optional tableIndex>
// <memberItem> := '(<memberName> '(<cmpItem>+))
// <cmpItem> := '(<cmpOp> <value>)
TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ExtractKeyRangeLegacy(TExprBase node, TExprContext& ctx) const {
    auto op = node.Cast<TYtTransientOpBase>();
    if (op.Input().Size() > 1) {
        // Extract key ranges before horizontal joins
        return node;
    }
    if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoVoid>()) {
        return node;
    }

    auto section = op.Input().Item(0);
    if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip)) {
        return node;
    }

    TYtSortMembersCollection sortMembers;
    for (size_t tableIndex = 0; tableIndex < section.Paths().Size(); ++tableIndex) {
        TYtPathInfo pathInfo(section.Paths().Item(tableIndex));
        if (pathInfo.Ranges) {
            return node;
        }
        TYtTableBaseInfo::TPtr tableInfo = pathInfo.Table;
        if (tableInfo->RowSpec && tableInfo->RowSpec->IsSorted() && !tableInfo->RowSpec->SortMembers.empty()) {
            sortMembers.AddTableInfo(tableIndex, tableInfo->Name,
                tableInfo->RowSpec->SortMembers,
                tableInfo->RowSpec->SortedByTypes,
                tableInfo->RowSpec->SortDirections);
        }
    }
    if (sortMembers.Empty()) {
        return node;
    }

    TCoLambda mapper = op.Maybe<TYtMap>() ? op.Cast<TYtMap>().Mapper() : op.Cast<TYtMapReduce>().Mapper().Cast<TCoLambda>();
    auto maybeLambda = GetFlatMapOverInputStream(mapper).Lambda();
    if (!maybeLambda) {
        return node;
    }
    TCoLambda lambda = maybeLambda.Cast();
    if (auto innerFlatMap = lambda.Body().Maybe<TCoFlatMapBase>()) {
        if (auto arg = innerFlatMap.Input().Maybe<TCoFilterNullMembersBase>().Input().Maybe<TCoJust>().Input()) {
            if (arg.Cast().Raw() == lambda.Args().Arg(0).Raw()) {
                lambda = innerFlatMap.Lambda().Cast();
            }
        }
    }
    if (!lambda.Body().Maybe<TCoConditionalValueBase>()) {
        return node;
    }
    auto predicate = lambda.Body().Cast<TCoConditionalValueBase>().Predicate();
    if (predicate.Ref().Type() != TExprNode::Callable) {
        return node;
    }

    const size_t maxTables = State_->Configuration->MaxInputTables.Get().GetOrElse(DEFAULT_MAX_INPUT_TABLES);
    TVector<TKeyFilterPredicates> ranges;
    if (!CollectKeyPredicates(lambda.Args().Arg(0), predicate, ranges, maxTables)) {
        return node;
    }

    if (ranges.size() > maxTables) {
        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__ << ": too many tables - " << ranges.size();
        return node;
    }

    if (!sortMembers.ApplyRanges(ranges, ctx)) {
        return {};
    }

    if (sortMembers.Empty()) {
        return node;
    }

    auto newSettingsChildren = section.Settings().Ref().ChildrenList();
    sortMembers.BuildKeyFilters(predicate.Pos(), section.Paths().Size(), ranges.size(), newSettingsChildren, ctx);

    auto newSection = ctx.ChangeChild(section.Ref(), TYtSection::idx_Settings,
        ctx.NewList(section.Settings().Pos(), std::move(newSettingsChildren)));
    return ctx.ChangeChild(op.Ref(), TYtTransientOpBase::idx_Input,
        Build<TYtSectionList>(ctx, op.Input().Pos())
            .Add(newSection)
            .Done().Ptr()
        );
}

TMaybe<TVector<TYtPhysicalOptProposalTransformer::TRangeBuildResult>>
TYtPhysicalOptProposalTransformer::ExtractKeyRangeFromLambda(TCoLambda lambda, TYtSection section, TExprContext& ctx) const {
    YQL_ENSURE(lambda.Body().Maybe<TCoConditionalValueBase>().IsValid());

    TMap<TVector<TString>, TSet<size_t>> tableIndexesBySortKey;
    TMap<size_t, TString> tableNamesByIndex;
    for (size_t tableIndex = 0; tableIndex < section.Paths().Size(); ++tableIndex) {
        TYtPathInfo pathInfo(section.Paths().Item(tableIndex));
        if (pathInfo.Ranges) {
            return TVector<TRangeBuildResult>{};
        }
        TYtTableBaseInfo::TPtr tableInfo = pathInfo.Table;
        if (tableInfo->RowSpec) {
            auto rowSpec = tableInfo->RowSpec;
            if (rowSpec->IsSorted()) {
                TVector<TString> keyPrefix;
                for (size_t i = 0; i < rowSpec->SortedBy.size(); ++i) {
                    if (!rowSpec->GetType()->FindItem(rowSpec->SortedBy[i])) {
                        break;
                    }
                    YQL_ENSURE(i < rowSpec->SortDirections.size());

                    if (!rowSpec->SortDirections[i]) {
                        // TODO: allow native descending YT sort if UseYtKeyBounds is enabled
                        break;
                    }
                    keyPrefix.push_back(rowSpec->SortedBy[i]);
                }
                if (!keyPrefix.empty()) {
                    tableIndexesBySortKey[keyPrefix].insert(tableIndex);
                }
                tableNamesByIndex[tableIndex] = tableInfo->Name;
            }
        }
    }
    if (tableIndexesBySortKey.empty()) {
        return TVector<TRangeBuildResult>{};
    }

    TPredicateExtractorSettings rangeSettings;
    rangeSettings.MergeAdjacentPointRanges = State_->Configuration->MergeAdjacentPointRanges.Get().GetOrElse(DEFAULT_MERGE_ADJACENT_POINT_RANGES);
    rangeSettings.HaveNextValueCallable = State_->Configuration->KeyFilterForStartsWith.Get().GetOrElse(DEFAULT_KEY_FILTER_FOR_STARTS_WITH);
    rangeSettings.MaxRanges = State_->Configuration->MaxKeyRangeCount.Get().GetOrElse(DEFAULT_MAX_KEY_RANGE_COUNT);

    THashSet<TString> possibleIndexKeys;
    auto rowType = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
    auto extractor = MakePredicateRangeExtractor(rangeSettings);
    if (!extractor->Prepare(lambda.Ptr(), *rowType, possibleIndexKeys, ctx, *State_->Types)) {
        return Nothing();
    }

    TVector<TRangeBuildResult> results;
    for (auto& [keys, tableIndexes] : tableIndexesBySortKey) {
        if (AllOf(keys, [&possibleIndexKeys](const TString& key) { return !possibleIndexKeys.contains(key); })) {
            continue;
        }

        TRangeBuildResult result;
        result.Keys = keys;
        result.TableIndexes = tableIndexes;
        result.BuildResult = extractor->BuildComputeNode(keys, ctx, *State_->Types);
        auto& compute = result.BuildResult.ComputeNode;
        if (compute) {
            compute = ctx.NewCallable(compute->Pos(), "EvaluateExprIfPure", { compute });
            results.push_back(result);

            TVector<TString> tableNames;
            for (const auto& idx : tableIndexes) {
                YQL_ENSURE(tableNamesByIndex.contains(idx));
                tableNames.push_back(tableNamesByIndex[idx]);
            }

            YQL_CLOG(INFO, ProviderYt) << __FUNCTION__
                << ": Will use key filter for tables [" << JoinSeq(",", tableNames) << "] with key columns ["
                << JoinSeq(",", keys) << "]";
        }
    }

    return results;
}

TExprNode::TPtr TYtPhysicalOptProposalTransformer::UpdateSectionWithKeyRanges(TPositionHandle pos, TYtSection section, const TVector<TRangeBuildResult>& results, TExprContext& ctx) const {
    const bool sameSort = results.size() == 1 && results.front().TableIndexes.size() == section.Paths().Size();

    auto newSettingsChildren = section.Settings().Ref().ChildrenList();

    TExprNode::TListType updatedPaths = section.Paths().Ref().ChildrenList();
    bool hasPathUpdates = false;
    for (auto& result : results) {
        TExprNodeList items = { result.BuildResult.ComputeNode };

        TExprNodeList usedKeys;
        YQL_ENSURE(result.BuildResult.UsedPrefixLen <= result.Keys.size());
        for (size_t i = 0; i < result.BuildResult.UsedPrefixLen; ++i) {
            usedKeys.push_back(ctx.NewAtom(pos, result.Keys[i]));
        }
        auto usedKeysNode = ctx.NewList(pos, std::move(usedKeys));
        auto usedKeysSetting = ctx.NewList(pos, { ctx.NewAtom(pos, "usedKeys"), usedKeysNode });
        items.push_back(ctx.NewList(pos, { usedKeysSetting }));

        for (const auto& idx : result.TableIndexes) {
            if (auto out = TYtPath(updatedPaths[idx]).Table().Maybe<TYtOutput>(); out && IsUnorderedOutput(out.Cast())) {
                updatedPaths[idx] = Build<TYtPath>(ctx, updatedPaths[idx]->Pos())
                    .InitFrom(TYtPath(updatedPaths[idx]))
                    .Table<TYtOutput>()
                        .InitFrom(out.Cast())
                        .Mode(TMaybeNode<TCoAtom>())
                    .Build()
                    .Done().Ptr();
                hasPathUpdates = true;
            }
        }

        if (!sameSort) {
            TExprNodeList idxs;
            for (const auto& idx : result.TableIndexes) {
                idxs.push_back(ctx.NewAtom(pos, idx));
            }
            items.push_back(ctx.NewList(pos, std::move(idxs)));
        }
        newSettingsChildren.push_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name()
                    .Value(ToString(EYtSettingType::KeyFilter2))
                .Build()
                .Value(ctx.NewList(pos, std::move(items)))
                .Done().Ptr());
    }

    auto newSection = ctx.ChangeChild(section.Ref(), TYtSection::idx_Settings,
        ctx.NewList(section.Settings().Pos(), std::move(newSettingsChildren)));
    if (hasPathUpdates) {
        newSection = ctx.ChangeChild(*newSection, TYtSection::idx_Paths,
            ctx.NewList(section.Paths().Pos(), std::move(updatedPaths)));
    }
    return newSection;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ExtractKeyRangeDqReadWrap(TExprBase node, TExprContext& ctx) const {
    auto flatMap = node.Cast<TCoFlatMapBase>();
    auto maybeYtRead = flatMap.Input().Maybe<TDqReadWrapBase>().Input().Maybe<TYtReadTable>();
    if (!maybeYtRead) {
        return node;
    }
    auto ytRead = maybeYtRead.Cast();
    if (ytRead.Input().Size() > 1) {
        return node;
    }

    TYtDSource dataSource = GetDataSource(ytRead, ctx);
    if (!State_->Configuration->_EnableYtPartitioning.Get(dataSource.Cluster().StringValue()).GetOrElse(false)) {
        return node;
    }

    auto section = ytRead.Input().Item(0);
    if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip)) {
        return node;
    }

    auto maybeLambda = GetLambdaWithPredicate(flatMap.Lambda());
    if (!maybeLambda) {
        return node;
    }
    auto lambda = maybeLambda.Cast();
    auto maybeResult = ExtractKeyRangeFromLambda(lambda, section, ctx);
    if (!maybeResult) {
        return {};
    }
    auto results = *maybeResult;
    if (results.empty()) {
        return node;
    }
    auto predPos = lambda.Body().Cast<TCoConditionalValueBase>().Predicate().Pos();
    auto newSection = UpdateSectionWithKeyRanges(predPos, section, results, ctx);

    auto newYtRead = Build<TYtReadTable>(ctx, ytRead.Pos())
        .InitFrom(ytRead)
        .Input()
            .Add(newSection)
        .Build()
        .Done().Ptr();

    auto newFlatMap = ctx.ChangeChild(flatMap.Ref(), TCoFlatMapBase::idx_Input,
        ctx.ChangeChild(flatMap.Input().Ref(), TDqReadWrapBase::idx_Input, std::move(newYtRead))
    );

    const bool sameSort = results.size() == 1 && results.front().TableIndexes.size() == section.Paths().Size();
    const bool pruneLambda = State_->Configuration->DqPruneKeyFilterLambda.Get().GetOrElse(DEFAULT_DQ_PRUNE_KEY_FILTER_LAMBDA);
    if (sameSort && pruneLambda) {
        YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Will prune key filter lambda";
        newFlatMap = ctx.ReplaceNodes(std::move(newFlatMap), {{ lambda.Raw(), results.front().BuildResult.PrunedLambda }});
    }

    return newFlatMap;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ExtractKeyRange(TExprBase node, TExprContext& ctx) const {
    auto op = node.Cast<TYtTransientOpBase>();
    if (op.Input().Size() > 1) {
        // Extract key ranges before horizontal joins
        return node;
    }
    if (op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoVoid>()) {
        return node;
    }

    auto section = op.Input().Item(0);
    if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip)) {
        return node;
    }

    TCoLambda mapper = op.Maybe<TYtMap>() ? op.Cast<TYtMap>().Mapper() : op.Cast<TYtMapReduce>().Mapper().Cast<TCoLambda>();
    auto maybeLambda = GetFlatMapOverInputStream(mapper).Lambda();
    if (!maybeLambda) {
        return node;
    }
    maybeLambda = GetLambdaWithPredicate(maybeLambda.Cast());
    if (!maybeLambda) {
        return node;
    }
    auto lambda = maybeLambda.Cast();
    auto maybeResult = ExtractKeyRangeFromLambda(lambda, section, ctx);
    if (!maybeResult) {
        return {};
    }
    auto results = *maybeResult;
    if (results.empty()) {
        return node;
    }

    auto predPos = lambda.Body().Cast<TCoConditionalValueBase>().Predicate().Pos();
    auto newSection = UpdateSectionWithKeyRanges(predPos, section, results, ctx);

    auto newOp = ctx.ChangeChild(op.Ref(),
        TYtTransientOpBase::idx_Input,
        Build<TYtSectionList>(ctx, op.Input().Pos())
            .Add(newSection)
        .Done().Ptr()
    );

    const bool sameSort = results.size() == 1 && results.front().TableIndexes.size() == section.Paths().Size();
    const bool pruneLambda = State_->Configuration->PruneKeyFilterLambda.Get().GetOrElse(DEFAULT_PRUNE_KEY_FILTER_LAMBDA);
    if (sameSort && pruneLambda) {
        YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Will prune key filter lambda";
        newOp = ctx.ReplaceNodes(std::move(newOp), {{ lambda.Raw(), results.front().BuildResult.PrunedLambda }});
    }

    return newOp;
}

}  // namespace NYql
