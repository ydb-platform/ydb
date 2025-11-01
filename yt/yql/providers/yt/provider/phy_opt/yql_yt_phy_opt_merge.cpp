#include "yql_yt_phy_opt.h"

#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>

namespace NYql {

using namespace NNodes;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::BypassMerge(TExprBase node, TExprContext& ctx) const {
    if (node.Ref().HasResult()) {
        return node;
    }

    auto op = node.Cast<TYtTransientOpBase>();
    if (op.Maybe<TYtCopy>()) {
        return node;
    }

    if (op.Maybe<TYtWithUserJobsOpBase>()) {
        size_t lambdaIdx = op.Maybe<TYtMapReduce>()
            ? TYtMapReduce::idx_Mapper
            : op.Maybe<TYtReduce>()
                ? TYtReduce::idx_Reducer
                : TYtMap::idx_Mapper;

        bool usesTableIndex = false;
        VisitExpr(op.Ref().ChildPtr(lambdaIdx), [&usesTableIndex](const TExprNode::TPtr& n) {
            if (TYtTableIndex::Match(n.Get())) {
                usesTableIndex = true;
            } else if (TYtOutput::Match(n.Get())) {
                return false;
            }
            return !usesTableIndex;
        });
        if (usesTableIndex) {
            return node;
        }
    }

    auto maxTables = State_->Configuration->MaxInputTables.Get();
    auto maxSortedTables = State_->Configuration->MaxInputTablesForSortedMerge.Get();
    const bool opOrdered = NYql::HasSetting(op.Settings().Ref(), EYtSettingType::Ordered);
    bool hasUpdates = false;
    TVector<TExprBase> updatedSections;
    TSyncMap syncList;
    for (auto section: op.Input()) {
        const EYtSettingType kfType = NYql::HasSetting(section.Settings().Ref(), EYtSettingType::KeyFilter2) ?
            EYtSettingType::KeyFilter2 : EYtSettingType::KeyFilter;
        const auto keyFiltersValues = NYql::GetAllSettingValues(section.Settings().Ref(), kfType);
        const bool hasTableKeyFilters = AnyOf(keyFiltersValues,
            [kfType](const TExprNode::TPtr& keyFilter) {
                return keyFilter->ChildrenSize() >= GetMinChildrenForIndexedKeyFilter(kfType);
            });
        const bool hasTakeSkip = NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip);

        bool hasPathUpdates = false;
        TVector<TYtPath> updatedPaths;
        size_t inputCount = section.Paths().Size();
        if (!hasTableKeyFilters) {
            for (auto path: section.Paths()) {
                updatedPaths.push_back(path);

                bool hasRanges = false;
                if (!path.Ranges().Maybe<TCoVoid>()) {
                    bool pathLimits = false;
                    for (auto range: path.Ranges().Cast<TExprList>()) {
                        if (range.Maybe<TYtRow>() || range.Maybe<TYtRowRange>()) {
                            pathLimits = true;
                            break;
                        }
                        if (range.Maybe<TYtRangeItemBase>()) {
                            hasRanges = true;
                        }
                    }
                    if (pathLimits) {
                        continue;
                    }
                }
                auto maybeInnerMerge = path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtMerge>();
                if (!maybeInnerMerge) {
                    continue;
                }
                auto innerMerge = maybeInnerMerge.Cast();

                if (innerMerge.Ref().StartsExecution() || innerMerge.Ref().HasResult()) {
                    continue;
                }

                if (innerMerge.DataSink().Cluster().Value() != op.DataSink().Cluster().Value()) {
                    continue;
                }

                if (NYql::HasSettingsExcept(innerMerge.Settings().Ref(), EYtSettingType::KeepSorted | EYtSettingType::Limit)) {
                    continue;
                }

                if (auto limitSetting = NYql::GetSetting(innerMerge.Settings().Ref(), EYtSettingType::Limit)) {
                    if (limitSetting->ChildPtr(1)->ChildrenSize()) {
                        continue;
                    }
                }

                auto innerMergeSection = innerMerge.Input().Item(0);

                bool hasIncompatibleSettings = false;
                TExprNode::TListType innerMergeKeyFiltersValues;
                for (auto& setting : innerMergeSection.Settings().Ref().Children()) {
                    const auto type = FromString<EYtSettingType>(setting->Child(0)->Content());
                    if (setting->ChildrenSize() == 2 && (type == EYtSettingType::KeyFilter || type == EYtSettingType::KeyFilter2)) {
                        innerMergeKeyFiltersValues.push_back(setting->ChildPtr(1));
                    } else {
                        hasIncompatibleSettings = true;
                        break;
                    }
                }

                if (hasIncompatibleSettings) {
                    continue;
                }
                if (AnyOf(innerMergeKeyFiltersValues, [](const TExprNode::TPtr& keyFilter) { return keyFilter->ChildrenSize() > 0; })) {
                    continue;
                }

                auto mergeOutRowSpec = TYqlRowSpecInfo(innerMerge.Output().Item(0).RowSpec());
                const bool sortedMerge = mergeOutRowSpec.IsSorted();
                if (hasTakeSkip && sortedMerge && NYql::HasSetting(innerMerge.Settings().Ref(), EYtSettingType::KeepSorted)) {
                    continue;
                }
                if ((hasTakeSkip || hasRanges) && AnyOf(innerMergeSection.Paths(), [](const auto& path) { return !path.Ranges().template Maybe<TCoVoid>(); })) {
                    continue;
                }

                const auto convertDynamicTablesToStatic = State_->Configuration->ConvertDynamicTablesToStatic.Get().GetOrElse(EConvertDynamicTablesToStatic::Disable);
                const auto keepMergeWithDynamicInput = State_->Configuration->KeepMergeWithDynamicInput.Get().GetOrElse(false);
                if (keepMergeWithDynamicInput || convertDynamicTablesToStatic != EConvertDynamicTablesToStatic::Disable) {
                    if (AnyOf(innerMergeSection.Paths(), [](TYtPath path) {
                        return TYtTableBaseInfo::GetMeta(path.Table())->IsDynamic;
                    })) {
                        continue;
                    }
                }

                const bool unordered = IsUnorderedOutput(path.Table().Cast<TYtOutput>());
                if (innerMergeSection.Paths().Size() > 1) {
                    if (hasTakeSkip && sortedMerge) {
                        continue;
                    }
                    // Only YtMap can change semantic if substitute single sorted input by multiple sorted ones.
                    // Other operations (YtMerge, YtReduce, YtMapReduce, YtEquiJoin, YtSort) can be safely optimized.
                    // YtCopy cannot, but it is ignored early
                    if (op.Maybe<TYtMap>() && opOrdered && !unordered && sortedMerge) {
                        continue;
                    }
                    auto limit = maxTables;
                    if (maxSortedTables && (op.Maybe<TYtReduce>() || (op.Maybe<TYtMerge>() && TYqlRowSpecInfo(op.Output().Item(0).RowSpec()).IsSorted()))) {
                        limit = maxSortedTables;
                    }
                    if (limit && (inputCount - 1 + innerMergeSection.Paths().Size()) > *limit) {
                        continue;
                    }

                    if (mergeOutRowSpec.GetAllConstraints(ctx).GetConstraint<TDistinctConstraintNode>() || mergeOutRowSpec.GetAllConstraints(ctx).GetConstraint<TUniqueConstraintNode>()) {
                        continue;
                    }
                }

                hasPathUpdates = true;
                updatedPaths.pop_back();
                TMaybeNode<TExprBase> columns;
                if (!path.Columns().Maybe<TCoVoid>()) {
                    columns = path.Columns();
                } else if ((op.Maybe<TYtWithUserJobsOpBase>() || op.Maybe<TYtEquiJoin>()) && mergeOutRowSpec.HasAuxColumns()) {
                    TVector<TStringBuf> items;
                    for (auto item: mergeOutRowSpec.GetType()->GetItems()) {
                        items.push_back(item->GetName());
                    }
                    columns = ToAtomList(items, op.Pos(), ctx);
                }

                if (!columns.IsValid() && path.Ranges().Maybe<TCoVoid>() && !unordered) {
                    for (auto mergePath: innerMergeSection.Paths()) {
                        updatedPaths.push_back(mergePath);
                    }
                } else {
                    for (auto mergePath: innerMergeSection.Paths()) {
                        auto builder = Build<TYtPath>(ctx, mergePath.Pos()).InitFrom(mergePath);

                        if (columns) {
                            builder.Columns(columns.Cast());
                        }
                        if (!path.Ranges().Maybe<TCoVoid>()) {
                            builder.Ranges(path.Ranges());
                        }
                        if (!path.QLFilter().Maybe<TCoVoid>()) {
                            builder.QLFilter(path.QLFilter());
                        }

                        auto updatedPath = builder.Done();
                        if (unordered) {
                            updatedPath = MakeUnorderedPath(updatedPath, false, ctx);
                        }

                        updatedPaths.push_back(updatedPath);
                    }
                }

                if (innerMerge.World().Ref().Type() != TExprNode::World) {
                    syncList.emplace(innerMerge.World().Ptr(), syncList.size());
                }
                inputCount += innerMergeSection.Paths().Size() - 1;
            }
        }
        if (hasPathUpdates) {
            hasUpdates = true;
            updatedSections.push_back(
                Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Paths()
                        .Add(updatedPaths)
                    .Build()
                    .Done());
        } else {
            updatedSections.push_back(section);
        }
    }
    if (!hasUpdates) {
        return node;
    }

    auto sectionList = Build<TYtSectionList>(ctx, op.Input().Pos())
        .Add(updatedSections)
        .Done();

    auto res = ctx.ChangeChild(node.Ref(), TYtTransientOpBase::idx_Input, sectionList.Ptr());
    if (!syncList.empty()) {
        res = ctx.ChangeChild(*res, TYtTransientOpBase::idx_World, ApplySyncListToWorld(op.World().Ptr(), syncList, ctx));
    }
    return TExprBase(res);
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::BypassMergeBeforePublish(TExprBase node, TExprContext& ctx) const {
    if (node.Ref().HasResult()) {
        return node;
    }

    auto publish = node.Cast<TYtPublish>();

    auto cluster = publish.DataSink().Cluster().StringValue();
    YQL_ENSURE(cluster != YtUnspecifiedCluster);
    auto path = publish.Publish().Name().StringValue();
    auto commitEpoch = TEpochInfo::Parse(publish.Publish().CommitEpoch().Ref()).GetOrElse(0);

    auto dstRowSpec = State_->TablesData->GetTable(cluster, path, commitEpoch).RowSpec;

    auto maxTables = dstRowSpec->IsSorted() ? State_->Configuration->MaxInputTablesForSortedMerge.Get() : State_->Configuration->MaxInputTables.Get();
    bool hasUpdates = false;
    TVector<TYtOutput> updateInputs;
    size_t inputCount = publish.Input().Size();
    for (auto out: publish.Input()) {
        updateInputs.push_back(out);
        if (auto maybeMerge = out.Operation().Maybe<TYtMerge>()) {
            auto merge = maybeMerge.Cast();

            if (!merge.World().Ref().IsWorld()) {
                continue;
            }

            if (merge.Ref().StartsExecution() || merge.Ref().HasResult()) {
                continue;
            }

            if (publish.DataSink().Cluster().Value() != merge.DataSink().Cluster().Value()) {
                continue;
            }

            if (merge.Settings().Size() != 0) {
                continue;
            }

            auto mergeSection = merge.Input().Item(0);
            if (NYql::HasSettingsExcept(mergeSection.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2)) {
                continue;
            }
            if (HasNonEmptyKeyFilter(mergeSection)) {
                continue;
            }

            if (maxTables && inputCount + mergeSection.Paths().Size() - 1 > *maxTables) {
                continue;
            }

            if (mergeSection.Paths().Size() < 2) {
                continue;
            }

            if (!AllOf(mergeSection.Paths(), [](TYtPath path) {
                return path.Table().Maybe<TYtOutput>()
                    && path.Columns().Maybe<TCoVoid>()
                    && path.Ranges().Maybe<TCoVoid>()
                    && !TYtTableBaseInfo::GetMeta(path.Table())->IsDynamic;
            })) {
                continue;
            }

            if (dstRowSpec->GetColumnOrder().Defined() && AnyOf(mergeSection.Paths(), [colOrder = *dstRowSpec->GetColumnOrder()](auto path) {
                auto rowSpec = TYtTableBaseInfo::GetRowSpec(path.Table());
                return rowSpec->GetColumnOrder().Defined() && *rowSpec->GetColumnOrder() != colOrder;
            })) {
                continue;
            }

            hasUpdates = true;
            inputCount += mergeSection.Paths().Size() - 1;
            updateInputs.pop_back();
            if (IsUnorderedOutput(out)) {
                std::transform(mergeSection.Paths().begin(), mergeSection.Paths().end(), std::back_inserter(updateInputs),
                    [mode = out.Mode(), &ctx](TYtPath path) {
                        auto origOut = path.Table().Cast<TYtOutput>();
                        return Build<TYtOutput>(ctx, origOut.Pos())
                            .InitFrom(origOut)
                            .Mode(mode)
                            .Done();
                    }
                );
            } else {
                std::transform(mergeSection.Paths().begin(), mergeSection.Paths().end(), std::back_inserter(updateInputs),
                    [](TYtPath path) {
                        return path.Table().Cast<TYtOutput>();
                    }
                );
            }
        }
    }
    if (hasUpdates) {
        return Build<TYtPublish>(ctx, publish.Pos())
            .InitFrom(publish)
            .Input()
                .Add(updateInputs)
            .Build()
            .Done().Ptr();
    }
    return node;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::MapToMerge(TExprBase node, TExprContext& ctx) const {
    auto map = node.Cast<TYtMap>();

    auto mapper = map.Mapper();
    if (mapper.Body().Raw() != mapper.Args().Arg(0).Raw()) {
        // Only trivial lambda
        return node;
    }

    if (map.Ref().HasResult()) {
        return node;
    }

    if (map.Input().Size() > 1 || map.Output().Size() > 1) {
        return node;
    }

    if (NYql::HasAnySetting(map.Settings().Ref(), EYtSettingType::JobCount | EYtSettingType::WeakFields | EYtSettingType::Sharded | EYtSettingType::SortLimitBy)) {
        return node;
    }

    auto section = map.Input().Item(0);
    if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::SysColumns)) {
        return node;
    }
    bool useExplicitColumns = false;
    const auto outRowSpec = TYqlRowSpecInfo(map.Output().Item(0).RowSpec());
    const auto nativeType = outRowSpec.GetNativeYtType();
    const auto nativeTypeFlags = outRowSpec.GetNativeYtTypeFlags();

    for (auto path: section.Paths()) {
        TYtPathInfo pathInfo(path);
        if (pathInfo.RequiresRemap()) {
            return node;
        }
        if (nativeType != pathInfo.GetNativeYtType()
            || nativeTypeFlags != pathInfo.GetNativeYtTypeFlags()) {
            return node;
        }
        if (!pathInfo.HasColumns() && (!pathInfo.Table->IsTemp || (pathInfo.Table->RowSpec && pathInfo.Table->RowSpec->HasAuxColumns()))) {
            useExplicitColumns = true;
        }
    }

    if (auto outSorted = map.Output().Item(0).Ref().GetConstraint<TSortedConstraintNode>()) {
        auto inputSorted = map.Input().Item(0).Ref().GetConstraint<TSortedConstraintNode>();
        if (!inputSorted || !outSorted->IsPrefixOf(*inputSorted)) {
            // Don't convert YtMap, which produces sorted output from unsorted input
            return node;
        }
        if (auto maxTablesForSortedMerge = State_->Configuration->MaxInputTablesForSortedMerge.Get()) {
            if (map.Input().Item(0).Paths().Size() > *maxTablesForSortedMerge) {
                return node;
            }
        }
    }

    if (useExplicitColumns) {
        auto inputStructType = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        TSet<TStringBuf> columns;
        for (auto item: inputStructType->GetItems()) {
            columns.insert(item->GetName());
        }

        section = UpdateInputFields(section, std::move(columns), ctx, false);
    }

    return Build<TYtMerge>(ctx, node.Pos())
        .World(map.World())
        .DataSink(map.DataSink())
        .Output(map.Output())
        .Input()
            .Add(section)
        .Build()
        .Settings(NYql::KeepOnlySettings(map.Settings().Ref(), EYtSettingType::Limit | EYtSettingType::KeepSorted | EYtSettingType::QLFilter, ctx))
        .Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::MergeToCopy(TExprBase node, TExprContext& ctx) const {
    auto merge = node.Cast<TYtMerge>();

    if (merge.Ref().HasResult()) {
        return node;
    }

    if (merge.Input().Item(0).Paths().Size() > 1) {
        return node;
    }

    auto cluster = merge.DataSink().Cluster().StringValue();
    if (cluster == YtUnspecifiedCluster || cluster != GetClusterFromSection(merge.Input().Item(0))) {
        return node;
    }

    if (NYql::HasAnySetting(merge.Settings().Ref(), EYtSettingType::ForceTransform | EYtSettingType::SoftTransform | EYtSettingType::CombineChunks | EYtSettingType::QLFilter)) {
        return node;
    }

    auto limitNode = NYql::GetSetting(merge.Settings().Ref(), EYtSettingType::Limit);
    if (limitNode && limitNode->ChildrenSize() > 0) {
        return node;
    }

    TYtSection section = merge.Input().Item(0);
    TYtPath path = section.Paths().Item(0);
    if (!path.Ranges().Maybe<TCoVoid>() || !path.Ref().GetTypeAnn()->Equals(*path.Table().Ref().GetTypeAnn())) {
        return node;
    }
    if (path.Table().Maybe<TYtOutput>().Operation().Maybe<TYtEquiJoin>()) {
        // YtEquiJoin may change output sort after rewrite
        return node;
    }
    auto tableInfo = TYtTableBaseInfo::Parse(path.Table());
    if (path.Table().Maybe<TYtTable>() || tableInfo->Meta->IsDynamic || !tableInfo->RowSpec || !tableInfo->RowSpec->StrictSchema) {
        return node;
    }
    if (tableInfo->IsUnordered && tableInfo->RowSpec->IsSorted()) {
        return node;
    }
    if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample)) {
        return node;
    }
    if (NYql::HasNonEmptyKeyFilter(section)) {
        return node;
    }
    if (NYql::HasSetting(merge.Settings().Ref(), EYtSettingType::KeepSorted)) {
        auto op = path.Table().Maybe<TYtOutput>().Operation().Cast();
        if (!(op.Ref().HasResult() && op.Ref().GetResult().Type() == TExprNode::World || op.Maybe<TYtTouch>())) {
            return node;
        }
    }

    const auto outTable = merge.Output().Item(0);
    TYtOutTableInfo outTableInfo(outTable);
    if (!tableInfo->RowSpec->CompareSortness(*outTableInfo.RowSpec)) {
        return node;
    }

    TStringBuf outColGroup;
    if (auto setting = NYql::GetSetting(outTable.Settings().Ref(), EYtSettingType::ColumnGroups)) {
        outColGroup = setting->Tail().Content();
    }

    YQL_ENSURE(path.Table().Maybe<TYtOutput>());
    TStringBuf inputColGroup;
    const auto out = path.Table().Cast<TYtOutput>();
    if (auto setting = NYql::GetSetting(GetOutputOp(out).Output().Item(FromString<ui32>(out.OutIndex().Value())).Settings().Ref(), EYtSettingType::ColumnGroups)) {
        inputColGroup = setting->Tail().Content();
    }

    if (outColGroup != inputColGroup) {
        return node;
    }

    return Build<TYtCopy>(ctx, node.Pos())
        .World(merge.World())
        .DataSink(merge.DataSink())
        .Output(merge.Output())
        .Input()
            .Add()
                .Paths()
                    .Add()
                        .InitFrom(path)
                        .Columns<TCoVoid>().Build()
                    .Build()
                .Build()
                .Settings()
                .Build()
            .Build()
        .Build()
        .Settings()
        .Build()
        .Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ForceTransform(TExprBase node, TExprContext& ctx) const {
    auto merge = node.Cast<TYtMerge>();

    if (merge.Ref().HasResult()) {
        return node;
    }

    if (!NYql::HasSetting(merge.Settings().Ref(), EYtSettingType::ForceTransform)
        && NYql::HasSetting(merge.Input().Item(0).Settings().Ref(), EYtSettingType::Sample)) {
        return TExprBase(ctx.ChangeChild(merge.Ref(), TYtMerge::idx_Settings, NYql::AddSetting(merge.Settings().Ref(), EYtSettingType::ForceTransform, {}, ctx)));
    }

    return node;
}

template TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ConvertDynamicTablesToStatic<TYtReadTable>(TExprBase node, TExprContext& ctx) const;
template TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ConvertDynamicTablesToStatic<TYtTransientOpBase>(TExprBase node, TExprContext& ctx) const;

template <class TNodeType>
TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ConvertDynamicTablesToStatic(TExprBase node, TExprContext& ctx) const {
    const auto convertDynamicTablesToStatic = State_->Configuration->ConvertDynamicTablesToStatic.Get().GetOrElse(EConvertDynamicTablesToStatic::Disable);
    if (convertDynamicTablesToStatic == EConvertDynamicTablesToStatic::Disable) {
        return node;
    } else if (convertDynamicTablesToStatic == EConvertDynamicTablesToStatic::Join
        && !TYtEquiJoin::Match(node.Raw())) {
        return node;
    }

    if (TYtMerge::Match(node.Raw())) {
        // To not get stuck in a loop
        return node;
    }

    auto op = node.Cast<TNodeType>();

    TVector<TYtSection> newInputs;
    newInputs.reserve(op.Input().Size());
    bool hasChanges = false;

    for (const auto& input : op.Input()) {
        auto section = input.template Cast<TYtSection>();

        TVector<TYtPath> dynamicTableInputs;
        dynamicTableInputs.reserve(section.Paths().Size());

        TVector<TYtPath> otherInputs;
        otherInputs.reserve(section.Paths().Size());

        for (const auto& path : section.Paths()) {
            if (TYtTableBaseInfo::GetMeta(path.Table())->IsDynamic) {
                dynamicTableInputs.emplace_back(path);
            } else {
                otherInputs.emplace_back(path);
            }
        }

        TMaybeNode<NNodes::TYtDSink> dataSink;
        if constexpr (std::is_same_v<TNodeType, TYtReadTable>) {
            dataSink = TYtDSink(ctx.RenameNode(op.DataSource().Ref(), "DataSink"));
        } else {
            dataSink = op.DataSink();
        }

        if (!dynamicTableInputs.empty()) {
            otherInputs.push_back(
                CopyOrTrivialMap(
                    section.Pos(),
                    op.World(),
                    dataSink.Cast(),
                    *section.Ref().GetTypeAnn()->template Cast<TListExprType>()->GetItemType(),
                    Build<TYtSection>(ctx, section.Pos())
                        .Paths()
                            .Add(dynamicTableInputs)
                        .Build()
                        .Settings(NYql::KeepOnlySettings(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::SysColumns, ctx))
                        .Done(),
                    {},
                    ctx,
                    State_,
                    TCopyOrTrivialMapOpts()
                        .SetTryKeepSortness(!NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered))
                        .SetConstraints(section.Ref().GetConstraintSet())
            ));
            newInputs.push_back(
                Build<TYtSection>(ctx, section.Pos())
                    .Paths()
                        .Add(otherInputs)
                    .Build()
                    .Settings(section.Settings())
                    .Done());
            hasChanges = true;
        } else {
            newInputs.emplace_back(input);
        }
    }

    if (hasChanges) {
        return ctx.ChangeChild(
            node.Ref(),
            TNodeType::idx_Input,
            Build<TYtSectionList>(ctx, op.Input().Pos())
                .Add(newInputs)
                .Done()
                .Ptr());
    }
    return node;
}

}  // namespace NYql
