#include "yql_yt_optimize.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_table.h"
#include "yql_yt_helpers.h"
#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/providers/yt/lib/res_pull/table_limiter.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_csee.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>

#include <util/generic/xrange.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/size_literals.h>
#include <util/generic/maybe.h>

#include <utility>

namespace NYql {

using namespace NNodes;
namespace {
TMaybeNode<TYtSection> MaterializeSectionIfRequired(TExprBase world, TYtSection section, TYtDSink dataSink, TYqlRowSpecInfo::TPtr outRowSpec, bool keepSortness,
    const TExprNode::TListType& limitNodes, const TYtState::TPtr& state, TExprContext& ctx)
{
    const bool hasLimit = NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip);
    bool needMaterialize = hasLimit && NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Sample);
    bool hasDynamic = false;
    if (!needMaterialize) {
        bool hasRanges = false;
        for (TYtPath path: section.Paths()) {
            TYtPathInfo pathInfo(path);
            hasDynamic = hasDynamic || (pathInfo.Table->Meta && pathInfo.Table->Meta->IsDynamic);
            hasRanges = hasRanges || pathInfo.Ranges;
        }
        needMaterialize = hasRanges || (hasLimit && hasDynamic);
    }

    if (needMaterialize) {
        auto scheme = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        auto path = CopyOrTrivialMap(section.Pos(),
            world, dataSink,
            *scheme,
            Build<TYtSection>(ctx, section.Pos())
                .Paths(section.Paths())
                .Settings(NYql::RemoveSettings(section.Settings().Ref(),
                    EYtSettingType::Take | EYtSettingType::Skip |
                    EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::JoinLabel |
                    EYtSettingType::Unordered | EYtSettingType::NonUnique | EYtSettingType::StatColumns, ctx))
                .Done(),
            outRowSpec,
            ctx,
            state,
            TCopyOrTrivialMapOpts()
                .SetTryKeepSortness(keepSortness || (!ctx.IsConstraintEnabled<TSortedConstraintNode>() && (!hasDynamic || (!hasLimit && limitNodes.empty()))))
                .SetRangesResetSort(false)
                .SetSectionUniq(section.Ref().GetConstraint<TDistinctConstraintNode>())
                .SetLimitNodes(limitNodes)
            );

        return Build<TYtSection>(ctx, section.Pos())
                .Paths()
                    .Add(path)
                .Build()
                .Settings(NYql::RemoveSetting(section.Settings().Ref(), EYtSettingType::Sample, ctx))
                .Done();
    }

    return {};
}

TMaybeNode<TYtSection> UpdateSectionWithRange(TExprBase world, TYtSection section, const TRecordsRange& range,
    TYtDSink dataSink, TYqlRowSpecInfo::TPtr outRowSpec, bool keepSortness, bool allowWorldDeps, bool allowMaterialize,
    TSyncMap& syncList, const TYtState::TPtr& state, TExprContext& ctx)
{
    bool isEmptyInput = allowWorldDeps;
    TVector<TYtPath> updatedPaths;
    TVector<TYtPath> skippedPaths;
    if (auto limiter = TTableLimiter(range)) {
        if (auto materialized = MaterializeSectionIfRequired(world, section, dataSink, outRowSpec, keepSortness,
            {NYql::KeepOnlySettings(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::SysColumns, ctx)}, state, ctx))
        {
            if (!allowMaterialize || state->Types->EvaluationInProgress) {
                // Keep section as is
                return {};
            }
            if (!allowWorldDeps) {
                if (const auto out = materialized.Paths().Item(0).Table().Maybe<TYtOutput>()) {
                    syncList.emplace(GetOutputOp(out.Cast()).Ptr(), syncList.size());
                }
            }
            return materialized;
        }

        for (size_t i: xrange(section.Paths().Size())) {
            auto path = section.Paths().Item(i);
            TYtPathInfo pathInfo(path);
            if (!pathInfo.Table->Stat) {
                // Not all tables have required info
                return {};
            }

            ui64 startRecordInTable = 0;
            ui64 endRecordInTable = 0;

            if (pathInfo.Table->Stat->RecordsCount) {
                if (!limiter.NextTable(pathInfo.Table->Stat->RecordsCount)) {
                    if (allowWorldDeps) {
                        skippedPaths.push_back(path);
                    } else {
                        pathInfo.Stat.Drop();
                        pathInfo.Ranges = TYtRangesInfo::MakeEmptyRange();
                        updatedPaths.push_back(pathInfo.ToExprNode(ctx, path.Pos(), path.Table()).Cast<TYtPath>());
                    }
                    continue;
                }
                startRecordInTable = limiter.GetTableStart();
                endRecordInTable = limiter.GetTableZEnd(); // 0 means the entire table usage
            }

            if (startRecordInTable || endRecordInTable) {
                pathInfo.Stat.Drop();
                pathInfo.Ranges = MakeIntrusive<TYtRangesInfo>();
                TYtRangesInfo::TRowRange range;
                if (startRecordInTable) {
                    range.Lower = startRecordInTable;
                }
                if (endRecordInTable) {
                    range.Upper = endRecordInTable;
                }
                pathInfo.Ranges->AddRowRange(range);
                updatedPaths.push_back(pathInfo.ToExprNode(ctx, path.Pos(), path.Table()).Cast<TYtPath>());
            } else {
                updatedPaths.push_back(path);
            }
            isEmptyInput = false;
            if (limiter.Exceed()) {
                if (allowWorldDeps) {
                    for (size_t j = i + 1; j < section.Paths().Size(); ++j) {
                        skippedPaths.push_back(section.Paths().Item(j));
                    }
                } else {
                    for (size_t j = i + 1; j < section.Paths().Size(); ++j) {
                        auto path = section.Paths().Item(j);
                        path = Build<TYtPath>(ctx, path.Pos())
                            .InitFrom(path)
                            .Ranges<TExprList>()
                            .Build()
                            .Stat<TCoVoid>().Build()
                            .Done();
                        updatedPaths.push_back(path);
                    }
                }
                break;
            }
        }
    } else if (!allowWorldDeps) {
        for (auto path: section.Paths()) {
            updatedPaths.push_back(Build<TYtPath>(ctx, path.Pos())
                .InitFrom(path)
                .Ranges<TExprList>()
                .Build()
                .Stat<TCoVoid>().Build()
                .Done());
        }
    }

    if (isEmptyInput) {
        skippedPaths.assign(section.Paths().begin(), section.Paths().end());
    }
    for (auto path: skippedPaths) {
        if (auto out = path.Table().Maybe<TYtOutput>()) {
            syncList.emplace(GetOutputOp(out.Cast()).Ptr(), syncList.size());
        }
    }

    if (isEmptyInput) {
        return MakeEmptySection(section, dataSink, keepSortness, state, ctx);
    }

    return Build<TYtSection>(ctx, section.Pos())
        .Paths()
            .Add(updatedPaths)
        .Build()
        .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip, ctx))
        .Done();
}

void EnableKeyBoundApi(TYtPathInfo& pathInfo, const TYtState::TPtr& state) {
    if (!pathInfo.Ranges) {
        return;
    }
    YQL_ENSURE(pathInfo.Table);
    const bool useKeyBoundApi =
        state->Configuration->_UseKeyBoundApi.Get(pathInfo.Table->Cluster).GetOrElse(DEFAULT_USE_KEY_BOUND_API);
    pathInfo.Ranges->SetUseKeyBoundApi(useKeyBoundApi);
}

TMaybeNode<TYtSection> UpdateSectionWithLegacyFilters(TYtSection section, const TVector<TExprBase>& filters, const TYtState::TPtr& state, TExprContext& ctx)
{
    TVector<TExprBase> commonFilters;
    TMap<size_t, TVector<TExprBase>> tableFilters;
    for (auto filter: filters) {
        auto filterList = filter.Cast<TExprList>();
        if (filterList.Size() == 2) {
            tableFilters[FromString<size_t>(filterList.Item(1).Cast<TCoAtom>().Value())].push_back(filterList.Item(0));
        }
        else {
            commonFilters.push_back(filterList.Item(0));
        }
    }

    TVector<TYtPath> updatedPaths;
    size_t tableIndex = 0;
    for (auto path: section.Paths()) {
        if (commonFilters.size() == filters.size()) {
            TYtPathInfo pathInfo(path);
            pathInfo.Stat.Drop();
            pathInfo.Ranges = TYtRangesInfo::ApplyLegacyKeyFilters(commonFilters, pathInfo.Table->RowSpec, ctx);
            EnableKeyBoundApi(pathInfo, state);
            updatedPaths.push_back(pathInfo.ToExprNode(ctx, path.Pos(), path.Table()).Cast<TYtPath>());
        }
        else {
            TVector<TExprBase> pathFilters = commonFilters;
            if (auto p = tableFilters.FindPtr(tableIndex)) {
                pathFilters.insert(pathFilters.end(), p->begin(), p->end());
            }
            if (pathFilters.empty()) {
                updatedPaths.push_back(path);
            }
            else {
                TYtPathInfo pathInfo(path);
                pathInfo.Stat.Drop();
                pathInfo.Ranges = TYtRangesInfo::ApplyLegacyKeyFilters(pathFilters, pathInfo.Table->RowSpec, ctx);
                EnableKeyBoundApi(pathInfo, state);
                updatedPaths.push_back(pathInfo.ToExprNode(ctx, path.Pos(), path.Table()).Cast<TYtPath>());
            }
        }
        ++tableIndex;
    }

    auto updatedSettings = NYql::RemoveSetting(section.Settings().Ref(), EYtSettingType::KeyFilter, ctx);
    updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::KeyFilter, ctx.NewList(section.Pos(), {}), ctx);

    return Build<TYtSection>(ctx, section.Pos())
        .Paths()
            .Add(updatedPaths)
        .Build()
        .Settings(updatedSettings)
        .Done();
}

TMaybeNode<TYtSection> UpdateSectionWithFilters(TYtSection section, const TVector<TExprBase>& filters, const TYtState::TPtr& state, TExprContext& ctx) {
    TMap<size_t, TExprNode::TPtr> filtersByTableIndex;
    TExprNode::TPtr commonFilter;
    for (auto filter: filters) {
        auto filterList = filter.Cast<TExprList>();
        auto computedFilter = filterList.Item(0).Ptr();
        if (filterList.Size() == 3) {
            for (auto idxNode : filterList.Item(2).Cast<TCoAtomList>()) {
                size_t idx = FromString<size_t>(idxNode.Value());
                YQL_ENSURE(!filtersByTableIndex.contains(idx));
                filtersByTableIndex[idx] = computedFilter;
            }
        } else {
            YQL_ENSURE(!commonFilter);
            commonFilter = computedFilter;
        }
    }

    YQL_ENSURE(filtersByTableIndex.empty() && commonFilter || !commonFilter && !filtersByTableIndex.empty());

    TVector<TYtPath> updatedPaths;
    size_t tableIndex = 0;
    for (auto path: section.Paths()) {
        TExprNode::TPtr filter;
        if (commonFilter) {
            filter = commonFilter;
        } else {
            auto it = filtersByTableIndex.find(tableIndex);
            if (it != filtersByTableIndex.end()) {
                filter = it->second;
            }
        }

        if (!filter) {
            updatedPaths.push_back(path);
        } else {
            TYtPathInfo pathInfo(path);
            pathInfo.Stat.Drop();
            pathInfo.Ranges = TYtRangesInfo::ApplyKeyFilter(*filter);
            EnableKeyBoundApi(pathInfo, state);
            updatedPaths.push_back(pathInfo.ToExprNode(ctx, path.Pos(), path.Table()).Cast<TYtPath>());
        }
        ++tableIndex;
    }

    auto updatedSettings = NYql::RemoveSetting(section.Settings().Ref(), EYtSettingType::KeyFilter2, ctx);
    updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::KeyFilter2, ctx.NewList(section.Pos(), {}), ctx);

    return Build<TYtSection>(ctx, section.Pos())
        .Paths()
            .Add(updatedPaths)
        .Build()
        .Settings(updatedSettings)
        .Done();
}

} //namespace

TMaybeNode<TYtSection> UpdateSectionWithSettings(TExprBase world, TYtSection section, TYtDSink dataSink, TYqlRowSpecInfo::TPtr outRowSpec, bool keepSortness, bool allowWorldDeps, bool allowMaterialize,
    TSyncMap& syncList, const TYtState::TPtr& state, TExprContext& ctx)
{
    if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::DirectRead)) {
        return {};
    }

    if (!NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2)) {
        return {};
    }

    if (HasNodesToCalculate(section.Ptr())) {
        return {};
    }

    TRecordsRange range;
    TVector<TExprBase> keyFilters;
    bool legacyKeyFilters = false;
    for (auto s: section.Settings()) {
        switch (FromString<EYtSettingType>(s.Name().Value())) {
        case EYtSettingType::KeyFilter:
            legacyKeyFilters = true;
            [[fallthrough]];
        case EYtSettingType::KeyFilter2:
            if (s.Value().Cast<TExprList>().Size() > 0) {
                keyFilters.push_back(s.Value().Cast());
            }
            break;
        default:
            // Skip other settings
            break;
        }
    }
    range.Fill(section.Settings().Ref());

    if (range.Limit || range.Offset) {
        return UpdateSectionWithRange(world, section, range, dataSink, outRowSpec, keepSortness, allowWorldDeps, allowMaterialize, syncList, state, ctx);
    }
    if (!keyFilters.empty()) {
        return legacyKeyFilters ? UpdateSectionWithLegacyFilters(section, keyFilters, state, ctx) : UpdateSectionWithFilters(section, keyFilters, state, ctx);
    }

    return {};
}

TYtSection MakeEmptySection(TYtSection section, NNodes::TYtDSink dataSink, bool keepSortness, const TYtState::TPtr& state, TExprContext& ctx) {
    TYtOutTableInfo outTable(GetSequenceItemType(section, false)->Cast<TStructExprType>(),
        state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
    if (section.Paths().Size() == 1) {
        auto srcTableInfo = TYtTableBaseInfo::Parse(section.Paths().Item(0).Table());
        if (keepSortness && srcTableInfo->RowSpec && srcTableInfo->RowSpec->IsSorted()) {
            outTable.RowSpec->CopySortness(ctx, *srcTableInfo->RowSpec, TYqlRowSpecInfo::ECopySort::WithCalc);
        }
    }
    outTable.SetUnique(section.Ref().GetConstraint<TDistinctConstraintNode>(), section.Pos(), ctx);
    return Build<TYtSection>(ctx, section.Pos())
        .Paths()
            .Add()
                .Table<TYtOutput>()
                    .Operation<TYtTouch>()
                        .World<TCoWorld>().Build()
                        .DataSink(dataSink)
                        .Output()
                            .Add(outTable.ToExprNode(ctx, section.Pos()).Cast<TYtOutTable>())
                        .Build()
                    .Build()
                    .OutIndex().Value("0").Build()
                .Build()
                .Columns<TCoVoid>().Build()
                .Ranges<TCoVoid>().Build()
                .Stat<TCoVoid>().Build()
            .Build()
        .Build()
        .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample, ctx))
        .Done();
}

TExprNode::TPtr OptimizeReadWithSettings(const TExprNode::TPtr& node, bool allowWorldDeps, bool allowMaterialize, TSyncMap& syncList,
    const TYtState::TPtr& state, TExprContext& ctx)
{
    auto read = TYtReadTable(node);
    auto dataSink = TYtDSink(ctx.RenameNode(read.DataSource().Ref(), "DataSink"));

    bool hasUpdates = false;
    TVector<TExprBase> updatedSections;
    for (auto section: read.Input()) {
        updatedSections.push_back(section);
        const bool keepSort = ctx.IsConstraintEnabled<TSortedConstraintNode>() && !NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered);
        if (auto updatedSection = UpdateSectionWithSettings(read.World(), section, dataSink, {}, keepSort, allowWorldDeps, allowMaterialize, syncList, state, ctx)) {
            updatedSections.back() = updatedSection.Cast();
            hasUpdates = true;
        }
    }
    if (!hasUpdates) {
        return node;
    }

    auto res = ctx.ChangeChild(read.Ref(), TYtReadTable::idx_Input,
        Build<TYtSectionList>(ctx, read.Input().Pos())
            .Add(updatedSections)
            .Done().Ptr());

    return res;
}

IGraphTransformer::TStatus UpdateTableContentMemoryUsage(const TExprNode::TPtr& input, TExprNode::TPtr& output, const TYtState::TPtr& state, TExprContext& ctx) {
    auto current = input;
    output.Reset();
    for (;;) {
        TProcessedNodesSet ignoreNodes;
        VisitExpr(current, [&ignoreNodes](const TExprNode::TPtr& node) {
            if (TYtOutput::Match(node.Get())) {
                // Stop traversing dependent operations
                ignoreNodes.insert(node->UniqueId());
                return false;
            }
            return true;
        });

        TOptimizeExprSettings settings(state->Types);
        settings.CustomInstantTypeTransformer = state->Types->CustomInstantTypeTransformer.Get();
        settings.ProcessedNodes = &ignoreNodes;

        TParentsMap parentsMap;
        GatherParents(*current, parentsMap);

        TExprNode::TPtr newCurrent;
        auto status = OptimizeExpr(current, newCurrent,
            [&parentsMap, current, state](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                if (auto maybeContent = TMaybeNode<TYtTableContent>(node)) {
                    auto content = maybeContent.Cast();
                    if (NYql::HasSetting(content.Settings().Ref(), EYtSettingType::MemUsage)) {
                        return node;
                    }

                    ui64 collectRowFactor = 0;
                    if (auto setting = NYql::GetSetting(content.Settings().Ref(), EYtSettingType::RowFactor)) {
                        collectRowFactor = FromString<ui64>(setting->Child(1)->Content());
                    } else {
                        const auto contentItemType = content.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                        size_t fieldsCount = 0;
                        switch (contentItemType->GetKind()) {
                        case ETypeAnnotationKind::Struct:
                            fieldsCount = contentItemType->Cast<TStructExprType>()->GetSize();
                            break;
                        case ETypeAnnotationKind::Tuple:
                            fieldsCount = contentItemType->Cast<TTupleExprType>()->GetSize();
                            break;
                        default:
                            break;
                        }
                        collectRowFactor = 2 * (1 + fieldsCount) * sizeof(NKikimr::NUdf::TUnboxedValuePod);
                    }

                    bool wrapToCollect = false;
                    TVector<std::pair<double, ui64>> factors; // first: sizeFactor, second: rowFactor
                    TNodeSet tableContentConsumers;
                    if (!GetTableContentConsumerNodes(*node, *current, parentsMap, tableContentConsumers)) {
                        wrapToCollect = true;
                        factors.emplace_back(2., collectRowFactor);
                    }
                    else {
                        for (auto consumer: tableContentConsumers) {
                            if (consumer->IsCallable({"ToDict","SqueezeToDict", "SqlIn"})) {
                                double sizeFactor = 1.;
                                ui64 rowFactor = 0ULL;
                                if (auto err = CalcToDictFactors(*consumer, ctx, sizeFactor, rowFactor)) {
                                    ctx.AddError(*err);
                                    return {};
                                }
                                factors.emplace_back(sizeFactor, rowFactor);
                            }
                            else if (consumer->IsCallable("Collect")) {
                                factors.emplace_back(2., collectRowFactor);
                            }
                        }
                    }

                    ui64 memUsage = 0;
                    ui64 itemsCount = 0;
                    bool useItemsCount = !NYql::HasSetting(content.Settings().Ref(), EYtSettingType::ItemsCount);

                    if (factors.empty()) {
                        // No ToDict or Collect consumers. Assume memory usage equals to max row size on YT
                        memUsage = 16_MB;
                        useItemsCount = false;
                    }
                    else {
                        if (auto maybeRead = content.Input().Maybe<TYtReadTable>()) {
                            TVector<ui64> records;
                            TVector<TYtPathInfo::TPtr> tableInfos;
                            bool hasNotCalculated = false;
                            for (auto section: maybeRead.Cast().Input()) {
                                for (auto path: section.Paths()) {
                                    TYtPathInfo::TPtr info = MakeIntrusive<TYtPathInfo>(path);
                                    if (info->Table->Stat) {
                                        ui64 tableRecord = info->Table->Stat->RecordsCount;
                                        if (info->Ranges) {
                                            const auto used = info->Ranges->GetUsedRows(tableRecord);
                                            tableRecord = used.GetOrElse(tableRecord);
                                            if (used) {
                                                itemsCount += *used;
                                            } else {
                                                useItemsCount = false;
                                            }
                                        } else {
                                            itemsCount += tableRecord;
                                        }
                                        if (info->Table->Meta->IsDynamic) {
                                            useItemsCount = false;
                                        }
                                        records.push_back(tableRecord);
                                        tableInfos.push_back(info);
                                    }
                                    else {
                                        YQL_CLOG(INFO, ProviderYt) << "Assume 1Gb memory usage for YtTableContent #"
                                            << node->UniqueId() << " because input table is not calculated yet";
                                        memUsage += 1_GB;
                                        hasNotCalculated = true;
                                        useItemsCount = false;
                                        break;
                                    }
                                }
                                if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Sample)) {
                                    useItemsCount = false;
                                }
                                if (hasNotCalculated) {
                                    break;
                                }
                            }
                            if (!hasNotCalculated && !tableInfos.empty()) {
                                if (auto dataSizes = EstimateDataSize(TString{maybeRead.Cast().DataSource().Cluster().Value()}, tableInfos, Nothing(), *state, ctx)) {
                                    YQL_ENSURE(dataSizes->size() == records.size());
                                    for (size_t i: xrange(records.size())) {
                                        for (auto& factor: factors) {
                                            memUsage += factor.first * dataSizes->at(i) + factor.second * records.at(i);
                                        }
                                    }
                                } else {
                                    return {};
                                }
                            }
                        }
                        else {
                            TYtOutTableInfo info(GetOutTable(content.Input().Cast<TYtOutput>()));
                            if (info.Stat) {
                                const ui64 dataSize = info.Stat->DataSize;
                                const ui64 records = info.Stat->RecordsCount;
                                for (auto& factor: factors) {
                                    memUsage += factor.first * dataSize + factor.second * records;
                                }
                                itemsCount += records;
                            }
                            else {
                                YQL_CLOG(INFO, ProviderYt) << "Assume 1Gb memory usage for YtTableContent #"
                                    << node->UniqueId() << " because input table is not calculated yet";
                                memUsage += 1_GB;
                                useItemsCount = false;
                            }
                        }
                    }

                    auto settings = content.Settings().Ptr();
                    settings = NYql::AddSetting(*settings, EYtSettingType::MemUsage, ctx.NewAtom(node->Pos(), ToString(memUsage), TNodeFlags::Default), ctx);
                    if (useItemsCount) {
                        settings = NYql::AddSetting(*settings, EYtSettingType::ItemsCount, ctx.NewAtom(node->Pos(), ToString(itemsCount), TNodeFlags::Default), ctx);
                    }

                    return ctx.WrapByCallableIf(wrapToCollect, "Collect", ctx.ChangeChild(*node, TYtTableContent::idx_Settings, std::move(settings)));
                }
                return node;
            },
            ctx, settings);

        if (IGraphTransformer::TStatus::Error == status.Level) {
            ctx.AddError(TIssue(ctx.GetPosition(current->Pos()), TStringBuilder() << "Failed to update YtTableContent memory usage in node: " << current->Content()));
            return status;
        }

        if (newCurrent != current) {
            if (current->IsLambda()) {
                YQL_ENSURE(newCurrent->IsLambda());
                YQL_ENSURE(newCurrent->Head().ChildrenSize() == current->Head().ChildrenSize());
                for (size_t i = 0; i < newCurrent->Head().ChildrenSize(); ++i) {
                    newCurrent->Head().Child(i)->SetTypeAnn(current->Head().Child(i)->GetTypeAnn());
                    newCurrent->Head().Child(i)->CopyConstraints(*current->Head().Child(i));
                }
            }
            auto typeTransformer = CreateTypeAnnotationTransformer(CreateExtCallableTypeAnnotationTransformer(*state->Types, true), *state->Types);
            auto constrTransformer = CreateConstraintTransformer(*state->Types, true, true);
            TVector<TTransformStage> transformers;
            const auto issueCode = TIssuesIds::CORE_TYPE_ANN;
            transformers.push_back(TTransformStage(typeTransformer, "TypeAnnotation", issueCode));
            transformers.push_back(TTransformStage(
                CreateFunctorTransformer([](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) { return UpdateCompletness(input, output, ctx); }),
                "UpdateCompletness", issueCode));
            transformers.push_back(TTransformStage(constrTransformer, "Constraints", issueCode));
            auto fullTransformer = CreateCompositeGraphTransformer(transformers, false);
            status = InstantTransform(*fullTransformer, newCurrent, ctx);
            if (status.Level == IGraphTransformer::TStatus::Error) {
                return status;
            }

            current = newCurrent;
            continue;
        }

        output = current;
        return IGraphTransformer::TStatus::Ok;
    }
}

struct TPeepholePipelineConfigurator : public IPipelineConfigurator {
    TPeepholePipelineConfigurator(TYtState::TPtr state)
        : State_(std::move(state))
        {}
private:
    void AfterCreate(TTransformationPipeline*) const final {}

    void AfterTypeAnnotation(TTransformationPipeline* pipeline) const final {
        pipeline->Add(CreateYtPeepholeTransformer(State_, {}), "Peephole");
        pipeline->Add(CreateYtWideFlowTransformer(State_), "WideFlow");
    }

    void AfterOptimize(TTransformationPipeline*) const final {}

    const TYtState::TPtr State_;
};

IGraphTransformer::TStatus PeepHoleOptimizeBeforeExec(TExprNode::TPtr input, TExprNode::TPtr& output,
    const TYtState::TPtr& state, bool& hasNonDeterministicFunctions, TExprContext& ctx)
{
    if (const auto status = UpdateTableContentMemoryUsage(input, output, state, ctx);
        status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    const TPeepholePipelineConfigurator wideFlowTransformers(state);
    TPeepholeSettings peepholeSettings;
    peepholeSettings.CommonConfig = &wideFlowTransformers;
    return PeepHoleOptimizeNode(output, output, ctx, *state->Types, nullptr, hasNonDeterministicFunctions, peepholeSettings);
}

} // NYql
