#include "yql_yt_provider_impl.h"
#include "yql_yt_provider.h"
#include "yql_yt_helpers.h"
#include "yql_yt_optimize.h"
#include "yql_yt_horizontal_join.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/generic/bitmap.h>
#include <util/generic/map.h>
#include <util/generic/hash.h>
#include <util/generic/algorithm.h>
#include <util/string/cast.h>
#include <util/str_stl.h>

#include <utility>
#include <unordered_set>

namespace NYql {

using namespace NNodes;

namespace {

TYtOutputOpBase GetRealOperation(TExprBase op) {
    if (const auto mayTry = op.Maybe<TYtTryFirst>())
        return mayTry.Cast().First();
    return op.Cast<TYtOutputOpBase>();
}

class TYtPhysicalFinalizingTransformer : public TSyncTransformerBase {
public:
    TYtPhysicalFinalizingTransformer(TYtState::TPtr state)
        : State_(state)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        TOpDeps opDeps;
        std::vector<const TExprNode*> opDepsOrder;
        TNodeSet hasWorldDeps; // Operations, which have world dependencies on them from another nodes
        TNodeSet toCombine;
        TNodeSet neverCombine;
        TNodeSet lefts;
        TNodeMap<TString> commits;

        bool enableChunkCombining = IsChunkCombiningEnabled();

        auto storeDep = [&opDeps, &opDepsOrder](const TYtOutput& out, const TExprNode* reader, const TExprNode* sec, const TExprNode* path) {
            const auto op = out.Operation().Raw();
            auto& res = opDeps[op];
            if (res.empty()) {
                opDepsOrder.push_back(op);
            }
            res.emplace_back(reader, sec, out.Raw(), path);
        };

        TParentsMap parentsMap;
        GatherParents(*input, parentsMap);

        TNodeSet visitedOutParents;
        std::vector<const TExprNode*> outs;
        VisitExpr(input, [&](const TExprNode::TPtr& node)->bool {
            if (auto maybeOp = TMaybeNode<TYtTransientOpBase>(node)) {
                auto op = maybeOp.Cast();
                for (auto section: op.Input()) {
                    for (auto path: section.Paths()) {
                        if (auto maybeOutput = path.Table().Maybe<TYtOutput>()) {
                            visitedOutParents.insert(path.Raw());
                            auto out = maybeOutput.Cast();
                            storeDep(out, op.Raw(), section.Raw(), path.Raw());
                            if (enableChunkCombining) {
                                CollectForCombine(out, toCombine, neverCombine);
                            }
                        }
                    }
                }
            }
            else if (auto maybeRead = TMaybeNode<TYtReadTable>(node)) {
                auto read = maybeRead.Cast();
                for (auto section: read.Input()) {
                    for (auto path: section.Paths()) {
                        if (auto maybeOutput = path.Table().Maybe<TYtOutput>()) {
                            visitedOutParents.insert(path.Raw());
                            auto out = maybeOutput.Cast();
                            storeDep(out, read.Raw(), section.Raw(), path.Raw());
                            if (enableChunkCombining) {
                                CollectForCombine(out, toCombine, neverCombine);
                            }
                        }
                    }
                }
            }
            else if (auto maybePublish = TMaybeNode<TYtPublish>(node)) {
                auto publish = maybePublish.Cast();
                visitedOutParents.insert(publish.Input().Raw());
                for (auto out: publish.Input()) {
                    storeDep(out, publish.Raw(), nullptr, nullptr);
                }
            }
            else if (auto maybeOutput = TMaybeNode<TYtOutput>(node)) {
                outs.push_back(node.Get());
            }
            else if (auto maybeLeft = TMaybeNode<TCoLeft>(node)) {
                if (auto maybeOp = maybeLeft.Input().Maybe<TYtOutputOpBase>()) {
                    hasWorldDeps.insert(maybeOp.Cast().Raw());
                }
                lefts.insert(maybeLeft.Raw());
            }
            else if (auto maybeCommit = TMaybeNode<TCoCommit>(node)) {
                if (auto ds = maybeCommit.DataSink().Maybe<TYtDSink>()) {
                    if (ProcessedMergePublish.find(node->UniqueId()) == ProcessedMergePublish.end()) {
                        commits.emplace(node.Get(), ds.Cast().Cluster().Value());
                    }
                }
            }

            return true;
        });

        for (auto out: outs) {
            std::vector<const TExprNode*> readers;
            if (auto it = parentsMap.find(out); it != parentsMap.end()) {
                std::copy_if(it->second.begin(), it->second.end(),
                    std::back_inserter(readers),
                    [&visitedOutParents](auto n) {
                        return !visitedOutParents.contains(n);
                    }
                );
            }

            if (!readers.empty()) {
                std::stable_sort(readers.begin(), readers.end(), [](auto l, auto r) { return l->UniqueId() < r->UniqueId(); });
                for (auto n: readers) {
                    YQL_ENSURE(!TYtPath::Match(n)); // All YtPath usages must be gathered in previous VisitExpr
                    storeDep(TYtOutput(out), n, nullptr, nullptr);
                    if (enableChunkCombining && (TYtTableContent::Match(n) || TResWriteBase::Match(n) || TYtStatOut::Match(n))) {
                        CollectForCombine(TYtOutput(out), toCombine, neverCombine);
                    }
                }
            }
        }

        YQL_ENSURE(opDeps.size() == opDepsOrder.size());

        const auto disableOptimizers = State_->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>());

        if (!disableOptimizers.contains("MergePublish")) {
            for (auto& c: commits) {
                THashMap<TString, TVector<const TExprNode*>> groupedPublishesByDst;
                VisitExprByFirst(*c.first, [&](const TExprNode& node) {
                    if (auto maybePub = TMaybeNode<TYtPublish>(&node)) {
                        auto pub = maybePub.Cast();
                        if (pub.DataSink().Cluster().Value() == c.second && !NYql::HasSetting(pub.Settings().Ref(), EYtSettingType::MonotonicKeys)) {
                            groupedPublishesByDst[TString{pub.Publish().Name().Value()}].push_back(&node);
                        }
                    } else if (auto maybeCommit = TMaybeNode<TCoCommit>(&node)) {
                        if (auto maybeSink = maybeCommit.DataSink().Maybe<TYtDSink>()) {
                            // Stop traversing when got another commit on the same cluster
                            return maybeSink.Cast().Cluster().Value() != c.second || &node == c.first;
                        }
                    }
                    return true;
                });
                TVector<TString> dstTables;
                for (auto& grp: groupedPublishesByDst) {
                    if (grp.second.size() > 1) {
                        dstTables.push_back(grp.first);
                    }
                }
                if (dstTables.size() > 1) {
                    ::Sort(dstTables);
                }
                for (auto& tbl: dstTables) {
                    auto& grp = groupedPublishesByDst[tbl];
                    while (grp.size() > 1) {
                        TNodeOnNodeOwnedMap replaceMap;
                        // Optimize only two YtPublish nodes at once to properly update world dependencies
                        auto last = TYtPublish(grp[0]);
                        auto prev = TYtPublish(grp[1]);
                        if (last.World().Raw() != prev.Raw()) {
                            // Has extra dependencies. Don't merge
                            grp.erase(grp.begin());
                            continue;
                        }

                        auto mode = NYql::GetSetting(last.Settings().Ref(), EYtSettingType::Mode);
                        YQL_ENSURE(mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Append);
                        YQL_ENSURE(!NYql::HasSetting(last.Settings().Ref(), EYtSettingType::Initial));

                        replaceMap.emplace(grp[0],
                            Build<TYtPublish>(ctx, grp[0]->Pos())
                                .InitFrom(prev)
                                .Input()
                                    .Add(prev.Input().Ref().ChildrenList())
                                    .Add(last.Input().Ref().ChildrenList())
                                .Build()
                                .Done().Ptr()
                        );
                        replaceMap.emplace(grp[1], prev.World().Ptr());

                        YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-MergePublish";
                        return RemapExpr(input, output, replaceMap, ctx, TOptimizeExprSettings(State_->Types));
                    }
                }
                ProcessedMergePublish.insert(c.first->UniqueId());
            }
        }

        TStatus status = TStatus::Ok;
        if (!disableOptimizers.contains("BypassMergeBeforeLength")) {
            status = BypassMergeBeforeLength(input, output, opDeps, lefts, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (!disableOptimizers.contains("AlignPublishTypes")) {
            status = AlignPublishTypes(input, output, opDeps, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (!disableOptimizers.contains("SuppressOuts")) {
            status = OptimizeUnusedOuts(input, output, opDeps, lefts, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (!disableOptimizers.contains("UnorderedOuts") && ctx.IsConstraintEnabled<TSortedConstraintNode>()) {
            status = OptimizeUnorderedOuts(input, output, opDepsOrder, opDeps, lefts, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (!disableOptimizers.contains("FieldSubsetForMultiUsage")) {
            status = OptimizeFieldSubsetForMultiUsage(input, output, opDeps, lefts, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (State_->Configuration->MaxInputTables.Get().Defined() || State_->Configuration->MaxInputTablesForSortedMerge.Get().Defined()) {
            // Run it after UnorderedOuts, because sorted YtMerge may become unsorted after UnorderedOuts
            status = SplitLargeInputs(input, output, ctx, !disableOptimizers.contains("SplitLargeMapInputs"));
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (!disableOptimizers.contains("MergeMultiOuts")) {
            status = OptimizeMultiOuts(input, output, opDeps, lefts, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (!State_->Configuration->DisableFuseOperations.Get().GetOrElse(DEFAULT_DISABLE_FUSE_OPERATIONS) &&
            !disableOptimizers.contains("FuseMultiOutsWithOuterMaps")) {
            status = FuseMultiOutsWithOuterMaps(input, output, opDeps, lefts, hasWorldDeps, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (enableChunkCombining) {
            EraseNodesIf(toCombine, [&neverCombine](const auto& entry) { return neverCombine.count(entry) != 0; });
            if (!toCombine.empty()) {
                status = AddChunkCombining(input, output, toCombine, lefts, opDeps, ctx);
                if (status.Level != TStatus::Ok) {
                    return status;
                }
            }
            toCombine.clear();
            neverCombine.clear();
        }
        lefts.clear();

        TParentsMap limits; // op -> settings with limits
        TNodeSet noLimits; // writers
        for (auto& x: opDeps) {
            auto writer = x.first;
            if (!TYtTransientOpBase::Match(writer)) {
                continue;
            }
            if (NYql::HasSetting(*writer->Child(TYtTransientOpBase::idx_Settings), EYtSettingType::Limit)) {
                continue;
            }
            if (writer->HasResult() && writer->GetResult().Type() == TExprNode::World) {
                continue;
            }

            bool canHaveLimit = TYtTransientOpBase(writer).Output().Size() == 1;
            if (canHaveLimit) {
                TString usedCluster;
                for (auto item: x.second) {
                    if (!std::get<1>(item)) { // YtLength, YtPublish
                        canHaveLimit = false;
                        break;
                    }
                    for (auto path: TYtSection(std::get<1>(item)).Paths()) {
                        if (!path.Ranges().Maybe<TCoVoid>()) {
                            canHaveLimit = false;
                            break;
                        }
                    }
                    if (!canHaveLimit) {
                        break;
                    }
                    bool hasTake = false;
                    for (auto setting: TYtSection(std::get<1>(item)).Settings()) {
                        auto kind = FromString<EYtSettingType>(setting.Name().Value());
                        if (EYtSettingType::Take == kind || EYtSettingType::Skip == kind) {
                            TSyncMap syncList;
                            if (!IsYtCompleteIsolatedLambda(setting.Value().Ref(), syncList, usedCluster, false) || !syncList.empty()) {
                                hasTake = false;
                                break;
                            }
                            if (EYtSettingType::Take == kind) {
                                hasTake = true;
                            }
                        }
                    }

                    if (!hasTake) {
                        canHaveLimit = false;
                        break;
                    }
                }
            }

            if (!canHaveLimit) {
                noLimits.insert(writer);
                continue;
            }

            auto& limit = limits[writer];
            for (auto item: x.second) {
                limit.insert(TYtSection(std::get<1>(item)).Settings().Raw());
            }
        }

        for (auto writer : noLimits) {
            limits.erase(writer);
        }

        status = OptimizeExpr(output, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) {
            if (!node->HasResult() || node->GetResult().Type() != TExprNode::World) {
                const bool opWithJobs = TYtWithUserJobsOpBase::Match(node.Get());
                if (opWithJobs || TYtSort::Match(node.Get()) || TYtMerge::Match(node.Get())) {
                    auto ret = Limits(node, limits, ctx);
                    if (ret != node) {
                        if (ret) {
                            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-Limits";
                        }
                        return ret;
                    }

                    if (opWithJobs) {
                        ret = LengthOverPhysicalList(node, opDeps, ctx);
                        if (ret != node) {
                            if (ret) {
                                YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-LengthOverPhysicalList";
                            }
                            return ret;
                        }
                        ret = TopSortForProducers(node, opDeps, ctx);
                        if (ret != node) {
                            if (ret) {
                                YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-TopSortForProducers";
                            }
                            return ret;
                        }
                    }
                }
            }
            return node;
        }, ctx, TOptimizeExprSettings(nullptr));

        if (status.Level != TStatus::Ok) {
            return status;
        }

        if (!disableOptimizers.contains("HorizontalJoin")) {
            status = THorizontalJoinOptimizer(State_, opDepsOrder, opDeps, hasWorldDeps, &ProcessedHorizontalJoin).Optimize(output, output, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (!disableOptimizers.contains("MultiHorizontalJoin")) {
            status = TMultiHorizontalJoinOptimizer(State_, opDepsOrder, opDeps, hasWorldDeps).Optimize(output, output, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (!disableOptimizers.contains("OutHorizontalJoin")) {
            status = TOutHorizontalJoinOptimizer(State_, opDepsOrder, opDeps, hasWorldDeps).Optimize(output, output, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        if (const auto mode = State_->Configuration->ColumnGroupMode.Get().GetOrElse(EColumnGroupMode::Disable); mode != EColumnGroupMode::Disable) {
            status = CalculateColumnGroups(input, output, opDeps, mode, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        return status;
    }

    void Rewind() final {
        ProcessedMergePublish.clear();
        ProcessedSplitLargeInputs.clear();
        ProcessedUnusedOuts.clear();
        ProcessedMultiOuts.clear();
        ProcessedHorizontalJoin.clear();
        ProcessedFieldSubsetForMultiUsage.clear();
        ProcessedFuseWithOuterMaps.clear();
        ProcessedCalculateColumnGroups.clear();
    }

private:

    static bool IsBeingExecuted(const TExprNode& op) {
        if (TYtTryFirst::Match(&op)) {
            return op.Head().StartsExecution() || op.Head().HasResult();
        } else {
            return op.StartsExecution() || op.HasResult();
        }
    }

    static THashSet<TStringBuf> OPS_WITH_SORTED_OUTPUT;

    void CollectForCombine(const TYtOutput& output, TNodeSet& toCombine, TNodeSet& neverCombine)
    {
        if (neverCombine.find(output.Raw()) != neverCombine.end()) {
            return;
        }

        const auto op = GetOutputOp(output);

        if (auto maybeMerge = op.Maybe<TYtMerge>()) {
            auto merge = maybeMerge.Cast();

            if (NYql::HasSetting(merge.Settings().Ref(), EYtSettingType::CombineChunks)) {

                // inputs of combining merge should never be combined
                for (auto section: merge.Input()) {
                    for (auto path: section.Paths()) {
                        if (auto maybeOut = path.Table().Maybe<TYtOutput>()) {
                            auto out = maybeOut.Cast();
                            neverCombine.insert(out.Raw());
                        }
                    }
                }
                return;
            }
        }

        if (!op.Ref().HasResult()) {
            return;
        }

        auto outTable = GetOutTable(output).Cast<TYtOutTable>();
        auto tableInfo = TYtTableBaseInfo::Parse(outTable);

        TMaybe<ui64> maybeLimit = State_->Configuration->MinTempAvgChunkSize.Get();

        if (!maybeLimit.Defined()) {
            return;
        }

        ui64 limit = *maybeLimit.Get();

        if (limit == 0) {
            // always combine
            toCombine.insert(output.Raw());
            return;

        }

        YQL_ENSURE(tableInfo->Stat);
        ui64 chunksCount = tableInfo->Stat->ChunkCount;
        ui64 dataSize = tableInfo->Stat->DataSize;
        if (chunksCount > 1 && dataSize > chunksCount) {
            ui64 chunkSize = dataSize / chunksCount;

            if (chunkSize < limit) {
                toCombine.insert(output.Raw());
            }
        }
    }

    bool IsChunkCombiningEnabled()
    {
        return State_->Configuration->MinTempAvgChunkSize.Get().Defined();
    }

    TStatus AddChunkCombining(TExprNode::TPtr input, TExprNode::TPtr& output, const TNodeSet& toCombine,
        const TNodeSet& lefts, const TOpDeps& opDeps, TExprContext& ctx)
    {
        TNodeOnNodeOwnedMap replaces;
        TNodeOnNodeOwnedMap newOps; // Old output -> new op
        for (auto node: toCombine) {
            TYtOutput ytOutput(node);
            const auto oldOp = GetOutputOp(ytOutput);

            auto combiningOp =
                Build<TYtMerge>(ctx, oldOp.Pos())
                    .World<TCoWorld>().Build()
                    .DataSink(oldOp.DataSink())
                    .Output()
                        .Add()
                            .InitFrom(GetOutTable(ytOutput).Cast<TYtOutTable>())
                            .Name().Value("").Build()
                            .Stat<TCoVoid>().Build()
                        .Build()
                    .Build()
                    .Input()
                        .Add()
                            .Paths()
                                .Add()
                                    .Table<TYtOutput>() // clone to exclude infinitive loop in RemapExpr
                                        .InitFrom(ytOutput)
                                    .Build()
                                    .Columns<TCoVoid>().Build()
                                    .Ranges<TCoVoid>().Build()
                                    .Stat<TCoVoid>().Build()
                                .Build()
                            .Build()
                            .Settings<TCoNameValueTupleList>()
                            .Build()
                        .Build()
                    .Build()
                    .Settings()
                        .Add()
                            .Name().Value(ToString(EYtSettingType::CombineChunks)).Build()
                        .Build()
                    .Build()
                .Done();

            auto newYtOutput =
                Build<TYtOutput>(ctx, ytOutput.Pos())
                    .Operation(combiningOp)
                    .OutIndex().Value("0").Build()
                .Done();

            replaces[node] = newYtOutput.Ptr();
            newOps[node] = combiningOp.Ptr();
        }

        if (!newOps.empty()) {
            for (auto node: lefts) {
                TCoLeft left(node);

                if (auto maybeOp = left.Input().Maybe<TYtOutputOpBase>()) {
                    auto op = maybeOp.Cast();
                    auto depsIt = opDeps.find(op.Raw());
                    if (depsIt != opDeps.end()) {
                        TExprNode::TListType toSync;
                        for (const auto& dep : depsIt->second) {
                            const TExprNode* oldOutput = std::get<2>(dep);
                            auto it = newOps.find(oldOutput);
                            if (it != newOps.end()) {
                                auto world = ctx.NewCallable(left.Pos(), TCoLeft::CallableName(), { it->second });
                                toSync.push_back(world);
                            }
                        }
                        if (!toSync.empty()) {
                            auto newLeft = ctx.NewCallable(left.Pos(), TCoSync::CallableName(), std::move(toSync));
                            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-AddChunkCombining-Worlds";
                            replaces[node] = newLeft;
                        }
                    }
                }
            }
        }

        if (!replaces.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-AddChunkCombining";
            return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(State_->Types));
        }
        return TStatus::Ok;
    }

    TStatus OptimizeFieldSubsetForMultiUsage(TExprNode::TPtr input, TExprNode::TPtr& output, const TOpDeps& opDeps, const TNodeSet& lefts, TExprContext& ctx) {
        TVector<std::pair<const TOpDeps::value_type*, THashSet<TString>>> matchedOps;
        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
        for (auto& x: opDeps) {
            auto writer = x.first;

            if (!ProcessedFieldSubsetForMultiUsage.insert(writer->UniqueId()).second) {
                continue;
            }

            if (IsBeingExecuted(*writer)) {
                continue;
            }
            if (!TYtMap::Match(writer) && !TYtMerge::Match(writer)) {
                continue;
            }
            if (writer->GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1]->Cast<TListExprType>()->GetItemType()->GetKind() == ETypeAnnotationKind::Variant) {
                // Operation with multi-output
                continue;
            }
            if (NYql::HasSetting(*writer->Child(TYtTransientOpBase::idx_Settings), EYtSettingType::SortLimitBy)) {
                continue;
            }

            auto outTable = TYtTransientOpBase(writer).Output().Item(0);
            const TYqlRowSpecInfo rowSpec(outTable.RowSpec());
            if (rowSpec.HasAuxColumns()) {
                continue;
            }

            auto type = outTable.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

            bool good = true;
            THashSet<TString> usedColumns;
            if (NYql::HasSetting(*writer->Child(TYtTransientOpBase::idx_Settings), EYtSettingType::KeepSorted)) {
                for (size_t i = 0; i < rowSpec.SortedBy.size(); ++i) {
                    usedColumns.insert(rowSpec.SortedBy[i]);
                }
            }

            for (auto& item: x.second) {
                if (auto rawSection = std::get<1>(item)) {
                    if (HasNonEmptyKeyFilter(TYtSection(rawSection))) {
                        // wait until key filter values are calculated and pushed to Path/Ranges
                        good = false;
                        break;
                    }
                }
                auto rawPath = std::get<3>(item);
                if (!rawPath) {
                    if (TYtLength::Match(std::get<0>(item))) {
                        continue;
                    }
                    good = false;
                    break;
                }
                auto path = TYtPath(rawPath);

                auto columns = TYtColumnsInfo(path.Columns());
                if (!columns.HasColumns()) {
                    good = false;
                    break;
                }

                if (type->GetSize() <= columns.GetColumns()->size()) {
                    good = false;
                    break;
                }
                std::transform(columns.GetColumns()->cbegin(), columns.GetColumns()->cend(),
                    std::inserter(usedColumns, usedColumns.end()),
                    [] (const TYtColumnsInfo::TColumn& c) { return c.Name; }
                );

                if (!path.Ranges().Maybe<TCoVoid>()) {
                    // add columns which are implicitly used by path.Ranges(), but not included in path.Columns();
                    const auto ranges = TYtRangesInfo(path.Ranges());
                    const size_t usedKeyPrefix = ranges.GetUsedKeyPrefixLength();
                    YQL_ENSURE(usedKeyPrefix <= rowSpec.SortedBy.size());
                    for (size_t i = 0; i < usedKeyPrefix; ++i) {
                        usedColumns.insert(rowSpec.SortedBy[i]);
                    }
                }

                if (type->GetSize() <= usedColumns.size()) {
                    good = false;
                    break;
                }
            }

            if (good && usedColumns.size() < type->GetSize()) {
                matchedOps.emplace_back(&x, std::move(usedColumns));
            }
        }

        if (matchedOps.empty()) {
            return TStatus::Ok;
        }

        TNodeOnNodeOwnedMap replaces;
        TNodeOnNodeOwnedMap newOps;
        for (auto& item: matchedOps) {
            auto writer = item.first->first;
            auto& columns = item.second;
            auto outTable = TYtTransientOpBase(writer).Output().Item(0);
            auto type = outTable.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

            TVector<const TItemExprType*> structItems;
            for (auto& column: columns) {
                auto pos = type->FindItem(column);
                YQL_ENSURE(pos);
                structItems.push_back(type->GetItems()[*pos]);
            }
            auto outStructType = ctx.MakeType<TStructExprType>(structItems);
            auto distinct = outTable.Ref().GetConstraint<TDistinctConstraintNode>();
            if (distinct) {
                distinct = distinct->FilterFields(ctx, [&columns](const TPartOfConstraintBase::TPathType& path) { return !path.empty() && columns.contains(path.front()); });
            }

            TExprNode::TPtr newOp;
            if (auto maybeMap = TMaybeNode<TYtMap>(writer)) {
                TYtMap map = maybeMap.Cast();

                auto mapper = ctx.Builder(map.Mapper().Pos())
                    .Lambda()
                        .Param("stream")
                        .Callable(NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Ordered) ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                            .Apply(0, map.Mapper().Ref())
                                .With(0, "stream")
                            .Seal()
                            .Lambda(1)
                                .Param("item")
                                .Callable(TCoJust::CallableName())
                                    .Callable(0, TCoAsStruct::CallableName())
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            size_t index = 0;
                                            for (auto& column: columns) {
                                                parent
                                                    .List(index++)
                                                        .Atom(0, column)
                                                        .Callable(1, TCoMember::CallableName())
                                                            .Arg(0, "item")
                                                            .Atom(1, column)
                                                        .Seal()
                                                    .Seal();
                                            }
                                            return parent;
                                        })
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                    .Build();

                TYtOutTableInfo mapOut(outStructType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);

                if (ctx.IsConstraintEnabled<TSortedConstraintNode>()) {
                    if (auto sorted = outTable.Ref().GetConstraint<TSortedConstraintNode>()) {
                        auto prefixLength = sorted->GetContent().size();
                        for (size_t i = 0; i < prefixLength; ++i) {
                            bool found = false;
                            for (const auto& path : sorted->GetContent()[i].first)
                                if (found = path.size() == 1U && columns.contains(path.front()))
                                    break;

                            if (!found)
                                prefixLength = i;
                        }

                        if (sorted = sorted->CutPrefix(prefixLength, ctx)) {
                            if (sorted = sorted->FilterFields(ctx, [&columns](const TPartOfConstraintBase::TPathType& path) { return !path.empty() && columns.contains(path.front()); })) {
                                TKeySelectorBuilder builder(map.Mapper().Pos(), ctx, useNativeDescSort, outStructType);
                                builder.ProcessConstraint(*sorted);
                                builder.FillRowSpecSort(*mapOut.RowSpec);

                                if (builder.NeedMap()) {
                                    mapper = ctx.Builder(map.Mapper().Pos())
                                        .Lambda()
                                            .Param("stream")
                                            .Apply(builder.MakeRemapLambda(true))
                                                .With(0)
                                                    .Apply(mapper)
                                                        .With(0, "stream")
                                                    .Seal()
                                                .Done()
                                            .Seal()
                                        .Seal()
                                        .Build();
                                }
                            }
                        }
                    }
                } else {
                    mapOut.RowSpec->CopySortness(ctx, TYqlRowSpecInfo(outTable.RowSpec()));
                }
                mapOut.SetUnique(distinct, map.Mapper().Pos(), ctx);
                mapOut.RowSpec->SetConstraints(outTable.Ref().GetConstraintSet());

                newOp = Build<TYtMap>(ctx, map.Pos())
                    .InitFrom(map)
                    .Output()
                        .Add(mapOut.ToExprNode(ctx, map.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Mapper(std::move(mapper))
                    .Done().Ptr();
            }
            else  {
                auto merge = TYtMerge(writer);
                auto prevRowSpec = TYqlRowSpecInfo(merge.Output().Item(0).RowSpec());
                TYtOutTableInfo mergeOut(outStructType, prevRowSpec.GetNativeYtTypeFlags());
                mergeOut.RowSpec->CopySortness(ctx, prevRowSpec, TYqlRowSpecInfo::ECopySort::WithDesc);
                mergeOut.SetUnique(distinct, merge.Pos(), ctx);
                mergeOut.RowSpec->SetConstraints(outTable.Ref().GetConstraintSet());

                if (auto nativeType = prevRowSpec.GetNativeYtType()) {
                    mergeOut.RowSpec->CopyTypeOrders(*nativeType);
                }

                TSet<TStringBuf> columnSet;
                for (auto& column: columns) {
                    columnSet.insert(column);
                }
                if (mergeOut.RowSpec->HasAuxColumns()) {
                    for (auto item: mergeOut.RowSpec->GetAuxColumns()) {
                        columnSet.insert(item.first);
                    }
                }

                newOp = Build<TYtMerge>(ctx, merge.Pos())
                    .InitFrom(merge)
                    .Input()
                        .Add(UpdateInputFields(merge.Input().Item(0), std::move(columnSet), ctx, false))
                    .Build()
                    .Output()
                        .Add(mergeOut.ToExprNode(ctx, merge.Pos()).Cast<TYtOutTable>())
                    .Build()
                    .Done().Ptr();
            }

            newOps[writer] = newOp;
            for (auto& reader: item.first->second) {
                if (TYtLength::Match(std::get<0>(reader))) {
                    auto rawLen = std::get<0>(reader);
                    auto len = TYtLength(rawLen);
                    replaces[rawLen] = Build<TYtLength>(ctx, len.Pos())
                        .InitFrom(len)
                        .Input(ctx.ChangeChild(len.Input().Ref(), TYtOutput::idx_Operation, TExprNode::TPtr(newOp)))
                        .Done().Ptr();
                } else {
                    auto rawPath = std::get<3>(reader);
                    auto path = TYtPath(rawPath);
                    replaces[rawPath] = Build<TYtPath>(ctx, path.Pos())
                        .InitFrom(path)
                        .Table(ctx.ChangeChild(path.Table().Ref(), TYtOutput::idx_Operation, TExprNode::TPtr(newOp)))
                        .Done().Ptr();
                }
            }
        }
        if (!lefts.empty() && !newOps.empty()) {
            for (auto node: lefts) {
                TCoLeft left(node);
                auto newIt = newOps.find(left.Input().Raw());
                if (newIt != newOps.end()) {
                    replaces[node] = ctx.ChangeChild(*node, TCoLeft::idx_Input, TExprNode::TPtr(newIt->second));
                }
            }
        }

        if (!replaces.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-FieldSubsetForMultiUsage";
            return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings{State_->Types});
        }
        return TStatus::Ok;
    }

    TStatus OptimizeUnorderedOuts(TExprNode::TPtr input, TExprNode::TPtr& output, const std::vector<const TExprNode*>& opDepsOrder, const TOpDeps& opDeps, const TNodeSet& lefts, TExprContext& ctx) {
        std::vector<const TExprNode*> matchedOps;
        for (auto writer: opDepsOrder) {
            if (!TYtEquiJoin::Match(writer) && !IsBeingExecuted(*writer)) {
                matchedOps.push_back(writer);
            }
        }

        if (matchedOps.empty()) {
            return TStatus::Ok;
        }

        TNodeOnNodeOwnedMap replaces;
        TNodeOnNodeOwnedMap newOps;
        for (auto writer: matchedOps) {
            TDynBitMap orderedOuts;
            TDynBitMap unorderedOuts;
            const auto& readers = opDeps.at(writer);
            for (auto& item: readers) {
                auto out = TYtOutput(std::get<2>(item));
                if (IsUnorderedOutput(out)) {
                    unorderedOuts.Set(FromString<size_t>(out.OutIndex().Value()));
                } else {
                    orderedOuts.Set(FromString<size_t>(out.OutIndex().Value()));
                }
            }
            if (!unorderedOuts.Empty()) {
                if (orderedOuts.Empty() || !writer->IsCallable(OPS_WITH_SORTED_OUTPUT)) {
                    TExprNode::TPtr newOp;
                    if (const auto mayTry = TExprBase(writer).Maybe<TYtTryFirst>()) {
                        TExprNode::TPtr newOpFirst = MakeUnorderedOp(mayTry.Cast().First().Ref(), unorderedOuts, ctx);
                        TExprNode::TPtr newOpSecond = MakeUnorderedOp(mayTry.Cast().Second().Ref(), unorderedOuts, ctx);
                        if (newOpFirst || newOpSecond) {
                            newOp = Build<TYtTryFirst>(ctx, writer->Pos())
                                .First(newOpFirst ? std::move(newOpFirst) : mayTry.Cast().First().Ptr())
                                .Second(newOpSecond ? std::move(newOpSecond) : mayTry.Cast().Second().Ptr())
                                .Done().Ptr();
                        }
                    } else {
                        newOp = MakeUnorderedOp(*writer, unorderedOuts, ctx);
                    }
                    if (newOp) {
                        newOps[writer] = newOp;
                    }
                    for (auto& item: readers) {
                        auto out = std::get<2>(item);
                        replaces[out] = Build<TYtOutput>(ctx, out->Pos())
                            .Operation(newOp ? newOp : out->ChildPtr(TYtOutput::idx_Operation))
                            .OutIndex(out->ChildPtr(TYtOutput::idx_OutIndex))
                            .Done().Ptr();
                    }
                }
            }
        }
        if (!lefts.empty() && !newOps.empty()) {
            for (auto node: lefts) {
                TCoLeft left(node);
                auto newIt = newOps.find(left.Input().Raw());
                if (newIt != newOps.end()) {
                    replaces[node] = ctx.ChangeChild(*node, TCoLeft::idx_Input, TExprNode::TPtr(newIt->second));
                }
            }
        }

        if (!replaces.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-UnorderedOuts";
            return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings{State_->Types});
        }
        return TStatus::Ok;
    }

    TExprNode::TPtr MakeUnorderedOp(const TExprNode& node, const TDynBitMap& unorderedOuts, TExprContext& ctx) const {
        if (!node.IsCallable(OPS_WITH_SORTED_OUTPUT)) {
            return {};
        }
        auto op = TYtOutputOpBase(&node);

        bool hasOtherSortedOuts = false;
        bool changedOutSort = false;
        TVector<TYtOutTable> outTables;
        TExprNode::TListType filterColumns(op.Output().Size());
        for (size_t i = 0; i < op.Output().Size(); ++i) {
            auto out = op.Output().Item(i);
            if (unorderedOuts.Test(i)) {
                auto rowSpec = TYtTableBaseInfo::GetRowSpec(out);
                YQL_ENSURE(rowSpec);
                if (rowSpec->IsSorted()) {
                    if (rowSpec->HasAuxColumns()) {
                        TVector<TString> columns;
                        for (auto item: rowSpec->GetType()->GetItems()) {
                            columns.emplace_back(item->GetName());
                        }
                        filterColumns[i] = ToAtomList(columns, node.Pos(), ctx);
                    }
                    rowSpec->ClearSortness(ctx);
                    outTables.push_back(TYtOutTable(ctx.ChangeChild(out.Ref(), TYtOutTable::idx_RowSpec, rowSpec->ToExprNode(ctx, out.Pos()).Ptr())));
                    changedOutSort = true;
                } else {
                    outTables.push_back(out);
                }
            } else {
                if (TYtTableBaseInfo::GetRowSpec(out)->IsSorted()) {
                    hasOtherSortedOuts = true;
                }
                outTables.push_back(out);
            }
        }

        bool isFill = false;
        int lambdaIdx = -1;
        TExprNode::TPtr lambda;
        if (TYtMap::Match(&node)) {
            lambdaIdx = TYtMap::idx_Mapper;
        } else if (TYtReduce::Match(&node)) {
            lambdaIdx = TYtReduce::idx_Reducer;
        } else if (TYtFill::Match(&node)) {
            lambdaIdx = TYtFill::idx_Content;
            isFill = true;
        }
        if (-1 != lambdaIdx && !hasOtherSortedOuts) {
            if (isFill) {
                if (node.ChildPtr(lambdaIdx)->GetConstraint<TSortedConstraintNode>()) {
                    lambda = Build<TCoLambda>(ctx, node.ChildPtr(lambdaIdx)->Pos())
                        .Args({})
                        .Body<TCoUnordered>()
                            .Input<TExprApplier>()
                                .Apply(TCoLambda(node.ChildPtr(lambdaIdx)))
                            .Build()
                        .Build()
                        .Done().Ptr();
                }
            } else {
                TProcessedNodesSet processedNodes;
                TNodeOnNodeOwnedMap remaps;
                VisitExpr(node.ChildPtr(lambdaIdx), [&processedNodes, &remaps, &ctx](const TExprNode::TPtr& node) {
                    if (TYtOutput::Match(node.Get())) {
                        // Stop traversing dependent operations
                        processedNodes.insert(node->UniqueId());
                        return false;
                    }
                    auto name = node->Content();
                    if (node->IsCallable() && node->ChildrenSize() > 0 && name.SkipPrefix("Ordered")) {
                        const auto inputKind = node->Child(0)->GetTypeAnn()->GetKind();
                        if (inputKind == ETypeAnnotationKind::Stream || inputKind == ETypeAnnotationKind::Flow) {
                            remaps[node.Get()] = ctx.RenameNode(*node, name);
                        }
                    }
                    return true;
                });
                if (!remaps.empty()) {
                    TOptimizeExprSettings settings{State_->Types};
                    settings.ProcessedNodes = &processedNodes;
                    auto status = RemapExpr(node.ChildPtr(lambdaIdx), lambda, remaps, ctx, settings);
                    if (status.Level == IGraphTransformer::TStatus::Error) {
                        return {};
                    }
                }
            }
        }

        TExprNode::TPtr res;
        if (changedOutSort) {
            res = ctx.ChangeChild(node, TYtOutputOpBase::idx_Output,
                Build<TYtOutSection>(ctx, op.Pos()).Add(outTables).Done().Ptr());

            if (TYtSort::Match(&node)) {
                res = ctx.RenameNode(*res, TYtMerge::CallableName());
            }

            if (lambdaIdx != -1 && AnyOf(filterColumns, [](const TExprNode::TPtr& p) { return !!p; })) {
                if (!lambda) {
                    lambda = node.ChildPtr(lambdaIdx);
                }
                if (op.Output().Size() == 1) {
                    lambda = Build<TCoLambda>(ctx, lambda->Pos())
                        .Args({"stream"})
                        .Body<TCoExtractMembers>()
                            .Input<TExprApplier>()
                                .Apply(TCoLambda(lambda))
                                .With(0, "stream")
                            .Build()
                            .Members(filterColumns[0])
                        .Build()
                        .Done().Ptr();
                } else {
                    auto varType = ExpandType(lambda->Pos(), *GetSeqItemType(node.Child(lambdaIdx)->GetTypeAnn()), ctx);
                    TVector<TExprBase> visitArgs;
                    for (size_t i = 0; i < op.Output().Size(); ++i) {
                        visitArgs.push_back(Build<TCoAtom>(ctx, lambda->Pos()).Value(ToString(i)).Done());
                        if (filterColumns[i]) {
                            visitArgs.push_back(Build<TCoLambda>(ctx, lambda->Pos())
                                .Args({"row"})
                                .Body<TCoVariant>()
                                    .Item<TCoUnwrap>()
                                        .Optional<TCoExtractMembers>()
                                            .Input<TCoJust>()
                                                .Input("row")
                                            .Build()
                                            .Members(filterColumns[i])
                                        .Build()
                                    .Build()
                                    .Index()
                                        .Value(ToString(i))
                                    .Build()
                                    .VarType(varType)
                                .Build()
                                .Done());
                        } else {
                            visitArgs.push_back(Build<TCoLambda>(ctx, lambda->Pos())
                                .Args({"row"})
                                .Body<TCoVariant>()
                                    .Item("row")
                                    .Index()
                                        .Value(ToString(i))
                                    .Build()
                                    .VarType(varType)
                                .Build()
                                .Done());
                        }
                    }

                    lambda = Build<TCoLambda>(ctx, lambda->Pos())
                        .Args({"stream"})
                        .Body<TCoFlatMapBase>()
                            .CallableName(hasOtherSortedOuts ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                            .Input<TExprApplier>()
                                .Apply(TCoLambda(lambda))
                                .With(0, "stream")
                            .Build()
                            .Lambda()
                                .Args({"var"})
                                .Body<TCoJust>()
                                    .Input<TCoVisit>()
                                        .Input("var")
                                        .FreeArgs()
                                            .Add(visitArgs)
                                        .Build()
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                        .Done().Ptr();
                }
            }
        }

        if (lambda) {
            res = ctx.ChangeChild(res ? *res : node, lambdaIdx, std::move(lambda));
        }

        if (op.Maybe<TYtTransientOpBase>()) {
            auto trOp = TYtTransientOpBase(&node);
            if (!hasOtherSortedOuts && NYql::HasSetting(trOp.Settings().Ref(), EYtSettingType::Ordered)) {
                res = ctx.ChangeChild(res ? *res : node, TYtTransientOpBase::idx_Settings,
                    NYql::RemoveSetting(trOp.Settings().Ref(), EYtSettingType::Ordered, ctx));
            }

            if (TYtMap::Match(&node)) {
                Fill(filterColumns.begin(), filterColumns.end(), TExprNode::TPtr());
                filterColumns.resize(trOp.Input().Size());
            }

            if (!TYtReduce::Match(&node)) {
                // Push Unordered and columns to operation inputs
                bool changedInput = false;
                TVector<TYtSection> updatedSections;
                for (size_t i = 0; i < trOp.Input().Size(); ++i) {
                    auto section = trOp.Input().Item(i);
                    if (!hasOtherSortedOuts) {
                        section = MakeUnorderedSection(section, ctx);
                        if (section.Raw() != trOp.Input().Item(i).Raw()) {
                            changedInput = true;
                        }
                    }
                    if (filterColumns[i]) {
                        section = UpdateInputFields(section, TExprBase(filterColumns[i]), ctx);
                        changedInput = true;
                    }
                    updatedSections.push_back(section);
                }

                if (changedInput) {
                    res = ctx.ChangeChild(res ? *res : node, TYtTransientOpBase::idx_Input,
                        Build<TYtSectionList>(ctx, trOp.Pos()).Add(updatedSections).Done().Ptr());
                }
            }
        }

        return res;
    }

    TStatus SplitLargeInputs(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx, bool splitMap) {
        auto maxTables = State_->Configuration->MaxInputTables.Get();
        auto maxSortedTables = State_->Configuration->MaxInputTablesForSortedMerge.Get();

        TOptimizeExprSettings settings(State_->Types);
        settings.ProcessedNodes = &ProcessedSplitLargeInputs;

        return OptimizeExpr(input, output, [maxTables, maxSortedTables, splitMap, this](const TExprNode::TPtr& node, TExprContext& ctx) {
            if (TYtTransientOpBase::Match(node.Get()) && !IsBeingExecuted(*node)) {
                auto op = TYtTransientOpBase(node);
                auto outRowSpec = MakeIntrusive<TYqlRowSpecInfo>(op.Output().Item(0).RowSpec());
                const bool sortedMerge = TYtMerge::Match(node.Get()) && outRowSpec->IsSorted();
                const bool keepSort = sortedMerge || TYtReduce::Match(node.Get()) || TYtEquiJoin::Match(node.Get());
                auto limit = maxTables;
                if (maxSortedTables.Defined() && sortedMerge) {
                    limit = maxSortedTables;
                }

                if (limit) {
                    if (splitMap && TYtMap::Match(node.Get())
                        && op.Input().Size() == 1 && op.Output().Size() == 1
                        && !NYql::HasAnySetting(op.Settings().Ref(), EYtSettingType::SortLimitBy | EYtSettingType::Sharded | EYtSettingType::JobCount | EYtSettingType::Ordered | EYtSettingType::KeepSorted)
                        && op.Input().Item(0).Paths().Size() > *limit
                        && !NYql::HasAnySetting(op.Input().Item(0).Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip))
                    {
                        auto section = op.Input().Item(0);
                        TVector<TYtPath> newPaths;
                        TVector<TYtPath> paths;
                        TVector<TYtPath> prevPaths(section.Paths().begin(), section.Paths().end());
                        while (!prevPaths.empty()) {
                            size_t count = Min<size_t>(*limit, prevPaths.size());
                            YQL_ENSURE(count > 0);

                            newPaths.push_back(
                                Build<TYtPath>(ctx, op.Pos())
                                    .Table<TYtOutput>()
                                        .Operation<TYtMap>()
                                            .InitFrom(op.Cast<TYtMap>())
                                            .Input()
                                                .Add()
                                                    .Paths()
                                                        .Add(TVector<TYtPath>(prevPaths.begin(), prevPaths.begin() + count))
                                                    .Build()
                                                    .Settings(section.Settings())
                                                .Build()
                                            .Build()
                                        .Build()
                                        .OutIndex().Value("0").Build()
                                    .Build()
                                    .Columns<TCoVoid>().Build()
                                    .Ranges<TCoVoid>().Build()
                                    .Stat<TCoVoid>().Build()
                                .Done()
                            );
                            prevPaths.erase(prevPaths.begin(), prevPaths.begin() + count);
                        }

                        YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-SplitLargeMapInputs";
                        return Build<TYtMerge>(ctx, op.Pos())
                            .World<TCoWorld>().Build()
                            .DataSink(op.DataSink())
                            .Input()
                                .Add()
                                    .Paths()
                                        .Add(newPaths)
                                    .Build()
                                    .Settings()
                                    .Build()
                                .Build()
                            .Build()
                            .Output()
                                .Add(op.Output().Item(0))
                            .Build()
                            .Settings(NYql::KeepOnlySettings(op.Settings().Ref(), EYtSettingType::Limit, ctx))
                            .Done().Ptr();
                    }

                    TVector<TYtSection> updatedSections;
                    bool hasUpdates = false;
                    for (auto section: op.Input()) {
                        const EYtSettingType kfType = NYql::HasSetting(section.Settings().Ref(), EYtSettingType::KeyFilter2) ?
                            EYtSettingType::KeyFilter2 : EYtSettingType::KeyFilter;
                        const auto keyFiltersValues = NYql::GetAllSettingValues(section.Settings().Ref(), kfType);
                        const bool hasKeyFilters = AnyOf(keyFiltersValues,
                            [](const TExprNode::TPtr& keyFilter) { return keyFilter->ChildrenSize() > 0; });
                        const bool hasTableKeyFilters = AnyOf(keyFiltersValues,
                            [kfType](const TExprNode::TPtr& keyFilter) {
                                return keyFilter->ChildrenSize() >= GetMinChildrenForIndexedKeyFilter(kfType);
                            });

                        if (hasTableKeyFilters) {
                            // Cannot optimize it
                            updatedSections.push_back(section);
                        } else if (section.Paths().Size() > *limit) {
                            auto scheme = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                            TVector<TYtPath> paths;
                            TVector<TYtPath> prevPaths(section.Paths().begin(), section.Paths().end());
                            const size_t min = NYql::HasSetting(section.Settings().Ref(), EYtSettingType::SysColumns) ? 0 : 1;
                            while (prevPaths.size() > min) {
                                size_t count = Min<size_t>(*limit, prevPaths.size());
                                YQL_ENSURE(count > 0);

                                auto path = CopyOrTrivialMap(section.Pos(),
                                    op.World(), op.DataSink(),
                                    *scheme,
                                    Build<TYtSection>(ctx, section.Pos())
                                        .Paths()
                                            .Add(TVector<TYtPath>(prevPaths.begin(), prevPaths.begin() + count))
                                        .Build()
                                        .Settings(NYql::KeepOnlySettings(section.Settings().Ref(), EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::SysColumns, ctx))
                                        .Done(),
                                    outRowSpec,
                                    ctx, State_,
                                    TCopyOrTrivialMapOpts().SetTryKeepSortness(keepSort).SetRangesResetSort(false));

                                paths.push_back(path);
                                prevPaths.erase(prevPaths.begin(), prevPaths.begin() + count);
                            }
                            auto settings = section.Settings().Ptr();
                            if (!prevPaths.empty()) {
                                if (hasKeyFilters) {
                                    // Modify keyFilters to point to remaining tables only
                                    const size_t from = paths.size();
                                    const size_t to = from + prevPaths.size();
                                    auto keyFiltersValues = NYql::GetAllSettingValues(section.Settings().Ref(), EYtSettingType::KeyFilter);
                                    settings = NYql::RemoveSetting(*settings, EYtSettingType::KeyFilter, ctx);
                                    for (auto val: keyFiltersValues) {
                                        for (size_t i = from; i < to; ++i) {
                                            auto children = val->ChildrenList();
                                            children.push_back(ctx.NewAtom(section.Pos(), ToString(i), TNodeFlags::Default));
                                            settings = NYql::AddSetting(*settings, EYtSettingType::KeyFilter,
                                                ctx.NewList(section.Pos(), std::move(children)), ctx);
                                        }
                                    }
                                    keyFiltersValues = NYql::GetAllSettingValues(section.Settings().Ref(), EYtSettingType::KeyFilter2);
                                    settings = NYql::RemoveSetting(*settings, EYtSettingType::KeyFilter2, ctx);
                                    for (auto val: keyFiltersValues) {
                                        TExprNode::TListType indicies;
                                        for (size_t i = from; i < to; ++i) {
                                            indicies.push_back(ctx.NewAtom(section.Pos(), ToString(i), TNodeFlags::Default));
                                        }
                                        auto children = val->ChildrenList();
                                        children.push_back(ctx.NewList(section.Pos(), std::move(indicies)));
                                        settings = NYql::AddSetting(*settings, EYtSettingType::KeyFilter2,
                                            ctx.NewList(section.Pos(), std::move(children)), ctx);
                                    }
                                }
                                paths.insert(paths.end(), prevPaths.begin(), prevPaths.end());
                            } else {
                                // All keyFilters are pushed to children operations. Can remove them from the current
                                if (hasKeyFilters) {
                                    settings = NYql::RemoveSettings(*settings, EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2, ctx);
                                }
                            }
                            updatedSections.push_back(Build<TYtSection>(ctx, section.Pos())
                                .InitFrom(section)
                                .Paths()
                                    .Add(paths)
                                .Build()
                                .Settings(settings)
                                .Done());
                            hasUpdates = true;
                        } else {
                            updatedSections.push_back(section);
                        }
                    }
                    if (hasUpdates) {
                        YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-SplitLargeInputs";
                        return ctx.ChangeChild(*node, TYtTransientOpBase::idx_Input,
                            Build<TYtSectionList>(ctx, op.Input().Pos()).Add(updatedSections).Done().Ptr());
                    }
                }
            }
            return node;
        }, ctx, settings);
    }

    static TExprNode::TPtr RebuildLambdaWithLessOuts(TExprNode::TPtr lambda, bool withArg, bool ordered, const TDynBitMap& usedOuts,
        const TYtOutSection& outs, TVector<TYtOutTable>& resTables, TExprContext& ctx)
    {
        if (usedOuts.Count() == 1) {
            auto ndx = usedOuts.FirstNonZeroBit();
            resTables.push_back(outs.Item(ndx));
            if (withArg) {
                auto oldLambda = TCoLambda(lambda);
                lambda = Build<TCoLambda>(ctx, lambda->Pos())
                    .Args({"stream"})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .template Input<TExprApplier>()
                            .Apply(oldLambda)
                            .With(oldLambda.Args().Arg(0), "stream")
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .template Body<TCoGuess>()
                                .Variant("item")
                                .Index()
                                    .Value(ToString(ndx))
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
            else {
                lambda = Build<TCoLambda>(ctx, lambda->Pos())
                    .Args({})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .template Input<TExprApplier>()
                            .Apply(TCoLambda(lambda))
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .template Body<TCoGuess>()
                                .Variant("item")
                                .Index()
                                    .Value(ToString(ndx))
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
        }
        else {
            TVector<TExprBase> tupleTypes;
            for (size_t i = 0; i < outs.Size(); ++i) {
                if (usedOuts.Test(i)) {
                    auto out = outs.Item(i);
                    tupleTypes.push_back(TExprBase(ExpandType(out.Pos(), *out.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType(), ctx)));
                }
            }
            TExprBase varType = Build<TCoVariantType>(ctx, lambda->Pos())
                .UnderlyingType<TCoTupleType>()
                    .Add(tupleTypes)
                .Build()
                .Done();

            TVector<TExprBase> visitArgs;
            size_t nextVarIndex = 0;
            for (size_t i = 0; i < outs.Size(); ++i) {
                if (usedOuts.Test(i)) {
                    visitArgs.push_back(Build<TCoAtom>(ctx, lambda->Pos()).Value(ToString(i)).Done());
                    visitArgs.push_back(Build<TCoLambda>(ctx, lambda->Pos())
                        .Args({"item"})
                        .template Body<TCoJust>()
                            .template Input<TCoVariant>()
                                .Item("item")
                                .Index()
                                    .Value(ToString(nextVarIndex++))
                                .Build()
                                .VarType(varType)
                            .Build()
                        .Build()
                        .Done()
                    );
                    resTables.push_back(outs.Item(i));
                }
            }
            visitArgs.push_back(Build<TCoNothing>(ctx, lambda->Pos())
                .template OptionalType<TCoOptionalType>()
                    .ItemType(varType)
                .Build()
                .Done());

            if (withArg) {
                auto oldLambda = TCoLambda(lambda);
                lambda = Build<TCoLambda>(ctx, lambda->Pos())
                    .Args({"stream"})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .template Input<TExprApplier>()
                            .Apply(oldLambda)
                            .With(oldLambda.Args().Arg(0), "stream")
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .template Body<TCoVisit>()
                                .Input("item")
                                .FreeArgs()
                                    .Add(visitArgs)
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
            else {
                lambda = Build<TCoLambda>(ctx, lambda->Pos())
                    .Args({})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .template Input<TExprApplier>()
                            .Apply(TCoLambda(lambda))
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .template Body<TCoVisit>()
                                .Input("item")
                                .FreeArgs()
                                    .Add(visitArgs)
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
        }
        return lambda;
    }

    TStatus BypassMergeBeforeLength(TExprNode::TPtr input, TExprNode::TPtr& output, const TOpDeps& opDeps, const TNodeSet& lefts, TExprContext& ctx) {
        TNodeOnNodeOwnedMap replaces;
        for (auto& x: opDeps) {
            if (TYtMerge::Match(x.first) && x.second.size() > 0 && AllOf(x.second, [](const auto& item) { return TYtLength::Match(std::get<0>(item)); } )) {
                auto merge = TYtMerge(x.first);
                if (merge.Ref().HasResult()) {
                    continue;
                }

                if (NYql::HasSetting(merge.Settings().Ref(), EYtSettingType::Limit)) {
                    continue;
                }

                auto section = merge.Input().Item(0);
                if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample)) {
                    continue;
                }
                if (NYql::HasNonEmptyKeyFilter(section)) {
                    continue;
                }

                if (AnyOf(section.Paths(), [](const TYtPath& path) { return !path.Ranges().Maybe<TCoVoid>() || TYtTableBaseInfo::GetMeta(path.Table())->IsDynamic; })) {
                    continue;
                }
                // Dependency on more than 1 operation
                if (1 < Accumulate(section.Paths(), 0ull, [](ui64 val, const TYtPath& path) { return val + path.Table().Maybe<TYtOutput>().IsValid(); })) {
                    continue;
                }

                TSyncMap syncList;
                for (auto path: section.Paths()) {
                    if (auto out = path.Table().Maybe<TYtOutput>()) {
                        syncList.emplace(GetOutputOp(out.Cast()).Ptr(), syncList.size());
                    }
                }
                auto newWorld = ApplySyncListToWorld(merge.World().Ptr(), syncList, ctx);
                for (auto node: lefts) {
                    TCoLeft left(node);
                    if (left.Input().Raw() == merge.Raw()) {
                        replaces[node] = newWorld;
                    }
                }

                for (auto& item : x.second) {
                    auto len = TYtLength(std::get<0>(item));
                    auto out = len.Input().Cast<TYtOutput>();

                    TExprNode::TPtr newLen;
                    if (section.Paths().Size() == 1) {
                        if (auto maybeOp = section.Paths().Item(0).Table().Maybe<TYtOutput>()) {
                            if (IsUnorderedOutput(out)) {
                                newLen = Build<TYtLength>(ctx, len.Pos())
                                    .InitFrom(len)
                                    .Input<TYtOutput>()
                                        .InitFrom(maybeOp.Cast())
                                        .Mode(out.Mode())
                                    .Build()
                                    .Done().Ptr();
                            } else {
                                newLen = Build<TYtLength>(ctx, len.Pos())
                                    .InitFrom(len)
                                    .Input(maybeOp.Cast())
                                    .Done().Ptr();
                            }
                        }
                    }
                    if (!newLen) {
                        newLen = Build<TYtLength>(ctx, len.Pos())
                            .InitFrom(len)
                            .Input<TYtReadTable>()
                                .World(ctx.NewWorld(len.Pos()))
                                .DataSource(GetDataSource(len.Input(), ctx))
                                .Input()
                                    .Add(IsUnorderedOutput(out) ? MakeUnorderedSection(section, ctx) : section)
                                .Build()
                            .Build()
                            .Done().Ptr();
                    }

                    replaces[len.Raw()] = newLen;
                }
            }
        }

        if (!replaces.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-BypassMergeBeforeLength";
            return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(State_->Types));
        }

        return TStatus::Ok;
    }

    TStatus OptimizeUnusedOuts(TExprNode::TPtr input, TExprNode::TPtr& output, const TOpDeps& opDeps, const TNodeSet& lefts, TExprContext& ctx) {
        TNodeOnNodeOwnedMap replaces;
        TNodeOnNodeOwnedMap newOps;
        for (auto& x: opDeps) {
            auto writer = x.first;
            if (const size_t outCount = GetRealOperation(TExprBase(writer)).Output().Size(); outCount > 1 && writer->GetState() != TExprNode::EState::ExecutionComplete
                && writer->GetState() != TExprNode::EState::ExecutionInProgress
                && (!writer->HasResult() || writer->GetResult().Type() != TExprNode::World)
                && ProcessedUnusedOuts.find(writer->UniqueId()) == ProcessedUnusedOuts.end())
            {
                TDynBitMap usedOuts;
                for (auto& item: x.second) {
                    usedOuts.Set(FromString<size_t>(std::get<2>(item)->Child(TYtOutput::idx_OutIndex)->Content()));
                }
                if (!usedOuts.Empty() && usedOuts.Count() < outCount) {
                    TExprNode::TPtr newOp;
                    if (const auto mayTry = TExprBase(writer).Maybe<TYtTryFirst>()) {
                        const auto opSecond = mayTry.Cast().Second().Raw();
                        TExprNode::TPtr newOpSecond = SuppressUnusedOuts(*opSecond, usedOuts, ctx);
                        if (newOpSecond) {
                            newOp = Build<TYtTryFirst>(ctx, writer->Pos())
                                .First(mayTry.Cast().First().Ptr())
                                .Second(std::move(newOpSecond))
                                .Done().Ptr();
                        }
                    } else {
                        newOp = SuppressUnusedOuts(*writer, usedOuts, ctx);
                    }
                    newOps[writer] = newOp;
                    TVector<size_t> remappedIndicies(outCount, Max<size_t>());
                    size_t newIndex = 0;
                    for (size_t i  = 0; i < outCount; ++i) {
                        if (usedOuts.Test(i)) {
                            remappedIndicies[i] = newIndex++;
                        }
                    }
                    for (auto& item: x.second) {
                        auto oldOutput = TYtOutput(std::get<2>(item));
                        auto oldNdx = FromString<size_t>(oldOutput.OutIndex().Value());
                        YQL_ENSURE(oldNdx < remappedIndicies.size());
                        auto newNdx = remappedIndicies[oldNdx];
                        YQL_ENSURE(newNdx != Max<size_t>());
                        replaces[oldOutput.Raw()] = Build<TYtOutput>(ctx, oldOutput.Pos())
                            .Operation(newOp)
                            .OutIndex()
                                .Value(ToString(newNdx))
                            .Build()
                            .Mode(oldOutput.Mode())
                            .Done().Ptr();
                    }
                }
            }
            ProcessedUnusedOuts.insert(writer->UniqueId());
        }
        if (!lefts.empty() && !newOps.empty()) {
            for (auto node: lefts) {
                TCoLeft left(node);
                auto newIt = newOps.find(left.Input().Raw());
                if (newIt != newOps.end()) {
                    replaces[node] = ctx.ChangeChild(*node, TCoLeft::idx_Input, TExprNode::TPtr(newIt->second));
                }
            }
        }

        if (!replaces.empty()) {
            return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(State_->Types));
        }
        return TStatus::Ok;
    }

    TStatus AlignPublishTypes(TExprNode::TPtr input, TExprNode::TPtr& output, const TOpDeps& opDeps, TExprContext& ctx) {
        TNodeOnNodeOwnedMap remap;
        TMap<size_t, size_t> outUsage;
        TNodeMap<TNodeOnNodeOwnedMap> updatePublish;
        const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
        for (auto& x : opDeps) {
            outUsage.clear();
            bool hasPublish = false;
            for (auto& item : x.second) {
                auto reader = std::get<0>(item);
                if (TYtPublish::Match(reader) || TYtCopy::Match(reader) || TYtMerge::Match(reader) || TYtSort::Match(reader)) {
                    const auto opIndex = FromString<size_t>(std::get<2>(item)->Child(TYtOutput::idx_OutIndex)->Content());
                    ++outUsage[opIndex];
                    hasPublish = hasPublish || TYtPublish::Match(reader);
                }
            }
            if (!hasPublish) {
                continue;
            }

            const TYtOutputOpBase operation = GetRealOperation(TExprBase(x.first));
            const bool canUpdateOp = !IsBeingExecuted(*x.first) && !operation.Maybe<TYtCopy>();
            const bool canChangeNativeTypeForOp = !operation.Maybe<TYtMerge>() && !operation.Maybe<TYtSort>();

            auto origOutput = operation.Output().Ptr();
            auto newOutput = origOutput;
            for (auto& item : x.second) {
                auto reader = std::get<0>(item);
                if (auto maybePublish = TMaybeNode<TYtPublish>(reader)) {
                    auto publish = maybePublish.Cast();
                    const auto out = TYtOutput(std::get<2>(item));
                    const size_t opIndex = FromString<size_t>(out.OutIndex().Value());
                    const bool unordered = IsUnorderedOutput(out);
                    TYtTableInfo dstInfo = publish.Publish();
                    const auto& desc = State_->TablesData->GetTable(dstInfo.Cluster, dstInfo.Name, dstInfo.CommitEpoch);
                    if (!desc.RowSpec) {
                        continue;
                    }
                    auto table = operation.Output().Item(opIndex);
                    if (auto outRowSpec = TYtTableBaseInfo::GetRowSpec(table)) {
                        auto mode = NYql::GetSetting(publish.Settings().Ref(), EYtSettingType::Mode);
                        const bool append = mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Append;

                        const bool diffNativeType = desc.RowSpec->GetNativeYtTypeFlags() != outRowSpec->GetNativeYtTypeFlags()
                            || desc.RowSpec->GetNativeYtType() != outRowSpec->GetNativeYtType();
                        const bool diffColumnOrder = State_->Types->OrderedColumns && !append && desc.RowSpec->GetTypeNode() != outRowSpec->GetTypeNode();

                        if (diffNativeType || diffColumnOrder) {
                            outRowSpec->CopyType(*desc.RowSpec);
                            TExprNode::TPtr newTable = ctx.ChangeChild(table.Ref(), TYtOutTable::idx_RowSpec,
                                outRowSpec->ToExprNode(ctx, table.RowSpec().Pos()).Ptr());

                            if (canUpdateOp && outUsage[opIndex] <= 1 && (!diffNativeType || canChangeNativeTypeForOp)) {
                                YQL_CLOG(INFO, ProviderYt) << "AlignPublishTypes: change " << opIndex << " output of " << operation.Ref().Content();
                                newOutput = ctx.ChangeChild(*newOutput, opIndex, std::move(newTable));
                            } else if (diffNativeType) {
                                YQL_CLOG(INFO, ProviderYt) << "AlignPublishTypes: add remap op for " << opIndex << " output of " << operation.Ref().Content();
                                auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, out.Pos());
                                auto mapper = Build<TCoLambda>(ctx, out.Pos())
                                    .Args({"stream"})
                                    .Body("stream")
                                    .Done().Ptr();

                                if (!unordered && outRowSpec->IsSorted()) {
                                    settingsBuilder
                                        .Add()
                                            .Name()
                                                .Value(ToString(EYtSettingType::Ordered), TNodeFlags::Default)
                                            .Build()
                                        .Build();
                                    TKeySelectorBuilder builder(out.Pos(), ctx, useNativeDescSort, outRowSpec->GetType());
                                    builder.ProcessRowSpec(*outRowSpec);
                                    if (builder.NeedMap()) {
                                        mapper = builder.MakeRemapLambda(true);
                                    }
                                }
                                if (State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
                                    settingsBuilder
                                        .Add()
                                            .Name()
                                                .Value(ToString(EYtSettingType::Flow), TNodeFlags::Default)
                                            .Build()
                                        .Build();
                                }

                                updatePublish[reader][out.Raw()] = Build<TYtOutput>(ctx, out.Pos())
                                    .Operation<TYtMap>()
                                        .World<TCoWorld>().Build()
                                        .DataSink(operation.DataSink())
                                        .Input()
                                            .Add()
                                                .Paths()
                                                    .Add()
                                                        .Table<TYtOutput>()
                                                            .InitFrom(out)
                                                        .Build()
                                                        .Columns<TCoVoid>().Build()
                                                        .Ranges<TCoVoid>().Build()
                                                        .Stat<TCoVoid>().Build()
                                                    .Build()
                                                .Build()
                                                .Settings().Build()
                                            .Build()
                                        .Build()
                                        .Output()
                                            .Add(std::move(newTable))
                                        .Build()
                                        .Settings(settingsBuilder.Done())
                                        .Mapper(mapper)
                                    .Build()
                                    .OutIndex().Build(0U)
                                    .Mode(out.Mode())
                                    .Done().Ptr();
                            }
                        }
                    }
                }
            }
            if (newOutput != origOutput) {
                remap[operation.Raw()] = ctx.ChangeChild(operation.Ref(), TYtOutputOpBase::idx_Output, std::move(newOutput));
            }
        }

        if (!updatePublish.empty()) {
            for (auto& item: updatePublish) {
                auto publish = TYtPublish(item.first);
                const TNodeOnNodeOwnedMap& updateOuts = item.second;
                auto origInput = publish.Input().Ptr();
                auto newInput = origInput;
                for (size_t i = 0; i < publish.Input().Size(); ++i) {
                    if (auto it = updateOuts.find(publish.Input().Item(i).Raw()); it != updateOuts.end()) {
                        newInput = ctx.ChangeChild(*newInput, i, TExprNode::TPtr(it->second));
                    }
                }
                YQL_ENSURE(newInput != origInput);
                remap[publish.Raw()] = ctx.ChangeChild(publish.Ref(), TYtPublish::idx_Input, std::move(newInput));
            }
        }

        if (!remap.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-AlignPublishTypes";
            return RemapExpr(input, output, remap, ctx, TOptimizeExprSettings(State_->Types));
        }

        return TStatus::Ok;
    }

    static TExprNode::TPtr SuppressUnusedOuts(const TExprNode& node, const TDynBitMap& usedOuts, TExprContext& ctx) {
        auto op = TYtOutputOpBase(&node);
        size_t lambdaNdx = 0;
        bool withArg = true;
        bool ordered = false;
        if (auto ytMap = op.Maybe<TYtMap>()) {
            lambdaNdx = TYtMap::idx_Mapper;
            ordered = NYql::HasSetting(ytMap.Cast().Settings().Ref(), EYtSettingType::Ordered);
        } else if (op.Maybe<TYtReduce>()) {
            lambdaNdx = TYtReduce::idx_Reducer;
        } else if (op.Maybe<TYtMapReduce>()) {
            lambdaNdx = TYtMapReduce::idx_Reducer;
        } else if (op.Maybe<TYtFill>()) {
            lambdaNdx = TYtFill::idx_Content;
            withArg = false;
        } else {
            YQL_ENSURE(false, "Unsupported operation " << node.Content());
        }
        TExprNode::TPtr lambda = node.ChildPtr(lambdaNdx);
        TVector<TYtOutTable> resTables;

        lambda = RebuildLambdaWithLessOuts(lambda, withArg, ordered, usedOuts, op.Output(), resTables, ctx);

        auto res = ctx.ChangeChild(node, TYtOutputOpBase::idx_Output,
            Build<TYtOutSection>(ctx, op.Pos()).Add(resTables).Done().Ptr());

        res = ctx.ChangeChild(*res, lambdaNdx, std::move(lambda));

        YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-SuppressOuts";
        return res;
    }

    static TExprNode::TPtr Limits(const TExprNode::TPtr& node, const TParentsMap& limits, TExprContext& ctx) {
        auto limitIt = limits.find(node.Get());
        if (limitIt == limits.end()) {
            return node;
        }
        auto op = TYtTransientOpBase(node);
        TExprNode::TListType limitNodeChildren;
        const TNodeSet& limitNodesSet = limitIt->second;
        TVector<const TExprNode*> limitNodes(limitNodesSet.cbegin(), limitNodesSet.cend());
        std::stable_sort(limitNodes.begin(), limitNodes.end(), [](const auto& p1, const auto& p2) { return p1->UniqueId() < p2->UniqueId(); });
        for (const auto& readerSettings: limitNodes) {
            TExprNode::TListType readerNodeChildren;
            for (auto setting : readerSettings->Children()) {
                if (setting->ChildrenSize() > 0) {
                    auto kind = FromString<EYtSettingType>(setting->Child(0)->Content());
                    if (EYtSettingType::Take == kind || EYtSettingType::Skip == kind) {
                        readerNodeChildren.push_back(setting);
                    }
                }
            }
            auto readerNode = ctx.NewList(op.Pos(), std::move(readerNodeChildren));
            limitNodeChildren.push_back(readerNode);
        }

        return ctx.ChangeChild(*node, TYtTransientOpBase::idx_Settings,
            NYql::AddSetting(op.Settings().Ref(), EYtSettingType::Limit, ctx.NewList(op.Pos(), std::move(limitNodeChildren)), ctx));
    }

    static TExprNode::TPtr LengthOverPhysicalList(const TExprNode::TPtr& node, const TOpDeps& opDeps, TExprContext& ctx) {
        auto op = TYtWithUserJobsOpBase(node);

        // TODO: support multi-output
        if (op.Output().Size() != 1) {
            return node;
        }
        auto outTable = op.Output().Item(0);
        if (outTable.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetSize() == 0) {
            // already empty struct
            return node;
        }

        auto readersIt = opDeps.find(op.Raw());
        if (readersIt == opDeps.end() || readersIt->second.size() != 1) {
            return node;
        }
        auto reader = std::get<0>(readersIt->second.front());
        if (!TYtLength::Match(reader)) {
            return node;
        }

        // only YtLength is used, rewrite
        auto lambdaIdx = op.Maybe<TYtMap>()
            ? TYtMap::idx_Mapper
            : op.Maybe<TYtReduce>()
                ? TYtReduce::idx_Reducer
                : TYtMapReduce::idx_Reducer;

        // Rebuild lambda
        auto lambda = TCoLambda(node->ChildPtr(lambdaIdx));
        lambda = Build<TCoLambda>(ctx, lambda.Pos())
            .Args({"stream"})
            .Body<TCoMap>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With(0, "stream")
                .Build()
                .Lambda()
                    .Args({"item"})
                    .Body<TCoAsStruct>()
                    .Build()
                .Build()
            .Build()
            .Done();

        // Rebuild output table
        TYtOutTableInfo outTableInfo(outTable);
        outTableInfo.RowSpec->ClearSortness(ctx);
        outTableInfo.RowSpec->SetType(ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>()));

        auto newOp = ctx.ShallowCopy(*node);
        if (NYql::HasSetting(op.Settings().Ref(), EYtSettingType::Ordered)) {
            newOp->ChildRef(TYtWithUserJobsOpBase::idx_Settings) = NYql::RemoveSetting(op.Settings().Ref(), EYtSettingType::Ordered, ctx);
        }

        newOp->ChildRef(lambdaIdx) = lambda.Ptr();
        newOp->ChildRef(TYtWithUserJobsOpBase::idx_Output) =
            Build<TYtOutSection>(ctx, op.Output().Pos())
                .Add(outTableInfo.ToExprNode(ctx, outTable.Pos()).Cast<TYtOutTable>())
                .Done().Ptr();

        return newOp;
    }

    static TExprNode::TPtr TopSortForProducers(const TExprNode::TPtr& node, const TOpDeps& opDeps, TExprContext& ctx) {
        auto op = TYtWithUserJobsOpBase(node);

        if (op.Output().Size() != 1) {
            return node;
        }

        if (NYql::HasAnySetting(op.Settings().Ref(), EYtSettingType::SortLimitBy | EYtSettingType::Limit)) {
            return node;
        }

        auto readersIt = opDeps.find(op.Raw());
        if (readersIt == opDeps.end()) {
            return node;
        }

        TExprNode::TListType limits;
        TVector<std::pair<TString, bool>> sortBy;
        for (auto item: readersIt->second) {
            if (!TYtSort::Match(std::get<0>(item))) {
                return node;
            }
            auto sort = TYtSort(std::get<0>(item));
            if (!NYql::HasSetting(sort.Settings().Ref(), EYtSettingType::Limit)) {
                return node;
            }

            auto opSortBy = TYtOutTableInfo(sort.Output().Item(0)).RowSpec->GetForeignSort();
            if (sortBy.empty()) {
                sortBy = std::move(opSortBy);
            }
            else if (opSortBy != sortBy) {
                return node;
            }
            auto limitValues = NYql::GetSetting(sort.Settings().Ref(), EYtSettingType::Limit)->Child(1);
            limits.insert(limits.end(), limitValues->Children().begin(), limitValues->Children().end());
        }

        if (limits.empty()) {
            return node;
        }

        auto newSettings = NYql::AddSetting(op.Settings().Ref(), EYtSettingType::Limit,
            ctx.NewList(node->Pos(), std::move(limits)), ctx);
        newSettings = NYql::AddSettingAsColumnPairList(*newSettings, EYtSettingType::SortLimitBy, sortBy, ctx);
        newSettings = NYql::RemoveSetting(*newSettings, EYtSettingType::Ordered, ctx);

        auto newOp = ctx.ShallowCopy(*node);
        newOp->ChildRef(TYtWithUserJobsOpBase::idx_Settings) = newSettings;

        auto outTable = op.Output().Item(0);
        TYtOutTableInfo outTableInfo(outTable);
        if (outTableInfo.RowSpec->IsSorted()) {
            outTableInfo.RowSpec->ClearSortness(ctx);
            newOp->ChildRef(TYtWithUserJobsOpBase::idx_Output) =
                Build<TYtOutSection>(ctx, op.Output().Pos())
                    .Add(outTableInfo.ToExprNode(ctx, outTable.Pos()).Cast<TYtOutTable>())
                    .Done().Ptr();
        }

        return newOp;
    }

    TStatus OptimizeMultiOuts(TExprNode::TPtr input, TExprNode::TPtr& output, const TOpDeps& opDeps, const TNodeSet& lefts, TExprContext& ctx) {
        TNodeOnNodeOwnedMap rewrite;
        TNodeOnNodeOwnedMap newOps;
        for (auto& x: opDeps) {
            auto writer = x.first;
            const TYtOutputOpBase op = GetRealOperation(TExprBase(writer));
            if (const size_t outCount = op.Output().Size(); outCount > 1 && !BeingExecuted(*writer)
                && (!op.Maybe<TYtMapReduce>() || GetMapDirectOutputsCount(op.Maybe<TYtMapReduce>().Cast()) == 0) // TODO: optimize this case
                && ProcessedMultiOuts.find(writer->UniqueId()) == ProcessedMultiOuts.end())
            {
                // Outputs cannot be merged if: 1) there are ranges over output, 2) output is used multiple times in the same section, 3) output is sorted
                TSet<size_t> exclusiveOuts;
                TVector<const TTypeAnnotationNode*> outTypes;
                for (size_t i: xrange(op.Output().Size())) {
                    if (TYqlRowSpecInfo(op.Output().Item(i).RowSpec()).IsSorted()) {
                        exclusiveOuts.insert(i);
                    }
                    outTypes.push_back(op.Output().Item(i).Ref().GetTypeAnn());
                }

                TMap<size_t, TSet<const TExprNode*>> outUsage; // output num -> set of readers (sections or operations)
                THashMap<size_t, TSet<std::pair<const TExprNode*, const TExprNode*>>> duplicateCheck; // output num -> set of readers (operation + section)
                for (auto& reader: x.second) {
                    const auto out = TYtOutput(std::get<2>(reader));
                    const auto outIndex = FromString<size_t>(out.OutIndex().Value());
                    YQL_ENSURE(outIndex < outTypes.size());
                    const auto path = std::get<3>(reader);
                    if (path && !TCoVoid::Match(path->Child(TYtPath::idx_Ranges))) {
                        exclusiveOuts.insert(outIndex);
                    }
                    auto section = std::get<1>(reader); // section
                    auto op = std::get<0>(reader); // operation
                    // Section may be used multiple times in different operations
                    // So, check only unique pair of operation + section
                    if (!duplicateCheck[outIndex].insert(std::make_pair(op, section)).second) {
                        exclusiveOuts.insert(outIndex);
                    }
                    outUsage[outIndex].insert(section ? section : op);
                }

                // Group by {{set of sections}, type}
                TMap<std::pair<TSet<const TExprNode*>, const TTypeAnnotationNode*>, TVector<size_t>> groupedOuts;
                for (auto& item: outUsage) {
                    if (!exclusiveOuts.contains(item.first)) {
                        groupedOuts[std::make_pair(item.second, outTypes[item.first])].push_back(item.first);
                    }
                }
                if (AnyOf(groupedOuts, [](const auto& item) { return item.second.size() > 1; })) {
                    size_t nextNewOutIndex = 0;
                    TVector<TYtOutTable> joinedOutTables;
                    TVector<std::pair<size_t, bool>> outRemap; // old index -> new index, drop flag
                    outRemap.resize(outCount);
                    for (auto old_out_num: exclusiveOuts) {
                        outRemap[old_out_num] = std::make_pair(nextNewOutIndex++, false);
                        joinedOutTables.push_back(op.Output().Item(old_out_num));
                    }
                    TVector<TVector<size_t>> sortedGroups;
                    sortedGroups.reserve(groupedOuts.size());
                    for (auto& item: groupedOuts) {
                        sortedGroups.push_back(std::move(item.second));
                    }
                    ::Sort(sortedGroups, [](const TVector<size_t>& v1, const TVector<size_t>& v2) { return v1.front() < v2.front(); });
                    for (auto& outs: sortedGroups) {
                        TMaybe<size_t> newIndex;
                        bool drop = false;
                        for (auto old_out_num: outs) {
                            if (!newIndex) {
                                newIndex = nextNewOutIndex++;
                                joinedOutTables.push_back(op.Output().Item(old_out_num));
                            }
                            outRemap[old_out_num] = std::make_pair(*newIndex, drop);
                            drop = true;
                        }
                    }

                    size_t lambdaNdx = 0;
                    bool ordered = false;
                    if (auto ytMap = op.Maybe<TYtMap>()) {
                        lambdaNdx = TYtMap::idx_Mapper;
                        ordered = NYql::HasSetting(ytMap.Cast().Settings().Ref(), EYtSettingType::Ordered);
                    } else if (op.Maybe<TYtReduce>()) {
                        lambdaNdx = TYtReduce::idx_Reducer;
                    } else if (op.Maybe<TYtMapReduce>()) {
                        lambdaNdx = TYtMapReduce::idx_Reducer;
                    } else if (op.Maybe<TYtFill>()) {
                        lambdaNdx = TYtFill::idx_Content;
                    } else {
                        YQL_ENSURE(false, "Unsupported operation " << writer->Content());
                    }
                    TExprNode::TPtr lambda = writer->ChildPtr(lambdaNdx);
                    lambda = RebuildLambdaWithMergedOuts(lambda, nextNewOutIndex == 1, ordered, joinedOutTables, outRemap, ctx);

                    auto newOp = ctx.ChangeChild(op.Ref(), TYtOutputOpBase::idx_Output,
                        Build<TYtOutSection>(ctx, op.Pos()).Add(joinedOutTables).Done().Ptr());

                    newOp = ctx.ChangeChild(*newOp, lambdaNdx, std::move(lambda));

                    rewrite[writer] = newOp;
                    newOps[writer] = newOp;

                    TNodeOnNodeOwnedMap newOuts;
                    TNodeSet processed;
                    for (auto& reader: x.second) {
                        auto oldOutput = TYtOutput(std::get<2>(reader));
                        if (processed.insert(oldOutput.Raw()).second) {
                            auto oldNdx = FromString<size_t>(oldOutput.OutIndex().Value());
                            YQL_ENSURE(oldNdx < outRemap.size());
                            auto newOut = Build<TYtOutput>(ctx, oldOutput.Pos())
                                .Operation(newOp)
                                .OutIndex()
                                    .Value(outRemap[oldNdx].second ? TString("canary") : ToString(outRemap[oldNdx].first)) // Insert invalid "canary" index for dropped outputs
                                .Build()
                                .Mode(oldOutput.Mode())
                                .Done().Ptr();
                            rewrite[oldOutput.Raw()] = newOut;
                            if (!outRemap[oldNdx].second) {
                                newOuts[oldOutput.Raw()] = newOut;
                            }
                        }
                    }

                    for (auto& reader: x.second) {
                        if (auto rawSection = std::get<1>(reader)) {
                            if (processed.insert(rawSection).second) {
                                auto section = TYtSection(rawSection);
                                bool updated = false;
                                TVector<TYtPath> updatedPaths;
                                for (auto path: section.Paths()) {
                                    if (path.Table().Maybe<TYtOutput>().Operation().Raw() == writer) {
                                        if (auto it = newOuts.find(path.Table().Cast<TYtOutput>().Raw()); it != newOuts.cend()) {
                                            updatedPaths.push_back(Build<TYtPath>(ctx, path.Pos())
                                                .InitFrom(path)
                                                .Table(it->second)
                                                .Done());
                                        }
                                        updated = true;
                                    } else {
                                        updatedPaths.push_back(path);
                                    }
                                }
                                if (updated) {
                                    YQL_ENSURE(!updatedPaths.empty());
                                    rewrite[rawSection] = Build<TYtSection>(ctx, section.Pos())
                                        .InitFrom(section)
                                        .Paths()
                                            .Add(updatedPaths)
                                        .Build()
                                        .Done().Ptr();
                                }
                            }
                        }
                        else if (TYtPublish::Match(std::get<0>(reader))) {
                            auto publish = TYtPublish(std::get<0>(reader));
                            if (processed.insert(publish.Raw()).second) {
                                bool updated = false;
                                TExprNode::TListType updatedOuts;
                                for (auto out: publish.Input()) {
                                    if (out.Operation().Raw() == writer) {
                                        if (auto it = newOuts.find(out.Raw()); it != newOuts.cend()) {
                                            updatedOuts.push_back(it->second);
                                        }
                                        updated = true;
                                    } else {
                                        updatedOuts.push_back(out.Ptr());
                                    }
                                }
                                if (updated) {
                                    YQL_ENSURE(!updatedOuts.empty());
                                    rewrite[publish.Raw()] = Build<TYtPublish>(ctx, publish.Pos())
                                        .InitFrom(publish)
                                        .Input()
                                            .Add(updatedOuts)
                                        .Build()
                                        .Done().Ptr();
                                }
                            }
                        }
                    }
                    break;
                }
            }
            ProcessedMultiOuts.insert(writer->UniqueId());
        }
        if (!lefts.empty() && !newOps.empty()) {
            for (auto node: lefts) {
                TCoLeft left(node);
                auto newIt = newOps.find(left.Input().Raw());
                if (newIt != newOps.end()) {
                    rewrite[node] = ctx.ChangeChild(*node, TCoLeft::idx_Input, TExprNode::TPtr(newIt->second));
                }
            }
        }

        if (!rewrite.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-MergeMultiOuts";
            return RemapExpr(input, output, rewrite, ctx, TOptimizeExprSettings(State_->Types));
        }
        return TStatus::Ok;
    }

    static TExprNode::TPtr RebuildLambdaWithMergedOuts(const TExprNode::TPtr& lambda, bool singleOutput, bool ordered,
        const TVector<TYtOutTable>& joinedOutTables, const TVector<std::pair<size_t, bool>>& outRemap, TExprContext& ctx)
    {
        if (singleOutput) {
            if (lambda->Child(0)->ChildrenSize()) {
                return Build<TCoLambda>(ctx, lambda->Pos())
                    .Args({"stream"})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .Input<TExprApplier>()
                            .Apply(TCoLambda(lambda))
                            .With(0, "stream")
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .Body<TCoJust>()
                                .Input<TCoVariantItem>()
                                    .Variant("item")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            } else {
                return Build<TCoLambda>(ctx, lambda->Pos())
                    .Args({})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .Input<TExprApplier>()
                            .Apply(TCoLambda(lambda))
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .Body<TCoJust>()
                                .Input<TCoVariantItem>()
                                    .Variant("item")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
        } else { // nextNewOutIndex > 1
            TVector<TExprBase> tupleTypes;
            for (auto out: joinedOutTables) {
                auto itemType = out.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                tupleTypes.push_back(TExprBase(ExpandType(out.Pos(), *itemType, ctx)));
            }
            TExprBase varType = Build<TCoVariantType>(ctx, lambda->Pos())
                .UnderlyingType<TCoTupleType>()
                    .Add(tupleTypes)
                .Build()
                .Done();

            TVector<TExprBase> visitArgs;
            for (size_t i: xrange(outRemap.size())) {
                visitArgs.push_back(Build<TCoAtom>(ctx, lambda->Pos()).Value(ToString(i)).Done());
                visitArgs.push_back(Build<TCoLambda>(ctx, lambda->Pos())
                    .Args({"row"})
                    .Body<TCoVariant>()
                        .Item("row")
                        .Index()
                            .Value(ToString(outRemap[i].first))
                        .Build()
                        .VarType(varType)
                    .Build()
                    .Done());
            }

            if (lambda->Child(0)->ChildrenSize()) {
                return Build<TCoLambda>(ctx, lambda->Pos())
                    .Args({"stream"})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .Input<TExprApplier>()
                            .Apply(TCoLambda(lambda))
                            .With(0, "stream")
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .Body<TCoJust>()
                                .Input<TCoVisit>()
                                    .Input("item")
                                    .FreeArgs()
                                        .Add(visitArgs)
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            } else {
                return Build<TCoLambda>(ctx, lambda->Pos())
                    .Args({})
                    .Body<TCoFlatMapBase>()
                        .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                        .Input<TExprApplier>()
                            .Apply(TCoLambda(lambda))
                        .Build()
                        .Lambda()
                            .Args({"item"})
                            .Body<TCoJust>()
                                .Input<TCoVisit>()
                                    .Input("item")
                                    .FreeArgs()
                                        .Add(visitArgs)
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
        }
    }

    TStatus FuseMultiOutsWithOuterMaps(TExprNode::TPtr input, TExprNode::TPtr& output, const TOpDeps& opDeps, const TNodeSet& lefts, const TNodeSet& hasWorldDeps, TExprContext& ctx) {
        const ui64 switchLimit = State_->Configuration->SwitchLimit.Get().GetOrElse(DEFAULT_SWITCH_MEMORY_LIMIT);
        const auto maxOperationFiles = State_->Configuration->MaxOperationFiles.Get().GetOrElse(DEFAULT_MAX_OPERATION_FILES);
        const auto maxJobMemoryLimit = State_->Configuration->MaxExtraJobMemoryToFuseOperations.Get();
        const auto maxOutTables = State_->Configuration->MaxOutputTables.Get().GetOrElse(DEFAULT_MAX_OUTPUT_TABLES);
        const auto maxOuterCpuUsage = State_->Configuration->MaxCpuUsageToFuseMultiOuts.Get().GetOrElse(2.0);
        const auto maxOuterReplicationFactor = State_->Configuration->MaxReplicationFactorToFuseMultiOuts.Get().GetOrElse(2.0);

        for (auto& x: opDeps) {
            if (TYtWithUserJobsOpBase::Match(x.first) && !BeingExecuted(*x.first) && !hasWorldDeps.contains(x.first)
                && !NYql::HasAnySetting(*x.first->Child(TYtWithUserJobsOpBase::idx_Settings), EYtSettingType::Limit | EYtSettingType::SortLimitBy | EYtSettingType::JobCount)) {

                const auto op = TYtWithUserJobsOpBase(x.first);

                if (op.Maybe<TYtMapReduce>() && GetMapDirectOutputsCount(op.Cast<TYtMapReduce>()) != 0) { // TODO: optimize this case
                    continue;
                }

                size_t lambdaNdx = 0;
                if (auto ytMap = op.Maybe<TYtMap>()) {
                    lambdaNdx = TYtMap::idx_Mapper;
                } else if (op.Maybe<TYtReduce>()) {
                    lambdaNdx = TYtReduce::idx_Reducer;
                } else if (op.Maybe<TYtMapReduce>()) {
                    lambdaNdx = TYtMapReduce::idx_Reducer;
                } else {
                    YQL_ENSURE(false, "Unsupported operation " << op.Ref().Content());
                }

                auto lambda = TCoLambda(op.Ref().ChildPtr(lambdaNdx));
                if (!IsYieldTransparent(lambda.Ptr(), *State_->Types)) {
                    continue;
                }

                const size_t opOutTables = op.Output().Size();
                std::map<size_t, std::pair<std::vector<const TExprNode*>, std::vector<const TExprNode*>>> maps; // output -> pair<vector<YtMap>, vector<other YtOutput's>>
                for (size_t i = 0; i < x.second.size(); ++i) {
                    auto reader = std::get<0>(x.second[i]);
                    if (BeingExecuted(*reader)) {
                        maps.clear();
                        break;
                    }
                    const auto out = std::get<2>(x.second[i]);
                    const auto opIndex = FromString<size_t>(out->Child(TYtOutput::idx_OutIndex)->Content());
                    auto& item = maps[opIndex];
                    const TExprNode* matched = nullptr;
                    const auto newPair = ProcessedFuseWithOuterMaps.insert(std::make_pair(x.first->UniqueId(), reader->UniqueId())).second;
                    if (newPair && TYtMap::Match(reader)) {
                        const auto outerMap = TYtMap(reader);
                        if ((outerMap.World().Ref().IsWorld() || outerMap.World().Raw() == op.World().Raw())
                            && outerMap.Input().Size() == 1
                            && outerMap.Output().Size() + item.first.size() <= maxOutTables // fast check for too many operations
                            && outerMap.DataSink().Cluster().Value() == op.DataSink().Cluster().Value()
                            && NYql::HasSetting(op.Settings().Ref(), EYtSettingType::Flow) == NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::Flow)
                            && !NYql::HasSetting(op.Settings().Ref(), EYtSettingType::JobCount)
                            && !NYql::HasSetting(outerMap.Settings().Ref(), EYtSettingType::JobCount)
                            && !HasYtRowNumber(outerMap.Mapper().Body().Ref())
                            && IsYieldTransparent(outerMap.Mapper().Ptr(), *State_->Types)
                            && (!op.Maybe<TYtMapReduce>() || AllOf(outerMap.Output(), [](const auto& out) { return !TYtTableBaseInfo::GetRowSpec(out)->IsSorted(); }))) {

                            const auto outerSection = outerMap.Input().Item(0);
                            if (outerSection.Paths().Size() == 1 && outerSection.Settings().Size() == 0) {
                                const auto outerPath = outerSection.Paths().Item(0);
                                if (outerPath.Ranges().Maybe<TCoVoid>()) {
                                    matched = reader;
                                }
                            }
                        }
                    }
                    if (matched) {
                        item.first.push_back(matched);
                    } else {
                        item.second.push_back(out);
                    }
                }

                // Check limits
                if (AnyOf(maps, [](const auto& item) { return item.second.first.size() > 0; })) {
                    TMap<TStringBuf, ui64> memUsage;
                    size_t currenFiles = 1; // jobstate. Take into account only once
                    size_t currOutTables = opOutTables;

                    TExprNode::TPtr updatedBody = lambda.Body().Ptr();
                    if (maxJobMemoryLimit) {
                        auto status = UpdateTableContentMemoryUsage(lambda.Body().Ptr(), updatedBody, State_, ctx);
                        if (status.Level != TStatus::Ok) {
                            return status;
                        }
                    }
                    ScanResourceUsage(*updatedBody, *State_->Configuration, State_->Types, maxJobMemoryLimit ? &memUsage : nullptr, nullptr, &currenFiles);

                    TMap<TStringBuf, ui64> newMemUsage;
                    TMap<TStringBuf, double> cpuUsage;
                    for (auto& item: maps) {
                        if (!item.second.first.empty()) {
                            size_t otherTablesDelta = item.second.second.empty() ? 1 : 0;
                            for (auto it = item.second.first.begin(); it != item.second.first.end(); ) {
                                const auto outerMap = TYtMap(*it);

                                const size_t outTablesDelta = outerMap.Output().Size() - otherTablesDelta;

                                updatedBody = outerMap.Mapper().Body().Ptr();
                                if (maxJobMemoryLimit) {
                                    auto status = UpdateTableContentMemoryUsage(outerMap.Mapper().Body().Ptr(), updatedBody, State_, ctx);
                                    if (status.Level != TStatus::Ok) {
                                        return status;
                                    }
                                }
                                TMap<TStringBuf, ui64>* pMemUsage = nullptr;
                                if (maxJobMemoryLimit) {
                                    pMemUsage = &newMemUsage;
                                    newMemUsage = memUsage;
                                }
                                size_t newCurrenFiles = currenFiles;
                                cpuUsage.clear();
                                ScanResourceUsage(*updatedBody, *State_->Configuration, State_->Types, pMemUsage, &cpuUsage, &newCurrenFiles);

                                auto usedMemory = Accumulate(newMemUsage.begin(), newMemUsage.end(), switchLimit,
                                    [](ui64 sum, const std::pair<const TStringBuf, ui64>& val) { return sum + val.second; });

                                // Take into account codec input/output buffers (one for all inputs and one per output)
                                usedMemory += YQL_JOB_CODEC_MEM * (currOutTables + outTablesDelta + 1);

                                const auto usedCpu = Accumulate(cpuUsage.begin(), cpuUsage.end(), 1.0,
                                    [](double prod, const std::pair<const TStringBuf, double>& val) { return prod * val.second; });

                                const auto replicationFactor = NCommon::GetDataReplicationFactor(outerMap.Mapper().Ref(), ctx);

                                bool skip = false;
                                if (replicationFactor > maxOuterReplicationFactor) {
                                    YQL_CLOG(DEBUG, ProviderYt) << "FuseMultiOutsWithOuterMaps: skip by replication factor limites " << replicationFactor << " > " << maxOuterReplicationFactor;
                                    skip = true;
                                }
                                else if (outTablesDelta && currOutTables + outTablesDelta > maxOutTables) {
                                    YQL_CLOG(DEBUG, ProviderYt) << "FuseMultiOutsWithOuterMaps: skip by out table limits " << (currOutTables + outTablesDelta) << " > " << maxOutTables;
                                    skip = true;
                                }
                                else if (newCurrenFiles > maxOperationFiles) {
                                    YQL_CLOG(DEBUG, ProviderYt) << "FuseMultiOutsWithOuterMaps: skip by files limits " << newCurrenFiles << " > " << maxOperationFiles;
                                    skip = true;
                                }
                                else if (maxJobMemoryLimit && usedMemory > *maxJobMemoryLimit) {
                                    YQL_CLOG(DEBUG, ProviderYt) << "FuseMultiOutsWithOuterMaps: skip by memory limits " << usedMemory << " > " << *maxJobMemoryLimit;
                                    skip = true;
                                }
                                else if (usedCpu > maxOuterCpuUsage) {
                                    YQL_CLOG(DEBUG, ProviderYt) << "FuseMultiOutsWithOuterMaps: skip by cpu limits " << usedCpu << " > " << maxOuterCpuUsage;
                                    skip = true;
                                }
                                if (skip) {
                                    // Move to other usages
                                    it = item.second.first.erase(it);
                                    if (item.second.second.empty()) {
                                        ++currOutTables;
                                    }
                                    item.second.second.push_back(outerMap.Input().Item(0).Paths().Item(0).Table().Raw());
                                    continue;
                                }
                                currenFiles = newCurrenFiles;
                                memUsage = std::move(newMemUsage);
                                currOutTables += outTablesDelta;
                                otherTablesDelta = 0; // Take into account only once
                                ++it;
                            }
                        }
                    }
                }

                if (AnyOf(maps, [](const auto& item) { return item.second.first.size() > 0; })) {
                    TNodeOnNodeOwnedMap remaps;
                    TNodeSet removed;
                    TNodeMap<size_t> newOutIndicies; // old YtOutput -> new index

                    TVector<TExprBase> switchArgs;
                    TVector<TYtOutTable> outs;
                    for (size_t i = 0; i < op.Output().Size(); ++i) {
                        auto& item = maps[i];

                        if (item.first.empty() || !item.second.empty()) {
                            for (const auto out: item.second) {
                                newOutIndicies[out] = outs.size();
                            }
                            switchArgs.push_back(
                                Build<TCoAtomList>(ctx, lambda.Pos())
                                    .Add()
                                        .Value(ctx.GetIndexAsString(i))
                                    .Build()
                                .Done());
                            switchArgs.push_back(
                                Build<TCoLambda>(ctx, lambda.Pos())
                                    .Args({"flow"})
                                    .Body("flow")
                                    .Done());
                            outs.push_back(op.Output().Item(i));
                        }

                        for (auto reader: item.first) {
                            if (auto it = opDeps.find(reader); it != opDeps.end()) {
                                for (auto& deps: it->second) {
                                    const auto out = std::get<2>(deps);
                                    const auto oldIndex = FromString<size_t>(out->Child(TYtOutput::idx_OutIndex)->Content());
                                    newOutIndicies[out] = outs.size() + oldIndex;
                                }
                            }
                            const auto outerMap = TYtMap(reader);
                            switchArgs.push_back(
                                Build<TCoAtomList>(ctx, lambda.Pos())
                                    .Add()
                                        .Value(ctx.GetIndexAsString(i))
                                    .Build()
                                .Done());

                            TExprNode::TListType members;
                            for (auto item : GetSeqItemType(*outerMap.Mapper().Args().Arg(0).Ref().GetTypeAnn()).Cast<TStructExprType>()->GetItems()) {
                                members.push_back(ctx.NewAtom(outerMap.Mapper().Pos(), item->GetName()));
                            }

                            auto [placeHolder, lambdaWithPlaceholder] = ReplaceDependsOn(outerMap.Mapper().Ptr(), ctx, State_->Types);
                            YQL_ENSURE(placeHolder);

                            switchArgs.push_back(Build<TCoLambda>(ctx, outerMap.Mapper().Pos())
                                .Args({"flow"})
                                .Body<TExprApplier>()
                                    .Apply(TCoLambda(lambdaWithPlaceholder))
                                    .With<TCoExtractMembers>(0)
                                        .Input("flow")
                                        .Members()
                                            .Add(members)
                                        .Build()
                                    .Build()
                                    .With(TExprBase(placeHolder), "flow")
                                .Build()
                                .Done());

                            outs.insert(outs.end(), outerMap.Output().begin(), outerMap.Output().end());
                            removed.insert(reader);
                        }
                    }

                    lambda = Build<TCoLambda>(ctx, lambda.Pos())
                        .Args({"flow"})
                        .Body<TCoSwitch>()
                            .Input<TExprApplier>()
                                .Apply(lambda)
                                .With(0, "flow")
                            .Build()
                            .BufferBytes()
                                .Value(ToString(switchLimit))
                            .Build()
                            .FreeArgs()
                                .Add(switchArgs)
                            .Build()
                        .Build()
                        .Done();

                    auto newOp = ctx.ChangeChild(op.Ref(), TYtOutputOpBase::idx_Output,
                        Build<TYtOutSection>(ctx, op.Pos()).Add(outs).Done().Ptr());

                    newOp = ctx.ChangeChild(*newOp, lambdaNdx, lambda.Ptr());

                    remaps[op.Raw()] = newOp;
                    for (auto node: lefts) {
                        TCoLeft left(node);
                        if (removed.contains(left.Input().Raw())) {
                            remaps[node] = ctx.ChangeChild(*node, TCoLeft::idx_Input, TExprNode::TPtr(newOp));
                        }
                    }
                    for (auto& item: newOutIndicies) {
                        remaps[item.first] = Build<TYtOutput>(ctx, item.first->Pos())
                            .InitFrom(TYtOutput(item.first))
                            .Operation(newOp)
                            .OutIndex()
                                .Value(ctx.GetIndexAsString(item.second))
                            .Build()
                            .Done().Ptr();
                    }
                    YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-FuseMultiOutsWithOuterMaps";
                    return RemapExpr(input, output, remaps, ctx, TOptimizeExprSettings(State_->Types));
                }
            }
        }
        return TStatus::Ok;
    }

    TExprNode::TPtr UpdateColumnGroups(const TYtOutputOpBase& op, const std::map<size_t, TString>& groupSpecs, TExprContext& ctx) {
        auto origOutput = op.Output().Ptr();
        auto newOutput = origOutput;
        for (const auto& item: groupSpecs) {
            const auto table = op.Output().Item(item.first);
            auto currentGroup = GetSetting(table.Settings().Ref(), EYtSettingType::ColumnGroups);
            if (!currentGroup || currentGroup->Tail().Content() != item.second) {
                auto newSettings = AddOrUpdateSettingValue(table.Settings().Ref(),
                    EYtSettingType::ColumnGroups,
                    ctx.NewAtom(table.Settings().Pos(), item.second, TNodeFlags::MultilineContent),
                    ctx);
                auto newTable = ctx.ChangeChild(table.Ref(), TYtOutTable::idx_Settings, std::move(newSettings));
                newOutput = ctx.ChangeChild(*newOutput, item.first, std::move(newTable));
            }
        }
        if (newOutput != origOutput) {
            return ctx.ChangeChild(op.Ref(), TYtOutputOpBase::idx_Output, std::move(newOutput));
        }
        return {};
    }

    TStatus CalculateColumnGroups(TExprNode::TPtr input, TExprNode::TPtr& output, const TOpDeps& opDeps, EColumnGroupMode mode, TExprContext& ctx) {
        const auto maxGroups = State_->Configuration->MaxColumnGroups.Get().GetOrElse(DEFAULT_MAX_COLUMN_GROUPS);
        const auto minGroupSize = State_->Configuration->MinColumnGroupSize.Get().GetOrElse(DEFAULT_MIN_COLUMN_GROUP_SIZE);
        TNodeOnNodeOwnedMap remap;
        for (auto& x: opDeps) {
            auto writer = x.first;
            if (TYtEquiJoin::Match(writer) || IsBeingExecuted(*writer)) {
                continue;
            }
            if (!ProcessedCalculateColumnGroups.insert(writer->UniqueId()).second) {
                continue;
            }

            std::vector<const TStructExprType*> outTypes;
            for (const auto& outTable: GetRealOperation(TExprBase(writer)).Output()) {
                outTypes.push_back(outTable.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>());
            }

            TNodeMap<size_t> uniquePaths;
            std::vector<std::unordered_map<TString, std::set<size_t>>> columnUsage;
            columnUsage.resize(outTypes.size());
            std::vector<bool> fullUsage;
            fullUsage.resize(outTypes.size());
            std::vector<std::unordered_set<TString>> publishUsage;
            publishUsage.resize(outTypes.size());
            // Collect column usage per consumer
            for (auto& item: x.second) {
                const auto out = TYtOutput(std::get<2>(item));
                const auto outIndex = FromString<size_t>(out.OutIndex().Value());
                auto rawPath = std::get<3>(item);
                if (!rawPath) {
                    if (TYtLength::Match(std::get<0>(item))) {
                        continue;
                    }
                    if (auto maybePublish = TMaybeNode<TYtPublish>(std::get<0>(item))) {
                        TYtTableInfo dstInfo = maybePublish.Cast().Publish();
                        const auto& desc = State_->TablesData->GetTable(dstInfo.Cluster, dstInfo.Name, dstInfo.CommitEpoch);
                        publishUsage.at(outIndex).insert(desc.ColumnGroupSpec);
                    } else {
                        fullUsage.at(outIndex) = true;
                    }
                } else if (EColumnGroupMode::Single == mode) {
                    fullUsage.at(outIndex) = true;
                } else {
                    auto path = TYtPath(rawPath);
                    auto columns = TYtColumnsInfo(path.Columns());
                    if (!columns.HasColumns() || outTypes[outIndex]->GetSize() <= columns.GetColumns()->size()) {
                        fullUsage.at(outIndex) = true;
                    } else {
                        const size_t pathNdx = uniquePaths.emplace(rawPath, uniquePaths.size()).first->second;
                        std::for_each(columns.GetColumns()->cbegin(), columns.GetColumns()->cend(),
                            [&columnUsage, outIndex, pathNdx](const TYtColumnsInfo::TColumn& c) {
                                 columnUsage.at(outIndex)[c.Name].insert(pathNdx);
                            }
                        );
                    }
                }
            }

            std::map<size_t, TString> groupSpecs;
            for (size_t i = 0; i < columnUsage.size(); ++i) {
                if (!publishUsage[i].empty()) {
                    if (publishUsage[i].size() == 1) {
                        if (auto spec = *publishUsage[i].cbegin(); !spec.empty()) {
                            groupSpecs[i] = spec;
                        }
                    }
                    continue;
                }
                if (EColumnGroupMode::Single == mode) {
                    if (fullUsage[i]) {
                        groupSpecs[i] = NYql::GetSingleColumnGroupSpec();
                    }
                } else {
                    if (fullUsage[i]) {
                        // Add all columns for tables with entire usage
                        const size_t pathNdx = uniquePaths.emplace(nullptr, uniquePaths.size()).first->second;
                        std::for_each(outTypes[i]->GetItems().cbegin(), outTypes[i]->GetItems().cend(),
                            [&columnUsage, i, pathNdx](const TItemExprType* itemType) {
                                columnUsage.at(i)[TString{itemType->GetName()}].insert(pathNdx);
                            }
                        );
                    }

                    if (!columnUsage.at(i).empty()) {
                        auto groupSpec = NYT::TNode();

                        // Find unique groups. Use ordered collections for stable names
                        std::map<std::set<size_t>, std::set<TString>> groups;
                        for (const auto& item: columnUsage.at(i)) {
                            groups[item.second].insert(item.first);
                            if (groups.size() > maxGroups) {
                                groups.clear();
                                break;
                            }
                        }
                        if (!groups.empty()) {
                            bool allGroups = true;
                            size_t maxSize = 0;
                            auto maxGrpIt = groups.end();
                            // Delete too short groups and find a group with max size
                            for (auto it = groups.begin(); it != groups.end();) {
                                if (it->second.size() < minGroupSize) {
                                    it = groups.erase(it);
                                    allGroups = false;
                                } else {
                                    if (it->second.size() > maxSize) {
                                        maxSize = it->second.size();
                                        maxGrpIt = it;
                                    }
                                    ++it;
                                }
                            }
                            if (!groups.empty()) {
                                groupSpec = NYT::TNode::CreateMap();
                                // If we keep all groups then use the group with max size as default
                                if (allGroups && maxGrpIt != groups.end()) {
                                    groupSpec["default"] = NYT::TNode::CreateEntity();
                                    groups.erase(maxGrpIt);
                                }
                                TStringBuilder nameBuilder;
                                nameBuilder.reserve(8); // "group" + 2 digit number + zero-terminator
                                nameBuilder << "group";
                                size_t num = 0;
                                for (const auto& g: groups) {
                                    nameBuilder.resize(5);
                                    nameBuilder << num++;
                                    auto columns = NYT::TNode::CreateList();
                                    for (const auto& n: g.second) {
                                        columns.Add(n);
                                    }
                                    groupSpec[nameBuilder] = std::move(columns);
                                }
                            }
                        }
                        if (!groupSpec.IsUndefined()) {
                            groupSpecs[i] = NYT::NodeToCanonicalYsonString(groupSpec, NYson::EYsonFormat::Text);
                        }
                    }
                }
            }
            if (!groupSpecs.empty()) {
                TExprNode::TPtr newOp;
                if (const auto mayTry = TExprBase(writer).Maybe<TYtTryFirst>()) {
                    TExprNode::TPtr newOpFirst = UpdateColumnGroups(mayTry.Cast().First(), groupSpecs, ctx);
                    TExprNode::TPtr newOpSecond = UpdateColumnGroups(mayTry.Cast().Second(), groupSpecs, ctx);
                    if (newOpFirst || newOpSecond) {
                        newOp = Build<TYtTryFirst>(ctx, writer->Pos())
                            .First(newOpFirst ? std::move(newOpFirst) : mayTry.Cast().First().Ptr())
                            .Second(newOpSecond ? std::move(newOpSecond) : mayTry.Cast().Second().Ptr())
                            .Done().Ptr();
                    }
                } else {
                    newOp = UpdateColumnGroups(TYtOutputOpBase(writer), groupSpecs, ctx);
                }
                if (newOp) {
                    remap[writer] = newOp;
                }
            }
        }

        if (!remap.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "PhysicalFinalizing-CalculateColumnGroups";
            return RemapExpr(input, output, remap, ctx, TOptimizeExprSettings{State_->Types});
        }
        return TStatus::Ok;
    }

    bool BeingExecuted(const TExprNode& node) {
        return node.GetState() > TExprNode::EState::ExecutionRequired || node.HasResult();
    }

private:
    TYtState::TPtr State_;
    TProcessedNodesSet ProcessedMergePublish;
    TProcessedNodesSet ProcessedSplitLargeInputs;
    TProcessedNodesSet ProcessedUnusedOuts;
    TProcessedNodesSet ProcessedMultiOuts;
    TProcessedNodesSet ProcessedHorizontalJoin;
    TProcessedNodesSet ProcessedFieldSubsetForMultiUsage;
    TProcessedNodesSet ProcessedCalculateColumnGroups;
    std::unordered_set<std::pair<ui64, ui64>, THash<std::pair<ui64, ui64>>> ProcessedFuseWithOuterMaps;
};

THashSet<TStringBuf> TYtPhysicalFinalizingTransformer::OPS_WITH_SORTED_OUTPUT = {
    TYtMerge::CallableName(),
    TYtMap::CallableName(),
    TYtSort::CallableName(),
    TYtReduce::CallableName(),
    TYtFill::CallableName(),
    TYtDqProcessWrite::CallableName(),
    TYtTryFirst::CallableName(),
};

}

THolder<IGraphTransformer> CreateYtPhysicalFinalizingTransformer(TYtState::TPtr state) {
    return THolder(new TYtPhysicalFinalizingTransformer(state));
}

} // NYql

