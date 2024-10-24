#include "yql_yt_phy_opt.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_optimize.h>

namespace NYql {

using namespace NNodes;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::TableContentWithSettings(TExprBase node, TExprContext& ctx) const {
    auto op = node.Cast<TYtOutputOpBase>();

    TExprNode::TPtr res = op.Ptr();

    TNodeSet nodesToOptimize;
    TProcessedNodesSet processedNodes;
    processedNodes.insert(res->Head().UniqueId());
    VisitExpr(res, [&nodesToOptimize, &processedNodes](const TExprNode::TPtr& input) {
        if (processedNodes.contains(input->UniqueId())) {
            return false;
        }

        if (auto read = TMaybeNode<TYtLength>(input).Input().Maybe<TYtReadTable>()) {
            nodesToOptimize.insert(read.Cast().Raw());
            return false;
        }

        if (auto read = TMaybeNode<TYtTableContent>(input).Input().Maybe<TYtReadTable>()) {
            nodesToOptimize.insert(read.Cast().Raw());
            return false;
        }
        if (TYtOutput::Match(input.Get())) {
            processedNodes.insert(input->UniqueId());
            return false;
        }
        return true;
    });

    if (nodesToOptimize.empty()) {
        return node;
    }

    TSyncMap syncList;
    TOptimizeExprSettings settings(State_->Types);
    settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
    auto status = OptimizeExpr(res, res, [&syncList, &nodesToOptimize, state = State_](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
        if (nodesToOptimize.find(input.Get()) != nodesToOptimize.end()) {
            return OptimizeReadWithSettings(input, false, true, syncList, state, ctx);
        }
        return input;
    }, ctx, settings);

    if (status.Level == IGraphTransformer::TStatus::Error) {
        return {};
    }

    if (status.Level == IGraphTransformer::TStatus::Ok) {
        return node;
    }

    if (!syncList.empty()) {
        using TPair = std::pair<TExprNode::TPtr, ui64>;
        TVector<TPair> sortedList(syncList.cbegin(), syncList.cend());
        TExprNode::TListType syncChildren;
        syncChildren.push_back(res->ChildPtr(TYtOutputOpBase::idx_World));
        ::Sort(sortedList, [](const TPair& x, const TPair& y) { return x.second < y.second; });
        for (auto& x: sortedList) {
            auto world = ctx.NewCallable(node.Pos(), TCoLeft::CallableName(), { x.first });
            syncChildren.push_back(world);
        }

        res = ctx.ChangeChild(*res, TYtOutputOpBase::idx_World,
            ctx.NewCallable(node.Pos(), TCoSync::CallableName(), std::move(syncChildren)));
    }

    return TExprBase(res);
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::NonOptimalTableContent(TExprBase node, TExprContext& ctx) const {
    auto op = node.Cast<TYtOutputOpBase>();

    TExprNode::TPtr res = op.Ptr();

    TNodeSet nodesToOptimize;
    TProcessedNodesSet processedNodes;
    processedNodes.insert(res->Head().UniqueId());
    VisitExpr(res, [&nodesToOptimize, &processedNodes](const TExprNode::TPtr& input) {
        if (processedNodes.contains(input->UniqueId())) {
            return false;
        }

        if (TYtTableContent::Match(input.Get())) {
            nodesToOptimize.insert(input.Get());
            return false;
        }
        if (TYtOutput::Match(input.Get())) {
            processedNodes.insert(input->UniqueId());
            return false;
        }
        return true;
    });

    if (nodesToOptimize.empty()) {
        return node;
    }

    TSyncMap syncList;
    const auto maxTables = State_->Configuration->TableContentMaxInputTables.Get().GetOrElse(1000);
    const auto minChunkSize = State_->Configuration->TableContentMinAvgChunkSize.Get().GetOrElse(1_GB);
    const auto maxChunks = State_->Configuration->TableContentMaxChunksForNativeDelivery.Get().GetOrElse(1000ul);
    auto state = State_;
    auto world = res->ChildPtr(TYtOutputOpBase::idx_World);
    TOptimizeExprSettings settings(State_->Types);
    settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
    auto status = OptimizeExpr(res, res, [&syncList, &nodesToOptimize, maxTables, minChunkSize, maxChunks, state, world](const TExprNode::TPtr& input, TExprContext& ctx) -> TExprNode::TPtr {
        if (nodesToOptimize.find(input.Get()) != nodesToOptimize.end()) {
            if (auto read = TYtTableContent(input).Input().Maybe<TYtReadTable>()) {
                bool materialize = false;
                const bool singleSection = 1 == read.Cast().Input().Size();
                TVector<TYtSection> newSections;
                for (auto section: read.Cast().Input()) {
                    if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Sample | EYtSettingType::SysColumns)) {
                        materialize = true;
                    }
                    else if (section.Paths().Size() > maxTables) {
                        materialize = true;
                    }
                    else {
                        TMaybeNode<TYtMerge> oldOp;
                        if (section.Paths().Size() == 1) {
                            oldOp = section.Paths().Item(0).Table().Maybe<TYtOutput>().Operation().Maybe<TYtMerge>();
                        }
                        if (!oldOp.IsValid() || !NYql::HasSetting(oldOp.Cast().Settings().Ref(), EYtSettingType::CombineChunks)) {
                            for (auto path: section.Paths()) {
                                TYtTableBaseInfo::TPtr tableInfo = TYtTableBaseInfo::Parse(path.Table());
                                if (auto tableStat = tableInfo->Stat) {
                                    if (tableStat->ChunkCount > maxChunks || (tableStat->ChunkCount > 1 && tableStat->DataSize / tableStat->ChunkCount < minChunkSize)) {
                                        materialize = true;
                                        break;
                                    }
                                }
                                if (!tableInfo->IsTemp && tableInfo->Meta) {
                                    auto p = tableInfo->Meta->Attrs.FindPtr("erasure_codec");
                                    if (p && *p != "none") {
                                        materialize = true;
                                        break;
                                    }
                                    else if (tableInfo->Meta->IsDynamic) {
                                        materialize = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (materialize) {
                        auto path = CopyOrTrivialMap(section.Pos(),
                            TExprBase(world),
                            TYtDSink(ctx.RenameNode(read.DataSource().Ref(), "DataSink")),
                            *section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType(),
                            Build<TYtSection>(ctx, section.Pos())
                                .Paths(section.Paths())
                                .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::Unordered | EYtSettingType::NonUnique, ctx))
                                .Done(),
                            {}, ctx, state,
                            TCopyOrTrivialMapOpts()
                                .SetTryKeepSortness(!NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered))
                                .SetSectionUniq(section.Ref().GetConstraint<TDistinctConstraintNode>())
                                .SetConstraints(section.Ref().GetConstraintSet())
                                .SetCombineChunks(true)
                            );

                        syncList[path.Table().Cast<TYtOutput>().Operation().Ptr()] = syncList.size();

                        if (singleSection) {
                            return ctx.ChangeChild(*input, TYtTableContent::idx_Input, path.Table().Ptr());
                        } else {
                            newSections.push_back(Build<TYtSection>(ctx, section.Pos())
                                .Paths()
                                    .Add(path)
                                .Build()
                                .Settings().Build()
                                .Done());
                        }

                    } else {
                        newSections.push_back(section);
                    }

                }
                if (materialize) {
                    auto newRead = Build<TYtReadTable>(ctx, read.Cast().Pos())
                        .InitFrom(read.Cast())
                        .Input()
                            .Add(newSections)
                        .Build()
                        .Done();

                    return ctx.ChangeChild(*input, TYtTableContent::idx_Input, newRead.Ptr());
                }
            }
            else if (auto out = TYtTableContent(input).Input().Maybe<TYtOutput>()) {
                auto oldOp = GetOutputOp(out.Cast());
                if (!oldOp.Maybe<TYtMerge>() || !NYql::HasSetting(oldOp.Cast<TYtMerge>().Settings().Ref(), EYtSettingType::CombineChunks)) {
                    auto outTable = GetOutTable(out.Cast());
                    TYtOutTableInfo tableInfo(outTable);
                    if (auto tableStat = tableInfo.Stat) {
                        if (tableStat->ChunkCount > maxChunks || (tableStat->ChunkCount > 1 && tableStat->DataSize / tableStat->ChunkCount < minChunkSize)) {
                            auto newOp = Build<TYtMerge>(ctx, input->Pos())
                                .World(world)
                                .DataSink(oldOp.DataSink())
                                .Output()
                                    .Add()
                                        .InitFrom(outTable.Cast<TYtOutTable>())
                                        .Name().Value("").Build()
                                        .Stat<TCoVoid>().Build()
                                    .Build()
                                .Build()
                                .Input()
                                    .Add()
                                        .Paths()
                                            .Add()
                                                .Table(out.Cast())
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
                                    .Add()
                                        .Name().Value(ToString(EYtSettingType::ForceTransform)).Build()
                                    .Build()
                                .Build()
                                .Done();

                            syncList[newOp.Ptr()] = syncList.size();

                            auto newOutput = Build<TYtOutput>(ctx, input->Pos())
                                .Operation(newOp)
                                .OutIndex().Value(0U).Build()
                                .Done().Ptr();

                            return ctx.ChangeChild(*input, TYtTableContent::idx_Input, std::move(newOutput));
                        }
                    }
                }
            }
        }
        return input;
    }, ctx, settings);

    if (status.Level == IGraphTransformer::TStatus::Error) {
        return {};
    }

    if (status.Level == IGraphTransformer::TStatus::Ok) {
        return node;
    }

    if (!syncList.empty()) {
        using TPair = std::pair<TExprNode::TPtr, ui64>;
        TVector<TPair> sortedList(syncList.cbegin(), syncList.cend());
        TExprNode::TListType syncChildren;
        syncChildren.push_back(res->ChildPtr(TYtOutputOpBase::idx_World));
        ::Sort(sortedList, [](const TPair& x, const TPair& y) { return x.second < y.second; });
        for (auto& x: sortedList) {
            auto world = ctx.NewCallable(node.Pos(), TCoLeft::CallableName(), { x.first });
            syncChildren.push_back(world);
        }

        res = ctx.ChangeChild(*res, TYtOutputOpBase::idx_World,
            ctx.NewCallable(node.Pos(), TCoSync::CallableName(), std::move(syncChildren)));
    }

    return TExprBase(res);
}

}  // namespace NYql
