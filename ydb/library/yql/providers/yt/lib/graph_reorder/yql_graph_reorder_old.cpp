#include "yql_graph_reorder_old.h"
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

TDependencyUpdaterOld::TDependencyUpdaterOld(TStringBuf provider, TStringBuf newConfigureName)
    : Provider(provider)
    , NewConfigureName(newConfigureName)
{
}

void TDependencyUpdaterOld::ScanConfigureDeps(const TExprNode::TPtr& node) {
    VisitedNodes.insert(node.Get());

    TMaybe<TExprNode*> prevConfigure;
    TMaybe<TExprNode*> prevWrite;
    TMaybe<TVector<TExprNode*>> prevReads;
    bool popRead = false;
    // Don't use TCoConfigure().DataSource() - result provider uses DataSink here
    const bool isConfigure = TCoConfigure::Match(node.Get()) && node->Child(TCoConfigure::idx_DataSource)->Child(0)->Content() == Provider;
    const bool isNewConfigure = node->IsCallable(NewConfigureName);
    if (isConfigure || isNewConfigure) {
        if (LastConfigure) {
            NodeToConfigureDeps[LastConfigure].insert(node);
        }
        if (isConfigure) {
            Configures.push_back(node);
            // All Configure! nodes must be present in NodeToConfigureDeps
            NodeToConfigureDeps.emplace(node.Get(), THashSet<TExprNode::TPtr, TTExprNodePtrHash>());
        }
        prevConfigure = LastConfigure;
        LastConfigure = node.Get();

        if (LastWrite) {
            NodeToConfigureDeps[LastWrite].insert(node);
        }
        prevWrite = LastWrite;
        LastWrite = nullptr;

        for (auto read: LastReads) {
            NodeToConfigureDeps[read].insert(node);
        }
        prevReads.ConstructInPlace();
        prevReads->swap(LastReads);
    }
    else if (TCoRead::Match(node.Get()) && TCoRead(node).DataSource().Category().Value() == Provider) {
        LastReads.push_back(node.Get());
        popRead = true;
    }
    else if (TCoWrite::Match(node.Get())) {
        auto category = TCoWrite(node).DataSink().Category().Value();
        if (Provider == category || ResultProviderName == category) {
            prevWrite = LastWrite;
            LastWrite = node.Get();
        }
    }

    if (!isNewConfigure) { // Assume all nodes under provider specific Configure are already processed
        for (const auto& child : node->Children()) {
            if (VisitedNodes.cend() == VisitedNodes.find(child.Get()) || child->Content().EndsWith('!')) {
                ScanConfigureDeps(child);
            }
        }
    }

    if (prevConfigure) {
        LastConfigure = *prevConfigure;
    }
    if (prevWrite) {
        LastWrite = *prevWrite;
    }
    if (prevReads) {
        LastReads.swap(*prevReads);
    }
    if (popRead) {
        LastReads.pop_back();
    }
};

void TDependencyUpdaterOld::ScanNewReadDeps(const TExprNode::TPtr& input, TExprContext& ctx) {
    THashMap<ui32, TExprNode::TPtr> commits;
    VisitExprByFirst(input, [&](const TExprNode::TPtr& node) {
        if (TCoCommit::Match(node.Get())) {
            auto commit = TCoCommit(node);
            if (commit.DataSink().Cast<TCoDataSink>().Category().Value() == Provider) {
                auto settings = NCommon::ParseCommitSettings(commit, ctx);
                if (!settings.Epoch) {
                    return false;
                }

                ui32 commitEpoch = FromString<ui32>(settings.Epoch.Cast().Value());
                commits.emplace(commitEpoch, node);
            }
        }
        return true;
    });

    TNodeMap<ui32> maxEpochs;
    VisitExpr(input, [&](const TExprNode::TPtr& node) {
        if (TCoRead::Match(node.Get()) && TCoRead(node).DataSource().Category().Value() == Provider) {
            if (TMaybe<ui32> maxEpoch = GetReadEpoch(node, ctx)) {
                auto world = node->Child(0);
                if (*maxEpoch == 0) {
                    if (world->Type() != TExprNode::World
                        && !TCoSync::Match(world)
                        && !world->IsCallable(NewConfigureName))
                    {
                        NewReadToCommitDeps[node.Get()] = TExprNode::TPtr();
                    }
                }
                else if (*maxEpoch > 0) {
                    auto commit = commits.find(*maxEpoch);
                    YQL_ENSURE(commits.cend() != commit);
                    if (world != commit->second.Get()
                        && !TCoSync::Match(world)
                        && !world->IsCallable(NewConfigureName))
                    {
                        NewReadToCommitDeps[node.Get()] = commit->second;
                    }
                }
            }
        }
        return true;
    });
}

IGraphTransformer::TStatus TDependencyUpdaterOld::ReorderGraph(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx)
{
    ScanConfigureDeps(input);
    ScanNewReadDeps(input, ctx);

    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;

    output = input;
    if (!NewReadToCommitDeps.empty()) {
        if (!NodeToConfigureDeps.empty()) {
            // Exclude all our Configure! from the graph
            status = status.Combine(OptimizeExpr(output, output, [&](const TExprNode::TPtr& node, TExprContext& /*ctx*/) -> TExprNode::TPtr {
                if (TCoConfigure::Match(node.Get()) && NodeToConfigureDeps.contains(node.Get())) {
                    // Skip Configure! itself
                    return node;
                }
                if (node->ChildrenSize() > 0) {
                    TExprNode::TPtr next = node->ChildPtr(0);
                    while (TCoConfigure::Match(next.Get()) && NodeToConfigureDeps.contains(next.Get())) {
                        next = next->ChildPtr(0);
                    }
                    if (next != node->ChildPtr(0)) {
                        node->ChildRef(0) = next;
                    }
                }
                return node;
            }, ctx, settings));

            if (status.Level == IGraphTransformer::TStatus::Error) {
                return status;
            }

            // Update root
            while (TCoConfigure::Match(output.Get()) && NodeToConfigureDeps.contains(output.Get())) {
                output = output->ChildPtr(0);
            }

            // Make a separate chain of Configure! nodes
            for (auto& item: NodeToConfigureDeps) {
                if (!TCoConfigure::Match(item.first)) {
                    continue;
                }
                if (item.second.empty()) {
                    if (item.first->Child(0)->Type() != TExprNode::World) {
                        item.first->ChildRef(0) = ctx.NewWorld(item.first->Pos());
                    }
                }
                else if (item.second.size() > 1) {
                    item.first->ChildRef(0) = ctx.NewCallable(item.first->Pos(), TCoSync::CallableName(), TExprNode::TListType(item.second.begin(), item.second.end()));
                }
                else if (item.first->ChildPtr(0) != *item.second.begin()) {
                    item.first->ChildRef(0) = *item.second.begin();
                }
            }
        }

        // Reorder graph
        TNodeOnNodeOwnedMap oldReadDeps;
        status = status.Combine(OptimizeExpr(output, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (node->ChildrenSize() > 0 && TCoLeft::Match(node->Child(0))) {
                auto read = node->Child(0)->Child(0);
                auto found = oldReadDeps.find(read);
                if (found != oldReadDeps.cend()) {
                    node->ChildRef(0) = found->second;
                }
                else if (read->ChildrenSize() > 1 && read->Child(1)->Child(0)->Content() == Provider) {
                    node->ChildRef(0) = read->ChildPtr(0);
                }
            }

            if (TCoRight::Match(node.Get())) {
                auto read = node->ChildPtr(0);
                auto confSet = NodeToConfigureDeps.FindPtr(read.Get());
                auto found = NewReadToCommitDeps.find(read.Get());
                if (NewReadToCommitDeps.cend() != found) {
                    TExprNode::TPtr newWorld;
                    if (!found->second) {
                        if (confSet) {
                            YQL_ENSURE(!confSet->empty());
                            newWorld = confSet->size() == 1
                                ? *confSet->begin()
                                : ctx.NewCallable(read->Pos(), TCoSync::CallableName(), TExprNode::TListType(confSet->begin(), confSet->end()));
                        }
                        else if (read->Child(0)->Type() != TExprNode::World) {
                            newWorld = ctx.NewWorld(read->Pos());
                        }
                    }
                    else {
                        if (confSet) {
                            auto syncChildren = TExprNode::TListType(confSet->begin(), confSet->end());
                            syncChildren.push_back(found->second);
                            newWorld = ctx.NewCallable(read->Pos(), TCoSync::CallableName(), std::move(syncChildren));
                        }
                        else if (read->Child(0) != found->second.Get()) {
                            newWorld = found->second;
                        }
                    }

                    if (newWorld) {
                        oldReadDeps[read.Get()] = read->ChildPtr(0);
                        read->ChildRef(0) = newWorld;
                    }
                }
                else if (confSet) {
                    auto syncChildren = TExprNode::TListType(confSet->begin(), confSet->end());
                    syncChildren.push_back(read->ChildPtr(0));
                    read->ChildRef(0) = ctx.NewCallable(read->Pos(), TCoSync::CallableName(), std::move(syncChildren));
                }
            }
            else if (TCoWrite::Match(node.Get())) {
                if (auto confSet = NodeToConfigureDeps.FindPtr(node.Get())) {
                    auto syncChildren = TExprNode::TListType(confSet->begin(), confSet->end());
                    syncChildren.push_back(node->ChildPtr(0));
                    node->ChildRef(0) = ctx.NewCallable(node->Pos(), TCoSync::CallableName(), std::move(syncChildren));
                }
            }
            return node;
        }, ctx, settings));

        if (status.Level == IGraphTransformer::TStatus::Error) {
            return status;
        }

        YQL_CLOG(INFO, ProviderYt) << "DependencyUpdater-ReorderGraph";
    }

    if (!NodeToConfigureDeps.empty()) {
        // Rename Configure! nodes
        status = status.Combine(OptimizeExpr(output, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            // Don't use TCoConfigure().DataSource() - result provider uses DataSink here
            if (TCoConfigure::Match(node.Get()) && node->Child(TCoConfigure::idx_DataSource)->Child(0)->Content() == Provider) {
                return ctx.RenameNode(*node, NewConfigureName);
            }
            return node;
        }, ctx, settings));
    }

    return status;
}

} // namespace NYql
