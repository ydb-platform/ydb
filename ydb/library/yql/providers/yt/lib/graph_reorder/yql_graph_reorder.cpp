#include "yql_graph_reorder.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>

#include <tuple>

namespace NYql {

using namespace NNodes;

TDependencyUpdater::TDependencyUpdater(TStringBuf provider, TStringBuf newConfigureName)
    : Provider(provider)
    , NewConfigureName(newConfigureName)
{
}

void TDependencyUpdater::ScanConfigureDeps(const TExprNode::TPtr& node) {
    VisitedNodes.insert(node.Get());

    TMaybe<TExprNode*> prevConfigure;
    TMaybe<TVector<TExprNode*>> prevReads;
    bool popRead = false;
    // Don't use TCoConfigure().DataSource() - result provider uses DataSink here
    const bool isConfigure = TCoConfigure::Match(node.Get()) && node->Child(TCoConfigure::idx_DataSource)->Child(0)->Content() == Provider;
    const bool isNewConfigure = node->IsCallable(NewConfigureName);
    if (isConfigure || isNewConfigure) {
        if (LastConfigure) {
            auto& map = NodeToConfigureDeps[LastConfigure];
            map.emplace(node, map.size());
        }
        if (isConfigure) {
            Configures.push_back(node);
            // All Configure! nodes must be present in NodeToConfigureDeps
            NodeToConfigureDeps.emplace(node.Get(), TSyncMap());
        }
        prevConfigure = LastConfigure;
        LastConfigure = node.Get();

        for (auto read: LastReads) {
            auto& map = NodeToConfigureDeps[read];
            map.emplace(node, map.size());
        }
        prevReads.ConstructInPlace();
        prevReads->swap(LastReads);
    }
    else if (TCoRead::Match(node.Get()) && TCoRead(node).DataSource().Category().Value() == Provider) {
        LastReads.push_back(node.Get());
        popRead = true;
    }

    if (!isNewConfigure) { // Assume all nodes under provider specific Configure are already processed
        for (const auto& child : node->Children()) {
            if (VisitedNodes.cend() == VisitedNodes.find(child.Get()) || (child->Content() != SyncName && child->Content().EndsWith('!'))) {
                ScanConfigureDeps(child);
            }
        }
    }

    if (prevConfigure) {
        LastConfigure = *prevConfigure;
    }
    if (prevReads) {
        LastReads.swap(*prevReads);
    }
    if (popRead) {
        LastReads.pop_back();
    }
};

void TDependencyUpdater::ScanNewReadDeps(const TExprNode::TPtr& input, TExprContext& ctx) {
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
            if (TMaybe<ui32> maxEpoch = GetReadEpoch(node)) {
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

void TDependencyUpdater::ScanNewWriteDeps(const TExprNode::TPtr& input) {
    TExprNode::TPtr lastWorld;
    TExprNode::TPtr lastConfigure;
    THashMap<TString, std::tuple<TExprNode::TPtr, size_t, TExprNode::TPtr>> writesByTarget; // target table -> (last Write! node, order between different writes, last Configure! dependency)
    VisitExprByFirst(input,
        [&](const TExprNode::TPtr& node) {
            // Initially Sync! and NewConfigureName cannot appear in graph.
            // So if we catch them then this part of graph is already processed.
            if (node->IsCallable(NewConfigureName)) {
                lastConfigure = node;
                return false;
            }
            if (TCoSync::Match(node.Get()) || node->IsCallable(NewConfigureName)) {
                lastWorld = node;
                return false;
            }
            return true;
        },
        [&](const TExprNode::TPtr& node) {
            if (TCoConfigure::Match(node.Get()) && node->Child(TCoConfigure::idx_DataSource)->Child(0)->Content() == Provider) {
                lastConfigure = node;
                return true;
            }

            if (TCoWrite::Match(node.Get())) {
                auto category = TCoWrite(node).DataSink().Category().Value();
                TString target;
                if (Provider == category) {
                    target = GetWriteTarget(node);
                } else if (ResultProviderName == category) {
                    target = category;
                }
                if (target) {
                    auto& prevWrite = writesByTarget[target];
                    if (std::get<0>(prevWrite)) {
                        if (node->HeadPtr() != std::get<0>(prevWrite)) {
                            NewWriteDeps[node.Get()].push_back(std::get<0>(prevWrite));
                        }
                        if (lastConfigure && lastConfigure != std::get<2>(prevWrite)) {
                            auto& map = NodeToConfigureDeps[node.Get()];
                            map.emplace(lastConfigure, map.size());
                            std::get<2>(prevWrite) = lastConfigure;
                        }
                    } else {
                        std::get<1>(prevWrite) = writesByTarget.size();
                        if (lastWorld && node->HeadPtr() != lastWorld) {
                            NewWriteDeps[node.Get()].push_back(lastWorld);
                        }
                        if (lastConfigure) {
                            auto& map = NodeToConfigureDeps[node.Get()];
                            map.emplace(lastConfigure, map.size());
                            std::get<2>(prevWrite) = lastConfigure;
                        }
                    }
                    std::get<0>(prevWrite) = node;
                    return true;
                }
            }
            if (!writesByTarget.empty()) {
                if (writesByTarget.size() > 1) {
                    auto& deps = NewWriteDeps[node.Get()];
                    std::vector<std::pair<TExprNode::TPtr, size_t>> values;
                    std::transform(writesByTarget.begin(), writesByTarget.end(), std::back_inserter(values),
                        [](const auto& v) { return std::make_pair(std::get<0>(v.second), std::get<1>(v.second)); });
                    std::sort(values.begin(), values.end(), [](const auto& l, const auto& r) { return l.second < r.second; });
                    std::transform(values.begin(), values.end(), std::back_inserter(deps), [](const auto& v) { return v.first; });
                } else {
                    if (node->HeadPtr() != std::get<0>(writesByTarget.begin()->second)) {
                        NewWriteDeps[node.Get()].push_back(std::get<0>(writesByTarget.begin()->second));
                    }
                }
            }

            lastWorld = node;
            writesByTarget.clear();
            return true;
        }
    );
}

TExprNode::TPtr TDependencyUpdater::MakeSync(const TSyncMap& nodes, TExprNode::TChildrenType extra, TPositionHandle pos, TExprContext& ctx) {
    using TPair = std::pair<TExprNode::TPtr, ui64>;
    TVector<TPair> sortedList(nodes.cbegin(), nodes.cend());
    Sort(sortedList, [](const TPair& x, const TPair& y) { return x.second < y.second; });
    TExprNode::TListType syncChildren;
    syncChildren.reserve(sortedList.size() + extra.size());
    std::transform(sortedList.begin(), sortedList.end(), std::back_inserter(syncChildren), [](const TPair& x) { return x.first; });
    if (extra) {
        syncChildren.insert(syncChildren.end(), extra.begin(), extra.end());
    }
    syncChildren.erase(std::remove_if(syncChildren.begin(), syncChildren.end(), [](const auto& n) { return n->IsWorld(); } ), syncChildren.end());
    return syncChildren.empty()
        ? ctx.NewWorld(pos)
        : syncChildren.size() == 1
              ? syncChildren.front()
              : ctx.NewCallable(pos, TCoSync::CallableName(), std::move(syncChildren));
}

IGraphTransformer::TStatus TDependencyUpdater::ReorderGraph(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx)
{
    ScanConfigureDeps(input);
    ScanNewReadDeps(input, ctx);
    ScanNewWriteDeps(input);

    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;

    output = input;
    if (!NewReadToCommitDeps.empty() || !NewWriteDeps.empty()) {
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
                else {
                    item.first->ChildRef(0) = MakeSync(item.second, {}, item.first->Pos(), ctx);
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
                            newWorld = MakeSync(*confSet, {}, read->Pos(), ctx);
                        }
                        else if (read->Child(0)->Type() != TExprNode::World) {
                            newWorld = ctx.NewWorld(read->Pos());
                        }
                    }
                    else {
                        if (confSet) {
                            newWorld = MakeSync(*confSet, {found->second}, read->Pos(), ctx);
                        }
                        else if (read->Child(0) != found->second.Get()) {
                            newWorld = found->second;
                        }
                    }

                    if (newWorld && oldReadDeps.emplace(read.Get(), read->ChildPtr(0)).second) {
                        read->ChildRef(0) = newWorld;
                    }
                }
                else if (confSet) {
                    read->ChildRef(0) = MakeSync(*confSet, {read->HeadPtr()}, read->Pos(), ctx);
                }
            }
            else if (!TCoRead::Match(node.Get())) {
                auto deps = NewWriteDeps.Value(node.Get(), TExprNode::TListType());
                if (auto confSet = NodeToConfigureDeps.FindPtr(node.Get())) {
                    if (deps.empty()) {
                        deps.push_back(node->HeadPtr());
                    }
                    node->ChildRef(0) = MakeSync(*confSet, deps, node->Pos(), ctx);
                } else if (!deps.empty()) {
                    node->ChildRef(0) = MakeSync({}, deps, node->Pos(), ctx);
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
