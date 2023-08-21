#include "yql_yt_provider_impl.h"
#include "yql_yt_table.h"
#include "yql_yt_op_settings.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/graph_reorder/yql_graph_reorder_old.h>
#include <ydb/library/yql/providers/yt/lib/graph_reorder/yql_graph_reorder.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/strbuf.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYql {

using namespace NNodes;

class TYtEpochTransformer : public TSyncTransformerBase {
public:
    TYtEpochTransformer(TYtState::TPtr state)
        : State_(state)
    {
    }

    void Rewind() final {
        State_->NextEpochId = 1;
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (ctx.Step.IsDone(TExprStep::Epochs)) {
            return TStatus::Ok;
        }

        TNodeMap<ui32> commitEpochs;
        TStatus status = AssignCommitEpochs(input, output, ctx, commitEpochs);
        if (status.Level == TStatus::Error) {
            return status;
        }

        TNodeMap<THashSet<TString>> tableWritesBeforeCommit;
        status = status.Combine(AssignWriteEpochs(output, output, ctx, commitEpochs, tableWritesBeforeCommit));
        if (status.Level == TStatus::Error) {
            return status;
        }

        status = status.Combine(AssignUseEpochs(output, output, ctx, commitEpochs, tableWritesBeforeCommit));
        if (status.Level == TStatus::Error) {
            return status;
        }

        if (State_->Configuration->_EnableWriteReorder.Get().GetOrElse(true)) {
            return status.Combine(TYtDependencyUpdater().ReorderGraph(output, output, ctx));
        } else {
            return status.Combine(TYtDependencyUpdaterOld().ReorderGraph(output, output, ctx));
        }
    }

private:
    class TYtDependencyUpdater: public TDependencyUpdater {
    public:
        TYtDependencyUpdater()
            : TDependencyUpdater(YtProviderName, TYtConfigure::CallableName())
        {
        }

        TMaybe<ui32> GetReadEpoch(const TExprNode::TPtr& readNode) const final {
            TYtRead read(readNode);
            TMaybe<ui32> maxEpoch;
            if (auto list = read.Arg(2).Maybe<TExprList>()) {
                for (auto item: list.Cast()) {
                    TMaybeNode<TYtTable> table = item.Maybe<TYtPath>().Table().Maybe<TYtTable>();
                    if (!table) {
                        table = item.Maybe<TYtTable>();
                    }
                    if (table) {
                        maxEpoch = Max(maxEpoch.GetOrElse(0), TEpochInfo::Parse(table.Cast().Epoch().Ref()).GetOrElse(0));
                    }
                }
            }
            return maxEpoch;
        }

        TString GetWriteTarget(const TExprNode::TPtr& node) const final {
            TYtWrite write(node);
            return TStringBuilder() << "yt;" << write.DataSink().Cluster().Value() << ';' << TYtTableInfo(write.Arg(2)).Name;
        }
    };

    class TYtDependencyUpdaterOld: public TDependencyUpdaterOld {
    public:
        TYtDependencyUpdaterOld()
            : TDependencyUpdaterOld(YtProviderName, TYtConfigure::CallableName())
        {
        }

        TMaybe<ui32> GetReadEpoch(const TExprNode::TPtr& readNode, TExprContext& /*ctx*/) const final {
            TYtRead read(readNode);
            TMaybe<ui32> maxEpoch;
            if (auto list = read.Arg(2).Maybe<TExprList>()) {
                for (auto item: list.Cast()) {
                    TMaybeNode<TYtTable> table = item.Maybe<TYtPath>().Table().Maybe<TYtTable>();
                    if (!table) {
                        table = item.Maybe<TYtTable>();
                    }
                    if (table) {
                        maxEpoch = Max(maxEpoch.GetOrElse(0), TEpochInfo::Parse(table.Cast().Epoch().Ref()).GetOrElse(0));
                    }
                }
            }
            return maxEpoch;
        }
    };

    TStatus AssignCommitEpochs(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
        TNodeMap<ui32>& commitEpochs)
    {
        TOptimizeExprSettings settings(nullptr);
        settings.VisitChanges = true;
        auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            // Assign uniq epoch for each commit
            if (auto maybeCommit = TMaybeNode<TCoCommit>(node)) {
                auto commit = maybeCommit.Cast();

                if (!commit.DataSink().Maybe<TYtDSink>()) {
                    return node;
                }

                auto settings = NCommon::ParseCommitSettings(commit, ctx);
                if (settings.Epoch) {
                    commitEpochs[node.Get()] = FromString<ui32>(settings.Epoch.Cast().Value());
                    return node;
                }

                const ui32 commitEpoch = State_->NextEpochId++;
                settings.Epoch = Build<TCoAtom>(ctx, commit.Pos())
                    .Value(ToString(commitEpoch))
                    .Done();

                auto ret = Build<TCoCommit>(ctx, commit.Pos())
                    .World(commit.World())
                    .DataSink(commit.DataSink())
                    .Settings(settings.BuildNode(ctx))
                    .Done();

                commitEpochs[ret.Raw()] = commitEpoch;

                return ret.Ptr();
            }
            return node;
        }, ctx, settings);

        if (input != output) {
            YQL_CLOG(INFO, ProviderYt) << "Epoch-AssignCommitEpochs";
        }

        return status;
    }

    TVector<TYtWrite> FindWritesBeforeCommit(const TCoCommit& commit) const {
        auto cluster = commit.DataSink().Cast<TYtDSink>().Cluster().Value();

        TVector<TYtWrite> writes;
        VisitExprByFirst(commit.Ptr(), [&](const TExprNode::TPtr& node) {
            if (auto maybeWrite = TMaybeNode<TYtWrite>(node)) {
                if (auto ds = maybeWrite.DataSink()) { // Validate provider
                    if (ds.Cast().Cluster().Value() == cluster) {
                        writes.push_back(maybeWrite.Cast());
                    }
                }
            } else if (auto ds = TMaybeNode<TCoCommit>(node).DataSink().Maybe<TYtDSink>()) {
                if (ds.Cast().Cluster().Value() == cluster && commit.Ptr().Get() != node.Get()) {
                    return false; // Stop traversing
                }
            }
            return true;
        });

        return writes;
    }

    TStatus AssignWriteEpochs(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
        const TNodeMap<ui32>& commitEpochs, TNodeMap<THashSet<TString>>& tableWritesBeforeCommit)
    {
        if (commitEpochs.empty()) {
            return TStatus::Ok;
        }

        TExprNode::TListType commits;
        VisitExprByFirst(input, [&](const TExprNode::TPtr& node) {
            if (auto ds = TMaybeNode<TCoCommit>(node).DataSink().Maybe<TYtDSink>()) {
                commits.push_back(node);
            }
            return true;
        });

        TNodeMap<ui32> writeCommitEpochs;
        // Find all writes before commits
        for (auto& commitNode: commits) {
            auto commit = TCoCommit(commitNode);
            auto it = commitEpochs.find(commitNode.Get());
            YQL_ENSURE(it != commitEpochs.end());
            const ui32 commitEpoch = it->second;

            TVector<TYtWrite> writes = FindWritesBeforeCommit(commit);
            THashMap<TString, TVector<TExprNode::TPtr>> writesByTable;
            THashSet<TString> tableNames;
            for (auto write: writes) {
                TYtTableInfo tableInfo(write.Arg(2));
                if (!tableInfo.CommitEpoch.Defined()) {
                    writeCommitEpochs[write.Raw()] = commitEpoch;
                } else if (*tableInfo.CommitEpoch != commitEpoch) {
                    ctx.AddError(TIssue(ctx.GetPosition(write.Pos()), TStringBuilder()
                        << "Table " << tableInfo.Name.Quote() << " write belongs to multiple commits with different epochs"));
                    return TStatus::Error;
                }
                tableNames.insert(tableInfo.Name);

                // Graph is traversed from the end, so store the last write for each table, which is
                // actually executed first
                writesByTable[tableInfo.Name].push_back(write.Ptr());
            }
            for (auto& w: writesByTable) {
                if (auto cnt = std::count_if(w.second.begin(), w.second.end(), [](const auto& p) { return NYql::HasSetting(*p->Child(4), EYtSettingType::Initial); })) {
                    YQL_ENSURE(cnt == 1, "Multiple 'initial' writes to the same table " << w.first);
                    continue;
                }
                if (w.second.size() > 1) {
                    std::stable_sort(w.second.begin(), w.second.end(), [](const auto& p1, const auto& p2) { return p1->UniqueId() < p2->UniqueId(); });
                }
                w.second.front()->ChildRef(4) = NYql::AddSetting(*w.second.front()->Child(4), EYtSettingType::Initial, {}, ctx);
            }
            if (!tableNames.empty()) {
                tableWritesBeforeCommit.emplace(commitNode.Get(), std::move(tableNames));
            }
        }

        if (writeCommitEpochs.empty()) {
            return TStatus::Ok;
        }

        TOptimizeExprSettings settings(nullptr);
        settings.VisitChanges = true;
        auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (node->IsCallable(TYtWrite::CallableName())) {
                const auto it = writeCommitEpochs.find(node.Get());
                if (writeCommitEpochs.cend() != it) {
                    TYtTableInfo tableInfo = node->ChildPtr(2);
                    tableInfo.CommitEpoch = it->second;
                    node->ChildRef(2) = tableInfo.ToExprNode(ctx, node->Pos()).Ptr();
                }
            }
            return node;
        }, ctx, settings);

        YQL_CLOG(INFO, ProviderYt) << "Epoch-AssignWriteEpochs";
        return status;
    }

    TVector<const TExprNode*> FindCommitsBeforeNode(const TExprNode& startNode, TStringBuf cluster,
        const TNodeMap<THashSet<TString>>& tableWritesBeforeCommit)
    {
        TVector<const TExprNode*> commits;
        VisitExprByFirst(startNode, [&](const TExprNode& node) {
            if (auto ds = TMaybeNode<TCoCommit>(&node).DataSink().Maybe<TYtDSink>()) {
                if (tableWritesBeforeCommit.find(&node) != tableWritesBeforeCommit.end() && ds.Cast().Cluster().Value() == cluster) {
                    commits.push_back(&node);
                }
            }
            return true;
        });
        return commits;
    }

    TStatus AssignUseEpochs(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
        const TNodeMap<ui32>& commitEpochs, const TNodeMap<THashSet<TString>>& tableWritesBeforeCommit)
    {
        TNodeMap<TNodeMap<ui32>> ioEpochs;
        VisitExpr(input, [&](const TExprNode::TPtr& node) {
            if (auto ds = TMaybeNode<TYtRead>(node).DataSource()) {
                if (tableWritesBeforeCommit.empty()) {
                    return true;
                }
                auto clusterName = ds.Cast().Cluster().Value();
                auto commitDeps = FindCommitsBeforeNode(*node, clusterName, tableWritesBeforeCommit);
                if (commitDeps.empty()) {
                    return true;
                }

                TYtRead read(node);
                if (auto list = read.Arg(2).Maybe<TExprList>()) {
                    THashMap<TString, ui32> tableEpochs;
                    for (auto item: list.Cast()) {
                        TMaybeNode<TYtTable> tableNode = item.Maybe<TYtPath>().Table().Maybe<TYtTable>();
                        if (!tableNode) {
                            tableNode = item.Maybe<TYtTable>();
                        }

                        if (tableNode) {
                            TYtTableInfo tableInfo(tableNode.Cast());
                            if (!tableInfo.Epoch.Defined()) {
                                TMaybe<ui32> epoch;
                                if (auto p = tableEpochs.FindPtr(tableInfo.Name)) {
                                    epoch = *p;
                                }
                                else {
                                    for (auto commit: commitDeps) {
                                        auto itWrites = tableWritesBeforeCommit.find(commit);
                                        if (itWrites != tableWritesBeforeCommit.end() && itWrites->second.contains(tableInfo.Name)) {
                                            auto itCommit = commitEpochs.find(commit);
                                            YQL_ENSURE(itCommit != commitEpochs.end());
                                            epoch = Max<ui32>(itCommit->second, epoch.GetOrElse(0));
                                        }
                                    }
                                }
                                tableEpochs[tableInfo.Name] = epoch.GetOrElse(0);

                                if (0 != epoch.GetOrElse(0)) {
                                    ioEpochs[node.Get()][tableNode.Raw()] = *epoch;
                                    State_->EpochDependencies[*epoch].emplace(clusterName, tableInfo.Name);
                                }
                            }
                        }
                    }
                }
            }
            else if (auto ds = TMaybeNode<TYtWrite>(node).DataSink()) {
                if (tableWritesBeforeCommit.empty()) {
                    return true;
                }
                auto clusterName = ds.Cast().Cluster().Value();
                auto commitDeps = FindCommitsBeforeNode(*node, clusterName, tableWritesBeforeCommit);
                if (commitDeps.empty()) {
                    return true;
                }

                TYtWrite write(node);
                if (write.Arg(2).Maybe<TYtTable>()) {
                    TYtTableInfo tableInfo(write.Arg(2));
                    if (!tableInfo.Epoch.Defined()) {

                        TMaybe<ui32> epoch;
                        for (auto commit: commitDeps) {
                            auto itWrites = tableWritesBeforeCommit.find(commit);
                            if (itWrites != tableWritesBeforeCommit.end() && itWrites->second.contains(tableInfo.Name)) {
                                auto itCommit = commitEpochs.find(commit);
                                YQL_ENSURE(itCommit != commitEpochs.end());
                                epoch = Max<ui32>(itCommit->second, epoch.GetOrElse(0));
                            }
                        }

                        if (0 != epoch.GetOrElse(0)) {
                            ioEpochs[node.Get()][write.Arg(2).Raw()] = *epoch;
                            State_->EpochDependencies[*epoch].emplace(clusterName, tableInfo.Name);
                        }
                    }
                }
            }
            else if (auto maybeRead = TMaybeNode<TYtReadTable>(node)) {
                auto cluster = TString{maybeRead.Cast().DataSource().Cluster().Value()};
                for (auto section: maybeRead.Cast().Input()) {
                    for (auto path: section.Paths()) {
                        if (auto table = path.Table().Maybe<TYtTable>()) {
                            if (auto epoch = TEpochInfo::Parse(table.Epoch().Ref()).GetOrElse(0)) {
                                State_->EpochDependencies[epoch].emplace(cluster, table.Cast().Name().Value());
                            }
                        }
                    }
                }
            }
            else if (auto maybeOp = TMaybeNode<TYtTransientOpBase>(node)) {
                auto cluster = TString{maybeOp.Cast().DataSink().Cluster().Value()};
                for (auto section: maybeOp.Cast().Input()) {
                    for (auto path: section.Paths()) {
                        if (auto table = path.Table().Maybe<TYtTable>()) {
                            if (auto epoch = TEpochInfo::Parse(table.Epoch().Ref()).GetOrElse(0)) {
                                State_->EpochDependencies[epoch].emplace(cluster, table.Cast().Name().Value());
                            }
                        }
                    }
                }
            }
            else if (auto maybePublish = TMaybeNode<TYtPublish>(node)) {
                auto cluster = TString{maybePublish.Cast().DataSink().Cluster().Value()};
                auto table = maybePublish.Cast().Publish();
                if (auto epoch = TEpochInfo::Parse(table.Epoch().Ref()).GetOrElse(0)) {
                    State_->EpochDependencies[epoch].emplace(cluster, table.Name().Value());
                }
            }
            else if (auto maybeDrop = TMaybeNode<TYtDropTable>(node)) {
                auto cluster = TString{maybeDrop.Cast().DataSink().Cluster().Value()};
                auto table = maybeDrop.Cast().Table();
                if (auto epoch = TEpochInfo::Parse(table.Epoch().Ref()).GetOrElse(0)) {
                    State_->EpochDependencies[epoch].emplace(cluster, table.Name().Value());
                }
            }
            else if (auto maybeScheme = TMaybeNode<TYtReadTableScheme>(node)) {
                auto cluster = TString{maybeScheme.Cast().DataSource().Cluster().Value()};
                auto table = maybeScheme.Cast().Table();
                if (auto epoch = TEpochInfo::Parse(table.Epoch().Ref()).GetOrElse(0)) {
                    State_->EpochDependencies[epoch].emplace(cluster, table.Name().Value());
                }
            }
            else if (auto maybeWrite = TMaybeNode<TYtWriteTable>(node)) {
                auto cluster = TString{maybeWrite.Cast().DataSink().Cluster().Value()};
                auto table = maybeWrite.Cast().Table();
                if (auto epoch = TEpochInfo::Parse(table.Epoch().Ref()).GetOrElse(0)) {
                    State_->EpochDependencies[epoch].emplace(cluster, table.Name().Value());
                }
            }
            return true;
        });

        if (ioEpochs.empty()) {
            return TStatus::Ok;
        }

        TOptimizeExprSettings settings(nullptr);
        settings.VisitChanges = true;
        auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (TYtRead::Match(node.Get()) || TYtWrite::Match(node.Get())) {
                const auto it = ioEpochs.find(node.Get());
                if (ioEpochs.cend() != it) {
                    auto tables = node->ChildPtr(2);
                    auto& tableEpochs = it->second;
                    TOptimizeExprSettings subSettings(nullptr);
                    subSettings.VisitChanges = true;
                    auto subStatus = OptimizeExpr(tables, tables, [&tableEpochs](const TExprNode::TPtr& subNode, TExprContext& ctx) -> TExprNode::TPtr {
                        if (TYtTable::Match(subNode.Get())) {
                            auto tableIt = tableEpochs.find(subNode.Get());
                            if (tableEpochs.cend() != tableIt) {
                                TYtTableInfo tableInfo = subNode;
                                tableInfo.Epoch = tableIt->second;
                                return tableInfo.ToExprNode(ctx, subNode->Pos()).Ptr();
                            }
                        }
                        return subNode;
                    }, ctx, subSettings);
                    if (subStatus.Level != TStatus::Ok) {
                        return {};
                    }

                    ioEpochs.erase(it);
                    node->ChildRef(2) = std::move(tables);
                }
            }

            return node;
        }, ctx, settings);

        YQL_CLOG(INFO, ProviderYt) << "Epoch-AssignUseEpochs";
        return status;
    }

private:
    TYtState::TPtr State_;
};

THolder<IGraphTransformer> CreateYtEpochTransformer(TYtState::TPtr state) {
    return THolder(new TYtEpochTransformer(state));
}

}
