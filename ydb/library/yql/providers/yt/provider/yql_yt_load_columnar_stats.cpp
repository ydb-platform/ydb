#include "yql_yt_provider_impl.h"
#include "yql_yt_gateway.h"
#include "yql_yt_helpers.h"

#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <library/cpp/threading/future/async.h>
#include <ydb/library/yql/utils/log/log.h>


namespace NYql {

namespace {

using namespace NNodes;

class TPathStatusState : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TPathStatusState>;

    void EnsureNoInflightRequests() const {
        TGuard<TMutex> guard(Lock);
        YQL_ENSURE(PathStatResults.empty());
    }

    TNodeMap<IYtGateway::TPathStatResult> PullPathStatResults() {
        TNodeMap<IYtGateway::TPathStatResult> results;
        TGuard<TMutex> guard(Lock);
        results.swap(PathStatResults);
        return results;
    }

    void MarkReady(TExprNode* node, const IYtGateway::TPathStatResult& result) {
        TGuard<TMutex> guard(Lock);
        YQL_ENSURE(PathStatResults.count(node) == 0);
        PathStatResults[node] = result;
    }

private:
    mutable TMutex Lock;
    TNodeMap<IYtGateway::TPathStatResult> PathStatResults;
};

class TYtLoadColumnarStatsTransformer : public TGraphTransformerBase {
public:
    TYtLoadColumnarStatsTransformer(TYtState::TPtr state)
        : State_(state)
        , PathStatusState(new TPathStatusState)
    {
    }

    void Rewind() final {
        PathStatusState = MakeIntrusive<TPathStatusState>();
        AsyncFuture = {};
    }

private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;

        PathStatusState->EnsureNoInflightRequests();
        TVector<std::pair<IYtGateway::TPathStatOptions, TExprNode*>> pathStatArgs;
        bool hasError = false;
        TNodeOnNodeOwnedMap sectionRewrites;
        VisitExpr(input, [this, &pathStatArgs, &hasError, &sectionRewrites, &ctx](const TExprNode::TPtr& node) {
            if (auto maybeSection = TMaybeNode<TYtSection>(node)) {
                TYtSection section = maybeSection.Cast();
                if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::StatColumns)) {
                    auto columnList = NYql::GetSettingAsColumnList(section.Settings().Ref(), EYtSettingType::StatColumns);

                    TMaybe<TString> cluster;
                    TVector<IYtGateway::TPathStatReq> pathStatReqs;
                    size_t idx = 0;
                    for (auto path: section.Paths()) {
                        bool hasStat = false;
                        if (path.Table().Maybe<TYtTable>().Stat().Maybe<TYtStat>()) {
                            hasStat = true;
                        } else if (path.Table().Maybe<TYtOutTable>().Stat().Maybe<TYtStat>()) {
                            hasStat = true;
                        } else if (auto ytOutput = path.Table().Maybe<TYtOutput>()) {
                            auto outTable = GetOutTable(ytOutput.Cast());
                            if (outTable.Maybe<TYtOutTable>().Stat().Maybe<TYtStat>()) {
                                hasStat = true;
                            }
                        }

                        if (!hasStat) {
                            YQL_CLOG(INFO, ProviderYt) << "Removing columnar stat from YtSection #" << section.Ref().UniqueId()
                                                       << " due to missing stats in path #" << idx;

                            sectionRewrites[section.Raw()] = Build<TYtSection>(ctx, section.Ref().Pos())
                                .InitFrom(section)
                                .Settings(NYql::RemoveSetting(section.Settings().Ref(), EYtSettingType::StatColumns, ctx))
                                .Done().Ptr();
                        }

                        if (!sectionRewrites.empty()) {
                            // no need to prepare columnar stat requests in this case
                            return !hasError;
                        }

                        TYtPathInfo pathInfo(path);
                        YQL_ENSURE(pathInfo.Table->Stat);

                        TString currCluster;
                        if (auto ytTable = path.Table().Maybe<TYtTable>()) {
                            currCluster = TString{ytTable.Cast().Cluster().Value()};
                        } else {
                            currCluster = TString{GetOutputOp(path.Table().Cast<TYtOutput>()).DataSink().Cluster().Value()};
                        }
                        YQL_ENSURE(currCluster);

                        if (cluster) {
                            YQL_ENSURE(currCluster == *cluster);
                        } else {
                            cluster = currCluster;
                        }

                        auto ytPath = BuildYtPathForStatRequest(*cluster, pathInfo, columnList, *State_, ctx);
                        if (!ytPath) {
                            hasError = true;
                            return false;
                        }

                        pathStatReqs.push_back(
                            IYtGateway::TPathStatReq()
                                .Path(*ytPath)
                                .IsTemp(pathInfo.Table->IsTemp)
                                .IsAnonymous(pathInfo.Table->IsAnonymous)
                                .Epoch(pathInfo.Table->Epoch.GetOrElse(0))
                        );

                        ++idx;
                    }

                    if (pathStatReqs) {
                        auto pathStatOptions = IYtGateway::TPathStatOptions(State_->SessionId)
                            .Cluster(*cluster)
                            .Paths(pathStatReqs)
                            .Config(State_->Configuration->Snapshot());

                        auto tryResult = State_->Gateway->TryPathStat(IYtGateway::TPathStatOptions(pathStatOptions));
                        if (!tryResult.Success()) {
                            pathStatArgs.emplace_back(std::move(pathStatOptions), node.Get());
                        }
                    }
                }
            }
            return !hasError;
        });

        if (hasError) {
            return TStatus::Error;
        }

        if (!sectionRewrites.empty()) {
            auto status = RemapExpr(input, output, sectionRewrites, ctx, TOptimizeExprSettings(State_->Types));
            YQL_ENSURE(status.Level != TStatus::Ok);
            return status;
        }

        if (pathStatArgs.empty()) {
            return TStatus::Ok;
        }

        TVector<NThreading::TFuture<void>> futures;
        YQL_CLOG(INFO, ProviderYt) << "Starting " << pathStatArgs.size() << " requests for columnar stats";
        for (auto& arg : pathStatArgs) {
            IYtGateway::TPathStatOptions& options = arg.first;
            TExprNode* node = arg.second;

            auto future = State_->Gateway->PathStat(std::move(options));

            futures.push_back(future.Apply([pathStatusState = PathStatusState, node](const NThreading::TFuture<IYtGateway::TPathStatResult>& result) {
                pathStatusState->MarkReady(node, result.GetValueSync());
            }));
        }

        AsyncFuture = WaitExceptionOrAll(futures);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        Y_UNUSED(input);
        return AsyncFuture;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;

        TNodeMap<IYtGateway::TPathStatResult> results = PathStatusState->PullPathStatResults();
        YQL_ENSURE(!results.empty());

        for (auto& item : results) {
            auto& node = item.first;
            auto& result = item.second;
            if (!result.Success()) {
                TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                    return MakeIntrusive<TIssue>(
                        ctx.GetPosition(node->Pos()),
                        TStringBuilder() << "Execution of node: " << node->Content()
                    );
                });
                result.ReportIssues(ctx.IssueManager);
                return TStatus::Error;
            }
        }

        YQL_CLOG(INFO, ProviderYt) << "Applied " << results.size() << " results of columnar stats";
        return TStatus::Repeat;
    }

    TYtState::TPtr State_;
    TPathStatusState::TPtr PathStatusState;
    NThreading::TFuture<void> AsyncFuture;
};

} // namespace

THolder<IGraphTransformer> CreateYtLoadColumnarStatsTransformer(TYtState::TPtr state) {
    return THolder(new TYtLoadColumnarStatsTransformer(state));
}

} // namespace NYql
