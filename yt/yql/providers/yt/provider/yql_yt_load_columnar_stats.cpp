#include "yql_yt_provider_impl.h"
#include "yql_yt_gateway.h"
#include "yql_yt_helpers.h"

#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_optimize.h>

#include <library/cpp/threading/future/async.h>
#include <yql/essentials/utils/log/log.h>


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

    TNodeMap<TVector<IYtGateway::TPathStatResult>> PullPathStatResults() {
        TNodeMap<TVector<IYtGateway::TPathStatResult>> results;
        TGuard<TMutex> guard(Lock);
        results.swap(PathStatResults);
        return results;
    }

    void AddResult(TExprNode* node, const IYtGateway::TPathStatResult& result) {
        TGuard<TMutex> guard(Lock);
        PathStatResults[node].push_back(result);
    }

private:
    mutable TMutex Lock;
    TNodeMap<TVector<IYtGateway::TPathStatResult>> PathStatResults;
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
        TVector<std::pair<TVector<IYtGateway::TPathStatOptions>, TExprNode*>> pathStatArgs;
        bool hasError = false;
        TNodeOnNodeOwnedMap sectionRewrites;
        VisitExpr(input, [this, &pathStatArgs, &hasError, &sectionRewrites, &ctx](const TExprNode::TPtr& node) {
            const TMaybe<ui64> maxChunkCountExtendedStats = State_->Configuration->ExtendedStatsMaxChunkCount.Get();
            if (auto maybeSection = TMaybeNode<TYtSection>(node)) {
                TYtSection section = maybeSection.Cast();
                if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::StatColumns)) {
                    auto columnList = NYql::GetSettingAsColumnList(section.Settings().Ref(), EYtSettingType::StatColumns);

                    TMap<TString, TVector<IYtGateway::TPathStatReq>> pathStatReqsByCluster;
                    size_t idx = 0;
                    THashMap<TString, ui64> totalChunkCountByCluster;
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
                        const TString cluster = pathInfo.Table->Cluster;
                        YQL_ENSURE(cluster);
                        YQL_ENSURE(pathInfo.Table->Stat);
                        totalChunkCountByCluster[cluster] += pathInfo.Table->Stat->ChunkCount;

                        auto ytPath = BuildYtPathForStatRequest(pathInfo, columnList, *State_, ctx);
                        if (!ytPath) {
                            hasError = true;
                            return false;
                        }

                        pathStatReqsByCluster[cluster].push_back(
                            IYtGateway::TPathStatReq()
                                .Path(*ytPath)
                                .IsTemp(pathInfo.Table->IsTemp)
                                .IsAnonymous(pathInfo.Table->IsAnonymous)
                                .Epoch(pathInfo.Table->Epoch.GetOrElse(0))
                        );

                        ++idx;
                    }

                    TVector<IYtGateway::TPathStatOptions> pathStatOptions;
                    for (auto& [cluster, pathStatReqs] : pathStatReqsByCluster) {
                        auto itCount = totalChunkCountByCluster.find(cluster);
                        YQL_ENSURE(itCount != totalChunkCountByCluster.end());
                        const ui64 totalChunkCount = itCount->second;
                        bool requestExtendedStats = maxChunkCountExtendedStats &&
                            (*maxChunkCountExtendedStats == 0 || totalChunkCount <= *maxChunkCountExtendedStats);
                        YQL_ENSURE(!pathStatReqs.empty());
                        auto options = IYtGateway::TPathStatOptions(State_->SessionId)
                            .Cluster(cluster)
                            .Paths(pathStatReqs)
                            .Config(State_->Configuration->Snapshot())
                            .Extended(requestExtendedStats);
                        auto tryResult = State_->Gateway->TryPathStat(IYtGateway::TPathStatOptions(options));
                        if (!tryResult.Success()) {
                            pathStatOptions.push_back(std::move(options));
                        }
                    }

                    if (pathStatOptions) {
                        pathStatArgs.emplace_back(std::move(pathStatOptions), node.Get());
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
        size_t reqCount = 0;
        for (const auto& arg : pathStatArgs) {
            reqCount += arg.first.size();
        }
        YQL_CLOG(INFO, ProviderYt) << "Starting " << reqCount << " requests for columnar stats";
        for (auto& arg : pathStatArgs) {
            TVector<IYtGateway::TPathStatOptions>& options = arg.first;
            TExprNode* node = arg.second;
            for (auto& opt : options) {
                auto future = State_->Gateway->PathStat(std::move(opt));
                futures.push_back(future.Apply([pathStatusState = PathStatusState, node](const NThreading::TFuture<IYtGateway::TPathStatResult>& result) {
                    pathStatusState->AddResult(node, result.GetValueSync());
                }));
            }
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

        TNodeMap<TVector<IYtGateway::TPathStatResult>> results = PathStatusState->PullPathStatResults();
        YQL_ENSURE(!results.empty());

        size_t applied = 0;
        TStatus status = TStatus::Repeat;
        for (auto& item : results) {
            auto& node = item.first;
            auto& batch = item.second;
            TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                return MakeIntrusive<TIssue>(
                    ctx.GetPosition(node->Pos()),
                    TStringBuilder() << "Execution of node: " << node->Content()
                );
            });
            for (auto& result : batch) {
                if (!result.Success()) {
                    result.ReportIssues(ctx.IssueManager);
                    status = status.Combine(TStatus::Error);
                }
                ++applied;
            }
        }

        YQL_CLOG(INFO, ProviderYt) << "Applied " << applied << " results of columnar stats "
                                   << (status == TStatus::Error ? "with errors" : "successfully");
        return status;
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
