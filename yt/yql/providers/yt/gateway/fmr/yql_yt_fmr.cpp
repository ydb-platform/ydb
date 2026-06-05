#include "yql_yt_fmr.h"

#include <thread>

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/gateway/lib/exec_ctx.h>
#include <yt/yql/providers/yt/gateway/lib/yt_attrs.h>
#include <yt/yql/providers/yt/gateway/lib/map_builder.h>
#include <yt/yql/providers/yt/gateway/lib/reduce_builder.h>
#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/fmr/job_launcher/yql_yt_job_launcher.h>
#include <yt/yql/providers/yt/fmr/process/yql_yt_job_fmr.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_write_distributed_session.h>
#include <yt/yql/providers/yt/lib/lambda_builder/lambda_builder.h>
#include <yt/yql/providers/yt/lib/schema/schema.h>
#include <yt/yql/providers/yt/lib/url_mapper/yql_yt_url_mapper.h>
#include <yt/yql/providers/yt/lib/yt_file_download/yql_yt_file_download.h>
#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>
#include <yt/yql/providers/yt/provider/yql_yt_table_desc.h>
#include <yt/yql/providers/yt/provider/yql_yt_mkql_compiler.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_type_helpers.h>
#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yt/yql/providers/yt/lib/res_pull/res_or_pull.h>
#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/profile.h>

#include <util/generic/ptr.h>
#include <util/string/split.h>
#include <util/thread/pool.h>

using namespace NThreading;
using namespace NYql::NNodes;

namespace NYql::NFmr {

namespace {

static void UpdateStage(TOperationProgress::TStage& stage, const TString& stageName) {
    if (stage.first != stageName) {
        stage.first = stageName;
        stage.second = Now();
    }
}

TIssue ToIssue(const TFmrError& error, const TPosition& pos){
    auto issue = TIssue(pos, error.ErrorMessage);
    if (error.Reason == EFmrErrorReason::RestartQuery) {
        issue.SetCode(TIssuesIds::FMR_NEED_FALLBACK, TSeverityIds::S_ERROR);
    } else if (error.Reason == EFmrErrorReason::UdfTerminate) {
        issue.SetCode(TIssuesIds::FMR_UDF_TERMINATE, TSeverityIds::S_ERROR);
    } else {
        issue.SetCode(TIssuesIds::FMR_UNKNOWN_ERROR, TSeverityIds::S_ERROR);
    }
    return issue;
};

TVector<TIssue> GetIssuesFromFmrErrors(const std::vector<TFmrError>& fmrOperationErrors, const TPosition& pos) {
    TVector<TIssue> issues;
    for (const auto& error : fmrOperationErrors) {
        issues.emplace_back(ToIssue(error, pos));
    }
    return issues;
}

struct TFmrOperationResult: public NCommon::TOperationResult {
    std::vector<TFmrError> Errors = {};
    std::vector<TTableStats> TablesStats = {};
    TString PullData; // non-empty when this is a completed Pull operation
};

TFmrOperationResult MergeSeveralFmrOperationResults(const std::vector<TFmrOperationResult>& fmrOperationResults) {
    TFmrOperationResult operationResult;
    bool hasErrors = false;
    for (auto& fmrResult: fmrOperationResults) {
        if (fmrResult.Repeat()) {
            operationResult.SetRepeat(true);
        }
        if (!fmrResult.Issues().Empty()) {
            operationResult.AddIssues(fmrResult.Issues());
        }
        if (!fmrResult.Success()) {
            hasErrors = true;
        }
        for (auto& error: fmrResult.Errors) {
            operationResult.Errors.emplace_back(error);
            hasErrors = true;
            if (error.Reason == EFmrErrorReason::RestartQuery) {
                YQL_CLOG(ERROR, FastMapReduce) << " Fmr query finished with error message " << error.ErrorMessage << " - should restart whole query without fmr gateway";
                return TFmrOperationResult{.Errors = {error}};
            } else if (error.Reason == EFmrErrorReason::RestartOperation) {
                operationResult.SetRepeat(true);
            }
        }
    }
    if (!hasErrors) {
        operationResult.SetSuccess();
    }
    return operationResult;
}

void ProcessFmrOperationResult(const TFmrOperationResult& fmrOperationResult, NCommon::TOperationResult& result, const TPosition& pos) {
    if (fmrOperationResult.Repeat()) {
        result.SetRepeat(true);
    }

    std::vector<TFmrError> fmrErrors;
    for (const auto& fmrError: fmrOperationResult.Errors) {
        if (fmrError.Reason == EFmrErrorReason::RestartOperation) {
            result.SetRepeat(true);
        } else {
            fmrErrors.emplace_back(fmrError);
        }
    }

    result.AddIssues(GetIssuesFromFmrErrors(fmrErrors, pos));

    if (!fmrOperationResult.Issues().Empty()) {
        result.AddIssues(fmrOperationResult.Issues());
    }

    if (fmrErrors.empty() && fmrOperationResult.Success()) {
        result.SetSuccess();
    }
}

bool HasFmrErrorReason(const TFmrOperationResult& fmrOperationResult, EFmrErrorReason reason) {
    return AnyOf(fmrOperationResult.Errors, [reason](const TFmrError& error) {
        return error.Reason == reason;
    });
}

class TFmrYtGateway final: public TYtForwardingGatewayBase {
public:
    TFmrYtGateway(IYtGateway::TPtr&& slave, IFmrCoordinator::TPtr coordinator, TFmrServices::TPtr fmrServices, const TFmrYtGatewaySettings& settings)
        : TYtForwardingGatewayBase(std::move(slave)),
        Coordinator_(coordinator),
        RandomProvider_(settings.RandomProvider),
        TimeProvider_(settings.TimeProvider),
        TimeToSleepBetweenGetOperationRequests_(settings.TimeToSleepBetweenGetOperationRequests),
        CoordinatorPingInterval_(settings.CoordinatorPingInterval),
        MaxDirectPullBytes_(settings.MaxDirectPullBytes),
        MaxDirectPullRows_(settings.MaxDirectPullRows),
        FmrServices_(fmrServices),
        MkqlCompiler_(MakeIntrusive<NCommon::TMkqlCommonCallableCompiler>()),
        YtJobService_(fmrServices->YtJobService)
    {
        if (fmrServices->Config) {
            Clusters_ = MakeIntrusive<TConfigClusters>(*FmrServices_->Config);
            UrlMapper_ = std::make_shared<TYtUrlMapper>(*FmrServices_->Config);
        }

        RegisterYtMkqlCompilers(*MkqlCompiler_);

        auto getOperationStatusesFunc = [this] {
            while (!StopFmrGateway_) {
                struct TOperationToCheck {
                    TString OperationId;
                    TString SessionId;
                };

                struct TCompletedOperation {
                    TString OperationId;
                    TString SessionId;
                    TFmrOperationResult Result;
                    bool IsSortedUpload = false;
                    std::vector<TString> FragmentResultsYson;
                    bool IsPull = false;
                    TString PullData;
                    TJobCounters JobCounters;
                };

                struct TOperationToAbort {
                    TString OperationId;
                    TString SessionId;
                    TString WriteSessionId;
                    TString PingError;
                };

                // Phase 1: collect operations to check under lock
                std::vector<TOperationToCheck> operationsToCheck;
                std::vector<TOperationToAbort> operationsToAbort;
                with_lock(Mutex_) {
                    for (auto& [sessionId, sessionInfo]: Sessions_) {
                        auto& operationStates = sessionInfo->OperationStates;
                        for (const auto& [operationId, promise]: operationStates.OperationStatuses) {
                            if (!promise.IsReady()) {
                                operationsToCheck.push_back({operationId, sessionId});
                            }
                        }
                        for (const auto& [operationId, writeSessionId]: operationStates.SortedUploadOperations) {
                            auto it = DistributedUploadSessions_.find(writeSessionId);
                            if (it != DistributedUploadSessions_.end() && it->second->HasPingError()) {
                                TString pingError = it->second->GetPingError();
                                YQL_CLOG(ERROR, FastMapReduce) << "Distributed upload session " << writeSessionId
                                    << " has ping error for operation " << operationId << ": " << pingError;
                                operationsToAbort.push_back({operationId, sessionId, writeSessionId, pingError});
                            }
                        }
                    }
                }

                // Phase 2: query coordinator and process results without lock
                std::vector<TCompletedOperation> completedOperations;
                for (const auto& op : operationsToCheck) {
                    YQL_CLOG(TRACE, FastMapReduce) << "Sending get operation request to coordinator with operationId: " << op.OperationId;
                    TGetOperationResponse getOperationResult;
                    try {
                        getOperationResult = Coordinator_->GetOperation({op.OperationId}).GetValueSync();
                    } catch (...) {
                        YQL_CLOG(ERROR, FastMapReduce) << "Failed to get operation status for " << op.OperationId << ": " << CurrentExceptionMessage();
                        TCompletedOperation completed;
                        completed.OperationId = op.OperationId;
                        completed.SessionId = op.SessionId;
                        completed.Result.Errors.emplace_back(TFmrError{
                            .Component = EFmrComponent::Gateway,
                            .Reason = EFmrErrorReason::FallbackOperation,
                            .ErrorMessage = TStringBuilder() << "FMR Coordinator is unavailable for operation, fallback to native gateway"
                        });
                        completedOperations.push_back(std::move(completed));
                        continue;
                    }

                    auto status = getOperationResult.Status;
                    bool operationCompleted = status != EOperationStatus::Accepted && status != EOperationStatus::InProgress;
                    if (!operationCompleted) {
                        with_lock(Mutex_) {
                            InProgressCounters_[op.OperationId] = getOperationResult.JobCounters;
                        }
                        continue;
                    }

                    TCompletedOperation completed;
                    completed.OperationId = op.OperationId;
                    completed.SessionId = op.SessionId;
                    completed.Result.TablesStats = getOperationResult.OutputTablesStats;
                    completed.Result.Errors = getOperationResult.ErrorMessages;
                    completed.JobCounters = getOperationResult.JobCounters;
                    bool hasCompletedSuccessfully = (status == EOperationStatus::Completed);
                    if (hasCompletedSuccessfully) {
                        completed.Result.SetSuccess();
                    }

                    with_lock(Mutex_) {
                        if (Sessions_.contains(op.SessionId)) {
                            auto& session = Sessions_[op.SessionId];
                            completed.IsSortedUpload = session->OperationStates.SortedUploadOperations.contains(op.OperationId);
                            if (completed.IsSortedUpload && hasCompletedSuccessfully) {
                                completed.FragmentResultsYson = getOperationResult.OperationResultsYson;
                            }
                            completed.IsPull = session->OperationStates.PullOperations.contains(op.OperationId);
                            if (completed.IsPull && hasCompletedSuccessfully && !getOperationResult.OperationResultsYson.empty()) {
                                completed.PullData = getOperationResult.OperationResultsYson[0];
                            }
                        }
                    }

                    if (completed.IsSortedUpload && hasCompletedSuccessfully) {
                        YQL_CLOG(TRACE, FastMapReduce) << "Finalizing sorted upload operation " << op.OperationId;
                        FinalizeSortedUploadOperation(op.OperationId, op.SessionId, completed.FragmentResultsYson);
                    }

                    YQL_CLOG(TRACE, FastMapReduce) << "Sending delete operation request to coordinator with operationId: " << op.OperationId;
                    try {
                        Coordinator_->DeleteOperation({op.OperationId});
                    } catch (...) {
                        YQL_CLOG(ERROR, FastMapReduce) << "Failed to delete operation " << op.OperationId << ": " << CurrentExceptionMessage();
                    }

                    completedOperations.push_back(std::move(completed));
                }

                for (const auto& abortOp : operationsToAbort) {
                    YQL_CLOG(INFO, FastMapReduce) << "Aborting operation " << abortOp.OperationId << " due to distributed upload ping error";
                    try {
                        Coordinator_->DeleteOperation({abortOp.OperationId});
                    } catch (...) {
                        YQL_CLOG(ERROR, FastMapReduce) << "Failed to delete aborted operation " << abortOp.OperationId << ": " << CurrentExceptionMessage();
                    }
                }

                // Phase 3: update state and resolve promises under lock, then set values outside
                struct TProgressUpdate {
                    const TOperationProgressWriter Writer;
                    const TOperationProgress Progress;

                    TProgressUpdate(TOperationProgressWriter writer, TOperationProgress progress)
                        : Writer(std::move(writer))
                        , Progress(std::move(progress))
                    {}
                };
                std::vector<std::pair<TPromise<TFmrOperationResult>, TFmrOperationResult>> promisesToResolve;
                std::vector<TProgressUpdate> progressUpdates;
                with_lock(Mutex_) {
                    for (const auto& op : operationsToCheck) {
                        if (completedOperations.end() != std::find_if(completedOperations.begin(), completedOperations.end(), [&op](const auto& c) { return c.OperationId == op.OperationId; })) {
                            continue;
                        }
                        if (!Sessions_.contains(op.SessionId)) {
                            continue;
                        }
                        auto& session = Sessions_[op.SessionId];
                        auto& operationStates = session->OperationStates;
                        auto publicIdIt = operationStates.OperationPublicIds.find(op.OperationId);
                        if (publicIdIt == operationStates.OperationPublicIds.end() || !publicIdIt->second.Defined()) {
                            continue;
                        }
                        auto countersIt = InProgressCounters_.find(op.OperationId);
                        if (countersIt == InProgressCounters_.end()) {
                            continue;
                        }
                        auto progressIt = operationStates.LastProgress.find(*publicIdIt->second);
                        YQL_ENSURE(progressIt != operationStates.LastProgress.end());
                        auto& progress = progressIt->second;
                        progress.State = TOperationProgress::EState::InProgress;
                        UpdateStage(progress.Stage, "FMR Running");
                        TOperationProgress::TCounters progressCounters;
                        const auto& jobCounters = countersIt->second;
                        progressCounters.Total = jobCounters.Total;
                        progressCounters.Pending = jobCounters.Pending;
                        progressCounters.Running = jobCounters.Running;
                        progressCounters.Completed = jobCounters.Completed;
                        progressCounters.Failed = jobCounters.Failed;
                        progressCounters.Lost = jobCounters.Lost;
                        progress.Counters = progressCounters;
                        progressUpdates.emplace_back(session->ProgressWriter_, progress);
                    }

                    for (auto& completed : completedOperations) {
                        if (!Sessions_.contains(completed.SessionId)) {
                            continue;
                        }
                        auto& session = Sessions_[completed.SessionId];
                        auto& operationStates = session->OperationStates;
                        auto& operationStatuses = operationStates.OperationStatuses;
                        if (!operationStatuses.contains(completed.OperationId)) {
                            continue;
                        }
                        if (completed.IsSortedUpload) {
                            operationStates.SortedUploadOperations.erase(completed.OperationId);
                        }
                        if (completed.IsPull) {
                            operationStates.PullOperations.erase(completed.OperationId);
                            completed.Result.PullData = std::move(completed.PullData);
                        }
                        auto publicIdIt = operationStates.OperationPublicIds.find(completed.OperationId);
                        if (publicIdIt != operationStates.OperationPublicIds.end() && publicIdIt->second.Defined()) {
                            auto state = completed.Result.Success() ? TOperationProgress::EState::Finished : TOperationProgress::EState::Failed;
                            auto stageName = completed.Result.Success() ? TString("FMR Complete") : TString("FMR Failed");
                            auto progressIt = operationStates.LastProgress.find(*publicIdIt->second);
                            YQL_ENSURE(progressIt != operationStates.LastProgress.end());
                            auto& progress = progressIt->second;
                            progress.State = state;
                            UpdateStage(progress.Stage, stageName);
                            TOperationProgress::TCounters progressCounters;
                            progressCounters.Total = completed.JobCounters.Total;
                            progressCounters.Pending = completed.JobCounters.Pending;
                            progressCounters.Running = completed.JobCounters.Running;
                            progressCounters.Completed = completed.JobCounters.Completed;
                            progressCounters.Failed = completed.JobCounters.Failed;
                            progressCounters.Lost = completed.JobCounters.Lost;
                            progress.Counters = progressCounters;
                            progressUpdates.emplace_back(session->ProgressWriter_, progress);
                        }
                        InProgressCounters_.erase(completed.OperationId);
                        promisesToResolve.emplace_back(operationStatuses[completed.OperationId], std::move(completed.Result));
                    }

                    for (const auto& abortOp : operationsToAbort) {
                        if (!Sessions_.contains(abortOp.SessionId)) {
                            continue;
                        }
                        auto& session = Sessions_[abortOp.SessionId];
                        auto& operationStates = session->OperationStates;
                        auto& operationStatuses = operationStates.OperationStatuses;
                        if (operationStatuses.contains(abortOp.OperationId)) {
                            TFmrOperationResult fmrOperationResult{};
                            fmrOperationResult.Errors.emplace_back(TFmrError{
                                .Component = EFmrComponent::Gateway,
                                .Reason = EFmrErrorReason::FallbackOperation,
                                .ErrorMessage = TStringBuilder() << "Distributed upload session ping failed: " << abortOp.PingError
                            });
                            auto publicIdIt = operationStates.OperationPublicIds.find(abortOp.OperationId);
                            if (publicIdIt != operationStates.OperationPublicIds.end() && publicIdIt->second.Defined()) {
                                auto progressIt = operationStates.LastProgress.find(*publicIdIt->second);
                                YQL_ENSURE(progressIt != operationStates.LastProgress.end());
                                auto& progress = progressIt->second;
                                progress.State = TOperationProgress::EState::Aborted;
                                UpdateStage(progress.Stage, "FMR Aborted");
                                progressUpdates.emplace_back(session->ProgressWriter_, progress);
                            }
                            InProgressCounters_.erase(abortOp.OperationId);
                            promisesToResolve.emplace_back(operationStatuses[abortOp.OperationId], std::move(fmrOperationResult));
                            DistributedUploadSessions_.erase(abortOp.WriteSessionId);
                        }
                        operationStates.SortedUploadOperations.erase(abortOp.OperationId);
                    }

                    for (auto& [sessionId, sessionInfo]: Sessions_) {
                        auto& operationStates = sessionInfo->OperationStates;
                        std::erase_if(operationStates.OperationStatuses, [] (const auto& item) {
                            return item.second.IsReady();
                        });
                        std::erase_if(operationStates.OperationPublicIds, [&operationStates] (const auto& item) {
                            return !operationStates.OperationStatuses.contains(item.first);
                        });
                    }
                }

                for (auto& update : progressUpdates) {
                    update.Writer(update.Progress);
                }
                for (auto& [promise, result] : promisesToResolve) {
                    promise.SetValue(std::move(result));
                }

                Sleep(TimeToSleepBetweenGetOperationRequests_);
            }
        };
        GetOperationStatusesThread_ = std::thread(getOperationStatusesFunc);

        auto pingGatewaySessionFunc = [this] {
            YQL_LOG_CTX_ROOT_SCOPE("PingGatewaySession");
            while (!StopFmrGateway_) {
                try {
                    std::unordered_map<TString, NThreading::TFuture<TPingSessionResponse>> pingFutures;
                    with_lock(Mutex_) {
                        for (const auto& [sessionId, sessionInfo]: Sessions_) {
                            YQL_CLOG(TRACE, FastMapReduce) << "Pinging gateway session " << sessionId;
                            try {
                                auto pingFuture = Coordinator_->PingSession(TPingSessionRequest{.SessionId = sessionId});
                                pingFutures.emplace(sessionId, pingFuture);
                            } catch (...) {
                                YQL_CLOG(ERROR, FastMapReduce) << "Exception while pinging gateway session " << sessionId
                                    << ": " << CurrentExceptionMessage();
                            }
                        }
                    }
                    if (!pingFutures.empty()) {
                        std::vector<NThreading::TFuture<TPingSessionResponse>> futures;
                        futures.reserve(pingFutures.size());
                        for (const auto& [sessionId, pingFuture] : pingFutures) {
                            futures.push_back(pingFuture);
                        }
                        try {
                            NThreading::WaitAll(futures).Wait();
                        } catch (...) {
                            YQL_CLOG(ERROR, FastMapReduce) << "Exception in WaitAll for ping futures: " << CurrentExceptionMessage();
                        }

                        for (auto& [sessionId, pingFuture] : pingFutures) {
                            try {
                                auto pingResult = pingFuture.GetValue();
                                if (!pingResult.Success) {
                                    YQL_CLOG(WARN, FastMapReduce) << "Failed to ping gateway session " << sessionId;
                                }
                            } catch (...) {
                                YQL_CLOG(ERROR, FastMapReduce) << "Exception while getting ping result for session " << sessionId
                                    << ": " << CurrentExceptionMessage();
                            }
                        }
                    }
                } catch (...) {
                    YQL_CLOG(ERROR, FastMapReduce) << "Unexpected exception in ping thread: " << CurrentExceptionMessage();
                }
                Sleep(CoordinatorPingInterval_);
            }
        };
        PingSessionThread_ = std::thread(pingGatewaySessionFunc);
    }

    ~TFmrYtGateway() {
        StopFmrGateway_ = true;
        GetOperationStatusesThread_.join();
        PingSessionThread_.join();
    }

    // Walks an expression tree and invokes `processTable` for every table referenced by a
    // YtTableContent (either via YtReadTable paths or via YtOutput) and by Right!(YtReadTable).
    // The traversal does not descend into already-handled YtTableContent / TCoRight nodes.
    template <typename TProcessTable>
    static void ScanYtTableContentTables(const TExprNode::TPtr& root, TProcessTable processTable) {
        VisitExpr(root, [&](const TExprNode::TPtr& exprNode) {
            if (auto maybeContent = TMaybeNode<TYtTableContent>(exprNode)) {
                auto content = maybeContent.Cast();
                if (auto maybeRead = content.Input().Maybe<TYtReadTable>()) {
                    for (auto section : maybeRead.Cast().Input()) {
                        for (auto path : section.Paths()) {
                            processTable(TYtTableBaseInfo::Parse(path.Table()));
                        }
                    }
                } else if (auto maybeOutput = content.Input().Maybe<TYtOutput>()) {
                    processTable(TYtTableBaseInfo::Parse(maybeOutput.Cast()));
                }
                return false;
            }
            if (auto maybeRead = TMaybeNode<TCoRight>(exprNode).Input().Maybe<TYtReadTable>()) {
                for (auto section : maybeRead.Cast().Input()) {
                    for (auto path : section.Paths()) {
                        processTable(TYtTableBaseInfo::Parse(path.Table()));
                    }
                }
                return false;
            }
            return true;
        });
    }

    // Dispatches by Yt operation kind and scans the relevant lambda bodies / input
    // expressions for YtTableContent references. Lambdaless ops are no-ops because their
    // input tables are already enumerated by the caller (execCtx->InputTables_).
    template <typename TProcessTable>
    static void ScanOperationForYtTableContent(const TExprNode::TPtr& node, TProcessTable processTable) {
        TYtOpBase opBase(node);
        if (auto map = opBase.Maybe<TYtMap>()) {
            ScanYtTableContentTables(map.Cast().Mapper().Body().Ptr(), processTable);
        } else if (auto reduce = opBase.Maybe<TYtReduce>()) {
            ScanYtTableContentTables(reduce.Cast().Reducer().Body().Ptr(), processTable);
        } else if (auto mapReduce = opBase.Maybe<TYtMapReduce>()) {
            if (auto mapper = mapReduce.Cast().Mapper().Maybe<TCoLambda>()) {
                ScanYtTableContentTables(mapper.Cast().Body().Ptr(), processTable);
            }
            ScanYtTableContentTables(mapReduce.Cast().Reducer().Body().Ptr(), processTable);
        } else if (auto fill = opBase.Maybe<TYtFill>()) {
            ScanYtTableContentTables(fill.Cast().Content().Body().Ptr(), processTable);
        } else if (auto dqWrite = opBase.Maybe<TYtDqProcessWrite>()) {
            ScanYtTableContentTables(dqWrite.Cast().Input().Ptr(), processTable);
        } else if (opBase.Maybe<TYtSort>() || opBase.Maybe<TYtMerge>()
                || opBase.Maybe<TYtCopy>() || opBase.Maybe<TYtEquiJoin>()
                || opBase.Maybe<TYtTouch>() || opBase.Maybe<TYtDropTable>()
                || opBase.Maybe<TYtDropView>() || opBase.Maybe<TYtCreateView>()
                || opBase.Maybe<TYtStatOut>()) {
            // Lambdaless ops: nothing to scan.
        } else {
            YQL_CLOG(ERROR, FastMapReduce) << "ScanOperationForYtTableContent: unknown operation kind '"
                << node->Content() << "', skipping YtTableContent scan";
        }
    }

    // Builds a callback that, given a TYtTableBaseInfo parsed from the AST, registers the
    // corresponding FMR-only table for upload to YT. Shared by Run and ResOrPull fallback paths.
    template <typename TOptions>
    auto MakeAstFmrTableProcessor(
        const TOptions& options,
        const TString& sessionId,
        const TString& defaultCluster,
        const TString& tmpFolder,
        std::unordered_map<TString, TVector<TOutputInfo>>& outputTablesByCluster,
        THashSet<TString>& seen,
        TStringBuf logPrefix)
    {
        return [this, &options, sessionId, defaultCluster, tmpFolder, &outputTablesByCluster, &seen, logPrefix]
               (TYtTableBaseInfo::TPtr tableInfo) {
            if (tableInfo->Cluster.empty()) {
                tableInfo->Cluster = defaultCluster;
            }
            TString tablePath = GetTransformedPath(sessionId, tableInfo->Name, tmpFolder);
            TString key = tableInfo->Cluster + "." + tablePath;
            if (!seen.insert(key).second) {
                return;
            }
            TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(tableInfo->Cluster, tablePath), sessionId);
            if (GetTablePresenceStatus(fmrTableId, sessionId) != ETablePresenceStatus::OnlyInFmr) {
                return;
            }
            TOutputInfo outputTableInfo;
            outputTableInfo.Path = tablePath;
            auto savedOutputSpec = GetTableOutputSpec(fmrTableId, sessionId);
            if (!savedOutputSpec.IsUndefined()) {
                outputTableInfo.Spec = savedOutputSpec;
            } else if (tableInfo->RowSpec) {
                outputTableInfo.Spec = FillAttrSpecNode(*tableInfo->RowSpec, options, tableInfo->Cluster);
            }
            outputTableInfo.AttrSpec = NYT::TNode::CreateMap();
            TString columnGroupSpec = GetColumnGroupSpec(fmrTableId, sessionId);
            if (!columnGroupSpec.empty()) {
                outputTableInfo.ColumnGroups = NYT::NodeFromYsonString(columnGroupSpec);
            }
            YQL_CLOG(INFO, FastMapReduce) << logPrefix << ": will upload FMR-only table " << fmrTableId << " to YT";
            outputTablesByCluster[tableInfo->Cluster].emplace_back(outputTableInfo);
        };
    }

    TFuture<TRunResult> UploadFmrInputsAndForwardToUnderlyingGateway(
        const TExecContextSimple<TRunOptions>::TPtr& execCtx,
        const TExprNode::TPtr& node,
        TExprContext& ctx,
        TRunOptions&& options,
        const TPosition& nodePos)
    {
        TString sessionId = options.SessionId();
        TString cluster = execCtx->Cluster_;
        auto config = options.Config();
        TString tmpFolder = GetTablesTmpFolder(*config, cluster, Sessions_[sessionId]->UseSecureTmp_, Sessions_[sessionId]->OperationOptions_);
        std::unordered_map<TString, TVector<TOutputInfo>> outputTablesByCluster;
        THashSet<TString> seen;

        YQL_CLOG(WARN, FastMapReduce) << "FMR fallback to native gateway: node=" << node->Content()
            << " inputTables=" << execCtx->InputTables_.size()
            << " outTables=" << execCtx->OutTables_.size();
        for (size_t i = 0; i < execCtx->InputTables_.size(); ++i) {
            YQL_CLOG(INFO, FastMapReduce) << "FMR fallback input[" << i << "]: " << execCtx->InputTables_[i].Cluster << "." << execCtx->InputTables_[i].Name;
        }
        for (size_t i = 0; i < execCtx->OutTables_.size(); ++i) {
            YQL_CLOG(INFO, FastMapReduce) << "FMR fallback output[" << i << "]: " << execCtx->Cluster_ << "." << execCtx->OutTables_[i].Path;
        }

        auto addFmrTable = [&](const TString& tableCluster, const TString& tablePath, const NYT::TNode& spec, const TString& columnGroupSpec) {
            TString key = tableCluster + "." + tablePath;
            if (seen.contains(key)) {
                return;
            }
            seen.insert(key);
            TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(tableCluster, tablePath), sessionId);
            auto status = GetTablePresenceStatus(fmrTableId, sessionId);
            if (status == ETablePresenceStatus::OnlyInFmr) {
                TOutputInfo outputTableInfo;
                outputTableInfo.Path = tablePath;
                auto savedOutputSpec = GetTableOutputSpec(fmrTableId, sessionId);
                outputTableInfo.Spec = savedOutputSpec.IsUndefined() ? spec : savedOutputSpec;
                outputTableInfo.AttrSpec = NYT::TNode::CreateMap();
                if (!columnGroupSpec.empty()) {
                    outputTableInfo.ColumnGroups = NYT::NodeFromYsonString(columnGroupSpec);
                }
                YQL_CLOG(INFO, FastMapReduce) << "UploadFmrInputs: will upload FMR-only table " << fmrTableId << " to YT";
                outputTablesByCluster[tableCluster].emplace_back(outputTableInfo);
            }
        };

        for (auto& inputInfo : execCtx->InputTables_) {
            TFmrTableId inputFmrTableId(inputInfo.Cluster, inputInfo.Name);
            TFmrTableId fmrTableId = GetAliasOrFmrId(inputFmrTableId, sessionId);
            TString columnGroupSpec = GetColumnGroupSpec(fmrTableId, sessionId);
            addFmrTable(inputInfo.Cluster, inputInfo.Name, inputInfo.Spec, columnGroupSpec);
        }

        auto runOptionsCopy = TRunOptions(options);
        ScanOperationForYtTableContent(node,
            MakeAstFmrTableProcessor(runOptionsCopy, sessionId, cluster, tmpFolder,
                outputTablesByCluster, seen, "UploadFmrInputs"));

        if (!outputTablesByCluster.empty()) {
            return UploadSeveralFmrTablesToYt<TRunResult, TRunOptions>(outputTablesByCluster, TRunOptions(options), nodePos)
                .Apply([this, node, &ctx, options = std::move(options)] (const auto& f) mutable {
                    auto uploadResult = f.GetValue();
                    if (!uploadResult.Success()) {
                        return MakeFuture(std::move(uploadResult));
                    }
                    return Slave_->Run(node, ctx, std::move(options));
                });
        }
        return Slave_->Run(node, ctx, std::move(options));
    }

    TFuture<TRunResult> Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        auto nodePos = ctx.GetPosition(node->Pos());
        TYtOpBase opBase(node);
        auto cluster = TString{opBase.DataSink().Cluster().Value()};
        TString sessionId = options.SessionId();

        auto execCtx = MakeExecCtx(TRunOptions(options), cluster, sessionId);

        if (auto transientOp = opBase.Maybe<TYtTransientOpBase>()) {
            THashSet<TString> extraSysColumns;
            if (NYql::HasSetting(transientOp.Settings().Ref(), EYtSettingType::KeySwitch)
                && !transientOp.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>().IsValid()) {
                extraSysColumns.insert("keyswitch");
            }

            execCtx->SetInput(transientOp.Cast().Input(), opBase.Maybe<TYtWithUserJobsOpBase>().IsValid(), extraSysColumns);
        }
        if (auto outputOp = opBase.Maybe<TYtOutputOpBase>()) {
            execCtx->SetOutput(outputOp.Cast().Output());
        }

        // FMR worker downloads table content as YsonBinary, but YtBlockTableContent expects
        // Arrow-formatted blocks. The block-input optimizer decision is made earlier and
        // can't be reversed here, so when the setting is enabled delegate the whole
        // operation to the slave gateway.
        if (options.Config()->JobBlockTableContent.Get(cluster).GetOrElse(false)) {
            YQL_CLOG(WARN, FastMapReduce) << "FMR fallback to YT (Run): node=" << node->Content()
                << " reason: JobBlockTableContent is enabled and is not supported by FMR worker;"
                << " consider pragma yt.JobBlockTableContent=\"false\"";
            return UploadFmrInputsAndForwardToUnderlyingGateway(execCtx, node, ctx, std::move(options), nodePos);
        }

        TFuture<TFmrOperationResult> future;
        if (auto op = opBase.Maybe<TYtMerge>()) {
            future = DoMerge(execCtx);
        } else if (auto op = opBase.Maybe<TYtMap>()) {
            future = DoMap(op.Cast(), execCtx, ctx);
        } else if (auto op = opBase.Maybe<TYtSort>()) {
            future = DoSort(execCtx);
        } else if (auto op = opBase.Maybe<TYtReduce>()) {
            future = DoReduce(op.Cast(), execCtx, ctx);
        } else if (auto op = opBase.Maybe<TYtFill>()) {
            future = DoFill(op.Cast(), execCtx, ctx);
        } else {
            // We don't support this operation
            return UploadFmrInputsAndForwardToUnderlyingGateway(execCtx, node, ctx, std::move(options), nodePos);
        }

        return future.Apply([this, pos = nodePos, options = std::move(options), execCtx, node, &ctx] (const TFuture<TFmrOperationResult>& f) mutable {
            try {
                auto fmrOperationResult = f.GetValue(); // rethrow error if any
                if (HasFmrErrorReason(fmrOperationResult, EFmrErrorReason::FallbackOperation)) {
                    for (const auto& error : fmrOperationResult.Errors) {
                        if (error.Reason == EFmrErrorReason::FallbackOperation) {
                            YQL_CLOG(WARN, FastMapReduce) << "FMR fallback to YT (Run): node=" << node->Content() << " reason: " << error.ErrorMessage;
                        }
                    }
                    return UploadFmrInputsAndForwardToUnderlyingGateway(execCtx, node, ctx, std::move(options), pos);
                }

                TRunResult result;
                ProcessFmrOperationResult(fmrOperationResult, result, pos);

                if (!fmrOperationResult.Success()) {
                    YQL_CLOG(ERROR, FastMapReduce) << "FMR Run failed: node=" << node->Content()
                        << " errors=" << fmrOperationResult.Errors.size();
                    for (const auto& error : fmrOperationResult.Errors) {
                        YQL_CLOG(ERROR, FastMapReduce) << "  error: component=" << static_cast<int>(error.Component)
                            << " reason=" << static_cast<int>(error.Reason) << " msg=" << error.ErrorMessage;
                    }
                }

                YQL_CLOG(INFO, FastMapReduce) << "FMR Run callback: fmrOperationResult.Success()=" << fmrOperationResult.Success()
                    << ", result.Success()=" << result.Success()
                    << ", result.Repeat()=" << result.Repeat()
                    << ", TablesStats.size()=" << fmrOperationResult.TablesStats.size()
                    << ", OutTables_.size()=" << execCtx->OutTables_.size()
                    << ", node=" << node->Content();

                if (fmrOperationResult.Success()) {
                    auto outputTables = execCtx->OutTables_;
                    YQL_ENSURE(fmrOperationResult.TablesStats.size() == outputTables.size());
                    for (size_t i = 0; i < outputTables.size(); ++i) {
                        auto outputTable = outputTables[i];

                        TFmrTableId fmrOutputTableId(execCtx->Cluster_, outputTable.Path);
                        YQL_CLOG(INFO, FastMapReduce) << "Run: registering FMR output table " << fmrOutputTableId << " as OnlyInFmr, node=" << node->Content();
                        SetTablePresenceStatus(fmrOutputTableId, execCtx->GetSessionId(), ETablePresenceStatus::OnlyInFmr);

                        auto tableStats = fmrOperationResult.TablesStats[i];
                        TYtTableStatInfo stats;
                        stats.Id = "fmr_" + fmrOutputTableId.Id;
                        stats.RecordsCount = tableStats.Rows;
                        stats.DataSize = tableStats.DataWeight;
                        stats.ChunkCount = tableStats.Chunks;
                        stats.ModifyTime = TInstant::Now().Seconds();
                        result.OutTableStats.emplace_back(outputTable.Name, MakeIntrusive<TYtTableStatInfo>(stats));
                        SetFmrTableStats(fmrOutputTableId, stats, execCtx->GetSessionId());

                        TYtTableMetaInfo meta;
                        meta.DoesExist = true;

                        SetFmrTableMeta(fmrOutputTableId, meta, execCtx->GetSessionId());

                        YQL_CLOG(INFO, FastMapReduce) << "Fmr output table info: RecordsCount = " << result.OutTableStats.back().second->RecordsCount << " DataSize = " << result.OutTableStats.back().second->DataSize << " ChunkCount = " << result.OutTableStats.back().second->ChunkCount;
                    }
                }
                YQL_CLOG(INFO, FastMapReduce) << "FMR Run returning TRunResult: Success()=" << result.Success()
                    << ", OutTableStats.size()=" << result.OutTableStats.size();
                return MakeFuture<TRunResult>(std::move(result));
            } catch (...) {
                return MakeFuture(ResultFromCurrentException<TRunResult>(pos));
            }
        });
    }

    template<std::derived_from<NCommon::TOperationResult> TOperationResult, std::derived_from<TCommonOptions> TOptions>
    TFuture<TOperationResult> UploadSeveralFmrTablesToYt(
        const std::unordered_map<TString, TVector<TOutputInfo>>& outputFmrTabesByCluster,
        TOptions&& options,
        const TPosition& pos
    ) {
        YQL_ENSURE(!outputFmrTabesByCluster.empty());
        TString sessionId = options.SessionId();
        std::vector<typename TExecContextSimple<TOptions>::TPtr> outputExecCtxs;
        TYtSettings::TConstPtr config = options.Config();

        for (auto& [outputCluster, outputTables]: outputFmrTabesByCluster) {
            auto outputExecCtx = MakeExecCtx(TOptions(options), outputCluster, sessionId);
            outputExecCtx->OutTables_ = outputTables;
            outputExecCtxs.emplace_back(outputExecCtx);
        }
        return DumpFmrTablesToYt(outputExecCtxs).Apply([outputFmrTabesByCluster = std::move(outputFmrTabesByCluster), pos = std::move(pos), sessionId, config] (const auto& f) mutable {
            try {
                TFmrOperationResult dumpFmrTablesOpResult = f.GetValue();
                TOperationResult result;
                ProcessFmrOperationResult(dumpFmrTablesOpResult, result, pos);

                if constexpr (std::is_same_v<TOperationResult, TRunResult>) {
                    for (auto& [outputCluster, outputTables]: outputFmrTabesByCluster) {
                        for (auto& outputTable: outputTables) {
                            TYtTableStatInfo stats;
                            stats.Id = outputTable.Name;
                            result.OutTableStats.emplace_back(outputTable.Name, MakeIntrusive<TYtTableStatInfo>(stats));
                        }
                    }
                }
                return MakeFuture<TOperationResult>(std::move(result));
            } catch (...) {
                return MakeFuture(ResultFromCurrentException<TOperationResult>(pos));
            }
        });
    }

    TString GetTableLabel(const TExprNode::TPtr& node) {
        auto publish = TYtPublish(node);
        auto outTable = publish.Publish().Cast<TYtTable>();
        return TString{TYtTableInfo::GetTableLabel(outTable)};
    }

    TString GetTransformedPath(const TString& sessionId, const TString& table, const TString& tmpFolder) const {
        if (!FmrServices_->NeedToTransformTmpTablePaths) {
            return table;
        }
        YQL_ENSURE(Sessions_.contains(sessionId));
        auto& session = Sessions_.at(sessionId);
        return NYql::TransformPath(tmpFolder, table, true, session->UserName_);
    }

    NYT::TRichYPath GetWriteTableWithTransformedPath(const TString& sessionId, const TString& cluster, const TString& transformedTablePath) const {
        TString realTableName = transformedTablePath;
        realTableName = NYT::AddPathPrefix(realTableName, NYT::TConfig::Get()->Prefix);
        auto richYPath = NYT::TRichYPath(realTableName).Cluster(cluster);
        TYtSettings::TConstPtr config = nullptr;
        auto clusterConnection = GetTableClusterConnection(cluster, sessionId, config);
        richYPath.TransactionId(GetGuid(clusterConnection.TransactionId));
        YQL_CLOG(DEBUG, ProviderYt) << "Write table path: " << SerializeRichPath(richYPath);
        return richYPath;
    }

    NYT::TRichYPath GetWriteTable(const TString& sessionId, const TString& cluster, const TString& table, const TString& tmpFolder) const final {
        TString transformedTablePath = GetTransformedPath(sessionId, table, tmpFolder);
        return GetWriteTableWithTransformedPath(sessionId, cluster, transformedTablePath);
    }

    TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) final {
        TString sessionId = options.SessionId();
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        auto nodePos = ctx.GetPosition(node->Pos());
        auto publish = TYtPublish(node);

        auto cluster = publish.DataSink().Cluster().StringValue();
        auto config = options.Config();

        std::vector<TFuture<TFmrOperationResult>> uploadFmrTablesToYtFutures;
        auto outputPath = publish.Publish().Name().StringValue();

        bool isAnonymous = NYql::HasSetting(publish.Publish().Settings().Ref(), EYtSettingType::Anonymous);

        TString tmpFolder = GetTablesTmpFolder(*config, cluster, Sessions_[sessionId]->UseSecureTmp_, Sessions_[sessionId]->OperationOptions_);
        auto outputTableRichPath = GetWriteTable(sessionId, cluster, outputPath, tmpFolder);
        TFmrTableId fmrOutputTableId(outputTableRichPath);

        std::vector<TYtTableRef> inputTables;
        std::vector<TYqlRowSpecInfo> inputTablesRowSpec;
        for (auto out: publish.Input()) {
            auto [outTableNode, inputCluster] = GetOutTableWithCluster(out);
            auto outTable = outTableNode.Cast<TYtOutTable>();
            TString inputPath = GetTransformedPath(sessionId, ToString(outTable.Name().Value()), tmpFolder);
            inputTables.emplace_back(TYtTableRef(inputCluster, inputPath));
            inputTablesRowSpec.emplace_back(TYqlRowSpecInfo(outTable.RowSpec()));
        }

        if (isAnonymous) {
            TString anonTableLabel = GetTableLabel(node);
            YQL_CLOG(DEBUG, FastMapReduce) << "Output table in publish is an anonymous table with id " << fmrOutputTableId << " and label " << anonTableLabel;

            if (inputTables.size() == 1) {
                TFmrTableId inputFmrId(inputTables[0].RichPath);
                auto tablePresenceStatus = GetTablePresenceStatus(inputFmrId, sessionId);
                if (tablePresenceStatus == ETablePresenceStatus::OnlyInFmr || tablePresenceStatus == ETablePresenceStatus::Both) {
                    YQL_CLOG(DEBUG, FastMapReduce) << "Table with label" << anonTableLabel << " is anonymous and has only one input in fmr, returning";
                    SetTablePresenceStatus(fmrOutputTableId, sessionId, ETablePresenceStatus::OnlyInFmr);
                    SetAnonymousTableFmrIdAlias(fmrOutputTableId, inputFmrId, sessionId);

                    TYtTableMetaInfo meta;
                    meta.DoesExist = true;
                    SetFmrTableMeta(fmrOutputTableId, meta, sessionId);

                    // Preserve stats for alias anonymous tables so GetTableInfo sees real row/chunk counters.
                    auto inputStats = GetFmrTableStats(inputFmrId, sessionId);
                    inputStats.Id = "fmr_" + fmrOutputTableId.Id;
                    inputStats.ModifyTime = TInstant::Now().Seconds();
                    SetFmrTableStats(fmrOutputTableId, inputStats, sessionId);

                    TFmrTableRef inputTable = GetFmrTableRef(inputFmrId, sessionId);
                    SetTableSortingSpec(fmrOutputTableId, inputTable.SortColumns, inputTable.SortOrder, sessionId);

                    auto deferredSpec = FillAttrSpecNode(inputTablesRowSpec[0], TPublishOptions(options), cluster);
                    MarkForDeferredUpload(inputFmrId, deferredSpec, sessionId);
                    MarkForDeferredUpload(fmrOutputTableId, deferredSpec, sessionId);

                    TPublishResult publishResult;
                    publishResult.SetSuccess();
                    return MakeFuture<TPublishResult>(publishResult);
                }
            }

            std::vector<TInputInfo> inputTablesInfo;
            for (auto& curTable: inputTables) {
                TInputInfo curInputInfo;
                curInputInfo.Cluster = curTable.GetCluster();
                curInputInfo.Name = curTable.GetPath();
                curInputInfo.Path = curTable.RichPath;
                curInputInfo.Temp = true;
                inputTablesInfo.emplace_back(curInputInfo);
            }
            TFmrTableRef outputTable = GetFmrTableRef(fmrOutputTableId, sessionId);

            bool allInputsSorted = true;
            for (auto& curTable: inputTables) {
                TFmrTableId curFmrId = GetAliasOrFmrId(TFmrTableId(curTable.RichPath), sessionId);
                TFmrTableRef curRef = GetFmrTableRef(curFmrId, sessionId);
                if (curRef.SortColumns.empty()) {
                    allInputsSorted = false;
                    break;
                }
            }

            TFuture<TFmrOperationResult> future;
            if (allInputsSorted) {
                TFmrTableId firstInputFmrId = GetAliasOrFmrId(TFmrTableId(inputTables[0].RichPath), sessionId);
                TFmrTableRef firstInputRef = GetFmrTableRef(firstInputFmrId, sessionId);
                outputTable.SortColumns = firstInputRef.SortColumns;
                outputTable.SortOrder = firstInputRef.SortOrder;
                SetTableSortingSpec(fmrOutputTableId, outputTable.SortColumns, outputTable.SortOrder, sessionId);
                future = ExecSortedMerge(inputTablesInfo, outputTable, cluster, sessionId, config);
            } else {
                SetTableSortingSpec(fmrOutputTableId, {}, {}, sessionId);
                future = ExecMerge(inputTablesInfo, outputTable, cluster, sessionId, config);
            }

            auto deferredSpec = FillAttrSpecNode(inputTablesRowSpec[0], TPublishOptions(options), cluster);
            return future.Apply([this, sessionId, fmrOutputTableId, pos = nodePos, deferredSpec = std::move(deferredSpec)] (const auto& f) {
                TFmrOperationResult anonTablesMergeResult = f.GetValue();
                TPublishResult publishResult;
                publishResult.AddIssues(GetIssuesFromFmrErrors(anonTablesMergeResult.Errors, pos));
                if (anonTablesMergeResult.Success()) {
                    publishResult.SetSuccess();
                    YQL_ENSURE(
                        anonTablesMergeResult.TablesStats.size() == 1,
                        "Expected exactly one output table stats entry for anonymous publish merge");

                    const auto& tableStats = anonTablesMergeResult.TablesStats[0];
                    TYtTableStatInfo stats;
                    stats.Id = "fmr_" + fmrOutputTableId.Id;
                    stats.RecordsCount = tableStats.Rows;
                    stats.DataSize = tableStats.DataWeight;
                    stats.ChunkCount = tableStats.Chunks;
                    stats.ModifyTime = TInstant::Now().Seconds();
                    SetFmrTableStats(fmrOutputTableId, stats, sessionId);
                }

                TYtTableMetaInfo meta;
                meta.DoesExist = true;
                SetFmrTableMeta(fmrOutputTableId, meta, sessionId);

                SetTablePresenceStatus(fmrOutputTableId, sessionId, ETablePresenceStatus::OnlyInFmr);
                MarkForDeferredUpload(fmrOutputTableId, deferredSpec, sessionId);
                return MakeFuture<TPublishResult>(publishResult);
            });
        }

        // Table is not anonymous, so first we need to upload all tables which are only in fmr to yt, then retry with underlying gateway,

        std::unordered_map<TString, TVector<TOutputInfo>> outputTablesByCluster;
        for (ui64 i = 0; i < inputTables.size(); ++i) {
            TOutputInfo outputTableInfo;
            auto& table = inputTables[i];
            TString outputCluster = table.GetCluster(), outputPath = table.GetPath();
            TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(outputCluster, outputPath), sessionId);
            auto status = GetTablePresenceStatus(fmrTableId, sessionId);
            if (status != ETablePresenceStatus::OnlyInFmr) {
                continue;
            }
            outputTableInfo.Path = outputPath;
            outputTableInfo.Spec = FillAttrSpecNode(inputTablesRowSpec[i], TPublishOptions(options), outputCluster);
            outputTablesByCluster[outputCluster].emplace_back(outputTableInfo);
        }

        if (!outputTablesByCluster.empty()) {
            return UploadSeveralFmrTablesToYt<TPublishResult, TPublishOptions>(outputTablesByCluster, std::move(options), nodePos);
        }
        return Slave_->Publish(node, ctx, std::move(options));
    }

    TFuture<TDropTrackablesResult> DropTrackables(TDropTrackablesOptions&& options) override {
        TMaybe<TFuture<TDropTablesResponse>> fmrFuture;
        TMaybe<TFuture<TDropTrackablesResult>> ytFuture;
        TVector<TFuture<void>> allFutures;

        with_lock(Mutex_) {
            TString sessionId = options.SessionId();
            std::vector<TString> fmrTableIds;
            TVector<IYtGateway::TDropTrackablesOptions::TClusterAndPath> ytPaths;

            for (const auto& path : options.Pathes()) {
                TFmrTableId tableId(path.Cluster, path.Path);

                auto tmpFolder = GetTablesTmpFolder(*options.Config(), path.Cluster, Sessions_[sessionId]->UseSecureTmp_, Sessions_[sessionId]->OperationOptions_);
                auto transformedTableId = GetTransformedPath(sessionId, path.Path, tmpFolder);
                auto status = GetTablePresenceStatus(transformedTableId, sessionId);

                if (status == ETablePresenceStatus::OnlyInFmr || status == ETablePresenceStatus::Both) {
                    fmrTableIds.push_back(tableId.Id);
                }

                if (status == ETablePresenceStatus::OnlyInYt || status == ETablePresenceStatus::Both) {
                    ytPaths.push_back(path);
                }
            }

            if (!fmrTableIds.empty()) {
                TDropTablesRequest fmrRequest{
                    .TableIds = fmrTableIds,
                    .SessionId = sessionId
                };
                fmrFuture = Coordinator_->DropTables(fmrRequest);
            }

            if (!ytPaths.empty()) {
                options.Pathes() = std::move(ytPaths);
                ytFuture = Slave_->DropTrackables(std::move(options));
            }

            RemoveFmrTablesWithDependents(fmrTableIds, sessionId);

            if (fmrFuture) {
                allFutures.push_back(fmrFuture->IgnoreResult());
            }
            if (ytFuture) {
                allFutures.push_back(ytFuture->IgnoreResult());
            }
        }

        return WaitExceptionOrAll(allFutures).Apply([fmrFuture, ytFuture](const TFuture<void>&) mutable {
            TDropTrackablesResult finalResult;
            bool fmrSuccess = true;

            if (fmrFuture) {
                try {
                    fmrFuture->GetValue();
                } catch (...) {
                    fmrSuccess = false;
                    FillResultFromCurrentException(finalResult);
                }
            }

            bool ytSuccess = true;
            if (ytFuture) {
                auto ytResult = ytFuture->GetValue();
                if (!ytResult.Success()) {
                    ytSuccess = false;
                    if (!ytResult.Issues().Empty()) {
                        YQL_CLOG(ERROR, FastMapReduce) << "YT Slave DropTrackables failed: " << ytResult.Issues().ToString();
                    }
                    finalResult.AddIssues(ytResult.Issues());
                }
            }

            if (fmrSuccess && ytSuccess) {
                finalResult.SetSuccess();
            }

            return MakeFuture<TDropTrackablesResult>(finalResult);
        });
    }

    TFuture<TTableInfoResult> GetTableInfo(TGetTableInfoOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        TVector<TTableReq> ytBoundTables;
        TVector<IYtGateway::TTableInfoResult::TTableData> fmrTablesInfo;
        std::unordered_set<ui64> fmrTableIndexes; // needed to keep ordering of results the same as in getTableInfoOptions
        std::unordered_map<TString, TVector<TOutputInfo>> deferredOutputsByCluster;
        std::vector<TFmrTableId> deferredTableFmrIds;

        if (options.Epoch() == 0) {
            return Slave_->GetTableInfo(std::move(options));
        }

        TString sessionId = options.SessionId();
        ui64 tableIndex = 0;
        for (const auto& table: options.Tables()) {
            TFmrTableId fmrTableId(table.Cluster(), table.Table());
            if (table.Anonymous()) {
                TString cluster = table.Cluster(), path = table.Table();
                auto anonTableRichPath = GetWriteTable(sessionId, cluster, path, GetTablesTmpFolder(*options.Config(), cluster, Sessions_[options.SessionId()]->UseSecureTmp_, Sessions_[options.SessionId()]->OperationOptions_)).Cluster(cluster);
                fmrTableId = TFmrTableId(anonTableRichPath);
            }
            auto tableStatus = GetTablePresenceStatus(fmrTableId, sessionId);
            YQL_CLOG(INFO, FastMapReduce) << "GetTableInfo: table=" << fmrTableId
                << " status=" << static_cast<int>(tableStatus)
                << " anonymous=" << table.Anonymous()
                << " epoch=" << options.Epoch();
            if (tableStatus != ETablePresenceStatus::OnlyInFmr) {
                YQL_CLOG(INFO, FastMapReduce) << "GetTableInfo: delegating table " << fmrTableId << " to native gateway (status != OnlyInFmr)";
                ytBoundTables.emplace_back(table);
            } else if (IsDeferredUpload(fmrTableId, sessionId)) {
                YQL_CLOG(INFO, FastMapReduce) << "Table " << fmrTableId << " needs deferred upload to YT for GetTableInfo";

                ytBoundTables.emplace_back(table);
                deferredTableFmrIds.emplace_back(fmrTableId);

                TString cluster = table.Cluster();
                TString path = fmrTableId.Id.substr(cluster.size() + 1);

                auto& info = Sessions_[sessionId]->FmrTables[fmrTableId];
                TOutputInfo outputInfo;
                outputInfo.Path = path;
                outputInfo.Spec = info.DeferredUploadSpec;
                outputInfo.AttrSpec = NYT::TNode::CreateMap();

                YQL_CLOG(INFO, FastMapReduce) << "Output table cluster: " << cluster << " path: " << path;
                YQL_CLOG(INFO, FastMapReduce) << "Output table spec: " << NYT::NodeToYsonString(outputInfo.Spec);

                auto columnGroupSpec = GetColumnGroupSpec(fmrTableId, sessionId);
                if (!columnGroupSpec.empty()) {
                    outputInfo.ColumnGroups = NYT::NodeFromYsonString(columnGroupSpec);
                }

                YQL_CLOG(INFO, FastMapReduce) << "Columns groups spec: " << (outputInfo.ColumnGroups.IsUndefined() ? "" : NYT::NodeToYsonString(outputInfo.ColumnGroups));
                deferredOutputsByCluster[cluster].emplace_back(std::move(outputInfo));
            } else {
                IYtGateway::TTableInfoResult::TTableData fmrTableInfo;

                auto meta = GetFmrTableMeta(fmrTableId, sessionId);
                YQL_ENSURE(meta.DoesExist);

                fmrTableInfo.Meta = MakeIntrusive<TYtTableMetaInfo>(meta);
                fmrTableInfo.Stat = MakeIntrusive<TYtTableStatInfo>(GetFmrTableStats(fmrTableId, sessionId));
                fmrTableInfo.Stat->Id = table.Table();
                fmrTableInfo.WriteLock = false;
                fmrTablesInfo.emplace_back(fmrTableInfo);
                fmrTableIndexes.emplace(tableIndex);
            }
            ++tableIndex;
        }

        // If no tables for deffered upload
        if (deferredTableFmrIds.size() == 0) {
            YQL_CLOG(INFO, FastMapReduce) << "No deferred upload tables";
            if (ytBoundTables.empty()) {
                TTableInfoResult result;
                result.SetSuccess();
                result.Data = fmrTablesInfo;
                return MakeFuture<TTableInfoResult>(result);
            }
            TGetTableInfoOptions ytTablesOptions = std::move(options);
            ytTablesOptions.Tables() = ytBoundTables;
            return Slave_->GetTableInfo(std::move(ytTablesOptions)).Apply([fmrTablesInfo, tableIndex, fmrTableIndexes] (const auto& f) {
                return MergeTableInfoResults(f.GetValue(), fmrTablesInfo, fmrTableIndexes, tableIndex);
            });
        }

        YQL_CLOG(INFO, FastMapReduce) << "Have " << deferredTableFmrIds.size() << " deferred upload tables";

        std::vector<typename TExecContextSimple<TRunOptions>::TPtr> execCtxs;
        for (auto& [cluster, outputTables] : deferredOutputsByCluster) {
            TRunOptions runOptions(sessionId);
            runOptions.Config() = options.Config();
            auto execCtx = MakeExecCtx(std::move(runOptions), cluster, sessionId);
            execCtx->OutTables_ = {outputTables.begin(), outputTables.end()};
            execCtxs.emplace_back(std::move(execCtx));
        }

        YQL_CLOG(INFO, FastMapReduce) << "Dumping";

        // Uploading tables for differed upload
        return DumpFmrTablesToYt(execCtxs).Apply(
            [this, sessionId, options = std::move(options),
             ytBoundTables = std::move(ytBoundTables),
             deferredTableFmrIds = std::move(deferredTableFmrIds),
             fmrTablesInfo = std::move(fmrTablesInfo),
             fmrTableIndexes = std::move(fmrTableIndexes),
             tableIndex] (const auto& f) mutable {
            f.GetValue();

            YQL_CLOG(INFO, FastMapReduce) << "Uploaded " << deferredTableFmrIds.size() << " deferred tables to YT";

            for (auto& fmrTableId : deferredTableFmrIds) {
                SetTablePresenceStatus(fmrTableId, sessionId, ETablePresenceStatus::OnlyInYt);
                ClearDeferredUpload(fmrTableId, sessionId);
            }

            if (ytBoundTables.empty()) {
                TTableInfoResult result;
                result.SetSuccess();
                result.Data = {fmrTablesInfo.begin(), fmrTablesInfo.end()};
                return MakeFuture<TTableInfoResult>(result);
            }

            TGetTableInfoOptions ytTablesOptions = std::move(options);
            ytTablesOptions.Tables() = ytBoundTables;
            return Slave_->GetTableInfo(std::move(ytTablesOptions)).Apply(
                [fmrTablesInfo = std::move(fmrTablesInfo), fmrTableIndexes = std::move(fmrTableIndexes),
                 tableIndex] (const auto& f) {
                return MergeTableInfoResults(f.GetValue(), fmrTablesInfo, fmrTableIndexes, tableIndex);
            });
        });
    }

    static TFuture<TTableInfoResult> MergeTableInfoResults(
        TTableInfoResult ytTablesInfoResult,
        const TVector<IYtGateway::TTableInfoResult::TTableData>& fmrTablesInfo,
        const std::unordered_set<ui64>& fmrTableIndexes,
        ui64 totalCount)
    {
        TTableInfoResult allTablesResult;
        allTablesResult.SetSuccess();
        ui64 fmrTablePos = 0, ytTablePos = 0;
        for (ui64 i = 0; i < totalCount; ++i) {
            if (fmrTableIndexes.contains(i)) {
                allTablesResult.Data.emplace_back(fmrTablesInfo[fmrTablePos]);
                ++fmrTablePos;
            } else {
                if (ytTablePos >= ytTablesInfoResult.Data.size()) {
                    continue;
                }
                allTablesResult.Data.emplace_back(ytTablesInfoResult.Data[ytTablePos]);
                ++ytTablePos;
            }
        }
        return MakeFuture<TTableInfoResult>(allTablesResult);
    }

    TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        std::unordered_map<TString, TVector<TOutputInfo>> outputFmrTablesByCluster;
        auto nodePos = ctx.GetPosition(node->Pos());
        auto execCtx = MakeExecCtx(TResOrPullOptions(options), options.UsedCluster(), options.SessionId());
        if (TStringBuf("Pull") == node->Content()) {
            auto pull = NNodes::TPull(node);
            TVector<TYtTableBaseInfo::TPtr> inputTableInfos = GetInputTableInfos(pull.Input());
            if (options.FillSettings().Discard) {
                TResOrPullResult res;
                res.SetSuccess();
                return MakeFuture(res);
            }

            bool ref = NCommon::HasResOrPullOption(pull.Ref(), "ref");
            bool autoref = NCommon::HasResOrPullOption(pull.Ref(), "autoref");

            // Collect FMR-only tables; track whether any table is not FMR-only
            struct TFmrOnlyTable {
                TString Cluster;
                TString TablePath;
                TFmrTableId FmrTableId;
                TYtTableStatInfo Stats;
            };
            TVector<TFmrOnlyTable> fmrOnlyTables;
            bool allTablesAreFmrOnly = true;
            ui64 totalDataWeight = 0;
            ui64 totalRows = 0;

            for (auto& tableInfo: inputTableInfos) {
                auto config = options.Config();
                TString tmpFolder = GetTablesTmpFolder(*config, tableInfo->Cluster, Sessions_[options.SessionId()]->UseSecureTmp_, Sessions_[options.SessionId()]->OperationOptions_);
                TString tablePath = GetTransformedPath(options.SessionId(), tableInfo->Name, tmpFolder);
                TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(tableInfo->Cluster, tablePath), options.SessionId());
                auto status = GetTablePresenceStatus(fmrTableId, options.SessionId());
                if (status != ETablePresenceStatus::OnlyInFmr) {
                    allTablesAreFmrOnly = false;
                    continue;
                }
                auto stats = GetFmrTableStats(fmrTableId, options.SessionId());
                totalDataWeight += stats.DataSize;
                totalRows += stats.RecordsCount;
                fmrOnlyTables.push_back({tableInfo->Cluster, tablePath, fmrTableId, stats});
            }

            // Use Pull only when caller does not want a table ref and all tables are FMR-only and small
            if (!ref && !fmrOnlyTables.empty() && allTablesAreFmrOnly && totalDataWeight <= MaxDirectPullBytes_ && totalRows <= MaxDirectPullRows_) {
                TString sessionId = options.SessionId();
                auto config = options.Config();
                TString cluster = fmrOnlyTables[0].Cluster;

                // Resolve column selection before building params and building output
                TVector<TString> columns(NCommon::GetResOrPullColumnHints(*node));
                if (columns.empty()) {
                    columns = NCommon::GetStructFields(node->Child(0)->GetTypeAnn());
                }

                TPullOperationParams pullParams;
                for (const auto& tbl : fmrOnlyTables) {
                    TFmrTableRef fmrTableRef = GetFmrTableRef(tbl.FmrTableId, sessionId);
                    fmrTableRef.SerializedColumnGroups = GetColumnGroupSpec(fmrTableRef.FmrTableId, sessionId);
                    fmrTableRef.Columns = columns;
                    pullParams.Input.emplace_back(std::move(fmrTableRef));
                }

                TStartOperationRequest pullRequest{
                    .OperationType = EOperationType::Pull,
                    .OperationParams = pullParams,
                    .SessionId = sessionId,
                    .IdempotencyKey = GenerateId(),
                    .NumRetries = 1,
                    .FmrOperationSpec = config->FmrOperationSpec.Get(cluster)
                };

                bool hasTypeOpt = NCommon::HasResOrPullOption(*node, "type");
                const TTypeAnnotationNode* typeAnnotation = node->Child(0)->GetTypeAnn();
                auto rowLimit = options.FillSettings().RowsLimitPerWrite;
                auto byteLimit = options.FillSettings().AllResultsBytesLimit;
                auto ysonFormat = NCommon::GetYsonFormat(options.FillSettings());

                // Build input spec for YT {col=val} → YQL result format conversion (synchronous, before async callback)
                const auto nativeTypeCompat = config->NativeYtTypeCompatibility.Get(cluster).GetOrElse(NTCF_LEGACY);
                TVector<TString> tableNames;
                for (const auto& tbl : fmrOnlyTables) {
                    TInputInfo tableInfoEntry;
                    tableInfoEntry.Name = tbl.TablePath;
                    tableInfoEntry.Cluster = tbl.Cluster;
                    TString tmpFolder = GetTablesTmpFolder(*config, tbl.Cluster,
                        Sessions_[sessionId]->UseSecureTmp_,
                        Sessions_[sessionId]->OperationOptions_);
                    auto it = std::find_if(inputTableInfos.begin(), inputTableInfos.end(), [&](const auto& ti) {
                        return GetTransformedPath(sessionId, ti->Name, tmpFolder) == tbl.TablePath;
                    });
                    if (it != inputTableInfos.end() && (*it)->RowSpec) {
                        tableInfoEntry.Spec = FillAttrSpecNode(*(*it)->RowSpec, TResOrPullOptions(options), tbl.Cluster);
                    }
                    execCtx->InputTables_.emplace_back(std::move(tableInfoEntry));
                    tableNames.push_back(TString());
                }
                TString inputSpec = execCtx->GetInputSpec(false, nativeTypeCompat, false);
                const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry = execCtx->FunctionRegistry_;

                auto fmrJobFuture = GetUploadResourcesFuture(sessionId, config, {}, {}, execCtx->Options_.PublicId());

                YQL_CLOG(INFO, FastMapReduce) << "ResOrPull: using Pull operation for " << fmrOnlyTables.size() << " small FMR tables";
                return fmrJobFuture.Apply([=, this](const auto& fmrJobF) mutable {
                    pullRequest.FmrJob = fmrJobF.GetValue().FmrJob;
                    return GetRunningOperationFuture(pullRequest, sessionId, Nothing(), execCtx->Options_.PublicId(), /*isPull=*/true)
                    .Apply([pos = nodePos, hasTypeOpt, typeAnnotation, columns = std::move(columns),
                            rowLimit, byteLimit, ysonFormat, autoref,
                            inputSpec = std::move(inputSpec),
                            tableNames = std::move(tableNames),
                            functionRegistry]
                           (const TFuture<TFmrOperationResult>& f) {
                        try {
                            auto fmrResult = f.GetValue();
                            TResOrPullResult result;
                            if (!fmrResult.Success()) {
                                for (const auto& err : fmrResult.Errors) {
                                    result.AddIssues({ToIssue(err, pos)});
                                }
                                return MakeFuture<TResOrPullResult>(std::move(result));
                            }

                            // Build same output format as native/file gateway: {Type=...; Data=...; Truncated=...;}
                            TStringStream out;
                            NYson::TYsonWriter writer(&out, ysonFormat, ::NYson::EYsonType::Node, false);
                            writer.OnBeginMap();

                            if (hasTypeOpt) {
                                writer.OnKeyedItem("Type");
                                NCommon::WriteResOrPullType(writer, typeAnnotation, TColumnOrder(columns));
                            }

                            // Decode YT table-format rows to YQL result format via codec
                            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                                functionRegistry->SupportsSizedAllocators());
                            TMemoryUsageInfo memInfo("FmrPull");
                            TTypeEnvironment env(alloc);
                            THolderFactory holderFactory(alloc.Ref(), memInfo, functionRegistry);
                            NCommon::TCodecContext codecCtx(env, *functionRegistry, &holderFactory);
                            TMkqlIOSpecs specs;
                            specs.Init(codecCtx, inputSpec, tableNames, MakeMaybe(columns));
                            TMkqlIOCache specsCache(specs, holderFactory);

                            TMkqlInput input(MakeStringInput(fmrResult.PullData, false));
                            TMkqlReaderImpl reader(input, 0, 4 << 10, 0);
                            reader.SetSpecs(specs, holderFactory);

                            TYsonExecuteResOrPull resultData(rowLimit, byteLimit, MakeMaybe(columns));
                            for (reader.Next(); reader.IsValid(); reader.Next()) {
                                if (!resultData.WriteNext(specsCache, reader.GetRow(), 0)) {
                                    break;
                                }
                            }
                            TString dataStr = resultData.Finish();
                            bool truncated = resultData.IsTruncated();

                            if (truncated && autoref) {
                                result.AddIssues({TIssue(pos, "Pull result was truncated with autoref=true; restart without FMR")
                                    .SetCode(TIssuesIds::FMR_NEED_FALLBACK, TSeverityIds::S_ERROR)});
                                return MakeFuture<TResOrPullResult>(std::move(result));
                            }

                            writer.OnKeyedItem("Data");
                            writer.OnBeginList();
                            writer.OnRaw(dataStr, ::NYson::EYsonType::ListFragment);
                            writer.OnEndList();

                            if (truncated) {
                                writer.OnKeyedItem("Truncated");
                                writer.OnBooleanScalar(true);
                            }

                            writer.OnEndMap();
                            result.Data = out.Str();
                            result.SetSuccess();
                            return MakeFuture<TResOrPullResult>(std::move(result));
                        } catch (...) {
                            return MakeFuture(ResultFromCurrentException<TResOrPullResult>(pos));
                        }
                    });
                });
            }

            // Fall back to Upload+Slave path for large tables, mixed YT+FMR inputs, or writeRef case
            for (const auto& tbl : fmrOnlyTables) {
                auto config = options.Config();
                TString tmpFolder = GetTablesTmpFolder(*config, tbl.Cluster, Sessions_[options.SessionId()]->UseSecureTmp_, Sessions_[options.SessionId()]->OperationOptions_);
                auto it = std::find_if(inputTableInfos.begin(), inputTableInfos.end(), [&](const auto& ti) {
                    return GetTransformedPath(options.SessionId(), ti->Name, tmpFolder) == tbl.TablePath;
                });
                if (it == inputTableInfos.end()) {
                    continue;
                }
                TOutputInfo outputTableInfo;
                outputTableInfo.Path = tbl.TablePath;
                outputTableInfo.Spec = FillAttrSpecNode(*((*it)->RowSpec), TResOrPullOptions(options), tbl.Cluster);
                outputTableInfo.AttrSpec = NYT::TNode::CreateMap();
                outputFmrTablesByCluster[tbl.Cluster].emplace_back(outputTableInfo);
            }
        } else {
            // Result node: traverse YtTableContent inside the result expression and upload
            // any FMR-only tables to YT before delegating to the slave gateway.
            TString sessionId = options.SessionId();
            TString cluster = options.UsedCluster();
            auto config = options.Config();
            TString tmpFolder = GetTablesTmpFolder(*config, cluster, Sessions_[sessionId]->UseSecureTmp_, Sessions_[sessionId]->OperationOptions_);
            THashSet<TString> seen;

            auto resOptionsCopy = TResOrPullOptions(options);
            ScanYtTableContentTables(NNodes::TResult(node).Input().Ptr(),
                MakeAstFmrTableProcessor(resOptionsCopy, sessionId, cluster, tmpFolder,
                    outputFmrTablesByCluster, seen, "ResOrPull(Result)"));
        }
        if (!outputFmrTablesByCluster.empty()) {
            return UploadSeveralFmrTablesToYt<TResOrPullResult, TResOrPullOptions>(outputFmrTablesByCluster, TResOrPullOptions(options), nodePos)
                .Apply([this, node, &ctx, options = std::move(options)] (const auto& f) mutable {
                    auto uploadResult = f.GetValue();
                    if (!uploadResult.Success()) {
                        return MakeFuture(std::move(uploadResult));
                    }
                    return Slave_->ResOrPull(node, ctx, std::move(options));
                });
        }
        return Slave_->ResOrPull(node, ctx, std::move(options));
    }

    TFuture<TCalcResult> Calc(const TExprNode::TListType& nodes, TExprContext& ctx, TCalcOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        TString sessionId = options.SessionId();
        TString cluster = options.Cluster();
        auto config = options.Config();
        TString tmpFolder = GetTablesTmpFolder(*config, cluster, Sessions_[sessionId]->UseSecureTmp_, Sessions_[sessionId]->OperationOptions_);

        struct TCalcTableInfo {
            TString Cluster;
            TString Path;
            ETablePresenceStatus Status;
            TYtTableBaseInfo::TPtr TableInfo;
        };

        std::vector<TCalcTableInfo> tableRefs;
        THashSet<TString> seen;

        auto processTableInfo = [&](TYtTableBaseInfo::TPtr tableInfo) {
            if (tableInfo->Cluster.empty()) {
                tableInfo->Cluster = cluster;
            }
            TString tablePath = GetTransformedPath(sessionId, tableInfo->Name, tmpFolder);
            TString key = tableInfo->Cluster + "." + tablePath;
            if (seen.contains(key)) {
                return;
            }
            seen.insert(key);
            TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(tableInfo->Cluster, tablePath), sessionId);
            auto status = GetTablePresenceStatus(fmrTableId, sessionId);
            YQL_CLOG(INFO, FastMapReduce) << "Calc: found table " << fmrTableId << " status=" << static_cast<int>(status);
            tableRefs.push_back(TCalcTableInfo{
                .Cluster = tableInfo->Cluster,
                .Path = tablePath,
                .Status = status,
                .TableInfo = tableInfo
            });
        };

        YQL_CLOG(INFO, FastMapReduce) << "Calc: processing " << nodes.size() << " nodes";
        for (auto& node : nodes) {
            VisitExpr(node, [&](const TExprNode::TPtr& exprNode) {
                if (auto maybeContent = TMaybeNode<TYtTableContent>(exprNode)) {
                    auto content = maybeContent.Cast();
                    if (auto maybeRead = content.Input().Maybe<TYtReadTable>()) {
                        for (auto section : maybeRead.Cast().Input()) {
                            for (auto path : section.Paths()) {
                                processTableInfo(TYtTableBaseInfo::Parse(path.Table()));
                            }
                        }
                    } else if (auto maybeOutput = content.Input().Maybe<TYtOutput>()) {
                        processTableInfo(TYtTableBaseInfo::Parse(maybeOutput.Cast()));
                    }
                    return false;
                }
                if (auto maybeRead = TMaybeNode<TCoRight>(exprNode).Input().Maybe<TYtReadTable>()) {
                    for (auto section : maybeRead.Cast().Input()) {
                        for (auto path : section.Paths()) {
                            processTableInfo(TYtTableBaseInfo::Parse(path.Table()));
                        }
                    }
                    return false;
                }
                return true;
            });
        }

        bool hasFmrTables = false;
        for (auto& ref : tableRefs) {
            if (ref.Status == ETablePresenceStatus::OnlyInFmr) {
                hasFmrTables = true;
            }
        }

        if (hasFmrTables) {
            YQL_CLOG(INFO, FastMapReduce) << "Calc: uploading FMR tables to YT before delegating to native gateway";
            std::unordered_map<TString, TVector<TOutputInfo>> outputFmrTablesByCluster;
            for (auto& ref : tableRefs) {
                if (ref.Status != ETablePresenceStatus::OnlyInFmr) {
                    continue;
                }
                TOutputInfo outputTableInfo;
                outputTableInfo.Path = ref.Path;
                if (ref.TableInfo->RowSpec) {
                    outputTableInfo.Spec = FillAttrSpecNode(*ref.TableInfo->RowSpec, TCalcOptions(options), ref.Cluster);
                }
                outputTableInfo.AttrSpec = NYT::TNode::CreateMap();
                outputFmrTablesByCluster[ref.Cluster].emplace_back(outputTableInfo);
            }

            TPosition pos;
            if (!nodes.empty()) {
                pos = ctx.GetPosition(nodes.front()->Pos());
            }
            return UploadSeveralFmrTablesToYt<TCalcResult, TCalcOptions>(outputFmrTablesByCluster, TCalcOptions(options), pos)
                .Apply([this, nodes, &ctx, options = std::move(options)] (const auto& f) mutable {
                    auto uploadResult = f.GetValue();
                    if (!uploadResult.Success()) {
                        TCalcResult calcResult;
                        calcResult.AddIssues(uploadResult.Issues());
                        return MakeFuture(std::move(calcResult));
                    }
                    return Slave_->Calc(nodes, ctx, std::move(options));
                });
        }

        return Slave_->Calc(nodes, ctx, std::move(options));
    }

    TPathStatResult TryPathStat(TPathStatOptions&& options) final {
        auto [partialResult, nativePaths, nativePathIndexes] = SplitPathStatByPresence(options);

        if (nativePaths.empty()) {
            partialResult.SetSuccess();
            return partialResult;
        }

        TPathStatOptions nativeOptions(options.SessionId());
        nativeOptions.Cluster(options.Cluster()).Paths(nativePaths).Config(options.Config()).Extended(options.Extended());
        auto nativeResult = Slave_->TryPathStat(std::move(nativeOptions));
        return MergePathStatResults(std::move(partialResult), nativeResult, nativePathIndexes);
    }

    TFuture<TPathStatResult> PathStat(TPathStatOptions&& options) final {
        auto [partialResult, nativePaths, nativePathIndexes] = SplitPathStatByPresence(options);

        if (nativePaths.empty()) {
            partialResult.SetSuccess();
            return MakeFuture(std::move(partialResult));
        }

        TPathStatOptions nativeOptions(options.SessionId());
        nativeOptions.Cluster(options.Cluster()).Paths(nativePaths).Config(options.Config()).Extended(options.Extended());

        return Slave_->PathStat(std::move(nativeOptions)).Apply(
            [partialResult = std::move(partialResult), nativePathIndexes = std::move(nativePathIndexes)](const TFuture<TPathStatResult>& f) mutable {
                auto nativeResult = f.GetValue();
                return MergePathStatResults(std::move(partialResult), nativeResult, nativePathIndexes);
            });
    }

    void OpenSession(TOpenSessionOptions&& options) final {
        TString sessionId = options.SessionId();
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        with_lock(Mutex_) {
            if (Sessions_.contains(sessionId)) {
                YQL_LOG_CTX_THROW yexception() << "Session already exists: " << sessionId;
            }
        }

        TOpenSessionRequest openRequest{.SessionId = sessionId};
        Coordinator_->OpenSession(openRequest).GetValueSync();

        with_lock(Mutex_) {
            Sessions_[sessionId] = MakeIntrusive<TFmrSession>(sessionId, options.UserName(), options.RandomProvider(), options.TimeProvider(), options.OperationOptions(), options.ProgressWriter(), options.UseSecureTmp());
        }
        YQL_CLOG(INFO, FastMapReduce) << "Registered session " << sessionId << " with coordinator";

        Slave_->OpenSession(std::move(options));
    }

    TFuture<void> CloseSession(TCloseSessionOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        TString sessionId = options.SessionId();
        YQL_CLOG(INFO, FastMapReduce) << "Close session: " << sessionId;
        std::vector<TString> writeSessionIdsToCleanup;
        bool hasSession = false;
        TVector<std::pair<TString, NThreading::TPromise<TFmrOperationResult>>> pendingPromises;
        with_lock(Mutex_) {
            if (Sessions_.contains(sessionId)) {
                hasSession = true;
                auto& operationStates = Sessions_[sessionId]->OperationStates;
                auto& operationStatuses = operationStates.OperationStatuses;
                for (auto& [operationId, promise] : operationStatuses) {
                    if (!promise.IsReady()) {
                        YQL_CLOG(WARN, FastMapReduce) << "Resolving pending promise for operation " << operationId << " during session close";
                        pendingPromises.push_back(std::make_pair(operationId, promise));
                    }
                }
                for (const auto& [operationId, writeSessionId] : operationStates.SortedUploadOperations) {
                    writeSessionIdsToCleanup.push_back(writeSessionId);
                }
                Sessions_.erase(sessionId);

                for (const auto& writeSessionId : writeSessionIdsToCleanup) {
                    DistributedUploadSessions_.erase(writeSessionId);
                }
            }
        }

        for (auto [operationId, promise] : pendingPromises) {
            TFmrOperationResult fmrOperationResult{};
            fmrOperationResult.Errors.emplace_back(TFmrError{
                .Component = EFmrComponent::Gateway,
                .Reason = EFmrErrorReason::Unknown,
                .ErrorMessage = TStringBuilder() << "Session " << sessionId << " closed while operation " << operationId << " was still in progress"
            });
            promise.TrySetValue(std::move(fmrOperationResult));
        }

        std::vector<TFuture<void>> futures;
        if (hasSession) {
            try {
                futures.emplace_back(Coordinator_->ClearSession({.SessionId = sessionId}));
            } catch (...) {
                futures.emplace_back(MakeErrorFuture<void>(std::current_exception()));
            }
        }

        futures.emplace_back(Slave_->CloseSession(std::move(options)));
        return NThreading::WaitExceptionOrAll(futures);
    }

private:
    std::tuple<TPathStatResult, TVector<TPathStatReq>, TVector<size_t>> SplitPathStatByPresence(
        TPathStatOptions& options)
    {
        TString sessionId = options.SessionId();
        TString cluster = options.Cluster();
        auto config = options.Config();
        TString tmpFolder = GetTablesTmpFolder(*config, cluster, Sessions_[sessionId]->UseSecureTmp_, Sessions_[sessionId]->OperationOptions_);

        TPathStatResult result;
        result.DataSize.resize(options.Paths().size(), 0);
        result.Extended.resize(options.Paths().size());

        TVector<TPathStatReq> nativePaths;
        TVector<size_t> nativePathIndexes;

        for (size_t i = 0; i < options.Paths().size(); ++i) {
            auto& req = options.Paths()[i];
            TString tablePath = req.Path().Path_;
            TString transformedPath = GetTransformedPath(sessionId, tablePath, tmpFolder);
            TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(cluster, transformedPath), sessionId);
            auto status = GetTablePresenceStatus(fmrTableId, sessionId);

            if (status == ETablePresenceStatus::OnlyInFmr) {
                auto stats = GetFmrTableStats(fmrTableId, sessionId);
                result.DataSize[i] = stats.DataSize;
                YQL_CLOG(INFO, FastMapReduce) << "PathStat: returning FMR stats for " << fmrTableId
                    << " DataSize=" << stats.DataSize;
            } else {
                nativePaths.emplace_back(req);
                nativePathIndexes.push_back(i);
            }
        }

        return {std::move(result), std::move(nativePaths), std::move(nativePathIndexes)};
    }

    static TPathStatResult MergePathStatResults(
        TPathStatResult&& partialResult,
        const TPathStatResult& nativeResult,
        const TVector<size_t>& nativePathIndexes)
    {
        if (!nativeResult.Success()) {
            return nativeResult;
        }
        for (size_t j = 0; j < nativePathIndexes.size(); ++j) {
            size_t originalIdx = nativePathIndexes[j];
            if (j < nativeResult.DataSize.size()) {
                partialResult.DataSize[originalIdx] = nativeResult.DataSize[j];
            }
            if (j < nativeResult.Extended.size()) {
                partialResult.Extended[originalIdx] = nativeResult.Extended[j];
            }
        }
        partialResult.SetSuccess();
        return std::move(partialResult);
    }

    void RemoveFmrTablesWithDependents(
        const std::vector<TString>& tableIdsToRemove,
        const TString& sessionId)
    {
        if (tableIdsToRemove.empty()) {
            return;
        }

        auto& fmrTables = Sessions_[sessionId]->FmrTables;

        std::unordered_set<TFmrTableId> anonymousTablesToDelete;

        for (const auto& tableIdStr : tableIdsToRemove) {
            TFmrTableId tableId(tableIdStr);

            for (auto& [anonTableId, anonTableInfo] : fmrTables) {
                if (anonTableInfo.AnonymousTableFmrIdAlias && *anonTableInfo.AnonymousTableFmrIdAlias == tableId) {
                    YQL_CLOG(DEBUG, FastMapReduce)
                    << "Clearing alias in anonymous table " << anonTableId
                    << " (was pointing to " << tableId << ")";
                    anonTableInfo.TableMeta = fmrTables[tableId].TableMeta;
                    anonTableInfo.TableStats = fmrTables[tableId].TableStats;
                    anonTableInfo.ColumnGroupSpec = fmrTables[tableId].ColumnGroupSpec;
                    anonTableInfo.SortColumns = fmrTables[tableId].SortColumns;
                    anonTableInfo.SortOrder = fmrTables[tableId].SortOrder;
                    anonTableInfo.AnonymousTableFmrIdAlias = Nothing();
                }
            }
        }

        for (const auto& tableIdStr : tableIdsToRemove) {
            TFmrTableId tableId(tableIdStr);
            YQL_CLOG(DEBUG, FastMapReduce) << "Removing table " << tableId;
            fmrTables.erase(tableId);
        }
    }
    TString GenerateId() {
        return GetGuidAsString(RandomProvider_->GenGuid());
    }

    void SetTablePresenceStatus(const TFmrTableId& fmrTableId, const TString& sessionId, ETablePresenceStatus newStatus) {
        YQL_CLOG(DEBUG, FastMapReduce) << "Setting table presence status " << newStatus << " for table with id " << fmrTableId;
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        fmrTableInfo[fmrTableId].TablePresenceStatus = newStatus;
    }

    ETablePresenceStatus GetTablePresenceStatus(const TFmrTableId& fmrTableId, const TString& sessionId) {
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        return fmrTableInfo[fmrTableId].TablePresenceStatus;
    }

    void SetAnonymousTableFmrIdAlias(const TFmrTableId& fmrTableId, const TFmrTableId& alias, const TString& sessionId) {
        YQL_CLOG(DEBUG, FastMapReduce) << "Setting table fmr id alias " << alias << " for anonymous table with id " << fmrTableId;
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        fmrTableInfo[fmrTableId].AnonymousTableFmrIdAlias = alias;
    }

    TFmrTableId GetAliasOrFmrId(const TFmrTableId& fmrTableId, const TString& sessionId) {
        // In case of anonymous table input, return alias of fmr table corresponding to it.
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        auto alias = fmrTableInfo[fmrTableId].AnonymousTableFmrIdAlias;
        return alias ? *alias : fmrTableId;
    }

    TFmrTableRef GetFmrTableRef(TFmrTableId fmrTableId, const TString& sessionId, const std::vector<TString>& columns = {}, const TMaybe<TString>& serializedColumnGroups = Nothing()) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        auto alias = GetAliasOrFmrId(fmrTableId, sessionId);
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        TFmrTableRef fmrTableRef{alias};
        fmrTableRef.SortColumns = fmrTableInfo[alias].SortColumns;
        fmrTableRef.SortOrder = fmrTableInfo[alias].SortOrder;
        if (!columns.empty()) {
            fmrTableRef.Columns = columns;
        }
        if (serializedColumnGroups.Defined()) {
            fmrTableRef.SerializedColumnGroups = *serializedColumnGroups;
        }
        return fmrTableRef;
    }

    void SetColumnGroupSpec(const TFmrTableId& fmrTableId, const TString& columnGroupSpec, const TString& sessionId) {
        if (!columnGroupSpec.empty()) {
            YQL_CLOG(DEBUG, FastMapReduce) << "Setting column group spec " << columnGroupSpec << " for table " << fmrTableId;
        }
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        fmrTableInfo[fmrTableId].ColumnGroupSpec = columnGroupSpec;
    }

    void SetTableSortingSpec(const TFmrTableId& fmrTableId, const std::vector<TString>& sortColumns, const std::vector<ESortOrder>& sortOrder, const TString& sessionId) {
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        fmrTableInfo[fmrTableId].SortColumns = sortColumns;
        fmrTableInfo[fmrTableId].SortOrder = sortOrder;
    }

    void SetTableOutputSpec(const TFmrTableId& fmrTableId, const NYT::TNode& spec, const TString& sessionId) {
        Sessions_[sessionId]->FmrTables[fmrTableId].OutputSpec = spec;
    }

    NYT::TNode GetTableOutputSpec(const TFmrTableId& fmrTableId, const TString& sessionId) {
        return Sessions_[sessionId]->FmrTables[fmrTableId].OutputSpec;
    }

    TString GetColumnGroupSpec(const TFmrTableId& fmrTableId, const TString& sessionId) {
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        TString columnGroupSpec = fmrTableInfo[fmrTableId].ColumnGroupSpec;
        YQL_CLOG(DEBUG, FastMapReduce) << "Gotten column group spec " << columnGroupSpec << " for table " << fmrTableId;
        return columnGroupSpec;
    }

    void MarkForDeferredUpload(const TFmrTableId& fmrTableId, const NYT::TNode& spec, const TString& sessionId) {
        YQL_CLOG(DEBUG, FastMapReduce) << "Marking table " << fmrTableId << " for deferred upload to YT";
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        fmrTableInfo[fmrTableId].NeedsDeferredUpload = true;
        fmrTableInfo[fmrTableId].DeferredUploadSpec = spec;
    }

    bool IsDeferredUpload(const TFmrTableId& fmrTableId, const TString& sessionId) {
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        return fmrTableInfo[fmrTableId].NeedsDeferredUpload;
    }

    void ClearDeferredUpload(const TFmrTableId& fmrTableId, const TString& sessionId) {
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        fmrTableInfo[fmrTableId].NeedsDeferredUpload = false;
        fmrTableInfo[fmrTableId].DeferredUploadSpec = NYT::TNode();
    }


    void SetFmrTableStats(const TFmrTableId& fmrTableId, const TYtTableStatInfo& stats, const TString& sessionId) {
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        fmrTableInfo[fmrTableId].TableStats = stats;
    }

    TYtTableStatInfo GetFmrTableStats(const TFmrTableId& fmrTableId, const TString& sessionId) {
        YQL_ENSURE(Sessions_.contains(sessionId), "Session not found while reading table stats: " << sessionId);
        const auto& fmrTableInfo = Sessions_.at(sessionId)->FmrTables;
        auto it = fmrTableInfo.find(fmrTableId);
        YQL_ENSURE(it != fmrTableInfo.end(), "FMR table not found while reading table stats: " << fmrTableId);
        return it->second.TableStats;
    }

    void SetFmrTableMeta(const TFmrTableId& fmrTableId, const TYtTableMetaInfo& meta, const TString& sessionId) {
        YQL_CLOG(DEBUG, FastMapReduce) << "Setting meta for fmrTableId " << fmrTableId << "\n";
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        fmrTableInfo[fmrTableId].TableMeta = meta;
    }

    TYtTableMetaInfo GetFmrTableMeta(const TFmrTableId& fmrTableId, const TString& sessionId) {
        YQL_CLOG(DEBUG, FastMapReduce) << "Getting meta for fmrTableId " << fmrTableId << "\n";
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        return fmrTableInfo[fmrTableId].TableMeta;
    }

    TClusterConnection GetTableClusterConnection(const TString& cluster, const TString& sessionId, TYtSettings::TConstPtr& config) const {
        auto clusterConnectionOptions = TClusterConnectionOptions(sessionId).Cluster(cluster).Config(config);
        auto clusterConnection = GetClusterConnection(std::move(clusterConnectionOptions));
        TClusterConnection result{
            .TransactionId = clusterConnection.TransactionId,
            .YtServerName = clusterConnection.YtServerName,
            .Token = clusterConnection.Token
        };

        return result;
    }

    void FinalizeSortedUploadOperation(const TString& operationId, const TString& sessionId, const std::vector<TString>& fragmentResultsYson) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        IWriteDistributedSession::TPtr writeSessionPtr;
        TString writeSessionId;
        with_lock(Mutex_) {
            if (!Sessions_.contains(sessionId)) {
                return;
            }
            auto& session = Sessions_[sessionId];
            writeSessionId = session->OperationStates.SortedUploadOperations[operationId];
            auto it = DistributedUploadSessions_.find(writeSessionId);
            if (it == DistributedUploadSessions_.end()) {
                return;
            }
            writeSessionPtr = it->second;
        }

        writeSessionPtr->Finish(fragmentResultsYson);

        with_lock(Mutex_) {
            DistributedUploadSessions_.erase(writeSessionId);
        }
        YQL_CLOG(DEBUG, FastMapReduce) << "Successfully finalized distributed write session for operation " << operationId;
    }

    TFuture<TFmrOperationResult> GetRunningOperationFuture(
        const TStartOperationRequest& startOperationRequest,
        const TString& sessionId,
        const TMaybe<TString>& distributedWriteSession = Nothing(),
        const TMaybe<ui32>& publicId = Nothing(),
        bool isPull = false)
    {
        auto promise = NewPromise<TFmrOperationResult>();
        auto future = promise.GetFuture();
        YQL_CLOG(INFO, FastMapReduce) << "Starting " << startOperationRequest.OperationType << " operation";

        if (publicId.Defined()) {
            TOperationProgressWriter progressWriter;
            TMaybe<TOperationProgress> progress;
            with_lock(Mutex_) {
                auto& session = Sessions_[sessionId];
                progressWriter = session->ProgressWriter_;
                auto progressIt = session->OperationStates.LastProgress.find(*publicId);
                YQL_ENSURE(progressIt != session->OperationStates.LastProgress.end());
                progress = progressIt->second;
                UpdateStage(progress->Stage, TStringBuilder() << "FMR Starting (" << startOperationRequest.OperationType << ")");
                progress->State = TOperationProgress::EState::InProgress;
                progressIt->second = *progress;
            }
            progressWriter(*progress);
        }

        auto startOperationResponseFuture = Coordinator_->StartOperation(startOperationRequest);

        startOperationResponseFuture.Subscribe([this, promise = std::move(promise), sessionId, distributedWriteSession, publicId, isPull] (const auto& startOperationFuture) mutable {
            TStartOperationResponse startOperationResponse = startOperationFuture.GetValueSync();
            if (startOperationResponse.Status == EOperationStatus::Failed) {
                YQL_CLOG(ERROR, FastMapReduce) << "Failed to start operation";
                TFmrOperationResult result;
                result.Errors = startOperationResponse.ErrorMessages;
                promise.SetValue(std::move(result));
                return;
            }
            TString operationId = startOperationResponse.OperationId;

            TOperationProgressWriter progressWriter;
            TMaybe<TOperationProgress> progress;
            with_lock(Mutex_) {
                auto& session = Sessions_[sessionId];
                auto& operationStates = session->OperationStates;
                auto& operationStatuses = operationStates.OperationStatuses;
                YQL_ENSURE(!operationStatuses.contains(operationId));
                operationStatuses[operationId] = promise;
                operationStates.OperationPublicIds[operationId] = publicId;

                if (distributedWriteSession.Defined()) {
                    operationStates.SortedUploadOperations.emplace(operationId, *distributedWriteSession);
                    YQL_CLOG(INFO, FastMapReduce) << "Marked operation " << operationId << " as distributed";
                }
                if (isPull) {
                    operationStates.PullOperations.emplace(operationId);
                    YQL_CLOG(INFO, FastMapReduce) << "Marked operation " << operationId << " as pull";
                }
                if (publicId.Defined()) {
                    progressWriter = session->ProgressWriter_;
                    progress = TOperationProgress(TString(YtProviderName), *publicId, TOperationProgress::EState::InProgress, "FMR Running");
                    if (FmrServices_->VanillaRemoteId.Defined()) {
                        progress->RemoteId = *FmrServices_->VanillaRemoteId;
                    }

                    auto progressIt = operationStates.LastProgress.find(*publicId);
                    YQL_ENSURE(progressIt != operationStates.LastProgress.end());
                    progressIt->second = *progress;
                }
            }
            if (publicId.Defined()) {
                progressWriter(*progress);
            }
        });
        return future;
    }

    TFuture<TFmrOperationResult> GetRunningSortedWriteOperationFuture(
        const TStartOperationRequest& startOperationRequest,
        const TString& sessionId)
    {
        auto promise = NewPromise<TFmrOperationResult>();
        auto future = promise.GetFuture();
        YQL_CLOG(INFO, FastMapReduce) << "Starting " << startOperationRequest.OperationType << " operation";
        auto startOperationResponseFuture = Coordinator_->StartOperation(startOperationRequest);

        startOperationResponseFuture.Subscribe([this, promise = std::move(promise), sessionId, startOperationRequest] (const auto& startOperationFuture) mutable {
            TStartOperationResponse startOperationResponse = startOperationFuture.GetValueSync();
            if (startOperationResponse.Status == EOperationStatus::Failed) {
                YQL_CLOG(ERROR, FastMapReduce) << "Failed to start operation";
                TFmrOperationResult result;
                result.Errors = startOperationResponse.ErrorMessages;
                promise.SetValue(std::move(result));
                return;
            }
            TString operationId = startOperationResponse.OperationId;

            with_lock(Mutex_) {
                auto& operationStates = Sessions_[sessionId]->OperationStates;
                auto& operationStatuses = operationStates.OperationStatuses;
                YQL_ENSURE(!operationStatuses.contains(operationId));
                operationStatuses[operationId] = promise;

                if (startOperationRequest.OperationType == EOperationType::SortedUpload) {
                    auto params = std::get<TSortedUploadOperationParams>(startOperationRequest.OperationParams);
                    TString session = params.SessionId;
                    operationStates.SortedUploadOperations.emplace(operationId, session);
                    YQL_CLOG(INFO, FastMapReduce) << "Marked operation " << operationId << " as distributed";
                }
            }
        });
        return future;
    }

    NYT::TRichYPath GetFilledRichPathFromInputTable(const TInputInfo& inputInfo) {
        auto richPath = inputInfo.Path;
        richPath.Path(inputInfo.Name).Cluster(inputInfo.Cluster);
        return richPath;
    }

    std::pair<std::vector<TOperationTableRef>, std::unordered_map<TFmrTableId, TClusterConnection>> GetInputTablesAndConnections(
        const std::vector<TInputInfo>& inputTables,
        const TString& sessionId,
        TYtSettings::TConstPtr& config)
    {
        std::vector<TOperationTableRef> operationInputTables;
        std::unordered_map<TFmrTableId, TClusterConnection> clusterConnections;
        for (auto& ytTable: inputTables) {
            TString inputCluster = ytTable.Cluster;
            auto richPath = GetFilledRichPathFromInputTable(ytTable);
            TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(richPath), sessionId);
            auto tablePresenceStatus = GetTablePresenceStatus(fmrTableId, sessionId);
            if (tablePresenceStatus == ETablePresenceStatus::Undefined) {
                SetTablePresenceStatus(fmrTableId, sessionId, ETablePresenceStatus::OnlyInYt);
            }

            if (tablePresenceStatus == ETablePresenceStatus::OnlyInFmr || tablePresenceStatus == ETablePresenceStatus::Both) {
                // table is in fmr, do not download
                TFmrTableRef fmrTableRef = GetFmrTableRef(fmrTableId, sessionId);
                fmrTableRef.SerializedColumnGroups = GetColumnGroupSpec(fmrTableRef.FmrTableId, sessionId);
                YQL_CLOG(INFO, FastMapReduce) << "GetInputTables: table=" << fmrTableRef.FmrTableId
                    << " columnGroups=" << (fmrTableRef.SerializedColumnGroups.empty() ? "(empty)" : fmrTableRef.SerializedColumnGroups.substr(0, 200));
                if (!richPath.Columns_.Empty()) {
                    std::vector<TString> neededColumns(richPath.Columns_->Parts_.begin(), richPath.Columns_->Parts_.end());
                    fmrTableRef.Columns = neededColumns;
                }
                operationInputTables.emplace_back(fmrTableRef);
            } else {
                TYtTableRef ytTableRef(richPath);
                ytTableRef.FilePath = GetTableFilePath(TGetTableFilePathOptions(sessionId).Cluster(inputCluster).Path(ytTable.Name).IsTemp(ytTable.Temp));
                operationInputTables.emplace_back(ytTableRef);
                auto connection = GetTableClusterConnection(ytTable.Cluster, sessionId, config);
                clusterConnections.emplace(fmrTableId, connection);
            }
        }
        return {operationInputTables, clusterConnections};
    }

    std::vector<TString> GetOutputTablesColumnGroups(const TExecContextSimple<TRunOptions>::TPtr& execCtx) {
    std::vector<TString> columnGroups;
        for (size_t idx = 0; idx < execCtx->OutTables_.size(); ++idx) {
            auto& out = execCtx->OutTables_[idx];
            auto curTableColumnGroups = out.ColumnGroups;
            if (curTableColumnGroups.IsUndefined()) {
                YQL_CLOG(INFO, FastMapReduce) << "GetOutputTablesColumnGroups[" << idx << "]: table=" << out.Path << " ColumnGroups=Undefined";
                columnGroups.emplace_back(TString());
                continue;
            }
            auto serialized = NYT::NodeToYsonString(curTableColumnGroups);
            YQL_CLOG(INFO, FastMapReduce) << "GetOutputTablesColumnGroups[" << idx << "]: table=" << out.Path << " ColumnGroups=" << serialized.substr(0, 200);
            columnGroups.emplace_back(serialized);
        }
        return columnGroups;
    }

    static void StripForeignSortColumns(NYT::TNode& rowSpec) {
        if (!rowSpec.IsMap()) {
            return;
        }
        if (!rowSpec.HasKey("SortMembers") || !rowSpec.HasKey("SortedBy")) {
            return;
        }
        const auto& sortMembers = rowSpec["SortMembers"].AsList();
        const auto& sortedBy = rowSpec["SortedBy"].AsList();
        if (sortMembers.size() >= sortedBy.size()) {
            return;
        }
        size_t n = sortMembers.size();
        rowSpec["SortedBy"] = rowSpec["SortMembers"];
        if (rowSpec.HasKey("SortDirections")) {
            auto& dirs = rowSpec["SortDirections"].AsList();
            while (dirs.size() > n) {
                dirs.pop_back();
            }
        }
        if (rowSpec.HasKey("SortedByTypes")) {
            auto& types = rowSpec["SortedByTypes"].AsList();
            while (types.size() > n) {
                types.pop_back();
            }
        }
    }

    bool GetIsSorted(const TOutputInfo& outputTable) {
        if (!outputTable.Spec.HasKey(YqlRowSpecAttribute)) {
            return false;
        }
        const auto& rowSpec = outputTable.Spec[YqlRowSpecAttribute];
        if (!rowSpec.HasKey("SortedBy") || rowSpec["SortedBy"].AsList().empty()) {
            return false;
        }
        YQL_ENSURE(rowSpec.HasKey("SortMembers") && !rowSpec["SortMembers"].AsList().empty(),
            "Table spec has SortedBy but missing SortMembers");
        return true;
    }

    std::vector<TString> GetTableColumns(const TOutputInfo& outputTable) {
        std::vector<TString> columns;
        THashSet<TString> seen;

        if (outputTable.Spec.HasKey(YqlRowSpecAttribute)) {
            const auto& rowSpec = outputTable.Spec[YqlRowSpecAttribute];
            if (rowSpec.HasKey(RowSpecAttrType) && rowSpec[RowSpecAttrType].IsList()) {
                for (const auto& entry : rowSpec[RowSpecAttrType].AsList()[1].AsList()) {
                    const auto& name = entry[0].AsString();
                    if (seen.insert(name).second) {
                        columns.emplace_back(name);
                    }
                }
            }
            // Include presort columns synthesized for DESC sort (present in SortedBy but not in Type).
            // These are real columns in the FMR table data and must be uploaded to YT, since the
            // target YT schema also requires them.
            if (rowSpec.HasKey("SortedBy")) {
                for (const auto& item : rowSpec["SortedBy"].AsList()) {
                    const auto& name = item.AsString();
                    if (seen.insert(name).second) {
                        columns.emplace_back(name);
                    }
                }
            }
        }
        return columns;
    }

    std::vector<TString> GetTableSortedColumns(const TOutputInfo& outputTable) {
        std::vector<TString> columns;

        if (outputTable.Spec.HasKey(YqlRowSpecAttribute)) {
            const auto& rowSpec = outputTable.Spec[YqlRowSpecAttribute];
            if (rowSpec.HasKey("SortMembers") && !rowSpec["SortMembers"].AsList().empty()) {
                for (const auto& item : rowSpec["SortMembers"].AsList()) {
                    columns.emplace_back(item.AsString());
                }
            }
        }
        return columns;
    }

    std::vector<ESortOrder> GetTableSortedOrders(const TOutputInfo& outputTable) {
        std::vector<ESortOrder> sortOrders;

        if (outputTable.Spec.HasKey(YqlRowSpecAttribute)) {
            const auto& rowSpec = outputTable.Spec[YqlRowSpecAttribute];
            ui64 limit = std::numeric_limits<size_t>::max();
            if (rowSpec.HasKey("SortMembers")) {
                limit = rowSpec["SortMembers"].AsList().size();
            }
            if (rowSpec.HasKey("SortDirections") && !rowSpec["SortDirections"].AsList().empty()) {
                for (const auto& item : rowSpec["SortDirections"].AsList()) {
                    if (sortOrders.size() >= limit) {
                        break;
                    }
                    sortOrders.emplace_back(item.AsInt64() == 0 ? ESortOrder::Descending : ESortOrder::Ascending);
                }
            }
        }
        return sortOrders;
    }

    template<class TOptions>
    NYT::TNode FillAttrSpecNode(const TYqlRowSpecInfo yqlRowSpecInfo, TOptions&& options, const TString& cluster) {
        NYT::TNode res = NYT::TNode::CreateMap();
        const auto nativeTypeCompat = options.Config()->NativeYtTypeCompatibility.Get(cluster).GetOrElse(NTCF_LEGACY);
        yqlRowSpecInfo.FillAttrNode(res[YqlRowSpecAttribute], nativeTypeCompat, false);
        return res;
    }


    template<class TExecCtx>
    TFuture<TFmrOperationResult> DoSortedUploadTableFromFmrToYt(const TExecCtx& execCtx, ui64 outputTableIndex) {
        TString sessionId = execCtx->GetSessionId();
        TYtSettings::TConstPtr& config = execCtx->Options_.Config();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);


        auto& fmrTable = execCtx->OutTables_[outputTableIndex];
        TString outputPath = fmrTable.Path;
        TString outputCluster = execCtx->Cluster_;

        TFmrTableId originalTableId(outputCluster, outputPath);
        TFmrTableRef fmrTableRef = GetFmrTableRef(originalTableId, sessionId);
        fmrTableRef.Columns = GetTableColumns(fmrTable);
        auto tablePresenceStatus = GetTablePresenceStatus(originalTableId, sessionId);

        if (tablePresenceStatus != ETablePresenceStatus::OnlyInFmr) {
            YQL_CLOG(TRACE, FastMapReduce) << "Table " << originalTableId << " has table presence status " << tablePresenceStatus << " so it won't be uploaded from fmr to yt";
            return GetSuccessfulFmrOperationResult();
        }

        fmrTableRef.SerializedColumnGroups = GetColumnGroupSpec(fmrTableRef.FmrTableId, sessionId);

        auto richPath = GetWriteTableWithTransformedPath(sessionId, outputCluster, outputPath);
        auto filePath = fmrTable.FilePath;
        if (!filePath.Defined()) {
            filePath = GetTableFilePath(TGetTableFilePathOptions(sessionId).Cluster(outputCluster).Path(outputPath).IsTemp(true));
        }

        fmrTable.FilePath = filePath;
        PrepareDestination(execCtx, outputTableIndex);

        auto clusterConnection = GetTableClusterConnection(outputCluster, sessionId, config);

        TSortedUploadOperationParams SortedUploadOperationParams{
            .Input = fmrTableRef,
            .Output = TYtTableRef(richPath, filePath),
            .IsOrdered = true
        };

        TPrepareOperationRequest PrepareOperationRequest{
            .OperationType = EOperationType::SortedUpload,
            .OperationParams = SortedUploadOperationParams,
            .ClusterConnections = std::unordered_map<TFmrTableId, TClusterConnection>{{fmrTableRef.FmrTableId, clusterConnection}},
            .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
        };

        YQL_CLOG(TRACE, FastMapReduce) << "Creating partition for distributed upload from fmr to yt for table: " << fmrTableRef.FmrTableId;

        auto publicId = execCtx->Options_.PublicId();
        auto fmrJobFuture = GetUploadResourcesFuture(sessionId, config, {}, {}, publicId);
        return Coordinator_->PrepareOperation(PrepareOperationRequest).Apply([this, sessionId, outputCluster, clusterConnection, config, SortedUploadOperationParams, originalTableId, publicId, fmrJobFuture] (const auto& PrepareOperationFuture) mutable {
            try {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                auto PrepareOperationResponse = PrepareOperationFuture.GetValue();
                if (!PrepareOperationResponse.ErrorMessages.empty()) {
                    TFmrOperationResult result;
                    result.Errors = PrepareOperationResponse.ErrorMessages;
                    return MakeFuture(result);
                }
                TString partitionId = PrepareOperationResponse.PartitionId;
                ui64 tasksNum = PrepareOperationResponse.TasksNum;

                YQL_CLOG(DEBUG, FastMapReduce) << "Partition created with id: " << partitionId << ", tasks num: " << tasksNum;
                YQL_CLOG(TRACE, FastMapReduce) << "Creating session for distributed upload from fmr to yt";
                auto options = TStartDistributedWriteOptions::FromSpec(config->FmrOperationSpec.Get(outputCluster));
                auto writeSession = YtJobService_->StartDistributedWriteSession(
                    SortedUploadOperationParams.Output,
                    tasksNum,
                    clusterConnection,
                    options
                );

                const TString& writeSessionId = writeSession->GetId();
                with_lock(Mutex_) {
                    DistributedUploadSessions_[writeSessionId] = writeSession;
                }

                YQL_CLOG(TRACE, FastMapReduce) << "Distributed session started!";
                auto cookies = writeSession->GetCookies();
                YQL_CLOG(DEBUG, FastMapReduce) << "Distributed Cookies count: " << cookies.size();

                SortedUploadOperationParams.UpdateAfterPreparation(cookies, partitionId);

                auto fmrTableId = SortedUploadOperationParams.Input.FmrTableId;

                TStartOperationRequest SortedUploadRequest{
                    .OperationType = EOperationType::SortedUpload,
                    .OperationParams = SortedUploadOperationParams,
                    .SessionId = sessionId,
                    .IdempotencyKey = GenerateId(),
                    .NumRetries = 1,
                    .ClusterConnections = std::unordered_map<TFmrTableId, TClusterConnection>{{fmrTableId, clusterConnection}},
                    .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
                };

                YQL_CLOG(TRACE, FastMapReduce) << "Starting SortedUpload from fmr to yt for table: " << fmrTableId;
                return fmrJobFuture.Apply([=, this](const auto& fmrJobF) mutable {
                    SortedUploadRequest.FmrJob = fmrJobF.GetValue().FmrJob;
                    return GetRunningOperationFuture(SortedUploadRequest, sessionId, writeSessionId, publicId).Apply([this, sessionId, originalTableId] (const TFuture<TFmrOperationResult>& f) {
                    try {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                        auto fmrUploadResult = f.GetValue();
                        YQL_CLOG(TRACE, FastMapReduce) << "GATEWAY: Distributed upload requested, get running operation feature";
                        SetTablePresenceStatus(originalTableId, sessionId, ETablePresenceStatus::OnlyInYt);
                        ClearDeferredUpload(originalTableId, sessionId);
                        fmrUploadResult.SetSuccess();
                        fmrUploadResult.SetRepeat(true);
                        return MakeFuture<TFmrOperationResult>(fmrUploadResult);
                    } catch (...) {
                        YQL_CLOG(ERROR, FastMapReduce) << CurrentExceptionMessage();
                        return MakeFuture(ResultFromCurrentException<TFmrOperationResult>());
                    }
                });
                });
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << "Error creating partition: " << CurrentExceptionMessage();
                return MakeFuture(ResultFromCurrentException<TFmrOperationResult>());
            }
        });
    }

    template<class TExecCtx>
    TFuture<TFmrOperationResult> DoUploadTableFromFmrToYt(const TExecCtx& execCtx, ui64 outputTableIndex) {
        TString sessionId = execCtx->GetSessionId();
        TYtSettings::TConstPtr& config = execCtx->Options_.Config();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);

        auto& fmrTable = execCtx->OutTables_[outputTableIndex];
        // Uploading this table to yt with the same cluster and path as in fmr (Dump for unsupported operations)

        TString outputPath = fmrTable.Path;
        TString outputCluster = execCtx->Cluster_;

        TFmrTableId originalTableId(outputCluster, outputPath);
        TFmrTableRef fmrTableRef = GetFmrTableRef(originalTableId, sessionId);
        fmrTableRef.Columns = GetTableColumns(fmrTable);
        auto tablePresenceStatus = GetTablePresenceStatus(originalTableId, sessionId);

        if (tablePresenceStatus != ETablePresenceStatus::OnlyInFmr) {
            YQL_CLOG(INFO, FastMapReduce) << "Table " << originalTableId << " has table presence status " << tablePresenceStatus << " so it won't be upload from fmr to yt";
            return GetSuccessfulFmrOperationResult();
        }

        fmrTableRef.SerializedColumnGroups = GetColumnGroupSpec(fmrTableRef.FmrTableId, sessionId);

        auto richPath = GetWriteTableWithTransformedPath(sessionId, outputCluster, outputPath);

        auto filePath = GetTableFilePath(TGetTableFilePathOptions(sessionId).Cluster(outputCluster).Path(outputPath).IsTemp(true));

        fmrTable.FilePath = filePath;
        PrepareDestination(execCtx, outputTableIndex);

        TUploadOperationParams uploadOperationParams{.Input = fmrTableRef, .Output = TYtTableRef(richPath, filePath)};
        auto clusterConnection = GetTableClusterConnection(outputCluster, sessionId, config);
        TStartOperationRequest uploadRequest{
            .OperationType = EOperationType::Upload,
            .OperationParams = uploadOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries = 1,
            .ClusterConnections = std::unordered_map<TFmrTableId, TClusterConnection>{{fmrTableRef.FmrTableId, clusterConnection}},
            .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
        };

        auto fmrJobFuture = GetUploadResourcesFuture(sessionId, config, {}, {}, execCtx->Options_.PublicId());
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_CLOG(INFO, FastMapReduce) << "Starting upload from fmr to yt for table: " << originalTableId;
        return fmrJobFuture.Apply([=, this](const auto& fmrJobF) mutable {
            uploadRequest.FmrJob = fmrJobF.GetValue().FmrJob;
            return GetRunningOperationFuture(uploadRequest, sessionId, Nothing(), execCtx->Options_.PublicId()).Apply([this, sessionId = std::move(sessionId), originalTableId = std::move(originalTableId)] (const TFuture<TFmrOperationResult>& f) {
                try {
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                    auto fmrUploadResult = f.GetValue();
                    SetTablePresenceStatus(originalTableId, sessionId, ETablePresenceStatus::OnlyInYt);
                    ClearDeferredUpload(originalTableId, sessionId);
                    fmrUploadResult.SetSuccess();
                    fmrUploadResult.SetRepeat(true);
                    return MakeFuture<TFmrOperationResult>(fmrUploadResult);
                } catch (...) {
                    YQL_CLOG(ERROR, FastMapReduce) << CurrentExceptionMessage();
                    return MakeFuture(ResultFromCurrentException<TFmrOperationResult>());
                }
            });
        });
    }

    bool IsFmrTableSorted(const TString& cluster, const TString& path, const TString& sessionId) {
        TFmrTableId fmrTableId(cluster, path);
        auto& fmrTables = Sessions_[sessionId]->FmrTables;
        auto it = fmrTables.find(fmrTableId);
        if (it == fmrTables.end()) {
            return false;
        }
        return !it->second.SortColumns.empty();
    }

    template<class TExecCtx>
    void StartUploadAsync(
        const TExecCtx& execCtx,
        ui64 tableIndex,
        bool isOrdered,
        const TFmrTableId& tableId,
        TPromise<TFmrOperationResult> promise)
    {
        TFuture<TFmrOperationResult> realFuture;
        try {
            if (isOrdered) {
                realFuture = DoSortedUploadTableFromFmrToYt(execCtx, tableIndex);
            } else {
                realFuture = DoUploadTableFromFmrToYt(execCtx, tableIndex);
            }
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "StartUploadAsync: sync error for " << tableId << ": " << CurrentExceptionMessage();
            with_lock(Mutex_) {
                InFlightUploads_.erase(tableId);
            }
            promise.SetValue(ResultFromCurrentException<TFmrOperationResult>());
            return;
        }

        realFuture.Subscribe([this, tableId, promise = std::move(promise)](const auto& f) mutable {
            TFmrOperationResult result;
            try {
                result = f.GetValue();
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << "StartUploadAsync: async error for " << tableId << ": " << CurrentExceptionMessage();
                result = ResultFromCurrentException<TFmrOperationResult>();
            }
            with_lock(Mutex_) {
                InFlightUploads_.erase(tableId);
            }
            promise.SetValue(std::move(result));
        });
    }

    template<class TExecCtx>
    TFuture<TFmrOperationResult> DumpFmrTablesToYt(const std::vector<TExecCtx>& execCtxs) {
        std::vector<TFuture<TFmrOperationResult>> uploadFmrTableToYtFutures;
        for (auto& ctx: execCtxs) {
            TString sessionId = ctx->GetSessionId();
            for (ui64 tableIndex = 0; tableIndex < ctx->OutTables_.size(); ++tableIndex) {
                const auto& outputTable = ctx->OutTables_[tableIndex];
                bool isOrdered = IsFmrTableSorted(ctx->Cluster_, outputTable.Path, sessionId);
                YQL_CLOG(INFO, FastMapReduce) << "DumpFmrTablesToYt: table=" << ctx->Cluster_ << "." << outputTable.Path
                    << " isOrdered(fmr)=" << isOrdered << " isOrdered(spec)=" << GetIsSorted(outputTable);

                TFmrTableId tableId(ctx->Cluster_, outputTable.Path);
                TFuture<TFmrOperationResult> uploadFuture;
                TMaybe<TPromise<TFmrOperationResult>> newPromise;

                with_lock(Mutex_) {
                    auto it = InFlightUploads_.find(tableId);
                    if (it != InFlightUploads_.end()) {
                        YQL_CLOG(INFO, FastMapReduce) << "DumpFmrTablesToYt: reusing in-flight upload for " << tableId;
                        uploadFuture = it->second;
                    } else {
                        auto promise = NewPromise<TFmrOperationResult>();
                        uploadFuture = promise.GetFuture();
                        InFlightUploads_[tableId] = uploadFuture;
                        newPromise = std::move(promise);
                    }
                }

                if (newPromise.Defined()) {
                    StartUploadAsync(ctx, tableIndex, isOrdered, tableId, std::move(*newPromise));
                }

                uploadFmrTableToYtFutures.emplace_back(uploadFuture);
            }
        }
        return WaitExceptionOrAll(uploadFmrTableToYtFutures).Apply([uploadFmrTableToYtFutures = std::move(uploadFmrTableToYtFutures)] (const auto& f) {
            f.GetValue();
            std::vector<TFmrOperationResult> uploadFmrToYtOperationResults;
            for (auto& uploadTableFuture: uploadFmrTableToYtFutures) {
                uploadFmrToYtOperationResults.emplace_back(uploadTableFuture.GetValue());
            }
            return MakeFuture<TFmrOperationResult>(MergeSeveralFmrOperationResults(uploadFmrToYtOperationResults));
        });
    }

    template<class TExecCtx>
    void PrepareDestination(const TExecCtx& execCtx, ui64 outputTableIndex) {
        TString sessionId = execCtx->GetSessionId();
        TYtSettings::TConstPtr& config = execCtx->Options_.Config();

        auto& outputTable = execCtx->OutTables_[outputTableIndex];
        TString outputPath = outputTable.Path, outputCluster = execCtx->Cluster_;
        TMaybe<TString> filePath = outputTable.FilePath;

        auto clusterConnection = GetTableClusterConnection(outputCluster, sessionId, config);

        TFmrTableId fmrTableId(outputCluster, outputPath);

        NYT::TNode rowSpecForSchema = outputTable.Spec.HasKey(YqlRowSpecAttribute)
            ? outputTable.Spec[YqlRowSpecAttribute]
            : NYT::TNode::CreateMap();
        StripForeignSortColumns(rowSpecForSchema);

        NYT::TNode attrs = NYT::TNode::CreateMap();
        attrs[YqlRowSpecAttribute] = rowSpecForSchema;
        PrepareAttributes(attrs, outputTable, execCtx, outputCluster, true, {});
        attrs["optimize_for"] = "scan";

        const auto nativeTypeCompat = config->NativeYtTypeCompatibility.Get(outputCluster).GetOrElse(NTCF_LEGACY);
        attrs["schema"] = RowSpecToYTSchema(rowSpecForSchema, nativeTypeCompat, outputTable.ColumnGroups).ToNode();
        YtJobService_->Create(TYtTableRef(outputCluster, outputPath, filePath), clusterConnection, attrs);
    }

    std::vector<TFmrTableRef> GetOutputTables(const TExecContextSimple<TRunOptions>::TPtr& execCtx) {
        TString sessionId = execCtx->GetSessionId();
        std::vector<TFmrTableRef> fmrOutputTables;
        auto outputTableColumnGroups = GetOutputTablesColumnGroups(execCtx);
        auto outputTables = execCtx->OutTables_;
        YQL_ENSURE(outputTables.size() == outputTableColumnGroups.size());

        for (ui64 i = 0; i < outputTables.size(); ++i) {

            auto& outputTable = outputTables[i];
            TFmrTableId outputTableFmrId(execCtx->Cluster_, outputTable.Path);
            auto columnGroupSpec = outputTableColumnGroups[i];
            YQL_CLOG(INFO, FastMapReduce) << "GetOutputTables: table=" << outputTableFmrId
                << " columnGroups=" << (columnGroupSpec.empty() ? "(empty)" : columnGroupSpec.substr(0, 200))
;
            if (outputTable.Spec.HasKey(YqlRowSpecAttribute)) {
                const auto& rowSpec = outputTable.Spec[YqlRowSpecAttribute];
                if (rowSpec.HasKey("SortedBy")) {
                    YQL_CLOG(INFO, FastMapReduce) << "GetOutputTables: table=" << outputTableFmrId
                        << " SortedBy=" << NYT::NodeToYsonString(rowSpec["SortedBy"]);
                }
                if (rowSpec.HasKey("SortMembers")) {
                    YQL_CLOG(INFO, FastMapReduce) << "GetOutputTables: table=" << outputTableFmrId
                        << " SortMembers=" << NYT::NodeToYsonString(rowSpec["SortMembers"]);
                }
            }
            SetColumnGroupSpec(outputTableFmrId, columnGroupSpec, sessionId);
            SetTableOutputSpec(outputTableFmrId, outputTable.Spec, sessionId);

            TFmrTableRef fmrOutputTable{
                .FmrTableId = outputTableFmrId,
                .SerializedColumnGroups = columnGroupSpec
            };
            if (GetIsSorted(outputTable)) {
                fmrOutputTable.SortColumns = GetTableSortedColumns(outputTable);
                fmrOutputTable.SortOrder = GetTableSortedOrders(outputTable);
                SetTableSortingSpec(outputTableFmrId, fmrOutputTable.SortColumns, fmrOutputTable.SortOrder, sessionId);
            } else {
                SetTableSortingSpec(outputTableFmrId, {}, {}, sessionId);
            }
            fmrOutputTables.emplace_back(fmrOutputTable);
        }
        return fmrOutputTables;
    }

    TFuture<TFmrOperationResult> DoMerge(TExecContextSimple<TRunOptions>::TPtr& execCtx) {
        TString sessionId = execCtx->GetSessionId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        auto outputTables = execCtx->OutTables_;
        YQL_ENSURE(outputTables.size() == 1, "Merge operation should have exacty one output table");
        auto outputTable = outputTables[0];
        TString outputCluster = execCtx->Cluster_;

        auto outputTableColumnGroups = GetOutputTablesColumnGroups(execCtx);
        TFmrTableId outputTableFmrId(outputCluster, outputTable.Path);
        auto columnGroupSpec = outputTableColumnGroups[0];
        SetColumnGroupSpec(outputTableFmrId, columnGroupSpec, sessionId);

        TFmrTableRef fmrOutputTable{
            .FmrTableId = outputTableFmrId,
            .SerializedColumnGroups = columnGroupSpec
        };
        if (GetIsSorted(outputTable)) {
            for (const auto& inputTable : execCtx->InputTables_) {
                TFmrTableId inputId = GetAliasOrFmrId(TFmrTableId(inputTable.Cluster, inputTable.Name), sessionId);
                const auto inputStatus = GetTablePresenceStatus(inputId, sessionId);
                if (inputStatus == ETablePresenceStatus::OnlyInYt || inputStatus == ETablePresenceStatus::Undefined) {
                    if (execCtx->InputTables_.size() != 1) {
                        YQL_CLOG(INFO, FastMapReduce) << "SortedMerge fallback requested due to non-FMR input table: "
                            << inputId << ", status=" << inputStatus;
                        TFmrOperationResult result;
                        result.Errors.emplace_back(TFmrError{
                            .Component = EFmrComponent::Gateway,
                            .Reason = EFmrErrorReason::FallbackOperation,
                            .ErrorMessage = "SortedMerge has non-FMR inputs, fallback to native gateway"
                        });
                        return MakeFuture(result);
                    }
                    YQL_CLOG(INFO, FastMapReduce) << "SortedMerge with single YT table: " << inputId;
                } else if (inputStatus == ETablePresenceStatus::OnlyInFmr || inputStatus == ETablePresenceStatus::Both) {
                    const auto meta = GetFmrTableMeta(inputId, sessionId);
                    if (!meta.DoesExist) {
                        YQL_CLOG(INFO, FastMapReduce) << "SortedMerge fallback requested due to missing FMR meta for input table: "
                            << inputId << ", status=" << inputStatus;
                        TFmrOperationResult result;
                        result.Errors.emplace_back(TFmrError{
                            .Component = EFmrComponent::Gateway,
                            .Reason = EFmrErrorReason::FallbackOperation,
                            .ErrorMessage = "SortedMerge has inputs without FMR meta, fallback to native gateway"
                        });
                        return MakeFuture(result);
                    }
                }
            }

            fmrOutputTable.SortColumns = GetTableSortedColumns(outputTable);
            fmrOutputTable.SortOrder = GetTableSortedOrders(outputTable);
            SetTableSortingSpec(outputTableFmrId, fmrOutputTable.SortColumns, fmrOutputTable.SortOrder, sessionId);
            return ExecSortedMerge(execCtx->InputTables_, fmrOutputTable, outputCluster, sessionId, execCtx->Options_.Config(), execCtx->Options_.PublicId());
        }
        SetTableSortingSpec(outputTableFmrId, {}, {}, sessionId);
        return ExecMerge(execCtx->InputTables_, fmrOutputTable, outputCluster, sessionId, execCtx->Options_.Config(), execCtx->Options_.PublicId());
    }

    TFuture<TFmrOperationResult> ExecMerge(
        const std::vector<TInputInfo>& inputTables,
        const TFmrTableRef& fmrOutputTable,
        const TString& outputCluster,
        const TString& sessionId,
        TYtSettings::TConstPtr& config,
        const TMaybe<ui32>& publicId = Nothing())
    {
        auto [mergeInputTables, clusterConnections] = GetInputTablesAndConnections(inputTables, sessionId, config);

        TMergeOperationParams mergeOperationParams{.Input = mergeInputTables, .Output = fmrOutputTable};
        TStartOperationRequest mergeOperationRequest{
            .OperationType = EOperationType::Merge,
            .OperationParams = mergeOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries = 1,
            .ClusterConnections = clusterConnections,
            .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
        };

        std::vector<TString> inputPaths;
        std::transform(inputTables.begin(), inputTables.end(), std::back_inserter(inputPaths), [](const auto& table) {
            return table.Cluster + "." + table.Name;}
        );

        auto fmrJobFuture = GetUploadResourcesFuture(sessionId, config, {}, {}, publicId);
        YQL_CLOG(INFO, FastMapReduce) << "Starting merge from tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end()) << " to fmr table " << fmrOutputTable.FmrTableId;
        return fmrJobFuture.Apply([=, this](const auto& fmrJobF) mutable {
            mergeOperationRequest.FmrJob = fmrJobF.GetValue().FmrJob;
            return GetRunningOperationFuture(mergeOperationRequest, sessionId, Nothing(), publicId);
        });
    }

    TFuture<TFmrOperationResult> ExecSortedMerge(
        const std::vector<TInputInfo>& inputTables,
        const TFmrTableRef& fmrOutputTable,
        const TString& outputCluster,
        const TString& sessionId,
        TYtSettings::TConstPtr& config,
        const TMaybe<ui32>& publicId = Nothing())
    {
        auto [mergeInputTables, clusterConnections] = GetInputTablesAndConnections(inputTables, sessionId, config);

        TSortedMergeOperationParams sortedMergeOperationParams{.Input = mergeInputTables, .Output = fmrOutputTable};
        TStartOperationRequest sortedMergeOperationRequest{
            .OperationType = EOperationType::SortedMerge,
            .OperationParams = sortedMergeOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries = 1,
            .ClusterConnections = clusterConnections,
            .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
        };

        std::vector<TString> inputPaths;
        std::transform(inputTables.begin(), inputTables.end(), std::back_inserter(inputPaths), [](const auto& table) {
            return table.Cluster + "." + table.Name;}
        );

        auto fmrJobFuture = GetUploadResourcesFuture(sessionId, config, {}, {}, publicId);
        YQL_CLOG(INFO, FastMapReduce) << "Starting merge from tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end()) << " to fmr table " << fmrOutputTable.FmrTableId;
        return fmrJobFuture.Apply([=, this](const auto& fmrJobF) mutable {
            sortedMergeOperationRequest.FmrJob = fmrJobF.GetValue().FmrJob;
            return GetRunningOperationFuture(sortedMergeOperationRequest, sessionId, Nothing(), publicId);
        });
    }

    void PrepareUserFilesForUpload(
        const TExecContextSimple<TRunOptions>::TPtr& execCtx,
        std::shared_ptr<TFmrUserJob> fmrJob,
        TString& lambdaCode,
        std::vector<TFileInfo>& filesToUpload,
        std::vector<TYtResourceInfo>& ytResources,
        std::vector<TFmrResourceOperationInfo>& fmrResources
    ) {
        if (!Clusters_ || !UrlMapper_) {
            // No gateway config (unit-test setup) — nothing to transform/upload.
            return;
        }

        TString sessionId = execCtx->GetSessionId();

        execCtx->MakeUserFiles();
        auto tmpFiles = MakeIntrusive<TTempFiles>(execCtx->FileStorage_->GetTemp());

        auto client = execCtx->CreateYtClient(execCtx->Options_.Config());

        auto downloader = MakeYtNativeFileDownloader(execCtx->Gateway, sessionId, execCtx->Cluster_, execCtx->Options_.Config(), client, tmpFiles);
        TTransformerFiles transformerFiles;
        {
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),execCtx->FunctionRegistry_->SupportsSizedAllocators());
            alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TMapJobBuilder jobBuilder;
            // TODO - this function is the same for map and reduce, make function with template builder argument instead of method.
            // FMR's worker downloads YT resources as YsonBinary, so build YtTableContent
            // with yson too — skiff would mismatch and read 0 records.
            transformerFiles = jobBuilder.UpdateAndSetMapLambda(alloc, execCtx, downloader, lambdaCode, fmrJob.get(), /*useSkiff*/ false);
        }

        for (auto& fileInfo: transformerFiles.LocalFiles) {
            filesToUpload.emplace_back(TFileInfo{
                .LocalPath = fileInfo.first,
                .Md5Key = fileInfo.second.Hash,
                .Alias = TFsPath(fileInfo.first).GetName() // uniqueId
            });
        }
        for (auto& fileInfo: transformerFiles.DeferredUdfFiles) {
            filesToUpload.emplace_back(TFileInfo{.LocalPath = fileInfo.first, .Md5Key = fileInfo.second.Hash});
        }

        TMaybe<TClusterConnection> remoteFilesClusterConnection;
        for (auto& richPath: transformerFiles.RemoteFiles) {
            if (!remoteFilesClusterConnection) {
                remoteFilesClusterConnection = GetTableClusterConnection(execCtx->Cluster_, sessionId, execCtx->Options_.Config());
            }
            // Remote files all should have the same cluster, and GatewayTransformer clears it from richPaths, so we need to fill it.
            richPath.Cluster(execCtx->Cluster_);

            // Checking in case remotePath is a table which is already inserted in fmr.
            TFmrTableId fmrTableId(richPath);
            auto fmrTablePresenceStatus = GetTablePresenceStatus(fmrTableId, sessionId);
            if (fmrTablePresenceStatus == ETablePresenceStatus::OnlyInFmr || fmrTablePresenceStatus == ETablePresenceStatus::Both) {
                TFmrTableRef fmrTableRef{.FmrTableId = fmrTableId};
                fmrTableRef.SerializedColumnGroups = GetColumnGroupSpec(fmrTableRef.FmrTableId, sessionId);
                if (!richPath.Columns_.Empty()) {
                    std::vector<TString> neededColumns(richPath.Columns_->Parts_.begin(), richPath.Columns_->Parts_.end());
                    fmrTableRef.Columns = neededColumns;
                }

                YQL_ENSURE(richPath.FileName_.Defined()); // uniqueId, filled in transformer.
                fmrResources.emplace_back(TFmrResourceOperationInfo{.FmrTable = fmrTableRef, .Alias = *richPath.FileName_});
                continue;
            }

            // adding remotePath info to list of ytResources to download in jobs.

            TYtResourceInfo ytResourceInfo{.RichPath = richPath};
            ytResourceInfo.YtServerName = remoteFilesClusterConnection->YtServerName;
            if (remoteFilesClusterConnection->Token.Defined()) {
                ytResourceInfo.Token = *remoteFilesClusterConnection->Token;
            }
            ytResources.emplace_back(ytResourceInfo);
        }

        for (auto& fileInfo: filesToUpload) {
            for (auto& [udfModule, udfPrefix]: transformerFiles.JobUdfs) {
                if (fileInfo.Alias.empty() && fileInfo.LocalPath.EndsWith(udfModule.substr(2))) {
                    YQL_CLOG(DEBUG, FastMapReduce) << "Setting file alias " << udfModule << " for udf with path " << fileInfo.LocalPath;
                    fileInfo.Alias = udfModule;
                }
            }
        }

    }

    TFuture<TFmrOperationResult> DoMap(
        TYtMap map,
        const TExecContextSimple<TRunOptions>::TPtr& execCtx,
        TExprContext& ctx
    ) {
        TString sessionId = execCtx->GetSessionId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        const bool ordered = NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Ordered);

        bool forceSingleTask = false;
        if (auto setting = NYql::GetSetting(map.Settings().Ref(), EYtSettingType::JobCount)) {
            if (FromString<ui64>(setting->Child(1)->Content()) == 1) {
                forceSingleTask = true;
            }
        }

        auto fmrOutputTables = GetOutputTables(execCtx);

        auto [mapInputTables, clusterConnections] = GetInputTablesAndConnections(execCtx->InputTables_, sessionId, execCtx->Options_.Config());

        auto mapJob = std::make_shared<TFmrUserJob>();
        TMapJobBuilder mapJobBuilder;

        mapJobBuilder.SetInputType(mapJob.get(), map);
        mapJobBuilder.SetBlockInput(mapJob.get(), map);
        mapJobBuilder.SetBlockOutput(mapJob.get(), map);
        TString mapLambda = mapJobBuilder.SetMapLambdaCode(mapJob.get(), map, execCtx, ctx, false);

        TRemapperMap remapperMap;
        TSet<TString> remapperAllFiles;
        bool useSkiff = false;
        bool forceYsonInputFormat = true;
        mapJobBuilder.SetMapJobParams(mapJob.get(), execCtx,remapperMap, remapperAllFiles, useSkiff, forceYsonInputFormat, false);
        auto mapJobType = ordered ? EFmrJobType::OrderedMap : EFmrJobType::Map;
        mapJob->SetFmrJobType(mapJobType);
        mapJob->SetSettings(TFmrUserJobSettings());

        std::vector<TFileInfo> filesToUpload; // Udfs and local files to upload to dist cache.
        std::vector<TYtResourceInfo> ytResources; // Yt files and small tables which we need to download as files in jobs.
        std::vector<TFmrResourceOperationInfo> fmrResources; // Yt small tables, which are already in fmr and we need to download as files in jobs.

        PrepareUserFilesForUpload(execCtx, mapJob, mapLambda, filesToUpload, ytResources, fmrResources);
        auto uploadResourcesFuture = GetUploadResourcesFuture(sessionId, execCtx->Options_.Config(), std::move(filesToUpload), std::move(ytResources), execCtx->Options_.PublicId());

        return uploadResourcesFuture.Apply([=, this] (const auto& uploadF) mutable {
            auto uploadResult = uploadF.GetValue();
            // serializing job State
            TStringStream jobStateStream;
            mapJob->Save(jobStateStream);

            TMapOperationParams mapOperationParams{.Input = mapInputTables,.Output = fmrOutputTables, .SerializedMapJobState = jobStateStream.Str(), .MapJobType = mapJobType, .ForceSingleTask = forceSingleTask};
            TStartOperationRequest mapOperationRequest{
                .OperationType = EOperationType::Map,
                .OperationParams = mapOperationParams,
                .SessionId = sessionId,
                .IdempotencyKey = GenerateId(),
                .NumRetries = 1,
                .ClusterConnections = clusterConnections,
                .FmrOperationSpec = execCtx->Options_.Config()->FmrOperationSpec.Get(execCtx->Cluster_),
                .Files = std::move(uploadResult.Files),
                .YtResources = std::move(uploadResult.YtResources),
                .FmrResources = fmrResources,
                .FmrJob = uploadResult.FmrJob
            };

            std::vector<TString> inputPaths, outputPaths;
            std::transform(execCtx->InputTables_.begin(), execCtx->InputTables_.end(), std::back_inserter(inputPaths), [](const auto& table) {
                return table.Cluster + "." + table.Name;}
            );
            std::transform(execCtx->OutTables_.begin(), execCtx->OutTables_.end(), std::back_inserter(outputPaths), [execCtx](const auto& table) {
                return execCtx->Cluster_ + "." + table.Path;}
            );

            YQL_CLOG(INFO, FastMapReduce) << "Starting " << (ordered ? "Ordered Map" : "Map")
                << " from yt tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end())
                << " to yt tables: " << JoinRange(' ', outputPaths.begin(), outputPaths.end());
            return GetRunningOperationFuture(mapOperationRequest, sessionId, Nothing(), execCtx->Options_.PublicId()).Apply(
                [this, sessionId, fmrOutputTables](const auto& f) {
                    auto result = f.GetValue();
                    if (result.Errors.empty()) {
                        for (const auto& output : fmrOutputTables) {
                            SetTableSortingSpec(output.FmrTableId, output.SortColumns, output.SortOrder, sessionId);
                        }
                    }
                    return result;
                });
        });
    }

    TFuture<TFmrOperationResult> DoFill(
        TYtFill fill,
        const TExecContextSimple<TRunOptions>::TPtr& execCtx,
        TExprContext& ctx
    ) {
        TString sessionId = execCtx->GetSessionId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        auto fmrOutputTables = GetOutputTables(execCtx);

        auto fillJob = std::make_shared<TFmrUserJob>();

        // Fill has no input tables — skip SetMapJobParams (which calls GetInputSpec and crashes
        // on empty InputTables_). Only set the output spec and common job flags.
        const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);
        fillJob->SetOutSpec(execCtx->GetOutSpec(true, nativeTypeCompat));
        fillJob->SetUseSkiff(false, TMkqlIOSpecs::ESystemField(0));
        fillJob->SetOptLLVM(execCtx->Options_.OptLLVM());
        fillJob->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());
        fillJob->SetRuntimeLogLevel(execCtx->Options_.RuntimeLogLevel());
        fillJob->SetLangVer(execCtx->Options_.LangVer());
        fillJob->SetRuntimeSettings(execCtx->Options_.RuntimeSettings());

        TString fillLambda;
        {
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                execCtx->FunctionRegistry_->SupportsSizedAllocators());
            alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TGatewayLambdaBuilder builder(execCtx->FunctionRegistry_, alloc);
            fillLambda = builder.BuildLambdaWithIO(*execCtx->MkqlCompiler_, fill.Content(), ctx, false);
        }
        fillJob->SetLambdaCode(fillLambda);
        fillJob->SetFmrJobType(EFmrJobType::Map);
        fillJob->SetSettings(TFmrUserJobSettings());

        std::vector<TFileInfo> filesToUpload;
        std::vector<TYtResourceInfo> ytResources;
        std::vector<TFmrResourceOperationInfo> fmrResources;

        PrepareUserFilesForUpload(execCtx, fillJob, fillLambda, filesToUpload, ytResources, fmrResources);
        auto uploadResourcesFuture = GetUploadResourcesFuture(sessionId, execCtx->Options_.Config(), std::move(filesToUpload), std::move(ytResources), execCtx->Options_.PublicId());

        return uploadResourcesFuture.Apply([=, this] (const auto& uploadF) mutable {
            auto uploadResult = uploadF.GetValue();
            TStringStream jobStateStream;
            fillJob->Save(jobStateStream);

            TFillOperationParams fillOperationParams{.Output = fmrOutputTables, .SerializedFillJobState = jobStateStream.Str()};
            TStartOperationRequest fillOperationRequest{
                .OperationType = EOperationType::Fill,
                .OperationParams = fillOperationParams,
                .SessionId = sessionId,
                .IdempotencyKey = GenerateId(),
                .NumRetries = 1,
                .ClusterConnections = {},
                .FmrOperationSpec = execCtx->Options_.Config()->FmrOperationSpec.Get(execCtx->Cluster_),
                .Files = std::move(uploadResult.Files),
                .YtResources = std::move(uploadResult.YtResources),
                .FmrResources = fmrResources,
                .FmrJob = uploadResult.FmrJob
            };

            std::vector<TString> outputPaths;
            std::transform(execCtx->OutTables_.begin(), execCtx->OutTables_.end(), std::back_inserter(outputPaths), [execCtx](const auto& table) {
                return execCtx->Cluster_ + "." + table.Path;
            });

            YQL_CLOG(INFO, FastMapReduce) << "Starting Fill to yt tables: " << JoinRange(' ', outputPaths.begin(), outputPaths.end());
            return GetRunningOperationFuture(fillOperationRequest, sessionId, Nothing(), execCtx->Options_.PublicId()).Apply(
                [this, sessionId, fmrOutputTables](const auto& f) {
                    auto result = f.GetValue();
                    if (result.Errors.empty()) {
                        for (const auto& output : fmrOutputTables) {
                            SetTableSortingSpec(output.FmrTableId, output.SortColumns, output.SortOrder, sessionId);
                        }
                    }
                    return result;
                });
        });
    }

    TFuture<TFmrOperationResult> DoSort(const TExecContextSimple<TRunOptions>::TPtr& execCtx) {
        TString sessionId = execCtx->GetSessionId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        auto [sortInputTables, clusterConnections] = GetInputTablesAndConnections(execCtx->InputTables_, sessionId, execCtx->Options_.Config());

        auto fmrOutputTables = GetOutputTables(execCtx);
        YQL_ENSURE(fmrOutputTables.size() == 1, "Sort operation output should only have one table");
        auto fmrOutputTable = fmrOutputTables[0];
        auto config = execCtx->Options_.Config();

        TSortOperationParams sortOperationParams{.Input = sortInputTables, .Output = fmrOutputTable};
        YQL_CLOG(INFO, FastMapReduce) << "DoSort: output columnGroups=" << (fmrOutputTable.SerializedColumnGroups.empty() ? "(empty)" : fmrOutputTable.SerializedColumnGroups.substr(0, 200));
        for (size_t i = 0; i < sortInputTables.size(); ++i) {
            if (auto* fmrRef = std::get_if<TFmrTableRef>(&sortInputTables[i])) {
                YQL_CLOG(INFO, FastMapReduce) << "DoSort: input[" << i << "]=" << fmrRef->FmrTableId
                    << " columnGroups=" << (fmrRef->SerializedColumnGroups.empty() ? "(empty)" : fmrRef->SerializedColumnGroups.substr(0, 200));
            }
        }
        TStartOperationRequest sortOperationRequest{
            .OperationType = EOperationType::Sort,
            .OperationParams = sortOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries = 1,
            .ClusterConnections = clusterConnections,
            .FmrOperationSpec = config->FmrOperationSpec.Get(execCtx->Cluster_)
        };

        std::vector<TString> inputPaths;
        std::transform(execCtx->InputTables_.begin(), execCtx->InputTables_.end(), std::back_inserter(inputPaths), [](const auto& table) {
            return table.Cluster + "." + table.Name;}
        );

        auto fmrJobFuture = GetUploadResourcesFuture(sessionId, execCtx->Options_.Config(), {}, {}, execCtx->Options_.PublicId());
        YQL_CLOG(INFO, FastMapReduce) << "Starting sort from tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end()) << " to fmr table " << fmrOutputTable.FmrTableId;
        return fmrJobFuture.Apply([=, this](const auto& fmrJobF) mutable {
            sortOperationRequest.FmrJob = fmrJobF.GetValue().FmrJob;
            return GetRunningOperationFuture(sortOperationRequest, sessionId, Nothing(), execCtx->Options_.PublicId());
        });
    }

    TSortingColumns GetSortingColumnsFromColumnPairList(const TVector<std::pair<TString, bool>>& sortColumns) {
        TSortingColumns sortingColumns;
        for (auto& [colName, isAscending]: sortColumns) {
            sortingColumns.Columns.emplace_back(colName);
            ESortOrder sortOrder = isAscending ? ESortOrder::Ascending : ESortOrder::Descending;
            sortingColumns.SortOrders.emplace_back(sortOrder);
        }
        return sortingColumns;
    }

    TFuture<TFmrOperationResult> DoReduce(
        TYtReduce reduce,
        const TExecContextSimple<TRunOptions>::TPtr& execCtx,
        TExprContext& ctx
    ) {
        TString sessionId = execCtx->GetSessionId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        auto reduceBy = NYql::GetSettingAsColumnPairList(reduce.Settings().Ref(), EYtSettingType::ReduceBy);
        auto sortBy = NYql::GetSettingAsColumnPairList(reduce.Settings().Ref(), EYtSettingType::SortBy);
        bool joinReduce = NYql::HasSetting(reduce.Settings().Ref(), EYtSettingType::JoinReduce);
        bool useFirstAsPrimary = NYql::HasSetting(reduce.Settings().Ref(), EYtSettingType::FirstAsPrimary);

        TVector<TString> sortLimitBy = NYql::GetSettingAsColumnList(reduce.Settings().Ref(), EYtSettingType::SortLimitBy);
        TMaybe<ui64> limit = GetLimit(reduce.Settings().Ref());
        if (limit && !sortLimitBy.empty() && *limit > execCtx->Options_.Config()->TopSortMaxLimit.Get().GetOrElse(DEFAULT_TOP_SORT_LIMIT)) {
            limit.Clear();
        }

        auto [reduceInputTables, clusterConnections] = GetInputTablesAndConnections(execCtx->InputTables_, sessionId, execCtx->Options_.Config());
        auto reduceOutputTables = GetOutputTables(execCtx);

        auto reduceJob = std::make_shared<TFmrUserJob>();
        TReduceJobBuilder reduceJobBuilder;

        TVector<ui32> groups;
        TVector<TString> tables;
        TVector<ui64> rowOffsets;
        ui64 currentRowOffset = 0;
        std::vector<NYT::TRichYPath> primaryInputTablesPaths;

        YQL_ENSURE(!execCtx->InputTables_.empty());
        const ui32 primaryGroup = useFirstAsPrimary ? execCtx->InputTables_.front().Group : execCtx->InputTables_.back().Group;
        for (const auto& table : execCtx->InputTables_) {
            if (joinReduce) {
                auto yPath = table.Path;
                if (table.Group == primaryGroup) {
                    primaryInputTablesPaths.emplace_back(yPath);
                }
            }

            if (!groups.empty() && groups.back() != table.Group) {
                currentRowOffset = 0;
            }

            groups.push_back(table.Group);
            tables.push_back(table.Temp ? TString() : table.Name);
            rowOffsets.push_back(currentRowOffset);
            currentRowOffset += table.Records;
        }

        THashSet<TString> auxColumns;
        std::for_each(reduceBy.begin(), reduceBy.end(), [&auxColumns](const auto& it) { auxColumns.insert(it.first); });
        if (!sortBy.empty()) {
            std::for_each(sortBy.begin(), sortBy.end(), [&auxColumns](const auto& it) { auxColumns.insert(it.first); });
        }

        if (sortBy.empty() && !joinReduce) {
            sortBy = reduceBy;
        }

        // handle unsupported reduce types.
        auto fallbackReduceOperationResult = TFmrOperationResult{
            .Errors = {
                TFmrError{
                    .Component = EFmrComponent::Gateway,
                    .Reason = EFmrErrorReason::FallbackOperation,
                }
            }
        };

        if (joinReduce) {
            fallbackReduceOperationResult.Errors[0].ErrorMessage = "Join Reduce is not supported yet, falling back to underlying gateway";
            return MakeFuture(fallbackReduceOperationResult);
        }

        for (auto& table: reduceInputTables) {
            if (std::holds_alternative<TYtTableRef>(table)) {
                auto ytTable = std::get<TYtTableRef>(table);
                fallbackReduceOperationResult.Errors[0].ErrorMessage = TStringBuilder() << "Table " << ytTable.GetPath() << " is not in fmr - falling back to underlying gateway";
                return MakeFuture(fallbackReduceOperationResult);
            }
        }


        TReduceOperationSpec reduceOperationSpec{
            .ReduceBy = GetSortingColumnsFromColumnPairList(reduceBy),
            .SortBy = GetSortingColumnsFromColumnPairList(sortBy),
            .ReduceType = joinReduce ? EReduceType::JoinReduce : EReduceType::SortedReduce
        }; // TODO - add JoinReduceSpec, for now not supported.

        reduceJobBuilder.SetInputType(reduceJob.get(), reduce);
        reduceJobBuilder.SetReduceJobParams(reduceJob.get(), execCtx, groups, tables, rowOffsets, auxColumns);
        TString reduceLambda = reduceJobBuilder.SetReduceLambdaCode(reduceJob.get(), reduce, execCtx, ctx);

        reduceJob->SetSettings(TFmrUserJobSettings());

        std::vector<TFileInfo> filesToUpload; // Udfs and local files to upload to dist cache.
        std::vector<TYtResourceInfo> ytResources; // Yt files and small tables which we need to download as files in jobs.
        std::vector<TFmrResourceOperationInfo> fmrResources; // Yt small tables, which are already in fmr and we need to download as files in jobs.

        PrepareUserFilesForUpload(execCtx, reduceJob, reduceLambda, filesToUpload, ytResources, fmrResources);
        auto uploadResourcesFuture = GetUploadResourcesFuture(sessionId, execCtx->Options_.Config(), std::move(filesToUpload), std::move(ytResources), execCtx->Options_.PublicId());

        return uploadResourcesFuture.Apply([=, this] (const auto& uploadF) mutable {
            auto uploadResult = uploadF.GetValue();
            // serializing job State
            TStringStream jobStateStream;
            reduceJob->Save(jobStateStream);

            TReduceOperationParams reduceOperationParams{
                .Input = reduceInputTables,
                .Output = reduceOutputTables,
                .SerializedReduceJobState = jobStateStream.Str(),
                .ReduceOperationSpec = reduceOperationSpec
            };
            TStartOperationRequest reduceOperationRequest{
                .OperationType = EOperationType::Reduce,
                .OperationParams = reduceOperationParams,
                .SessionId = sessionId,
                .IdempotencyKey = GenerateId(),
                .NumRetries = 1,
                .ClusterConnections = clusterConnections,
                .FmrOperationSpec = execCtx->Options_.Config()->FmrOperationSpec.Get(execCtx->Cluster_),
                .Files = std::move(uploadResult.Files),
                .YtResources = std::move(uploadResult.YtResources),
                .FmrResources = fmrResources,
                .FmrJob = uploadResult.FmrJob
            };

            std::vector<TString> inputPaths, outputPaths;
            std::transform(execCtx->InputTables_.begin(), execCtx->InputTables_.end(), std::back_inserter(inputPaths), [](const auto& table) {
                return table.Cluster + "." + table.Name;}
            );
            std::transform(execCtx->OutTables_.begin(), execCtx->OutTables_.end(), std::back_inserter(outputPaths), [execCtx](const auto& table) {
                return execCtx->Cluster_ + "." + table.Path;}
            );

            YQL_CLOG(INFO, FastMapReduce) << "Starting reduce from yt tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end()) << " to yt tables: " << JoinRange(' ', outputPaths.begin(), outputPaths.end());
            return GetRunningOperationFuture(reduceOperationRequest, sessionId, Nothing(), execCtx->Options_.PublicId());
        });
    }

    void ReportPreparingFmrStage(const TString& sessionId, ui32 publicId) {
        TOperationProgressWriter progressWriter;
        TOperationProgress progress(TString(YtProviderName), publicId, TOperationProgress::EState::InProgress, "FMR Upload artefacts");
        if (FmrServices_->VanillaRemoteId.Defined()) {
            progress.RemoteId = *FmrServices_->VanillaRemoteId;
        }
        with_lock(Mutex_) {
            auto sessionIt = Sessions_.find(sessionId);
            if (sessionIt == Sessions_.end()) {
                return;
            }
            progressWriter = sessionIt->second->ProgressWriter_;
            sessionIt->second->OperationStates.LastProgress.insert_or_assign(publicId, progress);
        }
        if (progressWriter) {
            progressWriter(progress);
        }
    }

    struct TUploadResourcesResult {
        TMaybe<TYtResourceInfo> FmrJob;
        std::vector<TFileInfo> Files;
        std::vector<TYtResourceInfo> YtResources;
    };

    // Upload arbitrary files to YT via the slave gateway. The result preserves input order;
    // callers are responsible for setting RichPath.FileName_ (alias) on each entry.
    NThreading::TFuture<std::vector<TYtResourceInfo>> UploadFilesToYt(
        const TString& sessionId,
        TYtSettings::TConstPtr config,
        TVector<IYtGateway::TFileWithMd5> files)
    {
        YQL_ENSURE(FmrServices_->YtServerForUpload);

        const TString ytServer = *FmrServices_->YtServerForUpload;
        const TString cluster = Clusters_->GetNameByYtName(ytServer);
        YQL_ENSURE(!cluster.empty(), "Cannot find cluster for YT server: " << ytServer);

        auto clusterConnectionOptions = TClusterConnectionOptions(sessionId).Cluster(cluster).Config(config);
        auto clusterConnection = GetClusterConnection(std::move(clusterConnectionOptions));
        const TString token = clusterConnection.Token.GetOrElse(TString{});

        IYtGateway::TUploadFilesToCacheOptions uploadOptions(sessionId);
        uploadOptions.Cluster(cluster).Config(config).Files(std::move(files));

        return Slave_->UploadFilesToCache(std::move(uploadOptions)).Apply(
            [ytServer, token](const NThreading::TFuture<IYtGateway::TUploadFilesToCacheResult>& f) {
                const auto& result = f.GetValue();
                YQL_ENSURE(result.Success(), "Failed to upload files to YT: " << result.Issues().ToString());
                std::vector<TYtResourceInfo> uploaded;
                uploaded.reserve(result.Files.size());
                for (const auto& uf : result.Files) {
                    YQL_ENSURE(uf.RemotePath.Defined(), "UploadFilesToCache did not return a RemotePath");
                    auto path = NYT::TRichYPath(*uf.RemotePath);
                    if (uf.RemoteTx) {
                        path.TransactionId(GetGuid(*uf.RemoteTx));
                    }
                    uploaded.push_back(TYtResourceInfo{
                        .RichPath = path,
                        .YtServerName = ytServer,
                        .Token = token,
                    });
                }
                return uploaded;
            });
    }

    NThreading::TFuture<TUploadResourcesResult> GetUploadResourcesFuture(
        const TString& sessionId,
        TYtSettings::TConstPtr config,
        std::vector<TFileInfo> filesToUpload = {},
        std::vector<TYtResourceInfo> ytResources = {},
        const TMaybe<ui32>& publicId = Nothing()
    ) {
        if (publicId.Defined()) {
            ReportPreparingFmrStage(sessionId, *publicId);
        }

        if (!FmrServices_->YtServerForUpload) {
            // No YT vanilla: binary is unused, user files go to the distributed cache.
            NThreading::TFuture<void> userFilesFuture = (FmrServices_->FileUploadService && !filesToUpload.empty())
                ? UploadFilesToDistributedCache(filesToUpload)
                : NThreading::MakeFuture();
            return userFilesFuture.Apply(
                [files = std::move(filesToUpload), ytRes = std::move(ytResources)](const NThreading::TFuture<void>& f) mutable {
                    f.GetValue();
                    return TUploadResourcesResult{
                        .FmrJob = Nothing(),
                        .Files = std::move(files),
                        .YtResources = std::move(ytRes),
                    };
                });
        }

        // YtServerForUpload set: upload binary and user files in a single call.
        // The binary is re-checked every operation because YT's cache can evict it.
        YQL_ENSURE(!FmrServices_->FmrJobBinaryPath.empty(), "YtServerForUpload is set but fmrjob binary path is unknown");
        YQL_ENSURE(!FmrServices_->FmrJobBinaryMd5.empty(), "YtServerForUpload is set but fmrjob binary MD5 is unknown");

        TVector<IYtGateway::TFileWithMd5> ytFiles;
        ytFiles.reserve(filesToUpload.size() + 1);

        ytFiles.push_back(IYtGateway::TFileWithMd5{.Path = FmrServices_->FmrJobBinaryPath, .Md5 = FmrServices_->FmrJobBinaryMd5});
        for (const auto& f : filesToUpload) {
            ytFiles.push_back(IYtGateway::TFileWithMd5{.Path = f.LocalPath, .Md5 = f.Md5Key});
        }

        return UploadFilesToYt(sessionId, config, std::move(ytFiles)).Apply(
            [filesToUpload = std::move(filesToUpload), ytResources = std::move(ytResources)](const NThreading::TFuture<std::vector<TYtResourceInfo>>& f) mutable {
                auto uploaded = f.GetValue();
                YQL_ENSURE(uploaded.size() == filesToUpload.size() + 1);
                TMaybe<TYtResourceInfo> fmrJob = std::move(uploaded.front());
                for (size_t i = 0; i < filesToUpload.size(); ++i) {
                    auto& res = uploaded[i + 1];
                    res.RichPath.FileName(filesToUpload[i].Alias);
                    ytResources.push_back(std::move(res));
                }
                return TUploadResourcesResult{
                    .FmrJob = std::move(fmrJob),
                    .Files = {},
                    .YtResources = std::move(ytResources),
                };
            });
    }

    TFuture<TFmrOperationResult> GetSuccessfulFmrOperationResult() {
        TFmrOperationResult fmrOperationResult = TFmrOperationResult();
        fmrOperationResult.SetSuccess();
        return MakeFuture(fmrOperationResult);
    }

    template <class TOptions>
    typename TExecContextSimple<TOptions>::TPtr MakeExecCtx(
        TOptions&& options,
        const TString& cluster,
        const TString& sessionId)
    {
        TFmrSession::TPtr session = Sessions_[sessionId];

        auto ctx = MakeIntrusive<TExecContextSimple<TOptions>>(TIntrusivePtr<TFmrYtGateway>(this), FmrServices_, Clusters_, MkqlCompiler_, std::move(options), UrlMapper_, cluster, session);
        return ctx;
    }

    TFuture<void> UploadFileToDistributedCache(const TFileInfo& fileInfo) {
        auto metadataService = FmrServices_->FileMetadataService;
        auto uploadService = FmrServices_->FileUploadService;
        TString fileMd5Hash = fileInfo.Md5Key;

        return metadataService->GetFileUploadStatus(fileMd5Hash).Apply([uploadService, fileMd5Hash, filePath = fileInfo.LocalPath] (const auto& getStatusFuture) {
            bool isFileUploaded = getStatusFuture.GetValue();
            if (isFileUploaded) {
                return MakeFuture();
            }
            return uploadService->UploadObject(fileMd5Hash, filePath);
        });
    }

    TFuture<void> UploadFilesToDistributedCache(const std::vector<TFileInfo>& filesToUpload) {
        for (auto& elem: filesToUpload) {
            YQL_CLOG(DEBUG, FastMapReduce) << " Uploading file with md5 key " << elem.Md5Key << " to dist cache";
        }

        std::vector<TFuture<void>> uploadFileFutures;
        for (auto& fileInfo: filesToUpload) {
            uploadFileFutures.emplace_back(UploadFileToDistributedCache(fileInfo));
        }
        return WaitExceptionOrAll(uploadFileFutures);
    }

private:
    struct TFmrGatewayOperationsState {
        std::unordered_map<TString, TPromise<TFmrOperationResult>> OperationStatuses = {}; // operationId -> promise which we set when operation completes
        std::unordered_map<TString, TString> SortedUploadOperations = {}; // operationId -> distributed write session
        std::unordered_set<TString> PullOperations = {}; // operationIds for Pull operations
        std::unordered_map<TString, TMaybe<ui32>> OperationPublicIds = {}; // operationId -> publicId for progress reporting
        std::unordered_map<ui32, TOperationProgress> LastProgress; // publicId -> progress
    };

    struct TFmrTableInfo {
        ETablePresenceStatus TablePresenceStatus = ETablePresenceStatus::Undefined; // Is table present in yt, fmr or both
        TMaybe<TFmrTableId> AnonymousTableFmrIdAlias = Nothing(); // Path to fmr table corresponding to anonymous table id.
        TString ColumnGroupSpec; // Serialized column group spec for fmr table.
        std::vector<TString> SortColumns; // Sorting columns for fmr table.
        std::vector<ESortOrder> SortOrder; // Sorting order for sorting columns.
        TYtTableStatInfo TableStats;
        TYtTableMetaInfo TableMeta;
        bool NeedsDeferredUpload = false; // Table is marked for deferred upload to YT (anonymous tables in Publish)
        NYT::TNode DeferredUploadSpec; // Spec (YqlRowSpecAttribute) for creating YT table during deferred upload
        NYT::TNode OutputSpec; // Original output spec from the operation that produced this table
    };

    struct TFmrSession: public TSessionBase {
        using TPtr = TIntrusivePtr<TFmrSession>;
        using TSessionBase::TSessionBase;

        TFmrGatewayOperationsState OperationStates; // Info about operations
        std::unordered_map<TFmrTableId, TFmrTableInfo> FmrTables; // Info about tables
    };

    IFmrCoordinator::TPtr Coordinator_;
    std::unordered_map<TString, IWriteDistributedSession::TPtr> DistributedUploadSessions_;
    std::unordered_map<TFmrTableId, TFuture<TFmrOperationResult>> InFlightUploads_;
    THashMap<TString, TFuture<void>> InFlightFileUploads_;
    TMutex Mutex_;
    std::unordered_map<TString, TFmrSession::TPtr> Sessions_;
    std::unordered_map<TString, TJobCounters> InProgressCounters_; // operationId -> latest job counters
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    const TIntrusivePtr<ITimeProvider> TimeProvider_;
    TDuration TimeToSleepBetweenGetOperationRequests_;
    TDuration CoordinatorPingInterval_;
    ui64 MaxDirectPullBytes_;
    ui64 MaxDirectPullRows_;
    std::thread GetOperationStatusesThread_;
    std::thread PingSessionThread_;
    std::atomic<bool> StopFmrGateway_;
    TFmrServices::TPtr FmrServices_;
    TConfigClusters::TPtr Clusters_;
    TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler> MkqlCompiler_;
    IYtJobService::TPtr YtJobService_;
    std::shared_ptr<TYtUrlMapper> UrlMapper_;
};

} // namespace

IYtGateway::TPtr CreateYtFmrGateway(IYtGateway::TPtr slave, IFmrCoordinator::TPtr coordinator, TFmrServices::TPtr fmrServices, const TFmrYtGatewaySettings& settings) {
    return MakeIntrusive<TFmrYtGateway>(std::move(slave), coordinator, fmrServices, settings);
}

} // namespace NYql::NFmr
