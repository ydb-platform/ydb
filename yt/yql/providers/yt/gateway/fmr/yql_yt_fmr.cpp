#include "yql_yt_fmr.h"

#include <thread>

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/gateway/lib/exec_ctx.h>
#include <yt/yql/providers/yt/gateway/lib/yt_attrs.h>
#include <yt/yql/providers/yt/gateway/lib/map_builder.h>
#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
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
#include <yql/essentials/providers/common/provider/yql_provider.h>
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
                        continue;
                    }

                    TCompletedOperation completed;
                    completed.OperationId = op.OperationId;
                    completed.SessionId = op.SessionId;
                    completed.Result.TablesStats = getOperationResult.OutputTablesStats;
                    completed.Result.Errors = getOperationResult.ErrorMessages;
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
                std::vector<std::pair<TPromise<TFmrOperationResult>, TFmrOperationResult>> promisesToResolve;
                with_lock(Mutex_) {
                    for (auto& completed : completedOperations) {
                        if (!Sessions_.contains(completed.SessionId)) {
                            continue;
                        }
                        auto& operationStates = Sessions_[completed.SessionId]->OperationStates;
                        auto& operationStatuses = operationStates.OperationStatuses;
                        if (!operationStatuses.contains(completed.OperationId)) {
                            continue;
                        }
                        if (completed.IsSortedUpload) {
                            operationStates.SortedUploadOperations.erase(completed.OperationId);
                        }
                        promisesToResolve.emplace_back(operationStatuses[completed.OperationId], std::move(completed.Result));
                    }

                    for (const auto& abortOp : operationsToAbort) {
                        if (!Sessions_.contains(abortOp.SessionId)) {
                            continue;
                        }
                        auto& operationStates = Sessions_[abortOp.SessionId]->OperationStates;
                        auto& operationStatuses = operationStates.OperationStatuses;
                        if (operationStatuses.contains(abortOp.OperationId)) {
                            TFmrOperationResult fmrOperationResult{};
                            fmrOperationResult.Errors.emplace_back(TFmrError{
                                .Component = EFmrComponent::Gateway,
                                .Reason = EFmrErrorReason::FallbackOperation,
                                .ErrorMessage = TStringBuilder() << "Distributed upload session ping failed: " << abortOp.PingError
                            });
                            promisesToResolve.emplace_back(operationStatuses[abortOp.OperationId], std::move(fmrOperationResult));
                            DistributedUploadSessions_.erase(abortOp.WriteSessionId);
                        }
                        operationStates.SortedUploadOperations.erase(abortOp.OperationId);
                    }

                    for (auto& [sessionId, sessionInfo]: Sessions_) {
                        auto& operationStatuses = sessionInfo->OperationStates.OperationStatuses;
                        std::erase_if(operationStatuses, [] (const auto& item) {
                            return item.second.IsReady();
                        });
                    }
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

        auto processTableInfoFromAst = [&](TYtTableBaseInfo::TPtr tableInfo) {
            if (tableInfo->Cluster.empty()) {
                tableInfo->Cluster = cluster;
            }
            TString tablePath = GetTransformedPath(sessionId, tableInfo->Name, tmpFolder);
            TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(tableInfo->Cluster, tablePath), sessionId);
            NYT::TNode spec;
            if (tableInfo->RowSpec) {
                spec = FillAttrSpecNode(*tableInfo->RowSpec);
            }
            addFmrTable(tableInfo->Cluster, tablePath, spec, GetColumnGroupSpec(fmrTableId, sessionId));
        };

        VisitExpr(node, [&](const TExprNode::TPtr& exprNode) {
            if (auto maybeContent = TMaybeNode<TYtTableContent>(exprNode)) {
                auto content = maybeContent.Cast();
                if (auto maybeRead = content.Input().Maybe<TYtReadTable>()) {
                    for (auto section : maybeRead.Cast().Input()) {
                        for (auto path : section.Paths()) {
                            processTableInfoFromAst(TYtTableBaseInfo::Parse(path.Table()));
                        }
                    }
                } else if (auto maybeOutput = content.Input().Maybe<TYtOutput>()) {
                    processTableInfoFromAst(TYtTableBaseInfo::Parse(maybeOutput.Cast()));
                }
                return false;
            }
            if (auto maybeRead = TMaybeNode<TCoRight>(exprNode).Input().Maybe<TYtReadTable>()) {
                for (auto section : maybeRead.Cast().Input()) {
                    for (auto path : section.Paths()) {
                        processTableInfoFromAst(TYtTableBaseInfo::Parse(path.Table()));
                    }
                }
                return false;
            }
            return true;
        });

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

        TFuture<TFmrOperationResult> future;
        if (auto op = opBase.Maybe<TYtMerge>()) {
            future = DoMerge(execCtx);
        } else if (auto op = opBase.Maybe<TYtMap>()) {
            future = DoMap(op.Cast(), execCtx, ctx);
        } else if (auto op = opBase.Maybe<TYtSort>()) {
            future = DoSort(execCtx);
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

                    auto deferredSpec = FillAttrSpecNode(inputTablesRowSpec[0]);
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

            auto deferredSpec = FillAttrSpecNode(inputTablesRowSpec[0]);
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
            outputTableInfo.Spec = FillAttrSpecNode(inputTablesRowSpec[i]);
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
            bool discard = options.FillSettings().Discard;

            bool writeRef = NCommon::HasResOrPullOption(pull.Ref(), "ref");
            if (!writeRef) {
                bool autoRef = NCommon::HasResOrPullOption(pull.Ref(), "autoref");
                if (autoRef) {
                    ui64 totalRecordsCount = 0;
                    for (auto& tableInfo: inputTableInfos) {
                        totalRecordsCount += tableInfo->Stat->RecordsCount;
                    }
                    writeRef = (totalRecordsCount <= options.FillSettings().RowsLimitPerWrite);
                }
            }

            for (auto& tableInfo: inputTableInfos) {
                auto config = options.Config();
                TString tmpFolder = GetTablesTmpFolder(*config, tableInfo->Cluster, Sessions_[options.SessionId()]->UseSecureTmp_, Sessions_[options.SessionId()]->OperationOptions_);
                TString tablePath = GetTransformedPath(options.SessionId(), tableInfo->Name, tmpFolder);
                TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(tableInfo->Cluster, tablePath), options.SessionId());
                auto status = GetTablePresenceStatus(fmrTableId, options.SessionId());
                if (status != ETablePresenceStatus::OnlyInFmr) {
                    continue;
                }
                if (!writeRef && discard) {
                    continue;
                }
                TOutputInfo outputTableInfo;
                outputTableInfo.Path = tablePath;
                outputTableInfo.Spec = FillAttrSpecNode(*(tableInfo->RowSpec));
                outputTableInfo.AttrSpec = NYT::TNode::CreateMap();

                outputFmrTablesByCluster[tableInfo->Cluster].emplace_back(outputTableInfo);
            }
        }
        if (!outputFmrTablesByCluster.empty()) {
            return UploadSeveralFmrTablesToYt<TResOrPullResult, TResOrPullOptions>(outputFmrTablesByCluster, std::move(options), nodePos);
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
                    outputTableInfo.Spec = FillAttrSpecNode(*ref.TableInfo->RowSpec);
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
        std::vector<TString> writeSessionIdsToCleanup;
        with_lock(Mutex_) {
            YQL_ENSURE(Sessions_.contains(sessionId));
            auto& operationStates = Sessions_[sessionId]->OperationStates;
            auto& operationStatuses = operationStates.OperationStatuses;
            for (auto& [operationId, promise] : operationStatuses) {
                if (!promise.IsReady()) {
                    YQL_CLOG(WARN, FastMapReduce) << "Resolving pending promise for operation " << operationId << " during session close";
                    TFmrOperationResult fmrOperationResult{};
                    fmrOperationResult.Errors.emplace_back(TFmrError{
                        .Component = EFmrComponent::Gateway,
                        .Reason = EFmrErrorReason::Unknown,
                        .ErrorMessage = TStringBuilder() << "Session " << sessionId << " closed while operation " << operationId << " was still in progress"
                    });
                    promise.SetValue(std::move(fmrOperationResult));
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

        std::vector<TFuture<void>> futures;
        futures.emplace_back(Coordinator_->ClearSession({.SessionId = sessionId}));
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
        const TMaybe<TString>& distributedWriteSession = Nothing())
    {
        auto promise = NewPromise<TFmrOperationResult>();
        auto future = promise.GetFuture();
        YQL_CLOG(INFO, FastMapReduce) << "Starting " << startOperationRequest.OperationType << " operation";
        auto startOperationResponseFuture = Coordinator_->StartOperation(startOperationRequest);

        startOperationResponseFuture.Subscribe([this, promise = std::move(promise), sessionId, distributedWriteSession] (const auto& startOperationFuture) mutable {
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

                if (distributedWriteSession.Defined()) {
                    operationStates.SortedUploadOperations.emplace(operationId, *distributedWriteSession);
                    YQL_CLOG(INFO, FastMapReduce) << "Marked operation " << operationId << " as distributed";
                }
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

        if (outputTable.Spec.HasKey(YqlRowSpecAttribute)) {
            const auto& rowSpec = outputTable.Spec[YqlRowSpecAttribute];
            if (rowSpec.HasKey(RowSpecAttrType) && rowSpec[RowSpecAttrType].IsList()) {
                for (const auto& entry : rowSpec[RowSpecAttrType].AsList()[1].AsList()) {
                    columns.emplace_back(entry[0].AsString());
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
                    sortOrders.emplace_back(item.AsInt64() < 0 ? ESortOrder::Descending : ESortOrder::Ascending);
                }
            }
        }
        return sortOrders;
    }

    NYT::TNode FillAttrSpecNode(const TYqlRowSpecInfo yqlRowSpecInfo) {
        NYT::TNode res = NYT::TNode::CreateMap();
        yqlRowSpecInfo.FillAttrNode(res[YqlRowSpecAttribute], false);
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

        return Coordinator_->PrepareOperation(PrepareOperationRequest).Apply([this, sessionId, outputCluster, clusterConnection, config, SortedUploadOperationParams, originalTableId] (const auto& PrepareOperationFuture) mutable {
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
                return GetRunningOperationFuture(SortedUploadRequest, sessionId, writeSessionId).Apply([this, sessionId, originalTableId] (const TFuture<TFmrOperationResult>& f) {
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

        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_CLOG(INFO, FastMapReduce) << "Starting upload from fmr to yt for table: " << originalTableId;
        return GetRunningOperationFuture(uploadRequest, sessionId).Apply([this, sessionId = std::move(sessionId), originalTableId = std::move(originalTableId)] (const TFuture<TFmrOperationResult>& f) {
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

                if (inputStatus == ETablePresenceStatus::OnlyInFmr || inputStatus == ETablePresenceStatus::Both) {
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
            return ExecSortedMerge(execCtx->InputTables_, fmrOutputTable, outputCluster, sessionId, execCtx->Options_.Config());
        }
        SetTableSortingSpec(outputTableFmrId, {}, {}, sessionId);
        return ExecMerge(execCtx->InputTables_, fmrOutputTable, outputCluster, sessionId, execCtx->Options_.Config());
    }

    TFuture<TFmrOperationResult> ExecMerge(
        const std::vector<TInputInfo>& inputTables,
        const TFmrTableRef& fmrOutputTable,
        const TString& outputCluster,
        const TString& sessionId,
        TYtSettings::TConstPtr& config)
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

        YQL_CLOG(INFO, FastMapReduce) << "Starting merge from tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end()) << " to fmr table " << fmrOutputTable.FmrTableId;
        return GetRunningOperationFuture(mergeOperationRequest, sessionId);
    }

    TFuture<TFmrOperationResult> ExecSortedMerge(
        const std::vector<TInputInfo>& inputTables,
        const TFmrTableRef& fmrOutputTable,
        const TString& outputCluster,
        const TString& sessionId,
        TYtSettings::TConstPtr& config)
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

        YQL_CLOG(INFO, FastMapReduce) << "Starting merge from tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end()) << " to fmr table " << fmrOutputTable.FmrTableId;
        return GetRunningOperationFuture(sortedMergeOperationRequest, sessionId);
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

        auto fmrOutputTables = GetOutputTables(execCtx);

        auto [mapInputTables, clusterConnections] = GetInputTablesAndConnections(execCtx->InputTables_, sessionId, execCtx->Options_.Config());


        auto mapJob = std::make_shared<TFmrUserJob>();
        TMapJobBuilder mapJobBuilder;

        mapJobBuilder.SetInputType(mapJob.get(), map);
        mapJobBuilder.SetBlockInput(mapJob.get(), map);
        mapJobBuilder.SetBlockOutput(mapJob.get(), map);
        TString mapLambda = mapJobBuilder.SetMapLambdaCode(mapJob.get(), map, execCtx, ctx);

        TRemapperMap remapperMap;
        TSet<TString> remapperAllFiles;
        bool useSkiff = false;
        bool forceYsonInputFormat = true;
        mapJobBuilder.SetMapJobParams(mapJob.get(), execCtx,remapperMap, remapperAllFiles, useSkiff, forceYsonInputFormat, false);
        mapJob->SetIsOrdered(ordered);
        mapJob->SetSettings(TFmrUserJobSettings());

        TFuture<void> uploadFilesToDistributedCacheIfNeededFuture;
        std::vector<TFileInfo> filesToUpload; // Udfs and local files to upload to dist cache.
        std::vector<TYtResourceInfo> ytResources; // Yt files and small tables which we need to download as files in jobs.
        std::vector<TFmrResourceOperationInfo> fmrResources; // Yt small tables, which are already in fmr and we need to download as files in jobs.

        if (!FmrServices_->FileUploadService) {
            uploadFilesToDistributedCacheIfNeededFuture = MakeFuture();
        } else {
            YQL_ENSURE(UrlMapper_ && Clusters_);

            execCtx->MakeUserFiles();
            auto tmpFiles = MakeIntrusive<TTempFiles>(execCtx->FileStorage_->GetTemp());

            auto client = execCtx->CreateYtClient(execCtx->Options_.Config());

            auto downloader = MakeYtNativeFileDownloader(execCtx->Gateway, sessionId, execCtx->Cluster_, execCtx->Options_.Config(), client, tmpFiles);
            TTransformerFiles transformerFiles;
            {
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),execCtx->FunctionRegistry_->SupportsSizedAllocators());
                alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                transformerFiles = mapJobBuilder.UpdateAndSetMapLambda(alloc, execCtx, downloader, mapLambda, mapJob.get(), useSkiff);
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

            auto remoteFilesClusterConnection = GetTableClusterConnection(execCtx->Cluster_, sessionId, execCtx->Options_.Config());

            for (auto& richPath: transformerFiles.RemoteFiles) {
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

                if (!richPath.TransactionId_.Defined() && remoteFilesClusterConnection.TransactionId) {
                    richPath.TransactionId(GetGuid(remoteFilesClusterConnection.TransactionId));
                }

                TYtResourceInfo ytResourceInfo{.RichPath = richPath};
                ytResourceInfo.YtServerName = remoteFilesClusterConnection.YtServerName;
                if (remoteFilesClusterConnection.Token.Defined()) {
                    ytResourceInfo.Token = *remoteFilesClusterConnection.Token;
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

            if (!filesToUpload.empty()) {
                YQL_ENSURE(FmrServices_->FileUploadService, "FileUploadService is not configured, but map operation requires uploading "
                    << filesToUpload.size() << " files (UDFs/local files) to distributed cache. "
                    << "Please configure FileRemoteCacheName in FmrConfigurations and FileCacheConfigurations in gateways.conf");
            }

            uploadFilesToDistributedCacheIfNeededFuture = UploadFilesToDistributedCache(filesToUpload);
        }

        return uploadFilesToDistributedCacheIfNeededFuture.Apply([=, this] (const auto& f) mutable {
            f.GetValue();
            // serializing job State
            TStringStream jobStateStream;
            mapJob->Save(jobStateStream);

            TMapOperationParams mapOperationParams{
                .Input = mapInputTables,
                .Output = fmrOutputTables,
                .SerializedMapJobState = jobStateStream.Str(),
                .IsOrdered = ordered
            };
            TStartOperationRequest mapOperationRequest{
                .OperationType = EOperationType::Map,
                .OperationParams = mapOperationParams,
                .SessionId = sessionId,
                .IdempotencyKey = GenerateId(),
                .NumRetries = 1,
                .ClusterConnections = clusterConnections,
                .FmrOperationSpec = execCtx->Options_.Config()->FmrOperationSpec.Get(execCtx->Cluster_),
                .Files = filesToUpload,
                .YtResources = ytResources,
                .FmrResources = fmrResources
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
            return GetRunningOperationFuture(mapOperationRequest, sessionId).Apply(
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

        YQL_CLOG(INFO, FastMapReduce) << "Starting sort from tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end()) << " to fmr table " << fmrOutputTable.FmrTableId;
        return GetRunningOperationFuture(sortOperationRequest, sessionId);
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
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    const TIntrusivePtr<ITimeProvider> TimeProvider_;
    TDuration TimeToSleepBetweenGetOperationRequests_;
    TDuration CoordinatorPingInterval_;
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
