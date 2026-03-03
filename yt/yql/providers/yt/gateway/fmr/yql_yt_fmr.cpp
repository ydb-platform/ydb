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
#include <yt/yql/providers/yt/provider/yql_yt_mkql_compiler.h>

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
    for (auto& fmrResult: fmrOperationResults) {
        if (fmrResult.Repeat()) {
            operationResult.SetRepeat(true);
        }
        for (auto& error: fmrResult.Errors) {
            operationResult.Errors.emplace_back(error);
            if (error.Reason == EFmrErrorReason::RestartQuery) {
                YQL_CLOG(ERROR, FastMapReduce) << " Fmr query finished with error message " << error.ErrorMessage << " - should restart whole query without fmr gateway";
                return TFmrOperationResult{.Errors = {error}};
            } else if (error.Reason == EFmrErrorReason::RestartOperation) {
                operationResult.SetRepeat(true);
            }
        }
    }
    return operationResult;
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
                with_lock(Mutex_) {
                    auto checkOperationStatuses = [this] (std::unordered_map<TString, TPromise<TFmrOperationResult>>& operationStatuses, const TString& sessionId) {
                        for (auto [operationId, promise]: operationStatuses) {
                            YQL_CLOG(TRACE, FastMapReduce) << "Sending get operation request to coordinator with operationId: " << operationId;
                            auto getOperationFuture = Coordinator_->GetOperation({operationId});
                            getOperationFuture.Subscribe([this, operationId, sessionId, &operationStatuses] (const auto& getFuture) {
                                auto getOperationResult = getFuture.GetValueSync();
                                auto getOperationStatus = getOperationResult.Status;
                                auto operationErrorMessages = getOperationResult.ErrorMessages;
                                auto operationOutputTablesStats = getOperationResult.OutputTablesStats;
                                auto operationResultsYson = getOperationResult.OperationResultsYson;
                                with_lock(Mutex_) {
                                    bool operationCompleted = getOperationStatus != EOperationStatus::Accepted && getOperationStatus != EOperationStatus::InProgress;
                                    if (operationCompleted) {
                                        // operation finished, set value in future returned in DoMerge / DoPublish / DoMap
                                        bool hasCompletedSuccessfully = getOperationStatus == EOperationStatus::Completed;
                                        TFmrOperationResult fmrOperationResult{};
                                        fmrOperationResult.TablesStats = operationOutputTablesStats;
                                        fmrOperationResult.Errors = operationErrorMessages;
                                        if (hasCompletedSuccessfully) {
                                            fmrOperationResult.SetSuccess();
                                        }
                                        auto& session = Sessions_[sessionId];
                                        bool isSortedUploadOperation = session->OperationStates.SortedUploadOperations.contains(operationId);

                                        if (isSortedUploadOperation && hasCompletedSuccessfully) {
                                            YQL_CLOG(TRACE, FastMapReduce) << "Finalizing sorted upload operation " << operationId;
                                            FinalizeSortedUploadOperation(operationId, sessionId, operationResultsYson);
                                            session->OperationStates.SortedUploadOperations.erase(operationId);
                                        }
                                        YQL_ENSURE(operationStatuses.contains(operationId));
                                        auto promise = operationStatuses[operationId];
                                        promise.SetValue(fmrOperationResult);
                                        YQL_CLOG(TRACE, FastMapReduce) << "Sending delete operation request to coordinator with operationId: " << operationId;
                                        auto deleteOperationFuture = Coordinator_->DeleteOperation({operationId});
                                        deleteOperationFuture.Subscribe([] (const auto& deleteFuture) {
                                            auto deleteOperationResult = deleteFuture.GetValueSync();
                                            auto deleteOperationStatus = deleteOperationResult.Status;
                                            YQL_ENSURE(deleteOperationStatus == EOperationStatus::Aborted || deleteOperationStatus == EOperationStatus::NotFound);
                                        });
                                    }
                                }
                            });
                        }
                        std::erase_if(operationStatuses, [] (const auto& item) {
                            auto [operationId, promise] = item;
                            return promise.IsReady();
                        });
                    };

                    for (auto& [sessionId, sessionInfo]: Sessions_) {
                        auto& operationStates = sessionInfo->OperationStates;
                        checkOperationStatuses(operationStates.OperationStatuses, sessionId);
                    }
                }

                Sleep(TimeToSleepBetweenGetOperationRequests_);
            }
        };
        GetOperationStatusesThread_ = std::thread(getOperationStatusesFunc);

        auto pingGatewaySessionFunc = [this] {
            YQL_LOG_CTX_ROOT_SCOPE("PingGatewaySession");
            while (!StopFmrGateway_) {
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
                    NThreading::WaitAll(futures).Wait();

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
        } else {
            std::vector<ui64> fmrInputTables; // list of tables to upload from fmr to yt
            std::unordered_map<TString, TVector<TOutputInfo>> outputTablesByCluster;

            for (auto& inputInfo : execCtx->InputTables_) {
                TString outputCluster = inputInfo.Cluster;
                TOutputInfo outputTableInfo;
                outputTableInfo.Path = inputInfo.Name;
                outputTableInfo.Spec = inputInfo.Spec;
                outputTableInfo.AttrSpec = NYT::TNode::CreateMap();

                TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(inputInfo.Cluster, inputInfo.Name), sessionId);

                TString columnGroupSpec = GetColumnGroupSpec(fmrTableId, sessionId);
                if (!columnGroupSpec.empty()) {
                    outputTableInfo.ColumnGroups = NYT::NodeFromYsonString(columnGroupSpec);
                }

                auto status = GetTablePresenceStatus(fmrTableId, sessionId);
                if (status == ETablePresenceStatus::OnlyInFmr) {
                    outputTablesByCluster[outputCluster].emplace_back(outputTableInfo);
                }
            }
            if (!outputTablesByCluster.empty()) {
                return UploadSeveralFmrTablesToYt<TRunResult, TRunOptions>(outputTablesByCluster, std::move(options), nodePos);
            }
            return Slave_->Run(node, ctx, std::move(options));
        }
        return future.Apply([this, pos = nodePos, options = std::move(options), execCtx] (const TFuture<TFmrOperationResult>& f) {
            try {
                auto fmrOperationResult = f.GetValue(); // rethrow error if any
                TRunResult result;
                result.AddIssues(GetIssuesFromFmrErrors(fmrOperationResult.Errors, pos));
                if (fmrOperationResult.Success()) {
                    result.SetSuccess();
                    auto outputTables = execCtx->OutTables_;
                    YQL_ENSURE(fmrOperationResult.TablesStats.size() == outputTables.size());
                    for (size_t i = 0; i < outputTables.size(); ++i) {
                        auto outputTable = outputTables[i];

                        TFmrTableId fmrOutputTableId(execCtx->Cluster_, outputTable.Path);
                        SetTablePresenceStatus(fmrOutputTableId, execCtx->GetSessionId(), ETablePresenceStatus::OnlyInFmr);

                        auto tableStats = fmrOperationResult.TablesStats[i];
                        // setting stats
                        TYtTableStatInfo stats;
                        stats.Id = "fmr_" + fmrOutputTableId.Id;
                        stats.RecordsCount = tableStats.Rows;
                        stats.DataSize = tableStats.DataWeight;
                        stats.ChunkCount = tableStats.Chunks;
                        stats.ModifyTime = TInstant::Now().Seconds();
                        result.OutTableStats.emplace_back(outputTable.Name, MakeIntrusive<TYtTableStatInfo>(stats));
                        SetFmrTableStats(fmrOutputTableId, stats, execCtx->GetSessionId());

                        // setting meta
                        TYtTableMetaInfo meta;
                        meta.DoesExist = true;

                        SetFmrTableMeta(fmrOutputTableId, meta, execCtx->GetSessionId());

                        YQL_CLOG(INFO, FastMapReduce) << "Fmr output table info: RecordsCount = " << result.OutTableStats.back().second->RecordsCount << " DataSize = " << result.OutTableStats.back().second->DataSize << " ChunkCount = " << result.OutTableStats.back().second->ChunkCount;
                    }
                }
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
                std::vector<TFmrError> fmrErrors;

                if (dumpFmrTablesOpResult.Repeat()) {
                    // Setting repeat if we loaded at least one fmr table to yt successfully, so that after loading fmr tables to yt provider will execute the same operation again with underlying gateway.
                    result.SetRepeat(true);
                }

                for (auto& fmrError: dumpFmrTablesOpResult.Errors) {
                    if (fmrError.Reason == EFmrErrorReason::RestartOperation) {
                        result.SetRepeat(true);
                    } else {
                        fmrErrors.emplace_back(fmrError);
                    }
                }
                if (fmrErrors.empty()) {
                    result.SetSuccess();
                }
                result.AddIssues(GetIssuesFromFmrErrors(fmrErrors, pos));

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

    NYT::TRichYPath GetWriteTable(const TString& sessionId, const TString& cluster, const TString& table, const TString& tmpFolder) const final {
        TString realTableName = GetTransformedPath(sessionId, table, tmpFolder);
        realTableName = NYT::AddPathPrefix(realTableName, NYT::TConfig::Get()->Prefix);
        auto richYPath = NYT::TRichYPath(realTableName);
        TYtSettings::TConstPtr config = nullptr;
        auto clusterConnection = GetTableClusterConnection(cluster, sessionId, config);
        richYPath.TransactionId(GetGuid(clusterConnection.TransactionId));
        YQL_CLOG(DEBUG, ProviderYt) << "Write table path: " << SerializeRichPath(richYPath);
        return richYPath;
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

        TString tmpFolder = GetTablesTmpFolder(*config, cluster);
        auto outputTableRichPath = GetWriteTable(sessionId, cluster, outputPath, tmpFolder).Cluster(cluster);
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

            // Logic in Publish for anon tables:
            // If there is only on input and it is in fmr - don't do anything
            // Else - create Merge operation with all inputs and dump result to fmr
            // TODO - handle publish mode.

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

            auto future = ExecMerge(inputTablesInfo, outputTable, cluster, sessionId, config);
            return future.Apply([this, sessionId, fmrOutputTableId, pos = nodePos] (const auto& f) {
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

                auto tmpFolder = GetTablesTmpFolder(*options.Config(), path.Cluster);
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
        TVector<TTableReq> ytPresentTables;
        TVector<IYtGateway::TTableInfoResult::TTableData> fmrTablesInfo;
        std::unordered_set<ui64> fmrTableIndexes; // needed to keep ordering of results the same as in getTableInfoOptions
        ui64 tableIndex = 0;
        for (auto& table: options.Tables()) {
            TFmrTableId fmrTableId(table.Cluster(), table.Table());
            if (table.Anonymous()) {
                TString cluster = table.Cluster(), path = table.Table();
                auto anonTableRichPath = GetWriteTable(options.SessionId(), cluster, path, GetTablesTmpFolder(*options.Config(), cluster)).Cluster(cluster);
                fmrTableId = TFmrTableId(anonTableRichPath);
            }
            fmrTableId = GetAliasOrFmrId(fmrTableId, options.SessionId());
            YQL_CLOG(DEBUG, FastMapReduce) << " Getting table info for table with id " << fmrTableId;

            if (GetTablePresenceStatus(fmrTableId, options.SessionId()) != ETablePresenceStatus::OnlyInFmr) {
                ytPresentTables.emplace_back(table);
            } else {
                IYtGateway::TTableInfoResult::TTableData fmrTableInfo;

                auto meta = GetFmrTableMeta(fmrTableId, options.SessionId());
                YQL_ENSURE(meta.DoesExist);

                fmrTableInfo.Meta = MakeIntrusive<TYtTableMetaInfo>(meta);
                fmrTableInfo.Stat = MakeIntrusive<TYtTableStatInfo>(GetFmrTableStats(fmrTableId, options.SessionId()));
                fmrTableInfo.Stat->Id = table.Table();
                fmrTableInfo.WriteLock = false;
                fmrTablesInfo.emplace_back(fmrTableInfo);
                fmrTableIndexes.emplace(tableIndex);
            }
            ++tableIndex;
        }
        if (ytPresentTables.empty()) {
            TTableInfoResult result;
            result.SetSuccess();
            result.Data = fmrTablesInfo;
            return MakeFuture<TTableInfoResult>(result);
        }
        TGetTableInfoOptions ytTablesOptions = std::move(options);
        ytTablesOptions.Tables() = ytPresentTables;
        return Slave_->GetTableInfo(std::move(ytTablesOptions)).Apply([fmrTablesInfo, tableIndex, fmrTableIndexes] (const auto& f) {
            TTableInfoResult ytTablesInfoResult = f.GetValue();
            TTableInfoResult allTablesResult;
            allTablesResult.SetSuccess();
            ui64 fmrTablePos = 0, ytTablePos = 0;
            for (ui64 i = 0; i < tableIndex; ++i) {
                if (fmrTableIndexes.contains(i)) {
                    // table number i should append fmr table info to total results
                    allTablesResult.Data.emplace_back(fmrTablesInfo[fmrTablePos]);
                    ++fmrTablePos;
                } else {
                    allTablesResult.Data.emplace_back(ytTablesInfoResult.Data[ytTablePos]);
                    ++ytTablePos;
                }
            }
            return MakeFuture<TTableInfoResult>(allTablesResult);
        });
    }

    TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        std::unordered_map<TString, TVector<TOutputInfo>> outputFmrTablesByCluster;
        auto nodePos = ctx.GetPosition(node->Pos());
        auto execCtx = MakeExecCtx(TResOrPullOptions(options), options.UsedCluster(), options.SessionId());
        if (TStringBuf("Pull") == node->Content()) {
            auto pull = NNodes::TPull(node);
            bool writeRef = NCommon::HasResOrPullOption(pull.Ref(), "ref");
            TVector<TYtTableBaseInfo::TPtr> inputTableInfos = GetInputTableInfos(pull.Input());
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
            if (writeRef && !options.FillSettings().Discard) {
                for (auto& tableInfo: inputTableInfos) {
                    TOutputInfo outputTableInfo;
                    auto config = options.Config();
                    TString tmpFolder = GetTablesTmpFolder(*config, tableInfo->Cluster);
                    TString tablePath = GetTransformedPath(options.SessionId(), tableInfo->Name, tmpFolder);
                    TFmrTableId fmrTableId = GetAliasOrFmrId(TFmrTableId(tableInfo->Cluster, tablePath), options.SessionId());
                    auto status = GetTablePresenceStatus(fmrTableId, options.SessionId());
                    if (status != ETablePresenceStatus::OnlyInFmr) {
                        continue;
                    }
                    outputTableInfo.Path = tablePath;
                    outputTableInfo.Spec = FillAttrSpecNode(*(tableInfo->RowSpec), TResOrPullOptions(options), tableInfo->Cluster);
                    outputTableInfo.AttrSpec = NYT::TNode::CreateMap();

                    outputFmrTablesByCluster[tableInfo->Cluster].emplace_back(outputTableInfo);
                }
            }
        }
        if (!outputFmrTablesByCluster.empty()) {
            return UploadSeveralFmrTablesToYt<TResOrPullResult, TResOrPullOptions>(outputFmrTablesByCluster, std::move(options), nodePos);
        }
        return Slave_->ResOrPull(node, ctx, std::move(options));
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
            Sessions_[sessionId] = MakeIntrusive<TFmrSession>(sessionId, options.UserName(), options.RandomProvider(), options.TimeProvider(), options.OperationOptions(), options.ProgressWriter());
        }
        YQL_CLOG(INFO, FastMapReduce) << "Registered session " << sessionId << " with coordinator";

        Slave_->OpenSession(std::move(options));
    }

    TFuture<void> CloseSession(TCloseSessionOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        TString sessionId = options.SessionId();
        with_lock(Mutex_) {
            YQL_ENSURE(Sessions_.contains(sessionId));
            Sessions_.erase(sessionId);
        }

        std::vector<TFuture<void>> futures;
        futures.emplace_back(Coordinator_->ClearSession({.SessionId = sessionId}));
        futures.emplace_back(Slave_->CloseSession(std::move(options)));
        return NThreading::WaitExceptionOrAll(futures);
    }

private:
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

    TString GetColumnGroupSpec(const TFmrTableId& fmrTableId, const TString& sessionId) {
        auto& fmrTableInfo = Sessions_[sessionId]->FmrTables;
        TString columnGroupSpec = fmrTableInfo[fmrTableId].ColumnGroupSpec;
        YQL_CLOG(DEBUG, FastMapReduce) << "Gotten column group spec " << columnGroupSpec << " for table " << fmrTableId;
        return columnGroupSpec;
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
        auto& session = Sessions_[sessionId];
        auto writeSession = session->OperationStates.SortedUploadOperations[operationId];
        DistributedUploadSessions_[writeSession]->Finish(fragmentResultsYson);

        YQL_CLOG(DEBUG, FastMapReduce) << "Successfully finalized distributed write session for operation " << operationId;
    }

    TFuture<TFmrOperationResult> GetRunningOperationFuture(
        const TStartOperationRequest& startOperationRequest,
        const TString& sessionId,
        const TMaybe<TString>& distributedWriteSession = Nothing())
    {
        auto promise = NewPromise<TFmrOperationResult>();
        auto future = promise.GetFuture();
        YQL_CLOG(INFO, FastMapReduce) << "Starting " << startOperationRequest.TaskType << " operation";
        auto startOperationResponseFuture = Coordinator_->StartOperation(startOperationRequest);

        startOperationResponseFuture.Subscribe([this, promise = std::move(promise), sessionId, distributedWriteSession] (const auto& startOperationFuture) {
            TStartOperationResponse startOperationResponse = startOperationFuture.GetValueSync();
            TString operationId = startOperationResponse.OperationId;

            auto& operationStates = Sessions_[sessionId]->OperationStates;
            auto& operationStatuses = operationStates.OperationStatuses;
            YQL_ENSURE(!operationStatuses.contains(operationId));
            operationStatuses[operationId] = promise;

            if (distributedWriteSession.Defined()) {
                operationStates.SortedUploadOperations.emplace(operationId, *distributedWriteSession);
                YQL_CLOG(INFO, FastMapReduce) << "Marked operation " << operationId << " as distributed";
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
        YQL_CLOG(INFO, FastMapReduce) << "Starting " << startOperationRequest.TaskType << " operation";
        auto startOperationResponseFuture = Coordinator_->StartOperation(startOperationRequest);

        startOperationResponseFuture.Subscribe([this, promise = std::move(promise), sessionId, &startOperationRequest] (const auto& startOperationFuture) {
            TStartOperationResponse startOperationResponse = startOperationFuture.GetValueSync();
            TString operationId = startOperationResponse.OperationId;

            auto& operationStates = Sessions_[sessionId]->OperationStates;
            auto& operationStatuses = operationStates.OperationStatuses;
            YQL_ENSURE(!operationStatuses.contains(operationId));
            operationStatuses[operationId] = promise;

            if (startOperationRequest.TaskType == ETaskType::SortedUpload) {
                auto params = std::get<TSortedUploadOperationParams>(startOperationRequest.OperationParams);
                TString session = params.SessionId;
                operationStates.SortedUploadOperations.emplace(operationId, session);
                YQL_CLOG(INFO, FastMapReduce) << "Marked operation " << operationId << " as distributed";
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
        for (auto& out: execCtx->OutTables_) {
            auto curTableColumnGroups = out.ColumnGroups;
            if (curTableColumnGroups.IsUndefined()) {
                columnGroups.emplace_back(TString());
                continue;
            }
            columnGroups.emplace_back(NYT::NodeToYsonString(curTableColumnGroups));
        }
        return columnGroups;
    }

    bool GetIsSorted(const TOutputInfo& outputTable) {
        bool isOrdered = false;
        if (outputTable.Spec.HasKey(YqlRowSpecAttribute)) {
            const auto& rowSpec = outputTable.Spec[YqlRowSpecAttribute];
            if (rowSpec.HasKey("SortedBy") && !rowSpec["SortedBy"].AsList().empty()) {
                isOrdered = true;
            }
        }
        return isOrdered;
    }

    std::vector<TString> GetTableSortedColumns(const TOutputInfo& outputTable) {
        std::vector<TString> columns;

        if (outputTable.Spec.HasKey(YqlRowSpecAttribute)) {
            const auto& rowSpec = outputTable.Spec[YqlRowSpecAttribute];
            if (rowSpec.HasKey("SortedBy") && !rowSpec["SortedBy"].AsList().empty()) {
                for (const auto& item : rowSpec["SortedBy"].AsList()) {
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
            if (rowSpec.HasKey("SortDirections") && !rowSpec["SortDirections"].AsList().empty()) {
                for (const auto& item : rowSpec["SortDirections"].AsList()) {
                    ESortOrder sortOrder = ESortOrder::Ascending;
                    if (item.AsInt64() < 0) {
                        sortOrder = ESortOrder::Descending;
                    }
                    sortOrders.emplace_back(sortOrder);
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
    TFuture<TFmrOperationResult> SortedUploadTableFromFmrToYt(const TExecCtx& execCtx, ui64 outputTableIndex) {
        TString sessionId = execCtx->GetSessionId();
        TYtSettings::TConstPtr& config = execCtx->Options_.Config();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);


        auto& fmrTable = execCtx->OutTables_[outputTableIndex];
        TString outputPath = fmrTable.Path;
        TString outputCluster = execCtx->Cluster_;

        TFmrTableRef fmrTableRef = GetFmrTableRef(TFmrTableId(outputCluster, outputPath), sessionId);
        auto tablePresenceStatus = GetTablePresenceStatus(fmrTableRef.FmrTableId, sessionId);

        if (tablePresenceStatus != ETablePresenceStatus::OnlyInFmr) {
            YQL_CLOG(TRACE, FastMapReduce) << "Table " << fmrTableRef.FmrTableId << " has table presence status " << tablePresenceStatus << " so don't upload from fmr to yt";
            return GetSuccessfulFmrOperationResult();
        }

        fmrTableRef.SerializedColumnGroups = GetColumnGroupSpec(fmrTableRef.FmrTableId, sessionId);

        auto richPath = GetWriteTable(sessionId, outputCluster, outputPath, GetTablesTmpFolder(*config, outputCluster)).Cluster(outputCluster);
        auto filePath = GetTableFilePath(TGetTableFilePathOptions(sessionId).Cluster(outputCluster).Path(outputPath).IsTemp(true));

        fmrTable.FilePath = filePath;
        PrepareDestination(execCtx, outputTableIndex);

        auto clusterConnection = GetTableClusterConnection(outputCluster, sessionId, config);

        TSortedUploadOperationParams SortedUploadOperationParams{
            .Input = fmrTableRef,
            .Output = TYtTableRef(richPath, filePath),
            .IsOrdered = true
        };

        TPrepareOperationRequest PrepareOperationRequest{
            .OperationParams = SortedUploadOperationParams,
            .ClusterConnections = std::unordered_map<TFmrTableId, TClusterConnection>{{fmrTableRef.FmrTableId, clusterConnection}},
            .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
        };

        YQL_CLOG(TRACE, FastMapReduce) << "Creating partition for distributed upload from fmr to yt for table: " << fmrTableRef.FmrTableId;

        return Coordinator_->PrepareOperation(PrepareOperationRequest).Apply([this, &sessionId, &outputCluster, &clusterConnection, &config, &SortedUploadOperationParams] (const auto& PrepareOperationFuture)  {
            try {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                auto PrepareOperationResponse = PrepareOperationFuture.GetValue();
                TString partitionId = PrepareOperationResponse.PartitionId;
                ui64 tasksNum = PrepareOperationResponse.TasksNum;

                YQL_CLOG(DEBUG, FastMapReduce) << "Partition created with id: " << partitionId << ", tasks num: " << tasksNum;
                YQL_CLOG(TRACE, FastMapReduce) << "Creating session for distributed upload from fmr to yt";
                TStartDistributedWriteOptions options;
                auto writeSession = YtJobService_->StartDistributedWriteSession(
                    SortedUploadOperationParams.Output.RichPath,
                    tasksNum,
                    clusterConnection,
                    options
                );

                const TString& writeSessionId = writeSession->GetId();
                DistributedUploadSessions_[writeSessionId] = writeSession;

                YQL_CLOG(TRACE, FastMapReduce) << "Distributed session started!";
                auto cookies = writeSession->GetCookies();
                YQL_CLOG(DEBUG, FastMapReduce) << "Distributed Cookies count: " << cookies.size();

                SortedUploadOperationParams.UpdateAfterPreparation(cookies, partitionId);

                auto fmrTableId = SortedUploadOperationParams.Input.FmrTableId;

                TStartOperationRequest SortedUploadRequest{
                    .TaskType = ETaskType::SortedUpload,
                    .OperationParams = SortedUploadOperationParams,
                    .SessionId = sessionId,
                    .IdempotencyKey = GenerateId(),
                    .NumRetries = 1,
                    .ClusterConnections = std::unordered_map<TFmrTableId, TClusterConnection>{{fmrTableId, clusterConnection}},
                    .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
                };

                YQL_CLOG(TRACE, FastMapReduce) << "Starting SortedUpload from fmr to yt for table: " << fmrTableId;
                return GetRunningOperationFuture(SortedUploadRequest, sessionId, writeSessionId).Apply([this, sessionId, fmrTableId] (const TFuture<TFmrOperationResult>& f) {
                    try {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                        auto fmrUploadResult = f.GetValue();
                        YQL_CLOG(TRACE, FastMapReduce) << "GATEWAY: Distributed upload requested, get running operation feature";
                        SetTablePresenceStatus(fmrTableId, sessionId, ETablePresenceStatus::Both);
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
    TFuture<TFmrOperationResult> UploadTableFromFmrToYt(const TExecCtx& execCtx, ui64 outputTableIndex) {
        TString sessionId = execCtx->GetSessionId();
        TYtSettings::TConstPtr& config = execCtx->Options_.Config();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);

        auto& fmrTable = execCtx->OutTables_[outputTableIndex];
        // Uploading this table to yt with the same cluster and path as in fmr (Dump for unsupported operations)

        TString outputPath = fmrTable.Path;
        TString outputCluster = execCtx->Cluster_;

        TFmrTableRef fmrTableRef = GetFmrTableRef(TFmrTableId(outputCluster, outputPath), sessionId);
        auto tablePresenceStatus = GetTablePresenceStatus(fmrTableRef.FmrTableId, sessionId);

        if (tablePresenceStatus != ETablePresenceStatus::OnlyInFmr) {
            YQL_CLOG(INFO, FastMapReduce) << "Table " << fmrTableRef.FmrTableId << " has table presence status " << tablePresenceStatus << " so don't upload from fmr to yt";
            return GetSuccessfulFmrOperationResult();
        }

        fmrTableRef.SerializedColumnGroups = GetColumnGroupSpec(fmrTableRef.FmrTableId, sessionId);

        auto richPath = GetWriteTable(sessionId, outputCluster, outputPath, GetTablesTmpFolder(*config, outputCluster)).Cluster(outputCluster);

        auto filePath = GetTableFilePath(TGetTableFilePathOptions(sessionId).Cluster(outputCluster).Path(outputPath).IsTemp(true));

        fmrTable.FilePath = filePath;
        PrepareDestination(execCtx, outputTableIndex);

        TUploadOperationParams uploadOperationParams{.Input = fmrTableRef, .Output = TYtTableRef(richPath, filePath)};
        auto clusterConnection = GetTableClusterConnection(outputCluster, sessionId, config);
        TStartOperationRequest uploadRequest{
            .TaskType = ETaskType::Upload,
            .OperationParams = uploadOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries = 1,
            .ClusterConnections = std::unordered_map<TFmrTableId, TClusterConnection>{{fmrTableRef.FmrTableId, clusterConnection}},
            .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
        };

        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        auto fmrTableId = fmrTableRef.FmrTableId;
        YQL_CLOG(INFO, FastMapReduce) << "Starting upload from fmr to yt for table: " << fmrTableId;
        return GetRunningOperationFuture(uploadRequest, sessionId).Apply([this, sessionId = std::move(sessionId), fmrTableId = std::move(fmrTableId)] (const TFuture<TFmrOperationResult>& f) {
            try {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                auto fmrUploadResult = f.GetValue();
                SetTablePresenceStatus(fmrTableId, sessionId, ETablePresenceStatus::Both);
                fmrUploadResult.SetRepeat(true);
                return MakeFuture<TFmrOperationResult>(fmrUploadResult);
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << CurrentExceptionMessage();
                return MakeFuture(ResultFromCurrentException<TFmrOperationResult>());
            }
        });
    }

    template<class TExecCtx>
    TFuture<TFmrOperationResult> DumpFmrTablesToYt(const std::vector<TExecCtx>& execCtxs) {
        std::vector<TFuture<TFmrOperationResult>> uploadFmrTableToYtFutures;
        for (auto& ctx: execCtxs) {
            for (ui64 tableIndex = 0; tableIndex < ctx->OutTables_.size(); ++tableIndex) {
                const auto& outputTable = ctx->OutTables_[tableIndex];
                bool isOrdered = GetIsSorted(outputTable);
                if (isOrdered) {
                    uploadFmrTableToYtFutures.emplace_back(SortedUploadTableFromFmrToYt(ctx, tableIndex));
                } else {
                    uploadFmrTableToYtFutures.emplace_back(UploadTableFromFmrToYt(ctx, tableIndex));
                }
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

        NYT::TNode attrs = NYT::TNode::CreateMap();
        attrs[YqlRowSpecAttribute] = outputTable.Spec[YqlRowSpecAttribute];
        PrepareAttributes(attrs, outputTable, execCtx, outputCluster, true, {});
        YtJobService_->Create(TYtTableRef(outputCluster, outputPath, filePath), clusterConnection, attrs);
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
            .TaskType = ETaskType::Merge,
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

        std::vector<TString> ytInputTables;
        std::vector<TString> unsortedInputTables;
        std::vector<TString> incompatibleSortedInputTables;
        for (const auto& inputTable : mergeInputTables) {
            if (auto ytInput = std::get_if<TYtTableRef>(&inputTable)) {
                ytInputTables.emplace_back(ytInput->GetCluster() + "." + ytInput->GetPath());
                continue;
            }

            const auto& fmrInput = std::get<TFmrTableRef>(inputTable);
            const bool hasSortingSpec = !fmrInput.SortColumns.empty() && !fmrInput.SortOrder.empty();
            if (!hasSortingSpec) {
                unsortedInputTables.emplace_back(fmrInput.FmrTableId.Id);
                continue;
            }

            if (fmrInput.SortColumns != fmrOutputTable.SortColumns || fmrInput.SortOrder != fmrOutputTable.SortOrder) {
                incompatibleSortedInputTables.emplace_back(fmrInput.FmrTableId.Id);
            }
        }

        if (!ytInputTables.empty()) {
            TFmrOperationResult result;
            result.Errors.emplace_back(TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::RestartQuery,
                .ErrorMessage = "SortedMerge supports only inputs present in FMR. Inputs resolved to YT tables: " + JoinRange(", ", ytInputTables.begin(), ytInputTables.end())
            });
            return MakeFuture(result);
        }

        if (!unsortedInputTables.empty()) {
            TFmrOperationResult result;
            result.Errors.emplace_back(TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::RestartQuery,
                .ErrorMessage = "SortedMerge requires sorted FMR inputs. Missing sorting metadata for tables: " + JoinRange(", ", unsortedInputTables.begin(), unsortedInputTables.end())
            });
            return MakeFuture(result);
        }

        if (!incompatibleSortedInputTables.empty()) {
            TFmrOperationResult result;
            result.Errors.emplace_back(TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::RestartQuery,
                .ErrorMessage = "SortedMerge input sorting spec is incompatible with output sorting spec for tables: " + JoinRange(", ", incompatibleSortedInputTables.begin(), incompatibleSortedInputTables.end())
            });
            return MakeFuture(result);
        }

        TSortedMergeOperationParams sortedMergeOperationParams{.Input = mergeInputTables, .Output = fmrOutputTable};
        TStartOperationRequest sortedMergeOperationRequest{
            .TaskType = ETaskType::SortedMerge,
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

        std::vector<TFmrTableRef> fmrOutputTables;
        auto outputTableColumnGroups = GetOutputTablesColumnGroups(execCtx);
        auto outputTables = execCtx->OutTables_;
        YQL_ENSURE(outputTables.size() == outputTableColumnGroups.size());

        for (ui64 i = 0; i < outputTables.size(); ++i) {
            auto& outputTable = outputTables[i];
            TFmrTableId outputTableFmrId(execCtx->Cluster_, outputTable.Path);
            auto columnGroupSpec = outputTableColumnGroups[i];
            SetColumnGroupSpec(outputTableFmrId, columnGroupSpec, sessionId);

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
            // For now, logic for file gateway is not implemented yet, and fileUpload service is not set.
            // TODO (@cdzyura171) - all udfs should be executed locally, use TFileLabmdaBuilder and file transformer.
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
                transformerFiles = mapJobBuilder.UpdateAndSetMapLambda(alloc, execCtx, downloader, mapLambda, mapJob.get());
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

            uploadFilesToDistributedCacheIfNeededFuture = UploadFilesToDistributedCache(filesToUpload);
        }

        return uploadFilesToDistributedCacheIfNeededFuture.Apply([=, this] (const auto& f) mutable {
            f.GetValue();
            // serializing job State
            TStringStream jobStateStream;
            mapJob->Save(jobStateStream);

            TMapOperationParams mapOperationParams{.Input = mapInputTables,.Output = fmrOutputTables, .SerializedMapJobState = jobStateStream.Str(), .IsOrdered = ordered};
            TStartOperationRequest mapOperationRequest{
                .TaskType = ETaskType::Map,
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

            YQL_CLOG(INFO, FastMapReduce) << "Starting map from yt tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end()) << " to yt tables: " << JoinRange(' ', outputPaths.begin(), outputPaths.end());
            return GetRunningOperationFuture(mapOperationRequest, sessionId);
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
    };

    struct TFmrTableInfo {
        ETablePresenceStatus TablePresenceStatus = ETablePresenceStatus::Undefined; // Is table present in yt, fmr or both
        TMaybe<TFmrTableId> AnonymousTableFmrIdAlias = Nothing(); // Path to fmr table corresponding to anonymous table id.
        TString ColumnGroupSpec; // Serialized column group spec for fmr table.
        std::vector<TString> SortColumns; // Sorting columns for fmr table.
        std::vector<ESortOrder> SortOrder; // Sorting order for sorting columns.
        TYtTableStatInfo TableStats;
        TYtTableMetaInfo TableMeta;
    };

    struct TFmrSession: public TSessionBase {
        using TPtr = TIntrusivePtr<TFmrSession>;
        using TSessionBase::TSessionBase;

        TFmrGatewayOperationsState OperationStates; // Info about operations
        std::unordered_map<TFmrTableId, TFmrTableInfo> FmrTables; // Info about tables
    };

    IFmrCoordinator::TPtr Coordinator_;
    std::unordered_map<TString, IWriteDistributedSession::TPtr> DistributedUploadSessions_;
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
