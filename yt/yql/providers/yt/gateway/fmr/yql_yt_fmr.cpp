#include "yql_yt_fmr.h"

#include <thread>

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/lib/schema/schema.h>
#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>

#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/profile.h>

#include <util/generic/ptr.h>
#include <util/string/split.h>
#include <util/thread/pool.h>

using namespace NThreading;
using namespace NYql::NNodes;

namespace NYql::NFmr {

enum class ETablePresenceStatus {
    OnlyInYt,
    OnlyInFmr,
    Both
};

namespace {

TIssue ToIssue(const TFmrError& error, const TPosition& pos){
    return TIssue(pos, error.ErrorMessage);
};

struct TFmrOperationResult: public NCommon::TOperationResult {
    std::vector<TFmrError> Errors = {};
    std::vector<TTableStats> TablesStats = {};
};

class TFmrYtGateway final: public TYtForwardingGatewayBase {
public:
    TFmrYtGateway(IYtGateway::TPtr&& slave, IFmrCoordinator::TPtr coordinator, const TFmrYtGatewaySettings& settings)
        : TYtForwardingGatewayBase(std::move(slave)),
        Coordinator_(coordinator),
        SessionStates_(std::make_shared<TSession>(TSession())),
        RandomProvider_(settings.RandomProvider),
        TimeToSleepBetweenGetOperationRequests_(settings.TimeToSleepBetweenGetOperationRequests)
    {
        auto getOperationStatusesFunc = [&] {
            while (!StopFmrGateway_) {
                with_lock(SessionStates_->Mutex) {
                    auto checkOperationStatuses = [&] (std::unordered_map<TFmrTableId, TPromise<TFmrOperationResult>>& operationStatuses, const TString& sessionId) {
                        for (auto& [operationId, promise]: operationStatuses) {
                            YQL_CLOG(TRACE, FastMapReduce) << "Sending get operation request to coordinator with operationId: " << operationId;

                            auto getOperationFuture = Coordinator_->GetOperation({operationId.Id});
                            getOperationFuture.Subscribe([&, operationId, sessionId] (const auto& getFuture) {
                                auto getOperationResult = getFuture.GetValueSync();
                                auto getOperationStatus = getOperationResult.Status;
                                auto operationErrorMessages = getOperationResult.ErrorMessages;
                                auto operationOutputTablesStats = getOperationResult.OutputTablesStats;
                                with_lock(SessionStates_->Mutex) {
                                    bool operationCompleted = getOperationStatus != EOperationStatus::Accepted && getOperationStatus != EOperationStatus::InProgress;
                                    if (operationCompleted) {
                                        // operation finished, set value in future returned in DoMerge / DoUpload
                                        bool hasCompletedSuccessfully = getOperationStatus == EOperationStatus::Completed;
                                        TFmrOperationResult fmrOperationResult{};
                                        fmrOperationResult.Errors = operationErrorMessages;
                                        if (hasCompletedSuccessfully) {
                                            fmrOperationResult.TablesStats = operationOutputTablesStats;
                                            fmrOperationResult.SetSuccess();
                                        }
                                        promise.SetValue(fmrOperationResult);
                                        YQL_CLOG(INFO, FastMapReduce) << "Sending delete operation request to coordinator with operationId: " << operationId;
                                        auto deleteOperationFuture = Coordinator_->DeleteOperation({operationId.Id});
                                        deleteOperationFuture.Subscribe([&, sessionId, operationId] (const auto& deleteFuture) {
                                            auto deleteOperationResult = deleteFuture.GetValueSync();
                                            auto deleteOperationStatus = deleteOperationResult.Status;
                                            YQL_ENSURE(deleteOperationStatus == EOperationStatus::Aborted || deleteOperationStatus == EOperationStatus::NotFound);
                                            with_lock(SessionStates_->Mutex) {
                                                YQL_ENSURE( SessionStates_->Sessions.contains(sessionId));
                                                auto& sessionInfo = SessionStates_->Sessions[sessionId];
                                                auto& operationStates = sessionInfo.OperationStates;
                                                operationStates.OperationStatuses.erase(operationId);
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    };

                    for (auto [sessionId, sessionInfo]: SessionStates_->Sessions) {
                        auto& operationStates = sessionInfo.OperationStates;
                        checkOperationStatuses(operationStates.OperationStatuses, sessionId);
                    }
                }
                Sleep(TimeToSleepBetweenGetOperationRequests_);
            }
        };
        GetOperationStatusesThread_ = std::thread(getOperationStatusesFunc);
    }

    ~TFmrYtGateway() {
        StopFmrGateway_ = true;
        GetOperationStatusesThread_.join();
    }

    TFuture<TRunResult> Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        auto nodePos = ctx.GetPosition(node->Pos());
        TYtOpBase opBase(node);
        TString sessionId = options.SessionId();

        TFuture<TFmrOperationResult> future;
        std::vector<std::pair<TYtTableRef, bool>> inputTables = GetInputTables(opBase);
        std::vector<TYtTableRef> outputTables = GetOutputTables(opBase);

        if (auto op = opBase.Maybe<TYtMerge>()) {
            future = DoMerge(inputTables, outputTables, std::move(options));
        // Тут у нас пока падает один тест который вызывает фильтрацию, которая использует map,
        // поэтому пока не сделаем функционал фильтрации через map не будем его вообще вызывать отсюда
        /*} else if (auto op = opBase.Maybe<TYtMap>()) {
            future = DoMap(inputTables, outputTables, std::move(options));*/
        } else {
            return Slave_->Run(node, ctx, std::move(options));
        }
        return future.Apply([this, pos = nodePos, outputTables = std::move(outputTables), options = std::move(options)] (const TFuture<TFmrOperationResult>& f) {
            try {
                auto fmrOperationResult = f.GetValue(); // rethrow error if any
                TString sessionId = options.SessionId();
                auto config = options.Config();
                TRunResult result;
                YQL_ENSURE(fmrOperationResult.TablesStats.size() == outputTables.size());
                for (size_t i = 0; i < outputTables.size(); ++i) {
                    auto outputTable = outputTables[i];
                    TFmrTableId fmrOutputTableId = {outputTable.Cluster, outputTable.Path};
                    SetTablePresenceStatus(fmrOutputTableId, sessionId, ETablePresenceStatus::OnlyInFmr);
                    auto tableStats = fmrOperationResult.TablesStats[i];
                    result.OutTableStats.emplace_back(outputTable.Path, MakeIntrusive<TYtTableStatInfo>());
                    result.OutTableStats.back().second->Id = "fmr_" + fmrOutputTableId.Id;
                    result.OutTableStats.back().second->RecordsCount = tableStats.Rows;
                    result.OutTableStats.back().second->DataSize = tableStats.DataWeight;
                    result.OutTableStats.back().second->ChunkCount = tableStats.Chunks;
                    YQL_CLOG(INFO, FastMapReduce) << "Fmr output table info: RecordsCount = " << result.OutTableStats.back().second->RecordsCount << " DataSize = " << result.OutTableStats.back().second->DataSize << " ChunkCount = " << result.OutTableStats.back().second->ChunkCount;
                }
                auto operationErrors = fmrOperationResult.Errors;
                TVector<TIssue> issues;
                for (const auto& error : operationErrors) {
                    issues.emplace_back(ToIssue(error, pos));
                }
                result.AddIssues(issues);
                if (fmrOperationResult.Success()) {
                    result.SetSuccess();
                }
                return MakeFuture<TRunResult>(std::move(result));
            } catch (...) {
                return MakeFuture(ResultFromCurrentException<TRunResult>(pos));
            }
        });
    }

    TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) final {
        TString sessionId = options.SessionId();
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        auto nodePos = ctx.GetPosition(node->Pos());
        auto publish = TYtPublish(node);

        auto cluster = publish.DataSink().Cluster().StringValue();
        std::vector<TFmrTableRef> fmrTableIds;
        auto config = options.Config();

        std::vector<TFuture<TFmrOperationResult>> uploadFmrTablesToYtFutures;
        auto outputPath = publish.Publish().Name().StringValue();

        bool isAnonymous = NYql::HasSetting(publish.Publish().Settings().Ref(), EYtSettingType::Anonymous);
        std::vector<TFmrTableId> currentAnonymousTableAliases;

        for (auto out: publish.Input()) {
            TString inputCluster = GetOutTableWithCluster(out).second;
            auto outTable = GetOutTable(out).Cast<TYtOutTable>();
            TString inputPath = ToString(outTable.Name().Value());
            if (isAnonymous) {
                currentAnonymousTableAliases.emplace_back(TFmrTableId(inputCluster, inputPath));
            }
            auto outputBase = out.Operation().Cast<TYtOutputOpBase>().Ptr();
            uploadFmrTablesToYtFutures.emplace_back(DoUpload(inputCluster, TString(inputPath), sessionId, config, outputBase, ctx));
        }

        if (isAnonymous) {
            YQL_CLOG(DEBUG, FastMapReduce) << "Table " << outputPath << " is anonymous, not uploading from fmr to yt";
            TFmrTableId fmrOutputTableId = {cluster, outputPath};
            SetTablePresenceStatus(fmrOutputTableId, sessionId, ETablePresenceStatus::OnlyInFmr);

            // TODO - figure out what to do here in case of multiple inputs
            SetFmrIdAlias(fmrOutputTableId, currentAnonymousTableAliases[0], sessionId);
            return Slave_->Publish(node, ctx, std::move(options));
        }

        auto idempotencyKey = GenerateId();

        return WaitExceptionOrAll(uploadFmrTablesToYtFutures).Apply([&, pos = nodePos, curNode = std::move(node), options = std::move(options)] (const TFuture<void>& f) mutable {
            try {
                f.GetValue(); // rethrow error if any
                return Slave_->Publish(curNode, ctx, std::move(options));
            } catch (...) {
                return MakeFuture(ResultFromCurrentException<TPublishResult>(pos));
            }
        });
    }

    void OpenSession(TOpenSessionOptions&& options) final {
        TString sessionId = options.SessionId();
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        with_lock(SessionStates_->Mutex) {
            auto& sessions = SessionStates_->Sessions;
            if (sessions.contains(sessionId)) {
                YQL_LOG_CTX_THROW yexception() << "Session already exists: " << sessionId;
            }
            sessions[sessionId] = TSessionInfo{.UserName = options.UserName()};
        }
        Slave_->OpenSession(std::move(options));
    }

    TFuture<void> CloseSession(TCloseSessionOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        TString sessionId = options.SessionId();
        with_lock(SessionStates_->Mutex) {
            auto& sessions = SessionStates_->Sessions;
            YQL_ENSURE(sessions.contains(sessionId));
            sessions.erase(sessionId);
        }
        std::vector<TFuture<void>> futures;
        futures.emplace_back(Coordinator_->ClearSession({.SessionId = sessionId}));
        futures.emplace_back(Slave_->CloseSession(std::move(options)));
        return NThreading::WaitExceptionOrAll(futures);
    }

private:
    TString GenerateId() {
        return GetGuidAsString(RandomProvider_->GenGuid());
    }

    TString GetRealTablePath(const TString& sessionId, const TString& cluster, const TString& path, TYtSettings::TConstPtr& config) {
        auto richPath = Slave_->GetWriteTable(sessionId, cluster, path, GetTablesTmpFolder(*config, cluster));
        return richPath.Path_;
    }

    void SetTablePresenceStatus(const TFmrTableId& fmrTableId, const TString& sessionId, ETablePresenceStatus newStatus) {
        with_lock(SessionStates_->Mutex) {
            YQL_CLOG(DEBUG, FastMapReduce) << "Setting table presence status " << newStatus << " for table with id " << fmrTableId;
            auto& tablePresenceStatuses = SessionStates_->Sessions[sessionId].TablePresenceStatuses;
            tablePresenceStatuses[fmrTableId] = newStatus;
        }
    }

    void SetFmrIdAlias(const TFmrTableId& fmrTableId, const TFmrTableId& alias, const TString& sessionId) {
        with_lock(SessionStates_->Mutex) {
            YQL_CLOG(DEBUG, FastMapReduce) << "Setting table fmr id alias " << alias << " for table with id " << fmrTableId;
            auto& fmrIdAliases = SessionStates_->Sessions[sessionId].FmrIdAliases;
            fmrIdAliases[fmrTableId] = alias;
        }
    }

    TFmrTableId GetFmrIdOrAlias(const TFmrTableId& fmrTableId, const TString& sessionId) {
        with_lock(SessionStates_->Mutex) {
            auto& fmrIdAliases = SessionStates_->Sessions[sessionId].FmrIdAliases;
            if (!fmrIdAliases.contains(fmrTableId)) {
                return fmrTableId;
            }
            return fmrIdAliases[fmrTableId];
        }
    }

    TMaybe<ETablePresenceStatus> GetTablePresenceStatus(const TFmrTableId& fmrTableId, const TString& sessionId) {
        with_lock(SessionStates_->Mutex) {
            auto& tablePresenceStatuses = SessionStates_->Sessions[sessionId].TablePresenceStatuses;
            if (!tablePresenceStatuses.contains(fmrTableId)) {
                return Nothing();
            }
            return tablePresenceStatuses[fmrTableId];
        }
    }

    std::vector<std::pair<TYtTableRef, bool>> GetInputTables(const TYtOpBase& op) {
        auto input = op.Maybe<TYtTransientOpBase>().Cast().Input();
        std::vector<std::pair<TYtTableRef, bool>> inputTables;
        for (auto section: input.Cast<TYtSectionList>()) {
            for (auto path: section.Paths()) {
                TYtPathInfo pathInfo(path);
                TYtTableRef ytTable{.Path = pathInfo.Table->Name, .Cluster = pathInfo.Table->Cluster};
                inputTables.emplace_back(ytTable, pathInfo.Table->IsTemp);
            }
        }
        return inputTables;
    }

    std::vector<TYtTableRef> GetOutputTables(const TYtOpBase& op) {
        auto output = op.Maybe<TYtOutputOpBase>().Cast().Output();
        std::vector<TYtTableRef> outputTables;
        for (auto table: output) {
            TYtOutTableInfo tableInfo(table);
            TString outTableName = tableInfo.Name;
            if (outTableName.empty()) {
                outTableName = TStringBuilder() << "tmp/" << GetGuidAsString(RandomProvider_->GenGuid());
            }
            outputTables.emplace_back(outTableName, tableInfo.Cluster);
        }
        return outputTables;
    }

    TClusterConnection GetTableClusterConnection(const TString& cluster, const TString& sessionId, TYtSettings::TConstPtr& config) {
        auto clusterConnectionOptions = TClusterConnectionOptions(sessionId).Cluster(cluster).Config(config);
        auto clusterConnection = GetClusterConnection(std::move(clusterConnectionOptions));
        return TClusterConnection{
            .TransactionId = clusterConnection.TransactionId,
            .YtServerName = clusterConnection.YtServerName,
            .Token = clusterConnection.Token
        };
    }

    TFuture<TFmrOperationResult> GetRunningOperationFuture(const TStartOperationRequest& startOperationRequest, const TString& sessionId) {
        auto promise = NewPromise<TFmrOperationResult>();
        auto future = promise.GetFuture();
        YQL_CLOG(INFO, FastMapReduce) << "Starting " << startOperationRequest.TaskType << " operation";
        auto startOperationResponseFuture = Coordinator_->StartOperation(startOperationRequest);
        startOperationResponseFuture.Subscribe([this, promise = std::move(promise), sessionId] (const auto& mergeFuture) {
            TStartOperationResponse mergeOperationResponse = mergeFuture.GetValueSync();
            TString operationId = mergeOperationResponse.OperationId;
            with_lock(SessionStates_->Mutex) {
                auto& operationStates = SessionStates_->Sessions[sessionId].OperationStates;
                auto& operationStatuses = operationStates.OperationStatuses;
                YQL_ENSURE(!operationStatuses.contains(operationId));
                operationStatuses[operationId] = promise;
            }
        });
        return future;
    }

    std::pair<std::vector<TOperationTableRef>, std::unordered_map<TFmrTableId, TClusterConnection>> GetInputTablesAndConnections(const std::vector<std::pair<TYtTableRef, bool>>& inputTables, TRunOptions&& options) {
        TString sessionId = options.SessionId();
        std::vector<TOperationTableRef> operationInputTables;
        std::unordered_map<TFmrTableId, TClusterConnection> clusterConnections;
        for (auto [ytTable, isTemp]: inputTables) {
            TString inputCluster = ytTable.Cluster, inputPath = ytTable.Path;
            TFmrTableId fmrTableId = {inputCluster, inputPath};
            auto tablePresenceStatus = GetTablePresenceStatus(fmrTableId, sessionId);
            if (!tablePresenceStatus) {
                SetTablePresenceStatus(fmrTableId, sessionId, ETablePresenceStatus::OnlyInYt);
            }

            if (tablePresenceStatus && *tablePresenceStatus != ETablePresenceStatus::OnlyInYt) {
                // table is in fmr, do not download
                operationInputTables.emplace_back(TFmrTableRef(GetFmrIdOrAlias(fmrTableId, sessionId)));
            } else {
                ytTable.FilePath = GetTableFilePath(TGetTableFilePathOptions(sessionId).Cluster(inputCluster).Path(inputPath).IsTemp(isTemp));
                operationInputTables.emplace_back(ytTable);
                clusterConnections.emplace(fmrTableId, GetTableClusterConnection(ytTable.Cluster, sessionId, options.Config()));
            }
        }
        return {operationInputTables, clusterConnections};
    }

    TFuture<TFmrOperationResult> DoUpload(const TString& outputCluster, const TString& outputPath, const TString& sessionId, TYtSettings::TConstPtr& config, TExprNode::TPtr outputOpBase, TExprContext& ctx) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        TFmrTableRef fmrTableRef{TFmrTableId(outputCluster, outputPath)};
        auto tablePresenceStatus = GetTablePresenceStatus(fmrTableRef.FmrTableId, sessionId);

        if (!tablePresenceStatus || *tablePresenceStatus != ETablePresenceStatus::OnlyInFmr) {
            YQL_CLOG(INFO, FastMapReduce) << " We assume table " << fmrTableRef.FmrTableId << " should be present in yt, not uploading from fmr";
            return GetSuccessfulFmrOperationResult();
        }

        TString realPath = GetRealTablePath(sessionId, outputCluster, outputPath, config);
        TYtTableRef outputTable{.Path = realPath, .Cluster = outputCluster};
        outputTable.FilePath = GetTableFilePath(TGetTableFilePathOptions(sessionId).Cluster(outputCluster).Path(outputPath).IsTemp(true));

        TUploadOperationParams uploadOperationParams{.Input = fmrTableRef, .Output = outputTable};

        auto clusterConnection = GetTableClusterConnection(outputCluster, sessionId, config);
        TStartOperationRequest uploadRequest{
            .TaskType = ETaskType::Upload,
            .OperationParams = uploadOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries=1,
            .ClusterConnections = std::unordered_map<TFmrTableId, TClusterConnection>{{fmrTableRef.FmrTableId, clusterConnection}},
            .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
        };

        auto prepareOptions = TPrepareOptions(sessionId)
                .Config(config);
        auto prepareFuture = Slave_->Prepare(outputOpBase, ctx, std::move(prepareOptions));

        return prepareFuture.Apply([this, uploadRequest = std::move(uploadRequest), sessionId = std::move(sessionId), fmrTableId = std::move(fmrTableRef.FmrTableId)] (const TFuture<TRunResult>& f) mutable {
            try {
                f.GetValue();
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                YQL_CLOG(INFO, FastMapReduce) << "Starting upload from fmr to yt for table: " << fmrTableId;
                return GetRunningOperationFuture(uploadRequest, sessionId).Apply([this, sessionId = std::move(sessionId), fmrTableId = std::move(fmrTableId)] (const TFuture<TFmrOperationResult>& f) {
                    try {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                        auto fmrUploadResult = f.GetValue();
                        SetTablePresenceStatus(fmrTableId, sessionId, ETablePresenceStatus::Both);
                        return f;
                    } catch (...) {
                        YQL_CLOG(ERROR, FastMapReduce) << CurrentExceptionMessage();
                        return MakeFuture(ResultFromCurrentException<TFmrOperationResult>());
                    }
                });
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << CurrentExceptionMessage();
                return MakeFuture(ResultFromCurrentException<TFmrOperationResult>());
            }
        });
    }

    TFuture<TFmrOperationResult> DoMerge(const std::vector<std::pair<TYtTableRef, bool>>& inputTables, std::vector<TYtTableRef>& outputTables, TRunOptions&& options) {
        auto& outputTable = outputTables.back();
        TString sessionId = options.SessionId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        if (outputTable.Cluster.empty()) {
            outputTable.Cluster = inputTables[0].first.Cluster;
        }

        TString outputCluster = outputTable.Cluster, outputPath = outputTable.Path;
        TFmrTableRef fmrOutputTable{TFmrTableId(outputCluster, outputPath)};

        auto [mergeInputTables, clusterConnections] = GetInputTablesAndConnections(inputTables, std::move(options));

        TMergeOperationParams mergeOperationParams{.Input = mergeInputTables,.Output = fmrOutputTable};
        TStartOperationRequest mergeOperationRequest{
            .TaskType = ETaskType::Merge,
            .OperationParams = mergeOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries = 1,
            .ClusterConnections = clusterConnections,
            .FmrOperationSpec = options.Config()->FmrOperationSpec.Get(outputCluster)
        };

        std::vector<TString> inputPaths;
        std::transform(inputTables.begin(),inputTables.end(), std::back_inserter(inputPaths), [](const std::pair<TYtTableRef, bool>& table){
            return table.first.Path;}
        );

        YQL_CLOG(INFO, FastMapReduce) << "Starting merge from yt tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end());
        return GetRunningOperationFuture(mergeOperationRequest, sessionId);
    }

    TFuture<TFmrOperationResult> DoMap(const std::vector<std::pair<TYtTableRef, bool>>& inputTables, std::vector<TYtTableRef>& outputTables, TRunOptions&& options) {
        TString sessionId = options.SessionId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        std::vector<TFmrTableRef> fmrOutputTables;

        for (auto& outputTable : outputTables) {
            if (outputTable.Cluster.empty()) {
                outputTable.Cluster = inputTables[0].first.Cluster;
            }
            TString outputCluster = outputTable.Cluster, outputPath = outputTable.Path;
            TFmrTableRef fmrOutputTable{TFmrTableId(outputCluster, outputPath)};
            fmrOutputTables.emplace_back(fmrOutputTable);
        }
        TString defaultOutputCluster =  outputTables[0].Cluster;
        if (defaultOutputCluster.empty()) {
            defaultOutputCluster = inputTables[0].first.Cluster;
        }

        auto [mapInputTables, clusterConnections] = GetInputTablesAndConnections(inputTables, std::move(options));

        TMapOperationParams mapOperationParams{.Input = mapInputTables,.Output = fmrOutputTables, .SerializedMapJobState = ""}; // TODO - fill
        TStartOperationRequest mapOperationRequest{
            .TaskType = ETaskType::Map,
            .OperationParams = mapOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries = 1,
            .ClusterConnections = clusterConnections,
            .FmrOperationSpec = options.Config()->FmrOperationSpec.Get(defaultOutputCluster)
        };

        std::vector<TString> inputPaths;
        std::transform(inputTables.begin(),inputTables.end(), std::back_inserter(inputPaths), [](const std::pair<TYtTableRef, bool>& table){
            return table.first.Path;}
        );
        std::vector<TString> outputPaths;
        std::transform(outputTables.begin(),outputTables.end(), std::back_inserter(outputPaths), [](const TYtTableRef& table){
            return table.Path;}
        );

        YQL_CLOG(INFO, FastMapReduce) << "Starting map from yt tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end()) << " to yt tables: " << JoinRange(' ', outputPaths.begin(), outputPaths.end());
        return GetRunningOperationFuture(mapOperationRequest, sessionId);
    }

    TFuture<TFmrOperationResult> GetSuccessfulFmrOperationResult() {
        TFmrOperationResult fmrOperationResult = TFmrOperationResult();
        fmrOperationResult.SetSuccess();
        return MakeFuture(fmrOperationResult);
    }

private:
    struct TFmrGatewayOperationsState {
        std::unordered_map<TFmrTableId, TPromise<TFmrOperationResult>> OperationStatuses = {}; // operationId -> promise which we set when operation completes
    };

    struct TSessionInfo {
        TFmrGatewayOperationsState OperationStates;
        std::unordered_map<TFmrTableId, ETablePresenceStatus> TablePresenceStatuses; // yt cluster and path -> is it In Yt, Fmr TableDataService
        TString UserName;
        std::unordered_map<TFmrTableId, TFmrTableId> FmrIdAliases;
    };

    struct TSession {
        std::unordered_map<TString, TSessionInfo> Sessions;
        TMutex Mutex = TMutex();
    };

    IFmrCoordinator::TPtr Coordinator_;
    std::shared_ptr<TSession> SessionStates_;
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    TDuration TimeToSleepBetweenGetOperationRequests_;
    std::thread GetOperationStatusesThread_;
    std::atomic<bool> StopFmrGateway_;
};

} // namespace

IYtGateway::TPtr CreateYtFmrGateway(IYtGateway::TPtr slave, IFmrCoordinator::TPtr coordinator, const TFmrYtGatewaySettings& settings) {
    return MakeIntrusive<TFmrYtGateway>(std::move(slave), coordinator, settings);
}

} // namespace NYql::NFmr

template<>
void Out<NYql::NFmr::ETablePresenceStatus>(IOutputStream& out, NYql::NFmr::ETablePresenceStatus status) {
    switch (status) {
        case NYql::NFmr::ETablePresenceStatus::Both: {
            out << "BOTH";
            return;
        }
        case NYql::NFmr::ETablePresenceStatus::OnlyInFmr: {
            out << "ONLY IN FMR";
            return;
        }
        case NYql::NFmr::ETablePresenceStatus::OnlyInYt: {
            out << "ONLY IN YT";
            return;
        }
    }
}
