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

namespace {

enum class ETablePresenceStatus {
    OnlyInYt,
    OnlyInFmr,
    Both
};

struct TFmrOperationResult: public NCommon::TOperationResult {};

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
                    auto checkOperationStatuses = [&] (std::unordered_map<TString, TPromise<TFmrOperationResult>>& operationStatuses, const TString& sessionId) {
                        for (auto& [operationId, promise]: operationStatuses) {
                            YQL_CLOG(TRACE, FastMapReduce) << "Sending get operation request to coordinator with operationId: " << operationId;

                            auto getOperationFuture = Coordinator_->GetOperation({operationId});
                            getOperationFuture.Subscribe([&, operationId, sessionId] (const auto& getFuture) {
                                auto getOperationResult = getFuture.GetValueSync();
                                auto getOperationStatus = getOperationResult.Status;
                                auto operationErrorMessages = getOperationResult.ErrorMessages;
                                with_lock(SessionStates_->Mutex) {
                                    bool operationCompleted = getOperationStatus != EOperationStatus::Accepted && getOperationStatus != EOperationStatus::InProgress;
                                    if (operationCompleted) {
                                        // operation finished, set value in future returned in DoMerge / DoUpload
                                        bool hasCompletedSuccessfully = getOperationStatus == EOperationStatus::Completed;
                                        if (hasCompletedSuccessfully) {
                                            TFmrOperationResult fmrOperationResult{};
                                            fmrOperationResult.SetSuccess();
                                            promise.SetValue(fmrOperationResult);
                                        } else {
                                            promise.SetException(JoinRange(' ', operationErrorMessages.begin(), operationErrorMessages.end()));
                                        }
                                        YQL_CLOG(DEBUG, FastMapReduce) << "Sending delete operation request to coordinator with operationId: " << operationId;
                                        auto deleteOperationFuture = Coordinator_->DeleteOperation({operationId});
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

        if (auto op = opBase.Maybe<TYtMerge>()) {
            auto ytMerge = op.Cast();
            std::vector<TYtTableRef> inputTables = GetMergeInputTables(ytMerge);
            TYtTableRef outputTable = GetMergeOutputTable(ytMerge);
            auto future = DoMerge(inputTables, outputTable, std::move(options));
            return future.Apply([this, pos = nodePos, outputTable = std::move(outputTable), options = std::move(options)] (const TFuture<TFmrOperationResult>& f) {
                try {
                    f.GetValue(); // rethrow error if any
                    TString sessionId = options.SessionId();
                    auto config = options.Config();
                    TString transformedOutputTableId = GetTransformedPath(outputTable.Path, sessionId, config);
                    TString fmrOutputTableId = outputTable.Cluster + "." + transformedOutputTableId;
                    SetTablePresenceStatus(fmrOutputTableId, sessionId, ETablePresenceStatus::OnlyInFmr);
                    TRunResult result;
                    result.OutTableStats.emplace_back(outputTable.Path, MakeIntrusive<TYtTableStatInfo>()); // TODO - add statistics?
                    result.OutTableStats.back().second->Id = "fmr_" + fmrOutputTableId;
                    result.SetSuccess();
                    return MakeFuture<TRunResult>(std::move(result));
                } catch (...) {
                    return MakeFuture(ResultFromCurrentException<TRunResult>(pos));
                }
            });
        } else {
            return Slave_->Run(node, ctx, std::move(options));
        }
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

        for (auto out: publish.Input()) {
            auto outTableWithCluster = GetOutTableWithCluster(out);
            auto outTable = GetOutTable(out).Cast<TYtOutTable>();
            TStringBuf inputPath = outTable.Name().Value();
            TString transformedInputPath = GetTransformedPath(ToString(inputPath), sessionId, config);
            auto outputBase = out.Operation().Cast<TYtOutputOpBase>().Ptr();

            TFmrTableRef fmrTableRef = TFmrTableRef{outTableWithCluster.second + "." + transformedInputPath};
            uploadFmrTablesToYtFutures.emplace_back(DoUpload(fmrTableRef, sessionId, config, outputBase, ctx));
        }

        auto outputPath = publish.Publish().Name().StringValue();
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

    TClusterConnectionResult GetClusterConnection(const TClusterConnectionOptions&& options) override {
        return Slave_->GetClusterConnection(std::move(options));
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

        with_lock(SessionStates_->Mutex) {
            auto& sessions = SessionStates_->Sessions;
            auto it = sessions.find(options.SessionId());
            if (it != sessions.end()) {
                sessions.erase(it);
            }
        }
        Slave_->CloseSession(std::move(options)).Wait();
        return MakeFuture();
    }

    TFuture<void> CleanupSession(TCleanupSessionOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        TString sessionId = options.SessionId();
        with_lock(SessionStates_->Mutex) {
            auto& sessions = SessionStates_->Sessions;
            YQL_ENSURE(sessions.contains(sessionId));
            auto& operationStates = sessions[sessionId].OperationStates;

            auto cancelOperationsFunc = [&] (std::unordered_map<TString, TPromise<TFmrOperationResult>>& operationStatuses) {
                std::vector<TFuture<TDeleteOperationResponse>> cancelOperationsFutures;

                for (auto& [operationId, promise]: operationStatuses) {
                    cancelOperationsFutures.emplace_back(Coordinator_->DeleteOperation({operationId}));
                }
                NThreading::WaitAll(cancelOperationsFutures).GetValueSync();
            };

            cancelOperationsFunc(operationStates.OperationStatuses);
        }
        Slave_->CleanupSession(std::move(options)).Wait();
        return MakeFuture();
    }

private:
    TString GenerateId() {
        return GetGuidAsString(RandomProvider_->GenGuid());
    }

    TString GetUsername(const TString& sessionId) {
        with_lock(SessionStates_->Mutex) {
            YQL_ENSURE(SessionStates_->Sessions.contains(sessionId));
            auto& session = SessionStates_->Sessions[sessionId];
            return session.UserName;
        }
    }

    TString GetTransformedPath(const TString& path, const TString& sessionId, TYtSettings::TConstPtr& config) {
        TString username = GetUsername(sessionId);
        return NYql::TransformPath(GetTablesTmpFolder(*config), path, true, username);
    }

    void SetTablePresenceStatus(const TString& fmrTableId, const TString& sessionId, ETablePresenceStatus newStatus) {
        with_lock(SessionStates_->Mutex) {
            auto& tablePresenceStatuses = SessionStates_->Sessions[sessionId].TablePresenceStatuses;
            tablePresenceStatuses[fmrTableId] = newStatus;
        }
    }

    TMaybe<ETablePresenceStatus> GetTablePresenceStatus(const TString& fmrTableId, const TString& sessionId) {
        with_lock(SessionStates_->Mutex) {
            auto& tablePresenceStatuses = SessionStates_->Sessions[sessionId].TablePresenceStatuses;
            if (!tablePresenceStatuses.contains(fmrTableId)) {
                return Nothing();
            }
            return tablePresenceStatuses[fmrTableId];
        }
    }

    std::vector<TYtTableRef> GetMergeInputTables(const TYtMerge& ytMerge) {
        auto input = ytMerge.Maybe<TYtTransientOpBase>().Cast().Input();
        std::vector<TYtTableRef> inputTables;
        for (auto section: input.Cast<TYtSectionList>()) {
            for (auto path: section.Paths()) {
                TYtPathInfo pathInfo(path);
                TYtTableRef ytTable{.Path = pathInfo.Table->Name, .Cluster = pathInfo.Table->Cluster};
                inputTables.emplace_back(ytTable);
            }
        }
        return inputTables;
    }

    TYtTableRef GetMergeOutputTable(const TYtMerge& ytMerge) {
        auto output = ytMerge.Maybe<TYtOutputOpBase>().Cast().Output();
        std::vector<TYtTableRef> outputTables;
        for (auto table: output) {
            TYtOutTableInfo tableInfo(table);
            TString outTableName = tableInfo.Name;
            if (outTableName.empty()) {
                outTableName = TStringBuilder() << "tmp/" << GetGuidAsString(RandomProvider_->GenGuid());
            }
            outputTables.emplace_back(outTableName, tableInfo.Cluster);
        }
        YQL_ENSURE(outputTables.size() == 1);
        return outputTables[0];
    }

    TString GetClusterFromMergeTables(const std::vector<TYtTableRef>& inputTables, TYtTableRef& outputTable) {
        std::unordered_set<TString> clusters;
        for (auto& [path, cluster]: inputTables) {
            clusters.emplace(cluster);
        }
        YQL_ENSURE(clusters.size() == 1);
        TString cluster = *clusters.begin();
        if (outputTable.Cluster) {
            YQL_ENSURE(outputTable.Cluster == cluster);
        } else {
            outputTable.Cluster = cluster;
        }
        return cluster;
    }

    TClusterConnection GetTablesClusterConnection(const TString& cluster, const TString& sessionId, TYtSettings::TConstPtr& config) {
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

    TFuture<TFmrOperationResult> DoUpload(const TFmrTableRef& fmrTableRef, const TString& sessionId, TYtSettings::TConstPtr& config, TExprNode::TPtr outputOpBase, TExprContext& ctx) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        std::vector<TString> ytTableInfo;
        StringSplitter(fmrTableRef.TableId).SplitByString(".").AddTo(&ytTableInfo);
        YQL_ENSURE(ytTableInfo.size() == 2);
        TString outputCluster = ytTableInfo[0], outputPath = ytTableInfo[1];
        auto tablePresenceStatus = GetTablePresenceStatus(fmrTableRef.TableId, sessionId);
        if (!tablePresenceStatus || *tablePresenceStatus != ETablePresenceStatus::OnlyInFmr) {
            YQL_CLOG(DEBUG, FastMapReduce) << " We assume table " << fmrTableRef.TableId << " should be present in yt, not uploading from fmr";
            TFmrOperationResult fmrOperationResult = TFmrOperationResult();
            fmrOperationResult.SetSuccess();
            return MakeFuture(fmrOperationResult);
        }

        TUploadOperationParams uploadOperationParams{
            .Input = fmrTableRef,
            .Output = TYtTableRef{.Path = outputPath, .Cluster = outputCluster}
        };

        auto clusterConnection = GetTablesClusterConnection(outputCluster, sessionId, config);
        TStartOperationRequest uploadRequest{
            .TaskType = ETaskType::Upload,
            .OperationParams = uploadOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries=1,
            .ClusterConnection = clusterConnection,
            .FmrOperationSpec = config->FmrOperationSpec.Get(outputCluster)
        };

        auto prepareOptions = TPrepareOptions(sessionId)
                .Config(config);
        auto prepareFuture = Slave_->Prepare(outputOpBase, ctx, std::move(prepareOptions));

        return prepareFuture.Apply([this, uploadRequest = std::move(uploadRequest), sessionId = std::move(sessionId), fmrTableId = std::move(fmrTableRef.TableId)] (const TFuture<TRunResult>& f) {
            try {
                f.GetValue(); // rethrow error if any
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
                YQL_CLOG(DEBUG, FastMapReduce) << "Starting upload from fmr to yt for table: " << fmrTableId;
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

    TFuture<TFmrOperationResult> DoMerge(const std::vector<TYtTableRef>& inputTables, TYtTableRef& outputTable, TRunOptions&& options) {
        TString sessionId = options.SessionId();
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        auto cluster = GetClusterFromMergeTables(inputTables, outputTable); // Can set outputTable.Cluster if empty

        TString outputTableId = outputTable.Path, outputCluster = outputTable.Cluster;
        TString transformedOutputTableId = GetTransformedPath(outputTableId, sessionId, options.Config());
        TFmrTableRef fmrOutputTable{.TableId = outputCluster + "." + transformedOutputTableId};

        std::vector<TOperationTableRef> mergeInputTables;
        for (auto& ytTable: inputTables) {
            TString fmrTableId = ytTable.Cluster + "." + ytTable.Path;
            auto tablePresenceStatus = GetTablePresenceStatus(fmrTableId, sessionId);
            if (!tablePresenceStatus) {
                SetTablePresenceStatus(fmrTableId, sessionId, ETablePresenceStatus::OnlyInYt);
            }

            if (tablePresenceStatus && *tablePresenceStatus != ETablePresenceStatus::OnlyInYt) {
                // table is in fmr, do not download
                mergeInputTables.emplace_back(TFmrTableRef{.TableId = fmrTableId});
            } else {
                mergeInputTables.emplace_back(ytTable);
            }
        }

        TMergeOperationParams mergeOperationParams{.Input = mergeInputTables,.Output = fmrOutputTable};
        auto clusterConnection = GetTablesClusterConnection(cluster, sessionId, options.Config());
        TStartOperationRequest mergeOperationRequest{
            .TaskType = ETaskType::Merge,
            .OperationParams = mergeOperationParams,
            .SessionId = sessionId,
            .IdempotencyKey = GenerateId(),
            .NumRetries = 1,
            .ClusterConnection = clusterConnection,
            .FmrOperationSpec = options.Config()->FmrOperationSpec.Get(outputCluster)
        };

        std::vector<TString> inputPaths;
        std::transform(inputTables.begin(),inputTables.end(), std::back_inserter(inputPaths), [](const TYtTableRef& ytTableRef){
            return ytTableRef.Path;}
        );

        YQL_CLOG(DEBUG, FastMapReduce) << "Starting merge from yt tables: " << JoinRange(' ', inputPaths.begin(), inputPaths.end());
        return GetRunningOperationFuture(mergeOperationRequest, sessionId);
    }

private:
    struct TFmrGatewayOperationsState {
        std::unordered_map<TString, TPromise<TFmrOperationResult>> OperationStatuses = {}; // operationId -> promise which we set when operation completes
    };

    struct TSessionInfo {
        TFmrGatewayOperationsState OperationStates;
        std::unordered_map<TString, ETablePresenceStatus> TablePresenceStatuses; // yt cluster and path -> is it In Yt, Fmr TableDataService
        TString UserName;
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
