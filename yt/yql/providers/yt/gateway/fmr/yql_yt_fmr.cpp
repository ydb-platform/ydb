#include "yql_yt_fmr.h"

#include <thread>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/profile.h>

#include <util/generic/ptr.h>
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

struct TDownloadTableToFmrResult: public NCommon::TOperationResult {}; // Download Yt -> Fmr TableDataService

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
                    auto checkOperationStatuses = [&] <typename T> (std::unordered_map<TString, TPromise<T>>& operationStatuses, const TString& sessionId) {
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
                                        // operation finished, set value in future returned in Publish / Download
                                        bool hasCompletedSuccessfully = getOperationStatus == EOperationStatus::Completed;
                                        SendOperationCompletionSignal(promise, hasCompletedSuccessfully, operationErrorMessages);
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
                                                operationStates.DownloadOperationStatuses.erase(operationId);
                                                operationStates.UploadOperationStatuses.erase(operationId);
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    };

                    for (auto [sessionId, sessionInfo]: SessionStates_->Sessions) {
                        auto& operationStates = sessionInfo.OperationStates;
                        checkOperationStatuses(operationStates.DownloadOperationStatuses, sessionId);
                        checkOperationStatuses(operationStates.UploadOperationStatuses, sessionId);
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

    TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        if (!Coordinator_) {
            return Slave_->Publish(node, ctx, std::move(options));
        }
        auto publish = TYtPublish(node);
        TString sessionId = options.SessionId();

        auto cluster = publish.DataSink().Cluster().StringValue();
        auto token = options.Config()->Auth.Get();
        TString transformedInputPath;
        TString userName = GetUsername(sessionId);
        for (auto out: publish.Input()) {
            auto outTable = GetOutTable(out).Cast<TYtOutTable>();
            TStringBuf inputPath = outTable.Name().Value();
            transformedInputPath = NYql::TransformPath(GetTablesTmpFolder(*options.Config()), inputPath, true, userName);
            break;
        }

        // TODO - handle several inputs in Publish, use ColumnGroups, Run Merge

        auto outputPath = publish.Publish().Name().StringValue();
        auto idempotencyKey = GenerateId();

        auto fmrTableId = cluster + "." + outputPath;

        TFuture<TDownloadTableToFmrResult> downloadToFmrFuture;
        TFuture<void> downloadedSuccessfully;

        with_lock(SessionStates_->Mutex) {
            auto& tablePresenceStatuses = SessionStates_->Sessions[sessionId].TablePresenceStatuses;

            if (!tablePresenceStatuses.contains(fmrTableId)) {
                TYtTableRef ytTable{.Path = transformedInputPath, .Cluster = cluster};
                TFmrTableRef fmrTable{.TableId = fmrTableId};
                tablePresenceStatuses[fmrTableId] = ETablePresenceStatus::Both;
                downloadToFmrFuture = DownloadToFmrTableDataSerivce(ytTable, fmrTable, sessionId, options.Config());
                downloadedSuccessfully = downloadToFmrFuture.Apply([downloadedSuccessfully] (auto& downloadFuture) {
                    auto downloadResult = downloadFuture.GetValueSync();
                });
            } else {
                downloadedSuccessfully = MakeFuture();
            }
        }
        downloadedSuccessfully.Wait(); // blocking until download to fmr finishes

        TUploadTaskParams uploadTaskParams{
            .Input = TFmrTableRef{fmrTableId},
            .Output = TYtTableRef{outputPath, cluster}
        };

        auto clusterConnectionOptions = TClusterConnectionOptions(options.SessionId())
            .Cluster(cluster).Config(options.Config());
        auto clusterConnection = GetClusterConnection(std::move(clusterConnectionOptions));
        YQL_ENSURE(clusterConnection.Success());

        TStartOperationRequest uploadRequest{
            .TaskType = ETaskType::Upload,
            .TaskParams = uploadTaskParams,
            .SessionId = sessionId,
            .IdempotencyKey=idempotencyKey,
            .NumRetries=1,
            .ClusterConnection = TClusterConnection{
                .TransactionId = clusterConnection.TransactionId,
                .YtServerName = clusterConnection.YtServerName,
                .Token = clusterConnection.Token
            }
        };

        auto promise = NewPromise<TPublishResult>();
        auto future = promise.GetFuture();

        YQL_CLOG(DEBUG, FastMapReduce) << "Starting upload to yt table: " << cluster + "." + outputPath;
        auto uploadOperationResponseFuture = Coordinator_->StartOperation(uploadRequest);
        uploadOperationResponseFuture.Subscribe([this, promise = std::move(promise), sessionId] (const auto& uploadFuture) {
            TStartOperationResponse startOperationResponse = uploadFuture.GetValueSync();
            TString operationId = startOperationResponse.OperationId;
            with_lock(SessionStates_->Mutex) {
                YQL_ENSURE(SessionStates_->Sessions.contains(sessionId));
                auto& operationStates = SessionStates_->Sessions[sessionId].OperationStates;
                auto& uploadOperationStatuses = operationStates.UploadOperationStatuses;
                YQL_ENSURE(!uploadOperationStatuses.contains(operationId));
                uploadOperationStatuses[operationId] = promise;
            }
        });
        return future;
    }

    TFuture<TDownloadTableToFmrResult> DownloadToFmrTableDataSerivce(
        const TYtTableRef& ytTableRef, const TFmrTableRef& fmrTableRef, const TString& sessionId, TYtSettings::TConstPtr& config)
    {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        TString fmrTableId = fmrTableRef.TableId;
        TDownloadTaskParams downloadTaskParams{
            .Input = ytTableRef,
            .Output = {fmrTableId}
        };
        auto idempotencyKey = GenerateId();
        auto clusterConnectionOptions = TClusterConnectionOptions(sessionId)
            .Cluster(ytTableRef.Cluster).Config(config);
        auto clusterConnection = GetClusterConnection(std::move(clusterConnectionOptions));
        YQL_ENSURE(clusterConnection.Success());
        TStartOperationRequest downloadRequest{
            .TaskType = ETaskType::Download,
            .TaskParams = downloadTaskParams,
            .SessionId = sessionId,
            .IdempotencyKey = idempotencyKey,
            .NumRetries=1,
            .ClusterConnection = TClusterConnection{
                .TransactionId = clusterConnection.TransactionId,
                .YtServerName = clusterConnection.YtServerName,
                .Token = clusterConnection.Token
            }
        };

        YQL_CLOG(DEBUG, FastMapReduce) << "Starting download from yt table: " << fmrTableId;

        auto promise = NewPromise<TDownloadTableToFmrResult>();
        auto future = promise.GetFuture();

        auto downloadOperationResponseFuture = Coordinator_->StartOperation(downloadRequest);
        downloadOperationResponseFuture.Subscribe([this, promise = std::move(promise), sessionId] (const auto& downloadFuture) {
            TStartOperationResponse downloadOperationResponse = downloadFuture.GetValueSync();
            TString operationId = downloadOperationResponse.OperationId;
            with_lock(SessionStates_->Mutex) {
                auto& operationStates = SessionStates_->Sessions[sessionId].OperationStates;
                auto& downloadOperationStatuses = operationStates.DownloadOperationStatuses;
                YQL_ENSURE(!downloadOperationStatuses.contains(operationId));
                downloadOperationStatuses[operationId] = promise;
            }
        });
        return future;
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

            auto cancelOperationsFunc = [&] <typename T> (std::unordered_map<TString, TPromise<T>>& operationStatuses) {
                std::vector<TFuture<TDeleteOperationResponse>> cancelOperationsFutures;

                for (auto& [operationId, promise]: operationStatuses) {
                    cancelOperationsFutures.emplace_back(Coordinator_->DeleteOperation({operationId}));
                }
                NThreading::WaitAll(cancelOperationsFutures).GetValueSync();
                for (auto& [operationId, promise]: operationStatuses) {
                    SendOperationCompletionSignal(promise, false);
                }
            };

            cancelOperationsFunc(operationStates.DownloadOperationStatuses);
            cancelOperationsFunc(operationStates.UploadOperationStatuses);
        }
        Slave_->CleanupSession(std::move(options)).Wait();
        return MakeFuture();
    }

private:
    struct TFmrGatewayOperationsState {
        std::unordered_map<TString, TPromise<TPublishResult>> UploadOperationStatuses = {}; // operationId -> promise which we set when operation completes
        std::unordered_map<TString, TPromise<TDownloadTableToFmrResult>> DownloadOperationStatuses = {};
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

    TString GenerateId() {
        return GetGuidAsString(RandomProvider_->GenGuid());
    }

    template <std::derived_from<NCommon::TOperationResult> T>
    void SendOperationCompletionSignal(TPromise<T> promise, bool completedSuccessfully = false, const std::vector<TFmrError>& errorMessages = {}) {
        YQL_ENSURE(!promise.HasValue());
        T commonOperationResult{};
        if (completedSuccessfully) {
            commonOperationResult.SetSuccess();
        } else if (!errorMessages.empty()) {
            auto exception = yexception() << "Operation failed with errors: " << JoinSeq(" ", errorMessages);
            commonOperationResult.SetException(exception);
        }
        promise.SetValue(commonOperationResult);
    }

    TString GetUsername(const TString& sessionId) {
        with_lock(SessionStates_->Mutex) {
            YQL_ENSURE(SessionStates_->Sessions.contains(sessionId));
            auto& session = SessionStates_->Sessions[sessionId];
            return session.UserName;
        }
    }
};

} // namespace

IYtGateway::TPtr CreateYtFmrGateway(IYtGateway::TPtr slave, IFmrCoordinator::TPtr coordinator, const TFmrYtGatewaySettings& settings) {
    return MakeIntrusive<TFmrYtGateway>(std::move(slave), coordinator, settings);
}

} // namespace NYql::NFmr
