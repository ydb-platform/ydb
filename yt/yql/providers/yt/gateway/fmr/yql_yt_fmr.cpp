#include "yql_yt_fmr.h"

#include <thread>

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
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
                    auto checkOperationStatuses = [&] <typename T> (std::unordered_map<TString, TPromise<T>>& operationStatuses) {
                        std::vector<TString> completedOperationIds;
                        for (auto& [operationId, promise]: operationStatuses) {
                            auto getOperationFuture = Coordinator_->GetOperation({operationId});
                            getOperationFuture.Subscribe([&, operationId] (const auto& getFuture) {
                                auto getOperationResult = getFuture.GetValueSync();
                                auto getOperationStatus = getOperationResult.Status;
                                auto operationErrorMessages = getOperationResult.ErrorMessages;
                                with_lock(SessionStates_->Mutex) {
                                    bool operationCompleted = getOperationStatus != EOperationStatus::Accepted && getOperationStatus != EOperationStatus::InProgress;
                                    if (operationCompleted) {
                                        // operation finished, set value in future returned in Publish / Download
                                        bool hasCompletedSuccessfully = getOperationStatus == EOperationStatus::Completed;
                                        SendOperationCompletionSignal(promise, hasCompletedSuccessfully, operationErrorMessages);
                                        completedOperationIds.emplace_back(operationId);
                                    }
                                }
                            });
                        }

                        for (auto& operationId: completedOperationIds) {
                            Coordinator_->DeleteOperation({operationId}).GetValueSync();
                            operationStatuses.erase(operationId);
                        }
                    };

                    for (auto& [sessionId, sessionInfo]: SessionStates_->Sessions) {
                        auto& operationStates = sessionInfo.OperationStates;
                        checkOperationStatuses(operationStates.DownloadOperationStatuses);
                        checkOperationStatuses(operationStates.UploadOperationStatuses);
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
        if (!Coordinator_) {
            return Slave_->Publish(node, ctx, std::move(options));
        }
        auto publish = TYtPublish(node);

        auto cluster = publish.DataSink().Cluster().StringValue();
        auto outputPath = publish.Publish().Name().StringValue();
        auto transactionId = GenerateId();
        auto idempotencyKey = GenerateId();

        auto fmrTableId = cluster + "." + outputPath;

        TFuture<TDownloadTableToFmrResult> downloadToFmrFuture;
        TFuture<void> downloadedSuccessfully;

        TString sessionId = options.SessionId();
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__, sessionId);

        with_lock(SessionStates_->Mutex) {
            auto& tablePresenceStatuses = SessionStates_->Sessions[sessionId].TablePresenceStatuses;

            if (!tablePresenceStatuses.contains(fmrTableId)) {
                TYtTableRef ytTable{.Path = outputPath, .Cluster = cluster, .TransactionId = transactionId};
                tablePresenceStatuses[fmrTableId] = ETablePresenceStatus::Both;
                downloadToFmrFuture = DownloadToFmrTableDataSerivce(ytTable, sessionId);
                downloadedSuccessfully = downloadToFmrFuture.Apply([downloadedSuccessfully] (auto& downloadFuture) {
                    auto downloadResult = downloadFuture.GetValueSync();
                });
            } else {
                downloadedSuccessfully = MakeFuture();
            }
        }
        downloadedSuccessfully.Wait(); // blocking until download to fmr finishes

        YQL_CLOG(INFO, FastMapReduce) << "Uploading table with cluster " << cluster << " and path " << outputPath << " from fmr to yt";

        TUploadTaskParams uploadTaskParams{
            .Input = TFmrTableRef{fmrTableId},
            .Output = TYtTableRef{outputPath, cluster, transactionId}
        };

        TStartOperationRequest uploadRequest{
            .TaskType = ETaskType::Upload, .TaskParams = uploadTaskParams, .SessionId = sessionId, .IdempotencyKey=idempotencyKey, .NumRetries=1
        };

        auto promise = NewPromise<TPublishResult>();
        auto future = promise.GetFuture();

        auto startOperationResponseFuture = Coordinator_->StartOperation(uploadRequest);
        startOperationResponseFuture.Subscribe([this, promise = std::move(promise), &sessionId] (const auto& startFuture) {
            TString operationId = startFuture.GetValueSync().OperationId;
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

    TFuture<TDownloadTableToFmrResult> DownloadToFmrTableDataSerivce(const TYtTableRef& ytTableRef, const TString& sessionId) {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        TString fmrTableId = ytTableRef.Cluster + "." + ytTableRef.Path;
        TDownloadTaskParams downloadTaskParams{
            .Input = ytTableRef,
            .Output = {fmrTableId}
        };
        auto idempotencyKey = GenerateId();
        TStartOperationRequest downloadRequest{
            .TaskType = ETaskType::Download, .TaskParams = downloadTaskParams, .SessionId = sessionId, .IdempotencyKey=idempotencyKey, .NumRetries=1
        };

        YQL_CLOG(INFO, FastMapReduce) << "Downloading table with cluster " << ytTableRef.Cluster << " and path " << ytTableRef.Path << " from yt to fmr";

        auto promise = NewPromise<TDownloadTableToFmrResult>();
        auto future = promise.GetFuture();

        auto startOperationResponseFuture = Coordinator_->StartOperation(downloadRequest);
        startOperationResponseFuture.Subscribe([this, promise = std::move(promise), &sessionId] (const auto& startFuture) {
            TString operationId = startFuture.GetValueSync().OperationId;
            with_lock(SessionStates_->Mutex) {
                auto& operationStates = SessionStates_->Sessions[sessionId].OperationStates;
                auto& downloadOperationStatuses = operationStates.DownloadOperationStatuses;
                YQL_ENSURE(!downloadOperationStatuses.contains(operationId));
                downloadOperationStatuses[operationId] = promise;
            }
        });
        return future;
    }

    void OpenSession(TOpenSessionOptions&& options) final {
        Slave_->OpenSession(std::move(options));

        TString sessionId = options.SessionId();
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        with_lock(SessionStates_->Mutex) {
            auto sessions = SessionStates_->Sessions;
            if (sessions.contains(sessionId)) {
                YQL_LOG_CTX_THROW yexception() << "Session already exists: " << sessionId;
            }
            sessions[sessionId] = TSessionInfo();
        }
    }

    TFuture<void> CloseSession(TCloseSessionOptions&& options) final {
        Slave_->CloseSession(std::move(options)).Wait();
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        with_lock(SessionStates_->Mutex) {
            auto& sessions = SessionStates_->Sessions;
            auto it = sessions.find(options.SessionId());
            if (it != sessions.end()) {
                sessions.erase(it);
            }
        }
        return MakeFuture();
    }

    TFuture<void> CleanupSession(TCleanupSessionOptions&& options) final {
        Slave_->CleanupSession(std::move(options)).Wait();
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
                std::vector<TFuture<T>> resultFutures;

                for (auto& [operationId, promise]: operationStatuses) {
                    SendOperationCompletionSignal(promise, false);
                    resultFutures.emplace_back(promise.GetFuture());
                }
                NThreading::WaitAll(resultFutures).GetValueSync();
            };

            cancelOperationsFunc(operationStates.DownloadOperationStatuses);
            cancelOperationsFunc(operationStates.UploadOperationStatuses);
        }

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
        T commonOperationResult;
        if (completedSuccessfully) {
            commonOperationResult.SetSuccess();
        } else if (!errorMessages.empty()) {
            auto exception = yexception() << "Operation failed with errors: " << JoinSeq(" ", errorMessages);
            commonOperationResult.SetException(exception);
        }
        promise.SetValue(commonOperationResult);
    }
};

} // namespace

IYtGateway::TPtr CreateYtFmrGateway(IYtGateway::TPtr slave, IFmrCoordinator::TPtr coordinator, const TFmrYtGatewaySettings& settings) {
    return MakeIntrusive<TFmrYtGateway>(std::move(slave), std::move(coordinator), settings);
}

} // namespace NYql::NFmr
