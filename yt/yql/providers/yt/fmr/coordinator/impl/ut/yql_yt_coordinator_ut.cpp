#include "yql_yt_coordinator_ut.h"

#include <yt/yql/providers/yt/fmr/test_tools/mock_time_provider/yql_yt_mock_time_provider.h>

namespace NYql::NFmr {

// TODO - rewrite all fmr tests without explicit Sleep().

Y_UNIT_TEST_SUITE(FmrCoordinatorTests) {
    Y_UNIT_TEST(StartOperation) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(setup.CreateOperationRequest()).GetValueSync();
        auto status = startOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Accepted);
    }
    Y_UNIT_TEST(RetryAcceptedOperation) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto downloadRequest = setup.CreateOperationRequest();
        auto firstResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        auto firstOperationId = firstResponse.OperationId;
        auto sameRequest = coordinator->StartOperation(downloadRequest);
        auto secondResponse = sameRequest.GetValueSync();
        auto secondOperationId = secondResponse.OperationId;
        auto status = secondResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Accepted);
        UNIT_ASSERT_NO_DIFF(firstOperationId, secondOperationId);
    }

    Y_UNIT_TEST(DeleteNonexistentOperation) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto deleteOperationResponse = coordinator->DeleteOperation({"delete_operation_id"}).GetValueSync();
        EOperationStatus status = deleteOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::NotFound);
    }
    Y_UNIT_TEST(DeleteOperationBeforeSendToWorker) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(setup.CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;
        auto deleteOperationResponse = coordinator->DeleteOperation({operationId}).GetValueSync();
        EOperationStatus status = deleteOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Aborted);
    }
    Y_UNIT_TEST(GetNonexistentOperation) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto getOperationResponse = coordinator->GetOperation({"get_operation_id"}).GetValueSync();
        EOperationStatus status = getOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::NotFound);
    }
    Y_UNIT_TEST(GetAcceptedOperationStatus) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(setup.CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;
        auto getOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();
        EOperationStatus status = getOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Accepted);
    }
    Y_UNIT_TEST(GetRunningOperationStatus) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(setup.CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;

        auto worker = setup.GetFmrWorker(coordinator);
        Sleep(TDuration::Seconds(1));
        auto getOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();
        EOperationStatus status = getOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::InProgress);
    }
    Y_UNIT_TEST(GetCompletedOperationStatuses) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto startOperationRequests = setup.CreateSeveralOperationRequests();
        std::vector<TString> operationIds;
        for (auto& request: startOperationRequests) {
            auto startOperationResponse = coordinator->StartOperation(request).GetValueSync();
            operationIds.emplace_back(startOperationResponse.OperationId);
        }

        auto worker = setup.GetFmrWorker(coordinator, 10);
        Sleep(TDuration::Seconds(6));

        for (auto& operationId: operationIds) {
            auto getOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();
            EOperationStatus status = getOperationResponse.Status;
            UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Completed);
        }
    }
    Y_UNIT_TEST(GetCompletedAndFailedOperationStatuses) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto downloadOperationRequests = setup.CreateSeveralOperationRequests();
        std::vector<TString> downloadOperationIds;
        for (auto& request: downloadOperationRequests) {
            auto startOperationResponse = coordinator->StartOperation(request).GetValueSync();
            downloadOperationIds.emplace_back(startOperationResponse.OperationId);
        }
        auto badDownloadRequest = setup.CreateOperationRequest(ETaskType::Download, TDownloadOperationParams{
            .Input = TYtTableRef{"BadCluster", "BadPath", "BadFilePath"},
            .Output = TFmrTableRef{{"bad_cluster", "bad_path"}}
        });
        auto badDownloadOperationResponse = coordinator->StartOperation(badDownloadRequest).GetValueSync();
        auto badDownloadOperationId = badDownloadOperationResponse.OperationId;

        auto func = [&] (TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (! cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                TDownloadTaskParams downloadTaskParams = std::get<TDownloadTaskParams>(task->TaskParams);
                if (downloadTaskParams.Output.TableId.Contains("bad_path")) {
                    return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
                }
                return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics()};
            }
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };

        auto worker = setup.GetFmrWorker(coordinator, 10, func);
        Sleep(TDuration::Seconds(5));

        for (auto& operationId: downloadOperationIds) {
            auto getDownloadOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();
            EOperationStatus status = getDownloadOperationResponse.Status;
            UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Completed);
        }
        auto getBadDownloadOperationResponse = coordinator->GetOperation({badDownloadOperationId}).GetValueSync();
        EOperationStatus badDownloadStatus = getBadDownloadOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(badDownloadStatus, EOperationStatus::Failed);
    }
    Y_UNIT_TEST(RetryRunningOperation) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto downloadRequest = setup.CreateOperationRequest();
        auto startOperationResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        TString firstOperationId = startOperationResponse.OperationId;

        auto worker = setup.GetFmrWorker(coordinator);
        Sleep(TDuration::Seconds(1));

        auto secondStartOperationResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        EOperationStatus status = secondStartOperationResponse.Status;
        TString secondOperationId = secondStartOperationResponse.OperationId;
        UNIT_ASSERT_NO_DIFF(firstOperationId, secondOperationId);
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::InProgress);
    }
    Y_UNIT_TEST(RetryRunningOperationAfterIdempotencyKeyClear) {
        TFmrTestSetup setup;
        auto coordinatorSettings = TFmrCoordinatorSettings();
        coordinatorSettings.IdempotencyKeyStoreTime = TDuration::Seconds(1);
        auto coordinator = setup.GetFmrCoordinator(coordinatorSettings);

        auto worker = setup.GetFmrWorker(coordinator);

        auto downloadRequest = setup.CreateOperationRequest();
        auto startOperationResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        TString firstOperationId = startOperationResponse.OperationId;

        Sleep(TDuration::Seconds(3));
        auto secondStartOperationResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        EOperationStatus secondOperationStatus = secondStartOperationResponse.Status;
        TString secondOperationId = secondStartOperationResponse.OperationId;
        auto getFirstOperationResponse = coordinator->GetOperation({firstOperationId}).GetValueSync();
        EOperationStatus firstOperationStatus = getFirstOperationResponse.Status;

        UNIT_ASSERT_VALUES_UNEQUAL(firstOperationId, secondOperationId);
        UNIT_ASSERT_VALUES_EQUAL(firstOperationStatus, EOperationStatus::InProgress);
        UNIT_ASSERT_VALUES_EQUAL(secondOperationStatus, EOperationStatus::Accepted);
    }
    Y_UNIT_TEST(HandleJobErrors) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(setup.CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;

        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (! cancelFlag->load()) {
                Sleep(TDuration::Seconds(2));
                throw std::runtime_error{"Function crashed"};
            }
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };

        auto worker = setup.GetFmrWorker(coordinator, 3, func);
        Sleep(TDuration::Seconds(4));
        auto getOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();

        EOperationStatus status = getOperationResponse.Status;
        std::vector<TFmrError> errorMessages = getOperationResponse.ErrorMessages;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Failed);
        UNIT_ASSERT(errorMessages.size() == 1);
        auto& error = errorMessages[0];
        TString expectedErrorMessage = "Function crashed";
        UNIT_ASSERT(error.ErrorMessage.Contains(expectedErrorMessage));
        UNIT_ASSERT_VALUES_EQUAL(error.Component, EFmrComponent::Job);
    }

    Y_UNIT_TEST(GetFmrTableInfo) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        ui64 totalChunkCount = 10, chunkRowCount = 1, chunkDataWeight = 2;
        TString tableId = "TestCluster.TestPath"; // corresponds to CreateOperationRequest()
        auto func = [&] (TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                TDownloadTaskParams downloadTaskParams = std::get<TDownloadTaskParams>(task->TaskParams);
                TString partId = downloadTaskParams.Output.PartId;
                TFmrTableOutputRef fmrTableOutputRef(tableId, partId);
                TTableChunkStats tableChunkStats{
                    .PartId = partId,
                    .PartIdChunkStats = std::vector<TChunkStats>(totalChunkCount, TChunkStats{.Rows = chunkRowCount, .DataWeight = chunkDataWeight})
                };
                std::unordered_map<TFmrTableOutputRef, TTableChunkStats> outputTables{{fmrTableOutputRef, tableChunkStats}};

                return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics{
                    .OutputTables = outputTables
                }};
            }
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };

        auto worker = setup.GetFmrWorker(coordinator, 3 ,func);

        coordinator->StartOperation(setup.CreateOperationRequest()).GetValueSync();
        Sleep(TDuration::Seconds(5));
        auto response = coordinator->GetFmrTableInfo({.TableId = tableId, .SessionId = "test-session-id"}).GetValueSync();
        worker->Stop();
        UNIT_ASSERT_VALUES_EQUAL(response.TableStats.Chunks, totalChunkCount);
        UNIT_ASSERT_VALUES_EQUAL(response.TableStats.Rows, totalChunkCount * chunkRowCount);
        UNIT_ASSERT_VALUES_EQUAL(response.TableStats.DataWeight, totalChunkCount * chunkDataWeight);
    }

    Y_UNIT_TEST(SessionAutoCleanup) {
        TFmrTestSetup setup;

        auto coordinatorSettings = TFmrCoordinatorSettings();

        auto timeProvider = MakeIntrusive<TMockTimeProvider>(TDuration::MilliSeconds(50));
        coordinatorSettings.TimeProvider = timeProvider;
        coordinatorSettings.SessionInactivityTimeout = TDuration::MilliSeconds(500);
        coordinatorSettings.TimeToSleepBetweenCheckWorkerStatusRequests = TDuration::MilliSeconds(50);
        coordinatorSettings.HealthCheckInterval = TDuration::MilliSeconds(200);
        TString sessionId = "test-session";

        auto coordinator = setup.GetFmrCoordinator(coordinatorSettings, sessionId);

        auto listResponse1 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse1.SessionIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listResponse1.SessionIds[0], sessionId);

        // Advance time but not enough to trigger cleanup
        timeProvider->Advance(TDuration::MilliSeconds(200));
        auto listResponse2 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse2.SessionIds.size(), 1);

        // Advance time past inactivity timeout
        timeProvider->Advance(TDuration::MilliSeconds(800), TDuration::MilliSeconds(500));

        // Session should be automatically cleaned up
        auto listResponse3 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse3.SessionIds.size(), 0);
    }
    Y_UNIT_TEST(SessionStaysActiveWithRegularRequests) {
        TFmrTestSetup setup;

        auto coordinatorSettings = TFmrCoordinatorSettings();
        auto timeProvider = MakeIntrusive<TMockTimeProvider>(TDuration::MilliSeconds(50));
        coordinatorSettings.TimeProvider = timeProvider;
        coordinatorSettings.SessionInactivityTimeout = TDuration::MilliSeconds(400);
        coordinatorSettings.TimeToSleepBetweenCheckWorkerStatusRequests = TDuration::MilliSeconds(50);
        coordinatorSettings.HealthCheckInterval = TDuration::MilliSeconds(100);
        TString sessionId = "test-active-gateway-session";

        auto coordinator = setup.GetFmrCoordinator(coordinatorSettings, sessionId);

        coordinator->OpenSession({.SessionId = sessionId}).GetValueSync();

        // Ping session regularly to keep it alive
        for (int i = 0; i < 4; ++i) {
            timeProvider->Advance(TDuration::MilliSeconds(200));
            auto pingResponse = coordinator->PingSession({.SessionId = sessionId}).GetValueSync();
            UNIT_ASSERT(pingResponse.Success);
        }

        // Session should still be active after regular pings
        Sleep(TDuration::MilliSeconds(500));
        auto listResponse = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse.SessionIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listResponse.SessionIds[0], sessionId);
    }

    Y_UNIT_TEST(ManualCloseSessionClearsSession) {
        TFmrTestSetup setup;
        TString sessionId = "test-manual-close-session";
        auto coordinator = setup.GetFmrCoordinator(TFmrCoordinatorSettings(), sessionId);

        auto listResponse1 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse1.SessionIds.size(), 1);

        // Manually clearing session
        coordinator->ClearSession({.SessionId = sessionId}).GetValueSync();

        // Session should be cleared
        auto listResponse2 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse2.SessionIds.size(), 0);
    }

    Y_UNIT_TEST(SessionFailureDetection) {
        TFmrTestSetup setup;
        auto coordinatorSettings = TFmrCoordinatorSettings();
        auto timeProvider = MakeIntrusive<TMockTimeProvider>(TDuration::MilliSeconds(50));
        coordinatorSettings.TimeProvider = timeProvider;
        coordinatorSettings.SessionInactivityTimeout = TDuration::MilliSeconds(400);
        coordinatorSettings.TimeToSleepBetweenCheckWorkerStatusRequests = TDuration::MilliSeconds(50);
        coordinatorSettings.HealthCheckInterval = TDuration::MilliSeconds(100);
        TString sessionId = "test-gateway-failure-session";

        auto coordinator = setup.GetFmrCoordinator(coordinatorSettings, sessionId);

        auto listResponse1 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse1.SessionIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listResponse1.SessionIds[0], sessionId);

        // Pinging session to keep it active
        coordinator->PingSession({.SessionId = sessionId}).GetValueSync();
        timeProvider->Advance(TDuration::MilliSeconds(150));
        coordinator->PingSession({.SessionId = sessionId}).GetValueSync();

        auto listResponse2 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse2.SessionIds.size(), 1);

        // Stopping pings to simulate failure
        timeProvider->Advance(TDuration::MilliSeconds(800), TDuration::MilliSeconds(500));

        auto listResponse3 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse3.SessionIds.size(), 0);
    }
}

} // namespace NYql::NFmr
