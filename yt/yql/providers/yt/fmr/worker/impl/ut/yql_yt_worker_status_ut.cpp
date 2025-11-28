#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <util/system/mutex.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>


namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(FmrWorkerStatusTests) {

    Y_UNIT_TEST(WorkerStartTransition) {
        auto coordinator = MakeFmrCoordinator(TFmrCoordinatorSettings(), MakeFileYtCoordinatorService());
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics()};
            }
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };
        TFmrJobFactorySettings settings{.NumThreads = 1, .Function = func};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        // Initially worker should be in Stopped state
        UNIT_ASSERT(worker->GetWorkerState() == EFmrWorkerRuntimeState::Stopped);
        // Start the worker
        worker->Start();

        // Give some time for the thread to start
        Sleep(TDuration::MilliSeconds(100));

        // Worker should be in Running state
        UNIT_ASSERT(worker->GetWorkerState() == EFmrWorkerRuntimeState::Running);

        worker->Stop();
        UNIT_ASSERT(worker->GetWorkerState() == EFmrWorkerRuntimeState::Stopped);
    }

}

} // namespace NYql::NFmr
