#include <yt/yql/providers/yt/fmr/coordinator/impl/ut/yql_yt_coordinator_ut.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(FmrWorkerStatusTests) {

    Y_UNIT_TEST(WorkerStartTransition) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();

        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics()};
            }
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };

        auto worker = setup.GetFmrWorker(coordinator, 3, func, TFmrTestSetup::WorkerSettings, false);
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
