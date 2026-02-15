#pragma once

#include "execution_context.h"
#include "initial_load_progress.h"
#include "key_bucket.h"
#include "keyvalue_client.h"
#include "run_stats.h"
#include "types.h"
#include "worker_load.h"

#include <ydb/tests/stress/kv_volume/protos/config.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <atomic>
#include <csignal>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

namespace NKvVolumeStress {

class TWorker {
public:
    TWorker(
        ui32 workerId,
        const TOptions& options,
        const NKikimrKeyValue::KeyValueVolumeStressLoad& config,
        const TString& hostPort,
        const TString& volumePath,
        TRunStats& stats,
        TInitialLoadProgress* initialLoadProgress = nullptr,
        TWorkerLoadTracker* workerLoadTracker = nullptr,
        const volatile std::sig_atomic_t* stopSignal = nullptr);

    void LoadInitialData();
    void Run(std::chrono::steady_clock::time_point endAt);

private:
    using TExecutionContextPtr = TExecutionContext::TPtr;

    struct TActionEntry {
        const NKikimrKeyValue::Action* Action = nullptr;
        ui32 StatsIndex = 0;
    };

    struct TActionTask {
        TString ActionName;
        TExecutionContextPtr ExecutionContext;
    };

    struct alignas(64) TActionQueue {
        std::mutex Mutex;
        std::condition_variable Cv;
        std::deque<TActionTask> Pending;
    };

    bool IsStopped() const;
    ui32 GetActionLimit(const NKikimrKeyValue::Action& action) const;
    ui32 GetActionPoolSize() const;
    void StartActionPool();
    void StopActionPool();
    void ActionWorkerLoop(ui32 queueIndex);
    void StopSchedulers();
    void WaitForActions();
    TExecutionContextPtr CreateExecutionContext(const TString& actionName, TExecutionContextPtr parentContext);
    TExecutionContextPtr FindNearestAncestorAction(
        const TExecutionContextPtr& context,
        const TString& actionName) const;
    TVector<std::pair<TString, TKeyInfo>> PickSourceKeys(
        const NKikimrKeyValue::Action& action,
        const TExecutionContextPtr& context,
        ui32 count,
        bool erase);
    void PeriodicLoop(
        const TString& actionName,
        ui32 periodUs,
        std::chrono::steady_clock::time_point endAt);
    void ScheduleAction(const TString& actionName, TExecutionContextPtr parentContext = nullptr);
    void ExecuteAction(const TString& actionName, TExecutionContextPtr executionContext);
    void WriteInitialDataImpl();

    bool ExecuteWriteCommand(
        const TString& actionName,
        const NKikimrKeyValue::ActionCommand_Write& writeCommand,
        std::optional<ui32> actionStatsIndex,
        const TExecutionContextPtr& executionContext);
    void ExecuteReadCommand(
        const NKikimrKeyValue::Action& action,
        ui32 actionStatsIndex,
        const TExecutionContextPtr& executionContext,
        const NKikimrKeyValue::ActionCommand_Read& readCommand);
    void ExecuteDeleteCommand(
        const NKikimrKeyValue::Action& action,
        ui32 actionStatsIndex,
        const TExecutionContextPtr& executionContext,
        const NKikimrKeyValue::ActionCommand_Delete& deleteCommand);
    ui32 SelectPartitionId();
    TString GetPatternData(ui32 size);

private:
    const ui32 WorkerId_;
    const TOptions& Options_;
    const NKikimrKeyValue::KeyValueVolumeStressLoad& Config_;
    const TString VolumePath_;
    TRunStats& Stats_;
    TInitialLoadProgress* const InitialLoadProgress_;
    TWorkerLoadTracker* const WorkerLoadTracker_;
    const volatile std::sig_atomic_t* const StopSignal_;
    std::unique_ptr<IKeyValueClient> Client_;

    std::atomic<bool> StopRequested_ = false;

    TKeyBucket WorkerDataStorage_;
    TExecutionContextPtr InitialContext_;

    THashMap<TString, TActionEntry> ActionsByName_;
    THashMap<TString, TVector<TString>> ChildrenByParent_;

    std::optional<ui32> FixedPartitionId_;

    alignas(64) std::mutex RunningByActionMutex_;
    alignas(64) THashMap<TString, ui32> RunningByAction_;

    alignas(64) std::atomic<ui64> ActiveActions_ = 0;
    alignas(64) std::mutex ActiveActionsMutex_;
    alignas(64) std::condition_variable ActiveActionsCv_;

    alignas(64) std::atomic<bool> ActionQueueStopRequested_ = false;
    alignas(64) std::atomic<ui64> NextActionQueueIndex_ = 0;
    alignas(64) TVector<std::unique_ptr<TActionQueue>> ActionQueues_;
    alignas(64) TVector<std::thread> ActionWorkers_;

    alignas(64) std::mutex PatternCacheMutex_;
    alignas(64) THashMap<ui32, TString> PatternCache_;

    alignas(64) std::atomic<ui64> WriteKeyCounter_ = 0;
    alignas(64) std::atomic<ui64> ExecutionIdCounter_ = 1;
    alignas(64) ui32 ActionCapacity_ = 0;
    ui32 ActionPoolSize_ = 1;

    TVector<std::thread> Schedulers_;
};

} // namespace NKvVolumeStress
