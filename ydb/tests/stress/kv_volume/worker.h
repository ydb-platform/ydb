#pragma once

#include "action_pool.h"
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
#include <functional>
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
    ~TWorker();

    void LoadInitialData();
    void Run(std::chrono::steady_clock::time_point endAt);

private:
    using TExecutionContextPtr = TExecutionContext::TPtr;
    using TCommandDone = std::function<void()>;

    struct TActionEntry {
        const NKikimrKeyValue::Action* Action = nullptr;
        TString Name;
        ui32 ActionId = 0;
        ui32 StatsIndex = 0;
        ui32 Limit = 0;
        std::optional<ui32> ParentActionId;
        std::optional<ui32> SourceActionId;
    };

    struct alignas(64) TAlignedActionCounter {
        std::atomic<ui32> Value = 0;
    };

    struct TActionExecutionState {
        ui32 ActionId = 0;
        ui32 ActionStatsIndex = 0;
        TExecutionContextPtr ExecutionContext;
        std::chrono::steady_clock::time_point StartedAt;
        size_t NextCommandIndex = 0;
        bool Finished = false;
    };

    struct TReadCommandState {
        ui32 ActionStatsIndex = 0;
        ui32 ReadSize = 0;
        bool VerifyData = false;
        TVector<std::pair<TString, TKeyInfo>> Keys;
        size_t NextKeyIndex = 0;
        TCommandDone Done;
    };

    struct TDeleteCommandState {
        ui32 ActionStatsIndex = 0;
        TVector<std::pair<TString, TKeyInfo>> Keys;
        size_t NextKeyIndex = 0;
        TCommandDone Done;
    };

    enum class EScheduleResult {
        Scheduled,
        LimitReached,
        Stopped,
        Failed,
    };

    struct alignas(64) TActionSlotSync {
        std::mutex Mutex;
        std::condition_variable Cv;
        std::atomic<bool> Waiter = false;
    };

    bool IsStopped() const;
    ui32 GetActionLimit(const NKikimrKeyValue::Action& action) const;
    ui32 GetActionPoolSize() const;
    void StopSchedulers();
    void WaitForActions();
    TExecutionContextPtr CreateExecutionContext(ui32 actionId, TExecutionContextPtr parentContext);
    TExecutionContextPtr FindNearestAncestorAction(
        const TExecutionContextPtr& context,
        ui32 actionId) const;
    TVector<std::pair<TString, TKeyInfo>> PickSourceKeys(
        const TActionEntry& actionEntry,
        const TExecutionContextPtr& context,
        ui32 count,
        bool erase);
    void PeriodicLoop(
        ui32 actionId,
        ui32 periodUs,
        std::chrono::steady_clock::time_point endAt);
    EScheduleResult TryScheduleAction(ui32 actionId, TExecutionContextPtr parentContext = nullptr);
    void ScheduleAction(ui32 actionId, TExecutionContextPtr parentContext = nullptr);
    bool WaitForActionSlot(ui32 actionId, std::chrono::steady_clock::time_point endAt);
    void ExecuteActionAsync(
        ui32 actionId,
        ui32 actionStatsIndex,
        TExecutionContextPtr executionContext,
        std::chrono::steady_clock::time_point startedAt);
    void ContinueAction(const std::shared_ptr<TActionExecutionState>& state);
    void FinishAction(const std::shared_ptr<TActionExecutionState>& state, bool recordAction);
    void DecreaseActiveActions();
    void WriteInitialDataImpl();

    bool ExecuteWriteCommand(
        const TString& actionName,
        const NKikimrKeyValue::ActionCommand_Write& writeCommand,
        std::optional<ui32> actionStatsIndex,
        const TExecutionContextPtr& executionContext);
    void ExecuteWriteCommandAsync(
        const TString& actionName,
        const NKikimrKeyValue::ActionCommand_Write& writeCommand,
        std::optional<ui32> actionStatsIndex,
        const TExecutionContextPtr& executionContext,
        TCommandDone done);
    void ExecuteReadCommandAsync(
        const TActionEntry& actionEntry,
        ui32 actionStatsIndex,
        const TExecutionContextPtr& executionContext,
        const NKikimrKeyValue::ActionCommand_Read& readCommand,
        TCommandDone done);
    void ContinueReadCommand(const std::shared_ptr<TReadCommandState>& state);
    void ExecuteDeleteCommandAsync(
        const TActionEntry& actionEntry,
        ui32 actionStatsIndex,
        const TExecutionContextPtr& executionContext,
        const NKikimrKeyValue::ActionCommand_Delete& deleteCommand,
        TCommandDone done);
    void ContinueDeleteCommand(const std::shared_ptr<TDeleteCommandState>& state);
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

    TVector<TActionEntry> Actions_;
    THashMap<TString, ui32> ActionIdByName_;
    TVector<TVector<ui32>> ChildrenByActionId_;

    std::optional<ui32> FixedPartitionId_;

    std::unique_ptr<TAlignedActionCounter[]> RunningByAction_;
    std::unique_ptr<TActionSlotSync[]> ActionSlotSync_;

    std::unique_ptr<TActionPool> ActionPool_;

    alignas(64) std::mutex PatternCacheMutex_;
    alignas(64) THashMap<ui32, TString> PatternCache_;

    alignas(64) std::atomic<ui64> WriteKeyCounter_ = 0;
    alignas(64) std::atomic<ui64> ExecutionIdCounter_ = 1;
    alignas(64) std::atomic<ui64> ActiveActions_ = 0;
    alignas(64) ui32 ActionCapacity_ = 0;
    ui32 ActionPoolSize_ = 1;
    std::mutex ActiveActionsMutex_;
    std::condition_variable ActiveActionsCv_;

    TVector<std::thread> Schedulers_;
};

} // namespace NKvVolumeStress
