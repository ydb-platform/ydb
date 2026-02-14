#pragma once

#include "data_storage.h"
#include "initial_load_progress.h"
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
    struct TActionEntry {
        const NKikimrKeyValue::Action* Action = nullptr;
        ui32 StatsIndex = 0;
    };

    bool IsStopped() const;
    ui32 GetActionLimit(const NKikimrKeyValue::Action& action) const;
    void WaitForActions();
    void PeriodicLoop(
        const TString& actionName,
        ui32 periodUs,
        std::chrono::steady_clock::time_point endAt);
    void ScheduleAction(const TString& actionName);
    void ExecuteAction(const TString& actionName);
    void WriteInitialDataImpl();

    bool ExecuteWriteCommand(
        const TString& actionName,
        const NKikimrKeyValue::ActionCommand_Write& writeCommand,
        std::optional<ui32> actionStatsIndex = std::nullopt);
    void ExecuteReadCommand(
        const NKikimrKeyValue::Action& action,
        ui32 actionStatsIndex,
        const NKikimrKeyValue::ActionCommand_Read& readCommand);
    void ExecuteDeleteCommand(
        const NKikimrKeyValue::Action& action,
        ui32 actionStatsIndex,
        const NKikimrKeyValue::ActionCommand_Delete& deleteCommand);

    TVector<TString> ResolveSources(const NKikimrKeyValue::Action& action) const;
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

    TDataStorage DataStorage_;

    THashMap<TString, TActionEntry> ActionsByName_;
    THashMap<TString, TVector<TString>> ChildrenByParent_;

    std::optional<ui32> FixedPartitionId_;

    std::mutex RunningByActionMutex_;
    THashMap<TString, ui32> RunningByAction_;

    std::atomic<ui64> ActiveActions_ = 0;
    std::mutex ActiveActionsMutex_;
    std::condition_variable ActiveActionsCv_;

    std::mutex PatternCacheMutex_;
    THashMap<ui32, TString> PatternCache_;

    std::atomic<ui64> WriteKeyCounter_ = 0;
    ui32 ActionCapacity_ = 0;

    TVector<std::thread> Schedulers_;
};

} // namespace NKvVolumeStress
