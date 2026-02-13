#pragma once

#include "data_storage.h"
#include "keyvalue_client.h"
#include "run_stats.h"
#include "types.h"

#include <ydb/tests/stress/kv_volume/protos/config.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <atomic>
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
        TRunStats& stats);

    void Run();

private:
    bool IsStopped() const;
    void WaitForActions();
    void PeriodicLoop(const TString& actionName, ui32 periodUs);
    void ScheduleAction(const TString& actionName);
    void ExecuteAction(const TString& actionName);
    void WriteInitialData();

    void ExecuteWriteCommand(const TString& actionName, const NKikimrKeyValue::ActionCommand_Write& writeCommand);
    void ExecuteReadCommand(const NKikimrKeyValue::Action& action, const NKikimrKeyValue::ActionCommand_Read& readCommand);
    void ExecuteDeleteCommand(const NKikimrKeyValue::Action& action, const NKikimrKeyValue::ActionCommand_Delete& deleteCommand);

    TVector<TString> ResolveSources(const NKikimrKeyValue::Action& action) const;
    ui32 SelectPartitionId();
    TString GetPatternData(ui32 size);

private:
    const ui32 WorkerId_;
    const TOptions& Options_;
    const NKikimrKeyValue::KeyValueVolumeStressLoad& Config_;
    const TString VolumePath_;
    TRunStats& Stats_;
    std::unique_ptr<IKeyValueClient> Client_;

    std::atomic<bool> StopRequested_ = false;
    const std::chrono::steady_clock::time_point EndAt_;

    TDataStorage DataStorage_;

    THashMap<TString, const NKikimrKeyValue::Action*> ActionsByName_;
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

    TVector<std::thread> Schedulers_;
};

} // namespace NKvVolumeStress
