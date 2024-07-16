#pragma once
#include "defs.h"
#include "tablet_flat_executor.h"
#include "flat_executor_misc.h"
#include "flat_store_bundle.h"
#include "flat_exec_broker.h"
#include "logic_redo_eggs.h"
#include "util_fmt_line.h"
#include <ydb/core/base/localdb.h>
#include <library/cpp/time_provider/time_provider.h>
#include <ydb/core/base/memory_controller_iface.h>

namespace NKikimr {

namespace NTable{
    class TScheme;
}

namespace NTabletFlatExecutor {

class TTableSnapshotContext;

using TCompactionPolicy = NLocalDb::TCompactionPolicy;

enum class EForceCompaction {
    Mem,
    Borrowed,
    Full,
};

enum class ECompactionState {
    Unknown,
    Free,
    Pending,
    PendingBackground,
    Compaction,
    SnapshotPending,
    SnapshotCompaction,
};

enum class EForcedCompactionState {
    None,
    PendingMem,
    CompactingMem,
};

struct TCompactionLogicState {
    struct TCompactionTask {
        ui64 TaskId = 0;
        ui32 Priority = 0;
        TInstant SubmissionTimestamp;
        ui64 CompactionId = 0;
    };

    struct TInMem {
        ui64 EstimatedSize = 0;
        ui32 Steps = 0;
        ui32 CompactingSteps = 0;
        ECompactionState State = ECompactionState::Free;
        TCompactionTask CompactionTask;
        ui64 LogOverheadCount = 0;
        ui64 LogOverheadSize = 0;
        float OverloadFactor = 0.0;
    };

    struct TSnapRequest {
        const NTable::TSnapEdge Edge;
        TIntrusivePtr<TTableSnapshotContext> Context;

        TSnapRequest(NTable::TSnapEdge edge, TTableSnapshotContext *context)
            : Edge(edge)
            , Context(context)
        {}

        ~TSnapRequest();
    };

    struct TTableInfo {
        ui32 TableId = Max<ui32>();

        TInMem InMem;

        // This identifies currently active strategy type
        // The default value is used as a marker for uninitialized strategies
        NKikimrSchemeOp::ECompactionStrategy StrategyType = NKikimrSchemeOp::CompactionStrategyUnset;

        THolder<NTable::ICompactionStrategy> Strategy;

        TIntrusivePtr<NMemory::IMemTableMemoryConsumer> MemTableMemoryConsumer;

        TDeque<TSnapRequest> SnapRequests;

        TIntrusiveConstPtr<TCompactionPolicy> Policy;

        EForcedCompactionState ForcedCompactionState = EForcedCompactionState::None;
        bool ForcedCompactionQueued = false;

        EForceCompaction ForcedCompactionMode = EForceCompaction::Full;
        EForceCompaction ForcedCompactionQueuedMode = EForceCompaction::Full;

        // monotonically growing, i.e. edge-like
        ui64 CurrentForcedMemCompactionId = 0;

        bool ChangesRequested = false;

        bool AllowBorrowedGarbageCompaction = false;

        TTableInfo() = default;

        ~TTableInfo();

        ui32 ComputeBackgroundPriority(const TCompactionLogicState::TCompactionTask &task,
                                                 const TCompactionPolicy::TBackgroundPolicy &policy,
                                                 ui32 percentage,
                                                 TInstant now) const;
        ui32 ComputeBackgroundPriority(const TCompactionLogicState::TInMem &inMem,
                                                 const TCompactionPolicy &policy,
                                                 TInstant now) const;
    };

    struct TSnapshotState {
        ui64 InMemSteps = 0;

        NTable::TCompactionState State;

        // This identifies the last known strategy that matches the state above
        // The default value is used for compatibility, i.e. tablets that did
        // not have any strategy markers in their history are assumed to have
        // used the generational compaction.
        NKikimrSchemeOp::ECompactionStrategy Strategy = NKikimrSchemeOp::CompactionStrategyGenerational;
    };

    TMap<ui32, TTableInfo> Tables;
    THashMap<ui32, TSnapshotState> Snapshots;
};

class TFlatTableScan;

struct TTableCompactionResult {
    NTable::TCompactionChanges Changes;
    NKikimrSchemeOp::ECompactionStrategy Strategy;
    TVector<TIntrusivePtr<TTableSnapshotContext>> CompleteSnapshots;
    bool MemCompacted = false;
};

struct TTableCompactionChanges {
    ui32 Table;
    NTable::TCompactionChanges Changes;
    NKikimrSchemeOp::ECompactionStrategy Strategy;
};

struct TReflectSchemeChangesResult {
    struct TStrategyChange {
        ui32 Table;
        NKikimrSchemeOp::ECompactionStrategy Strategy;
    };

    TVector<TStrategyChange> StrategyChanges;
};

class TCompactionLogic {
    NTable::IMemTableMemoryConsumersCollection * const MemTableMemoryConsumersCollection;
    NUtil::ILogger * const Logger;
    NTable::IResourceBroker * const Broker;
    NTable::ICompactionBackend * const Backend;
    ITimeProvider * const Time = nullptr;
    TAutoPtr<TCompactionLogicState> State;
    TString TaskNameSuffix;

    // Update background compaction task when priority changes
    // at least by 5% (1/20 of current value).
    static constexpr ui32 PRIORITY_UPDATE_FACTOR = 20;

    void SubmitCompactionTask(ui32 table, ui32 generation,
                              const TString &type, ui32 priority,
                              TCompactionLogicState::TCompactionTask &task);
    void UpdateCompactionTask(const TString &type, ui32 priority,
                              TCompactionLogicState::TCompactionTask &task);

    bool BeginMemTableCompaction(ui64 taskId, ui32 tableId);

    THolder<NTable::ICompactionStrategy> CreateStrategy(ui32 tableId, NKikimrSchemeOp::ECompactionStrategy);

    void StopTable(TCompactionLogicState::TTableInfo &table);
    void StrategyChanging(TCompactionLogicState::TTableInfo &table);

    TCompactionLogicState::TTableInfo* HandleCompaction(
        ui64 compactionId,
        const NTable::TCompactionParams* params,
        TTableCompactionResult* ret);

public:
    static constexpr ui32 BAD_PRIORITY = Max<ui32>();

    TCompactionLogic(
        NTable::IMemTableMemoryConsumersCollection*,
        NUtil::ILogger*,
        NTable::IResourceBroker*,
        NTable::ICompactionBackend*,
        TAutoPtr<TCompactionLogicState>,
        TString taskSuffix = { });
    ~TCompactionLogic();

    void Start();
    void Stop();

    TCompactionLogicState::TSnapshotState SnapToLog(ui32 tableId);

    // Update priorities for background compaction tasks.
    void UpdateCompactions();

    // Strategy of this table wants to apply some changes
    void RequestChanges(ui32 tableId);
    TVector<TTableCompactionChanges> ApplyChanges();

    //
    void PrepareTableSnapshot(ui32 table, NTable::TSnapEdge edge, TTableSnapshotContext *snapContext);

    // Force compaction support
    // See slightly simlified state diagram: jing.yandex-team.ru/files/eivanov89/ForcedCompactionPath.png
    // or img/ForcedCompactionQueue.drawio
    bool PrepareForceCompaction();
    ui64 PrepareForceCompaction(ui32 table, EForceCompaction mode = EForceCompaction::Full);

    void TriggerSharedPageCacheMemTableCompaction(ui32 table, ui64 expectedSize);

    TFinishedCompactionInfo GetFinishedCompactionInfo(ui32 table);

    void AllowBorrowedGarbageCompaction(ui32 table);

    TReflectSchemeChangesResult ReflectSchemeChanges();
    void ProvideMemTableMemoryConsumer(ui32 table, TIntrusivePtr<NMemory::IMemTableMemoryConsumer> memTableMemoryConsumer);
    void ReflectRemovedRowVersions(ui32 table);
    void UpdateInMemStatsStep(ui32 table, ui32 steps, ui64 size);
    void CheckInMemStats(ui32 table);
    void UpdateLogUsage(TArrayRef<const NRedo::TUsage>);
    void UpdateLogUsage(const NRedo::TUsage&);
    float GetOverloadFactor() const;
    ui64 GetBackingSize() const;
    ui64 GetBackingSize(ui64 ownerTabletId) const;

    TTableCompactionResult CompleteCompaction(
        ui64 compactionId,
        THolder<NTable::TCompactionParams> params,
        THolder<NTable::TCompactionResult> result);

    void CancelledCompaction(
        ui64 compactionId,
        THolder<NTable::TCompactionParams> params);

    void BorrowedPart(ui32 tableId, NTable::TPartView partView);
    void BorrowedPart(ui32 tableId, TIntrusiveConstPtr<NTable::TColdPart> part);
    ui32 BorrowedPartLevel();

    TTableCompactionChanges RemovedParts(ui32 tableId, TArrayRef<const TLogoBlobID> parts);

    void OutputHtml(IOutputStream &out, const NTable::TScheme &scheme, const TCgiParameters& cgi);
};

}}
