#pragma once

#include "defs.h"
#include "datashard.h"
#include <ydb/core/tx/locks/locks.h>
#include "datashard_outreadset.h"
#include "datashard_snapshots.h"
#include "execution_unit_kind.h"
#include "change_collector.h"

#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/engine/minikql/change_collector_iface.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/balance_coverage/balance_coverage_builder.h>
#include <ydb/core/tx/tx_processing.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>

namespace NKikimr {
namespace NDataShard {

using NTabletFlatExecutor::TTableSnapshotContext;

class TDataShard;

enum class ERestoreDataStatus {
    Ok,
    Restart,
    Error,
};

enum class ETxOrder {
    Unknown,
    Before,
    After,
    Any,
};

enum class EMvccTxMode {
    ReadOnly,
    ReadWrite,
};

struct TStepOrder {
    ui64 Step;
    ui64 TxId;

    TStepOrder(ui64 step, ui64 txId)
        : Step(step)
        , TxId(txId)
    {}

    bool operator == (const TStepOrder& s) const {
        return Step == s.Step && TxId == s.TxId;
    }

    bool operator != (const TStepOrder& s) const {
        return Step != s.Step || TxId != s.TxId;
    }

    bool operator < (const TStepOrder& s) const {
        if (Step == s.Step)
            return TxId < s.TxId;
        return Step < s.Step;
    }

    bool operator <= (const TStepOrder& s) const {
        return (*this) < s || (*this) == s;
    }

    ETxOrder CheckOrder(const TStepOrder& stepTxId) const {
        Y_ABORT_UNLESS(*this != stepTxId); // avoid self checks
        if (!Step && !stepTxId.Step) // immediate vs immediate
            return ETxOrder::Any;
        if (!Step || !stepTxId.Step) // planned vs immediate
            return ETxOrder::Unknown;
        return (*this < stepTxId) ? ETxOrder::Before : ETxOrder::After;
    }

    std::pair<ui64, ui64> ToPair() const { return std::pair<ui64, ui64>(Step, TxId); }

    TRowVersion ToRowVersion() const {
        return TRowVersion(Step, TxId);
    }

    TString ToString() const {
        return TStringBuilder() << Step << ':' << TxId;
    }
};

struct TOperationKey {
    ui64 TableId;
    TOwnedTableRange Key;

    TOperationKey(ui64 tableId, TOwnedTableRange key)
        : TableId(tableId)
        , Key(std::move(key))
    { }
};

enum class EOperationKind : ui32 {
    Unknown = 0,

    // Values [1, 100) are used to map NKikimrTxDataShard::ETransactionKind.
    DataTx = NKikimrTxDataShard::ETransactionKind::TX_KIND_DATA,
    SchemeTx = NKikimrTxDataShard::ETransactionKind::TX_KIND_SCHEME,
    ReadTable = NKikimrTxDataShard::ETransactionKind::TX_KIND_SCAN,
    Snapshot = NKikimrTxDataShard::ETransactionKind::TX_KIND_SNAPSHOT,
    DistributedErase = NKikimrTxDataShard::ETransactionKind::TX_KIND_DISTRIBUTED_ERASE,
    CommitWrites = NKikimrTxDataShard::ETransactionKind::TX_KIND_COMMIT_WRITES,

    // Values [100, inf) are used for internal kinds.
    DirectTx = 101,
    ReadTx = 102,
    WriteTx = 103,
};

class TBasicOpInfo {
public:
    TBasicOpInfo()
        : Kind(EOperationKind::Unknown)
        , Flags(0)
        , GlobalTxId(0)
        , Step(0)
        , MinStep(0)
        , MaxStep(0)
        , TieBreakerIndex(0)
    {
    }

    TBasicOpInfo(EOperationKind kind,
                 ui64 flags,
                 ui64 maxStep,
                 TInstant receivedAt,
                 ui64 tieBreakerIndex)
        : TBasicOpInfo(0, kind, flags, maxStep, receivedAt, tieBreakerIndex)
    {
    }

    TBasicOpInfo(ui64 txId,
                 EOperationKind kind,
                 ui64 flags,
                 ui64 maxStep,
                 TInstant receivedAt,
                 ui64 tieBreakerIndex)
        : Kind(kind)
        , Flags(flags)
        , GlobalTxId(txId)
        , Step(0)
        , ReceivedAt(receivedAt)
        , MinStep(0)
        , MaxStep(maxStep)
        , TieBreakerIndex(tieBreakerIndex)
    {
    }

    TBasicOpInfo(const TBasicOpInfo &other) = default;
    TBasicOpInfo(TBasicOpInfo &&other) = default;

    TBasicOpInfo &operator=(const TBasicOpInfo &other) = default;
    TBasicOpInfo &operator=(TBasicOpInfo &&other) = default;

    ////////////////////////////
    //     OPERATION KIND     //
    ////////////////////////////
    EOperationKind GetKind() const { return Kind; }

    bool IsDataTx() const { return Kind == EOperationKind::DataTx; }
    bool IsReadTx() const { return Kind == EOperationKind::ReadTx; }
    bool IsWriteTx() const { return Kind == EOperationKind::WriteTx; }
    bool IsDirectTx() const { return Kind == EOperationKind::DirectTx; }
    bool IsSchemeTx() const { return Kind == EOperationKind::SchemeTx; }
    bool IsReadTable() const { return Kind == EOperationKind::ReadTable; }
    bool IsSnapshotTx() const { return Kind == EOperationKind::Snapshot; }
    bool IsDistributedEraseTx() const { return Kind == EOperationKind::DistributedErase; }
    bool IsCommitWritesTx() const { return Kind == EOperationKind::CommitWrites; }
    bool Exists() const { return Kind != EOperationKind::Unknown; }

    /////////////////////////////
    //     OPERATION FLAGS     //
    /////////////////////////////
    ui64 GetFlags() const { return Flags; }

    bool HasFlag(ui64 flag) const { return Flags & flag; }
    void SetFlag(ui64 flag, bool val = true)
    {
        if (val)
            Flags |= flag;
        else
            Flags &= ~flag;
    }
    void ResetFlag(ui64 flag) { SetFlag(flag, false); }

    bool HasDirtyFlag() const { return HasFlag(TTxFlags::Dirty); }
    void SetDirtyFlag(bool val = true) { SetFlag(TTxFlags::Dirty, val); }
    void ResetDirtyFlag() { ResetFlag(TTxFlags::Dirty); }

    bool HasForceDirtyFlag() const { return HasFlag(TTxFlags::ForceDirty); }
    void SetForceDirtyFlag(bool val = true) { SetFlag(TTxFlags::ForceDirty, val); }
    void ResetForceDirtyFlag() { ResetFlag(TTxFlags::ForceDirty); }

    bool IsDirty() const { return HasDirtyFlag() || HasForceDirtyFlag(); }

    bool HasDenyOnlineIfSnapshotNotReadyFlag() const { return HasFlag(TTxFlags::DenyOnlineIfSnapshotNotReady); }
    void SetDenyOnlineIfSnapshotNotReadyFlag(bool val = true) { SetFlag(TTxFlags::DenyOnlineIfSnapshotNotReady, val); }
    void ResetDenyOnlineIfSnapshotNotReadyFlag() { ResetFlag(TTxFlags::DenyOnlineIfSnapshotNotReady); }

    bool HasForceOnlineFlag() const { return HasFlag(TTxFlags::ForceOnline); }
    void SetForceOnlineFlag(bool val = true) { SetFlag(TTxFlags::ForceOnline, val); }
    void ResetForceOnlineFlag() { ResetFlag(TTxFlags::ForceOnline); }

    bool HasImmediateFlag() const { return HasFlag(TTxFlags::Immediate); }
    void SetImmediateFlag(bool val = true) { SetFlag(TTxFlags::Immediate, val); }
    void ResetImmediateFlag() { ResetFlag(TTxFlags::Immediate); }

    bool HasVolatilePrepareFlag() const { return HasFlag(TTxFlags::VolatilePrepare); }
    void SetVolatilePrepareFlag(bool val = true) { SetFlag(TTxFlags::VolatilePrepare, val); }
    void ResetVolatilePrepareFlag() { ResetFlag(TTxFlags::VolatilePrepare); }

    bool IsImmediate() const { return HasImmediateFlag() && !HasForceOnlineFlag() && !HasVolatilePrepareFlag(); }

    bool HasInProgressFlag() const { return HasFlag(TTxFlags::InProgress); }
    void SetInProgressFlag(bool val = true) { SetFlag(TTxFlags::InProgress, val); }
    void ResetInProgressFlag() { ResetFlag(TTxFlags::InProgress); }

    bool IsInProgress() const { return HasInProgressFlag(); }
    void IncrementInProgress() {
        if (0 == InProgressCount++) {
            SetInProgressFlag();
        }
    }
    void DecrementInProgress() {
        if (0 == --InProgressCount) {
            ResetInProgressFlag();
        }
    }

    bool HasCompletedFlag() const { return HasFlag(TTxFlags::Completed); }
    void SetCompletedFlag(bool val = true) { SetFlag(TTxFlags::Completed, val); }
    void ResetCompletedFlag() { ResetFlag(TTxFlags::Completed); }

    bool IsCompleted() const { return HasCompletedFlag(); }

    bool HasInterruptedFlag() const { return HasFlag(TTxFlags::Interrupted); }
    void SetInterruptedFlag(bool val = true) { SetFlag(TTxFlags::Interrupted, val); }
    void ResetInterruptedFlag() { ResetFlag(TTxFlags::Interrupted); }

    bool IsInterrupted() const { return HasInterruptedFlag(); }

    bool HasAbortedFlag() const { return HasFlag(TTxFlags::Aborted); }
    void SetAbortedFlag(bool val = true) { SetFlag(TTxFlags::Aborted, val); }
    void ResetAbortedFlag() { ResetFlag(TTxFlags::Aborted); }

    bool IsAborted() const { return HasAbortedFlag(); }

    bool HasReadOnlyFlag() const { return HasFlag(TTxFlags::ReadOnly); }
    void SetReadOnlyFlag(bool val = true)
    {
        Y_ABORT_UNLESS(!val || !IsGlobalWriter());
        SetFlag(TTxFlags::ReadOnly, val);
    }
    void ResetReadOnlyFlag() { ResetFlag(TTxFlags::ReadOnly); }

    bool IsReadOnly() const { return HasReadOnlyFlag(); }

    bool HasProposeBlockerFlag() const { return HasFlag(TTxFlags::ProposeBlocker); }
    void SetProposeBlockerFlag(bool val = true) { SetFlag(TTxFlags::ProposeBlocker, val); }
    void ResetProposeBlockerFlag() { ResetFlag(TTxFlags::ProposeBlocker); }

    bool IsProposeBlocker() const { return HasProposeBlockerFlag(); }

    bool HasNeedDiagnosticsFlag() const { return HasFlag(TTxFlags::NeedDiagnostics); }
    void SetNeedDiagnosticsFlag(bool val = true) { SetFlag(TTxFlags::NeedDiagnostics, val); }
    void ResetNeedDiagnosticsFlag() { ResetFlag(TTxFlags::NeedDiagnostics); }

    bool HasGlobalReaderFlag() const { return HasFlag(TTxFlags::GlobalReader); }
    void SetGlobalReaderFlag(bool val = true) { SetFlag(TTxFlags::GlobalReader, val); }
    void ResetGlobalReaderFlag() { ResetFlag(TTxFlags::GlobalReader); }

    bool IsGlobalReader() const { return HasGlobalReaderFlag(); }

    bool HasGlobalWriterFlag() const { return HasFlag(TTxFlags::GlobalWriter); }
    void SetGlobalWriterFlag(bool val = true) {
        Y_ABORT_UNLESS(!val || !IsReadOnly());
        SetFlag(TTxFlags::GlobalWriter, val);
    }
    void ResetGlobalWriterFlag() { ResetFlag(TTxFlags::GlobalWriter); }

    bool IsGlobalWriter() const { return HasGlobalWriterFlag(); }

    bool HasWaitingDependenciesFlag() const { return HasFlag(TTxFlags::WaitingDependencies); }
    void SetWaitingDependenciesFlag(bool val = true) { SetFlag(TTxFlags::WaitingDependencies, val); }
    void ResetWaitingDependenciesFlag() { ResetFlag(TTxFlags::WaitingDependencies); }

    bool IsWaitingDependencies() const { return HasWaitingDependenciesFlag(); }

    bool HasExecutingFlag() const { return HasFlag(TTxFlags::Executing); }
    void SetExecutingFlag(bool val = true) { SetFlag(TTxFlags::Executing, val); }
    void ResetExecutingFlag() { ResetFlag(TTxFlags::Executing); }

    bool IsExecuting() const { return HasExecutingFlag(); }

    bool HasUsingSnapshotFlag() const { return HasFlag(TTxFlags::UsingSnapshot); }
    void SetUsingSnapshotFlag(bool val = true) { SetFlag(TTxFlags::UsingSnapshot, val); }
    void ResetUsingSnapshotFlag() { ResetFlag(TTxFlags::UsingSnapshot); }

    bool IsUsingSnapshot() const { return HasUsingSnapshotFlag(); }

    bool HasKqpDataTransactionFlag() const { return HasFlag(TTxFlags::KqpDataTransaction); }
    void SetKqpDataTransactionFlag(bool val = true) { SetFlag(TTxFlags::KqpDataTransaction, val); }
    void ResetKqpDataTransactionFlag() { ResetFlag(TTxFlags::KqpDataTransaction); }
    bool IsKqpDataTransaction() const { return HasKqpDataTransactionFlag(); }

    bool HasKqpAttachedRSFlag() const { return HasFlag(TTxFlags::KqpAttachedRS); }
    void SetKqpAttachedRSFlag(bool val = true) { SetFlag(TTxFlags::KqpAttachedRS, val); }
    void ResetKqpAttachedRSFlag() { ResetFlag(TTxFlags::KqpAttachedRS); }

    bool HasLoadedInRSFlag() const { return HasFlag(TTxFlags::LoadedInRS); }
    void SetLoadedInRSFlag(bool val = true) { SetFlag(TTxFlags::LoadedInRS, val); }
    void ResetLoadedInRSFlag() { ResetFlag(TTxFlags::LoadedInRS); }

    bool HasKqpScanTransactionFlag() const { return HasFlag(TTxFlags::KqpScanTransaction); }
    void SetKqpScanTransactionFlag(bool val = true) { SetFlag(TTxFlags::KqpScanTransaction, val); }
    void ResetKqpScanTransactionFlag() { ResetFlag(TTxFlags::KqpScanTransaction); }
    bool IsKqpScanTransaction() const { return HasKqpScanTransactionFlag(); }

    bool HasWaitingForStreamClearanceFlag() const { return HasFlag(TTxFlags::WaitingForStreamClearance); }
    void SetWaitingForStreamClearanceFlag(bool val = true) { SetFlag(TTxFlags::WaitingForStreamClearance, val); }
    void ResetWaitingForStreamClearanceFlag() { ResetFlag(TTxFlags::WaitingForStreamClearance); }
    bool IsWaitingForStreamClearance() const { return HasWaitingForStreamClearanceFlag(); }

    bool HasProcessDisconnectsFlag() const { return HasFlag(TTxFlags::ProcessDisconnects); }
    void SetProcessDisconnectsFlag(bool val = true) { SetFlag(TTxFlags::ProcessDisconnects, val); }
    void ResetProcessDisconnectsFlag() { ResetFlag(TTxFlags::ProcessDisconnects); }

    bool HasWaitingForScanFlag() const { return HasFlag(TTxFlags::WaitingForScan); }
    void SetWaitingForScanFlag(bool val = true) { SetFlag(TTxFlags::WaitingForScan, val); }
    void ResetWaitingForScanFlag() { ResetFlag(TTxFlags::WaitingForScan); }
    bool IsWaitingForScan() const { return HasWaitingForScanFlag(); }

    bool HasWaitingForAsyncJobFlag() const { return HasFlag(TTxFlags::WaitingForAsyncJob); }
    void SetWaitingForAsyncJobFlag(bool val = true) { SetFlag(TTxFlags::WaitingForAsyncJob, val); }
    void ResetWaitingForAsyncJobFlag() { ResetFlag(TTxFlags::WaitingForAsyncJob); }
    bool IsWaitingForAsyncJob() const { return HasWaitingForAsyncJobFlag(); }

    bool HasWaitingForSnapshotFlag() const { return HasFlag(TTxFlags::WaitingForSnapshot); }
    void SetWaitingForSnapshotFlag(bool val = true) { SetFlag(TTxFlags::WaitingForSnapshot, val); }
    void ResetWaitingForSnapshotFlag() { ResetFlag(TTxFlags::WaitingForSnapshot); }
    bool IsWaitingForSnapshot() const { return HasWaitingForSnapshotFlag(); }

    bool HasWaitingForRestartFlag() const { return HasFlag(TTxFlags::WaitingForRestart); }
    void SetWaitingForRestartFlag(bool val = true) { SetFlag(TTxFlags::WaitingForRestart, val); }
    void ResetWaitingForRestartFlag() { ResetFlag(TTxFlags::WaitingForRestart); }
    bool IsWaitingForRestart() const { return HasWaitingForRestartFlag(); }

    bool HasResultSentFlag() const { return HasFlag(TTxFlags::ResultSent); }
    void SetResultSentFlag(bool val = true) { SetFlag(TTxFlags::ResultSent, val); }

    bool HasStoredFlag() const { return HasFlag(TTxFlags::Stored); }
    void SetStoredFlag(bool val = true) { SetFlag(TTxFlags::Stored, val); }

    bool HasWaitCompletionFlag() const { return HasFlag(TTxFlags::WaitCompletion); }
    void SetWaitCompletionFlag(bool val = true) { SetFlag(TTxFlags::WaitCompletion, val); }

    bool HasWaitingForGlobalTxIdFlag() const { return HasFlag(TTxFlags::WaitingForGlobalTxId); }
    void SetWaitingForGlobalTxIdFlag(bool val = true) { SetFlag(TTxFlags::WaitingForGlobalTxId, val); }

    ///////////////////////////////////
    //     OPERATION ID AND PLAN     //
    ///////////////////////////////////
    ui64 GetTxId() const { return GlobalTxId ? GlobalTxId : TieBreakerIndex; }
    ui64 GetGlobalTxId() const { return GlobalTxId; }
    void SetGlobalTxId(ui64 txId) { GlobalTxId = txId; }

    ui64 GetStep() const { return Step; }
    void SetStep(ui64 step) { Step = step; }
    ui64 GetPredictedStep() const { return PredictedStep; }
    void SetPredictedStep(ui64 step) { PredictedStep = step; }

    TStepOrder GetStepOrder() const { return TStepOrder(GetStep(), GetTxId()); }

    ui64 GetMinStep() const { return MinStep; }
    void SetMinStep(ui64 step) { MinStep = step; }

    ui64 GetMaxStep() const { return MaxStep; }
    void SetMaxStep(ui64 step) { MaxStep = step; }

    ui64 GetTieBreakerIndex() const { return TieBreakerIndex; }

    TInstant GetReceivedAt() const { return ReceivedAt; }

    bool HasAcquiredSnapshotKey() const { return HasFlag(TTxFlags::AcquiredSnapshotReference); }
    const TSnapshotKey& GetAcquiredSnapshotKey() const { return AcquiredSnapshotKey; }

    void SetAcquiredSnapshotKey(const TSnapshotKey& key) {
        AcquiredSnapshotKey = key;
        SetFlag(TTxFlags::AcquiredSnapshotReference);
    }

    void ResetAcquiredSnapshotKey() {
        ResetFlag(TTxFlags::AcquiredSnapshotReference);
    }

    bool IsMvccSnapshotRead() const { return !MvccSnapshot.IsMax(); }
    const TRowVersion& GetMvccSnapshot() const { return MvccSnapshot; }
    bool IsMvccSnapshotRepeatable() const { return MvccSnapshotRepeatable_; }
    void SetMvccSnapshot(const TRowVersion& snapshot, bool isRepeatable = true) {
        MvccSnapshot = snapshot;
        MvccSnapshotRepeatable_ = isRepeatable;
    }

    bool IsProposeResultSentEarly() const { return ProposeResultSentEarly_; }
    void SetProposeResultSentEarly(bool value = true) { ProposeResultSentEarly_ = value; }

    bool GetPerformedUserReads() const { return PerformedUserReads_; }
    void SetPerformedUserReads(bool value = true) { PerformedUserReads_ = value; }

    ///////////////////////////////////
    //     DEBUG AND MONITORING      //
    ///////////////////////////////////
    void Serialize(NKikimrTxDataShard::TBasicOpInfo &info) const;

protected:
    EOperationKind Kind;
    // See TTxFlags.
    ui64 Flags;
    ui64 GlobalTxId;
    ui64 Step;
    ui64 PredictedStep = 0;
    TInstant ReceivedAt;

    ui64 MinStep;
    ui64 MaxStep;
    ui64 TieBreakerIndex;
    ui64 InProgressCount = 0;

    TSnapshotKey AcquiredSnapshotKey;
    TRowVersion MvccSnapshot = TRowVersion::Max();

private:
    // Runtime flags
    ui8 MvccSnapshotRepeatable_ : 1 = 0;
    ui8 ProposeResultSentEarly_ : 1 = 0;
    ui8 PerformedUserReads_ : 1 = 0;
};

struct TRSData {
    TString Body;
    ui64 Origin = 0;

    explicit TRSData(const TString &body = TString(),
                     ui64 origin = 0)
        : Body(body)
        , Origin(origin)
    {}
};

struct TInputOpData {
    using TEventsQueue = TQueue<TAutoPtr<IEventHandle>>;
    using TSnapshots = TVector<TIntrusivePtr<TTableSnapshotContext>>;
    using TInReadSets = TMap<std::pair<ui64, ui64>, TVector<TRSData>>;
    using TCoverageBuilders = TMap<std::pair<ui64, ui64>, std::shared_ptr<TBalanceCoverageBuilder>>;
    using TAwaitingDecisions = absl::flat_hash_set<ui64>;

    TInputOpData()
        : RemainReadSets(0)
    {
    }

    TEventsQueue Events;
    TSnapshots Snapshots;
    TLocksCache LocksCache;
    // Input read sets processing.
    TInReadSets InReadSets;
    TVector<NKikimrTx::TEvReadSet> DelayedInReadSets;
    TCoverageBuilders CoverageBuilders;
    ui32 RemainReadSets;
    TAutoPtr<IDestructable> ScanResult;
    TAutoPtr<IDestructable> AsyncJobResult;
    TAwaitingDecisions AwaitingDecisions;
};

struct TOutputOpData {
    using TResultPtr = THolder<TEvDataShard::TEvProposeTransactionResult>;
    using TDelayedAcks = TVector<THolder<IEventHandle>>;
    using TOutReadSets = TMap<std::pair<ui64, ui64>, TString>; // source:target -> body
    using TChangeRecord = IDataShardChangeCollector::TChange;
    using TExpectedReadSets = TMap<std::pair<ui64, ui64>, TStackVec<TActorId, 1>>;

    TResultPtr Result;
    // ACKs to send on successful operation completion.
    TDelayedAcks DelayedAcks;
    TOutReadSets OutReadSets;
    TExpectedReadSets ExpectedReadSets;
    TVector<THolder<TEvTxProcessing::TEvReadSet>> PreparedOutReadSets;
    // Access log of checked locks
    TLocksCache LocksAccessLog;
    // Collected change records
    TVector<TChangeRecord> ChangeRecords;
};

struct TExecutionProfile {
    struct TUnitProfile {
        TDuration WaitTime;
        TDuration ExecuteTime;
        TDuration CommitTime;
        TDuration CompleteTime;
        // Time spent in unit waiting for previous unit commit
        // (which returned DelayComplete).
        TDuration DelayedCommitTime;
        ui32 ExecuteCount;
    };

    TInstant StartExecutionAt;
    TInstant CompletedAt;
    TInstant StartUnitAt;
    THashMap<EExecutionUnitKind, TUnitProfile> UnitProfiles;
};

class TValidatedDataTx;
class TValidatedWriteTx;

class TValidatedTx {
public:
    using TPtr = std::shared_ptr<TValidatedTx>;

    virtual ~TValidatedTx() = default;

    enum class EType { 
        DataTx,
        WriteTx 
    };

public:
    virtual EType GetType() const = 0;
    virtual ui64 GetTxId() const = 0;
    virtual ui64 GetMemoryConsumption() const = 0;

    bool IsProposed() const {
        return GetSource() != TActorId();
    }

    YDB_ACCESSOR_DEF(TActorId, Source);
    YDB_ACCESSOR_DEF(ui64, TxCacheUsage);
};

struct TOperationAllListTag {};
struct TOperationGlobalListTag {};
struct TOperationDelayedReadListTag {};
struct TOperationDelayedWriteListTag {};

class TOperation
    : public TBasicOpInfo
    , public TSimpleRefCount<TOperation>
    , public TIntrusiveListItem<TOperation, TOperationAllListTag>
    , public TIntrusiveListItem<TOperation, TOperationGlobalListTag>
    , public TIntrusiveListItem<TOperation, TOperationDelayedReadListTag>
    , public TIntrusiveListItem<TOperation, TOperationDelayedWriteListTag>
{
public:
    enum EDepFlag {
        // Two operations might use the same key(s) and
        // at least on of them use it for write. This is
        // symmetrical relation. We don't split it into
        // RAW, WAW and WAR dependencies. It leads to
        // less accurate dependencies info and therefore
        // to lost out-of-order opportunities but it also
        // reduces the analysis cost (can stop analysis
        // after the first conflict).
        DF_DATA = 1 << 0,
        // Dependent operation has higher plan step.
        DF_PLAN_STEP = 1 << 1,
        // Src modifies scheme used by Dst.
        DF_SCHEME = 1 << 2,
        // Dependent operation cannot be proposed.
        DF_BLOCK_PROPOSE = 1 << 3,
        // Dependent operation cannot be executed.
        DF_BLOCK_EXECUTE = 1 << 4,
    };

    using TPtr = TIntrusivePtr<TOperation>;
    using EResultStatus = NKikimrTxDataShard::TEvProposeTransactionResult::EStatus;

    virtual ~TOperation() = default;

    TActorId GetTarget() const { return Target; }
    void SetTarget(TActorId target) { Target = target; }

    ui64 GetCookie() const { return Cookie; }
    void SetCookie(ui64 cookie) { Cookie = cookie; }

public:
    template<class TTag>
    bool IsInList() const {
        using TItem = TIntrusiveListItem<TOperation, TTag>;
        return !static_cast<const TItem*>(this)->Empty();
    }

    template<class TTag>
    void UnlinkFromList() {
        using TItem = TIntrusiveListItem<TOperation, TTag>;
        static_cast<TItem*>(this)->Unlink();
    }

public:
    TDuration GetTotalElapsed() const {
        return TDuration::Seconds(TotalTimer.Passed());
    }

    void ResetCurrentTimer() {
        CurrentTimer.Reset();
    }

    TDuration GetCurrentElapsed() const {
        return TDuration::Seconds(CurrentTimer.Passed());
    }

    TDuration GetCurrentElapsedAndReset() {
        return TDuration::Seconds(CurrentTimer.PassedReset());
    }

public:
    ////////////////////////////////////////
    //             INPUT DATA             //
    ////////////////////////////////////////
    TInputOpData::TEventsQueue &InputEvents() { return InputDataRef().Events; }
    void AddInputEvent(TAutoPtr<IEventHandle> ev) { InputEvents().push(ev); }
    bool HasPendingInputEvents() const
    {
        return InputData ? !InputData->Events.empty() : false;
    }

    TInputOpData::TSnapshots &InputSnapshots() { return InputDataRef().Snapshots; }
    void AddInputSnapshot(TIntrusivePtr<TTableSnapshotContext> snapContext)
    {
        InputSnapshots().push_back(snapContext);
    }

    TLocksCache &LocksCache() { return InputDataRef().LocksCache; }

    TInputOpData::TInReadSets &InReadSets() { return InputDataRef().InReadSets; }
    TVector<NKikimrTx::TEvReadSet> &DelayedInReadSets() { return InputDataRef().DelayedInReadSets; }
    TInputOpData::TCoverageBuilders &CoverageBuilders() { return InputDataRef().CoverageBuilders; }
    void InitRemainReadSets() { InputDataRef().RemainReadSets = InReadSets().size(); }
    ui32 GetRemainReadSets() const { return InputData ? InputData->RemainReadSets : 0; }

    void AddInReadSet(const NKikimrTx::TEvReadSet &rs);
    void AddInReadSet(const TReadSetKey &rsKey,
                      const NKikimrTx::TBalanceTrackList &btList,
                      TString readSet);

    void AddDelayedInReadSet(const NKikimrTx::TEvReadSet &rs)
    {
        DelayedInReadSets().emplace_back(rs);
    }

    TAutoPtr<IDestructable> &ScanResult() { return InputDataRef().ScanResult; }
    void SetScanResult(TAutoPtr<IDestructable> prod) { InputDataRef().ScanResult = prod; }
    bool HasScanResult() const { return InputData ? (bool)InputData->ScanResult : false; }

    TAutoPtr<IDestructable> &AsyncJobResult() { return InputDataRef().AsyncJobResult; }
    void SetAsyncJobResult(TAutoPtr<IDestructable> prod) { InputDataRef().AsyncJobResult = prod; }
    bool HasAsyncJobResult() const { return InputData ? (bool)InputData->AsyncJobResult : false; }

    TInputOpData::TAwaitingDecisions &AwaitingDecisions() { return InputDataRef().AwaitingDecisions; }

    ////////////////////////////////////////
    //            OUTPUT DATA             //
    ////////////////////////////////////////
    bool HasOutputData() { return bool(OutputData); }
    TOutputOpData::TResultPtr &Result() { return OutputDataRef().Result; }

    TOutputOpData::TDelayedAcks &DelayedAcks() { return OutputDataRef().DelayedAcks; }
    void AddDelayedAck(THolder<IEventHandle> ack)
    {
        DelayedAcks().emplace_back(ack.Release());
    }

    TOutputOpData::TOutReadSets &OutReadSets() { return OutputDataRef().OutReadSets; }
    TOutputOpData::TExpectedReadSets &ExpectedReadSets() { return OutputDataRef().ExpectedReadSets; }
    TVector<THolder<TEvTxProcessing::TEvReadSet>> &PreparedOutReadSets()
    {
        return OutputDataRef().PreparedOutReadSets;
    }

    TLocksCache &LocksAccessLog() { return OutputDataRef().LocksAccessLog; }

    TVector<TOutputOpData::TChangeRecord> &ChangeRecords() { return OutputDataRef().ChangeRecords; }

    const absl::flat_hash_set<ui64> &GetAffectedLocks() const { return AffectedLocks; }
    void AddAffectedLock(ui64 lockTxId) { AffectedLocks.insert(lockTxId); }

    ////////////////////////////////////////
    //       DELAYED IMMEDIATE KEYS       //
    ////////////////////////////////////////
    void SetDelayedKnownReads(const TVector<TOperationKey>& reads) {
        DelayedKnownReads.insert(DelayedKnownReads.end(), reads.begin(), reads.end());
    }

    void SetDelayedKnownWrites(const TVector<TOperationKey>& writes) {
        DelayedKnownWrites.insert(DelayedKnownWrites.end(), writes.begin(), writes.end());
    }

    TVector<TOperationKey> RemoveDelayedKnownReads() {
        return std::move(DelayedKnownReads);
    }

    TVector<TOperationKey> RemoveDelayedKnownWrites() {
        return std::move(DelayedKnownWrites);
    }

    ////////////////////////////////////////
    //            DEPENDENCIES            //
    ////////////////////////////////////////
    const absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> &GetDependents() const { return Dependents; }
    const absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> &GetDependencies() const { return Dependencies; }
    const absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> &GetSpecialDependents() const { return SpecialDependents; }
    const absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> &GetSpecialDependencies() const { return SpecialDependencies; }
    const absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> &GetPlannedConflicts() const { return PlannedConflicts; }
    const absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> &GetImmediateConflicts() const { return ImmediateConflicts; }
    const absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> &GetRepeatableReadConflicts() const { return RepeatableReadConflicts; }
    const absl::flat_hash_set<ui64> &GetVolatileDependencies() const { return VolatileDependencies; }
    bool HasVolatileDependencies() const { return !VolatileDependencies.empty(); }
    bool GetVolatileDependenciesAborted() const { return VolatileDependenciesAborted; }

    void AddDependency(const TOperation::TPtr &op);
    void AddSpecialDependency(const TOperation::TPtr &op);
    void AddImmediateConflict(const TOperation::TPtr &op);
    void PromoteImmediateConflicts();
    void PromoteImmediateWriteConflicts();

    void ClearDependents();
    void ClearDependencies();
    void ClearPlannedConflicts();
    void ClearImmediateConflicts();
    void ClearSpecialDependents();
    void ClearSpecialDependencies();

    void AddRepeatableReadConflict(const TOperation::TPtr &op);
    void PromoteRepeatableReadConflicts();
    void ClearRepeatableReadConflicts();

    void AddVolatileDependency(ui64 txId);
    void RemoveVolatileDependency(ui64 txId, bool success);
    void ClearVolatileDependenciesAborted() { VolatileDependenciesAborted = false; }

    TString DumpDependencies() const;

    /**
     * Returns true if operation has some runtime conflicts and cannot execute
     *
     * For example this may return true for immediate mutating operations after
     * conflicting planned operations create outgoing readsets, but before
     * main transaction body has completed.
     */
    bool HasRuntimeConflicts() const noexcept;

    virtual bool HasKeysInfo() const
    {
        return false;
    }
    virtual const NMiniKQL::IEngineFlat::TValidationInfo &GetKeysInfo() const
    {
        return EmptyKeysInfo;
    }
    ui64 KeysCount() const
    {
        return GetKeysInfo().ReadsCount + GetKeysInfo().WritesCount;
    }
    virtual ui64 LockTxId() const { return 0; }
    virtual ui32 LockNodeId() const { return 0; }
    virtual bool HasLockedWrites() const { return false; }

    ////////////////////////////////////////
    //           EXECUTION PLAN           //
    ////////////////////////////////////////

    // This method is to be called after operation is
    // created by parser (or loaded from local database)
    // to build its execution plan.
    // All data and flags required to build execution plan
    // should be filled prior this call.
    virtual void BuildExecutionPlan(bool loaded) = 0;

    EExecutionUnitKind GetCurrentUnit() const;
    size_t GetCurrentUnitIndex() const { return CurrentUnit; }
    const TVector<EExecutionUnitKind> &GetExecutionPlan() const;
    // Rewrite the rest of execution plan (all units going after
    // the current one) with specified units.
    void RewriteExecutionPlan(const TVector<EExecutionUnitKind> &plan);
    void RewriteExecutionPlan(EExecutionUnitKind unit);
    bool IsExecutionPlanFinished() const;
    void AdvanceExecutionPlan();

    void MarkAsExecuting()
    {
        SetExecutingFlag();
        SetStartExecutionAt(TAppData::TimeProvider->Now());
    }

    // This method is called when operation execution is completed
    // but operation still remains in pipeline. Method should cleanup
    // data not required any more keeping data required for monitoring.
    virtual void Deactivate() { ClearInputData(); }

    // Mark operation as aborted
    void Abort();

    // Mark operation as aborted and replace the rest of
    // operation's execution plan with specified unit.
    void Abort(EExecutionUnitKind unit);

    ////////////////////////////////////////
    //          EXECUTION PROFILE         //
    ////////////////////////////////////////
    const TExecutionProfile &GetExecutionProfile() const { return ExecutionProfile; }

    TInstant GetStartExecutionAt() const { return ExecutionProfile.StartExecutionAt; }
    void SetStartExecutionAt(TInstant val) { ExecutionProfile.StartExecutionAt = val; }

    TInstant GetCompletedAt() const { return ExecutionProfile.CompletedAt; }
    void SetCompletedAt(TInstant val) { ExecutionProfile.CompletedAt = val; }

    void AddExecutionTime(TDuration val)
    {
        Y_DEBUG_ABORT_UNLESS(!IsExecutionPlanFinished());
        auto &profile = ExecutionProfile.UnitProfiles[ExecutionPlan[CurrentUnit]];
        profile.ExecuteTime += val;
        ++profile.ExecuteCount;
    }

    void SetCommitTime(EExecutionUnitKind unit,
                       TDuration val)
    {
        ExecutionProfile.UnitProfiles[unit].CommitTime = val;
    }

    void SetCompleteTime(EExecutionUnitKind unit,
                         TDuration val)
    {
        ExecutionProfile.UnitProfiles[unit].CompleteTime = val;
    }

    void SetDelayedCommitTime(TDuration val)
    {
        ExecutionProfile.UnitProfiles[ExecutionPlan[CurrentUnit]].DelayedCommitTime = val;
    }

    TString ExecutionProfileLogString(ui64 tabletId) const;

    TMonotonic GetFinishProposeTs() const noexcept { return FinishProposeTs; }
    void SetFinishProposeTs(TMonotonic now) noexcept { FinishProposeTs = now; }
    void SetFinishProposeTs() noexcept;

    NWilson::TTraceId GetTraceId() const noexcept {
        return OperationSpan.GetTraceId();
    }

    /**
     * Called when datashard is going to stop soon
     *
     * Operation may override this method to support sending notifications or
     * results signalling that the operation will never complete. When result
     * is sent operation is supposed to set its ResultSentFlag.
     *
     * When this method returns true the operation will be added to the
     * pipeline as a candidate for execution.
     */
    virtual bool OnStopping(TDataShard& self, const TActorContext& ctx);

    /**
     * Called when operation is aborted on cleanup
     *
     * Distributed transaction is cleaned up when deadline is reached, and
     * it hasn't been planned yet. Additionally volatile transactions are
     * cleaned when shard is waiting for transaction queue to drain, and
     * the given operation wasn't planned yet.
     */
    virtual void OnCleanup(TDataShard& self, std::vector<std::unique_ptr<IEventHandle>>& replies);

protected:
    TOperation()
        : TOperation(TBasicOpInfo())
    {
    }

    TOperation(ui64 txId,
               EOperationKind kind,
               ui32 flags,
               ui64 maxStep,
               TInstant receivedAt,
               ui64 tieBreakerIndex)
        : TOperation(TBasicOpInfo(txId, kind, flags, maxStep, receivedAt, tieBreakerIndex))
    {
    }

    TOperation(const TBasicOpInfo &op)
        : TBasicOpInfo(op)
        , Cookie(0)
        , CurrentUnit(0)
    {
    }

    TOperation(const TOperation &other) = delete;
    TOperation(TOperation &&other) = default;

    TOutputOpData &OutputDataRef()
    {
        if (!OutputData)
            OutputData = MakeHolder<TOutputOpData>();
        return *OutputData;
    }

    TInputOpData &InputDataRef()
    {
        if (!InputData)
            InputData = MakeHolder<TInputOpData>();
        return *InputData;
    }
    void ClearInputData() { InputData = nullptr; }

    TActorId Target;

private:
    THPTimer TotalTimer;
    THPTimer CurrentTimer;
    THolder<TInputOpData> InputData;
    THolder<TOutputOpData> OutputData;
    ui64 Cookie;
    // A set of locks affected by this operation
    absl::flat_hash_set<ui64> AffectedLocks;
    // Delayed read/write keys for immediate transactions
    TVector<TOperationKey> DelayedKnownReads;
    TVector<TOperationKey> DelayedKnownWrites;
    // Bidirectional links between dependent transactions
    absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> Dependents;
    absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> Dependencies;
    absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> SpecialDependents;
    absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> SpecialDependencies;
    absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> PlannedConflicts;
    absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> ImmediateConflicts;
    absl::flat_hash_set<TOperation::TPtr, THash<TOperation::TPtr>> RepeatableReadConflicts;
    absl::flat_hash_set<ui64> VolatileDependencies;
    bool VolatileDependenciesAborted = false;
    TVector<EExecutionUnitKind> ExecutionPlan;
    // Index of current execution unit.
    size_t CurrentUnit;
    TExecutionProfile ExecutionProfile;

    TMonotonic FinishProposeTs;

    static NMiniKQL::IEngineFlat::TValidationInfo EmptyKeysInfo;

public:
    std::optional<TRowVersion> MvccReadWriteVersion;

public:
    // Orbit used for tracking operation progress
    NLWTrace::TOrbit Orbit;
    
    NWilson::TSpan OperationSpan;
};

inline IOutputStream &operator <<(IOutputStream &out,
                                  const TStepOrder &id)
{
    out << '[' << id.Step << ':' << id.TxId << ']';
    return out;
}

inline IOutputStream &operator <<(IOutputStream &out,
                                  const TOperation &op)
{
    return (out << op.GetStepOrder());
}

#define OHFunc(TEvType, HandleFunc)                                                  \
    case TEvType::EventType: {                                                      \
        typename TEvType::TPtr* x = reinterpret_cast<typename TEvType::TPtr*>(&ev); \
        HandleFunc(*x, op, ctx);                                                    \
        break;                                                                      \
    }

} // namespace NDataShard
} // namespace NKikimr
