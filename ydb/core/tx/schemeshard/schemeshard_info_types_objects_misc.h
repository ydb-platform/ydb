#pragma once

#include "schemeshard_info_types_base.h"
#include "schemeshard_info_types_table_column.h"

#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/protos/test_shard_control.pb.h>
#include <ydb/core/protos/yql_translation_settings.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TExternalTableInfo: TSimpleRefCount<TExternalTableInfo> {
    using TPtr = TIntrusivePtr<TExternalTableInfo>;

    TString SourceType;
    TString DataSourcePath;
    TString Location;
    ui64 AlterVersion = 0;
    THashMap<ui32, TTableColumn> Columns;
    TString Content;
};

struct TExternalDataSourceInfo: TSimpleRefCount<TExternalDataSourceInfo> {
    using TPtr = TIntrusivePtr<TExternalDataSourceInfo>;

    ui64 AlterVersion = 0;
    TString SourceType;
    TString Location;
    TString Installation;
    NKikimrSchemeOp::TAuth Auth;
    NKikimrSchemeOp::TExternalTableReferences ExternalTableReferences;
    NKikimrSchemeOp::TExternalDataSourceProperties Properties;

    void FillProto(NKikimrSchemeOp::TExternalDataSourceDescription& proto, bool withReferences = true) const {
        proto.SetVersion(AlterVersion);
        proto.SetSourceType(SourceType);
        proto.SetLocation(Location);
        proto.SetInstallation(Installation);
        proto.MutableAuth()->CopyFrom(Auth);
        proto.MutableProperties()->CopyFrom(Properties);
        if (withReferences) {
            proto.MutableReferences()->CopyFrom(ExternalTableReferences);
        }
    }
};

struct TViewInfo : TSimpleRefCount<TViewInfo> {
    using TPtr = TIntrusivePtr<TViewInfo>;

    ui64 AlterVersion = 0;
    TString QueryText;
    NYql::NProto::TTranslationSettings CapturedContext;
};

struct TResourcePoolInfo : TSimpleRefCount<TResourcePoolInfo> {
    using TPtr = TIntrusivePtr<TResourcePoolInfo>;

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TResourcePoolProperties Properties;
};

struct TBackupCollectionInfo : TSimpleRefCount<TBackupCollectionInfo> {
    using TPtr = TIntrusivePtr<TBackupCollectionInfo>;

    static TPtr New() {
        return new TBackupCollectionInfo();
    }

    static TPtr Create(const NKikimrSchemeOp::TBackupCollectionDescription& desc) {
        TPtr result = New();

        result->Description = desc;

        return result;
    }

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TBackupCollectionDescription Description;
};

struct TSysViewInfo : TSimpleRefCount<TSysViewInfo> {
    using TPtr = TIntrusivePtr<TSysViewInfo>;

    ui64 AlterVersion = 0;
    NKikimrSysView::ESysViewType Type;
};

struct TIncrementalRestoreState {
    using EState = EIncrementalRestoreState;

    EState State = EState::Running;

    TPathId BackupCollectionPathId;
    ui64 OriginalOperationId = 0;

    struct TIncrementalBackup {
        TPathId BackupPathId;
        TString BackupPath;
        ui64 Timestamp;
        bool Completed = false;

        TIncrementalBackup(const TPathId& pathId, const TString& path, ui64 timestamp)
            : BackupPathId(pathId), BackupPath(path), Timestamp(timestamp)
        {}
    };

    // Table operation state for tracking DataShard completion
    struct TTableOperationState {
        TOperationId OperationId;
        THashSet<TShardIdx> ExpectedShards;
        THashSet<TShardIdx> CompletedShards;
        THashSet<TShardIdx> FailedShards;

        // When true, completion is detected from per-shard reports rather than
        // Self->Operations.contains(txId) (no schema transaction was proposed).
        bool RequestsDispatched = false;

        TTableOperationState() = default;
        explicit TTableOperationState(const TOperationId& opId) : OperationId(opId) {}

        bool AllShardsComplete() const {
            return CompletedShards.size() + FailedShards.size() == ExpectedShards.size() &&
                    !ExpectedShards.empty();
        }

        bool HasFailures() const {
            return !FailedShards.empty();
        }

        bool HasNonRetriableFailure = false;

        // Idempotent against re-delivery: the first terminal report wins.
        bool RecordShardResult(TShardIdx shardIdx, bool success, bool retriable = true) {
            if (CompletedShards.contains(shardIdx) || FailedShards.contains(shardIdx)) {
                return false;
            }
            if (success) {
                CompletedShards.insert(shardIdx);
            } else {
                FailedShards.insert(shardIdx);
                if (!retriable) {
                    HasNonRetriableFailure = true;
                }
            }
            return true;
        }
    };

    // Pending sub-op dispatched in batches bounded by MaxIncrementalRestoreTablesInFlight.
    // Both Table and Index sub-ops share the queue so indexes don't fan out unbounded.
    struct TPendingRestoreOp {
        enum class EKind { Table, Index };
        EKind Kind = EKind::Table;

        TString BackupName;
        TString TablePath;

        TString IndexName;
        TString TargetTablePath;
        TString SpecificImplTableName;
    };

    // Sorted by timestamp.
    TVector<TIncrementalBackup> IncrementalBackups;
    ui32 CurrentIncrementalIdx = 0;
    bool CurrentIncrementalStarted = false;

    bool RetryNeeded = false;

    // Two-tier wall-clock deadline anchors. Persisted across reboots so the
    // budget cannot be defeated by a reboot loop.
    TInstant RestoreStartedAt;
    TInstant CurrentStageStartedAt;

    // Two-phase backoff guard: set when a retry is scheduled, cleared when it fires.
    // Prevents concurrent completion events from double-counting retries.
    bool RetryScheduled = false;
    TInstant NextRetryAttemptAt;

    bool NonRetriableFailure = false;

    // Set by TTxInit when retryNeeded=true. The first post-reboot retry-fire
    // absorbs failed sub-ops as completed rather than re-dispatching (pre-reboot
    // scan data is authoritative; re-dispatch won't get a fresh reply).
    bool FreshBootRetryAbsorbPending = false;

    THashSet<TOperationId> InProgressOperations;
    THashSet<TOperationId> CompletedOperations;

    THashMap<TOperationId, TTableOperationState> TableOperations;

    struct TPerShardDispatch {
        TPathId SrcPathId;
        TPathId DstPathId;
        ui64 SchemeShardGeneration = 0;
        THashMap<TShardIdx, TTabletId> ShardTablets;
    };
    THashMap<TOperationId, TPerShardDispatch> ShardDispatchByOp;

    // Populated by ProcessNextIncrementalBackup, drained by DispatchPendingIncrementalRestoreTables.
    // In-memory only; rebuilt after reboot from backup-collection contents.
    TDeque<TPendingRestoreOp> PendingTables;

    // ui32 maps to Ydb::StatusIds::StatusCode (0 == STATUS_CODE_UNSPECIFIED);
    // typed here to avoid pulling Ydb::StatusIds into the header.
    ui32 FinalStatus = 0;
    TString FinalIssues;

    // Per-sub-op tracking. Items move from PendingItems to InFlightItems once
    // TxAllocatorClient supplies a TxId.
    struct TItem {
        using EKind = EIncrementalRestoreItemKind;
        ui32 ItemSeq = 0;
        EKind Kind = EKind::Table;
        TPathId TablePathId;
        TPathId SrcTablePathId; // 0/0 for Finalize items or unresolved src path
        // 0 == awaiting allocation.
        ui64 WaitTxId = 0;

        // Filled in and sent once TxAllocatorClient replies. In-memory only;
        // rebuilt on reboot via the orchestrator's re-entry path.
        TAutoPtr<NActors::IEventBase> PendingRequest;
    };

    ui32 NextItemSeq = 0;
    TDeque<TItem> PendingItems;
    THashMap<ui32 /*ItemSeq*/, TItem> InFlightItems;
    THashMap<ui64 /*WaitTxId*/, ui32 /*ItemSeq*/> WaitTxIdToItemSeq;

    // Returns progress within the current incremental as a fraction [0.0, 1.0]
    // based on per-shard completion across all table operations
    float CalcCurrentIncrementalProgress() const {
        ui32 totalShards = 0;
        ui32 doneShards = 0;
        for (const auto& [_, tableOp] : TableOperations) {
            totalShards += tableOp.ExpectedShards.size();
            doneShards += tableOp.CompletedShards.size() + tableOp.FailedShards.size();
        }
        if (totalShards == 0) {
            return 0.0f;
        }
        return static_cast<float>(doneShards) / totalShards;
    }

    bool AllIncrementsProcessed() const {
        return CurrentIncrementalIdx >= IncrementalBackups.size();
    }

    bool IsCurrentIncrementalComplete() const {
        return CurrentIncrementalIdx < IncrementalBackups.size() &&
                IncrementalBackups[CurrentIncrementalIdx].Completed;
    }

    bool AreAllCurrentOperationsComplete() const {
        // If we started processing the current incremental but there are no operations at all,
        // it means no table backups were found in this incremental backup, so consider it complete
        // TODO: probably have to ensure that empty backups are impossible
        if (CurrentIncrementalStarted && InProgressOperations.empty() && CompletedOperations.empty()
            && PendingTables.empty() && PendingItems.empty()) {
            return true;
        }
        // All in-progress ops completed and no sub-ops still queued or awaiting allocation.
        return InProgressOperations.empty()
            && PendingTables.empty()
            && PendingItems.empty()
            && !CompletedOperations.empty();
    }

    void MarkCurrentIncrementalComplete() {
        if (CurrentIncrementalIdx < IncrementalBackups.size()) {
            IncrementalBackups[CurrentIncrementalIdx].Completed = true;
        }
    }

    void MoveToNextIncremental() {
        if (CurrentIncrementalIdx < IncrementalBackups.size()) {
            CurrentIncrementalIdx++;
            CurrentIncrementalStarted = false;

            InProgressOperations.clear();
            CompletedOperations.clear();
            // TableOperations.clear() also clears HasNonRetriableFailure on each table.
            TableOperations.clear();
            PendingTables.clear();

            // Stage deadline anchor reset is owned by the caller (HandleAllOperationsComplete)
            // because it needs ctx.Now() and a paired db.Update.
            RetryScheduled = false;
            NextRetryAttemptAt = TInstant::Zero();
            NonRetriableFailure = false;
        }
    }

    const TIncrementalBackup* GetCurrentIncremental() const {
        if (CurrentIncrementalIdx < IncrementalBackups.size()) {
            return &IncrementalBackups[CurrentIncrementalIdx];
        }

        return nullptr;
    }

    void AddIncrementalBackup(const TPathId& pathId, const TString& path, ui64 timestamp) {
        IncrementalBackups.emplace_back(pathId, path, timestamp);

        // Sort by timestamp to ensure chronological order
        std::sort(IncrementalBackups.begin(), IncrementalBackups.end(),
                    [](const TIncrementalBackup& a, const TIncrementalBackup& b) {
                        return a.Timestamp < b.Timestamp;
                    });
    }

    void AddCurrentIncrementalOperation(const TOperationId& opId) {
        InProgressOperations.insert(opId);
    }

    void MarkOperationComplete(const TOperationId& opId) {
        InProgressOperations.erase(opId);
        CompletedOperations.insert(opId);
    }

    bool AllCurrentIncrementalOperationsComplete() const {
        return InProgressOperations.empty() && !CompletedOperations.empty();
    }
};

struct TIncrementalBackupItem {
    enum class EState: ui8 {
        Invalid = 0,
        Transferring = 1,
        Dropping = 230,
        Done = 240,
        Cancellation = 250,
        Cancelled = 251,
    };

    TPathId PathId;
    EState State;

    bool IsDone() const {
        return State == EState::Done;
    }
};

struct TIncrementalBackupInfo : public TSimpleRefCount<TIncrementalBackupInfo> {
    using TPtr = TIntrusivePtr<TIncrementalBackupInfo>;
    using TItem = TIncrementalBackupItem;

    enum class EState: ui8 {
        Invalid = 0,
        Transferring = 1,
        Done = 240,
        Cancellation = 250,
        Cancelled = 251,
    };

    ui64 Id;
    EState State;
    TPathId DomainPathId;

    THashMap<TPathId, TItem> Items;

    TMaybe<TString> UserSID;
    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    explicit TIncrementalBackupInfo(
            const ui64 id,
            const TPathId domainPathId)
        : Id(id)
        , DomainPathId(domainPathId)
    {}

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsCancelled() const {
        return State == EState::Cancelled;
    }

    bool IsFinished() const {
        return IsDone() || IsCancelled();
    }

    bool IsAllItemsDone() const {
        for (const auto& item : Items) {
            if (!item.second.IsDone()) {
                return false;
            }
        }
        return true;
    }
};

// Trackable full backup op (the aggregator over a BackupBackupCollection's
// CCT). Mirrors TIncrementalBackupInfo with three changes:
//   - Adds Failed terminal state (header + item).
//   - Stores BackupCollectionPathId so reboot can rebuild
//     Self->BCPathToFullBackup from non-terminal rows.
//   - Stores FinalIssues for hard-fail diagnostics surfaced via GET.
struct TFullBackupItem {
    enum class EState: ui8 {
        Invalid = 0,
        Transferring = 1,
        Failed = 230,
        Done = 240,
    };

    TPathId PathId;
    EState State;

    bool IsDone() const {
        return State == EState::Done;
    }
};

struct TFullBackupInfo : public TSimpleRefCount<TFullBackupInfo> {
    using TPtr = TIntrusivePtr<TFullBackupInfo>;
    using TItem = TFullBackupItem;

    enum class EState: ui8 {
        Invalid = 0,
        Transferring = 1,
        Failed = 230,
        Done = 240,
    };

    ui64 Id;
    EState State;
    TPathId DomainPathId;
    TPathId BackupCollectionPathId;

    // Planned number of items (base tables + non-omitted index impl tables).
    // Advisory only (drives the progress percentage); the terminal state is
    // decided by control op completion, not by this count.
    ui32 ExpectedItemCount = 0;

    THashMap<TPathId, TItem> Items;

    TMaybe<TString> UserSID;
    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();
    TString FinalIssues;

    explicit TFullBackupInfo(
            const ui64 id,
            const TPathId domainPathId)
        : Id(id)
        , State(EState::Invalid)
        , DomainPathId(domainPathId)
    {}

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsFailed() const {
        return State == EState::Failed;
    }

    bool IsFinished() const {
        return IsDone() || IsFailed();
    }

    bool IsAllItemsDone() const {
        for (const auto& item : Items) {
            if (!item.second.IsDone()) {
                return false;
            }
        }
        return true;
    }

    // True if any observed item is in the Failed terminal state. Drives the
    // Done-vs-Failed choice when operation completion finalizes the header
    // (see FinalizeFullBackupOnOpComplete).
    bool HasAnyFailed() const {
        for (const auto& [_, item] : Items) {
            if (item.State == TItem::EState::Failed) {
                return true;
            }
        }
        return false;
    }
};

struct TSecretInfo : TSimpleRefCount<TSecretInfo> {
    using TPtr = TIntrusivePtr<TSecretInfo>;

    TSecretInfo(const ui64 alterVersion)
        : AlterVersion(alterVersion)
    {
    }

    TSecretInfo(const ui64 alterVersion, NKikimrSchemeOp::TSecretDescription&& desc)
        : AlterVersion(alterVersion)
        , Description(std::move(desc))
    {
    }

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);

        TPtr result = new TSecretInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;

        return result;
    }

    static TPtr New() {
        return new TSecretInfo(0);
    }

    static TPtr Create(NKikimrSchemeOp::TSecretDescription&& desc) {
        TPtr result = New();
        TPtr alterData = result->CreateNextVersion();
        alterData->Description = std::move(desc);

        return result;
    }

    ui64 AlterVersion = 0;
    TSecretInfo::TPtr AlterData = nullptr;
    NKikimrSchemeOp::TSecretDescription Description;
};

struct TStreamingQueryInfo : TSimpleRefCount<TStreamingQueryInfo> {
    using TPtr = TIntrusivePtr<TStreamingQueryInfo>;

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TStreamingQueryProperties Properties;
};

struct TTestShardSetInfo : public TSimpleRefCount<TTestShardSetInfo> {
    using TPtr = TIntrusivePtr<TTestShardSetInfo>;

    NKikimrClient::TTestShardControlRequest::TCmdInitialize CmdInitialize;
    THashMap<TShardIdx, TTabletId> TestShards; // ShardIdx -> TabletId
    ui64 AlterVersion = 0;

    explicit TTestShardSetInfo(ui64 alterVersion)
        : AlterVersion(alterVersion)
    {}
};

// namespace NForcedCompaction {
struct TForcedCompactionInfo : TSimpleRefCount<TForcedCompactionInfo> {
    using TPtr = TIntrusivePtr<TForcedCompactionInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        InProgress = 1,
        Done = 2,
        Cancelled = 3,
        Cancelling = 4,
    };

    ui64 Id;  // TxId from the original TEvCreateRequest
    EState State = EState::Invalid;
    TPathId TablePathId;
    TPathId SubdomainPathId;
    bool Cascade;
    ui32 MaxShardsInFlight;

    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    TMaybe<TString> UserSID;

    THashSet<TPathId> TablesToCompact;
    ui32 TotalShardCount = 0;
    ui32 DoneShardCount = 0; // updates only when persisting

    THashSet<TShardIdx> ShardsInFlight;

    TSet<TActorId> Subscribers;

    bool IsFinished() const;
    void AddNotifySubscriber(const TActorId& actorId);
    float CalcProgress() const;
};
// } // NForcedCompaction

} // namespace NSchemeShard
} // namespace NKikimr
