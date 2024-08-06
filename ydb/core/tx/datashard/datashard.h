#pragma once

#include "datashard_s3_download.h"
#include "datashard_s3_upload.h"

#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/message_seqno.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tablet_flat/flat_row_versions.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include <library/cpp/lwtrace/shuttle.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>

namespace arrow {

class RecordBatch;

}

namespace NKikimr {

namespace NDataShard {
    using TShardState = NKikimrTxDataShard::EDatashardState;

    struct TTxFlags {
        enum Flags : ui64 {
            ////////////////////////////
            // Public operation flags //
            ////////////////////////////
            Default = 0,
            Dirty = 0x01,
            DenyOnlineIfSnapshotNotReady = 0x02,
            ForceOnline = 0x04,
            Immediate = 0x08,

            // Transaction must be prepared as a ditributed transaction, but
            // must also be volatile, i.e. expect that other participants may
            // cancel it even after it is planned.
            VolatilePrepare = 0x10,

            PublicFlagsMask = 0x000000000000FFFF,

            //////////////////////////////
            // Internal operation flags //
            //////////////////////////////
            ForceDirty = 1ULL << 16,
            // Operation is currently executed within some
            // tablet transaction
            InProgress = 1ULL << 17,
            // Operation execution was completed. No more
            // local database accesses will be made for it
            Completed = 1ULL << 18,
            // Operation execution was interrupted by
            // external event
            Interrupted = 1ULL << 19,
            // Operation execution was aborted due to
            // internal error
            Aborted = 1ULL << 20,
            // Operation doesn't modify user data (operation
            // execution may still require system data
            // modification like storing operation in local
            // database)
            ReadOnly = 1ULL << 21,
            // Operation can block proposal of some
            // other operations
            ProposeBlocker = 1ULL << 22,
            // Operation requests additional diagnostics
            // in its response
            NeedDiagnostics = 1ULL << 24,
            // Operation is considered as reading
            // all shard data
            GlobalReader = 1ULL << 25,
            // Operation is considered as affecting
            // all shard data
            GlobalWriter = 1ULL << 26,
            // Operation waits for dependencies to
            // start execution
            WaitingDependencies = 1ULL << 27,
            // Operation is executing. It's considered that
            // all operations having this flag set at the
            // same time can be executed in any order. All
            // operations with this flag set should be added
            // to ExecutingOps set in pipeline. Only executing
            // operations can receive input data
            Executing = 1ULL << 28,
            // Operation is working on a snapshot and therefore
            // doesn't conflict with other operations by data
            UsingSnapshot = 1ULL << 29,
            // Operation is using new execution engine
            KqpDataTransaction = 1ULL << 30,
            // Operation attached RS to KQP task runner
            // TODO: rework execution to remove this flag
            KqpAttachedRS = 1ULL << 31,
            // All input RS for operation were loaded from
            // local database
            LoadedInRS = 1ULL << 32,
            // Operation is using new execution engine in scan mode
            KqpScanTransaction = 1ULL << 33,
            // Operation is waiting for stream clearance
            WaitingForStreamClearance = 1ULL << 34,
            // Operation is interested in interconnect disconnects
            ProcessDisconnects = 1ULL << 35,
            // Operation is waiting for async scan to finish
            WaitingForScan = 1ULL << 36,
            // Operation is waiting for snapshot
            WaitingForSnapshot = 1ULL << 37,
            // Operation is blocking conflicting immediate ops until it completes
            BlockingImmediateOps = 1ULL << 38,
            // Operation is blocking conflicting immediate writes until it completes
            BlockingImmediateWrites = 1ULL << 39,
            // Operation has acquired a reference to persistent snapshot
            AcquiredSnapshotReference = 1ULL << 40,
            // Operation's final result has already been sent
            ResultSent = 1ULL << 41,
            // Operation is confirmed to be stored in shard's local database
            Stored = 1ULL << 42,
            // Operation is waiting for async job to finish
            WaitingForAsyncJob = 1ULL << 43,
            // Operation must complete before results sending
            WaitCompletion = 1ULL << 44,
            // Waiting for global tx id allocation
            WaitingForGlobalTxId = 1ULL << 45,
            // Operation is waiting for restart
            WaitingForRestart = 1ULL << 46,
            // Operation has write keys registered in the cache
            DistributedWritesRegistered = 1ULL << 47,

            LastFlag = DistributedWritesRegistered,

            PrivateFlagsMask = 0xFFFFFFFFFFFF0000ULL,
            PreservedPrivateFlagsMask = ReadOnly | ProposeBlocker | NeedDiagnostics | GlobalReader
                | GlobalWriter | KqpDataTransaction | KqpScanTransaction
                | BlockingImmediateOps | BlockingImmediateWrites,
        };
    };

    // Old datashard uses Uint32 column type for flags in local database.
    static_assert(TTxFlags::PreservedPrivateFlagsMask <= Max<ui64>());
    static_assert(TTxFlags::PublicFlagsMask <= Max<ui32>());

    // NOTE: this switch should be modified only in tests !!!
    extern bool gAllowLogBatchingDefaultValue;
    extern TDuration gDbStatsReportInterval;
    extern ui64 gDbStatsDataSizeResolution;
    extern ui64 gDbStatsRowCountResolution;
    extern ui32 gDbStatsHistogramBucketsCount;

    // This SeqNo is used to discard outdated schema Tx requests on datashards.
    // In case of tablet restart on network disconnects SS can resend same Propose for the same schema Tx.
    // Because of this a DS might receive this Propose multiple times. In particular it might get Propose
    // for a Tx that has already been completed and erased from the queue. So the duplicate Proposal might be
    // treated as new. In order to avoid this the SS includes this SeqNo in each Proposal.
    // The SeqNo consists of SS tablet Generation (it's incremented on each SS restart) and Round within this
    // generation. The logic on DS is the following. When sending a Propose SS assings Round to it and increments
    // it's in-mem Round counter. If SS retires sending the same Propose it uses the previously assigned Round value.
    // This assigned Round value is not persisted on SS so in case of SS restart retry will be done with incremented
    // Generation and newly assigned Round. DS has LastSeenSeqNo persisted in it's local DB.
    // If it receives Propose with Generation < LastSeen.Generation it means that SS has restarted and it's
    // going to resend the Propose. If Generation == LastSeen.Generation && Round < LastSeen.Round then this is
    // an old Tx that should have already been acked (becuase SS never start new schema Tx before the previous one was
    // finised)
    struct TSchemeOpSeqNo : public TMessageSeqNo {
        explicit TSchemeOpSeqNo(ui64 gen = 0, ui64 round = 0)
            : TMessageSeqNo(gen, round)
        {}

        explicit TSchemeOpSeqNo(const NKikimrTxDataShard::TSchemeOpSeqNo& pb)
            : TMessageSeqNo(pb.GetGeneration(), pb.GetRound())
        {}

        TSchemeOpSeqNo& operator++() {
            if (0 == ++Round) {
                ++Generation;
            }
            return *this;
        }
    };

}

// legacy
namespace NTxDataShard {
    using NDataShard::TShardState;
    using NDataShard::TTxFlags;
}

struct TEvDataShard {
    enum EEv {
        EvProposeTransaction = EventSpaceBegin(TKikimrEvents::ES_TX_DATASHARD),
        EvCancelTransactionProposal,
        EvApplyReplicationChanges,
        EvGetReplicationSourceOffsets,

        EvProposeTransactionResult = EvProposeTransaction + 1 * 512,
        EvProposeTransactionRestart,
        EvProposeTransactionAttach,
        EvProposeTransactionAttachResult,
        EvApplyReplicationChangesResult,
        EvReplicationSourceOffsets,
        EvReplicationSourceOffsetsAck,
        EvReplicationSourceOffsetsCancel,

        EvInitDataShard = EvProposeTransaction + 4 * 512,
        EvGetShardState,
        EvReadOperationHistogram,
        EvUpdateConfig,
        EvSchemaChanged,
        EvStateChanged,
        EvCancelBackup,
        EvMigrateSchemeShardRequest,
        EvMigrateSchemeShardResponse,
        EvCancelRestore,

        EvInitDataShardResult = EvProposeTransaction + 5 * 512,
        EvGetShardStateResult,
        EvReadOperationHistogramResult,
        EvUpdateConfigResult,
        EvSchemaChangedResult,
        EvStateChangedResult,

        EvReturnBorrowedPart = 6 * 512,
        EvReturnBorrowedPartAck,

        EvInitSplitMergeDestination = EvProposeTransaction + 7*512,
        EvInitSplitMergeDestinationAck,
        EvSplit,
        EvSplitAck,
        EvSplitTransferSnapshot,
        EvSplitTransferSnapshotAck,
        EvSplitPartitioningChanged,
        EvSplitPartitioningChangedAck,

        EvGetTableStats,
        EvGetTableStatsResult,
        EvPeriodicTableStats,

        EvObjectStorageListingRequest,
        EvObjectStorageListingResponse,

        EvUploadRowsRequest,
        EvUploadRowsResponse,

        EvReadColumnsRequest,
        EvReadColumnsResponse,

        EvGetInfoRequest,
        EvGetInfoResponse,
        EvListOperationsRequest,
        EvListOperationsResponse,
        EvGetOperationRequest,
        EvGetOperationResponse,
        EvGetReadTableSinkStateRequest,
        EvGetReadTableSinkStateResponse,
        EvGetReadTableScanStateRequest,
        EvGetReadTableScanStateResponse,
        EvGetReadTableStreamStateRequest,
        EvGetReadTableStreamStateResponse,
        EvGetSlowOpProfilesRequest,
        EvGetSlowOpProfilesResponse,
        EvGetRSInfoRequest,
        EvGetRSInfoResponse,
        EvGetDataHistogramRequest,
        EvGetDataHistogramResponse,
        EvCancelFillIndex_DEPRECATED,
        EvRefreshVolatileSnapshotRequest,
        EvRefreshVolatileSnapshotResponse,
        EvDiscardVolatileSnapshotRequest,
        EvDiscardVolatileSnapshotResponse,

        EvGetS3Upload,
        EvStoreS3UploadId,
        EvS3Upload,

        EvEraseRowsRequest,
        EvEraseRowsResponse,
        EvConditionalEraseRowsRequest,
        EvConditionalEraseRowsResponse,

        EvBuildIndexCreateRequest,
        EvBuildIndexProgressResponse,

        EvGetS3DownloadInfo,
        EvStoreS3DownloadInfo,
        EvS3DownloadInfo,
        EvS3UploadRowsRequest,
        EvS3UploadRowsResponse,

        EvKqpScan,

        EvChangeS3UploadStatus,

        EvGetRemovedRowVersions, /* for tests */
        EvGetRemovedRowVersionsResult, /* for tests */

        EvCompactTable,
        EvCompactTableResult,

        EvCompactBorrowed, /* +60 */

        EvGetCompactTableStats,       /* for tests */
        EvGetCompactTableStatsResult, /* for tests */

        EvRead,
        EvReadResult,
        EvReadContinue,
        EvReadAck,
        EvReadCancel,

        EvCompactBorrowedResult,

        EvTestLoadRequest,
        EvTestLoadResponse,
        EvTestLoadFinished,

        EvGetOpenTxs, /* for tests */
        EvGetOpenTxsResult, /* for tests */

        EvCdcStreamScanRequest,
        EvCdcStreamScanResponse,

        EvOverloadReady,
        EvOverloadUnsubscribe,

        EvSampleKRequest,
        EvSampleKResponse,

        EvCheckConstraintCreateRequest,
        EvCheckConstraintProgressResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_DATASHARD), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_DATASHARD)");
    struct TEvGetShardState : public TEventPB<TEvGetShardState, NKikimrTxDataShard::TEvGetShardState,
        TEvDataShard::EvGetShardState> {
        TEvGetShardState()
        {
        }

        TEvGetShardState(const TActorId& source)
        {
            ActorIdToProto(source, Record.MutableSource());
        }

        TActorId GetSource() const {
            return ActorIdFromProto(Record.GetSource());
        }
    };

    struct TEvGetShardStateResult : public TEventPB<TEvGetShardStateResult, NKikimrTxDataShard::TEvGetShardStateResult,
        TEvDataShard::EvGetShardStateResult> {
        TEvGetShardStateResult()
        {
        }

        TEvGetShardStateResult(ui64 origin, ui32 state)
        {
            Record.SetOrigin(origin);
            Record.SetState(state);
        }

        ui64 GetOrigin() const {
            return Record.GetOrigin();
        }

        ui32 GetState() const {
            return Record.GetState();
        }
    };

    struct TEvSchemaChanged : public TEventPB<TEvSchemaChanged, NKikimrTxDataShard::TEvSchemaChanged,
                                        TEvDataShard::EvSchemaChanged> {
        TEvSchemaChanged()
        {}

        TEvSchemaChanged(const TActorId& source, ui64 origin, ui32 state, ui64 txId, ui64 step, ui32 generation) {
            ActorIdToProto(source, Record.MutableSource());
            Record.SetOrigin(origin);
            Record.SetState(state);
            Record.SetTxId(txId);
            Record.SetStep(step);
            Record.SetGeneration(generation);
        }

        TActorId GetSource() const {
            return ActorIdFromProto(Record.GetSource());
        }

        ui32 GetGeneration() const {
            return Record.GetGeneration();
        }
    };

    struct TEvSchemaChangedResult : public TEventPB<TEvSchemaChangedResult, NKikimrTxDataShard::TEvSchemaChangedResult,
                                                TEvDataShard::EvSchemaChangedResult> {
        TEvSchemaChangedResult()
        {}

        explicit TEvSchemaChangedResult(ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvStateChanged : public TEventPB<TEvStateChanged, NKikimrTxDataShard::TEvStateChanged,
                                        TEvDataShard::EvStateChanged> {
        TEvStateChanged()
        {}

        TEvStateChanged(const TActorId& source, ui64 tabletId, ui32 state) {
            ActorIdToProto(source, Record.MutableSource());
            Record.SetTabletId(tabletId);
            Record.SetState(state);
        }

        TActorId GetSource() const {
            return ActorIdFromProto(Record.GetSource());
        }
    };

    struct TEvStateChangedResult : public TEventPB<TEvStateChangedResult, NKikimrTxDataShard::TEvStateChangedResult,
                                        TEvDataShard::EvStateChangedResult> {
        TEvStateChangedResult()
        {}

        TEvStateChangedResult(ui64 tabletId, ui32 state) {
            Record.SetTabletId(tabletId);
            Record.SetState(state);
        }
    };

    struct TEvProposeTransaction : public TEventPB<TEvProposeTransaction, NKikimrTxDataShard::TEvProposeTransaction,
        TEvDataShard::EvProposeTransaction> {
        TEvProposeTransaction()
        {
        }

        TEvProposeTransaction(NKikimrTxDataShard::ETransactionKind txKind, const TActorId& source, ui64 txId,
            const TStringBuf& txBody, ui32 flags = NDataShard::TTxFlags::Default)
        {
            Record.SetTxKind(txKind);
            ActorIdToProto(source, Record.MutableSourceDeprecated());
            Record.SetTxId(txId);
            Record.SetExecLevel(0);
            Record.SetTxBody(txBody.data(), txBody.size());
            Record.SetFlags(flags);
        }

        TEvProposeTransaction(NKikimrTxDataShard::ETransactionKind txKind, const TActorId& source, ui64 txId,
            const TStringBuf& txBody, ui64 snapshotStep, ui64 snapshotTxId, ui32 flags = NDataShard::TTxFlags::Default)
            : TEvProposeTransaction(txKind, source, txId, txBody, flags)
        {
            auto &snapshot = *Record.MutableMvccSnapshot();
            snapshot.SetStep(snapshotStep);
            snapshot.SetTxId(snapshotTxId);
        }

        TEvProposeTransaction(NKikimrTxDataShard::ETransactionKind txKind, const TActorId& source, ui64 txId,
            const TStringBuf& txBody, const TRowVersion& snapshot, ui32 flags = NDataShard::TTxFlags::Default)
            : TEvProposeTransaction(txKind, source, txId, txBody, snapshot.Step, snapshot.TxId, flags)
        {
        }

        TEvProposeTransaction(NKikimrTxDataShard::ETransactionKind txKind, ui64 ssId, const TActorId& source, ui64 txId,
            const TStringBuf& txBody, const NKikimrSubDomains::TProcessingParams &processingParams, ui32 flags = NDataShard::TTxFlags::Default)
            : TEvProposeTransaction(txKind, source, txId, txBody, flags)
        {
            Y_ABORT_UNLESS(txKind == NKikimrTxDataShard::TX_KIND_SCHEME);
            Record.SetSchemeShardId(ssId);
            Record.MutableProcessingParams()->CopyFrom(processingParams);
        }

        TEvProposeTransaction(NKikimrTxDataShard::ETransactionKind txKind, ui64 ssId, ui64 subDomainPathId, const TActorId& source, ui64 txId,
            const TStringBuf& txBody, const NKikimrSubDomains::TProcessingParams &processingParams, ui32 flags = NDataShard::TTxFlags::Default)
            : TEvProposeTransaction(txKind, ssId, source, txId, txBody, processingParams, flags)
        {
            if (subDomainPathId) {
                Record.SetSubDomainPathId(subDomainPathId);
            }
        }

        NKikimrTxDataShard::ETransactionKind GetTxKind() const {
            return Record.GetTxKind();
        }

        ui64 GetTxId() const {
            return Record.GetTxId();
        }

        ui32 GetFlags() const {
            return Record.GetFlags();
        }

        TStringBuf GetTxBody() const {
            return Record.GetTxBody();
        }

        // Orbit used for tracking request events
        NLWTrace::TOrbit Orbit;
    };

    struct TEvCancelTransactionProposal : public TEventPB<TEvCancelTransactionProposal, NKikimrTxDataShard::TEvCancelTransactionProposal, TEvDataShard::EvCancelTransactionProposal> {
        TEvCancelTransactionProposal()
        {}

        TEvCancelTransactionProposal(ui64 txId)
        {
            Record.SetTxId(txId);
        }
    };

    struct TEvProposeTransactionResult : public TEventPB<TEvProposeTransactionResult, NKikimrTxDataShard::TEvProposeTransactionResult,
        TEvDataShard::EvProposeTransactionResult> {

        TEvProposeTransactionResult() = default;

        TEvProposeTransactionResult(NKikimrTxDataShard::ETransactionKind txKind, ui64 origin, ui64 txId,
            NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status)
        {
            Record.SetTxKind(txKind);
            Record.SetOrigin(origin);
            Record.SetTxId(txId);
            Record.SetStatus(status);
        }

        bool IsPrepared() const { return GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED; }
        bool IsComplete() const { return GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE; }
        bool IsTryLater() const { return GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::TRY_LATER; }
        bool IsExecError() const { return GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR; }
        bool IsError() const { return GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::ERROR; }
        bool IsBadRequest() const { return GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST; }

        bool IsForceOnline() const { return ForceOnline; }
        bool IsForceDirty() const { return ForceDirty; }
        void SetForceOnline(bool online = true) { ForceOnline = online; }
        void SetForceDirty(bool dirty = true) { ForceDirty = dirty; }

        void SetTryLater() {
            Record.SetStatus(NKikimrTxDataShard::TEvProposeTransactionResult::TRY_LATER);
        }

        void SetDomainCoordinators(const NKikimrSubDomains::TProcessingParams& params) {
            Record.MutableDomainCoordinators()->CopyFrom(params.GetCoordinators());
        }

        void SetSchemeTxDuplicate(bool sameTxId = true) {
            if (sameTxId) {
                Record.SetStatus(NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED);
            } else {
                SetProcessError(NKikimrTxDataShard::TError::SCHEME_CHANGED,
                    "Duplicate scheme tx must not be proposed with a different tx id");
            }
        }

        void SetPrepared(ui64 minStep, ui64 maxStep, const TInstant& RequestStartTime) {
            Record.SetStatus(NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED);
            Record.SetMinStep(minStep);
            Record.SetMaxStep(maxStep);
            Record.SetPrepareArriveTime(RequestStartTime.MicroSeconds());
        }

        void SetTxResult(const TStringBuf& txResult) {
            Record.SetTxResult(txResult.data(), txResult.size());
        }

        void SetExecutionError(const NKikimrTxDataShard::TError::EKind& error, const TStringBuf& message) {
            switch (error) {
                case NKikimrTxDataShard::TError::REPLY_SIZE_EXCEEDED:
                    Record.SetStatus(NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE);
                    break;
                case NKikimrTxDataShard::TError::EXECUTION_CANCELLED:
                    Record.SetStatus(NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED);
                    break;
                default:
                    Record.SetStatus(NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR);
                    break;
            }

            AddError(error, message);
        }

        void SetProcessError(const NKikimrTxDataShard::TError::EKind& error, const TStringBuf& message) {
            switch (error) {
                case NKikimrTxDataShard::TError::PROGRAM_ERROR:
                    Record.SetStatus(NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR);
                    break;
                default:
                    Record.SetStatus(NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
                    break;
            }

            AddError(error, message);
        }

        void SetStepOrderId(const std::pair<ui64, ui64>& stepOrderId) {
            Record.SetStep(stepOrderId.first);
            Record.SetOrderId(stepOrderId.second);
        }

        void AddTxLock(ui64 lockId, ui64 shard, ui32 generation, ui64 counter, ui64 ssId, ui64 pathId, bool hasWrites) {
            auto entry = Record.AddTxLocks();
            entry->SetLockId(lockId);
            entry->SetDataShard(shard);
            entry->SetGeneration(generation);
            entry->SetCounter(counter);
            entry->SetSchemeShard(ssId);
            entry->SetPathId(pathId);
            if (hasWrites) {
                entry->SetHasWrites(true);
            }
        }

        NKikimrTxDataShard::ETransactionKind GetTxKind() const {
            return Record.GetTxKind();
        }

        ui64 GetOrigin() const {
            return Record.GetOrigin();
        }

        ui64 GetTxId() const {
            return Record.GetTxId();
        }

        NKikimrTxDataShard::TEvProposeTransactionResult::EStatus GetStatus() const {
            return Record.GetStatus();
        }

        bool HasTxResult() const {
            return Record.HasTxResult();
        }

        TStringBuf GetTxResult() const {
            return Record.GetTxResult();
        }

        std::pair<ui64, ui64> GetStepOrderId() const {
            return std::make_pair(Record.GetStep(), Record.GetOrderId());
        }

        TString GetError() const {
            if (Record.ErrorSize() > 0) {
                TString result;
                TStringOutput out(result);
                for (ui32 i = 0; i < Record.ErrorSize(); ++i) {
                    out << Record.GetError(i).GetKind() << " ("
                        << (Record.GetError(i).HasReason() ? Record.GetError(i).GetReason() : "no reason")
                        << ") |";
                }
                return result;
            } else {
                return TString();
            }
        }

        void AddError(NKikimrTxDataShard::TError::EKind kind, const TStringBuf& reason, const TStringBuf& keyBuffer = TStringBuf()) {
            auto error = Record.MutableError()->Add();
            error->SetKind(kind);
            if (reason) {
                error->SetReason(reason.data(), reason.size());
            }

            if (keyBuffer) {
                error->SetKey(keyBuffer.data(), keyBuffer.size());
            }
        }
    private:
        bool ForceOnline = false;
        bool ForceDirty = false;

    public:
        // Orbit used for tracking request events
        NLWTrace::TOrbit Orbit;
    };

    struct TEvProposeTransactionRestart : public TEventPB<TEvProposeTransactionRestart, NKikimrTxDataShard::TEvProposeTransactionRestart, TEvDataShard::EvProposeTransactionRestart> {
        TEvProposeTransactionRestart() = default;
        TEvProposeTransactionRestart(ui64 tabletId, ui64 txId) {
            Record.SetTabletId(tabletId);
            Record.SetTxId(txId);
        }
    };

    struct TEvProposeTransactionAttach : public TEventPB<TEvProposeTransactionAttach, NKikimrTxDataShard::TEvProposeTransactionAttach, TEvDataShard::EvProposeTransactionAttach> {
        TEvProposeTransactionAttach() = default;
        TEvProposeTransactionAttach(ui64 tabletId, ui64 txId) {
            Record.SetTabletId(tabletId);
            Record.SetTxId(txId);
        }
    };

    struct TEvProposeTransactionAttachResult : public TEventPB<TEvProposeTransactionAttachResult, NKikimrTxDataShard::TEvProposeTransactionAttachResult, TEvDataShard::EvProposeTransactionAttachResult> {
        TEvProposeTransactionAttachResult() = default;
        TEvProposeTransactionAttachResult(ui64 tabletId, ui64 txId, NKikimrProto::EReplyStatus status) {
            Record.SetTabletId(tabletId);
            Record.SetTxId(txId);
            Record.SetStatus(status);
        }
    };

    struct TEvReturnBorrowedPart : public TEventPB<TEvReturnBorrowedPart, NKikimrTxDataShard::TEvReturnBorrowedPart, TEvDataShard::EvReturnBorrowedPart> {
        TEvReturnBorrowedPart() = default;
        TEvReturnBorrowedPart(ui64 tabletId, const TVector<TLogoBlobID>& partMetaVec) {
            Record.SetFromTabletId(tabletId);
            for (const auto& partMeta : partMetaVec) {
                LogoBlobIDFromLogoBlobID(partMeta, Record.AddPartMetadata());
            }
        }
    };

    struct TEvReturnBorrowedPartAck : public TEventPB<TEvReturnBorrowedPartAck, NKikimrTxDataShard::TEvReturnBorrowedPartAck, TEvDataShard::EvReturnBorrowedPartAck> {
        TEvReturnBorrowedPartAck() = default;
        explicit TEvReturnBorrowedPartAck(const TVector<TLogoBlobID>& partMetaVec) {
            for (const auto& partMeta : partMetaVec) {
                LogoBlobIDFromLogoBlobID(partMeta, Record.AddPartMetadata());
            }
        }
    };


    struct TEvInitSplitMergeDestination : public TEventPB<TEvInitSplitMergeDestination,
                                                        NKikimrTxDataShard::TEvInitSplitMergeDestination,
                                                        TEvDataShard::EvInitSplitMergeDestination> {
        TEvInitSplitMergeDestination() = default;
        TEvInitSplitMergeDestination(ui64 opId, ui64 schemeshardTabletId, ui64 subDomainPathId,
                                     const NKikimrTxDataShard::TSplitMergeDescription &splitDesc,
                                     const NKikimrSubDomains::TProcessingParams &processingParams) {
            Record.SetOperationCookie(opId);
            Record.SetSchemeshardTabletId(schemeshardTabletId);
            Record.SetSubDomainPathId(subDomainPathId);
            Record.MutableSplitDescription()->CopyFrom(splitDesc);
            Record.MutableProcessingParams()->CopyFrom(processingParams);
        }
    };

    struct TEvInitSplitMergeDestinationAck : public TEventPB<TEvInitSplitMergeDestinationAck,
                                                        NKikimrTxDataShard::TEvInitSplitMergeDestinationAck,
                                                        TEvDataShard::EvInitSplitMergeDestinationAck> {
        TEvInitSplitMergeDestinationAck() = default;
        explicit TEvInitSplitMergeDestinationAck(ui64 opId, ui64 tabletId) {
            Record.SetOperationCookie(opId);
            Record.SetTabletId(tabletId);
        }
    };

    struct TEvSplit : public TEventPB<TEvSplit, NKikimrTxDataShard::TEvSplit, TEvDataShard::EvSplit> {
        TEvSplit() = default;
        explicit TEvSplit(ui64 opId) {
            Record.SetOperationCookie(opId);
        }
    };

    struct TEvSplitAck : public TEventPB<TEvSplitAck, NKikimrTxDataShard::TEvSplitAck, TEvDataShard::EvSplitAck> {
        TEvSplitAck() = default;
        explicit TEvSplitAck(ui64 opId, ui64 tabletId) {
            Record.SetOperationCookie(opId);
            Record.SetTabletId(tabletId);
        }
    };

    struct TEvSplitTransferSnapshot : public TEventPB<TEvSplitTransferSnapshot,
                                                        NKikimrTxDataShard::TEvSplitTransferSnapshot,
                                                        TEvDataShard::EvSplitTransferSnapshot> {
        TEvSplitTransferSnapshot() = default;
        explicit TEvSplitTransferSnapshot(ui64 opId) {
            Record.SetOperationCookie(opId);
        }
    };

    struct TEvSplitTransferSnapshotAck : public TEventPB<TEvSplitTransferSnapshotAck,
                                                        NKikimrTxDataShard::TEvSplitTransferSnapshotAck,
                                                        TEvDataShard::EvSplitTransferSnapshotAck> {
        TEvSplitTransferSnapshotAck() = default;
        explicit TEvSplitTransferSnapshotAck(ui64 opId, ui64 tabletId) {
            Record.SetOperationCookie(opId);
            Record.SetTabletId(tabletId);
        }
    };

    struct TEvSplitPartitioningChanged : public TEventPB<TEvSplitPartitioningChanged,
                                                        NKikimrTxDataShard::TEvSplitPartitioningChanged,
                                                        TEvDataShard::EvSplitPartitioningChanged> {
        TEvSplitPartitioningChanged() = default;
        explicit TEvSplitPartitioningChanged(ui64 opId) {
            Record.SetOperationCookie(opId);
        }
    };

    struct TEvSplitPartitioningChangedAck : public TEventPB<TEvSplitPartitioningChangedAck,
                                                        NKikimrTxDataShard::TEvSplitPartitioningChangedAck,
                                                        TEvDataShard::EvSplitPartitioningChangedAck> {
        TEvSplitPartitioningChangedAck() = default;
        explicit TEvSplitPartitioningChangedAck(ui64 opId, ui64 tabletId) {
            Record.SetOperationCookie(opId);
            Record.SetTabletId(tabletId);
        }
    };

    struct TEvCancelBackup
        : public TEventPB<TEvCancelBackup,
                          NKikimrTxDataShard::TEvCancelBackup,
                          TEvDataShard::EvCancelBackup>
    {
        TEvCancelBackup() = default;

        explicit TEvCancelBackup(ui64 txid, ui64 tableId) {
            Record.SetBackupTxId(txid);
            Record.SetTableId(tableId);
        }
    };

    struct TEvCancelRestore
        : public TEventPB<TEvCancelRestore,
                          NKikimrTxDataShard::TEvCancelRestore,
                          TEvDataShard::EvCancelRestore>
    {
        TEvCancelRestore() = default;

        explicit TEvCancelRestore(ui64 txid, ui64 tableId) {
            Record.SetRestoreTxId(txid);
            Record.SetTableId(tableId);
        }
    };

    struct TEvGetTableStats : public TEventPB<TEvGetTableStats,
                                                        NKikimrTxDataShard::TEvGetTableStats,
                                                        TEvDataShard::EvGetTableStats> {
        TEvGetTableStats() = default;
        explicit TEvGetTableStats(ui64 tableId) {
            Record.SetTableId(tableId);
        }
    };

    struct TEvGetTableStatsResult : public TEventPB<TEvGetTableStatsResult,
                                                        NKikimrTxDataShard::TEvGetTableStatsResult,
                                                        TEvDataShard::EvGetTableStatsResult> {
        TEvGetTableStatsResult() = default;
        TEvGetTableStatsResult(ui64 datashardId, ui64 tableOwnerId, ui64 tableLocalId) {
            Record.SetDatashardId(datashardId);
            Record.SetTableOwnerId(tableOwnerId);
            Record.SetTableLocalId(tableLocalId);
        }
    };

    struct TEvPeriodicTableStats : public TEventPB<TEvPeriodicTableStats,
                                                        NKikimrTxDataShard::TEvPeriodicTableStats,
                                                        TEvDataShard::EvPeriodicTableStats> {
        TEvPeriodicTableStats() = default;
        TEvPeriodicTableStats(ui64 datashardId, ui64 tableOwnerId, ui64 tableLocalId) {
            Record.SetDatashardId(datashardId);
            Record.SetTableOwnerId(tableOwnerId);
            Record.SetTableLocalId(tableLocalId);
        }

        TEvPeriodicTableStats(ui64 datashardId, ui64 tableLocalId) {
            Record.SetDatashardId(datashardId);
            Record.SetTableLocalId(tableLocalId);
        }
    };

    struct TEvUploadRowsRequest : public TEventPBWithArena<TEvUploadRowsRequest,
                                                        NKikimrTxDataShard::TEvUploadRowsRequest,
                                                        TEvDataShard::EvUploadRowsRequest,
                                                        16*1024, 32*1024> {
        TEvUploadRowsRequest() = default;
    };

    struct TEvUploadRowsResponse : public TEventPB<TEvUploadRowsResponse,
                                                        NKikimrTxDataShard::TEvUploadRowsResponse,
                                                        TEvDataShard::EvUploadRowsResponse> {
        TEvUploadRowsResponse() = default;

        explicit TEvUploadRowsResponse(ui64 tabletId, ui32 status = NKikimrTxDataShard::TError::OK) {
            Record.SetTabletID(tabletId);
            Record.SetStatus(status);
        }
    };

    struct TEvOverloadReady
        : public TEventPB<
            TEvOverloadReady,
            NKikimrTxDataShard::TEvOverloadReady,
            EvOverloadReady>
    {
        TEvOverloadReady() = default;

        explicit TEvOverloadReady(ui64 tabletId, ui64 seqNo) {
            Record.SetTabletID(tabletId);
            Record.SetSeqNo(seqNo);
        }
    };

    struct TEvOverloadUnsubscribe
        : public TEventPB<
            TEvOverloadUnsubscribe,
            NKikimrTxDataShard::TEvOverloadUnsubscribe,
            EvOverloadUnsubscribe>
    {
        TEvOverloadUnsubscribe() = default;

        explicit TEvOverloadUnsubscribe(ui64 seqNo) {
            Record.SetSeqNo(seqNo);
        }
    };

    // In most cases this event is local, thus users must
    // use Keys, Ranges and Program struct members instead of corresponding
    // protobuf members. In case of remote event these struct members will
    // be serialized and deserialized.
    struct TEvRead : public TEventPB<TEvRead,
                                     NKikimrTxDataShard::TEvRead,
                                     TEvDataShard::EvRead> {
        using TBase = TEventPB<TEvRead,
                               NKikimrTxDataShard::TEvRead,
                               TEvDataShard::EvRead>;

        TEvRead() = default;

        TString ToString() const override;

        ui32 CalculateSerializedSize() const override {
            const_cast<TEvRead*>(this)->FillRecord();
            return TBase::CalculateSerializedSize();
        }

        bool SerializeToArcadiaStream(NActors::TChunkSerializer* chunker) const override {
            const_cast<TEvRead*>(this)->FillRecord();
            return TBase::SerializeToArcadiaStream(chunker);
        }

        static NActors::IEventBase* Load(TEventSerializedData* data);

    private:
        void FillRecord();

    public:
        // Either one of Keys, Ranges or Record.Program is allowed

        // TODO: consider TOwnedCellVec depending on kqp
        TVector<TSerializedCellVec> Keys;

        // In current kqp impl ranges are already in TSerializedTableRange
        // format, thus same format here
        TVector<TSerializedTableRange> Ranges;

        // True when TEvRead is cancelled while enqueued in a waiting queue
        bool Cancelled = false;

        // Orbit used for tracking request events
        NLWTrace::TOrbit Orbit;

        // Wilson span for this request.
        NWilson::TSpan ReadSpan;
    };

    struct TEvReadResult : public TEventPB<TEvReadResult,
                                           NKikimrTxDataShard::TEvReadResult,
                                           TEvDataShard::EvReadResult> {
        using TBase = TEventPB<TEvReadResult,
                               NKikimrTxDataShard::TEvReadResult,
                               TEvDataShard::EvReadResult>;

        TEvReadResult() = default;

        TString ToString() const override;

        ui32 CalculateSerializedSize() const override {
            const_cast<TEvReadResult*>(this)->FillRecord();
            return TBase::CalculateSerializedSize();
        }

        bool SerializeToArcadiaStream(NActors::TChunkSerializer* chunker) const override {
            const_cast<TEvReadResult*>(this)->FillRecord();
            return TBase::SerializeToArcadiaStream(chunker);
        }

        static NActors::IEventBase* Load(TEventSerializedData* data);

        size_t GetRowsCount() const {
            return Record.GetRowCount();
        }

    private:
        void FillRecord();

    public:
        // CellVec (TODO: add schema?)

        TConstArrayRef<TCell> GetCells(size_t row) const {
            if (Rows.empty() && Batch.Empty() && RowsSerialized.empty())
                return {};

            if (!Rows.empty()) {
                return Rows[row];
            }

            if (!Batch.Empty()) {
                return Batch[row];
            }

            return RowsSerialized[row].GetCells();
        }

        void SetRows(TVector<TOwnedCellVec>&& rows) {
            Rows = std::move(rows);
        }

        void SetBatch(TOwnedCellVecBatch&& batch) {
            Batch = std::move(batch);
        }

        // Arrow

        void SetArrowBatch(std::shared_ptr<arrow::RecordBatch>&& batch) {
            ArrowBatch = std::move(batch);
        }

        std::shared_ptr<arrow::RecordBatch> GetArrowBatch();
        std::shared_ptr<arrow::RecordBatch> GetArrowBatch() const;

    private:
        // for local events
        TVector<TOwnedCellVec> Rows;

        // batch for local events
        TOwnedCellVecBatch Batch;

        // for remote events to avoid extra copying
        TVector<TSerializedCellVec> RowsSerialized;

        std::shared_ptr<arrow::RecordBatch> ArrowBatch;
    };

    struct TEvReadContinue : public TEventLocal<TEvReadContinue, TEvDataShard::EvReadContinue> {
        TActorId Reader;
        ui64 ReadId;

        TEvReadContinue(TActorId reader, ui64 readId)
            : Reader(reader)
            , ReadId(readId)
        {}
    };

    struct TEvReadAck : public TEventPB<TEvReadAck,
                                        NKikimrTxDataShard::TEvReadAck,
                                        TEvDataShard::EvReadAck> {
        TEvReadAck() = default;
    };

    struct TEvReadCancel : public TEventPB<TEvReadCancel,
                                           NKikimrTxDataShard::TEvReadCancel,
                                           TEvDataShard::EvReadCancel> {
        TEvReadCancel() = default;
    };

    struct TEvReadColumnsRequest : public TEventPB<TEvReadColumnsRequest,
                                                   NKikimrTxDataShard::TEvReadColumnsRequest,
                                                   TEvDataShard::EvReadColumnsRequest> {
        TEvReadColumnsRequest() = default;
    };

    struct TEvReadColumnsResponse : public TEventPB<TEvReadColumnsResponse,
                                                    NKikimrTxDataShard::TEvReadColumnsResponse,
                                                    TEvDataShard::EvReadColumnsResponse> {
        TEvReadColumnsResponse() = default;

        explicit TEvReadColumnsResponse(ui64 tabletId, ui32 status = NKikimrTxDataShard::TError::OK) {
            Record.SetTabletID(tabletId);
            Record.SetStatus(status);
        }
    };

    struct TEvGetInfoRequest : public TEventPB<TEvGetInfoRequest,
                                               NKikimrTxDataShard::TEvGetInfoRequest,
                                               TEvDataShard::EvGetInfoRequest> {
    };

    struct TEvGetInfoResponse : public TEventPB<TEvGetInfoResponse,
                                                NKikimrTxDataShard::TEvGetInfoResponse,
                                                TEvDataShard::EvGetInfoResponse> {
    };

    struct TEvListOperationsRequest : public TEventPB<TEvListOperationsRequest,
                                                      NKikimrTxDataShard::TEvListOperationsRequest,
                                                      TEvDataShard::EvListOperationsRequest> {
    };

    struct TEvListOperationsResponse : public TEventPB<TEvListOperationsResponse,
                                                       NKikimrTxDataShard::TEvListOperationsResponse,
                                                       TEvDataShard::EvListOperationsResponse> {
    };

    struct TEvGetOperationRequest : public TEventPB<TEvGetOperationRequest,
                                                    NKikimrTxDataShard::TEvGetOperationRequest,
                                                    TEvDataShard::EvGetOperationRequest> {
    };

    struct TEvGetOperationResponse : public TEventPB<TEvGetOperationResponse,
                                                     NKikimrTxDataShard::TEvGetOperationResponse,
                                                     TEvDataShard::EvGetOperationResponse> {
    };

    struct TEvGetReadTableSinkStateRequest : public TEventPB<TEvGetReadTableSinkStateRequest,
                                                             NKikimrTxDataShard::TEvGetReadTableSinkStateRequest,
                                                             TEvDataShard::EvGetReadTableSinkStateRequest> {
    };

    struct TEvGetReadTableSinkStateResponse : public TEventPB<TEvGetReadTableSinkStateResponse,
                                                              NKikimrTxDataShard::TEvGetReadTableSinkStateResponse,
                                                              TEvDataShard::EvGetReadTableSinkStateResponse> {
    };

    struct TEvGetReadTableScanStateRequest : public TEventPB<TEvGetReadTableScanStateRequest,
                                                             NKikimrTxDataShard::TEvGetReadTableScanStateRequest,
                                                             TEvDataShard::EvGetReadTableScanStateRequest> {
    };

    struct TEvGetReadTableScanStateResponse : public TEventPB<TEvGetReadTableScanStateResponse,
                                                              NKikimrTxDataShard::TEvGetReadTableScanStateResponse,
                                                              TEvDataShard::EvGetReadTableScanStateResponse> {
    };

    struct TEvGetReadTableStreamStateRequest : public TEventPB<TEvGetReadTableStreamStateRequest,
                                                               NKikimrTxDataShard::TEvGetReadTableStreamStateRequest,
                                                               TEvDataShard::EvGetReadTableStreamStateRequest> {
    };

    struct TEvGetReadTableStreamStateResponse : public TEventPB<TEvGetReadTableStreamStateResponse,
                                                                NKikimrTxDataShard::TEvGetReadTableStreamStateResponse,
                                                                TEvDataShard::EvGetReadTableStreamStateResponse> {
    };

    struct TEvGetSlowOpProfilesRequest : public TEventPB<TEvGetSlowOpProfilesRequest,
                                                         NKikimrTxDataShard::TEvGetSlowOpProfilesRequest,
                                                         TEvDataShard::EvGetSlowOpProfilesRequest> {
    };

    struct TEvGetSlowOpProfilesResponse : public TEventPB<TEvGetSlowOpProfilesResponse,
                                                          NKikimrTxDataShard::TEvGetSlowOpProfilesResponse,
                                                          TEvDataShard::EvGetSlowOpProfilesResponse> {
    };

    struct TEvGetRSInfoRequest : public TEventPB<TEvGetRSInfoRequest,
                                                 NKikimrTxDataShard::TEvGetRSInfoRequest,
                                                 TEvDataShard::EvGetRSInfoRequest> {
    };

    struct TEvGetRSInfoResponse : public TEventPB<TEvGetRSInfoResponse,
                                                  NKikimrTxDataShard::TEvGetRSInfoResponse,
                                                  TEvDataShard::EvGetRSInfoResponse> {
    };

    struct TEvGetDataHistogramRequest : public TEventPB<TEvGetDataHistogramRequest,
                                                        NKikimrTxDataShard::TEvGetDataHistogramRequest,
                                                        TEvDataShard::EvGetDataHistogramRequest> {
    };

    struct TEvGetDataHistogramResponse : public TEventPB<TEvGetDataHistogramResponse,
                                                         NKikimrTxDataShard::TEvGetDataHistogramResponse,
                                                         TEvDataShard::EvGetDataHistogramResponse> {
    };

    struct TEvRefreshVolatileSnapshotRequest
        : public TEventPB<TEvRefreshVolatileSnapshotRequest,
                          NKikimrTxDataShard::TEvRefreshVolatileSnapshotRequest,
                          TEvDataShard::EvRefreshVolatileSnapshotRequest>
    {
    };

    struct TEvRefreshVolatileSnapshotResponse
        : public TEventPB<TEvRefreshVolatileSnapshotResponse,
                          NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse,
                          TEvDataShard::EvRefreshVolatileSnapshotResponse>
    {
        using EStatus = NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse::EStatus;
    };

    struct TEvDiscardVolatileSnapshotRequest
        : public TEventPB<TEvDiscardVolatileSnapshotRequest,
                          NKikimrTxDataShard::TEvDiscardVolatileSnapshotRequest,
                          TEvDataShard::EvDiscardVolatileSnapshotRequest>
    {
    };

    struct TEvDiscardVolatileSnapshotResponse
        : public TEventPB<TEvDiscardVolatileSnapshotResponse,
                          NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse,
                          TEvDataShard::EvDiscardVolatileSnapshotResponse>
    {
        using EStatus = NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse::EStatus;
    };

    struct TEvMigrateSchemeShardRequest
        : public TEventPB<TEvMigrateSchemeShardRequest,
                          NKikimrTxDataShard::TEvMigrateSchemeShardRequest,
                          TEvDataShard::EvMigrateSchemeShardRequest>
    {
        ui64 GetDatashardId() const {
            return Record.GetTabletId();
        }
    };

    struct TEvMigrateSchemeShardResponse
        : public TEventPB<TEvMigrateSchemeShardResponse,
                          NKikimrTxDataShard::TEvMigrateSchemeShardResponse,
                          TEvDataShard::EvMigrateSchemeShardResponse>
    {
        using EStatus = NKikimrTxDataShard::TEvMigrateSchemeShardResponse::EStatus;
    };

    struct TEvGetS3Upload
        : public TEventLocal<TEvGetS3Upload, TEvDataShard::EvGetS3Upload>
    {
        TActorId ReplyTo;
        ui64 TxId;

        explicit TEvGetS3Upload(const TActorId& replyTo, ui64 txId)
            : ReplyTo(replyTo)
            , TxId(txId)
        {
        }
    };

    struct TEvStoreS3UploadId
        : public TEventLocal<TEvStoreS3UploadId, TEvDataShard::EvStoreS3UploadId>
    {
        TActorId ReplyTo;
        ui64 TxId;
        TString UploadId;

        explicit TEvStoreS3UploadId(const TActorId& replyTo, ui64 txId, const TString& uploadId)
            : ReplyTo(replyTo)
            , TxId(txId)
            , UploadId(uploadId)
        {
        }
    };

    struct TEvChangeS3UploadStatus
        : public TEventLocal<TEvChangeS3UploadStatus, TEvDataShard::EvChangeS3UploadStatus>
    {
        using EStatus = NDataShard::TS3Upload::EStatus;

        TActorId ReplyTo;
        ui64 TxId;
        EStatus Status;
        TMaybe<TString> Error;
        TVector<TString> Parts;

        explicit TEvChangeS3UploadStatus(const TActorId& replyTo, ui64 txId, EStatus status, TVector<TString>&& parts)
            : ReplyTo(replyTo)
            , TxId(txId)
            , Status(status)
            , Parts(std::move(parts))
        {
        }

        explicit TEvChangeS3UploadStatus(const TActorId& replyTo, ui64 txId, EStatus status, const TString& error)
            : ReplyTo(replyTo)
            , TxId(txId)
            , Status(status)
            , Error(error)
        {
        }
    };

    struct TEvS3Upload
        : public TEventLocal<TEvS3Upload, TEvDataShard::EvS3Upload>
    {
        using TS3Upload = NDataShard::TS3Upload;

        TMaybe<TS3Upload> Upload;

        TEvS3Upload() = default;

        explicit TEvS3Upload(const TS3Upload& upload)
            : Upload(upload)
        {
        }
    };

    struct TEvGetS3DownloadInfo
        : public TEventLocal<TEvGetS3DownloadInfo, TEvDataShard::EvGetS3DownloadInfo>
    {
        ui64 TxId;

        explicit TEvGetS3DownloadInfo(ui64 txId)
            : TxId(txId)
        {
        }
    };

    struct TEvStoreS3DownloadInfo
        : public TEventLocal<TEvStoreS3DownloadInfo, TEvDataShard::EvStoreS3DownloadInfo>
    {
        ui64 TxId;
        NDataShard::TS3Download Info;

        explicit TEvStoreS3DownloadInfo(ui64 txId, const NDataShard::TS3Download& info)
            : TxId(txId)
            , Info(info)
        {
            Y_ABORT_UNLESS(Info.DataETag);
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " TxId: " << TxId
                << " Info: " << Info
            << " }";
        }
    };

    struct TEvS3DownloadInfo
        : public TEventLocal<TEvS3DownloadInfo, TEvDataShard::EvS3DownloadInfo>
    {
        NDataShard::TS3Download Info;

        TEvS3DownloadInfo() = default;

        explicit TEvS3DownloadInfo(const NDataShard::TS3Download& info)
            : Info(info)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " Info: " << Info
            << " }";
        }
    };

    struct TEvS3UploadRowsRequest
        : public TEventLocal<TEvS3UploadRowsRequest, TEvDataShard::EvS3UploadRowsRequest>
    {
        ui64 TxId;
        std::shared_ptr<NKikimrTxDataShard::TEvUploadRowsRequest> RecordHolder;
        const NKikimrTxDataShard::TEvUploadRowsRequest& Record;
        NDataShard::TS3Download Info;

        explicit TEvS3UploadRowsRequest(
                ui64 txId,
                const std::shared_ptr<NKikimrTxDataShard::TEvUploadRowsRequest>& record,
                const NDataShard::TS3Download& info)
            : TxId(txId)
            , RecordHolder(record)
            , Record(*RecordHolder)
            , Info(info)
        {
            Y_ABORT_UNLESS(Info.DataETag);
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " TxId: " << TxId
                << " Info: " << Info
            << " }";
        }
    };

    struct TEvS3UploadRowsResponse
        : public TEventLocal<TEvS3UploadRowsResponse, TEvDataShard::EvS3UploadRowsResponse>
    {
        NKikimrTxDataShard::TEvUploadRowsResponse Record;
        NDataShard::TS3Download Info;

        explicit TEvS3UploadRowsResponse(ui64 tabletId, ui32 status = NKikimrTxDataShard::TError::OK) {
            Record.SetTabletID(tabletId);
            Record.SetStatus(status);
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " Record: " << Record.ShortDebugString()
                << " Info: " << Info
            << " }";
        }
    };

    struct TEvObjectStorageListingRequest
        : public TEventPB<TEvObjectStorageListingRequest,
                            NKikimrTxDataShard::TEvObjectStorageListingRequest,
                            TEvDataShard::EvObjectStorageListingRequest> {
        TEvObjectStorageListingRequest() = default;
    };

    struct TEvObjectStorageListingResponse
         : public TEventPB<TEvObjectStorageListingResponse,
                            NKikimrTxDataShard::TEvObjectStorageListingResponse,
                            TEvDataShard::EvObjectStorageListingResponse> {
        TEvObjectStorageListingResponse() = default;

        explicit TEvObjectStorageListingResponse(ui64 tabletId, ui32 status = NKikimrTxDataShard::TError::OK) {
            Record.SetTabletID(tabletId);
            Record.SetStatus(status);
        }
    };

    struct TEvEraseRowsRequest
        : public TEventPB<TEvEraseRowsRequest,
                          NKikimrTxDataShard::TEvEraseRowsRequest,
                          TEvDataShard::EvEraseRowsRequest>
    {
    };

    struct TEvEraseRowsResponse
        : public TEventPB<TEvEraseRowsResponse,
                          NKikimrTxDataShard::TEvEraseRowsResponse,
                          TEvDataShard::EvEraseRowsResponse>
    {
    };

    struct TEvConditionalEraseRowsRequest
        : public TEventPB<TEvConditionalEraseRowsRequest,
                          NKikimrTxDataShard::TEvConditionalEraseRowsRequest,
                          TEvDataShard::EvConditionalEraseRowsRequest>
    {
    };

    struct TEvConditionalEraseRowsResponse
        : public TEventPB<TEvConditionalEraseRowsResponse,
                          NKikimrTxDataShard::TEvConditionalEraseRowsResponse,
                          TEvDataShard::EvConditionalEraseRowsResponse>
    {
    };

    struct TEvCheckConstraintCreateRequest
        : public TEventPB<TEvCheckConstraintCreateRequest,
                          NKikimrTxDataShard::TEvCheckConstraintCreateRequest,
                          TEvDataShard::EvCheckConstraintCreateRequest>
    {
    };

    struct TEvCheckConstraintProgressResponse
        : public TEventPB<TEvCheckConstraintProgressResponse,
                          NKikimrTxDataShard::TEvCheckConstraintProgressResponse,
                          TEvDataShard::EvCheckConstraintProgressResponse>
    {
    };

    struct TEvBuildIndexCreateRequest
        : public TEventPB<TEvBuildIndexCreateRequest,
                          NKikimrTxDataShard::TEvBuildIndexCreateRequest,
                          TEvDataShard::EvBuildIndexCreateRequest>
    {
    };

    struct TEvBuildIndexProgressResponse
        : public TEventPB<TEvBuildIndexProgressResponse,
                          NKikimrTxDataShard::TEvBuildIndexProgressResponse,
                          TEvDataShard::EvBuildIndexProgressResponse>
    {
    };

    struct TEvSampleKRequest
        : public TEventPB<TEvSampleKRequest,
                          NKikimrTxDataShard::TEvSampleKRequest,
                          TEvDataShard::EvSampleKRequest> {
    };

    struct TEvSampleKResponse
        : public TEventPB<TEvSampleKResponse,
                          NKikimrTxDataShard::TEvSampleKResponse,
                          TEvDataShard::EvSampleKResponse> {
    };

    struct TEvKqpScan
        : public TEventPB<TEvKqpScan,
                          NKikimrTxDataShard::TEvKqpScan,
                          TEvDataShard::EvKqpScan>
    {
    };

    struct TEvGetRemovedRowVersions : public TEventLocal<TEvGetRemovedRowVersions, EvGetRemovedRowVersions> {
        TPathId PathId;

        TEvGetRemovedRowVersions(const TPathId& pathId)
            : PathId(pathId)
        { }
    };

    struct TEvGetRemovedRowVersionsResult : public TEventLocal<TEvGetRemovedRowVersionsResult, EvGetRemovedRowVersionsResult> {
        NTable::TRowVersionRanges RemovedRowVersions;

        TEvGetRemovedRowVersionsResult(const NTable::TRowVersionRanges& removedRowVersions)
            : RemovedRowVersions(removedRowVersions)
        { }
    };

    struct TEvCompactTable : public TEventPB<TEvCompactTable, NKikimrTxDataShard::TEvCompactTable,
                                             TEvDataShard::EvCompactTable> {
        TEvCompactTable() = default;
        TEvCompactTable(ui64 ownerId, ui64 localId) {
            Record.MutablePathId()->SetOwnerId(ownerId);
            Record.MutablePathId()->SetLocalId(localId);
        }
        TEvCompactTable(const TPathId& pathId)
            : TEvCompactTable(pathId.OwnerId, pathId.LocalPathId)
        { }
    };

    struct TEvCompactTableResult : public TEventPB<TEvCompactTableResult, NKikimrTxDataShard::TEvCompactTableResult,
                                                   TEvDataShard::EvCompactTableResult> {
        TEvCompactTableResult() = default;

        TEvCompactTableResult(ui64 tabletId, const TPathId& pathId, NKikimrTxDataShard::TEvCompactTableResult::EStatus status)
            : TEvCompactTableResult(tabletId, pathId.OwnerId, pathId.LocalPathId, status)
        { }

        TEvCompactTableResult(ui64 tabletId, ui64 ownerId, ui64 localId, NKikimrTxDataShard::TEvCompactTableResult::EStatus status) {
            Record.SetTabletId(tabletId);
            Record.MutablePathId()->SetOwnerId(ownerId);
            Record.MutablePathId()->SetLocalId(localId);
            Record.SetStatus(status);
        }
    };

    /**
     * This message is used to ask datashard to compact any borrowed parts it has
     * for the specified user table.
     */
    struct TEvCompactBorrowed : public TEventPB<TEvCompactBorrowed,
                                                NKikimrTxDataShard::TEvCompactBorrowed,
                                                EvCompactBorrowed> {
        TEvCompactBorrowed() = default;

        TEvCompactBorrowed(ui64 ownerId, ui64 localId) {
            Record.MutablePathId()->SetOwnerId(ownerId);
            Record.MutablePathId()->SetLocalId(localId);
        }

        TEvCompactBorrowed(const TPathId& pathId)
            : TEvCompactBorrowed(pathId.OwnerId, pathId.LocalPathId)
        { }

        // Sanity check for safe merging to earlier versions
        static_assert(EvCompactBorrowed == EventSpaceBegin(TKikimrEvents::ES_TX_DATASHARD) + 7 * 512 + 60,
                    "EvCompactBorrowed event has an unexpected value");
    };

    struct TEvCompactBorrowedResult : public TEventPB<TEvCompactBorrowedResult,
                                                      NKikimrTxDataShard::TEvCompactBorrowedResult,
                                                      EvCompactBorrowedResult> {
        TEvCompactBorrowedResult() = default;

        TEvCompactBorrowedResult(ui64 tabletId, ui64 ownerId, ui64 localId) {
            Record.SetTabletId(tabletId);
            Record.MutablePathId()->SetOwnerId(ownerId);
            Record.MutablePathId()->SetLocalId(localId);
        }

        TEvCompactBorrowedResult(ui64 tabletId, const TPathId& pathId)
            : TEvCompactBorrowedResult(tabletId, pathId.OwnerId, pathId.LocalPathId)
        {}

        // Sanity check for safe merging to earlier versions
        static_assert(EvCompactBorrowedResult == EventSpaceBegin(TKikimrEvents::ES_TX_DATASHARD) + 7 * 512 + 68,
                    "EvCompactBorrowedResult event has an unexpected value");
    };

    struct TEvGetCompactTableStats : public TEventPB<TEvGetCompactTableStats, NKikimrTxDataShard::TEvGetCompactTableStats,
                                                     TEvDataShard::EvGetCompactTableStats> {
        TEvGetCompactTableStats() = default;
        TEvGetCompactTableStats(ui64 ownerId, ui64 localId) {
            Record.MutablePathId()->SetOwnerId(ownerId);
            Record.MutablePathId()->SetLocalId(localId);
        }
    };

    struct TEvGetCompactTableStatsResult : public TEventPB<TEvGetCompactTableStatsResult, NKikimrTxDataShard::TEvGetCompactTableStatsResult,
                                                           TEvDataShard::EvGetCompactTableStatsResult> {
        TEvGetCompactTableStatsResult() = default;
    };

    struct TEvApplyReplicationChanges
        : public TEventPB<TEvApplyReplicationChanges,
                          NKikimrTxDataShard::TEvApplyReplicationChanges,
                          EvApplyReplicationChanges>
    {
        TEvApplyReplicationChanges() = default;

        explicit TEvApplyReplicationChanges(const TPathId& pathId, ui64 schemaVersion = 0) {
            auto* p = Record.MutableTableId();
            p->SetOwnerId(pathId.OwnerId);
            p->SetTableId(pathId.LocalPathId);
            if (schemaVersion != 0) {
                p->SetSchemaVersion(schemaVersion);
            }
        }
    };

    struct TEvApplyReplicationChangesResult
        : public TEventPB<TEvApplyReplicationChangesResult,
                          NKikimrTxDataShard::TEvApplyReplicationChangesResult,
                          EvApplyReplicationChangesResult>
    {
        using EStatus = NKikimrTxDataShard::TEvApplyReplicationChangesResult::EStatus;
        using EReason = NKikimrTxDataShard::TEvApplyReplicationChangesResult::EReason;

        TEvApplyReplicationChangesResult() = default;

        explicit TEvApplyReplicationChangesResult(
                EStatus status,
                EReason reason = NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_NONE,
                const TString& errorDescription = TString())
        {
            Record.SetStatus(status);
            if (reason != NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_NONE) {
                Record.SetReason(reason);
            }
            if (!errorDescription.empty()) {
                Record.SetErrorDescription(errorDescription);
            }
        }
    };

    struct TEvGetReplicationSourceOffsets
        : public TEventPB<TEvGetReplicationSourceOffsets,
                          NKikimrTxDataShard::TEvGetReplicationSourceOffsets,
                          EvGetReplicationSourceOffsets>
    {
        TEvGetReplicationSourceOffsets() = default;

        TEvGetReplicationSourceOffsets(ui64 readId, const TPathId& pathId) {
            Record.SetReadId(readId);
            SetPathId(pathId);
        }

        TEvGetReplicationSourceOffsets* SetPathId(const TPathId& pathId) {
            auto* p = Record.MutableTableId();
            p->SetOwnerId(pathId.OwnerId);
            p->SetTableId(pathId.LocalPathId);
            return this;
        }

        TPathId GetPathId() const {
            const auto& p = Record.GetTableId();
            return TPathId(p.GetOwnerId(), p.GetTableId());
        }

        TEvGetReplicationSourceOffsets* SetFrom(ui64 sourceId, ui64 splitKeyId) {
            Record.SetFromSourceId(sourceId);
            Record.SetFromSplitKeyId(splitKeyId);
            return this;
        }
    };

    struct TEvReplicationSourceOffsets
        : public TEventPB<TEvReplicationSourceOffsets,
                          NKikimrTxDataShard::TEvReplicationSourceOffsets,
                          EvReplicationSourceOffsets>
    {
        TEvReplicationSourceOffsets() = default;

        TEvReplicationSourceOffsets(ui64 readId, ui64 seqNo) {
            Record.SetReadId(readId);
            Record.SetSeqNo(seqNo);
        }
    };

    struct TEvReplicationSourceOffsetsAck
        : public TEventPB<TEvReplicationSourceOffsetsAck,
                          NKikimrTxDataShard::TEvReplicationSourceOffsetsAck,
                          EvReplicationSourceOffsetsAck>
    {
        TEvReplicationSourceOffsetsAck() = default;

        TEvReplicationSourceOffsetsAck(ui64 readId, ui64 seqNo) {
            Record.SetReadId(readId);
            Record.SetAckSeqNo(seqNo);
        }
    };

    struct TEvReplicationSourceOffsetsCancel
        : public TEventPB<TEvReplicationSourceOffsetsCancel,
                          NKikimrTxDataShard::TEvReplicationSourceOffsetsCancel,
                          EvReplicationSourceOffsetsCancel>
    {
        TEvReplicationSourceOffsetsCancel() = default;

        explicit TEvReplicationSourceOffsetsCancel(ui64 readId) {
            Record.SetReadId(readId);
        }
    };

    struct TEvGetOpenTxs : public TEventLocal<TEvGetOpenTxs, EvGetOpenTxs> {
        TPathId PathId;

        TEvGetOpenTxs(const TPathId& pathId)
            : PathId(pathId)
        { }
    };

    struct TEvGetOpenTxsResult : public TEventLocal<TEvGetOpenTxsResult, EvGetOpenTxsResult> {
        TPathId PathId;
        absl::flat_hash_set<ui64> OpenTxs;

        TEvGetOpenTxsResult(const TPathId& pathId, absl::flat_hash_set<ui64> openTxs)
            : PathId(pathId)
            , OpenTxs(std::move(openTxs))
        { }
    };

    struct TEvCdcStreamScanRequest
        : public TEventPB<TEvCdcStreamScanRequest,
                          NKikimrTxDataShard::TEvCdcStreamScanRequest,
                          EvCdcStreamScanRequest>
    {
    };

    struct TEvCdcStreamScanResponse
        : public TEventPB<TEvCdcStreamScanResponse,
                          NKikimrTxDataShard::TEvCdcStreamScanResponse,
                          EvCdcStreamScanResponse>
    {
        TEvCdcStreamScanResponse() = default;

        explicit TEvCdcStreamScanResponse(
                const NKikimrTxDataShard::TEvCdcStreamScanRequest& request, ui64 tabletId,
                NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus status, const TString& error = {})
        {
            Record.SetTabletId(tabletId);
            Record.MutableTablePathId()->CopyFrom(request.GetTablePathId());
            Record.MutableStreamPathId()->CopyFrom(request.GetStreamPathId());
            Record.SetStatus(status);
            Record.SetErrorDescription(error);
        }
    };
};

IActor* CreateDataShard(const TActorId &tablet, TTabletStorageInfo *info);

}

inline TString DatashardStateName(ui32 state) {
    NKikimrTxDataShard::EDatashardState s = (NKikimrTxDataShard::EDatashardState)state;
    return NKikimrTxDataShard::EDatashardState_Name(s);
}
