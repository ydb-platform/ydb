#pragma once

#include "datashard.h"
#include "datashard_trans_queue.h"
#include "datashard_outreadset.h"
#include "datashard_pipeline.h"
#include "datashard_schema_snapshots.h"
#include "datashard_snapshots.h"
#include "datashard_s3_downloads.h"
#include "datashard_s3_uploads.h"
#include "datashard_user_table.h"
#include "datashard_repl_offsets.h"
#include "datashard_repl_offsets_client.h"
#include "datashard_repl_offsets_server.h"
#include "datashard_write.h"
#include "cdc_stream_heartbeat.h"
#include "cdc_stream_scan.h"
#include "change_exchange.h"
#include "change_record.h"
#include "change_record_cdc_serializer.h"
#include "progress_queue.h"
#include "read_iterator.h"
#include "volatile_tx.h"
#include "conflicts_cache.h"
#include "reject_reason.h"
#include "scan_common.h"

#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/locks/locks.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/engine/mkql_engine_flat_host.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tablet/pipe_tracker.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tablet_flat/flat_page_iface.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/protos/tx.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/protos/table_stats.pb.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <util/string/join.h>

namespace NKikimr {
namespace NDataShard {

extern TStringBuf SnapshotTransferReadSetMagic;

using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TScanOptions;
using NLongTxService::TEvLongTxService;

// For CopyTable and MoveShadow
class TTxTableSnapshotContext : public NTabletFlatExecutor::TTableSnapshotContext {
public:
    TTxTableSnapshotContext(ui64 step, ui64 txId, TVector<ui32>&& tables, bool hasOpenTxs = false)
        : StepOrder(step, txId)
        , Tables(tables)
        , HasOpenTxs_(hasOpenTxs)
    {}

    const TStepOrder& GetStepOrder() const {
        return StepOrder;
    }

    virtual TConstArrayRef<ui32> TablesToSnapshot() const override {
        return Tables;
    }

    bool HasOpenTxs() const {
        return HasOpenTxs_;
    }

private:
    TStepOrder StepOrder;
    TVector<ui32> Tables;
    bool HasOpenTxs_;
};

// For Split
class TSplitSnapshotContext : public NTabletFlatExecutor::TTableSnapshotContext {
public:
    TSplitSnapshotContext(ui64 txId, TVector<ui32> &&tables,
                          TRowVersion completeEdge = TRowVersion::Min(),
                          TRowVersion incompleteEdge = TRowVersion::Min(),
                          TRowVersion immediateWriteEdge = TRowVersion::Min(),
                          TRowVersion lowWatermark = TRowVersion::Min(),
                          bool performedUnprotectedReads = false)
        : TxId(txId)
        , CompleteEdge(completeEdge)
        , IncompleteEdge(incompleteEdge)
        , ImmediateWriteEdge(immediateWriteEdge)
        , LowWatermark(lowWatermark)
        , PerformedUnprotectedReads(performedUnprotectedReads)
        , Tables(tables)
    {}

    virtual TConstArrayRef<ui32> TablesToSnapshot() const override {
        return Tables;
    }

    ui64 TxId;
    TRowVersion CompleteEdge;
    TRowVersion IncompleteEdge;
    TRowVersion ImmediateWriteEdge;
    TRowVersion LowWatermark;
    bool PerformedUnprotectedReads;

private:
    TVector<ui32> Tables;
};

// Base class for non-Transactional scans of DataShard data
class INoTxScan : public NTable::IScan {
public:
    virtual void OnFinished(TDataShard* self) = 0;
};

struct TReadWriteVersions {
    TReadWriteVersions(const TRowVersion& readVersion, const TRowVersion& writeVersion)
        : ReadVersion(readVersion)
        , WriteVersion(writeVersion)
    {}

    TReadWriteVersions(const TRowVersion& version)
        : ReadVersion(version)
        , WriteVersion(version)
    {}

    const TRowVersion ReadVersion;
    const TRowVersion WriteVersion;
};

enum class TSwitchState {
    READY,
    SWITCHING,
    DONE
};

class TDataShardEngineHost;
struct TSetupSysLocks;

class TNeedGlobalTxId : public yexception {};

///
class TDataShard
    : public TActor<TDataShard>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
    class TTxStopGuard;
    class TTxGetShardState;
    class TTxInit;
    class TTxInitSchema;
    class TTxInitSchemaDefaults;
    class TTxPlanStep;
    class TTxPlanPredictedTxs;
    class TTxProgressResendRS;
    class TTxProgressTransaction;
    class TTxCleanupTransaction;
    class TTxCleanupVolatileTransaction;
    class TTxProposeDataTransaction;
    class TTxProposeSchemeTransaction;
    class TTxCancelTransactionProposal;
    class TTxProposeTransactionBase;
    class TTxWrite;
    class TTxReadSet;
    class TTxSchemaChanged;
    class TTxInitiateBorrowedPartsReturn;
    class TTxReturnBorrowedPart;
    class TTxReturnBorrowedPartAck;
    class TTxInitSplitMergeDestination;
    class TTxSplit;
    class TTxStartSplit;
    class TTxSplitSnapshotComplete;
    class TTxSplitReplicationSourceOffsets;
    class TTxSplitTransferSnapshot;
    class TTxSplitTransferSnapshotAck;
    class TTxSplitPartitioningChanged;
    class TTxStoreTablePath;
    class TTxGoOffline;
    class TTxGetTableStats;
    class TTxMonitoring;
    class TTxMonitoringCleanupBorrowedParts;
    class TTxMonitoringCleanupBorrowedPartsActor;
    class TTxMonitoringResetSchemaVersion;
    class TTxUndelivered;
    class TTxInterruptTransaction;
    class TTxInitiateStatsUpdate;
    class TTxCheckInReadSets;
    class TTxRemoveOldInReadSets;
    class TTxReadContinue;
    class TTxReadColumns;
    class TTxGetInfo;
    class TTxListOperations;
    class TTxGetOperation;
    class TTxStoreScanState;
    class TTxRefreshVolatileSnapshot;
    class TTxDiscardVolatileSnapshot;
    class TTxCleanupRemovedSnapshots;
    class TTxMigrateSchemeShard;
    class TTxGetS3Upload;
    class TTxStoreS3UploadId;
    class TTxChangeS3UploadStatus;
    class TTxGetS3DownloadInfo;
    class TTxStoreS3DownloadInfo;
    class TTxS3UploadRows;
    class TTxObjectStorageListing;
    class TTxExecuteMvccStateChange;
    class TTxGetRemovedRowVersions;
    class TTxCompactBorrowed;
    class TTxCompactTable;
    class TTxPersistFullCompactionTs;
    class TTxRemoveLock;
    class TTxGetOpenTxs;
    class TTxRemoveLockChangeRecords;
    class TTxVolatileTxCommit;
    class TTxVolatileTxAbort;
    class TTxCdcStreamScanRun;
    class TTxCdcStreamScanProgress;
    class TTxCdcStreamEmitHeartbeats;
    class TTxUpdateFollowerReadEdge;
    class TTxRemoveSchemaSnapshots;

    template <typename T> friend class TTxDirectBase;
    class TTxUploadRows;
    class TTxEraseRows;

    class TTxReadViaPipeline;
    class TReadOperation;

    class TTxHandleSafeKqpScan;
    class TTxHandleSafeBuildIndexScan;
    class TTxHandleSafeSampleKScan;
    class TTxHandleSafeStatisticsScan;

    class TTxMediatorStateRestored;

    ITransaction *CreateTxMonitoring(TDataShard *self,
                                     NMon::TEvRemoteHttpInfo::TPtr ev);
    ITransaction *CreateTxGetInfo(TDataShard *self,
                                  TEvDataShard::TEvGetInfoRequest::TPtr ev);
    ITransaction *CreateTxListOperations(TDataShard *self,
                                         TEvDataShard::TEvListOperationsRequest::TPtr ev);
    ITransaction *CreateTxGetOperation(TDataShard *self,
                                       TEvDataShard::TEvGetOperationRequest::TPtr ev);

    ITransaction *CreateTxMonitoringCleanupBorrowedParts(
            TDataShard *self,
            NMon::TEvRemoteHttpInfo::TPtr ev);

    ITransaction *CreateTxMonitoringResetSchemaVersion(
            TDataShard *self,
            NMon::TEvRemoteHttpInfo::TPtr ev);

    friend class TDataShardMiniKQLFactory;
    friend class TDataTransactionProcessor;
    friend class TSchemeTransactionProcessor;
    friend class TScanTransactionProcessor;
    friend class TDataShardEngineHost;
    friend class TExecuteKqpScanTxUnit;
    friend class TTableScan;
    friend class TKqpScan;

    friend class TTransQueue;
    friend class TOutReadSets;
    friend class TPipeline;
    friend class TLocksDataShardAdapter<TDataShard>;
    friend class TActiveTransaction;
    friend class TWriteOperation;
    friend class TValidatedDataTx;
    friend class TValidatedWriteTx;
    friend class TEngineBay;
    friend class NMiniKQL::TKqpScanComputeContext;
    friend class TSnapshotManager;
    friend class TSchemaSnapshotManager;
    friend class TVolatileTxManager;
    friend class TConflictsCache;
    friend class TCdcStreamScanManager;
    friend class TCdcStreamHeartbeatManager;
    friend class TReplicationSourceOffsetsClient;
    friend class TReplicationSourceOffsetsServer;

    friend class TTableStatsCoroBuilder;
    friend class TReadTableScan;
    friend class TWaitForStreamClearanceUnit;
    friend class TBuildIndexScan;
    friend class TReadColumnsScan;
    friend class TCondEraseScan;
    friend class TCdcStreamScan;
    friend class TDatashardKeySampler;

    friend class TS3UploadsManager;
    friend class TS3DownloadsManager;
    friend class TS3Downloader;
    template <typename T> friend class TBackupRestoreUnitBase;
    friend struct TSetupSysLocks;
    friend class TDataShardLocksDb;

    friend class TTxStartMvccStateChange;
    friend class TTxExecuteMvccStateChange;

    friend class TAsyncIndexChangeSenderShard;

    class TTxPersistSubDomainPathId;
    class TTxPersistSubDomainOutOfSpace;

    class TTxRequestChangeRecords;
    class TTxRemoveChangeRecords;
    class TTxChangeExchangeHandshake;
    class TTxApplyChangeRecords;
    class TTxActivateChangeSender;
    class TTxActivateChangeSenderAck;
    class TTxChangeExchangeSplitAck;

    class TTxApplyReplicationChanges;

    class TWaitVolatileDependencies;
    class TSendVolatileResult;
    class TSendVolatileWriteResult;
    class TSendArbiterReadSets;

    struct TEvPrivate {
        enum EEv {
            EvProgressTransaction = EventSpaceBegin(TKikimrEvents::ES_PRIVATE), // WARNING: tests use ES_PRIVATE + 0
            EvCleanupTransaction,
            EvDelayedProposeTransaction, // WARNING: tests use ES_PRIVATE + 2
            EvProgressResendReadSet,
            EvFlushOperationCounters,
            EvDelayedFlushOperationCounters,
            EvProgressOperationHistogramScan,
            EvPeriodicWakeup,
            EvAsyncTableStats,
            EvRemoveOldInReadSets, // WARNING: tests use ES_PRIVATE + 9
            EvRegisterScanActor,
            EvNodeDisconnected,
            EvScanStats,
            EvPersistScanState,
            EvPersistScanStateAck,
            EvConditionalEraseRowsRegistered,
            EvAsyncJobComplete,
            EvSubDomainPathIdFound, // unused
            EvRequestChangeRecords,
            EvRemoveChangeRecords,
            EvReplicationSourceOffsets,
            EvMediatorRestoreBackup,
            EvRemoveLockChangeRecords,
            EvCdcStreamScanRegistered,
            EvCdcStreamScanProgress, // WARNING: tests use ES_PRIVATE + 24
            EvCdcStreamScanContinue,
            EvRestartOperation, // used to restart after an aborted scan (e.g. backup)
            EvChangeExchangeExecuteHandshakes,
            EvConfirmReadonlyLease,
            EvReadonlyLeaseConfirmation,
            EvPlanPredictedTxs,
            EvStatisticsScanFinished,
            EvTableStatsError,
            EvRemoveSchemaSnapshots,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvProgressTransaction : public TEventLocal<TEvProgressTransaction, EvProgressTransaction> {};
        struct TEvCleanupTransaction : public TEventLocal<TEvCleanupTransaction, EvCleanupTransaction> {};
        struct TEvDelayedProposeTransaction : public TEventLocal<TEvDelayedProposeTransaction, EvDelayedProposeTransaction> {};

        struct TEvProgressResendReadSet : public TEventLocal<TEvProgressResendReadSet, EvProgressResendReadSet> {
            TEvProgressResendReadSet(ui64 seqno)
                : Seqno(seqno)
            {}

            const ui64 Seqno;
        };

        struct TEvFlushOperationCounters : public TEventLocal<TEvFlushOperationCounters, EvFlushOperationCounters> {};
        struct TEvDelayedFlushOperationCounters : public TEventLocal<TEvDelayedFlushOperationCounters, EvDelayedFlushOperationCounters> {};

        struct TEvProgressOperationHistogramScan : public TEventLocal<TEvProgressOperationHistogramScan, EvProgressOperationHistogramScan> {};

        struct TEvPeriodicWakeup : public TEventLocal<TEvPeriodicWakeup, EvPeriodicWakeup> {};

        struct TEvAsyncTableStats : public TEventLocal<TEvAsyncTableStats, EvAsyncTableStats> {
            ui64 TableId = -1;
            TInstant StatsUpdateTime;
            NTable::TStats Stats;
            THashSet<ui64> PartOwners;
            ui64 PartCount = 0;
            ui64 MemRowCount = 0;
            ui64 MemDataSize = 0;
            ui64 SearchHeight = 0;
        };

        struct TEvTableStatsError : public TEventLocal<TEvTableStatsError, EvTableStatsError> {
            enum class ECode {
                FETCH_PAGE_FAILED,
                RESOURCE_ALLOCATION_FAILED,
                ACTOR_DIED,
                UNKNOWN
            };

            TEvTableStatsError(ui64 tableId, ECode code, const TString& msg)
                : TableId(tableId)
                , Code(code)
                , Message(msg)
            {}

            TEvTableStatsError(ui64 tableId, ECode code)
                : TEvTableStatsError(tableId, code, "")
            {}

            const ui64 TableId;
            const ECode Code;
            const TString Message;
        };

        struct TEvRemoveOldInReadSets : public TEventLocal<TEvRemoveOldInReadSets, EvRemoveOldInReadSets> {};

        struct TEvRegisterScanActor : public TEventLocal<TEvRegisterScanActor, EvRegisterScanActor> {
            TEvRegisterScanActor(ui64 txId)
                : TxId(txId)
            {
            }

            ui64 TxId;
        };

        struct TEvNodeDisconnected : public TEventLocal<TEvNodeDisconnected, EvNodeDisconnected> {
            TEvNodeDisconnected(ui32 nodeId)
                : NodeId(nodeId)
            {
            }

            ui32 NodeId;
        };

        struct TEvScanStats : public TEventLocal<TEvScanStats, EvScanStats> {
            TEvScanStats(ui64 rows, ui64 bytes) : Rows(rows), Bytes(bytes) {}
            ui64 Rows;
            ui64 Bytes;
        };

        // Also updates scan statistic, i.e. like TEvScanStats but persist state for given tx
        struct TEvPersistScanState : public TEventLocal<TEvPersistScanState, EvPersistScanState> {
            TEvPersistScanState(
                ui64 txId,
                TString lastKey,
                ui64 rows,
                ui64 bytes,
                Ydb::StatusIds::StatusCode statusCode,
                const NYql::TIssues& issues)
                : TxId(txId)
                , LastKey(lastKey)
                , Rows(rows)
                , Bytes(bytes)
                , StatusCode(statusCode)
                , Issues(issues)
            {}
            ui64 TxId;
            TString LastKey;
            ui64 Rows;
            ui64 Bytes;
            Ydb::StatusIds::StatusCode StatusCode;
            NYql::TIssues Issues;
        };

        struct TEvPersistScanStateAck : public TEventLocal<TEvPersistScanStateAck, EvPersistScanStateAck> {};

        struct TEvConditionalEraseRowsRegistered : public TEventLocal<TEvConditionalEraseRowsRegistered, EvConditionalEraseRowsRegistered> {
            explicit TEvConditionalEraseRowsRegistered(ui64 txId, const TActorId& actorId)
                : TxId(txId)
                , ActorId(actorId)
            {
            }

            const ui64 TxId;
            const TActorId ActorId;
        };

        struct TEvAsyncJobComplete : public TEventLocal<TEvAsyncJobComplete, EvAsyncJobComplete> {
            explicit TEvAsyncJobComplete(TAutoPtr<IDestructable> prod)
                : Prod(prod)
            {
            }

            TAutoPtr<IDestructable> Prod;
        };

        struct TEvRequestChangeRecords : public TEventLocal<TEvRequestChangeRecords, EvRequestChangeRecords> {};
        struct TEvRemoveChangeRecords : public TEventLocal<TEvRemoveChangeRecords, EvRemoveChangeRecords> {};

        struct TEvReplicationSourceOffsets : public TEventLocal<TEvReplicationSourceOffsets, EvReplicationSourceOffsets> {
            struct TSplitKey {
                TSerializedCellVec SplitKey;
                i64 MaxOffset = -1;
            };

            TEvReplicationSourceOffsets(ui64 srcTabletId, const TPathId& pathId)
                : SrcTabletId(srcTabletId)
                , PathId(pathId)
            { }

            const ui64 SrcTabletId;
            const TPathId PathId;

            // Source -> SplitKey -> MaxOffset
            // Note that keys are NOT sorted in any way
            THashMap<TString, TVector<TSplitKey>> SourceOffsets;
        };

        struct TEvMediatorRestoreBackup : public TEventLocal<TEvMediatorRestoreBackup, EvMediatorRestoreBackup> {};

        struct TEvRemoveLockChangeRecords : public TEventLocal<TEvRemoveLockChangeRecords, EvRemoveLockChangeRecords> {};

        struct TEvCdcStreamScanRegistered : public TEventLocal<TEvCdcStreamScanRegistered, EvCdcStreamScanRegistered> {
            explicit TEvCdcStreamScanRegistered(ui64 txId, const TActorId& actorId)
                : TxId(txId)
                , ActorId(actorId)
            {
            }

            const ui64 TxId;
            const TActorId ActorId;
        };

        struct TEvCdcStreamScanProgress : public TEventLocal<TEvCdcStreamScanProgress, EvCdcStreamScanProgress> {
            explicit TEvCdcStreamScanProgress(
                    const TPathId& tablePathId,
                    const TPathId& streamPathId,
                    const TRowVersion& readVersion,
                    const TVector<ui32>& valueTags,
                    TVector<std::pair<TSerializedCellVec, TSerializedCellVec>>&& rows,
                    const TCdcStreamScanManager::TStats& stats)
                : TablePathId(tablePathId)
                , StreamPathId(streamPathId)
                , ReadVersion(readVersion)
                , ValueTags(valueTags)
                , Rows(std::move(rows))
                , Stats(stats)
            {
            }

            const TPathId TablePathId;
            const TPathId StreamPathId;
            const TRowVersion ReadVersion;
            const TVector<ui32> ValueTags;
            TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> Rows;
            ui64 ReservationCookie = 0;
            const TCdcStreamScanManager::TStats Stats;
        };

        struct TEvCdcStreamScanContinue : public TEventLocal<TEvCdcStreamScanContinue, EvCdcStreamScanContinue> {};

        struct TEvRestartOperation : public TEventLocal<TEvRestartOperation, EvRestartOperation> {
            explicit TEvRestartOperation(ui64 txId)
                : TxId(txId)
            {
            }

            const ui64 TxId;
        };

        struct TEvChangeExchangeExecuteHandshakes : public TEventLocal<TEvChangeExchangeExecuteHandshakes, EvChangeExchangeExecuteHandshakes> {};

        struct TEvConfirmReadonlyLease : public TEventLocal<TEvConfirmReadonlyLease, EvConfirmReadonlyLease> {
            explicit TEvConfirmReadonlyLease(TMonotonic ts = TMonotonic::Zero())
                : Timestamp(ts)
            {
            }

            const TMonotonic Timestamp;
        };

        struct TEvReadonlyLeaseConfirmation: public TEventLocal<TEvReadonlyLeaseConfirmation, EvReadonlyLeaseConfirmation> {};

        struct TEvPlanPredictedTxs : public TEventLocal<TEvPlanPredictedTxs, EvPlanPredictedTxs> {};

        struct TEvStatisticsScanFinished : public TEventLocal<TEvStatisticsScanFinished, EvStatisticsScanFinished> {};

        struct TEvRemoveSchemaSnapshots : public TEventLocal<TEvRemoveSchemaSnapshots, EvRemoveSchemaSnapshots> {};
    };

    struct Schema : NIceDb::Schema {
        struct Sys : Table<1> {
            struct Id :             Column<1, NScheme::NTypeIds::Uint64> {};
            struct Bytes :          Column<2, NScheme::NTypeIds::String> {};
            struct Uint64 :         Column<3, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<Id>;
            using TColumns = TableColumns<Id, Bytes, Uint64>;
        };

        // Note that table UserTablesStats must be always updated with this one
        struct UserTables : Table<2> {
            struct Tid :            Column<1, NScheme::NTypeIds::Uint64> {};
            struct LocalTid :       Column<2, NScheme::NTypeIds::Uint32> {};
            struct Path :           Column<3, NScheme::NTypeIds::String> {};
            struct Name :           Column<4, NScheme::NTypeIds::String> {};
            struct Schema :         Column<5, NScheme::NTypeIds::String> { using Type = TString; };
            struct ShadowTid :      Column<6, NScheme::NTypeIds::Uint32> { static constexpr ui32 Default = 0; };

            using TKey = TableKey<Tid>;
            using TColumns = TableColumns<Tid, LocalTid, Path, Name, Schema, ShadowTid>;
        };

        struct TxMain : Table<3> {
            struct TxId :           Column<1, NScheme::NTypeIds::Uint64> {};
            struct Kind :           Column<2, NScheme::NTypeIds::Uint32> { using Type = EOperationKind; };
            struct Flags :          Column<3, NScheme::NTypeIds::Uint32> {};
            struct State :          Column<4, NScheme::NTypeIds::Uint32> {};
            struct InRSRemain :     Column<5, NScheme::NTypeIds::Uint64> {};
            struct MaxStep :        Column<6, NScheme::NTypeIds::Uint64> {};
            struct ReceivedAt :     Column<7, NScheme::NTypeIds::Uint64> {};
            struct Flags64 :        Column<8, NScheme::NTypeIds::Uint64> {};
            struct Source :         Column<9, NScheme::NTypeIds::ActorId> {};
            struct Cookie :         Column<10, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<TxId>;
            using TColumns = TableColumns<TxId, Kind, Flags, State, InRSRemain, MaxStep, ReceivedAt, Flags64, Source, Cookie>;
        };

        struct TxDetails : Table<4> {
            struct TxId :           Column<1, NScheme::NTypeIds::Uint64> {};
            struct Origin :         Column<2, NScheme::NTypeIds::Uint64> {};
            struct InReadSetState : Column<3, NScheme::NTypeIds::Uint64> {}; // Not used
            struct Body :           Column<4, NScheme::NTypeIds::String> { using Type = TString; };
            struct Source :         Column<5, NScheme::NTypeIds::ActorId> {};

            using TKey = TableKey<TxId, Origin>;
            using TColumns = TableColumns<TxId, Origin, InReadSetState, Body, Source>;
        };

        struct InReadSets : Table<5> {
            struct TxId :           Column<1, NScheme::NTypeIds::Uint64> {};
            struct Origin :         Column<2, NScheme::NTypeIds::Uint64> {};
            struct From :           Column<3, NScheme::NTypeIds::Uint64> {};
            struct To :             Column<4, NScheme::NTypeIds::Uint64> {};
            struct Body :           Column<5, NScheme::NTypeIds::String> { using Type = TString; };
            struct BalanceTrackList :      Column<6, NScheme::NTypeIds::String> { using Type = TString; };

            using TKey = TableKey<TxId, Origin, From, To>;
            using TColumns = TableColumns<TxId, Origin, From, To, Body, BalanceTrackList>;
        };

        struct OutReadSets : Table<6> {
            struct Seqno :          Column<1, NScheme::NTypeIds::Uint64> {};
            struct Step :           Column<2, NScheme::NTypeIds::Uint64> {};
            struct TxId :           Column<3, NScheme::NTypeIds::Uint64> {};
            struct Origin :         Column<4, NScheme::NTypeIds::Uint64> {};
            struct From :           Column<5, NScheme::NTypeIds::Uint64> {};
            struct To :             Column<6, NScheme::NTypeIds::Uint64> {};
            struct Body :           Column<7, NScheme::NTypeIds::String> { using Type = TString; };
            struct SplitTraj :      Column<8, NScheme::NTypeIds::String> { using Type = TString; };

            using TKey = TableKey<Seqno>;
            using TColumns = TableColumns<Seqno, Step, TxId, Origin, From, To, Body, SplitTraj>;
        };

        struct PlanQueue : Table<7> {
            struct Step :           Column<1, NScheme::NTypeIds::Uint64> {};
            struct TxId :           Column<2, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<Step, TxId>;
            using TColumns = TableColumns<Step, TxId>;
        };

        struct DeadlineQueue : Table<8> {
            struct MaxStep :        Column<1, NScheme::NTypeIds::Uint64> {};
            struct TxId :           Column<2, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<MaxStep, TxId>;
            using TColumns = TableColumns<MaxStep, TxId>;
        };

        struct SchemaOperations : Table<9> {
            struct TxId :           Column<1, NScheme::NTypeIds::Uint64> {};
            struct Operation :      Column<2, NScheme::NTypeIds::Uint32> {};
            struct Source :         Column<3, NScheme::NTypeIds::ActorId> {};
            struct SourceTablet :   Column<4, NScheme::NTypeIds::Uint64> {};
            struct MinStep :        Column<5, NScheme::NTypeIds::Uint64> {};
            struct MaxStep :        Column<6, NScheme::NTypeIds::Uint64> {};
            struct PlanStep :       Column<7, NScheme::NTypeIds::Uint64> {};
            struct ReadOnly :       Column<8, NScheme::NTypeIds::Bool> {};

            struct Success :        Column<9, NScheme::NTypeIds::Bool> {};
            struct Error :          Column<10, NScheme::NTypeIds::String> { using Type = TString; };
            struct DataSize :       Column<11, NScheme::NTypeIds::Uint64> {}; // Bytes
            struct Rows :           Column<12, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<TxId>;
            using TColumns = TableColumns<TxId, Operation, Source, SourceTablet,
                MinStep, MaxStep, PlanStep, ReadOnly, Success, Error, DataSize, Rows>;
        };

        // Here we persist snapshots metadata to preserve it across Src datashard restarts
        struct SplitSrcSnapshots : Table<10> {
            struct DstTabletId :    Column<1, NScheme::NTypeIds::Uint64> {};
            struct SnapshotMeta :   Column<2, NScheme::NTypeIds::String> { using Type = TString; };

            using TKey = TableKey<DstTabletId>;
            using TColumns = TableColumns<DstTabletId, SnapshotMeta>;
        };

        // Here we persist the fact that snapshot has ben received by Dst datashard
        struct SplitDstReceivedSnapshots : Table<11> {
            struct SrcTabletId :    Column<1, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<SrcTabletId>;
            using TColumns = TableColumns<SrcTabletId>;
        };

        // Additional tx artifacts which can be reused on tx restart.
        struct TxArtifacts : Table<12> {
            struct TxId :            Column<1, NScheme::NTypeIds::Uint64> {};
            // Specify which tx artifacts have been stored to local DB and can be
            // reused on tx replay. See TActiveTransaction::EArtifactFlags.
            struct Flags :           Column<2, NScheme::NTypeIds::Uint64> {};
            struct Locks :           Column<3, NScheme::NTypeIds::String> { using Type = TStringBuf; };

            using TKey = TableKey<TxId>;
            using TColumns = TableColumns<TxId, Flags, Locks>;
        };

        struct ScanProgress : Table<13> {
            struct TxId :            Column<1, NScheme::NTypeIds::Uint64> {};
            struct LastKey :         Column<2, NScheme::NTypeIds::String> {};
            struct LastBytes :       Column<3, NScheme::NTypeIds::Uint64> {};
            struct LastStatus :      Column<4, NScheme::NTypeIds::Uint64> {};
            struct LastIssues :      Column<5, NScheme::NTypeIds::String> { using Type = TString; };

            using TKey = TableKey<TxId>;
            using TColumns = TableColumns<TxId, LastKey, LastBytes, LastStatus, LastIssues>;
        };

        struct Snapshots : Table<14> {
            struct Oid :             Column<1, NScheme::NTypeIds::Uint64> {}; // PathOwnerId
            struct Tid :             Column<2, NScheme::NTypeIds::Uint64> {}; // LocalPathId
            struct Step :            Column<3, NScheme::NTypeIds::Uint64> {};
            struct TxId :            Column<4, NScheme::NTypeIds::Uint64> {};
            struct Name :            Column<5, NScheme::NTypeIds::String> {};
            struct Flags :           Column<6, NScheme::NTypeIds::Uint64> {};
            struct TimeoutMs :       Column<7, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<Oid, Tid, Step, TxId>;
            using TColumns = TableColumns<Oid, Tid, Step, TxId, Name, Flags, TimeoutMs>;
        };

        struct S3Uploads : Table<15> {
            struct TxId :            Column<1, NScheme::NTypeIds::Uint64> {};
            struct UploadId :        Column<2, NScheme::NTypeIds::String> { using Type = TString; };
            struct Status :          Column<3, NScheme::NTypeIds::Uint8> { using Type = TS3Upload::EStatus; static constexpr Type Default = TS3Upload::EStatus::UploadParts; };
            struct Error :           Column<4, NScheme::NTypeIds::Utf8> { using Type = TString; };

            using TKey = TableKey<TxId>;
            using TColumns = TableColumns<TxId, UploadId, Status, Error>;
        };

        // deprecated
        struct S3UploadParts : Table<20> {
            struct TxId :            Column<1, NScheme::NTypeIds::Uint64> {};
            struct PartNumber :      Column<2, NScheme::NTypeIds::Uint32> {};
            struct ETag :            Column<3, NScheme::NTypeIds::String> { using Type = TString; };

            using TKey = TableKey<TxId>;
            using TColumns = TableColumns<TxId, PartNumber, ETag>;
        };

        struct S3UploadedParts : Table<23> {
            struct TxId :            Column<1, NScheme::NTypeIds::Uint64> {};
            struct PartNumber :      Column<2, NScheme::NTypeIds::Uint32> {};
            struct ETag :            Column<3, NScheme::NTypeIds::String> { using Type = TString; };

            using TKey = TableKey<TxId, PartNumber>;
            using TColumns = TableColumns<TxId, PartNumber, ETag>;
        };

        struct S3Downloads : Table<16> {
            struct TxId :            Column<1, NScheme::NTypeIds::Uint64> {};
            struct SchemeETag :      Column<2, NScheme::NTypeIds::String> { using Type = TString; };
            struct DataETag :        Column<3, NScheme::NTypeIds::String> { using Type = TString; };
            struct ProcessedBytes :  Column<4, NScheme::NTypeIds::Uint64> {};
            struct WrittenBytes :    Column<5, NScheme::NTypeIds::Uint64> {};
            struct WrittenRows :     Column<6, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<TxId>;
            using TColumns = TableColumns<TxId, SchemeETag, DataETag, ProcessedBytes, WrittenBytes, WrittenRows>;
        };

        struct ChangeRecords : Table<17> {
            struct Order :       Column<1, NScheme::NTypeIds::Uint64> {};
            struct Group :       Column<2, NScheme::NTypeIds::Uint64> {};
            struct PlanStep :    Column<3, NScheme::NTypeIds::Uint64> {};
            struct TxId :        Column<4, NScheme::NTypeIds::Uint64> {};
            struct PathOwnerId : Column<5, NScheme::NTypeIds::Uint64> {};
            struct LocalPathId : Column<6, NScheme::NTypeIds::Uint64> {};
            struct BodySize    : Column<7, NScheme::NTypeIds::Uint64> {};
            struct SchemaVersion : Column<8, NScheme::NTypeIds::Uint64> {};
            struct TableOwnerId :  Column<9, NScheme::NTypeIds::Uint64> {};
            struct TablePathId :   Column<10, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<Order>;
            using TColumns = TableColumns<
                Order,
                Group,
                PlanStep,
                TxId,
                PathOwnerId,
                LocalPathId,
                BodySize,
                SchemaVersion,
                TableOwnerId,
                TablePathId
            >;
        };

        struct ChangeRecordDetails : Table<18> {
            struct Order : Column<1, NScheme::NTypeIds::Uint64> {};
            struct Kind :  Column<2, NScheme::NTypeIds::Uint8> { using Type = TChangeRecord::EKind; };
            struct Body :  Column<3, NScheme::NTypeIds::String> { using Type = TString; };
            struct Source :  Column<4, NScheme::NTypeIds::Uint8> { using Type = TChangeRecord::ESource; };

            using TKey = TableKey<Order>;
            using TColumns = TableColumns<Order, Kind, Body, Source>;
        };

        struct ChangeSenders : Table<19> {
            struct Origin :          Column<1, NScheme::NTypeIds::Uint64> {};
            struct Generation :      Column<2, NScheme::NTypeIds::Uint64> {};
            struct LastRecordOrder : Column<3, NScheme::NTypeIds::Uint64> {};
            struct LastSeenAt :      Column<4, NScheme::NTypeIds::Uint64> { using Type = TInstant::TValue; };

            using TKey = TableKey<Origin>;
            using TColumns = TableColumns<Origin, Generation, LastRecordOrder, LastSeenAt>;
        };

        // Table<20> was taken by S3UploadParts

        struct SrcChangeSenderActivations : Table<21> {
            struct DstTabletId : Column<1, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<DstTabletId>;
            using TColumns = TableColumns<DstTabletId>;
        };

        struct DstChangeSenderActivations : Table<22> {
            struct SrcTabletId : Column<1, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<SrcTabletId>;
            using TColumns = TableColumns<SrcTabletId>;
        };

        // Table<23> is taken by S3UploadedParts

        struct ReplicationSourceOffsets : Table<24> {
            struct PathOwnerId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct TablePathId : Column<2, NScheme::NTypeIds::Uint64> {};
            struct SourceId : Column<3, NScheme::NTypeIds::Uint64> {};
            struct SplitKeyId : Column<4, NScheme::NTypeIds::Uint64> {};
            // Note: Column Source (id=5) was abandoned, but may be present in some trunk clusters
            struct SplitKey : Column<6, NScheme::NTypeIds::String> {}; // TSerializedCellVec of PK columns
            struct MaxOffset : Column<7, NScheme::NTypeIds::Int64> {};

            using TKey = TableKey<PathOwnerId, TablePathId, SourceId, SplitKeyId>;
            using TColumns = TableColumns<PathOwnerId, TablePathId, SourceId, SplitKeyId, SplitKey, MaxOffset>;
        };

        struct ReplicationSources : Table<25> {
            struct PathOwnerId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct TablePathId : Column<2, NScheme::NTypeIds::Uint64> {};
            struct SourceId : Column<3, NScheme::NTypeIds::Uint64> {};
            struct SourceName : Column<4, NScheme::NTypeIds::String> {};

            using TKey = TableKey<PathOwnerId, TablePathId, SourceId>;
            using TColumns = TableColumns<PathOwnerId, TablePathId, SourceId, SourceName>;
        };

        struct DstReplicationSourceOffsetsReceived : Table<26> {
            struct SrcTabletId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct PathOwnerId : Column<2, NScheme::NTypeIds::Uint64> {};
            struct TablePathId : Column<3, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<SrcTabletId, PathOwnerId, TablePathId>;
            using TColumns = TableColumns<SrcTabletId, PathOwnerId, TablePathId>;
        };

        struct UserTablesStats : Table<27> {
            struct Tid :                 Column<1, NScheme::NTypeIds::Uint64> {};

            // seconds since epoch
            struct FullCompactionTs :    Column<2, NScheme::NTypeIds::Uint64> { static constexpr ui64 Default = 0; };

            using TKey = TableKey<Tid>;
            using TColumns = TableColumns<Tid, FullCompactionTs>;
        };

        struct SchemaSnapshots : Table<28> {
            struct PathOwnerId :   Column<1, NScheme::NTypeIds::Uint64> {};
            struct LocalPathId :   Column<2, NScheme::NTypeIds::Uint64> {};
            struct SchemaVersion : Column<3, NScheme::NTypeIds::Uint64> {};
            struct Step :          Column<4, NScheme::NTypeIds::Uint64> {};
            struct TxId :          Column<5, NScheme::NTypeIds::Uint64> {};
            struct Schema :        Column<6, NScheme::NTypeIds::String> {};

            using TKey = TableKey<PathOwnerId, LocalPathId, SchemaVersion>;
            using TColumns = TableColumns<PathOwnerId, LocalPathId, SchemaVersion, Step, TxId, Schema>;
        };

        struct Locks : Table<29> {
            struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct LockNodeId : Column<2, NScheme::NTypeIds::Uint32> {};
            struct Generation : Column<3, NScheme::NTypeIds::Uint32> {};
            struct Counter : Column<4, NScheme::NTypeIds::Uint64> {};
            struct CreateTimestamp : Column<5, NScheme::NTypeIds::Uint64> {};
            struct Flags : Column<6, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<LockId>;
            using TColumns = TableColumns<LockId, LockNodeId, Generation, Counter, CreateTimestamp, Flags>;
        };

        struct LockRanges : Table<30> {
            struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct RangeId : Column<2, NScheme::NTypeIds::Uint64> {};
            struct PathOwnerId : Column<3, NScheme::NTypeIds::Uint64> {};
            struct LocalPathId : Column<4, NScheme::NTypeIds::Uint64> {};
            struct Flags : Column<5, NScheme::NTypeIds::Uint64> {};
            struct Data : Column<6, NScheme::NTypeIds::String> {};

            using TKey = TableKey<LockId, RangeId>;
            using TColumns = TableColumns<LockId, RangeId, PathOwnerId, LocalPathId, Flags, Data>;
        };

        struct LockConflicts : Table<31> {
            struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct ConflictId : Column<2, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<LockId, ConflictId>;
            using TColumns = TableColumns<LockId, ConflictId>;
        };

        struct LockChangeRecords : Table<101> {
            struct LockId :        Column<1, NScheme::NTypeIds::Uint64> {};
            struct LockOffset :    Column<2, NScheme::NTypeIds::Uint64> {};
            struct PathOwnerId :   Column<3, NScheme::NTypeIds::Uint64> {};
            struct LocalPathId :   Column<4, NScheme::NTypeIds::Uint64> {};
            struct BodySize :      Column<5, NScheme::NTypeIds::Uint64> {};
            struct SchemaVersion : Column<6, NScheme::NTypeIds::Uint64> {};
            struct TableOwnerId :  Column<7, NScheme::NTypeIds::Uint64> {};
            struct TablePathId :   Column<8, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<LockId, LockOffset>;
            using TColumns = TableColumns<
                LockId,
                LockOffset,
                PathOwnerId,
                LocalPathId,
                BodySize,
                SchemaVersion,
                TableOwnerId,
                TablePathId
            >;
        };

        struct LockChangeRecordDetails : Table<102> {
            struct LockId :     Column<1, NScheme::NTypeIds::Uint64> {};
            struct LockOffset : Column<2, NScheme::NTypeIds::Uint64> {};
            struct Kind :       Column<3, NScheme::NTypeIds::Uint8> { using Type = TChangeRecord::EKind; };
            struct Body :       Column<4, NScheme::NTypeIds::String> { using Type = TString; };
            struct Source :     Column<5, NScheme::NTypeIds::Uint8> { using Type = TChangeRecord::ESource; };

            using TKey = TableKey<LockId, LockOffset>;
            using TColumns = TableColumns<LockId, LockOffset, Kind, Body, Source>;
        };

        // Maps [Order ... Order+N-1] change records in the shard order
        // to [0 ... N-1] change records from LockId
        struct ChangeRecordCommits : Table<103> {
            struct Order :         Column<1, NScheme::NTypeIds::Uint64> {};
            struct LockId :        Column<2, NScheme::NTypeIds::Uint64> {};
            struct Group :         Column<3, NScheme::NTypeIds::Uint64> {};
            struct PlanStep :      Column<4, NScheme::NTypeIds::Uint64> {};
            struct TxId :          Column<5, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<Order>;
            using TColumns = TableColumns<
                Order,
                LockId,
                Group,
                PlanStep,
                TxId
            >;
        };

        // Describes a volatile transaction that executed and possibly made
        // some changes, but is not fully committed, waiting for decision from
        // other participants. Transaction would be committed at Step:TxId on
        // success, and usually has 1-2 uncommitted TxIds that need to be
        // committed.
        struct TxVolatileDetails : Table<32> {
            // Volatile TxId of the transaction
            struct TxId : Column<1, NScheme::NTypeIds::Uint64> {};
            // State of transaction, initially undecided, but becomes committed or aborted until it is removed
            struct State : Column<2, NScheme::NTypeIds::Uint32> { using Type = EVolatileTxState; };
            // Transaction details encoded in a protobuf message
            struct Details : Column<3, NScheme::NTypeIds::String> { using Type = NKikimrTxDataShard::TTxVolatileDetails; };

            using TKey = TableKey<TxId>;
            using TColumns = TableColumns<TxId, State, Details>;
        };

        // Associated participants for a volatile transaction that need to
        // decide a transaction and from which a readset is expected. Usually a
        // COMMIT decision from a participant causes removal of a corresponding
        // row, and ABORT decision causes full transaction abort, with removal
        // of all corresponding rows.
        struct TxVolatileParticipants : Table<33> {
            struct TxId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct ShardId : Column<2, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<TxId, ShardId>;
            using TColumns = TableColumns<TxId, ShardId>;
        };

        struct CdcStreamScans : Table<34> {
            struct TableOwnerId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct TablePathId : Column<2, NScheme::NTypeIds::Uint64> {};
            struct StreamOwnerId : Column<3, NScheme::NTypeIds::Uint64> {};
            struct StreamPathId : Column<4, NScheme::NTypeIds::Uint64> {};
            struct LastKey : Column<5, NScheme::NTypeIds::String> {};
            struct SnapshotStep : Column<6, NScheme::NTypeIds::Uint64> {};
            struct SnapshotTxId : Column<7, NScheme::NTypeIds::Uint64> {};
            struct RowsProcessed : Column<8, NScheme::NTypeIds::Uint64> {};
            struct BytesProcessed : Column<9, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<TableOwnerId, TablePathId, StreamOwnerId, StreamPathId>;
            using TColumns = TableColumns<
                TableOwnerId,
                TablePathId,
                StreamOwnerId,
                StreamPathId,
                LastKey,
                SnapshotStep,
                SnapshotTxId,
                RowsProcessed,
                BytesProcessed
            >;
        };

        struct LockVolatileDependencies : Table<35> {
            struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct TxId : Column<2, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<LockId, TxId>;
            using TColumns = TableColumns<LockId, TxId>;
        };

        struct CdcStreamHeartbeats : Table<36> {
            struct TableOwnerId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct TablePathId : Column<2, NScheme::NTypeIds::Uint64> {};
            struct StreamOwnerId : Column<3, NScheme::NTypeIds::Uint64> {};
            struct StreamPathId : Column<4, NScheme::NTypeIds::Uint64> {};
            struct IntervalMs : Column<5, NScheme::NTypeIds::Uint64> {};
            struct LastStep : Column<6, NScheme::NTypeIds::Uint64> {};
            struct LastTxId : Column<7, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<TableOwnerId, TablePathId, StreamOwnerId, StreamPathId>;
            using TColumns = TableColumns<
                TableOwnerId,
                TablePathId,
                StreamOwnerId,
                StreamPathId,
                IntervalMs,
                LastStep,
                LastTxId
            >;
        };

        using TTables = SchemaTables<Sys, UserTables, TxMain, TxDetails, InReadSets, OutReadSets, PlanQueue,
            DeadlineQueue, SchemaOperations, SplitSrcSnapshots, SplitDstReceivedSnapshots, TxArtifacts, ScanProgress,
            Snapshots, S3Uploads, S3Downloads, ChangeRecords, ChangeRecordDetails, ChangeSenders, S3UploadedParts,
            SrcChangeSenderActivations, DstChangeSenderActivations,
            ReplicationSourceOffsets, ReplicationSources, DstReplicationSourceOffsetsReceived,
            UserTablesStats, SchemaSnapshots, Locks, LockRanges, LockConflicts,
            LockChangeRecords, LockChangeRecordDetails, ChangeRecordCommits,
            TxVolatileDetails, TxVolatileParticipants, CdcStreamScans,
            LockVolatileDependencies, CdcStreamHeartbeats>;

        // These settings are persisted on each Init. So we use empty settings in order not to overwrite what
        // was changed by the user
        struct EmptySettings {
            static void Materialize(NIceDb::TToughDb&) {}
        };

        using TSettings = SchemaSettings<EmptySettings>;

        enum ESysTableKeys : ui64 {
            Sys_Config = 1,
            Sys_State,
            Sys_LastPlannedStep,
            Sys_LastPlannedTx,
            Sys_LastCompleteStep,
            Sys_LastCompleteTx,
            Sys_LastLocalTid,
            Sys_NextSeqno, // Next sequence number of out read set
            Sys_AliveStep, // Last known step we shouldn't drop at
            Sys_TxReadSizeLimit_DEPRECATED, // 10// No longer used but is present in old tables
            Sys_CurrentSchemeShardId, // TabletID of the schmemeshard that manages the datashard right now
            Sys_DstSplitDescription, // Split/Merge operation description at destination shard
            Sys_DstSplitOpId,        // TxId of split operation at destination shard
            Sys_SrcSplitDescription, // Split/Merge operation description at source shard
            Sys_SrcSplitOpId,        // TxId of split operation at source shard
            Sys_LastSchemeShardGeneration,  // LastSchemeOpSeqNo.Generation
            Sys_LastSchemeShardRound,       // LastSchemeOpSeqNo.Round
            Sys_TxReadSizeLimit, // Maximum size in bytes that is allowed to be read by a single Tx
            Sys_SubDomainInfo,  //19 Subdomain setting which owns this table
            Sys_StatisticsDisabled,
            Sys_DstSplitSchemaInitialized,
            Sys_MinWriteVersionStep, // 22 Minimum Step for new writes (past known snapshots)
            Sys_MinWriteVersionTxId, // 23 Minimum TxId for new writes (past known snapshots)
            Sys_PathOwnerId, // TabletID of the schmemeshard that allocated the TPathId(ownerId,localId)

            SysMvcc_State,
            SysMvcc_CompleteEdgeStep,
            SysMvcc_CompleteEdgeTxId,
            SysMvcc_IncompleteEdgeStep,
            SysMvcc_IncompleteEdgeTxId,
            SysMvcc_LowWatermarkStep,
            SysMvcc_LowWatermarkTxId,
            SysMvcc_KeepSnapshotTimeout,

            Sys_SubDomainOwnerId, // 33 OwnerId of the subdomain path id
            Sys_SubDomainLocalPathId, // 34 LocalPathId of the subdomain path id
            Sys_SubDomainOutOfSpace, // 35 Boolean flag indicating database is out of space

            Sys_NextChangeRecordOrder, // 36 Next order of change record
            Sys_LastChangeRecordGroup, // 37 Last group number of change records

            SysMvcc_UnprotectedReads, // 38 Shard may have performed unprotected mvcc reads when non-zero
            SysMvcc_ImmediateWriteEdgeStep, // 39 Maximum step of immediate writes with mvcc enabled
            SysMvcc_ImmediateWriteEdgeTxId, // 40 Maximum txId of immediate writes with mvcc enabled

            Sys_LastLoanTableTid, // 41 Last tid that we used in LoanTable

            // The last step:txId that is unconditionally readable on followers
            // without producing possibly inconsistent results. When repeatable
            // is set leader will also never add new writes to this edge, making
            // it possible to use the edge as a local snapshot.
            SysMvcc_FollowerReadEdgeStep = 42,
            SysMvcc_FollowerReadEdgeTxId = 43,
            SysMvcc_FollowerReadEdgeRepeatable = 44,

            // reserved
            SysPipeline_Flags = 1000,
            SysPipeline_LimitActiveTx,
            SysPipeline_LimitDataTxCache,
        };

        static_assert(ESysTableKeys::Sys_SubDomainOwnerId == 33, "Sys_SubDomainOwnerId changed its value");
        static_assert(ESysTableKeys::Sys_SubDomainLocalPathId == 34, "Sys_SubDomainLocalPathId changed its value");
        static_assert(ESysTableKeys::Sys_SubDomainOutOfSpace == 35, "Sys_SubDomainOutOfSpace changed its value");
        static_assert(ESysTableKeys::SysMvcc_UnprotectedReads == 38, "SysMvcc_UnprotectedReads changed its value");
        static_assert(ESysTableKeys::SysMvcc_ImmediateWriteEdgeStep == 39, "SysMvcc_ImmediateWriteEdgeStep changed its value");
        static_assert(ESysTableKeys::SysMvcc_ImmediateWriteEdgeTxId == 40, "SysMvcc_ImmediateWriteEdgeTxId changed its value");
        static_assert(ESysTableKeys::Sys_LastLoanTableTid == 41, "Sys_LastLoanTableTid changed its value");

        static constexpr ui64 MinLocalTid = TSysTables::SysTableMAX + 1; // 1000

        static constexpr const char* UserTablePrefix = "__user__";
        static constexpr const char* ShadowTablePrefix = "__shadow__";
    };

    inline static bool SysGetUi64(NIceDb::TNiceDb& db, ui64 row, ui64& value) {
        auto rowset = db.Table<Schema::Sys>().Key(row).Select<Schema::Sys::Uint64>();
        if (!rowset.IsReady())
            return false;
        if (rowset.IsValid())
            value = rowset.GetValue<Schema::Sys::Uint64>();
        return true;
    }

    inline static bool SysGetUi64(NIceDb::TNiceDb& db, ui64 row, ui32& value) {
        auto rowset = db.Table<Schema::Sys>().Key(row).Select<Schema::Sys::Uint64>();
        if (!rowset.IsReady())
            return false;
        if (rowset.IsValid()) {
            ui64 val = rowset.GetValue<Schema::Sys::Uint64>();
            Y_ABORT_UNLESS(val <= std::numeric_limits<ui32>::max());
            value = static_cast<ui32>(val);
        }
        return true;
    }

    inline static bool SysGetBool(NIceDb::TNiceDb& db, ui64 row, bool& value) {
        auto rowset = db.Table<Schema::Sys>().Key(row).Select<Schema::Sys::Uint64>();
        if (!rowset.IsReady())
            return false;
        if (rowset.IsValid()) {
            ui64 val = rowset.GetValue<Schema::Sys::Uint64>();
            Y_ABORT_UNLESS(val <= 1, "Unexpected bool value %" PRIu64, val);
            value = (val != 0);
        }
        return true;
    }

    inline static bool SysGetBytes(NIceDb::TNiceDb& db, ui64 row, TString& value) {
        auto rowset = db.Table<Schema::Sys>().Key(row).Select<Schema::Sys::Bytes>();
        if (!rowset.IsReady())
            return false;
        if (rowset.IsValid())
            value = rowset.GetValue<Schema::Sys::Bytes>();
        return true;
    }

    template <typename TEvHandle>
    void ForwardEventToOperation(TAutoPtr<TEvHandle> ev, const TActorContext &ctx) {
        TOperation::TPtr op = Pipeline.FindOp(ev->Get()->Record.GetTxId());
        if (op)
            ForwardEventToOperation(ev, op, ctx);
    }

    template <typename TEvHandle>
    void ForwardEventToOperation(TAutoPtr<TEvHandle> ev,
                                 TOperation::TPtr op,
                                 const TActorContext &ctx) {
        op->AddInputEvent(ev.Release());
        Pipeline.AddCandidateOp(op);
        PlanQueue.Progress(ctx);
    }

    void Handle(TEvents::TEvGone::TPtr &ev);
    void Handle(TEvDataShard::TEvGetShardState::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvSchemaChangedResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvStateChangedResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvProposeTransactionAttach::TPtr &ev, const TActorContext &ctx);
    void HandleAsFollower(TEvDataShard::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx);
    void ProposeTransaction(TEvDataShard::TEvProposeTransaction::TPtr &&ev, const TActorContext &ctx);
    void Handle(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx);
    void ProposeTransaction(NEvents::TDataEvents::TEvWrite::TPtr&& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvReadSet::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProcessing::TEvReadSetAck::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvProgressTransaction::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvCleanupTransaction::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvDelayedProposeTransaction::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvProgressResendReadSet::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvRemoveOldInReadSets::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvRegisterScanActor::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvScanStats::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvPersistScanState::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvSubscribeReadStepResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvNotifyPlanStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMediatorRestoreBackup::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvCancelTransactionProposal::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvReturnBorrowedPart::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvReturnBorrowedPartAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvInitSplitMergeDestination::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvSplit::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvSplitTransferSnapshot::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvReplicationSourceOffsets::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvSplitTransferSnapshotAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvSplitPartitioningChanged::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetTableStats::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvAsyncTableStats::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvTableStatsError::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvKqpScan::TPtr& ev, const TActorContext& ctx);
    void HandleSafe(TEvDataShard::TEvKqpScan::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvUploadRowsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvEraseRowsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvOverloadUnsubscribe::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvConditionalEraseRowsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvConditionalEraseRowsRegistered::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvRead::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvReadContinue::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvReadAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvReadCancel::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvReadColumnsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetInfoRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvListOperationsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetDataHistogramRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetOperationRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetReadTableSinkStateRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetReadTableScanStateRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetReadTableStreamStateRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetRSInfoRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetSlowOpProfilesRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvRefreshVolatileSnapshotRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvDiscardVolatileSnapshotRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvMigrateSchemeShardRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetS3Upload::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvStoreS3UploadId::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvChangeS3UploadStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetS3DownloadInfo::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvStoreS3DownloadInfo::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvS3UploadRowsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvObjectStorageListingRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvBuildIndexCreateRequest::TPtr& ev, const TActorContext& ctx);
    void HandleSafe(TEvDataShard::TEvBuildIndexCreateRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvSampleKRequest::TPtr& ev, const TActorContext& ctx);
    void HandleSafe(TEvDataShard::TEvSampleKRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvCdcStreamScanRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvCdcStreamScanRegistered::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvCdcStreamScanProgress::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvAsyncJobComplete::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRestartOperation::TPtr& ev, const TActorContext& ctx);
    void Handle(NStat::TEvStatistics::TEvStatisticsRequest::TPtr& ev, const TActorContext& ctx);
    void HandleSafe(NStat::TEvStatistics::TEvStatisticsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvStatisticsScanFinished::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvDataShard::TEvCancelBackup::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvCancelRestore::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvTxProcessing::TEvStreamClearanceResponse::TPtr &ev, const TActorContext &ctx) {
        ForwardEventToOperation(ev, ctx);
    }
    void Handle(TEvTxProcessing::TEvStreamClearancePending::TPtr &ev, const TActorContext &ctx) {
        ForwardEventToOperation(ev, ctx);
    }
    void Handle(TEvTxProcessing::TEvInterruptTransaction::TPtr &ev, const TActorContext &ctx) {
        ForwardEventToOperation(ev, ctx);
    }
    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr ev, const TActorContext &ctx);

    void Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext& ctx);

    void Handle(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx);

    // change sending
    void Handle(NChangeExchange::TEvChangeExchange::TEvRequestRecords::TPtr& ev, const TActorContext& ctx);
    void Handle(NChangeExchange::TEvChangeExchange::TEvRemoveRecords::TPtr& ev, const TActorContext& ctx);
    void ScheduleRequestChangeRecords(const TActorContext& ctx);
    void ScheduleRemoveChangeRecords(const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRequestChangeRecords::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRemoveChangeRecords::TPtr& ev, const TActorContext& ctx);
    // change receiving
    void Handle(TEvChangeExchange::TEvHandshake::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvChangeExchangeExecuteHandshakes::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvChangeExchange::TEvApplyRecords::TPtr& ev, const TActorContext& ctx);
    // activation
    void Handle(TEvChangeExchange::TEvActivateSender::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvChangeExchange::TEvActivateSenderAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvChangeExchange::TEvSplitAck::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvDataShard::TEvGetRemovedRowVersions::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvDataShard::TEvCompactBorrowed::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvDataShard::TEvCompactTable::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetCompactTableStats::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvDataShard::TEvApplyReplicationChanges::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvLongTxService::TEvLockStatus::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvDataShard::TEvGetOpenTxs::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPrivate::TEvRemoveLockChangeRecords::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPrivate::TEvConfirmReadonlyLease::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPrivate::TEvPlanPredictedTxs::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPrivate::TEvRemoveSchemaSnapshots::TPtr& ev, const TActorContext& ctx);

    void HandleByReplicationSourceOffsetsServer(STATEFN_SIG);

    void DoPeriodicTasks(const TActorContext &ctx);
    void DoPeriodicTasks(TEvPrivate::TEvPeriodicWakeup::TPtr&, const TActorContext &ctx);

    TDuration GetDataTxCompleteLag()
    {
        ui64 mediatorTime = MediatorTimeCastEntry ? MediatorTimeCastEntry->Get(TabletID()) : 0;
        return TDuration::MilliSeconds(Pipeline.GetDataTxCompleteLag(mediatorTime));
    }
    TDuration GetScanTxCompleteLag()
    {
        ui64 mediatorTime = MediatorTimeCastEntry ? MediatorTimeCastEntry->Get(TabletID()) : 0;
        return TDuration::MilliSeconds(Pipeline.GetScanTxCompleteLag(mediatorTime));
    }

    void UpdateLagCounters(const TActorContext &ctx);

    void OnDetach(const TActorContext &ctx) override;
    void OnTabletStop(TEvTablet::TEvTabletStop::TPtr &ev, const TActorContext &ctx) override;
    void OnStopGuardStarting(const TActorContext &ctx);
    void OnStopGuardComplete(const TActorContext &ctx);
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override;
    void IcbRegister();
    bool ReadOnlyLeaseEnabled() override;
    TDuration ReadOnlyLeaseDuration() override;
    void OnActivateExecutor(const TActorContext &ctx) override;

    void Cleanup(const TActorContext &ctx);
    void SwitchToWork(const TActorContext &ctx);
    void SyncConfig();

    TMaybe<TInstant> GetTxPlanStartTimeAndCleanup(ui64 step);

    struct TPersistentTablet;

    TPersistentTablet& SendPersistent(ui64 tabletId, IEventBase* event, ui64 cookie = 0);

    void AckRSToDeletedTablet(ui64 tabletId, TPersistentTablet& state, const TActorContext& ctx);
    void AbortExpectationsFromDeletedTablet(ui64 tabletId, THashMap<ui64, ui64>&& expectations);
    void RestartPipeRS(ui64 tabletId, TPersistentTablet& state, const TActorContext& ctx);

    void DefaultSignalTabletActive(const TActorContext &ctx) override {
        // This overriden in order to pospone SignalTabletActive until TxInit completes
        Y_UNUSED(ctx);
    }

    void PersistSys(NIceDb::TNiceDb& db, ui64 key, const TString& value) const;
    void PersistSys(NIceDb::TNiceDb& db, ui64 key, ui64 value) const;
    void PersistSys(NIceDb::TNiceDb& db, ui64 key, ui32 value) const;
    void PersistSys(NIceDb::TNiceDb& db, ui64 key, bool value) const;
    void PersistUserTable(NIceDb::TNiceDb& db, ui64 tableId, const TUserTable& tableInfo);
    void PersistUserTableFullCompactionTs(NIceDb::TNiceDb& db, ui64 tableId, ui64 ts);
    void PersistMoveUserTable(NIceDb::TNiceDb& db, ui64 prevTableId, ui64 tableId, const TUserTable& tableInfo);

    void DropAllUserTables(TTransactionContext& txc);
    void PurgeTxTables(TTransactionContext& txc);

    bool CheckMediatorAuthorisation(ui64 mediatorId);

    NTabletFlatExecutor::ITransaction* CreateTxInit();
    NTabletFlatExecutor::ITransaction* CreateTxInitSchema();
    NTabletFlatExecutor::ITransaction* CreateTxInitSchemaDefaults();
    NTabletFlatExecutor::ITransaction* CreateTxSchemaChanged(TEvDataShard::TEvSchemaChangedResult::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxStartSplit();
    NTabletFlatExecutor::ITransaction* CreateTxSplitSnapshotComplete(TIntrusivePtr<TSplitSnapshotContext> snapContext);
    NTabletFlatExecutor::ITransaction* CreateTxSplitPartitioningChanged(THashMap<TActorId, THashSet<ui64>>&& waiters);
    NTabletFlatExecutor::ITransaction* CreateTxInitiateBorrowedPartsReturn();
    NTabletFlatExecutor::ITransaction* CreateTxCheckInReadSets();
    NTabletFlatExecutor::ITransaction* CreateTxRemoveOldInReadSets();
    NTabletFlatExecutor::ITransaction* CreateTxExecuteMvccStateChange();

    TReadWriteVersions GetLocalReadWriteVersions() const;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_DATASHARD_ACTOR;
    }

    TDataShard(const TActorId &tablet, TTabletStorageInfo *info);


    void PrepareAndSaveOutReadSets(ui64 step,
                                   ui64 txId,
                                   const TMap<std::pair<ui64, ui64>, TString>& outReadSets,
                                   TVector<THolder<TEvTxProcessing::TEvReadSet>> &preparedRS,
                                   TTransactionContext& txc,
                                   const TActorContext& ctx);
    THolder<TEvTxProcessing::TEvReadSet> PrepareReadSet(ui64 step, ui64 txId, ui64 source, ui64 target,
                                                        const TString& body, ui64 seqno);
    THolder<TEvTxProcessing::TEvReadSet> PrepareReadSetExpectation(ui64 step, ui64 txId, ui64 source, ui64 target);
    void SendReadSet(const TActorContext& ctx, THolder<TEvTxProcessing::TEvReadSet>&& rs);
    void SendReadSet(const TActorContext& ctx, ui64 step, ui64 txId, ui64 source, ui64 target, const TString& body, ui64 seqno);
    bool AddExpectation(ui64 target, ui64 step, ui64 txId);
    bool RemoveExpectation(ui64 target, ui64 txId);
    void SendReadSetExpectation(const TActorContext& ctx, ui64 step, ui64 txId, ui64 source, ui64 target);
    std::unique_ptr<IEventHandle> GenerateReadSetNoData(const TActorId& recipient, ui64 step, ui64 txId, ui64 source, ui64 target);
    void SendReadSetNoData(const TActorContext& ctx, const TActorId& recipient, ui64 step, ui64 txId, ui64 source, ui64 target);
    bool ProcessReadSetExpectation(TEvTxProcessing::TEvReadSet::TPtr& ev);
    void SendReadSets(const TActorContext& ctx,
                      TVector<THolder<TEvTxProcessing::TEvReadSet>> &&readsets);
    void ResendReadSet(const TActorContext& ctx, ui64 step, ui64 txId, ui64 source, ui64 target, const TString& body, ui64 seqno);
    void SendDelayedAcks(const TActorContext& ctx, TVector<THolder<IEventHandle>>& delayedAcks) const;
    void GetCleanupReplies(const TOperation::TPtr& op, std::vector<std::unique_ptr<IEventHandle>>& cleanupReplies);
    void SendConfirmedReplies(TMonotonic ts, std::vector<std::unique_ptr<IEventHandle>>&& replies);
    void SendCommittedReplies(std::vector<std::unique_ptr<IEventHandle>>&& replies);

    void WaitVolatileDependenciesThenSend(
            const absl::flat_hash_set<ui64>& dependencies,
            const TActorId& target, std::unique_ptr<IEventBase> event,
            ui64 cookie = 0);

    void SendResult(const TActorContext &ctx, TOutputOpData::TResultPtr &result, const TActorId &target, ui64 step, ui64 txId,
        NWilson::TTraceId traceId);
    void SendWriteResult(const TActorContext& ctx, std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>& result, const TActorId& target, ui64 step, ui64 txId,
        NWilson::TTraceId traceId);

    void FillSplitTrajectory(ui64 origin, NKikimrTx::TBalanceTrackList& tracks);

    void SetCounter(NDataShard::ESimpleCounters counter, ui64 num) const {
        TabletCounters->Simple()[counter].Set(num);
    }

    void DecCounter(NDataShard::ESimpleCounters counter, ui64 num = 1) const {
        TabletCounters->Simple()[counter].Sub(num);
    }

    void IncCounter(NDataShard::ESimpleCounters counter, ui64 num = 1) const {
        TabletCounters->Simple()[counter].Add(num);
    }

    void IncCounter(NDataShard::ECumulativeCounters counter, ui64 num = 1) const {
        TabletCounters->Cumulative()[counter].Increment(num);
    }

    void IncCounter(NDataShard::EPercentileCounters counter, ui64 num) const {
        TabletCounters->Percentile()[counter].IncrementFor(num);
    }

    void IncCounter(NDataShard::EPercentileCounters counter, const TDuration& latency) const {
        TabletCounters->Percentile()[counter].IncrementFor(latency.MilliSeconds());
    }

    static NDataShard::ECumulativeCounters NotEnoughMemoryCounter(ui64 count) {
        if (count == 1)
            return COUNTER_TX_NOT_ENOUGH_MEMORY_1;
        if (count == 2)
            return COUNTER_TX_NOT_ENOUGH_MEMORY_2;
        if (count == 3)
            return COUNTER_TX_NOT_ENOUGH_MEMORY_3;
        return COUNTER_TX_NOT_ENOUGH_MEMORY_4;
    }

    bool IsStateActive() const {
        return State == TShardState::Ready ||
                State == TShardState::Readonly ||
                State == TShardState::WaitScheme ||
                State == TShardState::SplitSrcWaitForNoTxInFlight ||
                State == TShardState::Frozen;
    }

    bool IsStateNewReadAllowed() const {
        return State == TShardState::Ready ||
                State == TShardState::Readonly ||
                State == TShardState::Frozen;
    }

    bool IsStateFrozen() const {
        return State == TShardState::Frozen;
    }

    bool IsReplicated() const {
        for (const auto& [_, info] : TableInfos) {
            if (info->IsReplicated()) {
                return true;
            }
        }
        return false;
    }

    ui32 Generation() const { return Executor()->Generation(); }
    bool IsFollower() const { return Executor()->GetStats().IsFollower; }
    bool SyncSchemeOnFollower(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx,
                           NKikimrTxDataShard::TError::EKind & status, TString& errMessage);

    ui64 GetMaxTxInFly() { return MaxTxInFly; }

    static constexpr ui64 DefaultTxStepDeadline() { return 30 * 1000; }

    ui64 TxInFly() const { return TransQueue.TxInFly(); }
    ui64 TxPlanned() const { return TransQueue.TxPlanned(); }
    ui64 TxPlanWaiting() const { return TransQueue.TxPlanWaiting(); }
    ui64 ImmediateInFly() const { return Pipeline.ImmediateInFly(); }
    ui64 TxWaiting() const { return Pipeline.WaitingTxs() + Pipeline.WaitingReadIterators(); }

    // note that not part of ImmediateInFly() to not block scheme ops:
    // we rather abort iterator if scheme changes between iterations
    ui64 ReadIteratorsInFly() const { return ReadIterators.size();}

    inline TRowVersion LastCompleteTxVersion() const {
        auto order = Pipeline.GetLastCompleteTx();
        return TRowVersion(order.Step, order.TxId);
    }

    bool CanDrop() const {
        Y_ABORT_UNLESS(State != TShardState::Offline, "Unexpexted repeated drop");
        // FIXME: why are we waiting for OutReadSets.Empty()?
        return (TxInFly() == 1) && OutReadSets.Empty() && (State != TShardState::PreOffline);
    }

    void UpdateProposeQueueSize() const;

    void CheckDelayedProposeQueue(const TActorContext &ctx);

    bool CheckDataTxReject(const TString& opDescr,
                           const TActorContext &ctx,
                           NKikimrTxDataShard::TEvProposeTransactionResult::EStatus& rejectStatus,
                           ERejectReasons& rejectReasons,
                           TString& rejectDescription);
    bool CheckDataTxRejectAndReply(const TEvDataShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx);
    bool CheckDataTxRejectAndReply(const NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx);

    TSysLocks& SysLocksTable() { return SysLocks; }

    static const TString& GetUserTablePrefix() {
        static TString prefix = Schema::UserTablePrefix;
        return prefix;
    }

    bool HasUserTable(const TPathId& tableId) {
        return TableInfos.contains(tableId.LocalPathId);
    }

    TUserTable::TCPtr FindUserTable(const TPathId& tableId) {
        auto it = TableInfos.find(tableId.LocalPathId);
        if (it != TableInfos.end()) {
            return it->second;
        }
        return nullptr;
    }

    void RemoveUserTable(const TPathId& tableId, ILocksDb* locksDb) {
        SysLocks.RemoveSchema(tableId, locksDb);
        Pipeline.GetDepTracker().RemoveSchema(tableId);
        TableInfos.erase(tableId.LocalPathId);
    }

    void AddUserTable(const TPathId& tableId, TUserTable::TPtr tableInfo) {
        TableInfos[tableId.LocalPathId] = tableInfo;
        SysLocks.UpdateSchema(tableId, tableInfo->KeyColumnTypes);
        Pipeline.GetDepTracker().UpdateSchema(tableId, *tableInfo);
    }

    bool IsUserTable(const TTableId& tableId) const {
        return (TableInfos.find(tableId.PathId.LocalPathId) != TableInfos.end())
                && !TSysTables::IsSystemTable(tableId);
    }

    const THashMap<ui64, TUserTable::TCPtr> &GetUserTables() const { return TableInfos; }

    ui64 GetLocalTableId(const TTableId& tableId) const {
        Y_ABORT_UNLESS(!TSysTables::IsSystemTable(tableId));
        auto it = TableInfos.find(tableId.PathId.LocalPathId);
        return it == TableInfos.end() ? 0 : it->second->LocalTid;
    }

    ui64 GetShadowTableId(const TTableId& tableId) const {
        Y_ABORT_UNLESS(!TSysTables::IsSystemTable(tableId));
        auto it = TableInfos.find(tableId.PathId.LocalPathId);
        return it == TableInfos.end() ? 0 : it->second->ShadowTid;
    }

    ui64 GetTxReadSizeLimit() const {
        return TxReadSizeLimit ? TxReadSizeLimit : (ui64)PerShardReadSizeLimit;
    }

    ui64 GetDataTxProfileLogThresholdMs() const {
        return DataTxProfileLogThresholdMs;
    }

    ui64 GetDataTxProfileBufferThresholdMs() const {
        return DataTxProfileBufferThresholdMs;
    }

    ui64 GetDataTxProfileBufferSize() const {
        return DataTxProfileBufferSize;
    }

    ui64 GetOutdatedCleanupStep() const {
        ui64 mediatorTime = MediatorTimeCastEntry ? MediatorTimeCastEntry->Get(TabletID()) : 0;
        ui64 pipelineTime = Pipeline.OutdatedCleanupStep();
        return Max(mediatorTime, pipelineTime);
    }

    ui64 GetBackupReadAheadLoOverride() const {
        return BackupReadAheadLo;
    }

    ui64 GetBackupReadAheadHiOverride() const {
        return BackupReadAheadHi;
    }

    ui64 GetTtlReadAheadLoOverride() const {
        return TtlReadAheadLo;
    }

    ui64 GetTtlReadAheadHiOverride() const {
        return TtlReadAheadHi;
    }

    bool GetEnableLockedWrites() const {
        ui64 value = EnableLockedWrites;
        return value != 0;
    }

    ui64 GetMaxLockedWritesPerKey() const {
        ui64 value = MaxLockedWritesPerKey;
        return value;
    }

    bool GetChangeRecordDebugPrint() const {
        ui64 value = ChangeRecordDebugPrint;
        return value != 0;
    }

    template <typename T>
    void ReleaseCache(T& tx) {
        ReleaseTxCache(tx.GetTxCacheUsage());
        tx.SetTxCacheUsage(0);
    }

    ui64 MakeScanSnapshot(ui32 tableId) { return Executor()->MakeScanSnapshot(tableId); }
    ui64 QueueScan(ui32 tableId, TAutoPtr<NTable::IScan> scan, ui64 cookie, const TScanOptions& options = TScanOptions())
    {
        return Executor()->QueueScan(tableId, scan, cookie, options);
    }
    void DropScanSnapshot(ui64 id) { Executor()->DropScanSnapshot(id); }
    void CancelScan(ui32 tableId, ui64 scanId) { Executor()->CancelScan(tableId, scanId); }
    TString BorrowSnapshot(ui32 tableId,
                           const TTableSnapshotContext &ctx,
                           NTable::TRawVals from,
                           NTable::TRawVals to,
                           ui64 loaner) const
    {
        return Executor()->BorrowSnapshot(tableId, ctx, from, to, loaner);
    }

    void SnapshotComplete(TIntrusivePtr<NTabletFlatExecutor::TTableSnapshotContext> snapContext, const TActorContext &ctx) override;
    void CompactionComplete(ui32 tableId, const TActorContext &ctx) override;
    void CompletedLoansChanged(const TActorContext &ctx) override;

    void ReplyCompactionWaiters(
        ui32 tableId,
        TLocalPathId localPathId,
        const NTabletFlatExecutor::TFinishedCompactionInfo& compactionInfo,
        const TActorContext &ctx);

    TUserTable::TSpecialUpdate SpecialUpdates(const NTable::TDatabase& db, const TTableId& tableId) const;

    void SetTableAccessTime(const TTableId& tableId, TInstant ts);
    void SetTableUpdateTime(const TTableId& tableId, TInstant ts);
    void SampleKeyAccess(const TTableId& tableId, const TArrayRef<const TCell>& row);
    NMiniKQL::IKeyAccessSampler::TPtr GetKeyAccessSampler();
    void EnableKeyAccessSampling(const TActorContext &ctx, TInstant until);
    void UpdateTableStats(const TActorContext& ctx);
    void CollectCpuUsage(const TActorContext& ctx);

    void ScanComplete(NTable::EAbort status, TAutoPtr<IDestructable> prod, ui64 cookie, const TActorContext &ctx) override;
    bool ReassignChannelsEnabled() const override;
    void OnYellowChannelsChanged() override;
    void OnRejectProbabilityRelaxed() override;
    void OnFollowersCountChanged() override;
    ui64 GetMemoryUsage() const override;

    bool HasPipeServer(const TActorId& pipeServerId);
    bool AddOverloadSubscriber(const TActorId& pipeServerId, const TActorId& actorId, ui64 seqNo, ERejectReasons reasons);
    void NotifyOverloadSubscribers(ERejectReason reason);
    void NotifyAllOverloadSubscribers();

    template <typename TResponseRecord>
    void SetOverloadSubscribed(const std::optional<ui64>& overloadSubscribe, const TActorId& recipient, const TActorId& sender, const ERejectReasons rejectReasons, TResponseRecord& responseRecord) {
        if (overloadSubscribe && HasPipeServer(recipient)) {
            ui64 seqNo = overloadSubscribe.value();
            auto allowed = (ERejectReasons::OverloadByProbability | ERejectReasons::YellowChannels | ERejectReasons::ChangesQueueOverflow);
            if ((rejectReasons & allowed) != ERejectReasons::None &&
                (rejectReasons - allowed) == ERejectReasons::None)
            {
                if (AddOverloadSubscriber(recipient, sender, seqNo, rejectReasons)) {
                    responseRecord.SetOverloadSubscribed(seqNo);
                }
            }
        }
    }

    bool HasSharedBlobs() const;
    void CheckInitiateBorrowedPartsReturn(const TActorContext& ctx);
    void CheckStateChange(const TActorContext& ctx);
    void CheckSplitCanStart(const TActorContext& ctx);
    void CheckMvccStateChangeCanStart(const TActorContext& ctx);

    ui32 GetState() const { return State; }
    TSwitchState GetMvccSwitchState() { return MvccSwitchState; }
    void SetPersistState(ui32 state, TTransactionContext &txc)
    {
        NIceDb::TNiceDb db(txc.DB);
        PersistSys(db, Schema::Sys_State, state);

        State = state;
    }

    bool IsStopping() const { return Stopping; }

    ui64 GetPathOwnerId() const { return PathOwnerId; }
    ui64 GetCurrentSchemeShardId() const { return CurrentSchemeShardId; }

    TSchemeOpSeqNo GetLastSchemeOpSeqNo() { return LastSchemeOpSeqNo; }
    void UpdateLastSchemeOpSeqNo(const TSchemeOpSeqNo &newSeqNo,
                                 TTransactionContext &txc);
    void ResetLastSchemeOpSeqNo(TTransactionContext &txc);
    void PersistProcessingParams(const NKikimrSubDomains::TProcessingParams &params,
                                 NTabletFlatExecutor::TTransactionContext &txc);
    void PersistCurrentSchemeShardId(ui64 id,
                                    NTabletFlatExecutor::TTransactionContext &txc);
    void PersistSubDomainPathId(ui64 ownerId, ui64 localPathId,
                                NTabletFlatExecutor::TTransactionContext &txc);
    void PersistOwnerPathId(ui64 id,
                            NTabletFlatExecutor::TTransactionContext &txc);

    TDuration CleanupTimeout() const;
    void PlanCleanup(const TActorContext &ctx) {
        CleanupQueue.Schedule(ctx, CleanupTimeout());
    }

    void SendRegistrationRequestTimeCast(const TActorContext &ctx);

    const NKikimrSubDomains::TProcessingParams *GetProcessingParams() const
    {
        return ProcessingParams.get();
    }

    const TSysLocks &GetSysLocks() const { return SysLocks; }

    using TTabletExecutedFlat::TryCaptureTxCache;

    bool IsAnyChannelYellowMove() const {
        return Executor()->GetStats().IsAnyChannelYellowMove;
    }

    bool IsAnyChannelYellowStop() const {
        return Executor()->GetStats().IsAnyChannelYellowStop;
    }

    bool IsSubDomainOutOfSpace() const
    {
        return SubDomainOutOfSpace;
    }

    ui64 GetExecutorStep() const
    {
        return Executor()->Step();
    }

    TSchemaOperation *FindSchemaTx(ui64 txId) { return TransQueue.FindSchemaTx(txId); }

    TUserTable::TPtr AlterTableSchemaVersion(
        const TActorContext& ctx, TTransactionContext& txc,
        const TPathId& pathId, const ui64 tableSchemaVersion,
        bool persist = true);

    TUserTable::TPtr AlterTableAddIndex(
        const TActorContext& ctx, TTransactionContext& txc,
        const TPathId& pathId, ui64 tableSchemaVersion,
        const NKikimrSchemeOp::TIndexDescription& indexDesc);

    TUserTable::TPtr AlterTableSwitchIndexState(
        const TActorContext& ctx, TTransactionContext& txc,
        const TPathId& pathId, ui64 tableSchemaVersion,
        const TPathId& streamPathId, NKikimrSchemeOp::EIndexState state);

    TUserTable::TPtr AlterTableDropIndex(
        const TActorContext& ctx, TTransactionContext& txc,
        const TPathId& pathId, ui64 tableSchemaVersion,
        const TPathId& indexPathId);

    TUserTable::TPtr AlterTableAddCdcStream(
        const TActorContext& ctx, TTransactionContext& txc,
        const TPathId& pathId, ui64 tableSchemaVersion,
        const NKikimrSchemeOp::TCdcStreamDescription& streamDesc);

    TUserTable::TPtr AlterTableSwitchCdcStreamState(
        const TActorContext& ctx, TTransactionContext& txc,
        const TPathId& pathId, ui64 tableSchemaVersion,
        const TPathId& streamPathId, NKikimrSchemeOp::ECdcStreamState state);

    TUserTable::TPtr AlterTableDropCdcStream(
        const TActorContext& ctx, TTransactionContext& txc,
        const TPathId& pathId, ui64 tableSchemaVersion,
        const TPathId& streamPathId);

    TUserTable::TPtr CreateUserTable(TTransactionContext& txc, const NKikimrSchemeOp::TTableDescription& tableScheme);
    TUserTable::TPtr AlterUserTable(const TActorContext& ctx, TTransactionContext& txc,
                                    const NKikimrSchemeOp::TTableDescription& tableScheme);
    static THashMap<TPathId, TPathId> GetRemapIndexes(const NKikimrTxDataShard::TMoveTable& move);
    TUserTable::TPtr MoveUserTable(TOperation::TPtr op, const NKikimrTxDataShard::TMoveTable& move,
        const TActorContext& ctx, TTransactionContext& txc);
    TUserTable::TPtr MoveUserIndex(TOperation::TPtr op, const NKikimrTxDataShard::TMoveIndex& move,
        const TActorContext& ctx, TTransactionContext& txc);
    void DropUserTable(TTransactionContext& txc, ui64 tableId);

    ui32 GetLastLocalTid() const { return LastLocalTid; }
    ui32 GetLastLoanTableTid() const { return LastLoanTableTid; }

    void PersistLastLoanTableTid(NIceDb::TNiceDb& db, ui32 localTid);

    ui64 AllocateChangeRecordOrder(NIceDb::TNiceDb& db, ui64 count = 1);
    ui64 AllocateChangeRecordGroup(NIceDb::TNiceDb& db);
    ui64 GetNextChangeRecordLockOffset(ui64 lockId);
    void PersistChangeRecord(NIceDb::TNiceDb& db, const TChangeRecord& record);
    bool HasLockChangeRecords(ui64 lockId) const;
    void CommitLockChangeRecords(NIceDb::TNiceDb& db, ui64 lockId, ui64 group, const TRowVersion& rowVersion, TVector<IDataShardChangeCollector::TChange>& collected);
    void MoveChangeRecord(NIceDb::TNiceDb& db, ui64 order, const TPathId& pathId);
    void MoveChangeRecord(NIceDb::TNiceDb& db, ui64 lockId, ui64 lockOffset, const TPathId& pathId);
    void RemoveChangeRecord(NIceDb::TNiceDb& db, ui64 order);
    // TODO(ilnaz): remove 'afterMove' after #6541
    void EnqueueChangeRecords(TVector<IDataShardChangeCollector::TChange>&& records, ui64 cookie = 0, bool afterMove = false);
    ui32 GetFreeChangeQueueCapacity(ui64 cookie);
    ui64 ReserveChangeQueueCapacity(ui32 capacity);
    void UpdateChangeExchangeLag(TInstant now);
    void CreateChangeSender(const TActorContext& ctx);
    void KillChangeSender(const TActorContext& ctx);
    void MaybeActivateChangeSender(const TActorContext& ctx);
    void SuspendChangeSender(const TActorContext& ctx);
    const TActorId& GetChangeSender() const { return OutChangeSender; }
    bool LoadChangeRecords(NIceDb::TNiceDb& db, TVector<IDataShardChangeCollector::TChange>& records);
    bool LoadLockChangeRecords(NIceDb::TNiceDb& db);
    bool LoadChangeRecordCommits(NIceDb::TNiceDb& db, TVector<IDataShardChangeCollector::TChange>& records);
    void ScheduleRemoveLockChanges(ui64 lockId);
    void ScheduleRemoveAbandonedLockChanges();
    void ScheduleRemoveSchemaSnapshot(const TSchemaSnapshotKey& key);
    void ScheduleRemoveAbandonedSchemaSnapshots();

    static void PersistCdcStreamScanLastKey(NIceDb::TNiceDb& db, const TSerializedCellVec& value,
        const TPathId& tablePathId, const TPathId& streamPathId);
    static bool LoadCdcStreamScanLastKey(NIceDb::TNiceDb& db, TMaybe<TSerializedCellVec>& result,
        const TPathId& tablePathId, const TPathId& streamPathId);
    static void RemoveCdcStreamScanLastKey(NIceDb::TNiceDb& db, const TPathId& tablePathId, const TPathId& streamPathId);

    static void PersistSchemeTxResult(NIceDb::TNiceDb &db, const TSchemaOperation& op);
    void NotifySchemeshard(const TActorContext& ctx, ui64 txId = 0);

    TThrRefBase* GetDataShardSysTables() { return DataShardSysTables.Get(); }

    TSnapshotManager& GetSnapshotManager() { return SnapshotManager; }
    const TSnapshotManager& GetSnapshotManager() const { return SnapshotManager; }

    TSchemaSnapshotManager& GetSchemaSnapshotManager() { return SchemaSnapshotManager; }
    const TSchemaSnapshotManager& GetSchemaSnapshotManager() const { return SchemaSnapshotManager; }
    void AddSchemaSnapshot(const TPathId& pathId, ui64 tableSchemaVersion, ui64 step, ui64 txId,
        TTransactionContext& txc, const TActorContext& ctx);

    TVolatileTxManager& GetVolatileTxManager() { return VolatileTxManager; }
    const TVolatileTxManager& GetVolatileTxManager() const { return VolatileTxManager; }

    TConflictsCache& GetConflictsCache() { return ConflictsCache; }

    TCdcStreamScanManager& GetCdcStreamScanManager() { return CdcStreamScanManager; }
    const TCdcStreamScanManager& GetCdcStreamScanManager() const { return CdcStreamScanManager; }

    TCdcStreamHeartbeatManager& GetCdcStreamHeartbeatManager() { return CdcStreamHeartbeatManager; }
    const TCdcStreamHeartbeatManager& GetCdcStreamHeartbeatManager() const { return CdcStreamHeartbeatManager; }
    void EmitHeartbeats();

    template <typename... Args>
    bool PromoteCompleteEdge(Args&&... args) {
        return SnapshotManager.PromoteCompleteEdge(std::forward<Args>(args)...);
    }

    TScanManager& GetScanManager() { return ScanManager; }

    // Returns true when datashard is working in mvcc mode
    bool IsMvccEnabled() const;

    // Calculates current follower read edge
    std::tuple<TRowVersion, bool, ui64> CalculateFollowerReadEdge() const;

    // Promotes current follower read edge
    bool PromoteFollowerReadEdge(TTransactionContext& txc);
    bool PromoteFollowerReadEdge();

    // Returns true when this shard has potential followers
    bool HasFollowers() const;

    // Returns a suitable row version for performing a transaction
    TRowVersion GetMvccTxVersion(EMvccTxMode mode, TOperation* op = nullptr) const;

    enum class EPromotePostExecuteEdges {
        ReadOnly,
        RepeatableRead,
        ReadWrite,
    };

    struct TPromotePostExecuteEdges {
        bool HadWrites = false;
        bool WaitCompletion = false;
    };

    TReadWriteVersions GetReadWriteVersions(TOperation* op = nullptr) const;
    TPromotePostExecuteEdges PromoteImmediatePostExecuteEdges(
            const TRowVersion& version, EPromotePostExecuteEdges mode, TTransactionContext& txc);
    ui64 GetMaxObservedStep() const;
    void SendImmediateWriteResult(
            const TRowVersion& version, const TActorId& target, IEventBase* event, ui64 cookie = 0,
            const TActorId& sessionId = {},
            NWilson::TTraceId traceId = {});
    TMonotonic ConfirmReadOnlyLease();
    void ConfirmReadOnlyLease(TMonotonic ts);
    void SendWithConfirmedReadOnlyLease(
        TMonotonic ts,
        const TActorId& target,
        IEventBase* event,
        ui64 cookie = 0,
        const TActorId& sessionId = {},
        NWilson::TTraceId traceId = {});
    void SendWithConfirmedReadOnlyLease(
        const TActorId& target,
        IEventBase* event,
        ui64 cookie = 0,
        const TActorId& sessionId = {},
        NWilson::TTraceId traceId = {});
    void SendImmediateReadResult(
        TMonotonic readTime,
        const TActorId& target,
        IEventBase* event,
        ui64 cookie = 0,
        const TActorId& sessionId = {},
        NWilson::TTraceId traceId = {});
    void SendImmediateReadResult(
        const TActorId& target,
        IEventBase* event,
        ui64 cookie = 0,
        const TActorId& sessionId = {},
        NWilson::TTraceId traceId = {});
    void SendAfterMediatorStepActivate(ui64 mediatorStep, const TActorContext& ctx);

    void CheckMediatorStateRestored();
    void FinishMediatorStateRestore(TTransactionContext&, ui64, ui64);

    void FillExecutionStats(const TExecutionProfile& execProfile, NKikimrQueryStats::TTxStats& txStats) const;

    // Executes TTxProgressTransaction without specific operation
    void ExecuteProgressTx(const TActorContext& ctx);

    // Executes TTxProgressTransaction for the specific operation
    void ExecuteProgressTx(TOperation::TPtr op, const TActorContext& ctx);

    // Executes TTxCleanupTransaction
    void ExecuteCleanupTx(const TActorContext& ctx);
    void ExecuteCleanupVolatileTx(ui64 txId, const TActorContext& ctx);

    void StopFindSubDomainPathId();
    void StartFindSubDomainPathId(bool delayFirstRequest = true);

    void StopWatchingSubDomainPathId();
    void StartWatchingSubDomainPathId();

    bool WaitPlanStep(ui64 step);
    bool CheckTxNeedWait() const;
    bool CheckTxNeedWait(const TRowVersion& mvccSnapshot) const;
    bool CheckTxNeedWait(const TEvDataShard::TEvProposeTransaction::TPtr& ev) const;
    bool CheckTxNeedWait(const NEvents::TDataEvents::TEvWrite::TPtr& ev) const;

    void WaitPredictedPlanStep(ui64 step);
    void SchedulePlanPredictedTxs();

    bool CheckChangesQueueOverflow(ui64 cookie = 0) const;
    void CheckChangesQueueNoOverflow(ui64 cookie = 0);

    void DeleteReadIterator(TReadIteratorsMap::iterator it);
    void CancelReadIterators(Ydb::StatusIds::StatusCode code, const TString& issue, const TActorContext& ctx);
    void ReadIteratorsOnNodeDisconnected(const TActorId& sessionId, const TActorContext &ctx);
    void UnsubscribeReadIteratorSessions(const TActorContext& ctx);

    void SubscribeNewLocks(const TActorContext &ctx);
    void SubscribeNewLocks();

    /**
     * Breaks uncommitted write locks at the specified key
     *
     * Prerequisites: TSetupSysLocks is active and caller does not have any
     * uncommitted write locks.
     * Note: the specified table should have potential conflicting changes,
     * otherwise this call is a very expensive no-op.
     *
     * Returns true on success and false on page fault.
     */
    bool BreakWriteConflicts(NTable::TDatabase& db, const TTableId& tableId,
        TArrayRef<const TCell> keyCells, absl::flat_hash_set<ui64>& volatileDependencies);

    /**
     * Handles a specific write conflict txId
     *
     * Prerequisites: TSetupSysLocks is active and caller does not have any
     * uncommitted write locks.
     *
     * Either adds txId to volatile dependencies or breaks a known write lock.
     */
    void BreakWriteConflict(ui64 txId, absl::flat_hash_set<ui64>& volatileDependencies);

    enum ELogThrottlerType {
        CheckDataTxUnit_Execute = 0,
        CheckWriteUnit_Execute = 0,
        TxProposeTransactionBase_Execute,
        FinishProposeUnit_CompleteRequest,
        FinishProposeUnit_UpdateCounters,
        UploadRows_Reject,
        EraseRows_Reject,

        LAST
    };

    TTrivialLogThrottler& GetLogThrottler(ELogThrottlerType type) {
        Y_ABORT_UNLESS(type != ELogThrottlerType::LAST);
        return LogThrottlers[type];
    };

private:
    ///
    class TLoanReturnTracker {
        struct TLoanReturnInfo {
            TActorId PipeToOwner;
            THashSet<TLogoBlobID> PartMeta;
        };

        ui64 MyTabletID;
        // TabletID -> non-acked loans
        THashMap<ui64, TLoanReturnInfo> LoanReturns;
        // part -> owner
        THashMap<TLogoBlobID, ui64> LoanOwners;
        NTabletPipe::TClientRetryPolicy PipeRetryPolicy;

    public:
        explicit TLoanReturnTracker(ui64 myTabletId)
            : MyTabletID(myTabletId)
            , PipeRetryPolicy{
                .RetryLimitCount = 20,
                .MinRetryTime = TDuration::MilliSeconds(10),
                .MaxRetryTime = TDuration::MilliSeconds(500),
                .BackoffMultiplier = 2}
        {}

        TLoanReturnTracker(const TLoanReturnTracker&) = delete;
        TLoanReturnTracker& operator=(const TLoanReturnTracker&) = delete;

        void Shutdown(const TActorContext& ctx) {
            for (auto& info : LoanReturns) {
                NTabletPipe::CloseClient(ctx, info.second.PipeToOwner);
            }
            LoanReturns.clear();
        }

        void ReturnLoan(ui64 ownerTabletId, const TVector<TLogoBlobID>& partMetaVec, const TActorContext& ctx) {
            TLoanReturnInfo& info = LoanReturns[ownerTabletId];

            TVector<TLogoBlobID> partsToReturn(Reserve(partMetaVec.size()));
            for (const auto& partMeta : partMetaVec) {
                auto it = LoanOwners.find(partMeta);
                if (it != LoanOwners.end()) {
                    Y_ABORT_UNLESS(it->second == ownerTabletId,
                        "Part is already registered with a different owner");
                } else {
                    LoanOwners[partMeta] = ownerTabletId;
                }
                if (info.PartMeta.insert(partMeta).second) {
                    partsToReturn.emplace_back(partMeta);
                }
            }

            if (partsToReturn.empty()) {
                return;
            }

            if (!info.PipeToOwner) {
                NTabletPipe::TClientConfig clientConfig;
                clientConfig.CheckAliveness = true;
                clientConfig.RetryPolicy = PipeRetryPolicy;
                info.PipeToOwner = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, ownerTabletId, clientConfig));
            }

            THolder<TEvDataShard::TEvReturnBorrowedPart> ev = MakeHolder<TEvDataShard::TEvReturnBorrowedPart>(MyTabletID, partMetaVec);
            NTabletPipe::SendData(ctx, info.PipeToOwner, ev.Release());
        }

        void ResendLoans(ui64 ownerTabletId, const TActorContext& ctx) {
            if (!LoanReturns.contains(ownerTabletId))
                return;

            THashSet<TLogoBlobID> toResend;
            toResend.swap(LoanReturns[ownerTabletId].PartMeta);

            LoanReturns.erase(ownerTabletId);

            ReturnLoan(ownerTabletId, {toResend.begin(), toResend.end()}, ctx);
        }

        void AutoAckLoans(ui64 deadTabletId, const TActorContext& ctx) {
            if (!LoanReturns.contains(deadTabletId))
                return;

            TVector<TLogoBlobID> partMetaVec(LoanReturns[deadTabletId].PartMeta.begin(), LoanReturns[deadTabletId].PartMeta.end());

            ctx.Send(ctx.SelfID, new TEvDataShard::TEvReturnBorrowedPartAck(partMetaVec));
        }

        void LoanDone(TLogoBlobID partMeta, const TActorContext& ctx) {
            if (!LoanOwners.contains(partMeta))
                return;

            ui64 ownerTabletId = LoanOwners[partMeta];
            LoanOwners.erase(partMeta);
            LoanReturns[ownerTabletId].PartMeta.erase(partMeta);

            if (LoanReturns[ownerTabletId].PartMeta.empty()) {
                NTabletPipe::CloseClient(ctx, LoanReturns[ownerTabletId].PipeToOwner);
                LoanReturns.erase(ownerTabletId);
            }
        }

        bool Has(ui64 ownerTabletId, TActorId pipeClientActorId) const {
            return LoanReturns.contains(ownerTabletId) && LoanReturns.FindPtr(ownerTabletId)->PipeToOwner == pipeClientActorId;
        }

        bool Empty() const {
            return LoanReturns.empty();
        }
    };

    ///
    class TSplitSrcSnapshotSender {
    public:
        TSplitSrcSnapshotSender(TDataShard* self)
            : Self(self)
        { }

        void AddDst(ui64 dstTabeltId) {
            Dst.insert(dstTabeltId);
        }

        const THashSet<ui64>& GetDstSet() const {
            return Dst;
        }

        void SaveSnapshotForSending(ui64 dstTabletId, TAutoPtr<NKikimrTxDataShard::TEvSplitTransferSnapshot> snapshot) {
            Y_ABORT_UNLESS(Dst.contains(dstTabletId));
            DataToSend[dstTabletId] = snapshot;
        }

        void DoSend(const TActorContext &ctx) {
            Y_ABORT_UNLESS(Dst.size() == DataToSend.size());
            for (const auto& ds : DataToSend) {
                ui64 dstTablet = ds.first;
                DoSend(dstTablet, ctx);
            }
        }

        void DoSend(ui64 dstTabletId, const TActorContext &ctx) {
            Y_ABORT_UNLESS(Dst.contains(dstTabletId));
            NTabletPipe::TClientConfig clientConfig;
            PipesToDstShards[dstTabletId] = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, dstTabletId, clientConfig));

            THolder<TEvDataShard::TEvSplitTransferSnapshot> ev = MakeHolder<TEvDataShard::TEvSplitTransferSnapshot>(0);
            ev->Record.CopyFrom(*DataToSend[dstTabletId]);
            ev->Record.SetSrcTabletGeneration(Self->Generation());

            auto fnCalcTotalSize = [] (const TEvDataShard::TEvSplitTransferSnapshot& ev) {
                ui64 size = 0;
                for (ui32 i = 0; i < ev.Record.TableSnapshotSize(); ++i) {
                    size += ev.Record.GetTableSnapshot(i).GetSnapshotData().size();
                }
                return size;
            };

            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                            "Sending snapshot for split opId " << ev->Record.GetOperationCookie()
                            << " from datashard " << ev->Record.GetSrcTabletId()
                            << " to datashard " << dstTabletId << " size " << fnCalcTotalSize(*ev));

            NTabletPipe::SendData(ctx, PipesToDstShards[dstTabletId], ev.Release());
        }

        void AckSnapshot(ui64 dstTabletId, const TActorContext &ctx) {
            if (!DataToSend.contains(dstTabletId))
                return;

            NTabletPipe::CloseClient(ctx, PipesToDstShards[dstTabletId]);
            PipesToDstShards.erase(dstTabletId);
            DataToSend.erase(dstTabletId);
        }

        bool AllAcked() const {
            return DataToSend.empty();
        }

        bool Acked(ui64 dstTabletId) const {
            return !DataToSend.contains(dstTabletId);
        }

        bool Has(ui64 dstTabletId, TActorId pipeClientActorId) const {
            return PipesToDstShards.contains(dstTabletId) && *PipesToDstShards.FindPtr(dstTabletId) == pipeClientActorId;
        }

        void Shutdown(const TActorContext &ctx) {
            for (const auto& p : PipesToDstShards) {
                NTabletPipe::CloseClient(ctx, p.second);
            }
        }

    private:
        TDataShard* Self;
        THashSet<ui64> Dst;
        THashMap<ui64, TAutoPtr<NKikimrTxDataShard::TEvSplitTransferSnapshot>> DataToSend;
        THashMap<ui64, TActorId> PipesToDstShards;
    };

    ///
    class TChangeSenderActivator {
    public:
        explicit TChangeSenderActivator(ui64 selfTabletId)
            : Origin(selfTabletId)
            , PipeRetryPolicy{
                .RetryLimitCount = 20,
                .MinRetryTime = TDuration::MilliSeconds(10),
                .MaxRetryTime = TDuration::MilliSeconds(500),
                .BackoffMultiplier = 2
            }
        {
        }

        void AddDst(ui64 dstTabletId) {
            Dst.insert(dstTabletId);
        }

        const THashSet<ui64>& GetDstSet() const {
            return Dst;
        }

        void DoSend(ui64 dstTabletId, const TActorContext& ctx) {
            Y_ABORT_UNLESS(Dst.contains(dstTabletId));
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.CheckAliveness = true;
            clientConfig.RetryPolicy = PipeRetryPolicy;
            PipesToDstShards[dstTabletId] = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, dstTabletId, clientConfig));

            auto ev = MakeHolder<TEvChangeExchange::TEvActivateSender>();
            ev->Record.SetOrigin(Origin);

            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Activate change sender"
                << ": origin# " << ev->Record.GetOrigin()
                << ", dst# " << dstTabletId);

            NTabletPipe::SendData(ctx, PipesToDstShards[dstTabletId], ev.Release());
        }

        bool Ack(ui64 dstTabletId, const TActorContext &ctx) {
            if (!Dst.contains(dstTabletId)) {
                return false;
            }

            NTabletPipe::CloseClient(ctx, PipesToDstShards[dstTabletId]);
            PipesToDstShards.erase(dstTabletId);
            Dst.erase(dstTabletId);

            return true;
        }

        void AutoAck(ui64 dstTabletId, const TActorContext &ctx) {
            if (!Ack(dstTabletId, ctx)) {
                return;
            }

            auto ev = MakeHolder<TEvChangeExchange::TEvActivateSenderAck>();
            ev->Record.SetOrigin(dstTabletId);
            ctx.Send(ctx.SelfID, ev.Release());
        }

        bool AllAcked() const {
            return Dst.empty();
        }

        bool Acked(ui64 dstTabletId) const {
            return !Dst.contains(dstTabletId);
        }

        bool Has(ui64 dstTabletId, TActorId pipeClientActorId) const {
            auto it = PipesToDstShards.find(dstTabletId);
            if (it == PipesToDstShards.end()) {
                return false;
            }

            return it->second == pipeClientActorId;
        }

        void Shutdown(const TActorContext &ctx) {
            for (const auto& p : PipesToDstShards) {
                NTabletPipe::CloseClient(ctx, p.second);
            }
        }

        TString Dump() const {
            return JoinSeq(", ", Dst);
        }

    private:
        const ui64 Origin;
        NTabletPipe::TClientRetryPolicy PipeRetryPolicy;

        THashSet<ui64> Dst;
        THashMap<ui64, TActorId> PipesToDstShards;
    };

    class TChangeExchangeSplitter {
    public:
        explicit TChangeExchangeSplitter(const TDataShard* self)
            : Self(self)
        {
        }

        void AddDst(ui64 dstTabletId) {
            DstTabletIds.insert(dstTabletId);
        }

        void DoSplit(const TActorContext& ctx) {
            Y_ABORT_UNLESS(DstTabletIds);
            Worker = ctx.Register(CreateChangeExchangeSplit(Self, TVector<ui64>(DstTabletIds.begin(), DstTabletIds.end())));
            Acked = false;
        }

        void Ack() {
            Acked = true;
        }

        bool Done() const {
            return !DstTabletIds || Acked;
        }

        void Shutdown(const TActorContext& ctx) {
            if (Worker) {
                ctx.Send(std::exchange(Worker, TActorId()), new TEvents::TEvPoisonPill());
            }
        }

    private:
        const TDataShard* Self;

        THashSet<ui64> DstTabletIds;
        TActorId Worker;
        bool Acked = false;
    };

    // For follower only
    struct TFollowerState {
        NTable::TDatabase::TChangeCounter LastSysUpdate;
        NTable::TDatabase::TChangeCounter LastSchemeUpdate;
        NTable::TDatabase::TChangeCounter LastSnapshotsUpdate;
    };

    //

    TTabletCountersBase* TabletCounters;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;
    TAlignedPagePoolCounters AllocCounters;

    TTxProgressIdempotentScalarQueue<TEvPrivate::TEvProgressTransaction> PlanQueue;
    TTxProgressIdempotentScalarScheduleQueue<TEvPrivate::TEvCleanupTransaction> CleanupQueue;
    TTxProgressQueue<ui64, TNoOpDestroy, TEvPrivate::TEvProgressResendReadSet> ResendReadSetQueue;

    struct TPipeServerInfoOverloadSubscribersTag {};

    struct TOverloadSubscriber {
        ui64 SeqNo = 0;
        ERejectReasons Reasons = ERejectReasons::None;
    };

    struct TPipeServerInfo
        : public TIntrusiveListItem<TPipeServerInfo, TPipeServerInfoOverloadSubscribersTag>
    {
        TPipeServerInfo() = default;

        TActorId InterconnectSession;
        THashMap<TActorId, TOverloadSubscriber> OverloadSubscribers;
    };

    using TPipeServers = THashMap<TActorId, TPipeServerInfo>;
    using TPipeServersWithOverloadSubscribers = TIntrusiveList<TPipeServerInfo, TPipeServerInfoOverloadSubscribersTag>;

    TPipeServers PipeServers;
    TPipeServersWithOverloadSubscribers PipeServersWithOverloadSubscribers;
    size_t OverloadSubscribersByReason[RejectReasonCount] = { 0 };

    void DiscardOverloadSubscribers(TPipeServerInfo& pipeServer);

    class TProposeQueue : private TTxProgressIdempotentScalarQueue<TEvPrivate::TEvDelayedProposeTransaction> {
    public:
        struct TItem : public TMoveOnly {
            TItem(TAutoPtr<IEventHandle>&& event, TInstant receivedAt, ui64 tieBreakerIndex)
                : Event(std::move(event))
                , ReceivedAt(receivedAt)
                , TieBreakerIndex(tieBreakerIndex)
                , Next(nullptr)
                , Cancelled(false)
            { }

            TAutoPtr<IEventHandle> Event;
            TInstant ReceivedAt;
            ui64 TieBreakerIndex;
            TItem* Next;
            bool Cancelled;
        };

        struct TItemList {
            TItem* First = nullptr;
            TItem* Last = nullptr;
        };

        void Enqueue(TAutoPtr<IEventHandle> event, TInstant receivedAt, ui64 tieBreakerIndex, const TActorContext& ctx) {
            TItem* item = &Items.emplace_back(std::move(event), receivedAt, tieBreakerIndex);

            const ui64 txId = NEvWrite::TConvertor::GetTxId(item->Event);

            auto& links = TxIds[txId];
            if (Y_UNLIKELY(links.Last)) {
                links.Last->Next = item;
            } else {
                links.First = item;
            }
            links.Last = item;

            Progress(ctx);
        }

        TItem Dequeue() {
            TItem* first = &Items.front();
            const ui64 txId = NEvWrite::TConvertor::GetTxId(first->Event);

            auto it = TxIds.find(txId);
            Y_ABORT_UNLESS(it != TxIds.end() && it->second.First == first,
                "Consistency check: proposed txId %" PRIu64 " in deque, but not in hashmap", txId);

            // N.B. there should almost always be exactly one propose per txId
            it->second.First = first->Next;
            if (Y_LIKELY(it->second.First == nullptr)) {
                TxIds.erase(it);
            } else {
                first->Next = nullptr;
            }

            TItem item = std::move(*first);
            Items.pop_front();
            return item;
        }

        void Cancel(ui64 txId) {
            auto it = TxIds.find(txId);
            if (it != TxIds.end()) {
                auto* item = it->second.First;
                while (item) {
                    item->Cancelled = true;
                    item = item->Next;
                }
            }
        }

        void Ack(const TActorContext& ctx) {
            Reset(ctx);
            if (Items) {
                Progress(ctx);
            }
        }

        explicit operator bool() const {
            return bool(Items);
        }

        size_t Size() const {
            return Items.size();
        }

    private:
        TDeque<TItem> Items;
        THashMap<ui64, TItemList> TxIds;
    };

    TProposeQueue ProposeQueue;
    TVector<THolder<IEventHandle>> DelayedProposeQueue;

    TActorId PersistentPipeCache;
    NTabletPipe::TClientRetryPolicy SchemeShardPipeRetryPolicy;
    TActorId SchemeShardPipe;   // For notifications about schema changes
    TActorId StateReportPipe;   // For notifications about shard state changes
    ui64 PathOwnerId; // TabletID of the schmemeshard that allocated the TPathId(ownerId,localId)
    ui64 CurrentSchemeShardId; // TabletID of SchemeShard wich manages the path right now
    ui64 LastKnownMediator;
    bool RegistrationSended;
    bool PeriodicWakeupPending = false;
    std::unique_ptr<NKikimrSubDomains::TProcessingParams> ProcessingParams;
    TSchemeOpSeqNo LastSchemeOpSeqNo;
    TInstant LastDbStatsUpdateTime;
    TInstant LastDbStatsReportTime;
    TInstant LastCpuWarnTime;
    TInstant LastDataSizeWarnTime;
    TActorId DbStatsReportPipe;
    TActorId TableResolvePipe;
    ui64 StatsReportRound = 0;

    TActorId FindSubDomainPathIdActor;

    std::optional<TPathId> SubDomainPathId;
    std::optional<TPathId> WatchingSubDomainPathId;
    bool SubDomainOutOfSpace = false;

    THashSet<TActorId> Actors;
    TLoanReturnTracker LoanReturnTracker;
    TFollowerState FollowerState;

    TSwitchState MvccSwitchState;
    bool SplitSnapshotStarted;      // Non-persistent flag that is used to restart snapshot in case of datashard restart
    TSplitSrcSnapshotSender SplitSrcSnapshotSender;
    // TODO: make this persitent
    THashSet<ui64> ReceiveSnapshotsFrom;
    ui64 DstSplitOpId;
    ui64 SrcSplitOpId;
    bool DstSplitSchemaInitialized = false;
    std::shared_ptr<NKikimrTxDataShard::TSplitMergeDescription> DstSplitDescription;
    std::shared_ptr<NKikimrTxDataShard::TSplitMergeDescription> SrcSplitDescription;
    THashSet<TActorId> SrcAckSplitTo;
    THashMap<TActorId, THashSet<ui64>> SrcAckPartitioningChangedTo;
    const ui32 SysTablesToTransferAtSplit[4] = {
            Schema::TxMain::TableId,
            Schema::TxDetails::TableId,
            // Schema::InReadSets::TableId, // need to fix InReadSets cleanup
            Schema::PlanQueue::TableId,
            Schema::DeadlineQueue::TableId
        };
    THashSet<ui64> SysTablesPartOwners;

    // Sys table contents
    ui32 State;
    ui32 LastLocalTid;
    ui32 LastLoanTableTid;
    ui64 NextSeqno;
    ui64 NextChangeRecordOrder;
    ui64 LastChangeRecordGroup;
    ui64 TxReadSizeLimit;
    ui64 StatisticsDisabled;
    bool Stopping = false;

    NMiniKQL::IKeyAccessSampler::TPtr DisabledKeySampler;
    NMiniKQL::IKeyAccessSampler::TPtr EnabledKeySampler;
    NMiniKQL::IKeyAccessSampler::TPtr CurrentKeySampler; // Points to enabled or disabled
    TInstant StartedKeyAccessSamplingAt;
    TInstant StopKeyAccessSamplingAt;

    TUserTable::TTableInfos TableInfos;  // tableId -> local table info
    TTransQueue TransQueue;
    TOutReadSets OutReadSets;
    TPipeline Pipeline;
    TSysLocks SysLocks;

    TSnapshotManager SnapshotManager;
    TSchemaSnapshotManager SchemaSnapshotManager;
    TVolatileTxManager VolatileTxManager;
    TConflictsCache ConflictsCache;
    TCdcStreamScanManager CdcStreamScanManager;
    TCdcStreamHeartbeatManager CdcStreamHeartbeatManager;

    TReplicationSourceOffsetsServerLink ReplicationSourceOffsetsServer;

    TScanManager ScanManager;

    TS3UploadsManager S3Uploads;
    TS3DownloadsManager S3Downloads;

    struct TMediatorDelayedReply {
        TActorId Target;
        THolder<IEventBase> Event;
        ui64 Cookie;
        TActorId SessionId;
        NWilson::TSpan Span;

        TMediatorDelayedReply(const TActorId& target, THolder<IEventBase> event, ui64 cookie,
                const TActorId& sessionId, NWilson::TSpan&& span)
            : Target(target)
            , Event(std::move(event))
            , Cookie(cookie)
            , SessionId(sessionId)
            , Span(std::move(span))
        { }
    };

    TIntrusivePtr<TMediatorTimecastEntry> MediatorTimeCastEntry;
    TSet<ui64> MediatorTimeCastWaitingSteps;
    TMultiMap<TRowVersion, TMediatorDelayedReply> MediatorDelayedReplies;

    struct TCoordinatorSubscription {
        ui64 CoordinatorId;
        TMediatorTimecastReadStep::TCPtr ReadStep;
    };

    TVector<TCoordinatorSubscription> CoordinatorSubscriptions;
    THashMap<ui64, size_t> CoordinatorSubscriptionById;
    size_t CoordinatorSubscriptionsPending = 0;
    ui64 CoordinatorPrevReadStepMin = 0;
    ui64 CoordinatorPrevReadStepMax = Max<ui64>();

    TVector<THolder<IEventHandle>> MediatorStateWaitingMsgs;
    bool MediatorStateWaiting = false;
    bool MediatorStateRestoreTxPending = false;

    bool IcbRegistered = false;

    TControlWrapper DisableByKeyFilter;
    TControlWrapper MaxTxInFly;
    TControlWrapper MaxTxLagMilliseconds;
    TControlWrapper CanCancelROWithReadSets;
    TControlWrapper PerShardReadSizeLimit;
    TControlWrapper CpuUsageReportThreshlodPercent;
    TControlWrapper CpuUsageReportIntervalSeconds;
    TControlWrapper HighDataSizeReportThreshlodBytes;
    TControlWrapper HighDataSizeReportIntervalSeconds;

    TControlWrapper DataTxProfileLogThresholdMs;
    TControlWrapper DataTxProfileBufferThresholdMs;
    TControlWrapper DataTxProfileBufferSize;

    TControlWrapper BackupReadAheadLo;
    TControlWrapper BackupReadAheadHi;

    TControlWrapper TtlReadAheadLo;
    TControlWrapper TtlReadAheadHi;

    TControlWrapper EnableLockedWrites;
    TControlWrapper MaxLockedWritesPerKey;

    TControlWrapper EnableLeaderLeases;
    TControlWrapper MinLeaderLeaseDurationUs;

    TControlWrapper ChangeRecordDebugPrint;

    // Set of InRS keys to remove from local DB.
    THashSet<TReadSetKey> InRSToRemove;
    TIntrusivePtr<TThrRefBase> DataShardSysTables;

    // Simple volatile counter
    ui64 NextTieBreakerIndex = 1;

    struct TPersistentTablet {
        // Outgoing readsets currently inflight (SeqNo)
        absl::flat_hash_set<ui64> OutReadSets;
        // When true the pipe is currently subscribed
        bool Subscribed = false;

        bool IsEmpty() const;
    };

    THashMap<ui64, TPersistentTablet> PersistentTablets;

    struct TInFlightCondErase {
        ui64 TxId;
        ui64 ScanId;
        TActorId ActorId;
        ui32 Condition;

        TInFlightCondErase() {
            Clear();
        }

        void Clear() {
            TxId = 0;
            ScanId = 0;
            ActorId = TActorId();
            Condition = 0;
        }

        void Enqueue(ui64 txId, ui64 scanId, ui32 condition) {
            TxId = txId;
            ScanId = scanId;
            Condition = condition;
        }

        explicit operator bool() const {
            return bool(ScanId);
        }

        bool IsActive() const {
            return bool(ActorId);
        }
    };

    TInFlightCondErase InFlightCondErase;

    /// change sending & receiving
    struct TInChangeSender {
        ui64 Generation;
        ui64 LastRecordOrder;

        explicit TInChangeSender(ui64 generation, ui64 lastRecordOrder = 0)
            : Generation(generation)
            , LastRecordOrder(lastRecordOrder)
        {
        }
    };

    struct TEnqueuedRecordTag {};
    struct TEnqueuedRecord: public TIntrusiveListItem<TEnqueuedRecord, TEnqueuedRecordTag> {
        ui64 BodySize;
        TPathId TableId;
        ui64 SchemaVersion;
        bool SchemaSnapshotAcquired;
        TInstant CreatedAt;
        TInstant EnqueuedAt;
        ui64 LockId;
        ui64 LockOffset;
        ui64 ReservationCookie;

        explicit TEnqueuedRecord(ui64 bodySize, const TPathId& tableId, ui64 schemaVersion,
                TInstant created, ui64 lockId = 0, ui64 lockOffset = 0)
            : BodySize(bodySize)
            , TableId(tableId)
            , SchemaVersion(schemaVersion)
            , SchemaSnapshotAcquired(false)
            , CreatedAt(created)
            , EnqueuedAt(TInstant::Zero())
            , LockId(lockId)
            , LockOffset(lockOffset)
            , ReservationCookie(0)
        {
        }

        explicit TEnqueuedRecord(const IDataShardChangeCollector::TChange& record)
            : TEnqueuedRecord(record.BodySize, record.TableId, record.SchemaVersion,
                record.CreatedAt(), record.LockId, record.LockOffset)
        {
        }

        explicit TEnqueuedRecord(const TChangeRecord& record)
            : TEnqueuedRecord(record.GetBody().size(), record.GetTableId(), record.GetSchemaVersion(),
                record.GetApproximateCreationDateTime(), record.GetLockId(), record.GetLockOffset())
        {
        }
    };

    using TRequestedRecord = NChangeExchange::TEvChangeExchange::TEvRequestRecords::TRecordInfo;

    // split/merge
    TChangeSenderActivator ChangeSenderActivator;
    TChangeExchangeSplitter ChangeExchangeSplitter;
    THashSet<ui64> ReceiveActivationsFrom;

    // out
    THashMap<TActorId, TSet<TRequestedRecord>> ChangeRecordsRequested;
    TSet<ui64> ChangeRecordsToRemove; // ui64 is order
    bool RequestChangeRecordsInFly = false;
    bool RemoveChangeRecordsInFly = false;
    THashMap<ui64, TEnqueuedRecord> ChangesQueue; // ui64 is order
    TIntrusiveList<TEnqueuedRecord, TEnqueuedRecordTag> ChangesList;
    ui64 ChangesQueueBytes = 0;
    THashMap<ui64, ui32> ChangeQueueReservations;
    ui64 NextChangeQueueReservationCookie = 1;
    ui32 ChangeQueueReservedCapacity = 0;
    TActorId OutChangeSender;
    bool OutChangeSenderSuspended = false;
    THolder<IChangeRecordSerializer> ChangeRecordDebugSerializer;

    struct TUncommittedLockChangeRecords {
        TVector<IDataShardChangeCollector::TChange> Changes;
        size_t PersistentCount = 0;
    };

    struct TCommittedLockChangeRecords {
        ui64 Order = Max<ui64>();
        ui64 Group;
        ui64 Step;
        ui64 TxId;

        // The number of records that are not deleted yet
        size_t Count = 0;
    };

    TVector<ui64> CommittingChangeRecords;
    THashMap<ui64, TUncommittedLockChangeRecords> LockChangeRecords; // ui64 is lock id
    THashMap<ui64, TCommittedLockChangeRecords> CommittedLockChangeRecords; // ui64 is lock id
    TVector<ui64> PendingLockChangeRecordsToRemove;
    TVector<TSchemaSnapshotKey> PendingSchemaSnapshotsToGc;

    // in
    THashMap<ui64, TInChangeSender> InChangeSenders; // ui64 is shard id
    TList<std::pair<TActorId, NKikimrChangeExchange::TEvHandshake>> PendingChangeExchangeHandshakes;
    bool ChangeExchangeHandshakesCollecting = false;
    bool ChangeExchangeHandshakeTxScheduled = false;

    void StartCollectingChangeExchangeHandshakes(const TActorContext& ctx);
    void RunChangeExchangeHandshakeTx();
    void ChangeExchangeHandshakeExecuted();

    // compactionId, actorId
    using TCompactionWaiter = std::tuple<ui64, TActorId>;
    using TCompactionWaiterList = TList<TCompactionWaiter>;

    // tableLocalTid -> waiters, note that compactionId is monotonically
    // increasing and compactions for same table finish in order:
    // thus we always add waiters to the end of the list and remove
    // from the front
    THashMap<ui32, TCompactionWaiterList> CompactionWaiters;

    struct TCompactBorrowedWaiter : public TThrRefBase {
        TCompactBorrowedWaiter(TActorId actorId, TLocalPathId requestedTable)
            : ActorId(actorId)
            , RequestedTable(requestedTable)
        { }

        TActorId ActorId;
        TLocalPathId RequestedTable;
        THashSet<ui32> CompactingTables;
    };

    // tableLocalTid -> waiters, similar to CompactionWaiters
    THashMap<ui32, TList<TIntrusivePtr<TCompactBorrowedWaiter>>> CompactBorrowedWaiters;

    struct TReplicationSourceOffsetsReceiveState {
        // A set of tables for which we already received offsets
        THashSet<TPathId> Received;
        // A set of tables for which we are waiting source offsets data
        THashSet<TPathId> Pending;
        // The latest pending transfer snapshot event
        TEvDataShard::TEvSplitTransferSnapshot::TPtr Snapshot;
    };

    THashMap<ui64, TReplicationSourceOffsetsReceiveState> ReceiveReplicationSourceOffsetsFrom;
    THashMap<ui64, TSerializedTableRange> SrcTabletToRange;

    friend class TReplicationSourceOffsetsDb;
    friend class TReplicationSourceState;
    friend class TReplicatedTableState;
    THashMap<TPathId, TReplicatedTableState> ReplicatedTables;
    TReplicatedTableState* FindReplicatedTable(const TPathId& pathId);
    TReplicatedTableState* EnsureReplicatedTable(const TPathId& pathId);

    TReadIteratorsMap ReadIterators;
    THashMap<TActorId, TReadIteratorSession> ReadIteratorSessions;

    NTable::ITransactionObserverPtr BreakWriteConflictsTxObserver;

    bool UpdateFollowerReadEdgePending = false;

    bool ScheduledPlanPredictedTxs = false;

    TVector<THolder<IEventHandle>> DelayedS3UploadRows;

    std::vector<TTrivialLogThrottler> LogThrottlers = {ELogThrottlerType::LAST, TDuration::Seconds(1)};

    ui32 StatisticsScanTableId = 0;
    ui64 StatisticsScanId = 0;

public:
    auto& GetLockChangeRecords() {
        return LockChangeRecords;
    }

    auto TakeLockChangeRecords() {
        auto result = std::move(LockChangeRecords);
        LockChangeRecords.clear();
        return result;
    }

    void SetLockChangeRecords(THashMap<ui64, TUncommittedLockChangeRecords>&& lockChangeRecords) {
        LockChangeRecords = std::move(lockChangeRecords);
    }

    auto& GetCommittedLockChangeRecords() {
        return CommittedLockChangeRecords;
    }

    auto TakeCommittedLockChangeRecords() {
        auto result = std::move(CommittedLockChangeRecords);
        CommittedLockChangeRecords.clear();
        return result;
    }

    void SetCommittedLockChangeRecords(THashMap<ui64, TCommittedLockChangeRecords>&& committedLockChangeRecords) {
        CommittedLockChangeRecords = std::move(committedLockChangeRecords);
    }

    auto TakeChangesQueue() {
        auto result = std::move(ChangesQueue);
        ChangesQueue.clear();
        return result;
    }

    void SetChangesQueue(THashMap<ui64, TEnqueuedRecord>&& changesQueue) {
        ChangesQueue = std::move(changesQueue);
    }

protected:
    // Redundant init state required by flat executor implementation
    void StateInit(TAutoPtr<NActors::IEventHandle> &ev) {
        TRACE_EVENT(NKikimrServices::TX_DATASHARD);
        StateInitImpl(ev, SelfId());
    }

    void Enqueue(STFUNC_SIG) override {
        ALOG_WARN(NKikimrServices::TX_DATASHARD, "TDataShard::StateInit unhandled event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
    }

    // In this state we are not handling external pipes to datashard tablet (it's just another init phase)
    void StateInactive(TAutoPtr<NActors::IEventHandle> &ev) {
        TRACE_EVENT(NKikimrServices::TX_DATASHARD);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvMediatorTimecast::TEvRegisterTabletResult, Handle);
            HFuncTraced(TEvMediatorTimecast::TEvSubscribeReadStepResult, Handle);
            HFuncTraced(TEvMediatorTimecast::TEvNotifyPlanStep, Handle);
            HFuncTraced(TEvPrivate::TEvMediatorRestoreBackup, Handle);
            HFuncTraced(TEvPrivate::TEvRemoveLockChangeRecords, Handle);
            HFuncTraced(TEvPrivate::TEvRemoveSchemaSnapshots, Handle);
        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                ALOG_WARN(NKikimrServices::TX_DATASHARD, "TDataShard::StateInactive unhandled event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
            }
            break;
        }
    }

    // This is the main state
    void StateWork(TAutoPtr<NActors::IEventHandle> &ev) {
        TRACE_EVENT(NKikimrServices::TX_DATASHARD);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvGone, Handle);
            HFuncTraced(TEvDataShard::TEvGetShardState, Handle);
            HFuncTraced(TEvDataShard::TEvSchemaChangedResult, Handle);
            HFuncTraced(TEvDataShard::TEvStateChangedResult, Handle);
            HFuncTraced(TEvDataShard::TEvProposeTransaction, Handle);
            HFuncTraced(TEvDataShard::TEvProposeTransactionAttach, Handle);
            HFuncTraced(TEvDataShard::TEvCancelBackup, Handle);
            HFuncTraced(TEvDataShard::TEvCancelRestore, Handle);
            HFuncTraced(TEvDataShard::TEvGetS3Upload, Handle);
            HFuncTraced(TEvDataShard::TEvStoreS3UploadId, Handle);
            HFuncTraced(TEvDataShard::TEvChangeS3UploadStatus, Handle);
            HFuncTraced(TEvDataShard::TEvGetS3DownloadInfo, Handle);
            HFuncTraced(TEvDataShard::TEvStoreS3DownloadInfo, Handle);
            HFuncTraced(TEvDataShard::TEvS3UploadRowsRequest, Handle);
            HFuncTraced(TEvDataShard::TEvObjectStorageListingRequest, Handle);
            HFuncTraced(TEvDataShard::TEvMigrateSchemeShardRequest, Handle);
            HFuncTraced(TEvTxProcessing::TEvPlanStep, Handle);
            HFuncTraced(TEvTxProcessing::TEvReadSet, Handle);
            HFuncTraced(TEvTxProcessing::TEvReadSetAck, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamClearanceResponse, Handle);
            HFuncTraced(TEvTxProcessing::TEvStreamClearancePending, Handle);
            HFuncTraced(TEvTxProcessing::TEvInterruptTransaction, Handle);
            HFuncTraced(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
            HFuncTraced(TEvPrivate::TEvProgressTransaction, Handle);
            HFuncTraced(TEvPrivate::TEvCleanupTransaction, Handle);
            HFuncTraced(TEvPrivate::TEvDelayedProposeTransaction, Handle);
            HFuncTraced(TEvPrivate::TEvProgressResendReadSet, Handle);
            HFuncTraced(TEvPrivate::TEvRemoveOldInReadSets, Handle);
            HFuncTraced(TEvPrivate::TEvRegisterScanActor, Handle);
            HFuncTraced(TEvPrivate::TEvScanStats, Handle);
            HFuncTraced(TEvPrivate::TEvPersistScanState, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFuncTraced(TEvPipeCache::TEvDeliveryProblem, Handle);
            HFuncTraced(TEvTabletPipe::TEvServerConnected, Handle);
            HFuncTraced(TEvTabletPipe::TEvServerDisconnected, Handle);
            HFuncTraced(TEvMediatorTimecast::TEvRegisterTabletResult, Handle);
            HFuncTraced(TEvMediatorTimecast::TEvSubscribeReadStepResult, Handle);
            HFuncTraced(TEvMediatorTimecast::TEvNotifyPlanStep, Handle);
            HFuncTraced(TEvPrivate::TEvMediatorRestoreBackup, Handle);
            HFuncTraced(TEvDataShard::TEvCancelTransactionProposal, Handle);
            HFuncTraced(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(TEvDataShard::TEvReturnBorrowedPart, Handle);
            HFunc(TEvDataShard::TEvReturnBorrowedPartAck, Handle);
            HFunc(TEvDataShard::TEvInitSplitMergeDestination, Handle);
            HFunc(TEvDataShard::TEvSplit, Handle);
            HFunc(TEvDataShard::TEvSplitTransferSnapshot, Handle);
            HFunc(TEvPrivate::TEvReplicationSourceOffsets, Handle);
            HFunc(TEvDataShard::TEvSplitTransferSnapshotAck, Handle);
            HFunc(TEvDataShard::TEvSplitPartitioningChanged, Handle);
            HFunc(TEvDataShard::TEvGetTableStats, Handle);
            HFunc(TEvPrivate::TEvAsyncTableStats, Handle);
            HFunc(TEvPrivate::TEvTableStatsError, Handle);
            HFunc(TEvDataShard::TEvKqpScan, Handle);
            HFunc(TEvDataShard::TEvUploadRowsRequest, Handle);
            HFunc(TEvDataShard::TEvEraseRowsRequest, Handle);
            HFunc(TEvDataShard::TEvOverloadUnsubscribe, Handle);
            HFunc(TEvDataShard::TEvConditionalEraseRowsRequest, Handle);
            HFunc(TEvPrivate::TEvConditionalEraseRowsRegistered, Handle);
            HFunc(TEvDataShard::TEvRead, Handle);
            HFunc(TEvDataShard::TEvReadContinue, Handle);
            HFunc(TEvDataShard::TEvReadAck, Handle);
            HFunc(TEvDataShard::TEvReadCancel, Handle);
            HFunc(TEvDataShard::TEvReadColumnsRequest, Handle);
            HFunc(NEvents::TDataEvents::TEvWrite, Handle);
            HFunc(TEvDataShard::TEvGetInfoRequest, Handle);
            HFunc(TEvDataShard::TEvListOperationsRequest, Handle);
            HFunc(TEvDataShard::TEvGetDataHistogramRequest, Handle);
            HFunc(TEvDataShard::TEvGetOperationRequest, Handle);
            HFunc(TEvDataShard::TEvGetReadTableSinkStateRequest, Handle);
            HFunc(TEvDataShard::TEvGetReadTableScanStateRequest, Handle);
            HFunc(TEvDataShard::TEvGetReadTableStreamStateRequest, Handle);
            HFunc(TEvDataShard::TEvGetRSInfoRequest, Handle);
            HFunc(TEvDataShard::TEvGetSlowOpProfilesRequest, Handle);
            HFunc(TEvDataShard::TEvRefreshVolatileSnapshotRequest, Handle);
            HFunc(TEvDataShard::TEvDiscardVolatileSnapshotRequest, Handle);
            HFuncTraced(TEvDataShard::TEvBuildIndexCreateRequest, Handle);
            HFunc(TEvDataShard::TEvSampleKRequest, Handle);
            HFunc(TEvDataShard::TEvCdcStreamScanRequest, Handle);
            HFunc(TEvPrivate::TEvCdcStreamScanRegistered, Handle);
            HFunc(TEvPrivate::TEvCdcStreamScanProgress, Handle);
            HFunc(TEvPrivate::TEvAsyncJobComplete, Handle);
            HFunc(TEvPrivate::TEvRestartOperation, Handle);
            HFunc(TEvPrivate::TEvPeriodicWakeup, DoPeriodicTasks);
            HFunc(TEvents::TEvUndelivered, Handle);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound, Handle);
            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted);
            IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable);
            HFunc(NChangeExchange::TEvChangeExchange::TEvRequestRecords, Handle);
            HFunc(NChangeExchange::TEvChangeExchange::TEvRemoveRecords, Handle);
            HFunc(TEvPrivate::TEvRequestChangeRecords, Handle);
            HFunc(TEvPrivate::TEvRemoveChangeRecords, Handle);
            HFunc(TEvChangeExchange::TEvHandshake, Handle);
            HFunc(TEvPrivate::TEvChangeExchangeExecuteHandshakes, Handle);
            HFunc(TEvChangeExchange::TEvApplyRecords, Handle);
            HFunc(TEvChangeExchange::TEvActivateSender, Handle);
            HFunc(TEvChangeExchange::TEvActivateSenderAck, Handle);
            HFunc(TEvChangeExchange::TEvSplitAck, Handle);
            HFunc(TEvDataShard::TEvGetRemovedRowVersions, Handle);
            HFunc(TEvDataShard::TEvCompactBorrowed, Handle);
            HFunc(TEvDataShard::TEvCompactTable, Handle);
            HFunc(TEvDataShard::TEvGetCompactTableStats, Handle);
            HFunc(TEvDataShard::TEvApplyReplicationChanges, Handle);
            fFunc(TEvDataShard::EvGetReplicationSourceOffsets, HandleByReplicationSourceOffsetsServer);
            fFunc(TEvDataShard::EvReplicationSourceOffsetsAck, HandleByReplicationSourceOffsetsServer);
            fFunc(TEvDataShard::EvReplicationSourceOffsetsCancel, HandleByReplicationSourceOffsetsServer);
            HFunc(TEvLongTxService::TEvLockStatus, Handle);
            HFunc(TEvDataShard::TEvGetOpenTxs, Handle);
            HFuncTraced(TEvPrivate::TEvRemoveLockChangeRecords, Handle);
            HFunc(TEvPrivate::TEvConfirmReadonlyLease, Handle);
            HFunc(TEvPrivate::TEvPlanPredictedTxs, Handle);
            HFunc(NStat::TEvStatistics::TEvStatisticsRequest, Handle);
            HFunc(TEvPrivate::TEvStatisticsScanFinished, Handle);
            HFuncTraced(TEvPrivate::TEvRemoveSchemaSnapshots, Handle);
            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    ALOG_WARN(NKikimrServices::TX_DATASHARD, "TDataShard::StateWork unhandled event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString());
                }
                break;
        }
    }

    // This is the main state
    void StateWorkAsFollower(TAutoPtr<NActors::IEventHandle> &ev) {
        TRACE_EVENT(NKikimrServices::TX_DATASHARD);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvGone, Handle);
            HFuncTraced(TEvDataShard::TEvProposeTransaction, HandleAsFollower);
            HFuncTraced(TEvPrivate::TEvDelayedProposeTransaction, Handle);
            HFuncTraced(TEvDataShard::TEvReadColumnsRequest, Handle);
            HFuncTraced(TEvTabletPipe::TEvServerConnected, Handle);
            HFuncTraced(TEvTabletPipe::TEvServerDisconnected, Handle);
            HFuncTraced(TEvDataShard::TEvRead, Handle);
            HFuncTraced(TEvDataShard::TEvReadContinue, Handle);
            HFuncTraced(TEvDataShard::TEvReadAck, Handle);
            HFuncTraced(TEvDataShard::TEvReadCancel, Handle);
            HFuncTraced(NEvents::TDataEvents::TEvWrite, Handle);
        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                ALOG_WARN(NKikimrServices::TX_DATASHARD, "TDataShard::StateWorkAsFollower unhandled event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
            }
            break;
        }
    }

    void Die(const TActorContext &ctx) override;

    void SendViaSchemeshardPipe(const TActorContext &ctx, ui64 tabletId, THolder<TEvDataShard::TEvSchemaChanged> event) {
        Y_ABORT_UNLESS(tabletId);
        Y_ABORT_UNLESS(CurrentSchemeShardId == tabletId);

        if (!SchemeShardPipe) {
            NTabletPipe::TClientConfig clientConfig;
            SchemeShardPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
        }
        NTabletPipe::SendData(ctx, SchemeShardPipe, event.Release());
    }

    void ReportState(const TActorContext &ctx, ui32 state) {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, TabletID() << " Reporting state " << DatashardStateName(State)
                    << " to schemeshard " << CurrentSchemeShardId);
        Y_ABORT_UNLESS(state != TShardState::Offline || !HasSharedBlobs(),
                 "Datashard %" PRIu64 " tried to go offline while having shared blobs", TabletID());
        if (!StateReportPipe) {
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = SchemeShardPipeRetryPolicy;
            StateReportPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, CurrentSchemeShardId, clientConfig));
        }
        THolder<TEvDataShard::TEvStateChanged> ev(new TEvDataShard::TEvStateChanged(ctx.SelfID, TabletID(), state));
        NTabletPipe::SendData(ctx, StateReportPipe, ev.Release());
    }

    void SendPeriodicTableStats(const TActorContext &ctx) {
        if (StatisticsDisabled)
            return;

        TInstant now = AppData(ctx)->TimeProvider->Now();

        if (LastDbStatsReportTime + gDbStatsReportInterval > now)
            return;

        auto* resourceMetrics = Executor()->GetResourceMetrics();

        for (const auto& t : TableInfos) {
            ui64 tableId = t.first;

            const TUserTable &ti = *t.second;

            // Don't report stats until they are build for the first time
            if (!ti.Stats.StatsUpdateTime) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "SendPeriodicTableStats at datashard " << TabletID()
                            << ", for tableId " << tableId << ", but no stats yet"
                );
                continue;
            }

            if (!DbStatsReportPipe) {
                NTabletPipe::TClientConfig clientConfig;
                DbStatsReportPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, CurrentSchemeShardId, clientConfig));
            }

            THolder<TEvDataShard::TEvPeriodicTableStats> ev(new TEvDataShard::TEvPeriodicTableStats(TabletID(), PathOwnerId, tableId));
            ev->Record.SetShardState(State);
            ev->Record.SetGeneration(Executor()->Generation());
            ev->Record.SetRound(StatsReportRound++);
            ev->Record.MutableTableStats()->SetRowCount(ti.Stats.DataStats.RowCount + ti.Stats.MemRowCount);

            ev->Record.MutableTableStats()->SetDataSize(ti.Stats.DataStats.DataSize.Size + ti.Stats.MemDataSize);
            ev->Record.MutableTableStats()->SetIndexSize(ti.Stats.DataStats.IndexSize.Size);
            ev->Record.MutableTableStats()->SetInMemSize(ti.Stats.MemDataSize);

            TMap<ui8, std::tuple<ui64, ui64>> channels; // Channel -> (DataSize, IndexSize)
            for (size_t channel = 0; channel < ti.Stats.DataStats.DataSize.ByChannel.size(); channel++) {
                if (ti.Stats.DataStats.DataSize.ByChannel[channel]) {
                    std::get<0>(channels[channel]) = ti.Stats.DataStats.DataSize.ByChannel[channel];
                }
            }
            for (size_t channel = 0; channel < ti.Stats.DataStats.IndexSize.ByChannel.size(); channel++) {
                if (ti.Stats.DataStats.IndexSize.ByChannel[channel]) {
                    std::get<1>(channels[channel]) = ti.Stats.DataStats.IndexSize.ByChannel[channel];
                }
            }
            for (auto p : channels) {
                auto item = ev->Record.MutableTableStats()->AddChannels();
                item->SetChannel(p.first);
                item->SetDataSize(std::get<0>(p.second));
                item->SetIndexSize(std::get<1>(p.second));
            }

            ev->Record.MutableTableStats()->SetLastAccessTime(ti.Stats.AccessTime.MilliSeconds());
            ev->Record.MutableTableStats()->SetLastUpdateTime(ti.Stats.UpdateTime.MilliSeconds());

            ev->Record.MutableTableStats()->SetImmediateTxCompleted(TabletCounters->Cumulative()[COUNTER_PREPARE_IMMEDIATE].Get() + TabletCounters->Cumulative()[COUNTER_WRITE_IMMEDIATE].Get());
            ev->Record.MutableTableStats()->SetPlannedTxCompleted(TabletCounters->Cumulative()[COUNTER_PLANNED_TX_COMPLETE].Get());
            ev->Record.MutableTableStats()->SetTxRejectedByOverload(TabletCounters->Cumulative()[COUNTER_PREPARE_OVERLOADED].Get() + TabletCounters->Cumulative()[COUNTER_WRITE_OVERLOADED].Get());
            ev->Record.MutableTableStats()->SetTxRejectedBySpace(TabletCounters->Cumulative()[COUNTER_PREPARE_OUT_OF_SPACE].Get() + TabletCounters->Cumulative()[COUNTER_WRITE_OUT_OF_SPACE].Get());
            ev->Record.MutableTableStats()->SetTxCompleteLagMsec(TabletCounters->Simple()[COUNTER_TX_COMPLETE_LAG].Get());
            ev->Record.MutableTableStats()->SetInFlightTxCount(TabletCounters->Simple()[COUNTER_TX_IN_FLY].Get()
                + TabletCounters->Simple()[COUNTER_IMMEDIATE_TX_IN_FLY].Get());

            ev->Record.MutableTableStats()->SetRowUpdates(TabletCounters->Cumulative()[COUNTER_ENGINE_HOST_UPDATE_ROW].Get()
                + TabletCounters->Cumulative()[COUNTER_UPLOAD_ROWS].Get());
            ev->Record.MutableTableStats()->SetRowDeletes(TabletCounters->Cumulative()[COUNTER_ENGINE_HOST_ERASE_ROW].Get()
                + TabletCounters->Cumulative()[COUNTER_ERASE_ROWS].Get());
            ev->Record.MutableTableStats()->SetRowReads(TabletCounters->Cumulative()[COUNTER_ENGINE_HOST_SELECT_ROW].Get());
            ev->Record.MutableTableStats()->SetRangeReads(TabletCounters->Cumulative()[COUNTER_ENGINE_HOST_SELECT_RANGE].Get());
            ev->Record.MutableTableStats()->SetRangeReadRows(TabletCounters->Cumulative()[COUNTER_ENGINE_HOST_SELECT_RANGE_ROWS].Get());
            if (resourceMetrics != nullptr) {
                resourceMetrics->Fill(*ev->Record.MutableTabletMetrics());
            }

            ev->Record.MutableTableStats()->SetPartCount(ti.Stats.PartCount);
            ev->Record.MutableTableStats()->SetSearchHeight(ti.Stats.SearchHeight);
            ev->Record.MutableTableStats()->SetLastFullCompactionTs(ti.Stats.LastFullCompaction.Seconds());
            ev->Record.MutableTableStats()->SetHasLoanedParts(Executor()->HasLoanedParts());

            if (!ti.Stats.PartOwners.contains(TabletID())) {
                ev->Record.AddUserTablePartOwners(TabletID());
            }
            for (const auto& pi : ti.Stats.PartOwners) {
                ev->Record.AddUserTablePartOwners(pi);
            }
            for (const auto& pi : SysTablesPartOwners) {
                ev->Record.AddSysTablesPartOwners(pi);
            }

            ev->Record.SetNodeId(ctx.ExecutorThread.ActorSystem->NodeId);
            ev->Record.SetStartTime(StartTime().MilliSeconds());

            if (DstSplitDescription)
                ev->Record.SetIsDstSplit(true);

            NTabletPipe::SendData(ctx, DbStatsReportPipe, ev.Release());
        }

        LastDbStatsReportTime = now;
    }

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override;
    void SerializeHistogram(const TUserTable &tinfo,
                            const NTable::THistogram &histogram,
                            NKikimrTxDataShard::TEvGetDataHistogramResponse::THistogram &hist);
    void SerializeKeySample(const TUserTable &tinfo,
                            const NTable::TKeyAccessSample &keySample,
                            NKikimrTxDataShard::TEvGetDataHistogramResponse::THistogram &hist);

    bool ByKeyFilterDisabled() const;
    bool AllowCancelROwithReadsets() const;

    void ResolveTablePath(const TActorContext &ctx);

public:
    NMonitoring::TDynamicCounters::TCounterPtr CounterReadIteratorLastKeyReset;
    void IncCounterReadIteratorLastKeyReset();
};

NKikimrTxDataShard::TError::EKind ConvertErrCode(NMiniKQL::IEngineFlat::EResult code);

Ydb::StatusIds::StatusCode ConvertToYdbStatusCode(NKikimrTxDataShard::TError::EKind);

template <class T>
void SetStatusError(T &rec,
                    Ydb::StatusIds::StatusCode status,
                    const TString &msg,
                    ui32 severity = NYql::TSeverityIds::S_ERROR)
{
    rec.MutableStatus()->SetCode(status);
    auto *issue = rec.MutableStatus()->AddIssues();
    issue->set_severity(severity);
    issue->set_message(msg);
}

void SendViaSession(const TActorId& sessionId,
                    const TActorId& target,
                    const TActorId& src,
                    IEventBase* event,
                    ui32 flags = 0,
                    ui64 cookie = 0,
                    NWilson::TTraceId traceId = {});

}}
