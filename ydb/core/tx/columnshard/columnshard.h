#pragma once
#include "defs.h"
#include "blob.h"
#include "common/snapshot.h"

#include <ydb/core/protos/statistics.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/message_seqno.h>
#include <ydb/core/tx/data_events/common/modification_type.h>
#include <ydb/core/tx/data_events/write_data.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/public/types.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr {

namespace NOlap {
class TPKRangesFilter;
}

namespace NColumnShard {

inline Ydb::StatusIds::StatusCode ConvertToYdbStatus(NKikimrTxColumnShard::EResultStatus columnShardStatus) {
    switch (columnShardStatus) {
    case NKikimrTxColumnShard::UNSPECIFIED:
        return Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;

    case NKikimrTxColumnShard::PREPARED:
    case NKikimrTxColumnShard::SUCCESS:
        return Ydb::StatusIds::SUCCESS;

    case NKikimrTxColumnShard::ABORTED:
        return Ydb::StatusIds::ABORTED;

    case NKikimrTxColumnShard::ERROR:
        return Ydb::StatusIds::GENERIC_ERROR;

    case NKikimrTxColumnShard::TIMEOUT:
        return Ydb::StatusIds::TIMEOUT;

    case NKikimrTxColumnShard::SCHEMA_ERROR:
    case NKikimrTxColumnShard::SCHEMA_CHANGED:
        return Ydb::StatusIds::SCHEME_ERROR;

    case NKikimrTxColumnShard::OVERLOADED:
        return Ydb::StatusIds::OVERLOADED;

    case NKikimrTxColumnShard::STORAGE_ERROR:
        return Ydb::StatusIds::UNAVAILABLE;

    default:
        return Ydb::StatusIds::GENERIC_ERROR;
    }
}
}

namespace TEvColumnShard {
    enum EEv {
        EvProposeTransaction = EventSpaceBegin(TKikimrEvents::ES_TX_COLUMNSHARD),
        EvCancelTransactionProposal,
        EvProposeTransactionResult,
        EvNotifyTxCompletion,
        EvNotifyTxCompletionResult,
        EvReadBlobRanges,
        EvReadBlobRangesResult,
        EvCheckPlannedTransaction,

        EvWrite = EvProposeTransaction + 256,
        EvRead,
        EvWriteResult,
        EvReadResult,

        EvDeleteSharedBlobs,
        EvDeleteSharedBlobsFinished,

        EvDataSharingProposeFromInitiator,
        EvDataSharingConfirmFromInitiator,
        EvDataSharingAckFinishFromInitiator,
        EvDataSharingStartToSource,
        EvDataSharingSendDataFromSource,
        EvDataSharingAckDataToSource,
        EvDataSharingFinishedFromSource,
        EvDataSharingAckFinishToSource,
        EvDataSharingCheckStatusFromInitiator,
        EvDataSharingCheckStatusResult,
        EvApplyLinksModification,
        EvApplyLinksModificationFinished,
        EvInternalScan,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COLUMNSHARD),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COLUMNSHARD)");

    struct TEvInternalScan: public TEventLocal<TEvInternalScan, EvInternalScan> {
    private:
        YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, PathId);
        YDB_READONLY(NOlap::TSnapshot, Snapshot, NOlap::TSnapshot::Zero());
        YDB_READONLY_DEF(std::optional<ui64>, LockId);
        YDB_ACCESSOR(bool, Reverse, false);
        YDB_ACCESSOR(ui32, ItemsLimit, 0);
        YDB_READONLY_DEF(std::vector<ui32>, ColumnIds);
        std::set<ui32> ColumnIdsSet;
    public:
        TString TaskIdentifier;
        std::shared_ptr<NOlap::TPKRangesFilter> RangesFilter;
    public:
        void AddColumn(const ui32 id) {
            AFL_VERIFY(ColumnIdsSet.emplace(id).second);
            ColumnIds.emplace_back(id);
        }

        TEvInternalScan(const NColumnShard::TUnifiedPathId pathId, const NOlap::TSnapshot& snapshot, const std::optional<ui64> lockId)
            : PathId(pathId)
            , Snapshot(snapshot)
            , LockId(lockId)
        {
            AFL_VERIFY(Snapshot.Valid());
        }
    };

    struct TEvProposeTransaction
        : public TEventPB<TEvProposeTransaction,
                          NKikimrTxColumnShard::TEvProposeTransaction,
                          EvProposeTransaction>
    {
        TEvProposeTransaction() = default;

        TEvProposeTransaction(NKikimrTxColumnShard::ETransactionKind txKind, const TActorId& source,
                ui64 txId, TString txBody, const ui32 flags = 0)
        {
            Record.SetTxKind(txKind);
            ActorIdToProto(source, Record.MutableSource());
            Record.SetTxId(txId);
            Record.SetTxBody(std::move(txBody));
            Record.SetFlags(flags);
        }

        TEvProposeTransaction(NKikimrTxColumnShard::ETransactionKind txKind, ui64 ssId, const TActorId& source,
                ui64 txId, TString txBody, const ui32 flags, ui64 subDomainPathId)
            : TEvProposeTransaction(txKind, source, txId, std::move(txBody), flags)
        {
//            Y_ABORT_UNLESS(txKind == NKikimrTxColumnShard::TX_KIND_SCHEMA);
            Record.SetSchemeShardId(ssId);
            if (subDomainPathId != 0) {
                Record.SetSubDomainPathId(subDomainPathId);
            }
        }

        TEvProposeTransaction(NKikimrTxColumnShard::ETransactionKind txKind, ui64 ssId, const TActorId& source,
            ui64 txId, TString txBody, const TMessageSeqNo& seqNo, const NKikimrSubDomains::TProcessingParams& processingParams, const ui32 flags, ui64 subDomainPathId)
            : TEvProposeTransaction(txKind, ssId, source, txId, std::move(txBody), flags, subDomainPathId)
        {
            Record.MutableProcessingParams()->CopyFrom(processingParams);
            *Record.MutableSeqNo() = seqNo.SerializeToProto();
        }

        TActorId GetSource() const {
            return ActorIdFromProto(Record.GetSource());
        }
    };

    struct TEvCheckPlannedTransaction
        : public TEventPB<TEvCheckPlannedTransaction,
                          NKikimrTxColumnShard::TEvCheckPlannedTransaction,
                          EvCheckPlannedTransaction>
    {
        TEvCheckPlannedTransaction() = default;

        TEvCheckPlannedTransaction(const TActorId& source, ui64 planStep, ui64 txId) {
            ActorIdToProto(source, Record.MutableSource());
            Record.SetStep(planStep);
            Record.SetTxId(txId);
        }

        TActorId GetSource() const {
            return ActorIdFromProto(Record.GetSource());
        }
    };

    struct TEvCancelTransactionProposal
        : public TEventPB<TEvCancelTransactionProposal,
                          NKikimrTxColumnShard::TEvCancelTransactionProposal,
                          EvCancelTransactionProposal>
    {
        TEvCancelTransactionProposal() = default;

        explicit TEvCancelTransactionProposal(ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvProposeTransactionResult : public TEventPB<TEvProposeTransactionResult,
                                            NKikimrTxColumnShard::TEvProposeTransactionResult,
                                            TEvColumnShard::EvProposeTransactionResult> {
        TEvProposeTransactionResult() = default;

        TEvProposeTransactionResult(ui64 origin, NKikimrTxColumnShard::ETransactionKind txKind, ui64 txId,
                                    NKikimrTxColumnShard::EResultStatus status,
                                    const TString& statusMessage = TString())
        {
            Record.SetOrigin(origin);
            Record.SetTxKind(txKind);
            Record.SetTxId(txId);
            Record.SetMinStep(0);
            Record.SetStatus(status);
            if (!statusMessage.empty()) {
                Record.SetStatusMessage(statusMessage);
            }
        }
    };

    struct TEvNotifyTxCompletion
        : public TEventPB<TEvNotifyTxCompletion,
                          NKikimrTxColumnShard::TEvNotifyTxCompletion,
                          EvNotifyTxCompletion>
    {
        TEvNotifyTxCompletion() = default;

        explicit TEvNotifyTxCompletion(ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvNotifyTxCompletionResult
        : public TEventPB<TEvNotifyTxCompletionResult,
                          NKikimrTxColumnShard::TEvNotifyTxCompletionResult,
                          EvNotifyTxCompletionResult>
    {
        TEvNotifyTxCompletionResult() = default;

        TEvNotifyTxCompletionResult(ui64 origin, ui64 txId) {
            Record.SetOrigin(origin);
            Record.SetTxId(txId);
        }
    };
};

inline auto& Proto(TEvColumnShard::TEvProposeTransaction* ev) {
    return ev->Record;
}

inline auto& Proto(TEvColumnShard::TEvCheckPlannedTransaction* ev) {
    return ev->Record;
}

inline auto& Proto(TEvColumnShard::TEvProposeTransactionResult* ev) {
    return ev->Record;
}

inline TMessageSeqNo SeqNoFromProto(const NKikimrTxColumnShard::TSchemaSeqNo& proto) {
    return TMessageSeqNo(proto.GetGeneration(), proto.GetRound());
}

IActor* CreateColumnShard(const TActorId& tablet, TTabletStorageInfo* info);

}
