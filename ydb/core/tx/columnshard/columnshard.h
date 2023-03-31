#pragma once
#include "defs.h"
#include "blob_manager.h"

#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/message_seqno.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

#include <ydb/core/tx/long_tx_service/public/types.h>

// TODO: temporarily reuse datashard TEvScan (KIKIMR-11069) and TEvPeriodicTableStats
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {

namespace NColumnShard {
class TBlobGroupSelector;

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

    default:
        return Ydb::StatusIds::GENERIC_ERROR;
    }
}
}

struct TEvColumnShard {
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

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COLUMNSHARD),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COLUMNSHARD)");

    struct TEvProposeTransaction
        : public TEventPB<TEvProposeTransaction,
                          NKikimrTxColumnShard::TEvProposeTransaction,
                          EvProposeTransaction>
    {
        TEvProposeTransaction() = default;

        TEvProposeTransaction(NKikimrTxColumnShard::ETransactionKind txKind, const TActorId& source,
                ui64 txId, TString txBody)
        {
            Record.SetTxKind(txKind);
            ActorIdToProto(source, Record.MutableSource());
            Record.SetTxId(txId);
            Record.SetTxBody(std::move(txBody));
        }

        TEvProposeTransaction(NKikimrTxColumnShard::ETransactionKind txKind, ui64 ssId, const TActorId& source,
                ui64 txId, TString txBody)
            : TEvProposeTransaction(txKind, source, txId, std::move(txBody))
        {
            Y_VERIFY(txKind == NKikimrTxColumnShard::TX_KIND_SCHEMA);
            Record.SetSchemeShardId(ssId);
        }

        TEvProposeTransaction(NKikimrTxColumnShard::ETransactionKind txKind, ui64 ssId, const TActorId& source,
                ui64 txId, TString txBody, const NKikimrSubDomains::TProcessingParams& processingParams)
            : TEvProposeTransaction(txKind, ssId, source, txId, std::move(txBody))
        {
            Record.MutableProcessingParams()->CopyFrom(processingParams);
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

    // Fallback read BlobCache read to tablet (small blobs or S3)
    struct TEvReadBlobRanges : public TEventPB<TEvReadBlobRanges,
                                                NKikimrTxColumnShard::TEvReadBlobRanges,
                                                TEvColumnShard::EvReadBlobRanges>
    {
        std::vector<NOlap::TBlobRange> BlobRanges;

        TEvReadBlobRanges() = default;

        TEvReadBlobRanges(const std::vector<NOlap::TBlobRange>& blobRanges)
            : BlobRanges(blobRanges)
        {
            for (const auto& r : BlobRanges) {
                auto* range = Record.AddBlobRanges();
                range->SetBlobId(r.BlobId.ToStringNew());
                range->SetOffset(r.Offset);
                range->SetSize(r.Size);
            }
        }

        void RestoreFromProto(NColumnShard::TBlobGroupSelector* dsGroupSelector, TString& errString) {
            BlobRanges.clear();
            BlobRanges.reserve(Record.BlobRangesSize());

            for (const auto& range : Record.GetBlobRanges()) {
                auto blobId = NColumnShard::TUnifiedBlobId::ParseFromString(range.GetBlobId(), dsGroupSelector,
                                                                            errString);
                if (!errString.empty()) {
                    return;
                }
                BlobRanges.push_back(NOlap::TBlobRange{blobId, (ui32)range.GetOffset(), (ui32)range.GetSize()});
            }
        }
    };

    struct TEvReadBlobRangesResult : public TEventPB<TEvReadBlobRangesResult,
                                                NKikimrTxColumnShard::TEvReadBlobRangesResult,
                                                TEvColumnShard::EvReadBlobRangesResult>
    {
        explicit TEvReadBlobRangesResult(ui64 tabletId = 0) {
            Record.SetTabletId(tabletId);
        }
    };

    struct TEvWrite : public TEventPB<TEvWrite, NKikimrTxColumnShard::TEvWrite, TEvColumnShard::EvWrite> {
        TEvWrite() = default;

        TEvWrite(const TActorId& source, ui64 metaShard, ui64 writeId, ui64 tableId,
                 const TString& dedupId, const TString& data) {
            ActorIdToProto(source, Record.MutableSource());
            Record.SetTxInitiator(metaShard);
            Record.SetWriteId(writeId);
            Record.SetTableId(tableId);
            Record.SetDedupId(dedupId);
            Record.SetData(data);
        }

        TEvWrite(const TActorId& source, const NLongTxService::TLongTxId& longTxId, ui64 tableId,
                 const TString& dedupId, const TString& data) {
            ActorIdToProto(source, Record.MutableSource());
            Record.SetTableId(tableId);
            Record.SetDedupId(dedupId);
            Record.SetData(data);
            longTxId.ToProto(Record.MutableLongTxId());
        }

        // Optionally set schema to deserialize data with
        void SetArrowSchema(const TString& arrowSchema) {
            Record.MutableMeta()->SetFormat(NKikimrTxColumnShard::FORMAT_ARROW);
            Record.MutableMeta()->SetSchema(arrowSchema);
        }

        TActorId GetSource() const {
            return ActorIdFromProto(Record.GetSource());
        }

        NKikimrProto::EReplyStatus PutStatus = NKikimrProto::UNKNOWN;
        NColumnShard::TUnifiedBlobId BlobId;
        std::shared_ptr<arrow::RecordBatch> WrittenBatch;
        NColumnShard::TBlobBatch BlobBatch;
        NColumnShard::TUsage ResourceUsage;
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
        ui64 MaxSmallBlobSize;
    };

    struct TEvWriteResult : public TEventPB<TEvWriteResult, NKikimrTxColumnShard::TEvWriteResult,
                            TEvColumnShard::EvWriteResult> {
        TEvWriteResult() = default;

        TEvWriteResult(ui64 origin, ui64 metaShard, ui64 writeId, ui64 tableId, const TString& dedupId, ui32 status) {
            Record.SetOrigin(origin);
            Record.SetTxInitiator(metaShard);
            Record.SetWriteId(writeId);
            Record.SetTableId(tableId);
            Record.SetDedupId(dedupId);
            Record.SetStatus(status);
        }
    };

    struct TEvRead : public TEventPB<TEvRead, NKikimrTxColumnShard::TEvRead, TEvColumnShard::EvRead> {
        TEvRead() = default;

        TEvRead(const TActorId& source, ui64 metaShard, ui64 planStep, ui64 txId, ui64 tableId = 0) {
            ActorIdToProto(source, Record.MutableSource());
            Record.SetTxInitiator(metaShard);
            Record.SetPlanStep(planStep);
            Record.SetTxId(txId);
            Record.SetTableId(tableId);
        }

        TActorId GetSource() const {
            return ActorIdFromProto(Record.GetSource());
        }
    };

    struct TEvReadResult : public TEventPB<TEvReadResult, NKikimrTxColumnShard::TEvReadResult,
                            TEvColumnShard::EvReadResult> {
        TEvReadResult() = default;

        TEvReadResult(ui64 origin, ui64 metaShard, ui64 planStep, ui64 txId, ui64 tableId, ui32 batch,
                      bool finished, ui32 status) {
            Record.SetOrigin(origin);
            Record.SetTxInitiator(metaShard);
            Record.SetPlanStep(planStep);
            Record.SetTxId(txId);
            Record.SetTableId(tableId);
            Record.SetBatch(batch);
            Record.SetFinished(finished);
            Record.SetStatus(status);
        }

        TEvReadResult(const TEvReadResult& ev) {
            Record.CopyFrom(ev.Record);
        }
    };

    using TEvScan = TEvDataShard::TEvKqpScan;
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

inline auto& Proto(TEvColumnShard::TEvWrite* ev) {
    return ev->Record;
}

inline auto& Proto(TEvColumnShard::TEvRead* ev) {
    return ev->Record;
}

inline auto& Proto(TEvColumnShard::TEvWriteResult* ev) {
    return ev->Record;
}

inline auto& Proto(TEvColumnShard::TEvReadResult* ev) {
    return ev->Record;
}

inline TMessageSeqNo SeqNoFromProto(const NKikimrTxColumnShard::TSchemaSeqNo& proto) {
    return TMessageSeqNo(proto.GetGeneration(), proto.GetRound());
}

IActor* CreateColumnShard(const TActorId& tablet, TTabletStorageInfo* info);

}
