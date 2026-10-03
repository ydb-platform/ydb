#pragma once

#include "status.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/protos/tx.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/message_seqno.h>

namespace NKikimr {

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

    EvOverloadReady,
    EvOverloadUnsubscribe,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COLUMNSHARD), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COLUMNSHARD)");

struct TEvProposeTransaction: public TEventPB<TEvProposeTransaction, NKikimrTxColumnShard::TEvProposeTransaction, EvProposeTransaction> {
    TEvProposeTransaction() = default;

    TEvProposeTransaction(
        NKikimrTxColumnShard::ETransactionKind txKind, const TActorId& source, ui64 txId, TString txBody, const ui32 flags = 0) {
        Record.SetTxKind(txKind);
        ActorIdToProto(source, Record.MutableSource());
        Record.SetTxId(txId);
        Record.SetTxBody(std::move(txBody));
        Record.SetFlags(flags);
    }

    TEvProposeTransaction(NKikimrTxColumnShard::ETransactionKind txKind, ui64 ssId, const TActorId& source, ui64 txId, TString txBody,
        const ui32 flags, ui64 subDomainPathId)
        : TEvProposeTransaction(txKind, source, txId, std::move(txBody), flags)
    {
        //            Y_ABORT_UNLESS(txKind == NKikimrTxColumnShard::TX_KIND_SCHEMA);
        Record.SetSchemeShardId(ssId);
        if (subDomainPathId != 0) {
            Record.SetSubDomainPathId(subDomainPathId);
        }
    }

    TEvProposeTransaction(NKikimrTxColumnShard::ETransactionKind txKind, ui64 ssId, const TActorId& source, ui64 txId, TString txBody,
        const TMessageSeqNo& seqNo, const NKikimrSubDomains::TProcessingParams& processingParams, const ui32 flags, ui64 subDomainPathId)
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
    : public TEventPB<TEvCheckPlannedTransaction, NKikimrTxColumnShard::TEvCheckPlannedTransaction, EvCheckPlannedTransaction> {
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

struct TEvProposeTransactionResult: public TEventPB<TEvProposeTransactionResult, NKikimrTxColumnShard::TEvProposeTransactionResult,
                                        TEvColumnShard::EvProposeTransactionResult> {
    TEvProposeTransactionResult() = default;

    TEvProposeTransactionResult(ui64 origin, NKikimrTxColumnShard::ETransactionKind txKind, ui64 txId,
        NKikimrTxColumnShard::EResultStatus status, const TString& statusMessage = TString()) {
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

struct TEvNotifyTxCompletion: public TEventPB<TEvNotifyTxCompletion, NKikimrTxColumnShard::TEvNotifyTxCompletion, EvNotifyTxCompletion> {
    TEvNotifyTxCompletion() = default;

    explicit TEvNotifyTxCompletion(ui64 txId) {
        Record.SetTxId(txId);
    }
};

struct TEvNotifyTxCompletionResult
    : public TEventPB<TEvNotifyTxCompletionResult, NKikimrTxColumnShard::TEvNotifyTxCompletionResult, EvNotifyTxCompletionResult> {
    TEvNotifyTxCompletionResult() = default;

    TEvNotifyTxCompletionResult(ui64 origin, ui64 txId) {
        Record.SetOrigin(origin);
        Record.SetTxId(txId);
    }
};

struct TEvOverloadReady: public TEventPB<TEvOverloadReady, NKikimrTxColumnShard::TEvOverloadReady, EvOverloadReady> {
    TEvOverloadReady() = default;

    explicit TEvOverloadReady(ui64 tabletId, ui64 seqNo) {
        Record.SetTabletID(tabletId);
        Record.SetSeqNo(seqNo);
    }
};

struct TEvOverloadUnsubscribe: public TEventPB<TEvOverloadUnsubscribe, NKikimrTxColumnShard::TEvOverloadUnsubscribe, EvOverloadUnsubscribe> {
    TEvOverloadUnsubscribe() = default;

    explicit TEvOverloadUnsubscribe(ui64 seqNo) {
        Record.SetSeqNo(seqNo);
    }
};
};   // namespace TEvColumnShard

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

}   // namespace NKikimr
