#pragma once

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/tx.pb.h>
#include <ydb/core/tx/tx_processing.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>
#include <util/system/types.h>

namespace NKikimr::NPQ {

struct TDistributedTransaction {
    TDistributedTransaction() = default;

    void OnProposeTransaction(const NKikimrPQ::TEvProposeTransaction& event,
                              ui64 minStep);
    void OnPlanStep(ui64 step);
    void OnTxCalcPredicateResult(const TEvPQ::TEvTxCalcPredicateResult& event);
    void OnReadSet(const NKikimrTx::TEvReadSet& event,
                   const TActorId& sender,
                   std::unique_ptr<TEvTxProcessing::TEvReadSetAck> ack);
    void OnReadSetAck(const NKikimrTx::TEvReadSetAck& event);
    void OnTxCommitDone(const TEvPQ::TEvTxCommitDone& event);

    using EDecision = NKikimrTx::TReadSetData::EDecision;
    using EState = NKikimrPQ::TTransaction::EState;

    ui64 TxId = Max<ui64>();
    ui64 Step = Max<ui64>();
    EState State = NKikimrPQ::TTransaction::UNKNOWN;
    ui64 MinStep = Max<ui64>();
    ui64 MaxStep = Max<ui64>();
    THashSet<ui64> Senders;        // список отправителей TEvReadSet
    THashSet<ui64> Receivers;      // список получателей TEvReadSet
    TVector<NKikimrPQ::TPartitionOperation> Operations;

    EDecision SelfDecision = NKikimrTx::TReadSetData::DECISION_UNKNOWN;
    EDecision ParticipantsDecision = NKikimrTx::TReadSetData::DECISION_UNKNOWN;
    NActors::TActorId Source;      // отправитель TEvProposeTransaction
    THashSet<ui32> Partitions;     // список участвующих партиций

    size_t PartitionRepliesCount = 0;
    size_t PartitionRepliesExpected = 0;

    size_t ReadSetCount = 0;

    THashMap<NActors::TActorId, std::unique_ptr<TEvTxProcessing::TEvReadSetAck>> ReadSetAcks;

    bool WriteInProgress = false;

    void SetDecision(EDecision decision);
    void SetDecision(ui64 tablet, EDecision decision);

    EDecision GetDecision() const;

    bool HaveParticipantsDecision() const;
    bool HaveAllRecipientsReceive() const;

    void AddCmdWrite(NKikimrClient::TKeyValueRequest& request, EState state);
    void AddCmdDelete(NKikimrClient::TKeyValueRequest& request);

    static void SetDecision(NKikimrTx::TReadSetData::EDecision& var, NKikimrTx::TReadSetData::EDecision value);

    TString GetKey() const;
};

}
