#pragma once

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/msgbus_kv.pb.h>
#include <ydb/core/protos/tx.pb.h>
#include <ydb/core/tx/tx_processing.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>
#include <util/system/types.h>

namespace NKikimr::NPQ {

struct TDistributedTransaction {
    TDistributedTransaction() = default;
    explicit TDistributedTransaction(const NKikimrPQ::TTransaction& tx);

    void OnProposeTransaction(const NKikimrPQ::TEvProposeTransaction& event,
                              ui64 minStep,
                              ui64 extractTabletId);
    void OnProposeTransaction(const NKikimrPQ::TDataTransaction& txBody,
                              ui64 extractTabletId);
    void OnProposeTransaction(const NKikimrPQ::TConfigTransaction& txBody,
                              ui64 extractTabletId);
    void OnPlanStep(ui64 step);
    void OnTxCalcPredicateResult(const TEvPQ::TEvTxCalcPredicateResult& event);
    void OnProposePartitionConfigResult(const TEvPQ::TEvProposePartitionConfigResult& event);
    void OnReadSet(const NKikimrTx::TEvReadSet& event,
                   const TActorId& sender,
                   std::unique_ptr<TEvTxProcessing::TEvReadSetAck> ack);
    void OnReadSetAck(const NKikimrTx::TEvReadSetAck& event);
    void OnTxCommitDone(const TEvPQ::TEvTxCommitDone& event);

    using EDecision = NKikimrTx::TReadSetData::EDecision;
    using EState = NKikimrPQ::TTransaction::EState;

    NKikimrPQ::TTransaction::EKind Kind = NKikimrPQ::TTransaction::KIND_UNKNOWN;

    ui64 TxId = Max<ui64>();
    ui64 Step = Max<ui64>();
    EState State = NKikimrPQ::TTransaction::UNKNOWN;
    ui64 MinStep = Max<ui64>();
    ui64 MaxStep = Max<ui64>();
    THashMap<ui64, NKikimrPQ::TTransaction::TPredicateReceived> PredicatesReceived;
    THashMap<ui64, bool> PredicateRecipients;
    TVector<NKikimrPQ::TPartitionOperation> Operations;
    TMaybe<TWriteId> WriteId;

    EDecision SelfDecision = NKikimrTx::TReadSetData::DECISION_UNKNOWN;
    EDecision ParticipantsDecision = NKikimrTx::TReadSetData::DECISION_UNKNOWN;
    NActors::TActorId SourceActor; // отправитель TEvProposeTransaction
    THashSet<ui32> Partitions;     // список участвующих партиций

    size_t PartitionRepliesCount = 0;
    size_t PartitionRepliesExpected = 0;

    size_t ReadSetCount = 0;

    THashMap<NActors::TActorId, std::unique_ptr<TEvTxProcessing::TEvReadSetAck>> ReadSetAcks;

    NKikimrPQ::TPQTabletConfig TabletConfig;
    NKikimrPQ::TBootstrapConfig BootstrapConfig;
    NPersQueue::TTopicConverterPtr TopicConverter;

    bool WriteInProgress = false;

    EDecision GetDecision() const;

    bool HaveParticipantsDecision() const;
    bool HaveAllRecipientsReceive() const;

    void AddCmdWrite(NKikimrClient::TKeyValueRequest& request, EState state);
    void AddCmdDelete(NKikimrClient::TKeyValueRequest& request);

    static void SetDecision(NKikimrTx::TReadSetData::EDecision& var, NKikimrTx::TReadSetData::EDecision value);

    TString GetKey() const;

    void AddCmdWriteDataTx(NKikimrPQ::TTransaction& tx);
    void AddCmdWriteConfigTx(NKikimrPQ::TTransaction& tx);

    void InitDataTransaction(const NKikimrPQ::TTransaction& tx);
    void InitConfigTransaction(const NKikimrPQ::TTransaction& tx);

    void InitPartitions(const google::protobuf::RepeatedPtrField<NKikimrPQ::TPartitionOperation>& tx);
    void InitPartitions();

    template<class E>
    void OnPartitionResult(const E& event, EDecision decision);

    TString LogPrefix() const;

    struct TSerializedMessage {
        ui32 Type;
        TIntrusivePtr<TEventSerializedData> Data;

        TSerializedMessage(ui32 type, TIntrusivePtr<TEventSerializedData> data) :
            Type(type),
            Data(data)
        {
        }
    };

    THashMap<ui64, TVector<TSerializedMessage>> OutputMsgs;

    void BindMsgToPipe(ui64 tabletId, const IEventBase& event);
    void UnbindMsgsFromPipe(ui64 tabletId);
    const TVector<TSerializedMessage>& GetBindedMsgs(ui64 tabletId);

    bool HasWriteOperations = false;
    size_t PredicateAcksCount = 0;
};

}
