#include "transaction.h"

namespace NKikimr::NPQ {

void TDistributedTransaction::OnProposeTransaction(const NKikimrPQ::TEvProposeTransaction& event,
                                                   ui64 minStep)
{
    Y_VERIFY(event.GetTxBodyCase() != NKikimrPQ::TEvProposeTransaction::TXBODY_NOT_SET);
    Y_VERIFY(TxId == Max<ui64>());

    TxId = event.GetTxId();

    MinStep = minStep;
    MaxStep = MinStep + TDuration::Seconds(30).MilliSeconds();

    switch (event.GetTxBodyCase()) {
    case NKikimrPQ::TEvProposeTransaction::kData:
        Y_VERIFY(event.HasData());
        OnProposeTransaction(event.GetData());
        break;
    case NKikimrPQ::TEvProposeTransaction::kConfig:
        Y_VERIFY(event.HasConfig());
        OnProposeTransaction(event.GetConfig());
        break;
    case NKikimrPQ::TEvProposeTransaction::TXBODY_NOT_SET:
        break;
    }

    Source = ActorIdFromProto(event.GetSource());
}

void TDistributedTransaction::OnProposeTransaction(const NKikimrPQ::TDataTransaction& txBody)
{
    Kind = NKikimrPQ::TTransaction::KIND_DATA;

    for (ui64 tablet : txBody.GetSendingShards()) {
        Senders.insert(tablet);
    }

    for (ui64 tablet : txBody.GetReceivingShards()) {
        Receivers.insert(tablet);
    }

    for (auto& operation : txBody.GetOperations()) {
        Operations.push_back(operation);
        Partitions.insert(operation.GetPartitionId());
    }

    PartitionRepliesCount = 0;
    PartitionRepliesExpected = 0;

    ReadSetCount = 0;
}

void TDistributedTransaction::OnProposeTransaction(const NKikimrPQ::TConfigTransaction& txBody)
{
    Kind = NKikimrPQ::TTransaction::KIND_CONFIG;

    TabletConfig = txBody.GetTabletConfig();
    BootstrapConfig = txBody.GetBootstrapConfig();

    if (TabletConfig.PartitionsSize()) {
        for (const auto& partition : TabletConfig.GetPartitions()) {
            Partitions.insert(partition.GetPartitionId());
        }
    } else {
        for (auto partitionId : TabletConfig.GetPartitionIds()) {
            Partitions.insert(partitionId);
        }
    }

    PartitionRepliesCount = 0;
    PartitionRepliesExpected = 0;

    ReadSetCount = 0;
}

void TDistributedTransaction::OnPlanStep(ui64 step)
{
    Y_VERIFY(Step == Max<ui64>());
    Y_VERIFY(TxId != Max<ui64>());

    Step = step;
}

void TDistributedTransaction::OnTxCalcPredicateResult(const TEvPQ::TEvTxCalcPredicateResult& event)
{
    Y_VERIFY(Step == event.Step);
    Y_VERIFY(TxId == event.TxId);

    Y_VERIFY(Partitions.contains(event.Partition));

    SetDecision(event.Predicate ? NKikimrTx::TReadSetData::DECISION_COMMIT : NKikimrTx::TReadSetData::DECISION_ABORT);

    ++PartitionRepliesCount;
}

void TDistributedTransaction::OnProposePartitionConfigResult(const TEvPQ::TEvProposePartitionConfigResult& event)
{
    Y_VERIFY(Step == event.Step);
    Y_VERIFY(TxId == event.TxId);

    SetDecision(NKikimrTx::TReadSetData::DECISION_COMMIT);

    ++PartitionRepliesCount;
}

void TDistributedTransaction::OnReadSet(const NKikimrTx::TEvReadSet& event,
                                        const TActorId& sender,
                                        std::unique_ptr<TEvTxProcessing::TEvReadSetAck> ack)
{
    Y_VERIFY(event.HasStep() && (Step == event.GetStep()));
    Y_VERIFY(event.HasTxId() && (TxId == event.GetTxId()));

    if (Senders.contains(event.GetTabletProducer())) {
        NKikimrTx::TReadSetData data;
        Y_VERIFY(event.HasReadSet() && data.ParseFromString(event.GetReadSet()));

        SetDecision(event.GetTabletProducer(), data.GetDecision());
        ReadSetAcks[sender] = std::move(ack);

        ++ReadSetCount;
    }
}

void TDistributedTransaction::OnReadSetAck(const NKikimrTx::TEvReadSetAck& event)
{
    Y_VERIFY(event.HasStep() && (Step == event.GetStep()));
    Y_VERIFY(event.HasTxId() && (TxId == event.GetTxId()));

    Receivers.erase(event.GetTabletConsumer());
}

void TDistributedTransaction::OnTxCommitDone(const TEvPQ::TEvTxCommitDone& event)
{
    Y_VERIFY(Step == event.Step);
    Y_VERIFY(TxId == event.TxId);

    Y_VERIFY(Partitions.contains(event.Partition));

    ++PartitionRepliesCount;
}

void TDistributedTransaction::SetDecision(NKikimrTx::TReadSetData::EDecision decision)
{
    SetDecision(SelfDecision, decision);
}

void TDistributedTransaction::SetDecision(ui64 tabletId, NKikimrTx::TReadSetData::EDecision decision)
{
    if (Senders.contains(tabletId)) {
        SetDecision(ParticipantsDecision, decision);
    }
}

auto TDistributedTransaction::GetDecision() const -> EDecision
{
    constexpr EDecision commit = NKikimrTx::TReadSetData::DECISION_COMMIT;
    constexpr EDecision abort = NKikimrTx::TReadSetData::DECISION_ABORT;
    constexpr EDecision unknown = NKikimrTx::TReadSetData::DECISION_UNKNOWN;

    EDecision aggrDecision = Senders.empty() ? commit : ParticipantsDecision;

    if ((SelfDecision == commit) && (aggrDecision == commit)) {
        return commit;
    }
    if ((SelfDecision == unknown) || (aggrDecision == unknown)) {
        return unknown;
    }

    return abort;
}

bool TDistributedTransaction::HaveParticipantsDecision() const
{
    return
        (Senders.size() == ReadSetCount) &&
        (ParticipantsDecision != NKikimrTx::TReadSetData::DECISION_UNKNOWN) ||
        Senders.empty();
}

bool TDistributedTransaction::HaveAllRecipientsReceive() const
{
    return Receivers.empty();
}

void TDistributedTransaction::AddCmdWrite(NKikimrClient::TKeyValueRequest& request,
                                          EState state)
{
    NKikimrPQ::TTransaction tx;

    tx.SetKind(Kind);
    if (Step != Max<ui64>()) {
        tx.SetStep(Step);
    }
    tx.SetTxId(TxId);
    tx.SetState(state);
    tx.SetMinStep(MinStep);
    tx.SetMaxStep(MaxStep);

    switch (Kind) {
    case NKikimrPQ::TTransaction::KIND_DATA:
        AddCmdWriteDataTx(tx);
        break;
    case NKikimrPQ::TTransaction::KIND_CONFIG:
        AddCmdWriteConfigTx(tx);
        break;
    case NKikimrPQ::TTransaction::KIND_UNKNOWN:
        break;
    }

    TString value;
    Y_VERIFY(tx.SerializeToString(&value));

    auto command = request.AddCmdWrite();
    command->SetKey(GetKey());
    command->SetValue(value);
}

void TDistributedTransaction::AddCmdWriteDataTx(NKikimrPQ::TTransaction& tx)
{
    for (ui64 tabletId : Senders) {
        tx.AddSenders(tabletId);
    }
    for (ui64 tabletId : Receivers) {
        tx.AddReceivers(tabletId);
    }
    tx.MutableOperations()->Add(Operations.begin(), Operations.end());
    if (SelfDecision != NKikimrTx::TReadSetData::DECISION_UNKNOWN) {
        tx.SetSelfPredicate(SelfDecision == NKikimrTx::TReadSetData::DECISION_COMMIT);
    }
    if (ParticipantsDecision != NKikimrTx::TReadSetData::DECISION_UNKNOWN) {
        tx.SetAggrPredicate(ParticipantsDecision == NKikimrTx::TReadSetData::DECISION_COMMIT);
    }
}

void TDistributedTransaction::AddCmdWriteConfigTx(NKikimrPQ::TTransaction& tx)
{
    *tx.MutableTabletConfig() = TabletConfig;
    *tx.MutableBootstrapConfig() = BootstrapConfig;
}

void TDistributedTransaction::AddCmdDelete(NKikimrClient::TKeyValueRequest& request)
{
    TString key = GetKey();
    auto range = request.AddCmdDeleteRange()->MutableRange();
    range->SetFrom(key);
    range->SetIncludeFrom(true);
    range->SetTo(key);
    range->SetIncludeTo(true);
}

void TDistributedTransaction::SetDecision(NKikimrTx::TReadSetData::EDecision& var, NKikimrTx::TReadSetData::EDecision value)
{
    if ((var == NKikimrTx::TReadSetData::DECISION_UNKNOWN) || (value == NKikimrTx::TReadSetData::DECISION_ABORT)) {
        var = value;
    }
}

TString TDistributedTransaction::GetKey() const
{
    return Sprintf("tx_%lu", TxId);
}

}
