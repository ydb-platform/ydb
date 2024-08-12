#include "transaction.h"
#include "utils.h"
#include "partition_log.h"

namespace NKikimr::NPQ {

TDistributedTransaction::TDistributedTransaction(const NKikimrPQ::TTransaction& tx) :
    TDistributedTransaction()
{
    Kind = tx.GetKind();
    if (tx.HasStep()) {
        Step = tx.GetStep();
    }
    TxId = tx.GetTxId();
    State = tx.GetState();
    MinStep = tx.GetMinStep();
    MaxStep = tx.GetMaxStep();

    ReadSetCount = 0;

    for (auto& p : tx.GetPredicatesReceived()) {
        PredicatesReceived[p.GetTabletId()] = p;

        if (p.HasPredicate()) {
            SetDecision(ParticipantsDecision,
                        p.GetPredicate() ? NKikimrTx::TReadSetData::DECISION_COMMIT : NKikimrTx::TReadSetData::DECISION_ABORT);
            ++ReadSetCount;
        }
    }

    PredicateAcksCount = 0;

    for (ui64 tabletId : tx.GetPredicateRecipients()) {
        PredicateRecipients[tabletId] = false;
    }

    if (tx.HasPredicate()) {
        SelfDecision =
            tx.GetPredicate() ? NKikimrTx::TReadSetData::DECISION_COMMIT : NKikimrTx::TReadSetData::DECISION_ABORT;
    } else {
        SelfDecision = NKikimrTx::TReadSetData::DECISION_UNKNOWN;
    }

    switch (Kind) {
    case NKikimrPQ::TTransaction::KIND_DATA:
        InitDataTransaction(tx);
        break;
    case NKikimrPQ::TTransaction::KIND_CONFIG:
        InitConfigTransaction(tx);
        break;
    case NKikimrPQ::TTransaction::KIND_UNKNOWN:
        Y_FAIL_S("unknown transaction type");
    }

    Y_ABORT_UNLESS(tx.HasSourceActor());
    SourceActor = ActorIdFromProto(tx.GetSourceActor());

    if (tx.HasWriteId()) {
        WriteId = GetWriteId(tx);
    }
}

TString TDistributedTransaction::LogPrefix() const
{
    return TStringBuilder() << "[TxId: " << TxId << "] ";
}

void TDistributedTransaction::InitDataTransaction(const NKikimrPQ::TTransaction& tx)
{
    InitPartitions(tx.GetOperations());
}

void TDistributedTransaction::InitPartitions(const google::protobuf::RepeatedPtrField<NKikimrPQ::TPartitionOperation>& operations)
{
    Partitions.clear();

    for (auto& o : operations) {
        if (!o.HasBegin()) {
            HasWriteOperations = true;
        }

        Operations.push_back(o);
        Partitions.insert(o.GetPartitionId());
    }
}

void TDistributedTransaction::InitConfigTransaction(const NKikimrPQ::TTransaction& tx)
{
    TabletConfig = tx.GetTabletConfig();
    BootstrapConfig = tx.GetBootstrapConfig();

    Migrate(TabletConfig);

    InitPartitions();
}

void TDistributedTransaction::InitPartitions()
{
    Partitions.clear();

    for (const auto& partition : TabletConfig.GetPartitions()) {
        Partitions.emplace(partition.GetPartitionId());
    }
}

void TDistributedTransaction::OnProposeTransaction(const NKikimrPQ::TEvProposeTransaction& event,
                                                   ui64 minStep,
                                                   ui64 extractTabletId)
{
    Y_ABORT_UNLESS(event.GetTxBodyCase() != NKikimrPQ::TEvProposeTransaction::TXBODY_NOT_SET);
    Y_ABORT_UNLESS(TxId == Max<ui64>());

    TxId = event.GetTxId();

    MinStep = minStep;

    switch (event.GetTxBodyCase()) {
    case NKikimrPQ::TEvProposeTransaction::kData:
        Y_ABORT_UNLESS(event.HasData());
        MaxStep = MinStep + TDuration::Seconds(30).MilliSeconds();
        OnProposeTransaction(event.GetData(), extractTabletId);
        break;
    case NKikimrPQ::TEvProposeTransaction::kConfig:
        Y_ABORT_UNLESS(event.HasConfig());
        MaxStep = Max<ui64>();
        OnProposeTransaction(event.GetConfig(), extractTabletId);
        break;
    default:
        Y_FAIL_S("unknown TxBody case");
    }

    PartitionRepliesCount = 0;
    PartitionRepliesExpected = 0;

    ReadSetCount = 0;

    Y_ABORT_UNLESS(event.HasSourceActor());
    SourceActor = ActorIdFromProto(event.GetSourceActor());
}

void TDistributedTransaction::OnProposeTransaction(const NKikimrPQ::TDataTransaction& txBody,
                                                   ui64 extractTabletId)
{
    Kind = NKikimrPQ::TTransaction::KIND_DATA;

    for (ui64 tabletId : txBody.GetSendingShards()) {
        if (tabletId != extractTabletId) {
            PredicatesReceived[tabletId].SetTabletId(tabletId);
        }
    }

    for (ui64 tabletId : txBody.GetReceivingShards()) {
        if (tabletId != extractTabletId) {
            PredicateRecipients[tabletId] = false;
        }
    }

    InitPartitions(txBody.GetOperations());

    if (txBody.HasWriteId() && HasWriteOperations) {
        WriteId = GetWriteId(txBody);
    } else {
        WriteId = Nothing();
    }
}

void TDistributedTransaction::OnProposeTransaction(const NKikimrPQ::TConfigTransaction& txBody,
                                                   ui64 extractTabletId)
{
    Kind = NKikimrPQ::TTransaction::KIND_CONFIG;

    TabletConfig = txBody.GetTabletConfig();
    BootstrapConfig = txBody.GetBootstrapConfig();

    Migrate(TabletConfig);

    TPartitionGraph graph = MakePartitionGraph(TabletConfig);

    for (const auto& p : TabletConfig.GetPartitions()) {
        auto node = graph.GetPartition(p.GetPartitionId());
        if (!node) {
            // Old configuration format without AllPartitions. Split/Merge is not supported.
            continue;
        }

        if (node->Children.empty()) {
            for (const auto* r : node->Parents) {
                if (extractTabletId != r->TabletId) {
                    PredicatesReceived[r->TabletId].SetTabletId(r->TabletId);
                }
            }
        }

        for (const auto* r : node->Children) {
            if (r->Children.empty()) {
                if (extractTabletId != r->TabletId) {
                    PredicateRecipients[r->TabletId] = false;
                }
            }
        }
    }

    InitPartitions();
}

void TDistributedTransaction::OnPlanStep(ui64 step)
{
    Y_ABORT_UNLESS(Step == Max<ui64>());
    Y_ABORT_UNLESS(TxId != Max<ui64>());

    Step = step;
}

void TDistributedTransaction::OnTxCalcPredicateResult(const TEvPQ::TEvTxCalcPredicateResult& event)
{
    PQ_LOG_D("Handle TEvTxCalcPredicateResult");

    OnPartitionResult(event,
                      event.Predicate ? NKikimrTx::TReadSetData::DECISION_COMMIT : NKikimrTx::TReadSetData::DECISION_ABORT);
}

void TDistributedTransaction::OnProposePartitionConfigResult(const TEvPQ::TEvProposePartitionConfigResult& event)
{
    PQ_LOG_D("Handle TEvProposePartitionConfigResult");

    OnPartitionResult(event,
                      NKikimrTx::TReadSetData::DECISION_COMMIT);
}

template<class E>
void TDistributedTransaction::OnPartitionResult(const E& event, EDecision decision)
{
    Y_ABORT_UNLESS(Step == event.Step);
    Y_ABORT_UNLESS(TxId == event.TxId);

    Y_ABORT_UNLESS(Partitions.contains(event.Partition.OriginalPartitionId));

    SetDecision(SelfDecision, decision);

    ++PartitionRepliesCount;

    PQ_LOG_D("Partition responses " << PartitionRepliesCount << "/" << PartitionRepliesExpected);
}

void TDistributedTransaction::OnReadSet(const NKikimrTx::TEvReadSet& event,
                                        const TActorId& sender,
                                        std::unique_ptr<TEvTxProcessing::TEvReadSetAck> ack)
{
    PQ_LOG_D("Handle TEvReadSet");

    Y_ABORT_UNLESS((Step == Max<ui64>()) || (event.HasStep() && (Step == event.GetStep())));
    Y_ABORT_UNLESS(event.HasTxId() && (TxId == event.GetTxId()));

    if (PredicatesReceived.contains(event.GetTabletProducer())) {
        NKikimrTx::TReadSetData data;
        Y_ABORT_UNLESS(event.HasReadSet() && data.ParseFromString(event.GetReadSet()));

        SetDecision(ParticipantsDecision, data.GetDecision());
        ReadSetAcks[sender] = std::move(ack);

        auto& p = PredicatesReceived[event.GetTabletProducer()];
        if (!p.HasPredicate()) {
            p.SetPredicate(data.GetDecision() == NKikimrTx::TReadSetData::DECISION_COMMIT);
            ++ReadSetCount;

            PQ_LOG_D("Predicates " << ReadSetCount << "/" << PredicatesReceived.size());
        }
    } else {
        Y_DEBUG_ABORT("unknown sender tablet %" PRIu64, event.GetTabletProducer());
    }
}

void TDistributedTransaction::OnReadSetAck(const NKikimrTx::TEvReadSetAck& event)
{
    PQ_LOG_D("Handle TEvReadSetAck");

    Y_ABORT_UNLESS(event.HasStep() && (Step == event.GetStep()));
    Y_ABORT_UNLESS(event.HasTxId() && (TxId == event.GetTxId()));

    if (PredicateRecipients.contains(event.GetTabletConsumer())) {
        PredicateRecipients[event.GetTabletConsumer()] = true;
        ++PredicateAcksCount;

        PQ_LOG_D("Predicate acks " << PredicateAcksCount << "/" << PredicateRecipients.size());
    }
}

void TDistributedTransaction::OnTxCommitDone(const TEvPQ::TEvTxCommitDone& event)
{
    Y_ABORT_UNLESS(Step == event.Step);
    Y_ABORT_UNLESS(TxId == event.TxId);

    Y_ABORT_UNLESS(Partitions.contains(event.Partition.OriginalPartitionId));

    ++PartitionRepliesCount;
}

auto TDistributedTransaction::GetDecision() const -> EDecision
{
    constexpr EDecision commit = NKikimrTx::TReadSetData::DECISION_COMMIT;
    constexpr EDecision abort = NKikimrTx::TReadSetData::DECISION_ABORT;
    constexpr EDecision unknown = NKikimrTx::TReadSetData::DECISION_UNKNOWN;

    const EDecision aggrDecision = PredicatesReceived.empty() ? commit : ParticipantsDecision;

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
        (PredicatesReceived.size() == ReadSetCount) &&
        (ParticipantsDecision != NKikimrTx::TReadSetData::DECISION_UNKNOWN) ||
        PredicatesReceived.empty();
}

bool TDistributedTransaction::HaveAllRecipientsReceive() const
{
    PQ_LOG_D("PredicateAcks: " << PredicateAcksCount << "/" << PredicateRecipients.size());
    return PredicateRecipients.size() == PredicateAcksCount;
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
        Y_FAIL_S("unknown transaction type");
    }

    tx.MutableOperations()->Add(Operations.begin(), Operations.end());
    if (SelfDecision != NKikimrTx::TReadSetData::DECISION_UNKNOWN) {
        tx.SetPredicate(SelfDecision == NKikimrTx::TReadSetData::DECISION_COMMIT);
    }

    for (auto& [_, predicate] : PredicatesReceived) {
        *tx.AddPredicatesReceived() = predicate;
    }
    for (auto& [tabletId, _] : PredicateRecipients) {
        tx.AddPredicateRecipients(tabletId);
    }

    Y_ABORT_UNLESS(SourceActor != TActorId());
    ActorIdToProto(SourceActor, tx.MutableSourceActor());

    PQ_LOG_D("save tx " << tx.ShortDebugString());

    TString value;
    Y_ABORT_UNLESS(tx.SerializeToString(&value));

    auto command = request.AddCmdWrite();
    command->SetKey(GetKey());
    command->SetValue(value);
}

void TDistributedTransaction::AddCmdWriteDataTx(NKikimrPQ::TTransaction& tx)
{
    if (WriteId.Defined()) {
        SetWriteId(tx, *WriteId);
    }
}

void TDistributedTransaction::AddCmdWriteConfigTx(NKikimrPQ::TTransaction& tx)
{
    *tx.MutableTabletConfig() = TabletConfig;
    *tx.MutableBootstrapConfig() = BootstrapConfig;
}

//void TDistributedTransaction::AddCmdDelete(NKikimrClient::TKeyValueRequest& request)
//{
//    TString key = GetKey();
//    auto range = request.AddCmdDeleteRange()->MutableRange();
//    range->SetFrom(key);
//    range->SetIncludeFrom(true);
//    range->SetTo(key);
//    range->SetIncludeTo(true);
//
//    PQ_LOG_D("add CmdDeleteRange for key " << key);
//}

void TDistributedTransaction::SetDecision(NKikimrTx::TReadSetData::EDecision& var, NKikimrTx::TReadSetData::EDecision value)
{
    if ((var == NKikimrTx::TReadSetData::DECISION_UNKNOWN) || (value == NKikimrTx::TReadSetData::DECISION_ABORT)) {
        var = value;
    }
}

TString TDistributedTransaction::GetKey() const
{
    return GetTxKey(TxId);
}

void TDistributedTransaction::BindMsgToPipe(ui64 tabletId, const IEventBase& event)
{
    Y_ABORT_UNLESS(event.IsSerializable());

    TAllocChunkSerializer serializer;
    Y_ABORT_UNLESS(event.SerializeToArcadiaStream(&serializer));
    auto data = serializer.Release(event.CreateSerializationInfo());
    OutputMsgs[tabletId].emplace_back(event.Type(), std::move(data));
}

void TDistributedTransaction::UnbindMsgsFromPipe(ui64 tabletId)
{
    OutputMsgs.erase(tabletId);
}

auto TDistributedTransaction::GetBindedMsgs(ui64 tabletId) -> const TVector<TSerializedMessage>&
{
    if (auto p = OutputMsgs.find(tabletId); p != OutputMsgs.end()) {
        return p->second;
    }

    static TVector<TSerializedMessage> empty;

    return empty;
}

}
