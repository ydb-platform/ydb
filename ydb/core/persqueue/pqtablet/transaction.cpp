#include "transaction.h"
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/persqueue/pqtablet/common/logging.h>

#include <ydb/library/wilson_ids/wilson.h>

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

    PartitionsData = std::move(tx.GetPartitions());

    if (tx.HasExecuteTraceId()) {
        NWilson::TTraceId traceId(tx.GetExecuteTraceId());
        ExecuteSpan = NWilson::TSpan(TWilsonTopic::TopicTopLevel,
                                     std::move(traceId),
                                     "Topic.Transaction",
                                     NWilson::EFlags::AUTO_END);
    }
    if (tx.HasWaitRSTraceId()) {
        NWilson::TTraceId traceId(tx.GetWaitRSTraceId());
        WaitRSSpan = NWilson::TSpan(TWilsonTopic::TopicTopLevel,
                                    std::move(traceId),
                                    "Topic.Transaction.WaitRS",
                                    NWilson::EFlags::AUTO_END);
    }
    if (tx.HasWaitRSAcksTraceId()) {
        NWilson::TTraceId traceId(tx.GetWaitRSAcksTraceId());
        WaitRSAcksSpan = NWilson::TSpan(TWilsonTopic::TopicTopLevel,
                                        std::move(traceId),
                                        "Topic.Transaction.WaitRSAcks",
                                        NWilson::EFlags::AUTO_END);
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
        if (!o.HasCommitOffsetsBegin()) {
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

        if (node->DirectChildren.empty()) {
            for (const auto* r : node->DirectParents) {
                if (extractTabletId != r->TabletId) {
                    PredicatesReceived[r->TabletId].SetTabletId(r->TabletId);
                }
            }
        }

        for (const auto* r : node->DirectChildren) {
            if (r->DirectChildren.empty()) {
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
    PQ_LOG_TX_D("Handle TEvTxCalcPredicateResult");

    TMaybe<EDecision> decision;

    if (event.Predicate.Defined()) {
        decision = *event.Predicate ? NKikimrTx::TReadSetData::DECISION_COMMIT : NKikimrTx::TReadSetData::DECISION_ABORT;
    }

    OnPartitionResult(event, decision);

    if (!event.IssueMsg.empty()) {
        NKikimrPQ::TError error;
        error.SetKind(NKikimrPQ::TError::BAD_REQUEST);
        error.SetReason(event.IssueMsg);

        Error = std::move(error);
    }
}

void UpdatePartitionsData(NKikimrPQ::TPartitions& partitionsData, NKikimrPQ::TPartitions::TPartitionInfo& partition) {
    NKikimrPQ::TPartitions::TPartitionInfo* info = nullptr;
    for (auto& p : *partitionsData.MutablePartition()) {
        if (p.GetPartitionId() == partition.GetPartitionId()) {
            info = &p;
            break;
        }
    }
    if (!info) {
        info = partitionsData.AddPartition();
    }
    *info = std::move(partition);
}

void TDistributedTransaction::OnProposePartitionConfigResult(TEvPQ::TEvProposePartitionConfigResult& event)
{
    PQ_LOG_TX_D("Handle TEvProposePartitionConfigResult");

    UpdatePartitionsData(PartitionsData, event.Data);

    OnPartitionResult(event,
                      NKikimrTx::TReadSetData::DECISION_COMMIT);
}

template<class E>
void TDistributedTransaction::OnPartitionResult(const E& event, TMaybe<EDecision> decision)
{
    Y_ABORT_UNLESS(Step == event.Step);
    Y_ABORT_UNLESS(TxId == event.TxId);

    Y_ABORT_UNLESS(Partitions.contains(event.Partition.OriginalPartitionId));

    if (decision.Defined()) {
        SetDecision(SelfDecision, *decision);
    }

    ++PartitionRepliesCount;

    PQ_LOG_TX_D("Partition responses " << PartitionRepliesCount << "/" << PartitionRepliesExpected);
}

void TDistributedTransaction::OnReadSet(const NKikimrTx::TEvReadSet& event,
                                        const TActorId& sender,
                                        std::unique_ptr<TEvTxProcessing::TEvReadSetAck> ack)
{
    PQ_LOG_TX_D("Handle TEvReadSet " << TxId);

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

            PQ_LOG_TX_D("Predicates " << ReadSetCount << "/" << PredicatesReceived.size());
        }

        NKikimrPQ::TPartitions d;
        if (data.HasData()) {
            auto r = data.GetData().UnpackTo(&d);
            Y_ABORT_UNLESS(r, "Unexpected data");
        }

        for (auto& v : *d.MutablePartition()) {
            UpdatePartitionsData(PartitionsData, v);
        }
    } else {
        Y_DEBUG_ABORT("unknown sender tablet %" PRIu64, event.GetTabletProducer());
    }
}

void TDistributedTransaction::OnReadSetAck(const NKikimrTx::TEvReadSetAck& event)
{
    PQ_LOG_TX_D("Handle TEvReadSetAck txId " << TxId);

    Y_ABORT_UNLESS(event.HasStep() && (Step == event.GetStep()));
    Y_ABORT_UNLESS(event.HasTxId() && (TxId == event.GetTxId()));

    OnReadSetAck(event.GetTabletConsumer());
}

void TDistributedTransaction::OnReadSetAck(ui64 tabletId)
{
    if (PredicateRecipients.contains(tabletId)) {
        PredicateRecipients[tabletId] = true;
        ++PredicateAcksCount;

        PQ_LOG_TX_D("Predicate acks " << PredicateAcksCount << "/" << PredicateRecipients.size());
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
    PQ_LOG_TX_D("PredicateAcks: " << PredicateAcksCount << "/" << PredicateRecipients.size());
    return PredicateRecipients.size() == PredicateAcksCount;
}

void TDistributedTransaction::AddCmdWrite(NKikimrClient::TKeyValueRequest& request,
                                          EState state)
{
    auto tx = Serialize(state);
    PQ_LOG_TX_D("save tx " << tx.ShortDebugString());

    TString value;
    Y_ABORT_UNLESS(tx.SerializeToString(&value));

    auto command = request.AddCmdWrite();
    command->SetKey(GetKey());
    command->SetValue(value);
}

NKikimrPQ::TTransaction TDistributedTransaction::Serialize() {
    return Serialize(State);
}

NKikimrPQ::TTransaction TDistributedTransaction::Serialize(EState state) {
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

    *tx.MutablePartitions() = PartitionsData;

    if (ExecuteSpan) {
        ExecuteSpan.GetTraceId().Serialize(tx.MutableExecuteTraceId());
    }
    if (WaitRSSpan) {
        WaitRSSpan.GetTraceId().Serialize(tx.MutableWaitRSTraceId());
    }
    if (WaitRSAcksSpan) {
        WaitRSAcksSpan.GetTraceId().Serialize(tx.MutableWaitRSAcksTraceId());
    }

    return tx;
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

void TDistributedTransaction::BindMsgToPipe(ui64 tabletId, const TEvTxProcessing::TEvReadSet& event)
{
    OutputMsgs[tabletId].push_back(event.Record);
}

void TDistributedTransaction::UnbindMsgsFromPipe(ui64 tabletId)
{
    OutputMsgs.erase(tabletId);
}

const TVector<NKikimrTx::TEvReadSet>& TDistributedTransaction::GetBindedMsgs(ui64 tabletId)
{
    if (auto p = OutputMsgs.find(tabletId); p != OutputMsgs.end()) {
        return p->second;
    }

    static TVector<NKikimrTx::TEvReadSet> empty;

    return empty;
}

void TDistributedTransaction::SetExecuteSpan(NWilson::TSpan&& span)
{
    ExecuteSpan = std::move(span);
}

void TDistributedTransaction::EndExecuteSpan()
{
    if (ExecuteSpan) {
        ExecuteSpan.End();
        ExecuteSpan = {};
    }
}

NWilson::TSpan TDistributedTransaction::CreatePlanStepSpan(ui64 tabletId, ui64 step)
{
    auto span = CreateSpan("Topic.Transaction.Plan", tabletId);
    span.Attribute("PlanStep", static_cast<i64>(step));
    return span;
}

void TDistributedTransaction::BeginWaitRSSpan(ui64 tabletId)
{
    if (!WaitRSSpan) {
        WaitRSSpan = CreateSpan("Topic.Transaction.WaitRS", tabletId);
    }
}

void TDistributedTransaction::SetLastTabletSentByRS(ui64 tabletId)
{
    if (WaitRSSpan) {
        WaitRSSpan.Attribute("LastTabletId", static_cast<i64>(tabletId));
    }
}

void TDistributedTransaction::EndWaitRSSpan()
{
    if (WaitRSSpan) {
        WaitRSSpan.End();
        WaitRSSpan = {};
    }
}

void TDistributedTransaction::BeginWaitRSAcksSpan(ui64 tabletId)
{
    WaitRSAcksSpan = CreateSpan("Topic.Transaction.WaitRSAcks", tabletId);
}

void TDistributedTransaction::EndWaitRSAcksSpan()
{
    if (WaitRSAcksSpan) {
        WaitRSAcksSpan.End();
        WaitRSAcksSpan = {};
    }
}

void TDistributedTransaction::BeginPersistSpan(ui64 tabletId, EState state, const NWilson::TTraceId& traceId)
{
    PersistSpan = CreateSpan("Topic.Transaction.Persist", tabletId);
    PersistSpan.Attribute("State", NKikimrPQ::TTransaction_EState_Name(state));
    if (traceId) {
        PersistSpan.Link(traceId);
    }
}

void TDistributedTransaction::EndPersistSpan()
{
    if (PersistSpan) {
        PersistSpan.End();
        PersistSpan = {};
    }
}

void TDistributedTransaction::BeginDeleteSpan(ui64 tabletId, const NWilson::TTraceId& traceId)
{
    DeleteSpan = CreateSpan("Topic.Transaction.Delete", tabletId);
    if (traceId) {
        DeleteSpan.Link(traceId);
    }
}

void TDistributedTransaction::EndDeleteSpan()
{
    if (DeleteSpan) {
        DeleteSpan.End();
        DeleteSpan = {};
    }
}

NWilson::TSpan TDistributedTransaction::CreateSpan(const char* name, ui64 tabletId)
{
    if (!ExecuteSpan) {
        return {};
    }

    auto span = ExecuteSpan.CreateChild(TWilsonTopic::TopicTopLevel,
                                        name,
                                        NWilson::EFlags::AUTO_END);
    span.Attribute("TxId", static_cast<i64>(TxId));
    span.Attribute("TabletId", static_cast<i64>(tabletId));
    return span;
}

NWilson::TTraceId TDistributedTransaction::GetExecuteSpanTraceId()
{
    return ExecuteSpan.GetTraceId();
}

}
