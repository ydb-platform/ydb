#include "pqrb_ut_common.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace NKikimr::NPQ {
namespace {

bool ReadReplicationDataFinished(TTestContext& tc, ui32 partitionId) {
    const TString query = TStringBuilder()
        << "("
        << "(let key '('('Partition (Uint32 '" << partitionId << "))))"
        << "(let fields '('ReplicationDataFinished))"
        << "(return (AsList (SetResult 'Result (SelectRow 'Partitions key fields))))"
        << ")";
    const auto result = NSchemeShardUT_Private::LocalMiniKQL(*tc.Runtime, tc.BalancerTabletId, query);
    const auto& row = result.GetValue().GetStruct(0).GetOptional();
    UNIT_ASSERT_C(row.HasOptional(), result.DebugString());
    return row.GetOptional().GetStruct(0).GetOptional().GetBool();
}

void SendFinished(TTestContext& tc, ui32 partitionId, ui64 cookie) {
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge,
        new TEvPQ::TEvPartitionReplicationFinished(partitionId), 0, GetPipeConfigWithRetries(), TActorId(), cookie);
}

void ExpectAck(TTestContext& tc, ui32 partitionId, ui64 cookie) {
    auto ack = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvPartitionReplicationFinishedAck>(tc.Edge, TDuration::Seconds(10));
    UNIT_ASSERT(ack);
    UNIT_ASSERT_VALUES_EQUAL(ack->Get()->Record.GetPartitionId(), partitionId);
    UNIT_ASSERT_VALUES_EQUAL(ack->Cookie, cookie);
}

void RestartBalancer(TTestContext& tc) {
    ForwardToTablet(*tc.Runtime, tc.BalancerTabletId, tc.Edge, new TEvents::TEvPoisonPill());
    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvRestored, 2));
    tc.Runtime->DispatchEvents(options);
    WaitBalancerReady(tc);
}

} // namespace

Y_UNIT_TEST_SUITE(TPqrbReplication) {

Y_UNIT_TEST(RequestReplicationConfigurationOnlyAfterAllPartitionsFinish) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    PQTabletPrepare({}, {}, tc);
    TBalancerUpdate update{
        .Partitions = {{0, {tc.TabletId, 1}}, {1, {tc.TabletId, 2}}},
        .ReplicationStatus = NKikimrPQ::ReplicationData,
        .DatabasePath = "/Root",
    };
    SendBalancerUpdate(tc, update);
    WaitBalancerReady(tc);

    TProposeCapture proxy;
    InstallProposeCapture(tc, proxy);
    TVector<THolder<IEventHandle>> proposals;
    tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvTxUserProxy::TEvProposeTransaction::EventType) {
            proposals.emplace_back(ev.Release());
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });
    auto checkProgress = [&] {
        ForwardToTablet(*tc.Runtime, tc.BalancerTabletId, tc.Edge, new TEvents::TEvWakeup(12));
        tc.Runtime->SimulateSleep(TDuration::MilliSeconds(200));
    };

    checkProgress();
    UNIT_ASSERT(proposals.empty());
    SendFinished(tc, 0, 1);
    ExpectAck(tc, 0, 1);
    checkProgress();
    UNIT_ASSERT(proposals.empty());

    SendFinished(tc, 1, 2);
    ExpectAck(tc, 1, 2);
    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 1);
    const auto& proposal = proposals.front()->Get<TEvTxUserProxy::TEvProposeTransaction>()->Record;
    UNIT_ASSERT_VALUES_EQUAL(proposal.GetDatabaseName(), "/Root");
    const auto& modify = proposal.GetTransaction().GetModifyScheme();
    UNIT_ASSERT(modify.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup);
    UNIT_ASSERT_VALUES_EQUAL(modify.GetWorkingDir(), "/Root");
    UNIT_ASSERT(modify.GetInternal());
    UNIT_ASSERT_VALUES_EQUAL(modify.ApplyIfSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(modify.GetApplyIf(0).GetPathId(), 1);
    UNIT_ASSERT(modify.GetApplyIf(0).GetCheckEntityVersion());
    const auto& config = modify.GetAlterPersQueueGroup().GetPQTabletConfig();
    UNIT_ASSERT_VALUES_EQUAL(modify.GetAlterPersQueueGroup().GetName(), update.Topic);
    UNIT_ASSERT(config.GetReplicationStatus() == NKikimrPQ::ReplicationConfiguration);
    UNIT_ASSERT_VALUES_EQUAL(config.ConsumersSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(config.GetConsumers(0).GetName(), "client");
    const auto firstVersion = modify.GetApplyIf(0).GetPathVersion();

    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 1);

    // A rejected proposal is retried without changing the local config.
    tc.Runtime->Send(new IEventHandle(proposals.back()->Sender, tc.Edge,
        new TEvTxUserProxy::TEvProposeTransactionStatus(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError)));
    DispatchFor(tc);
    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 2);

    // A concurrent alter adds a partition, so the old result must not complete the move.
    update.Partitions.push_back({2, {tc.TabletId, 3}});
    SendBalancerUpdate(tc, update);
    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 2);
    SendFinished(tc, 2, 3);
    ExpectAck(tc, 2, 3);
    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 3);
    UNIT_ASSERT(proposals.back()->Get<TEvTxUserProxy::TEvProposeTransaction>()->Record
        .GetTransaction().GetModifyScheme().GetApplyIf(0).GetPathVersion() > firstVersion);

    // SchemeShard's config update is the authoritative completion of the transition.
    update.ReplicationStatus = NKikimrPQ::ReplicationConfiguration;
    SendBalancerUpdate(tc, update);
    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 3);
    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
}

Y_UNIT_TEST(ConfigurationOffsetsAckAndReplicationDoneRequest) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    PQTabletPrepare({}, {{"client", false}}, tc);
    TProposeCapture proxy;
    InstallProposeCapture(tc, proxy);
    TActorId replicator;
    TVector<THolder<IEventHandle>> proposals;
    size_t acks = 0;
    tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvPersQueue::TEvUpdateBalancerConfig::EventType) {
            auto& config = *ev->Get<TEvPersQueue::TEvUpdateBalancerConfig>()->Record.MutableTabletConfig();
            config.MutablePartitionConfig()->MutableMirrorFrom()->SetTopic("source");
        } else if (ev->GetTypeRewrite() == TEvPQ::TEvInitCredentials::EventType) {
            replicator = ev->Recipient;
            return TTestActorRuntimeBase::EEventAction::DROP;
        } else if (ev->GetTypeRewrite() == TEvPQ::TEvPartitionConsumerOffsetsAck::EventType) {
            UNIT_ASSERT(ev->Recipient == replicator);
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 17);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvPQ::TEvPartitionConsumerOffsetsAck>()->Record.GetPartitionId(), 0);
            ++acks;
            return TTestActorRuntimeBase::EEventAction::DROP;
        } else if (ev->GetTypeRewrite() == TEvTxUserProxy::TEvProposeTransaction::EventType) {
            proposals.emplace_back(ev.Release());
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });
    TBalancerUpdate update{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .ReplicationStatus = NKikimrPQ::ReplicationConfiguration,
        .DatabasePath = "/Root",
    };
    SendBalancerUpdate(tc, update);
    tc.Runtime->SimulateSleep(TDuration::MilliSeconds(100));
    UNIT_ASSERT(replicator);
    auto offsets = MakeHolder<TEvPQ::TEvPartitionConsumerOffsets>();
    offsets->Record.SetPartitionId(0);
    auto* consumer = offsets->Record.AddConsumerOffsets();
    consumer->SetConsumer("client");
    consumer->SetCommittedOffset(0);
    tc.Runtime->SendToPipe(tc.BalancerTabletId, replicator, offsets.Release(), 0, GetPipeConfigWithRetries(), TActorId(), 17);
    tc.Runtime->SimulateSleep(TDuration::MilliSeconds(200));
    UNIT_ASSERT_VALUES_EQUAL(acks, 1);
    UNIT_ASSERT(proposals.empty());
    auto complete = [&](const TActorId& sender) {
        ForwardToTablet(*tc.Runtime, tc.BalancerTabletId, sender, new TEvPQ::TEvTopicConfigurationReplicationFinished());
        tc.Runtime->SimulateSleep(TDuration::MilliSeconds(100));
    };
    complete(tc.Edge); // Unrelated sender cannot finish the move.
    UNIT_ASSERT(proposals.empty());
    complete(replicator);
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 1);
    const auto& modify = proposals.back()->Get<TEvTxUserProxy::TEvProposeTransaction>()->Record.GetTransaction().GetModifyScheme();
    UNIT_ASSERT(modify.GetAlterPersQueueGroup().GetPQTabletConfig().GetReplicationStatus() == NKikimrPQ::ReplicationDone);
    UNIT_ASSERT(modify.GetApplyIf(0).GetCheckEntityVersion());
    complete(replicator);
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 1);
    tc.Runtime->Send(new IEventHandle(proposals.back()->Sender, tc.Edge,
        new TEvTxUserProxy::TEvProposeTransactionStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError)));
    tc.Runtime->SimulateSleep(TDuration::MilliSeconds(100));
    complete(replicator);
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 2);
    const auto oldReplicator = replicator;
    SendBalancerUpdate(tc, update);
    tc.Runtime->SimulateSleep(TDuration::MilliSeconds(100));
    UNIT_ASSERT(replicator != oldReplicator);
    complete(oldReplicator);
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 2);
    complete(replicator);
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 3);
    update.ReplicationStatus = NKikimrPQ::ReplicationDone;
    SendBalancerUpdate(tc, update);
    complete(replicator);
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 3);
    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
}

Y_UNIT_TEST(PersistBeforeAckAndRestoreAfterRestart) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{.Partitions = {{0, {tc.TabletId, 1}}, {1, {tc.TabletId, 2}}}});
    WaitBalancerReady(tc);
    UNIT_ASSERT(!ReadReplicationDataFinished(tc, 0));
    UNIT_ASSERT(!ReadReplicationDataFinished(tc, 1));

    bool blockPuts = true;
    TVector<THolder<IEventHandle>> pendingPuts;
    tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (blockPuts && ev->GetTypeRewrite() == TEvBlobStorage::EvPut
            && ev->Get<TEvBlobStorage::TEvPut>()->Id.TabletID() == tc.BalancerTabletId) {
            pendingPuts.emplace_back(ev.Release());
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    SendFinished(tc, 0, 42);
    SendFinished(tc, 1, 43);
    auto earlyAck = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvPartitionReplicationFinishedAck>(TDuration::MilliSeconds(200));
    UNIT_ASSERT(!earlyAck);
    UNIT_ASSERT(!pendingPuts.empty());
    blockPuts = false;
    for (auto& put : pendingPuts) {
        tc.Runtime->Send(put.Release(), 0, true);
    }
    ExpectAck(tc, 0, 42);
    ExpectAck(tc, 1, 43);
    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

    RestartBalancer(tc);
    UNIT_ASSERT(ReadReplicationDataFinished(tc, 0));
    UNIT_ASSERT(ReadReplicationDataFinished(tc, 1));
    SendFinished(tc, 0, 44);
    ExpectAck(tc, 0, 44);
}

Y_UNIT_TEST(LostAckCanBeRetriedAfterRestart) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{.Partitions = {{0, {tc.TabletId, 1}}}});
    WaitBalancerReady(tc);

    bool droppedAck = false;
    tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvPQ::TEvPartitionReplicationFinishedAck::EventType && ev->Recipient == tc.Edge) {
            droppedAck = true;
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });
    SendFinished(tc, 0, 100);
    auto ack = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvPartitionReplicationFinishedAck>(TDuration::Seconds(1));
    UNIT_ASSERT(!ack);
    UNIT_ASSERT(droppedAck);
    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

    RestartBalancer(tc);
    UNIT_ASSERT(ReadReplicationDataFinished(tc, 0));
    SendFinished(tc, 0, 101);
    ExpectAck(tc, 0, 101);
}

} // Y_UNIT_TEST_SUITE(TPqrbReplication)
} // namespace NKikimr::NPQ
