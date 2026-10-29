#include "pqrb_ut_common.h"

#include <ydb/core/persqueue/pqrb/partition_scale_manager.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

namespace NKikimr::NPQ {
namespace {

void SendFinished(TTestContext& tc, ui32 partitionId, ui64 cookie) {
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge,
        new TEvPQ::TEvPartitionReplicationFinished(partitionId), 0, GetPipeConfigWithRetries(), TActorId(), cookie);
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

Y_UNIT_TEST(RequestReplicationDoneOnlyAfterAllPartitionsFinish) {
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
    DispatchFor(tc);
    checkProgress();
    UNIT_ASSERT(proposals.empty());

    SendFinished(tc, 1, 2);
    DispatchFor(tc);
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
    UNIT_ASSERT(config.GetReplicationStatus() == NKikimrPQ::ReplicationDone);
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
    DispatchFor(tc);
    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 3);
    UNIT_ASSERT(proposals.back()->Get<TEvTxUserProxy::TEvProposeTransaction>()->Record
        .GetTransaction().GetModifyScheme().GetApplyIf(0).GetPathVersion() > firstVersion);

    // SchemeShard's config update is the authoritative completion of the transition.
    update.ReplicationStatus = NKikimrPQ::ReplicationDone;
    SendBalancerUpdate(tc, update);
    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 3);
    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
}

Y_UNIT_TEST(CombinedCompletionWhenSplitMergeDisabled) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}, {1, {tc.TabletId, 2}}},
        .Strategy = NKikimrPQ::TPQTabletConfig::DISABLED,
        .ParentPartitionIds = {{1, {0}}},
        .ChildPartitionIds = {{0, {1}}},
        .ReplicationStatus = NKikimrPQ::ReplicationData,
        .DatabasePath = "/Root",
    });
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
    SendFinished(tc, 1, 1);
    tc.Runtime->SimulateSleep(TDuration::MilliSeconds(200));
    UNIT_ASSERT(proposals.empty());

    auto event = MakeHolder<TEvPQ::TEvPartitionReplicationFinished>(0);
    event->Record.MutableParticipatingPartitions()->AddChildPartitionIds(1);
    ForwardToTablet(*tc.Runtime, tc.BalancerTabletId, tc.Edge, event.Release());
    tc.Runtime->SimulateSleep(TDuration::MilliSeconds(200));
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 1);
    const auto& alter = proposals.front()->Get<TEvTxUserProxy::TEvProposeTransaction>()->Record
        .GetTransaction().GetModifyScheme().GetAlterPersQueueGroup();
    UNIT_ASSERT_VALUES_EQUAL(alter.SplitSize(), 0);
    UNIT_ASSERT(alter.GetPQTabletConfig().GetReplicationStatus() == NKikimrPQ::ReplicationDone);
    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
}

Y_UNIT_TEST(CombinedCompletionWaitsForSplitAndChildren) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    PQTabletPrepare({}, {}, tc);
    TBalancerUpdate update{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
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
    auto event = MakeHolder<TEvPQ::TEvPartitionReplicationFinished>(0);
    event->Record.MutableParticipatingPartitions()->AddChildPartitionIds(1);
    event->Record.MutableParticipatingPartitions()->AddChildPartitionIds(2);
    ForwardToTablet(*tc.Runtime, tc.BalancerTabletId, tc.Edge, event.Release());
    checkProgress();
    UNIT_ASSERT(proposals.empty()); // The split is queued until the database is known.
    NotifyDatabasePath(tc);
    checkProgress();
    // The parent is finished, but the only proposal must be the split.
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 1);
    const auto& split = proposals.front()->Get<TEvTxUserProxy::TEvProposeTransaction>()->Record
        .GetTransaction().GetModifyScheme().GetAlterPersQueueGroup();
    UNIT_ASSERT_VALUES_EQUAL(split.SplitSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(split.GetSplit(0).ChildPartitionIdsSize(), 2);
    UNIT_ASSERT(!split.GetPQTabletConfig().HasReplicationStatus());

    // The new config may arrive before the scale request reports completion.
    update.Partitions.push_back({1, {tc.TabletId, 2}});
    update.Partitions.push_back({2, {tc.TabletId, 3}});
    update.ChildPartitionIds[0] = {1, 2};
    update.ParentPartitionIds[1] = {0};
    update.ParentPartitionIds[2] = {0};
    SendBalancerUpdate(tc, update);
    SendFinished(tc, 1, 2);
    SendFinished(tc, 2, 3);
    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 1);

    ForwardToTablet(*tc.Runtime, tc.BalancerTabletId, tc.Edge,
        new TPartitionScaleRequest::TEvPartitionScaleRequestDone(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete));
    checkProgress();
    UNIT_ASSERT_VALUES_EQUAL(proposals.size(), 2);
    UNIT_ASSERT(proposals.back()->Get<TEvTxUserProxy::TEvProposeTransaction>()->Record.GetTransaction()
        .GetModifyScheme().GetAlterPersQueueGroup().GetPQTabletConfig().GetReplicationStatus() == NKikimrPQ::ReplicationDone);
    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
}

Y_UNIT_TEST(NotificationWithoutPersist) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{.Partitions = {{0, {tc.TabletId, 1}}, {1, {tc.TabletId, 2}}}});
    WaitBalancerReady(tc);

    TVector<THolder<IEventHandle>> pendingPuts;
    tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut
            && ev->Get<TEvBlobStorage::TEvPut>()->Id.TabletID() == tc.BalancerTabletId) {
            pendingPuts.emplace_back(ev.Release());
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    SendFinished(tc, 0, 42);
    SendFinished(tc, 1, 43);
    DispatchFor(tc);
    DispatchFor(tc);
    UNIT_ASSERT(pendingPuts.empty());
    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

    RestartBalancer(tc);
    SendFinished(tc, 0, 44);
    DispatchFor(tc);
}

Y_UNIT_TEST(NotificationDoesNotSendAck) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{.Partitions = {{0, {tc.TabletId, 1}}}});
    WaitBalancerReady(tc);

    SendFinished(tc, 0, 100);
    auto ack = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvPartitionReplicationFinishedAck>(TDuration::MilliSeconds(200));
    UNIT_ASSERT(!ack);
    RestartBalancer(tc);
    SendFinished(tc, 0, 101);
    ack = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvPartitionReplicationFinishedAck>(TDuration::MilliSeconds(200));
    UNIT_ASSERT(!ack);
}

} // Y_UNIT_TEST_SUITE(TPqrbReplication)
} // namespace NKikimr::NPQ
