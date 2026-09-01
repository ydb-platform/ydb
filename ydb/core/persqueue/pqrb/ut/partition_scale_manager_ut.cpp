#include "pqrb_ut_common.h"

#include <ydb/core/persqueue/pqrb/partition_scale_manager.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <expected>

namespace NKikimr::NPQ {

namespace {

enum EEv {
    EvScaleStatus = EventSpaceBegin(TEvents::ES_PRIVATE),
    EvUpdateDb,
    EvUpdateConfig,
    EvAbort,
    EvDieMgr,
    EvTrySend,
    EvMirrorHandled,
    EvReady,
};

struct TEvScaleStatus : TEventLocal<TEvScaleStatus, EvScaleStatus> {
    ui32 PartitionId = 0;
    NKikimrPQ::EScaleStatus Status = NKikimrPQ::EScaleStatus::NEED_SPLIT;
    TMaybe<NKikimrPQ::TPartitionScaleParticipants> Participants;
    TMaybe<TString> SplitBoundary;
};

struct TEvUpdateDb : TEventLocal<TEvUpdateDb, EvUpdateDb> {
    TString Path;
    explicit TEvUpdateDb(TString path)
        : Path(std::move(path))
    {}
};

struct TEvUpdateConfig : TEventLocal<TEvUpdateConfig, EvUpdateConfig> {
    ui64 PathId = 1;
    int Version = 1;
    NKikimrPQ::TPQTabletConfig Config;
};

struct TEvAbort : TEventLocal<TEvAbort, EvAbort> {};
struct TEvDieMgr : TEventLocal<TEvDieMgr, EvDieMgr> {};
struct TEvTrySend : TEventLocal<TEvTrySend, EvTrySend> {};

struct TEvMirrorHandled : TEventLocal<TEvMirrorHandled, EvMirrorHandled> {
    std::expected<void, std::string> Result;
    explicit TEvMirrorHandled(std::expected<void, std::string> result)
        : Result(std::move(result))
    {}
};

struct TEvReady : TEventLocal<TEvReady, EvReady> {};

NKikimrPQ::TPQTabletConfig MakeScaleConfig(
    ui32 maxPartitions,
    ui32 curPartitions,
    bool mirror = false,
    const std::vector<std::pair<ui32, std::vector<ui32>>>& children = {}
) {
    NKikimrPQ::TPQTabletConfig config;
    auto* strategy = config.MutablePartitionStrategy();
    strategy->SetPartitionStrategyType(NKikimrPQ::TPQTabletConfig::CAN_SPLIT);
    strategy->SetMinPartitionCount(1);
    strategy->SetMaxPartitionCount(maxPartitions);
    for (ui32 i = 0; i < curPartitions; ++i) {
        auto* p = config.AddAllPartitions();
        p->SetPartitionId(i);
        p->SetStatus(NKikimrPQ::ETopicPartitionStatus::Active);
        p->MutableKeyRange()->SetFromBound(TString(1, 'a' + i));
        p->MutableKeyRange()->SetToBound(TString(1, 'b' + i));
    }
    for (const auto& [id, childIds] : children) {
        while (config.AllPartitionsSize() <= id) {
            auto* p = config.AddAllPartitions();
            p->SetPartitionId(config.AllPartitionsSize() - 1);
            p->SetStatus(NKikimrPQ::ETopicPartitionStatus::Active);
        }
        auto* p = config.MutableAllPartitions(id);
        for (ui32 child : childIds) {
            p->AddChildPartitionIds(child);
            while (config.AllPartitionsSize() <= child) {
                auto* c = config.AddAllPartitions();
                c->SetPartitionId(config.AllPartitionsSize() - 1);
                c->SetStatus(NKikimrPQ::ETopicPartitionStatus::Active);
            }
            config.MutableAllPartitions(child)->AddParentPartitionIds(id);
        }
    }
    if (mirror) {
        config.MutablePartitionConfig()->MutableMirrorFrom()->SetTopic("src");
        config.MutablePartitionConfig()->MutableMirrorFrom()->SetEndpoint("src");
        config.MutablePartitionConfig()->MutableMirrorFrom()->SetEndpointPort(2135);
    }
    return config;
}

NYdb::NTopic::TDescribeTopicResult MakeDescribeResult(ui32 rootCount, bool ok = true) {
    Ydb::Topic::DescribeTopicResult proto;
    for (ui32 i = 0; i < rootCount; ++i) {
        auto* p = proto.add_partitions();
        p->set_partition_id(i);
        p->set_active(true);
        p->mutable_key_range()->set_from_bound(TString(1, 'A' + i));
        p->mutable_key_range()->set_to_bound(TString(1, 'B' + i));
    }
    return NYdb::NTopic::TDescribeTopicResult(
        NYdb::TStatus(ok ? NYdb::EStatus::SUCCESS : NYdb::EStatus::UNAVAILABLE, NYdb::NIssue::TIssues()),
        std::move(proto)
    );
}

class TScaleManagerHost : public TActorBootstrapped<TScaleManagerHost> {
public:
    TScaleManagerHost(
        TActorId edge,
        TPartitionGraph graph,
        NKikimrPQ::TPQTabletConfig config,
        TString dbPath
    )
        : Edge(edge)
        , Graph(std::move(graph))
        , Config(std::move(config))
        , Manager("topic", "/Root/topic", dbPath, /*pathId=*/1, /*version=*/1, Config, Graph)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(Edge, new TEvReady());
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvScaleStatus, Handle);
        hFunc(TEvUpdateDb, Handle);
        hFunc(TEvUpdateConfig, Handle);
        hFunc(TEvAbort, Handle);
        hFunc(TEvDieMgr, Handle);
        hFunc(TEvTrySend, Handle);
        hFunc(TEvPQ::TEvMirrorTopicDescription, Handle);
        hFunc(TPartitionScaleRequest::TEvPartitionScaleRequestDone, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )

private:
    void Handle(TEvScaleStatus::TPtr& ev) {
        Manager.HandleScaleStatusChange(
            ev->Get()->PartitionId,
            ev->Get()->Status,
            ev->Get()->Participants,
            ev->Get()->SplitBoundary,
            ActorContext()
        );
    }

    void Handle(TEvUpdateDb::TPtr& ev) {
        Manager.UpdateDatabasePath(ev->Get()->Path, ActorContext());
    }

    void Handle(TEvUpdateConfig::TPtr& ev) {
        Manager.UpdateBalancerConfig(ev->Get()->PathId, ev->Get()->Version, ev->Get()->Config);
    }

    void Handle(TEvAbort::TPtr&) {
        Manager.AbortInflightScaleRequest(ActorContext());
    }

    void Handle(TEvDieMgr::TPtr&) {
        Manager.Die(ActorContext());
    }

    void Handle(TEvTrySend::TPtr&) {
        Manager.TrySendScaleRequest(ActorContext());
    }

    void Handle(TEvPQ::TEvMirrorTopicDescription::TPtr& ev) {
        auto result = Manager.HandleMirrorTopicDescriptionResult(ev, ActorContext());
        Send(Edge, new TEvMirrorHandled(std::move(result)));
    }

    void Handle(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev) {
        const auto status = ev->Get()->Status;
        Manager.HandleScaleRequestResult(ev, ActorContext());
        Send(Edge, new TPartitionScaleRequest::TEvPartitionScaleRequestDone(status));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == TPartitionScaleManager::TRY_SCALE_REQUEST_WAKE_UP_TAG) {
            Manager.TrySendScaleRequest(ActorContext());
        }
    }

    const TActorId Edge;
    TPartitionGraph Graph;
    NKikimrPQ::TPQTabletConfig Config;
    TPartitionScaleManager Manager;
};

TActorId StartHost(
    TTestContext& tc,
    const NKikimrPQ::TPQTabletConfig& config,
    const TString& dbPath = "/Root"
) {
    auto graph = MakePartitionGraph(config);
    auto host = tc.Runtime->Register(new TScaleManagerHost(tc.Edge, std::move(graph), config, dbPath));
    tc.Runtime->EnableScheduleForActor(host);
    tc.Runtime->SetRegistrationObserverFunc(
        [&](TTestActorRuntimeBase& runtime, const TActorId&, const TActorId& id) {
            runtime.EnableScheduleForActor(id);
        }
    );
    auto ready = tc.Runtime->GrabEdgeEvent<TEvReady>(TDuration::Seconds(10));
    UNIT_ASSERT(ready);
    return host;
}

const NKikimrSchemeOp::TPersQueueGroupDescription& GroupOf(TProposeCapture& captured) {
    UNIT_ASSERT(!captured.Records.empty());
    return captured.Records.front().GetTransaction().GetModifyScheme().GetAlterPersQueueGroup();
}

void SendScale(
    TTestContext& tc,
    const TActorId& host,
    ui32 partitionId,
    NKikimrPQ::EScaleStatus status = NKikimrPQ::EScaleStatus::NEED_SPLIT,
    TMaybe<TString> boundary = TString("m"),
    TMaybe<NKikimrPQ::TPartitionScaleParticipants> participants = Nothing()
) {
    auto ev = MakeHolder<TEvScaleStatus>();
    ev->PartitionId = partitionId;
    ev->Status = status;
    ev->SplitBoundary = std::move(boundary);
    ev->Participants = std::move(participants);
    tc.Runtime->Send(new IEventHandle(host, tc.Edge, ev.Release()));
    DispatchFor(tc);
}

} // namespace

Y_UNIT_TEST_SUITE(TPqrbPartitionScaleManager) {

Y_UNIT_TEST(EmptyDatabasePathDoesNotSendUntilUpdated) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1), /*dbPath=*/"");

    SendScale(tc, host, 0);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);

    tc.Runtime->Send(new IEventHandle(host, tc.Edge, new TEvUpdateDb("/Root")));
    WaitProposes(tc, blocked);
}

Y_UNIT_TEST(NormalStatusRemovesPendingSplit) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1), /*dbPath=*/"");

    SendScale(tc, host, 0);
    SendScale(tc, host, 0, NKikimrPQ::EScaleStatus::NORMAL);
    tc.Runtime->Send(new IEventHandle(host, tc.Edge, new TEvUpdateDb("/Root")));
    DispatchFor(tc, TDuration::MilliSeconds(200));
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(SplitWithoutBoundaryUsesMiddleOfRange) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1));
    SendScale(tc, host, 0, NKikimrPQ::EScaleStatus::NEED_SPLIT, Nothing());
    WaitProposes(tc, blocked);
    UNIT_ASSERT_VALUES_EQUAL(GroupOf(blocked).SplitSize(), 1u);
    UNIT_ASSERT(!GroupOf(blocked).GetSplit(0).GetSplitBoundary().empty());
}

Y_UNIT_TEST(EmptySplitBoundaryIsDropped) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1));
    SendScale(tc, host, 0, NKikimrPQ::EScaleStatus::NEED_SPLIT, TString());
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(MissingPartitionWithoutParticipantsIsDropped) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1));
    SendScale(tc, host, 99);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(MissingPartitionWithParticipantsStaysPending) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1));
    NKikimrPQ::TPartitionScaleParticipants participants;
    participants.AddChildPartitionIds(10);
    participants.AddChildPartitionIds(11);
    SendScale(tc, host, 5, NKikimrPQ::EScaleStatus::NEED_SPLIT, TString("m"), participants);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(AdjacentParticipantsAreRejected) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1));
    NKikimrPQ::TPartitionScaleParticipants participants;
    participants.AddAdjacentPartitionIds(1);
    SendScale(tc, host, 0, NKikimrPQ::EScaleStatus::NEED_SPLIT, TString("m"), participants);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(ExistingChildrenAreRemovedFromPending) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto config = MakeScaleConfig(10, 1, false, {{0, {1, 2}}});
    auto host = StartHost(tc, config);
    NKikimrPQ::TPartitionScaleParticipants participants;
    participants.AddChildPartitionIds(1);
    participants.AddChildPartitionIds(3);
    SendScale(tc, host, 0, NKikimrPQ::EScaleStatus::NEED_SPLIT, TString("m"), participants);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(UnorderedSplitWhenChildAlreadyExists) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto config = MakeScaleConfig(10, 3);
    auto host = StartHost(tc, config);
    NKikimrPQ::TPartitionScaleParticipants participants;
    participants.AddChildPartitionIds(1);
    participants.AddChildPartitionIds(2);
    SendScale(tc, host, 0, NKikimrPQ::EScaleStatus::NEED_SPLIT, TString("m"), participants);
    WaitProposes(tc, blocked);

    const auto& split = GroupOf(blocked).GetSplit(0);
    UNIT_ASSERT(split.GetCreateRootLevelSibling());
    UNIT_ASSERT_VALUES_EQUAL(split.ChildPartitionIdsSize(), 2u);
}

Y_UNIT_TEST(QuotaLimitsNumberOfSplits) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(2, 1));
    SendScale(tc, host, 0, NKikimrPQ::EScaleStatus::NEED_SPLIT, TString("m"));
    WaitProposes(tc, blocked);
    UNIT_ASSERT_VALUES_EQUAL(GroupOf(blocked).SplitSize(), 1u);
}

Y_UNIT_TEST(QuotaExhaustedDoesNotSendSplit) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(1, 1));
    SendScale(tc, host, 0, NKikimrPQ::EScaleStatus::NEED_SPLIT, TString("m"));
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(InflightRequestBlocksSecondSendUntilResult) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 2));
    SendScale(tc, host, 0);
    WaitProposes(tc, blocked);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 1u);
    SendScale(tc, host, 1);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 1u);
}

Y_UNIT_TEST(SuccessfulResultRetriesRemainingSplits) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 2));
    SendScale(tc, host, 0);
    SendScale(tc, host, 1);
    WaitProposes(tc, blocked);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 1u);

    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TPartitionScaleRequest::TEvPartitionScaleRequestDone(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
        )
    ));
    WaitProposes(tc, blocked, 2);
}

Y_UNIT_TEST(FailedResultSchedulesRetry) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1));
    SendScale(tc, host, 0);
    WaitProposes(tc, blocked);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 1u);

    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TPartitionScaleRequest::TEvPartitionScaleRequestDone(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError
        )
    ));
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(host, tc.Edge, new TEvTrySend()));
    DispatchFor(tc);
    UNIT_ASSERT_VALUES_EQUAL_C(blocked.Records.size(), 1u, "Retry must wait for backoff");

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(2));
    WaitProposes(tc, blocked, 2);
}

Y_UNIT_TEST(AbortInflightAllowsNewRequest) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1));
    SendScale(tc, host, 0);
    WaitProposes(tc, blocked);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 1u);

    tc.Runtime->Send(new IEventHandle(host, tc.Edge, new TEvAbort()));
    DispatchFor(tc);
    SendScale(tc, host, 0);
    WaitProposes(tc, blocked, 2);
}

Y_UNIT_TEST(DiePoisonsInflightRequest) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    auto host = StartHost(tc, MakeScaleConfig(10, 1));
    SendScale(tc, host, 0);
    tc.Runtime->Send(new IEventHandle(host, tc.Edge, new TEvDieMgr()));
    DispatchFor(tc);
}

Y_UNIT_TEST(MirroredSplitDisabledByFeatureFlag) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    tc.Runtime->GetAppData(0).FeatureFlags.SetEnableMirroredTopicSplitMerge(false);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1, true));
    SendScale(tc, host, 0);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(MirroredSplitWithoutParticipantsIsDropped) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    tc.Runtime->GetAppData(0).FeatureFlags.SetEnableMirroredTopicSplitMerge(true);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1, true));
    SendScale(tc, host, 0);
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(ReorderPrefersSmallerPrescribedChildren) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 2), /*dbPath=*/"");
    NKikimrPQ::TPartitionScaleParticipants late;
    late.AddChildPartitionIds(20);
    late.AddChildPartitionIds(21);
    NKikimrPQ::TPartitionScaleParticipants early;
    early.AddChildPartitionIds(5);
    early.AddChildPartitionIds(6);

    auto first = MakeHolder<TEvScaleStatus>();
    first->PartitionId = 0;
    first->SplitBoundary = TString("m");
    first->Participants = late;
    tc.Runtime->Send(new IEventHandle(host, tc.Edge, first.Release()));

    auto second = MakeHolder<TEvScaleStatus>();
    second->PartitionId = 1;
    second->SplitBoundary = TString("n");
    second->Participants = early;
    tc.Runtime->Send(new IEventHandle(host, tc.Edge, second.Release()));
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(host, tc.Edge, new TEvUpdateDb("/Root")));
    WaitProposes(tc, blocked);

    UNIT_ASSERT_VALUES_EQUAL(GroupOf(blocked).GetSplit(0).GetPartition(), 1u);
}

Y_UNIT_TEST(MirrorDescriptionCreatesRootBoundaries) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 1, true));
    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TEvPQ::TEvMirrorTopicDescription(MakeDescribeResult(3))
    ));
    auto handled = tc.Runtime->GrabEdgeEvent<TEvMirrorHandled>(TDuration::Seconds(10));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(handled->Result.has_value());
    WaitProposes(tc, blocked);

    const auto& group = GroupOf(blocked);
    UNIT_ASSERT_VALUES_EQUAL(group.RootPartitionBoundariesSize(), 3u);
    UNIT_ASSERT(!group.GetRootPartitionBoundaries(0).GetCreatePartition());
    UNIT_ASSERT(group.GetRootPartitionBoundaries(1).GetCreatePartition());
}

Y_UNIT_TEST(MirrorDescriptionQuotaTooLowSendsNothing) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(1, 1, true));
    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TEvPQ::TEvMirrorTopicDescription(MakeDescribeResult(3))
    ));
    auto handled = tc.Runtime->GrabEdgeEvent<TEvMirrorHandled>(TDuration::Seconds(10));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(handled->Result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(MirrorDescriptionMismatchReturnsError) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    auto host = StartHost(tc, MakeScaleConfig(10, 1, true));
    Ydb::Topic::DescribeTopicResult proto;
    auto* p = proto.add_partitions();
    p->set_partition_id(2);
    p->set_active(true);
    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TEvPQ::TEvMirrorTopicDescription(NYdb::NTopic::TDescribeTopicResult(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()),
            std::move(proto)
        ))
    ));
    auto handled = tc.Runtime->GrabEdgeEvent<TEvMirrorHandled>(TDuration::Seconds(10));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(!handled->Result.has_value());
}

Y_UNIT_TEST(InvalidMirrorDescriptionIsIgnored) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    auto host = StartHost(tc, MakeScaleConfig(10, 1, true));
    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TEvPQ::TEvMirrorTopicDescription(TString("nope"))
    ));
    auto handled = tc.Runtime->GrabEdgeEvent<TEvMirrorHandled>(TDuration::Seconds(10));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(handled->Result.has_value());

    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TEvPQ::TEvMirrorTopicDescription(MakeDescribeResult(1, false))
    ));
    handled = tc.Runtime->GrabEdgeEvent<TEvMirrorHandled>(TDuration::Seconds(10));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(handled->Result.has_value());
}

Y_UNIT_TEST(MatchingMirrorDescriptionDoesNotScale) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TProposeCapture blocked;
    InstallProposeCapture(tc, blocked);
    auto host = StartHost(tc, MakeScaleConfig(10, 2, true));
    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TEvPQ::TEvMirrorTopicDescription(MakeDescribeResult(2))
    ));
    auto handled = tc.Runtime->GrabEdgeEvent<TEvMirrorHandled>(TDuration::Seconds(10));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(handled->Result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(blocked.Records.size(), 0u);
}

Y_UNIT_TEST(NonMirroredTopicClearsMirrorInfo) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    auto host = StartHost(tc, MakeScaleConfig(10, 1, false));
    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TEvPQ::TEvMirrorTopicDescription(MakeDescribeResult(5))
    ));
    auto handled = tc.Runtime->GrabEdgeEvent<TEvMirrorHandled>(TDuration::Seconds(10));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(handled->Result.has_value());
}

Y_UNIT_TEST(UpdateConfigClearsMirrorInfoWhenMirroringDisabled) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    auto host = StartHost(tc, MakeScaleConfig(10, 1, true));
    tc.Runtime->Send(new IEventHandle(
        host,
        tc.Edge,
        new TEvPQ::TEvMirrorTopicDescription(MakeDescribeResult(1))
    ));
    Y_UNUSED(tc.Runtime->GrabEdgeEvent<TEvMirrorHandled>(TDuration::Seconds(10)));

    auto ev = MakeHolder<TEvUpdateConfig>();
    ev->Config = MakeScaleConfig(10, 1, false);
    tc.Runtime->Send(new IEventHandle(host, tc.Edge, ev.Release()));
    DispatchFor(tc);
}

} // Y_UNIT_TEST_SUITE(TPqrbPartitionScaleManager)

} // namespace NKikimr::NPQ
