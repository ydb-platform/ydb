#include "pqrb_ut_common.h"

#include <library/cpp/threading/future/future.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/pqrb/mirror_describer_factory.h>
#include <ydb/core/persqueue/pqrb/mirror_describer.h>
#include <ydb/core/persqueue/pqrb/partition_scale_request.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/events.h>
#include <util/generic/yexception.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

namespace NKikimr::NPQ {

namespace {

void DropScaleRequestNoise(TTestContext& tc) {
    const TActorId edge = tc.Edge;
    tc.Runtime->SetObserverFunc([edge](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvTxUserProxy::TEvProposeTransaction::EventType) {
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        if ((ev->GetTypeRewrite() == TEvTabletPipe::TEvClientConnected::EventType ||
             ev->GetTypeRewrite() == TEvTabletPipe::TEvClientDestroyed::EventType) &&
            ev->Sender != edge)
        {
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });
}

} // namespace

Y_UNIT_TEST_SUITE(TPqrbScaleRequest) {

Y_UNIT_TEST(PoisonPillStopsActorWithoutDone) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    DropScaleRequestNoise(tc);

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic",
        "/Root/topic",
        "/Root",
        /*pathId=*/1,
        /*pathVersion=*/1,
        {},
        {},
        {},
        tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, new TEvents::TEvPoisonPill()));
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(
        actorId,
        tc.Edge,
        new TEvTabletPipe::TEvClientDestroyed(/*tabletId=*/1, TActorId(1, 1, 1, 1), TActorId(1, 1, 2, 1))
    ));

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!done, "Poisoned scale request must not send TEvPartitionScaleRequestDone");
}

Y_UNIT_TEST(ForeignPipeDestroyedIsIgnored) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    DropScaleRequestNoise(tc);

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic",
        "/Root/topic",
        "/Root",
        /*pathId=*/1,
        /*pathVersion=*/1,
        {},
        {},
        {},
        tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(
        actorId,
        tc.Edge,
        new TEvTabletPipe::TEvClientDestroyed(/*tabletId=*/1, TActorId(1, 1, 1, 1), TActorId(1, 1, 2, 1))
    ));

    auto early = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!early, "ClientDestroyed for an unknown pipe must be ignored");

    auto status = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError
    );
    tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, status.Release()));

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(done);
}

Y_UNIT_TEST(ProposeStatusSendsDoneOnlyOnce) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    DropScaleRequestNoise(tc);

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic",
        "/Root/topic",
        "/Root",
        /*pathId=*/1,
        /*pathVersion=*/1,
        {},
        {},
        {},
        tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);
    DispatchFor(tc);

    for (ui32 i = 0; i < 2; ++i) {
        auto status = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError
        );
        tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, status.Release()));
    }

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(done);

    auto extra = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!extra, "ReplyAndDie must send TEvPartitionScaleRequestDone only once");
}

Y_UNIT_TEST(ScaleRequestInflightIsClearedWhenSplitMergeDisabled) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
        .MaxPartitionCount = 10,
    });
    NotifyDatabasePath(tc);

    ui32 scaleRequestActors = 0;
    TTestActorRuntime::TRegistrationObserver prevRegistration;
    prevRegistration = tc.Runtime->SetRegistrationObserverFunc(
        [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
            if (prevRegistration) {
                prevRegistration(runtime, parentId, actorId);
            }
            ++scaleRequestActors;
        }
    );

    auto needSplit = MakeHolder<TEvPQ::TEvPartitionScaleStatusChanged>(0, NKikimrPQ::EScaleStatus::NEED_SPLIT);
    needSplit->Record.SetSplitBoundary("m");
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        needSplit.Release(),
        0,
        GetPipeConfigWithRetries()
    );
    DispatchFor(tc, TDuration::MilliSeconds(200));
    UNIT_ASSERT_GT(scaleRequestActors, 0u);

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Strategy = NKikimrPQ::TPQTabletConfig::DISABLED,
    });

    ForwardToTablet(
        *tc.Runtime,
        tc.BalancerTabletId,
        tc.Edge,
        new TPartitionScaleRequest::TEvPartitionScaleRequestDone(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
        )
    );
    DispatchFor(tc);

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
        .MaxPartitionCount = 10,
    });
    NotifyDatabasePath(tc);
    const ui32 afterReenable = scaleRequestActors;

    auto needSplitAgain = MakeHolder<TEvPQ::TEvPartitionScaleStatusChanged>(0, NKikimrPQ::EScaleStatus::NEED_SPLIT);
    needSplitAgain->Record.SetSplitBoundary("m");
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        needSplitAgain.Release(),
        0,
        GetPipeConfigWithRetries()
    );
    DispatchFor(tc, TDuration::MilliSeconds(200));
    UNIT_ASSERT_GT_C(scaleRequestActors, afterReenable, "A new scale request must be sent after split/merge is re-enabled");
}

Y_UNIT_TEST(FillProposeRequestSerializesSplitsMergesAndBoundaries) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionSplit split;
    split.SetPartition(0);
    split.SetSplitBoundary("mid");

    NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionMerge merge;
    merge.SetPartition(1);
    merge.SetAdjacentPartition(2);

    NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionBoundary boundary;
    boundary.SetPartition(3);
    boundary.MutableKeyRange()->SetFromBound("aa");
    boundary.MutableKeyRange()->SetToBound("zz");
    boundary.SetCreatePartition(true);

    TProposeCapture captured;
    InstallProposeCapture(tc, captured);

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic",
        "/Root/topic",
        "/Root",
        /*pathId=*/7,
        /*pathVersion=*/0,
        {split},
        {merge},
        {boundary},
        tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);
    WaitProposes(tc, captured);

    const auto& record = captured.Records.front();
    UNIT_ASSERT_VALUES_EQUAL(record.GetDatabaseName(), "/Root");
    const auto& modify = record.GetTransaction().GetModifyScheme();
    UNIT_ASSERT(modify.GetInternal());
    UNIT_ASSERT_VALUES_EQUAL(modify.GetWorkingDir(), "/Root/");
    UNIT_ASSERT_VALUES_EQUAL(modify.GetApplyIf(0).GetPathId(), 7u);
    UNIT_ASSERT_VALUES_EQUAL(modify.GetApplyIf(0).GetPathVersion(), 1u);
    const auto& group = modify.GetAlterPersQueueGroup();
    UNIT_ASSERT_VALUES_EQUAL(group.GetName(), "topic");
    UNIT_ASSERT_VALUES_EQUAL(group.SplitSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(group.GetSplit(0).GetPartition(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(group.MergeSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(group.GetMerge(0).GetAdjacentPartition(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(group.RootPartitionBoundariesSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(group.GetRootPartitionBoundaries(0).GetPartition(), 3u);
    UNIT_ASSERT(group.GetRootPartitionBoundaries(0).GetCreatePartition());
}

Y_UNIT_TEST(DirectExecCompleteFromTxProxySendsDone) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    DropScaleRequestNoise(tc);

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic", "/Root/topic", "/Root", 1, 2, {}, {}, {}, tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);

    ui32 childActors = 0;
    tc.Runtime->SetRegistrationObserverFunc(
        [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& id) {
            runtime.EnableScheduleForActor(id);
            if (parentId == actorId) {
                ++childActors;
            }
        }
    );
    DispatchFor(tc);

    auto status = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
    );
    tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, status.Release()));

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(TDuration::Seconds(10));
    UNIT_ASSERT(done);
    UNIT_ASSERT_EQUAL(
        done->Status,
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
    );
    UNIT_ASSERT_VALUES_EQUAL_C(childActors, 0u, "Immediate ExecComplete must not open a SchemeShard pipe");
}

Y_UNIT_TEST(ExecInProgressPipeConnectErrorRepliesUnavailable) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    DropScaleRequestNoise(tc);

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic", "/Root/topic", "/Root", 1, 2, {}, {}, {}, tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);

    TActorId pipeActor;
    auto prev = tc.Runtime->SetRegistrationObserverFunc(
        [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& id) {
            runtime.EnableScheduleForActor(id);
            if (parentId == actorId) {
                pipeActor = id;
            }
        }
    );
    Y_UNUSED(prev);
    DispatchFor(tc);

    auto status = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress
    );
    status->Record.SetSchemeShardTabletId(999);
    status->Record.SetTxId(42);
    tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, status.Release()));
    DispatchFor(tc);
    UNIT_ASSERT(pipeActor);

    tc.Runtime->Send(new IEventHandle(
        actorId,
        tc.Edge,
        new TEvTabletPipe::TEvClientConnected(
            999, NKikimrProto::ERROR, pipeActor, TActorId(1, 2, 3, 4), true, false, 1
        )
    ));

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(TDuration::Seconds(10));
    UNIT_ASSERT(done);
    UNIT_ASSERT_EQUAL(
        done->Status,
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable
    );
}

Y_UNIT_TEST(ExecInProgressNotifyCompletionSendsExecComplete) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    DropScaleRequestNoise(tc);

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic", "/Root/topic", "/Root", 1, 2, {}, {}, {}, tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);

    TActorId pipeActor;
    tc.Runtime->SetRegistrationObserverFunc(
        [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& id) {
            runtime.EnableScheduleForActor(id);
            if (parentId == actorId) {
                pipeActor = id;
            }
        }
    );
    DispatchFor(tc);

    auto status = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress
    );
    status->Record.SetSchemeShardTabletId(999);
    status->Record.SetTxId(42);
    tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, status.Release()));
    DispatchFor(tc);
    UNIT_ASSERT(pipeActor);

    tc.Runtime->Send(new IEventHandle(
        actorId,
        tc.Edge,
        new TEvTabletPipe::TEvClientConnected(
            999, NKikimrProto::OK, pipeActor, TActorId(1, 2, 3, 4), true, false, 1
        )
    ));
    DispatchFor(tc);

    auto early = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::MilliSeconds(50)
    );
    UNIT_ASSERT_C(!early, "Successful pipe connect must wait for tx completion");

    tc.Runtime->Send(new IEventHandle(
        actorId,
        tc.Edge,
        new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult(42)
    ));

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(TDuration::Seconds(10));
    UNIT_ASSERT(done);
    UNIT_ASSERT_EQUAL(
        done->Status,
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
    );
}

Y_UNIT_TEST(OurPipeDestroyedRepliesUnavailable) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    DropScaleRequestNoise(tc);

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic", "/Root/topic", "/Root", 1, 2, {}, {}, {}, tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);

    TActorId pipeActor;
    tc.Runtime->SetRegistrationObserverFunc(
        [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& id) {
            runtime.EnableScheduleForActor(id);
            if (parentId == actorId) {
                pipeActor = id;
            }
        }
    );
    DispatchFor(tc);

    auto status = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress
    );
    status->Record.SetSchemeShardTabletId(999);
    status->Record.SetTxId(42);
    tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, status.Release()));
    DispatchFor(tc);
    UNIT_ASSERT(pipeActor);

    tc.Runtime->Send(new IEventHandle(
        actorId,
        tc.Edge,
        new TEvTabletPipe::TEvClientDestroyed(999, pipeActor, TActorId(1, 2, 3, 4))
    ));

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(TDuration::Seconds(10));
    UNIT_ASSERT(done);
    UNIT_ASSERT_EQUAL(
        done->Status,
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable
    );
}

Y_UNIT_TEST(ForeignPipeConnectedIsIgnored) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    DropScaleRequestNoise(tc);

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic", "/Root/topic", "/Root", 1, 2, {}, {}, {}, tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(
        actorId,
        tc.Edge,
        new TEvTabletPipe::TEvClientConnected(
            1, NKikimrProto::ERROR, TActorId(1, 1, 1, 1), TActorId(1, 1, 2, 1), true, false, 1
        )
    ));

    auto early = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!early, "ClientConnected for an unknown pipe must be ignored");

    auto err = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError
    );
    err->Record.AddIssues()->set_message("denied");
    tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, err.Release()));

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(TDuration::Seconds(10));
    UNIT_ASSERT(done);
}

} // Y_UNIT_TEST_SUITE(TPqrbScaleRequest)

class TControllableMirrorFactory : public IPersQueueMirrorReaderFactory {
public:
    mutable ui32 DescribeCalls = 0;
    mutable ui32 CredCalls = 0;
    bool ThrowCredentials = false;
    bool ImmediateCredentials = true;
    mutable std::vector<NThreading::TPromise<NYdb::NTopic::TDescribeTopicResult>> DescribePromises;
    mutable std::vector<NThreading::TPromise<NYdb::TCredentialsProviderFactoryPtr>> CredPromises;
    mutable std::vector<TString> CredTokens;
    mutable NYdb::TCredentialsProviderFactoryPtr LastDescribeCredentials;
    mutable TString LastDescribeTopic;

    NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr> GetCredentialsProviderImpl(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials& cred
    ) const override {
        CredTokens.push_back(TString{cred.GetOauthToken()});
        ++CredCalls;
        if (ThrowCredentials) {
            ythrow yexception() << "cred-fail";
        }
        if (ImmediateCredentials) {
            return NThreading::MakeFuture(NYdb::CreateInsecureCredentialsProviderFactory());
        }
        auto promise = NThreading::NewPromise<NYdb::TCredentialsProviderFactoryPtr>();
        CredPromises.push_back(promise);
        return promise.GetFuture();
    }

    std::shared_ptr<NYdb::NTopic::IReadSession> GetReadSession(
        const NKikimrPQ::TMirrorPartitionConfig&,
        ui32,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>,
        ui64,
        TMaybe<TLog>
    ) const override {
        return nullptr;
    }

    NThreading::TFuture<NYdb::NTopic::TDescribeTopicResult> GetTopicDescription(
        const NKikimrPQ::TMirrorPartitionConfig& config,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory
    ) const override {
        LastDescribeCredentials = credentialsProviderFactory;
        LastDescribeTopic = config.GetTopic();
        ++DescribeCalls;
        auto promise = NThreading::NewPromise<NYdb::NTopic::TDescribeTopicResult>();
        DescribePromises.push_back(promise);
        return promise.GetFuture();
    }

    NThreading::TFuture<NYdb::TStatus> CommitOffset(
        const NKikimrPQ::TMirrorPartitionConfig&,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>,
        ui32,
        ui64
    ) const override {
        return NThreading::MakeFuture(NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()));
    }
};

NKikimrPQ::TMirrorPartitionConfig MakeMirrorConfig(const TString& topic = "src-topic") {
    NKikimrPQ::TMirrorPartitionConfig config;
    config.SetEndpoint("src");
    config.SetEndpointPort(2135);
    config.SetTopic(topic);
    return config;
}

TActorId StartDescriber(TTestContext& tc, TControllableMirrorFactory& factory, const NKikimrPQ::TMirrorPartitionConfig& config) {
    tc.Runtime->GetAppData(0).PersQueueMirrorReaderFactory = &factory;
    auto describer = tc.Runtime->Register(CreateMirrorDescriber(1, tc.Edge, "topic", config));
    tc.Runtime->EnableScheduleForActor(describer);
    DispatchFor(tc);
    return describer;
}

void FireDescribeWakeup(TTestContext& tc, TDuration delay = TDuration::Seconds(1)) {
    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(delay);
    DispatchFor(tc);
}

Y_UNIT_TEST_SUITE(TPqrbMirrorDescriber) {

Y_UNIT_TEST(ConfigChangeDuringInflightDescribeDoesNotStuck) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    auto describer = StartDescriber(tc, factory, MakeMirrorConfig());
    FireDescribeWakeup(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 1u);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribePromises.size(), 1u);

    NKikimrPQ::TPQTabletConfig newConfig;
    newConfig.MutablePartitionConfig()->MutableMirrorFrom()->CopyFrom(MakeMirrorConfig("src-topic-2"));
    tc.Runtime->Send(new IEventHandle(
        describer,
        tc.Edge,
        new TEvPQ::TEvChangePartitionConfig(nullptr, newConfig)
    ));
    DispatchFor(tc);

    factory.DescribePromises[0].SetValue(NYdb::NTopic::TDescribeTopicResult(
        NYdb::TStatus(NYdb::EStatus::UNAVAILABLE, NYdb::NIssue::TIssues()),
        Ydb::Topic::DescribeTopicResult{}
    ));
    DispatchFor(tc);

    FireDescribeWakeup(tc);
    UNIT_ASSERT_GT_C(factory.DescribeCalls, 1u, "Mirror describer must start a new describe after config change");
}

Y_UNIT_TEST(ConfigChangeDuringInflightCredentialsIgnoresStaleReply) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    factory.ImmediateCredentials = false;

    auto oldMirror = MakeMirrorConfig("src-topic");
    oldMirror.MutableCredentials()->SetOauthToken("old-token");
    auto describer = StartDescriber(tc, factory, oldMirror);
    UNIT_ASSERT_VALUES_EQUAL(factory.CredCalls, 1u);
    UNIT_ASSERT_VALUES_EQUAL(factory.CredPromises.size(), 1u);

    NKikimrPQ::TPQTabletConfig newTabletConfig;
    auto newMirror = MakeMirrorConfig("src-topic-2");
    newMirror.MutableCredentials()->SetOauthToken("new-token");
    newTabletConfig.MutablePartitionConfig()->MutableMirrorFrom()->CopyFrom(newMirror);
    tc.Runtime->Send(new IEventHandle(
        describer,
        tc.Edge,
        new TEvPQ::TEvChangePartitionConfig(nullptr, newTabletConfig)
    ));
    DispatchFor(tc);

    UNIT_ASSERT_VALUES_EQUAL(factory.CredCalls, 2u);
    UNIT_ASSERT_VALUES_EQUAL(factory.CredPromises.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(factory.CredTokens[0], "old-token");
    UNIT_ASSERT_VALUES_EQUAL(factory.CredTokens[1], "new-token");

    auto oldCreds = NYdb::CreateInsecureCredentialsProviderFactory();
    auto newCreds = NYdb::CreateInsecureCredentialsProviderFactory();
    factory.CredPromises[0].SetValue(oldCreds);
    DispatchFor(tc);
    factory.CredPromises[1].SetValue(newCreds);
    DispatchFor(tc);

    FireDescribeWakeup(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 1u);
    UNIT_ASSERT_VALUES_EQUAL(factory.LastDescribeTopic, "src-topic-2");
    UNIT_ASSERT_C(
        factory.LastDescribeCredentials == newCreds,
        "Stale credentials from the previous MirrorFrom config must not be used after a config change"
    );
}

Y_UNIT_TEST(EqualConfigChangeIsIgnored) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    auto describer = StartDescriber(tc, factory, MakeMirrorConfig());
    FireDescribeWakeup(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 1u);

    NKikimrPQ::TPQTabletConfig sameConfig;
    sameConfig.MutablePartitionConfig()->MutableMirrorFrom()->CopyFrom(MakeMirrorConfig());
    tc.Runtime->Send(new IEventHandle(
        describer,
        tc.Edge,
        new TEvPQ::TEvChangePartitionConfig(nullptr, sameConfig)
    ));
    DispatchFor(tc);

    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 1u);
}

Y_UNIT_TEST(SuccessfulDescribeIsForwardedToTablet) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    StartDescriber(tc, factory, MakeMirrorConfig());
    FireDescribeWakeup(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribePromises.size(), 1u);

    Ydb::Topic::DescribeTopicResult proto;
    auto* part = proto.add_partitions();
    part->set_partition_id(0);
    part->set_active(true);
    factory.DescribePromises[0].SetValue(NYdb::NTopic::TDescribeTopicResult(
        NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()),
        std::move(proto)
    ));

    auto forwarded = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMirrorTopicDescription>(TDuration::Seconds(10));
    UNIT_ASSERT(forwarded);
    UNIT_ASSERT(forwarded->Description.has_value());
    UNIT_ASSERT(forwarded->Description->IsSuccess());
}

Y_UNIT_TEST(FailedDescribeRetriesAndSecondWakeupIsInflight) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    auto describer = StartDescriber(tc, factory, MakeMirrorConfig());
    FireDescribeWakeup(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 1u);

    factory.DescribePromises[0].SetValue(NYdb::NTopic::TDescribeTopicResult(
        NYdb::TStatus(NYdb::EStatus::UNAVAILABLE, NYdb::NIssue::TIssues()),
        Ydb::Topic::DescribeTopicResult{}
    ));
    DispatchFor(tc);

    FireDescribeWakeup(tc, TDuration::Seconds(3));
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 2u);

    tc.Runtime->Send(new IEventHandle(describer, tc.Edge, new TEvents::TEvWakeup()));
    DispatchFor(tc);
    UNIT_ASSERT_VALUES_EQUAL_C(factory.DescribeCalls, 2u, "Second wakeup must be ignored while describe is inflight");

    factory.DescribePromises[1].SetException(std::make_exception_ptr(std::runtime_error("describe-boom")));
    DispatchFor(tc);
}

Y_UNIT_TEST(SuccessfulDescribeReschedulesAfterMaxTimeout) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    StartDescriber(tc, factory, MakeMirrorConfig());
    FireDescribeWakeup(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribePromises.size(), 1u);

    Ydb::Topic::DescribeTopicResult proto;
    proto.add_partitions()->set_partition_id(0);
    factory.DescribePromises[0].SetValue(NYdb::NTopic::TDescribeTopicResult(
        NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()),
        std::move(proto)
    ));
    UNIT_ASSERT(tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMirrorTopicDescription>(TDuration::Seconds(10)));

    FireDescribeWakeup(tc, TDuration::Seconds(241));
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 2u);
}

Y_UNIT_TEST(StaleDescribeIsIgnored) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    auto describer = StartDescriber(tc, factory, MakeMirrorConfig());

    tc.Runtime->Send(new IEventHandle(
        describer,
        describer,
        new TEvPQ::TEvMirrorTopicDescription(TString("stale")),
        0,
        /*cookie=*/0
    ));
    DispatchFor(tc);

    FireDescribeWakeup(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 1u);
}

Y_UNIT_TEST(CredentialsErrorRetriesThenSucceeds) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    factory.ThrowCredentials = true;
    auto describer = StartDescriber(tc, factory, MakeMirrorConfig());
    UNIT_ASSERT_VALUES_EQUAL(factory.CredCalls, 1u);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 0u);

    factory.ThrowCredentials = false;
    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(1));
    DispatchFor(tc);
    UNIT_ASSERT_GT(factory.CredCalls, 1u);

    FireDescribeWakeup(tc);
    UNIT_ASSERT_GT(factory.DescribeCalls, 0u);
    Y_UNUSED(describer);
}

Y_UNIT_TEST(DuplicateInitCredentialsIsIgnoredWhileInflight) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    factory.ImmediateCredentials = false;
    auto describer = StartDescriber(tc, factory, MakeMirrorConfig());
    UNIT_ASSERT_VALUES_EQUAL(factory.CredCalls, 1u);

    tc.Runtime->Send(new IEventHandle(describer, tc.Edge, new TEvPQ::TEvInitCredentials()));
    DispatchFor(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.CredCalls, 1u);

    factory.CredPromises[0].SetValue(NYdb::CreateInsecureCredentialsProviderFactory());
    DispatchFor(tc);
    FireDescribeWakeup(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 1u);
}

Y_UNIT_TEST(UnknownEventsAndPoisonPill) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TControllableMirrorFactory factory;
    factory.ImmediateCredentials = false;
    auto describer = StartDescriber(tc, factory, MakeMirrorConfig());

    tc.Runtime->Send(new IEventHandle(describer, tc.Edge, new TEvents::TEvWakeup()));
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(describer, tc.Edge, new TEvents::TEvPoisonPill()));
    DispatchFor(tc);

    factory.CredPromises[0].SetValue(NYdb::CreateInsecureCredentialsProviderFactory());
    DispatchFor(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory.DescribeCalls, 0u);
}

} // Y_UNIT_TEST_SUITE(TPqrbMirrorDescriber)

} // namespace NKikimr::NPQ
