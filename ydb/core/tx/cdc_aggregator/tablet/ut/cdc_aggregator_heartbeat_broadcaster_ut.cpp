#include <ydb/core/tx/cdc_aggregator/tablet/cdc_aggregator_heartbeat_broadcaster.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

namespace NKikimr::NCdcAggregator {

namespace {

constexpr ui64 FirstPersQueueTabletId = MakeTabletID(false, 100);
constexpr ui64 SecondPersQueueTabletId = MakeTabletID(false, 101);
constexpr auto Active = NKikimrPQ::ETopicPartitionStatus::Active;
constexpr auto Inactive = NKikimrPQ::ETopicPartitionStatus::Inactive;
const TPathId StreamPathId(1, 2);
const TPathId TopicPathId(1, 3);
const TString TopicPath = "/Root/Table/Stream/streamImpl";

using TPartitionIdToHeartbeatRequest = TMap<ui32, TEvPersQueue::TEvRequest::TPtr>;

class TFakePersQueue
    : public TActor<TFakePersQueue>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    TFakePersQueue(const TActorId& tablet, TTabletStorageInfo* info, const TActorId& requestsSink)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, nullptr)
        , RequestsSink(requestsSink)
    {
    }

private:
    void OnActivateExecutor(const TActorContext& ctx) override {
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
    }

    void DefaultSignalTabletActive(const TActorContext&) override {
    }

    void OnDetach(const TActorContext& ctx) override {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) override {
        Die(ctx);
    }

    STFUNC(StateInit) {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvRequest, Handle);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
        default:
            HandleDefaultEvents(ev, SelfId());
        }
    }

    void Handle(TEvPersQueue::TEvRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Forward(RequestsSink));
    }

private:
    const TActorId RequestsSink;
};

NPQ::THeartbeat MakeHeartbeat(ui64 step) {
    return NPQ::THeartbeat{.Version = TRowVersion(step, 0), .Data = ToString(step)};
}

TVector<TCdcAggregatorStreamTopicPartition> MakePartitions() {
    return {
        {.PartitionId = 0, .TabletId = FirstPersQueueTabletId},
        {.PartitionId = 1, .TabletId = FirstPersQueueTabletId},
        {.PartitionId = 2, .TabletId = SecondPersQueueTabletId},
    };
}

ui64 ParseHeartbeatStep(const TEvPersQueue::TEvRequest::TPtr& request) {
    return NPQ::THeartbeat::Parse(request->Get()->Record.GetPartitionRequest().GetCmdEmitHeartbeat().GetHeartbeat()).Version.Step;
}

TVector<ui32> CollectPartitionIds(const TPartitionIdToHeartbeatRequest& requests) {
    TVector<ui32> partitionIds;
    for (const auto& [partitionId, _] : requests) {
        partitionIds.push_back(partitionId);
    }
    return partitionIds;
}

class TTestEnv {
public:
    explicit TTestEnv(const TVector<TCdcAggregatorStreamTopicPartition>& partitions = MakePartitions())
        : Runtime(MakeHolder<TTestBasicRuntime>(1, false))
    {
        Runtime->SetLogPriority(NKikimrServices::CDC_AGGREGATOR, NLog::PRI_TRACE);
        SetupTabletServices(*Runtime);
        Edge = Runtime->AllocateEdgeActor();
        SchemeCache = Runtime->AllocateEdgeActor();
        Runtime->RegisterService(MakeSchemeCacheID(), SchemeCache);

        for (const auto tabletId : {FirstPersQueueTabletId, SecondPersQueueTabletId}) {
            BootPersQueue(tabletId);
        }

        Broadcaster = Runtime->Register(CreateCdcAggregatorHeartbeatBroadcaster(TCdcAggregatorStreamTopic{
            .StreamPathId = StreamPathId,
            .StreamPath = "/Root/Table/Stream",
            .Partitions = partitions,
        }));
        Runtime->EnableScheduleForActor(Broadcaster);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        Runtime->DispatchEvents(options);
    }

    void RebootPersQueue(ui64 tabletId) {
        ForwardToTablet(*Runtime, tabletId, TActorId(), new TEvents::TEvPoison);
        WaitTabletBoot();
        InvalidateTabletResolverCache(*Runtime, tabletId);
    }

    void SubscribeToTopic() {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request = Runtime->GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(SchemeCache)->Get()->Request;
        auto& entry = request->ResultSet.at(0);
        UNIT_ASSERT_VALUES_EQUAL(entry.TableId.PathId, StreamPathId);
        UNIT_ASSERT(entry.ShowPrivatePath);
        auto stream = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TListNodeEntry>();
        stream->Children.emplace_back("streamImpl", TopicPathId, NSchemeCache::TSchemeCacheNavigate::KindTopic);
        entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        entry.ListNodeEntry = stream;
        Runtime->Send(new IEventHandle(Broadcaster, SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request)));
        UNIT_ASSERT_VALUES_EQUAL(Runtime->GrabEdgeEvent<TEvTxProxySchemeCache::TEvWatchPathId>(SchemeCache)->Get()->PathId, TopicPathId);
    }

    void NotifyTopicPartitions(const TVector<std::pair<TCdcAggregatorStreamTopicPartition, NKikimrPQ::ETopicPartitionStatus>>& partitions) {
        auto result = MakeIntrusive<NSchemeCache::TDescribeResult>();
        auto& topic = *result->MutablePathDescription()->MutablePersQueueGroup();
        for (const auto& [partition, status] : partitions) {
            auto& description = *topic.AddPartitions();
            description.SetPartitionId(partition.PartitionId);
            description.SetTabletId(partition.TabletId);
            description.SetStatus(status);
        }
        Runtime->Send(new IEventHandle(Broadcaster, SchemeCache, new TEvTxProxySchemeCache::TEvWatchNotifyUpdated(0, TopicPath, TopicPathId, result)));
    }

    void BroadcastHeartbeat(ui64 step) {
        Runtime->Send(new IEventHandle(Broadcaster, Edge, new TEvCdcAggregatorHeartbeatBroadcaster::TEvBroadcastHeartbeat(MakeHeartbeat(step))));
    }

    TEvPersQueue::TEvRequest::TPtr GrabHeartbeatRequest(TDuration timeout = TDuration::Max()) {
        return Runtime->GrabEdgeEvent<TEvPersQueue::TEvRequest>(Edge, timeout);
    }

    TPartitionIdToHeartbeatRequest GrabHeartbeatRequests(size_t count) {
        TPartitionIdToHeartbeatRequest requests;
        for (size_t i = 0; i < count; ++i) {
            auto request = GrabHeartbeatRequest();
            const auto partitionId = request->Get()->Record.GetPartitionRequest().GetPartition();
            requests.emplace(partitionId, std::move(request));
        }
        return requests;
    }

    void ReplyConfirmed(const TEvPersQueue::TEvRequest::TPtr& request, std::optional<ui64> confirmedStep = std::nullopt) {
        auto response = MakeHolder<TEvPersQueue::TEvResponse>();
        response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
        response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
        auto& partitionResponse = *response->Record.MutablePartitionResponse();
        partitionResponse.SetCookie(request->Get()->Record.GetPartitionRequest().GetCookie());
        partitionResponse.MutableCmdEmitHeartbeatResult()->SetStep(confirmedStep.value_or(ParseHeartbeatStep(request)));
        Runtime->Send(new IEventHandle(request->Sender, Edge, response.Release()));
    }

    void ReplyError(const TEvPersQueue::TEvRequest::TPtr& request, NPersQueue::NErrorCode::EErrorCode errorCode) {
        auto response = MakeHolder<TEvPersQueue::TEvResponse>();
        response->Record.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
        response->Record.SetErrorCode(errorCode);
        response->Record.MutablePartitionResponse()->SetCookie(request->Get()->Record.GetPartitionRequest().GetCookie());
        Runtime->Send(new IEventHandle(request->Sender, Edge, response.Release()));
    }

    TInstant GetCurrentTime() const {
        return Runtime->GetCurrentTime();
    }

private:
    void BootPersQueue(ui64 tabletId) {
        const auto bootstrapper = CreateTestBootstrapper(*Runtime, CreateTestTabletInfo(tabletId, TTabletTypes::PersQueue),
            [edge = Edge](const TActorId& tablet, TTabletStorageInfo* info) -> IActor* {
                return new TFakePersQueue(tablet, info, edge);
            });
        Runtime->EnableScheduleForActor(bootstrapper);
        WaitTabletBoot();
    }

    void WaitTabletBoot() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime->DispatchEvents(options);
    }

private:
    THolder<TTestBasicRuntime> Runtime;
    TActorId Edge;
    TActorId SchemeCache;
    TActorId Broadcaster;
};

} // namespace

Y_UNIT_TEST_SUITE(TCdcAggregatorHeartbeatBroadcasterTest) {
    Y_UNIT_TEST(ShouldSendHeartbeatToEveryPartitionWhenHeartbeatIsBroadcast) {
        // given
        TTestEnv env;

        // when
        env.BroadcastHeartbeat(100);

        // then
        const auto requests = env.GrabHeartbeatRequests(3);
        UNIT_ASSERT_VALUES_EQUAL(CollectPartitionIds(requests), TVector<ui32>({0, 1, 2}));
        for (const auto& [partitionId, request] : requests) {
            UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetPartitionRequest().GetCookie(), ui64(partitionId));
            UNIT_ASSERT_VALUES_EQUAL(ParseHeartbeatStep(request), 100);
        }
    }

    Y_UNIT_TEST(ShouldNotSendAgainWhenSentHeartbeatIsNotConfirmed) {
        // given
        TTestEnv env({{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}});
        env.BroadcastHeartbeat(100);
        env.GrabHeartbeatRequest();

        // when
        env.BroadcastHeartbeat(200);

        // then
        UNIT_ASSERT(!env.GrabHeartbeatRequest(TDuration::Seconds(3)));
    }

    Y_UNIT_TEST(ShouldSendNewestHeartbeatWhenSentHeartbeatIsConfirmed) {
        // given
        TTestEnv env({{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}});
        env.BroadcastHeartbeat(100);
        const auto request = env.GrabHeartbeatRequest();
        env.BroadcastHeartbeat(200);

        // when
        env.ReplyConfirmed(request);

        // then
        UNIT_ASSERT_VALUES_EQUAL(ParseHeartbeatStep(env.GrabHeartbeatRequest()), 200);
    }

    Y_UNIT_TEST(ShouldRetryOnNextTickWhenPartitionRepliesInactive) {
        // given
        TTestEnv env;
        env.BroadcastHeartbeat(100);
        const auto requests = env.GrabHeartbeatRequests(3);
        env.ReplyConfirmed(requests.at(1));
        env.ReplyConfirmed(requests.at(2));
        const auto failedAt = env.GetCurrentTime();

        // when
        env.ReplyError(requests.at(0), NPersQueue::NErrorCode::WRITE_ERROR_PARTITION_INACTIVE);

        // then
        const auto retry = env.GrabHeartbeatRequest();
        UNIT_ASSERT_VALUES_EQUAL(retry->Get()->Record.GetPartitionRequest().GetPartition(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ParseHeartbeatStep(retry), 100);
        UNIT_ASSERT_LE(env.GetCurrentTime() - failedAt, TDuration::Seconds(1));
        UNIT_ASSERT(!env.GrabHeartbeatRequest(TDuration::Seconds(3)));
    }

    Y_UNIT_TEST(ShouldRetryOnNextTickWhenPartitionConfirmedOlderVersion) {
        // given
        TTestEnv env({{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}});
        env.BroadcastHeartbeat(100);
        const auto request = env.GrabHeartbeatRequest();

        // when
        env.ReplyConfirmed(request, 50);

        // then
        UNIT_ASSERT_VALUES_EQUAL(ParseHeartbeatStep(env.GrabHeartbeatRequest()), 100);
    }

    Y_UNIT_TEST(ShouldSendNewHeartbeatImmediatelyWhenPartitionFailed) {
        // given
        TTestEnv env({{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}});
        env.BroadcastHeartbeat(100);
        env.ReplyError(env.GrabHeartbeatRequest(), NPersQueue::NErrorCode::OVERLOAD);
        const auto failedAt = env.GetCurrentTime();

        // when
        env.BroadcastHeartbeat(200);

        // then
        UNIT_ASSERT_VALUES_EQUAL(ParseHeartbeatStep(env.GrabHeartbeatRequest()), 200);
        UNIT_ASSERT_VALUES_EQUAL(env.GetCurrentTime(), failedAt);
    }

    Y_UNIT_TEST(ShouldSlowDownRetriesWhenPartitionKeepsFailing) {
        // given
        TTestEnv env({{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}});
        env.BroadcastHeartbeat(100);
        env.ReplyError(env.GrabHeartbeatRequest(), NPersQueue::NErrorCode::OVERLOAD);
        const auto firstRetry = env.GrabHeartbeatRequest();
        const auto firstRetryAt = env.GetCurrentTime();

        // when
        env.ReplyError(firstRetry, NPersQueue::NErrorCode::OVERLOAD);

        // then
        UNIT_ASSERT_VALUES_EQUAL(ParseHeartbeatStep(env.GrabHeartbeatRequest()), 100);
        UNIT_ASSERT_GE(env.GetCurrentTime() - firstRetryAt, TDuration::Seconds(2));
    }

    Y_UNIT_TEST(ShouldRetryOnNextTickWhenPipeIsLost) {
        // given
        TTestEnv env;
        env.BroadcastHeartbeat(100);
        env.GrabHeartbeatRequests(3);

        // when
        env.RebootPersQueue(FirstPersQueueTabletId);

        // then
        const auto retries = env.GrabHeartbeatRequests(2);
        UNIT_ASSERT_VALUES_EQUAL(CollectPartitionIds(retries), TVector<ui32>({0, 1}));
        for (const auto& [_, retry] : retries) {
            UNIT_ASSERT_VALUES_EQUAL(ParseHeartbeatStep(retry), 100);
        }
    }

    Y_UNIT_TEST(ShouldSendHeartbeatToNewPartitionsWhenTopicIsSplit) {
        // given
        TTestEnv env({{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}});
        env.SubscribeToTopic();
        env.BroadcastHeartbeat(100);
        env.ReplyConfirmed(env.GrabHeartbeatRequest());

        // when
        env.NotifyTopicPartitions({
            {{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}, Inactive},
            {{.PartitionId = 1, .TabletId = SecondPersQueueTabletId}, Active},
            {{.PartitionId = 2, .TabletId = SecondPersQueueTabletId}, Active},
        });

        // then
        const auto requests = env.GrabHeartbeatRequests(2);
        UNIT_ASSERT_VALUES_EQUAL(CollectPartitionIds(requests), TVector<ui32>({1, 2}));
        for (const auto& [_, request] : requests) {
            UNIT_ASSERT_VALUES_EQUAL(ParseHeartbeatStep(request), 100);
        }
    }

    Y_UNIT_TEST(ShouldNotSendHeartbeatToPartitionWhenTopicMarkedItInactive) {
        // given
        TTestEnv env;
        env.SubscribeToTopic();
        env.NotifyTopicPartitions({
            {{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}, Active},
            {{.PartitionId = 1, .TabletId = FirstPersQueueTabletId}, Active},
            {{.PartitionId = 2, .TabletId = SecondPersQueueTabletId}, Inactive},
        });

        // when
        env.BroadcastHeartbeat(100);

        // then
        UNIT_ASSERT_VALUES_EQUAL(CollectPartitionIds(env.GrabHeartbeatRequests(2)), TVector<ui32>({0, 1}));
        UNIT_ASSERT(!env.GrabHeartbeatRequest(TDuration::Seconds(3)));
    }

    Y_UNIT_TEST(ShouldIgnoreResponseWhenPartitionIsRemovedFromTopic) {
        // given
        TTestEnv env({{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}});
        env.SubscribeToTopic();
        env.BroadcastHeartbeat(100);
        const auto request = env.GrabHeartbeatRequest();
        env.NotifyTopicPartitions({{{.PartitionId = 0, .TabletId = FirstPersQueueTabletId}, Inactive}});

        // when
        env.ReplyError(request, NPersQueue::NErrorCode::OVERLOAD);

        // then
        UNIT_ASSERT(!env.GrabHeartbeatRequest(TDuration::Seconds(3)));
    }
}

} // namespace NKikimr::NCdcAggregator
