#include <ydb/library/yql/providers/solomon/actors/dq_solomon_metrics_queue.h>
#include <ydb/library/yql/providers/solomon/events/events.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NYql;
using namespace NYql::NDq;

namespace {

using TRequest = TEvSolomonProvider::TEvGetNextBatch;
using TBatch = TEvSolomonProvider::TEvMetricsBatch;
using TAck = TEvSolomonProvider::TEvAck;
using TUpdate = TEvSolomonProvider::TEvUpdateConsumersCount;
size_t Count(const TBatch& batch) { return batch.Record.MetricsSize(); }
bool Finished(const TBatch& batch) { return batch.Record.GetNoMoreMetrics(); }
TString Item(const TBatch& batch) { return batch.Record.GetMetrics(0).GetType(); }

class TClient : public NSo::ISolomonAccessorClient {
public:
    NThreading::TPromise<NSo::TListMetricsResponse> Listing = NThreading::NewPromise<NSo::TListMetricsResponse>();

    NThreading::TFuture<NSo::TListMetricsLabelsResponse> ListMetricsLabels(const NSo::TSelectors&, TInstant, TInstant) const override {
        return NThreading::MakeFuture(NSo::TListMetricsLabelsResponse(NSo::TListMetricsLabelsResult{{}, 2}, 10));
    }
    NThreading::TFuture<NSo::TListMetricsResponse> ListMetrics(const NSo::TSelectors&, TInstant, TInstant) const override {
        return Listing.GetFuture();
    }
    NThreading::TFuture<NSo::TGetLabelsResponse> GetLabelNames(const NSo::TSelectors&, TInstant, TInstant) const override {
        UNIT_FAIL("Unexpected GetLabelNames");
        return {};
    }
    NThreading::TFuture<NSo::TGetPointsCountResponse> GetPointsCount(const NSo::TSelectors&, TInstant, TInstant) const override {
        UNIT_FAIL("Unexpected GetPointsCount");
        return {};
    }
    NThreading::TFuture<NSo::TGetDataResponse> GetData(const NSo::TSelectors&, TInstant, TInstant) const override {
        UNIT_FAIL("Unexpected GetData");
        return {};
    }
    NThreading::TFuture<NSo::TGetDataResponse> GetData(const TString&, TInstant, TInstant) const override {
        UNIT_FAIL("Unexpected GetData");
        return {};
    }
};

struct TFixture : public NUnitTest::TBaseFixture {
    TTestActorRuntimeBase Runtime{2};
    TActorId QueueId;
    TActorId Consumer;
    std::deque<THolder<IEventHandle>> Responses;
    ui32 Unsubscribes = 0;
    std::deque<THolder<IEventHandle>> DisconnectDeadlines;
    std::shared_ptr<TClient> Client = std::make_shared<TClient>();

    TFixture() {
        Runtime.Initialize();
        Runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        Consumer = Runtime.AllocateEdgeActor(1);
        // Keep transport deterministic: capture remote output and drive connection
        // notifications explicitly, without a live interconnect or wall-clock timers.
        Runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->Sender == QueueId) {
                if (ev->Type == TEvents::TEvUnsubscribe::EventType) {
                    ++Unsubscribes;
                    return true;
                }
                if (ev->Type >= TEvSolomonProvider::EvBegin
                    && ev->Type < TEvSolomonProvider::EvEnd) {
                    Responses.emplace_back(ev.Release());
                    return true;
                }
            }
            return false;
        });
        Runtime.SetScheduledEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev, TDuration delay, TInstant&) {
            if (ev->Recipient == QueueId && ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                UNIT_ASSERT_VALUES_EQUAL(delay, TDuration::Minutes(2));
                DisconnectDeadlines.emplace_back(ev.Release());
                return true;
            }
            return ev->Sender == QueueId;
        });
    }

    void Pump() {
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }

    void Start(ui32 items, ui64 consumers = 1, bool defer = false) {
        NSo::TSolomonReadActorConfig cfg{};
        cfg.MaxApiInflight = 1;
        cfg.MaxListingPageSize = 100;
        cfg.MetricsQueueBatchCountLimit = 1;
        cfg.MetricsQueuePrefetchSize = 100;
        cfg.PoisonTimeout = TDuration::Hours(1);
        cfg.RoundRobinStageTimeout = TDuration::Seconds(1);
        QueueId = Runtime.Register(CreateSolomonMetricsQueueActor(consumers, {}, nullptr, cfg, Client));
        if (!defer) {
            CompleteListing(items);
        }
        Pump();
    }

    void CompleteListing(ui32 items) {
        NSo::TListMetricsResult result{};
        for (ui32 i = 0; i < items; ++i) {
            result.Metrics.push_back({{}, TStringBuilder() << "metric-" << i});
        }
        result.TotalCount = items;
        result.PagesCount = 1;
        Client->Listing.SetValue(NSo::TListMetricsResponse(std::move(result), 123));
        Pump();
    }

    template <class T>
    void Send(T* event, ui64 seqNo, ui64 confirmed = 0, TActorId consumer = {}) {
        event->Record.MutableTransportMeta()->SetSeqNo(seqNo);
        event->Record.MutableTransportMeta()->SetConfirmedSeqNo(confirmed);
        Runtime.Send(new IEventHandle(QueueId, consumer ? consumer : Consumer, event));
        Pump();
    }

    void Connect() {
        Runtime.Send(new IEventHandle(QueueId, Consumer, new TEvInterconnect::TEvNodeConnected(Consumer.NodeId())));
        Pump();
    }

    void Disconnect() {
        Runtime.Send(new IEventHandle(QueueId, Consumer, new TEvInterconnect::TEvNodeDisconnected(Consumer.NodeId())));
        Pump();
    }

    void ExpireDeadline() {
        UNIT_ASSERT(!DisconnectDeadlines.empty());
        Runtime.Send(DisconnectDeadlines.front().Release());
        DisconnectDeadlines.pop_front();
        Pump();
    }

    void Reconnect() {
        Runtime.Send(new IEventHandle(QueueId, Consumer, new TEvInterconnect::TEvNodeDisconnected(Consumer.NodeId())));
        Pump();
        Connect();
    }

    template <class T = TBatch>
    THolder<T> Pop(ui64 seqNo, ui64 confirmed, TActorId consumer = {}) {
        UNIT_ASSERT_C(!Responses.empty(), "Expected queue response");
        auto event = std::move(Responses.front());
        Responses.pop_front();
        UNIT_ASSERT_VALUES_EQUAL(event->Type, T::EventType);
        UNIT_ASSERT_VALUES_EQUAL(event->Recipient, consumer ? consumer : Consumer);
        auto response = event->Release<T>();
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTransportMeta().GetSeqNo(), seqNo);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTransportMeta().GetConfirmedSeqNo(), confirmed);
        return THolder<T>(response.Release());
    }

    void AssertStopped() {
        auto edge = Runtime.AllocateEdgeActor();
        Runtime.Send(new IEventHandle(QueueId, edge, new TEvents::TEvWakeup(), IEventHandle::FlagTrackDelivery));
        auto event = Runtime.GrabEdgeEvent<TEvents::TEvUndelivered>(edge, TDuration::Seconds(1));
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->Reason, TEvents::TEvUndelivered::ReasonActorUnknown);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TSolomonMetricsQueueRetry) {
    Y_UNIT_TEST_F(DisconnectedConsumerFailsWholeQueue, TFixture) {
        Start(4, 2);
        Send(new TRequest(), 1);
        Connect();
        Pop(1, 1);
        // A surviving consumer must receive an error, not the lost consumer's
        // batch or successful end of input.
        const auto survivor = Runtime.AllocateEdgeActor();
        Send(new TUpdate(0), 1, 0, survivor);
        Responses.clear();
        Disconnect();
        Disconnect();
        UNIT_ASSERT_VALUES_EQUAL(DisconnectDeadlines.size(), 1);
        ExpireDeadline();
        UNIT_ASSERT_VALUES_EQUAL(Responses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Responses.front()->Type, TEvSolomonProvider::TEvMetricsReadError::EventType);
        UNIT_ASSERT_VALUES_EQUAL(Responses.front()->Recipient, survivor);
        AssertStopped();
        // Returning consumers cannot obtain successful completion from a new
        // session after their unacknowledged data was discarded.
        // Use the local consumer to observe ActorUnknown without live interconnect.
        Runtime.Send(new IEventHandle(QueueId, survivor, new TRequest(), IEventHandle::FlagTrackDelivery));
        auto undelivered = Runtime.GrabEdgeEvent<TEvents::TEvUndelivered>(survivor, TDuration::Seconds(1));
        UNIT_ASSERT(undelivered);
        UNIT_ASSERT_VALUES_EQUAL(undelivered->Get()->Reason, TEvents::TEvUndelivered::ReasonActorUnknown);
    }

    Y_UNIT_TEST_F(ReconnectInvalidatesOldDisconnectDeadline, TFixture) {
        Start(3);
        Send(new TRequest(), 1);
        Connect();
        const auto original = Pop(1, 1)->Record.SerializeAsString();
        Disconnect();
        Connect();
        UNIT_ASSERT_VALUES_EQUAL(Pop(1, 1)->Record.SerializeAsString(), original);
        Disconnect();
        UNIT_ASSERT_VALUES_EQUAL(DisconnectDeadlines.size(), 2);
        ExpireDeadline(); // First outage's timer must not expire the second outage.
        Connect();
        UNIT_ASSERT_VALUES_EQUAL(Pop(1, 1)->Record.SerializeAsString(), original);
        ExpireDeadline(); // Second outage has also recovered.
        Send(new TRequest(), 2, 1);
        UNIT_ASSERT_VALUES_EQUAL(Item(*Pop(2, 2)), "metric-1");
    }

    Y_UNIT_TEST_F(UndeliveredDisconnectStartsDeadline, TFixture) {
        Start(2);
        Send(new TRequest(), 1);
        Connect();
        Pop(1, 1);
        for (unsigned i = 0; i != 2; ++i) {
            Runtime.Send(new IEventHandle(QueueId, Consumer,
                new TEvents::TEvUndelivered(TBatch::EventType, TEvents::TEvUndelivered::Disconnected)));
            Pump();
        }
        UNIT_ASSERT_VALUES_EQUAL(DisconnectDeadlines.size(), 1);
        ExpireDeadline();
        AssertStopped();
    }

    Y_UNIT_TEST_F(LostResponseReplaysOriginalItems, TFixture) {
        Start(2);
        Send(new TRequest(), 1);
        Connect();
        auto first = Pop(1, 1);
        UNIT_ASSERT_VALUES_EQUAL(Count(*first), 1);
        UNIT_ASSERT(!Finished(*first));
        const auto original = first->Record.SerializeAsString();

        // The request was delivered, but its response was lost. Replaying the
        // request must not consume a different item or enqueue another response.
        Send(new TRequest(), 1);
        UNIT_ASSERT(Responses.empty());
        Reconnect();
        UNIT_ASSERT_VALUES_EQUAL(Pop(1, 1)->Record.SerializeAsString(), original);
        UNIT_ASSERT(Responses.empty());

        Send(new TRequest(), 2, 1);
        auto second = Pop(2, 2);
        UNIT_ASSERT_VALUES_EQUAL(Count(*second), 1);
        UNIT_ASSERT(Finished(*second));
        UNIT_ASSERT_VALUES_UNEQUAL(Item(*first), Item(*second));
    }

    Y_UNIT_TEST_F(RequestDoesNotWaitForEarlierControlEvent, TFixture) {
        Start(2);
        // One batch request is in flight, but it overtakes a control ACK.
        Send(new TRequest(), 2);
        Connect();
        auto first = Pop(1, 0);
        UNIT_ASSERT_VALUES_EQUAL(Count(*first), 1);
        Send(new TRequest(), 2);
        UNIT_ASSERT(Responses.empty());

        // Filling the gap confirms both events without processing the request twice.
        Send(new TAck(), 1, 1);
        Send(new TRequest(), 3, 1);
        auto second = Pop(2, 3);
        UNIT_ASSERT_VALUES_UNEQUAL(Item(*first), Item(*second));
        Send(new TRequest(), 3, 2);
        Reconnect();
        UNIT_ASSERT(Responses.empty());
    }

    Y_UNIT_TEST_F(FinalResponseWaitsForPeerAcknowledgment, TFixture) {
        Start(1);
        Send(new TRequest(), 1);
        Connect();
        UNIT_ASSERT(Finished(*Pop(1, 1)));
        // The second request confirms exhaustion, but its empty response still
        // has to survive reconnect until the reader acknowledges receiving it.
        Send(new TRequest(), 2, 1);
        auto final = Pop(2, 2);
        UNIT_ASSERT_VALUES_EQUAL(Count(*final), 0);
        UNIT_ASSERT(Finished(*final));
        Reconnect();
        UNIT_ASSERT_VALUES_EQUAL(Pop(2, 2)->Record.SerializeAsString(), final->Record.SerializeAsString());
        Send(new TAck(), 3, 2);
        AssertStopped();
    }

    Y_UNIT_TEST_F(DuplicateRequestCanAcknowledgeFinalResponse, TFixture) {
        Start(0);
        Send(new TRequest(), 1);
        Connect();
        Pop(1, 1);
        Send(new TRequest(), 2, 1);
        Pop(2, 2);
        Send(new TRequest(), 2, 2);
        UNIT_ASSERT(Responses.empty());
        AssertStopped();
    }

    Y_UNIT_TEST_F(ConsumersHaveIndependentSequences, TFixture) {
        Start(2, 2);
        auto other = Runtime.AllocateEdgeActor(1);
        Send(new TRequest(), 1);
        Send(new TRequest(), 1, 0, other);
        Connect();
        // Hash-map iteration can replay either consumer first.
        UNIT_ASSERT_VALUES_EQUAL(Responses.size(), 2);
        THashSet<TString> items;
        while (!Responses.empty()) {
            auto recipient = Responses.front()->Recipient;
            items.insert(Item(*Pop(1, 1, recipient)));
        }
        UNIT_ASSERT_VALUES_EQUAL(items.size(), 2);
        Send(new TAck(), 2, 1);
        Reconnect();
        UNIT_ASSERT_VALUES_EQUAL(Responses.size(), 1);
        Pop(1, 1, other);
    }

    Y_UNIT_TEST_F(DeadConsumerKeepsOtherConsumersSubscription, TFixture) {
        Start(2, 2);
        auto other = Runtime.AllocateEdgeActor(1);
        Send(new TRequest(), 1);
        Send(new TRequest(), 1, 0, other);
        Connect();
        Responses.clear();
        Runtime.Send(new IEventHandle(QueueId, Consumer,
            new TEvents::TEvUndelivered(TBatch::EventType, TEvents::TEvUndelivered::ReasonActorUnknown)));
        Pump();
        UNIT_ASSERT_VALUES_EQUAL(Unsubscribes, 0);
        Reconnect();
        UNIT_ASSERT_VALUES_EQUAL(Responses.size(), 1);
        Pop(1, 1, other);
    }

    Y_UNIT_TEST_F(UnorderedReaderHandlesAckBeforeDataWithoutLosingData, TFixture) {
        Start(1);
        Send(new TRequest(), 1);
        Connect();
        auto data = Pop(1, 1);
        Send(new TUpdate(0), 2);
        auto ack = Pop<TAck>(2, 2);

        TRetryEventsQueue reader;
        reader.Init("test", Consumer, Consumer, /* eventQueueId */ 0, /* keepAlive */ false, /* useConnect */ false, /* ordered */ false);
        reader.OnNewRecipientId(QueueId, /* unsubscribe */ false);
        UNIT_ASSERT(reader.OnEventReceived(ack.Get()));
        UNIT_ASSERT(reader.OnEventReceived(data.Get()));
        UNIT_ASSERT_VALUES_EQUAL(Count(*data), 1);
        UNIT_ASSERT(!reader.OnEventReceived(ack.Get()));
        UNIT_ASSERT(!reader.OnEventReceived(data.Get()));

        // Acknowledging requests is independent of retaining their data replies.
        Reconnect();
        UNIT_ASSERT_VALUES_EQUAL(Pop(1, 2)->Record.GetTransportMeta().GetSeqNo(), 1);
        Pop<TAck>(2, 2);
        UNIT_ASSERT(Responses.empty());
    }

    Y_UNIT_TEST_F(LocalConsumerCompletesWithoutTransportSequence, TFixture) {
        Consumer = Runtime.AllocateEdgeActor(0);
        Start(1);
        Send(new TRequest(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Count(*Pop(0, 0)), 1);
        Send(new TRequest(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Count(*Pop(0, 0)), 0);
        AssertStopped();
    }

    Y_UNIT_TEST_F(DuplicatePendingRequestDoesNotAllocateAnotherBatch, TFixture) {
        Start(2, 1, true);
        Send(new TRequest(), 1);
        Send(new TRequest(), 1);
        Connect();
        UNIT_ASSERT(Responses.empty());
        CompleteListing(2);
        UNIT_ASSERT_VALUES_EQUAL(Responses.size(), 1);
        auto first = Pop(1, 1);
        Send(new TRequest(), 2, 1);
        auto second = Pop(2, 2);
        UNIT_ASSERT_VALUES_UNEQUAL(Item(*first), Item(*second));
    }

    Y_UNIT_TEST_F(ListingErrorSurvivesReconnect, TFixture) {
        Start(0, 1, true);
        Send(new TRequest(), 1);
        Connect();
        Client->Listing.SetValue(NSo::TListMetricsResponse("listing failed"));
        Pump();
        auto error = Pop<TEvSolomonProvider::TEvMetricsReadError>(1, 1);
        UNIT_ASSERT_VALUES_EQUAL(error->Record.GetIssues(), "listing failed");
        Send(new TRequest(), 1);
        Reconnect();
        UNIT_ASSERT_VALUES_EQUAL(Pop<TEvSolomonProvider::TEvMetricsReadError>(1, 1)->Record.SerializeAsString(),
            error->Record.SerializeAsString());
        UNIT_ASSERT(Responses.empty());
    }

    Y_UNIT_TEST_F(ConsumerFinishedWaitsForRetainedResponse, TFixture) {
        Start(1);
        Send(new TRequest(), 1);
        Connect();
        Pop(1, 1);
        Send(new TEvSolomonProvider::TEvConsumerFinished(), 2);
        Reconnect();
        Pop(1, 2);
        Send(new TAck(), 3, 1);
        AssertStopped();
    }
}
