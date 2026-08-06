#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/digest/md5/md5.h>
#include <util/random/fast.h>
#include <util/string/vector.h>

using namespace NActors;

<<<<<<< HEAD
=======
namespace {

ui64 GetSessionCounter(TTestICCluster& cluster, ui32 me, ui32 peer, TStringBuf name) {
    const TString start = TStringBuilder() << "<tr><td>" << name << "</td><td>";
    return FromString<ui64>(ExtractPattern(cluster, me, peer, start, "<"));
}

TDuration GetSessionDurationMetric(TTestICCluster& cluster, ui32 me, ui32 peer, TStringBuf name) {
    const TString start = TStringBuilder() << "<tr><td>" << name << "</td><td>";
    return TDuration::Parse(ExtractPattern(cluster, me, peer, start, "<"));
}

i64 GetSessionSignedDurationMetricUs(TTestICCluster& cluster, ui32 me, ui32 peer, TStringBuf name) {
    const TString start = TStringBuilder() << "<tr><td>" << name << "</td><td>";
    const TString value = ExtractPattern(cluster, me, peer, start, "<");
    TStringBuf metric(value);
    i64 sign = 1;
    if (metric && (metric[0] == '+' || metric[0] == '-')) {
        sign = metric[0] == '-' ? -1 : 1;
        metric = metric.SubStr(1);
    }
    return sign * TDuration::Parse(metric).MicroSeconds();
}

TString GetSessionTextMetric(TTestICCluster& cluster, ui32 me, ui32 peer, TStringBuf name) {
    const TString start = TStringBuilder() << "<tr><td>" << name << "</td><td>";
    return ExtractPattern(cluster, me, peer, start, "<");
}

i64 GetSessionSocketFd(TTestICCluster& cluster, ui32 me, ui32 peer) {
    return FromString<i64>(ExtractPattern(cluster, me, peer, "<tr><td>Socket</td><td>", "<"));
}

ui64 WaitForSessionCounter(TTestICCluster& cluster, ui32 me, ui32 peer, TStringBuf name,
        TDuration timeout = TDuration::Seconds(10)) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        try {
            return GetSessionCounter(cluster, me, peer, name);
        } catch (const TPatternNotFound&) {
            Sleep(TDuration::MilliSeconds(100));
        }
    }
    UNIT_FAIL(TStringBuilder() << "failed to read session counter " << name << " from " << me << " to " << peer);
    return 0;
}

ui64 GetPeerCounterValue(
        const NMonitoring::TDynamicCounterPtr& counters,
        TStringBuf peerLabel,
        TStringBuf name) {
    auto peerCounters = counters->FindSubgroup("peer", TString(peerLabel));
    UNIT_ASSERT_C(peerCounters, TStringBuilder() << "peer=" << peerLabel << " counters were not created");
    auto counter = peerCounters->FindCounter(TString(name));
    UNIT_ASSERT_C(counter, TStringBuilder() << name << " counter was not created for peer=" << peerLabel);
    return counter->Val();
}

void RunScopeClassCounterRebindTest(TScopeId peerScopeId, TStringBuf expectedPeerLabel) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    common->LocalScopeId = TScopeId(1, 42);
    common->Settings.MergePerScopeClassCounters = true;

    auto counters = CreateInterconnectCounters(common);
    counters->SetPeerInfo("peer-host:19001", "dc-1", "unknown");
    counters->SetConnected(0);
    counters->SetRdmaRetryWatchdogPending(0);

    UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "unknown", "Connected"), 0);
    UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "unknown", "RdmaRetryWatchdogPendingSessions"), 0);
    UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "unknown", "Disconnections"), 0);

    counters->SetPeerScopeId(peerScopeId);
    counters->SetConnected(1);
    counters->SetRdmaRetryWatchdogPending(1);
    counters->IncDisconnections();

    UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, expectedPeerLabel, "Connected"), 1);
    UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, expectedPeerLabel, "RdmaRetryWatchdogPendingSessions"), 1);
    UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, expectedPeerLabel, "Disconnections"), 1);
    UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "unknown", "Connected"), 0);
    UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "unknown", "RdmaRetryWatchdogPendingSessions"), 0);
    UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "unknown", "Disconnections"), 0);
    UNIT_ASSERT_C(!common->MonCounters->FindSubgroup("peer", "peer-host:19001"),
        "scope class aggregation must not publish counters under the original peer host label");
}

ui64 GetHistogramSamples(const NMonitoring::THistogramPtr& histogram) {
    ui64 samples = 0;
    auto snapshot = histogram->Snapshot();
    for (ui32 i = 0; i < snapshot->Count(); ++i) {
        samples += snapshot->Value(i);
    }
    return samples;
}

ui64 GetHistogramBucketSamples(const NMonitoring::THistogramPtr& histogram, NMonitoring::TBucketBound upperBound) {
    auto snapshot = histogram->Snapshot();
    for (ui32 i = 0; i < snapshot->Count(); ++i) {
        if (snapshot->UpperBound(i) == upperBound) {
            return snapshot->Value(i);
        }
    }
    return 0;
}

template <typename TCallback>
void WaitForCondition(TDuration timeout, TCallback&& callback, TStringBuf description) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        if (callback()) {
            return;
        }
        Sleep(TDuration::MilliSeconds(50));
    }
    UNIT_FAIL(TStringBuilder() << "condition failed: " << description);
}

class TDropRecipientActor : public TActor<TDropRecipientActor> {
public:
    TDropRecipientActor()
        : TActor(&TThis::StateFunc)
    {}

    size_t GetReceived() const noexcept {
        return Received.load(std::memory_order_relaxed);
    }

private:
    void HandlePing(TAutoPtr<IEventHandle>&) {
        Received.fetch_add(1, std::memory_order_relaxed);
    }

    STRICT_STFUNC(StateFunc,
        fFunc(TEvents::THelloWorld::Ping, HandlePing);
    )

private:
    std::atomic<size_t> Received = 0;
};

class TConnectionSubscriberActor : public TActorBootstrapped<TConnectionSubscriberActor> {
public:
    explicit TConnectionSubscriberActor(ui32 peerNodeId)
        : PeerNodeId(peerNodeId)
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
        Send(TActivationContext::InterconnectProxy(PeerNodeId), new TEvents::TEvSubscribe);
    }

    bool IsConnected() const {
        return Connected.load(std::memory_order_acquire);
    }

private:
    void Handle(TEvInterconnect::TEvNodeConnected::TPtr&) {
        Connected.store(true, std::memory_order_release);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvInterconnect::TEvNodeConnected, Handle)
        cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, PassAway)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )

private:
    const ui32 PeerNodeId;
    std::atomic<bool> Connected = false;
};

class TBurstSenderActor : public TActorBootstrapped<TBurstSenderActor> {
public:
    TBurstSenderActor(TActorId recipient, size_t messages, size_t payloadSize)
        : Recipient(recipient)
        , Messages(messages)
        , PayloadSize(payloadSize)
    {}

    void Bootstrap() {
        TString payload = TString::Uninitialized(PayloadSize);
        memset(payload.Detach(), 'x', payload.size());
        for (size_t i = 0; i < Messages; ++i) {
            TActivationContext::Send(new IEventHandle(TEvents::THelloWorld::Ping, 0, Recipient, SelfId(),
                MakeIntrusive<TEventSerializedData>(TString(payload), TEventSerializationInfo{}), i));
        }
        PassAway();
    }

private:
    const TActorId Recipient;
    const size_t Messages;
    const size_t PayloadSize;
};

struct TEvXdcCatchReplay
    : TEventPB<TEvXdcCatchReplay, NInterconnectTest::TEvTestSerialization, EventSpaceBegin(TEvents::ES_PRIVATE) + 100>
{};

struct TEvOversizedTcpEvent
    : TEventPB<TEvOversizedTcpEvent, NInterconnectTest::TEvTestSerialization, EventSpaceBegin(TEvents::ES_PRIVATE) + 101>
{};

struct TOversizedTcpEventContext {
    std::atomic<bool> Undelivered = false;
    std::atomic<bool> Received = false;
};

class TOversizedTcpEventSenderActor : public TActorBootstrapped<TOversizedTcpEventSenderActor> {
public:
    TOversizedTcpEventSenderActor(TActorId recipient, std::unique_ptr<IEventBase> event,
            std::shared_ptr<TOversizedTcpEventContext> context)
        : Recipient(recipient)
        , Event(std::move(event))
        , Context(std::move(context))
    {}

    void Bootstrap() {
        Send(Recipient, std::move(Event), IEventHandle::FlagTrackDelivery);
        Become(&TThis::StateFunc);
    }

private:
    void Handle(TEvents::TEvUndelivered::TPtr&) {
        Context->Undelivered.store(true, std::memory_order_release);
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvUndelivered, Handle);
    )

private:
    const TActorId Recipient;
    std::unique_ptr<IEventBase> Event;
    const std::shared_ptr<TOversizedTcpEventContext> Context;
};

class TOversizedTcpEventReceiverActor : public TActorBootstrapped<TOversizedTcpEventReceiverActor> {
public:
    explicit TOversizedTcpEventReceiverActor(std::shared_ptr<TOversizedTcpEventContext> context)
        : Context(std::move(context))
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
    }

private:
    void Handle(TEvOversizedTcpEvent::TPtr&) {
        Context->Received.store(true, std::memory_order_release);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvOversizedTcpEvent, Handle);
    )

private:
    const std::shared_ptr<TOversizedTcpEventContext> Context;
};

class TXdcCatchReplaySenderActor : public TActorBootstrapped<TXdcCatchReplaySenderActor> {
public:
    TXdcCatchReplaySenderActor(TActorId recipient, IEventBase* event)
        : Recipient(recipient)
        , Event(event)
    {}

    void Bootstrap() {
        Send(Recipient, Event);
        PassAway();
    }

private:
    const TActorId Recipient;
    IEventBase* const Event;
};

class TXdcCatchReplayReceiverActor : public TActorBootstrapped<TXdcCatchReplayReceiverActor> {
public:
    TXdcCatchReplayReceiverActor(TString expectedPayload, ui32 expectedPayloadCount)
        : ExpectedPayload(std::move(expectedPayload))
        , ExpectedPayloadCount(expectedPayloadCount)
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
    }

    void Handle(TEvXdcCatchReplay::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 42u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "catch-replay");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), ExpectedPayloadCount);
        for (ui32 i = 0; i < ExpectedPayloadCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[i].GetSize(), ExpectedPayload.size());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[i].ConvertToString(), ExpectedPayload);
        }
        Received.fetch_add(1, std::memory_order_relaxed);
    }

    size_t GetReceived() const noexcept {
        return Received.load(std::memory_order_relaxed);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvXdcCatchReplay, Handle);
    )

private:
    const TString ExpectedPayload;
    const ui32 ExpectedPayloadCount;
    std::atomic<size_t> Received = 0;
};

enum class EXdcCatchReplayMode {
    Tcp,
    Rdma,
};

enum class EXdcCatchReplayReconnectAction {
    CloseInputSession,
    ClosePeerSocket,
};

TEvXdcCatchReplay* MakeXdcCatchReplayEvent(
        TStringBuf payload,
        ui32 payloadCount,
        const std::shared_ptr<NInterconnect::NRdma::IMemPool>& rdmaMemPool) {
    auto* event = new TEvXdcCatchReplay;
    event->Record.SetBlobID(42);
    event->Record.SetBuffer("catch-replay");

    for (ui32 i = 0; i < payloadCount; ++i) {
        if (rdmaMemPool) {
            auto buffer = rdmaMemPool->AllocRcBuf(payload.size(), 0).value();
            Y_ABORT_UNLESS(buffer);
            memcpy(buffer.GetDataMut(), payload.data(), payload.size());
            event->AddPayload(TRope(std::move(buffer)));
        } else {
            event->AddPayload(TRope(TString(payload)));
        }
    }

    UNIT_ASSERT(event->AllowExternalDataChannel());
    return event;
}

void WaitForXdcCatchReplayPreReconnectState(
        TTestICCluster& cluster,
        TXdcCatchReplayReceiverActor* receiver,
        EXdcCatchReplayMode mode) {
    WaitForCondition(TDuration::Seconds(30), [&] {
        try {
            if (receiver->GetReceived() != 0
                    || GetSessionCounter(cluster, 1, 2, "Params.UseExternalDataChannel") != 1
                    || GetSessionCounter(cluster, 1, 2, "Context->LastProcessedSerial") == 0) {
                return false;
            }

            switch (mode) {
                case EXdcCatchReplayMode::Tcp: {
                    const ui64 bytesReadFromXdc = GetSessionCounter(cluster, 1, 2, "BytesReadFromXdcSocket");
                    return bytesReadFromXdc > 0
                        && bytesReadFromXdc < 16 * 1024
                        && GetSessionCounter(cluster, 1, 2, "XdcInputQ.size()") > 0
                        && GetSessionCounter(cluster, 1, 2, "InboundPacketQ.size()") > 0;
                }

                case EXdcCatchReplayMode::Rdma:
                    return GetRdmaChecksumStatus(cluster, 1, 2).StartsWith("On")
                        && GetSessionCounter(cluster, 1, 2, "RdmaBytesReadScheduled") == 0
                        && GetSessionCounter(cluster, 1, 2, "RdmaWrReadScheduled") == 0;
            }
        } catch (const TPatternNotFound&) {
            return false;
        } catch (const TFromStringException&) {
            return false;
        }
    }, mode == EXdcCatchReplayMode::Tcp
        ? "partial TCP XDC payload read before reconnect"
        : "partial RDMA XDC section replay state before reconnect");
}

void WaitForRdmaXdcCatchReplayAfterPartialReadScheduled(
        TTestICCluster& cluster,
        TXdcCatchReplayReceiverActor* receiver,
        ui64 totalRdmaBytes) {
    WaitForCondition(TDuration::Seconds(30), [&] {
        try {
            if (receiver->GetReceived() != 0
                    || GetSessionCounter(cluster, 1, 2, "Params.UseExternalDataChannel") != 1
                    || GetSessionCounter(cluster, 1, 2, "Context->LastProcessedSerial") == 0
                    || !GetRdmaChecksumStatus(cluster, 1, 2).StartsWith("On")) {
                return false;
            }

            const ui64 rdmaBytesReadScheduled = GetSessionCounter(cluster, 1, 2, "RdmaBytesReadScheduled");
            return rdmaBytesReadScheduled > 0
                && rdmaBytesReadScheduled < totalRdmaBytes
                && GetSessionCounter(cluster, 1, 2, "RdmaWrReadScheduled") > 0;
        } catch (const TPatternNotFound&) {
            return false;
        } catch (const TFromStringException&) {
            return false;
        }
    }, "partial RDMA XDC read scheduling before reconnect");
}

void CloseXdcCatchReplayInputSession(
        TTestICCluster& cluster,
        TXdcCatchReplayReceiverActor* receiver) {
    UNIT_ASSERT_VALUES_EQUAL(receiver->GetReceived(), 0u);

    // Freeze the old TCP control transport before closing the input session, so callers exercise reconnect behavior
    // with a partially consumed XDC/RDMA receive context.
    cluster.StartBlackhole(1);
    Sleep(TDuration::MilliSeconds(100));
    UNIT_ASSERT_VALUES_EQUAL(receiver->GetReceived(), 0u);

    cluster.GetNode(1)->Send(cluster.InterconnectProxy(2, 1), new TEvInterconnect::TEvCloseInputSession);
    Sleep(TDuration::MilliSeconds(100));
    cluster.StopBlackhole(1);
}

void ReconnectXdcCatchReplayInputSession(
        TTestICCluster& cluster,
        TXdcCatchReplayReceiverActor* receiver,
        TStringBuf description) {
    const TString handshakeBefore = GetSessionTextMetric(cluster, 1, 2, "LastHandshakeDone");
    CloseXdcCatchReplayInputSession(cluster, receiver);

    WaitForCondition(TDuration::Seconds(30), [&] {
        try {
            return GetSessionTextMetric(cluster, 1, 2, "LastHandshakeDone") != handshakeBefore
                && GetSessionSocketFd(cluster, 1, 2) >= 0;
        } catch (const TPatternNotFound&) {
            return false;
        } catch (const TFromStringException&) {
            return false;
        }
    }, description);
}

void CloseXdcCatchReplayPeerSocket(
        TTestICCluster& cluster,
        TXdcCatchReplayReceiverActor* receiver) {
    UNIT_ASSERT_VALUES_EQUAL(receiver->GetReceived(), 0u);
    cluster.GetNode(2)->Send(cluster.InterconnectProxy(1, 2), new TEvInterconnect::TEvClosePeerSocket);
}

bool XdcCatchReplaySessionChangedOrGone(
        TTestICCluster& cluster,
        ui32 nodeId,
        ui32 peerNodeId,
        const TString& createdBefore) {
    try {
        return GetSessionTextMetric(cluster, nodeId, peerNodeId, "Created") != createdBefore;
    } catch (const TPatternNotFound&) {
        return true;
    } catch (const TFromStringException&) {
        return false;
    }
}

void WaitForXdcCatchReplayRdmaReceiveSessionReplacement(
        TTestICCluster& cluster,
        TXdcCatchReplayReceiverActor* receiver,
        const TString& receiveSessionCreatedBefore) {
    WaitForCondition(TDuration::Seconds(30), [&] {
        return receiver->GetReceived() == 0
            && XdcCatchReplaySessionChangedOrGone(cluster, 1, 2, receiveSessionCreatedBefore);
    }, "RDMA XDC receive session replaced instead of graceful reconnect");

    Sleep(TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(receiver->GetReceived(), 0u);
}

void WaitForXdcCatchReplayDelivery(
        TTestICCluster& cluster,
        TXdcCatchReplayReceiverActor* receiver,
        bool useRdma) {
    WaitForCondition(TDuration::Seconds(30), [&] {
        return receiver->GetReceived() == 1;
    }, "XDC catch replay delivery");

    Sleep(TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(receiver->GetReceived(), 1u);
    if (useRdma) {
        UNIT_ASSERT_C(WaitForSessionCounter(cluster, 1, 2, "RdmaBytesReadScheduled") > 0,
            "replayed session did not schedule RDMA reads");
    } else {
        UNIT_ASSERT_C(WaitForSessionCounter(cluster, 1, 2, "XdcRefs") > 0,
            "replayed session did not parse XDC refs");
    }
}

void RunXdcCatchReplayAfterPartialPayloadRead(EXdcCatchReplayMode mode) {
    const bool useRdma = mode == EXdcCatchReplayMode::Rdma;
    const TString payload(useRdma ? 4 * 1024 : 32 * 1024, 'x');
    const ui32 payloadCount = useRdma ? 1400 : 1;

    TTestICCluster::TTrafficInterrupterSettings interrupterSettings{
        .RejectingTrafficTimeout = TDuration::Zero(),
        .BandWidth = 8 * 1024,
        .Disconnect = false,
    };
    TTestICCluster cluster(2, TChannelsConfig(), &interrupterSettings, nullptr,
        useRdma ? TTestICCluster::EMPTY : TTestICCluster::DISABLE_RDMA,
        {}, TDuration::Seconds(30), useRdma ? 16u << 20 : TNode::DefaultInflight());

    auto* receiverPtr = new TXdcCatchReplayReceiverActor(payload, payloadCount);
    const TActorId recipient = cluster.RegisterActor(receiverPtr, 1);

    auto* event = MakeXdcCatchReplayEvent(
        payload,
        payloadCount,
        useRdma ? cluster.GetNode(2)->GetRdmaMemPool() : nullptr);

    cluster.RegisterActor(new TXdcCatchReplaySenderActor(recipient, event), 2);

    WaitForXdcCatchReplayPreReconnectState(cluster, receiverPtr, mode);

    if (useRdma) {
        const TString receiveSessionCreatedBefore = GetSessionTextMetric(cluster, 1, 2, "Created");
        CloseXdcCatchReplayInputSession(cluster, receiverPtr);
        WaitForXdcCatchReplayRdmaReceiveSessionReplacement(cluster, receiverPtr, receiveSessionCreatedBefore);
    } else {
        ReconnectXdcCatchReplayInputSession(cluster, receiverPtr,
            "XDC input session reconnected after partial payload read");
        WaitForXdcCatchReplayDelivery(cluster, receiverPtr, false);
    }
}

void RunRdmaXdcCatchReplayAfterPartialRdmaRead(EXdcCatchReplayReconnectAction reconnectAction) {
    const TString payload(4 * 1024, 'x');
    const ui32 payloadCount = 1400;
    const ui64 totalRdmaBytes = ui64(payload.size()) * payloadCount;

    TTestICCluster::TTrafficInterrupterSettings interrupterSettings{
        .RejectingTrafficTimeout = TDuration::Zero(),
        .BandWidth = 8 * 1024,
        .Disconnect = false,
    };
    TTestICCluster cluster(2, TChannelsConfig(), &interrupterSettings, nullptr,
        TTestICCluster::EMPTY, {}, TDuration::Seconds(30), 16u << 20);

    auto* receiverPtr = new TXdcCatchReplayReceiverActor(payload, payloadCount);
    const TActorId recipient = cluster.RegisterActor(receiverPtr, 1);

    auto* event = MakeXdcCatchReplayEvent(payload, payloadCount, cluster.GetNode(2)->GetRdmaMemPool());
    cluster.RegisterActor(new TXdcCatchReplaySenderActor(recipient, event), 2);

    WaitForRdmaXdcCatchReplayAfterPartialReadScheduled(cluster, receiverPtr, totalRdmaBytes);
    const TString receiveSessionCreatedBefore = GetSessionTextMetric(cluster, 1, 2, "Created");
    switch (reconnectAction) {
        case EXdcCatchReplayReconnectAction::CloseInputSession:
            CloseXdcCatchReplayInputSession(cluster, receiverPtr);
            break;

        case EXdcCatchReplayReconnectAction::ClosePeerSocket:
            CloseXdcCatchReplayPeerSocket(cluster, receiverPtr);
            break;
    }
    WaitForXdcCatchReplayRdmaReceiveSessionReplacement(cluster, receiverPtr, receiveSessionCreatedBefore);
}

struct THandshakeFailureLogCounters {
    std::atomic<ui32> Notice = 0;
    std::atomic<ui32> Debug = 0;
};

class TCountingLogBackend : public TLogBackend {
public:
    explicit TCountingLogBackend(std::shared_ptr<THandshakeFailureLogCounters> outgoingHandshakeFailures)
        : OutgoingHandshakeFailures(std::move(outgoingHandshakeFailures))
    {}

    void WriteData(const TLogRecord& rec) override {
        const TStringBuf line(rec.Data, rec.Len);
        if (line.Contains("ICP25") && line.Contains("outgoing handshake failed")) {
            if (rec.Priority == TLOG_NOTICE) {
                OutgoingHandshakeFailures->Notice.fetch_add(1, std::memory_order_relaxed);
            } else if (rec.Priority == TLOG_DEBUG) {
                OutgoingHandshakeFailures->Debug.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    void ReopenLog() override {
    }

private:
    std::shared_ptr<THandshakeFailureLogCounters> OutgoingHandshakeFailures;
};

struct TSubscriberLivenessLogState {
    std::atomic<ui32> Warnings = 0;
    TMutex Mutex;
    TString LastWarning;
};

class TSubscriberLivenessLogBackend : public TLogBackend {
public:
    explicit TSubscriberLivenessLogBackend(std::shared_ptr<TSubscriberLivenessLogState> state)
        : State(std::move(state))
    {}

    void WriteData(const TLogRecord& rec) override {
        const TStringBuf line(rec.Data, rec.Len);
        if (rec.Priority == TLOG_WARNING &&
                line.Contains("Subscriber liveness check found leaked subscriptions")) {
            with_lock (State->Mutex) {
                State->LastWarning = line;
            }
            State->Warnings.fetch_add(1, std::memory_order_release);
        }
    }

    void ReopenLog() override {
    }

private:
    std::shared_ptr<TSubscriberLivenessLogState> State;
};

} // namespace

>>>>>>> 9f8d5a7e15d (Fix cleanup after oversized event serialization (#48532))
class TSenderActor : public TActorBootstrapped<TSenderActor> {
    const TActorId Recipient;
    const size_t SendLimit;
    using TSessionToCookie = std::unordered_multimap<TActorId, ui64, THash<TActorId>>;
    TSessionToCookie SessionToCookie;
    std::unordered_map<ui64, std::pair<TSessionToCookie::iterator, TString>> InFlight;
    std::unordered_map<ui64, TString> Tentative;
    ui64 NextCookie = 0;
    TActorId SessionId;
    bool SubscribeInFlight = false;

public:
    TSenderActor(TActorId recipient, size_t sendLimit = -1)
        : Recipient(recipient)
        , SendLimit(sendLimit)
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
        Subscribe();
    }

    void Subscribe() {
        Cerr << (TStringBuilder() << "Subscribe" << Endl);
        Y_ABORT_UNLESS(!SubscribeInFlight);
        SubscribeInFlight = true;
        Send(TActivationContext::InterconnectProxy(Recipient.NodeId()), new TEvents::TEvSubscribe);
    }

    void IssueQueries() {
        if (!SessionId) {
            return;
        }
        while (InFlight.size() < 10 && NextCookie < SendLimit) {
            size_t len = RandomNumber<size_t>(65536) + 1;
            TString data = TString::Uninitialized(len);
            TReallyFastRng32 rng(RandomNumber<ui32>());
            char *p = data.Detach();
            for (size_t i = 0; i < len; ++i) {
                p[i] = rng();
            }
            const TSessionToCookie::iterator s2cIt = SessionToCookie.emplace(SessionId, NextCookie);
            InFlight.emplace(NextCookie, std::make_tuple(s2cIt, MD5::CalcRaw(data)));
            TActivationContext::Send(new IEventHandle(TEvents::THelloWorld::Ping, IEventHandle::FlagTrackDelivery, Recipient,
                SelfId(), MakeIntrusive<TEventSerializedData>(std::move(data), TEventSerializationInfo{}), NextCookie));
//            Cerr << (TStringBuilder() << "Send# " << NextCookie << Endl);
            ++NextCookie;
        }
    }

    void HandlePong(TAutoPtr<IEventHandle> ev) {
//        Cerr << (TStringBuilder() << "Receive# " << ev->Cookie << Endl);
        if (const auto it = InFlight.find(ev->Cookie); it != InFlight.end()) {
            auto& [s2cIt, hash] = it->second;
            Y_ABORT_UNLESS(hash == ev->GetChainBuffer()->GetString());
            SessionToCookie.erase(s2cIt);
            InFlight.erase(it);
        } else if (const auto it = Tentative.find(ev->Cookie); it != Tentative.end()) {
            Y_ABORT_UNLESS(it->second == ev->GetChainBuffer()->GetString());
            Tentative.erase(it);
        } else {
            Y_ABORT("Cookie# %" PRIu64, ev->Cookie);
        }
        IssueQueries();
    }

    void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
        Cerr << (TStringBuilder() << "TEvNodeConnected" << Endl);
        Y_ABORT_UNLESS(SubscribeInFlight);
        SubscribeInFlight = false;
        Y_ABORT_UNLESS(!SessionId);
        SessionId = ev->Sender;
        IssueQueries();
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        Cerr << (TStringBuilder() << "TEvNodeDisconnected" << Endl);
        SubscribeInFlight = false;
        if (SessionId) {
            Y_ABORT_UNLESS(SessionId == ev->Sender);
            auto r = SessionToCookie.equal_range(SessionId);
            for (auto it = r.first; it != r.second; ++it) {
                const auto inFlightIt = InFlight.find(it->second);
                Y_ABORT_UNLESS(inFlightIt != InFlight.end());
                Tentative.emplace(inFlightIt->first, inFlightIt->second.second);
                InFlight.erase(it->second);
            }
            SessionToCookie.erase(r.first, r.second);
            SessionId = TActorId();
        }
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
    }

    void Handle(TEvents::TEvUndelivered::TPtr ev) {
        Cerr << (TStringBuilder() << "TEvUndelivered Cookie# " << ev->Cookie << Endl);
        if (const auto it = InFlight.find(ev->Cookie); it != InFlight.end()) {
            auto& [s2cIt, hash] = it->second;
            Tentative.emplace(it->first, hash);
            SessionToCookie.erase(s2cIt);
            InFlight.erase(it);
            IssueQueries();
        }
    }

    STRICT_STFUNC(StateFunc,
        fFunc(TEvents::THelloWorld::Pong, HandlePong);
        hFunc(TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        cFunc(TEvents::TSystem::Wakeup, Subscribe);
    )
};

class TRecipientActor : public TActor<TRecipientActor> {
public:
    TRecipientActor()
        : TActor(&TThis::StateFunc)
        , Received(0)
    {}

    void HandlePing(TAutoPtr<IEventHandle>& ev) {
        const TString& data = ev->GetChainBuffer()->GetString();
        const TString& response = MD5::CalcRaw(data);
        TActivationContext::Send(new IEventHandle(TEvents::THelloWorld::Pong, 0, ev->Sender, SelfId(),
            MakeIntrusive<TEventSerializedData>(response, TEventSerializationInfo{}), ev->Cookie));
        Received.fetch_add(1, std::memory_order_relaxed);
    }

    size_t GetReceived() const noexcept {
        return Received.load(std::memory_order_relaxed);
    }

    STRICT_STFUNC(StateFunc,
        fFunc(TEvents::THelloWorld::Ping, HandlePing);
    )
private:
    std::atomic<size_t> Received;
};

Y_UNIT_TEST_SUITE(Interconnect) {

<<<<<<< HEAD
=======
    Y_UNIT_TEST(ProcessUndeliveredAfterOversizedTcpEvent) {
        TTestICCluster cluster(2, TChannelsConfig(), nullptr, nullptr, TTestICCluster::DISABLE_RDMA);

        auto context = std::make_shared<TOversizedTcpEventContext>();
        const TActorId recipient = cluster.RegisterActor(new TOversizedTcpEventReceiverActor(context), 1);

        auto event = std::make_unique<TEvOversizedTcpEvent>();
        // TEvTestSerialization.Buffer is encoded as a one-byte field tag, a four-byte varint length at
        // this payload size, and the payload itself. Therefore its serialized size is:
        //
        //   1 + 4 + (EventMaxByteSize - 4) = EventMaxByteSize + 1.
        //
        // Exceeding the limit by exactly one byte makes the coroutine request more output after consuming
        // the complete serialization budget. The size check must terminate the session while the coroutine
        // is suspended and ProcessUndelivered must abort the pending serialization.
        event->Record.SetBuffer(TString(EventMaxByteSize - 4, 'x'));
        UNIT_ASSERT_VALUES_EQUAL(event->CalculateSerializedSize(), EventMaxByteSize + 1);

        cluster.RegisterActor(new TOversizedTcpEventSenderActor(recipient, std::move(event), context), 2);

        WaitForCondition(TDuration::Seconds(60), [&] {
            return context->Undelivered.load(std::memory_order_acquire)
                || context->Received.load(std::memory_order_acquire);
        }, "oversized TCP event result");

        UNIT_ASSERT(context->Undelivered.load(std::memory_order_acquire));
        UNIT_ASSERT(!context->Received.load(std::memory_order_acquire));

        auto regular = std::make_unique<TEvOversizedTcpEvent>();
        regular->Record.SetBuffer("after oversized event");
        cluster.RegisterActor(new TSingleEventSenderActor(recipient, regular.release()), 2);

        WaitForCondition(TDuration::Seconds(60), [&] {
            return context->Received.load(std::memory_order_acquire);
        }, "regular TCP event delivery after oversized event");
    }

    Y_UNIT_TEST(ScopeClassCountersRebindPeerLabel) {
        RunScopeClassCounterRebindTest(TScopeId(0, 1), "system");
        RunScopeClassCounterRebindTest(TScopeId(1, 42), "same_tenant");
        RunScopeClassCounterRebindTest(TScopeId(2, 42), "other_tenant");
    }

    Y_UNIT_TEST(SubscriberLivenessCheck) {
        RunSubscriberLivenessCheck(false, TDuration::MilliSeconds(100));
    }

    Y_UNIT_TEST(SubscriberLivenessCheckV2) {
        RunSubscriberLivenessCheck(true, TDuration::MilliSeconds(100));
    }

    Y_UNIT_TEST(SubscriberLivenessCheckDisabled) {
        RunSubscriberLivenessCheck(false, TDuration::Zero());
    }

    Y_UNIT_TEST(SubscriberLivenessCheckDisabledV2) {
        RunSubscriberLivenessCheck(true, TDuration::Zero());
    }

    Y_UNIT_TEST(RdmaRetryWatchdogPendingSessionsAggregated) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        auto common = MakeIntrusive<TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();

        const TActorId aggregator = runtime.Register(
            NInterconnectMetricsAggregator::CreateInterconnectMetricsAggregatorActor(common));
        const TActorId sender = runtime.AllocateEdgeActor();

        auto getPendingSessions = [&]() -> ui64 {
            const auto peerCounters = common->MonCounters->FindSubgroup("peer", "rack-a");
            if (!peerCounters) {
                return ui64(0);
            }
            const auto counter = peerCounters->FindCounter("RdmaRetryWatchdogPendingSessions");
            return counter ? ui64(counter->Val()) : ui64(0);
        };

        auto send = [&](IEventBase* event) {
            runtime.Send(new IEventHandle(aggregator, sender, event), 0, true);
        };

        auto waitForPendingSessions = [&](ui64 expected) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() -> bool {
                return getPendingSessions() == expected;
            };
            options.Quiet = true;
            UNIT_ASSERT_C(runtime.DispatchEvents(options, TDuration::Seconds(1)),
                "last RDMA retry watchdog pending sessions: " << getPendingSessions());
        };

        send(new NInterconnectMetricsAggregator::TEvRegisterPeer("rack-a", "peer-1"));
        send(new NInterconnectMetricsAggregator::TEvUpdateRdmaRetryWatchdogPending("rack-a", "peer-1", 1));
        waitForPendingSessions(1);
        UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "rack-a", "RdmaRetryWatchdogPendingSessions"), 1);

        send(new NInterconnectMetricsAggregator::TEvRegisterPeer("rack-a", "peer-2"));
        send(new NInterconnectMetricsAggregator::TEvUpdateRdmaRetryWatchdogPending("rack-a", "peer-2", 1));
        waitForPendingSessions(2);
        UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "rack-a", "RdmaRetryWatchdogPendingSessions"), 2);

        send(new NInterconnectMetricsAggregator::TEvUpdateRdmaRetryWatchdogPending("rack-a", "peer-1", 0));
        waitForPendingSessions(1);
        UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "rack-a", "RdmaRetryWatchdogPendingSessions"), 1);

        send(new NInterconnectMetricsAggregator::TEvUnregisterPeer("rack-a", "peer-2"));
        waitForPendingSessions(0);
        UNIT_ASSERT_VALUES_EQUAL(GetPeerCounterValue(common->MonCounters, "rack-a", "RdmaRetryWatchdogPendingSessions"), 0);
    }

>>>>>>> 9f8d5a7e15d (Fix cleanup after oversized event serialization (#48532))
    Y_UNIT_TEST(SessionContinuation) {
        TTestICCluster cluster(2);
        const TActorId recipient = cluster.RegisterActor(new TRecipientActor, 1);
        cluster.RegisterActor(new TSenderActor(recipient), 2);
        for (ui32 i = 0; i < 100; ++i) {
            const ui32 nodeId = 1 + RandomNumber(2u);
            const ui32 peerNodeId = 3 - nodeId;
            const ui32 action = RandomNumber(3u);
            auto *node = cluster.GetNode(nodeId);
            TActorId proxyId = node->InterconnectProxy(peerNodeId);

            switch (action) {
                case 0:
                    node->Send(proxyId, new TEvInterconnect::TEvClosePeerSocket);
                    Cerr << (TStringBuilder() << "nodeId# " << nodeId << " peerNodeId# " << peerNodeId
                        << " TEvClosePeerSocket" << Endl);
                    break;

                case 1:
                    node->Send(proxyId, new TEvInterconnect::TEvCloseInputSession);
                    Cerr << (TStringBuilder() << "nodeId# " << nodeId << " peerNodeId# " << peerNodeId
                        << " TEvCloseInputSession" << Endl);
                    break;

                case 2:
                    node->Send(proxyId, new TEvInterconnect::TEvPoisonSession);
                    Cerr << (TStringBuilder() << "nodeId# " << nodeId << " peerNodeId# " << peerNodeId
                        << " TEvPoisonSession" << Endl);
                    break;

                default:
                    Y_ABORT();
            }

            Sleep(TDuration::MilliSeconds(RandomNumber<ui32>(500) + 100));
        }
    }

    Y_UNIT_TEST(SetupRdmaSession) {
        if (NRdmaTest::IsRdmaTestDisabled()) {
            Cerr << "SetupRdmaSession test skipped" << Endl;
            return;
        }
        TTestICCluster cluster(2);
        const size_t limit = 10;
        auto receiverPtr = new TRecipientActor;
        const TActorId recipient = cluster.RegisterActor(receiverPtr, 1);
        auto senderPtr = new TSenderActor(recipient, limit);
        cluster.RegisterActor(senderPtr, 2);

        while (receiverPtr->GetReceived() < limit) {
            Sleep(TDuration::MilliSeconds(100));
        }

        {
            auto s = GetRdmaQpStatus(cluster, 1, 2);
            auto tokens = SplitString(s, ",");
            UNIT_ASSERT(tokens.size() > 2);
            UNIT_ASSERT(tokens[1] == "QPS_RTS");
        }

        {
            auto s = GetRdmaChecksumStatus(cluster, 2, 1);
            UNIT_ASSERT_VALUES_EQUAL(s, "On | SoftwareChecksum");
        }
    }
}
