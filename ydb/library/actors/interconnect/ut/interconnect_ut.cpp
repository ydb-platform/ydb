#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/interconnect_counters.h>
#include <ydb/library/actors/interconnect/interconnect_metrics_aggregator.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/interconnect/ut/protos/interconnect_test.pb.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/digest/md5/md5.h>
#include <atomic>
#include <cstring>
#include <memory>
#include <util/random/fast.h>
#include <util/string/cast.h>
#include <util/string/vector.h>

using namespace NActors;

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
    std::atomic<ui32> HoldByErrorNotice = 0;
    std::atomic<ui32> HoldByErrorDebug = 0;
};

class TCountingLogBackend : public TLogBackend {
public:
    explicit TCountingLogBackend(std::shared_ptr<THandshakeFailureLogCounters> outgoingHandshakeFailures)
        : OutgoingHandshakeFailures(std::move(outgoingHandshakeFailures))
    {}

    void WriteData(const TLogRecord& rec) override {
        const TStringBuf line(rec.Data, rec.Len);
        if (line.Contains("ICP25") && line.Contains("outgoing handshake failed")) {
            CountPriority(rec, OutgoingHandshakeFailures->Notice, OutgoingHandshakeFailures->Debug);
        } else if (line.Contains("ICP32") && line.Contains("transit to hold-by-error state")) {
            CountPriority(rec, OutgoingHandshakeFailures->HoldByErrorNotice, OutgoingHandshakeFailures->HoldByErrorDebug);
        }
    }

    void ReopenLog() override {
    }

private:
    static void CountPriority(const TLogRecord& rec, std::atomic<ui32>& notice, std::atomic<ui32>& debug) {
        if (rec.Priority == TLOG_NOTICE) {
            notice.fetch_add(1, std::memory_order_relaxed);
        } else if (rec.Priority == TLOG_DEBUG) {
            debug.fetch_add(1, std::memory_order_relaxed);
        }
    }

    std::shared_ptr<THandshakeFailureLogCounters> OutgoingHandshakeFailures;
};

} // namespace

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

namespace {

TTestICCluster::Flags GetKernelLivenessFlags(bool withRdma) {
    return withRdma ? TTestICCluster::EMPTY : TTestICCluster::DISABLE_RDMA;
}

bool SkipIfRdmaUnavailable(bool withRdma, TStringBuf testName) {
    if (withRdma && NRdmaTest::IsRdmaTestDisabled()) {
        Cerr << testName << " test skipped" << Endl;
        return true;
    }
    return false;
}

void WaitForRdmaQpRts(TTestICCluster& cluster, ui32 nodeId, ui32 peerNodeId, TStringBuf description) {
    WaitForCondition(TDuration::Seconds(30), [&] {
        try {
            const auto tokens = SplitString(GetRdmaQpStatus(cluster, nodeId, peerNodeId), ",");
            return tokens.size() > 2 && tokens[1] == "QPS_RTS";
        } catch (const TPatternNotFound&) {
            return false;
        }
    }, description);
}

ui64 MeasureIdleGeneratedPackets(bool enableKernelLiveness, bool withRdma) {
    auto settingsCustomizer = [enableKernelLiveness](ui32, TInterconnectSettings& settings) {
        settings.EnableKernelLiveness = enableKernelLiveness;
        settings.PingPeriod = TDuration::MilliSeconds(200);
    };

    TTestICCluster cluster(2, TChannelsConfig(), nullptr, nullptr, GetKernelLivenessFlags(withRdma),
        {}, TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto* recipientPtr = new TRecipientActor;
    const TActorId recipient = cluster.RegisterActor(recipientPtr, 1);
    cluster.RegisterActor(new TSenderActor(recipient, 1), 2);

    WaitForCondition(TDuration::Seconds(10), [&] {
        return recipientPtr->GetReceived() >= 1;
    }, "initial message delivery");

    const ui64 negotiated = WaitForSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness");
    UNIT_ASSERT_VALUES_EQUAL(negotiated, enableKernelLiveness ? 1ULL : 0ULL);

    Sleep(TDuration::Seconds(1));
    const ui64 packetsBefore = WaitForSessionCounter(cluster, 2, 1, "PacketsGenerated");
    Sleep(TDuration::Seconds(4));
    const ui64 packetsAfter = WaitForSessionCounter(cluster, 2, 1, "PacketsGenerated");
    UNIT_ASSERT_C(packetsAfter >= packetsBefore, "PacketsGenerated counter regressed while measuring idle traffic");
    return packetsAfter - packetsBefore;
}

void RunKernelLivenessMixedConfigAsymmetric(bool withRdma, ui32 kernelLivenessNodeId) {
    if (SkipIfRdmaUnavailable(withRdma, "KernelLivenessMixedConfigAsymmetricRdma")) {
        return;
    }

    auto settingsCustomizer = [kernelLivenessNodeId](ui32 nodeId, TInterconnectSettings& settings) {
        settings.EnableKernelLiveness = (nodeId == kernelLivenessNodeId);
        settings.PingPeriod = TDuration::MilliSeconds(200);
    };

    TTestICCluster cluster(2, TChannelsConfig(), nullptr, nullptr, GetKernelLivenessFlags(withRdma),
        {}, TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto* recipientPtr = new TRecipientActor;
    const TActorId recipient = cluster.RegisterActor(recipientPtr, 1);
    cluster.RegisterActor(new TSenderActor(recipient, 1), 2);

    WaitForCondition(TDuration::Seconds(10), [&] {
        return recipientPtr->GetReceived() >= 1;
    }, "mixed cluster initial message delivery");

    const ui64 node2Expected = kernelLivenessNodeId == 2 ? 1ULL : 0ULL;
    const ui64 node1Expected = kernelLivenessNodeId == 1 ? 1ULL : 0ULL;
    UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness"), node2Expected);
    UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 1, 2, "Params.UseKernelLiveness"), node1Expected);
}

void RunKernelLivenessSocketSetupFallback(bool withRdma) {
    if (SkipIfRdmaUnavailable(withRdma, "KernelLivenessSocketSetupFallbackRdma")) {
        return;
    }

    auto settingsCustomizer = [](ui32 nodeId, TInterconnectSettings& settings) {
        settings.EnableKernelLiveness = true;
        settings.PingPeriod = TDuration::MilliSeconds(200);
        if (nodeId == 2) {
            settings.KernelKeepAliveProbes = 0; // force local socket setup failure
        }
    };

    TTestICCluster cluster(2, TChannelsConfig(), nullptr, nullptr, GetKernelLivenessFlags(withRdma),
        {}, TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto* recipientPtr = new TRecipientActor;
    const TActorId recipient = cluster.RegisterActor(recipientPtr, 1);
    cluster.RegisterActor(new TSenderActor(recipient, 1), 2);

    WaitForCondition(TDuration::Seconds(10), [&] {
        return recipientPtr->GetReceived() >= 1;
    }, "socket-setup fallback initial message delivery");

    UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness"), 0ULL);
    UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 1, 2, "Params.UseKernelLiveness"), 1ULL);
}

void RunKernelLivenessReducesIdlePackets(bool withRdma) {
    if (SkipIfRdmaUnavailable(withRdma, "KernelLivenessReducesIdlePacketsRdma")) {
        return;
    }

    const ui64 legacyPackets = MeasureIdleGeneratedPackets(false, withRdma);
    const ui64 kernelPackets = MeasureIdleGeneratedPackets(true, withRdma);
    Cerr << "legacyPackets# " << legacyPackets << " kernelPackets# " << kernelPackets << Endl;
    UNIT_ASSERT_GT(legacyPackets, kernelPackets);
}

void RunKernelLivenessPreservesFlowControlConfirms(bool withRdma) {
    if (SkipIfRdmaUnavailable(withRdma, "KernelLivenessPreservesFlowControlConfirmsRdma")) {
        return;
    }

    constexpr size_t messages = 4000;
    constexpr size_t payloadSize = 256;

    auto settingsCustomizer = [](ui32, TInterconnectSettings& settings) {
        settings.EnableKernelLiveness = true;
        settings.PingPeriod = TDuration::MilliSeconds(200);
    };

    TTestICCluster cluster(2, TChannelsConfig(), nullptr, nullptr, GetKernelLivenessFlags(withRdma),
        {}, TDuration::Seconds(2), 64 * 1024, settingsCustomizer);

    auto* recipientPtr = new TDropRecipientActor;
    const TActorId recipient = cluster.RegisterActor(recipientPtr, 1);
    cluster.RegisterActor(new TBurstSenderActor(recipient, messages, payloadSize), 2);

    WaitForCondition(TDuration::Seconds(20), [&] {
        return recipientPtr->GetReceived() >= messages;
    }, "bulk one-way delivery in kernel liveness mode");

    UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness"), 1ULL);
    const ui64 confirmBySize = WaitForSessionCounter(cluster, 1, 2, "ConfirmPacketsForcedBySize");
    const ui64 confirmByTimeout = WaitForSessionCounter(cluster, 1, 2, "ConfirmPacketsForcedByTimeout");
    Cerr << "confirmBySize# " << confirmBySize << " confirmByTimeout# " << confirmByTimeout << Endl;
    UNIT_ASSERT_GT(confirmBySize + confirmByTimeout, 0ULL);
}

void RunKernelLivenessClockSkewPingTimeoutUpdatesMetrics(bool withRdma) {
    if (SkipIfRdmaUnavailable(withRdma, "KernelLivenessClockSkewPingTimeoutUpdatesMetricsRdma")) {
        return;
    }

    auto settingsCustomizer = [](ui32, TInterconnectSettings& settings) {
        settings.EnableKernelLiveness = true;
        settings.ClockSkewPingTimeout = TDuration::MilliSeconds(200);
        settings.PingPeriod = TDuration::Seconds(30);
    };

    TTestICCluster cluster(2, TChannelsConfig(), nullptr, nullptr, GetKernelLivenessFlags(withRdma),
        {}, TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto* recipientPtr = new TRecipientActor;
    const TActorId recipient = cluster.RegisterActor(recipientPtr, 1);
    cluster.RegisterActor(new TSenderActor(recipient, 1), 2);

    WaitForCondition(TDuration::Seconds(10), [&] {
        return recipientPtr->GetReceived() >= 1;
    }, "initial message delivery for clock-skew metrics");

    UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness"), 1ULL);

    WaitForCondition(TDuration::Seconds(10), [&] {
        try {
            return GetSessionDurationMetric(cluster, 2, 1, "GetPingRTT()") > TDuration::Zero();
        } catch (const TPatternNotFound&) {
            return false;
        }
    }, "kernel-liveness ping RTT metric updated");

    const TDuration pingRtt = GetSessionDurationMetric(cluster, 2, 1, "GetPingRTT()");
    const TDuration sinceLastPing = GetSessionDurationMetric(cluster, 2, 1, "now - LastPingTimestamp");
    const i64 clockSkewUs = GetSessionSignedDurationMetricUs(cluster, 2, 1, "clockSkew");
    Cerr << "pingRtt# " << pingRtt << " sinceLastPing# " << sinceLastPing << " clockSkewUs# " << clockSkewUs << Endl;

    UNIT_ASSERT_GT(pingRtt, TDuration::Zero());
    UNIT_ASSERT_LT(sinceLastPing, TDuration::Seconds(2));
}

void RunKernelLivenessReconnectLocalFallbackNotApplied(bool withRdma) {
    if (SkipIfRdmaUnavailable(withRdma, "KernelLivenessReconnectLocalFallbackNotAppliedRdma")) {
        return;
    }

    auto settingsCustomizer = [](ui32, TInterconnectSettings& settings) {
        settings.EnableKernelLiveness = true;
        settings.PingPeriod = TDuration::MilliSeconds(200);
    };

    TTestICCluster cluster(2, TChannelsConfig(), nullptr, nullptr, GetKernelLivenessFlags(withRdma),
        {}, TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto* recipientPtr = new TRecipientActor;
    const TActorId recipient = cluster.RegisterActor(recipientPtr, 1);
    cluster.RegisterActor(new TSenderActor(recipient), 2);

    WaitForCondition(TDuration::Seconds(10), [&] {
        return recipientPtr->GetReceived() >= 1;
    }, "initial message delivery for reconnect fallback");

    auto waitKernelMode = [&](ui64 expected, TStringBuf description) {
        WaitForCondition(TDuration::Seconds(20), [&] {
            try {
                return GetSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness") == expected;
            } catch (const TPatternNotFound&) {
                return false;
            }
        }, description);
    };

    auto reconnectFromNode2 = [&](TStringBuf description) {
        const TString handshakeBefore = GetSessionTextMetric(cluster, 2, 1, "LastHandshakeDone");
        WaitForCondition(TDuration::Seconds(10), [&] {
            try {
                return GetSessionCounter(cluster, 2, 1, "NumEventsInQueue") > 0;
            } catch (const TPatternNotFound&) {
                return false;
            }
        }, "session has pending output before forced reconnect");
        cluster.GetNode(2)->Send(cluster.InterconnectProxy(1, 2), new TEvInterconnect::TEvClosePeerSocket);
        WaitForCondition(TDuration::Seconds(20), [&] {
            try {
                return GetSessionTextMetric(cluster, 2, 1, "LastHandshakeDone") != handshakeBefore &&
                    GetSessionSocketFd(cluster, 2, 1) >= 0;
            } catch (const TPatternNotFound&) {
                return false;
            } catch (const TFromStringException&) {
                return false;
            }
        }, description);
    };

    waitKernelMode(1ULL, "initial kernel liveness negotiated");

    if (withRdma) {
        const TString createdBefore = GetSessionTextMetric(cluster, 2, 1, "Created");

        // RDMA sessions must not use same-session continuation. A reconnect request should replace the old session
        // and negotiate transport params from current local settings through a fresh initial handshake.
        cluster.GetNode(2)->MutableInterconnectSettings().EnableKernelLiveness = false;
        reconnectFromNode2("RDMA session recreated with local kernel liveness disabled");

        const TString createdAfter = GetSessionTextMetric(cluster, 2, 1, "Created");
        UNIT_ASSERT_VALUES_UNEQUAL(createdAfter, createdBefore);
        UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness"), 0ULL);
        return;
    }

    bool exercisedSameSessionContinuation = false;
    for (ui32 attempt = 0; attempt < 5; ++attempt) {
        const TString createdBefore = GetSessionTextMetric(cluster, 2, 1, "Created");

        // Force local fallback on reconnect.
        cluster.GetNode(2)->MutableInterconnectSettings().EnableKernelLiveness = false;
        reconnectFromNode2("session reconnected with local kernel liveness disabled");

        const TString createdAfter = GetSessionTextMetric(cluster, 2, 1, "Created");
        if (createdAfter == createdBefore) {
            exercisedSameSessionContinuation = true;
            break;
        }

        // Session was recreated while local kernel mode was disabled, so this new session template now carries
        // UseKernelLiveness=false. Force another session recreation after restoring local settings to avoid
        // continuation within the stale template.
        cluster.GetNode(2)->MutableInterconnectSettings().EnableKernelLiveness = true;
        const TString restoreCreatedBefore = GetSessionTextMetric(cluster, 2, 1, "Created");
        cluster.GetNode(2)->Send(cluster.InterconnectProxy(1, 2), new TEvInterconnect::TEvPoisonSession);
        WaitForCondition(TDuration::Seconds(20), [&] {
            try {
                return GetSessionTextMetric(cluster, 2, 1, "Created") != restoreCreatedBefore &&
                    GetSessionSocketFd(cluster, 2, 1) >= 0;
            } catch (const TPatternNotFound&) {
                return false;
            } catch (const TFromStringException&) {
                return false;
            }
        }, "restore baseline session after forced recreation");
        waitKernelMode(1ULL, "baseline kernel liveness restored");
    }

    UNIT_ASSERT_C(exercisedSameSessionContinuation,
        "failed to exercise continuation within the same session instance");

    // Expected behavior: resumed continuation should apply local fallback and disable kernel liveness.
    // Buggy behavior: existing session keeps stale Params.UseKernelLiveness from initial connect.
    UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness"), 0ULL);
}

} // namespace

Y_UNIT_TEST_SUITE(Interconnect) {

    Y_UNIT_TEST(ScopeClassCountersRebindPeerLabel) {
        RunScopeClassCounterRebindTest(TScopeId(0, 1), "system");
        RunScopeClassCounterRebindTest(TScopeId(1, 42), "same_tenant");
        RunScopeClassCounterRebindTest(TScopeId(2, 42), "other_tenant");
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

        auto handshakeHistogram1 = cluster.GetCounters()->GetSubgroup("nodeId", "1")->FindHistogram("HandshakeActorCreateUs");
        auto handshakeHistogram2 = cluster.GetCounters()->GetSubgroup("nodeId", "2")->FindHistogram("HandshakeActorCreateUs");
        UNIT_ASSERT_C(handshakeHistogram1 || handshakeHistogram2, "HandshakeActorCreateUs histogram was not created");

        WaitForCondition(TDuration::Seconds(10), [&] {
            return (handshakeHistogram1 ? GetHistogramSamples(handshakeHistogram1) : 0)
                + (handshakeHistogram2 ? GetHistogramSamples(handshakeHistogram2) : 0) > 0;
        }, "handshake actor create histogram has samples");

        auto histogram = cluster.GetCounters()->FindHistogram("PollerSyncOperationTimeUs");
        UNIT_ASSERT_C(histogram, "PollerSyncOperationTimeUs histogram was not created");

        WaitForCondition(TDuration::Seconds(10), [&] {
            return GetHistogramSamples(histogram) > 0;
        }, "poller sync operation histogram has samples");
    }

    // Scenario: a TCP XDC payload is partially read, the input session is closed without dropping the output session,
    // and replay must use the saved XDC catch buffer to finish the event exactly once.
    Y_UNIT_TEST(TcpXdcCatchReplayAfterPartialPayloadRead) {
        RunXdcCatchReplayAfterPartialPayloadRead(EXdcCatchReplayMode::Tcp);
    }

    // Scenario: RDMA section declarations are already applied to the receive context, but no RDMA_READ has been
    // scheduled yet. RDMA sessions must not use graceful reconnect here; the old receive session is replaced instead
    // of replaying serialized RDMA commands across reconnect.
    Y_UNIT_TEST(RdmaXdcCatchReplayAfterPartialPayloadRead) {
        if (SkipIfRdmaUnavailable(true, "RdmaXdcCatchReplayAfterPartialPayloadRead")) {
            return;
        }
        RunXdcCatchReplayAfterPartialPayloadRead(EXdcCatchReplayMode::Rdma);
    }

    // Scenario: at least one RDMA_READ was already scheduled before reconnect, so the pending event's RDMA buffer
    // cursor may have moved. RDMA sessions must use a fresh session instead of attempting graceful replay with stale
    // RDMA state.
    Y_UNIT_TEST(RdmaXdcCatchReplayAfterPartialRdmaRead) {
        if (SkipIfRdmaUnavailable(true, "RdmaXdcCatchReplayAfterPartialRdmaRead")) {
            return;
        }
        RunRdmaXdcCatchReplayAfterPartialRdmaRead(EXdcCatchReplayReconnectAction::CloseInputSession);
    }

    // Scenario: at least one RDMA_READ was already scheduled, then the interconnect socket is closed through the
    // debug API. RDMA sessions must reject graceful continuation and replace the old receive session instead of
    // replaying stale RDMA state.
    Y_UNIT_TEST(RdmaXdcCatchReplayAfterPartialRdmaReadOnPeerSocketClose) {
        if (SkipIfRdmaUnavailable(true, "RdmaXdcCatchReplayAfterPartialRdmaReadOnPeerSocketClose")) {
            return;
        }
        RunRdmaXdcCatchReplayAfterPartialRdmaRead(EXdcCatchReplayReconnectAction::ClosePeerSocket);
    }

    // Scenario: both peers close the TCP control socket of an established RDMA interconnect session at nearly the
    // same time. Both old RDMA sessions must be replaced by fresh handshakes instead of bouncing continuation rejects.
    Y_UNIT_TEST(RdmaSimultaneousReconnectDoesNotLoop) {
        if (SkipIfRdmaUnavailable(true, "RdmaSimultaneousReconnectDoesNotLoop")) {
            return;
        }

        TTestICCluster::TTrafficInterrupterSettings interrupterSettings{
            .RejectingTrafficTimeout = TDuration::Zero(),
            .BandWidth = 0.0,
            .Disconnect = false,
        };
        TTestICCluster cluster(2, TChannelsConfig(), &interrupterSettings);

        auto* recipientOnNode1Ptr = new TRecipientActor;
        const TActorId recipientOnNode1 = cluster.RegisterActor(recipientOnNode1Ptr, 1);
        auto* recipientOnNode2Ptr = new TRecipientActor;
        const TActorId recipientOnNode2 = cluster.RegisterActor(recipientOnNode2Ptr, 2);

        cluster.RegisterActor(new TSenderActor(recipientOnNode1, 1), 2);
        cluster.RegisterActor(new TSenderActor(recipientOnNode2, 1), 1);

        WaitForCondition(TDuration::Seconds(10), [&] {
            return recipientOnNode1Ptr->GetReceived() >= 1
                && recipientOnNode2Ptr->GetReceived() >= 1;
        }, "initial bidirectional RDMA delivery");

        WaitForRdmaQpRts(cluster, 1, 2, "initial RDMA session 1->2 is RTS");
        WaitForRdmaQpRts(cluster, 2, 1, "initial RDMA session 2->1 is RTS");

        const TString created12Before = GetSessionTextMetric(cluster, 1, 2, "Created");
        const TString created21Before = GetSessionTextMetric(cluster, 2, 1, "Created");

        const TActorId dropRecipientOnNode1 = cluster.RegisterActor(new TDropRecipientActor, 1);
        const TActorId dropRecipientOnNode2 = cluster.RegisterActor(new TDropRecipientActor, 2);

        auto sessionStaysAliveOnEof = [&](ui32 fromNode, ui32 toNode) {
            const ui64 numEventsInQueue = GetSessionCounter(cluster, fromNode, toNode, "NumEventsInQueue");
            const ui64 outputCounter = GetSessionCounter(cluster, fromNode, toNode, "OutputCounter");
            const ui64 lastConfirmed = GetSessionCounter(cluster, fromNode, toNode, "LastConfirmed");
            return numEventsInQueue > 0 || outputCounter != lastConfirmed;
        };

        cluster.StartBlackhole(1);
        cluster.StartBlackhole(2);
        cluster.RegisterActor(new TBurstSenderActor(dropRecipientOnNode1, 4096, 4096), 2);
        cluster.RegisterActor(new TBurstSenderActor(dropRecipientOnNode2, 4096, 4096), 1);

        WaitForCondition(TDuration::Seconds(10), [&] {
            try {
                return sessionStaysAliveOnEof(1, 2) && sessionStaysAliveOnEof(2, 1);
            } catch (const TPatternNotFound&) {
                return false;
            }
        }, "both RDMA sessions have pending traffic while proxy forwarding is frozen");

        cluster.GetNode(1)->Send(cluster.InterconnectProxy(2, 1), new TEvInterconnect::TEvClosePeerSocket);
        cluster.GetNode(2)->Send(cluster.InterconnectProxy(1, 2), new TEvInterconnect::TEvClosePeerSocket);

        Sleep(TDuration::MilliSeconds(100));
        cluster.StopBlackhole(1);
        cluster.StopBlackhole(2);

        WaitForCondition(TDuration::Seconds(30), [&] {
            try {
                return GetSessionTextMetric(cluster, 1, 2, "Created") != created12Before
                    && GetSessionTextMetric(cluster, 2, 1, "Created") != created21Before
                    && GetSessionSocketFd(cluster, 1, 2) >= 0
                    && GetSessionSocketFd(cluster, 2, 1) >= 0;
            } catch (const TPatternNotFound&) {
                return false;
            } catch (const TFromStringException&) {
                return false;
            }
        }, "simultaneous RDMA reconnect replaced both sessions");

        WaitForRdmaQpRts(cluster, 1, 2, "reconnected RDMA session 1->2 is RTS");
        WaitForRdmaQpRts(cluster, 2, 1, "reconnected RDMA session 2->1 is RTS");

        const size_t receivedOnNode1Before = recipientOnNode1Ptr->GetReceived();
        const size_t receivedOnNode2Before = recipientOnNode2Ptr->GetReceived();
        cluster.RegisterActor(new TSenderActor(recipientOnNode1, 1), 2);
        cluster.RegisterActor(new TSenderActor(recipientOnNode2, 1), 1);

        WaitForCondition(TDuration::Seconds(20), [&] {
            return recipientOnNode1Ptr->GetReceived() > receivedOnNode1Before
                && recipientOnNode2Ptr->GetReceived() > receivedOnNode2Before;
        }, "bidirectional delivery after simultaneous RDMA reconnect");
    }

    Y_UNIT_TEST(KernelLivenessMixedConfigFallback) {
        RunKernelLivenessMixedConfigAsymmetric(false, 2);
    }

    Y_UNIT_TEST(KernelLivenessMixedConfigFallbackReverse) {
        RunKernelLivenessMixedConfigAsymmetric(false, 1);
    }

    Y_UNIT_TEST(KernelLivenessSocketSetupFallback) {
        RunKernelLivenessSocketSetupFallback(false);
    }

    Y_UNIT_TEST(KernelLivenessReducesIdlePackets) {
        RunKernelLivenessReducesIdlePackets(false);
    }

    Y_UNIT_TEST(KernelLivenessPreservesFlowControlConfirms) {
        RunKernelLivenessPreservesFlowControlConfirms(false);
    }

    Y_UNIT_TEST(KernelLivenessClockSkewPingTimeoutUpdatesMetrics) {
        RunKernelLivenessClockSkewPingTimeoutUpdatesMetrics(false);
    }

    Y_UNIT_TEST(KernelLivenessMixedConfigFallbackRdma) {
        RunKernelLivenessMixedConfigAsymmetric(true, 2);
    }

    Y_UNIT_TEST(KernelLivenessMixedConfigFallbackReverseRdma) {
        RunKernelLivenessMixedConfigAsymmetric(true, 1);
    }

    Y_UNIT_TEST(KernelLivenessSocketSetupFallbackRdma) {
        RunKernelLivenessSocketSetupFallback(true);
    }

    Y_UNIT_TEST(KernelLivenessReducesIdlePacketsRdma) {
        RunKernelLivenessReducesIdlePackets(true);
    }

    Y_UNIT_TEST(KernelLivenessPreservesFlowControlConfirmsRdma) {
        RunKernelLivenessPreservesFlowControlConfirms(true);
    }

    Y_UNIT_TEST(KernelLivenessClockSkewPingTimeoutUpdatesMetricsRdma) {
        RunKernelLivenessClockSkewPingTimeoutUpdatesMetrics(true);
    }

    Y_UNIT_TEST(KernelLivenessReconnectLocalFallbackNotApplied) {
        RunKernelLivenessReconnectLocalFallbackNotApplied(false);
    }

    Y_UNIT_TEST(KernelLivenessReconnectLocalFallbackNotAppliedRdma) {
        RunKernelLivenessReconnectLocalFallbackNotApplied(true);
    }

    Y_UNIT_TEST(NumEventsInQueueHistogram) {
        constexpr size_t messages = 32;
        constexpr size_t payloadSize = 64;

        TTestICCluster cluster(2, TChannelsConfig(), nullptr, nullptr, TTestICCluster::DISABLE_RDMA);

        auto* recipientPtr = new TDropRecipientActor;
        const TActorId recipient = cluster.RegisterActor(recipientPtr, 1);
        cluster.RegisterActor(new TBurstSenderActor(recipient, messages, payloadSize), 2);

        WaitForCondition(TDuration::Seconds(10), [&] {
            return recipientPtr->GetReceived() >= messages;
        }, "one-way delivery for NumEventsInQueue histogram");

        auto nodeCounters = cluster.GetCounters()->FindSubgroup("nodeId", "2");
        UNIT_ASSERT_C(nodeCounters, "nodeId=2 counters were not created");

        TString peerLabel;
        nodeCounters->EnumerateSubgroups([&](const TString& name, const TString& value) {
            if (name == "peer") {
                UNIT_ASSERT_C(peerLabel.empty(), "more than one peer counters subgroup");
                peerLabel = value;
            }
        });
        UNIT_ASSERT_C(!peerLabel.empty(), "peer counters were not created");
        auto peerCounters = nodeCounters->FindSubgroup("peer", peerLabel);

        UNIT_ASSERT_C(peerCounters, "peer counters were not created");

        auto histogram = peerCounters->FindHistogram("NumEventsInQueue");
        UNIT_ASSERT_C(histogram, "NumEventsInQueue histogram was not created");
        UNIT_ASSERT_GE(GetHistogramSamples(histogram), messages);
        UNIT_ASSERT_GT(GetHistogramBucketSamples(histogram, 0), 0ULL);
    }

    Y_UNIT_TEST(UnavailableNodeOutgoingHandshakeLogCount) {
        auto outgoingHandshakeFailures = std::make_shared<THandshakeFailureLogCounters>();
        auto settingsCustomizer = [](ui32, TInterconnectSettings& settings) {
            settings.Handshake = TDuration::Seconds(1);
            settings.FirstErrorSleep = TDuration::MilliSeconds(10);
            settings.MaxErrorSleep = TDuration::Seconds(1);
            settings.ErrorSleepRetryMultiplier = 4.0;
        };
        auto loggerSettings = MakeIntrusive<NLog::TSettings>(
            TActorId(0, "logger"),
            static_cast<NLog::EComponent>(NActorsServices::LOGGER),
            NLog::PRI_DEBUG,
            NLog::PRI_DEBUG,
            0U);
        loggerSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name);
        loggerSettings->SetAllowDrop(false);
        loggerSettings->SetThrottleDelay(TDuration::Zero());
        auto logBackendFactory = [outgoingHandshakeFailures] {
            return TAutoPtr<TLogBackend>(new TCountingLogBackend(outgoingHandshakeFailures));
        };

        TTestICCluster cluster(2, TChannelsConfig(), nullptr, loggerSettings, TTestICCluster::DISABLE_RDMA,
            {}, TDuration::Seconds(2), TNode::DefaultInflight(), settingsCustomizer, logBackendFactory);

        auto* recipientPtr = new TRecipientActor;
        const TActorId recipient = cluster.RegisterActor(recipientPtr, 1);
        cluster.RegisterActor(new TSenderActor(recipient), 2);

        WaitForCondition(TDuration::Seconds(10), [&] {
            return recipientPtr->GetReceived() >= 1;
        }, "initial interconnect message delivery");

        outgoingHandshakeFailures->Notice.store(0, std::memory_order_relaxed);
        outgoingHandshakeFailures->Debug.store(0, std::memory_order_relaxed);
        outgoingHandshakeFailures->HoldByErrorNotice.store(0, std::memory_order_relaxed);
        outgoingHandshakeFailures->HoldByErrorDebug.store(0, std::memory_order_relaxed);
        cluster.StopNode(1);
        Sleep(TDuration::Seconds(10));

        const ui32 noticeLogCount = outgoingHandshakeFailures->Notice.load(std::memory_order_relaxed);
        const ui32 debugLogCount = outgoingHandshakeFailures->Debug.load(std::memory_order_relaxed);
        UNIT_ASSERT_C(noticeLogCount == 1 || noticeLogCount == 2,
            TStringBuilder() << "expected one or two notice-level ICP25 outgoing handshake failure log records in 10 seconds, got "
                << noticeLogCount);
        UNIT_ASSERT_C(debugLogCount > 0,
            "expected repeated ICP25 outgoing handshake failure log records to be demoted to debug");

        const ui32 holdByErrorNoticeLogCount = outgoingHandshakeFailures->HoldByErrorNotice.load(std::memory_order_relaxed);
        const ui32 holdByErrorDebugLogCount = outgoingHandshakeFailures->HoldByErrorDebug.load(std::memory_order_relaxed);
        UNIT_ASSERT_C(holdByErrorNoticeLogCount == 1 || holdByErrorNoticeLogCount == 2,
            TStringBuilder() << "expected one or two notice-level ICP32 hold-by-error transition log records in 10 seconds, got "
                << holdByErrorNoticeLogCount);
        UNIT_ASSERT_C(holdByErrorDebugLogCount > 0,
            "expected repeated ICP32 hold-by-error transition log records to be demoted to debug");
    }

    Y_UNIT_TEST(SetupRdmaSession) {
        if (NRdmaTest::IsRdmaTestDisabled()) {
            Cerr << "SetupRdmaSession test skipped" << Endl;
            return;
        }
        TTestICCluster cluster(2);
        const size_t limit = 10;
        auto recieverPtr = new TRecipientActor;
        const TActorId recipient = cluster.RegisterActor(recieverPtr, 1);
        auto senderPtr = new TSenderActor(recipient, limit);
        cluster.RegisterActor(senderPtr, 2);

        while (recieverPtr->GetReceived() < limit) {
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
