#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>
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
        cluster.StopNode(1);
        Sleep(TDuration::Seconds(10));

        const ui32 noticeLogCount = outgoingHandshakeFailures->Notice.load(std::memory_order_relaxed);
        const ui32 debugLogCount = outgoingHandshakeFailures->Debug.load(std::memory_order_relaxed);
        UNIT_ASSERT_VALUES_EQUAL_C(noticeLogCount, 1,
            "expected exactly one notice-level ICP25 outgoing handshake failure log record in 10 seconds");
        UNIT_ASSERT_C(debugLogCount > 0,
            "expected repeated ICP25 outgoing handshake failure log records to be demoted to debug");
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
