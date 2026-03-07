#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cstring>
#include <limits>

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

using namespace NActors;

namespace {

ui64 GetSessionCounter(TTestICCluster& cluster, ui32 me, ui32 peer, TStringBuf name) {
    const TString start = TStringBuilder() << "<tr><td>" << name << "</td><td>";
    return FromString<ui64>(ExtractPattern(cluster, me, peer, start, "<"));
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

i64 GetSessionSocketFd(TTestICCluster& cluster, ui32 me, ui32 peer) {
    return FromString<i64>(ExtractPattern(cluster, me, peer, "<tr><td>Socket</td><td>", "<"));
}

i64 TryGetSessionSocketFd(TTestICCluster& cluster, ui32 me, ui32 peer) {
    try {
        return GetSessionSocketFd(cluster, me, peer);
    } catch (const TPatternNotFound&) {
        return -1;
    }
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
    void HandlePing(TAutoPtr<IEventHandle>& ev) {
        Received.fetch_add(1, std::memory_order_relaxed);
        TActivationContext::Send(new IEventHandle(TEvents::THelloWorld::Pong, 0, ev->Sender, SelfId(), nullptr, ev->Cookie));
    }

    STRICT_STFUNC(StateFunc,
        fFunc(TEvents::THelloWorld::Ping, HandlePing);
    )

private:
    std::atomic<size_t> Received = 0;
};

class TBlackholeFloodSenderActor : public TActorBootstrapped<TBlackholeFloodSenderActor> {
public:
    TBlackholeFloodSenderActor(TActorId recipient, size_t messages, size_t payloadSize)
        : Recipient(recipient)
        , Messages(messages)
        , CookieState(messages + 1, 0)
        , Payload(TString::Uninitialized(payloadSize))
    {
        memset(Payload.Detach(), 'x', Payload.size());
    }

    bool IsConnected() const noexcept {
        return Connected.load(std::memory_order_relaxed);
    }

    size_t GetSent() const noexcept {
        return Sent.load(std::memory_order_relaxed);
    }

    size_t GetConnects() const noexcept {
        return Connects.load(std::memory_order_relaxed);
    }

    size_t GetUndelivered() const noexcept {
        return Undelivered.load(std::memory_order_relaxed);
    }

    size_t GetDelivered() const noexcept {
        return Delivered.load(std::memory_order_relaxed);
    }

    size_t GetObservedUnion() const noexcept {
        return ObservedUnion.load(std::memory_order_relaxed);
    }

    size_t GetDisconnects() const noexcept {
        return Disconnects.load(std::memory_order_relaxed);
    }

    TDuration GetFirstUndeliveredDelay() const noexcept {
        const ui64 start = FloodStartedAtUs.load(std::memory_order_relaxed);
        const ui64 first = FirstUndeliveredAtUs.load(std::memory_order_relaxed);
        if (!start || !first || first < start) {
            return TDuration::Zero();
        }
        return TDuration::MicroSeconds(first - start);
    }

    TDuration GetFirstDisconnectDelay() const noexcept {
        const ui64 start = FloodStartedAtUs.load(std::memory_order_relaxed);
        const ui64 first = FirstDisconnectAtUs.load(std::memory_order_relaxed);
        if (!start || !first || first < start) {
            return TDuration::Zero();
        }
        return TDuration::MicroSeconds(first - start);
    }

    void Bootstrap() {
        Become(&TThis::StateFunc);
        Send(TActivationContext::InterconnectProxy(Recipient.NodeId()), new TEvents::TEvSubscribe);
    }

private:
    static constexpr ui8 DeliveredBit = 1u << 0;
    static constexpr ui8 UndeliveredBit = 1u << 1;

    void MarkCookie(ui64 cookie, ui8 bit, std::atomic<size_t>& metric) {
        Y_ABORT_UNLESS(0 < cookie && cookie <= Messages);
        ui8& state = CookieState[cookie];
        if (state & bit) {
            return;
        }
        if (!state) {
            ObservedUnion.fetch_add(1, std::memory_order_relaxed);
        }
        state |= bit;
        metric.fetch_add(1, std::memory_order_relaxed);
    }

    void HandleNodeConnected(TEvInterconnect::TEvNodeConnected::TPtr&) {
        Connected.store(true, std::memory_order_relaxed);
        Connects.fetch_add(1, std::memory_order_relaxed);
    }

    void HandleNodeDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        Connected.store(false, std::memory_order_relaxed);
        Disconnects.fetch_add(1, std::memory_order_relaxed);
        const ui64 nowUs = TInstant::Now().MicroSeconds();
        ui64 expected = 0;
        FirstDisconnectAtUs.compare_exchange_strong(expected, nowUs, std::memory_order_relaxed);
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
        MarkCookie(ev->Cookie, UndeliveredBit, Undelivered);
        const ui64 nowUs = TInstant::Now().MicroSeconds();
        ui64 expected = 0;
        FirstUndeliveredAtUs.compare_exchange_strong(expected, nowUs, std::memory_order_relaxed);
    }

    void HandlePong(TAutoPtr<IEventHandle>& ev) {
        MarkCookie(ev->Cookie, DeliveredBit, Delivered);
    }

    void HandleStartFlood() {
        if (FloodStarted) {
            return;
        }
        FloodStarted = true;
        FloodStartedAtUs.store(TInstant::Now().MicroSeconds(), std::memory_order_relaxed);
        const ui32 flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagGenerateUnsureUndelivered;
        for (size_t i = 0; i < Messages; ++i) {
            TActivationContext::Send(new IEventHandle(TEvents::THelloWorld::Ping, flags, Recipient,
                SelfId(), MakeIntrusive<TEventSerializedData>(TString(Payload), TEventSerializationInfo{}), i + 1));
            Sent.fetch_add(1, std::memory_order_relaxed);
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvInterconnect::TEvNodeConnected, HandleNodeConnected)
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleNodeDisconnected)
        hFunc(TEvents::TEvUndelivered, HandleUndelivered)
        fFunc(TEvents::THelloWorld::Pong, HandlePong)
        cFunc(TEvents::TSystem::Wakeup, HandleStartFlood)
    )

private:
    const TActorId Recipient;
    const size_t Messages;
    TVector<ui8> CookieState;
    TString Payload;

    std::atomic<bool> Connected = false;
    std::atomic<size_t> Connects = 0;
    std::atomic<size_t> Sent = 0;
    std::atomic<size_t> Undelivered = 0;
    std::atomic<size_t> Delivered = 0;
    std::atomic<size_t> ObservedUnion = 0;
    std::atomic<size_t> Disconnects = 0;
    std::atomic<ui64> FloodStartedAtUs = 0;
    std::atomic<ui64> FirstUndeliveredAtUs = 0;
    std::atomic<ui64> FirstDisconnectAtUs = 0;
    bool FloodStarted = false;
};

} // namespace

Y_UNIT_TEST_SUITE(InterconnectKernelLiveness) {

    Y_UNIT_TEST(TcpUserTimeoutBlackholeDisconnectsSession) {
        constexpr size_t messages = 512;
        constexpr size_t payloadSize = 64 * 1024;
        constexpr TDuration userTimeout = TDuration::Seconds(3);

        auto settingsCustomizer = [](ui32, TInterconnectSettings& settings) {
            settings.EnableKernelLiveness = true;
            settings.KernelKeepAliveIdle = TDuration::Seconds(30);
            settings.KernelKeepAliveInterval = TDuration::Seconds(1);
            settings.KernelKeepAliveProbes = 3;
            settings.KernelUserTimeout = TDuration::Seconds(3);

            // Keep this large so that the test observes transport timeout rather than session watchdog timeout.
            settings.LostConnection = TDuration::Seconds(60);
        };

        TTestICCluster::TTrafficInterrupterSettings interrupterSettings{
            .RejectingTrafficTimeout = TDuration::Zero(),
            .BandWidth = 0.0,
            .Disconnect = false,
        };

        TTestICCluster cluster(2, TChannelsConfig(), &interrupterSettings, nullptr, TTestICCluster::DISABLE_RDMA,
            {}, TDuration::Seconds(30), 128 * 1024 * 1024, settingsCustomizer);

        auto* recipient12 = new TDropRecipientActor;
        const TActorId recipient12Id = cluster.RegisterActor(recipient12, 1);
        auto* sender12 = new TBlackholeFloodSenderActor(recipient12Id, messages, payloadSize);
        const TActorId sender12Id = cluster.RegisterActor(sender12, 2);

        auto* recipient21 = new TDropRecipientActor;
        const TActorId recipient21Id = cluster.RegisterActor(recipient21, 2);
        auto* sender21 = new TBlackholeFloodSenderActor(recipient21Id, messages, payloadSize);
        const TActorId sender21Id = cluster.RegisterActor(sender21, 1);

        WaitForCondition(TDuration::Seconds(10), [&] {
            return sender12->IsConnected() && sender21->IsConnected();
        }, "senders connected to peer");

        UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 1, 2, "Params.UseKernelLiveness"), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness"), 1ULL);

        // Freeze the proxy path to force zero-window/persist behavior on sender-side TCP.
        // Connection direction is not deterministic in tests; force blackhole on both interrupters.
        cluster.StartBlackhole(1);
        cluster.StartBlackhole(2);
        const TInstant floodStart = TInstant::Now();
        cluster.GetNode(2)->Send(sender12Id, new TEvents::TEvWakeup);
        cluster.GetNode(1)->Send(sender21Id, new TEvents::TEvWakeup);

        TDuration earliestDisconnect = TDuration::Zero();
        const TInstant deadline = floodStart + TDuration::Seconds(20);
        while (TInstant::Now() < deadline) {
            bool disconnected = false;
            try {
                disconnected = GetSessionSocketFd(cluster, 1, 2) < 0 || GetSessionSocketFd(cluster, 2, 1) < 0;
            } catch (const TPatternNotFound&) {
            }
            if (disconnected) {
                earliestDisconnect = TInstant::Now() - floodStart;
                break;
            }
            Sleep(TDuration::MilliSeconds(200));
        }

        // Reestablish path may keep session alive without immediate undelivered notifications.
        // Force session termination to flush unsure-undelivered for in-flight tracked events.
        cluster.GetNode(1)->Send(cluster.InterconnectProxy(2, 1), new TEvInterconnect::TEvPoisonSession);
        cluster.GetNode(2)->Send(cluster.InterconnectProxy(1, 2), new TEvInterconnect::TEvPoisonSession);

        WaitForCondition(TDuration::Seconds(20), [&] {
            return sender12->GetObservedUnion() == messages && sender21->GetObservedUnion() == messages;
        }, "each sent cookie is either delivered or undelivered");

        i64 socket12 = std::numeric_limits<i64>::max();
        i64 socket21 = std::numeric_limits<i64>::max();
        try {
            socket12 = GetSessionSocketFd(cluster, 1, 2);
        } catch (const TPatternNotFound&) {
        }
        try {
            socket21 = GetSessionSocketFd(cluster, 2, 1);
        } catch (const TPatternNotFound&) {
        }
        Cerr << "tcpUserTimeout# " << userTimeout
            << " earliestDisconnect# " << earliestDisconnect
            << " socket12# " << socket12
            << " socket21# " << socket21
            << " sent12# " << sender12->GetSent()
            << " sent21# " << sender21->GetSent()
            << " delivered12# " << sender12->GetDelivered()
            << " delivered21# " << sender21->GetDelivered()
            << " undelivered12# " << sender12->GetUndelivered()
            << " undelivered21# " << sender21->GetUndelivered()
            << " observed12# " << sender12->GetObservedUnion()
            << " observed21# " << sender21->GetObservedUnion()
            << " received12# " << recipient12->GetReceived()
            << " received21# " << recipient21->GetReceived()
            << "\n";

        UNIT_ASSERT_VALUES_EQUAL(sender12->GetSent(), messages);
        UNIT_ASSERT_VALUES_EQUAL(sender21->GetSent(), messages);
        UNIT_ASSERT_C(earliestDisconnect != TDuration::Zero(),
            "first disconnect delay is not captured");
        UNIT_ASSERT_C(earliestDisconnect < TDuration::Seconds(20),
            TStringBuilder() << "disconnect was too late, delay# " << earliestDisconnect);
        UNIT_ASSERT_VALUES_EQUAL(sender12->GetObservedUnion(), messages);
        UNIT_ASSERT_VALUES_EQUAL(sender21->GetObservedUnion(), messages);
        UNIT_ASSERT_C(sender12->GetUndelivered() > 0 || sender21->GetUndelivered() > 0,
            "expected at least one undelivered event after forced termination");
    }

    Y_UNIT_TEST(TcpUserTimeoutNoReconnectGeneratesUndelivered) {
        constexpr size_t messages = 512;
        constexpr size_t payloadSize = 64 * 1024;
        constexpr TDuration userTimeout = TDuration::Seconds(3);
        constexpr TDuration lostConnection = TDuration::Seconds(8);

        auto settingsCustomizer = [](ui32, TInterconnectSettings& settings) {
            settings.EnableKernelLiveness = true;
            settings.KernelKeepAliveIdle = TDuration::Seconds(30);
            settings.KernelKeepAliveInterval = TDuration::Seconds(1);
            settings.KernelKeepAliveProbes = 3;
            settings.KernelUserTimeout = TDuration::Seconds(3);

            // Keep this above KernelUserTimeout to observe socket-level timeout first,
            // but still short enough to naturally terminate the session during this test.
            settings.LostConnection = TDuration::Seconds(8);
        };

        TTestICCluster::TTrafficInterrupterSettings interrupterSettings{
            .RejectingTrafficTimeout = TDuration::Zero(),
            .BandWidth = 0.0,
            .Disconnect = false,
        };

        TTestICCluster cluster(2, TChannelsConfig(), &interrupterSettings, nullptr, TTestICCluster::DISABLE_RDMA,
            {}, TDuration::Seconds(30), 128 * 1024 * 1024, settingsCustomizer);

        auto* recipient = new TDropRecipientActor;
        const TActorId recipientId = cluster.RegisterActor(recipient, 1);
        auto* sender = new TBlackholeFloodSenderActor(recipientId, messages, payloadSize);
        const TActorId senderId = cluster.RegisterActor(sender, 2);

        WaitForCondition(TDuration::Seconds(10), [&] {
            return sender->IsConnected();
        }, "sender connected to peer");

        UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 1, 2, "Params.UseKernelLiveness"), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(WaitForSessionCounter(cluster, 2, 1, "Params.UseKernelLiveness"), 1ULL);

        const size_t connectsBeforeFlood = sender->GetConnects();

        // Keep the transport blackholed after the first timeout-driven disconnect,
        // so that handshake/reconnect cannot progress.
        cluster.StartBlackhole(1);
        cluster.StartBlackhole(2);
        Sleep(TDuration::MilliSeconds(200));

        const TInstant floodStart = TInstant::Now();
        cluster.GetNode(2)->Send(senderId, new TEvents::TEvWakeup);

        TDuration earliestDisconnect = TDuration::Zero();
        WaitForCondition(TDuration::Seconds(20), [&] {
            const bool disconnected = TryGetSessionSocketFd(cluster, 1, 2) < 0 || TryGetSessionSocketFd(cluster, 2, 1) < 0;
            if (disconnected && earliestDisconnect == TDuration::Zero()) {
                earliestDisconnect = TInstant::Now() - floodStart;
            }
            return disconnected;
        }, "session disconnected by tcp user timeout");

        const TInstant waitUndeliveredDeadline = floodStart + TDuration::Seconds(35);
        while (TInstant::Now() < waitUndeliveredDeadline && sender->GetObservedUnion() < messages) {
            Sleep(TDuration::MilliSeconds(200));
        }

        const size_t connectsAfterWait = sender->GetConnects();
        const i64 socket12 = TryGetSessionSocketFd(cluster, 1, 2);
        const i64 socket21 = TryGetSessionSocketFd(cluster, 2, 1);
        Cerr << "tcpUserTimeoutNoReconnect# " << userTimeout
            << " lostConnection# " << lostConnection
            << " earliestDisconnect# " << earliestDisconnect
            << " socket12# " << socket12
            << " socket21# " << socket21
            << " connectsBeforeFlood# " << connectsBeforeFlood
            << " connectsAfterWait# " << connectsAfterWait
            << " sent# " << sender->GetSent()
            << " delivered# " << sender->GetDelivered()
            << " undelivered# " << sender->GetUndelivered()
            << " observed# " << sender->GetObservedUnion()
            << " received# " << recipient->GetReceived()
            << "\n";

        UNIT_ASSERT_VALUES_EQUAL(sender->GetSent(), messages);
        UNIT_ASSERT_C(earliestDisconnect != TDuration::Zero(),
            "first disconnect delay is not captured");
        UNIT_ASSERT_C(earliestDisconnect < lostConnection,
            TStringBuilder() << "disconnect was too late, delay# " << earliestDisconnect);
        UNIT_ASSERT_VALUES_EQUAL(sender->GetObservedUnion(), messages);
        UNIT_ASSERT_VALUES_EQUAL(sender->GetUndelivered(), messages);
        UNIT_ASSERT_VALUES_EQUAL(connectsAfterWait, connectsBeforeFlood);
    }

}
