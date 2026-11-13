#include <ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <ydb/library/actors/interconnect/interconnect_direct_session.h>
#include <ydb/library/actors/interconnect/uring_context.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/protos/unittests.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/network/pair.h>
#include <util/network/socket.h>
#include <util/string/cast.h>
#include <util/system/hp_timer.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>

using namespace NActors;

namespace {

    struct TEvTest final : TEventPB<TEvTest, TMessageWithPayload, EventSpaceBegin(TEvents::ES_PRIVATE)> {};

    class TClient final : public IUringEngineClient {
        std::mutex Mutex;
        std::condition_variable Changed;
        std::deque<std::unique_ptr<IEventHandle>> Received;
        size_t Destroyed = 0;
        bool Disconnected = false;
        std::atomic<ui64> DestroyEventsCycles{0};

    public:
        IUringEngine* StartedEngine = nullptr;
        size_t StopCalls = 0;
        static constexpr ui64 DestructionCycles = 1'000'000;

        void Start(const TUringEnginePtr& engine) override {
            StartedEngine = engine.Get();
        }

        void Stop() override {
            ++StopCalls;
        }

        ui64 GetAndResetDestroyEventsCycles() override {
            return DestroyEventsCycles.exchange(0, std::memory_order_relaxed);
        }

        void Send(std::unique_ptr<IEventHandle> ev) override {
            std::lock_guard guard(Mutex);
            Received.push_back(std::move(ev));
            Changed.notify_all();
        }

        void DestroyEvent(
                std::unique_ptr<IEventBase> event,
                TIntrusivePtr<TEventSerializedData> buffer) override {
            event.reset();
            buffer.Reset();
            std::lock_guard guard(Mutex);
            ++Destroyed;
            DestroyEventsCycles.fetch_add(DestructionCycles, std::memory_order_relaxed);
            Changed.notify_all();
        }

        void OnDisconnect() {
            std::lock_guard guard(Mutex);
            Disconnected = true;
            Changed.notify_all();
        }

        std::unique_ptr<IEventHandle> Receive() {
            std::unique_lock lock(Mutex);
            UNIT_ASSERT_C(Changed.wait_for(lock, std::chrono::seconds(10), [&] { return !Received.empty(); }),
                "Interconnect V2 did not deliver an event");
            auto ev = std::move(Received.front());
            Received.pop_front();
            return ev;
        }

        void WaitDestroyed(size_t count) {
            std::unique_lock lock(Mutex);
            UNIT_ASSERT_C(Changed.wait_for(lock, std::chrono::seconds(10), [&] { return Destroyed == count; }),
                "Interconnect V2 did not release completed events");
        }

        void WaitDisconnected() {
            std::unique_lock lock(Mutex);
            UNIT_ASSERT_C(Changed.wait_for(lock, std::chrono::seconds(10), [&] { return Disconnected; }),
                "Interconnect V2 did not report peer disconnection");
        }
    };

    struct TEngine {
        std::shared_ptr<TClient> Client = std::make_shared<TClient>();
        TUringEnginePtr Engine;
        NMonitoring::TDynamicCounters::TCounterPtr DestroyEventsTotalTime;

        TEngine() {
            auto common = MakeIntrusive<TInterconnectProxyCommon>();
            common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            DestroyEventsTotalTime = common->MonCounters->GetSubgroup("subsystem", "uring")
                ->GetSubgroup("shard", "0")->GetCounter("TotalTime/DestroyEventsTotalTime", true);
            common->Settings.V2.Threads = 1;
            common->Settings.V2.EnableSQPOLL = false;
            Engine = CreateUringEngine(common);
            UNIT_ASSERT(Engine);
            Engine->SetClient(Client);
            UNIT_ASSERT_VALUES_EQUAL(Client->StartedEngine, Engine.Get());
        }

        ~TEngine() {
            Engine->Stop();
        }
    };

    using TSockets = std::array<TIntrusivePtr<NInterconnect::TStreamSocket>, 2>;

    TSockets MakeSockets() {
        SOCKET fds[2];
        SocketPair(fds);
        TSockets sockets{
            MakeIntrusive<NInterconnect::TStreamSocket>(fds[0]),
            MakeIntrusive<NInterconnect::TStreamSocket>(fds[1])};
        SetNonBlock(fds[0]);
        SetNonBlock(fds[1]);
        return sockets;
    }

    void CheckExchange(bool useXdc) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }

        std::array<TEngine, 2> peers;
        auto sockets = MakeSockets();
        auto xdcSockets = useXdc ? MakeSockets() : TSockets{};
        const std::array<TActorId, 2> actors{
            TActorId(1, 0, 1, 1),
            TActorId(2, 0, 1, 1)};
        std::array<ui64, 2> connections;
        for (size_t i = 0; i < peers.size(); ++i) {
            connections[i] = peers[i].Engine->Register(sockets[i], actors[i], TScopeId(),
                [client = peers[i].Client](TDisconnectReason) { client->OnDisconnect(); },
                false, std::make_shared<std::atomic<int64_t>>(0),
                std::make_shared<std::atomic<uint64_t>>(0), xdcSockets[i]);
            UNIT_ASSERT(connections[i]);
        }

        constexpr size_t eventCount = 4;
        const TString payload(256 * 1024, 'p');
        for (size_t i = 0; i < peers.size(); ++i) {
            for (size_t cookie = 0; cookie < eventCount; ++cookie) {
                auto ev = std::make_unique<TEvTest>();
                ev->Record.SetMeta(ToString(cookie));
                ev->AddPayload(TRope(payload));
                peers[i].Engine->Send(connections[i], std::make_unique<IEventHandle>(
                    actors[1 - i], actors[i], ev.release(), IEventHandle::MakeFlags(3, 0), cookie));
            }
        }
        for (size_t i = 0; i < peers.size(); ++i) {
            for (size_t cookie = 0; cookie < eventCount; ++cookie) {
                auto ev = peers[i].Client->Receive();
                UNIT_ASSERT_VALUES_EQUAL(ev->Type, TEvTest::EventType);
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender, actors[1 - i]);
                UNIT_ASSERT_VALUES_EQUAL(ev->Recipient, actors[i]);
                UNIT_ASSERT_VALUES_EQUAL(ev->GetChannel(), 3);
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, cookie);
                auto* message = ev->Get<TEvTest>();
                UNIT_ASSERT_VALUES_EQUAL(message->Record.GetMeta(), ToString(cookie));
                UNIT_ASSERT_VALUES_EQUAL(message->GetPayloadCount(), 1);
                UNIT_ASSERT_VALUES_EQUAL(message->GetPayload(0).ConvertToString(), payload);
            }
            peers[i].Client->WaitDestroyed(eventCount);
        }

        sockets[0]->Shutdown(SHUT_RDWR);
        peers[1].Client->WaitDisconnected();
        sockets[1]->Shutdown(SHUT_RDWR);
        for (auto& socket : xdcSockets) {
            if (socket) {
                socket->Shutdown(SHUT_RDWR);
            }
        }
        for (size_t i = 0; i < peers.size(); ++i) {
            peers[i].Engine->Unregister(connections[i]);
            peers[i].Engine->Stop();
            peers[i].Engine->Stop();
            UNIT_ASSERT_VALUES_EQUAL(peers[i].Client->StopCalls, 1);
            const ui64 expected = eventCount * TClient::DestructionCycles * (1e9 * NHPTimer::GetSeconds(1));
            const ui64 actual = peers[i].DestroyEventsTotalTime->Val();
            // Each publication rounds cycles to whole nanoseconds independently.
            UNIT_ASSERT_C(actual <= expected && expected - actual < eventCount,
                "destruction time must be published once for each completed event");
            UNIT_ASSERT_VALUES_EQUAL(peers[i].Client->GetAndResetDestroyEventsCycles(), 0);
        }
    }

}

Y_UNIT_TEST_SUITE(InterconnectUringEngine) {
    Y_UNIT_TEST(ExchangeWithoutActorSystem) {
        CheckExchange(false);
    }

    Y_UNIT_TEST(XdcExchangeWithoutActorSystem) {
        CheckExchange(true);
    }
}
