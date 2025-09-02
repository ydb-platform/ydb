#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/ut/protos/interconnect_test.pb.h>
#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/ut/lib/interrupter.h>
#include <ydb/library/actors/interconnect/ut/lib/test_events.h>
#include <ydb/library/actors/interconnect/ut/lib/test_actors.h>
#include <ydb/library/actors/interconnect/ut/lib/node.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/network/sock.h>
#include <util/network/poller.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/set.h>

using namespace NActors;

namespace {
    class TSenderActor: public TSenderBaseActor {
        TDeque<ui64> InFly;
        ui16 SendFlags;
        bool UseRope;

    public:
        TSenderActor(const TActorId& recipientActorId, ui16 sendFlags, bool useRope)
            : TSenderBaseActor(recipientActorId, 32)
            , SendFlags(sendFlags)
            , UseRope(useRope)
        {
        }

        ~TSenderActor() override {
            Cerr << "Sent " << SequenceNumber << " messages\n";
        }

        void SendMessage(const TActorContext& ctx) override {
            const ui32 flags = IEventHandle::MakeFlags(0, SendFlags);
            const ui64 cookie = SequenceNumber;

            const TString payload(RandomNumber<size_t>(65536) + 4096, '@');

            auto ev = new TEvTest(SequenceNumber);
            ev->Record.SetDataCrc(Crc32c(payload.data(), payload.size()));

            if (UseRope) {
                ev->Record.SetPayloadId(ev->AddPayload(TRope(payload)));
            } else {
                ev->Record.SetPayload(payload);
            }

            ctx.Send(RecipientActorId, ev, flags, cookie);
            InFly.push_back(SequenceNumber);
            ++InFlySize;
            ++SequenceNumber;
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) override {
            auto record = std::find(InFly.begin(), InFly.end(), ev->Cookie);
            if (SendFlags & IEventHandle::FlagGenerateUnsureUndelivered) {
                if (record != InFly.end()) {
                    InFly.erase(record);
                    --InFlySize;
                    SendMessage(ctx);
                }
            } else {
                Y_ABORT_UNLESS(record != InFly.end());
            }
        }

        void Handle(TEvTestResponse::TPtr& ev, const TActorContext& ctx) override {
            Y_ABORT_UNLESS(InFly);
            const NInterconnectTest::TEvTestResponse& record = ev->Get()->Record;
            Y_ABORT_UNLESS(record.HasConfirmedSequenceNumber());
            if (!(SendFlags & IEventHandle::FlagGenerateUnsureUndelivered)) {
                while (record.GetConfirmedSequenceNumber() != InFly.front()) {
                    InFly.pop_front();
                    --InFlySize;
                }
            }
            Y_ABORT_UNLESS(record.GetConfirmedSequenceNumber() == InFly.front(), "got# %" PRIu64 " expected# %" PRIu64,
                     record.GetConfirmedSequenceNumber(), InFly.front());
            InFly.pop_front();
            --InFlySize;
            SendMessagesIfPossible(ctx);
        }
    };

    class TReceiverActor: public TReceiverBaseActor {
        std::atomic<ui64> ReceivedCount = 0;
        TNode* SenderNode = nullptr;

    public:
        TReceiverActor(TNode* senderNode)
            : TReceiverBaseActor()
            , SenderNode(senderNode)
        {
        }

        ui64 GetReceivedCount() const {
            return ReceivedCount.load(std::memory_order_relaxed);
        }

        void Handle(TEvTest::TPtr& ev, const TActorContext& /*ctx*/) override {
            const NInterconnectTest::TEvTest& m = ev->Get()->Record;
            Y_ABORT_UNLESS(m.HasSequenceNumber());
            ui64 cur = GetReceivedCount();
            Y_ABORT_UNLESS(m.GetSequenceNumber() >= cur, "got #%" PRIu64 " expected at least #%" PRIu64,
                     m.GetSequenceNumber(), cur);
            if (m.HasPayloadId()) {
                auto rope = ev->Get()->GetPayload(m.GetPayloadId());
                auto data = rope.GetContiguousSpan();
                auto crc = Crc32c(data.data(), data.size());
                Y_ABORT_UNLESS(m.GetDataCrc() == crc);
            } else {
                Y_ABORT_UNLESS(m.HasPayload());
            }
            ReceivedCount.fetch_add(1);
            SenderNode->Send(ev->Sender, new TEvTestResponse(m.GetSequenceNumber()));
        }

        ~TReceiverActor() override {
            Cerr << "Received " << GetReceivedCount() << " messages\n";
        }
    };

    TString GetZcState(TTestICCluster& testCluster, ui32 me, ui32 peer) {
        auto httpResp = testCluster.GetSessionDbg(me, peer);
        const TString& resp = httpResp.GetValueSync();
        const TString pattern = "<tr><td>ZeroCopy state</td><td>";
        auto pos = resp.find(pattern);
        UNIT_ASSERT_C(pos != std::string::npos, "zero copy field was not found in http info");
        pos += pattern.size();
        size_t end = resp.find('<', pos);
        UNIT_ASSERT(end != std::string::npos);
        return resp.substr(pos, end - pos);
    }
}

Y_UNIT_TEST_SUITE(InterconnectUnstableConnection) {
    Y_UNIT_TEST(InterconnectTestWithProxyUnsureUndelivered) {
        ui32 numNodes = 2;
        double bandWidth = 1000000;
        ui16 flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagGenerateUnsureUndelivered;
        TTestICCluster::TTrafficInterrupterSettings interrupterSettings{TDuration::Seconds(2), bandWidth, true};

        TTestICCluster testCluster(numNodes, TChannelsConfig(), &interrupterSettings);

        TReceiverActor* receiverActor = new TReceiverActor(testCluster.GetNode(1));
        const TActorId recipient = testCluster.RegisterActor(receiverActor, 2);
        TSenderActor* senderActor = new TSenderActor(recipient, flags, false);
        testCluster.RegisterActor(senderActor, 1);

        NanoSleep(30ULL * 1000 * 1000 * 1000);
    }

    Y_UNIT_TEST(InterconnectTestWithProxyUnsureUndeliveredWithRopeXdc) {
        ui32 numNodes = 2;
        double bandWidth = 1000000;
        ui16 flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagGenerateUnsureUndelivered;
        TTestICCluster::TTrafficInterrupterSettings interrupterSettings{TDuration::Seconds(2), bandWidth, true};

        TTestICCluster testCluster(numNodes, TChannelsConfig(), &interrupterSettings);

        TReceiverActor* receiverActor = new TReceiverActor(testCluster.GetNode(1));
        const TActorId recipient = testCluster.RegisterActor(receiverActor, 2);
        TSenderActor* senderActor = new TSenderActor(recipient, flags, true);
        testCluster.RegisterActor(senderActor, 1);

        NanoSleep(30ULL * 1000 * 1000 * 1000);
    }

    Y_UNIT_TEST(InterconnectTestWithProxyTlsReestablishWithXdc) {
        ui32 numNodes = 2;
        double bandWidth = 1000000;
        ui16 flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagGenerateUnsureUndelivered;
        TTestICCluster::TTrafficInterrupterSettings interrupterSettings{TDuration::Seconds(2), bandWidth, true};

        TTestICCluster testCluster(numNodes, TChannelsConfig(), &interrupterSettings, nullptr, TTestICCluster::USE_TLS);

        TReceiverActor* receiverActor = new TReceiverActor(testCluster.GetNode(1));
        const TActorId recipient = testCluster.RegisterActor(receiverActor, 2);
        TSenderActor* senderActor = new TSenderActor(recipient, flags, true);
        testCluster.RegisterActor(senderActor, 1);

        NanoSleep(30ULL * 1000 * 1000 * 1000);

        UNIT_ASSERT_C(receiverActor->GetReceivedCount() > 0, "no traffic detected!");
    }

    Y_UNIT_TEST(InterconnectTestWithProxy) {
        ui32 numNodes = 2;
        double bandWidth = 1000000;
        ui16 flags = IEventHandle::FlagTrackDelivery;
        TTestICCluster::TTrafficInterrupterSettings interrupterSettings{TDuration::Seconds(2), bandWidth, true};

        TTestICCluster testCluster(numNodes, TChannelsConfig(), &interrupterSettings);

        TReceiverActor* receiverActor = new TReceiverActor(testCluster.GetNode(1));
        const TActorId recipient = testCluster.RegisterActor(receiverActor, 2);
        TSenderActor* senderActor = new TSenderActor(recipient, flags, false);
        testCluster.RegisterActor(senderActor, 1);

        NanoSleep(30ULL * 1000 * 1000 * 1000);
    }
}

Y_UNIT_TEST_SUITE(InterconnectZcLocalOp) {

    Y_UNIT_TEST(ZcIsDisabledByDefault) {
        ui32 numNodes = 2;
        TTestICCluster testCluster(numNodes, TChannelsConfig());
        ui16 flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagGenerateUnsureUndelivered;

        TReceiverActor* receiverActor = new TReceiverActor(testCluster.GetNode(1));
        const TActorId recipient = testCluster.RegisterActor(receiverActor, 2);
        TSenderActor* senderActor = new TSenderActor(recipient, flags, false);
        testCluster.RegisterActor(senderActor, 1);

        NanoSleep(5ULL * 1000 * 1000 * 1000);
        UNIT_ASSERT_VALUES_EQUAL("Disabled", GetZcState(testCluster, 1, 2));
    }

    Y_UNIT_TEST(ZcDisabledAfterHiddenCopy) {
        ui32 numNodes = 2;
        ui16 flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagGenerateUnsureUndelivered;

        TTestICCluster testCluster(numNodes, TChannelsConfig(), nullptr, nullptr, TTestICCluster::USE_ZC);

        TReceiverActor* receiverActor = new TReceiverActor(testCluster.GetNode(1));
        const TActorId recipient = testCluster.RegisterActor(receiverActor, 2);
        TSenderActor* senderActor = new TSenderActor(recipient, flags, true);
        testCluster.RegisterActor(senderActor, 1);

        NanoSleep(5ULL * 1000 * 1000 * 1000);
        // Zero copy send via loopback causes hidden copy inside linux kernel
#if defined (__linux__)
        UNIT_ASSERT_VALUES_EQUAL("DisabledHiddenCopy", GetZcState(testCluster, 1, 2));
#else
        UNIT_ASSERT_VALUES_EQUAL("Disabled", GetZcState(testCluster, 1, 2));
#endif
    }
}
