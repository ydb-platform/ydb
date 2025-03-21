
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

Y_UNIT_TEST_SUITE(InterconnectUnstableConnection) {
    using namespace NActors;

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
        ui64 ReceivedCount = 0;
        TNode* SenderNode = nullptr;

    public:
        TReceiverActor(TNode* senderNode)
            : TReceiverBaseActor()
            , SenderNode(senderNode)
        {
        }

        void Handle(TEvTest::TPtr& ev, const TActorContext& /*ctx*/) override {
            const NInterconnectTest::TEvTest& m = ev->Get()->Record;
            Y_ABORT_UNLESS(m.HasSequenceNumber());
            Y_ABORT_UNLESS(m.GetSequenceNumber() >= ReceivedCount, "got #%" PRIu64 " expected at least #%" PRIu64,
                     m.GetSequenceNumber(), ReceivedCount);
            if (m.HasPayloadId()) {
                auto rope = ev->Get()->GetPayload(m.GetPayloadId());
                auto data = rope.GetContiguousSpan();
                auto crc = Crc32c(data.data(), data.size());
                Y_ABORT_UNLESS(m.GetDataCrc() == crc);
            } else {
                Y_ABORT_UNLESS(m.HasPayload());
            }
            ++ReceivedCount;
            SenderNode->Send(ev->Sender, new TEvTestResponse(m.GetSequenceNumber()));
        }

        ~TReceiverActor() override {
            Cerr << "Received " << ReceivedCount << " messages\n";
        }
    };

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
