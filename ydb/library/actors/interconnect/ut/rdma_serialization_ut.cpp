#include <ydb/library/actors/interconnect/channel_scheduler.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/interconnect/ut/protos/interconnect_test.pb.h>
#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

using namespace NActors;

struct TEvTestSerialization : public TEventPB<TEvTestSerialization, NInterconnectTest::TEvTestSerialization, 123> {};

Y_UNIT_TEST_SUITE(RdmaSerialization) {

    Y_UNIT_TEST(Simple) {
        auto common = MakeIntrusive<TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
        ctr->SetPeerInfo(1, "peer", "1");
        auto callback = [](THolder<IEventBase>) {};
        TEventHolderPool pool(common, callback);
        TSessionParams p;
        p.UseExternalDataChannel = true;
        TEventOutputChannel channel(1, 1, 64 << 20, ctr, p);

        auto ev = new TEvTestSerialization();
        ev->Record.SetBlobID(123);
        ev->Record.SetBuffer("hello world");
        ev->AddPayload(TRope(TString(5000, 'X')));
        UNIT_ASSERT(ev->AllowExternalDataChannel());
        auto evHandle = MakeHolder<IEventHandle>(TActorId(), TActorId(), ev);

        channel.Push(*evHandle, pool);

        NInterconnect::TOutgoingStream main, xdc;
        TTcpPacketOutTask task(p, main, xdc);

        UNIT_ASSERT(channel.FeedBuf(task, 0));

        TVector<TConstIoVec> mainData, xdcData;
        main.ProduceIoVec(mainData, 100, 10000);
        xdc.ProduceIoVec(xdcData, 100, 10000);

        ui32 totalXdcSize = 0;
        for (const auto& [_, len] : xdcData) {
            totalXdcSize += len;
        }


        auto allocRcBuf = [](ui32 size) {
            return TRcBuf::Uninitialized(size);
        };
        auto serializedRope = ev->SerializeToRope(allocRcBuf);
        UNIT_ASSERT(serializedRope.has_value());
        auto rope = serializedRope->ConvertToString();
        // 6 1 -120 39 88x5000 8 123 18 11 104 101 108 108 111 32 119 111 114 108 100

        UNIT_ASSERT_VALUES_EQUAL(totalXdcSize, rope.size());
        ui32 index = 0;
        for (const auto& [ptr, len] : xdcData) {
            for (size_t i = 0; i < len; ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C((i32)(((char*)ptr)[i]), (i32)rope[index], "Index: " << index);
                ++index;
            }
        }

    }

    class TSendActor: public TActorBootstrapped<TSendActor> {
    public:
        TSendActor(TActorId recipient, IEventBase* ev)
            : Recipient(recipient)
            , Event(ev)
        {}

        void Bootstrap() {
            Send(Recipient, Event);
            Cerr << "Sent event to " << Recipient.ToString() << Endl;
            PassAway();
        }

    private:
        TActorId Recipient;
        IEventBase* Event;
    };

    class TReceiveActor: public TActorBootstrapped<TReceiveActor> {
    public:
        TReceiveActor(std::function<void(TEvTestSerialization::TPtr)> check)
            : Check(check)
        {}

        void Bootstrap() {
            Become(&TReceiveActor::StateFunc);
        }
        void Handle(TEvTestSerialization::TPtr& ev) {
            Cerr << "Received event" << Endl;
            Check(ev);
            Received = true;
        }
        STRICT_STFUNC(StateFunc,
            hFunc(TEvTestSerialization, Handle);
        )
    public:
        std::atomic<bool> Received = false;
    private:
        std::function<void(TEvTestSerialization::TPtr)> Check;
    };

    Y_UNIT_TEST(Send) {
        TTestICCluster cluster(2);
        auto ev = new TEvTestSerialization();
        ev->Record.SetBlobID(123);
        ev->Record.SetBuffer("hello world");
        ev->AddPayload(TRope(TString(5000, 'X')));
        UNIT_ASSERT(ev->AllowExternalDataChannel());

        auto recieverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        });
        const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);

        Sleep(TDuration::MilliSeconds(1000));

        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);

        Sleep(TDuration::MilliSeconds(1000));
        // while (!recieverPtr->Received) {
        //     Sleep(TDuration::MilliSeconds(100));
        // }
    }

}
