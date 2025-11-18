#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <ydb/library/actors/interconnect/ut/protos/interconnect_test.pb.h>
#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/channel_scheduler.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

using namespace NActors;

struct TEvTestSerialization : public TEventPB<TEvTestSerialization, NInterconnectTest::TEvTestSerialization, 123> {};

static void GTestSkip() {
    GTEST_SKIP() << "Skipping all rdma tests for suit, set \""
                 << NRdmaTest::RdmaTestEnvSwitchName << "\" env if it is RDMA compatible";
}

class XdcRdmaTest : public ::testing::Test {
public:
    void SetUp() override {
        using namespace NRdmaTest;
        if (NRdmaTest::IsRdmaTestDisabled()) {
            GTestSkip();
        }
    }
};

class TSendActor: public TActorBootstrapped<TSendActor> {
public:
    TSendActor(TActorId recipient, IEventBase* ev)
        : Recipient(recipient)
        , Event(ev)
    {}

    void Bootstrap() {
        Send(Recipient, Event, IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&TSendActor::StateResolve);
    }

    void HandleUndelivered() {
        Undelivered = true;
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvUndelivered::EventType, HandleUndelivered);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, HandleUndelivered);
        }
    }

public:
    bool Undelivered = false;
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
        Check(ev);
        ReceivedEvents.fetch_add(1, std::memory_order_relaxed);
    }
    STRICT_STFUNC(StateFunc,
        hFunc(TEvTestSerialization, Handle);
    )
public:
    std::atomic<ui32> ReceivedEvents = 0;
    bool WhaitForRecieve(ui32 expected, ui32 maxAttempt) {
        while (ReceivedEvents.load(std::memory_order_relaxed) != expected && maxAttempt) {
            Sleep(TDuration::MilliSeconds(1000));
            maxAttempt--;
        }
        return ReceivedEvents.load(std::memory_order_relaxed) == expected;
    }
private:
    std::function<void(TEvTestSerialization::TPtr)> Check;
};

struct TEventsForTest {
    std::vector<TEvTestSerialization*> Events;
    std::unordered_map<ui64, std::function<void(TEvTestSerialization*)>> Checks;
    std::shared_ptr<NInterconnect::NRdma::IMemPool> MemPool;

    TEventsForTest(ui32 numEvents)
        : MemPool(NInterconnect::NRdma::CreateDummyMemPool())
    {
        Generate(numEvents, MemPool.get());
    }

    void Generate(ui32 numEvents, NInterconnect::NRdma::IMemPool* memPool) {
        for (ui32 i = 0; i < numEvents; ++i) {
            const bool isInline = i % 3 == 0;
            const bool isXdc = i % 3 == 1;
            const bool isRdma = i % 3 == 2;
            const ui32 numPayloads = i % 5 + (isXdc || isRdma);

            auto ev = new TEvTestSerialization();
            ev->Record.SetBlobID(i);
            ev->Record.SetBuffer(TStringBuilder{} << "hello world " << i);
            for (ui32 j = 0; j < numPayloads; ++j) {
                if (isInline) {
                    ev->AddPayload(TRope(TString(10 + j, j + i)));
                } else if (isXdc) {
                    ev->AddPayload(TRope(TString(5000 + j, j + i)));
                } else if (isRdma) {
                    auto buf = memPool->AllocRcBuf(5000 + j, 0).value();
                    std::fill(buf.GetDataMut(), buf.GetDataMut() + 5000 + j, j + i);
                    ev->AddPayload(TRope(std::move(buf)));
                    UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().back().size(), 5000 + j);
                }
            }

            if (isXdc || isRdma) {
                UNIT_ASSERT(ev->AllowExternalDataChannel());
            }

            Events.push_back(ev);

            Checks.emplace(i, [i, numPayloads, isInline](TEvTestSerialization* ev) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetBlobID(), i);
                UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetBuffer(), TStringBuilder{} << "hello world " << i);
                UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().size(), numPayloads);
                for (ui32 j = 0; j < numPayloads; ++j) {
                    ui32 payloadSize = isInline ? 10 + j : 5000 + j;
                    UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload()[j].GetSize(), payloadSize);
                    UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload()[j].ConvertToString(), TString(payloadSize, j + i));
                }
            });

        }

        std::random_shuffle(Events.begin(), Events.end());
    }
};

TEvTestSerialization* MakeMultuGlueTestEvent(ui64 blobId, NInterconnect::NRdma::IMemPool* memPool) {
    auto ev = new TEvTestSerialization();
    ev->Record.SetBlobID(blobId);
    ev->Record.SetBuffer("hello world");
    auto buf = memPool->AllocRcBuf(5000, 0).value();
    auto b = buf.data();
    TRcBuf rcbuf1(TRcBuf::Piece, b, b + 500, buf);
    std::fill(rcbuf1.UnsafeGetDataMut(), rcbuf1.UnsafeGetDataMut() + 500, 'X');
    TRcBuf rcbuf2(TRcBuf::Piece, b + 500, b + 2000, buf);
    std::fill(rcbuf2.UnsafeGetDataMut(), rcbuf2.UnsafeGetDataMut() + 1500, 'Y');
    TRcBuf rcbuf3(TRcBuf::Piece, b + 2000, b + 5000, buf);
    std::fill(rcbuf3.UnsafeGetDataMut(), rcbuf3.UnsafeGetDataMut() + 3000, 'Z');
    ev->AddPayload(TRcBuf(std::move(rcbuf1)));
    ev->AddPayload(TRcBuf(std::move(rcbuf2)));
    ev->AddPayload(TRcBuf(std::move(rcbuf3)));

    bool done = ev->AllowExternalDataChannel();
    UNIT_ASSERT_VALUES_EQUAL(done, true); 
    return ev;
}

TEvTestSerialization* MakeTestEvent(ui64 blobId, NInterconnect::NRdma::IMemPool* memPool = nullptr, bool withGlue = false, bool withOffset = false) {
    auto ev = new TEvTestSerialization();
    ev->Record.SetBlobID(blobId);
    ev->Record.SetBuffer("hello world");
    if (!memPool) {
        TRope tmp(TString(5000, 'X'));
        if (withOffset) {
            tmp.Insert(tmp.End(), TRope(TString(999, 'Z'))); 
        }
        ev->AddPayload(std::move(tmp));
    } else {
        auto buf = memPool->AllocRcBuf(5000, 0).value();
        // TRope can "glue" rcbufs in they have same backend and placed in the contiguous memory regions
        if (withGlue) {
            auto b = buf.data();
            TRcBuf rcbuf1(TRcBuf::Piece, b, b + 2500, buf);
            std::fill(rcbuf1.UnsafeGetDataMut(), rcbuf1.UnsafeGetDataMut() + 2500, 'X');
            TRcBuf rcbuf2(TRcBuf::Piece, b + 2500, b + 5000, buf);
            std::fill(rcbuf2.UnsafeGetDataMut(), rcbuf2.UnsafeGetDataMut() + 2500, 'X');
            TRope rope1(std::move(rcbuf1));
            if (withOffset) {
                TRcBuf rcbuf3 = memPool->AllocRcBuf(999, 0).value();
                std::fill(rcbuf3.UnsafeGetDataMut(), rcbuf3.UnsafeGetDataMut() + 999, 'Z');
                rope1.Insert(rope1.Begin(), TRope(std::move(rcbuf3)));
            }
            ev->AddPayload(std::move(rope1));
            TRope tmp(std::move(rcbuf2));
            if (withOffset) {
                TRcBuf rcbuf3 = memPool->AllocRcBuf(999, 0).value();
                std::fill(rcbuf3.UnsafeGetDataMut(), rcbuf3.UnsafeGetDataMut() + 999, 'Z');
                tmp.Insert(tmp.End(), TRope(std::move(rcbuf3)));
            }
            ev->AddPayload(std::move(tmp));
            {
                auto it = ev->GetPayload().rbegin();
                UNIT_ASSERT_VALUES_EQUAL(it->size(), withOffset ? 3499u : 2500u);
                it++;
                UNIT_ASSERT_VALUES_EQUAL(it->size(), withOffset ? 3499u : 2500u);
            }
        } else {
            std::fill(buf.GetDataMut(), buf.GetDataMut() + 5000, 'X');
            TRope tmp(std::move(buf));
            if (withOffset) {
                TRcBuf rcbuf3 = memPool->AllocRcBuf(999, 0).value();
                std::fill(rcbuf3.UnsafeGetDataMut(), rcbuf3.UnsafeGetDataMut() + 999, 'Z');
                tmp.Insert(tmp.End(), TRope(std::move(rcbuf3))); 
            }
            ev->AddPayload(std::move(tmp));
            UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().back().size(), withOffset ? 5999u : 5000u);
        }
    }
    bool done = ev->AllowExternalDataChannel();
    UNIT_ASSERT_VALUES_EQUAL(done, true); 
    return ev;
}

TEST_F(XdcRdmaTest, SerializeToRope) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
    ctr->SetPeerInfo(1, "peer", "1");
    auto callback = [](THolder<IEventBase>) {};
    TEventHolderPool pool(common, callback);
    TSessionParams p;
    p.UseExternalDataChannel = true;
    TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, nullptr);

    auto ev = MakeTestEvent(123);
    auto evHandle = MakeHolder<IEventHandle>(TActorId(), TActorId(), ev);

    TInstant t = TInstant::Zero();

    channel.Push(*evHandle, pool, t);

    NInterconnect::TOutgoingStream main, xdc;
    TTcpPacketOutTask task(p, main, xdc);

    ASSERT_TRUE(channel.FeedBuf(task, 0));

    TVector<TConstIoVec> mainData, xdcData;
    main.ProduceIoVec(mainData, 100, 10000);
    xdc.ProduceIoVec(xdcData, 100, 10000);

    ui32 totalXdcSize = 0;
    for (const auto& [_, len] : xdcData) {
        totalXdcSize += len;
    }

    auto mempool = NInterconnect::NRdma::CreateSlotMemPool(nullptr);

    auto serializedRope = ev->SerializeToRope(mempool.get());

    ASSERT_TRUE(serializedRope.has_value());
    auto rope = serializedRope->ConvertToString();
    // 6 1 -120 39 88x5000 8 123 18 11 104 101 108 108 111 32 119 111 114 108 100

    UNIT_ASSERT_VALUES_EQUAL(totalXdcSize, rope.size());
    ui32 index = 0;
    for (const auto& [ptr, len] : xdcData) {
        for (size_t i = 0; i < len; ++i) {
            TStringStream msg;
            msg << "Index: " << index << " " << (i32)(((char*)ptr)[i]) << " != " << (i32)rope[index];
            UNIT_ASSERT_EQUAL_C((i32)(((char*)ptr)[i]), (i32)rope[index], msg.Data());
            ++index;
        }
    }

    auto serializationInfo = ev->CreateSerializationInfo();
    auto parsedEventHandle = std::make_unique<IEventHandle>(
        TActorId(),
        ev->Type(),
        ~IEventHandle::FlagExtendedFormat,
        TActorId(),
        TActorId(),
        MakeIntrusive<TEventSerializedData>(std::move(*serializedRope), std::move(serializationInfo)),
        0,
        TScopeId(),
        NWilson::TTraceId()
    );
    auto parsedEvent = parsedEventHandle->Get<TEvTestSerialization>();
    UNIT_ASSERT(parsedEvent);
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->Record.GetBlobID(), 123u);
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->Record.GetBuffer(), "hello world");
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->GetPayload().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->GetPayload()[0].GetSize(), 5000u);
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
}

TEST_F(XdcRdmaTest, SendRdma) {
    TTestICCluster cluster(2);
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    auto* ev = MakeTestEvent(123, memPool.get());

    auto recieverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(recieverPtr->WhaitForRecieve(1, 20));
}

TEST_F(XdcRdmaTest, SendRdmaWithRegionOffset) {
    TTestICCluster cluster(2);
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    auto* ev = MakeTestEvent(123, memPool.get(), false, true);

    auto recieverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5999u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X') + TString(999, 'Z'));
    });
    const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(recieverPtr->WhaitForRecieve(1, 20));
}

TEST_F(XdcRdmaTest, SendRdmaWithGlueWithRegionOffset) {
    TTestICCluster cluster(2);
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr);
    auto* ev = MakeTestEvent(123, memPool.get(), true, true);

    auto recieverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 3499u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].GetSize(), 3499u);
        const TString pattern1 = TString(999, 'Z') + TString(2500, 'X');
        const TString pattern2 = TString(2500, 'X') + TString(999, 'Z'); 
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), pattern1);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].ConvertToString(), pattern2);
    });
    const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(recieverPtr->WhaitForRecieve(1, 20));
}

TEST_F(XdcRdmaTest, SendRdmaWithGlue) {
    TTestICCluster cluster(2);
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr);
    auto* ev = MakeTestEvent(123, memPool.get(), true, false);

    auto recieverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 2500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].GetSize(), 2500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(2500, 'X'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].ConvertToString(), TString(2500, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(recieverPtr->WhaitForRecieve(1, 20));
}

TEST_F(XdcRdmaTest, SendRdmaWithMultiGlue) {
    TTestICCluster cluster(2);
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr);
    auto* ev = MakeMultuGlueTestEvent(123, memPool.get());

    auto recieverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].GetSize(), 1500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[2].GetSize(), 3000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(500, 'X'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].ConvertToString(), TString(1500, 'Y'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[2].ConvertToString(), TString(3000, 'Z'));
    });
    const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(recieverPtr->WhaitForRecieve(1, 20));
}

TEST_F(XdcRdmaTest, SendMix) {
    TTestICCluster cluster(2);

    ui32 index = 0;
    auto recieverPtr = new TReceiveActor([&index](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), index++);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    const ui32 numEvents = 10;
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    for (ui32 i = 0; i < numEvents; ++i) {
        const bool isRdma = i % 2 == 0;
        auto* ev = MakeTestEvent(i, isRdma ? memPool.get() : nullptr);
        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);
    }

    UNIT_ASSERT(recieverPtr->WhaitForRecieve(numEvents, 20));
}

TEST_F(XdcRdmaTest, SendMixBig) {
    TTestICCluster cluster(2);
    TEventsForTest events(1000);

    auto recieverPtr = new TReceiveActor([&events](TEvTestSerialization::TPtr ev) {
        ui64 blobId = ev->Get()->Record.GetBlobID();
        auto checkIt = events.Checks.find(blobId);
        UNIT_ASSERT(checkIt != events.Checks.end());
        checkIt->second(ev->Get());
        events.Checks.erase(checkIt);
    });
    const TActorId receiver = cluster.RegisterActor(recieverPtr, 1);
    Sleep(TDuration::MilliSeconds(1000));

    for (auto* ev : events.Events) {
        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);
    }

    for (ui32 attempt = 0; attempt < 10 && !events.Checks.empty(); ++attempt) {
        Sleep(TDuration::MilliSeconds(1000));
    }
    UNIT_ASSERT_VALUES_EQUAL(events.Checks.size(), 0u);
}
