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

class XdcRdmaTestCqMode : public ::testing::TestWithParam<NInterconnect::NRdma::ECqMode> {
public:
    void SetUp() override {
        using namespace NRdmaTest;
        if (IsRdmaTestDisabled()) {
            GTestSkip();
        }
    }
};

class TSendActor: public TActorBootstrapped<TSendActor> {
public:
    struct TExtCtx {
        std::atomic<bool> Undelivered = false;
        bool WhaitForUndelivered(ui32 maxAttempt) {
            while (Undelivered.load(std::memory_order_relaxed) == false && maxAttempt) {
                Sleep(TDuration::MilliSeconds(1000));
                maxAttempt--;
            }
            return Undelivered.load(std::memory_order_relaxed);
        }
    };

    TSendActor(TActorId recipient, IEventBase* ev, std::shared_ptr<TExtCtx> ctx = nullptr)
        : Recipient(recipient)
        , Event(ev)
        , Ctx(ctx)
    {}

    void Bootstrap() {
        Send(Recipient, Event, IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&TSendActor::StateResolve);
    }

    void HandleUndelivered() {
        if (Ctx) {
            Ctx->Undelivered.store(true);
        }
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvUndelivered::EventType, HandleUndelivered);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, HandleUndelivered);
        }
    }

private:
    TActorId Recipient;
    IEventBase* Event;
    std::shared_ptr<TExtCtx> Ctx;
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
    bool WhaitForReceive(ui32 expected, ui32 maxAttempt) {
        while (ReceivedEvents.load(std::memory_order_relaxed) != expected && maxAttempt) {
            Sleep(TDuration::MilliSeconds(1000));
            maxAttempt--;
        }
        auto received = ReceivedEvents.load(std::memory_order_relaxed);
        if (received != expected) {
            Cerr << "received != expected " << received << " " << expected << Endl;
        }
        return received == expected;
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
            ui32 numPayloads = i % 5 + (isXdc || isRdma);
            ui32 sz = 5000;
            if (i % 128 == 127) {
                numPayloads += 500;
                sz = 512;
            }

            auto ev = new TEvTestSerialization();
            ev->Record.SetBlobID(i);
            ev->Record.SetBuffer(TStringBuilder{} << "hello world " << i);
            for (ui32 j = 0; j < numPayloads; ++j) {
                if (isInline) {
                    ev->AddPayload(TRope(TString(10 + j, j + i)));
                } else if (isXdc) {
                    ev->AddPayload(TRope(TString(sz + j, j + i)));
                } else if (isRdma) {
                    auto buf = memPool->AllocRcBuf(sz + j, 0).value();
                    Y_ABORT_UNLESS(buf);
                    std::fill(buf.GetDataMut(), buf.GetDataMut() + sz + j, j + i);
                    ev->AddPayload(TRope(std::move(buf)));
                    UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().back().size(), sz + j);
                }
            }

            if (isXdc || isRdma) {
                UNIT_ASSERT(ev->AllowExternalDataChannel());
            }

            Events.push_back(ev);

            Checks.emplace(i, [i, numPayloads, isInline, sz](TEvTestSerialization* ev) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetBlobID(), i);
                UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetBuffer(), TStringBuilder{} << "hello world " << i);
                UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().size(), numPayloads);
                for (ui32 j = 0; j < numPayloads; ++j) {
                    ui32 payloadSize = isInline ? 10 + j : sz + j;
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

    auto mempool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});

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

    auto serializationInfo = ev->CreateSerializationInfo(false);
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

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WhaitForReceive(1, 20));
}

TEST_F(XdcRdmaTest, SendRdmaWithRegionOffset) {
    TTestICCluster cluster(2);
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    auto* ev = MakeTestEvent(123, memPool.get(), false, true);

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5999u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X') + TString(999, 'Z'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WhaitForReceive(1, 20));
}

TEST_F(XdcRdmaTest, SendRdmaWithGlueWithRegionOffset) {
    TTestICCluster cluster(2);
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
    auto* ev = MakeTestEvent(123, memPool.get(), true, true);

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
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
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WhaitForReceive(1, 20));
}

TEST_F(XdcRdmaTest, SendRdmaWithGlue) {
    TTestICCluster cluster(2);
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
    auto* ev = MakeTestEvent(123, memPool.get(), true, false);

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 2500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].GetSize(), 2500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(2500, 'X'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].ConvertToString(), TString(2500, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WhaitForReceive(1, 20));
}

TEST_F(XdcRdmaTest, SendRdmaWithMultiGlue) {
    TTestICCluster cluster(2);
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
    auto* ev = MakeMultuGlueTestEvent(123, memPool.get());

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
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
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, ev);
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WhaitForReceive(1, 20));
}

TEST_F(XdcRdmaTest, RestoreRdmaSession) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(9999)); //Disable dead peer detection to parallel activity

    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;

    // Create receiver
    ui32 index = 0;
    auto receiverPtr = new TReceiveActor([&index](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), index++);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    // Send one packet to establish session
    {
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        auto* ev = MakeTestEvent(0, memPool.get());
        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);

        UNIT_ASSERT(receiverPtr->WhaitForReceive(1, 20));
    }

    // Allocate all rdma memory to trigger an error during the next transmissions
    for (size_t i = 0; i < 7; i++) {
        regions.emplace_back(pool->Alloc(32u << 20, 0));
    }

    // Send more
    {
        auto extCtx = std::make_shared<TSendActor::TExtCtx>();
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        auto* ev = MakeTestEvent(1, memPool.get());
        auto senderPtr = new TSendActor(receiver, ev, extCtx);
        cluster.RegisterActor(senderPtr, 2);
        // Undelivered because we can't allocate memory on the receiver side
        UNIT_ASSERT(extCtx->WhaitForUndelivered(10));
    }

    // The event was not delivered
    UNIT_ASSERT(receiverPtr->WhaitForReceive(1, 20));

    // Session is going to be recreated without rdma
    // but pending handshake timer are triggered (we can't check it directly in this ut(()
    TString lastRdmaStatus;
    for (size_t i = 0; i < 10; i++) {
        lastRdmaStatus = GetRdmaChecksumStatus(cluster, 2, 1);
        if (lastRdmaStatus == "Off") {
            break;
        }
        Sleep(TDuration::Seconds(1));
    }

    UNIT_ASSERT_VALUES_EQUAL(lastRdmaStatus, "Off");
    lastRdmaStatus.clear();

    // Send one more time (will be delivered through TCP)
    {
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        auto* ev = MakeTestEvent(1, memPool.get());
        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);
    }
    UNIT_ASSERT(receiverPtr->WhaitForReceive(2, 20));
    // Free memory
    regions.clear();
    // Whait for the pending hendshake timer
    Sleep(TDuration::MilliSeconds(5000));

    for (size_t i = 0; i < 5; i++) {
        try {
            lastRdmaStatus = GetRdmaChecksumStatus(cluster, 2, 1);
        } catch (const TPatternNotFound&) {
            // retry case if the session was not created yiet
            Sleep(TDuration::Seconds(1));
            continue;
        }
        if (lastRdmaStatus == "On") {
            break;
        }
        Sleep(TDuration::Seconds(1));
    }

    {
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        auto* ev = MakeTestEvent(2, memPool.get());
        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);
    }
    UNIT_ASSERT(receiverPtr->WhaitForReceive(3, 20));
    lastRdmaStatus = GetRdmaChecksumStatus(cluster, 2, 1);
    UNIT_ASSERT_VALUES_EQUAL(lastRdmaStatus, "On | SoftwareChecksum");
}

TEST_P(XdcRdmaTestCqMode, SendMix) {
    TTestICCluster::Flags flags = TTestICCluster::EMPTY;
    if (GetParam() == NInterconnect::NRdma::ECqMode::POLLING) {
        flags = TTestICCluster::RDMA_POLLING_CQ;
    }
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, flags);

    ui32 index = 0;
    auto receiverPtr = new TReceiveActor([&index](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), index++);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    const ui32 numEvents = 10;
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    for (ui32 i = 0; i < numEvents; ++i) {
        const bool isRdma = i % 2 == 0;
        auto* ev = MakeTestEvent(i, isRdma ? memPool.get() : nullptr);
        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);
    }

    UNIT_ASSERT(receiverPtr->WhaitForReceive(numEvents, 20));
}

TEST_P(XdcRdmaTestCqMode, SendMixBig) {
    TTestICCluster::Flags flags = TTestICCluster::EMPTY;
    if (GetParam() == NInterconnect::NRdma::ECqMode::POLLING) {
        flags = TTestICCluster::RDMA_POLLING_CQ;
    }
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, flags);
    std::mutex mtx;
    mtx.lock();
    TEventsForTest events(500);
    mtx.unlock();

    auto receiverPtr = new TReceiveActor([&events, &mtx](TEvTestSerialization::TPtr ev) {
        ui64 blobId = ev->Get()->Record.GetBlobID();
        {
            std::lock_guard<std::mutex> guard(mtx);
            auto checkIt = events.Checks.find(blobId);
            UNIT_ASSERT(checkIt != events.Checks.end());
            checkIt->second(ev->Get());
            events.Checks.erase(checkIt);
        }
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);
    Sleep(TDuration::MilliSeconds(1000));

    for (auto* ev : events.Events) {
        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);
    }

    for (ui32 attempt = 0; attempt < 50; ++attempt) {
        {
            std::lock_guard<std::mutex> guard(mtx);
            if (events.Checks.empty()) {
                break;
            }
        }
        Sleep(TDuration::MilliSeconds(1000));
    }
    UNIT_ASSERT_VALUES_EQUAL(events.Checks.size(), 0u);
}

static void DoSendHugePayloadsNum(const ui32 numPayloads, const size_t payloadSz, TTestICCluster& cluster,
    std::shared_ptr<NInterconnect::NRdma::IMemPool> pool)
{
    auto ev = new TEvTestSerialization();
    ev->Record.SetBlobID(0);
    ev->Record.SetBuffer(TStringBuilder{} << "hello world ");
    for (ui32 j = 0; j < numPayloads; ++j) {
        auto buf = pool->AllocRcBuf(payloadSz, NInterconnect::NRdma::IMemPool::PAGE_ALIGNED).value();
        ui32* p = reinterpret_cast<ui32*>(buf.GetDataMut());
        std::fill(p, p + (payloadSz / sizeof(*p)), j);
        ev->AddPayload(TRope(std::move(buf)));
    }
    UNIT_ASSERT(ev->AllowExternalDataChannel());

    auto receiverPtr = new TReceiveActor([numPayloads, payloadSz](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayloadCount(), numPayloads);
        for (ui32 j = 0; j < numPayloads; ++j) {
            TRope buf = ev->Get()->GetPayload(j);
            auto span = buf.GetContiguousSpan();
            const ui32* p = reinterpret_cast<const ui32*>(span.GetData());
            UNIT_ASSERT_VALUES_EQUAL(span.GetSize(), payloadSz);

            while (p < reinterpret_cast<const ui32*>(span.GetData() + payloadSz)) {
                UNIT_ASSERT_VALUES_EQUAL(*p, j);
                p++;
            }
        }
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(100));

    {
        auto senderPtr = new TSendActor(receiver, ev);
        cluster.RegisterActor(senderPtr, 2);
    }

    UNIT_ASSERT(receiverPtr->WhaitForReceive(1, 20));
}

TEST_F(XdcRdmaTest, Send1Payload) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20);

    DoSendHugePayloadsNum(1, 8192, cluster, pool);
}

TEST_F(XdcRdmaTest, Send2Payloads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20);

    DoSendHugePayloadsNum(2, 8192, cluster, pool);
}

TEST_F(XdcRdmaTest, Send250Payloads) {
        const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20);

    DoSendHugePayloadsNum(250, 512, cluster, pool);
}

TEST_F(XdcRdmaTest, Send500Payloads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20);

    DoSendHugePayloadsNum(500, 512, cluster, pool);
}

TEST_F(XdcRdmaTest, Send4000Payloads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20);

    DoSendHugePayloadsNum(4000, 512, cluster, pool);
}

TEST_F(XdcRdmaTest, Send16000Payloads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20);

    DoSendHugePayloadsNum(16000, 512, cluster, pool);
}

TEST_F(XdcRdmaTest, Send32000Payloads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20);

    DoSendHugePayloadsNum(32000, 512, cluster, pool);
}

TEST_F(XdcRdmaTest, SendXPayloads) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20);

    for (size_t i = 640; i < 650; i++) {
        DoSendHugePayloadsNum(i, 512, cluster, pool);
    }
}

TEST_F(XdcRdmaTest, SendXPayloadsWithRandSize) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, TTestICCluster::Flags::EMPTY,
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20);

    for (size_t i = 640; i < 650; i++) {
        DoSendHugePayloadsNum(i, 512 + (RandomNumber<ui16>(4096) * 4), cluster, pool);
    }
}

INSTANTIATE_TEST_SUITE_P(
    XdcRdmaTest,
    XdcRdmaTestCqMode,
    ::testing::Values(
        NInterconnect::NRdma::ECqMode::POLLING,
        NInterconnect::NRdma::ECqMode::EVENT
    ),
    [](const testing::TestParamInfo<NInterconnect::NRdma::ECqMode>& info) {
        const NInterconnect::NRdma::TMemPoolSettings settings {
            .SizeLimitMb = 256
        };
        NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);
        switch (info.param) {
            case NInterconnect::NRdma::ECqMode::POLLING:
                return "POLLING";
            case NInterconnect::NRdma::ECqMode::EVENT:
                return "EVENT";
        }
    }
);
