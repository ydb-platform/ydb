#pragma once

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <ydb/library/actors/interconnect/ut/protos/interconnect_test.pb.h>
#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/channel_scheduler.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

#include <util/string/cast.h>

using namespace NActors;

struct TEvTestSerialization : public TEventPB<TEvTestSerialization, NInterconnectTest::TEvTestSerialization, 123> {};

struct TEvSerializeToRopeFailure : public TEventBase<TEvSerializeToRopeFailure, 124> {
    TString Payload;
    mutable ui32 SerializeToRopeCallCount = 0;

    explicit TEvSerializeToRopeFailure(size_t payloadSize = 5000)
        : Payload(payloadSize, 'R')
    {}

    TString ToStringHeader() const override {
        return "TEvSerializeToRopeFailure";
    }

    bool SerializeToArcadiaStream(TChunkSerializer* serializer) const override {
        return serializer->WriteString(&Payload);
    }

    std::optional<TRope> SerializeToRope(IRcBufAllocator*) const override {
        ++SerializeToRopeCallCount;
        return std::nullopt;
    }

    TEventSerializationInfo CreateSerializationInfo(bool allowExternalDataChannel) const override {
        if (!allowExternalDataChannel) {
            return {};
        }
        TEventSerializationInfo info;
        info.Sections.push_back(TEventSectionInfo{0, Payload.size(), 0, 0, false, true});
        return info;
    }

    ui32 CalculateSerializedSize() const override {
        return Payload.size();
    }

    bool IsSerializable() const override {
        return true;
    }
};

inline void GTestSkip() {
    GTEST_SKIP() << "Skipping all rdma tests for suite, set \""
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

class RdmaSendReceiveTestCqMode : public ::testing::TestWithParam<NInterconnect::NRdma::ECqMode> {
public:
    void SetUp() override {
        using namespace NRdmaTest;
        if (IsRdmaTestDisabled()) {
            GTestSkip();
        }
    }
};

struct TRdmaTransportTestParams {
    NInterconnect::NRdma::ECqMode CqMode = NInterconnect::NRdma::ECqMode::EVENT;
    bool EnableSendReceive = false;
};

class XdcRdmaTransportTest : public ::testing::TestWithParam<TRdmaTransportTestParams> {
public:
    void SetUp() override {
        using namespace NRdmaTest;
        if (IsRdmaTestDisabled()) {
            GTestSkip();
        }
    }
};

// These tests retain SlotMemPool-backed payloads before main-channel metadata is serialized.
// Keep them on TCP main until RDMA control-buffer allocation guarantees forward progress under pool pressure.
class XdcRdmaPoolPressureTest : public XdcRdmaTransportTest {
};

class TSendActor: public TActorBootstrapped<TSendActor> {
public:
    struct TExtCtx {
        std::atomic<bool> Undelivered = false;
        bool WaitForUndelivered(ui32 maxAttempt) {
            while (Undelivered.load(std::memory_order_relaxed) == false && maxAttempt) {
                Sleep(TDuration::MilliSeconds(1000));
                maxAttempt--;
            }
            return Undelivered.load(std::memory_order_relaxed);
        }
    };

    TSendActor(TActorId recipient, std::unique_ptr<IEventBase>&& ev, std::shared_ptr<TExtCtx> ctx = nullptr)
        : Recipient(recipient)
        , Event(std::move(ev))
        , Ctx(ctx)
    {}

    void Bootstrap() {
        Send(Recipient, std::move(Event), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
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
    std::unique_ptr<IEventBase> Event;
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
    bool WaitForReceive(ui32 expected, ui32 maxAttempt) {
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
    std::vector<std::unique_ptr<IEventBase>> Events;
    std::unordered_map<ui64, std::function<void(TEvTestSerialization*)>> Checks;
    NMonitoring::TDynamicCounterPtr Counters;
    std::shared_ptr<NInterconnect::NRdma::IMemPool> MemPool;

    TEventsForTest(ui32 numEvents, bool shuffle = false)
        : Counters(new NMonitoring::TDynamicCounters())
        , MemPool(NInterconnect::NRdma::CreateSlotMemPool(Counters.Get(), {}))
    {
        Generate(numEvents, MemPool.get(), shuffle);
    }

    void Generate(ui32 numEvents, NInterconnect::NRdma::IMemPool* memPool, bool shuffle = false) {
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

            auto ev = std::make_unique<TEvTestSerialization>();
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
            if (shuffle) {
                for (ui32 j = 0; j < numPayloads; ++j) {
                    ev->AddPayload(TRope(TString(10 + j, j + i)));
                    ev->AddPayload(TRope(TString(5000 + j, j + i)));
                    auto buf = memPool->AllocRcBuf(5000 + j, 0).value();
                    std::fill(buf.GetDataMut(), buf.GetDataMut() + 5000 + j, j + i);
                    ev->AddPayload(TRope(std::move(buf)));
                    UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().back().size(), 5000 + j);
                }
            }

            if (isXdc || isRdma) {
                UNIT_ASSERT(ev->AllowExternalDataChannel());
            }

            Events.push_back(std::move(ev));

            Checks.emplace(i, [i, numPayloads, isInline, sz, shuffle](TEvTestSerialization* ev) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetBlobID(), i);
                UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetBuffer(), TStringBuilder{} << "hello world " << i);
                UNIT_ASSERT_VALUES_EQUAL(ev->GetPayload().size(), numPayloads * (shuffle ? 4 : 1));
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

inline TEvTestSerialization* MakeMultiGlueTestEvent(ui64 blobId, NInterconnect::NRdma::IMemPool* memPool) {
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

inline TEvTestSerialization* MakeTestEvent(ui64 blobId, NInterconnect::NRdma::IMemPool* memPool = nullptr, bool withGlue = false, bool withOffset = false) {
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
        // TRope can "glue" rcbufs if they have the same backend and are placed in contiguous memory regions.
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

inline bool WaitForRdmaChecksumStatus(TTestICCluster& cluster, ui32 me, ui32 peer, const TString& expected, ui32 maxAttempt,
        TString& lastStatus)
{
    while (maxAttempt--) {
        try {
            lastStatus = GetRdmaChecksumStatus(cluster, me, peer);
            if (lastStatus == expected) {
                return true;
            }
        } catch (const TPatternNotFound&) {
            lastStatus.clear();
        }
        Sleep(TDuration::Seconds(1));
    }
    return false;
}

inline bool WaitForRdmaSessionDropOrStatus(TTestICCluster& cluster, ui32 me, ui32 peer, const TString& expected, ui32 maxAttempt,
        TString& lastStatus)
{
    ui32 missingAttempts = 0;
    while (maxAttempt--) {
        try {
            lastStatus = GetRdmaChecksumStatus(cluster, me, peer);
            missingAttempts = 0;
            if (lastStatus == expected) {
                return true;
            }
        } catch (const TPatternNotFound&) {
            lastStatus.clear();
            if (++missingAttempts >= 2) {
                return true;
            }
        }
        Sleep(TDuration::Seconds(1));
    }
    return false;
}

inline TString FormatLastRdmaStatus(const TString& status) {
    return status.empty() ? TString("<no session>") : status;
}

inline ui64 GetSessionCounter(TTestICCluster& cluster, ui32 me, ui32 peer, TStringBuf name) {
    const TString start = TStringBuilder() << "<tr><td>" << name << "</td><td>";
    return FromString<ui64>(ExtractPattern(cluster, me, peer, start, "<"));
}

inline ui64 WaitForSessionCounter(TTestICCluster& cluster, ui32 me, ui32 peer, TStringBuf name,
        TDuration timeout = TDuration::Seconds(10)) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        try {
            return GetSessionCounter(cluster, me, peer, name);
        } catch (const TPatternNotFound&) {
            Sleep(TDuration::MilliSeconds(100));
        }
    }
    return GetSessionCounter(cluster, me, peer, name);
}

struct TCounterSumConsumer : NMonitoring::ICountableConsumer {
    const TString CounterName;
    ui64 Sum = 0;

    explicit TCounterSumConsumer(TStringBuf counterName)
        : CounterName(counterName)
    {}

    void OnCounter(const TString& /*labelName*/, const TString& labelValue,
            const NMonitoring::TCounterForPtr* counter) override {
        if (labelValue == CounterName) {
            Sum += counter->Val();
        }
    }

    void OnHistogram(const TString& /*labelName*/, const TString& /*labelValue*/,
            NMonitoring::IHistogramSnapshotPtr /*snapshot*/, bool /*derivative*/) override {
    }

    void OnGroupBegin(const TString& /*labelName*/, const TString& /*labelValue*/,
            const NMonitoring::TDynamicCounters* /*group*/) override {
    }

    void OnGroupEnd(const TString& /*labelName*/, const TString& /*labelValue*/,
            const NMonitoring::TDynamicCounters* /*group*/) override {
    }
};

inline ui64 GetNodeCounterSum(TTestICCluster& cluster, ui32 nodeId, TStringBuf counterName) {
    const auto nodeCounters = cluster.GetCounters()->FindSubgroup("nodeId", ToString(nodeId));
    if (!nodeCounters) {
        return 0;
    }

    TCounterSumConsumer consumer(counterName);
    nodeCounters->Accept({}, {}, consumer);
    return consumer.Sum;
}

inline bool WaitForNodeCounterSum(TTestICCluster& cluster, ui32 nodeId, TStringBuf counterName, ui64 expected,
        TDuration timeout, ui64& lastValue) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        lastValue = GetNodeCounterSum(cluster, nodeId, counterName);
        if (lastValue == expected) {
            return true;
        }
        Sleep(TDuration::MilliSeconds(100));
    }
    return false;
}

class TWaitForConnectionActor: public TActorBootstrapped<TWaitForConnectionActor> {
public:
    TWaitForConnectionActor(ui32 peerNodeId, NThreading::TPromise<bool> promise, ui32 attempts)
        : PeerNodeId(peerNodeId)
        , Promise(std::move(promise))
        , AttemptsLeft(attempts)
    {}

    void Bootstrap() {
        Become(&TWaitForConnectionActor::StateFunc);
        SendConnect();
    }

private:
    void SendConnect() {
        if (!AttemptsLeft) {
            return Finish(false);
        }
        --AttemptsLeft;
        Send(TActivationContext::InterconnectProxy(PeerNodeId), new TEvInterconnect::TEvConnectNode);
    }

    void Finish(bool connected) {
        Promise.SetValue(connected);
        PassAway();
    }

    void Handle(TEvInterconnect::TEvNodeConnected::TPtr&) {
        Send(TActivationContext::InterconnectProxy(PeerNodeId), new TEvents::TEvUnsubscribe);
        Finish(true);
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        SendConnect();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
    )

private:
    const ui32 PeerNodeId;
    NThreading::TPromise<bool> Promise;
    ui32 AttemptsLeft;
};

inline void WaitForInterconnectConnection(TTestICCluster& cluster, ui32 fromNode, ui32 toNode) {
    auto promise = NThreading::NewPromise<bool>();
    auto future = promise.GetFuture();
    cluster.RegisterActor(new TWaitForConnectionActor(toNode, std::move(promise), 200), fromNode);

    const bool connected = future.Wait(TDuration::Seconds(30)) && future.GetValueSync();
    UNIT_ASSERT_C(connected, "failed to establish interconnect session from node " << fromNode << " to node " << toNode);
}

inline TTestICCluster::Flags GetRdmaCqModeFlags(NInterconnect::NRdma::ECqMode cqMode) {
    return cqMode == NInterconnect::NRdma::ECqMode::POLLING
        ? TTestICCluster::RDMA_POLLING_CQ
        : TTestICCluster::EMPTY;
}

inline TTestICCluster::Flags GetRdmaCqModeFlags(const TRdmaTransportTestParams& params) {
    return GetRdmaCqModeFlags(params.CqMode);
}

inline void EnableRdmaSendReceive(ui32, TInterconnectSettings& settings) {
    settings.EnableRdmaSendReceive = true;
}

inline std::function<void(ui32, TInterconnectSettings&)> GetRdmaSettingsCustomizer(
        const TRdmaTransportTestParams& params) {
    return params.EnableSendReceive
        ? std::function<void(ui32, TInterconnectSettings&)>(EnableRdmaSendReceive)
        : std::function<void(ui32, TInterconnectSettings&)>();
}

inline std::string FormatRdmaCqMode(NInterconnect::NRdma::ECqMode cqMode) {
    switch (cqMode) {
        case NInterconnect::NRdma::ECqMode::POLLING:
            return "POLLING";
        case NInterconnect::NRdma::ECqMode::EVENT:
            return "EVENT";
    }
    Y_ABORT("unexpected RDMA CQ mode");
}

inline std::string FormatRdmaTransportParam(const TRdmaTransportTestParams& params) {
    return FormatRdmaCqMode(params.CqMode) + (params.EnableSendReceive ? "_SendReceive" : "_TcpMain");
}

inline TString GetExpectedRdmaStatus(const TRdmaTransportTestParams& params) {
    return params.EnableSendReceive
        ? TString("On | SoftwareChecksum | SendReceive")
        : TString("On | SoftwareChecksum");
}
