#include <util/thread/pool.h>
#include <util/generic/hash_set.h>
#include <util/system/thread.h>
#include <util/system/yield.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <string.h>
#include <vector>

#include <contrib/libs/ibdrv/include/infiniband/verbs.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/events.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/interconnect/rdma/rdma_impl.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/address/interconnect_address.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

#include "utils.h"

using namespace NRdmaTest;
using namespace NInterconnect::NRdma;
using namespace NActors;

static const size_t MEM_REG_SZ = 4096;

class TRdmaLow : public TSkipFixture {};
class TCqMode : public TSkipFixtureWithParams<NInterconnect::NRdma::ECqMode> {};

struct TRegistrationTestCq {
    THolder<TTestActorRuntimeBase> ActorSystem;
    TRdmaCtx* Ctx = nullptr;
    ICq::TPtr Cq;
};

class TReceiveDoneCounterActor : public TActorBootstrapped<TReceiveDoneCounterActor> {
public:
    explicit TReceiveDoneCounterActor(std::shared_ptr<std::atomic<ui32>> counter)
        : Counter(std::move(counter))
    {}

    void Bootstrap() {
        Become(&TReceiveDoneCounterActor::StateFunc);
    }

    void Handle(TEvRdmaIoReceiveDone::TPtr&) {
        Counter->fetch_add(1, std::memory_order_relaxed);
    }

    void Handle(TEvents::TEvPing::TPtr& ev) {
        Send(ev->Sender, new TEvents::TEvPong());
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRdmaIoReceiveDone, Handle);
        hFunc(TEvents::TEvPing, Handle);
    )

private:
    std::shared_ptr<std::atomic<ui32>> Counter;
};

struct TEvSendReceiveProbeResult : public TEventLocal<TEvSendReceiveProbeResult, EventSpaceBegin(TEvents::ES_PRIVATE) + 1> {
    bool Success = false;
    TString ErrSource;
    TString Payload;
    TDuration Latency;

    TEvSendReceiveProbeResult(bool success, TString errSource, TString payload, TDuration latency)
        : Success(success)
        , ErrSource(std::move(errSource))
        , Payload(std::move(payload))
        , Latency(latency)
    {}
};

struct TEvTriggerCqTerminalError : public TEventLocal<TEvTriggerCqTerminalError, EventSpaceBegin(TEvents::ES_PRIVATE) + 2> {
    std::shared_ptr<std::atomic<bool>> Ready;
    std::shared_ptr<std::atomic<bool>> Start;

    TEvTriggerCqTerminalError() = default;

    TEvTriggerCqTerminalError(std::shared_ptr<std::atomic<bool>> ready, std::shared_ptr<std::atomic<bool>> start)
        : Ready(std::move(ready))
        , Start(std::move(start))
    {}
};

struct TEvCqTerminalErrorTriggered : public TEventLocal<TEvCqTerminalErrorTriggered, EventSpaceBegin(TEvents::ES_PRIVATE) + 3> {
    bool Success = false;
    TDuration NotifyErrDuration;

    TEvCqTerminalErrorTriggered(bool success, TDuration notifyErrDuration)
        : Success(success)
        , NotifyErrDuration(notifyErrDuration)
    {}
};

struct TEvReceiveTerminalProbeResult : public TEventLocal<TEvReceiveTerminalProbeResult, EventSpaceBegin(TEvents::ES_PRIVATE) + 4> {
    ui32 QpNum = 0;
    TString ErrSource;

    TEvReceiveTerminalProbeResult(ui32 qpNum, TString errSource)
        : QpNum(qpNum)
        , ErrSource(std::move(errSource))
    {}
};

class TReceiveDoneProbeActor : public TActorBootstrapped<TReceiveDoneProbeActor> {
public:
    TReceiveDoneProbeActor(TActorId edge, std::shared_ptr<std::atomic<ui64>> sendTsUs)
        : Edge(edge)
        , SendTsUs(std::move(sendTsUs))
    {}

    void Bootstrap() {
        Become(&TReceiveDoneProbeActor::StateFunc);
    }

    void Handle(TEvRdmaIoReceiveDone::TPtr& ev) {
        const ui64 sendTsUs = SendTsUs->load(std::memory_order_acquire);
        const TDuration latency = TInstant::Now() - TInstant::MicroSeconds(sendTsUs);

        TString payload;
        if (ev->Get()->IsSuccess()) {
            const auto& received = std::get<TEvRdmaIoReceiveDone::TSuccess>(ev->Get()->Record).Buf;
            payload = TString(received.GetData(), received.GetSize());
        }

        Send(Edge, new TEvSendReceiveProbeResult(ev->Get()->IsSuccess(), TString(ev->Get()->GetErrSource()), std::move(payload), latency));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRdmaIoReceiveDone, Handle);
    )

private:
    TActorId Edge;
    std::shared_ptr<std::atomic<ui64>> SendTsUs;
};

class TReceiveTerminalProbeActor : public TActorBootstrapped<TReceiveTerminalProbeActor> {
public:
    TReceiveTerminalProbeActor(TActorId edge, ui32 qpNum)
        : Edge(edge)
        , QpNum(qpNum)
    {}

    void Bootstrap() {
        Become(&TReceiveTerminalProbeActor::StateFunc);
    }

    void Handle(TEvRdmaIoReceiveDone::TPtr& ev) {
        Send(Edge, new TEvReceiveTerminalProbeResult(QpNum, TString(ev->Get()->GetErrSource())));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRdmaIoReceiveDone, Handle);
    )

private:
    TActorId Edge;
    ui32 QpNum;
};

class TNotifyCqTerminalErrorActor : public TActorBootstrapped<TNotifyCqTerminalErrorActor> {
public:
    TNotifyCqTerminalErrorActor(ICq::TPtr cq, TActorId edge)
        : Cq(std::move(cq))
        , Edge(edge)
    {}

    void Bootstrap() {
        Become(&TNotifyCqTerminalErrorActor::StateFunc);
    }

    void Handle(TEvTriggerCqTerminalError::TPtr& ev) {
        if (ev->Get()->Ready && ev->Get()->Start) {
            ev->Get()->Ready->store(true, std::memory_order_release);
            while (!ev->Get()->Start->load(std::memory_order_acquire)) {
                ThreadYield();
            }
        }
        auto* cqImpl = dynamic_cast<TSimpleCqBase*>(Cq.get());
        const TInstant start = TInstant::Now();
        if (cqImpl) {
            cqImpl->NotifyErr();
        }
        Send(Edge, new TEvCqTerminalErrorTriggered(cqImpl != nullptr, TInstant::Now() - start));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvTriggerCqTerminalError, Handle);
    )

private:
    ICq::TPtr Cq;
    TActorId Edge;
};

static TRegistrationTestCq CreateRegistrationTestCq(TString bindTo, ECqMode mode, TRdmaRuntimeParams params) {
    auto [actorSystem, ctx] = PrepareTestRuntime(bindTo);
    auto memPool = CreateDummyMemPool();

    ICq::TPtr cq;
    switch (mode) {
        case ECqMode::POLLING:
            cq = CreateSimpleCq(ctx, actorSystem->GetActorSystem(0), params, std::move(memPool), nullptr);
            break;
        case ECqMode::EVENT:
            cq = CreateSimpleEventDrivenCq(ctx, actorSystem->GetActorSystem(0), params, std::move(memPool), nullptr);
            break;
    }

    return TRegistrationTestCq{
        .ActorSystem = std::move(actorSystem),
        .Ctx = ctx,
        .Cq = std::move(cq),
    };
}

static void NotifyCqTerminalError(ICq::TPtr cq) {
    auto* cqImpl = dynamic_cast<TSimpleCqBase*>(cq.get());
    ASSERT_TRUE(cqImpl);
    cqImpl->NotifyErr();
}

static NInterconnect::NRdma::TMemRegionPtr AllocSourceRegion(std::shared_ptr<IMemPool> memPool) {
    auto reg = memPool->Alloc(MEM_REG_SZ, IMemPool::EMPTY);
    memset(reg->GetAddr(), 0, MEM_REG_SZ);
    const char* testString = "-_RDMA_YDB_INTERCONNECT_-";
    strncpy((char*)reg->GetAddr(), testString, MEM_REG_SZ);
    return reg;
}

static void ConnectQps(TRdmaCtx* ctx, TQueuePair& qp1, TQueuePair& qp2) {
    ASSERT_EQ(qp1.ToRtsState(THandshakeData{
        .QpNum = qp2.GetQpNum(),
        .SubnetPrefix = ctx->GetGid().global.subnet_prefix,
        .InterfaceId = ctx->GetGid().global.interface_id,
        .MtuIndex = ctx->GetPortAttr().active_mtu,
    }), 0);

    ASSERT_EQ(qp2.ToRtsState(THandshakeData{
        .QpNum = qp1.GetQpNum(),
        .SubnetPrefix = ctx->GetGid().global.subnet_prefix,
        .InterfaceId = ctx->GetGid().global.interface_id,
        .MtuIndex = ctx->GetPortAttr().active_mtu,
    }), 0);
}

void DoReadInOneProcess(TString bindTo, NInterconnect::NRdma::ECqMode mode) {
    auto rdma = InitLocalRdmaStuff(bindTo, mode);

    auto reg1 = AllocSourceRegion(rdma->MemPool);
    auto reg2 = rdma->MemPool->Alloc(MEM_REG_SZ, 0);

    ReadOneMemRegion(rdma, rdma->Qp2, reg1->GetAddr(), reg1->GetRKey(rdma->Ctx->GetDeviceIndex()), MEM_REG_SZ, reg2);

    ASSERT_TRUE(strncmp((char*)reg1->GetAddr(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
}

void DoSendReceiveInOneProcess(TString bindTo, NInterconnect::NRdma::ECqMode mode) {
    static constexpr ui32 ReceiveBufSz = 1024;
    auto rdma = CreateRegistrationTestCq(bindTo, mode, TRdmaRuntimeParams{
        .MaxCqe = 16,
        .MaxWr = 4,
        .MaxSrqWr = 8,
        .RecieveBufSz = ReceiveBufSz,
    });
    ASSERT_TRUE(rdma.Cq);
    ASSERT_TRUE(rdma.Cq->GetSrq());

    auto senderQp = std::make_shared<TQueuePair>();
    auto receiverQp = std::make_shared<TQueuePair>();
    ASSERT_EQ(senderQp->Init(rdma.Ctx, rdma.Cq.get(), 16), 0);
    ASSERT_EQ(receiverQp->Init(rdma.Ctx, rdma.Cq.get(), 16), 0);
    ConnectQps(rdma.Ctx, *senderQp, *receiverQp);

    const TActorId edge = rdma.ActorSystem->AllocateEdgeActor(0);
    auto sendTsUs = std::make_shared<std::atomic<ui64>>(0);
    const TActorId receiverActor = rdma.ActorSystem->Register(new TReceiveDoneProbeActor(edge, sendTsUs));
    ASSERT_TRUE(rdma.Cq->RegisterQpAsync(receiverQp->GetQpNum(), receiverActor));

    const TString payload = "RDMA_SEND_RECEIVE_LOW_LEVEL_TEST";
    ASSERT_LT(payload.size(), ReceiveBufSz);

    auto sendMemPool = CreateDummyMemPool();
    auto sendRegion = sendMemPool->Alloc(payload.size(), IMemPool::EMPTY);
    ASSERT_TRUE(sendRegion);
    memcpy(sendRegion->GetAddr(), payload.data(), payload.size());

    struct TSendResult {
        int Err = 0;
        bool BadWr = false;
    };

    auto sendPromise = NThreading::NewPromise<TSendResult>();
    auto sendFuture = sendPromise.GetFuture();
    TThread senderThread([sendPromise, senderQp, sendRegion, sendTsUs, deviceIndex = rdma.Ctx->GetDeviceIndex(), payloadSize = payload.size()]() mutable {
        ibv_sge sg = {
            .addr = reinterpret_cast<ui64>(sendRegion->GetAddr()),
            .length = static_cast<ui32>(payloadSize),
            .lkey = sendRegion->GetLKey(deviceIndex),
        };
        ibv_send_wr wr = {
            .sg_list = &sg,
            .num_sge = 1,
            .opcode = IBV_WR_SEND,
        };
        ibv_send_wr* badWr = nullptr;

        TSendResult result;
        sendTsUs->store(TInstant::Now().MicroSeconds(), std::memory_order_release);
        result.Err = senderQp->PostSend(&wr, &badWr);
        result.BadWr = badWr != nullptr;
        sendPromise.SetValue(result);
    });
    senderThread.Start();

    const TSendResult sendResult = sendFuture.GetValueSync();
    senderThread.Join();
    ASSERT_EQ(sendResult.Err, 0);
    ASSERT_FALSE(sendResult.BadWr);

    auto ev = rdma.ActorSystem->GrabEdgeEvent<TEvSendReceiveProbeResult>(edge, TDuration::Seconds(5));
    ASSERT_TRUE(ev);
    ASSERT_TRUE(ev->Get()->Success) << ev->Get()->ErrSource;

    Cerr << "RDMA send/receive actor latency: " << ev->Get()->Latency.MicroSeconds() << " us" << Endl;
    ::testing::Test::RecordProperty("SendReceiveActorLatencyUs", ev->Get()->Latency.MicroSeconds());
    EXPECT_EQ(ev->Get()->Payload, payload);

    EXPECT_TRUE(rdma.Cq->DeregisterQpAsync(receiverQp->GetQpNum()));
}

void DoSendReceiveViaBuilderInOneProcess(TString bindTo, NInterconnect::NRdma::ECqMode mode) {
    static constexpr ui32 ReceiveBufSz = 1024;
    auto rdma = CreateRegistrationTestCq(bindTo, mode, TRdmaRuntimeParams{
        .MaxCqe = 16,
        .MaxWr = 4,
        .MaxSrqWr = 8,
        .RecieveBufSz = ReceiveBufSz,
    });
    ASSERT_TRUE(rdma.Cq);
    ASSERT_TRUE(rdma.Cq->GetSrq());

    auto senderQp = std::make_shared<TQueuePair>();
    auto receiverQp = std::make_shared<TQueuePair>();
    ASSERT_EQ(senderQp->Init(rdma.Ctx, rdma.Cq.get(), 16), 0);
    ASSERT_EQ(receiverQp->Init(rdma.Ctx, rdma.Cq.get(), 16), 0);
    ConnectQps(rdma.Ctx, *senderQp, *receiverQp);

    const TActorId edge = rdma.ActorSystem->AllocateEdgeActor(0);
    auto sendTsUs = std::make_shared<std::atomic<ui64>>(0);
    const TActorId receiverActor = rdma.ActorSystem->Register(new TReceiveDoneProbeActor(edge, sendTsUs));
    ASSERT_TRUE(rdma.Cq->RegisterQpAsync(receiverQp->GetQpNum(), receiverActor));

    const TString payload = "RDMA_SEND_RECEIVE_BUILDER_TEST";
    ASSERT_LT(payload.size(), ReceiveBufSz);

    auto sendMemPool = CreateDummyMemPool();
    auto sendBuf = sendMemPool->AllocRcBuf(payload.size(), IMemPool::EMPTY);
    ASSERT_TRUE(sendBuf);
    memcpy(sendBuf->UnsafeGetDataMut(), payload.data(), payload.size());

    auto sendPromise = NThreading::NewPromise<TEvRdmaIoDone*>();
    auto sendFuture = sendPromise.GetFuture();

    auto builder = CreateIbVerbsBuilder(1);
    ASSERT_TRUE(builder);
    builder->AddSendVerb(*sendBuf, [sendPromise](TActorSystem*, TEvRdmaIoDone* ev) mutable {
        sendPromise.SetValue(ev);
    });

    sendTsUs->store(TInstant::Now().MicroSeconds(), std::memory_order_release);
    auto submitErr = rdma.Cq->DoWrBatchAsync(senderQp, std::move(builder));
    ASSERT_FALSE(submitErr);

    auto receiveEv = rdma.ActorSystem->GrabEdgeEvent<TEvSendReceiveProbeResult>(edge, TDuration::Seconds(5));
    ASSERT_TRUE(receiveEv);
    ASSERT_TRUE(receiveEv->Get()->Success) << receiveEv->Get()->ErrSource;
    EXPECT_EQ(receiveEv->Get()->Payload, payload);

    ASSERT_TRUE(sendFuture.Wait(TDuration::Seconds(5)));
    std::unique_ptr<TEvRdmaIoDone> sendDone(sendFuture.GetValueSync());
    ASSERT_TRUE(sendDone->IsSuccess()) << sendDone->GetErrSource();

    EXPECT_TRUE(rdma.Cq->DeregisterQpAsync(receiverQp->GetQpNum()));
}

TEST_P(TCqMode, ReadInOneProcessIpV4) {
    DoReadInOneProcess("127.0.0.1", GetParam());
}

TEST_P(TCqMode, ReadInOneProcessIpV6) {
    DoReadInOneProcess("::1", GetParam());
}

TEST_P(TCqMode, SendReceiveInOneProcessIpV4) {
    DoSendReceiveInOneProcess("127.0.0.1", GetParam());
}

TEST_P(TCqMode, SendReceiveInOneProcessIpV6) {
    DoSendReceiveInOneProcess("::1", GetParam());
}

TEST_P(TCqMode, SendReceiveViaBuilderInOneProcessIpV4) {
    DoSendReceiveViaBuilderInOneProcess("127.0.0.1", GetParam());
}

TEST_P(TCqMode, SendReceiveViaBuilderInOneProcessIpV6) {
    DoSendReceiveViaBuilderInOneProcess("::1", GetParam());
}

TEST_P(TCqMode, RegisterQpWithoutSrqIsRejected) {
    auto rdma = CreateRegistrationTestCq("127.0.0.1", GetParam(), TRdmaRuntimeParams{
        .MaxCqe = 8,
        .MaxWr = 4,
        .MaxSrqWr = 0,
        .RecieveBufSz = 0,
    });
    ASSERT_TRUE(rdma.Cq);

    const TActorId edge = rdma.ActorSystem->AllocateEdgeActor(0);
    EXPECT_FALSE(rdma.Cq->RegisterQpAsync(42, edge));
    EXPECT_FALSE(rdma.Cq->DeregisterQpAsync(42));
}

TEST_P(TCqMode, QpCanBeCreatedWithSrq) {
    auto rdma = CreateRegistrationTestCq("127.0.0.1", GetParam(), TRdmaRuntimeParams{
        .MaxCqe = 8,
        .MaxWr = 4,
        .MaxSrqWr = 4,
        .RecieveBufSz = 1024,
    });
    ASSERT_TRUE(rdma.Cq);
    ASSERT_TRUE(rdma.Cq->GetSrq());

    TQueuePair qp;
    EXPECT_EQ(qp.Init(rdma.Ctx, rdma.Cq.get(), 16), 0);
}

TEST_P(TCqMode, RegisteredQpGetsTerminalReceiveError) {
    auto rdma = CreateRegistrationTestCq("127.0.0.1", GetParam(), TRdmaRuntimeParams{
        .MaxCqe = 8,
        .MaxWr = 4,
        .MaxSrqWr = 4,
        .RecieveBufSz = 1024,
    });
    ASSERT_TRUE(rdma.Cq);

    const TActorId edge = rdma.ActorSystem->AllocateEdgeActor(0);
    ASSERT_TRUE(rdma.Cq->RegisterQpAsync(42, edge));

    NotifyCqTerminalError(rdma.Cq);

    auto ev = rdma.ActorSystem->GrabEdgeEvent<TEvRdmaIoReceiveDone>(edge, TDuration::Seconds(5));
    ASSERT_TRUE(ev);
    EXPECT_TRUE(ev->Get()->IsCqError());
    EXPECT_FALSE(rdma.Cq->RegisterQpAsync(43, edge));
}

TEST_P(TCqMode, NotifyErrFromActorNotifiesAllRegisteredQpActors) {
    auto rdma = CreateRegistrationTestCq("127.0.0.1", GetParam(), TRdmaRuntimeParams{
        .MaxCqe = 16,
        .MaxWr = 4,
        .MaxSrqWr = 8,
        .RecieveBufSz = 1024,
    });
    ASSERT_TRUE(rdma.Cq);

    const TActorId edge = rdma.ActorSystem->AllocateEdgeActor(0);
    const std::array<ui32, 4> qpNums = {42, 43, 44, 45};
    for (ui32 qpNum : qpNums) {
        const TActorId actor = rdma.ActorSystem->Register(new TReceiveTerminalProbeActor(edge, qpNum));
        ASSERT_TRUE(rdma.Cq->RegisterQpAsync(qpNum, actor));
    }

    const TActorId notifier = rdma.ActorSystem->Register(new TNotifyCqTerminalErrorActor(rdma.Cq, edge));
    rdma.ActorSystem->Send(new IEventHandle(notifier, edge, new TEvTriggerCqTerminalError()), 0);

    auto triggerResult = rdma.ActorSystem->GrabEdgeEvent<TEvCqTerminalErrorTriggered>(edge, TDuration::Seconds(5));
    ASSERT_TRUE(triggerResult);
    ASSERT_TRUE(triggerResult->Get()->Success);

    THashSet<ui32> notifiedQps;
    for (size_t i = 0; i < qpNums.size(); ++i) {
        auto ev = rdma.ActorSystem->GrabEdgeEvent<TEvReceiveTerminalProbeResult>(edge, TDuration::Seconds(5));
        ASSERT_TRUE(ev);
        EXPECT_EQ(ev->Get()->ErrSource, "TCqErr");
        notifiedQps.insert(ev->Get()->QpNum);
    }

    EXPECT_EQ(notifiedQps.size(), qpNums.size());
    for (ui32 qpNum : qpNums) {
        EXPECT_TRUE(notifiedQps.contains(qpNum)) << "Missing terminal notification for qp " << qpNum;
    }
    EXPECT_FALSE(rdma.Cq->RegisterQpAsync(46, edge));
}

TEST_P(TCqMode, ConcurrentRegisterAndNotifyErrEitherRejectsOrNotifies) {
    static constexpr ui32 Iterations = 1000;
    ui32 notificationCount = 0;
    ui32 registerRejectedCount = 0;
    std::vector<TDuration> notifyErrDurations;
    notifyErrDurations.reserve(Iterations);

    for (ui32 i = 0; i < Iterations; ++i) {
        auto rdma = CreateRegistrationTestCq("127.0.0.1", GetParam(), TRdmaRuntimeParams{
            .MaxCqe = 16,
            .MaxWr = 4,
            .MaxSrqWr = 8,
            .RecieveBufSz = 1024,
        });
        ASSERT_TRUE(rdma.Cq);

        const TActorId edge = rdma.ActorSystem->AllocateEdgeActor(0);
        const ui32 qpNum = 10000 + i;
        const TActorId receiver = rdma.ActorSystem->Register(new TReceiveTerminalProbeActor(edge, qpNum));
        const TActorId notifier = rdma.ActorSystem->Register(new TNotifyCqTerminalErrorActor(rdma.Cq, edge));

        auto ready = std::make_shared<std::atomic<bool>>(false);
        auto start = std::make_shared<std::atomic<bool>>(false);
        auto registered = std::make_shared<std::atomic<bool>>(false);

        TThread registerThread([cq = rdma.Cq, receiver, qpNum, ready, start, registered]() {
            while (!ready->load(std::memory_order_acquire)) {
                ThreadYield();
            }
            start->store(true, std::memory_order_release);
            registered->store(cq->RegisterQpAsync(qpNum, receiver), std::memory_order_release);
        });
        registerThread.Start();

        rdma.ActorSystem->Send(new IEventHandle(notifier, edge, new TEvTriggerCqTerminalError(ready, start)), 0);

        auto triggerResult = rdma.ActorSystem->GrabEdgeEvent<TEvCqTerminalErrorTriggered>(edge, TDuration::Seconds(5));
        registerThread.Join();
        ASSERT_TRUE(triggerResult) << "iteration " << i;
        ASSERT_TRUE(triggerResult->Get()->Success) << "iteration " << i;
        notifyErrDurations.push_back(triggerResult->Get()->NotifyErrDuration);

        if (registered->load(std::memory_order_acquire)) {
            auto ev = rdma.ActorSystem->GrabEdgeEvent<TEvReceiveTerminalProbeResult>(edge, TDuration::Seconds(5));
            ASSERT_TRUE(ev) << "iteration " << i;
            EXPECT_EQ(ev->Get()->QpNum, qpNum) << "iteration " << i;
            EXPECT_EQ(ev->Get()->ErrSource, "TCqErr") << "iteration " << i;
            ++notificationCount;
        } else {
            ++registerRejectedCount;
        }
    }

    ASSERT_EQ(notifyErrDurations.size(), Iterations);
    std::sort(notifyErrDurations.begin(), notifyErrDurations.end());
    const size_t p99Index = (notifyErrDurations.size() * 99 + 99) / 100 - 1;
    const TDuration notifyErrP99 = notifyErrDurations[p99Index];

    Cerr << "Concurrent Register/NotifyErr stats: notifications=" << notificationCount
         << ", registerRejected=" << registerRejectedCount
         << ", notifyErrP99Us=" << notifyErrP99.MicroSeconds()
         << Endl;
    ::testing::Test::RecordProperty("NotificationCount", notificationCount);
    ::testing::Test::RecordProperty("RegisterRejectedCount", registerRejectedCount);
    ::testing::Test::RecordProperty("NotifyErrP99Us", notifyErrP99.MicroSeconds());

    EXPECT_EQ(notificationCount + registerRejectedCount, Iterations);
}

TEST_P(TCqMode, DeregisteredQpDoesNotGetTerminalReceiveError) {
    auto rdma = CreateRegistrationTestCq("127.0.0.1", GetParam(), TRdmaRuntimeParams{
        .MaxCqe = 8,
        .MaxWr = 4,
        .MaxSrqWr = 4,
        .RecieveBufSz = 1024,
    });
    ASSERT_TRUE(rdma.Cq);

    auto receiveCounter = std::make_shared<std::atomic<ui32>>(0);
    const TActorId deregisteredActor = rdma.ActorSystem->Register(new TReceiveDoneCounterActor(receiveCounter));
    const TActorId edge = rdma.ActorSystem->AllocateEdgeActor(0);

    ASSERT_TRUE(rdma.Cq->RegisterQpAsync(42, deregisteredActor));
    ASSERT_TRUE(rdma.Cq->DeregisterQpAsync(42));
    ASSERT_TRUE(rdma.Cq->RegisterQpAsync(43, edge));

    NotifyCqTerminalError(rdma.Cq);

    auto ev = rdma.ActorSystem->GrabEdgeEvent<TEvRdmaIoReceiveDone>(edge, TDuration::Seconds(5));
    ASSERT_TRUE(ev);
    EXPECT_TRUE(ev->Get()->IsCqError());

    rdma.ActorSystem->Send(new IEventHandle(deregisteredActor, edge, new TEvents::TEvPing()), 0);
    auto pong = rdma.ActorSystem->GrabEdgeEvent<TEvents::TEvPong>(edge, TDuration::Seconds(5));
    ASSERT_TRUE(pong);
    EXPECT_EQ(receiveCounter->load(std::memory_order_relaxed), 0);
    EXPECT_FALSE(rdma.Cq->DeregisterQpAsync(42));
}

/*
 * This test covers the situation when sender is going to reuse memory but has no
 * information about remote reading in progress.
 * In this case we change QP on the sender to the 'Reset' state and expect reader will fail with read error.
 */
TEST_P(TCqMode, ReadInOneProcessWithQpInterruption) {
    TString addr = "127.0.0.1";

    auto rdma = InitLocalRdmaStuff(addr, GetParam());

    THolder<IThreadPool> pool = CreateThreadPool(2, 2);
    const int initialAttempts = 50000;

    // Use attempt as timeout to delay the memory corruptor.
    int attempt = initialAttempts;

    // Binary search is unstable here due to the small amount of time to trigger the race.
    while (attempt--) {
        auto reg1 = AllocSourceRegion(rdma->MemPool);
        auto reg2 = rdma->MemPool->Alloc(reg1->GetSize(), 0);
        std::vector<char> expected(reg1->GetSize());
        memcpy(expected.data(), (char*)reg1->GetAddr(), reg1->GetSize());

        NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
        NThreading::TFuture<void> done = promise.GetFuture();

        class TMemCorruptor : public IObjectInQueue {
        public:
            TMemCorruptor(char* mem, size_t sz, TQueuePair* qp, int attempt, NThreading::TPromise<void> promise)
                : Mem(mem)
                , Sz(sz)
                , Qp(qp)
                , Attempt(attempt)
                , Promise(std::move(promise))
            {}
            virtual void Process(void*) override {
                // Delay to get a chance to trigger memset just during the RDMA read.
                Sleep(TDuration::MicroSeconds(Attempt / 128));
                Qp->ToErrorState();
                memset(Mem, 'Q', Sz);
                Promise.SetValue();
                delete this;
            }
        private:
            char* Mem;
            size_t Sz;
            TQueuePair* Qp;
            const int Attempt;
            NThreading::TPromise<void> Promise;
        };

        std::function<void()> srcInterruptHook = [&]() noexcept {
            bool added = pool->Add(
                new TMemCorruptor((char*)reg1->GetAddr(), reg1->GetSize(), rdma->Qp1.get(), attempt, std::move(promise))
            );
            Y_ABORT_UNLESS(added);
        };

        auto readResult = ReadOneMemRegion(rdma, rdma->Qp2, reg1->GetAddr(), reg1->GetRKey(rdma->Ctx->GetDeviceIndex()), MEM_REG_SZ, reg2, std::move(srcInterruptHook));

        // Wait until corruptor finished.
        done.Wait();

        switch (readResult) {
            case EReadResult::OK: // corruptor fired too late, just check data is ok
                {
                    ASSERT_TRUE(strncmp(expected.data(), (char*)reg2->GetAddr(), MEM_REG_SZ) == 0);
                    // Additional check CQ has no leaked WR after async completion callback returns.
                    // In CQ processing we call wr->Reply(...) first and ReturnWr(wr) second. ReadOneMemRegion()
                    // unblocks on the callback from Reply(), so immediately after it returns we may observe
                    // a transient "allocated WR still not returned" state (Ready < Total), especially in EVENT mode.
                    // Bounded waiting (<=100ms) keeps this check strict for real leaks while tolerating that ordering race.
                    ICq::TWrStats stats = rdma->CqPtr->GetWrStats();
                    for (ui32 i = 0; i < 2000 && stats.Ready != stats.Total; ++i) {
                        Sleep(TDuration::MicroSeconds(50));
                        stats = rdma->CqPtr->GetWrStats();
                    }
                    EXPECT_TRUE(stats.Total > 0);
                    EXPECT_EQ(stats.Ready, stats.Total);
                }
                break;
            case EReadResult::WRPOST_ERR: // corruptor fired too early, increase timeout
                attempt = std::min(initialAttempts, attempt *= 2);
                break;
            case EReadResult::READ_ERR:
                Cerr << "passed at " << attempt << Endl;
                return;
        }
        if (attempt == 0) {
            Cerr << "race was not triggered, restart..." << Endl;
            attempt = initialAttempts;
        }

        {
            rdma->Qp1->ToResetState();
            rdma->Qp2->ToResetState();

            auto qp1num = rdma->Qp1->GetQpNum();

            {
                int err = rdma->Qp2->ToRtsState(NInterconnect::NRdma::THandshakeData {
                    .QpNum = qp1num,
                    .SubnetPrefix = rdma->Ctx->GetGid().global.subnet_prefix,
                    .InterfaceId = rdma->Ctx->GetGid().global.interface_id,
                    .MtuIndex = rdma->Ctx->GetPortAttr().active_mtu
                 });
                EXPECT_TRUE(err == 0);
            }

            {
                int err = rdma->Qp1->ToRtsState(NInterconnect::NRdma::THandshakeData {
                    .QpNum = rdma->Qp2->GetQpNum(),
                    .SubnetPrefix = rdma->Ctx->GetGid().global.subnet_prefix,
                    .InterfaceId = rdma->Ctx->GetGid().global.interface_id,
                    .MtuIndex = rdma->Ctx->GetPortAttr().active_mtu
                });
                EXPECT_TRUE(err == 0);
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    TRdmaLow,
    TCqMode,
    ::testing::Values(
        NInterconnect::NRdma::ECqMode::POLLING,
        NInterconnect::NRdma::ECqMode::EVENT
    ),
    [](const testing::TestParamInfo<NInterconnect::NRdma::ECqMode>& info) {
        switch (info.param) {
            case NInterconnect::NRdma::ECqMode::POLLING:
                return "POLLING";
            case NInterconnect::NRdma::ECqMode::EVENT:
                return "EVENT";
        }
    }
);
