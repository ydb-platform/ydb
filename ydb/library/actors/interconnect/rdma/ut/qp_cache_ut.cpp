#include "utils.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/rdma/events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash_set.h>
#include <util/system/yield.h>

#include <atomic>
#include <cerrno>
#include <memory>
#include <variant>
#include <vector>

using namespace NActors;
using namespace NInterconnect::NRdma;
using namespace NRdmaTest;

namespace {

struct TEvQpCacheTestResult
    : public TEventLocal<TEvQpCacheTestResult, EventSpaceBegin(TEvents::ES_PRIVATE) + 100>
{
    bool Success = false;
    bool ProbeFoundEmptyCache = false;
    ui32 EmptyResults = 0;
    ui32 ReadyNotifications = 0;
    TString Error;
    std::vector<TQueuePair::TPtr> Qps;
};

struct TEvStartQpCacheTest
    : public TEventLocal<TEvStartQpCacheTest, EventSpaceBegin(TEvents::ES_PRIVATE) + 101>
{};

struct TStartBarrier {
    std::atomic<ui32> Ready = 0;
};

class TQpCacheClientActor : public TActorBootstrapped<TQpCacheClientActor> {
public:
    TQpCacheClientActor(ICq::TPtr cq, TActorId edge, ui32 qpCount, bool probeEmptyCache,
            std::shared_ptr<TStartBarrier> startBarrier = {})
        : Cq(std::move(cq))
        , Edge(edge)
        , QpCount(qpCount)
        , ProbeEmptyCache(probeEmptyCache)
        , StartBarrier(std::move(startBarrier))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TQpCacheClientActor::StateFunc);
        if (StartBarrier) {
            StartBarrier->Ready.fetch_add(1, std::memory_order_release);
            return;
        }
        GetQps(ctx);
    }

private:
    void GetQps(const TActorContext& ctx) {
        while (Qps.size() < QpCount) {
            auto result = Cq->GetQp(ctx);
            if (auto* qp = std::get_if<TQueuePair::TPtr>(&result)) {
                Qps.push_back(std::move(*qp));
            } else if (std::holds_alternative<ICq::TQpCacheEmpty>(result)) {
                ++EmptyResults;
                return;
            } else if (std::holds_alternative<ICq::TQpNoResources>(result)) {
                return Finish(ctx, false, "QP cache has no resources");
            } else {
                return Finish(ctx, false, "QP cache terminal error");
            }
        }

        if (ProbeEmptyCache) {
            auto result = Cq->GetQp(ctx);
            if (std::holds_alternative<ICq::TQpCacheEmpty>(result)) {
                ++EmptyResults;
                ProbeFoundEmptyCache = true;
            } else if (auto* qp = std::get_if<TQueuePair::TPtr>(&result)) {
                Qps.push_back(std::move(*qp));
                return Finish(ctx, false, "QP cache contains more QPs than configured");
            } else if (std::holds_alternative<ICq::TQpNoResources>(result)) {
                return Finish(ctx, false, "QP cache has no resources during empty-cache probe");
            } else {
                return Finish(ctx, false, "QP cache terminal error during empty-cache probe");
            }
        }

        Finish(ctx, true, {});
    }

    void Handle(TEvQpCacheReady::TPtr&) {
        ++ReadyNotifications;
        GetQps(TActivationContext::AsActorContext());
    }

    void Handle(TEvQpCacheNoResources::TPtr&) {
        Finish(TActivationContext::AsActorContext(), false, "QP cache refill has no resources");
    }

    void Handle(TEvQpCacheError::TPtr&) {
        Finish(TActivationContext::AsActorContext(), false, "QP cache refill terminal error");
    }

    void Handle(TEvStartQpCacheTest::TPtr&) {
        GetQps(TActivationContext::AsActorContext());
    }

    void Finish(const TActorContext& ctx, bool success, TString error) {
        auto result = MakeHolder<TEvQpCacheTestResult>();
        result->Success = success;
        result->ProbeFoundEmptyCache = ProbeFoundEmptyCache;
        result->EmptyResults = EmptyResults;
        result->ReadyNotifications = ReadyNotifications;
        result->Error = std::move(error);
        result->Qps = std::move(Qps);
        Send(Edge, result.Release());
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvQpCacheReady, Handle);
        hFunc(TEvQpCacheNoResources, Handle);
        hFunc(TEvQpCacheError, Handle);
        hFunc(TEvStartQpCacheTest, Handle);
    )

private:
    ICq::TPtr Cq;
    const TActorId Edge;
    const ui32 QpCount;
    const bool ProbeEmptyCache;
    const std::shared_ptr<TStartBarrier> StartBarrier;
    ui32 EmptyResults = 0;
    ui32 ReadyNotifications = 0;
    bool ProbeFoundEmptyCache = false;
    std::vector<TQueuePair::TPtr> Qps;
};

struct TQpCacheTestEnvironment {
    NMonitoring::TDynamicCounterPtr Counters;
    THolder<TTestActorRuntimeBase> ActorSystem;
    TRdmaCtx* Ctx;
    TActorId CqActor;
    ICq::TPtr Cq;
};

TQpCacheTestEnvironment CreateQpCacheTestEnvironment(ui16 cacheSize, bool useRealThreads = true) {
    auto [actorSystem, ctx] = PrepareTestRuntime("127.0.0.1", useRealThreads);
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    const TRdmaRuntimeParams params{
        .MaxCqe = -1,
        .MaxWr = 128,
        .MaxSrqWr = 0,
        .RecieveBufSz = 0,
        .KeepQpPreinited = cacheSize,
        .MaxWrPerQp = 16,
    };
    const TActorId cqActor = actorSystem->Register(CreateCqActor(params, ECqMode::POLLING, counters.Get()));
    if (!useRealThreads) {
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        actorSystem->DispatchEvents(opts);
    }
    ICq::TPtr cq = GetCqHandle(actorSystem.Get(), ctx, cqActor);
    return {
        .Counters = std::move(counters),
        .ActorSystem = std::move(actorSystem),
        .Ctx = ctx,
        .CqActor = cqActor,
        .Cq = std::move(cq),
    };
}

class TQpCacheTest : public TSkipFixture {
};

ui64 GetHistogramSamples(const NMonitoring::THistogramPtr& histogram) {
    auto snapshot = histogram->Snapshot();
    ui64 samples = 0;
    for (ui32 i = 0; i < snapshot->Count(); ++i) {
        samples += snapshot->Value(i);
    }
    return samples;
}

template <typename TPredicate>
bool WaitFor(TPredicate predicate, TDuration timeout = TDuration::Seconds(10)) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (!predicate() && TInstant::Now() < deadline) {
        ThreadYield();
    }
    return predicate();
}

TEST_F(TQpCacheTest, EmptyResultIsFollowedByNotification) {
    auto env = CreateQpCacheTestEnvironment(4);
    const TActorId edge = env.ActorSystem->AllocateEdgeActor();
    env.ActorSystem->Register(new TQpCacheClientActor(env.Cq, edge, 1, false));

    auto ev = env.ActorSystem->GrabEdgeEvent<TEvQpCacheTestResult>(edge, TDuration::Seconds(10));
    ASSERT_TRUE(ev);
    ASSERT_TRUE(ev->Get()->Success) << ev->Get()->Error;
    EXPECT_EQ(ev->Get()->EmptyResults, 1u);
    EXPECT_EQ(ev->Get()->ReadyNotifications, 1u);
    EXPECT_EQ(ev->Get()->Qps.size(), 1u);
}

TEST_F(TQpCacheTest, FillsConfiguredNumberOfQps) {
    constexpr ui16 CacheSize = 17;
    auto env = CreateQpCacheTestEnvironment(CacheSize);
    const TActorId edge = env.ActorSystem->AllocateEdgeActor();
    env.ActorSystem->Register(new TQpCacheClientActor(env.Cq, edge, 1, false));

    auto first = env.ActorSystem->GrabEdgeEvent<TEvQpCacheTestResult>(edge, TDuration::Seconds(10));
    ASSERT_TRUE(first);
    ASSERT_TRUE(first->Get()->Success) << first->Get()->Error;

    auto qpsCreated = env.Counters->FindCounter("RdmaQpCacheQpsCreated");
    auto fillTime = env.Counters->FindHistogram("RdmaQpCacheFillTimeUs");
    ASSERT_TRUE(qpsCreated);
    ASSERT_TRUE(fillTime);
    ASSERT_TRUE(WaitFor([&] {
        return qpsCreated->Val() == CacheSize && GetHistogramSamples(fillTime) == 1;
    }));

    env.ActorSystem->Register(new TQpCacheClientActor(env.Cq, edge, CacheSize - 1, true));
    auto rest = env.ActorSystem->GrabEdgeEvent<TEvQpCacheTestResult>(edge, TDuration::Seconds(10));
    ASSERT_TRUE(rest);
    ASSERT_TRUE(rest->Get()->Success) << rest->Get()->Error;
    EXPECT_TRUE(rest->Get()->ProbeFoundEmptyCache);

    std::vector<TQueuePair::TPtr> qps = std::move(first->Get()->Qps);
    for (auto& qp : rest->Get()->Qps) {
        qps.push_back(std::move(qp));
    }
    ASSERT_EQ(qps.size(), CacheSize);

    THashSet<const TQueuePair*> uniqueQps;
    for (const auto& qp : qps) {
        uniqueQps.insert(qp.get());
    }
    EXPECT_EQ(uniqueQps.size(), CacheSize);
}

TEST_F(TQpCacheTest, ReportsMetrics) {
    constexpr ui16 CacheSize = 4;
    auto env = CreateQpCacheTestEnvironment(CacheSize);
    const TActorId edge = env.ActorSystem->AllocateEdgeActor();
    env.ActorSystem->Register(new TQpCacheClientActor(env.Cq, edge, 1, false));

    auto ev = env.ActorSystem->GrabEdgeEvent<TEvQpCacheTestResult>(edge, TDuration::Seconds(10));
    ASSERT_TRUE(ev);
    ASSERT_TRUE(ev->Get()->Success) << ev->Get()->Error;

    auto hits = env.Counters->FindCounter("RdmaQpCacheHits");
    auto misses = env.Counters->FindCounter("RdmaQpCacheMisses");
    auto qpsCreated = env.Counters->FindCounter("RdmaQpCacheQpsCreated");
    ASSERT_TRUE(hits);
    ASSERT_TRUE(misses);
    ASSERT_TRUE(qpsCreated);
    auto fillTime = env.Counters->FindHistogram("RdmaQpCacheFillTimeUs");
    ASSERT_TRUE(fillTime);
    ASSERT_TRUE(WaitFor([&] {
        return qpsCreated->Val() == CacheSize && GetHistogramSamples(fillTime) == 1;
    }));
    EXPECT_EQ(hits->Val(), 1);
    EXPECT_EQ(misses->Val(), 1);
    EXPECT_EQ(qpsCreated->Val(), CacheSize);

    EXPECT_EQ(GetHistogramSamples(fillTime), 1);
}

TEST_F(TQpCacheTest, ConcurrentConsumers) {
    constexpr ui16 CacheSize = 16;
    constexpr ui32 ClientCount = 2;
    constexpr ui32 QpsPerClient = 32;

    auto env = CreateQpCacheTestEnvironment(CacheSize);
    const TActorId edge = env.ActorSystem->AllocateEdgeActor();
    auto startBarrier = std::make_shared<TStartBarrier>();
    std::vector<TActorId> clients;
    for (ui32 i = 0; i < ClientCount; ++i) {
        clients.push_back(env.ActorSystem->Register(
            new TQpCacheClientActor(env.Cq, edge, QpsPerClient, false, startBarrier)));
    }

    ASSERT_TRUE(WaitFor([&] {
        return startBarrier->Ready.load(std::memory_order_acquire) == ClientCount;
    }));
    for (const TActorId client : clients) {
        env.ActorSystem->Send(new IEventHandle(client, edge, new TEvStartQpCacheTest), 0);
    }

    std::vector<TQueuePair::TPtr> qps;
    ui32 emptyResults = 0;
    ui32 readyNotifications = 0;
    for (ui32 i = 0; i < ClientCount; ++i) {
        auto ev = env.ActorSystem->GrabEdgeEvent<TEvQpCacheTestResult>(edge, TDuration::Seconds(30));
        ASSERT_TRUE(ev);
        ASSERT_TRUE(ev->Get()->Success) << ev->Get()->Error;
        emptyResults += ev->Get()->EmptyResults;
        readyNotifications += ev->Get()->ReadyNotifications;
        for (auto& qp : ev->Get()->Qps) {
            qps.push_back(std::move(qp));
        }
    }

    ASSERT_EQ(qps.size(), ClientCount * QpsPerClient);
    EXPECT_GT(emptyResults, 0u);
    EXPECT_GT(readyNotifications, 0u);

    THashSet<const TQueuePair*> uniqueQps;
    for (const auto& qp : qps) {
        uniqueQps.insert(qp.get());
    }
    EXPECT_EQ(uniqueQps.size(), qps.size());
}

// A QP must not keep its ICq wrapper alive after a terminal error. It keeps only
// the shared native verbs resources alive, so the wrapper is sent for destruction
// before a still-referenced QP, while the verbs resources remain valid until that
// QP is destroyed.
TEST_F(TQpCacheTest, TerminalErrorDestroysCqWrapperBeforeQp) {
    std::atomic<ui32> eventSequence = 0;
    std::atomic<ui32> destroyCqEventAt = 0;
    std::atomic<ui32> destroyQpEventAt = 0;

    auto env = CreateQpCacheTestEnvironment(1, false);
    const TActorId edge = env.ActorSystem->AllocateEdgeActor();
    env.ActorSystem->Register(new TQpCacheClientActor(env.Cq, edge, 1, false));

    auto ev = env.ActorSystem->GrabEdgeEvent<TEvQpCacheTestResult>(edge, TDuration::Seconds(10));
    ASSERT_TRUE(ev);
    ASSERT_TRUE(ev->Get()->Success) << ev->Get()->Error;
    ASSERT_EQ(ev->Get()->Qps.size(), 1u);
    TQueuePair::TPtr qp = std::move(ev->Get()->Qps.front());

    env.ActorSystem->SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvPrivateDestroyCq::EventType) {
            destroyCqEventAt.store(eventSequence.fetch_add(1, std::memory_order_relaxed) + 1,
                std::memory_order_release);
        } else if (ev->GetTypeRewrite() == TEvPrivateDestroyQp::EventType) {
            destroyQpEventAt.store(eventSequence.fetch_add(1, std::memory_order_relaxed) + 1,
                std::memory_order_release);
        }
        return false;
    });

    env.ActorSystem->Send(new IEventHandle(env.CqActor, edge,
        new TEvPrivateInvalidateCq(env.Ctx, env.Cq, EIO)), 0);

    // A newly returned handle proves that the invalidation was processed and
    // the old wrapper was removed from TCqActor's map.
    ICq::TPtr newCq = GetCqHandle(env.ActorSystem.Get(), env.Ctx, env.CqActor);
    ASSERT_TRUE(newCq);
    ASSERT_NE(newCq, env.Cq);

    env.Cq.reset();
    ASSERT_TRUE(WaitFor([&] {
        return destroyCqEventAt.load(std::memory_order_acquire) != 0;
    })) << "QP keeps the terminal CQ wrapper alive, destroy event was not sent";
    EXPECT_EQ(destroyQpEventAt.load(std::memory_order_acquire), 0u);

    qp.reset();

    ASSERT_TRUE(WaitFor([&] {
        return destroyQpEventAt.load(std::memory_order_acquire) != 0;
    })) << "QP destroy event was not sent";
    EXPECT_LT(destroyCqEventAt.load(std::memory_order_acquire),
        destroyQpEventAt.load(std::memory_order_acquire));
}

} // anonymous namespace
