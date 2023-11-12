#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/backtrace_introspector/introspect.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NBacktraceIntrospector {
namespace {

using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

TEST(TBacktraceIntrospectorTest, Fibers)
{
    constexpr int HeavyQueueCount = 5;
    std::vector<TActionQueuePtr> heavyQueues;
    const TString HeavyThreadNamePrefix("Heavy:");
    for (int index = 0; index < HeavyQueueCount; ++index) {
        heavyQueues.push_back(New<TActionQueue>(HeavyThreadNamePrefix + ToString(index)));
    }

    constexpr int LightQueueCount = 3;
    std::vector<TActionQueuePtr> lightQueues;
    const TString LightThreadNamePrefix("Light:");
    for (int index = 0; index < LightQueueCount; ++index) {
        lightQueues.push_back(New<TActionQueue>(LightThreadNamePrefix + ToString(index)));
    }

    constexpr int HeavyCallbackCount = 3;
    std::vector<TTraceContextPtr> heavyTraceContexts;
    std::set<TTraceId> expectedHeavyTraceIds;
    for (int index = 0; index < HeavyCallbackCount; ++index) {
        auto traceContext = TTraceContext::NewRoot("Heavy");
        traceContext->SetLoggingTag(Format("HeavyLoggingTag:%v", index));
        heavyTraceContexts.push_back(traceContext);
        InsertOrCrash(expectedHeavyTraceIds, traceContext->GetTraceId());
    }

    std::vector<TFuture<void>> heavyFutures;
    for (int index = 0; index < HeavyCallbackCount; ++index) {
        heavyFutures.push_back(
            BIND([&, index] {
                TTraceContextGuard traceContextGuard(heavyTraceContexts[index]);
                YT_LOG_INFO("Heavy callback started (Index: %v)", index);
                Sleep(TDuration::Seconds(3));
                YT_LOG_INFO("Heavy callback finished (Index: %v)", index);
            })
            .AsyncVia(heavyQueues[index % HeavyQueueCount]->GetInvoker())
            .Run());
    }

    constexpr int LightCallbackCount = 10;
    std::vector<TTraceContextPtr> lightTraceContexts;
    std::set<TTraceId> expectedLightTraceIds;
    for (int index = 0; index < LightCallbackCount; ++index) {
        auto traceContext = TTraceContext::NewRoot("Light");
        traceContext->SetLoggingTag(Format("LightLoggingTag:%v", index));
        lightTraceContexts.push_back(traceContext);
        InsertOrCrash(expectedLightTraceIds, traceContext->GetTraceId());
    }

    std::vector<TFuture<void>> lightFutures;
    for (int index = 0; index < LightCallbackCount; ++index) {
        lightFutures.push_back(
            BIND([&, index] {
                TTraceContextGuard traceContextGuard(lightTraceContexts[index]);
                YT_LOG_INFO("Light callback started (Index: %v)", index);
                TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
                YT_LOG_INFO("Light callback finished (Index: %v)", index);
            })
            .AsyncVia(lightQueues[index % LightQueueCount]->GetInvoker())
            .Run());
    }

    Sleep(TDuration::MilliSeconds(100));

    auto infos = IntrospectFibers();
    Cerr << FormatIntrospectionInfos(infos);

    std::set<TTraceId> actualHeavyTraceIds;
    std::set<TTraceId> actualLightTraceIds;
    for (const auto& info : infos) {
        if (!info.TraceId) {
            continue;
        }
        switch (info.State) {
            case EFiberState::Running:
                EXPECT_TRUE(actualHeavyTraceIds.insert(info.TraceId).second);
                if (expectedHeavyTraceIds.contains(info.TraceId)) {
                    EXPECT_TRUE(info.ThreadName.StartsWith(HeavyThreadNamePrefix));
                }
                break;

            case EFiberState::Waiting:
                EXPECT_TRUE(actualLightTraceIds.insert(info.TraceId).second);
                break;

            default:
                break;
        }
    }

    EXPECT_EQ(expectedLightTraceIds, actualLightTraceIds);
    EXPECT_EQ(expectedHeavyTraceIds, actualHeavyTraceIds);

    for (const auto& future : heavyFutures) {
        future.Get().ThrowOnError();
    }

    for (const auto& future : lightFutures) {
        future.Get().ThrowOnError();
    }

    for (const auto& queue : heavyQueues) {
        queue->Shutdown(/*graceful*/ true);
    }
    for (const auto& queue : lightQueues) {
        queue->Shutdown(/*graceful*/ true);
    }
}

TEST(TBacktraceIntrospectorTest, Threads)
{
    constexpr int QueueCount = 5;
    std::vector<TActionQueuePtr> queues;
    const TString ThreadNamePrefix("Queue:");
    for (int index = 0; index < QueueCount; ++index) {
        queues.push_back(New<TActionQueue>(ThreadNamePrefix + ToString(index)));
    }

    constexpr int CallbackCount = 3;
    std::vector<TTraceContextPtr> traceContexts;
    std::set<TTraceId> expectedTraceIds;
    for (int index = 0; index < CallbackCount; ++index) {
        auto traceContext = TTraceContext::NewRoot("Heavy");
        traceContexts.push_back(traceContext);
        InsertOrCrash(expectedTraceIds, traceContext->GetTraceId());
    }

    std::vector<TFuture<void>> futures;
    for (int index = 0; index < CallbackCount; ++index) {
        futures.push_back(
            BIND([&, index] {
                TTraceContextGuard traceContextGuard(traceContexts[index]);
                YT_LOG_INFO("Callback started (Index: %v)", index);
                Sleep(TDuration::Seconds(3));
                YT_LOG_INFO("Callback finished (Index: %v)", index);
            })
            .AsyncVia(queues[index % QueueCount]->GetInvoker())
            .Run());
    }

    Sleep(TDuration::MilliSeconds(100));

    auto infos = IntrospectThreads();
    Cerr << FormatIntrospectionInfos(infos);

    std::set<TTraceId> actualTraceIds;
    for (const auto& info : infos) {
        if (!info.TraceId) {
            continue;
        }
        EXPECT_TRUE(actualTraceIds.insert(info.TraceId).second);
        if (expectedTraceIds.contains(info.TraceId)) {
            EXPECT_TRUE(info.ThreadName.StartsWith(ThreadNamePrefix));
        }
    }

    EXPECT_EQ(expectedTraceIds, actualTraceIds);

    for (const auto& future : futures) {
        future.Get().ThrowOnError();
    }
    for (const auto& queue : queues) {
        queue->Shutdown(/*graceful*/ true);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBacktraceIntrospector
