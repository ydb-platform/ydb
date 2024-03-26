#include <yt/yt/core/rpc/unittests/lib/common.h>

#include <yt/yt/library/ytprof/heap_profiler.h>

#if defined(_linux_)
#include <tcmalloc/common.h>
#endif

namespace NYT::NRpc {
namespace {

using namespace NTracing;
using namespace NConcurrency;
using namespace NYTProf;

////////////////////////////////////////////////////////////////////////////////

#if !defined(_asan_enabled_) && !defined(_msan_enabled_) && defined(_linux_)

////////////////////////////////////////////////////////////////////////////////

constexpr auto MemoryAllocationTag = "memory_tag";

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
using TRpcTest = TTestBase<TImpl>;
TYPED_TEST_SUITE(TRpcTest, TAllTransports);

TYPED_TEST(TRpcTest, ResponseWithAllocationTags)
{
    auto memoryUsageTracker = this->GetMemoryUsageTracker();
    auto previousLimit = memoryUsageTracker->GetLimit();
    memoryUsageTracker->SetLimit(2_GB);
    static TMemoryTag testMemoryTag = 1 << 20;
    testMemoryTag++;

    EnableMemoryProfilingTags();

    auto initialMemoryUsage = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTag, ToString(testMemoryTag));

    auto actionQueue = New<TActionQueue>();

    using TRspPtr = typename TTestProxy::TRspAllocationCallPtr;
    std::vector<TFuture<TRspPtr>> responses;

    TTestProxy proxy(this->CreateChannel());

    constexpr auto size = 4_MB - 1_KB;
    constexpr auto numberOfLoops = 10;
    for (int i = 0; i < numberOfLoops; ++i) {
        auto context = CreateTraceContextFromCurrent("ResponseWithAllocationTags");
        auto contextGuard = TTraceContextGuard(context);
        context->SetAllocationTag(MemoryAllocationTag, testMemoryTag);

        auto req1 = proxy.AllocationCall();
        req1->set_size(size);

        auto rspFutureNoProp = req1->Invoke()
            .Apply(BIND_NO_PROPAGATE([] (const TRspPtr& res) {
                EXPECT_EQ(TryGetCurrentTraceContext(), nullptr);
                return res;
            }).AsyncVia(actionQueue->GetInvoker()));
        responses.push_back(rspFutureNoProp);

        auto req2 = proxy.AllocationCall();
        req2->set_size(size);

        auto rspFutureProp = req2->Invoke()
            .Apply(BIND([testMemoryTag=testMemoryTag] (const TRspPtr& res) {
                auto localContext = TryGetCurrentTraceContext();
                EXPECT_NE(localContext, nullptr);
                if (localContext) {
                    EXPECT_EQ(localContext->FindAllocationTag<TMemoryTag>(MemoryAllocationTag).value_or(NullMemoryTag), testMemoryTag);
                }
                return res;
            }).AsyncVia(actionQueue->GetInvoker()));
        responses.push_back(rspFutureProp);
    }

    auto memoryUsageBefore = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTag, ToString(testMemoryTag));
    EXPECT_LE(memoryUsageBefore, numberOfLoops * 1536_KB);

    for (const auto& rsp : responses) {
        WaitFor(rsp).ValueOrThrow();
    }

    auto memoryUsageAfter = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTag, ToString(testMemoryTag));
    auto deltaMemoryUsage = memoryUsageAfter - initialMemoryUsage - memoryUsageBefore;
    EXPECT_GE(deltaMemoryUsage, numberOfLoops * size * 6 / 5)
        << "InitialUsage: " << initialMemoryUsage << std::endl
        << "MemoryUsage before waiting: " << memoryUsageBefore << std::endl
        << "MemoryUsage after waiting: " << memoryUsageAfter;

    memoryUsageTracker->SetLimit(previousLimit);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
