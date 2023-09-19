#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <library/cpp/yt/memory/memory_tag.h>

#include <util/random/random.h>

#include <util/system/compiler.h>

// These tests do not work under MSAN and ASAN.
#if !defined(_msan_enabled_) and !defined(_asan_enabled_) and defined(_linux_) and defined(YT_ALLOC_ENABLED)

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

// Used for fake side effects to disable compiler optimizations.
volatile const void* FakeSideEffectVolatileVariable = nullptr;

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

class TMemoryTagTest
    : public TestWithParam<void(*)()>
{
public:
    TMemoryTagTest() = default;
};

////////////////////////////////////////////////////////////////////////////////

// Allocate vector that results in exactly `size` memory usage considering the 16-byte header.
std::vector<char> MakeAllocation(size_t size)
{
    YT_VERIFY(IsPowerOf2(size));

    auto result = std::vector<char>(size);

    // We make fake side effect to prevent any compiler optimizations here.
    // (Clever compilers like to throw away our unused allocations).
    FakeSideEffectVolatileVariable = result.data();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TestStackingGuards()
{
    TMemoryTagGuard guard1(1);
    EXPECT_EQ(GetMemoryUsageForTag(1), 0u);
    auto allocation1 = MakeAllocation(1 << 5);
    EXPECT_EQ(GetMemoryUsageForTag(1), 1u << 5);
    {
        TMemoryTagGuard guard2(2);
        auto allocation2 = MakeAllocation(1 << 6);
        EXPECT_EQ(GetMemoryUsageForTag(1), 1u << 5);
        EXPECT_EQ(GetMemoryUsageForTag(2), 1u << 6);
    }
    EXPECT_EQ(GetMemoryUsageForTag(1), 1u << 5);
    EXPECT_EQ(GetMemoryUsageForTag(2), 0u);
    {
        TMemoryTagGuard guard2(std::move(guard1));
        auto allocation2 = MakeAllocation(1 << 7);
        EXPECT_EQ(GetMemoryUsageForTag(1), (1u << 5) + (1u << 7));
        EXPECT_EQ(GetMemoryUsageForTag(2), 0u);
    }
    EXPECT_EQ(GetMemoryUsageForTag(1), (1u << 5));
    EXPECT_EQ(GetMemoryUsageForTag(2), 0u);
}

////////////////////////////////////////////////////////////////////////////////

void Action1()
{
    TMemoryTagGuard guard(1);
    Yield();
    auto allocation1 = MakeAllocation(1 << 5);
    EXPECT_EQ(GetMemoryUsageForTag(1), 1u << 5);
    Yield();
    auto allocation2 = MakeAllocation(1 << 7);
    EXPECT_EQ(GetMemoryUsageForTag(1), (1u << 5) + (1u << 7));
    Yield();
    auto allocation3 = MakeAllocation(1 << 9);
    EXPECT_EQ(GetMemoryUsageForTag(1), (1u << 5) + (1u << 7) + (1u << 9));
}

void Action2()
{
    TMemoryTagGuard guard(2);
    Yield();
    auto allocation1 = MakeAllocation(1 << 6);
    EXPECT_EQ(GetMemoryUsageForTag(2), 1u << 6);
    Yield();
    auto allocation2 = MakeAllocation(1 << 8);
    EXPECT_EQ(GetMemoryUsageForTag(2), (1u << 6) + (1u << 8));
    Yield();
    auto allocation3 = MakeAllocation(1 << 10);
    EXPECT_EQ(GetMemoryUsageForTag(2), (1u << 6) + (1u << 8) + (1u << 10));
}

void TestSwitchingFibers()
{
    auto future1 = BIND(&Action1)
        .AsyncVia(GetCurrentInvoker())
        .Run();
    auto future2 = BIND(&Action2)
        .AsyncVia(GetCurrentInvoker())
        .Run();
    WaitFor(AllSucceeded(std::vector<TFuture<void>>{future1, future2}))
        .ThrowOnError();
    EXPECT_EQ(GetMemoryUsageForTag(1), 0u);
    EXPECT_EQ(GetMemoryUsageForTag(2), 0u);
}

////////////////////////////////////////////////////////////////////////////////

class TMiniController
    : public TRefCounted
{
public:
    TMiniController(IInvokerPtr controlInvoker, TMemoryTag memoryTag)
        : MemoryTag_(memoryTag)
        , Invoker_(CreateMemoryTaggingInvoker(CreateSerializedInvoker(std::move(controlInvoker)), MemoryTag_))
    { }

    ssize_t GetMemoryUsage() const
    {
        return GetMemoryUsageForTag(MemoryTag_);
    }

    IInvokerPtr GetControlInvoker() const
    {
        return Invoker_;
    }

    std::vector<std::vector<char>>& Allocations()
    {
        return Allocations_;
    }

private:
    TMemoryTag MemoryTag_;
    IInvokerPtr Invoker_;
    std::vector<std::vector<char>> Allocations_;
};

DEFINE_REFCOUNTED_TYPE(TMiniController)
DECLARE_REFCOUNTED_CLASS(TMiniController)

void Action3(TMiniControllerPtr controller)
{
    controller->Allocations().emplace_back(MakeAllocation(128_MB));
}

void TestMemoryTaggingInvoker()
{
    auto queue = New<TActionQueue>();
    auto controller = New<TMiniController>(queue->GetInvoker(), 1);
    EXPECT_EQ(controller->GetMemoryUsage(), 0);

    WaitFor(BIND(&Action3, controller)
        .AsyncVia(controller->GetControlInvoker())
        .Run())
        .ThrowOnError();
    EXPECT_NEAR(controller->GetMemoryUsage(), 128_MB, 1_MB);

    controller->Allocations().clear();
    controller->Allocations().shrink_to_fit();

    EXPECT_NEAR(GetMemoryUsageForTag(1), 0, 1_MB);
}

void TestControllersInThreadPool()
{
    std::vector<TMiniControllerPtr> controllers;
    constexpr int controllerCount = 1000;
    auto pool = CreateThreadPool(16, "TestPool");
    for (int index = 0; index < controllerCount; ++index) {
        controllers.emplace_back(New<TMiniController>(pool->GetInvoker(), index + 1));
    }
    constexpr int actionCount = 100 * 1000;
    std::vector<TFuture<void>> futures;
    std::vector<int> memoryUsages(controllerCount);
    srand(42);
    for (int index = 0; index < actionCount; ++index) {
        int controllerIndex = rand() % controllerCount;
        auto allocationSize = 1 << (5 + rand() % 10);
        memoryUsages[controllerIndex] += allocationSize;
        const auto& controller = controllers[controllerIndex];
        futures.emplace_back(
            BIND([] (TMiniControllerPtr controller, int allocationSize) {
                controller->Allocations().emplace_back(MakeAllocation(allocationSize));
            }, controller, allocationSize)
                .AsyncVia(controller->GetControlInvoker())
                .Run());
    }
    WaitFor(AllSucceeded(futures))
        .ThrowOnError();
    for (int index = 0; index < controllerCount; ++index) {
        EXPECT_NEAR(memoryUsages[index], controllers[index]->GetMemoryUsage(), 10_KB);
    }
    controllers.clear();
    for (int index = 0; index < controllerCount; ++index) {
        EXPECT_NEAR(GetMemoryUsageForTag(index + 1), 0, 10_KB);
        EXPECT_GE(GetMemoryUsageForTag(index + 1), 0u);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TMemoryTagTest, Test)
{
    // We wrap anything with an outer action queue to make
    // fiber-friendly environment.
    auto outerQueue = New<TActionQueue>();
    WaitFor(BIND(GetParam())
        .AsyncVia(outerQueue->GetInvoker())
        .Run())
        .ThrowOnError();
}

INSTANTIATE_TEST_SUITE_P(MemoryTagTest, TMemoryTagTest, Values(
    &TestStackingGuards,
    &TestSwitchingFibers,
    &TestMemoryTaggingInvoker,
    &TestControllersInThreadPool));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

#endif // !defined(_msan_enabled_)
