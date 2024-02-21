#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/invoker_detail.h>
#include <yt/yt/core/actions/invoker_pool.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <library/cpp/yt/threading/count_down_latch.h>

namespace NYT::NConcurrency {
namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

IPrioritizedInvokerPtr CreatePrioritizedInvokerTest(IInvokerPtr underlyingInvoker)
{
    return CreatePrioritizedInvoker(std::move(underlyingInvoker));
}

////////////////////////////////////////////////////////////////////////////////

IInvokerPoolPtr CreateDummyInvokerPool(IInvokerPtr underlyingInvoker, int invokerCount)
{
    YT_VERIFY(invokerCount > 0);
    std::vector<IInvokerPtr> underlyingInvokers(invokerCount, underlyingInvoker);
    return New<NYT::NDetail::TInvokerPoolWrapper<IInvoker>>(std::move(underlyingInvokers));
}

IInvokerPtr CreateIdenticalInvoker(IInvokerPtr invoker)
{
    return invoker;
}

IInvokerPtr CreateIdenticalInvokerByConstReference(const IInvokerPtr& invoker)
{
    return invoker;
}

////////////////////////////////////////////////////////////////////////////////

class TMockInvoker;
using IMockInvokerPool = TGenericInvokerPool<TMockInvoker>;
using IMockInvokerPoolPtr = TIntrusivePtr<IMockInvokerPool>;

////////////////////////////////////////////////////////////////////////////////

class TMockInvoker
    : public TInvokerWrapper
{
public:
    explicit TMockInvoker(IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , InvocationCount_(0)
    { }

    void Invoke(TClosure callback) override
    {
        ++InvocationCount_;
        if (Bounded_ ) {
            EXPECT_TRUE(Parent_.Lock());
        }
        TInvokerWrapper::Invoke(std::move(callback));
    }

    void Bound(const IMockInvokerPoolPtr& parent)
    {
        Bounded_ = true;
        Parent_ = MakeWeak(parent);
    }

    DEFINE_BYVAL_RO_PROPERTY(int, InvocationCount);

private:
    bool Bounded_ = false;
    TWeakPtr<IMockInvokerPool> Parent_;
};

using TMockInvokerPtr = TIntrusivePtr<TMockInvoker>;

TMockInvokerPtr CreateMockInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TMockInvoker>(std::move(underlyingInvoker));
}

////////////////////////////////////////////////////////////////////////////////

class TTransformInvokerPoolTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr Queue_ = New<TActionQueue>();

    void TearDown() override
    {
        Queue_->Shutdown();
    }

    template <class TInvokerPoolPtr, class TGetCallbackCount>
    static void CallPerInvoker(
        const TInvokerPoolPtr& invokerPool,
        const TGetCallbackCount& getCallbackCount)
    {
        for (int i = 0, size = invokerPool->GetSize(); i < size; ++i) {
            const auto callbackCount = getCallbackCount(i);

            for (int j = 0; j < callbackCount; ++j) {
                invokerPool->GetInvoker(i)->Invoke(BIND([] { }));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTransformInvokerPoolTest, Simple)
{
    static constexpr auto InvokerCount = 5;

    auto inputInvokerPool = CreateDummyInvokerPool(Queue_->GetInvoker(), InvokerCount);
    auto mockInvokerPool = TransformInvokerPool(std::move(inputInvokerPool), CreateMockInvoker);
    auto outputInvokerPool = TransformInvokerPool(mockInvokerPool, CreateSuspendableInvoker);

    EXPECT_EQ(InvokerCount, outputInvokerPool->GetSize());

    const auto getCallbackCount = [] (int invokerIndex) {
        return 2 * (invokerIndex + 1);
    };

    CallPerInvoker(outputInvokerPool, getCallbackCount);

    for (int i = 0; i < InvokerCount; ++i) {
        EXPECT_EQ(getCallbackCount(i), mockInvokerPool->GetInvoker(i)->GetInvocationCount());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTransformInvokerPoolTest, OutputPoolOutlivesInputPool)
{
    // NB! We do not use std::move in that test intentionally.

    static constexpr auto InvokerCount = 10;

    auto inputInvokerPool = CreateDummyInvokerPool(Queue_->GetInvoker(), InvokerCount);
    auto mockInvokerPool = TransformInvokerPool(inputInvokerPool, CreateMockInvoker);

    // Bound invokers and invoker pool so destruction of the pool breaks invokers functionality.
    for (int i = 0; i < InvokerCount; ++i) {
        mockInvokerPool->GetInvoker(i)->Bound(mockInvokerPool);
    }

    auto outputInvokerPool = TransformInvokerPool(mockInvokerPool, CreateIdenticalInvoker);

    // Manually destroy underlying invoker pools.
    inputInvokerPool.Reset();
    mockInvokerPool.Reset();

    // Check that output invoker pool still works correctly.
    CallPerInvoker(
        outputInvokerPool,
        /*getCallbackCount*/ [] (int /*invokerIndex*/) {
            return 1;
        });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTransformInvokerPoolTest, InstantiateForWellKnownTypes)
{
    const auto testOne = [this] (auto invokerFunctor) {
        auto inputInvokerPool = CreateDummyInvokerPool(Queue_->GetInvoker(), /*invokerCount*/ 10);
        return TransformInvokerPool(std::move(inputInvokerPool), invokerFunctor);
    };

    IInvokerPoolPtr x = testOne(CreateIdenticalInvoker);
    ISuspendableInvokerPoolPtr y = testOne(CreateSuspendableInvoker);
    IPrioritizedInvokerPoolPtr z = testOne(CreatePrioritizedInvokerTest);
    IInvokerPoolPtr w = testOne(CreateIdenticalInvokerByConstReference);

    const auto testPair = [this] (auto firstInvokerFunctor, auto secondInvokerFunctor) {
        auto inputInvokerPool = CreateDummyInvokerPool(Queue_->GetInvoker(), /*invokerCount*/ 10);
        auto intermediateInvokerPool = TransformInvokerPool(std::move(inputInvokerPool), firstInvokerFunctor);
        auto outputInvokerPool = TransformInvokerPool(std::move(intermediateInvokerPool), secondInvokerFunctor);
    };

    testPair(CreateIdenticalInvoker, CreateIdenticalInvoker);
    testPair(CreateIdenticalInvoker, CreateSuspendableInvoker);
    testPair(CreateIdenticalInvoker, CreatePrioritizedInvokerTest);

    testPair(CreateSuspendableInvoker, CreateIdenticalInvoker);
    testPair(CreateSuspendableInvoker, CreateSuspendableInvoker);
    testPair(CreateSuspendableInvoker, CreatePrioritizedInvokerTest);

    testPair(CreatePrioritizedInvokerTest, CreateIdenticalInvoker);
    testPair(CreatePrioritizedInvokerTest, CreateSuspendableInvoker);
    testPair(CreatePrioritizedInvokerTest, CreatePrioritizedInvokerTest);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTransformInvokerPoolTest, Chaining)
{
    static constexpr auto InvokerCount = 10;

    // NB! Intentionally do not use 'auto' here.
    IMockInvokerPoolPtr invokerPool = TransformInvokerPool(TransformInvokerPool(TransformInvokerPool(
        CreateDummyInvokerPool(Queue_->GetInvoker(), InvokerCount),
        CreateSuspendableInvoker),
        CreatePrioritizedInvokerTest),
        CreateMockInvoker);

    const auto getCallbackCount = [] (int invokerIndex) {
        return 2 * (invokerIndex + 1);
    };

    CallPerInvoker(invokerPool, getCallbackCount);

    for (int i = 0; i < InvokerCount; ++i) {
        EXPECT_EQ(getCallbackCount(i), invokerPool->GetInvoker(i)->GetInvocationCount());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTransformInvokerPoolTest, ReturnTypeConvertability)
{
    auto invokerPool = CreateDummyInvokerPool(Queue_->GetInvoker(), /*invokerCount*/ 10);
    auto suspendableInvokerPool = TransformInvokerPool(invokerPool, CreateSuspendableInvoker);
    auto prioritizedInvokerPool = TransformInvokerPool(suspendableInvokerPool, CreatePrioritizedInvokerTest);

    EXPECT_TRUE((std::is_convertible<decltype(invokerPool), IInvokerPoolPtr>::value));
    EXPECT_FALSE((std::is_convertible<decltype(invokerPool), ISuspendableInvokerPoolPtr>::value));
    EXPECT_FALSE((std::is_convertible<decltype(invokerPool), IPrioritizedInvokerPoolPtr>::value));

    EXPECT_FALSE((std::is_convertible<decltype(suspendableInvokerPool), IInvokerPoolPtr>::value));
    EXPECT_TRUE((std::is_convertible<decltype(suspendableInvokerPool), ISuspendableInvokerPoolPtr>::value));
    EXPECT_FALSE((std::is_convertible<decltype(suspendableInvokerPool), IPrioritizedInvokerPoolPtr>::value));

    EXPECT_FALSE((std::is_convertible<decltype(prioritizedInvokerPool), IInvokerPoolPtr>::value));
    EXPECT_FALSE((std::is_convertible<decltype(prioritizedInvokerPool), ISuspendableInvokerPoolPtr>::value));
    EXPECT_TRUE((std::is_convertible<decltype(prioritizedInvokerPool), IPrioritizedInvokerPoolPtr>::value));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EInvokerIndex,
    (ZeroInvoker)
    (FirstInvoker)
    (SecondInvoker)
);

TEST(TInvokerPoolTest, IndexByEnum)
{
    auto queue = New<TActionQueue>();

    auto invokerPool = CreateDummyInvokerPool(queue->GetInvoker(), /*invokerCount*/ 3);

    EXPECT_EQ(invokerPool->GetInvoker(0).Get(), invokerPool->GetInvoker(EInvokerIndex::ZeroInvoker).Get());
    EXPECT_EQ(invokerPool->GetInvoker(1).Get(), invokerPool->GetInvoker(EInvokerIndex::FirstInvoker).Get());
    EXPECT_EQ(invokerPool->GetInvoker(2).Get(), invokerPool->GetInvoker(EInvokerIndex::SecondInvoker).Get());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPrioritizedInvokerTest, SamePriorityOrder)
{
    auto actionQueue = New<TActionQueue>();
    auto prioritizedInvoker = CreatePrioritizedInvoker(actionQueue->GetInvoker());

    int actualOrder = 0;
    TCountDownLatch latch(100);

    for (int index = 0; index < 100; ++index) {
        prioritizedInvoker->Invoke(BIND([&, expected = index] {
            EXPECT_EQ(expected, actualOrder++);
            Sleep(TDuration::MilliSeconds(1));
            latch.CountDown();
        }), 0);
    }

    latch.Wait();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
