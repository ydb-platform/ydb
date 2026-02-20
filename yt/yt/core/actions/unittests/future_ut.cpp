#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/context_switch.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/ytree/attributes.h>

#include <util/system/thread.h>

#include <thread>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto SleepQuantum = TDuration::MilliSeconds(50);

class TFutureTest
    : public ::testing::Test
{ };

struct TNonAssignable
{
    const int Value = 0;
};

TEST_F(TFutureTest, UniqueVoidOK)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    auto uniqueFuture = future.AsUnique();
    EXPECT_FALSE(uniqueFuture.IsSet());
    promise.Set();
    EXPECT_TRUE(uniqueFuture.IsSet());
    EXPECT_TRUE(uniqueFuture.Get().IsOK());
}

TEST_F(TFutureTest, UniqueVoidError)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    auto uniqueFuture = future.AsUnique();
    EXPECT_FALSE(uniqueFuture.IsSet());
    promise.Set(TError("oops"));
    EXPECT_TRUE(uniqueFuture.IsSet());
    EXPECT_EQ(uniqueFuture.Get().GetCode(), NYT::EErrorCode::Generic);
}

TEST_F(TFutureTest, VoidUniqueApply)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    // Try various chaining scenarios.
    auto chainedFuture = future
        .AsUnique()
        .Apply(BIND([] (TError&& error) {
            EXPECT_TRUE(error.IsOK());
        }))
        .AsUnique()
        .Apply(BIND([] {
        }))
        .AsUnique()
        .Apply(BIND([] {
            return 123;
        }))
        .AsVoid()
        .AsUnique()
        .Apply(BIND([] {
            return MakeFuture<int>(456);
        }))
        .AsVoid()
        .AsUnique()
        .Apply(BIND([] {
            return MakeFuture<int>(888).AsUnique();
        }));
    EXPECT_FALSE(chainedFuture.IsSet());
    promise.Set();
    EXPECT_TRUE(chainedFuture.IsSet());
    EXPECT_EQ(chainedFuture.Get().ValueOrThrow(), 888);
}

TEST_F(TFutureTest, WellKnownUniqueFuture)
{
    auto future = OKFuture.AsUnique();
    // Multiple subscriptions are fine.
    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(future.Get().IsOK());
    EXPECT_TRUE(future.Get().IsOK());
    {
        bool invoked = false;
        future.Subscribe(BIND([&] (const TError& error) {
            EXPECT_TRUE(error.IsOK());
            invoked = true;
        }));
        EXPECT_TRUE(invoked);
    }
    {
        bool invoked = false;
        future.Subscribe(BIND([&] (TError&& error) {
            EXPECT_TRUE(error.IsOK());
            invoked = true;
        }));
        EXPECT_TRUE(invoked);
    }
}

TEST_F(TFutureTest, NoncopyableGet)
{
    auto f = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(1));
    EXPECT_TRUE(f.IsSet());
    auto result = f.AsUnique().Get();
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(1, *result.Value());
}

TEST_F(TFutureTest, NoncopyableApply1)
{
    auto f = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(1));
    auto g = f.AsUnique().Apply(BIND([] (TErrorOr<std::unique_ptr<int>>&& ptrOrError) {
        EXPECT_TRUE(ptrOrError.IsOK());
        EXPECT_EQ(1, *ptrOrError.Value());
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
}

TEST_F(TFutureTest, NoncopyableApply2)
{
    auto f = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(1));
    auto g = f.AsUnique().Apply(BIND([] (TErrorOr<std::unique_ptr<int>>&& ptrOrError) {
        EXPECT_TRUE(ptrOrError.IsOK());
        EXPECT_EQ(1, *ptrOrError.Value());
        return TErrorOr<int>(2);
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
    EXPECT_EQ(2, g.Get().Value());
}

TEST_F(TFutureTest, NoncopyableApply3)
{
    auto f = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(1));
    auto g = f.AsUnique().Apply(BIND([] (TErrorOr<std::unique_ptr<int>>&& ptrOrError) {
        EXPECT_TRUE(ptrOrError.IsOK());
        EXPECT_EQ(1, *ptrOrError.Value());
        return MakeFuture(2);
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
    EXPECT_EQ(2, g.Get().Value());
}

TEST_F(TFutureTest, NoncopyableApply4)
{
    auto f = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(1));
    auto g = f.AsUnique().Apply(BIND([] (std::unique_ptr<int>&& ptr) {
        EXPECT_EQ(1, *ptr);
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
}

TEST_F(TFutureTest, NoncopyableApply5)
{
    auto f = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(1));
    auto g = f.AsUnique().Apply(BIND([] (std::unique_ptr<int>&& ptr) {
        EXPECT_EQ(1, *ptr);
        return MakeFuture(2);
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
    EXPECT_EQ(2, g.Get().Value());
}

TEST_F(TFutureTest, NoncopyableApply6)
{
    auto f = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(1));
    auto g = f.AsUnique().Apply(BIND([] (TErrorOr<std::unique_ptr<int>>&& ptrOrError) {
        EXPECT_TRUE(ptrOrError.IsOK());
        EXPECT_EQ(1, *ptrOrError.Value());
        return MakeFuture<std::unique_ptr<double>>(nullptr).AsUnique();
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
    EXPECT_EQ(nullptr, g.Get().Value());
}

TEST_F(TFutureTest, NoncopyableApply7)
{
    auto f = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(1));
    auto g = f.AsUnique().Apply(BIND([] (std::unique_ptr<int>&& ptr) {
        EXPECT_EQ(1, *ptr);
        return MakeFuture<std::unique_ptr<double>>(nullptr).AsUnique();
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
    EXPECT_EQ(nullptr, g.Get().Value());
}

TEST_F(TFutureTest, NoncopyableApply8)
{
    auto f = OKFuture;
    auto g = f.Apply(BIND([] {
        return MakeFuture<std::unique_ptr<double>>(nullptr).AsUnique();
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
    EXPECT_EQ(nullptr, g.Get().Value());
}

TEST_F(TFutureTest, NoncopyableApply9)
{
    auto f = MakeFuture(1);
    auto g = f.Apply(BIND([] (const int& x) {
        EXPECT_EQ(1, x);
        return MakeFuture<std::unique_ptr<int>>(nullptr).AsUnique();
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
    EXPECT_EQ(nullptr, g.Get().Value());
}

TEST_F(TFutureTest, NoncopyableApply10)
{
    auto f = MakeFuture(1);
    auto g = f.Apply(BIND([] (int x) {
        EXPECT_EQ(1, x);
        return MakeFuture<std::unique_ptr<int>>(nullptr).AsUnique();
    }));
    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
    EXPECT_EQ(nullptr, g.Get().Value());
}

TEST_F(TFutureTest, NoncopyableApplySO5086)
{
    auto result = MakeFuture(std::string("hello"))
        .AsUnique()
        .Apply(BIND([] (std::string&& str) {
            EXPECT_EQ("hello", str);
            return MakeFuture(std::make_unique<int>(42)).AsUnique();
        }))
        .AsUnique()
        .Get();
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(42, *result.Value());
}

TEST_F(TFutureTest, NonAssignable1)
{
    auto f = MakeFuture<TNonAssignable>({
        .Value = 1,
    });

    auto g = f.AsUnique().Apply(BIND([] (TNonAssignable&& object) {
        EXPECT_EQ(1, object.Value);
    }));

    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
}

TEST_F(TFutureTest, NonAssignable2)
{
    auto f = MakeFuture<TNonAssignable>({
        .Value = 1,
    });

    std::vector<decltype(f)> futures;

    futures.push_back(f);
    futures.push_back(f);

    auto g = AllSet(futures).AsUnique().Apply(BIND([] (std::vector<TErrorOr<TNonAssignable>>&& objects) {
        EXPECT_TRUE(objects.at(0).IsOK());
        EXPECT_TRUE(objects.at(1).IsOK());
        EXPECT_EQ(1, objects[0].Value().Value);
    }));

    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
}

TEST_F(TFutureTest, NonAssignable3)
{
    auto f = MakeFuture<TNonAssignable>({
        .Value = 1,
    });

    std::vector<decltype(f)> futures;

    futures.push_back(f);
    futures.push_back(f);

    auto g = AllSucceeded(futures).AsUnique().Apply(BIND([] (std::vector<TNonAssignable>&& objects) {
        EXPECT_EQ(1, objects[0].Value);
    }));

    EXPECT_TRUE(g.IsSet());
    EXPECT_TRUE(g.Get().IsOK());
}

TEST_F(TFutureTest, Unsubscribe)
{
    auto p = NewPromise<int>();
    auto f = p.ToFuture();

    bool f1 = false;
    auto c1 = f.AsVoid().Subscribe(BIND([&] (const TError&) { f1 = true; }));

    bool f2 = false;
    auto c2 = f.Subscribe(BIND([&] (const TErrorOr<int>&) { f2 = true; }));

    EXPECT_NE(c1, c2);

    f.Unsubscribe(c1);
    f.Unsubscribe(c2);

    bool f3 = false;
    auto c3 = f.AsVoid().Subscribe(BIND([&] (const TError&) { f3 = true; }));
    EXPECT_EQ(c1, c3);

    bool f4 = false;
    auto c4 = f.Subscribe(BIND([&] (const TErrorOr<int>&) { f4 = true; }));
    EXPECT_EQ(c2, c4);

    p.Set(1);

    EXPECT_FALSE(f1);
    EXPECT_FALSE(f2);
    EXPECT_TRUE(f3);
    EXPECT_TRUE(f4);

    bool f5 = false;
    EXPECT_EQ(NullFutureCallbackCookie, f.Subscribe(BIND([&] (const TError&) { f5 = true; })));
    EXPECT_TRUE(f5);

    bool f6 = false;
    EXPECT_EQ(NullFutureCallbackCookie, f.Subscribe(BIND([&] (const TErrorOr<int>&) { f6 = true; })));
    EXPECT_TRUE(f6);
}

TEST_F(TFutureTest, IsNull)
{
    TFuture<int> empty;
    TFuture<int> nonEmpty = MakeFuture(42);

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);

    empty = std::move(nonEmpty);

    EXPECT_TRUE(empty);
    EXPECT_FALSE(nonEmpty);

    swap(empty, nonEmpty);

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);
}

TEST_F(TFutureTest, IsNullVoid)
{
    TFuture<void> empty;
    TFuture<void> nonEmpty = OKFuture;

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);

    empty = std::move(nonEmpty);

    EXPECT_TRUE(empty);
    EXPECT_FALSE(nonEmpty);

    swap(empty, nonEmpty);

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);
}

TEST_F(TFutureTest, Reset)
{
    auto foo = MakeFuture(42);

    EXPECT_TRUE(foo);
    foo.Reset();
    EXPECT_FALSE(foo);
}

TEST_F(TFutureTest, IsSet)
{
    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    EXPECT_FALSE(future.IsSet());
    EXPECT_FALSE(promise.IsSet());
    promise.Set(42);
    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(promise.IsSet());
}

TEST_F(TFutureTest, SetAndGet)
{
    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    promise.Set(57);
    EXPECT_EQ(57, future.Get().Value());
    EXPECT_EQ(57, future.Get().Value()); // Second Get() should also work.
}

TEST_F(TFutureTest, SetAndTryGet)
{
    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    {
        auto result = future.TryGet();
        EXPECT_FALSE(result);
    }

    promise.Set(42);

    {
        auto result = future.TryGet();
        EXPECT_TRUE(result);
        EXPECT_EQ(42, result->Value());
    }
}

class TMock
{
public:
    MOCK_METHOD(void, Tacke, (int), ());
};

TEST_F(TFutureTest, Subscribe)
{
    TMock firstMock;
    TMock secondMock;

    EXPECT_CALL(firstMock, Tacke(42)).Times(1);
    EXPECT_CALL(secondMock, Tacke(42)).Times(1);

    auto firstSubscriber = BIND([&] (const TErrorOr<int>& x) { firstMock.Tacke(x.Value()); });
    auto secondSubscriber = BIND([&] (const TErrorOr<int>& x) { secondMock.Tacke(x.Value()); });

    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    future.Subscribe(firstSubscriber);
    promise.Set(42);
    future.Subscribe(secondSubscriber);
}

TEST_F(TFutureTest, GetUnique)
{
    auto promise = NewPromise<std::vector<int>>();
    auto future = promise.ToFuture();

    EXPECT_FALSE(future.IsSet());

    std::vector v{1, 2, 3};
    promise.Set(v);

    EXPECT_TRUE(future.IsSet());
    auto w = future.AsUnique().Get();
    EXPECT_TRUE(w.IsOK());
    EXPECT_EQ(v, w.Value());
    EXPECT_TRUE(future.IsSet());
}

TEST_F(TFutureTest, TryGetUnique)
{
    auto promise = NewPromise<std::vector<int>>();
    auto future = promise.ToFuture();

    EXPECT_FALSE(future.IsSet());
    EXPECT_FALSE(future.AsUnique().TryGet());

    std::vector v{1, 2, 3};
    promise.Set(v);

    EXPECT_TRUE(future.IsSet());
    auto w = future.AsUnique().TryGet();
    EXPECT_TRUE(w);
    EXPECT_TRUE(w->IsOK());
    EXPECT_EQ(v, w->Value());
    EXPECT_TRUE(future.IsSet());
}

TEST_F(TFutureTest, SubscribeUniqueBeforeSet)
{
    std::vector v{1, 2, 3};

    auto promise = NewPromise<std::vector<int>>();
    auto future = promise.ToFuture();

    std::vector<int> vv;
    future.AsUnique().Subscribe(BIND([&] (TErrorOr<std::vector<int>>&& arg) {
        EXPECT_TRUE(arg.IsOK());
        vv = std::move(arg.Value());
    }));

    EXPECT_FALSE(future.IsSet());
    promise.Set(v);
    EXPECT_TRUE(future.IsSet());
    EXPECT_EQ(v, vv);
}

TEST_F(TFutureTest, SubscribeUniqueAfterSet)
{
    std::vector v{1, 2, 3};

    auto promise = NewPromise<std::vector<int>>();
    auto future = promise.ToFuture();

    EXPECT_FALSE(future.IsSet());
    promise.Set(v);
    EXPECT_TRUE(future.IsSet());

    std::vector<int> vv;
    future.AsUnique().Subscribe(BIND([&] (TErrorOr<std::vector<int>>&& arg) {
        EXPECT_TRUE(arg.IsOK());
        vv = std::move(arg.Value());
    }));

    EXPECT_EQ(v, vv);
    EXPECT_TRUE(future.IsSet());
}

static void* AsynchronousIntSetter(void* param)
{
    Sleep(SleepQuantum);

    auto* promise = reinterpret_cast<TPromise<int>*>(param);
    promise->Set(42);

    return nullptr;
}

static void* AsynchronousVoidSetter(void* param)
{
    Sleep(SleepQuantum);

    auto* promise = reinterpret_cast<TPromise<void>*>(param);
    promise->Set();

    return nullptr;
}

TEST_F(TFutureTest, SubscribeWithAsynchronousSet)
{
    TMock firstMock;
    TMock secondMock;

    EXPECT_CALL(firstMock, Tacke(42)).Times(1);
    EXPECT_CALL(secondMock, Tacke(42)).Times(1);

    auto firstSubscriber = BIND([&] (const TErrorOr<int>& x) { firstMock.Tacke(x.Value()); });
    auto secondSubscriber = BIND([&] (const TErrorOr<int>& x) { secondMock.Tacke(x.Value()); });

    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    future.Subscribe(firstSubscriber);

    ::TThread thread(&AsynchronousIntSetter, &promise);
    thread.Start();
    thread.Join();

    future.Subscribe(secondSubscriber);
}

TEST_F(TFutureTest, CascadedApply)
{
    auto kicker = NewPromise<bool>();

    auto left   = NewPromise<int>();
    auto right  = NewPromise<int>();

    ::TThread thread(&AsynchronousIntSetter, &left);

    auto leftPrime =
        kicker.ToFuture()
        .Apply(BIND([=, &thread] (bool /*f*/) -> TFuture<int> {
            thread.Start();
            return left.ToFuture();
        }))
        .Apply(BIND([=] (int xv) -> int {
            return xv + 8;
        }));
    auto rightPrime =
        right.ToFuture()
        .Apply(BIND([=] (int xv) -> TFuture<int> {
            return MakeFuture(xv + 4);
        }));

    int accumulator = 0;
    auto accumulate = BIND([&] (const TErrorOr<int>& x) { accumulator += x.Value(); });

    leftPrime.Subscribe(accumulate);
    rightPrime.Subscribe(accumulate);

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

    // Initial computation condition.
    EXPECT_FALSE(left.IsSet());  EXPECT_FALSE(leftPrime.IsSet());
    EXPECT_FALSE(right.IsSet()); EXPECT_FALSE(rightPrime.IsSet());
    EXPECT_EQ(0, accumulator);

    // Kick off!
    kicker.Set(true);
    EXPECT_FALSE(left.IsSet());  EXPECT_FALSE(leftPrime.IsSet());
    EXPECT_FALSE(right.IsSet()); EXPECT_FALSE(rightPrime.IsSet());
    EXPECT_EQ(0, accumulator);

    // Kick off!
    right.Set(1);

    EXPECT_FALSE(left.IsSet());  EXPECT_FALSE(leftPrime.IsSet());
    EXPECT_TRUE(right.IsSet());  EXPECT_TRUE(rightPrime.IsSet());
    EXPECT_EQ( 5, accumulator);
    EXPECT_EQ( 1, right.Get().Value());
    EXPECT_EQ( 5, rightPrime.Get().Value());

    // This will sleep for a while until left branch will be evaluated.
    thread.Join();

    EXPECT_TRUE(left.IsSet());   EXPECT_TRUE(leftPrime.IsSet());
    EXPECT_TRUE(right.IsSet());  EXPECT_TRUE(rightPrime.IsSet());
    EXPECT_EQ(55, accumulator);
    EXPECT_EQ(42, left.Get().Value());
    EXPECT_EQ(50, leftPrime.Get().Value());
}

TEST_F(TFutureTest, ApplyVoidToVoid)
{
    int state = 0;

    auto kicker = NewPromise<void>();

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] () -> void { ++state; }));

    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    kicker.Set();

    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());
}

TEST_F(TFutureTest, ApplyVoidToFutureVoid)
{
    int state = 0;

    auto kicker = NewPromise<void>();
    auto setter = NewPromise<void>();

    ::TThread thread(&AsynchronousVoidSetter, &setter);

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] () -> TFuture<void> {
            ++state;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set();

    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // This will sleep for a while until evaluation completion.
    thread.Join();

    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());
}

TEST_F(TFutureTest, ApplyVoidToInt)
{
    int state = 0;

    auto kicker = NewPromise<void>();

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] () -> int {
            ++state;
            return 17;
        }));

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set();

    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(17, target.Get().Value());
}

TEST_F(TFutureTest, ApplyVoidToFutureInt)
{
    int state = 0;

    auto kicker = NewPromise<void>();
    auto setter = NewPromise<int>();

    ::TThread thread(&AsynchronousIntSetter, &setter);

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] () -> TFuture<int> {
            ++state;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set();

    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // This will sleep for a while until evaluation completion.
    thread.Join();

    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(42, target.Get().Value());
}

TEST_F(TFutureTest, ApplyIntToVoid)
{
    int state = 0;

    auto kicker = NewPromise<int>();

    auto  source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] (int x) -> void { state += x; }));

    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    kicker.Set(21);

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(21, source.Get().Value());
}

TEST_F(TFutureTest, ApplyIntToFutureVoid)
{
    int state = 0;

    auto kicker = NewPromise<int>();
    auto setter = NewPromise<void>();

    ::TThread thread(&AsynchronousVoidSetter, &setter);

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] (int x) -> TFuture<void> {
            state += x;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set(21);

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    EXPECT_EQ(21, source.Get().Value());

    // This will sleep for a while until evaluation completion.
    thread.Join();

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());
}

TEST_F(TFutureTest, ApplyIntToInt)
{
    int state = 0;

    auto kicker = NewPromise<int>();

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] (int x) -> int {
            state += x;
            return x * 2;
        }));

    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    kicker.Set(21);

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(21, source.Get().Value());
    EXPECT_EQ(42, target.Get().Value());
}

TEST_F(TFutureTest, ApplyIntToFutureInt)
{
    int state = 0;

    auto kicker = NewPromise<int>();
    auto setter = NewPromise<int>();

    ::TThread thread(&AsynchronousIntSetter, &setter);

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] (int x) -> TFuture<int> {
            state += x;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set(21);

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    EXPECT_EQ(21, source.Get().Value());

    // This will sleep for a while until evaluation completion.
    thread.Join();

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(21, source.Get().Value());
    EXPECT_EQ(42, target.Get().Value());
}

TEST_F(TFutureTest, TestCancelDelayed)
{
    auto future = NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::Seconds(10));
    future.Cancel(TError("Canceled"));
    EXPECT_TRUE(future.IsSet());
    EXPECT_FALSE(future.Get().IsOK());
}

TEST_F(TFutureTest, CancelDoesntSpuriouslyFail)
{
    constexpr auto timeLimit = TDuration::Seconds(10);
    const auto t0 = TInstant::Now();

    std::atomic<i64> counter = 0;

    auto wait = [&counter] (i64 expected) -> i64 {
        while (true) {
            auto value = counter.load();
            if (value == expected || value == -1) {
                return value;
            }
        }
    };

    TPromise<void>* promisePtr = nullptr;

    auto setter = [&] () {
        i64 i = 0;
        while (true) {
            ++i;
            if (wait(i) == -1) {
                return;
            }

            promisePtr->Set();

            ++i;
            counter.fetch_add(1);
        }
    };

    ::TThread thread(setter);
    thread.Start();

    i64 i = 0;
    while (TInstant::Now() - t0 < timeLimit) {
        auto promise = NewPromise<void>();
        auto future = promise.ToFuture().AsCancelable();
        promisePtr = &promise;

        ++i;
        counter.fetch_add(1);

        bool cancelSuccess = future.Cancel(TError());

        EXPECT_EQ(cancelSuccess, promise.IsCanceled());

        ++i;
        wait(i);

        promisePtr = nullptr;
    }

    counter.store(-1);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFutureTest, AnyCombiner)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnySucceeded(futures);
    EXPECT_FALSE(f.IsSet());
    p2.Set(2);
    EXPECT_TRUE(f.IsSet());
    auto resultOrError = f.Get();
    EXPECT_TRUE(resultOrError.IsOK());
    auto result = resultOrError.Value();
    EXPECT_EQ(2, result);
}

TEST_F(TFutureTest, AnyCombinerRetainError)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnySet(futures);
    EXPECT_FALSE(f.IsSet());
    p2.Set(TError("oops"));
    EXPECT_TRUE(f.IsSet());
    auto resultOrError = f.Get();
    EXPECT_FALSE(resultOrError.IsOK());
}

TEST_F(TFutureTest, AnyCombinerEmpty)
{
    std::vector<TFuture<int>> futures;
    auto error = AnySucceeded(futures).Get();
    EXPECT_EQ(NYT::EErrorCode::FutureCombinerFailure, error.GetCode());
}

TEST_F(TFutureTest, AnyCombinerSkipError)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    auto p3 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AnySucceeded(futures);
    EXPECT_FALSE(f.IsSet());
    EXPECT_FALSE(p2.IsCanceled());
    p1.Set(TError("oops"));
    EXPECT_FALSE(f.IsSet());
    p2.Set(123);
    EXPECT_TRUE(f.IsSet());
    auto result = f.Get();
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(123, result.Value());
    EXPECT_TRUE(p3.IsCanceled());
}

TEST_F(TFutureTest, AnyCombinerSuccessShortcut)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnySucceeded(futures);
    EXPECT_FALSE(f.IsSet());
    EXPECT_FALSE(p2.IsCanceled());
    p1.Set(1);
    EXPECT_TRUE(f.IsSet());
    auto result = f.Get();
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(1, result.Value());
    EXPECT_TRUE(p2.IsCanceled());
}

TEST_F(TFutureTest, AnyCombinerDontCancelOnShortcut)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnySucceeded(
        futures,
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
    p1.Set(1);
    EXPECT_FALSE(p2.IsCanceled());
}

TEST_F(TFutureTest, AnyCombinerPropagateCancelation)
{
    auto p1 = NewPromise<void>();
    auto p2 = NewPromise<void>();
    std::vector<TFuture<void>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnySucceeded(futures);
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
    f.Cancel(TError("oops"));
    EXPECT_TRUE(p1.IsCanceled());
    EXPECT_TRUE(p2.IsCanceled());
}

TEST_F(TFutureTest, AnyCombinerDontPropagateCancelation)
{
    auto p1 = NewPromise<void>();
    auto p2 = NewPromise<void>();
    std::vector<TFuture<void>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnySucceeded(
        futures,
        TFutureCombinerOptions{.PropagateCancelationToInput = false});
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
    f.Cancel(TError("oops"));
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
}

TEST_F(TFutureTest, AnyCombiner1)
{
    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();
    std::vector<TFuture<int>> futures{
        future
    };
    EXPECT_EQ(future, AnySucceeded(futures));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFutureTest, AllCombinerEmpty)
{
    std::vector<TFuture<int>> futures{};
    auto resultOrError = AllSucceeded(futures).Get();
    EXPECT_TRUE(resultOrError.IsOK());
    const auto& result = resultOrError.Value();
    EXPECT_TRUE(result.empty());
}

TEST_F(TFutureTest, AllCombiner)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AllSucceeded(futures);
    EXPECT_FALSE(f.IsSet());
    p1.Set(2);
    EXPECT_FALSE(f.IsSet());
    p2.Set(10);
    EXPECT_TRUE(f.IsSet());
    auto resultOrError = f.Get();
    EXPECT_TRUE(resultOrError.IsOK());
    const auto& result = resultOrError.Value();
    EXPECT_EQ(2, std::ssize(result));
    EXPECT_EQ(2, result[0]);
    EXPECT_EQ(10, result[1]);
}

TEST_F(TFutureTest, AllCombinerError)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AllSucceeded(futures);
    EXPECT_FALSE(f.IsSet());
    p1.Set(2);
    EXPECT_FALSE(f.IsSet());
    p2.Set(TError("oops"));
    EXPECT_TRUE(f.IsSet());
    auto resultOrError = f.Get();
    EXPECT_FALSE(resultOrError.IsOK());
}

TEST_F(TFutureTest, AllCombinerFailureShortcut)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AllSucceeded(futures);
    EXPECT_FALSE(f.IsSet());
    EXPECT_FALSE(p2.IsCanceled());
    p1.Set(TError("oops"));
    EXPECT_TRUE(f.IsSet());
    auto result = f.Get();
    EXPECT_FALSE(result.IsOK());
    EXPECT_TRUE(p2.IsCanceled());
}

TEST_F(TFutureTest, AllCombinerDontCancelOnShortcut)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AllSucceeded(
        futures,
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
    p1.Set(TError("oops"));
    EXPECT_FALSE(p2.IsCanceled());
}

TEST_F(TFutureTest, AllCombinerCancel)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    auto p3 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AllSucceeded(futures);
    EXPECT_FALSE(f.IsSet());
    f.Cancel(TError("oops"));
    EXPECT_TRUE(f.IsSet());
    const auto& result = f.Get();
    EXPECT_EQ(NYT::EErrorCode::Canceled, result.GetCode());
}

TEST_F(TFutureTest, AllCombinerVoid0)
{
    std::vector<TFuture<void>> futures;
    EXPECT_EQ(OKFuture, AllSucceeded(futures));
}

TEST_F(TFutureTest, AllCombinerVoid1)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    std::vector<TFuture<void>> futures{
        future
    };
    EXPECT_EQ(future, AllSucceeded(futures));
}

TEST_F(TFutureTest, AllCombinerRetainError)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
    };
    auto f = AllSet(futures);
    EXPECT_FALSE(f.IsSet());
    p1.Set(2);
    EXPECT_FALSE(f.IsSet());
    p2.Set(TError("oops"));
    EXPECT_TRUE(f.IsSet());
    auto resultOrError = f.Get();
    EXPECT_TRUE(resultOrError.IsOK());
    const auto& result = resultOrError.Value();
    EXPECT_EQ(2, std::ssize(result));
    EXPECT_TRUE(result[0].IsOK());
    EXPECT_EQ(2, result[0].Value());
    EXPECT_FALSE(result[1].IsOK());
}

TEST_F(TFutureTest, AllCombinerPropagateCancelation)
{
    auto p1 = NewPromise<void>();
    auto p2 = NewPromise<void>();
    std::vector<TFuture<void>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AllSucceeded(futures);
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
    f.Cancel(TError("oops"));
    EXPECT_TRUE(p1.IsCanceled());
    EXPECT_TRUE(p2.IsCanceled());
}

TEST_F(TFutureTest, AllCombinerDontPropagateCancelation)
{
    auto p1 = NewPromise<void>();
    auto p2 = NewPromise<void>();
    std::vector<TFuture<void>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AllSucceeded(
        futures,
        TFutureCombinerOptions{.PropagateCancelationToInput = false});
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
    f.Cancel(TError("oops"));
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
}

////////////////////////////////////////////////////////////////////////////////

TEST_PI(TFutureTest, AllSetWithTimeoutWorks, testing::Bool())
{
    bool cancelInputOnShortcut = GetParam();
    TFutureCombinerOptions combinerOptions{
        .CancelInputOnShortcut = cancelInputOnShortcut,
    };

    auto p1 = NewPromise<void>();
    auto p2 = NewPromise<void>();
    auto p3 = NewPromise<void>();
    std::vector<TFuture<void>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AllSetWithTimeout(futures, TDuration::MilliSeconds(100), combinerOptions);

    p1.Set();
    EXPECT_FALSE(f.IsSet());

    Sleep(TDuration::MilliSeconds(20));
    p3.Set(TError("oops"));
    EXPECT_FALSE(f.IsSet());

    Sleep(TDuration::MilliSeconds(300));
    EXPECT_TRUE(f.IsSet());
    const auto& resultOrError = f.Get();

    EXPECT_TRUE(p1.IsSet());
    EXPECT_TRUE(resultOrError.Value()[0].IsOK());

    EXPECT_EQ(p2.IsSet(), cancelInputOnShortcut);
    EXPECT_EQ(resultOrError.Value()[1].GetCode(), NYT::EErrorCode::Timeout);

    EXPECT_TRUE(p3.IsSet());
    EXPECT_FALSE(resultOrError.Value()[2].IsOK());
}

TEST_F(TFutureTest, AllSetWithTimeoutFuturesAreReleased)
{
    TWeakPtr<TRefCounted> wip1;
    TWeakPtr<TRefCounted> wip2;
    {
        auto p1 = NewPromise<TRefCountedPtr>();
        auto p2 = NewPromise<TRefCountedPtr>();
        std::vector<TFuture<TRefCountedPtr>> futures{
            p1.ToFuture(),
            p2.ToFuture()
        };
        auto f = AllSetWithTimeout(futures, TDuration::MilliSeconds(100));

        auto ip1 = New<TRefCounted>();
        wip1 = MakeWeak(ip1);
        p1.Set(std::move(ip1));
        EXPECT_FALSE(f.IsSet());

        auto ip2 = New<TRefCounted>();
        wip2 = MakeWeak(ip2);
        p2.Set(std::move(ip2));
        EXPECT_TRUE(f.IsSet());
    }

    // The WaitFor below ensures that the internal delayed executor cookie in AllSetWithTimeout is actually cancelled.
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::Zero()))
        .ThrowOnError();
    EXPECT_TRUE(wip1.IsExpired());
    EXPECT_TRUE(wip2.IsExpired());
}

TEST_PI(TFutureTest, AllSetWithTimeoutCancellation, testing::Bool())
{
    bool propagateCancelation = GetParam();
    TFutureCombinerOptions combinerOptions{
        .PropagateCancelationToInput = propagateCancelation,
    };

    auto p1 = NewPromise<void>();
    auto p2 = NewPromise<void>();
    auto p3 = NewPromise<void>();
    std::vector<TFuture<void>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AllSetWithTimeout(futures, TDuration::MilliSeconds(100), combinerOptions);

    Sleep(TDuration::MilliSeconds(20));
    p3.Set();

    Sleep(TDuration::MilliSeconds(20));
    f.Cancel(TError("oops"));

    EXPECT_EQ(propagateCancelation, p1.IsCanceled());
    EXPECT_EQ(propagateCancelation, p2.IsCanceled());
    EXPECT_TRUE(p3.IsSet());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFutureTest, AnyNCombinerEmpty)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnyNSucceeded(futures, 0);
    EXPECT_TRUE(f.IsSet());
    const auto& resultOrError = f.Get();
    EXPECT_TRUE(resultOrError.IsOK());
    const auto& result = resultOrError.Value();
    EXPECT_TRUE(result.empty());
    EXPECT_TRUE(p1.IsCanceled());
    EXPECT_TRUE(p2.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombinerDontCancelOnEmptyShortcut)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnyNSucceeded(
        futures,
        0,
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombinerInsufficientInputs)
{
    auto p1 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture()
    };
    auto f = AnyNSucceeded(futures, 2);
    EXPECT_TRUE(f.IsSet());
    const auto& resultOrError = f.Get();
    EXPECT_EQ(NYT::EErrorCode::FutureCombinerFailure, resultOrError.GetCode());
    EXPECT_TRUE(p1.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombinerDontCancelOnInsufficientInputsShortcut)
{
    auto p1 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture()
    };
    auto f = AnyNSucceeded(
        futures,
        2,
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
    EXPECT_FALSE(p1.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombinerTooManyFailures)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    auto p3 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AnyNSucceeded(futures, 2);
    EXPECT_FALSE(f.IsSet());
    EXPECT_FALSE(p3.IsCanceled());
    p1.Set(TError("oops1"));
    p2.Set(TError("oops2"));
    EXPECT_TRUE(f.IsSet());
    const auto& resultOrError = f.Get();
    EXPECT_EQ(NYT::EErrorCode::FutureCombinerFailure, resultOrError.GetCode());
    EXPECT_TRUE(p3.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombinerDontCancelOnTooManyFailuresShortcut)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    auto p3 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AnyNSucceeded(
        futures,
        2,
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
    p1.Set(TError("oops1"));
    p2.Set(TError("oops2"));
    EXPECT_FALSE(p3.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombiner)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    auto p3 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AnyNSucceeded(futures, 2);
    EXPECT_FALSE(f.IsSet());
    EXPECT_FALSE(p1.IsCanceled());
    p2.Set(1);
    p3.Set(2);
    EXPECT_TRUE(f.IsSet());
    const auto& resultOrError = f.Get();
    EXPECT_TRUE(resultOrError.IsOK());
    auto result = resultOrError.Value();
    std::sort(result.begin(), result.end());
    EXPECT_EQ(2, std::ssize(result));
    EXPECT_EQ(1, result[0]);
    EXPECT_EQ(2, result[1]);
    EXPECT_TRUE(p1.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombinerDontCancelOnShortcut)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    auto p3 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AnyNSucceeded(
        futures,
        2,
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
    p2.Set(1);
    p3.Set(2);
    EXPECT_FALSE(p1.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombinerDontCancelOnPropagateErrorShortcut)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    auto p3 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AnyNSucceeded(
        futures,
        2,
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
    p3.Set(TError("oops"));
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombinerVoid1)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    std::vector<TFuture<void>> futures{
        future
    };
    EXPECT_EQ(future, AnyNSucceeded(futures, 1));
}

TEST_F(TFutureTest, AnyNCombinerRetainError)
{
    auto p1 = NewPromise<int>();
    auto p2 = NewPromise<int>();
    auto p3 = NewPromise<int>();
    std::vector<TFuture<int>> futures{
        p1.ToFuture(),
        p2.ToFuture(),
        p3.ToFuture()
    };
    auto f = AnyNSet(futures, 2);
    EXPECT_FALSE(f.IsSet());
    p1.Set(2);
    EXPECT_FALSE(f.IsSet());
    p3.Set(TError("oops"));
    EXPECT_TRUE(f.IsSet());
    auto resultOrError = f.Get();
    EXPECT_TRUE(resultOrError.IsOK());
    const auto& result = resultOrError.Value();
    EXPECT_EQ(2, std::ssize(result));
    EXPECT_TRUE(result[0].IsOK());
    EXPECT_EQ(2, result[0].Value());
    EXPECT_FALSE(result[1].IsOK());
}

TEST_F(TFutureTest, AnyNCombinerPropagateCancelation)
{
    auto p1 = NewPromise<void>();
    auto p2 = NewPromise<void>();
    std::vector<TFuture<void>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnyNSucceeded(futures, 1);
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
    f.Cancel(TError("oops"));
    EXPECT_TRUE(p1.IsCanceled());
    EXPECT_TRUE(p2.IsCanceled());
}

TEST_F(TFutureTest, AnyNCombinerDontPropagateCancelation)
{
    auto p1 = NewPromise<void>();
    auto p2 = NewPromise<void>();
    std::vector<TFuture<void>> futures{
        p1.ToFuture(),
        p2.ToFuture()
    };
    auto f = AnyNSucceeded(
        futures,
        1,
        TFutureCombinerOptions{.PropagateCancelationToInput = false});
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
    f.Cancel(TError("oops"));
    EXPECT_FALSE(p1.IsCanceled());
    EXPECT_FALSE(p2.IsCanceled());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFutureTest, LastPromiseDied)
{
    TFuture<void> future;
    {
        auto promise = NewPromise<void>();
        future = promise;
        EXPECT_FALSE(future.IsSet());
    }
    Sleep(SleepQuantum);
    EXPECT_TRUE(future.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, future.Get().GetCode());
}

TEST_F(TFutureTest, PropagateErrorSync)
{
    auto p = NewPromise<int>();
    auto f1 = p.ToFuture();
    auto f2 = f1.Apply(BIND([] (int x) { return x + 1; }));
    p.Set(TError("Oops"));
    EXPECT_TRUE(f2.IsSet());
    EXPECT_FALSE(f2.Get().IsOK());
}

TEST_F(TFutureTest, PropagateErrorAsync)
{
    auto p = NewPromise<int>();
    auto f1 = p.ToFuture();
    auto f2 = f1.Apply(BIND([] (int x) { return MakeFuture(x + 1);}));
    p.Set(TError("Oops"));
    EXPECT_TRUE(f2.IsSet());
    EXPECT_FALSE(f2.Get().IsOK());
}

TEST_F(TFutureTest, WithDeadlineSuccess)
{
    auto p = NewPromise<void>();
    auto f1 = p.ToFuture();
    auto f2 = f1.WithDeadline(TInstant::Now() + TDuration::MilliSeconds(100));
    Sleep(TDuration::MilliSeconds(10));
    p.Set();
    EXPECT_TRUE(f2.Get().IsOK());
}

TEST_F(TFutureTest, WithDeadlineOnSet)
{
    auto p = NewPromise<void>();
    p.Set();
    auto f1 = p.ToFuture();
    auto f2 = f1.WithDeadline(TInstant::Now());
    EXPECT_TRUE(f1.Get().IsOK());
    EXPECT_TRUE(f2.Get().IsOK());
}

TEST_F(TFutureTest, WithDeadlineFail)
{
    auto p = NewPromise<int>();
    auto f1 = p.ToFuture();
    auto deadline = TInstant::Now() + SleepQuantum;
    auto f2 = f1.WithDeadline(deadline);
    EXPECT_EQ(NYT::EErrorCode::Timeout, f2.Get().GetCode());
    EXPECT_EQ(deadline, f2.Get().Attributes().Get<TInstant>("deadline"));
}

TEST_F(TFutureTest, WithTimeoutSuccess)
{
    auto p = NewPromise<void>();
    auto f1 = p.ToFuture();
    auto f2 = f1.WithTimeout(TDuration::MilliSeconds(100));
    Sleep(TDuration::MilliSeconds(10));
    p.Set();
    EXPECT_TRUE(f2.Get().IsOK());
}

TEST_F(TFutureTest, WithTimeoutOnSet)
{
    auto p = NewPromise<void>();
    p.Set();
    auto f1 = p.ToFuture();
    auto f2 = f1.WithTimeout(TDuration::MilliSeconds(0));
    EXPECT_TRUE(f1.Get().IsOK());
    EXPECT_TRUE(f2.Get().IsOK());
}

TEST_F(TFutureTest, WithTimeoutFail)
{
    auto p = NewPromise<int>();
    auto f1 = p.ToFuture();
    auto f2 = f1.WithTimeout(SleepQuantum);
    EXPECT_EQ(NYT::EErrorCode::Timeout, f2.Get().GetCode());
    EXPECT_EQ(SleepQuantum, f2.Get().Attributes().Get<TDuration>("timeout"));
}

TEST_W(TFutureTest, Holder)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    {
        TFutureHolder<void> holder(future);
    }
    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(promise.IsCanceled());
}

TEST_F(TFutureTest, JustAbandon)
{
    Y_UNUSED(NewPromise<void>());
}

TEST_F(TFutureTest, AbandonIsSet)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    promise.Reset();
    EXPECT_TRUE(future.IsSet());
}

TEST_F(TFutureTest, AbandonTryGet)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    promise.Reset();
    EXPECT_EQ(NYT::EErrorCode::Canceled, future.TryGet()->GetCode());
}

TEST_F(TFutureTest, AbandonGet)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    promise.Reset();
    EXPECT_EQ(NYT::EErrorCode::Canceled, future.Get().GetCode());
}

TEST_F(TFutureTest, AbandonSubscribe)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    promise.Reset();
    bool called = false;
    future.Subscribe(BIND([&] (const TError&) mutable { called = true; }));
    EXPECT_TRUE(called);
}

TEST_F(TFutureTest, SubscribeAbandon)
{
    bool called = false;
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    future.Subscribe(BIND([&] (const TError&) mutable {
        YT_ASSERT_INVOKER_AFFINITY(GetFinalizerInvoker());
        called = true;
    }));
    promise.Reset();
    Sleep(SleepQuantum);
    EXPECT_TRUE(called);
}

TEST_F(TFutureTest, OnCanceledAbandon)
{
    bool called = false;
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    promise.OnCanceled(BIND([&] (const TError& /*error*/) {
        called = true;
    }));
    promise.Reset();
    Sleep(SleepQuantum);
    EXPECT_FALSE(called);
}

TEST_F(TFutureTest, OnCanceledResult)
{
    {
        auto promise = NewPromise<void>();
        EXPECT_TRUE(promise.OnCanceled(BIND([&] (const TError& /*error*/) {})));
    }

    {
        auto promise = NewPromise<void>();
        promise.Set();
        EXPECT_FALSE(promise.OnCanceled(BIND([&] (const TError& /*error*/) {})));
    }
}

TString OnCallResult(const TErrorOr<int>& /*callResult*/)
{
    THROW_ERROR_EXCEPTION("Call failed");
}

TEST_F(TFutureTest, LtoCrash)
{
    auto future = MakeFuture<int>(0);
    auto nextFuture = future.Apply(BIND(OnCallResult));
}

struct S
{
    static int DestroyedCounter;

    ~S()
    {
        ++DestroyedCounter;
    }
};

int S::DestroyedCounter = 0;

TEST_F(TFutureTest, CancelableDoesNotProhibitDestruction)
{
    auto promise = NewPromise<S>();
    promise.Set(S());

    auto cancelable = promise.ToFuture().AsCancelable();

    auto before = S::DestroyedCounter;
    promise.Reset();
    auto after = S::DestroyedCounter;
    EXPECT_EQ(1, after - before);
}

TEST_F(TFutureTest, AbandonCancel)
{
    TMpscStack<TFuture<void>> queue;
    std::thread producer([&] {
        for (int i = 0; i < 10000; i++) {
            auto p = NewPromise<void>();
            queue.Enqueue(p.ToFuture());
            Sleep(TDuration::MicroSeconds(1));
        }

        queue.Enqueue(TFuture<void>());
    });

    bool stop = false;
    while (!stop) {
        for (auto future : queue.DequeueAll(true)) {
            if (!future) {
                stop = true;
                break;
            }

            future.Cancel(TError("Cancel"));
        }
    }

    producer.join();
}

TEST_F(TFutureTest, AbandonBeforeGet)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    promise.Reset();
    EXPECT_EQ(future.Get().GetCode(), NYT::EErrorCode::Canceled);
}

TEST_F(TFutureTest, AbandonDuringGet)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    std::thread thread([&] {
        Sleep(TDuration::MilliSeconds(100));
        promise.Reset();
    });
    EXPECT_EQ(future.Get().GetCode(), NYT::EErrorCode::Canceled);
    thread.join();
}

TEST_F(TFutureTest, CancelAppliedToUncancellable)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    auto uncancelable = future.ToUncancelable();
    auto future1 = uncancelable.Apply(BIND([&] () -> void {}));
    future1.Cancel(TError("Cancel"));
    EXPECT_FALSE(promise.IsSet());
    EXPECT_FALSE(promise.IsCanceled());
    EXPECT_FALSE(uncancelable.IsSet());
    EXPECT_FALSE(future1.IsSet());

    auto immediatelyCancelable = uncancelable.ToImmediatelyCancelable();
    auto future2 = immediatelyCancelable.Apply(BIND([&] () -> void {}));
    future2.Cancel(TError("Cancel"));
    EXPECT_FALSE(promise.IsSet());
    EXPECT_FALSE(promise.IsCanceled());
    EXPECT_TRUE(immediatelyCancelable.IsSet());
    EXPECT_TRUE(future2.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, future2.Get().GetCode());

    auto immediatelyCancelable2 = future.ToImmediatelyCancelable(/*propagateCancelation*/ false);
    auto future3 = immediatelyCancelable2.Apply(BIND([&] () -> void {}));
    future3.Cancel(TError("Cancel"));
    EXPECT_FALSE(promise.IsSet());
    EXPECT_FALSE(promise.IsCanceled());
    EXPECT_TRUE(immediatelyCancelable2.IsSet());
    EXPECT_TRUE(future3.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, future3.Get().GetCode());

    promise.Set();
    EXPECT_TRUE(uncancelable.IsSet());
    EXPECT_TRUE(future1.IsSet());
    EXPECT_TRUE(future1.Get().IsOK());
}

TEST_F(TFutureTest, AsyncViaCanceledInvoker1)
{
    auto queue = New<NConcurrency::TActionQueue>();
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(queue->GetInvoker());

    context->Cancel(TError(NYT::EErrorCode::Canceled, "From cancelable context!"));

    auto error = WaitFor(BIND([] {}).AsyncVia(invoker).Run());

    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Canceled);
    EXPECT_TRUE(NYT::ToString(error).Contains("From cancelable context!"))
        << NYT::ToString(error);
}

TEST_F(TFutureTest, AsyncViaCanceledInvoker2)
{
    auto queue = New<NConcurrency::TActionQueue>();
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(queue->GetInvoker());

    auto taskReady = NewPromise<void>();
    auto promise = NewPromise<void>();

    auto future = BIND([promise, taskReady, invoker] {
        taskReady.Set();
        WaitFor(promise.ToFuture(), invoker).ThrowOnError();
    })
        .AsyncVia(invoker)
        .Run();

    WaitFor(taskReady.ToFuture()).ThrowOnError();

    context->Cancel(TError(NYT::EErrorCode::Canceled, "From cancelable context!"));
    promise.Set();

    auto error = WaitFor(future);

    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Canceled);
    EXPECT_TRUE(NYT::ToString(error).Contains("From cancelable context!"))
        << NYT::ToString(error);
}

TError CreateFiberCanceledError(TError cancelationError)
{
    auto aqueue = New<NConcurrency::TActionQueue>();
    auto invoker = aqueue->GetInvoker();
    auto leash = NewPromise<void>();
    auto taskStarted = NewPromise<void>();
    auto future = BIND([leash, taskStarted] {
        taskStarted.Set();
        WaitFor(leash.ToFuture()).ThrowOnError();
    }).AsyncVia(invoker).Run();

    WaitFor(taskStarted.ToFuture()).ThrowOnError();

    future.Cancel(cancelationError);
    return WaitFor(future);
}

TEST_F(TFutureTest, YT_12720)
{
    auto error = CreateFiberCanceledError(NYT::TError(NYT::EErrorCode::Canceled, "Fiber canceled in .Reset() of TFiberGuard"));
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Canceled);
    EXPECT_TRUE(NYT::ToString(error).Contains("Fiber canceled in .Reset() of TFiberGuard"))
        << NYT::ToString(error);
}

TEST_F(TFutureTest, DiscardInApply)
{
    auto aqueue = New<NConcurrency::TActionQueue>();
    auto invoker = aqueue->GetInvoker();

    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    // NB(arkady-e1ppa): mutable is required to destructively move promise out of the
    // closure thus forcing it to be destroyed inside the scope.
    auto canceled = BIND([p = std::move(promise)] () mutable {
        auto localPromise = std::move(p);
        while (true) {
            Yield();
        }
    }).AsyncVia(invoker).Run();

    Sleep(std::chrono::seconds(1));

    canceled.Cancel(TError(NYT::EErrorCode::Canceled, "Canceled!"));

    auto error = WaitFor(future);
    EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Canceled);
    EXPECT_TRUE(NYT::ToString(error).Contains("Canceled!"))
        << NYT::ToString(error);
}

TEST_F(TFutureTest, ErrorFromException)
{
    // Creating error from exception whenever possible is important for FromExceptionEnricher. So test it with enricher.

    static thread_local bool testFromExceptionEnricherEnabled = false;
    testFromExceptionEnricherEnabled = true;
    auto finally = Finally([] {
        testFromExceptionEnricherEnabled = false;
    });

    static auto getAttribute = [] (const TError& error) {
        return error.Attributes().Get<TString>("test_attribute", "");
    };

    TError::RegisterFromExceptionEnricher([](TError* error, const std::exception&) {
        if (testFromExceptionEnricherEnabled) {
            *error <<= TErrorAttribute("test_attribute", getAttribute(*error) + "X");
        }
    });

    static auto getError = [] (auto&& func) -> TError {
        return BIND(func).AsyncVia(GetSyncInvoker()).Run().Get();
    };

    // If there is no exception, there is no error.
    EXPECT_TRUE(getError([] {}).IsOK());

    // If there is std::exception, there is error and enricher is called.
    {
        auto error = getError([] {
            throw std::runtime_error("test_std");
        });
        ASSERT_FALSE(error.IsOK());
        EXPECT_TRUE(error.GetMessage().contains("test_std"));
        EXPECT_EQ(getAttribute(error), "X");
    }

    // If there is TErrorException, there is an error and enricher is called.
    {
        auto error = getError([] {
            THROW_ERROR_EXCEPTION("test_yt");
        });
        ASSERT_FALSE(error.IsOK());
        EXPECT_TRUE(error.GetMessage().contains("test_yt"));
        EXPECT_EQ(getAttribute(error), "X");
    }

    // If there is TFiberCanceledException, there is an error, but enricher is not called.
    {
        auto error = CreateFiberCanceledError(NYT::TError(NYT::EErrorCode::Canceled, "test_fiber_canceled"));

        ASSERT_FALSE(error.IsOK());
        EXPECT_TRUE(error.GetMessage().contains("test_fiber_canceled"));
        EXPECT_EQ(getAttribute(error), "");
    }
}

class TContextSwitchTracker
    : public TContextSwitchGuard
{
public:
    TContextSwitchTracker()
        : TContextSwitchGuard([this] { Switched_ = true; }, nullptr)
    { }

    bool IsSwitched() const
    {
        return Switched_;
    }

private:
    bool Switched_ = false;
};

TEST_W(TFutureTest, WaitForDelayed)
{
    auto future = TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(10))
        .Apply(BIND([] { return 123; }));
    TContextSwitchTracker switchTracker;
    auto result = WaitFor(future);
    EXPECT_TRUE(switchTracker.IsSwitched());
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(result.Value(), 123);
}

TEST_W(TFutureTest, WaitForAlreadySet)
{
    auto future = MakeFuture<int>(123);
    TContextSwitchTracker switchTracker;
    auto result = WaitFor(future);
    EXPECT_TRUE(switchTracker.IsSwitched());
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(result.Value(), 123);
}

TEST_W(TFutureTest, WaitForFast)
{
    auto future = MakeFuture<int>(123);
    TContextSwitchTracker switchTracker;
    auto result = WaitForFast(future);
    EXPECT_FALSE(switchTracker.IsSwitched());
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(result.Value(), 123);
}

TEST_W(TFutureTest, WaitForUnique)
{
    auto future = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(123));
    TContextSwitchTracker switchTracker;
    auto result = WaitFor(future.AsUnique());
    EXPECT_TRUE(switchTracker.IsSwitched());
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(*result.Value(), 123);
}

TEST_W(TFutureTest, WaitForUniqueFast)
{
    auto future = MakeFuture<std::unique_ptr<int>>(std::make_unique<int>(123));
    TContextSwitchTracker switchTracker;
    auto result = WaitForFast(future.AsUnique());
    EXPECT_FALSE(switchTracker.IsSwitched());
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(*result.Value(), 123);
}

TEST_F(TFutureTest, TrySetDoesNotMoveValueOnFailure)
{
    auto promise = NewPromise<std::vector<int>>();
    auto v = std::vector{1, 2, 3};
    promise.Set(v);
    EXPECT_FALSE(promise.TrySet(std::move(v)));
    EXPECT_EQ(std::ssize(v), 3);
}

TEST_F(TFutureTest, TrySetDoesNotMoveErrorOnFailure)
{
    auto promise = NewPromise<std::vector<int>>();
    auto err = TError("oops");
    promise.Set(err);
    EXPECT_FALSE(promise.TrySet(std::move(err)));
    EXPECT_FALSE(err.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
