#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/propagating_storage.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFirst
{
    TString Value;
};

struct TSecond
{
    TString Value;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TPropagatingStorageTest, Simple)
{
    auto actionQueue = New<TActionQueue>();

    auto& storage = GetCurrentPropagatingStorage();
    storage.Exchange<TFirst>({"hello"});
    ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");

    WaitFor(
        BIND([actionQueue] {
            auto& storage = GetCurrentPropagatingStorage();
            ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");
            storage.Exchange<TFirst>({"inner"});
            ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "inner");
            storage.Exchange<TSecond>({"some"});

            WaitFor(
                BIND([] {
                    auto& storage = GetCurrentPropagatingStorage();
                    ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "inner");
                    storage.Remove<TFirst>();
                    ASSERT_FALSE(storage.Has<TFirst>());
                    ASSERT_EQ(storage.Find<TFirst>(), nullptr);
                })
                    .AsyncVia(actionQueue->GetInvoker())
                    .Run())
                .ThrowOnError();

            ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "inner");
            ASSERT_EQ(storage.GetOrCrash<TSecond>().Value, "some");
        })
            .AsyncVia(actionQueue->GetInvoker())
            .Run())
        .ThrowOnError();

    ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");
    ASSERT_FALSE(storage.Has<TSecond>());
}

TEST(TPropagatingStorageTest, Cow)
{
    auto actionQueue = New<TActionQueue>();

    auto& storage = GetCurrentPropagatingStorage();
    storage.Exchange<TFirst>({"hello"});
    ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(
            BIND([] {
                auto& storage = GetCurrentPropagatingStorage();
                ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");
                TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
                ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");
            })
                .AsyncVia(actionQueue->GetInvoker())
                .Run());
    }

    futures.push_back(
        BIND([] {
            auto& storage = GetCurrentPropagatingStorage();
            ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");
            storage.Exchange<TFirst>({"goodbye"});
            ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "goodbye");
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "goodbye");
        })
            .AsyncVia(actionQueue->GetInvoker())
            .Run());

    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

TEST(TPropagatingStorageTest, Null)
{
    TNullPropagatingStorageGuard guard;

    auto &storage = GetCurrentPropagatingStorage();

    ASSERT_TRUE(storage.IsNull());
    ASSERT_TRUE(storage.IsEmpty());

    storage.Exchange<TFirst>({"goodbye"});
    ASSERT_FALSE(storage.IsNull());
    ASSERT_FALSE(storage.IsEmpty());

    storage.Remove<TFirst>();
    ASSERT_FALSE(storage.IsNull());
    ASSERT_TRUE(storage.IsEmpty());
}

TEST(TPropagatingStorageTest, PropagatingValue)
{
    auto& storage = GetCurrentPropagatingStorage();
    storage.Exchange<TFirst>({"hello"});
    storage.Exchange<TSecond>({"world"});

    ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");
    ASSERT_EQ(storage.GetOrCrash<TSecond>().Value, "world");

    {
        TPropagatingValueGuard<TFirst> guard({"next"});
        ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "next");
        ASSERT_EQ(storage.GetOrCrash<TSecond>().Value, "world");
    }

    ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");
    ASSERT_EQ(storage.GetOrCrash<TSecond>().Value, "world");

    storage.Remove<TSecond>();

    {
        TPropagatingValueGuard<TSecond> guard({"earth"});
        ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");
        ASSERT_EQ(storage.GetOrCrash<TSecond>().Value, "earth");
    }

    ASSERT_EQ(storage.GetOrCrash<TFirst>().Value, "hello");
    ASSERT_FALSE(storage.Has<TSecond>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency

