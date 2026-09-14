#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/intern_registry.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/yt/threading/event_count.h>

#include <util/system/yield.h>

#include <atomic>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TInternRegistryTestControl
{
    std::atomic<bool> BlockNextHash = true;
    NThreading::TEvent HashStarted;
    NThreading::TEvent ContinueHash;
};

struct TInternRegistryTestValue
{
    std::string Data;
    TInternRegistryTestControl* Control = nullptr;
};

bool operator==(const TInternRegistryTestValue& lhs, const TInternRegistryTestValue& rhs)
{
    return lhs.Data == rhs.Data;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

template <>
struct THash<NYT::TInternRegistryTestValue>
{
    size_t operator()(const NYT::TInternRegistryTestValue& value) const
    {
        if (value.Control && value.Control->BlockNextHash.exchange(false)) {
            value.Control->HashStarted.NotifyAll();
            value.Control->ContinueHash.Wait();
        }
        return THash<std::string>()(value.Data);
    }
};

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TStringRegistry = TInternRegistry<std::string>;
using TInternedString = TInternedObject<std::string>;
using TRaceRegistry = TInternRegistry<TInternRegistryTestValue>;
using TInternedRaceValue = TInternedObject<TInternRegistryTestValue>;

TEST(TInternRegistryTest, TestEmptyRegistry)
{
    auto registry = New<TStringRegistry>();
    EXPECT_EQ(0, registry->GetSize());
}

TEST(TInternRegistryTest, TestEmptyInstance)
{
    TInternedString s;
    EXPECT_EQ(0u, s->length());
}

TEST(TInternRegistryTest, Simple)
{
    auto registry = New<TStringRegistry>();
    EXPECT_EQ(0, registry->GetSize());

    auto s1 = registry->Intern(std::string("hello"));
    EXPECT_EQ(1, registry->GetSize());

    auto s2 = registry->Intern(std::string("world"));
    EXPECT_EQ(2, registry->GetSize());

    auto s3 = registry->Intern(std::string("hello"));
    EXPECT_EQ(2, registry->GetSize());

    EXPECT_TRUE(*s1 == *s3);
    EXPECT_FALSE(*s1 == *s2);

    auto s4 = registry->Intern(std::string("test"));
    EXPECT_EQ(3, registry->GetSize());

    s4 = TInternedString();
    EXPECT_EQ(2, registry->GetSize());

    s3 = TInternedString();
    EXPECT_EQ(2, registry->GetSize());

    s2 = TInternedString();
    EXPECT_EQ(1, registry->GetSize());

    s1 = TInternedString();
    EXPECT_EQ(0, registry->GetSize());
}


TEST(TInternRegistryTest, Default)
{
    auto registry = New<TStringRegistry>();
    EXPECT_EQ(0, registry->GetSize());

    auto s1 = TInternedString();

    auto s2 = registry->Intern(std::string());
    EXPECT_EQ(0, registry->GetSize());

    EXPECT_TRUE(*s1 == *s2);
}

TEST(TInternRegistryTest, DoesNotResurrectObjectPendingDestruction)
{
    auto registry = New<TRaceRegistry>();
    auto interned = registry->Intern(TInternRegistryTestValue{.Data = "value"});
    auto* internedData = interned.ToDataPtr().Get();

    TInternRegistryTestControl control;
    TInternRegistryTestValue lookupValue{
        .Data = "value",
        .Control = &control,
    };
    TInternedRaceValue result;

    auto internQueue = New<NConcurrency::TActionQueue>("Intern");
    auto destroyQueue = New<NConcurrency::TActionQueue>("Destroy");

    auto internFuture = BIND([&] {
        result = registry->Intern(lookupValue);
    })
        .AsyncVia(internQueue->GetInvoker())
        .Run();

    control.HashStarted.Wait();

    auto destroyFuture = BIND([&] {
        interned = TInternedRaceValue();
    })
        .AsyncVia(destroyQueue->GetInvoker())
        .Run();

    // The intern action holds the registry lock, so the object remains alive while
    // its destructor waits in OnInternedDataDestroyed after the refcount reaches zero.
    while (internedData->GetRefCount() != 0) {
        ThreadYield();
    }

    control.ContinueHash.NotifyAll();

    NConcurrency::WaitFor(destroyFuture)
        .ThrowOnError();
    NConcurrency::WaitFor(internFuture)
        .ThrowOnError();

    destroyQueue->Shutdown();
    internQueue->Shutdown();

    EXPECT_EQ("value", result->Data);
    EXPECT_EQ(1, registry->GetSize());

    result = TInternedRaceValue();
    EXPECT_EQ(0, registry->GetSize());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
