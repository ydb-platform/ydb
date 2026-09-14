#include <yt/yt/library/profiling/sensors_owner/sensors_owner.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace std::literals;

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSensorsOwnerTest, Example)
{
    TProfiler profiler("", "flow/test");
    auto owner = TSensorsOwner(profiler);

    struct TSensors
    {
        TProfiler Profiler;
        TCounter Counter = Profiler.Counter("/my_counter");
        TSensorsOwner OtherSensors{Profiler};
    };

    struct TChildSensors
    {
        TCounter Counter;

        explicit TChildSensors(const TProfiler& p)
            : Counter(p.Counter("/my_counter_2"))
        { }
    };

    struct TAnotherSensors
    {
        TProfiler Profiler;
        int Key;
        TCounter Counter = Profiler.WithTag("counter", ToString(Key)).Counter("/another_counter");
    };

    struct TWithTagsSensors
    {
        TProfiler Profiler;
        TCounter Counter = Profiler.Counter("/by_tags_counter");
    };

    struct TSharedSensors final
    {
        TProfiler Profiler;
        TCounter Counter = Profiler.Counter("/under_ptr_counter");
    };

    using TSharedSensorsPtr = NYT::TIntrusivePtr<TSharedSensors>;

    owner.Increment("/my_simple_counter", 1);
    owner.Get<TSensors>().OtherSensors.Get<TChildSensors>().Counter.Increment(1);
    owner.Get<TSharedSensorsPtr>()->Counter.Increment(1);
    owner.Get<TAnotherSensors>(42).Counter.Increment(1);
    owner.WithPrefix("/prefix").Get<TAnotherSensors>(42).Counter.Increment(1);
    owner.GetWithTags<TWithTagsSensors>(TTagSet().WithTag({"key", "value"})).Counter.Increment(1);
    owner.WithTags(TTagSet().WithTag({"key", "value2"})).Get<TWithTagsSensors>().Counter.Increment(1);

    struct THistogramSensors
    {
        TProfiler Profiler;
        int Key;
        std::vector<TDuration> Buckets;
        TEventTimer Histogram = Profiler.WithTag("tag", ToString(Key)).TimeHistogram("/another_counter", Buckets);
    };

    owner.Get<THistogramSensors>(/*Key*/ 132, /*Buckets*/ std::vector<TDuration>{5s, 10min}).Histogram.Record(6s);
}

void DoSmth(/*... , */ const TSensorsOwner& sensorsOwner)
{
    // In a function, you can declare a structure with metrics right in place and use it.
    struct TSensors
    {
        TProfiler Profiler;
        TCounter TotalCount = Profiler.Counter("/count");
        TCounter FailedCount = Profiler.Counter("/failed_count");
    };

    // Here, it is the same reference to the metric object, assuming that the same sensorsOwner is passed into the function.
    // The .Get method is quite efficient, but it is still better not to call it unnecessarily.
    const auto& sensors = sensorsOwner.Get<TSensors>();

    //...
    bool failed = false;
    //...

    sensors.TotalCount.Increment(1);
    if (failed) {
        sensors.FailedCount.Increment(1);
    }
}

TEST(TSensorsOwnerTest, Simple)
{
    TProfiler profiler("", "flow/test");
    auto registryPtr = profiler.GetRegistry();
    auto owner = TSensorsOwner(profiler);

    DoSmth(owner);

    ASSERT_EQ(registryPtr, owner.GetProfiler().GetRegistry()); // Equal profilers.

    struct TChild1
    {
        TProfiler Profiler;
        int A = 1;
    };

    ASSERT_EQ(registryPtr, owner.Get<TChild1>().Profiler.GetRegistry());
    ASSERT_EQ(1, owner.Get<TChild1>().A);
    ASSERT_EQ(owner.Get<TChild1>().Profiler.GetRegistry(), owner.Get<TChild1>().Profiler.GetRegistry());
    ASSERT_EQ(&owner.Get<TChild1>(), &owner.Get<TChild1>());

    struct TChild2
    {
        TProfiler Profiler;
        int B = 2;

        explicit TChild2(const TProfiler& p)
            : Profiler(p)
        {
        }
    };

    ASSERT_EQ(registryPtr, owner.Get<TChild2>().Profiler.GetRegistry());
    ASSERT_EQ(2, owner.Get<TChild2>().B);
    ASSERT_EQ(owner.Get<TChild2>().Profiler.GetRegistry(), owner.Get<TChild2>().Profiler.GetRegistry());

    struct TSensorsByKey
    {
        TProfiler Profiler;
        int Key;
        TCounter Counter = Profiler.WithTag("key", ToString(Key)).Counter("/by_key_counter");
    };

    ASSERT_EQ(42, (owner.Get<TSensorsByKey>(42).Key));
    ASSERT_EQ(43, (owner.Get<TSensorsByKey>(43).Key));

    struct TWithTagsSensors
    {
        TProfiler Profiler;
        TCounter Counter = Profiler.Counter("/by_tags_counter");
    };

    ASSERT_EQ(
        &owner.GetWithTags<TWithTagsSensors>(TTagSet().WithTag({"key", "value"})),
        &owner.GetWithTags<TWithTagsSensors>(TTagSet().WithTag({"key", "value"})));

    ASSERT_EQ(
        &owner.WithTags(TTagSet().WithTag({"key", "value2"})).Get<TWithTagsSensors>(),
        &owner.WithTags(TTagSet().WithTag({"key", "value2"})).Get<TWithTagsSensors>());

    ASSERT_EQ(
        &owner.WithPrefix("/prefix").Get<TChild1>(),
        &owner.WithPrefix("/prefix").Get<TChild1>());
}

TEST(TSensorsOwnerTest, Copy)
{
    auto owner = TSensorsOwner(TProfiler("", "flow/test"));
    auto owner2 = owner;

    struct TChild
    {
        TProfiler Profiler;
    };

    ASSERT_EQ(&owner.Get<TChild>(), &owner2.Get<TChild>()); // The same owner.
}

TEST(TSensorsOwnerTest, WithGlobal)
{
    auto owner = TSensorsOwner(TProfiler("", "flow/test"));
    owner.WithGlobal().GetCounter("counter").Increment(1); // Expect no fail here.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
