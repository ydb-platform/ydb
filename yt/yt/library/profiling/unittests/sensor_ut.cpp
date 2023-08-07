#include <gtest/gtest.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/testing.h>

#include <yt/yt/library/profiling/solomon/registry.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

TProfiler EventQueue{"/event_queue"};

auto GlobalEventCounter = EventQueue.Counter("/event_counter");
auto GlobalQueueSize = EventQueue.Gauge("/queue_size");

TEST(Sensor, TestingApi)
{
    GlobalEventCounter.Increment();
    ASSERT_EQ(1, TTesting::ReadCounter(GlobalEventCounter));

    GlobalQueueSize.Update(10);
    ASSERT_EQ(10, TTesting::ReadGauge(GlobalQueueSize));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
