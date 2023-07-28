#include <gtest/gtest.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/testing.h>

#include <yt/yt/library/profiling/solomon/registry.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////


TEST(Profiler, SaveOptions)
{
    auto profiler = TProfiler(
        "my_prefix", "my_namespace", TTagSet{{{"my_tag", "my_tag_value"}}}, nullptr,
        TSensorOptions{.TimeHistogramBounds = {TDuration::Seconds(1)}});

    {
        ASSERT_FALSE(TTesting::ReadOptions(profiler).Global);
        auto newProfiler = profiler.WithGlobal();
        ASSERT_TRUE(TTesting::ReadOptions(newProfiler).Global);
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
    {
        ASSERT_FALSE(TTesting::ReadOptions(profiler).Sparse);
        auto newProfiler = profiler.WithSparse();
        ASSERT_TRUE(TTesting::ReadOptions(newProfiler).Sparse);
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
    {
        ASSERT_FALSE(TTesting::ReadOptions(profiler).Hot);
        auto newProfiler = profiler.WithHot();
        ASSERT_TRUE(TTesting::ReadOptions(newProfiler).Hot);
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
    {
        ASSERT_FALSE(TTesting::ReadOptions(profiler).DisableDefault);
        auto newProfiler = profiler.WithDefaultDisabled();
        ASSERT_TRUE(TTesting::ReadOptions(newProfiler).DisableDefault);
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
    {
        ASSERT_FALSE(TTesting::ReadOptions(profiler).DisableProjections);
        auto newProfiler = profiler.WithProjectionsDisabled();
        ASSERT_TRUE(TTesting::ReadOptions(newProfiler).DisableProjections);
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
    {
        ASSERT_FALSE(TTesting::ReadOptions(profiler).DisableSensorsRename);
        auto newProfiler = profiler.WithRenameDisabled();
        ASSERT_TRUE(TTesting::ReadOptions(newProfiler).DisableSensorsRename);
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }

    {
        auto newProfiler = profiler.WithPrefix("new_prefix");
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
    {
        auto newProfiler = profiler.WithTag("new_key", "new_value");
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
    {
        auto newProfiler = profiler.WithRequiredTag("new_key", "new_value");
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
    {
        auto newProfiler = profiler.WithExcludedTag("new_key", "new_value");
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
    {
        auto newProfiler = profiler.WithAlternativeTag("new_key", "new_value", 0);
        ASSERT_EQ(TTesting::ReadOptions(newProfiler).TimeHistogramBounds.size(), 1u);
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
