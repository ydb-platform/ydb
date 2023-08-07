#include <gtest/gtest.h>

#include <gmock/gmock.h>

#include <library/cpp/yt/memory/safe_memory_reader.h>

#include <library/cpp/yt/backtrace/cursors/frame_pointer/frame_pointer_cursor.h>

#include <library/cpp/yt/backtrace/cursors/interop/interop.h>

#include <util/system/compiler.h>

#include <contrib/libs/libunwind/include/libunwind.h>

namespace NYT::NBacktrace {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <int Depth, class TFn>
Y_NO_INLINE void RunInDeepStack(TFn cb)
{
    if constexpr (Depth == 0) {
        cb();
    } else {
        std::vector<int> touchMem;
        touchMem.push_back(0);

        RunInDeepStack<Depth-1>(cb);

        DoNotOptimizeAway(touchMem);
    }
}

TEST(TFramePointerCursor, FramePointerCursor)
{
    std::vector<const void*> backtrace;
    RunInDeepStack<64>([&] {
        unw_context_t unwContext;
        ASSERT_TRUE(unw_getcontext(&unwContext) == 0);

        unw_cursor_t unwCursor;
        ASSERT_TRUE(unw_init_local(&unwCursor, &unwContext) == 0);

        TSafeMemoryReader reader;
        auto fpCursorContext = NBacktrace::FramePointerCursorContextFromLibunwindCursor(unwCursor);
        NBacktrace::TFramePointerCursor fpCursor(&reader, fpCursorContext);

        while (!fpCursor.IsFinished()) {
            backtrace.push_back(fpCursor.GetCurrentIP());
            fpCursor.MoveNext();
        }
    });

    ASSERT_THAT(backtrace, testing::SizeIs(testing::Ge(64u)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBacktrace
