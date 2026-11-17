#include <gtest/gtest.h>

#include <gmock/gmock.h>

#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/backtrace/absl_unwinder/absl_unwinder.h>

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

#include <absl/debugging/stacktrace.h>

#include <util/generic/size_literals.h>

#include <util/system/context.h>

namespace NYT::NBacktrace {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCapturedFrames
{
    std::array<const void*, 64> Frames{};
    const void* Caller = nullptr;
    int Count = 0;
};

Y_NO_INLINE TCapturedFrames CaptureFrames(int framesToSkip)
{
    TCapturedFrames result;
    result.Caller = __builtin_return_address(0);
    unw_context_t context;
    if (unw_getcontext(&context) != 0) {
        return result;
    }
    TLibunwindCursor cursor(context);
    result.Count = std::ssize(GetBacktrace(&cursor, TMutableRange(result.Frames), framesToSkip));
    return result;
}

Y_NO_INLINE TCapturedFrames CaptureAbslFrames(int framesToSkip)
{
    TCapturedFrames result;
    result.Caller = static_cast<const char*>(__builtin_return_address(0)) - 1;
    std::array<void*, 64> frames;
    result.Count = absl::GetStackTrace(frames.data(), std::ssize(frames), framesToSkip);
    std::copy_n(frames.begin(), result.Count, result.Frames.begin());
    return result;
}

TEST(TStackTrace, SkipFrames)
{
    auto frames = CaptureFrames(/*framesToSkip*/ 0);
    ASSERT_GT(frames.Count, 1);
    EXPECT_EQ(frames.Frames[1], frames.Caller);

    frames = CaptureFrames(/*framesToSkip*/ 1);
    ASSERT_GT(frames.Count, 0);
    EXPECT_EQ(frames.Frames[0], frames.Caller);
}

TEST(TStackTrace, AbslFrames)
{
    SetAbslStackUnwinder();

    auto frames = CaptureAbslFrames(/*framesToSkip*/ 0);
    auto skippedFrames = CaptureAbslFrames(/*framesToSkip*/ 1);
    absl::SetStackUnwinder(nullptr);

    ASSERT_GT(frames.Count, 1);
    EXPECT_EQ(frames.Frames[1], frames.Caller);

    ASSERT_GT(skippedFrames.Count, 0);
    EXPECT_EQ(skippedFrames.Frames[0], skippedFrames.Caller);
}

TEST(TStackTrace, UnwindFiber)
{
    class TTrampoline
        : public ITrampoLine
    {
    public:
        TContMachineContext* Caller = nullptr;
        TContMachineContext* Fiber = nullptr;
        int FrameCount = 0;
        int UnwindResult = 0;
        TCapturedFrames Captured;

        void DoRun() override
        {
            unw_context_t context;
            UnwindResult = unw_getcontext(&context);
            if (UnwindResult == 0) {
                unw_cursor_t cursor;
                UnwindResult = unw_init_local(&cursor, &context);
                if (UnwindResult == 0) {
                    do {
                        ++FrameCount;
                        UnwindResult = unw_step(&cursor);
                    } while (UnwindResult > 0 && FrameCount < 64);
                }
            }
            Captured = CaptureFrames(/*framesToSkip*/ 0);
            Fiber->SwitchTo(Caller);
        }
    } trampoline;

    std::array<char, 64_KB> stack;
    TContMachineContext caller;
    TContMachineContext fiber({&trampoline, TArrayRef(stack.data(), stack.size())});
    trampoline.Caller = &caller;
    trampoline.Fiber = &fiber;
    caller.SwitchTo(&fiber);

    EXPECT_EQ(trampoline.UnwindResult, 0);
    EXPECT_GT(trampoline.FrameCount, 1);
    EXPECT_LT(trampoline.FrameCount, 64);
    ASSERT_GT(trampoline.Captured.Count, 1);
    EXPECT_EQ(trampoline.Captured.Frames[1], trampoline.Captured.Caller);
}

#ifdef SYMBOLIZED_BUILD

using ::testing::ContainsRegex;

std::string GetStackTrace()
{
    std::array<const void*, 1> buffer;
    unw_context_t context;
    if (unw_getcontext(&context) != 0) {
        return {};
    }
    TLibunwindCursor cursor(context);
    auto backtrace = GetBacktrace(
        &cursor,
        TMutableRange(buffer),
        /*framesToSkip*/ 0);
    return SymbolizeBacktrace(backtrace);
}

TEST(TStackTrace, Format)
{
    ASSERT_THAT(
        GetStackTrace(),
        ContainsRegex(
            "^ 1\\. 0x[0-9a-f]+ in NYT::NBacktrace::\\(anonymous namespace\\)::GetStackTrace.* "
            "at .+/library/cpp/yt/backtrace/symbolizers/dwarf/unittests/backtrace_ut.cpp:[0-9]+\\n"));
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBacktrace
