#include "spinlock_profiler.h"

#include <library/cpp/yt/backtrace/cursors/interop/interop.h>

#include <library/cpp/yt/misc/tls.h>

#include <absl/base/internal/spinlock.h>
#include <absl/base/internal/cycleclock.h>

#include <util/system/yield.h>

#include <mutex>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TSpinlockProfiler::TSpinlockProfiler(TSpinlockProfilerOptions options)
    : TSignalSafeProfiler(options)
    , Options_(options)
{ }

TSpinlockProfiler::~TSpinlockProfiler()
{
    Stop();
}

std::atomic<int> TSpinlockProfiler::SamplingRate_;
std::atomic<TSpinlockProfiler*> TSpinlockProfiler::ActiveProfiler_;
std::atomic<bool> TSpinlockProfiler::HandlingEvent_;
std::once_flag TSpinlockProfiler::HookInitialized_;

void TSpinlockProfiler::EnableProfiler()
{
    std::call_once(HookInitialized_, [] {
        absl::base_internal::RegisterSpinLockProfiler(&TSpinlockProfiler::OnEvent);
        return true;
    });

    TSpinlockProfiler* expected = nullptr;
    if (!ActiveProfiler_.compare_exchange_strong(expected, this)) {
        throw yexception() << "Another instance of spinlock profiler is running";
    }
    SamplingRate_ = Options_.ProfileFraction;
}

void TSpinlockProfiler::DisableProfiler()
{
    SamplingRate_ = 0;
    ActiveProfiler_ = nullptr;
    while (HandlingEvent_) {
        SchedYield();
    }
}

void TSpinlockProfiler::RecordEvent(const void* /*lock*/, int64_t waitCycles)
{
    unw_context_t uwContext;
    YT_VERIFY(unw_getcontext(&uwContext) == 0);

    unw_cursor_t unwCursor;
    YT_VERIFY(unw_init_local(&unwCursor, &uwContext) == 0);

    auto fpCursorContext = NBacktrace::FramePointerCursorContextFromLibunwindCursor(unwCursor);
    NBacktrace::TFramePointerCursor fpCursor(&Reader_, fpCursorContext);
    RecordSample(&fpCursor, waitCycles);
}

YT_DEFINE_THREAD_LOCAL(int, SpinlockEventCount);

void TSpinlockProfiler::OnEvent(const void* lock, int64_t waitCycles)
{
    auto samplingRate = SamplingRate_.load(std::memory_order::relaxed);
    if (samplingRate == 0) {
        return;
    }

    auto& spinlockEventCount = SpinlockEventCount();

    if (spinlockEventCount < samplingRate) {
        spinlockEventCount++;
        return;
    }

    spinlockEventCount = 0;
    while (HandlingEvent_.exchange(true)) {
        SchedYield();
    }

    if (auto profiler = ActiveProfiler_.load(); profiler) {
        profiler->RecordEvent(lock, waitCycles);
    }

    HandlingEvent_.store(false);
}

void TSpinlockProfiler::AnnotateProfile(NProto::Profile* profile, const std::function<i64(const TString&)>& stringify)
{
    auto sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("sample"));
    sampleType->set_unit(stringify("count"));

    sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("cpu"));
    sampleType->set_unit(stringify("nanoseconds"));

    auto periodType = profile->mutable_period_type();
    periodType->set_type(stringify("sample"));
    periodType->set_unit(stringify("count"));

    profile->set_period(Options_.ProfileFraction);
}

i64 TSpinlockProfiler::EncodeValue(i64 value)
{
    return value / absl::base_internal::CycleClock::Frequency() * 1e9;
}

////////////////////////////////////////////////////////////////////////////////


TBlockingProfiler::TBlockingProfiler(TSpinlockProfilerOptions options)
    : TSignalSafeProfiler(options)
    , Options_(options)
{ }

TBlockingProfiler::~TBlockingProfiler()
{
    Stop();
}

std::atomic<int> TBlockingProfiler::SamplingRate_;
std::atomic<TBlockingProfiler*> TBlockingProfiler::ActiveProfiler_;
std::atomic<bool> TBlockingProfiler::HandlingEvent_;
std::once_flag TBlockingProfiler::HookInitialized_;

void TBlockingProfiler::EnableProfiler()
{
    std::call_once(HookInitialized_, [] {
        NThreading::RegisterSpinWaitSlowPathHook(&TBlockingProfiler::OnEvent);
        return true;
    });

    TBlockingProfiler* expected = nullptr;
    if (!ActiveProfiler_.compare_exchange_strong(expected, this)) {
        throw yexception() << "Another instance of spinlock profiler is running";
    }
    SamplingRate_ = Options_.ProfileFraction;
}

void TBlockingProfiler::DisableProfiler()
{
    SamplingRate_ = 0;
    ActiveProfiler_ = nullptr;
    while (HandlingEvent_) {
        SchedYield();
    }
}

void TBlockingProfiler::RecordEvent(
    TCpuDuration cpuDelay,
    const ::TSourceLocation& /*location*/,
    NThreading::ESpinLockActivityKind /*activityKind*/)
{
    unw_context_t unwContext;
    YT_VERIFY(unw_getcontext(&unwContext) == 0);

    unw_cursor_t unwCursor;
    YT_VERIFY(unw_init_local(&unwCursor, &unwContext) == 0);

    auto fpCursorContext = NBacktrace::FramePointerCursorContextFromLibunwindCursor(unwCursor);
    NBacktrace::TFramePointerCursor fpCursor(&Reader_, fpCursorContext);
    RecordSample(&fpCursor, cpuDelay);
}

YT_DEFINE_THREAD_LOCAL(int, YTSpinlockEventCount);

void TBlockingProfiler::OnEvent(
    TCpuDuration cpuDelay,
    const ::TSourceLocation& location,
    NThreading::ESpinLockActivityKind activityKind)
{
    auto samplingRate = SamplingRate_.load(std::memory_order::relaxed);
    if (samplingRate == 0) {
        return;
    }

    auto& ytSpinlockEventCount = YTSpinlockEventCount();

    if (ytSpinlockEventCount < samplingRate) {
        ytSpinlockEventCount++;
        return;
    }

    ytSpinlockEventCount = 0;
    while (HandlingEvent_.exchange(true)) {
        SchedYield();
    }

    if (auto profiler = ActiveProfiler_.load(); profiler) {
        profiler->RecordEvent(cpuDelay, location, activityKind);
    }

    HandlingEvent_.store(false);
}

void TBlockingProfiler::AnnotateProfile(NProto::Profile* profile, const std::function<i64(const TString&)>& stringify)
{
    auto sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("sample"));
    sampleType->set_unit(stringify("count"));

    sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("cpu"));
    sampleType->set_unit(stringify("nanoseconds"));

    auto periodType = profile->mutable_period_type();
    periodType->set_type(stringify("sample"));
    periodType->set_unit(stringify("count"));

    profile->set_period(Options_.ProfileFraction);
}

i64 TBlockingProfiler::EncodeValue(i64 value)
{
    return CpuDurationToDuration(value).NanoSeconds();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
