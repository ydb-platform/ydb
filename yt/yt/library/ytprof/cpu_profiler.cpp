#include "cpu_profiler.h"
#include "symbolize.h"

#if defined(_linux_)
#include <link.h>
#include <sys/syscall.h>
#include <sys/prctl.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/backtrace/cursors/frame_pointer/frame_pointer_cursor.h>

#include <library/cpp/yt/backtrace/cursors/interop/interop.h>

#include <util/system/yield.h>

#include <csignal>
#include <functional>
#endif

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

#if not defined(_linux_)

TCpuProfiler::TCpuProfiler(TCpuProfilerOptions options)
    : TSignalSafeProfiler(options)
{ }

TCpuProfiler::~TCpuProfiler()
{ }

void TCpuProfiler::EnableProfiler()
{ }

void TCpuProfiler::DisableProfiler()
{ }

void TCpuProfiler::AnnotateProfile(NProto::Profile* /* profile */, const std::function<i64(const TString&)>& /* stringify */)
{ }

i64 TCpuProfiler::EncodeValue(i64 value)
{
    return value;
}

TCpuProfilerOptions::TSampleFilter GetActionMinExecTimeFilter(TDuration)
{
    return {};
}

#endif

////////////////////////////////////////////////////////////////////////////////

#if defined(_linux_)

std::atomic<TCpuProfiler*> TCpuProfiler::ActiveProfiler_;
std::atomic<bool> TCpuProfiler::HandlingSigprof_;

TCpuProfiler::TCpuProfiler(TCpuProfilerOptions options)
    : TSignalSafeProfiler(options)
    , Options_(options)
    , ProfilePeriod_(1000000 / Options_.SamplingFrequency)
{ }

TCpuProfiler::~TCpuProfiler()
{
    Stop();
}

void TCpuProfiler::SigProfHandler(int /* sig */, siginfo_t* info, void* ucontext)
{
    int savedErrno = errno;

    while (HandlingSigprof_.exchange(true)) {
        SchedYield();
    }

    auto profiler = ActiveProfiler_.load();
    if (profiler) {
        profiler->OnSigProf(info, reinterpret_cast<ucontext_t*>(ucontext));
    }

    HandlingSigprof_.store(false);

    errno = savedErrno;
}

void TCpuProfiler::EnableProfiler()
{
    TCpuProfiler* expected = nullptr;
    if (!ActiveProfiler_.compare_exchange_strong(expected, this)) {
        throw yexception() << "Another instance of CPU profiler is running";
    }

    struct sigaction sig;
    sig.sa_flags = SA_SIGINFO | SA_RESTART;
    sigemptyset(&sig.sa_mask);
    sig.sa_sigaction = &TCpuProfiler::SigProfHandler;

    if (sigaction(SIGPROF, &sig, NULL) != 0) {
        throw TSystemError(LastSystemError());
    }

    itimerval period;
    period.it_value.tv_sec = 0;
    period.it_value.tv_usec = 1000000 / Options_.SamplingFrequency;
    period.it_interval = period.it_value;

    if (setitimer(ITIMER_PROF, &period, nullptr) != 0) {
        throw TSystemError(LastSystemError());
    }
}

void TCpuProfiler::DisableProfiler()
{
    itimerval period{};
    if (setitimer(ITIMER_PROF, &period, nullptr) != 0) {
        throw TSystemError(LastSystemError());
    }

    struct sigaction sig;
    sig.sa_flags = SA_RESTART;
    sigemptyset(&sig.sa_mask);
    sig.sa_handler = SIG_IGN;

    if (sigaction(SIGPROF, &sig, NULL) != 0) {
        throw TSystemError(LastSystemError());
    }

    ActiveProfiler_ = nullptr;
    while (HandlingSigprof_ > 0) {
        SchedYield();
    }
}

void TCpuProfiler::AnnotateProfile(NProto::Profile* profile, const std::function<i64(const TString&)>& stringify)
{
    auto sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("sample"));
    sampleType->set_unit(stringify("count"));

    sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("cpu"));
    sampleType->set_unit(stringify("microseconds"));

    auto periodType = profile->mutable_period_type();
    periodType->set_type(stringify("cpu"));
    periodType->set_unit(stringify("microseconds"));

    profile->set_period(1000000 / Options_.SamplingFrequency);

    if (SignalOverruns_ > 0) {
        profile->add_comment(stringify("cpu.signal_overruns=" + std::to_string(SignalOverruns_)));
    }
}

i64 TCpuProfiler::EncodeValue(i64 value)
{
    return value;
}

void TCpuProfiler::OnSigProf(siginfo_t* info, ucontext_t* ucontext)
{
    SignalOverruns_ += info->si_overrun;

    for (const auto& filter : Options_.SampleFilters) {
        if (!filter()) {
            // This sample is filtered out.
            return;
        }
    }

    auto cursorContext = NBacktrace::FramePointerCursorContextFromUcontext(*ucontext);
    NBacktrace::TFramePointerCursor cursor(&Reader_, cursorContext);

    RecordSample(&cursor, ProfilePeriod_);
}

TCpuProfilerOptions::TSampleFilter GetActionMinExecTimeFilter(TDuration minExecTime)
{
    auto minCpuDuration = DurationToCpuDuration(minExecTime);

    return [minCpuDuration] {
        auto fiberStartTime = GetTraceContextTimingCheckpoint();
        if (fiberStartTime == 0) {
            return false;
        }

        auto delta = GetCpuInstant() - fiberStartTime;
        return delta > minCpuDuration;
    };
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
