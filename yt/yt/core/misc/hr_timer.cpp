#include "hr_timer.h"

#if defined(_linux_)
#include <time.h>
#elif defined(_darwin_)
#include <mach/mach_time.h>
#elif defined(_win_)
// Any other includes?
#endif

#include <limits>

namespace NYT::NHRTimer {

////////////////////////////////////////////////////////////////////////////////

static const ui64 NumberOfNsInS = 1000000000UL;
static const i64 NumberOfSamples = 1000UL;

void GetHRInstant(THRInstant* instant)
{
#if defined(_linux_)
    // See:
    // http://stackoverflow.com/questions/6814792/why-is-clock-gettime-so-erratic
    // http://stackoverflow.com/questions/7935518/is-clock-gettime-adequate-for-submicrosecond-timing
    struct timespec* ts = reinterpret_cast<struct timespec*>(instant);
    YT_VERIFY(clock_gettime(CLOCK_REALTIME, ts) == 0);
#elif defined(_darwin_)
    // See http://lists.mysql.com/commits/70966
    static mach_timebase_info_data_t info = { 0, 0 };
    if (Y_UNLIKELY(info.denom == 0)) {
        YT_VERIFY(mach_timebase_info(&info) == 0);
    }
    ui64 time;
    YT_VERIFY((time = mach_absolute_time()));
    time *= info.numer;
    time /= info.denom;
    instant->Seconds = time / NumberOfNsInS;
    instant->Nanoseconds = time % NumberOfNsInS;
#elif defined(_win_)
    static LARGE_INTEGER frequency = { 0 };
    if (Y_UNLIKELY(frequency.QuadPart == 0)) {
        YT_VERIFY(QueryPerformanceFrequency(&frequency));

    }
    LARGE_INTEGER time;
    YT_VERIFY((QueryPerformanceCounter(&time)));
    instant->Seconds = time.QuadPart / frequency.QuadPart;
    instant->Nanoseconds = (time.QuadPart % frequency.QuadPart) * NumberOfNsInS / frequency.QuadPart;
#else
    #error "Unsupported architecture"
#endif
}

THRDuration GetHRDuration(const THRInstant& begin, const THRInstant& end)
{
    if (end.Seconds == begin.Seconds) {
        YT_ASSERT(end.Nanoseconds >= begin.Nanoseconds);
        return end.Nanoseconds - begin.Nanoseconds;
    }

    YT_ASSERT(
        end.Seconds > begin.Seconds &&
        end.Seconds - begin.Seconds <
            static_cast<i64>(std::numeric_limits<THRDuration>::max() / NumberOfNsInS));

    return
        ( end.Seconds - begin.Seconds ) * NumberOfNsInS
        + end.Nanoseconds - begin.Nanoseconds;
}

THRDuration GetHRResolution()
{
    static THRDuration result = 0;
    if (Y_LIKELY(result)) {
        return result;
    }

    std::vector<THRDuration> samples(NumberOfSamples);
    THRInstant begin;
    THRInstant end;
    for (int i = 0; i < NumberOfSamples; ++i) {
        GetHRInstant(&begin);
        GetHRInstant(&end);
        samples[i] = GetHRDuration(begin, end);
    }

    std::sort(samples.begin(), samples.end());
    result = samples[samples.size() / 2];
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHRTimer
